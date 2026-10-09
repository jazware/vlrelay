//! In-flight caps: how much a host (and every host together) may have
//! between its socket and durable. Each frame read takes a [`Permit`] that
//! rides with it through the pipeline and is dropped once the frame is done
//! (durable and committed, a duplicate, rejected, or dropped). A host at its
//! cap, or any host while the node is at the global cap, stops reading its
//! socket until permits come back, so catch-up after a kick, a takeover or
//! a restart is bounded in memory however far behind the cursors are.
//!
//! A paused host reads again only once it and the node are under
//! [`RESUME`] of their caps, and paused hosts are woken one per frame done
//! rather than all at once. Waking every paused host on each frame done
//! let them all through on the first free slot: the busy ones overfilled
//! the cap together and the rest flapped between paused and reading, with
//! every paused host polled again on every frame done.
//!
//! The memory budget (`--ingest-mem-mb`) is a ceiling on the process's
//! memory, not a cap the pipeline counts, so it can't be the only way back
//! to reading: freed memory the allocator keeps, or memory held outside the
//! pipeline, doesn't fall when the hosts stop. Over it the node may hold
//! [`MEM_HOLD`] of its in-flight caps, and paused hosts read again once it
//! holds under [`MEM_DRAINED`]: a drained pipeline always reads, whatever
//! the process's memory says.

use parking_lot::Mutex;
use prometheus::{
    IntCounter, IntCounterVec, IntGauge, IntGaugeVec, register_int_counter, register_int_counter_vec,
    register_int_gauge, register_int_gauge_vec,
};
use std::sync::Arc;
use std::sync::LazyLock;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::time::Duration;
use tokio::sync::Notify;

static INFLIGHT_EVENTS: LazyLock<IntGauge> = LazyLock::new(|| {
    register_int_gauge!("vlrelay_upstream_inflight_events", "Upstream frames read and not yet done, all hosts").unwrap()
});
static INFLIGHT_BYTES: LazyLock<IntGauge> = LazyLock::new(|| {
    register_int_gauge!("vlrelay_upstream_inflight_bytes", "Bytes of upstream frames read and not yet done, all hosts")
        .unwrap()
});
static PAUSED_HOSTS: LazyLock<IntGauge> = LazyLock::new(|| {
    register_int_gauge!("vlrelay_upstream_paused_hosts", "Hosts whose socket isn't read because of an in-flight cap")
        .unwrap()
});
static PAUSED_BY: LazyLock<IntGaugeVec> = LazyLock::new(|| {
    register_int_gauge_vec!(
        "vlrelay_upstream_paused_hosts_by_cap",
        "Paused hosts by the cap that paused them: host, global (in-flight caps) or memory (--ingest-mem-mb)",
        &["cap"]
    )
    .unwrap()
});
static PAUSES: LazyLock<IntCounterVec> = LazyLock::new(|| {
    register_int_counter_vec!("vlrelay_upstream_pauses_total", "Socket reads paused at an in-flight cap", &["cap"])
        .unwrap()
});
static MEMORY: LazyLock<IntGauge> = LazyLock::new(|| {
    register_int_gauge!(
        "vlrelay_process_memory_bytes",
        "The process's anonymous memory (its cgroup's anon, or jemalloc's resident bytes), as --ingest-mem-mb counts it"
    )
    .unwrap()
});
static MEMORY_OVER: LazyLock<IntGauge> = LazyLock::new(|| {
    register_int_gauge!(
        "vlrelay_upstream_memory_over",
        "1 from when the process's memory passes --ingest-mem-mb until it's back under 90% of it"
    )
    .unwrap()
});
static MEMORY_PAUSED: LazyLock<IntGauge> = LazyLock::new(|| {
    register_int_gauge!(
        "vlrelay_upstream_memory_paused",
        "1 while --ingest-mem-mb holds upstream reads: over it, with the pipeline past its share of the in-flight caps"
    )
    .unwrap()
});
static MEMORY_MARKS: LazyLock<IntGaugeVec> = LazyLock::new(|| {
    register_int_gauge_vec!(
        "vlrelay_upstream_memory_mark_bytes",
        "--ingest-mem-mb's marks: pause (the budget) and resume (90% of it)",
        &["mark"]
    )
    .unwrap()
});
static MEMORY_PURGES: LazyLock<IntCounter> = LazyLock::new(|| {
    register_int_counter!(
        "vlrelay_upstream_memory_purges_total",
        "Freed pages handed back to the OS because the process was over --ingest-mem-mb"
    )
    .unwrap()
});
pub static HOST_INFLIGHT_MAX: LazyLock<IntGauge> = LazyLock::new(|| {
    register_int_gauge!("vlrelay_upstream_host_inflight_events_max", "The most frames any one host has in flight")
        .unwrap()
});

/// Share of each cap a paused host waits to be under before reading again.
pub const RESUME: f64 = 0.9;

/// Under this share of the node's caps every paused host is woken: the
/// pipeline has drained, and one at a time would leave it idle.
const DRAINED: f64 = 0.5;

/// The share of the node's caps it may hold while over its memory budget.
pub const MEM_HOLD: f64 = 0.1;

/// Under this share of the node's caps paused hosts read again, over the
/// memory budget or not: what's left isn't the pipeline's to give back.
pub const MEM_DRAINED: f64 = 0.05;

/// How often a node over its memory budget purges the allocator.
const PURGE_EVERY: Duration = Duration::from_secs(1);

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct FlowLimits {
    pub host_events: usize,
    pub host_bytes: usize,
    pub events: usize,
    pub bytes: usize,
    /// The process's anonymous memory ([`process_memory`]) past which no
    /// host is read; 0: none.
    pub memory: u64,
}

impl Default for FlowLimits {
    fn default() -> Self {
        FlowLimits { host_events: 8192, host_bytes: 64 << 20, events: 32768, bytes: 384 << 20, memory: 0 }
    }
}

#[derive(Default)]
struct Counts {
    events: AtomicUsize,
    bytes: AtomicUsize,
}

/// One host's frames in flight. Lives on its registry entry, so permits of
/// a socket that's gone still come back to the same counts.
#[derive(Default)]
pub struct HostFlow {
    counts: Counts,
    notify: Notify,
    waiting: AtomicBool,
}

impl HostFlow {
    pub fn events(&self) -> usize {
        self.counts.events.load(Ordering::Relaxed)
    }

    pub fn bytes(&self) -> usize {
        self.counts.bytes.load(Ordering::Relaxed)
    }

    pub fn paused(&self) -> bool {
        self.waiting.load(Ordering::Relaxed)
    }
}

/// The node's caps, shared by every host task.
pub struct Flow {
    limits: Mutex<FlowLimits>,
    counts: Counts,
    notify: Notify,
    waiters: AtomicUsize,
    /// Over `FlowLimits::memory` ([`Flow::watch_memory`]).
    mem_over: AtomicBool,
    probe: Arc<dyn MemoryProbe>,
}

/// What [`Flow::watch_memory`] reads the process's memory with.
pub trait MemoryProbe: Send + Sync + 'static {
    fn sample(&self) -> Option<u64>;
    /// Hands freed pages the allocator keeps back to the OS.
    fn purge(&self) {}
}

struct ProcessMemory;

impl MemoryProbe for ProcessMemory {
    fn sample(&self) -> Option<u64> {
        process_memory()
    }

    fn purge(&self) {
        crate::qlog::flush::release_freed();
    }
}

impl Flow {
    pub fn new(limits: FlowLimits) -> Arc<Flow> {
        Arc::new(Flow {
            limits: Mutex::new(limits),
            counts: Counts::default(),
            notify: Notify::new(),
            waiters: AtomicUsize::new(0),
            mem_over: AtomicBool::new(false),
            probe: Arc::new(ProcessMemory),
        })
    }

    pub fn with_probe(limits: FlowLimits, probe: Arc<dyn MemoryProbe>) -> Arc<Flow> {
        let mut f = Flow::new(limits);
        Arc::get_mut(&mut f).expect("just made").probe = probe;
        f
    }

    /// Samples the process's memory against `FlowLimits::memory`, if set.
    /// The in-flight caps count frames, not what each one costs on its way
    /// (decoded, copied into the log and the ring) or what else the heap
    /// holds meanwhile (a flush, the state's memtables, the PLC export), so
    /// a catch-up burst could pass a small box's limit with every cap
    /// respected.
    pub fn watch_memory(self: &Arc<Self>) -> Option<tokio::task::JoinHandle<()>> {
        let budget = self.limits().memory;
        if budget == 0 {
            return None;
        }
        MEMORY_MARKS.with_label_values(&["pause"]).set(budget as i64);
        MEMORY_MARKS.with_label_values(&["resume"]).set((budget as f64 * RESUME) as i64);
        let me = Arc::downgrade(self);
        Some(tokio::spawn(async move {
            let mut t = tokio::time::interval(Duration::from_millis(200));
            t.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
            let mut purged: Option<std::time::Instant> = None;
            loop {
                t.tick().await;
                let Some(f) = me.upgrade() else { return };
                let Some(mut m) = f.probe.sample() else { continue };
                let was = f.mem_over.load(Ordering::Acquire);
                let past = |m: u64| if was { m as f64 >= budget as f64 * RESUME } else { m > budget };
                // jemalloc decays freed pages only as it allocates, and a
                // paused node allocates little, so what it freed would stay
                // counted
                if past(m) && purged.is_none_or(|t| t.elapsed() >= PURGE_EVERY) {
                    f.probe.purge();
                    MEMORY_PURGES.inc();
                    purged = Some(std::time::Instant::now());
                    m = f.probe.sample().unwrap_or(m);
                }
                MEMORY.set(m as i64);
                let over = past(m);
                if over != was {
                    f.mem_over.store(over, Ordering::Release);
                    MEMORY_OVER.set(over as i64);
                    tracing::info!(
                        memory_mb = m >> 20,
                        budget_mb = budget >> 20,
                        over,
                        inflight_mb = f.counts.bytes.load(Ordering::Relaxed) >> 20,
                        "upstream: memory budget"
                    );
                    if !over {
                        f.notify.notify_waiters();
                    }
                }
                MEMORY_PAUSED.set(f.mem_holds(&f.limits(), MEM_HOLD) as i64);
            }
        }))
    }

    /// Whether the memory budget holds reads: the process is over it and
    /// the node holds at least `share` of its caps.
    fn mem_holds(&self, l: &FlowLimits, share: f64) -> bool {
        self.mem_over.load(Ordering::Acquire) && !self.node_under(l, share)
    }

    pub fn limits(&self) -> FlowLimits {
        *self.limits.lock()
    }

    pub fn events(&self) -> usize {
        self.counts.events.load(Ordering::Relaxed)
    }

    /// How full the node's caps are, 0-1 (the fuller of events and bytes).
    pub fn fill(&self) -> f64 {
        let l = self.limits();
        let e = self.counts.events.load(Ordering::Relaxed) as f64 / l.events.max(1) as f64;
        let b = self.counts.bytes.load(Ordering::Relaxed) as f64 / l.bytes.max(1) as f64;
        e.max(b).min(1.0)
    }

    /// Which cap `h` is at, if any.
    fn full(&self, h: &HostFlow) -> Option<&'static str> {
        let l = self.limits();
        if h.events() >= l.host_events.max(1) || h.bytes() >= l.host_bytes.max(1) {
            Some("host")
        } else if self.counts.events.load(Ordering::Relaxed) >= l.events.max(1)
            || self.counts.bytes.load(Ordering::Relaxed) >= l.bytes.max(1)
        {
            Some("global")
        } else if self.mem_holds(&l, MEM_HOLD) {
            Some("memory")
        } else {
            None
        }
    }

    pub fn has_room(&self, h: &HostFlow) -> bool {
        self.full(h).is_none()
    }

    /// Whether the node holds less than `share` of its caps.
    fn node_under(&self, l: &FlowLimits, share: f64) -> bool {
        under(self.counts.events.load(Ordering::Relaxed), l.events, share)
            && under(self.counts.bytes.load(Ordering::Relaxed), l.bytes, share)
    }

    /// Whether a paused `h` may read again.
    fn resumes(&self, h: &HostFlow) -> bool {
        let l = self.limits();
        under(h.events(), l.host_events, RESUME)
            && under(h.bytes(), l.host_bytes, RESUME)
            && self.node_under(&l, RESUME)
            && !self.mem_holds(&l, MEM_DRAINED)
    }

    /// The cap `h` is at, as the host's status reports it.
    pub fn backpressure(&self, h: &HostFlow) -> Option<super::Backpressure> {
        self.full(h).map(|cap| match cap {
            "host" => super::Backpressure::InflightFull,
            "memory" => super::Backpressure::MemoryFull,
            _ => super::Backpressure::NodeInflightFull,
        })
    }

    /// Counts a frame of `len` bytes in flight until the permit drops.
    pub fn acquire(self: &Arc<Self>, h: &Arc<HostFlow>, len: usize) -> Arc<Permit> {
        h.counts.events.fetch_add(1, Ordering::Relaxed);
        h.counts.bytes.fetch_add(len, Ordering::Relaxed);
        self.counts.events.fetch_add(1, Ordering::Relaxed);
        self.counts.bytes.fetch_add(len, Ordering::Relaxed);
        INFLIGHT_EVENTS.inc();
        INFLIGHT_BYTES.add(len as i64);
        Arc::new(Permit { flow: self.clone(), host: h.clone(), len })
    }

    /// Returns once `h` and the node are under [`RESUME`] of their caps
    /// (and under [`MEM_DRAINED`] of them while over the memory budget).
    /// Cancel-safe.
    pub async fn wait_room(&self, h: &HostFlow) {
        let Some(cap) = self.full(h) else { return };
        PAUSES.with_label_values(&[cap]).inc();
        struct Waiting<'a>(&'a Flow, &'a HostFlow, &'static str);
        impl Drop for Waiting<'_> {
            fn drop(&mut self) {
                self.0.waiters.fetch_sub(1, Ordering::AcqRel);
                self.1.waiting.store(false, Ordering::Release);
                PAUSED_HOSTS.dec();
                PAUSED_BY.with_label_values(&[self.2]).dec();
            }
        }
        self.waiters.fetch_add(1, Ordering::AcqRel);
        h.waiting.store(true, Ordering::Release);
        PAUSED_HOSTS.inc();
        PAUSED_BY.with_label_values(&[cap]).inc();
        let _w = Waiting(self, h, cap);
        loop {
            let global = self.notify.notified();
            let host = h.notify.notified();
            tokio::pin!(global, host);
            global.as_mut().enable();
            host.as_mut().enable();
            if self.resumes(h) {
                return;
            }
            // a safety net for frames that stay in flight without finishing,
            // spread out so the waiters don't come back together
            let recheck = Duration::from_millis(500 + rand::random::<u64>() % 1000);
            tokio::select! {
                _ = global => {}
                _ = host => {}
                _ = tokio::time::sleep(recheck) => {}
            }
        }
    }
}

/// One frame in flight. Dropping it gives the room back.
pub struct Permit {
    flow: Arc<Flow>,
    host: Arc<HostFlow>,
    len: usize,
}

impl std::fmt::Debug for Permit {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Permit").field("len", &self.len).finish()
    }
}

impl Drop for Permit {
    fn drop(&mut self) {
        let (f, h) = (&self.flow, &self.host);
        h.counts.events.fetch_sub(1, Ordering::Relaxed);
        h.counts.bytes.fetch_sub(self.len, Ordering::Relaxed);
        f.counts.events.fetch_sub(1, Ordering::Relaxed);
        f.counts.bytes.fetch_sub(self.len, Ordering::Relaxed);
        INFLIGHT_EVENTS.dec();
        INFLIGHT_BYTES.sub(self.len as i64);
        if h.waiting.load(Ordering::Acquire) {
            h.notify.notify_one();
        }
        if f.waiters.load(Ordering::Acquire) > 0 {
            let l = f.limits();
            // over the memory budget no waiter goes until the pipeline has
            // drained, so waking them sooner would only poll them all
            let (drained, one) = match f.mem_over.load(Ordering::Acquire) {
                true => (MEM_DRAINED, false),
                false => (DRAINED, f.node_under(&l, RESUME)),
            };
            if f.node_under(&l, drained) {
                f.notify.notify_waiters();
            } else if one {
                f.notify.notify_one();
            }
        }
    }
}

fn under(n: usize, cap: usize, share: f64) -> bool {
    (n as f64) < cap.max(1) as f64 * share
}

/// The process's anonymous memory: its cgroup v2's `anon`, which counts
/// what jemalloc holds and hasn't returned, or outside one jemalloc's
/// resident bytes. Not the sockets' buffers: reading is what drains them,
/// so pausing for them would never end (`--upstream-rcvbuf-kb` bounds
/// them instead).
pub fn process_memory() -> Option<u64> {
    static STAT: LazyLock<Option<std::path::PathBuf>> = LazyLock::new(|| {
        let c = std::fs::read_to_string("/proc/self/cgroup").ok()?;
        let rel = c.lines().find_map(|l| l.strip_prefix("0::"))?;
        let p = std::path::Path::new("/sys/fs/cgroup").join(rel.trim_start_matches('/')).join("memory.stat");
        p.exists().then_some(p)
    });
    if let Some(p) = STAT.as_ref()
        && let Ok(s) = std::fs::read_to_string(p)
    {
        return s.lines().find_map(|l| l.strip_prefix("anon ")).and_then(|v| v.trim().parse().ok());
    }
    tikv_jemalloc_ctl::epoch::advance().ok()?;
    tikv_jemalloc_ctl::stats::resident::read().ok().map(|b| b as u64)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn a_host_at_its_cap_waits_for_permits() {
        let f = Flow::new(FlowLimits { host_events: 2, host_bytes: 1 << 20, events: 3, bytes: 1 << 20, memory: 0 });
        let a = Arc::new(HostFlow::default());
        let b = Arc::new(HostFlow::default());
        let p1 = f.acquire(&a, 10);
        let p2 = f.acquire(&a, 10);
        assert!(!f.has_room(&a) && f.has_room(&b));
        let f2 = f.clone();
        let a2 = a.clone();
        let waiter = tokio::spawn(async move { f2.wait_room(&a2).await });
        tokio::time::sleep(Duration::from_millis(20)).await;
        assert!(!waiter.is_finished());
        assert!(a.paused());
        drop(p1);
        tokio::time::timeout(Duration::from_millis(100), waiter).await.expect("woken").unwrap();
        assert!(!a.paused());
        // the global cap holds every host
        let p3 = f.acquire(&b, 10);
        let p4 = f.acquire(&b, 10);
        assert!(!f.has_room(&b) && !f.has_room(&Arc::new(HostFlow::default())));
        drop((p2, p3, p4));
        assert_eq!((f.events(), a.events(), a.bytes()), (0, 0, 0));
    }

    #[tokio::test]
    async fn paused_hosts_wait_for_the_resume_mark_and_leave_one_at_a_time() {
        let f = Flow::new(FlowLimits { host_events: 100, host_bytes: 1 << 20, events: 10, bytes: 1 << 20, memory: 0 });
        let busy = Arc::new(HostFlow::default());
        let mut held: Vec<_> = (0..10).map(|_| f.acquire(&busy, 10)).collect();
        let hosts: Vec<_> = (0..3).map(|_| Arc::new(HostFlow::default())).collect();
        let waiters: Vec<_> = hosts
            .iter()
            .map(|h| {
                let (f, h) = (f.clone(), h.clone());
                tokio::spawn(async move { f.wait_room(&h).await })
            })
            .collect();
        tokio::time::sleep(Duration::from_millis(20)).await;
        // one slot free is under the cap, not under the resume mark
        held.pop();
        tokio::time::sleep(Duration::from_millis(50)).await;
        assert!(waiters.iter().all(|w| !w.is_finished()));
        assert!(hosts.iter().all(|h| h.paused()));
        // under it, each frame done lets one host go
        held.pop();
        tokio::time::sleep(Duration::from_millis(50)).await;
        assert_eq!(waiters.iter().filter(|w| w.is_finished()).count(), 1);
        held.pop();
        tokio::time::sleep(Duration::from_millis(50)).await;
        assert_eq!(waiters.iter().filter(|w| w.is_finished()).count(), 2);
        // drained: everyone
        held.truncate(4);
        held.pop();
        for w in waiters {
            tokio::time::timeout(Duration::from_millis(100), w).await.expect("woken").unwrap();
        }
    }

    /// Over the memory budget the node holds a tenth of its caps, and
    /// paused hosts read again under a twentieth: a drained pipeline reads
    /// whatever the memory says.
    #[tokio::test]
    async fn over_the_memory_budget_the_pipeline_drains_before_hosts_read() {
        let f = Flow::new(FlowLimits { events: 40, memory: 1, ..FlowLimits::default() });
        let a = Arc::new(HostFlow::default());
        let b = Arc::new(HostFlow::default());
        let mut held: Vec<_> = (0..3).map(|_| f.acquire(&a, 10)).collect();
        assert!(f.has_room(&b));
        f.mem_over.store(true, Ordering::Release);
        assert!(f.has_room(&b), "under the memory share");
        held.push(f.acquire(&a, 10));
        assert!(!f.has_room(&b));
        assert_eq!(f.backpressure(&b), Some(super::super::Backpressure::MemoryFull));
        let (f2, b2) = (f.clone(), b.clone());
        let w = tokio::spawn(async move { f2.wait_room(&b2).await });
        tokio::time::sleep(Duration::from_millis(20)).await;
        assert!(!w.is_finished() && b.paused());
        // under the share it paused at, not under the one it resumes at
        held.truncate(2);
        tokio::time::sleep(Duration::from_millis(50)).await;
        assert!(f.has_room(&b) && !w.is_finished());
        held.pop();
        tokio::time::timeout(Duration::from_millis(100), w).await.expect("woken by the last frame done").unwrap();
    }

    /// Under 90% of the budget the memory pause ends however full the
    /// pipeline is (its own caps still apply).
    #[tokio::test]
    async fn memory_back_under_the_resume_mark_wakes_every_host() {
        let probe = Arc::new(Probe::default());
        probe.set(2_000, 2_000);
        let f = Flow::with_probe(FlowLimits { events: 40, memory: 1_000, ..FlowLimits::default() }, probe.clone());
        let _watch = f.watch_memory().unwrap();
        let busy = Arc::new(HostFlow::default());
        let _held: Vec<_> = (0..10).map(|_| f.acquire(&busy, 10)).collect();
        wait_for("the memory pause", || f.mem_over.load(Ordering::Acquire)).await;
        let hosts: Vec<_> = (0..3).map(|_| Arc::new(HostFlow::default())).collect();
        let waiters: Vec<_> = hosts
            .iter()
            .map(|h| {
                let (f, h) = (f.clone(), h.clone());
                tokio::spawn(async move { f.wait_room(&h).await })
            })
            .collect();
        // between the marks: still over
        probe.set(950, 950);
        tokio::time::sleep(Duration::from_millis(500)).await;
        assert!(f.mem_over.load(Ordering::Acquire));
        assert!(waiters.iter().all(|w| !w.is_finished()));
        probe.set(800, 800);
        for w in waiters {
            tokio::time::timeout(Duration::from_secs(1), w).await.expect("woken").unwrap();
        }
        assert!(!f.mem_over.load(Ordering::Acquire));
    }

    /// Memory past the budget is purged first, and only what's still over
    /// it then pauses.
    #[tokio::test]
    async fn memory_the_allocator_gives_back_never_pauses() {
        let probe = Arc::new(Probe::default());
        let f = Flow::with_probe(FlowLimits { memory: 1_000, ..FlowLimits::default() }, probe.clone());
        let _watch = f.watch_memory().unwrap();
        probe.set(1_200, 700);
        wait_for("a purge", || probe.purges.load(Ordering::Relaxed) > 0).await;
        tokio::time::sleep(Duration::from_millis(500)).await;
        assert!(!f.mem_over.load(Ordering::Acquire));
        assert_eq!(probe.purges.load(Ordering::Relaxed), 1, "under the budget nothing is purged");
        probe.set(1_200, 1_100);
        wait_for("the memory pause", || f.mem_over.load(Ordering::Acquire)).await;
    }

    /// The process's memory as a test sets it; a purge takes it down to
    /// `after_purge`.
    #[derive(Default)]
    struct Probe {
        now: std::sync::atomic::AtomicU64,
        after_purge: std::sync::atomic::AtomicU64,
        purges: AtomicUsize,
    }

    impl Probe {
        fn set(&self, now: u64, after_purge: u64) {
            self.now.store(now, Ordering::Relaxed);
            self.after_purge.store(after_purge, Ordering::Relaxed);
        }
    }

    impl MemoryProbe for Probe {
        fn sample(&self) -> Option<u64> {
            Some(self.now.load(Ordering::Relaxed))
        }

        fn purge(&self) {
            self.purges.fetch_add(1, Ordering::Relaxed);
            self.now.fetch_min(self.after_purge.load(Ordering::Relaxed), Ordering::Relaxed);
        }
    }

    async fn wait_for(what: &str, f: impl Fn() -> bool) {
        let t0 = std::time::Instant::now();
        while !f() {
            assert!(t0.elapsed() < Duration::from_secs(5), "{what}");
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    }

    /// A pause at the memory budget whose memory doesn't come back (the
    /// allocator keeps freed pages, or it's held outside the pipeline)
    /// stopped every host for good: nothing was in flight to finish, and
    /// reads resumed only under 90% of the budget.
    #[tokio::test]
    async fn memory_over_the_resume_mark_with_nothing_in_flight_resumes() {
        let probe = Arc::new(Probe::default());
        probe.set(2_000, 2_000);
        let f = Flow::with_probe(FlowLimits { memory: 1_000, ..FlowLimits::default() }, probe.clone());
        let _watch = f.watch_memory().unwrap();
        let h = Arc::new(HostFlow::default());
        let p = f.acquire(&h, 10);
        wait_for("the memory pause", || f.mem_over.load(Ordering::Acquire)).await;
        // what the pipeline held is done; the memory stays over the resume mark
        probe.set(950, 950);
        drop(p);
        let (f2, h2) = (f.clone(), h.clone());
        let w = tokio::spawn(async move { f2.wait_room(&h2).await });
        tokio::time::timeout(Duration::from_secs(3), w).await.expect("a drained pipeline reads again").unwrap();
    }
}
