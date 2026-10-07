//! In-flight caps: how much a host (and every host together) may have
//! between its socket and durable. Each frame read takes a [`Permit`] that
//! rides with it through the pipeline and is dropped once the frame is done
//! (durable and committed, a duplicate, rejected, or dropped). A host at its
//! cap, or any host while the node is at the global cap, stops reading its
//! socket until permits come back, so catch-up after a kick, a takeover or
//! a restart is bounded in memory however far behind the cursors are.

use parking_lot::Mutex;
use prometheus::{IntCounterVec, IntGauge, register_int_counter_vec, register_int_gauge};
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
static PAUSES: LazyLock<IntCounterVec> = LazyLock::new(|| {
    register_int_counter_vec!("vlrelay_upstream_pauses_total", "Socket reads paused at an in-flight cap", &["cap"])
        .unwrap()
});
pub static HOST_INFLIGHT_MAX: LazyLock<IntGauge> = LazyLock::new(|| {
    register_int_gauge!("vlrelay_upstream_host_inflight_events_max", "The most frames any one host has in flight")
        .unwrap()
});

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct FlowLimits {
    pub host_events: usize,
    pub host_bytes: usize,
    pub events: usize,
    pub bytes: usize,
}

impl Default for FlowLimits {
    fn default() -> Self {
        FlowLimits { host_events: 8192, host_bytes: 64 << 20, events: 32768, bytes: 384 << 20 }
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
}

impl Flow {
    pub fn new(limits: FlowLimits) -> Arc<Flow> {
        Arc::new(Flow {
            limits: Mutex::new(limits),
            counts: Counts::default(),
            notify: Notify::new(),
            waiters: AtomicUsize::new(0),
        })
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
        } else {
            None
        }
    }

    pub fn has_room(&self, h: &HostFlow) -> bool {
        self.full(h).is_none()
    }

    /// The cap `h` is at, as the host's status reports it.
    pub fn backpressure(&self, h: &HostFlow) -> Option<super::Backpressure> {
        self.full(h).map(|cap| match cap {
            "host" => super::Backpressure::InflightFull,
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

    /// Returns once `h` is under its cap and the node under the global one.
    /// Cancel-safe.
    pub async fn wait_room(&self, h: &HostFlow) {
        let Some(cap) = self.full(h) else { return };
        PAUSES.with_label_values(&[cap]).inc();
        struct Waiting<'a>(&'a Flow, &'a HostFlow);
        impl Drop for Waiting<'_> {
            fn drop(&mut self) {
                self.0.waiters.fetch_sub(1, Ordering::AcqRel);
                self.1.waiting.store(false, Ordering::Release);
                PAUSED_HOSTS.dec();
            }
        }
        self.waiters.fetch_add(1, Ordering::AcqRel);
        h.waiting.store(true, Ordering::Release);
        PAUSED_HOSTS.inc();
        let _w = Waiting(self, h);
        loop {
            let global = self.notify.notified();
            let host = h.notify.notified();
            tokio::pin!(global, host);
            global.as_mut().enable();
            host.as_mut().enable();
            if self.has_room(h) {
                return;
            }
            // the timer covers a cap lowered while we wait
            tokio::select! {
                _ = global => {}
                _ = host => {}
                _ = tokio::time::sleep(Duration::from_millis(250)) => {}
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
            f.notify.notify_waiters();
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn a_host_at_its_cap_waits_for_permits() {
        let f = Flow::new(FlowLimits { host_events: 2, host_bytes: 1 << 20, events: 3, bytes: 1 << 20 });
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
}
