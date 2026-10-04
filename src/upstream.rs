//! Upstream subscriptions: the host registry, one websocket task per host,
//! per-host rate limits, a weighted fair queue into one output channel, and
//! requestCrawl.
//!
//! Cursors come in two kinds. A host's *received* seq is the last one read
//! off its socket. Its *acked* seq is the last one whose events are all
//! durable; the relay advances it through [`Manager::ack`] once the log is in
//! the bucket, and only that one is persisted and resumed from. A reconnect
//! drops whatever was still queued and replays from the acked cursor, so
//! downstream sees each event at least once and never a gap.

pub(crate) mod client;
pub mod crawl;
pub mod fair;
pub mod flow;
pub mod frame;
pub mod host;
pub mod limits;

pub use crawl::{Admission, CrawlError, CrawlPolicy, Crawler, DomainAction, DomainRule};
pub use host::{
    ErrorCounters, HostEntry, HostRecord, HostStatus, HostStore, HostView, HostnameError, MemHostStore, Registry, Tier,
    normalize_hostname,
};
pub use limits::{Limits, TierLimits, TokenBucket};

use crate::types::{Host, UpstreamFrame};
use fair::{FairQueue, HostQueue};
use parking_lot::Mutex;
use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::{Notify, mpsc, watch};
use tokio::task::JoinHandle;

/// Which hosts this node subscribes to. A cluster narrows it to the hosts
/// in the host shards this node owns; a single node takes every host.
pub type HostFilter = Arc<dyn Fn(&Host) -> bool + Send + Sync>;

/// Called when a host's socket is up, before any of its frames: the host,
/// the socket's epoch, the cursor it resumed after (None: live), and whether
/// the host's sequence restarted (FutureCursor).
pub type ConnectFn = Arc<dyn Fn(&Host, u64, Option<i64>, bool) + Send + Sync>;

/// Base URL for a host (`https://{host}` in production); the client turns
/// it into the `wss://` subscribeRepos URL.
pub type EndpointFn = Arc<dyn Fn(&Host) -> String + Send + Sync>;

#[derive(Clone)]
pub struct UpstreamConfig {
    /// Allows `ws://`, IPs, localhost and ports.
    pub dev_mode: bool,
    pub endpoint: EndpointFn,
    pub limits: Limits,
    pub backoff_base: Duration,
    pub backoff_max: Duration,
    pub connect_timeout: Duration,
    /// Sent only when a socket has been quiet this long.
    pub ping_interval: Duration,
    /// No frame, ping or pong for this long and the socket is dropped.
    pub stall_timeout: Duration,
    /// Per host. A full queue stops the host's reads.
    pub host_queue_frames: usize,
    pub output_capacity: usize,
    /// Bytes a weight-1 host sends per fair-queue round.
    pub quantum_bytes: usize,
    pub flush_interval: Duration,
    pub max_frame_bytes: usize,
    /// tungstenite's per-socket read buffer; its 128 KiB default dominates
    /// an idle connection's memory.
    pub read_buffer_bytes: usize,
    /// Frames read and not yet done, per host and in all; a host at a cap
    /// isn't read.
    pub inflight: flow::FlowLimits,
}

impl UpstreamConfig {
    pub fn new(dev_mode: bool) -> UpstreamConfig {
        UpstreamConfig {
            dev_mode,
            endpoint: Arc::new(|h: &Host| format!("https://{}", h.0)),
            limits: Limits::default(),
            backoff_base: Duration::from_secs(1),
            backoff_max: Duration::from_secs(300),
            connect_timeout: Duration::from_secs(15),
            ping_interval: Duration::from_secs(30),
            stall_timeout: Duration::from_secs(90),
            host_queue_frames: 256,
            output_capacity: 1024,
            quantum_bytes: 16 * 1024,
            flush_interval: Duration::from_secs(5),
            // the reference relay's limit on a single event
            max_frame_bytes: 5 << 20,
            read_buffer_bytes: 16 * 1024,
            inflight: flow::FlowLimits::default(),
        }
    }
}

/// Where a reconnecting host resumes. The relay wires this to its durable
/// checkpoints; without one the manager uses the registry's acked seq.
pub trait CursorSource: Send + Sync + 'static {
    fn durable_cursor(&self, host: &Host) -> Option<i64>;
    /// The host answered our cursor with FutureCursor and is being resumed live.
    fn on_future_cursor(&self, _host: &Host) {}
}

/// What the policy engine says about one host. The tier is what the host
/// runs as after domain rules; `limits` replaces the tier's defaults (the
/// manager keeps the fair-queue weight of the tier).
#[derive(Clone, Debug, PartialEq)]
pub struct HostPolicy {
    pub tier: Tier,
    pub connect: bool,
    pub limits: Option<TierLimits>,
}

/// The policy engine, as the manager sees it. It owns host tiers: the
/// registry follows it and doesn't persist tiers of its own.
pub trait PolicySource: Send + Sync + 'static {
    /// None: no opinion yet (the registry's tier and default limits apply).
    fn host_policy(&self, host: &Host) -> Option<HostPolicy>;
}

struct RegistryCursor(Arc<Registry>);

impl CursorSource for RegistryCursor {
    fn durable_cursor(&self, host: &Host) -> Option<i64> {
        self.0.get(host).and_then(|e| e.acked_seq())
    }
}

struct Running {
    stop: watch::Sender<bool>,
    kick: Arc<Notify>,
    wake: Arc<Notify>,
    queue: Arc<HostQueue>,
    join: JoinHandle<()>,
}

pub struct Manager {
    cfg: Arc<UpstreamConfig>,
    registry: Arc<Registry>,
    cursor: Arc<dyn CursorSource>,
    fair: FairQueue,
    out: Mutex<Option<mpsc::Sender<UpstreamFrame>>>,
    tasks: Mutex<HashMap<Host, Running>>,
    background: Mutex<Vec<JoinHandle<()>>>,
    filter: Mutex<Option<HostFilter>>,
    policy: parking_lot::RwLock<Option<Arc<dyn PolicySource>>>,
    on_refused: parking_lot::RwLock<Option<OnRefused>>,
    on_connect: parking_lot::RwLock<Option<ConnectFn>>,
    flow: Arc<flow::Flow>,
}

/// Called once when a host is refused for good (`client::Refused`): its
/// task has stopped, and the hook records the ban.
pub type OnRefused = Arc<dyn Fn(&Host, &str) + Send + Sync>;

impl Manager {
    /// Returns the manager and the receiving end of its single output
    /// channel. Nothing connects until [`Manager::start`].
    pub fn new(
        cfg: UpstreamConfig,
        store: Arc<dyn HostStore>,
        cursor: Option<Arc<dyn CursorSource>>,
    ) -> (Arc<Manager>, mpsc::Receiver<UpstreamFrame>) {
        let registry = Arc::new(Registry::new(store));
        let cursor = cursor.unwrap_or_else(|| Arc::new(RegistryCursor(registry.clone())));
        let (tx, rx) = mpsc::channel(cfg.output_capacity.max(1));
        let fair = FairQueue::new(cfg.quantum_bytes);
        let flow = flow::Flow::new(cfg.inflight);
        let m = Manager {
            cfg: Arc::new(cfg),
            registry,
            cursor,
            fair,
            out: Mutex::new(Some(tx)),
            tasks: Mutex::new(HashMap::new()),
            background: Mutex::new(Vec::new()),
            filter: Mutex::new(None),
            policy: parking_lot::RwLock::new(None),
            on_refused: parking_lot::RwLock::new(None),
            on_connect: parking_lot::RwLock::new(None),
            flow,
        };
        (Arc::new(m), rx)
    }

    pub fn set_on_refused(&self, f: OnRefused) {
        *self.on_refused.write() = Some(f);
    }

    pub fn config(&self) -> &UpstreamConfig {
        &self.cfg
    }

    pub fn registry(&self) -> &Arc<Registry> {
        &self.registry
    }

    /// Hands host tiers and limits to the policy engine. Set it before
    /// [`Manager::start`] so no host connects against the policy.
    pub fn set_policy_source(&self, p: Arc<dyn PolicySource>) {
        *self.policy.write() = Some(p);
    }

    /// Set before [`Manager::start`]: sockets opened earlier don't call it.
    pub fn on_connect(&self, f: ConnectFn) {
        *self.on_connect.write() = Some(f);
    }

    /// Copies the policy's view of the host onto its entry and says whether
    /// it should have a socket.
    fn follow_policy(&self, e: &HostEntry) -> bool {
        let p = self.policy.read().clone();
        match p.and_then(|p| p.host_policy(&e.host)) {
            Some(hp) => {
                e.set_tier(hp.tier);
                e.set_limits(hp.limits.map(|mut l| {
                    l.weight = self.cfg.limits.for_tier(hp.tier).weight;
                    l
                }));
                hp.connect
            }
            None => e.tier().connects(),
        }
    }

    /// Applies the policy's current view of `host`: tier and limits on the
    /// running socket, and a disconnect or a connect when that flips.
    pub async fn apply_policy(self: &Arc<Self>, host: &Host) {
        let Some(e) = self.registry.get(host) else { return };
        let connect = self.follow_policy(&e);
        if !connect {
            if let Some(j) = self.stop_host(host) {
                tracing::info!(host = %host.0, tier = e.tier().as_str(), "upstream disconnected by policy");
                let _ = j.await;
            }
            return;
        }
        if let Some(r) = self.tasks.lock().get(host) {
            r.queue.set_weight(self.cfg.limits.for_tier(e.tier()).weight);
        }
        if self.out.lock().is_none() {
            self.spawn_host(e);
        }
    }

    /// Loads the registry and connects every host whose tier connects.
    pub async fn start(self: &Arc<Self>) -> anyhow::Result<()> {
        let out = self.out.lock().take().ok_or_else(|| anyhow::anyhow!("upstream manager already started"))?;
        self.registry.load().await?;
        let mut bg = self.background.lock();
        bg.push(tokio::spawn(self.fair.clone().run(out)));
        let me = Arc::downgrade(self);
        let every = self.cfg.flush_interval;
        bg.push(tokio::spawn(async move {
            let mut t = tokio::time::interval(every);
            t.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
            loop {
                t.tick().await;
                let Some(m) = me.upgrade() else { return };
                if let Err(e) = m.registry.flush().await {
                    tracing::warn!("host registry flush failed: {e:#}");
                }
            }
        }));
        drop(bg);
        for e in self.registry.all() {
            if self.follow_policy(&e) {
                self.spawn_host(e);
            }
        }
        Ok(())
    }

    fn wanted(&self, host: &Host) -> bool {
        self.filter.lock().as_ref().is_none_or(|f| f(host))
    }

    fn spawn_host(&self, entry: Arc<HostEntry>) {
        if !self.wanted(&entry.host) {
            return;
        }
        let mut tasks = self.tasks.lock();
        if let Some(r) = tasks.get(&entry.host)
            && !r.join.is_finished()
        {
            return;
        }
        let weight = self.cfg.limits.for_tier(entry.tier()).weight;
        let queue = self.fair.host_queue(self.cfg.host_queue_frames, weight);
        let (stop_tx, stop_rx) = watch::channel(false);
        let kick = Arc::new(Notify::new());
        let wake = Arc::new(Notify::new());
        let task = client::HostTask {
            cfg: self.cfg.clone(),
            entry: entry.clone(),
            queue: queue.clone(),
            cursor: self.cursor.clone(),
            stop: stop_rx,
            kick: kick.clone(),
            wake: wake.clone(),
            on_refused: self.on_refused.read().clone(),
            on_connect: self.on_connect.read().clone(),
            flow: self.flow.clone(),
        };
        let join = tokio::spawn(task.run());
        tasks.insert(entry.host.clone(), Running { stop: stop_tx, kick, wake, queue, join });
    }

    fn stop_host(&self, host: &Host) -> Option<JoinHandle<()>> {
        let r = self.tasks.lock().remove(host)?;
        let _ = r.stop.send(true);
        Some(r.join)
    }

    /// Narrows the hosts this manager subscribes to: stops the sockets of
    /// hosts the filter drops (their registry rows flushed, so the acked
    /// cursors are written) and connects the known hosts it adds, after
    /// reloading the registry for rows another node admitted. Returns the
    /// hosts it stopped.
    pub async fn set_filter(self: &Arc<Self>, filter: HostFilter) -> anyhow::Result<Vec<Host>> {
        *self.filter.lock() = Some(filter.clone());
        let dropped: Vec<Host> = self.tasks.lock().keys().filter(|h| !filter(h)).cloned().collect();
        let joins: Vec<_> = dropped.iter().filter_map(|h| self.stop_host(h)).collect();
        for j in joins {
            let _ = j.await;
        }
        if let Err(e) = self.registry.load().await {
            tracing::warn!("reloading the host registry failed: {e:#}");
        }
        if self.out.lock().is_none() {
            for e in self.registry.all() {
                if self.follow_policy(&e) {
                    self.spawn_host(e);
                }
            }
        }
        self.registry.flush().await?;
        Ok(dropped)
    }

    /// Applies every filter `rx` publishes until the manager is dropped.
    pub fn follow_filter(self: &Arc<Self>, mut rx: tokio::sync::watch::Receiver<HostFilter>) {
        let me = Arc::downgrade(self);
        let task = tokio::spawn(async move {
            loop {
                let f = rx.borrow_and_update().clone();
                let Some(m) = me.upgrade() else { return };
                if let Err(e) = m.set_filter(f).await {
                    tracing::warn!("applying the host filter failed: {e:#}");
                }
                drop(m);
                if rx.changed().await.is_err() {
                    return;
                }
            }
        });
        self.background.lock().push(task);
    }

    /// Registers `host` at `tier` unless it's known, and connects it if the
    /// manager is running. Returns whether it was new.
    pub async fn admit(self: &Arc<Self>, host: &Host, tier: Tier) -> anyhow::Result<bool> {
        let (e, new) = self.registry.admit(host, tier).await?;
        if self.follow_policy(&e) && self.out.lock().is_none() {
            self.spawn_host(e);
        }
        Ok(new)
    }

    pub async fn set_tier(self: &Arc<Self>, host: &Host, tier: Tier) -> anyhow::Result<()> {
        let e = self.registry.get(host).ok_or_else(|| anyhow::anyhow!("unknown host {}", host.0))?;
        e.set_tier(tier);
        if !tier.connects() {
            if let Some(j) = self.stop_host(host) {
                let _ = j.await;
            }
        } else if let Some(r) = self.tasks.lock().get(host) {
            r.queue.set_weight(self.cfg.limits.for_tier(tier).weight);
        }
        if tier.connects() && self.out.lock().is_none() {
            self.spawn_host(e);
        }
        self.registry.flush().await?;
        Ok(())
    }

    /// Everything from `host` up to `seq` is durable.
    pub fn ack(&self, host: &Host, seq: i64) {
        if let Some(e) = self.registry.get(host) {
            e.ack(seq);
        }
    }

    pub fn set_account_count(&self, host: &Host, n: u64) {
        if let Some(e) = self.registry.get(host) {
            e.set_account_count(n);
        }
    }

    /// Drops `host`'s socket; it reconnects from its durable cursor.
    pub fn kick(&self, host: &Host) {
        if let Some(r) = self.tasks.lock().get(host) {
            r.kick.notify_one();
        }
    }

    /// [`Self::kick`], only if `host` is still on socket `epoch`: the events
    /// of an older socket that fail can't drop the replay that replaced it.
    pub fn kick_epoch(&self, host: &Host, epoch: u64) {
        if self.registry.get(host).is_some_and(|e| e.epoch() == epoch) {
            self.kick(host);
        }
    }

    /// Cuts `host`'s backoff short, if it's in one.
    pub fn wake(&self, host: &Host) {
        if let Some(r) = self.tasks.lock().get(host) {
            r.wake.notify_one();
        }
    }

    pub fn host(&self, host: &Host) -> Option<HostView> {
        self.registry.get(host).map(|e| e.view())
    }

    pub fn hosts(&self) -> Vec<HostView> {
        let mut v: Vec<HostView> = self.registry.all().iter().map(|e| e.view()).collect();
        v.sort_by(|a, b| a.record.hostname.cmp(&b.record.hostname));
        v
    }

    pub fn queued(&self, host: &Host) -> usize {
        self.tasks.lock().get(host).map_or(0, |r| r.queue.len())
    }

    /// Frames in flight over every host.
    pub fn inflight(&self) -> usize {
        self.flow.events()
    }

    pub fn running(&self) -> usize {
        self.tasks.lock().len()
    }

    /// Closes every socket and writes the registry.
    pub async fn shutdown(&self) -> anyhow::Result<()> {
        let hosts: Vec<Host> = self.tasks.lock().keys().cloned().collect();
        let joins: Vec<_> = hosts.iter().filter_map(|h| self.stop_host(h)).collect();
        for j in joins {
            let _ = j.await;
        }
        for b in self.background.lock().drain(..) {
            b.abort();
        }
        self.registry.flush().await?;
        Ok(())
    }
}
