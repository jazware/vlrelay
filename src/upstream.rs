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

mod client;
pub mod crawl;
pub mod fair;
pub mod frame;
pub mod host;
pub mod limits;

pub use crawl::{CrawlPolicy, Crawler, DomainAction, DomainRule};
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
}

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
        let m = Manager {
            cfg: Arc::new(cfg),
            registry,
            cursor,
            fair,
            out: Mutex::new(Some(tx)),
            tasks: Mutex::new(HashMap::new()),
            background: Mutex::new(Vec::new()),
        };
        (Arc::new(m), rx)
    }

    pub fn config(&self) -> &UpstreamConfig {
        &self.cfg
    }

    pub fn registry(&self) -> &Arc<Registry> {
        &self.registry
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
            if e.tier().connects() {
                self.spawn_host(e);
            }
        }
        Ok(())
    }

    fn spawn_host(&self, entry: Arc<HostEntry>) {
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
        };
        let join = tokio::spawn(task.run());
        tasks.insert(entry.host.clone(), Running { stop: stop_tx, kick, wake, queue, join });
    }

    fn stop_host(&self, host: &Host) -> Option<JoinHandle<()>> {
        let r = self.tasks.lock().remove(host)?;
        let _ = r.stop.send(true);
        Some(r.join)
    }

    /// Registers `host` at `tier` unless it's known, and connects it if the
    /// manager is running. Returns whether it was new.
    pub async fn admit(self: &Arc<Self>, host: &Host, tier: Tier) -> anyhow::Result<bool> {
        let (e, new) = self.registry.admit(host, tier).await?;
        if self.out.lock().is_none() && e.tier().connects() {
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
