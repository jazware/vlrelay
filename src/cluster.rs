//! The multi-node layer: who owns which DIDs and hosts, forwarding events
//! between them, and every node merging every node's log.
//!
//! A core node holds one vlpds node lease (`nodes/`) and with it two kinds
//! of shards:
//!
//! - DID shards are vlpds's slots and shards unchanged, assigned by vlpds's
//!   `Cluster` (`assign/`): CAS assignments, fair shares, handoffs, and a
//!   takeover that fences the dead node's log before replaying it. This
//!   node is the `ShardHost`; opening and closing a shard's state is the
//!   node pipeline's [`DidShards`].
//! - Host shards ([`hosts`]) group hostnames the same way under
//!   `assign-hosts/`, judged live by the same lease. The upstream manager
//!   follows [`ClusterNode::host_filter`].
//!
//! Each core node writes its own log (`seq::NodeLog` with the lease's log
//! id, writer byte and validity check) and streams it to its peers
//! ([`follow`]). Every node merges all live logs by seq with vlpds's
//! firehose, so the relay seq, which is the merge key, is the same on every
//! node. A dead node's log is fenced (only that log) and drained from the
//! bucket to the fence.
//!
//! Events reach their DID's owner through [`forward`]: batched over peer
//! mTLS, straight to the stage when this node is the owner, retried against
//! the new owner after a takeover.
//!
//! Edge nodes hold no lease and no shards: they follow every log over mTLS
//! and serve. Replicas do the same from the bucket alone, with read-only
//! credentials and no peer certificate.
//!
//! How `node.rs` plugs in: docs/cluster.md.

pub mod follow;
pub mod forward;
pub mod hosts;
pub mod peer;
#[cfg(test)]
mod tests;

use crate::seq::{LogConfig, NodeLog};
use crate::serve::{Serve, ServeConfig};
use crate::types::Host;
use crate::upstream::HostFilter;
use forward::{DidStage, ForwardConfig, Forwarded, Forwarder, StageError, StageResult};
use hosts::{HostHandler, HostOwnership, HostShards, Member};
use parking_lot::{Mutex, RwLock};
use std::collections::{BTreeMap, HashMap, HashSet};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Weak};
use std::time::{Duration, Instant};
use tokio::sync::{Notify, watch};
use vlpds::cluster::{Cluster, ClusterConfig, Handoff, NodeLease, ShardHost};
use vlpds::nodelog::{LeaseCheck, Span};
use vlpds::slots::{Layout, ShardId};
use vlpds::store::Store;

/// The merger source that holds a joining core's merge below its start
/// floor until it follows its peers.
const JOIN_HOLD: &str = "~join";

#[derive(Clone, Copy, Debug, PartialEq, Eq, clap::ValueEnum)]
pub enum Role {
    /// Lease, DID and host shards, a log; serves.
    Core,
    /// Follows every log over peer mTLS and serves. No lease, no shards.
    Edge,
    /// Follows every log from the bucket (read-only) and serves.
    Replica,
}

#[derive(Clone)]
pub struct ClusterOptions {
    pub node_id: String,
    pub role: Role,
    /// `https://host:port` of this node's peer listener.
    pub addr: String,
    /// Used only when the bucket has no DID layout yet.
    pub did_shards: u32,
    /// Used only when the bucket has no host layout yet.
    pub host_shards: u32,
    pub ttl: Duration,
    pub renew_every: Duration,
    pub skew: Duration,
    /// Host shards are stepped this often (and at once when nudged).
    pub host_step: Duration,
    /// Upstream cursors are checkpointed this often.
    pub checkpoint_every: Duration,
    /// Linger, sizes, in-flight PUTs. The cluster sets the log id, writer,
    /// lease check and floor.
    pub log: LogConfigTemplate,
    pub serve: ServeConfig,
    pub runtime: Option<tokio::runtime::Handle>,
    /// None: no peer transport (a replica, or a lone core node).
    pub tls: Option<Arc<vlpds::peer_tls::PeerTls>>,
    pub peer_connections: usize,
    pub internal_token: String,
    pub forward: ForwardConfig,
    /// Edges and replicas list the leases this often.
    pub poll: Duration,
    /// See `follow`: how far an edge's merge stays behind its last listing.
    pub guard: Duration,
}

/// The parts of `LogConfig` that are the operator's to choose.
#[derive(Clone, Debug)]
pub struct LogConfigTemplate {
    pub linger: Duration,
    pub max_segment_bytes: usize,
    pub max_segment_events: usize,
    pub inflight: usize,
    pub hedge_after: Duration,
    /// See `LogConfig::idle_heartbeat`: replicas need it.
    pub idle_heartbeat: Option<Duration>,
}

impl Default for LogConfigTemplate {
    fn default() -> Self {
        let c = LogConfig::new("");
        LogConfigTemplate {
            linger: c.linger,
            max_segment_bytes: c.max_segment_bytes,
            max_segment_events: c.max_segment_events,
            inflight: c.inflight,
            hedge_after: c.hedge_after,
            idle_heartbeat: Some(Duration::from_secs(1)),
        }
    }
}

impl ClusterOptions {
    pub fn new(node_id: &str, role: Role, addr: &str) -> ClusterOptions {
        let d = ClusterConfig::default();
        ClusterOptions {
            node_id: node_id.to_string(),
            role,
            addr: addr.to_string(),
            did_shards: d.shards,
            host_shards: hosts::DEFAULT_HOST_SHARDS,
            ttl: d.ttl,
            renew_every: d.renew_every,
            skew: d.skew,
            host_step: d.renew_every,
            checkpoint_every: Duration::from_secs(2),
            log: LogConfigTemplate::default(),
            serve: ServeConfig::default(),
            runtime: None,
            tls: None,
            peer_connections: 2,
            internal_token: String::new(),
            forward: ForwardConfig::default(),
            poll: Duration::from_millis(100),
            guard: Duration::from_millis(250),
        }
    }
}

/// Where a DID's owner is.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct NodeAddr {
    pub node_id: String,
    pub addr: String,
}

/// Opening and closing DID shards' state: the node pipeline's part of a
/// shard move.
#[async_trait::async_trait]
pub trait DidShards: Send + Sync + 'static {
    /// Replays each shard's history (the earlier owners' log spans, oldest
    /// first) and starts serving it.
    async fn open(&self, shards: Vec<(ShardId, Vec<Span>)>) -> Vec<(ShardId, anyhow::Result<()>)>;
    /// Called once nothing for these shards is in flight and the log is
    /// drained: checkpoint and close.
    async fn close(&self, shards: Vec<ShardId>) -> Vec<(ShardId, anyhow::Result<()>)>;
    fn on_layout(&self, _layout: Arc<Layout>) {}
}

/// No per-shard state (tests, and a pipeline that hasn't wired state yet).
pub struct NoState;

#[async_trait::async_trait]
impl DidShards for NoState {
    async fn open(&self, shards: Vec<(ShardId, Vec<Span>)>) -> Vec<(ShardId, anyhow::Result<()>)> {
        shards.into_iter().map(|(s, _)| (s, Ok(()))).collect()
    }
    async fn close(&self, shards: Vec<ShardId>) -> Vec<(ShardId, anyhow::Result<()>)> {
        shards.into_iter().map(|s| (s, Ok(()))).collect()
    }
}

/// Counts what's in flight per DID shard, so a close can wait it out.
#[derive(Default)]
struct Gate {
    serving: RwLock<HashSet<ShardId>>,
    inflight: Mutex<HashMap<ShardId, usize>>,
    idle: Notify,
}

struct Pass<'a> {
    gate: &'a Gate,
    shards: Vec<ShardId>,
}

impl Drop for Pass<'_> {
    fn drop(&mut self) {
        let mut m = self.gate.inflight.lock();
        for s in &self.shards {
            if let Some(n) = m.get_mut(s) {
                *n -= 1;
                if *n == 0 {
                    m.remove(s);
                }
            }
        }
        drop(m);
        self.gate.idle.notify_waiters();
    }
}

impl Gate {
    /// The subset of `shards` being served, held open until the pass drops.
    fn enter(&self, shards: &HashSet<ShardId>) -> (Pass<'_>, HashSet<ShardId>) {
        let serving = self.serving.read();
        let mut m = self.inflight.lock();
        let ok: HashSet<ShardId> = shards.iter().filter(|s| serving.contains(s)).copied().collect();
        for s in &ok {
            *m.entry(*s).or_default() += 1;
        }
        (Pass { gate: self, shards: ok.iter().copied().collect() }, ok)
    }

    /// Stops serving `shards` and waits until nothing for them is in flight.
    async fn close(&self, shards: &[ShardId]) {
        {
            let mut s = self.serving.write();
            for id in shards {
                s.remove(id);
            }
        }
        loop {
            let notified = self.idle.notified();
            if !shards.iter().any(|s| self.inflight.lock().contains_key(s)) {
                return;
            }
            notified.await;
        }
    }
}

pub type KeyHook = Arc<dyn Fn(Vec<String>) + Send + Sync>;
pub type LostHook = Box<dyn FnOnce(&str) + Send>;

pub struct ClusterNode {
    pub node_id: String,
    pub role: Role,
    pub store: Store,
    pub cluster: Option<Arc<Cluster>>,
    pub log: Option<Arc<NodeLog>>,
    pub serve: Arc<Serve>,
    pub hosts: Option<Arc<HostShards>>,
    pub followers: Arc<follow::Followers>,
    pub http: Option<vlpds::http::PeerClient>,
    pub forwarder: Option<Arc<Forwarder>>,
    opts: ClusterOptions,
    stage: RwLock<Option<Arc<dyn DidStage>>>,
    did_shards: RwLock<Arc<dyn DidShards>>,
    host_handler: RwLock<Option<Arc<dyn HostHandler>>>,
    key_hook: RwLock<Option<KeyHook>>,
    on_lost: Mutex<Option<LostHook>>,
    gate: Gate,
    /// Our log stream ends (the node is leaving or crashed).
    pub(crate) closed: Arc<AtomicBool>,
    halted: AtomicBool,
    stop: Arc<AtomicBool>,
    host_nudge: Arc<Notify>,
    leaving_peers: Mutex<HashMap<String, Instant>>,
}

impl ClusterNode {
    /// Joins (a core node), starts the log, the merger and the followers.
    /// Nothing is stepped until [`ClusterNode::run`]: set the hooks first.
    pub async fn start(opts: ClusterOptions, store: Store) -> anyhow::Result<Arc<ClusterNode>> {
        let http = opts.tls.clone().map(|t| vlpds::http::PeerClient::new(opts.peer_connections, t));
        let serve_cfg = ServeConfig { write_seq_checkpoints: opts.role == Role::Core, ..opts.serve.clone() };
        let serve = Serve::new(store.clone(), serve_cfg, opts.runtime.clone());
        let (tx, rx) = tokio::sync::mpsc::unbounded_channel();
        // A failed log (lease lapsed, fenced) can never append again, but a
        // renewal sent before the lapse can still land and keep the lease
        // valid: without this the node lived on holding its shards, every
        // event to them failing for 20 s (the chaos minio-pause scenario).
        let log_owner: Arc<std::sync::OnceLock<Weak<ClusterNode>>> = Arc::default();
        let (cluster, log, hosts) = if opts.role == Role::Core {
            let cc = ClusterConfig {
                node_id: opts.node_id.clone(),
                addr: opts.addr.clone(),
                shards: opts.did_shards,
                ttl: opts.ttl,
                renew_every: opts.renew_every,
                skew: opts.skew,
                ..Default::default()
            };
            let cluster = Cluster::join(cc, store.clone()).await?;
            if let Some(http) = &http {
                let c = cluster.clone();
                http.set_registry(Arc::new(move |origin: &str| {
                    let mut v: Vec<String> =
                        c.peers().into_iter().filter(|l| l.addr == origin).map(|l| l.node_id).collect();
                    let me = c.own_lease();
                    if me.addr == origin {
                        v.push(me.node_id);
                    }
                    v
                }));
            }
            let lease = cluster.clone();
            let t = &opts.log;
            let cfg = LogConfig {
                log_id: cluster.log_id.clone(),
                writer: cluster.writer,
                linger: t.linger,
                max_segment_bytes: t.max_segment_bytes,
                max_segment_events: t.max_segment_events,
                inflight: t.inflight,
                hedge_after: t.hedge_after,
                seq_floor: vlpds::nodelog::seq_floor(vlpds::tid::now_micros()).max(serve.firehose.position()),
                lease_ok: Some(Arc::new(move || lease.lease_valid())),
                idle_heartbeat: t.idle_heartbeat,
            };
            let owner = log_owner.clone();
            let on_fatal: crate::seq::OnFatal = Box::new(move |e| {
                if let Some(n) = owner.get().and_then(Weak::upgrade) {
                    n.lost_now(&format!("node log failed: {e}"));
                }
            });
            let log = NodeLog::start(store.clone(), cfg, tx.clone(), Some(on_fatal));
            serve.follow_local(&log);
            let hosts =
                HostShards::new(store.clone(), &opts.node_id, &cluster.log_id, &opts.addr, opts.host_shards).await?;
            (Some(cluster), Some(log), Some(hosts))
        } else {
            (None, None, None)
        };
        let followers = follow::Followers::new(
            serve.firehose.clone(),
            store.clone(),
            tx,
            opts.internal_token.clone(),
            if opts.role == Role::Replica { None } else { http.clone() },
            log.as_ref().map(|l| l.log_id.to_string()),
        );
        // Follow the peers the join found before the merger runs. With only
        // our own log as a source it would settle past the start floor, the
        // peers would then be followed from there, and their events between
        // the two would be in nobody's stream count on this node (stream
        // seqs anchor at the start floor).
        // The join may not list them yet, so the merge is also held just
        // below the floor until the first membership sync (or a lease TTL).
        if let Some(c) = &cluster {
            let live: Vec<follow::LiveLog> = c.peers().iter().map(follow::LiveLog::from).collect();
            followers.sync(&live);
            let hold = Arc::new(std::sync::atomic::AtomicI64::new(serve.firehose.position() - 1));
            serve.firehose.set_source(JOIN_HOLD, Some(vlpds::firehose::Source::Remote(hold)));
            let (fh, wait) = (serve.firehose.clone(), opts.ttl + opts.skew);
            tokio::spawn(async move {
                tokio::time::sleep(wait).await;
                fh.set_source(JOIN_HOLD, None);
            });
        }
        serve.firehose.spawn_merger(rx);
        let node = Arc::new_cyclic(|me: &Weak<ClusterNode>| {
            let forwarder = (opts.role == Role::Core)
                .then(|| Forwarder::start(opts.forward.clone(), Arc::new(NodeRoute(me.clone()))));
            ClusterNode {
                node_id: opts.node_id.clone(),
                role: opts.role,
                store,
                cluster,
                log,
                serve,
                hosts,
                followers,
                http,
                forwarder,
                opts,
                stage: RwLock::new(None),
                did_shards: RwLock::new(Arc::new(NoState)),
                host_handler: RwLock::new(None),
                key_hook: RwLock::new(None),
                on_lost: Mutex::new(None),
                gate: Gate::default(),
                closed: Arc::new(AtomicBool::new(false)),
                halted: AtomicBool::new(false),
                stop: Arc::new(AtomicBool::new(false)),
                host_nudge: Arc::new(Notify::new()),
                leaving_peers: Mutex::new(HashMap::new()),
            }
        });
        let _ = log_owner.set(Arc::downgrade(&node));
        Ok(node)
    }

    pub fn set_stage(&self, s: Arc<dyn DidStage>) {
        *self.stage.write() = Some(s);
    }

    pub fn set_did_shards(&self, s: Arc<dyn DidShards>) {
        *self.did_shards.write() = s;
    }

    pub fn set_host_handler(&self, h: Arc<dyn HostHandler>) {
        if let Some(hs) = &self.hosts {
            hs.set_handler(h.clone());
        }
        *self.host_handler.write() = Some(h);
    }

    /// Called with the DIDs whose signing keys a peer saw change.
    pub fn on_key_change(&self, f: KeyHook) {
        *self.key_hook.write() = Some(f);
    }

    /// Called once if this node must stop (its lease lapsed or its log was
    /// fenced). Without one it logs and goes inert.
    pub fn on_lost(&self, f: LostHook) {
        *self.on_lost.lock() = Some(f);
    }

    /// Starts the control plane: DID shard steps, host shard steps,
    /// checkpoints, the watermark cap (core); lease listing (edge, replica).
    pub fn run(self: &Arc<Self>) {
        match (&self.cluster, &self.log) {
            (Some(cluster), Some(log)) => {
                let host: Arc<dyn ShardHost> = self.clone();
                cluster.spawn(host);
                let (c, wm, stop) = (cluster.clone(), log.wm.clone(), self.stop.clone());
                tokio::spawn(async move {
                    // never announce a watermark beyond our node lease
                    while !stop.load(Ordering::Acquire) {
                        wm.set_lease_expiry(c.lease_expiry_us());
                        tokio::time::sleep(Duration::from_millis(250)).await;
                    }
                });
                let me = Arc::downgrade(self);
                tokio::spawn(async move { host_loop(me).await });
                let me = Arc::downgrade(self);
                let every = self.opts.checkpoint_every;
                tokio::spawn(async move {
                    let mut tick = tokio::time::interval(every);
                    tick.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
                    loop {
                        tick.tick().await;
                        let Some(n) = me.upgrade() else { return };
                        if n.stop.load(Ordering::Acquire) {
                            return;
                        }
                        if let Some(h) = &n.hosts
                            && let Err(e) = h.checkpoint().await
                        {
                            tracing::warn!("host checkpoint failed: {e:#}");
                        }
                    }
                });
            }
            _ => {
                let watch = follow::LeaseWatch::new(self.store.clone(), self.opts.ttl, self.opts.skew);
                follow::spawn_membership(
                    self.followers.clone(),
                    watch,
                    self.opts.poll,
                    self.opts.guard,
                    self.stop.clone(),
                );
            }
        }
    }

    // ---- ownership ----

    pub fn layout(&self) -> Option<Arc<Layout>> {
        self.cluster.as_ref().map(|c| c.layout())
    }

    pub fn did_shard(&self, did: &str) -> Option<ShardId> {
        self.layout().map(|l| l.shard_of(did))
    }

    /// This node serves `did`'s shard right now (open, not closing).
    pub fn owns_did(&self, did: &str) -> bool {
        self.did_shard(did).is_some_and(|s| self.gate.serving.read().contains(&s))
    }

    /// The live owner of `did`'s shard as last read (this node included).
    pub fn owner_of_did(&self, did: &str) -> Option<NodeAddr> {
        let c = self.cluster.as_ref()?;
        let (node_id, addr) = c.owner_of(c.layout().shard_of(did))?;
        Some(NodeAddr { node_id, addr })
    }

    /// The token peers present on internal routes.
    /// Whether this node should read the PLC export: the live core with the
    /// lowest node id. Peers listed as draining or leaving don't count.
    pub fn plc_ingest_leader(&self) -> bool {
        if self.cluster.is_none() || self.halted() || self.stop.load(Ordering::Acquire) {
            return false;
        }
        let members = self.members();
        let me = members.iter().find(|m| m.node_id == self.node_id);
        if me.is_none_or(|m| m.draining) {
            return false;
        }
        members.iter().filter(|m| !m.draining).all(|m| m.node_id >= self.node_id)
    }

    pub fn internal_token(&self) -> &str {
        &self.opts.internal_token
    }

    pub fn owns_host(&self, host: &Host) -> bool {
        self.hosts.as_ref().is_some_and(|h| h.owns(host))
    }

    /// The host shards this node holds, as they change.
    pub fn host_watch(&self) -> Option<watch::Receiver<HostOwnership>> {
        self.hosts.as_ref().map(|h| h.watch())
    }

    /// The same as a filter for `upstream::Manager::follow_filter`.
    pub fn host_filter(&self) -> Option<watch::Receiver<HostFilter>> {
        self.hosts.as_ref().map(|h| h.filter_watch())
    }

    /// The `upstream::CursorSource` that resumes from host checkpoints.
    /// Hand it the manager's registry once the manager exists.
    pub fn cursor_source(&self) -> Option<Arc<hosts::ClusterCursors>> {
        let ck = self.hosts.as_ref()?.checkpoints.clone();
        Some(Arc::new(hosts::ClusterCursors { checkpoints: ck, registry: Default::default() }))
    }

    /// True while this node may ack anything as durable.
    pub fn lease_valid(&self) -> bool {
        self.cluster.as_ref().is_some_and(|c| c.lease_valid()) && !self.halted.load(Ordering::Acquire)
    }

    pub fn lease_check(&self) -> Option<LeaseCheck> {
        let c = self.cluster.clone()?;
        Some(Arc::new(move || c.lease_valid()))
    }

    // ---- forwarding ----

    /// To the DID's owner (here or a peer); resolves once it's durable there,
    /// a duplicate, or rejected.
    pub async fn forward(&self, ev: Forwarded) -> Result<forward::Outcome, forward::ForwardError> {
        match &self.forwarder {
            Some(f) => f.forward(ev).await,
            None => Err(forward::ForwardError::Stopped),
        }
    }

    /// Runs a batch through this node's stage, for the DIDs whose shard it
    /// serves; the rest answer `NotOwner`.
    pub async fn apply_local(&self, batch: Vec<Forwarded>) -> Vec<StageResult> {
        let Some(layout) = self.layout() else {
            return batch.iter().map(|_| Err(StageError::NotOwner)).collect();
        };
        let stage = self.stage.read().clone();
        let Some(stage) = stage.filter(|_| !self.halted.load(Ordering::Acquire)) else {
            return batch.iter().map(|_| Err(StageError::Unavailable("no stage".into()))).collect();
        };
        let shards: Vec<ShardId> = batch.iter().map(|e| layout.shard_of(&e.did)).collect();
        let wanted: HashSet<ShardId> = shards.iter().copied().collect();
        let (_pass, ok) = self.gate.enter(&wanted);
        let mut out: Vec<Option<StageResult>> = vec![None; batch.len()];
        let mut idx = Vec::new();
        let mut mine = Vec::new();
        for (i, (ev, s)) in batch.into_iter().zip(shards).enumerate() {
            if ok.contains(&s) {
                idx.push(i);
                mine.push(ev);
            } else {
                out[i] = Some(Err(StageError::NotOwner));
            }
        }
        if !mine.is_empty() {
            let n = mine.len();
            let rs = stage.apply(mine).await;
            if rs.len() != n {
                for i in idx {
                    out[i] = Some(Err(StageError::Unavailable("stage answered the wrong count".into())));
                }
            } else {
                for (i, r) in idx.into_iter().zip(rs) {
                    out[i] = Some(r);
                }
            }
        }
        out.into_iter().map(|r| r.unwrap_or(Err(StageError::NotOwner))).collect()
    }

    /// Tells every live peer these DIDs' signing keys changed, so their key
    /// caches drop them. Best effort.
    pub async fn invalidate_keys(&self, dids: Vec<String>) {
        let (Some(c), Some(http)) = (&self.cluster, &self.http) else {
            return;
        };
        let body = serde_json::json!({ "dids": dids });
        let sends = c.peers().into_iter().map(|l| {
            let body = body.clone();
            async move {
                let r = http
                    .post(format!("{}{}", l.addr.trim_end_matches('/'), peer::KEYS))
                    .header(peer::TOKEN_HEADER, &self.opts.internal_token)
                    .json(&body)
                    .timeout(Duration::from_secs(2))
                    .send()
                    .await
                    .and_then(|r| r.error_for_status());
                if let Err(e) = r {
                    tracing::warn!(peer = %l.node_id, "key invalidation failed: {e}");
                }
            }
        });
        futures::future::join_all(sends).await;
    }

    pub(crate) fn keys_changed(&self, dids: Vec<String>) {
        if let Some(f) = self.key_hook.read().clone() {
            f(dids);
        }
    }

    // ---- lifecycle ----

    fn members(&self) -> Vec<Member> {
        let Some(c) = &self.cluster else {
            return Vec::new();
        };
        let me = c.own_lease();
        let leaving = self.leaving_peers.lock().clone();
        std::iter::once(me)
            .chain(c.peers())
            .map(|l| Member {
                draining: l.draining || leaving.contains_key(&l.log_id),
                node_id: l.node_id,
                log_id: l.log_id,
                addr: l.addr,
            })
            .collect()
    }

    /// Tells every peer we're leaving, ahead of their next lease listing.
    async fn announce_leaving(&self, log_id: &str) {
        let (Some(c), Some(http)) = (&self.cluster, &self.http) else { return };
        let sends = c.peers().into_iter().map(|l| async move {
            let body = peer::NudgeIn { handoffs: Vec::new(), hosts: false, leaving: Some(log_id.to_string()) };
            let r = http
                .post(format!("{}{}", l.addr.trim_end_matches('/'), peer::NUDGE))
                .header(peer::TOKEN_HEADER, &self.opts.internal_token)
                .json(&body)
                .timeout(Duration::from_secs(1))
                .send()
                .await
                .and_then(|r| r.error_for_status());
            if let Err(e) = r {
                tracing::debug!(peer = %l.node_id, "leaving nudge failed: {e}");
            }
        });
        futures::future::join_all(sends).await;
    }

    /// A peer said it's leaving (by its log id, so a restart of the same
    /// node is a member again).
    pub(crate) fn peer_leaving(&self, log_id: String) {
        let mut m = self.leaving_peers.lock();
        m.retain(|_, at| at.elapsed() < Duration::from_secs(60));
        m.insert(log_id, Instant::now());
    }

    /// One host-shard round now (tests; the loop does this every
    /// `host_step` and on nudges).
    pub async fn step_hosts(&self) -> anyhow::Result<hosts::StepReport> {
        let (Some(c), Some(h)) = (&self.cluster, &self.hosts) else {
            return Ok(Default::default());
        };
        if !c.joined() || !self.lease_valid() {
            return Ok(Default::default());
        }
        let (report, nudges) = h.step(&self.members()).await?;
        self.nudge_hosts(nudges).await;
        Ok(report)
    }

    async fn nudge_hosts(&self, addrs: Vec<String>) {
        let Some(http) = &self.http else { return };
        let sends = addrs.into_iter().map(|addr| async move {
            let r = http
                .post(format!("{}{}", addr.trim_end_matches('/'), peer::NUDGE))
                .header(peer::TOKEN_HEADER, &self.opts.internal_token)
                .json(&peer::NudgeIn { handoffs: Vec::new(), hosts: true, leaving: None })
                .timeout(Duration::from_secs(1))
                .send()
                .await
                .and_then(|r| r.error_for_status());
            if let Err(e) = r {
                tracing::debug!(%addr, "host nudge failed: {e}");
            }
        });
        futures::future::join_all(sends).await;
    }

    pub(crate) fn nudged(&self, handoffs: Vec<Handoff>, hosts: bool) {
        if let Some(c) = &self.cluster
            && (!handoffs.is_empty() || !hosts)
        {
            c.nudge(handoffs);
        }
        if hosts {
            self.host_nudge.notify_one();
        }
    }

    pub fn halted(&self) -> bool {
        self.halted.load(Ordering::Acquire)
    }

    /// A graceful leave: hands host shards over (closing sockets and
    /// checkpointing first), then vlpds's shutdown hands the DID shards
    /// over, quiesces and fences our log, and deletes our lease.
    pub async fn shutdown(self: &Arc<Self>) -> anyhow::Result<()> {
        if let (Some(c), Some(h)) = (&self.cluster, &self.hosts) {
            // peers that still count us as a member hand host shards back
            // to us while we hand them out
            let host: Arc<dyn ShardHost> = self.clone();
            c.announce_drain(&host).await;
            self.announce_leaving(&c.log_id).await;
            let mut members = self.members();
            for m in members.iter_mut().filter(|m| m.node_id == self.node_id) {
                m.draining = true;
            }
            if c.lease_valid() {
                match h.step(&members).await {
                    Ok((_, nudges)) => self.nudge_hosts(nudges).await,
                    Err(e) => tracing::warn!("handing host shards over failed: {e:#}"),
                }
            }
            if let Err(e) = h.checkpoint().await {
                tracing::warn!("final host checkpoint failed: {e:#}");
            }
            c.shutdown(&host).await?;
        }
        self.stop.store(true, Ordering::Release);
        self.closed.store(true, Ordering::Release);
        self.followers.stop_all();
        Ok(())
    }

    /// Tests: stops dead, as a crash would. Nothing is handed over,
    /// checkpointed or fenced; peers take over once the lease lapses.
    pub fn halt(&self) {
        self.halted.store(true, Ordering::Release);
        self.stop.store(true, Ordering::Release);
        self.closed.store(true, Ordering::Release);
        if let Some(c) = &self.cluster {
            c.halt();
        }
        if let Some(l) = &self.log {
            l.halt();
        }
        if let Some(h) = &self.hosts {
            h.halt();
        }
        self.gate.serving.write().clear();
        self.followers.stop_all();
        self.serve.firehose.freeze();
    }

    fn lost_now(&self, why: &str) {
        if self.halted.swap(true, Ordering::AcqRel) {
            return;
        }
        tracing::error!("cluster node lost: {why}");
        self.stop.store(true, Ordering::Release);
        self.closed.store(true, Ordering::Release);
        self.gate.serving.write().clear();
        if let Some(f) = self.on_lost.lock().take() {
            f(why);
        }
    }

    /// Fences the logs we follow whose node is no longer live (only those):
    /// their followers then drain them to the fence and retire.
    fn fence_dead(&self, live: &HashSet<String>) {
        let Some(c) = self.cluster.clone() else {
            return;
        };
        let fenced = c.fenced_logs();
        let dead: Vec<String> =
            self.followers.followed().into_iter().filter(|l| !live.contains(l) && !fenced.contains_key(l)).collect();
        if dead.is_empty() {
            return;
        }
        tokio::spawn(async move {
            for log_id in dead {
                if let Err(e) = c.fence(&log_id).await {
                    tracing::warn!(%log_id, "fencing a dead node's log failed: {e:#}");
                }
            }
        });
    }
}

async fn host_loop(me: Weak<ClusterNode>) {
    let every = match me.upgrade() {
        Some(n) => n.opts.host_step,
        None => return,
    };
    loop {
        let Some(n) = me.upgrade() else { return };
        if n.stop.load(Ordering::Acquire) {
            return;
        }
        if let Err(e) = n.step_hosts().await {
            tracing::warn!("host shard step failed: {e:#}");
        }
        let nudge = n.host_nudge.clone();
        drop(n);
        let _ = tokio::time::timeout(every, nudge.notified()).await;
    }
}

struct NodeRoute(Weak<ClusterNode>);

#[async_trait::async_trait]
impl forward::Route for NodeRoute {
    fn owner(&self, did: &str) -> Option<Option<String>> {
        let n = self.0.upgrade()?;
        if n.owns_did(did) {
            return Some(None);
        }
        let o = n.owner_of_did(did)?;
        Some((o.node_id != n.node_id).then_some(o.addr))
    }

    async fn local(&self, batch: Vec<Forwarded>) -> Vec<StageResult> {
        match self.0.upgrade() {
            Some(n) => n.apply_local(batch).await,
            None => batch.iter().map(|_| Err(StageError::Unavailable("stopped".into()))).collect(),
        }
    }

    async fn remote(&self, addr: &str, batch: Vec<Forwarded>) -> anyhow::Result<Vec<StageResult>> {
        let n = self.0.upgrade().ok_or_else(|| anyhow::anyhow!("stopped"))?;
        let http = n.http.as_ref().ok_or_else(|| anyhow::anyhow!("no peer transport"))?;
        let r = http
            .post(format!("{}{}", addr.trim_end_matches('/'), peer::FORWARD))
            .header(peer::TOKEN_HEADER, &n.opts.internal_token)
            .body(forward::encode_batch(&batch))
            .send()
            .await?
            .error_for_status()?;
        forward::decode_results(r.bytes().await?)
    }
}

#[async_trait::async_trait]
impl ShardHost for ClusterNode {
    fn next_ordinal(&self) -> u64 {
        self.log.as_ref().map_or(0, |l| l.next_ordinal.load(Ordering::Acquire))
    }

    fn durable_end(&self) -> u64 {
        self.log.as_ref().map_or(0, |l| l.durable_ordinal.load(Ordering::Acquire).wrapping_add(1))
    }

    fn seq_high(&self) -> i64 {
        self.log.as_ref().map_or(0, |l| l.wm.get())
    }

    async fn wait_seq_floor(&self, seq: i64) {
        // seqs are clock-based: wait until ours pass the previous owner's,
        // bounded so a wildly wrong clock costs order, not availability
        let deadline = Instant::now() + Duration::from_secs(30);
        while vlpds::nodelog::seq_floor(vlpds::tid::now_micros()) <= seq {
            if Instant::now() > deadline {
                tracing::error!(seq, "our clock is still behind a shard's previous owner");
                return;
            }
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
    }

    async fn open_many(&self, shards: Vec<(ShardId, u64, Vec<Span>)>) -> Vec<(ShardId, anyhow::Result<()>)> {
        let handler = self.did_shards.read().clone();
        let res = handler.open(shards.into_iter().map(|(s, _, h)| (s, h)).collect()).await;
        let mut serving = self.gate.serving.write();
        for (s, r) in &res {
            if r.is_ok() && !self.halted.load(Ordering::Acquire) {
                serving.insert(*s);
            }
        }
        res
    }

    async fn close_many(&self, shards: Vec<ShardId>) -> Vec<(ShardId, anyhow::Result<()>)> {
        self.gate.close(&shards).await;
        if let Some(log) = &self.log
            && let Err(e) = log.append(Vec::new()).await
        {
            let e = anyhow::anyhow!("draining our log: {e}");
            return shards.into_iter().map(|s| (s, Err(anyhow::anyhow!("{e:#}")))).collect();
        }
        let handler = self.did_shards.read().clone();
        handler.close(shards).await
    }

    async fn quiesce(&self) -> bool {
        let Some(log) = &self.log else { return true };
        let deadline = Instant::now() + Duration::from_secs(10);
        while !log.idle() {
            if Instant::now() > deadline {
                return false;
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
        true
    }

    fn lost(&self) {
        if self.cluster.as_ref().is_some_and(|c| c.halted()) {
            return;
        }
        self.lost_now("node lease lost");
    }

    fn on_membership(&self) {
        let Some(c) = &self.cluster else { return };
        let peers = c.peers();
        let live: Vec<follow::LiveLog> = peers.iter().map(follow::LiveLog::from).collect();
        self.followers.sync(&live);
        self.serve.firehose.set_source(JOIN_HOLD, None);
        let ids: HashSet<String> = peers.iter().map(|l| l.log_id.clone()).collect();
        self.fence_dead(&ids);
    }

    async fn nudge(&self, nudges: Vec<(String, Vec<Handoff>)>) {
        let Some(http) = &self.http else { return };
        let sends = nudges.into_iter().map(|(addr, handoffs)| async move {
            let r = http
                .post(format!("{}{}", addr.trim_end_matches('/'), peer::NUDGE))
                .header(peer::TOKEN_HEADER, &self.opts.internal_token)
                .json(&peer::NudgeIn { handoffs, hosts: false, leaving: None })
                .timeout(Duration::from_secs(1))
                .send()
                .await
                .and_then(|r| r.error_for_status());
            if let Err(e) = r {
                tracing::debug!(%addr, "nudge failed: {e}");
            }
        });
        futures::future::join_all(sends).await;
    }

    async fn greet(&self, peers: Vec<NodeLease>) -> Vec<Option<i64>> {
        let Some(http) = &self.http else {
            return peers.iter().map(|_| None).collect();
        };
        let sends = peers.into_iter().map(|l| async move {
            let r = http
                .post(format!("{}{}", l.addr.trim_end_matches('/'), peer::HELLO))
                .header(peer::TOKEN_HEADER, &self.opts.internal_token)
                .json(&peer::HelloIn { node_id: self.node_id.clone() })
                .timeout(Duration::from_secs(2))
                .send()
                .await
                .and_then(|r| r.error_for_status());
            match r {
                Ok(r) => r.json::<peer::HelloOut>().await.ok().and_then(|h| h.floor),
                Err(e) => {
                    tracing::debug!(peer = %l.node_id, "hello failed: {e}");
                    None
                }
            }
        });
        futures::future::join_all(sends).await
    }

    fn leaving(&self) {
        self.serve.firehose.freeze();
        self.closed.store(true, Ordering::Release);
    }

    fn follow_floors(&self) -> BTreeMap<String, i64> {
        self.followers.floors()
    }

    async fn refused(&self, addr: &str) -> bool {
        let Some(authority) = reqwest::Url::parse(addr)
            .ok()
            .and_then(|u| Some(format!("{}:{}", u.host_str()?, u.port_or_known_default()?)))
        else {
            return false;
        };
        match tokio::time::timeout(Duration::from_millis(500), tokio::net::TcpStream::connect(&authority)).await {
            Ok(Err(e)) => e.kind() == std::io::ErrorKind::ConnectionRefused,
            _ => false,
        }
    }

    fn on_layout(&self, layout: Arc<Layout>) {
        self.did_shards.read().on_layout(layout);
    }
}
