//! The node pipeline on the cluster library (docs/cluster.md): a core node
//! is the same pipeline as a single node, with four seams moved.
//!
//! - Upstream: the manager subscribes only to the hosts in this node's host
//!   shards (`host_filter`), resumes from `hostck/` (`ClusterCursors`), and
//!   its registry rows live in the bucket ([`BucketHosts`]), one object per
//!   host shard, so a host admitted on any node exists for every node.
//! - Host owner to DID owner: the lane's [`DidOwner`] is [`Forwarding`],
//!   which hands each checked event to `ClusterNode::forward`'s lanes (in
//!   order per DID) and answers with the outcome once the DID owner has it
//!   durable, a duplicate or rejected.
//! - DID owner: [`Stage`] runs forwarded batches through the same
//!   `LocalOwner` as a single node: state apply, append to this node's log,
//!   commit once durable.
//! - DID shards: [`Shards`] opens a shard's state and replays every earlier
//!   owner's log span, oldest first, then checkpoints and closes it on the
//!   way out.
//!
//! Restart dedupe. A single node drops the events an upstream replays past
//! its durable cursor by reading its own log's tail. In a cluster the host
//! owner can't read the DID owner's log, so the DID owner does it: it keeps
//! the (host, upstream seq) of every `#identity`, `#account` and `#sync` it
//! appended ([`Recent`]) until the host's checkpointed cursor (`hostck/`)
//! has passed it, and answers a second copy as a duplicate. Commits need
//! none of this: the chain catches them by rev. The set survives a crash
//! because the DID owner's applied marker for its own log never passes an
//! entry still in it, so the shard's next owner replays those entries and
//! rebuilds the set from them.

use super::adapters::{LogReplay, VerifyChain};
use super::metrics::{self, Ttf};
use super::{Checked, CheckedKind, DidOwner, LocalOwner, Node, NodeConfig, Rejection, State, Submitted};
use crate::cluster::forward::{DidStage, Forwarded, Outcome, StageError, StageResult};
use crate::cluster::hosts::HostHandler;
use crate::cluster::{ClusterNode, ClusterOptions, DidShards, Role};
use crate::event::SeqSpan;
use crate::seq::NodeLog;
use crate::state::{self, HostPage, HostRecord, ReplaySource, StateDelta, StateStore};
use crate::types::Host;
use crate::upstream::{self, HostFilter, Manager, UpstreamConfig};
use crate::verify::{Verified, VerifiedKind};
use bytes::{Buf, BufMut, Bytes};
use object_store::path::Path;
use object_store::{ObjectStoreExt, PutMode, PutOptions, PutPayload};
use parking_lot::Mutex;
use serde::{Deserialize, Serialize};
use std::collections::{BTreeMap, HashMap};
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{Duration, Instant};
use vlpds::cid::Cid;
use vlpds::nodelog::Span;
use vlpds::slots::{Layout, ShardId};
use vlpds::store::Store;
use vlpds::tid::Tid;

static CLUSTER_DEDUPE: std::sync::LazyLock<prometheus::IntGauge> = std::sync::LazyLock::new(|| {
    prometheus::register_int_gauge!(
        "vlrelay_cluster_dedupe_entries",
        "(host, upstream seq) pairs the DID owner holds until the host's checkpoint passes them"
    )
    .unwrap()
});
static CLUSTER_SHARD_OPEN: std::sync::LazyLock<prometheus::Histogram> = std::sync::LazyLock::new(|| {
    prometheus::register_histogram!(
        "vlrelay_cluster_shard_open_seconds",
        "Opening a batch of DID shards: state open plus replay of earlier owners' log spans",
        prometheus::exponential_buckets(0.01, 2.0, 14).unwrap()
    )
    .unwrap()
});

/// What `main` gives a cluster node.
#[derive(Clone)]
pub struct ClusterSetup {
    pub role: Role,
    /// `https://host:port` of the peer listener.
    pub advertise: String,
    pub tls: Option<Arc<vlpds::peer_tls::PeerTls>>,
    pub internal_token: String,
    pub ttl: Duration,
    /// Used only when the bucket has no host layout yet.
    pub host_shards: u32,
    pub checkpoint_every: Duration,
    /// The policy engine's live-node count, pointed at this node's cluster
    /// once it has joined.
    pub cores: Arc<LiveCores>,
}

/// Live core nodes, as the policy's cluster-wide budgets divide them: 1
/// until the cluster is up.
#[derive(Default)]
pub struct LiveCores(std::sync::OnceLock<std::sync::Weak<ClusterNode>>);

impl crate::policy::LiveNodes for LiveCores {
    fn live_nodes(&self) -> usize {
        let Some(n) = self.0.get().and_then(|w| w.upgrade()) else { return 1 };
        n.cluster.as_ref().map_or(1, |c| 1 + c.peers().iter().filter(|l| !l.draining).count())
    }
}

impl ClusterSetup {
    pub fn options(&self, cfg: &NodeConfig) -> ClusterOptions {
        let mut o = ClusterOptions::new(&cfg.node_id, self.role, &self.advertise);
        o.did_shards = cfg.did_shards;
        o.host_shards = self.host_shards;
        o.ttl = self.ttl;
        o.renew_every = (self.ttl / 5).max(Duration::from_millis(100));
        o.skew = (self.ttl / 5).max(Duration::from_millis(100));
        o.host_step = o.renew_every;
        o.checkpoint_every = self.checkpoint_every;
        o.log.linger = cfg.linger;
        o.serve = cfg.serve_config();
        o.runtime = Some(vlpds::firehose::runtime(cfg.serve_threads));
        o.tls = self.tls.clone();
        o.internal_token = self.internal_token.clone();
        o
    }
}

/// An edge or a replica: no pipeline, it follows every core node's log and
/// serves the merged firehose.
pub async fn start_follower(store: Store, cfg: &NodeConfig, setup: &ClusterSetup) -> anyhow::Result<Arc<ClusterNode>> {
    anyhow::ensure!(setup.role != Role::Core, "a core node starts with Node::start_cluster");
    let node = ClusterNode::start(setup.options(cfg), store).await?;
    node.on_lost(Box::new(|why| vlpds::lifecycle::fail_stop(5, &format!("cluster: {why}"))));
    node.run();
    Ok(node)
}

/// The cluster half of a core node.
pub struct Glue {
    pub cluster: Arc<ClusterNode>,
    pub hosts: Arc<BucketHosts>,
    pub recent: Arc<Recent>,
    inflight: Inflight,
    state: Arc<State>,
    log: Arc<NodeLog>,
    local: Arc<LocalOwner>,
    /// One past the highest own-log ordinal whose state is committed.
    committed: AtomicU64,
    /// `committed` as the previous checkpoint tick read it.
    committed_prev: AtomicU64,
    hostck: HostCks,
    dedupe: DedupeStore,
    markers: Mutex<HashMap<ShardId, u64>>,
    /// Opened at their current epoch with every earlier span durable in
    /// their state.
    clean: Mutex<std::collections::HashSet<ShardId>>,
    /// The node's admin, for the peer admin RPC (`node::peer_admin`).
    pub admin: super::peer_admin::Slot,
}

impl Node {
    /// A core cluster node: joins, opens its DID shards as they're
    /// assigned, subscribes to its host shards. The firehose and every API
    /// are ready on return; `peer` must already be bound (peers greet it
    /// during the join).
    pub async fn start_cluster(
        store: Store,
        cfg: NodeConfig,
        setup: &ClusterSetup,
        peer: tokio::net::TcpListener,
    ) -> anyhow::Result<Arc<Node>> {
        anyhow::ensure!(setup.role == Role::Core, "edges and replicas start with start_follower");
        let cluster = ClusterNode::start(setup.options(&cfg), store.clone()).await?;
        let log = cluster.log.clone().expect("a core node has a log");
        let layout = cluster.layout().expect("a core node has a layout");
        let identity = Arc::new(crate::identity::IdentityCache::new(
            crate::identity::HttpFetch::new(&cfg.plc_url, cfg.dev_mode),
            cfg.identity.clone(),
        ));
        let state = Arc::new(StateStore::new(
            store.clone(),
            layout.shards.clone(),
            VerifyChain,
            Arc::new(super::adapters::CacheIdentity(identity.clone())),
            state::ApplyConfig::default(),
        ));
        let archive = cfg.policy.as_ref().map(|p| crate::archive::wiring::install(&state, p.0.clone(), identity.clone()));
        let seeds = Arc::new(crate::plc_seed::LocalSeeds::new(state.clone(), cfg.identity.ttl));
        identity.set_seeder(Arc::new(crate::plc_seed::peer::ClusterSeeder {
            seeds: seeds.clone(),
            cluster: Arc::downgrade(&cluster),
        }));
        let seed_owner = Arc::new(crate::plc_seed::peer::Owner {
            seeds,
            cache: identity.clone(),
            cluster: Arc::downgrade(&cluster),
            token: cluster.internal_token().to_string(),
        });
        let host_layout = cluster.hosts.as_ref().expect("a core node has host shards").layout();
        let hosts = Arc::new(BucketHosts::new(store.clone(), host_layout));

        let (explicit, cli_hosts) = super::cli_hosts(&cfg)?;
        let mut ucfg = UpstreamConfig::new(cfg.dev_mode);
        ucfg.endpoint = super::endpoint_fn(cfg.dev_mode, explicit);
        ucfg.limits = cfg.upstream_limits.clone();
        let cursors = cluster.cursor_source().expect("a core node has host checkpoints");
        let (manager, rx) = Manager::new(ucfg, hosts.clone(), Some(cursors.clone() as Arc<dyn upstream::CursorSource>));
        let _ = cursors.registry.set(manager.registry().clone());
        let crawler = upstream::Crawler::new(manager.clone(), upstream::CrawlPolicy::default());
        let _ = setup.cores.0.set(Arc::downgrade(&cluster));
        let hooks = cfg.policy.as_ref().map(|p| {
            let raw: Arc<dyn state::HostStore> = hosts.clone();
            super::policy::PolicyHooks::with_hosts(p.0.clone(), state.clone(), raw, cfg.dev_mode)
        });
        if let Some(h) = &hooks {
            h.install(&manager, &crawler, &identity);
            h.load().await?;
            if let Some((_, g)) = &archive {
                let _ = g.hooks.set(h.clone());
            }
        }
        let cli_tier = cfg.cli_host_tier;

        let ttf = Arc::new(Ttf::default());
        let local = LocalOwner::start(state.clone(), log.clone(), ttf.clone(), cfg.did_shards as usize);
        let glue = Arc::new(Glue {
            cluster: cluster.clone(),
            hosts: hosts.clone(),
            recent: Arc::new(Recent::default()),
            inflight: Inflight::default(),
            state: state.clone(),
            log: log.clone(),
            local: local.clone(),
            committed: AtomicU64::new(0),
            committed_prev: AtomicU64::new(0),
            hostck: HostCks::new(store.clone()),
            dedupe: DedupeStore::new(store.clone(), log.log_id.to_string()),
            markers: Mutex::new(HashMap::new()),
            clean: Mutex::new(Default::default()),
            admin: Default::default(),
        });
        let node = Node::assemble(
            cfg,
            store,
            manager.clone(),
            crawler,
            state,
            identity.clone(),
            log.clone(),
            cluster.serve.clone(),
            local,
            Arc::new(Forwarding(cluster.clone())),
            ttf,
            HashMap::new(),
            Default::default(),
            hooks.clone(),
            Some(glue.clone()),
            rx,
        )?;

        cluster.set_stage(Arc::new(Stage(glue.clone())));
        cluster.set_did_shards(Arc::new(Shards(glue.clone())));
        cluster.set_host_handler(Arc::new(Upstreams { node: Arc::downgrade(&node) }));
        let id = identity.clone();
        cluster.on_key_change(Arc::new(move |dids: Vec<String>| {
            for d in &dids {
                id.invalidate(d);
            }
        }));
        cluster.on_lost(Box::new(|why| vlpds::lifecycle::fail_stop(5, &format!("cluster: {why}"))));
        crate::cluster::peer::spawn_listener_with(
            &cluster,
            peer,
            crate::archive::wiring::peer_reads(node.state.clone(), cluster.internal_token().to_string())
                .merge(crate::plc_seed::peer::router(seed_owner.clone()))
                .merge(super::peer_admin::core_router(glue.admin.clone(), cluster.internal_token().to_string())),
        )?;
        if let Some(pc) = &node.cfg.plc_export {
            let sink = Arc::new(crate::plc_seed::peer::ForwardSink::new(seed_owner));
            let ing = crate::plc_seed::ingest::Ingester::new(pc.clone(), node.store.clone(), sink);
            let _ = node.plc_ingest.set(ing.clone());
            let c = Arc::downgrade(&cluster);
            tokio::spawn(ing.supervise(Arc::new(move || c.upgrade().is_some_and(|c| c.plc_ingest_leader()))));
        }
        cluster.serve.spawn_retention(log.log_id.to_string());
        cluster.run();

        manager.follow_filter(cluster.host_filter().expect("a core node has host shards"));
        manager.start().await?;
        for h in cli_hosts {
            manager.admit(&h, cli_tier).await?;
        }
        if let Some(h) = &hooks {
            h.spawn();
        }
        tokio::spawn(glue.clone().ticks(Arc::downgrade(&node)));
        tracing::info!(node = %cluster.node_id, log = %log.log_id, "cluster core node started");
        Ok(node)
    }
}

impl Glue {
    /// The admin's cluster view: members from the leases, owners from the
    /// assignments as last read. Rates and resources are this node's only;
    /// `admin::fleet::fill_cluster` fills in every member's.
    pub fn view(&self, node: &Node) -> crate::admin::ClusterView {
        let c = &self.cluster;
        let host_shards = c.hosts.as_ref().map(|h| h.owners()).unwrap_or_default();
        let did_shards: Vec<Option<String>> = match (&c.cluster, c.layout()) {
            (Some(cl), Some(l)) => l.ids().into_iter().map(|s| cl.owner_of(s).map(|(n, _)| n)).collect(),
            _ => Vec::new(),
        };
        let count = |v: &[Option<String>], id: &str| v.iter().filter(|o| o.as_deref() == Some(id)).count() as u32;
        let mut leases = Vec::new();
        if let Some(cl) = &c.cluster {
            leases.push(cl.own_lease());
            leases.extend(cl.peers());
        }
        let last = node.dash.lock().history.back().cloned();
        let nodes = leases
            .into_iter()
            .map(|l| {
                let me = l.node_id == c.node_id;
                crate::admin::NodeView {
                    host_shards: count(&host_shards, &l.node_id),
                    did_shards: count(&did_shards, &l.node_id),
                    hosts: if me { node.manager.running() as u32 } else { 0 },
                    consumers: if me { vlpds::metrics::FIREHOSE_SUBSCRIBERS.get().max(0) as u32 } else { 0 },
                    events_in_per_sec: if me { last.as_ref().map_or(0.0, |s| s.events_in) } else { 0.0 },
                    events_out_per_sec: if me { last.as_ref().map_or(0.0, |s| s.events_out) } else { 0.0 },
                    log_durability_lag_ms: if me { last.as_ref().map_or(0.0, |s| s.durable_lag_ms) } else { 0.0 },
                    reachable: !l.draining,
                    lease_valid: if me { c.lease_valid() } else { !l.draining },
                    lease_expires_ms: l.expires_ms as i64,
                    version: env!("CARGO_PKG_VERSION").to_string(),
                    rev: l.rev,
                    id: l.node_id,
                    addr: l.addr,
                    cpu: 0.0,
                    mem_bytes: 0,
                    role: "core".into(),
                    stale: false,
                    error: None,
                    reported_ms: 0,
                    bytes_out_per_sec: 0.0,
                    stream_seq: 0,
                }
            })
            .collect();
        crate::admin::ClusterView {
            nodes,
            host_shards,
            did_shards,
            last_seq: self.log.last_durable_seq.load(Ordering::Acquire),
        }
    }

    /// Every checkpoint interval: prune the dedupe set by the hosts'
    /// checkpointed cursors, write the DID shards' applied markers for our
    /// log, flush host counters, and pick up hosts other nodes admitted.
    async fn ticks(self: Arc<Self>, node: std::sync::Weak<Node>) {
        let every = node.upgrade().map_or(Duration::from_secs(2), |n| n.cfg.checkpoint_interval);
        let mut tick = tokio::time::interval(every);
        tick.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
        loop {
            tick.tick().await;
            let Some(node) = node.upgrade() else { return };
            if self.cluster.halted() {
                return;
            }
            match self.hostck.refresh().await {
                Ok(c) => self.recent.prune(&c, Duration::from_secs(900)),
                Err(e) => tracing::warn!("reading host checkpoints failed: {e:#}"),
            }
            let prev = self.committed_prev.swap(self.committed.load(Ordering::Acquire), Ordering::AcqRel);
            for s in self.state.shards() {
                if let Err(e) = self.checkpoint_shard(s.id, prev).await {
                    tracing::warn!(shard = %s.id, "checkpoint failed: {e:#}");
                }
                if let Err(e) = self.persist_dedupe(s.id).await {
                    tracing::warn!(shard = %s.id, "dedupe set write failed: {e:#}");
                }
            }
            if let Err(e) = self.state.flush_host_counts(&*self.hosts).await {
                tracing::warn!("host counts flush failed: {e:#}");
            }
            // reloads the registry, so a host another node admitted into one
            // of our host shards connects here
            if let Some(f) = self.cluster.host_filter() {
                let f = f.borrow().clone();
                if let Err(e) = node.manager.set_filter(f).await {
                    tracing::warn!("host registry refresh failed: {e:#}");
                }
            }
            metrics::ACK_PENDING.set(node.acks.snapshot().pending as i64);
            CLUSTER_DEDUPE.set(self.recent.len() as i64);
        }
    }

    /// The applied marker for our log may go up to what's committed, but
    /// never past an entry the dedupe set still needs.
    fn marker(&self, shard: ShardId, committed: u64) -> Option<u64> {
        let mut m = committed.checked_sub(1);
        if let Some(o) = self.recent.min_ordinal(shard.0) {
            m = m.and_then(|m| o.checked_sub(1).map(|o| m.min(o)));
        }
        m
    }

    /// Writes the shard's inherited dedupe entries (those our marker doesn't
    /// protect) when they changed. Our own appends need no copy: the marker
    /// for our log stays below them, so the next owner replays them.
    async fn persist_dedupe(&self, id: ShardId) -> anyhow::Result<()> {
        if !self.cluster.lease_valid() {
            return Ok(());
        }
        self.dedupe.write(id, self.recent.inherited(id.0)).await
    }

    async fn checkpoint_shard(&self, id: ShardId, committed: u64) -> anyhow::Result<()> {
        let Some(m) = self.marker(id, committed) else {
            return Ok(());
        };
        if self.markers.lock().get(&id) == Some(&m) {
            return Ok(());
        }
        let Some(s) = self.state.shard(id) else {
            return Ok(());
        };
        s.checkpoint(&self.log.log_id, m).await?;
        self.markers.lock().insert(id, m);
        Ok(())
    }
}

// ---- host owner -> DID owner ----

/// The lane's DID owner on a cluster node: the forwarder.
pub struct Forwarding(pub Arc<ClusterNode>);

#[async_trait::async_trait]
impl DidOwner for Forwarding {
    async fn submit(&self, c: Checked) -> Submitted {
        let Some(f) = &self.0.forwarder else {
            return Submitted::Rejected(Rejection { reason: "no_forwarder", detail: "not a core node".into() });
        };
        let meta = encode_meta(&c);
        let ev = Forwarded { did: c.did, host: c.host, upstream_seq: c.upstream_seq, meta, frame: c.frame };
        Submitted::Forwarded(f.submit(ev).await)
    }
}

impl Node {
    /// The host owner's end of a forward: count it and move the host's
    /// cursor, or, if the DID owner never answered, hold the cursor and
    /// have the host send it again.
    pub(super) async fn forwarded(
        self: Arc<Self>,
        rx: tokio::sync::oneshot::Receiver<Result<Outcome, crate::cluster::forward::ForwardError>>,
        host: Host,
        did: String,
        useq: i64,
        kind: &'static str,
    ) {
        match rx.await {
            Ok(Ok(Outcome::Appended(_))) => {
                metrics::ACCEPTED_BY_KIND.inc(kind);
                self.finish(&host, useq, None);
            }
            Ok(Ok(Outcome::Duplicate)) => {
                metrics::EVENTS_DUPLICATE.with_label_values(&["owner"]).inc();
                self.finish(&host, useq, None);
            }
            Ok(Ok(Outcome::Rejected(m))) => {
                let (reason, detail) = m.split_once(": ").unwrap_or((m.as_str(), ""));
                let r = Rejection { reason: static_reason(reason), detail: detail.to_string() };
                self.reject(&host, &did, useq, r);
                self.finish(&host, useq, None);
            }
            Ok(Err(e)) => {
                tracing::warn!(host = %host.0, did, useq, "forward failed, replaying from the host: {e}");
                self.acks.fail(&host, useq);
                self.manager.kick(&host);
            }
            Err(_) => self.acks.fail(&host, useq),
        }
    }
}

/// Reasons travel as text; metrics labels want the fixed set.
fn static_reason(r: &str) -> &'static str {
    const KNOWN: &[&str] = &[
        "stale",
        "wrong_host",
        "inactive",
        "desynchronized",
        "rev_not_newer",
        "prev_data_mismatch",
        "chain",
        "rate_limited",
        "new_account_deferred",
        "no_identity",
        "bad_cid",
        "not_owner",
        "identity_unavailable",
        "store",
        "bad_meta",
    ];
    KNOWN.iter().find(|k| **k == r).copied().unwrap_or("owner_rejected")
}

/// What the DID owner needs from the host owner's parse: the kind, the
/// verified chain fields, where the frame's seq is, and whether this was
/// the first copy the host owner saw.
///
/// kind u8 | first u8 | span u32 u32 | commit/sync: vkind u8, rev u64,
/// commit cid, data cid, prev_data (u8 flags: 1 = a cid follows, 2 = the
/// repo's first commit; + cid) | account: active u8,
/// status (u16 len + bytes, 0xffff = none)
fn encode_meta(c: &Checked) -> Bytes {
    let mut b = Vec::with_capacity(120);
    let tag = match &c.kind {
        CheckedKind::Commit(_) => 0u8,
        CheckedKind::Sync(_) => 1,
        CheckedKind::Identity => 2,
        CheckedKind::Account { .. } => 3,
    };
    b.put_u8(tag);
    b.put_u8(c.first_sighting as u8);
    b.put_u32(c.span.start);
    b.put_u32(c.span.end);
    let cid = |b: &mut Vec<u8>, c: &Cid| {
        b.put_u8(c.codec);
        b.put_slice(&c.digest);
    };
    match &c.kind {
        CheckedKind::Commit(v) | CheckedKind::Sync(v) => {
            b.put_u8(matches!(v.kind, VerifiedKind::Sync) as u8);
            b.put_u64(v.rev.0);
            cid(&mut b, &v.commit);
            cid(&mut b, &v.data);
            let created = (v.created as u8) << 1;
            match &v.prev_data {
                Some(p) => {
                    b.put_u8(1 | created);
                    cid(&mut b, p);
                }
                None => b.put_u8(created),
            }
        }
        CheckedKind::Identity => {}
        CheckedKind::Account { active, status } => {
            b.put_u8(*active as u8);
            match status {
                Some(s) => {
                    let s = &s.as_bytes()[..s.len().min(1024)];
                    b.put_u16(s.len() as u16);
                    b.put_slice(s);
                }
                None => b.put_u16(u16::MAX),
            }
        }
    }
    b.into()
}

struct Meta {
    kind: CheckedKind,
    first_sighting: bool,
    span: SeqSpan,
}

fn decode_meta(did: &str, mut r: Bytes) -> anyhow::Result<Meta> {
    anyhow::ensure!(r.remaining() >= 10, "short meta");
    let tag = r.get_u8();
    let first_sighting = r.get_u8() != 0;
    let span = SeqSpan { start: r.get_u32(), end: r.get_u32() };
    let cid = |r: &mut Bytes| -> anyhow::Result<Cid> {
        anyhow::ensure!(r.remaining() >= 33, "short cid");
        let codec = r.get_u8();
        let mut digest = [0u8; 32];
        r.copy_to_slice(&mut digest);
        Ok(Cid { codec, digest })
    };
    let kind = match tag {
        0 | 1 => {
            anyhow::ensure!(r.remaining() >= 9, "short verified");
            let vkind = if r.get_u8() == 1 { VerifiedKind::Sync } else { VerifiedKind::Commit };
            let rev = Tid(r.get_u64());
            let commit = cid(&mut r)?;
            let data = cid(&mut r)?;
            anyhow::ensure!(r.remaining() >= 1, "short verified");
            let flags = r.get_u8();
            let prev_data = if flags & 1 != 0 { Some(cid(&mut r)?) } else { None };
            let created = flags & 2 != 0;
            let v = Verified { kind: vkind, did: did.to_string(), rev, commit, data, prev_data, created };
            if tag == 0 { CheckedKind::Commit(v) } else { CheckedKind::Sync(v) }
        }
        2 => CheckedKind::Identity,
        3 => {
            anyhow::ensure!(r.remaining() >= 3, "short account");
            let active = r.get_u8() != 0;
            let n = r.get_u16();
            let status = if n == u16::MAX {
                None
            } else {
                anyhow::ensure!(r.remaining() >= n as usize, "short status");
                Some(String::from_utf8(r.split_to(n as usize).to_vec())?)
            };
            CheckedKind::Account { active, status }
        }
        t => anyhow::bail!("unknown kind {t}"),
    };
    Ok(Meta { kind, first_sighting, span })
}

/// Rejections that mean "not now" rather than "never": the forwarder
/// retries them, against a new owner if one takes over.
fn retryable(reason: &str) -> Option<StageError> {
    match reason {
        "not_owner" => Some(StageError::NotOwner),
        "store" => Some(StageError::Unavailable("state store".into())),
        _ => None,
    }
}

// ---- the DID owner ----

pub struct Stage(Arc<Glue>);

#[async_trait::async_trait]
impl DidStage for Stage {
    async fn apply(&self, batch: Vec<Forwarded>) -> Vec<StageResult> {
        // DIDs in parallel, each DID's events in order
        let mut by_did: HashMap<String, Vec<(usize, Forwarded)>> = HashMap::new();
        let n = batch.len();
        for (i, f) in batch.into_iter().enumerate() {
            by_did.entry(f.did.clone()).or_default().push((i, f));
        }
        let runs = by_did.into_values().map(|evs| self.apply_did(evs));
        let mut out: Vec<Option<StageResult>> = vec![None; n];
        for rs in futures::future::join_all(runs).await {
            for (i, r) in rs {
                out[i] = Some(r);
            }
        }
        out.into_iter().map(|r| r.unwrap_or_else(|| Err(StageError::Unavailable("unanswered".into())))).collect()
    }
}

impl Stage {
    async fn apply_did(&self, evs: Vec<(usize, Forwarded)>) -> Vec<(usize, StageResult)> {
        let g = &self.0;
        let mut out = Vec::with_capacity(evs.len());
        let mut waits = Vec::new();
        let mut blocked: Option<StageError> = None;
        for (i, f) in evs {
            // claim, apply, append and register as one step per DID, so a
            // second copy from another host owner sees the first one's append
            let _did = g.inflight.lock(&f.did).await;
            if let Some(e) = &blocked {
                out.push((i, Err(e.clone())));
                continue;
            }
            let m = match decode_meta(&f.did, f.meta.clone()) {
                Ok(m) => m,
                Err(e) => {
                    out.push((i, Ok(Outcome::Rejected(format!("bad_meta: {e:#}")))));
                    continue;
                }
            };
            let shard = g.state.shard_id_of_slot(vlpds::slots::slot_of(&f.did)).0;
            let dedupe = !matches!(m.kind, CheckedKind::Commit(_)) && f.upstream_seq > 0;
            if dedupe
                && !g.recent.claim(&f.host, f.upstream_seq, &f.did, shard, g.log.next_ordinal.load(Ordering::Acquire))
            {
                metrics::EVENTS_DUPLICATE.with_label_values(&["cluster_recent"]).inc();
                waits.push(Wait::Duplicate { i, of: g.inflight.watch(&f.did) });
                continue;
            }
            let identity = matches!(m.kind, CheckedKind::Identity);
            let c = Checked {
                did: f.did.clone(),
                host: f.host.clone(),
                upstream_seq: f.upstream_seq,
                kind: m.kind,
                frame: f.frame,
                span: m.span,
                received: Instant::now(),
                first_sighting: m.first_sighting,
            };
            let r = g.local.submit(c).await;
            let unclaim = || {
                if dedupe {
                    g.recent.release(&f.host, f.upstream_seq, &f.did);
                }
            };
            match r {
                Submitted::Appended(rx) => {
                    let (id, done) = g.inflight.begin(&f.did);
                    waits.push(Wait::Appended {
                        i,
                        rx,
                        host: f.host,
                        useq: f.upstream_seq,
                        dedupe,
                        identity,
                        did: f.did,
                        id,
                        done,
                    })
                }
                Submitted::Duplicate => {
                    unclaim();
                    waits.push(Wait::Duplicate { i, of: g.inflight.watch(&f.did) });
                }
                Submitted::Rejected(rj) => {
                    unclaim();
                    match retryable(rj.reason) {
                        Some(e) => {
                            blocked = Some(e.clone());
                            out.push((i, Err(e)));
                        }
                        None => out.push((i, Ok(Outcome::Rejected(format!("{}: {}", rj.reason, rj.detail))))),
                    }
                }
                Submitted::Forwarded(_) => {
                    unclaim();
                    out.push((i, Err(StageError::Unavailable("local owner forwarded".into()))));
                }
            }
        }
        let mut changed_keys = Vec::new();
        for w in waits {
            let (i, rx, host, useq, dedupe, identity, did, id, done) = match w {
                Wait::Appended { i, rx, host, useq, dedupe, identity, did, id, done } => {
                    (i, rx, host, useq, dedupe, identity, did, id, done)
                }
                Wait::Duplicate { i, of } => {
                    out.push((i, g.duplicate(of).await));
                    continue;
                }
            };
            let r = match rx.await {
                Ok(Ok(d)) if g.cluster.lease_valid() => {
                    g.committed.fetch_max(d.ordinal + 1, Ordering::AcqRel);
                    if dedupe {
                        g.recent.settle(&host, useq, &did, d.ordinal);
                    }
                    if identity {
                        changed_keys.push(did.clone());
                    }
                    Ok(Outcome::Appended(d.seqs.first().copied().unwrap_or(0)))
                }
                Ok(Ok(_)) => Err(StageError::Unavailable("node lease lapsed".into())),
                Ok(Err(e)) => Err(StageError::Unavailable(format!("log: {e}"))),
                Err(_) => Err(StageError::Unavailable("log closed".into())),
            };
            if r.is_err() && dedupe {
                g.recent.release(&host, useq, &did);
            }
            g.inflight.done(&did, id, done, r.is_ok());
            out.push((i, r));
        }
        if !changed_keys.is_empty() {
            let c = g.cluster.clone();
            tokio::spawn(async move { c.invalidate_keys(changed_keys).await });
        }
        out
    }
}

/// Resolves to whether an append became durable (None until it did or failed).
type Durability = tokio::sync::watch::Receiver<Option<bool>>;

enum Wait {
    Appended {
        i: usize,
        rx: super::DurableRx,
        host: Host,
        useq: i64,
        dedupe: bool,
        identity: bool,
        did: String,
        id: u64,
        done: tokio::sync::watch::Sender<Option<bool>>,
    },
    Duplicate {
        i: usize,
        of: Option<Durability>,
    },
}

impl Glue {
    /// A duplicate is answered only once the copy it duplicates is durable.
    /// The first copy may still be in flight (a zombie host owner and the
    /// real one both sent it), and a host owner acks past a duplicate: if
    /// this node died before the first copy landed, the event would be
    /// acked and lost.
    async fn duplicate(&self, of: Option<Durability>) -> StageResult {
        let durable = match of {
            None => true,
            Some(rx) => durable(rx, Duration::from_secs(10)).await,
        };
        if durable && self.cluster.lease_valid() {
            Ok(Outcome::Duplicate)
        } else {
            Err(StageError::Unavailable("the duplicated event isn't durable".into()))
        }
    }
}

async fn durable(mut rx: Durability, limit: Duration) -> bool {
    matches!(
        tokio::time::timeout(limit, rx.wait_for(|v| v.is_some())).await,
        Ok(Ok(v)) if *v == Some(true)
    )
}

/// Per DID, the newest event this owner appended that may not be durable
/// yet, and a lock per stripe of DIDs held from the dedupe claim to the
/// append's registration here.
struct Inflight {
    stripes: Box<[tokio::sync::Mutex<()>]>,
    last: Mutex<HashMap<String, (u64, Durability)>>,
    next: AtomicU64,
}

impl Default for Inflight {
    fn default() -> Inflight {
        Inflight {
            stripes: (0..1024).map(|_| tokio::sync::Mutex::new(())).collect(),
            last: Mutex::new(HashMap::new()),
            next: AtomicU64::new(0),
        }
    }
}

impl Inflight {
    async fn lock(&self, did: &str) -> tokio::sync::MutexGuard<'_, ()> {
        self.stripes[(did_key(did) % self.stripes.len() as u64) as usize].lock().await
    }

    fn begin(&self, did: &str) -> (u64, tokio::sync::watch::Sender<Option<bool>>) {
        let id = self.next.fetch_add(1, Ordering::Relaxed);
        let (tx, rx) = tokio::sync::watch::channel(None);
        self.last.lock().insert(did.to_string(), (id, rx));
        (id, tx)
    }

    /// The log commits in append order, so the newest append's outcome
    /// covers every earlier one of the DID.
    fn watch(&self, did: &str) -> Option<Durability> {
        self.last.lock().get(did).map(|(_, rx)| rx.clone())
    }

    fn done(&self, did: &str, id: u64, tx: tokio::sync::watch::Sender<Option<bool>>, ok: bool) {
        let _ = tx.send(Some(ok));
        let mut m = self.last.lock();
        if m.get(did).is_some_and(|(i, _)| *i == id) {
            m.remove(did);
        }
    }
}

/// (host, upstream seq) of the non-commit events this DID owner appended
/// (or replayed from an earlier owner's log), kept until the host's
/// checkpointed cursor passes them.
#[derive(Default)]
pub struct Recent {
    m: Mutex<HashMap<Host, BTreeMap<i64, Ent>>>,
}

#[derive(Clone, Copy)]
struct Ent {
    shard: u32,
    /// [`did_key`] of the event's DID: a host whose sequence restarted
    /// (FutureCursor) reuses seqs, and a reused seq is almost always
    /// another DID's event, which must not be dropped as a replay.
    did: u64,
    /// Our log's ordinal (a lower bound until settled); None: replayed from
    /// another log.
    ordinal: Option<u64>,
    at: Instant,
}

/// FNV-1a: stable across processes and versions, since the dedupe set is
/// persisted and read back by another node.
pub fn did_key(did: &str) -> u64 {
    did.bytes().fold(0xcbf2_9ce4_8422_2325, |h, b| (h ^ b as u64).wrapping_mul(0x100_0000_01b3))
}

impl Recent {
    /// False if this (host, seq) is already here for this DID: a replay.
    pub fn claim(&self, host: &Host, useq: i64, did: &str, shard: u32, floor: u64) -> bool {
        let did = did_key(did);
        let mut m = self.m.lock();
        let seqs = m.entry(host.clone()).or_default();
        if seqs.get(&useq).is_some_and(|e| e.did == did) {
            return false;
        }
        seqs.insert(useq, Ent { shard, did, ordinal: Some(floor), at: Instant::now() });
        true
    }

    fn get_mut<'a>(
        m: &'a mut HashMap<Host, BTreeMap<i64, Ent>>,
        host: &Host,
        useq: i64,
        did: &str,
    ) -> Option<&'a mut Ent> {
        m.get_mut(host).and_then(|s| s.get_mut(&useq)).filter(|e| e.did == did_key(did))
    }

    pub fn settle(&self, host: &Host, useq: i64, did: &str, ordinal: u64) {
        if let Some(e) = Self::get_mut(&mut self.m.lock(), host, useq, did) {
            e.ordinal = Some(ordinal);
        }
    }

    /// Only this DID's claim: after a sequence restart the seq may already
    /// belong to another DID's event.
    pub fn release(&self, host: &Host, useq: i64, did: &str) {
        let mut m = self.m.lock();
        if Self::get_mut(&mut m, host, useq, did).is_none() {
            return;
        }
        if let Some(s) = m.get_mut(host) {
            s.remove(&useq);
            if s.is_empty() {
                m.remove(host);
            }
        }
    }

    pub fn replayed(&self, host: &Host, useq: i64, did: u64, shard: u32) {
        self.m.lock().entry(host.clone()).or_default().entry(useq).or_insert(Ent {
            shard,
            did,
            ordinal: None,
            at: Instant::now(),
        });
    }

    pub fn prune(&self, cursors: &HashMap<Host, i64>, max_age: Duration) {
        let mut m = self.m.lock();
        m.retain(|h, seqs| {
            if let Some(c) = cursors.get(h) {
                *seqs = seqs.split_off(&(c + 1));
            }
            seqs.retain(|_, e| e.at.elapsed() < max_age);
            !seqs.is_empty()
        });
    }

    /// The shard's entries replayed from another log or inherited from an
    /// earlier owner's set, sorted.
    pub fn inherited(&self, shard: u32) -> Vec<DedupeEntry> {
        let m = self.m.lock();
        let mut v: Vec<DedupeEntry> = m
            .iter()
            .flat_map(|(h, s)| {
                s.iter().filter(|(_, e)| e.shard == shard && e.ordinal.is_none()).map(|(q, e)| (h.clone(), *q, e.did))
            })
            .collect();
        v.sort();
        v
    }

    pub fn min_ordinal(&self, shard: u32) -> Option<u64> {
        self.m.lock().values().flat_map(|s| s.values()).filter(|e| e.shard == shard).filter_map(|e| e.ordinal).min()
    }

    pub fn len(&self) -> usize {
        self.m.lock().values().map(|s| s.len()).sum()
    }

    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }
}

/// Each DID shard's inherited dedupe entries, one object per (shard, log):
/// `dedupe/{shard}/{log_id}`. An owner that crashes before the hosts'
/// checkpoints pass what it inherited would otherwise take those entries
/// with it, since its open already moved the earlier log's marker past them.
/// Only the shard's owner writes its own object; the next owner reads every
/// log of the shard's history and deletes the earlier ones once its own is
/// written.
/// (host, upstream seq, [`did_key`]) of one dedupe entry.
type DedupeEntry = (Host, i64, u64);

struct DedupeStore {
    store: Store,
    log_id: String,
    /// What each shard's object holds as last written (absent: no object).
    written: Mutex<HashMap<ShardId, Vec<DedupeEntry>>>,
}

#[derive(Serialize, Deserialize, Default)]
struct DedupeDoc {
    #[serde(default)]
    entries: Vec<(String, i64, u64)>,
}

impl DedupeStore {
    fn new(store: Store, log_id: String) -> DedupeStore {
        DedupeStore { store, log_id, written: Mutex::new(HashMap::new()) }
    }

    fn path(&self, shard: ShardId, log_id: &str) -> Path {
        Path::from(format!("{}/dedupe/{}/{log_id}", self.store.prefix, shard.key()))
    }

    async fn read(&self, shard: ShardId, log_id: &str) -> anyhow::Result<Vec<DedupeEntry>> {
        match self.store.raw.get(&self.path(shard, log_id)).await {
            Ok(r) => {
                let doc: DedupeDoc = serde_json::from_slice(&r.bytes().await?)?;
                Ok(doc.entries.into_iter().map(|(h, q, d)| (Host(h), q, d)).collect())
            }
            Err(object_store::Error::NotFound { .. }) => Ok(Vec::new()),
            Err(e) => Err(e.into()),
        }
    }

    /// Our object for the shard holds `entries`: written when they changed,
    /// deleted when they're gone.
    async fn write(&self, shard: ShardId, entries: Vec<DedupeEntry>) -> anyhow::Result<()> {
        if self.written.lock().get(&shard).map_or(entries.is_empty(), |w| *w == entries) {
            return Ok(());
        }
        if entries.is_empty() {
            self.delete(shard, &self.log_id).await?;
            self.written.lock().remove(&shard);
            return Ok(());
        }
        let doc = DedupeDoc { entries: entries.iter().map(|(h, q, d)| (h.0.clone(), *q, *d)).collect() };
        self.store.raw.put(&self.path(shard, &self.log_id), PutPayload::from(serde_json::to_vec(&doc)?)).await?;
        self.written.lock().insert(shard, entries);
        Ok(())
    }

    async fn delete(&self, shard: ShardId, log_id: &str) -> anyhow::Result<()> {
        match self.store.raw.delete(&self.path(shard, log_id)).await {
            Ok(()) | Err(object_store::Error::NotFound { .. }) => Ok(()),
            Err(e) => Err(e.into()),
        }
    }
}

/// The hosts' checkpointed cursors (`hostck/`, every host shard), re-read
/// only where an object changed.
/// An object as read, with its ETag.
type Tagged<T> = (Option<String>, T);

struct HostCks {
    store: Store,
    docs: Mutex<HashMap<String, Tagged<BTreeMap<String, i64>>>>,
}

#[derive(Deserialize, Default)]
struct CkDoc {
    #[serde(default)]
    cursors: BTreeMap<String, i64>,
}

impl HostCks {
    fn new(store: Store) -> HostCks {
        HostCks { store, docs: Mutex::new(HashMap::new()) }
    }

    async fn refresh(&self) -> anyhow::Result<HashMap<Host, i64>> {
        use futures::StreamExt;
        let prefix = Path::from(format!("{}/hostck", self.store.prefix));
        let mut listed = Vec::new();
        let mut list = self.store.raw.list(Some(&prefix));
        while let Some(m) = list.next().await {
            let m = m?;
            listed.push((m.location.to_string(), m.e_tag.clone()));
        }
        let stale: Vec<(String, Option<String>)> = {
            let d = self.docs.lock();
            listed.into_iter().filter(|(p, e)| e.is_none() || d.get(p).is_none_or(|(x, _)| x != e)).collect()
        };
        for (p, etag) in stale {
            match self.store.raw.get(&Path::from(p.as_str())).await {
                Ok(r) => {
                    let doc: CkDoc = serde_json::from_slice(&r.bytes().await?).unwrap_or_default();
                    self.docs.lock().insert(p, (etag, doc.cursors));
                }
                Err(object_store::Error::NotFound { .. }) => {}
                Err(e) => return Err(e.into()),
            }
        }
        let mut out = HashMap::new();
        for (_, c) in self.docs.lock().values() {
            for (h, s) in c {
                let e = out.entry(Host(h.clone())).or_insert(*s);
                *e = (*e).max(*s);
            }
        }
        Ok(out)
    }
}

// ---- DID shards ----

pub struct Shards(Arc<Glue>);

/// One span of an earlier owner's log: only its ordinals, so a log that
/// held the shard twice doesn't replay the second stay before the owner in
/// between.
struct SpanTail<'a> {
    replay: &'a LogReplay,
    start: u64,
    end: Option<u64>,
}

impl SpanTail<'_> {
    /// Past `after` and inside the span.
    fn from(&self, after: Option<u64>) -> u64 {
        after.map_or(0, |a| a + 1).max(self.start)
    }
}

#[async_trait::async_trait]
impl ReplaySource for SpanTail<'_> {
    async fn tail(
        &self,
        log_id: &str,
        shard: ShardId,
        after: Option<u64>,
    ) -> anyhow::Result<Vec<(u64, Vec<StateDelta>)>> {
        let from = self.from(after);
        let t = self.replay.read_span(log_id, from, self.end).await?;
        let mut v = super::adapters::deltas_of(&t, log_id, shard, from)?;
        v.retain(|(o, _)| self.end.is_none_or(|e| *o < e));
        Ok(v)
    }
    async fn frames(
        &self,
        log_id: &str,
        shard: ShardId,
        after: Option<u64>,
    ) -> anyhow::Result<Vec<(u64, Vec<(String, Bytes)>)>> {
        let from = self.from(after);
        let t = self.replay.read_span(log_id, from, self.end).await?;
        let mut v = super::adapters::frames_of(&t, shard, from);
        v.retain(|(o, _)| self.end.is_none_or(|e| *o < e));
        Ok(v)
    }
}

impl Shards {
    async fn open_one(&self, id: ShardId, history: &[Span], replay: &LogReplay) -> anyhow::Result<usize> {
        let g = &self.0;
        let t0 = Instant::now();
        let s = g.state.open_shard(id, None).await?;
        let opened = t0.elapsed();
        let mut n = 0;
        for sp in history {
            let before = s.applied_marker(&sp.log_id).await?;
            let src = SpanTail { replay, start: sp.start, end: sp.end };
            n += g.state.recover(id, &sp.log_id, &src, state::now_secs()).await?;
            // what the earlier owner appended past its marker may come again
            // from a host whose checkpoint is behind it
            let from = before.map_or(sp.start, |b| (b + 1).max(sp.start));
            for (ord, evs) in replay.read_span(&sp.log_id, from, sp.end).await?.iter() {
                if *ord < from || sp.end.is_some_and(|e| *ord >= e) {
                    continue;
                }
                for e in evs.iter().filter(|e| e.meta.shard == id.0 && e.meta.upstream_seq > 0) {
                    let commit = e
                        .delta
                        .as_ref()
                        .and_then(|d| StateDelta::decode(d).ok())
                        .is_some_and(|d| d.kind == state::ChangeKind::Commit);
                    if !commit {
                        g.recent.replayed(&e.meta.host, e.meta.upstream_seq, did_key(&e.meta.did), id.0);
                    }
                }
            }
        }
        // What earlier owners inherited themselves: entries whose log marker
        // a previous open already moved past (two crashes in a row).
        let mut logs: Vec<&str> = Vec::new();
        for sp in history {
            if !logs.contains(&sp.log_id.as_str()) {
                logs.push(&sp.log_id);
            }
        }
        let mut inherited = 0;
        let sets = futures::future::join_all(logs.iter().map(|l| g.dedupe.read(id, l))).await;
        for set in sets {
            for (h, useq, did) in set? {
                g.recent.replayed(&h, useq, did, id.0);
                inherited += 1;
            }
        }
        // ours is written before any event is routed here, so a crash right
        // after this open loses nothing; then the earlier copies can go
        g.dedupe.write(id, g.recent.inherited(id.0)).await?;
        let earlier: Vec<&str> = logs.into_iter().filter(|l| *l != g.dedupe.log_id).collect();
        let deletes = futures::future::join_all(earlier.iter().map(|l| g.dedupe.delete(id, l))).await;
        for (l, r) in earlier.iter().zip(deletes) {
            if let Err(e) = r {
                tracing::warn!(shard = %id, log = l, "deleting an earlier dedupe set failed: {e:#}");
            }
        }
        // Every earlier span is in the shard's state now; once that's durable
        // the cluster may drop them from the history (`checkpointed`), so the
        // next open replays our span alone however many owners came before.
        s.flush_memtable().await?;
        tracing::info!(
            shard = %id,
            spans = history.len(),
            replayed = n,
            inherited,
            open_ms = opened.as_millis() as u64,
            replay_ms = (t0.elapsed() - opened).as_millis() as u64,
            "opened DID shard"
        );
        Ok(n)
    }
}

#[async_trait::async_trait]
impl DidShards for Shards {
    async fn open(&self, shards: Vec<(ShardId, Vec<Span>)>) -> Vec<(ShardId, anyhow::Result<()>)> {
        let t0 = Instant::now();
        let replay = LogReplay::new(self.0.state.store.clone());
        let opens = shards.into_iter().map(|(id, history)| {
            let replay = &replay;
            async move {
                let r = self.open_one(id, &history, replay).await;
                if r.is_ok() {
                    self.0.clean.lock().insert(id);
                }
                if let Err(e) = &r {
                    tracing::warn!(shard = %id, "opening DID shard failed: {e:#}");
                    let _ = self.0.state.close_shard(id).await;
                }
                (id, r.map(|_| ()))
            }
        });
        let out = futures::future::join_all(opens).await;
        CLUSTER_SHARD_OPEN.observe(t0.elapsed().as_secs_f64());
        out
    }

    async fn close(&self, shards: Vec<ShardId>) -> Vec<(ShardId, anyhow::Result<()>)> {
        let g = &self.0;
        let committed = g.committed.load(Ordering::Acquire);
        let mut out = Vec::new();
        for id in shards {
            let r = async {
                g.clean.lock().remove(&id);
                g.checkpoint_shard(id, committed).await?;
                g.markers.lock().remove(&id);
                g.state.close_shard(id).await
            }
            .await;
            if let Err(e) = &r {
                tracing::warn!(shard = %id, "closing DID shard failed: {e:#}");
            }
            out.push((id, r));
        }
        out
    }

    fn checkpointed(&self, shard: ShardId) -> bool {
        self.0.clean.lock().contains(&shard)
    }

    fn on_layout(&self, layout: Arc<Layout>) {
        self.0.state.set_layout(layout.shards.clone());
    }
}

// ---- upstreams ----

struct Upstreams {
    node: std::sync::Weak<Node>,
}

#[async_trait::async_trait]
impl HostHandler for Upstreams {
    async fn release(&self, keep: HostFilter) -> Vec<(Host, i64)> {
        let Some(node) = self.node.upgrade() else {
            return Vec::new();
        };
        let stopped = match node.manager.set_filter(keep).await {
            Ok(s) => s,
            Err(e) => {
                tracing::warn!("releasing hosts failed: {e:#}");
                return Vec::new();
            }
        };
        // what's still in the pipeline lands before the cursor is handed
        // over, so the next owner doesn't get it again
        let deadline = Instant::now() + Duration::from_secs(5);
        while stopped.iter().any(|h| node.acks.pending_for(h) > 0) && Instant::now() < deadline {
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
        stopped
            .into_iter()
            .filter_map(|h| node.manager.host(&h).and_then(|v| v.record.acked_seq).map(|s| (h, s)))
            .collect()
    }

    fn acked(&self) -> Vec<(Host, i64)> {
        let Some(node) = self.node.upgrade() else {
            return Vec::new();
        };
        node.manager.hosts().into_iter().filter_map(|v| Some((Host(v.record.hostname), v.record.acked_seq?))).collect()
    }
}

/// The host registry in the bucket: one JSON object of `state::HostRecord`s
/// per host shard (`hosts/{shard}`), every write a CAS read-modify-write.
/// It's both the upstream registry's store and the `state::HostStore` that
/// listHosts and the counters use.
pub struct BucketHosts {
    store: Store,
    layout: Arc<Layout>,
    docs: Mutex<HashMap<ShardId, Tagged<Arc<HostDoc>>>>,
    /// One `update_host` at a time on this node: its closure runs once, so
    /// a CAS conflict can only be retried when the host's own record didn't
    /// change under it.
    updates: tokio::sync::Mutex<()>,
}

#[derive(Serialize, Deserialize, Default, Clone)]
struct HostDoc {
    #[serde(default)]
    hosts: BTreeMap<String, HostRecord>,
}

impl BucketHosts {
    pub fn new(store: Store, layout: Arc<Layout>) -> BucketHosts {
        BucketHosts { store, layout, docs: Mutex::new(HashMap::new()), updates: tokio::sync::Mutex::new(()) }
    }

    fn path(&self, s: ShardId) -> Path {
        Path::from(format!("{}/hosts/{}", self.store.prefix, s.key()))
    }

    async fn get(&self, s: ShardId) -> anyhow::Result<(Option<String>, HostDoc)> {
        match self.store.raw.get(&self.path(s)).await {
            Ok(r) => {
                let etag = r.meta.e_tag.clone();
                Ok((etag, serde_json::from_slice(&r.bytes().await?)?))
            }
            Err(object_store::Error::NotFound { .. }) => Ok((None, HostDoc::default())),
            Err(e) => Err(e.into()),
        }
    }

    /// Read, change, write if-match; again on a conflict. `f` returns
    /// whether it changed anything.
    async fn update(&self, s: ShardId, f: impl Fn(&mut HostDoc) -> bool) -> anyhow::Result<Arc<HostDoc>> {
        loop {
            let (etag, mut doc) = self.get(s).await?;
            if !f(&mut doc) {
                let doc = Arc::new(doc);
                self.docs.lock().insert(s, (etag, doc.clone()));
                return Ok(doc);
            }
            let mode = match &etag {
                Some(_) => vlpds::cluster::if_match(etag.clone()),
                None => PutMode::Create,
            };
            let body = PutPayload::from(serde_json::to_vec(&doc)?);
            match self.store.raw.put_opts(&self.path(s), body, PutOptions { mode, ..Default::default() }).await {
                Ok(r) => {
                    let doc = Arc::new(doc);
                    self.docs.lock().insert(s, (r.e_tag, doc.clone()));
                    return Ok(doc);
                }
                Err(object_store::Error::Precondition { .. } | object_store::Error::AlreadyExists { .. }) => continue,
                Err(e) => return Err(e.into()),
            }
        }
    }

    /// Every shard's object, re-read where its ETag changed.
    async fn refresh(&self) -> anyhow::Result<Vec<HostRecord>> {
        use futures::StreamExt;
        let prefix = Path::from(format!("{}/hosts", self.store.prefix));
        let mut listed = Vec::new();
        let mut list = self.store.raw.list(Some(&prefix));
        while let Some(m) = list.next().await {
            let m = m?;
            if let Some(id) = m.location.filename().and_then(ShardId::from_key) {
                listed.push((id, m.e_tag.clone()));
            }
        }
        let stale: Vec<ShardId> = {
            let d = self.docs.lock();
            listed.iter().filter(|(id, e)| e.is_none() || d.get(id).is_none_or(|(x, _)| x != e)).map(|x| x.0).collect()
        };
        let got: Vec<anyhow::Result<(ShardId, Option<String>, HostDoc)>> = futures::stream::iter(stale)
            .map(|id| async move { self.get(id).await.map(|(e, d)| (id, e, d)) })
            .buffered(16)
            .collect()
            .await;
        {
            let mut d = self.docs.lock();
            for g in got {
                let (id, e, doc) = g?;
                d.insert(id, (e, Arc::new(doc)));
            }
        }
        let d = self.docs.lock();
        Ok(d.values().flat_map(|(_, doc)| doc.hosts.values().cloned()).collect())
    }

    fn group<'a, T>(&self, items: &'a [T], name: impl Fn(&T) -> &str) -> BTreeMap<ShardId, Vec<&'a T>> {
        let mut by: BTreeMap<ShardId, Vec<&T>> = BTreeMap::new();
        for it in items {
            by.entry(self.layout.shard_of(name(it))).or_default().push(it);
        }
        by
    }
}

impl upstream::HostStore for BucketHosts {
    fn load(&self) -> upstream::host::StoreFuture<'_, Vec<upstream::HostRecord>> {
        Box::pin(async move { Ok(self.refresh().await?.iter().map(super::adapters::to_upstream).collect()) })
    }

    /// As `StateHosts`: the registry's tier only seeds a new record, after
    /// that the policy engine owns it.
    fn put(&self, records: Vec<upstream::HostRecord>) -> upstream::host::StoreFuture<'_, ()> {
        Box::pin(async move {
            for (s, rows) in self.group(&records, |r| &r.hostname) {
                self.update(s, |doc| {
                    for r in &rows {
                        let rec = doc.hosts.entry(r.hostname.clone()).or_insert_with(|| {
                            HostRecord::new(&r.hostname, super::adapters::tier_to_state(r.tier), state::now_secs())
                        });
                        super::adapters::apply_upstream(rec, r);
                    }
                    true
                })
                .await?;
            }
            Ok(())
        })
    }
}

#[async_trait::async_trait]
impl state::HostStore for BucketHosts {
    async fn update_host(&self, hostname: &str, f: state::HostUpdate<'_>) -> anyhow::Result<Option<HostRecord>> {
        let _one = self.updates.lock().await;
        let s = self.layout.shard_of(hostname);
        let (mut etag, mut doc) = self.get(s).await?;
        let before = doc.hosts.get(hostname).cloned();
        let Some(next) = f(before.clone()) else { return Ok(None) };
        loop {
            doc.hosts.insert(hostname.to_string(), next.clone());
            let mode = match &etag {
                Some(_) => vlpds::cluster::if_match(etag.clone()),
                None => PutMode::Create,
            };
            let body = PutPayload::from(serde_json::to_vec(&doc)?);
            match self.store.raw.put_opts(&self.path(s), body, PutOptions { mode, ..Default::default() }).await {
                Ok(r) => {
                    self.docs.lock().insert(s, (r.e_tag, Arc::new(doc)));
                    return Ok(Some(next));
                }
                Err(object_store::Error::Precondition { .. } | object_store::Error::AlreadyExists { .. }) => {
                    (etag, doc) = self.get(s).await?;
                    // another host of the shard changed: ours still stands
                    anyhow::ensure!(
                        doc.hosts.get(hostname) == before.as_ref(),
                        "host {hostname} changed on another node during the update: try again"
                    );
                }
                Err(e) => return Err(e.into()),
            }
        }
    }

    async fn get_host(&self, hostname: &str) -> anyhow::Result<Option<HostRecord>> {
        let (_, doc) = self.get(self.layout.shard_of(hostname)).await?;
        Ok(doc.hosts.get(hostname).cloned())
    }

    async fn put_host(&self, rec: &HostRecord) -> anyhow::Result<()> {
        self.update(self.layout.shard_of(&rec.hostname), |doc| {
            doc.hosts.insert(rec.hostname.clone(), rec.clone());
            true
        })
        .await?;
        Ok(())
    }

    async fn checkpoint_cursors(&self, cursors: &[(String, i64)]) -> anyhow::Result<()> {
        for (s, rows) in self.group(cursors, |c| &c.0) {
            self.update(s, |doc| {
                let mut changed = false;
                for (h, seq) in &rows {
                    if let Some(r) = doc.hosts.get_mut(h)
                        && r.cursor < *seq
                    {
                        r.cursor = *seq;
                        changed = true;
                    }
                }
                changed
            })
            .await?;
        }
        Ok(())
    }

    async fn add_counts(&self, counts: &[(String, state::HostCounts)]) -> anyhow::Result<()> {
        let counts: Vec<&(String, state::HostCounts)> = counts.iter().filter(|(_, c)| !c.is_zero()).collect();
        for (s, rows) in self.group(&counts, |c| &c.0) {
            self.update(s, |doc| {
                for (h, c) in &rows {
                    let r = doc
                        .hosts
                        .entry(h.clone())
                        .or_insert_with(|| HostRecord::new(h, state::Tier::New, state::now_secs()));
                    r.account_count += c.accounts;
                    r.events += c.events;
                    r.failed_checks += c.failed_checks;
                    r.dropped += c.dropped;
                }
                !rows.is_empty()
            })
            .await?;
        }
        Ok(())
    }

    async fn list_hosts(&self, cursor: Option<&str>, limit: usize) -> anyhow::Result<HostPage> {
        let mut rows = self.refresh().await?;
        let key = |h: &str| (vlpds::slots::slot_of(h), h.to_string());
        rows.sort_by_key(|r| key(&r.hostname));
        if let Some(c) = cursor.filter(|c| !c.is_empty()) {
            let k = key(c);
            rows.retain(|r| key(&r.hostname) > k);
        }
        let limit = limit.max(1);
        let more = rows.len() > limit;
        rows.truncate(limit);
        let cursor = more.then(|| rows.last().map(|r| r.hostname.clone())).flatten();
        Ok(HostPage { hosts: rows, cursor })
    }
}

/// The sync API on a core node: repos from the DID shards it holds, hosts
/// from the bucket registry.
pub struct ClusterSync {
    pub state: Arc<State>,
    pub hosts: Arc<BucketHosts>,
}

#[async_trait::async_trait]
impl crate::sync_api::SyncSource for ClusterSync {
    async fn list_repos(&self, cursor: Option<&str>, limit: usize) -> Result<state::RepoPage, state::StoreError> {
        self.state.list_repos(cursor, limit).await
    }
    async fn repo(&self, did: &str) -> Result<Option<Arc<state::Record>>, state::StoreError> {
        self.state.get(did).await
    }
    async fn host(&self, hostname: &str) -> anyhow::Result<Option<HostRecord>> {
        state::HostStore::get_host(&*self.hosts, hostname).await
    }
    async fn list_hosts(&self, cursor: Option<&str>, limit: usize) -> anyhow::Result<HostPage> {
        state::HostStore::list_hosts(&*self.hosts, cursor, limit).await
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn meta_round_trips() {
        let cid = |b| Cid { codec: 0x71, digest: [b; 32] };
        let v = Verified {
            kind: VerifiedKind::Commit,
            did: "did:plc:x".into(),
            rev: Tid(42),
            commit: cid(1),
            data: cid(2),
            prev_data: Some(cid(3)),
            created: false,
        };
        let mk = |kind| Checked {
            did: "did:plc:x".into(),
            host: Host("pds.test".into()),
            upstream_seq: 7,
            kind,
            frame: Bytes::new(),
            span: SeqSpan { start: 3, end: 9 },
            received: Instant::now(),
            first_sighting: true,
        };
        for kind in [
            CheckedKind::Commit(v.clone()),
            CheckedKind::Commit(Verified { prev_data: None, created: true, ..v.clone() }),
            CheckedKind::Sync(Verified { kind: VerifiedKind::Sync, prev_data: None, ..v.clone() }),
            CheckedKind::Identity,
            CheckedKind::Account { active: false, status: Some("deactivated".into()) },
            CheckedKind::Account { active: true, status: None },
        ] {
            let c = mk(kind);
            let m = decode_meta(&c.did, encode_meta(&c)).unwrap();
            assert_eq!(m.span, c.span);
            assert!(m.first_sighting);
            match (&m.kind, &c.kind) {
                (CheckedKind::Commit(a), CheckedKind::Commit(b)) | (CheckedKind::Sync(a), CheckedKind::Sync(b)) => {
                    assert_eq!(a, b)
                }
                (CheckedKind::Identity, CheckedKind::Identity) => {}
                (CheckedKind::Account { active: a, status: s }, CheckedKind::Account { active: b, status: t }) => {
                    assert_eq!((a, s), (b, t))
                }
                _ => panic!("kind changed"),
            }
        }
    }

    #[test]
    fn recent_dedupes_until_the_cursor_passes() {
        let r = Recent::default();
        let h = Host("pds.test".into());
        assert!(r.claim(&h, 10, "did:a", 0, 5));
        assert!(!r.claim(&h, 10, "did:a", 0, 5));
        r.settle(&h, 10, "did:a", 7);
        r.replayed(&h, 12, did_key("did:b"), 1);
        assert_eq!(r.min_ordinal(0), Some(7));
        assert_eq!(r.min_ordinal(1), None);
        r.prune(&HashMap::from([(h.clone(), 10)]), Duration::from_secs(60));
        assert!(r.claim(&h, 10, "did:a", 0, 5), "past the cursor: forgotten");
        assert!(!r.claim(&h, 12, "did:b", 1, 5));
        r.release(&h, 10, "did:a");
        assert_eq!(r.len(), 1);
    }

    /// The zombie-plus-crash gap: a second copy of an event is a duplicate
    /// only once the first copy is durable, and not at all if it failed.
    #[tokio::test]
    async fn a_duplicate_waits_for_the_first_copy() {
        let inf = Inflight::default();
        assert!(inf.watch("did:a").is_none(), "nothing in flight: answer at once");
        let (id, tx) = inf.begin("did:a");
        let rx = inf.watch("did:a").unwrap();
        assert!(!durable(rx.clone(), Duration::from_millis(20)).await, "not durable yet");
        let waiter = tokio::spawn(durable(rx, Duration::from_secs(5)));
        inf.done("did:a", id, tx, true);
        assert!(waiter.await.unwrap());
        assert!(inf.watch("did:a").is_none());

        let (id1, tx1) = inf.begin("did:b");
        let (id2, tx2) = inf.begin("did:b");
        let rx = inf.watch("did:b").unwrap();
        inf.done("did:b", id1, tx1, true);
        assert!(inf.watch("did:b").is_some(), "the newer append is still in flight");
        inf.done("did:b", id2, tx2, false);
        assert!(!durable(rx, Duration::from_secs(1)).await, "a failed append is no duplicate to ack");
    }

    /// The two-crash gap: B inherits A's entries by replaying A's log past
    /// A's marker, and its open moves that marker past them. If B crashes
    /// before the hosts' checkpoints pass them, C gets them back only from
    /// B's persisted set.
    #[tokio::test]
    async fn inherited_dedupe_survives_a_second_crash() {
        let store = Store::memory(None);
        let shard = ShardId(3);
        let h = Host("pds.test".into());
        let b = Recent::default();
        b.replayed(&h, 41, did_key("did:a"), 3);
        assert!(b.claim(&h, 42, "did:a", 3, 7), "B's own append");
        b.settle(&h, 42, "did:a", 7);
        assert_eq!(b.inherited(3), vec![(h.clone(), 41, did_key("did:a"))], "only what B's marker doesn't cover");
        let bs = DedupeStore::new(store.clone(), "log-b".into());
        bs.write(shard, b.inherited(3)).await.unwrap();

        // B crashes; C opens the shard with A's and B's logs in its history
        let c = Recent::default();
        let cs = DedupeStore::new(store.clone(), "log-c".into());
        for l in ["log-a", "log-b"] {
            for (h, q, d) in cs.read(shard, l).await.unwrap() {
                c.replayed(&h, q, d, 3);
            }
        }
        assert!(!c.claim(&h, 41, "did:a", 3, 0), "the #identity A appended is still a replay on C");
        cs.write(shard, c.inherited(3)).await.unwrap();
        cs.delete(shard, "log-b").await.unwrap();
        assert!(cs.read(shard, "log-b").await.unwrap().is_empty());
        assert_eq!(cs.read(shard, "log-c").await.unwrap().len(), 1);

        // pruned past the checkpoint: C's object goes away
        c.prune(&HashMap::from([(h.clone(), 41)]), Duration::from_secs(60));
        cs.write(shard, c.inherited(3)).await.unwrap();
        assert!(cs.read(shard, "log-c").await.unwrap().is_empty());
    }

    /// The FutureCursor gap: a host whose sequence restarts reuses seqs the
    /// set still holds. Another DID's event at a held seq is new, and
    /// releasing it mustn't drop the held entry or the reverse.
    #[test]
    fn recent_tells_a_restarted_sequence_from_a_replay() {
        let r = Recent::default();
        let h = Host("pds.test".into());
        assert!(r.claim(&h, 5, "did:old", 0, 1));
        r.settle(&h, 5, "did:old", 1);
        assert!(r.claim(&h, 5, "did:new", 0, 2), "a reused seq for another DID is not a replay");
        assert!(!r.claim(&h, 5, "did:new", 0, 2), "the same DID at that seq again is");
        r.release(&h, 5, "did:old");
        assert_eq!(r.len(), 1, "releasing the old DID leaves the new claim");
        r.settle(&h, 5, "did:old", 9);
        assert_eq!(r.min_ordinal(0), Some(2), "settling the old DID doesn't touch the new claim");
        r.replayed(&h, 6, did_key("did:x"), 0);
        assert!(r.claim(&h, 6, "did:y", 0, 3));
    }
}
