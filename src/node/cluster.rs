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
use crate::serve::ServeConfig;
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
        o.serve = ServeConfig { retention: cfg.retention, threads: cfg.serve_threads, ..Default::default() };
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
    state: Arc<State>,
    log: Arc<NodeLog>,
    local: Arc<LocalOwner>,
    /// One past the highest own-log ordinal whose state is committed.
    committed: AtomicU64,
    /// `committed` as the previous checkpoint tick read it.
    committed_prev: AtomicU64,
    hostck: HostCks,
    markers: Mutex<HashMap<ShardId, u64>>,
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
        }
        let cli_tier = cfg.cli_host_tier;

        let ttf = Arc::new(Ttf::default());
        let local = LocalOwner::start(state.clone(), log.clone(), ttf.clone());
        let glue = Arc::new(Glue {
            cluster: cluster.clone(),
            hosts: hosts.clone(),
            recent: Arc::new(Recent::default()),
            state: state.clone(),
            log: log.clone(),
            local: local.clone(),
            committed: AtomicU64::new(0),
            committed_prev: AtomicU64::new(0),
            hostck: HostCks::new(store.clone()),
            markers: Mutex::new(HashMap::new()),
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
        crate::cluster::peer::spawn_listener(&cluster, peer)?;
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
                metrics::EVENTS_ACCEPTED.with_label_values(&[kind]).inc();
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
/// commit cid, data cid, prev_data (u8 flag + cid) | account: active u8,
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
            match &v.prev_data {
                Some(p) => {
                    b.put_u8(1);
                    cid(&mut b, p);
                }
                None => b.put_u8(0),
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
            let prev_data = if r.get_u8() == 1 { Some(cid(&mut r)?) } else { None };
            let v = Verified { kind: vkind, did: did.to_string(), rev, commit, data, prev_data };
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
            if dedupe && !g.recent.claim(&f.host, f.upstream_seq, shard, g.log.next_ordinal.load(Ordering::Acquire)) {
                metrics::EVENTS_DUPLICATE.with_label_values(&["cluster_recent"]).inc();
                out.push((i, Ok(Outcome::Duplicate)));
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
                    g.recent.release(&f.host, f.upstream_seq);
                }
            };
            match r {
                Submitted::Appended(rx) => waits.push((i, rx, f.host, f.upstream_seq, dedupe, identity, f.did)),
                Submitted::Duplicate => {
                    unclaim();
                    out.push((i, Ok(Outcome::Duplicate)));
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
        for (i, rx, host, useq, dedupe, identity, did) in waits {
            let r = match rx.await {
                Ok(Ok(d)) if g.cluster.lease_valid() => {
                    g.committed.fetch_max(d.ordinal + 1, Ordering::AcqRel);
                    if dedupe {
                        g.recent.settle(&host, useq, d.ordinal);
                    }
                    if identity {
                        changed_keys.push(did);
                    }
                    Ok(Outcome::Appended(d.seqs.first().copied().unwrap_or(0)))
                }
                Ok(Ok(_)) => Err(StageError::Unavailable("node lease lapsed".into())),
                Ok(Err(e)) => Err(StageError::Unavailable(format!("log: {e}"))),
                Err(_) => Err(StageError::Unavailable("log closed".into())),
            };
            if r.is_err() && dedupe {
                g.recent.release(&host, useq);
            }
            out.push((i, r));
        }
        if !changed_keys.is_empty() {
            let c = g.cluster.clone();
            tokio::spawn(async move { c.invalidate_keys(changed_keys).await });
        }
        out
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
    /// Our log's ordinal (a lower bound until settled); None: replayed from
    /// another log.
    ordinal: Option<u64>,
    at: Instant,
}

impl Recent {
    /// False if this (host, seq) is already here: a replay.
    pub fn claim(&self, host: &Host, useq: i64, shard: u32, floor: u64) -> bool {
        let mut m = self.m.lock();
        let seqs = m.entry(host.clone()).or_default();
        if seqs.contains_key(&useq) {
            return false;
        }
        seqs.insert(useq, Ent { shard, ordinal: Some(floor), at: Instant::now() });
        true
    }

    pub fn settle(&self, host: &Host, useq: i64, ordinal: u64) {
        if let Some(e) = self.m.lock().get_mut(host).and_then(|s| s.get_mut(&useq)) {
            e.ordinal = Some(ordinal);
        }
    }

    pub fn release(&self, host: &Host, useq: i64) {
        let mut m = self.m.lock();
        if let Some(s) = m.get_mut(host) {
            s.remove(&useq);
            if s.is_empty() {
                m.remove(host);
            }
        }
    }

    pub fn replayed(&self, host: &Host, useq: i64, shard: u32) {
        self.m.lock().entry(host.clone()).or_default().entry(useq).or_insert(Ent {
            shard,
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

/// The hosts' checkpointed cursors (`hostck/`, every host shard), re-read
/// only where an object changed.
struct HostCks {
    store: Store,
    docs: Mutex<HashMap<String, (Option<String>, BTreeMap<String, i64>)>>,
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

#[async_trait::async_trait]
impl ReplaySource for SpanTail<'_> {
    async fn tail(
        &self,
        log_id: &str,
        shard: ShardId,
        after: Option<u64>,
    ) -> anyhow::Result<Vec<(u64, Vec<StateDelta>)>> {
        let after = match (after, self.start.checked_sub(1)) {
            (a, None) => a,
            (None, s) => s,
            (Some(a), Some(s)) => Some(a.max(s)),
        };
        let mut v = self.replay.tail(log_id, shard, after).await?;
        v.retain(|(o, _)| *o >= self.start && self.end.is_none_or(|e| *o < e));
        Ok(v)
    }
}

impl Shards {
    async fn open_one(&self, id: ShardId, history: &[Span], replay: &LogReplay) -> anyhow::Result<usize> {
        let g = &self.0;
        let s = g.state.open_shard(id, None).await?;
        let mut n = 0;
        for sp in history {
            let before = s.applied_marker(&sp.log_id).await?;
            let src = SpanTail { replay, start: sp.start, end: sp.end };
            n += g.state.recover(id, &sp.log_id, &src, state::now_secs()).await?;
            // what the earlier owner appended past its marker may come again
            // from a host whose checkpoint is behind it
            let from = before.map_or(sp.start, |b| (b + 1).max(sp.start));
            for (ord, evs) in replay.read(&sp.log_id, from).await?.iter() {
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
                        g.recent.replayed(&e.meta.host, e.meta.upstream_seq, id.0);
                    }
                }
            }
        }
        Ok(n)
    }
}

#[async_trait::async_trait]
impl DidShards for Shards {
    async fn open(&self, shards: Vec<(ShardId, Vec<Span>)>) -> Vec<(ShardId, anyhow::Result<()>)> {
        let t0 = Instant::now();
        let replay = LogReplay::new(self.0.state.store.clone());
        let mut out = Vec::new();
        for (id, history) in shards {
            let r = self.open_one(id, &history, &replay).await;
            match &r {
                Ok(n) => {
                    tracing::info!(shard = %id, spans = history.len(), replayed = n, "opened DID shard")
                }
                Err(e) => {
                    tracing::warn!(shard = %id, "opening DID shard failed: {e:#}");
                    let _ = self.0.state.close_shard(id).await;
                }
            }
            out.push((id, r.map(|_| ())));
        }
        CLUSTER_SHARD_OPEN.observe(t0.elapsed().as_secs_f64());
        out
    }

    async fn close(&self, shards: Vec<ShardId>) -> Vec<(ShardId, anyhow::Result<()>)> {
        let g = &self.0;
        let committed = g.committed.load(Ordering::Acquire);
        let mut out = Vec::new();
        for id in shards {
            let r = async {
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
    docs: Mutex<HashMap<ShardId, (Option<String>, Arc<HostDoc>)>>,
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
        assert!(r.claim(&h, 10, 0, 5));
        assert!(!r.claim(&h, 10, 0, 5));
        r.settle(&h, 10, 7);
        r.replayed(&h, 12, 1);
        assert_eq!(r.min_ordinal(0), Some(7));
        assert_eq!(r.min_ordinal(1), None);
        r.prune(&HashMap::from([(h.clone(), 10)]), Duration::from_secs(60));
        assert!(r.claim(&h, 10, 0, 5), "past the cursor: forgotten");
        assert!(!r.claim(&h, 12, 1, 5));
        r.release(&h, 10);
        assert_eq!(r.len(), 1);
    }
}
