//! One relay node: upstream sockets in, a verified and sequenced firehose
//! out, with the object store as the only durable state.
//!
//! The pipeline, per upstream frame:
//!
//! 1. The upstream manager's fair queue hands the dispatcher a frame. It
//!    takes the cheap parse (`event::route`: kind and DID), notes the frame
//!    as pending for its host's cursor, and queues it on one of the lanes,
//!    picked by a hash of the DID.
//! 2. The lane runs the host owner's stage: the strict parse and the sync 1.1
//!    checks (`verify`), with the signing key from the DID document cache. A
//!    signature that fails against a cached key refreshes it and tries once
//!    more.
//! 3. The lane hands the checked event to the DID owner ([`DidOwner`]). The
//!    local one applies it to the DID's state (`state::StateStore::apply`:
//!    host authority, account status, the chain), and appends an accepted
//!    event to the node log with its state delta. It returns once the
//!    append is queued, before it's durable.
//! 4. When the segment holding it is durable, the DID owner commits the
//!    state change, and only then does the host's cursor move past it.
//!    The firehose merger has the event from the log by then.
//!
//! Order: a lane works one event at a time, start to append, and every
//! event of a DID goes to the same lane, so a DID's events reach the log in
//! the order its host sent them. Lanes run in parallel on their own
//! runtime, so the CPU of verification spreads over its threads. A host's
//! events finish out of order across lanes, so its cursor only moves past
//! a seq once every earlier one is done ([`acks::Tracker`]).
//!
//! The DID owner sits behind a trait so that the cluster can put a peer on
//! the other side: the lane's contract is "submit in order per DID, get told
//! when it's durable".
//!
//! Restart: earlier logs are fenced, the state shards replay each one past
//! their applied markers, and the upstream registry resumes each host from
//! its durable cursor. Events the log already holds beyond a host's cursor
//! come again; commits and syncs are caught as duplicates by their rev, and
//! the rest by the (host, upstream seq) pairs read out of the log tail.
//!
//! A cluster node (`Node::start_cluster`, [`cluster`]) is this pipeline
//! with the DID owner behind the cluster's forwarder, DID shards opened and
//! closed as they move, and the restart dedupe on the DID owner.

pub mod acks;
pub mod adapters;
pub mod admin;
pub mod cluster;
pub mod metrics;
pub mod peer_admin;
pub mod policy;

use crate::event::{self, Kind, SeqSpan};
use crate::identity::{HttpFetch, Identity, IdentityCache, LookupError};
use crate::seq::{self, Durable, EncodeWithSeq, EventMeta, LogConfig, LogError, NodeLog};
use crate::serve::{self, Serve, ServeConfig};
use crate::state::{self, Applied, EventKind, Incoming, StateStore};
use crate::types::{Host, UpstreamFrame};
use crate::upstream::{self, Manager, Tier, UpstreamConfig};
use crate::verify::{self, Reject, SigningKey, Verified};
use adapters::{CacheIdentity, LogReplay, StateHosts, VerifyChain};
use bytes::Bytes;
use futures::FutureExt;
use metrics::{Dash, HostRejects, RejectNote, Ttf};
use parking_lot::Mutex;
use std::collections::{HashMap, HashSet};
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{Duration, Instant};
use tokio::sync::{mpsc, oneshot};
use vlpds::store::Store;

pub type State = StateStore<VerifyChain>;

/// A host reader this far behind opens a case ([`Node::lag_cases`]).
pub const LAG_CASE_MS: i64 = 10 * 60_000;
const LAG_CASE_EVERY: Duration = Duration::from_secs(300);

#[derive(Clone, Debug)]
pub struct NodeConfig {
    /// The node log's id prefix.
    pub node_id: String,
    pub dev_mode: bool,
    pub plc_url: String,
    pub linger: Duration,
    /// Segment PUTs in flight.
    pub log_inflight: usize,
    pub max_segment_bytes: usize,
    pub did_shards: u32,
    pub retention: Duration,
    /// None: the firehose's default.
    pub max_lag_bytes: Option<usize>,
    /// Pipeline lanes (each a task; a DID always maps to the same one).
    pub lanes: usize,
    /// Threads of the runtime the lanes run on.
    pub ingest_threads: usize,
    /// Threads serving subscribeRepos.
    pub serve_threads: usize,
    /// Upstreams from the command line: a URL (`http://` means plain
    /// `ws://`, dev mode only) or a bare hostname.
    pub hosts: Vec<String>,
    /// Host cursors and shard applied markers go to the bucket this often.
    pub checkpoint_interval: Duration,
    pub identity: crate::identity::Options,
    pub upstream_limits: upstream::Limits,
    /// In-flight caps on what's read from upstreams (`upstream::flow`).
    pub inflight: upstream::flow::FlowLimits,
    /// Enforced when set (`node::policy`); without it hosts run at their
    /// tier's default limits and nothing is counted.
    pub policy: Option<policy::PolicyEngine>,
    /// The tier a `--host` upstream starts at the first time it's seen.
    pub cli_host_tier: Tier,
    /// Seeds DID documents from the PLC directory's export (`plc_seed`).
    pub plc_export: Option<crate::plc_seed::ingest::Config>,
}

impl NodeConfig {
    pub fn new(plc_url: &str) -> NodeConfig {
        let cores = std::thread::available_parallelism().map_or(4, |n| n.get());
        NodeConfig {
            node_id: "relay".into(),
            dev_mode: false,
            plc_url: plc_url.into(),
            linger: seq::DEFAULT_LINGER,
            log_inflight: seq::DEFAULT_INFLIGHT,
            max_segment_bytes: seq::DEFAULT_MAX_SEGMENT_BYTES,
            did_shards: 4,
            retention: seq::DEFAULT_RETENTION,
            max_lag_bytes: None,
            lanes: 64,
            ingest_threads: cores.clamp(2, 16),
            serve_threads: 4,
            hosts: Vec::new(),
            checkpoint_interval: Duration::from_secs(5),
            identity: crate::identity::Options::default(),
            upstream_limits: upstream::Limits::default(),
            inflight: upstream::flow::FlowLimits::default(),
            policy: None,
            cli_host_tier: Tier::Trusted,
            plc_export: None,
        }
    }

    pub fn serve_config(&self) -> ServeConfig {
        let d = ServeConfig::default();
        ServeConfig {
            retention: self.retention,
            threads: self.serve_threads,
            max_lag_bytes: self.max_lag_bytes.unwrap_or(d.max_lag_bytes),
            // a short (dev) window still prunes and checkpoints well inside it
            retention_interval: d.retention_interval.min(self.retention / 2).max(Duration::from_secs(1)),
            seq_checkpoint_every: d.seq_checkpoint_every.min(self.retention / 4).max(Duration::from_millis(250)),
            // the ring serves whatever it holds, however old: a window short
            // enough to test with (dev only) gets a ring small enough that old
            // cursors go to the bucket, where retention applies
            ring_bytes: if self.retention < Duration::from_secs(3600) { 1 << 20 } else { d.ring_bytes },
            ..d
        }
    }
}

/// An event that passed the host owner's checks, on its way to the DID owner.
pub struct Checked {
    pub did: String,
    pub host: Host,
    pub upstream_seq: i64,
    pub kind: CheckedKind,
    /// The frame as received, and where its seq is.
    pub frame: Bytes,
    pub span: SeqSpan,
    pub received: Instant,
    /// The first copy of this upstream seq (not a reconnect's replay).
    pub first_sighting: bool,
    /// The host socket it came on, for the forwarder (None on the DID
    /// owner's side).
    pub fence: Option<crate::cluster::forward::Fence>,
}

#[derive(Clone)]
pub enum CheckedKind {
    Commit(Verified),
    Sync(Verified),
    Identity,
    Account { active: bool, status: Option<String> },
}

impl CheckedKind {
    fn label(&self) -> &'static str {
        match self {
            CheckedKind::Commit(_) => "commit",
            CheckedKind::Sync(_) => "sync",
            CheckedKind::Identity => "identity",
            CheckedKind::Account { .. } => "account",
        }
    }
}

/// Why an event was dropped: a stable reason (metrics, dashboard) and the
/// specifics (logs, the host's recent rejects).
#[derive(Clone, Debug)]
pub struct Rejection {
    pub reason: &'static str,
    pub detail: String,
}

impl Rejection {
    fn verify(r: Reject) -> Rejection {
        Rejection { reason: r.reason(), detail: r.to_string() }
    }
}

pub type DurableRx = oneshot::Receiver<Result<Durable, LogError>>;

pub enum Submitted {
    /// Appended; resolves once the log segment is durable and the state
    /// change committed.
    Appended(DurableRx),
    /// Already applied (a replay): nothing to append.
    Duplicate,
    Rejected(Rejection),
    /// Handed to the cluster's forwarder: resolves with the DID owner's
    /// outcome once it's durable there, a duplicate or rejected.
    Forwarded(oneshot::Receiver<Result<crate::cluster::forward::Outcome, crate::cluster::forward::ForwardError>>),
}

/// The DID owner's side of the pipeline. Calls for one DID come in the
/// order its host sent the events, and each returns once the event is
/// applied and queued on the log, so the next one is checked against it.
#[async_trait::async_trait]
pub trait DidOwner: Send + Sync + 'static {
    async fn submit(&self, ev: Checked) -> Submitted;
}

/// The frame as received, its seq spliced on the way into the segment.
struct Spliced {
    frame: Bytes,
    span: SeqSpan,
}

impl EncodeWithSeq for Spliced {
    fn encode_with_seq(&self, seq: i64, out: &mut Vec<u8>) {
        event::splice_seq_into(&self.frame, self.span, seq, out);
    }
    fn len_hint(&self) -> usize {
        self.frame.len() + 9
    }
}

struct PendingCommit {
    durable: seq::Ticket,
    ticket: Option<state::Ticket>,
    tx: oneshot::Sender<Result<Durable, LogError>>,
    received: Instant,
}

/// This process's DID shards.
pub struct LocalOwner {
    pub state: Arc<State>,
    pub log: Arc<NodeLog>,
    /// One committer per DID shard (by shard id, modulo): one task couldn't
    /// commit much past 90k events/s, and shards commit independently. A
    /// DID's events all go to one committer, in the order they were applied.
    commits: Vec<mpsc::UnboundedSender<PendingCommit>>,
    /// Per committer: when its current batch became durable (µs since
    /// `epoch`, 0 = none).
    committing: Vec<Arc<AtomicU64>>,
}

impl LocalOwner {
    pub fn start(state: Arc<State>, log: Arc<NodeLog>, ttf: Arc<Ttf>, committers: usize) -> Arc<LocalOwner> {
        let mut commits = Vec::new();
        let mut committing = Vec::new();
        for _ in 0..committers.max(1) {
            let (tx, rx) = mpsc::unbounded_channel();
            let since = Arc::new(AtomicU64::new(0));
            tokio::spawn(committer(state.clone(), rx, ttf.clone(), since.clone()));
            commits.push(tx);
            committing.push(since);
        }
        Arc::new(LocalOwner { state, log, commits, committing })
    }

    fn commit(&self, shard: u32, p: PendingCommit) {
        let _ = self.commits[shard as usize % self.commits.len()].send(p);
    }

    /// When the oldest durable-but-uncommitted batch became durable (µs since
    /// `epoch`, 0 = none): a checkpoint must not pass an uncommitted entry.
    pub fn committing_since_us(&self) -> u64 {
        self.committing.iter().map(|c| c.load(Ordering::Acquire)).filter(|&t| t != 0).min().unwrap_or(0)
    }

    /// Appends a frame the relay made itself (a takedown's `#account`),
    /// with no state delta: the change it announces is already written.
    pub async fn append_own(&self, meta: EventMeta, frame: vlpds::events::Frame) -> Result<Durable, LogError> {
        let t = self.log.submit(vec![seq::Event { meta, frame: Box::new(frame), delta: None }]).await;
        let (tx, rx) = oneshot::channel();
        self.commit(0, PendingCommit { durable: t, ticket: None, tx, received: Instant::now() });
        rx.await.unwrap_or(Err(LogError::Closed))
    }
}

fn state_rejection(e: &state::Reject) -> Rejection {
    use state::Reject as R;
    let reason = match e {
        R::Stale { .. } => "stale",
        R::WrongHost { .. } => "wrong_host",
        R::Inactive(_) => "inactive",
        R::Desynchronized => "desynchronized",
        R::Chain(state::ChainError::RevNotNewer { .. }) => "rev_not_newer",
        R::Chain(state::ChainError::PrevDataMismatch) => "prev_data_mismatch",
        R::Chain(_) => "chain",
        R::RateLimited { .. } => "rate_limited",
        R::NewAccountDeferred => "new_account_deferred",
        R::NoIdentity => "no_identity",
        R::BadCid => "bad_cid",
        R::NotOwner(_) => "not_owner",
        R::Identity(_) => "identity_unavailable",
        R::Store(_) => "store",
    };
    Rejection { reason, detail: e.to_string() }
}

#[async_trait::async_trait]
impl DidOwner for LocalOwner {
    async fn submit(&self, c: Checked) -> Submitted {
        let t0 = Instant::now();
        let mut tries = 0u32;
        let r = loop {
            let kind = match &c.kind {
                CheckedKind::Commit(v) => EventKind::Commit(v.clone()),
                CheckedKind::Sync(v) => EventKind::Sync { rev: v.rev, commit: v.commit, data: v.data },
                CheckedKind::Identity => EventKind::Identity,
                CheckedKind::Account { active, status } => {
                    EventKind::Account { active: *active, status: status.clone() }
                }
            };
            let ev = Incoming { did: &c.did, host: &c.host, now: state::now_secs(), kind };
            match self.state.apply_with_frame(ev, Some(&c.frame)).await {
                Err(e) if e.retryable() && tries < 3 => {
                    tries += 1;
                    tokio::time::sleep(Duration::from_millis(100 << (2 * tries))).await;
                }
                r => break r,
            }
        };
        metrics::APPLY.busy(t0.elapsed());
        match r {
            Ok(Applied::Append(a)) => {
                let meta =
                    EventMeta { did: c.did, host: c.host, upstream_seq: c.upstream_seq, shard: a.ticket.shard.0 };
                let ev = seq::Event {
                    meta,
                    frame: Box::new(Spliced { frame: c.frame, span: c.span }),
                    delta: Some(Bytes::from(a.delta.encode())),
                };
                let durable = self.log.submit(vec![ev]).await;
                let (tx, rx) = oneshot::channel();
                self.commit(
                    a.ticket.shard.0,
                    PendingCommit { durable, ticket: Some(a.ticket), tx, received: c.received },
                );
                Submitted::Appended(rx)
            }
            // A #sync may restate the head the last #commit left (a
            // reactivation does): it's news to consumers, not a replay,
            // unless this very upstream seq was seen before.
            Ok(Applied::Duplicate) if matches!(c.kind, CheckedKind::Sync(_)) && c.first_sighting => {
                let shard = self.state.shard_id_of_slot(vlpds::slots::slot_of(&c.did));
                let meta = EventMeta { did: c.did, host: c.host, upstream_seq: c.upstream_seq, shard: shard.0 };
                let ev = seq::Event { meta, frame: Box::new(Spliced { frame: c.frame, span: c.span }), delta: None };
                let durable = self.log.submit(vec![ev]).await;
                let (tx, rx) = oneshot::channel();
                self.commit(shard.0, PendingCommit { durable, ticket: None, tx, received: c.received });
                Submitted::Appended(rx)
            }
            Ok(Applied::Duplicate) => Submitted::Duplicate,
            Err(state::Reject::Stale { .. }) => Submitted::Duplicate,
            Err(e) => Submitted::Rejected(state_rejection(&e)),
        }
    }
}

fn mono_us(epoch: Instant) -> u64 {
    epoch.elapsed().as_micros() as u64 + 1
}

/// Waits for appended events to be durable, in append order, and commits
/// their state changes in batches before reporting them done.
async fn committer(
    state: Arc<State>,
    mut rx: mpsc::UnboundedReceiver<PendingCommit>,
    ttf: Arc<Ttf>,
    since: Arc<AtomicU64>,
) {
    use futures::StreamExt;
    use futures::stream::FuturesOrdered;
    type Done = (Result<Durable, LogError>, Option<state::Ticket>, oneshot::Sender<Result<Durable, LogError>>, Instant);
    let epoch = *EPOCH;
    let mut q: FuturesOrdered<futures::future::BoxFuture<'static, Done>> = FuturesOrdered::new();
    let mut open = true;
    loop {
        tokio::select! {
            p = rx.recv(), if open => match p {
                Some(p) => q.push_back(Box::pin(async move { (p.durable.await, p.ticket, p.tx, p.received) })),
                None => open = false,
            },
            Some(first) = q.next(), if !q.is_empty() => {
                since.store(mono_us(epoch), Ordering::Release);
                let mut batch = vec![first];
                while let Some(Some(d)) = q.next().now_or_never() {
                    batch.push(d);
                }
                let tickets: Vec<state::Ticket> =
                    batch.iter().filter(|d| d.0.is_ok()).filter_map(|d| d.1).collect();
                if let Err(e) = state.commit(&tickets).await {
                    vlpds::lifecycle::fail_stop(4, &format!("state commit failed: {e}"));
                }
                since.store(0, Ordering::Release);
                ttf.durable_batch(batch.iter().filter_map(|(r, _, _, received)| {
                    r.as_ref().ok().map(|d| (d.seqs.as_slice(), *received))
                }));
                let now = Instant::now();
                for (r, _, tx, received) in batch {
                    if r.is_ok() {
                        metrics::TIME_TO_DURABLE.observe(now.saturating_duration_since(received).as_secs_f64());
                    }
                    let _ = tx.send(r);
                }
            },
            else => return,
        }
    }
}

/// How long the host stage retries a DID lookup that fails for a reason
/// other than the DID not existing.
const IDENTITY_PATIENCE: Duration = Duration::from_secs(30);

static EPOCH: std::sync::LazyLock<Instant> = std::sync::LazyLock::new(Instant::now);

struct Job {
    frame: UpstreamFrame,
    received: Instant,
    first: bool,
    fence: crate::cluster::forward::Fence,
}

pub struct Node {
    pub cfg: NodeConfig,
    pub store: Store,
    pub manager: Arc<Manager>,
    pub crawler: Arc<upstream::Crawler>,
    pub state: Arc<State>,
    pub identity: Arc<IdentityCache<HttpFetch>>,
    pub log: Arc<NodeLog>,
    pub serve: Arc<Serve>,
    pub local: Arc<LocalOwner>,
    pub owner: Arc<dyn DidOwner>,
    pub acks: acks::Tracker,
    pub ttf: Arc<Ttf>,
    pub dash: Mutex<Dash>,
    pub rejects: Mutex<HashMap<Host, HostRejects>>,
    pub policy: Option<Arc<policy::PolicyHooks>>,
    /// (host, upstream seq) pairs already in an earlier log past the host's
    /// durable cursor: the replay after a restart drops them.
    replayed: Mutex<HashMap<Host, HashSet<i64>>>,
    /// Per host: sockets below this epoch are fenced (`forward::Fence`).
    fences: Mutex<crate::types::FastMap<Host, Arc<AtomicU64>>>,
    lanes: Vec<mpsc::Sender<Job>>,
    pub ingest: tokio::runtime::Handle,
    pub started_ms: i64,
    pub recovery: Mutex<RecoveryReport>,
    /// A core cluster node's cluster half (docs/cluster.md).
    pub cluster: Option<Arc<cluster::Glue>>,
    /// The PLC export reader, when `--plc-export` is on (on a cluster, every
    /// core has one and the lowest-named live core runs it).
    pub plc_ingest: std::sync::OnceLock<Arc<crate::plc_seed::ingest::Ingester>>,
}

#[derive(Clone, Debug, Default, serde::Serialize)]
pub struct RecoveryReport {
    pub logs: usize,
    pub replayed_deltas: usize,
    pub logged_past_cursor: usize,
    pub took_ms: u64,
}

/// Scheme for each `--host` given as a URL, so a dev upstream on
/// `http://127.0.0.1:2984` is dialed with `ws://`.
fn endpoint_fn(dev_mode: bool, explicit: HashMap<Host, String>) -> upstream::EndpointFn {
    Arc::new(move |h: &Host| {
        if let Some(base) = explicit.get(h) {
            return base.clone();
        }
        let name = h.0.rsplit_once(':').map_or(h.0.as_str(), |(n, _)| n);
        let local = name == "localhost" || name.parse::<std::net::IpAddr>().is_ok() || name.starts_with('[');
        if dev_mode && local { format!("http://{}", h.0) } else { format!("https://{}", h.0) }
    })
}

/// The `--host` upstreams: each normalized, and its scheme kept.
fn cli_hosts(cfg: &NodeConfig) -> anyhow::Result<(HashMap<Host, String>, Vec<Host>)> {
    let mut explicit = HashMap::new();
    let mut cli_hosts = Vec::new();
    for h in &cfg.hosts {
        let host = upstream::normalize_hostname(h, cfg.dev_mode).map_err(|e| anyhow::anyhow!("--host {h}: {e}"))?;
        let base = if h.starts_with("http://") || h.starts_with("ws://") {
            anyhow::ensure!(cfg.dev_mode, "--host {h}: plain http needs --dev-mode");
            format!("http://{}", host.0)
        } else {
            format!("https://{}", host.0)
        };
        explicit.insert(host.clone(), base);
        cli_hosts.push(host);
    }
    Ok((explicit, cli_hosts))
}

impl Node {
    /// Recovers from the bucket, connects the upstreams and starts the
    /// pipeline. The firehose and every API are ready on return.
    pub async fn start(store: Store, cfg: NodeConfig) -> anyhow::Result<Arc<Node>> {
        let t0 = Instant::now();
        let identity = Arc::new(IdentityCache::new(HttpFetch::new(&cfg.plc_url, cfg.dev_mode), cfg.identity.clone()));
        let layout = vlpds::slots::Layout::uniform(cfg.did_shards).shards;
        let state = Arc::new(StateStore::new(
            store.clone(),
            layout.clone(),
            VerifyChain,
            Arc::new(CacheIdentity(identity.clone())),
            state::ApplyConfig::default(),
        ));
        for s in &layout {
            state.open_shard(s.id, None).await?;
        }
        let archive =
            cfg.policy.as_ref().map(|p| crate::archive::wiring::install(&state, p.0.clone(), identity.clone()));
        // after a restart every DID misses the cache: its state record or
        // its export entry fills it, where resolving them all again at
        // --did-lookups-per-sec would take hours
        let seeds = Arc::new(crate::plc_seed::LocalSeeds::new(state.clone(), cfg.identity.ttl));
        identity.set_seeder(Arc::new(crate::plc_seed::SingleNode(seeds.clone())));
        let plc_ingest = cfg.plc_export.as_ref().map(|pc| {
            let sink = Arc::new(crate::plc_seed::LocalSink { seeds, cache: identity.clone() });
            crate::plc_seed::ingest::Ingester::new(pc.clone(), store.clone(), sink)
        });

        let mut lcfg = LogConfig::new(seq::new_log_id(&cfg.node_id));
        lcfg.linger = cfg.linger;
        lcfg.inflight = cfg.log_inflight.max(1);
        lcfg.max_segment_bytes = cfg.max_segment_bytes.max(64 << 10);
        let scfg = cfg.serve_config();
        let on_fatal: seq::OnFatal = Box::new(|e: &LogError| {
            let code = if matches!(e, LogError::LeaseLapsed) { 5 } else { 3 };
            vlpds::lifecycle::fail_stop(code, &format!("node log: {e}"));
        });
        let started = serve::start_single_node(
            store.clone(),
            lcfg,
            scfg,
            Some(vlpds::firehose::runtime(cfg.serve_threads)),
            Some(on_fatal),
        )
        .await?;
        let log = started.log;
        let srv = started.serve;

        // replay every earlier log into the state shards, oldest first
        let replay = LogReplay::new(store.clone());
        let mut logs = started.recovered.logs.clone();
        logs.sort();
        let mut report = RecoveryReport { logs: logs.len(), ..Default::default() };
        for (log_id, fence_ord, _) in &logs {
            let mut from = u64::MAX;
            for s in state.shards() {
                from = from.min(s.applied_marker(log_id).await?.map_or(0, |a| a + 1));
            }
            if from >= *fence_ord {
                continue;
            }
            replay.read(log_id, from).await?;
            for s in state.shards() {
                report.replayed_deltas += state.recover(s.id, log_id, &replay, state::now_secs()).await?;
                if *fence_ord > 0 {
                    s.checkpoint(log_id, fence_ord - 1).await?;
                }
            }
        }
        let mut cursors: HashMap<Host, i64> = HashMap::new();
        {
            use crate::state::HostStore as _;
            let mut c: Option<String> = None;
            loop {
                let page = state.list_hosts(c.as_deref(), 1000).await?;
                for h in &page.hosts {
                    cursors.insert(Host(h.hostname.clone()), h.cursor);
                }
                match page.cursor {
                    Some(x) => c = Some(x),
                    None => break,
                }
            }
        }
        let replayed = replay.logged_above(|h| cursors.get(h).copied().unwrap_or(0));
        replay.clear();
        report.logged_past_cursor = replayed.values().map(|s| s.len()).sum();
        report.took_ms = t0.elapsed().as_millis() as u64;
        tracing::info!(?report, "recovered");

        let (explicit, cli_hosts) = cli_hosts(&cfg)?;
        let mut ucfg = UpstreamConfig::new(cfg.dev_mode);
        ucfg.endpoint = endpoint_fn(cfg.dev_mode, explicit);
        ucfg.limits = cfg.upstream_limits.clone();
        ucfg.inflight = cfg.inflight;
        // the node's checkpoint tick flushes the registry (after it takes the
        // snapshot its applied markers depend on)
        ucfg.flush_interval = Duration::from_secs(3600);
        let (manager, rx) = Manager::new(ucfg, Arc::new(StateHosts(state.clone())), None);
        let crawler = upstream::Crawler::new(manager.clone(), upstream::CrawlPolicy::default());
        let hooks = cfg.policy.as_ref().map(|p| policy::PolicyHooks::new(p.0.clone(), state.clone(), cfg.dev_mode));
        if let Some(h) = &hooks {
            h.install(&manager, &crawler, &identity);
            h.load().await?;
            if let Some((_, g)) = &archive {
                let _ = g.hooks.set(h.clone());
            }
        }

        let ttf = Arc::new(Ttf::default());
        let local = LocalOwner::start(state.clone(), log.clone(), ttf.clone(), cfg.did_shards as usize);
        let owner: Arc<dyn DidOwner> = local.clone();
        let cli_tier = cfg.cli_host_tier;
        let node = Node::assemble(
            cfg,
            store,
            manager.clone(),
            crawler,
            state,
            identity,
            log,
            srv,
            local,
            owner,
            ttf,
            replayed,
            report,
            hooks.clone(),
            None,
            rx,
        )?;
        if let Some(ing) = plc_ingest {
            let _ = node.plc_ingest.set(ing.clone());
            tokio::spawn(ing.supervise(Arc::new(|| true)));
        }
        manager.start().await?;
        for h in manager.hosts() {
            tracing::info!(host = %h.record.hostname, acked = ?h.record.acked_seq, tier = h.record.tier.as_str(), "upstream resumes");
        }
        for h in cli_hosts {
            manager.admit(&h, cli_tier).await?;
        }
        if let Some(h) = &hooks {
            h.spawn();
        }
        Ok(node)
    }

    /// The pipeline around the parts `start` (or `start_cluster`) built:
    /// the ingest runtime, the lanes, the dispatcher and the background
    /// loops.
    #[allow(clippy::too_many_arguments)]
    fn assemble(
        cfg: NodeConfig,
        store: Store,
        manager: Arc<Manager>,
        crawler: Arc<upstream::Crawler>,
        state: Arc<State>,
        identity: Arc<IdentityCache<HttpFetch>>,
        log: Arc<NodeLog>,
        srv: Arc<Serve>,
        local: Arc<LocalOwner>,
        owner: Arc<dyn DidOwner>,
        ttf: Arc<Ttf>,
        replayed: HashMap<Host, HashSet<i64>>,
        report: RecoveryReport,
        hooks: Option<Arc<policy::PolicyHooks>>,
        cluster: Option<Arc<cluster::Glue>>,
        rx: mpsc::Receiver<UpstreamFrame>,
    ) -> anyhow::Result<Arc<Node>> {
        let ingest = tokio::runtime::Builder::new_multi_thread()
            .worker_threads(cfg.ingest_threads)
            .thread_name("ingest")
            .enable_all()
            .build()?;
        let ingest_handle = ingest.handle().clone();
        // lives as long as the process
        std::mem::forget(ingest);

        let mut lane_tx = Vec::new();
        let mut lane_rx = Vec::new();
        for _ in 0..cfg.lanes.max(1) {
            let (tx, rx) = mpsc::channel(256);
            lane_tx.push(tx);
            lane_rx.push(rx);
        }
        let node = Arc::new(Node {
            cfg: cfg.clone(),
            store,
            manager: manager.clone(),
            crawler,
            state,
            identity,
            log,
            serve: srv,
            owner,
            local,
            acks: acks::Tracker::default(),
            ttf,
            dash: Mutex::new(Dash::default()),
            rejects: Mutex::new(HashMap::new()),
            policy: hooks.clone(),
            replayed: Mutex::new(replayed),
            fences: Mutex::new(Default::default()),
            lanes: lane_tx,
            ingest: ingest_handle.clone(),
            started_ms: upstream::host::now_ms() as i64,
            recovery: Mutex::new(report),
            cluster,
            plc_ingest: Default::default(),
        });
        let weak = Arc::downgrade(&node);
        manager.on_connect(Arc::new(move |host: &Host, epoch, cursor, restarted| {
            if let Some(n) = weak.upgrade() {
                n.acks.connected(host, epoch, cursor, restarted);
            }
        }));
        for rx in lane_rx {
            ingest_handle.spawn(node.clone().lane(rx));
        }
        ingest_handle.spawn(node.clone().dispatch(rx));
        tokio::spawn(node.clone().tap());
        // a cluster node checkpoints in node::cluster
        if node.cluster.is_none() {
            tokio::spawn(node.clone().checkpoints());
        }
        tokio::spawn(node.clone().sampler());

        Ok(node)
    }

    async fn dispatch(self: Arc<Self>, mut rx: mpsc::Receiver<UpstreamFrame>) {
        let max = event::Limits::default().max_frame_bytes;
        while let Some(f) = rx.recv().await {
            let received = Instant::now();
            let r = match event::route(&f.frame, max) {
                Ok(r) => r,
                Err(e) => {
                    metrics::EVENTS_IN.with_label_values(&["malformed"]).inc();
                    self.acks.begin_at(&f.host, f.upstream_seq, f.epoch, received);
                    self.reject(&f.host, "", f.upstream_seq, Rejection::verify(e));
                    self.finish(&f.host, f.upstream_seq, f.epoch, None);
                    continue;
                }
            };
            metrics::IN_BY_KIND.inc(r.kind.as_str());
            let first = self.acks.begin_at(&f.host, f.upstream_seq, f.epoch, received);
            let did = match (r.kind, r.did) {
                (Kind::Commit | Kind::Sync | Kind::Identity | Kind::Account, Some(d)) => d,
                (k, _) => {
                    metrics::EVENTS_SKIPPED.with_label_values(&[k.as_str()]).inc();
                    self.finish(&f.host, f.upstream_seq, f.epoch, None);
                    continue;
                }
            };
            if self.already_logged(&f.host, f.upstream_seq) {
                metrics::EVENTS_DUPLICATE.with_label_values(&["restart_log"]).inc();
                self.finish(&f.host, f.upstream_seq, f.epoch, None);
                continue;
            }
            let lane = &self.lanes[lane_of(did, self.lanes.len())];
            let fence = self.fence(&f.host, f.epoch);
            metrics::LANE_QUEUED.inc();
            if lane.send(Job { frame: f, received, first, fence }).await.is_err() {
                return;
            }
        }
    }

    fn already_logged(&self, host: &Host, seq: i64) -> bool {
        let mut r = self.replayed.lock();
        if r.is_empty() {
            return false;
        }
        match r.get_mut(host) {
            Some(s) => {
                let hit = s.remove(&seq);
                if s.is_empty() {
                    r.remove(host);
                }
                hit
            }
            None => false,
        }
    }

    fn finish(&self, host: &Host, seq: i64, epoch: u64, ordinal: Option<u64>) {
        if let Some(acked) = self.acks.finish(host, seq, epoch, ordinal) {
            self.manager.ack(host, acked);
        }
    }

    fn fence(&self, host: &Host, epoch: u64) -> crate::cluster::forward::Fence {
        let below = self.fences.lock().entry(host.clone()).or_default().clone();
        crate::cluster::forward::Fence::new(epoch, below)
    }

    /// Stops what's left in the pipeline from `host`'s current and earlier
    /// sockets: none of it is forwarded, and none of it moves the cursor.
    pub fn fence_host(&self, host: &Host) {
        if let Some(e) = self.manager.registry().get(host) {
            self.fence(host, e.epoch()).trip();
        }
    }

    async fn lane(self: Arc<Self>, mut rx: mpsc::Receiver<Job>) {
        use futures::StreamExt;
        use futures::stream::FuturesUnordered;
        // durable acks of this lane's appended (or forwarded) events, polled
        // between jobs instead of a task per event
        type Done = Option<(Host, i64, u64, u64)>;
        let mut acks: FuturesUnordered<futures::future::BoxFuture<'static, Done>> = FuturesUnordered::new();
        loop {
            let mut job = tokio::select! {
                biased;
                Some(done) = acks.next(), if !acks.is_empty() => {
                    if let Some((host, useq, epoch, ordinal)) = done {
                        self.finish(&host, useq, epoch, Some(ordinal));
                    }
                    continue;
                }
                job = rx.recv() => match job {
                    Some(j) => j,
                    None => break,
                },
            };
            metrics::LANE_QUEUED.dec();
            // held until the event is done, which bounds what's in flight
            let permit = job.frame.permit.take();
            let host = job.frame.host.clone();
            let useq = job.frame.upstream_seq;
            let epoch = job.frame.epoch;
            if !job.fence.live() {
                metrics::EVENTS_FENCED.inc();
                self.acks.fail(&host, useq, epoch);
                continue;
            }
            let checked = match self.check(job).await {
                Ok(Some(c)) => c,
                Ok(None) => {
                    self.finish(&host, useq, epoch, None);
                    continue;
                }
                Err((did, r)) => {
                    self.reject(&host, &did, useq, r);
                    self.finish(&host, useq, epoch, None);
                    continue;
                }
            };
            let did = checked.did.clone();
            let kind = checked.kind.label();
            match self.owner.submit(checked).await {
                Submitted::Appended(rx) => {
                    metrics::ACCEPTED_BY_KIND.inc(kind);
                    if let Some(p) = &self.policy {
                        p.on_accepted(&host.0, &did, kind);
                    }
                    acks.push(
                        async move {
                            let _permit = permit;
                            // a failed log fail-stops the process (on_fatal): no ack
                            match rx.await {
                                Ok(Ok(d)) => Some((host, useq, epoch, d.ordinal)),
                                _ => None,
                            }
                        }
                        .boxed(),
                    );
                }
                Submitted::Duplicate => {
                    metrics::EVENTS_DUPLICATE.with_label_values(&["state"]).inc();
                    self.finish(&host, useq, epoch, None);
                }
                Submitted::Rejected(r) => {
                    self.reject(&host, &did, useq, r);
                    self.finish(&host, useq, epoch, None);
                }
                Submitted::Forwarded(rx) => {
                    let node = self.clone();
                    acks.push(
                        async move {
                            let _permit = permit;
                            node.forwarded(rx, host, did, useq, epoch, kind).await;
                            None
                        }
                        .boxed(),
                    );
                }
            }
        }
        while let Some(done) = acks.next().await {
            if let Some((host, useq, epoch, ordinal)) = done {
                self.finish(&host, useq, epoch, Some(ordinal));
            }
        }
    }

    /// The host owner's stage: strict parse and the stateless checks.
    async fn check(&self, job: Job) -> Result<Option<Checked>, (String, Rejection)> {
        let Job { frame: f, received, first, fence } = job;
        let t0 = Instant::now();
        let ev = cpu(f.frame.len(), || event::parse(f.frame.clone(), &event::Limits::default()))
            .map_err(|r| (String::new(), Rejection::verify(r)))?;
        let parse_us = t0.elapsed();
        let checked = |did: String, kind, frame, span| Checked {
            did,
            host: f.host.clone(),
            upstream_seq: f.upstream_seq,
            kind,
            frame,
            span,
            received,
            first_sighting: first,
            fence: Some(fence.clone()),
        };
        let opts = verify::Options::default();
        let out = match ev {
            event::Event::Commit(c) => {
                let v = self
                    .verified(&c.repo, f.frame.len(), |k| verify::verify_commit_with(&c, k, &opts))
                    .await
                    .map_err(|r| (c.repo.clone(), r))?;
                Some(checked(c.repo.clone(), CheckedKind::Commit(v), c.frame.clone(), c.seq_span))
            }
            event::Event::Sync(s) => {
                let v = self
                    .verified(&s.did, f.frame.len(), |k| verify::verify_sync_with(&s, k, &opts))
                    .await
                    .map_err(|r| (s.did.clone(), r))?;
                Some(checked(s.did.clone(), CheckedKind::Sync(v), s.frame.clone(), s.seq_span))
            }
            event::Event::Identity(i) => {
                // the DID owner re-resolves on #identity; drop the stale copy
                // so the host stage's next lookup is fresh too
                self.identity.invalidate(&i.did);
                Some(checked(i.did.clone(), CheckedKind::Identity, i.frame.clone(), i.seq_span))
            }
            event::Event::Account(a) => Some(checked(
                a.did.clone(),
                CheckedKind::Account { active: a.active, status: a.status.clone() },
                a.frame.clone(),
                a.seq_span,
            )),
            _ => None,
        };
        metrics::PARSE.busy(parse_us);
        Ok(out)
    }

    /// Runs `check` against the DID's signing key, refreshing the key once
    /// when it may be stale.
    async fn verified(
        &self,
        did: &str,
        len: usize,
        check: impl Fn(&SigningKey) -> Result<Verified, Reject>,
    ) -> Result<Verified, Rejection> {
        let mut fresh = false;
        loop {
            let t0 = Instant::now();
            let id = self.lookup(did, fresh).await?;
            let t1 = Instant::now();
            metrics::IDENTITY.wall(t1 - t0);
            let r = match &id.signing_key {
                Some(k) => cpu(len, || check(k)),
                None => Err(Reject::NoSigningKey),
            };
            metrics::VERIFY.busy(t1.elapsed());
            match r {
                Err(e) if e.may_be_stale_key() && !fresh => fresh = true,
                r => return r.map_err(Rejection::verify),
            }
        }
    }

    async fn lookup(&self, did: &str, fresh: bool) -> Result<Arc<Identity>, Rejection> {
        // A rejection is acked, and a lost commit desynchronizes its
        // account: PLC trouble is waited out (holding this lane, and so the
        // host's reads through the in-flight caps) for longer than a blip.
        let t0 = Instant::now();
        let mut tries = 0u32;
        loop {
            match self.identity.lookup_paced(did, fresh).await {
                Ok(id) => return Ok(id),
                Err(e @ (LookupError::NotFound | LookupError::BadDid)) => {
                    return Err(Rejection { reason: "unknown_did", detail: e.to_string() });
                }
                Err(e) if t0.elapsed() >= IDENTITY_PATIENCE => {
                    return Err(Rejection { reason: "identity_unavailable", detail: e.to_string() });
                }
                Err(_) => {
                    tries += 1;
                    tokio::time::sleep(Duration::from_millis(100u64 << (2 * tries.min(4)))).await;
                }
            }
        }
    }

    fn reject(&self, host: &Host, did: &str, useq: i64, r: Rejection) {
        metrics::EVENTS_REJECTED.with_label_values(&[r.reason]).inc();
        if let Some(p) = &self.policy {
            p.on_reject(&host.0, did, r.reason, &r.detail);
        }
        tracing::debug!(host = %host.0, did, useq, reason = r.reason, "rejected: {}", r.detail);
        let mut m = self.rejects.lock();
        let h = m.entry(host.clone()).or_default();
        *h.by_reason.entry(r.reason).or_default() += 1;
        h.total += 1;
        h.recent.push_back(RejectNote {
            at_ms: upstream::host::now_ms() as i64,
            did: did.to_string(),
            reason: r.reason,
            upstream_seq: useq,
            detail: r.detail,
        });
        if h.recent.len() > 50 {
            h.recent.pop_front();
        }
    }

    /// Times each emitted event against its arrival (time to firehose).
    async fn tap(self: Arc<Self>) {
        let fh = self.serve.firehose.clone();
        let mut head = fh.subscribe();
        let mut last = fh.last_emitted.load(std::sync::atomic::Ordering::Acquire);
        while head.changed().await.is_ok() {
            let now = Instant::now();
            let (batches, _) = fh.from_ring(last);
            for b in batches {
                self.ttf.emitted_batch((0..b.events.len()).map(|i| b.key(i)), now);
                metrics::EVENTS_OUT.inc_by(b.events.len() as u64);
                last = b.last;
            }
        }
    }

    /// Host cursors and the shards' applied markers, every interval.
    ///
    /// The marker for this log must not pass an entry whose upstream seq is
    /// above its host's persisted cursor: on restart that entry's upstream
    /// replays it and only the log tail past the marker can say it's a
    /// duplicate. So the marker is computed from the ack state first, and
    /// the cursors (which can only have moved forward since) written after.
    async fn checkpoints(self: Arc<Self>) {
        let every = self.cfg.checkpoint_interval;
        let mut tick = tokio::time::interval(every);
        tick.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
        let mut prev_durable = u64::MAX;
        let mut last_marker: Option<u64> = None;
        loop {
            tick.tick().await;
            let snap = self.acks.snapshot();
            let durable = self.log.durable_ordinal.load(Ordering::Acquire);
            let committing = self.local.committing_since_us();
            let stuck_commit = committing != 0 && mono_us(*EPOCH).saturating_sub(committing) > every.as_micros() as u64;
            let mut marker = (prev_durable != u64::MAX).then_some(prev_durable);
            if let Some(m) = snap.min_ordinal_above_ack {
                marker = marker.and_then(|x| m.checked_sub(1).map(|y| x.min(y)));
            }
            // an event pending this long may be durable below the marker
            // without its host knowing yet
            if snap.oldest_pending.is_some_and(|t| t.elapsed() > every) || stuck_commit {
                marker = None;
            }
            prev_durable = durable;
            if let Err(e) = self.state.flush_host_counts(&*self.state).await {
                tracing::warn!("host counts flush failed: {e:#}");
            }
            if let Err(e) = self.manager.registry().flush().await {
                tracing::warn!("host cursor flush failed: {e:#}");
                continue;
            }
            if let Some(m) = marker
                && last_marker != Some(m)
            {
                let mut ok = true;
                for s in self.state.shards() {
                    if let Err(e) = s.checkpoint(&self.log.log_id, m).await {
                        tracing::warn!(shard = %s.id, "checkpoint failed: {e}");
                        ok = false;
                    }
                }
                if ok {
                    last_marker = Some(m);
                }
            }
            metrics::ACK_PENDING.set(snap.pending as i64);
            if !self.replayed.lock().is_empty() && self.started_ms + 600_000 < upstream::host::now_ms() as i64 {
                self.replayed.lock().clear();
            }
        }
    }

    /// Opens (or adds to) a case for each host whose reader is more than
    /// [`LAG_CASE_MS`] behind, at most every [`LAG_CASE_EVERY`] per host. A
    /// host held to its limits that long is losing ground, and the PDS will
    /// cut the socket with `ConsumerTooSlow` once it's past its own outbox.
    fn lag_cases(&self, lags: &[(&str, i64)], last: &mut HashMap<String, Instant>) {
        let Some(p) = self.policy.clone() else { return };
        last.retain(|_, at| at.elapsed() < LAG_CASE_EVERY);
        for &(host, lag) in lags {
            if lag < LAG_CASE_MS || last.contains_key(host) {
                continue;
            }
            last.insert(host.to_string(), Instant::now());
            let (host, node, engine) = (host.to_string(), self.cfg.node_id.clone(), p.engine.clone());
            tokio::spawn(async move {
                let o = crate::policy::cases::CaseOpen {
                    kind: "read-lag".into(),
                    host: host.clone(),
                    did: None,
                    severity: crate::admin::Severity::High,
                    summary: format!("{host}: reader {} min behind the host's stream", lag / 60_000),
                    observed: lag as f64 / 1000.0,
                    threshold: LAG_CASE_MS as f64 / 1000.0,
                    auto_action: None,
                    evidence: crate::policy::cases::Evidence {
                        at_ms: upstream::host::now_ms() as i64,
                        observed: lag as f64 / 1000.0,
                        threshold: LAG_CASE_MS as f64 / 1000.0,
                        window_secs: 0,
                        node,
                        detail: Some("raise the host's limits or tier, or it will fall out of the PDS's outbox".into()),
                        signals: Default::default(),
                    },
                };
                if let Err(e) = engine.cases.open_or_update(o).await {
                    tracing::warn!(host, "read-lag case not opened: {e:#}");
                }
            });
        }
    }

    /// One sample per second for the dashboard and the gauges.
    async fn sampler(self: Arc<Self>) {
        let mut tick = tokio::time::interval(Duration::from_secs(1));
        tick.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
        let mut prev_in = 0u64;
        let mut prev_out = 0u64;
        let mut prev_bytes_in = 0u64;
        let mut prev_bytes_out = 0u64;
        let mut prev_rejects: HashMap<&'static str, u64> = HashMap::new();
        let mut prev_lat = (0u64, 0u64);
        let mut lag_cased: HashMap<String, Instant> = HashMap::new();
        let mut first = true;
        loop {
            tick.tick().await;
            let t = upstream::host::now_ms() as i64 / 1000;
            let hosts = self.manager.hosts();
            let ev_in: u64 = hosts.iter().map(|h| h.frames).sum();
            let bytes_in: u64 = hosts.iter().map(|h| h.bytes).sum();
            let ev_out = metrics::EVENTS_OUT.get();
            let bytes_out = vlpds::metrics::FIREHOSE_SENT_BYTES.get();
            let mut rejects_now: HashMap<&'static str, u64> = HashMap::new();
            let host_rejects: HashMap<Host, u64> = {
                let r = self.rejects.lock();
                for h in r.values() {
                    for (k, v) in &h.by_reason {
                        *rejects_now.entry(k).or_default() += v;
                    }
                }
                r.iter().map(|(h, x)| (h.clone(), x.total)).collect()
            };
            let (p50, p99) = self.ttf.roll();
            let lat =
                (self.log.stats.latency_us.load(Ordering::Relaxed), self.log.stats.events.load(Ordering::Relaxed));
            let lag_ms = if lat.1 > prev_lat.1 {
                (lat.0 - prev_lat.0) as f64 / (lat.1 - prev_lat.1) as f64 / 1000.0
            } else {
                0.0
            };
            prev_lat = lat;
            metrics::DURABLE_LAG.set(lag_ms as i64);
            let consumers = vlpds::metrics::FIREHOSE_SUBSCRIBERS.get();
            metrics::CONSUMERS.set(consumers);
            let st = &self.identity.stats;
            for (k, v) in [("hit", &st.hits), ("seeded", &st.seeded), ("fetched", &st.fetches)] {
                metrics::IDENTITY_LOOKUPS.with_label_values(&[k]).set(v.load(Ordering::Relaxed) as i64);
            }
            if let Some(ing) = self.plc_ingest.get() {
                let s = &ing.stats;
                for (k, v) in [
                    ("requests", &s.requests),
                    ("pages", &s.pages),
                    ("ops", &s.ops),
                    ("bytes", &s.bytes),
                    ("written", &s.written),
                    ("nullified", &s.nullified),
                    ("invalid", &s.invalid),
                    ("throttled", &s.throttled),
                    ("errors", &s.errors),
                    ("restarts", &s.restarts),
                ] {
                    metrics::PLC_EXPORT.with_label_values(&[k]).set(v.load(Ordering::Relaxed) as i64);
                }
                metrics::PLC_EXPORT.with_label_values(&["caught_up"]).set(s.caught_up.load(Ordering::Relaxed) as i64);
                metrics::PLC_EXPORT
                    .with_label_values(&["newest"])
                    .set((s.newest_ms.load(Ordering::Relaxed) / 1000) as i64);
            }
            metrics::IDENTITY_CACHE.set(self.identity.len() as i64);
            let lags: Vec<(&str, i64)> =
                hosts.iter().filter_map(|h| Some((h.record.hostname.as_str(), h.read_lag_ms?))).collect();
            metrics::HOST_READ_LAG_MAX.set(lags.iter().map(|l| l.1 / 1000).max().unwrap_or(0));
            metrics::HOSTS_LAGGING.set(lags.iter().filter(|l| l.1 > 60_000).count() as i64);
            self.lag_cases(&lags, &mut lag_cased);
            let mut by_status: HashMap<&'static str, i64> = HashMap::new();
            for h in &hosts {
                *by_status.entry(admin::host_status_label(h)).or_default() += 1;
            }
            for s in ["connected", "idle", "backoff", "throttled", "suspended", "banned"] {
                metrics::HOSTS.with_label_values(&[s]).set(by_status.get(s).copied().unwrap_or(0));
            }
            upstream::flow::HOST_INFLIGHT_MAX.set(hosts.iter().map(|h| h.inflight_events).max().unwrap_or(0) as i64);
            let mut d = self.dash.lock();
            for h in &hosts {
                let host = Host(h.record.hostname.clone());
                let rj = host_rejects.get(&host).copied().unwrap_or(0);
                d.hosts.entry(host).or_default().push(t, h.frames, rj);
            }
            if !first {
                let rejects = rejects_now
                    .iter()
                    .map(|(k, v)| (*k, v.saturating_sub(prev_rejects.get(k).copied().unwrap_or(0)) as f64))
                    .collect();
                d.history.push_back(metrics::Sample {
                    t,
                    events_in: ev_in.saturating_sub(prev_in) as f64,
                    events_out: ev_out.saturating_sub(prev_out) as f64,
                    bytes_in: bytes_in.saturating_sub(prev_bytes_in) as f64,
                    bytes_out: bytes_out.saturating_sub(prev_bytes_out) as f64,
                    ttf_p50_ms: p50,
                    ttf_p99_ms: p99,
                    durable_lag_ms: lag_ms,
                    rejects,
                });
                while d.history.len() > metrics::HISTORY {
                    d.history.pop_front();
                }
            }
            first = false;
            prev_in = ev_in;
            prev_out = ev_out;
            prev_bytes_in = bytes_in;
            prev_bytes_out = bytes_out;
            prev_rejects = rejects_now;
        }
    }

    /// Stops reading upstreams and writes their cursors. The log isn't
    /// closed: what's durable stays, and the next start fences it. A
    /// cluster node first hands its host and DID shards over.
    pub async fn shutdown(&self) -> anyhow::Result<()> {
        if let Some(g) = &self.cluster {
            g.cluster.shutdown().await?;
        }
        self.manager.shutdown().await
    }
}

fn lane_of(did: &str, n: usize) -> usize {
    let mut h: u64 = 0xcbf29ce484222325;
    for b in did.bytes() {
        h = (h ^ b as u64).wrapping_mul(0x100000001b3);
    }
    (h % n as u64) as usize
}

/// Big frames (a 2 MB CAR) take milliseconds to hash: off the lane's worker
/// so the runtime keeps polling the other lanes.
fn cpu<T>(len: usize, f: impl FnOnce() -> T) -> T {
    if len > 256 << 10 { tokio::task::block_in_place(f) } else { f() }
}
