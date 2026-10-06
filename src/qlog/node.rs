//! A quorum log node: leader, follower or candidate (docs/quorum.md §1, §3).
//!
//! The leader appends, gives each entry the next seq, replicates it to
//! every follower and commits it once a quorum (itself included) holds it.
//! Every node emits only up to the commit index it knows, so nothing reaches
//! a consumer before a quorum holds it. A follower that hears nothing from
//! the leader for `election_timeout` (or finds its port refused) CASes
//! `qlog/leader` to epoch + 1, collects promises from a quorum, adopts the
//! longest tail among them, re-tags that tail with its epoch and carries on.

use super::bucket::Bucket;
use super::commitlog::{CommitLog, Recovered};
use super::emit::Emitter;
use super::flush;
use super::log::{Entry, Log, Op, encode_cursors, merge_cursors};
use super::wire::{self, Append, AppendResp, Item, Msg, Outcome, PromiseResp};
use bytes::Bytes;
use futures::future::BoxFuture;
use object_store::path::Path;
use object_store::{ObjectStoreExt, PutMode, PutOptions, PutPayload, UpdateVersion};
use parking_lot::Mutex;
use serde::{Deserialize, Serialize};
use std::collections::{BTreeMap, HashMap, HashSet, VecDeque};
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{Duration, Instant};
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::{mpsc, oneshot, watch};
use vlpds::store::Store;

#[derive(Clone, Debug)]
pub struct Config {
    pub id: String,
    /// The bootstrap member set, this node included: what the first
    /// `qlog/leader` CAS writes. From then on the set recorded there at the
    /// current epoch is the one that votes and counts (`Core::members`).
    pub members: Vec<String>,
    /// Where to dial each other node (members, learners, or nodes that may
    /// become either); an address in `qlog/leader` fills in the rest.
    pub peers: HashMap<String, String>,
    pub heartbeat: Duration,
    pub election_timeout: Duration,
    /// Silence from the leader after which it's pinged: a refused port
    /// starts a takeover at once.
    pub probe_after: Duration,
    /// Between successive candidates (by rank), so two rarely race the CAS.
    pub stagger: Duration,
    pub rpc_timeout: Duration,
    pub max_batch_bytes: usize,
    /// Committed and emitted entries kept in memory past this are dropped,
    /// oldest first (older ones come from the commitlog, if there is one; a
    /// follower behind both is reset to the base).
    pub retain_bytes: usize,
    /// The leader takes no new submits while this much is uncommitted:
    /// without it, a quorum slower than the submitters (a saturated disk)
    /// grows the leader's memory without bound.
    pub max_pending_bytes: usize,
    /// The bucket flush (segments, state at F, manifest); None: nothing is
    /// flushed and nothing caps the commit index.
    pub flush: Option<flush::Options>,
    /// A follower heard from within this long still holds the leader's disk
    /// trimming back to what it has matched.
    pub laggard_grace: Duration,
    /// Run a bucket recovery as soon as the promise round proves no quorum
    /// of intact logs can exist (see `try_takeover`). False: a candidate
    /// that finds that only logs it and keeps retrying (an operator
    /// restarts one member with it on).
    pub auto_recover: bool,
    /// A membership change waits this long for its learners to catch up...
    pub catch_up_timeout: Duration,
    /// ...and this long, with commits paused, for everything appended to
    /// commit, the learners to hold it and the flush to cover it.
    pub switch_timeout: Duration,
    /// What the relay plugs in (`SubmitEvents`, the state, every committed
    /// entry); None for a bare log.
    pub hooks: HooksSlot,
    /// The bearer token a membership change sent over the peer protocol
    /// (`Ask members`) must carry; None refuses them there.
    pub admin_token: Option<String>,
}

/// What the relay plugs into the log (docs/quorum.md, "The relay on the
/// log"). All but `committed` and `answer` run on the leader only, for its
/// `epoch`.
pub trait Hooks: Send + Sync + 'static {
    /// Decides each submitted event before it's appended, in order. Runs
    /// while no membership barrier can begin, and its `Verdict::Append`s are
    /// appended in this order, unless the node stops leading `epoch` first
    /// (then nothing is, and the term's decisions are void). `control` is
    /// the submitter's, for the hooks.
    fn admit<'a>(&'a self, epoch: u64, items: &'a [Item], control: Bytes) -> BoxFuture<'a, Admission>;
    /// Where each `Verdict::Append` went: (ticket, seq).
    fn appended(&self, epoch: u64, seqs: Vec<(u64, u64)>);
    /// The state is open, applied up to `applied`, and nothing has been
    /// admitted yet this term: `node.read_tail` gives what's above it.
    fn state_opened<'a>(
        &'a self,
        node: &'a Arc<Node>,
        epoch: u64,
        db: slatedb::Db,
        applied: u64,
    ) -> BoxFuture<'a, anyhow::Result<()>>;
    /// Everything up to `upto` is written to the state.
    fn state_applied(&self, epoch: u64, upto: u64);
    /// The term's flush loop stopped (the node no longer leads `epoch`).
    fn term_ended(&self, epoch: u64);
    /// Every node: committed entries in seq order, as they're emitted (a
    /// bucket recovery's catch-up included).
    fn committed(&self, entries: &[Entry]);
    /// `Msg::Ask` topics the node doesn't answer itself.
    fn answer<'a>(&'a self, topic: &'a str, body: Bytes) -> BoxFuture<'a, Option<Bytes>>;
    /// For the node's status.
    fn report(&self) -> serde_json::Value {
        serde_json::Value::Null
    }
}

/// The hooks' decisions on a batch, in order, and what they hold until the
/// batch is appended (or given up): what they decided must reach the log in
/// the order they decided it, which a concurrent batch could break.
pub struct Admission {
    pub verdicts: Vec<Verdict>,
    pub hold: Option<Box<dyn std::any::Any + Send>>,
}

/// The hooks' decision on one submitted event.
pub enum Verdict {
    /// Append it with this meta; `ticket` comes back from `appended`.
    Append { meta: Bytes, ticket: u64 },
    /// Answer with this once the entry at `after` (already in the log, if
    /// any) has committed.
    Answer { outcome: Outcome, after: Option<u64> },
}

#[derive(Clone, Default)]
pub struct HooksSlot(pub Option<Arc<dyn Hooks>>);

impl std::fmt::Debug for HooksSlot {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(if self.0.is_some() { "Hooks" } else { "None" })
    }
}

impl Config {
    pub fn new(id: &str, peers: HashMap<String, String>) -> Config {
        let mut members: Vec<String> = peers.keys().cloned().chain([id.to_string()]).collect();
        members.sort();
        Config {
            id: id.to_string(),
            members,
            peers,
            heartbeat: Duration::from_millis(100),
            election_timeout: Duration::from_millis(1000),
            probe_after: Duration::from_millis(300),
            stagger: Duration::from_millis(500),
            rpc_timeout: Duration::from_millis(500),
            max_batch_bytes: 4 << 20,
            retain_bytes: 512 << 20,
            max_pending_bytes: 256 << 20,
            flush: None,
            laggard_grace: Duration::from_secs(10),
            auto_recover: true,
            catch_up_timeout: Duration::from_secs(300),
            switch_timeout: Duration::from_secs(10),
            hooks: HooksSlot::default(),
            admin_token: None,
        }
    }
}

pub fn quorum(members: usize) -> usize {
    members / 2 + 1
}

/// `qlog/leader`: one leader per epoch, decided by the bucket's CAS.
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
pub struct LeaderRecord {
    pub epoch: u64,
    pub leader: String,
    /// The voters at this epoch: quorums, promise rounds and the lost-quorum
    /// trigger count these and nothing else.
    pub members: Vec<String>,
    /// Nodes the leader replicates to that don't count until a membership
    /// change makes them members (at epoch + 1).
    #[serde(default)]
    pub learners: Vec<String>,
    /// Addresses an operator gave for nodes this cluster's `--peer` flags
    /// may not name.
    #[serde(default)]
    pub addrs: BTreeMap<String, String>,
    /// The epoch `members` took effect at (the bootstrap's, or a change's
    /// epoch + 1): a member removed then never holds or promises it.
    #[serde(default)]
    pub since: u64,
}

fn leader_path(store: &Store) -> Path {
    Path::from(format!("{}/qlog/leader", store.prefix))
}

pub async fn read_leader(store: &Store) -> anyhow::Result<Option<(LeaderRecord, Option<String>)>> {
    match store.raw.get(&leader_path(store)).await {
        Ok(r) => {
            let etag = r.meta.e_tag.clone();
            Ok(Some((serde_json::from_slice(&r.bytes().await?)?, etag)))
        }
        Err(object_store::Error::NotFound { .. }) => Ok(None),
        Err(e) => Err(e.into()),
    }
}

/// Writes `rec` if `qlog/leader` is still the version read (`None`: absent).
pub async fn cas_leader(store: &Store, rec: &LeaderRecord, read: Option<Option<String>>) -> anyhow::Result<bool> {
    let mode = match read {
        None => PutMode::Create,
        Some(e_tag) => PutMode::Update(UpdateVersion { e_tag, version: None }),
    };
    let body = PutPayload::from(serde_json::to_vec(rec)?);
    match store.raw.put_opts(&leader_path(store), body, PutOptions { mode, ..Default::default() }).await {
        Ok(_) => Ok(true),
        Err(object_store::Error::Precondition { .. } | object_store::Error::AlreadyExists { .. }) => Ok(false),
        Err(e) => Err(e.into()),
    }
}

/// Where the log's changes go before its holder acks them (a follower),
/// counts itself toward the quorum (the leader) or answers a promise.
/// Memory-only acks at once; the commitlog writes and group-fsyncs.
///
/// `stage` is called under the node's lock, in the order the changes were
/// made, so the disk replays them in that order; `wait` is awaited outside
/// it.
pub trait Durability: Send + Sync + 'static {
    /// Whether the log should journal its changes for `stage`.
    fn journaling(&self) -> bool;
    fn stage(&self, ops: Vec<Op>) -> u64;
    /// The ticket of the last stage: waiting for it covers every op so far.
    fn staged(&self) -> u64;
    fn wait(&self, ticket: u64) -> BoxFuture<'_, anyhow::Result<()>>;
    fn note_commit(&self, _seq: u64) {}
    /// Everything at or below `seq` may leave local disk (once over budget).
    fn set_floor(&self, _seq: u64) {}
    /// The last seq readable back with `read`: in-memory trimming stays at
    /// or below it, so a lagging follower can always be served.
    fn written_last(&self) -> u64 {
        u64::MAX
    }
    /// Entries above this are readable with `read` (None: no disk).
    fn first_readable(&self) -> Option<u64> {
        None
    }
    /// Committed entries from `from` (see `CommitLog::read`), off the
    /// async runtime.
    fn read(&self, _from: u64, _upto: u64, _max_bytes: usize) -> BoxFuture<'_, Option<(u64, Vec<Entry>)>> {
        Box::pin(std::future::ready(None))
    }
    fn report(&self, _reset: bool) -> Option<DiskStatus> {
        None
    }
}

#[derive(Clone, Debug, Default, Serialize)]
pub struct DiskStatus {
    pub fsyncs: u64,
    pub fsync_us: Quantiles,
    /// Ops per group commit.
    pub batch_ops: Quantiles,
    pub bytes_written: u64,
    pub disk_bytes: u64,
    pub rollovers: u64,
    pub deleted: u64,
}

pub struct MemoryOnly;

impl Durability for MemoryOnly {
    fn journaling(&self) -> bool {
        false
    }
    fn stage(&self, _: Vec<Op>) -> u64 {
        0
    }
    fn staged(&self) -> u64 {
        0
    }
    fn wait(&self, _: u64) -> BoxFuture<'_, anyhow::Result<()>> {
        Box::pin(std::future::ready(Ok(())))
    }
}

impl Durability for Arc<CommitLog> {
    fn journaling(&self) -> bool {
        true
    }
    fn stage(&self, ops: Vec<Op>) -> u64 {
        CommitLog::stage(self, ops)
    }
    fn staged(&self) -> u64 {
        CommitLog::staged(self)
    }
    fn wait(&self, ticket: u64) -> BoxFuture<'_, anyhow::Result<()>> {
        Box::pin(CommitLog::wait(self, ticket))
    }
    fn note_commit(&self, seq: u64) {
        CommitLog::note_commit(self, seq)
    }
    fn set_floor(&self, seq: u64) {
        CommitLog::set_floor(self, seq)
    }
    fn written_last(&self) -> u64 {
        CommitLog::written_last(self)
    }
    fn first_readable(&self) -> Option<u64> {
        Some(CommitLog::first_readable(self))
    }
    fn report(&self, reset: bool) -> Option<DiskStatus> {
        let st = &self.stats;
        let r = DiskStatus {
            fsyncs: st.fsyncs.load(Ordering::Relaxed),
            fsync_us: Quantiles::of(&st.fsync_us.lock()),
            batch_ops: Quantiles::of(&st.batch_ops.lock()),
            bytes_written: st.bytes.load(Ordering::Relaxed),
            disk_bytes: self.disk_bytes(),
            rollovers: st.rollovers.load(Ordering::Relaxed),
            deleted: st.deleted.load(Ordering::Relaxed),
        };
        if reset {
            st.fsync_us.lock().reset();
            st.batch_ops.lock().reset();
        }
        Some(r)
    }
    fn read(&self, from: u64, upto: u64, max_bytes: usize) -> BoxFuture<'_, Option<(u64, Vec<Entry>)>> {
        let cl = self.clone();
        Box::pin(async move {
            tokio::task::spawn_blocking(move || CommitLog::read(&cl, from, upto, max_bytes)).await.ok().flatten()
        })
    }
}

/// What a staged change waits on, and what it makes durable.
#[derive(Clone, Copy, Debug)]
struct Ticket {
    n: u64,
    last: u64,
    cut: u64,
}

/// In-process partitions for tests: requests to and from a blocked peer
/// are dropped (a blackhole, not a refusal).
#[derive(Default)]
pub struct Faults {
    blocked: parking_lot::RwLock<HashSet<String>>,
}

impl Faults {
    pub fn block(&self, peers: &[&str]) {
        self.blocked.write().extend(peers.iter().map(|p| p.to_string()));
    }
    pub fn heal(&self) {
        self.blocked.write().clear();
    }
    pub fn blocked(&self, peer: &str) -> bool {
        self.blocked.read().contains(peer)
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize)]
#[serde(rename_all = "lowercase")]
pub enum Role {
    Follower,
    Candidate,
    Leader,
}

struct Core {
    log: Log,
    /// The highest epoch seen or promised: appends and promises below it
    /// are refused, which is what fences a deposed leader's memory.
    promised: u64,
    role: Role,
    /// The epoch of the leader followed (ours as leader or candidate).
    epoch: u64,
    leader: Option<String>,
    last_heard: Instant,
    /// The newest `qlog/leader` epoch a takeover attempt has seen: a newer
    /// one gets a timeout's grace to show up before it's taken over too.
    seen_record: u64,
    electing: bool,
    probing: bool,
    /// No new takeover attempt before this (after one that went nowhere).
    retry_at: Instant,
    /// The silence (by its `last_heard`) the ticker last probed the leader in.
    probed_for: Option<Instant>,
    /// Holds every entry it ever acked. Memory-only: false after a restart
    /// (it may have acked entries it no longer has) until it has caught up
    /// to the first leader's last seq it hears (see `on_append`). Only
    /// intact nodes count toward a takeover's quorum.
    intact: bool,
    need_upto: Option<u64>,
    emitted: u64,
    /// This node's log is on its own disk up to here: it emits no further,
    /// so a power cut never leaves it behind what its consumers saw.
    durable: u64,
    /// Bumped by every truncation, which lowers `durable`.
    cut: u64,
    // leader only
    matched: HashMap<String, u64>,
    next: HashMap<String, u64>,
    acked_at: HashMap<String, Instant>,
    self_durable: u64,
    /// Submits waiting for their last seq to commit.
    waiters: BTreeMap<u64, Vec<oneshot::Sender<Result<(), String>>>>,
    /// Leader: `SubmitEvents` between their capacity check and their
    /// append (a membership barrier waits for none).
    admitting: usize,
    /// (first, last, appended at) per submit, for the commit latency.
    pending: VecDeque<(u64, u64, Instant, usize)>,
    pending_bytes: usize,
    /// The last committed manifest's F as this node knows it: its log keeps
    /// everything above it (a takeover flushes and replays state from there).
    flushed: u64,
    /// The last committed manifest's R as known: the leader commits nothing
    /// above it, so after a lost quorum seqs resume above anything emitted.
    reserve: u64,
    /// Submitted cursors waiting for an entry to ride on.
    pending_cursors: BTreeMap<String, u64>,
    /// Bucket recoveries so far, as known (the manifest's generation; also
    /// learned from appends and promises).
    generation: u64,
    /// The cursors the last recovery left each host at, if known, with its
    /// generation: host owners re-read their hosts from these.
    recovery_cursors: Option<(u64, Bytes)>,
    /// Leader: followers whose log is empty (a new or wiped disk, or a
    /// memory-only restart). They start at the leader's oldest local entry
    /// rather than replaying the bucket from seq 1: nothing they emitted
    /// is behind it.
    fresh: HashSet<String>,
    /// The member set at `epoch` as last read from `qlog/leader` (or set by
    /// this node's own change): only these vote and count.
    members: Vec<String>,
    learners: Vec<String>,
    members_since: u64,
    /// Leader: replicas whose last answer said their log is intact.
    peer_intact: HashSet<String>,
    /// Leader: a membership change holds new appends at its barrier.
    paused: bool,
    switching: bool,
    /// `qlog/leader` no longer names this node: it doesn't campaign until a
    /// leader appends to it again (it was added back).
    retired: bool,
}

pub struct Stats {
    /// Leader: append to quorum commit, per event, µs.
    pub commit_us: Mutex<hdrhistogram::Histogram<u64>>,
    pub appended: AtomicU64,
    pub takeovers: AtomicU64,
    pub step_downs: AtomicU64,
    pub resets: AtomicU64,
    /// Seqs a node skipped emitting because it was reset past them.
    pub emit_gaps: AtomicU64,
    pub promise_rounds: AtomicU64,
    /// Batches served from the commitlog (a follower or a candidate behind
    /// what's in memory).
    pub disk_reads: AtomicU64,
    /// Batches a lagging follower was served from the bucket segments.
    pub bucket_reads: AtomicU64,
    /// Bucket recoveries this node ran, and the promise rounds that found no
    /// quorum of intact logs could exist.
    pub recoveries: AtomicU64,
    pub lost_quorums: AtomicU64,
}

impl Default for Stats {
    fn default() -> Self {
        Stats {
            commit_us: Mutex::new(hdrhistogram::Histogram::new_with_bounds(1, 120_000_000, 3).expect("bounds")),
            appended: AtomicU64::new(0),
            takeovers: AtomicU64::new(0),
            step_downs: AtomicU64::new(0),
            resets: AtomicU64::new(0),
            emit_gaps: AtomicU64::new(0),
            promise_rounds: AtomicU64::new(0),
            disk_reads: AtomicU64::new(0),
            bucket_reads: AtomicU64::new(0),
            recoveries: AtomicU64::new(0),
            lost_quorums: AtomicU64::new(0),
        }
    }
}

#[derive(Clone, Debug, Serialize)]
pub struct Status {
    pub id: String,
    pub role: Role,
    pub epoch: u64,
    pub promised: u64,
    pub leader: Option<String>,
    pub base: u64,
    pub last: u64,
    pub commit: u64,
    pub emitted: u64,
    pub intact: bool,
    pub log_bytes: usize,
    pub appended: u64,
    pub takeovers: u64,
    pub step_downs: u64,
    pub resets: u64,
    pub emit_gaps: u64,
    pub promise_rounds: u64,
    pub disk_reads: u64,
    pub bucket_reads: u64,
    pub commit_us: Quantiles,
    pub disk: Option<DiskStatus>,
    pub flushed: u64,
    pub reserve: u64,
    pub flush: Option<flush::Status>,
    pub generation: u64,
    pub recoveries: u64,
    pub lost_quorums: u64,
    /// The bucket recoveries this node ran, with their timings.
    pub recovered: Vec<flush::RecoveryStats>,
    pub members: Vec<String>,
    pub learners: Vec<String>,
    pub members_since: u64,
    pub retired: bool,
    pub paused: bool,
    /// The epoch of the last entry held (a removed member never holds one
    /// from after its removal).
    pub last_epoch: u64,
    /// The membership changes this node ran as leader, with their timings.
    pub switches: Vec<SwitchStats>,
    /// Every bucket request this process has sent, by R2 class, purpose and
    /// key component (`bucket::requests`).
    pub requests: super::bucket::Requests,
    /// What the hooks report (the relay's admissions, its host table).
    #[serde(skip_serializing_if = "Option::is_none")]
    pub relay: Option<serde_json::Value>,
}

/// One membership change, as the leader that ran it saw it.
#[derive(Clone, Debug, Default, Serialize)]
pub struct SwitchStats {
    pub from_epoch: u64,
    pub epoch: u64,
    pub from: Vec<String>,
    pub to: Vec<String>,
    /// Who leads `epoch`: this node, or the member it handed off to.
    pub leader: String,
    /// Recording the learners in `qlog/leader`.
    pub record_ms: u64,
    /// From the learners' first append to every one of them holding the
    /// commit index.
    pub catch_up_ms: u64,
    /// The flush just before the pause, so the barrier's has little to do.
    pub pre_flush_ms: u64,
    /// Paused: until every appended entry committed and the learners held it...
    pub drain_ms: u64,
    /// ...then the flush to that point...
    pub flush_ms: u64,
    /// ...then the CAS to epoch + 1.
    pub cas_ms: u64,
    /// No new appends from the pause to leading `epoch` (or handing off).
    pub paused_ms: u64,
    pub flushed: u64,
    pub at_ms: i64,
}

#[derive(Clone, Debug, Default, Serialize)]
pub struct Quantiles {
    pub count: u64,
    pub p50: u64,
    pub p90: u64,
    pub p99: u64,
    pub p999: u64,
    pub max: u64,
}

impl Quantiles {
    pub fn of(h: &hdrhistogram::Histogram<u64>) -> Quantiles {
        Quantiles {
            count: h.len(),
            p50: h.value_at_quantile(0.5),
            p90: h.value_at_quantile(0.9),
            p99: h.value_at_quantile(0.99),
            p999: h.value_at_quantile(0.999),
            max: h.max(),
        }
    }
}

pub struct Node {
    pub cfg: Config,
    core: Mutex<Core>,
    bucket: Bucket,
    /// The leader's last seq: wakes its replicators.
    head: watch::Sender<u64>,
    commit: watch::Sender<u64>,
    pub faults: Arc<Faults>,
    /// Where each other node is dialed: `--peer`, then addresses from
    /// `qlog/leader` and membership changes.
    addrs: Mutex<HashMap<String, String>>,
    ctl: Mutex<HashMap<String, Arc<Rpc>>>,
    /// A membership change's barrier: submits wait while it's set.
    paused: watch::Sender<bool>,
    switches: Mutex<Vec<SwitchStats>>,
    durability: Arc<dyn Durability>,
    pub emit: Arc<Emitter>,
    pub stats: Stats,
    pub flush: flush::Shared,
    /// Held from reading what to emit until it's handed over, so batches
    /// reach the firehose in order (a recovery emits outside the emitter).
    emit_order: Mutex<()>,
    recovered: Mutex<Vec<flush::RecoveryStats>>,
}

impl Node {
    /// Starts serving peers on `listener`. A node that finds no
    /// `qlog/leader` is at genesis and whole, and so is one that recovered
    /// its commitlog. Any other starts empty and not intact (memory-only or
    /// a lost disk: it can't vouch for what it acked before).
    pub async fn start(
        cfg: Config,
        bucket: Bucket,
        listener: TcpListener,
        emit: Arc<Emitter>,
        faults: Arc<Faults>,
        durability: Arc<dyn Durability>,
        recovered: Option<Recovered>,
    ) -> anyhow::Result<Arc<Node>> {
        let record = read_leader(&bucket.leader).await?.map(|(r, _)| r);
        let genesis = record.is_none();
        let manifest = match &cfg.flush {
            Some(_) => flush::read_manifest(&bucket.flush).await?.map(|(m, _)| m),
            None => None,
        };
        let (mut log, promised, promised_to, whole) = match recovered {
            Some(r) => (r.log, r.promised, r.promised_to, !r.fresh),
            None => (Log::new(), 0, None, false),
        };
        if durability.journaling() {
            log.journal();
        }
        let (emitted, commit, log_last) = (log.base().1, log.commit(), log.last_seq());
        durability.note_commit(commit);
        let mut addrs = cfg.peers.clone();
        if let Some(r) = &record {
            for (id, a) in &r.addrs {
                addrs.entry(id.clone()).or_insert_with(|| a.clone());
            }
        }
        let (members, learners, members_since) = match &record {
            Some(r) => (r.members.clone(), r.learners.clone(), r.since),
            None => (cfg.members.clone(), Vec::new(), 0),
        };
        let node = Arc::new(Node {
            core: Mutex::new(Core {
                log,
                promised,
                role: Role::Follower,
                epoch: promised,
                leader: promised_to,
                last_heard: Instant::now(),
                seen_record: 0,
                electing: false,
                probing: false,
                retry_at: Instant::now(),
                probed_for: None,
                intact: genesis || whole,
                need_upto: None,
                emitted,
                durable: if durability.journaling() { log_last } else { u64::MAX },
                cut: 0,
                matched: HashMap::new(),
                next: HashMap::new(),
                acked_at: HashMap::new(),
                self_durable: 0,
                waiters: BTreeMap::new(),
                admitting: 0,
                pending: VecDeque::new(),
                pending_bytes: 0,
                flushed: manifest.as_ref().map_or(0, |m| m.flushed),
                reserve: match (&cfg.flush, &manifest) {
                    (None, _) => u64::MAX,
                    (Some(_), m) => m.as_ref().map_or(0, |m| m.reserve),
                },
                pending_cursors: BTreeMap::new(),
                generation: manifest.as_ref().map_or(0, |m| m.generation()),
                recovery_cursors: manifest
                    .as_ref()
                    .and_then(|m| m.recovery.as_ref())
                    .map(|r| (r.generation, encode_cursors(&r.cursors))),
                fresh: HashSet::new(),
                members,
                learners,
                members_since,
                peer_intact: HashSet::new(),
                paused: false,
                switching: false,
                retired: false,
            }),
            cfg,
            bucket,
            head: watch::channel(0).0,
            commit: watch::channel(commit).0,
            faults,
            addrs: Mutex::new(addrs),
            ctl: Mutex::new(HashMap::new()),
            paused: watch::channel(false).0,
            switches: Mutex::new(Vec::new()),
            durability,
            emit,
            stats: Stats::default(),
            flush: flush::Shared::default(),
            emit_order: Mutex::new(()),
            recovered: Mutex::new(Vec::new()),
        });
        node.emit.attach(&node);
        tracing::info!(id = %node.cfg.id, genesis, whole, promised, emitted, commit, "qlog: node up");
        tokio::spawn(node.clone().accept(listener));
        tokio::spawn(node.clone().ticker());
        tokio::spawn(node.clone().emitter());
        Ok(node)
    }

    pub fn status(&self) -> Status {
        self.status_and(false)
    }

    /// The status, then the latency histograms start over if `reset`.
    pub fn status_and(&self, reset: bool) -> Status {
        let disk = self.durability.report(reset);
        let c = self.core.lock();
        Status {
            id: self.cfg.id.clone(),
            role: c.role,
            epoch: c.epoch,
            promised: c.promised,
            leader: c.leader.clone(),
            base: c.log.base().1,
            last: c.log.last_seq(),
            commit: c.log.commit(),
            emitted: c.emitted,
            intact: c.intact,
            log_bytes: c.log.bytes(),
            appended: self.stats.appended.load(Ordering::Relaxed),
            takeovers: self.stats.takeovers.load(Ordering::Relaxed),
            step_downs: self.stats.step_downs.load(Ordering::Relaxed),
            resets: self.stats.resets.load(Ordering::Relaxed),
            emit_gaps: self.stats.emit_gaps.load(Ordering::Relaxed),
            promise_rounds: self.stats.promise_rounds.load(Ordering::Relaxed),
            disk_reads: self.stats.disk_reads.load(Ordering::Relaxed),
            bucket_reads: self.stats.bucket_reads.load(Ordering::Relaxed),
            commit_us: Quantiles::of(&self.stats.commit_us.lock()),
            disk,
            flushed: c.flushed,
            reserve: c.reserve,
            flush: self.cfg.flush.as_ref().map(|_| self.flush.status(reset)),
            generation: c.generation,
            recoveries: self.stats.recoveries.load(Ordering::Relaxed),
            lost_quorums: self.stats.lost_quorums.load(Ordering::Relaxed),
            recovered: self.recovered.lock().clone(),
            members: c.members.clone(),
            learners: c.learners.clone(),
            members_since: c.members_since,
            retired: c.retired,
            paused: c.paused,
            last_epoch: c.log.last().0,
            switches: self.switches.lock().clone(),
            requests: super::bucket::requests(),
            relay: self.cfg.hooks.0.as_ref().map(|h| h.report()),
        }
    }

    /// (seq, data) of every committed entry still held, for checkers.
    pub fn committed(&self) -> Vec<(u64, Bytes)> {
        let c = self.core.lock();
        let (_, base) = c.log.base();
        c.log.range(base, c.log.commit()).map(|e| (e.seq, e.data.clone())).collect()
    }

    // ---- for the flush (flush.rs) and the firehose's local tail (emit.rs)

    pub(crate) fn bucket(&self) -> &Bucket {
        &self.bucket
    }

    /// Still leading `epoch`, and the commit index.
    pub(crate) fn leading(&self, epoch: u64) -> Option<u64> {
        let c = self.core.lock();
        (c.role == Role::Leader && c.epoch == epoch).then(|| c.log.commit())
    }

    pub(crate) fn commit_rx(&self) -> watch::Receiver<u64> {
        self.commit.subscribe()
    }

    /// Committed entries above this are readable here (memory or disk).
    pub(crate) fn readable_floor(&self) -> u64 {
        let base = self.core.lock().log.base().1;
        self.durability.first_readable().map_or(base, |d| d.min(base))
    }

    pub fn emitted(&self) -> u64 {
        self.core.lock().emitted
    }

    /// Leader of `epoch`: the members it has heard from within `within`,
    /// itself included (who can own hosts). None when not leading it.
    pub fn live_members(&self, epoch: u64, within: Duration) -> Option<Vec<String>> {
        let c = self.core.lock();
        if c.role != Role::Leader || c.epoch != epoch {
            return None;
        }
        let now = Instant::now();
        Some(
            c.members
                .iter()
                .filter(|m| **m == self.cfg.id || c.acked_at.get(*m).is_some_and(|t| now.duration_since(*t) < within))
                .cloned()
                .collect(),
        )
    }

    /// The address this node dials `id` at, if it knows one.
    pub fn addr_of(&self, id: &str) -> Option<String> {
        self.addrs.lock().get(id).cloned()
    }

    /// A committed manifest's F and R: the commit index may rise to R, and
    /// the log may leave local disk up to F. Both only ever move up.
    pub(crate) fn set_flushed(&self, flushed: u64, reserve: u64) {
        let mut c = self.core.lock();
        c.flushed = c.flushed.max(flushed);
        c.reserve = c.reserve.max(reserve);
        self.advance_commit(&mut c);
    }

    /// What a manifest says about recoveries (a new leader's fence read).
    pub(crate) fn note_manifest(&self, m: &flush::Manifest) {
        let mut c = self.core.lock();
        c.generation = c.generation.max(m.generation());
        if let Some(r) = &m.recovery
            && c.recovery_cursors.as_ref().is_none_or(|(g, _)| *g < r.generation)
        {
            c.recovery_cursors = Some((r.generation, encode_cursors(&r.cursors)));
        }
    }

    pub(crate) fn step_down_from(&self, epoch: u64, why: &str) {
        let mut c = self.core.lock();
        if c.epoch == epoch {
            self.step_down(&mut c, why);
        }
    }

    /// Committed entries from `from` to at most `upto`, about `max_bytes` of
    /// them (at least one), from memory or the commitlog. Fails if this node
    /// no longer holds `from`.
    pub(crate) async fn committed_chunk(&self, from: u64, upto: u64, max_bytes: usize) -> anyhow::Result<Vec<Entry>> {
        let base = {
            let c = self.core.lock();
            anyhow::ensure!(upto <= c.log.commit(), "qlog: {upto} isn't committed (commit {})", c.log.commit());
            if from > upto {
                return Ok(Vec::new());
            }
            let base = c.log.base().1;
            if from > base {
                let mut n = 0;
                let mut out = Vec::new();
                for e in c.log.range(from - 1, upto) {
                    if !out.is_empty() && n + e.data.len() > max_bytes {
                        break;
                    }
                    n += e.data.len();
                    out.push(e.clone());
                }
                return Ok(out);
            }
            base
        };
        match self.durability.read(from, upto.min(base), max_bytes).await {
            Some((_, es)) if !es.is_empty() => Ok(es),
            _ => anyhow::bail!("qlog: seq {from} is no longer held on this node"),
        }
    }

    async fn accept(self: Arc<Self>, listener: TcpListener) {
        loop {
            match listener.accept().await {
                Ok((s, _)) => {
                    let _ = s.set_nodelay(true);
                    tokio::spawn(self.clone().serve_conn(s));
                }
                Err(e) => {
                    tracing::warn!("qlog: accept failed: {e}");
                    tokio::time::sleep(Duration::from_millis(50)).await;
                }
            }
        }
    }

    async fn serve_conn(self: Arc<Self>, s: TcpStream) {
        let (mut rd, mut wr) = s.into_split();
        let (tx, mut rx) = mpsc::unbounded_channel::<Bytes>();
        let writer = tokio::spawn(async move {
            use tokio::io::AsyncWriteExt;
            while let Some(b) = rx.recv().await {
                if wr.write_all(&b).await.is_err() {
                    return;
                }
            }
        });
        let mut appender: Option<String> = None;
        while let Ok((rid, m)) = wire::read_msg(&mut rd).await {
            if m.from_peer().is_some_and(|p| self.faults.blocked(p)) {
                continue;
            }
            if let Msg::Append(a) = &m
                && appender.as_deref() != Some(a.leader.as_str())
            {
                appender = Some(a.leader.clone());
            }
            let resp = match m {
                Msg::Submit { frames, cursors, generation } => {
                    let (n, tx) = (self.clone(), tx.clone());
                    tokio::spawn(async move {
                        let r = n.submit_from(frames, cursors, generation).await;
                        let _ = tx.send(wire::encode(rid, &r));
                    });
                    continue;
                }
                Msg::SubmitEvents { items, cursors, control, generation } => {
                    let (n, tx) = (self.clone(), tx.clone());
                    tokio::spawn(async move {
                        let r = n.submit_events(items, cursors, control, generation).await;
                        let _ = tx.send(wire::encode(rid, &r));
                    });
                    continue;
                }
                Msg::Ask { topic, body } => {
                    let (n, tx) = (self.clone(), tx.clone());
                    tokio::spawn(async move {
                        let r = n.ask(&topic, body).await;
                        let _ = tx.send(wire::encode(rid, &r));
                    });
                    continue;
                }
                Msg::Append(a) => Msg::AppendResp(self.on_append(a).await),
                Msg::Promise { epoch, from } => Msg::PromiseResp(self.on_promise(epoch, &from).await),
                Msg::Fetch { epoch, from_seq, max_bytes, .. } => {
                    self.on_fetch(epoch, from_seq, max_bytes as usize).await
                }
                Msg::Ping { .. } => Msg::Pong,
                Msg::Lead { epoch, .. } => {
                    self.on_lead(epoch);
                    Msg::Pong
                }
                Msg::Cursors => {
                    let c = self.core.lock();
                    match &c.recovery_cursors {
                        Some((g, b)) if *g == c.generation => {
                            Msg::CursorsResp { generation: *g, known: true, cursors: b.clone() }
                        }
                        _ => Msg::CursorsResp {
                            generation: c.generation,
                            known: c.generation == 0,
                            cursors: Bytes::new(),
                        },
                    }
                }
                _ => continue,
            };
            if tx.send(wire::encode(rid, &resp)).is_err() {
                break;
            }
        }
        drop(tx);
        let _ = writer.await;
        if let Some(l) = appender {
            self.leader_conn_closed(&l);
        }
    }

    // ---- leader

    /// A host owner's events: appended, replicated, and answered once a
    /// quorum holds them (or failed if this node stops leading first; the
    /// sender then resends them to the next leader, under new seqs).
    pub async fn submit(self: &Arc<Self>, frames: Vec<(Bytes, Bytes)>, cursors: Bytes) -> Msg {
        let g = self.core.lock().generation;
        self.submit_from(frames, cursors, g).await
    }

    /// As `submit`, from a submitter that last rewound for recovery
    /// `generation`: its cursors are dropped if it's behind, since they may
    /// count events a recovery lost (they'd put the manifest's cursors past
    /// the log).
    pub async fn submit_from(self: &Arc<Self>, frames: Vec<(Bytes, Bytes)>, cursors: Bytes, generation: u64) -> Msg {
        // under the submitter's timeout, so a busy leader isn't taken for a dead one
        let deadline = tokio::time::Instant::now() + Duration::from_millis(500);
        let mut commits = self.commit.subscribe();
        let mut paused = self.paused.subscribe();
        let (rx, first, last, epoch, ticket) = loop {
            {
                let mut c = self.core.lock();
                if c.role != Role::Leader {
                    return Msg::NotLeader { hint: c.leader.clone().filter(|l| *l != self.cfg.id).unwrap_or_default() };
                }
                if !c.paused && c.pending_bytes < self.cfg.max_pending_bytes {
                    let epoch = c.epoch;
                    let first = c.log.last_seq() + 1;
                    if generation >= c.generation {
                        merge_cursors(&mut c.pending_cursors, &cursors);
                    }
                    if frames.is_empty() {
                        return Msg::Submitted { first, n: 0, generation: c.generation };
                    }
                    let mut ride = encode_cursors(&std::mem::take(&mut c.pending_cursors));
                    for (p, s) in &frames {
                        let seq = c.log.last_seq() + 1;
                        c.log.append_with(epoch, wire::splice_seq(p, s, seq), std::mem::take(&mut ride));
                    }
                    let last = c.log.last_seq();
                    let (tx, rx) = oneshot::channel();
                    c.waiters.entry(last).or_default().push(tx);
                    let bytes = c.log.range(first - 1, last).map(|e| e.data.len()).sum();
                    c.pending.push_back((first, last, Instant::now(), bytes));
                    c.pending_bytes += bytes;
                    let ticket = self.sync(&mut c);
                    break (rx, first, last, epoch, ticket);
                }
            }
            let busy = Msg::Failed { reason: "busy: too much uncommitted, or a membership change".into() };
            tokio::select! {
                _ = commits.changed() => {}
                _ = paused.changed() => {}
                _ = tokio::time::sleep_until(deadline) => return busy,
            }
        };
        self.stats.appended.fetch_add(last - first + 1, Ordering::Relaxed);
        self.head.send_replace(last);
        if let Err(e) = self.settle(ticket).await {
            let mut c = self.core.lock();
            self.step_down(&mut c, "persist failed");
            return Msg::Failed { reason: format!("persist: {e:#}") };
        }
        {
            let mut c = self.core.lock();
            if c.role == Role::Leader && c.epoch == epoch {
                c.self_durable = c.self_durable.max(last);
                self.advance_commit(&mut c);
            }
        }
        match rx.await {
            Ok(Ok(())) => Msg::Submitted { first, n: last - first + 1, generation: self.core.lock().generation },
            Ok(Err(reason)) => Msg::Failed { reason },
            Err(_) => Msg::Failed { reason: "dropped".into() },
        }
    }

    /// A host owner's checked events (`Msg::SubmitEvents`): the hooks decide
    /// each, the ones they accept are appended in order with their meta,
    /// and each is answered once its entry (or the one it duplicates) has
    /// committed. An event is decided only once the leader can append it,
    /// so a decision is never left standing without its entry while this
    /// term lasts; if the term ends first, the hooks drop the term's
    /// decisions and the submitter resends to the next leader.
    pub async fn submit_events(
        self: &Arc<Self>,
        items: Vec<Item>,
        cursors: Bytes,
        control: Bytes,
        generation: u64,
    ) -> Msg {
        let Some(hooks) = self.cfg.hooks.0.clone() else {
            return Msg::Failed { reason: "this log takes no relay events".into() };
        };
        let deadline = tokio::time::Instant::now() + Duration::from_millis(500);
        let mut commits = self.commit.subscribe();
        let mut paused = self.paused.subscribe();
        let epoch = loop {
            {
                let mut c = self.core.lock();
                if c.role != Role::Leader {
                    return Msg::NotLeader { hint: c.leader.clone().filter(|l| *l != self.cfg.id).unwrap_or_default() };
                }
                if !c.paused && c.pending_bytes < self.cfg.max_pending_bytes {
                    c.admitting += 1;
                    break c.epoch;
                }
            }
            tokio::select! {
                _ = commits.changed() => {}
                _ = paused.changed() => {}
                _ = tokio::time::sleep_until(deadline) => {
                    return Msg::Failed { reason: "busy: too much uncommitted, or a membership change".into() };
                }
            }
        };
        let admitting = Admitting(self);
        let Admission { verdicts, hold } = hooks.admit(epoch, &items, control).await;
        let mut outcomes = vec![Outcome::Retry("undecided".into()); items.len()];
        let mut appended = Vec::new();
        let (rx, first, last, ticket) = {
            let mut c = self.core.lock();
            if c.role != Role::Leader || c.epoch != epoch {
                return Msg::NotLeader { hint: c.leader.clone().filter(|l| *l != self.cfg.id).unwrap_or_default() };
            }
            if generation >= c.generation {
                merge_cursors(&mut c.pending_cursors, &cursors);
            }
            let first = c.log.last_seq() + 1;
            let mut wait_for = 0u64;
            let mut ride = Bytes::new();
            for (i, (it, v)) in items.iter().zip(verdicts).enumerate() {
                match v {
                    Verdict::Append { meta, ticket } => {
                        if c.log.last_seq() + 1 == first {
                            ride = encode_cursors(&std::mem::take(&mut c.pending_cursors));
                        }
                        let seq = c.log.last_seq() + 1;
                        c.log.append_meta(
                            epoch,
                            wire::splice_seq(&it.prefix, &it.suffix, seq),
                            std::mem::take(&mut ride),
                            meta,
                        );
                        appended.push((ticket, seq));
                        outcomes[i] = Outcome::Appended(seq);
                        wait_for = seq;
                    }
                    Verdict::Answer { outcome, after } => {
                        outcomes[i] = outcome;
                        if let Some(a) = after {
                            wait_for = wait_for.max(a);
                        }
                    }
                }
            }
            let last = c.log.last_seq();
            let n = last + 1 - first;
            let ticket = (n > 0).then(|| {
                let bytes = c.log.range(first - 1, last).map(|e| e.data.len()).sum();
                c.pending.push_back((first, last, Instant::now(), bytes));
                c.pending_bytes += bytes;
                self.sync(&mut c)
            });
            let rx = (wait_for > c.log.commit()).then(|| {
                let (tx, rx) = oneshot::channel();
                c.waiters.entry(wait_for).or_default().push(tx);
                rx
            });
            (rx, first, last, ticket)
        };
        drop(admitting);
        if !appended.is_empty() {
            hooks.appended(epoch, appended);
        }
        drop(hold);
        if last >= first {
            self.stats.appended.fetch_add(last + 1 - first, Ordering::Relaxed);
            self.head.send_replace(last);
        }
        if let Some(ticket) = ticket {
            if let Err(e) = self.settle(ticket).await {
                let mut c = self.core.lock();
                self.step_down(&mut c, "persist failed");
                return Msg::Failed { reason: format!("persist: {e:#}") };
            }
            let mut c = self.core.lock();
            if c.role == Role::Leader && c.epoch == epoch {
                c.self_durable = c.self_durable.max(last);
                self.advance_commit(&mut c);
            }
        }
        if let Some(rx) = rx {
            match rx.await {
                Ok(Ok(())) => {}
                Ok(Err(reason)) => return Msg::Failed { reason },
                Err(_) => return Msg::Failed { reason: "dropped".into() },
            }
        }
        Msg::SubmittedEvents { outcomes, generation: self.core.lock().generation }
    }

    async fn ask(self: &Arc<Self>, topic: &str, body: Bytes) -> Msg {
        match topic {
            "status" => Msg::Answer { body: serde_json::to_vec(&self.status()).unwrap_or_default().into() },
            "members" => {
                #[derive(Deserialize)]
                struct Req {
                    token: String,
                    members: Vec<String>,
                    #[serde(default)]
                    addrs: BTreeMap<String, String>,
                }
                let r: Req = match serde_json::from_slice(&body) {
                    Ok(r) => r,
                    Err(e) => return Msg::Failed { reason: format!("bad request: {e}") },
                };
                let allowed = self
                    .cfg
                    .admin_token
                    .as_deref()
                    .is_some_and(|t| !t.is_empty() && vlpds::auth::token_eq(t, &r.token));
                if !allowed {
                    return Msg::Failed { reason: "unauthorized: the qlog admin token is required".into() };
                }
                match self.change_members(r.members, r.addrs).await {
                    Ok(st) => Msg::Answer { body: serde_json::to_vec(&st).unwrap_or_default().into() },
                    Err(e) => match e.downcast_ref::<NotLeading>() {
                        Some(NotLeading(l)) => Msg::NotLeader { hint: l.clone() },
                        None => Msg::Failed { reason: format!("{e:#}") },
                    },
                }
            }
            t if t.starts_with("leader:") && self.core.lock().role != Role::Leader => {
                let c = self.core.lock();
                Msg::NotLeader { hint: c.leader.clone().filter(|l| *l != self.cfg.id).unwrap_or_default() }
            }
            t => match &self.cfg.hooks.0 {
                Some(h) => match h.answer(t, body).await {
                    Some(b) => Msg::Answer { body: b },
                    None => Msg::Failed { reason: format!("nothing to say about {t}") },
                },
                None => Msg::Failed { reason: format!("unknown topic {t}") },
            },
        }
    }

    /// Entries from `from` on, about `max_bytes` of them (at least one if
    /// any is held): committed ones from memory or the commitlog, the
    /// uncommitted tail from memory. Empty past the last.
    pub async fn read_tail(&self, from: u64, max_bytes: usize) -> anyhow::Result<Vec<Entry>> {
        let commit = self.core.lock().log.commit();
        if from <= commit {
            return self.committed_chunk(from, commit, max_bytes).await;
        }
        let c = self.core.lock();
        let mut n = 0;
        let mut out = Vec::new();
        for e in c.log.range(from - 1, c.log.last_seq()) {
            if !out.is_empty() && n + e.data.len() > max_bytes {
                break;
            }
            n += e.data.len();
            out.push(e.clone());
        }
        Ok(out)
    }

    fn advance_commit(&self, c: &mut Core) {
        if c.role != Role::Leader {
            return;
        }
        // only this epoch's members count: never a learner, never a member
        // a membership change removed (and a leader outside its own set,
        // mid-handoff, commits nothing)
        if !c.members.contains(&self.cfg.id) {
            return;
        }
        let mut v: Vec<u64> = c
            .members
            .iter()
            .map(|m| if *m == self.cfg.id { c.self_durable } else { c.matched.get(m).copied().unwrap_or(0) })
            .collect();
        v.sort_unstable_by(|a, b| b.cmp(a));
        let q = v[quorum(v.len()) - 1].min(c.reserve);
        // only this term's entries commit by count; the adopted tail was re-tagged
        if q <= c.log.commit() || c.log.epoch_at(q) != Some(c.epoch) {
            return;
        }
        c.log.set_commit(q);
        self.durability.note_commit(q);
        let now = Instant::now();
        {
            let mut h = self.stats.commit_us.lock();
            while let Some(&(first, last, at, bytes)) = c.pending.front() {
                if last > q {
                    break;
                }
                c.pending.pop_front();
                c.pending_bytes -= bytes;
                let _ = h.record_n((now - at).as_micros().max(1) as u64, last - first + 1);
            }
        }
        let rest = c.waiters.split_off(&(q + 1));
        for w in std::mem::replace(&mut c.waiters, rest).into_values().flatten() {
            let _ = w.send(Ok(()));
        }
        self.commit.send_replace(q);
    }

    fn step_down(&self, c: &mut Core, why: &str) {
        if c.role == Role::Follower {
            return;
        }
        tracing::warn!(id = %self.cfg.id, epoch = c.epoch, why, "qlog: stepping down");
        self.stats.step_downs.fetch_add(1, Ordering::Relaxed);
        c.role = Role::Follower;
        c.leader = None;
        c.last_heard = Instant::now();
        for w in std::mem::take(&mut c.waiters).into_values().flatten() {
            let _ = w.send(Err(format!("not leader: {why}")));
        }
        c.pending.clear();
        c.pending_bytes = 0;
    }

    fn become_leader(self: &Arc<Self>, c: &mut Core, epoch: u64) {
        let last = c.log.last_seq();
        let commit = c.log.commit();
        c.log.restamp_after(commit, epoch);
        c.role = Role::Leader;
        c.epoch = epoch;
        c.leader = Some(self.cfg.id.clone());
        c.intact = true;
        c.need_upto = None;
        // the adopted, re-tagged tail counts once it's on disk
        c.self_durable = commit;
        let ticket = self.sync(c);
        c.matched.clear();
        c.next.clear();
        c.acked_at.clear();
        c.peer_intact.clear();
        c.retired = false;
        self.stats.takeovers.fetch_add(1, Ordering::Relaxed);
        tracing::info!(id = %self.cfg.id, epoch, last, commit, members = ?c.members, learners = ?c.learners, "qlog: leading");
        let replicas: Vec<String> =
            c.members.iter().chain(&c.learners).filter(|p| **p != self.cfg.id).cloned().collect();
        for p in replicas {
            self.add_replica(c, p, epoch);
        }
        if let Some(o) = &self.cfg.flush {
            tokio::spawn(flush::lead(self.clone(), epoch, o.clone()));
        }
        self.head.send_replace(last);
        let n = self.clone();
        tokio::spawn(async move {
            let r = n.settle(ticket).await;
            let mut c = n.core.lock();
            if c.role != Role::Leader || c.epoch != epoch {
                return;
            }
            match r {
                Ok(()) => {
                    c.self_durable = c.self_durable.max(last);
                    n.advance_commit(&mut c);
                }
                Err(_) => n.step_down(&mut c, "persisting the adopted tail failed"),
            }
        });
    }

    fn add_replica(self: &Arc<Self>, c: &mut Core, peer: String, epoch: u64) {
        if c.matched.contains_key(&peer) {
            return;
        }
        c.matched.insert(peer.clone(), 0);
        c.next.insert(peer.clone(), c.log.last_seq() + 1);
        c.acked_at.insert(peer.clone(), Instant::now());
        tokio::spawn(self.clone().replicate(peer, epoch));
    }

    /// Stages the log's journaled changes; the ticket covers everything
    /// staged so far (call it under the lock, before acting on them).
    fn sync(&self, c: &mut Core) -> Ticket {
        let ops = c.log.take_journal();
        for op in &ops {
            if let Op::TruncateAfter(s) | Op::Reset { seq: s, .. } = op {
                c.durable = c.durable.min(*s);
                c.cut += 1;
            }
        }
        let n = if ops.is_empty() { self.durability.staged() } else { self.durability.stage(ops) };
        Ticket { n, last: c.log.last_seq(), cut: c.cut }
    }

    /// Waits for `t` to be durable; this node may then emit up to its last
    /// seq, unless the log was cut back since (a later ticket covers that).
    async fn settle(&self, t: Ticket) -> anyhow::Result<()> {
        self.durability.wait(t.n).await?;
        let mut c = self.core.lock();
        if c.cut == t.cut && t.last > c.durable {
            c.durable = t.last;
            if c.log.commit() > c.emitted {
                self.commit.send_modify(|_| {});
            }
        }
        Ok(())
    }

    /// A promise is never forgotten: it's journaled with the log, and the
    /// caller waits for it to be durable before answering.
    fn raise_promised(&self, c: &mut Core, epoch: u64, to: &str) {
        if epoch > c.promised {
            c.promised = epoch;
            c.log.record_promise(epoch, to);
        }
    }

    async fn replicate(self: Arc<Self>, peer: String, epoch: u64) {
        let Some(addr) = self.addrs.lock().get(&peer).cloned() else {
            tracing::warn!(id = %self.cfg.id, peer, "qlog: no address for a replica, not replicating to it");
            return;
        };
        let rpc = Rpc::new(&peer, &addr, self.faults.clone());
        let mut head = self.head.subscribe();
        let mut commit = self.commit.subscribe();
        let mut sent_commit = 0;
        let mut last_send = Instant::now() - self.cfg.heartbeat;
        let mut seg_cache: flush::SegCache = None;
        loop {
            head.borrow_and_update();
            commit.borrow_and_update();
            let (behind, flushed, fresh) = {
                let c = self.core.lock();
                let next = c.next.get(&peer).copied().unwrap_or(0);
                let b = (c.role == Role::Leader && c.epoch == epoch && next <= c.log.base().1)
                    .then(|| (next, c.log.base().1));
                (b, c.flushed, c.fresh.contains(&peer))
            };
            // where a fresh follower starts: the oldest entry held here, if
            // the bucket has everything below it
            let fresh_start = behind.and_then(|(_, base)| {
                let t = self.durability.first_readable().unwrap_or(base).min(base);
                (fresh && t <= flushed).then_some(t)
            });
            // behind what's in memory: committed entries from the commitlog,
            // from the oldest it still holds if it doesn't reach back to `next`
            // (a reset there, never past what a takeover would flush from)
            let from_disk = match behind {
                Some((next, base)) => match self.durability.read(next, base, self.cfg.max_batch_bytes).await {
                    Some(r) => Some((next, r, false)),
                    // behind the disk too: from the bucket, if it's flushed
                    None if self.cfg.flush.is_some() && next <= flushed && fresh_start.is_none() => {
                        match flush::read_bucket(
                            &self.bucket.backfill,
                            &mut seg_cache,
                            next,
                            flushed.min(base),
                            self.cfg.max_batch_bytes,
                        )
                        .await
                        {
                            Ok(Some(r)) => {
                                self.stats.bucket_reads.fetch_add(1, Ordering::Relaxed);
                                Some((next, r, false))
                            }
                            Ok(None) => None,
                            Err(e) => {
                                tracing::warn!(
                                    peer,
                                    next,
                                    "qlog: reading the bucket for a lagging follower failed: {e:#}"
                                );
                                tokio::time::sleep(Duration::from_millis(200)).await;
                                continue;
                            }
                        }
                    }
                    None => match self.durability.first_readable() {
                        Some(f) if f + 1 > next && f < base => {
                            self.durability.read(f + 1, base, self.cfg.max_batch_bytes).await.map(|r| (f + 1, r, true))
                        }
                        _ => None,
                    },
                },
                None => None,
            };
            let req = {
                let c = self.core.lock();
                if c.role != Role::Leader || c.epoch != epoch || !c.matched.contains_key(&peer) {
                    return;
                }
                let next = c.next[&peer];
                let (base_epoch, base_seq) = c.log.base();
                let fresh = next <= c.log.last_seq() || c.log.commit() > sent_commit;
                if let Some((from, (prev_epoch, entries), reset)) =
                    from_disk.filter(|(f, (_, e), reset)| (*f == next || *reset) && !e.is_empty())
                {
                    self.stats.disk_reads.fetch_add(1, Ordering::Relaxed);
                    Some(Append {
                        epoch,
                        leader: self.cfg.id.clone(),
                        prev_epoch,
                        prev_seq: from - 1,
                        commit: c.log.commit(),
                        leader_last: c.log.last_seq(),
                        reset,
                        flushed: c.flushed,
                        reserve: c.reserve,
                        generation: c.generation,
                        entries,
                    })
                } else if !fresh && last_send.elapsed() < self.cfg.heartbeat {
                    None
                } else {
                    let reset = next <= base_seq;
                    let (prev_epoch, prev_seq) = if reset {
                        (base_epoch, base_seq)
                    } else {
                        (c.log.epoch_at(next - 1).expect("next - 1 is held"), next - 1)
                    };
                    Some(Append {
                        epoch,
                        leader: self.cfg.id.clone(),
                        prev_epoch,
                        prev_seq,
                        commit: c.log.commit(),
                        leader_last: c.log.last_seq(),
                        reset,
                        flushed: c.flushed,
                        reserve: c.reserve,
                        generation: c.generation,
                        entries: c.log.entries_from(prev_seq + 1, self.cfg.max_batch_bytes),
                    })
                }
            };
            let Some(req) = req else {
                let wait = self.cfg.heartbeat.saturating_sub(last_send.elapsed());
                tokio::select! {
                    _ = head.changed() => {}
                    _ = commit.changed() => {}
                    _ = tokio::time::sleep(wait) => {}
                }
                continue;
            };
            last_send = Instant::now();
            let req_commit = req.commit;
            match rpc.call(&Msg::Append(req), self.cfg.rpc_timeout).await {
                Ok(Msg::AppendResp(r)) => {
                    if r.ok {
                        sent_commit = req_commit;
                    }
                    self.on_append_resp(&peer, epoch, r);
                }
                Ok(other) => tracing::warn!(peer, "qlog: unexpected reply to append: {other:?}"),
                Err(_) => tokio::time::sleep(Duration::from_millis(20)).await,
            }
        }
    }

    fn on_append_resp(&self, peer: &str, epoch: u64, r: AppendResp) {
        let mut c = self.core.lock();
        if c.role != Role::Leader || c.epoch != epoch {
            return;
        }
        if r.promised > epoch {
            self.raise_promised(&mut c, r.promised, "");
            self.sync(&mut c);
            self.step_down(&mut c, "a follower promised a newer epoch");
            return;
        }
        if !c.matched.contains_key(peer) {
            return;
        }
        c.acked_at.insert(peer.to_string(), Instant::now());
        if r.intact {
            c.peer_intact.insert(peer.to_string());
        } else {
            c.peer_intact.remove(peer);
        }
        if r.ok {
            c.fresh.remove(peer);
        } else if r.last_seq == 0 {
            c.fresh.insert(peer.to_string());
        }
        if r.ok {
            let m = c.matched.entry(peer.to_string()).or_default();
            *m = (*m).max(r.matched);
            let m = *m;
            c.next.insert(peer.to_string(), m + 1);
            self.advance_commit(&mut c);
        } else {
            // its committed prefix matches ours for certain
            let n = (r.commit + 1).min(c.log.last_seq() + 1);
            c.next.insert(peer.to_string(), n);
        }
    }

    // ---- follower

    async fn on_append(self: &Arc<Self>, a: Append) -> AppendResp {
        let (resp, ticket, commit_moved) = {
            let mut c = self.core.lock();
            if a.epoch < c.promised {
                return AppendResp {
                    ok: false,
                    promised: c.promised,
                    matched: 0,
                    commit: c.log.commit(),
                    last_seq: c.log.last_seq(),
                    intact: c.intact,
                };
            }
            if c.role != Role::Follower {
                self.step_down(&mut c, "a leader of the same or a newer epoch appended");
            }
            self.raise_promised(&mut c, a.epoch, &a.leader);
            c.epoch = a.epoch;
            c.leader = Some(a.leader.clone());
            c.role = Role::Follower;
            c.last_heard = Instant::now();
            c.retired = false;
            c.flushed = c.flushed.max(a.flushed);
            c.reserve = c.reserve.max(a.reserve);
            c.generation = c.generation.max(a.generation);
            if !c.intact && c.need_upto.is_none() {
                c.need_upto = Some(a.leader_last);
            }
            if a.reset && a.prev_seq > c.log.commit() && c.log.epoch_at(a.prev_seq) != Some(a.prev_epoch) {
                self.stats.resets.fetch_add(1, Ordering::Relaxed);
                if a.prev_seq > c.emitted {
                    // a node that hasn't emitted yet just starts its stream at the base
                    if c.emitted > 0 {
                        self.stats.emit_gaps.fetch_add(a.prev_seq - c.emitted, Ordering::Relaxed);
                        tracing::warn!(id = %self.cfg.id, from = c.emitted, to = a.prev_seq, "qlog: reset past what was emitted: a gap in this node's stream");
                    }
                    c.emitted = a.prev_seq;
                }
                c.log.reset(a.prev_epoch, a.prev_seq);
            }
            let commit_before = c.log.commit();
            let r = match c.log.try_append(a.prev_epoch, a.prev_seq, a.entries) {
                Ok(m) => {
                    c.log.set_commit(a.commit.min(m));
                    self.durability.note_commit(c.log.commit());
                    if !c.intact && c.need_upto.is_some_and(|n| m >= n) {
                        tracing::info!(id = %self.cfg.id, matched = m, "qlog: caught up after a restart, intact again");
                        c.intact = true;
                    }
                    let resp = AppendResp {
                        ok: true,
                        promised: c.promised,
                        matched: m,
                        commit: c.log.commit(),
                        last_seq: c.log.last_seq(),
                        intact: c.intact,
                    };
                    (resp, c.log.commit() > commit_before)
                }
                Err(_) => (
                    AppendResp {
                        ok: false,
                        promised: c.promised,
                        matched: 0,
                        commit: c.log.commit(),
                        last_seq: c.log.last_seq(),
                        intact: c.intact,
                    },
                    false,
                ),
            };
            (r.0, self.sync(&mut c), r.1)
        };
        if commit_moved {
            self.commit.send_replace(resp.commit);
        }
        // the ack (and the promise it carries) only once it's all on disk
        if self.settle(ticket).await.is_err() {
            return AppendResp { ok: false, matched: 0, ..resp };
        }
        resp
    }

    async fn on_promise(&self, epoch: u64, from: &str) -> PromiseResp {
        let (resp, ticket) = self.promise_locked(epoch, from);
        if self.settle(ticket).await.is_err() {
            return PromiseResp { ok: false, ..resp };
        }
        resp
    }

    fn promise_locked(&self, epoch: u64, from: &str) -> (PromiseResp, Ticket) {
        let mut c = self.core.lock();
        let ok =
            epoch > c.promised || (epoch == c.promised && c.leader.as_deref() == Some(from) && from != self.cfg.id);
        if ok && epoch == c.promised {
            // the candidate is still at it (a long bucket recovery): it's
            // alive, so don't take over from it
            c.last_heard = Instant::now();
        }
        if ok && epoch > c.promised {
            self.step_down(&mut c, "promised a newer epoch");
            self.raise_promised(&mut c, epoch, from);
            c.epoch = epoch;
            c.leader = Some(from.to_string());
            c.role = Role::Follower;
            c.last_heard = Instant::now();
        }
        let (last_epoch, last_seq) = c.log.last();
        let resp = PromiseResp {
            ok,
            promised: c.promised,
            last_epoch,
            last_seq,
            base_seq: c.log.base().1,
            commit: c.log.commit(),
            intact: c.intact,
            generation: c.generation,
        };
        (resp, self.sync(&mut c))
    }

    async fn on_fetch(&self, epoch: u64, from_seq: u64, max_bytes: usize) -> Msg {
        let below = {
            let c = self.core.lock();
            (epoch == c.promised && from_seq <= c.log.base().1).then(|| c.log.base().1)
        };
        // older than memory holds: committed, so straight from the commitlog
        if let Some(base) = below
            && let Some((prev_epoch, entries)) = self.durability.read(from_seq, base, max_bytes).await
            && !entries.is_empty()
        {
            let c = self.core.lock();
            if epoch == c.promised {
                self.stats.disk_reads.fetch_add(1, Ordering::Relaxed);
                return Msg::FetchResp {
                    ok: true,
                    base_epoch: prev_epoch,
                    base_seq: from_seq - 1,
                    last_seq: c.log.last_seq(),
                    entries,
                };
            }
        }
        let c = self.core.lock();
        let (base_epoch, base_seq) = c.log.base();
        if epoch != c.promised {
            return Msg::FetchResp { ok: false, base_epoch, base_seq, last_seq: c.log.last_seq(), entries: Vec::new() };
        }
        Msg::FetchResp {
            ok: true,
            base_epoch,
            base_seq,
            last_seq: c.log.last_seq(),
            entries: c.log.entries_from(from_seq, max_bytes),
        }
    }

    // ---- liveness and takeover

    async fn ticker(self: Arc<Self>) {
        let mut tick = tokio::time::interval(Duration::from_millis(20));
        tick.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
        loop {
            tick.tick().await;
            enum Act {
                Takeover,
                Probe(String),
            }
            let act = {
                let mut c = self.core.lock();
                let floor = self.trim_floor(&c);
                // with no disk, memory is the only copy of the unflushed tail
                let upto = if self.durability.first_readable().is_some() {
                    c.emitted.min(self.durability.written_last())
                } else {
                    floor
                };
                c.log.trim(self.cfg.retain_bytes, upto);
                self.durability.set_floor(floor);
                match c.role {
                    Role::Leader => {
                        let alive = c
                            .members
                            .iter()
                            .filter(|m| {
                                **m == self.cfg.id
                                    || c.acked_at.get(*m).is_some_and(|t| t.elapsed() < self.cfg.election_timeout)
                            })
                            .count();
                        if alive < quorum(c.members.len()) {
                            self.step_down(&mut c, "no quorum heard within the election timeout");
                        }
                        None
                    }
                    Role::Candidate => None,
                    Role::Follower if c.electing || c.retired => None,
                    Role::Follower => {
                        let quiet = c.last_heard.elapsed();
                        // a single node has nobody to hear from: it leads at once
                        let alone = c.members.len() == 1 && c.members[0] == self.cfg.id;
                        if (alone || quiet > self.cfg.election_timeout) && Instant::now() >= c.retry_at {
                            c.electing = true;
                            Some(Act::Takeover)
                        } else if quiet > self.cfg.probe_after
                            && !c.probing
                            && c.probed_for != Some(c.last_heard)
                            && let Some(l) = c.leader.clone().filter(|l| *l != self.cfg.id)
                        {
                            // once per silence: a probe that times out says nothing
                            c.probing = true;
                            c.probed_for = Some(c.last_heard);
                            Some(Act::Probe(l))
                        } else {
                            None
                        }
                    }
                }
            };
            match act {
                Some(Act::Takeover) => {
                    tokio::spawn(self.clone().takeover());
                }
                Some(Act::Probe(l)) => {
                    tokio::spawn(self.clone().probe(l));
                }
                None => {}
            }
        }
    }

    /// What may leave local disk: emitted, committed and flushed to the
    /// bucket, and (leader) not still needed by a live follower catching up,
    /// so it's served from disk rather than reset past.
    fn trim_floor(&self, c: &Core) -> u64 {
        let mut f = c.emitted.min(c.log.commit());
        if self.cfg.flush.is_some() {
            f = f.min(c.flushed);
        }
        if c.role == Role::Leader {
            for (p, m) in &c.matched {
                if c.acked_at.get(p).is_some_and(|t| t.elapsed() < self.cfg.laggard_grace) {
                    f = f.min(*m);
                }
            }
        }
        f
    }

    /// The leader's appends stopped coming over this connection: if its
    /// port now refuses us, its process is gone and a takeover starts at
    /// once instead of after the election timeout.
    fn leader_conn_closed(self: &Arc<Self>, leader: &str) {
        let mut c = self.core.lock();
        if c.role == Role::Follower && !c.electing && !c.probing && c.leader.as_deref() == Some(leader) {
            c.probing = true;
            tokio::spawn(self.clone().probe(leader.to_string()));
        }
    }

    async fn probe(self: Arc<Self>, l: String) {
        let dead = match self.rpc(&l) {
            Some(rpc) => matches!(
                rpc.call(&Msg::Ping { from: self.cfg.id.clone() }, self.cfg.rpc_timeout).await,
                Err(CallError::Refused | CallError::Io(_))
            ),
            None => false,
        };
        let mut c = self.core.lock();
        c.probing = false;
        if dead && c.role == Role::Follower && !c.electing && c.leader.as_deref() == Some(&l) {
            tracing::info!(id = %self.cfg.id, leader = %l, "qlog: leader's port refused, taking over");
            c.electing = true;
            drop(c);
            tokio::spawn(self.clone().takeover());
        }
    }

    async fn takeover(self: Arc<Self>) {
        let started = Instant::now();
        if let Err(e) = self.try_takeover(started).await {
            tracing::warn!(id = %self.cfg.id, "qlog: takeover failed: {e:#}");
        }
        let mut c = self.core.lock();
        c.electing = false;
        c.retry_at = Instant::now() + Duration::from_millis(200);
        if c.role == Role::Candidate {
            self.step_down(&mut c, "takeover abandoned");
        }
    }

    fn heard_since(&self, t: Instant) -> bool {
        let c = self.core.lock();
        c.last_heard > t && c.role == Role::Follower
    }

    async fn try_takeover(self: &Arc<Self>, started: Instant) -> anyhow::Result<()> {
        let rec = read_leader(&self.bucket.leader).await?;
        let (cur, etag) = match &rec {
            Some((r, e)) => (Some(r.clone()), Some(e.clone())),
            None => (None, None),
        };
        let cur_epoch = cur.as_ref().map_or(0, |r| r.epoch);
        // the set in force is the record's; --peer only bootstraps it
        let (members, learners, addrs, since) = match &cur {
            Some(r) => (r.members.clone(), r.learners.clone(), r.addrs.clone(), r.since),
            None => (self.cfg.members.clone(), Vec::new(), BTreeMap::new(), 1),
        };
        self.learn_addrs(&addrs);
        {
            let mut c = self.core.lock();
            if self.heard_since_locked(&c, started) {
                return Ok(());
            }
            if cur_epoch >= c.epoch {
                c.members = members.clone();
                c.learners = learners.clone();
                c.members_since = since;
            }
            if !members.contains(&self.cfg.id) {
                // A learner waits for the change that makes it a member; a
                // node the record doesn't name at all was removed (or was
                // never added) and stays out until a leader appends to it.
                c.last_heard = Instant::now();
                if !learners.contains(&self.cfg.id) && !c.retired {
                    tracing::info!(id = %self.cfg.id, epoch = cur_epoch, ?members, "qlog: qlog/leader doesn't name this node: not campaigning");
                    c.retired = true;
                }
                return Ok(());
            }
            if cur_epoch > c.seen_record && cur_epoch > c.epoch && cur.as_ref().is_some_and(|r| r.leader != self.cfg.id)
            {
                // someone else took over; give them a timeout to reach us
                c.seen_record = cur_epoch;
                c.last_heard = Instant::now();
                return Ok(());
            }
            c.seen_record = c.seen_record.max(cur_epoch);
        }
        let leader = cur.as_ref().map(|r| r.leader.as_str());
        let rank = members.iter().filter(|m| Some(m.as_str()) != leader).position(|m| *m == self.cfg.id).unwrap_or(0);
        if rank > 0 {
            tokio::time::sleep(self.cfg.stagger * rank as u32).await;
            if self.heard_since(started) {
                return Ok(());
            }
        }
        // A minority never takes over: it can't tell "they're dead" from "I'm
        // cut off", and a CAS from it would only unseat the majority's
        // leader when the partition heals.
        let q = quorum(members.len());
        let others: Vec<String> = members.iter().filter(|m| **m != self.cfg.id).cloned().collect();
        let mut rx = self.broadcast(Msg::Ping { from: self.cfg.id.clone() }, &others);
        let mut reachable = 0;
        while reachable + 1 < q {
            match rx.recv().await {
                Some((_, Ok(Msg::Pong))) => reachable += 1,
                Some(_) => {}
                None => break,
            }
        }
        if reachable + 1 < q {
            tracing::debug!(id = %self.cfg.id, reachable, "qlog: can't reach a quorum, not taking over");
            return Ok(());
        }
        if self.heard_since(started) {
            return Ok(());
        }
        let epoch = cur_epoch + 1;
        let rec = LeaderRecord { epoch, leader: self.cfg.id.clone(), members: members.clone(), learners, addrs, since };
        if !cas_leader(&self.bucket.leader, &rec, etag).await? {
            let mut c = self.core.lock();
            c.last_heard = Instant::now();
            return Ok(());
        }
        self.won(epoch, members).await
    }

    /// `qlog/leader` names this node at `epoch` (its own CAS, or a removed
    /// leader's handoff): promises from a quorum of `members`, the longest
    /// tail among them, then lead (or recover from the bucket if no quorum
    /// of intact logs can exist).
    async fn won(self: &Arc<Self>, epoch: u64, members: Vec<String>) -> anyhow::Result<()> {
        let q = quorum(members.len());
        let others: Vec<String> = members.iter().filter(|m| **m != self.cfg.id).cloned().collect();
        let own = {
            let mut c = self.core.lock();
            if c.promised >= epoch {
                return Ok(());
            }
            self.step_down(&mut c, "taking over");
            let me = self.cfg.id.clone();
            self.raise_promised(&mut c, epoch, &me);
            c.epoch = epoch;
            c.members = members.clone();
            c.role = Role::Candidate;
            c.leader = Some(self.cfg.id.clone());
            self.sync(&mut c)
        };
        // our own promise counts toward the round only once it's durable
        self.settle(own).await?;
        tracing::info!(id = %self.cfg.id, epoch, "qlog: won qlog/leader, collecting promises");
        loop {
            {
                let c = self.core.lock();
                if c.role != Role::Candidate || c.epoch != epoch {
                    return Ok(());
                }
            }
            self.stats.promise_rounds.fetch_add(1, Ordering::Relaxed);
            let mut voters: Vec<(String, u64, u64)> = Vec::new();
            // every member that promised, with its commit index, intact or not
            let mut answered: Vec<(String, u64)> = Vec::new();
            {
                let c = self.core.lock();
                if c.intact {
                    let (e, s) = c.log.last();
                    voters.push((self.cfg.id.clone(), e, s));
                }
                answered.push((self.cfg.id.clone(), c.log.commit()));
            }
            let mut rx = self.broadcast(Msg::Promise { epoch, from: self.cfg.id.clone() }, &others);
            while voters.len() < q {
                let Some((id, r)) = rx.recv().await else { break };
                if let Ok(Msg::PromiseResp(p)) = r {
                    if p.promised > epoch {
                        let mut c = self.core.lock();
                        self.raise_promised(&mut c, p.promised, "");
                        self.sync(&mut c);
                        self.step_down(&mut c, "a member promised a newer epoch");
                        return Ok(());
                    }
                    if p.ok {
                        let mut c = self.core.lock();
                        c.generation = c.generation.max(p.generation);
                        drop(c);
                        answered.push((id.clone(), p.commit));
                        if p.intact {
                            voters.push((id, p.last_epoch, p.last_seq));
                        }
                    }
                }
            }
            if voters.len() < q {
                // A member that didn't promise may hold an intact log; one
                // that promised this epoch and isn't intact can't become
                // intact behind our back (it refuses older leaders now).
                // Only when even counting every silent member as intact
                // falls short of a quorum has the quorum been lost.
                let silent = members.len() - answered.len();
                if voters.len() + silent < q {
                    self.stats.lost_quorums.fetch_add(1, Ordering::Relaxed);
                    if self.cfg.flush.is_some() && self.cfg.auto_recover {
                        tracing::warn!(id = %self.cfg.id, epoch, intact = voters.len(), answered = answered.len(), "qlog: no quorum of intact logs can exist: bucket recovery");
                        return self.bucket_recover(epoch, answered).await;
                    }
                    tracing::warn!(id = %self.cfg.id, epoch, intact = voters.len(), "qlog: the quorum is lost; bucket recovery is off, waiting");
                } else {
                    tracing::warn!(id = %self.cfg.id, epoch, intact = voters.len(), "qlog: no quorum of intact logs yet, retrying");
                }
                tokio::time::sleep(Duration::from_millis(200)).await;
                continue;
            }
            // the most up-to-date log of a quorum holds every committed entry
            let best = voters
                .iter()
                .max_by_key(|(id, e, s)| (*e, *s, *id == self.cfg.id))
                .expect("a quorum is non-empty")
                .clone();
            if best.0 != self.cfg.id
                && let Err(e) = self.adopt(&best.0, epoch).await
            {
                tracing::warn!(id = %self.cfg.id, from = %best.0, "qlog: fetching the longest tail failed: {e:#}");
                tokio::time::sleep(Duration::from_millis(100)).await;
                continue;
            }
            let mut c = self.core.lock();
            if c.role != Role::Candidate || c.epoch != epoch {
                return Ok(());
            }
            self.become_leader(&mut c, epoch);
            return Ok(());
        }
    }

    /// After a lost quorum (docs/quorum.md, "Bucket recovery"): this
    /// candidate, holding promises from a quorum of members, takes the log
    /// from the bucket. Committed entries past the manifest's F (orphan
    /// segments, then the longest committed prefix a member that promised
    /// holds) are kept with their seqs; the seqs from there up to R are
    /// skipped (anything emitted among them is re-ingested from the hosts
    /// above R); the state is the manifest's checkpoint cloned; and this
    /// node leads from R + 1.
    async fn bucket_recover(self: &Arc<Self>, epoch: u64, answered: Vec<(String, u64)>) -> anyhow::Result<()> {
        let o = self.cfg.flush.clone().expect("checked by the caller");
        let t0 = Instant::now();
        let Some(p) = flush::recovery_point(&self.bucket.recovery).await? else {
            // Nothing was ever fenced, so the reservation was 0: nothing was
            // ever committed, let alone emitted. Start over at the bottom.
            let mut c = self.core.lock();
            if c.role != Role::Candidate || c.epoch != epoch {
                return Ok(());
            }
            anyhow::ensure!(c.log.commit() == 0, "qlog: a commit index with no manifest");
            c.log.reset(epoch, 0);
            self.become_leader(&mut c, epoch);
            return Ok(());
        };
        let mut stats = flush::RecoveryStats { read_ms: t0.elapsed().as_millis() as u64, ..Default::default() };
        // A recovery can outlast the election timeout (at 100x a 10 s
        // interval's salvage is over a GB): the members that promised hear
        // from this candidate meanwhile, or they'd depose it and the next
        // recovery would jump another H.
        let keepalive = {
            let n = self.clone();
            let members: Vec<String> = answered.iter().map(|(id, _)| id.clone()).filter(|id| *id != n.cfg.id).collect();
            tokio::spawn(async move {
                loop {
                    tokio::time::sleep(n.cfg.heartbeat).await;
                    let mut rx = n.broadcast(Msg::Promise { epoch, from: n.cfg.id.clone() }, &members);
                    while rx.recv().await.is_some() {}
                }
            })
        };
        let _stop = AbortOnDrop(keepalive);
        let from = p.flushed + 1;
        let mut by_commit = answered;
        by_commit.sort_by_key(|a| std::cmp::Reverse(a.1));
        // The salvage streams into segments as it's fetched, two chunks
        // ahead at most. A member that fails midway still leaves a dense,
        // committed prefix, and that's what's kept.
        let (tx, rx) = mpsc::channel::<Vec<Entry>>(2);
        let n = self.clone();
        let fetcher = tokio::spawn(async move {
            for (id, commit) in by_commit {
                if commit < from {
                    break;
                }
                let mut next = from;
                while next <= commit {
                    match n.committed_chunk_of(&id, epoch, next, commit).await {
                        Ok(es) if es.first().is_some_and(|e| e.seq == next) => {
                            next = es.last().expect("non-empty").seq + 1;
                            if tx.send(es).await.is_err() {
                                return;
                            }
                        }
                        Ok(_) => break,
                        Err(e) => {
                            tracing::warn!(id = %n.cfg.id, from = %id, next, "qlog recovery: salvage failed: {e:#}");
                            break;
                        }
                    }
                }
                if next > from {
                    tracing::info!(id = %n.cfg.id, from = %id, first = from, last = next - 1, "qlog recovery: salvaged committed entries");
                    return;
                }
            }
        });
        let m = flush::recover(&self.bucket.recovery, &self.cfg.id, epoch, &o, p, rx, &mut stats).await;
        fetcher.abort();
        let m = m?;
        let rec = m.recovery.clone().expect("a recovery manifest");
        let (after, base) = (rec.after, rec.base);
        // This incarnation's live consumers get everything up to S before the
        // jump; S is in the bucket now. A process that hasn't emitted has no
        // live consumers (reconnecting ones backfill).
        let emitted = self.core.lock().emitted;
        let mut catch_up = Vec::new();
        if self.emit.firehose().is_some() && emitted < after {
            let mut cache: flush::SegCache = None;
            let mut next = emitted + 1;
            while next <= after {
                match flush::read_bucket(&self.bucket.backfill, &mut cache, next, after, 8 << 20).await? {
                    Some((_, es)) if !es.is_empty() => {
                        next = es.last().expect("non-empty").seq + 1;
                        catch_up.extend(es);
                    }
                    _ => break,
                }
            }
        }
        let _order = self.emit_order.lock();
        let mut c = self.core.lock();
        if c.role != Role::Candidate || c.epoch != epoch {
            // the manifest is ours and fences older leaders; whoever leads
            // next reads it
            return Ok(());
        }
        if !catch_up.is_empty() && catch_up[0].seq == c.emitted + 1 {
            let upto = catch_up.last().expect("non-empty").seq;
            if let Some(h) = &self.cfg.hooks.0 {
                h.committed(&catch_up);
            }
            let ev: Vec<(i64, Bytes)> = catch_up.into_iter().map(|e| (e.seq as i64, e.data)).collect();
            self.emit.emit(c.emitted, upto, ev);
            c.emitted = upto;
        }
        if c.emitted < base {
            if self.emit.firehose().is_some() && c.emitted < after {
                self.stats.emit_gaps.fetch_add(after - c.emitted, Ordering::Relaxed);
                tracing::warn!(id = %self.cfg.id, from = c.emitted, to = after, "qlog recovery: couldn't emit up to S before the jump");
            }
            c.emitted = base;
        }
        // the log's base takes this epoch, so a promise round prefers it
        // over any older log still on a member's disk
        c.log.reset(epoch, base);
        c.flushed = c.flushed.max(m.flushed);
        c.reserve = c.reserve.max(m.reserve);
        c.generation = c.generation.max(rec.generation);
        c.recovery_cursors = Some((rec.generation, encode_cursors(&rec.cursors)));
        self.stats.recoveries.fetch_add(1, Ordering::Relaxed);
        stats.total_ms = t0.elapsed().as_millis() as u64;
        tracing::warn!(
            id = %self.cfg.id, epoch, generation = rec.generation, f = stats.manifest_flushed, after, base,
            salvaged = stats.salvaged, orphans = stats.orphan_segments, read_ms = stats.read_ms,
            clone_ms = stats.clone_ms, apply_seal_ms = stats.apply_seal_ms, segments_ms = stats.segments_ms,
            manifest_ms = stats.manifest_ms, ms = stats.total_ms, end_ms = chrono::Utc::now().timestamp_millis(),
            "qlog recovery: the bucket's log adopted, resuming above R"
        );
        self.recovered.lock().push(stats);
        self.become_leader(&mut c, epoch);
        Ok(())
    }

    /// One chunk of committed entries from `from` (at most `upto`) as `id`
    /// holds them (this node: its own log; a member that promised `epoch`:
    /// fetched). Empty if it doesn't reach back to `from`.
    async fn committed_chunk_of(&self, id: &str, epoch: u64, from: u64, upto: u64) -> anyhow::Result<Vec<Entry>> {
        if id == self.cfg.id {
            let upto = upto.min(self.core.lock().log.commit());
            return self.committed_chunk(from, upto, self.cfg.max_batch_bytes).await;
        }
        let rpc = self.rpc(id).ok_or_else(|| anyhow::anyhow!("unknown member {id}"))?;
        let m =
            Msg::Fetch { epoch, from: self.cfg.id.clone(), from_seq: from, max_bytes: self.cfg.max_batch_bytes as u64 };
        let Msg::FetchResp { ok, base_seq, entries, .. } =
            rpc.call(&m, self.cfg.rpc_timeout * 4).await.map_err(|e| anyhow::anyhow!("{e:?}"))?
        else {
            anyhow::bail!("unexpected reply to fetch");
        };
        anyhow::ensure!(ok, "{id} no longer promised to epoch {epoch}");
        if base_seq >= from {
            return Ok(Vec::new());
        }
        let es: Vec<Entry> = entries.into_iter().take_while(|e| e.seq <= upto).collect();
        anyhow::ensure!(es.first().is_none_or(|e| e.seq == from), "{id} sent {} for {from}", es[0].seq);
        Ok(es)
    }

    fn rpc(&self, id: &str) -> Option<Arc<Rpc>> {
        if let Some(r) = self.ctl.lock().get(id) {
            return Some(r.clone());
        }
        let addr = self.addrs.lock().get(id)?.clone();
        Some(
            self.ctl
                .lock()
                .entry(id.to_string())
                .or_insert_with(|| Arc::new(Rpc::new(id, &addr, self.faults.clone())))
                .clone(),
        )
    }

    fn learn_addrs(&self, a: &BTreeMap<String, String>) {
        let mut m = self.addrs.lock();
        for (id, addr) in a {
            if *id != self.cfg.id {
                m.entry(id.clone()).or_insert_with(|| addr.clone());
            }
        }
    }

    /// Sends `m` to every peer; replies arrive as they come. Each call runs
    /// to its end on its own task (a dropped call would leave its connection
    /// mid-frame), so a caller waits only for as many as it needs, not for a
    /// member that's down or cut off.
    fn broadcast(&self, m: Msg, to: &[String]) -> mpsc::UnboundedReceiver<(String, Result<Msg, CallError>)> {
        let (tx, rx) = mpsc::unbounded_channel();
        for id in to {
            let Some(rpc) = self.rpc(id) else {
                tracing::warn!(id = %self.cfg.id, peer = %id, "qlog: no address for a member");
                continue;
            };
            let (id, tx, m) = (id.clone(), tx.clone(), m.clone());
            let t = self.cfg.rpc_timeout;
            tokio::spawn(async move {
                let _ = tx.send((id, rpc.call(&m, t).await));
            });
        }
        rx
    }

    fn heard_since_locked(&self, c: &Core, t: Instant) -> bool {
        c.last_heard > t && c.role == Role::Follower
    }

    /// Replaces our uncommitted tail with `best`'s log past our commit index.
    async fn adopt(self: &Arc<Self>, best: &str, epoch: u64) -> anyhow::Result<()> {
        let rpc = self.rpc(best).ok_or_else(|| anyhow::anyhow!("unknown member {best}"))?;
        let my_commit = self.core.lock().log.commit();
        let mut from = my_commit + 1;
        let mut got: Vec<Entry> = Vec::new();
        let mut reset_to = None;
        loop {
            let m = Msg::Fetch {
                epoch,
                from: self.cfg.id.clone(),
                from_seq: from,
                max_bytes: self.cfg.max_batch_bytes as u64,
            };
            let Msg::FetchResp { ok, base_epoch, base_seq, last_seq, entries } =
                rpc.call(&m, self.cfg.rpc_timeout * 4).await.map_err(|e| anyhow::anyhow!("{e:?}"))?
            else {
                anyhow::bail!("unexpected reply to fetch");
            };
            anyhow::ensure!(ok, "{best} no longer promised to epoch {epoch}");
            if base_seq >= from {
                anyhow::ensure!(got.is_empty(), "{best} trimmed its log mid-fetch");
                reset_to = Some((base_epoch, base_seq));
            }
            let n = entries.len();
            got.extend(entries);
            from = got.last().map_or(from, |e| e.seq + 1);
            if from > last_seq || n == 0 {
                break;
            }
        }
        let mut c = self.core.lock();
        if c.role != Role::Candidate || c.epoch != epoch {
            anyhow::bail!("no longer a candidate");
        }
        anyhow::ensure!(c.log.commit() == my_commit, "commit index moved while a candidate");
        let prev = match reset_to {
            Some((e, s)) => {
                self.stats.resets.fetch_add(1, Ordering::Relaxed);
                if s > c.emitted {
                    if c.emitted > 0 {
                        self.stats.emit_gaps.fetch_add(s - c.emitted, Ordering::Relaxed);
                        tracing::warn!(id = %self.cfg.id, from = c.emitted, to = s, "qlog: adopting a tail past what was emitted: a gap in this node's stream");
                    }
                    c.emitted = s;
                }
                c.log.reset(e, s);
                (e, s)
            }
            None => {
                c.log.truncate_after(my_commit);
                (c.log.epoch_at(my_commit).unwrap_or(0), my_commit)
            }
        };
        let n = got.len();
        c.log
            .try_append(prev.0, prev.1, got)
            .map_err(|_| anyhow::anyhow!("adopted tail doesn't follow our committed prefix"))?;
        tracing::info!(id = %self.cfg.id, from = %best, entries = n, last = c.log.last_seq(), "qlog: adopted the longest tail");
        Ok(())
    }

    // ---- membership (docs/quorum.md, "Membership changes")

    /// A removed leader handed `epoch` to this node: if `qlog/leader` says
    /// so, collect promises and lead now instead of after the timeout.
    fn on_lead(self: &Arc<Self>, epoch: u64) {
        {
            let mut c = self.core.lock();
            if c.promised >= epoch || c.electing || c.role != Role::Follower {
                return;
            }
            c.electing = true;
        }
        let n = self.clone();
        tokio::spawn(async move {
            let r = async {
                let Some((rec, _)) = read_leader(&n.bucket.leader).await? else { anyhow::bail!("no qlog/leader") };
                anyhow::ensure!(
                    rec.epoch == epoch && rec.leader == n.cfg.id && rec.members.contains(&n.cfg.id),
                    "qlog/leader is epoch {} naming {}, not {epoch} naming this node",
                    rec.epoch,
                    rec.leader
                );
                n.learn_addrs(&rec.addrs);
                {
                    let mut c = n.core.lock();
                    c.members = rec.members.clone();
                    c.learners = rec.learners.clone();
                    c.members_since = rec.since;
                    c.retired = false;
                }
                tracing::info!(id = %n.cfg.id, epoch, "qlog: handed leadership by a membership change");
                n.won(epoch, rec.members).await
            }
            .await;
            if let Err(e) = r {
                tracing::warn!(id = %n.cfg.id, epoch, "qlog: handoff failed: {e:#}");
            }
            let mut c = n.core.lock();
            c.electing = false;
            if c.role == Role::Candidate {
                n.step_down(&mut c, "handoff abandoned");
            }
        });
    }

    /// Changes the member set to `target` at a flush barrier, from the
    /// leader. The steps, and why one step from the old set to the new is
    /// safe there (no joint consensus):
    ///
    /// 1. Nodes in `target` that aren't members are recorded as learners in
    ///    `qlog/leader` (same epoch) and replicated to. They don't count
    ///    toward commits, promises or the lost-quorum trigger.
    /// 2. Once each learner is intact and holds the commit index, a flush
    ///    (so the barrier's own flush is short), then appends pause.
    /// 3. Paused, until everything appended has committed under the old set
    ///    and every learner (and enough of `target` for any quorum of it to
    ///    include one) durably holds it: the last seq is the barrier B.
    /// 4. The flush reaches B: everything committed is in the bucket, so no
    ///    committed entry depends on which set a later takeover asks.
    /// 5. `qlog/leader` CASes to epoch + 1 with `target`. Before it no entry
    ///    is committed under `target`; after it none can be under the old
    ///    set, since this leader moves to epoch + 1 (or steps down) without
    ///    appending again at epoch, and every later leader reads the record.
    /// 6. This node leads epoch + 1 with `target`, or, if it isn't in
    ///    `target`, hands epoch + 1 to a member holding B and retires.
    ///
    /// Removed members never hold an entry of epoch + 1 or promise it (no
    /// leader sends them either), so they never count after the switch,
    /// and a takeover reads the record first, so they never campaign.
    /// Runs to its end even if the caller goes away: a change cut short
    /// after its CAS must not resume commits at the old epoch.
    pub async fn change_members(
        self: &Arc<Self>,
        target: Vec<String>,
        addrs: BTreeMap<String, String>,
    ) -> anyhow::Result<SwitchStats> {
        let n = self.clone();
        tokio::spawn(async move { n.switch(target, addrs).await }).await?
    }

    async fn switch(
        self: Arc<Self>,
        mut target: Vec<String>,
        addrs: BTreeMap<String, String>,
    ) -> anyhow::Result<SwitchStats> {
        target.sort();
        target.dedup();
        anyhow::ensure!(!target.is_empty(), "an empty member set");
        let (epoch, from) = {
            let mut c = self.core.lock();
            anyhow::ensure!(c.role == Role::Leader, NotLeading(c.leader.clone().unwrap_or_default()));
            anyhow::ensure!(!c.switching, "a membership change is already running");
            if c.members == target {
                return Ok(SwitchStats {
                    from_epoch: c.epoch,
                    epoch: c.epoch,
                    from: target.clone(),
                    to: target,
                    leader: self.cfg.id.clone(),
                    ..Default::default()
                });
            }
            c.switching = true;
            (c.epoch, c.members.clone())
        };
        let _guard = SwitchGuard(self.clone());
        self.learn_addrs(&addrs);
        let learners: Vec<String> = target.iter().filter(|m| !from.contains(m)).cloned().collect();
        for l in &learners {
            anyhow::ensure!(*l != self.cfg.id && self.addrs.lock().contains_key(l), "no address for {l}");
        }
        let mut st = SwitchStats {
            from_epoch: epoch,
            from: from.clone(),
            to: target.clone(),
            at_ms: chrono::Utc::now().timestamp_millis(),
            ..Default::default()
        };
        tracing::info!(id = %self.cfg.id, epoch, ?from, ?target, ?learners, "qlog member change: starting");

        // 1. learners
        let t = Instant::now();
        let (rec, etag) = self.own_record(epoch).await?;
        let mut rec_addrs = rec.addrs.clone();
        for (id, a) in &addrs {
            rec_addrs.insert(id.clone(), a.clone());
        }
        if rec.learners != learners || rec_addrs != rec.addrs {
            let next = LeaderRecord { learners: learners.clone(), addrs: rec_addrs.clone(), ..rec };
            anyhow::ensure!(
                cas_leader(&self.bucket.leader, &next, Some(etag)).await?,
                "qlog/leader moved: no longer leading {epoch}"
            );
        }
        {
            let mut c = self.core.lock();
            anyhow::ensure!(c.role == Role::Leader && c.epoch == epoch, "no longer leading {epoch}");
            c.learners = learners.clone();
            for l in &learners {
                self.add_replica(&mut c, l.clone(), epoch);
            }
        }
        st.record_ms = t.elapsed().as_millis() as u64;
        self.crash(flush::Step::SwitchCatchUp)?;

        // 2. catch-up, then a flush so the barrier's is small
        let t = Instant::now();
        loop {
            {
                let c = self.core.lock();
                anyhow::ensure!(c.role == Role::Leader && c.epoch == epoch, "no longer leading {epoch}");
                let commit = c.log.commit();
                if learners.iter().all(|l| c.peer_intact.contains(l) && c.matched.get(l).is_some_and(|m| *m >= commit))
                {
                    break;
                }
            }
            anyhow::ensure!(
                t.elapsed() < self.cfg.catch_up_timeout,
                "learners {learners:?} didn't catch up within {:?}",
                self.cfg.catch_up_timeout
            );
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
        st.catch_up_ms = t.elapsed().as_millis() as u64;
        tracing::info!(id = %self.cfg.id, epoch, ms = st.catch_up_ms, "qlog member change: learners caught up");
        let t = Instant::now();
        let commit = self.core.lock().log.commit();
        self.flush_to(epoch, commit, self.cfg.switch_timeout).await?;
        st.pre_flush_ms = t.elapsed().as_millis() as u64;
        self.crash(flush::Step::SwitchBeforePause)?;

        // 3. the barrier
        let paused_at = Instant::now();
        {
            let mut c = self.core.lock();
            anyhow::ensure!(c.role == Role::Leader && c.epoch == epoch, "no longer leading {epoch}");
            c.paused = true;
        }
        self.paused.send_replace(true);
        let mut commit_rx = self.commit.subscribe();
        let barrier = loop {
            {
                let c = self.core.lock();
                anyhow::ensure!(c.role == Role::Leader && c.epoch == epoch, "no longer leading {epoch}");
                let last = c.log.last_seq();
                let holders = self.holders(&c, &target, last);
                if c.log.commit() == last
                    && c.admitting == 0
                    && learners.iter().all(|l| holders.contains(l))
                    && holders.len() + quorum(target.len()) > target.len()
                {
                    break last;
                }
            }
            let left = self.cfg.switch_timeout.saturating_sub(paused_at.elapsed());
            anyhow::ensure!(!left.is_zero(), "the barrier didn't drain within {:?}", self.cfg.switch_timeout);
            tokio::select! {
                _ = commit_rx.changed() => {}
                _ = tokio::time::sleep(Duration::from_millis(1).min(left)) => {}
            }
        };
        st.drain_ms = paused_at.elapsed().as_millis() as u64;

        // 4. the flush to the barrier
        let t = Instant::now();
        self.flush_to(epoch, barrier, self.cfg.switch_timeout.saturating_sub(paused_at.elapsed())).await?;
        st.flush_ms = t.elapsed().as_millis() as u64;
        st.flushed = barrier;
        self.crash(flush::Step::SwitchFlushed)?;

        // 5. the new set at epoch + 1
        let t = Instant::now();
        let (rec, etag) = self.own_record(epoch).await?;
        let me_in = target.contains(&self.cfg.id);
        let next_leader = if me_in {
            self.cfg.id.clone()
        } else {
            // a continuing member before a learner: it has the longer local log
            let c = self.core.lock();
            let holders = self.holders(&c, &target, barrier);
            holders
                .iter()
                .find(|h| from.contains(h))
                .or_else(|| holders.first())
                .cloned()
                .ok_or_else(|| anyhow::anyhow!("nobody in {target:?} holds the barrier"))?
        };
        let next = LeaderRecord {
            epoch: epoch + 1,
            leader: next_leader.clone(),
            members: target.clone(),
            learners: Vec::new(),
            addrs: rec.addrs.clone(),
            since: epoch + 1,
        };
        let landed = match cas_leader(&self.bucket.leader, &next, Some(etag)).await {
            Ok(ok) => ok,
            // the PUT may have landed with its answer lost
            Err(e) => {
                tracing::warn!(id = %self.cfg.id, epoch, "qlog member change: the CAS failed: {e:#}");
                matches!(read_leader(&self.bucket.leader).await, Ok(Some((r, _))) if r == next)
            }
        };
        if !landed {
            // Whether or not it landed, commits at `epoch` can't resume: the
            // record may already name the new set.
            let mut c = self.core.lock();
            if c.epoch == epoch {
                self.step_down(&mut c, "a membership change's CAS failed");
            }
            anyhow::bail!("qlog/leader's CAS to epoch {} failed", epoch + 1);
        }
        st.cas_ms = t.elapsed().as_millis() as u64;
        self.crash(flush::Step::SwitchCas)?;

        // 6. lead the new set, or hand it off
        {
            let mut c = self.core.lock();
            anyhow::ensure!(c.role == Role::Leader && c.epoch == epoch, "deposed during the switch");
            c.members = target.clone();
            c.learners.clear();
            c.members_since = epoch + 1;
            if me_in {
                let me = self.cfg.id.clone();
                self.raise_promised(&mut c, epoch + 1, &me);
                self.become_leader(&mut c, epoch + 1);
            } else {
                self.step_down(&mut c, "removed by a membership change");
                c.retired = true;
                c.leader = Some(next_leader.clone());
            }
            c.paused = false;
        }
        self.paused.send_replace(false);
        st.paused_ms = paused_at.elapsed().as_millis() as u64;
        st.epoch = epoch + 1;
        st.leader = next_leader.clone();
        if !me_in {
            self.hand_off(&next_leader, epoch + 1).await;
        }
        tracing::info!(
            id = %self.cfg.id, from = ?st.from, to = ?st.to, epoch = st.epoch, leader = %st.leader,
            catch_up_ms = st.catch_up_ms, pre_flush_ms = st.pre_flush_ms, drain_ms = st.drain_ms,
            flush_ms = st.flush_ms, cas_ms = st.cas_ms, paused_ms = st.paused_ms, barrier,
            end_ms = chrono::Utc::now().timestamp_millis(), "qlog member change: done"
        );
        self.switches.lock().push(st.clone());
        Ok(st)
    }

    /// Who in `set` durably holds everything up to `seq`, as this leader
    /// knows (itself included); intact replicas only.
    fn holders(&self, c: &Core, set: &[String], seq: u64) -> Vec<String> {
        set.iter()
            .filter(|m| {
                if **m == self.cfg.id {
                    c.self_durable >= seq
                } else {
                    c.peer_intact.contains(*m) && c.matched.get(*m).is_some_and(|x| *x >= seq)
                }
            })
            .cloned()
            .collect()
    }

    /// `qlog/leader` as this leader of `epoch` holds it, with its ETag; a
    /// newer epoch there deposes it.
    async fn own_record(&self, epoch: u64) -> anyhow::Result<(LeaderRecord, Option<String>)> {
        let Some((rec, etag)) = read_leader(&self.bucket.leader).await? else { anyhow::bail!("no qlog/leader") };
        if rec.epoch != epoch || rec.leader != self.cfg.id {
            let mut c = self.core.lock();
            if c.epoch == epoch {
                self.step_down(&mut c, "qlog/leader moved on");
            }
            anyhow::bail!("qlog/leader is epoch {} led by {}, not {epoch} led by this node", rec.epoch, rec.leader);
        }
        Ok((rec, etag))
    }

    /// Until the flush has committed a manifest at or past `seq` (no bucket:
    /// at once).
    async fn flush_to(&self, epoch: u64, seq: u64, within: Duration) -> anyhow::Result<()> {
        if self.cfg.flush.is_none() {
            return Ok(());
        }
        let t = Instant::now();
        self.flush.request(seq);
        loop {
            {
                let c = self.core.lock();
                anyhow::ensure!(c.role == Role::Leader && c.epoch == epoch, "no longer leading {epoch}");
                if c.flushed >= seq {
                    return Ok(());
                }
            }
            anyhow::ensure!(t.elapsed() < within, "the flush didn't reach {seq} within {within:?}");
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
    }

    /// Chaos: the hook says die here. In-process that's stepping down (the
    /// test then kills the node), the binary SIGKILLs itself.
    fn crash(&self, step: flush::Step) -> anyhow::Result<()> {
        if let Some(h) = self.cfg.flush.as_ref().and_then(|o| o.crash.as_ref())
            && h(step)
        {
            let mut c = self.core.lock();
            self.step_down(&mut c, "crash injected");
            anyhow::bail!("crash injected at {step:?}");
        }
        Ok(())
    }

    async fn hand_off(&self, to: &str, epoch: u64) {
        let Some(rpc) = self.rpc(to) else { return };
        for _ in 0..5 {
            if rpc.call(&Msg::Lead { epoch, from: self.cfg.id.clone() }, self.cfg.rpc_timeout).await.is_ok() {
                return;
            }
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
        tracing::warn!(id = %self.cfg.id, to, epoch, "qlog: the handoff went unanswered; the members take over after the timeout");
    }

    // ---- emission

    /// Hands committed entries to the firehose, in order, as the commit
    /// index moves. Nothing above the commit index is ever handed over.
    async fn emitter(self: Arc<Self>) {
        let mut rx = self.commit.subscribe();
        // a recovered commit index is emitted without waiting for a leader
        rx.mark_changed();
        loop {
            if rx.changed().await.is_err() {
                return;
            }
            loop {
                let _order = self.emit_order.lock();
                let (from, upto, events) = {
                    let mut c = self.core.lock();
                    let upto = c.log.commit().min(c.durable);
                    if upto <= c.emitted {
                        break;
                    }
                    let from = c.emitted;
                    let ents: Vec<Entry> = c.log.range(from, upto).cloned().collect();
                    debug_assert_eq!(ents.first().map(|e| e.seq), Some(from + 1));
                    c.emitted = upto;
                    (from, upto, ents)
                };
                if let Some(h) = &self.cfg.hooks.0 {
                    h.committed(&events);
                }
                self.emit.emit(from, upto, events.into_iter().map(|e| (e.seq as i64, e.data)).collect());
            }
        }
    }
}

/// A `SubmitEvents` past its capacity check: a membership barrier waits
/// until it has appended (or given up).
struct Admitting<'a>(&'a Node);

impl Drop for Admitting<'_> {
    fn drop(&mut self) {
        self.0.core.lock().admitting -= 1;
        self.0.commit.send_modify(|_| {});
    }
}

struct AbortOnDrop(tokio::task::JoinHandle<()>);

impl Drop for AbortOnDrop {
    fn drop(&mut self) {
        self.0.abort();
    }
}

/// A membership change asked of a node that isn't leading (the leader it
/// follows, if known).
#[derive(Debug)]
pub struct NotLeading(pub String);

impl std::fmt::Display for NotLeading {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "not the leader (the leader is {:?})", self.0)
    }
}

impl std::error::Error for NotLeading {}

/// Ends a membership change: its barrier lifts however it ends. Every path
/// that reaches the CAS has moved to the new epoch or stepped down by then,
/// so lifting it never resumes commits under the old set.
struct SwitchGuard(Arc<Node>);

impl Drop for SwitchGuard {
    fn drop(&mut self) {
        let mut c = self.0.core.lock();
        c.switching = false;
        if c.paused {
            c.paused = false;
            drop(c);
            self.0.paused.send_replace(false);
        }
    }
}

#[derive(Debug)]
pub enum CallError {
    /// The port refused the connection: the process is gone.
    Refused,
    Timeout,
    Io(String),
    Blocked,
}

/// One connection to a peer, one request at a time.
pub struct Rpc {
    peer: String,
    addr: String,
    conn: tokio::sync::Mutex<Option<TcpStream>>,
    rid: AtomicU64,
    faults: Arc<Faults>,
}

impl Rpc {
    pub fn new(peer: &str, addr: &str, faults: Arc<Faults>) -> Rpc {
        Rpc {
            peer: peer.to_string(),
            addr: addr.to_string(),
            conn: tokio::sync::Mutex::new(None),
            rid: AtomicU64::new(1),
            faults,
        }
    }

    pub async fn call(&self, m: &Msg, timeout: Duration) -> Result<Msg, CallError> {
        if self.faults.blocked(&self.peer) {
            tokio::time::sleep(timeout).await;
            return Err(CallError::Blocked);
        }
        let mut g = self.conn.lock().await;
        let rid = self.rid.fetch_add(1, Ordering::Relaxed);
        let r = tokio::time::timeout(timeout, async {
            if g.is_none() {
                let s = TcpStream::connect(&self.addr).await.map_err(|e| {
                    if e.kind() == std::io::ErrorKind::ConnectionRefused {
                        CallError::Refused
                    } else {
                        CallError::Io(e.to_string())
                    }
                })?;
                let _ = s.set_nodelay(true);
                *g = Some(s);
            }
            let s = g.as_mut().expect("connected above");
            wire::write_msg(s, rid, m).await.map_err(|e| CallError::Io(e.to_string()))?;
            loop {
                let (r, resp) = wire::read_msg(s).await.map_err(|e| CallError::Io(e.to_string()))?;
                if r == rid {
                    return Ok(resp);
                }
            }
        })
        .await;
        match r {
            Ok(Ok(m)) => Ok(m),
            Ok(Err(e)) => {
                *g = None;
                Err(e)
            }
            Err(_) => {
                *g = None;
                Err(CallError::Timeout)
            }
        }
    }
}
