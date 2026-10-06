//! A quorum log node: leader, follower or candidate (docs/quorum.md §1, §3).
//!
//! The leader appends, gives each entry the next seq, replicates it to
//! every follower and commits it once a quorum (itself included) holds it.
//! Every node emits only up to the commit index it knows, so nothing reaches
//! a consumer before a quorum holds it. A follower that hears nothing from
//! the leader for `election_timeout` (or finds its port refused) CASes
//! `qlog/leader` to epoch + 1, collects promises from a quorum, adopts the
//! longest tail among them, re-tags that tail with its epoch and carries on.

use super::commitlog::{CommitLog, Recovered};
use super::emit::Emitter;
use super::flush;
use super::log::{Entry, Log, Op, encode_cursors, merge_cursors};
use super::wire::{self, Append, AppendResp, Msg, PromiseResp};
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
    /// Every member, this node included.
    pub members: Vec<String>,
    /// Where to dial each other member.
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
        }
    }

    pub fn quorum(&self) -> usize {
        self.members.len() / 2 + 1
    }
}

/// `qlog/leader`: one leader per epoch, decided by the bucket's CAS.
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
pub struct LeaderRecord {
    pub epoch: u64,
    pub leader: String,
    pub members: Vec<String>,
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
    waiters: BTreeMap<u64, oneshot::Sender<Result<(), String>>>,
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
    store: Store,
    /// The leader's last seq: wakes its replicators.
    head: watch::Sender<u64>,
    commit: watch::Sender<u64>,
    pub faults: Arc<Faults>,
    ctl: HashMap<String, Arc<Rpc>>,
    durability: Arc<dyn Durability>,
    pub emit: Arc<Emitter>,
    pub stats: Stats,
    pub flush: flush::Shared,
}

impl Node {
    /// Starts serving peers on `listener`. A node that finds no
    /// `qlog/leader` is at genesis and whole, and so is one that recovered
    /// its commitlog. Any other starts empty and not intact (memory-only or
    /// a lost disk: it can't vouch for what it acked before).
    pub async fn start(
        cfg: Config,
        store: Store,
        listener: TcpListener,
        emit: Arc<Emitter>,
        faults: Arc<Faults>,
        durability: Arc<dyn Durability>,
        recovered: Option<Recovered>,
    ) -> anyhow::Result<Arc<Node>> {
        let genesis = read_leader(&store).await?.is_none();
        let (mut log, promised, promised_to, whole) = match recovered {
            Some(r) => (r.log, r.promised, r.promised_to, !r.fresh),
            None => (Log::new(), 0, None, false),
        };
        if durability.journaling() {
            log.journal();
        }
        let (emitted, commit, log_last) = (log.base().1, log.commit(), log.last_seq());
        durability.note_commit(commit);
        let ctl =
            cfg.peers.iter().map(|(id, addr)| (id.clone(), Arc::new(Rpc::new(id, addr, faults.clone())))).collect();
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
                pending: VecDeque::new(),
                pending_bytes: 0,
                flushed: 0,
                reserve: if cfg.flush.is_some() { 0 } else { u64::MAX },
                pending_cursors: BTreeMap::new(),
            }),
            cfg,
            store,
            head: watch::channel(0).0,
            commit: watch::channel(commit).0,
            faults,
            ctl,
            durability,
            emit,
            stats: Stats::default(),
            flush: flush::Shared::default(),
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
        }
    }

    /// (seq, data) of every committed entry still held, for checkers.
    pub fn committed(&self) -> Vec<(u64, Bytes)> {
        let c = self.core.lock();
        let (_, base) = c.log.base();
        c.log.range(base, c.log.commit()).map(|e| (e.seq, e.data.clone())).collect()
    }

    // ---- for the flush (flush.rs) and the firehose's local tail (emit.rs)

    pub(crate) fn store(&self) -> &Store {
        &self.store
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

    /// A committed manifest's F and R: the commit index may rise to R, and
    /// the log may leave local disk up to F. Both only ever move up.
    pub(crate) fn set_flushed(&self, flushed: u64, reserve: u64) {
        let mut c = self.core.lock();
        c.flushed = c.flushed.max(flushed);
        c.reserve = c.reserve.max(reserve);
        self.advance_commit(&mut c);
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
                Msg::Submit { frames, cursors } => {
                    let (n, tx) = (self.clone(), tx.clone());
                    tokio::spawn(async move {
                        let r = n.submit(frames, cursors).await;
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
        // under the submitter's timeout, so a busy leader isn't taken for a dead one
        let deadline = tokio::time::Instant::now() + Duration::from_millis(500);
        let mut commits = self.commit.subscribe();
        loop {
            {
                let c = self.core.lock();
                if c.role != Role::Leader || c.pending_bytes < self.cfg.max_pending_bytes {
                    break;
                }
            }
            if tokio::time::timeout_at(deadline, commits.changed()).await.is_err() {
                return Msg::Failed { reason: "busy: too much uncommitted".into() };
            }
        }
        let (rx, first, last, epoch, ticket) = {
            let mut c = self.core.lock();
            if c.role != Role::Leader {
                return Msg::NotLeader { hint: c.leader.clone().filter(|l| *l != self.cfg.id).unwrap_or_default() };
            }
            let epoch = c.epoch;
            let first = c.log.last_seq() + 1;
            merge_cursors(&mut c.pending_cursors, &cursors);
            if frames.is_empty() {
                return Msg::Submitted { first, n: 0 };
            }
            let mut ride = encode_cursors(&std::mem::take(&mut c.pending_cursors));
            for (p, s) in &frames {
                let seq = c.log.last_seq() + 1;
                c.log.append_with(epoch, wire::splice_seq(p, s, seq), std::mem::take(&mut ride));
            }
            let last = c.log.last_seq();
            let (tx, rx) = oneshot::channel();
            c.waiters.insert(last, tx);
            let bytes = c.log.range(first - 1, last).map(|e| e.data.len()).sum();
            c.pending.push_back((first, last, Instant::now(), bytes));
            c.pending_bytes += bytes;
            let ticket = self.sync(&mut c);
            (rx, first, last, epoch, ticket)
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
            Ok(Ok(())) => Msg::Submitted { first, n: last - first + 1 },
            Ok(Err(reason)) => Msg::Failed { reason },
            Err(_) => Msg::Failed { reason: "dropped".into() },
        }
    }

    fn advance_commit(&self, c: &mut Core) {
        if c.role != Role::Leader {
            return;
        }
        let mut v: Vec<u64> = std::iter::once(c.self_durable)
            .chain(self.cfg.peers.keys().map(|p| c.matched.get(p).copied().unwrap_or(0)))
            .collect();
        v.sort_unstable_by(|a, b| b.cmp(a));
        let q = v[self.cfg.quorum() - 1].min(c.reserve);
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
        for (_, w) in std::mem::replace(&mut c.waiters, rest) {
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
        for (_, w) in std::mem::take(&mut c.waiters) {
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
        c.matched = self.cfg.peers.keys().map(|p| (p.clone(), 0)).collect();
        c.next = self.cfg.peers.keys().map(|p| (p.clone(), last + 1)).collect();
        let now = Instant::now();
        c.acked_at = self.cfg.peers.keys().map(|p| (p.clone(), now)).collect();
        self.stats.takeovers.fetch_add(1, Ordering::Relaxed);
        tracing::info!(id = %self.cfg.id, epoch, last, commit, "qlog: leading");
        for p in self.cfg.peers.keys() {
            tokio::spawn(self.clone().replicate(p.clone(), epoch));
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
        let rpc = Rpc::new(&peer, &self.cfg.peers[&peer], self.faults.clone());
        let mut head = self.head.subscribe();
        let mut commit = self.commit.subscribe();
        let mut sent_commit = 0;
        let mut last_send = Instant::now() - self.cfg.heartbeat;
        let mut seg_cache: flush::SegCache = None;
        loop {
            head.borrow_and_update();
            commit.borrow_and_update();
            let (behind, flushed) = {
                let c = self.core.lock();
                let next = c.next.get(&peer).copied().unwrap_or(0);
                let b = (c.role == Role::Leader && c.epoch == epoch && next <= c.log.base().1)
                    .then(|| (next, c.log.base().1));
                (b, c.flushed)
            };
            // behind what's in memory: committed entries from the commitlog,
            // from the oldest it still holds if it doesn't reach back to `next`
            // (a reset there, never past what a takeover would flush from)
            let from_disk = match behind {
                Some((next, base)) => match self.durability.read(next, base, self.cfg.max_batch_bytes).await {
                    Some(r) => Some((next, r, false)),
                    // behind the disk too: from the bucket, if it's flushed
                    None if self.cfg.flush.is_some() && next <= flushed => {
                        match flush::read_bucket(
                            &self.store,
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
                                tracing::warn!(peer, next, "qlog: reading the bucket for a lagging follower failed: {e:#}");
                                tokio::time::sleep(Duration::from_millis(200)).await;
                                continue;
                            }
                        }
                    }
                    None => match self.durability.first_readable() {
                        Some(f) if f + 1 > next && f < base => self
                            .durability
                            .read(f + 1, base, self.cfg.max_batch_bytes)
                            .await
                            .map(|r| (f + 1, r, true)),
                        _ => None,
                    },
                },
                None => None,
            };
            let req = {
                let c = self.core.lock();
                if c.role != Role::Leader || c.epoch != epoch {
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
        c.acked_at.insert(peer.to_string(), Instant::now());
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
            c.flushed = c.flushed.max(a.flushed);
            c.reserve = c.reserve.max(a.reserve);
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
                        let alive = 1 + c.acked_at.values().filter(|t| t.elapsed() < self.cfg.election_timeout).count();
                        if alive < self.cfg.quorum() {
                            self.step_down(&mut c, "no quorum heard within the election timeout");
                        }
                        None
                    }
                    Role::Candidate => None,
                    Role::Follower if c.electing => None,
                    Role::Follower => {
                        let quiet = c.last_heard.elapsed();
                        if quiet > self.cfg.election_timeout && Instant::now() >= c.retry_at {
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
        let dead = match self.ctl.get(&l) {
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
        let rec = read_leader(&self.store).await?;
        let (cur, etag) = match &rec {
            Some((r, e)) => (Some(r.clone()), Some(e.clone())),
            None => (None, None),
        };
        let cur_epoch = cur.as_ref().map_or(0, |r| r.epoch);
        {
            let mut c = self.core.lock();
            if self.heard_since_locked(&c, started) {
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
        let rank =
            self.cfg.members.iter().filter(|m| Some(m.as_str()) != leader).position(|m| *m == self.cfg.id).unwrap_or(0);
        if rank > 0 {
            tokio::time::sleep(self.cfg.stagger * rank as u32).await;
            if self.heard_since(started) {
                return Ok(());
            }
        }
        // A minority never takes over: it can't tell "they're dead" from "I'm
        // cut off", and a CAS from it would only unseat the majority's
        // leader when the partition heals.
        let mut rx = self.broadcast(Msg::Ping { from: self.cfg.id.clone() });
        let mut reachable = 0;
        while reachable + 1 < self.cfg.quorum() {
            match rx.recv().await {
                Some((_, Ok(Msg::Pong))) => reachable += 1,
                Some(_) => {}
                None => break,
            }
        }
        if reachable + 1 < self.cfg.quorum() {
            tracing::debug!(id = %self.cfg.id, reachable, "qlog: can't reach a quorum, not taking over");
            return Ok(());
        }
        if self.heard_since(started) {
            return Ok(());
        }
        let epoch = cur_epoch + 1;
        let rec = LeaderRecord { epoch, leader: self.cfg.id.clone(), members: self.cfg.members.clone() };
        if !cas_leader(&self.store, &rec, etag).await? {
            let mut c = self.core.lock();
            c.last_heard = Instant::now();
            return Ok(());
        }
        let own = {
            let mut c = self.core.lock();
            if c.promised >= epoch {
                return Ok(());
            }
            self.step_down(&mut c, "taking over");
            let me = self.cfg.id.clone();
            self.raise_promised(&mut c, epoch, &me);
            c.epoch = epoch;
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
            {
                let c = self.core.lock();
                if c.intact {
                    let (e, s) = c.log.last();
                    voters.push((self.cfg.id.clone(), e, s));
                }
            }
            let mut rx = self.broadcast(Msg::Promise { epoch, from: self.cfg.id.clone() });
            while voters.len() < self.cfg.quorum() {
                let Some((id, r)) = rx.recv().await else { break };
                if let Ok(Msg::PromiseResp(p)) = r {
                    if p.promised > epoch {
                        let mut c = self.core.lock();
                        self.raise_promised(&mut c, p.promised, "");
                        self.sync(&mut c);
                        self.step_down(&mut c, "a member promised a newer epoch");
                        return Ok(());
                    }
                    if p.ok && p.intact {
                        voters.push((id, p.last_epoch, p.last_seq));
                    }
                }
            }
            if voters.len() < self.cfg.quorum() {
                tracing::warn!(id = %self.cfg.id, epoch, intact = voters.len(), "qlog: no quorum of intact logs yet, retrying");
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

    /// Sends `m` to every peer; replies arrive as they come. Each call runs
    /// to its end on its own task (a dropped call would leave its connection
    /// mid-frame), so a caller waits only for as many as it needs, not for a
    /// member that's down or cut off.
    fn broadcast(&self, m: Msg) -> mpsc::UnboundedReceiver<(String, Result<Msg, CallError>)> {
        let (tx, rx) = mpsc::unbounded_channel();
        for (id, rpc) in &self.ctl {
            let (id, rpc, tx, m) = (id.clone(), rpc.clone(), tx.clone(), m.clone());
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
        let rpc = self.ctl.get(best).ok_or_else(|| anyhow::anyhow!("unknown member {best}"))?;
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
                let (from, upto, events) = {
                    let mut c = self.core.lock();
                    let upto = c.log.commit().min(c.durable);
                    if upto <= c.emitted {
                        break;
                    }
                    let from = c.emitted;
                    let ev: Vec<(i64, Bytes)> =
                        c.log.range(from, upto).map(|e| (e.seq as i64, e.data.clone())).collect();
                    debug_assert_eq!(ev.first().map(|e| e.0 as u64), Some(from + 1));
                    c.emitted = upto;
                    (from, upto, ev)
                };
                self.emit.emit(from, upto, events);
            }
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
