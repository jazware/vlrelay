//! The relay's node log: validated events in, durable relay-sequenced
//! segments out.
//!
//! A DID owner hands `append` a batch of events that passed every check.
//! Each event gets its relay seq (vlpds's time-ordered merge key) and its
//! frame is re-encoded with that seq right then, into the open segment. A
//! segment closes on linger (25 ms by default), size (8 MiB) or count,
//! whichever comes first, and is PUT with If-None-Match at the next dense
//! ordinal. Once a PUT lands (and every earlier one has), its events are
//! acked, handed to the firehose merger, and the log's watermark moves past
//! them. Nothing is acked or emitted before it's in the bucket.
//!
//! What comes from vlpds as is:
//!
//! - The segment format and layout (`segment::SegmentBuilder`, `compress`,
//!   `parse`, `{prefix}/log/{log_id}/{ordinal:012}.seg`, fences). So vlpds's
//!   backfill reader, merger read-back, `first_free` and `prefix_hole` read
//!   relay logs without changes. Each entry's `shard` is the event's DID
//!   shard, and its one mutation (`META_KEY`) carries the did, host and
//!   upstream seq for host checkpoints and replay.
//! - `nodelog::Watermark` for seqs (`unix_micros << 8 | writer`, strictly
//!   increasing, never below the floor it starts at) and the watermark the
//!   merger reads. Its `assign` and `set_durable` were made `pub` for this.
//! - `nodelog::commit_pool` for compression off the runtime, and
//!   `nodelog::LogBatch` as the hand-off to `firehose::Firehose`.
//! - The fencing rule: a node never reopens a log. A restart writes a new
//!   log id, fences the old log at its first free ordinal
//!   (`nodelog::first_free`), and a zombie's next PUT collides with the fence.
//!
//! What's wrapped or rewritten:
//!
//! - The sequencer. vlpds's `NodeLog` seals as soon as a PUT slot is free (no
//!   linger) and applies every segment into per-shard SlateDBs before acking.
//!   A relay wants a linger and has no state to apply, so this one keeps
//!   vlpds's pipelining (K PUTs in flight, completions taken in ordinal
//!   order, `prefix_end` in each header) and adds the linger and count caps.
//! - The upload. Same conditional PUT, hedge and conflict check as vlpds,
//!   but a fence or a foreign segment fails the log (every pending and later
//!   append errs with `Fenced`) instead of exiting the process, so tests can
//!   run two writers in one binary. `on_fatal` is where `main` fail-stops.
//! - Retention (`prune`): vlpds's pruner is tied to shard replay floors. A
//!   relay log has none, so a segment goes once it's past the window, and
//!   `retain/{log_id}` is raised first so `retention::retained_floor` (and
//!   so `OutdatedCursor`) works unchanged.
//!
//! Multi-node is a config change: the cluster supplies `log_id`, a claimed
//! `writer`, a `lease_ok` check and the floor, fences only dead peers' logs,
//! and the serving side follows peer logs with `Firehose::add_remote`.

use bytes::Bytes;
use object_store::path::Path;
use object_store::{ObjectStoreExt, PutMode, PutOptions, PutPayload};
use std::ops::Range;
use std::sync::Arc;
use std::sync::atomic::{AtomicI64, AtomicU64, Ordering};
use std::time::{Duration, Instant};
use tokio::sync::{mpsc, oneshot};
use vlpds::nodelog::{self, Head, LeaseCheck, LogBatch, Watermark};
use vlpds::segment::{self, LogObject, Mutation, SegmentBuilder};
use vlpds::slots::ShardId;
use vlpds::store::Store;

use crate::types::Host;

pub mod dense;

pub const DEFAULT_LINGER: Duration = Duration::from_millis(25);
pub const DEFAULT_MAX_SEGMENT_BYTES: usize = 8 << 20;
pub const DEFAULT_MAX_SEGMENT_EVENTS: usize = 65_536;
/// Past ~50k events/s a segment seals on size, not linger, and its PUT takes
/// 100-400 ms on MinIO: 4 in flight capped a node at ~50k events/s, 16 at
/// ~85k (docs/perf.md). The bytes held are at most this many segments.
pub const DEFAULT_INFLIGHT: usize = 32;
pub const DEFAULT_RETENTION: Duration = Duration::from_secs(72 * 3600);
const LEASE_HOLD_POLL: Duration = Duration::from_millis(10);
/// Durable batches a peer stream may fall behind by before it's dropped.
const LIVE_BATCHES: usize = 4096;

/// The key of the mutation every relay entry carries.
pub const META_KEY: &[u8] = b"relay/meta";
/// The key of an entry's optional second mutation: the DID owner's state
/// delta (`state::StateDelta::encode`), what a shard's next owner replays.
pub const DELTA_KEY: &[u8] = b"relay/delta";

/// Re-encodes an event's frame with its relay seq, straight into the open
/// segment. The verify workstream's `event.rs` owns parsing; anything it
/// produces only has to implement this.
pub trait EncodeWithSeq: Send + Sync + 'static {
    fn encode_with_seq(&self, seq: i64, out: &mut Vec<u8>);
    fn len_hint(&self) -> usize;
}

/// vlpds's split frame (built without a seq) splices it in.
impl EncodeWithSeq for vlpds::events::Frame {
    fn encode_with_seq(&self, seq: i64, out: &mut Vec<u8>) {
        self.finish(seq, out)
    }
    fn len_hint(&self) -> usize {
        Frame::len_hint(self)
    }
}
use vlpds::events::Frame;

/// An upstream frame as received, with the byte range of its body's `seq`
/// value found once (on the caller's task, so the sequencer only copies).
#[derive(Clone, Debug)]
pub struct SeqSplice {
    frame: Bytes,
    at: Range<usize>,
}

impl SeqSplice {
    pub fn parse(frame: Bytes) -> anyhow::Result<SeqSplice> {
        let at = find_seq(&frame).ok_or_else(|| anyhow::anyhow!("frame has no seq field"))?;
        Ok(SeqSplice { frame, at })
    }

    /// The seq the frame carries now (the upstream's).
    pub fn seq(&self) -> Option<i64> {
        let mut i = self.at.start;
        read_int(&self.frame, &mut i)
    }
}

impl EncodeWithSeq for SeqSplice {
    fn encode_with_seq(&self, seq: i64, out: &mut Vec<u8>) {
        out.extend_from_slice(&self.frame[..self.at.start]);
        write_int(out, seq);
        out.extend_from_slice(&self.frame[self.at.end..]);
    }
    fn len_hint(&self) -> usize {
        self.frame.len() + 9
    }
}

/// What the log keeps next to each frame: enough to rebuild host
/// checkpoints and per-DID state from a log tail.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct EventMeta {
    pub did: String,
    pub host: Host,
    pub upstream_seq: i64,
    /// The event's DID shard (0 on a single node).
    pub shard: u32,
}

impl EventMeta {
    fn encode(&self) -> Bytes {
        let mut b = Vec::with_capacity(12 + self.did.len() + self.host.0.len());
        b.extend_from_slice(&(self.did.len() as u16).to_be_bytes());
        b.extend_from_slice(self.did.as_bytes());
        b.extend_from_slice(&(self.host.0.len() as u16).to_be_bytes());
        b.extend_from_slice(self.host.0.as_bytes());
        b.extend_from_slice(&self.upstream_seq.to_be_bytes());
        b.into()
    }

    fn decode(b: &[u8], shard: u32) -> Option<EventMeta> {
        let n = u16::from_be_bytes(b.get(..2)?.try_into().ok()?) as usize;
        let did = std::str::from_utf8(b.get(2..2 + n)?).ok()?.to_string();
        let p = 2 + n;
        let m = u16::from_be_bytes(b.get(p..p + 2)?.try_into().ok()?) as usize;
        let host = std::str::from_utf8(b.get(p + 2..p + 2 + m)?).ok()?.to_string();
        let q = p + 2 + m;
        let upstream_seq = i64::from_be_bytes(b.get(q..q + 8)?.try_into().ok()?);
        (b.len() == q + 8).then_some(EventMeta { did, host: Host(host), upstream_seq, shard })
    }
}

pub struct Event {
    pub meta: EventMeta,
    pub frame: Box<dyn EncodeWithSeq>,
    pub delta: Option<Bytes>,
}

/// An appended batch, durable.
#[derive(Clone, Debug)]
pub struct Durable {
    /// Relay seqs, one per event, in the order given.
    pub seqs: Vec<i64>,
    /// The segment holding the batch's last event.
    pub ordinal: u64,
}

#[derive(Clone, Debug, thiserror::Error, PartialEq, Eq)]
pub enum LogError {
    /// Another writer took this log's next ordinal (a successor's fence, or
    /// a second process on the same log id). Nothing from here on is acked.
    #[error("node log fenced: {0}")]
    Fenced(String),
    #[error("node lease lapsed")]
    LeaseLapsed,
    /// Stopped (shutdown or a test crash) before the batch was durable.
    #[error("node log closed")]
    Closed,
}

pub struct LogConfig {
    pub log_id: String,
    pub writer: u8,
    pub linger: Duration,
    pub max_segment_bytes: usize,
    pub max_segment_events: usize,
    /// PUTs in flight (K).
    pub inflight: usize,
    /// A PUT slower than this gets one hedged duplicate.
    pub hedge_after: Duration,
    /// Every seq this log assigns is above this.
    pub seq_floor: i64,
    pub lease_ok: Option<LeaseCheck>,
    /// A log idle this long PUTs an empty segment carrying a fresh seq, so
    /// followers that only read the bucket (replicas) see its watermark
    /// move. None: never (a log nobody follows from the bucket alone).
    pub idle_heartbeat: Option<Duration>,
    /// How long a lapsed lease holds the log (nothing sealed, nothing
    /// acked) before it fails. A cluster that revalidates a lapsed lease
    /// (vlpds's `Cluster::set_revalidate`) sets its revalidation window, and
    /// fail-stops by itself if the lease is really lost. Zero: fail at once.
    pub lapse_grace: Duration,
}

impl LogConfig {
    pub fn new(log_id: impl Into<String>) -> LogConfig {
        LogConfig {
            log_id: log_id.into(),
            writer: 0,
            linger: DEFAULT_LINGER,
            max_segment_bytes: DEFAULT_MAX_SEGMENT_BYTES,
            max_segment_events: DEFAULT_MAX_SEGMENT_EVENTS,
            inflight: DEFAULT_INFLIGHT,
            hedge_after: Duration::from_secs(2),
            seq_floor: nodelog::seq_floor(vlpds::tid::now_micros()),
            lease_ok: None,
            idle_heartbeat: None,
            lapse_grace: Duration::ZERO,
        }
    }
}

/// A fresh log id: a node never reopens a log.
pub fn new_log_id(node: &str) -> String {
    let ms = std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH).unwrap_or_default().as_millis();
    format!("{node}-{ms:013}-{:08x}", rand::random::<u32>())
}

type Ack = oneshot::Sender<Result<Durable, LogError>>;

/// Runs once when the log fails for good (fenced, or its lease lapsed):
/// where `main` fail-stops.
pub type OnFatal = Box<dyn FnOnce(&LogError) + Send>;

struct Pending {
    events: Vec<Event>,
    ack: Ack,
    enqueued: Instant,
}

/// A submitted batch; resolves once it's durable.
pub struct Ticket(oneshot::Receiver<Result<Durable, LogError>>);

impl std::future::Future for Ticket {
    type Output = Result<Durable, LogError>;
    fn poll(mut self: std::pin::Pin<&mut Self>, cx: &mut std::task::Context<'_>) -> std::task::Poll<Self::Output> {
        std::pin::Pin::new(&mut self.0).poll(cx).map(|r| r.unwrap_or(Err(LogError::Closed)))
    }
}

#[derive(Default)]
pub struct LogStats {
    pub segments: AtomicU64,
    pub events: AtomicU64,
    pub bytes: AtomicU64,
    pub stored_bytes: AtomicU64,
    pub hedges: AtomicU64,
    /// Sum of append-to-durable latencies in µs (for a mean).
    pub latency_us: AtomicU64,
}

pub struct NodeLog {
    pub log_id: Arc<str>,
    pub wm: Arc<Watermark>,
    tx: mpsc::Sender<Pending>,
    failed: Arc<parking_lot::Mutex<Option<LogError>>>,
    /// Last durable ordinal (u64::MAX = none yet).
    pub durable_ordinal: Arc<AtomicU64>,
    pub last_durable_seq: Arc<AtomicI64>,
    /// The ordinal the next sealed segment gets: a lower bound on any entry
    /// appended from now on (a cluster span starts here).
    pub next_ordinal: Arc<AtomicU64>,
    pub stats: Arc<LogStats>,
    /// When the oldest append not yet durable was submitted (None: none).
    oldest_pending: Arc<parking_lot::Mutex<Option<Instant>>>,
    /// Durable batches as they finalize, for peers streaming this log
    /// (`cluster::follow`). A lagging receiver catches up from the bucket.
    live: tokio::sync::broadcast::Sender<Arc<LogBatch>>,
    task: parking_lot::Mutex<Option<tokio::task::JoinHandle<()>>>,
}

impl NodeLog {
    /// Starts a new log. Durable batches go to `out` (the firehose merger).
    /// `on_fatal` runs once if the log is fenced or its lease lapses.
    pub fn start(
        store: Store,
        cfg: LogConfig,
        out: mpsc::UnboundedSender<LogBatch>,
        on_fatal: Option<OnFatal>,
    ) -> Arc<NodeLog> {
        let wm = Arc::new(Watermark::new(cfg.writer, cfg.seq_floor));
        let (tx, rx) = mpsc::channel(16 * 1024);
        let failed = Arc::new(parking_lot::Mutex::new(None));
        let durable_ordinal = Arc::new(AtomicU64::new(u64::MAX));
        let last_durable_seq = Arc::new(AtomicI64::new(cfg.seq_floor));
        let next_ordinal = Arc::new(AtomicU64::new(0));
        let stats = Arc::new(LogStats::default());
        let oldest_pending = Arc::new(parking_lot::Mutex::new(None));
        let log_id: Arc<str> = cfg.log_id.clone().into();
        let (live, _) = tokio::sync::broadcast::channel(LIVE_BATCHES);
        let seq = Sequencer {
            store,
            log_id: log_id.clone(),
            cfg,
            wm: wm.clone(),
            out,
            failed: failed.clone(),
            durable_ordinal: durable_ordinal.clone(),
            last_durable_seq: last_durable_seq.clone(),
            next_ordinal: next_ordinal.clone(),
            stats: stats.clone(),
            live: live.clone(),
            on_fatal,
            lapsed_at: None,
            oldest_pending: oldest_pending.clone(),
        };
        let task = tokio::spawn(seq.run(rx));
        Arc::new(NodeLog {
            log_id,
            wm,
            tx,
            failed,
            durable_ordinal,
            last_durable_seq,
            next_ordinal,
            stats,
            oldest_pending,
            live,
            task: parking_lot::Mutex::new(Some(task)),
        })
    }

    /// Queues a batch (waiting while the log's queue is full, which is the
    /// backpressure). The ticket resolves when every event in it is durable.
    pub async fn submit(&self, events: Vec<Event>) -> Ticket {
        let (ack, rx) = oneshot::channel();
        if let Some(e) = self.failed.lock().clone() {
            let _ = ack.send(Err(e));
            return Ticket(rx);
        }
        let p = Pending { events, ack, enqueued: Instant::now() };
        if let Err(mpsc::error::SendError(p)) = self.tx.send(p).await {
            let _ = p.ack.send(Err(self.failed.lock().clone().unwrap_or(LogError::Closed)));
        }
        Ticket(rx)
    }

    /// Appends a batch and waits until it's durable.
    pub async fn append(&self, events: Vec<Event>) -> Result<Durable, LogError> {
        self.submit(events).await.await
    }

    pub fn failed(&self) -> Option<LogError> {
        self.failed.lock().clone()
    }

    /// Every durable batch from now on, in ordinal order.
    pub fn live(&self) -> tokio::sync::broadcast::Receiver<Arc<LogBatch>> {
        self.live.subscribe()
    }

    /// How long the oldest append not yet durable has waited (zero when
    /// none): what a slow bucket path does to this log.
    pub fn pending_age(&self) -> Duration {
        self.oldest_pending.lock().map_or(Duration::ZERO, |t| t.elapsed())
    }

    /// True once nothing appended is still waiting to be durable.
    pub fn idle(&self) -> bool {
        self.wm.idle()
    }

    /// Stops dead, as a crash would: nothing queued or in flight is acked,
    /// and PUTs not yet sent never land.
    pub fn halt(&self) {
        if let Some(t) = self.task.lock().take() {
            t.abort();
        }
    }

    /// A graceful stop: drains what's queued (acking it), then fences this
    /// log at its next ordinal so nothing can append to it again.
    pub async fn close(&self, store: &Store) -> anyhow::Result<()> {
        // An empty batch is acked once everything queued before it is
        // durable (segments complete in ordinal order).
        let drained = self.append(Vec::new()).await;
        if let Some(t) = self.task.lock().take() {
            t.abort();
        }
        drained.map_err(|e| anyhow::anyhow!("{e}"))?;
        fence(store, &self.log_id, &format!("{}:close", self.log_id)).await?;
        Ok(())
    }
}

struct Sequencer {
    store: Store,
    log_id: Arc<str>,
    cfg: LogConfig,
    wm: Arc<Watermark>,
    out: mpsc::UnboundedSender<LogBatch>,
    failed: Arc<parking_lot::Mutex<Option<LogError>>>,
    durable_ordinal: Arc<AtomicU64>,
    last_durable_seq: Arc<AtomicI64>,
    next_ordinal: Arc<AtomicU64>,
    stats: Arc<LogStats>,
    live: tokio::sync::broadcast::Sender<Arc<LogBatch>>,
    on_fatal: Option<OnFatal>,
    /// When the lease was first seen lapsed (None while it's valid).
    lapsed_at: Option<Instant>,
    oldest_pending: Arc<parking_lot::Mutex<Option<Instant>>>,
}

enum Lease {
    Valid,
    /// Lapsed, within `lapse_grace`: hold.
    Hold,
    Lapsed,
}

/// Events being gathered into the next segment.
struct Open {
    seg: SegmentBuilder,
    frames: Vec<(i64, Range<usize>)>,
    /// (ack, seqs for it, enqueued)
    acks: Vec<(Ack, Vec<i64>, Instant)>,
    opened: Option<Instant>,
    /// An empty segment that only moves the watermark.
    heartbeat: bool,
}

impl Open {
    fn new(log_id: &str) -> Open {
        Open {
            seg: SegmentBuilder::for_log(log_id),
            frames: Vec::new(),
            acks: Vec::new(),
            opened: None,
            heartbeat: false,
        }
    }

    fn push(&mut self, wm: &Watermark, p: Pending) {
        self.opened.get_or_insert_with(Instant::now);
        let mut seqs = Vec::with_capacity(p.events.len());
        for ev in p.events {
            let seq = wm.assign();
            let meta = Mutation { key: Bytes::from_static(META_KEY), val: Some(ev.meta.encode()) };
            let muts = match ev.delta {
                Some(d) => vec![meta, Mutation { key: Bytes::from_static(DELTA_KEY), val: Some(d) }],
                None => vec![meta],
            };
            let range = self.seg.push(seq, ShardId(ev.meta.shard), 0, |out| ev.frame.encode_with_seq(seq, out), &muts);
            self.frames.push((seq, range));
            seqs.push(seq);
        }
        self.acks.push((p.ack, seqs, p.enqueued));
    }
}

struct Sealed {
    ordinal: u64,
    data: Bytes,
    frames: Vec<(i64, Range<usize>)>,
    acks: Vec<(Ack, Vec<i64>, Instant)>,
    last_seq: i64,
    stored_bytes: usize,
    heartbeat: bool,
}

/// Aborts a spawned upload when the sequencer drops it (a halt).
struct AbortOnDrop(tokio::task::JoinHandle<(Sealed, Result<(), LogError>)>);

impl Drop for AbortOnDrop {
    fn drop(&mut self) {
        self.0.abort();
    }
}

impl std::future::Future for AbortOnDrop {
    type Output = Result<(Sealed, Result<(), LogError>), tokio::task::JoinError>;
    fn poll(mut self: std::pin::Pin<&mut Self>, cx: &mut std::task::Context<'_>) -> std::task::Poll<Self::Output> {
        std::pin::Pin::new(&mut self.0).poll(cx)
    }
}

impl Sequencer {
    fn lease(&mut self) -> Lease {
        match &self.cfg.lease_ok {
            Some(ok) if !ok() => {
                let at = *self.lapsed_at.get_or_insert_with(Instant::now);
                if at.elapsed() < self.cfg.lapse_grace { Lease::Hold } else { Lease::Lapsed }
            }
            _ => {
                self.lapsed_at = None;
                Lease::Valid
            }
        }
    }

    /// A segment landed: ack it only under a valid lease, waiting out a
    /// lapse within the grace.
    async fn lease_for_ack(&mut self) -> Result<(), LogError> {
        loop {
            match self.lease() {
                Lease::Valid => return Ok(()),
                Lease::Lapsed => return Err(LogError::LeaseLapsed),
                Lease::Hold => tokio::time::sleep(LEASE_HOLD_POLL).await,
            }
        }
    }

    fn fail(&mut self, e: LogError) {
        let mut f = self.failed.lock();
        if f.is_none() {
            tracing::error!(log_id = %self.log_id, "node log failed: {e}");
            *f = Some(e.clone());
            drop(f);
            if let Some(cb) = self.on_fatal.take() {
                cb(&e);
            }
        }
    }

    async fn run(mut self, mut rx: mpsc::Receiver<Pending>) {
        use futures::StreamExt;
        use futures::stream::FuturesOrdered;
        let k = self.cfg.inflight.max(1);
        let mut ordinal = 0u64;
        let mut prefix_end = 0u64;
        let mut open = Open::new(&self.log_id);
        let mut inflight: FuturesOrdered<AbortOnDrop> = FuturesOrdered::new();
        // per segment in flight, in ordinal order: its oldest append
        let mut inflight_oldest: std::collections::VecDeque<Option<Instant>> = Default::default();
        let mut closed = false;
        let mut last_sealed = Instant::now();
        // sealing is held for a lapsed lease: retry then
        let mut retry_at: Option<Instant> = None;
        loop {
            let heartbeat_at = match self.cfg.idle_heartbeat {
                Some(h) if inflight.is_empty() && open.opened.is_none() && !closed => Some(last_sealed + h),
                _ => None,
            };
            let full = open.seg.len() >= self.cfg.max_segment_bytes || open.frames.len() >= self.cfg.max_segment_events;
            let linger_at = open.opened.map(|t| t + self.cfg.linger);
            let can_seal_later = inflight.len() < k && linger_at.is_some() && !full && retry_at.is_none();
            tokio::select! {
                biased;
                r = inflight.next(), if !inflight.is_empty() => {
                    let Some(r) = r else { continue };
                    inflight_oldest.pop_front();
                    let (sealed, res) = match r {
                        Ok(v) => v,
                        Err(e) if e.is_cancelled() => return,
                        Err(e) => {
                            tracing::error!(log_id = %self.log_id, "segment upload task failed: {e}");
                            self.fail(LogError::Closed);
                            return;
                        }
                    };
                    // a close marker holds the next ordinal without writing it
                    if !sealed.data.is_empty() {
                        prefix_end = sealed.ordinal + 1;
                    }
                    let res = match res {
                        Ok(()) => self.lease_for_ack().await,
                        Err(e) => Err(e),
                    };
                    let prior = self.failed.lock().clone();
                    match (res, prior) {
                        (Ok(()), None) => self.finalize(sealed),
                        // a segment after a failure may have landed past a
                        // fence: never ack it
                        (Ok(()), Some(e)) | (Err(e), _) => {
                            self.fail(e.clone());
                            for (ack, _, _) in sealed.acks {
                                let _ = ack.send(Err(e.clone()));
                            }
                        }
                    }
                }
                p = rx.recv(), if !closed && !full => match p {
                    Some(p) => {
                        if let Some(e) = self.failed.lock().clone() {
                            let _ = p.ack.send(Err(e));
                            continue;
                        }
                        open.push(&self.wm, p);
                        while open.seg.len() < self.cfg.max_segment_bytes
                            && open.frames.len() < self.cfg.max_segment_events
                        {
                            match rx.try_recv() {
                                Ok(p) => open.push(&self.wm, p),
                                Err(_) => break,
                            }
                        }
                    }
                    None => closed = true,
                },
                _ = tokio::time::sleep_until(tokio::time::Instant::from_std(linger_at.unwrap_or_else(Instant::now))), if can_seal_later => {}
                _ = tokio::time::sleep_until(tokio::time::Instant::from_std(retry_at.unwrap_or_else(Instant::now))), if retry_at.is_some() => {
                    retry_at = None;
                }
                _ = tokio::time::sleep_until(tokio::time::Instant::from_std(heartbeat_at.unwrap_or_else(Instant::now))), if heartbeat_at.is_some() => {
                    let seq = self.wm.assign();
                    open.seg.first_seq = seq;
                    open.seg.last_seq = seq;
                    open.heartbeat = true;
                    open.opened = Some(Instant::now() - self.cfg.linger);
                }
            }
            let failed = self.failed.lock().clone();
            if let Some(e) = failed {
                for (ack, _, _) in std::mem::replace(&mut open, Open::new(&self.log_id)).acks {
                    let _ = ack.send(Err(e.clone()));
                }
            }
            let due = open.opened.is_some_and(|t| t.elapsed() >= self.cfg.linger)
                || open.seg.len() >= self.cfg.max_segment_bytes
                || open.frames.len() >= self.cfg.max_segment_events
                || closed;
            if inflight.len() < k && open.opened.is_some() && due && retry_at.is_none() {
                match self.lease() {
                    Lease::Valid => {}
                    Lease::Hold => {
                        retry_at = Some(Instant::now() + LEASE_HOLD_POLL);
                        continue;
                    }
                    Lease::Lapsed => {
                        self.fail(LogError::LeaseLapsed);
                        continue;
                    }
                }
                let o = std::mem::replace(&mut open, Open::new(&self.log_id));
                if inflight.is_empty() {
                    prefix_end = ordinal;
                }
                let last_seq = o.seg.last_seq;
                // an empty segment (a close marker): nothing to PUT, ack in order
                let data = if o.frames.is_empty() && !o.heartbeat {
                    Bytes::new()
                } else {
                    Bytes::from(o.seg.seal(&self.log_id, ordinal, prefix_end))
                };
                let empty = data.is_empty();
                let sealed = Sealed {
                    ordinal,
                    data,
                    frames: o.frames,
                    acks: o.acks,
                    last_seq,
                    stored_bytes: 0,
                    heartbeat: o.heartbeat,
                };
                last_sealed = Instant::now();
                inflight_oldest.push_back(sealed.acks.first().map(|a| a.2));
                if !empty {
                    ordinal += 1;
                    self.next_ordinal.store(ordinal, Ordering::Release);
                }
                let (store, log_id, hedge, stats) =
                    (self.store.clone(), self.log_id.clone(), self.cfg.hedge_after, self.stats.clone());
                inflight.push_back(AbortOnDrop(tokio::spawn(async move {
                    let mut sealed = sealed;
                    if sealed.data.is_empty() {
                        return (sealed, Ok(()));
                    }
                    let raw = sealed.data.clone();
                    let put = nodelog::commit_pool()
                        .run(move || match segment::compress(&raw, segment::compression_level()) {
                            Ok(Some(z)) => Bytes::from(z),
                            _ => raw,
                        })
                        .await
                        .expect("segment compression task");
                    sealed.stored_bytes = put.len();
                    let t = Instant::now();
                    let r = upload(&store, &log_id, sealed.ordinal, put, hedge, &stats).await;
                    vlpds::metrics::PUT_DURATION.with_label_values(&["relay"]).observe(t.elapsed().as_secs_f64());
                    (sealed, r)
                })));
            }
            *self.oldest_pending.lock() =
                inflight_oldest.iter().flatten().next().copied().or_else(|| open.acks.first().map(|a| a.2));
            if closed && inflight.is_empty() && open.opened.is_none() {
                return;
            }
        }
    }

    fn finalize(&mut self, s: Sealed) {
        if !s.frames.is_empty() || s.heartbeat {
            let events: Vec<(i64, Bytes)> = s.frames.iter().map(|(seq, r)| (*seq, s.data.slice(r.clone()))).collect();
            // to the merger before the watermark moves: it reads the
            // watermark first, then drains (firehose.rs)
            let batch = LogBatch { log_id: self.log_id.clone(), ordinal: s.ordinal, events };
            if self.live.receiver_count() > 0 {
                let _ = self.live.send(Arc::new(batch.clone()));
            }
            let _ = self.out.send(batch);
            self.wm.set_durable(s.last_seq);
            self.durable_ordinal.store(s.ordinal, Ordering::Release);
            self.last_durable_seq.store(s.last_seq, Ordering::Release);
            self.stats.segments.fetch_add(1, Ordering::Relaxed);
            self.stats.events.fetch_add(s.frames.len() as u64, Ordering::Relaxed);
            self.stats.bytes.fetch_add(s.data.len() as u64, Ordering::Relaxed);
            self.stats.stored_bytes.fetch_add(s.stored_bytes as u64, Ordering::Relaxed);
        }
        let ordinal =
            if s.frames.is_empty() && !s.heartbeat { self.durable_ordinal.load(Ordering::Acquire) } else { s.ordinal };
        let now = Instant::now();
        for (ack, seqs, enq) in s.acks {
            self.stats.latency_us.fetch_add(now.saturating_duration_since(enq).as_micros() as u64, Ordering::Relaxed);
            let _ = ack.send(Ok(Durable { seqs, ordinal }));
        }
    }
}

async fn put_once(store: &Store, path: &Path, data: Bytes) -> object_store::Result<()> {
    store.inject_latency().await;
    let opts = PutOptions { mode: PutMode::Create, ..Default::default() };
    store.raw.put_opts(path, PutPayload::from_bytes(data), opts).await.map(|_| ())
}

/// PUTs a segment with If-None-Match until it's durable, with one hedge for
/// a slow attempt (vlpds's `nodelog::upload`). A fence or someone else's
/// segment at our ordinal is fatal for the log.
async fn upload(
    store: &Store,
    log_id: &str,
    ordinal: u64,
    data: Bytes,
    hedge_after: Duration,
    stats: &LogStats,
) -> Result<(), LogError> {
    use futures::stream::{FuturesUnordered, StreamExt};
    let path = nodelog::segment_path(store, log_id, ordinal);
    let mut backoff = Duration::from_millis(20);
    let mut hedged = false;
    loop {
        let mut attempts = FuturesUnordered::new();
        attempts.push(put_once(store, &path, data.clone()));
        let hedge = tokio::time::sleep(hedge_after);
        tokio::pin!(hedge);
        let result = loop {
            tokio::select! {
                Some(r) = attempts.next() => match r {
                    Ok(()) | Err(object_store::Error::AlreadyExists { .. }) => break r,
                    Err(e) if attempts.is_empty() => break Err(e),
                    Err(_) => continue,
                },
                _ = &mut hedge, if !hedged => {
                    hedged = true;
                    stats.hedges.fetch_add(1, Ordering::Relaxed);
                    attempts.push(put_once(store, &path, data.clone()));
                }
            }
        };
        match result {
            Ok(()) => return Ok(()),
            Err(object_store::Error::AlreadyExists { .. }) => {
                let got = match store.raw.get(&path).await {
                    Ok(r) => r.bytes().await,
                    Err(e) => Err(e),
                };
                match got {
                    Ok(b) if b == data => return Ok(()),
                    Ok(b) => {
                        let why = match segment::parse(b, false, None) {
                            Ok(LogObject::Fence { by }) => {
                                format!("fenced by {by} at ordinal {ordinal}")
                            }
                            _ => format!("ordinal {ordinal} taken by another writer"),
                        };
                        return Err(LogError::Fenced(why));
                    }
                    // S3 answers 409 on conditional-write races too (our own hedge)
                    Err(object_store::Error::NotFound { .. }) => {}
                    Err(e) => {
                        tracing::warn!(log_id, ordinal, "reading a conflicting segment failed: {e}")
                    }
                }
            }
            Err(e) => tracing::warn!(log_id, ordinal, "segment PUT failed, retrying: {e}"),
        }
        let jitter = rand::Rng::gen_range(&mut rand::thread_rng(), 0.5..1.5);
        tokio::time::sleep(backoff.mul_f64(jitter)).await;
        backoff = (backoff * 2).min(Duration::from_secs(2));
    }
}

/// Closes `log_id` for good: a fence object at its first free ordinal.
/// Returns that ordinal and the log's last durable seq (0 if empty).
pub async fn fence(store: &Store, log_id: &str, by: &str) -> anyhow::Result<(u64, i64)> {
    loop {
        let (free, fenced) = nodelog::first_free(store, log_id).await?;
        if !fenced {
            let path = nodelog::segment_path(store, log_id, free);
            let opts = PutOptions { mode: PutMode::Create, ..Default::default() };
            match store.raw.put_opts(&path, PutPayload::from_bytes(segment::fence_object(by)), opts).await {
                Ok(_) => {}
                // the writer got there first: its log moved, look again
                Err(object_store::Error::AlreadyExists { .. }) => continue,
                Err(e) => return Err(e.into()),
            }
        }
        let mut last = 0;
        if free > 0
            && let Head::Segment(h) = nodelog::read_head(store, log_id, free - 1).await?
        {
            last = h.last_seq;
        }
        return Ok((free, last));
    }
}

/// What startup found in the bucket.
#[derive(Debug, Default)]
pub struct Recovered {
    /// (log id, fence ordinal, last durable seq)
    pub logs: Vec<(String, u64, i64)>,
    /// Every durable seq of every earlier log is at or below this.
    pub seq_floor: i64,
}

/// Single-node startup: every log in the bucket belongs to an earlier
/// incarnation (or a zombie of one), so fence them all. Their durable
/// events stay readable for backfill, and a zombie's next PUT fails.
pub async fn fence_all(store: &Store, by: &str) -> anyhow::Result<Recovered> {
    let mut r = Recovered::default();
    for log_id in vlpds::backfill::list_logs(store).await? {
        let (ord, last) = fence(store, &log_id, by).await?;
        r.seq_floor = r.seq_floor.max(last);
        r.logs.push((log_id, ord, last));
    }
    Ok(r)
}

/// One logged event read back for replay.
#[derive(Clone, Debug)]
pub struct Logged {
    pub seq: i64,
    pub meta: EventMeta,
    pub frame: Bytes,
    pub delta: Option<Bytes>,
}

/// The events of one segment (None: missing; Some(empty) for a fence).
pub async fn read_segment(store: &Store, log_id: &str, ordinal: u64) -> anyhow::Result<Option<Vec<Logged>>> {
    let path = nodelog::segment_path(store, log_id, ordinal);
    let data = match store.raw.get(&path).await {
        Ok(r) => r.bytes().await?,
        Err(object_store::Error::NotFound { .. }) => return Ok(None),
        Err(e) => return Err(e.into()),
    };
    match segment::parse(data, true, None)? {
        LogObject::Fence { .. } => Ok(Some(Vec::new())),
        LogObject::Segment(h, entries) => {
            nodelog::check_header(&h, log_id, ordinal)?;
            entries
                .into_iter()
                .map(|e| {
                    let meta = e
                        .muts
                        .iter()
                        .find(|m| &m.key[..] == META_KEY)
                        .and_then(|m| EventMeta::decode(m.val.as_deref()?, e.shard.0))
                        .ok_or_else(|| {
                            anyhow::anyhow!("segment {log_id}/{ordinal}: entry {} has no relay meta", e.seq)
                        })?;
                    let delta = e.muts.iter().find(|m| &m.key[..] == DELTA_KEY).and_then(|m| m.val.clone());
                    Ok(Logged { seq: e.seq, meta, frame: e.frame, delta })
                })
                .collect::<anyhow::Result<Vec<_>>>()
                .map(Some)
        }
    }
}

/// Every durable event of a log from `from` on, up to its first free
/// ordinal (the gap-free prefix: a crash's holes end it).
pub async fn read_log(store: &Store, log_id: &str, from: u64) -> anyhow::Result<Vec<Logged>> {
    let (free, _) = nodelog::first_free(store, log_id).await?;
    let mut out = Vec::new();
    for ord in from..free {
        if let Some(evs) = read_segment(store, log_id, ord).await? {
            out.extend(evs);
        }
    }
    Ok(out)
}

/// What a retention pass did.
#[derive(Debug, Default, Clone)]
pub struct Pruned {
    pub deleted: usize,
    pub pruned_seq: i64,
}

/// Deletes segments whose events are all older than `window`, oldest
/// first, for every log in the bucket. A log's newest segment stays (it's
/// how `first_free` finds the end of the log), and fences stay. The
/// retained floor (`retain/{reporter}`) goes up before anything is deleted,
/// so a cursor below it gets `OutdatedCursor` instead of a silent gap.
pub async fn prune(store: &Store, reporter: &str, window: Duration, max_deletes: usize) -> anyhow::Result<Pruned> {
    use futures::StreamExt;
    let cutoff = nodelog::seq_floor(vlpds::tid::now_micros().saturating_sub(window.as_micros() as u64));
    // Stream seqs are counted from a checkpoint, so cut at one: everything
    // after it stays numbered. What's left below it is served from it
    // (OutdatedCursor), at most a checkpoint interval late.
    let Some(cutoff) = dense::newest_at_or_below(store, cutoff).await? else { return Ok(Pruned::default()) };
    let mut doomed: Vec<(Path, i64)> = Vec::new();
    for log_id in vlpds::backfill::list_logs(store).await? {
        let prefix = Path::from(format!("{}/log/{}", store.prefix, log_id));
        // Keys list in ordinal order, so the first page holds the oldest
        // segments. Reading only that much keeps a pass at one LIST per log
        // however long the log is.
        let room = max_deletes.saturating_sub(doomed.len());
        let mut ords: Vec<u64> = Vec::new();
        let mut list = store.raw.list(Some(&prefix)).take(room + 1);
        while let Some(m) = list.next().await {
            if let Some(o) = m?.location.filename().and_then(|f| f.strip_suffix(".seg")).and_then(|f| f.parse().ok()) {
                ords.push(o);
            }
        }
        // The newest object stays (a live log's last segment, or a dead
        // log's fence). It's the last one listed unless the page was cut.
        if ords.len() <= room {
            ords.pop();
        } else {
            ords.truncate(room);
        }
        for ord in ords {
            match nodelog::read_head(store, &log_id, ord).await? {
                Head::Segment(h) if h.last_seq < cutoff => {
                    doomed.push((nodelog::segment_path(store, &log_id, ord), h.last_seq))
                }
                _ => break,
            }
        }
    }
    let mut out = Pruned::default();
    if doomed.is_empty() {
        return Ok(out);
    }
    out.pruned_seq = doomed.iter().map(|d| d.1).max().unwrap_or(0);
    let report_path = Path::from(format!("{}/retain/{}", store.prefix, reporter));
    let prev = match store.raw.get(&report_path).await {
        Ok(r) => serde_json::from_slice::<vlpds::retention::Report>(&r.bytes().await?)?.pruned_seq,
        Err(object_store::Error::NotFound { .. }) => 0,
        Err(e) => return Err(e.into()),
    };
    if out.pruned_seq > prev {
        let rep = vlpds::retention::Report::new(Default::default(), out.pruned_seq, vlpds::version::active());
        store.raw.put(&report_path, PutPayload::from(serde_json::to_vec(&rep)?)).await?;
    }
    for (p, _) in doomed {
        match store.raw.delete(&p).await {
            Ok(()) | Err(object_store::Error::NotFound { .. }) => out.deleted += 1,
            Err(e) => return Err(e.into()),
        }
    }
    dense::prune_below(store, out.pruned_seq.max(prev), max_deletes).await?;
    Ok(out)
}

// ---- just enough CBOR to find and write a frame's seq ----

fn head(b: &[u8], i: &mut usize) -> Option<(u8, u64)> {
    let ib = *b.get(*i)?;
    *i += 1;
    let (major, info) = (ib >> 5, ib & 0x1f);
    let n = match info {
        0..=23 => info as u64,
        24..=27 => {
            let len = 1usize << (info - 24);
            let v = b.get(*i..*i + len)?.iter().fold(0u64, |a, &x| (a << 8) | x as u64);
            *i += len;
            v
        }
        _ => return None,
    };
    Some((major, n))
}

fn skip(b: &[u8], i: &mut usize, depth: u32) -> Option<()> {
    if depth > 64 {
        return None;
    }
    let (major, n) = head(b, i)?;
    match major {
        2 | 3 => {
            *i = i.checked_add(usize::try_from(n).ok()?)?;
            (*i <= b.len()).then_some(())?;
        }
        4 => {
            for _ in 0..n {
                skip(b, i, depth + 1)?;
            }
        }
        5 => {
            for _ in 0..n.checked_mul(2)? {
                skip(b, i, depth + 1)?;
            }
        }
        6 => skip(b, i, depth + 1)?,
        _ => {}
    }
    Some(())
}

fn read_int(b: &[u8], i: &mut usize) -> Option<i64> {
    match head(b, i)? {
        (0, n) => i64::try_from(n).ok(),
        (1, n) => Some(-1 - i64::try_from(n).ok()?),
        _ => None,
    }
}

/// The byte range of the `seq` value in a frame's body map.
pub fn find_seq(frame: &[u8]) -> Option<Range<usize>> {
    let mut i = 0;
    skip(frame, &mut i, 0)?;
    let (major, n) = head(frame, &mut i)?;
    if major != 5 {
        return None;
    }
    for _ in 0..n {
        let k0 = i;
        let (km, kl) = head(frame, &mut i)?;
        if km == 3 && frame.get(i..i + kl as usize)? == b"seq" {
            i += 3;
            let v0 = i;
            read_int(frame, &mut i)?;
            return Some(v0..i);
        }
        i = k0;
        skip(frame, &mut i, 0)?;
        skip(frame, &mut i, 0)?;
    }
    None
}

/// A frame's `seq`, for consumers and tests.
pub fn frame_seq(frame: &[u8]) -> Option<i64> {
    let r = find_seq(frame)?;
    let mut i = r.start;
    read_int(frame, &mut i)
}

fn write_int(out: &mut Vec<u8>, v: i64) {
    let (major, n) = if v >= 0 { (0u8, v as u64) } else { (1u8, (-1 - v) as u64) };
    let m = major << 5;
    if n < 24 {
        out.push(m | n as u8);
    } else if n <= u8::MAX as u64 {
        out.extend_from_slice(&[m | 24, n as u8]);
    } else if n <= u16::MAX as u64 {
        out.push(m | 25);
        out.extend_from_slice(&(n as u16).to_be_bytes());
    } else if n <= u32::MAX as u64 {
        out.push(m | 26);
        out.extend_from_slice(&(n as u32).to_be_bytes());
    } else {
        out.push(m | 27);
        out.extend_from_slice(&n.to_be_bytes());
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn splice_rewrites_only_the_seq() {
        let f = vlpds::events::sync_frame("did:plc:abc", "3jzfcijpj2z2a", &[7u8; 300], "2026-10-04T00:00:00Z");
        let mut up = Vec::new();
        f.finish(42, &mut up);
        let s = SeqSplice::parse(Bytes::from(up.clone())).unwrap();
        assert_eq!(s.seq(), Some(42));
        let big = (1_759_000_000_000_000i64 << 8) | 3;
        let mut out = Vec::new();
        s.encode_with_seq(big, &mut out);
        assert_eq!(frame_seq(&out), Some(big));
        let mut direct = Vec::new();
        f.finish(big, &mut direct);
        assert_eq!(out, direct);
    }

    /// An idle log's heartbeats are real segments (dense ordinals, a seq in
    /// the header) with no entries, and move its watermark.
    #[tokio::test]
    async fn idle_heartbeats_move_the_watermark() {
        let store = Store::memory(None);
        let mut cfg = LogConfig::new("hb");
        cfg.linger = Duration::from_millis(2);
        cfg.idle_heartbeat = Some(Duration::from_millis(20));
        let (tx, mut rx) = mpsc::unbounded_channel();
        let log = NodeLog::start(store.clone(), cfg, tx, None);
        tokio::time::sleep(Duration::from_millis(120)).await;
        let (free, fenced) = nodelog::first_free(&store, "hb").await.unwrap();
        assert!(free >= 2 && !fenced, "{free}");
        let mut last = 0;
        for ord in 0..free {
            let Head::Segment(h) = nodelog::read_head(&store, "hb", ord).await.unwrap() else { panic!("{ord}") };
            assert!(h.last_seq > last);
            last = h.last_seq;
            assert_eq!(read_segment(&store, "hb", ord).await.unwrap().unwrap().len(), 0);
        }
        let b = rx.recv().await.unwrap();
        assert!(b.events.is_empty());
        assert!(log.last_durable_seq.load(Ordering::Acquire) >= last);
        let d = log.append(Vec::new()).await.unwrap();
        assert!(d.seqs.is_empty());
    }

    fn one_event(upstream_seq: i64) -> Vec<Event> {
        let f = vlpds::events::sync_frame("did:plc:x", "3jzfcijpj2z2a", &[1u8; 32], "2026-10-04T00:00:00Z");
        let mut raw = Vec::new();
        f.finish(upstream_seq, &mut raw);
        vec![Event {
            meta: EventMeta { did: "did:plc:x".into(), host: Host("pds.test".into()), upstream_seq, shard: 0 },
            frame: Box::new(SeqSplice::parse(Bytes::from(raw)).unwrap()),
            delta: None,
        }]
    }

    /// A lapsed lease holds the log within `lapse_grace` (the batch is acked
    /// once the lease is back) and fails it past the grace.
    #[tokio::test]
    async fn a_lapsed_lease_holds_the_log_within_its_grace() {
        let store = Store::memory(None);
        let valid = Arc::new(std::sync::atomic::AtomicBool::new(false));
        let v = valid.clone();
        let mut cfg = LogConfig::new("lapse");
        cfg.linger = Duration::from_millis(2);
        cfg.lease_ok = Some(Arc::new(move || v.load(Ordering::SeqCst)));
        cfg.lapse_grace = Duration::from_millis(300);
        let (tx, _rx) = mpsc::unbounded_channel();
        let log = NodeLog::start(store.clone(), cfg, tx, None);
        let t = log.submit(one_event(1)).await;
        tokio::time::sleep(Duration::from_millis(100)).await;
        assert_eq!(nodelog::first_free(&store, "lapse").await.unwrap().0, 0, "nothing sealed while lapsed");
        valid.store(true, Ordering::SeqCst);
        assert!(t.await.is_ok(), "acked once the lease is back");
        valid.store(false, Ordering::SeqCst);
        let t = log.submit(one_event(2)).await;
        assert_eq!(t.await.unwrap_err(), LogError::LeaseLapsed, "past the grace");
        assert_eq!(log.failed(), Some(LogError::LeaseLapsed));
    }

    #[test]
    fn meta_round_trips() {
        let m = EventMeta { did: "did:plc:x".into(), host: Host("pds.example.com".into()), upstream_seq: 99, shard: 7 };
        assert_eq!(EventMeta::decode(&m.encode(), 7), Some(m));
    }
}
