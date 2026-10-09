//! The flush (docs/quorum.md §2): every interval the leader seals its state
//! at a committed seq F, uploads the log up to F as vlpds segments, and
//! CASes `qlog/manifest` last. The manifest is the commit point: nothing it
//! names is referenced before it's durably written, and a crash anywhere
//! before the CAS leaves the previous manifest in charge.
//!
//! - Segments are vlpds's (`log/qlog/{ordinal:012}.seg`, create-only), so
//!   vlpds's backfill reader serves old cursors from them. Ordinals are
//!   dense; each flush writes the entries in (previous F, F], cut at
//!   `segment_bytes`. A create that finds its ordinal taken (a flush that
//!   died before its CAS, or a deposed leader's) adopts that segment if it
//!   starts where ours would: every flush writes committed entries only,
//!   which are the same on every node.
//! - The state is sealed at exactly F (`state.rs`), and its host cursors
//!   are the ones carried by entries at or below F, so neither gets ahead
//!   of the log the manifest covers.
//! - R = max(previous R, F + H). The leader commits nothing above the last
//!   committed manifest's R (`Node::set_flushed`), so after a lost quorum
//!   seqs can resume at R + 1, above anything any node emitted.
//! - Fencing: a new leader first CASes the manifest to its own epoch. An
//!   older leader's flush then fails its CAS (the ETag moved), and one that
//!   reads the manifest after it sees a newer epoch and stops.

use super::log::Entry;
use super::node::{Node, Quantiles};
use super::state::{self, State, StateRef};
use futures::StreamExt;
use futures::stream::FuturesUnordered;
use object_store::path::Path;
use object_store::{ObjectStoreExt, PutMode, PutOptions, PutPayload, UpdateVersion};
use parking_lot::Mutex;
use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;
use std::sync::Arc;
use std::time::{Duration, Instant};
use vlsync_firehose::log;
use vlsync_firehose::log::Head;
use vlsync_store::segment::{self, SegmentBuilder};
use vlsync_store::slots::ShardId;
use vlsync_store::store::Store;

pub const LOG_ID: &str = super::emit::LOG_ID;

/// Where a flush can be cut short, for crash injection.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize)]
pub enum Step {
    /// A new leader's manifest CAS to its epoch.
    Fenced,
    /// The state checkpoint at F exists.
    Sealed,
    /// One segment PUT is durable (every one of them).
    SegmentPut,
    /// Everything is uploaded, the manifest isn't written.
    BeforeManifest,
    /// The manifest CAS succeeded; the old checkpoint isn't deleted yet.
    AfterManifest,
    /// Bucket recovery: the clone is sealed at R.
    RecoverSealed,
    /// Bucket recovery: salvage is uploaded, the manifest isn't written.
    RecoverBeforeManifest,
    /// Bucket recovery: the manifest is written; the node doesn't lead yet.
    RecoverAfterManifest,
    /// Membership change: learners are recorded and catching up.
    SwitchCatchUp,
    /// Membership change: learners caught up, commits not paused yet.
    SwitchBeforePause,
    /// Membership change: paused and flushed to the commit index, the new
    /// set not CASed yet.
    SwitchFlushed,
    /// Membership change: `qlog/leader` holds the new set at epoch + 1, the
    /// leader hasn't moved to it (or handed off) yet.
    SwitchCas,
}

impl std::str::FromStr for Step {
    type Err = String;
    fn from_str(s: &str) -> Result<Step, String> {
        Ok(match s {
            "fenced" => Step::Fenced,
            "sealed" => Step::Sealed,
            "segment" => Step::SegmentPut,
            "before-manifest" => Step::BeforeManifest,
            "after-manifest" => Step::AfterManifest,
            "recover-sealed" => Step::RecoverSealed,
            "recover-before-manifest" => Step::RecoverBeforeManifest,
            "recover-after-manifest" => Step::RecoverAfterManifest,
            "switch-catch-up" => Step::SwitchCatchUp,
            "switch-before-pause" => Step::SwitchBeforePause,
            "switch-flushed" => Step::SwitchFlushed,
            "switch-cas" => Step::SwitchCas,
            _ => return Err(format!("unknown flush step {s}")),
        })
    }
}

/// Returns true to cut the flush short there, as if the process died.
pub type CrashHook = Arc<dyn Fn(Step) -> bool + Send + Sync>;

#[derive(Clone)]
pub struct Options {
    pub interval: Duration,
    /// H: R = F + H.
    pub headroom: u64,
    /// Raw entry bytes a segment is cut at (before compression).
    pub segment_bytes: usize,
    pub crash: Option<CrashHook>,
}

impl Default for Options {
    fn default() -> Self {
        Options { interval: Duration::from_secs(30), headroom: 8_640_000, segment_bytes: 64 << 20, crash: None }
    }
}

impl std::fmt::Debug for Options {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Options")
            .field("interval", &self.interval)
            .field("headroom", &self.headroom)
            .field("segment_bytes", &self.segment_bytes)
            .finish()
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct SegRef {
    pub ordinal: u64,
    pub first: u64,
    pub last: u64,
    pub bytes: u64,
}

/// `qlog/manifest`.
#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct Manifest {
    /// The epoch of the leader that wrote it (a new leader's fence raises it).
    pub epoch: u64,
    pub leader: String,
    /// F: the log is in the bucket up to here, and the state is sealed here.
    pub flushed: u64,
    /// R: no seq above it was ever emitted while this manifest was current.
    pub reserve: u64,
    /// Segments are dense from ordinal 0 up to here (headers carry seqs).
    pub next_ordinal: u64,
    /// The segments this flush wrote (or adopted): (previous F, F].
    pub segments: Vec<SegRef>,
    pub state: Option<StateRef>,
    /// Each host's highest cursor carried by an entry at or below F.
    pub cursors: BTreeMap<String, u64>,
    pub flushes: u64,
    pub at_ms: i64,
    /// Seqs `(after, upto]` that a bucket recovery skipped: never held by
    /// the log, so never emitted again (any emitted before the recovery
    /// were lost and re-ingested above `upto`). Kept for good: verify and
    /// readers walk the segments across them.
    #[serde(default)]
    pub gaps: Vec<(u64, u64)>,
    /// The last bucket recovery, if any.
    #[serde(default)]
    pub recovery: Option<Recovery>,
}

/// What a bucket recovery decided (docs/quorum.md, "Bucket recovery").
#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct Recovery {
    /// Recoveries so far: host owners compare it to the one they last
    /// rewound for.
    pub generation: u64,
    pub epoch: u64,
    /// The log held everything up to here (the old manifest's F, adopted
    /// orphan segments and salvaged committed entries)...
    pub after: u64,
    /// ...and resumes at `base + 1` (the old manifest's R).
    pub base: u64,
    /// Each host's cursor at `after`: host owners re-read from here, and
    /// what they send again comes back above `base`.
    pub cursors: BTreeMap<String, u64>,
    pub at_ms: i64,
}

impl Manifest {
    /// The state's SlateDB path (under the store's prefix).
    pub fn state_path(&self) -> &str {
        self.state.as_ref().map_or(state::DEFAULT_PATH, |s| s.path.as_str())
    }

    pub fn generation(&self) -> u64 {
        self.recovery.as_ref().map_or(0, |r| r.generation)
    }

    /// Whether a log that ends at `last` may go on at `next` (dense, or
    /// across a recovery's gap).
    pub fn continues(&self, last: u64, next: u64) -> bool {
        // recoveries in a row with nothing committed between leave gaps
        // that follow each other
        let mut end = last;
        while next != end + 1 {
            match self.gaps.iter().find(|&&(a, _)| a == end) {
                Some(&(_, u)) => end = u,
                None => return false,
            }
        }
        true
    }

    /// Where a stream that has emitted up to `at` goes on: past the gap
    /// `at` starts or is inside (and any gaps right after it), else `at`.
    pub fn past_gaps(&self, mut at: u64) -> u64 {
        while let Some(&(_, u)) = self.gaps.iter().find(|&&(a, u)| a <= at && at < u) {
            at = u;
        }
        at
    }
}

fn manifest_path(store: &Store) -> Path {
    Path::from(format!("{}/qlog/manifest", store.prefix))
}

pub async fn read_manifest(store: &Store) -> anyhow::Result<Option<(Manifest, Option<String>)>> {
    match store.raw.get(&manifest_path(store)).await {
        Ok(r) => {
            let etag = r.meta.e_tag.clone();
            Ok(Some((serde_json::from_slice(&r.bytes().await?)?, etag)))
        }
        Err(object_store::Error::NotFound { .. }) => Ok(None),
        Err(e) => Err(e.into()),
    }
}

/// Writes `m` if the manifest is still the version read (`None`: absent).
/// Ok(None): it moved.
async fn cas_manifest(store: &Store, m: &Manifest, read: Option<Option<String>>) -> anyhow::Result<Option<String>> {
    let mode = match read {
        None => PutMode::Create,
        Some(e_tag) => PutMode::Update(UpdateVersion { e_tag, version: None }),
    };
    let body = PutPayload::from(serde_json::to_vec(m)?);
    match store.raw.put_opts(&manifest_path(store), body, PutOptions { mode, ..Default::default() }).await {
        Ok(r) => Ok(Some(r.e_tag.unwrap_or_default())),
        // an If-Match PUT of a missing key is a 404 on S3
        Err(
            object_store::Error::Precondition { .. }
            | object_store::Error::AlreadyExists { .. }
            | object_store::Error::NotFound { .. },
        ) => Ok(None),
        Err(e) => Err(e.into()),
    }
}

/// Object-store requests this process has sent, by op (vlpds's counters,
/// so the store must be `counted`).
pub fn requests_by_op() -> BTreeMap<String, u64> {
    use prometheus::core::Collector;
    let mut out = BTreeMap::new();
    for mf in vlsync_store::metrics::OBJ_REQUESTS.collect() {
        for m in mf.get_metric() {
            let op = m.get_label().iter().find(|l| l.name() == "op").map(|l| l.value().to_string());
            if let Some(op) = op {
                *out.entry(op).or_default() += m.get_counter().get_value() as u64;
            }
        }
    }
    out
}

#[derive(Default)]
struct Stats {
    flushes: u64,
    aborted: u64,
    failed: u64,
    fences: u64,
    adopted: u64,
    segments: u64,
    segment_bytes: u64,
    raw_bytes: u64,
    entries: u64,
    duration_us: Option<hdrhistogram::Histogram<u64>>,
    seal_us: Option<hdrhistogram::Histogram<u64>>,
    requests: BTreeMap<String, u64>,
    applied: u64,
    last: Option<Manifest>,
    recent: std::collections::VecDeque<FlushRecord>,
}

/// One flush this leader made.
#[derive(Clone, Debug, Default, Serialize)]
pub struct FlushRecord {
    /// When its manifest was written.
    pub at_ms: i64,
    pub epoch: u64,
    /// F after it.
    pub flushed: u64,
    pub entries: u64,
    pub segments: u64,
    /// Compressed segment bytes, and the frames' raw bytes.
    pub bytes: u64,
    pub raw_bytes: u64,
    /// Seal to manifest CAS.
    pub took_us: u64,
    /// The applier's pause for the checkpoint.
    pub seal_us: u64,
}

const RECENT_FLUSHES: usize = 32;

/// The flush's counters, shared with `/qlog/status`, and requests for a
/// flush now (a membership change's barrier).
#[derive(Default)]
pub struct Shared {
    s: Mutex<Stats>,
    want: std::sync::atomic::AtomicU64,
    wake: tokio::sync::Notify,
}

#[derive(Clone, Debug, Default, Serialize)]
pub struct Status {
    pub flushes: u64,
    pub aborted: u64,
    pub failed: u64,
    pub fences: u64,
    pub adopted: u64,
    pub segments: u64,
    pub segment_bytes: u64,
    pub raw_bytes: u64,
    pub entries: u64,
    /// Seal to manifest CAS, per flush.
    pub duration_us: Quantiles,
    /// The applier's pause for the checkpoint, per flush.
    pub seal_us: Quantiles,
    /// Object-store requests sent while flushing (summed over flushes; the
    /// state's own background work in those windows counts too).
    pub requests: BTreeMap<String, u64>,
    /// Every object-store request this process has sent, flushing or not
    /// (the state's compactor and GC run between flushes).
    pub requests_total: BTreeMap<String, u64>,
    pub applied: u64,
    pub last_flushed: u64,
    pub last_reserve: u64,
    /// When this leader's last manifest was written (None before its first).
    pub last_at_ms: Option<i64>,
    /// This process's last flushes, oldest first.
    pub recent: Vec<FlushRecord>,
}

fn hist() -> hdrhistogram::Histogram<u64> {
    hdrhistogram::Histogram::new_with_bounds(1, 600_000_000, 3).expect("bounds")
}

impl Shared {
    /// Asks the leader's flush loop to flush up to at least `seq` now,
    /// rather than at its next interval.
    pub fn request(&self, seq: u64) {
        self.want.fetch_max(seq, std::sync::atomic::Ordering::AcqRel);
        self.wake.notify_one();
    }

    fn wanted(&self) -> u64 {
        self.want.load(std::sync::atomic::Ordering::Acquire)
    }

    pub fn status(&self, reset: bool) -> Status {
        let mut s = self.s.lock();
        let q = |h: &Option<hdrhistogram::Histogram<u64>>| h.as_ref().map(Quantiles::of).unwrap_or_default();
        let st = Status {
            flushes: s.flushes,
            aborted: s.aborted,
            failed: s.failed,
            fences: s.fences,
            adopted: s.adopted,
            segments: s.segments,
            segment_bytes: s.segment_bytes,
            raw_bytes: s.raw_bytes,
            entries: s.entries,
            duration_us: q(&s.duration_us),
            seal_us: q(&s.seal_us),
            requests: s.requests.clone(),
            requests_total: requests_by_op(),
            applied: s.applied,
            last_flushed: s.last.as_ref().map_or(0, |m| m.flushed),
            last_reserve: s.last.as_ref().map_or(0, |m| m.reserve),
            last_at_ms: s.last.as_ref().map(|m| m.at_ms).filter(|&t| t > 0),
            recent: s.recent.iter().cloned().collect(),
        };
        if reset {
            s.duration_us = None;
            s.seal_us = None;
        }
        st
    }
}

/// Makes the manifest this epoch's. None: a newer epoch already wrote it.
async fn fence(store: &Store, id: &str, epoch: u64, headroom: u64) -> anyhow::Result<Option<(Manifest, String)>> {
    loop {
        let (next, read) = match read_manifest(store).await? {
            None => (Manifest { epoch, leader: id.to_string(), reserve: headroom, ..Default::default() }, None),
            Some((m, _)) if m.epoch > epoch => return Ok(None),
            Some((m, etag)) => (Manifest { epoch, leader: id.to_string(), ..m }, Some(etag)),
        };
        if let Some(etag) = cas_manifest(store, &next, read).await? {
            return Ok(Some((next, etag)));
        }
    }
}

/// The leader's flush loop for `epoch`: fences the manifest, opens the
/// state, applies committed entries to it as they commit, and flushes every
/// interval. Ends when the node stops leading `epoch`.
pub async fn lead(node: Arc<Node>, epoch: u64, o: Options) {
    let store = node.bucket().flush.clone();
    let state_store = node.bucket().state.clone();
    let mut l = Leader {
        node,
        epoch,
        o,
        store,
        state_store,
        man: Manifest::default(),
        etag: String::new(),
        crashed: false,
        pending: BTreeMap::new(),
        probed: false,
    };
    if let Err(e) = l.run().await {
        tracing::warn!(epoch, "qlog flush: stopped: {e:#}");
        l.node.flush.s.lock().failed += 1;
        // a leader with no flush loop never moves F again (and has no
        // applier for whatever needs its state): the next term starts one
        l.node.step_down_from(epoch, "the flush loop stopped");
    }
    if let Some(h) = &l.node.cfg.hooks.0 {
        h.term_ended(epoch);
    }
}

struct Leader {
    node: Arc<Node>,
    epoch: u64,
    o: Options,
    /// Everything but the state's own SlateDB, which is `state_store`.
    store: Store,
    state_store: Store,
    man: Manifest,
    etag: String,
    /// Cut short by the crash hook: leave everything as a dead process would.
    crashed: bool,
    /// Segments past the manifest's `next_ordinal` that this term knows of.
    pending: BTreeMap<u64, Pending>,
    /// Whether this term looked past the manifest yet ([`Leader::probe`]).
    probed: bool,
}

enum Outcome {
    Done,
    Nothing,
    /// Not this time (an adopted segment reaches past F, the CAS lost to
    /// the same epoch): the next tick tries again.
    Retry,
    /// Fenced by a newer epoch, or cut short by the crash hook.
    Stop,
}

impl Leader {
    fn leading(&self) -> Option<u64> {
        self.node.leading(self.epoch)
    }

    fn crash(&mut self, step: Step) -> bool {
        if let Some(h) = &self.o.crash
            && h(step)
        {
            tracing::warn!(?step, "qlog flush: crash injected");
            self.crashed = true;
        }
        self.crashed
    }

    async fn run(&mut self) -> anyhow::Result<()> {
        loop {
            if self.leading().is_none() {
                return Ok(());
            }
            match fence(&self.store, &self.node.cfg.id, self.epoch, self.o.headroom).await {
                Ok(Some((m, etag))) => {
                    (self.man, self.etag) = (m, etag);
                    break;
                }
                Ok(None) => {
                    self.node.step_down_from(self.epoch, "the manifest is a newer epoch's");
                    return Ok(());
                }
                Err(e) => {
                    tracing::warn!(epoch = self.epoch, "qlog flush: fencing the manifest failed: {e:#}");
                    tokio::time::sleep(Duration::from_millis(200)).await;
                }
            }
        }
        {
            let mut s = self.node.flush.s.lock();
            s.fences += 1;
            s.last = Some(self.man.clone());
        }
        self.node.set_flushed(self.man.flushed, self.man.reserve);
        self.node.note_manifest(&self.man);
        tracing::info!(
            epoch = self.epoch,
            flushed = self.man.flushed,
            reserve = self.man.reserve,
            "qlog flush: manifest fenced"
        );
        if self.crash(Step::Fenced) {
            return Ok(());
        }
        // checkpoints of flushes that never committed (this or an older leader's)
        let keep = self.man.state.as_ref().map(|s| s.checkpoint.clone());
        let rel = self.man.state_path().to_string();
        if let Err(e) = state::delete_checkpoints_except(&self.store, &rel, keep.as_deref()).await {
            tracing::warn!("qlog flush: deleting stale checkpoints failed: {e:#}");
        }
        let mut st = loop {
            if self.leading().is_none() {
                return Ok(());
            }
            match State::open(&self.state_store, &rel).await {
                Ok(s) => break s,
                Err(e) => {
                    tracing::warn!("qlog flush: opening the state failed: {e:#}");
                    tokio::time::sleep(Duration::from_millis(500)).await;
                }
            }
        };
        anyhow::ensure!(
            st.applied() >= self.man.flushed,
            "qlog state is at {}, behind the manifest's F {}",
            st.applied(),
            self.man.flushed
        );
        tracing::info!(epoch = self.epoch, applied = st.applied(), "qlog flush: state open");
        if let Some(h) = self.node.cfg.hooks.0.clone()
            && let Err(e) = h.state_opened(&self.node, self.epoch, st.db(), st.applied()).await
        {
            st.close().await;
            return Err(e.context("the hooks' state"));
        }
        let r = self.drive(&mut st).await;
        if !self.crashed {
            st.close().await;
        }
        r
    }

    async fn drive(&mut self, st: &mut State) -> anyhow::Result<()> {
        let mut commit_rx = self.node.commit_rx();
        let mut next_flush = tokio::time::Instant::now() + self.o.interval;
        loop {
            let Some(commit) = self.leading() else { return Ok(()) };
            // a requested flush covers everything committed when it's taken
            let urgent = self.node.flush.wanted() > self.man.flushed;
            while st.applied() < commit {
                let chunk = self.node.committed_chunk(st.applied() + 1, commit, 4 << 20).await?;
                st.apply(&chunk).await?;
                if let Some(h) = &self.node.cfg.hooks.0 {
                    h.state_applied(self.epoch, st.applied());
                }
                if !urgent && tokio::time::Instant::now() >= next_flush {
                    break;
                }
            }
            self.node.flush.s.lock().applied = st.applied();
            if urgent || tokio::time::Instant::now() >= next_flush {
                next_flush = tokio::time::Instant::now() + self.o.interval;
                let done = match self.flush(st).await {
                    Ok(Outcome::Stop) => return Ok(()),
                    Ok(Outcome::Retry) => {
                        self.node.flush.s.lock().aborted += 1;
                        false
                    }
                    Ok(Outcome::Done) => true,
                    Ok(Outcome::Nothing) => false,
                    // the state was opened over ours (a deposed leader's
                    // open that finished late): no seal here can succeed
                    Err(e) if st.closed() => return Err(e),
                    Err(e) => {
                        tracing::warn!(epoch = self.epoch, "qlog flush failed: {e:#}");
                        self.node.flush.s.lock().failed += 1;
                        false
                    }
                };
                if urgent && !done {
                    tokio::time::sleep(Duration::from_millis(20)).await;
                }
                continue;
            }
            tokio::select! {
                _ = commit_rx.changed() => {}
                _ = self.node.flush.wake.notified() => {}
                _ = tokio::time::sleep_until(next_flush) => {}
                _ = tokio::time::sleep(Duration::from_millis(100)) => {}
            }
        }
    }

    async fn flush(&mut self, st: &State) -> anyhow::Result<Outcome> {
        let f = st.applied();
        if f <= self.man.flushed {
            return Ok(Outcome::Nothing);
        }
        let t0 = Instant::now();
        let req0 = requests_by_op();
        // The applier is this task: nothing above F is written until it
        // returns. The segments don't depend on the seal, so they upload
        // while it runs.
        let (sealed, segs) = tokio::join!(
            async {
                let r = st.seal().await;
                (r, t0.elapsed().as_micros() as u64)
            },
            self.put_segments(f)
        );
        let (sref, seal_us) = match sealed {
            (Ok(s), us) => (s, us),
            (Err(e), _) => {
                if matches!(&segs, Err(x) if x.is::<Crash>()) {
                    return Ok(Outcome::Stop);
                }
                return Err(e);
            }
        };
        if self.crash(Step::Sealed) {
            return Ok(Outcome::Stop);
        }
        let segs = match segs {
            Ok(Some(s)) => s,
            r => {
                let _ = state::delete_checkpoint(&self.store, &sref).await;
                return match r {
                    Ok(_) => Ok(Outcome::Retry),
                    Err(e) if e.is::<Crash>() => Ok(Outcome::Stop),
                    Err(e) => Err(e),
                };
            }
        };
        if self.crash(Step::BeforeManifest) {
            return Ok(Outcome::Stop);
        }
        let (raw, entries): (u64, u64) = segs.1;
        let next = Manifest {
            epoch: self.epoch,
            leader: self.node.cfg.id.clone(),
            flushed: f,
            reserve: self.man.reserve.max(f + self.o.headroom),
            next_ordinal: segs.0.last().map_or(self.man.next_ordinal, |s| s.ordinal + 1),
            segments: segs.0,
            state: Some(sref.clone()),
            cursors: st.cursors().clone(),
            flushes: self.man.flushes + 1,
            at_ms: chrono::Utc::now().timestamp_millis(),
            gaps: self.man.gaps.clone(),
            recovery: self.man.recovery.clone(),
        };
        let etag = match cas_manifest(&self.store, &next, Some(Some(self.etag.clone()))).await {
            Ok(Some(e)) => e,
            Ok(None) => return self.lost_cas(&sref).await,
            Err(e) => {
                // the PUT may have landed with its answer lost: look before
                // deleting a checkpoint it might name
                if let Ok(Some((m, etag))) = read_manifest(&self.store).await
                    && m == next
                {
                    etag.unwrap_or_default()
                } else {
                    let _ = state::delete_checkpoint(&self.store, &sref).await;
                    return Err(e.context("manifest CAS"));
                }
            }
        };
        if self.crash(Step::AfterManifest) {
            return Ok(Outcome::Stop);
        }
        let old = std::mem::replace(&mut self.man, next);
        self.etag = etag;
        let next_ordinal = self.man.next_ordinal;
        self.pending.retain(|&o, _| o >= next_ordinal);
        self.node.set_flushed(self.man.flushed, self.man.reserve);
        if let Some(o) = old.state.filter(|o| o.checkpoint != sref.checkpoint)
            && let Err(e) = state::delete_checkpoint(&self.store, &o).await
        {
            tracing::warn!("qlog flush: deleting the previous checkpoint failed: {e:#}");
        }
        let req1 = requests_by_op();
        let took = t0.elapsed().as_micros() as u64;
        let mut s = self.node.flush.s.lock();
        s.flushes += 1;
        s.segments += self.man.segments.len() as u64;
        s.segment_bytes += self.man.segments.iter().map(|x| x.bytes).sum::<u64>();
        s.raw_bytes += raw;
        s.entries += entries;
        let _ = s.duration_us.get_or_insert_with(hist).record(took.max(1));
        let _ = s.seal_us.get_or_insert_with(hist).record(seal_us.max(1));
        for (op, n) in req1 {
            let d = n - req0.get(&op).copied().unwrap_or(0);
            if d > 0 {
                *s.requests.entry(op).or_default() += d;
            }
        }
        s.last = Some(self.man.clone());
        if s.recent.len() >= RECENT_FLUSHES {
            s.recent.pop_front();
        }
        s.recent.push_back(FlushRecord {
            at_ms: self.man.at_ms,
            epoch: self.epoch,
            flushed: self.man.flushed,
            entries,
            segments: self.man.segments.len() as u64,
            bytes: self.man.segments.iter().map(|x| x.bytes).sum(),
            raw_bytes: raw,
            took_us: took,
            seal_us,
        });
        tracing::info!(
            flushed = f,
            reserve = self.man.reserve,
            segments = self.man.segments.len(),
            seal_ms = seal_us / 1000,
            ms = took / 1000,
            end_ms = chrono::Utc::now().timestamp_millis(),
            "qlog flush: committed"
        );
        Ok(Outcome::Done)
    }

    async fn lost_cas(&mut self, sref: &StateRef) -> anyhow::Result<Outcome> {
        let _ = state::delete_checkpoint(&self.store, sref).await;
        match read_manifest(&self.store).await? {
            Some((m, _)) if m.epoch > self.epoch => {
                tracing::warn!(epoch = self.epoch, newer = m.epoch, "qlog flush: fenced by a newer leader");
                self.node.step_down_from(self.epoch, "the manifest is a newer epoch's");
                Ok(Outcome::Stop)
            }
            Some((m, Some(etag))) => {
                // only this epoch writes it now; take it as it is and go again
                self.man = m;
                self.etag = etag;
                Ok(Outcome::Retry)
            }
            _ => anyhow::bail!("qlog manifest vanished"),
        }
    }

    /// Uploads (man.flushed, f] as segments from `man.next_ordinal`, up to
    /// [`PUT_WINDOW`] at once. None: an existing segment there reaches past
    /// `f` (try again later).
    #[allow(clippy::type_complexity)]
    async fn put_segments(&mut self, f: u64) -> anyhow::Result<Option<(Vec<SegRef>, (u64, u64))>> {
        if !self.probed {
            self.probe().await?;
            self.probed = true;
        }
        let start = self.man.next_ordinal;
        let mut done: BTreeMap<u64, SegRef> = BTreeMap::new();
        let (mut raw, mut n) = (0u64, 0u64);
        let mut ord = start;
        let mut from = self.man.flushed + 1;
        let mut puts = FuturesUnordered::new();
        let r = 'flush: loop {
            // every ordinal below `open` is durable
            let open = (start..ord).find(|o| !done.contains_key(o)).unwrap_or(ord);
            if from > f || ord >= open + PUT_WINDOW {
                let Some(p) = puts.next().await else { break Ok(true) };
                match self.landed(p, f, &mut done, &mut raw, &mut n).await {
                    Ok(true) => continue,
                    other => break other,
                }
            }
            match self.pending.get(&ord).copied() {
                Some(p) if p.durable && p.first == from => {
                    if p.last > f {
                        break Ok(false);
                    }
                    self.adopt(ord, p, &mut done);
                    (ord, from) = (ord + 1, p.last + 1);
                    continue;
                }
                Some(_) => match self.existing(ord, from, f).await {
                    Ok(Existing::Adopt(p)) => {
                        self.adopt(ord, p, &mut done);
                        (ord, from) = (ord + 1, p.last + 1);
                        continue;
                    }
                    Ok(Existing::PastF) => break Ok(false),
                    Ok(Existing::Free) => {}
                    Err(e) => break Err(e),
                },
                None => {}
            }
            let mut b = SegmentBuilder::for_log(LOG_ID);
            let mut last = from - 1;
            'fill: while last < f {
                let chunk = match self.node.committed_chunk(last + 1, f, 4 << 20).await {
                    Ok(c) => c,
                    Err(e) => break 'flush Err(e),
                };
                for e in chunk {
                    push(&mut b, &e);
                    last = e.seq;
                    if b.len() >= self.o.segment_bytes {
                        break 'fill;
                    }
                }
            }
            // one segment is compressed at a time: the PUTs in flight hold
            // only their compressed bodies
            let body_len = b.len() as u64;
            let obj = match seal(b, ord, open).await {
                Ok(o) => o,
                Err(e) => break Err(e),
            };
            self.pending.insert(ord, Pending { first: from, last, bytes: 0, durable: false, ours: true });
            puts.push(put_segment(self.store.clone(), obj, body_len, ord, from, last));
            (ord, from) = (ord + 1, last + 1);
        };
        // every PUT this flush started has its answer before it returns, so
        // `pending` knows what the next attempt will find
        if !matches!(r, Ok(true)) {
            while let Some(p) = puts.next().await {
                if let Ok(Put::Created(s)) = &p.result {
                    self.pending.insert(
                        s.ordinal,
                        Pending { first: s.first, last: s.last, bytes: s.bytes, durable: true, ours: true },
                    );
                }
            }
        }
        match r {
            Ok(true) => Ok(Some((done.into_values().collect(), (raw, n)))),
            Ok(false) => Ok(None),
            Err(e) => Err(e),
        }
    }

    /// One segment PUT's answer. False: the segment there reaches past `f`.
    async fn landed(
        &mut self,
        p: PutDone,
        f: u64,
        done: &mut BTreeMap<u64, SegRef>,
        raw: &mut u64,
        n: &mut u64,
    ) -> anyhow::Result<bool> {
        match p.result? {
            Put::Created(s) => {
                self.pending.insert(
                    s.ordinal,
                    Pending { first: s.first, last: s.last, bytes: s.bytes, durable: true, ours: true },
                );
                *raw += p.body_len;
                *n += s.last - s.first + 1;
                done.insert(s.ordinal, s);
                if self.crash(Step::SegmentPut) {
                    return Err(Crash.into());
                }
                Ok(true)
            }
            Put::Exists => match self.existing(p.ord, p.first, f).await? {
                Existing::Adopt(x) if x.last == p.last => {
                    self.adopt(p.ord, x, done);
                    Ok(true)
                }
                Existing::Adopt(x) => {
                    // the ordinals after it were cut where ours ended: the
                    // next attempt adopts this one and rewrites those
                    self.pending.insert(p.ord, x);
                    anyhow::bail!("qlog flush: segment {} there ends at {}, not {}", p.ord, x.last, p.last)
                }
                Existing::PastF => Ok(false),
                Existing::Free => anyhow::bail!("qlog flush: segment {} was taken, then gone", p.ord),
            },
        }
    }

    fn adopt(&mut self, ord: u64, p: Pending, done: &mut BTreeMap<u64, SegRef>) {
        if !p.ours {
            tracing::info!(
                ord,
                first = p.first,
                last = p.last,
                "qlog flush: adopted a segment an unfinished flush wrote"
            );
            self.node.flush.s.lock().adopted += 1;
        }
        self.pending.insert(ord, Pending { durable: true, ..p });
        done.insert(ord, SegRef { ordinal: ord, first: p.first, last: p.last, bytes: p.bytes });
    }

    /// Reads what's at `ord`, where this flush's segment would start at
    /// `from`, and clears it if it's in the way.
    async fn existing(&mut self, ord: u64, from: u64, f: u64) -> anyhow::Result<Existing> {
        let ours = self.pending.remove(&ord).is_some_and(|p| p.ours);
        let path = log::segment_path(&self.store, LOG_ID, ord);
        match log::read_head(&self.store, LOG_ID, ord).await? {
            Head::Missing => Ok(Existing::Free),
            // A deposed leader's flush, still running when a bucket
            // recovery moved F past it: no manifest names an ordinal at or
            // past next_ordinal, and this one doesn't continue the log, so
            // it's nobody's.
            Head::Segment(h) if (h.first_seq as u64) <= self.man.flushed => {
                tracing::warn!(
                    ord,
                    first = h.first_seq,
                    f = self.man.flushed,
                    "qlog flush: deleting a stale segment in the way"
                );
                self.store.raw.delete(&path).await?;
                Ok(Existing::Free)
            }
            Head::Segment(h) if h.first_seq as u64 == from => {
                let p = Pending { first: from, last: h.last_seq as u64, bytes: 0, durable: true, ours: false };
                if p.last > f {
                    tracing::info!(ord, last = p.last, f, "qlog flush: an existing segment reaches past F");
                    self.pending.insert(ord, p);
                    return Ok(Existing::PastF);
                }
                Ok(Existing::Adopt(p))
            }
            // this leader's, cut after a segment that turned out to end
            // elsewhere: unnamed by any manifest
            Head::Segment(h) if ours => {
                tracing::info!(
                    ord,
                    first = h.first_seq,
                    from,
                    "qlog flush: rewriting a segment cut at the wrong place"
                );
                self.store.raw.delete(&path).await?;
                Ok(Existing::Free)
            }
            h => anyhow::bail!(
                "qlog flush: ordinal {ord} is taken by {} where seq {from} should start",
                match h {
                    Head::Segment(h) => format!("a segment from {}", h.first_seq),
                    Head::Fence => "a fence".into(),
                    Head::Missing => "nothing (deleted?)".into(),
                }
            ),
        }
    }

    /// This term's first look past the manifest: what a flush that died
    /// before its CAS left there. Its PUTs were windowed, so nothing lies
    /// past [`PUT_WINDOW`] missing ordinals in a row.
    async fn probe(&mut self) -> anyhow::Result<()> {
        let mut ord = self.man.next_ordinal;
        let mut missing = 0;
        while missing < PUT_WINDOW {
            match log::read_head(&self.store, LOG_ID, ord).await? {
                Head::Missing => missing += 1,
                Head::Segment(h) => {
                    missing = 0;
                    let (first, last) = (h.first_seq as u64, h.last_seq as u64);
                    self.pending.insert(ord, Pending { first, last, bytes: 0, durable: true, ours: false });
                }
                Head::Fence => {
                    missing = 0;
                    self.pending.insert(ord, Pending { first: 0, last: 0, bytes: 0, durable: false, ours: false });
                }
            }
            ord += 1;
        }
        Ok(())
    }
}

/// Segment PUTs a flush has in flight, as a window over ordinals: a PUT
/// starts only once every ordinal this far below it is durable, so a crash
/// leaves holes only within it (the segments' `prefix_end`).
const PUT_WINDOW: u64 = 4;

/// Tries per segment PUT. A create isn't idempotent, so object_store
/// doesn't retry one that timed out, and one slow PUT shouldn't cost the
/// whole flush.
const PUT_TRIES: u32 = 3;

/// A segment past the manifest this leader knows of: written by one of its
/// flushes that didn't reach the CAS (`ours`), or found by [`Leader::probe`].
#[derive(Clone, Copy, Debug)]
struct Pending {
    first: u64,
    last: u64,
    bytes: u64,
    /// False: a PUT whose answer was lost, so it may or may not exist.
    durable: bool,
    ours: bool,
}

enum Existing {
    Free,
    Adopt(Pending),
    PastF,
}

enum Put {
    Created(SegRef),
    Exists,
}

struct PutDone {
    ord: u64,
    first: u64,
    last: u64,
    body_len: u64,
    result: anyhow::Result<Put>,
}

/// Seals and compresses segment `ord`, off the runtime.
async fn seal(b: SegmentBuilder, ord: u64, prefix_end: u64) -> anyhow::Result<bytes::Bytes> {
    tokio::task::spawn_blocking(move || -> anyhow::Result<bytes::Bytes> {
        let obj = b.seal(LOG_ID, ord, prefix_end);
        let mut obj = segment::compress(&obj, segment::compression_level())?.unwrap_or(obj);
        // compress leaves a buffer as big as its worst case
        obj.shrink_to_fit();
        release_freed();
        Ok(obj.into())
    })
    .await?
}

/// Hands the pages of freed buffers back to the OS now. The node keeps
/// freed large pages for reuse until they decay (main.rs's malloc conf),
/// so a backlog flush's 64 MiB buffers, a few freed a second, held ~600 MB
/// of RSS past what was allocated for ~10 s.
pub(crate) fn release_freed() {
    // MALLCTL_ARENAS_ALL
    let name = b"arena.4096.purge\0";
    // SAFETY: a NUL-terminated name, and purge reads and writes nothing
    unsafe {
        tikv_jemalloc_sys::mallctl(
            name.as_ptr().cast(),
            std::ptr::null_mut(),
            std::ptr::null_mut(),
            std::ptr::null_mut(),
            0,
        );
    }
}

/// Creates segment `ord`.
fn put_segment(
    store: Store,
    obj: bytes::Bytes,
    body_len: u64,
    ord: u64,
    first: u64,
    last: u64,
) -> futures::future::BoxFuture<'static, PutDone> {
    use futures::FutureExt;
    async move {
        let result = async {
            let bytes = obj.len() as u64;
            let path = log::segment_path(&store, LOG_ID, ord);
            let put = || {
                let opts = PutOptions { mode: PutMode::Create, ..Default::default() };
                store.raw.put_opts(&path, PutPayload::from(obj.clone()), opts)
            };
            let hedge_after = HEDGE_AFTER + Duration::from_secs_f64(bytes as f64 / HEDGE_RATE);
            let mut tries = 0;
            loop {
                tries += 1;
                let one = put();
                tokio::pin!(one);
                let r = match tokio::time::timeout(hedge_after, &mut one).await {
                    Ok(r) => r,
                    Err(_) => {
                        // A PUT this slow is likelier stuck than slow: a
                        // second one races it, and whichever lands first
                        // wins (the other finds the segment there).
                        tracing::info!(ord, bytes, "qlog flush: segment PUT is slow, sending it again alongside");
                        let two = put();
                        tokio::pin!(two);
                        tokio::select! {
                            r = &mut one => if done_put(&r) { r } else { two.await },
                            r = &mut two => if done_put(&r) { r } else { one.await },
                        }
                    }
                };
                match r {
                    Ok(_) => return Ok(Put::Created(SegRef { ordinal: ord, first, last, bytes })),
                    Err(object_store::Error::AlreadyExists { .. } | object_store::Error::Precondition { .. }) => {
                        return Ok(Put::Exists);
                    }
                    Err(e) if tries < PUT_TRIES => {
                        tracing::warn!(ord, tries, "qlog flush: segment PUT failed, trying again: {e}");
                    }
                    Err(e) => return Err(anyhow::Error::from(e).context(format!("segment {ord}"))),
                }
            }
        }
        .await;
        PutDone { ord, first, last, body_len, result }
    }
    .boxed()
}

/// Whether a segment PUT has its answer: created, or found there.
fn done_put(r: &object_store::Result<object_store::PutResult>) -> bool {
    matches!(r, Ok(_) | Err(object_store::Error::AlreadyExists { .. } | object_store::Error::Precondition { .. }))
}

/// A segment PUT gets a second copy racing it after this, plus its bytes
/// at [`HEDGE_RATE`]: well past a healthy PUT, well short of the client's
/// 30 s timeout.
const HEDGE_AFTER: Duration = Duration::from_secs(5);
const HEDGE_RATE: f64 = 4e6;

/// A segment read back from the bucket: (ordinal, its entries).
pub(crate) type SegCache = Option<(u64, Arc<Vec<Entry>>)>;

async fn load_segment(store: &Store, ord: u64, cache: &mut SegCache) -> anyhow::Result<Option<Arc<Vec<Entry>>>> {
    if let Some((o, es)) = cache
        && *o == ord
    {
        return Ok(Some(es.clone()));
    }
    let Some((_, ents)) = read_segment(store, ord).await? else {
        return Ok(None);
    };
    let es: Arc<Vec<Entry>> = Arc::new(ents);
    *cache = Some((ord, es.clone()));
    Ok(Some(es))
}

/// Flushed entries from `from` to at most `upto`, about `max_bytes` (at
/// least one), with the epoch of `from - 1`, read from the bucket segments:
/// what a follower further behind than the leader's disk catches up from,
/// instead of being reset past it. None: the bucket doesn't hold `from`.
pub(crate) async fn read_bucket(
    store: &Store,
    cache: &mut SegCache,
    from: u64,
    upto: u64,
    max_bytes: usize,
) -> anyhow::Result<Option<(u64, Vec<Entry>)>> {
    let Some((ord, seg, i)) = locate(store, cache, from, upto).await? else { return Ok(None) };
    let prev_epoch = if i > 0 {
        seg[i - 1].epoch
    } else if from == 1 {
        0
    } else {
        let mut c = None;
        match load_segment(store, ord - 1, &mut c).await? {
            Some(p) if p.last().is_some_and(|e| e.seq == from - 1) => p.last().expect("checked").epoch,
            _ => return Ok(None),
        }
    };
    Ok(Some((prev_epoch, take(&seg[i..], upto, max_bytes))))
}

/// As `read_bucket`, without the epoch of `from - 1`, so `from` may be the
/// first seq past a recovery's gap (what an emitter hands its consumers).
pub(crate) async fn read_bucket_entries(
    store: &Store,
    cache: &mut SegCache,
    from: u64,
    upto: u64,
    max_bytes: usize,
) -> anyhow::Result<Option<Vec<Entry>>> {
    let Some((_, seg, i)) = locate(store, cache, from, upto).await? else { return Ok(None) };
    Ok(Some(take(&seg[i..], upto, max_bytes)))
}

/// The segment holding `from` and its index there.
async fn locate(
    store: &Store,
    cache: &mut SegCache,
    from: u64,
    upto: u64,
) -> anyhow::Result<Option<(u64, Arc<Vec<Entry>>, usize)>> {
    if from == 0 || from > upto {
        return Ok(None);
    }
    let ord = vlsync_firehose::backfill::seek(store, LOG_ID, from as i64 - 1).await?;
    let Some(seg) = load_segment(store, ord, cache).await? else { return Ok(None) };
    let Some(first) = seg.first().map(|e| e.seq) else { return Ok(None) };
    if first > from {
        return Ok(None);
    }
    let i = (from - first) as usize;
    // segments are dense, but one that isn't (or `from` in a gap past its
    // end) must not hand out other seqs as `from`'s
    if seg.get(i).is_none_or(|e| e.seq != from) {
        return Ok(None);
    }
    Ok(Some((ord, seg, i)))
}

fn take(es: &[Entry], upto: u64, max_bytes: usize) -> Vec<Entry> {
    let mut n = 0;
    let mut out = Vec::new();
    for e in es.iter().take_while(|e| e.seq <= upto) {
        if !out.is_empty() && n + e.data.len() > max_bytes {
            break;
        }
        n += e.data.len();
        out.push(e.clone());
    }
    out
}

#[derive(Debug)]
struct Crash;

/// Whether `e` is an injected crash (tests: the step was cut short).
pub fn is_crash(e: &anyhow::Error) -> bool {
    e.is::<Crash>()
}

impl std::fmt::Display for Crash {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("crash injected")
    }
}

impl std::error::Error for Crash {}

/// An entry's cursors and meta ride its segment entry as one mutation, so
/// what's flushed replays the state exactly (`verify`, recovery's orphans,
/// a follower caught up from the bucket).
const SIDE_KEY: &[u8] = b"qside";

fn push(b: &mut SegmentBuilder, e: &Entry) {
    let side = super::log::encode_side(&e.cursors, &e.meta);
    let muts = if side.is_empty() {
        Vec::new()
    } else {
        vec![segment::Mutation { key: bytes::Bytes::from_static(SIDE_KEY), val: Some(side) }]
    };
    b.push(e.seq as i64, ShardId(0), e.epoch, |out| out.extend_from_slice(&e.data), &muts);
}

fn entry_of(e: segment::SegEntry) -> Entry {
    let mut out = Entry::new(e.epoch, e.seq as u64, e.frame);
    if let Some(side) = e.muts.into_iter().find(|m| m.key.as_ref() == SIDE_KEY).and_then(|m| m.val) {
        (out.cursors, out.meta) = super::log::decode_side(side);
    }
    out
}

/// Segment `ord`'s entries with their cursors and meta, if it exists.
pub async fn read_entries(store: &Store, ord: u64) -> anyhow::Result<Option<Vec<Entry>>> {
    Ok(read_segment(store, ord).await?.map(|(_, es)| es))
}

/// A segment with its entries' cursors and meta (vlpds's `read_object`
/// skips mutations).
async fn read_segment(store: &Store, ord: u64) -> anyhow::Result<Option<(segment::SegHeader, Vec<Entry>)>> {
    let data = match store.raw.get(&log::segment_path(store, LOG_ID, ord)).await {
        Ok(r) => r.bytes().await?,
        Err(object_store::Error::NotFound { .. }) => return Ok(None),
        Err(e) => return Err(e.into()),
    };
    match segment::parse(data, true, None)? {
        segment::LogObject::Segment(h, ents) => {
            log::check_header(&h, LOG_ID, ord)?;
            Ok(Some((h, ents.into_iter().map(entry_of).collect())))
        }
        segment::LogObject::Fence { .. } => anyhow::bail!("qlog: ordinal {ord} is a fence"),
    }
}

/// What `verify` found.
#[derive(Debug, Default, Serialize)]
pub struct Verified {
    pub ok: bool,
    pub messages: Vec<String>,
    pub epoch: u64,
    pub flushed: u64,
    pub reserve: u64,
    pub segments: u64,
    /// Segments past the manifest (written by a flush that didn't commit).
    pub orphans: u64,
    /// Segments past the manifest that a deposed leader wrote below F (the
    /// next flush deletes them).
    pub stale: u64,
    /// Recovery gaps the segments were walked across.
    pub gaps: u64,
    /// Retention's floor (`retain/qlog`): the walk starts at the oldest
    /// segment left, which must continue the log from here.
    pub pruned: u64,
    pub entries: u64,
    /// Entries with the relay's meta (state writes the leader decided).
    pub relay_entries: u64,
    pub dids: u64,
    pub hosts: u64,
}

/// A host and an event's number within it, from a load generator's DID
/// (`did:q:{host}:{n}`).
pub fn host_event(did: &str) -> Option<(&str, u64)> {
    let (h, n) = did.strip_prefix("did:q:")?.rsplit_once(':')?;
    Some((h, n.parse().ok()?))
}

/// Checks the manifest describes one consistent point: the segments hold
/// the log densely up to F (and nothing it names past F), the state
/// checkpoint equals replaying those entries to F, its cursors are the
/// manifest's, and no cursor counts an event that isn't in the log at or
/// below F. Segments past the manifest must still continue the log.
///
/// After retention deleted segments (`pruned_seq` P > 0), the walk starts
/// at the oldest segment left and must continue from P; a DID the state
/// holds but the remaining log doesn't must be at or below P, and a host's
/// cursor is checked against its events' run from the first one left.
pub async fn verify(store: &Store) -> anyhow::Result<Verified> {
    let mut v = Verified::default();
    let Some((m, _)) = read_manifest(store).await? else {
        v.ok = true;
        v.messages.push("no manifest yet".into());
        return Ok(v);
    };
    (v.epoch, v.flushed, v.reserve) = (m.epoch, m.flushed, m.reserve);
    let bad = |v: &mut Verified, s: String| {
        if v.messages.len() < 50 {
            v.messages.push(s);
        }
    };
    if m.reserve < m.flushed {
        bad(&mut v, format!("R {} is below F {}", m.reserve, m.flushed));
    }
    // first, while the manifest still names it (the next flush deletes it;
    // SlateDB's GC keeps the files a while, so a reader opened now is fine)
    let state = match &m.state {
        Some(r) if m.flushed > 0 => Some((r.clone(), state::read_checkpoint(store, r).await?)),
        _ => None,
    };
    let mut rows: BTreeMap<bytes::Bytes, bytes::Bytes> = BTreeMap::new();
    let mut hosts: BTreeMap<String, Vec<u64>> = BTreeMap::new();
    v.pruned = super::retain::pruned_seq(store).await?;
    let mut last = v.pruned;
    let mut ord = if v.pruned > 0 { oldest_segment(store).await?.unwrap_or(m.next_ordinal) } else { 0 };
    v.gaps = m.gaps.len() as u64;
    loop {
        let (h, ents) = match read_segment(store, ord).await {
            Ok(Some(x)) => x,
            Ok(None) => break,
            Err(e) => {
                bad(&mut v, format!("ordinal {ord}: {e:#}"));
                break;
            }
        };
        if ord >= m.next_ordinal && (h.first_seq as u64) <= m.flushed {
            v.stale += 1;
            ord += 1;
            continue;
        }
        // nothing above R is appended while this manifest is current: a
        // segment past it is a later recovery's, whose manifest landed
        // after this one was read
        if ord >= m.next_ordinal && (h.first_seq as u64) > m.reserve {
            break;
        }
        if !m.continues(last, h.first_seq as u64) {
            bad(&mut v, format!("segment {ord} starts at {}, after {last}", h.first_seq));
        }
        for e in ents {
            if !m.continues(last, e.seq) {
                bad(&mut v, format!("segment {ord}: seq {} after {last}", e.seq));
            }
            last = e.seq;
            if ord >= m.next_ordinal || last > m.flushed {
                continue;
            }
            v.entries += 1;
            if !e.meta.is_empty() {
                v.relay_entries += 1;
            }
            for (k, val) in state::effects(&e).0 {
                if let Some((h, n)) = k.strip_prefix(b"d/").and_then(|d| host_event(std::str::from_utf8(d).ok()?)) {
                    hosts.entry(h.to_string()).or_default().push(n);
                }
                rows.insert(k, val);
            }
        }
        if ord < m.next_ordinal {
            v.segments += 1;
            // after a recovery that salvaged nothing new, the last segment
            // ends where the gap up to F starts
            if ord + 1 == m.next_ordinal && !m.continues(last, m.flushed + 1) {
                bad(&mut v, format!("the manifest's last segment ends at {last}, not F {}", m.flushed));
            }
        } else {
            v.orphans += 1;
        }
        ord += 1;
    }
    if ord < m.next_ordinal {
        bad(&mut v, format!("segments end at ordinal {ord}, the manifest names up to {}", m.next_ordinal));
    }
    if m.flushed > 0 {
        match state {
            None => bad(&mut v, "no state checkpoint".into()),
            Some((r, (kv, l0))) => {
                if r.seq != m.flushed {
                    bad(&mut v, format!("state checkpoint at {}, F is {}", r.seq, m.flushed));
                }
                if l0 != m.flushed {
                    bad(&mut v, format!("checkpoint manifest's last_l0_seq {l0} isn't F {}", m.flushed));
                }
                let applied =
                    kv.get(state::applied_key()).map(|b| u64::from_be_bytes(b[..8].try_into().unwrap_or([0; 8])));
                if applied != Some(m.flushed) {
                    bad(&mut v, format!("state's _applied is {applied:?}, F is {}", m.flushed));
                }
                let mut state_rows = 0u64;
                let mut pruned_rows = 0u64;
                let mut state_cursors = BTreeMap::new();
                for (k, val) in &kv {
                    if k.as_ref() == state::applied_key() {
                        continue;
                    }
                    if let Some(h) = k.strip_prefix(b"c/") {
                        state_cursors.insert(
                            String::from_utf8_lossy(h).into_owned(),
                            u64::from_be_bytes(val[..8].try_into().unwrap_or([0; 8])),
                        );
                        continue;
                    }
                    state_rows += 1;
                    let name = || String::from_utf8_lossy(k).into_owned();
                    match rows.get(k) {
                        Some(x) if x == val => {}
                        Some(_) => bad(&mut v, format!("state holds {} with another value than the log", name())),
                        // a test frame's row carries its seq; the relay's
                        // don't, so past retention a row the remaining log
                        // doesn't hold is taken as pruned
                        None if k.starts_with(b"d/") => {
                            if u64::from_be_bytes(val[..8].try_into().unwrap_or([0; 8])) <= v.pruned {
                                pruned_rows += 1;
                            } else {
                                bad(&mut v, format!("state holds {}, which isn't in the log up to F", name()));
                            }
                        }
                        None if v.pruned > 0 => pruned_rows += 1,
                        None => bad(&mut v, format!("state holds {}, which isn't in the log up to F", name())),
                    }
                }
                if state_rows != rows.len() as u64 + pruned_rows {
                    bad(&mut v, format!("state holds {state_rows} rows, the log up to F {}", rows.len()));
                }
                if state_cursors != m.cursors {
                    bad(&mut v, "the state's cursors aren't the manifest's".into());
                }
            }
        }
    }
    for (h, c) in &m.cursors {
        let mut ns = hosts.remove(h).unwrap_or_default();
        ns.sort_unstable();
        ns.dedup();
        // a relay's host sends events the log never holds (duplicates,
        // rejections), so only a test load's hosts are numbered densely
        if ns.is_empty() && v.relay_entries > 0 {
            v.hosts += 1;
            continue;
        }
        let from = match ns.first() {
            Some(&f) if v.pruned > 0 => f,
            // every event of it pruned: nothing left to hold the cursor to
            None if v.pruned > 0 => {
                v.hosts += 1;
                continue;
            }
            _ => 1,
        };
        let contiguous = from - 1 + ns.iter().enumerate().take_while(|(i, n)| **n == from + *i as u64).count() as u64;
        if *c > contiguous {
            bad(&mut v, format!("host {h}'s cursor {c} is ahead of the log at F (events 1..={contiguous} there)"));
        }
        v.hosts += 1;
    }
    v.dids = rows.len() as u64;
    v.ok = v.messages.is_empty();
    Ok(v)
}

async fn oldest_segment(store: &Store) -> anyhow::Result<Option<u64>> {
    let p = Path::from(format!("{}/log/{LOG_ID}", store.prefix));
    let mut l = store.raw.list(Some(&p));
    let mut min = None;
    while let Some(o) = l.next().await {
        let o = o?;
        if let Some(ord) =
            o.location.filename().and_then(|f| f.strip_suffix(".seg")).and_then(|f| f.parse::<u64>().ok())
        {
            min = Some(min.map_or(ord, |m: u64| m.min(ord)));
        }
    }
    Ok(min)
}

/// Where the log stands in the bucket when a quorum is lost.
#[derive(Debug)]
pub struct RecoveryPoint {
    pub manifest: Manifest,
    pub etag: String,
    /// Segments past the manifest that continue the log: a flush that died
    /// before its CAS. Every flush writes committed entries only, so they
    /// are adopted and move F, their entries' cursors and meta with them.
    pub orphans: Vec<(SegRef, Vec<Entry>)>,
    /// F': the manifest's F, or the last orphan's end.
    pub flushed: u64,
}

/// Reads the manifest and adopts the orphan segments past it. A segment
/// past it that starts at or below F is a deposed leader's: deleted, so it
/// can't take an ordinal the recovery writes.
pub async fn recovery_point(store: &Store) -> anyhow::Result<Option<RecoveryPoint>> {
    let Some((m, etag)) = read_manifest(store).await? else { return Ok(None) };
    let mut orphans = Vec::new();
    let mut last = m.flushed;
    let mut ord = m.next_ordinal;
    loop {
        let Some((h, es)) = read_segment(store, ord).await? else {
            // a flush's PUTs past this hole: they can't be adopted, and
            // the recovery writes these ordinals
            for o in ord + 1..ord + PUT_WINDOW {
                if let Head::Segment(h) = log::read_head(store, LOG_ID, o).await? {
                    tracing::warn!(ord = o, first = h.first_seq, "qlog recovery: deleting a segment past a hole");
                    store.raw.delete(&log::segment_path(store, LOG_ID, o)).await?;
                }
            }
            break;
        };
        let first = h.first_seq as u64;
        if first <= m.flushed {
            tracing::warn!(ord, first, f = m.flushed, "qlog recovery: deleting a stale segment past the manifest");
            store.raw.delete(&log::segment_path(store, LOG_ID, ord)).await?;
            ord += 1;
            continue;
        }
        if first != last + 1 {
            tracing::warn!(ord, first, last, "qlog recovery: a segment past the manifest doesn't continue the log");
            break;
        }
        let Some(end) = es.last().map(|e| e.seq) else { break };
        // committed entries never pass R, so neither can a flush of them
        anyhow::ensure!(end <= m.reserve, "qlog recovery: segment {ord} ends at {end}, past R {}", m.reserve);
        orphans.push((SegRef { ordinal: ord, first, last: end, bytes: 0 }, es));
        last = end;
        ord += 1;
    }
    Ok(Some(RecoveryPoint { manifest: m, etag: etag.unwrap_or_default(), orphans, flushed: last }))
}

/// How long each part of a bucket recovery took.
#[derive(Clone, Debug, Default, Serialize)]
pub struct RecoveryStats {
    pub generation: u64,
    pub epoch: u64,
    /// The old manifest's F, after orphans, after salvage (S), and R.
    pub manifest_flushed: u64,
    pub orphans_to: u64,
    pub after: u64,
    pub base: u64,
    pub orphan_segments: u64,
    pub salvaged: u64,
    /// Reading the manifest and the orphans (the node measures it).
    pub read_ms: u64,
    pub clone_ms: u64,
    /// Applying orphans and salvage to the clone, the jump and the seal.
    pub apply_seal_ms: u64,
    pub segments_ms: u64,
    pub manifest_ms: u64,
    pub total_ms: u64,
}

/// The bucket side of a recovery by `epoch`, the leader of a quorum with
/// no quorum of intact logs (docs/quorum.md, "Bucket recovery"): the
/// state cloned from the manifest's checkpoint, the orphans and `salvage`
/// (committed entries from F' + 1 on, densely, that a reachable node held,
/// in chunks as they're fetched) applied to it and uploaded as segments
/// as they arrive, the applied point jumped to R and sealed there; then
/// the manifest CASed with F = R, the gap `(S, R]` and the recovery's
/// cursors. Seqs resume at R + 1. A crash or a lost CAS anywhere before
/// the manifest leaves the old one in charge (the clone and segments are
/// left for the next attempt, which adopts the segments as orphans, or for
/// the retention report).
pub async fn recover(
    store: &Store,
    id: &str,
    epoch: u64,
    o: &Options,
    p: RecoveryPoint,
    mut salvage: tokio::sync::mpsc::Receiver<Vec<Entry>>,
    stats: &mut RecoveryStats,
) -> anyhow::Result<Manifest> {
    let t0 = Instant::now();
    let crash = |step| o.crash.as_ref().is_some_and(|h| h(step));
    let m = &p.manifest;
    anyhow::ensure!(m.epoch < epoch, "qlog recovery: the manifest is epoch {}'s, not older than {epoch}", m.epoch);
    let r = m.reserve;
    (stats.manifest_flushed, stats.orphans_to, stats.base) = (m.flushed, p.flushed, r);
    (stats.orphan_segments, stats.epoch) = (p.orphans.len() as u64, epoch);
    let rel = state::recovery_path(epoch);
    let mut st = State::recover(store, m.state.as_ref(), &rel).await?;
    stats.clone_ms = t0.elapsed().as_millis() as u64;
    let t1 = Instant::now();
    for (_, es) in &p.orphans {
        for chunk in es.chunks(4096) {
            st.apply(chunk).await?;
        }
    }
    // Salvage is held one segment at a time: at 100x a whole interval of
    // it is gigabytes.
    let mut segments: Vec<SegRef> = p.orphans.iter().map(|(r, _)| r.clone()).collect();
    let mut ord = m.next_ordinal + p.orphans.len() as u64;
    let mut s = p.flushed;
    let mut seg: Option<(SegmentBuilder, u64)> = None;
    let mut put_us = 0u64;
    while let Some(chunk) = salvage.recv().await {
        for e in &chunk {
            anyhow::ensure!(e.seq == s + 1, "qlog recovery: salvage isn't dense at {} after {s}", e.seq);
            s = e.seq;
        }
        anyhow::ensure!(s <= r, "qlog recovery: salvage reaches {s}, past R {r}");
        st.apply(&chunk).await?;
        stats.salvaged += chunk.len() as u64;
        for e in &chunk {
            let (b, _) = seg.get_or_insert_with(|| (SegmentBuilder::for_log(LOG_ID), e.seq));
            push(b, e);
            if b.len() >= o.segment_bytes {
                let (b, first) = seg.take().expect("just filled");
                let t = Instant::now();
                segments.push(put_recovery_segment(store, b, ord, first, e.seq).await?);
                put_us += t.elapsed().as_micros() as u64;
                ord += 1;
            }
        }
    }
    if let Some((b, first)) = seg.take() {
        let t = Instant::now();
        segments.push(put_recovery_segment(store, b, ord, first, s).await?);
        put_us += t.elapsed().as_micros() as u64;
        ord += 1;
    }
    stats.after = s;
    let cursors = st.cursors().clone();
    st.jump(r).await?;
    let sref = st.seal().await?;
    st.close().await;
    stats.segments_ms = put_us / 1000;
    stats.apply_seal_ms = (t1.elapsed().as_micros() as u64).saturating_sub(put_us) / 1000;
    if crash(Step::RecoverSealed) {
        anyhow::bail!(Crash);
    }
    if crash(Step::RecoverBeforeManifest) {
        anyhow::bail!(Crash);
    }
    let t3 = Instant::now();
    let mut gaps = m.gaps.clone();
    if s < r {
        gaps.push((s, r));
    }
    let generation = m.generation() + 1;
    let now = chrono::Utc::now().timestamp_millis();
    let next = Manifest {
        epoch,
        leader: id.to_string(),
        flushed: r,
        reserve: r + o.headroom,
        next_ordinal: ord,
        segments,
        state: Some(sref.clone()),
        cursors: cursors.clone(),
        flushes: m.flushes + 1,
        at_ms: now,
        gaps,
        recovery: Some(Recovery { generation, epoch, after: s, base: r, cursors, at_ms: now }),
    };
    match cas_manifest(store, &next, Some(Some(p.etag.clone()))).await {
        Ok(Some(_)) => {}
        Ok(None) => {
            let _ = state::delete_checkpoint(store, &sref).await;
            anyhow::bail!("qlog recovery: the manifest moved under it");
        }
        Err(e) => {
            if !matches!(read_manifest(store).await, Ok(Some((x, _))) if x == next) {
                let _ = state::delete_checkpoint(store, &sref).await;
                return Err(e.context("qlog recovery: manifest CAS"));
            }
        }
    }
    stats.manifest_ms = t3.elapsed().as_millis() as u64;
    stats.generation = generation;
    if crash(Step::RecoverAfterManifest) {
        anyhow::bail!(Crash);
    }
    // the clone pins what it needs of the old state with its own checkpoint
    if let Some(old) = &m.state
        && let Err(e) = state::delete_checkpoint(store, old).await
    {
        tracing::warn!("qlog recovery: deleting the old manifest's checkpoint failed: {e:#}");
    }
    stats.total_ms = t0.elapsed().as_millis() as u64 + stats.read_ms;
    Ok(next)
}

async fn put_recovery_segment(
    store: &Store,
    b: SegmentBuilder,
    ord: u64,
    first: u64,
    last: u64,
) -> anyhow::Result<SegRef> {
    let obj = seal(b, ord, ord).await?;
    let bytes = obj.len() as u64;
    store
        .raw
        .put_opts(
            &log::segment_path(store, LOG_ID, ord),
            PutPayload::from(obj),
            PutOptions { mode: PutMode::Create, ..Default::default() },
        )
        .await
        .map_err(|e| anyhow::anyhow!("qlog recovery: segment {ord}: {e}"))?;
    Ok(SegRef { ordinal: ord, first, last, bytes })
}

#[cfg(test)]
pub(crate) async fn put_test_segment(store: &Store, ord: u64, es: &[Entry]) {
    let mut b = SegmentBuilder::for_log(LOG_ID);
    for e in es {
        push(&mut b, e);
    }
    put_recovery_segment(store, b, ord, es[0].seq, es.last().expect("non-empty").seq).await.unwrap();
}

#[cfg(test)]
pub(crate) async fn put_test_manifest(store: &Store, m: &Manifest) {
    use object_store::ObjectStoreExt;
    store.raw.put(&manifest_path(store), PutPayload::from(serde_json::to_vec(m).unwrap())).await.unwrap();
}

#[cfg(test)]
mod tests {
    use super::*;

    fn with_gaps(gaps: &[(u64, u64)]) -> Manifest {
        Manifest { gaps: gaps.to_vec(), ..Default::default() }
    }

    #[test]
    fn past_gaps_edges() {
        for gaps in [vec![(10, 20), (20, 30), (50, 60)], vec![(50, 60), (20, 30), (10, 20)]] {
            let m = with_gaps(&gaps);
            for (at, want) in [
                (0, 0),
                (9, 9),
                (10, 30),
                (11, 30),
                (19, 30),
                (20, 30),
                (29, 30),
                (30, 30),
                (31, 31),
                (49, 49),
                (50, 60),
                (59, 60),
                (60, 60),
                (61, 61),
            ] {
                assert_eq!(m.past_gaps(at), want, "at {at} gaps {gaps:?}");
            }
        }
        // never written, but must still end past both
        assert_eq!(with_gaps(&[(10, 20), (15, 30)]).past_gaps(12), 30);
        // a recovery before anything was flushed
        assert_eq!(with_gaps(&[(0, 100)]).past_gaps(0), 100);
        assert_eq!(with_gaps(&[]).past_gaps(7), 7);
        let m = with_gaps(&[(10, 20), (20, 30), (50, 60)]);
        for last in 0..70 {
            let inside = (11..30).contains(&last) || (51..60).contains(&last);
            assert!(inside || m.continues(last, m.past_gaps(last) + 1), "last {last}");
        }
    }

    async fn put(store: &Store, ord: u64, seqs: std::ops::RangeInclusive<u64>, epoch: u64) {
        let es: Vec<Entry> = seqs.map(|s| Entry::new(epoch, s, bytes::Bytes::from(format!("e{epoch}s{s}")))).collect();
        put_test_segment(store, ord, &es).await;
    }

    fn seqs(es: &[Entry]) -> Vec<u64> {
        es.iter().map(|e| e.seq).collect()
    }

    #[tokio::test]
    async fn read_bucket_entries_across_gaps() {
        let store = Store::memory(None);
        // 1..=10, gap (10, 20], 21..=30, gaps (30, 40] and (40, 50], 51..=55
        put(&store, 0, 1..=10, 1).await;
        put(&store, 1, 21..=30, 2).await;
        put(&store, 2, 51..=55, 3).await;
        let mut c = None;
        // there's no epoch for the seq before a gap's end
        assert!(read_bucket(&store, &mut c, 21, 30, 1 << 20).await.unwrap().is_none());
        let es = read_bucket_entries(&store, &mut c, 21, 30, 1 << 20).await.unwrap().unwrap();
        assert_eq!(seqs(&es), (21..=30).collect::<Vec<_>>());
        let es = read_bucket_entries(&store, &mut c, 51, 100, 1 << 20).await.unwrap().unwrap();
        assert_eq!(seqs(&es), (51..=55).collect::<Vec<_>>());
        for s in [11, 15, 20, 31, 40, 45, 50] {
            let r = read_bucket_entries(&store, &mut c, s, 100, 1 << 20).await.unwrap();
            assert!(r.is_none(), "seq {s}: {:?}", r.map(|es| seqs(&es)));
        }
        // an emitter's walk from behind the first gap, crossing each gap
        // before reading
        let m = with_gaps(&[(10, 20), (30, 40), (40, 50)]);
        let (mut next, upto, mut got) = (6, 55, Vec::new());
        loop {
            next = m.past_gaps(next - 1) + 1;
            if next > upto {
                break;
            }
            let es = read_bucket_entries(&store, &mut c, next, upto, 16).await.unwrap().unwrap();
            assert_eq!(es[0].seq, next);
            next = es.last().unwrap().seq + 1;
            got.extend(seqs(&es));
        }
        assert_eq!(got, (6..=10).chain(21..=30).chain(51..=55).collect::<Vec<_>>());
    }

    /// A flush that died with a PUT missing below ones that landed: the
    /// recovery adopts up to the hole and clears what's past it, so its
    /// salvage can take those ordinals.
    #[tokio::test]
    async fn recovery_stops_at_a_hole_and_clears_past_it() {
        let store = Store::memory(None);
        put(&store, 0, 1..=10, 1).await;
        put(&store, 1, 11..=20, 1).await;
        // ordinal 2 (21..=30) never landed
        put(&store, 3, 31..=40, 1).await;
        put(&store, 2 + PUT_WINDOW, 99..=99, 1).await;
        put_test_manifest(&store, &Manifest { flushed: 10, reserve: 1000, next_ordinal: 1, ..Default::default() })
            .await;
        let p = recovery_point(&store).await.unwrap().unwrap();
        assert_eq!((p.orphans.len(), p.flushed), (1, 20));
        assert!(matches!(log::read_head(&store, LOG_ID, 3).await.unwrap(), Head::Missing));
        // past the window: no flush put it there
        assert!(matches!(log::read_head(&store, LOG_ID, 2 + PUT_WINDOW).await.unwrap(), Head::Segment(_)));
    }

    /// Why an emitter crosses a gap before reading the bucket: a deposed
    /// leader's flush can put a segment at the recovery's next ordinal,
    /// holding seqs from the gap's start, and nothing in the bucket read
    /// tells it apart.
    #[tokio::test]
    async fn a_stale_segment_in_a_gap_is_readable_at_its_start() {
        let store = Store::memory(None);
        put(&store, 0, 1..=10, 1).await;
        put(&store, 1, 11..=15, 1).await;
        let m = Manifest { flushed: 100, reserve: 200, next_ordinal: 1, gaps: vec![(10, 100)], ..Default::default() };
        let mut c = None;
        let es = read_bucket_entries(&store, &mut c, 11, 150, 1 << 20).await.unwrap().unwrap();
        assert_eq!(seqs(&es), (11..=15).collect::<Vec<_>>());
        assert!(read_bucket(&store, &mut c, 11, 150, 1 << 20).await.unwrap().is_some());
        assert_eq!(m.past_gaps(10), 100);
    }
}
