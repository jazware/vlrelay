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
use object_store::path::Path;
use object_store::{ObjectStoreExt, PutMode, PutOptions, PutPayload, UpdateVersion};
use parking_lot::Mutex;
use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;
use std::sync::Arc;
use std::time::{Duration, Instant};
use vlpds::nodelog::{self, Head};
use vlpds::segment::{self, SegmentBuilder};
use vlpds::slots::ShardId;
use vlpds::store::Store;

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
    for mf in vlpds::metrics::OBJ_REQUESTS.collect() {
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
}

/// The flush's counters, shared with `/qlog/status`.
#[derive(Default)]
pub struct Shared {
    s: Mutex<Stats>,
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
    pub applied: u64,
    pub last_flushed: u64,
    pub last_reserve: u64,
}

fn hist() -> hdrhistogram::Histogram<u64> {
    hdrhistogram::Histogram::new_with_bounds(1, 600_000_000, 3).expect("bounds")
}

impl Shared {
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
            applied: s.applied,
            last_flushed: s.last.as_ref().map_or(0, |m| m.flushed),
            last_reserve: s.last.as_ref().map_or(0, |m| m.reserve),
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
    let store = node.store().clone();
    let mut l = Leader { node, epoch, o, store, man: Manifest::default(), etag: String::new(), crashed: false };
    if let Err(e) = l.run().await {
        tracing::warn!(epoch, "qlog flush: stopped: {e:#}");
        l.node.flush.s.lock().failed += 1;
    }
}

struct Leader {
    node: Arc<Node>,
    epoch: u64,
    o: Options,
    store: Store,
    man: Manifest,
    etag: String,
    /// Cut short by the crash hook: leave everything as a dead process would.
    crashed: bool,
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
        self.node.flush.s.lock().fences += 1;
        self.node.set_flushed(self.man.flushed, self.man.reserve);
        tracing::info!(epoch = self.epoch, flushed = self.man.flushed, reserve = self.man.reserve, "qlog flush: manifest fenced");
        if self.crash(Step::Fenced) {
            return Ok(());
        }
        // checkpoints of flushes that never committed (this or an older leader's)
        let keep = self.man.state.as_ref().map(|s| s.checkpoint.clone());
        if let Err(e) = state::delete_checkpoints_except(&self.store, keep.as_deref()).await {
            tracing::warn!("qlog flush: deleting stale checkpoints failed: {e:#}");
        }
        let mut st = loop {
            if self.leading().is_none() {
                return Ok(());
            }
            match State::open(&self.store).await {
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
            while st.applied() < commit {
                let chunk = self.node.committed_chunk(st.applied() + 1, commit, 4 << 20).await?;
                st.apply(&chunk).await?;
                if tokio::time::Instant::now() >= next_flush {
                    break;
                }
            }
            self.node.flush.s.lock().applied = st.applied();
            if tokio::time::Instant::now() >= next_flush {
                next_flush = tokio::time::Instant::now() + self.o.interval;
                match self.flush(st).await {
                    Ok(Outcome::Stop) => return Ok(()),
                    Ok(Outcome::Retry) => self.node.flush.s.lock().aborted += 1,
                    Ok(Outcome::Done | Outcome::Nothing) => {}
                    Err(e) => {
                        tracing::warn!(epoch = self.epoch, "qlog flush failed: {e:#}");
                        self.node.flush.s.lock().failed += 1;
                    }
                }
                continue;
            }
            tokio::select! {
                _ = commit_rx.changed() => {}
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
        // the applier is this task: nothing above F is written until it returns
        let sref = st.seal().await?;
        let seal_us = t0.elapsed().as_micros() as u64;
        if self.crash(Step::Sealed) {
            return Ok(Outcome::Stop);
        }
        let segs = match self.put_segments(f).await {
            Ok(Some(s)) => s,
            r => {
                let _ = state::delete_checkpoint(&self.store, &sref.checkpoint).await;
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
                    let _ = state::delete_checkpoint(&self.store, &sref.checkpoint).await;
                    return Err(e.context("manifest CAS"));
                }
            }
        };
        if self.crash(Step::AfterManifest) {
            return Ok(Outcome::Stop);
        }
        let old = std::mem::replace(&mut self.man, next);
        self.etag = etag;
        self.node.set_flushed(self.man.flushed, self.man.reserve);
        if let Some(o) = old.state.filter(|o| o.checkpoint != sref.checkpoint)
            && let Err(e) = state::delete_checkpoint(&self.store, &o.checkpoint).await
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
        let _ = state::delete_checkpoint(&self.store, &sref.checkpoint).await;
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

    /// Uploads (man.flushed, f] as segments from `man.next_ordinal`. None:
    /// an existing segment there reaches past `f` (try again later).
    #[allow(clippy::type_complexity)]
    async fn put_segments(&mut self, f: u64) -> anyhow::Result<Option<(Vec<SegRef>, (u64, u64))>> {
        let mut out = Vec::new();
        let (mut raw, mut n) = (0u64, 0u64);
        let mut ord = self.man.next_ordinal;
        let mut from = self.man.flushed + 1;
        while from <= f {
            let mut b = SegmentBuilder::for_log(LOG_ID);
            let mut last = from - 1;
            'fill: while last < f {
                for e in self.node.committed_chunk(last + 1, f, 4 << 20).await? {
                    push(&mut b, &e);
                    last = e.seq;
                    if b.len() >= self.o.segment_bytes {
                        break 'fill;
                    }
                }
            }
            let body_len = b.len() as u64;
            let obj = tokio::task::spawn_blocking(move || -> anyhow::Result<Vec<u8>> {
                let obj = b.seal(LOG_ID, ord, ord);
                Ok(segment::compress(&obj, segment::compression_level())?.unwrap_or(obj))
            })
            .await??;
            let bytes = obj.len() as u64;
            let path = nodelog::segment_path(&self.store, LOG_ID, ord);
            let put = self
                .store
                .raw
                .put_opts(&path, PutPayload::from(obj), PutOptions { mode: PutMode::Create, ..Default::default() })
                .await;
            match put {
                Ok(_) => {
                    out.push(SegRef { ordinal: ord, first: from, last, bytes });
                    raw += body_len;
                    n += last - from + 1;
                    if self.crash(Step::SegmentPut) {
                        return Err(Crash.into());
                    }
                }
                Err(object_store::Error::AlreadyExists { .. } | object_store::Error::Precondition { .. }) => {
                    match nodelog::read_head(&self.store, LOG_ID, ord).await? {
                        Head::Segment(h) if h.first_seq as u64 == from => {
                            if h.last_seq as u64 > f {
                                tracing::info!(ord, last = h.last_seq, f, "qlog flush: an existing segment reaches past F");
                                return Ok(None);
                            }
                            tracing::info!(ord, first = h.first_seq, last = h.last_seq, "qlog flush: adopted a segment an unfinished flush wrote");
                            self.node.flush.s.lock().adopted += 1;
                            last = h.last_seq as u64;
                            out.push(SegRef { ordinal: ord, first: from, last, bytes: 0 });
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
                Err(e) => return Err(e.into()),
            }
            ord += 1;
            from = last + 1;
        }
        Ok(Some((out, (raw, n))))
    }
}

/// A segment read back from the bucket: (ordinal, its entries).
pub(crate) type SegCache = Option<(u64, Arc<Vec<Entry>>)>;

async fn load_segment(store: &Store, ord: u64, cache: &mut SegCache) -> anyhow::Result<Option<Arc<Vec<Entry>>>> {
    if let Some((o, es)) = cache
        && *o == ord
    {
        return Ok(Some(es.clone()));
    }
    let Some(segment::LogObject::Segment(_, ents)) = nodelog::read_object(store, LOG_ID, ord).await? else {
        return Ok(None);
    };
    let es: Arc<Vec<Entry>> =
        Arc::new(ents.into_iter().map(|e| Entry::new(e.epoch, e.seq as u64, e.frame)).collect());
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
    if from == 0 || from > upto {
        return Ok(None);
    }
    let ord = vlpds::backfill::seek(store, LOG_ID, from as i64 - 1).await?;
    let Some(seg) = load_segment(store, ord, cache).await? else { return Ok(None) };
    let Some(first) = seg.first().map(|e| e.seq) else { return Ok(None) };
    if first > from {
        return Ok(None);
    }
    let i = (from - first) as usize;
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
    let mut n = 0;
    let mut out = Vec::new();
    for e in seg[i..].iter().take_while(|e| e.seq <= upto) {
        if !out.is_empty() && n + e.data.len() > max_bytes {
            break;
        }
        n += e.data.len();
        out.push(e.clone());
    }
    Ok(Some((prev_epoch, out)))
}

#[derive(Debug)]
struct Crash;

impl std::fmt::Display for Crash {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("crash injected")
    }
}

impl std::error::Error for Crash {}

fn push(b: &mut SegmentBuilder, e: &Entry) {
    b.push(e.seq as i64, ShardId(0), e.epoch, |out| out.extend_from_slice(&e.data), &[]);
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
    pub entries: u64,
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
    let mut dids: BTreeMap<String, [u8; 16]> = BTreeMap::new();
    let mut hosts: BTreeMap<String, Vec<u64>> = BTreeMap::new();
    let mut last = 0u64;
    let mut ord = 0u64;
    loop {
        let Some(obj) = nodelog::read_object(store, LOG_ID, ord).await? else { break };
        let segment::LogObject::Segment(h, ents) = obj else {
            bad(&mut v, format!("ordinal {ord} is a fence"));
            break;
        };
        if h.first_seq as u64 != last + 1 {
            bad(&mut v, format!("segment {ord} starts at {}, after {last}", h.first_seq));
        }
        for e in ents {
            if e.seq as u64 != last + 1 {
                bad(&mut v, format!("segment {ord}: seq {} after {last}", e.seq));
            }
            last = e.seq as u64;
            if ord >= m.next_ordinal || last > m.flushed {
                continue;
            }
            v.entries += 1;
            let ent = Entry::new(e.epoch, last, e.frame.clone());
            if let (Some((did, val)), _) = state::effects(&ent) {
                if let Some((h, n)) = host_event(&did) {
                    hosts.entry(h.to_string()).or_default().push(n);
                }
                dids.insert(did, val);
            }
        }
        if ord < m.next_ordinal {
            v.segments += 1;
            if ord + 1 == m.next_ordinal && last != m.flushed {
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
        match &m.state {
            None => bad(&mut v, "no state checkpoint".into()),
            Some(r) => {
                if r.seq != m.flushed {
                    bad(&mut v, format!("state checkpoint at {}, F is {}", r.seq, m.flushed));
                }
                let (kv, l0) = state::read_checkpoint(store, r).await?;
                if l0 != m.flushed {
                    bad(&mut v, format!("checkpoint manifest's last_l0_seq {l0} isn't F {}", m.flushed));
                }
                let applied = kv.get(state::applied_key()).map(|b| u64::from_be_bytes(b[..8].try_into().unwrap_or([0; 8])));
                if applied != Some(m.flushed) {
                    bad(&mut v, format!("state's _applied is {applied:?}, F is {}", m.flushed));
                }
                let mut state_dids = 0u64;
                let mut state_cursors = BTreeMap::new();
                for (k, val) in &kv {
                    if let Some(did) = k.strip_prefix(b"d/") {
                        state_dids += 1;
                        let did = String::from_utf8_lossy(did);
                        match dids.get(did.as_ref()) {
                            Some(x) if x[..] == val[..] => {}
                            Some(_) => bad(&mut v, format!("state holds {did} at another seq or content than the log")),
                            None => bad(&mut v, format!("state holds {did}, which isn't in the log up to F")),
                        }
                    } else if let Some(h) = k.strip_prefix(b"c/") {
                        state_cursors.insert(
                            String::from_utf8_lossy(h).into_owned(),
                            u64::from_be_bytes(val[..8].try_into().unwrap_or([0; 8])),
                        );
                    }
                }
                if state_dids != dids.len() as u64 {
                    bad(&mut v, format!("state holds {state_dids} DIDs, the log up to F {}", dids.len()));
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
        let contiguous = ns.iter().enumerate().take_while(|(i, n)| **n == *i as u64 + 1).count() as u64;
        if *c > contiguous {
            bad(&mut v, format!("host {h}'s cursor {c} is ahead of the log at F (events 1..={contiguous} there)"));
        }
        v.hosts += 1;
    }
    v.dids = dids.len() as u64;
    v.ok = v.messages.is_empty();
    Ok(v)
}
