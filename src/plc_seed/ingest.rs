//! The export reader: pages of `GET {plc}/export?count=N&after=<createdAt>`,
//! a few time windows read side by side, merged newest-wins per DID and
//! handed to a [`Sink`], with the cursors checkpointed in the bucket.
//!
//! The export is one cursor-ordered stream, so one reader is bound by a
//! page's round trip (~0.6 s for 1,000 ops from plc.directory). Splitting
//! history into windows, each its own cursor, lets the request rate rather
//! than the latency set the pace. Ops then land out of order across
//! windows, which the newest-wins write absorbs. The last window has no end
//! and becomes the live tail once it catches up.
//!
//! A checkpoint is written only after the sink has made everything before
//! it durable (`Sink::flush`), so a restart or a new leader re-reads at most
//! one interval. Any sink error ends the run, and the job starts over from
//! the last checkpoint.

use super::{ExportOp, LineError, Seed, format_ms, parse_line, parse_ms};
use crate::policy::store as obj;
use object_store::PutMode;
use parking_lot::Mutex;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering::Relaxed};
use std::time::{Duration, Instant};
use tokio::sync::mpsc;
use vlsync_store::store::Store;

#[derive(Clone, Debug)]
pub struct Config {
    /// The directory, e.g. `https://plc.directory`.
    pub url: String,
    /// Export requests per second, all windows together.
    pub rate: f64,
    /// Windows read at once on a fresh start.
    pub streams: usize,
    /// Ops per request (the export's maximum is 1,000).
    pub page: usize,
    /// How often the caught-up tail asks again.
    pub tail_poll: Duration,
    /// Entries are handed to the sink at least this often, or every
    /// `batch` DIDs.
    pub apply_every: Duration,
    pub batch: usize,
    pub checkpoint_every: Duration,
    /// History starts here (the first op on plc.directory is from
    /// 2022-11-17).
    pub start_ms: u64,
    /// The leader stops its term of the export (and its seed reads) while
    /// the process has more than this allocated, MiB; 0: no limit.
    pub mem_budget_mb: u64,
    /// How long a held-back export waits for the memory to fall under its
    /// resume mark before it runs anyway, under the budget; doubled each
    /// time up to 16x while the memory stays there.
    pub mem_hold_retry: Duration,
}

impl Config {
    pub fn new(url: &str) -> Config {
        Config {
            url: url.trim_end_matches('/').to_string(),
            rate: 2.0,
            streams: 4,
            page: 1000,
            tail_poll: Duration::from_secs(2),
            apply_every: Duration::from_secs(2),
            batch: 50_000,
            checkpoint_every: Duration::from_secs(10),
            start_ms: 1_668_643_200_000,
            mem_budget_mb: 0,
            mem_hold_retry: Duration::from_secs(60),
        }
    }
}

/// Where entries go: the seed database.
#[async_trait::async_trait]
pub trait Sink: Send + Sync {
    /// Writes the entries newer than what's stored; returns how many.
    async fn apply(&self, ops: Vec<(String, Seed)>) -> anyhow::Result<usize>;
    /// Makes every applied entry durable.
    async fn flush(&self) -> anyhow::Result<()>;
}

pub const CHECKPOINT: &str = "plc/export-checkpoint.json";

#[derive(Clone, Debug, Default, Serialize, Deserialize, PartialEq)]
#[serde(rename_all = "camelCase")]
pub struct Window {
    /// The last `createdAt` read (the next request's `after`).
    pub after: String,
    /// Ops after this belong to the next window; None for the last one.
    pub until: Option<String>,
    /// Ops read in this window so far.
    pub count: u64,
    pub done: bool,
}

#[derive(Clone, Debug, Default, Serialize, Deserialize, PartialEq)]
#[serde(rename_all = "camelCase")]
pub struct Checkpoint {
    pub windows: Vec<Window>,
    pub updated_ms: i64,
}

impl Checkpoint {
    pub fn fresh(cfg: &Config, now_ms: u64) -> Checkpoint {
        let n = cfg.streams.max(1) as u64;
        let span = now_ms.saturating_sub(cfg.start_ms);
        let at = |k: u64| cfg.start_ms + span * k / n;
        let windows = (0..n)
            .map(|k| Window {
                // `after` is exclusive: start a millisecond early
                after: format_ms(at(k).saturating_sub(1)),
                until: (k + 1 < n).then(|| format_ms(at(k + 1) - 1)),
                count: 0,
                done: false,
            })
            .collect();
        Checkpoint { windows, updated_ms: 0 }
    }

    pub fn ops(&self) -> u64 {
        self.windows.iter().map(|w| w.count).sum()
    }

    pub async fn load(store: &Store) -> anyhow::Result<Option<Checkpoint>> {
        Ok(match obj::get(store, &obj::path(store, CHECKPOINT), None).await? {
            Some((b, _)) => Some(serde_json::from_slice(&b)?),
            None => None,
        })
    }

    pub async fn save(&self, store: &Store) -> anyhow::Result<()> {
        obj::put(store, &obj::path(store, CHECKPOINT), serde_json::to_vec(self)?, PutMode::Overwrite).await?;
        Ok(())
    }
}

#[derive(Default, Debug)]
pub struct Stats {
    pub requests: AtomicU64,
    pub pages: AtomicU64,
    pub bytes: AtomicU64,
    pub ops: AtomicU64,
    pub written: AtomicU64,
    pub nullified: AtomicU64,
    pub invalid: AtomicU64,
    pub throttled: AtomicU64,
    pub errors: AtomicU64,
    pub checkpoints: AtomicU64,
    pub restarts: AtomicU64,
    /// Time spent per phase, microseconds summed over every reader
    /// ([`Phases`]).
    pub phases: Phases,
    /// Every window but the last is done and the last one read a short page.
    pub caught_up: AtomicBool,
    /// The newest `createdAt` read, unix ms.
    pub newest_ms: AtomicU64,
    /// [`Stats::rate`]'s value, f64 bits, kept by the ingest loop's
    /// [`RateMeter`].
    rate_bits: AtomicU64,
}

impl Stats {
    /// Export ops read per second, smoothed over about
    /// [`RateMeter::TAU`]; 0 when no ingest is running.
    pub fn rate(&self) -> f64 {
        f64::from_bits(self.rate_bits.load(Relaxed))
    }

    fn set_rate(&self, r: f64) {
        self.rate_bits.store(r.to_bits(), Relaxed);
    }
}

struct ZeroRate<'a>(&'a Stats);

impl Drop for ZeroRate<'_> {
    fn drop(&mut self) {
        self.0.set_rate(0.0);
    }
}

/// An exponentially weighted ops rate, sampled by the ingest loop so the
/// admin view's value doesn't depend on who reads it or how often.
#[derive(Debug)]
pub struct RateMeter {
    at: Instant,
    ops: u64,
    ewma: f64,
    /// The weight the EWMA has gathered since the start, 1 - e^(-t/τ).
    weight: f64,
}

impl RateMeter {
    /// The export reads in pages of up to 1,000 ops at a couple of requests
    /// a second, so per-second counts are bursty; ~15 s smooths a few
    /// pages while still showing a pace-out or catch-up within half a
    /// minute.
    pub const TAU: Duration = Duration::from_secs(15);
    /// The loop wakes at least every `apply_every` (2 s) even when idle;
    /// sampling no more often than this keeps each sample's interval long
    /// enough to hold whole pages.
    pub const EVERY: Duration = Duration::from_secs(1);

    pub fn new(now: Instant, ops: u64) -> RateMeter {
        RateMeter { at: now, ops, ewma: 0.0, weight: 0.0 }
    }

    /// Folds in the ops since the last sample, if at least [`Self::EVERY`]
    /// has passed, and publishes the rate to `stats`.
    pub fn sample(&mut self, stats: &Stats, now: Instant, ops: u64) {
        let dt = now.saturating_duration_since(self.at);
        if dt < Self::EVERY {
            return;
        }
        let inst = ops.saturating_sub(self.ops) as f64 / dt.as_secs_f64();
        // α from the actual interval, so a late sample (a slow apply or
        // checkpoint) weighs what that much time should.
        let alpha = 1.0 - (-dt.as_secs_f64() / Self::TAU.as_secs_f64()).exp();
        self.ewma += alpha * (inst - self.ewma);
        self.weight += alpha * (1.0 - self.weight);
        self.at = now;
        self.ops = ops;
        // Dividing by the weight gathered so far debiases the start (the
        // EWMA begins at 0): the first samples read as the average since
        // the start rather than a slow climb from 0.
        stats.set_rate(self.ewma / self.weight);
    }
}

#[derive(Default, Debug)]
pub struct Phases {
    /// Readers waiting on the request pace.
    pub pace_us: AtomicU64,
    /// Request sent to body read.
    pub fetch_us: AtomicU64,
    pub parse_us: AtomicU64,
    /// Readers waiting for the coordinator to take their page.
    pub handoff_us: AtomicU64,
    /// The coordinator in `Sink::apply`.
    pub apply_us: AtomicU64,
    /// The coordinator in `Sink::flush` and the checkpoint's PUT.
    pub checkpoint_us: AtomicU64,
}

impl Phases {
    pub fn snapshot(&self) -> [(&'static str, u64); 6] {
        [
            ("pace", self.pace_us.load(Relaxed)),
            ("fetch", self.fetch_us.load(Relaxed)),
            ("parse", self.parse_us.load(Relaxed)),
            ("handoff", self.handoff_us.load(Relaxed)),
            ("apply", self.apply_us.load(Relaxed)),
            ("checkpoint", self.checkpoint_us.load(Relaxed)),
        ]
    }
}

fn add_us(c: &AtomicU64, since: Instant) {
    c.fetch_add(since.elapsed().as_micros() as u64, Relaxed);
}

/// The checkpoint's windows as the admin API shows them: each one's span
/// and how far it's read.
pub fn windows_view(ck: &Checkpoint, start_ms: u64) -> Vec<crate::admin::PlcWindow> {
    let now = crate::policy::store::now_ms();
    let ms = |s: &str| parse_ms(s).map_or(0, |v| v as i64);
    let mut from = start_ms as i64;
    ck.windows
        .iter()
        .map(|w| {
            let after = ms(&w.after).max(from);
            let until = w.until.as_deref().map(ms);
            let end = until.unwrap_or(now);
            let progress = if w.done {
                1.0
            } else if end > from {
                ((after - from) as f64 / (end - from) as f64).clamp(0.0, 1.0)
            } else {
                0.0
            };
            let out = crate::admin::PlcWindow {
                from_ms: from,
                after_ms: after,
                until_ms: until,
                ops: w.count,
                done: w.done,
                progress,
            };
            if let Some(u) = until {
                from = u + 1;
            }
            out
        })
        .collect()
}

/// Paces requests across windows.
struct Pace {
    every: Duration,
    next: Mutex<Instant>,
}

impl Pace {
    async fn take(&self) {
        let wait = {
            let mut n = self.next.lock();
            let now = Instant::now();
            let at = (*n).max(now);
            *n = at + self.every;
            at - now
        };
        if !wait.is_zero() {
            tokio::time::sleep(wait).await;
        }
    }
}

struct Page {
    window: usize,
    ops: Vec<ExportOp>,
    /// The cursor after this page.
    after: String,
    read: u64,
    done: bool,
    /// The export had nothing more right now.
    short: bool,
}

pub struct Ingester {
    pub cfg: Config,
    pub store: Store,
    pub sink: Arc<dyn Sink>,
    pub stats: Arc<Stats>,
    http: reqwest::Client,
}

const MAX_PAGE_BYTES: usize = 32 << 20;

impl Ingester {
    pub fn new(cfg: Config, store: Store, sink: Arc<dyn Sink>) -> Arc<Ingester> {
        let http = reqwest::Client::builder()
            .timeout(Duration::from_secs(60))
            .user_agent(concat!("vlrelay/", env!("CARGO_PKG_VERSION")))
            .build()
            .expect("http client");
        Arc::new(Ingester { cfg, store, sink, stats: Default::default(), http })
    }

    /// Runs until `keep_going` turns false, restarting from the last
    /// checkpoint after any error.
    pub async fn supervise(self: Arc<Self>, keep_going: Arc<dyn Fn() -> bool + Send + Sync>) {
        let mut backoff = Duration::from_secs(1);
        while keep_going() {
            match self.run(keep_going.clone()).await {
                Ok(()) => backoff = Duration::from_secs(1),
                Err(e) => {
                    self.stats.restarts.fetch_add(1, Relaxed);
                    tracing::warn!("PLC export ingest stopped, resuming from the checkpoint: {e:#}");
                    tokio::time::sleep(backoff).await;
                    backoff = (backoff * 2).min(Duration::from_secs(60));
                }
            }
        }
    }

    /// One run from the stored checkpoint. Returns when `keep_going` turns
    /// false (after a last checkpoint), or on the first error.
    pub async fn run(&self, keep_going: Arc<dyn Fn() -> bool + Send + Sync>) -> anyhow::Result<()> {
        let now = crate::policy::store::now_ms() as u64;
        let mut ck = match Checkpoint::load(&self.store).await? {
            Some(c) if !c.windows.is_empty() => c,
            _ => Checkpoint::fresh(&self.cfg, now),
        };
        tracing::info!(windows = ck.windows.len(), ops = ck.ops(), url = %self.cfg.url, "PLC export ingest starting");
        let pace = Arc::new(Pace {
            every: Duration::from_secs_f64(1.0 / self.cfg.rate.max(0.001)),
            next: Mutex::new(Instant::now()),
        });
        let (tx, mut rx) = mpsc::channel::<anyhow::Result<Page>>(ck.windows.len() * 2 + 2);
        let mut readers = tokio::task::JoinSet::new();
        for (i, w) in ck.windows.iter().enumerate() {
            if w.done {
                continue;
            }
            let r = Reader {
                cfg: self.cfg.clone(),
                http: self.http.clone(),
                pace: pace.clone(),
                stats: self.stats.clone(),
                window: i,
                after: w.after.clone(),
                until_ms: w.until.as_deref().and_then(parse_ms),
                boundary: (0, Default::default()),
            };
            readers.spawn(r.run(tx.clone()));
        }
        drop(tx);
        let mut acc: HashMap<String, Seed> = HashMap::new();
        let mut last_apply = Instant::now();
        let mut last_ck = Instant::now();
        let mut last_log = (Instant::now(), self.stats.ops.load(Relaxed));
        let mut meter = RateMeter::new(Instant::now(), self.stats.ops.load(Relaxed));
        // A stopped run (an error's backoff, a lost term) reads nothing, so
        // its last rate mustn't linger.
        let _zero = ZeroRate(&self.stats);
        let mut dirty = false;
        let mut tail_short = false;
        loop {
            let page = tokio::select! {
                p = rx.recv() => p,
                _ = tokio::time::sleep(self.cfg.apply_every) => Some(Ok(Page::idle())),
            };
            let Some(page) = page else { break };
            let page = page?;
            if page.window != usize::MAX {
                let w = &mut ck.windows[page.window];
                w.after = page.after;
                w.count += page.read;
                w.done |= page.done;
                if w.until.is_none() {
                    tail_short = page.short;
                }
                for op in page.ops {
                    match acc.get(&op.did) {
                        Some(s) if s.created_ms >= op.seed.created_ms => {}
                        _ => {
                            acc.insert(op.did, op.seed);
                        }
                    }
                }
                dirty = true;
            }
            let others_done = ck.windows.iter().all(|w| w.done || w.until.is_none());
            let caught_up = others_done && tail_short;
            let apply = acc.len() >= self.cfg.batch
                || (!acc.is_empty() && (caught_up || last_apply.elapsed() >= self.cfg.apply_every));
            if apply {
                let t = Instant::now();
                let n = self.sink.apply(acc.drain().collect()).await?;
                add_us(&self.stats.phases.apply_us, t);
                self.stats.written.fetch_add(n as u64, Relaxed);
                last_apply = Instant::now();
            }
            if dirty && (last_ck.elapsed() >= self.cfg.checkpoint_every || ck.windows.iter().all(|w| w.done)) {
                self.checkpoint(&mut acc, &mut ck).await?;
                dirty = false;
                last_ck = Instant::now();
            }
            meter.sample(&self.stats, Instant::now(), self.stats.ops.load(Relaxed));
            if last_log.0.elapsed() >= Duration::from_secs(60) {
                let ops = self.stats.ops.load(Relaxed);
                tracing::info!(
                    total = ck.ops(),
                    ops_per_sec = (ops - last_log.1) as f64 / last_log.0.elapsed().as_secs_f64(),
                    newest = %format_ms(self.stats.newest_ms.load(Relaxed)),
                    windows_left = ck.windows.iter().filter(|w| !w.done).count(),
                    throttled = self.stats.throttled.load(Relaxed),
                    phases_ms = ?self.stats.phases.snapshot().map(|(k, us)| (k, us / 1000)),
                    "PLC export ingest"
                );
                last_log = (Instant::now(), ops);
            }
            if caught_up && !self.stats.caught_up.swap(true, Relaxed) {
                tracing::info!(ops = ck.ops(), "PLC export ingest caught up; following the tail");
            }
            if !keep_going() {
                break;
            }
        }
        readers.abort_all();
        if dirty {
            self.checkpoint(&mut acc, &mut ck).await?;
        }
        Ok(())
    }

    async fn checkpoint(&self, acc: &mut HashMap<String, Seed>, ck: &mut Checkpoint) -> anyhow::Result<()> {
        if !acc.is_empty() {
            let t = Instant::now();
            let n = self.sink.apply(acc.drain().collect()).await?;
            add_us(&self.stats.phases.apply_us, t);
            self.stats.written.fetch_add(n as u64, Relaxed);
        }
        let t = Instant::now();
        self.sink.flush().await?;
        ck.updated_ms = crate::policy::store::now_ms();
        ck.save(&self.store).await?;
        add_us(&self.stats.phases.checkpoint_us, t);
        self.stats.checkpoints.fetch_add(1, Relaxed);
        Ok(())
    }
}

impl Page {
    /// A tick with no page, so the coordinator applies on time.
    fn idle() -> Page {
        Page { window: usize::MAX, ops: Vec::new(), after: String::new(), read: 0, done: false, short: false }
    }
}

struct Reader {
    cfg: Config,
    http: reqwest::Client,
    pace: Arc<Pace>,
    stats: Arc<Stats>,
    window: usize,
    /// The next request's `after`.
    after: String,
    until_ms: Option<u64>,
    /// The newest `createdAt` read and the DIDs read at it. `after` trails
    /// it by a millisecond, since ops sharing a millisecond can straddle a
    /// page and `after` is exclusive; these are the repeats to skip.
    boundary: (u64, std::collections::HashSet<String>),
}

enum Fetched {
    Body(bytes::Bytes),
    /// 429 or 5xx: wait this long.
    Later(Duration),
}

impl Reader {
    async fn run(mut self, tx: mpsc::Sender<anyhow::Result<Page>>) {
        let mut backoff = Duration::from_secs(1);
        loop {
            let t = Instant::now();
            self.pace.take().await;
            add_us(&self.stats.phases.pace_us, t);
            let t = Instant::now();
            let fetched = self.fetch().await;
            add_us(&self.stats.phases.fetch_us, t);
            let body = match fetched {
                Ok(Fetched::Body(b)) => {
                    backoff = Duration::from_secs(1);
                    b
                }
                Ok(Fetched::Later(d)) => {
                    self.stats.throttled.fetch_add(1, Relaxed);
                    tokio::time::sleep(d.max(backoff)).await;
                    backoff = (backoff * 2).min(Duration::from_secs(120));
                    continue;
                }
                Err(e) => {
                    self.stats.errors.fetch_add(1, Relaxed);
                    tracing::debug!(window = self.window, "PLC export request failed: {e:#}");
                    tokio::time::sleep(backoff).await;
                    backoff = (backoff * 2).min(Duration::from_secs(120));
                    continue;
                }
            };
            let t = Instant::now();
            let page = self.page(&body);
            add_us(&self.stats.phases.parse_us, t);
            let (done, short) = (page.done, page.short);
            let t = Instant::now();
            if tx.send(Ok(page)).await.is_err() {
                return;
            }
            add_us(&self.stats.phases.handoff_us, t);
            if done {
                return;
            }
            if short {
                tokio::time::sleep(self.cfg.tail_poll).await;
            }
        }
    }

    async fn fetch(&self) -> anyhow::Result<Fetched> {
        use futures::StreamExt;
        let url = format!("{}/export", self.cfg.url);
        self.stats.requests.fetch_add(1, Relaxed);
        let r = self
            .http
            .get(url)
            .query(&[("count", self.cfg.page.to_string()), ("after", self.after.clone())])
            .send()
            .await?;
        let st = r.status();
        if st == reqwest::StatusCode::TOO_MANY_REQUESTS || st.is_server_error() {
            let after = r
                .headers()
                .get(reqwest::header::RETRY_AFTER)
                .and_then(|v| v.to_str().ok())
                .and_then(|v| v.parse::<u64>().ok())
                .map_or(Duration::from_secs(1), |s| Duration::from_secs(s.min(600)));
            return Ok(Fetched::Later(after));
        }
        anyhow::ensure!(st.is_success(), "status {st}");
        let mut buf = Vec::new();
        let mut body = r.bytes_stream();
        while let Some(c) = body.next().await {
            let c = c?;
            anyhow::ensure!(buf.len() + c.len() <= MAX_PAGE_BYTES, "page over {MAX_PAGE_BYTES} bytes");
            buf.extend_from_slice(&c);
        }
        Ok(Fetched::Body(buf.into()))
    }

    /// The ops of one response past the cursor, and the cursor after them.
    fn page(&mut self, body: &[u8]) -> Page {
        self.stats.pages.fetch_add(1, Relaxed);
        self.stats.bytes.fetch_add(body.len() as u64, Relaxed);
        let mut ops = Vec::new();
        let (mut lines, mut read, mut done) = (0usize, 0u64, false);
        let prev = self.boundary.0;
        let mut newest = prev;
        let mut at_newest: Vec<String> = Vec::new();
        let mut note = |ms: u64, did: Option<&str>, newest: &mut u64| {
            if ms > *newest {
                *newest = ms;
                at_newest.clear();
            }
            if ms == *newest
                && let Some(d) = did
            {
                at_newest.push(d.to_string());
            }
        };
        for line in body.split(|&b| b == b'\n').filter(|l| !l.iter().all(u8::is_ascii_whitespace)) {
            lines += 1;
            let op = match parse_line(line) {
                Ok(op) => op,
                Err(e) => {
                    let c = if e == LineError::Nullified { &self.stats.nullified } else { &self.stats.invalid };
                    c.fetch_add(1, Relaxed);
                    // still moves the cursor past it
                    if let Some(ms) = created_at(line).as_deref().and_then(parse_ms) {
                        note(ms, None, &mut newest);
                    }
                    continue;
                }
            };
            let ms = op.seed.created_ms;
            if self.until_ms.is_some_and(|u| ms > u) {
                done = true;
                break;
            }
            if ms < prev || (ms == prev && self.boundary.1.contains(&op.did)) {
                continue;
            }
            note(ms, Some(&op.did), &mut newest);
            read += 1;
            ops.push(op);
        }
        self.stats.ops.fetch_add(read, Relaxed);
        self.stats.newest_ms.fetch_max(newest, Relaxed);
        let short = lines < self.cfg.page;
        if newest > 0 {
            if newest == prev {
                if !short && read == 0 {
                    // a whole page inside one millisecond: step past it
                    // rather than ask for the same page again
                    tracing::warn!(window = self.window, at = %format_ms(prev), "a full export page in one millisecond");
                    self.after = format_ms(prev);
                    self.boundary = (prev + 1, Default::default());
                } else {
                    self.boundary.1.extend(at_newest);
                }
            } else {
                self.boundary = (newest, at_newest.into_iter().collect());
                self.after = format_ms(newest - 1);
            }
        }
        // a window whose end is in the past has nothing after a short page
        if short && self.until_ms.is_some() {
            done = true;
        }
        Page { window: self.window, ops, after: self.after.clone(), read, done, short }
    }
}

/// `createdAt` of a line that didn't parse as an op, for the cursor.
fn created_at(line: &[u8]) -> Option<String> {
    #[derive(Deserialize)]
    #[serde(rename_all = "camelCase")]
    struct C {
        created_at: String,
    }
    serde_json::from_slice::<C>(line).ok().map(|c| c.created_at)
}
