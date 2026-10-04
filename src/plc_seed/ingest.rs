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
//! it durable (`Sink::flush`), so a restart re-reads at most one interval.
//! Any sink error ends the run, and the supervisor starts over from the
//! last checkpoint.

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
use vlpds::store::Store;

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
        }
    }
}

/// Where entries go: the DID owners' shards.
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
    /// Every window but the last is done and the last one read a short page.
    pub caught_up: AtomicBool,
    /// The newest `createdAt` read, unix ms.
    pub newest_ms: AtomicU64,
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
        loop {
            if !keep_going() {
                tokio::time::sleep(Duration::from_secs(5)).await;
                continue;
            }
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
                let n = self.sink.apply(acc.drain().collect()).await?;
                self.stats.written.fetch_add(n as u64, Relaxed);
                last_apply = Instant::now();
            }
            if dirty && (last_ck.elapsed() >= self.cfg.checkpoint_every || ck.windows.iter().all(|w| w.done)) {
                self.checkpoint(&mut acc, &mut ck).await?;
                dirty = false;
                last_ck = Instant::now();
            }
            if last_log.0.elapsed() >= Duration::from_secs(60) {
                let ops = self.stats.ops.load(Relaxed);
                tracing::info!(
                    total = ck.ops(),
                    ops_per_sec = (ops - last_log.1) as f64 / last_log.0.elapsed().as_secs_f64(),
                    newest = %format_ms(self.stats.newest_ms.load(Relaxed)),
                    windows_left = ck.windows.iter().filter(|w| !w.done).count(),
                    throttled = self.stats.throttled.load(Relaxed),
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
            let n = self.sink.apply(acc.drain().collect()).await?;
            self.stats.written.fetch_add(n as u64, Relaxed);
        }
        self.sink.flush().await?;
        ck.updated_ms = crate::policy::store::now_ms();
        ck.save(&self.store).await?;
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
            self.pace.take().await;
            let body = match self.fetch().await {
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
            let page = self.page(&body);
            let (done, short) = (page.done, page.short);
            if tx.send(Ok(page)).await.is_err() {
                return;
            }
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
