//! Host discovery for a cold start, run by the quorum log's leader.
//!
//! Two sources, both in the policy document's `discovery` section:
//!
//! - **Other relays' `com.atproto.sync.listHosts`** (`seedRelays`): read
//!   only, a page at a time at `requestsPerSec`, waiting out 429s and 5xxs;
//!   a whole list again every `refreshIntervalSecs`. This relay never asks
//!   them to crawl anything.
//! - **The PLC export** (`plc`, with `--plc-export`): the distinct PDS hosts
//!   the documents the export reader reads name.
//!
//! A seed relay's `accountCount` for a host it lists as active or idle is
//! kept on the host's record (`tiers::Seeded`), and a `new` or `default`
//! host's limits start from it rather than from the accounts this relay has
//! seen so far. It comes only from the relays the policy names, never from
//! the PDS or the PLC export, and lapses unless a run reports it again.
//!
//! Every host found goes through this relay's own admission
//! ([`Crawler::admit_from`]: domain rules, bans, the allow list, the
//! describeServer probe, the starting tier), as a requestCrawl does. Nothing
//! is imported from the other relay (status, bans, tiers), and a new host
//! starts live, with no backfill. Admissions are paced by their own budget
//! (`connectsPerMin`), not the daily new-host one requestCrawl spends.
//!
//! Each source's progress (its cursor in the list it's reading, its last
//! and next runs, its counts) is saved in the bucket after every page, so a
//! new leader resumes a list where the old one stopped.

use crate::policy::doc::Discovery;
use crate::policy::{Engine, tiers};
use crate::qlog::node::{Node as QNode, Role};
use crate::state::{HostStore, Tier};
use crate::upstream::{CrawlError, Crawler};
use futures::StreamExt;
use parking_lot::Mutex;
use serde::{Deserialize, Serialize};
use std::collections::{BTreeMap, BTreeSet, HashSet};
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering::Relaxed};
use std::time::{Duration, Instant};
use vlsync_store::store::Store;

const STATE: &str = "discovery/state.json";
/// PDS hosts from the PLC export waiting for admission (each is admitted or
/// refused once; a refused one isn't retried until it's named again).
const PLC_PENDING_MAX: usize = 50_000;
const PAGE: usize = 1000;
const MAX_PAGE_BYTES: usize = 16 << 20;
/// Admissions (probes included) in flight at once.
const PROBES: usize = 8;
pub const PLC_SOURCE: &str = "plc";

/// What one source has done: the run in progress (or the last one) and
/// when the next is due.
#[derive(Clone, Debug, Default, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", default)]
pub struct SourceState {
    pub url: Option<String>,
    pub runs: u64,
    pub last_started_ms: Option<i64>,
    pub last_finished_ms: Option<i64>,
    /// Where the run in progress reads next (None: no run in progress).
    pub cursor: Option<String>,
    pub in_progress: bool,
    pub run_requested: bool,
    /// This run's (or the last one's) counts.
    pub hosts_seen: u64,
    /// Hosts this relay already had.
    pub known: u64,
    /// Hosts it didn't, of which `admitted` got in and `refused` didn't.
    pub new: u64,
    pub admitted: u64,
    pub refused: u64,
    /// Hosts whose seeded account count this run wrote.
    pub seeded: u64,
    pub errors: u64,
    /// 429s and 5xxs waited out.
    pub throttled: u64,
    pub pages: u64,
    /// Times a leader took over a run in progress from its cursor.
    pub resumed: u64,
    pub last_error: Option<String>,
}

#[derive(Clone, Debug, Default, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", default)]
pub struct State {
    pub sources: BTreeMap<String, SourceState>,
    pub plc_pending: BTreeSet<String>,
}

/// PDS hosts the PLC export reader saw, for the leader's discovery.
#[derive(Default)]
pub struct Feed {
    hosts: Mutex<BTreeSet<String>>,
}

impl Feed {
    pub fn push(&self, host: &str) {
        let mut h = self.hosts.lock();
        if h.len() < PLC_PENDING_MAX {
            h.insert(host.to_string());
        }
    }

    fn take(&self) -> BTreeSet<String> {
        std::mem::take(&mut *self.hosts.lock())
    }
}

/// `bootstrap:<host of the relay's url>`.
pub fn source_key(url: &str) -> String {
    let host = crate::identity::normalize_host(url).map_or_else(|| url.to_string(), |h| h.0);
    format!("bootstrap:{host}")
}

/// One host in a `listHosts` page.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Listed {
    pub hostname: String,
    /// Its `accountCount`, or 0 when the relay doesn't list it as active or
    /// idle: a relay that throttled or banned a host doesn't vouch for it.
    pub accounts: u64,
}

pub enum Page {
    Hosts {
        hosts: Vec<Listed>,
        cursor: Option<String>,
    },
    /// 429 or 5xx: wait this long.
    Later(Duration),
}

/// One `listHosts` page from `url`.
pub async fn list_hosts(http: &reqwest::Client, url: &str, cursor: Option<&str>) -> anyhow::Result<Page> {
    let mut q: Vec<(&str, String)> = vec![("limit", PAGE.to_string())];
    if let Some(c) = cursor {
        q.push(("cursor", c.to_string()));
    }
    let r = http.get(format!("{}/xrpc/com.atproto.sync.listHosts", url.trim_end_matches('/'))).query(&q).send().await?;
    let st = r.status();
    if st == reqwest::StatusCode::TOO_MANY_REQUESTS || st.is_server_error() {
        let after = r
            .headers()
            .get(reqwest::header::RETRY_AFTER)
            .and_then(|v| v.to_str().ok())
            .and_then(|v| v.parse::<u64>().ok())
            .map_or(Duration::from_secs(5), |s| Duration::from_secs(s.clamp(1, 600)));
        return Ok(Page::Later(after));
    }
    anyhow::ensure!(st.is_success(), "listHosts: status {st}");
    let mut body = Vec::new();
    let mut s = r.bytes_stream();
    while let Some(c) = s.next().await {
        let c = c?;
        anyhow::ensure!(body.len() + c.len() <= MAX_PAGE_BYTES, "listHosts: a page over {MAX_PAGE_BYTES} bytes");
        body.extend_from_slice(&c);
    }
    #[derive(Deserialize)]
    #[serde(rename_all = "camelCase")]
    struct Row {
        hostname: String,
        #[serde(default)]
        account_count: Option<i64>,
        #[serde(default)]
        status: Option<String>,
    }
    #[derive(Deserialize)]
    struct Out {
        cursor: Option<String>,
        #[serde(default)]
        hosts: Vec<Row>,
    }
    let o: Out = serde_json::from_slice(&body)?;
    Ok(Page::Hosts {
        hosts: o
            .hosts
            .into_iter()
            .map(|r| {
                let vouched = r.status.as_deref().is_none_or(|s| matches!(s, "active" | "idle"));
                let accounts = if vouched { r.account_count.unwrap_or(0).max(0) as u64 } else { 0 };
                Listed { hostname: r.hostname, accounts }
            })
            .collect(),
        cursor: o.cursor.filter(|c| !c.is_empty()),
    })
}

/// Admissions a minute, shared by every source.
struct Pace {
    tokens: f64,
    at: Instant,
}

pub struct DiscoveryJob {
    engine: Arc<Engine>,
    crawler: Arc<Crawler>,
    /// The host records, for seeded account counts (None: not kept).
    hosts: Option<Arc<dyn HostStore>>,
    /// The bucket, counted as `qlog_discovery`.
    store: Store,
    http: reqwest::Client,
    pub feed: Arc<Feed>,
    /// This term's state (None while this node doesn't lead).
    state: Mutex<Option<State>>,
    requested: Mutex<HashSet<Option<String>>>,
    pace: Mutex<Pace>,
    running: AtomicBool,
    stopped: AtomicBool,
}

const WATCH: Duration = Duration::from_millis(500);

impl DiscoveryJob {
    pub fn new(
        engine: Arc<Engine>,
        crawler: Arc<Crawler>,
        hosts: Option<Arc<dyn HostStore>>,
        store: Store,
        feed: Arc<Feed>,
    ) -> Arc<DiscoveryJob> {
        let http = reqwest::Client::builder()
            .timeout(Duration::from_secs(60))
            .user_agent(concat!("vlrelay/", env!("CARGO_PKG_VERSION")))
            .build()
            .expect("http client");
        Arc::new(DiscoveryJob {
            engine,
            crawler,
            hosts,
            store,
            http,
            feed,
            state: Mutex::new(None),
            requested: Mutex::new(HashSet::new()),
            pace: Mutex::new(Pace { tokens: 0.0, at: Instant::now() }),
            running: AtomicBool::new(false),
            stopped: AtomicBool::new(false),
        })
    }

    pub fn stop(&self) {
        self.stopped.store(true, Relaxed);
    }

    /// A run of `source` (or every enabled source) now, rather than at its
    /// interval.
    pub fn request(&self, source: Option<String>) {
        self.requested.lock().insert(source);
    }

    fn policy(&self) -> Discovery {
        self.engine.snapshot().policy.body.discovery.clone()
    }

    async fn load(&self) -> anyhow::Result<State> {
        let p = crate::policy::store::path(&self.store, STATE);
        Ok(match crate::policy::store::get(&self.store, &p, None).await? {
            Some((b, _)) => serde_json::from_slice(&b)?,
            None => State::default(),
        })
    }

    async fn save(&self, st: &State) -> anyhow::Result<()> {
        let p = crate::policy::store::path(&self.store, STATE);
        crate::policy::store::put(&self.store, &p, serde_json::to_vec(st)?, object_store::PutMode::Overwrite).await?;
        Ok(())
    }

    /// The state as this term knows it, with the policy's sources in it.
    pub fn view(&self) -> crate::admin::DiscoveryView {
        let d = self.policy();
        let st = self.state.lock().clone().unwrap_or_default();
        let now = crate::policy::store::now_ms();
        let mut sources = Vec::new();
        for r in &d.seed_relays {
            let key = source_key(&r.url);
            let mut s = st.sources.get(&key).cloned().unwrap_or_default();
            // the policy's spelling of it, not the one a run last saved
            s.url = Some(r.url.clone());
            let next = if !r.enabled {
                None
            } else if s.in_progress {
                Some(now)
            } else {
                Some(s.last_finished_ms.map_or(now, |f| f + r.refresh_interval_secs as i64 * 1000))
            };
            sources.push(crate::admin::DiscoverySource {
                key,
                enabled: r.enabled,
                refresh_interval_secs: Some(r.refresh_interval_secs),
                next_run_ms: next,
                pending: 0,
                state: s,
            });
        }
        let s = st.sources.get(PLC_SOURCE).cloned().unwrap_or_default();
        sources.push(crate::admin::DiscoverySource {
            key: PLC_SOURCE.into(),
            enabled: d.plc,
            refresh_interval_secs: None,
            next_run_ms: None,
            pending: st.plc_pending.len() as u64,
            state: s,
        });
        crate::admin::DiscoveryView {
            leader: None,
            leading: self.running.load(Relaxed),
            connects_per_min: d.connects_per_min,
            requests_per_sec: d.requests_per_sec,
            sources,
        }
    }

    fn leads(q: &QNode, epoch: u64) -> bool {
        let st = q.status();
        st.role == Role::Leader && st.epoch == epoch
    }

    /// Runs for the node's life: a term of discovery for each term this
    /// node leads.
    pub async fn run(self: Arc<Self>, qnode: std::sync::Weak<QNode>) {
        let mut tick = tokio::time::interval(WATCH);
        loop {
            tick.tick().await;
            if self.stopped.load(Relaxed) {
                return;
            }
            let Some(q) = qnode.upgrade() else { return };
            let st = q.status();
            if st.role != Role::Leader {
                continue;
            }
            let epoch = st.epoch;
            drop(q);
            if let Err(e) = self.clone().term(&qnode, epoch).await {
                tracing::warn!(epoch, "discovery: the term's job stopped: {e:#}");
                tokio::time::sleep(Duration::from_secs(1)).await;
            }
            self.running.store(false, Relaxed);
            *self.state.lock() = None;
        }
    }

    async fn term(self: Arc<Self>, qnode: &std::sync::Weak<QNode>, epoch: u64) -> anyhow::Result<()> {
        let keep = || !self.stopped.load(Relaxed) && qnode.upgrade().is_some_and(|q| DiscoveryJob::leads(&q, epoch));
        let mut st = self.load().await?;
        for s in st.sources.values_mut() {
            if s.in_progress {
                s.resumed += 1;
            }
        }
        *self.state.lock() = Some(st.clone());
        self.running.store(true, Relaxed);
        tracing::info!(epoch, "discovery: this node leads; running the sources");
        let mut last_request: BTreeMap<String, Instant> = BTreeMap::new();
        while keep() {
            let d = self.policy();
            let now = crate::policy::store::now_ms();
            let asked: HashSet<Option<String>> = std::mem::take(&mut *self.requested.lock());
            for r in &d.seed_relays {
                let key = source_key(&r.url);
                let s = st.sources.entry(key.clone()).or_default();
                s.url = Some(r.url.clone());
                if asked.contains(&None) || asked.contains(&Some(key.clone())) {
                    s.run_requested = true;
                }
            }
            if asked.contains(&Some(PLC_SOURCE.to_string())) || asked.contains(&None) {
                st.sources.entry(PLC_SOURCE.into()).or_default().run_requested = true;
            }
            // the first due source gets a page
            let due = d.seed_relays.iter().filter(|r| r.enabled).find(|r| {
                let s = &st.sources[&source_key(&r.url)];
                s.in_progress
                    || s.run_requested
                    || s.last_finished_ms.is_none_or(|f| now >= f + r.refresh_interval_secs as i64 * 1000)
            });
            let mut worked = false;
            if let Some(r) = due {
                let key = source_key(&r.url);
                let wait = Duration::from_secs_f64(1.0 / d.requests_per_sec.max(0.01));
                if last_request.get(&key).is_none_or(|t| t.elapsed() >= wait) {
                    last_request.insert(key.clone(), Instant::now());
                    self.page(&mut st, &key, &r.url, &d).await;
                    worked = true;
                }
            }
            if d.plc {
                let fed = self.feed.take();
                for h in fed {
                    if st.plc_pending.len() < PLC_PENDING_MAX && !self.crawler.knows(&h) {
                        st.plc_pending.insert(h);
                    }
                }
                let s = st.sources.entry(PLC_SOURCE.into()).or_default();
                s.run_requested = false;
                if !st.plc_pending.is_empty() {
                    let batch: Vec<String> = st.plc_pending.iter().take(PROBES * 4).cloned().collect();
                    for h in &batch {
                        st.plc_pending.remove(h);
                    }
                    let s = st.sources.get_mut(PLC_SOURCE).expect("just made");
                    s.last_started_ms.get_or_insert(now);
                    self.admit_all(s, &batch, PLC_SOURCE, &d).await;
                    s.last_finished_ms = Some(crate::policy::store::now_ms());
                    worked = true;
                }
            } else {
                // a disabled source forgets what it was fed
                self.feed.take();
            }
            if worked {
                *self.state.lock() = Some(st.clone());
                if let Err(e) = self.save(&st).await {
                    tracing::warn!("discovery: saving its state: {e:#}");
                }
            } else {
                tokio::time::sleep(Duration::from_millis(200)).await;
            }
        }
        tracing::info!(epoch, "discovery: the term ended");
        Ok(())
    }

    /// One page of `key`'s list, and its hosts through admission.
    async fn page(&self, st: &mut State, key: &str, url: &str, d: &Discovery) {
        let s = st.sources.get_mut(key).expect("made before");
        if !s.in_progress {
            *s = SourceState {
                url: s.url.clone(),
                runs: s.runs + 1,
                last_finished_ms: s.last_finished_ms,
                last_started_ms: Some(crate::policy::store::now_ms()),
                in_progress: true,
                resumed: 0,
                ..SourceState::default()
            };
        }
        s.run_requested = false;
        match list_hosts(&self.http, url, s.cursor.as_deref()).await {
            Ok(Page::Later(after)) => {
                s.throttled += 1;
                tracing::info!(source = key, ?after, "discovery: the relay asks to wait");
                tokio::time::sleep(after).await;
            }
            Ok(Page::Hosts { hosts, cursor }) => {
                s.pages += 1;
                s.hosts_seen += hosts.len() as u64;
                s.last_error = None;
                let names: Vec<String> = hosts.iter().map(|h| h.hostname.clone()).collect();
                self.admit_all(s, &names, key, d).await;
                s.seeded += self.seed(&hosts, key, d).await;
                let done = hosts.is_empty() || cursor.is_none() || cursor == s.cursor;
                if done {
                    s.cursor = None;
                    s.in_progress = false;
                    s.last_finished_ms = Some(crate::policy::store::now_ms());
                    tracing::info!(
                        source = key,
                        seen = s.hosts_seen,
                        new = s.new,
                        admitted = s.admitted,
                        "discovery: a run finished"
                    );
                } else {
                    s.cursor = cursor;
                }
            }
            Err(e) => {
                s.errors += 1;
                s.last_error = Some(format!("{e:#}"));
                tracing::warn!(source = key, "discovery: listHosts failed: {e:#}");
                tokio::time::sleep(Duration::from_secs(2)).await;
            }
        }
    }

    /// Keeps each listed host's reported account count on its record. A
    /// host admission refused has no record and isn't seeded.
    async fn seed(&self, listed: &[Listed], from: &str, d: &Discovery) -> u64 {
        let Some(store) = &self.hosts else { return 0 };
        let sa = &d.seed_accounts;
        let mut wrote = 0;
        for l in listed.iter().filter(|l| l.accounts > 0 && sa.enabled) {
            let Some(host) = self.crawler.normalize(&l.hostname) else { continue };
            let now = crate::state::now_secs();
            let (n, from) = (l.accounts, from.to_string());
            let sa = sa.clone();
            let r = store
                .update_host(
                    &host.0,
                    Box::new(move |rec| {
                        let mut rec = rec?;
                        if matches!(rec.tier, Tier::Trusted | Tier::Suspended | Tier::Banned) {
                            return None;
                        }
                        let mut hp = tiers::host_policy(&rec);
                        hp.seeded = Some(tiers::seed_update(hp.seeded.as_ref(), n, &from, &sa, now)?);
                        tiers::set_host_policy(&mut rec, &hp);
                        Some(rec)
                    }),
                )
                .await;
            match r {
                Ok(Some(_)) => wrote += 1,
                Ok(None) => {}
                Err(e) => tracing::warn!(host = %host.0, "discovery: seeding its account count: {e:#}"),
            }
        }
        wrote
    }

    async fn take_connect(&self, per_min: f64) {
        loop {
            let wait = {
                let mut p = self.pace.lock();
                let rate = per_min.max(0.1) / 60.0;
                let now = Instant::now();
                p.tokens = (p.tokens + now.duration_since(p.at).as_secs_f64() * rate).min(per_min.max(1.0));
                p.at = now;
                if p.tokens >= 1.0 {
                    p.tokens -= 1.0;
                    return;
                }
                Duration::from_secs_f64((1.0 - p.tokens) / rate)
            };
            tokio::time::sleep(wait.min(Duration::from_secs(5))).await;
        }
    }

    async fn admit_all(&self, s: &mut SourceState, hosts: &[String], source: &str, d: &Discovery) {
        let mut fresh = Vec::new();
        for h in hosts {
            if self.crawler.knows(h) {
                s.known += 1;
            } else {
                fresh.push(h.clone());
            }
        }
        s.new += fresh.len() as u64;
        let results: Vec<Result<bool, CrawlError>> = futures::stream::iter(fresh)
            .map(|h| async move {
                self.take_connect(d.connects_per_min).await;
                self.crawler.admit_from(&h, source).await
            })
            .buffer_unordered(PROBES)
            .collect()
            .await;
        for r in results {
            match r {
                Ok(_) => s.admitted += 1,
                Err(_) => s.refused += 1,
            }
        }
    }
}

#[cfg(test)]
mod tests;
