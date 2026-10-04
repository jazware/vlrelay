//! DID document cache: per DID, the parsed `#atproto` signing key and the
//! `#atproto_pds` endpoint.
//!
//! Lookups are single-flighted (concurrent callers for one DID share one
//! fetch), spend from a global lookups/s budget (PLC is shared, and did:web
//! hosts are anyone's), and failures are cached briefly so a burst of events
//! from a broken DID costs one fetch. `#identity` events force a refresh, at
//! most one per DID per `min_refresh`: any host can send `#identity` for any
//! DID, and each forced fetch spends the shared budget.
//! This is a bounded hot cache; the durable copy of an account's key and host
//! lives in the DID owner's state. A miss asks the [`Seeder`] (the state
//! record or the PLC export's entry, `crate::plc_seed`) before it fetches.

use crate::types::Host;
use crate::verify::SigningKey;
use parking_lot::Mutex;
use serde_json::Value as J;
use std::collections::HashMap;
use std::future::Future;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{Duration, Instant};
use tokio::sync::OnceCell;

#[derive(Clone, Debug)]
pub struct Identity {
    pub did: String,
    /// None: the document has no usable `#atproto` key.
    pub signing_key: Option<SigningKey>,
    pub signing_key_multibase: Option<String>,
    /// The `#atproto_pds` service endpoint as written.
    pub pds: Option<String>,
    pub pds_host: Option<Host>,
    pub handle: Option<String>,
}

impl Identity {
    pub fn from_doc(did: &str, doc: &J) -> Result<Identity, LookupError> {
        if doc.get("id").and_then(J::as_str) != Some(did) {
            return Err(LookupError::Failed("document id does not match DID".into()));
        }
        let mb = vlpds::did_resolver::signing_key_multibase(doc);
        let pds = vlpds::did_resolver::service_endpoint(doc, "atproto_pds");
        let handle = doc
            .get("alsoKnownAs")
            .and_then(J::as_array)
            .and_then(|a| a.iter().filter_map(J::as_str).find_map(|s| s.strip_prefix("at://")))
            .map(String::from);
        Ok(Identity {
            did: did.to_string(),
            signing_key: mb.as_deref().and_then(|m| SigningKey::from_multibase(m).ok()),
            signing_key_multibase: mb,
            pds_host: pds.as_deref().and_then(normalize_host),
            pds,
            handle,
        })
    }

    pub fn authorized(&self, host: &Host) -> bool {
        self.pds_host.as_ref().is_some_and(|h| h == host)
    }
}

/// A PDS URL or bare hostname as a [`Host`]: lowercase, no scheme, no
/// trailing dot, and the port only when it isn't the scheme's default.
pub fn normalize_host(s: &str) -> Option<Host> {
    let s = s.trim();
    let url = if s.contains("://") { reqwest::Url::parse(s) } else { reqwest::Url::parse(&format!("https://{s}")) };
    let url = url.ok()?;
    if !matches!(url.scheme(), "http" | "https") {
        return None;
    }
    let host = url.host_str()?.trim_end_matches('.').to_ascii_lowercase();
    if host.is_empty() {
        return None;
    }
    // Url::port() is None for the scheme's default port
    Some(Host(match url.port() {
        Some(p) => format!("{host}:{p}"),
        None => host,
    }))
}

#[derive(Clone, Debug, PartialEq, Eq, thiserror::Error)]
pub enum LookupError {
    #[error("unsupported or malformed DID")]
    BadDid,
    #[error("DID not found")]
    NotFound,
    #[error("lookup failed: {0}")]
    Failed(String),
    /// The lookup budget is spent; retry later. Not cached.
    #[error("DID lookup budget exhausted")]
    OverBudget,
}

impl LookupError {
    pub fn reason(&self) -> &'static str {
        match self {
            LookupError::BadDid => "bad_did",
            LookupError::NotFound => "not_found",
            LookupError::Failed(_) => "failed",
            LookupError::OverBudget => "over_budget",
        }
    }
}

/// Where documents come from; [`HttpFetch`] in production, a map in tests.
pub trait Fetch: Send + Sync + 'static {
    fn fetch(&self, did: &str) -> impl Future<Output = Result<J, LookupError>> + Send;
}

const MAX_DOC_BYTES: usize = 256 << 10;
const FETCH_TIMEOUT: Duration = Duration::from_secs(5);

/// did:plc from the configured directory, did:web (hostname-level only) over
/// vlpds's SSRF-guarded client.
pub struct HttpFetch {
    plc_url: String,
    allow_insecure: bool,
}

impl HttpFetch {
    pub fn new(plc_url: &str, allow_insecure: bool) -> HttpFetch {
        HttpFetch { plc_url: plc_url.trim_end_matches('/').to_string(), allow_insecure }
    }

    fn request(&self, did: &str) -> Result<reqwest::RequestBuilder, LookupError> {
        if let Some(id) = did.strip_prefix("did:plc:") {
            // alphanumeric only, so nothing can change the URL's path
            if id.is_empty() || !id.bytes().all(|b| b.is_ascii_alphanumeric()) {
                return Err(LookupError::BadDid);
            }
            return Ok(vlpds::http::public().get(format!("{}/{did}", self.plc_url)));
        }
        let rest = did.strip_prefix("did:web:").ok_or(LookupError::BadDid)?;
        if rest.is_empty() || rest.contains(':') || rest.contains('/') {
            return Err(LookupError::BadDid);
        }
        let host = rest.replace("%3A", ":").replace("%3a", ":");
        if !host.bytes().all(|b| b.is_ascii_alphanumeric() || b == b'.' || b == b'-' || b == b':') {
            return Err(LookupError::BadDid);
        }
        let plain = host.split(':').next() == Some("localhost");
        let url = format!("{}://{host}/.well-known/did.json", if plain { "http" } else { "https" });
        vlpds::http::guarded(self.allow_insecure).get(&url).map_err(LookupError::Failed)
    }
}

impl Fetch for HttpFetch {
    async fn fetch(&self, did: &str) -> Result<J, LookupError> {
        let req = self.request(did)?;
        let fut = async {
            use futures::StreamExt;
            let resp = req
                .header("accept", "application/did+ld+json, application/json")
                .send()
                .await
                .map_err(|e| LookupError::Failed(e.to_string()))?;
            match resp.status() {
                s if s == reqwest::StatusCode::NOT_FOUND || s == reqwest::StatusCode::GONE => {
                    return Err(LookupError::NotFound);
                }
                s if !s.is_success() => return Err(LookupError::Failed(format!("status {s}"))),
                _ => {}
            }
            let mut buf = Vec::new();
            let mut body = resp.bytes_stream();
            while let Some(chunk) = body.next().await {
                let chunk = chunk.map_err(|e| LookupError::Failed(e.to_string()))?;
                if buf.len() + chunk.len() > MAX_DOC_BYTES {
                    return Err(LookupError::Failed("document too large".into()));
                }
                buf.extend_from_slice(&chunk);
            }
            serde_json::from_slice(&buf).map_err(|e| LookupError::Failed(format!("invalid JSON: {e}")))
        };
        tokio::time::timeout(FETCH_TIMEOUT, fut).await.map_err(|_| LookupError::Failed("timed out".into()))?
    }
}

#[derive(Clone, Debug)]
pub struct Options {
    pub ttl: Duration,
    pub negative_ttl: Duration,
    pub capacity: usize,
    /// Fetches per second, all DIDs together, with this much burst.
    pub lookups_per_sec: f64,
    pub burst: f64,
    /// A lookup waits at most this long for budget, else [`LookupError::OverBudget`].
    pub max_budget_wait: Duration,
    /// How often a store also drops expired entries, so the cache holds the
    /// DIDs seen within a TTL rather than every DID up to `capacity`.
    pub sweep_every: Duration,
    /// A forced refresh within this long of the DID's last one returns that
    /// one's result. 30 s is the state step's re-resolve interval for a
    /// host mismatch, so it opens no window that path doesn't have already.
    pub min_refresh: Duration,
}

impl Default for Options {
    fn default() -> Self {
        Options {
            ttl: Duration::from_secs(3600),
            negative_ttl: Duration::from_secs(60),
            capacity: 1 << 20,
            lookups_per_sec: 50.0,
            burst: 100.0,
            max_budget_wait: Duration::from_secs(2),
            sweep_every: Duration::from_secs(60),
            min_refresh: Duration::from_secs(30),
        }
    }
}

type Outcome = Result<Arc<Identity>, LookupError>;

/// How long [`IdentityCache::lookup_paced`] sleeps before asking a spent
/// budget again, on top of the `max_budget_wait` the lookup already waited.
const OVER_BUDGET_RETRY: Duration = Duration::from_millis(50);
type Flight = Arc<OnceCell<Outcome>>;

struct Entry {
    at: Instant,
    v: Outcome,
    /// When a forced refresh last fetched this.
    forced_at: Option<Instant>,
}

struct Bucket {
    tokens: f64,
    at: Instant,
}

#[derive(Default, Debug)]
pub struct Stats {
    pub hits: AtomicU64,
    pub negative_hits: AtomicU64,
    pub fetches: AtomicU64,
    pub joined: AtomicU64,
    pub over_budget: AtomicU64,
    /// Forced refreshes answered by one made within `min_refresh`.
    pub refresh_coalesced: AtomicU64,
    /// Misses the seeder filled, with no fetch.
    pub seeded: AtomicU64,
}

pub struct IdentityCache<F: Fetch = HttpFetch> {
    fetcher: F,
    opts: Options,
    entries: Mutex<HashMap<String, Entry>>,
    swept: Mutex<Instant>,
    /// DID -> (when the fetch began, its shared result).
    inflight: Mutex<HashMap<String, (Instant, Flight)>>,
    budget: Mutex<Bucket>,
    /// The cluster's share of PLC lookups for this node, on top of `budget`.
    gate: parking_lot::RwLock<Option<BudgetGate>>,
    seeder: parking_lot::RwLock<Option<Arc<dyn Seeder>>>,
    pub stats: Stats,
}

/// Takes one lookup from an outside budget; false when it's spent.
pub type BudgetGate = Arc<dyn Fn() -> bool + Send + Sync>;

/// A document the relay already holds, offered on a cache miss in place of
/// a fetch. None: fetch it. Not asked on a forced refresh.
pub trait Seeder: Send + Sync {
    fn seed<'a>(&'a self, did: &'a str) -> futures::future::BoxFuture<'a, Option<Identity>>;
}

impl<F: Fetch> IdentityCache<F> {
    pub fn new(fetcher: F, opts: Options) -> IdentityCache<F> {
        IdentityCache {
            fetcher,
            budget: Mutex::new(Bucket { tokens: opts.burst, at: Instant::now() }),
            opts,
            entries: Default::default(),
            swept: Mutex::new(Instant::now()),
            inflight: Default::default(),
            gate: Default::default(),
            seeder: Default::default(),
            stats: Stats::default(),
        }
    }

    pub fn set_budget_gate(&self, gate: BudgetGate) {
        *self.gate.write() = Some(gate);
    }

    pub fn set_seeder(&self, s: Arc<dyn Seeder>) {
        *self.seeder.write() = Some(s);
    }

    /// Cached documents whose handle is `q`, or starts with it when `q`
    /// ends in `*`. A scan: for the operator API, not the hot path.
    pub fn find_handle(&self, q: &str, limit: usize) -> Vec<Arc<Identity>> {
        let q = q.trim().trim_start_matches('@').to_ascii_lowercase();
        let (prefix, q) = match q.strip_suffix('*') {
            Some(p) => (true, p.to_string()),
            None => (false, q),
        };
        if q.is_empty() {
            return Vec::new();
        }
        let e = self.entries.lock();
        let mut out: Vec<Arc<Identity>> = e
            .values()
            .filter_map(|x| x.v.as_ref().ok())
            .filter(|id| {
                id.handle.as_deref().is_some_and(|h| {
                    let h = h.to_ascii_lowercase();
                    if prefix { h.starts_with(&q) } else { h == q }
                })
            })
            .take(limit)
            .cloned()
            .collect();
        out.sort_by(|a, b| a.handle.cmp(&b.handle));
        out
    }

    /// The cached outcome, if fresh. No I/O.
    pub fn cached(&self, did: &str) -> Option<Outcome> {
        let e = self.entries.lock();
        let e = e.get(did)?;
        let ttl = if e.v.is_ok() { self.opts.ttl } else { self.opts.negative_ttl };
        (e.at.elapsed() < ttl).then(|| e.v.clone())
    }

    pub async fn resolve(&self, did: &str) -> Outcome {
        if let Some(v) = self.cached(did) {
            let c = if v.is_ok() { &self.stats.hits } else { &self.stats.negative_hits };
            c.fetch_add(1, Ordering::Relaxed);
            return v;
        }
        self.lookup(did, false).await
    }

    /// A fetch that starts after this call (on `#identity`, or after a
    /// signature fails against the cached key).
    pub async fn refresh(&self, did: &str) -> Outcome {
        self.lookup(did, true).await
    }

    /// [`Self::resolve`] (or [`Self::refresh`] when `fresh`), waiting out a
    /// spent lookup budget instead of failing. The budget paces a cold
    /// start's millions of unknown DIDs: an event that waits holds up its
    /// lane and, through it, its host's socket, where failing would drop it.
    pub async fn lookup_paced(&self, did: &str, fresh: bool) -> Outcome {
        loop {
            let r = if fresh { self.refresh(did).await } else { self.resolve(did).await };
            match r {
                Err(LookupError::OverBudget) => tokio::time::sleep(OVER_BUDGET_RETRY).await,
                r => return r,
            }
        }
    }

    pub fn invalidate(&self, did: &str) {
        self.entries.lock().remove(did);
    }

    /// Seeds the cache, e.g. from the DID owner's stored state.
    pub fn insert(&self, id: Identity) {
        let did = id.did.clone();
        self.store(&did, Ok(Arc::new(id)), false);
    }

    /// The outcome of a forced refresh made within `min_refresh`.
    fn recently_forced(&self, did: &str) -> Option<Outcome> {
        let e = self.entries.lock();
        let e = e.get(did)?;
        e.forced_at.is_some_and(|t| t.elapsed() < self.opts.min_refresh).then(|| e.v.clone())
    }

    /// Whether `host` is the DID's current PDS per the cached document.
    /// False when nothing is cached: call [`Self::check_host`] to resolve.
    pub fn authorized(&self, did: &str, host: &Host) -> bool {
        matches!(self.cached(did), Some(Ok(id)) if id.authorized(host))
    }

    /// [`Self::authorized`], resolving if needed and re-resolving once when
    /// the cached document names another host (the account may have just
    /// migrated).
    pub async fn check_host(&self, did: &str, host: &Host) -> Result<bool, LookupError> {
        let id = self.resolve(did).await?;
        if id.authorized(host) {
            return Ok(true);
        }
        Ok(self.refresh(did).await?.authorized(host))
    }

    async fn lookup(&self, did: &str, force: bool) -> Outcome {
        if force && let Some(v) = self.recently_forced(did) {
            self.stats.refresh_coalesced.fetch_add(1, Ordering::Relaxed);
            return v;
        }
        let asked = Instant::now();
        let cell = {
            let mut m = self.inflight.lock();
            match m.get(did) {
                // a forced refresh can't share a fetch that began before it
                Some((started, c)) if !force || *started >= asked => {
                    self.stats.joined.fetch_add(1, Ordering::Relaxed);
                    c.clone()
                }
                _ => {
                    let c = Arc::new(OnceCell::new());
                    m.insert(did.to_string(), (asked, c.clone()));
                    c
                }
            }
        };
        let out = cell
            .get_or_init(|| async {
                let seeder = if force { None } else { self.seeder.read().clone() };
                if let Some(s) = seeder
                    && let Some(id) = s.seed(did).await
                {
                    self.stats.seeded.fetch_add(1, Ordering::Relaxed);
                    let r: Outcome = Ok(Arc::new(id));
                    self.store(did, r.clone(), false);
                    return r;
                }
                let r = self.fetch_now(did).await;
                if !matches!(r, Err(LookupError::OverBudget)) {
                    self.store(did, r.clone(), force);
                }
                r
            })
            .await
            .clone();
        let mut m = self.inflight.lock();
        if m.get(did).is_some_and(|(_, c)| Arc::ptr_eq(c, &cell)) {
            m.remove(did);
        }
        out
    }

    async fn fetch_now(&self, did: &str) -> Outcome {
        if !vlpds::xrpc::syntax::valid_did(did) {
            return Err(LookupError::BadDid);
        }
        if let Err(e) = self.spend_budget().await {
            self.stats.over_budget.fetch_add(1, Ordering::Relaxed);
            return Err(e);
        }
        let gate = self.gate.read().clone();
        if let Some(g) = gate {
            let t0 = Instant::now();
            while !g() {
                if t0.elapsed() >= self.opts.max_budget_wait {
                    self.stats.over_budget.fetch_add(1, Ordering::Relaxed);
                    return Err(LookupError::OverBudget);
                }
                tokio::time::sleep(Duration::from_millis(20)).await;
            }
        }
        self.stats.fetches.fetch_add(1, Ordering::Relaxed);
        let doc = self.fetcher.fetch(did).await?;
        Ok(Arc::new(Identity::from_doc(did, &doc)?))
    }

    /// Takes a token, waiting for one up to `max_budget_wait`. Waiters
    /// reserve their token up front (the balance goes negative), so they
    /// leave in order and the rate holds.
    async fn spend_budget(&self) -> Result<(), LookupError> {
        let wait = {
            let mut b = self.budget.lock();
            let now = Instant::now();
            b.tokens =
                (b.tokens + now.duration_since(b.at).as_secs_f64() * self.opts.lookups_per_sec).min(self.opts.burst);
            b.at = now;
            let wait = if b.tokens >= 1.0 {
                Duration::ZERO
            } else {
                Duration::from_secs_f64((1.0 - b.tokens) / self.opts.lookups_per_sec)
            };
            if wait > self.opts.max_budget_wait {
                return Err(LookupError::OverBudget);
            }
            b.tokens -= 1.0;
            wait
        };
        if !wait.is_zero() {
            tokio::time::sleep(wait).await;
        }
        Ok(())
    }

    fn store(&self, did: &str, v: Outcome, forced: bool) {
        let due = {
            let mut s = self.swept.lock();
            let due = s.elapsed() >= self.opts.sweep_every;
            if due {
                *s = Instant::now();
            }
            due
        };
        let mut m = self.entries.lock();
        let full = m.len() >= self.opts.capacity && !m.contains_key(did);
        if due || full {
            Self::drop_expired(&mut m, &self.opts);
        }
        if full && m.len() >= self.opts.capacity {
            // arbitrary eighth: HashMap order is random per process
            let drop: Vec<String> = m.keys().take(self.opts.capacity / 8 + 1).cloned().collect();
            for k in drop {
                m.remove(&k);
            }
        }
        let now = Instant::now();
        let forced_at = if forced { Some(now) } else { m.get(did).and_then(|e| e.forced_at) };
        m.insert(did.to_string(), Entry { at: now, v, forced_at });
    }

    fn drop_expired(m: &mut HashMap<String, Entry>, o: &Options) {
        m.retain(|_, e| e.at.elapsed() < if e.v.is_ok() { o.ttl } else { o.negative_ttl });
    }

    pub fn len(&self) -> usize {
        self.entries.lock().len()
    }

    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }
}

#[cfg(test)]
mod tests;
