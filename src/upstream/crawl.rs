//! `com.atproto.sync.requestCrawl`: a PDS asks to be subscribed to.
//!
//! A new host has to answer `describeServer` and accept a `subscribeRepos`
//! socket before it's admitted, and then it starts at tier `new`. Admissions
//! spend a per-hour budget so a spammer can't register thousands of hosts in
//! one go; hosts on the allow list (exact name or an allow domain rule) skip
//! the budget. Domain ban rules refuse a whole suffix at once.

use super::host::{HostnameError, Tier, normalize_hostname};
use super::{Manager, client};
use crate::types::Host;
use axum::extract::State;
use axum::http::StatusCode;
use axum::response::{IntoResponse, Response};
use axum::routing::post;
use axum::{Json, Router};
use parking_lot::Mutex;
use serde::{Deserialize, Serialize};
use std::collections::{HashSet, VecDeque};
use std::sync::Arc;
use std::time::{Duration, Instant};

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum DomainAction {
    Allow,
    Ban,
}

/// Matches the domain itself and everything under it.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct DomainRule {
    pub suffix: String,
    pub action: DomainAction,
}

impl DomainRule {
    fn matches(&self, host: &str) -> bool {
        let name = host.rsplit_once(':').map_or(host, |(n, _)| n);
        let s = self.suffix.trim_start_matches('.');
        name == s || name.strip_suffix(s).is_some_and(|p| p.ends_with('.'))
    }
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(default)]
pub struct CrawlPolicy {
    /// Exact hostnames admitted without spending the budget, whatever the
    /// domain rules say.
    pub allow: Vec<String>,
    /// The longest matching suffix decides.
    pub rules: Vec<DomainRule>,
    /// Only allow-listed hosts and allow-rule domains may join.
    pub allow_only: bool,
    pub new_hosts_per_hour: u32,
    pub probe_timeout_secs: u64,
}

impl Default for CrawlPolicy {
    fn default() -> CrawlPolicy {
        CrawlPolicy {
            allow: Vec::new(),
            rules: Vec::new(),
            allow_only: false,
            new_hosts_per_hour: 50,
            probe_timeout_secs: 10,
        }
    }
}

#[derive(Debug, PartialEq, Eq)]
enum Verdict {
    Banned,
    Allowed,
    Unlisted,
}

impl CrawlPolicy {
    fn verdict(&self, host: &Host) -> Verdict {
        if self.allow.iter().any(|a| a.eq_ignore_ascii_case(&host.0)) {
            return Verdict::Allowed;
        }
        let rule = self
            .rules
            .iter()
            .filter(|r| r.matches(&host.0))
            .max_by_key(|r| (r.suffix.trim_start_matches('.').len(), r.action == DomainAction::Ban));
        match rule.map(|r| r.action) {
            Some(DomainAction::Ban) => Verdict::Banned,
            Some(DomainAction::Allow) => Verdict::Allowed,
            None => Verdict::Unlisted,
        }
    }
}

/// requestCrawl admission by the policy engine, in place of the
/// [`CrawlPolicy`] rules and hourly budget: crawl switch, bans, allow-list
/// mode, the starting tier and the cluster's daily new-host budget.
#[async_trait::async_trait]
pub trait Admission: Send + Sync + 'static {
    /// The tier to start the host at (ignored for a known host).
    async fn admit(&self, host: &Host) -> Result<Tier, CrawlError>;
}

pub struct Crawler {
    admission: parking_lot::RwLock<Option<Arc<dyn Admission>>>,
    manager: Arc<Manager>,
    policy: parking_lot::RwLock<CrawlPolicy>,
    /// Admission times in the last hour, plus reservations for probes in flight.
    admitted: Mutex<VecDeque<Instant>>,
    probing: Mutex<HashSet<Host>>,
}

#[derive(Debug, PartialEq, Eq)]
pub enum CrawlError {
    InvalidHost(HostnameError),
    HostBanned,
    NotAllowed,
    Budget,
    Busy,
    Unreachable(String),
    Internal(String),
    /// Refused by the policy, with a message fit for the caller.
    Refused(String),
}

impl IntoResponse for CrawlError {
    fn into_response(self) -> Response {
        let (status, error, message) = match self {
            CrawlError::InvalidHost(e) => (StatusCode::BAD_REQUEST, "InvalidRequest", e.to_string()),
            CrawlError::HostBanned => (StatusCode::BAD_REQUEST, "HostBanned", "host is banned".into()),
            CrawlError::NotAllowed => {
                (StatusCode::FORBIDDEN, "HostNotAllowed", "this relay only crawls allow-listed hosts".into())
            }
            CrawlError::Budget => {
                (StatusCode::TOO_MANY_REQUESTS, "RateLimitExceeded", "new-host budget is spent; try later".into())
            }
            CrawlError::Busy => {
                (StatusCode::TOO_MANY_REQUESTS, "RateLimitExceeded", "a crawl of this host is in progress".into())
            }
            CrawlError::Unreachable(m) => {
                (StatusCode::BAD_REQUEST, "InvalidRequest", format!("host check failed: {m}"))
            }
            CrawlError::Internal(m) => (StatusCode::INTERNAL_SERVER_ERROR, "InternalServerError", m),
            CrawlError::Refused(m) => (StatusCode::BAD_REQUEST, "InvalidRequest", m),
        };
        (status, Json(serde_json::json!({"error": error, "message": message}))).into_response()
    }
}

#[derive(Deserialize)]
struct CrawlBody {
    hostname: String,
}

const HOUR: Duration = Duration::from_secs(3600);

impl Crawler {
    pub fn new(manager: Arc<Manager>, policy: CrawlPolicy) -> Arc<Crawler> {
        Arc::new(Crawler {
            admission: parking_lot::RwLock::new(None),
            manager,
            policy: parking_lot::RwLock::new(policy),
            admitted: Mutex::new(VecDeque::new()),
            probing: Mutex::new(HashSet::new()),
        })
    }

    pub fn set_admission(&self, a: Arc<dyn Admission>) {
        *self.admission.write() = Some(a);
    }

    pub fn set_policy(&self, p: CrawlPolicy) {
        *self.policy.write() = p;
    }

    pub fn policy(&self) -> CrawlPolicy {
        self.policy.read().clone()
    }

    /// The `requestCrawl` route, for the lead to merge into the public router.
    pub fn router(self: &Arc<Self>) -> Router {
        Router::new().route("/xrpc/com.atproto.sync.requestCrawl", post(handle)).with_state(self.clone())
    }

    /// Validates, checks policy and budget, probes, admits. Returns whether
    /// the host was new.
    pub async fn request_crawl(&self, hostname: &str) -> Result<bool, CrawlError> {
        let dev = self.manager.config().dev_mode;
        let host = normalize_hostname(hostname, dev).map_err(CrawlError::InvalidHost)?;
        let admission = self.admission.read().clone();
        if let Some(a) = admission {
            return self.request_crawl_with(&host, a.as_ref()).await;
        }
        let policy = self.policy();
        let verdict = policy.verdict(&host);
        if verdict == Verdict::Banned {
            return Err(CrawlError::HostBanned);
        }
        if let Some(e) = self.manager.registry().get(&host) {
            return match e.tier() {
                Tier::Banned => Err(CrawlError::HostBanned),
                // an operator's suspension isn't lifted by asking
                Tier::Suspended => Err(CrawlError::HostBanned),
                _ => {
                    self.manager.wake(&host);
                    Ok(false)
                }
            };
        }
        if policy.allow_only && verdict != Verdict::Allowed {
            return Err(CrawlError::NotAllowed);
        }
        if !self.probing.lock().insert(host.clone()) {
            return Err(CrawlError::Busy);
        }
        let _probing = Unmark(&self.probing, host.clone());
        let reserved = verdict != Verdict::Allowed;
        let slot = if reserved { Some(self.reserve(policy.new_hosts_per_hour)?) } else { None };
        let probe = self.probe(&host, Duration::from_secs(policy.probe_timeout_secs.max(1))).await;
        if let Err(e) = probe {
            if let Some(s) = slot {
                self.release(s);
            }
            return Err(e);
        }
        self.manager.admit(&host, Tier::New).await.map_err(|e| CrawlError::Internal(format!("{e:#}")))
    }

    async fn request_crawl_with(&self, host: &Host, a: &dyn Admission) -> Result<bool, CrawlError> {
        let probe_timeout = Duration::from_secs(self.policy().probe_timeout_secs.max(1));
        if self.manager.registry().get(host).is_some() {
            // a known host is still checked: a ban added since must hold
            a.admit(host).await?;
            self.manager.wake(host);
            return Ok(false);
        }
        if !self.probing.lock().insert(host.clone()) {
            return Err(CrawlError::Busy);
        }
        let _probing = Unmark(&self.probing, host.clone());
        let tier = a.admit(host).await?;
        self.probe(host, probe_timeout).await?;
        self.manager.admit(host, tier).await.map_err(|e| CrawlError::Internal(format!("{e:#}")))
    }

    fn reserve(&self, per_hour: u32) -> Result<Instant, CrawlError> {
        let now = Instant::now();
        let mut q = self.admitted.lock();
        while q.front().is_some_and(|t| now.duration_since(*t) >= HOUR) {
            q.pop_front();
        }
        if q.len() >= per_hour as usize {
            return Err(CrawlError::Budget);
        }
        q.push_back(now);
        Ok(now)
    }

    fn release(&self, slot: Instant) {
        let mut q = self.admitted.lock();
        if let Some(i) = q.iter().rposition(|t| *t == slot) {
            q.remove(i);
        }
    }

    async fn probe(&self, host: &Host, timeout: Duration) -> Result<(), CrawlError> {
        let cfg = self.manager.config();
        let base = (cfg.endpoint)(host);
        let url = format!("{}/xrpc/com.atproto.server.describeServer", base.trim_end_matches('/'));
        let req = vlpds::http::guarded(cfg.dev_mode).get(&url).map_err(CrawlError::Unreachable)?;
        let resp =
            req.timeout(timeout).send().await.map_err(|e| CrawlError::Unreachable(format!("describeServer: {e}")))?;
        if !resp.status().is_success() {
            return Err(CrawlError::Unreachable(format!("describeServer: HTTP {}", resp.status())));
        }
        let body = resp.bytes().await.map_err(|e| CrawlError::Unreachable(format!("describeServer: {e}")))?;
        let j: serde_json::Value = serde_json::from_slice(&body[..body.len().min(64 * 1024)])
            .map_err(|_| CrawlError::Unreachable("describeServer: not JSON".into()))?;
        if !j.get("did").and_then(|d| d.as_str()).is_some_and(|d| d.starts_with("did:")) {
            return Err(CrawlError::Unreachable("describeServer: no service DID".into()));
        }
        let mut ws = tokio::time::timeout(timeout, client::connect(cfg, host, None))
            .await
            .map_err(|_| CrawlError::Unreachable("subscribeRepos: timed out".into()))?
            .map_err(|e| CrawlError::Unreachable(format!("subscribeRepos: {e:#}")))?;
        let _ = tokio::time::timeout(Duration::from_secs(1), ws.close(None)).await;
        Ok(())
    }
}

struct Unmark<'a>(&'a Mutex<HashSet<Host>>, Host);

impl Drop for Unmark<'_> {
    fn drop(&mut self) {
        self.0.lock().remove(&self.1);
    }
}

async fn handle(State(c): State<Arc<Crawler>>, body: Option<Json<CrawlBody>>) -> Response {
    let Some(Json(body)) = body else {
        return (
            StatusCode::BAD_REQUEST,
            Json(serde_json::json!({"error": "InvalidRequest", "message": "body must be {\"hostname\": ...}"})),
        )
            .into_response();
    };
    match c.request_crawl(&body.hostname).await {
        Ok(new) => {
            tracing::info!(hostname = %body.hostname, new, "requestCrawl");
            Json(serde_json::json!({})).into_response()
        }
        Err(e) => {
            tracing::info!(hostname = %body.hostname, error = ?e, "requestCrawl refused");
            e.into_response()
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn domain_rules() {
        let p = CrawlPolicy {
            allow: vec!["good.spam.example".into()],
            rules: vec![
                DomainRule { suffix: "spam.example".into(), action: DomainAction::Ban },
                DomainRule { suffix: "ok.spam.example".into(), action: DomainAction::Allow },
                DomainRule { suffix: ".bsky.network".into(), action: DomainAction::Allow },
            ],
            ..Default::default()
        };
        let v = |h: &str| p.verdict(&Host(h.into()));
        assert_eq!(v("spam.example"), Verdict::Banned);
        assert_eq!(v("a.b.spam.example"), Verdict::Banned);
        // the exact allow list outranks any domain rule
        assert_eq!(v("good.spam.example"), Verdict::Allowed);
        // a longer allow rule outranks the shorter ban
        assert_eq!(v("pds.ok.spam.example"), Verdict::Allowed);
        assert_eq!(v("notspam.example"), Verdict::Unlisted);
        assert_eq!(v("morel.us-east.host.bsky.network"), Verdict::Allowed);
        assert_eq!(v("bsky.network.evil.com"), Verdict::Unlisted);
    }
}
