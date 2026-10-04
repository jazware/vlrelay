//! The operator API (`/admin/api/...`) and the dashboard UI it feeds.
//!
//! The wire types here are the contract with `ui/`; `docs/admin-api.md`
//! lists the endpoints. Everything behind them goes through [`AdminSource`],
//! so the relay can implement it over its real state while [`demo::Demo`]
//! simulates a busy relay for UI work.

pub mod demo;
mod diff;
mod ui;

use axum::{
    Json, Router,
    extract::{Path, Query, Request, State},
    http::{StatusCode, header},
    middleware::{self, Next},
    response::{IntoResponse, Response},
    routing::{get, post},
};
use serde::{Deserialize, Serialize};
use std::{collections::BTreeMap, future::Future, sync::Arc};

pub use diff::diff_json;
pub use ui::{UiFiles, ui_routes};

// ---------------------------------------------------------------- shared enums

#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub enum HostStatus {
    /// Socket open and events arriving (or idle but healthy).
    Connected,
    /// Socket open, no event in the idle window.
    Idle,
    /// Disconnected, waiting to redial.
    Backoff,
    /// Gave up redialing; comes back on the next requestCrawl.
    Offline,
    /// Over its tier's limit, or throttled by an operator.
    Throttled,
    /// Paused by an operator: no socket, cursor kept.
    Suspended,
    /// Banned: no socket, its events are dropped, requestCrawl refused.
    Banned,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub enum Severity {
    Info,
    Warn,
    High,
    Critical,
}

/// Why a frame was dropped instead of sequenced.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub enum RejectReason {
    BadSignature,
    InvalidCommit,
    RevOutOfOrder,
    PrevDataMismatch,
    WrongHost,
    UnknownDid,
    TooLarge,
    RateLimited,
    Takendown,
    Malformed,
}

impl RejectReason {
    pub const ALL: [RejectReason; 10] = [
        Self::BadSignature,
        Self::InvalidCommit,
        Self::RevOutOfOrder,
        Self::PrevDataMismatch,
        Self::WrongHost,
        Self::UnknownDid,
        Self::TooLarge,
        Self::RateLimited,
        Self::Takendown,
        Self::Malformed,
    ];
}

// ---------------------------------------------------------------- overview

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct Overview {
    pub time_ms: i64,
    pub events_in_per_sec: f64,
    pub events_out_per_sec: f64,
    pub bytes_in_per_sec: f64,
    pub bytes_out_per_sec: f64,
    pub consumers: u32,
    pub hosts_connected: u32,
    pub hosts_total: u32,
    pub hosts_by_status: BTreeMap<HostStatus, u32>,
    pub rejects_per_sec: f64,
    /// Rejects per second by reason, over the last minute.
    pub rejects_by_reason: BTreeMap<RejectReason, f64>,
    /// Upstream receive until the frame goes out on subscribeRepos.
    pub time_to_firehose_p50_ms: f64,
    pub time_to_firehose_p99_ms: f64,
    /// Oldest sequenced-but-not-durable event in any node log.
    pub log_durability_lag_ms: f64,
    pub last_seq: i64,
    pub open_cases: u32,
    /// Busiest hosts right now, by events/s.
    pub top_hosts: Vec<HostRow>,
    /// One sample per `sample_secs`, oldest first, so charts fill on first load.
    pub history: History,
}

#[derive(Clone, Debug, Default, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct History {
    pub sample_secs: u32,
    /// Unix seconds.
    pub t: Vec<i64>,
    pub events_in: Vec<f64>,
    pub events_out: Vec<f64>,
    pub bytes_in: Vec<f64>,
    pub bytes_out: Vec<f64>,
    pub ttf_p50_ms: Vec<f64>,
    pub ttf_p99_ms: Vec<f64>,
    pub durability_lag_ms: Vec<f64>,
    pub rejects: BTreeMap<RejectReason, Vec<f64>>,
}

// ---------------------------------------------------------------- hosts

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct HostRow {
    pub host: String,
    pub tier: String,
    pub status: HostStatus,
    pub events_per_sec: f64,
    /// Rejected frames / all frames, last minute.
    pub error_rate: f64,
    pub accounts: u64,
    pub last_upstream_seq: i64,
    pub connected_since_ms: Option<i64>,
    /// Upstream receive minus the host's own event time, p50 over the last minute.
    pub lag_ms: f64,
    /// Operator throttle (events/s) on top of the tier, if any.
    pub throttle: Option<f64>,
    /// The domain rule that applies to this host, if any.
    pub rule: Option<u64>,
    /// Owning node (host shard owner).
    pub node: String,
}

#[derive(Clone, Debug, Default, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct HostQuery {
    /// Substring of the hostname.
    pub q: Option<String>,
    pub tier: Option<String>,
    pub status: Option<HostStatus>,
    /// `host`, `tier`, `status`, `events`, `errors`, `accounts`, `seq`, `since`, `lag`.
    pub sort: Option<String>,
    #[serde(default)]
    pub desc: bool,
    pub limit: Option<usize>,
    pub offset: Option<usize>,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct HostList {
    /// Matches before limit/offset.
    pub total: usize,
    pub hosts: Vec<HostRow>,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct HostDetail {
    pub row: HostRow,
    /// The tier's limits plus any throttle, as enforced.
    pub limits: TierLimits,
    pub new_accounts_per_hour: f64,
    pub rejects_by_reason: BTreeMap<RejectReason, u64>,
    pub recent_rejects: Vec<RejectSample>,
    /// Per-second samples, oldest first.
    pub series: HostSeries,
    pub actions: Vec<HostActionRecord>,
    pub open_cases: Vec<u64>,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct RejectSample {
    pub at_ms: i64,
    pub did: String,
    pub reason: RejectReason,
    pub upstream_seq: i64,
    pub detail: String,
}

#[derive(Clone, Debug, Default, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct HostSeries {
    pub sample_secs: u32,
    pub t: Vec<i64>,
    pub events: Vec<f64>,
    pub rejects: Vec<f64>,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(tag = "action", rename_all = "kebab-case")]
pub enum HostAction {
    SetTier {
        tier: String,
    },
    /// `events_per_sec` None lifts the throttle.
    Throttle {
        #[serde(rename = "eventsPerSec")]
        events_per_sec: Option<f64>,
    },
    Suspend {
        reason: String,
    },
    Ban {
        reason: String,
    },
    Unban,
    Reconnect,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct HostActionRecord {
    pub at_ms: i64,
    pub by: String,
    pub action: HostAction,
}

// ---------------------------------------------------------------- domain rules

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct DomainRule {
    pub id: u64,
    /// `example.com` (exact) or `*.example.com` (the domain and every subdomain).
    pub pattern: String,
    pub effect: RuleEffect,
    pub note: String,
    pub created_at_ms: i64,
    pub created_by: String,
    /// Known hosts the pattern matches right now.
    pub matches: u32,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "kebab-case")]
pub enum RuleEffect {
    Ban,
    /// Admitted when requestCrawl is allow-list only, and not counted
    /// against the new-hosts-per-day budget.
    Allow,
    Tier {
        tier: String,
    },
    Throttle {
        #[serde(rename = "eventsPerSec")]
        events_per_sec: f64,
    },
}

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct DomainRuleInput {
    pub pattern: String,
    pub effect: RuleEffect,
    #[serde(default)]
    pub note: String,
}

// ---------------------------------------------------------------- policy

/// Tier limits and spam thresholds, stored and versioned as one object so
/// a change to several of them lands atomically on every node.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct Policy {
    pub tiers: BTreeMap<String, TierLimits>,
    /// The tier a newly crawled host starts in.
    pub default_tier: String,
    pub spam: SpamThresholds,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct TierLimits {
    pub events_per_sec: f64,
    pub events_per_hour: u64,
    pub events_per_day: u64,
    pub max_accounts: u64,
    pub new_accounts_per_hour: u64,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct SpamThresholds {
    /// New accounts per hour on one host before a case opens.
    pub new_accounts_per_hour: u64,
    /// Rejected / all frames over 5 minutes.
    pub reject_ratio: f64,
    /// Bad signatures per minute on one host.
    pub bad_signatures_per_min: u64,
    /// Events per second from one account.
    pub account_events_per_sec: f64,
    /// Throttle a host automatically when a case opens (else cases only).
    pub auto_throttle: bool,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct PolicyDoc {
    pub version: u64,
    pub policy: Policy,
    pub updated_at_ms: i64,
    pub updated_by: String,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct PolicyUpdate {
    /// The version the edit was made against; a mismatch is a 409.
    pub base_version: u64,
    pub policy: Policy,
    #[serde(default)]
    pub note: String,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct PolicyAudit {
    pub version: u64,
    pub at_ms: i64,
    pub by: String,
    pub note: String,
    /// `tiers.standard.eventsPerSec: 50 → 80`, one per changed leaf.
    pub changes: Vec<String>,
}

/// The whole policy document (tier limits, transitions, spam thresholds,
/// cluster budgets, consumer limits, crawl rules), for the raw editor. The
/// body is the engine's `PolicyBody` as JSON, so this type doesn't track it.
#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct FullPolicyDoc {
    pub version: u64,
    pub updated_at_ms: i64,
    pub updated_by: String,
    #[serde(default)]
    pub note: String,
    pub policy: serde_json::Value,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct FullPolicyUpdate {
    pub base_version: u64,
    pub policy: serde_json::Value,
    #[serde(default)]
    pub note: String,
}

// ---------------------------------------------------------------- consumers

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct Consumer {
    pub id: u64,
    pub ip: String,
    pub user_agent: String,
    pub node: String,
    pub connected_since_ms: i64,
    /// Relay seq last sent.
    pub cursor: i64,
    /// Newest seq minus what this consumer has been sent, as time.
    pub lag_ms: f64,
    pub events_per_sec: f64,
    pub bytes_per_sec: f64,
    /// Replaying from a cursor (true) or live.
    pub backfilling: bool,
}

// ---------------------------------------------------------------- cluster

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ClusterView {
    pub nodes: Vec<NodeView>,
    /// Owner node per host shard (None while unowned).
    pub host_shards: Vec<Option<String>>,
    /// Owner node per DID shard.
    pub did_shards: Vec<Option<String>>,
    pub last_seq: i64,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct NodeView {
    pub id: String,
    pub addr: String,
    pub version: String,
    pub rev: String,
    pub reachable: bool,
    pub lease_valid: bool,
    pub lease_expires_ms: i64,
    pub host_shards: u32,
    pub did_shards: u32,
    pub hosts: u32,
    pub consumers: u32,
    pub events_in_per_sec: f64,
    pub events_out_per_sec: f64,
    /// Sequenced but not yet durable, as time.
    pub log_durability_lag_ms: f64,
    pub cpu: f64,
    pub mem_bytes: u64,
}

// ---------------------------------------------------------------- accounts

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct Account {
    pub did: String,
    pub handle: Option<String>,
    pub host: String,
    /// The relay's view: active, takendown, suspended, deactivated, deleted, throttled.
    pub status: String,
    /// What the host last said in an #account event.
    pub upstream_status: String,
    pub takedown: Option<Takedown>,
    pub rev: String,
    pub last_seq: i64,
    pub last_event_ms: i64,
    pub events_last_hour: u64,
    pub rejects_last_hour: u64,
    pub did_shard: u32,
    pub node: String,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct Takedown {
    pub at_ms: i64,
    pub by: String,
    pub reason: String,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct ReasonBody {
    #[serde(default)]
    pub reason: String,
}

#[derive(Clone, Debug, Default, Deserialize)]
pub struct AccountQuery {
    /// A DID, a handle, or a prefix of either.
    pub q: Option<String>,
}

// ---------------------------------------------------------------- cases

#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub enum CaseStatus {
    Open,
    Acknowledged,
    Resolved,
    Dismissed,
}

/// One trip folded into a case: what was measured and every signal's
/// count for the same host (and DID) at that moment.
#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct CaseEvidence {
    pub at_ms: i64,
    pub observed: f64,
    pub threshold: f64,
    pub window_secs: u32,
    pub node: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub detail: Option<String>,
    #[serde(default)]
    pub signals: BTreeMap<String, f64>,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct CaseDetail {
    #[serde(flatten)]
    pub case: Case,
    /// Trips folded into this case, including the first.
    pub trips: u32,
    /// The newest trips, oldest first.
    pub evidence: Vec<CaseEvidence>,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct Case {
    pub id: u64,
    pub host: String,
    pub did: Option<String>,
    /// The threshold that tripped: `new-accounts`, `reject-ratio`, `bad-signatures`, `account-rate`.
    pub kind: String,
    pub severity: Severity,
    pub status: CaseStatus,
    pub opened_at_ms: i64,
    pub updated_at_ms: i64,
    pub summary: String,
    /// The measured value and the threshold it crossed.
    pub observed: f64,
    pub threshold: f64,
    pub auto_action: Option<String>,
    pub notes: Vec<CaseNote>,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct CaseNote {
    pub at_ms: i64,
    pub by: String,
    pub text: String,
}

#[derive(Clone, Debug, Default, Deserialize)]
pub struct CaseQuery {
    pub status: Option<CaseStatus>,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct CaseUpdate {
    pub status: Option<CaseStatus>,
    #[serde(default)]
    pub note: String,
}

// ---------------------------------------------------------------- source trait

#[derive(Debug, thiserror::Error)]
pub enum AdminError {
    #[error("{0}")]
    NotFound(String),
    #[error("{0}")]
    BadRequest(String),
    /// A CAS update against a stale version.
    #[error("{0}")]
    Conflict(String),
    #[error(transparent)]
    Internal(#[from] anyhow::Error),
}

impl IntoResponse for AdminError {
    fn into_response(self) -> Response {
        let (status, error) = match &self {
            AdminError::NotFound(_) => (StatusCode::NOT_FOUND, "NotFound"),
            AdminError::BadRequest(_) => (StatusCode::BAD_REQUEST, "InvalidRequest"),
            AdminError::Conflict(_) => (StatusCode::CONFLICT, "VersionConflict"),
            AdminError::Internal(e) => {
                tracing::error!(error = %e, "admin api");
                (StatusCode::INTERNAL_SERVER_ERROR, "InternalServerError")
            }
        };
        (
            status,
            Json(serde_json::json!({ "error": error, "message": self.to_string() })),
        )
            .into_response()
    }
}

pub type AdminResult<T> = Result<T, AdminError>;

/// What the dashboard needs from a relay. `by` is the operator label the
/// audit trail records (the admin token has no user, so it's "admin" plus
/// the client IP).
pub trait AdminSource: Send + Sync + 'static {
    fn overview(&self) -> impl Future<Output = AdminResult<Overview>> + Send;
    fn hosts(&self, q: HostQuery) -> impl Future<Output = AdminResult<HostList>> + Send;
    fn host(&self, host: &str) -> impl Future<Output = AdminResult<HostDetail>> + Send;
    fn host_action(
        &self,
        host: &str,
        action: HostAction,
        by: &str,
    ) -> impl Future<Output = AdminResult<HostRow>> + Send;

    fn domain_rules(&self) -> impl Future<Output = AdminResult<Vec<DomainRule>>> + Send;
    fn create_domain_rule(
        &self,
        rule: DomainRuleInput,
        by: &str,
    ) -> impl Future<Output = AdminResult<DomainRule>> + Send;
    fn update_domain_rule(
        &self,
        id: u64,
        rule: DomainRuleInput,
        by: &str,
    ) -> impl Future<Output = AdminResult<DomainRule>> + Send;
    fn delete_domain_rule(&self, id: u64, by: &str)
    -> impl Future<Output = AdminResult<()>> + Send;

    fn policy(&self) -> impl Future<Output = AdminResult<PolicyDoc>> + Send;
    fn update_policy(
        &self,
        update: PolicyUpdate,
        by: &str,
    ) -> impl Future<Output = AdminResult<PolicyDoc>> + Send;
    fn policy_audit(&self) -> impl Future<Output = AdminResult<Vec<PolicyAudit>>> + Send;
    fn full_policy(&self) -> impl Future<Output = AdminResult<FullPolicyDoc>> + Send {
        async { Err(AdminError::NotFound("this relay has no full policy document".into())) }
    }
    fn update_full_policy(
        &self,
        _update: FullPolicyUpdate,
        _by: &str,
    ) -> impl Future<Output = AdminResult<FullPolicyDoc>> + Send {
        async { Err(AdminError::NotFound("this relay has no full policy document".into())) }
    }
    fn domain_rules_audit(&self) -> impl Future<Output = AdminResult<Vec<PolicyAudit>>> + Send {
        async { Ok(Vec::new()) }
    }

    fn consumers(&self) -> impl Future<Output = AdminResult<Vec<Consumer>>> + Send;
    fn kick_consumer(&self, id: u64, by: &str) -> impl Future<Output = AdminResult<()>> + Send;

    fn cluster(&self) -> impl Future<Output = AdminResult<ClusterView>> + Send;

    fn accounts(&self, q: AccountQuery) -> impl Future<Output = AdminResult<Vec<Account>>> + Send;
    fn account(&self, did: &str) -> impl Future<Output = AdminResult<Account>> + Send;
    fn takedown(
        &self,
        did: &str,
        reason: String,
        by: &str,
    ) -> impl Future<Output = AdminResult<Account>> + Send;
    fn untakedown(&self, did: &str, by: &str) -> impl Future<Output = AdminResult<Account>> + Send;

    fn cases(&self, q: CaseQuery) -> impl Future<Output = AdminResult<Vec<Case>>> + Send;
    fn case(&self, id: u64) -> impl Future<Output = AdminResult<Case>> + Send;
    fn case_detail(&self, id: u64) -> impl Future<Output = AdminResult<CaseDetail>> + Send {
        async move {
            let case = self.case(id).await?;
            Ok(CaseDetail { case, trips: 1, evidence: Vec::new() })
        }
    }
    fn update_case(
        &self,
        id: u64,
        update: CaseUpdate,
        by: &str,
    ) -> impl Future<Output = AdminResult<Case>> + Send;
}

// ---------------------------------------------------------------- router

struct Ctx<S> {
    src: Arc<S>,
    token: String,
}

/// `/admin/api/...`, behind `Authorization: Basic admin:<token>` (the same
/// scheme as vlpds's console, so the UI's token handling carries over).
pub fn api_routes<S: AdminSource>(src: Arc<S>, admin_token: String) -> Router {
    let ctx = Arc::new(Ctx {
        src,
        token: admin_token,
    });
    Router::new()
        .route("/admin/api/overview", get(overview::<S>))
        .route("/admin/api/hosts", get(hosts::<S>))
        .route("/admin/api/hosts/{host}", get(host::<S>))
        .route("/admin/api/hosts/{host}/action", post(host_action::<S>))
        .route(
            "/admin/api/domain-rules",
            get(rules::<S>).post(create_rule::<S>),
        )
        .route(
            "/admin/api/domain-rules/{id}",
            axum::routing::put(update_rule::<S>).delete(delete_rule::<S>),
        )
        .route(
            "/admin/api/policy",
            get(policy::<S>).put(update_policy::<S>),
        )
        .route("/admin/api/policy/audit", get(policy_audit::<S>))
        .route(
            "/admin/api/policy/full",
            get(full_policy::<S>).put(update_full_policy::<S>),
        )
        .route("/admin/api/domain-rules/audit", get(rules_audit::<S>))
        .route("/admin/api/consumers", get(consumers::<S>))
        .route("/admin/api/consumers/{id}/kick", post(kick::<S>))
        .route("/admin/api/cluster", get(cluster::<S>))
        .route("/admin/api/accounts", get(accounts::<S>))
        .route("/admin/api/accounts/{did}", get(account::<S>))
        .route("/admin/api/accounts/{did}/takedown", post(takedown::<S>))
        .route(
            "/admin/api/accounts/{did}/untakedown",
            post(untakedown::<S>),
        )
        .route("/admin/api/cases", get(cases::<S>))
        .route(
            "/admin/api/cases/{id}",
            get(case::<S>).post(update_case::<S>),
        )
        .route("/admin/api/cases/{id}/evidence", get(case_detail::<S>))
        .route_layer(middleware::from_fn_with_state(ctx.clone(), auth::<S>))
        .with_state(ctx)
}

async fn auth<S: AdminSource>(
    State(ctx): State<Arc<Ctx<S>>>,
    req: Request,
    next: Next,
) -> Response {
    let ok = req
        .headers()
        .get(header::AUTHORIZATION)
        .and_then(|h| h.to_str().ok())
        .and_then(|h| h.strip_prefix("Basic "))
        .is_some_and(|b| vlpds::auth::basic_admin_ok(b, &ctx.token));
    if !ok {
        return (
            StatusCode::UNAUTHORIZED,
            Json(serde_json::json!({ "error": "AuthenticationRequired", "message": "Admin token required" })),
        )
            .into_response();
    }
    let mut res = next.run(req).await;
    res.headers_mut().insert(
        header::CACHE_CONTROL,
        header::HeaderValue::from_static("no-store"),
    );
    res
}

/// The audit label. The token is shared, so the best we can record is that
/// an admin did it (a forwarded-for IP is the client's claim, so it's left out).
const BY: &str = "admin";

type Ax<S> = State<Arc<Ctx<S>>>;

async fn overview<S: AdminSource>(State(c): Ax<S>) -> AdminResult<Json<Overview>> {
    Ok(Json(c.src.overview().await?))
}
async fn hosts<S: AdminSource>(
    State(c): Ax<S>,
    Query(q): Query<HostQuery>,
) -> AdminResult<Json<HostList>> {
    Ok(Json(c.src.hosts(q).await?))
}
async fn host<S: AdminSource>(
    State(c): Ax<S>,
    Path(h): Path<String>,
) -> AdminResult<Json<HostDetail>> {
    Ok(Json(c.src.host(&h).await?))
}
async fn host_action<S: AdminSource>(
    State(c): Ax<S>,
    Path(h): Path<String>,
    Json(a): Json<HostAction>,
) -> AdminResult<Json<HostRow>> {
    Ok(Json(c.src.host_action(&h, a, BY).await?))
}
async fn rules<S: AdminSource>(State(c): Ax<S>) -> AdminResult<Json<Vec<DomainRule>>> {
    Ok(Json(c.src.domain_rules().await?))
}
async fn create_rule<S: AdminSource>(
    State(c): Ax<S>,
    Json(r): Json<DomainRuleInput>,
) -> AdminResult<Json<DomainRule>> {
    Ok(Json(c.src.create_domain_rule(r, BY).await?))
}
async fn update_rule<S: AdminSource>(
    State(c): Ax<S>,
    Path(id): Path<u64>,
    Json(r): Json<DomainRuleInput>,
) -> AdminResult<Json<DomainRule>> {
    Ok(Json(c.src.update_domain_rule(id, r, BY).await?))
}
async fn delete_rule<S: AdminSource>(
    State(c): Ax<S>,
    Path(id): Path<u64>,
) -> AdminResult<StatusCode> {
    c.src.delete_domain_rule(id, BY).await?;
    Ok(StatusCode::NO_CONTENT)
}
async fn policy<S: AdminSource>(State(c): Ax<S>) -> AdminResult<Json<PolicyDoc>> {
    Ok(Json(c.src.policy().await?))
}
async fn update_policy<S: AdminSource>(
    State(c): Ax<S>,
    Json(u): Json<PolicyUpdate>,
) -> AdminResult<Json<PolicyDoc>> {
    validate_policy(&u.policy).map_err(AdminError::BadRequest)?;
    Ok(Json(c.src.update_policy(u, BY).await?))
}
async fn policy_audit<S: AdminSource>(State(c): Ax<S>) -> AdminResult<Json<Vec<PolicyAudit>>> {
    Ok(Json(c.src.policy_audit().await?))
}
async fn full_policy<S: AdminSource>(State(c): Ax<S>) -> AdminResult<Json<FullPolicyDoc>> {
    Ok(Json(c.src.full_policy().await?))
}
async fn update_full_policy<S: AdminSource>(
    State(c): Ax<S>,
    Json(u): Json<FullPolicyUpdate>,
) -> AdminResult<Json<FullPolicyDoc>> {
    Ok(Json(c.src.update_full_policy(u, BY).await?))
}
async fn rules_audit<S: AdminSource>(State(c): Ax<S>) -> AdminResult<Json<Vec<PolicyAudit>>> {
    Ok(Json(c.src.domain_rules_audit().await?))
}
async fn case_detail<S: AdminSource>(
    State(c): Ax<S>,
    Path(id): Path<u64>,
) -> AdminResult<Json<CaseDetail>> {
    Ok(Json(c.src.case_detail(id).await?))
}
async fn consumers<S: AdminSource>(State(c): Ax<S>) -> AdminResult<Json<Vec<Consumer>>> {
    Ok(Json(c.src.consumers().await?))
}
async fn kick<S: AdminSource>(State(c): Ax<S>, Path(id): Path<u64>) -> AdminResult<StatusCode> {
    c.src.kick_consumer(id, BY).await?;
    Ok(StatusCode::NO_CONTENT)
}
async fn cluster<S: AdminSource>(State(c): Ax<S>) -> AdminResult<Json<ClusterView>> {
    Ok(Json(c.src.cluster().await?))
}
async fn accounts<S: AdminSource>(
    State(c): Ax<S>,
    Query(q): Query<AccountQuery>,
) -> AdminResult<Json<Vec<Account>>> {
    Ok(Json(c.src.accounts(q).await?))
}
async fn account<S: AdminSource>(
    State(c): Ax<S>,
    Path(did): Path<String>,
) -> AdminResult<Json<Account>> {
    Ok(Json(c.src.account(&did).await?))
}
async fn takedown<S: AdminSource>(
    State(c): Ax<S>,
    Path(did): Path<String>,
    Json(b): Json<ReasonBody>,
) -> AdminResult<Json<Account>> {
    if b.reason.trim().is_empty() {
        return Err(AdminError::BadRequest("a takedown needs a reason".into()));
    }
    Ok(Json(c.src.takedown(&did, b.reason, BY).await?))
}
async fn untakedown<S: AdminSource>(
    State(c): Ax<S>,
    Path(did): Path<String>,
) -> AdminResult<Json<Account>> {
    Ok(Json(c.src.untakedown(&did, BY).await?))
}
async fn cases<S: AdminSource>(
    State(c): Ax<S>,
    Query(q): Query<CaseQuery>,
) -> AdminResult<Json<Vec<Case>>> {
    Ok(Json(c.src.cases(q).await?))
}
async fn case<S: AdminSource>(State(c): Ax<S>, Path(id): Path<u64>) -> AdminResult<Json<Case>> {
    Ok(Json(c.src.case(id).await?))
}
async fn update_case<S: AdminSource>(
    State(c): Ax<S>,
    Path(id): Path<u64>,
    Json(u): Json<CaseUpdate>,
) -> AdminResult<Json<Case>> {
    Ok(Json(c.src.update_case(id, u, BY).await?))
}

/// Checks every implementation would otherwise repeat; the UI runs the same
/// rules before it offers Save.
pub fn validate_policy(p: &Policy) -> Result<(), String> {
    if p.tiers.is_empty() {
        return Err("at least one tier is required".into());
    }
    if !p.tiers.contains_key(&p.default_tier) {
        return Err(format!("default tier {:?} is not defined", p.default_tier));
    }
    for (name, t) in &p.tiers {
        if name.is_empty()
            || !name
                .chars()
                .all(|c| c.is_ascii_lowercase() || c.is_ascii_digit() || c == '-')
        {
            return Err(format!("tier name {name:?}: use a-z, 0-9 and -"));
        }
        if !(t.events_per_sec.is_finite() && t.events_per_sec > 0.0) {
            return Err(format!("tiers.{name}.eventsPerSec must be > 0"));
        }
        if (t.events_per_hour as f64) < t.events_per_sec {
            return Err(format!(
                "tiers.{name}.eventsPerHour is below one second's worth"
            ));
        }
        if t.events_per_day < t.events_per_hour {
            return Err(format!("tiers.{name}.eventsPerDay is below eventsPerHour"));
        }
    }
    let s = &p.spam;
    if !(0.0..=1.0).contains(&s.reject_ratio) {
        return Err("spam.rejectRatio must be between 0 and 1".into());
    }
    if !(s.account_events_per_sec.is_finite() && s.account_events_per_sec > 0.0) {
        return Err("spam.accountEventsPerSec must be > 0".into());
    }
    Ok(())
}

/// The demo binary's whole app: API plus UI.
pub fn app<S: AdminSource>(src: Arc<S>, admin_token: String, ui: Arc<UiFiles>) -> Router {
    api_routes(src, admin_token).merge(ui_routes(ui))
}
