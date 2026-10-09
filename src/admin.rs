//! The operator API (`/admin/api/...`) and the dashboard UI it feeds.
//!
//! The wire types here are the contract with `ui/`; `docs/admin-api.md`
//! lists the endpoints. Everything behind them goes through [`AdminSource`],
//! so the relay can implement it over its real state while [`demo::Demo`]
//! simulates a busy relay for UI work.

pub mod changes;
pub mod demo;
mod diff;
pub mod fleet;
pub mod proxy;
pub mod public;
pub mod settings;
mod ui;

use axum::{
    Extension, Json, Router,
    extract::{Path, Query, Request, State},
    http::{StatusCode, header},
    middleware::{self, Next},
    response::{IntoResponse, Response},
    routing::{get, post},
};
use serde::{Deserialize, Serialize};
use std::{collections::BTreeMap, future::Future, sync::Arc};

pub use diff::diff_json;
pub use public::{PublicStats, public_routes};
pub use settings::{ConfigEntry, SettingsView};
pub use ui::{UiFiles, docs_routes, ui_routes};

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
    /// Held at its own limits: its tier's, a domain rule's or an operator's
    /// throttle.
    Throttled,
    /// Paused by the relay, not by its limits: the pipeline behind the
    /// socket is full ([`HostRow::backpressure_reason`] says which part).
    Backpressure,
    /// Paused by an operator: no socket, cursor kept.
    Suspended,
    /// Banned: no socket, its events are dropped, requestCrawl refused.
    Banned,
}

/// What's full while a host is in [`HostStatus::Backpressure`].
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum BackpressureReason {
    /// The host's in-flight cap: frames read from it and not yet durable.
    InflightFull,
    /// The node's in-flight cap, over every host it reads.
    NodeInflightFull,
    /// Its fair-queue slot: the lanes aren't taking frames, usually while
    /// they wait on identity lookups.
    QueueFull,
    /// The node's memory budget: the process is over it while the pipeline
    /// holds its share of the in-flight caps.
    MemoryFull,
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
    /// The account isn't active on this relay: deactivated, suspended,
    /// throttled by policy, deleted or taken down.
    Inactive,
    Malformed,
}

impl RejectReason {
    pub const ALL: [RejectReason; 11] = [
        Self::BadSignature,
        Self::InvalidCommit,
        Self::RevOutOfOrder,
        Self::PrevDataMismatch,
        Self::WrongHost,
        Self::UnknownDid,
        Self::TooLarge,
        Self::RateLimited,
        Self::Takendown,
        Self::Inactive,
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
    /// An event's submit to the leader until it committed (a quorum held it).
    pub commit_lag_ms: f64,
    pub last_seq: i64,
    pub open_cases: u32,
    /// Busiest hosts right now, by events/s.
    pub top_hosts: Vec<HostRow>,
    /// One sample per `sample_secs`, oldest first, so charts fill on first load.
    pub history: History,
    /// The merged stream's own rate (what one consumer of the whole stream
    /// gets); `events_out_per_sec` sums every node's emits.
    #[serde(default)]
    pub stream_events_per_sec: f64,
    /// What each node contributed. The totals above are their sums over the
    /// nodes that answered.
    #[serde(default)]
    pub by_node: Vec<NodeTotals>,
}

#[derive(Clone, Debug, Default, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct NodeTotals {
    pub node: String,
    /// `core`, `edge`, `replica`, or `single`.
    pub role: String,
    /// It didn't answer this round: its numbers are 0 and left out of the sums.
    pub stale: bool,
    pub error: Option<String>,
    pub events_in_per_sec: f64,
    pub events_out_per_sec: f64,
    pub bytes_in_per_sec: f64,
    pub bytes_out_per_sec: f64,
    pub consumers: u32,
    pub hosts_connected: u32,
    pub hosts_total: u32,
    pub rejects_per_sec: f64,
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
    /// Set while `status` is `backpressure`.
    #[serde(default)]
    pub backpressure_reason: Option<BackpressureReason>,
    pub events_per_sec: f64,
    /// Rejected frames / all frames, last minute.
    pub error_rate: f64,
    pub accounts: u64,
    pub last_upstream_seq: i64,
    pub connected_since_ms: Option<i64>,
    /// How far the reader is behind the host's stream: the newest frame's
    /// read time minus its event time, plus the time since while held back;
    /// 0 once the reader waits on an empty socket.
    pub lag_ms: f64,
    /// Set while the host's events are read faster than it sent them (a
    /// backlog after a restart, a reconnect or a cursor resume): host
    /// seconds per wall second. Its limits and spam signals count on its
    /// own timeline meanwhile, so the backlog costs what the traffic did.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub catch_up_pace: Option<f64>,
    /// Operator throttle (events/s) on top of the tier, if any.
    pub throttle: Option<f64>,
    /// The domain rule that applies to this host, if any.
    pub rule: Option<u64>,
    /// The member that reads it (the leader's host table).
    pub node: String,
    /// The account cap in force (tier, or an operator's per-host limit).
    /// 0: unknown.
    #[serde(default)]
    pub max_accounts: u64,
    /// Events per second, one sample a second, oldest first: set on the
    /// overview's top hosts only.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub history: Vec<f64>,
    /// Accounts it created that the relay throttled (past its cap) and no
    /// operator has released, as the leader counts them.
    #[serde(default)]
    pub throttled_accounts: u64,
    /// How the relay found it: `requestCrawl`, `bootstrap:<relay>`, `plc`
    /// or `cli` (None: before sources were recorded).
    #[serde(default)]
    pub source: Option<String>,
    /// The reject reason with the most of its recent rejects (the last
    /// five minutes), if any.
    #[serde(default)]
    pub top_reason: Option<RejectReason>,
    /// The node-scoped version of the row's last change as the answering
    /// node reads it (docs/admin-api.md, "Versions"); None until it sees one.
    #[serde(default)]
    pub version: Option<String>,
    #[serde(default)]
    pub updated_at_ms: Option<i64>,
    /// The newest `host` version the host's owner (`node`) made, as far as
    /// the answering node has heard: comparable with the owner's own events
    /// whichever node answers, where `version` is the answering node's and
    /// `updatedAtMs` is its clock. None until it has heard one.
    #[serde(default)]
    pub owner_version: Option<String>,
    /// Only on a host action's answer: the cluster hadn't confirmed the
    /// change within a few seconds, so this is the answering node's view
    /// so far, not the action's result. A later `host` change says when it
    /// lands (docs/admin-api.md, "Host actions").
    #[serde(default, skip_serializing_if = "std::ops::Not::not")]
    pub pending: bool,
}

#[derive(Clone, Debug, Default, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct HostQuery {
    /// Substring of the hostname.
    pub q: Option<String>,
    pub tier: Option<String>,
    pub status: Option<HostStatus>,
    /// A source (`requestCrawl`, `plc`, `cli`, `bootstrap:<relay>`), or a
    /// prefix of one ending in `:` or `*` (`bootstrap:` is every relay), or
    /// `none` (not recorded).
    pub source: Option<String>,
    /// Only hosts with (true) or without (false) throttled accounts.
    pub throttled: Option<bool>,
    /// Only hosts this domain rule decides (their `rule`): the hosts it
    /// matches less those a more specific rule takes.
    pub rule: Option<u64>,
    /// `atCap`, `lagging`, `erroring` or `throttledOrAtCap`, as
    /// [`HostQuery::keeps`] reads them.
    pub flag: Option<String>,
    /// `host`, `tier`, `status`, `events`, `errors`, `accounts`, `seq`,
    /// `since`, `lag`, `throttled`, `source`.
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
    /// The host's own account cap in place of its tier's (indigo's
    /// `changeLimits` `repo_limit`). None goes back to the tier's.
    SetAccountLimit {
        #[serde(rename = "maxAccounts")]
        max_accounts: Option<u64>,
    },
}

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct HostActionRecord {
    pub at_ms: i64,
    pub by: String,
    pub action: HostAction,
    /// Why the relay moved the host on its own (operators' actions carry
    /// their reason in the action, if any).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub reason: Option<String>,
    /// The case the relay opened or updated for it.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub case: Option<u64>,
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
    /// Known hosts the rule decides right now: those the pattern matches
    /// less those a more specific rule takes.
    pub matches: u32,
    /// The rule set's version when it was read.
    #[serde(default)]
    pub version: u64,
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
    /// Where its next events come from: `ring` (the firehose's memory, as
    /// every live consumer), `disk` (the node's own log) or `bucket`.
    #[serde(default)]
    pub read_tier: String,
}

#[derive(Clone, Debug, Default, Deserialize)]
pub struct KickQuery {
    /// The node serving the consumer (ids are per node); default this one.
    pub node: Option<String>,
}

// ---------------------------------------------------------------- cluster

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ClusterView {
    pub nodes: Vec<NodeView>,
    pub leader: Option<String>,
    pub epoch: u64,
    /// Hosts in the leader's host table, and those no live member owns.
    pub hosts: u32,
    pub unowned_hosts: u32,
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
    /// Reachable, in the member set, and holding an intact log.
    pub healthy: bool,
    /// `leader`, `follower`, `candidate`, or `unreachable`.
    pub role: String,
    /// Copying the leader's log before a membership change makes it a member.
    pub learner: bool,
    /// Hosts the leader's table gives it.
    pub owned_hosts: u32,
    /// Hosts it has a socket open to.
    pub hosts: u32,
    pub consumers: u32,
    pub events_in_per_sec: f64,
    pub events_out_per_sec: f64,
    /// Its events' submit to the leader until committed.
    pub commit_lag_ms: f64,
    /// Cores busy over the last sample.
    pub cpu: f64,
    /// Resident memory; None where the platform doesn't say.
    pub mem_bytes: Option<u64>,
    /// It didn't answer this round (down, hung or partitioned): the numbers
    /// above are 0, not its last ones.
    pub stale: bool,
    pub error: Option<String>,
    /// When its numbers were last read (unix ms; 0: never).
    pub reported_ms: i64,
    pub bytes_out_per_sec: f64,
    /// The last seq its stream emitted.
    pub stream_seq: i64,
}

/// The quorum log as every node reports it (`/qlog/status`, docs/quorum.md).
/// `status` is passed through as the node serialized it, so a field the log
/// adds shows up without a change here; `ui/src/lib/api.ts` mirrors it.
#[derive(Clone, Debug, Default, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct QuorumView {
    pub nodes: Vec<QuorumNode>,
}

#[derive(Clone, Debug, Default, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct QuorumNode {
    pub node: String,
    pub addr: String,
    /// It didn't answer this round; `status` is None.
    pub stale: bool,
    pub error: Option<String>,
    /// When `status` was read (unix ms): the last contact.
    pub reported_ms: i64,
    pub status: Option<serde_json::Value>,
}

/// A membership change, forwarded to the leader's `POST /qlog/members`.
#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct QuorumMembersChange {
    /// The whole member set wanted.
    pub members: Vec<String>,
    /// Addresses for nodes the leader can't dial yet.
    #[serde(default)]
    pub addrs: BTreeMap<String, String>,
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

impl CaseStatus {
    pub fn as_str(self) -> &'static str {
        match self {
            CaseStatus::Open => "open",
            CaseStatus::Acknowledged => "acknowledged",
            CaseStatus::Resolved => "resolved",
            CaseStatus::Dismissed => "dismissed",
        }
    }
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
    /// `GET cases` only (an [`AdminSource`] filters by status alone).
    #[serde(default)]
    pub kind: Option<String>,
    #[serde(default)]
    pub host: Option<String>,
    #[serde(default)]
    pub limit: Option<usize>,
    #[serde(default)]
    pub offset: Option<usize>,
}

/// `GET cases`: a page of the cases matching every filter, worst severity
/// first, how many match, and facet counts for the filter tabs.
#[derive(Clone, Debug, Default, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct CaseList {
    pub cases: Vec<Case>,
    pub total: usize,
    pub counts: CaseCounts,
}

#[derive(Clone, Debug, Default, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct CaseCounts {
    /// Every filter but `status`.
    pub by_status: BTreeMap<String, usize>,
    /// Every filter but `kind`.
    pub by_kind: BTreeMap<String, usize>,
}

/// [`CaseList`] out of every case (sorted as [`AdminSource::cases`] sorts).
pub fn case_page(all: Vec<Case>, q: &CaseQuery) -> CaseList {
    let kind = |c: &Case| q.kind.as_ref().is_none_or(|k| *k == c.kind);
    let status = |c: &Case| q.status.is_none_or(|s| s == c.status);
    let host = |c: &Case| q.host.as_ref().is_none_or(|h| h.eq_ignore_ascii_case(&c.host));
    let mut counts = CaseCounts::default();
    for c in all.iter().filter(|c| host(c)) {
        if kind(c) {
            *counts.by_status.entry(c.status.as_str().to_string()).or_default() += 1;
        }
        if status(c) {
            *counts.by_kind.entry(c.kind.clone()).or_default() += 1;
        }
    }
    let matching: Vec<Case> = all.into_iter().filter(|c| host(c) && kind(c) && status(c)).collect();
    let total = matching.len();
    let cases = matching.into_iter().skip(q.offset.unwrap_or(0)).take(q.limit.unwrap_or(usize::MAX)).collect();
    CaseList { cases, total, counts }
}

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct CaseUpdate {
    pub status: Option<CaseStatus>,
    #[serde(default)]
    pub note: String,
}

/// Which cases a bulk update takes: every field given has to match.
#[derive(Clone, Debug, Default, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct CaseFilter {
    pub kind: Option<String>,
    pub status: Option<CaseStatus>,
    pub host: Option<String>,
}

/// `POST cases/bulk`: the cases in `ids`, or matching `filter` (both:
/// those in `ids` that match), get `status` and `note`.
#[derive(Clone, Debug, Default, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct CaseBulkUpdate {
    #[serde(default)]
    pub ids: Option<Vec<u64>>,
    #[serde(default)]
    pub filter: Option<CaseFilter>,
    pub status: Option<CaseStatus>,
    #[serde(default)]
    pub note: String,
}

#[derive(Clone, Debug, Default, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct CaseBulkResult {
    pub updated: usize,
    pub ids: Vec<u64>,
}

/// At most this many cases change in one bulk update.
pub const CASE_BULK_MAX: usize = 5_000;

/// The cases `b` takes out of `all`, and the per-case update each gets. A
/// bulk update with nothing to scope it, or nothing to change, is refused;
/// one without a note gets one, so every case's trail says who did it.
pub fn bulk_targets(all: &[Case], b: &CaseBulkUpdate) -> AdminResult<(Vec<u64>, CaseUpdate)> {
    if b.ids.is_none() && b.filter.is_none() {
        return Err(AdminError::BadRequest("give ids, a filter or both".into()));
    }
    if b.status.is_none() && b.note.trim().is_empty() {
        return Err(AdminError::BadRequest("give a status, a note or both".into()));
    }
    let ids: Option<std::collections::HashSet<u64>> = b.ids.as_ref().map(|v| v.iter().copied().collect());
    let f = b.filter.clone().unwrap_or_default();
    let out: Vec<u64> = all
        .iter()
        .filter(|c| ids.as_ref().is_none_or(|i| i.contains(&c.id)))
        .filter(|c| f.kind.as_ref().is_none_or(|k| *k == c.kind))
        .filter(|c| f.status.is_none_or(|s| s == c.status))
        .filter(|c| f.host.as_ref().is_none_or(|h| h.eq_ignore_ascii_case(&c.host)))
        .map(|c| c.id)
        .collect();
    if out.len() > CASE_BULK_MAX {
        return Err(AdminError::BadRequest(format!("{} cases match; at most {CASE_BULK_MAX} at once", out.len())));
    }
    let note = match (b.note.trim(), b.status) {
        ("", Some(s)) => format!("bulk: {}", s.as_str()),
        (n, _) => n.to_string(),
    };
    Ok((out, CaseUpdate { status: b.status, note }))
}

// ---------------------------------------------------------------- operations

/// PLC export seeding (docs/policy.md): one core reads the export, the
/// lowest-named live one.
#[derive(Clone, Debug, Default, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct PlcView {
    /// Some node runs with `--plc-export`.
    pub enabled: bool,
    pub leader: Option<String>,
    pub caught_up: bool,
    pub ops: u64,
    /// Ops read per second, over the leader's last few seconds.
    pub ops_per_sec: f64,
    pub written: u64,
    pub requests: u64,
    /// Requests the directory answered 429.
    pub throttled: u64,
    pub errors: u64,
    pub restarts: u64,
    /// The newest `createdAt` read (unix ms).
    pub newest_ms: i64,
    /// The stored checkpoint's windows (written every 10 s).
    pub windows: Vec<PlcWindow>,
    pub checkpoint_ms: i64,
    /// Fetched DID documents written to the seeds (every leader since its
    /// start), and dropped.
    pub learned: u64,
    pub learned_dropped: u64,
    pub nodes: Vec<PlcNode>,
}

#[derive(Clone, Debug, Default, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct PlcWindow {
    pub from_ms: i64,
    /// Read up to here.
    pub after_ms: i64,
    /// None: the last window, which follows the tail.
    pub until_ms: Option<i64>,
    pub ops: u64,
    pub done: bool,
    /// 0..1 of the window's time span.
    pub progress: f64,
}

#[derive(Clone, Debug, Default, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct PlcNode {
    pub node: String,
    pub stale: bool,
    pub leader: bool,
    pub ops: u64,
    pub ops_per_sec: f64,
    pub throttled: u64,
    pub errors: u64,
}

/// The ack backlog and restart dedupe: what's been read off upstream
/// sockets and isn't done yet, per node and per host.
#[derive(Clone, Debug, Default, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct PipelineView {
    pub nodes: Vec<PipelineNode>,
    /// Hosts with events in flight or a paused reader, most in flight first.
    pub hosts: Vec<PipelineHost>,
}

#[derive(Clone, Debug, Default, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct PipelineNode {
    pub node: String,
    pub stale: bool,
    /// Upstream events read but not yet durable, rejected or skipped.
    pub ack_pending: u64,
    /// Age of the oldest of them.
    pub oldest_pending_ms: f64,
    /// Events queued in front of the lanes.
    pub lane_queued: u64,
    /// (host, upstream seq) pairs held for restart dedupe (cluster DID owners).
    pub dedupe_entries: u64,
    pub paused_hosts: u32,
    /// Every relay gauge about in-flight work, queues, backlogs, caps and
    /// pauses, as `name{label="v"}`, so new ones show up as they land.
    pub gauges: BTreeMap<String, f64>,
}

#[derive(Clone, Debug, Default, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct PipelineHost {
    pub host: String,
    pub node: String,
    pub inflight: u64,
    /// The host's in-flight cap, once the relay has one.
    pub inflight_cap: Option<u64>,
    /// Its reader is paused by the relay (status `backpressure`).
    pub paused: bool,
    pub status: Option<HostStatus>,
    pub events_per_sec: f64,
}

/// One host's rejects, for `ops/rejects/top`.
#[derive(Clone, Debug, Default, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct RejectTop {
    pub host: String,
    /// Over the answering nodes' last sample windows (~10 s or more).
    pub rejects_per_sec: f64,
    /// Since each node's start.
    pub total: u64,
    pub last_at_ms: Option<i64>,
    /// The newest one.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub sample: Option<RejectSample>,
}

#[derive(Clone, Debug, Default, Deserialize)]
pub struct RejectTopQuery {
    /// A reason (`bad-signature`, `prev-data-mismatch` ...); none: all.
    pub reason: Option<RejectReason>,
    pub limit: Option<usize>,
}

/// Host discovery as the leader runs it.
#[derive(Clone, Debug, Default, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct DiscoveryView {
    /// The member running it (the leader).
    pub leader: Option<String>,
    /// The answering node is the one running it.
    pub leading: bool,
    pub connects_per_min: f64,
    pub requests_per_sec: f64,
    pub sources: Vec<DiscoverySource>,
}

#[derive(Clone, Debug, Default, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct DiscoverySource {
    /// `bootstrap:<relay host>` or `plc`.
    pub key: String,
    pub enabled: bool,
    pub refresh_interval_secs: Option<u64>,
    /// When its next run starts (now while one is in progress).
    pub next_run_ms: Option<i64>,
    /// `plc`: hosts waiting for admission.
    pub pending: u64,
    #[serde(flatten)]
    pub state: crate::discovery::SourceState,
}

#[derive(Clone, Debug, Default, Deserialize)]
pub struct DiscoveryRun {
    /// A source's key; None runs every enabled one.
    pub source: Option<String>,
}

/// requestCrawl outcomes this node saw, and the cluster's new-host budget.
#[derive(Clone, Debug, Default, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct AdmissionLog {
    pub new_hosts_today: u32,
    pub new_hosts_per_day: u32,
    /// Newest first.
    pub entries: Vec<crate::upstream::crawl::CrawlAdmission>,
}

#[derive(Clone, Debug, Default, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct TailQuery {
    /// Only this host's frames, passed ones included.
    pub host: Option<String>,
    /// Rejected and held frames (from every host, or `host`'s).
    #[serde(default)]
    pub rejects: Option<u8>,
    /// Only frames after this time (unix ms), for a poll.
    pub since_ms: Option<i64>,
    pub limit: Option<usize>,
}

/// A frame this node read: rejected, held (an account the relay throttled,
/// or a new one deferred), or passed (with the seq it went out at).
#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct TailFrame {
    pub at_ms: i64,
    pub host: String,
    pub did: String,
    /// `reject`, `held` or `passed`.
    pub kind: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub reason: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub detail: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub upstream_seq: Option<i64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub seq: Option<i64>,
    /// The event's kind (`commit`, `identity` ...), for passed frames.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub event: Option<String>,
}

#[derive(Clone, Debug, Default, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct Released {
    pub released: u64,
}

/// Object-store requests by class: A (writes, lists), B (reads), free
/// (deletes, aborts).
#[derive(Clone, Debug, Default, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ClassCounts {
    pub a: f64,
    pub b: f64,
    pub free: f64,
}

#[derive(Clone, Debug, Default, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct StorePurpose {
    pub purpose: String,
    /// Since this process started.
    pub requests: ClassCounts,
    /// Per second over `windowSecs`.
    pub per_sec: ClassCounts,
    pub bytes_up: u64,
    pub bytes_down: u64,
}

#[derive(Clone, Debug, Default, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct StoreLatency {
    pub op: String,
    pub count: u64,
    pub mean_ms: f64,
    /// From the histogram's buckets: the upper bound of the bucket the
    /// quantile falls in.
    pub p50_ms: f64,
    pub p99_ms: f64,
}

/// The object store as this node uses it: requests by purpose and class,
/// rates, latency by op, and the bucket's sizes and retention from the
/// leader's last retention pass (`retain/qlog`).
#[derive(Clone, Debug, Default, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct StoreView {
    pub node: String,
    pub at_ms: i64,
    /// The rates' window: since the previous sample this node kept (the
    /// first call has none: rates are 0).
    pub window_secs: f64,
    pub total: StorePurpose,
    pub purposes: Vec<StorePurpose>,
    pub latency: Vec<StoreLatency>,
    /// The leader's last retention pass, as it wrote it; None before the
    /// first or with retention off.
    pub retention: Option<serde_json::Value>,
    /// Every SlateDB database this node has open (the quorum log's state,
    /// the PLC seeds' writer or reader): its LSM shape, memtable, cache and
    /// compaction, as `slatedb_*{db=...}` exports them.
    #[serde(default)]
    pub dbs: Vec<slate_metrics::DbShape>,
}

/// The cluster budgets against their use on the answering node.
#[derive(Clone, Debug, Default, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct PolicyUsage {
    pub node: String,
    /// PLC (and did:web) document fetches a second, over the last sample.
    pub plc_lookups_per_sec: f64,
    /// `cluster.plcLookupsPerSec`, and this node's share of it.
    pub plc_lookups_budget: f64,
    pub plc_lookups_share: f64,
    /// Misses the PLC export's seeds filled, a second.
    pub seeded_per_sec: f64,
    /// New accounts a minute, over the last sample (counted where the
    /// account gate runs: the leader).
    pub new_accounts_per_min: f64,
    pub new_accounts_budget: f64,
    pub new_hosts_today: u32,
    pub new_hosts_per_day: u32,
    /// The sample's window.
    pub window_secs: f64,
}

/// The heaviest keys of each spam signal on the answering node.
#[derive(Clone, Debug, Default, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct SignalsView {
    pub node: String,
    pub signals: Vec<SignalTop>,
}

#[derive(Clone, Debug, Default, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct SignalTop {
    /// The rule (`host-new-accounts`, `account-records` ...).
    pub rule: String,
    /// `host` or `account`.
    pub per: String,
    pub limit: f64,
    pub window_secs: u32,
    pub enabled: bool,
    /// Heaviest first (Space-Saving: `estimate` may overcount by up to
    /// `estimate - lower`).
    pub top: Vec<SignalKey>,
}

#[derive(Clone, Debug, Default, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct SignalKey {
    pub key: String,
    pub host: String,
    pub estimate: f64,
    pub lower: f64,
}

/// Leadership changes across the members, newest first.
#[derive(Clone, Debug, Default, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct QuorumHistory {
    pub events: Vec<QuorumEvent>,
    /// Members that didn't answer: their changes are missing.
    pub stale: Vec<String>,
}

#[derive(Clone, Debug, Default, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct QuorumEvent {
    pub node: String,
    pub at_ms: i64,
    /// `lead`, `step_down`, `recovery` or `membership`.
    pub kind: String,
    pub epoch: u64,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub from: Option<String>,
    /// How it led (`election`, `recovery`, `membership change`) or why it
    /// stepped down; for a membership change, `a,b,c -> a,b,d`.
    pub why: String,
}

#[derive(Clone, Debug, Default, Deserialize)]
pub struct NodeQuery {
    /// A member's id; default the answering node.
    pub node: Option<String>,
}

impl HostQuery {
    /// A `flag` this query can't filter by.
    pub fn bad_flag(&self) -> Option<&str> {
        self.flag.as_deref().filter(|f| !matches!(*f, "" | "atCap" | "lagging" | "erroring" | "throttledOrAtCap"))
    }

    /// The filters past the name, tier and status ones.
    pub fn keeps(&self, r: &HostRow) -> bool {
        let src = self.source.as_deref().filter(|s| !s.is_empty()).is_none_or(|want| {
            let have = r.source.as_deref().unwrap_or("");
            match want.strip_suffix('*') {
                Some(p) => have.starts_with(p),
                None if want.ends_with(':') => have.starts_with(want),
                None if want == "none" => have.is_empty(),
                None => have == want,
            }
        });
        let live = matches!(r.status, HostStatus::Connected | HostStatus::Throttled | HostStatus::Backpressure);
        let at_cap = r.max_accounts > 0 && r.accounts >= r.max_accounts;
        let flag = match self.flag.as_deref() {
            Some("atCap") => at_cap,
            Some("lagging") => live && r.lag_ms > 60_000.0,
            Some("erroring") => r.error_rate > 0.1,
            Some("throttledOrAtCap") => r.throttled_accounts > 0 || at_cap,
            _ => true,
        };
        src && flag
            && self.throttled.is_none_or(|t| (r.throttled_accounts > 0) == t)
            && self.rule.is_none_or(|id| r.rule == Some(id))
    }
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
    /// `set-tier` on a host whose tier a domain rule decides. The rule would
    /// win, so the action is refused instead of recorded and ignored.
    #[error("{0}")]
    TierSetByRule(String),
    /// Something this depends on (the bucket) failed for now: 503 with a
    /// Retry-After, so a client retries instead of reporting a bug.
    #[error("{0}")]
    Unavailable(String),
    #[error(transparent)]
    Internal(#[from] anyhow::Error),
}

/// What an [`AdminError::Unavailable`] asks a client to wait, in seconds:
/// about one bucket call's deadline.
pub const UNAVAILABLE_RETRY_AFTER_SECS: u64 = 5;

impl IntoResponse for AdminError {
    fn into_response(self) -> Response {
        let (status, error) = match &self {
            AdminError::NotFound(_) => (StatusCode::NOT_FOUND, "NotFound"),
            AdminError::BadRequest(_) => (StatusCode::BAD_REQUEST, "InvalidRequest"),
            AdminError::Conflict(_) => (StatusCode::CONFLICT, "VersionConflict"),
            AdminError::TierSetByRule(_) => (StatusCode::CONFLICT, "TierSetByRule"),
            AdminError::Unavailable(m) => {
                tracing::warn!(error = %m, "admin api: unavailable");
                let body = Json(serde_json::json!({ "error": "Unavailable", "message": self.to_string() }));
                return (
                    StatusCode::SERVICE_UNAVAILABLE,
                    [(header::RETRY_AFTER, UNAVAILABLE_RETRY_AFTER_SECS.to_string())],
                    body,
                )
                    .into_response();
            }
            AdminError::Internal(e) => {
                tracing::error!(error = %e, "admin api");
                (StatusCode::INTERNAL_SERVER_ERROR, "InternalServerError")
            }
        };
        (status, Json(serde_json::json!({ "error": error, "message": self.to_string() }))).into_response()
    }
}

impl AdminError {
    pub fn tier_set_by_rule(id: u64, pattern: &str, tier: &str) -> AdminError {
        AdminError::TierSetByRule(format!(
            "domain rule {id} ({pattern}) sets this host's tier to {tier}: edit or remove the rule to change it"
        ))
    }
}

pub type AdminResult<T> = Result<T, AdminError>;

/// What the dashboard needs from a relay. `by` is the operator label the
/// audit trail records ([`Actor::label`]).
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
    fn delete_domain_rule(&self, id: u64, by: &str) -> impl Future<Output = AdminResult<()>> + Send;

    fn policy(&self) -> impl Future<Output = AdminResult<PolicyDoc>> + Send;
    fn update_policy(&self, update: PolicyUpdate, by: &str) -> impl Future<Output = AdminResult<PolicyDoc>> + Send;
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
    /// A consumer of `node` (ids are per node); None is this node.
    fn kick_consumer_on(&self, node: Option<&str>, id: u64, by: &str) -> impl Future<Output = AdminResult<()>> + Send {
        let other = node.map(str::to_string);
        async move {
            match other {
                Some(n) => Err(AdminError::NotFound(format!("no node {n}"))),
                None => self.kick_consumer(id, by).await,
            }
        }
    }

    fn admissions(&self) -> impl Future<Output = AdminResult<AdmissionLog>> + Send {
        async { Err(AdminError::NotFound("this relay keeps no admission log".into())) }
    }
    fn tail(&self, _q: TailQuery) -> impl Future<Output = AdminResult<Vec<TailFrame>>> + Send {
        async { Err(AdminError::NotFound("this relay keeps no tail".into())) }
    }
    /// Lifts the relay throttle of every account `host` created.
    fn release_throttled(&self, _host: &str, _by: &str) -> impl Future<Output = AdminResult<Released>> + Send {
        async { Err(AdminError::NotFound("this relay can't release throttled accounts".into())) }
    }
    fn policy_usage(&self) -> impl Future<Output = AdminResult<PolicyUsage>> + Send {
        async { Err(AdminError::NotFound("this relay doesn't report its budgets' use".into())) }
    }
    fn policy_signals(&self) -> impl Future<Output = AdminResult<SignalsView>> + Send {
        async { Err(AdminError::NotFound("this relay doesn't report its spam signals".into())) }
    }
    /// Every account under a relay takedown, newest first.
    fn takedowns(&self) -> impl Future<Output = AdminResult<Vec<crate::policy::takedowns::TakedownEntry>>> + Send {
        async { Err(AdminError::NotFound("this relay doesn't list takedowns".into())) }
    }
    fn quorum_history(&self) -> impl Future<Output = AdminResult<QuorumHistory>> + Send {
        async { Err(AdminError::NotFound("this relay doesn't run the quorum log".into())) }
    }
    /// A member's settings (None: this node's).
    fn settings_of(&self, node: Option<&str>) -> impl Future<Output = AdminResult<SettingsView>> + Send {
        async move {
            match node {
                None => self.settings().await,
                Some(n) => Err(AdminError::NotFound(format!("no node {n}"))),
            }
        }
    }
    /// Asks the leader to flush now; its status after the flush.
    fn flush_now(&self, _by: &str) -> impl Future<Output = AdminResult<serde_json::Value>> + Send {
        async { Err(AdminError::NotFound("this relay doesn't run the quorum log".into())) }
    }
    /// The hosts with the most rejects (of `reason`), across the members.
    fn rejects_top(&self, _q: RejectTopQuery) -> impl Future<Output = AdminResult<Vec<RejectTop>>> + Send {
        async { Err(AdminError::NotFound("this relay doesn't rank its rejects".into())) }
    }
    fn discovery(&self) -> impl Future<Output = AdminResult<DiscoveryView>> + Send {
        async { Err(AdminError::NotFound("this relay doesn't run host discovery".into())) }
    }
    fn discovery_run(&self, _req: DiscoveryRun, _by: &str) -> impl Future<Output = AdminResult<DiscoveryView>> + Send {
        async { Err(AdminError::NotFound("this relay doesn't run host discovery".into())) }
    }
    fn store(&self) -> impl Future<Output = AdminResult<StoreView>> + Send {
        async { Err(AdminError::NotFound("this relay keeps no object-store numbers".into())) }
    }

    fn plc_view(&self) -> impl Future<Output = AdminResult<PlcView>> + Send {
        async { Err(AdminError::NotFound("PLC export seeding isn't available on this relay".into())) }
    }
    fn pipeline_view(&self) -> impl Future<Output = AdminResult<PipelineView>> + Send {
        async { Err(AdminError::NotFound("pipeline numbers aren't available on this relay".into())) }
    }

    /// The change feed this source serves (`GET changes`); None: a 404.
    fn changes(&self) -> Option<Arc<changes::ChangeFeed>> {
        None
    }
    fn cluster(&self) -> impl Future<Output = AdminResult<ClusterView>> + Send;
    fn quorum(&self) -> impl Future<Output = AdminResult<QuorumView>> + Send {
        async { Err(AdminError::NotFound("this relay doesn't run the quorum log".into())) }
    }
    /// The leader's status after the change.
    fn change_quorum_members(
        &self,
        _req: QuorumMembersChange,
        _by: &str,
    ) -> impl Future<Output = AdminResult<serde_json::Value>> + Send {
        async { Err(AdminError::NotFound("this relay doesn't run the quorum log".into())) }
    }
    fn settings(&self) -> impl Future<Output = AdminResult<SettingsView>> + Send {
        async { Err(AdminError::NotFound("this relay doesn't report its config".into())) }
    }
    /// The public page's numbers; see [`public::project`] for why it's a
    /// projection and not its own query.
    fn public_stats(&self) -> impl Future<Output = AdminResult<PublicStats>> + Send {
        async {
            let o = self.overview().await?;
            let q = self.quorum().await.ok();
            Ok(public::project(&o, q.as_ref()))
        }
    }

    fn accounts(&self, q: AccountQuery) -> impl Future<Output = AdminResult<Vec<Account>>> + Send;
    fn account(&self, did: &str) -> impl Future<Output = AdminResult<Account>> + Send;
    fn takedown(&self, did: &str, reason: String, by: &str) -> impl Future<Output = AdminResult<Account>> + Send;
    fn untakedown(&self, did: &str, by: &str) -> impl Future<Output = AdminResult<Account>> + Send;

    fn cases(&self, q: CaseQuery) -> impl Future<Output = AdminResult<Vec<Case>>> + Send;
    fn case(&self, id: u64) -> impl Future<Output = AdminResult<Case>> + Send;
    fn case_detail(&self, id: u64) -> impl Future<Output = AdminResult<CaseDetail>> + Send {
        async move {
            let case = self.case(id).await?;
            Ok(CaseDetail { case, trips: 1, evidence: Vec::new() })
        }
    }
    fn update_case(&self, id: u64, update: CaseUpdate, by: &str) -> impl Future<Output = AdminResult<Case>> + Send;
    /// [`bulk_targets`], each updated as [`Self::update_case`] would, its
    /// change events coalesced.
    fn bulk_update_cases(
        &self,
        update: CaseBulkUpdate,
        by: &str,
    ) -> impl Future<Output = AdminResult<CaseBulkResult>> + Send;
}

// ---------------------------------------------------------------- router

struct Ctx<S> {
    src: Arc<S>,
    token: String,
}

/// `/admin/api/...`, behind `Authorization: Basic admin:<token>` (the same
/// scheme as vlpds's console, so the UI's token handling carries over), or
/// an operator a proxy named on the admin listener ([`proxy`]).
pub fn api_routes<S: AdminSource>(src: Arc<S>, admin_token: String) -> Router {
    let ctx = Arc::new(Ctx { src, token: admin_token });
    Router::new()
        .route("/admin/api/session", get(session))
        .route("/admin/api/changes", get(change_feed::<S>))
        .route("/admin/api/overview", get(overview::<S>))
        .route("/admin/api/hosts", get(hosts::<S>))
        .route("/admin/api/hosts/{host}", get(host::<S>))
        .route("/admin/api/hosts/admissions", get(admissions::<S>))
        .route("/admin/api/hosts/{host}/action", post(host_action::<S>))
        .route("/admin/api/hosts/{host}/release-throttled", post(release_throttled::<S>))
        .route("/admin/api/ops/tail", get(tail::<S>))
        .route("/admin/api/store", get(store::<S>))
        .route("/admin/api/discovery", get(discovery::<S>))
        .route("/admin/api/ops/rejects/top", get(rejects_top::<S>))
        .route("/admin/api/discovery/run", post(discovery_run::<S>))
        .route("/admin/api/policy/usage", get(policy_usage::<S>))
        .route("/admin/api/policy/signals", get(policy_signals::<S>))
        .route("/admin/api/takedowns", get(takedowns::<S>))
        .route("/admin/api/cluster/quorum/history", get(quorum_history::<S>))
        .route("/admin/api/cluster/quorum/flush", post(flush_now::<S>))
        .route("/admin/api/domain-rules", get(rules::<S>).post(create_rule::<S>))
        .route("/admin/api/domain-rules/{id}", axum::routing::put(update_rule::<S>).delete(delete_rule::<S>))
        .route("/admin/api/policy", get(policy::<S>).put(update_policy::<S>))
        .route("/admin/api/policy/audit", get(policy_audit::<S>))
        .route("/admin/api/policy/full", get(full_policy::<S>).put(update_full_policy::<S>))
        .route("/admin/api/domain-rules/audit", get(rules_audit::<S>))
        .route("/admin/api/consumers", get(consumers::<S>))
        .route("/admin/api/consumers/{id}/kick", post(kick::<S>))
        .route("/admin/api/cluster", get(cluster::<S>))
        .route("/admin/api/cluster/quorum", get(quorum::<S>))
        .route("/admin/api/cluster/quorum/members", post(quorum_members::<S>))
        .route("/admin/api/settings", get(settings::<S>))
        .route("/admin/api/policy/defaults", get(policy_defaults))
        .route("/admin/api/ops/plc", get(plc_view::<S>))
        .route("/admin/api/ops/pipeline", get(pipeline_view::<S>))
        .route("/admin/api/accounts", get(accounts::<S>))
        .route("/admin/api/accounts/{did}", get(account::<S>))
        .route("/admin/api/accounts/{did}/takedown", post(takedown::<S>))
        .route("/admin/api/accounts/{did}/untakedown", post(untakedown::<S>))
        .route("/admin/api/cases", get(cases::<S>))
        .route("/admin/api/cases/bulk", post(bulk_cases::<S>))
        .route("/admin/api/cases/{id}", get(case::<S>).post(update_case::<S>))
        .route("/admin/api/cases/{id}/evidence", get(case_detail::<S>))
        .route_layer(middleware::from_fn_with_state(ctx.clone(), auth::<S>))
        .with_state(ctx)
}

async fn auth<S: AdminSource>(State(ctx): State<Arc<Ctx<S>>>, mut req: Request, next: Next) -> Response {
    let actor = match authenticate(&ctx.token, &req) {
        Ok(a) => a,
        Err((status, error, message)) => {
            let body = Json(serde_json::json!({ "error": error, "message": message }));
            return (status, [(header::CACHE_CONTROL, "no-store")], body).into_response();
        }
    };
    req.extensions_mut().insert(actor);
    let mut res = next.run(req).await;
    res.headers_mut().insert(header::CACHE_CONTROL, header::HeaderValue::from_static("no-store"));
    res
}

/// The token whenever the request brings an `Authorization` header (a wrong
/// one is a 401 whatever a proxy said), else the operator the admin
/// listener's proxy named ([`proxy`]).
fn authenticate(token: &str, req: &Request) -> Result<Actor, (StatusCode, &'static str, String)> {
    let unauthorized = || (StatusCode::UNAUTHORIZED, "AuthenticationRequired", "Admin token required".to_string());
    if let Some(h) = req.headers().get(header::AUTHORIZATION) {
        let ok = h
            .to_str()
            .ok()
            .and_then(|h| h.strip_prefix("Basic "))
            .is_some_and(|b| vlatproto::xrpc::basic_admin_ok(b, token));
        return if ok { Ok(Actor::Token) } else { Err(unauthorized()) };
    }
    match req.extensions().get::<proxy::ProxyIdentity>() {
        Some(proxy::ProxyIdentity::Operator(login)) => Ok(Actor::Operator(login.clone())),
        Some(proxy::ProxyIdentity::Refused(why)) => Err((StatusCode::FORBIDDEN, "OperatorRefused", why.clone())),
        None => Err(unauthorized()),
    }
}

/// Who an operator call came from, as the audit trail records it.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Actor {
    /// The admin token: shared, so it names no one.
    Token,
    /// A login the admin listener's proxy named.
    Operator(Arc<str>),
    /// The relay itself, acting on its own (closing a case it opened).
    Service(Arc<str>),
}

impl Actor {
    /// How the caller got in: `token` or `proxy`.
    pub fn auth(&self) -> &'static str {
        match self {
            Actor::Token => "token",
            Actor::Operator(_) => "proxy",
            Actor::Service(_) => "service",
        }
    }

    /// The audit trail's `by`: `admin (token)`, or `<login> (proxy)`. The
    /// server writes both halves, so a token caller can't pass as an operator.
    pub fn label(&self) -> String {
        match self {
            Actor::Token => "admin (token)".into(),
            Actor::Operator(login) => format!("{login} (proxy)"),
            Actor::Service(name) => format!("{name} (service)"),
        }
    }
}

/// How the caller got in: `{"auth": "token"}`, or `{"auth": "proxy",
/// "operator": <login>}`. The console asks first, without a token, to skip
/// its token form.
async fn session(Extension(a): Extension<Actor>) -> Json<serde_json::Value> {
    Json(match &a {
        Actor::Token => serde_json::json!({ "auth": "token" }),
        Actor::Operator(login) => serde_json::json!({ "auth": "proxy", "operator": login.as_ref() }),
        Actor::Service(name) => serde_json::json!({ "auth": "service", "operator": name.as_ref() }),
    })
}

type Ax<S> = State<Arc<Ctx<S>>>;

#[derive(Deserialize)]
struct ChangesQuery {
    since: Option<String>,
}

async fn change_feed<S: AdminSource>(
    State(c): Ax<S>,
    headers: axum::http::HeaderMap,
    Query(q): Query<ChangesQuery>,
) -> Response {
    let Some(feed) = c.src.changes() else {
        return AdminError::NotFound("this relay serves no change feed".into()).into_response();
    };
    let since = headers.get("last-event-id").and_then(|v| v.to_str().ok()).map(str::to_string).or(q.since);
    match feed.sse(since.as_deref()) {
        // a buffering proxy (nginx) would hold events back
        Ok(sse) => ([(header::HeaderName::from_static("x-accel-buffering"), "no")], sse).into_response(),
        Err(changes::TooManyFeeds) => (
            StatusCode::SERVICE_UNAVAILABLE,
            Json(serde_json::json!({
                "error": "TooManyFeeds",
                "message": format!("{} change feeds are open on this node", changes::MAX_FEEDS),
            })),
        )
            .into_response(),
    }
}

async fn overview<S: AdminSource>(State(c): Ax<S>) -> AdminResult<Json<Overview>> {
    Ok(Json(c.src.overview().await?))
}
async fn hosts<S: AdminSource>(State(c): Ax<S>, Query(q): Query<HostQuery>) -> AdminResult<Json<HostList>> {
    if let Some(f) = q.bad_flag() {
        return Err(AdminError::BadRequest(format!("unknown flag {f}")));
    }
    Ok(Json(c.src.hosts(q).await?))
}
async fn host<S: AdminSource>(State(c): Ax<S>, Path(h): Path<String>) -> AdminResult<Json<HostDetail>> {
    Ok(Json(c.src.host(&h).await?))
}
async fn host_action<S: AdminSource>(
    State(c): Ax<S>,
    Extension(a): Extension<Actor>,
    Path(h): Path<String>,
    Json(action): Json<HostAction>,
) -> AdminResult<Json<HostRow>> {
    Ok(Json(c.src.host_action(&h, action, &a.label()).await?))
}
async fn rules<S: AdminSource>(State(c): Ax<S>) -> AdminResult<Json<Vec<DomainRule>>> {
    Ok(Json(c.src.domain_rules().await?))
}
async fn create_rule<S: AdminSource>(
    State(c): Ax<S>,
    Extension(a): Extension<Actor>,
    Json(r): Json<DomainRuleInput>,
) -> AdminResult<Json<DomainRule>> {
    Ok(Json(c.src.create_domain_rule(r, &a.label()).await?))
}
async fn update_rule<S: AdminSource>(
    State(c): Ax<S>,
    Extension(a): Extension<Actor>,
    Path(id): Path<u64>,
    Json(r): Json<DomainRuleInput>,
) -> AdminResult<Json<DomainRule>> {
    Ok(Json(c.src.update_domain_rule(id, r, &a.label()).await?))
}
async fn delete_rule<S: AdminSource>(
    State(c): Ax<S>,
    Extension(a): Extension<Actor>,
    Path(id): Path<u64>,
) -> AdminResult<StatusCode> {
    c.src.delete_domain_rule(id, &a.label()).await?;
    Ok(StatusCode::NO_CONTENT)
}
async fn policy<S: AdminSource>(State(c): Ax<S>) -> AdminResult<Json<PolicyDoc>> {
    Ok(Json(c.src.policy().await?))
}
async fn update_policy<S: AdminSource>(
    State(c): Ax<S>,
    Extension(a): Extension<Actor>,
    Json(u): Json<PolicyUpdate>,
) -> AdminResult<Json<PolicyDoc>> {
    validate_policy(&u.policy).map_err(AdminError::BadRequest)?;
    Ok(Json(c.src.update_policy(u, &a.label()).await?))
}
async fn policy_audit<S: AdminSource>(State(c): Ax<S>) -> AdminResult<Json<Vec<PolicyAudit>>> {
    Ok(Json(c.src.policy_audit().await?))
}
async fn full_policy<S: AdminSource>(State(c): Ax<S>) -> AdminResult<Json<FullPolicyDoc>> {
    Ok(Json(c.src.full_policy().await?))
}
async fn update_full_policy<S: AdminSource>(
    State(c): Ax<S>,
    Extension(a): Extension<Actor>,
    Json(u): Json<FullPolicyUpdate>,
) -> AdminResult<Json<FullPolicyDoc>> {
    Ok(Json(c.src.update_full_policy(u, &a.label()).await?))
}
async fn rules_audit<S: AdminSource>(State(c): Ax<S>) -> AdminResult<Json<Vec<PolicyAudit>>> {
    Ok(Json(c.src.domain_rules_audit().await?))
}
async fn case_detail<S: AdminSource>(State(c): Ax<S>, Path(id): Path<u64>) -> AdminResult<Json<CaseDetail>> {
    Ok(Json(c.src.case_detail(id).await?))
}
async fn consumers<S: AdminSource>(State(c): Ax<S>) -> AdminResult<Json<Vec<Consumer>>> {
    Ok(Json(c.src.consumers().await?))
}
async fn kick<S: AdminSource>(
    State(c): Ax<S>,
    Extension(a): Extension<Actor>,
    Path(id): Path<u64>,
    Query(q): Query<KickQuery>,
) -> AdminResult<StatusCode> {
    let node = q.node.as_deref().filter(|n| !n.is_empty());
    c.src.kick_consumer_on(node, id, &a.label()).await?;
    Ok(StatusCode::NO_CONTENT)
}
async fn admissions<S: AdminSource>(State(c): Ax<S>) -> AdminResult<Json<AdmissionLog>> {
    Ok(Json(c.src.admissions().await?))
}
async fn tail<S: AdminSource>(State(c): Ax<S>, Query(q): Query<TailQuery>) -> AdminResult<Json<Vec<TailFrame>>> {
    if q.host.as_deref().is_none_or(str::is_empty) && q.rejects.unwrap_or(0) == 0 {
        return Err(AdminError::BadRequest("ask for a host's frames (host=) or the rejected ones (rejects=1)".into()));
    }
    Ok(Json(c.src.tail(q).await?))
}
async fn release_throttled<S: AdminSource>(
    State(c): Ax<S>,
    Extension(a): Extension<Actor>,
    Path(h): Path<String>,
) -> AdminResult<Json<Released>> {
    tracing::info!(target: "vlrelay::audit", host = %h, by = %a.label(), auth = a.auth(), "release throttled accounts");
    Ok(Json(c.src.release_throttled(&h, &a.label()).await?))
}
async fn policy_usage<S: AdminSource>(State(c): Ax<S>) -> AdminResult<Json<PolicyUsage>> {
    Ok(Json(c.src.policy_usage().await?))
}
async fn policy_signals<S: AdminSource>(State(c): Ax<S>) -> AdminResult<Json<SignalsView>> {
    Ok(Json(c.src.policy_signals().await?))
}
async fn takedowns<S: AdminSource>(State(c): Ax<S>) -> AdminResult<Json<Vec<crate::policy::takedowns::TakedownEntry>>> {
    Ok(Json(c.src.takedowns().await?))
}
async fn quorum_history<S: AdminSource>(State(c): Ax<S>) -> AdminResult<Json<QuorumHistory>> {
    Ok(Json(c.src.quorum_history().await?))
}
async fn flush_now<S: AdminSource>(
    State(c): Ax<S>,
    Extension(a): Extension<Actor>,
) -> AdminResult<Json<serde_json::Value>> {
    tracing::info!(target: "vlrelay::audit", by = %a.label(), auth = a.auth(), "flush now");
    Ok(Json(c.src.flush_now(&a.label()).await?))
}
async fn rejects_top<S: AdminSource>(
    State(c): Ax<S>,
    Query(q): Query<RejectTopQuery>,
) -> AdminResult<Json<Vec<RejectTop>>> {
    Ok(Json(c.src.rejects_top(q).await?))
}
async fn discovery<S: AdminSource>(State(c): Ax<S>) -> AdminResult<Json<DiscoveryView>> {
    Ok(Json(c.src.discovery().await?))
}
async fn discovery_run<S: AdminSource>(
    State(c): Ax<S>,
    Extension(a): Extension<Actor>,
    body: Option<Json<DiscoveryRun>>,
) -> AdminResult<Json<DiscoveryView>> {
    let req = body.map(|Json(b)| b).unwrap_or_default();
    tracing::info!(target: "vlrelay::audit", source = ?req.source, by = %a.label(), auth = a.auth(), "discovery run");
    Ok(Json(c.src.discovery_run(req, &a.label()).await?))
}
async fn store<S: AdminSource>(State(c): Ax<S>) -> AdminResult<Json<StoreView>> {
    Ok(Json(c.src.store().await?))
}
async fn plc_view<S: AdminSource>(State(c): Ax<S>) -> AdminResult<Json<PlcView>> {
    Ok(Json(c.src.plc_view().await?))
}
async fn pipeline_view<S: AdminSource>(State(c): Ax<S>) -> AdminResult<Json<PipelineView>> {
    Ok(Json(c.src.pipeline_view().await?))
}
async fn cluster<S: AdminSource>(State(c): Ax<S>) -> AdminResult<Json<ClusterView>> {
    Ok(Json(c.src.cluster().await?))
}
async fn quorum<S: AdminSource>(State(c): Ax<S>) -> AdminResult<Json<QuorumView>> {
    Ok(Json(c.src.quorum().await?))
}
async fn quorum_members<S: AdminSource>(
    State(c): Ax<S>,
    Extension(a): Extension<Actor>,
    Json(req): Json<QuorumMembersChange>,
) -> AdminResult<Json<serde_json::Value>> {
    if req.members.is_empty() || req.members.iter().any(|m| m.trim().is_empty()) {
        return Err(AdminError::BadRequest("members must be a non-empty list of node ids".into()));
    }
    tracing::info!(target: "vlrelay::audit", members = ?req.members, by = %a.label(), auth = a.auth(), "quorum members");
    Ok(Json(c.src.change_quorum_members(req, &a.label()).await?))
}
async fn settings<S: AdminSource>(State(c): Ax<S>, Query(q): Query<NodeQuery>) -> AdminResult<Json<SettingsView>> {
    Ok(Json(c.src.settings_of(q.node.as_deref().filter(|n| !n.is_empty())).await?))
}
/// What every policy field is on a fresh relay, for the Tuning page.
async fn policy_defaults() -> AdminResult<Json<serde_json::Value>> {
    Ok(Json(serde_json::to_value(crate::policy::doc::PolicyBody::default()).map_err(anyhow::Error::from)?))
}
async fn accounts<S: AdminSource>(State(c): Ax<S>, Query(q): Query<AccountQuery>) -> AdminResult<Json<Vec<Account>>> {
    Ok(Json(c.src.accounts(q).await?))
}
async fn account<S: AdminSource>(State(c): Ax<S>, Path(did): Path<String>) -> AdminResult<Json<Account>> {
    Ok(Json(c.src.account(&did).await?))
}
async fn takedown<S: AdminSource>(
    State(c): Ax<S>,
    Extension(a): Extension<Actor>,
    Path(did): Path<String>,
    Json(b): Json<ReasonBody>,
) -> AdminResult<Json<Account>> {
    if b.reason.trim().is_empty() {
        return Err(AdminError::BadRequest("a takedown needs a reason".into()));
    }
    Ok(Json(c.src.takedown(&did, b.reason, &a.label()).await?))
}
async fn untakedown<S: AdminSource>(
    State(c): Ax<S>,
    Extension(a): Extension<Actor>,
    Path(did): Path<String>,
) -> AdminResult<Json<Account>> {
    Ok(Json(c.src.untakedown(&did, &a.label()).await?))
}
async fn cases<S: AdminSource>(State(c): Ax<S>, Query(q): Query<CaseQuery>) -> AdminResult<Json<CaseList>> {
    let all = c.src.cases(CaseQuery::default()).await?;
    Ok(Json(case_page(all, &q)))
}
async fn case<S: AdminSource>(State(c): Ax<S>, Path(id): Path<u64>) -> AdminResult<Json<Case>> {
    Ok(Json(c.src.case(id).await?))
}
async fn bulk_cases<S: AdminSource>(
    State(c): Ax<S>,
    Extension(a): Extension<Actor>,
    Json(u): Json<CaseBulkUpdate>,
) -> AdminResult<Json<CaseBulkResult>> {
    Ok(Json(c.src.bulk_update_cases(u, &a.label()).await?))
}
async fn update_case<S: AdminSource>(
    State(c): Ax<S>,
    Extension(a): Extension<Actor>,
    Path(id): Path<u64>,
    Json(u): Json<CaseUpdate>,
) -> AdminResult<Json<Case>> {
    Ok(Json(c.src.update_case(id, u, &a.label()).await?))
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
        if name.is_empty() || !name.chars().all(|c| c.is_ascii_lowercase() || c.is_ascii_digit() || c == '-') {
            return Err(format!("tier name {name:?}: use a-z, 0-9 and -"));
        }
        if !(t.events_per_sec.is_finite() && t.events_per_sec > 0.0) {
            return Err(format!("tiers.{name}.eventsPerSec must be > 0"));
        }
        if (t.events_per_hour as f64) < t.events_per_sec {
            return Err(format!("tiers.{name}.eventsPerHour is below one second's worth"));
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

/// The dashboard's whole app: the operator API, the public stats and the UI.
pub fn app<S: AdminSource>(src: Arc<S>, admin_token: String, ui: Arc<UiFiles>) -> Router {
    api_routes(src.clone(), admin_token).merge(public_routes(src)).merge(ui_routes(ui))
}
