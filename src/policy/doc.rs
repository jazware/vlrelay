//! The policy object (`policy/current.json`): every limit and threshold the
//! relay enforces, as one versioned document.
//!
//! Defaults follow indigo's relay where the reference notes give a number
//! (docs/reference-notes.md, "Limits and policy"), so a fresh cluster behaves
//! like `bsky.network` until an operator says otherwise.

use crate::state::Tier;
use serde::{Deserialize, Serialize};

/// Limits for one tier. `0` means unlimited for the count fields.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", default)]
pub struct TierLimits {
    pub events_per_sec: f64,
    pub events_per_hour: u64,
    pub events_per_day: u64,
    /// Read rate off the host's socket. The reader pauses past it, so the PDS
    /// buffers instead of us dropping.
    pub bytes_per_sec: u64,
    /// Accounts the host may carry. New accounts past it are created
    /// `host-throttled` (indigo's `--default-account-limit`).
    pub max_accounts: u64,
    pub new_accounts_per_hour: u64,
    pub identity_events_per_hour: u64,
    /// Dials per hour, so a flapping host can't keep a worker busy.
    pub reconnects_per_hour: u64,
    /// Whether an error or spam budget may move this tier to `throttled`.
    /// Off for `trusted`: throttling the big PDSes for a buggy minute would
    /// stall most of the network.
    pub auto_throttle: bool,
    /// Archival bootstrap `getRepo` fetches per second from one host. A
    /// first backfill of a big PDS takes days at these rates, on purpose.
    pub archival_fetches_per_host: f64,
}

impl Default for TierLimits {
    fn default() -> Self {
        TierLimits::untrusted(1000)
    }
}

impl TierLimits {
    /// indigo's untrusted host limits for an account limit of `accounts`
    /// (`slurper.go:L178-L191`): 50 + n/1000 per s, 2,500 + n per h,
    /// 20,000 + 10n per day.
    pub fn untrusted(accounts: u64) -> TierLimits {
        TierLimits {
            events_per_sec: 50.0 + accounts as f64 / 1000.0,
            events_per_hour: 2_500 + accounts,
            events_per_day: 20_000 + 10 * accounts,
            bytes_per_sec: 2 * 1024 * 1024,
            max_accounts: accounts,
            new_accounts_per_hour: 100,
            identity_events_per_hour: 1_000,
            reconnects_per_hour: 60,
            auto_throttle: true,
            archival_fetches_per_host: 1.0,
        }
    }

    pub fn trusted() -> TierLimits {
        TierLimits {
            events_per_sec: 5_000.0,
            events_per_hour: 50_000_000,
            events_per_day: 500_000_000,
            bytes_per_sec: 200 * 1024 * 1024,
            max_accounts: 10_000_000,
            new_accounts_per_hour: 0,
            identity_events_per_hour: 0,
            reconnects_per_hour: 0,
            auto_throttle: false,
            archival_fetches_per_host: 5.0,
        }
    }

    pub fn throttled() -> TierLimits {
        TierLimits {
            events_per_sec: 5.0,
            events_per_hour: 2_500,
            events_per_day: 20_000,
            bytes_per_sec: 256 * 1024,
            max_accounts: 100,
            new_accounts_per_hour: 10,
            identity_events_per_hour: 100,
            reconnects_per_hour: 12,
            auto_throttle: true,
            archival_fetches_per_host: 0.1,
        }
    }
}

/// The tiers that carry limits. `suspended` and `banned` have none: the
/// relay doesn't connect to them at all.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", default)]
pub struct Tiers {
    pub trusted: TierLimits,
    pub default: TierLimits,
    pub new: TierLimits,
    pub throttled: TierLimits,
}

impl Default for Tiers {
    fn default() -> Self {
        Tiers {
            trusted: TierLimits::trusted(),
            default: TierLimits::untrusted(1000),
            new: TierLimits { new_accounts_per_hour: 25, ..TierLimits::untrusted(1000) },
            throttled: TierLimits::throttled(),
        }
    }
}

impl Tiers {
    pub fn get(&self, t: Tier) -> Option<&TierLimits> {
        match t {
            Tier::Trusted => Some(&self.trusted),
            Tier::Default => Some(&self.default),
            Tier::New => Some(&self.new),
            Tier::Throttled => Some(&self.throttled),
            Tier::Suspended | Tier::Banned => None,
        }
    }

    pub fn get_mut(&mut self, t: Tier) -> Option<&mut TierLimits> {
        match t {
            Tier::Trusted => Some(&mut self.trusted),
            Tier::Default => Some(&mut self.default),
            Tier::New => Some(&mut self.new),
            Tier::Throttled => Some(&mut self.throttled),
            Tier::Suspended | Tier::Banned => None,
        }
    }
}

pub const LIMIT_TIERS: [Tier; 4] = [Tier::Trusted, Tier::Default, Tier::New, Tier::Throttled];

pub fn tier_name(t: Tier) -> &'static str {
    match t {
        Tier::Trusted => "trusted",
        Tier::Default => "default",
        Tier::New => "new",
        Tier::Throttled => "throttled",
        Tier::Suspended => "suspended",
        Tier::Banned => "banned",
    }
}

pub fn parse_tier(s: &str) -> Option<Tier> {
    Some(match s {
        "trusted" => Tier::Trusted,
        "default" => Tier::Default,
        "new" => Tier::New,
        "throttled" => Tier::Throttled,
        "suspended" => Tier::Suspended,
        "banned" => Tier::Banned,
        _ => return None,
    })
}

/// How hosts move between tiers on their own (`policy::tiers`).
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", default)]
pub struct Transitions {
    /// `new` → `default` once a host is this old and has had no trip for as
    /// long.
    pub promote_after_days: u32,
    /// An auto-throttled host goes back to its tier after this long without
    /// a trip.
    pub recover_after_secs: u32,
    /// Error budget: rejected / all frames over one driver interval.
    pub error_ratio: f64,
    /// Fewer frames than this in an interval never trip the error budget, so
    /// one bad commit from a tiny PDS doesn't throttle it.
    pub error_min_events: u64,
}

impl Default for Transitions {
    fn default() -> Self {
        Transitions { promote_after_days: 7, recover_after_secs: 3_600, error_ratio: 0.5, error_min_events: 200 }
    }
}

/// What crossing a spam threshold does.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub enum SpamAction {
    /// Log and count it.
    Alert,
    /// Open (or update) a case for an operator.
    Case,
    /// Move the host to `throttled` (if its tier allows auto-throttling).
    Throttle,
    ThrottleAndCase,
}

impl SpamAction {
    pub fn throttles(self) -> bool {
        matches!(self, SpamAction::Throttle | SpamAction::ThrottleAndCase)
    }
    pub fn opens_case(self) -> bool {
        matches!(self, SpamAction::Case | SpamAction::ThrottleAndCase)
    }
}

/// `limit` events per `window_secs`, counted per key. `limit` 0 turns it off.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct Threshold {
    pub limit: f64,
    pub window_secs: u32,
    pub action: SpamAction,
}

impl Threshold {
    pub fn enabled(&self) -> bool {
        self.limit > 0.0 && self.window_secs > 0
    }
}

/// The spam signals and when they trip. Per-host thresholds are counted by
/// the host owner, per-DID ones by the DID owner. A per-DID threshold that
/// throttles throttles the DID's host, since spam accounts come in groups.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", default)]
pub struct Spam {
    pub host_new_accounts: Threshold,
    pub account_records: Threshold,
    pub host_failed_validation: Threshold,
    pub account_failed_validation: Threshold,
    pub host_identity_churn: Threshold,
    pub account_identity_churn: Threshold,
    pub host_oversized_commits: Threshold,
    /// Keys tracked per signal on each node. Only the heaviest keys are
    /// tracked, so memory stays fixed however many hosts or DIDs are noisy.
    pub track_hosts: u32,
    pub track_accounts: u32,
}

impl Default for Spam {
    fn default() -> Self {
        let t = |limit: f64, window_secs: u32, action| Threshold { limit, window_secs, action };
        Spam {
            host_new_accounts: t(300.0, 3_600, SpamAction::ThrottleAndCase),
            account_records: t(600.0, 60, SpamAction::Case),
            host_failed_validation: t(600.0, 60, SpamAction::ThrottleAndCase),
            account_failed_validation: t(60.0, 60, SpamAction::Case),
            host_identity_churn: t(2_000.0, 3_600, SpamAction::Case),
            account_identity_churn: t(20.0, 3_600, SpamAction::Case),
            host_oversized_commits: t(100.0, 3_600, SpamAction::Alert),
            track_hosts: 1_024,
            track_accounts: 8_192,
        }
    }
}

/// Cluster-wide budgets. The fast ones are split evenly over live nodes
/// (`Engine::budget`); `new_hosts_per_day` is a counter object in the
/// bucket shared by every node.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", default)]
pub struct Cluster {
    pub plc_lookups_per_sec: f64,
    pub new_accounts_per_min: f64,
    pub new_hosts_per_day: u32,
    /// Archival `getRepo` fetches in flight, and their bytes per second,
    /// across the cluster.
    pub archival_fetch_concurrency: u32,
    pub archival_fetch_bytes_per_sec: u64,
}

impl Default for Cluster {
    fn default() -> Self {
        Cluster {
            plc_lookups_per_sec: 500.0,
            new_accounts_per_min: 6_000.0,
            new_hosts_per_day: 50,
            archival_fetch_concurrency: 32,
            archival_fetch_bytes_per_sec: 50 * 1024 * 1024,
        }
    }
}

/// Enforced by the serving node.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", default)]
pub struct Consumers {
    pub connections_per_ip: u32,
    pub consumers_per_node: u32,
    /// A consumer this far behind live is cut off.
    pub slow_consumer_lag_secs: u32,
    /// Replay requests further back than this get `OutdatedCursor`.
    pub max_backfill_secs: u32,
}

impl Default for Consumers {
    fn default() -> Self {
        Consumers {
            connections_per_ip: 16,
            consumers_per_node: 2_000,
            slow_consumer_lag_secs: 600,
            max_backfill_secs: 72 * 3_600,
        }
    }
}

/// requestCrawl admission (indigo's `handlers.go:L19-L69` checks, minus the
/// `describeServer` call, which the upstream module makes).
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", default)]
pub struct Crawl {
    /// Public requestCrawl. Operators can always add hosts.
    pub enabled: bool,
    /// Only hosts an `allow` rule or a trusted domain covers get in.
    pub allowlist_only: bool,
    pub allow_insecure: bool,
    /// `*.host.bsky.network` style patterns. A host they match starts
    /// `trusted` (decided at admission, as indigo does).
    pub trusted_domains: Vec<String>,
    /// The tier every other admitted host starts in.
    pub initial_tier: Tier,
}

impl Default for Crawl {
    fn default() -> Self {
        Crawl {
            enabled: true,
            allowlist_only: false,
            allow_insecure: false,
            trusted_domains: vec!["*.host.bsky.network".into()],
            initial_tier: Tier::New,
        }
    }
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize, Default)]
#[serde(rename_all = "camelCase", default)]
pub struct PolicyBody {
    pub tiers: Tiers,
    pub transitions: Transitions,
    pub spam: Spam,
    pub cluster: Cluster,
    pub consumers: Consumers,
    pub crawl: Crawl,
    pub archive: Archive,
}

/// Which accounts an archival relay mirrors. Off by default (PLAN.md
/// decision 6). The relay has no archival mode, so nothing reads it.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize, Default)]
#[serde(rename_all = "lowercase")]
pub enum ArchiveMode {
    #[default]
    Off,
    All,
    /// Accounts on hosts of the listed tiers.
    Tiers,
    /// Accounts on the listed hosts.
    Hosts,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", default)]
pub struct Archive {
    pub mode: ArchiveMode,
    pub tiers: Vec<Tier>,
    pub hosts: Vec<String>,
    /// A taken-down account's mirror stops serving at once and is deleted
    /// this long after the takedown (an untakedown before then keeps it).
    pub takedown_retention_hours: u32,
}

impl Default for Archive {
    fn default() -> Self {
        Archive { mode: ArchiveMode::Off, tiers: Vec::new(), hosts: Vec::new(), takedown_retention_hours: 72 }
    }
}

impl Archive {
    /// Whether an account on `host` (running at `tier`) is mirrored.
    pub fn wants(&self, host: &str, tier: Option<Tier>) -> bool {
        match self.mode {
            ArchiveMode::Off => false,
            ArchiveMode::All => true,
            ArchiveMode::Tiers => tier.is_some_and(|t| self.tiers.contains(&t)),
            ArchiveMode::Hosts => self.hosts.iter().any(|h| h.eq_ignore_ascii_case(host)),
        }
    }
}

pub fn validate(p: &PolicyBody) -> Result<(), Vec<String>> {
    let mut errs = Vec::new();
    for t in LIMIT_TIERS {
        let l = p.tiers.get(t).expect("limit tier");
        let n = tier_name(t);
        if !(l.events_per_sec.is_finite() && l.events_per_sec > 0.0) {
            errs.push(format!("tiers.{n}.eventsPerSec must be > 0"));
        }
        if l.events_per_hour != 0 && (l.events_per_hour as f64) < l.events_per_sec {
            errs.push(format!("tiers.{n}.eventsPerHour is below one second's worth"));
        }
        if !(l.archival_fetches_per_host.is_finite() && l.archival_fetches_per_host > 0.0) {
            errs.push(format!("tiers.{n}.archivalFetchesPerHost must be > 0"));
        }
        if l.events_per_day != 0 && l.events_per_day < l.events_per_hour {
            errs.push(format!("tiers.{n}.eventsPerDay is below eventsPerHour"));
        }
    }
    let tr = &p.transitions;
    if !(0.0..=1.0).contains(&tr.error_ratio) || tr.error_ratio == 0.0 {
        errs.push("transitions.errorRatio must be in (0, 1]".into());
    }
    let s = &p.spam;
    for (name, t) in [
        ("hostNewAccounts", &s.host_new_accounts),
        ("accountRecords", &s.account_records),
        ("hostFailedValidation", &s.host_failed_validation),
        ("accountFailedValidation", &s.account_failed_validation),
        ("hostIdentityChurn", &s.host_identity_churn),
        ("accountIdentityChurn", &s.account_identity_churn),
        ("hostOversizedCommits", &s.host_oversized_commits),
    ] {
        if !t.limit.is_finite() || t.limit < 0.0 {
            errs.push(format!("spam.{name}.limit must be ≥ 0"));
        }
        if t.limit > 0.0 && t.window_secs == 0 {
            errs.push(format!("spam.{name}.windowSecs must be > 0"));
        }
        if t.window_secs > 86_400 {
            errs.push(format!("spam.{name}.windowSecs is over a day"));
        }
    }
    if !(16..=1 << 20).contains(&s.track_hosts) || !(16..=1 << 20).contains(&s.track_accounts) {
        errs.push("spam.trackHosts and spam.trackAccounts must be 16..1048576".into());
    }
    let c = &p.cluster;
    if !(c.plc_lookups_per_sec.is_finite() && c.plc_lookups_per_sec > 0.0)
        || !(c.new_accounts_per_min.is_finite() && c.new_accounts_per_min > 0.0)
    {
        errs.push("cluster budgets must be > 0".into());
    }
    if c.archival_fetch_concurrency == 0 || c.archival_fetch_bytes_per_sec == 0 {
        errs.push("cluster.archivalFetchConcurrency and archivalFetchBytesPerSec must be > 0".into());
    }
    if p.archive.mode == ArchiveMode::Tiers && p.archive.tiers.is_empty() {
        errs.push("archive.tiers is empty with mode tiers".into());
    }
    if p.archive.mode == ArchiveMode::Hosts && p.archive.hosts.is_empty() {
        errs.push("archive.hosts is empty with mode hosts".into());
    }
    if matches!(p.crawl.initial_tier, Tier::Throttled | Tier::Suspended | Tier::Banned) {
        errs.push("crawl.initialTier must be trusted, default or new".into());
    }
    for d in &p.crawl.trusted_domains {
        if let Err(e) = super::rules::normalize_pattern(d) {
            errs.push(format!("crawl.trustedDomains: {e}"));
        }
    }
    if errs.is_empty() { Ok(()) } else { Err(errs) }
}
