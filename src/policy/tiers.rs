//! Host tiers and how hosts move between them.
//!
//! [`step`] is the whole state machine. It takes the host's tier, its policy
//! state, what happened since the last step, the policy and the time, and
//! returns the change (if any). Nothing in it reads a clock or a store, so
//! every transition is a unit test.
//!
//! - `new` → `default` after `promote_after_days` with no trip in that time.
//! - `new`, `default` (and `trusted`, if its tier allows) → `throttled` when
//!   a host crosses its error budget or a spam threshold whose action
//!   throttles. The host remembers where it came from.
//! - An auto-throttled host goes back after `recover_after_secs` without a
//!   trip. One an operator throttled stays until an operator moves it.
//! - `suspended` and `banned` are set and lifted by operators only.
//!
//! The policy state lives in the host record under `extra.policy`, so the
//! state module's record format doesn't change and old records read as
//! "never tripped".

use super::doc::{PolicyBody, SeedAccounts, TierLimits, tier_name};
use crate::admin::{HostAction, HostActionRecord};
use crate::state::{HostRecord, Tier};
use serde::{Deserialize, Serialize};

#[derive(Clone, Debug, Default, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", default)]
pub struct HostPolicy {
    /// Set while auto-throttled (or suspended/banned): the tier to go back
    /// to.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub restore_tier: Option<Tier>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub throttled_at: Option<u32>,
    /// Last time the host crossed a budget (unix seconds).
    #[serde(skip_serializing_if = "Option::is_none")]
    pub last_trip: Option<u32>,
    pub trips: u32,
    /// Why the tier last changed.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub reason: Option<String>,
    /// Operator throttle (events/s) on top of the tier.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub throttle_eps: Option<f64>,
    /// Operator account cap in place of the tier's.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub max_accounts: Option<u64>,
    /// Set while the host is another name for a PDS the relay reads under
    /// `of`: no socket, and `of`'s events speak for its accounts.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub alias: Option<Alias>,
    /// An operator said this host is its own PDS: the relay never marks it
    /// an alias.
    #[serde(skip_serializing_if = "std::ops::Not::not")]
    pub not_alias: bool,
    /// An operator's choice for this host with no saved cursor: read
    /// from cursor 0 (true) or the live head (false). None: the node's
    /// `--backfill-new-hosts`.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub backfill: Option<bool>,
    /// The account count a seed relay's `listHosts` last reported.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub seeded: Option<Seeded>,
    /// The last tier actions, the operators' and the relay's own, newest
    /// last.
    #[serde(skip_serializing_if = "Vec::is_empty")]
    pub actions: Vec<serde_json::Value>,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct Alias {
    pub of: String,
    /// When the relay last confirmed it (unix seconds): it checks again a
    /// day later. An operator's alias isn't rechecked.
    pub at: u32,
    #[serde(default, skip_serializing_if = "std::ops::Not::not")]
    pub by_operator: bool,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct Seeded {
    pub accounts: u64,
    /// The discovery source that reported it (`bootstrap:<relay>`).
    pub from: String,
    /// When it was last reported (unix seconds).
    pub at: u32,
}

impl Seeded {
    pub fn fresh(&self, sa: &SeedAccounts, now: u32) -> bool {
        (now.saturating_sub(self.at) as u64) < sa.ttl_secs
    }
}

/// The seed to write for a host `from` reports `accounts` for. None:
/// nothing to write (seeding is off, nothing reported, another relay's
/// fresh report is higher, or the stored one is close and recent enough).
pub fn seed_update(cur: Option<&Seeded>, accounts: u64, from: &str, sa: &SeedAccounts, now: u32) -> Option<Seeded> {
    if !sa.enabled || accounts == 0 {
        return None;
    }
    if let Some(c) = cur.filter(|c| c.fresh(sa, now)) {
        if c.from != from && c.accounts >= accounts {
            return None;
        }
        // a host's count moves every day: rewriting it on every run would
        // be a host-table write per host per refresh
        let close = c.accounts.abs_diff(accounts) * 10 <= c.accounts;
        if c.from == from && close && (now.saturating_sub(c.at) as u64) < sa.ttl_secs / 2 {
            return None;
        }
    }
    Some(Seeded { accounts, from: from.to_string(), at: now })
}

/// Raises `l` as indigo's untrusted formula would for `n` more accounts:
/// n/1000 a second, n an hour, 10n a day, n on the cap. 0 (unlimited)
/// stays unlimited.
pub fn add_seeded(l: &mut TierLimits, n: u64) {
    l.events_per_sec += n as f64 / 1000.0;
    if l.events_per_hour > 0 {
        l.events_per_hour = l.events_per_hour.saturating_add(n);
    }
    if l.events_per_day > 0 {
        l.events_per_day = l.events_per_day.saturating_add(n.saturating_mul(10));
    }
    if l.max_accounts > 0 {
        l.max_accounts = l.max_accounts.saturating_add(n);
    }
}

/// How far an alias of an alias is followed to the host the relay reads.
pub const ALIAS_HOPS: usize = 4;

pub const ACTIONS_KEPT: usize = 20;
/// Who the trail says moved a host when the relay did it on its own.
pub const RELAY_ACTOR: &str = "relay (service)";
const EXTRA_KEY: &str = "policy";

pub fn host_policy(rec: &HostRecord) -> HostPolicy {
    rec.extra.get(EXTRA_KEY).and_then(|v| serde_json::from_value(v.clone()).ok()).unwrap_or_default()
}

pub fn set_host_policy(rec: &mut HostRecord, p: &HostPolicy) {
    if *p == HostPolicy::default() {
        rec.extra.remove(EXTRA_KEY);
    } else {
        rec.extra.insert(EXTRA_KEY.into(), serde_json::to_value(p).expect("serializable"));
    }
}

/// Appends `a` to the host's action trail, keeping the newest
/// [`ACTIONS_KEPT`].
pub fn record_action(rec: &mut HostRecord, a: &HostActionRecord) {
    let mut s = host_policy(rec);
    s.actions.push(serde_json::to_value(a).expect("serializable"));
    let drop = s.actions.len().saturating_sub(ACTIONS_KEPT);
    s.actions.drain(..drop);
    set_host_policy(rec, &s);
}

/// The trail entry the relay writes for a tier change of its own.
pub fn relay_action(to: Tier, reason: &str, at_ms: i64) -> HostActionRecord {
    HostActionRecord {
        at_ms,
        by: RELAY_ACTOR.into(),
        action: HostAction::SetTier { tier: tier_name(to).into() },
        reason: Some(reason.into()),
        case: None,
    }
}

/// Points the relay's trail entry at `at_ms` to `case`. False if the entry
/// has left the trail (or never was in it).
pub fn link_case(rec: &mut HostRecord, at_ms: i64, case: u64) -> bool {
    let mut s = host_policy(rec);
    let Some(v) = s.actions.iter_mut().rev().find(|v| {
        serde_json::from_value::<HostActionRecord>((*v).clone())
            .is_ok_and(|a| a.at_ms == at_ms && a.by == RELAY_ACTOR && a.case.is_none())
    }) else {
        return false;
    };
    v["case"] = case.into();
    set_host_policy(rec, &s);
    true
}

/// What happened to a host since its last step.
#[derive(Clone, Debug, Default, PartialEq)]
pub struct Obs {
    /// Frames accepted.
    pub events: u64,
    /// Frames rejected by a check.
    pub failed: u64,
    /// A spam threshold whose action throttles, by rule name.
    pub spam_trip: Option<String>,
}

#[derive(Clone, Debug, PartialEq)]
pub struct Change {
    pub tier: Tier,
    pub state: HostPolicy,
    /// Set when the tier changed.
    pub reason: Option<String>,
}

/// The error budget's verdict on an interval, if it tripped.
pub fn error_trip(obs: &Obs, p: &PolicyBody) -> Option<String> {
    let total = obs.events + obs.failed;
    let t = &p.transitions;
    if total == 0 || total < t.error_min_events {
        return None;
    }
    let ratio = obs.failed as f64 / total as f64;
    (ratio >= t.error_ratio).then(|| {
        format!("{:.0}% of {total} frames failed checks (budget {:.0}%)", ratio * 100.0, t.error_ratio * 100.0)
    })
}

/// One host, one step. None: nothing to write.
pub fn step(tier: Tier, first_seen: u32, st: &HostPolicy, obs: &Obs, p: &PolicyBody, now: u32) -> Option<Change> {
    if matches!(tier, Tier::Suspended | Tier::Banned) {
        return None;
    }
    let trip = obs.spam_trip.as_ref().map(|r| format!("spam threshold {r}")).or_else(|| error_trip(obs, p));
    if let Some(why) = trip {
        let mut s = st.clone();
        s.last_trip = Some(now);
        s.trips = s.trips.saturating_add(1);
        let can = p.tiers.get(tier).is_some_and(|l| l.auto_throttle);
        if tier != Tier::Throttled && can {
            s.restore_tier = Some(tier);
            s.throttled_at = Some(now);
            s.reason = Some(format!("auto-throttled from {}: {why}", tier_name(tier)));
            return Some(Change { tier: Tier::Throttled, reason: s.reason.clone(), state: s });
        }
        // Already throttled (the quiet period restarts) or exempt.
        return Some(Change { tier, state: s, reason: None });
    }
    let quiet_for = |secs: u64| st.last_trip.is_none_or(|t| now.saturating_sub(t) as u64 >= secs);
    if tier == Tier::Throttled {
        if let Some(back) = st.restore_tier
            && quiet_for(p.transitions.recover_after_secs as u64)
        {
            let mut s = st.clone();
            s.restore_tier = None;
            s.throttled_at = None;
            s.reason = Some(format!(
                "recovered to {} after {} s without a trip",
                tier_name(back),
                p.transitions.recover_after_secs
            ));
            return Some(Change { tier: back, reason: s.reason.clone(), state: s });
        }
        return None;
    }
    if tier == Tier::New {
        let days = p.transitions.promote_after_days as u64 * 86_400;
        if now.saturating_sub(first_seen) as u64 >= days && quiet_for(days) {
            let mut s = st.clone();
            s.reason = Some(format!("promoted to default after {} clean days", p.transitions.promote_after_days));
            return Some(Change { tier: Tier::Default, reason: s.reason.clone(), state: s });
        }
    }
    None
}

/// What an operator can do to a host's tier. `Reconnect` isn't here: it's
/// the upstream module's.
#[derive(Clone, Debug, PartialEq)]
pub enum Manual {
    SetTier(Tier),
    Throttle(Option<f64>),
    AccountLimit(Option<u64>),
    Suspend(String),
    Ban(String),
    /// Lifts a suspension or a ban.
    Unban,
    /// Marks the host another name for `of`'s PDS (`by_operator`: pinned,
    /// never rechecked).
    Alias {
        of: String,
        by_operator: bool,
    },
    /// Clears an alias; `pin` also keeps the relay from finding it again.
    Unalias {
        pin: bool,
    },
    /// The host's backfill choice (None: back to the node's default).
    Backfill(Option<bool>),
}

/// Applies an operator action to a record. Errors are the operator's (a
/// bad tier, an unban of a host that isn't banned).
pub fn apply_manual(rec: &mut HostRecord, m: &Manual, now: u32) -> Result<(), String> {
    let mut s = host_policy(rec);
    match m {
        Manual::SetTier(t) => {
            if matches!(t, Tier::Suspended | Tier::Banned) {
                return Err("use suspend or ban for those".into());
            }
            // An operator's tier is final: no auto-recovery to an older one.
            s.restore_tier = None;
            s.throttled_at = (*t == Tier::Throttled).then_some(now);
            s.reason = Some(format!("set to {} by an operator", tier_name(*t)));
            rec.tier = *t;
        }
        Manual::Throttle(eps) => {
            if let Some(x) = eps
                && !(x.is_finite() && *x >= 0.0)
            {
                return Err("throttle must be ≥ 0 events/s".into());
            }
            s.throttle_eps = *eps;
        }
        Manual::AccountLimit(n) => s.max_accounts = *n,
        Manual::Suspend(why) | Manual::Ban(why) => {
            let to = if matches!(m, Manual::Ban(_)) { Tier::Banned } else { Tier::Suspended };
            if !matches!(rec.tier, Tier::Suspended | Tier::Banned) {
                s.restore_tier = Some(match (rec.tier, s.restore_tier) {
                    (Tier::Throttled, Some(r)) => r,
                    (t, _) => t,
                });
            }
            s.reason = Some(format!("{} by an operator: {why}", tier_name(to)));
            rec.tier = to;
        }
        Manual::Unban => {
            if !matches!(rec.tier, Tier::Suspended | Tier::Banned) {
                return Err(format!("{} is {}, not suspended or banned", rec.hostname, tier_name(rec.tier)));
            }
            rec.tier = s.restore_tier.take().unwrap_or(Tier::Default);
            s.throttled_at = None;
            s.reason = Some(format!("restored to {} by an operator", tier_name(rec.tier)));
        }
        Manual::Alias { of, by_operator } => {
            if of.eq_ignore_ascii_case(&rec.hostname) {
                return Err(format!("{} can't be an alias of itself", rec.hostname));
            }
            if s.not_alias && !by_operator {
                return Err(format!("an operator marked {} not an alias", rec.hostname));
            }
            s.not_alias = false;
            s.alias = Some(Alias { of: of.clone(), at: now, by_operator: *by_operator });
        }
        Manual::Unalias { pin } => {
            s.alias = None;
            s.not_alias = *pin;
        }
        Manual::Backfill(b) => s.backfill = *b,
    }
    set_host_policy(rec, &s);
    Ok(())
}
