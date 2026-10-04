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

use super::doc::{PolicyBody, tier_name};
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
    /// The last operator actions, newest last.
    #[serde(skip_serializing_if = "Vec::is_empty")]
    pub actions: Vec<serde_json::Value>,
}

pub const ACTIONS_KEPT: usize = 20;
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
    }
    set_host_policy(rec, &s);
    Ok(())
}
