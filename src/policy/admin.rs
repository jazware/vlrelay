//! The policy half of the operator API, over the real engine. The relay's
//! `AdminSource` delegates these calls here: policy get/put, domain rules,
//! cases and host tier actions. Live numbers (rates, consumers, the
//! cluster) come from the other modules.
//!
//! The dashboard's `Policy` wire type is a subset of the engine's policy
//! (five limits per tier and four spam thresholds). A PUT through it
//! changes those fields and keeps every other one, so an older UI can't
//! wipe settings it doesn't show. [`PolicyAdmin::full_policy`] and
//! [`PolicyAdmin::update_full_policy`] carry the whole document.

use super::doc::{self, LIMIT_TIERS, PolicyBody, SpamAction, parse_tier, tier_name};
use super::rules::{self, Rule, RuleSet};
use super::store::{AuditEntry, SaveError, Stored, now_ms};
use super::tiers::{self, Manual};
use super::{Engine, RuleEffect};
use crate::admin::{
    self as wire, AdminError, AdminResult, CaseQuery, CaseUpdate, DomainRuleInput, HostAction, HostActionRecord,
};
use crate::state::{HostRecord, HostStore, Tier};
use std::collections::BTreeMap;
use std::sync::Arc;

const AUDIT_LIMIT: usize = 200;

pub struct PolicyAdmin {
    pub engine: Arc<Engine>,
    pub hosts: Arc<dyn HostStore>,
}

fn save_err(e: SaveError) -> AdminError {
    match e {
        SaveError::Invalid(v) => AdminError::BadRequest(v.join("; ")),
        SaveError::NoChange => AdminError::BadRequest("nothing changed".into()),
        c @ SaveError::Conflict { .. } => AdminError::Conflict(c.to_string()),
        SaveError::Store(s) => AdminError::Internal(anyhow::anyhow!(s)),
        u @ SaveError::Unavailable(_) => AdminError::Unavailable(u.to_string()),
    }
}

fn wire_tier(l: &doc::TierLimits) -> wire::TierLimits {
    wire::TierLimits {
        events_per_sec: l.events_per_sec,
        events_per_hour: l.events_per_hour,
        events_per_day: l.events_per_day,
        max_accounts: l.max_accounts,
        new_accounts_per_hour: l.new_accounts_per_hour,
    }
}

/// Per-window limit → per-unit rate.
fn rate(t: &doc::Threshold, unit_secs: f64) -> f64 {
    if t.window_secs == 0 {
        return 0.0;
    }
    t.limit * unit_secs / t.window_secs as f64
}

fn set_rate(t: &mut doc::Threshold, v: f64, unit_secs: f64) {
    t.limit = v * t.window_secs.max(1) as f64 / unit_secs;
}

pub fn to_wire(p: &PolicyBody) -> wire::Policy {
    let s = &p.spam;
    wire::Policy {
        tiers: LIMIT_TIERS.iter().map(|&t| (tier_name(t).to_string(), wire_tier(p.tiers.get(t).unwrap()))).collect(),
        default_tier: tier_name(p.crawl.initial_tier).to_string(),
        spam: wire::SpamThresholds {
            new_accounts_per_hour: rate(&s.host_new_accounts, 3_600.0).round() as u64,
            reject_ratio: p.transitions.error_ratio,
            bad_signatures_per_min: rate(&s.host_failed_validation, 60.0).round() as u64,
            account_events_per_sec: rate(&s.account_records, 1.0),
            auto_throttle: s.host_new_accounts.action.throttles() || s.host_failed_validation.action.throttles(),
        },
    }
}

/// Folds a wire policy into the full one.
pub fn merge_wire(base: &PolicyBody, w: &wire::Policy) -> Result<PolicyBody, String> {
    let mut p = base.clone();
    for (name, wl) in &w.tiers {
        let t = parse_tier(name)
            .filter(|t| LIMIT_TIERS.contains(t))
            .ok_or_else(|| format!("tier {name:?}: the tiers are trusted, default, new and throttled"))?;
        let l = p.tiers.get_mut(t).unwrap();
        l.events_per_sec = wl.events_per_sec;
        l.events_per_hour = wl.events_per_hour;
        l.events_per_day = wl.events_per_day;
        l.max_accounts = wl.max_accounts;
        l.new_accounts_per_hour = wl.new_accounts_per_hour;
    }
    p.crawl.initial_tier =
        parse_tier(&w.default_tier).ok_or_else(|| format!("default tier {:?} is not a tier", w.default_tier))?;
    let s = &w.spam;
    set_rate(&mut p.spam.host_new_accounts, s.new_accounts_per_hour as f64, 3_600.0);
    set_rate(&mut p.spam.host_failed_validation, s.bad_signatures_per_min as f64, 60.0);
    set_rate(&mut p.spam.account_records, s.account_events_per_sec, 1.0);
    p.transitions.error_ratio = s.reject_ratio;
    for t in [&mut p.spam.host_new_accounts, &mut p.spam.host_failed_validation] {
        t.action = match (s.auto_throttle, t.action) {
            (true, SpamAction::Case) => SpamAction::ThrottleAndCase,
            (true, SpamAction::Alert) => SpamAction::Throttle,
            (false, SpamAction::ThrottleAndCase) => SpamAction::Case,
            (false, SpamAction::Throttle) => SpamAction::Alert,
            (_, a) => a,
        };
    }
    Ok(p)
}

fn wire_doc(d: &Stored<PolicyBody>) -> wire::PolicyDoc {
    wire::PolicyDoc {
        version: d.version,
        policy: to_wire(&d.body),
        updated_at_ms: d.updated_at_ms,
        updated_by: d.updated_by.clone(),
    }
}

fn wire_audit(a: AuditEntry) -> wire::PolicyAudit {
    wire::PolicyAudit { version: a.version, at_ms: a.at_ms, by: a.by, note: a.note, changes: a.changes }
}

pub fn effect_to_wire(e: &RuleEffect) -> wire::RuleEffect {
    match e {
        RuleEffect::Ban => wire::RuleEffect::Ban,
        RuleEffect::Allow => wire::RuleEffect::Allow,
        RuleEffect::Tier { tier } => wire::RuleEffect::Tier { tier: tier_name(*tier).to_string() },
        RuleEffect::Throttle { events_per_sec } => wire::RuleEffect::Throttle { events_per_sec: *events_per_sec },
    }
}

pub fn effect_from_wire(e: &wire::RuleEffect) -> AdminResult<RuleEffect> {
    Ok(match e {
        wire::RuleEffect::Ban => RuleEffect::Ban,
        wire::RuleEffect::Allow => RuleEffect::Allow,
        wire::RuleEffect::Tier { tier } => RuleEffect::Tier {
            tier: parse_tier(tier)
                .filter(|t| LIMIT_TIERS.contains(t))
                .ok_or_else(|| AdminError::BadRequest(format!("no tier {tier:?}")))?,
        },
        wire::RuleEffect::Throttle { events_per_sec } => RuleEffect::Throttle { events_per_sec: *events_per_sec },
    })
}

impl PolicyAdmin {
    pub fn new(engine: Arc<Engine>, hosts: Arc<dyn HostStore>) -> PolicyAdmin {
        PolicyAdmin { engine, hosts }
    }

    // ------------------------------------------------------------ policy

    pub async fn policy(&self) -> AdminResult<wire::PolicyDoc> {
        Ok(wire_doc(&self.engine.policy()))
    }

    pub async fn update_policy(&self, u: wire::PolicyUpdate, by: &str) -> AdminResult<wire::PolicyDoc> {
        // Merge onto the version the operator edited, so fields the wire
        // type lacks come from that version too. The refresh catches this
        // node up if the edit was read from a peer that saw a newer save.
        if let Err(e) = self.engine.refresh().await {
            tracing::warn!("policy refresh before save: {e:#}");
        }
        let cur = self.engine.policy();
        if u.base_version != cur.version {
            return Err(AdminError::Conflict(format!(
                "the policy is at version {} (you edited {}): reload and reapply your change",
                cur.version, u.base_version
            )));
        }
        let body = merge_wire(&cur.body, &u.policy).map_err(AdminError::BadRequest)?;
        let d = self.engine.save_policy(u.base_version, body, by, &u.note).await.map_err(save_err)?;
        Ok(wire_doc(&d))
    }

    pub async fn full_policy(&self) -> Stored<PolicyBody> {
        self.engine.policy()
    }

    pub async fn update_full_policy(
        &self,
        base_version: u64,
        body: PolicyBody,
        note: &str,
        by: &str,
    ) -> AdminResult<Stored<PolicyBody>> {
        self.engine.save_policy(base_version, body, by, note).await.map_err(save_err)
    }

    pub async fn policy_audit(&self) -> AdminResult<Vec<wire::PolicyAudit>> {
        Ok(self.engine.policy_audit(AUDIT_LIMIT).await?.into_iter().map(wire_audit).collect())
    }

    // ------------------------------------------------------------ domain rules

    /// Known hosts each rule matches (the dashboard's `matches`). One scan
    /// of the host records this node can list.
    async fn match_counts(&self, set: &RuleSet) -> AdminResult<BTreeMap<u64, u32>> {
        let compiled = rules::Compiled::new(set.clone());
        let mut out = BTreeMap::new();
        let mut cursor: Option<String> = None;
        loop {
            let page = self.hosts.list_hosts(cursor.as_deref(), 1_000).await?;
            for h in &page.hosts {
                if let Some(r) = compiled.lookup(&h.hostname) {
                    *out.entry(r.id).or_insert(0) += 1;
                }
            }
            match page.cursor {
                Some(c) => cursor = Some(c),
                None => return Ok(out),
            }
        }
    }

    fn rule_view(r: &Rule, matches: &BTreeMap<u64, u32>, version: u64) -> wire::DomainRule {
        wire::DomainRule {
            id: r.id,
            pattern: r.pattern.clone(),
            effect: effect_to_wire(&r.effect),
            note: r.note.clone(),
            created_at_ms: r.created_at_ms,
            created_by: r.created_by.clone(),
            matches: matches.get(&r.id).copied().unwrap_or(0),
            version,
        }
    }

    pub async fn domain_rules(&self) -> AdminResult<Vec<wire::DomainRule>> {
        let (version, set) = self.engine.rules();
        let m = self.match_counts(&set).await?;
        Ok(set.rules.iter().map(|r| Self::rule_view(r, &m, version)).collect())
    }

    async fn edit_rules(
        &self,
        by: &str,
        note: &str,
        f: impl FnOnce(&mut RuleSet) -> AdminResult<u64>,
    ) -> AdminResult<(RuleSet, u64, u64)> {
        if let Err(e) = self.engine.refresh().await {
            tracing::warn!("rules refresh before save: {e:#}");
        }
        let (version, mut set) = self.engine.rules();
        let id = f(&mut set)?;
        let d = self.engine.save_rules(version, set, by, note).await.map_err(save_err)?;
        Ok((d.body, id, d.version))
    }

    pub async fn create_domain_rule(&self, input: DomainRuleInput, by: &str) -> AdminResult<wire::DomainRule> {
        let pattern = rules::normalize_pattern(&input.pattern).map_err(AdminError::BadRequest)?;
        let effect = effect_from_wire(&input.effect)?;
        let note = format!("create {pattern}");
        let (set, id, version) = self
            .edit_rules(by, &note, |set| {
                if set.rules.iter().any(|r| r.pattern == pattern) {
                    return Err(AdminError::Conflict(format!("a rule for {pattern} already exists")));
                }
                let id = set.next_id.max(1);
                set.next_id = id + 1;
                set.rules.push(Rule {
                    id,
                    pattern: pattern.clone(),
                    effect,
                    note: input.note.clone(),
                    created_at_ms: now_ms(),
                    created_by: by.to_string(),
                });
                Ok(id)
            })
            .await?;
        let m = self.match_counts(&set).await?;
        let r = set.rules.iter().find(|r| r.id == id).expect("just added");
        Ok(Self::rule_view(r, &m, version))
    }

    pub async fn update_domain_rule(&self, id: u64, input: DomainRuleInput, by: &str) -> AdminResult<wire::DomainRule> {
        let pattern = rules::normalize_pattern(&input.pattern).map_err(AdminError::BadRequest)?;
        let effect = effect_from_wire(&input.effect)?;
        let note = format!("update rule {id} ({pattern})");
        let (set, _, version) = self
            .edit_rules(by, &note, |set| {
                if set.rules.iter().any(|r| r.pattern == pattern && r.id != id) {
                    return Err(AdminError::Conflict(format!("a rule for {pattern} already exists")));
                }
                let r = set
                    .rules
                    .iter_mut()
                    .find(|r| r.id == id)
                    .ok_or_else(|| AdminError::NotFound(format!("no rule {id}")))?;
                r.pattern = pattern.clone();
                r.effect = effect;
                r.note = input.note.clone();
                Ok(id)
            })
            .await?;
        let m = self.match_counts(&set).await?;
        let r = set.rules.iter().find(|r| r.id == id).expect("exists");
        Ok(Self::rule_view(r, &m, version))
    }

    pub async fn delete_domain_rule(&self, id: u64, by: &str) -> AdminResult<()> {
        self.edit_rules(by, &format!("delete rule {id}"), |set| {
            let i = set
                .rules
                .iter()
                .position(|r| r.id == id)
                .ok_or_else(|| AdminError::NotFound(format!("no rule {id}")))?;
            set.rules.remove(i);
            Ok(id)
        })
        .await?;
        Ok(())
    }

    /// Not in the dashboard yet: the rules' own audit log.
    pub async fn domain_rules_audit(&self) -> AdminResult<Vec<AuditEntry>> {
        Ok(self.engine.rules_audit(AUDIT_LIMIT).await?)
    }

    // ------------------------------------------------------------ cases

    pub async fn cases(&self, q: CaseQuery) -> AdminResult<Vec<wire::Case>> {
        Ok(self.engine.cases.list(q.status).await?.iter().map(|c| c.to_wire()).collect())
    }

    pub async fn case(&self, id: u64) -> AdminResult<wire::Case> {
        self.engine
            .cases
            .get_case(id)
            .await?
            .map(|c| c.to_wire())
            .ok_or_else(|| AdminError::NotFound(format!("no case {id}")))
    }

    /// With its evidence, which the wire `Case` doesn't carry yet.
    pub async fn case_detail(&self, id: u64) -> AdminResult<super::cases::StoredCase> {
        self.engine.cases.get_case(id).await?.ok_or_else(|| AdminError::NotFound(format!("no case {id}")))
    }

    pub async fn update_case(&self, id: u64, u: CaseUpdate, by: &str) -> AdminResult<wire::Case> {
        self.engine
            .cases
            .update(id, u.status, &u.note, by)
            .await?
            .map(|c| c.to_wire())
            .ok_or_else(|| AdminError::NotFound(format!("no case {id}")))
    }

    // ------------------------------------------------------------ hosts

    /// The tier actions. Returns the updated record, which the caller turns
    /// into a `HostRow` with its live numbers. `Reconnect` is the upstream
    /// module's and comes back as a BadRequest here.
    pub async fn host_action(&self, host: &str, action: HostAction, by: &str) -> AdminResult<HostRecord> {
        let m = match &action {
            HostAction::SetTier { tier } => {
                Manual::SetTier(parse_tier(tier).ok_or_else(|| AdminError::BadRequest(format!("no tier {tier:?}")))?)
            }
            HostAction::Throttle { events_per_sec } => Manual::Throttle(*events_per_sec),
            HostAction::Suspend { reason } => Manual::Suspend(reason.clone()),
            HostAction::Ban { reason } => Manual::Ban(reason.clone()),
            HostAction::Unban => Manual::Unban,
            HostAction::SetAccountLimit { max_accounts } => Manual::AccountLimit(*max_accounts),
            HostAction::Alias { of } => {
                let of = self.alias_target(host, of).await?;
                Manual::Alias { of, by_operator: by != tiers::RELAY_ACTOR }
            }
            HostAction::Unalias { pin } => Manual::Unalias { pin: *pin },
            HostAction::Reconnect => {
                return Err(AdminError::BadRequest("reconnect is an upstream action, not a policy one".into()));
            }
        };
        if let Manual::SetTier(t) = m
            && !matches!(t, Tier::Suspended | Tier::Banned)
            && let Some(r) = self.engine.rule_for(host)
        {
            // what `Engine::for_host` does with the new record: a ban rule always wins, a tier
            // rule unless the host is throttled
            let wins = match r.effect {
                RuleEffect::Ban => Some(Tier::Banned),
                RuleEffect::Tier { tier } if tier != t && t != Tier::Throttled => Some(tier),
                _ => None,
            };
            if let Some(w) = wins {
                return Err(AdminError::tier_set_by_rule(r.id, &r.pattern, tier_name(w)));
            }
        }
        let entry = HostActionRecord { at_ms: now_ms(), by: by.to_string(), action, reason: None, case: None };
        let mut outcome: Option<Result<Tier, String>> = None;
        let out = &mut outcome;
        let written = self
            .hosts
            .update_host(
                host,
                Box::new(move |cur| {
                    let mut rec = cur?;
                    let from = rec.tier;
                    let mut entry = entry;
                    if let (Manual::Unalias { .. }, Some(a)) = (&m, tiers::host_policy(&rec).alias) {
                        entry.reason = Some(format!("cursor reset to head: its accounts were read through {}", a.of));
                    }
                    if let Err(e) = tiers::apply_manual(&mut rec, &m, crate::state::now_secs()) {
                        *out = Some(Err(e));
                        return None;
                    }
                    tiers::record_action(&mut rec, &entry);
                    *out = Some(Ok(from));
                    Some(rec)
                }),
            )
            .await?;
        let from = match outcome {
            None => return Err(AdminError::NotFound(format!("no host {host}"))),
            Some(Err(e)) => return Err(AdminError::BadRequest(e)),
            Some(Ok(from)) => from,
        };
        let rec = written.ok_or_else(|| AdminError::NotFound(format!("no host {host}")))?;
        tracing::info!(
            target: "vlrelay::audit",
            host,
            by,
            from = tier_name(from),
            to = tier_name(rec.tier),
            "host action"
        );
        Ok(rec)
    }

    /// `of` as a host the relay knows, refusing an alias that would lead
    /// back to `host`.
    async fn alias_target(&self, host: &str, of: &str) -> AdminResult<String> {
        // a known host's name, as the host table spells it
        let of = of.trim().trim_start_matches("https://").trim_end_matches('/').to_ascii_lowercase();
        let mut at = of.clone();
        for _ in 0..=tiers::ALIAS_HOPS {
            if at == host {
                return Err(AdminError::BadRequest(format!("{of} leads back to {host}")));
            }
            let Some(rec) = self.hosts.get_host(&at).await? else {
                return Err(AdminError::NotFound(format!("no host {at}")));
            };
            match tiers::host_policy(&rec).alias {
                Some(a) => at = a.of,
                None => return Ok(of),
            }
        }
        Err(AdminError::BadRequest(format!("{of} is an alias more than {} hops deep", tiers::ALIAS_HOPS)))
    }

    /// The operator actions recorded on a host, oldest first.
    pub fn host_actions(rec: &HostRecord) -> Vec<HostActionRecord> {
        tiers::host_policy(rec).actions.into_iter().filter_map(|v| serde_json::from_value(v).ok()).collect()
    }

    /// The limits in force for the dashboard's `HostDetail.limits`.
    pub fn host_limits(&self, rec: &HostRecord) -> wire::TierLimits {
        match self.engine.for_host(rec).limits {
            Some(l) => wire_tier(&l),
            None => wire::TierLimits {
                events_per_sec: 0.0,
                events_per_hour: 0,
                events_per_day: 0,
                max_accounts: 0,
                new_accounts_per_hour: 0,
            },
        }
    }

    pub fn tier_label(t: Tier) -> &'static str {
        tier_name(t)
    }
}
