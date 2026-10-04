//! The policy engine wired into one node (docs/policy.md, "Integration
//! points"). The engine decides and counts; this module carries its
//! decisions to the parts that enforce them and feeds it what they see:
//!
//! - Host tiers and limits: the state host records and the engine own them.
//!   A per-host cache of `Engine::for_host` is the upstream manager's
//!   [`PolicySource`]; it's refreshed when a record is written through
//!   [`PolicyHooks::hosts`] (the driver, operator actions), when the policy
//!   or the domain rules change version, and every [`RESYNC_EVERY`] for
//!   writes this node didn't make.
//! - requestCrawl goes through `Engine::admit_host` ([`Admission`]).
//! - New accounts go through [`AccountGate`]: the host's account cap, its
//!   new-accounts-per-hour limit and the cluster's new-account budget.
//! - DID document fetches spend the cluster's PLC budget.
//! - Rejects, commits, identity events and new accounts become spam signals.
//! - The driver (tier steps, trips, cases) runs here.

use super::State;
use crate::policy::admin::PolicyAdmin;
use crate::policy::driver::Driver;
use crate::policy::{self, Admit, AdmitRequest, BudgetKind, Engine, HostLimits, RejectHost, Signal, SignalKind};
use crate::state::{self, HostCounts, HostKey, HostPage, HostRecord, HostStore, HostUpdate, NewAccount};
use crate::types::Host;
use crate::upstream::{self, Admission, CrawlError, HostPolicy, Manager, PolicySource};
use parking_lot::Mutex;
use std::collections::{HashMap, HashSet};
use std::sync::{Arc, OnceLock, Weak};
use std::time::{Duration, Instant};
use tokio::sync::mpsc;

/// Catches host records written elsewhere (a peer, the counter flush) and
/// time-based limits.
pub const RESYNC_EVERY: Duration = Duration::from_secs(30);

/// The engine as a `NodeConfig` field.
#[derive(Clone)]
pub struct PolicyEngine(pub Arc<Engine>);

impl std::fmt::Debug for PolicyEngine {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "PolicyEngine({})", self.0.node)
    }
}

struct HostState {
    limits: HostLimits,
    /// The record's count at the last sync plus the accounts admitted here
    /// since. The record lags by one counter flush, so this can undercount
    /// by that many seconds of new accounts, never overcount.
    accounts: i64,
    admitted_since_sync: i64,
    new_accounts: Hourly,
    throttle_eps: Option<f64>,
}

/// A per-hour limit with an hour's allowance as its depth, the way the
/// tier limit reads (the cluster budgets' buckets hold one second).
struct Hourly {
    tokens: f64,
    at: Instant,
}

impl Hourly {
    fn new() -> Hourly {
        Hourly { tokens: f64::MAX, at: Instant::now() }
    }

    fn try_take(&mut self, per_hour: f64, now: Instant) -> bool {
        let dt = now.saturating_duration_since(self.at).as_secs_f64();
        self.at = now;
        self.tokens = (self.tokens + dt * per_hour / 3_600.0).min(per_hour);
        let ok = self.tokens >= 1.0;
        if ok {
            self.tokens -= 1.0;
        }
        ok
    }
}

pub struct PolicyHooks {
    pub engine: Arc<Engine>,
    /// The host records, telling the sync loop about every write.
    pub hosts: Arc<dyn HostStore>,
    pub admin: Arc<PolicyAdmin>,
    pub driver: Arc<Driver>,
    state: Arc<State>,
    cache: Mutex<HashMap<String, HostState>>,
    changed: mpsc::UnboundedSender<String>,
    changed_rx: Mutex<Option<mpsc::UnboundedReceiver<String>>>,
    manager: OnceLock<Weak<Manager>>,
    dev_mode: bool,
}

/// A `HostStore` that names every host it writes to the sync loop.
struct Notifying {
    inner: Arc<dyn HostStore>,
    tx: mpsc::UnboundedSender<String>,
}

#[async_trait::async_trait]
impl HostStore for Notifying {
    async fn get_host(&self, hostname: &str) -> anyhow::Result<Option<HostRecord>> {
        self.inner.get_host(hostname).await
    }
    async fn put_host(&self, rec: &HostRecord) -> anyhow::Result<()> {
        self.inner.put_host(rec).await?;
        let _ = self.tx.send(rec.hostname.clone());
        Ok(())
    }
    async fn checkpoint_cursors(&self, cursors: &[(String, i64)]) -> anyhow::Result<()> {
        self.inner.checkpoint_cursors(cursors).await
    }
    async fn add_counts(&self, counts: &[(String, HostCounts)]) -> anyhow::Result<()> {
        self.inner.add_counts(counts).await
    }
    async fn list_hosts(&self, cursor: Option<&str>, limit: usize) -> anyhow::Result<HostPage> {
        self.inner.list_hosts(cursor, limit).await
    }
    async fn update_host(&self, hostname: &str, f: HostUpdate<'_>) -> anyhow::Result<Option<HostRecord>> {
        let r = self.inner.update_host(hostname, f).await?;
        if r.is_some() {
            let _ = self.tx.send(hostname.to_string());
        }
        Ok(r)
    }
}

pub fn tier_to_upstream(t: state::Tier) -> upstream::Tier {
    match t {
        state::Tier::Trusted => upstream::Tier::Trusted,
        state::Tier::Default => upstream::Tier::Default,
        state::Tier::New => upstream::Tier::New,
        state::Tier::Throttled => upstream::Tier::Throttled,
        state::Tier::Suspended => upstream::Tier::Suspended,
        state::Tier::Banned => upstream::Tier::Banned,
    }
}

fn unlimited_if_zero(x: f64) -> f64 {
    if x > 0.0 { x } else { f64::INFINITY }
}

/// The engine's limits as the upstream host task's buckets.
pub fn upstream_limits(l: &policy::TierLimits) -> upstream::TierLimits {
    upstream::TierLimits {
        events_per_sec: unlimited_if_zero(l.events_per_sec),
        bytes_per_sec: unlimited_if_zero(l.bytes_per_sec as f64),
        burst_secs: 5.0,
        weight: 1,
        events_per_hour: l.events_per_hour as f64,
        events_per_day: l.events_per_day as f64,
        reconnects_per_hour: l.reconnects_per_hour as f64,
    }
}

/// Reject reasons that are the state step's (counted in the host's
/// `failed_checks` there) and those that say nothing about the host.
const STATE_REASONS: &[&str] = &[
    "stale",
    "wrong_host",
    "inactive",
    "desynchronized",
    "rev_not_newer",
    "prev_data_mismatch",
    "chain",
    "rate_limited",
    "new_account_deferred",
    "no_identity",
    "bad_cid",
    "not_owner",
    "identity_unavailable",
    "store",
];

/// Not the host's fault, or a follow-on of a failure already counted.
const NOT_SIGNALS: &[&str] =
    &["stale", "desynchronized", "inactive", "rate_limited", "new_account_deferred", "identity_unavailable", "store", "not_owner"];

fn oversized(reason: &str) -> bool {
    matches!(reason, "frame_too_big" | "blocks_too_big" | "too_many_ops" | "too_many_blocks")
}

impl PolicyHooks {
    pub fn new(engine: Arc<Engine>, state: Arc<State>, dev_mode: bool) -> Arc<PolicyHooks> {
        let (tx, rx) = mpsc::unbounded_channel();
        let raw: Arc<dyn HostStore> = state.clone();
        let hosts: Arc<dyn HostStore> = Arc::new(Notifying { inner: raw, tx: tx.clone() });
        Arc::new(PolicyHooks {
            admin: Arc::new(PolicyAdmin::new(engine.clone(), hosts.clone())),
            driver: Arc::new(Driver::new(engine.clone(), hosts.clone())),
            engine,
            hosts,
            state,
            cache: Mutex::new(HashMap::new()),
            changed: tx,
            changed_rx: Mutex::new(Some(rx)),
            manager: OnceLock::new(),
            dev_mode,
        })
    }

    /// Plugs the hooks into the node's parts. Call before the manager starts.
    pub fn install(
        self: &Arc<Self>,
        manager: &Arc<Manager>,
        crawler: &upstream::Crawler,
        identity: &crate::identity::IdentityCache<crate::identity::HttpFetch>,
    ) {
        let _ = self.manager.set(Arc::downgrade(manager));
        manager.set_policy_source(self.clone());
        crawler.set_admission(self.clone());
        self.state.set_account_gate(self.clone());
        let e = self.engine.clone();
        identity.set_budget_gate(Arc::new(move || e.try_take(BudgetKind::PlcLookupsPerSec, 1.0)));
    }

    /// Loads the stored policy and every host record this node lists, so
    /// the first connections already follow them.
    pub async fn load(&self) -> anyhow::Result<()> {
        if let Err(e) = self.engine.refresh().await {
            tracing::warn!("policy load failed (starting on the defaults): {e:#}");
        }
        let mut cursor: Option<String> = None;
        loop {
            let page = self.hosts.list_hosts(cursor.as_deref(), 1_000).await?;
            for rec in &page.hosts {
                self.remember(rec);
            }
            match page.cursor {
                Some(c) => cursor = Some(c),
                None => return Ok(()),
            }
        }
    }

    /// The engine's refresher, the driver and the sync loop.
    pub fn spawn(self: &Arc<Self>) {
        self.engine.spawn_refresher();
        self.driver.spawn();
        if let Some(rx) = self.changed_rx.lock().take() {
            tokio::spawn(self.clone().sync_loop(rx));
        }
    }

    fn remember(&self, rec: &HostRecord) -> HostLimits {
        let limits = self.engine.for_host(rec);
        let throttle_eps = policy::tiers::host_policy(rec).throttle_eps;
        let mut c = self.cache.lock();
        match c.get_mut(&rec.hostname) {
            Some(st) => {
                st.limits = limits.clone();
                st.throttle_eps = throttle_eps;
                st.accounts = rec.account_count + st.admitted_since_sync;
                st.admitted_since_sync = 0;
            }
            None => {
                c.insert(
                    rec.hostname.clone(),
                    HostState {
                        limits: limits.clone(),
                        accounts: rec.account_count,
                        admitted_since_sync: 0,
                        new_accounts: Hourly::new(),
                        throttle_eps,
                    },
                );
            }
        }
        limits
    }

    /// The limits in force for a host, as last synced.
    pub fn limits(&self, host: &str) -> Option<HostLimits> {
        self.cache.lock().get(host).map(|s| s.limits.clone())
    }

    /// The operator throttle on a host (events/s), as last synced.
    pub fn throttle(&self, host: &str) -> Option<f64> {
        self.cache.lock().get(host).and_then(|s| s.throttle_eps)
    }

    /// Re-reads one host's record and applies it to its socket now.
    pub async fn refresh_host(&self, host: &str) -> anyhow::Result<()> {
        if let Some(rec) = self.hosts.get_host(host).await? {
            self.remember(&rec);
        }
        if let Some(m) = self.manager.get().and_then(|w| w.upgrade()) {
            m.apply_policy(&Host(host.to_string())).await;
        }
        Ok(())
    }

    async fn resync_all(&self) {
        let Some(m) = self.manager.get().and_then(|w| w.upgrade()) else { return };
        for e in m.registry().all() {
            if let Err(err) = self.refresh_host(&e.host.0).await {
                tracing::warn!(host = %e.host.0, "policy sync: {err:#}");
            }
        }
    }

    async fn sync_loop(self: Arc<Self>, mut rx: mpsc::UnboundedReceiver<String>) {
        let mut tick = tokio::time::interval(Duration::from_secs(1));
        tick.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
        let versions = |e: &Engine| {
            let s = e.snapshot();
            (s.policy.version, s.rules_version)
        };
        let mut seen = versions(&self.engine);
        let mut last_full = Instant::now();
        loop {
            tokio::select! {
                h = rx.recv() => {
                    let Some(h) = h else { return };
                    let mut batch = HashSet::from([h]);
                    while let Ok(h) = rx.try_recv() {
                        batch.insert(h);
                    }
                    for h in batch {
                        if let Err(e) = self.refresh_host(&h).await {
                            tracing::warn!(host = %h, "policy sync: {e:#}");
                        }
                    }
                }
                _ = tick.tick() => {
                    let v = versions(&self.engine);
                    if v != seen || last_full.elapsed() >= RESYNC_EVERY {
                        if v != seen {
                            tracing::info!(policy = v.0, rules = v.1, "policy changed: re-applying host limits");
                        }
                        seen = v;
                        last_full = Instant::now();
                        self.resync_all().await;
                    }
                }
            }
        }
    }

    /// A frame the node dropped. Verification failures and oversized
    /// commits are spam signals, and the host stage's own rejects count
    /// toward the host's error budget (the state step counts its own).
    pub fn on_reject(&self, host: &str, did: &str, reason: &'static str, detail: &str) {
        if NOT_SIGNALS.contains(&reason) {
            return;
        }
        let kind = if oversized(reason) { SignalKind::OversizedCommit } else { SignalKind::FailedValidation };
        let did = (!did.is_empty()).then_some(did);
        let mut s = Signal::new(kind, host, did);
        let d = format!("{reason}: {detail}");
        s.detail = Some(&d);
        self.engine.record_signal(s);
        if !STATE_REASONS.contains(&reason) {
            self.state.add_host_counts(HostKey::of(host), HostCounts { failed_checks: 1, ..Default::default() });
        }
    }

    /// An event the node accepted.
    pub fn on_accepted(&self, host: &str, did: &str, kind: &str) {
        let kind = match kind {
            "commit" => SignalKind::Record,
            "identity" => SignalKind::IdentityChange,
            _ => return,
        };
        self.engine.record_signal(Signal::new(kind, host, Some(did)));
    }
}

impl PolicySource for PolicyHooks {
    fn host_policy(&self, host: &Host) -> Option<HostPolicy> {
        let l = self.limits(&host.0);
        let Some(l) = l else {
            // a host admitted since the last sync: look it up now
            let _ = self.changed.send(host.0.clone());
            return None;
        };
        Some(HostPolicy {
            tier: tier_to_upstream(l.tier),
            connect: l.connect,
            limits: l.limits.as_ref().map(upstream_limits),
        })
    }
}

/// Past the host's account cap an account is created throttled, as indigo
/// does. Past a rate (the host's new accounts per hour, the cluster's per
/// minute) its event is dropped and nothing is created, so a burst of
/// real accounts on a fresh relay isn't throttled for good.
impl state::AccountGate for PolicyHooks {
    fn admit_account(&self, host: &str, did: &str) -> NewAccount {
        let mut c = self.cache.lock();
        let verdict = 'v: {
            if let Some(st) = c.get_mut(host)
                && let Some(l) = &st.limits.limits
            {
                if l.max_accounts > 0 && st.accounts >= l.max_accounts as i64 {
                    super::metrics::ACCOUNTS_THROTTLED.with_label_values(&["host_cap"]).inc();
                    break 'v NewAccount::Throttle;
                }
                if l.new_accounts_per_hour > 0
                    && !st.new_accounts.try_take(l.new_accounts_per_hour as f64, Instant::now())
                {
                    super::metrics::ACCOUNTS_DEFERRED.with_label_values(&["host_rate"]).inc();
                    break 'v NewAccount::Defer;
                }
            }
            if !self.engine.try_take(BudgetKind::NewAccountsPerMin, 1.0) {
                super::metrics::ACCOUNTS_DEFERRED.with_label_values(&["cluster_budget"]).inc();
                break 'v NewAccount::Defer;
            }
            if let Some(st) = c.get_mut(host) {
                st.accounts += 1;
                st.admitted_since_sync += 1;
            }
            NewAccount::Admit
        };
        drop(c);
        if verdict != NewAccount::Defer {
            self.engine.record_signal(Signal::new(SignalKind::NewAccount, host, Some(did)));
        }
        verdict
    }
}

#[async_trait::async_trait]
impl Admission for PolicyHooks {
    async fn admit(&self, host: &Host) -> Result<upstream::Tier, CrawlError> {
        let existing = self.hosts.get_host(&host.0).await.map_err(|e| CrawlError::Internal(format!("{e:#}")))?;
        // the engine takes indigo's hostname rules, which refuse the IPs and
        // ports a dev network runs on
        let dev_local = self.dev_mode && policy::rules::parse_hostname(&host.0).is_err();
        let req = AdmitRequest { hostname: &host.0, by_admin: dev_local, existing: existing.as_ref() };
        match self.engine.admit_host(&req).await {
            Admit::Admit { tier, .. } => Ok(tier_to_upstream(tier)),
            Admit::Reject(r) => Err(match r {
                RejectHost::Banned { .. } => CrawlError::HostBanned,
                RejectHost::NotAllowed => CrawlError::NotAllowed,
                RejectHost::DailyLimit { .. } => CrawlError::Budget,
                RejectHost::Store(m) => CrawlError::Internal(m),
                r @ (RejectHost::CrawlDisabled | RejectHost::BadHostname(_) | RejectHost::Localhost) => {
                    CrawlError::Refused(r.to_string())
                }
            }),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::admin::HostAction;
    use crate::node::adapters::VerifyChain;
    use crate::policy::{FixedNodes, Rule, RuleEffect, RuleSet};
    use crate::state::{AccountGate, ApplyConfig, StateStore, Tier};
    use vlpds::store::Store;

    async fn setup() -> (Arc<PolicyHooks>, Arc<State>) {
        let store = Store::memory(None);
        let layout = vlpds::slots::Layout::uniform(2).shards;
        let id = crate::state::tests::MapIdentity::new();
        let state = Arc::new(StateStore::new(store.clone(), layout.clone(), VerifyChain, id, ApplyConfig::default()));
        for s in layout {
            state.open_shard(s.id, None).await.unwrap();
        }
        let engine = Engine::new(store, "n1", Arc::new(FixedNodes::new(1)));
        (PolicyHooks::new(engine, state.clone(), false), state)
    }

    async fn add_host(state: &State, h: &str, tier: Tier) {
        state.put_host(&HostRecord::new(h, tier, state::now_secs())).await.unwrap();
    }

    async fn edit_policy(hooks: &PolicyHooks, f: impl FnOnce(&mut policy::PolicyBody)) {
        let cur = hooks.engine.policy();
        let mut body = cur.body.clone();
        f(&mut body);
        hooks.engine.save_policy(cur.version, body, "test", "").await.unwrap();
    }

    fn hp(hooks: &PolicyHooks, h: &str) -> HostPolicy {
        hooks.host_policy(&Host(h.into())).expect("cached")
    }

    #[tokio::test]
    async fn rejects_become_signals_and_count_against_the_host() {
        let (hooks, state) = setup().await;
        add_host(&state, "pds.example", Tier::Default).await;
        for _ in 0..3 {
            hooks.on_reject("pds.example", "did:plc:a", "bad_signature", "sig");
        }
        // the state step's own rejects aren't counted twice; follow-ons aren't signals
        hooks.on_reject("pds.example", "did:plc:a", "prev_data_mismatch", "chain");
        hooks.on_reject("pds.example", "did:plc:a", "desynchronized", "waiting");
        hooks.on_reject("pds.example", "did:plc:a", "frame_too_big", "big");
        let now = policy::store::now_ms();
        let snap = hooks.engine.signals.snapshot("pds.example", Some("did:plc:a"), now);
        assert_eq!(snap.get("failed-validation"), Some(&4.0), "{snap:?}");
        assert_eq!(snap.get("account-failed-validation"), Some(&4.0), "{snap:?}");
        assert_eq!(snap.get("oversized-commits"), Some(&1.0), "{snap:?}");
        state.flush_host_counts(&*state).await.unwrap();
        // bad_signature x3 and frame_too_big; prev_data_mismatch is the state step's
        assert_eq!(state.get_host("pds.example").await.unwrap().unwrap().failed_checks, 4);

        hooks.on_accepted("pds.example", "did:plc:b", "commit");
        hooks.on_accepted("pds.example", "did:plc:b", "identity");
        let snap = hooks.engine.signals.snapshot("pds.example", Some("did:plc:b"), now);
        assert_eq!(snap.get("account-records"), Some(&1.0), "{snap:?}");
        assert_eq!(snap.get("identity-churn"), Some(&1.0), "{snap:?}");
    }

    #[tokio::test]
    async fn new_accounts_hit_the_host_cap_and_rate() {
        let (hooks, state) = setup().await;
        edit_policy(&hooks, |p| {
            p.tiers.default.max_accounts = 2;
            p.tiers.new.new_accounts_per_hour = 1;
        })
        .await;
        add_host(&state, "capped.example", Tier::Default).await;
        add_host(&state, "young.example", Tier::New).await;
        hooks.load().await.unwrap();
        let admit = |h: &str, i: u32| hooks.admit_account(h, &format!("did:plc:{h}{i}"));
        use NewAccount::*;
        assert_eq!([admit("capped.example", 1), admit("capped.example", 2), admit("capped.example", 3)], [
            Admit, Admit, Throttle
        ]);
        assert_eq!([admit("young.example", 1), admit("young.example", 2)], [Admit, Defer]);
        // admitted and throttled accounts are new-account signals, deferred ones not yet
        let now = policy::store::now_ms();
        assert_eq!(hooks.engine.signals.snapshot("capped.example", None, now).get("new-accounts"), Some(&3.0));
        assert_eq!(hooks.engine.signals.snapshot("young.example", None, now).get("new-accounts"), Some(&1.0));
        // an hourly limit holds an hour's allowance, not one second's
        edit_policy(&hooks, |p| p.tiers.default.new_accounts_per_hour = 50).await;
        add_host(&state, "busy.example", Tier::Default).await;
        hooks.refresh_host("busy.example").await.unwrap();
        edit_policy(&hooks, |p| p.tiers.default.max_accounts = 1000).await;
        hooks.refresh_host("busy.example").await.unwrap();
        let admitted = (0..60).filter(|i| admit("busy.example", *i) == Admit).count();
        assert_eq!(admitted, 50);
        // the cluster budget: one node's share of 60/min is one a second
        edit_policy(&hooks, |p| p.cluster.new_accounts_per_min = 60.0).await;
        add_host(&state, "open.example", Tier::Trusted).await;
        hooks.refresh_host("open.example").await.unwrap();
        assert_eq!(admit("open.example", 1), Admit);
        assert_eq!(admit("open.example", 2), Defer);
    }

    #[tokio::test]
    async fn host_policy_follows_records_actions_and_rules() {
        let (hooks, state) = setup().await;
        add_host(&state, "a.example", Tier::Default).await;
        add_host(&state, "b.spam.example", Tier::Trusted).await;
        hooks.load().await.unwrap();
        let a = hp(&hooks, "a.example");
        assert_eq!((a.tier, a.connect), (upstream::Tier::Default, true));
        assert_eq!(a.limits.unwrap().events_per_hour, 2_600.0);

        // an operator throttle caps events/s; a ban disconnects
        hooks.admin.host_action("a.example", HostAction::Throttle { events_per_sec: Some(2.0) }, "op").await.unwrap();
        hooks.refresh_host("a.example").await.unwrap();
        assert_eq!(hp(&hooks, "a.example").limits.unwrap().events_per_sec, 2.0);
        assert_eq!(hooks.throttle("a.example"), Some(2.0));
        hooks.admin.host_action("a.example", HostAction::Ban { reason: "spam".into() }, "op").await.unwrap();
        hooks.refresh_host("a.example").await.unwrap();
        let a = hp(&hooks, "a.example");
        assert_eq!((a.tier, a.connect, a.limits), (upstream::Tier::Banned, false, None));

        // a domain rule bans a host whose record says trusted
        let set = RuleSet {
            next_id: 2,
            rules: vec![Rule {
                id: 1,
                pattern: "*.spam.example".into(),
                effect: RuleEffect::Ban,
                note: String::new(),
                created_at_ms: 0,
                created_by: "op".into(),
            }],
        };
        hooks.engine.save_rules(0, set, "op", "").await.unwrap();
        hooks.refresh_host("b.spam.example").await.unwrap();
        let b = hp(&hooks, "b.spam.example");
        assert_eq!((b.tier, b.connect), (upstream::Tier::Banned, false));
        assert_eq!(hooks.limits("b.spam.example").unwrap().rule, Some(1));
    }

    #[tokio::test]
    async fn a_spam_trip_throttles_the_host_through_the_driver() {
        let (hooks, state) = setup().await;
        edit_policy(&hooks, |p| {
            p.spam.host_failed_validation.limit = 10.0;
        })
        .await;
        add_host(&state, "bad.example", Tier::Default).await;
        hooks.load().await.unwrap();
        for _ in 0..11 {
            hooks.on_reject("bad.example", "", "bad_signature", "sig");
        }
        let r = hooks.driver.process_trips().await.unwrap();
        assert_eq!(r.moved.len(), 1, "{r:?}");
        assert_eq!(r.cases.len(), 1, "{r:?}");
        // the driver wrote through the notifying store: the sync loop would
        // pick it up; here it's applied by hand
        hooks.refresh_host("bad.example").await.unwrap();
        let b = hp(&hooks, "bad.example");
        assert_eq!((b.tier, b.connect), (upstream::Tier::Throttled, true));
        assert_eq!(b.limits.unwrap().events_per_sec, 5.0);
    }

    #[tokio::test]
    async fn admission_is_the_engines() {
        let (hooks, state) = setup().await;
        edit_policy(&hooks, |p| p.cluster.new_hosts_per_day = 1).await;
        let set = RuleSet {
            next_id: 2,
            rules: vec![Rule {
                id: 1,
                pattern: "*.spam.example".into(),
                effect: RuleEffect::Ban,
                note: String::new(),
                created_at_ms: 0,
                created_by: "op".into(),
            }],
        };
        hooks.engine.save_rules(0, set, "op", "").await.unwrap();
        let admit = |h: &str| {
            let h = Host(h.to_string());
            let hooks = hooks.clone();
            async move { hooks.admit(&h).await }
        };
        assert_eq!(admit("x.spam.example").await, Err(CrawlError::HostBanned));
        assert_eq!(admit("one.example").await, Ok(upstream::Tier::New));
        assert_eq!(admit("two.example").await, Err(CrawlError::Budget));
        // trusted domains skip the budget and start trusted
        assert_eq!(admit("morel.us-east.host.bsky.network").await, Ok(upstream::Tier::Trusted));
        // a known host keeps its tier, unless it's banned
        add_host(&state, "known.example", Tier::Default).await;
        assert_eq!(admit("known.example").await, Ok(upstream::Tier::Default));
        add_host(&state, "gone.example", Tier::Banned).await;
        assert_eq!(admit("gone.example").await, Err(CrawlError::HostBanned));
        assert!(matches!(admit("localhost").await, Err(CrawlError::Refused(_))));
    }

    #[tokio::test]
    async fn the_manager_disconnects_and_reconnects_with_the_policy() {
        let (hooks, state) = setup().await;
        add_host(&state, "127.0.0.1:9", Tier::Default).await;
        hooks.load().await.unwrap();
        let mut cfg = upstream::UpstreamConfig::new(true);
        cfg.endpoint = Arc::new(|h: &Host| format!("http://{}", h.0));
        let store: Arc<dyn upstream::HostStore> = Arc::new(upstream::MemHostStore::default());
        let (m, _rx) = Manager::new(cfg, store, None);
        m.set_policy_source(hooks.clone());
        let _ = hooks.manager.set(Arc::downgrade(&m));
        m.start().await.unwrap();
        let h = Host("127.0.0.1:9".into());
        m.admit(&h, upstream::Tier::Default).await.unwrap();
        assert_eq!(m.running(), 1);
        hooks.admin.host_action(&h.0, HostAction::Suspend { reason: "x".into() }, "op").await.unwrap();
        hooks.refresh_host(&h.0).await.unwrap();
        assert_eq!(m.running(), 0);
        assert_eq!(m.host(&h).unwrap().record.tier, upstream::Tier::Suspended);
        hooks.admin.host_action(&h.0, HostAction::Unban, "op").await.unwrap();
        hooks.refresh_host(&h.0).await.unwrap();
        assert_eq!(m.running(), 1);
        assert_eq!(m.host(&h).unwrap().record.tier, upstream::Tier::Default);
        m.shutdown().await.unwrap();
    }
}
