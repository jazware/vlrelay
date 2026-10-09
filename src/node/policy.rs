//! The policy engine wired into one node (docs/policy-internals.md, "Wiring
//! points"). The engine decides and counts; this module carries its
//! decisions to the parts that enforce them and feeds it what they see:
//!
//! - Host tiers and limits: the state host records and the engine own them.
//!   A per-host cache of `Engine::for_host` is the upstream manager's
//!   [`PolicySource`]; it's refreshed when a record is written through
//!   [`PolicyHooks::hosts`] (the driver, operator actions), when the quorum
//!   log's host table moves a host's tier or policy (another member's
//!   write), when the policy or the domain rules change version, and every
//!   [`RESYNC_EVERY`] for anything else.
//! - requestCrawl goes through `Engine::admit_host` ([`Admission`]).
//! - Accounts this node hasn't seen go through [`AccountGate`]. Every one
//!   counts toward its host's account cap. Only newly created ones (a
//!   repo's first commit) spend the host's new-accounts-per-hour limit and
//!   the cluster's new-account budget, and only they are new-account
//!   signals: to a relay starting cold, every established account is
//!   unknown, not new.
//! - DID document fetches spend the cluster's PLC budget.
//! - Rejects, commits, identity events and new accounts become spam signals.
//! - The driver (tier steps, trips, cases) runs here.

use super::State;
use crate::policy::admin::PolicyAdmin;
use crate::policy::driver::Driver;
use crate::policy::{self, Admit, AdmitRequest, BudgetKind, Engine, HostLimits, RejectHost, Signal, SignalKind};
use crate::state::{self, Arrival, HostCounts, HostKey, HostPage, HostRecord, HostStore, HostUpdate, NewAccount};
use crate::types::Host;
use crate::upstream::{self, Admission, CrawlError, HostPolicy, Manager, PolicySource};
use parking_lot::Mutex;
use std::collections::{HashMap, HashSet};
use std::sync::{Arc, OnceLock, Weak};
use std::time::{Duration, Instant};
use tokio::sync::mpsc;

/// Catches what nothing announces (the counter flush, time-based limits).
pub const RESYNC_EVERY: Duration = Duration::from_secs(30);

/// Newly created accounts whose event was deferred, so their later events
/// (which no longer look like a creation) still wait for the rate budget.
/// Bounded: past it a deferred account's next event is only first-seen,
/// and its host's account cap still holds.
const DEFERRED_MAX: usize = 100_000;
const DEFERRED_FOR: Duration = Duration::from_secs(3_600);

fn wall_ms() -> i64 {
    upstream::host::now_ms() as i64
}

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
    /// `identityEventsPerHour`.
    identity_events: Hourly,
    /// Fresh DID document fetches this host's events asked for, at the same
    /// hourly rate with a minute's depth: a burst of them would otherwise
    /// drain the node's PLC budget that every host's lookups wait on.
    forced_lookups: Hourly,
    throttle_eps: Option<f64>,
    alias: Option<policy::tiers::Alias>,
    not_alias: bool,
}

/// A per-hour limit with an hour's allowance as its depth, the way the
/// tier limit reads (the cluster budgets' buckets hold one second). It
/// runs on whichever clock the caller passes (unix ms): the wall clock, or
/// a host's own (`upstream::clock`), which lanes hand it slightly out of
/// order, so it never steps back.
struct Hourly {
    tokens: f64,
    at_ms: i64,
}

impl Hourly {
    fn new() -> Hourly {
        Hourly { tokens: f64::MAX, at_ms: 0 }
    }

    fn try_take(&mut self, per_hour: f64, now_ms: i64) -> bool {
        self.try_take_depth(per_hour, per_hour, now_ms)
    }

    fn try_take_depth(&mut self, per_hour: f64, depth: f64, now_ms: i64) -> bool {
        let dt = (now_ms - self.at_ms).max(0) as f64 / 1000.0;
        self.at_ms = self.at_ms.max(now_ms);
        self.tokens = (self.tokens + dt * per_hour / 3_600.0).min(depth);
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
    deferred: Mutex<HashMap<String, Instant>>,
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

/// Where `host`'s alias chain ends: the host the relay reads it as. None
/// for a host it doesn't know, or a chain past `ALIAS_HOPS` or in a loop.
fn resolve_alias(c: &HashMap<String, HostState>, host: &str) -> Option<String> {
    let mut at = host;
    for _ in 0..=policy::tiers::ALIAS_HOPS {
        match &c.get(at)?.alias {
            Some(a) => at = &a.of,
            None => return Some(at.to_string()),
        }
    }
    None
}

fn alias_map(c: &HashMap<String, HostState>) -> HashMap<HostKey, HostKey> {
    c.iter()
        .filter(|(_, s)| s.alias.is_some())
        .filter_map(|(h, _)| Some((HostKey::of(h), HostKey::of(&resolve_alias(c, h)?))))
        .collect()
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
const NOT_SIGNALS: &[&str] = &[
    "stale",
    "desynchronized",
    "inactive",
    "rate_limited",
    "new_account_deferred",
    "identity_unavailable",
    "store",
    "not_owner",
];

/// An `#identity` past its host's `identityEventsPerHour`.
pub const IDENTITY_RATE: &str = "identity_rate";

fn oversized(reason: &str) -> bool {
    matches!(reason, "frame_too_big" | "blocks_too_big" | "too_many_ops" | "too_many_blocks")
}

impl PolicyHooks {
    /// Over `raw`, the host records (on the quorum log, its host table).
    pub fn new(engine: Arc<Engine>, state: Arc<State>, raw: Arc<dyn HostStore>, dev_mode: bool) -> Arc<PolicyHooks> {
        let (tx, rx) = mpsc::unbounded_channel();
        let hosts: Arc<dyn HostStore> = Arc::new(Notifying { inner: raw, tx: tx.clone() });
        Arc::new(PolicyHooks {
            admin: Arc::new(PolicyAdmin::new(engine.clone(), hosts.clone())),
            driver: Arc::new(Driver::new(engine.clone(), hosts.clone())),
            engine,
            hosts,
            state,
            cache: Mutex::new(HashMap::new()),
            deferred: Mutex::new(HashMap::new()),
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
        self.ban_refusals(manager);
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

    /// Where to name a host whose record changed elsewhere, for the sync
    /// loop to re-apply.
    pub fn changed_sender(&self) -> mpsc::UnboundedSender<String> {
        self.changed.clone()
    }

    fn remember(&self, rec: &HostRecord) -> HostLimits {
        let limits = self.engine.for_host(rec);
        let hp = policy::tiers::host_policy(rec);
        let mut c = self.cache.lock();
        let mut cleared = false;
        let aliases_moved = match c.get_mut(&rec.hostname) {
            Some(st) => {
                st.limits = limits.clone();
                st.throttle_eps = hp.throttle_eps;
                st.accounts = rec.account_count + st.admitted_since_sync;
                st.admitted_since_sync = 0;
                st.not_alias = hp.not_alias;
                let moved = st.alias.as_ref().map(|a| &a.of) != hp.alias.as_ref().map(|a| &a.of);
                cleared = st.alias.is_some() && hp.alias.is_none();
                st.alias = hp.alias;
                moved
            }
            None => {
                // an alias remembered before the host it names
                let moved =
                    hp.alias.is_some() || c.values().any(|s| s.alias.as_ref().is_some_and(|a| a.of == rec.hostname));
                c.insert(
                    rec.hostname.clone(),
                    HostState {
                        limits: limits.clone(),
                        accounts: rec.account_count,
                        admitted_since_sync: 0,
                        new_accounts: Hourly::new(),
                        identity_events: Hourly::new(),
                        forced_lookups: Hourly::new(),
                        throttle_eps: hp.throttle_eps,
                        alias: hp.alias,
                        not_alias: hp.not_alias,
                    },
                );
                moved
            }
        };
        if aliases_moved {
            let m = alias_map(&c);
            super::metrics::HOST_ALIASES.set(m.len() as i64);
            self.state.set_host_aliases(m);
        }
        drop(c);
        // its accounts came through the other name all along: a replay from
        // its old cursor would be duplicates, and after a wrong alias that
        // cursor can't be trusted
        if cleared && let Some(m) = self.manager.get().and_then(Weak::upgrade) {
            m.start_at_head(&Host(rec.hostname.clone()));
        }
        limits
    }

    /// The host `host` is an alias of, if it's one.
    pub fn alias_of(&self, host: &str) -> Option<String> {
        self.cache.lock().get(host).and_then(|s| s.alias.as_ref().map(|a| a.of.clone()))
    }

    /// Whether the relay may mark `host` an alias of `of` (neither is one
    /// already, no operator said otherwise), and the host to name: `of`,
    /// or what it's an alias of.
    pub fn alias_candidate(&self, host: &str, of: &str) -> Option<String> {
        let c = self.cache.lock();
        let st = c.get(host)?;
        if st.alias.is_some() || st.not_alias {
            return None;
        }
        let root = resolve_alias(&c, of)?;
        (root != host).then_some(root)
    }

    /// The relay's own aliases (not an operator's), with when each was
    /// last confirmed.
    pub fn relay_aliases(&self) -> Vec<(String, policy::tiers::Alias)> {
        let c = self.cache.lock();
        c.iter()
            .filter_map(|(h, s)| s.alias.as_ref().filter(|a| !a.by_operator).map(|a| (h.clone(), a.clone())))
            .collect()
    }

    /// The limits in force for a host, as last synced.
    pub fn limits(&self, host: &str) -> Option<HostLimits> {
        self.cache.lock().get(host).map(|s| s.limits.clone())
    }

    /// Accounts on a host: the record's count plus those admitted since.
    pub fn accounts(&self, host: &str) -> Option<u64> {
        self.cache.lock().get(host).map(|s| s.accounts.max(0) as u64)
    }

    /// Takes one `#identity` from the host's `identityEventsPerHour` (0:
    /// unlimited), counted at `clock_ms` on the host's own timeline, so a
    /// replayed hour costs an hour's allowance. False: drop it.
    pub fn take_identity_event(&self, host: &str, clock_ms: i64) -> bool {
        let mut c = self.cache.lock();
        let Some(st) = c.get_mut(host) else { return true };
        let Some(per_hour) = st.limits.limits.as_ref().map(|l| l.identity_events_per_hour) else { return true };
        per_hour == 0 || st.identity_events.try_take(per_hour as f64, clock_ms)
    }

    /// Takes one fresh DID document fetch from the host's budget: its
    /// `identityEventsPerHour`, at most a minute's worth (and 10) at once.
    pub fn take_forced_lookup(&self, host: &str) -> bool {
        let mut c = self.cache.lock();
        let Some(st) = c.get_mut(host) else { return true };
        let Some(per_hour) = st.limits.limits.as_ref().map(|l| l.identity_events_per_hour) else { return true };
        if per_hour == 0 {
            return true;
        }
        // the wall clock: these fetches are the PLC budget's, spent now
        let ok = st.forced_lookups.try_take_depth(per_hour as f64, (per_hour as f64 / 60.0).max(10.0), wall_ms());
        if !ok {
            super::metrics::FORCED_LOOKUPS_REFUSED.inc();
        }
        ok
    }

    /// The operator throttle on a host (events/s), as last synced.
    pub fn throttle(&self, host: &str) -> Option<f64> {
        self.cache.lock().get(host).and_then(|s| s.throttle_eps)
    }

    fn ban_refusals(self: &Arc<Self>, manager: &Manager) {
        let me = Arc::downgrade(self);
        manager.set_on_refused(Arc::new(move |host: &Host, why: &str| {
            let (Some(me), host, why) = (me.upgrade(), host.0.clone(), why.to_string()) else { return };
            tokio::spawn(async move { me.ban_refused(&host, &why).await });
        }));
    }

    /// Bans a host the upstream refused for good, through the same audited
    /// action an operator's ban takes, so it shows as banned and an unban
    /// is how to retry it.
    async fn ban_refused(&self, host: &str, why: &str) {
        let action = crate::admin::HostAction::Ban { reason: format!("refused at connect: {why}") };
        if let Err(e) = self.admin.host_action(host, action, "vlrelay").await {
            tracing::warn!(host, "banning a refused upstream failed: {e:?}");
        }
        if let Err(e) = self.refresh_host(host).await {
            tracing::warn!(host, "applying a refused upstream's ban: {e:#}");
        }
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
        let kind = match reason {
            r if oversized(r) => SignalKind::OversizedCommit,
            // the same churn an accepted one counts as
            IDENTITY_RATE => SignalKind::IdentityChange,
            _ => SignalKind::FailedValidation,
        };
        let did = (!did.is_empty()).then_some(did);
        let mut s = Signal::new(kind, host, did);
        let d = format!("{reason}: {detail}");
        s.detail = Some(&d);
        s.weight = self.event_weight(host);
        self.engine.record_signal(s);
        if !STATE_REASONS.contains(&reason) {
            let k = HostKey::of(host);
            self.state.note_host(k, host);
            self.state.add_host_counts(k, HostCounts { failed_checks: 1, ..Default::default() });
        }
    }

    /// An event the node accepted.
    pub fn on_accepted(&self, host: &str, did: &str, kind: &str) {
        let kind = match kind {
            "commit" => SignalKind::Record,
            "identity" => SignalKind::IdentityChange,
            _ => return,
        };
        let mut s = Signal::new(kind, host, Some(did));
        s.weight = self.event_weight(host);
        self.engine.record_signal(s);
    }

    /// What one of the host's events weighs in the spam windows: 1/pace of
    /// its own timeline, so a backlog read at 60× its pace fills a window
    /// as the original hour did, not 60 times over. New accounts aren't
    /// weighed: a farm replayed is still a farm.
    fn event_weight(&self, host: &str) -> f64 {
        let Some(m) = self.manager.get().and_then(Weak::upgrade) else { return 1.0 };
        m.registry().get(&Host(host.to_string())).map_or(1.0, |e| 1.0 / e.pace())
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
            backfill: l.backfill,
        })
    }
}

/// Past the host's account cap an account is created throttled, as indigo
/// does. A newly created account past a rate (the host's new accounts per
/// hour, the cluster's per minute) has its event dropped and is remembered,
/// so its next event asks again; nothing about it is throttled for good.
/// Established accounts seen for the first time only meet the cap.
impl state::AccountGate for PolicyHooks {
    fn admit_account(&self, host: &str, did: &str, how: Arrival) -> NewAccount {
        let now = Instant::now();
        let was_deferred = {
            let mut d = self.deferred.lock();
            match d.get(did) {
                Some(at) if now.duration_since(*at) < DEFERRED_FOR => true,
                Some(_) => {
                    d.remove(did);
                    false
                }
                None => false,
            }
        };
        let (counts, created) = match how {
            Arrival::FirstSeen => (true, false),
            Arrival::Created => (true, true),
            Arrival::FirstCommit { created } => (false, created),
        };
        let rated = created || was_deferred;
        if !counts && !rated {
            return NewAccount::Admit;
        }
        let mut c = self.cache.lock();
        let verdict = 'v: {
            if let Some(st) = c.get_mut(host)
                && let Some(l) = &st.limits.limits
            {
                if counts && l.max_accounts > 0 && st.accounts >= l.max_accounts as i64 {
                    super::metrics::ACCOUNTS_THROTTLED.with_label_values(&["host_cap"]).inc();
                    break 'v NewAccount::Throttle;
                }
                if rated
                    && l.new_accounts_per_hour > 0
                    && !st.new_accounts.try_take(l.new_accounts_per_hour as f64, wall_ms())
                {
                    super::metrics::ACCOUNTS_DEFERRED.with_label_values(&["host_rate"]).inc();
                    break 'v NewAccount::Defer;
                }
            }
            if rated && !self.engine.try_take(BudgetKind::NewAccountsPerMin, 1.0) {
                super::metrics::ACCOUNTS_DEFERRED.with_label_values(&["cluster_budget"]).inc();
                break 'v NewAccount::Defer;
            }
            if counts && let Some(st) = c.get_mut(host) {
                st.accounts += 1;
                st.admitted_since_sync += 1;
            }
            NewAccount::Admit
        };
        drop(c);
        if verdict == NewAccount::Admit && rated {
            super::metrics::NEW_ACCOUNTS.inc();
        }
        if verdict == NewAccount::Defer {
            let mut d = self.deferred.lock();
            if d.len() >= DEFERRED_MAX {
                d.retain(|_, at| now.duration_since(*at) < DEFERRED_FOR);
            }
            if d.len() < DEFERRED_MAX || d.contains_key(did) {
                d.insert(did.to_string(), now);
            }
        } else if was_deferred {
            self.deferred.lock().remove(did);
        }
        // once per creation, deferred or not, so a farm trips its threshold
        // at the rate it creates accounts, not the rate they're let in
        if created && !was_deferred {
            self.engine.record_signal(Signal::new(SignalKind::NewAccount, host, Some(did)));
        }
        verdict
    }

    fn forced_lookup(&self, host: &str) -> bool {
        self.take_forced_lookup(host)
    }
}

#[async_trait::async_trait]
impl Admission for PolicyHooks {
    async fn admit(&self, host: &Host, spend: bool) -> Result<upstream::Tier, CrawlError> {
        let existing = self.hosts.get_host(&host.0).await.map_err(|e| CrawlError::Internal(format!("{e:#}")))?;
        // the engine takes indigo's hostname rules, which refuse the IPs and
        // ports a dev network runs on
        let dev_local = self.dev_mode && policy::rules::parse_hostname(&host.0).is_err();
        let req = AdmitRequest { hostname: &host.0, by_admin: dev_local, existing: existing.as_ref(), dry_run: !spend };
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
pub(crate) mod tests {
    use super::*;
    use crate::admin::HostAction;
    use crate::node::adapters::VerifyChain;
    use crate::policy::{FixedNodes, Rule, RuleEffect, RuleSet};
    use crate::state::{AccountGate, ApplyConfig, StateStore, Tier};
    use vlsync_store::store::Store;

    pub(crate) async fn setup() -> (Arc<PolicyHooks>, Arc<State>) {
        let store = Store::memory(None);
        let id = crate::state::tests::MapIdentity::new();
        let state = Arc::new(StateStore::new(VerifyChain, id, ApplyConfig::default()));
        crate::state::tests::attach_memory_shard(&state).await;
        let engine = Engine::new(store, "n1", Arc::new(FixedNodes::new(1)));
        let hosts: Arc<dyn HostStore> = Arc::new(crate::state::tests::MemHosts::default());
        (PolicyHooks::new(engine, state.clone(), hosts, false), state)
    }

    #[tokio::test]
    async fn identity_events_and_forced_lookups_are_per_host() {
        let (hooks, _) = setup().await;
        add_host(&*hooks.hosts, "noisy.example", Tier::Throttled).await;
        add_host(&*hooks.hosts, "quiet.example", Tier::Throttled).await;
        hooks.load().await.unwrap();
        let per_hour = policy::TierLimits::throttled().identity_events_per_hour;
        let t0 = wall_ms();
        let taken = (0..per_hour + 50).filter(|_| hooks.take_identity_event("noisy.example", t0)).count() as u64;
        assert_eq!(taken, per_hour);
        assert!(hooks.take_identity_event("quiet.example", t0));
        // on the host's timeline: an hour of twice the allowance, replayed in
        // no wall time at all, gets that hour's allowance and no more
        let step = 3_600_000 / (2 * per_hour as i64);
        let replayed = (1..=2 * per_hour as i64)
            .filter(|i| hooks.take_identity_event("noisy.example", t0 + i * step))
            .count() as u64;
        assert!(replayed.abs_diff(per_hour) <= 1, "{replayed} of {per_hour}");
        // and an event dated before the last doesn't refill anything
        assert!(!hooks.take_identity_event("noisy.example", t0));
        // forced lookups: a minute's depth (at least 10), not an hour's
        let forced = (0..per_hour).filter(|_| hooks.take_forced_lookup("noisy.example")).count();
        assert_eq!(forced, 10);
        assert!(hooks.take_forced_lookup("quiet.example"));
        assert!(state::AccountGate::forced_lookup(&*hooks, "quiet.example"));
    }

    pub(crate) async fn add_host(hosts: &dyn HostStore, h: &str, tier: Tier) {
        hosts.put_host(&HostRecord::new(h, tier, state::now_secs())).await.unwrap();
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
        add_host(&*hooks.hosts, "pds.example", Tier::Default).await;
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
        state.flush_host_counts(&*hooks.hosts).await.unwrap();
        // bad_signature x3 and frame_too_big; prev_data_mismatch is the state step's
        assert_eq!(hooks.hosts.get_host("pds.example").await.unwrap().unwrap().failed_checks, 4);

        hooks.on_accepted("pds.example", "did:plc:b", "commit");
        hooks.on_accepted("pds.example", "did:plc:b", "identity");
        let snap = hooks.engine.signals.snapshot("pds.example", Some("did:plc:b"), now);
        assert_eq!(snap.get("account-records"), Some(&1.0), "{snap:?}");
        assert_eq!(snap.get("identity-churn"), Some(&1.0), "{snap:?}");
    }

    #[tokio::test]
    async fn new_accounts_hit_the_host_cap_and_rate() {
        let (hooks, _) = setup().await;
        edit_policy(&hooks, |p| {
            p.tiers.default.max_accounts = 2;
            p.tiers.new.new_accounts_per_hour = 1;
            p.tiers.new.max_accounts = 100;
        })
        .await;
        add_host(&*hooks.hosts, "capped.example", Tier::Default).await;
        add_host(&*hooks.hosts, "young.example", Tier::New).await;
        hooks.load().await.unwrap();
        let did = |h: &str, i: u32| format!("did:plc:{h}{i}");
        let admit = |h: &str, i: u32| hooks.admit_account(h, &did(h, i), Arrival::Created);
        use NewAccount::*;
        assert_eq!(
            [admit("capped.example", 1), admit("capped.example", 2), admit("capped.example", 3)],
            [Admit, Admit, Throttle]
        );
        assert_eq!([admit("young.example", 1), admit("young.example", 2)], [Admit, Defer]);
        // every creation is a new-account signal once, whatever the verdict
        let now = policy::store::now_ms();
        assert_eq!(hooks.engine.signals.snapshot("capped.example", None, now).get("new-accounts"), Some(&3.0));
        assert_eq!(hooks.engine.signals.snapshot("young.example", None, now).get("new-accounts"), Some(&2.0));
        // a deferred creation's later events, which don't look like one, still wait
        let young2 = did("young.example", 2);
        assert_eq!(hooks.admit_account("young.example", &young2, Arrival::FirstSeen), Defer);
        assert_eq!(hooks.admit_account("young.example", &young2, Arrival::FirstCommit { created: false }), Defer);
        assert_eq!(hooks.engine.signals.snapshot("young.example", None, now).get("new-accounts"), Some(&2.0));
        // established accounts seen for the first time skip the rate (spent
        // here) and aren't signals, but meet the cap
        let seen = |i: u32| hooks.admit_account("young.example", &format!("did:plc:seen{i}"), Arrival::FirstSeen);
        assert_eq!((0..99).filter(|i| seen(*i) == Admit).count(), 99);
        assert_eq!(seen(99), Throttle);
        assert_eq!(hooks.accounts("young.example"), Some(100));
        assert_eq!(hooks.engine.signals.snapshot("young.example", None, now).get("new-accounts"), Some(&2.0));
        // a known account's first commit counts nothing unless it's a creation
        assert_eq!(
            hooks.admit_account("young.example", "did:plc:known", Arrival::FirstCommit { created: false }),
            Admit
        );
        assert_eq!(
            hooks.admit_account("young.example", "did:plc:known", Arrival::FirstCommit { created: true }),
            Defer
        );
        assert_eq!(hooks.accounts("young.example"), Some(100));
        // an hourly limit holds an hour's allowance, not one second's
        edit_policy(&hooks, |p| p.tiers.default.new_accounts_per_hour = 50).await;
        add_host(&*hooks.hosts, "busy.example", Tier::Default).await;
        hooks.refresh_host("busy.example").await.unwrap();
        edit_policy(&hooks, |p| p.tiers.default.max_accounts = 1000).await;
        hooks.refresh_host("busy.example").await.unwrap();
        let admitted = (0..60).filter(|i| admit("busy.example", *i) == Admit).count();
        assert_eq!(admitted, 50);
        // the cluster budget: one node's share of 60/min is one a second
        edit_policy(&hooks, |p| p.cluster.new_accounts_per_min = 60.0).await;
        add_host(&*hooks.hosts, "open.example", Tier::Trusted).await;
        hooks.refresh_host("open.example").await.unwrap();
        assert_eq!(admit("open.example", 1), Admit);
        assert_eq!(admit("open.example", 2), Defer);
    }

    #[tokio::test]
    async fn a_farm_of_new_repos_trips_the_rate_and_a_case_where_old_accounts_dont() {
        let (hooks, _) = setup().await;
        edit_policy(&hooks, |p| {
            p.tiers.default.max_accounts = 10_000;
            p.tiers.default.new_accounts_per_hour = 100;
            p.spam.host_new_accounts.limit = 120.0;
            p.spam.host_new_accounts.action = policy::SpamAction::ThrottleAndCase;
        })
        .await;
        add_host(&*hooks.hosts, "farm.example", Tier::Default).await;
        add_host(&*hooks.hosts, "old.example", Tier::Default).await;
        hooks.load().await.unwrap();
        let run = |h: &str, how: Arrival| {
            (0..1_000).map(|i| hooks.admit_account(h, &format!("did:plc:{h}{i}"), how)).collect::<Vec<_>>()
        };
        let farm = run("farm.example", Arrival::Created);
        let old = run("old.example", Arrival::FirstSeen);
        let count = |v: &[NewAccount], x| v.iter().filter(|a| **a == x).count();
        // an hour's allowance, then the rest wait
        assert_eq!((count(&farm, NewAccount::Admit), count(&farm, NewAccount::Defer)), (100, 900));
        assert_eq!(count(&old, NewAccount::Admit), 1_000);
        let r = hooks.driver.process_trips().await.unwrap();
        assert_eq!(r.moved.len(), 1, "{r:?}");
        assert_eq!(r.cases.len(), 1, "{r:?}");
        hooks.refresh_host("farm.example").await.unwrap();
        hooks.refresh_host("old.example").await.unwrap();
        assert_eq!(hp(&hooks, "farm.example").tier, upstream::Tier::Throttled);
        assert_eq!(hp(&hooks, "old.example").tier, upstream::Tier::Default);
        let now = policy::store::now_ms();
        assert_eq!(hooks.engine.signals.snapshot("old.example", None, now).get("new-accounts"), None);
    }

    /// A DID document service that answers for any DID, with one key.
    struct AnyDid {
        key: String,
        pds: String,
        fetches: std::sync::atomic::AtomicU64,
    }

    impl crate::identity::Fetch for Arc<AnyDid> {
        async fn fetch(&self, did: &str) -> Result<serde_json::Value, crate::identity::LookupError> {
            self.fetches.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
            Ok(serde_json::json!({
                "id": did,
                "alsoKnownAs": [],
                "verificationMethod": [{"id": format!("{did}#atproto"), "type": "Multikey", "controller": did, "publicKeyMultibase": self.key}],
                "service": [{"id": "#atproto_pds", "type": "AtprotoPersonalDataServer", "serviceEndpoint": self.pds}],
            }))
        }
    }

    /// A relay with an empty state store meets 50k established accounts
    /// through one trusted host: the host stage's lookup, signature and MST
    /// checks, then the DID owner's step with the gate. Every event lands;
    /// the PLC budget paces them and nothing is deferred.
    #[tokio::test(flavor = "multi_thread", worker_threads = 8)]
    async fn a_cold_start_paces_established_accounts_without_dropping_any() {
        use crate::identity::{IdentityCache, Options};
        use crate::verify::synth::{Curve, Repo, Signer};
        use std::sync::atomic::Ordering::Relaxed;
        const DIDS: usize = 50_000;
        const PLC_PER_SEC: f64 = 10_000.0;
        let host = "pds.cold.example";
        let fetch = Arc::new(AnyDid {
            key: Signer::new(Curve::K256, 7).multibase(),
            pds: format!("https://{host}"),
            fetches: Default::default(),
        });
        let identity = Arc::new(IdentityCache::new(
            fetch.clone(),
            Options {
                lookups_per_sec: 1e9,
                burst: 1e9,
                // short, so lookups over the cluster budget take the paced retry
                max_budget_wait: Duration::from_millis(20),
                ..Options::default()
            },
        ));
        let store = Store::memory(None);
        let state = Arc::new(StateStore::new(
            VerifyChain,
            Arc::new(crate::node::adapters::CacheIdentity(identity.clone(), Default::default())),
            ApplyConfig::default(),
        ));
        crate::state::tests::attach_memory_shard(&state).await;
        let engine = Engine::new(store, "n1", Arc::new(FixedNodes::new(1)));
        let hosts: Arc<dyn HostStore> = Arc::new(crate::state::tests::MemHosts::default());
        let hooks = PolicyHooks::new(engine, state.clone(), hosts, false);
        state.set_account_gate(hooks.clone());
        let e = hooks.engine.clone();
        identity.set_budget_gate(Arc::new(move || e.try_take(BudgetKind::PlcLookupsPerSec, 1.0)));
        edit_policy(&hooks, |p| p.cluster.plc_lookups_per_sec = PLC_PER_SEC).await;
        add_host(&*hooks.hosts, host, Tier::Trusted).await;
        hooks.load().await.unwrap();

        // one commit per account, each with prevData: none is its repo's first
        let mut makers = Vec::new();
        for t in 0..8usize {
            makers.push(tokio::task::spawn_blocking(move || {
                (t..DIDS)
                    .step_by(8)
                    .map(|i| {
                        let did = crate::state::tests::plc(i as u64 + 1);
                        let mut r = Repo::new(&did, Signer::new(Curve::K256, 7), 1);
                        let ops = r.mixed_ops(1);
                        (did, r.commit(&ops))
                    })
                    .collect::<Vec<_>>()
            }));
        }
        let mut frames = Vec::with_capacity(DIDS);
        for g in makers {
            frames.extend(g.await.unwrap());
        }
        let frames = Arc::new(frames);

        let t0 = Instant::now();
        let next = Arc::new(std::sync::atomic::AtomicUsize::new(0));
        let mut workers = Vec::new();
        for _ in 0..64 {
            let (frames, next, identity, state) = (frames.clone(), next.clone(), identity.clone(), state.clone());
            workers.push(tokio::spawn(async move {
                let mut out = Vec::new();
                loop {
                    let i = next.fetch_add(1, Relaxed);
                    let Some((did, frame)) = frames.get(i) else { return out };
                    let crate::event::Event::Commit(c) =
                        crate::event::parse(frame.clone(), &crate::event::Limits::default()).unwrap()
                    else {
                        panic!("not a commit")
                    };
                    let id = identity.lookup_paced(did, false).await.unwrap();
                    let v = crate::verify::verify_commit(&c, id.signing_key.as_ref().unwrap()).unwrap();
                    assert!(!v.created && v.prev_data.is_some());
                    let h = Host(host.into());
                    let r = state
                        .apply(state::Incoming {
                            did,
                            host: &h,
                            now: state::now_secs(),
                            kind: state::EventKind::Commit(v),
                        })
                        .await;
                    out.push(r);
                }
            }));
        }
        let mut accepted = 0;
        let mut rejects = HashMap::new();
        for w in workers {
            for r in w.await.unwrap() {
                match r {
                    Ok(state::Applied::Append(a)) if a.new_account => accepted += 1,
                    other => *rejects.entry(format!("{other:?}")).or_insert(0) += 1,
                }
            }
        }
        let took = t0.elapsed();
        assert!(rejects.is_empty(), "{rejects:?}");
        assert_eq!(accepted, DIDS);
        assert_eq!(fetch.fetches.load(Relaxed), DIDS as u64);
        // a second of budget in the bucket, then PLC_PER_SEC: ~4 s at the least
        let floor = (DIDS as f64 - PLC_PER_SEC) / PLC_PER_SEC;
        assert!(took.as_secs_f64() >= floor * 0.9, "{took:?}");
        assert!(identity.stats.over_budget.load(Relaxed) > 0, "the budget never ran out in {took:?}");
        assert_eq!(hooks.accounts(host), Some(DIDS as u64));
        let now = policy::store::now_ms();
        assert_eq!(hooks.engine.signals.snapshot(host, None, now).get("new-accounts"), None);
        assert!(hooks.deferred.lock().is_empty());
    }

    #[tokio::test]
    async fn host_policy_follows_records_actions_and_rules() {
        let (hooks, _) = setup().await;
        add_host(&*hooks.hosts, "a.example", Tier::Default).await;
        add_host(&*hooks.hosts, "b.spam.example", Tier::Trusted).await;
        hooks.load().await.unwrap();
        let a = hp(&hooks, "a.example");
        assert_eq!((a.tier, a.connect), (upstream::Tier::Default, true));
        assert_eq!(a.limits.unwrap().events_per_hour, 3_500.0);

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
        let (hooks, _) = setup().await;
        edit_policy(&hooks, |p| {
            p.spam.host_failed_validation.limit = 10.0;
        })
        .await;
        add_host(&*hooks.hosts, "bad.example", Tier::Default).await;
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
        let (hooks, _) = setup().await;
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
            async move { hooks.admit(&h, true).await }
        };
        let check = |h: &str| {
            let h = Host(h.to_string());
            let hooks = hooks.clone();
            async move { hooks.admit(&h, false).await }
        };
        assert_eq!(admit("x.spam.example").await, Err(CrawlError::HostBanned));
        assert_eq!(check("x.spam.example").await, Err(CrawlError::HostBanned));
        // the check before a probe spends nothing
        for _ in 0..3 {
            assert_eq!(check("zero.example").await, Ok(upstream::Tier::New));
        }
        assert_eq!(admit("one.example").await, Ok(upstream::Tier::New));
        assert_eq!(check("two.example").await, Err(CrawlError::Budget));
        assert_eq!(admit("two.example").await, Err(CrawlError::Budget));
        // trusted domains skip the budget and start trusted
        assert_eq!(admit("morel.us-east.host.bsky.network").await, Ok(upstream::Tier::Trusted));
        // a known host keeps its tier, unless it's banned
        add_host(&*hooks.hosts, "known.example", Tier::Default).await;
        assert_eq!(admit("known.example").await, Ok(upstream::Tier::Default));
        add_host(&*hooks.hosts, "gone.example", Tier::Banned).await;
        assert_eq!(admit("gone.example").await, Err(CrawlError::HostBanned));
        assert!(matches!(admit("localhost").await, Err(CrawlError::Refused(_))));
    }

    #[tokio::test]
    async fn the_manager_disconnects_and_reconnects_with_the_policy() {
        let (hooks, _) = setup().await;
        add_host(&*hooks.hosts, "127.0.0.1:9", Tier::Default).await;
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

    /// An upstream that says it's a relay is refused once and banned through
    /// the operator's ban action (so listHosts says `banned` and the action
    /// is on the record), and never dialed again.
    #[tokio::test]
    async fn a_relay_upstream_is_banned_not_retried() {
        let (hooks, _) = setup().await;
        let h = crate::upstream::client::tests::ws_server(Some("indigo-relay/v0.0.0 (atproto-relay)")).await;
        add_host(&*hooks.hosts, &h.0, Tier::Default).await;
        hooks.load().await.unwrap();
        let mut cfg = upstream::UpstreamConfig::new(true);
        cfg.endpoint = Arc::new(|h: &Host| format!("http://{}", h.0));
        cfg.backoff_base = Duration::from_millis(10);
        let store: Arc<dyn upstream::HostStore> = Arc::new(upstream::MemHostStore::default());
        let (m, _rx) = Manager::new(cfg, store, None);
        m.set_policy_source(hooks.clone());
        let _ = hooks.manager.set(Arc::downgrade(&m));
        hooks.ban_refusals(&m);
        m.start().await.unwrap();
        m.admit(&h, upstream::Tier::Default).await.unwrap();
        let t = Instant::now();
        loop {
            let rec = hooks.hosts.get_host(&h.0).await.unwrap().unwrap();
            if rec.tier == Tier::Banned {
                assert_eq!(rec.lexicon_status(), "banned");
                let actions = PolicyAdmin::host_actions(&rec);
                assert!(matches!(&actions[..], [a] if a.by == "vlrelay"), "{actions:?}");
                break;
            }
            assert!(t.elapsed() < Duration::from_secs(5), "not banned");
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
        tokio::time::sleep(Duration::from_millis(300)).await;
        assert_eq!(m.running(), 0);
        let e = m.host(&h).unwrap().record;
        assert_eq!((e.tier, e.errors.connect), (upstream::Tier::Banned, 1));
        m.shutdown().await.unwrap();
    }
}
