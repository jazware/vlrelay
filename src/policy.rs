//! The policy engine: host tiers and limits, domain rules, requestCrawl
//! admission, cluster budgets, spam signals and cases. docs/policy.md has
//! the integration points for the other modules.
//!
//! The policy and the domain rules are two versioned objects in the bucket
//! (`policy/current.json`, `policy/domain-rules.json`). Every node polls
//! them with conditional GETs and swaps in a new [`Snapshot`] when either
//! changes, so the hot-path calls ([`Engine::for_host`],
//! [`Engine::record_signal`], [`Engine::try_take`]) only read memory.

pub mod admin;
pub mod budget;
pub mod cases;
pub mod doc;
pub mod driver;
pub mod rules;
pub mod signals;
pub mod store;
pub mod takedowns;
pub mod tiers;

#[cfg(test)]
mod tests;

pub use budget::{BudgetKind, FixedNodes, LiveNodes};
pub use doc::{PolicyBody, SpamAction, TierLimits};
pub use rules::{Rule, RuleEffect, RuleSet};
pub use signals::{Signal, SignalKind, SpamRule, Trip};
pub use store::{AuditEntry, SaveError, Stored};

use crate::state::{HostRecord, Tier};
use crate::types::Host;
use parking_lot::{Mutex, RwLock};
use std::sync::Arc;
use std::time::Duration;
use store::{Fetched, Versioned, now_ms};
use vlpds::store::Store;

/// A lost nudge or a node that missed a save catches up within this.
pub const REFRESH_EVERY: Duration = Duration::from_secs(10);

pub const POLICY_PATH: &str = "policy/current.json";
pub const POLICY_AUDIT: &str = "policy/audit";
pub const RULES_PATH: &str = "policy/domain-rules.json";
pub const RULES_AUDIT: &str = "policy/domain-rules-audit";
pub const NEW_HOSTS_PATH: &str = "policy/counters/new-hosts.json";

/// The policy in force on this node.
pub struct Snapshot {
    pub policy: Stored<PolicyBody>,
    pub rules: rules::Compiled,
    pub rules_version: u64,
}

/// The limits the host owner enforces, with the rule and tier that set them.
#[derive(Clone, Debug, PartialEq)]
pub struct HostLimits {
    /// After domain rules: what the host actually runs as.
    pub tier: Tier,
    /// False for suspended and banned hosts: no socket at all.
    pub connect: bool,
    /// None when `connect` is false. An operator or rule throttle is
    /// already folded into `events_per_sec`.
    pub limits: Option<TierLimits>,
    pub rule: Option<u64>,
    pub policy_version: u64,
}

#[derive(Clone, Debug)]
pub struct AdmitRequest<'a> {
    /// As the caller sent it (scheme, path and case are normalized away).
    pub hostname: &'a str,
    /// The operator API's requestCrawl: skips the crawl switch, the insecure
    /// check and the daily budget, as indigo's admin route does.
    pub by_admin: bool,
    /// The host's record, if the relay already knows it.
    pub existing: Option<&'a HostRecord>,
}

#[derive(Clone, Debug, PartialEq)]
pub enum Admit {
    Admit {
        host: Host,
        /// The tier to create the record with (ignored for known hosts).
        tier: Tier,
        /// Whether this took one of today's new-host admissions.
        counted: bool,
    },
    Reject(RejectHost),
}

#[derive(Clone, Debug, PartialEq, thiserror::Error)]
pub enum RejectHost {
    #[error("requestCrawl is disabled on this relay")]
    CrawlDisabled,
    #[error("{0}")]
    BadHostname(String),
    #[error("localhost is only accepted from an operator")]
    Localhost,
    #[error("{hostname} is banned ({why})")]
    Banned { hostname: String, why: String },
    #[error("this relay only accepts allow-listed hosts")]
    NotAllowed,
    #[error("the relay has admitted its {limit} new hosts for today; try again tomorrow")]
    DailyLimit { limit: u32 },
    /// The daily counter couldn't be read or written. Refusing is the safe
    /// side, unlike indigo, which answers 200 when its ban lookup fails.
    #[error("policy store unavailable: {0}")]
    Store(String),
}

pub struct Engine {
    store: Store,
    pub node: String,
    snap: RwLock<Arc<Snapshot>>,
    policy_obj: Versioned,
    rules_obj: Versioned,
    /// ETags last fetched (policy, rules).
    seen: Mutex<(Option<String>, Option<String>)>,
    /// Serializes loads and saves on this node, so a slow load can't install
    /// an older version over a newer one.
    io: tokio::sync::Mutex<()>,
    wake: tokio::sync::Notify,
    pub signals: signals::Signals,
    live: Arc<dyn LiveNodes>,
    plc: budget::Bucket,
    new_accounts: budget::Bucket,
    new_hosts: budget::DailyCounter,
    pub cases: cases::CaseStore,
    pub takedowns: takedowns::Takedowns,
    /// The last load error, while the newest object is rejected.
    pub last_error: Mutex<Option<String>>,
}

impl Engine {
    /// Starts on the defaults; call [`Engine::refresh`] (or
    /// [`Engine::spawn_refresher`]) to load what's stored.
    pub fn new(store: Store, node: &str, live: Arc<dyn LiveNodes>) -> Arc<Engine> {
        let policy = Stored::<PolicyBody>::initial();
        Arc::new(Engine {
            signals: signals::Signals::new(&policy.body.spam),
            snap: RwLock::new(Arc::new(Snapshot {
                policy,
                rules: rules::Compiled::default(),
                rules_version: 0,
            })),
            policy_obj: Versioned::new(store.clone(), POLICY_PATH, POLICY_AUDIT),
            rules_obj: Versioned::new(store.clone(), RULES_PATH, RULES_AUDIT),
            new_hosts: budget::DailyCounter::new(store.clone(), NEW_HOSTS_PATH),
            cases: cases::CaseStore::new(store.clone()),
            takedowns: takedowns::Takedowns::new(store.clone()),
            store,
            node: node.to_string(),
            seen: Default::default(),
            io: Default::default(),
            wake: Default::default(),
            live,
            plc: Default::default(),
            new_accounts: Default::default(),
            last_error: Default::default(),
        })
    }

    pub fn store(&self) -> &Store {
        &self.store
    }

    pub fn snapshot(&self) -> Arc<Snapshot> {
        self.snap.read().clone()
    }

    pub fn policy(&self) -> Stored<PolicyBody> {
        self.snapshot().policy.clone()
    }

    pub fn rules(&self) -> (u64, RuleSet) {
        let s = self.snapshot();
        (s.rules_version, s.rules.set.clone())
    }

    fn install(&self, policy: Option<Stored<PolicyBody>>, rules: Option<Stored<RuleSet>>) {
        let mut w = self.snap.write();
        let cur = w.clone();
        let policy = policy.unwrap_or_else(|| cur.policy.clone());
        if policy.body.spam != cur.policy.body.spam {
            self.signals.reconfigure(&policy.body.spam);
        }
        let (rules, rules_version) = match rules {
            Some(r) => (rules::Compiled::new(r.body), r.version),
            None => (
                rules::Compiled::new(cur.rules.set.clone()),
                cur.rules_version,
            ),
        };
        *w = Arc::new(Snapshot {
            policy,
            rules,
            rules_version,
        });
    }

    /// Re-reads both objects (conditional on the last ETags) and installs
    /// whatever changed and validates. Returns whether anything changed. An
    /// invalid object never takes a node down: it keeps its last good copy.
    pub async fn refresh(&self) -> anyhow::Result<bool> {
        let _io = self.io.lock().await;
        let (pe, re) = self.seen.lock().clone();
        let mut changed = false;
        let mut errors = Vec::new();
        let p = match self.policy_obj.fetch::<PolicyBody>(pe).await? {
            Fetched::Got { doc, etag } => {
                self.seen.lock().0 = etag;
                match doc::validate(&doc.body) {
                    Ok(()) if doc.version != self.snapshot().policy.version => Some(doc),
                    Ok(()) => None,
                    Err(e) => {
                        errors.push(format!("policy v{}: {}", doc.version, e.join("; ")));
                        None
                    }
                }
            }
            Fetched::Invalid {
                version,
                message,
                etag,
            } => {
                self.seen.lock().0 = etag;
                errors.push(format!("policy v{version:?}: {message}"));
                None
            }
            Fetched::NotModified | Fetched::Absent => None,
        };
        let r = match self.rules_obj.fetch::<RuleSet>(re).await? {
            Fetched::Got { doc, etag } => {
                self.seen.lock().1 = etag;
                match rules::validate(&doc.body) {
                    Ok(()) if doc.version != self.snapshot().rules_version => Some(doc),
                    Ok(()) => None,
                    Err(e) => {
                        errors.push(format!("rules v{}: {}", doc.version, e.join("; ")));
                        None
                    }
                }
            }
            Fetched::Invalid {
                version,
                message,
                etag,
            } => {
                self.seen.lock().1 = etag;
                errors.push(format!("rules v{version:?}: {message}"));
                None
            }
            Fetched::NotModified | Fetched::Absent => None,
        };
        if p.is_some() || r.is_some() {
            if let Some(p) = &p {
                tracing::info!(version = p.version, "policy applied");
            }
            if let Some(r) = &r {
                tracing::info!(version = r.version, "domain rules applied");
            }
            self.install(p, r);
            changed = true;
        }
        for e in &errors {
            tracing::warn!("policy object rejected (keeping the last good one): {e}");
        }
        *self.last_error.lock() = (!errors.is_empty()).then(|| errors.join("; "));
        Ok(changed)
    }

    /// A load now, then every [`REFRESH_EVERY`] or on [`Engine::wake`].
    /// Stops when the engine is dropped.
    pub fn spawn_refresher(self: &Arc<Self>) {
        let weak = Arc::downgrade(self);
        tokio::spawn(async move {
            loop {
                let Some(e) = weak.upgrade() else { return };
                if let Err(err) = e.refresh().await {
                    tracing::warn!("policy refresh failed (retrying): {err:#}");
                }
                let wait = async {
                    tokio::select! {
                        _ = tokio::time::sleep(REFRESH_EVERY) => {}
                        _ = e.wake.notified() => {}
                    }
                };
                wait.await;
            }
        });
    }

    /// Asks the refresher to re-read now (a peer saved a change).
    pub fn wake(&self) {
        self.wake.notify_one();
    }

    pub async fn save_policy(
        &self,
        base_version: u64,
        body: PolicyBody,
        by: &str,
        note: &str,
    ) -> Result<Stored<PolicyBody>, SaveError> {
        let _io = self.io.lock().await;
        let doc = self
            .policy_obj
            .save(base_version, body, by, note, doc::validate)
            .await?;
        self.install(Some(doc.clone()), None);
        Ok(doc)
    }

    pub async fn save_rules(
        &self,
        base_version: u64,
        set: RuleSet,
        by: &str,
        note: &str,
    ) -> Result<Stored<RuleSet>, SaveError> {
        let _io = self.io.lock().await;
        let doc = self
            .rules_obj
            .save(base_version, set, by, note, rules::validate)
            .await?;
        self.install(None, Some(doc.clone()));
        Ok(doc)
    }

    pub async fn policy_audit(&self, limit: usize) -> anyhow::Result<Vec<AuditEntry>> {
        self.policy_obj.audit(limit).await
    }

    pub async fn rules_audit(&self, limit: usize) -> anyhow::Result<Vec<AuditEntry>> {
        self.rules_obj.audit(limit).await
    }

    // ------------------------------------------------------------ enforcement

    /// The limits for a host, from its record, the domain rules and the
    /// policy. Cheap (two hash probes per hostname label); the upstream
    /// module can call it per connection and again when the policy version
    /// changes.
    pub fn for_host(&self, rec: &HostRecord) -> HostLimits {
        let snap = self.snapshot();
        let p = &snap.policy.body;
        let rule = snap.rules.lookup(&rec.hostname);
        let hp = tiers::host_policy(rec);
        let mut tier = rec.tier;
        let mut cap = hp.throttle_eps;
        if !matches!(tier, Tier::Suspended | Tier::Banned) {
            match rule.map(|r| &r.effect) {
                Some(RuleEffect::Ban) => tier = Tier::Banned,
                Some(RuleEffect::Tier { tier: t }) if tier != Tier::Throttled => tier = *t,
                Some(RuleEffect::Throttle { events_per_sec }) => {
                    cap = Some(cap.map_or(*events_per_sec, |c| c.min(*events_per_sec)))
                }
                _ => {}
            }
        }
        let limits = p.tiers.get(tier).cloned().map(|mut l| {
            if let Some(c) = cap {
                l.events_per_sec = l.events_per_sec.min(c);
            }
            l
        });
        HostLimits {
            tier,
            connect: limits.is_some(),
            limits,
            rule: rule.map(|r| r.id),
            policy_version: snap.policy.version,
        }
    }

    /// The domain rule that applies to a hostname, if any.
    pub fn rule_for(&self, hostname: &str) -> Option<Rule> {
        self.snapshot().rules.lookup(hostname).cloned()
    }

    /// requestCrawl admission, in indigo's order (`handlers.go:L19-L69`),
    /// minus `describeServer`, which the caller makes after an `Admit`.
    /// Spends one of today's new-host admissions for a new, non-admin host.
    pub async fn admit_host(&self, req: &AdmitRequest<'_>) -> Admit {
        let snap = self.snapshot();
        let crawl = &snap.policy.body.crawl;
        if !crawl.enabled && !req.by_admin {
            return Admit::Reject(RejectHost::CrawlDisabled);
        }
        let parsed = match rules::parse_hostname(req.hostname) {
            Ok(p) => p,
            Err(e) => return Admit::Reject(RejectHost::BadHostname(e.to_string())),
        };
        if parsed.insecure && !crawl.allow_insecure && !req.by_admin {
            return Admit::Reject(RejectHost::BadHostname(
                rules::HostnameError::Insecure.to_string(),
            ));
        }
        let hostname = parsed.hostname;
        if hostname.starts_with("localhost") && !req.by_admin {
            return Admit::Reject(RejectHost::Localhost);
        }
        let rule = snap.rules.lookup(&hostname);
        if let Some(r) = rule
            && r.effect == RuleEffect::Ban
        {
            return Admit::Reject(RejectHost::Banned {
                hostname,
                why: format!("domain rule {} ({})", r.id, r.pattern),
            });
        }
        if let Some(rec) = req.existing {
            if matches!(rec.tier, Tier::Banned | Tier::Suspended) && !req.by_admin {
                return Admit::Reject(RejectHost::Banned {
                    hostname,
                    why: doc::tier_name(rec.tier).into(),
                });
            }
            return Admit::Admit {
                host: Host(hostname),
                tier: rec.tier,
                counted: false,
            };
        }
        let trusted = crawl
            .trusted_domains
            .iter()
            .any(|d| rules::pattern_matches(d, &hostname));
        let allowed = trusted || matches!(rule.map(|r| &r.effect), Some(RuleEffect::Allow));
        if crawl.allowlist_only && !allowed && !req.by_admin {
            return Admit::Reject(RejectHost::NotAllowed);
        }
        let tier = match rule.map(|r| &r.effect) {
            Some(RuleEffect::Tier { tier }) => *tier,
            _ if trusted => Tier::Trusted,
            _ => crawl.initial_tier,
        };
        let counted = !req.by_admin && !allowed;
        if counted {
            let limit = snap.policy.body.cluster.new_hosts_per_day;
            match self.new_hosts.spend(limit, now_ms()).await {
                Ok(budget::Spend::Spent(_)) => {}
                Ok(budget::Spend::Exhausted(_)) => {
                    return Admit::Reject(RejectHost::DailyLimit { limit });
                }
                Err(e) => return Admit::Reject(RejectHost::Store(e.to_string())),
            }
        }
        Admit::Admit {
            host: Host(hostname),
            tier,
            counted,
        }
    }

    /// New hosts admitted today, cluster-wide.
    pub async fn new_hosts_today(&self) -> anyhow::Result<u32> {
        self.new_hosts.read(now_ms()).await
    }

    /// This node's share of a cluster budget: the budget ÷ live nodes for
    /// the fast kinds, the whole daily budget for new hosts.
    pub fn budget(&self, kind: BudgetKind) -> f64 {
        budget::share(
            &self.snapshot().policy.body.cluster,
            kind,
            self.live.live_nodes(),
        )
    }

    /// Takes `n` from this node's share of a fast budget. False: over it,
    /// so wait or skip (a PLC lookup can be retried, a new account can be
    /// created `host-throttled`).
    pub fn try_take(&self, kind: BudgetKind, n: f64) -> bool {
        let share = self.budget(kind);
        let now = now_ms();
        match kind {
            BudgetKind::PlcLookupsPerSec => self.plc.try_take(share, n, now),
            BudgetKind::NewAccountsPerMin => self.new_accounts.try_take(share / 60.0, n, now),
            // the archive's fetch queue spends these itself
            BudgetKind::NewHostsPerDay | BudgetKind::ArchivalFetchConcurrency | BudgetKind::ArchivalFetchBytesPerSec => {
                true
            }
        }
    }

    pub fn consumer_limits(&self) -> doc::Consumers {
        self.snapshot().policy.body.consumers.clone()
    }

    /// Counts a spam signal. Returns the thresholds it tripped (each at most
    /// once per key per window); they're also queued for the driver, which
    /// throttles and opens cases. A few hash probes and a mutex per call.
    pub fn record_signal(&self, s: Signal<'_>) -> Vec<Trip> {
        self.record_signal_at(s, now_ms())
    }

    pub fn record_signal_at(&self, s: Signal<'_>, now_ms: i64) -> Vec<Trip> {
        self.signals.record(&s, now_ms)
    }
}
