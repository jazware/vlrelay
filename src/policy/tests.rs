use super::admin::{PolicyAdmin, merge_wire, to_wire};
use super::cases::{CaseOpen, Evidence, Opened};
use super::doc::{PolicyBody, SpamAction};
use super::driver::Driver;
use super::rules::{Compiled, RuleEffect, parse_hostname};
use super::signals::{Signal, SignalKind, Signals, SpamRule, TopK};
use super::tiers::{self, HostPolicy, Manual, Obs, step};
use super::*;
use crate::admin::{CaseStatus, HostAction, Severity};
use crate::state::{HostCounts, HostPage, HostRecord, HostStore, Tier};
use object_store::{ObjectStoreExt, PutPayload};
use std::collections::BTreeMap;

const DAY: u32 = 86_400;

// ---------------------------------------------------------------- helpers

#[derive(Default)]
struct MemHosts(parking_lot::Mutex<BTreeMap<String, HostRecord>>);

#[async_trait::async_trait]
impl HostStore for MemHosts {
    async fn get_host(&self, h: &str) -> anyhow::Result<Option<HostRecord>> {
        Ok(self.0.lock().get(h).cloned())
    }
    async fn put_host(&self, rec: &HostRecord) -> anyhow::Result<()> {
        self.0.lock().insert(rec.hostname.clone(), rec.clone());
        Ok(())
    }
    async fn checkpoint_cursors(&self, _: &[(String, i64)]) -> anyhow::Result<()> {
        Ok(())
    }
    async fn add_counts(&self, counts: &[(String, HostCounts)]) -> anyhow::Result<()> {
        let mut m = self.0.lock();
        for (h, c) in counts {
            if let Some(r) = m.get_mut(h) {
                r.events += c.events;
                r.failed_checks += c.failed_checks;
            }
        }
        Ok(())
    }
    async fn list_hosts(&self, cursor: Option<&str>, limit: usize) -> anyhow::Result<HostPage> {
        let m = self.0.lock();
        let hosts: Vec<HostRecord> =
            m.values().filter(|r| cursor.is_none_or(|c| r.hostname.as_str() > c)).take(limit).cloned().collect();
        let cursor = (hosts.len() == limit).then(|| hosts.last().unwrap().hostname.clone());
        Ok(HostPage { hosts, cursor })
    }
}

fn engine(store: &Store, node: &str) -> Arc<Engine> {
    Engine::new(store.clone(), node, Arc::new(FixedNodes::new(1)))
}

fn rec(host: &str, tier: Tier, first_seen: u32) -> HostRecord {
    HostRecord::new(host, tier, first_seen)
}

fn p() -> PolicyBody {
    PolicyBody::default()
}

// ---------------------------------------------------------------- tiers

#[test]
fn new_hosts_are_promoted_after_clean_days() {
    let p = p();
    let st = HostPolicy::default();
    let quiet = Obs::default();
    assert_eq!(step(Tier::New, 0, &st, &quiet, &p, 6 * DAY), None);
    let ch = step(Tier::New, 0, &st, &quiet, &p, 7 * DAY).unwrap();
    assert_eq!(ch.tier, Tier::Default);
    assert!(ch.reason.unwrap().contains("7 clean days"));
    // a trip on day 5 holds promotion until day 12
    let tripped = HostPolicy { last_trip: Some(5 * DAY), ..Default::default() };
    assert_eq!(step(Tier::New, 0, &tripped, &quiet, &p, 8 * DAY), None);
    assert_eq!(step(Tier::New, 0, &tripped, &quiet, &p, 12 * DAY).unwrap().tier, Tier::Default);
}

#[test]
fn error_budget_throttles_and_quiet_recovers() {
    let p = p();
    let now = 100 * DAY;
    let bad = Obs { events: 100, failed: 150, spam_trip: None };
    let ch = step(Tier::Default, 0, &HostPolicy::default(), &bad, &p, now).unwrap();
    assert_eq!(ch.tier, Tier::Throttled);
    assert_eq!(ch.state.restore_tier, Some(Tier::Default));
    assert_eq!(ch.state.trips, 1);
    assert!(ch.reason.unwrap().contains("60% of 250 frames"));

    // still bad while throttled: no tier change, quiet period restarts
    let again = step(Tier::Throttled, 0, &ch.state, &bad, &p, now + 600).unwrap();
    assert_eq!((again.tier, again.reason.as_deref()), (Tier::Throttled, None));
    assert_eq!(again.state.last_trip, Some(now + 600));

    let quiet = Obs::default();
    let rs = p.transitions.recover_after_secs;
    assert_eq!(step(Tier::Throttled, 0, &again.state, &quiet, &p, now + 600 + rs - 1), None);
    let back = step(Tier::Throttled, 0, &again.state, &quiet, &p, now + 600 + rs).unwrap();
    assert_eq!(back.tier, Tier::Default);
    assert_eq!(back.state.restore_tier, None);
    assert_eq!(back.state.trips, 2, "trip history is kept");
}

#[test]
fn small_hosts_and_exempt_tiers_are_not_auto_throttled() {
    let p = p();
    // under errorMinEvents: one bad commit from a tiny PDS
    let tiny = Obs { events: 1, failed: 5, spam_trip: None };
    assert_eq!(step(Tier::New, 0, &HostPolicy::default(), &tiny, &p, DAY), None);
    // trusted records the trip but stays
    let spam = Obs { spam_trip: Some("new-accounts".into()), ..Default::default() };
    let ch = step(Tier::Trusted, 0, &HostPolicy::default(), &spam, &p, DAY).unwrap();
    assert_eq!(ch.tier, Tier::Trusted);
    assert_eq!(ch.state.last_trip, Some(DAY));
    // a spam trip throttles a new host
    let ch = step(Tier::New, 0, &HostPolicy::default(), &spam, &p, DAY).unwrap();
    assert_eq!(ch.tier, Tier::Throttled);
    assert_eq!(ch.state.restore_tier, Some(Tier::New));
    // suspended and banned never move on their own
    for t in [Tier::Suspended, Tier::Banned] {
        assert_eq!(step(t, 0, &HostPolicy::default(), &spam, &p, DAY), None);
        assert_eq!(step(t, 0, &HostPolicy::default(), &Obs::default(), &p, 30 * DAY), None);
    }
}

#[test]
fn operator_tiers_are_final_and_unban_restores() {
    let p = p();
    let mut r = rec("pds.example.com", Tier::Default, 0);
    tiers::apply_manual(&mut r, &Manual::SetTier(Tier::Throttled), DAY).unwrap();
    assert_eq!(r.tier, Tier::Throttled);
    let st = tiers::host_policy(&r);
    assert_eq!(st.restore_tier, None);
    // never auto-recovers
    assert_eq!(step(r.tier, 0, &st, &Obs::default(), &p, 90 * DAY), None);

    // auto-throttled, then banned, then unbanned: back to the original tier
    let mut r = rec("pds.example.com", Tier::New, 0);
    let ch =
        step(r.tier, 0, &HostPolicy::default(), &Obs { spam_trip: Some("x".into()), ..Default::default() }, &p, DAY)
            .unwrap();
    r.tier = ch.tier;
    tiers::set_host_policy(&mut r, &ch.state);
    tiers::apply_manual(&mut r, &Manual::Ban("spam".into()), DAY).unwrap();
    assert_eq!(r.tier, Tier::Banned);
    tiers::apply_manual(&mut r, &Manual::Unban, DAY).unwrap();
    assert_eq!(r.tier, Tier::New);
    assert!(tiers::apply_manual(&mut r, &Manual::Unban, DAY).is_err());
    assert!(tiers::apply_manual(&mut r, &Manual::SetTier(Tier::Banned), DAY).is_err());
    assert!(tiers::apply_manual(&mut r, &Manual::Throttle(Some(-1.0)), DAY).is_err());
    // the record round-trips through JSON with the policy state in `extra`
    let back: HostRecord = serde_json::from_slice(&serde_json::to_vec(&r).unwrap()).unwrap();
    assert_eq!(tiers::host_policy(&back), tiers::host_policy(&r));
}

// ---------------------------------------------------------------- CAS + audit

#[tokio::test]
async fn concurrent_policy_edits_conflict() {
    let store = Store::memory(None);
    let (a, b) = (engine(&store, "a"), engine(&store, "b"));
    let mut pa = p();
    pa.cluster.new_hosts_per_day = 10;
    let mut pb = p();
    pb.cluster.new_hosts_per_day = 20;
    let d = a.save_policy(0, pa, "alice", "fewer hosts").await.unwrap();
    assert_eq!(d.version, 1);
    // b edited version 0 too
    match b.save_policy(0, pb.clone(), "bob", "").await {
        Err(SaveError::Conflict { expected: 0, current: 1 }) => {}
        other => panic!("{other:?}"),
    }
    // at once, from eight writers on version 1: exactly one wins
    let mut tasks = Vec::new();
    for i in 0..8u32 {
        let e = engine(&store, &format!("n{i}"));
        let mut body = p();
        body.cluster.new_hosts_per_day = 100 + i;
        tasks.push(tokio::spawn(async move { e.save_policy(1, body, "x", "").await }));
    }
    let mut ok = 0;
    for t in tasks {
        match t.await.unwrap() {
            Ok(d) => {
                assert_eq!(d.version, 2);
                ok += 1;
            }
            Err(SaveError::Conflict { .. }) => {}
            Err(e) => panic!("{e}"),
        }
    }
    assert_eq!(ok, 1);
    // invalid bodies never reach the store
    let mut bad = p();
    bad.tiers.default.events_per_sec = 0.0;
    assert!(matches!(a.save_policy(2, bad, "x", "").await, Err(SaveError::Invalid(_))));
    assert!(matches!(a.save_policy(2, p(), "x", "").await.map(|d| d.version), Ok(3)));
    assert!(matches!(a.save_policy(3, p(), "x", "").await, Err(SaveError::NoChange)));
}

#[tokio::test]
async fn audit_log_lists_every_changed_leaf() {
    let store = Store::memory(None);
    let e = engine(&store, "a");
    let mut body = p();
    body.tiers.default.events_per_sec = 80.0;
    e.save_policy(0, body.clone(), "alice", "raise default").await.unwrap();
    body.crawl.trusted_domains.push("*.example.net".into());
    body.spam.host_new_accounts.action = SpamAction::Case;
    e.save_policy(1, body.clone(), "bob", "").await.unwrap();

    let audit = e.policy_audit(10).await.unwrap();
    assert_eq!(audit.iter().map(|a| a.version).collect::<Vec<_>>(), vec![2, 1]);
    assert_eq!(audit[1].by, "alice");
    assert_eq!(audit[1].note, "raise default");
    // the first save diffs against the defaults
    assert_eq!(audit[1].changes, vec!["tiers.default.eventsPerSec: 51.0 → 80.0".to_string()]);
    assert_eq!(
        audit[0].changes,
        vec![
            "crawl.trustedDomains: [\"*.host.bsky.network\"] → [\"*.host.bsky.network\",\"*.example.net\"]".to_string(),
            "spam.hostNewAccounts.action: \"throttle-and-case\" → \"case\"".to_string(),
        ],
        "{:?}",
        audit[0].changes
    );

    // A crash between the CAS and the audit write: the next save fills it in.
    let p2 = store::path(&store, &format!("{POLICY_AUDIT}/{:020}.json", 2));
    store.raw.delete(&p2).await.unwrap();
    assert_eq!(e.policy_audit(10).await.unwrap().len(), 1);
    body.consumers.connections_per_ip = 4;
    e.save_policy(2, body, "carol", "").await.unwrap();
    let audit = e.policy_audit(10).await.unwrap();
    assert_eq!(audit.iter().map(|a| a.version).collect::<Vec<_>>(), vec![3, 2, 1]);
    assert_eq!(audit[1].by, "bob");

    // rules keep their own log
    let admin = PolicyAdmin::new(e.clone(), Arc::new(MemHosts::default()));
    admin
        .create_domain_rule(
            crate::admin::DomainRuleInput {
                pattern: "*.Spam.example".into(),
                effect: crate::admin::RuleEffect::Ban,
                note: "farm".into(),
            },
            "dave",
        )
        .await
        .unwrap();
    admin.delete_domain_rule(1, "erin").await.unwrap();
    let ra = admin.domain_rules_audit().await.unwrap();
    assert_eq!(ra.iter().map(|a| (a.version, a.by.as_str())).collect::<Vec<_>>(), vec![(2, "erin"), (1, "dave")]);
    assert_eq!(ra[0].note, "delete rule 1");
    assert_eq!(e.policy_audit(10).await.unwrap().len(), 3);
}

// ---------------------------------------------------------------- rules

#[test]
fn rule_matching_prefers_the_most_specific() {
    let mk = |id, pattern: &str, effect| Rule {
        id,
        pattern: pattern.into(),
        effect,
        note: String::new(),
        created_at_ms: 0,
        created_by: "t".into(),
    };
    let set = RuleSet {
        next_id: 10,
        rules: vec![
            mk(1, "*.spam.example", RuleEffect::Ban),
            mk(2, "good.spam.example", RuleEffect::Allow),
            mk(3, "*.eu.spam.example", RuleEffect::Tier { tier: Tier::New }),
            mk(4, "exact.example.org", RuleEffect::Throttle { events_per_sec: 3.0 }),
        ],
    };
    rules::validate(&set).unwrap();
    let c = Compiled::new(set);
    let id = |h: &str| c.lookup(h).map(|r| r.id);
    assert_eq!(id("spam.example"), Some(1));
    assert_eq!(id("a.b.spam.example"), Some(1));
    assert_eq!(id("good.spam.example"), Some(2));
    assert_eq!(id("x.good.spam.example"), Some(1), "exact rules don't cover subdomains");
    assert_eq!(id("pds.eu.spam.example"), Some(3));
    assert_eq!(id("exact.example.org"), Some(4));
    assert_eq!(id("sub.exact.example.org"), None);
    assert_eq!(id("notspam.example"), None);
    assert_eq!(id("example"), None);

    assert_eq!(rules::normalize_pattern(" *.Example.COM. ").unwrap(), "*.example.com");
    for bad in ["com", "*.com", "a..b", "-a.com", "*.1.2.3.4", "*.*.x.com", "", "x.com:0", "x.com:http", "*.x.com:443"]
    {
        assert!(rules::normalize_pattern(bad).is_err(), "{bad}");
    }
    // one host by address and port, as a dev network or a PDS on a port is known
    for ok in ["1.2.3.4", "127.0.0.1:30003", "localhost:2583", "pds.example.com:8443"] {
        assert_eq!(rules::normalize_pattern(ok).as_deref(), Ok(ok));
    }
    let ports = Compiled::new(RuleSet {
        next_id: 3,
        rules: vec![mk(1, "127.0.0.1:30003", RuleEffect::Ban), mk(2, "10.0.0.1", RuleEffect::Ban)],
    });
    assert_eq!(ports.lookup("127.0.0.1:30003").map(|r| r.id), Some(1));
    assert_eq!(ports.lookup("127.0.0.1:30004").map(|r| r.id), None);
    assert_eq!(ports.lookup("10.0.0.1:443").map(|r| r.id), Some(2), "a portless rule covers every port");
    let dup =
        RuleSet { next_id: 3, rules: vec![mk(1, "a.example", RuleEffect::Ban), mk(2, "a.example", RuleEffect::Allow)] };
    assert!(rules::validate(&dup).is_err());
}

#[test]
fn hostnames_parse_like_indigo() {
    let ok = |s: &str| parse_hostname(s).map(|p| (p.hostname, p.insecure));
    assert_eq!(ok("https://PDS.Example.com/xrpc?x=1"), Ok(("pds.example.com".into(), false)));
    assert_eq!(ok("wss://pds.example.com"), Ok(("pds.example.com".into(), false)));
    assert_eq!(ok("pds.example.com."), Ok(("pds.example.com".into(), false)));
    assert_eq!(ok("http://pds.example.com"), Ok(("pds.example.com".into(), true)));
    assert_eq!(ok("http://localhost:2583"), Ok(("localhost:2583".into(), true)));
    assert!(parse_hostname("pds.example.com:8443").is_err());
    assert!(parse_hostname("ftp://pds.example.com").is_err());
    assert!(parse_hostname("10.0.0.1").is_err());
    assert!(parse_hostname("single").is_err());
}

#[tokio::test]
async fn for_host_and_admission_apply_rules_and_budgets() {
    let store = Store::memory(None);
    let e = engine(&store, "a");
    let mut body = p();
    body.cluster.new_hosts_per_day = 2;
    e.save_policy(0, body, "t", "").await.unwrap();
    let admin = PolicyAdmin::new(e.clone(), Arc::new(MemHosts::default()));
    for (pattern, effect) in [
        ("*.spam.example", crate::admin::RuleEffect::Ban),
        ("good.spam.example", crate::admin::RuleEffect::Allow),
        ("*.friends.example", crate::admin::RuleEffect::Tier { tier: "trusted".into() }),
        ("*.slow.example", crate::admin::RuleEffect::Throttle { events_per_sec: 2.0 }),
    ] {
        admin
            .create_domain_rule(
                crate::admin::DomainRuleInput { pattern: pattern.into(), effect, note: String::new() },
                "t",
            )
            .await
            .unwrap();
    }

    let l = e.for_host(&rec("pds.spam.example", Tier::Default, 0));
    assert_eq!((l.tier, l.connect, l.rule), (Tier::Banned, false, Some(1)));
    let l = e.for_host(&rec("a.friends.example", Tier::New, 0));
    assert_eq!(l.tier, Tier::Trusted);
    assert_eq!(l.limits.unwrap().events_per_sec, 5_000.0);
    // auto-throttled beats a forced tier
    assert_eq!(e.for_host(&rec("a.friends.example", Tier::Throttled, 0)).tier, Tier::Throttled);
    let l = e.for_host(&rec("x.slow.example", Tier::Default, 0));
    assert_eq!(l.limits.unwrap().events_per_sec, 2.0);
    let mut r = rec("plain.example", Tier::Default, 0);
    assert_eq!(e.for_host(&r).limits.unwrap().events_per_sec, 51.0);
    tiers::apply_manual(&mut r, &Manual::Throttle(Some(1.5)), 0).unwrap();
    assert_eq!(e.for_host(&r).limits.unwrap().events_per_sec, 1.5);
    // an operator's account cap replaces the tier's (indigo's per-host repo_limit)
    assert_eq!(e.for_host(&r).limits.unwrap().max_accounts, 1000);
    tiers::apply_manual(&mut r, &Manual::AccountLimit(Some(50_000)), 0).unwrap();
    let l = e.for_host(&r).limits.unwrap();
    assert_eq!((l.max_accounts, l.events_per_sec), (50_000, 1.5));
    tiers::apply_manual(&mut r, &Manual::AccountLimit(None), 0).unwrap();
    assert_eq!(e.for_host(&r).limits.unwrap().max_accounts, 1000);
    assert!(!e.for_host(&rec("s.example", Tier::Suspended, 0)).connect);

    let admit = |h: &'static str, by_admin| {
        let e = e.clone();
        async move { e.admit_host(&AdmitRequest { hostname: h, by_admin, existing: None, dry_run: false }).await }
    };
    assert!(matches!(admit("https://x.spam.example", false).await, Admit::Reject(RejectHost::Banned { .. })));
    assert!(matches!(admit("pds.example.com:99", false).await, Admit::Reject(RejectHost::BadHostname(_))));
    assert!(matches!(admit("http://pds.example.com", false).await, Admit::Reject(RejectHost::BadHostname(_))));
    assert!(matches!(admit("localhost", false).await, Admit::Reject(RejectHost::Localhost)));
    // trusted domain: starts trusted and, like allow rules, skips the daily budget
    assert_eq!(
        admit("morel.us-east.host.bsky.network", false).await,
        Admit::Admit { host: Host("morel.us-east.host.bsky.network".into()), tier: Tier::Trusted, counted: false }
    );
    // allow rules and admins skip the daily budget
    assert!(matches!(admit("good.spam.example", false).await, Admit::Admit { counted: false, .. }));
    assert!(matches!(admit("one.example", false).await, Admit::Admit { tier: Tier::New, counted: true, .. }));
    assert!(matches!(admit("two.example", false).await, Admit::Admit { counted: true, .. }));
    assert_eq!(admit("three.example", false).await, Admit::Reject(RejectHost::DailyLimit { limit: 2 }));
    assert!(matches!(admit("three.example", true).await, Admit::Admit { counted: false, .. }));
    // a peer sees the same count
    assert_eq!(engine(&store, "b").new_hosts_today().await.unwrap(), 2);
    // known hosts aren't new, banned ones stay out
    let known = rec("three.example", Tier::Default, 0);
    let r = e
        .admit_host(&AdmitRequest {
            hostname: "three.example",
            by_admin: false,
            existing: Some(&known),
            dry_run: false,
        })
        .await;
    assert!(matches!(r, Admit::Admit { counted: false, tier: Tier::Default, .. }));
    let banned = rec("three.example", Tier::Banned, 0);
    let r = e
        .admit_host(&AdmitRequest {
            hostname: "three.example",
            by_admin: false,
            existing: Some(&banned),
            dry_run: false,
        })
        .await;
    assert!(matches!(r, Admit::Reject(RejectHost::Banned { .. })));

    // allow-list only
    let mut body = e.policy().body;
    body.crawl.allowlist_only = true;
    e.save_policy(1, body, "t", "").await.unwrap();
    assert_eq!(admit("four.example", false).await, Admit::Reject(RejectHost::NotAllowed));
    assert!(matches!(admit("good.spam.example", false).await, Admit::Admit { .. }));
}

// ---------------------------------------------------------------- budgets

#[test]
fn budget_shares_follow_live_nodes() {
    let store = Store::memory(None);
    let live = Arc::new(FixedNodes::new(3));
    let e = Engine::new(store, "a", live.clone());
    let plc = || e.budget(BudgetKind::PlcLookupsPerSec);
    assert!((plc() - 500.0 / 3.0).abs() < 1e-9);
    assert_eq!(e.budget(BudgetKind::NewAccountsPerMin), 2_000.0);
    live.set(2);
    assert_eq!(plc(), 250.0);
    live.set(5);
    assert_eq!(plc(), 100.0);
    // no lease seen yet: the whole budget, not nothing
    live.set(0);
    assert_eq!(plc(), 500.0);
    assert_eq!(e.budget(BudgetKind::NewHostsPerDay), 50.0);

    let b = budget::Bucket::default();
    // 10/s: a full second's burst, then 1 per 100 ms
    assert!((0..10).all(|_| b.try_take(10.0, 1.0, 1_000)));
    assert!(!b.try_take(10.0, 1.0, 1_000));
    assert!(b.try_take(10.0, 1.0, 1_100));
    assert!(!b.try_take(10.0, 1.0, 1_100));
    // the share shrinks when a node joins: the refill slows at once
    assert!(!b.try_take(5.0, 1.0, 1_250));
    assert!(b.try_take(5.0, 1.0, 1_300));
}

// ---------------------------------------------------------------- hot reload

#[tokio::test]
async fn peers_hot_reload_policy_and_rules() {
    let store = Store::memory(None);
    let (a, b) = (engine(&store, "a"), engine(&store, "b"));
    assert!(!b.refresh().await.unwrap(), "nothing stored: defaults");
    let mut body = p();
    body.tiers.new.max_accounts = 7;
    a.save_policy(0, body, "t", "").await.unwrap();
    let r = rec("x.example", Tier::New, 0);
    assert_eq!(a.for_host(&r).limits.unwrap().max_accounts, 7);
    assert_eq!(b.for_host(&r).limits.unwrap().max_accounts, 1000);
    assert!(b.refresh().await.unwrap());
    assert_eq!(b.for_host(&r).limits.unwrap().max_accounts, 7);
    assert!(!b.refresh().await.unwrap(), "304 until the next change");

    let set = RuleSet {
        next_id: 2,
        rules: vec![Rule {
            id: 1,
            pattern: "x.example".into(),
            effect: RuleEffect::Ban,
            note: String::new(),
            created_at_ms: 0,
            created_by: "t".into(),
        }],
    };
    a.save_rules(0, set, "t", "").await.unwrap();
    assert!(b.refresh().await.unwrap());
    assert_eq!(b.for_host(&r).tier, Tier::Banned);

    // a hand-written bad object: b keeps the last good policy and says why
    store
        .raw
        .put(
            &store::path(&store, POLICY_PATH),
            PutPayload::from(
                r#"{"version":9,"updatedAtMs":0,"updatedBy":"x","body":{"tiers":{"new":{"eventsPerSec":-1}}}}"#,
            ),
        )
        .await
        .unwrap();
    assert!(!b.refresh().await.unwrap());
    assert_eq!(b.policy().version, 1);
    assert!(b.last_error.lock().as_deref().unwrap().contains("eventsPerSec"));
    // and the API can still replace it (version 9 seen)
    let d = a.save_policy(9, p(), "t", "fix").await.unwrap();
    assert_eq!(d.version, 10);
    assert!(b.refresh().await.unwrap());
    assert!(b.last_error.lock().is_none());
    assert_eq!(b.for_host(&r).tier, Tier::Banned, "rules untouched by a policy load");
}

// ---------------------------------------------------------------- signals

#[test]
fn topk_stays_bounded_with_10k_hosts() {
    let spam = super::doc::Spam::default();
    assert_eq!(spam.track_hosts, 1_024);
    let s = Signals::new(&spam);
    let t0 = 1_000 * 3_600 * 1000;
    let mut trips = Vec::new();
    let heavy: Vec<String> = (0..5).map(|i| format!("farm{i}.spam.example")).collect();
    // 10k hosts with a few new accounts each, 5 farms with 400 each
    for round in 0..400u32 {
        for i in 0..10_000u32 {
            if round < 3 && (i + round) % 3 == 0 {
                let h = format!("pds{i}.example.com");
                trips.extend(s.record(&Signal::new(SignalKind::NewAccount, &h, None), t0 + round as i64));
            }
        }
        for h in &heavy {
            trips.extend(s.record(&Signal::new(SignalKind::NewAccount, h, None), t0 + round as i64));
        }
    }
    let tracked = s.tracked(SpamRule::HostNewAccounts);
    assert!(tracked <= 1_024, "{tracked}");
    let bytes = s.heap_bytes();
    // 7 tables, 1,024 hosts and 8,192 DIDs at most: ~2 MiB whatever the load
    assert!(bytes < 4 << 20, "{bytes}");
    let mut tripped: Vec<_> = trips.iter().map(|t| t.host.clone()).collect();
    tripped.sort();
    assert_eq!(tripped, heavy, "each farm trips once, no small host does");
    assert!(trips.iter().all(|t| t.observed >= 300.0 && t.rule == SpamRule::HostNewAccounts));
    let top = s.top(SpamRule::HostNewAccounts, 5, t0 + 400);
    assert!(top.iter().all(|(k, ..)| heavy.contains(k)), "{top:?}");

    // a raw table: never more than capacity, lower bound never above truth
    let mut t = TopK::new(64, 60);
    let mut truth = std::collections::HashMap::new();
    for i in 0..10_000u64 {
        let k = format!("k{}", if i % 4 == 0 { i % 8 } else { i });
        *truth.entry(k.clone()).or_insert(0u64) += 1;
        let h = {
            use std::hash::{Hash, Hasher};
            let mut s = std::collections::hash_map::DefaultHasher::new();
            k.hash(&mut s);
            s.finish()
        };
        let a = t.add(&k, h, "h", 1.0, None, 0.0, 30);
        assert!(a.lower <= truth[&k] as f64);
        assert!(t.len() <= 64);
    }
    assert!(t.heap_bytes() < 64 * 1024);
    // the heavy keys (k0 and k4, 1,250 each) are found
    let top: Vec<String> = t.top(2, 30).into_iter().map(|x| x.0).collect();
    assert_eq!(top, vec!["k0".to_string(), "k4".to_string()]);
}

#[test]
fn signals_window_slides() {
    let mut spam = super::doc::Spam::default();
    spam.account_records.limit = 100.0; // per 60 s
    let s = Signals::new(&spam);
    let t0: i64 = 6_000_000 * 60 * 1000;
    let did = Some("did:plc:flood");
    let rec = |n: u32, at: i64| {
        let mut sig = Signal::new(SignalKind::Record, "pds.example", did);
        sig.count = n;
        s.record(&sig, at)
    };
    assert!(rec(90, t0).is_empty());
    // 30 s into the next window, half the previous one still counts: 45 + 50
    assert!(rec(50, t0 + 90_000).is_empty());
    let trips = rec(10, t0 + 90_000);
    assert_eq!(trips.len(), 1);
    assert_eq!(trips[0].did.as_deref(), Some("did:plc:flood"));
    assert_eq!(trips[0].action, SpamAction::Case);
    // once per window
    assert!(rec(500, t0 + 100_000).is_empty());
    assert_eq!(rec(500, t0 + 125_000).len(), 1);
    // records without a DID don't count for per-account rules
    assert!(s.record(&Signal::new(SignalKind::Record, "pds.example", None), t0).is_empty());
}

#[test]
fn a_catching_up_hosts_events_weigh_what_their_own_time_did() {
    let mut spam = super::doc::Spam::default();
    spam.account_records.limit = 100.0; // per 60 s
    let s = Signals::new(&spam);
    let t0: i64 = 6_000_000 * 60 * 1000;
    let rec = |n: u32, weight: f64| {
        let mut sig = Signal::new(SignalKind::Record, "pds.example", Some("did:plc:busy"));
        sig.count = n;
        sig.weight = weight;
        s.record(&sig, t0)
    };
    // ten minutes of 80/min, replayed at 10× its pace inside one window
    assert!(rec(800, 0.1).is_empty());
    // and the same at arrival time is a burst
    assert_eq!(rec(800, 1.0).len(), 1);
}

// ---------------------------------------------------------------- cases

fn open(host: &str, observed: f64, at_ms: i64) -> CaseOpen {
    CaseOpen {
        kind: "new-accounts".into(),
        host: host.into(),
        did: None,
        severity: Severity::High,
        summary: format!("{observed}"),
        observed,
        threshold: 300.0,
        auto_action: None,
        evidence: Evidence {
            at_ms,
            observed,
            threshold: 300.0,
            window_secs: 3600,
            node: "a".into(),
            detail: None,
            signals: Default::default(),
        },
    }
}

#[tokio::test]
async fn cases_are_deduplicated_per_key() {
    let store = Store::memory(None);
    let (a, b) = (engine(&store, "a"), engine(&store, "b"));
    assert_eq!(a.cases.open_or_update(open("farm.example", 310.0, 1)).await.unwrap(), Opened::Created(1));
    assert_eq!(b.cases.open_or_update(open("farm.example", 900.0, 2)).await.unwrap(), Opened::Updated(1));
    assert_eq!(a.cases.open_or_update(open("other.example", 400.0, 3)).await.unwrap(), Opened::Created(2));
    let c = a.cases.get_case(1).await.unwrap().unwrap();
    assert_eq!((c.trips, c.evidence.len(), c.observed), (2, 2, 900.0));
    assert_eq!(a.cases.list(Some(CaseStatus::Open)).await.unwrap().len(), 2);

    // acknowledged still dedupes; resolved frees the key
    a.cases.update(1, Some(CaseStatus::Acknowledged), "looking", "op").await.unwrap();
    assert_eq!(a.cases.open_or_update(open("farm.example", 320.0, 4)).await.unwrap(), Opened::Updated(1));
    let c = a.cases.update(1, Some(CaseStatus::Resolved), "banned the domain", "op").await.unwrap().unwrap();
    assert_eq!(c.notes.len(), 2);
    assert_eq!(b.cases.open_or_update(open("farm.example", 330.0, 5)).await.unwrap(), Opened::Created(3));

    // the evidence list is capped
    for i in 0..30 {
        a.cases.open_or_update(open("farm.example", 300.0 + i as f64, 10 + i)).await.unwrap();
    }
    let c = a.cases.get_case(3).await.unwrap().unwrap();
    assert_eq!((c.trips, c.evidence.len()), (31, cases::EVIDENCE_KEPT));
    assert_eq!(c.evidence.last().unwrap().observed, 329.0);

    // racing first trips from several nodes land on one case
    let mut tasks = Vec::new();
    for i in 0..6 {
        let e = engine(&store, &format!("n{i}"));
        tasks.push(tokio::spawn(async move { e.cases.open_or_update(open("race.example", 301.0, 100)).await }));
    }
    let mut created = 0;
    for t in tasks {
        if let Opened::Created(_) = t.await.unwrap().unwrap() {
            created += 1;
        }
    }
    assert_eq!(created, 1);
    let race: Vec<_> = a.cases.list(None).await.unwrap().into_iter().filter(|c| c.host == "race.example").collect();
    assert_eq!(race.len(), 1);
    assert_eq!(race[0].trips, 6);
}

// ---------------------------------------------------------------- driver + admin

#[tokio::test]
async fn driver_throttles_opens_cases_and_sweeps() {
    let store = Store::memory(None);
    let e = engine(&store, "a");
    let hosts = Arc::new(MemHosts::default());
    let now = crate::state::now_secs();
    hosts.put_host(&rec("farm.example", Tier::New, now - 3_600)).await.unwrap();
    hosts.put_host(&rec("buggy.example", Tier::Default, now - 30 * DAY)).await.unwrap();
    hosts.put_host(&rec("old.example", Tier::New, now - 30 * DAY)).await.unwrap();
    let d = Driver::new(e.clone(), hosts.clone());

    let at = store::now_ms();
    for _ in 0..300 {
        e.record_signal_at(Signal::new(SignalKind::NewAccount, "farm.example", None), at);
    }
    let r = d.process_trips().await.unwrap();
    assert_eq!(r.moved.len(), 1);
    assert_eq!(r.cases, vec![Opened::Created(1)]);
    let farm = hosts.get_host("farm.example").await.unwrap().unwrap();
    assert_eq!(farm.tier, Tier::Throttled);
    assert_eq!(e.for_host(&farm).limits.unwrap().events_per_sec, 5.0);
    let c = e.cases.get_case(1).await.unwrap().unwrap();
    assert_eq!(c.auto_action.as_deref(), Some("throttled"));
    assert_eq!(c.evidence[0].signals["new-accounts"], 300.0);

    // first sweep: baselines, and the old new host is promoted
    let r = d.sweep_at(now).await.unwrap();
    assert_eq!(r.scanned, 3);
    assert_eq!(
        r.moved.iter().map(|m| (m.host.as_str(), m.to)).collect::<Vec<_>>(),
        vec![("old.example", Tier::Default)]
    );
    // a bad interval on buggy.example
    hosts
        .add_counts(&[("buggy.example".into(), HostCounts { events: 100, failed_checks: 400, ..Default::default() })])
        .await
        .unwrap();
    let r = d.sweep_at(now + 30).await.unwrap();
    assert_eq!(r.moved.len(), 1);
    assert_eq!(r.moved[0].to, Tier::Throttled);
    // quiet for the recovery period: both throttled hosts come back
    let r = d.sweep_at(now + 30 + 3_600).await.unwrap();
    let mut back: Vec<_> = r.moved.iter().map(|m| (m.host.clone(), m.to)).collect();
    back.sort_by(|a, b| a.0.cmp(&b.0));
    assert_eq!(back, vec![("buggy.example".into(), Tier::Default), ("farm.example".into(), Tier::New)]);
}

/// A bucket whose writes fail on the way (a reset connection, a timeout).
#[derive(Debug)]
struct WritesFail(Arc<dyn object_store::ObjectStore>);

impl std::fmt::Display for WritesFail {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "WritesFail")
    }
}

#[async_trait::async_trait]
impl object_store::ObjectStore for WritesFail {
    async fn put_opts(
        &self,
        _: &object_store::path::Path,
        _: PutPayload,
        _: object_store::PutOptions,
    ) -> object_store::Result<object_store::PutResult> {
        Err(object_store::Error::Generic { store: "S3", source: "error sending request: connection reset".into() })
    }
    async fn put_multipart_opts(
        &self,
        l: &object_store::path::Path,
        o: object_store::PutMultipartOptions,
    ) -> object_store::Result<Box<dyn object_store::MultipartUpload>> {
        self.0.put_multipart_opts(l, o).await
    }
    async fn get_opts(
        &self,
        l: &object_store::path::Path,
        o: object_store::GetOptions,
    ) -> object_store::Result<object_store::GetResult> {
        self.0.get_opts(l, o).await
    }
    fn delete_stream(
        &self,
        l: futures::stream::BoxStream<'static, object_store::Result<object_store::path::Path>>,
    ) -> futures::stream::BoxStream<'static, object_store::Result<object_store::path::Path>> {
        self.0.delete_stream(l)
    }
    fn list(
        &self,
        p: Option<&object_store::path::Path>,
    ) -> futures::stream::BoxStream<'static, object_store::Result<object_store::ObjectMeta>> {
        self.0.list(p)
    }
    async fn list_with_delimiter(
        &self,
        p: Option<&object_store::path::Path>,
    ) -> object_store::Result<object_store::ListResult> {
        self.0.list_with_delimiter(p).await
    }
    async fn copy_opts(
        &self,
        f: &object_store::path::Path,
        t: &object_store::path::Path,
        o: object_store::CopyOptions,
    ) -> object_store::Result<()> {
        self.0.copy_opts(f, t, o).await
    }
}

/// A save the bucket failed is a 503 with a Retry-After, which says the
/// same save may work again, not a 500; a bucket that refuses the keys
/// stays a 500.
#[tokio::test]
async fn a_policy_save_the_bucket_failed_is_unavailable() {
    use axum::response::IntoResponse;
    let raw: Arc<dyn object_store::ObjectStore> = Arc::new(WritesFail(Arc::new(object_store::memory::InMemory::new())));
    let store = Store { raw, prefix: "vlrelay".into(), latency: None };
    let a = PolicyAdmin::new(engine(&store, "a"), Arc::new(MemHosts::default()));
    let mut body = p();
    body.cluster.new_hosts_per_day = 10;
    let err = a.update_full_policy(0, body, "op", "").await.unwrap_err();
    assert!(matches!(err, crate::admin::AdminError::Unavailable(_)), "{err:?}");
    let r = err.into_response();
    assert_eq!(r.status(), axum::http::StatusCode::SERVICE_UNAVAILABLE);
    assert_eq!(r.headers()[axum::http::header::RETRY_AFTER], "5");

    let denied = object_store::Error::PermissionDenied { path: "p".into(), source: "403".into() };
    assert!(matches!(store::SaveError::from_store(denied), store::SaveError::Store(_)));
    let timed_out = object_store::Error::Generic { store: "policy", source: "object store call timed out".into() };
    assert!(matches!(store::SaveError::from_store(timed_out), store::SaveError::Unavailable(_)));
}

#[tokio::test]
async fn policy_admin_maps_the_wire_types() {
    let store = Store::memory(None);
    let e = engine(&store, "a");
    let hosts = Arc::new(MemHosts::default());
    hosts.put_host(&rec("pds.example.com", Tier::Default, 0)).await.unwrap();
    let a = PolicyAdmin::new(e.clone(), hosts.clone());

    // a wire edit changes its fields and keeps the rest
    let mut full = p();
    full.consumers.connections_per_ip = 3;
    e.save_policy(0, full, "t", "").await.unwrap();
    let mut doc = a.policy().await.unwrap();
    assert_eq!(doc.policy.tiers.len(), 4);
    assert_eq!(doc.policy.default_tier, "new");
    assert_eq!(doc.policy.spam.new_accounts_per_hour, 300);
    assert!(doc.policy.spam.auto_throttle);
    crate::admin::validate_policy(&doc.policy).unwrap();
    doc.policy.tiers.get_mut("default").unwrap().events_per_sec = 75.0;
    doc.policy.spam.auto_throttle = false;
    doc.policy.spam.bad_signatures_per_min = 30;
    let u = crate::admin::PolicyUpdate { base_version: 1, policy: doc.policy.clone(), note: "x".into() };
    let d = a.update_policy(u.clone(), "op").await.unwrap();
    assert_eq!(d.version, 2);
    let full = e.policy().body;
    assert_eq!(full.tiers.default.events_per_sec, 75.0);
    assert_eq!(full.consumers.connections_per_ip, 3);
    assert_eq!(full.spam.host_new_accounts.action, SpamAction::Case);
    assert_eq!(full.spam.host_failed_validation.limit, 30.0);
    assert_eq!(to_wire(&merge_wire(&full, &doc.policy).unwrap()), doc.policy);
    // stale base
    assert!(matches!(a.update_policy(u, "op").await, Err(crate::admin::AdminError::Conflict(_))));
    let audit = a.policy_audit().await.unwrap();
    assert!(
        audit[0].changes.contains(&"tiers.default.eventsPerSec: 51.0 → 75.0".to_string()),
        "{:?}",
        audit[0].changes
    );

    // host actions go through the HostStore and are recorded on the host
    let r = a.host_action("pds.example.com", HostAction::Ban { reason: "spam".into() }, "op").await.unwrap();
    assert_eq!(r.tier, Tier::Banned);
    let r = a.host_action("pds.example.com", HostAction::Unban, "op").await.unwrap();
    assert_eq!(r.tier, Tier::Default);
    a.host_action("pds.example.com", HostAction::SetTier { tier: "trusted".into() }, "op").await.unwrap();
    let r = hosts.get_host("pds.example.com").await.unwrap().unwrap();
    assert_eq!(r.tier, Tier::Trusted);
    assert_eq!(PolicyAdmin::host_actions(&r).len(), 3);
    assert!(a.host_action("pds.example.com", HostAction::Reconnect, "op").await.is_err());
    assert!(a.host_action("nope.example", HostAction::Unban, "op").await.is_err());

    // rules: CRUD with match counts
    let rule = a
        .create_domain_rule(
            crate::admin::DomainRuleInput {
                pattern: "*.example.com".into(),
                effect: crate::admin::RuleEffect::Allow,
                note: String::new(),
            },
            "op",
        )
        .await
        .unwrap();
    assert_eq!((rule.id, rule.matches), (1, 1));
    assert!(matches!(
        a.create_domain_rule(
            crate::admin::DomainRuleInput {
                pattern: "*.EXAMPLE.com".into(),
                effect: crate::admin::RuleEffect::Ban,
                note: String::new()
            },
            "op"
        )
        .await,
        Err(crate::admin::AdminError::Conflict(_))
    ));
    let rule = a
        .update_domain_rule(
            1,
            crate::admin::DomainRuleInput {
                pattern: "*.example.org".into(),
                effect: crate::admin::RuleEffect::Ban,
                note: "n".into(),
            },
            "op",
        )
        .await
        .unwrap();
    assert_eq!(rule.matches, 0);
    assert_eq!(a.domain_rules().await.unwrap().len(), 1);
    a.delete_domain_rule(1, "op").await.unwrap();
    assert!(a.domain_rules().await.unwrap().is_empty());
    assert!(matches!(a.delete_domain_rule(1, "op").await, Err(crate::admin::AdminError::NotFound(_))));

    // a tier a domain rule decides is refused, not recorded and ignored
    let rule_input =
        |effect| crate::admin::DomainRuleInput { pattern: "pds.example.com".into(), effect, note: String::new() };
    let trusted = crate::admin::RuleEffect::Tier { tier: "trusted".into() };
    let rule = a.create_domain_rule(rule_input(trusted), "op").await.unwrap();
    let err = a.host_action("pds.example.com", HostAction::SetTier { tier: "new".into() }, "op").await.unwrap_err();
    assert!(
        matches!(&err, crate::admin::AdminError::TierSetByRule(m) if m.contains(&format!("rule {} (pds.example.com)", rule.id))),
        "{err}"
    );
    let r = hosts.get_host("pds.example.com").await.unwrap().unwrap();
    assert_eq!((r.tier, PolicyAdmin::host_actions(&r).len()), (Tier::Trusted, 3));
    // the rule's own tier and throttled take effect, so they land
    a.host_action("pds.example.com", HostAction::SetTier { tier: "trusted".into() }, "op").await.unwrap();
    a.host_action("pds.example.com", HostAction::SetTier { tier: "throttled".into() }, "op").await.unwrap();
    assert_eq!(e.for_host(&hosts.get_host("pds.example.com").await.unwrap().unwrap()).tier, Tier::Throttled);
    a.update_domain_rule(rule.id, rule_input(crate::admin::RuleEffect::Ban), "op").await.unwrap();
    assert!(matches!(
        a.host_action("pds.example.com", HostAction::SetTier { tier: "throttled".into() }, "op").await,
        Err(crate::admin::AdminError::TierSetByRule(_))
    ));
    a.delete_domain_rule(rule.id, "op").await.unwrap();
    a.host_action("pds.example.com", HostAction::SetTier { tier: "trusted".into() }, "op").await.unwrap();

    // cases through the wire types
    e.cases.open_or_update(open("farm.example", 400.0, 1)).await.unwrap();
    assert_eq!(a.cases(Default::default()).await.unwrap().len(), 1);
    let c = a
        .update_case(1, crate::admin::CaseUpdate { status: Some(CaseStatus::Dismissed), note: "fine".into() }, "op")
        .await
        .unwrap();
    assert_eq!(c.status, CaseStatus::Dismissed);
    assert!(
        a.cases(crate::admin::CaseQuery { status: Some(CaseStatus::Open), ..Default::default() })
            .await
            .unwrap()
            .is_empty()
    );
    assert_eq!(a.case_detail(1).await.unwrap().evidence.len(), 1);
}

/// `GET policy` (the wire view) and `GET policy/full` (the document) name
/// the same tiers.
#[test]
fn the_wire_policy_and_the_document_name_the_same_tiers() {
    let body = PolicyBody::default();
    let wire: std::collections::BTreeSet<String> = crate::policy::admin::to_wire(&body).tiers.keys().cloned().collect();
    let doc: std::collections::BTreeSet<String> =
        serde_json::to_value(&body).unwrap()["tiers"].as_object().unwrap().keys().cloned().collect();
    assert_eq!(wire, doc);
    assert_eq!(wire, ["default", "new", "throttled", "trusted"].iter().map(|s| s.to_string()).collect());
}
