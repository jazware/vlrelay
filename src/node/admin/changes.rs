//! The admin change feed on a relay node (docs/admin-api.md, "Change
//! feed"). Committed takedowns and throttle lifts come from the quorum
//! log's hooks (`node::quorum::logged_change`); everything else is watched
//! here, once a second while the feed is wanted: each host's row as this
//! node reads it, its consumers, the policy and rule versions, the quorum's
//! leader and members, and (on the leader) discovery and the PLC export.
//! A node with a feed open asks every other member for what it saw.

use super::NodeAdmin;
use crate::admin::changes::{ChangeFeed, ChangeKind, PULL_EVERY, PullAnswer, PullRequest};
use crate::admin::{self, BackpressureReason, HostStatus};
use bytes::Bytes;
use std::collections::{BTreeMap, BTreeSet, HashMap};
use std::sync::Arc;

/// Cases are a bucket listing, so they're compared this often.
const CASES_EVERY: u64 = 10;

/// What a host's row shows that an operator would want to hear changed
/// (its rates and lag move every second, so they don't count).
#[derive(Clone, Debug, PartialEq)]
pub(super) struct HostSig {
    status: HostStatus,
    reason: Option<BackpressureReason>,
    tier: String,
    throttle: Option<u64>,
    rule: Option<u64>,
    max_accounts: u64,
    source: Option<String>,
    throttled_accounts: u64,
    node: String,
}

impl HostSig {
    pub(super) fn of(r: &admin::HostRow) -> HostSig {
        HostSig {
            status: r.status,
            reason: r.backpressure_reason,
            tier: r.tier.clone(),
            throttle: r.throttle.map(f64::to_bits),
            rule: r.rule,
            max_accounts: r.max_accounts,
            source: r.source.clone(),
            throttled_accounts: r.throttled_accounts,
            node: r.node.clone(),
        }
    }
}

/// A host's last change as this node saw it.
pub(super) struct Seen {
    sig: HostSig,
    version: Option<String>,
    at_ms: Option<i64>,
}

/// What the watch compared last. None: not looked at yet, so the first
/// look is a baseline and announces nothing.
#[derive(Default)]
pub(super) struct Watch {
    hosts: Option<HashMap<String, Seen>>,
    consumers: Option<BTreeSet<u64>>,
    versions: Option<(u64, u64)>,
    cluster: Option<String>,
    discovery: Option<BTreeMap<String, String>>,
    plc: Option<String>,
    cases: Option<BTreeMap<u64, i64>>,
    ticks: u64,
}

fn hint(r: &admin::HostRow) -> serde_json::Value {
    serde_json::json!({ "status": r.status, "backpressureReason": r.backpressure_reason, "tier": r.tier })
}

impl NodeAdmin {
    pub fn feed(&self) -> &Arc<ChangeFeed> {
        &self.feed
    }

    /// The watch, the pulls from members and the coalescing flush, for as
    /// long as this admin source lives; and the log's takedowns into the feed.
    pub fn start_changes(self: &Arc<Self>) {
        let _ = self.node.quorum.hooks.changes.set(self.feed.clone());
        self.feed.spawn_flusher();
        let weak = Arc::downgrade(self);
        tokio::spawn(async move {
            let mut tick = tokio::time::interval(PULL_EVERY);
            tick.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
            loop {
                tick.tick().await;
                let Some(a) = weak.upgrade() else { return };
                if a.feed.wanted() {
                    a.watch_once().await;
                }
                if a.feed.open_feeds() > 0 {
                    a.pull_members().await;
                }
            }
        });
    }

    /// The versions the rows carry: the last change this node saw to each.
    pub(super) fn stamp(&self, rows: &mut [admin::HostRow]) {
        let w = self.watch.lock();
        let Some(seen) = &w.hosts else { return };
        for r in rows {
            if let Some(s) = seen.get(&r.host) {
                r.version = s.version.clone();
                r.updated_at_ms = s.at_ms;
            }
        }
    }

    /// An operator's action on `row`: its event now, not at the next look.
    pub(super) fn host_acted(&self, row: &mut admin::HostRow) {
        // forwarded wherever the host is read: an operator's action is news
        // to every console, and a throttle is held on this node only
        let version = self.feed.emit(ChangeKind::Host, row.host.clone(), Some(hint(row)), true);
        let at = crate::admin::changes::now_ms();
        if let Some(seen) = &mut self.watch.lock().hosts {
            seen.insert(
                row.host.clone(),
                Seen { sig: HostSig::of(row), version: Some(version.clone()), at_ms: Some(at) },
            );
        }
        row.version = Some(version);
        row.updated_at_ms = Some(at);
    }

    pub(super) fn consumer_left(&self, id: u64) {
        if let Some(c) = &mut self.watch.lock().consumers {
            c.remove(&id);
        }
        let hint = serde_json::json!({ "event": "disconnect" });
        self.feed.emit(ChangeKind::Consumer, format!("{}/{id}", self.id()), Some(hint), true);
    }

    pub(super) fn policy_saved(&self, kind: ChangeKind, version: u64) {
        let id = if kind == ChangeKind::Policy { "policy" } else { "rules" };
        self.feed.publish_versioned(kind, id, version.to_string(), None, true);
    }

    pub(super) fn case_changed(&self, c: &admin::Case) {
        let hint = serde_json::json!({ "status": c.status });
        self.feed.emit(ChangeKind::Case, c.id.to_string(), Some(hint), true);
    }

    /// One look at everything watched.
    pub(super) async fn watch_once(&self) {
        self.watch_hosts();
        self.watch_consumers();
        self.watch_versions();
        self.watch_cluster();
        self.watch_leader_jobs().await;
        let tick = {
            let mut w = self.watch.lock();
            w.ticks += 1;
            w.ticks
        };
        // a serving node lists the cases itself; a member only pulled for
        // its own observations needn't spend a listing
        if self.feed.open_feeds() > 0 && (tick % CASES_EVERY == 1) {
            self.watch_cases().await;
        }
    }

    fn watch_hosts(&self) {
        let mut rows = self.rows();
        let owners = self.node.quorum.hosts.owners();
        for r in &mut rows {
            if let Some(o) = owners.get(&r.host) {
                r.node = o.clone();
            }
        }
        let at = crate::admin::changes::now_ms();
        let mut w = self.watch.lock();
        let first = w.hosts.is_none();
        let seen = w.hosts.get_or_insert_with(HashMap::new);
        for r in &rows {
            let sig = HostSig::of(r);
            match seen.get_mut(&r.host) {
                Some(s) if s.sig == sig => {}
                _ if first => {
                    seen.insert(r.host.clone(), Seen { sig, version: None, at_ms: None });
                }
                cur => {
                    let forward = self.node.manager.is_running(&crate::types::Host(r.host.clone()));
                    let version = self.feed.touch(ChangeKind::Host, r.host.clone(), Some(hint(r)), forward);
                    let s = Seen { sig, version: Some(version), at_ms: Some(at) };
                    match cur {
                        Some(c) => *c = s,
                        None => {
                            seen.insert(r.host.clone(), s);
                        }
                    }
                }
            }
        }
        if seen.len() > rows.len() {
            let live: std::collections::HashSet<&str> = rows.iter().map(|r| r.host.as_str()).collect();
            seen.retain(|h, _| live.contains(h.as_str()));
        }
    }

    fn watch_consumers(&self) {
        let now: BTreeSet<u64> = self.node.serve.consumers().iter().map(|c| c.id).collect();
        let mut w = self.watch.lock();
        if let Some(prev) = &w.consumers {
            for (ids, event) in [(now.difference(prev), "connect"), (prev.difference(&now), "disconnect")] {
                for id in ids {
                    let hint = serde_json::json!({ "event": event });
                    self.feed.touch(ChangeKind::Consumer, format!("{}/{id}", self.id()), Some(hint), true);
                }
            }
        }
        w.consumers = Some(now);
    }

    fn watch_versions(&self) {
        let s = self.policy.engine.snapshot();
        let now = (s.policy.version, s.rules_version);
        let prev = self.watch.lock().versions.replace(now);
        if let Some(p) = prev {
            if p.0 != now.0 {
                self.policy_saved(ChangeKind::Policy, now.0);
            }
            if p.1 != now.1 {
                self.policy_saved(ChangeKind::Rules, now.1);
            }
        }
    }

    fn watch_cluster(&self) {
        let st = self.node.quorum.qnode.status();
        let sig =
            serde_json::json!([st.epoch, st.leader, st.members, st.learners, st.generation, st.recoveries]).to_string();
        let prev = self.watch.lock().cluster.replace(sig.clone());
        if prev.is_some_and(|p| p != sig) {
            let hint = serde_json::json!({ "epoch": st.epoch, "leader": st.leader });
            self.feed.publish_versioned(ChangeKind::Cluster, "quorum", st.commit.to_string(), Some(hint), false);
        }
    }

    /// Discovery and the PLC export run on the leader, so only it watches them.
    async fn watch_leader_jobs(&self) {
        let st = self.node.quorum.qnode.status();
        if st.leader.as_deref() != Some(self.id()) {
            let mut w = self.watch.lock();
            w.discovery = None;
            w.plc = None;
            return;
        }
        if let Ok(v) = self.node.quorum.discovery(None).await {
            let now: BTreeMap<String, (String, bool)> = v
                .sources
                .iter()
                .map(|s| {
                    let sig = serde_json::json!([s.enabled, s.state]).to_string();
                    (s.key.clone(), (sig, s.state.in_progress))
                })
                .collect();
            let mut w = self.watch.lock();
            if let Some(prev) = &w.discovery {
                for (k, (sig, running)) in &now {
                    if prev.get(k) != Some(sig) {
                        let hint = serde_json::json!({ "inProgress": running });
                        self.feed.touch(ChangeKind::Discovery, k.clone(), Some(hint), true);
                    }
                }
            }
            w.discovery = Some(now.into_iter().map(|(k, (s, _))| (k, s)).collect());
        }
        if let Some(r) = self.node.quorum.plc_report().await {
            let sig = serde_json::json!([r.ops, r.caught_up, r.written, r.errors, r.restarts, r.throttled]).to_string();
            let prev = self.watch.lock().plc.replace(sig.clone());
            if prev.is_some_and(|p| p != sig) {
                let hint = serde_json::json!({ "caughtUp": r.caught_up });
                self.feed.touch(ChangeKind::Plc, "export", Some(hint), true);
            }
        }
    }

    async fn watch_cases(&self) {
        let Ok(cases) = self.policy.engine.cases.list(None).await else { return };
        let now: BTreeMap<u64, i64> = cases.iter().map(|c| (c.id, c.updated_at_ms)).collect();
        let prev = self.watch.lock().cases.replace(now.clone());
        let Some(prev) = prev else { return };
        for c in &cases {
            if prev.get(&c.id) != now.get(&c.id) {
                let hint = serde_json::json!({ "status": c.to_wire().status });
                self.feed.touch(ChangeKind::Case, c.id.to_string(), Some(hint), false);
            }
        }
    }

    /// Every other member's new events, into this node's feed.
    async fn pull_members(&self) {
        let st = self.node.quorum.qnode.status();
        let mut ids: Vec<String> = st.members.iter().chain(&st.learners).filter(|m| *m != self.id()).cloned().collect();
        ids.sort();
        ids.dedup();
        let pulls = ids.iter().map(|m| async move {
            // a member with a burst to hand over answers in pages
            for _ in 0..8 {
                let req = self.feed.pull_request(m);
                let body: Bytes = serde_json::to_vec(&req).unwrap_or_default().into();
                let Ok(b) = self.node.quorum.ask_member(m, "node:changes", body).await else { return };
                let Ok(a) = serde_json::from_slice::<PullAnswer>(&b) else { return };
                if !self.feed.absorb(m, a) {
                    return;
                }
            }
        });
        futures::future::join_all(pulls).await;
        let mut keep = ids;
        keep.push(self.id().to_string());
        self.feed.retain_members(&keep);
    }

    /// A member's side of [`Self::pull_members`].
    pub(super) fn answer_pull(&self, body: &[u8]) -> Option<serde_json::Value> {
        let req: PullRequest = serde_json::from_slice(body).ok()?;
        serde_json::to_value(self.feed.pull(&req)).ok()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::admin::AdminSource;
    use crate::admin::changes::Change;
    use crate::node::quorum::QuorumSetup;
    use crate::node::{Node, NodeConfig};
    use crate::types::Host;
    use std::time::{Duration, Instant};

    async fn relay(id: &str, addrs: &HashMap<String, String>, store: vlpds::store::Store) -> Arc<NodeAdmin> {
        let mut cfg = NodeConfig::new("http://127.0.0.1:9");
        cfg.node_id = id.into();
        cfg.dev_mode = true;
        cfg.lanes = 4;
        cfg.ingest_threads = 2;
        cfg.serve_threads = 1;
        let live: Arc<dyn crate::policy::LiveNodes> = Arc::new(crate::policy::FixedNodes::new(1));
        cfg.policy = Some(crate::node::policy::PolicyEngine(crate::policy::Engine::new(store.clone(), id, live)));
        let mut q = QuorumSetup::new(&addrs[id]);
        q.peers = addrs.iter().filter(|(k, _)| *k != id).map(|(k, v)| (k.clone(), v.clone())).collect();
        q.host_poll = Duration::from_millis(100);
        q.heartbeat = Duration::from_millis(50);
        q.election_timeout = Duration::from_millis(600);
        q.flush = Duration::from_millis(300);
        q.retain_horizon = None;
        let node = Node::start(store, cfg, q).await.unwrap();
        let a = Arc::new(NodeAdmin::new(node.clone(), node.policy.clone().unwrap()));
        let _ = node.quorum.hooks.answers.set(a.clone());
        a.start_changes();
        a
    }

    async fn until<T>(what: &str, mut f: impl FnMut() -> Option<T>) -> T {
        let t = Instant::now();
        loop {
            if let Some(x) = f() {
                return x;
            }
            assert!(t.elapsed() < Duration::from_secs(20), "waiting for {what}");
            tokio::time::sleep(Duration::from_millis(100)).await;
        }
    }

    fn find(a: &NodeAdmin, f: impl Fn(&Change) -> bool) -> Option<Change> {
        a.feed().recent().into_iter().find(|c| f(c))
    }

    /// Three relays on one log, each with a feed open: what an operator
    /// does on one node and what a member's own hosts do reach the feeds
    /// the others serve, with versions that say where they came from.
    #[tokio::test(flavor = "multi_thread", worker_threads = 8)]
    async fn changes_on_one_member_reach_every_members_feed() {
        let store = vlpds::store::Store::memory(None);
        let ids = ["n1", "n2", "n3"];
        let addrs: HashMap<String, String> =
            ids.iter().map(|i| (i.to_string(), format!("127.0.0.1:{}", crate::qlog::tests::free_port()))).collect();
        let mut admins = HashMap::new();
        for id in ids {
            admins.insert(id, relay(id, &addrs, store.clone()).await);
        }
        let subs: Vec<_> = admins.values().map(|a| a.feed().subscribe(None).unwrap()).collect();
        until("a leader", || admins["n1"].node.quorum.qnode.status().leader).await;
        // the members' first look is a baseline
        tokio::time::sleep(Duration::from_secs(3)).await;

        // a policy saved on n1 is on n2's feed with the document's version
        let p = admins["n1"].policy().await.unwrap();
        let mut pol = p.policy.clone();
        pol.spam.reject_ratio = (pol.spam.reject_ratio * 0.5).max(0.01);
        let u = admin::PolicyUpdate { base_version: p.version, policy: pol, note: String::new() };
        let saved = admins["n1"].update_policy(u, "admin (token)").await.unwrap();
        let c = until("the policy on n2", || {
            find(&admins["n2"], |c| c.kind == ChangeKind::Policy && c.version == saved.version.to_string())
        })
        .await;
        assert_eq!(c.node, "n1");

        // a host's status changes on the member that reads it
        let port = crate::qlog::tests::free_port();
        let host = format!("127.0.0.1:{port}");
        admins["n1"].node.manager.admit(&Host(host.clone()), crate::upstream::Tier::Trusted).await.unwrap();
        let owner = until("an owner", || admins["n1"].node.quorum.hosts.owners().get(&host).cloned()).await;
        let other = *ids.iter().find(|i| **i != owner).unwrap();
        let c = until("the owner's host event elsewhere", || {
            find(&admins[other], |c| c.kind == ChangeKind::Host && c.id == host && c.node == owner)
        })
        .await;
        assert!(c.version.starts_with(&format!("{owner}:")), "{c:?}");

        // an operator's tier change on one node is on another's feed, and
        // the row it answered carries the event's version
        let acting = admins["n1"].clone();
        let row = acting
            .host_action(&host, admin::HostAction::SetTier { tier: "default".into() }, "admin (token)")
            .await
            .unwrap();
        let v = row.version.clone().expect("a version");
        let third = "n3";
        until("the action elsewhere", || {
            find(&admins[third], |c| c.kind == ChangeKind::Host && c.id == host && c.version == v)
        })
        .await;
        let listed = acting.hosts(admin::HostQuery::default()).await.unwrap();
        let r = listed.hosts.iter().find(|r| r.host == host).unwrap();
        assert!(r.version.is_some() && r.updated_at_ms.is_some());

        // every feed resumes after its last event, and one from another
        // node is a resync
        let a = &admins["n2"];
        let last = format!("{}.{}", a.feed().boot(), a.feed().last());
        assert!(a.feed().subscribe(Some(&last)).unwrap().resumed);
        let b = admins["n3"].feed().subscribe(Some(&last)).unwrap();
        assert_eq!(b.resync, Some(crate::admin::changes::ResyncReason::UnknownCursor));
        drop(subs);
    }
}
