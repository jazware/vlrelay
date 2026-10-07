//! The operator API over the node's real state: hosts and their actions,
//! consumers, the overview's numbers and account lookups and takedowns.
//! Policy, domain rules, cases and host tier actions go to the policy
//! engine's admin half (`node::policy`); a host action's answer waits for
//! the leader's host table to hold it (`Glue::settle_host`). The cluster and Quorum views are
//! the quorum log's members (`node::quorum::Glue`).
//!
//! Numbers are this node's own: the hosts it reads, its consumers, its
//! rates. Other members' rates and hosts show in the cluster view (from
//! their statuses); their consumers, host details and reconnects are on
//! their own dashboards. Accounts are the leader's records, so an account
//! lookup or a takedown answers on any node only through the leader's log
//! (takedowns) or on the leader itself (lookups).

mod changes;

use super::Node;
use super::metrics::HostSeries as Series;
use super::policy::PolicyHooks;
use crate::admin::changes::ChangeKind;
use crate::admin::fleet::{self, Member, NodeReport};
use crate::admin::{self, AdminError, AdminResult, AdminSource, RejectReason};
use crate::policy::admin::PolicyAdmin;
use crate::state::{AccountStatus, Upstream};
use crate::types::Host;
use crate::upstream::{Backpressure, HostStatus, HostView, Tier};
use bytes::Bytes;
use parking_lot::Mutex;
use std::collections::{BTreeMap, HashMap};
use std::sync::Arc;
use std::time::{Duration, Instant};

pub struct NodeAdmin {
    pub node: Arc<Node>,
    pub policy: Arc<PolicyHooks>,
    /// (when, open cases): the overview polls every second or two, and a
    /// count is a bucket listing.
    open_cases: Mutex<Option<(Instant, u32)>>,
    cpu: Mutex<Option<(Instant, f64, f64)>>,
    settings: Option<admin::SettingsView>,
    /// The request counts at the last store sample, for its rates.
    store_prev: Mutex<Option<(Instant, crate::qlog::bucket::Requests)>>,
    /// `retain/qlog`, read at most every [`RETAIN_TTL`].
    retain: tokio::sync::Mutex<Option<(Instant, Option<serde_json::Value>)>>,
    /// (when, PLC fetches, seeded misses, new accounts) at the last usage
    /// sample, for its rates.
    usage_prev: Mutex<(Instant, u64, u64, u64)>,
    /// Rejects per (host, reason) at the last sample, and the rates then.
    rejects_prev: Mutex<RejectSample>,
    feed: Arc<crate::admin::changes::ChangeFeed>,
    watch: Mutex<changes::Watch>,
    /// How long a host action waits to see its write in the cluster's host
    /// table before answering `pending`.
    settle: Duration,
}

/// [`NodeAdmin::settle`]'s default: the leader's table is a round trip
/// away, so only a missing leader or a lost write takes this long.
pub const HOST_ACTION_SETTLE: Duration = Duration::from_secs(3);

#[derive(Default)]
struct RejectSample {
    at: Option<Instant>,
    counts: HashMap<(String, RejectReason), u64>,
    rates: HashMap<(String, RejectReason), f64>,
}

fn usage_counts(n: &Node) -> (u64, u64, u64) {
    use std::sync::atomic::Ordering::Relaxed;
    let s = &n.identity.stats;
    (s.fetches.load(Relaxed), s.seeded.load(Relaxed), super::metrics::NEW_ACCOUNTS.get())
}

const RETAIN_TTL: Duration = Duration::from_secs(30);
/// A store sample this young isn't replaced, so rates span a few polls.
const STORE_WINDOW: Duration = Duration::from_secs(10);

const OPEN_CASES_TTL: Duration = Duration::from_secs(10);

fn now_ms() -> i64 {
    crate::upstream::host::now_ms() as i64
}

/// CPU seconds this process has used (user + system).
pub(crate) fn cpu_seconds() -> f64 {
    // SAFETY: getrusage fills the struct it's handed.
    unsafe {
        let mut r: libc::rusage = std::mem::zeroed();
        if libc::getrusage(libc::RUSAGE_SELF, &mut r) != 0 {
            return 0.0;
        }
        let s = |t: libc::timeval| t.tv_sec as f64 + t.tv_usec as f64 / 1e6;
        s(r.ru_utime) + s(r.ru_stime)
    }
}

/// Every relay gauge about in-flight work, queues, backlogs, caps and
/// pauses, by name and labels. Read by name so the ones the pipeline adds
/// later show up without a change here.
fn pipeline_gauges() -> BTreeMap<String, f64> {
    const WORDS: [&str; 9] =
        ["inflight", "in_flight", "pending", "queued", "backlog", "paused", "cap", "dedupe", "lag"];
    let mut out = BTreeMap::new();
    for fam in prometheus::gather() {
        let name = fam.name();
        if !name.starts_with("vlrelay_") || !WORDS.iter().any(|w| name.contains(w)) {
            continue;
        }
        if fam.get_field_type() != prometheus::proto::MetricType::GAUGE {
            continue;
        }
        for m in fam.get_metric() {
            let labels: Vec<String> = m.get_label().iter().map(|l| format!("{}=\"{}\"", l.name(), l.value())).collect();
            let key = if labels.is_empty() { name.to_string() } else { format!("{name}{{{}}}", labels.join(",")) };
            out.insert(key, m.get_gauge().get_value());
            if out.len() >= 200 {
                return out;
            }
        }
    }
    out
}

impl NodeAdmin {
    pub fn new(node: Arc<Node>, policy: Arc<PolicyHooks>) -> NodeAdmin {
        NodeAdmin {
            policy,
            open_cases: Mutex::new(None),
            cpu: Mutex::new(None),
            settings: None,
            // primed now, so the first answer has a window
            store_prev: Mutex::new(Some((Instant::now(), crate::qlog::bucket::requests()))),
            retain: tokio::sync::Mutex::new(None),
            rejects_prev: Mutex::new(RejectSample { at: Some(Instant::now()), ..Default::default() }),
            usage_prev: Mutex::new({
                let (f, s, a) = usage_counts(&node);
                (Instant::now(), f, s, a)
            }),
            feed: crate::admin::changes::ChangeFeed::new(&node.cfg.node_id),
            watch: Mutex::new(changes::Watch::default()),
            settle: HOST_ACTION_SETTLE,
            node,
        }
    }

    /// The process's effective config, for the Settings page.
    pub fn with_settings(mut self, s: admin::SettingsView) -> NodeAdmin {
        self.settings = Some(s);
        self
    }

    pub fn with_settle(mut self, d: Duration) -> NodeAdmin {
        self.settle = d;
        self
    }

    fn id(&self) -> &str {
        &self.node.cfg.node_id
    }

    async fn members(&self) -> Arc<Vec<Member>> {
        Arc::new(vec![Member::ok(self.local_report().await)])
    }

    /// (cores busy since the last read at least a second ago, resident bytes).
    fn process(&self) -> (f64, u64) {
        let mut g = self.cpu.lock();
        let (now, total) = (Instant::now(), cpu_seconds());
        let rate = match *g {
            Some((at, _, rate)) if now.duration_since(at) < Duration::from_secs(1) => rate,
            Some((at, prev, _)) => {
                let r = ((total - prev) / now.duration_since(at).as_secs_f64()).max(0.0);
                *g = Some((now, total, r));
                r
            }
            None => {
                *g = Some((now, total, 0.0));
                0.0
            }
        };
        (rate, vlpds::metrics::resident_bytes().unwrap_or(0))
    }

    fn admin(&self) -> &PolicyAdmin {
        &self.policy.admin
    }
}

pub fn host_status_label(h: &HostView) -> &'static str {
    match status(h) {
        admin::HostStatus::Connected => "connected",
        admin::HostStatus::Idle => "idle",
        admin::HostStatus::Backoff | admin::HostStatus::Offline => "backoff",
        admin::HostStatus::Throttled => "throttled",
        admin::HostStatus::Backpressure => "backpressure",
        admin::HostStatus::Suspended => "suspended",
        admin::HostStatus::Banned => "banned",
    }
}

fn status(h: &HostView) -> admin::HostStatus {
    match h.record.tier {
        Tier::Banned => return admin::HostStatus::Banned,
        Tier::Suspended => return admin::HostStatus::Suspended,
        _ => {}
    }
    match h.record.status {
        HostStatus::Active => admin::HostStatus::Connected,
        HostStatus::Idle => admin::HostStatus::Idle,
        HostStatus::Connecting | HostStatus::Backoff => admin::HostStatus::Backoff,
        HostStatus::Throttled => admin::HostStatus::Throttled,
        HostStatus::Backpressure => admin::HostStatus::Backpressure,
    }
}

fn backpressure_reason(h: &HostView) -> Option<admin::BackpressureReason> {
    if status(h) != admin::HostStatus::Backpressure {
        return None;
    }
    h.backpressure.map(|b| match b {
        Backpressure::InflightFull => admin::BackpressureReason::InflightFull,
        Backpressure::NodeInflightFull => admin::BackpressureReason::NodeInflightFull,
        Backpressure::QueueFull => admin::BackpressureReason::QueueFull,
    })
}

/// The dashboard's coarser reject classes.
pub fn reject_class(reason: &str) -> RejectReason {
    match reason {
        "bad_signature" | "no_signing_key" => RejectReason::BadSignature,
        "frame_too_big" | "blocks_too_big" | "too_many_ops" | "too_many_blocks" => RejectReason::TooLarge,
        "bad_header" | "bad_frame" | "missing_field" | "bad_field" | "bad_seq" | "bad_did" | "bad_rev" => {
            RejectReason::Malformed
        }
        "stale" | "rev_not_newer" => RejectReason::RevOutOfOrder,
        "prev_data_mismatch" | "inversion_mismatch" | "desynchronized" | "chain" => RejectReason::PrevDataMismatch,
        "wrong_host" => RejectReason::WrongHost,
        "unknown_did" | "no_identity" | "identity_unavailable" => RejectReason::UnknownDid,
        "inactive" => RejectReason::Inactive,
        "rate_limited" | "new_account_deferred" => RejectReason::RateLimited,
        _ => RejectReason::InvalidCommit,
    }
}

/// A host this far ahead of real time on its own timeline is shown
/// catching up.
const CATCHING_UP: f64 = 1.5;

/// Seconds of rate history on each of the overview's top hosts.
const TOP_HISTORY: usize = 60;

impl NodeAdmin {
    fn row(
        &self,
        h: &HostView,
        series: Option<&Series>,
        rejects: Option<&super::metrics::HostRejects>,
    ) -> admin::HostRow {
        let (rate, ratio) = series.map_or((0.0, 0.0), |s| (s.rate(), s.reject_ratio()));
        admin::HostRow {
            host: h.record.hostname.clone(),
            tier: h.record.tier.as_str().into(),
            status: status(h),
            backpressure_reason: backpressure_reason(h),
            events_per_sec: rate,
            error_rate: ratio,
            accounts: self.policy.accounts(&h.record.hostname).unwrap_or(h.record.account_count),
            last_upstream_seq: h.received_seq.unwrap_or(0),
            connected_since_ms: matches!(
                h.record.status,
                HostStatus::Active | HostStatus::Throttled | HostStatus::Backpressure
            )
            .then_some(h.record.last_connected_ms.map(|m| m as i64))
            .flatten(),
            lag_ms: h.read_lag_ms.unwrap_or(0) as f64,
            catch_up_pace: (h.pace >= CATCHING_UP).then_some((h.pace * 10.0).round() / 10.0),
            throttle: self.policy.throttle(&h.record.hostname),
            rule: self.policy.limits(&h.record.hostname).and_then(|l| l.rule),
            node: self.node.cfg.node_id.clone(),
            max_accounts: self.policy.limits(&h.record.hostname).and_then(|l| l.limits).map_or(0, |l| l.max_accounts),
            history: Vec::new(),
            throttled_accounts: self.node.quorum.hosts.throttled(&h.record.hostname),
            source: self.node.quorum.hosts.source(&h.record.hostname),
            top_reason: rejects.and_then(top_reason),
            version: None,
            updated_at_ms: None,
            owner_version: None,
            pending: false,
        }
    }

    /// Every host in this node's registry (every host the leader's table
    /// has; only the ones this node reads have live numbers).
    fn rows(&self) -> Vec<admin::HostRow> {
        let hosts = self.node.manager.hosts();
        let dash = self.node.dash.lock();
        let rejects = self.node.rejects.lock();
        let mut rows: Vec<admin::HostRow> = hosts
            .iter()
            .map(|h| {
                let k = Host(h.record.hostname.clone());
                self.row(h, dash.hosts.get(&k), rejects.get(&k))
            })
            .collect();
        drop((dash, rejects));
        self.stamp(&mut rows);
        rows
    }

    /// The hosts this node reads.
    pub fn owned_rows(&self) -> Vec<admin::HostRow> {
        let mut rows = self.rows();
        rows.retain(|r| self.node.manager.is_running(&Host(r.host.clone())));
        rows
    }

    fn host_row(&self, host: &str) -> AdminResult<admin::HostRow> {
        let k = Host(host.to_string());
        let h = self.node.manager.host(&k).ok_or_else(|| AdminError::NotFound(format!("unknown host {host}")))?;
        let mut row = {
            let dash = self.node.dash.lock();
            let rejects = self.node.rejects.lock();
            self.row(&h, dash.hosts.get(&k), rejects.get(&k))
        };
        self.stamp(std::slice::from_mut(&mut row));
        Ok(row)
    }

    async fn open_case_count(&self) -> u32 {
        if let Some((at, n)) = *self.open_cases.lock()
            && at.elapsed() < OPEN_CASES_TTL
        {
            return n;
        }
        match self.policy.engine.cases.list(None).await {
            Ok(cs) => {
                let n = cs.iter().filter(|c| c.is_open()).count() as u32;
                *self.open_cases.lock() = Some((Instant::now(), n));
                n
            }
            Err(e) => {
                tracing::warn!("listing cases: {e:#}");
                0
            }
        }
    }

    async fn account_view(&self, did: &str) -> AdminResult<admin::Account> {
        let rec = self
            .node
            .state
            .get(did)
            .await
            .map_err(|e| match e {
                crate::state::StoreError::NotOwner(_) => AdminError::BadRequest(format!(
                    "accounts are the leader's records: ask {}",
                    self.node.quorum.qnode.status().leader.unwrap_or_else(|| "the leader".into())
                )),
                e => AdminError::Internal(anyhow::anyhow!("{e}")),
            })?
            .ok_or_else(|| AdminError::NotFound(format!("no account {did}")))?;
        let handle = match self.node.identity.cached(did) {
            Some(Ok(id)) => id.handle.clone(),
            _ => None,
        };
        let status = rec.status();
        let upstream = match rec.upstream {
            Upstream::Active => "active",
            Upstream::Takendown => "takendown",
            Upstream::Suspended => "suspended",
            Upstream::Deleted => "deleted",
            Upstream::Deactivated => "deactivated",
            Upstream::Desynchronized => "desynchronized",
            Upstream::Throttled => "throttled",
            Upstream::Inactive => "inactive",
        };
        Ok(admin::Account {
            did: did.to_string(),
            handle,
            host: self.node.state.host_name(rec.host).map(|h| h.to_string()).unwrap_or_default(),
            status: status_str(status).into(),
            upstream_status: upstream.into(),
            takedown: match rec.relay_takedown {
                false => None,
                true => Some(match self.policy.engine.takedowns.latest(did).await {
                    Ok(Some(t)) if t.takedown => admin::Takedown { at_ms: t.at_ms, by: t.by, reason: t.reason },
                    _ => admin::Takedown { at_ms: 0, by: "admin".into(), reason: String::new() },
                }),
            },
            rev: rec.chain.map(|c| c.rev.to_string()).unwrap_or_default(),
            last_seq: 0,
            last_event_ms: 0,
            events_last_hour: 0,
            rejects_last_hour: 0,
            did_shard: 0,
            node: self.node.cfg.node_id.clone(),
        })
    }

    /// On the quorum log the leader makes the takedown: the record's flag
    /// and the `#account` announcing it are one entry.
    async fn set_takedown(&self, did: &str, takedown: bool, by: &str, reason: &str) -> AdminResult<admin::Account> {
        let e = self.policy.engine.takedowns.record(did, takedown, by, reason).await?;
        self.node.serve.takedowns.apply_local(did, takedown);
        self.node.serve.takedowns.note(e);
        match self.node.quorum.takedown(did, takedown).await {
            Ok(_) => {}
            Err(e) if format!("{e}").starts_with("no_account") => {
                return Err(AdminError::NotFound(format!("no account {did}")));
            }
            Err(e) => return Err(AdminError::Internal(e)),
        }
        match self.account_view(did).await {
            // a follower made it through the leader's log but doesn't hold
            // the record: answer with what it knows
            Err(AdminError::BadRequest(_)) => Ok(admin::Account {
                did: did.to_string(),
                handle: None,
                host: String::new(),
                status: if takedown { "takendown".into() } else { String::new() },
                upstream_status: String::new(),
                takedown: takedown.then(|| admin::Takedown {
                    at_ms: chrono::Utc::now().timestamp_millis(),
                    by: by.to_string(),
                    reason: reason.to_string(),
                }),
                rev: String::new(),
                last_seq: 0,
                last_event_ms: 0,
                events_last_hour: 0,
                rejects_last_hour: 0,
                did_shard: 0,
                node: self.node.quorum.qnode.status().leader.unwrap_or_default(),
            }),
            r => r,
        }
    }
}

fn status_str(s: AccountStatus) -> &'static str {
    s.as_str().unwrap_or(if s.is_active() { "active" } else { "inactive" })
}

/// What this node itself measures and holds.
impl NodeAdmin {
    pub async fn local_report(&self) -> NodeReport {
        let rows = self.owned_rows();
        let mut by_status: BTreeMap<admin::HostStatus, u32> = BTreeMap::new();
        for r in &rows {
            *by_status.entry(r.status).or_default() += 1;
        }
        let (last, rejects_by_reason, h) = self.dash_numbers();
        let (pipeline, pipeline_hosts) = self.pipeline(&rows);
        let mut top = rows;
        top.sort_by(|a, b| b.events_per_sec.total_cmp(&a.events_per_sec));
        top.truncate(10);
        {
            let d = self.node.dash.lock();
            for r in &mut top {
                if let Some(s) = d.hosts.get(&Host(r.host.clone())) {
                    r.history = s.events.iter().rev().take(TOP_HISTORY).rev().copied().collect();
                }
            }
        }
        let (cpu, mem_bytes) = self.process();
        NodeReport {
            node: self.id().to_string(),
            role: match self.node.quorum.qnode.status().role {
                crate::qlog::node::Role::Leader => "leader",
                crate::qlog::node::Role::Candidate => "candidate",
                crate::qlog::node::Role::Follower => "follower",
            }
            .into(),
            version: env!("CARGO_PKG_VERSION").into(),
            time_ms: now_ms(),
            events_in_per_sec: last.events_in,
            events_out_per_sec: last.events_out,
            bytes_in_per_sec: last.bytes_in,
            bytes_out_per_sec: last.bytes_out,
            consumers: self.node.serve.consumers().len() as u32,
            hosts_by_status: by_status,
            rejects_by_reason,
            ttf_p50_ms: last.ttf_p50_ms,
            ttf_p99_ms: last.ttf_p99_ms,
            commit_lag_ms: last.durable_lag_ms,
            stream_seq: self.node.serve.head(),
            cpu,
            mem_bytes,
            top_hosts: top,
            history: h,
            pipeline,
            pipeline_hosts,
            plc: None,
        }
    }

    /// The last sample, rejects per second by class over the last minute,
    /// and the history.
    fn dash_numbers(&self) -> (super::metrics::Sample, BTreeMap<RejectReason, f64>, admin::History) {
        let dash = self.node.dash.lock();
        let last = dash.history.back().cloned().unwrap_or_default();
        let window: Vec<_> = dash.history.iter().rev().take(60).collect();
        let mut rejects_by_reason: BTreeMap<RejectReason, f64> = BTreeMap::new();
        for s in &window {
            for (k, v) in &s.rejects {
                *rejects_by_reason.entry(reject_class(k)).or_default() += v / window.len().max(1) as f64;
            }
        }
        let mut h = admin::History { sample_secs: 1, ..Default::default() };
        for s in &dash.history {
            h.t.push(s.t);
            h.events_in.push(s.events_in);
            h.events_out.push(s.events_out);
            h.bytes_in.push(s.bytes_in);
            h.bytes_out.push(s.bytes_out);
            h.ttf_p50_ms.push(s.ttf_p50_ms);
            h.ttf_p99_ms.push(s.ttf_p99_ms);
            h.durability_lag_ms.push(s.durable_lag_ms);
        }
        for r in RejectReason::ALL {
            let v: Vec<f64> = dash
                .history
                .iter()
                .map(|s| s.rejects.iter().filter(|(k, _)| reject_class(k) == r).map(|(_, v)| v).sum())
                .collect();
            h.rejects.insert(r, v);
        }
        (last, rejects_by_reason, h)
    }

    fn pipeline(&self, owned: &[admin::HostRow]) -> (admin::PipelineNode, Vec<admin::PipelineHost>) {
        let snap = self.node.acks.snapshot();
        let mut hosts: Vec<admin::PipelineHost> = owned
            .iter()
            .filter_map(|r| {
                let inflight = self.node.acks.pending_for(&Host(r.host.clone())) as u64;
                let paused = r.status == admin::HostStatus::Backpressure;
                (inflight > 0 || paused).then(|| admin::PipelineHost {
                    host: r.host.clone(),
                    node: self.id().to_string(),
                    inflight,
                    inflight_cap: None,
                    paused,
                    status: Some(r.status),
                    events_per_sec: r.events_per_sec,
                })
            })
            .collect();
        let paused_hosts = hosts.iter().filter(|h| h.paused).count() as u32;
        hosts.sort_by_key(|h| std::cmp::Reverse(h.inflight));
        hosts.truncate(100);
        let node = admin::PipelineNode {
            node: self.id().to_string(),
            stale: false,
            ack_pending: snap.pending as u64,
            oldest_pending_ms: snap.oldest_pending.map_or(0.0, |t| t.elapsed().as_secs_f64() * 1000.0),
            lane_queued: super::metrics::LANE_QUEUED.get().max(0) as u64,
            dedupe_entries: 0,
            paused_hosts,
            gauges: pipeline_gauges(),
        };
        (node, hosts)
    }

    pub async fn local_host(&self, host: &str) -> AdminResult<admin::HostDetail> {
        let row = self.host_row(host)?;
        let k = Host(host.to_string());
        let rec = self.policy.hosts.get_host(host).await?;
        let (limits, actions) = match &rec {
            Some(r) => (self.admin().host_limits(r), PolicyAdmin::host_actions(r)),
            None => (
                admin::TierLimits {
                    events_per_sec: 0.0,
                    events_per_hour: 0,
                    events_per_day: 0,
                    max_accounts: 0,
                    new_accounts_per_hour: 0,
                },
                Vec::new(),
            ),
        };
        let open_cases = match self.policy.engine.cases.list(None).await {
            Ok(cs) => cs.iter().filter(|c| c.host == host && c.is_open()).map(|c| c.id).collect(),
            Err(e) => {
                tracing::warn!("listing cases: {e:#}");
                Vec::new()
            }
        };
        let now = crate::upstream::host::now_ms() as i64;
        let new_accounts_per_hour =
            self.policy.engine.signals.snapshot(host, None, now).get("new-accounts").copied().unwrap_or(0.0);
        let (rejects_by_reason, recent_rejects) = {
            let r = self.node.rejects.lock();
            match r.get(&k) {
                Some(x) => {
                    let mut by: BTreeMap<RejectReason, u64> = BTreeMap::new();
                    for (reason, n) in &x.by_reason {
                        *by.entry(reject_class(reason)).or_default() += n;
                    }
                    let recent = x
                        .recent
                        .iter()
                        .rev()
                        .map(|n| admin::RejectSample {
                            at_ms: n.at_ms,
                            did: n.did.clone(),
                            reason: reject_class(n.reason),
                            upstream_seq: n.upstream_seq,
                            detail: format!("{}: {}", n.reason, n.detail),
                        })
                        .collect();
                    (by, recent)
                }
                None => Default::default(),
            }
        };
        let series = {
            let d = self.node.dash.lock();
            match d.hosts.get(&k) {
                Some(s) => admin::HostSeries {
                    sample_secs: 1,
                    t: s.t.iter().copied().collect(),
                    events: s.events.iter().copied().collect(),
                    rejects: s.rejects.iter().copied().collect(),
                },
                None => admin::HostSeries { sample_secs: 1, ..Default::default() },
            }
        };
        Ok(admin::HostDetail {
            row,
            limits,
            new_accounts_per_hour,
            rejects_by_reason,
            recent_rejects,
            series,
            actions,
            open_cases,
        })
    }

    pub fn local_reconnect(&self, host: &str) -> AdminResult<()> {
        let k = Host(host.to_string());
        if self.node.manager.host(&k).is_none() {
            return Err(AdminError::NotFound(format!("unknown host {host}")));
        }
        self.node.manager.kick(&k);
        Ok(())
    }

    pub fn local_consumers(&self) -> Vec<admin::Consumer> {
        self.node
            .serve
            .consumers()
            .into_iter()
            .map(|c| admin::Consumer {
                id: c.id,
                ip: c.ip.to_string(),
                user_agent: c.user_agent,
                node: self.id().to_string(),
                connected_since_ms: c.connected_since_ms,
                cursor: c.last_seq,
                lag_ms: c.lag_ms,
                events_per_sec: c.events_per_sec,
                bytes_per_sec: c.bytes_per_sec,
                backfilling: c.backfilling,
                read_tier: self.read_tier(c.pos, c.backfilling).into(),
            })
            .collect()
    }

    /// Where a consumer at `pos` reads next: the firehose's ring, this
    /// node's own log (memory or commitlog), or the bucket's segments.
    fn read_tier(&self, pos: i64, backfilling: bool) -> &'static str {
        let in_ring = self.node.serve.firehose().is_some_and(|f| f.from_ring(pos).1);
        if !backfilling || in_ring {
            "ring"
        } else if pos as u64 + 1 >= self.node.quorum.qnode.readable_floor() {
            "disk"
        } else {
            "bucket"
        }
    }

    pub fn local_kick(&self, id: u64, by: &str) -> AdminResult<()> {
        if !self.node.serve.kick(id) {
            return Err(AdminError::NotFound(format!("no connected consumer {id}")));
        }
        tracing::info!(target: "vlrelay::audit", consumer = id, by, "consumer kicked");
        self.consumer_left(id);
        Ok(())
    }

    pub async fn local_account(&self, did: &str) -> AdminResult<admin::Account> {
        self.account_view(did).await
    }

    /// Accounts this node can show: by DID, or by handle from the DID
    /// documents its identity cache holds (every account with recent
    /// traffic on its hosts), exact first, then as a prefix.
    pub async fn local_accounts(&self, q: &str) -> AdminResult<Vec<admin::Account>> {
        if q.is_empty() {
            return Ok(Vec::new());
        }
        if q.starts_with("did:") {
            return match self.account_view(q).await {
                Ok(a) => Ok(vec![a]),
                Err(AdminError::NotFound(_)) => Ok(Vec::new()),
                Err(e) => Err(e),
            };
        }
        let mut ids = self.node.identity.find_handle(q, 100);
        if ids.is_empty() && !q.ends_with('*') {
            ids = self.node.identity.find_handle(&format!("{q}*"), 100);
        }
        let mut out = Vec::new();
        for id in ids {
            match self.account_view(&id.did).await {
                Ok(a) => out.push(a),
                Err(AdminError::NotFound(_)) => {}
                Err(e) => return Err(e),
            }
        }
        Ok(out)
    }

    pub async fn local_takedown(
        &self,
        did: &str,
        takedown: bool,
        by: &str,
        reason: &str,
    ) -> AdminResult<admin::Account> {
        self.set_takedown(did, takedown, by, reason).await
    }

    async fn local_usage(&self) -> AdminResult<admin::PolicyUsage> {
        let (f, sd, a) = usage_counts(&self.node);
        let (secs, df, ds, da) = {
            let mut p = self.usage_prev.lock();
            let secs = p.0.elapsed().as_secs_f64();
            let d = (f.saturating_sub(p.1), sd.saturating_sub(p.2), a.saturating_sub(p.3));
            if secs >= STORE_WINDOW.as_secs_f64() {
                *p = (Instant::now(), f, sd, a);
            }
            (secs, d.0, d.1, d.2)
        };
        let per = |n: u64| if secs > 0.0 { n as f64 / secs } else { 0.0 };
        let e = &self.policy.engine;
        let c = e.snapshot().policy.body.cluster.clone();
        Ok(admin::PolicyUsage {
            node: self.id().to_string(),
            plc_lookups_per_sec: per(df),
            plc_lookups_budget: c.plc_lookups_per_sec,
            plc_lookups_share: e.budget(crate::policy::budget::BudgetKind::PlcLookupsPerSec),
            seeded_per_sec: per(ds),
            new_accounts_per_min: per(da) * 60.0,
            new_accounts_budget: c.new_accounts_per_min,
            new_hosts_today: e.new_hosts_today().await.map_err(AdminError::Internal)?,
            new_hosts_per_day: c.new_hosts_per_day,
            window_secs: secs,
        })
    }

    /// This node's hosts with the most rejects of `reason` (all: None).
    fn local_rejects_top(&self, reason: Option<RejectReason>, limit: usize) -> Vec<admin::RejectTop> {
        let r = self.node.rejects.lock();
        let mut counts: HashMap<(String, RejectReason), u64> = HashMap::new();
        for (h, x) in r.iter() {
            for (raw, n) in &x.by_reason {
                *counts.entry((h.0.clone(), reject_class(raw))).or_default() += n;
            }
        }
        let rates = {
            let mut p = self.rejects_prev.lock();
            let secs = p.at.map_or(0.0, |t| t.elapsed().as_secs_f64());
            if secs >= STORE_WINDOW.as_secs_f64() || p.rates.is_empty() {
                p.rates = counts
                    .iter()
                    .map(|(k, n)| {
                        (k.clone(), n.saturating_sub(p.counts.get(k).copied().unwrap_or(0)) as f64 / secs.max(1.0))
                    })
                    .collect();
                p.counts = counts.clone();
                p.at = Some(Instant::now());
            }
            p.rates.clone()
        };
        let mut out: Vec<admin::RejectTop> = r
            .iter()
            .filter_map(|(h, x)| {
                let keep = |c: RejectReason| reason.is_none_or(|w| w == c);
                let total: u64 = x.by_reason.iter().filter(|(raw, _)| keep(reject_class(raw))).map(|(_, n)| n).sum();
                if total == 0 {
                    return None;
                }
                let per_sec =
                    rates.iter().filter(|((host, c), _)| *host == h.0 && keep(*c)).map(|(_, v)| v).sum::<f64>();
                let newest = x.recent.iter().rev().find(|n| keep(reject_class(n.reason)));
                Some(admin::RejectTop {
                    host: h.0.clone(),
                    rejects_per_sec: per_sec,
                    total,
                    last_at_ms: newest.map(|n| n.at_ms),
                    sample: newest.map(|n| admin::RejectSample {
                        at_ms: n.at_ms,
                        did: n.did.clone(),
                        reason: reject_class(n.reason),
                        upstream_seq: n.upstream_seq,
                        detail: format!("{}: {}", n.reason, n.detail),
                    }),
                })
            })
            .collect();
        rank_rejects(&mut out, limit);
        out
    }

    fn sort_page(mut rows: Vec<admin::HostRow>, q: &admin::HostQuery) -> admin::HostList {
        rows.retain(|r| q.q.as_deref().is_none_or(|s| r.host.contains(s)));
        rows.retain(|r| q.tier.as_deref().is_none_or(|t| r.tier == t));
        rows.retain(|r| q.status.is_none_or(|s| r.status == s));
        rows.retain(|r| q.keeps(r));
        match q.sort.as_deref() {
            Some("throttled") => rows.sort_by_key(|r| r.throttled_accounts),
            Some("source") => rows.sort_by(|a, b| a.source.cmp(&b.source)),
            Some("events") => rows.sort_by(|a, b| a.events_per_sec.total_cmp(&b.events_per_sec)),
            Some("errors") => rows.sort_by(|a, b| a.error_rate.total_cmp(&b.error_rate)),
            Some("accounts") => rows.sort_by_key(|r| r.accounts),
            Some("seq") => rows.sort_by_key(|r| r.last_upstream_seq),
            Some("tier") => rows.sort_by(|a, b| a.tier.cmp(&b.tier)),
            Some("status") => rows.sort_by_key(|r| r.status),
            Some("since") => rows.sort_by_key(|r| r.connected_since_ms),
            Some("lag") => rows.sort_by(|a, b| a.lag_ms.total_cmp(&b.lag_ms)),
            _ => rows.sort_by(|a, b| a.host.cmp(&b.host)),
        }
        if q.desc {
            rows.reverse();
        }
        let total = rows.len();
        let rows = rows.into_iter().skip(q.offset.unwrap_or(0)).take(q.limit.unwrap_or(10_000)).collect();
        admin::HostList { total, hosts: rows }
    }
}

impl AdminSource for NodeAdmin {
    fn changes(&self) -> Option<Arc<crate::admin::changes::ChangeFeed>> {
        Some(self.feed.clone())
    }

    async fn overview(&self) -> AdminResult<admin::Overview> {
        let open_cases = self.open_case_count().await;
        let members = self.members().await;
        Ok(fleet::overview(&members, open_cases, now_ms()))
    }

    async fn hosts(&self, q: admin::HostQuery) -> AdminResult<admin::HostList> {
        let mut rows = self.rows();
        // a host another member reads: named, without this node's numbers
        let owners = self.node.quorum.hosts.owners();
        for r in &mut rows {
            if let Some(o) = owners.get(&r.host) {
                r.node = o.clone();
            }
        }
        Ok(Self::sort_page(rows, &q))
    }

    async fn host(&self, host: &str) -> AdminResult<admin::HostDetail> {
        self.local_host(host).await
    }

    async fn host_action(&self, host: &str, action: admin::HostAction, by: &str) -> AdminResult<admin::HostRow> {
        let k = Host(host.to_string());
        if self.node.manager.host(&k).is_none() {
            return Err(AdminError::NotFound(format!("unknown host {host}")));
        }
        let _acting = self.acting(host);
        let mut pending = false;
        match action {
            admin::HostAction::Reconnect if !self.node.manager.is_running(&k) => {
                let owner = self.node.quorum.hosts.owners().get(host).cloned().unwrap_or_else(|| "nobody yet".into());
                return Err(AdminError::BadRequest(format!("{host} is read by {owner}: reconnect it there")));
            }
            admin::HostAction::Reconnect => self.node.manager.kick(&k),
            a => {
                self.admin().host_action(host, a, by).await?;
                // this node's record reads the tier and policy from its copy
                // of the leader's table, so the answer waits for that copy
                // to hold the write
                pending = !self.node.quorum.settle_host(host, self.settle).await;
                // the socket follows now, not at the sync loop's next pass
                self.policy.refresh_host(host).await?;
            }
        }
        let mut row = self.host_row(host)?;
        if let Some(o) = self.node.quorum.hosts.owners().get(host) {
            row.node = o.clone();
        }
        row.pending = pending;
        self.host_acted(&mut row);
        Ok(row)
    }

    async fn domain_rules(&self) -> AdminResult<Vec<admin::DomainRule>> {
        self.admin().domain_rules().await
    }
    async fn create_domain_rule(&self, rule: admin::DomainRuleInput, by: &str) -> AdminResult<admin::DomainRule> {
        let r = self.admin().create_domain_rule(rule, by).await?;
        self.policy_saved(ChangeKind::Rules, r.version);
        Ok(r)
    }
    async fn update_domain_rule(
        &self,
        id: u64,
        rule: admin::DomainRuleInput,
        by: &str,
    ) -> AdminResult<admin::DomainRule> {
        let r = self.admin().update_domain_rule(id, rule, by).await?;
        self.policy_saved(ChangeKind::Rules, r.version);
        Ok(r)
    }
    async fn delete_domain_rule(&self, id: u64, by: &str) -> AdminResult<()> {
        self.admin().delete_domain_rule(id, by).await?;
        self.policy_saved(ChangeKind::Rules, self.policy.engine.rules().0);
        Ok(())
    }
    async fn policy(&self) -> AdminResult<admin::PolicyDoc> {
        self.admin().policy().await
    }
    async fn update_policy(&self, update: admin::PolicyUpdate, by: &str) -> AdminResult<admin::PolicyDoc> {
        let d = self.admin().update_policy(update, by).await?;
        self.policy_saved(ChangeKind::Policy, d.version);
        Ok(d)
    }
    async fn policy_audit(&self) -> AdminResult<Vec<admin::PolicyAudit>> {
        self.admin().policy_audit().await
    }
    async fn full_policy(&self) -> AdminResult<admin::FullPolicyDoc> {
        Ok(full_doc(&self.admin().full_policy().await))
    }
    async fn update_full_policy(&self, u: admin::FullPolicyUpdate, by: &str) -> AdminResult<admin::FullPolicyDoc> {
        let body: crate::policy::PolicyBody =
            serde_json::from_value(u.policy).map_err(|e| AdminError::BadRequest(format!("policy: {e}")))?;
        let d = self.admin().update_full_policy(u.base_version, body, &u.note, by).await?;
        self.policy_saved(ChangeKind::Policy, d.version);
        Ok(full_doc(&d))
    }
    async fn domain_rules_audit(&self) -> AdminResult<Vec<admin::PolicyAudit>> {
        Ok(self
            .admin()
            .domain_rules_audit()
            .await?
            .into_iter()
            .map(|a| admin::PolicyAudit {
                version: a.version,
                at_ms: a.at_ms,
                by: a.by,
                note: a.note,
                changes: a.changes,
            })
            .collect())
    }

    async fn case_detail(&self, id: u64) -> AdminResult<admin::CaseDetail> {
        let c = self.admin().case_detail(id).await?;
        Ok(admin::CaseDetail {
            case: c.to_wire(),
            trips: c.trips,
            evidence: c
                .evidence
                .into_iter()
                .map(|e| admin::CaseEvidence {
                    at_ms: e.at_ms,
                    observed: e.observed,
                    threshold: e.threshold,
                    window_secs: e.window_secs,
                    node: e.node,
                    detail: e.detail,
                    signals: e.signals,
                })
                .collect(),
        })
    }

    /// This node's consumers (each member's dashboard lists its own).
    /// Every member's consumers, asked over the peer protocol; a member
    /// that doesn't answer is left out (the cluster view marks it stale).
    async fn consumers(&self) -> AdminResult<Vec<admin::Consumer>> {
        let mut out = Vec::new();
        for (id, r) in self.node.quorum.ask_all("node:consumers", Bytes::new()).await {
            if id == self.id() {
                continue;
            }
            match r.ok().and_then(|b| serde_json::from_slice::<Vec<admin::Consumer>>(&b).ok()) {
                Some(cs) => out.extend(cs),
                None => tracing::debug!(node = %id, "a member's consumers didn't come back"),
            }
        }
        out.extend(self.local_consumers());
        out.sort_by(|a, b| (&a.node, a.id).cmp(&(&b.node, b.id)));
        Ok(out)
    }

    async fn kick_consumer(&self, id: u64, by: &str) -> AdminResult<()> {
        self.local_kick(id, by)
    }

    async fn kick_consumer_on(&self, node: Option<&str>, id: u64, by: &str) -> AdminResult<()> {
        match node.filter(|n| *n != self.id()) {
            None => self.local_kick(id, by),
            Some(n) => {
                let token = self.node.quorum.admin_token().ok_or_else(|| {
                    AdminError::BadRequest(format!(
                        "consumer {id} is on {n}: kicking it from here needs --qlog-admin-token on the nodes"
                    ))
                })?;
                let body = kick_body(token, id, by);
                let b = self
                    .node
                    .quorum
                    .ask_member(n, "node:kick", serde_json::to_vec(&body).unwrap_or_default().into())
                    .await
                    .map_err(|e| AdminError::BadRequest(format!("{n}: {e}")))?;
                let v: serde_json::Value = serde_json::from_slice(&b).unwrap_or_default();
                match v["error"].as_str() {
                    None => Ok(()),
                    Some(e) if e.starts_with("no connected") => Err(AdminError::NotFound(format!("{n}: {e}"))),
                    Some(e) => Err(AdminError::BadRequest(format!("{n}: {e}"))),
                }
            }
        }
    }

    async fn settings(&self) -> AdminResult<admin::SettingsView> {
        self.settings.clone().ok_or_else(|| AdminError::NotFound("this node doesn't report its config".into()))
    }

    async fn settings_of(&self, node: Option<&str>) -> AdminResult<admin::SettingsView> {
        match node.filter(|n| *n != self.id()) {
            None => self.settings().await,
            Some(n) => {
                let b = self
                    .node
                    .quorum
                    .ask_member(n, "node:settings", Bytes::new())
                    .await
                    .map_err(|e| AdminError::NotFound(format!("{n}: {e}")))?;
                serde_json::from_slice(&b).map_err(|e| AdminError::Internal(anyhow::anyhow!("{n}'s settings: {e}")))
            }
        }
    }

    /// This node's numbers, with the leader's new accounts (the account
    /// gate runs on the leader, so it's the only one counting them).
    async fn policy_usage(&self) -> AdminResult<admin::PolicyUsage> {
        let mut u = self.local_usage().await?;
        let st = self.node.quorum.qnode.status();
        if let Some(l) = st.leader.filter(|l| *l != self.id())
            && let Ok(b) = self.node.quorum.ask_member(&l, "node:usage", Bytes::new()).await
            && let Ok(lu) = serde_json::from_slice::<admin::PolicyUsage>(&b)
        {
            u.new_accounts_per_min = lu.new_accounts_per_min;
        }
        Ok(u)
    }
    async fn policy_signals(&self) -> AdminResult<admin::SignalsView> {
        use crate::policy::signals::SpamRule;
        let e = &self.policy.engine;
        let spam = e.snapshot().policy.body.spam.clone();
        let now = now_ms();
        let signals = SpamRule::ALL
            .iter()
            .map(|&r| {
                let t = r.threshold(&spam);
                admin::SignalTop {
                    rule: r.name().to_string(),
                    per: if r.per_account() { "account" } else { "host" }.into(),
                    limit: t.limit,
                    window_secs: t.window_secs,
                    enabled: t.enabled(),
                    top: e
                        .signals
                        .top(r, 10, now)
                        .into_iter()
                        .map(|(key, host, estimate, lower)| admin::SignalKey { key, host, estimate, lower })
                        .collect(),
                }
            })
            .collect();
        Ok(admin::SignalsView { node: self.id().to_string(), signals })
    }

    async fn takedowns(&self) -> AdminResult<Vec<crate::policy::takedowns::TakedownEntry>> {
        Ok(self.node.serve.takedowns.list())
    }

    async fn quorum_history(&self) -> AdminResult<admin::QuorumHistory> {
        let v = self.node.quorum.view().await;
        let mut h = admin::QuorumHistory::default();
        for n in v.nodes {
            let Some(st) = n.status else {
                h.stale.push(n.node);
                continue;
            };
            for e in st["history"].as_array().into_iter().flatten() {
                h.events.push(admin::QuorumEvent {
                    node: n.node.clone(),
                    at_ms: e["at_ms"].as_i64().unwrap_or(0),
                    kind: e["kind"].as_str().unwrap_or("").to_string(),
                    epoch: e["epoch"].as_u64().unwrap_or(0),
                    from: e["from"].as_str().map(str::to_string),
                    why: e["why"].as_str().unwrap_or("").to_string(),
                });
            }
        }
        h.events.sort_by(|a, b| b.at_ms.cmp(&a.at_ms).then_with(|| a.node.cmp(&b.node)));
        Ok(h)
    }

    /// Every member's ranking, merged by host.
    async fn rejects_top(&self, q: admin::RejectTopQuery) -> AdminResult<Vec<admin::RejectTop>> {
        let limit = q.limit.unwrap_or(10).clamp(1, 500);
        let body = serde_json::json!({ "reason": q.reason, "limit": limit });
        let mut all = self.local_rejects_top(q.reason, limit);
        for (id, r) in
            self.node.quorum.ask_all("node:rejects-top", serde_json::to_vec(&body).unwrap_or_default().into()).await
        {
            if id == self.id() {
                continue;
            }
            if let Some(v) = r.ok().and_then(|b| serde_json::from_slice::<Vec<admin::RejectTop>>(&b).ok()) {
                all.extend(v);
            }
        }
        Ok(merge_rejects(all, limit))
    }

    async fn discovery(&self) -> AdminResult<admin::DiscoveryView> {
        self.node.quorum.discovery(None).await.map_err(|e| AdminError::BadRequest(format!("{e:#}")))
    }

    async fn discovery_run(&self, req: admin::DiscoveryRun, _by: &str) -> AdminResult<admin::DiscoveryView> {
        self.node.quorum.discovery(Some(req.source)).await.map_err(|e| AdminError::BadRequest(format!("{e:#}")))
    }

    async fn flush_now(&self, _by: &str) -> AdminResult<serde_json::Value> {
        self.node.quorum.flush_now().await.map_err(|e| AdminError::BadRequest(format!("{e:#}")))
    }

    /// Each member's status, asked over the peer protocol (addresses from
    /// `qlog/leader` and `--qlog-peer`); never resets its histograms.
    async fn quorum(&self) -> AdminResult<admin::QuorumView> {
        Ok(self.node.quorum.view().await)
    }

    /// Sent to the leader with the qlog admin token (`--qlog-admin-token`):
    /// the dashboard's own token only gets it this far.
    async fn change_quorum_members(
        &self,
        req: admin::QuorumMembersChange,
        _by: &str,
    ) -> AdminResult<serde_json::Value> {
        let q = &self.node.quorum;
        if req.members.is_empty() {
            return Err(AdminError::BadRequest("an empty member set".into()));
        }
        q.change_members(req).await.map_err(|e| {
            let m = format!("{e:#}");
            if m.contains("unauthorized") || m.contains("are off") {
                AdminError::BadRequest(m)
            } else {
                AdminError::Internal(e)
            }
        })
    }

    async fn cluster(&self) -> AdminResult<admin::ClusterView> {
        Ok(self.node.quorum.cluster_view().await)
    }

    async fn plc_view(&self) -> AdminResult<admin::PlcView> {
        let Some(r) = self.node.quorum.plc_report().await else {
            return Ok(admin::PlcView { enabled: self.node.quorum.plc.is_some(), ..Default::default() });
        };
        let leader = self.node.quorum.qnode.status().leader;
        let member =
            Member::ok(NodeReport { node: leader.clone().unwrap_or_default(), plc: Some(r), ..Default::default() });
        Ok(fleet::plc_view(&[member]))
    }

    async fn admissions(&self) -> AdminResult<admin::AdmissionLog> {
        let e = &self.policy.engine;
        Ok(admin::AdmissionLog {
            new_hosts_today: e.new_hosts_today().await.map_err(AdminError::Internal)?,
            new_hosts_per_day: e.snapshot().policy.body.cluster.new_hosts_per_day,
            entries: self.node.crawler.admissions(),
        })
    }

    async fn tail(&self, q: admin::TailQuery) -> AdminResult<Vec<admin::TailFrame>> {
        Ok(tail_frames(&self.node, &q))
    }

    async fn release_throttled(&self, host: &str, by: &str) -> AdminResult<admin::Released> {
        let _acting = self.acting(host);
        let released = self.node.quorum.release_throttled(host).await.map_err(AdminError::Internal)?;
        tracing::info!(target: "vlrelay::audit", host, by, released, "released throttled accounts");
        if released > 0 {
            // the leader's count is in its table: read it now, so the row
            // and its event show the release rather than the next poll's
            self.node.quorum.refresh_table().await;
            if let Ok(mut row) = self.host_row(host) {
                if let Some(o) = self.node.quorum.hosts.owners().get(host) {
                    row.node = o.clone();
                }
                self.host_acted(&mut row);
            }
        }
        Ok(admin::Released { released })
    }

    async fn store(&self) -> AdminResult<admin::StoreView> {
        let retention = {
            let mut r = self.retain.lock().await;
            match &*r {
                Some((at, v)) if at.elapsed() < RETAIN_TTL => v.clone(),
                _ => {
                    let v = match crate::qlog::retain::read_report(&self.node.quorum.qnode.bucket().retain).await {
                        Ok(v) => v,
                        Err(e) => {
                            tracing::debug!("reading retain/qlog: {e:#}");
                            None
                        }
                    };
                    *r = Some((Instant::now(), v.clone()));
                    v
                }
            }
        };
        let now = crate::qlog::bucket::requests();
        let (window, prev) = {
            let mut p = self.store_prev.lock();
            let out = p.as_ref().map(|(at, r)| (at.elapsed(), r.clone()));
            if p.as_ref().is_none_or(|(at, _)| at.elapsed() >= STORE_WINDOW) {
                *p = Some((Instant::now(), now.clone()));
            }
            out.map_or((Duration::ZERO, None), |(w, r)| (w, Some(r)))
        };
        Ok(store_view(self.id(), &now, prev.as_ref(), window, retention))
    }

    async fn pipeline_view(&self) -> AdminResult<admin::PipelineView> {
        Ok(fleet::pipeline_view(&self.members().await, 50))
    }

    async fn accounts(&self, q: admin::AccountQuery) -> AdminResult<Vec<admin::Account>> {
        let Some(q) = q.q.filter(|s| !s.is_empty()) else {
            return Ok(Vec::new());
        };
        if q.starts_with("did:") {
            return match self.account(&q).await {
                Ok(a) => Ok(vec![a]),
                Err(AdminError::NotFound(_)) => Ok(Vec::new()),
                Err(e) => Err(e),
            };
        }
        self.local_accounts(&q).await
    }

    async fn account(&self, did: &str) -> AdminResult<admin::Account> {
        self.account_view(did).await
    }

    async fn takedown(&self, did: &str, reason: String, by: &str) -> AdminResult<admin::Account> {
        self.set_takedown(did, true, by, &reason).await
    }

    async fn untakedown(&self, did: &str, by: &str) -> AdminResult<admin::Account> {
        self.set_takedown(did, false, by, "").await
    }

    async fn cases(&self, q: admin::CaseQuery) -> AdminResult<Vec<admin::Case>> {
        self.admin().cases(q).await
    }
    async fn case(&self, id: u64) -> AdminResult<admin::Case> {
        self.admin().case(id).await
    }
    async fn update_case(&self, id: u64, update: admin::CaseUpdate, by: &str) -> AdminResult<admin::Case> {
        let c = self.admin().update_case(id, update, by).await?;
        self.case_changed(&c);
        Ok(c)
    }
    async fn bulk_update_cases(&self, b: admin::CaseBulkUpdate, by: &str) -> AdminResult<admin::CaseBulkResult> {
        let all = self.admin().cases(admin::CaseQuery::default()).await?;
        let (targets, update) = admin::bulk_targets(&all, &b)?;
        let mut ids = Vec::with_capacity(targets.len());
        for id in targets {
            let c = match self.admin().update_case(id, update.clone(), by).await {
                Ok(c) => c,
                Err(AdminError::NotFound(_)) => continue,
                Err(e) => return Err(e),
            };
            // coalesced: past the feed's bound they go out as one `*`
            let hint = serde_json::json!({ "status": c.status });
            self.feed.touch(ChangeKind::Case, c.id.to_string(), Some(hint), true);
            ids.push(c.id);
        }
        Ok(admin::CaseBulkResult { updated: ids.len(), ids })
    }
}

fn full_doc(d: &crate::policy::Stored<crate::policy::PolicyBody>) -> admin::FullPolicyDoc {
    admin::FullPolicyDoc {
        version: d.version,
        updated_at_ms: d.updated_at_ms,
        updated_by: d.updated_by.clone(),
        note: d.note.clone(),
        policy: serde_json::to_value(&d.body).unwrap_or_default(),
    }
}

/// What a frame the leader rejected counts as in the tail: `held` when the
/// relay keeps the account back (throttled past its host's cap, or a new
/// one deferred), else `reject`.
fn tail_kind(reason: &str, detail: &str) -> &'static str {
    let throttled = reason == "inactive" && detail.to_ascii_lowercase().contains("throttled");
    if throttled || reason == "new_account_deferred" { "held" } else { "reject" }
}

pub(crate) fn tail_frames(node: &Node, q: &admin::TailQuery) -> Vec<admin::TailFrame> {
    let host = q.host.as_deref().filter(|h| !h.is_empty());
    let since = q.since_ms.unwrap_or(i64::MIN);
    let limit = q.limit.unwrap_or(200).clamp(1, 2000);
    let mut out = Vec::new();
    if q.rejects.unwrap_or(0) != 0 {
        let r = node.rejects.lock();
        for (h, x) in r.iter() {
            if host.is_some_and(|w| w != h.0) {
                continue;
            }
            for n in x.recent.iter().filter(|n| n.at_ms > since) {
                out.push(admin::TailFrame {
                    at_ms: n.at_ms,
                    host: h.0.clone(),
                    did: n.did.clone(),
                    kind: tail_kind(n.reason, &n.detail).into(),
                    reason: Some(n.reason.to_string()),
                    detail: Some(n.detail.clone()),
                    upstream_seq: Some(n.upstream_seq),
                    seq: None,
                    event: None,
                });
            }
        }
    }
    if let Some(h) = host {
        let p = node.passed.lock();
        for n in p.iter().rev().filter(|n| n.host.0 == h && n.at_ms > since).take(limit) {
            out.push(admin::TailFrame {
                at_ms: n.at_ms,
                host: n.host.0.clone(),
                did: n.did.clone(),
                kind: "passed".into(),
                reason: None,
                detail: None,
                upstream_seq: Some(n.upstream_seq),
                seq: Some(n.seq),
                event: Some(n.kind.to_string()),
            });
        }
    }
    out.sort_by(|a, b| b.at_ms.cmp(&a.at_ms).then_with(|| b.seq.cmp(&a.seq)));
    out.truncate(limit);
    out
}

fn class_counts(c: &crate::qlog::bucket::Counts) -> admin::ClassCounts {
    admin::ClassCounts { a: c.a as f64, b: c.b as f64, free: c.free as f64 }
}

fn class_rate(
    now: &crate::qlog::bucket::Counts,
    prev: Option<&crate::qlog::bucket::Counts>,
    secs: f64,
) -> admin::ClassCounts {
    let Some(p) = prev.filter(|_| secs > 0.0) else { return admin::ClassCounts::default() };
    let r = |a: u64, b: u64| a.saturating_sub(b) as f64 / secs;
    admin::ClassCounts { a: r(now.a, p.a), b: r(now.b, p.b), free: r(now.free, p.free) }
}

/// Payload bytes (up, down) by the quorum log's client purpose.
fn store_bytes() -> HashMap<String, (u64, u64)> {
    use prometheus::core::Collector;
    let mut out: HashMap<String, (u64, u64)> = HashMap::new();
    for mf in vlpds::metrics::OBJ_BYTES.collect() {
        for m in mf.get_metric() {
            let label = |k: &str| m.get_label().iter().find(|l| l.name() == k).map(|l| l.value().to_string());
            let (Some(dir), Some(client)) = (label("dir"), label("client")) else { continue };
            let Some(purpose) = client.strip_prefix("qlog_") else { continue };
            let n = m.get_counter().get_value() as u64;
            let e = out.entry(purpose.to_string()).or_default();
            if dir == "up" {
                e.0 += n;
            } else {
                e.1 += n;
            }
        }
    }
    out
}

/// Latency by op over every key component, from the request histogram.
fn store_latency() -> Vec<admin::StoreLatency> {
    use prometheus::core::Collector;
    // (count, sum s, [(upper bound s, cumulative count)])
    type Hist = (u64, f64, Vec<(f64, u64)>);
    let mut by: BTreeMap<String, Hist> = BTreeMap::new();
    for mf in vlpds::metrics::OBJ_DURATION.collect() {
        for m in mf.get_metric() {
            let Some(op) = m.get_label().iter().find(|l| l.name() == "op").map(|l| l.value().to_string()) else {
                continue;
            };
            let h = m.get_histogram();
            let e = by.entry(op).or_default();
            e.0 += h.get_sample_count();
            e.1 += h.get_sample_sum();
            for (i, b) in h.get_bucket().iter().enumerate() {
                match e.2.get_mut(i) {
                    Some(x) => x.1 += b.cumulative_count(),
                    None => e.2.push((b.upper_bound(), b.cumulative_count())),
                }
            }
        }
    }
    by.into_iter()
        .filter(|(_, (n, _, _))| *n > 0)
        .map(|(op, (n, sum, buckets))| {
            let q = |p: f64| bucket_quantile(&buckets, n, p) * 1000.0;
            admin::StoreLatency { op, count: n, mean_ms: sum / n as f64 * 1000.0, p50_ms: q(0.5), p99_ms: q(0.99) }
        })
        .collect()
}

/// The `p` quantile of a histogram (`(upper bound, cumulative count)`,
/// ascending) of `n` samples, interpolated linearly within its bucket as
/// Prometheus's `histogram_quantile` does; past the last finite bound, that
/// bound.
fn bucket_quantile(buckets: &[(f64, u64)], n: u64, p: f64) -> f64 {
    let want = n as f64 * p;
    let mut lower = (0.0, 0u64);
    for &(ub, c) in buckets {
        if c as f64 >= want {
            if !ub.is_finite() {
                return lower.0;
            }
            let span = (c - lower.1) as f64;
            let into = if span > 0.0 { (want - lower.1 as f64) / span } else { 1.0 };
            return lower.0 + (ub - lower.0) * into.clamp(0.0, 1.0);
        }
        lower = (ub, c);
    }
    lower.0
}

pub(crate) fn store_view(
    node: &str,
    now: &crate::qlog::bucket::Requests,
    prev: Option<&crate::qlog::bucket::Requests>,
    window: Duration,
    retention: Option<serde_json::Value>,
) -> admin::StoreView {
    let secs = window.as_secs_f64();
    let bytes = store_bytes();
    let purposes: Vec<admin::StorePurpose> = now
        .by_purpose
        .iter()
        .map(|(p, c)| {
            let (up, down) = bytes.get(p).copied().unwrap_or_default();
            admin::StorePurpose {
                purpose: p.clone(),
                requests: class_counts(c),
                per_sec: class_rate(c, prev.and_then(|r| r.by_purpose.get(p)), secs),
                bytes_up: up,
                bytes_down: down,
            }
        })
        .collect();
    let total = admin::StorePurpose {
        purpose: "total".into(),
        requests: class_counts(&now.total),
        per_sec: class_rate(&now.total, prev.map(|r| &r.total), secs),
        bytes_up: purposes.iter().map(|p| p.bytes_up).sum(),
        bytes_down: purposes.iter().map(|p| p.bytes_down).sum(),
    };
    admin::StoreView {
        node: node.to_string(),
        at_ms: now_ms(),
        window_secs: secs,
        total,
        purposes,
        latency: store_latency(),
        retention,
    }
}

impl crate::node::quorum::LocalAsk for NodeAdmin {
    fn answer<'a>(&'a self, topic: &'a str, body: Bytes) -> futures::future::BoxFuture<'a, Option<Bytes>> {
        Box::pin(async move {
            let v = match topic {
                "node:consumers" => serde_json::to_value(self.local_consumers()).ok()?,
                "node:changes" => self.answer_pull(&body)?,
                "node:settings" => serde_json::to_value(self.settings.as_ref()?).ok()?,
                "node:usage" => serde_json::to_value(self.local_usage().await.ok()?).ok()?,
                "node:rejects-top" => {
                    let q: admin::RejectTopQuery = serde_json::from_slice(&body).ok()?;
                    serde_json::to_value(self.local_rejects_top(q.reason, q.limit.unwrap_or(10))).ok()?
                }
                "node:kick" => match kick_request(&body, self.node.quorum.admin_token()) {
                    Err(e) => serde_json::json!({"error": e}),
                    Ok((id, by)) => match self.local_kick(id, &by) {
                        Ok(()) => serde_json::json!({}),
                        Err(e) => serde_json::json!({"error": e.to_string()}),
                    },
                },
                _ => return None,
            };
            serde_json::to_vec(&v).ok().map(Bytes::from)
        })
    }
}

/// A kick sent on to the member serving the consumer. `by` is the audit
/// label ([`admin::Actor::label`]), so an operator a proxy named stays named
/// there; the qlog admin token is what makes the member believe it.
fn kick_body(token: &str, id: u64, by: &str) -> serde_json::Value {
    serde_json::json!({ "token": token, "id": id, "by": by })
}

/// The member's side of [`kick_body`]: the consumer and the label, only
/// with this node's qlog admin token.
fn kick_request(body: &[u8], token: Option<&str>) -> Result<(u64, String), &'static str> {
    let req: serde_json::Value = serde_json::from_slice(body).map_err(|_| "bad request")?;
    let given = req["token"].as_str().unwrap_or("");
    if !token.is_some_and(|t| vlpds::auth::token_eq(t, given)) {
        return Err("unauthorized");
    }
    let id = req["id"].as_u64().ok_or("bad request")?;
    let by = req["by"].as_str().filter(|b| !b.is_empty()).unwrap_or("admin (token)");
    Ok((id, by.to_string()))
}

/// Rejects older than this don't count toward a host's top reason.
const TOP_REASON_WINDOW_MS: i64 = 5 * 60_000;

/// The reason with the most of a host's recent rejects (its last 50, in
/// the last five minutes).
fn top_reason(r: &super::metrics::HostRejects) -> Option<RejectReason> {
    let since = now_ms() - TOP_REASON_WINDOW_MS;
    let mut by: BTreeMap<RejectReason, usize> = BTreeMap::new();
    for n in r.recent.iter().filter(|n| n.at_ms >= since) {
        *by.entry(reject_class(n.reason)).or_default() += 1;
    }
    by.into_iter().max_by_key(|(_, n)| *n).map(|(c, _)| c)
}

/// Most rejects a second first, then most in all.
fn rank_rejects(v: &mut Vec<admin::RejectTop>, limit: usize) {
    v.sort_by(|a, b| {
        b.rejects_per_sec.total_cmp(&a.rejects_per_sec).then(b.total.cmp(&a.total)).then(a.host.cmp(&b.host))
    });
    v.truncate(limit);
}

/// Members' rankings as one: a host read by two members over time (it
/// moved) sums, keeping the newest sample.
pub(crate) fn merge_rejects(all: Vec<admin::RejectTop>, limit: usize) -> Vec<admin::RejectTop> {
    let mut by: BTreeMap<String, admin::RejectTop> = BTreeMap::new();
    for t in all {
        match by.get_mut(&t.host) {
            None => {
                by.insert(t.host.clone(), t);
            }
            Some(e) => {
                e.rejects_per_sec += t.rejects_per_sec;
                e.total += t.total;
                if t.last_at_ms > e.last_at_ms {
                    e.last_at_ms = t.last_at_ms;
                    e.sample = t.sample;
                }
            }
        }
    }
    let mut v: Vec<admin::RejectTop> = by.into_values().collect();
    rank_rejects(&mut v, limit);
    v
}

#[cfg(test)]
mod tests {
    use super::*;

    fn view(tier: Tier, st: HostStatus, bp: Option<Backpressure>) -> HostView {
        let mut record = crate::upstream::HostRecord::new(&Host("pds.example.com".into()), tier);
        record.status = st;
        HostView {
            record,
            received_seq: None,
            frames: 0,
            bytes: 0,
            connects: 1,
            read_lag_ms: None,
            host_lag_ms: None,
            backpressure_at_ms: None,
            pace: 1.0,
            inflight_events: 0,
            inflight_bytes: 0,
            paused: false,
            backpressure: bp,
        }
    }

    fn row(host: &str, status: admin::HostStatus, reason: Option<admin::BackpressureReason>) -> admin::HostRow {
        admin::HostRow {
            host: host.into(),
            tier: "trusted".into(),
            status,
            backpressure_reason: reason,
            events_per_sec: 1.0,
            error_rate: 0.0,
            accounts: 1,
            last_upstream_seq: 1,
            connected_since_ms: None,
            lag_ms: 120_000.0,
            catch_up_pace: None,
            throttle: None,
            rule: None,
            node: "n1".into(),
            max_accounts: 0,
            history: Vec::new(),
            throttled_accounts: 0,
            source: None,
            top_reason: None,
            version: None,
            updated_at_ms: None,
            owner_version: None,
            pending: false,
        }
    }

    #[test]
    fn store_latency_quantiles_interpolate_within_their_bucket() {
        let b = [(0.01, 0), (0.1, 50), (1.0, 100), (f64::INFINITY, 100)];
        assert!((bucket_quantile(&b, 100, 0.5) - 0.1).abs() < 1e-9);
        assert!((bucket_quantile(&b, 100, 0.25) - 0.055).abs() < 1e-9);
        assert!((bucket_quantile(&b, 100, 0.99) - 0.982).abs() < 1e-9);
        let tail = [(0.1, 10), (f64::INFINITY, 20)];
        assert_eq!(bucket_quantile(&tail, 20, 0.99), 0.1);
    }

    #[test]
    fn relay_backpressure_is_its_own_status_with_a_reason() {
        let bp = view(Tier::Trusted, HostStatus::Backpressure, Some(Backpressure::QueueFull));
        assert_eq!(status(&bp), admin::HostStatus::Backpressure);
        assert_eq!(backpressure_reason(&bp), Some(admin::BackpressureReason::QueueFull));
        assert_eq!(host_status_label(&bp), "backpressure");
        let inflight = view(Tier::Default, HostStatus::Backpressure, Some(Backpressure::InflightFull));
        assert_eq!(backpressure_reason(&inflight), Some(admin::BackpressureReason::InflightFull));
        let node = view(Tier::Default, HostStatus::Backpressure, Some(Backpressure::NodeInflightFull));
        assert_eq!(backpressure_reason(&node), Some(admin::BackpressureReason::NodeInflightFull));

        // the limiter's pause (tier, rule or operator limits) stays throttled, with no reason
        let thr = view(Tier::Throttled, HostStatus::Throttled, None);
        assert_eq!((status(&thr), backpressure_reason(&thr)), (admin::HostStatus::Throttled, None));
        assert_eq!(host_status_label(&thr), "throttled");
        let active = view(Tier::Throttled, HostStatus::Active, None);
        assert_eq!((status(&active), backpressure_reason(&active)), (admin::HostStatus::Connected, None));
        // a ban outranks whatever the reader was last doing
        let banned = view(Tier::Banned, HostStatus::Backpressure, Some(Backpressure::QueueFull));
        assert_eq!((status(&banned), backpressure_reason(&banned)), (admin::HostStatus::Banned, None));
    }

    #[test]
    fn host_rows_carry_and_filter_backpressure() {
        let v = serde_json::to_value(row(
            "a.example.com",
            admin::HostStatus::Backpressure,
            Some(admin::BackpressureReason::QueueFull),
        ))
        .unwrap();
        assert_eq!((&v["status"], &v["backpressureReason"]), (&"backpressure".into(), &"queue_full".into()));
        let v = serde_json::to_value(row("b.example.com", admin::HostStatus::Throttled, None)).unwrap();
        assert_eq!((&v["status"], &v["backpressureReason"]), (&"throttled".into(), &serde_json::Value::Null));

        let rows = vec![
            row("a.example.com", admin::HostStatus::Backpressure, Some(admin::BackpressureReason::InflightFull)),
            row("b.example.com", admin::HostStatus::Throttled, None),
            row("c.example.com", admin::HostStatus::Connected, None),
            row("d.example.com", admin::HostStatus::Idle, None),
        ];
        let q: admin::HostQuery = serde_json::from_value(serde_json::json!({"status": "backpressure"})).unwrap();
        let hosts = |l: admin::HostList| l.hosts.into_iter().map(|r| r.host).collect::<Vec<_>>();
        assert_eq!(hosts(NodeAdmin::sort_page(rows.clone(), &q)), ["a.example.com"]);
        let q: admin::HostQuery = serde_json::from_value(serde_json::json!({"status": "throttled"})).unwrap();
        assert_eq!(hosts(NodeAdmin::sort_page(rows.clone(), &q)), ["b.example.com"]);
        // a host the relay pauses is still live, so it can be lagging
        let q: admin::HostQuery = serde_json::from_value(serde_json::json!({"flag": "lagging"})).unwrap();
        assert_eq!(hosts(NodeAdmin::sort_page(rows, &q)), ["a.example.com", "b.example.com", "c.example.com"]);
    }

    #[test]
    fn throttled_and_deferred_accounts_are_held_in_the_tail() {
        assert_eq!(tail_kind("inactive", "account is Throttled"), "held");
        assert_eq!(tail_kind("new_account_deferred", ""), "held");
        assert_eq!(tail_kind("inactive", "account is Deactivated"), "reject");
        assert_eq!(tail_kind("prev_data_mismatch", ""), "reject");
    }

    #[test]
    fn store_rates_are_over_the_window_and_sum_to_the_total() {
        use crate::qlog::bucket::{Counts, Requests};
        let req = |a: u64, b: u64| {
            let mut r = Requests::default();
            r.by_purpose.insert("flush".into(), Counts { a, b: 0, free: 0 });
            r.by_purpose.insert("state".into(), Counts { a: 0, b, free: 1 });
            r.total = Counts { a, b, free: 1 };
            r
        };
        let (prev, now) = (req(10, 100), req(30, 160));
        let v = store_view("n1", &now, Some(&prev), Duration::from_secs(10), None);
        assert_eq!((v.total.per_sec.a, v.total.per_sec.b), (2.0, 6.0));
        let flush = v.purposes.iter().find(|p| p.purpose == "flush").unwrap();
        assert_eq!((flush.requests.a, flush.per_sec.a), (30.0, 2.0));
        assert_eq!(v.purposes.iter().map(|p| p.per_sec.b).sum::<f64>(), v.total.per_sec.b);
        let first = store_view("n1", &now, None, Duration::ZERO, None);
        assert_eq!(first.total.per_sec.a, 0.0, "no rate without a previous sample");
    }

    #[test]
    fn a_hosts_top_reason_is_its_most_frequent_recent_one() {
        use super::super::metrics::{HostRejects, RejectNote};
        let note = |reason: &'static str, ago_ms: i64| RejectNote {
            at_ms: now_ms() - ago_ms,
            did: "did:plc:x".into(),
            reason,
            upstream_seq: 1,
            detail: String::new(),
        };
        let mut r = HostRejects::default();
        // many old ones don't count, only the last five minutes'
        for _ in 0..10 {
            r.recent.push_back(note("bad_signature", 10 * 60_000));
        }
        for _ in 0..2 {
            r.recent.push_back(note("prev_data_mismatch", 1_000));
        }
        r.recent.push_back(note("bad_signature", 2_000));
        assert_eq!(top_reason(&r), Some(RejectReason::PrevDataMismatch));
        assert_eq!(top_reason(&HostRejects::default()), None);
    }

    #[test]
    fn members_rankings_merge_by_host() {
        let t = |h: &str, r: f64, n: u64, at: i64| admin::RejectTop {
            host: h.into(),
            rejects_per_sec: r,
            total: n,
            last_at_ms: Some(at),
            sample: None,
        };
        let v = merge_rejects(vec![t("a", 1.0, 10, 5), t("b", 3.0, 7, 1), t("a", 2.5, 4, 9), t("c", 0.0, 99, 2)], 2);
        assert_eq!(v.len(), 2);
        assert_eq!((v[0].host.as_str(), v[0].rejects_per_sec, v[0].total, v[0].last_at_ms), ("a", 3.5, 14, Some(9)));
        assert_eq!(v[1].host, "b");
    }

    #[test]
    fn a_kick_sent_on_keeps_the_operator_only_with_the_qlog_token() {
        let by = admin::Actor::Operator("alice@example.com".into()).label();
        let body = serde_json::to_vec(&kick_body("qlog-secret", 7, &by)).unwrap();
        assert_eq!(kick_request(&body, Some("qlog-secret")), Ok((7, "alice@example.com (proxy)".into())));
        // without the token the label means nothing, as for any peer ask
        assert_eq!(kick_request(&body, Some("other")), Err("unauthorized"));
        assert_eq!(kick_request(&body, None), Err("unauthorized"));
        let forged = serde_json::to_vec(&serde_json::json!({"id": 7, "by": by})).unwrap();
        assert_eq!(kick_request(&forged, Some("qlog-secret")), Err("unauthorized"));
        let bare = serde_json::to_vec(&serde_json::json!({"token": "qlog-secret", "id": 7})).unwrap();
        assert_eq!(kick_request(&bare, Some("qlog-secret")), Ok((7, "admin (token)".into())));
    }

    #[test]
    fn inactive_rejects_are_not_takedowns() {
        // a policy-throttled or deactivated account's commits come back as
        // "inactive"; calling them takedowns sent operators looking for one
        assert_eq!(reject_class("inactive"), RejectReason::Inactive);
        assert_eq!(serde_json::to_value(RejectReason::Inactive).unwrap(), "inactive");
    }
}
