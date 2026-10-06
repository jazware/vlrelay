//! The operator API over the node's real state: hosts and their actions,
//! consumers, the overview's numbers and account lookups and takedowns.
//! Policy, domain rules, cases and host tier actions go to the policy
//! engine's admin half (`node::policy`). The cluster and Quorum views are
//! the quorum log's members (`node::quorum::Glue`).
//!
//! Numbers are this node's own: the hosts it reads, its consumers, its
//! rates. Other members' rates and hosts show in the cluster view (from
//! their statuses); their consumers, host details and host actions are on
//! their own dashboards. Accounts are the leader's records, so an account
//! lookup or a takedown answers on any node only through the leader's log
//! (takedowns) or on the leader itself (lookups).

use super::Node;
use super::metrics::HostSeries as Series;
use super::policy::PolicyHooks;
use crate::admin::fleet::{self, Member, NodeReport};
use crate::admin::{self, AdminError, AdminResult, AdminSource, RejectReason, demo::Demo};
use crate::policy::admin::PolicyAdmin;
use crate::state::{AccountStatus, Upstream};
use crate::types::Host;
use crate::upstream::{HostStatus, HostView, Tier};
use parking_lot::Mutex;
use std::collections::BTreeMap;
use std::sync::Arc;
use std::time::{Duration, Instant};

pub struct NodeAdmin {
    pub node: Arc<Node>,
    pub policy: Arc<PolicyHooks>,
    pub demo: Arc<Demo>,
    /// (when, open cases): the overview polls every second or two, and a
    /// count is a bucket listing.
    open_cases: Mutex<Option<(Instant, u32)>>,
    cpu: Mutex<Option<(Instant, f64, f64)>>,
    settings: Option<admin::SettingsView>,
}

const OPEN_CASES_TTL: Duration = Duration::from_secs(10);

fn now_ms() -> i64 {
    crate::upstream::host::now_ms() as i64
}

/// CPU seconds this process has used (user + system).
fn cpu_seconds() -> f64 {
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
    pub fn new(node: Arc<Node>, policy: Arc<PolicyHooks>, demo: Arc<Demo>) -> NodeAdmin {
        NodeAdmin { node, policy, demo, open_cases: Mutex::new(None), cpu: Mutex::new(None), settings: None }
    }

    /// The process's effective config, for the Settings page.
    pub fn with_settings(mut self, s: admin::SettingsView) -> NodeAdmin {
        self.settings = Some(s);
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
    }
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

impl NodeAdmin {
    fn row(&self, h: &HostView, series: Option<&Series>, rejects: u64) -> admin::HostRow {
        let (rate, ratio) = series.map_or((0.0, 0.0), |s| (s.rate(), s.reject_ratio()));
        let _ = rejects;
        admin::HostRow {
            host: h.record.hostname.clone(),
            tier: h.record.tier.as_str().into(),
            status: status(h),
            events_per_sec: rate,
            error_rate: ratio,
            accounts: self.policy.accounts(&h.record.hostname).unwrap_or(h.record.account_count),
            last_upstream_seq: h.received_seq.unwrap_or(0),
            connected_since_ms: (h.record.status == HostStatus::Active)
                .then_some(h.record.last_connected_ms.map(|m| m as i64))
                .flatten(),
            lag_ms: h.read_lag_ms.unwrap_or(0) as f64,
            throttle: self.policy.throttle(&h.record.hostname),
            rule: self.policy.limits(&h.record.hostname).and_then(|l| l.rule),
            node: self.node.cfg.node_id.clone(),
            max_accounts: self.policy.limits(&h.record.hostname).and_then(|l| l.limits).map_or(0, |l| l.max_accounts),
        }
    }

    /// Every host in this node's registry (every host the leader's table
    /// has; only the ones this node reads have live numbers).
    fn rows(&self) -> Vec<admin::HostRow> {
        let hosts = self.node.manager.hosts();
        let dash = self.node.dash.lock();
        let rejects = self.node.rejects.lock();
        hosts
            .iter()
            .map(|h| {
                let k = Host(h.record.hostname.clone());
                self.row(h, dash.hosts.get(&k), rejects.get(&k).map_or(0, |r| r.total))
            })
            .collect()
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
        let dash = self.node.dash.lock();
        let rejects = self.node.rejects.lock();
        Ok(self.row(&h, dash.hosts.get(&k), rejects.get(&k).map_or(0, |r| r.total)))
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
        self.policy.engine.takedowns.record(did, takedown, by, reason).await?;
        self.node.serve.takedowns.apply_local(did, takedown);
        match self.node.quorum.takedown(did, takedown).await {
            Ok(_) => {}
            Err(e) if format!("{e}").starts_with("no_account") => {
                return Err(AdminError::NotFound(format!("no account {did}")));
            }
            Err(e) => return Err(AdminError::Internal(e)),
        }
        self.account_view(did).await
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
            log_durability_lag_ms: last.durable_lag_ms,
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
                let paused = r.status == admin::HostStatus::Throttled;
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
            })
            .collect()
    }

    pub fn local_kick(&self, id: u64, by: &str) -> AdminResult<()> {
        if !self.node.serve.kick(id) {
            return Err(AdminError::NotFound(format!("no connected consumer {id}")));
        }
        tracing::info!(target: "vlrelay::audit", consumer = id, by, "consumer kicked");
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

    fn sort_page(mut rows: Vec<admin::HostRow>, q: &admin::HostQuery) -> admin::HostList {
        rows.retain(|r| q.q.as_deref().is_none_or(|s| r.host.contains(s)));
        rows.retain(|r| q.tier.as_deref().is_none_or(|t| r.tier == t));
        rows.retain(|r| q.status.is_none_or(|s| r.status == s));
        match q.sort.as_deref() {
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
        match action {
            admin::HostAction::Reconnect if !self.node.manager.is_running(&k) => {
                let owner = self.node.quorum.hosts.owners().get(host).cloned().unwrap_or_else(|| "nobody yet".into());
                return Err(AdminError::BadRequest(format!("{host} is read by {owner}: reconnect it there")));
            }
            admin::HostAction::Reconnect => self.node.manager.kick(&k),
            a => {
                self.admin().host_action(host, a, by).await?;
                // the socket follows now, not at the sync loop's next pass
                self.policy.refresh_host(host).await?;
            }
        }
        self.host_row(host)
    }

    async fn domain_rules(&self) -> AdminResult<Vec<admin::DomainRule>> {
        self.admin().domain_rules().await
    }
    async fn create_domain_rule(&self, rule: admin::DomainRuleInput, by: &str) -> AdminResult<admin::DomainRule> {
        self.admin().create_domain_rule(rule, by).await
    }
    async fn update_domain_rule(
        &self,
        id: u64,
        rule: admin::DomainRuleInput,
        by: &str,
    ) -> AdminResult<admin::DomainRule> {
        self.admin().update_domain_rule(id, rule, by).await
    }
    async fn delete_domain_rule(&self, id: u64, by: &str) -> AdminResult<()> {
        self.admin().delete_domain_rule(id, by).await
    }
    async fn policy(&self) -> AdminResult<admin::PolicyDoc> {
        self.admin().policy().await
    }
    async fn update_policy(&self, update: admin::PolicyUpdate, by: &str) -> AdminResult<admin::PolicyDoc> {
        self.admin().update_policy(update, by).await
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
    async fn consumers(&self) -> AdminResult<Vec<admin::Consumer>> {
        Ok(self.local_consumers())
    }

    async fn kick_consumer(&self, id: u64, by: &str) -> AdminResult<()> {
        self.local_kick(id, by)
    }

    async fn kick_consumer_on(&self, node: Option<&str>, id: u64, by: &str) -> AdminResult<()> {
        match node.filter(|n| *n != self.id()) {
            None => self.local_kick(id, by),
            Some(n) => Err(AdminError::BadRequest(format!("consumer {id} is on {n}: kick it from {n}'s dashboard"))),
        }
    }

    async fn settings(&self) -> AdminResult<admin::SettingsView> {
        self.settings.clone().ok_or_else(|| AdminError::NotFound("this node doesn't report its config".into()))
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
        Ok(fleet::plc_view(&self.members().await))
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
        self.admin().update_case(id, update, by).await
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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn inactive_rejects_are_not_takedowns() {
        // a policy-throttled or deactivated account's commits come back as
        // "inactive"; calling them takedowns sent operators looking for one
        assert_eq!(reject_class("inactive"), RejectReason::Inactive);
        assert_eq!(serde_json::to_value(RejectReason::Inactive).unwrap(), "inactive");
    }
}
