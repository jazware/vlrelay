//! The operator API over the node's real state: hosts and their actions,
//! consumers, the overview's numbers and account lookups and takedowns.
//! Domain rules, policy, cases and the cluster view still come from the
//! simulation until the policy and cluster workstreams provide them.

use super::Node;
use super::metrics::HostSeries as Series;
use crate::admin::{self, AdminError, AdminResult, AdminSource, RejectReason, demo::Demo};
use crate::seq::EventMeta;
use crate::state::{AccountStatus, Upstream};
use crate::types::Host;
use crate::upstream::{HostStatus, HostView, Tier};
use parking_lot::Mutex;
use std::collections::{BTreeMap, HashMap};
use std::net::IpAddr;
use std::sync::Arc;

pub struct NodeAdmin {
    pub node: Arc<Node>,
    pub demo: Arc<Demo>,
}

/// subscribeRepos clients by address, as the route saw them connect. The
/// firehose's per-address count says which are still connected.
#[derive(Default)]
pub struct Consumers {
    seen: Mutex<HashMap<IpAddr, Seen>>,
}

struct Seen {
    id: u64,
    user_agent: String,
    since_ms: i64,
    cursor: Option<i64>,
}

impl Consumers {
    pub fn connected(&self, ip: IpAddr, user_agent: &str, cursor: Option<i64>, live: usize) {
        let mut m = self.seen.lock();
        let n = m.len() as u64;
        let now = crate::upstream::host::now_ms() as i64;
        let e = m.entry(ip).or_insert_with(|| Seen { id: n + 1, user_agent: String::new(), since_ms: now, cursor });
        // a fresh streak when nothing from this address was connected
        if live == 0 {
            e.since_ms = now;
            e.cursor = cursor;
        }
        e.user_agent = user_agent.chars().take(200).collect();
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
        "inactive" => RejectReason::Takendown,
        "rate_limited" => RejectReason::RateLimited,
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
            accounts: h.record.account_count,
            last_upstream_seq: h.received_seq.unwrap_or(0),
            connected_since_ms: (h.record.status == HostStatus::Active)
                .then_some(h.record.last_connected_ms.map(|m| m as i64))
                .flatten(),
            lag_ms: 0.0,
            throttle: None,
            rule: None,
            node: self.node.cfg.node_id.clone(),
        }
    }

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

    fn host_row(&self, host: &str) -> AdminResult<admin::HostRow> {
        let k = Host(host.to_string());
        let h = self.node.manager.host(&k).ok_or_else(|| AdminError::NotFound(format!("unknown host {host}")))?;
        let dash = self.node.dash.lock();
        let rejects = self.node.rejects.lock();
        Ok(self.row(&h, dash.hosts.get(&k), rejects.get(&k).map_or(0, |r| r.total)))
    }

    async fn account_view(&self, did: &str) -> AdminResult<admin::Account> {
        let rec = self
            .node
            .state
            .get(did)
            .await
            .map_err(|e| AdminError::Internal(anyhow::anyhow!("{e}")))?
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
        let shard = self.node.state.shard_id_of_slot(vlpds::slots::slot_of(did));
        Ok(admin::Account {
            did: did.to_string(),
            handle,
            host: self.node.state.host_name(rec.host).map(|h| h.to_string()).unwrap_or_default(),
            status: status_str(status).into(),
            upstream_status: upstream.into(),
            takedown: rec.relay_takedown.then(|| admin::Takedown {
                at_ms: 0,
                by: "admin".into(),
                reason: String::new(),
            }),
            rev: rec.chain.map(|c| c.rev.to_string()).unwrap_or_default(),
            last_seq: 0,
            last_event_ms: 0,
            events_last_hour: 0,
            rejects_last_hour: 0,
            did_shard: shard.0,
            node: self.node.cfg.node_id.clone(),
        })
    }

    /// Writes the takedown flag, then announces the new status on the
    /// firehose with an `#account` the relay makes itself.
    async fn set_takedown(&self, did: &str, takedown: bool) -> AdminResult<admin::Account> {
        let st = self
            .node
            .state
            .set_relay_takedown(did, takedown)
            .await
            .map_err(|e| AdminError::Internal(anyhow::anyhow!("{e}")))?
            .ok_or_else(|| AdminError::NotFound(format!("no account {did}")))?;
        let host = match self.node.state.get(did).await {
            Ok(Some(r)) => self.node.state.host_name(r.host).map(|h| h.to_string()).unwrap_or_default(),
            _ => String::new(),
        };
        let frame = vlpds::events::account_frame(did, st.is_active(), st.as_str(), &vlpds::events::now_rfc3339());
        let shard = self.node.state.shard_id_of_slot(vlpds::slots::slot_of(did));
        let meta = EventMeta { did: did.to_string(), host: Host(host), upstream_seq: 0, shard: shard.0 };
        self.node.local.append_own(meta, frame).await.map_err(|e| AdminError::Internal(anyhow::anyhow!("{e}")))?;
        self.account_view(did).await
    }
}

fn status_str(s: AccountStatus) -> &'static str {
    s.as_str().unwrap_or(if s.is_active() { "active" } else { "inactive" })
}

impl AdminSource for NodeAdmin {
    async fn overview(&self) -> AdminResult<admin::Overview> {
        let rows = self.rows();
        let mut by_status: BTreeMap<admin::HostStatus, u32> = BTreeMap::new();
        for r in &rows {
            *by_status.entry(r.status).or_default() += 1;
        }
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
        drop(dash);
        let mut top = rows.clone();
        top.sort_by(|a, b| b.events_per_sec.total_cmp(&a.events_per_sec));
        top.truncate(10);
        Ok(admin::Overview {
            time_ms: crate::upstream::host::now_ms() as i64,
            events_in_per_sec: last.events_in,
            events_out_per_sec: last.events_out,
            bytes_in_per_sec: last.bytes_in,
            bytes_out_per_sec: last.bytes_out,
            consumers: vlpds::metrics::FIREHOSE_SUBSCRIBERS.get().max(0) as u32,
            hosts_connected: *by_status.get(&admin::HostStatus::Connected).unwrap_or(&0),
            hosts_total: rows.len() as u32,
            hosts_by_status: by_status,
            rejects_per_sec: rejects_by_reason.values().sum(),
            rejects_by_reason,
            time_to_firehose_p50_ms: last.ttf_p50_ms,
            time_to_firehose_p99_ms: last.ttf_p99_ms,
            log_durability_lag_ms: last.durable_lag_ms,
            last_seq: self.node.log.last_durable_seq.load(std::sync::atomic::Ordering::Acquire),
            open_cases: 0,
            top_hosts: top,
            history: h,
        })
    }

    async fn hosts(&self, q: admin::HostQuery) -> AdminResult<admin::HostList> {
        let mut rows: Vec<admin::HostRow> = self
            .rows()
            .into_iter()
            .filter(|r| q.q.as_deref().is_none_or(|s| r.host.contains(s)))
            .filter(|r| q.tier.as_deref().is_none_or(|t| r.tier == t))
            .filter(|r| q.status.is_none_or(|s| r.status == s))
            .collect();
        match q.sort.as_deref() {
            Some("events") => rows.sort_by(|a, b| a.events_per_sec.total_cmp(&b.events_per_sec)),
            Some("errors") => rows.sort_by(|a, b| a.error_rate.total_cmp(&b.error_rate)),
            Some("accounts") => rows.sort_by_key(|r| r.accounts),
            Some("seq") => rows.sort_by_key(|r| r.last_upstream_seq),
            Some("tier") => rows.sort_by(|a, b| a.tier.cmp(&b.tier)),
            Some("status") => rows.sort_by_key(|r| r.status),
            _ => rows.sort_by(|a, b| a.host.cmp(&b.host)),
        }
        if q.desc {
            rows.reverse();
        }
        let total = rows.len();
        let rows = rows.into_iter().skip(q.offset.unwrap_or(0)).take(q.limit.unwrap_or(10_000)).collect();
        Ok(admin::HostList { total, hosts: rows })
    }

    async fn host(&self, host: &str) -> AdminResult<admin::HostDetail> {
        let row = self.host_row(host)?;
        let k = Host(host.to_string());
        let tier = self.node.manager.host(&k).map(|h| h.record.tier).unwrap_or(Tier::Default);
        let l = self.node.manager.config().limits.for_tier(tier);
        let eps = if l.events_per_sec.is_finite() { l.events_per_sec } else { 0.0 };
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
            limits: admin::TierLimits {
                events_per_sec: eps,
                events_per_hour: (eps * 3600.0) as u64,
                events_per_day: (eps * 86400.0) as u64,
                max_accounts: 0,
                new_accounts_per_hour: 0,
            },
            new_accounts_per_hour: 0.0,
            rejects_by_reason,
            recent_rejects,
            series,
            actions: Vec::new(),
            open_cases: Vec::new(),
        })
    }

    async fn host_action(&self, host: &str, action: admin::HostAction, _by: &str) -> AdminResult<admin::HostRow> {
        let k = Host(host.to_string());
        let m = &self.node.manager;
        if m.host(&k).is_none() {
            return Err(AdminError::NotFound(format!("unknown host {host}")));
        }
        let set = |t: Tier| async move { m.set_tier(&k, t).await.map_err(AdminError::Internal) };
        match action {
            admin::HostAction::SetTier { tier } => {
                let t = Tier::parse(&tier).ok_or_else(|| AdminError::BadRequest(format!("unknown tier {tier}")))?;
                set(t).await?
            }
            admin::HostAction::Suspend { .. } => set(Tier::Suspended).await?,
            admin::HostAction::Ban { .. } => set(Tier::Banned).await?,
            admin::HostAction::Unban => set(Tier::Default).await?,
            admin::HostAction::Reconnect => m.kick(&Host(host.to_string())),
            admin::HostAction::Throttle { .. } => {
                return Err(AdminError::BadRequest("per-host throttles come with the policy engine".into()));
            }
        }
        self.host_row(host)
    }

    async fn domain_rules(&self) -> AdminResult<Vec<admin::DomainRule>> {
        self.demo.domain_rules().await
    }
    async fn create_domain_rule(&self, rule: admin::DomainRuleInput, by: &str) -> AdminResult<admin::DomainRule> {
        self.demo.create_domain_rule(rule, by).await
    }
    async fn update_domain_rule(
        &self,
        id: u64,
        rule: admin::DomainRuleInput,
        by: &str,
    ) -> AdminResult<admin::DomainRule> {
        self.demo.update_domain_rule(id, rule, by).await
    }
    async fn delete_domain_rule(&self, id: u64, by: &str) -> AdminResult<()> {
        self.demo.delete_domain_rule(id, by).await
    }
    async fn policy(&self) -> AdminResult<admin::PolicyDoc> {
        self.demo.policy().await
    }
    async fn update_policy(&self, update: admin::PolicyUpdate, by: &str) -> AdminResult<admin::PolicyDoc> {
        self.demo.update_policy(update, by).await
    }
    async fn policy_audit(&self) -> AdminResult<Vec<admin::PolicyAudit>> {
        self.demo.policy_audit().await
    }

    async fn consumers(&self) -> AdminResult<Vec<admin::Consumer>> {
        let fh = &self.node.serve.firehose;
        let head = fh.last_emitted.load(std::sync::atomic::Ordering::Acquire);
        let seen = self.node.consumers.seen.lock();
        let mut out: Vec<admin::Consumer> = seen
            .iter()
            .filter(|(ip, _)| fh.connections_from(**ip) > 0)
            .map(|(ip, s)| admin::Consumer {
                id: s.id,
                ip: format!("{ip} ({} connections)", fh.connections_from(*ip)),
                user_agent: s.user_agent.clone(),
                node: self.node.cfg.node_id.clone(),
                connected_since_ms: s.since_ms,
                cursor: head,
                lag_ms: 0.0,
                events_per_sec: 0.0,
                bytes_per_sec: 0.0,
                backfilling: s.cursor.is_some_and(|c| c < head),
            })
            .collect();
        out.sort_by_key(|c| c.id);
        Ok(out)
    }

    async fn kick_consumer(&self, _id: u64, _by: &str) -> AdminResult<()> {
        Err(AdminError::BadRequest("kicking a consumer isn't wired to the firehose yet".into()))
    }

    async fn cluster(&self) -> AdminResult<admin::ClusterView> {
        self.demo.cluster().await
    }

    async fn accounts(&self, q: admin::AccountQuery) -> AdminResult<Vec<admin::Account>> {
        let Some(q) = q.q.filter(|s| !s.is_empty()) else { return Ok(Vec::new()) };
        if q.starts_with("did:") {
            return match self.account_view(&q).await {
                Ok(a) => Ok(vec![a]),
                Err(AdminError::NotFound(_)) => Ok(Vec::new()),
                Err(e) => Err(e),
            };
        }
        // handles aren't indexed; a DID prefix pages through listRepos
        Ok(Vec::new())
    }

    async fn account(&self, did: &str) -> AdminResult<admin::Account> {
        self.account_view(did).await
    }

    async fn takedown(&self, did: &str, _reason: String, _by: &str) -> AdminResult<admin::Account> {
        self.set_takedown(did, true).await
    }

    async fn untakedown(&self, did: &str, _by: &str) -> AdminResult<admin::Account> {
        self.set_takedown(did, false).await
    }

    async fn cases(&self, q: admin::CaseQuery) -> AdminResult<Vec<admin::Case>> {
        self.demo.cases(q).await
    }
    async fn case(&self, id: u64) -> AdminResult<admin::Case> {
        self.demo.case(id).await
    }
    async fn update_case(&self, id: u64, update: admin::CaseUpdate, by: &str) -> AdminResult<admin::Case> {
        self.demo.update_case(id, update, by).await
    }
}
