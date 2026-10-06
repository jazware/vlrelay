//! The operator API over the node's real state: hosts and their actions,
//! consumers, the overview's numbers and account lookups and takedowns.
//! Policy, domain rules, cases and host tier actions go to the policy
//! engine's admin half (`node::policy`). The cluster view is the real
//! cluster on a cluster node (`node::cluster::Glue::view`) and the
//! simulation on a single node.
//!
//! On a cluster every number is the cluster's: this node reports its own
//! (`local_*`) and asks every other member for theirs over the peer admin
//! RPC (`node::peer_admin`), and `admin::fleet` adds them up. A host's
//! detail and actions go to the node reading it, an account's to its DID
//! shard's owner, a consumer's kick to the node serving it.

use super::Node;
use super::metrics::HostSeries as Series;
use super::peer_admin::{self, Fleet, Process, Rate, Target};
use super::policy::PolicyHooks;
use crate::admin::fleet::{self, ArchiveReport, Member, NodeReport, PlcReport};
use crate::admin::{self, AdminError, AdminResult, AdminSource, RejectReason, demo::Demo};
use crate::cluster::ClusterNode;
use crate::policy::admin::PolicyAdmin;
use crate::seq::EventMeta;
use crate::state::{AccountStatus, Upstream};
use crate::types::Host;
use crate::upstream::{HostStatus, HostView, Tier};
use parking_lot::Mutex;
use std::collections::BTreeMap;
use std::sync::Arc;
use std::time::{Duration, Instant};

fn layout_json(c: &vlpds::cluster::Cluster) -> serde_json::Value {
    let l = c.layout();
    let shards: Vec<serde_json::Value> = l
        .shards
        .iter()
        .map(|r| serde_json::json!({"id": r.id, "lo": r.lo, "hi": r.hi, "owner": c.owner_of(r.id).map(|o| o.0)}))
        .collect();
    serde_json::json!({"version": l.version, "shards": shards, "nextId": l.next_id, "op": l.op})
}

pub struct NodeAdmin {
    pub node: Arc<Node>,
    pub policy: Arc<PolicyHooks>,
    pub demo: Arc<Demo>,
    /// (when, open cases): the overview polls every second or two, and a
    /// count is a bucket listing.
    open_cases: Mutex<Option<(Instant, u32)>>,
    pub fleet: Fleet,
    process: Process,
    plc_rate: Rate,
    /// (when read, windows, checkpoint time): the stored PLC checkpoint,
    /// which only changes every 10 s.
    plc_ck: Mutex<Option<(Instant, Vec<admin::PlcWindow>, i64)>>,
    settings: Option<admin::SettingsView>,
}

const OPEN_CASES_TTL: Duration = Duration::from_secs(10);
const PLC_CK_TTL: Duration = Duration::from_secs(5);

impl NodeAdmin {
    pub fn new(node: Arc<Node>, policy: Arc<PolicyHooks>, demo: Arc<Demo>) -> NodeAdmin {
        NodeAdmin {
            node,
            policy,
            demo,
            open_cases: Mutex::new(None),
            fleet: Fleet::new(Vec::new(), String::new()),
            process: Process::default(),
            plc_rate: Rate::default(),
            plc_ck: Mutex::new(None),
            settings: None,
        }
    }

    /// The process's effective config, for the Settings page.
    pub fn with_settings(mut self, s: admin::SettingsView) -> NodeAdmin {
        self.settings = Some(s);
        self
    }

    /// Edges and replicas to poll (their public URLs), with the admin token
    /// they share with this node.
    pub fn with_followers(mut self, urls: Vec<String>, admin_token: String) -> NodeAdmin {
        self.fleet = Fleet::new(urls, admin_token);
        self
    }

    fn cluster_node(&self) -> Option<&ClusterNode> {
        self.node.cluster.as_ref().map(|g| &*g.cluster)
    }

    fn id(&self) -> &str {
        &self.node.cfg.node_id
    }

    /// This node reads `host` (always, on a single node).
    fn owns(&self, host: &str) -> bool {
        match &self.node.cluster {
            Some(g) => g.cluster.owns_host(&Host(host.to_string())),
            None => true,
        }
    }

    /// The core reading `host`, when it's another live one.
    fn host_owner(&self, host: &str) -> Option<Target> {
        let g = self.node.cluster.as_ref()?;
        let (id, addr) = g.cluster.hosts.as_ref()?.owner_of(&Host(host.to_string()))?;
        (id != *self.id()).then_some(Target::Core { id, addr })
    }

    /// The core owning `did`'s shard, when it's another one.
    fn did_owner(&self, did: &str) -> Option<Target> {
        let g = self.node.cluster.as_ref()?;
        let o = g.cluster.owner_of_did(did)?;
        (o.node_id != *self.id()).then_some(Target::Core { id: o.node_id, addr: o.addr })
    }

    async fn members(&self) -> Arc<Vec<Member>> {
        if self.node.cluster.is_none() && !self.fleet.has_followers() {
            return Arc::new(vec![Member::ok(self.local_report().await)]);
        }
        self.fleet.members(self.cluster_node(), self.local_report()).await
    }

    async fn remote<T: serde::de::DeserializeOwned>(
        &self,
        t: &Target,
        method: reqwest::Method,
        path: &str,
        body: Option<serde_json::Value>,
    ) -> AdminResult<T> {
        let who = self.fleet.id_of(t).0;
        self.fleet.call(self.cluster_node(), t, method, path, body).await.map_err(|e| {
            if e.starts_with("HTTP 404") {
                AdminError::NotFound(format!("{who}: {e}"))
            } else if e.starts_with("HTTP 400") {
                AdminError::BadRequest(format!("{who}: {e}"))
            } else {
                AdminError::Internal(anyhow::anyhow!("{who} didn't answer: {e}"))
            }
        })
    }

    fn admin(&self) -> &PolicyAdmin {
        &self.policy.admin
    }
}

fn internal(e: impl std::fmt::Display) -> AdminError {
    AdminError::Internal(anyhow::anyhow!("{e}"))
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

    /// Every host in this node's registry (on a cluster, every host; only
    /// the owned ones have live numbers).
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
        rows.retain(|r| self.owns(&r.host));
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
            did_shard: shard.0,
            node: self.node.cfg.node_id.clone(),
            archive: self
                .account_archive(did, &self.node.state.host_name(rec.host).map(|h| h.to_string()).unwrap_or_default())
                .await,
        })
    }

    async fn account_archive(&self, did: &str, host: &str) -> Option<admin::AccountArchive> {
        let a = self.node.state.archive()?;
        let snap = self.policy.engine.snapshot();
        if snap.policy.body.archive.mode == crate::policy::doc::ArchiveMode::Off {
            return None;
        }
        let shard = self.node.state.shard_for(did).ok()?;
        let meta = crate::archive::mirror::read_meta(&shard.db, did).await.ok().flatten().unwrap_or_default();
        let head = match meta.live {
            Some(_) => crate::archive::mirror::read_head(&shard.db, did).await.ok().flatten(),
            None => None,
        };
        let last_error = a.queue.errors.lock().iter().rev().find(|(d, _)| d == did).map(|(_, e)| e.clone());
        Some(admin::AccountArchive {
            wanted: a.gate().wants(host),
            mirrored: meta.live.is_some(),
            rev: head.map(|h| h.rev.to_string()),
            fetching: a.queue.contains(did),
            staging: meta.staging.is_some(),
            last_error,
            takedown_at_ms: (meta.takedown_at != 0).then_some(meta.takedown_at as i64 * 1000),
        })
    }

    /// Writes the takedown flag, then announces the new status on the
    /// firehose with an `#account` the relay makes itself.
    async fn set_takedown(&self, did: &str, takedown: bool, by: &str, reason: &str) -> AdminResult<admin::Account> {
        if self.node.state.get(did).await.map_err(internal)?.is_none() {
            return Err(AdminError::NotFound(format!("no account {did}")));
        }
        let _own = match &self.node.cluster {
            Some(g) => Some(g.own_write(did).await.ok_or_else(|| {
                AdminError::Internal(anyhow::anyhow!("{did}'s shard isn't held here with a valid lease: try again"))
            })?),
            None => None,
        };
        self.policy.engine.takedowns.record(did, takedown, by, reason).await?;
        // before the #account below: this node's consumers must not see it
        // and then the account's old commits replayed
        self.node.serve.takedowns.apply_local(did, takedown);
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

impl NodeAdmin {
    fn vlpds_cluster(&self) -> AdminResult<(Arc<vlpds::cluster::Cluster>, Arc<dyn vlpds::cluster::ShardHost>)> {
        let g = self.node.cluster.as_ref().ok_or_else(|| AdminError::BadRequest("not a cluster core node".into()))?;
        let c = g.cluster.cluster.clone().ok_or_else(|| AdminError::BadRequest("not a cluster core node".into()))?;
        let host: Arc<dyn vlpds::cluster::ShardHost> = g.cluster.clone();
        Ok((c, host))
    }
}

/// For the peer RPC's query strings.
fn enc(s: &str) -> String {
    let mut out = String::with_capacity(s.len());
    for b in s.bytes() {
        if b.is_ascii_alphanumeric() || b"-_.~".contains(&b) {
            out.push(b as char);
        } else {
            out.push_str(&format!("%{b:02X}"));
        }
    }
    out
}

/// What this node itself measures and holds; the peer RPC serves these.
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
        let (cpu, mem_bytes) = self.process.sample();
        NodeReport {
            node: self.id().to_string(),
            role: if self.node.cluster.is_some() { "core" } else { "single" }.into(),
            version: env!("CARGO_PKG_VERSION").into(),
            time_ms: peer_admin::now_ms(),
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
            stream_seq: self.node.serve.firehose.last_emitted.load(std::sync::atomic::Ordering::Acquire),
            cpu,
            mem_bytes,
            top_hosts: top,
            history: h,
            pipeline,
            pipeline_hosts,
            archive: self.archive_report(),
            plc: self.plc_report().await,
            seq_checkpoints: self.node.serve.seq_checkpoints(peer_admin::SEQ_CHECKPOINTS),
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
            dedupe_entries: self.node.cluster.as_ref().map_or(0, |g| g.recent.len() as u64),
            paused_hosts,
            gauges: peer_admin::pipeline_gauges(),
        };
        (node, hosts)
    }

    fn archive_report(&self) -> Option<ArchiveReport> {
        use std::sync::atomic::Ordering::Relaxed;
        let a = self.node.state.archive()?;
        let snap = self.policy.engine.snapshot();
        let mode = serde_json::to_value(snap.policy.body.archive.mode)
            .ok()
            .and_then(|v| v.as_str().map(str::to_string))
            .unwrap_or_default();
        let (queued, running) = a.queue.depth();
        let f = &a.queue.stats;
        let counts = admin::ArchiveCounts {
            mirrored: a.stats.mirrors.load(Relaxed),
            queued: queued as u64,
            running: running as u64,
            failed: f.failed.load(Relaxed),
            fetched: f.done.load(Relaxed),
            retried: f.retried.load(Relaxed),
            bytes: f.bytes.load(Relaxed),
            records: f.records.load(Relaxed),
            sst_bytes: self.node.state.shards().iter().map(|s| s.sst_bytes()).sum(),
            applied: a.stats.applied.load(Relaxed),
            mismatches: a.stats.mismatches.load(Relaxed),
            healed: f.healed.load(Relaxed),
            swept_at_ms: a.stats.swept_at_ms.load(Relaxed) as i64,
        };
        Some(ArchiveReport {
            mode,
            policy_version: a.gate().version(),
            counts,
            errors: a.queue.errors.lock().iter().cloned().collect(),
        })
    }

    async fn plc_report(&self) -> Option<PlcReport> {
        use std::sync::atomic::Ordering::Relaxed;
        let ing = self.node.plc_ingest.get()?;
        let s = &ing.stats;
        let leader = self.node.cluster.as_ref().is_none_or(|g| g.cluster.plc_ingest_leader());
        let ops = s.ops.load(Relaxed);
        let (windows, checkpoint_ms) = if leader { self.plc_windows(ing).await } else { (Vec::new(), 0) };
        Some(PlcReport {
            leader,
            caught_up: s.caught_up.load(Relaxed),
            ops,
            ops_per_sec: self.plc_rate.update(ops as f64),
            written: s.written.load(Relaxed),
            requests: s.requests.load(Relaxed),
            throttled: s.throttled.load(Relaxed),
            errors: s.errors.load(Relaxed),
            restarts: s.restarts.load(Relaxed),
            newest_ms: s.newest_ms.load(Relaxed) as i64,
            windows,
            checkpoint_ms,
        })
    }

    async fn plc_windows(&self, ing: &crate::plc_seed::ingest::Ingester) -> (Vec<admin::PlcWindow>, i64) {
        if let Some((at, w, ms)) = &*self.plc_ck.lock()
            && at.elapsed() < PLC_CK_TTL
        {
            return (w.clone(), *ms);
        }
        let ck = match crate::plc_seed::ingest::Checkpoint::load(&ing.store).await {
            Ok(Some(c)) => c,
            Ok(None) => return (Vec::new(), 0),
            Err(e) => {
                tracing::debug!("reading the PLC export checkpoint: {e:#}");
                return (Vec::new(), 0);
            }
        };
        let now = peer_admin::now_ms();
        let ms = |s: &str| crate::plc_seed::parse_ms(s).map_or(0, |v| v as i64);
        let mut from = ing.cfg.start_ms as i64;
        let windows = ck
            .windows
            .iter()
            .map(|w| {
                let after = ms(&w.after).max(from);
                let until = w.until.as_deref().map(ms);
                let end = until.unwrap_or(now);
                let progress = if w.done {
                    1.0
                } else if end > from {
                    ((after - from) as f64 / (end - from) as f64).clamp(0.0, 1.0)
                } else {
                    0.0
                };
                let out = admin::PlcWindow {
                    from_ms: from,
                    after_ms: after,
                    until_ms: until,
                    ops: w.count,
                    done: w.done,
                    progress,
                };
                if let Some(u) = until {
                    from = u + 1;
                }
                out
            })
            .collect::<Vec<_>>();
        *self.plc_ck.lock() = Some((Instant::now(), windows.clone(), ck.updated_ms));
        (windows, ck.updated_ms)
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
        peer_admin::consumers_of(&self.node.serve, self.id())
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
            if let Some(t) = self.did_owner(&id.did) {
                match self
                    .remote::<admin::Account>(&t, reqwest::Method::GET, &format!("/account?did={}", enc(&id.did)), None)
                    .await
                {
                    Ok(a) => out.push(a),
                    Err(e) => tracing::debug!(did = %id.did, "account from its owner: {e}"),
                }
                continue;
            }
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

    async fn takedown_anywhere(
        &self,
        did: &str,
        takedown: bool,
        by: &str,
        reason: &str,
    ) -> AdminResult<admin::Account> {
        match self.did_owner(did) {
            Some(t) => {
                let body = serde_json::to_value(peer_admin::TakedownIn {
                    did: did.to_string(),
                    takedown,
                    by: by.to_string(),
                    reason: reason.to_string(),
                })
                .map_err(internal)?;
                self.remote(&t, reqwest::Method::POST, "/takedown", Some(body)).await
            }
            None => self.set_takedown(did, takedown, by, reason).await,
        }
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
        Ok(fleet::overview(&members, open_cases, peer_admin::now_ms()))
    }

    async fn hosts(&self, q: admin::HostQuery) -> AdminResult<admin::HostList> {
        let Some(g) = &self.node.cluster else {
            return Ok(Self::sort_page(self.rows(), &q));
        };
        let mut owned = vec![(self.id().to_string(), self.owned_rows())];
        owned.extend(self.fleet.each::<Vec<admin::HostRow>>(self.cluster_node(), "/hosts").await);
        let hs = g.cluster.hosts.clone();
        let rows = fleet::merge_hosts(self.rows(), &owned, |h| {
            hs.as_ref().and_then(|s| s.owner_of(&Host(h.to_string()))).map(|(id, _)| id)
        });
        Ok(Self::sort_page(rows, &q))
    }

    async fn host(&self, host: &str) -> AdminResult<admin::HostDetail> {
        if let Some(t) = self.host_owner(host) {
            match self.remote(&t, reqwest::Method::GET, &format!("/host?host={}", enc(host)), None).await {
                Ok(d) => return Ok(d),
                Err(AdminError::NotFound(m)) => return Err(AdminError::NotFound(m)),
                // the owner is down: this node's registry row, without its live numbers
                Err(e) => tracing::debug!(host, "host detail from its owner: {e}"),
            }
        }
        self.local_host(host).await
    }

    async fn host_action(&self, host: &str, action: admin::HostAction, by: &str) -> AdminResult<admin::HostRow> {
        let k = Host(host.to_string());
        if self.node.manager.host(&k).is_none() {
            return Err(AdminError::NotFound(format!("unknown host {host}")));
        }
        let owner = self.host_owner(host);
        match (action, &owner) {
            (admin::HostAction::Reconnect, Some(t)) => {
                self.remote::<serde_json::Value>(
                    t,
                    reqwest::Method::POST,
                    &format!("/reconnect?host={}", enc(host)),
                    None,
                )
                .await?;
            }
            (admin::HostAction::Reconnect, None) => self.node.manager.kick(&k),
            (a, _) => {
                self.admin().host_action(host, a, by).await?;
                // the socket follows now, not at the sync loop's next pass
                self.policy.refresh_host(host).await?;
            }
        }
        if let Some(t) = owner
            && let Ok(d) = self
                .remote::<admin::HostDetail>(&t, reqwest::Method::GET, &format!("/host?host={}", enc(host)), None)
                .await
        {
            return Ok(d.row);
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

    async fn consumers(&self) -> AdminResult<Vec<admin::Consumer>> {
        let mut out = self.local_consumers();
        for (_, cs) in self.fleet.each::<Vec<admin::Consumer>>(self.cluster_node(), "/consumers").await {
            out.extend(cs);
        }
        Ok(out)
    }

    async fn kick_consumer(&self, id: u64, by: &str) -> AdminResult<()> {
        self.local_kick(id, by)
    }

    async fn kick_consumer_on(&self, node: Option<&str>, id: u64, by: &str) -> AdminResult<()> {
        let Some(n) = node.filter(|n| *n != self.id()) else {
            return self.local_kick(id, by);
        };
        let t = self
            .fleet
            .target(self.cluster_node(), n)
            .ok_or_else(|| AdminError::NotFound(format!("no node {n} in this cluster")))?;
        self.remote::<serde_json::Value>(&t, reqwest::Method::POST, &format!("/kick?id={id}"), None).await?;
        tracing::info!(target: "vlrelay::audit", consumer = id, node = n, by, "consumer kicked");
        Ok(())
    }

    async fn settings(&self) -> AdminResult<admin::SettingsView> {
        self.settings.clone().ok_or_else(|| AdminError::NotFound("this node doesn't report its config".into()))
    }

    // TODO(qlog wiring): once a relay node runs the quorum log, answer
    // `quorum` from each member's `/qlog/status` (addrs from `qlog/leader`,
    // never `?reset=true`) and forward `change_quorum_members` to the
    // leader's `POST /qlog/members`. Until then the trait's defaults say
    // the relay doesn't run one, and the public page leaves quorum out.

    async fn cluster(&self) -> AdminResult<admin::ClusterView> {
        match &self.node.cluster {
            Some(g) => {
                let mut v = g.view(&self.node);
                let members = self.members().await;
                fleet::fill_cluster(&mut v, &members);
                Ok(v)
            }
            None => self.demo.cluster().await,
        }
    }

    async fn archive_view(&self) -> AdminResult<admin::ArchiveView> {
        fleet::archive_view(&self.members().await)
            .ok_or_else(|| AdminError::NotFound("no node reported archival numbers".into()))
    }

    async fn plc_view(&self) -> AdminResult<admin::PlcView> {
        Ok(fleet::plc_view(&self.members().await))
    }

    async fn seq_view(&self) -> AdminResult<admin::SeqView> {
        Ok(fleet::seq_view(&self.members().await, 8))
    }

    async fn pipeline_view(&self) -> AdminResult<admin::PipelineView> {
        Ok(fleet::pipeline_view(&self.members().await, 50))
    }

    async fn shard_layout(&self) -> AdminResult<serde_json::Value> {
        let (c, _) = self.vlpds_cluster()?;
        Ok(layout_json(&c))
    }

    async fn reshard(&self, req: admin::ReshardReq) -> AdminResult<serde_json::Value> {
        use vlpds::reshard::Plan;
        use vlpds::slots::ShardId;
        let (c, host) = self.vlpds_cluster()?;
        let (plan, wait) = match req {
            admin::ReshardReq::Split { shard, at, wait } => (Plan::Split { shard: ShardId(shard), at }, wait),
            admin::ReshardReq::Merge { left, right, wait } => {
                (Plan::Merge { left: ShardId(left), right: ShardId(right) }, wait)
            }
            admin::ReshardReq::Abort => {
                let op = c.abort_reshard(&host).await?;
                return Ok(serde_json::json!({ "aborted": op, "layout": layout_json(&c) }));
            }
        };
        let before = c.layout().version;
        let op = c.plan_reshard(&host, plan).await.map_err(|e| AdminError::BadRequest(format!("{e:#}")))?;
        let mut out = serde_json::json!({ "op": op });
        if wait {
            let deadline = Instant::now() + Duration::from_secs(120);
            loop {
                let l = c.layout();
                if l.version > before && l.op.as_ref().is_none_or(|o| o.id != op.id) {
                    out["done"] = l.shards.iter().any(|r| op.children.iter().any(|ch| ch.id == r.id)).into();
                    break;
                }
                if l.op.is_none() && l.version == before {
                    out["done"] = false.into();
                    break;
                }
                if Instant::now() > deadline {
                    return Err(AdminError::Internal(anyhow::anyhow!(
                        "reshard {} still in progress after 120 s",
                        op.id
                    )));
                }
                tokio::time::sleep(Duration::from_millis(20)).await;
            }
        }
        out["layout"] = layout_json(&c);
        Ok(out)
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
        let mut out = self.local_accounts(&q).await?;
        if self.node.cluster.is_some() {
            // handles are in the identity caches of the nodes reading their hosts
            let peers =
                self.fleet.each::<Vec<admin::Account>>(self.cluster_node(), &format!("/accounts?q={}", enc(&q))).await;
            for (_, accts) in peers {
                for a in accts {
                    if !out.iter().any(|x| x.did == a.did) {
                        out.push(a);
                    }
                }
            }
            out.truncate(100);
        }
        Ok(out)
    }

    async fn account(&self, did: &str) -> AdminResult<admin::Account> {
        match self.did_owner(did) {
            Some(t) => self.remote(&t, reqwest::Method::GET, &format!("/account?did={}", enc(did)), None).await,
            None => self.account_view(did).await,
        }
    }

    async fn takedown(&self, did: &str, reason: String, by: &str) -> AdminResult<admin::Account> {
        self.takedown_anywhere(did, true, by, &reason).await
    }

    async fn untakedown(&self, did: &str, by: &str) -> AdminResult<admin::Account> {
        self.takedown_anywhere(did, false, by, "").await
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
