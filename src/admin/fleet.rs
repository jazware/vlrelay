//! Cluster-wide numbers for the dashboard. Each node reports its own
//! ([`NodeReport`], over the peer admin RPC in `node::peer_admin`) and the
//! node answering the dashboard adds them up here. A node that didn't
//! answer is a [`Member`] with no report: it's listed as stale with zeros,
//! and left out of every sum, so the totals always equal the sum of the
//! rows shown beside them.

use super::{
    ArchiveCounts, ArchiveError, ArchiveNode, ArchiveView, ClusterView, History, HostRow, HostStatus, NodeTotals,
    NodeView, Overview, PipelineHost, PipelineNode, PipelineView, PlcNode, PlcView, PlcWindow, RejectReason,
    SeqBoundary, SeqNode, SeqPair, SeqView,
};
use serde::{Deserialize, Serialize};
use std::collections::{BTreeMap, BTreeSet, HashMap};

/// One node's live numbers, as it measured them.
#[derive(Clone, Debug, Default, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct NodeReport {
    pub node: String,
    /// `core`, `edge`, `replica` or `single`.
    pub role: String,
    pub version: String,
    pub time_ms: i64,
    pub events_in_per_sec: f64,
    pub events_out_per_sec: f64,
    pub bytes_in_per_sec: f64,
    pub bytes_out_per_sec: f64,
    pub consumers: u32,
    /// The hosts this node reads (on a core, its host shards' hosts).
    pub hosts_by_status: BTreeMap<HostStatus, u32>,
    pub rejects_by_reason: BTreeMap<RejectReason, f64>,
    pub ttf_p50_ms: f64,
    pub ttf_p99_ms: f64,
    pub log_durability_lag_ms: f64,
    /// The last seq its merged stream emitted.
    pub stream_seq: i64,
    pub cpu: f64,
    pub mem_bytes: u64,
    pub top_hosts: Vec<HostRow>,
    pub history: History,
    pub pipeline: PipelineNode,
    pub pipeline_hosts: Vec<PipelineHost>,
    pub archive: Option<ArchiveReport>,
    pub plc: Option<PlcReport>,
    /// Its newest (key, seq) stream checkpoints, oldest first.
    pub seq_checkpoints: Vec<(i64, i64)>,
}

#[derive(Clone, Debug, Default, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ArchiveReport {
    pub mode: String,
    pub policy_version: u64,
    pub counts: ArchiveCounts,
    /// (did, error), newest last.
    pub errors: Vec<(String, String)>,
}

#[derive(Clone, Debug, Default, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct PlcReport {
    /// This node is the one reading the export.
    pub leader: bool,
    pub caught_up: bool,
    pub ops: u64,
    pub ops_per_sec: f64,
    pub written: u64,
    pub requests: u64,
    pub throttled: u64,
    pub errors: u64,
    pub restarts: u64,
    pub newest_ms: i64,
    /// The stored checkpoint, read by the leader.
    pub windows: Vec<PlcWindow>,
    pub checkpoint_ms: i64,
}

impl NodeReport {
    pub fn hosts_total(&self) -> u32 {
        self.hosts_by_status.values().sum()
    }

    pub fn hosts_connected(&self) -> u32 {
        self.hosts_by_status.get(&HostStatus::Connected).copied().unwrap_or(0)
    }

    pub fn rejects_per_sec(&self) -> f64 {
        self.rejects_by_reason.values().sum()
    }
}

/// A node as the dashboard sees it this round.
#[derive(Clone, Debug)]
pub struct Member {
    pub id: String,
    pub role: String,
    /// None: it didn't answer (`error` says why).
    pub report: Option<NodeReport>,
    pub error: Option<String>,
    /// When it last answered (unix ms; 0: never).
    pub last_ok_ms: i64,
}

impl Member {
    pub fn ok(r: NodeReport) -> Member {
        Member { id: r.node.clone(), role: r.role.clone(), last_ok_ms: r.time_ms, report: Some(r), error: None }
    }

    pub fn stale(id: &str, role: &str, error: String, last_ok_ms: i64) -> Member {
        Member { id: id.to_string(), role: role.to_string(), report: None, error: Some(error), last_ok_ms }
    }

    pub fn is_stale(&self) -> bool {
        self.report.is_none()
    }
}

fn fresh(members: &[Member]) -> impl Iterator<Item = &NodeReport> {
    members.iter().filter_map(|m| m.report.as_ref())
}

/// Sums aligned by sample time. Rates add; latencies and lags take the
/// worst node, since a mean would hide the one node that's behind.
pub fn merge_history(hs: &[&History]) -> History {
    let mut ts: BTreeSet<i64> = BTreeSet::new();
    let mut keep = 0usize;
    for h in hs {
        ts.extend(h.t.iter().copied());
        keep = keep.max(h.t.len());
    }
    let ts: Vec<i64> = ts.into_iter().rev().take(keep).collect::<Vec<_>>().into_iter().rev().collect();
    let at: HashMap<i64, usize> = ts.iter().enumerate().map(|(i, t)| (*t, i)).collect();
    let n = ts.len();
    let mut out = History {
        sample_secs: hs.iter().map(|h| h.sample_secs).max().unwrap_or(1).max(1),
        t: ts,
        events_in: vec![0.0; n],
        events_out: vec![0.0; n],
        bytes_in: vec![0.0; n],
        bytes_out: vec![0.0; n],
        ttf_p50_ms: vec![0.0; n],
        ttf_p99_ms: vec![0.0; n],
        durability_lag_ms: vec![0.0; n],
        rejects: BTreeMap::new(),
    };
    for h in hs {
        for (j, t) in h.t.iter().enumerate() {
            let Some(&i) = at.get(t) else { continue };
            let v = |s: &Vec<f64>| s.get(j).copied().unwrap_or(0.0);
            out.events_in[i] += v(&h.events_in);
            out.events_out[i] += v(&h.events_out);
            out.bytes_in[i] += v(&h.bytes_in);
            out.bytes_out[i] += v(&h.bytes_out);
            out.ttf_p50_ms[i] = out.ttf_p50_ms[i].max(v(&h.ttf_p50_ms));
            out.ttf_p99_ms[i] = out.ttf_p99_ms[i].max(v(&h.ttf_p99_ms));
            out.durability_lag_ms[i] = out.durability_lag_ms[i].max(v(&h.durability_lag_ms));
            for (r, s) in &h.rejects {
                out.rejects.entry(*r).or_insert_with(|| vec![0.0; n])[i] += v(s);
            }
        }
    }
    out
}

pub fn totals(m: &Member) -> NodeTotals {
    let mut t = NodeTotals {
        node: m.id.clone(),
        role: m.role.clone(),
        stale: m.is_stale(),
        error: m.error.clone(),
        ..Default::default()
    };
    if let Some(r) = &m.report {
        t.events_in_per_sec = r.events_in_per_sec;
        t.events_out_per_sec = r.events_out_per_sec;
        t.bytes_in_per_sec = r.bytes_in_per_sec;
        t.bytes_out_per_sec = r.bytes_out_per_sec;
        t.consumers = r.consumers;
        t.hosts_connected = r.hosts_connected();
        t.hosts_total = r.hosts_total();
        t.rejects_per_sec = r.rejects_per_sec();
    }
    t
}

/// The overview over every member that answered.
pub fn overview(members: &[Member], open_cases: u32, now_ms: i64) -> Overview {
    let reports: Vec<&NodeReport> = fresh(members).collect();
    let mut by_status: BTreeMap<HostStatus, u32> = BTreeMap::new();
    let mut rejects: BTreeMap<RejectReason, f64> = BTreeMap::new();
    let mut top: Vec<HostRow> = Vec::new();
    for r in &reports {
        for (s, n) in &r.hosts_by_status {
            *by_status.entry(*s).or_default() += n;
        }
        for (k, v) in &r.rejects_by_reason {
            *rejects.entry(*k).or_default() += v;
        }
        top.extend(r.top_hosts.iter().cloned());
    }
    top.sort_by(|a, b| b.events_per_sec.total_cmp(&a.events_per_sec));
    top.truncate(10);
    let sum = |f: fn(&NodeReport) -> f64| reports.iter().map(|r| f(r)).sum::<f64>();
    let max = |f: fn(&NodeReport) -> f64| reports.iter().map(|r| f(r)).fold(0.0, f64::max);
    Overview {
        time_ms: now_ms,
        events_in_per_sec: sum(|r| r.events_in_per_sec),
        events_out_per_sec: sum(|r| r.events_out_per_sec),
        bytes_in_per_sec: sum(|r| r.bytes_in_per_sec),
        bytes_out_per_sec: sum(|r| r.bytes_out_per_sec),
        consumers: reports.iter().map(|r| r.consumers).sum(),
        hosts_connected: by_status.get(&HostStatus::Connected).copied().unwrap_or(0),
        hosts_total: by_status.values().sum(),
        hosts_by_status: by_status,
        rejects_per_sec: rejects.values().sum(),
        rejects_by_reason: rejects,
        time_to_firehose_p50_ms: max(|r| r.ttf_p50_ms),
        time_to_firehose_p99_ms: max(|r| r.ttf_p99_ms),
        log_durability_lag_ms: max(|r| r.log_durability_lag_ms),
        last_seq: reports.iter().map(|r| r.stream_seq).max().unwrap_or(0),
        open_cases,
        top_hosts: top,
        history: merge_history(&reports.iter().map(|r| &r.history).collect::<Vec<_>>()),
        stream_events_per_sec: max(|r| r.events_out_per_sec),
        by_node: members.iter().map(totals).collect(),
    }
}

/// Fills the lease-based node rows with each member's numbers and adds the
/// members that hold no lease (edges, replicas).
pub fn fill_cluster(view: &mut ClusterView, members: &[Member]) {
    if let Some(s) = fresh(members).map(|r| r.stream_seq).max() {
        view.last_seq = s;
    }
    let by_id: HashMap<&str, &Member> = members.iter().map(|m| (m.id.as_str(), m)).collect();
    for n in &mut view.nodes {
        if n.role.is_empty() {
            n.role = "core".into();
        }
        match by_id.get(n.id.as_str()) {
            Some(m) => fill_node(n, m),
            None => {
                n.stale = true;
                n.error = Some("not polled".into());
            }
        }
    }
    for m in members {
        if view.nodes.iter().any(|n| n.id == m.id) {
            continue;
        }
        let mut n = NodeView {
            id: m.id.clone(),
            addr: String::new(),
            version: String::new(),
            rev: String::new(),
            reachable: false,
            lease_valid: false,
            lease_expires_ms: 0,
            host_shards: 0,
            did_shards: 0,
            hosts: 0,
            consumers: 0,
            events_in_per_sec: 0.0,
            events_out_per_sec: 0.0,
            log_durability_lag_ms: 0.0,
            cpu: 0.0,
            mem_bytes: 0,
            role: m.role.clone(),
            stale: false,
            error: None,
            reported_ms: 0,
            bytes_out_per_sec: 0.0,
            stream_seq: 0,
        };
        fill_node(&mut n, m);
        view.nodes.push(n);
    }
    view.nodes.sort_by(|a, b| (a.role != "core", &a.id).cmp(&(b.role != "core", &b.id)));
}

fn fill_node(n: &mut NodeView, m: &Member) {
    n.reported_ms = m.last_ok_ms;
    n.error = m.error.clone();
    n.stale = m.is_stale();
    if !m.role.is_empty() {
        n.role = m.role.clone();
    }
    match &m.report {
        Some(r) => {
            n.reachable = true;
            n.hosts = r.hosts_total();
            n.consumers = r.consumers;
            n.events_in_per_sec = r.events_in_per_sec;
            n.events_out_per_sec = r.events_out_per_sec;
            n.bytes_out_per_sec = r.bytes_out_per_sec;
            n.log_durability_lag_ms = r.log_durability_lag_ms;
            n.cpu = r.cpu;
            n.mem_bytes = r.mem_bytes;
            n.stream_seq = r.stream_seq;
            n.version = r.version.clone();
        }
        None => {
            n.reachable = false;
            n.hosts = 0;
            n.consumers = 0;
            n.events_in_per_sec = 0.0;
            n.events_out_per_sec = 0.0;
            n.bytes_out_per_sec = 0.0;
            n.log_durability_lag_ms = 0.0;
            n.cpu = 0.0;
            n.mem_bytes = 0;
        }
    }
}

/// Every host once: the row from its owner where the owner answered (live
/// rates), else this node's registry row, attributed to the owner.
pub fn merge_hosts(
    registry: Vec<HostRow>,
    owned: &[(String, Vec<HostRow>)],
    owner_of: impl Fn(&str) -> Option<String>,
) -> Vec<HostRow> {
    let mut out: BTreeMap<String, HostRow> = BTreeMap::new();
    for mut r in registry {
        r.events_per_sec = 0.0;
        r.error_rate = 0.0;
        r.node = owner_of(&r.host).unwrap_or_default();
        out.insert(r.host.clone(), r);
    }
    for (node, rows) in owned {
        for r in rows {
            let mut r = r.clone();
            r.node = node.clone();
            out.insert(r.host.clone(), r);
        }
    }
    out.into_values().collect()
}

pub fn archive_view(members: &[Member]) -> Option<ArchiveView> {
    let with: Vec<&Member> =
        members.iter().filter(|m| m.role == "core" || m.role == "single" || m.report.is_none()).collect();
    let first = fresh(members).filter_map(|r| r.archive.as_ref()).max_by_key(|a| a.policy_version)?;
    let mut v = ArchiveView { mode: first.mode.clone(), policy_version: first.policy_version, ..Default::default() };
    for m in with {
        let a = m.report.as_ref().and_then(|r| r.archive.as_ref());
        if m.report.is_some() && a.is_none() {
            continue;
        }
        let counts = a.map(|a| a.counts.clone()).unwrap_or_default();
        let t = &mut v.totals;
        t.mirrored += counts.mirrored;
        t.queued += counts.queued;
        t.running += counts.running;
        t.failed += counts.failed;
        t.fetched += counts.fetched;
        t.retried += counts.retried;
        t.bytes += counts.bytes;
        t.records += counts.records;
        t.sst_bytes += counts.sst_bytes;
        t.applied += counts.applied;
        t.mismatches += counts.mismatches;
        t.healed += counts.healed;
        t.swept_at_ms = t.swept_at_ms.max(counts.swept_at_ms);
        if let Some(a) = a {
            for (did, error) in &a.errors {
                v.errors.push(ArchiveError { node: m.id.clone(), did: did.clone(), error: error.clone() });
            }
        }
        v.nodes.push(ArchiveNode { node: m.id.clone(), stale: m.is_stale(), counts });
    }
    let keep = v.errors.len().saturating_sub(64);
    v.errors.drain(..keep);
    Some(v)
}

pub fn plc_view(members: &[Member]) -> PlcView {
    let mut v = PlcView::default();
    for m in members {
        let p = m.report.as_ref().and_then(|r| r.plc.as_ref());
        if m.report.is_some() && p.is_none() {
            continue;
        }
        let Some(p) = p else {
            if m.role == "core" {
                v.nodes.push(PlcNode { node: m.id.clone(), stale: true, ..Default::default() });
            }
            continue;
        };
        v.enabled = true;
        v.nodes.push(PlcNode {
            node: m.id.clone(),
            stale: false,
            leader: p.leader,
            ops: p.ops,
            ops_per_sec: p.ops_per_sec,
            throttled: p.throttled,
            errors: p.errors,
        });
        // counters restart with each leader, so the cluster's numbers are
        // the current leader's; the checkpoint is shared
        if p.leader {
            v.leader = Some(m.id.clone());
            v.caught_up = p.caught_up;
            v.ops = p.ops;
            v.ops_per_sec = p.ops_per_sec;
            v.written = p.written;
            v.requests = p.requests;
            v.newest_ms = p.newest_ms;
        }
        v.throttled += p.throttled;
        v.errors += p.errors;
        v.restarts += p.restarts;
        if p.checkpoint_ms >= v.checkpoint_ms && !p.windows.is_empty() {
            v.checkpoint_ms = p.checkpoint_ms;
            v.windows = p.windows.clone();
        }
    }
    v
}

pub fn seq_pair(key: i64, seq: i64) -> SeqPair {
    SeqPair { key, time_ms: (key >> 8) / 1000, seq }
}

/// Lines up every node's checkpoints by boundary. A boundary two nodes
/// counted differently means they numbered the stream differently.
pub fn seq_view(members: &[Member], boundaries: usize) -> SeqView {
    let mut by_key: BTreeMap<i64, BTreeMap<String, i64>> = BTreeMap::new();
    let mut nodes = Vec::new();
    for m in members {
        let r = m.report.as_ref();
        for (k, s) in r.map(|r| r.seq_checkpoints.as_slice()).unwrap_or_default() {
            by_key.entry(*k).or_default().insert(m.id.clone(), *s);
        }
        nodes.push(SeqNode {
            node: m.id.clone(),
            role: m.role.clone(),
            stale: m.is_stale(),
            head: r.map_or(0, |r| r.stream_seq),
            latest: r.and_then(|r| r.seq_checkpoints.last()).map(|(k, s)| seq_pair(*k, *s)),
        });
    }
    let all: Vec<SeqBoundary> = by_key
        .into_iter()
        .map(|(key, seqs)| {
            let agree = seqs.values().collect::<BTreeSet<_>>().len() <= 1;
            SeqBoundary { key, time_ms: (key >> 8) / 1000, seqs, agree }
        })
        .collect();
    let agree = all.iter().all(|b| b.agree);
    let boundaries = all.into_iter().rev().take(boundaries).collect();
    SeqView { nodes, boundaries, agree }
}

pub fn pipeline_view(members: &[Member], hosts: usize) -> PipelineView {
    let mut v = PipelineView::default();
    for m in members {
        if m.role != "core" && m.role != "single" {
            continue;
        }
        match &m.report {
            Some(r) => {
                let mut p = r.pipeline.clone();
                p.node = m.id.clone();
                p.stale = false;
                v.nodes.push(p);
                for h in &r.pipeline_hosts {
                    let mut h = h.clone();
                    h.node = m.id.clone();
                    v.hosts.push(h);
                }
            }
            None => v.nodes.push(PipelineNode { node: m.id.clone(), stale: true, ..Default::default() }),
        }
    }
    v.hosts.sort_by(|a, b| b.inflight.cmp(&a.inflight).then(b.paused.cmp(&a.paused)).then(a.host.cmp(&b.host)));
    v.hosts.truncate(hosts);
    v
}

#[cfg(test)]
mod tests {
    use super::*;

    fn hist(t0: i64, evs: &[f64], p99: &[f64]) -> History {
        History {
            sample_secs: 1,
            t: (0..evs.len() as i64).map(|i| t0 + i).collect(),
            events_in: evs.to_vec(),
            events_out: evs.to_vec(),
            bytes_in: evs.iter().map(|e| e * 100.0).collect(),
            bytes_out: evs.to_vec(),
            ttf_p50_ms: p99.iter().map(|x| x / 2.0).collect(),
            ttf_p99_ms: p99.to_vec(),
            durability_lag_ms: p99.to_vec(),
            rejects: BTreeMap::from([(RejectReason::BadSignature, vec![1.0; evs.len()])]),
        }
    }

    fn row(host: &str, eps: f64) -> HostRow {
        HostRow {
            host: host.into(),
            tier: "default".into(),
            status: HostStatus::Connected,
            events_per_sec: eps,
            error_rate: 0.0,
            accounts: 1,
            last_upstream_seq: 1,
            connected_since_ms: None,
            lag_ms: 0.0,
            throttle: None,
            rule: None,
            node: String::new(),
            max_accounts: 100,
        }
    }

    fn report(node: &str, role: &str, ev: f64, consumers: u32, connected: u32) -> NodeReport {
        NodeReport {
            node: node.into(),
            role: role.into(),
            time_ms: 1000,
            events_in_per_sec: ev,
            events_out_per_sec: ev * 2.0,
            bytes_in_per_sec: ev * 100.0,
            bytes_out_per_sec: ev * 10.0,
            consumers,
            hosts_by_status: BTreeMap::from([(HostStatus::Connected, connected), (HostStatus::Backoff, 1)]),
            rejects_by_reason: BTreeMap::from([(RejectReason::BadSignature, 0.5)]),
            ttf_p50_ms: ev / 10.0,
            ttf_p99_ms: ev,
            stream_seq: ev as i64,
            top_hosts: vec![row(&format!("{node}-busy"), ev), row(&format!("{node}-idle"), 0.0)],
            history: hist(100, &[ev, ev], &[ev, ev]),
            ..Default::default()
        }
    }

    #[test]
    fn overview_totals_are_the_sum_of_the_nodes() {
        let ms = vec![
            Member::ok(report("n1", "core", 100.0, 2, 3)),
            Member::ok(report("n2", "core", 50.0, 1, 2)),
            Member::ok(report("e1", "edge", 0.0, 4, 0)),
        ];
        let o = overview(&ms, 7, 5);
        assert_eq!(o.events_in_per_sec, 150.0);
        assert_eq!(o.events_out_per_sec, 300.0);
        assert_eq!(o.bytes_in_per_sec, 15_000.0);
        assert_eq!(o.consumers, 7);
        assert_eq!(o.hosts_connected, 5);
        assert_eq!(o.hosts_total, 5 + 3);
        assert_eq!(o.rejects_per_sec, 1.5);
        assert_eq!(o.open_cases, 7);
        assert_eq!(o.last_seq, 100);
        assert_eq!(o.stream_events_per_sec, 200.0);
        // the worst node's latency, not a sum
        assert_eq!(o.time_to_firehose_p99_ms, 100.0);
        assert_eq!(o.top_hosts[0].host, "n1-busy");
        assert_eq!(o.top_hosts[1].host, "n2-busy");
        assert_eq!(o.by_node.len(), 3);
        let s: f64 = o.by_node.iter().map(|n| n.events_in_per_sec).sum();
        assert_eq!(s, o.events_in_per_sec);
        let c: u32 = o.by_node.iter().map(|n| n.consumers).sum();
        assert_eq!(c, o.consumers);
        assert_eq!(o.history.events_in, vec![150.0, 150.0]);
        assert_eq!(o.history.ttf_p99_ms, vec![100.0, 100.0]);
        assert_eq!(o.history.rejects[&RejectReason::BadSignature], vec![3.0, 3.0]);
    }

    #[test]
    fn a_stale_node_is_listed_but_not_summed() {
        let ms = vec![
            Member::ok(report("n1", "core", 100.0, 2, 3)),
            Member::stale("n2", "core", "connection refused".into(), 900),
        ];
        let o = overview(&ms, 0, 5);
        assert_eq!(o.events_in_per_sec, 100.0);
        assert_eq!(o.consumers, 2);
        assert_eq!(o.hosts_total, 4);
        let n2 = o.by_node.iter().find(|n| n.node == "n2").unwrap();
        assert!(n2.stale);
        assert_eq!(n2.events_in_per_sec, 0.0);
        assert_eq!(n2.error.as_deref(), Some("connection refused"));
        let s: f64 = o.by_node.iter().map(|n| n.events_in_per_sec).sum();
        assert_eq!(s, o.events_in_per_sec);

        let mut v = ClusterView {
            nodes: vec![lease_row("n1"), lease_row("n2")],
            host_shards: vec![],
            did_shards: vec![],
            last_seq: 0,
        };
        fill_cluster(&mut v, &ms);
        let n2 = v.nodes.iter().find(|n| n.id == "n2").unwrap();
        assert!(n2.stale && !n2.reachable);
        assert_eq!((n2.consumers, n2.events_in_per_sec, n2.reported_ms), (0, 0.0, 900));
        let n1 = v.nodes.iter().find(|n| n.id == "n1").unwrap();
        assert_eq!((n1.consumers, n1.hosts, n1.events_in_per_sec), (2, 4, 100.0));
        assert!(!n1.stale && n1.reachable);
    }

    fn lease_row(id: &str) -> NodeView {
        NodeView {
            id: id.into(),
            addr: format!("https://{id}"),
            version: "1".into(),
            rev: "r".into(),
            reachable: true,
            lease_valid: true,
            lease_expires_ms: 0,
            host_shards: 1,
            did_shards: 1,
            hosts: 0,
            consumers: 0,
            events_in_per_sec: 0.0,
            events_out_per_sec: 0.0,
            log_durability_lag_ms: 0.0,
            cpu: 0.0,
            mem_bytes: 0,
            role: String::new(),
            stale: false,
            error: None,
            reported_ms: 0,
            bytes_out_per_sec: 0.0,
            stream_seq: 0,
        }
    }

    #[test]
    fn followers_without_a_lease_get_rows() {
        let ms = vec![
            Member::ok(report("n1", "core", 10.0, 0, 1)),
            Member::ok(report("edge", "edge", 0.0, 5, 0)),
            Member::stale("replica", "replica", "timed out".into(), 0),
        ];
        let mut v = ClusterView { nodes: vec![lease_row("n1")], host_shards: vec![], did_shards: vec![], last_seq: 0 };
        fill_cluster(&mut v, &ms);
        assert_eq!(v.nodes.len(), 3);
        let e = v.nodes.iter().find(|n| n.id == "edge").unwrap();
        assert_eq!((e.role.as_str(), e.consumers, e.stale), ("edge", 5, false));
        let r = v.nodes.iter().find(|n| n.id == "replica").unwrap();
        assert!(r.stale);
        assert_eq!(r.role, "replica");
    }

    #[test]
    fn history_aligns_by_time_and_keeps_the_window() {
        let a = hist(100, &[1.0, 2.0, 3.0], &[5.0, 5.0, 5.0]);
        let b = hist(101, &[10.0, 20.0, 30.0], &[1.0, 9.0, 1.0]);
        let h = merge_history(&[&a, &b]);
        // the union is 100..=103, but each node keeps 3 samples
        assert_eq!(h.t, vec![101, 102, 103]);
        assert_eq!(h.events_in, vec![12.0, 23.0, 30.0]);
        assert_eq!(h.ttf_p99_ms, vec![5.0, 9.0, 1.0]);
    }

    #[test]
    fn hosts_come_from_their_owner() {
        let registry = vec![row("a", 9.0), row("b", 9.0), row("c", 9.0)];
        let owned = vec![("n1".to_string(), vec![row("a", 5.0)]), ("n2".to_string(), vec![row("b", 7.0)])];
        let owner = |h: &str| Some(if h == "a" { "n1" } else { "n2" }.to_string());
        let rows = merge_hosts(registry, &owned, owner);
        assert_eq!(rows.len(), 3);
        let get = |h: &str| rows.iter().find(|r| r.host == h).unwrap();
        assert_eq!((get("a").events_per_sec, get("a").node.as_str()), (5.0, "n1"));
        assert_eq!((get("b").events_per_sec, get("b").node.as_str()), (7.0, "n2"));
        // its owner didn't report it: the registry row, with no rate
        assert_eq!((get("c").events_per_sec, get("c").node.as_str()), (0.0, "n2"));
    }

    #[test]
    fn seq_checkpoints_agree_or_not() {
        let mut a = report("n1", "core", 1.0, 0, 0);
        a.seq_checkpoints = vec![(10 << 8, 5), (20 << 8, 9)];
        let mut b = report("n2", "core", 1.0, 0, 0);
        b.seq_checkpoints = vec![(20 << 8, 9), (30 << 8, 12)];
        let v = seq_view(&[Member::ok(a.clone()), Member::ok(b.clone())], 10);
        assert!(v.agree);
        assert_eq!(v.boundaries.len(), 3);
        assert_eq!(v.boundaries[0].key, 30 << 8);
        assert_eq!(v.boundaries[1].seqs.len(), 2);
        assert_eq!(v.nodes[1].latest, Some(seq_pair(30 << 8, 12)));

        b.seq_checkpoints = vec![(20 << 8, 8)];
        let v = seq_view(&[Member::ok(a), Member::ok(b), Member::stale("n3", "core", "down".into(), 0)], 10);
        assert!(!v.agree);
        assert!(!v.boundaries.iter().find(|x| x.key == 20 << 8).unwrap().agree);
        assert!(v.nodes[2].stale);
    }

    #[test]
    fn archive_and_plc_sum_cores_and_take_the_leader() {
        let mut a = report("n1", "core", 1.0, 0, 0);
        a.archive = Some(ArchiveReport {
            mode: "all".into(),
            policy_version: 3,
            counts: ArchiveCounts { mirrored: 10, queued: 2, failed: 1, ..Default::default() },
            errors: vec![("did:a".into(), "boom".into())],
        });
        a.plc = Some(PlcReport { leader: true, ops: 500, ops_per_sec: 50.0, throttled: 2, ..Default::default() });
        let mut b = report("n2", "core", 1.0, 0, 0);
        b.archive = Some(ArchiveReport {
            mode: "all".into(),
            policy_version: 3,
            counts: ArchiveCounts { mirrored: 5, running: 1, ..Default::default() },
            errors: vec![],
        });
        b.plc = Some(PlcReport { leader: false, ops: 40, throttled: 1, ..Default::default() });
        let ms = [Member::ok(a), Member::ok(b), Member::stale("n3", "core", "down".into(), 0)];
        let v = archive_view(&ms).unwrap();
        assert_eq!((v.totals.mirrored, v.totals.queued, v.totals.running, v.totals.failed), (15, 2, 1, 1));
        assert_eq!(v.nodes.len(), 3);
        assert!(v.nodes[2].stale);
        assert_eq!(v.errors[0].node, "n1");
        let p = plc_view(&ms);
        assert!(p.enabled);
        assert_eq!(p.leader.as_deref(), Some("n1"));
        assert_eq!((p.ops, p.ops_per_sec, p.throttled), (500, 50.0, 3));
        assert_eq!(p.nodes.len(), 3);
    }

    #[test]
    fn pipeline_lists_cores_and_their_busiest_hosts() {
        let mut a = report("n1", "core", 1.0, 0, 0);
        a.pipeline = PipelineNode { ack_pending: 7, ..Default::default() };
        a.pipeline_hosts = vec![
            PipelineHost { host: "x".into(), inflight: 3, ..Default::default() },
            PipelineHost { host: "y".into(), inflight: 9, ..Default::default() },
        ];
        let ms =
            [Member::ok(a), Member::ok(report("e", "edge", 0.0, 1, 0)), Member::stale("n2", "core", "x".into(), 0)];
        let v = pipeline_view(&ms, 1);
        assert_eq!(v.nodes.len(), 2);
        assert_eq!((v.nodes[0].node.as_str(), v.nodes[0].ack_pending), ("n1", 7));
        assert!(v.nodes[1].stale);
        assert_eq!(v.hosts.len(), 1);
        assert_eq!((v.hosts[0].host.as_str(), v.hosts[0].node.as_str()), ("y", "n1"));
    }
}
