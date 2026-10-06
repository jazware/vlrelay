//! The dashboard's numbers from node reports. Each node reports its own
//! ([`NodeReport`]) and the dashboard's views add the reports up here; a
//! node's dashboard has its own report. A node that didn't
//! answer is a [`Member`] with no report: it's listed as stale with zeros,
//! and left out of every sum, so the totals always equal the sum of the
//! rows shown beside them.

use super::{
    History, HostRow, HostStatus, NodeTotals, Overview, PipelineHost, PipelineNode, PipelineView, PlcNode, PlcView,
    PlcWindow, RejectReason,
};
use serde::{Deserialize, Serialize};
use std::collections::{BTreeMap, BTreeSet, HashMap};

/// One node's live numbers, as it measured them.
#[derive(Clone, Debug, Default, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct NodeReport {
    pub node: String,
    /// `leader`, `follower` or `candidate` (the quorum log's role).
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
    pub commit_lag_ms: f64,
    /// The last seq its merged stream emitted.
    pub stream_seq: i64,
    pub cpu: f64,
    pub mem_bytes: u64,
    pub top_hosts: Vec<HostRow>,
    pub history: History,
    pub pipeline: PipelineNode,
    pub pipeline_hosts: Vec<PipelineHost>,
    pub plc: Option<PlcReport>,
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
        commit_lag_ms: max(|r| r.commit_lag_ms),
        last_seq: reports.iter().map(|r| r.stream_seq).max().unwrap_or(0),
        open_cases,
        top_hosts: top,
        history: merge_history(&reports.iter().map(|r| &r.history).collect::<Vec<_>>()),
        stream_events_per_sec: max(|r| r.events_out_per_sec),
        by_node: members.iter().map(totals).collect(),
    }
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
            history: Vec::new(),
            throttled_accounts: 0,
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
    fn plc_takes_the_leaders_counters() {
        let mut a = report("n1", "core", 1.0, 0, 0);
        a.plc = Some(PlcReport { leader: true, ops: 500, ops_per_sec: 50.0, throttled: 2, ..Default::default() });
        let mut b = report("n2", "core", 1.0, 0, 0);
        b.plc = Some(PlcReport { leader: false, ops: 40, throttled: 1, ..Default::default() });
        let ms = [Member::ok(a), Member::ok(b), Member::stale("n3", "core", "down".into(), 0)];
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
