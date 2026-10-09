//! Where a host's socket starts on the quorum log: a new host at its live
//! head, a saved cursor where it left off, a backfill from 0 when an
//! operator asks, and a restarted sequence from 0 for as long as the
//! leader's highest cursor is the old sequence's.

use super::aliases::tests::{pds::Pds, until};
use super::{Node, NodeConfig};
use crate::admin::HostAction;
use crate::types::Host;
use crate::verify::synth::{Curve, Repo, Signer};
use std::sync::Arc;
use std::time::Duration;

const DID: &str = "did:plc:startstartstartstartaaaa";

async fn pds() -> (Arc<Pds>, Host) {
    let pds = Pds::start(vec![(Repo::new(DID, Signer::new(Curve::K256, 21), 3), "127.0.0.1")]).await;
    let host = Host(format!("127.0.0.1:{}", pds.port));
    (pds, host)
}

async fn node(host: &Host, backfill: bool) -> Arc<Node> {
    let store = vlsync_store::store::Store::memory(None);
    let mut cfg = NodeConfig::new(&format!("http://{}", host.0));
    cfg.node_id = "n1".into();
    cfg.dev_mode = true;
    cfg.lanes = 2;
    cfg.ingest_threads = 2;
    cfg.serve_threads = 1;
    cfg.hosts = vec![format!("http://{}", host.0)];
    cfg.backfill_new_hosts = backfill;
    let live: Arc<dyn crate::policy::LiveNodes> = Arc::new(crate::policy::FixedNodes::new(1));
    cfg.policy = Some(super::policy::PolicyEngine(crate::policy::Engine::new(store.clone(), "n1", live)));
    let mut q = super::quorum::QuorumSetup::new(&format!("127.0.0.1:{}", crate::qlog::tests::free_port()));
    q.host_poll = Duration::from_millis(100);
    q.cursor_every = Duration::from_millis(100);
    q.flush = Duration::from_millis(300);
    q.retain_horizon = None;
    Node::start(store, cfg, q).await.unwrap()
}

/// The cursor of the `n`th subscription (from 0).
async fn sub(pds: &Pds, n: usize) -> Option<i64> {
    until("the subscription", 30, || pds.subs.lock().len() > n).await;
    pds.subs.lock()[n].1
}

async fn passed(node: &Node, seqs: std::ops::RangeInclusive<i64>) {
    until("the events", 30, || {
        let p = node.passed.lock();
        seqs.clone().all(|s| p.iter().any(|n| n.upstream_seq == s))
    })
    .await;
}

/// The leader's committed cursor for the host, once this node's copy of
/// its table has it. A cursor commits riding on the next event appended.
async fn acked(node: &Node, host: &Host, want: i64) {
    until("the ack", 30, || node.manager.registry().get(host).and_then(|e| e.acked_seq()) == Some(want)).await;
}

/// Long enough for this node to have sent its acked cursors to the leader
/// (every `cursor_every`), where they wait for an event to ride on.
async fn sent() {
    tokio::time::sleep(Duration::from_millis(500)).await;
}

async fn committed(node: &Node, host: &Host, want: u64) {
    until("the committed cursor", 30, || node.quorum.hosts.committed_cursor(&host.0) == Some(want)).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_new_host_starts_at_its_head_and_resumes_from_its_cursor() {
    let (pds, host) = pds().await;
    for _ in 0..3 {
        pds.commit(0);
    }
    let node = node(&host, false).await;
    assert_eq!(sub(&pds, 0).await, None, "no cursor: the live head");
    pds.commit(0);
    pds.commit(0);
    passed(&node, 4..=5).await;
    assert!(!node.passed.lock().iter().any(|n| n.upstream_seq <= 3), "nothing from before the head");
    acked(&node, &host, 5).await;

    node.manager.kick(&host);
    assert_eq!(sub(&pds, 1).await, Some(5), "a saved cursor is used");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn backfill_new_hosts_reads_a_new_host_from_zero() {
    let (pds, host) = pds().await;
    for _ in 0..3 {
        pds.commit(0);
    }
    let node = node(&host, true).await;
    assert_eq!(sub(&pds, 0).await, Some(0));
    passed(&node, 1..=3).await;
}

/// An operator's `set-backfill` on a host that started at its head and has
/// read nothing reconnects it from 0. On a host with a saved cursor it
/// changes nothing, and the trail says why.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_per_host_backfill_reconnects_a_host_without_a_cursor() {
    let (pds, host) = pds().await;
    for _ in 0..3 {
        pds.commit(0);
    }
    let node = node(&host, false).await;
    let hooks = node.policy.clone().unwrap();
    assert_eq!(sub(&pds, 0).await, None);
    until("the socket", 30, || node.manager.is_running(&host)).await;

    hooks.admin.host_action(&host.0, HostAction::SetBackfill { backfill: Some(true) }, "op").await.unwrap();
    assert!(node.quorum.settle_host(&host.0, Duration::from_secs(10)).await);
    hooks.refresh_host(&host.0).await.unwrap();
    assert_eq!(sub(&pds, 1).await, Some(0), "reconnected from the start");
    passed(&node, 1..=3).await;
    acked(&node, &host, 3).await;
    sent().await;
    pds.commit(0);
    passed(&node, 4..=4).await;
    acked(&node, &host, 4).await;
    committed(&node, &host, 3).await;

    let rec = hooks.admin.host_action(&host.0, HostAction::SetBackfill { backfill: Some(true) }, "op").await.unwrap();
    let last = crate::policy::admin::PolicyAdmin::host_actions(&rec).pop().unwrap();
    assert!(last.reason.as_deref().is_some_and(|r| r.starts_with("keeps its saved cursor")), "{:?}", last.reason);
    hooks.refresh_host(&host.0).await.unwrap();
    node.manager.kick(&host);
    assert_eq!(sub(&pds, 2).await, Some(4));
}

/// The leader keeps the highest cursor it was sent. After a PDS's sequence
/// restarts, that's the old sequence's, and resuming from it would get
/// FutureCursor again on every reconnect.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_restarted_sequence_resumes_from_its_own_acks() {
    let (pds, host) = pds().await;
    let node = node(&host, false).await;
    assert_eq!(sub(&pds, 0).await, None);
    until("the socket", 30, || node.manager.is_running(&host)).await;
    for _ in 0..5 {
        pds.commit(0);
    }
    passed(&node, 1..=5).await;
    acked(&node, &host, 5).await;
    sent().await;

    pds.restart_sequence();
    pds.commit(0);
    pds.commit(0);
    node.manager.kick(&host);
    assert_eq!(sub(&pds, 1).await, Some(5));
    assert_eq!(sub(&pds, 2).await, Some(0), "FutureCursor: the new sequence from its start");
    acked(&node, &host, 2).await;
    // the old sequence's 5 rode on the new sequence's first event
    committed(&node, &host, 5).await;
    node.manager.kick(&host);
    assert_eq!(sub(&pds, 3).await, Some(2), "this node's acks, not the old sequence's committed cursor");
}
