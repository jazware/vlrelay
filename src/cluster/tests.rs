//! Three in-process nodes on one in-memory bucket, talking peer mTLS on
//! localhost, as vlpds's cluster tests do.

use super::forward::{DidStage, Forwarded, Outcome, StageError, StageResult};
use super::hosts::HostHandler;
use super::*;
use crate::seq::{Event, EventMeta, SeqSplice};
use bytes::Bytes;
use futures::StreamExt;
use std::collections::{BTreeMap, BTreeSet};
use std::net::SocketAddr;
use tokio_tungstenite::tungstenite::Message;

struct Ca(vlpds::peer_tls::Issued);

impl Ca {
    fn new() -> Ca {
        Ca(vlpds::peer_tls::create_ca("test CA", 2).unwrap())
    }
    fn node(&self, id: &str) -> Arc<vlpds::peer_tls::PeerTls> {
        let n = vlpds::peer_tls::issue_node(&self.0.cert_pem, &self.0.key_pem, id, &["127.0.0.1".into()], 2).unwrap();
        vlpds::peer_tls::PeerTls::from_pem(&self.0.cert_pem, &n.cert_pem, &n.key_pem).unwrap()
    }
}

/// Stands in for durable per-DID state across the cluster: an event is
/// recorded once its log entry is durable, and a replay of a recorded one
/// is a duplicate (what `check_chain` does with the same commit at the
/// current rev).
#[derive(Default)]
struct Applied(Mutex<HashMap<(String, i64), i64>>);

struct LogStage {
    node: Weak<ClusterNode>,
    applied: Arc<Applied>,
}

#[async_trait::async_trait]
impl DidStage for LogStage {
    async fn apply(&self, batch: Vec<Forwarded>) -> Vec<StageResult> {
        let Some(n) = self.node.upgrade() else {
            return batch.iter().map(|_| Err(StageError::Unavailable("gone".into()))).collect();
        };
        let (log, layout) = (n.log.clone().unwrap(), n.layout().unwrap());
        let mut out: Vec<Option<StageResult>> = vec![None; batch.len()];
        let mut events = Vec::new();
        let mut idx = Vec::new();
        for (i, f) in batch.iter().enumerate() {
            if self.applied.0.lock().contains_key(&(f.did.clone(), f.upstream_seq)) {
                out[i] = Some(Ok(Outcome::Duplicate));
                continue;
            }
            idx.push(i);
            events.push(Event {
                meta: EventMeta {
                    did: f.did.clone(),
                    host: f.host.clone(),
                    upstream_seq: f.upstream_seq,
                    shard: layout.shard_of(&f.did).0,
                },
                frame: Box::new(SeqSplice::parse(f.frame.clone()).unwrap()),
                delta: None,
            });
        }
        if !events.is_empty() {
            match log.append(events).await {
                Ok(d) => {
                    for (i, seq) in idx.into_iter().zip(d.seqs) {
                        let f = &batch[i];
                        self.applied.0.lock().insert((f.did.clone(), f.upstream_seq), seq);
                        out[i] = Some(Ok(Outcome::Appended(seq)));
                    }
                }
                Err(e) => {
                    for i in idx {
                        out[i] = Some(Err(StageError::Unavailable(e.to_string())));
                    }
                }
            }
        }
        out.into_iter().map(|r| r.unwrap()).collect()
    }
}

/// An upstream side that subscribes to whatever the filter passes and
/// reports the acked cursors the test sets.
#[derive(Default)]
struct FakeHosts {
    acked: Mutex<HashMap<Host, i64>>,
    released: Mutex<Vec<Host>>,
    /// Sockets open, as `follow` (the manager's filter-follower) leaves them.
    running: Mutex<BTreeSet<Host>>,
}

impl FakeHosts {
    /// Stops the hosts each published filter drops, as
    /// `upstream::Manager::follow_filter` does, racing `release`.
    fn follow(self: &Arc<Self>, mut rx: watch::Receiver<HostFilter>) {
        let me = Arc::downgrade(self);
        tokio::spawn(async move {
            while rx.changed().await.is_ok() {
                let f = rx.borrow_and_update().clone();
                let Some(me) = me.upgrade() else { return };
                me.running.lock().retain(|h| f(h));
            }
        });
    }
}

#[async_trait::async_trait]
impl HostHandler for FakeHosts {
    async fn release(&self, give: HostFilter) -> Vec<(Host, i64)> {
        // the follower has stopped them by now
        tokio::time::sleep(Duration::from_millis(50)).await;
        self.running.lock().retain(|h| !give(h));
        let acked = self.acked.lock().clone();
        let mut out = Vec::new();
        for (h, s) in acked {
            if give(&h) {
                self.released.lock().push(h.clone());
                out.push((h, s));
            }
        }
        out
    }
    fn acked(&self) -> Vec<(Host, i64)> {
        self.acked.lock().iter().map(|(h, s)| (h.clone(), *s)).collect()
    }
}

struct TNode {
    node: Arc<ClusterNode>,
    public: SocketAddr,
    hosts: Arc<FakeHosts>,
}

fn opts(ca: &Ca, id: &str, role: Role, addr: &str) -> ClusterOptions {
    let mut o = ClusterOptions::new(id, role, addr);
    o.did_shards = 8;
    o.host_shards = 8;
    o.ttl = Duration::from_millis(1500);
    o.renew_every = Duration::from_millis(200);
    o.skew = Duration::from_millis(200);
    o.host_step = Duration::from_millis(200);
    o.checkpoint_every = Duration::from_millis(100);
    o.log.linger = Duration::from_millis(5);
    o.log.idle_heartbeat = Some(Duration::from_millis(50));
    o.serve.retention_interval = Duration::ZERO;
    o.serve.threads = 1;
    o.serve.ring_bytes = 64 << 20;
    o.serve.takedown_poll = Duration::from_millis(100);
    o.internal_token = "test-token".into();
    o.poll = Duration::from_millis(50);
    o.guard = Duration::from_millis(100);
    if role != Role::Replica {
        o.tls = Some(ca.node(id));
    }
    o
}

async fn spawn(store: &Store, ca: &Ca, id: &str, role: Role, applied: &Arc<Applied>) -> TNode {
    spawn_at(store, ca, id, role, applied, None, None).await
}

/// `advertise`: where peers are told to reach it (default: its listener).
async fn spawn_at(
    store: &Store,
    ca: &Ca,
    id: &str,
    role: Role,
    applied: &Arc<Applied>,
    peer: Option<tokio::net::TcpListener>,
    advertise: Option<String>,
) -> TNode {
    let peer = match peer {
        Some(l) => l,
        None => tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap(),
    };
    let addr = advertise.unwrap_or_else(|| format!("https://{}", peer.local_addr().unwrap()));
    let node = ClusterNode::start(opts(ca, id, role, &addr), store.clone()).await.unwrap();
    node.set_stage(Arc::new(LogStage { node: Arc::downgrade(&node), applied: applied.clone() }));
    let hosts = Arc::new(FakeHosts::default());
    node.set_host_handler(hosts.clone());
    if role != Role::Replica {
        peer::spawn_listener(&node, peer).unwrap();
    }
    let l = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let public = l.local_addr().unwrap();
    let app = node.serve.router();
    tokio::spawn(async move {
        let _ = axum::serve(l, app.into_make_service_with_connect_info::<SocketAddr>()).await;
    });
    node.run();
    TNode { node, public, hosts }
}

async fn eventually(what: &str, max: Duration, mut f: impl FnMut() -> bool) {
    let deadline = Instant::now() + max;
    while !f() {
        assert!(Instant::now() < deadline, "timed out waiting for: {what}");
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
}

fn did(i: usize) -> String {
    format!("did:plc:{i:024}")
}

fn frame(did: &str, upstream_seq: i64) -> Bytes {
    let f = vlpds::events::sync_frame(did, "3jzfcijpj2z2a", &[0xab; 64], "2026-10-04T00:00:00.000Z");
    let mut raw = Vec::new();
    f.finish(upstream_seq, &mut raw);
    Bytes::from(raw)
}

fn fwd(i: usize, upstream_seq: i64) -> Forwarded {
    let d = did(i);
    Forwarded {
        frame: frame(&d, upstream_seq),
        did: d,
        host: Host(format!("pds{}.test", i % 5)),
        upstream_seq,
        meta: Bytes::new(),
    }
}

/// DID and host shards each owned by exactly one live node, every live
/// node holding some.
fn spread(nodes: &[&TNode]) -> bool {
    let did_total = nodes[0].node.layout().unwrap().shards.len();
    let host_total = nodes[0].node.hosts.as_ref().unwrap().layout().shards.len();
    let mut dids = BTreeSet::new();
    let mut hosts = BTreeSet::new();
    for n in nodes {
        let c = n.node.cluster.as_ref().unwrap();
        let o = c.owned();
        let h = n.node.hosts.as_ref().unwrap().owned();
        if o.is_empty() || h.is_empty() {
            return false;
        }
        for s in o {
            if !dids.insert(s) {
                return false;
            }
        }
        for s in h {
            if !hosts.insert(s) {
                return false;
            }
        }
    }
    dids.len() == did_total && hosts.len() == host_total
}

async fn read_stream(addr: SocketAddr, cursor: i64, n: usize, max: Duration) -> Vec<(i64, Vec<u8>)> {
    let url = format!("ws://{addr}/xrpc/com.atproto.sync.subscribeRepos?cursor={cursor}");
    let (mut ws, _) = tokio_tungstenite::connect_async(url).await.unwrap();
    let mut out = Vec::new();
    let deadline = tokio::time::Instant::now() + max;
    while out.len() < n {
        match tokio::time::timeout_at(deadline, ws.next()).await {
            Ok(Some(Ok(Message::Binary(b)))) => {
                let seq = crate::seq::frame_seq(&b)
                    .unwrap_or_else(|| panic!("{addr} cursor {cursor}: {}", String::from_utf8_lossy(&b)));
                out.push((seq, b.to_vec()));
            }
            Ok(Some(Ok(_))) => {}
            _ => break,
        }
    }
    out
}

async fn forward_all(n: &ClusterNode, evs: Vec<Forwarded>) -> Vec<Outcome> {
    let waits = futures::future::join_all(evs.into_iter().map(|e| async move { n.forward(e).await })).await;
    waits.into_iter().map(|r| r.expect("forwarded")).collect()
}

/// A frame with its seq zeroed: the same for an upstream frame and the
/// relay's copy of it.
fn norm(f: &[u8]) -> Vec<u8> {
    let r = crate::seq::find_seq(f).expect("a seq");
    [&f[..r.start], &[0u8][..], &f[r.end..]].concat()
}

/// Forwards `evs` and returns the appended ones as (log key, normalized frame).
async fn forward_keyed(n: &ClusterNode, evs: Vec<Forwarded>) -> Vec<(i64, Vec<u8>)> {
    let frames: Vec<Vec<u8>> = evs.iter().map(|e| norm(&e.frame)).collect();
    let o = forward_all(n, evs).await;
    o.iter()
        .zip(frames)
        .filter_map(|(o, f)| match o {
            Outcome::Appended(k) => Some((*k, f)),
            _ => None,
        })
        .collect()
}

/// A stream from cursor `start` holds exactly the appended events, in key
/// order, numbered `start + 1`, `start + 2`, ...
fn assert_stream(got: &[(i64, Vec<u8>)], appended: &[(i64, Vec<u8>)], start: i64, what: &str) {
    let mut want = appended.to_vec();
    want.sort();
    let seqs: Vec<i64> = got.iter().map(|e| e.0).collect();
    assert_eq!(seqs, (start + 1..=start + want.len() as i64).collect::<Vec<_>>(), "{what}: dense seqs");
    let frames: Vec<Vec<u8>> = got.iter().map(|e| norm(&e.1)).collect();
    let want: Vec<Vec<u8>> = want.into_iter().map(|e| e.1).collect();
    assert!(frames == want, "{what}: the appended events in key order, no gaps or duplicates");
}

async fn three(store: &Store, ca: &Ca, applied: &Arc<Applied>) -> Vec<TNode> {
    let a = spawn(store, ca, "node-a", Role::Core, applied).await;
    let b = spawn(store, ca, "node-b", Role::Core, applied).await;
    let c = spawn(store, ca, "node-c", Role::Core, applied).await;
    let v = vec![a, b, c];
    let refs: Vec<&TNode> = v.iter().collect();
    eventually("shards spread over three nodes", Duration::from_secs(20), || spread(&refs)).await;
    v
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn shards_spread_and_every_node_serves_the_same_bytes() {
    let (store, ca, applied) = (Store::memory(None), Ca::new(), Arc::new(Applied::default()));
    let nodes = three(&store, &ca, &applied).await;
    let start = 0;
    // every node a host owner for some events: forwards cross every pair
    let mut seqs = Vec::new();
    for round in 0..3 {
        let jobs = nodes.iter().enumerate().map(|(k, n)| {
            let evs: Vec<Forwarded> = (0..40).map(|i| fwd(k * 1000 + i, round * 100 + i as i64)).collect();
            forward_keyed(&n.node, evs)
        });
        for o in futures::future::join_all(jobs).await {
            seqs.extend(o);
        }
    }
    assert_eq!(seqs.len(), 360);
    // each landed on the log of its DID's owner
    let mut by_log: BTreeMap<String, usize> = BTreeMap::new();
    for n in &nodes {
        let log = n.node.log.as_ref().unwrap();
        by_log.insert(log.log_id.to_string(), log.stats.events.load(Ordering::Relaxed) as usize);
    }
    assert!(by_log.values().all(|&c| c > 0), "{by_log:?}");
    assert_eq!(by_log.values().sum::<usize>(), 360);
    let streams = futures::future::join_all(
        nodes.iter().map(|n| read_stream(n.public, start, seqs.len(), Duration::from_secs(10))),
    )
    .await;
    for s in &streams {
        assert_stream(s, &seqs, start, "stream");
    }
    assert_eq!(streams[0], streams[1]);
    assert_eq!(streams[0], streams[2]);
    // and from a cursor in the middle
    let mid = streams[0][seqs.len() / 2].0;
    let tails = futures::future::join_all(
        nodes.iter().map(|n| read_stream(n.public, mid, seqs.len() / 2 - 1, Duration::from_secs(10))),
    )
    .await;
    assert_eq!(tails[0], tails[1]);
    assert_eq!(tails[0], tails[2]);
    assert_eq!(tails[0].first().map(|e| e.0), Some(mid + 1));
    assert_eq!(tails[0][..], streams[0][seqs.len() / 2 + 1..]);
    for n in &nodes {
        n.node.shutdown().await.unwrap();
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_crash_moves_its_shards_fences_only_its_log_and_consumers_see_no_gap() {
    let (store, ca, applied) = (Store::memory(None), Ca::new(), Arc::new(Applied::default()));
    let nodes = three(&store, &ca, &applied).await;
    let start = 0;
    // a consumer on a survivor from before the crash to after it
    let watcher = {
        let addr = nodes[0].public;
        tokio::spawn(async move { read_stream(addr, start, 400, Duration::from_secs(30)).await })
    };
    let mut seqs = forward_keyed(&nodes[0].node, (0..100).map(|i| fwd(i, i as i64)).collect()).await;
    // host cursors c acked, checkpointed before the crash
    let c = &nodes[2];
    let c_hosts: Vec<Host> = (0..50).map(|i| Host(format!("h{i}.test"))).filter(|h| c.node.owns_host(h)).collect();
    assert!(!c_hosts.is_empty());
    for (i, h) in c_hosts.iter().enumerate() {
        c.hosts.acked.lock().insert(h.clone(), 1000 + i as i64);
    }
    c.node.hosts.as_ref().unwrap().checkpoint().await.unwrap();
    let logs: Vec<String> = nodes.iter().map(|n| n.node.log.as_ref().unwrap().log_id.to_string()).collect();
    let dead_dids = c.node.cluster.as_ref().unwrap().owned();
    c.node.halt();
    let crashed = Instant::now();
    // forwards to its DIDs during the takeover wait, then land at the new owner
    let during = {
        let a = nodes[0].node.clone();
        tokio::spawn(async move { forward_keyed(&a, (100..200).map(|i| fwd(i, i as i64)).collect()).await })
    };
    let survivors = [&nodes[0], &nodes[1]];
    eventually("survivors own every shard", Duration::from_secs(15), || spread(&survivors)).await;
    let took = crashed.elapsed();
    eprintln!("crash takeover: {took:?}");
    assert!(took >= Duration::from_millis(1500), "taken over before the lease lapsed: {took:?}");
    seqs.extend(during.await.unwrap());
    // a one-node start on a cluster's prefix would fence live logs: refused
    let single = crate::serve::start_single_node(
        store.clone(),
        crate::seq::LogConfig::new("single"),
        Default::default(),
        None,
        None,
    )
    .await;
    assert!(single.is_err());
    // only the dead node's log is fenced
    let fenced = |log: &str| {
        let (s, l) = (store.clone(), log.to_string());
        async move { vlpds::nodelog::first_free(&s, &l).await.unwrap().1 }
    };
    assert!(fenced(&logs[2]).await, "the dead log is fenced");
    assert!(!fenced(&logs[0]).await && !fenced(&logs[1]).await, "live logs are not");
    for s in &dead_dids {
        assert!(nodes.iter().take(2).any(|n| n.node.cluster.as_ref().unwrap().is_owner(*s)), "shard {s} moved");
    }
    // a replay of events already durable is absorbed as duplicates
    let replay = forward_all(&nodes[1].node, (0..50).map(|i| fwd(i, i as i64)).collect()).await;
    assert!(replay.iter().all(|o| *o == Outcome::Duplicate), "{replay:?}");
    seqs.extend(forward_keyed(&nodes[1].node, (200..400).map(|i| fwd(i, i as i64)).collect()).await);
    assert_eq!(seqs.len(), 400);
    // upstreams resume from c's checkpoints at their new owners
    for (i, h) in c_hosts.iter().enumerate() {
        let owner = nodes.iter().take(2).find(|n| n.node.owns_host(h)).expect("a survivor owns it");
        let cs = owner.node.cursor_source().unwrap();
        assert_eq!(crate::upstream::CursorSource::durable_cursor(&*cs, h), Some(1000 + i as i64));
    }
    let got = watcher.await.unwrap();
    assert_stream(&got, &seqs, start, "a consumer across the crash");
    // a consumer arriving now at either survivor sees the same bytes
    let s0 = read_stream(nodes[0].public, start, 400, Duration::from_secs(10)).await;
    let s1 = read_stream(nodes[1].public, start, 400, Duration::from_secs(10)).await;
    assert_eq!(s0, got);
    assert_eq!(s1, got);
    for n in &nodes[..2] {
        n.node.shutdown().await.unwrap();
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_planned_handoff_checkpoints_and_pauses_under_a_second() {
    let (store, ca, applied) = (Store::memory(None), Ca::new(), Arc::new(Applied::default()));
    let nodes = three(&store, &ca, &applied).await;
    let start = 0;
    let c = &nodes[2];
    let c_hosts: Vec<Host> = (0..50).map(|i| Host(format!("h{i}.test"))).filter(|h| c.node.owns_host(h)).collect();
    for (i, h) in c_hosts.iter().enumerate() {
        c.hosts.acked.lock().insert(h.clone(), 5000 + i as i64);
        c.hosts.running.lock().insert(h.clone());
    }
    c.hosts.follow(c.node.host_filter().unwrap());
    // steady traffic through a survivor while c leaves
    let stop = Arc::new(AtomicBool::new(false));
    let load = {
        let (a, stop) = (nodes[0].node.clone(), stop.clone());
        tokio::spawn(async move {
            let mut out = Vec::new();
            let mut i = 0usize;
            while !stop.load(Ordering::Acquire) {
                out.extend(forward_keyed(&a, (i..i + 5).map(|k| fwd(k, k as i64)).collect()).await);
                i += 5;
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
            out
        })
    };
    let watcher = {
        let addr = nodes[1].public;
        tokio::spawn(async move {
            let url = format!("ws://{addr}/xrpc/com.atproto.sync.subscribeRepos?cursor={start}");
            let (mut ws, _) = tokio_tungstenite::connect_async(url).await.unwrap();
            let mut arrivals = Vec::new();
            while let Ok(Some(Ok(m))) = tokio::time::timeout(Duration::from_secs(5), ws.next()).await {
                if let Message::Binary(b) = m {
                    arrivals.push((Instant::now(), (crate::seq::frame_seq(&b).unwrap(), b.to_vec())));
                }
            }
            arrivals
        })
    };
    tokio::time::sleep(Duration::from_millis(500)).await;
    let left = Instant::now();
    c.node.shutdown().await.unwrap();
    let survivors = [&nodes[0], &nodes[1]];
    eventually("survivors own every shard", Duration::from_secs(5), || spread(&survivors)).await;
    let moved = left.elapsed();
    assert!(moved < Duration::from_secs(3), "planned handoff took {moved:?}");
    tokio::time::sleep(Duration::from_millis(500)).await;
    stop.store(true, Ordering::Release);
    let seqs = load.await.unwrap();
    let arrivals = watcher.await.unwrap();
    let got: Vec<(i64, Vec<u8>)> = arrivals.iter().map(|a| a.1.clone()).collect();
    assert_stream(&got, &seqs, start, "a consumer across the handoff");
    let pause = arrivals.windows(2).map(|w| w[1].0 - w[0].0).max().unwrap();
    eprintln!("planned handoff: shards moved in {moved:?}, longest pause {pause:?}");
    assert!(pause < Duration::from_secs(1), "longest pause {pause:?}");
    // c closed its sockets and checkpointed their cursors before handing
    // over, every one of them, though its filter-follower stopped them first
    assert!(c.hosts.running.lock().is_empty());
    let released: BTreeSet<Host> = c.hosts.released.lock().iter().cloned().collect();
    assert_eq!(released, c_hosts.iter().cloned().collect());
    for (i, h) in c_hosts.iter().enumerate() {
        let owner = nodes.iter().take(2).find(|n| n.node.owns_host(h)).expect("a survivor owns it");
        let cs = owner.node.cursor_source().unwrap();
        assert_eq!(crate::upstream::CursorSource::durable_cursor(&*cs, h), Some(5000 + i as i64));
    }
    // c's log is closed for good, the others aren't
    let c_log = c.node.log.as_ref().unwrap().log_id.to_string();
    assert!(vlpds::nodelog::first_free(&store, &c_log).await.unwrap().1);
    for n in &nodes[..2] {
        n.node.shutdown().await.unwrap();
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn edges_and_replicas_serve_the_identical_stream() {
    let (store, ca, applied) = (Store::memory(None), Ca::new(), Arc::new(Applied::default()));
    let a = spawn(&store, &ca, "node-a", Role::Core, &applied).await;
    let b = spawn(&store, &ca, "node-b", Role::Core, &applied).await;
    eventually("shards spread over two nodes", Duration::from_secs(20), || spread(&[&a, &b])).await;
    let edge = spawn(&store, &ca, "edge-1", Role::Edge, &applied).await;
    let replica = spawn(&store, &ca, "replica-1", Role::Replica, &applied).await;
    eventually("edge and replica follow both logs", Duration::from_secs(5), || {
        edge.node.followers.followed().len() == 2 && replica.node.followers.followed().len() == 2
    })
    .await;
    assert!(edge.node.cluster.is_none() && replica.node.cluster.is_none());
    let start = 0;
    let mut seqs = Vec::new();
    for round in 0..10 {
        let o = futures::future::join_all([
            forward_keyed(&a.node, (0..20).map(|i| fwd(i, round * 100 + i as i64)).collect()),
            forward_keyed(&b.node, (20..40).map(|i| fwd(i, round * 100 + i as i64)).collect()),
        ])
        .await;
        for o in o {
            seqs.extend(o);
        }
    }
    let n = seqs.len();
    let reads = futures::future::join_all(
        [&a, &edge, &replica].map(|t| read_stream(t.public, start, n, Duration::from_secs(10))),
    )
    .await;
    assert_stream(&reads[0], &seqs, start, "core");
    assert_eq!(reads[0], reads[1], "edge");
    assert_eq!(reads[0], reads[2], "replica");
    // a replica started after all that numbers from the bucket (seq
    // checkpoints plus a count) and serves it from there (backfill across
    // the heartbeat segments)
    tokio::time::sleep(Duration::from_millis(200)).await;
    let late = spawn(&store, &ca, "replica-2", Role::Replica, &applied).await;
    let head = reads[0].last().unwrap().0;
    eventually("the late replica anchors its seqs", Duration::from_secs(10), || {
        late.node.serve.firehose.last_emitted.load(Ordering::Acquire) == head
    })
    .await;
    let backfilled = read_stream(late.public, start, n, Duration::from_secs(10)).await;
    assert_eq!(reads[0], backfilled, "late replica");
    // the next events get the same seqs everywhere, the late replica included
    let more = forward_keyed(&a.node, (0..20).map(|i| fwd(i, 5000 + i as i64)).collect()).await;
    let tails = futures::future::join_all(
        [&a, &edge, &late].map(|t| read_stream(t.public, head, more.len(), Duration::from_secs(10))),
    )
    .await;
    assert_stream(&tails[0], &more, head, "after the late start");
    assert_eq!(tails[0], tails[1], "edge");
    assert_eq!(tails[0], tails[2], "late replica, live");
    late.node.shutdown().await.unwrap();
    // no lease, no shards, nothing written by the edge or the replica
    let leases = store.raw.list(Some(&object_store::path::Path::from(format!("{}/nodes", store.prefix))));
    let names: Vec<String> =
        leases.map(|m| m.unwrap().location.filename().unwrap().to_string()).collect::<Vec<_>>().await;
    assert_eq!(names.len(), 2, "{names:?}");
    for n in [&edge, &replica] {
        n.node.shutdown().await.unwrap();
    }
    for n in [&a, &b] {
        n.node.shutdown().await.unwrap();
    }
}

/// A core whose log fails (here: fenced under it) is lost at once. Before,
/// the log died but the node lived on holding its shards, and every event
/// for them failed until the forwarder gave up 20 s later (chaos
/// minio-pause: a renewal sent before the lapse kept the lease valid).
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_failed_log_loses_the_node() {
    let (store, ca, applied) = (Store::memory(None), Ca::new(), Arc::new(Applied::default()));
    let n = spawn(&store, &ca, "solo", Role::Core, &applied).await;
    let lost = Arc::new(AtomicBool::new(false));
    let l = lost.clone();
    n.node.on_lost(Box::new(move |_| l.store(true, Ordering::SeqCst)));
    eventually("a layout", Duration::from_secs(10), || n.node.layout().is_some()).await;
    let log = n.node.log.clone().unwrap();
    crate::seq::fence(&store, &log.log_id, "test").await.unwrap();
    let _ = tokio::time::timeout(Duration::from_secs(5), n.node.forward(fwd(1, 1))).await;
    eventually("the node is lost", Duration::from_secs(10), || lost.load(Ordering::SeqCst)).await;
    assert!(n.node.halted());
}

/// A TCP proxy to `to` that, once `open` is cleared, cuts every
/// connection and holds new ones without answering (a blackholed port).
async fn gate_proxy(to: SocketAddr, open: Arc<AtomicBool>) -> String {
    let l = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = format!("https://{}", l.local_addr().unwrap());
    tokio::spawn(async move {
        let conns: Arc<Mutex<Vec<tokio::task::JoinHandle<()>>>> = Arc::default();
        let (o, c) = (open.clone(), conns.clone());
        tokio::spawn(async move {
            while o.load(Ordering::SeqCst) {
                tokio::time::sleep(Duration::from_millis(20)).await;
            }
            for h in c.lock().drain(..) {
                h.abort();
            }
        });
        let mut held = Vec::new();
        while let Ok((mut s, _)) = l.accept().await {
            if !open.load(Ordering::SeqCst) {
                held.push(s);
                continue;
            }
            conns.lock().push(tokio::spawn(async move {
                if let Ok(mut t) = tokio::net::TcpStream::connect(to).await {
                    let _ = tokio::io::copy_bidirectional(&mut s, &mut t).await;
                }
            }));
        }
    });
    addr
}

/// A core whose peer port goes dark once it holds shards (as the chaos
/// partition-peer does: its own advertised address stops answering too)
/// hands them to its peer and steps down. A core that starts dark never
/// joins (`may_join`), so a restart can't take shards just to give them
/// up again.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_core_its_peers_cannot_reach_steps_down() {
    let (store, ca, applied) = (Store::memory(None), Ca::new(), Arc::new(Applied::default()));
    let a = spawn(&store, &ca, "node-a", Role::Core, &applied).await;
    let total = a.node.layout().unwrap().shards.len();
    eventually("a holds every shard", Duration::from_secs(10), || {
        a.node.cluster.as_ref().unwrap().owned().len() == total
    })
    .await;
    let real = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let open = Arc::new(AtomicBool::new(true));
    let front = gate_proxy(real.local_addr().unwrap(), open.clone()).await;
    let b = spawn_at(&store, &ca, "node-b", Role::Core, &applied, Some(real), Some(front)).await;
    let lost = Arc::new(AtomicBool::new(false));
    let l = lost.clone();
    b.node.on_lost(Box::new(move |_| l.store(true, Ordering::SeqCst)));
    eventually("b holds shards", Duration::from_secs(15), || !b.node.cluster.as_ref().unwrap().owned().is_empty())
        .await;
    open.store(false, Ordering::SeqCst);
    let t0 = Instant::now();
    eventually("the unreachable core stepped down", Duration::from_secs(20), || lost.load(Ordering::SeqCst)).await;
    eventually("a holds every shard again", Duration::from_secs(10), || {
        a.node.cluster.as_ref().unwrap().owned().len() == total
    })
    .await;
    assert!(t0.elapsed() < Duration::from_secs(15), "took {:?}", t0.elapsed());

    let real = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let dark = gate_proxy(real.local_addr().unwrap(), Arc::new(AtomicBool::new(false))).await;
    let c = spawn_at(&store, &ca, "node-c", Role::Core, &applied, Some(real), Some(dark)).await;
    tokio::time::sleep(Duration::from_secs(3)).await;
    let cc = c.node.cluster.as_ref().unwrap();
    assert!(!cc.joined() && cc.owned().is_empty(), "a core its peers can't reach doesn't join");
}

/// Answers once released, after saying it started.
struct HeldStage {
    started: Arc<Notify>,
    release: Arc<tokio::sync::Semaphore>,
}

#[async_trait::async_trait]
impl DidStage for HeldStage {
    async fn apply(&self, batch: Vec<Forwarded>) -> Vec<StageResult> {
        self.started.notify_one();
        let _ = self.release.acquire().await;
        batch.iter().map(|_| Ok(Outcome::Duplicate)).collect()
    }
}

/// A caller dropped mid-batch (a peer's request abandoned) must not end
/// the batch's gate pass while the stage still runs: a close then took the
/// shard from under a detached append.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_dropped_caller_keeps_its_shard_open_until_the_stage_answers() {
    let (store, ca, applied) = (Store::memory(None), Ca::new(), Arc::new(Applied::default()));
    let n = spawn(&store, &ca, "solo", Role::Core, &applied).await;
    let ev = fwd(1, 1);
    eventually("the DID's shard is served", Duration::from_secs(10), || n.node.owns_did(&ev.did)).await;
    let shard = n.node.layout().unwrap().shard_of(&ev.did);
    let (started, release) = (Arc::new(Notify::new()), Arc::new(tokio::sync::Semaphore::new(0)));
    n.node.set_stage(Arc::new(forward::Detached::new(
        Arc::new(HeldStage { started: started.clone(), release: release.clone() }),
        64,
    )));
    let node = n.node.clone();
    let caller = tokio::spawn(async move { node.apply_local(vec![ev]).await });
    started.notified().await;
    caller.abort();
    let _ = caller.await;
    let gate = n.node.gate.clone();
    let closing = tokio::spawn(async move { gate.close(&[shard]).await });
    tokio::time::sleep(Duration::from_millis(300)).await;
    assert!(!closing.is_finished(), "the close waits for the detached batch");
    release.add_permits(1);
    tokio::time::timeout(Duration::from_secs(5), closing).await.expect("the close finishes").unwrap();
}

/// Node A held host h at 1,000 when the host moved to B, whose sequence
/// restart (FutureCursor) set the checkpoint back to 50. A's late write of
/// its old cursor doesn't push it back up, and A taking the shard again
/// resumes from 50, not 1,000 (which would skip the new sequence's
/// events up to it).
#[tokio::test]
async fn a_restarted_sequence_survives_a_stale_owners_cursor() {
    use crate::upstream::{MemHostStore, Registry, Tier};
    let store = Store::memory(None);
    let (a, b) = (hosts::Checkpoints::new(store.clone()), hosts::Checkpoints::new(store.clone()));
    let reg = Arc::new(Registry::new(Arc::new(MemHostStore::default())));
    let _ = a.registry.set(reg.clone());
    let h = Host("pds.test".into());
    let (e, _) = reg.admit(&h, Tier::Default).await.unwrap();
    let s = ShardId(0);
    e.ack(1000);
    // one assignment epoch throughout: this is about generations alone (a
    // claim by the new owner would refuse the stale writes outright)
    a.write(s, &[(h.clone(), 1000)], 1).await.unwrap();

    b.load(s).await.unwrap();
    assert_eq!(b.get(&h), Some(1000));
    b.reset(&h);
    b.write(s, &[(h.clone(), 50)], 1).await.unwrap();

    a.write(s, &[(h.clone(), 1100)], 1).await.unwrap();
    b.load(s).await.unwrap();
    assert_eq!(b.get(&h), Some(50), "the stale owner's write was ignored");

    a.load(s).await.unwrap();
    assert_eq!(a.get(&h), Some(50));
    assert_eq!(e.acked_seq(), Some(50), "the registry's cursor is the checkpoint's");
    b.write(s, &[(h.clone(), 60)], 1).await.unwrap();
    a.write(s, &[(h.clone(), 70)], 1).await.unwrap();
    b.load(s).await.unwrap();
    assert_eq!(b.get(&h), Some(70), "one generation merges by max again");

    // a starting node takes the shard before its registry has the host,
    // whose entry is then seeded from a stale host record
    use crate::upstream::CursorSource;
    let c = hosts::Checkpoints::new(store.clone());
    c.load(s).await.unwrap();
    let reg2 = Arc::new(Registry::new(Arc::new(MemHostStore::default())));
    let (e2, _) = reg2.admit(&h, Tier::Default).await.unwrap();
    e2.ack(1000);
    let cur = hosts::ClusterCursors { checkpoints: c };
    cur.set_registry(reg2);
    assert_eq!(cur.durable_cursor(&h), Some(70));
    assert_eq!(e2.acked_seq(), Some(70));
    e2.ack(80);
    assert_eq!(cur.durable_cursor(&h), Some(80), "then our own acks count");
}

fn commit_fwd(i: usize, upstream_seq: i64) -> Forwarded {
    let d = did(i);
    let cid = vlpds::cid::Cid::dag_cbor(b"x");
    let ops = [vlpds::events::RepoOp { action: "create", path: "app.bsky.feed.post/3k", cid: Some(cid), prev: None }];
    let f = vlpds::events::commit_frame(&vlpds::events::CommitFrame {
        repo: &d,
        rev: "3kabc",
        since: None,
        commit: cid,
        prev_data: None,
        blocks: &[7u8; 64],
        ops: &ops,
        time: "2026-10-04T00:00:00.000Z",
    });
    let mut raw = Vec::new();
    f.finish(upstream_seq, &mut raw);
    Forwarded { frame: Bytes::from(raw), ..fwd(i, upstream_seq) }
}

fn account_fwd(i: usize, upstream_seq: i64, active: bool) -> Forwarded {
    let d = did(i);
    let status = (!active).then_some("takendown");
    let f = vlpds::events::account_frame(&d, active, status, "2026-10-04T00:00:00.000Z");
    let mut raw = Vec::new();
    f.finish(upstream_seq, &mut raw);
    Forwarded { frame: Bytes::from(raw), ..fwd(i, upstream_seq) }
}

/// A takedown leaves the account's earlier #commit and #sync frames out of
/// every node's replay: at once on the core that took it down, within a
/// poll on the edge and the replica, and from the bucket on a replica
/// started afterwards. Its #account and everyone else's frames pass, seqs
/// unchanged. Lifting it brings the frames back everywhere.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn takedowns_filter_the_replay_window_on_every_role() {
    use crate::policy::takedowns::Takedowns;
    let (store, ca, applied) = (Store::memory(None), Ca::new(), Arc::new(Applied::default()));
    let a = spawn(&store, &ca, "node-a", Role::Core, &applied).await;
    let edge = spawn(&store, &ca, "edge-1", Role::Edge, &applied).await;
    let replica = spawn(&store, &ca, "replica-1", Role::Replica, &applied).await;
    eventually("edge and replica follow the core", Duration::from_secs(5), || {
        edge.node.followers.followed().len() == 1 && replica.node.followers.followed().len() == 1
    })
    .await;
    let mut evs = Vec::new();
    for round in 0..3 {
        for i in 0..6 {
            evs.push(commit_fwd(i, round * 100 + 2 * i as i64));
            evs.push(fwd(i, round * 100 + 2 * i as i64 + 1));
        }
    }
    let written = forward_keyed(&a.node, evs).await;
    let n = written.len();
    let full = read_stream(a.public, 0, n, Duration::from_secs(10)).await;
    assert_stream(&full, &written, 0, "before");
    let taken = did(3);
    let is_taken = |f: &[u8]| {
        let m = vlpds::firehose::frame_meta(f);
        m.did == Some(taken.as_bytes())
    };

    // what set_takedown does: record, apply here, then announce
    let takedowns = Takedowns::new(store.clone());
    takedowns.record(&taken, true, "test", "spam").await.unwrap();
    a.node.serve.takedowns.apply_local(&taken, true);
    let account = forward_keyed(&a.node, vec![account_fwd(3, 1000, false)]).await;
    assert_eq!(account.len(), 1);
    let head = full.last().unwrap().0 + 1;
    let want: Vec<(i64, Vec<u8>)> = full.iter().filter(|(_, f)| !is_taken(f)).cloned().collect();
    assert_eq!(want.len(), n - 6, "did 3's 3 commits and 3 syncs");
    let check = |got: Vec<(i64, Vec<u8>)>, what: &str| {
        assert_eq!(got.len(), want.len() + 1, "{what}");
        assert_eq!(got[..want.len()], want[..], "{what}: everything but did 3's commits and syncs");
        let last = &got[want.len()];
        assert_eq!(last.0, head, "{what}: the #account keeps its seq");
        let m = vlpds::firehose::frame_meta(&last.1);
        assert_eq!((m.kind, m.did), (vlpds::firehose::FrameKind::Account, Some(taken.as_bytes())), "{what}");
    };
    check(read_stream(a.public, 0, want.len() + 1, Duration::from_secs(10)).await, "core, at once");
    for (t, what) in [(&edge, "edge"), (&replica, "replica")] {
        eventually("the takedown reaches it by polling", Duration::from_secs(5), || {
            t.node.serve.takedowns.contains(&taken)
        })
        .await;
        check(read_stream(t.public, 0, want.len() + 1, Duration::from_secs(10)).await, what);
    }
    // a replica started now has the list before it serves, and backfills
    let late = spawn(&store, &ca, "replica-2", Role::Replica, &applied).await;
    assert!(late.node.serve.takedowns.contains(&taken), "loaded at start");
    eventually("the late replica anchors its seqs", Duration::from_secs(10), || {
        late.node.serve.firehose.last_emitted.load(Ordering::Acquire) == head
    })
    .await;
    check(read_stream(late.public, 0, want.len() + 1, Duration::from_secs(10)).await, "late replica");

    // lifted: the old frames replay again
    takedowns.record(&taken, false, "test", "").await.unwrap();
    a.node.serve.takedowns.apply_local(&taken, false);
    let all = read_stream(a.public, 0, n + 1, Duration::from_secs(10)).await;
    assert_eq!(all[..n], full[..], "core: did 3's frames are back");
    for t in [&edge, &replica, &late] {
        eventually("the reversal reaches it", Duration::from_secs(5), || !t.node.serve.takedowns.contains(&taken))
            .await;
        assert_eq!(read_stream(t.public, 0, n + 1, Duration::from_secs(10)).await, all);
    }
    for t in [&late, &replica, &edge, &a] {
        t.node.shutdown().await.unwrap();
    }
}

/// An admin takedown's `#account` is appended outside the stage: its hold
/// keeps the shard from closing under it, and a shard we don't serve gives
/// none.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_held_did_keeps_its_shard_open() {
    let (store, ca, applied) = (Store::memory(None), Ca::new(), Arc::new(Applied::default()));
    let n = spawn(&store, &ca, "solo", Role::Core, &applied).await;
    let d = did(1);
    eventually("the DID's shard is served", Duration::from_secs(10), || n.node.owns_did(&d)).await;
    let shard = n.node.layout().unwrap().shard_of(&d);
    let hold = n.node.hold_did(&d).expect("served here");
    let gate = n.node.gate.clone();
    let closing = tokio::spawn(async move { gate.close(&[shard]).await });
    tokio::time::sleep(Duration::from_millis(200)).await;
    assert!(!closing.is_finished(), "the close waits for the hold");
    drop(hold);
    tokio::time::timeout(Duration::from_secs(5), closing).await.expect("the close finishes").unwrap();
    assert!(n.node.hold_did(&d).is_none(), "closed: nothing to hold");
}

/// The slow-log step-down compares our oldest pending append with what our
/// peers publish of theirs, each on its own clock, not with our clock
/// minus their watermark (which carries the clock offset between us).
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn peers_publish_their_own_pending_append_age() {
    let (store, ca, applied) = (Store::memory(None), Ca::new(), Arc::new(Applied::default()));
    let a = spawn(&store, &ca, "node-a", Role::Core, &applied).await;
    let b = spawn(&store, &ca, "node-b", Role::Core, &applied).await;
    let ac = a.node.cluster.clone().unwrap();
    eventually("a reads b's pending age from its lease", Duration::from_secs(10), || {
        ac.peers().iter().any(|l| l.node_id == "node-b" && l.pending_age_ms.is_some())
    })
    .await;
    let lease = |id: &str, ms: Option<u64>, draining: bool| {
        let mut l = ac.own_lease();
        (l.node_id, l.pending_age_ms, l.draining) = (id.into(), ms, draining);
        l
    };
    assert_eq!(median_peer_pending_age(&[lease("x", None, false)]), None, "no reports, no evidence");
    let peers = [
        lease("x", Some(40), false),
        lease("y", Some(900), false),
        lease("z", Some(9000), true),
        lease("w", None, false),
    ];
    assert_eq!(median_peer_pending_age(&peers), Some(Duration::from_millis(900)));
    a.node.halt();
    b.node.halt();
}

struct Hang;

#[async_trait::async_trait]
impl DidStage for Hang {
    async fn apply(&self, _batch: Vec<Forwarded>) -> Vec<StageResult> {
        std::future::pending().await
    }
}

/// A shard close waits for its in-flight batches inside vlpds's step,
/// under its step lock. A batch that never finishes (segment PUTs that
/// retry forever) fail-stops the node after the lapse window instead of
/// wedging every later step.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_close_stuck_on_an_endless_batch_fail_stops() {
    let (store, ca, applied) = (Store::memory(None), Ca::new(), Arc::new(Applied::default()));
    let a = spawn(&store, &ca, "node-a", Role::Core, &applied).await;
    let total = a.node.layout().unwrap().shards.len();
    let ac = a.node.cluster.clone().unwrap();
    eventually("a holds every shard", Duration::from_secs(10), || ac.owned().len() == total).await;
    let lost = Arc::new(AtomicBool::new(false));
    let l = lost.clone();
    a.node.on_lost(Box::new(move |_| l.store(true, Ordering::SeqCst)));
    a.node.set_stage(Arc::new(Hang));
    let n = a.node.clone();
    tokio::spawn(async move { n.forward(fwd(1, 1)).await });
    eventually("a batch in flight", Duration::from_secs(5), || !a.node.gate.inflight.lock().is_empty()).await;
    let shard = a.node.did_shard(&did(1)).unwrap();
    let t0 = Instant::now();
    let rs = ShardHost::close_many(&*a.node, vec![shard]).await;
    assert!(rs.iter().all(|(_, r)| r.is_err()), "the close fails");
    assert!(lost.load(Ordering::SeqCst), "and the node fail-stops");
    assert!(t0.elapsed() < Duration::from_secs(5), "within the lapse window: {:?}", t0.elapsed());
    a.node.halt();
}

/// An edge's certificate and the shared token get it streams and hellos
/// only. Forwards, nudges, key invalidations and fences of a live core's
/// log want a leased core, and a fence of one's own log is always allowed.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn peer_routes_are_bound_to_the_callers_certificate() {
    let (store, ca, applied) = (Store::memory(None), Ca::new(), Arc::new(Applied::default()));
    let a = spawn(&store, &ca, "node-a", Role::Core, &applied).await;
    let b = spawn(&store, &ca, "node-b", Role::Core, &applied).await;
    let ac = a.node.cluster.clone().unwrap();
    eventually("a lists b", Duration::from_secs(10), || ac.peers().iter().any(|l| l.node_id == "node-b")).await;
    let a_addr = ac.own_lease().addr;
    let a_log = ac.log_id.clone();
    let b_log = b.node.cluster.as_ref().unwrap().log_id.clone();
    let post = |who: &str, path: &str, body: Vec<u8>| {
        let http = vlpds::http::PeerClient::new(1, ca.node(who));
        let url = format!("{a_addr}{path}");
        async move {
            let r = http.post(url).header(peer::TOKEN_HEADER, "test-token").header("content-type", "application/json");
            r.body(body).send().await.unwrap().status().as_u16()
        }
    };
    let json = |v: serde_json::Value| serde_json::to_vec(&v).unwrap();
    let fence = |log: &str| json(serde_json::json!({ "log_id": log }));
    let batch = || forward::encode_batch(&[fwd(1, 1)]).to_vec();
    for who in ["edge-1", "node-z"] {
        assert_eq!(post(who, peer::FORWARD, batch()).await, 403, "{who} forward");
        assert_eq!(post(who, peer::NUDGE, json(serde_json::json!({ "hosts": true }))).await, 403, "{who} nudge");
        assert_eq!(post(who, peer::KEYS, json(serde_json::json!({ "dids": [] }))).await, 403, "{who} keys");
        assert_eq!(post(who, peer::FENCE, fence(&a_log)).await, 403, "{who} fence");
    }
    let hello = json(serde_json::json!({ "node_id": "edge-1" }));
    assert_eq!(post("edge-1", peer::HELLO, hello).await, 200);
    let posing = json(serde_json::json!({ "node_id": "node-b" }));
    assert_eq!(post("edge-1", peer::HELLO, posing).await, 403, "a hello names its caller");
    let leaving = json(serde_json::json!({ "leaving": a_log }));
    assert_eq!(post("node-b", peer::NUDGE, leaving).await, 403, "only a node's own leave");
    // b is leased: it forwards, but may not fence a's live log
    assert_eq!(post("node-b", peer::FORWARD, batch()).await, 200);
    assert_eq!(post("node-b", peer::FENCE, fence(&a_log)).await, 403);
    assert!(!vlpds::nodelog::first_free(&store, &a_log).await.unwrap().1, "a's log stays open");
    // a node may always have its own log fenced
    assert_eq!(post("node-b", peer::FENCE, fence(&b_log)).await, 200);
    assert!(vlpds::nodelog::first_free(&store, &b_log).await.unwrap().1, "b's log is fenced");
    a.node.halt();
    b.node.halt();
}
