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
}

#[async_trait::async_trait]
impl HostHandler for FakeHosts {
    async fn release(&self, keep: HostFilter) -> Vec<(Host, i64)> {
        let acked = self.acked.lock().clone();
        let mut out = Vec::new();
        for (h, s) in acked {
            if !keep(&h) {
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
    o.internal_token = "test-token".into();
    o.poll = Duration::from_millis(50);
    o.guard = Duration::from_millis(100);
    if role != Role::Replica {
        o.tls = Some(ca.node(id));
    }
    o
}

async fn spawn(store: &Store, ca: &Ca, id: &str, role: Role, applied: &Arc<Applied>) -> TNode {
    spawn_at(store, ca, id, role, applied, None).await
}

/// `advertise`: where peers are told to reach it (default: its listener).
async fn spawn_at(
    store: &Store,
    ca: &Ca,
    id: &str,
    role: Role,
    applied: &Arc<Applied>,
    advertise: Option<String>,
) -> TNode {
    let peer = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
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
    }
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
    // c closed its sockets and checkpointed their cursors before handing over
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

/// A core whose advertised peer address takes connections and never
/// answers (a blackholed port in front of it): it hands its shards to the
/// peer that can't reach it and steps down, instead of holding them for
/// the whole partition.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_core_its_peers_cannot_reach_steps_down() {
    let (store, ca, applied) = (Store::memory(None), Ca::new(), Arc::new(Applied::default()));
    let a = spawn(&store, &ca, "node-a", Role::Core, &applied).await;
    eventually("a holds every shard", Duration::from_secs(10), || {
        a.node.cluster.as_ref().unwrap().owned().len() == a.node.layout().unwrap().shards.len()
    })
    .await;
    let hole = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let hole_addr = format!("https://{}", hole.local_addr().unwrap());
    tokio::spawn(async move {
        let mut held = Vec::new();
        while let Ok((s, _)) = hole.accept().await {
            held.push(s);
        }
    });
    let b = spawn_at(&store, &ca, "node-b", Role::Core, &applied, Some(hole_addr)).await;
    let lost = Arc::new(AtomicBool::new(false));
    let l = lost.clone();
    b.node.on_lost(Box::new(move |_| l.store(true, Ordering::SeqCst)));
    let t0 = Instant::now();
    eventually("the unreachable core stepped down", Duration::from_secs(20), || lost.load(Ordering::SeqCst)).await;
    let total = a.node.layout().unwrap().shards.len();
    eventually("a holds every shard again", Duration::from_secs(10), || {
        a.node.cluster.as_ref().unwrap().owned().len() == total
    })
    .await;
    assert!(t0.elapsed() < Duration::from_secs(15), "took {:?}", t0.elapsed());
}
