//! In-process clusters: three nodes on real TCP, each on its own runtime so
//! a crash is a runtime shut down (memory and sockets gone at once), with
//! partitions from `node::Faults`. Every test runs the emission checker
//! over every node's emitted stream.

use super::check::{Checker, content_id};
use super::client::{Client, test_frame};
use super::commitlog::{self, CommitLog};
use super::emit::{Emitted, Emitter};
use super::flush;
use super::log::encode_cursors;
use super::node::{Config, Durability, Faults, MemoryOnly, Node, Role};
use super::wire;
use bytes::Bytes;
use parking_lot::Mutex;
use rand::{Rng, SeedableRng};
use std::collections::HashMap;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::time::{Duration, Instant};
use tokio::sync::mpsc;
use vlpds::store::Store;

struct Running {
    rt: tokio::runtime::Runtime,
    node: Arc<Node>,
    faults: Arc<Faults>,
    cl: Option<Arc<CommitLog>>,
}

type ConfigFn = Arc<dyn Fn(&str, &HashMap<String, String>) -> Config + Send + Sync>;

struct Cluster {
    ids: Vec<String>,
    addrs: HashMap<String, String>,
    store: Store,
    nodes: HashMap<String, Running>,
    incarnations: HashMap<String, u64>,
    tap: mpsc::UnboundedSender<Emitted>,
    checker: Arc<Mutex<Checker>>,
    /// Faults that outlive a restart: (node, peers it can't reach).
    blocks: Vec<(String, String)>,
    /// Each node's commitlog lives under here (memory-only without).
    disk: Option<(tempfile::TempDir, commitlog::Options)>,
    /// Overrides `config` for nodes started from now on.
    cfg: Option<ConfigFn>,
    ring_bytes: usize,
}

fn free_port() -> u16 {
    std::net::TcpListener::bind("127.0.0.1:0").unwrap().local_addr().unwrap().port()
}

fn config(id: &str, addrs: &HashMap<String, String>) -> Config {
    let peers = addrs.iter().filter(|(k, _)| *k != id).map(|(k, v)| (k.clone(), v.clone())).collect();
    let mut c = Config::new(id, peers);
    c.heartbeat = Duration::from_millis(50);
    c.election_timeout = Duration::from_millis(600);
    c.probe_after = Duration::from_millis(200);
    c.stagger = Duration::from_millis(300);
    c.rpc_timeout = Duration::from_millis(300);
    c
}

impl Cluster {
    async fn new(n: usize) -> Cluster {
        Cluster::with(n, None).await
    }

    async fn durable(n: usize, sync_delay: Option<Duration>) -> Cluster {
        let o = commitlog::Options {
            segment_bytes: 1 << 20,
            retain_bytes: 8 << 20,
            memory_bytes: 1 << 20,
            sync_delay,
            ..commitlog::Options::default()
        };
        Cluster::with(n, Some((tempfile::tempdir().unwrap(), o))).await
    }

    async fn with(n: usize, disk: Option<(tempfile::TempDir, commitlog::Options)>) -> Cluster {
        Cluster::with_cfg(n, disk, None, 64 << 20).await
    }

    async fn with_cfg(
        n: usize,
        disk: Option<(tempfile::TempDir, commitlog::Options)>,
        cfg: Option<ConfigFn>,
        ring_bytes: usize,
    ) -> Cluster {
        let ids: Vec<String> = (1..=n).map(|i| format!("n{i}")).collect();
        let addrs = ids.iter().map(|id| (id.clone(), format!("127.0.0.1:{}", free_port()))).collect();
        let (tap, mut rx) = mpsc::unbounded_channel::<Emitted>();
        let checker = Arc::new(Mutex::new(Checker::new()));
        let ck = checker.clone();
        tokio::spawn(async move {
            while let Some(e) = rx.recv().await {
                let stream = format!("{}#{}", e.node, e.incarnation);
                let mut c = ck.lock();
                for (seq, data) in &e.events {
                    c.observe(&stream, *seq as u64, content_id(data));
                }
            }
        });
        let mut c = Cluster {
            ids: ids.clone(),
            addrs,
            store: Store::memory(None),
            nodes: HashMap::new(),
            incarnations: HashMap::new(),
            tap,
            checker,
            blocks: Vec::new(),
            disk,
            cfg,
            ring_bytes,
        };
        for id in &ids {
            c.start(id).await;
        }
        c
    }

    async fn start(&mut self, id: &str) {
        let rt = tokio::runtime::Builder::new_multi_thread().worker_threads(2).enable_all().build().unwrap();
        let inc = {
            let i = self.incarnations.entry(id.to_string()).or_default();
            *i += 1;
            *i
        };
        let faults = Arc::new(Faults::default());
        for (a, b) in &self.blocks {
            if a == id {
                faults.block(&[b]);
            }
        }
        let emit = Emitter::with_store(id, inc, self.ring_bytes, Some(self.tap.clone()), self.store.clone());
        let (cl, recovered) = match &self.disk {
            Some((dir, o)) => {
                let (cl, r) = CommitLog::open(&dir.path().join(id), o.clone()).unwrap();
                (Some(cl), Some(r))
            }
            None => (None, None),
        };
        let durability: Arc<dyn Durability> = match &cl {
            Some(cl) => Arc::new(cl.clone()),
            None => Arc::new(MemoryOnly),
        };
        let (cfg, store, addr, f) = (
            self.cfg.as_ref().map_or_else(|| config(id, &self.addrs), |f| f(id, &self.addrs)),
            self.store.clone(),
            self.addrs[id].clone(),
            faults.clone(),
        );
        let (tx, rx) = tokio::sync::oneshot::channel();
        rt.spawn(async move {
            let t = Instant::now();
            let listener = loop {
                match tokio::net::TcpListener::bind(&addr).await {
                    Ok(l) => break l,
                    Err(e) if t.elapsed() < Duration::from_secs(5) => {
                        let _ = e;
                        tokio::time::sleep(Duration::from_millis(20)).await;
                    }
                    Err(e) => panic!("bind {addr}: {e}"),
                }
            };
            let n = Node::start(cfg, store, listener, emit, f, durability, recovered).await.unwrap();
            let _ = tx.send(n);
        });
        let node = rx.await.unwrap();
        self.nodes.insert(id.to_string(), Running { rt, node, faults, cl });
    }

    /// kill -9: the runtime goes, with every task, socket and byte of memory.
    /// What the commitlog wrote stays (the page cache outlives a process).
    fn kill(&mut self, id: &str) {
        if let Some(r) = self.nodes.remove(id) {
            r.rt.shutdown_background();
            if let Some(cl) = r.cl {
                cl.halt();
            }
        }
    }

    /// The box loses power: as kill, and the commitlog also loses a random
    /// part of what it wrote since its last fsync, ending in a torn record.
    fn power_cut(&mut self, id: &str, rng: &mut impl Rng) {
        if let Some(r) = self.nodes.remove(id) {
            // the disk is cut first: nothing after this point reaches it
            if let Some(cl) = &r.cl {
                let garbage: Vec<u8> = (0..rng.gen_range(0..40)).map(|_| rng.r#gen()).collect();
                cl.power_cut(rng.gen_range(0.0..1.0), &garbage).unwrap();
            }
            r.rt.shutdown_background();
        }
    }

    /// Cuts `id` off from every other node, both ways.
    fn isolate(&mut self, id: &str) {
        for other in self.ids.clone() {
            if other != id {
                self.block(id, &other);
            }
        }
    }

    fn block(&mut self, a: &str, b: &str) {
        for (x, y) in [(a, b), (b, a)] {
            self.blocks.push((x.to_string(), y.to_string()));
            if let Some(r) = self.nodes.get(x) {
                r.faults.block(&[y]);
            }
        }
    }

    fn heal(&mut self) {
        self.blocks.clear();
        for r in self.nodes.values() {
            r.faults.heal();
        }
    }

    fn leader(&self) -> Option<String> {
        let mut leaders: Vec<(u64, String)> = self
            .nodes
            .values()
            .map(|r| r.node.status())
            .filter(|s| s.role == Role::Leader)
            .map(|s| (s.epoch, s.id))
            .collect();
        leaders.sort();
        leaders.pop().map(|l| l.1)
    }

    async fn wait_leader(&self, within: Duration) -> String {
        let t = Instant::now();
        loop {
            if let Some(l) = self.leader() {
                return l;
            }
            assert!(t.elapsed() < within, "no leader within {within:?}");
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
    }

    fn client(&self) -> Arc<Client> {
        Client::new(self.ids.iter().map(|id| (id.clone(), self.addrs[id].clone())).collect())
    }

    /// Waits until every running node has emitted the same, full commit.
    async fn converge(&self, within: Duration) {
        let t = Instant::now();
        loop {
            let st: Vec<_> = self.nodes.values().map(|r| r.node.status()).collect();
            let top = st.iter().map(|s| s.last).max().unwrap_or(0);
            if st.iter().all(|s| s.emitted == top && s.commit == top) && st.iter().any(|s| s.role == Role::Leader) {
                // the tap is asynchronous
                tokio::time::sleep(Duration::from_millis(100)).await;
                return;
            }
            assert!(t.elapsed() < within, "no convergence within {within:?}: {st:#?}");
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
    }

    fn finish(&self, acked: &[(u64, u64)]) -> super::check::Report {
        let mut c = self.checker.lock();
        for &(s, h) in acked {
            c.acked(s, h);
        }
        let logs: Vec<(String, Vec<(u64, u64)>)> = self
            .nodes
            .iter()
            .map(|(id, r)| (id.clone(), r.node.committed().into_iter().map(|(s, d)| (s, content_id(&d))).collect()))
            .collect();
        c.finish(&logs)
    }

    fn shutdown(mut self) {
        for id in self.ids.clone() {
            self.kill(&id);
        }
    }
}

/// Submitters that keep `n` batches in flight until stopped; every ack is
/// recorded as (seq, content id).
struct Load {
    stop: Arc<AtomicBool>,
    acked: Arc<Mutex<Vec<(u64, u64)>>>,
    tasks: Vec<tokio::task::JoinHandle<()>>,
    sent: Arc<AtomicU64>,
}

impl Load {
    fn start(client: Arc<Client>, n: usize, batch: usize, pause: Duration) -> Load {
        let stop = Arc::new(AtomicBool::new(false));
        let acked = Arc::new(Mutex::new(Vec::new()));
        let sent = Arc::new(AtomicU64::new(0));
        let tasks = (0..n)
            .map(|w| {
                let (client, stop, acked, sent) = (client.clone(), stop.clone(), acked.clone(), sent.clone());
                tokio::spawn(async move {
                    // one host per submitter, its events numbered from 1; a
                    // submit carries the cursor of every event acked before it
                    let host = format!("t{w}");
                    let mut n = 0u64;
                    while !stop.load(Ordering::Acquire) {
                        let cursors =
                            if n > 0 { encode_cursors(&[(host.clone(), n)].into()) } else { Bytes::new() };
                        let frames: Vec<(Bytes, Bytes)> =
                            (1..=batch as u64).map(|i| test_frame(&format!("did:q:{host}:{}", n + i), 64, 0)).collect();
                        let (first, cnt) = client.submit_with(frames.clone(), cursors).await;
                        n += batch as u64;
                        assert_eq!(cnt as usize, batch);
                        sent.fetch_add(cnt, Ordering::Relaxed);
                        {
                            let mut a = acked.lock();
                            for (i, (p, s)) in frames.iter().enumerate() {
                                let seq = first + i as u64;
                                a.push((seq, content_id(&wire::splice_seq(p, s, seq))));
                            }
                        }
                        tokio::time::sleep(pause).await;
                    }
                })
            })
            .collect();
        Load { stop, acked, tasks, sent }
    }

    async fn stop(self) -> Vec<(u64, u64)> {
        self.stop.store(true, Ordering::Release);
        for t in self.tasks {
            tokio::time::timeout(Duration::from_secs(20), t).await.expect("submitter stuck").unwrap();
        }
        std::mem::take(&mut *self.acked.lock())
    }
}

fn assert_clean(r: &super::check::Report) {
    assert!(r.ok, "checker: {r:#?}");
    assert_eq!(r.holes, 0, "{r:#?}");
    assert!(r.acked > 0 && r.acked_missing == 0, "{r:#?}");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn commits_at_quorum_and_every_node_emits() {
    let c = Cluster::new(3).await;
    c.wait_leader(Duration::from_secs(5)).await;
    let load = Load::start(c.client(), 4, 20, Duration::from_millis(1));
    tokio::time::sleep(Duration::from_secs(2)).await;
    let acked = load.stop().await;
    c.converge(Duration::from_secs(5)).await;
    let r = c.finish(&acked);
    assert_clean(&r);
    // three nodes emitted every committed seq once
    assert_eq!(r.observed, 3 * r.max_seq, "{r:#?}");
    assert_eq!(r.distinct_seqs, r.max_seq);
    c.shutdown();
}

/// The hard rule: a leader that can't reach a quorum emits nothing, and
/// what it appended alone never reaches anyone.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn nothing_is_emitted_without_a_quorum() {
    let mut c = Cluster::new(3).await;
    let l = c.wait_leader(Duration::from_secs(5)).await;
    let client = c.client();
    client.submit(vec![test_frame("did:t:before", 8, 0)]).await;
    c.converge(Duration::from_secs(5)).await;
    let before: HashMap<String, u64> = c.nodes.iter().map(|(id, r)| (id.clone(), r.node.status().emitted)).collect();
    c.isolate(&l);
    let leader = c.nodes[&l].node.clone();
    let lone = vec![test_frame("did:t:alone", 8, 0)];
    let lone_ids: Vec<u64> = {
        let seq = leader.status().last + 1;
        lone.iter().map(|(p, s)| content_id(&wire::splice_seq(p, s, seq))).collect()
    };
    let r = tokio::time::timeout(Duration::from_secs(3), leader.submit(lone, Bytes::new())).await;
    assert!(!matches!(r, Ok(wire::Msg::Submitted { .. })), "an isolated leader acked: {r:?}");
    assert!(leader.status().emitted == before[&l], "the isolated leader emitted past its quorum");
    assert_ne!(leader.status().role, Role::Leader, "it stepped down");
    // the majority side carries on under a new leader
    let l2 = c.wait_leader(Duration::from_secs(5)).await;
    assert_ne!(l2, l);
    client.submit(vec![test_frame("did:t:after", 8, 0)]).await;
    c.heal();
    c.converge(Duration::from_secs(10)).await;
    let logs: Vec<Vec<u64>> =
        c.nodes.values().map(|r| r.node.committed().iter().map(|(_, d)| content_id(d)).collect()).collect();
    for log in &logs {
        assert!(!log.iter().any(|h| lone_ids.contains(h)), "the lone leader's entry was committed");
    }
    let r = c.finish(&[]);
    assert!(r.ok && r.holes == 0, "{r:#?}");
    c.shutdown();
}

/// kill -9 the leader under load, again and again, restarting each victim
/// empty (memory only) once a new leader is up. Every emitted seq keeps its
/// content everywhere, every ack is emitted, no stream repeats or skips.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn leader_kills_lose_nothing_emitted() {
    let mut c = Cluster::new(3).await;
    c.wait_leader(Duration::from_secs(5)).await;
    let load = Load::start(c.client(), 4, 10, Duration::from_millis(2));
    let mut pauses = Vec::new();
    for _ in 0..5 {
        tokio::time::sleep(Duration::from_millis(700)).await;
        let l = c.wait_leader(Duration::from_secs(5)).await;
        let sent = load.sent.load(Ordering::Relaxed);
        let t = Instant::now();
        c.kill(&l);
        while load.sent.load(Ordering::Relaxed) == sent {
            assert!(t.elapsed() < Duration::from_secs(10), "no commits after killing {l}");
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
        pauses.push(t.elapsed());
        c.start(&l).await;
        // the restarted node can't vote until it has caught up
        let t = Instant::now();
        while !c.nodes[&l].node.status().intact {
            assert!(t.elapsed() < Duration::from_secs(10), "{l} never caught up");
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    }
    let acked = load.stop().await;
    c.converge(Duration::from_secs(10)).await;
    let r = c.finish(&acked);
    eprintln!("takeover pauses: {pauses:?}; {r:?}");
    assert_clean(&r);
    c.shutdown();
}

/// A restarted (empty) node doesn't count toward a takeover's quorum until
/// it has caught up: with the leader dead and only one intact node left,
/// nothing is elected and nothing is emitted, rather than a quorum of one
/// real log and one empty one.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn restarted_node_cannot_vote_before_catching_up() {
    let mut c = Cluster::new(3).await;
    let l = c.wait_leader(Duration::from_secs(5)).await;
    let client = c.client();
    for i in 0..20 {
        client.submit(vec![test_frame(&format!("did:t:{i}"), 8, 0)]).await;
    }
    c.converge(Duration::from_secs(5)).await;
    let f = c.ids.iter().find(|id| **id != l).unwrap().clone();
    c.kill(&f);
    // it comes back unable to hear the leader, so it can't catch up
    c.block(&f, &l);
    c.start(&f).await;
    tokio::time::sleep(Duration::from_millis(300)).await;
    assert!(!c.nodes[&f].node.status().intact);
    let top = c.nodes[&l].node.status().commit;
    c.kill(&l);
    tokio::time::sleep(Duration::from_secs(4)).await;
    assert_eq!(c.leader(), None, "a leader was elected from one intact log");
    for r in c.nodes.values() {
        assert!(r.node.status().emitted <= top);
    }
    let r = c.finish(&[]);
    assert!(r.ok, "{r:#?}");
    c.shutdown();
}

/// Partitions: one node cut off at a time (leader or not), under load.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn partitions_lose_nothing_emitted() {
    let mut c = Cluster::new(3).await;
    c.wait_leader(Duration::from_secs(5)).await;
    let load = Load::start(c.client(), 4, 10, Duration::from_millis(2));
    for round in 0..6 {
        tokio::time::sleep(Duration::from_millis(500)).await;
        let victim =
            if round % 2 == 0 { c.wait_leader(Duration::from_secs(5)).await } else { format!("n{}", round % 3 + 1) };
        c.isolate(&victim);
        tokio::time::sleep(Duration::from_millis(1500)).await;
        c.heal();
    }
    let acked = load.stop().await;
    c.converge(Duration::from_secs(10)).await;
    let r = c.finish(&acked);
    eprintln!("{r:?}");
    assert_clean(&r);
    c.shutdown();
}

/// Random kills, restarts, partitions and heals for a while, seeded.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn random_chaos_loses_nothing_emitted() {
    let seed: u64 = std::env::var("QLOG_SEED").ok().and_then(|s| s.parse().ok()).unwrap_or(7);
    let mut rng = rand::rngs::StdRng::seed_from_u64(seed);
    let mut c = Cluster::new(3).await;
    c.wait_leader(Duration::from_secs(5)).await;
    let load = Load::start(c.client(), 4, 8, Duration::from_millis(2));
    let t = Instant::now();
    let mut actions = Vec::new();
    while t.elapsed() < Duration::from_secs(12) {
        tokio::time::sleep(Duration::from_millis(rng.gen_range(200..900))).await;
        let down: Vec<String> = c.ids.iter().filter(|id| !c.nodes.contains_key(*id)).cloned().collect();
        let id = c.ids[rng.gen_range(0..c.ids.len())].clone();
        match rng.gen_range(0..4) {
            // at most one node down or cut off at once: memory-only can't
            // survive two (that's the bucket recovery of a later phase)
            0 if down.is_empty() && c.blocks.is_empty() => {
                c.kill(&id);
                actions.push(format!("kill {id}"));
            }
            1 if !down.is_empty() => {
                c.start(&down[0]).await;
                let t = Instant::now();
                while !c.nodes[&down[0]].node.status().intact && t.elapsed() < Duration::from_secs(10) {
                    tokio::time::sleep(Duration::from_millis(10)).await;
                }
                actions.push(format!("start {}", down[0]));
            }
            2 if down.is_empty() && c.blocks.is_empty() => {
                c.isolate(&id);
                actions.push(format!("isolate {id}"));
            }
            _ => {
                c.heal();
            }
        }
    }
    c.heal();
    for id in c.ids.clone() {
        if !c.nodes.contains_key(&id) {
            c.start(&id).await;
        }
    }
    let acked = load.stop().await;
    c.converge(Duration::from_secs(15)).await;
    let r = c.finish(&acked);
    eprintln!("seed {seed}: {actions:?}\n{r:?}");
    assert_clean(&r);
    c.shutdown();
}

fn status_line(c: &Cluster) -> String {
    let mut v: Vec<String> = c
        .nodes
        .values()
        .map(|r| {
            let s = r.node.status();
            format!("{} {:?} e{} last {} commit {}", s.id, s.role, s.epoch, s.last, s.commit)
        })
        .collect();
    v.sort();
    v.join("; ")
}

/// With the commitlog, a restarted node is whole at once: kill -9 the
/// leader again and again and each victim comes back intact, with its log.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn durable_restarts_are_intact_at_once() {
    let mut c = Cluster::durable(3, None).await;
    c.wait_leader(Duration::from_secs(5)).await;
    let load = Load::start(c.client(), 4, 10, Duration::from_millis(2));
    for _ in 0..4 {
        tokio::time::sleep(Duration::from_millis(600)).await;
        let l = c.wait_leader(Duration::from_secs(5)).await;
        let before = c.nodes[&l].node.status().commit;
        c.kill(&l);
        c.start(&l).await;
        let st = c.nodes[&l].node.status();
        assert!(st.intact, "{l} restarted not intact");
        assert!(st.last >= before, "{l} came back with less than it committed: {} < {before}", st.last);
    }
    let acked = load.stop().await;
    c.converge(Duration::from_secs(10)).await;
    let r = c.finish(&acked);
    eprintln!("{r:?}");
    assert_clean(&r);
    c.shutdown();
}

/// Two nodes killed at once (the leader among them), then all three: with
/// the commitlog it's a normal takeover each time, not a lost quorum.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn durable_two_and_three_node_kills_lose_nothing() {
    let mut c = Cluster::durable(3, None).await;
    c.wait_leader(Duration::from_secs(5)).await;
    let load = Load::start(c.client(), 4, 10, Duration::from_millis(2));
    for round in 0..6 {
        tokio::time::sleep(Duration::from_millis(500)).await;
        let l = c.wait_leader(Duration::from_secs(10)).await;
        let victims: Vec<String> = if round % 2 == 0 {
            let other = c.ids.iter().find(|id| **id != l).unwrap().clone();
            vec![l, other]
        } else {
            c.ids.clone()
        };
        let sent = load.sent.load(Ordering::Relaxed);
        for v in &victims {
            c.kill(v);
        }
        tokio::time::sleep(Duration::from_millis(200)).await;
        for v in &victims {
            c.start(v).await;
        }
        let t = Instant::now();
        while load.sent.load(Ordering::Relaxed) == sent {
            assert!(t.elapsed() < Duration::from_secs(15), "no commits after killing {victims:?}: {}", status_line(&c));
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
    }
    let acked = load.stop().await;
    c.converge(Duration::from_secs(10)).await;
    let r = c.finish(&acked);
    eprintln!("{r:?}");
    assert_clean(&r);
    c.shutdown();
}

/// Power cuts on every node at once, under load, with a slow fsync: each
/// box loses what it wrote since its last fsync (and gets a torn record).
/// Every acked seq survives, because nothing is acked before its fsync.
/// Acking before the fsync fails this (seqs acked but never emitted, or
/// reissued with other contents).
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn durable_power_cuts_on_every_node_lose_nothing_acked() {
    let seed: u64 = std::env::var("QLOG_SEED").ok().and_then(|s| s.parse().ok()).unwrap_or(11);
    let mut rng = rand::rngs::StdRng::seed_from_u64(seed);
    let mut c = Cluster::durable(3, Some(Duration::from_millis(3))).await;
    c.wait_leader(Duration::from_secs(5)).await;
    let load = Load::start(c.client(), 8, 6, Duration::from_millis(1));
    for _ in 0..6 {
        tokio::time::sleep(Duration::from_millis(rng.gen_range(300..800))).await;
        c.wait_leader(Duration::from_secs(10)).await;
        let sent = load.sent.load(Ordering::Relaxed);
        for id in c.ids.clone() {
            c.power_cut(&id, &mut rng);
        }
        for id in c.ids.clone() {
            c.start(&id).await;
        }
        let t = Instant::now();
        while load.sent.load(Ordering::Relaxed) == sent {
            assert!(t.elapsed() < Duration::from_secs(15), "no commits after the power cut: {}", status_line(&c));
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
    }
    let acked = load.stop().await;
    c.converge(Duration::from_secs(10)).await;
    let r = c.finish(&acked);
    eprintln!("seed {seed}: {r:?}");
    assert_clean(&r);
    c.shutdown();
}

/// Random kills, power cuts, restarts, partitions and heals, any number of
/// nodes down at once, with the commitlog. Seeded (`QLOG_SEED`).
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn durable_random_chaos_loses_nothing() {
    let seed: u64 = std::env::var("QLOG_SEED").ok().and_then(|s| s.parse().ok()).unwrap_or(7);
    let mut rng = rand::rngs::StdRng::seed_from_u64(seed);
    let mut c = Cluster::durable(3, None).await;
    c.wait_leader(Duration::from_secs(5)).await;
    let load = Load::start(c.client(), 4, 8, Duration::from_millis(2));
    let t = Instant::now();
    let mut actions = Vec::new();
    while t.elapsed() < Duration::from_secs(12) {
        tokio::time::sleep(Duration::from_millis(rng.gen_range(200..900))).await;
        let down: Vec<String> = c.ids.iter().filter(|id| !c.nodes.contains_key(*id)).cloned().collect();
        let id = c.ids[rng.gen_range(0..c.ids.len())].clone();
        match rng.gen_range(0..6) {
            0 if c.nodes.contains_key(&id) => {
                c.kill(&id);
                actions.push(format!("kill {id}"));
            }
            1 if c.nodes.contains_key(&id) => {
                c.power_cut(&id, &mut rng);
                actions.push(format!("power cut {id}"));
            }
            2 | 3 if !down.is_empty() => {
                c.start(&down[0]).await;
                actions.push(format!("start {}", down[0]));
            }
            4 if c.blocks.is_empty() => {
                c.isolate(&id);
                actions.push(format!("isolate {id}"));
            }
            _ => {
                c.heal();
            }
        }
    }
    c.heal();
    for id in c.ids.clone() {
        if !c.nodes.contains_key(&id) {
            c.start(&id).await;
        }
    }
    let acked = load.stop().await;
    c.converge(Duration::from_secs(15)).await;
    let r = c.finish(&acked);
    eprintln!("seed {seed}: {actions:?}\n{r:?}");
    assert_clean(&r);
    c.shutdown();
}

/// A follower down for longer than the leader keeps in memory catches up
/// from the leader's commitlog rather than jumping its stream.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_lagging_follower_catches_up_from_disk() {
    let mut c = Cluster::durable(3, None).await;
    for id in c.ids.clone() {
        c.kill(&id);
    }
    // a small in-memory window, so the lag is served from disk
    let cfg = move |id: &str, addrs: &HashMap<String, String>| {
        let mut k = config(id, addrs);
        k.retain_bytes = 32 << 10;
        k
    };
    c.cfg = Some(Arc::new(cfg));
    for id in c.ids.clone() {
        c.start(&id).await;
    }
    let l = c.wait_leader(Duration::from_secs(5)).await;
    let f = c.ids.iter().find(|id| **id != l).unwrap().clone();
    c.kill(&f);
    let load = Load::start(c.client(), 4, 10, Duration::from_millis(1));
    tokio::time::sleep(Duration::from_secs(2)).await;
    c.start(&f).await;
    let acked = load.stop().await;
    c.converge(Duration::from_secs(10)).await;
    let st = c.nodes[&f].node.status();
    assert_eq!(st.resets, 0, "the follower was reset past its lag: {st:?}");
    let reads: u64 = c.nodes.values().map(|r| r.node.status().disk_reads).sum();
    assert!(reads > 0, "nothing was served from disk");
    assert_eq!(st.emit_gaps, 0);
    let r = c.finish(&acked);
    assert_clean(&r);
    c.shutdown();
}

// ---- the flush (Phase 3)

fn flush_opts() -> flush::Options {
    flush::Options {
        interval: Duration::from_millis(150),
        headroom: 10_000_000,
        segment_bytes: 32 << 10,
        crash: None,
    }
}

/// Durable nodes that flush; `opts` per node id.
async fn flushing(opts: impl Fn(&str) -> flush::Options + Send + Sync + 'static, ring_bytes: usize) -> Cluster {
    let o = commitlog::Options {
        segment_bytes: 256 << 10,
        retain_bytes: 1 << 20,
        memory_bytes: 64 << 10,
        ..commitlog::Options::default()
    };
    let cfg = move |id: &str, addrs: &HashMap<String, String>| {
        let mut k = config(id, addrs);
        k.retain_bytes = 64 << 10;
        k.flush = Some(opts(id));
        k
    };
    Cluster::with_cfg(3, Some((tempfile::tempdir().unwrap(), o)), Some(Arc::new(cfg)), ring_bytes).await
}

/// The manifest's consistency check, retried past a race with the next
/// flush deleting the checkpoint it named.
async fn verify(c: &Cluster) -> flush::Verified {
    let mut last = None;
    for _ in 0..5 {
        match flush::verify(&c.store).await {
            Ok(v) => return v,
            Err(e) => last = Some(e),
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    panic!("verify kept failing: {:#}", last.unwrap());
}

/// Until the manifest's F is at least `seq`.
async fn wait_flushed(c: &Cluster, seq: u64, within: Duration) -> flush::Manifest {
    let t = Instant::now();
    loop {
        if let Some((m, _)) = flush::read_manifest(&c.store).await.unwrap()
            && m.flushed >= seq
        {
            return m;
        }
        assert!(t.elapsed() < within, "F didn't reach {seq} within {within:?}: {}", status_line(c));
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
}

async fn settle_and_verify(c: &Cluster, acked: &[(u64, u64)]) -> flush::Verified {
    c.converge(Duration::from_secs(15)).await;
    let top = c.nodes.values().map(|r| r.node.status().commit).max().unwrap();
    wait_flushed(c, top, Duration::from_secs(10)).await;
    let v = verify(c).await;
    assert!(v.ok, "manifest inconsistent: {v:#?}");
    assert_eq!(v.flushed, top);
    assert_eq!(v.entries, top, "the bucket doesn't hold the whole log");
    let r = c.finish(acked);
    assert_clean(&r);
    v
}

/// Flushes under load and across a leader kill: every manifest describes
/// one point (segments, state and cursors at F), and the final one the
/// whole log.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn flush_seals_log_state_and_cursors_at_one_point() {
    let mut c = flushing(|_| flush_opts(), 64 << 20).await;
    c.wait_leader(Duration::from_secs(5)).await;
    let load = Load::start(c.client(), 4, 10, Duration::from_millis(2));
    for _ in 0..3 {
        let m = wait_flushed(&c, 1, Duration::from_secs(5)).await;
        tokio::time::sleep(Duration::from_millis(500)).await;
        let v = verify(&c).await;
        assert!(v.ok, "mid-run: {v:#?}");
        assert!(v.flushed >= m.flushed && v.hosts > 0, "{v:#?}");
        let l = c.wait_leader(Duration::from_secs(5)).await;
        c.kill(&l);
        c.start(&l).await;
    }
    let acked = load.stop().await;
    let v = settle_and_verify(&c, &acked).await;
    eprintln!("{v:?}");
    assert_eq!(v.hosts, 4);
    assert_eq!(v.orphans, 0);
    let ck = flush::read_manifest(&c.store).await.unwrap().unwrap().0;
    let cps = super::state::list_checkpoints(&c.store).await.unwrap();
    assert_eq!(cps, vec![ck.state.unwrap().checkpoint], "stale checkpoints kept");
    c.shutdown();
}

/// With no flush after the first, the commit index stops at R = H: the
/// reservation holds back everything past it, on every node.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn commit_stops_at_the_reservation() {
    let c = flushing(
        |_| flush::Options { interval: Duration::from_secs(3600), headroom: 300, ..flush_opts() },
        64 << 20,
    )
    .await;
    let l = c.wait_leader(Duration::from_secs(5)).await;
    let load = Load::start(c.client(), 4, 10, Duration::from_millis(2));
    tokio::time::sleep(Duration::from_millis(1500)).await;
    for r in c.nodes.values() {
        let st = r.node.status();
        assert!(st.commit <= 300 && st.emitted <= 300, "{st:?}");
    }
    let st = c.nodes[&l].node.status();
    assert_eq!((st.commit, st.reserve), (300, 300), "{st:?}");
    assert!(st.last > 300, "the load didn't get past R: {st:?}");
    drop(load);
    c.shutdown();
}

/// A crash at each step of a flush (the leader killed right there, then
/// restarted): the manifest stays consistent, a later flush picks up any
/// segment the dead one left, and nothing emitted is lost.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn crashes_at_every_flush_step_leave_a_consistent_manifest() {
    use flush::Step;
    let armed: Arc<Mutex<Option<Step>>> = Arc::default();
    let (tx, mut rx) = mpsc::unbounded_channel::<(String, Step)>();
    let (a, t) = (armed.clone(), tx.clone());
    let mut c = flushing(
        move |id| {
            let (a, t, id) = (a.clone(), t.clone(), id.to_string());
            flush::Options {
                crash: Some(Arc::new(move |s| {
                    let mut g = a.lock();
                    if *g == Some(s) {
                        *g = None;
                        let _ = t.send((id.clone(), s));
                        return true;
                    }
                    false
                })),
                ..flush_opts()
            }
        },
        64 << 20,
    )
    .await;
    c.wait_leader(Duration::from_secs(5)).await;
    let load = Load::start(c.client(), 4, 10, Duration::from_millis(2));
    let steps = [Step::Sealed, Step::SegmentPut, Step::BeforeManifest, Step::AfterManifest, Step::Fenced];
    for step in steps.iter().cycle().take(10) {
        tokio::time::sleep(Duration::from_millis(300)).await;
        *armed.lock() = Some(*step);
        if *step == Step::Fenced {
            // fences happen at takeovers
            let l = c.wait_leader(Duration::from_secs(5)).await;
            c.kill(&l);
            c.start(&l).await;
        }
        let (id, s) = tokio::time::timeout(Duration::from_secs(10), rx.recv())
            .await
            .unwrap_or_else(|_| panic!("no flush reached {step:?}: {}", status_line(&c)))
            .unwrap();
        c.kill(&id);
        let v = verify(&c).await;
        assert!(v.ok, "after a crash at {s:?}: {v:#?}");
        eprintln!("crash at {s:?} on {id}: F {} orphans {}", v.flushed, v.orphans);
        tokio::time::sleep(Duration::from_millis(100)).await;
        c.start(&id).await;
    }
    let acked = load.stop().await;
    let v = settle_and_verify(&c, &acked).await;
    assert_eq!(v.orphans, 0, "{v:#?}");
    c.shutdown();
}

/// A leader cut off mid-flush (stalled just before its manifest CAS) while
/// the others take over: the new leader's fence makes the old flush lose
/// its CAS, and the old leader deletes the checkpoint it made.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn an_old_leaders_flush_loses_to_a_takeover() {
    use flush::Step;
    let hold: Arc<Mutex<Option<String>>> = Arc::default();
    let (held_tx, mut held_rx) = mpsc::unbounded_channel::<String>();
    let (go_tx, go_rx) = std::sync::mpsc::channel::<()>();
    let go_rx = Arc::new(std::sync::Mutex::new(go_rx));
    let h = hold.clone();
    let mut c = flushing(
        move |id| {
            let (h, held_tx, go_rx, id) = (h.clone(), held_tx.clone(), go_rx.clone(), id.to_string());
            flush::Options {
                crash: Some(Arc::new(move |s| {
                    if s == Step::BeforeManifest && h.lock().as_deref() == Some(id.as_str()) {
                        *h.lock() = None;
                        let _ = held_tx.send(id.clone());
                        // the flush stalls here (a GC pause, a slow disk)
                        let _ = go_rx.lock().unwrap().recv_timeout(Duration::from_secs(20));
                    }
                    false
                })),
                // the new leader's first flush comes well after its fence
                interval: Duration::from_secs(1),
                ..flush_opts()
            }
        },
        64 << 20,
    )
    .await;
    let l = c.wait_leader(Duration::from_secs(5)).await;
    let load = Load::start(c.client(), 4, 10, Duration::from_millis(2));
    wait_flushed(&c, 1, Duration::from_secs(5)).await;
    let old_epoch = c.nodes[&l].node.status().epoch;
    *hold.lock() = Some(l.clone());
    let held = tokio::time::timeout(Duration::from_secs(5), held_rx.recv()).await.unwrap().unwrap();
    assert_eq!(held, l);
    c.isolate(&l);
    // the old flush goes on once a new leader leads (and has fenced the
    // manifest), before that leader's first flush
    let t = Instant::now();
    loop {
        let led = c.nodes.iter().any(|(id, r)| {
            let st = r.node.status();
            *id != l && st.role == Role::Leader && st.epoch > old_epoch
        });
        if led {
            break;
        }
        assert!(t.elapsed() < Duration::from_secs(10), "no takeover: {}", status_line(&c));
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
    tokio::time::sleep(Duration::from_millis(200)).await;
    let new = flush::read_manifest(&c.store).await.unwrap().unwrap().0;
    go_tx.send(()).unwrap();
    tokio::time::sleep(Duration::from_millis(300)).await;
    let (m, _) = flush::read_manifest(&c.store).await.unwrap().unwrap();
    assert!(m.epoch >= new.epoch && m.leader != l, "the old leader's flush won: {m:?}");
    assert_eq!(m.flushes, new.flushes, "a flush landed before the new leader's first: {m:?}");
    let st = c.nodes[&l].node.status();
    assert_ne!(st.role, Role::Leader, "{st:?}");
    c.heal();
    let acked = load.stop().await;
    let v = settle_and_verify(&c, &acked).await;
    eprintln!("{v:?}");
    tokio::time::sleep(Duration::from_millis(300)).await;
    let (m, _) = flush::read_manifest(&c.store).await.unwrap().unwrap();
    let cps = super::state::list_checkpoints(&c.store).await.unwrap();
    assert_eq!(cps, vec![m.state.unwrap().checkpoint], "the losing flush's checkpoint was kept");
    c.shutdown();
}

/// A consumer from cursor 0 on a node whose ring holds only the last few
/// events, with the bucket behind the head: it's served the bucket up to F,
/// the node's own log above F, then the ring, densely and with the content
/// every submitter was acked for. A cursor at F and one just above it too.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn old_cursors_backfill_from_the_bucket_then_the_local_log() {
    use futures::StreamExt;
    let c = flushing(|_| flush::Options { interval: Duration::from_millis(700), ..flush_opts() }, 32 << 10).await;
    c.wait_leader(Duration::from_secs(5)).await;
    let load = Load::start(c.client(), 4, 10, Duration::from_millis(2));
    tokio::time::sleep(Duration::from_millis(1600)).await;
    let acked = load.stop().await;
    c.converge(Duration::from_secs(10)).await;
    let m = flush::read_manifest(&c.store).await.unwrap().unwrap().0;
    let l = c.wait_leader(Duration::from_secs(5)).await;
    let id = c.ids.iter().find(|i| **i != l).unwrap().clone();
    let node = c.nodes[&id].node.clone();
    let top = node.status().commit;
    assert!(m.flushed > 0 && m.flushed + 500 < top, "want F well behind the head: F {} head {top}", m.flushed);
    let by_seq: HashMap<u64, u64> = acked.iter().copied().collect();
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    c.nodes[&id].rt.spawn(async move { axum::serve(listener, super::emit::router(node)).await });
    for cursor in [0, m.flushed - 1, m.flushed, m.flushed + 1] {
        let url = format!("ws://{addr}/xrpc/com.atproto.sync.subscribeRepos?cursor={cursor}");
        let (mut ws, _) = tokio_tungstenite::connect_async(&url).await.unwrap();
        let mut next = cursor + 1;
        while next <= top {
            let msg = tokio::time::timeout(Duration::from_secs(10), ws.next())
                .await
                .unwrap_or_else(|_| panic!("stalled at {next} of {top} from cursor {cursor}"))
                .unwrap()
                .unwrap();
            let tokio_tungstenite::tungstenite::Message::Binary(b) = msg else { continue };
            let (seq, _, _) = super::client::parse_test_frame(&b)
                .unwrap_or_else(|| panic!("not an event at {next}: {:?}", super::client::info_name(&b)));
            assert_eq!(seq, next, "from cursor {cursor}");
            if let Some(&want) = by_seq.get(&seq) {
                assert_eq!(content_id(&b), want, "seq {seq} served with other content");
            }
            next += 1;
        }
    }
    c.shutdown();
}

/// Seeded kills, power cuts and partitions while the leader flushes, with
/// flushes also dying at random steps (`QLOG_SEED`): the checker stays
/// clean, and every manifest seen describes one consistent point.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn flushing_random_chaos_keeps_every_manifest_consistent() {
    let seed: u64 = std::env::var("QLOG_SEED").ok().and_then(|s| s.parse().ok()).unwrap_or(5);
    let mut rng = rand::rngs::StdRng::seed_from_u64(seed);
    let (tx, mut rx) = mpsc::unbounded_channel::<String>();
    let armed = Arc::new(AtomicBool::new(true));
    let a = armed.clone();
    let mut c = flushing(
        move |id| {
            let (t, id, a) = (tx.clone(), id.to_string(), a.clone());
            flush::Options {
                crash: Some(Arc::new(move |_| {
                    if a.load(Ordering::Acquire) && rand::thread_rng().gen_bool(0.04) {
                        let _ = t.send(id.clone());
                        return true;
                    }
                    false
                })),
                ..flush_opts()
            }
        },
        64 << 20,
    )
    .await;
    c.wait_leader(Duration::from_secs(5)).await;
    let load = Load::start(c.client(), 4, 8, Duration::from_millis(2));
    let t = Instant::now();
    let mut actions = Vec::new();
    let mut verified = 0;
    while t.elapsed() < Duration::from_secs(15) {
        tokio::time::sleep(Duration::from_millis(rng.gen_range(200..900))).await;
        while let Ok(id) = rx.try_recv() {
            if c.nodes.contains_key(&id) {
                c.kill(&id);
                actions.push(format!("flush crash {id}"));
            }
        }
        let down: Vec<String> = c.ids.iter().filter(|id| !c.nodes.contains_key(*id)).cloned().collect();
        let id = c.ids[rng.gen_range(0..c.ids.len())].clone();
        match rng.gen_range(0..7) {
            0 if c.nodes.contains_key(&id) => {
                c.kill(&id);
                actions.push(format!("kill {id}"));
            }
            1 if c.nodes.contains_key(&id) => {
                c.power_cut(&id, &mut rng);
                actions.push(format!("power cut {id}"));
            }
            2 | 3 if !down.is_empty() => {
                c.start(&down[0]).await;
                actions.push(format!("start {}", down[0]));
            }
            4 if c.blocks.is_empty() => {
                c.isolate(&id);
                actions.push(format!("isolate {id}"));
            }
            5 => {
                let v = verify(&c).await;
                assert!(v.ok, "seed {seed} after {actions:?}: {v:#?}");
                verified += 1;
            }
            _ => {
                c.heal();
            }
        }
    }
    c.heal();
    armed.store(false, Ordering::Release);
    tokio::time::sleep(Duration::from_millis(100)).await;
    while let Ok(id) = rx.try_recv() {
        c.kill(&id);
    }
    for id in c.ids.clone() {
        if !c.nodes.contains_key(&id) {
            c.start(&id).await;
        }
    }
    let acked = load.stop().await;
    let v = settle_and_verify(&c, &acked).await;
    eprintln!("seed {seed}: {verified} mid-run verifies, {actions:?}\n{v:?}");
    c.shutdown();
}
