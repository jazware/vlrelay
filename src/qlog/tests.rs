//! In-process clusters: three nodes on real TCP, each on its own runtime so
//! a crash is a runtime shut down (memory and sockets gone at once), with
//! partitions from `node::Faults`. Every test runs the emission checker
//! over every node's emitted stream.

use super::check::{Checker, content_id};
use super::client::{Client, parse_test_frame, test_frame};
use super::commitlog::{self, CommitLog};
use super::emit::{Emitted, Emitter};
use super::flush;
use super::log::encode_cursors;
use super::node::{Config, Durability, Faults, LeaderRecord, MemoryOnly, Node, Role, SwitchStats, read_leader};
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

/// Each port handed out once per process, below the ephemeral range: an
/// OS-picked port can come back to a cluster running alongside, whose
/// nodes would then append to this one's.
fn free_port() -> u16 {
    static NEXT: AtomicU64 = AtomicU64::new(0);
    let base = 15_000 + (std::process::id() as u64 % 20) * 500;
    loop {
        let p = (base + NEXT.fetch_add(1, Ordering::Relaxed) % 500) as u16;
        if std::net::TcpListener::bind(("127.0.0.1", p)).is_ok() {
            return p;
        }
    }
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
        Cluster::with_spares(n, 0, disk, cfg, ring_bytes).await
    }

    /// `n` members started, and `spares` more ids with addresses that only
    /// start when a test starts them (to be added as learners).
    async fn with_spares(
        n: usize,
        spares: usize,
        disk: Option<(tempfile::TempDir, commitlog::Options)>,
        cfg: Option<ConfigFn>,
        ring_bytes: usize,
    ) -> Cluster {
        let ids: Vec<String> = (1..=n + spares).map(|i| format!("n{i}")).collect();
        let addrs = ids.iter().map(|id| (id.clone(), format!("127.0.0.1:{}", free_port()))).collect();
        let (tap, mut rx) = mpsc::unbounded_channel::<Emitted>();
        let checker = Arc::new(Mutex::new(Checker::new()));
        let ck = checker.clone();
        tokio::spawn(async move {
            while let Some(e) = rx.recv().await {
                let stream = format!("{}#{}", e.node, e.incarnation);
                let mut c = ck.lock();
                for (seq, data) in &e.events {
                    match parse_test_frame(data) {
                        Some((_, did, _)) => {
                            c.observe_event(&stream, *seq as u64, content_id(data), content_id(did.as_bytes()))
                        }
                        None => c.observe(&stream, *seq as u64, content_id(data)),
                    }
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
        for id in &ids[..n] {
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
        let bucket = super::bucket::Bucket::new(self.store.clone());
        let emit = Emitter::with_store(id, inc, self.ring_bytes, Some(self.tap.clone()), bucket.backfill.clone());
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
            bucket,
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

    /// The disk is gone: kill -9, and its commitlog with it.
    fn wipe(&mut self, id: &str) {
        self.kill(id);
        if let Some((dir, _)) = &self.disk {
            let _ = std::fs::remove_dir_all(dir.path().join(id));
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

    /// The leader's member set (every running node without a leader).
    fn members(&self) -> Vec<String> {
        let mut st: Vec<_> = self.nodes.values().map(|r| r.node.status()).filter(|s| s.role == Role::Leader).collect();
        st.sort_by_key(|s| s.epoch);
        st.pop().map_or_else(|| self.nodes.keys().cloned().collect(), |s| s.members)
    }

    /// Waits until every running member has emitted the same, full commit
    /// (a removed node stops where it was removed).
    async fn converge(&self, within: Duration) {
        let t = Instant::now();
        loop {
            let members = self.members();
            let st: Vec<_> = self.nodes.values().map(|r| r.node.status()).filter(|s| members.contains(&s.id)).collect();
            let top = st.iter().map(|s| s.last).max().unwrap_or(0);
            if st.iter().all(|s| s.emitted == top && s.commit == top) && st.iter().any(|s| s.role == Role::Leader) {
                // the tap is asynchronous: wait for the checker to have seen
                // every running node's stream reach the top
                let seen = {
                    let ck = self.checker.lock();
                    st.iter().all(|s| {
                        let inc = self.incarnations.get(&s.id).copied().unwrap_or(0);
                        // (a restarted node with nothing past its base emits nothing)
                        ck.last(&format!("{}#{inc}", s.id)).map_or(s.base >= top, |l| l >= top)
                    })
                };
                if seen {
                    tokio::time::sleep(Duration::from_millis(50)).await;
                    return;
                }
            }
            assert!(t.elapsed() < within, "no convergence within {within:?}: {st:#?}");
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
    }

    fn finish(&self, acked: &[(u64, u64)]) -> super::check::Report {
        let members = self.members();
        let mut c = self.checker.lock();
        for id in self.nodes.keys().filter(|id| !members.contains(id)) {
            c.removed(id);
        }
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
                        let cursors = if n > 0 { encode_cursors(&[(host.clone(), n)].into()) } else { Bytes::new() };
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
            format!(
                "{} {:?} e{} base {} last {} commit {} emitted {} intact {} gen {} resets {}",
                s.id, s.role, s.epoch, s.base, s.last, s.commit, s.emitted, s.intact, s.generation, s.resets
            )
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
    flush::Options { interval: Duration::from_millis(150), headroom: 10_000_000, segment_bytes: 32 << 10, crash: None }
}

/// Durable nodes that flush; `opts` per node id.
async fn flushing(opts: impl Fn(&str) -> flush::Options + Send + Sync + 'static, ring_bytes: usize) -> Cluster {
    flushing_n(3, opts, ring_bytes).await
}

async fn flushing_n(
    n: usize,
    opts: impl Fn(&str) -> flush::Options + Send + Sync + 'static,
    ring_bytes: usize,
) -> Cluster {
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
    Cluster::with_cfg(n, Some((tempfile::tempdir().unwrap(), o)), Some(Arc::new(cfg)), ring_bytes).await
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
    let cps = super::state::list_checkpoints(&c.store, super::state::DEFAULT_PATH).await.unwrap();
    assert_eq!(cps, vec![ck.state.unwrap().checkpoint], "stale checkpoints kept");
    c.shutdown();
}

/// With no flush after the first, the commit index stops at R = H: the
/// reservation holds back everything past it, on every node.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn commit_stops_at_the_reservation() {
    let c =
        flushing(|_| flush::Options { interval: Duration::from_secs(3600), headroom: 300, ..flush_opts() }, 64 << 20)
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
    let cps = super::state::list_checkpoints(&c.store, super::state::DEFAULT_PATH).await.unwrap();
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
    // flushes only when asked, so F stays where the test put it whatever
    // this box's speed
    let c = flushing(|_| flush::Options { interval: Duration::from_secs(3600), ..flush_opts() }, 32 << 10).await;
    let l = c.wait_leader(Duration::from_secs(5)).await;
    let load = Load::start(c.client(), 4, 10, Duration::from_millis(2));
    let leader = c.nodes[&l].node.clone();
    let until_commit = |n: u64| {
        let leader = leader.clone();
        async move {
            let t = Instant::now();
            while leader.status().commit < n {
                assert!(t.elapsed() < Duration::from_secs(20), "commit didn't reach {n}");
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
        }
    };
    until_commit(1000).await;
    let f = leader.status().commit;
    leader.flush.request(f);
    wait_flushed(&c, f, Duration::from_secs(10)).await;
    until_commit(f + 1000).await;
    let acked = load.stop().await;
    c.converge(Duration::from_secs(10)).await;
    let m = flush::read_manifest(&c.store).await.unwrap().unwrap().0;
    let id = c.ids.iter().find(|i| **i != l).unwrap().clone();
    let node = c.nodes[&id].node.clone();
    let top = node.status().commit;
    assert!(m.flushed > 0 && m.flushed + 500 < top, "want F well behind the head: F {} head {top}", m.flushed);
    let by_seq: HashMap<u64, u64> = acked.iter().copied().collect();
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    c.nodes[&id]
        .rt
        .spawn(async move { axum::serve(listener, super::emit::router(node, super::emit::Admin::Open)).await });
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

/// A follower down long enough that the leader's disk no longer reaches
/// back to it catches up from the bucket segments, not by a reset (which
/// would be a gap in its stream).
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_follower_behind_the_leaders_disk_catches_up_from_the_bucket() {
    let mut c = flushing(|_| flush_opts(), 64 << 20).await;
    for id in c.ids.clone() {
        c.kill(&id);
    }
    let inner = c.cfg.clone().unwrap();
    c.cfg = Some(Arc::new(move |id: &str, addrs: &HashMap<String, String>| {
        let mut k = inner(id, addrs);
        k.laggard_grace = Duration::from_millis(500);
        k
    }));
    for id in c.ids.clone() {
        c.start(&id).await;
    }
    let l = c.wait_leader(Duration::from_secs(5)).await;
    let f = c.ids.iter().find(|id| **id != l).unwrap().clone();
    let load = Load::start(c.client(), 4, 10, Duration::from_millis(1));
    tokio::time::sleep(Duration::from_millis(300)).await;
    let behind = c.nodes[&f].node.status().last;
    c.kill(&f);
    // until the leader can't serve it from memory or disk, however fast
    // this box fills and trims the leader's commitlog
    let t = Instant::now();
    while c.nodes[&l].node.readable_floor() <= behind + 1 {
        assert!(t.elapsed() < Duration::from_secs(30), "the leader's disk still reaches {behind}: {}", status_line(&c));
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    wait_flushed(&c, behind + 1, Duration::from_secs(10)).await;
    c.start(&f).await;
    let acked = load.stop().await;
    settle_and_verify(&c, &acked).await;
    let st = c.nodes[&f].node.status();
    let reads: u64 = c.nodes.values().map(|r| r.node.status().bucket_reads).sum();
    assert!(reads > 0, "nothing was served from the bucket (the leader's disk reached back): {}", status_line(&c));
    assert_eq!((st.resets, st.emit_gaps), (0, 0), "{st:?}");
    c.shutdown();
}

// ---- bucket recovery and the single node (Phase 4)

/// Host owners, one per host `t{w}` with a client of its own, that re-read
/// their host from a recovery's cursors when an ack says one happened
/// (the PDS replays from there): events after the cursor are sent again and
/// come back above R. Acks are (seq, frame content id, DID content id).
struct HostLoad {
    stop: Arc<AtomicBool>,
    acked: Arc<Mutex<Vec<(u64, u64, u64)>>>,
    tasks: Vec<tokio::task::JoinHandle<()>>,
    /// Each host's highest event number sent.
    sent: Arc<Mutex<HashMap<String, u64>>>,
    rewinds: Arc<AtomicU64>,
}

impl HostLoad {
    fn start(c: &Cluster, n: usize, batch: u64, pause: Duration) -> HostLoad {
        let stop = Arc::new(AtomicBool::new(false));
        let acked: Arc<Mutex<Vec<(u64, u64, u64)>>> = Arc::default();
        let sent: Arc<Mutex<HashMap<String, u64>>> = Arc::default();
        let rewinds = Arc::new(AtomicU64::new(0));
        let tasks = (0..n)
            .map(|w| {
                let client = c.client();
                let (stop, acked, sent, rewinds) = (stop.clone(), acked.clone(), sent.clone(), rewinds.clone());
                tokio::spawn(async move {
                    let host = format!("t{w}");
                    // every event up to n is acked (and, after a rewind, in the log)
                    let mut n = 0u64;
                    // after a rewind, everything sent before is sent again
                    // even once stopped
                    let mut high = 0u64;
                    while !stop.load(Ordering::Acquire) || n < high {
                        let cg = client.generation();
                        let cursors = if n > 0 { encode_cursors(&[(host.clone(), n)].into()) } else { Bytes::new() };
                        let dids: Vec<String> = (n + 1..=n + batch).map(|i| format!("did:q:{host}:{i}")).collect();
                        let frames: Vec<(Bytes, Bytes)> = dids.iter().map(|d| test_frame(d, 64, 0)).collect();
                        let a = client.submit_acked(frames.clone(), cursors, cg).await;
                        assert_eq!(a.n, batch);
                        {
                            let mut ak = acked.lock();
                            for (i, ((p, s), d)) in frames.iter().zip(&dids).enumerate() {
                                let seq = a.first + i as u64;
                                ak.push((seq, content_id(&wire::splice_seq(p, s, seq)), content_id(d.as_bytes())));
                            }
                        }
                        n += batch;
                        high = high.max(n);
                        {
                            let mut m = sent.lock();
                            let e = m.entry(host.clone()).or_default();
                            *e = (*e).max(n);
                        }
                        if a.generation > client.generation() {
                            let (g, cur) = client.recovery_cursors(a.generation).await;
                            n = cur.get(&host).copied().unwrap_or(0);
                            client.rewound(g);
                            rewinds.fetch_add(1, Ordering::Relaxed);
                        }
                        tokio::time::sleep(pause).await;
                    }
                })
            })
            .collect();
        HostLoad { stop, acked, tasks, sent, rewinds }
    }

    /// (acks, every event's DID content id).
    async fn stop(self) -> (Vec<(u64, u64, u64)>, Vec<u64>) {
        self.stop.store(true, Ordering::Release);
        for t in self.tasks {
            tokio::time::timeout(Duration::from_secs(30), t).await.expect("submitter stuck").unwrap();
        }
        let expected = self
            .sent
            .lock()
            .iter()
            .flat_map(|(h, &n)| (1..=n).map(move |i| content_id(format!("did:q:{h}:{i}").as_bytes())))
            .collect();
        (std::mem::take(&mut *self.acked.lock()), expected)
    }
}

impl Cluster {
    fn finish_recovered(
        &self,
        acked: &[(u64, u64, u64)],
        gaps: &[(u64, u64)],
        expected: &[u64],
    ) -> super::check::Report {
        let members = self.members();
        let mut c = self.checker.lock();
        for id in self.nodes.keys().filter(|id| !members.contains(id)) {
            c.removed(id);
        }
        for &(s, h, d) in acked {
            c.acked_event(s, h, d);
        }
        let logs: Vec<(String, Vec<(u64, u64)>)> = self
            .nodes
            .iter()
            .map(|(id, r)| (id.clone(), r.node.committed().into_iter().map(|(s, d)| (s, content_id(&d))).collect()))
            .collect();
        c.finish_with(&logs, gaps, Some(expected))
    }

    fn recoveries(&self) -> Vec<flush::RecoveryStats> {
        self.nodes.values().flat_map(|r| r.node.status().recovered).collect()
    }
}

/// Converges, flushes to the top and verifies (segments across the gaps,
/// the state equal to replaying them, cursors), then runs the checker with
/// the gaps and every event the hosts sent.
async fn settle_recovered(c: &Cluster, load: HostLoad) -> (flush::Verified, super::check::Report, flush::Manifest) {
    let (acked, expected) = load.stop().await;
    c.converge(Duration::from_secs(20)).await;
    let top = c.nodes.values().map(|r| r.node.status().commit).max().unwrap();
    let m = wait_flushed(c, top, Duration::from_secs(10)).await;
    let v = verify(c).await;
    assert!(v.ok, "manifest inconsistent: {v:#?}");
    let skipped: u64 = m.gaps.iter().map(|(a, u)| u - a).sum();
    assert_eq!(v.entries + skipped, top, "the bucket doesn't hold the whole log: {v:#?} {m:?}");
    let r = c.finish_recovered(&acked, &m.gaps, &expected);
    assert!(r.ok, "checker: {r:#?}");
    assert_eq!((r.holes, r.events_lost, r.acked_missing), (0, 0, 0), "{r:#?}");
    (v, r, m)
}

/// All three disks gone at once under load: the cluster comes back from
/// the bucket at R + 1 on its own, hosts re-ingest from the manifest's
/// cursors, and nothing a host sent is lost: every event is in the log at
/// or below the recovery point or again above R.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn wiping_every_disk_recovers_from_the_bucket_at_r_plus_one() {
    let mut c = flushing(|_| flush::Options { headroom: 5_000, ..flush_opts() }, 64 << 20).await;
    c.wait_leader(Duration::from_secs(5)).await;
    let load = HostLoad::start(&c, 4, 10, Duration::from_millis(2));
    for round in 0..2 {
        wait_flushed(&c, 1, Duration::from_secs(5)).await;
        tokio::time::sleep(Duration::from_millis(700)).await;
        let before = flush::read_manifest(&c.store).await.unwrap().unwrap().0;
        for id in c.ids.clone() {
            c.wipe(&id);
        }
        for id in c.ids.clone() {
            c.start(&id).await;
        }
        let l = c.wait_leader(Duration::from_secs(10)).await;
        let st = c.nodes[&l].node.status();
        eprintln!(
            "round {round}: {}
{:?}",
            status_line(&c),
            c.recoveries()
        );
        tokio::time::sleep(Duration::from_millis(300)).await;
        eprintln!("round {round} +300ms: {}", status_line(&c));
        assert_eq!(st.generation, round + 1, "{st:?}");
        let m = flush::read_manifest(&c.store).await.unwrap().unwrap().0;
        let rec = m.recovery.clone().unwrap();
        assert!(rec.base >= before.reserve && rec.after >= before.flushed, "{rec:?} after {before:?}");
        assert!(st.commit >= rec.base, "{st:?}");
    }
    tokio::time::sleep(Duration::from_millis(800)).await;
    let rewinds = load.rewinds.load(Ordering::Relaxed);
    let (v, r, m) = settle_recovered(&c, load).await;
    eprintln!("{v:?}\n{r:?}\ngaps {:?} rewinds {rewinds} {:#?}", m.gaps, c.recoveries());
    assert_eq!(m.gaps.len(), 2, "{m:?}");
    assert!(rewinds >= 4 && r.reingested + r.duplicates > 0, "{r:?}");
    c.shutdown();
}

/// Two disks gone, one survivor. If the survivor leads and the others are
/// back within its election timeout, it never loses its quorum: they start
/// empty and catch up from it, with no jump. Down for longer (or with a
/// follower surviving), the quorum is lost (one intact log can't make two)
/// and the bucket recovery runs, keeping the survivor's committed entries
/// past F with their seqs.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn wiping_two_disks_salvages_the_survivors_log() {
    let mut c = flushing(
        // long enough that a survivor has committed entries past F
        |_| flush::Options { interval: Duration::from_millis(2500), headroom: 50_000, ..flush_opts() },
        64 << 20,
    )
    .await;
    let load = HostLoad::start(&c, 4, 10, Duration::from_millis(2));
    let mut salvaged = 0;
    let mut recoveries = 0u64;
    for round in 0..4 {
        let l = c.wait_leader(Duration::from_secs(10)).await;
        wait_flushed(&c, 1, Duration::from_secs(5)).await;
        tokio::time::sleep(Duration::from_millis(400)).await;
        let keep = if round < 2 { l.clone() } else { c.ids.iter().find(|id| **id != l).unwrap().clone() };
        for id in c.ids.clone() {
            if id != keep {
                c.wipe(&id);
            }
        }
        if round > 0 {
            tokio::time::sleep(Duration::from_millis(1000)).await;
        }
        for id in c.ids.clone() {
            if id != keep {
                c.start(&id).await;
            }
        }
        if round == 0 {
            c.wait_leader(Duration::from_secs(5)).await;
            tokio::time::sleep(Duration::from_millis(500)).await;
            assert!(c.recoveries().is_empty(), "{}", status_line(&c));
            assert_eq!(c.nodes[&keep].node.status().role, Role::Leader, "{}", status_line(&c));
            continue;
        }
        recoveries += 1;
        let t = Instant::now();
        let rec = loop {
            // (the node that ran an earlier one may have been wiped since)
            if let Some(r) = c.recoveries().into_iter().find(|r| r.generation == recoveries) {
                break r;
            }
            assert!(t.elapsed() < Duration::from_secs(10), "no recovery: {}", status_line(&c));
            tokio::time::sleep(Duration::from_millis(20)).await;
        };
        salvaged += rec.salvaged;
        eprintln!("round {round}, survivor {keep}: {rec:?}");
    }
    let (v, r, m) = settle_recovered(&c, load).await;
    eprintln!("{v:?}\n{r:?}\ngaps {:?}", m.gaps);
    assert_eq!(m.generation(), 3, "{m:?}");
    assert!(salvaged > 0, "nothing was salvaged from the survivors");
    c.shutdown();
}

/// Never while an intact quorum could exist: one wiped disk plus a dead
/// leader leaves one intact log answering and one member silent, which
/// could be intact. Nobody recovers from the bucket; the dead leader's
/// return makes a normal takeover, with no jump.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn no_recovery_while_a_silent_member_could_be_intact() {
    let mut c = flushing(|_| flush_opts(), 64 << 20).await;
    let l = c.wait_leader(Duration::from_secs(5)).await;
    let load = HostLoad::start(&c, 4, 10, Duration::from_millis(2));
    wait_flushed(&c, 1, Duration::from_secs(5)).await;
    let wiped = c.ids.iter().find(|id| **id != l).unwrap().clone();
    c.kill(&l);
    c.wipe(&wiped);
    c.start(&wiped).await;
    tokio::time::sleep(Duration::from_secs(3)).await;
    let lost: u64 = c.nodes.values().map(|r| r.node.status().lost_quorums).sum();
    assert_eq!(lost, 0, "{}", status_line(&c));
    assert!(c.leader().is_none(), "a leader without an intact quorum: {}", status_line(&c));
    c.start(&l).await;
    c.wait_leader(Duration::from_secs(10)).await;
    let (_, r, m) = settle_recovered(&c, load).await;
    assert!(m.gaps.is_empty() && m.recovery.is_none(), "{m:?}");
    assert_eq!(r.jumped, 0);
    assert!(c.recoveries().is_empty());
    // the wiped node started at the leader's oldest local entry, not seq 1
    let st = c.nodes[&wiped].node.status();
    assert_eq!(st.emit_gaps, 0, "{st:?}");
    c.shutdown();
}

/// A flush that uploaded its segments and died before its manifest CAS,
/// then every disk gone: recovery adopts those orphan segments (committed
/// entries) and F moves to their end before the jump.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn recovery_adopts_orphan_segments() {
    use flush::Step;
    let armed = Arc::new(AtomicBool::new(false));
    let (tx, mut rx) = mpsc::unbounded_channel::<String>();
    let a = armed.clone();
    let mut c = flushing(
        move |id| {
            let (a, t, id) = (a.clone(), tx.clone(), id.to_string());
            flush::Options {
                crash: Some(Arc::new(move |s| {
                    if s == Step::BeforeManifest && a.swap(false, Ordering::AcqRel) {
                        let _ = t.send(id.clone());
                        return true;
                    }
                    false
                })),
                headroom: 5_000,
                ..flush_opts()
            }
        },
        64 << 20,
    )
    .await;
    c.wait_leader(Duration::from_secs(5)).await;
    let load = HostLoad::start(&c, 4, 10, Duration::from_millis(2));
    wait_flushed(&c, 1, Duration::from_secs(5)).await;
    armed.store(true, Ordering::Release);
    tokio::time::timeout(Duration::from_secs(5), rx.recv()).await.unwrap().unwrap();
    let v = verify(&c).await;
    assert!(v.orphans > 0, "{v:?}");
    for id in c.ids.clone() {
        c.wipe(&id);
    }
    for id in c.ids.clone() {
        c.start(&id).await;
    }
    c.wait_leader(Duration::from_secs(10)).await;
    let rec = c.recoveries().pop().unwrap();
    assert!(rec.orphan_segments > 0 && rec.orphans_to > rec.manifest_flushed, "{rec:?}");
    let (v, r, _) = settle_recovered(&c, load).await;
    eprintln!("{rec:?}\n{v:?}\n{r:?}");
    assert_eq!(v.orphans, 0);
    c.shutdown();
}

/// The recovery manifest itself, before any flush follows it: the state
/// is the old checkpoint cloned, plus what was adopted, at exactly R, and
/// equals replaying the bucket's segments (across the gap) to R. A crash
/// at each recovery step leaves the old manifest or the new one whole, and
/// the next attempt finishes it.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn the_recovered_state_equals_replaying_the_bucket() {
    use flush::Step;
    let armed: Arc<Mutex<Vec<Step>>> = Arc::default();
    let a = armed.clone();
    let mut c = flushing(
        move |_| {
            let a = a.clone();
            flush::Options {
                crash: Some(Arc::new(move |s| {
                    let mut g = a.lock();
                    if g.first() == Some(&s) {
                        g.remove(0);
                        return true;
                    }
                    false
                })),
                headroom: 5_000,
                ..flush_opts()
            }
        },
        64 << 20,
    )
    .await;
    c.wait_leader(Duration::from_secs(5)).await;
    let load = HostLoad::start(&c, 4, 10, Duration::from_millis(2));
    wait_flushed(&c, 1, Duration::from_secs(5)).await;
    tokio::time::sleep(Duration::from_millis(500)).await;
    // three attempts die (one at each step), the fourth leads and stops
    // its flush at the fence, so the recovery manifest stays current
    *armed.lock() = vec![Step::RecoverSealed, Step::RecoverBeforeManifest, Step::RecoverAfterManifest, Step::Fenced];
    for id in c.ids.clone() {
        c.wipe(&id);
    }
    for id in c.ids.clone() {
        c.start(&id).await;
    }
    let t = Instant::now();
    while !armed.lock().is_empty() {
        assert!(t.elapsed() < Duration::from_secs(20), "steps left {:?}: {}", armed.lock(), status_line(&c));
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
    let l = c.wait_leader(Duration::from_secs(10)).await;
    let m = flush::read_manifest(&c.store).await.unwrap().unwrap().0;
    let rec = m.recovery.clone().unwrap();
    // the attempt that died after its manifest CAS did recover (it may
    // have led and committed above its R for all anyone can tell), so the
    // next one jumped again
    assert_eq!((rec.generation, m.flushed, m.state.as_ref().unwrap().seq), (2, rec.base, rec.base), "{m:?}");
    assert_ne!(m.state_path(), super::state::DEFAULT_PATH);
    let v = verify(&c).await;
    assert!(v.ok && v.gaps == 2 && v.flushed == rec.base, "{v:#?}");
    let applied = super::state::read_checkpoint(&c.store, m.state.as_ref().unwrap()).await.unwrap().0;
    assert_eq!(
        applied.get(super::state::applied_key()).map(|b| u64::from_be_bytes(b[..8].try_into().unwrap())),
        Some(rec.base)
    );
    // flushing again from here (a takeover fences and flushes as usual)
    c.kill(&l);
    c.start(&l).await;
    c.wait_leader(Duration::from_secs(10)).await;
    let (v, r, m) = settle_recovered(&c, load).await;
    eprintln!("{v:?}\n{r:?}\n{m:?}");
    assert_eq!(m.generation(), 2, "{m:?}");
    c.shutdown();
}

/// Single-node mode: a quorum of one, its commitlog the WAL and the emit
/// point. kill -9 and power cuts lose nothing acked and make no jump; a
/// wiped disk comes back from the bucket at R + 1.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_single_node_recovers_from_its_wal_and_from_the_bucket() {
    let mut rng = rand::rngs::StdRng::seed_from_u64(7);
    let mut c = flushing_n(1, |_| flush::Options { headroom: 5_000, ..flush_opts() }, 64 << 20).await;
    c.wait_leader(Duration::from_secs(5)).await;
    let load = HostLoad::start(&c, 4, 10, Duration::from_millis(2));
    for i in 0..6 {
        tokio::time::sleep(Duration::from_millis(400)).await;
        if i % 2 == 0 {
            c.kill("n1");
        } else {
            c.power_cut("n1", &mut rng);
        }
        c.start("n1").await;
        c.wait_leader(Duration::from_secs(5)).await;
    }
    assert!(c.recoveries().is_empty() && flush::read_manifest(&c.store).await.unwrap().unwrap().0.gaps.is_empty());
    wait_flushed(&c, 1, Duration::from_secs(5)).await;
    tokio::time::sleep(Duration::from_millis(400)).await;
    c.wipe("n1");
    c.start("n1").await;
    c.wait_leader(Duration::from_secs(5)).await;
    assert_eq!(c.recoveries().len(), 1);
    tokio::time::sleep(Duration::from_millis(600)).await;
    let (v, r, m) = settle_recovered(&c, load).await;
    eprintln!("{v:?}\n{r:?}\n{m:?}");
    assert_eq!(m.gaps.len(), 1);
    c.shutdown();
}

// ---- membership (Phase 5)

/// `n` durable, flushing members (the bootstrap set) and `spares` more ids
/// that can be started and added.
async fn members_cluster(
    n: usize,
    spares: usize,
    opts: impl Fn(&str) -> flush::Options + Send + Sync + 'static,
) -> Cluster {
    let o = commitlog::Options {
        segment_bytes: 256 << 10,
        retain_bytes: 1 << 20,
        memory_bytes: 64 << 10,
        ..commitlog::Options::default()
    };
    let first: Vec<String> = (1..=n).map(|i| format!("n{i}")).collect();
    let cfg = move |id: &str, addrs: &HashMap<String, String>| {
        let mut k = config(id, addrs);
        k.members = first.clone();
        k.retain_bytes = 64 << 10;
        k.flush = Some(opts(id));
        k.switch_timeout = Duration::from_secs(5);
        k.catch_up_timeout = Duration::from_secs(20);
        k
    };
    Cluster::with_spares(n, spares, Some((tempfile::tempdir().unwrap(), o)), Some(Arc::new(cfg)), 64 << 20).await
}

impl Cluster {
    /// A membership change on `id`, run on its own runtime (a kill takes
    /// it down with the node, as with a process).
    fn change_on(&self, id: &str, target: &[String]) -> tokio::task::JoinHandle<anyhow::Result<SwitchStats>> {
        let n = self.nodes[id].node.clone();
        let t = target.to_vec();
        self.nodes[id].rt.spawn(async move { n.change_members(t, Default::default()).await })
    }

    async fn record(&self) -> LeaderRecord {
        read_leader(&self.store).await.unwrap().unwrap().0
    }

    /// Until `qlog/leader` holds `target`: a change on whoever leads, again
    /// after each one that fails.
    async fn change_to(&self, target: &[&str], within: Duration) -> Vec<SwitchStats> {
        let mut target: Vec<String> = target.iter().map(|s| s.to_string()).collect();
        target.sort();
        let t = Instant::now();
        let mut done = Vec::new();
        loop {
            if self.record().await.members == target {
                return done;
            }
            if let Some(l) = self.leader() {
                match self.change_on(&l, &target).await {
                    Ok(Ok(s)) => {
                        eprintln!("change on {l}: {s:?}");
                        done.push(s);
                    }
                    Ok(Err(e)) => eprintln!("change on {l}: {e:#}"),
                    Err(e) => eprintln!("change on {l}: {e}"),
                }
            }
            assert!(t.elapsed() < within, "no change to {target:?} within {within:?}: {}", status_line(self));
            tokio::time::sleep(Duration::from_millis(100)).await;
        }
    }

    fn commit_of(&self, id: &str) -> u64 {
        self.nodes[id].node.status().commit
    }

    /// Until a node leads at least `epoch` (one of `among`).
    async fn wait_leading(&self, among: &[String], within: Duration) -> (String, u64) {
        let t = Instant::now();
        loop {
            if let Some(l) = self.leader()
                && among.contains(&l)
            {
                return (l.clone(), self.nodes[&l].node.status().epoch);
            }
            assert!(t.elapsed() < within, "none of {among:?} leads within {within:?}: {}", status_line(self));
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    }
}

fn others<'a>(all: &'a [&'a str], not: &[&str]) -> Vec<&'a str> {
    all.iter().copied().filter(|x| !not.contains(x)).collect()
}

/// A removed member never holds an entry, or a promise, from the epoch that
/// removed it on: no leader of that epoch or later can have counted it.
fn assert_never_counted(c: &Cluster, id: &str, from_epoch: u64) {
    if let Some(r) = c.nodes.get(id) {
        let s = r.node.status();
        assert!(s.promised < from_epoch && s.last_epoch < from_epoch, "{id} after its removal at {from_epoch}: {s:?}");
        let since = c.nodes.values().map(|r| r.node.status()).find(|s| s.role == Role::Leader).map(|s| s.members_since);
        assert!(since.is_none_or(|e| e >= from_epoch), "members since {since:?}, removed at {from_epoch}");
    }
}

/// Replace a follower with a new id under load: the new box catches up as a
/// learner, the switch moves the set at a flush barrier, the new member
/// counts at once (the leader commits with it while the third is down) and
/// the removed one, still running, never counts or campaigns again.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn replacing_a_follower_under_load() {
    let mut c = members_cluster(3, 1, |_| flush_opts()).await;
    let l = c.wait_leader(Duration::from_secs(5)).await;
    let load = Load::start(c.client(), 4, 10, Duration::from_millis(2));
    tokio::time::sleep(Duration::from_millis(400)).await;
    let ids = ["n1", "n2", "n3"];
    let rest = others(&ids, &[&l]);
    let (gone, kept) = (rest[0], rest[1]);
    c.start("n4").await;
    let e0 = c.record().await.epoch;
    let sw = c.change_to(&[&l, kept, "n4"], Duration::from_secs(20)).await;
    let rec = c.record().await;
    assert_eq!((rec.epoch, rec.leader.as_str()), (e0 + 1, l.as_str()), "{rec:?} {sw:?}");
    assert!(rec.learners.is_empty());
    tokio::time::sleep(Duration::from_millis(300)).await;
    c.kill(kept);
    let before = c.commit_of(&l);
    tokio::time::sleep(Duration::from_millis(500)).await;
    assert!(c.commit_of(&l) > before, "the leader and the new member don't commit: {}", status_line(&c));
    c.start(kept).await;
    tokio::time::sleep(Duration::from_millis(1500)).await;
    assert_never_counted(&c, gone, e0 + 1);
    assert!(c.nodes[gone].node.status().retired, "{}", status_line(&c));
    let acked = load.stop().await;
    settle_and_verify(&c, &acked).await;
    assert_never_counted(&c, gone, e0 + 1);
    c.shutdown();
}

/// Replace the leader with a new id under load: it hands epoch + 1 to a
/// member holding the barrier, which leads without waiting out the timeout.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn replacing_the_leader_hands_off_under_load() {
    let mut c = members_cluster(3, 1, |_| flush_opts()).await;
    let l = c.wait_leader(Duration::from_secs(5)).await;
    let load = Load::start(c.client(), 4, 10, Duration::from_millis(2));
    tokio::time::sleep(Duration::from_millis(400)).await;
    let ids = ["n1", "n2", "n3"];
    let rest = others(&ids, &[&l]);
    c.start("n4").await;
    let e0 = c.record().await.epoch;
    let target = [rest[0], rest[1], "n4"];
    c.change_to(&target, Duration::from_secs(20)).await;
    let t: Vec<String> = target.iter().map(|s| s.to_string()).collect();
    let (nl, ne) = c.wait_leading(&t, Duration::from_secs(5)).await;
    let rec = c.record().await;
    // the handoff: the named member leads epoch + 1 itself, no timeout
    assert_eq!((ne, rec.epoch, rec.leader.as_str()), (e0 + 1, e0 + 1, nl.as_str()), "{}", status_line(&c));
    let st = c.nodes[&l].node.status();
    assert!(st.retired && st.role == Role::Follower, "{st:?}");
    tokio::time::sleep(Duration::from_millis(300)).await;
    // n4 counts: the new leader commits with it alone
    let other = if nl == rest[0] { rest[1] } else { rest[0] };
    c.kill(other);
    let before = c.commit_of(&nl);
    tokio::time::sleep(Duration::from_millis(500)).await;
    assert!(c.commit_of(&nl) > before, "{}", status_line(&c));
    c.start(other).await;
    let acked = load.stop().await;
    settle_and_verify(&c, &acked).await;
    assert_never_counted(&c, &l, e0 + 1);
    c.shutdown();
}

/// 3 -> 5 -> 3 under load. With five members the quorum is three: two down
/// and it carries on. Then back to three, a different three.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn growing_to_five_and_shrinking_back() {
    let mut c = members_cluster(3, 2, |_| flush_opts()).await;
    let l = c.wait_leader(Duration::from_secs(5)).await;
    let load = Load::start(c.client(), 4, 10, Duration::from_millis(2));
    tokio::time::sleep(Duration::from_millis(300)).await;
    c.start("n4").await;
    c.start("n5").await;
    let all = ["n1", "n2", "n3", "n4", "n5"];
    c.change_to(&all, Duration::from_secs(20)).await;
    let e5 = c.record().await.epoch;
    let down = others(&all, &[&l]);
    c.kill(down[0]);
    c.kill(down[1]);
    let before = c.commit_of(&l);
    tokio::time::sleep(Duration::from_millis(500)).await;
    assert!(c.commit_of(&l) > before, "three of five don't commit: {}", status_line(&c));
    c.start(down[0]).await;
    c.start(down[1]).await;
    tokio::time::sleep(Duration::from_millis(500)).await;
    // the three left are the two spares and one original, maybe not the leader
    let keep = ["n3", "n4", "n5"];
    c.change_to(&keep, Duration::from_secs(20)).await;
    let e3 = c.record().await.epoch;
    assert!(e3 > e5);
    tokio::time::sleep(Duration::from_millis(1500)).await;
    for gone in ["n1", "n2"] {
        assert_never_counted(&c, gone, e3);
    }
    let acked = load.stop().await;
    settle_and_verify(&c, &acked).await;
    c.shutdown();
}

/// A learner holds the log but never counts: with only the leader and the
/// learner up nothing commits, and with only one member and the learner up
/// nobody takes over. A removed member, still running, never takes over
/// either, even when it and a new member are all that's left.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn learners_never_count_and_removed_members_never_lead() {
    use flush::Step;
    let armed: Arc<Mutex<Option<Step>>> = Arc::default();
    let a = armed.clone();
    let mut c = members_cluster(3, 1, move |_| {
        let a = a.clone();
        flush::Options {
            crash: Some(Arc::new(move |s| {
                let mut g = a.lock();
                if *g == Some(s) {
                    *g = None;
                    return true;
                }
                false
            })),
            ..flush_opts()
        }
    })
    .await;
    let l = c.wait_leader(Duration::from_secs(5)).await;
    let load = Load::start(c.client(), 4, 10, Duration::from_millis(2));
    tokio::time::sleep(Duration::from_millis(300)).await;
    c.start("n4").await;
    // the change stops after the learner caught up: it stays a learner
    *armed.lock() = Some(Step::SwitchBeforePause);
    let all4: Vec<String> = ["n1", "n2", "n3", "n4"].iter().map(|s| s.to_string()).collect();
    let r = c.change_on(&l, &all4).await.unwrap();
    assert!(r.is_err(), "{r:?}");
    c.kill(&l);
    c.start(&l).await;
    let l = c.wait_leader(Duration::from_secs(5)).await;
    let rec = c.record().await;
    assert_eq!(rec.learners, vec!["n4".to_string()], "{rec:?}");
    tokio::time::sleep(Duration::from_millis(500)).await;
    assert!(c.commit_of("n4") > 0 && c.nodes["n4"].node.status().role == Role::Follower);
    // only the leader and the learner
    let ids = ["n1", "n2", "n3"];
    let rest = others(&ids, &[&l]);
    c.kill(rest[0]);
    c.kill(rest[1]);
    tokio::time::sleep(Duration::from_millis(200)).await;
    let (cl, c4) = (c.commit_of(&l), c.commit_of("n4"));
    tokio::time::sleep(Duration::from_millis(800)).await;
    assert_eq!((c.commit_of(&l), c.commit_of("n4")), (cl, c4), "a learner counted: {}", status_line(&c));
    c.start(rest[0]).await;
    c.start(rest[1]).await;
    let l = c.wait_leader(Duration::from_secs(5)).await;
    tokio::time::sleep(Duration::from_millis(500)).await;
    // only one member and the learner: nobody takes over
    let rest = others(&ids, &[&l]);
    c.kill(&l);
    c.kill(rest[0]);
    tokio::time::sleep(Duration::from_millis(2000)).await;
    assert_eq!(c.leader(), None, "{}", status_line(&c));
    c.start(&l).await;
    c.start(rest[0]).await;
    let l = c.wait_leader(Duration::from_secs(5)).await;
    // now make n4 a member and remove one, which keeps running
    let rest = others(&ids, &[&l]);
    let (gone, kept) = (rest[0], rest[1]);
    c.change_to(&[&l, kept, "n4"], Duration::from_secs(20)).await;
    let e = c.record().await.epoch;
    tokio::time::sleep(Duration::from_millis(300)).await;
    // the removed member and n4 are all that's left: nobody leads
    c.kill(&l);
    c.kill(kept);
    tokio::time::sleep(Duration::from_millis(2500)).await;
    assert_eq!(c.leader(), None, "{}", status_line(&c));
    assert_never_counted(&c, gone, e);
    c.start(&l).await;
    c.start(kept).await;
    c.wait_leader(Duration::from_secs(5)).await;
    let acked = load.stop().await;
    settle_and_verify(&c, &acked).await;
    assert_never_counted(&c, gone, e);
    c.shutdown();
}

/// A crash at every step of a membership change (catch-up, before the
/// pause, after the barrier's flush, after the CAS), each killing the
/// leader right there, replacing a follower and then the leader in turn;
/// the change is retried on whoever leads until it lands. Nothing emitted
/// is lost and every manifest stays consistent.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn crashes_at_every_switch_step_lose_nothing() {
    use flush::Step;
    let armed: Arc<Mutex<Option<Step>>> = Arc::default();
    let (tx, mut rx) = mpsc::unbounded_channel::<(String, Step)>();
    let (a, t) = (armed.clone(), tx.clone());
    let mut c = members_cluster(3, 8, move |id| {
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
    })
    .await;
    c.wait_leader(Duration::from_secs(5)).await;
    let load = Load::start(c.client(), 4, 10, Duration::from_millis(2));
    let steps = [Step::SwitchCatchUp, Step::SwitchBeforePause, Step::SwitchFlushed, Step::SwitchCas];
    for (spare, (i, step)) in (4..).zip(steps.iter().cycle().take(8).enumerate()) {
        tokio::time::sleep(Duration::from_millis(300)).await;
        let l = c.wait_leader(Duration::from_secs(10)).await;
        let mut members = c.record().await.members;
        let new = format!("n{spare}");
        c.start(&new).await;
        let out = if i % 2 == 0 { members.iter().find(|m| **m != l).unwrap().clone() } else { l.clone() };
        members.retain(|m| *m != out);
        members.push(new.clone());
        members.sort();
        *armed.lock() = Some(*step);
        let mut h = c.change_on(&l, &members);
        let mut done = None;
        let fired = tokio::select! {
            biased;
            r = rx.recv() => Some(r.unwrap()),
            r = &mut h => {
                done = Some(r);
                rx.try_recv().ok()
            }
            _ = tokio::time::sleep(Duration::from_secs(30)) => panic!("no change reached {step:?}: {}", status_line(&c)),
        };
        let Some((id, s)) = fired else {
            // ended before the step (deposed by a takeover, say): no crash this round
            armed.lock().take();
            eprintln!("change ended before {step:?}: {done:?}");
            let m: Vec<&str> = members.iter().map(|s| s.as_str()).collect();
            c.change_to(&m, Duration::from_secs(30)).await;
            c.kill(&out);
            continue;
        };
        assert_eq!(id, l);
        let r = match done {
            Some(r) => r,
            None => h.await,
        };
        assert!(r.unwrap().is_err());
        c.kill(&id);
        let v = verify(&c).await;
        assert!(v.ok, "after a crash at {s:?}: {v:#?}");
        c.start(&id).await;
        let m: Vec<&str> = members.iter().map(|s| s.as_str()).collect();
        c.change_to(&m, Duration::from_secs(30)).await;
        let e = c.record().await.epoch;
        eprintln!("crash at {s:?} on {id} replacing {out} with {new}: epoch {e}");
        tokio::time::sleep(Duration::from_millis(300)).await;
        // a removed node goes the way a replaced box does
        c.kill(&out);
    }
    let acked = load.stop().await;
    settle_and_verify(&c, &acked).await;
    c.shutdown();
}

/// The leader cut off from every peer in the middle of a change: before the
/// barrier (the change times out and the majority takes over with the
/// learner still a learner) and after the barrier's flush (the CAS lands,
/// and the majority of the new set takes over from the record).
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_partition_during_a_switch_loses_nothing() {
    use flush::Step;
    let armed: Arc<Mutex<Option<Step>>> = Arc::default();
    let (tx, mut rx) = mpsc::unbounded_channel::<(String, Step)>();
    let (a, t) = (armed.clone(), tx.clone());
    let mut c = members_cluster(3, 2, move |id| {
        let (a, t, id) = (a.clone(), t.clone(), id.to_string());
        flush::Options {
            crash: Some(Arc::new(move |s| {
                let hit = {
                    let mut g = a.lock();
                    let hit = *g == Some(s);
                    if hit {
                        *g = None;
                    }
                    hit
                };
                if hit {
                    // the test cuts this node off while the change waits here
                    let _ = t.send((id.clone(), s));
                    std::thread::sleep(Duration::from_millis(200));
                }
                false
            })),
            ..flush_opts()
        }
    })
    .await;
    c.wait_leader(Duration::from_secs(5)).await;
    let load = Load::start(c.client(), 4, 10, Duration::from_millis(2));
    for (step, new) in [(Step::SwitchBeforePause, "n4"), (Step::SwitchFlushed, "n5")] {
        tokio::time::sleep(Duration::from_millis(300)).await;
        let l = c.wait_leader(Duration::from_secs(10)).await;
        let mut members = c.record().await.members;
        c.start(new).await;
        let out = members.iter().find(|m| **m != l).unwrap().clone();
        members.retain(|m| *m != out);
        members.push(new.to_string());
        members.sort();
        *armed.lock() = Some(step);
        let h = c.change_on(&l, &members);
        let (id, _) = tokio::time::timeout(Duration::from_secs(20), rx.recv()).await.unwrap().unwrap();
        c.isolate(&id);
        let r = h.await.unwrap();
        eprintln!("change with {id} cut off at {step:?}: {r:?}");
        tokio::time::sleep(Duration::from_millis(1500)).await;
        c.heal();
        let m: Vec<&str> = members.iter().map(|s| s.as_str()).collect();
        c.change_to(&m, Duration::from_secs(30)).await;
        tokio::time::sleep(Duration::from_millis(300)).await;
        c.kill(&out);
    }
    let acked = load.stop().await;
    settle_and_verify(&c, &acked).await;
    c.shutdown();
}

/// A single node grows into a three-node cluster under load (its WAL and
/// bucket carry over; the two new boxes catch up as learners), loses a
/// member without stopping, and shrinks back to one.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_single_node_grows_to_three_and_back() {
    let mut c = members_cluster(1, 2, |_| flush_opts()).await;
    c.wait_leader(Duration::from_secs(5)).await;
    let load = Load::start(c.client(), 4, 10, Duration::from_millis(2));
    tokio::time::sleep(Duration::from_millis(400)).await;
    c.start("n2").await;
    c.start("n3").await;
    c.change_to(&["n1", "n2", "n3"], Duration::from_secs(20)).await;
    tokio::time::sleep(Duration::from_millis(300)).await;
    let l = c.wait_leader(Duration::from_secs(5)).await;
    let other = others(&["n1", "n2", "n3"], &[&l])[0];
    c.kill(other);
    let before = c.commit_of(&l);
    tokio::time::sleep(Duration::from_millis(500)).await;
    assert!(c.commit_of(&l) > before, "{}", status_line(&c));
    c.start(other).await;
    tokio::time::sleep(Duration::from_millis(300)).await;
    let l = c.wait_leader(Duration::from_secs(5)).await;
    c.change_to(&[&l], Duration::from_secs(20)).await;
    let e = c.record().await.epoch;
    tokio::time::sleep(Duration::from_millis(1500)).await;
    for gone in others(&["n1", "n2", "n3"], &[&l]) {
        assert_never_counted(&c, gone, e);
    }
    let acked = load.stop().await;
    settle_and_verify(&c, &acked).await;
    c.shutdown();
}

async fn wipe_all_and_recover(c: &mut Cluster, generation: u64) {
    for id in c.ids.clone() {
        c.wipe(&id);
    }
    for id in c.ids.clone() {
        c.start(&id).await;
    }
    let l = c.wait_leader(Duration::from_secs(10)).await;
    let st = c.nodes[&l].node.status();
    assert_eq!(st.generation, generation, "{st:?}");
}

/// `qlog retain --apply` deletes what the report marks deletable and
/// nothing else: segments past the horizon (with `pruned_seq` raised to
/// their end), and a state path only when it's an older epoch's and no
/// state that's still read lists it. The manifest still verifies, and a
/// bucket recovery after the deletes works and loses nothing.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn retention_deletes_only_what_it_reports_and_recovery_still_works() {
    use super::retain;
    use object_store::ObjectStoreExt;
    let mut c = flushing(|_| flush::Options { headroom: 5_000, ..flush_opts() }, 64 << 20).await;
    c.wait_leader(Duration::from_secs(5)).await;
    let load = HostLoad::start(&c, 4, 10, Duration::from_millis(2));
    // two recoveries: the current state is a clone of a clone
    for g in 1..=2 {
        wait_flushed(&c, 1, Duration::from_secs(5)).await;
        tokio::time::sleep(Duration::from_millis(700)).await;
        wipe_all_and_recover(&mut c, g).await;
    }
    tokio::time::sleep(Duration::from_millis(1500)).await;
    let m = flush::read_manifest(&c.store).await.unwrap().unwrap().0;
    let current = m.state_path().to_string();
    let cur_epoch: u64 = current.strip_prefix("qlog/state-e").unwrap().parse().unwrap();
    // a recovery whose CAS lost (an older epoch, nobody reads it), and one
    // in progress (a newer epoch): only the first may go
    let (lost, running) = ("qlog/state-e1".to_string(), format!("qlog/state-e{}", cur_epoch + 1000));
    assert!(cur_epoch > 1);
    for p in [&lost, &running] {
        super::state::State::open(&c.store, p).await.unwrap().close().await;
    }

    let plan = retain::plan(&c.store, Duration::from_secs(1)).await.unwrap().unwrap();
    assert!(!plan.deletable.is_empty(), "{plan:#?}");
    let by_path = |p: &str| plan.states.iter().find(|s| s.path == p).unwrap_or_else(|| panic!("{p}: {plan:#?}"));
    assert!(by_path(&lost).deletable && !by_path(&running).deletable && !by_path(&current).deletable);
    let a = retain::apply(&c.store, &plan).await.unwrap();
    eprintln!("{a:#?}");
    assert_eq!(a.segments, plan.deletable.len() as u64);
    assert_eq!(a.pruned_seq, plan.pruned_seq_after);
    assert_eq!(retain::pruned_seq(&c.store).await.unwrap(), plan.pruned_seq_after);
    assert!(a.state_paths.contains(&lost) && !a.state_paths.contains(&running), "{a:#?}");
    let seg = |o: u64| vlpds::nodelog::segment_path(&c.store, flush::LOG_ID, o);
    for s in &plan.deletable {
        assert!(matches!(c.store.raw.head(&seg(s.ordinal)).await, Err(object_store::Error::NotFound { .. })));
    }
    assert!(c.store.raw.head(&seg(plan.deletable.last().unwrap().ordinal + 1)).await.is_ok());
    for sp in &plan.states {
        let admin =
            slatedb::admin::Admin::builder(super::state::db_path(&c.store, &sp.path), c.store.raw.clone()).build();
        let kept = admin.read_manifest(None).await.unwrap().is_some();
        assert_eq!(kept, !a.state_paths.contains(&sp.path), "{}", sp.path);
    }
    let v = verify(&c).await;
    assert!(v.ok && v.pruned == a.pruned_seq, "after the deletes: {v:#?}");

    // a recovery from the pruned bucket, under load
    wipe_all_and_recover(&mut c, 3).await;
    tokio::time::sleep(Duration::from_millis(800)).await;
    let (acked, expected) = load.stop().await;
    c.converge(Duration::from_secs(20)).await;
    let top = c.nodes.values().map(|r| r.node.status().commit).max().unwrap();
    let m = wait_flushed(&c, top, Duration::from_secs(10)).await;
    let v = verify(&c).await;
    assert!(v.ok && v.flushed == top && v.pruned == a.pruned_seq, "after a recovery: {v:#?}");
    let r = c.finish_recovered(&acked, &m.gaps, &expected);
    assert!(r.ok, "checker: {r:#?}");
    assert_eq!((r.holes, r.events_lost, r.acked_missing), (0, 0, 0), "{r:#?}");
    // and the planner still runs on what's left
    let again = retain::plan(&c.store, Duration::from_secs(3600)).await.unwrap().unwrap();
    assert!(again.deletable.is_empty() && again.segments > 0, "{again:#?}");
    c.shutdown();
}
