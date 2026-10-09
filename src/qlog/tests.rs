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
use super::node::{Config, Durability, Faults, LeaderRecord, MemoryOnly, Node, Role, Status, SwitchStats, read_leader};
use super::wire;
use bytes::Bytes;
use parking_lot::Mutex;
use rand::{Rng, SeedableRng};
use std::collections::HashMap;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::time::{Duration, Instant};
use tokio::sync::mpsc;
use vlsync_store::store::Store;

pub(crate) struct Running {
    rt: tokio::runtime::Runtime,
    pub(crate) node: Arc<Node>,
    faults: Arc<Faults>,
    cl: Option<Arc<CommitLog>>,
    /// The last seq its commitlog replayed at start.
    recovered: u64,
}

pub(crate) type ConfigFn = Arc<dyn Fn(&str, &HashMap<String, String>) -> Config + Send + Sync>;

pub(crate) struct Cluster {
    ids: Vec<String>,
    addrs: HashMap<String, String>,
    pub(crate) store: Store,
    pub(crate) nodes: HashMap<String, Running>,
    incarnations: HashMap<String, u64>,
    tap: mpsc::UnboundedSender<Emitted>,
    checker: Arc<Mutex<Checker>>,
    /// Every batch emitted, by stream (`node#incarnation`).
    pub(crate) emitted: Arc<Mutex<Vec<(String, u64, Bytes)>>>,
    /// Faults that outlive a restart: (node, peers it can't reach).
    blocks: Vec<(String, String)>,
    /// Each node's commitlog lives under here (memory-only without).
    disk: Option<(tempfile::TempDir, commitlog::Options)>,
    /// Overrides `config` for nodes started from now on.
    cfg: Option<ConfigFn>,
    ring_bytes: usize,
}

/// Each port handed out once, below the ephemeral range: an OS-picked port
/// can come back to a cluster running alongside, whose nodes would then
/// append to this one's. Within a process a counter keeps ports apart;
/// across processes (nextest runs each test in its own) a lock file per
/// port does, held until the process exits. A bind check alone isn't
/// enough: a killed node's port is free until it restarts.
pub(crate) fn free_port() -> u16 {
    const LO: u64 = 15_000;
    const SPAN: u64 = 10_000;
    static NEXT: AtomicU64 = AtomicU64::new(0);
    static HELD: Mutex<Vec<std::fs::File>> = Mutex::new(Vec::new());
    let dir = std::env::temp_dir().join("vlrelay-test-ports");
    let _ = std::fs::create_dir_all(&dir);
    let start = (std::process::id() as u64 * 97) % SPAN;
    for _ in 0..SPAN {
        let p = (LO + (start + NEXT.fetch_add(1, Ordering::Relaxed)) % SPAN) as u16;
        let Ok(f) = std::fs::File::create(dir.join(format!("{p}.lock"))) else { continue };
        if f.try_lock().is_ok() && std::net::TcpListener::bind(("127.0.0.1", p)).is_ok() {
            HELD.lock().push(f);
            return p;
        }
    }
    panic!("no free test port in {LO}..{}", LO + SPAN);
}

pub(crate) fn config(id: &str, addrs: &HashMap<String, String>) -> Config {
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

    pub(crate) async fn durable(n: usize, sync_delay: Option<Duration>) -> Cluster {
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

    pub(crate) async fn with_cfg(
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
        let emitted: Arc<Mutex<Vec<(String, u64, Bytes)>>> = Default::default();
        let (ck, em) = (checker.clone(), emitted.clone());
        tokio::spawn(async move {
            while let Some(e) = rx.recv().await {
                let stream = format!("{}#{}", e.node, e.incarnation);
                em.lock().extend(e.events.iter().map(|(s, d)| (stream.clone(), *s as u64, d.clone())));
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
            emitted,
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

    pub(crate) async fn start(&mut self, id: &str) {
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
        let replayed = recovered.as_ref().map_or(0, |r| r.log.last_seq());
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
        self.nodes.insert(id.to_string(), Running { rt, node, faults, cl, recovered: replayed });
    }

    /// kill -9: the runtime goes, with every task, socket and byte of memory.
    /// What the commitlog wrote stays (the page cache outlives a process).
    pub(crate) fn kill(&mut self, id: &str) {
        if let Some(r) = self.nodes.remove(id) {
            r.rt.shutdown_background();
            if let Some(cl) = r.cl {
                cl.halt();
            }
        }
    }

    /// The box loses power: as kill, and the commitlog also loses a random
    /// part of what it wrote since its last fsync, ending in a torn record.
    pub(crate) fn power_cut(&mut self, id: &str, rng: &mut impl Rng) {
        if let Some(r) = self.nodes.remove(id) {
            // the disk is cut first: nothing after this point reaches it
            if let Some(cl) = &r.cl {
                let garbage: Vec<u8> = (0..rng.gen_range(0..40)).map(|_| rng.r#gen()).collect();
                cl.power_cut(rng.gen_range(0.0..1.0), &garbage).unwrap();
            }
            r.rt.shutdown_background();
        }
    }

    /// The power goes out on `ids` at once, each losing everything written
    /// since its last fsync. Every writer stops before any log is cut: a
    /// cut's own fsyncs can take long enough on a busy disk for the nodes
    /// still running to sync what the cut was meant to lose.
    pub(crate) fn power_cut_all_unsynced(&mut self, ids: &[String]) {
        let cut: Vec<Running> = ids.iter().filter_map(|id| self.nodes.remove(id)).collect();
        for r in &cut {
            if let Some(cl) = &r.cl {
                cl.halt();
            }
        }
        for r in cut {
            if let Some(cl) = &r.cl {
                cl.power_cut(0.0, &[0xde, 0xad]).unwrap();
            }
            r.rt.shutdown_background();
        }
    }

    /// The disk is gone: kill -9, and its commitlog with it.
    pub(crate) fn wipe(&mut self, id: &str) {
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

    pub(crate) fn leader(&self) -> Option<String> {
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

    pub(crate) async fn wait_leader(&self, within: Duration) -> String {
        let t = Instant::now();
        loop {
            if let Some(l) = self.leader() {
                return l;
            }
            assert!(t.elapsed() < within, "no leader within {within:?}");
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
    }

    pub(crate) fn client(&self) -> Arc<Client> {
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
    pub(crate) async fn converge(&self, within: Duration) {
        if let Err(e) = self.try_converge(within).await {
            panic!("{e}");
        }
    }

    /// As `converge`, saying why it didn't.
    pub(crate) async fn try_converge(&self, within: Duration) -> Result<(), String> {
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
                    return Ok(());
                }
            }
            if t.elapsed() >= within {
                return Err(format!("no convergence within {within:?}: {st:#?}"));
            }
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
    }

    pub(crate) fn finish(&self, acked: &[(u64, u64)]) -> super::check::Report {
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

    pub(crate) fn shutdown(mut self) {
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
            let flush = s.flush.as_ref().map_or(String::new(), |f| {
                format!(
                    " F {} applied {} flushes {} failed {} aborted {} fences {}",
                    s.flushed, f.applied, f.flushes, f.failed, f.aborted, f.fences
                )
            });
            format!(
                "{} {:?} e{} base {} last {} commit {} emitted {} intact {} gen {} resets {}{flush}",
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
        // not its commit index: two followers' acks commit entries its own
        // writer may not have reached yet (a kill loses those, and the
        // followers hold them); it emits only what its own disk holds
        let before = c.nodes[&l].node.status().emitted;
        c.kill(&l);
        c.start(&l).await;
        let st = c.nodes[&l].node.status();
        assert!(st.intact, "{l} restarted not intact");
        assert!(st.last >= before, "{l} came back with less than it emitted: {} < {before}", st.last);
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
    flushing_with(n, opts, ring_bytes, commitlog::Options::default()).await
}

/// As [`flushing_n`], with the commitlog's sync mode and test knobs from
/// `base`.
async fn flushing_with(
    n: usize,
    opts: impl Fn(&str) -> flush::Options + Send + Sync + 'static,
    ring_bytes: usize,
    base: commitlog::Options,
) -> Cluster {
    flushing_tuned(n, opts, ring_bytes, base, |_| {}).await
}

/// As [`flushing_with`], with `tune` applied to each node's config.
async fn flushing_tuned(
    n: usize,
    opts: impl Fn(&str) -> flush::Options + Send + Sync + 'static,
    ring_bytes: usize,
    base: commitlog::Options,
    tune: impl Fn(&mut Config) + Send + Sync + 'static,
) -> Cluster {
    let o = commitlog::Options { segment_bytes: 256 << 10, retain_bytes: 1 << 20, memory_bytes: 64 << 10, ..base };
    flushing_on(n, opts, ring_bytes, o, tune).await
}

/// As [`flushing`], on disks a quarter the size: a wait for a disk to trim
/// its oldest segment gets there after a quarter of the writes, which
/// counts when fsyncs (and the load with them) crawl.
async fn flushing_small_disks(opts: impl Fn(&str) -> flush::Options + Send + Sync + 'static) -> Cluster {
    let o = commitlog::Options {
        segment_bytes: 64 << 10,
        retain_bytes: 256 << 10,
        memory_bytes: 64 << 10,
        ..commitlog::Options::default()
    };
    flushing_on(3, opts, 64 << 20, o, |_| {}).await
}

async fn flushing_on(
    n: usize,
    opts: impl Fn(&str) -> flush::Options + Send + Sync + 'static,
    ring_bytes: usize,
    o: commitlog::Options,
    tune: impl Fn(&mut Config) + Send + Sync + 'static,
) -> Cluster {
    let cfg = move |id: &str, addrs: &HashMap<String, String>| {
        let mut k = config(id, addrs);
        k.retain_bytes = 64 << 10;
        k.flush = Some(opts(id));
        tune(&mut k);
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
    let l = c.wait_leader(Duration::from_secs(5)).await;
    let fs = c.nodes[&l].node.status().flush.unwrap();
    assert!(fs.last_at_ms >= Some(ck.at_ms), "the leader's status names the last flush's time: {fs:?}");
    let r = fs.recent.last().expect("the leader's recent flushes");
    assert!(r.flushed >= ck.flushed && r.at_ms >= ck.at_ms && r.took_us > 0, "{r:?}");
    assert!(fs.recent.windows(2).all(|w| w[0].flushed <= w[1].flushed));
    let h = c.nodes[&l].node.status().history;
    assert!(
        h.iter().any(|e| e.kind == "lead" && e.why == "election"),
        "the leader's takeover isn't in its history: {h:?}"
    );
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

/// Segment PUTs that fail outright, or land with their answer lost (a
/// timeout): the flush tries the PUT again or adopts what landed, and every
/// manifest stays whole.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn flush_rides_out_failed_and_lost_segment_puts() {
    let o = commitlog::Options {
        segment_bytes: 256 << 10,
        retain_bytes: 1 << 20,
        memory_bytes: 64 << 10,
        ..commitlog::Options::default()
    };
    let cfg = move |id: &str, addrs: &HashMap<String, String>| {
        let mut k = config(id, addrs);
        k.retain_bytes = 64 << 10;
        k.flush = Some(flush_opts());
        k
    };
    let mut c =
        Cluster::with_spares(0, 3, Some((tempfile::tempdir().unwrap(), o)), Some(Arc::new(cfg)), 64 << 20).await;
    let flaky =
        Arc::new(FlakySegments { inner: c.store.raw.clone(), failed: AtomicU64::new(0), lost: AtomicU64::new(0) });
    c.store = Store { raw: flaky.clone(), ..c.store.clone() };
    for id in c.ids.clone() {
        c.start(&id).await;
    }
    c.wait_leader(Duration::from_secs(5)).await;
    let load = Load::start(c.client(), 4, 10, Duration::from_millis(2));
    tokio::time::sleep(Duration::from_secs(4)).await;
    let acked = load.stop().await;
    let v = settle_and_verify(&c, &acked).await;
    let (failed, lost) = (flaky.failed.load(Ordering::Relaxed), flaky.lost.load(Ordering::Relaxed));
    eprintln!("{v:?} failed {failed} lost {lost}");
    assert!(failed > 0 && lost > 0, "no faults injected: failed {failed} lost {lost}");
    assert!(v.segments > 10, "{v:?}");
    assert_eq!(v.orphans, 0, "{v:#?}");
    c.shutdown();
}

/// Fails a quarter of segment PUTs before they're written, and loses the
/// answer of another sixth after they are.
#[derive(Debug)]
struct FlakySegments {
    inner: Arc<dyn object_store::ObjectStore>,
    failed: AtomicU64,
    lost: AtomicU64,
}

impl std::fmt::Display for FlakySegments {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "FlakySegments({})", self.inner)
    }
}

#[async_trait::async_trait]
impl object_store::ObjectStore for FlakySegments {
    async fn put_opts(
        &self,
        location: &object_store::path::Path,
        payload: object_store::PutPayload,
        opts: object_store::PutOptions,
    ) -> object_store::Result<object_store::PutResult> {
        let injected = |what: &str| object_store::Error::Generic { store: "flaky", source: what.to_string().into() };
        if location.as_ref().ends_with(".seg") {
            let r: f64 = rand::random();
            if r < 0.25 {
                self.failed.fetch_add(1, Ordering::Relaxed);
                return Err(injected("injected: failed"));
            }
            if r < 0.4 {
                self.inner.put_opts(location, payload, opts).await?;
                self.lost.fetch_add(1, Ordering::Relaxed);
                return Err(injected("injected: timed out"));
            }
        }
        self.inner.put_opts(location, payload, opts).await
    }

    async fn put_multipart_opts(
        &self,
        location: &object_store::path::Path,
        opts: object_store::PutMultipartOptions,
    ) -> object_store::Result<Box<dyn object_store::MultipartUpload>> {
        self.inner.put_multipart_opts(location, opts).await
    }

    async fn get_opts(
        &self,
        location: &object_store::path::Path,
        options: object_store::GetOptions,
    ) -> object_store::Result<object_store::GetResult> {
        self.inner.get_opts(location, options).await
    }

    fn delete_stream(
        &self,
        locations: futures::stream::BoxStream<'static, object_store::Result<object_store::path::Path>>,
    ) -> futures::stream::BoxStream<'static, object_store::Result<object_store::path::Path>> {
        self.inner.delete_stream(locations)
    }

    fn list(
        &self,
        prefix: Option<&object_store::path::Path>,
    ) -> futures::stream::BoxStream<'static, object_store::Result<object_store::ObjectMeta>> {
        self.inner.list(prefix)
    }

    fn list_with_offset(
        &self,
        prefix: Option<&object_store::path::Path>,
        offset: &object_store::path::Path,
    ) -> futures::stream::BoxStream<'static, object_store::Result<object_store::ObjectMeta>> {
        self.inner.list_with_offset(prefix, offset)
    }

    async fn list_with_delimiter(
        &self,
        prefix: Option<&object_store::path::Path>,
    ) -> object_store::Result<object_store::ListResult> {
        self.inner.list_with_delimiter(prefix).await
    }

    async fn copy_opts(
        &self,
        from: &object_store::path::Path,
        to: &object_store::path::Path,
        options: object_store::CopyOptions,
    ) -> object_store::Result<()> {
        self.inner.copy_opts(from, to, options).await
    }
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
    // a crash and its report happen under the lock, so once disarmed every
    // crashed flush loop is in `rx`
    let armed = Arc::new(Mutex::new(Some(tx)));
    let a = armed.clone();
    let mut c = flushing(
        move |id| {
            let (id, a) = (id.to_string(), a.clone());
            flush::Options {
                crash: Some(Arc::new(move |_| {
                    let a = a.lock();
                    if let Some(t) = a.as_ref()
                        && rand::thread_rng().gen_bool(0.04)
                    {
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
    armed.lock().take();
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

/// A deposed leader whose state open finishes after the new leader's
/// fences the new leader's writer (the test opens it, as that late open
/// would): the leader can't seal, and F has to move on anyway.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_leader_whose_state_writer_is_fenced_still_flushes() {
    let c = flushing(|_| flush_opts(), 64 << 20).await;
    c.wait_leader(Duration::from_secs(5)).await;
    let load = Load::start(c.client(), 4, 10, Duration::from_millis(2));
    let m = wait_flushed(&c, 1, Duration::from_secs(5)).await;
    let stale = super::state::State::open(&c.store, m.state_path()).await.unwrap();
    tokio::time::sleep(Duration::from_millis(500)).await;
    let acked = load.stop().await;
    settle_and_verify(&c, &acked).await;
    stale.close().await;
    c.shutdown();
}

/// A candidate behind a member's disk adopts that member's tail with a
/// reset, at the oldest entry its disk holds: never past F, so the
/// candidate's stream (from the bucket up to there) and its state (applied
/// from F) both carry on. Its memory, trimmed past F while flushes stopped,
/// is no place to reset to.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_fetch_behind_the_disk_resets_to_its_oldest_entry_not_past_f() {
    let stop = Arc::new(AtomicBool::new(false));
    let stopped = Arc::new(AtomicU64::new(0));
    let (s, n) = (stop.clone(), stopped.clone());
    let c = flushing_small_disks(move |_| {
        let (s, n) = (s.clone(), n.clone());
        let crash = move |_| {
            let stop = s.load(Ordering::Acquire);
            if stop {
                n.fetch_add(1, Ordering::Release);
            }
            stop
        };
        flush::Options { crash: Some(Arc::new(crash)), ..flush_opts() }
    })
    .await;
    let l = c.wait_leader(Duration::from_secs(5)).await;
    let f = c.ids.iter().find(|id| **id != l).unwrap().clone();
    let load = Load::start(c.client(), 4, 10, Duration::from_millis(1));
    let node = c.nodes[&f].node.clone();
    let t = Instant::now();
    while node.readable_floor() <= 1 {
        assert!(t.elapsed() < Duration::from_secs(30), "{f}'s disk still reaches seq 1: {}", status_line(&c));
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    stop.store(true, Ordering::Release);
    // the leader's flusher stops at its next step, after the CAS of the
    // flush in flight if it's past its last check
    let t = Instant::now();
    while stopped.load(Ordering::Acquire) == 0 {
        assert!(t.elapsed() < Duration::from_secs(30), "the flusher never stopped: {}", status_line(&c));
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
    let m = flush::read_manifest(&c.store).await.unwrap().unwrap().0;
    let t = Instant::now();
    while node.status().base <= m.flushed + 1000 {
        assert!(
            t.elapsed() < Duration::from_secs(30),
            "{f}'s memory still reaches F {}: {}",
            m.flushed,
            status_line(&c)
        );
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    load.stop().await;
    assert_eq!(
        flush::read_manifest(&c.store).await.unwrap().unwrap().0.flushed,
        m.flushed,
        "a flush ran after the stop"
    );
    let rpc = super::node::Rpc::new(&f, &c.addrs[&f], Arc::new(Faults::default()));
    let fetch =
        wire::Msg::Fetch { epoch: node.status().promised, from: "test".into(), from_seq: 1, max_bytes: 64 << 10 };
    match rpc.call(&fetch, Duration::from_secs(2)).await {
        Ok(wire::Msg::FetchResp { ok: true, base_seq, entries, .. }) => {
            assert!(base_seq <= m.flushed, "a reset to {base_seq}, past F {}: {}", m.flushed, status_line(&c));
            assert_eq!(entries.first().map(|e| e.seq), Some(base_seq + 1));
        }
        r => panic!("{r:?}"),
    }
    c.shutdown();
}

/// A follower down long enough that the leader's disk no longer reaches
/// back to it catches up from the bucket segments, not by a reset (which
/// would be a gap in its stream).
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_follower_behind_the_leaders_disk_catches_up_from_the_bucket() {
    let mut c = flushing_small_disks(|_| flush_opts()).await;
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
    // until no node can serve it from memory or disk (the other one may
    // lead by the time it's back), however fast this box fills and trims
    // their commitlogs
    let t = Instant::now();
    while c.nodes.values().any(|r| r.node.readable_floor() <= behind + 1) {
        assert!(t.elapsed() < Duration::from_secs(30), "a disk still reaches {behind}: {}", status_line(&c));
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
    /// Each host's generation, as of its last rewind.
    rewound: Arc<Mutex<HashMap<String, u64>>>,
}

impl HostLoad {
    fn start(c: &Cluster, n: usize, batch: u64, pause: Duration) -> HostLoad {
        let stop = Arc::new(AtomicBool::new(false));
        let acked: Arc<Mutex<Vec<(u64, u64, u64)>>> = Arc::default();
        let sent: Arc<Mutex<HashMap<String, u64>>> = Arc::default();
        let rewound: Arc<Mutex<HashMap<String, u64>>> = Arc::default();
        let tasks = (0..n)
            .map(|w| {
                let client = c.client();
                let (stop, acked, sent, rewound) = (stop.clone(), acked.clone(), sent.clone(), rewound.clone());
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
                            rewound.lock().insert(host.clone(), g);
                        }
                        tokio::time::sleep(pause).await;
                    }
                })
            })
            .collect();
        HostLoad { stop, acked, tasks, sent, rewound }
    }

    /// Until each of the `hosts` has rewound to `generation` or past it.
    async fn wait_rewound(&self, hosts: usize, generation: u64, within: Duration) {
        let t = Instant::now();
        loop {
            {
                let r = self.rewound.lock();
                if r.values().filter(|g| **g >= generation).count() == hosts {
                    return;
                }
                assert!(t.elapsed() < within, "hosts not rewound to generation {generation} within {within:?}: {r:?}");
            }
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
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
    let (mut generation, mut floor) = (0, 0);
    for round in 0..2 {
        // the log has grown, and been flushed, past the last jump
        wait_flushed(&c, floor + 500, Duration::from_secs(10)).await;
        let before = flush::read_manifest(&c.store).await.unwrap().unwrap().0;
        for id in c.ids.clone() {
            c.wipe(&id);
        }
        for id in c.ids.clone() {
            c.start(&id).await;
        }
        let (st, m) = settle_after_wipe(&c, Duration::from_secs(20)).await;
        eprintln!("round {round}: {}\n{:?}", status_line(&c), c.recoveries());
        let rec = m.recovery.clone().unwrap();
        // Usually one recovery per wipe. Under load another member's
        // election can depose the recovered leader before any wiped follower
        // has caught up from it, and with no quorum of intact logs that's
        // one more.
        assert!(rec.generation > generation, "{rec:?} after generation {generation}");
        assert_eq!(st.generation, rec.generation, "{st:?}");
        assert!(rec.base >= before.reserve && rec.after >= before.flushed, "{rec:?} after {before:?}");
        assert!(st.commit >= rec.base, "{st:?}");
        (generation, floor) = (rec.generation, rec.base);
        load.wait_rewound(4, generation, Duration::from_secs(20)).await;
    }
    let (v, r, m) = settle_recovered(&c, load).await;
    eprintln!("{v:?}\n{r:?}\ngaps {:?} {:#?}", m.gaps, c.recoveries());
    assert_eq!(m.gaps.len() as u64, generation, "{m:?}");
    assert!(r.reingested + r.duplicates > 0, "{r:?}");
    c.shutdown();
}

/// After every disk was wiped: the leader's status and the manifest, once
/// every log is intact again (so no further recovery can follow) and the
/// leader has adopted the manifest's latest recovery.
async fn settle_after_wipe(c: &Cluster, within: Duration) -> (Status, flush::Manifest) {
    let t = Instant::now();
    loop {
        if let Some(l) = c.leader()
            && c.nodes.values().all(|r| r.node.status().intact)
        {
            let st = c.nodes[&l].node.status();
            let m = flush::read_manifest(&c.store).await.unwrap().unwrap().0;
            if st.role == Role::Leader && m.recovery.is_some() && st.generation == m.generation() {
                return (st, m);
            }
        }
        assert!(t.elapsed() < within, "no intact cluster on a recovery within {within:?}: {}", status_line(c));
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
}

/// Two disks gone, one survivor. If the survivor leads and the others are
/// back within its election timeout, it never loses its quorum: they start
/// empty and catch up from it, with no jump. Down for longer (or with a
/// follower surviving), the quorum is lost (one intact log can't make two)
/// and the bucket recovery runs, keeping the survivor's committed entries
/// past F with their seqs.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn wiping_two_disks_salvages_the_survivors_log() {
    let mut c = flushing_tuned(
        3,
        // long enough that a survivor has committed entries past F
        |_| flush::Options { interval: Duration::from_millis(2500), headroom: 50_000, ..flush_opts() },
        64 << 20,
        commitlog::Options::default(),
        // round 0's restart (two fresh commitlogs, fsynced) must fit in it
        // even on a slow disk
        |k| k.election_timeout = Duration::from_secs(3),
    )
    .await;
    let load = HostLoad::start(&c, 4, 10, Duration::from_millis(2));
    let mut salvaged = 0;
    let mut recoveries = 0u64;
    for round in 0..4 {
        let l = c.wait_leader(Duration::from_secs(10)).await;
        wait_flushed(&c, 1, Duration::from_secs(15)).await;
        let keep = if round < 2 { l.clone() } else { c.ids.iter().find(|id| **id != l).unwrap().clone() };
        let epoch = c.nodes[&l].node.status().epoch;
        // the survivor holds committed entries past F, to salvage
        let t = Instant::now();
        loop {
            let f = flush::read_manifest(&c.store).await.unwrap().unwrap().0.flushed;
            if c.nodes[&keep].node.status().commit > f {
                break;
            }
            assert!(t.elapsed() < Duration::from_secs(10), "nothing committed past F {f}: {}", status_line(&c));
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
        for id in c.ids.clone() {
            if id != keep {
                c.wipe(&id);
            }
        }
        if round > 0 {
            // down for longer than the survivor's election timeout
            let t = Instant::now();
            while c.nodes[&keep].node.status().role == Role::Leader {
                assert!(t.elapsed() < Duration::from_secs(10), "{keep} kept leading: {}", status_line(&c));
                tokio::time::sleep(Duration::from_millis(20)).await;
            }
        }
        for id in c.ids.clone() {
            if id != keep {
                c.start(&id).await;
            }
        }
        if round == 0 {
            let t = Instant::now();
            while !c.nodes.values().all(|r| r.node.status().intact) {
                assert!(t.elapsed() < Duration::from_secs(20), "not caught up: {}", status_line(&c));
                tokio::time::sleep(Duration::from_millis(20)).await;
            }
            assert!(c.recoveries().is_empty(), "{}", status_line(&c));
            let st = c.nodes[&keep].node.status();
            assert_eq!((st.role, st.epoch), (Role::Leader, epoch), "{}", status_line(&c));
            continue;
        }
        recoveries += 1;
        let t = Instant::now();
        let rec = loop {
            // (the node that ran an earlier one may have been wiped since)
            if let Some(r) = c.recoveries().into_iter().find(|r| r.generation == recoveries) {
                break r;
            }
            assert!(t.elapsed() < Duration::from_secs(20), "no recovery: {}", status_line(&c));
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
    let stop_at_fence = Arc::new(AtomicBool::new(false));
    let (a, stop) = (armed.clone(), stop_at_fence.clone());
    let mut c = flushing(
        move |_| {
            let (a, stop) = (a.clone(), stop.clone());
            flush::Options {
                crash: Some(Arc::new(move |s| {
                    let mut g = a.lock();
                    if g.first() == Some(&s) {
                        g.remove(0);
                        return true;
                    }
                    s == Step::Fenced && g.is_empty() && stop.load(Ordering::SeqCst)
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
    // Three attempts die (one at each step). Every leader after them stops
    // its flush at the fence, so the last recovery manifest stays current.
    // That needn't be the fourth attempt's: under load another member's
    // election can depose the recovered leader before any wiped follower
    // has caught up from it, and with no quorum of intact logs that's one
    // more recovery.
    *armed.lock() = vec![Step::RecoverSealed, Step::RecoverBeforeManifest, Step::RecoverAfterManifest];
    stop_at_fence.store(true, Ordering::SeqCst);
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
    // with every log intact no recovery follows
    let t = Instant::now();
    loop {
        c.wait_leader(Duration::from_secs(10)).await;
        if c.nodes.values().all(|r| r.node.status().intact) {
            break;
        }
        assert!(t.elapsed() < Duration::from_secs(20), "not every log is intact: {}", status_line(&c));
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
    let m = flush::read_manifest(&c.store).await.unwrap().unwrap().0;
    let rec = m.recovery.clone().unwrap();
    // the attempt that died after its manifest CAS did recover (it may
    // have led and committed above its R for all anyone can tell), so the
    // next one jumped again
    assert!(rec.generation >= 2, "{m:?}");
    assert_eq!((m.flushed, m.state.as_ref().unwrap().seq), (rec.base, rec.base), "{m:?}");
    assert_ne!(m.state_path(), super::state::DEFAULT_PATH);
    let v = verify(&c).await;
    assert!(v.ok && v.gaps == rec.generation && v.flushed == rec.base, "{v:#?}");
    let applied = super::state::read_checkpoint(&c.store, m.state.as_ref().unwrap()).await.unwrap().0;
    assert_eq!(
        applied.get(super::state::applied_key()).map(|b| u64::from_be_bytes(b[..8].try_into().unwrap())),
        Some(rec.base)
    );
    // flushing again from here (a takeover fences and flushes as usual);
    // every node restarts, since leadership may have moved since the check
    // to another whose flush stopped at its fence
    stop_at_fence.store(false, Ordering::SeqCst);
    for id in c.ids.clone() {
        c.kill(&id);
    }
    for id in c.ids.clone() {
        c.start(&id).await;
    }
    c.wait_leader(Duration::from_secs(10)).await;
    let (v, r, m) = settle_recovered(&c, load).await;
    eprintln!("{v:?}\n{r:?}\n{m:?}");
    assert_eq!(m.generation(), rec.generation, "{m:?}");
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
    let seg = |o: u64| vlsync_firehose::log::segment_path(&c.store, flush::LOG_ID, o);
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

// ---------------------------------------------------------------- durability modes

fn page_cache(trust: bool) -> commitlog::Options {
    commitlog::Options {
        sync: commitlog::SyncMode::PageCache { every: Duration::from_millis(400) },
        trust_after_power_loss: trust,
        ..commitlog::Options::default()
    }
}

/// Every process killed at once in page-cache mode: the kernel still holds
/// what they wrote, so nothing acked is lost and no recovery runs.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn page_cache_loses_nothing_when_every_process_dies() {
    let mut c = flushing_with(3, |_| flush_opts(), 64 << 20, page_cache(false)).await;
    c.wait_leader(Duration::from_secs(5)).await;
    let load = HostLoad::start(&c, 4, 10, Duration::from_millis(2));
    for _ in 0..3 {
        wait_flushed(&c, 1, Duration::from_secs(5)).await;
        tokio::time::sleep(Duration::from_millis(300)).await;
        for id in c.ids.clone() {
            c.kill(&id);
        }
        for id in c.ids.clone() {
            c.start(&id).await;
        }
        c.wait_leader(Duration::from_secs(10)).await;
    }
    tokio::time::sleep(Duration::from_millis(500)).await;
    let (_, r, m) = settle_recovered(&c, load).await;
    assert!(m.gaps.is_empty() && c.recoveries().is_empty(), "a recovery ran: {m:?} {:?}", c.recoveries());
    assert_eq!(r.reingested, 0, "{r:?}");
    let st = c.nodes.values().next().unwrap().node.status();
    assert_eq!(st.durability.mode, "page-cache");
    c.shutdown();
}

/// A power cut on every node in page-cache mode, each losing everything
/// it wrote since its last background fsync: no node vouches for its log
/// afterwards, so the cluster recovers from the bucket at R + 1 with a
/// recorded gap, hosts re-ingest, and no seq is reused or acked event
/// lost. Then the same on two of the three (the third's log is intact but
/// alone, so the recovery salvages from it).
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn page_cache_power_cuts_on_a_majority_recover_from_the_bucket() {
    let mut c =
        flushing_with(3, |_| flush::Options { headroom: 50_000, ..flush_opts() }, 64 << 20, page_cache(false)).await;
    c.wait_leader(Duration::from_secs(5)).await;
    let load = HostLoad::start(&c, 4, 10, Duration::from_millis(2));
    for (round, cut) in [3usize, 2].into_iter().enumerate() {
        wait_flushed(&c, 1, Duration::from_secs(5)).await;
        tokio::time::sleep(Duration::from_millis(300)).await;
        let l = c.wait_leader(Duration::from_secs(10)).await;
        // the leader and as many others as it takes
        let mut ids = vec![l.clone()];
        ids.extend(c.ids.iter().filter(|i| **i != l).take(cut - 1).cloned());
        c.power_cut_all_unsynced(&ids);
        for id in &ids {
            c.start(id).await;
        }
        let t = Instant::now();
        while !c.recoveries().iter().any(|r| r.generation == round as u64 + 1) {
            assert!(t.elapsed() < Duration::from_secs(15), "round {round}: no recovery: {}", status_line(&c));
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
    }
    tokio::time::sleep(Duration::from_millis(500)).await;
    let (_, r, m) = settle_recovered(&c, load).await;
    eprintln!("{r:?} gaps {:?}", m.gaps);
    assert_eq!(m.gaps.len(), 2, "{m:?}");
    c.shutdown();
}

/// A follower reset past what it emitted (by a bucket recovery's leader,
/// say, while it was still catching up) hands its consumers the skipped
/// seqs the bucket holds before it jumps. Here the follower is cut off
/// while the leader flushes on, then a leader the test plays resets it
/// well past the bucket's F.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_follower_reset_past_what_it_emitted_emits_the_bucket_first() {
    follower_reset_past_emitted(false).await;
}

/// The reset's `flushed` doesn't bound what the follower hands over: it
/// emits everything the bucket holds when it reads it, here a flush that
/// landed after the leader's view the reset carries.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_follower_reset_with_a_stale_f_emits_all_the_bucket_holds() {
    follower_reset_past_emitted(true).await;
}

async fn follower_reset_past_emitted(stale: bool) {
    let mut c = flushing(|_| flush_opts(), 64 << 20).await;
    let l = c.wait_leader(Duration::from_secs(5)).await;
    let load = Load::start(c.client(), 4, 10, Duration::from_millis(2));
    wait_flushed(&c, 1, Duration::from_secs(5)).await;
    let f = c.ids.iter().find(|id| **id != l).unwrap().clone();
    let t = Instant::now();
    while c.nodes[&f].node.status().emitted == 0 {
        assert!(t.elapsed() < Duration::from_secs(5), "{f} emitted nothing: {}", status_line(&c));
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
    c.isolate(&f);
    // appends already on the wire land
    tokio::time::sleep(Duration::from_millis(200)).await;
    let behind = c.nodes[&f].node.status().emitted;
    let early = wait_flushed(&c, behind + 500, Duration::from_secs(10)).await;
    load.stop().await;
    let st = c.nodes[&f].node.status();
    assert_eq!(st.emitted, behind, "{f} emitted while cut off");
    // The leader flushes on its interval, so F is only fixed once it has
    // flushed its whole log: the follower reads the bucket as it is then.
    let top = c.nodes[&l].node.status().commit;
    c.nodes[&l].node.flush.request(top);
    let m = wait_flushed(&c, top, Duration::from_secs(10)).await;
    assert_eq!(m.flushed, top);
    let told = if stale {
        assert!(early.flushed < m.flushed, "nothing flushed past {}", early.flushed);
        early.flushed
    } else {
        m.flushed
    };
    let base = m.flushed + 1000;
    let rpc = super::node::Rpc::new(&f, &c.addrs[&f], Arc::new(Faults::default()));
    let reset = wire::Append {
        epoch: st.promised,
        leader: "test".into(),
        prev_epoch: st.promised,
        prev_seq: base,
        commit: base,
        leader_last: base,
        reset: true,
        flushed: told,
        reserve: m.reserve,
        generation: st.generation,
        entries: Vec::new(),
    };
    match rpc.call(&wire::Msg::Append(reset), Duration::from_secs(2)).await {
        Ok(wire::Msg::AppendResp(r)) => assert!(r.ok, "{r:?}"),
        r => panic!("{r:?}"),
    }
    let stream = format!("{f}#{}", c.incarnations[&f]);
    let t = Instant::now();
    loop {
        let st = c.nodes[&f].node.status();
        let last = c.checker.lock().last(&stream).unwrap_or(0);
        if st.emitted == base && last >= m.flushed {
            break;
        }
        assert!(
            t.elapsed() < Duration::from_secs(10),
            "{f} emitted through {} and its stream reached {last}: want {base}, after every seq through F {}",
            st.emitted,
            m.flushed
        );
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
    let after: Vec<u64> =
        c.emitted.lock().iter().filter(|(s, q, _)| *s == stream && *q > behind).map(|e| e.1).collect();
    assert_eq!(after, (behind + 1..=m.flushed).collect::<Vec<_>>());
    assert_eq!(c.nodes[&f].node.status().emit_gaps, base - m.flushed);
    let r = c.finish(&[]);
    assert!(r.ok, "checker: {r:#?}");
    c.shutdown();
}

/// The mutation: nodes that trust their logs after a power loss elect a
/// leader from logs that lost acked entries, and the checker catches it
/// (acked events lost, or seqs emitted twice with other contents).
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn trusting_a_short_log_after_a_power_loss_is_caught() {
    // one segment each, so the background fsync every 400 ms decides what a
    // cut loses (a rollover fsyncs too, every ~150 ms at this load)
    let o = commitlog::Options { segment_bytes: 64 << 20, memory_bytes: 64 << 10, ..page_cache(true) };
    let opts = |_: &str| flush::Options { headroom: 50_000, ..flush_opts() };
    let mut c = flushing_on(3, opts, 64 << 20, o, |_| {}).await;
    c.wait_leader(Duration::from_secs(5)).await;
    let load = HostLoad::start(&c, 4, 10, Duration::from_millis(2));
    // a cut just after the nodes' syncs (or while the load waits on a
    // leader, or while a node's fsync is under way) loses nothing acked, and
    // there's nothing to catch: only cuts that took acked entries off every
    // log count
    let (mut lossy, mut rounds) = (0, 0);
    while lossy < 3 {
        assert!(rounds < 20, "{lossy} of {rounds} power cuts lost acked entries: {}", status_line(&c));
        rounds += 1;
        wait_flushed(&c, 1, Duration::from_secs(5)).await;
        tokio::time::sleep(Duration::from_millis(300)).await;
        let t = Instant::now();
        while !c.nodes.values().all(|r| {
            let d = r.node.status().durability;
            d.unsynced_bytes > 0 && d.since_sync_ms.is_some_and(|ms| ms >= 100)
        }) {
            assert!(t.elapsed() < Duration::from_secs(10), "no node went 100 ms unsynced: {}", status_line(&c));
            tokio::time::sleep(Duration::from_millis(2)).await;
        }
        let acked = load.acked.lock().iter().map(|a| a.0).max().unwrap_or(0);
        c.power_cut_all_unsynced(&c.ids.clone());
        for id in c.ids.clone() {
            c.start(&id).await;
        }
        if c.nodes.values().all(|r| r.recovered < acked) {
            lossy += 1;
        }
        c.wait_leader(Duration::from_secs(10)).await;
    }
    tokio::time::sleep(Duration::from_millis(500)).await;
    let (acked, expected) = load.stop().await;
    // logs that diverged for good are caught too
    if let Err(e) = c.try_converge(Duration::from_secs(20)).await {
        eprintln!("caught after {rounds} cuts: {e}");
        c.shutdown();
        return;
    }
    let m = flush::read_manifest(&c.store).await.unwrap().map(|(m, _)| m).unwrap_or_default();
    let r = c.finish_recovered(&acked, &m.gaps, &expected);
    eprintln!("{rounds} cuts: {r:?}");
    assert!(
        !r.ok || r.acked_missing > 0 || r.events_lost > 0 || r.holes > 0,
        "trusting short logs after power losses went unnoticed: {r:?}"
    );
    c.shutdown();
}

/// A follower cut off behind the bucket's F, the only node left
/// running: nothing flushes over the bucket the test then plays recoveries
/// into. Returns it, what it emitted and the manifest.
async fn a_lone_follower_behind_the_bucket() -> (Cluster, String, u64, flush::Manifest) {
    let mut c = flushing(|_| flush_opts(), 64 << 20).await;
    let l = c.wait_leader(Duration::from_secs(5)).await;
    let load = Load::start(c.client(), 4, 10, Duration::from_millis(2));
    tokio::time::sleep(Duration::from_millis(300)).await;
    let f = c.ids.iter().find(|id| **id != l).unwrap().clone();
    c.isolate(&f);
    tokio::time::sleep(Duration::from_millis(200)).await;
    let behind = c.nodes[&f].node.status().emitted;
    assert!(behind > 0, "{f} emitted nothing before it was cut off");
    load.stop().await;
    let top = c.nodes[&l].node.status().commit;
    let m = wait_flushed(&c, top, Duration::from_secs(10)).await;
    for id in c.ids.clone() {
        if id != f {
            c.kill(&id);
        }
    }
    (c, f, behind, m)
}

/// Plays a recovery's leader resetting `f` to `to` and waits for its
/// stream to get there. Returns the seqs it emitted past `behind`.
async fn reset_and_emit(c: &Cluster, f: &str, behind: u64, to: u64, m: &flush::Manifest) -> Vec<u64> {
    let st = c.nodes[f].node.status();
    let rpc = super::node::Rpc::new(f, &c.addrs[f], Arc::new(Faults::default()));
    let reset = wire::Append {
        epoch: st.promised,
        leader: "test".into(),
        prev_epoch: st.promised,
        prev_seq: to,
        commit: to,
        leader_last: to,
        reset: true,
        flushed: m.flushed,
        reserve: m.reserve,
        generation: st.generation,
        entries: Vec::new(),
    };
    match rpc.call(&wire::Msg::Append(reset), Duration::from_secs(2)).await {
        Ok(wire::Msg::AppendResp(r)) => assert!(r.ok, "{r:?}"),
        r => panic!("{r:?}"),
    }
    let t = Instant::now();
    while c.nodes[f].node.status().emitted != to {
        assert!(t.elapsed() < Duration::from_secs(10), "{:?}", c.nodes[f].node.status());
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
    let stream = format!("{f}#{}", c.incarnations[f]);
    c.emitted.lock().iter().filter(|(s, q, _)| *s == stream && *q > behind).map(|e| e.1).collect()
}

/// A deposed leader's flush, still running when a recovery moved F past
/// it, can leave a segment at the recovery's next ordinal holding seqs from
/// S + 1 until the new leader's first flush replaces it. A follower that
/// emitted up to S, reset past R, crosses the gap without emitting them.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_follower_reset_across_a_gap_never_emits_a_deposed_leaders_segment() {
    let (c, f, behind, m) = a_lone_follower_behind_the_bucket().await;
    let (s, r) = (m.flushed, m.flushed + 1000);
    let mut m2 = m.clone();
    m2.gaps.push((s, r));
    (m2.flushed, m2.reserve) = (r, r + 5000);
    flush::put_test_manifest(&c.store, &m2).await;
    let stale: Vec<super::log::Entry> =
        (s + 1..=s + 5).map(|q| super::log::Entry::new(1, q, Bytes::from(format!("stale{q}")))).collect();
    flush::put_test_segment(&c.store, m.next_ordinal, &stale).await;
    let got = reset_and_emit(&c, &f, behind, r + 10, &m2).await;
    assert_eq!(got, (behind + 1..=s).collect::<Vec<_>>(), "emitted seqs in the gap ({s}, {r}]");
    c.shutdown();
}

/// A follower whose stream is still behind one recovery's gap when a
/// second recovery's leader resets it hands its consumers the log on both
/// sides of the first gap, up to the second's S: (e, S1] and (R1, S2].
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_follower_reset_across_two_gaps_emits_the_log_between_them() {
    let (c, f, behind, m) = a_lone_follower_behind_the_bucket().await;
    let (s1, r1) = (m.flushed, m.flushed + 1000);
    let (s2, r2) = (r1 + 100, r1 + 2000);
    let between: Vec<super::log::Entry> =
        (r1 + 1..=s2).map(|q| super::log::Entry::new(2, q, Bytes::from(format!("between{q}")))).collect();
    flush::put_test_segment(&c.store, m.next_ordinal, &between).await;
    let mut m2 = m.clone();
    m2.gaps.extend([(s1, r1), (s2, r2)]);
    (m2.flushed, m2.reserve, m2.next_ordinal) = (r2, r2 + 5000, m.next_ordinal + 1);
    flush::put_test_manifest(&c.store, &m2).await;
    let got = reset_and_emit(&c, &f, behind, r2 + 10, &m2).await;
    assert_eq!(got, (behind + 1..=s1).chain(r1 + 1..=s2).collect::<Vec<_>>());
    assert_eq!(c.nodes[&f].node.status().emit_gaps, (r1 - s1) + (r2 - s2) + 10);
    c.shutdown();
}
