use super::ingest::{Checkpoint, Config, Ingester, Sink};
use super::job::PlcJob;
use super::*;
use crate::identity::{HttpFetch, IdentityCache, Options};
use crate::state::record::SigningKey as KeyBytes;
use crate::verify::synth::Signer;
use std::collections::HashMap;
use std::sync::atomic::Ordering::Relaxed;

#[allow(dead_code)]
#[path = "../fakepds/export.rs"]
pub(crate) mod export;
#[allow(dead_code)]
#[path = "../fakepds/fleet.rs"]
pub(crate) mod fleet;

use export::FakePlc;
use fleet::Layout;

const PAUL: &str = r#"{"did":"did:plc:ragtjsm2j2vknwkz3zp4oxrd","cid":"bafyreieibu2mtgsovktnswo6l7dv4i4ztioutzpsy7wsasmbznzqxkpyje","createdAt":"2022-11-17T00:35:16.391Z","operation":{"sig":"DyaPWDItkJnVkN1izINSW-fdjUzP9BkIKlD7SnzD5axfK_870ZZ-1EYcrQLQtP9VkWcp2cdbyIHprjPfeUs8WQ","prev":null,"type":"create","handle":"paul.bsky.social","service":"https://bsky.social","signingKey":"did:key:zQ3shP5TBe1sQfSttXty15FAEHV1DZgcxRZNxvEWnPfLFwLxJ","recoveryKey":"did:key:zQ3shhCGUqDKjStzuDxPkTxN6ujddP4RkEKJJouJGRRkaLGbg"},"nullified":false}"#;

fn op_line(did: &str, at: &str, key: &str, endpoint: &str, nullified: bool) -> String {
    serde_json::json!({
        "did": did, "cid": "bafyx", "createdAt": at, "nullified": nullified,
        "operation": {
            "type": "plc_operation", "prev": "bafyprev", "sig": "x",
            "rotationKeys": [key], "alsoKnownAs": ["at://a.test"],
            "verificationMethods": {"atproto": key},
            "services": {"atproto_pds": {"type": "AtprotoPersonalDataServer", "endpoint": endpoint}},
        }
    })
    .to_string()
}

#[test]
fn parses_export_lines() {
    let op = parse_line(PAUL.as_bytes()).unwrap();
    assert_eq!(op.did, "did:plc:ragtjsm2j2vknwkz3zp4oxrd");
    assert_eq!(op.seed.pds.as_deref(), Some("bsky.social"));
    assert_eq!(op.seed.created_ms, parse_ms("2022-11-17T00:35:16.391Z").unwrap());
    assert!(op.seed.identity(&op.did).is_some());

    let key = format!("did:key:{}", Signer::new(crate::verify::synth::Curve::K256, 1).multibase());
    let did = "did:plc:aaaaaaaaaaaaaaaaaaaaaaaa";
    let l = op_line(did, "2025-01-02T03:04:05.678Z", &key, "https://pds.example.com:443/", false);
    let op = parse_line(l.as_bytes()).unwrap();
    assert_eq!(op.seed.pds.as_deref(), Some("pds.example.com"));
    assert!(op.seed.key.is_some() && !op.seed.tombstone);
    assert_eq!(Seed::decode(&op.seed.encode()).unwrap(), op.seed);

    let l = op_line(did, "2025-01-02T03:04:05.678Z", &key, "https://pds.example.com", true);
    assert_eq!(parse_line(l.as_bytes()).unwrap_err(), LineError::Nullified);
    let l = op_line("did:plc:short", "2025-01-02T03:04:05.678Z", &key, "https://p", false);
    assert_eq!(parse_line(l.as_bytes()).unwrap_err(), LineError::Did);
    // an unknown field is not a valid op
    let l = l.replace("did:plc:short", did).replace("\"sig\"", "\"extra\":1,\"sig\"");
    assert_eq!(parse_line(l.as_bytes()).unwrap_err(), LineError::Op);

    let t = serde_json::json!({"did": did, "cid": "c", "createdAt": "2025-01-02T03:04:05.678Z", "nullified": false,
        "operation": {"type": "plc_tombstone", "prev": "bafyprev", "sig": "x"}});
    let op = parse_line(t.to_string().as_bytes()).unwrap();
    assert!(op.seed.tombstone && op.seed.identity(did).is_none());
}

/// A local PDS on plain http (the dev network's) keeps its scheme through
/// the seed row.
#[test]
fn an_http_endpoint_keeps_its_scheme() {
    let key = format!("did:key:{}", Signer::new(crate::verify::synth::Curve::K256, 1).multibase());
    let did = "did:plc:aaaaaaaaaaaaaaaaaaaaaaaa";
    let l = op_line(did, "2025-01-02T03:04:05.678Z", &key, "http://localhost:3683", false);
    let seed = parse_line(l.as_bytes()).unwrap().seed;
    assert_eq!((seed.pds.as_deref(), seed.pds_http), (Some("localhost:3683"), true));
    let back = Seed::decode(&seed.encode()).unwrap();
    assert_eq!(back, seed);
    assert_eq!(back.identity(did).unwrap().pds.as_deref(), Some("http://localhost:3683"));
    let l = op_line(did, "2025-01-02T03:04:05.678Z", &key, "HTTPS://pds.example.com", false);
    assert!(!parse_line(l.as_bytes()).unwrap().seed.pds_http);
}

#[test]
fn choose_weighs_the_record_against_the_seed() {
    let ttl = 3600;
    let now = 1_800_000_000u32;
    let a = Bytes::from_static(b"key-a");
    let b = Bytes::from_static(b"key-b");
    let seed = |at_s: u32, k: &Bytes| Seed {
        created_ms: at_s as u64 * 1000,
        tombstone: false,
        key: Some(k.clone()),
        pds: Some("pds.test".into()),
        pds_http: false,
        lookup: false,
    };
    let rec = |fetched: u32, k: &Bytes| {
        let mut r = Record::new(HostKey::of("pds.test"), now - 100_000);
        r.key = Some(KeyBytes(k.clone()));
        r.pds = Some(HostKey::of("pds.test"));
        r.fetched_at = fetched;
        r
    };
    // nothing but a seed: used, however old
    assert_eq!(choose(None, Some(&seed(now - 90 * 86400, &a)), now, ttl), Pick::Seed);
    assert_eq!(choose(None, None, now, ttl), Pick::Resolve);
    let mut t = seed(now, &a);
    t.tombstone = true;
    assert_eq!(choose(None, Some(&t), now, ttl), Pick::Resolve);
    // a fresh record wins over an older op, a newer op over the record
    assert_eq!(choose(Some(&rec(now - 10, &b)), Some(&seed(now - 20, &a)), now, ttl), Pick::Record);
    assert_eq!(choose(Some(&rec(now - 20, &a)), Some(&seed(now - 10, &b)), now, ttl), Pick::Seed);
    // a stale record that agrees with an older op: the seed
    assert_eq!(choose(Some(&rec(now - 7200, &a)), Some(&seed(now - 86400, &a)), now, ttl), Pick::Seed);
    // a later resolve found another key, and the op is older than the TTL
    assert_eq!(choose(Some(&rec(now - 7200, &b)), Some(&seed(now - 86400, &a)), now, ttl), Pick::Resolve);
    // an #identity replayed and not resolved since
    assert_eq!(choose(Some(&rec(0, &a)), Some(&seed(now - 86400, &a)), now, ttl), Pick::Resolve);
    assert_eq!(choose(Some(&rec(0, &a)), Some(&seed(now - 60, &a)), now, ttl), Pick::Seed);
    assert_eq!(choose(Some(&rec(now - 7200, &a)), None, now, ttl), Pick::Resolve);
}

struct Fake {
    plc: Arc<FakePlc>,
    url: String,
    start_ms: u64,
}

async fn fake(hosts: u32, dids: u32) -> Fake {
    let layout = Layout::new("plc-seed-test", "http://127.0.0.1", 41000);
    let plc = FakePlc::new(layout, hosts, dids, export::now_ms() - 3_600_000, 1);
    let lis = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let url = format!("http://{}", lis.local_addr().unwrap());
    tokio::spawn(axum::serve(lis, plc.router(None)).into_future());
    let first = plc.export(None, 1);
    let start_ms = parse_line(first.trim().as_bytes()).unwrap().seed.created_ms;
    Fake { plc, url, start_ms }
}

impl Fake {
    fn cfg(&self, streams: usize) -> Config {
        let mut cfg = Config::new(&self.url);
        cfg.rate = 10_000.0;
        cfg.streams = streams;
        cfg.tail_poll = Duration::from_millis(50);
        cfg.apply_every = Duration::from_millis(200);
        cfg.checkpoint_every = Duration::from_millis(300);
        cfg.start_ms = self.start_ms;
        cfg
    }
}

fn cache(url: &str) -> Arc<IdentityCache<HttpFetch>> {
    Arc::new(IdentityCache::new(
        HttpFetch::new(url, true),
        Options { lookups_per_sec: 1e9, burst: 1e9, ..Options::default() },
    ))
}

struct Writes(Arc<SeedWriter>);

#[async_trait::async_trait]
impl Sink for Writes {
    async fn apply(&self, ops: Vec<(String, Seed)>) -> anyhow::Result<usize> {
        self.0.apply(ops).await
    }
    async fn flush(&self) -> anyhow::Result<()> {
        self.0.flush().await
    }
}

async fn wait(mut f: impl FnMut() -> bool, what: &str) {
    let t0 = Instant::now();
    while !f() {
        assert!(t0.elapsed() < Duration::from_secs(120), "timed out waiting for {what}");
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
}

/// Newest-wins holds without reads: in one batch, across batches, across
/// flushed L0s and compactions, and on a member's reader.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn an_older_op_never_replaces_a_newer_one() {
    let store = Store::memory(None);
    let w = SeedWriter::open(&store).await.unwrap();
    let did = "did:plc:aaaaaaaaaaaaaaaaaaaaaaaa";
    let seed = |ms: u64, pds: &str| Seed {
        created_ms: ms,
        tombstone: false,
        key: None,
        pds: Some(pds.into()),
        pds_http: false,
        lookup: false,
    };
    w.apply(vec![(did.into(), seed(2_000, "b.test")), (did.into(), seed(1_000, "a.test"))]).await.unwrap();
    assert_eq!(w.get(did).await.unwrap().unwrap().pds.as_deref(), Some("b.test"));
    w.flush().await.unwrap();
    for (ms, pds) in [(1_500, "c.test"), (2_000, "a.test")] {
        w.apply(vec![(did.into(), seed(ms, pds))]).await.unwrap();
        w.flush().await.unwrap();
    }
    // the same instant: the encodings decide, the same way everywhere
    assert_eq!(w.get(did).await.unwrap().unwrap().pds.as_deref(), Some("b.test"));
    w.apply(vec![(did.into(), seed(3_000, "d.test"))]).await.unwrap();
    w.flush().await.unwrap();
    w.apply(vec![(did.into(), seed(500, "e.test"))]).await.unwrap();
    w.flush().await.unwrap();
    // the test compactor polls every 5 s
    tokio::time::sleep(Duration::from_secs(11)).await;
    assert_eq!(w.get(did).await.unwrap().unwrap().pds.as_deref(), Some("d.test"));
    let r = SeedReader::new(store.clone());
    assert_eq!(r.get(did).await.unwrap().pds.as_deref(), Some("d.test"));
    w.close().await;
    let w = SeedWriter::open(&store).await.unwrap();
    assert_eq!(w.get(did).await.unwrap().unwrap().pds.as_deref(), Some("d.test"));
    w.close().await;
}

/// A takeover mid-export: the old leader stops between checkpoints (its
/// last rows unflushed), the new one opens the database (which fences the
/// old writer), resumes from the checkpoint's cursors rather than the
/// start, waits out the directory's 429s, and every DID ends up seeded and
/// readable by a member.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_new_leader_resumes_the_export_from_the_checkpoint() {
    const HOSTS: u32 = 4;
    const DIDS: u32 = 5_000;
    let f = fake(HOSTS, DIDS).await;
    f.plc.throttle_every.store(5, Relaxed);
    let store = Store::memory(None);
    let total = f.plc.op_count() as u64;

    let wa = Arc::new(SeedWriter::open(&store).await.unwrap());
    let a = Ingester::new(f.cfg(2), store.clone(), Arc::new(Writes(wa.clone())));
    let stats = a.stats.clone();
    let run = tokio::spawn(a.clone().supervise(Arc::new(move || stats.pages.load(Relaxed) < 9)));
    wait(|| a.stats.pages.load(Relaxed) >= 9, "the first leader's pages").await;
    run.abort();
    let ck = Checkpoint::load(&store).await.unwrap().expect("a checkpoint");
    assert!(ck.ops() > 0 && ck.ops() < total, "{} of {total}", ck.ops());
    assert!(a.stats.throttled.load(Relaxed) >= 1, "the fake answered 429s");
    let asked_before = f.plc.afters.lock().len();

    // the new leader's open fences the old writer
    let wb = Arc::new(SeedWriter::open(&store).await.unwrap());
    let late = Seed { created_ms: u64::MAX / 2, tombstone: true, key: None, pds: None, pds_http: false, lookup: false };
    let fenced = match wa.apply(vec![("did:plc:aaaaaaaaaaaaaaaaaaaaaaaa".into(), late)]).await {
        Err(_) => true,
        Ok(_) => wa.flush().await.is_err(),
    };
    assert!(fenced, "the old leader's writer still writes");

    let b = Ingester::new(f.cfg(2), store.clone(), Arc::new(Writes(wb.clone())));
    let run = tokio::spawn(b.clone().supervise(Arc::new(|| true)));
    wait(|| b.stats.caught_up.load(Relaxed), "the new leader's catch-up").await;
    run.abort();
    let afters = f.plc.afters.lock().clone();
    for w in &ck.windows {
        assert!(afters[asked_before..].contains(&w.after), "{} not resumed", w.after);
    }
    let read = b.stats.ops.load(Relaxed);
    eprintln!(
        "first leader {} ops (checkpointed {}), second {read}, total {total}",
        a.stats.ops.load(Relaxed),
        ck.ops()
    );
    assert!(read < total, "the new leader read {read} of {total}: it started over");
    assert!(b.stats.throttled.load(Relaxed) >= 1);
    wb.flush().await.unwrap();

    // a member reads the seeds through its own reader
    let reader = SeedReader::new(store.clone());
    for i in (0..DIDS).step_by(37) {
        let did = f.plc.layout.did(i % HOSTS, i);
        let s = reader.get(&did).await.unwrap_or_else(|| panic!("{did} missing"));
        let want = f.plc.key(i % HOSTS, i, 0).public_multibase();
        assert_eq!(s.identity(&did).unwrap().signing_key_multibase.as_deref(), Some(want.as_str()));
    }
}

/// The job on a three-node quorum log: the leader reads the export, dies
/// mid-export, and the new leader's job resumes it from the checkpoint; a
/// member's identity cache then fills from the seeds without asking PLC.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn the_leaders_job_survives_a_takeover_mid_export() {
    use crate::qlog::tests::Cluster;
    const HOSTS: u32 = 4;
    const DIDS: u32 = 4_000;
    let f = fake(HOSTS, DIDS).await;
    f.plc.throttle_every.store(7, Relaxed);
    let total = f.plc.op_count() as u64;
    let mut c = Cluster::with_cfg(3, None, None, 64 << 20).await;
    // as Node::start counts it
    let store = crate::qlog::bucket::counted(&c.store, "plc");
    let start = |c: &Cluster, id: &str| {
        let mut cfg = f.cfg(2);
        cfg.rate = 400.0;
        let j = PlcJob::new(cfg, store.clone(), SeedReader::new(store.clone()), cache(&f.url));
        tokio::spawn(j.clone().run(Arc::downgrade(&c.nodes[id].node)));
        j
    };
    let ids: Vec<String> = c.nodes.keys().cloned().collect();
    let mut jobs: HashMap<String, Arc<PlcJob>> = ids.iter().map(|id| (id.clone(), start(&c, id))).collect();
    let l = c.wait_leader(Duration::from_secs(5)).await;
    wait(|| jobs[&l].stats().is_some_and(|s| s.ops.load(Relaxed) > total / 4), "a quarter of the export").await;
    let first = jobs[&l].stats().unwrap().ops.load(Relaxed);
    // the leader dies, and its job with it
    jobs[&l].stop();
    c.kill(&l);
    let asked_before = f.plc.afters.lock().len();
    let l2 = c.wait_leader(Duration::from_secs(5)).await;
    assert_ne!(l, l2);
    wait(|| jobs[&l2].stats().is_some_and(|s| s.caught_up.load(Relaxed)), "the new leader's catch-up").await;
    let second = jobs[&l2].stats().unwrap().ops.load(Relaxed);
    eprintln!("leader {l} read {first} ops, then {l2} read {second}, of {total}");
    assert!(second < total, "the new leader started over ({second} of {total})");
    let rep = jobs[&l2].report().await.unwrap();
    assert!(rep.leader && rep.caught_up && rep.throttled >= 1, "{rep:?}");
    assert!(!rep.windows.is_empty() && rep.windows.iter().all(|w| (0.0..=1.0).contains(&w.progress)));
    assert!(f.plc.afters.lock().len() > asked_before);
    // the dead leader comes back as a follower, and doesn't read
    c.start(&l).await;
    jobs.insert(l.clone(), start(&c, &l));
    tokio::time::sleep(Duration::from_secs(1)).await;
    if c.leader().as_deref() != Some(l.as_str()) {
        assert!(jobs[&l].stats().is_none(), "a follower reads the export");
    }
    // what the new leader read is flushed at its next checkpoint
    tokio::time::sleep(Duration::from_millis(800)).await;
    let follower = ids.iter().find(|i| **i != l2).unwrap();
    let state = Arc::new(crate::state::StateStore::new(
        crate::node::adapters::VerifyChain,
        crate::state::tests::MapIdentity::new(),
        crate::state::ApplyConfig::default(),
    ));
    let cache = cache(&f.url);
    cache.set_seeder(Arc::new(Seeder {
        seeds: jobs[follower].seeds.clone(),
        state,
        ttl: Duration::from_secs(3600),
        web_ttl: Duration::from_secs(3600),
    }));
    let fetched = f.plc.doc_fetches.load(Relaxed);
    for i in (0..DIDS).step_by(53) {
        let did = f.plc.layout.did(i % HOSTS, i);
        let id = cache.lookup_paced(&did, false).await.unwrap();
        let want = f.plc.key(i % HOSTS, i, 0).public_multibase();
        assert_eq!(id.signing_key_multibase.as_deref(), Some(want.as_str()), "{did}");
    }
    assert_eq!(f.plc.doc_fetches.load(Relaxed), fetched, "a member asked PLC");
    for j in jobs.values() {
        j.stop();
    }
    c.shutdown();
}

fn env_or<T: std::str::FromStr>(k: &str, d: T) -> T {
    std::env::var(k).ok().and_then(|v| v.parse().ok()).unwrap_or(d)
}

/// `qlog_plc` requests so far: (writes, the rest).
fn plc_requests() -> (u64, u64) {
    use prometheus::core::Collector;
    let (mut w, mut r) = (0, 0);
    for mf in vlpds::metrics::OBJ_REQUESTS.collect() {
        for m in mf.get_metric() {
            let label = |k: &str| m.get_label().iter().find(|l| l.name() == k).map(|l| l.value().to_string());
            if label("client").as_deref() != Some("qlog_plc") {
                continue;
            }
            let n = m.get_counter().get_value() as u64;
            if label("op").is_some_and(|o| o.starts_with("put") || o.starts_with("mpu") || o == "copy") {
                w += n;
            } else {
                r += n;
            }
        }
    }
    (w, r)
}

/// The export's fill rate against the fake with plc.directory's page
/// latency and a bucket with R2's (`VLPDS_INJECT_QLOG_PLC_MS`, set by
/// the runner), reported per phase. `just`-free:
/// `VLPDS_INJECT_QLOG_PLC_MS=30,80 BENCH_SECS=60 cargo test --release
/// export_fill_bench -- --ignored --nocapture`.
#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
#[ignore]
async fn export_fill_bench() {
    // prod's cache holds a fraction of a 3 GB database, so most reads of a
    // stored row go to the bucket
    crate::qlog::cache::configure(env_or("BENCH_CACHE_MB", 8));
    let hosts = 8;
    let dids: u32 = env_or("BENCH_DIDS", 100_000);
    let f = fake(hosts, dids).await;
    f.plc.export_delay_ms.store(env_or("BENCH_EXPORT_MS", 300), Relaxed);
    // a second op for every DID (a handle change, a PDS move), as most of
    // plc.directory's history has: those writes find a stored row
    if env_or("BENCH_REPEAT", 1) > 0 {
        let at = export::now_ms() - 1_800_000;
        for i in 0..dids {
            for g in 0..hosts {
                f.plc.append(g, i, 1, at + (i * hosts + g) as u64 / 4);
            }
        }
    }
    let store = crate::qlog::bucket::counted(&Store::memory(None), "plc");
    let w = Arc::new(SeedWriter::open(&store).await.unwrap());
    let mut cfg = f.cfg(env_or("BENCH_STREAMS", 2));
    cfg.rate = env_or("BENCH_RATE", 1.0);
    cfg.apply_every = Duration::from_secs(2);
    cfg.checkpoint_every = Duration::from_secs(10);
    let ing = Ingester::new(cfg, store.clone(), Arc::new(Writes(w.clone())));
    let secs: u64 = env_or("BENCH_SECS", 60);
    // the cache's fetches, kept as the leader keeps them
    let learn: u64 = env_or("BENCH_LEARN_PER_SEC", 0);
    let seeds = SeedReader::new(store.clone());
    *seeds.writer.write() = Some(w.clone());
    let learner = {
        let seeds = seeds.clone();
        tokio::spawn(async move {
            if learn == 0 {
                return;
            }
            let mut tick = tokio::time::interval(Duration::from_secs_f64(1.0 / learn as f64));
            let mut i = 0u64;
            loop {
                tick.tick().await;
                i += 1;
                let now = crate::policy::store::now_ms() as u64;
                seeds.learn(&format!("did:plc:{i:z>24}"), Seed::from_lookup(None, now));
            }
        })
    };
    let writer = {
        let (seeds, w) = (seeds.clone(), w.clone());
        let t0 = Instant::now();
        tokio::spawn(async move {
            seeds.write_learned(&w, &move || t0.elapsed() < Duration::from_secs(secs)).await;
        })
    };
    let req0 = plc_requests();
    let t0 = Instant::now();
    let stats = ing.stats.clone();
    let run = tokio::spawn(
        ing.clone()
            .supervise(Arc::new(move || t0.elapsed() < Duration::from_secs(secs) && !stats.caught_up.load(Relaxed))),
    );
    let _ = run.await;
    let el = t0.elapsed().as_secs_f64();
    learner.abort();
    let _ = writer.await;
    let req1 = plc_requests();
    let s = &ing.stats;
    let reqs = s.requests.load(Relaxed);
    eprintln!(
        "BENCH ops={} ops/s={:.0} requests={} req/s={:.2} s/request={:.2} written={} learned={} bucket_writes={} bucket_reads={} phases_ms={:?}",
        s.ops.load(Relaxed),
        s.ops.load(Relaxed) as f64 / el,
        reqs,
        reqs as f64 / el,
        el / reqs.max(1) as f64,
        s.written.load(Relaxed),
        seeds.learned_written.load(Relaxed),
        req1.0 - req0.0,
        req1.1 - req0.1,
        s.phases.snapshot().map(|(k, us)| (k, us / 1000)),
    );
    w.close().await;
}

fn member_state() -> Arc<crate::node::State> {
    Arc::new(crate::state::StateStore::new(
        crate::node::adapters::VerifyChain,
        crate::state::tests::MapIdentity::new(),
        crate::state::ApplyConfig::default(),
    ))
}

fn seeder(seeds: &Arc<SeedReader>, web_ttl: Duration) -> Arc<Seeder> {
    Arc::new(Seeder { seeds: seeds.clone(), state: member_state(), ttl: Duration::from_secs(3600), web_ttl })
}

/// The leader's seeds with their writer and the lookups' writer loop, as
/// a term of the job sets them up.
struct Leader {
    w: Arc<SeedWriter>,
    seeds: Arc<SeedReader>,
    keep: Arc<std::sync::atomic::AtomicBool>,
    loop_: tokio::task::JoinHandle<()>,
}

async fn leader(store: &Store) -> Leader {
    let w = Arc::new(SeedWriter::open(store).await.unwrap());
    let seeds = SeedReader::new(store.clone());
    *seeds.writer.write() = Some(w.clone());
    let keep = Arc::new(std::sync::atomic::AtomicBool::new(true));
    let loop_ = {
        let (seeds, w, keep) = (seeds.clone(), w.clone(), keep.clone());
        tokio::spawn(async move { seeds.write_learned(&w, &move || keep.load(Relaxed)).await })
    };
    Leader { w, seeds, keep, loop_ }
}

impl Leader {
    /// Ends the term as the job does: a last batch, a flush, the close.
    async fn end(self) {
        self.keep.store(false, Relaxed);
        *self.seeds.writer.write() = None;
        self.loop_.await.unwrap();
        self.w.flush().await.unwrap();
        self.w.close().await;
    }
}

/// What the cache fetched on the leader is in the seeds after a restart:
/// the new process's cache fills from them without asking PLC.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_restart_keeps_the_documents_it_looked_up() {
    let f = fake(2, 40).await;
    let store = Store::memory(None);
    let l = leader(&store).await;
    let c = cache(&f.url);
    c.set_seeder(seeder(&l.seeds, Duration::from_secs(3600)));
    let dids: Vec<String> = (0..40).map(|i| f.plc.layout.did(i % 2, i)).collect();
    for d in &dids {
        c.lookup_paced(d, false).await.unwrap();
    }
    let fetched = f.plc.doc_fetches.load(Relaxed);
    assert_eq!(fetched, 40);
    l.end().await;

    let seeds = SeedReader::new(store.clone());
    let c = cache(&f.url);
    c.set_seeder(seeder(&seeds, Duration::from_secs(3600)));
    for (i, d) in dids.iter().enumerate() {
        let id = c.lookup_paced(d, false).await.unwrap();
        let want = f.plc.key(i as u32 % 2, i as u32, 0).public_multibase();
        assert_eq!(id.signing_key_multibase.as_deref(), Some(want.as_str()));
        assert_eq!(
            id.pds_host.as_ref().map(|h| h.0.clone()),
            identity::normalize_host(&f.plc.layout.host_url(i as u32 % 2)).map(|h| h.0)
        );
    }
    assert_eq!(f.plc.doc_fetches.load(Relaxed), fetched, "the restarted cache asked PLC");
    assert_eq!(c.stats.seeded.load(Relaxed), 40);
}

/// A fetched document is stamped with when it was fetched: an export op
/// created after that replaces it, an older one doesn't, and a forced
/// refresh (an #identity) replaces both.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn an_export_op_newer_than_the_fetch_wins() {
    let f = fake(1, 4).await;
    let store = Store::memory(None);
    let l = leader(&store).await;
    let c = cache(&f.url);
    c.set_seeder(seeder(&l.seeds, Duration::from_secs(3600)));
    let did = f.plc.layout.did(0, 1);
    c.lookup_paced(&did, false).await.unwrap();
    tokio::time::sleep(LEARN_EVERY * 2).await;
    let row = l.w.get(&did).await.unwrap().expect("the fetched document's row");
    assert!(row.lookup && !row.tombstone && row.key.is_some());
    let now = crate::policy::store::now_ms() as u64;
    assert!(row.created_ms <= now - LOOKUP_SKEW.as_millis() as u64);

    let op = |ms: u64, pds: &str| Seed {
        created_ms: ms,
        tombstone: false,
        key: row.key.clone(),
        pds: Some(pds.into()),
        pds_http: false,
        lookup: false,
    };
    // the history window reaching an older op
    l.w.apply(vec![(did.clone(), op(now - 86_400_000, "old.test"))]).await.unwrap();
    assert_eq!(l.w.get(&did).await.unwrap().unwrap(), row);
    // the tail reading a move made after the fetch's stamp: it wins, and
    // the cached copy goes
    let moved = op(row.created_ms + 1_000, "moved.test");
    invalidate_if_stale(&c, &did, &moved);
    assert!(c.cached(&did).is_none(), "the cached document outlived a newer op");
    l.w.apply(vec![(did.clone(), moved.clone())]).await.unwrap();
    assert_eq!(l.w.get(&did).await.unwrap().unwrap(), moved);
    let id = c.lookup_paced(&did, false).await.unwrap();
    assert_eq!(id.pds_host.as_ref().map(|h| h.0.as_str()), Some("moved.test"));
    // an op from before the cached fetch leaves the cache alone
    invalidate_if_stale(&c, &did, &op(now - 86_400_000, "old.test"));
    assert!(c.cached(&did).is_some());

    // an #identity: the forced fetch finds a rotated key and replaces the row
    f.plc.rotate_hidden(0, 1, 3);
    tokio::time::sleep(Duration::from_millis(5)).await;
    let id = c.refresh(&did).await.unwrap();
    let want = f.plc.key(0, 1, 3).public_multibase();
    assert_eq!(id.signing_key_multibase.as_deref(), Some(want.as_str()));
    tokio::time::sleep(LEARN_EVERY * 2).await;
    let row = l.w.get(&did).await.unwrap().unwrap();
    assert!(row.lookup);
    let mb = format!("z{}", bs58::encode(row.key.as_ref().unwrap()).into_string());
    assert_eq!(mb, want);
    l.end().await;
}

/// A did:web document from the seeds is used for `web_ttl`, then fetched
/// again; a did:plc one has no such bound (the export's tail and #identity
/// keep it current).
#[tokio::test]
async fn a_did_web_row_expires() {
    use crate::identity::Seeder as _;
    let store = Store::memory(None);
    let l = leader(&store).await;
    let s = seeder(&l.seeds, Duration::from_secs(600));
    let key = format!("did:key:{}", Signer::new(crate::verify::synth::Curve::K256, 1).multibase());
    let doc = |did: &str| {
        let l =
            op_line("did:plc:aaaaaaaaaaaaaaaaaaaaaaaa", "2025-01-02T03:04:05.678Z", &key, "https://pds.test", false);
        let seed = parse_line(l.as_bytes()).unwrap().seed;
        seed.identity(did).unwrap()
    };
    let now = crate::policy::store::now_ms() as u64;
    let (web, plc) = ("did:web:alice.test", "did:plc:bbbbbbbbbbbbbbbbbbbbbbbb");
    let old = now - 3_600_000;
    s.learned(web, Ok(&doc(web)), old);
    s.learned(plc, Ok(&doc(plc)), old);
    s.learned("did:web:bob.test", Ok(&doc("did:web:bob.test")), now);
    tokio::time::sleep(LEARN_EVERY * 2).await;
    assert!(s.seed(web).await.is_none(), "a did:web row past its TTL was used");
    assert!(s.seed("did:web:bob.test").await.is_some());
    assert!(s.seed(plc).await.is_some());
    l.end().await;
}

/// Lookups reach the seeds in batches, into the memtable: no bucket
/// writes of their own, and one batch per `LEARN_BATCH` documents.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn lookups_are_written_in_batches() {
    use futures::TryStreamExt;
    let store = Store::memory(None);
    let l = leader(&store).await;
    let objects = || async { store.raw.list(None).try_collect::<Vec<_>>().await.unwrap().len() };
    let before = objects().await;
    let batches0 = l.seeds.learned_batches.load(Relaxed);
    let n = 3 * LEARN_BATCH + 10;
    let now = crate::policy::store::now_ms() as u64;
    for i in 0..n {
        let did = format!("did:plc:{i:0>24}");
        l.seeds.learn(&did, Seed::from_lookup(None, now));
    }
    tokio::time::sleep(LEARN_EVERY * 3).await;
    let batches = l.seeds.learned_batches.load(Relaxed) - batches0;
    assert!((1..=5).contains(&batches), "{batches} batches for {n} documents");
    assert_eq!(l.seeds.learned_written.load(Relaxed), n as u64);
    // at a cold start's pace, 100 lookups a second: about a batch a second
    let batches0 = l.seeds.learned_batches.load(Relaxed);
    for i in 0..200 {
        l.seeds.learn(&format!("did:plc:{:a>24}", i), Seed::from_lookup(None, now));
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
    tokio::time::sleep(LEARN_EVERY * 2).await;
    let batches = l.seeds.learned_batches.load(Relaxed) - batches0;
    assert!((1..=5).contains(&batches), "{batches} batches for 200 paced lookups");
    assert_eq!(l.seeds.learned_written.load(Relaxed), n as u64 + 200);
    assert_eq!(objects().await, before, "learning wrote to the bucket");
    // a follower keeps nothing
    let follower = SeedReader::new(store.clone());
    follower.learn("did:plc:cccccccccccccccccccccccc", Seed::from_lookup(None, now));
    assert!(follower.learned.lock().is_empty());
    l.end().await;
}

/// The export's readers aren't held up by the sink: with plc.directory's
/// page latency, more windows read more pages.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn export_throughput_scales_with_streams() {
    let layout = Layout::new("plc-seed-streams", "http://127.0.0.1", 41000);
    // ops up to now, so every window has its share
    let plc = FakePlc::new(layout, 4, 20_000, export::now_ms(), 10);
    plc.export_delay_ms.store(150, Relaxed);
    let lis = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let url = format!("http://{}", lis.local_addr().unwrap());
    tokio::spawn(axum::serve(lis, plc.router(None)).into_future());
    let first = plc.export(None, 1);
    let f = Fake { plc, url, start_ms: parse_line(first.trim().as_bytes()).unwrap().seed.created_ms };
    let mut rates = Vec::new();
    for streams in [1, 4] {
        let store = Store::memory(None);
        let w = Arc::new(SeedWriter::open(&store).await.unwrap());
        let mut cfg = f.cfg(streams);
        cfg.checkpoint_every = Duration::from_secs(1);
        let ing = Ingester::new(cfg, store.clone(), Arc::new(Writes(w.clone())));
        let t0 = Instant::now();
        let run = tokio::spawn(ing.clone().supervise(Arc::new(move || t0.elapsed() < Duration::from_secs(4))));
        run.await.unwrap();
        let pages = ing.stats.pages.load(Relaxed) as f64 / t0.elapsed().as_secs_f64();
        eprintln!("{streams} streams: {pages:.1} pages/s, phases {:?}", ing.stats.phases.snapshot());
        rates.push(pages);
        w.close().await;
    }
    assert!(rates[1] > rates[0] * 2.5, "pages/s with 1 and 4 streams: {rates:?}");
}

#[test]
fn rate_meter_rises_holds_and_decays() {
    use super::ingest::{RateMeter, Stats};
    use std::time::{Duration, Instant};
    let stats = Stats::default();
    let t0 = Instant::now();
    let mut m = RateMeter::new(t0, 0);
    assert_eq!(stats.rate(), 0.0);
    // 1,000-op pages every 2 s (500 ops/s), sampled every second like the
    // loop does, so samples alternate between a page and nothing.
    let mut ops = 0u64;
    let at = |secs: u64, m: &mut RateMeter, ops: u64| m.sample(&stats, t0 + Duration::from_secs(secs), ops);
    for s in 1..=2 {
        if s % 2 == 0 {
            ops += 1000;
        }
        at(s, &mut m, ops);
    }
    let early = stats.rate();
    assert!(early > 250.0, "rises within a couple of samples: {early}");
    // Sub-interval samples change nothing.
    m.sample(&stats, t0 + Duration::from_millis(2500), ops + 10_000);
    assert_eq!(stats.rate(), early);
    for s in 3..=120 {
        if s % 2 == 0 {
            ops += 1000;
        }
        at(s, &mut m, ops);
    }
    let steady = stats.rate();
    assert!((steady - 500.0).abs() / 500.0 < 0.05, "steady state: {steady}");
    // Reads are pure: they never reset or move the value.
    assert_eq!(stats.rate(), stats.rate());
    assert_eq!(stats.rate(), steady);
    // Idle (paced out or caught up); the idle loop still wakes every 2 s.
    for s in (122..=180).step_by(2) {
        at(s, &mut m, ops);
    }
    let idle = stats.rate();
    assert!(idle < steady * 0.05, "decays when idle: {idle}");
    at(240, &mut m, ops);
    assert!(stats.rate() < idle);
}
