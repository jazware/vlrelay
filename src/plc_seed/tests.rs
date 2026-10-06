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
mod export;
#[allow(dead_code)]
#[path = "../fakepds/fleet.rs"]
mod fleet;

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
        Ok(self.0.apply(ops).await?.written)
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

#[tokio::test]
async fn an_op_no_newer_than_the_stored_one_writes_nothing() {
    let store = Store::memory(None);
    let w = SeedWriter::open(&store, Duration::from_secs(3600)).await.unwrap();
    let did = "did:plc:aaaaaaaaaaaaaaaaaaaaaaaa";
    let seed =
        |ms: u64| Seed { created_ms: ms, tombstone: false, key: None, pds: Some("pds.test".into()), pds_http: false };
    assert_eq!(w.apply(vec![(did.into(), seed(2_000))]).await.unwrap().written, 1);
    for ms in [1_000, 2_000] {
        assert_eq!(w.apply(vec![(did.into(), seed(ms))]).await.unwrap().written, 0);
    }
    assert_eq!(w.get(did).await.unwrap().unwrap().created_ms, 2_000);
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

    let wa = Arc::new(SeedWriter::open(&store, Duration::from_secs(3600)).await.unwrap());
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
    let wb = Arc::new(SeedWriter::open(&store, Duration::from_secs(3600)).await.unwrap());
    let late = Seed { created_ms: u64::MAX / 2, tombstone: true, key: None, pds: None, pds_http: false };
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
    let store = c.store.clone();
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
    cache.set_seeder(Arc::new(Seeder { seeds: jobs[follower].seeds.clone(), state, ttl: Duration::from_secs(3600) }));
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
