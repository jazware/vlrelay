use super::ingest::{Checkpoint, Config, Ingester};
use super::*;
use crate::identity::{HttpFetch, IdentityCache, Options};
use crate::node::adapters::{CacheIdentity, VerifyChain};
use crate::state::record::SigningKey as KeyBytes;
use crate::state::{ApplyConfig, StateStore};
use crate::verify::synth::{Repo, Signer};
use std::sync::atomic::Ordering::Relaxed;
use std::time::Instant;
use vlpds::store::Store;

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
/// the seed row, so the archive fetches it over http; a record's identity
/// takes the scheme from the seed of the same host.
#[tokio::test]
async fn an_http_endpoint_keeps_its_scheme() {
    let key = format!("did:key:{}", Signer::new(crate::verify::synth::Curve::K256, 1).multibase());
    let did = "did:plc:aaaaaaaaaaaaaaaaaaaaaaaa";
    let l = op_line(did, "2025-01-02T03:04:05.678Z", &key, "http://localhost:3683", false);
    let seed = parse_line(l.as_bytes()).unwrap().seed;
    assert_eq!((seed.pds.as_deref(), seed.pds_http), (Some("localhost:3683"), true));
    let back = Seed::decode(&seed.encode()).unwrap();
    assert_eq!(back, seed);
    assert_eq!(back.identity(did).unwrap().pds.as_deref(), Some("http://localhost:3683"));
    let l = op_line(did, "2025-01-02T03:04:05.678Z", &key, "HTTPS://pds.example.com", false);
    let s2 = parse_line(l.as_bytes()).unwrap().seed;
    assert_eq!(s2.identity(did).unwrap().pds.as_deref(), Some("https://pds.example.com"));
    assert!(seed.differs(&Seed { pds_http: false, ..seed.clone() }));

    let st = Arc::new(StateStore::new(
        Store::memory(None),
        vlpds::slots::Layout::uniform(1).shards,
        crate::state::StubChain,
        crate::state::tests::MapIdentity::new(),
        ApplyConfig::default(),
    ));
    st.open_shard(vlpds::slots::ShardId(0), None).await.unwrap();
    st.note_host(HostKey::of("localhost:3683"), "localhost:3683");
    let seeds = LocalSeeds::new(st, Duration::from_secs(3600));
    let mut rec = Record::new(HostKey::of("localhost:3683"), 1);
    rec.pds = Some(HostKey::of("localhost:3683"));
    rec.key = Some(KeyBytes(seed.key.clone().unwrap()));
    let id = seeds.record_identity(did, &rec, Some(&seed)).unwrap();
    assert_eq!(id.pds.as_deref(), Some("http://localhost:3683"));
    assert_eq!(seeds.record_identity(did, &rec, None).unwrap().pds.as_deref(), Some("https://localhost:3683"));
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
    // a stale record and no seed: resolve, as before seeds
    assert_eq!(choose(Some(&rec(now - 7200, &a)), None, now, ttl), Pick::Resolve);
}

struct Rig {
    plc: Arc<FakePlc>,
    url: String,
    identity: Arc<IdentityCache<HttpFetch>>,
    state: Arc<StateStore<VerifyChain>>,
    seeds: Arc<LocalSeeds<VerifyChain>>,
    store: Store,
}

async fn rig(hosts: u32, dids: u32, end_ms: u64) -> Rig {
    let layout = Layout::new("plc-seed-test", "http://127.0.0.1", 41000);
    let plc = FakePlc::new(layout, hosts, dids, end_ms, 1);
    let lis = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let url = format!("http://{}", lis.local_addr().unwrap());
    tokio::spawn(axum::serve(lis, plc.router(None)).into_future());
    let identity = Arc::new(IdentityCache::new(
        HttpFetch::new(&url, true),
        Options { lookups_per_sec: 1e9, burst: 1e9, ..Options::default() },
    ));
    let store = Store::memory(None);
    let layout = vlpds::slots::Layout::uniform(4).shards;
    let state = Arc::new(StateStore::new(
        store.clone(),
        layout.clone(),
        VerifyChain,
        Arc::new(CacheIdentity(identity.clone())),
        ApplyConfig::default(),
    ));
    for s in layout {
        state.open_shard(s.id, None).await.unwrap();
    }
    let seeds = Arc::new(LocalSeeds::new(state.clone(), Options::default().ttl));
    identity.set_seeder(Arc::new(SingleNode(seeds.clone())));
    Rig { plc, url, identity, state, seeds, store }
}

impl Rig {
    fn ingester(&self, f: impl FnOnce(&mut Config)) -> Arc<Ingester> {
        let mut cfg = Config::new(&self.url);
        cfg.rate = 10_000.0;
        cfg.tail_poll = Duration::from_millis(50);
        cfg.apply_every = Duration::from_millis(200);
        cfg.checkpoint_every = Duration::from_millis(500);
        cfg.start_ms = self.plc.layout_start();
        f(&mut cfg);
        let sink = Arc::new(LocalSink { seeds: self.seeds.clone(), cache: self.identity.clone() });
        Ingester::new(cfg, self.store.clone(), sink)
    }

    fn host(&self, g: u32) -> Host {
        identity::normalize_host(&self.plc.layout.host_url(g)).unwrap()
    }
}

impl FakePlc {
    fn layout_start(&self) -> u64 {
        let first = self.export(None, 1);
        parse_line(first.trim().as_bytes()).unwrap().seed.created_ms
    }
}

async fn wait(mut f: impl FnMut() -> bool, what: &str) {
    let t0 = Instant::now();
    while !f() {
        assert!(t0.elapsed() < Duration::from_secs(120), "timed out waiting for {what}");
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
}

/// Whether a commit by `did` signed with `key` passes the host stage's
/// check against the cached (or seeded) key.
async fn verifies(r: &Rig, did: &str, key: Signer) -> bool {
    let mut repo = Repo::new(did, key, 1);
    let ops = repo.mixed_ops(1);
    let frame = repo.commit(&ops);
    let crate::event::Event::Commit(c) = crate::event::parse(frame, &crate::event::Limits::default()).unwrap() else {
        panic!("not a commit")
    };
    let id = r.identity.lookup_paced(did, false).await.unwrap();
    crate::verify::verify_commit(&c, id.signing_key.as_ref().unwrap()).is_ok()
}

/// One commit by `did`, signed with `key`, through the host stage's check
/// and the DID owner's apply. Returns whether it was accepted.
async fn commit(r: &Rig, did: &str, host: &Host, key: Signer) -> bool {
    let frame = {
        let mut repo = Repo::new(did, key, 1);
        let ops = repo.mixed_ops(1);
        repo.commit(&ops)
    };
    let crate::event::Event::Commit(c) = crate::event::parse(frame, &crate::event::Limits::default()).unwrap() else {
        panic!("not a commit")
    };
    let id = r.identity.lookup_paced(did, false).await.unwrap();
    let Ok(v) = crate::verify::verify_commit(&c, id.signing_key.as_ref().unwrap()) else { return false };
    let ev =
        crate::state::Incoming { did, host, now: crate::state::now_secs(), kind: crate::state::EventKind::Commit(v) };
    matches!(r.state.apply(ev).await, Ok(crate::state::Applied::Append(_)))
}

#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
async fn a_cold_relay_seeds_100k_dids_then_takes_their_traffic_without_lookups() {
    const HOSTS: u32 = 10;
    const DIDS: u32 = 10_000;
    let t0 = Instant::now();
    let r = rig(HOSTS, DIDS, export::now_ms() - 3_600_000).await;
    eprintln!("fake export of {} ops built in {:?}", r.plc.op_count(), t0.elapsed());

    let ing = r.ingester(|_| {});
    let t0 = Instant::now();
    let run = tokio::spawn(ing.clone().supervise(Arc::new(|| true)));
    wait(|| ing.stats.caught_up.load(Relaxed), "catch-up").await;
    let took = t0.elapsed();
    run.abort();
    let s = &ing.stats;
    let (ops, bytes, pages) = (s.ops.load(Relaxed), s.bytes.load(Relaxed), s.pages.load(Relaxed));
    eprintln!(
        "ingest: {ops} ops in {pages} pages, {:.1} MB, {took:?}: {:.0} ops/s, {:.1} MB/s, written {}",
        bytes as f64 / 1e6,
        ops as f64 / took.as_secs_f64(),
        bytes as f64 / 1e6 / took.as_secs_f64(),
        s.written.load(Relaxed)
    );
    assert!(ops >= (HOSTS * DIDS) as u64);
    assert_eq!(s.written.load(Relaxed), (HOSTS * DIDS) as u64);
    assert_eq!(s.invalid.load(Relaxed), 0);

    // storage per DID: the raw rows, and the shards' SSTs after a flush
    r.seeds.flush().await.unwrap();
    let mut raw = 0usize;
    for i in (0..DIDS).step_by(97) {
        let did = r.plc.layout.did(i % HOSTS, i);
        raw += seed_key(&did).len() + r.seeds.get(&did).await.unwrap().unwrap().encode().len();
    }
    let raw_per = raw as f64 / DIDS.div_ceil(97) as f64;
    let sst: u64 = r.state.shards().iter().map(|s| s.sst_bytes()).sum();
    eprintln!(
        "storage: {raw_per:.1} bytes per DID raw (key + value), {:.1} bytes per DID in SSTs",
        sst as f64 / (HOSTS * DIDS) as f64
    );

    // every DID commits once: verified with the seeded key, accepted by the
    // DID owner, and nothing asks PLC
    let jobs: Vec<(u32, u32)> = (0..HOSTS * DIDS).map(|n| (n % HOSTS, n / HOSTS)).collect();
    let r = Arc::new(r);
    let t0 = Instant::now();
    let next = Arc::new(std::sync::atomic::AtomicUsize::new(0));
    let mut workers = Vec::new();
    for _ in 0..32 {
        let (r, jobs, next) = (r.clone(), jobs.clone(), next.clone());
        workers.push(tokio::spawn(async move {
            let mut ok = 0;
            loop {
                let Some(&(g, i)) = jobs.get(next.fetch_add(1, Relaxed)) else { return ok };
                let did = r.plc.layout.did(g, i);
                if commit(&r, &did, &r.host(g), Signer::K256(r.plc.key(g, i, 0))).await {
                    ok += 1;
                }
            }
        }));
    }
    let mut accepted = 0;
    for w in workers {
        accepted += w.await.unwrap();
    }
    eprintln!("traffic: {accepted} first commits in {:?}", t0.elapsed());
    assert_eq!(accepted, HOSTS * DIDS);
    assert_eq!(r.plc.doc_fetches.load(Relaxed), 0, "per-DID PLC lookups");
    assert_eq!(r.identity.stats.fetches.load(Relaxed), 0);
    assert_eq!(r.identity.stats.seeded.load(Relaxed), (HOSTS * DIDS) as u64);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_restart_mid_ingest_resumes_from_the_checkpoint() {
    let r = rig(4, 5_000, export::now_ms() - 3_600_000).await;
    let total = r.plc.op_count() as u64;
    // the first run stops after ~a third of the pages, with a 503 on the way
    r.plc.fail_next.store(2, Relaxed);
    let first = r.ingester(|c| c.streams = 2);
    let stats = first.stats.clone();
    first.run(Arc::new(move || stats.pages.load(Relaxed) < 7)).await.unwrap();
    let ck = Checkpoint::load(&r.store).await.unwrap().unwrap();
    let done_first = ck.ops();
    assert!(done_first > 0 && done_first < total, "{done_first} of {total}");
    assert!(first.stats.throttled.load(Relaxed) >= 1);
    let asked_before = r.plc.afters.lock().len();

    let second = r.ingester(|c| c.streams = 2);
    let run = tokio::spawn(second.clone().supervise(Arc::new(|| true)));
    wait(|| second.stats.caught_up.load(Relaxed), "catch-up").await;
    run.abort();
    // the second run asked from the checkpoint's cursors, not the start
    let afters = r.plc.afters.lock().clone();
    for w in &ck.windows {
        assert!(afters[asked_before..].contains(&w.after), "{} not resumed", w.after);
    }
    let read = second.stats.ops.load(Relaxed);
    eprintln!("first run {done_first} ops, second {read}, total {total}");
    assert!(read < total, "the second run read {read} of {total}");
    for i in (0..5_000).step_by(7) {
        let did = r.plc.layout.did(i % 4, i);
        assert!(r.seeds.get(&did).await.unwrap().is_some(), "{did} missing");
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_key_rotation_in_the_export_tail_reaches_the_cache() {
    let r = rig(2, 50, export::now_ms() - 3_600_000).await;
    let ing = r.ingester(|c| c.streams = 1);
    let run = tokio::spawn(ing.clone().supervise(Arc::new(|| true)));
    wait(|| ing.stats.caught_up.load(Relaxed), "catch-up").await;
    let (g, i) = (1, 7);
    let did = r.plc.layout.did(g, i);
    assert!(commit(&r, &did, &r.host(g), Signer::K256(r.plc.key(g, i, 0))).await);

    let written = ing.stats.written.load(Relaxed);
    r.plc.append(g, i, 1, export::now_ms());
    wait(|| ing.stats.written.load(Relaxed) > written, "the rotation's op").await;
    assert!(r.identity.cached(&did).is_none(), "the rotation dropped the cached key");
    let id = r.identity.resolve(&did).await.unwrap();
    assert_eq!(id.signing_key_multibase.as_deref(), Some(r.plc.key(g, i, 1).public_multibase().as_str()));
    assert!(verifies(&r, &did, Signer::K256(r.plc.key(g, i, 1))).await);
    assert!(!verifies(&r, &did, Signer::K256(r.plc.key(g, i, 0))).await);
    assert_eq!(r.plc.doc_fetches.load(Relaxed), 0);
    run.abort();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn an_identity_event_newer_than_the_export_wins() {
    let r = rig(2, 50, export::now_ms() - 3_600_000).await;
    let ing = r.ingester(|c| c.streams = 1);
    let run = tokio::spawn(ing.clone().supervise(Arc::new(|| true)));
    wait(|| ing.stats.caught_up.load(Relaxed), "catch-up").await;
    let (g, i) = (0, 3);
    let did = r.plc.layout.did(g, i);
    let host = r.host(g);

    // the directory has key 2; the export hasn't shown it
    r.plc.rotate_hidden(g, i, 2);
    r.identity.invalidate(&did);
    let ev = crate::state::Incoming {
        did: &did,
        host: &host,
        now: crate::state::now_secs(),
        kind: crate::state::EventKind::Identity,
    };
    assert!(matches!(r.state.apply(ev).await, Ok(crate::state::Applied::Append(_))));
    assert_eq!(r.plc.doc_fetches.load(Relaxed), 1, "the #identity resolved fresh");

    // then the export delivers an op from before the #identity, with key 1
    let written = ing.stats.written.load(Relaxed);
    r.plc.append(g, i, 1, export::now_ms() - 30_000);
    r.plc.rotate_hidden(g, i, 2);
    wait(|| ing.stats.written.load(Relaxed) > written, "the older op").await;
    let seeded = r.seeds.get(&did).await.unwrap().unwrap();
    assert_eq!(seeded.identity(&did).unwrap().signing_key_multibase, Some(r.plc.key(g, i, 1).public_multibase()));

    r.identity.invalidate(&did);
    let id = r.identity.resolve(&did).await.unwrap();
    assert_eq!(id.signing_key_multibase.as_deref(), Some(r.plc.key(g, i, 2).public_multibase().as_str()));
    assert!(verifies(&r, &did, Signer::K256(r.plc.key(g, i, 2))).await);
    assert_eq!(r.plc.doc_fetches.load(Relaxed), 1);
    run.abort();
}

#[tokio::test]
async fn an_op_no_newer_than_the_stored_one_writes_nothing() {
    let r = rig(1, 4, export::now_ms() - 3_600_000).await;
    let did = r.plc.layout.did(0, 1);
    let seed =
        |ms: u64| Seed { created_ms: ms, tombstone: false, key: None, pds: Some("pds.test".into()), pds_http: false };
    assert_eq!(r.seeds.apply(vec![(did.clone(), seed(2_000))]).await.unwrap().written, 1);
    for ms in [1_000, 2_000] {
        assert_eq!(r.seeds.apply(vec![(did.clone(), seed(ms))]).await.unwrap().written, 0);
    }
    assert_eq!(r.seeds.get(&did).await.unwrap().unwrap().created_ms, 2_000);
}
