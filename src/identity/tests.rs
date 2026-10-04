use super::*;
use crate::verify::synth::{Curve, Signer};
use serde_json::json;
use std::sync::atomic::AtomicUsize;

#[derive(Default)]
struct Mock {
    docs: Mutex<HashMap<String, J>>,
    calls: AtomicUsize,
    delay: Duration,
}

impl Fetch for Arc<Mock> {
    async fn fetch(&self, did: &str) -> Result<J, LookupError> {
        self.calls.fetch_add(1, Ordering::SeqCst);
        tokio::time::sleep(self.delay).await;
        self.docs.lock().get(did).cloned().ok_or(LookupError::NotFound)
    }
}

fn doc(did: &str, key: &str, pds: &str) -> J {
    json!({
        "id": did,
        "alsoKnownAs": ["at://alice.test"],
        "verificationMethod": [{"id": format!("{did}#atproto"), "type": "Multikey", "controller": did, "publicKeyMultibase": key}],
        "service": [{"id": "#atproto_pds", "type": "AtprotoPersonalDataServer", "serviceEndpoint": pds}],
    })
}

fn cache(m: &Arc<Mock>, opts: Options) -> IdentityCache<Arc<Mock>> {
    IdentityCache::new(m.clone(), opts)
}

#[test]
fn hosts_normalize() {
    let h = |s: &str| normalize_host(s).map(|h| h.0);
    assert_eq!(h("https://PDS.Example.com/"), Some("pds.example.com".into()));
    assert_eq!(h("https://pds.example.com:443"), Some("pds.example.com".into()));
    assert_eq!(h("http://pds.example.com:80/xrpc"), Some("pds.example.com".into()));
    assert_eq!(h("https://pds.example.com:8443"), Some("pds.example.com:8443".into()));
    assert_eq!(h("pds.example.com."), Some("pds.example.com".into()));
    assert_eq!(h("localhost:2583"), Some("localhost:2583".into()));
    assert_eq!(h("ftp://pds.example.com"), None);
    assert_eq!(h(""), None);
}

#[test]
fn documents_parse() {
    let k = Signer::new(Curve::P256, 1).multibase();
    let id = Identity::from_doc("did:plc:abc", &doc("did:plc:abc", &k, "https://Pds.Example.com")).unwrap();
    assert!(matches!(id.signing_key, Some(SigningKey::P256(_))));
    assert_eq!(id.pds_host, Some(Host("pds.example.com".into())));
    assert_eq!(id.handle.as_deref(), Some("alice.test"));
    assert!(id.authorized(&Host("pds.example.com".into())));
    assert!(!id.authorized(&Host("evil.example.com".into())));
    assert!(Identity::from_doc("did:plc:other", &doc("did:plc:abc", &k, "https://x")).is_err());
    let bad = Identity::from_doc("did:plc:abc", &doc("did:plc:abc", "zNotAKey", "not a url")).unwrap();
    assert!(bad.signing_key.is_none() && bad.pds_host.is_none());
}

#[tokio::test]
async fn single_flight_and_caching() {
    let m = Arc::new(Mock { delay: Duration::from_millis(50), ..Default::default() });
    let k = Signer::new(Curve::K256, 1).multibase();
    m.docs.lock().insert("did:plc:abc".into(), doc("did:plc:abc", &k, "https://pds.example.com"));
    let c = Arc::new(cache(&m, Options::default()));
    let tasks: Vec<_> = (0..50)
        .map(|_| {
            let c = c.clone();
            tokio::spawn(async move { c.resolve("did:plc:abc").await })
        })
        .collect();
    for t in tasks {
        assert!(t.await.unwrap().is_ok());
    }
    assert_eq!(m.calls.load(Ordering::SeqCst), 1, "one fetch for 50 concurrent lookups");
    assert!(c.resolve("did:plc:abc").await.is_ok());
    assert_eq!(m.calls.load(Ordering::SeqCst), 1);
    assert!(c.authorized("did:plc:abc", &Host("pds.example.com".into())));
    assert!(!c.authorized("did:plc:nope", &Host("pds.example.com".into())));

    // #identity: a forced refresh fetches again and sees the move
    m.docs.lock().insert("did:plc:abc".into(), doc("did:plc:abc", &k, "https://new.example.com"));
    assert!(c.refresh("did:plc:abc").await.is_ok());
    assert_eq!(m.calls.load(Ordering::SeqCst), 2);
    assert!(c.authorized("did:plc:abc", &Host("new.example.com".into())));
}

#[tokio::test]
async fn negative_results_cached() {
    let m = Arc::new(Mock::default());
    let c = cache(&m, Options { negative_ttl: Duration::from_millis(100), ..Options::default() });
    for _ in 0..10 {
        assert_eq!(c.resolve("did:plc:missing").await.err(), Some(LookupError::NotFound));
    }
    assert_eq!(m.calls.load(Ordering::SeqCst), 1);
    tokio::time::sleep(Duration::from_millis(120)).await;
    assert!(c.resolve("did:plc:missing").await.is_err());
    assert_eq!(m.calls.load(Ordering::SeqCst), 2);
    // malformed DIDs never reach the fetcher
    assert_eq!(c.resolve("did:plc:").await.err(), Some(LookupError::BadDid));
    assert_eq!(m.calls.load(Ordering::SeqCst), 2);
}

#[tokio::test]
async fn budget_limits_rate() {
    let m = Arc::new(Mock::default());
    let c = cache(
        &m,
        Options {
            lookups_per_sec: 20.0,
            burst: 2.0,
            max_budget_wait: Duration::from_millis(200),
            ..Options::default()
        },
    );
    let t0 = Instant::now();
    let mut over = 0;
    for i in 0..10 {
        if matches!(c.resolve(&format!("did:plc:d{i}")).await, Err(LookupError::OverBudget)) {
            over += 1;
        }
    }
    // 2 burst + 8 at 20/s: ~400 ms, every wait under the 200 ms cap
    assert_eq!(over, 0);
    assert!(t0.elapsed() >= Duration::from_millis(350), "{:?}", t0.elapsed());

    // concurrent demand past the wait cap is turned away, and not cached
    let c = Arc::new(cache(
        &m,
        Options { lookups_per_sec: 5.0, burst: 1.0, max_budget_wait: Duration::from_millis(250), ..Options::default() },
    ));
    let tasks: Vec<_> = (0..10)
        .map(|i| {
            let c = c.clone();
            tokio::spawn(async move { c.resolve(&format!("did:plc:e{i}")).await })
        })
        .collect();
    let mut over = 0;
    for t in tasks {
        if matches!(t.await.unwrap(), Err(LookupError::OverBudget)) {
            over += 1;
        }
    }
    assert!((7..=9).contains(&over), "{over}");
    assert_eq!(c.stats.over_budget.load(Ordering::Relaxed), over);
}

#[tokio::test]
async fn capacity_is_bounded() {
    let m = Arc::new(Mock::default());
    let c = cache(&m, Options { capacity: 100, lookups_per_sec: 1e6, burst: 1e6, ..Options::default() });
    for i in 0..1000 {
        let _ = c.resolve(&format!("did:plc:f{i}")).await;
    }
    assert!(c.len() <= 100, "{}", c.len());
}

#[tokio::test]
async fn check_host_rechecks_on_mismatch() {
    let m = Arc::new(Mock::default());
    let k = Signer::new(Curve::K256, 1).multibase();
    m.docs.lock().insert("did:plc:mover".into(), doc("did:plc:mover", &k, "https://old.example.com"));
    let c = cache(&m, Options::default());
    assert_eq!(c.check_host("did:plc:mover", &Host("old.example.com".into())).await, Ok(true));
    m.docs.lock().insert("did:plc:mover".into(), doc("did:plc:mover", &k, "https://new.example.com"));
    assert_eq!(c.check_host("did:plc:mover", &Host("new.example.com".into())).await, Ok(true));
    assert_eq!(c.check_host("did:plc:mover", &Host("old.example.com".into())).await, Ok(false));
}

#[tokio::test]
async fn the_cluster_gate_holds_back_fetches() {
    let m = Arc::new(Mock::default());
    m.docs.lock().insert("did:plc:ok".into(), doc("did:plc:ok", "zQ3sh", "https://pds.example.com"));
    let c = cache(&m, Options { max_budget_wait: Duration::from_millis(100), ..Options::default() });
    let open = Arc::new(std::sync::atomic::AtomicBool::new(false));
    let o = open.clone();
    c.set_budget_gate(Arc::new(move || o.load(Ordering::SeqCst)));
    assert!(matches!(c.resolve("did:plc:ok").await, Err(LookupError::OverBudget)));
    assert_eq!(m.calls.load(Ordering::SeqCst), 0);
    // over budget isn't cached: the next try fetches once the gate opens
    open.store(true, Ordering::SeqCst);
    assert!(c.resolve("did:plc:ok").await.is_ok());
    assert_eq!(m.calls.load(Ordering::SeqCst), 1);
}

#[tokio::test]
async fn handles_are_found_in_cached_documents() {
    let m = Arc::new(Mock::default());
    m.docs.lock().insert("did:plc:a".into(), doc("did:plc:a", "zQ3sh", "https://pds.example.com"));
    let c = cache(&m, Options::default());
    c.resolve("did:plc:a").await.unwrap();
    let dids = |q: &str| c.find_handle(q, 10).iter().map(|i| i.did.clone()).collect::<Vec<_>>();
    assert_eq!(dids("alice.test"), ["did:plc:a"]);
    assert_eq!(dids("@Alice.Test"), ["did:plc:a"]);
    assert_eq!(dids("ali*"), ["did:plc:a"]);
    assert!(dids("ali").is_empty() && dids("*").is_empty() && dids("bob.test").is_empty());
}

#[tokio::test]
async fn expired_entries_are_swept_before_the_cache_fills() {
    let m = Arc::new(Mock::default());
    let k = Signer::new(Curve::K256, 1).multibase();
    for i in 0..50 {
        let did = format!("did:plc:s{i}");
        m.docs.lock().insert(did.clone(), doc(&did, &k, "https://pds.example.com"));
    }
    let c = cache(
        &m,
        Options {
            ttl: Duration::from_millis(500),
            negative_ttl: Duration::from_millis(500),
            sweep_every: Duration::from_millis(20),
            lookups_per_sec: 1e6,
            burst: 1e6,
            ..Options::default()
        },
    );
    for i in 0..50 {
        c.resolve(&format!("did:plc:s{i}")).await.unwrap();
    }
    assert_eq!(c.len(), 50);
    tokio::time::sleep(Duration::from_millis(600)).await;
    // one store sweeps the 50 that expired, far below capacity
    c.resolve("did:plc:s0").await.unwrap();
    assert_eq!(c.len(), 1);
}

/// Any host can send `#identity` for any DID: a stream of them for one DID
/// costs one fetch per `min_refresh`, not one each.
#[tokio::test]
async fn forced_refreshes_are_coalesced_per_did() {
    let m = Arc::new(Mock::default());
    let k = Signer::new(Curve::K256, 1).multibase();
    m.docs.lock().insert("did:plc:abc".into(), doc("did:plc:abc", &k, "https://pds.example.com"));
    let c = cache(&m, Options { min_refresh: Duration::from_millis(300), ..Options::default() });
    c.resolve("did:plc:abc").await.unwrap();
    // a plain lookup isn't a refresh: the first #identity still fetches
    c.refresh("did:plc:abc").await.unwrap();
    assert_eq!(m.calls.load(Ordering::SeqCst), 2);
    for _ in 0..20 {
        c.refresh("did:plc:abc").await.unwrap();
    }
    assert_eq!(m.calls.load(Ordering::SeqCst), 2);
    assert_eq!(c.stats.refresh_coalesced.load(Ordering::Relaxed), 20);
    tokio::time::sleep(Duration::from_millis(350)).await;
    c.refresh("did:plc:abc").await.unwrap();
    assert_eq!(m.calls.load(Ordering::SeqCst), 3);
}
