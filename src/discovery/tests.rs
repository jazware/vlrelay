use super::*;
use crate::upstream::{CrawlPolicy, Manager, MemHostStore, UpstreamConfig};
use std::collections::HashMap;

use crate::plc_seed::tests::{export, fleet};

/// A fake relay's listHosts: `known` hosts this relay has and `extra`
/// that don't answer, `page` a page, a 429 every `throttle` requests.
async fn fake(extra: u64, page: u64, throttle: u64) -> (Arc<export::FakePlc>, String) {
    let layout = fleet::Layout::new("discovery-test", "http://127.0.0.1", 42000);
    let plc = export::FakePlc::new(layout, 0, 0, export::now_ms(), 1);
    plc.list_extra.store(extra, Relaxed);
    plc.list_page.store(page, Relaxed);
    plc.throttle_every.store(throttle, Relaxed);
    let lis = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let url = format!("http://{}", lis.local_addr().unwrap());
    tokio::spawn(axum::serve(lis, plc.router(None)).into_future());
    (plc, url)
}

/// Pages until the cursor runs out, waiting out the 429s, every host once.
#[tokio::test]
async fn list_hosts_pages_through_and_reports_429s() {
    let (plc, url) = fake(23, 5, 3).await;
    let http = reqwest::Client::new();
    let (mut cursor, mut seen, mut later) = (None, Vec::new(), 0);
    loop {
        match list_hosts(&http, &url, cursor.as_deref()).await.unwrap() {
            Page::Later(d) => {
                assert_eq!(d, Duration::from_secs(1));
                later += 1;
            }
            Page::Hosts { hosts, cursor: next } => {
                seen.extend(hosts.into_iter().map(|h| h.hostname));
                cursor = next;
                if cursor.is_none() {
                    break;
                }
            }
        }
    }
    let unique: BTreeSet<&String> = seen.iter().collect();
    assert_eq!((seen.len(), unique.len()), (23, 23));
    assert!(later >= 2 && plc.throttled.load(Relaxed) == later, "{later}");
}

async fn policy(engine: &Engine, url: &str) {
    let cur = engine.snapshot().policy.clone();
    let mut body = cur.body.clone();
    body.discovery = Discovery {
        seed_relays: vec![crate::policy::doc::SeedRelay {
            url: url.into(),
            enabled: true,
            refresh_interval_secs: 3600,
        }],
        plc: false,
        connects_per_min: 6_000.0,
        requests_per_sec: 4.0,
        aliases: true,
        ..Default::default()
    };
    engine.save_policy(cur.version, body, "test", "").await.unwrap();
}

/// The job on a three-node log: the leader reads a seed relay's list,
/// the hosts it has are counted known and the rest go through
/// admission (refused here: they don't answer); the leader dies
/// mid-list and the new one resumes from the saved cursor, so the run
/// finishes with every host seen about once.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_new_leader_resumes_discovery_mid_list() {
    use crate::qlog::tests::Cluster;
    const EXTRA: u64 = 60;
    const PAGE: u64 = 5;
    let (plc, url) = fake(EXTRA, PAGE, 4).await;
    let mut c = Cluster::with_cfg(3, None, None, 64 << 20).await;
    let store = crate::qlog::bucket::counted(&c.store, "discovery");
    let engine = Engine::new(c.store.clone(), "test", Arc::new(crate::policy::budget::FixedNodes::new(1)));
    policy(&engine, &url).await;
    let mut jobs: HashMap<String, Arc<DiscoveryJob>> = HashMap::new();
    let start = |c: &Cluster, id: &str| {
        let (m, _rx) = Manager::new(UpstreamConfig::new(true), Arc::new(MemHostStore::default()), None);
        let crawler = Crawler::new(m.clone(), CrawlPolicy { probe_timeout_secs: 1, ..Default::default() });
        let j = DiscoveryJob::new(engine.clone(), crawler, None, store.clone(), Arc::new(Feed::default()));
        tokio::spawn(j.clone().run(Arc::downgrade(&c.nodes[id].node)));
        (j, m)
    };
    let ids: Vec<String> = c.nodes.keys().cloned().collect();
    let mut managers = HashMap::new();
    for id in &ids {
        let (j, m) = start(&c, id);
        // every node already has the first five
        for i in 0..5 {
            m.admit(&crate::types::Host(format!("gone-{i}.fakepds.invalid")), crate::upstream::host::Tier::New)
                .await
                .unwrap();
        }
        jobs.insert(id.clone(), j);
        managers.insert(id.clone(), m);
    }
    let l = c.wait_leader(Duration::from_secs(5)).await;
    let t = Instant::now();
    while jobs[&l].view().sources[0].state.pages < 4 {
        assert!(t.elapsed() < Duration::from_secs(30), "no pages: {:?}", jobs[&l].view());
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
    jobs[&l].stop();
    c.kill(&l);
    let l2 = c.wait_leader(Duration::from_secs(5)).await;
    let t = Instant::now();
    let s = loop {
        let v = jobs[&l2].view();
        let s = v.sources[0].clone();
        if s.state.last_finished_ms.is_some() && !s.state.in_progress {
            break s;
        }
        assert!(t.elapsed() < Duration::from_secs(60), "the run didn't finish: {v:?}");
        tokio::time::sleep(Duration::from_millis(50)).await;
    };
    eprintln!("{s:?}");
    // the view goes over the peer protocol and the admin API as JSON
    let v = jobs[&l2].view();
    let back: crate::admin::DiscoveryView = serde_json::from_slice(&serde_json::to_vec(&v).unwrap()).unwrap();
    assert_eq!(back.sources[0].state.url.as_deref(), Some(url.as_str()));
    assert!(s.state.resumed >= 1, "the new leader started over: {s:?}");
    assert!(s.state.hosts_seen >= EXTRA && s.state.hosts_seen <= EXTRA + PAGE, "{s:?}");
    assert_eq!(s.state.known, 5, "{s:?}");
    assert_eq!(s.state.admitted, 0);
    assert!(s.state.refused >= EXTRA - 5 && s.state.refused <= EXTRA - 5 + PAGE, "{s:?}");
    assert!(s.state.throttled >= 1, "{s:?}");
    assert!(plc.list_requests.load(Relaxed) < 2 * EXTRA.div_ceil(PAGE) + 10, "re-read from the start");
    for j in jobs.values() {
        j.stop();
    }
    c.shutdown();
}

/// With `plc` on, the PDS hosts the export reader feeds go through the
/// same admission under source `plc`, recorded in the admission log; the
/// ones the relay has are dropped before they queue.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn plc_hosts_go_through_admission_as_plc() {
    use crate::qlog::tests::Cluster;
    let c = Cluster::with_cfg(1, None, None, 64 << 20).await;
    let engine = Engine::new(c.store.clone(), "test", Arc::new(crate::policy::budget::FixedNodes::new(1)));
    let cur = engine.snapshot().policy.clone();
    let mut body = cur.body.clone();
    body.discovery.plc = true;
    body.discovery.connects_per_min = 6_000.0;
    engine.save_policy(cur.version, body, "test", "").await.unwrap();
    let (m, _rx) = Manager::new(UpstreamConfig::new(true), Arc::new(MemHostStore::default()), None);
    m.admit(&crate::types::Host("known.fakepds.invalid".into()), crate::upstream::host::Tier::New).await.unwrap();
    let crawler = Crawler::new(m, CrawlPolicy { probe_timeout_secs: 1, ..Default::default() });
    let feed = Arc::new(Feed::default());
    let store = crate::qlog::bucket::counted(&c.store, "discovery");
    let j = DiscoveryJob::new(engine, crawler.clone(), None, store, feed.clone());
    let id = c.nodes.keys().next().unwrap().clone();
    tokio::spawn(j.clone().run(Arc::downgrade(&c.nodes[&id].node)));
    for h in ["known.fakepds.invalid", "a.fakepds.invalid", "b.fakepds.invalid", "a.fakepds.invalid"] {
        feed.push(h);
    }
    let t = Instant::now();
    let s = loop {
        let v = j.view();
        let s = v.sources.iter().find(|s| s.key == PLC_SOURCE).unwrap().clone();
        if s.state.refused == 2 {
            break s;
        }
        assert!(t.elapsed() < Duration::from_secs(20), "{v:?}");
        tokio::time::sleep(Duration::from_millis(50)).await;
    };
    assert!(s.enabled && s.pending == 0, "{s:?}");
    let log = crawler.admissions();
    assert!(log.iter().all(|a| a.source == "plc") && log.len() == 2, "{log:?}");
    j.stop();
    c.shutdown();
}

/// A seed relay's listHosts, one page: (hostname, accountCount, status).
async fn seed_relay(rows: &'static [(&'static str, i64, &'static str)]) -> String {
    let body = serde_json::json!({
        "hosts": rows.iter().map(|(h, n, st)| serde_json::json!({"hostname": h, "accountCount": n, "status": st, "seq": 1})).collect::<Vec<_>>(),
    });
    let app = axum::Router::new().route(
        "/xrpc/com.atproto.sync.listHosts",
        axum::routing::get(move || std::future::ready(axum::Json(body.clone()))),
    );
    let lis = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let url = format!("http://{}", lis.local_addr().unwrap());
    tokio::spawn(axum::serve(lis, app).into_future());
    url
}

/// A host a seed relay lists with 50,000 accounts gets limits for them from
/// the first run, before this relay has seen any of its accounts. What the
/// seed relay throttled or banned, a trusted host, a host admission
/// refused, and anything past `seedAccounts.max` aren't seeded (or are
/// capped). The count is on the host record, so a restarted engine reading
/// it back applies it.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_seed_relays_account_count_seeds_a_hosts_limits() {
    use crate::policy::tiers;
    use crate::qlog::tests::Cluster;
    use crate::state::{HostRecord, HostStore, Tier};
    const ROWS: &[(&str, i64, &str)] = &[
        ("big.fakepds.invalid", 50_000, "active"),
        ("huge.fakepds.invalid", 5_000_000, "active"),
        ("spammy.fakepds.invalid", 40_000, "throttled"),
        ("trusted.fakepds.invalid", 900_000, "active"),
        ("small.fakepds.invalid", 0, "active"),
        ("refused.fakepds.invalid", 30_000, "active"),
    ];
    let url = seed_relay(ROWS).await;
    let c = Cluster::with_cfg(1, None, None, 64 << 20).await;
    let engine = Engine::new(c.store.clone(), "test", Arc::new(crate::policy::budget::FixedNodes::new(1)));
    policy(&engine, &url).await;
    let (m, _rx) = Manager::new(UpstreamConfig::new(true), Arc::new(MemHostStore::default()), None);
    let hosts = Arc::new(crate::state::tests::MemHosts::default());
    let now = crate::state::now_secs();
    for (h, _, _) in &ROWS[..5] {
        let tier = if h.starts_with("trusted") { Tier::Trusted } else { Tier::New };
        m.admit(&crate::types::Host(h.to_string()), crate::upstream::host::Tier::New).await.unwrap();
        hosts.put_host(&HostRecord::new(h, tier, now)).await.unwrap();
    }
    let before = engine.for_host(&hosts.get_host("big.fakepds.invalid").await.unwrap().unwrap());
    assert_eq!(before.limits.unwrap().events_per_hour, 3_500);
    let crawler = Crawler::new(m, CrawlPolicy { probe_timeout_secs: 1, ..Default::default() });
    let store = crate::qlog::bucket::counted(&c.store, "discovery");
    let dyn_hosts: Arc<dyn HostStore> = hosts.clone();
    let j = DiscoveryJob::new(engine.clone(), crawler, Some(dyn_hosts), store, Arc::new(Feed::default()));
    let id = c.nodes.keys().next().unwrap().clone();
    tokio::spawn(j.clone().run(Arc::downgrade(&c.nodes[&id].node)));
    let t = Instant::now();
    let s = loop {
        let v = j.view();
        let s = v.sources[0].state.clone();
        if s.last_finished_ms.is_some() && !s.in_progress {
            break s;
        }
        assert!(t.elapsed() < Duration::from_secs(20), "{v:?}");
        tokio::time::sleep(Duration::from_millis(50)).await;
    };
    j.stop();
    assert_eq!((s.known, s.refused, s.seeded), (5, 1, 2), "{s:?}");
    let seed = |h: &str| {
        let hosts = hosts.clone();
        let h = h.to_string();
        async move { hosts.get_host(&h).await.unwrap().map(|r| tiers::host_policy(&r).seeded) }
    };
    let big = seed("big.fakepds.invalid").await.unwrap().unwrap();
    assert_eq!((big.accounts, big.from.as_str()), (50_000, crate::discovery::source_key(&url).as_str()));
    assert_eq!(seed("huge.fakepds.invalid").await.unwrap().unwrap().accounts, 5_000_000);
    for h in ["spammy.fakepds.invalid", "trusted.fakepds.invalid", "small.fakepds.invalid"] {
        assert_eq!(seed(h).await.unwrap(), None, "{h}");
    }
    assert_eq!(seed("refused.fakepds.invalid").await, None, "no record");

    // a restart: a new engine over the same bucket, the records as stored
    let engine2 = Engine::new(c.store.clone(), "test", Arc::new(crate::policy::budget::FixedNodes::new(1)));
    engine2.refresh().await.unwrap();
    let l = engine2.for_host(&hosts.get_host("big.fakepds.invalid").await.unwrap().unwrap());
    assert_eq!(l.seeded_accounts, Some(50_000));
    let lim = l.limits.unwrap();
    // ~8 events/s, a busy 50k-account PDS, fits every window from the start
    assert!(
        lim.events_per_sec >= 8.0 && lim.events_per_hour >= 8 * 3_600 && lim.events_per_day >= 8 * 86_400,
        "{lim:?}"
    );
    assert_eq!(lim.max_accounts, 201_000);
    let huge = engine2.for_host(&hosts.get_host("huge.fakepds.invalid").await.unwrap().unwrap());
    assert_eq!(huge.limits.unwrap().max_accounts, 1_001_000, "capped at seedAccounts.max");
    c.shutdown();
}
