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
                seen.extend(hosts);
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
        let j = DiscoveryJob::new(engine.clone(), crawler, store.clone(), Arc::new(Feed::default()));
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
    let j = DiscoveryJob::new(engine, crawler.clone(), store, feed.clone());
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
