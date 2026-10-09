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
    seed_relay_paged(rows, rows.len().max(1)).await
}

/// A seed relay's listHosts, `page` rows a page, the cursor an offset.
async fn seed_relay_paged(rows: &'static [(&'static str, i64, &'static str)], page: usize) -> String {
    #[derive(Deserialize)]
    struct Q {
        cursor: Option<String>,
    }
    let list = move |axum::extract::Query(q): axum::extract::Query<Q>| {
        let at: usize = q.cursor.and_then(|c| c.parse().ok()).unwrap_or(0);
        let end = (at + page).min(rows.len());
        let hosts: Vec<_> = rows[at..end]
            .iter()
            .map(|(h, n, st)| serde_json::json!({"hostname": h, "accountCount": n, "status": st, "seq": 1}))
            .collect();
        let mut body = serde_json::json!({ "hosts": hosts });
        if end < rows.len() {
            body["cursor"] = end.to_string().into();
        }
        std::future::ready(axum::Json(body))
    };
    let app = axum::Router::new().route("/xrpc/com.atproto.sync.listHosts", axum::routing::get(list));
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

/// Admission by the policy engine, stood in for: refuses `banned-*` hosts
/// as a ban would, and records the order (and time) hosts reach it.
#[derive(Default)]
struct Recording {
    seen: Mutex<Vec<(String, Instant)>>,
}

#[async_trait::async_trait]
impl crate::upstream::crawl::Admission for Recording {
    async fn admit(&self, host: &crate::types::Host, spend: bool) -> Result<crate::upstream::host::Tier, CrawlError> {
        if !spend {
            self.seen.lock().push((host.0.clone(), Instant::now()));
        }
        if host.0.starts_with("banned-") {
            return Err(CrawlError::HostBanned);
        }
        Ok(crate::upstream::host::Tier::New)
    }
}

async fn wait_finished(j: &DiscoveryJob, runs: u64, secs: u64) -> SourceState {
    let t = Instant::now();
    loop {
        let v = j.view();
        let s = v.sources[0].state.clone();
        if s.runs >= runs && s.last_finished_ms.is_some() && !s.in_progress && !s.run_requested {
            return s;
        }
        assert!(t.elapsed() < Duration::from_secs(secs), "{v:?}");
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
}

/// A seed relay lists its hosts over several pages in no order of size.
/// The run reads them all, then admits the new ones largest first across
/// pages, the uncounted ones (no count, or a status the relay doesn't
/// vouch for) after them in list order, at `connectsPerMin`. The biggest
/// host is banned and still refused; a suspended one this relay has is
/// counted known and never goes through admission.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn discovery_admits_the_largest_seeded_hosts_first() {
    use crate::qlog::tests::Cluster;
    const ROWS: &[(&str, i64, &str)] = &[
        ("u1.fakepds.invalid", 0, "active"),
        ("c300.fakepds.invalid", 300, "active"),
        ("c10.fakepds.invalid", 10, "active"),
        ("suspended-big.fakepds.invalid", 9_000_000, "active"),
        ("u2.fakepds.invalid", 0, "active"),
        ("c50000.fakepds.invalid", 50_000, "active"),
        ("t1.fakepds.invalid", 70_000, "throttled"),
        ("c20.fakepds.invalid", 20, "active"),
        ("banned-big.fakepds.invalid", 5_000_000, "active"),
        ("c4000.fakepds.invalid", 4_000, "idle"),
        ("u3.fakepds.invalid", 0, "active"),
        ("c1.fakepds.invalid", 1, "active"),
        ("c120000.fakepds.invalid", 120_000, "active"),
        ("c300.fakepds.invalid", 300, "active"),
        ("c2.fakepds.invalid", 2, "active"),
        ("u4.fakepds.invalid", 0, "active"),
        ("c800.fakepds.invalid", 800, "active"),
    ];
    const EXPECT: &[&str] = &[
        "banned-big",
        "c120000",
        "c50000",
        "c4000",
        "c800",
        "c300",
        "c20",
        "c10",
        "c2",
        "c1",
        "u1",
        "u2",
        "t1",
        "u3",
        "u4",
    ];
    const PER_MIN: f64 = 1_200.0;
    let url = seed_relay_paged(ROWS, 4).await;
    let c = Cluster::with_cfg(1, None, None, 64 << 20).await;
    let engine = Engine::new(c.store.clone(), "test", Arc::new(crate::policy::budget::FixedNodes::new(1)));
    let cur = engine.snapshot().policy.clone();
    let mut body = cur.body.clone();
    body.discovery = Discovery {
        seed_relays: vec![crate::policy::doc::SeedRelay {
            url: url.clone(),
            enabled: true,
            refresh_interval_secs: 3600,
        }],
        connects_per_min: PER_MIN,
        requests_per_sec: 50.0,
        ..Default::default()
    };
    engine.save_policy(cur.version, body, "test", "").await.unwrap();
    let (m, _rx) = Manager::new(UpstreamConfig::new(true), Arc::new(MemHostStore::default()), None);
    m.admit(&crate::types::Host("suspended-big.fakepds.invalid".into()), crate::upstream::host::Tier::Suspended)
        .await
        .unwrap();
    let crawler = Crawler::new(m.clone(), CrawlPolicy { probe_timeout_secs: 1, ..Default::default() });
    let rec = Arc::new(Recording::default());
    crawler.set_admission(rec.clone());
    let store = crate::qlog::bucket::counted(&c.store, "discovery");
    let t0 = Instant::now();
    let j = DiscoveryJob::new(engine, crawler.clone(), None, store, Arc::new(Feed::default()));
    let id = c.nodes.keys().next().unwrap().clone();
    tokio::spawn(j.clone().run(Arc::downgrade(&c.nodes[&id].node)));
    let s = wait_finished(&j, 1, 30).await;
    j.stop();

    let seen = rec.seen.lock().clone();
    let order: Vec<&str> = seen.iter().map(|(h, _)| h.trim_end_matches(".fakepds.invalid")).collect();
    assert_eq!(order, EXPECT, "admission order");
    assert_eq!((s.pages, s.hosts_seen, s.known, s.new), (5, 17, 1, EXPECT.len() as u64), "{s:?}");
    assert_eq!((s.admitted, s.refused), (0, EXPECT.len() as u64), "{s:?}");
    // the pace's bucket starts empty when the job does
    let last = seen.last().unwrap().1.duration_since(t0).as_secs_f64();
    let floor = (EXPECT.len() - 1) as f64 / (PER_MIN / 60.0);
    assert!(last >= floor * 0.95, "{} admissions in {last:.2}s, under the pace's {floor:.2}s", EXPECT.len());
    let log = crawler.admissions();
    let banned = log.iter().find(|a| a.host == "banned-big.fakepds.invalid").unwrap();
    assert_eq!(banned.outcome, "banned", "{banned:?}");
    assert!(log.iter().all(|a| a.host != "suspended-big.fakepds.invalid"), "{log:?}");
    let sus = m.registry().get(&crate::types::Host("suspended-big.fakepds.invalid".into())).unwrap();
    assert_eq!(sus.tier(), crate::upstream::host::Tier::Suspended);
    assert!(m.registry().get(&crate::types::Host("banned-big.fakepds.invalid".into())).is_none());
    c.shutdown();
}

/// A node restarted into a build that seeds, with the stored state of a run
/// from before seeding (finished 10 minutes ago, its refresh not due for most of an hour),
/// runs the source again on its first term, so the counts are on the host
/// records now. Only once: the run that seeds settles it.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_run_from_before_seeding_is_run_again_on_the_first_term() {
    use crate::qlog::tests::Cluster;
    use crate::state::{HostRecord, HostStore, Tier};
    const ROWS: &[(&str, i64, &str)] =
        &[("big.fakepds.invalid", 50_000, "active"), ("small.fakepds.invalid", 30, "active")];
    let url = seed_relay(ROWS).await;
    let c = Cluster::with_cfg(1, None, None, 64 << 20).await;
    let engine = Engine::new(c.store.clone(), "test", Arc::new(crate::policy::budget::FixedNodes::new(1)));
    policy(&engine, &url).await;
    let (m, _rx) = Manager::new(UpstreamConfig::new(true), Arc::new(MemHostStore::default()), None);
    let hosts = Arc::new(crate::state::tests::MemHosts::default());
    let now = crate::state::now_secs();
    for (h, _, _) in ROWS {
        m.admit(&crate::types::Host(h.to_string()), crate::upstream::host::Tier::New).await.unwrap();
        hosts.put_host(&HostRecord::new(h, Tier::New, now)).await.unwrap();
    }
    let crawler = Crawler::new(m, CrawlPolicy { probe_timeout_secs: 1, ..Default::default() });
    let store = crate::qlog::bucket::counted(&c.store, "discovery");
    let dyn_hosts: Arc<dyn HostStore> = hosts.clone();
    let j = DiscoveryJob::new(engine, crawler, Some(dyn_hosts), store, Arc::new(Feed::default()));
    // what the earlier build saved: no seedPass, nothing seeded
    let finished = crate::policy::store::now_ms() - 600_000;
    let old = serde_json::json!({"sources": {source_key(&url): {
        "url": url, "runs": 1, "lastStartedMs": finished - 60_000, "lastFinishedMs": finished,
        "hostsSeen": 2, "new": 2, "admitted": 2, "pages": 1,
    }}});
    j.save(&serde_json::from_value(old).unwrap()).await.unwrap();
    let id = c.nodes.keys().next().unwrap().clone();
    tokio::spawn(j.clone().run(Arc::downgrade(&c.nodes[&id].node)));
    let s = wait_finished(&j, 2, 20).await;
    assert!(s.seed_pass && s.seeded == 2 && s.known == 2, "{s:?}");
    let rec = hosts.get_host("big.fakepds.invalid").await.unwrap().unwrap();
    assert_eq!(crate::policy::tiers::host_policy(&rec).seeded.unwrap().accounts, 50_000);
    tokio::time::sleep(Duration::from_millis(1_500)).await;
    let s = j.view().sources[0].state.clone();
    assert!(s.runs == 2 && !s.in_progress && !s.run_requested, "ran again: {s:?}");
    j.stop();
    c.shutdown();
}

/// A run asked of every source marks only the enabled ones: a disabled seed
/// relay and the PLC source with `plc` off would show runRequested forever,
/// as a request stored before would. An enabled one shows it at once.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_requested_run_marks_only_enabled_sources() {
    use crate::qlog::tests::Cluster;
    const ROWS: &[(&str, i64, &str)] = &[("a.fakepds.invalid", 5, "active")];
    let on = seed_relay(ROWS).await;
    let off = seed_relay(ROWS).await;
    let c = Cluster::with_cfg(1, None, None, 64 << 20).await;
    let engine = Engine::new(c.store.clone(), "test", Arc::new(crate::policy::budget::FixedNodes::new(1)));
    let cur = engine.snapshot().policy.clone();
    let mut body = cur.body.clone();
    let relay =
        |url: &str, enabled| crate::policy::doc::SeedRelay { url: url.into(), enabled, refresh_interval_secs: 3600 };
    body.discovery = Discovery {
        seed_relays: vec![relay(&on, true), relay(&off, false)],
        plc: false,
        connects_per_min: 6_000.0,
        requests_per_sec: 50.0,
        ..Default::default()
    };
    engine.save_policy(cur.version, body, "test", "").await.unwrap();
    let (m, _rx) = Manager::new(UpstreamConfig::new(true), Arc::new(MemHostStore::default()), None);
    let crawler = Crawler::new(m, CrawlPolicy { probe_timeout_secs: 1, ..Default::default() });
    let store = crate::qlog::bucket::counted(&c.store, "discovery");
    let j = DiscoveryJob::new(engine, crawler, None, store, Arc::new(Feed::default()));
    let stale = serde_json::json!({"sources": {
        source_key(&off): {"runRequested": true},
        PLC_SOURCE: {"runRequested": true},
    }});
    j.save(&serde_json::from_value(stale).unwrap()).await.unwrap();
    let id = c.nodes.keys().next().unwrap().clone();
    tokio::spawn(j.clone().run(Arc::downgrade(&c.nodes[&id].node)));
    wait_finished(&j, 1, 20).await;
    j.request(None);
    let v = j.view();
    let s = &v.sources[0].state;
    assert!(s.run_requested || s.in_progress || s.runs >= 2, "the request didn't show: {v:?}");
    wait_finished(&j, 2, 20).await;
    let v = j.view();
    for src in &v.sources[1..] {
        assert!(!src.enabled && !src.state.run_requested, "{src:?}");
    }
    j.stop();
    c.shutdown();
}
