//! Upstream subscriptions against real upstreams: an in-process vlpds for
//! real sync 1.1 frames, and a synthetic fan for rates, stalls and errors.

mod fan;
mod pds;
mod scale;

use fan::{Fan, HostSpec};
use pds::Pds;
use std::collections::HashMap;
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::sync::mpsc;
use vlrelay::types::{Host, UpstreamFrame};
use vlrelay::upstream::{
    CrawlPolicy, Crawler, DomainAction, DomainRule, HostRecord, HostStatus, HostStore, Limits, Manager, MemHostStore,
    Tier, TierLimits, UpstreamConfig,
};

fn dev_config() -> UpstreamConfig {
    let mut c = UpstreamConfig::new(true);
    c.endpoint = Arc::new(|h: &Host| format!("http://{}", h.0));
    c.backoff_base = Duration::from_millis(50);
    c.backoff_max = Duration::from_millis(400);
    c.connect_timeout = Duration::from_secs(2);
    c.flush_interval = Duration::from_millis(100);
    c
}

/// Routes `*.fan.test` to the fan, anything else to `http://{host}`.
fn fan_config(fan: &Arc<Fan>) -> UpstreamConfig {
    let mut c = dev_config();
    let f = fan.clone();
    c.endpoint = Arc::new(move |h: &Host| match h.0.strip_suffix(".fan.test") {
        Some(name) => f.url(name),
        None => format!("http://{}", h.0),
    });
    c
}

/// A store holding `host` at `tier` with an acked cursor.
async fn seeded(host: &Host, tier: Tier, acked: Option<i64>) -> Arc<MemHostStore> {
    let store = Arc::new(MemHostStore::default());
    let mut r = HostRecord::new(host, tier);
    r.acked_seq = acked;
    store.put(vec![r]).await.unwrap();
    store
}

async fn wait_for(what: &str, timeout: Duration, mut f: impl FnMut() -> bool) {
    let t = Instant::now();
    while !f() {
        assert!(t.elapsed() < timeout, "timed out waiting for {what}");
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
}

/// Frames until none arrive for `quiet`.
async fn drain(rx: &mut mpsc::Receiver<UpstreamFrame>, quiet: Duration) -> Vec<UpstreamFrame> {
    let mut out = Vec::new();
    while let Ok(Some(f)) = tokio::time::timeout(quiet, rx.recv()).await {
        out.push(f);
    }
    out
}

/// Frames for `d`, for streams that never go quiet.
async fn collect_for(rx: &mut mpsc::Receiver<UpstreamFrame>, d: Duration) -> Vec<UpstreamFrame> {
    let mut out = Vec::new();
    let end = tokio::time::Instant::now() + d;
    while let Ok(Some(f)) = tokio::time::timeout_at(end, rx.recv()).await {
        out.push(f);
    }
    out
}

fn seqs(frames: &[UpstreamFrame]) -> Vec<i64> {
    frames.iter().map(|f| f.upstream_seq).collect()
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn subscribes_to_vlpds() {
    let pds = Pds::spawn().await;
    let alice = pds.create_account().await;
    for i in 0..5 {
        pds.post_text(&alice, &format!("post {i}")).await;
    }
    let host = Host(pds.hostname());
    let store = seeded(&host, Tier::Default, Some(0)).await;
    let (m, mut rx) = Manager::new(dev_config(), store.clone(), None);
    m.start().await.unwrap();

    let want = pds.seqs_after(0).await;
    assert!(want.len() >= 6, "account + 5 posts: {want:?}");
    let got = drain(&mut rx, Duration::from_millis(800)).await;
    assert_eq!(seqs(&got), want);
    assert!(got.iter().all(|f| f.host == host));
    let kinds: Vec<_> = got
        .iter()
        .map(|f| match vlrelay::upstream::frame::peek(&f.frame).unwrap() {
            vlrelay::upstream::frame::Peek::Message { t, .. } => t.to_string(),
            p => panic!("{p:?}"),
        })
        .collect();
    assert!(kinds.iter().filter(|t| *t == "#commit").count() >= 5, "{kinds:?}");

    // live events keep coming on the same socket
    pds.post_text(&alice, "live").await;
    let live = drain(&mut rx, Duration::from_millis(500)).await;
    assert_eq!(seqs(&live), pds.seqs_after(*want.last().unwrap()).await);
    let v = m.host(&host).unwrap();
    assert_eq!((v.connects, v.record.status), (1, HostStatus::Active));
    assert_eq!(v.received_seq, live.last().map(|f| f.upstream_seq));
    // received isn't acked until the relay says so
    assert_eq!(v.record.acked_seq, Some(0));
    m.shutdown().await.unwrap();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn resumes_from_acked_cursor() {
    let pds = Pds::spawn().await;
    let alice = pds.create_account().await;
    let host = Host(pds.hostname());
    let store = seeded(&host, Tier::Default, Some(0)).await;
    let (m, mut rx) = Manager::new(dev_config(), store.clone(), None);
    m.start().await.unwrap();
    let mut delivered = drain(&mut rx, Duration::from_millis(500)).await;
    let first = seqs(&delivered);
    m.ack(&host, *first.last().unwrap());

    // everything acked: a reconnect replays nothing and misses nothing
    m.kick(&host);
    wait_for("reconnect", Duration::from_secs(5), || {
        m.host(&host).is_some_and(|v| v.connects == 2 && v.record.status == HostStatus::Active)
    })
    .await;
    for i in 0..5 {
        pds.post_text(&alice, &format!("a{i}")).await;
    }
    let second = drain(&mut rx, Duration::from_millis(500)).await;
    assert_eq!(seqs(&second), pds.seqs_after(*first.last().unwrap()).await);
    assert_eq!(second.len(), 5);
    delivered.extend(second.iter().cloned());

    // ack only the first two of those: the other three come again, once
    let acked = second[1].upstream_seq;
    m.ack(&host, acked);
    m.kick(&host);
    wait_for("second reconnect", Duration::from_secs(5), || m.host(&host).is_some_and(|v| v.connects == 3)).await;
    for i in 0..3 {
        pds.post_text(&alice, &format!("b{i}")).await;
    }
    let third = drain(&mut rx, Duration::from_millis(500)).await;
    let want = pds.seqs_after(acked).await;
    assert_eq!(seqs(&third), want, "resume from the acked cursor exactly");
    assert_eq!(want.len(), 6);
    for f in &third {
        m.ack(&host, f.upstream_seq);
    }
    // durable, so a fresh manager on the same store resumes after it
    m.shutdown().await.unwrap();
    assert_eq!(store.get(&host.0).unwrap().acked_seq, Some(*want.last().unwrap()));

    let (m2, mut rx2) = Manager::new(dev_config(), store.clone(), None);
    m2.start().await.unwrap();
    pds.post_text(&alice, "after restart").await;
    let fourth = drain(&mut rx2, Duration::from_millis(500)).await;
    // vlpds seqs aren't dense, so compare with what the PDS has after it
    let after = pds.seqs_after(*want.last().unwrap()).await;
    assert_eq!(after.len(), 1);
    assert_eq!(seqs(&fourth), after);
    m2.shutdown().await.unwrap();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn backs_off_a_dead_host() {
    let l = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
    let host = Host(l.local_addr().unwrap().to_string());
    drop(l);
    let (m, _rx) = Manager::new(dev_config(), Arc::new(MemHostStore::default()), None);
    m.start().await.unwrap();
    m.admit(&host, Tier::New).await.unwrap();
    tokio::time::sleep(Duration::from_millis(2500)).await;
    let v = m.host(&host).unwrap();
    // 50 ms doubling to a 400 ms cap, half jittered: 25-50, 50-100,
    // 100-200, then 200-400 each, so 7-14 attempts in 2.5 s
    let n = v.record.errors.connect;
    assert!((6..=16).contains(&n), "connect attempts: {n}");
    assert_eq!(v.record.status, HostStatus::Backoff);
    assert_eq!(v.connects, 0);

    // a wake (requestCrawl for a known host) cuts the backoff short
    let before = m.host(&host).unwrap().record.errors.connect;
    m.wake(&host);
    tokio::time::sleep(Duration::from_millis(20)).await;
    assert!(m.host(&host).unwrap().record.errors.connect > before);
    m.shutdown().await.unwrap();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn detects_a_stall() {
    let fan = Fan::spawn().await;
    fan.set("deaf", HostSpec { deaf: true, ..HostSpec::rate(0.0, 0) });
    fan.set("quiet", HostSpec::rate(0.0, 0));
    let mut cfg = fan_config(&fan);
    cfg.ping_interval = Duration::from_millis(100);
    cfg.stall_timeout = Duration::from_millis(500);
    let (m, _rx) = Manager::new(cfg, Arc::new(MemHostStore::default()), None);
    m.start().await.unwrap();
    let deaf = Host("deaf.fan.test".into());
    let quiet = Host("quiet.fan.test".into());
    m.admit(&deaf, Tier::Default).await.unwrap();
    m.admit(&quiet, Tier::Default).await.unwrap();
    tokio::time::sleep(Duration::from_millis(2000)).await;
    let d = m.host(&deaf).unwrap();
    assert!(d.record.errors.stalls >= 2, "{:?}", d.record.errors);
    assert!(d.connects >= 2 && fan.connects("deaf") >= 2);
    // silent but answering pings is fine
    let q = m.host(&quiet).unwrap();
    assert_eq!((q.record.errors.stalls, q.connects), (0, 1));
    m.shutdown().await.unwrap();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn future_and_outdated_cursors() {
    let fan = Fan::spawn().await;
    fan.set("reset", HostSpec { max_seq: Some(10), ..HostSpec::rate(200.0, 10) });
    fan.set("pruned", HostSpec { min_seq: Some(500), ..HostSpec::rate(200.0, 10) });
    let reset = Host("reset.fan.test".into());
    let pruned = Host("pruned.fan.test".into());
    let store = seeded(&reset, Tier::Default, Some(1000)).await;
    store.put(vec![HostRecord { acked_seq: Some(7), ..HostRecord::new(&pruned, Tier::Default) }]).await.unwrap();
    let (m, mut rx) = Manager::new(fan_config(&fan), store.clone(), None);
    m.start().await.unwrap();
    let got = collect_for(&mut rx, Duration::from_millis(800)).await;
    let by_host = |h: &Host| got.iter().filter(|f| &f.host == h).map(|f| f.upstream_seq).collect::<Vec<_>>();

    // the reset host answered 1000 with FutureCursor; we replay its new sequence from 0
    let r = m.host(&reset).unwrap();
    assert_eq!(r.record.errors.future_cursor, 1);
    assert_eq!(r.connects, 2);
    assert!(r.record.acked_seq.is_some_and(|s| (0..=10).contains(&s)), "{:?}", r.record.acked_seq);
    assert!(by_host(&reset).iter().all(|s| *s <= 10));

    // the pruned host skipped us ahead with an info frame, no reconnect
    let p = m.host(&pruned).unwrap();
    assert_eq!((p.record.errors.outdated_cursor, p.connects), (1, 1));
    let ps = by_host(&pruned);
    assert_eq!(ps.first(), Some(&500));
    assert!(ps.windows(2).all(|w| w[1] == w[0] + 1));
    m.shutdown().await.unwrap();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn rate_limit_pauses_reads() {
    let fan = Fan::spawn().await;
    fan.set("flood", HostSpec::rate(f64::INFINITY, 200));
    let mut cfg = fan_config(&fan);
    cfg.limits.new = TierLimits {
        events_per_sec: 500.0,
        bytes_per_sec: f64::INFINITY,
        burst_secs: 0.2,
        weight: 1,
        events_per_hour: 0.0,
        events_per_day: 0.0,
        reconnects_per_hour: 0.0,
    };
    let (m, mut rx) = Manager::new(cfg, Arc::new(MemHostStore::default()), None);
    m.start().await.unwrap();
    let h = Host("flood.fan.test".into());
    m.admit(&h, Tier::New).await.unwrap();
    let t = Instant::now();
    let mut n = 0;
    let mut throttled = false;
    while t.elapsed() < Duration::from_secs(2) {
        if tokio::time::timeout(Duration::from_millis(50), rx.recv()).await.is_ok() {
            n += 1;
        }
        throttled |= m.host(&h).unwrap().record.status == HostStatus::Throttled;
    }
    // 500/s for 2 s plus the 100-frame burst, a little slack for the
    // queue and channel that filled before the bucket emptied
    assert!((900..=1500).contains(&n), "delivered {n}");
    assert!(throttled);
    let v = m.host(&h).unwrap();
    assert_eq!(v.connects, 1, "a paused read isn't a stall");
    // nothing was dropped: what we didn't read is still upstream
    assert_eq!(v.received_seq, Some(v.frames as i64));

    // promoting the host lifts the limit on the same socket
    m.set_tier(&h, Tier::Trusted).await.unwrap();
    let before = v.frames;
    tokio::time::sleep(Duration::from_millis(50)).await;
    let t = Instant::now();
    let mut n2 = 0;
    while t.elapsed() < Duration::from_millis(500) {
        if rx.recv().await.is_some() {
            n2 += 1;
        }
    }
    assert!(n2 > 2000, "trusted delivered {n2} in 0.5 s (from {before})");
    m.shutdown().await.unwrap();
}

/// One host at 50x the rate of 20 others, a consumer that can't keep up
/// with the total: the 20 still get all their frames through with bounded
/// latency, and the bursting host gets the rest of the capacity.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn fair_queue_bounds_latency() {
    let fan = Fan::spawn().await;
    let small_rate = 100.0;
    fan.set("big", HostSpec::rate(small_rate * 50.0, 1000));
    let small: Vec<Host> = (0..20).map(|i| Host(format!("small{i}.fan.test"))).collect();
    for i in 0..20 {
        fan.set(&format!("small{i}"), HostSpec::rate(small_rate, 1000));
    }
    let mut cfg = fan_config(&fan);
    // every host in one tier with no rate limit: fairness alone has to do it
    cfg.limits = Limits::unlimited();
    cfg.output_capacity = 64;
    let (m, mut rx) = Manager::new(cfg, Arc::new(MemHostStore::default()), None);
    m.start().await.unwrap();
    let big = Host("big.fan.test".into());
    m.admit(&big, Tier::Default).await.unwrap();
    for h in &small {
        m.admit(h, Tier::Default).await.unwrap();
    }

    // the consumer takes 3000 frames/s against 7000/s offered
    let consume_rate = 3000.0;
    let warmup = Duration::from_millis(500);
    let run = Duration::from_secs(4);
    let t0 = Instant::now();
    let mut lat: HashMap<Host, Vec<f64>> = HashMap::new();
    let mut count: HashMap<Host, u64> = HashMap::new();
    let mut taken = 0u64;
    while t0.elapsed() < warmup + run {
        let due = (t0.elapsed().as_secs_f64() * consume_rate) as u64;
        while taken < due {
            let Ok(Some(f)) = tokio::time::timeout(Duration::from_millis(5), rx.recv()).await else { break };
            taken += 1;
            if t0.elapsed() < warmup {
                continue;
            }
            let ms = (fan::now_ns() - fan::sent_ns(&f.frame)) as f64 / 1e6;
            lat.entry(f.host.clone()).or_default().push(ms);
            *count.entry(f.host).or_default() += 1;
        }
        tokio::time::sleep(Duration::from_millis(2)).await;
    }
    let secs = run.as_secs_f64();
    let mut small_lat: Vec<f64> = small.iter().flat_map(|h| lat.get(h).cloned().unwrap_or_default()).collect();
    small_lat.sort_by(|a, b| a.partial_cmp(b).unwrap());
    let p = |q: f64| small_lat[((small_lat.len() as f64 * q) as usize).min(small_lat.len() - 1)];
    let big_rate = count.get(&big).copied().unwrap_or(0) as f64 / secs;
    let small_rates: Vec<f64> = small.iter().map(|h| count.get(h).copied().unwrap_or(0) as f64 / secs).collect();
    let min_small = small_rates.iter().cloned().fold(f64::INFINITY, f64::min);
    eprintln!(
        "fairness: small p50 {:.1} ms p99 {:.1} ms max {:.1} ms, min small rate {min_small:.0}/s, big {big_rate:.0}/s",
        p(0.5),
        p(0.99),
        p(1.0)
    );
    assert!(min_small >= small_rate * 0.9, "a small host got {min_small:.0}/s");
    assert!(p(0.99) < 100.0, "small-host p99 {:.1} ms", p(0.99));
    // the bursting host gets everything the small ones don't use
    assert!(big_rate > (consume_rate - 20.0 * small_rate) * 0.8, "big host {big_rate:.0}/s");
    // and its backlog sits upstream, not in our memory
    assert!(m.queued(&big) <= m.config().host_queue_frames);
    m.shutdown().await.unwrap();
}

/// Read-only, one socket, a few seconds: the production relay through the
/// non-dev path (wss, webpki roots, public-address check).
///   LIVE_HOST=relay1.us-east.bsky.network cargo test --test upstream live_ -- --ignored --nocapture
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[ignore]
async fn live_upstream() {
    let host = Host(std::env::var("LIVE_HOST").unwrap_or_else(|_| "relay1.us-east.bsky.network".into()));
    let (m, mut rx) = Manager::new(UpstreamConfig::new(false), Arc::new(MemHostStore::default()), None);
    m.start().await.unwrap();
    m.admit(&host, Tier::Trusted).await.unwrap();
    let t = Instant::now();
    let got = collect_for(&mut rx, Duration::from_secs(5)).await;
    let el = t.elapsed().as_secs_f64();
    let v = m.host(&host).unwrap();
    let bytes: usize = got.iter().map(|f| f.frame.len()).sum();
    eprintln!(
        "live {}: {} frames in {el:.1} s ({:.0}/s, {:.0} B avg), errors {:?}",
        host.0,
        got.len(),
        got.len() as f64 / el,
        bytes as f64 / got.len().max(1) as f64,
        v.record.errors
    );
    assert!(!got.is_empty());
    assert!(got.windows(2).all(|w| w[1].upstream_seq > w[0].upstream_seq));
    m.shutdown().await.unwrap();
}

async fn serve_crawler(c: &Arc<Crawler>) -> String {
    let l = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let url = format!("http://{}", l.local_addr().unwrap());
    let r = c.router();
    tokio::spawn(async move { axum::serve(l, r).await.unwrap() });
    url
}

async fn crawl(url: &str, hostname: &str) -> (u16, serde_json::Value) {
    let r = reqwest::Client::new()
        .post(format!("{url}/xrpc/com.atproto.sync.requestCrawl"))
        .json(&serde_json::json!({"hostname": hostname}))
        .send()
        .await
        .unwrap();
    (r.status().as_u16(), r.json().await.unwrap_or_default())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn request_crawl() {
    let pds = Pds::spawn().await;
    let fan = Fan::spawn().await;
    fan.set("second", HostSpec::rate(0.0, 0));
    fan.set("listed", HostSpec::rate(0.0, 0));
    let (m, _rx) = Manager::new(fan_config(&fan), Arc::new(MemHostStore::default()), None);
    m.start().await.unwrap();
    let policy = CrawlPolicy {
        allow: vec!["listed.fan.test".into()],
        rules: vec![DomainRule { suffix: "spam.test".into(), action: DomainAction::Ban }],
        new_hosts_per_hour: 1,
        probe_timeout_secs: 2,
        ..Default::default()
    };
    let c = Crawler::new(m.clone(), policy);
    let url = serve_crawler(&c).await;

    let (s, j) = crawl(&url, "not a host!").await;
    assert_eq!((s, j["error"].as_str()), (400, Some("InvalidRequest")));
    let (s, j) = crawl(&url, "pds7.spam.test").await;
    assert_eq!((s, j["error"].as_str()), (400, Some("HostBanned")));
    // nothing there: refused, and it doesn't spend the budget
    let (s, j) = crawl(&url, "missing.fan.test").await;
    assert_eq!((s, j["error"].as_str()), (400, Some("InvalidRequest")), "{j}");

    // a real PDS is probed and admitted at tier new
    let pds_host = Host(pds.hostname());
    let (s, j) = crawl(&url, &format!("http://{}/", pds.hostname())).await;
    assert_eq!(s, 200, "{j}");
    let v = m.host(&pds_host).unwrap();
    assert_eq!(v.record.tier, Tier::New);
    wait_for("subscribed", Duration::from_secs(5), || m.host(&pds_host).unwrap().connects == 1).await;

    // asking again is fine and doesn't spend anything
    assert_eq!(crawl(&url, &pds.hostname()).await.0, 200);
    // the hourly budget is one new host
    let (s, j) = crawl(&url, "second.fan.test").await;
    assert_eq!((s, j["error"].as_str()), (429, Some("RateLimitExceeded")));
    // the allow list skips it
    assert_eq!(crawl(&url, "listed.fan.test").await.0, 200);

    // banned by hand: refused even though it's known
    m.set_tier(&pds_host, Tier::Banned).await.unwrap();
    assert_eq!(crawl(&url, &pds.hostname()).await.1["error"], "HostBanned");
    assert_eq!(m.running(), 1);
    m.shutdown().await.unwrap();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn tiers_stop_and_start_sockets() {
    let fan = Fan::spawn().await;
    fan.set("a", HostSpec::rate(100.0, 10));
    let (m, mut rx) = Manager::new(fan_config(&fan), Arc::new(MemHostStore::default()), None);
    m.start().await.unwrap();
    let h = Host("a.fan.test".into());
    m.admit(&h, Tier::Default).await.unwrap();
    assert!(!collect_for(&mut rx, Duration::from_millis(300)).await.is_empty());
    m.set_tier(&h, Tier::Suspended).await.unwrap();
    assert_eq!(m.running(), 0);
    let _ = drain(&mut rx, Duration::from_millis(100)).await;
    assert!(drain(&mut rx, Duration::from_millis(300)).await.is_empty());
    assert_eq!(m.host(&h).unwrap().record.status, HostStatus::Idle);
    m.set_tier(&h, Tier::Default).await.unwrap();
    assert!(!collect_for(&mut rx, Duration::from_millis(300)).await.is_empty());
    assert_eq!(fan.connects("a"), 2);
    m.shutdown().await.unwrap();
}
