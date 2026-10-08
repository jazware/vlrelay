//! Upstream subscriptions against a synthetic fan of upstreams, for rates,
//! stalls and errors. Real sync 1.1 frames from an in-process vlpds are in
//! interop/tests/vlpds.

mod common;
mod fan;
mod scale;

use common::*;
use fan::{Fan, HostSpec, Stamp};
use std::collections::HashMap;
use std::sync::Arc;
use std::time::{Duration, Instant};
use vlrelay::types::Host;
use vlrelay::upstream::flow::FlowLimits;
use vlrelay::upstream::{
    Backpressure, HostRecord, HostStatus, HostStore, Limits, Manager, MemHostStore, Tier, TierLimits, UpstreamConfig,
};

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

fn limited(events_per_sec: f64) -> TierLimits {
    TierLimits {
        events_per_sec,
        bytes_per_sec: f64::INFINITY,
        burst_secs: 1.0,
        weight: 1,
        events_per_hour: events_per_sec * 3_600.0,
        events_per_day: 0.0,
        reconnects_per_hour: 0.0,
    }
}

/// `spec` at 30 events/s (and 108k/h); frames delivered in `d`, and whether
/// the host ever showed throttled.
async fn under_limit(name: &str, spec: HostSpec, d: Duration, want: usize) -> (usize, bool) {
    let fan = Fan::spawn().await;
    fan.set(name, spec);
    let mut cfg = fan_config(&fan);
    cfg.limits.new = limited(30.0);
    let (m, mut rx) = Manager::new(cfg, Arc::new(MemHostStore::default()), None);
    m.start().await.unwrap();
    let h = Host(format!("{name}.fan.test"));
    m.admit(&h, Tier::New).await.unwrap();
    let t = Instant::now();
    let (mut n, mut throttled) = (0, false);
    while t.elapsed() < d && n < want {
        if let Ok(Some(_)) = tokio::time::timeout(Duration::from_millis(20), rx.recv()).await {
            n += 1;
        }
        throttled |= m.host(&h).unwrap().record.status == HostStatus::Throttled;
    }
    m.shutdown().await.unwrap();
    (n, throttled)
}

/// An hour the host sent at 20/s, under its 30/s, replayed as fast as the
/// socket goes: counted on the host's own timeline it costs what the
/// traffic did, so nothing holds it (on the wall clock it would take 40 min).
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_backlog_replays_at_the_cost_the_traffic_had() {
    let spec = HostSpec { stamp: Stamp::Backlog { secs: 3_600.0, rate: 20.0 }, ..HostSpec::rate(0.0, 50) };
    let (n, throttled) = under_limit("behind", spec, Duration::from_secs(60), 72_000).await;
    assert!(n >= 72_000, "replayed {n} of 72000");
    assert!(!throttled, "a backlog within its limits was throttled");
}

/// The same frames stamped as they're sent are a burst, and the limit holds.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_burst_stamped_as_sent_is_held_to_the_limit() {
    let spec = HostSpec { stamp: Stamp::Sent { skew_secs: 0.0 }, ..HostSpec::rate(f64::INFINITY, 50) };
    let (n, throttled) = under_limit("burst", spec, Duration::from_secs(2), usize::MAX).await;
    // 30/s for 2 s, the 30-frame burst, and what the queue took before it emptied
    assert!(n <= 400, "delivered {n}");
    assert!(throttled);
}

/// Stamping its events an hour ahead buys a host nothing.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn future_dated_events_dont_evade_the_limit() {
    let spec = HostSpec { stamp: Stamp::Sent { skew_secs: 3_600.0 }, ..HostSpec::rate(f64::INFINITY, 50) };
    let (n, throttled) = under_limit("ahead", spec, Duration::from_secs(2), usize::MAX).await;
    assert!(n <= 400, "delivered {n}");
    assert!(throttled);
}

/// Nobody takes frames, so the relay pauses the host itself: `backpressure`
/// naming what's full, never `throttled` (its limits are unlimited here).
/// Once frames are taken again the status clears.
async fn backpressured(tweak: impl FnOnce(&mut UpstreamConfig), want: Backpressure) {
    let fan = Fan::spawn().await;
    fan.set("busy", HostSpec::rate(2000.0, 200));
    let mut cfg = fan_config(&fan);
    cfg.limits = Limits::unlimited();
    tweak(&mut cfg);
    let (m, mut rx) = Manager::new(cfg, Arc::new(MemHostStore::default()), None);
    m.start().await.unwrap();
    let h = Host("busy.fan.test".into());
    m.admit(&h, Tier::Default).await.unwrap();
    let mut throttled = false;
    wait_for(&format!("{want:?}"), Duration::from_secs(5), || {
        let v = m.host(&h).unwrap();
        throttled |= v.record.status == HostStatus::Throttled;
        v.backpressure == Some(want)
    })
    .await;
    assert_eq!(m.host(&h).unwrap().record.status, HostStatus::Backpressure);
    // the socket buffered the host's frames while paused, and the host keeps
    // sending: only a consumer that outruns it ever lets the queue empty (a
    // polling one, slowed by a busy machine, can fall behind for good)
    let drain = tokio::spawn(async move { while rx.recv().await.is_some() {} });
    let t = Instant::now();
    let mut clear = false;
    while !clear && t.elapsed() < Duration::from_secs(20) {
        tokio::time::sleep(Duration::from_millis(1)).await;
        let v = m.host(&h).unwrap();
        throttled |= v.record.status == HostStatus::Throttled;
        clear = v.record.status == HostStatus::Active && v.backpressure.is_none();
    }
    assert!(clear, "still {:?} {:?}", m.host(&h).unwrap().record.status, m.host(&h).unwrap().backpressure);
    assert!(!throttled, "an unlimited host showed as throttled");
    m.shutdown().await.unwrap();
    drain.abort();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn host_inflight_cap_is_backpressure() {
    backpressured(|c| c.inflight = FlowLimits { host_events: 16, ..FlowLimits::default() }, Backpressure::InflightFull)
        .await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn node_inflight_cap_is_backpressure() {
    backpressured(|c| c.inflight = FlowLimits { events: 16, ..FlowLimits::default() }, Backpressure::NodeInflightFull)
        .await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn full_lane_queue_is_backpressure() {
    backpressured(
        |c| {
            c.output_capacity = 1;
            c.host_queue_frames = 4;
        },
        Backpressure::QueueFull,
    )
    .await;
}

/// Read-lag cases (`vlrelay::node::lag`) against real readers: a host
/// whose stream runs 10 s late trips once the relay has room for it, and
/// the same host behind a relay that isn't taking frames doesn't.
async fn lag_trips(drained: bool) -> (usize, bool) {
    use vlrelay::node::lag::{LagCaseConfig, LagWatch, Pressure, Sample};
    let fan = Fan::spawn().await;
    fan.set("late", HostSpec { stamp: Stamp::Sent { skew_secs: -10.0 }, ..HostSpec::rate(200.0, 50) });
    let mut cfg = fan_config(&fan);
    cfg.limits = Limits::unlimited();
    if !drained {
        cfg.output_capacity = 1;
        cfg.host_queue_frames = 4;
    }
    let (m, mut rx) = Manager::new(cfg, Arc::new(MemHostStore::default()), None);
    m.start().await.unwrap();
    let h = Host("late.fan.test".into());
    m.admit(&h, Tier::Default).await.unwrap();
    let drain = drained.then(|| tokio::spawn(async move { while rx.recv().await.is_some() {} }));
    let mut w = LagWatch::new(LagCaseConfig {
        threshold: Duration::from_secs(5),
        sustain: Duration::from_secs(1),
        grace: Duration::from_secs(2),
        ..LagCaseConfig::default()
    });
    let (mut trips, mut held) = (0, false);
    let t = Instant::now();
    while t.elapsed() < Duration::from_secs(6) {
        let v = m.host(&h).unwrap();
        held |= v.record.status == HostStatus::Backpressure;
        let now = std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH).unwrap().as_millis() as i64;
        let s = Sample { host: &v.record.hostname, lag_ms: v.host_lag_ms, held_at_ms: v.backpressure_at_ms };
        trips += w.observe(now, Pressure::default(), &[s]).len();
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    m.shutdown().await.unwrap();
    if let Some(d) = drain {
        d.abort();
    }
    (trips, held)
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_late_host_with_room_trips_read_lag() {
    let (trips, held) = lag_trips(true).await;
    assert_eq!(trips, 1);
    assert!(!held);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_late_host_the_relay_holds_doesnt_trip() {
    let (trips, held) = lag_trips(false).await;
    assert!(held, "never in backpressure");
    assert_eq!(trips, 0);
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
