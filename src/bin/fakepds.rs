//! fakepds: a synthetic upstream fleet for benching the relay
//! (docs/loadfleet.md). One process plays many PDS hosts, one port each,
//! whose accounts have real secp256k1 keys and MSTs and emit signed sync 1.1
//! commits at a target rate, plus an optional fake PLC directory that
//! resolves every fleet DID.
//!
//!   fakepds run --hosts 10 --dids 1000 --rate 20000 --plc-port 29999
//!   fakepds consume --host http://127.0.0.1:30000 ... --verify --duration 30
//!   fakepds selftest

#[path = "../fakepds/check.rs"]
mod check;
#[path = "../fakepds/fleet.rs"]
mod fleet;
#[path = "../fakepds/generate.rs"]
mod generate;
#[path = "../fakepds/serve.rs"]
mod serve;

use clap::{Args, Parser, Subcommand};
use fleet::Layout;
use futures::StreamExt;
use generate::{Gen, GenCfg, GenStats, HostFaults, HostQueue, KindMix, Label, SizeMix};
use parking_lot::RwLock;
use serve::{HostState, Shape};
use std::collections::BTreeMap;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::time::{Duration, Instant};

#[derive(Parser)]
struct Cli {
    #[command(subcommand)]
    cmd: Cmd,
}

#[derive(Subcommand)]
enum Cmd {
    /// Serve hosts (and optionally the fake PLC) and stream events.
    Run(RunArgs),
    /// Subscribe to hosts, count events/s and optionally check every frame.
    Consume(ConsumeArgs),
    /// Generate every event and fault kind, check them with vlpds's code,
    /// then again over a real websocket and the fake PLC.
    Selftest,
}

#[derive(Args, Clone)]
struct FleetArgs {
    /// Fleet seed: DIDs and keys derive from it, so every process (and the
    /// relay's PLC lookups) must agree on it.
    #[arg(long, default_value = "fakepds")]
    seed: String,
    /// Scheme and address the DID documents and `--host` list name.
    #[arg(long, default_value = "http://127.0.0.1")]
    advertise: String,
    /// Global host g listens on port-base + g.
    #[arg(long, default_value_t = 30000)]
    port_base: u16,
}

#[derive(Args)]
struct RunArgs {
    #[command(flatten)]
    fleet: FleetArgs,
    #[arg(long, default_value = "127.0.0.1")]
    bind: String,
    /// Global index of this process's first host.
    #[arg(long, default_value_t = 0)]
    host_base: u32,
    #[arg(long, default_value_t = 10)]
    hosts: u32,
    /// Accounts per host.
    #[arg(long, default_value_t = 1000)]
    dids: u32,
    /// Serve the fake PLC directory on this port (one process per fleet).
    #[arg(long)]
    plc_port: Option<u16>,
    /// Events/s for this process, split over its hosts.
    #[arg(long, default_value_t = 10000.0)]
    rate: f64,
    /// Zipf exponent of the per-host rate split (0: even).
    #[arg(long, default_value_t = 0.0)]
    skew: f64,
    /// Generator threads (default: half the cores, at most 12).
    #[arg(long)]
    gen_threads: Option<usize>,
    #[arg(long, default_value_t = 1)]
    emit_threads: usize,
    #[arg(long, default_value_t = 5)]
    tick_ms: u64,
    /// Pre-built events kept ahead of the stream, MB per process (default:
    /// room for pregen-secs of the target rate, plus a quarter).
    #[arg(long)]
    pool_mb: Option<usize>,
    /// Wait until this many seconds of events are pre-built before streaming.
    #[arg(long, default_value_t = 2.0)]
    pregen_secs: f64,
    /// Emitted frames kept for cursor replay, MB per process.
    #[arg(long, default_value_t = 256)]
    replay_mb: usize,
    /// How far (seconds) a subscriber may fall behind live before it gets
    /// ConsumerTooSlow. Frames this recent stay in memory either way.
    #[arg(long, default_value_t = 1.0)]
    lag_secs: f64,
    #[arg(long, default_value_t = 50)]
    initial_records: usize,
    /// Records per account above which deletes outpace creates. Memory is
    /// ~250 bytes per record, so accounts x this bounds the trees.
    #[arg(long, default_value_t = 100)]
    target_records: usize,
    #[arg(long, default_value_t = 5200.0)]
    size_p50: f64,
    #[arg(long, default_value_t = 9600.0)]
    size_p99: f64,
    #[arg(long, default_value_t = 16000.0)]
    size_max: f64,
    #[arg(long, default_value_t = 0.0005)]
    big_share: f64,
    #[arg(long, default_value_t = 200000.0)]
    big_max: f64,
    #[arg(long, default_value_t = 0.002)]
    identity_share: f64,
    #[arg(long, default_value_t = 0.002)]
    account_share: f64,
    #[arg(long, default_value_t = 0.001)]
    sync_share: f64,
    /// Per-host rate noise: sigma of the log-normal multiplier.
    #[arg(long, default_value_t = 0.3)]
    noise: f64,
    #[arg(long, default_value_t = 3.0)]
    burst_x: f64,
    #[arg(long, default_value_t = 250.0)]
    burst_ms: f64,
    #[arg(long, default_value_t = 6.0)]
    bursts_per_min: f64,
    /// kind:hosts[:k=v,...], repeatable. Kinds: badsig (rate), gap (rate,
    /// heal), foreign (rate), spam (rate, secs, every, delay), stall (secs,
    /// every), disconnect (every, down), replay (every, count). Hosts are
    /// global indexes: `all`, `3`, `0,2-4`.
    #[arg(long)]
    fault: Vec<String>,
    /// Stop after this many seconds (0: run until killed).
    #[arg(long, default_value_t = 0)]
    duration: u64,
    #[arg(long, default_value_t = 5)]
    stats_secs: u64,
}

#[derive(Args)]
struct ConsumeArgs {
    #[command(flatten)]
    fleet: FleetArgs,
    /// http(s) or ws(s) origin of a host. Repeatable.
    #[arg(long = "host")]
    hosts: Vec<String>,
    /// Also every fleet host `first..first+count` at advertise:port-base+g.
    #[arg(long, default_value_t = 0)]
    count: u32,
    #[arg(long, default_value_t = 0)]
    first: u32,
    #[arg(long)]
    cursor: Option<i64>,
    #[arg(long, default_value_t = 30)]
    duration: u64,
    /// Check every frame (signature, CAR, chain, MST inversion).
    #[arg(long)]
    verify: bool,
    #[arg(long, default_value_t = 4)]
    verify_threads: usize,
}

fn main() -> anyhow::Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(tracing_subscriber::EnvFilter::try_from_default_env().unwrap_or_else(|_| "info".into()))
        .with_writer(std::io::stderr)
        .init();
    let cli = Cli::parse();
    let rt = tokio::runtime::Builder::new_multi_thread().enable_all().build()?;
    match cli.cmd {
        Cmd::Run(a) => rt.block_on(run(a)),
        Cmd::Consume(a) => rt.block_on(consume(a)),
        Cmd::Selftest => rt.block_on(selftest(SelftestOpts::default())),
    }
}

fn weights(hosts: u32, skew: f64) -> Vec<f64> {
    (0..hosts).map(|h| 1.0 / ((h + 1) as f64).powf(skew)).collect()
}

async fn run(a: RunArgs) -> anyhow::Result<()> {
    let layout = Layout::new(&a.fleet.seed, &a.fleet.advertise, a.fleet.port_base);
    let cpus = std::thread::available_parallelism().map_or(4, |n| n.get());
    let threads = a.gen_threads.unwrap_or((cpus / 2).clamp(1, 12));
    let mut faults = vec![HostFaults::default(); a.hosts as usize];
    for f in &a.fault {
        generate::parse_fault(f, a.host_base, a.hosts, &mut faults)?;
    }
    let w = weights(a.hosts, a.skew);
    let wsum: f64 = w.iter().sum();
    let rates: Vec<f64> = w.iter().map(|x| a.rate * x / wsum).collect();
    let cfg = Arc::new(GenCfg {
        layout: layout.clone(),
        host_base: a.host_base,
        hosts: a.hosts,
        dids: a.dids,
        threads,
        initial_records: a.initial_records,
        target_records: a.target_records,
        size: SizeMix { p50: a.size_p50, p99: a.size_p99, max: a.size_max, big_share: a.big_share, big_max: a.big_max },
        mix: KindMix { identity: a.identity_share, account: a.account_share, sync: a.sync_share },
        faults: faults.clone(),
        weights: w.clone(),
    });
    let pool_bytes = a.pool_mb.map_or(a.rate * a.size_p50 * 1.1 * a.pregen_secs.max(0.5) * 1.25, |m| (m << 20) as f64);
    let queues: Arc<Vec<Arc<HostQueue>>> =
        Arc::new(w.iter().map(|x| Arc::new(HostQueue::new((pool_bytes * x / wsum) as usize))).collect());

    let t0 = Instant::now();
    let stop = Arc::new(AtomicBool::new(false));
    let started: Arc<RwLock<Option<Instant>>> = Arc::new(RwLock::new(None));
    let stats: Vec<Arc<GenStats>> = (0..threads).map(|_| Arc::new(GenStats::default())).collect();
    for (t, st) in stats.iter().enumerate() {
        let (cfg, queues, started, stop, st) = (cfg.clone(), queues.clone(), started.clone(), stop.clone(), st.clone());
        std::thread::Builder::new().name(format!("gen-{t}")).spawn(move || match Gen::new(cfg, t) {
            Ok(g) => {
                st.ready.store(1, Ordering::Release);
                g.run(queues, started, stop, st)
            }
            Err(e) => tracing::error!(error = %e, "generator init"),
        })?;
    }

    // hosts and listeners come up first, so a relay can connect early
    let ring_cap = |x: f64| ((a.replay_mb << 20) as f64 * x / wsum) as usize;
    let tick = Duration::from_millis(a.tick_ms.max(1));
    let ticks_per_s = 1000.0 / a.tick_ms.max(1) as f64;
    let mut hosts = Vec::new();
    for l in 0..a.hosts as usize {
        let g = a.host_base + l as u32;
        // a stalled host's subscribers fall behind by the whole stall
        let lag = a.lag_secs + faults[l].stall.map_or(0.0, |(secs, _)| secs);
        let bcap = ((lag * ticks_per_s) as usize).max(16);
        let h = HostState::new(g, layout.clone(), faults[l].clone(), queues[l].clone(), ring_cap(w[l]), bcap);
        let addr = format!("{}:{}", a.bind, a.fleet.port_base as u32 + g);
        let lis = tokio::net::TcpListener::bind(&addr).await.map_err(|e| anyhow::anyhow!("bind {addr}: {e}"))?;
        tokio::spawn(axum::serve(lis, serve::router(h.clone())).into_future());
        println!("HOST {}", layout.host_url(g));
        hosts.push(h);
    }
    if let Some(p) = a.plc_port {
        let addr = format!("{}:{p}", a.bind);
        let lis = tokio::net::TcpListener::bind(&addr).await.map_err(|e| anyhow::anyhow!("bind {addr}: {e}"))?;
        tokio::spawn(axum::serve(lis, serve::plc_router(layout.clone())).into_future());
        println!("PLC {}:{p}", a.fleet.advertise.trim_end_matches('/'));
    }

    while stats.iter().any(|s| s.ready.load(Ordering::Acquire) == 0) {
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    tracing::info!(secs = t0.elapsed().as_secs_f64(), accounts = a.hosts as u64 * a.dids as u64, threads, "accounts built");
    let want = (a.rate * a.pregen_secs) as u64;
    let deadline = Instant::now() + Duration::from_secs(120);
    loop {
        let have: u64 = queues.iter().map(|q| q.q.lock().len() as u64).sum();
        let full = queues.iter().all(|q| q.bytes.load(Ordering::Relaxed) >= q.cap.load(Ordering::Relaxed));
        if have >= want || full || Instant::now() > deadline {
            tracing::info!(events = have, secs = t0.elapsed().as_secs_f64(), "pool ready, streaming");
            break;
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    println!("READY");
    *started.write() = Some(Instant::now());

    let shape = Arc::new(Shape {
        sigma: a.noise,
        half_life_s: 2.0,
        burst_x: a.burst_x,
        burst_ms: a.burst_ms,
        bursts_per_min: a.bursts_per_min,
    });
    let scale = Arc::new(AtomicU64::new(1f64.to_bits()));
    let et = a.emit_threads.clamp(1, hosts.len().max(1));
    for e in 0..et {
        let mine: Vec<_> = hosts.iter().enumerate().filter(|(i, _)| i % et == e).map(|(i, h)| (h.clone(), rates[i])).collect();
        let (shape, stop, scale) = (shape.clone(), stop.clone(), scale.clone());
        std::thread::Builder::new()
            .name(format!("emit-{e}"))
            .spawn(move || serve::run_emitter(mine, tick, shape, stop, scale))?;
    }

    let begin = Instant::now();
    let mut last = (Instant::now(), 0u64, 0u64, 0u64, 0u64);
    loop {
        tokio::time::sleep(Duration::from_secs(a.stats_secs.max(1))).await;
        let now = Instant::now();
        let ev: u64 = hosts.iter().map(|h| h.emitted.load(Ordering::Relaxed)).sum();
        let by: u64 = hosts.iter().map(|h| h.emitted_bytes.load(Ordering::Relaxed)).sum();
        let ge: u64 = stats.iter().map(|s| s.events.load(Ordering::Relaxed)).sum();
        let busy: u64 = stats.iter().map(|s| s.busy_ns.load(Ordering::Relaxed)).sum();
        let starved: u64 = hosts.iter().map(|h| h.starved.load(Ordering::Relaxed)).sum();
        let pool: usize = queues.iter().map(|q| q.bytes.load(Ordering::Relaxed)).sum();
        let subs: usize = hosts.iter().map(|h| h.subs.load(Ordering::Relaxed)).sum();
        let dt = (now - last.0).as_secs_f64();
        println!(
            "{}",
            serde_json::json!({
                "t": begin.elapsed().as_secs(),
                "events_per_s": ((ev - last.1) as f64 / dt).round(),
                "mb_per_s": ((by - last.2) as f64 / dt / 1e6 * 10.0).round() / 10.0,
                "gen_per_s": ((ge - last.3) as f64 / dt).round(),
                "gen_us_per_event": if ge > last.3 { ((busy - last.4) as f64 / 1e3 / (ge - last.3) as f64 * 10.0).round() / 10.0 } else { 0.0 },
                "starved": starved,
                "pool_mb": pool >> 20,
                "subscribers": subs,
                "rss_mb": rss_mb(),
            })
        );
        last = (now, ev, by, ge, busy);
        if a.duration > 0 && begin.elapsed() >= Duration::from_secs(a.duration) {
            stop.store(true, Ordering::Relaxed);
            tokio::time::sleep(Duration::from_millis(200)).await;
            return Ok(());
        }
    }
}

fn rss_mb() -> u64 {
    let out = std::process::Command::new("ps").args(["-o", "rss=", "-p", &std::process::id().to_string()]).output();
    out.ok().and_then(|o| String::from_utf8_lossy(&o.stdout).trim().parse::<u64>().ok()).unwrap_or(0) / 1024
}

fn ws_url(s: &str) -> String {
    let s = s.trim().trim_end_matches('/');
    let s = if let Some(r) = s.strip_prefix("https://") {
        format!("wss://{r}")
    } else if let Some(r) = s.strip_prefix("http://") {
        format!("ws://{r}")
    } else if s.starts_with("ws://") || s.starts_with("wss://") {
        s.to_string()
    } else {
        format!("ws://{s}")
    };
    format!("{s}/xrpc/com.atproto.sync.subscribeRepos")
}

fn port_of(url: &str) -> Option<u16> {
    url.trim_end_matches('/').rsplit(':').next()?.split('/').next()?.parse().ok()
}

#[derive(Default)]
struct Tally {
    kinds: BTreeMap<String, u64>,
    fails: BTreeMap<String, u64>,
}

async fn consume(a: ConsumeArgs) -> anyhow::Result<()> {
    let layout = Layout::new(&a.fleet.seed, &a.fleet.advertise, a.fleet.port_base);
    let events = Arc::new(AtomicU64::new(0));
    let bytes = Arc::new(AtomicU64::new(0));
    let regress = Arc::new(AtomicU64::new(0));
    let vt = a.verify_threads.max(1);
    let mut senders = Vec::new();
    let mut workers = Vec::new();
    if a.verify {
        for _ in 0..vt {
            let (tx, rx) = std::sync::mpsc::sync_channel::<(u32, bytes::Bytes)>(65536);
            senders.push(tx);
            let layout = layout.clone();
            workers.push(std::thread::spawn(move || {
                let mut c = check::Checker::default();
                let mut t = Tally::default();
                for (g, f) in rx {
                    match c.check(&layout, g, &f) {
                        Ok(k) => *t.kinds.entry(k.kind.into()).or_default() += 1,
                        Err((fail, _)) => *t.fails.entry(format!("{fail:?}")).or_default() += 1,
                    }
                }
                t
            }));
        }
    }
    let senders = Arc::new(senders);
    let mut tasks = Vec::new();
    let sizes = Arc::new(parking_lot::Mutex::new(
        hdrhistogram::Histogram::<u64>::new_with_bounds(1, 10_000_000, 2).expect("histogram"),
    ));
    let mut all = a.hosts.clone();
    all.extend((a.first..a.first + a.count).map(|g| layout.host_url(g)));
    anyhow::ensure!(!all.is_empty(), "no hosts: --host or --count");
    for h in &all {
        let url = match a.cursor {
            Some(c) => format!("{}?cursor={c}", ws_url(h)),
            None => ws_url(h),
        };
        let g = port_of(h).map_or(0, |p| p.saturating_sub(a.fleet.port_base) as u32);
        let (events, bytes, regress, senders) = (events.clone(), bytes.clone(), regress.clone(), senders.clone());
        let hist = sizes.clone();
        tasks.push(tokio::spawn(async move {
            let mut local = hdrhistogram::Histogram::<u64>::new_with_bounds(1, 10_000_000, 2).expect("histogram");
            let mut n = 0u32;
            let (mut ws, _) = match tokio_tungstenite::connect_async(&url).await {
                Ok(x) => x,
                Err(e) => {
                    eprintln!("connect {url}: {e}");
                    return;
                }
            };
            let mut last = i64::MIN;
            while let Some(Ok(m)) = ws.next().await {
                let tokio_tungstenite::tungstenite::Message::Binary(b) = m else { continue };
                events.fetch_add(1, Ordering::Relaxed);
                bytes.fetch_add(b.len() as u64, Ordering::Relaxed);
                let _ = local.record(b.len() as u64);
                n += 1;
                if n % 4096 == 0 {
                    let _ = hist.lock().add(&local);
                    local.reset();
                }
                if let Ok((_, n)) = vlpds::cbor::ValueRef::decode_prefix(&b) {
                    if let Ok(body) = vlpds::cbor::ValueRef::decode(&b[n..]) {
                        if let Some(vlpds::cbor::ValueRef::Int(s)) = body.get("seq") {
                            if *s <= last {
                                regress.fetch_add(1, Ordering::Relaxed);
                            }
                            last = last.max(*s);
                        }
                        if !senders.is_empty() {
                            let did = body.get("repo").or_else(|| body.get("did")).and_then(|v| v.as_str()).unwrap_or("");
                            let shard = did.bytes().fold(0usize, |h, c| h.wrapping_mul(31).wrapping_add(c as usize)) % senders.len();
                            let _ = senders[shard].send((g, b));
                        }
                    }
                }
            }
        }));
    }
    let begin = Instant::now();
    let mut last = (Instant::now(), 0u64, 0u64);
    while begin.elapsed() < Duration::from_secs(a.duration) {
        tokio::time::sleep(Duration::from_secs(1)).await;
        let (e, b) = (events.load(Ordering::Relaxed), bytes.load(Ordering::Relaxed));
        let dt = last.0.elapsed().as_secs_f64();
        eprintln!("{:>4}s {:>9.0} ev/s {:>8.1} MB/s", begin.elapsed().as_secs(), (e - last.1) as f64 / dt, (b - last.2) as f64 / dt / 1e6);
        last = (Instant::now(), e, b);
    }
    for t in tasks {
        t.abort();
    }
    let secs = begin.elapsed().as_secs_f64();
    let (e, b) = (events.load(Ordering::Relaxed), bytes.load(Ordering::Relaxed));
    let mut summary = serde_json::json!({
        "events": e,
        "events_per_s": (e as f64 / secs).round(),
        "mb_per_s": (b as f64 / secs / 1e6 * 10.0).round() / 10.0,
        "mean_bytes": if e > 0 { b / e } else { 0 },
        "seq_regressions": regress.load(Ordering::Relaxed),
    });
    {
        let h = sizes.lock();
        summary["frame_bytes"] = serde_json::json!({
            "p50": h.value_at_quantile(0.5), "p90": h.value_at_quantile(0.9),
            "p99": h.value_at_quantile(0.99), "max": h.max(),
        });
    }
    drop(senders);
    if a.verify {
        let mut kinds: BTreeMap<String, u64> = BTreeMap::new();
        let mut fails: BTreeMap<String, u64> = BTreeMap::new();
        for w in workers {
            if let Ok(t) = w.join() {
                for (k, v) in t.kinds {
                    *kinds.entry(k).or_default() += v;
                }
                for (k, v) in t.fails {
                    *fails.entry(k).or_default() += v;
                }
            }
        }
        summary["verified"] = serde_json::json!(kinds);
        summary["failed"] = serde_json::json!(fails);
    }
    println!("{summary}");
    Ok(())
}

#[derive(Clone)]
struct SelftestOpts {
    per_host: usize,
}

impl Default for SelftestOpts {
    fn default() -> Self {
        SelftestOpts { per_host: 3000 }
    }
}

/// What a correct checker says about each label.
fn expected(l: Label) -> Option<check::Fail> {
    match l {
        Label::BadSig => Some(check::Fail::Signature),
        Label::AfterGap => Some(check::Fail::Chain),
        Label::Foreign => Some(check::Fail::Foreign),
        _ => None,
    }
}

fn pct(v: &mut [u32], p: f64) -> u32 {
    if v.is_empty() {
        return 0;
    }
    v.sort_unstable();
    v[((v.len() - 1) as f64 * p).round() as usize]
}

async fn selftest(o: SelftestOpts) -> anyhow::Result<()> {
    let port_base = 39000 + (std::process::id() % 500) as u16 * 8;
    let layout = Layout::new("selftest", "http://127.0.0.1", port_base);
    let hosts = 5u32;
    let mut faults = vec![HostFaults::default(); hosts as usize];
    for f in ["badsig:0:rate=0.03", "gap:1:rate=0.03,heal=2", "foreign:2:rate=0.03", "spam:3:rate=500,secs=1,every=2,delay=0"] {
        generate::parse_fault(f, 0, hosts, &mut faults)?;
    }
    let cfg = Arc::new(GenCfg {
        layout: layout.clone(),
        host_base: 0,
        hosts,
        dids: 40,
        threads: 1,
        initial_records: 50,
        target_records: 80,
        size: SizeMix::default(),
        mix: KindMix { identity: 0.01, account: 0.01, sync: 0.01 },
        faults: faults.clone(),
        weights: vec![1.0; hosts as usize],
    });
    let t0 = Instant::now();
    let mut g = Gen::new(cfg.clone(), 0)?;
    let mut per_host: Vec<Vec<generate::Pending>> = (0..hosts).map(|_| Vec::new()).collect();
    for (h, out) in per_host.iter_mut().enumerate() {
        g.spam(h, 1.0, out);
        while out.len() < o.per_host {
            g.next(h, out)?;
        }
    }
    let n: usize = per_host.iter().map(Vec::len).sum();
    let gen_s = t0.elapsed().as_secs_f64();

    // in process: every frame through the checker
    let mut matrix: BTreeMap<(Label, String), u64> = BTreeMap::new();
    let mut bad = 0u64;
    let mut sizes = Vec::new();
    let mut ops: BTreeMap<usize, u64> = BTreeMap::new();
    let mut frames: Vec<Vec<(Label, Vec<u8>)>> = Vec::new();
    let now = vlpds::events::now_rfc3339();
    let t1 = Instant::now();
    for (h, evs) in per_host.iter().enumerate() {
        let mut c = check::Checker::default();
        let mut fs = Vec::new();
        let mut seen = std::collections::HashSet::new();
        for (i, p) in evs.iter().enumerate() {
            let mut f = Vec::new();
            p.finish(i as i64 + 1, &now, &mut f);
            let r = c.check(&layout, h as u32, &f);
            let got = r.as_ref().err().map(|e| e.0);
            // a gap before the checker first saw the account isn't visible to it
            let first_sight = r.as_ref().is_ok_and(|k| seen.insert(k.did.clone()));
            if got != expected(p.label) && !(p.label == Label::AfterGap && first_sight) {
                bad += 1;
                if bad <= 10 {
                    eprintln!("host {h} #{i} {:?}: expected {:?}, got {:?}", p.label, expected(p.label), r.as_ref().err());
                }
            }
            *matrix.entry((p.label, got.map_or("ok".into(), |f| format!("{f:?}")))).or_default() += 1;
            if p.label == Label::Commit {
                sizes.push(f.len() as u32);
                if let Ok(k) = &r {
                    *ops.entry(k.ops).or_default() += 1;
                }
            }
            fs.push((p.label, f));
        }
        frames.push(fs);
    }
    let check_s = t1.elapsed().as_secs_f64();
    println!("generated {n} events in {gen_s:.2}s ({:.0}/s on one thread), checked in {check_s:.2}s", n as f64 / gen_s);
    for ((l, v), c) in &matrix {
        println!("  {:<9} -> {:<10} {c}", l.name(), v);
    }
    let mean = sizes.iter().map(|&s| s as u64).sum::<u64>() / sizes.len().max(1) as u64;
    println!(
        "  #commit frame bytes: mean {mean} p50 {} p90 {} p99 {} max {} | ops per commit {:?}",
        pct(&mut sizes, 0.5),
        pct(&mut sizes, 0.9),
        pct(&mut sizes, 0.99),
        pct(&mut sizes, 1.0),
        ops
    );

    // over the wire: host 0..hosts on real ports, cursor 0, and the PLC
    let mut states = Vec::new();
    for h in 0..hosts {
        let q = Arc::new(HostQueue::new(usize::MAX));
        let st = HostState::new(h, layout.clone(), HostFaults::default(), q, 1 << 30, 1024);
        let lis = tokio::net::TcpListener::bind(("127.0.0.1", port_base + h as u16)).await?;
        tokio::spawn(axum::serve(lis, serve::router(st.clone())).into_future());
        st.publish(&per_host[h as usize], &now);
        states.push(st);
    }
    let plc = tokio::net::TcpListener::bind(("127.0.0.1", 0)).await?;
    let plc_url = format!("http://{}", plc.local_addr()?);
    tokio::spawn(axum::serve(plc, serve::plc_router(layout.clone())).into_future());

    let mut wire_bad = 0u64;
    for h in 0..hosts {
        let url = format!("{}?cursor=0", ws_url(&layout.host_url(h)));
        let (mut ws, _) = tokio_tungstenite::connect_async(&url).await?;
        let mut c = check::Checker::default();
        let want = &frames[h as usize];
        let mut got = 0usize;
        while got < want.len() {
            let m = tokio::time::timeout(Duration::from_secs(10), ws.next()).await?.ok_or_else(|| anyhow::anyhow!("closed"))??;
            let tokio_tungstenite::tungstenite::Message::Binary(b) = m else { continue };
            let (label, f) = &want[got];
            let r = c.check(&layout, h, &b).err().map(|e| e.0);
            if b.as_ref() != f.as_slice() || (r != expected(*label) && !(*label == Label::AfterGap && r.is_none())) {
                wire_bad += 1;
            }
            got += 1;
        }
    }
    println!("websocket replay from cursor 0: {} frames, {wire_bad} mismatches", n);

    let resolver = vlpds::did_resolver::DidResolver::new(&plc_url, true);
    let mut plc_bad = 0;
    for (g, i) in [(0u32, 0u32), (3, 7), (4, 39), (3, cfg.dids + 5)] {
        let did = layout.did(g, i);
        let doc = resolver.resolve(&did).await.map_err(|e| anyhow::anyhow!("{e}"))?;
        let ep = vlpds::did_resolver::service_endpoint(&doc, "atproto_pds");
        let key = vlpds::did_resolver::signing_key_multibase(&doc).unwrap_or_default();
        if ep.as_deref() != Some(layout.host_url(g).as_str()) || !layout.key(g, i).matches_public(&key) {
            plc_bad += 1;
        }
    }
    let lr: serde_json::Value =
        reqwest::get(format!("{}/xrpc/com.atproto.sync.listRepos?limit=1000", layout.host_url(0))).await?.json().await?;
    let listed = lr["repos"].as_array().map_or(0, Vec::len);
    println!("fake PLC: 4 DIDs resolved through vlpds's DidResolver, {plc_bad} wrong; host 0 listRepos: {listed} repos");

    if bad > 0 || wire_bad > 0 || plc_bad > 0 || listed == 0 {
        anyhow::bail!("selftest failed: {bad} in-process, {wire_bad} over the wire, {plc_bad} PLC, {listed} listed");
    }
    println!("selftest ok");
    Ok(())
}

#[cfg(test)]
mod tests {
    #[tokio::test(flavor = "multi_thread")]
    async fn selftest_small() {
        super::selftest(super::SelftestOpts { per_host: 400 }).await.unwrap();
    }
}
