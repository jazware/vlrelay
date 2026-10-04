//! Single-node bench of the relay log and subscribeRepos.
//!
//!   cargo run --release --example log_bench -- --s3 http://127.0.0.1:9300 \
//!       --rate 33000 --size 4500 --linger-ms 25 --secs 30 --subscribers 1
//!
//! `--rate 0` appends as fast as the log takes it. Append latency is per
//! batch (submit to durable). Emit delay is seq assignment to a subscriber
//! reading the event (the seq is the assign time in µs << 8). Serving CPU is
//! the firehose runtime's threads (Linux only).

use bytes::Bytes;
use clap::Parser;
use futures::StreamExt;
use std::net::SocketAddr;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{Duration, Instant};
use vlpds::store::Store;
use vlrelay::seq::{self, Event, EventMeta, LogConfig, SeqSplice};
use vlrelay::serve::{self, ServeConfig};
use vlrelay::types::Host;

#[derive(Parser, Debug, Clone)]
struct Args {
    #[arg(long)]
    s3: Option<String>,
    #[arg(long, default_value = "vlrelay")]
    bucket: String,
    /// events/s in total (0 = unthrottled)
    #[arg(long, default_value_t = 33_000)]
    rate: u64,
    #[arg(long, default_value_t = 4500)]
    size: usize,
    #[arg(long, default_value_t = 25)]
    linger_ms: u64,
    #[arg(long, default_value_t = 4)]
    inflight: usize,
    #[arg(long, default_value_t = 30)]
    secs: u64,
    /// producers, each appending its own batches
    #[arg(long, default_value_t = 16)]
    producers: usize,
    /// events per append
    #[arg(long, default_value_t = 32)]
    batch: usize,
    #[arg(long, default_value_t = 0)]
    subscribers: usize,
    #[arg(long, default_value_t = 4)]
    firehose_threads: usize,
    #[arg(long, default_value_t = 4)]
    client_threads: usize,
    #[arg(long, default_value_t = 1)]
    zstd: i32,
    /// share of each payload that is zeros
    #[arg(long, default_value_t = 0.5)]
    compressible: f64,
}

fn store(a: &Args) -> Store {
    let prefix = format!("bench-{:08x}", rand::random::<u32>());
    match &a.s3 {
        Some(endpoint) => {
            let cfg = vlpds::store::S3Config {
                endpoint: endpoint.clone(),
                bucket: a.bucket.clone(),
                access_key: "minioadmin".into(),
                secret_key: "minioadmin".into(),
                region: "us-east-1".into(),
            };
            Store::s3(&cfg, &prefix, None, 256).unwrap()
        }
        None => Store::memory(None),
    }
}

/// `compressible` of the payload is zeros, the rest random (CAR blocks are
/// mostly hashes and signatures, so real frames compress ~2-3x at best).
fn template(size: usize, i: u64, compressible: f64) -> Bytes {
    let did = format!("did:plc:{:024}", i % 100_000);
    let zeros = (size as f64 * compressible) as usize;
    let mut payload: Vec<u8> = (0..size - zeros).map(|_| rand::random::<u8>()).collect();
    payload.resize(size, 0);
    let f = vlpds::events::sync_frame(&did, "3jzfcijpj2z2a", &payload, "2026-10-04T00:00:00.000Z");
    let mut raw = Vec::new();
    f.finish(i as i64, &mut raw);
    raw.into()
}

fn pct(v: &mut [f64], p: f64) -> f64 {
    if v.is_empty() {
        return 0.0;
    }
    v.sort_by(|a, b| a.partial_cmp(b).unwrap());
    v[((v.len() - 1) as f64 * p).round() as usize]
}

/// CPU seconds of this process's threads whose name starts with `prefix`.
fn thread_cpu(prefix: &str) -> f64 {
    let Ok(dir) = std::fs::read_dir("/proc/self/task") else { return f64::NAN };
    let tick = 100.0;
    let mut total = 0.0;
    for t in dir.flatten() {
        let p = t.path();
        let name = std::fs::read_to_string(p.join("comm")).unwrap_or_default();
        if !name.starts_with(prefix) {
            continue;
        }
        let stat = std::fs::read_to_string(p.join("stat")).unwrap_or_default();
        let f: Vec<&str> = stat.rsplit(')').next().unwrap_or("").split_whitespace().collect();
        if f.len() > 13 {
            total += (f[11].parse::<f64>().unwrap_or(0.0) + f[12].parse::<f64>().unwrap_or(0.0)) / tick;
        }
    }
    total
}

fn proc_cpu() -> f64 {
    let stat = std::fs::read_to_string("/proc/self/stat").unwrap_or_default();
    let f: Vec<&str> = stat.rsplit(')').next().unwrap_or("").split_whitespace().collect();
    if f.len() > 13 {
        (f[11].parse::<f64>().unwrap_or(0.0) + f[12].parse::<f64>().unwrap_or(0.0)) / 100.0
    } else {
        f64::NAN
    }
}

#[derive(Default)]
struct SubStats {
    events: AtomicU64,
    bytes: AtomicU64,
    delays: parking_lot::Mutex<Vec<f64>>,
}

async fn subscriber(addr: SocketAddr, stats: Arc<SubStats>, sample: bool, stop: Arc<std::sync::atomic::AtomicBool>) {
    let url = format!("ws://{addr}/xrpc/com.atproto.sync.subscribeRepos");
    let (mut ws, _) = tokio_tungstenite::connect_async(url).await.unwrap();
    let mut n = 0u64;
    while let Some(m) = ws.next().await {
        if stop.load(Ordering::Relaxed) {
            return;
        }
        let Ok(tokio_tungstenite::tungstenite::Message::Binary(b)) = m else { continue };
        stats.events.fetch_add(1, Ordering::Relaxed);
        stats.bytes.fetch_add(b.len() as u64, Ordering::Relaxed);
        n += 1;
        if sample && n % 16 == 0 {
            if let Some(s) = seq::frame_seq(&b) {
                let now = vlpds::tid::now_micros() as i64;
                stats.delays.lock().push((now - (s >> 8)) as f64 / 1000.0);
            }
        }
    }
}

fn main() {
    let a = Args::parse();
    vlpds::segment::set_compression_level(a.zstd);
    let rt = tokio::runtime::Builder::new_multi_thread().worker_threads(8).thread_name("relay").enable_all().build().unwrap();
    rt.block_on(run(a));
}

async fn run(a: Args) {
    let store = store(&a);
    let mut cfg = LogConfig::new(seq::new_log_id("bench"));
    cfg.linger = Duration::from_millis(a.linger_ms);
    cfg.inflight = a.inflight;
    let sc = ServeConfig { ring_bytes: 512 << 20, threads: a.firehose_threads, ..Default::default() };
    let fh_rt = vlpds::firehose::runtime(a.firehose_threads);
    let st = serve::start_single_node(store.clone(), cfg, sc, Some(fh_rt), None).await.unwrap();
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let app = st.serve.router();
    tokio::spawn(async move {
        axum::serve(listener, app.into_make_service_with_connect_info::<SocketAddr>()).await.unwrap();
    });

    // subscribers on their own runtime, so their CPU isn't counted as serving
    let stop = Arc::new(std::sync::atomic::AtomicBool::new(false));
    let sub_stats = Arc::new(SubStats::default());
    let client_rt = tokio::runtime::Builder::new_multi_thread().worker_threads(a.client_threads).thread_name("client").enable_all().build().unwrap();
    for i in 0..a.subscribers {
        client_rt.spawn(subscriber(addr, sub_stats.clone(), i == 0, stop.clone()));
    }
    tokio::time::sleep(Duration::from_millis(300)).await;

    // ~48 MB of distinct frames: more than zstd's window, so segments don't
    // compress better than real traffic would
    let n = (48_000_000 / a.size.max(1)).clamp(1024, 200_000) as u64;
    let templates: Arc<Vec<SeqSplice>> =
        Arc::new((0..n).map(|i| SeqSplice::parse(template(a.size, i, a.compressible)).unwrap()).collect());
    let lat = Arc::new(parking_lot::Mutex::new(Vec::<f64>::new()));
    let sent = Arc::new(AtomicU64::new(0));
    let t0 = Instant::now();
    let end = t0 + Duration::from_secs(a.secs);
    let cpu0 = (proc_cpu(), thread_cpu("firehose"));
    let mut tasks = Vec::new();
    for p in 0..a.producers {
        let (log, templates, lat, sent, a) = (st.log.clone(), templates.clone(), lat.clone(), sent.clone(), a.clone());
        tasks.push(tokio::spawn(async move {
            // each producer paces its share, with several batches in flight
            let per = if a.rate == 0 { 0.0 } else { a.rate as f64 / a.producers as f64 };
            let mut inflight = futures::stream::FuturesUnordered::new();
            let mut i = 0u64;
            let start = Instant::now();
            while Instant::now() < end {
                if per > 0.0 {
                    let due = start + Duration::from_secs_f64((i * a.batch as u64) as f64 / per);
                    if due > Instant::now() {
                        tokio::select! {
                            _ = tokio::time::sleep_until(due.into()) => {}
                            Some(()) = inflight.next(), if !inflight.is_empty() => continue,
                        }
                    }
                }
                let evs: Vec<Event> = (0..a.batch)
                    .map(|j| {
                        let k = (i as usize * a.batch + j + p) % templates.len();
                        Event {
                            meta: EventMeta { did: format!("did:plc:{k:024}"), host: Host("pds.bench".into()), upstream_seq: i as i64, shard: 0 },
                            frame: Box::new(templates[k].clone()),
                        }
                    })
                    .collect();
                let t = Instant::now();
                let ticket = log.submit(evs).await;
                let (lat, sent, n) = (lat.clone(), sent.clone(), a.batch as u64);
                inflight.push(async move {
                    if ticket.await.is_ok() {
                        lat.lock().push(t.elapsed().as_secs_f64() * 1000.0);
                        sent.fetch_add(n, Ordering::Relaxed);
                    }
                });
                i += 1;
                while inflight.len() > 64 {
                    inflight.next().await;
                }
            }
            while inflight.next().await.is_some() {}
        }));
    }
    let mut last = (0u64, 0u64, Instant::now());
    while Instant::now() < end {
        tokio::time::sleep(Duration::from_secs(5)).await;
        let ev = st.log.stats.events.load(Ordering::Relaxed);
        let segs = st.log.stats.segments.load(Ordering::Relaxed);
        let dt = last.2.elapsed().as_secs_f64();
        eprintln!(
            "t={:.0}s durable {:.0} ev/s, {:.1} PUT/s, subs got {}",
            t0.elapsed().as_secs_f64(),
            (ev - last.0) as f64 / dt,
            (segs - last.1) as f64 / dt,
            sub_stats.events.load(Ordering::Relaxed)
        );
        last = (ev, segs, Instant::now());
    }
    for t in tasks {
        t.await.unwrap();
    }
    let wall = t0.elapsed().as_secs_f64();
    // let subscribers drain
    let want = st.log.stats.events.load(Ordering::Relaxed) * a.subscribers as u64;
    let drain = Instant::now();
    while sub_stats.events.load(Ordering::Relaxed) < want && drain.elapsed() < Duration::from_secs(20) {
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    let cpu1 = (proc_cpu(), thread_cpu("firehose"));
    stop.store(true, Ordering::Relaxed);
    let s = &st.log.stats;
    let events = s.events.load(Ordering::Relaxed);
    let segs = s.segments.load(Ordering::Relaxed).max(1);
    let mut l = lat.lock().clone();
    let mut d = sub_stats.delays.lock().clone();
    let out = serde_json::json!({
        "store": if a.s3.is_some() { "minio" } else { "memory" },
        "rate_target": a.rate, "size": a.size, "linger_ms": a.linger_ms, "inflight": a.inflight,
        "batch": a.batch, "producers": a.producers, "subscribers": a.subscribers, "secs": wall,
        "events_per_s": events as f64 / wall,
        "mb_per_s": s.bytes.load(Ordering::Relaxed) as f64 / wall / 1e6,
        "puts_per_s": segs as f64 / wall,
        "seg_mb": s.bytes.load(Ordering::Relaxed) as f64 / segs as f64 / 1e6,
        "seg_events": events as f64 / segs as f64,
        "zstd_ratio": s.bytes.load(Ordering::Relaxed) as f64 / s.stored_bytes.load(Ordering::Relaxed).max(1) as f64,
        "hedges": s.hedges.load(Ordering::Relaxed),
        "durable_ms_p50": pct(&mut l, 0.5), "durable_ms_p99": pct(&mut l, 0.99),
        "emit_ms_p50": pct(&mut d, 0.5), "emit_ms_p99": pct(&mut d, 0.99),
        "sub_events": sub_stats.events.load(Ordering::Relaxed),
        "sub_gbit_per_s": sub_stats.bytes.load(Ordering::Relaxed) as f64 * 8.0 / wall / 1e9,
        "sub_complete": sub_stats.events.load(Ordering::Relaxed) >= want,
        "cpu_process_s": cpu1.0 - cpu0.0,
        "cpu_firehose_s": cpu1.1 - cpu0.1,
        "failed": st.log.failed().map(|e| e.to_string()),
    });
    println!("{out}");
    std::process::exit(0);
}
