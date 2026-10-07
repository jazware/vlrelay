//! The PLC export's fill as the leader runs it: the real ingester
//! (`plc_seed::ingest`) reading a fake `/export` at `--rate` requests a
//! second into the seeds' SlateDB on a local directory standing in for the
//! bucket, with `--gets-per-sec` seed reads beside it (the identity cache's
//! misses) and `--ballast-mb` of touched memory standing in for the rest of
//! a live relay. Prints the process's RSS and jemalloc's numbers every few
//! seconds and the peaks at the end. Run it under the box's memory cap,
//! after `seeds_bench` has left a database of the production size behind:
//!
//!   seeds_bench --dir /tmp/seeds --rows 57000000 --tail-secs 0
//!   systemd-run --user --scope -p MemoryMax=2300M env VLPDS_INJECT_QLOG_PLC_MS=40,150 \
//!     export_mem_bench --dir /tmp/seeds --keep --ballast-mb 1300 --secs 900

use axum::extract::Query;
use axum::routing::get;
use clap::Parser;
use std::collections::HashMap;
use std::path::PathBuf;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering::Relaxed};
use std::time::{Duration, Instant};
use vlrelay::plc_seed::ingest::{Config, Ingester, Sink};
use vlrelay::plc_seed::{Seed, SeedWriter};
use vlrelay::qlog::state::Bounds;

#[global_allocator]
static GLOBAL: tikv_jemallocator::Jemalloc = tikv_jemallocator::Jemalloc;

/// The relay's (src/main.rs), so the numbers are the ones it would see.
#[unsafe(export_name = "_rjem_malloc_conf")]
pub static MALLOC_CONF: &[u8; 21] = b"oversize_threshold:0\0";

#[derive(Parser)]
struct Cli {
    /// The "bucket": emptied first, unless --keep.
    #[arg(long)]
    dir: PathBuf,
    #[arg(long)]
    keep: bool,
    /// `--plc-export-rate`: requests a second.
    #[arg(long, default_value_t = 1.0)]
    rate: f64,
    /// Ops per page.
    #[arg(long, default_value_t = 1000)]
    page: usize,
    /// Each page's answer waits this long (plc.directory: 0.2-0.6 s).
    #[arg(long, default_value_t = 400)]
    export_ms: u64,
    /// The ingester's apply batch (DIDs).
    #[arg(long, default_value_t = 50_000)]
    batch: usize,
    /// Seed reads a second, random DIDs (mostly misses, as a cold cache's).
    #[arg(long, default_value_t = 200)]
    gets_per_sec: u64,
    /// Seed reads in flight at most (the relay's lanes plus prefetch slots).
    #[arg(long, default_value_t = 128)]
    gets_inflight: usize,
    /// Share of reads for DIDs the export has written (hits); the rest
    /// are random DIDs (misses).
    #[arg(long, default_value_t = 0.7)]
    hit_ratio: f64,
    /// Memory allocated and touched up front: the live relay beside the fill.
    #[arg(long, default_value_t = 0)]
    ballast_mb: usize,
    /// How long the fill runs.
    #[arg(long, default_value_t = 600)]
    secs: u64,
    /// `--plc-seeds-slatedb`'s spelling over the seeds' defaults.
    #[arg(long, default_value = "")]
    bounds: String,
    #[arg(long, default_value_t = vlrelay::qlog::cache::DEFAULT_MB)]
    cache_mb: u64,
}

struct BenchSink(Arc<SeedWriter>);

#[async_trait::async_trait]
impl Sink for BenchSink {
    async fn apply(&self, ops: Vec<(String, Seed)>) -> anyhow::Result<usize> {
        self.0.apply(ops).await
    }
    async fn flush(&self) -> anyhow::Result<()> {
        self.0.flush().await
    }
}

fn rss_kb(field: &str) -> u64 {
    std::fs::read_to_string("/proc/self/status")
        .ok()
        .and_then(|s| {
            s.lines()
                .find(|l| l.starts_with(field))
                .and_then(|l| l.split_whitespace().nth(1).and_then(|v| v.parse().ok()))
        })
        .unwrap_or(0)
}

/// jemalloc's stats as vlpds's /metrics reads them, in MiB.
fn jemalloc_mb() -> HashMap<String, u64> {
    vlpds::metrics::render()
        .lines()
        .filter_map(|l| l.strip_prefix("vlpds_jemalloc_bytes{stat=\""))
        .filter_map(|l| {
            let (k, v) = l.split_once("\"} ")?;
            Some((k.to_string(), v.trim().parse::<f64>().ok()? as u64 >> 20))
        })
        .collect()
}

fn did(rng: &mut impl rand::Rng) -> String {
    const B32: &[u8] = b"abcdefghijklmnopqrstuvwxyz234567";
    let s: String = (0..24).map(|_| B32[rng.gen_range(0..32)] as char).collect();
    format!("did:plc:{s}")
}

fn iso(ms: u64) -> String {
    vlrelay::plc_seed::format_ms(ms)
}

/// One page of new genesis-like ops after `after`, a millisecond apart, each
/// about the size of a real export line.
static GETS: AtomicU64 = AtomicU64::new(0);
static SEEN: std::sync::Mutex<Vec<String>> = std::sync::Mutex::new(Vec::new());
const SEEN_MAX: usize = 1 << 20;

fn page(keys: &[String], after: Option<&str>, count: usize) -> String {
    let mut rng = rand::thread_rng();
    let from = after.and_then(vlrelay::plc_seed::parse_ms).unwrap_or(1_746_000_000_000) + 1;
    let mut out = String::with_capacity(count * 800);
    for k in 0..count.clamp(1, 1000) {
        let key = &keys[rand::Rng::gen_range(&mut rng, 0..keys.len())];
        let d = did(&mut rng);
        {
            let mut seen = SEEN.lock().unwrap();
            if seen.len() < SEEN_MAX {
                seen.push(d.clone());
            } else {
                let i = rand::Rng::gen_range(&mut rng, 0..SEEN_MAX);
                seen[i] = d.clone();
            }
        }
        let op = serde_json::json!({
            "did": d,
            "cid": "bafyreieibu2mtgsovktnswo6l7dv4i4ztioutzpsy7wsasmbznzqxkpyje",
            "createdAt": iso(from + k as u64),
            "nullified": false,
            "operation": {
                "type": "plc_operation",
                "prev": "bafyreieibu2mtgsovktnswo6l7dv4i4ztioutzpsy7wsasmbznzqxkpyje",
                "sig": "DyaPWDItkJnVkN1izINSW-fdjUzP9BkIKlD7SnzD5axfK_870ZZ-1EYcrQLQtP9VkWcp2cdbyIHprjPfeUs8WQ",
                "rotationKeys": [key, key],
                "alsoKnownAs": [format!("at://{}.bsky.social", &d[8..20])],
                "verificationMethods": {"atproto": key},
                "services": {"atproto_pds": {"type": "AtprotoPersonalDataServer",
                    "endpoint": format!("https://host{}.us-east.host.bsky.network", rand::Rng::gen_range(&mut rng, 0..100))}},
            },
        });
        out.push_str(&op.to_string());
        out.push('\n');
    }
    out
}

#[tokio::main(flavor = "multi_thread", worker_threads = 2)]
async fn main() -> anyhow::Result<()> {
    let a = Cli::parse();
    let bounds = Bounds::SEEDS.parse(&a.bounds).map_err(anyhow::Error::msg)?;
    vlrelay::qlog::state::set_seed_bounds(bounds);
    vlrelay::qlog::cache::configure(a.cache_mb);
    if !a.keep {
        let _ = std::fs::remove_dir_all(&a.dir);
    }
    std::fs::create_dir_all(&a.dir)?;

    let mut ballast: Vec<u8> = vec![0; a.ballast_mb << 20];
    for i in (0..ballast.len()).step_by(4096) {
        ballast[i] = 1;
    }

    let keys: Arc<Vec<String>> = Arc::new(
        (0..64)
            .map(|i| {
                format!(
                    "did:key:{}",
                    vlrelay::verify::synth::Signer::new(vlrelay::verify::synth::Curve::K256, i).multibase()
                )
            })
            .collect(),
    );
    let export_ms = a.export_ms;
    let app = axum::Router::new().route(
        "/export",
        get(move |Query(q): Query<HashMap<String, String>>| {
            let keys = keys.clone();
            async move {
                tokio::time::sleep(Duration::from_millis(export_ms)).await;
                let count = q.get("count").and_then(|c| c.parse().ok()).unwrap_or(1000);
                page(&keys, q.get("after").map(String::as_str), count)
            }
        }),
    );
    let lis = tokio::net::TcpListener::bind("127.0.0.1:0").await?;
    let url = format!("http://{}", lis.local_addr()?);
    tokio::spawn(async move { axum::serve(lis, app).await });

    let fs = object_store::local::LocalFileSystem::new_with_prefix(&a.dir)?;
    let base = vlpds::store::Store { raw: Arc::new(fs), prefix: "vlrelay".into(), latency: None };
    let store = vlrelay::qlog::bucket::counted(&base, "plc");

    let peak = Arc::new(AtomicU64::new(0));
    let peak_alloc = Arc::new(AtomicU64::new(0));
    let done = Arc::new(AtomicBool::new(false));
    let t0 = Instant::now();
    let sampler = {
        let (peak, peak_alloc, done) = (peak.clone(), peak_alloc.clone(), done.clone());
        std::thread::spawn(move || {
            let mut last_print = Instant::now() - Duration::from_secs(60);
            while !done.load(Relaxed) {
                let r = rss_kb("VmRSS:");
                peak.fetch_max(r, Relaxed);
                if last_print.elapsed() >= Duration::from_secs(10) {
                    let j = jemalloc_mb();
                    let g = |k: &str| j.get(k).copied().unwrap_or(0);
                    peak_alloc.fetch_max(g("allocated"), Relaxed);
                    eprintln!(
                        "t={:>4}s rss={} MiB allocated={} active={} resident={} retained={} (MiB)",
                        t0.elapsed().as_secs(),
                        r >> 10,
                        g("allocated"),
                        g("active"),
                        g("resident"),
                        g("retained"),
                    );
                    last_print = Instant::now();
                }
                std::thread::sleep(Duration::from_millis(100));
            }
        })
    };

    eprintln!("bounds: {bounds}; ballast {} MiB", a.ballast_mb);
    let w = Arc::new(SeedWriter::open(&store).await?);
    eprintln!("t={:>4}s opened the seeds", t0.elapsed().as_secs());
    let mut cfg = Config::new(&url);
    cfg.rate = a.rate;
    cfg.streams = 1;
    cfg.page = a.page;
    cfg.batch = a.batch;
    let ing = Ingester::new(cfg, store.clone(), Arc::new(BenchSink(w.clone())));
    let deadline = Instant::now() + Duration::from_secs(a.secs);
    let keep: Arc<dyn Fn() -> bool + Send + Sync> = Arc::new(move || Instant::now() < deadline);

    let reads = {
        let (w, keep, n, inflight, hit_ratio) = (w.clone(), keep.clone(), a.gets_per_sec, a.gets_inflight, a.hit_ratio);
        tokio::spawn(async move {
            if n == 0 {
                return 0u64;
            }
            let mut tick = tokio::time::interval(Duration::from_secs_f64(1.0 / n as f64));
            let sem = Arc::new(tokio::sync::Semaphore::new(inflight));
            let hits = Arc::new(AtomicU64::new(0));
            while keep() {
                tick.tick().await;
                let Ok(p) = sem.clone().try_acquire_owned() else { continue };
                let (w, hits) = (w.clone(), hits.clone());
                let d = {
                    let mut rng = rand::thread_rng();
                    let seen = SEEN.lock().unwrap();
                    if !seen.is_empty() && rand::Rng::gen_bool(&mut rng, hit_ratio.clamp(0.0, 1.0)) {
                        seen[rand::Rng::gen_range(&mut rng, 0..seen.len())].clone()
                    } else {
                        drop(seen);
                        did(&mut rng)
                    }
                };
                tokio::spawn(async move {
                    let r = w.get(&d).await;
                    GETS.fetch_add(1, Relaxed);
                    if let Ok(Some(_)) = r {
                        hits.fetch_add(1, Relaxed);
                    }
                    drop(p);
                });
            }
            hits.load(Relaxed)
        })
    };
    let stats = ing.stats.clone();
    let progress = {
        let (stats, keep) = (stats.clone(), keep.clone());
        tokio::spawn(async move {
            while keep() {
                tokio::time::sleep(Duration::from_secs(30)).await;
                eprintln!(
                    "t={:>4}s gets={} ops={} written={} checkpoints={} restarts={} phases_ms={:?}",
                    t0.elapsed().as_secs(),
                    GETS.load(Relaxed),
                    stats.ops.load(Relaxed),
                    stats.written.load(Relaxed),
                    stats.checkpoints.load(Relaxed),
                    stats.restarts.load(Relaxed),
                    stats.phases.snapshot().map(|(k, us)| (k, us / 1000)),
                );
            }
        })
    };
    ing.clone().supervise(keep).await;
    let _ = progress.await;
    let _ = reads.await;
    w.close().await;
    done.store(true, Relaxed);
    let _ = sampler.join();
    drop(ballast);
    let secs = t0.elapsed().as_secs_f64();
    let down: f64 = vlpds::metrics::render()
        .lines()
        .filter(|l| l.starts_with("vlpds_object_store_bytes_total{") && l.contains("dir=\"down\""))
        .filter_map(|l| l.rsplit(' ').next()?.parse::<f64>().ok())
        .sum();
    println!("BUCKET read {:.1} MiB/s", down / secs / 1048576.0);
    println!(
        "BENCH bounds=\"{bounds}\" rate={} ballast_mb={} secs={secs:.0} ops={} ops_per_s={:.0} checkpoints={} \
         restarts={} peak_rss_mb={} peak_allocated_mb={} vmhwm_mb={}",
        a.rate,
        a.ballast_mb,
        stats.ops.load(Relaxed),
        stats.ops.load(Relaxed) as f64 / secs,
        stats.checkpoints.load(Relaxed),
        stats.restarts.load(Relaxed),
        peak.load(Relaxed) >> 10,
        peak_alloc.load(Relaxed),
        rss_kb("VmHWM:") >> 10,
    );
    Ok(())
}
