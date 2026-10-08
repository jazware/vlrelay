//! The PLC seeds' SlateDB filled as fast as it takes rows, on a local
//! directory standing in for the bucket, reporting the process's peak RSS
//! and the fill rate under a given `--bounds` (`--plc-seeds-slatedb`'s
//! spelling; `slatedb` for SlateDB's own defaults). With
//! `VLPDS_INJECT_QLOG_PLC_MS=<read ms>,<write ms>[,<sigma>]` every request,
//! multipart parts included, takes a bucket's latency. Run it under a
//! memory cap to see whether a box that size survives the fill:
//!
//!   systemd-run --user --scope -p MemoryMax=3G env VLPDS_INJECT_QLOG_PLC_MS=40,150 \
//!     seeds_bench --dir /tmp/seeds --rows 40000000 --bounds slatedb
//!
//! `--keep` reopens a database left behind (`--tail-secs 0` leaves its
//! compactions owed), the restart a node in an OOM loop goes through.
//!
//! `--lookups N` then reads N seeds at `--lookup-concurrency`, a share
//! `--hit-pct` of them for DIDs the fill wrote (the DIDs are a function
//! of their index, so `--keep --rows 0 --known <n>` looks up a database
//! filled before), through the leader's writer or with `--reader` a
//! member's reader, and reports p50/p99 and bucket GETs per lookup.
//! `--disk-cache-dir` and `--meta-mb` are the node's flags.

use bytes::Bytes;
use clap::Parser;
use std::path::PathBuf;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering::Relaxed};
use std::time::{Duration, Instant};
use vlrelay::plc_seed::{Seed, SeedWriter};
use vlrelay::qlog::state::Bounds;

#[global_allocator]
static GLOBAL: tikv_jemallocator::Jemalloc = tikv_jemallocator::Jemalloc;

/// The relay's (src/main.rs), so the peak is the one it would see.
#[unsafe(export_name = "_rjem_malloc_conf")]
pub static MALLOC_CONF: &[u8; 21] = b"oversize_threshold:0\0";

#[derive(Parser)]
struct Cli {
    /// The "bucket": emptied first, unless --keep.
    #[arg(long)]
    dir: PathBuf,
    /// Opens the database already in --dir (a restart on a database whose
    /// compactions are behind, as a leader's takeover finds it).
    #[arg(long)]
    keep: bool,
    /// DIDs written, each once.
    #[arg(long, default_value_t = 20_000_000)]
    rows: u64,
    /// Rows per write batch (an export apply).
    #[arg(long, default_value_t = 10_000)]
    batch: usize,
    /// A durable flush every this many rows (the export's checkpoint).
    #[arg(long, default_value_t = 1_000_000)]
    flush_rows: u64,
    /// Rows per second at most (0: as fast as the database takes them).
    #[arg(long, default_value_t = 0)]
    rate: u64,
    /// Keeps the database open this long after the last row, its
    /// compactions still running, before closing it.
    #[arg(long, default_value_t = 60)]
    tail_secs: u64,
    /// `--plc-seeds-slatedb`'s spelling over the seeds' defaults, or
    /// `slatedb` (SlateDB's own, as before the bounds).
    #[arg(long, default_value = "")]
    bounds: String,
    /// The shared block cache, as `--slatedb-cache-mb`.
    #[arg(long, default_value_t = vlrelay::qlog::cache::DEFAULT_MB)]
    cache_mb: u64,
    /// As `--slatedb-meta-mb`.
    #[arg(long)]
    meta_mb: Option<u64>,
    /// As `--slatedb-disk-cache-dir` (emptied first, unless --keep).
    #[arg(long)]
    disk_cache_dir: Option<PathBuf>,
    #[arg(long, default_value_t = 16384)]
    disk_cache_mb: u64,
    /// Seed lookups after the fill.
    #[arg(long, default_value_t = 0)]
    lookups: u64,
    #[arg(long, default_value_t = 32)]
    lookup_concurrency: usize,
    /// Percent of lookups for DIDs the database holds.
    #[arg(long, default_value_t = 90)]
    hit_pct: u32,
    /// DIDs a database reopened with --keep holds (default: --rows).
    #[arg(long)]
    known: Option<u64>,
    /// Looks up through a member's reader instead of the leader's writer.
    #[arg(long)]
    reader: bool,
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

fn dir_bytes(d: &std::path::Path) -> u64 {
    let Ok(rd) = std::fs::read_dir(d) else { return 0 };
    rd.filter_map(|e| e.ok())
        .map(|e| match e.file_type() {
            Ok(t) if t.is_dir() => dir_bytes(&e.path()),
            _ => e.metadata().map(|m| m.len()).unwrap_or(0),
        })
        .sum()
}

/// The `i`th DID: random-looking, and the same in every run.
fn did(i: u64) -> String {
    const B32: &[u8] = b"abcdefghijklmnopqrstuvwxyz234567";
    let (mut a, mut b) = (splitmix(i), splitmix(i ^ 0x5bd1_e995_0000_0000));
    let s: String = (0..24)
        .map(|n| {
            let c = if n < 12 { a & 31 } else { b & 31 };
            if n < 12 {
                a >>= 5
            } else {
                b >>= 5
            }
            B32[c as usize] as char
        })
        .collect();
    format!("did:plc:{s}")
}

fn splitmix(mut z: u64) -> u64 {
    z = z.wrapping_add(0x9e37_79b9_7f4a_7c15);
    z = (z ^ (z >> 30)).wrapping_mul(0xbf58_476d_1ce4_e5b9);
    z = (z ^ (z >> 27)).wrapping_mul(0x94d0_49bb_1331_11eb);
    z ^ (z >> 31)
}

/// A row shaped like the export's: a 35-byte multikey, and a host from a
/// skewed set of ~26-character names (the real average is 26).
fn seed(i: u64, created_ms: u64) -> Seed {
    let h = splitmix(i ^ 0xabcd);
    let mut key = vec![0xe7u8, 0x01];
    key.extend((0..33u64).map(|n| (splitmix(h ^ n) & 0xff) as u8));
    let host = match h % 100 {
        0..=35 => "pds.example-big.com".to_string(),
        36..=55 => format!("shroom{}.us-east.host.example.net", h % 100),
        _ => format!("pds{}.example.org", h % 20_000),
    };
    Seed { created_ms, tombstone: false, key: Some(Bytes::from(key)), pds: Some(host), pds_http: false, lookup: false }
}

fn get_count() -> u64 {
    prometheus::gather()
        .iter()
        .filter(|f| f.name() == "vlpds_object_store_requests_total")
        .flat_map(|f| f.get_metric().iter())
        .filter(|m| {
            m.get_label().iter().any(|l| l.name() == "op" && l.value().starts_with("get"))
                && m.get_label().iter().any(|l| l.name() == "client" && l.value() == "qlog_plc")
        })
        .map(|m| m.get_counter().get_value() as u64)
        .sum()
}

#[tokio::main(flavor = "multi_thread", worker_threads = 2)]
async fn main() -> anyhow::Result<()> {
    let a = Cli::parse();
    let bounds = match a.bounds.as_str() {
        "slatedb" => Bounds::SLATEDB_DEFAULT,
        s => Bounds::SEEDS.parse(s).map_err(anyhow::Error::msg)?,
    };
    vlrelay::qlog::state::set_seed_bounds(bounds);
    vlrelay::qlog::cache::configure_split(a.cache_mb, a.meta_mb);
    vlrelay::qlog::cache::configure_disk(a.disk_cache_dir.clone(), a.disk_cache_mb);
    if !a.keep {
        let _ = std::fs::remove_dir_all(&a.dir);
        if let Some(d) = &a.disk_cache_dir {
            let _ = std::fs::remove_dir_all(d);
        }
    }
    std::fs::create_dir_all(&a.dir)?;
    let fs = object_store::local::LocalFileSystem::new_with_prefix(&a.dir)?;
    let base = vlsync_store::store::Store { raw: Arc::new(fs), prefix: "vlrelay".into(), latency: None };
    let store = vlrelay::qlog::bucket::counted(&base, "plc");

    let peak = Arc::new(AtomicU64::new(0));
    let done = Arc::new(AtomicBool::new(false));
    let sampler = {
        let (peak, done) = (peak.clone(), done.clone());
        std::thread::spawn(move || {
            let mut last_print = Instant::now();
            let t0 = Instant::now();
            while !done.load(Relaxed) {
                let r = rss_kb("VmRSS:");
                peak.fetch_max(r, Relaxed);
                if last_print.elapsed() >= Duration::from_secs(10) {
                    eprintln!("t={:>4}s rss={} MiB", t0.elapsed().as_secs(), r >> 10);
                    last_print = Instant::now();
                }
                std::thread::sleep(Duration::from_millis(100));
            }
        })
    };

    eprintln!("bounds: {bounds}");
    let w = SeedWriter::open(&store).await?;
    let t0 = Instant::now();
    let mut written = 0u64;
    let mut since_flush = 0u64;
    let mut flush_ms_max = 0u128;
    while written < a.rows {
        let n = (a.batch as u64).min(a.rows - written);
        let now_ms = 1_700_000_000_000 + written;
        let rows: Vec<(String, Seed)> = (written..written + n).map(|i| (did(i), seed(i, now_ms))).collect();
        w.apply(rows).await?;
        written += n;
        since_flush += n;
        if since_flush >= a.flush_rows {
            let f = Instant::now();
            w.flush().await?;
            flush_ms_max = flush_ms_max.max(f.elapsed().as_millis());
            since_flush = 0;
        }
        if a.rate > 0 {
            let due = Duration::from_secs_f64(written as f64 / a.rate as f64);
            if let Some(d) = due.checked_sub(t0.elapsed()) {
                tokio::time::sleep(d).await;
            }
        }
    }
    w.flush().await?;
    let fill = t0.elapsed().as_secs_f64();
    let fill_peak = peak.load(Relaxed);
    eprintln!("filled {written} rows in {fill:.1}s; holding {}s for compactions", a.tail_secs);
    tokio::time::sleep(Duration::from_secs(a.tail_secs)).await;
    let lookups = if a.lookups > 0 {
        let known = a.known.unwrap_or(written).max(1);
        Some(lookups(&a, &store, Arc::new(w), known).await?)
    } else {
        w.close().await;
        None
    };
    done.store(true, Relaxed);
    let _ = sampler.join();
    println!(
        "BENCH bounds=\"{bounds}\" rows={written} fill_s={fill:.1} rows_per_s={:.0} flush_ms_max={flush_ms_max} \
         peak_rss_fill_mb={} peak_rss_mb={} vmhwm_mb={} bucket_mb={}",
        written as f64 / fill,
        fill_peak >> 10,
        peak.load(Relaxed) >> 10,
        rss_kb("VmHWM:") >> 10,
        dir_bytes(&a.dir) >> 20,
    );
    if let Some(l) = lookups {
        println!("{l}");
    }
    Ok(())
}

async fn lookups(
    a: &Cli,
    store: &vlsync_store::store::Store,
    w: Arc<SeedWriter>,
    known: u64,
) -> anyhow::Result<String> {
    use vlrelay::plc_seed::SeedReader;
    vlrelay::plc_seed::set_read_slots(a.lookup_concurrency);
    let r = SeedReader::new(store.clone());
    if a.reader {
        w.close().await;
    } else {
        r.set_writer(w.clone()).await;
    }
    let hist = Arc::new(parking_lot::Mutex::new(hdrhistogram::Histogram::<u64>::new(3)?));
    let (found, next) = (Arc::new(AtomicU64::new(0)), Arc::new(AtomicU64::new(0)));
    let gets0 = get_count();
    let t0 = Instant::now();
    let mut tasks = Vec::new();
    for _ in 0..a.lookup_concurrency {
        let (r, hist, found, next) = (r.clone(), hist.clone(), found.clone(), next.clone());
        let (n, hit_pct) = (a.lookups, a.hit_pct as u64);
        tasks.push(tokio::spawn(async move {
            loop {
                let i = next.fetch_add(1, Relaxed);
                if i >= n {
                    return;
                }
                let h = splitmix(i ^ 0x1234_5678);
                let d = if h % 100 < hit_pct { did(h % known) } else { did(u64::MAX - h % (1 << 40)) };
                let t = Instant::now();
                if r.get(&d).await.is_some() {
                    found.fetch_add(1, Relaxed);
                }
                hist.lock().record(t.elapsed().as_micros() as u64).ok();
            }
        }));
    }
    for t in tasks {
        t.await?;
    }
    let secs = t0.elapsed().as_secs_f64();
    let gets = get_count() - gets0;
    let h = hist.lock().clone();
    let out = format!(
        "LOOKUPS via={} n={} conc={} found={} per_s={:.0} p50_us={} p90_us={} p99_us={} max_us={} gets_per_lookup={:.2} rss_mb={}",
        if a.reader { "reader" } else { "writer" },
        a.lookups,
        a.lookup_concurrency,
        found.load(Relaxed),
        a.lookups as f64 / secs,
        h.value_at_quantile(0.5),
        h.value_at_quantile(0.9),
        h.value_at_quantile(0.99),
        h.max(),
        gets as f64 / a.lookups as f64,
        rss_kb("VmRSS:") >> 10,
    );
    if !a.reader {
        w.close().await;
    }
    Ok(out)
}
