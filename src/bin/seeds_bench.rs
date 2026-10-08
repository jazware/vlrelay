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

fn did(rng: &mut impl rand::Rng) -> String {
    const B32: &[u8] = b"abcdefghijklmnopqrstuvwxyz234567";
    let s: String = (0..24).map(|_| B32[rng.gen_range(0..32)] as char).collect();
    format!("did:plc:{s}")
}

#[tokio::main(flavor = "multi_thread", worker_threads = 2)]
async fn main() -> anyhow::Result<()> {
    let a = Cli::parse();
    let bounds = match a.bounds.as_str() {
        "slatedb" => Bounds::SLATEDB_DEFAULT,
        s => Bounds::SEEDS.parse(s).map_err(anyhow::Error::msg)?,
    };
    vlrelay::qlog::state::set_seed_bounds(bounds);
    vlrelay::qlog::cache::configure(a.cache_mb);
    if !a.keep {
        let _ = std::fs::remove_dir_all(&a.dir);
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
    let mut rng = rand::thread_rng();
    let key = Bytes::from(vec![0xe7u8; 35]);
    let t0 = Instant::now();
    let mut written = 0u64;
    let mut since_flush = 0u64;
    let mut flush_ms_max = 0u128;
    while written < a.rows {
        let n = (a.batch as u64).min(a.rows - written);
        let now_ms = 1_700_000_000_000 + written;
        let rows: Vec<(String, Seed)> = (0..n)
            .map(|_| {
                let seed = Seed {
                    created_ms: now_ms,
                    tombstone: false,
                    key: Some(key.clone()),
                    pds: Some("pds.example.com".into()),
                    pds_http: false,
                    lookup: false,
                };
                (did(&mut rng), seed)
            })
            .collect();
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
    w.close().await;
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
    Ok(())
}
