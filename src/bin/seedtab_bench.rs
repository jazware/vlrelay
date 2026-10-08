//! The local seed table (`plc_seed::table`) at a given DID count: build it
//! from rows in key order (as a member's build streams the seed database),
//! then time lookups and in-place writes. Run it under the memory a node
//! leaves for page cache, with `--drop-cache` so lookups start cold:
//!
//!   systemd-run --user --scope -p MemoryMax=512M seedtab_bench --dir /tmp/t \
//!     --dids 91000000 --lookups 200000 --drop-cache
//!
//! `--keep` skips the build and uses the table in `--dir`.

use bytes::Bytes;
use clap::Parser;
use std::path::PathBuf;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering::Relaxed};
use std::time::Instant;
use vlrelay::plc_seed::Seed;
use vlrelay::plc_seed::table::{Builder, SeedTable};

#[derive(Parser)]
struct Cli {
    #[arg(long)]
    dir: PathBuf,
    #[arg(long, default_value_t = 1_000_000)]
    dids: u64,
    #[arg(long)]
    keep: bool,
    #[arg(long, default_value_t = 100_000)]
    lookups: u64,
    /// Lookups in flight (threads).
    #[arg(long, default_value_t = 32)]
    concurrency: usize,
    /// Percent of lookups for DIDs the table holds.
    #[arg(long, default_value_t = 90)]
    hit_pct: u64,
    /// New and changed rows written in place after the lookups.
    #[arg(long, default_value_t = 100_000)]
    writes: u64,
    /// Evicts the table from the page cache before the lookups.
    #[arg(long)]
    drop_cache: bool,
}

fn splitmix(mut z: u64) -> u64 {
    z = z.wrapping_add(0x9e37_79b9_7f4a_7c15);
    z = (z ^ (z >> 30)).wrapping_mul(0xbf58_476d_1ce4_e5b9);
    z = (z ^ (z >> 27)).wrapping_mul(0x94d0_49bb_1331_11eb);
    z ^ (z >> 31)
}

/// The `i`th of `n` DIDs, in key order: the id space cut into `n` equal
/// steps, a random point in each.
fn key(i: u64, n: u64) -> Vec<u8> {
    let step = (1u128 << 120) / n as u128;
    let jitter = ((splitmix(i) as u128) << 64 | splitmix(!i) as u128) % step.max(1);
    let id = i as u128 * step + jitter;
    let mut k = Vec::with_capacity(16);
    k.push(b'p');
    k.extend_from_slice(&id.to_be_bytes()[1..]);
    k
}

fn seed(i: u64) -> Seed {
    let h = splitmix(i ^ 0xabcd);
    let mut k = vec![0xe7u8, 0x01, 2 + (h & 1) as u8];
    k.extend((0..32u64).map(|n| (splitmix(h ^ n) & 0xff) as u8));
    let host = match h % 100 {
        0..=35 => "pds.example-big.com".to_string(),
        36..=55 => format!("shroom{}.us-east.host.example.net", h % 100),
        _ => format!("pds{}.example.org", h % 20_000),
    };
    Seed {
        created_ms: 1_700_000_000_000 + i,
        tombstone: false,
        key: Some(Bytes::from(k)),
        pds: Some(host),
        pds_http: false,
        lookup: false,
    }
}

fn rss_mb() -> u64 {
    std::fs::read_to_string("/proc/self/status")
        .ok()
        .and_then(|s| {
            s.lines().find(|l| l.starts_with("VmRSS:")).and_then(|l| l.split_whitespace().nth(1)?.parse().ok())
        })
        .unwrap_or(0)
        >> 10
}

#[cfg(target_os = "linux")]
fn drop_cache(dir: &std::path::Path) {
    use std::os::fd::AsRawFd;
    for e in std::fs::read_dir(dir).into_iter().flatten().flatten() {
        if let Ok(f) = std::fs::File::open(e.path()) {
            unsafe { libc::posix_fadvise(f.as_raw_fd(), 0, 0, libc::POSIX_FADV_DONTNEED) };
        }
    }
}

/// macOS has no `posix_fadvise`: the cold reads here may be warm.
#[cfg(not(target_os = "linux"))]
fn drop_cache(_dir: &std::path::Path) {}

fn main() -> anyhow::Result<()> {
    let a = Cli::parse();
    let n = a.dids.max(1);
    if !a.keep {
        let _ = std::fs::remove_dir_all(&a.dir);
        let t0 = Instant::now();
        let mut b = Builder::new(&a.dir)?;
        for i in 0..n {
            b.push(&key(i, n), &seed(i))?;
        }
        let t = b.finish()?;
        println!(
            "BUILD dids={n} secs={:.1} rows_per_s={:.0} disk_mb={} bytes_per_did={:.1} load={:.3} rss_mb={}",
            t0.elapsed().as_secs_f64(),
            n as f64 / t0.elapsed().as_secs_f64(),
            t.disk_bytes() >> 20,
            t.disk_bytes() as f64 / n as f64,
            t.len() as f64 / (t.pages() as f64 * vlrelay::plc_seed::table::SLOTS as f64),
            rss_mb()
        );
    }
    let t = Arc::new(SeedTable::open(&a.dir)?);
    if a.drop_cache {
        drop_cache(&a.dir);
    }
    let hist = Arc::new(parking_lot::Mutex::new(hdrhistogram::Histogram::<u64>::new(3)?));
    let (next, found) = (Arc::new(AtomicU64::new(0)), Arc::new(AtomicU64::new(0)));
    let t0 = Instant::now();
    std::thread::scope(|s| {
        for _ in 0..a.concurrency {
            let (t, hist, next, found) = (t.clone(), hist.clone(), next.clone(), found.clone());
            s.spawn(move || {
                let mut local = hdrhistogram::Histogram::<u64>::new(3).unwrap();
                loop {
                    let i = next.fetch_add(1, Relaxed);
                    if i >= a.lookups {
                        break;
                    }
                    let h = splitmix(i ^ 0x77);
                    let k = if h % 100 < a.hit_pct {
                        key(h % n, n)
                    } else {
                        let mut k = key(h % n, n);
                        k[3] ^= 0x5a;
                        k
                    };
                    let did = vlrelay::plc_seed::local::did_of(&k).unwrap();
                    let at = Instant::now();
                    if t.get(&did).unwrap().is_some() {
                        found.fetch_add(1, Relaxed);
                    }
                    local.record(at.elapsed().as_micros() as u64).ok();
                }
                hist.lock().add(&local).ok();
            });
        }
    });
    let secs = t0.elapsed().as_secs_f64();
    let h = hist.lock().clone();
    println!(
        "LOOKUPS dids={n} n={} conc={} cold={} found={} per_s={:.0} p50_us={} p90_us={} p99_us={} max_us={} probed={} corrupt={} rss_mb={}",
        a.lookups,
        a.concurrency,
        a.drop_cache,
        found.load(Relaxed),
        a.lookups as f64 / secs,
        h.value_at_quantile(0.5),
        h.value_at_quantile(0.9),
        h.value_at_quantile(0.99),
        h.max(),
        t.stats.probed.load(Relaxed),
        t.stats.corrupt.load(Relaxed),
        rss_mb()
    );
    if a.writes > 0 {
        let t0 = Instant::now();
        for i in 0..a.writes {
            let h = splitmix(i ^ 0x99);
            // half newer ops for known DIDs, half new DIDs
            let mut k = key(h % n, n);
            if i % 2 == 1 {
                k[4] ^= 0xa5;
            }
            let mut s = seed(h);
            s.created_ms += 1 << 40;
            t.put_key(&k, &s)?;
        }
        t.sync()?;
        let secs = t0.elapsed().as_secs_f64();
        println!(
            "WRITES n={} secs={secs:.2} per_s={:.0} grown={} dropped={} rows={}",
            a.writes,
            a.writes as f64 / secs,
            t.stats.grown.load(Relaxed),
            t.stats.dropped.load(Relaxed),
            t.len()
        );
    }
    Ok(())
}
