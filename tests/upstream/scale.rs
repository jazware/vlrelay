//! Connection-scale measurements (ignored by default; they print numbers).
//!
//!   cargo test --release --test upstream scale_ -- --ignored --nocapture --test-threads 1
//!
//! The fan runs in a child process (this test binary re-run as
//! `scale::fan_process`) so its memory and CPU don't count against ours.
//! Knobs: SCALE_HOSTS (idle test, default 2000), SCALE_FLOOD_HOSTS (default
//! 200), SCALE_FRAME (bytes per frame, default 4500), SCALE_SECS (default 5).

use super::fan::{Fan, HostSpec};
use super::fan_config;
use std::io::{BufRead, BufReader};
use std::process::{Child, Command, Stdio};
use std::sync::Arc;
use std::time::{Duration, Instant};
use vlrelay::types::Host;
use vlrelay::upstream::{HostStatus, Limits, Manager, MemHostStore, Tier, UpstreamConfig};

fn env_or(k: &str, d: usize) -> usize {
    std::env::var(k).ok().and_then(|v| v.parse().ok()).unwrap_or(d)
}

#[test]
#[ignore]
fn fan_process() {
    let Ok(spec) = std::env::var("FAN_SPEC") else { return };
    let mut it = spec.split(',');
    let rate: f64 = it.next().unwrap().parse().unwrap();
    let size: usize = it.next().unwrap().parse().unwrap();
    raise_nofile();
    let threads = env_or("FAN_THREADS", 8);
    let rt = tokio::runtime::Builder::new_multi_thread().worker_threads(threads).enable_all().build().unwrap();
    rt.block_on(async {
        let fan = Fan::spawn().await;
        fan.set_default(HostSpec::rate(rate, size));
        println!("FAN_ADDR={}", fan.addr);
        std::future::pending::<()>().await;
    });
}

struct FanChild {
    child: Child,
    fan: Arc<Fan>,
}

impl Drop for FanChild {
    fn drop(&mut self) {
        let _ = self.child.kill();
        let _ = self.child.wait();
    }
}

/// A fan in a child process; the returned `Fan` is only used for its URL
/// scheme, pointed at the child's address.
async fn fan_child(rate: f64, size: usize) -> FanChild {
    let mut child = Command::new(std::env::current_exe().unwrap())
        .args(["--ignored", "--exact", "scale::fan_process", "--nocapture", "--test-threads", "1"])
        .env("FAN_SPEC", format!("{rate},{size}"))
        .stdout(Stdio::piped())
        .spawn()
        .unwrap();
    let mut lines = BufReader::new(child.stdout.take().unwrap()).lines();
    let addr = loop {
        let l = lines.next().expect("fan child exited").unwrap();
        // libtest's "test ... " prefix shares the line
        if let Some(i) = l.find("FAN_ADDR=") {
            break l[i + "FAN_ADDR=".len()..].trim().to_string();
        }
    };
    std::thread::spawn(move || for _ in lines {});
    FanChild { child, fan: Arc::new(Fan::remote(addr.parse().unwrap())) }
}

fn raise_nofile() {
    unsafe {
        let mut r = libc::rlimit { rlim_cur: 0, rlim_max: 0 };
        libc::getrlimit(libc::RLIMIT_NOFILE, &mut r);
        let want = if cfg!(target_os = "macos") { r.rlim_max.min(24_000) } else { r.rlim_max };
        r.rlim_cur = want;
        libc::setrlimit(libc::RLIMIT_NOFILE, &r);
    }
}

fn rss_kb(pid: u32) -> u64 {
    let out = Command::new("ps").args(["-o", "rss=", "-p", &pid.to_string()]).output().unwrap();
    String::from_utf8_lossy(&out.stdout).trim().parse().unwrap_or(0)
}

/// User + system CPU seconds of this process.
fn cpu_secs() -> f64 {
    unsafe {
        let mut u: libc::rusage = std::mem::zeroed();
        libc::getrusage(libc::RUSAGE_SELF, &mut u);
        let tv = |t: libc::timeval| t.tv_sec as f64 + t.tv_usec as f64 / 1e6;
        tv(u.ru_utime) + tv(u.ru_stime)
    }
}

fn scale_config(fan: &Arc<Fan>) -> UpstreamConfig {
    let mut c = fan_config(fan);
    c.limits = Limits::unlimited();
    c.connect_timeout = Duration::from_secs(30);
    c
}

#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
#[ignore]
async fn scale_idle_connections() {
    raise_nofile();
    let n = env_or("SCALE_HOSTS", 2000);
    let fc = fan_child(0.0, 0).await;
    let mut cfg = scale_config(&fc.fan);
    cfg.ping_interval = Duration::from_secs(5);
    cfg.stall_timeout = Duration::from_secs(30);
    let (m, _rx) = Manager::new(cfg, Arc::new(MemHostStore::default()), None);
    m.start().await.unwrap();
    let hosts: Vec<Host> = (0..n).map(|i| Host(format!("idle{i}.fan.test"))).collect();
    tokio::time::sleep(Duration::from_millis(500)).await;
    let me = std::process::id();
    let rss0 = rss_kb(me);
    let fan0 = rss_kb(fc.child.id());
    let t = Instant::now();
    for h in &hosts {
        m.admit(h, Tier::Default).await.unwrap();
    }
    loop {
        let up = hosts.iter().filter(|h| m.host(h).unwrap().record.status == HostStatus::Active).count();
        if up == n {
            break;
        }
        assert!(t.elapsed() < Duration::from_secs(120), "{up}/{n} connected");
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    let connect_time = t.elapsed();
    tokio::time::sleep(Duration::from_secs(2)).await;
    let rss1 = rss_kb(me);
    let fan1 = rss_kb(fc.child.id());
    let c0 = cpu_secs();
    let idle = Duration::from_secs(10);
    tokio::time::sleep(idle).await;
    let cpu = cpu_secs() - c0;
    let errs: u64 = hosts.iter().map(|h| m.host(h).unwrap().record.errors.stalls).sum();
    eprintln!(
        "idle: {n} hosts connected in {:.2} s; client RSS +{} MB ({:.1} KB/host); fan RSS +{} MB; \
         idle CPU {:.2}% of a core with a 5 s ping ({:.1} us/host/s); stalls {errs}",
        connect_time.as_secs_f64(),
        (rss1 - rss0) / 1024,
        (rss1 - rss0) as f64 / n as f64,
        (fan1.saturating_sub(fan0)) / 1024,
        cpu / idle.as_secs_f64() * 100.0,
        cpu / idle.as_secs_f64() / n as f64 * 1e6,
    );
    m.shutdown().await.unwrap();
}

async fn flood(hosts: usize) {
    raise_nofile();
    let size = env_or("SCALE_FRAME", 4500);
    let secs = env_or("SCALE_SECS", 5) as u64;
    let fc = fan_child(f64::INFINITY, size).await;
    let mut cfg = scale_config(&fc.fan);
    cfg.output_capacity = 4096;
    let (m, mut rx) = Manager::new(cfg, Arc::new(MemHostStore::default()), None);
    m.start().await.unwrap();
    for i in 0..hosts {
        m.admit(&Host(format!("flood{i}.fan.test")), Tier::Default).await.unwrap();
    }
    // warm up, then count what the single consumer receives
    let t = Instant::now();
    while t.elapsed() < Duration::from_secs(1) {
        rx.recv().await.unwrap();
    }
    let c0 = cpu_secs();
    let t = Instant::now();
    let (mut n, mut bytes) = (0u64, 0u64);
    let mut per_host = std::collections::HashMap::<Host, u64>::new();
    while t.elapsed() < Duration::from_secs(secs) {
        let f = rx.recv().await.unwrap();
        n += 1;
        bytes += f.frame.len() as u64;
        *per_host.entry(f.host).or_default() += 1;
    }
    let el = t.elapsed().as_secs_f64();
    let cpu = cpu_secs() - c0;
    let mut counts: Vec<u64> = per_host.values().copied().collect();
    counts.sort();
    eprintln!(
        "flood: {hosts} hosts x {size} B frames: {:.0} frames/s, {:.0} MB/s through one channel; \
         client CPU {:.2} cores ({:.2} us/frame); per-host min/median/max {}/{}/{} over {el:.1} s",
        n as f64 / el,
        bytes as f64 / el / 1e6,
        cpu / el,
        cpu / n as f64 * 1e6,
        counts.first().unwrap_or(&0),
        counts.get(counts.len() / 2).unwrap_or(&0),
        counts.last().unwrap_or(&0),
    );
    m.shutdown().await.unwrap();
}

/// The per-node target's shape: thousands of hosts at a modest rate each
/// (default 2000 x 17/s = 34k events/s of 4.5 KB).
#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
#[ignore]
async fn scale_steady_many_hosts() {
    raise_nofile();
    let hosts = env_or("SCALE_STEADY_HOSTS", 2000);
    let rate = env_or("SCALE_STEADY_RATE", 17) as f64;
    let size = env_or("SCALE_FRAME", 4500);
    let secs = env_or("SCALE_SECS", 5) as u64;
    let fc = fan_child(rate, size).await;
    let (m, mut rx) = Manager::new(scale_config(&fc.fan), Arc::new(MemHostStore::default()), None);
    m.start().await.unwrap();
    for i in 0..hosts {
        m.admit(&Host(format!("steady{i}.fan.test")), Tier::Default).await.unwrap();
    }
    let t = Instant::now();
    while m.hosts().iter().filter(|h| h.connects > 0).count() < hosts || t.elapsed() < Duration::from_secs(2) {
        assert!(t.elapsed() < Duration::from_secs(120));
        while rx.try_recv().is_ok() {}
        tokio::time::sleep(Duration::from_millis(5)).await;
    }
    let c0 = cpu_secs();
    let t = Instant::now();
    let mut lat = Vec::new();
    while t.elapsed() < Duration::from_secs(secs) {
        let f = rx.recv().await.unwrap();
        lat.push((super::fan::now_ns() - super::fan::sent_ns(&f.frame)) as f64 / 1e6);
    }
    let el = t.elapsed().as_secs_f64();
    let cpu = cpu_secs() - c0;
    lat.sort_by(|a, b| a.partial_cmp(b).unwrap());
    let p = |q: f64| lat[((lat.len() as f64 * q) as usize).min(lat.len() - 1)];
    eprintln!(
        "steady: {hosts} hosts x {rate}/s x {size} B: {:.0} frames/s delivered (offered {:.0}); client CPU {:.2} cores \
         ({:.2} us/frame); fan->consumer latency p50 {:.1} ms p99 {:.1} ms; RSS {} MB",
        lat.len() as f64 / el,
        hosts as f64 * rate,
        cpu / el,
        cpu / lat.len() as f64 * 1e6,
        p(0.5),
        p(0.99),
        rss_kb(std::process::id()) / 1024,
    );
    m.shutdown().await.unwrap();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
#[ignore]
async fn scale_flood_one_host() {
    flood(1).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
#[ignore]
async fn scale_flood_many_hosts() {
    flood(env_or("SCALE_FLOOD_HOSTS", 200)).await;
}
