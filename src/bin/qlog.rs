//! The quorum log's bench and chaos tools (tests/qlog/chaos.sh):
//!
//!   qlog node   one member: peers on --listen, subscribeRepos and
//!               /qlog/status on --http, `qlog/leader` in the bucket
//!   qlog load   a host owner at --rate events/s: submits to the leader,
//!               resends until acked, writes every ack (seq, did)
//!   qlog check  a websocket consumer per node, the emission checker over
//!               all of them, end-to-end latency and emission pauses, and
//!               at the end a consumer from cursor 0 (the bucket backfill)
//!   qlog verify the flush manifest's consistency (segments, state at F,
//!               cursors at F) against the bucket

use bytes::Bytes;
use clap::{Parser, Subcommand};
use futures::{SinkExt, StreamExt};
use parking_lot::Mutex;
use std::collections::HashMap;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{Duration, Instant};
use tokio::sync::mpsc;
use tokio_tungstenite::tungstenite::Message;
use vlrelay::qlog::check::{Checker, content_id};
use vlrelay::qlog::client::{Client, info_name, parse_test_frame, test_frame};
use vlrelay::qlog::commitlog::{self, CommitLog};
use vlrelay::qlog::emit::{self, Emitter};
use vlrelay::qlog::log::encode_cursors;
use vlrelay::qlog::node::{Config, Durability, Faults, MemoryOnly, Node, Quantiles};

#[global_allocator]
static GLOBAL: tikv_jemallocator::Jemalloc = tikv_jemallocator::Jemalloc;

#[derive(Parser)]
struct Cli {
    #[command(subcommand)]
    cmd: Cmd,
}

#[derive(Subcommand)]
enum Cmd {
    Node(Box<NodeArgs>),
    Load(LoadArgs),
    Check(CheckArgs),
    Verify(VerifyArgs),
}

#[derive(Parser)]
struct S3Args {
    #[arg(long)]
    s3_endpoint: String,
    #[arg(long, default_value = "vlrelay")]
    s3_bucket: String,
    #[arg(long, default_value = "minioadmin")]
    s3_access_key: String,
    #[arg(long, default_value = "minioadmin")]
    s3_secret_key: String,
    #[arg(long)]
    prefix: String,
}

impl S3Args {
    fn store(&self) -> anyhow::Result<vlpds::store::Store> {
        let s3 = vlpds::store::S3Config {
            endpoint: self.s3_endpoint.clone(),
            bucket: self.s3_bucket.clone(),
            access_key: self.s3_access_key.clone(),
            secret_key: self.s3_secret_key.clone(),
            region: "us-east-1".into(),
        };
        Ok(vlpds::store::Store::s3(&s3, &self.prefix, None, 8)?.counted("qlog"))
    }
}

#[derive(Parser)]
struct VerifyArgs {
    #[command(flatten)]
    s3: S3Args,
    /// Retries past a race with a flush deleting the checkpoint just read.
    #[arg(long, default_value_t = 5)]
    attempts: u32,
}

#[derive(Parser)]
struct NodeArgs {
    #[arg(long)]
    id: String,
    /// Peer and submit port.
    #[arg(long)]
    listen: String,
    /// subscribeRepos and /qlog/status.
    #[arg(long)]
    http: String,
    /// id=host:port, once per other member (how this node dials it).
    #[arg(long = "peer")]
    peers: Vec<String>,
    #[command(flatten)]
    s3: S3Args,
    /// Flush every this many ms (0: no flush, no reservation).
    #[arg(long, default_value_t = 30_000)]
    flush_ms: u64,
    /// H: each manifest's R is F + H.
    #[arg(long, default_value_t = 8_640_000)]
    headroom: u64,
    /// Raw bytes a bucket segment is cut at.
    #[arg(long, default_value_t = 64)]
    flush_segment_mb: usize,
    /// Chaos: die (SIGKILL) at this flush step (fenced, sealed, segment,
    /// before-manifest, after-manifest), with --crash-prob; or "mid-trim",
    /// between two commitlog segment deletions; "any" picks every step.
    #[arg(long)]
    crash_at: Option<String>,
    #[arg(long, default_value_t = 0.05)]
    crash_prob: f64,
    /// No more injected crashes once this file exists.
    #[arg(long)]
    crash_stop_file: Option<std::path::PathBuf>,
    #[arg(long, default_value_t = 512)]
    ring_mb: usize,
    /// Committed log kept in memory for replication (default: 64 with a
    /// commitlog, which serves anything older; 512 without).
    #[arg(long)]
    retain_mb: Option<usize>,
    /// The commitlog's directory; memory-only without.
    #[arg(long)]
    commitlog: Option<std::path::PathBuf>,
    #[arg(long, default_value_t = 64)]
    segment_mb: u64,
    /// Local disk kept past what's been emitted (until Phase 3's flush
    /// takes over trimming).
    #[arg(long, default_value_t = 4096)]
    disk_retain_mb: u64,
    /// Chaos: SIGUSR1 is a power cut (the commitlog loses a random part of
    /// what it wrote since its last fsync, plus a torn record, then SIGKILL).
    #[arg(long)]
    power_cut_on_usr1: bool,
    /// Benches: sleep this long before every fsync, to emulate a slower
    /// device on tmpfs.
    #[arg(long)]
    fsync_delay_us: Option<u64>,
    #[arg(long, default_value_t = 100)]
    heartbeat_ms: u64,
    #[arg(long, default_value_t = 1000)]
    election_ms: u64,
    #[arg(long, default_value_t = 300)]
    probe_ms: u64,
    #[arg(long, default_value_t = 500)]
    stagger_ms: u64,
    #[arg(long, default_value_t = 500)]
    rpc_ms: u64,
}

#[derive(Parser)]
struct LoadArgs {
    /// id=host:port of each member's submit port.
    #[arg(long = "node")]
    nodes: Vec<String>,
    #[arg(long, default_value_t = 350.0)]
    rate: f64,
    /// Frame padding: ~5.3 KB is the network's mean frame.
    #[arg(long, default_value_t = 5200)]
    pad: usize,
    #[arg(long, default_value_t = 5)]
    tick_ms: u64,
    #[arg(long, default_value_t = 60)]
    duration: u64,
    #[arg(long, default_value_t = 256)]
    inflight: usize,
    /// Every ack, "seq did" per line.
    #[arg(long)]
    acked: String,
    /// Summary JSON.
    #[arg(long)]
    out: String,
    #[arg(long, default_value = "r")]
    run: String,
    /// Upstream hosts the events come from (round robin): each event's DID
    /// is `did:q:{run}-h{host}:{n}`, n counting from 1 per host, and about
    /// once a second a submit carries every host's cursor (the events acked
    /// so far without a gap).
    #[arg(long, default_value_t = 64)]
    hosts: u64,
}

#[derive(Parser)]
struct CheckArgs {
    /// id=host:port of each member's http port.
    #[arg(long = "node")]
    nodes: Vec<String>,
    /// Stop (and run the final checks) once this file exists.
    #[arg(long)]
    stop_file: String,
    /// The load generator's acks, read at the end.
    #[arg(long)]
    acked: String,
    #[arg(long)]
    out: String,
    /// Global emission pauses longer than this are logged.
    #[arg(long, default_value_t = 50)]
    gap_ms: u64,
    /// At the end, one more consumer from cursor 0 on the first node, through
    /// the bucket backfill, the node's local log and the ring.
    #[arg(long, default_value_t = true, action = clap::ArgAction::Set)]
    backfill: bool,
}

fn pairs(v: &[String]) -> anyhow::Result<Vec<(String, String)>> {
    v.iter()
        .map(|p| {
            p.split_once('=')
                .map(|(a, b)| (a.to_string(), b.to_string()))
                .ok_or_else(|| anyhow::anyhow!("want id=addr: {p}"))
        })
        .collect()
}

fn now_us() -> i64 {
    std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH).unwrap().as_micros() as i64
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    tracing_subscriber::fmt()
        .with_writer(std::io::stderr)
        .with_ansi(false)
        .with_env_filter(tracing_subscriber::EnvFilter::try_from_default_env().unwrap_or_else(|_| "info".into()))
        .init();
    match Cli::parse().cmd {
        Cmd::Node(a) => node(*a).await,
        Cmd::Load(a) => load(a).await,
        Cmd::Check(a) => check(a).await,
        Cmd::Verify(a) => verify(a).await,
    }
}

async fn node(a: NodeArgs) -> anyhow::Result<()> {
    let peers: HashMap<String, String> = pairs(&a.peers)?.into_iter().collect();
    let mut cfg = Config::new(&a.id, peers);
    cfg.heartbeat = Duration::from_millis(a.heartbeat_ms);
    cfg.election_timeout = Duration::from_millis(a.election_ms);
    cfg.probe_after = Duration::from_millis(a.probe_ms);
    cfg.stagger = Duration::from_millis(a.stagger_ms);
    cfg.rpc_timeout = Duration::from_millis(a.rpc_ms);
    cfg.retain_bytes = a.retain_mb.unwrap_or(if a.commitlog.is_some() { 64 } else { 512 }) << 20;
    let store = a.s3.store()?;
    let die = |what: &str| {
        eprintln!("qlog: crash injected at {what}");
        // as sudden as a crash: no unwinding, no flush of anything
        unsafe { libc::kill(libc::getpid(), libc::SIGKILL) };
    };
    let crash_at = a.crash_at.clone().unwrap_or_default();
    let prob = a.crash_prob;
    let stop_file = a.crash_stop_file.clone();
    let roll = move || {
        use rand::Rng;
        stop_file.as_ref().is_none_or(|f| !f.exists()) && rand::thread_rng().gen_bool(prob)
    };
    let roll2 = roll.clone();
    if a.flush_ms > 0 {
        let crash: Option<vlrelay::qlog::flush::CrashHook> = match crash_at.as_str() {
            "" | "mid-trim" => None,
            s => {
                let only: Option<vlrelay::qlog::flush::Step> = if s == "any" { None } else { Some(s.parse().map_err(anyhow::Error::msg)?) };
                Some(Arc::new(move |step| {
                    if only.is_none_or(|o| o == step) && roll() {
                        die(&format!("{step:?}"));
                    }
                    false
                }))
            }
        };
        cfg.flush = Some(vlrelay::qlog::flush::Options {
            interval: Duration::from_millis(a.flush_ms),
            headroom: a.headroom,
            segment_bytes: a.flush_segment_mb << 20,
            crash,
        });
    }
    let listener = tokio::net::TcpListener::bind(&a.listen).await?;
    let emit = if a.flush_ms > 0 {
        Emitter::with_store(&a.id, now_us() as u64, a.ring_mb << 20, None, store.clone())
    } else {
        Emitter::new(&a.id, now_us() as u64, a.ring_mb << 20, None)
    };
    let (durability, recovered): (Arc<dyn Durability>, _) = match &a.commitlog {
        Some(dir) => {
            let o = commitlog::Options {
                segment_bytes: a.segment_mb << 20,
                retain_bytes: a.disk_retain_mb << 20,
                memory_bytes: cfg.retain_bytes,
                abort_on_error: true,
                sync_delay: a.fsync_delay_us.map(Duration::from_micros),
                mid_trim: (crash_at == "mid-trim").then(|| {
                    commitlog::Hook(Arc::new(move || {
                        if roll2() {
                            die("mid-trim");
                        }
                    }))
                }),
            };
            let (cl, r) = CommitLog::open(dir, o)?;
            if a.power_cut_on_usr1 {
                let cl = cl.clone();
                let mut sig = tokio::signal::unix::signal(tokio::signal::unix::SignalKind::user_defined1())?;
                tokio::spawn(async move {
                    sig.recv().await;
                    use rand::Rng;
                    let mut rng = rand::thread_rng();
                    let garbage: Vec<u8> = (0..rng.gen_range(0..40)).map(|_| rng.r#gen()).collect();
                    let keep = rng.gen_range(0.0..1.0);
                    let r = cl.power_cut(keep, &garbage);
                    eprintln!(
                        "qlog: power cut (kept {keep:.2} of the unsynced tail, {} bytes torn): {r:?}",
                        garbage.len()
                    );
                    // as sudden as the power going: no unwinding, no core dump
                    unsafe { libc::kill(libc::getpid(), libc::SIGKILL) };
                });
            }
            (Arc::new(cl) as Arc<dyn Durability>, Some(r))
        }
        None => (Arc::new(MemoryOnly) as Arc<dyn Durability>, None),
    };
    let node = Node::start(cfg, store, listener, emit, Arc::new(Faults::default()), durability, recovered).await?;
    let http = tokio::net::TcpListener::bind(&a.http).await?;
    axum::serve(http, emit::router(node)).await?;
    Ok(())
}

#[derive(serde::Serialize, Default)]
struct LoadSummary {
    rate: f64,
    pad: usize,
    seconds: f64,
    submitted: u64,
    acked: u64,
    acked_per_sec: f64,
    retries: u64,
    /// submit to ack (append, quorum commit, reply), per event, µs
    ack_us: Quantiles,
    cursor_updates: u64,
}

/// Which events are acked with nothing missing below: [0, contig).
#[derive(Default)]
struct Acked {
    contig: u64,
    done: std::collections::BTreeMap<u64, u64>,
}

impl Acked {
    fn ack(&mut self, from: u64, n: u64) {
        self.done.insert(from, n);
        while let Some(n) = self.done.remove(&self.contig) {
            self.contig += n;
        }
    }

    /// Each host's cursor: how many of its events are in [0, contig).
    fn cursors(&self, run: &str, hosts: u64) -> std::collections::BTreeMap<String, u64> {
        (0..hosts)
            .filter(|h| self.contig > *h)
            .map(|h| (format!("{run}-h{h}"), (self.contig - h - 1) / hosts + 1))
            .collect()
    }
}

async fn load(a: LoadArgs) -> anyhow::Result<()> {
    let client = Client::new(pairs(&a.nodes)?);
    let hist = Arc::new(Mutex::new(hdrhistogram::Histogram::<u64>::new_with_bounds(1, 120_000_000, 3)?));
    let (ack_tx, mut ack_rx) = mpsc::unbounded_channel::<(u64, Vec<String>)>();
    let writer = {
        let path = a.acked.clone();
        tokio::spawn(async move {
            use std::io::Write;
            let mut f = std::io::BufWriter::new(std::fs::File::create(&path).expect("acked file"));
            let mut n = 0u64;
            while let Some((first, dids)) = ack_rx.recv().await {
                for (i, d) in dids.iter().enumerate() {
                    writeln!(f, "{} {d}", first + i as u64).expect("write acked");
                }
                n += dids.len() as u64;
            }
            f.flush().expect("flush acked");
            n
        })
    };
    let slots = Arc::new(tokio::sync::Semaphore::new(a.inflight));
    let submitted = Arc::new(AtomicU64::new(0));
    let acked = Arc::new(AtomicU64::new(0));
    let t0 = Instant::now();
    let mut tick = tokio::time::interval(Duration::from_millis(a.tick_ms));
    tick.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Burst);
    let mut owed = 0.0f64;
    let mut k = 0u64;
    let mut last_report = Instant::now();
    let mut last_acked = 0u64;
    let mut tasks = tokio::task::JoinSet::new();
    let track = Arc::new(Mutex::new(Acked::default()));
    let mut pending_cursors = Bytes::new();
    let mut cursor_updates = 0u64;
    let mut last_cursors = Instant::now();
    let window = Arc::new(Mutex::new(hdrhistogram::Histogram::<u64>::new_with_bounds(1, 120_000_000, 3)?));
    let mut timeline = std::io::BufWriter::new(std::fs::File::create(format!("{}.timeline.jsonl", a.out))?);
    while t0.elapsed() < Duration::from_secs(a.duration) {
        tick.tick().await;
        owed += a.rate * a.tick_ms as f64 / 1000.0;
        let n = owed.floor() as usize;
        owed -= n as f64;
        if n > 0 {
            let sent = now_us();
            let dids: Vec<String> = (0..n as u64)
                .map(|i| {
                    let e = k + i;
                    format!("did:q:{}-h{}:{}", a.run, e % a.hosts, e / a.hosts + 1)
                })
                .collect();
            let k0 = k;
            k += n as u64;
            let frames: Vec<(Bytes, Bytes)> = dids.iter().map(|d| test_frame(d, a.pad, sent)).collect();
            submitted.fetch_add(n as u64, Ordering::Relaxed);
            if last_cursors.elapsed() >= Duration::from_secs(1) {
                last_cursors = Instant::now();
                pending_cursors = encode_cursors(&track.lock().cursors(&a.run, a.hosts));
                cursor_updates += 1;
            }
            let cursors = std::mem::take(&mut pending_cursors);
            let permit = slots.clone().acquire_owned().await?;
            let (client, hist, ack_tx, acked, track, window) =
                (client.clone(), hist.clone(), ack_tx.clone(), acked.clone(), track.clone(), window.clone());
            tasks.spawn(async move {
                let t = Instant::now();
                let (first, cnt) = client.submit_with(frames, cursors).await;
                let us = t.elapsed().as_micros().max(1) as u64;
                track.lock().ack(k0, n as u64);
                let _ = hist.lock().record_n(us, cnt);
                let _ = window.lock().record_n(us, cnt);
                acked.fetch_add(cnt, Ordering::Relaxed);
                let _ = ack_tx.send((first, dids));
                drop(permit);
            });
        }
        while tasks.try_join_next().is_some() {}
        if last_report.elapsed() >= Duration::from_secs(1) {
            {
                use std::io::Write;
                let mut w = window.lock();
                writeln!(
                    timeline,
                    "{{\"t_ms\":{},\"n\":{},\"p50\":{},\"p99\":{},\"max\":{}}}",
                    now_us() / 1000,
                    w.len(),
                    w.value_at_quantile(0.5),
                    w.value_at_quantile(0.99),
                    w.max()
                )?;
                w.reset();
            }
            let n = acked.load(Ordering::Relaxed);
            let h = hist.lock();
            tracing::info!(
                acked_per_sec = (n - last_acked) as f64 / last_report.elapsed().as_secs_f64(),
                total = n,
                p50_us = h.value_at_quantile(0.5),
                p99_us = h.value_at_quantile(0.99),
                retries = client.retries.load(Ordering::Relaxed),
                "load"
            );
            last_acked = n;
            last_report = Instant::now();
        }
    }
    while tasks.join_next().await.is_some() {}
    drop(ack_tx);
    let n = writer.await?;
    let secs = t0.elapsed().as_secs_f64();
    let s = LoadSummary {
        rate: a.rate,
        pad: a.pad,
        seconds: secs,
        submitted: submitted.load(Ordering::Relaxed),
        acked: n,
        acked_per_sec: n as f64 / secs,
        retries: client.retries.load(Ordering::Relaxed),
        ack_us: Quantiles::of(&hist.lock()),
        cursor_updates,
    };
    {
        use std::io::Write;
        timeline.flush()?;
    }
    std::fs::write(&a.out, serde_json::to_vec_pretty(&s)?)?;
    println!("{}", serde_json::to_string(&s)?);
    Ok(())
}

enum Seen {
    Event { node: String, seq: u64, did: String, sent: i64, at: i64 },
    Skip { node: String },
    Fresh { node: String },
}

async fn consume(
    node: String,
    addr: String,
    tx: mpsc::UnboundedSender<Seen>,
    cursor: Arc<AtomicU64>,
    stop: Arc<std::sync::atomic::AtomicBool>,
) {
    let mut fresh = true;
    while !stop.load(Ordering::Acquire) {
        // from the start of the stream, so nothing emitted before we connect is missed
        let c = cursor.load(Ordering::Acquire);
        let url = format!("ws://{addr}/xrpc/com.atproto.sync.subscribeRepos?cursor={c}");
        let Ok((mut ws, _)) = tokio_tungstenite::connect_async(&url).await else {
            tokio::time::sleep(Duration::from_millis(100)).await;
            continue;
        };
        if c == 0 && fresh {
            let _ = tx.send(Seen::Fresh { node: node.clone() });
        }
        fresh = false;
        while let Some(m) = ws.next().await {
            let b = match m {
                Ok(Message::Binary(b)) => b,
                Ok(Message::Ping(p)) => {
                    let _ = ws.send(Message::Pong(p)).await;
                    continue;
                }
                Ok(Message::Close(_)) | Err(_) => break,
                Ok(_) => continue,
            };
            if let Some((seq, did, sent)) = parse_test_frame(&b) {
                cursor.store(seq, Ordering::Release);
                let _ = tx.send(Seen::Event { node: node.clone(), seq, did, sent, at: now_us() });
            } else if info_name(&b).as_deref() == Some("OutdatedCursor") {
                let _ = tx.send(Seen::Skip { node: node.clone() });
            }
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
}

#[derive(serde::Serialize)]
struct CheckOut {
    verdict: &'static str,
    report: vlrelay::qlog::check::Report,
    /// submit to the first consumer receiving it (any node), µs
    e2e_first_us: Quantiles,
    /// submit to receipt, per node's consumer, µs
    e2e_by_node_us: HashMap<String, Quantiles>,
    last_by_node: HashMap<String, u64>,
    pauses_ms: Vec<(i64, i64)>,
    /// The consumer from cursor 0 at the end: (events, seconds).
    backfilled: Option<(u64, f64)>,
}

/// Reads `addr`'s stream from cursor 0 up to `upto` into the checker as
/// its own stream (dense, and each seq with the content every other
/// consumer saw).
async fn backfill_from_zero(addr: &str, upto: u64, ck: &mut Checker) -> anyhow::Result<u64> {
    let url = format!("ws://{addr}/xrpc/com.atproto.sync.subscribeRepos?cursor=0");
    let (mut ws, _) = tokio_tungstenite::connect_async(&url).await?;
    let mut n = 0u64;
    loop {
        let m = tokio::time::timeout(Duration::from_secs(30), ws.next())
            .await
            .map_err(|_| anyhow::anyhow!("stalled after {n} events"))?
            .ok_or_else(|| anyhow::anyhow!("closed after {n} events"))??;
        let b = match m {
            Message::Binary(b) => b,
            Message::Ping(p) => {
                ws.send(Message::Pong(p)).await?;
                continue;
            }
            Message::Close(_) => anyhow::bail!("closed after {n} events"),
            _ => continue,
        };
        if let Some((seq, did, _)) = parse_test_frame(&b) {
            ck.observe("backfill", seq, content_id(did.as_bytes()));
            n += 1;
            if seq >= upto {
                return Ok(n);
            }
        } else if let Some(name) = info_name(&b) {
            anyhow::bail!("got #info {name} after {n} events");
        }
    }
}

async fn check(a: CheckArgs) -> anyhow::Result<()> {
    let nodes = pairs(&a.nodes)?;
    let (tx, mut rx) = mpsc::unbounded_channel::<Seen>();
    let stop = Arc::new(std::sync::atomic::AtomicBool::new(false));
    let mut cursors = HashMap::new();
    for (id, addr) in &nodes {
        let c = Arc::new(AtomicU64::new(0));
        cursors.insert(id.clone(), c.clone());
        tokio::spawn(consume(id.clone(), addr.clone(), tx.clone(), c, stop.clone()));
    }
    drop(tx);
    let mut ck = Checker::new();
    let mk = || hdrhistogram::Histogram::<u64>::new_with_bounds(1, 600_000_000, 3).expect("bounds");
    let mut first = mk();
    let mut by_node: HashMap<String, hdrhistogram::Histogram<u64>> = HashMap::new();
    let mut skip_next: HashMap<String, bool> = HashMap::new();
    let mut top = 0u64;
    let mut top_at = now_us();
    let mut pauses: Vec<(i64, i64)> = Vec::new();
    let mut stopping: Option<Instant> = None;
    let mut status_tick = tokio::time::interval(Duration::from_millis(500));
    let http = reqwest::Client::new();
    let mut gaps_file = std::fs::File::create(format!("{}/pauses.jsonl", a.out))?;
    loop {
        tokio::select! {
            m = rx.recv() => {
                let Some(m) = m else { break };
                match m {
                    Seen::Event { node, seq, did, sent, at } => {
                        if skip_next.remove(&node).unwrap_or(false) && ck.last(&node).is_some_and(|l| seq > l + 1) {
                            ck.skip(&node, seq - 1);
                        }
                        ck.observe(&node, seq, content_id(did.as_bytes()));
                        let lat = (at - sent).max(1) as u64;
                        let _ = by_node.entry(node).or_insert_with(mk).record(lat);
                        if seq > top {
                            let _ = first.record_n(lat, seq - top);
                            if at - top_at > a.gap_ms as i64 * 1000 {
                                use std::io::Write;
                                pauses.push((top_at / 1000, at / 1000));
                                writeln!(gaps_file, "{{\"from_ms\":{},\"to_ms\":{},\"ms\":{}}}", top_at / 1000, at / 1000, (at - top_at) / 1000)?;
                            }
                            top = seq;
                            top_at = at;
                        }
                    }
                    Seen::Skip { node } => { skip_next.insert(node, true); }
                    Seen::Fresh { node } => ck.restart(&node),
                }
            }
            _ = status_tick.tick() => {
                if stopping.is_none() && std::path::Path::new(&a.stop_file).exists() {
                    tracing::info!("check: stop file seen, waiting for every node to converge");
                    stopping = Some(Instant::now());
                }
                if let Some(t) = stopping {
                    // every node's commit is the same and each consumer has it
                    let mut commits = Vec::new();
                    for (_, addr) in &nodes {
                        let s: Option<serde_json::Value> = match http.get(format!("http://{addr}/qlog/status")).timeout(Duration::from_secs(1)).send().await {
                            Ok(r) => r.json().await.ok(),
                            Err(_) => None,
                        };
                        commits.push(s.and_then(|v| v["commit"].as_u64()));
                    }
                    let done = commits.iter().all(|c| c.is_some() && *c == commits[0])
                        && nodes.iter().all(|(id, _)| ck.last(id) == commits[0]);
                    if done || t.elapsed() > Duration::from_secs(60) {
                        if !done {
                            tracing::warn!(?commits, "check: nodes didn't converge within 60 s");
                        }
                        break;
                    }
                }
            }
        }
    }
    stop.store(true, Ordering::Release);
    let mut backfilled = None;
    if a.backfill
        && let Some((id, addr)) = nodes.first()
        && let Some(upto) = ck.last(id)
    {
        let t = Instant::now();
        let r = backfill_from_zero(addr, upto, &mut ck).await;
        tracing::info!(node = %id, upto, secs = t.elapsed().as_secs_f64(), "check: consumer from cursor 0 done: {r:?}");
        match r {
            Ok(n) => backfilled = Some((n, t.elapsed().as_secs_f64())),
            Err(e) => {
                ck.violations += 1;
                ck.messages.push(format!("the consumer from cursor 0 on {id} failed: {e:#}"));
            }
        }
    }
    for line in std::fs::read_to_string(&a.acked)?.lines() {
        // a load generator killed mid-write leaves a last line without its did
        if let Some((s, d)) = line.split_once(' ')
            && !d.is_empty()
        {
            ck.acked(s.parse()?, content_id(d.as_bytes()));
        }
    }
    let last_by_node: HashMap<String, u64> =
        nodes.iter().map(|(id, _)| (id.clone(), ck.last(id).unwrap_or(0))).collect();
    for (id, l) in &last_by_node {
        if *l != ck.max_seq {
            ck.violations += 1;
            ck.messages.push(format!("{id}'s consumer ended at {l}, below the highest emitted seq {}", ck.max_seq));
        }
    }
    let report = ck.finish(&[]);
    let out = CheckOut {
        verdict: if report.ok { "PASS" } else { "FAIL" },
        e2e_first_us: Quantiles::of(&first),
        e2e_by_node_us: by_node.iter().map(|(k, h)| (k.clone(), Quantiles::of(h))).collect(),
        last_by_node,
        pauses_ms: pauses,
        backfilled,
        report,
    };
    std::fs::write(format!("{}/check.json", a.out), serde_json::to_vec_pretty(&out)?)?;
    println!(
        "check: {} observed={} distinct={} max_seq={} acked={} acked_missing={} violations={} holes={} skipped={}",
        out.verdict,
        out.report.observed,
        out.report.distinct_seqs,
        out.report.max_seq,
        out.report.acked,
        out.report.acked_missing,
        out.report.violations,
        out.report.holes,
        out.report.skipped_with_notice
    );
    for m in &out.report.messages {
        println!("  {m}");
    }
    if !out.report.ok {
        std::process::exit(1);
    }
    Ok(())
}

async fn verify(a: VerifyArgs) -> anyhow::Result<()> {
    let store = a.s3.store()?;
    let mut tries = 0;
    let v = loop {
        tries += 1;
        match vlrelay::qlog::flush::verify(&store).await {
            Ok(v) => break v,
            Err(e) if tries < a.attempts => {
                tracing::warn!("verify: {e:#}, again");
                tokio::time::sleep(Duration::from_millis(200)).await;
            }
            Err(e) => return Err(e),
        }
    };
    println!("{}", serde_json::to_string(&v)?);
    if !v.ok {
        std::process::exit(1);
    }
    Ok(())
}
