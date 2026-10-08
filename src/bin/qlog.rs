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
//!   qlog retain what bucket retention could delete below a horizon
//!               (segments, checkpoints, old state paths), written to
//!               `retain/qlog`; nothing is deleted
//!   qlog member a membership change (add, remove, replace, set) through
//!               the leader's `POST /qlog/members`, retried until
//!               `qlog/leader` holds the new set

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
    Retain(RetainArgs),
    Member(MemberArgs),
}

#[derive(Parser)]
struct MemberArgs {
    /// id=host:port of each node's http port: any that answers finds the
    /// leader.
    #[arg(long = "node")]
    nodes: Vec<String>,
    /// id=host:port of a new member's peer port, for a leader whose --peer
    /// flags don't name it.
    #[arg(long = "addr")]
    addrs: Vec<String>,
    /// The nodes' --admin-token.
    #[arg(long, env = "QLOG_ADMIN_TOKEN")]
    admin_token: Option<String>,
    /// Keep trying (a leader change, a crash mid-change) this long.
    #[arg(long, default_value_t = 120)]
    retry_secs: u64,
    /// add ID | remove ID | replace OLD NEW | set ID,ID,...
    op: String,
    ids: Vec<String>,
}

#[derive(Parser)]
struct S3Args {
    #[arg(long)]
    s3_endpoint: String,
    #[arg(long, default_value = "vlrelay")]
    s3_bucket: String,
    #[arg(long, env = "QLOG_S3_ACCESS_KEY", default_value = "minioadmin", hide_env_values = true)]
    s3_access_key: String,
    #[arg(long, env = "QLOG_S3_SECRET_KEY", default_value = "minioadmin", hide_env_values = true)]
    s3_secret_key: String,
    #[arg(long)]
    prefix: String,
}

impl S3Args {
    /// Not counted: every user counts it once, under its purpose
    /// (`qlog::bucket`).
    fn store(&self) -> anyhow::Result<vlsync_store::store::Store> {
        let s3 = vlsync_store::store::S3Config {
            endpoint: self.s3_endpoint.clone(),
            bucket: self.s3_bucket.clone(),
            access_key: self.s3_access_key.clone(),
            secret_key: self.s3_secret_key.clone(),
            region: "us-east-1".into(),
        };
        vlsync_store::store::Store::s3(&s3, &self.prefix, None, 8)
    }
}

#[derive(Parser)]
struct VerifyArgs {
    #[command(flatten)]
    s3: S3Args,
    /// Retries past a race with a flush deleting the checkpoint just read.
    #[arg(long, default_value_t = 5)]
    attempts: u32,
    #[command(flatten)]
    budget: vlrelay::qlog::budget::BudgetArgs,
}

#[derive(Parser)]
struct RetainArgs {
    #[command(flatten)]
    s3: S3Args,
    /// Keep segments with events this recent (vlpds's backfill window).
    #[arg(long, default_value_t = 72 * 3600)]
    horizon_secs: u64,
    /// Don't write `retain/qlog`, only print.
    #[arg(long)]
    dry_run: bool,
    #[command(flatten)]
    budget: vlrelay::qlog::budget::BudgetArgs,
    /// Delete what the report marks deletable (segments past the horizon,
    /// state paths nothing reads any more), re-checked against the manifest
    /// first; `pruned_seq` is raised before any segment goes.
    #[arg(long, conflicts_with = "dry_run")]
    apply: bool,
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
    /// Bearer token for `POST /qlog/members`. Unset, membership changes are
    /// taken only on a loopback --http.
    #[arg(long, env = "QLOG_ADMIN_TOKEN")]
    admin_token: Option<String>,
    /// id=host:port, once per other member (how this node dials it).
    #[arg(long = "peer")]
    peers: Vec<String>,
    #[command(flatten)]
    s3: S3Args,
    /// Flush every this many ms (0: no flush, no reservation).
    #[arg(long, default_value_t = 30_000)]
    flush_ms: u64,
    /// The state's SlateDB compactor and worker poll (SlateDB's default is
    /// 5000).
    #[arg(long, default_value_t = 30_000)]
    state_compactor_poll_ms: u64,
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
    /// When an entry counts: `fsync` (after its fdatasync), `page-cache`
    /// (once written; fdatasync'd every --durability-sync-ms) or `memory`
    /// (no commitlog). Default: page-cache for three members or more,
    /// fsync below; a single node only runs fsync.
    #[arg(long)]
    durability: Option<String>,
    #[arg(long, default_value_t = 100)]
    durability_sync_ms: u64,
    /// Mutation tests only: trust the commitlog after a power loss in
    /// page-cache mode (what the chaos must catch).
    #[arg(long)]
    unsafe_trust_log: bool,
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
    /// Never run a bucket recovery on its own: a candidate that finds the
    /// quorum lost logs it and waits for an operator.
    #[arg(long)]
    no_auto_recover: bool,
    /// The bootstrap member set (comma separated), written by the first
    /// `qlog/leader` CAS; this node and its --peer ids by default. After
    /// that, `qlog/leader` holds the set and `qlog member` changes it.
    #[arg(long, value_delimiter = ',')]
    members: Vec<String>,
    #[command(flatten)]
    budget: vlrelay::qlog::budget::BudgetArgs,
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
    acked: Option<String>,
    /// The streams carry real relay frames (`vlrelay --quorum`), not the
    /// load generator's: each is identified by its bytes.
    #[arg(long)]
    relay_frames: bool,
    #[arg(long)]
    out: String,
    /// Global emission pauses longer than this are logged.
    #[arg(long, default_value_t = 50)]
    gap_ms: u64,
    /// At the end, one more consumer from cursor 0 on the first node, through
    /// the bucket backfill, the node's local log and the ring.
    #[arg(long, default_value_t = true, action = clap::ArgAction::Set)]
    backfill: bool,
    /// The bucket, for the recovery gaps in the manifest (the R + 1 jump
    /// and re-ingest checks); without it any jump is a violation.
    #[arg(long)]
    s3_endpoint: Option<String>,
    #[arg(long, default_value = "vlrelay")]
    s3_bucket: String,
    #[arg(long)]
    prefix: Option<String>,
    /// The load generator's summary (`load --out`): every event it sent
    /// must be emitted outside the recovery gaps.
    #[arg(long)]
    load_summary: Option<String>,
    #[command(flatten)]
    budget: vlrelay::qlog::budget::BudgetArgs,
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
        Cmd::Retain(a) => retain(a).await,
        Cmd::Member(a) => member(a).await,
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
    cfg.auto_recover = !a.no_auto_recover;
    if !a.members.is_empty() {
        cfg.members = a.members.clone();
        cfg.members.sort();
        cfg.members.dedup();
    }
    vlrelay::qlog::state::set_compactor_poll(Duration::from_millis(a.state_compactor_poll_ms));
    cfg.retain_bytes = a.retain_mb.unwrap_or(if a.commitlog.is_some() { 64 } else { 512 }) << 20;
    let bucket = vlrelay::qlog::bucket::Bucket::new(a.s3.store()?);
    vlrelay::qlog::budget::start(&a.budget, bucket.leader.clone())?;
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
                use vlrelay::qlog::flush::Step;
                let only: Option<Step> =
                    if s == "any" || s == "switch-any" { None } else { Some(s.parse().map_err(anyhow::Error::msg)?) };
                let switch_only = s == "switch-any";
                let is_switch = |st: Step| {
                    matches!(st, Step::SwitchCatchUp | Step::SwitchBeforePause | Step::SwitchFlushed | Step::SwitchCas)
                };
                Some(Arc::new(move |step| {
                    if only.is_none_or(|o| o == step) && (!switch_only || is_switch(step)) && roll() {
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
        Emitter::with_store(&a.id, now_us() as u64, a.ring_mb << 20, None, bucket.backfill.clone())
    } else {
        Emitter::new(&a.id, now_us() as u64, a.ring_mb << 20, None)
    };
    let mode = commitlog::DurabilityMode::choose(
        a.durability.as_deref(),
        Duration::from_millis(a.durability_sync_ms),
        cfg.members.len().max(cfg.peers.len() + 1),
    )?;
    let commitlog_dir = match mode {
        commitlog::DurabilityMode::Memory => None,
        commitlog::DurabilityMode::Sync(_) => a.commitlog.as_ref(),
    };
    let sync = match mode {
        commitlog::DurabilityMode::Sync(s) => s,
        commitlog::DurabilityMode::Memory => commitlog::SyncMode::Fsync,
    };
    tracing::info!(durability = mode.name(), "qlog: durability");
    let (durability, recovered): (Arc<dyn Durability>, _) = match commitlog_dir {
        Some(dir) => {
            let o = commitlog::Options {
                sync,
                trust_after_power_loss: a.unsafe_trust_log,
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
    let node = Node::start(cfg, bucket, listener, emit, Arc::new(Faults::default()), durability, recovered).await?;
    let http = tokio::net::TcpListener::bind(&a.http).await?;
    let admin = emit::Admin::for_listener(a.admin_token.clone(), http.local_addr()?);
    if matches!(admin, emit::Admin::Refused) {
        tracing::warn!("qlog: --http isn't loopback and no --admin-token: POST /qlog/members is refused");
    }
    axum::serve(http, emit::router(node, admin)).await?;
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
    run: String,
    hosts: u64,
    /// Events generated: every one of `did:q:{run}-h{e % hosts}:{e / hosts + 1}`
    /// for e below this.
    events: u64,
    /// Bucket recoveries the hosts were rewound for, with the events sent
    /// again each time and how long until all of them were acked again.
    rewinds: Vec<Rewind>,
}

#[derive(serde::Serialize, Clone, Default)]
struct Rewind {
    generation: u64,
    /// From the first ack under the new generation to the cursors read.
    cursors_ms: u64,
    resent: u64,
    /// Of them, acked before the rewind (and so possibly also in the log
    /// at or below the recovery point: duplicates).
    acked_before: u64,
    /// From the cursors read until every resent event was acked again.
    catch_up_ms: Option<u64>,
    at_ms: i64,
}

/// Which events are acked with nothing missing below: [0, contig).
#[derive(Default)]
struct Acked {
    contig: u64,
    done: std::collections::BTreeMap<u64, u64>,
    /// Recent acks with the generation each came under: (first, n, gen).
    recent: std::collections::VecDeque<(u64, u64, u64)>,
}

impl Acked {
    fn ack(&mut self, from: u64, n: u64) {
        if from + n <= self.contig {
            return;
        }
        self.done.insert(from, n);
        while let Some(n) = self.done.remove(&self.contig) {
            self.contig += n;
        }
    }

    fn note(&mut self, es: &[u64], generation: u64) {
        let dense = es.windows(2).all(|w| w[1] == w[0] + 1);
        if dense {
            self.recent.push_back((es[0], es.len() as u64, generation));
        } else {
            self.recent.extend(es.iter().map(|&e| (e, 1, generation)));
        }
        // a few minutes of batches at the rates used: more than any rewind reaches back
        while self.recent.len() > 200_000 {
            self.recent.pop_front();
        }
    }

    fn is_acked(&self, e: u64) -> bool {
        e < self.contig || self.done.range(..=e).next_back().is_some_and(|(f, n)| e < f + n)
    }

    /// Back to a recovery's cursors: every event at or below its host's
    /// cursor stays acked (it's in the log), every later one of the first
    /// `k` is returned to send again, `acked_before` counting those that
    /// were acked already.
    fn rewind(
        &mut self,
        cursors: &std::collections::BTreeMap<String, u64>,
        g: u64,
        run: &str,
        hosts: u64,
        k: u64,
    ) -> (Vec<u64>, u64) {
        let cur = |h: u64| cursors.get(&format!("{run}-h{h}")).copied().unwrap_or(0);
        let start = (0..hosts).map(|h| cur(h) * hosts + h).min().unwrap_or(0).min(k);
        let mut resend = Vec::new();
        let mut acked_before = 0;
        let mut done = std::collections::BTreeMap::new();
        let since: Vec<(u64, u64)> =
            self.recent.iter().filter(|r| r.2 >= g && r.0 + r.1 > start).map(|r| (r.0, r.1)).collect();
        for e in start..k {
            if e / hosts < cur(e % hosts) || since.iter().any(|&(f, n)| e >= f && e < f + n) {
                done.insert(e, 1);
            } else {
                if self.is_acked(e) {
                    acked_before += 1;
                }
                resend.push(e);
            }
        }
        self.contig = start;
        self.done = done;
        while let Some(n) = self.done.remove(&self.contig) {
            self.contig += n;
        }
        (resend, acked_before)
    }

    /// Each host's cursor: how many of its events are in [0, contig).
    fn cursors(&self, run: &str, hosts: u64) -> std::collections::BTreeMap<String, u64> {
        (0..hosts)
            .filter(|h| self.contig > *h)
            .map(|h| (format!("{run}-h{h}"), (self.contig - h - 1) / hosts + 1))
            .collect()
    }
}

/// (generation, events left, since).
type Owed = (u64, std::collections::HashSet<u64>, Instant);

fn load_did(run: &str, hosts: u64, e: u64) -> String {
    format!("did:q:{run}-h{}:{}", e % hosts, e / hosts + 1)
}

struct LoadCtx {
    client: Arc<Client>,
    hist: Arc<Mutex<hdrhistogram::Histogram<u64>>>,
    window: Arc<Mutex<hdrhistogram::Histogram<u64>>>,
    ack_tx: mpsc::UnboundedSender<(u64, Vec<String>)>,
    acked: Arc<AtomicU64>,
    track: Arc<Mutex<Acked>>,
    /// Generation -> the rewind for it (one at a time).
    rewinds: Arc<Mutex<Vec<Rewind>>>,
    rewinding: Arc<tokio::sync::Mutex<()>>,
    rewinds_pending: Arc<AtomicU64>,
    /// Events still owed after a rewind: (generation, events left).
    owed: Arc<Mutex<Option<Owed>>>,
    resend_tx: mpsc::UnboundedSender<Vec<u64>>,
    run: String,
    hosts: u64,
    pad: usize,
    next_event: Arc<AtomicU64>,
}

impl LoadCtx {
    /// Submits events `es` (indices) with `cursors` (as of `cgen`) until
    /// acked; marks them acked unless the ack is from before a recovery
    /// this load already rewound for (they're being sent again).
    async fn send(self: Arc<Self>, es: Vec<u64>, cursors: Bytes, cgen: u64, permit: tokio::sync::OwnedSemaphorePermit) {
        let sent = now_us();
        let dids: Vec<String> = es.iter().map(|&e| load_did(&self.run, self.hosts, e)).collect();
        let frames: Vec<(Bytes, Bytes)> = dids.iter().map(|d| test_frame(d, self.pad, sent)).collect();
        let t = Instant::now();
        let a = self.client.submit_acked(frames, cursors, cgen).await;
        let us = t.elapsed().as_micros().max(1) as u64;
        let _ = self.hist.lock().record_n(us, a.n);
        let _ = self.window.lock().record_n(us, a.n);
        self.acked.fetch_add(a.n, Ordering::Relaxed);
        let _ = self.ack_tx.send((a.first, dids));
        {
            let mut tr = self.track.lock();
            tr.note(&es, a.generation);
            if a.generation >= self.client.generation() {
                let dense = es.windows(2).all(|w| w[1] == w[0] + 1);
                if dense {
                    tr.ack(es[0], es.len() as u64);
                } else {
                    for &e in &es {
                        tr.ack(e, 1);
                    }
                }
                let mut o = self.owed.lock();
                if let Some((g, left, since)) = o.as_mut() {
                    for e in &es {
                        left.remove(e);
                    }
                    if left.is_empty() {
                        let ms = since.elapsed().as_millis() as u64;
                        if let Some(r) = self.rewinds.lock().iter_mut().find(|r| r.generation == *g) {
                            r.catch_up_ms = Some(ms);
                        }
                        tracing::info!(generation = *g, ms, "load: every event sent again after the recovery is acked");
                        *o = None;
                    }
                }
            }
        }
        drop(permit);
        if a.generation > self.client.generation() {
            self.rewinds_pending.fetch_add(1, Ordering::AcqRel);
            tokio::spawn(self.clone().rewind(a.generation));
        }
    }

    async fn rewind(self: Arc<Self>, g: u64) {
        let _one = self.rewinding.lock().await;
        self.clone().rewind_locked(g).await;
        self.rewinds_pending.fetch_sub(1, Ordering::AcqRel);
    }

    async fn rewind_locked(self: Arc<Self>, g: u64) {
        if self.client.generation() >= g {
            return;
        }
        let t = Instant::now();
        let (g2, cursors) = self.client.recovery_cursors(g).await;
        let cursors_ms = t.elapsed().as_millis() as u64;
        let (resend, acked_before) = {
            let mut tr = self.track.lock();
            let k = self.next_event.load(Ordering::Acquire);
            let r = tr.rewind(&cursors, g2, &self.run, self.hosts, k);
            // under the track lock: cursors computed from here on count
            // from the recovery's, and say so
            self.client.rewound(g2);
            *self.owed.lock() = Some((g2, r.0.iter().copied().collect(), Instant::now()));
            r
        };
        tracing::warn!(
            generation = g2,
            resend = resend.len(),
            acked_before,
            cursors_ms,
            "load: a bucket recovery: hosts re-read from its cursors"
        );
        self.rewinds.lock().push(Rewind {
            generation: g2,
            cursors_ms,
            resent: resend.len() as u64,
            acked_before,
            catch_up_ms: if resend.is_empty() { Some(0) } else { None },
            at_ms: now_us() / 1000,
        });
        if resend.is_empty() {
            *self.owed.lock() = None;
        }
        for chunk in resend.chunks(256) {
            let _ = self.resend_tx.send(chunk.to_vec());
        }
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
    let (resend_tx, mut resend_rx) = mpsc::unbounded_channel::<Vec<u64>>();
    let ctx = Arc::new(LoadCtx {
        client: client.clone(),
        hist: hist.clone(),
        window: Arc::new(Mutex::new(hdrhistogram::Histogram::<u64>::new_with_bounds(1, 120_000_000, 3)?)),
        ack_tx,
        acked: Arc::new(AtomicU64::new(0)),
        track: Arc::new(Mutex::new(Acked::default())),
        rewinds: Arc::default(),
        rewinding: Arc::default(),
        rewinds_pending: Arc::default(),
        owed: Arc::default(),
        resend_tx,
        run: a.run.clone(),
        hosts: a.hosts,
        pad: a.pad,
        next_event: Arc::new(AtomicU64::new(0)),
    });
    let t0 = Instant::now();
    let mut tick = tokio::time::interval(Duration::from_millis(a.tick_ms));
    tick.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Burst);
    let mut owed = 0.0f64;
    let mut last_report = Instant::now();
    let mut last_acked = 0u64;
    let mut tasks = tokio::task::JoinSet::new();
    let mut pending_cursors: Option<(Bytes, u64)> = None;
    let mut cursor_updates = 0u64;
    let mut last_cursors = Instant::now();
    let mut timeline = std::io::BufWriter::new(std::fs::File::create(format!("{}.timeline.jsonl", a.out))?);
    let mut stopping = false;
    loop {
        if !stopping && t0.elapsed() >= Duration::from_secs(a.duration) {
            stopping = true;
        }
        if stopping {
            // the generator stops; resends after a late recovery still go out
            while tasks.try_join_next().is_some() {}
            let idle =
                tasks.is_empty() && ctx.owed.lock().is_none() && ctx.rewinds_pending.load(Ordering::Acquire) == 0;
            match resend_rx.try_recv() {
                Ok(es) => {
                    let permit = slots.clone().acquire_owned().await?;
                    tasks.spawn(ctx.clone().send(es, Bytes::new(), client.generation(), permit));
                    continue;
                }
                Err(_) if idle => break,
                Err(_) => {
                    tokio::select! {
                        _ = tasks.join_next(), if !tasks.is_empty() => {}
                        _ = tokio::time::sleep(Duration::from_millis(20)) => {}
                    }
                    continue;
                }
            }
        }
        tick.tick().await;
        while let Ok(es) = resend_rx.try_recv() {
            let permit = slots.clone().acquire_owned().await?;
            tasks.spawn(ctx.clone().send(es, Bytes::new(), client.generation(), permit));
        }
        owed += a.rate * a.tick_ms as f64 / 1000.0;
        let n = owed.floor() as u64;
        owed -= n as f64;
        if n > 0 {
            let k0 = ctx.next_event.fetch_add(n, Ordering::AcqRel);
            submitted.fetch_add(n, Ordering::Relaxed);
            if last_cursors.elapsed() >= Duration::from_secs(1) {
                last_cursors = Instant::now();
                let tr = ctx.track.lock();
                pending_cursors = Some((encode_cursors(&tr.cursors(&a.run, a.hosts)), client.generation()));
                cursor_updates += 1;
            }
            let (cursors, cgen) = pending_cursors.take().unwrap_or((Bytes::new(), client.generation()));
            let permit = slots.clone().acquire_owned().await?;
            tasks.spawn(ctx.clone().send((k0..k0 + n).collect(), cursors, cgen, permit));
        }
        while tasks.try_join_next().is_some() {}
        if last_report.elapsed() >= Duration::from_secs(1) {
            {
                use std::io::Write;
                let mut w = ctx.window.lock();
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
            let n = ctx.acked.load(Ordering::Relaxed);
            let h = hist.lock();
            tracing::info!(
                acked_per_sec = (n - last_acked) as f64 / last_report.elapsed().as_secs_f64(),
                total = n,
                p50_us = h.value_at_quantile(0.5),
                p99_us = h.value_at_quantile(0.99),
                retries = client.retries.load(Ordering::Relaxed),
                generation = client.generation(),
                "load"
            );
            last_acked = n;
            last_report = Instant::now();
        }
    }
    while tasks.join_next().await.is_some() {}
    let events = ctx.next_event.load(Ordering::Acquire);
    let rewinds = ctx.rewinds.lock().clone();
    drop(ctx);
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
        run: a.run.clone(),
        hosts: a.hosts,
        events,
        rewinds,
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

/// (seq, did, sent) of a test frame, or for a relay frame (`relay`) its
/// seq and its bytes' id in place of a DID (every node emits the same bytes
/// for a seq), sent now.
fn parse_frame(b: &[u8], relay: bool) -> Option<(u64, String, i64)> {
    if !relay {
        return parse_test_frame(b);
    }
    use vlsync_atproto::cbor::ValueRef;
    let (hdr, n) = ValueRef::decode_prefix(b).ok()?;
    if !matches!(hdr.get("op"), Some(ValueRef::Int(1))) {
        return None;
    }
    let seq = match ValueRef::decode(&b[n..]).ok()?.get("seq") {
        Some(ValueRef::Int(i)) if *i > 0 => *i as u64,
        _ => return None,
    };
    Some((seq, format!("{:016x}", content_id(b)), now_us()))
}

async fn consume(
    node: String,
    addr: String,
    tx: mpsc::UnboundedSender<Seen>,
    cursor: Arc<AtomicU64>,
    stop: Arc<std::sync::atomic::AtomicBool>,
    relay: bool,
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
            if let Some((seq, did, sent)) = parse_frame(&b, relay) {
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
async fn backfill_from_zero(addr: &str, upto: u64, ck: &mut Checker, relay: bool) -> anyhow::Result<u64> {
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
        if let Some((seq, did, _)) = parse_frame(&b, relay) {
            let d = content_id(did.as_bytes());
            ck.observe_event("backfill", seq, d, d);
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
        tokio::spawn(consume(id.clone(), addr.clone(), tx.clone(), c, stop.clone(), a.relay_frames));
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
    // the leader's member set at the end; every node until it's known
    let mut members: Vec<String> = nodes.iter().map(|(id, _)| id.clone()).collect();
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
                        let d = content_id(did.as_bytes());
                        ck.observe_event(&node, seq, d, d);
                        let lat = (at - sent).max(1) as u64;
                        let _ = by_node.entry(node).or_insert_with(mk).record(lat);
                        if seq > top {
                            let _ = first.record(lat);
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
                    // every member's commit is the same and each consumer has
                    // it (a removed node, or one never added, stops where it
                    // stopped)
                    let mut sts = Vec::new();
                    for (id, addr) in &nodes {
                        let s: Option<serde_json::Value> = match http.get(format!("http://{addr}/qlog/status")).timeout(Duration::from_secs(1)).send().await {
                            Ok(r) => r.json().await.ok(),
                            Err(_) => None,
                        };
                        sts.push((id.clone(), s));
                    }
                    let leader = sts
                        .iter()
                        .filter_map(|(_, s)| s.as_ref())
                        .filter(|s| s["role"] == "leader")
                        .max_by_key(|s| s["epoch"].as_u64().unwrap_or(0));
                    if let Some(l) = leader {
                        members = l["members"]
                            .as_array()
                            .map(|v| v.iter().filter_map(|x| x.as_str().map(String::from)).collect())
                            .unwrap_or_default();
                    }
                    let commits: Vec<Option<u64>> = sts
                        .iter()
                        .filter(|(id, _)| members.contains(id))
                        .map(|(_, s)| s.as_ref().and_then(|v| v["commit"].as_u64()))
                        .collect();
                    let done = leader.is_some()
                        && !commits.is_empty()
                        && commits.iter().all(|c| c.is_some() && *c == commits[0])
                        && nodes.iter().filter(|(id, _)| members.contains(id)).all(|(id, _)| ck.last(id) == commits[0]);
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
        && let Some((id, addr)) = nodes.iter().find(|(id, _)| members.contains(id))
        && let Some(upto) = ck.last(id)
    {
        let t = Instant::now();
        let r = backfill_from_zero(addr, upto, &mut ck, a.relay_frames).await;
        tracing::info!(node = %id, upto, secs = t.elapsed().as_secs_f64(), "check: consumer from cursor 0 done: {r:?}");
        match r {
            Ok(n) => backfilled = Some((n, t.elapsed().as_secs_f64())),
            Err(e) => {
                ck.violations += 1;
                ck.messages.push(format!("the consumer from cursor 0 on {id} failed: {e:#}"));
            }
        }
    }
    let acked = match &a.acked {
        Some(f) => std::fs::read_to_string(f)?,
        None => String::new(),
    };
    for line in acked.lines() {
        // a load generator killed mid-write leaves a last line without its did
        if let Some((s, d)) = line.split_once(' ')
            && !d.is_empty()
        {
            let d = content_id(d.as_bytes());
            ck.acked_event(s.parse()?, d, d);
        }
    }
    let last_by_node: HashMap<String, u64> =
        nodes.iter().map(|(id, _)| (id.clone(), ck.last(id).unwrap_or(0))).collect();
    for (id, l) in &last_by_node {
        if members.contains(id) && *l != ck.max_seq {
            ck.violations += 1;
            ck.messages.push(format!("{id}'s consumer ended at {l}, below the highest emitted seq {}", ck.max_seq));
        }
    }
    let gaps = match (&a.s3_endpoint, &a.prefix) {
        (Some(e), Some(p)) => {
            let s3 = S3Args {
                s3_endpoint: e.clone(),
                s3_bucket: a.s3_bucket.clone(),
                s3_access_key: std::env::var("QLOG_S3_ACCESS_KEY").unwrap_or_else(|_| "minioadmin".into()),
                s3_secret_key: std::env::var("QLOG_S3_SECRET_KEY").unwrap_or_else(|_| "minioadmin".into()),
                prefix: p.clone(),
            };
            let store = vlrelay::qlog::bucket::counted(&s3.store()?, "tool");
            vlrelay::qlog::budget::start(&a.budget, store.clone())?;
            vlrelay::qlog::flush::read_manifest(&store).await?.map(|(m, _)| m.gaps).unwrap_or_default()
        }
        _ => Vec::new(),
    };
    let expected: Option<Vec<u64>> = match &a.load_summary {
        Some(f) => {
            let v: serde_json::Value = serde_json::from_slice(&std::fs::read(f)?)?;
            let (run, hosts, events) = (
                v["run"].as_str().unwrap_or("r").to_string(),
                v["hosts"].as_u64().unwrap_or(64),
                v["events"].as_u64().unwrap_or(0),
            );
            Some((0..events).map(|e| content_id(load_did(&run, hosts, e).as_bytes())).collect())
        }
        None => None,
    };
    let report = ck.finish_with(&[], &gaps, expected.as_deref());
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
        "check: {} observed={} distinct={} max_seq={} acked={} acked_missing={} violations={} holes={} skipped={} gaps={} jumped={} reingested={} duplicates={} events_lost={}",
        out.verdict,
        out.report.observed,
        out.report.distinct_seqs,
        out.report.max_seq,
        out.report.acked,
        out.report.acked_missing,
        out.report.violations,
        out.report.holes,
        out.report.skipped_with_notice,
        gaps.len(),
        out.report.jumped,
        out.report.reingested,
        out.report.duplicates,
        out.report.events_lost
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
    let store = vlrelay::qlog::bucket::counted(&a.s3.store()?, "tool");
    vlrelay::qlog::budget::start(&a.budget, store.clone())?;
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

async fn retain(a: RetainArgs) -> anyhow::Result<()> {
    use vlrelay::qlog::retain;
    let store = vlrelay::qlog::bucket::counted(&a.s3.store()?, "retain");
    vlrelay::qlog::budget::start(&a.budget, store.clone())?;
    let Some(plan) = retain::plan(&store, Duration::from_secs(a.horizon_secs)).await? else {
        println!("no manifest");
        return Ok(());
    };
    if a.dry_run {
        println!("{}", serde_json::to_string_pretty(&plan)?);
    } else {
        let applied = if a.apply { Some(retain::apply(&store, &plan).await?) } else { None };
        let r = retain::write(&store, plan, applied).await?;
        println!("{}", serde_json::to_string_pretty(&r)?);
    }
    Ok(())
}

async fn member(a: MemberArgs) -> anyhow::Result<()> {
    let nodes = pairs(&a.nodes)?;
    let addrs: std::collections::BTreeMap<String, String> = pairs(&a.addrs)?.into_iter().collect();
    let http = reqwest::Client::new();
    let deadline = Instant::now() + Duration::from_secs(a.retry_secs);
    let t0 = Instant::now();
    let mut target: Option<Vec<String>> = None;
    let mut attempts = 0u32;
    loop {
        let mut leader: Option<(u64, String, Vec<String>)> = None;
        for (_, addr) in &nodes {
            let Ok(r) = http.get(format!("http://{addr}/qlog/status")).timeout(Duration::from_secs(1)).send().await
            else {
                continue;
            };
            let Ok(v) = r.json::<serde_json::Value>().await else { continue };
            let epoch = v["epoch"].as_u64().unwrap_or(0);
            if v["role"] == "leader" && leader.as_ref().is_none_or(|l| l.0 < epoch) {
                let m = v["members"]
                    .as_array()
                    .map(|v| v.iter().filter_map(|x| x.as_str().map(String::from)).collect())
                    .unwrap_or_default();
                leader = Some((epoch, addr.clone(), m));
            }
        }
        if let Some((epoch, addr, current)) = &leader {
            // the target is fixed from the first member set seen, so a retry
            // after a change that landed is a no-op
            let want = match &target {
                Some(t) => t.clone(),
                None => {
                    let mut t = current.clone();
                    match (a.op.as_str(), a.ids.as_slice()) {
                        ("add", [id]) => t.push(id.clone()),
                        ("remove", [id]) => t.retain(|m| m != id),
                        ("replace", [old, new]) => {
                            t.retain(|m| m != old);
                            t.push(new.clone());
                        }
                        ("set", ids) if !ids.is_empty() => {
                            t = ids.iter().flat_map(|s| s.split(',')).map(String::from).collect();
                        }
                        _ => anyhow::bail!("want: add ID | remove ID | replace OLD NEW | set ID,ID,..."),
                    }
                    t.sort();
                    t.dedup();
                    target = Some(t.clone());
                    t
                }
            };
            if *current == want {
                println!(
                    "{}",
                    serde_json::json!({ "done": true, "epoch": epoch, "members": current, "attempts": attempts, "ms": t0.elapsed().as_millis() as u64 })
                );
                return Ok(());
            }
            attempts += 1;
            let body = vlrelay::qlog::emit::MembersRequest { members: want.clone(), addrs: addrs.clone() };
            match {
                let mut req = http.post(format!("http://{addr}/qlog/members")).json(&body);
                if let Some(t) = &a.admin_token {
                    req = req.bearer_auth(t);
                }
                req
            }
            .send()
            .await
            {
                Ok(r) => {
                    let (ok, code) = (r.status().is_success(), r.status());
                    let v: serde_json::Value = r.json().await.unwrap_or_default();
                    // no retry fixes a missing or wrong token
                    anyhow::ensure!(code != reqwest::StatusCode::UNAUTHORIZED, "member: {addr}: {v}");
                    if ok {
                        println!("{}", serde_json::json!({ "switch": v, "attempts": attempts }));
                    } else {
                        eprintln!("member: {addr}: {v}");
                    }
                }
                Err(e) => eprintln!("member: {addr}: {e}"),
            }
        }
        anyhow::ensure!(Instant::now() < deadline, "member: no change to {target:?} within {} s", a.retry_secs);
        tokio::time::sleep(Duration::from_millis(500)).await;
    }
}
