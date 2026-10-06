//! vlRelay: one node of a relay on the quorum log (docs/cluster.md;
//! with no peers, a single node).

use axum::http::{HeaderValue, header};
use axum::middleware;
use axum::response::Response;
use clap::{CommandFactory, FromArgMatches, Parser};
use std::net::SocketAddr;
use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;
use vlrelay::node::{Node, NodeConfig, admin::NodeAdmin};

#[global_allocator]
static GLOBAL: tikv_jemallocator::Jemalloc = tikv_jemallocator::Jemalloc;

/// Segments are ~8 MiB, jemalloc's default oversize threshold: allocations
/// that size get an arena that returns pages to the OS as soon as they're
/// freed, so every segment buffer and its compressed copy were faulted in
/// afresh (~3% of a loaded node's CPU). Without the oversize arena they
/// reuse dirty pages within the normal decay time.
#[unsafe(export_name = "_rjem_malloc_conf")]
pub static MALLOC_CONF: &[u8; 21] = b"oversize_threshold:0\0";

#[derive(Parser, Debug)]
#[command(version, about = "An atproto relay whose only durable state is an object store")]
struct Args {
    /// Serves subscribeRepos, the sync API, requestCrawl, /admin and /metrics.
    #[arg(long, default_value = "127.0.0.1:2980", env = "VLRELAY_LISTEN")]
    listen: SocketAddr,
    /// Everything in memory: nothing survives a restart.
    #[arg(long)]
    memory: bool,
    #[arg(long, env = "VLRELAY_S3_ENDPOINT")]
    s3_endpoint: Option<String>,
    #[arg(long, env = "VLRELAY_S3_BUCKET")]
    s3_bucket: Option<String>,
    #[arg(long, env = "VLRELAY_S3_ACCESS_KEY")]
    s3_access_key: Option<String>,
    #[arg(long, env = "VLRELAY_S3_SECRET_KEY", hide_env_values = true)]
    s3_secret_key: Option<String>,
    #[arg(long, default_value = "auto", env = "VLRELAY_S3_REGION")]
    s3_region: String,
    /// Send PUT bodies as SigV4 UNSIGNED-PAYLOAD instead of hashing each
    /// one (default: on for an https endpoint, where TLS covers the body).
    #[arg(long, env = "VLRELAY_S3_UNSIGNED_PAYLOAD")]
    s3_unsigned_payload: Option<bool>,
    /// Key prefix in the bucket: one relay per prefix.
    #[arg(long, default_value = "vlrelay", env = "VLRELAY_PREFIX")]
    prefix: String,
    #[arg(long, default_value = "https://plc.directory", env = "VLRELAY_PLC_URL")]
    plc_url: String,
    /// zstd level for log segments: 0 stores them uncompressed, negative
    /// levels are zstd's fast ones. Firehose frames are mostly hashes:
    /// on production frames -1 compresses 1.8x faster than 1 for 0.6% more
    /// bytes (docs/perf.md, "Compression").
    #[arg(long, default_value_t = -1, allow_negative_numbers = true)]
    log_compression: i32,
    /// An upstream to subscribe to (repeatable). `http://` means plain
    /// `ws://` (dev mode); a bare hostname means `wss://`.
    #[arg(long = "host")]
    hosts: Vec<String>,
    /// Accept com.atproto.sync.requestCrawl.
    #[arg(long)]
    crawl: bool,
    /// The tier a --host upstream starts at the first time it's seen.
    /// After that its record's tier holds (operators, auto-throttle).
    #[arg(long, default_value = "trusted")]
    host_tier: String,
    /// Turns on /admin (dashboard and API) with this token.
    #[arg(long, env = "VLRELAY_ADMIN_TOKEN", hide_env_values = true)]
    admin_token: Option<String>,
    /// A built dashboard (`ui/dist`); default: this tree's, if built.
    #[arg(long)]
    ui_dir: Option<PathBuf>,
    /// Proxies whose `X-Forwarded-For` names the client (CIDRs, repeatable
    /// or comma-separated): per-IP limits key on its rightmost address that
    /// isn't one of these. Other peers' headers are ignored.
    #[arg(long = "trusted-proxy", env = "VLRELAY_TRUSTED_PROXIES", value_delimiter = ',')]
    trusted_proxies: Vec<vlrelay::serve::Cidr>,
    /// Allows plain ws://, IPs, localhost and ports for upstreams and DID
    /// documents. Implied by an http:// --host or a loopback --plc-url.
    #[arg(long)]
    dev_mode: bool,
    /// Dev mode only: how far a live consumer may fall behind before
    /// `ConsumerTooSlow`, in MiB (default 128).
    #[arg(long)]
    max_lag_mb: Option<usize>,
    /// The firehose's in-memory ring of recent events, in MiB (default
    /// 512); older cursors read the node's log, then the bucket.
    #[arg(long)]
    ring_mb: Option<usize>,
    /// Pipeline lanes; a DID always maps to the same one.
    #[arg(long, default_value_t = 64)]
    lanes: usize,
    /// Threads verifying events (default: the core count, at most 16).
    #[arg(long)]
    ingest_threads: Option<usize>,
    /// Upstream frames one host may have read and not yet durable; past it
    /// (or its MB cap) the host's socket isn't read.
    #[arg(long, default_value_t = 8192)]
    host_inflight_events: usize,
    /// The same cap in bytes.
    #[arg(long, default_value_t = 64)]
    host_inflight_mb: usize,
    /// The same over every host together.
    #[arg(long, default_value_t = 32768)]
    inflight_events: usize,
    /// The same cap over every host, in bytes.
    #[arg(long, default_value_t = 384)]
    inflight_mb: usize,
    /// DID document fetches per second, all DIDs together.
    #[arg(long, default_value_t = 50.0)]
    did_lookups_per_sec: f64,
    /// The member's name in the quorum log.
    #[arg(long, default_value = "relay", env = "VLRELAY_NODE_ID")]
    node_id: String,
    #[command(flatten)]
    quorum: QuorumArgs,
}

/// The quorum log (docs/quorum.md): one node of a cluster of any size (one
/// is a single node with the commitlog as its WAL).
#[derive(clap::Args, Debug)]
struct QuorumArgs {
    /// The peer protocol: replication, submits, members' questions.
    #[arg(long, default_value = "127.0.0.1:2978", env = "VLRELAY_QLOG_LISTEN")]
    qlog_listen: String,
    /// Another node: `id=host:port` of its --qlog-listen (repeatable, or
    /// comma-separated).
    #[arg(long = "qlog-peer", env = "VLRELAY_QLOG_PEERS", value_delimiter = ',')]
    qlog_peers: Vec<String>,
    /// The bootstrap member set (default: this node and its peers); after
    /// the first start, `qlog/leader` holds it.
    #[arg(long, env = "VLRELAY_QLOG_MEMBERS", value_delimiter = ',')]
    qlog_members: Vec<String>,
    /// The commitlog's directory (NVMe). Without one the log is memory only.
    #[arg(long, env = "VLRELAY_QLOG_DIR")]
    qlog_dir: Option<PathBuf>,
    /// The bucket flush interval.
    #[arg(long, default_value_t = 30_000)]
    qlog_flush_ms: u64,
    /// Seqs reserved past each flush (R = F + H).
    #[arg(long, default_value_t = 8_640_000)]
    qlog_headroom: u64,
    /// Bearer token membership changes need (`qlog member`, the dashboard).
    #[arg(long, env = "QLOG_ADMIN_TOKEN", hide_env_values = true)]
    qlog_admin_token: Option<String>,
    /// Bucket retention, run by the leader: segments older than this go
    /// (0: never).
    #[arg(long, default_value_t = 72)]
    qlog_retain_hours: u64,
    /// Dev: retention in seconds instead.
    #[arg(long)]
    qlog_retain_secs: Option<u64>,
    /// How often the leader runs a retention pass.
    #[arg(long, default_value_t = 600)]
    qlog_retain_every_secs: u64,
    /// A member silent this long loses its hosts to the others.
    #[arg(long, default_value_t = 2_000)]
    qlog_host_failover_ms: u64,
    /// How often a member reads the host table and the hosts' cursors from the leader.
    #[arg(long, default_value_t = 500)]
    qlog_host_poll_ms: u64,
    /// Silence from the leader that starts an election.
    #[arg(long, default_value_t = 1_000)]
    qlog_election_ms: u64,
    /// How often the leader heartbeats its followers.
    #[arg(long, default_value_t = 100)]
    qlog_heartbeat_ms: u64,
    /// The state's SlateDB compactor and worker poll.
    #[arg(long, default_value_t = 30_000)]
    qlog_state_compactor_poll_ms: u64,
    /// A lost quorum waits for an operator instead of recovering from the
    /// bucket.
    #[arg(long)]
    qlog_no_auto_recover: bool,
    /// Commitlog file size on the local disk.
    #[arg(long, default_value_t = 64)]
    qlog_segment_mb: u64,
    /// Flushed commitlog kept on the local disk, for followers catching up.
    #[arg(long, default_value_t = 4096)]
    qlog_disk_retain_mb: u64,
    /// Committed log kept in memory (default 64 with --qlog-dir, else 512).
    #[arg(long)]
    qlog_memory_mb: Option<usize>,
    /// Chaos: kill -9 at this flush step (or `any`), with --qlog-crash-prob.
    #[arg(long)]
    qlog_crash_at: Option<String>,
    /// Chaos: the chance of the crash at each step.
    #[arg(long, default_value_t = 0.05)]
    qlog_crash_prob: f64,
    /// Chaos: no crash injected once this file exists.
    #[arg(long)]
    qlog_crash_stop_file: Option<PathBuf>,
    /// Chaos: SIGUSR1 is a power cut.
    #[arg(long)]
    qlog_power_cut_on_usr1: bool,
    /// Chaos: sleep this long before each commitlog fsync (emulates a disk).
    #[arg(long)]
    qlog_fsync_delay_us: Option<u64>,
}

fn quorum_setup(q: &QuorumArgs, node_id: &str) -> anyhow::Result<vlrelay::node::quorum::QuorumSetup> {
    let mut s = vlrelay::node::quorum::QuorumSetup::new(&q.qlog_listen);
    for p in &q.qlog_peers {
        let (id, addr) = p.split_once('=').ok_or_else(|| anyhow::anyhow!("--qlog-peer {p}: want id=host:port"))?;
        anyhow::ensure!(id != node_id, "--qlog-peer {p} names this node");
        s.peers.insert(id.to_string(), addr.to_string());
    }
    s.members = q.qlog_members.clone();
    s.commitlog = q.qlog_dir.clone();
    s.flush = Duration::from_millis(q.qlog_flush_ms.max(100));
    s.headroom = q.qlog_headroom.max(1);
    s.admin_token = q.qlog_admin_token.clone().filter(|t| !t.is_empty());
    s.retain_horizon = match (q.qlog_retain_secs, q.qlog_retain_hours) {
        (Some(secs), _) => Some(Duration::from_secs(secs.max(1))),
        (None, 0) => None,
        (None, h) => Some(Duration::from_secs(h * 3600)),
    };
    s.retain_every = Duration::from_secs(q.qlog_retain_every_secs.max(1));
    s.host_failover = Duration::from_millis(q.qlog_host_failover_ms.max(100));
    s.host_poll = Duration::from_millis(q.qlog_host_poll_ms.max(50));
    s.election_timeout = Duration::from_millis(q.qlog_election_ms.max(100));
    s.heartbeat = Duration::from_millis(q.qlog_heartbeat_ms.max(10));
    s.auto_recover = !q.qlog_no_auto_recover;
    s.commitlog_segment_bytes = q.qlog_segment_mb.max(1) << 20;
    s.disk_retain_bytes = q.qlog_disk_retain_mb.max(1) << 20;
    s.memory_bytes = q.qlog_memory_mb.map(|m| m.max(1) << 20);
    s.power_cut_on_usr1 = q.qlog_power_cut_on_usr1;
    s.fsync_delay = q.qlog_fsync_delay_us.map(Duration::from_micros);
    vlrelay::qlog::state::set_compactor_poll(Duration::from_millis(q.qlog_state_compactor_poll_ms));
    if let Some(at) = q.qlog_crash_at.clone().filter(|a| !a.is_empty()) {
        use vlrelay::qlog::flush::Step;
        let only: Option<Step> = if at == "any" { None } else { Some(at.parse().map_err(anyhow::Error::msg)?) };
        let (prob, stop) = (q.qlog_crash_prob, q.qlog_crash_stop_file.clone());
        s.crash = Some(Arc::new(move |step| {
            use rand::Rng;
            if only.is_none_or(|o| o == step)
                && stop.as_ref().is_none_or(|f| !f.exists())
                && rand::thread_rng().gen_bool(prob)
            {
                eprintln!("vlrelay: crash injected at {step:?}");
                // as sudden as a crash: no unwinding, no flush of anything
                unsafe { libc::kill(libc::getpid(), libc::SIGKILL) };
            }
            false
        }));
    }
    Ok(s)
}

fn main() {
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env().unwrap_or_else(|_| "info,slatedb=warn".into()),
        )
        .init();
    // reqwest, tungstenite and object_store each pull rustls; with more than
    // one provider compiled in, nothing picks one unless we do
    let _ = rustls::crypto::ring::default_provider().install_default();
    let cmd = Args::command();
    let matches = cmd.clone().get_matches();
    let args = Args::from_arg_matches(&matches).unwrap_or_else(|e| e.exit());
    let settings = vlrelay::admin::settings::from_clap(&cmd, &matches);
    let rt = tokio::runtime::Builder::new_multi_thread().enable_all().thread_name("main").build().expect("runtime");
    let code = match rt.block_on(run(args, settings)) {
        Ok(()) => 0,
        Err(e) => {
            tracing::error!("{e:#}");
            1
        }
    };
    std::process::exit(code);
}

async fn run(a: Args, settings: vlrelay::admin::SettingsView) -> anyhow::Result<()> {
    let loopback_plc = a.plc_url.contains("://127.") || a.plc_url.contains("://localhost");
    let dev_mode = a.dev_mode || a.hosts.iter().any(|h| h.starts_with("http://")) || loopback_plc;
    let store = if a.memory {
        vlpds::store::Store::memory(None)
    } else {
        let need =
            |v: &Option<String>, f: &str| v.clone().ok_or_else(|| anyhow::anyhow!("{f} is required without --memory"));
        let cfg = vlpds::store::S3Config {
            endpoint: need(&a.s3_endpoint, "--s3-endpoint")?,
            bucket: need(&a.s3_bucket, "--s3-bucket")?,
            access_key: need(&a.s3_access_key, "--s3-access-key")?,
            secret_key: need(&a.s3_secret_key, "--s3-secret-key")?,
            region: a.s3_region.clone(),
        };
        let unsigned = a.s3_unsigned_payload.unwrap_or(cfg.endpoint.starts_with("https://"));
        vlpds::store::Store::s3_with(&cfg, &a.prefix, None, 256, unsigned)?
    };

    let mut cfg = NodeConfig::new(&a.plc_url);
    cfg.node_id = a.node_id.clone();
    cfg.dev_mode = dev_mode;
    vlpds::segment::set_compression_level(a.log_compression);
    if a.max_lag_mb.is_some() && !dev_mode {
        anyhow::bail!("--max-lag-mb is for dev networks (--dev-mode)");
    }
    cfg.max_lag_bytes = a.max_lag_mb.map(|mb| mb.max(1) << 20);
    cfg.ring_bytes = a.ring_mb.map(|mb| mb.max(1) << 20);
    cfg.lanes = a.lanes.max(1);
    if let Some(n) = a.ingest_threads {
        cfg.ingest_threads = n.max(1);
    }
    cfg.inflight = vlrelay::upstream::flow::FlowLimits {
        host_events: a.host_inflight_events.max(1),
        host_bytes: a.host_inflight_mb.max(1) << 20,
        events: a.inflight_events.max(1),
        bytes: a.inflight_mb.max(1) << 20,
    };
    cfg.hosts = a.hosts.clone();
    cfg.cli_host_tier = vlrelay::upstream::Tier::parse(&a.host_tier)
        .filter(|t| t.connects())
        .ok_or_else(|| anyhow::anyhow!("--host-tier {}: one of trusted, default, new, throttled", a.host_tier))?;
    // the leader decides every new account, so its budgets are the cluster's
    let live: Arc<dyn vlrelay::policy::LiveNodes> = Arc::new(vlrelay::policy::FixedNodes::new(1));
    cfg.policy =
        Some(vlrelay::node::policy::PolicyEngine(vlrelay::policy::Engine::new(store.clone(), &a.node_id, live)));
    cfg.identity.lookups_per_sec = a.did_lookups_per_sec;
    cfg.identity.burst = (a.did_lookups_per_sec * 2.0).max(1.0);
    if dev_mode {
        // every DID in a dev network is new, and PLC is local
        cfg.identity.lookups_per_sec = cfg.identity.lookups_per_sec.max(1000.0);
        cfg.identity.burst = cfg.identity.burst.max(1000.0);
    }
    let q = quorum_setup(&a.quorum, &a.node_id)?;
    let node = Node::start(store, cfg, q).await?;

    let admin = vlrelay::qlog::emit::Admin::for_listener(a.quorum.qlog_admin_token.clone(), a.listen);
    let mut app = axum::Router::new()
        .route("/xrpc/_health", axum::routing::get(health))
        .route("/metrics", axum::routing::get(|| async { vlpds::metrics::render() }))
        .merge(node.serve.router())
        .merge(vlrelay::qlog::emit::control_router(node.quorum.qnode.clone(), admin))
        .merge(vlrelay::sync_api::router(Arc::new(vlrelay::node::quorum::QuorumSync {
            state: node.state.clone(),
            hosts: node.quorum.hosts.clone(),
        })));
    if a.crawl {
        app = app.merge(node.crawler.router());
    }
    let token = a.admin_token.clone().filter(|t| !t.is_empty());
    // always built: the public page's stats come from it
    let admin_src = {
        let policy = node.policy.clone().expect("the relay always runs the policy engine");
        Arc::new(NodeAdmin::new(node.clone(), policy).with_settings(settings))
    };
    let ui = Arc::new(vlrelay::admin::UiFiles::load(a.ui_dir.as_deref())?);
    if let Some(token) = token {
        app = app.merge(vlrelay::admin::app(admin_src.clone(), token, ui));
    } else {
        app = app.merge(vlrelay::admin::docs_routes(ui)).merge(vlrelay::admin::public_routes(admin_src.clone()));
    }
    let app = with_real_ip(app.layer(middleware::map_response(server_header)), &a.trusted_proxies);

    let listener = tokio::net::TcpListener::bind(a.listen).await?;
    tracing::info!(addr = %a.listen, node = %a.node_id, dev_mode, "vlrelay listening");
    // Consumers (a reconnect storm's accepts and upgrades) are served on the
    // subscriber runtime, so they can't starve the pipeline and the quorum
    // log on this one. The tokio listener must be registered there too.
    let listener = listener.into_std()?;
    let server = vlpds::firehose::runtime(node.cfg.serve_threads).spawn(async move {
        let listener = tokio::net::TcpListener::from_std(listener)?;
        axum::serve(listener, app.into_make_service_with_connect_info::<SocketAddr>()).await
    });
    signal().await;
    tracing::info!("shutting down");
    let r = node.shutdown().await;
    server.abort();
    r
}

async fn signal() {
    let mut term = tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate()).expect("SIGTERM");
    tokio::select! {
        _ = tokio::signal::ctrl_c() => {}
        _ = term.recv() => {}
    }
}

async fn health() -> axum::Json<serde_json::Value> {
    axum::Json(serde_json::json!({ "version": env!("CARGO_PKG_VERSION") }))
}

/// Other relays refuse to subscribe to anything that says it's a relay.
async fn server_header(mut r: Response) -> Response {
    r.headers_mut().insert(
        header::SERVER,
        HeaderValue::from_static(concat!("vlrelay/", env!("CARGO_PKG_VERSION"), " (atproto-relay)")),
    );
    r
}

fn with_real_ip(app: axum::Router, trusted: &[vlrelay::serve::Cidr]) -> axum::Router {
    if trusted.is_empty() {
        return app;
    }
    app.layer(middleware::from_fn_with_state(Arc::new(trusted.to_vec()), vlrelay::serve::real_ip))
}
