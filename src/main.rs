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
    #[arg(long, env = "VLRELAY_S3_ACCESS_KEY", hide_env_values = true)]
    s3_access_key: Option<String>,
    /// --s3-access-key from a file, less one trailing newline.
    #[arg(long, env = "VLRELAY_S3_ACCESS_KEY_FILE", conflicts_with = "s3_access_key")]
    s3_access_key_file: Option<PathBuf>,
    #[arg(long, env = "VLRELAY_S3_SECRET_KEY", hide_env_values = true)]
    s3_secret_key: Option<String>,
    /// --s3-secret-key from a file, less one trailing newline.
    #[arg(long, env = "VLRELAY_S3_SECRET_KEY_FILE", conflicts_with = "s3_secret_key")]
    s3_secret_key_file: Option<PathBuf>,
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
    /// Seed DID documents in bulk from the PLC directory's /export: the
    /// quorum log's leader reads the history, then follows the tail, into a
    /// database in the bucket every member reads, so a cold relay doesn't
    /// resolve each account.
    #[arg(long, env = "VLRELAY_PLC_EXPORT", value_parser = clap::builder::BoolishValueParser::new())]
    plc_export: bool,
    /// The directory --plc-export reads (default: --plc-url).
    #[arg(long, env = "VLRELAY_PLC_EXPORT_URL")]
    plc_export_url: Option<String>,
    /// /export requests per second, all streams together (a 429 waits out
    /// its Retry-After on top).
    #[arg(long, default_value_t = 2.0)]
    plc_export_rate: f64,
    /// Time windows of the export read side by side on a fresh start.
    #[arg(long, default_value_t = 4)]
    plc_export_streams: usize,
    /// A relay whose com.atproto.sync.listHosts seeds host discovery (read
    /// only; repeatable): added to the policy's discovery.seedRelays when
    /// it has none yet. The dashboard edits the list after that.
    #[arg(long = "bootstrap-relay", env = "VLRELAY_BOOTSTRAP_RELAYS", value_delimiter = ',')]
    bootstrap_relays: Vec<String>,
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
    /// --admin-token from a file, less one trailing newline.
    #[arg(long, env = "VLRELAY_ADMIN_TOKEN_FILE", conflicts_with = "admin_token")]
    admin_token_file: Option<PathBuf>,
    /// The admin listener: everything --listen serves, and the only listener
    /// that reads --admin-proxy-header. For operators only: never route
    /// public traffic to it. Unset: none.
    #[arg(long, env = "VLRELAY_ADMIN_LISTEN")]
    admin_listen: Option<SocketAddr>,
    /// Header naming the operator, set by the proxy in front of
    /// --admin-listen (e.g. Tailscale-User-Login). Taken only from
    /// --admin-proxy-from peers, only for --admin-operators logins, and never
    /// on --listen; the admin token works as before. Unset: token only.
    #[arg(long, env = "VLRELAY_ADMIN_PROXY_HEADER", requires_all = ["admin_listen", "admin_proxy_from", "admin_operators"])]
    admin_proxy_header: Option<String>,
    /// The proxy's addresses as --admin-listen sees them (the TCP peer, never
    /// X-Forwarded-For; IPs or CIDRs, comma-separated; name the proxy's own
    /// /32). --admin-proxy-header from anywhere else is ignored.
    #[arg(long, env = "VLRELAY_ADMIN_PROXY_FROM", value_delimiter = ',', requires = "admin_proxy_header")]
    admin_proxy_from: Vec<vlrelay::serve::Cidr>,
    /// Logins (comma-separated, exactly as the proxy sends them) let in by
    /// --admin-proxy-header; the audit trail names them.
    #[arg(long, env = "VLRELAY_ADMIN_OPERATORS", value_delimiter = ',', requires = "admin_proxy_header")]
    admin_operators: Vec<String>,
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
    /// --qlog-admin-token from a file, less one trailing newline.
    #[arg(long, env = "QLOG_ADMIN_TOKEN_FILE", conflicts_with = "qlog_admin_token")]
    qlog_admin_token_file: Option<PathBuf>,
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
    /// When an entry counts on this node: `fsync` (after its fdatasync),
    /// `page-cache` (once written to the commitlog, fdatasync'd every
    /// --durability-sync-ms; a power cut on a majority within that window
    /// is a bucket recovery) or `memory` (no commitlog). Default:
    /// page-cache for three members or more, fsync below; a single node
    /// only runs fsync.
    #[arg(long, env = "VLRELAY_DURABILITY")]
    durability: Option<String>,
    /// Page-cache mode's background fdatasync interval.
    #[arg(long, default_value_t = 100)]
    durability_sync_ms: u64,
    /// Mutation tests only: trust the commitlog after a power loss in
    /// page-cache mode (the check the chaos must catch it without).
    #[arg(long)]
    qlog_unsafe_trust_log: bool,
    /// SlateDB's block and metadata cache, shared by every database this
    /// node opens (the quorum log's state, the PLC seeds): the total in
    /// MiB, four parts blocks to one of indexes and filters.
    #[arg(long, env = "VLRELAY_SLATEDB_CACHE_MB", default_value_t = vlrelay::qlog::cache::DEFAULT_MB)]
    slatedb_cache_mb: u64,
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
    let members = if s.members.is_empty() { s.peers.len() + 1 } else { s.members.len() };
    s.durability = vlrelay::qlog::commitlog::DurabilityMode::choose(
        q.durability.as_deref(),
        Duration::from_millis(q.durability_sync_ms),
        members,
    )?;
    s.trust_after_power_loss = q.qlog_unsafe_trust_log;
    s.fsync_delay = q.qlog_fsync_delay_us.map(Duration::from_micros);
    vlrelay::qlog::state::set_compactor_poll(Duration::from_millis(q.qlog_state_compactor_poll_ms));
    vlrelay::qlog::cache::configure(q.slatedb_cache_mb);
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

/// Fills each secret given as a file (clap's `conflicts_with` keeps a secret
/// and its file from both being set). Errors name the flag and the path,
/// never the contents.
fn read_secret_files(a: &mut Args) -> anyhow::Result<()> {
    use vlpds::secret_file::resolve;
    resolve("s3-access-key-file", &a.s3_access_key_file, &mut a.s3_access_key)?;
    resolve("s3-secret-key-file", &a.s3_secret_key_file, &mut a.s3_secret_key)?;
    resolve("admin-token-file", &a.admin_token_file, &mut a.admin_token)?;
    resolve("qlog-admin-token-file", &a.quorum.qlog_admin_token_file, &mut a.quorum.qlog_admin_token)?;
    Ok(())
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

async fn run(mut a: Args, settings: vlrelay::admin::SettingsView) -> anyhow::Result<()> {
    read_secret_files(&mut a)?;
    let admin_proxy = admin_proxy(&a)?;
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
    let mut q = quorum_setup(&a.quorum, &a.node_id)?;
    if a.plc_export {
        let mut pc = vlrelay::plc_seed::ingest::Config::new(a.plc_export_url.as_deref().unwrap_or(&a.plc_url));
        anyhow::ensure!(a.plc_export_rate > 0.0, "--plc-export-rate must be above 0");
        pc.rate = a.plc_export_rate;
        pc.streams = a.plc_export_streams.max(1);
        q.plc_export = Some(pc);
    }
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
    let _ = node.quorum.hooks.answers.set(admin_src.clone());
    if !a.bootstrap_relays.is_empty() {
        seed_discovery(admin_src.as_ref(), &a.bootstrap_relays).await;
    }
    let ui = Arc::new(vlrelay::admin::UiFiles::load(a.ui_dir.as_deref())?);
    if let Some(token) = token {
        app = app.merge(vlrelay::admin::app(admin_src.clone(), token, ui));
    } else {
        app = app.merge(vlrelay::admin::docs_routes(ui)).merge(vlrelay::admin::public_routes(admin_src.clone()));
    }
    let app = with_real_ip(app.layer(middleware::map_response(server_header)), &a.trusted_proxies);

    let listener = tokio::net::TcpListener::bind(a.listen).await?;
    tracing::info!(addr = %a.listen, node = %a.node_id, dev_mode, "vlrelay listening");
    let admin_listener = match a.admin_listen {
        Some(addr) => {
            let l = tokio::net::TcpListener::bind(addr).await?;
            let proxy = admin_proxy.as_ref().map(|p| p.header.to_string());
            tracing::info!(%addr, proxy_header = proxy.as_deref().unwrap_or("(token only)"), "admin listener");
            Some(l.into_std()?)
        }
        None => None,
    };
    // Consumers (a reconnect storm's accepts and upgrades) are served on the
    // subscriber runtime, so they can't starve the pipeline and the quorum
    // log on this one. The tokio listener must be registered there too.
    let rt = vlpds::firehose::runtime(node.cfg.serve_threads);
    let admin_server = admin_listener.map(|l| {
        let app = vlrelay::admin::proxy::admin_listener(app.clone(), admin_proxy);
        rt.spawn(async move {
            let l = tokio::net::TcpListener::from_std(l)?;
            axum::serve(l, app.into_make_service_with_connect_info::<SocketAddr>()).await
        })
    });
    let listener = listener.into_std()?;
    let server = rt.spawn(async move {
        let listener = tokio::net::TcpListener::from_std(listener)?;
        axum::serve(listener, app.into_make_service_with_connect_info::<SocketAddr>()).await
    });
    signal().await;
    tracing::info!("shutting down");
    let r = node.shutdown().await;
    server.abort();
    if let Some(s) = admin_server {
        s.abort();
    }
    r
}

/// `--admin-proxy-header` and its company, checked before anything starts.
fn admin_proxy(a: &Args) -> anyhow::Result<Option<Arc<vlrelay::admin::proxy::Settings>>> {
    let Some(h) = &a.admin_proxy_header else { return Ok(None) };
    anyhow::ensure!(
        a.admin_token.as_deref().is_some_and(|t| !t.is_empty()),
        "--admin-proxy-header needs --admin-token: without it /admin is off"
    );
    let s = vlrelay::admin::proxy::Settings::parse(h, &a.admin_proxy_from, &a.admin_operators)?;
    Ok(Some(Arc::new(s)))
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

/// `--bootstrap-relay`: the policy's seed relays, if it has none yet (an
/// operator's list is never overwritten). Retried a few times: another node
/// may be saving the document at once.
async fn seed_discovery(admin: &NodeAdmin, urls: &[String]) {
    use vlrelay::admin::AdminSource;
    for _ in 0..5 {
        let doc = match admin.full_policy().await {
            Ok(d) => d,
            Err(e) => {
                tracing::warn!("--bootstrap-relay: reading the policy: {e}");
                return;
            }
        };
        let mut p = doc.policy.clone();
        let have = p["discovery"]["seedRelays"].as_array().is_some_and(|a| !a.is_empty());
        if have {
            return;
        }
        p["discovery"]["seedRelays"] = serde_json::Value::Array(
            urls.iter()
                .map(|u| serde_json::json!({ "url": u, "enabled": true, "refreshIntervalSecs": 6 * 3600 }))
                .collect(),
        );
        let u =
            vlrelay::admin::FullPolicyUpdate { base_version: doc.version, policy: p, note: "--bootstrap-relay".into() };
        match admin.update_full_policy(u, "--bootstrap-relay").await {
            Ok(_) => {
                tracing::info!(relays = ?urls, "discovery: seeded the policy's relays");
                return;
            }
            Err(e) => tracing::info!("--bootstrap-relay: saving the policy ({e}); again"),
        }
        tokio::time::sleep(Duration::from_millis(500)).await;
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::ffi::OsStr;

    const SECRETS: [(&str, &str, &str, &str); 4] = [
        ("--s3-access-key", "VLRELAY_S3_ACCESS_KEY", "--s3-access-key-file", "VLRELAY_S3_ACCESS_KEY_FILE"),
        ("--s3-secret-key", "VLRELAY_S3_SECRET_KEY", "--s3-secret-key-file", "VLRELAY_S3_SECRET_KEY_FILE"),
        ("--admin-token", "VLRELAY_ADMIN_TOKEN", "--admin-token-file", "VLRELAY_ADMIN_TOKEN_FILE"),
        ("--qlog-admin-token", "QLOG_ADMIN_TOKEN", "--qlog-admin-token-file", "QLOG_ADMIN_TOKEN_FILE"),
    ];

    /// Parsing `Args` reads the process env, which one test sets.
    static ENV: std::sync::Mutex<()> = std::sync::Mutex::new(());

    fn tmp(contents: &str) -> PathBuf {
        let p = std::env::temp_dir().join(format!("vlrelay-secret-{}-{}", std::process::id(), rand::random::<u64>()));
        std::fs::write(&p, contents).unwrap();
        p
    }

    #[test]
    fn secret_and_its_file_conflict_without_printing_the_secret() {
        let _g = ENV.lock().unwrap_or_else(|e| e.into_inner());
        for (flag, _, file_flag, _) in SECRETS {
            let e = Args::try_parse_from(["vlrelay", flag, "hunter2", file_flag, "/run/secret"]).unwrap_err();
            assert_eq!(e.kind(), clap::error::ErrorKind::ArgumentConflict, "{flag}");
            assert!(!e.render().to_string().contains("hunter2"), "{flag}");
        }
    }

    #[test]
    fn secret_and_its_file_conflict_from_env() {
        let _g = ENV.lock().unwrap_or_else(|e| e.into_inner());
        for (_, env, _, file_env) in SECRETS {
            // SAFETY: every test that reads the env holds ENV
            unsafe {
                std::env::set_var(env, "hunter2");
                std::env::set_var(file_env, "/run/secret");
            }
            let r = Args::try_parse_from(["vlrelay"]);
            unsafe {
                std::env::remove_var(env);
                std::env::remove_var(file_env);
            }
            let e = r.unwrap_err();
            assert_eq!(e.kind(), clap::error::ErrorKind::ArgumentConflict, "{env}");
            assert!(!e.render().to_string().contains("hunter2"), "{env}");
        }
    }

    #[test]
    fn files_fill_the_secrets_less_a_trailing_newline() {
        let _g = ENV.lock().unwrap_or_else(|e| e.into_inner());
        let mut argv = vec!["vlrelay".to_string()];
        let mut paths = Vec::new();
        for (i, (_, _, file_flag, _)) in SECRETS.iter().enumerate() {
            let p = tmp(&format!("secret-{i}\n"));
            argv.extend([file_flag.to_string(), p.display().to_string()]);
            paths.push(p);
        }
        let mut a = Args::try_parse_from(&argv).unwrap();
        read_secret_files(&mut a).unwrap();
        assert_eq!(a.s3_access_key.as_deref(), Some("secret-0"));
        assert_eq!(a.s3_secret_key.as_deref(), Some("secret-1"));
        assert_eq!(a.admin_token.as_deref(), Some("secret-2"));
        assert_eq!(a.quorum.qlog_admin_token.as_deref(), Some("secret-3"));
        for p in paths {
            std::fs::remove_file(p).unwrap();
        }
    }

    #[test]
    fn proxy_sign_in_needs_its_listener_peers_operators_and_the_token() {
        let _g = ENV.lock().unwrap_or_else(|e| e.into_inner());
        let full = [
            "vlrelay",
            "--admin-token",
            "t",
            "--admin-listen",
            "127.0.0.1:2985",
            "--admin-proxy-header",
            "Tailscale-User-Login",
            "--admin-proxy-from",
            "100.64.0.1/32",
            "--admin-operators",
            "alice@example.com,bob@example.com",
        ];
        let a = Args::try_parse_from(full).unwrap();
        let s = admin_proxy(&a).unwrap().expect("on");
        assert_eq!(s.operators, ["alice@example.com", "bob@example.com"]);
        // off unless asked for
        let a = Args::try_parse_from(["vlrelay", "--admin-listen", "127.0.0.1:2985"]).unwrap();
        assert!(admin_proxy(&a).unwrap().is_none());
        // each of the header's companions is required, and none means anything alone
        let without = |skip: &[usize]| -> Vec<&str> {
            full.iter().enumerate().filter(|(j, _)| !skip.contains(j)).map(|(_, f)| *f).collect()
        };
        for skip in ["--admin-listen", "--admin-proxy-from", "--admin-operators"] {
            let i = full.iter().position(|f| *f == skip).unwrap();
            assert!(Args::try_parse_from(without(&[i, i + 1])).is_err(), "without {skip}");
        }
        assert!(Args::try_parse_from(["vlrelay", "--admin-operators", "alice@example.com"]).is_err());
        assert!(Args::try_parse_from(["vlrelay", "--admin-proxy-from", "100.64.0.1"]).is_err());
        assert!(admin_proxy(&Args::try_parse_from(without(&[1, 2])).unwrap()).is_err());
    }

    #[test]
    fn unreadable_or_empty_file_is_an_error_naming_the_flag() {
        let _g = ENV.lock().unwrap_or_else(|e| e.into_inner());
        let missing = std::env::temp_dir().join("vlrelay-secret-does-not-exist");
        let mut a =
            Args::try_parse_from([OsStr::new("vlrelay"), OsStr::new("--admin-token-file"), missing.as_os_str()])
                .unwrap();
        let e = read_secret_files(&mut a).unwrap_err().to_string();
        assert!(e.contains("--admin-token-file") && e.contains("vlrelay-secret-does-not-exist"), "{e}");
        let empty = tmp("\n");
        let mut a =
            Args::try_parse_from([OsStr::new("vlrelay"), OsStr::new("--qlog-admin-token-file"), empty.as_os_str()])
                .unwrap();
        let e = read_secret_files(&mut a).unwrap_err().to_string();
        assert!(e.contains("--qlog-admin-token-file") && e.contains("is empty"), "{e}");
        std::fs::remove_file(empty).unwrap();
    }
}
