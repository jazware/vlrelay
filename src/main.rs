//! vlRelay: the single-node relay (docs/devloop.md "The e2e contract").

use axum::http::{HeaderValue, header};
use axum::middleware;
use axum::response::Response;
use clap::Parser;
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
    /// Segment linger: a segment seals this long after its first event (docs/design.md, "Decisions").
    #[arg(long, default_value_t = 25)]
    linger_ms: u64,
    /// Segment PUTs in flight at once.
    #[arg(long, default_value_t = vlrelay::seq::DEFAULT_INFLIGHT)]
    log_inflight: usize,
    /// A segment seals at this size even before its linger is up.
    #[arg(long, default_value_t = vlrelay::seq::DEFAULT_MAX_SEGMENT_BYTES >> 20)]
    max_segment_mb: usize,
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
    /// An edge's or a replica's public URL (repeatable, or comma-separated),
    /// for a core's dashboard to include its numbers and consumers. They
    /// answer with the same --admin-token.
    #[arg(long = "admin-follower", env = "VLRELAY_ADMIN_FOLLOWERS", value_delimiter = ',')]
    admin_followers: Vec<String>,
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
    /// DID state shards (SlateDB instances). Default: 4 on one node, 24 in
    /// a cluster. Only read when the bucket has no DID layout yet.
    #[arg(long)]
    did_shards: Option<u32>,
    /// How long the log keeps events for cursor replay, in hours.
    #[arg(long, default_value_t = 72)]
    retention: u64,
    /// Dev mode only: retention in seconds instead (overrides --retention),
    /// so a test can see `OutdatedCursor`.
    #[arg(long)]
    retention_secs: Option<u64>,
    /// Dev mode only: how far a live consumer may fall behind before
    /// `ConsumerTooSlow`, in MiB (default 128).
    #[arg(long)]
    max_lag_mb: Option<usize>,
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
    #[arg(long, default_value_t = 64)]
    host_inflight_mb: usize,
    /// The same over every host together.
    #[arg(long, default_value_t = 32768)]
    inflight_events: usize,
    #[arg(long, default_value_t = 384)]
    inflight_mb: usize,
    /// DID document fetches per second, all DIDs together.
    #[arg(long, default_value_t = 50.0)]
    did_lookups_per_sec: f64,
    /// Seed DID documents from the PLC directory's /export (resumable, then
    /// follows its tail), so a cold relay doesn't resolve each account. On
    /// a cluster the lowest-named live core reads it.
    #[arg(long, env = "VLRELAY_PLC_EXPORT", value_parser = clap::builder::BoolishValueParser::new())]
    plc_export: bool,
    /// The directory --plc-export reads (default: --plc-url).
    #[arg(long, env = "VLRELAY_PLC_EXPORT_URL")]
    plc_export_url: Option<String>,
    /// /export requests per second, all streams together.
    #[arg(long, default_value_t = 2.0)]
    plc_export_rate: f64,
    /// Time windows of the export read side by side on a fresh start.
    #[arg(long, default_value_t = 4)]
    plc_export_streams: usize,
    /// Node id: the node log's id prefix, and the cluster member name.
    #[arg(long, default_value = "relay", env = "VLRELAY_NODE_ID")]
    node_id: String,
    /// Run as a core cluster node (the same as --role core).
    #[arg(long)]
    cluster: bool,
    /// Cluster role: core (lease, shards, a log), edge (follows every log
    /// over peer mTLS) or replica (follows every log from the bucket).
    #[arg(long, value_enum)]
    role: Option<vlrelay::cluster::Role>,
    /// The peer listener (node-to-node mTLS): forwarding, log streams.
    #[arg(long, default_value = "127.0.0.1:2979", env = "VLRELAY_PEER_LISTEN")]
    peer_listen: SocketAddr,
    /// `https://host:port` peers reach --peer-listen at.
    #[arg(long, env = "VLRELAY_ADVERTISE_URL")]
    advertise_url: Option<String>,
    /// Peer TLS: `ca.crt`, `{node-id}.crt`, `{node-id}.key` (vlpds admin
    /// tls ca / issue). With --dev-mode they're created as needed.
    #[arg(long, env = "VLRELAY_PEER_TLS_DIR")]
    peer_tls_dir: Option<PathBuf>,
    /// Shared secret on every peer request.
    #[arg(long, env = "VLRELAY_INTERNAL_TOKEN", hide_env_values = true)]
    internal_token: Option<String>,
    /// Node lease TTL: a crashed core node's shards move after about this
    /// plus a fifth of it.
    #[arg(long, default_value_t = 10_000)]
    lease_ttl_ms: u64,
    /// Host shards (used only when the bucket has no host layout yet).
    #[arg(long, default_value_t = 64)]
    host_shards: u32,
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
    let args = Args::parse();
    let rt = tokio::runtime::Builder::new_multi_thread().enable_all().thread_name("main").build().expect("runtime");
    let code = match rt.block_on(run(args)) {
        Ok(()) => 0,
        Err(e) => {
            tracing::error!("{e:#}");
            1
        }
    };
    std::process::exit(code);
}

async fn run(a: Args) -> anyhow::Result<()> {
    let loopback_plc = a.plc_url.contains("://127.") || a.plc_url.contains("://localhost");
    let dev_mode = a.dev_mode || a.hosts.iter().any(|h| h.starts_with("http://")) || loopback_plc;
    let mut lease_store = None;
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
        lease_store = Some(vlpds::store::Store::s3_with(&cfg, &a.prefix, None, 2, unsigned)?);
        vlpds::store::Store::s3_with(&cfg, &a.prefix, None, 256, unsigned)?
    };

    let mut cfg = NodeConfig::new(&a.plc_url);
    cfg.node_id = a.node_id.clone();
    cfg.dev_mode = dev_mode;
    cfg.linger = Duration::from_millis(a.linger_ms);
    cfg.log_inflight = a.log_inflight;
    cfg.max_segment_bytes = a.max_segment_mb << 20;
    vlpds::segment::set_compression_level(a.log_compression);
    let role = a.role.or(a.cluster.then_some(vlrelay::cluster::Role::Core));
    cfg.did_shards = a.did_shards.unwrap_or(default_did_shards(role)).max(1);
    cfg.retention = Duration::from_secs(a.retention.max(1) * 3600);
    if (a.retention_secs.is_some() || a.max_lag_mb.is_some()) && !dev_mode {
        anyhow::bail!("--retention-secs and --max-lag-mb are for dev networks (--dev-mode)");
    }
    if let Some(s) = a.retention_secs {
        cfg.retention = Duration::from_secs(s.max(1));
    }
    cfg.max_lag_bytes = a.max_lag_mb.map(|mb| mb.max(1) << 20);
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
    // a cluster splits the policy's budgets over its live core nodes
    let cores = Arc::new(vlrelay::node::cluster::LiveCores::default());
    let live: Arc<dyn vlrelay::policy::LiveNodes> = match role {
        Some(_) => cores.clone(),
        None => Arc::new(vlrelay::policy::FixedNodes::new(1)),
    };
    cfg.policy =
        Some(vlrelay::node::policy::PolicyEngine(vlrelay::policy::Engine::new(store.clone(), &a.node_id, live)));
    cfg.identity.lookups_per_sec = a.did_lookups_per_sec;
    cfg.identity.burst = (a.did_lookups_per_sec * 2.0).max(1.0);
    if dev_mode {
        // every DID in a dev network is new, and PLC is local
        cfg.identity.lookups_per_sec = cfg.identity.lookups_per_sec.max(1000.0);
        cfg.identity.burst = cfg.identity.burst.max(1000.0);
    }
    if a.plc_export {
        let mut pc = vlrelay::plc_seed::ingest::Config::new(a.plc_export_url.as_deref().unwrap_or(&a.plc_url));
        anyhow::ensure!(a.plc_export_rate > 0.0, "--plc-export-rate must be above 0");
        pc.rate = a.plc_export_rate;
        pc.streams = a.plc_export_streams.max(1);
        cfg.plc_export = Some(pc);
    }
    let setup = match role {
        Some(role) => Some(cluster_setup(&a, role, dev_mode, cores, lease_store)?),
        None => None,
    };
    let node = match &setup {
        None => Node::start(store, cfg).await?,
        Some(s) if s.role == vlrelay::cluster::Role::Core => {
            let peer = tokio::net::TcpListener::bind(a.peer_listen).await?;
            Node::start_cluster(store, cfg, s, peer).await?
        }
        Some(s) => return run_follower(&a, store, cfg, s).await,
    };

    let mut app = axum::Router::new()
        .route("/xrpc/_health", axum::routing::get(health))
        .route("/metrics", axum::routing::get(|| async { vlpds::metrics::render() }))
        .merge(node.serve.router())
        .merge(vlrelay::sync_api::router(match &node.cluster {
            Some(g) => {
                Arc::new(vlrelay::node::cluster::ClusterSync { state: node.state.clone(), hosts: g.hosts.clone() })
            }
            None => node.state.clone(),
        }))
        .merge(vlrelay::archive::read::router(
            node.state.clone(),
            node.cluster.as_ref().map(|g| -> Arc<dyn vlrelay::archive::read::Forward> {
                Arc::new(vlrelay::archive::wiring::PeerForward(Arc::downgrade(&g.cluster)))
            }),
        ));
    if a.crawl {
        app = app.merge(node.crawler.router());
    }
    let token = a.admin_token.clone().filter(|t| !t.is_empty());
    // a core answers its peers' dashboards even without a dashboard of its own
    let admin_src = (token.is_some() || node.cluster.is_some()).then(|| {
        let policy = node.policy.clone().expect("the relay always runs the policy engine");
        let demo = vlrelay::admin::demo::Demo::start(42);
        let src = NodeAdmin::new(node.clone(), policy, demo)
            .with_followers(a.admin_followers.clone(), token.clone().unwrap_or_default());
        Arc::new(src)
    });
    if let (Some(g), Some(src)) = (&node.cluster, &admin_src) {
        let _ = g.admin.set(Arc::downgrade(src));
    }
    // admin_src stays bound for the life of `run`: the peer slot holds it weakly
    let ui = Arc::new(vlrelay::admin::UiFiles::load(a.ui_dir.as_deref())?);
    if let (Some(token), Some(src)) = (token, admin_src.clone()) {
        app = app.merge(vlrelay::admin::app(src, token.clone(), ui));
        app = app.merge(vlrelay::archive::admin::router(node.state.clone(), token));
    } else {
        app = app.merge(vlrelay::admin::docs_routes(ui));
    }
    let app = with_real_ip(app.layer(middleware::map_response(server_header)), &a.trusted_proxies);

    let listener = tokio::net::TcpListener::bind(a.listen).await?;
    tracing::info!(addr = %a.listen, log = %node.log.log_id, dev_mode, "vlrelay listening");
    // Consumers (a reconnect storm's accepts and upgrades) are served on the
    // subscriber runtime, so they can't starve the pipeline and peer RPC on
    // this one. The tokio listener must be registered there too.
    let listener = listener.into_std()?;
    let server = vlpds::firehose::runtime(node.cfg.serve_threads).spawn(async move {
        let listener = tokio::net::TcpListener::from_std(listener)?;
        axum::serve(listener, app.into_make_service_with_connect_info::<SocketAddr>()).await
    });
    // the shards go first: consumers keep their sockets until the process
    // exits, then resume on another node from their cursor
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

fn cluster_setup(
    a: &Args,
    role: vlrelay::cluster::Role,
    dev_mode: bool,
    cores: Arc<vlrelay::node::cluster::LiveCores>,
    lease_store: Option<vlpds::store::Store>,
) -> anyhow::Result<vlrelay::node::cluster::ClusterSetup> {
    use vlrelay::cluster::Role;
    let advertise = a.advertise_url.clone().unwrap_or_else(|| format!("https://{}", a.peer_listen));
    let tls = match (role, &a.peer_tls_dir) {
        (Role::Replica, _) => None,
        (_, None) => anyhow::bail!("--role core and edge need --peer-tls-dir"),
        (_, Some(dir)) => {
            let files = if dev_mode {
                let host = vlpds::peer_tls::url_host(&advertise)?;
                vlpds::peer_tls::dev_files(dir, &a.node_id, &[host])?
            } else {
                vlpds::peer_tls::Files::in_dir(dir, &a.node_id)
            };
            let t = vlpds::peer_tls::PeerTls::load(files)?;
            t.spawn_reloader();
            Some(t)
        }
    };
    let internal_token = a.internal_token.clone().filter(|t| !t.is_empty());
    anyhow::ensure!(role == Role::Replica || internal_token.is_some(), "--role core and edge need --internal-token");
    Ok(vlrelay::node::cluster::ClusterSetup {
        role,
        advertise,
        tls,
        internal_token: internal_token.unwrap_or_default(),
        ttl: Duration::from_millis(a.lease_ttl_ms.max(500)),
        host_shards: a.host_shards.max(1),
        checkpoint_every: Duration::from_secs(2),
        cores,
        lease_store,
    })
}

/// An edge or a replica: the merged firehose and health, nothing else.
async fn run_follower(
    a: &Args,
    store: vlpds::store::Store,
    cfg: NodeConfig,
    setup: &vlrelay::node::cluster::ClusterSetup,
) -> anyhow::Result<()> {
    let peer = match setup.role {
        vlrelay::cluster::Role::Edge => Some(tokio::net::TcpListener::bind(a.peer_listen).await?),
        _ => None,
    };
    let node = vlrelay::node::cluster::start_follower(store, &cfg, setup).await?;
    if let Some(p) = peer {
        vlrelay::cluster::peer::spawn_listener(&node, p)?;
    }
    let mut app = axum::Router::new()
        .route("/xrpc/_health", axum::routing::get(health))
        .route("/metrics", axum::routing::get(|| async { vlpds::metrics::render() }))
        .merge(node.serve.router());
    // the cores' dashboards read this node's numbers and consumers here
    if let Some(token) = a.admin_token.clone().filter(|t| !t.is_empty()) {
        let fa = vlrelay::node::peer_admin::FollowerAdmin::start(node.clone());
        app = app.merge(vlrelay::node::peer_admin::follower_router(fa, token));
    }
    let app = with_real_ip(app.layer(middleware::map_response(server_header)), &a.trusted_proxies);
    let listener = tokio::net::TcpListener::bind(a.listen).await?;
    tracing::info!(addr = %a.listen, role = ?setup.role, "vlrelay listening");
    let server =
        tokio::spawn(
            async move { axum::serve(listener, app.into_make_service_with_connect_info::<SocketAddr>()).await },
        );
    signal().await;
    let r = node.shutdown().await;
    server.abort();
    r
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

/// Cores take at most `ceil(shards / live cores)` DID shards each, so 4
/// shards over 3 cores can leave one core with none. 24 splits evenly over
/// 2, 3, 4, 6 and 8 cores and leaves none empty at 5 (5/5/5/5/4); at 7 or
/// 9+ the last core can still come up empty, so size --did-shards to the
/// cluster there.
fn default_did_shards(role: Option<vlrelay::cluster::Role>) -> u32 {
    match role {
        None => 4,
        Some(_) => 24,
    }
}

fn with_real_ip(app: axum::Router, trusted: &[vlrelay::serve::Cidr]) -> axum::Router {
    if trusted.is_empty() {
        return app;
    }
    app.layer(middleware::from_fn_with_state(Arc::new(trusted.to_vec()), vlrelay::serve::real_ip))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn cluster_default_did_shards_leave_no_core_idle() {
        let shards = default_did_shards(Some(vlrelay::cluster::Role::Core));
        for cores in [2u32, 3, 4, 5, 6, 8] {
            // the greedy fill: each core takes up to its fair share in turn
            let fair = shards.div_ceil(cores);
            let mut left = shards;
            let mut min = u32::MAX;
            for _ in 0..cores {
                let take = fair.min(left);
                left -= take;
                min = min.min(take);
            }
            assert!(min > 0, "{shards} shards over {cores} cores leaves one with none");
        }
        assert_eq!(default_did_shards(None), 4);
    }
}
