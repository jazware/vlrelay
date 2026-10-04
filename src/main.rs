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
    /// Key prefix in the bucket: one relay per prefix.
    #[arg(long, default_value = "vlrelay", env = "VLRELAY_PREFIX")]
    prefix: String,
    #[arg(long, default_value = "https://plc.directory", env = "VLRELAY_PLC_URL")]
    plc_url: String,
    /// Segment linger (PLAN.md decision 1).
    #[arg(long, default_value_t = 25)]
    linger_ms: u64,
    /// Segment PUTs in flight at once.
    #[arg(long, default_value_t = vlrelay::seq::DEFAULT_INFLIGHT)]
    log_inflight: usize,
    /// A segment seals at this size even before its linger is up.
    #[arg(long, default_value_t = vlrelay::seq::DEFAULT_MAX_SEGMENT_BYTES >> 20)]
    max_segment_mb: usize,
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
    /// Allows plain ws://, IPs, localhost and ports for upstreams and DID
    /// documents. Implied by an http:// --host or a loopback --plc-url.
    #[arg(long)]
    dev_mode: bool,
    /// DID state shards (SlateDB instances).
    #[arg(long, default_value_t = 4)]
    did_shards: u32,
    /// How long the log keeps events for cursor replay, in hours.
    #[arg(long, default_value_t = 72)]
    retention: u64,
    /// Pipeline lanes; a DID always maps to the same one.
    #[arg(long, default_value_t = 64)]
    lanes: usize,
    /// Threads verifying events (default: the core count, at most 16).
    #[arg(long)]
    ingest_threads: Option<usize>,
    /// DID document fetches per second, all DIDs together.
    #[arg(long, default_value_t = 50.0)]
    did_lookups_per_sec: f64,
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
        vlpds::store::Store::s3(&cfg, &a.prefix, None, 256)?
    };

    let mut cfg = NodeConfig::new(&a.plc_url);
    cfg.node_id = a.node_id.clone();
    cfg.dev_mode = dev_mode;
    cfg.linger = Duration::from_millis(a.linger_ms);
    cfg.log_inflight = a.log_inflight;
    cfg.max_segment_bytes = a.max_segment_mb << 20;
    cfg.did_shards = a.did_shards.max(1);
    cfg.retention = Duration::from_secs(a.retention.max(1) * 3600);
    cfg.lanes = a.lanes.max(1);
    if let Some(n) = a.ingest_threads {
        cfg.ingest_threads = n.max(1);
    }
    cfg.hosts = a.hosts.clone();
    cfg.cli_host_tier = vlrelay::upstream::Tier::parse(&a.host_tier)
        .filter(|t| t.connects())
        .ok_or_else(|| anyhow::anyhow!("--host-tier {}: one of trusted, default, new, throttled", a.host_tier))?;
    let role = a.role.or(a.cluster.then_some(vlrelay::cluster::Role::Core));
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
    let setup = match role {
        Some(role) => Some(cluster_setup(&a, role, dev_mode, cores)?),
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
        .merge(vlrelay::archive::read::router(node.state.clone(), None));
    if a.crawl {
        app = app.merge(node.crawler.router());
    }
    if let Some(token) = a.admin_token.clone().filter(|t| !t.is_empty()) {
        let ui = Arc::new(vlrelay::admin::UiFiles::load(a.ui_dir.as_deref())?);
        let policy = node.policy.clone().expect("the relay always runs the policy engine");
        let src = Arc::new(NodeAdmin::new(node.clone(), policy, vlrelay::admin::demo::Demo::start(42)));
        app = app.merge(vlrelay::admin::app(src, token.clone(), ui));
        app = app.merge(vlrelay::archive::admin::router(node.state.clone(), token));
    }
    let app = app.layer(middleware::map_response(server_header));

    let listener = tokio::net::TcpListener::bind(a.listen).await?;
    tracing::info!(addr = %a.listen, log = %node.log.log_id, dev_mode, "vlrelay listening");
    let server =
        tokio::spawn(
            async move { axum::serve(listener, app.into_make_service_with_connect_info::<SocketAddr>()).await },
        );
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
    let app = axum::Router::new()
        .route("/xrpc/_health", axum::routing::get(health))
        .route("/metrics", axum::routing::get(|| async { vlpds::metrics::render() }))
        .merge(node.serve.router())
        .layer(middleware::map_response(server_header));
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
