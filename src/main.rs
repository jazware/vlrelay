//! vlRelay: the single-node relay (docs/devloop.md "The e2e contract").

use axum::extract::{ConnectInfo, Request};
use axum::http::{HeaderValue, header};
use axum::middleware::{self, Next};
use axum::response::Response;
use clap::Parser;
use std::net::SocketAddr;
use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;
use vlrelay::node::{Node, NodeConfig, admin::NodeAdmin};

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
    /// An upstream to subscribe to (repeatable). `http://` means plain
    /// `ws://` (dev mode); a bare hostname means `wss://`.
    #[arg(long = "host")]
    hosts: Vec<String>,
    /// Accept com.atproto.sync.requestCrawl.
    #[arg(long)]
    crawl: bool,
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
    /// Node id: the node log's id prefix.
    #[arg(long, default_value = "relay")]
    node_id: String,
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
    cfg.did_shards = a.did_shards.max(1);
    cfg.retention = Duration::from_secs(a.retention.max(1) * 3600);
    cfg.lanes = a.lanes.max(1);
    if let Some(n) = a.ingest_threads {
        cfg.ingest_threads = n.max(1);
    }
    cfg.hosts = a.hosts.clone();
    cfg.identity.lookups_per_sec = a.did_lookups_per_sec;
    cfg.identity.burst = (a.did_lookups_per_sec * 2.0).max(1.0);
    if dev_mode {
        // every DID in a dev network is new, and PLC is local
        cfg.identity.lookups_per_sec = cfg.identity.lookups_per_sec.max(1000.0);
        cfg.identity.burst = cfg.identity.burst.max(1000.0);
    }
    let node = Node::start(store, cfg).await?;

    let mut app = axum::Router::new()
        .route("/xrpc/_health", axum::routing::get(health))
        .route("/metrics", axum::routing::get(|| async { vlpds::metrics::render() }))
        .merge(node.serve.router().route_layer(middleware::from_fn_with_state(node.clone(), track_consumer)))
        .merge(vlrelay::sync_api::router(node.state.clone()));
    if a.crawl {
        app = app.merge(node.crawler.router());
    }
    if let Some(token) = a.admin_token.clone().filter(|t| !t.is_empty()) {
        let ui = Arc::new(vlrelay::admin::UiFiles::load(a.ui_dir.as_deref())?);
        let engine =
            vlrelay::policy::Engine::new(node.store.clone(), &a.node_id, Arc::new(vlrelay::policy::FixedNodes::new(1)));
        engine.spawn_refresher();
        let hosts: Arc<dyn vlrelay::state::HostStore> = node.state.clone();
        let policy = Arc::new(vlrelay::policy::admin::PolicyAdmin::new(engine, hosts));
        let src = Arc::new(NodeAdmin { node: node.clone(), policy, demo: vlrelay::admin::demo::Demo::start(42) });
        app = app.merge(vlrelay::admin::app(src, token, ui));
    }
    let app = app.layer(middleware::map_response(server_header));

    let listener = tokio::net::TcpListener::bind(a.listen).await?;
    tracing::info!(addr = %a.listen, log = %node.log.log_id, dev_mode, "vlrelay listening");
    let shutdown = async {
        let mut term = tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate()).expect("SIGTERM");
        tokio::select! {
            _ = tokio::signal::ctrl_c() => {}
            _ = term.recv() => {}
        }
    };
    axum::serve(listener, app.into_make_service_with_connect_info::<SocketAddr>())
        .with_graceful_shutdown(shutdown)
        .await?;
    tracing::info!("shutting down");
    node.shutdown().await
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

async fn track_consumer(
    axum::extract::State(node): axum::extract::State<Arc<Node>>,
    req: Request,
    next: Next,
) -> Response {
    if let Some(ConnectInfo(addr)) = req.extensions().get::<ConnectInfo<SocketAddr>>().cloned() {
        let ua = req.headers().get(header::USER_AGENT).and_then(|v| v.to_str().ok()).unwrap_or("").to_string();
        let cursor = req
            .uri()
            .query()
            .and_then(|q| q.split('&').find_map(|kv| kv.strip_prefix("cursor=")))
            .and_then(|c| c.parse().ok());
        let live = node.serve.firehose.connections_from(addr.ip());
        node.consumers.connected(addr.ip(), &ua, cursor, live);
    }
    next.run(req).await
}
