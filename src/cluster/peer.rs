//! The peer listener: node-to-node routes over mTLS (vlpds's peer
//! transport, HTTP/2 with the log stream upgrading over HTTP/1.1). Every
//! route also wants the internal token, as vlpds's do.

use super::ClusterNode;
use super::forward;
use axum::body::Bytes;
use axum::extract::{Query, State, WebSocketUpgrade};
use axum::http::{HeaderMap, StatusCode};
use axum::response::{IntoResponse, Response};
use axum::routing::{get, post};
use serde::{Deserialize, Serialize};
use std::sync::Arc;
use std::sync::atomic::Ordering;
use vlpds::cluster::{Handoff, ShardHost};

pub const TOKEN_HEADER: &str = "x-vlpds-internal";
/// vlpds's path: `vlpds::remote::follow_log` streams from here.
pub const STREAM: &str = "/internal/v1/log/stream";
pub const HELLO: &str = "/internal/relay/v1/cluster/hello";
pub const NUDGE: &str = "/internal/relay/v1/cluster/nudge";
pub const FORWARD: &str = "/internal/relay/v1/forward";
pub const KEYS: &str = "/internal/relay/v1/keys/invalidate";
pub const FENCE: &str = "/internal/relay/v1/cluster/fence";

/// A leaving node whose own fence failed asks a peer to fence its log.
#[derive(Serialize, Deserialize)]
pub struct FenceIn {
    pub log_id: String,
}

#[derive(Serialize, Deserialize)]
pub struct HelloIn {
    pub node_id: String,
}

#[derive(Serialize, Deserialize)]
pub struct HelloOut {
    pub floor: Option<i64>,
}

#[derive(Serialize, Deserialize)]
pub struct NudgeIn {
    #[serde(default)]
    pub handoffs: Vec<Handoff>,
    /// Step the host shards now.
    #[serde(default)]
    pub hosts: bool,
    /// The sender is leaving: hand it nothing, even before its draining
    /// lease is listed.
    #[serde(default)]
    pub leaving: Option<String>,
}

#[derive(Serialize, Deserialize)]
struct KeysIn {
    dids: Vec<String>,
}

#[derive(Deserialize)]
struct StreamQuery {
    log: String,
}

type S = State<Arc<ClusterNode>>;

fn authorized(n: &ClusterNode, h: &HeaderMap) -> Result<(), StatusCode> {
    let t = h.get(TOKEN_HEADER).and_then(|v| v.to_str().ok()).unwrap_or("");
    if n.halted() {
        // a crashed test node: nothing answers
        return Err(StatusCode::SERVICE_UNAVAILABLE);
    }
    if t.is_empty() || !vlpds::auth::token_eq(&n.opts.internal_token, t) {
        return Err(StatusCode::UNAUTHORIZED);
    }
    Ok(())
}

pub fn router(node: &Arc<ClusterNode>) -> axum::Router {
    axum::Router::new()
        .route(STREAM, get(stream))
        .route(HELLO, post(hello))
        .route(NUDGE, post(nudge))
        .route(FORWARD, post(forward_batch))
        .route(KEYS, post(keys))
        .route(FENCE, post(fence))
        .with_state(node.clone())
}

/// Serves [`router`] over peer mTLS on `listener`.
pub fn spawn_listener(node: &Arc<ClusterNode>, listener: tokio::net::TcpListener) -> anyhow::Result<()> {
    spawn_listener_with(node, listener, axum::Router::new())
}

/// [`spawn_listener`] plus routes of the node's own (archival reads
/// forwarded to the DID owner), which check the token themselves.
pub fn spawn_listener_with(
    node: &Arc<ClusterNode>,
    listener: tokio::net::TcpListener,
    extra: axum::Router,
) -> anyhow::Result<()> {
    let tls = node.opts.tls.as_ref().ok_or_else(|| anyhow::anyhow!("a peer listener needs peer TLS"))?;
    let opts = vlpds::server::ServeOptions {
        h2: vlpds::server::H2Profile::Peer,
        max_connections: vlpds::server::DEFAULT_MAX_CONNECTIONS,
        tls: Some(tls.server_config()),
    };
    let r = router(node).merge(extra);
    tokio::spawn(async move {
        if let Err(e) = vlpds::server::serve_with(listener, r, opts).await {
            tracing::error!("peer listener exited: {e:#}");
        }
    });
    Ok(())
}

async fn stream(State(n): S, h: HeaderMap, Query(q): Query<StreamQuery>, ws: WebSocketUpgrade) -> Response {
    if let Err(r) = authorized(&n, &h) {
        return r.into_response();
    }
    let Some(log) = n.log.clone() else {
        return StatusCode::NOT_FOUND.into_response();
    };
    // the address may already serve a later incarnation's log
    if *log.log_id != *q.log || n.closed.load(Ordering::Acquire) {
        return StatusCode::GONE.into_response();
    }
    let closed = n.closed.clone();
    ws.on_upgrade(move |ws| super::follow::serve_stream(ws, log, closed))
}

async fn hello(State(n): S, h: HeaderMap, axum::Json(inp): axum::Json<HelloIn>) -> Response {
    if let Err(r) = authorized(&n, &h) {
        return r.into_response();
    }
    let Some(c) = n.cluster.clone() else {
        return axum::Json(HelloOut { floor: None }).into_response();
    };
    let host: Arc<dyn ShardHost> = n.clone();
    match c.learn_peer(&host, &inp.node_id).await {
        Ok(floor) => axum::Json(HelloOut { floor }).into_response(),
        Err(e) => (StatusCode::SERVICE_UNAVAILABLE, format!("{e:#}")).into_response(),
    }
}

async fn nudge(State(n): S, h: HeaderMap, axum::Json(inp): axum::Json<NudgeIn>) -> Response {
    if let Err(r) = authorized(&n, &h) {
        return r.into_response();
    }
    if let Some(id) = inp.leaving {
        n.peer_leaving(id);
    }
    n.nudged(inp.handoffs, inp.hosts);
    StatusCode::OK.into_response()
}

async fn fence(State(n): S, h: HeaderMap, axum::Json(inp): axum::Json<FenceIn>) -> Response {
    if let Err(r) = authorized(&n, &h) {
        return r.into_response();
    }
    let Some(c) = &n.cluster else { return StatusCode::NOT_FOUND.into_response() };
    n.peer_leaving(inp.log_id.clone());
    match c.fence(&inp.log_id).await {
        Ok(_) => {
            tracing::info!(log_id = %inp.log_id, "fenced a leaving peer's log at its request");
            n.on_membership();
            StatusCode::OK.into_response()
        }
        Err(e) => {
            tracing::warn!(log_id = %inp.log_id, "fencing a leaving peer's log failed: {e:#}");
            StatusCode::SERVICE_UNAVAILABLE.into_response()
        }
    }
}

async fn forward_batch(State(n): S, h: HeaderMap, body: Bytes) -> Response {
    if let Err(r) = authorized(&n, &h) {
        return r.into_response();
    }
    let batch = match forward::decode_batch(body) {
        Ok(b) => b,
        Err(e) => return (StatusCode::BAD_REQUEST, format!("{e:#}")).into_response(),
    };
    let rs = n.apply_local(batch).await;
    forward::encode_results(&rs).into_response()
}

async fn keys(State(n): S, h: HeaderMap, axum::Json(inp): axum::Json<KeysIn>) -> Response {
    if let Err(r) = authorized(&n, &h) {
        return r.into_response();
    }
    n.keys_changed(inp.dids);
    StatusCode::OK.into_response()
}
