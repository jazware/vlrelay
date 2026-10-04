//! Operator routes for archival mode, behind the admin token: the mirror's
//! counters and fetch queue, a manual fetch, and a forced desync (the e2e's
//! way to break an account's chain on purpose).

use super::fetch::Why;
use crate::state::{Chain, StateStore};
use axum::extract::{Query, Request, State};
use axum::http::{StatusCode, header};
use axum::middleware::{self, Next};
use axum::response::{IntoResponse, Response};
use axum::routing::{get, post};
use axum::{Json, Router};
use serde_json::{Value, json};
use std::collections::HashMap;
use std::sync::Arc;
use std::sync::atomic::Ordering::Relaxed;

struct Ctx<C: Chain> {
    state: Arc<StateStore<C>>,
    token: String,
}

pub fn router<C: Chain>(state: Arc<StateStore<C>>, token: String) -> Router {
    let ctx = Arc::new(Ctx { state, token });
    Router::new()
        .route("/admin/api/archive", get(status::<C>))
        .route("/admin/api/archive/fetch", post(fetch::<C>))
        .route("/admin/api/archive/desync", post(desync::<C>))
        .route_layer(middleware::from_fn_with_state(ctx.clone(), auth::<C>))
        .with_state(ctx)
}

async fn auth<C: Chain>(State(ctx): State<Arc<Ctx<C>>>, req: Request, next: Next) -> Response {
    let ok = req
        .headers()
        .get(header::AUTHORIZATION)
        .and_then(|h| h.to_str().ok())
        .and_then(|h| h.strip_prefix("Basic "))
        .is_some_and(|b| vlpds::auth::basic_admin_ok(b, &ctx.token));
    if !ok {
        return (StatusCode::UNAUTHORIZED, Json(json!({ "error": "AuthenticationRequired" }))).into_response();
    }
    next.run(req).await
}

type S<C> = State<Arc<Ctx<C>>>;
type Q = Query<HashMap<String, String>>;

async fn status<C: Chain>(State(c): S<C>) -> Response {
    let Some(a) = c.state.archive() else {
        return (StatusCode::NOT_FOUND, Json(json!({ "error": "ArchiveOff" }))).into_response();
    };
    let g = a.gate();
    let (queued, running) = a.queue.depth();
    let f = &a.queue.stats;
    let s = &a.stats;
    let mut shards = Vec::new();
    for sh in c.state.shards() {
        let (slots, tickets) = sh.mirror.len();
        shards.push(json!({ "id": sh.id.0, "trees": slots, "tickets": tickets, "sstBytes": sh.sst_bytes() }));
    }
    let errors: Vec<Value> = a.queue.errors.lock().iter().map(|(d, e)| json!({ "did": d, "error": e })).collect();
    Json(json!({
        "policyVersion": g.version(),
        "queue": { "queued": queued, "running": running },
        "fetch": {
            "queued": f.queued.load(Relaxed),
            "done": f.done.load(Relaxed),
            "failed": f.failed.load(Relaxed),
            "retried": f.retried.load(Relaxed),
            "bytes": f.bytes.load(Relaxed),
            "records": f.records.load(Relaxed),
            "fetchUs": f.fetch_us.load(Relaxed),
            "importUs": f.import_us.load(Relaxed),
            "replayedFrames": f.replayed_frames.load(Relaxed),
            "healed": f.healed.load(Relaxed),
            "byWhy": *f.by_why.lock(),
        },
        "apply": {
            "applied": s.applied.load(Relaxed),
            "appliedUs": s.applied_us.load(Relaxed),
            "skipped": s.skipped.load(Relaxed),
            "buffered": s.buffered.load(Relaxed),
            "mismatches": s.mismatches.load(Relaxed),
            "stale": s.stale.load(Relaxed),
            "replayed": s.replayed.load(Relaxed),
            "deletedRepos": s.deleted_repos.load(Relaxed),
            "deletedRows": s.deleted_rows.load(Relaxed),
        },
        "reads": {
            "exports": a.reads.exports.load(Relaxed),
            "exportUs": a.reads.export_us.load(Relaxed),
        },
        "shards": shards,
        "errors": errors,
    }))
    .into_response()
}

async fn fetch<C: Chain>(State(c): S<C>, Query(q): Q) -> Response {
    let (Some(a), Some(did)) = (c.state.archive(), q.get("did")) else {
        return (StatusCode::BAD_REQUEST, Json(json!({ "error": "InvalidRequest" }))).into_response();
    };
    let host = match c.state.shard_for(did) {
        Ok(s) => c.state.host_of(&s, did).await.unwrap_or_default(),
        Err(e) => return (StatusCode::SERVICE_UNAVAILABLE, Json(json!({ "error": e.to_string() }))).into_response(),
    };
    Json(json!({ "queued": a.queue.enqueue(did, &host, Why::Admin) })).into_response()
}

async fn desync<C: Chain>(State(c): S<C>, Query(q): Q) -> Response {
    let Some(did) = q.get("did") else {
        return (StatusCode::BAD_REQUEST, Json(json!({ "error": "InvalidRequest" }))).into_response();
    };
    match c.state.force_desync(did).await {
        Ok(found) => Json(json!({ "desynced": found })).into_response(),
        Err(e) => (StatusCode::INTERNAL_SERVER_ERROR, Json(json!({ "error": format!("{e:#}") }))).into_response(),
    }
}

impl<C: Chain> StateStore<C> {
    /// Points the account's stored chain at a data CID no commit has, as if
    /// the relay had missed one: its next commit fails prevData.
    pub async fn force_desync(&self, did: &str) -> anyhow::Result<bool> {
        let s = self.shard_for(did)?;
        let _g = s.lock_did(did).await;
        let Some(cur) = s.load(did).await? else { return Ok(false) };
        let mut rec = (*cur).clone();
        let Some(c) = rec.chain.as_mut() else { return Ok(false) };
        c.data = vlpds::cid::Cid::dag_cbor(format!("forced desync of {did}").as_bytes());
        s.stage_unlogged(did, rec);
        s.flush_unlogged().await?;
        Ok(true)
    }
}
