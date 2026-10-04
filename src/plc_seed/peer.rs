//! Seeds in a cluster: one core node reads the export and forwards each
//! entry to its DID's owner, and a host stage asks the owner for a DID it
//! doesn't hold.
//!
//! One reader, not every owner filtering the whole stream for its slots:
//! the export is a public service's rate-limited endpoint, and N readers
//! would cost it (and each node) N times the requests and bytes for the
//! same data. The forward costs each owner only its share. The reader is
//! the lowest-named live core ([`ClusterNode::plc_ingest_leader`]); two
//! readers during a handover are harmless, since a write keeps the newer
//! op and the checkpoint only ever sends the next reader back a little.

use super::ingest::Sink;
use super::{LocalSeeds, Seed};
use crate::cluster::ClusterNode;
use crate::cluster::peer::TOKEN_HEADER;
use crate::identity::{Fetch, Identity, IdentityCache, Seeder};
use crate::state::record::{Reader, put_str};
use crate::state::{Chain, StoreError};
use crate::types::Host;
use axum::body::Bytes;
use axum::extract::{Query, State};
use axum::http::{HeaderMap, StatusCode};
use axum::response::{IntoResponse, Json, Response};
use parking_lot::Mutex;
use std::collections::{HashMap, HashSet};
use std::sync::{Arc, Weak};
use std::time::Duration;

pub const APPLY: &str = "/internal/relay/v1/plc/apply";
pub const FLUSH: &str = "/internal/relay/v1/plc/flush";
pub const PICK: &str = "/internal/relay/v1/plc/pick";

pub fn encode_batch(ops: &[(String, Seed)]) -> Vec<u8> {
    let mut b = Vec::with_capacity(ops.len() * 96);
    for (did, s) in ops {
        put_str(&mut b, did);
        let e = s.encode();
        crate::state::record::put_varint(&mut b, e.len() as u64);
        b.extend_from_slice(&e);
    }
    b
}

pub fn decode_batch(b: &[u8]) -> anyhow::Result<Vec<(String, Seed)>> {
    let mut r = Reader(b);
    let mut out = Vec::new();
    let bad = |_| anyhow::anyhow!("corrupt seed batch");
    while !r.0.is_empty() {
        let did = r.str().map_err(bad)?.to_string();
        let n = r.varint().map_err(bad)? as usize;
        let s = Seed::decode(r.take(n).map_err(bad)?)?;
        out.push((did, s));
    }
    Ok(out)
}

#[derive(serde::Serialize, serde::Deserialize)]
struct Picked {
    key: String,
    pds: String,
}

#[derive(serde::Deserialize)]
struct PickQuery {
    did: String,
}

/// What a DID owner serves its peers.
pub struct Owner<C: Chain, F: Fetch> {
    pub seeds: Arc<LocalSeeds<C>>,
    pub cache: Arc<IdentityCache<F>>,
    pub cluster: Weak<ClusterNode>,
    pub token: String,
}

impl<C: Chain, F: Fetch> Owner<C, F> {
    /// Applies a batch held here and drops the changed DIDs from every
    /// node's cache.
    pub async fn apply(&self, ops: Vec<(String, Seed)>) -> Result<usize, StoreError> {
        let a = self.seeds.apply(ops).await?;
        if !a.changed.is_empty() {
            for d in &a.changed {
                self.cache.invalidate(d);
            }
            if let Some(n) = self.cluster.upgrade() {
                tokio::spawn(async move { n.invalidate_keys(a.changed).await });
            }
        }
        Ok(a.written)
    }
}

pub fn router<C: Chain, F: Fetch>(owner: Arc<Owner<C, F>>) -> axum::Router {
    use axum::routing::{get, post};
    axum::Router::new()
        .route(APPLY, post(apply::<C, F>))
        .route(FLUSH, post(flush::<C, F>))
        .route(PICK, get(pick::<C, F>))
        .with_state(owner)
}

fn authorized(token: &str, h: &HeaderMap) -> Result<(), StatusCode> {
    let t = h.get(TOKEN_HEADER).and_then(|v| v.to_str().ok()).unwrap_or("");
    if t.is_empty() || !vlpds::auth::token_eq(token, t) {
        return Err(StatusCode::UNAUTHORIZED);
    }
    Ok(())
}

fn store_error(e: StoreError) -> Response {
    match e {
        StoreError::NotOwner(id) => (StatusCode::CONFLICT, format!("shard {id} is not here")).into_response(),
        e => (StatusCode::INTERNAL_SERVER_ERROR, e.to_string()).into_response(),
    }
}

async fn apply<C: Chain, F: Fetch>(State(o): State<Arc<Owner<C, F>>>, h: HeaderMap, body: Bytes) -> Response {
    if let Err(r) = authorized(&o.token, &h) {
        return r.into_response();
    }
    let ops = match decode_batch(&body) {
        Ok(x) => x,
        Err(e) => return (StatusCode::BAD_REQUEST, e.to_string()).into_response(),
    };
    match o.apply(ops).await {
        Ok(n) => Json(serde_json::json!({ "written": n })).into_response(),
        Err(e) => store_error(e),
    }
}

async fn flush<C: Chain, F: Fetch>(State(o): State<Arc<Owner<C, F>>>, h: HeaderMap) -> Response {
    if let Err(r) = authorized(&o.token, &h) {
        return r.into_response();
    }
    match o.seeds.flush().await {
        Ok(()) => StatusCode::NO_CONTENT.into_response(),
        Err(e) => store_error(e),
    }
}

async fn pick<C: Chain, F: Fetch>(
    State(o): State<Arc<Owner<C, F>>>,
    h: HeaderMap,
    Query(q): Query<PickQuery>,
) -> Response {
    if let Err(r) = authorized(&o.token, &h) {
        return r.into_response();
    }
    match o.seeds.pick(&q.did).await {
        Ok(Some(id)) => match (id.signing_key_multibase, id.pds_host) {
            (Some(key), Some(pds)) => Json(Picked { key, pds: pds.0 }).into_response(),
            _ => StatusCode::NOT_FOUND.into_response(),
        },
        Ok(None) => StatusCode::NOT_FOUND.into_response(),
        Err(e) => store_error(e),
    }
}

/// The cache's seeder on a core node: the local shards, else the DID's
/// owner. Any failure means "resolve from PLC".
pub struct ClusterSeeder<C: Chain> {
    pub seeds: Arc<LocalSeeds<C>>,
    pub cluster: Weak<ClusterNode>,
}

const PICK_TIMEOUT: Duration = Duration::from_secs(1);

impl<C: Chain> ClusterSeeder<C> {
    async fn remote(&self, did: &str) -> Option<Identity> {
        let n = self.cluster.upgrade()?;
        let owner = n.owner_of_did(did)?;
        if owner.node_id == n.node_id {
            return None;
        }
        let http = n.http.as_ref()?;
        let r = http
            .get(format!("{}{PICK}", owner.addr.trim_end_matches('/')))
            .query(&[("did", did)])
            .header(TOKEN_HEADER, n.internal_token())
            .timeout(PICK_TIMEOUT)
            .send()
            .await
            .ok()?;
        if !r.status().is_success() {
            return None;
        }
        let p: Picked = r.json().await.ok()?;
        let k = crate::verify::SigningKey::from_multibase(&p.key).ok()?;
        Some(Identity {
            did: did.to_string(),
            signing_key: Some(k),
            signing_key_multibase: Some(p.key),
            pds: Some(format!("https://{}", p.pds)),
            pds_host: Some(Host(p.pds)),
            handle: None,
        })
    }
}

impl<C: Chain> Seeder for ClusterSeeder<C> {
    fn seed<'a>(&'a self, did: &'a str) -> futures::future::BoxFuture<'a, Option<Identity>> {
        Box::pin(async move {
            match self.seeds.pick(did).await {
                Ok(x) => x,
                Err(StoreError::NotOwner(_)) => self.remote(did).await,
                Err(_) => None,
            }
        })
    }
}

/// The reader's sink in a cluster: entries grouped by owner, ours applied
/// here, the rest posted to their owners.
pub struct ForwardSink<C: Chain, F: Fetch> {
    pub owner: Arc<Owner<C, F>>,
    /// Peers sent entries since the last flush.
    touched: Mutex<HashSet<String>>,
}

impl<C: Chain, F: Fetch> ForwardSink<C, F> {
    pub fn new(owner: Arc<Owner<C, F>>) -> ForwardSink<C, F> {
        ForwardSink { owner, touched: Default::default() }
    }
}

#[async_trait::async_trait]
impl<C: Chain, F: Fetch> Sink for ForwardSink<C, F> {
    async fn apply(&self, ops: Vec<(String, Seed)>) -> anyhow::Result<usize> {
        let n = self.owner.cluster.upgrade().ok_or_else(|| anyhow::anyhow!("stopped"))?;
        let mut mine = Vec::new();
        let mut by: HashMap<String, Vec<(String, Seed)>> = HashMap::new();
        for (did, s) in ops {
            let o = n.owner_of_did(&did).ok_or_else(|| anyhow::anyhow!("no owner for {did}'s shard yet"))?;
            if o.node_id == n.node_id {
                mine.push((did, s));
            } else {
                by.entry(o.addr).or_default().push((did, s));
            }
        }
        let http = n.http.clone().ok_or_else(|| anyhow::anyhow!("no peer transport"))?;
        let token = n.internal_token().to_string();
        let sends = by.into_iter().map(|(addr, ops)| {
            let (http, token) = (http.clone(), token.clone());
            async move {
                let r = http
                    .post(format!("{}{APPLY}", addr.trim_end_matches('/')))
                    .header(TOKEN_HEADER, token)
                    .body(encode_batch(&ops))
                    .timeout(Duration::from_secs(60))
                    .send()
                    .await?
                    .error_for_status()?;
                let v: serde_json::Value = r.json().await?;
                anyhow::Ok((addr, v["written"].as_u64().unwrap_or(0) as usize))
            }
        });
        let local = async { if mine.is_empty() { Ok(0) } else { self.owner.apply(mine).await } };
        let (remote, local) = futures::join!(futures::future::join_all(sends), local);
        let mut written = local?;
        for r in remote {
            let (addr, w) = r?;
            self.touched.lock().insert(addr);
            written += w;
        }
        Ok(written)
    }

    async fn flush(&self) -> anyhow::Result<()> {
        let n = self.owner.cluster.upgrade().ok_or_else(|| anyhow::anyhow!("stopped"))?;
        let http = n.http.clone().ok_or_else(|| anyhow::anyhow!("no peer transport"))?;
        let addrs: Vec<String> = self.touched.lock().iter().cloned().collect();
        let token = n.internal_token().to_string();
        let sends = addrs.iter().map(|addr| {
            let (http, token) = (http.clone(), token.clone());
            async move {
                http.post(format!("{}{FLUSH}", addr.trim_end_matches('/')))
                    .header(TOKEN_HEADER, token)
                    .timeout(Duration::from_secs(60))
                    .send()
                    .await?
                    .error_for_status()?;
                anyhow::Ok(())
            }
        });
        let (remote, local) = futures::join!(futures::future::join_all(sends), self.owner.seeds.flush());
        local?;
        for r in remote {
            r?;
        }
        self.touched.lock().clear();
        Ok(())
    }
}
