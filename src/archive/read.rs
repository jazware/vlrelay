//! The archival read endpoints: `getRepo` (streamed from storage the way
//! vlpds exports), `getRecord`, `getBlocks`, and `listBlobs` (not served:
//! blobs stay on the PDS). Only the DID's owner answers (PLAN.md decision
//! 7); a cluster node that doesn't hold the shard hands the request to
//! [`Forward`].

use super::mirror;
use crate::state::{AccountStatus, Chain, StateStore, StoreError};
use axum::Router;
use axum::body::Body;
use axum::extract::{Query, RawQuery, State};
use axum::http::{StatusCode, header};
use axum::response::{IntoResponse, Response};
use axum::routing::get;
use bytes::Bytes;
use std::collections::{HashMap, HashSet};
use std::sync::Arc;
use std::sync::atomic::Ordering;
use vlpds::cid::Cid;
use vlpds::state::{self as vs, Head};
use vlpds::tid::Tid;
use vlpds::xrpc::XrpcError;

/// Sends a read for a DID this node doesn't own to its owner.
#[async_trait::async_trait]
pub trait Forward: Send + Sync {
    /// None: no owner known right now.
    async fn forward(&self, did: &str, path_and_query: &str) -> Option<Response>;
}

pub struct Reads<C: Chain> {
    pub state: Arc<StateStore<C>>,
    pub forward: Option<Arc<dyn Forward>>,
    exports: Arc<tokio::sync::Semaphore>,
    pub stall: std::time::Duration,
}

pub const MAX_EXPORTS: usize = 32;

pub fn router<C: Chain>(state: Arc<StateStore<C>>, forward: Option<Arc<dyn Forward>>) -> Router {
    let r = Arc::new(Reads {
        state,
        forward,
        exports: Arc::new(tokio::sync::Semaphore::new(MAX_EXPORTS)),
        stall: vlpds::xrpc::DEFAULT_EXPORT_STALL,
    });
    Router::new()
        .route("/xrpc/com.atproto.sync.getRepo", get(get_repo::<C>))
        .route("/xrpc/com.atproto.sync.getRecord", get(get_record::<C>))
        .route("/xrpc/com.atproto.sync.getBlocks", get(get_blocks::<C>))
        .route("/xrpc/com.atproto.sync.listBlobs", get(list_blobs))
        .with_state(r)
}

type S<C> = State<Arc<Reads<C>>>;

fn did_ok(did: &str) -> Result<(), XrpcError> {
    let ok = did.len() <= 2048
        && did.strip_prefix("did:").and_then(|r| r.split_once(':')).is_some_and(|(m, id)| {
            !m.is_empty() && m.bytes().all(|b| b.is_ascii_lowercase()) && !id.is_empty() && !id.ends_with(':')
        });
    if ok { Ok(()) } else { Err(XrpcError::bad("InvalidRequest", "did is not a valid DID")) }
}

fn not_found(did: &str) -> XrpcError {
    XrpcError::bad("RepoNotFound", format!("Could not find repo for DID: {did}"))
}

fn internal(e: impl std::fmt::Display) -> XrpcError {
    XrpcError::internal(e.to_string())
}

enum Owner {
    Here(u64, Head, Arc<slatedb::DbSnapshot>),
    Elsewhere(Response),
}

impl<C: Chain> Reads<C> {
    /// The mirror to read, checked the way a PDS checks repo availability.
    async fn open(&self, did: &str, pq: &str) -> Result<Owner, XrpcError> {
        did_ok(did)?;
        let s = match self.state.shard_for(did) {
            Ok(s) => s,
            Err(StoreError::NotOwner(id)) => {
                if let Some(f) = &self.forward
                    && let Some(r) = f.forward(did, pq).await
                {
                    return Ok(Owner::Elsewhere(r));
                }
                return Err(XrpcError::unavailable("ShardUnavailable", format!("shard {id} has no owner right now")));
            }
            Err(e) => return Err(internal(e)),
        };
        let rec = s.load(did).await.map_err(internal)?.ok_or_else(|| not_found(did))?;
        match rec.status() {
            AccountStatus::Takendown => {
                return Err(XrpcError::bad("RepoTakendown", format!("Repo has been takendown: {did}")));
            }
            AccountStatus::Suspended => {
                return Err(XrpcError::bad("RepoSuspended", format!("Repo has been suspended: {did}")));
            }
            AccountStatus::Deactivated => {
                return Err(XrpcError::bad("RepoDeactivated", format!("Repo has been deactivated: {did}")));
            }
            AccountStatus::Deleted | AccountStatus::Inactive => return Err(not_found(did)),
            AccountStatus::Active | AccountStatus::Desynchronized | AccountStatus::Throttled => {}
        }
        let snap = s.db.snapshot().await.map_err(internal)?;
        let Some(generation) =
            snap.get(mirror::meta_key(did)).await.map_err(internal)?.and_then(|b| mirror::Meta::decode(&b).ok()?.live)
        else {
            return Err(not_found(did));
        };
        let head = match snap.get(vs::head_key(did)).await.map_err(internal)? {
            Some(b) => Head::decode(&b).map_err(internal)?,
            None => return Err(not_found(did)),
        };
        Ok(Owner::Here(generation, head, snap))
    }
}

fn pq(path: &str, raw: &Option<String>) -> String {
    match raw {
        Some(q) => format!("{path}?{q}"),
        None => path.to_string(),
    }
}

fn car_response(body: Vec<u8>) -> Response {
    ([(header::CONTENT_TYPE, "application/vnd.ipld.car")], Body::from(body)).into_response()
}

async fn get_repo<C: Chain>(
    State(r): S<C>,
    RawQuery(raw): RawQuery,
    Query(q): Query<HashMap<String, String>>,
) -> Response {
    match get_repo_inner(&r, &raw, &q).await {
        Ok(resp) => resp,
        Err(e) => e.into_response(),
    }
}

async fn get_repo_inner<C: Chain>(
    r: &Arc<Reads<C>>,
    raw: &Option<String>,
    q: &HashMap<String, String>,
) -> Result<Response, XrpcError> {
    let did = q.get("did").ok_or_else(|| XrpcError::bad("InvalidRequest", "missing did"))?;
    let since = match q.get("since").filter(|s| !s.is_empty()) {
        Some(s) => Some(Tid::parse(s).ok_or_else(|| XrpcError::bad("InvalidRequest", "since must be a TID"))?.0),
        None => None,
    };
    let (generation, head, snap) = match r.open(did, &pq("/xrpc/com.atproto.sync.getRepo", raw)).await? {
        Owner::Here(g, h, s) => (g, h, s),
        Owner::Elsewhere(resp) => return Ok(resp),
    };
    let slot = r
        .exports
        .clone()
        .try_acquire_owned()
        .map_err(|_| XrpcError::unavailable("Overloaded", "too many repo exports in progress; retry shortly"))?;
    let (tx, mut rx) = tokio::sync::mpsc::channel::<Result<Bytes, std::io::Error>>(4);
    let (did, stall) = (Arc::<str>::from(did.as_str()), r.stall);
    let served = r.state.archive().map(|a| a.clone());
    tokio::spawn(async move {
        let _slot = slot;
        let t0 = std::time::Instant::now();
        let res = vlpds::xrpc::stream_export(snap, did.clone(), generation, head, since, &tx, stall).await;
        if let Some(a) = served {
            a.reads.exports.fetch_add(1, Ordering::Relaxed);
            a.reads.export_us.fetch_add(t0.elapsed().as_micros() as u64, Ordering::Relaxed);
        }
        if let Err(why) = res
            && why != "client_gone"
        {
            tracing::debug!(%did, why, "archival getRepo ended early");
            let _ = tx.send(Err(std::io::Error::other("repo export aborted"))).await;
        }
    });
    let stream = futures::stream::poll_fn(move |cx| rx.poll_recv(cx));
    Ok(([(header::CONTENT_TYPE, "application/vnd.ipld.car")], Body::from_stream(stream)).into_response())
}

async fn get_record<C: Chain>(
    State(r): S<C>,
    RawQuery(raw): RawQuery,
    Query(q): Query<HashMap<String, String>>,
) -> Response {
    let run = async {
        let did = q.get("did").ok_or_else(|| XrpcError::bad("InvalidRequest", "missing did"))?;
        let (coll, rkey) = match (q.get("collection"), q.get("rkey")) {
            (Some(c), Some(k)) if vlpds::xrpc::syntax::valid_nsid(c) && vlpds::xrpc::syntax::valid_rkey(k) => (c, k),
            _ => return Err(XrpcError::bad("InvalidRequest", "invalid collection or rkey")),
        };
        let (generation, head, snap) = match r.open(did, &pq("/xrpc/com.atproto.sync.getRecord", &raw)).await? {
            Owner::Here(g, h, s) => (g, h, s),
            Owner::Elsewhere(resp) => return Ok(resp),
        };
        let path = format!("{coll}/{rkey}");
        let mut out = Vec::new();
        vlpds::car::write_header(&mut out, &head.commit);
        vlpds::car::write_block(&mut out, &head.commit, &head.commit_block);
        let (snap2, d, p, root) = (snap.clone(), did.clone(), path.clone(), head.data);
        let proof = tokio::task::spawn_blocking(move || {
            let rt = tokio::runtime::Handle::current();
            let src = vlpds::mst_store::DbSource::new(&*snap2, &d, generation, &rt);
            let mut t = vlpds::mst_lazy::LazyTree::open(root, mirror::PERSIST_MIN, &src)?;
            t.proof_blocks(p.as_bytes(), &src)
        })
        .await
        .map_err(internal)?
        .map_err(internal)?;
        for (c, b) in proof {
            vlpds::car::write_block(&mut out, &c, &b);
        }
        if let Some(v) = snap.get(vs::record_key(did, generation, &path)).await.map_err(internal)? {
            let (cid, bytes) = vs::decode_record_value(&v).map_err(internal)?;
            vlpds::car::write_block(&mut out, &cid, &bytes);
        }
        Ok(car_response(out))
    };
    match run.await {
        Ok(resp) => resp,
        Err(e) => e.into_response(),
    }
}

async fn get_blocks<C: Chain>(State(r): S<C>, RawQuery(raw): RawQuery) -> Response {
    let run = async {
        let pairs: Vec<(String, String)> = raw
            .as_deref()
            .unwrap_or("")
            .split('&')
            .filter(|p| !p.is_empty())
            .map(|p| {
                let (k, v) = p.split_once('=').unwrap_or((p, ""));
                (pct(k), pct(v))
            })
            .collect();
        let did = pairs
            .iter()
            .find(|(k, _)| k == "did")
            .map(|(_, v)| v.clone())
            .ok_or_else(|| XrpcError::bad("InvalidRequest", "Error: Params must have the property \"did\""))?;
        let mut want = Vec::new();
        let mut seen = HashSet::new();
        for (k, v) in &pairs {
            if k == "cids" || k == "cids[]" {
                let c = Cid::parse(v).map_err(|_| XrpcError::bad("InvalidRequest", format!("invalid cid: {v}")))?;
                if seen.insert(c) {
                    want.push(c);
                }
            }
        }
        let (generation, head, snap) = match r.open(&did, &pq("/xrpc/com.atproto.sync.getBlocks", &raw)).await? {
            Owner::Here(g, h, s) => (g, h, s),
            Owner::Elsewhere(resp) => return Ok(resp),
        };
        let mut found: HashMap<Cid, Vec<u8>> = HashMap::new();
        if seen.contains(&head.commit) {
            found.insert(head.commit, head.commit_block.to_vec());
        }
        for c in &want {
            if found.contains_key(c) || c.codec != vlpds::cid::CODEC_DAG_CBOR {
                continue;
            }
            if let Some(b) = snap.get(vs::mst_node_key(&did, generation, c)).await.map_err(internal)? {
                found.insert(*c, b.to_vec());
            } else if let Some(b) = vlpds::xrpc::find_record(&snap, &did, generation, c).await? {
                found.insert(*c, b);
            }
        }
        // leaves aren't stored: one walk of the tree finds the rest
        let rest: HashSet<Cid> = want.iter().filter(|c| !found.contains_key(c)).copied().collect();
        if !rest.is_empty() {
            let (snap2, d, root) = (snap.clone(), did.clone(), head.data);
            let got = tokio::task::spawn_blocking(move || {
                let rt = tokio::runtime::Handle::current();
                let nodes = vlpds::mst_store::DbSource::new(&*snap2, &d, generation, &rt);
                let scan = vlpds::mst_store::ScanSource::open(&*snap2, &d, generation, nodes, &rt)?;
                let mut got = Vec::new();
                vlpds::mst_lazy::export_blocks(root, mirror::PERSIST_MIN, &scan, &mut |c, b| {
                    if rest.contains(&c) {
                        got.push((c, b.to_vec()));
                    }
                })?;
                Ok::<_, vlpds::mst::MstError>(got)
            })
            .await
            .map_err(internal)?
            .map_err(internal)?;
            found.extend(got);
        }
        let missing: Vec<String> = want.iter().filter(|c| !found.contains_key(c)).map(|c| c.to_string()).collect();
        if !missing.is_empty() {
            return Err(XrpcError::bad("BlockNotFound", format!("Could not find cids: {}", missing.join(","))));
        }
        let mut out = Vec::new();
        let mut h = Vec::with_capacity(32);
        vlpds::cbor::write_map_head(&mut h, 2);
        vlpds::cbor::write_text(&mut h, "roots");
        vlpds::cbor::write_array_head(&mut h, 0);
        vlpds::cbor::write_text(&mut h, "version");
        vlpds::cbor::write_uint(&mut h, 1);
        vlpds::car::write_varint(&mut out, h.len() as u64);
        out.extend_from_slice(&h);
        for c in &want {
            vlpds::car::write_block(&mut out, c, &found[c]);
        }
        Ok(car_response(out))
    };
    match run.await {
        Ok(resp) => resp,
        Err(e) => e.into_response(),
    }
}

async fn list_blobs() -> XrpcError {
    XrpcError {
        status: StatusCode::NOT_IMPLEMENTED,
        error: "MethodNotImplemented".into(),
        message: "this relay mirrors repos, not blobs: ask the account's PDS".into(),
    }
}

/// Percent-decoding for query values ('+' is a space).
fn pct(s: &str) -> String {
    let b = s.as_bytes();
    let mut out = Vec::with_capacity(b.len());
    let mut i = 0;
    while i < b.len() {
        match b[i] {
            b'+' => out.push(b' '),
            b'%' if i + 2 < b.len() && b[i + 1].is_ascii_hexdigit() && b[i + 2].is_ascii_hexdigit() => {
                out.push(u8::from_str_radix(&s[i + 1..i + 3], 16).unwrap_or(b'%'));
                i += 2;
            }
            c => out.push(c),
        }
        i += 1;
    }
    String::from_utf8_lossy(&out).into_owned()
}
