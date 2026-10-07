//! The fake PLC directory with an `/export`: one genesis op per fleet
//! account, then whatever the test or the tail appends (key rotations,
//! re-announcements), paginated like plc.directory's (`count` up to 1,000,
//! `after` an exclusive `createdAt`). `GET /{did}` serves each DID's latest
//! key, which [`FakePlc::rotate_hidden`] can move ahead of the export.

use super::fleet::Layout;
use axum::Router;
use axum::extract::{Path, Query, State};
use axum::http::StatusCode;
use axum::response::{IntoResponse, Json, Response};
use axum::routing::get;
use parking_lot::RwLock;
use serde_json::json;
use sha2::{Digest, Sha256};
use std::collections::HashMap;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering::Relaxed};
use vlpds::crypto::Keypair;

#[derive(Clone, Copy, Debug)]
struct Op {
    ms: u64,
    g: u32,
    i: u32,
    ver: u32,
}

pub struct FakePlc {
    pub layout: Layout,
    /// Global hosts `0..hosts`.
    pub hosts: u32,
    ops: RwLock<Vec<Op>>,
    /// The key version `GET /{did}` serves, when not the genesis one.
    doc_ver: RwLock<HashMap<(u32, u32), u32>>,
    genesis_keys: Vec<String>,
    pub doc_fetches: AtomicU64,
    pub export_requests: AtomicU64,
    /// Every `after` asked for, in order.
    pub afters: parking_lot::Mutex<Vec<String>>,
    /// Requests answered 503 before serving again.
    pub fail_next: AtomicU64,
    /// Every Nth `/export` request is a 429 with `Retry-After: 1`, as
    /// plc.directory's rate limit answers (0: never).
    pub throttle_every: AtomicU64,
    pub throttled: AtomicU64,
    /// Each `/export` answer waits this long first (plc.directory's is
    /// 0.2-0.6 s for a full page).
    pub export_delay_ms: AtomicU64,
    /// `listHosts`, as a relay would serve it: the fleet's hosts and this
    /// many more that don't answer, `list_page` a page at most.
    pub list_extra: AtomicU64,
    pub list_page: AtomicU64,
    pub list_requests: AtomicU64,
}

pub fn now_ms() -> u64 {
    std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH).map(|d| d.as_millis() as u64).unwrap_or(0)
}

fn iso(ms: u64) -> String {
    chrono::DateTime::from_timestamp_millis(ms as i64)
        .unwrap_or_default()
        .to_rfc3339_opts(chrono::SecondsFormat::Millis, true)
}

impl FakePlc {
    /// Genesis ops `spacing_ms` apart, ending at `end_ms`, hosts interleaved
    /// (account i of every host, then i + 1).
    pub fn new(layout: Layout, hosts: u32, dids: u32, end_ms: u64, spacing_ms: u64) -> Arc<FakePlc> {
        let n = hosts as u64 * dids as u64;
        let start = end_ms.saturating_sub(n * spacing_ms);
        let mut ops = Vec::with_capacity(n as usize);
        for i in 0..dids {
            for g in 0..hosts {
                ops.push(Op { ms: start + ops.len() as u64 * spacing_ms, g, i, ver: 0 });
            }
        }
        let threads = std::thread::available_parallelism().map_or(4, |n| n.get()).min(16);
        let chunk = (ops.len() / threads).max(1);
        let genesis_keys = std::thread::scope(|s| {
            let hs: Vec<_> = ops
                .chunks(chunk)
                .map(|c| s.spawn(|| c.iter().map(|o| layout.key(o.g, o.i).public_multibase()).collect::<Vec<_>>()))
                .collect();
            hs.into_iter().flat_map(|h| h.join().expect("key thread")).collect()
        });
        Arc::new(FakePlc {
            layout,
            hosts,
            ops: RwLock::new(ops),
            doc_ver: Default::default(),
            genesis_keys,
            doc_fetches: AtomicU64::new(0),
            export_requests: AtomicU64::new(0),
            afters: Default::default(),
            fail_next: AtomicU64::new(0),
            throttle_every: AtomicU64::new(0),
            throttled: AtomicU64::new(0),
            export_delay_ms: AtomicU64::new(0),
            list_extra: AtomicU64::new(0),
            list_page: AtomicU64::new(100),
            list_requests: AtomicU64::new(0),
        })
    }

    pub fn op_count(&self) -> usize {
        self.ops.read().len()
    }

    /// The key of version `ver` (0: the fleet's own).
    pub fn key(&self, g: u32, i: u32, ver: u32) -> Keypair {
        if ver == 0 {
            return self.layout.key(g, i);
        }
        let did = self.layout.did(g, i);
        let mut n = 0u32;
        loop {
            let h = Sha256::digest(format!("fakeplc rotated\0{did}\0{ver}\0{n}"));
            if let Ok(k) = Keypair::from_bytes(&h) {
                return k;
            }
            n += 1;
        }
    }

    fn multibase(&self, g: u32, i: u32, ver: u32) -> String {
        if ver == 0 {
            return self.genesis_keys[(i as usize) * self.hosts as usize + g as usize].clone();
        }
        self.key(g, i, ver).public_multibase()
    }

    pub fn current(&self, g: u32, i: u32) -> u32 {
        self.doc_ver.read().get(&(g, i)).copied().unwrap_or(0)
    }

    /// Appends an op at `at_ms` with key version `ver`, which `GET /{did}`
    /// serves from now on. Ops must be appended in time order.
    pub fn append(&self, g: u32, i: u32, ver: u32, at_ms: u64) {
        let mut ops = self.ops.write();
        let ms = at_ms.max(ops.last().map_or(0, |o| o.ms));
        ops.push(Op { ms, g, i, ver });
        self.doc_ver.write().insert((g, i), ver);
    }

    /// A new key the directory serves but the export doesn't show yet.
    pub fn rotate_hidden(&self, g: u32, i: u32, ver: u32) {
        self.doc_ver.write().insert((g, i), ver);
    }

    fn line(&self, o: &Op, prev: bool) -> String {
        let did = self.layout.did(o.g, o.i);
        let key = format!("did:key:{}", self.multibase(o.g, o.i, o.ver));
        let op = json!({
            "type": "plc_operation",
            "rotationKeys": [key],
            "verificationMethods": {"atproto": key},
            "alsoKnownAs": [format!("at://{}", self.layout.handle(o.g, o.i))],
            "services": {"atproto_pds": {"type": "AtprotoPersonalDataServer", "endpoint": self.layout.host_url(o.g)}},
            "prev": if prev { json!("bafyreifakeprev") } else { serde_json::Value::Null },
            "sig": "fake",
        });
        json!({
            "did": did,
            "cid": format!("bafyfake{}x{}x{}", o.g, o.i, o.ver),
            "createdAt": iso(o.ms),
            "operation": op,
            "nullified": false,
        })
        .to_string()
    }

    pub fn export(&self, after: Option<&str>, count: usize) -> String {
        let after_ms = after.and_then(|a| chrono::DateTime::parse_from_rfc3339(a).ok()).map(|t| t.timestamp_millis());
        let ops = self.ops.read();
        let from = match after_ms {
            Some(a) => ops.partition_point(|o| o.ms as i64 <= a),
            None => 0,
        };
        let mut out = String::new();
        for o in ops[from..].iter().take(count.clamp(1, 1000)) {
            out.push_str(&self.line(o, o.ver > 0));
            out.push('\n');
        }
        out
    }

    pub fn doc(&self, did: &str) -> Option<serde_json::Value> {
        let (g, i) = self.layout.parse_did(did)?;
        let mut d = self.layout.doc(g, i);
        let ver = self.current(g, i);
        if ver > 0 {
            d["verificationMethod"][0]["publicKeyMultibase"] = json!(self.multibase(g, i, ver));
        }
        Some(d)
    }

    pub fn router(self: &Arc<Self>, fallback: Option<String>) -> Router {
        Router::new()
            .route("/export", get(export))
            .route("/xrpc/com.atproto.sync.listHosts", get(list_hosts))
            .route("/{did}", get(doc))
            .route("/_health", get(|| async { Json(json!({"version": "fakepds-plc"})) }))
            .with_state((self.clone(), fallback, reqwest::Client::new()))
    }
}

#[derive(serde::Deserialize)]
struct ListQuery {
    cursor: Option<String>,
    limit: Option<usize>,
}

/// Every host the fleet serves, then the extra ones, in pages; the cursor
/// is the next offset.
async fn list_hosts(State((p, _, _)): S, Query(q): Query<ListQuery>) -> Response {
    let n = p.list_requests.fetch_add(1, Relaxed) + 1;
    let every = p.throttle_every.load(Relaxed);
    if every > 0 && n % every == 0 {
        p.throttled.fetch_add(1, Relaxed);
        return (StatusCode::TOO_MANY_REQUESTS, [("retry-after", "1")]).into_response();
    }
    let total = p.hosts as u64 + p.list_extra.load(Relaxed);
    let from: u64 = q.cursor.as_deref().and_then(|c| c.parse().ok()).unwrap_or(0);
    let page = q.limit.unwrap_or(200).min(p.list_page.load(Relaxed) as usize).max(1) as u64;
    let to = (from + page).min(total);
    let hosts: Vec<serde_json::Value> = (from..to)
        .map(|i| {
            let name = if i < p.hosts as u64 {
                let u = p.layout.host_url(i as u32);
                u.split("://").nth(1).unwrap_or(&u).trim_end_matches('/').to_string()
            } else {
                format!("gone-{i}.fakepds.invalid")
            };
            json!({"hostname": name, "seq": i, "accountCount": 0, "status": "active"})
        })
        .collect();
    let mut out = json!({ "hosts": hosts });
    if to < total {
        out["cursor"] = json!(to.to_string());
    }
    Json(out).into_response()
}

#[derive(serde::Deserialize)]
struct ExportQuery {
    after: Option<String>,
    count: Option<usize>,
}

type S = State<(Arc<FakePlc>, Option<String>, reqwest::Client)>;

async fn export(State((p, _, _)): S, Query(q): Query<ExportQuery>) -> Response {
    let n = p.export_requests.fetch_add(1, Relaxed) + 1;
    let every = p.throttle_every.load(Relaxed);
    if every > 0 && n % every == 0 {
        p.throttled.fetch_add(1, Relaxed);
        return (StatusCode::TOO_MANY_REQUESTS, [("retry-after", "1")]).into_response();
    }
    if p.fail_next.load(Relaxed) > 0 {
        p.fail_next.fetch_sub(1, Relaxed);
        return (StatusCode::SERVICE_UNAVAILABLE, [("retry-after", "0")]).into_response();
    }
    p.afters.lock().push(q.after.clone().unwrap_or_default());
    let delay = p.export_delay_ms.load(Relaxed);
    if delay > 0 {
        tokio::time::sleep(std::time::Duration::from_millis(delay)).await;
    }
    let body = p.export(q.after.as_deref(), q.count.unwrap_or(10));
    ([("content-type", "application/jsonlines")], body).into_response()
}

async fn doc(State((p, fallback, client)): S, Path(did): Path<String>) -> Response {
    p.doc_fetches.fetch_add(1, Relaxed);
    if let Some(d) = p.doc(&did) {
        return Json(d).into_response();
    }
    if let Some(f) = fallback {
        return match client.get(format!("{}/{did}", f.trim_end_matches('/'))).send().await {
            Ok(r) => {
                let code = StatusCode::from_u16(r.status().as_u16()).unwrap_or(StatusCode::BAD_GATEWAY);
                (code, [("content-type", "application/json")], r.bytes().await.unwrap_or_default()).into_response()
            }
            Err(e) => (StatusCode::BAD_GATEWAY, Json(json!({"message": e.to_string()}))).into_response(),
        };
    }
    (StatusCode::NOT_FOUND, Json(json!({"message": format!("DID not registered: {did}")}))).into_response()
}
