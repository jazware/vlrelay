//! The stream side: per-host emitters that turn queued events into seq'd
//! frames at the target rate, the replay ring behind cursors, the host's
//! xrpc surface, and the fake PLC directory.

use super::fleet::Layout;
use super::generate::{HostFaults, HostQueue, Pending, Repos};
use axum::Router;
use axum::extract::ws::{Message, WebSocket, WebSocketUpgrade};
use axum::extract::{Path, Query, State};
use axum::http::StatusCode;
use axum::response::{IntoResponse, Json, Response};
use axum::routing::get;
use bytes::Bytes;
use futures::SinkExt;
use parking_lot::Mutex;
use rand::rngs::StdRng;
use rand::{Rng, SeedableRng};
use serde_json::json;
use std::collections::{HashMap, VecDeque};
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicU64, AtomicUsize, Ordering};
use std::time::{Duration, Instant};
use tokio::sync::{broadcast, watch};
use vlpds::cbor::{write_int, write_map_head, write_text};
use vlpds::cid::Cid;
use vlpds::tid::Tid;

pub type Batch = Arc<Vec<(i64, Bytes)>>;

struct Ring {
    items: VecDeque<(i64, Bytes)>,
    bytes: usize,
    cap: usize,
    seq: i64,
}

pub struct HostState {
    pub g: u32,
    pub layout: Layout,
    pub faults: HostFaults,
    pub queue: Arc<HostQueue>,
    ring: Mutex<Ring>,
    tx: broadcast::Sender<Batch>,
    /// Each account's repo as the generators last left it, which is up to
    /// the pre-generated pool ahead of the stream.
    pub repos: Arc<Repos>,
    stalled: AtomicBool,
    down_until: Mutex<Option<Instant>>,
    kill: watch::Sender<u64>,
    pub subs: AtomicUsize,
    pub emitted: AtomicU64,
    pub emitted_bytes: AtomicU64,
    pub starved: AtomicU64,
}

impl HostState {
    pub fn new(
        g: u32,
        layout: Layout,
        faults: HostFaults,
        queue: Arc<HostQueue>,
        repos: Arc<Repos>,
        ring_cap: usize,
        broadcast_cap: usize,
    ) -> Arc<HostState> {
        Arc::new(HostState {
            g,
            layout,
            faults,
            queue,
            ring: Mutex::new(Ring { items: VecDeque::new(), bytes: 0, cap: ring_cap, seq: 0 }),
            tx: broadcast::channel(broadcast_cap).0,
            repos,
            stalled: AtomicBool::new(false),
            down_until: Mutex::new(None),
            kill: watch::channel(0).0,
            subs: AtomicUsize::new(0),
            emitted: AtomicU64::new(0),
            emitted_bytes: AtomicU64::new(0),
            starved: AtomicU64::new(0),
        })
    }

    /// Sequences `items`, keeps them for cursors and sends them live. The
    /// ring and the broadcast move under one lock, so a subscriber that
    /// snapshots the ring and subscribes under it sees no gap and no overlap.
    pub fn publish(&self, items: &[Pending], now: &str) {
        if items.is_empty() {
            return;
        }
        let mut frames = Vec::with_capacity(items.len());
        let mut bytes = 0u64;
        let mut ring = self.ring.lock();
        for p in items {
            ring.seq += 1;
            let mut out = Vec::new();
            p.finish(ring.seq, now, &mut out);
            bytes += out.len() as u64;
            frames.push((ring.seq, Bytes::from(out)));
        }
        for (s, f) in &frames {
            ring.bytes += f.len();
            ring.items.push_back((*s, f.clone()));
        }
        while ring.bytes > ring.cap && ring.items.len() > 1 {
            let (_, f) = ring.items.pop_front().expect("non-empty");
            ring.bytes -= f.len();
        }
        let _ = self.tx.send(Arc::new(frames));
        drop(ring);
        self.emitted.fetch_add(items.len() as u64, Ordering::Relaxed);
        self.emitted_bytes.fetch_add(bytes, Ordering::Relaxed);
    }

    /// The replay fault: the last `n` frames again, with their old seqs.
    fn replay(&self, n: usize) {
        let ring = self.ring.lock();
        let skip = ring.items.len().saturating_sub(n);
        let again: Vec<_> = ring.items.iter().skip(skip).cloned().collect();
        if !again.is_empty() {
            let _ = self.tx.send(Arc::new(again));
        }
    }

    fn disconnect_all(&self, down: Duration) {
        *self.down_until.lock() = Some(Instant::now() + down);
        self.kill.send_modify(|k| *k += 1);
    }
}

/// Per-host rate shaping: a mean-reverting log-normal multiplier per host
/// and occasional fleet-wide bursts per emitter thread.
pub struct Shape {
    pub sigma: f64,
    pub half_life_s: f64,
    pub burst_x: f64,
    pub burst_ms: f64,
    pub bursts_per_min: f64,
}

struct Emit {
    host: Arc<HostState>,
    rate: f64,
    tokens: f64,
    log_m: f64,
    next_stall: Option<Instant>,
    stall_end: Option<Instant>,
    next_disconnect: Option<Instant>,
    next_replay: Option<Instant>,
}

/// One emitter thread for `hosts`. `rates` are events/s per host.
pub fn run_emitter(
    hosts: Vec<(Arc<HostState>, f64)>,
    tick: Duration,
    shape: Arc<Shape>,
    stop: Arc<AtomicBool>,
    rate_scale: Arc<AtomicU64>,
) {
    let mut rng = StdRng::from_entropy();
    let start = Instant::now();
    let at = |s: f64| Some(start + Duration::from_secs_f64(s));
    let mut es: Vec<Emit> = hosts
        .into_iter()
        .map(|(host, rate)| {
            let f = host.faults.clone();
            Emit {
                host,
                rate,
                tokens: 0.0,
                log_m: 0.0,
                next_stall: f.stall.and_then(|(_, every)| at(every)),
                stall_end: None,
                next_disconnect: f.disconnect.and_then(|(every, _)| at(every)),
                next_replay: f.replay.and_then(|(every, _)| at(every)),
            }
        })
        .collect();
    // E[exp(N(0, s^2))] = exp(s^2/2): divide it out so the mean rate holds
    let norm = (-shape.sigma * shape.sigma / 2.0).exp();
    let theta = std::f64::consts::LN_2 / shape.half_life_s.max(0.01);
    let mut burst_until: Option<Instant> = None;
    let mut last = Instant::now();
    let mut next = last + tick;
    let mut buf = Vec::new();
    while !stop.load(Ordering::Relaxed) {
        let now_i = Instant::now();
        if next > now_i {
            std::thread::sleep(next - now_i);
        }
        next += tick;
        let now_i = Instant::now();
        let dt = (now_i - last).as_secs_f64();
        last = now_i;
        let scale = f64::from_bits(rate_scale.load(Ordering::Relaxed));
        if burst_until.is_none_or(|b| now_i > b) {
            burst_until = None;
            if rng.r#gen::<f64>() < shape.bursts_per_min / 60.0 * dt {
                burst_until = Some(now_i + Duration::from_secs_f64(shape.burst_ms / 1000.0));
            }
        }
        let burst = if burst_until.is_some() { shape.burst_x } else { 1.0 };
        let now = vlpds::events::now_rfc3339();
        for e in es.iter_mut() {
            let f = &e.host.faults;
            if let (Some((secs, every)), Some(t)) = (f.stall, e.next_stall) {
                if now_i >= t {
                    e.host.stalled.store(true, Ordering::Relaxed);
                    e.stall_end = Some(now_i + Duration::from_secs_f64(secs));
                    e.next_stall = Some(t + Duration::from_secs_f64(every));
                }
            }
            if e.stall_end.is_some_and(|t| now_i >= t) {
                e.host.stalled.store(false, Ordering::Relaxed);
                e.stall_end = None;
            }
            if let (Some((every, down)), Some(t)) = (f.disconnect, e.next_disconnect) {
                if now_i >= t {
                    e.host.disconnect_all(Duration::from_secs_f64(down));
                    e.next_disconnect = Some(t + Duration::from_secs_f64(every));
                }
            }
            if let (Some((every, n)), Some(t)) = (f.replay, e.next_replay) {
                if now_i >= t {
                    e.host.replay(n);
                    e.next_replay = Some(t + Duration::from_secs_f64(every));
                }
            }
            // Ornstein-Uhlenbeck on log(rate multiplier)
            e.log_m += -theta * e.log_m * dt + shape.sigma * (2.0 * theta * dt).sqrt() * normal(&mut rng);
            let lambda = e.rate * scale * dt * e.log_m.exp() * norm * burst;
            // at most a second of backlog: a starved queue doesn't turn into a flood
            e.tokens = (e.tokens + lambda).min(e.rate * scale + 1.0);
            let want = (e.tokens + rng.r#gen::<f64>() - 0.5).max(0.0).floor() as usize;
            if want == 0 {
                continue;
            }
            buf.clear();
            e.host.queue.pop(want, &mut buf);
            e.tokens -= buf.len() as f64;
            if buf.len() < want {
                // the generators are behind: emit what there is, owe nothing
                e.host.starved.fetch_add((want - buf.len()) as u64, Ordering::Relaxed);
                e.tokens = 0.0;
            }
            e.host.publish(&buf, &now);
        }
    }
}

fn normal(rng: &mut StdRng) -> f64 {
    let u1: f64 = rng.r#gen::<f64>().max(1e-12);
    let u2: f64 = rng.r#gen();
    (-2.0 * u1.ln()).sqrt() * (std::f64::consts::TAU * u2).cos()
}

pub fn router(h: Arc<HostState>) -> Router {
    Router::new()
        .route("/xrpc/com.atproto.sync.subscribeRepos", get(subscribe))
        .route("/xrpc/com.atproto.server.describeServer", get(describe))
        .route("/xrpc/com.atproto.sync.listRepos", get(list_repos))
        .route("/xrpc/com.atproto.sync.getRepoStatus", get(repo_status))
        .route("/xrpc/com.atproto.sync.getLatestCommit", get(latest_commit))
        .route("/xrpc/com.atproto.sync.getRepo", get(get_repo))
        .route("/xrpc/_health", get(|| async { Json(json!({"version": "fakepds"})) }))
        .fallback(|| async {
            xrpc_err(StatusCode::NOT_IMPLEMENTED, "MethodNotImplemented", "fakepds doesn't serve this")
        })
        .with_state(h)
}

fn xrpc_err(code: StatusCode, error: &str, message: &str) -> Response {
    (code, Json(json!({"error": error, "message": message}))).into_response()
}

async fn describe(State(h): State<Arc<HostState>>) -> Json<serde_json::Value> {
    Json(json!({
        "did": format!("did:web:h{}.fakepds.test", h.g),
        "availableUserDomains": [format!(".h{}.fakepds.test", h.g)],
        "inviteCodeRequired": false,
        "phoneVerificationRequired": false,
        "links": {},
        "contact": {},
    }))
}

fn own_did(h: &HostState, q: &HashMap<String, String>) -> Result<(String, u32), Response> {
    let did = q.get("did").cloned().unwrap_or_default();
    match h.layout.parse_did(&did) {
        Some((g, i)) if g == h.g => Ok((did, i)),
        _ => Err(xrpc_err(StatusCode::BAD_REQUEST, "RepoNotFound", "Could not find repo")),
    }
}

async fn list_repos(State(h): State<Arc<HostState>>, Query(q): Query<HashMap<String, String>>) -> Response {
    let limit = q.get("limit").and_then(|v| v.parse::<usize>().ok()).unwrap_or(500).clamp(1, 1000);
    let after = q.get("cursor").and_then(|v| v.parse::<u32>().ok());
    let mut heads: Vec<(u32, Cid, Tid)> = h.repos.heads();
    heads.retain(|x| after.is_none_or(|a| x.0 > a));
    heads.sort_unstable_by_key(|x| x.0);
    heads.truncate(limit);
    let cursor = (heads.len() == limit).then(|| heads.last().map(|x| x.0.to_string())).flatten();
    let repos: Vec<_> = heads
        .iter()
        .map(|(i, c, r)| json!({"did": h.layout.did(h.g, *i), "head": c.to_string(), "rev": r.to_string(), "active": true}))
        .collect();
    let mut body = json!({ "repos": repos });
    if let Some(c) = cursor {
        body["cursor"] = json!(c);
    }
    Json(body).into_response()
}

async fn repo_status(State(h): State<Arc<HostState>>, Query(q): Query<HashMap<String, String>>) -> Response {
    let (did, i) = match own_did(&h, &q) {
        Ok(x) => x,
        Err(r) => return r,
    };
    let mut body = json!({"did": did, "active": true});
    if let Some(s) = h.repos.get(i) {
        body["rev"] = json!(s.rev.to_string());
    }
    Json(body).into_response()
}

async fn latest_commit(State(h): State<Arc<HostState>>, Query(q): Query<HashMap<String, String>>) -> Response {
    let (_, i) = match own_did(&h, &q) {
        Ok(x) => x,
        Err(r) => return r,
    };
    match h.repos.get(i) {
        Some(s) => Json(json!({"cid": s.commit.to_string(), "rev": s.rev.to_string()})).into_response(),
        None => xrpc_err(StatusCode::BAD_REQUEST, "RepoNotFound", "no commit yet"),
    }
}

/// The whole repo from one snapshot, so its root is what getLatestCommit
/// returned at the same moment. `since` is ignored: always the full repo.
async fn get_repo(State(h): State<Arc<HostState>>, Query(q): Query<HashMap<String, String>>) -> Response {
    let (_, i) = match own_did(&h, &q) {
        Ok(x) => x,
        Err(r) => return r,
    };
    let Some(s) = h.repos.get(i) else {
        return xrpc_err(StatusCode::BAD_REQUEST, "RepoNotFound", "no commit yet");
    };
    match s.car() {
        Ok(car) => ([(axum::http::header::CONTENT_TYPE, "application/vnd.ipld.car")], car).into_response(),
        Err(e) => xrpc_err(StatusCode::INTERNAL_SERVER_ERROR, "InternalServerError", &e.to_string()),
    }
}

fn info_frame(name: &str, message: &str) -> Bytes {
    let mut out = Vec::new();
    write_map_head(&mut out, 2);
    write_text(&mut out, "t");
    write_text(&mut out, "#info");
    write_text(&mut out, "op");
    write_int(&mut out, 1);
    write_map_head(&mut out, 2);
    write_text(&mut out, "name");
    write_text(&mut out, name);
    write_text(&mut out, "message");
    write_text(&mut out, message);
    Bytes::from(out)
}

async fn subscribe(
    State(h): State<Arc<HostState>>,
    Query(q): Query<HashMap<String, String>>,
    ws: WebSocketUpgrade,
) -> Response {
    if h.down_until.lock().is_some_and(|t| Instant::now() < t) {
        return xrpc_err(StatusCode::SERVICE_UNAVAILABLE, "Unavailable", "disconnect fault");
    }
    let cursor = match q.get("cursor").map(|v| v.parse::<i64>()) {
        Some(Ok(c)) if c >= 0 => Some(c),
        Some(_) => return xrpc_err(StatusCode::BAD_REQUEST, "InvalidRequest", "bad cursor"),
        None => None,
    };
    ws.max_message_size(1 << 20).on_upgrade(move |sock| stream(h, sock, cursor))
}

async fn stream(h: Arc<HostState>, mut sock: WebSocket, cursor: Option<i64>) {
    h.subs.fetch_add(1, Ordering::Relaxed);
    let _guard = scopeguard(&h);
    let mut kill = h.kill.subscribe();
    kill.borrow_and_update();
    let snap = {
        let ring = h.ring.lock();
        let rx = h.tx.subscribe();
        match cursor {
            None => Some((rx, Vec::new(), None)),
            Some(c) if c > ring.seq => None,
            Some(c) => {
                let oldest = ring.items.front().map_or(ring.seq + 1, |x| x.0);
                let info = (c + 1 < oldest)
                    .then(|| info_frame("OutdatedCursor", "Requested cursor exceeded limit. Possibly missing events"));
                let start = ring.items.partition_point(|x| x.0 <= c);
                Some((rx, ring.items.iter().skip(start).map(|x| x.1.clone()).collect::<Vec<_>>(), info))
            }
        }
    };
    let Some((mut rx, backlog, info)) = snap else {
        let f = vlpds::events::error_frame("FutureCursor", "Cursor in the future.");
        let _ = sock.send(Message::Binary(Bytes::from(f))).await;
        let _ = sock.send(Message::Close(None)).await;
        return;
    };
    if let Some(i) = info {
        if sock.send(Message::Binary(i)).await.is_err() {
            return;
        }
    }
    for chunk in backlog.chunks(256) {
        for f in chunk {
            if sock.feed(Message::Binary(f.clone())).await.is_err() {
                return;
            }
        }
        if sock.flush().await.is_err() {
            return;
        }
    }
    drop(backlog);
    loop {
        tokio::select! {
            r = rx.recv() => match r {
                Ok(batch) => {
                    while h.stalled.load(Ordering::Relaxed) {
                        if kill.has_changed().unwrap_or(true) {
                            return;
                        }
                        tokio::time::sleep(Duration::from_millis(20)).await;
                    }
                    for (_, f) in batch.iter() {
                        if sock.feed(Message::Binary(f.clone())).await.is_err() {
                            return;
                        }
                    }
                    if sock.flush().await.is_err() {
                        return;
                    }
                }
                Err(broadcast::error::RecvError::Lagged(_)) => {
                    let f = vlpds::events::error_frame("ConsumerTooSlow", "Stream consumer too slow");
                    let _ = sock.send(Message::Binary(Bytes::from(f))).await;
                    let _ = sock.send(Message::Close(None)).await;
                    return;
                }
                Err(broadcast::error::RecvError::Closed) => return,
            },
            _ = kill.changed() => {
                let _ = sock.send(Message::Close(None)).await;
                return;
            }
            m = sock.recv() => match m {
                None | Some(Err(_)) | Some(Ok(Message::Close(_))) => return,
                _ => {}
            },
        }
    }
}

struct SubGuard<'a>(&'a HostState);

impl Drop for SubGuard<'_> {
    fn drop(&mut self) {
        self.0.subs.fetch_sub(1, Ordering::Relaxed);
    }
}

fn scopeguard(h: &HostState) -> SubGuard<'_> {
    SubGuard(h)
}

/// `GET /{did}`: the document any fleet DID resolves to.
pub fn plc_router(layout: Layout) -> Router {
    Router::new()
        .route(
            "/{did}",
            get(|State(l): State<Layout>, Path(did): Path<String>| async move {
                match l.parse_did(&did) {
                    Some((g, i)) => Json(l.doc(g, i)).into_response(),
                    None => (StatusCode::NOT_FOUND, Json(json!({"message": format!("DID not registered: {did}")})))
                        .into_response(),
                }
            }),
        )
        .route("/_health", get(|| async { Json(json!({"version": "fakepds-plc"})) }))
        .with_state(layout)
}
