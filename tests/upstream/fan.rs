//! A synthetic fan of upstream PDSes on one listener: host `name` lives at
//! `http://addr/name`, answers describeServer, and its subscribeRepos emits
//! `#commit`-shaped frames at a set rate. Each frame carries `sentNs` (unix
//! nanoseconds) so tests can measure latency through the relay.

use axum::Router;
use axum::extract::ws::{Message, WebSocket, WebSocketUpgrade};
use axum::extract::{Path, Query, State};
use axum::response::{IntoResponse, Response};
use axum::routing::get;
use futures::{SinkExt, StreamExt};
use parking_lot::Mutex;
use std::collections::HashMap;
use std::net::SocketAddr;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration;
use vlrelay::upstream::frame::{encode_error, encode_message};
use vlsync_atproto::cbor::Value;

#[derive(Clone, Copy, Debug)]
pub struct HostSpec {
    /// Events/s; 0 = idle, infinite = as fast as the socket takes them.
    pub rate: f64,
    pub size: usize,
    /// Never read the socket, so pings go unanswered.
    pub deaf: bool,
    /// Highest seq the host has; a cursor past it gets FutureCursor.
    pub max_seq: Option<i64>,
    /// Lowest seq still held; an older cursor gets OutdatedCursor first.
    pub min_seq: Option<i64>,
    /// The `time` each frame carries.
    pub stamp: Stamp,
}

/// What a frame's `time` says.
#[derive(Clone, Copy, Debug)]
pub enum Stamp {
    /// The same instant on every frame, long past.
    Fixed,
    /// When it was sent, give or take `skew_secs` (negative: behind).
    Sent { skew_secs: f64 },
    /// An hour-long backlog, or any `secs`: the events the host sent at
    /// `rate`/s over the `secs` before the socket opened, as fast as the
    /// socket takes them, then live at `rate`. (The spec's own `rate` is
    /// ignored.)
    Backlog { secs: f64, rate: f64 },
}

impl HostSpec {
    pub fn rate(rate: f64, size: usize) -> HostSpec {
        HostSpec { rate, size, deaf: false, max_seq: None, min_seq: None, stamp: Stamp::Fixed }
    }
}

#[derive(Default)]
struct FanHost {
    /// Highest seq emitted on any connection: a cursorless connect starts after it.
    head: AtomicU64,
    connects: AtomicU64,
}

pub struct Fan {
    pub addr: SocketAddr,
    specs: Mutex<HashMap<String, HostSpec>>,
    hosts: Mutex<HashMap<String, Arc<FanHost>>>,
    default: Mutex<Option<HostSpec>>,
}

impl Fan {
    pub async fn spawn() -> Arc<Fan> {
        Self::spawn_on("127.0.0.1:0").await
    }

    pub async fn spawn_on(bind: &str) -> Arc<Fan> {
        let l = tokio::net::TcpListener::bind(bind).await.unwrap();
        let fan = Arc::new(Fan {
            addr: l.local_addr().unwrap(),
            specs: Mutex::new(HashMap::new()),
            hosts: Mutex::new(HashMap::new()),
            default: Mutex::new(None),
        });
        let app = Router::new()
            .route("/{name}/xrpc/com.atproto.sync.subscribeRepos", get(subscribe))
            .route("/{name}/xrpc/com.atproto.server.describeServer", get(describe))
            .with_state(fan.clone());
        tokio::spawn(async move { axum::serve(l, app).await.unwrap() });
        fan
    }

    /// A handle to a fan served by another process, for its URLs.
    pub fn remote(addr: SocketAddr) -> Fan {
        Fan { addr, specs: Mutex::new(HashMap::new()), hosts: Mutex::new(HashMap::new()), default: Mutex::new(None) }
    }

    pub fn set(&self, name: &str, spec: HostSpec) {
        self.specs.lock().insert(name.to_string(), spec);
    }

    /// Spec for names never `set`.
    pub fn set_default(&self, spec: HostSpec) {
        *self.default.lock() = Some(spec);
    }

    pub fn url(&self, name: &str) -> String {
        format!("http://{}/{name}", self.addr)
    }

    pub fn connects(&self, name: &str) -> u64 {
        self.hosts.lock().get(name).map_or(0, |h| h.connects.load(Ordering::Relaxed))
    }

    fn spec(&self, name: &str) -> Option<HostSpec> {
        self.specs.lock().get(name).copied().or(*self.default.lock())
    }

    fn host(&self, name: &str) -> Arc<FanHost> {
        self.hosts.lock().entry(name.to_string()).or_default().clone()
    }
}

async fn describe(State(fan): State<Arc<Fan>>, Path(name): Path<String>) -> Response {
    if fan.spec(&name).is_none() {
        return axum::http::StatusCode::NOT_FOUND.into_response();
    }
    axum::Json(serde_json::json!({"did": format!("did:web:{name}.fan"), "availableUserDomains": []})).into_response()
}

async fn subscribe(
    State(fan): State<Arc<Fan>>,
    Path(name): Path<String>,
    Query(q): Query<HashMap<String, String>>,
    ws: WebSocketUpgrade,
) -> Response {
    let Some(spec) = fan.spec(&name) else {
        return axum::http::StatusCode::NOT_FOUND.into_response();
    };
    let cursor = q.get("cursor").and_then(|c| c.parse::<i64>().ok());
    let host = fan.host(&name);
    host.connects.fetch_add(1, Ordering::Relaxed);
    ws.max_message_size(1 << 20).on_upgrade(move |sock| serve(sock, name, spec, host, cursor))
}

pub fn now_ns() -> i64 {
    std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH).unwrap().as_nanos() as i64
}

pub fn frame(name: &str, seq: i64, size: usize) -> Vec<u8> {
    frame_at(name, seq, size, "2026-10-04T00:00:00.000Z")
}

pub fn frame_at(name: &str, seq: i64, size: usize, time: &str) -> Vec<u8> {
    encode_message(
        "#commit",
        &[
            ("seq", Value::Int(seq)),
            ("repo", Value::Text(format!("did:plc:{name}"))),
            ("rev", Value::Text("3l3qo2vutsw2b".into())),
            ("time", Value::Text(time.into())),
            ("blocks", Value::Bytes(vec![0x5a; size])),
            ("sentNs", Value::Int(now_ns())),
        ],
    )
}

/// `sentNs` of a fan frame.
pub fn sent_ns(f: &[u8]) -> i64 {
    let (_, n) = vlsync_atproto::cbor::ValueRef::decode_prefix(f).unwrap();
    match vlsync_atproto::cbor::ValueRef::decode(&f[n..]).unwrap().get("sentNs") {
        Some(vlsync_atproto::cbor::ValueRef::Int(t)) => *t,
        _ => panic!("no sentNs"),
    }
}

async fn serve(sock: WebSocket, name: String, spec: HostSpec, host: Arc<FanHost>, cursor: Option<i64>) {
    let (mut tx, mut rx) = sock.split();
    if !spec.deaf {
        // reading is what answers the relay's pings
        tokio::spawn(async move { while let Some(Ok(_)) = rx.next().await {} });
    } else {
        std::mem::forget(rx);
    }
    if let (Some(c), Some(max)) = (cursor, spec.max_seq)
        && c > max
    {
        let _ = tx.send(Message::Binary(encode_error("FutureCursor", "cursor in the future").into())).await;
        let _ = tx.close().await;
        return;
    }
    let mut seq = match cursor {
        Some(c) => c,
        None => host.head.load(Ordering::Relaxed) as i64,
    };
    if let (Some(c), Some(min)) = (cursor, spec.min_seq)
        && c < min
    {
        let info = encode_message(
            "#info",
            &[("name", Value::Text("OutdatedCursor".into())), ("message", Value::Text("too old".into()))],
        );
        if tx.send(Message::Binary(info.into())).await.is_err() {
            return;
        }
        seq = min - 1;
    }
    let rfc = |ns: i64| chrono::DateTime::from_timestamp_nanos(ns).to_rfc3339_opts(chrono::SecondsFormat::Micros, true);
    if let Stamp::Backlog { secs, rate } = spec.stamp {
        let start = now_ns() - (secs * 1e9) as i64;
        for i in 0i64.. {
            let at = start + (i as f64 * 1e9 / rate) as i64;
            let wait = at - now_ns();
            if wait > 0 {
                if tx.flush().await.is_err() {
                    return;
                }
                tokio::time::sleep(Duration::from_nanos(wait as u64)).await;
            }
            seq += 1;
            host.head.fetch_max(seq as u64, Ordering::Relaxed);
            if tx.feed(Message::Binary(frame_at(&name, seq, spec.size, &rfc(at)).into())).await.is_err() {
                return;
            }
            if i % 64 == 63 && tx.flush().await.is_err() {
                return;
            }
        }
    }
    if spec.rate <= 0.0 {
        std::future::pending::<()>().await;
    }
    let emit = |seq: &mut i64| {
        *seq += 1;
        host.head.fetch_max(*seq as u64, Ordering::Relaxed);
        let f = match spec.stamp {
            Stamp::Sent { skew_secs } => frame_at(&name, *seq, spec.size, &rfc(now_ns() + (skew_secs * 1e9) as i64)),
            _ => frame(&name, *seq, spec.size),
        };
        Message::Binary(f.into())
    };
    if spec.rate.is_infinite() {
        loop {
            for _ in 0..64 {
                if tx.feed(emit(&mut seq)).await.is_err() {
                    return;
                }
            }
            if tx.flush().await.is_err() {
                return;
            }
        }
    }
    // thousands of slow hosts on 5 ms ticks would swamp the fan's own runtime
    let tick = Duration::from_secs_f64((1.0 / spec.rate).clamp(0.005, 0.1));
    let mut t = tokio::time::interval(tick);
    t.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
    let mut owed = 0.0;
    loop {
        t.tick().await;
        owed += spec.rate * tick.as_secs_f64();
        while owed >= 1.0 {
            owed -= 1.0;
            if spec.max_seq.is_some_and(|m| seq >= m) {
                break;
            }
            if tx.feed(emit(&mut seq)).await.is_err() {
                return;
            }
        }
        if tx.flush().await.is_err() {
            return;
        }
    }
}
