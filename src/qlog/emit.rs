//! Serving the quorum log through vlpds's firehose (docs/quorum.md,
//! "Implementation notes"): one followed log whose watermark is the commit
//! index, fed only committed entries, with seqs counted from the log's base
//! rather than the clock (`firehose::Options::start_floor`).

use axum::extract::{Query, Request, State};
use axum::response::{IntoResponse, Response};
use bytes::Bytes;
use std::sync::atomic::{AtomicI64, AtomicU64, Ordering};
use std::sync::{Arc, OnceLock, Weak};
use tokio::sync::mpsc;
use vlsync_firehose::firehose::{self, Firehose};
use vlsync_firehose::log::LogBatch;

pub const LOG_ID: &str = "qlog";

/// A batch a node emitted, for in-process checkers.
#[derive(Clone, Debug)]
pub struct Emitted {
    pub node: String,
    /// Bumped per process start: a restarted node is a new stream.
    pub incarnation: u64,
    pub events: Vec<(i64, Bytes)>,
}

struct Live {
    fh: Arc<Firehose>,
    tx: mpsc::UnboundedSender<LogBatch>,
    wm: Arc<AtomicI64>,
}

pub struct Emitter {
    node: String,
    incarnation: u64,
    ring_bytes: usize,
    live: OnceLock<Live>,
    ordinal: AtomicU64,
    tap: Option<mpsc::UnboundedSender<Emitted>>,
    /// Where the flush writes segments: cursors older than the ring are
    /// backfilled from there, and above the last flush from the node's own
    /// log (`set_local_tail`).
    store: Option<vlsync_store::store::Store>,
    owner: Arc<OnceLock<Weak<super::node::Node>>>,
    serving: Serving,
}

/// Who serves a node's firehose once it's made.
pub type OnStart = Box<dyn Fn(&Arc<Firehose>) + Send + Sync>;

type Serving = parking_lot::Mutex<Option<(firehose::Options, OnStart)>>;

impl Emitter {
    pub fn new(
        node: &str,
        incarnation: u64,
        ring_bytes: usize,
        tap: Option<mpsc::UnboundedSender<Emitted>>,
    ) -> Arc<Emitter> {
        Arc::new(Emitter {
            node: node.to_string(),
            incarnation,
            ring_bytes,
            live: OnceLock::new(),
            ordinal: AtomicU64::new(0),
            tap,
            store: None,
            owner: Arc::new(OnceLock::new()),
            serving: Default::default(),
        })
    }

    /// As `new`, backfilling old cursors from the flushed bucket segments.
    pub fn with_store(
        node: &str,
        incarnation: u64,
        ring_bytes: usize,
        tap: Option<mpsc::UnboundedSender<Emitted>>,
        store: vlsync_store::store::Store,
    ) -> Arc<Emitter> {
        let mut e = Arc::into_inner(Emitter::new(node, incarnation, ring_bytes, tap)).expect("just made");
        e.store = Some(store);
        Arc::new(e)
    }

    pub(crate) fn attach(&self, node: &Arc<super::node::Node>) {
        let _ = self.owner.set(Arc::downgrade(node));
    }

    /// The firehose, once this node has emitted anything (its start floor
    /// is where its first emission began).
    pub fn firehose(&self) -> Option<&Arc<Firehose>> {
        self.live.get().map(|l| &l.fh)
    }

    /// The relay's serving options for the firehose this node makes at its
    /// first emission, and who serves it once it's made.
    pub fn set_serving(&self, opts: firehose::Options, on_start: OnStart) {
        *self.serving.lock() = Some((opts, on_start));
    }

    fn make(&self, floor: u64, opts: firehose::Options) -> Live {
        let fh = Firehose::new(firehose::Options { start_floor: Some(floor as i64), ..opts });
        if let Some(store) = &self.store {
            *fh.store.write() = Some(store.clone());
            // one followed log never waits on another, so nothing queues;
            // and a spill's read-back would want bucket ordinals, which
            // these batches don't have
            fh.set_max_queue_bytes(usize::MAX);
            fh.set_local_tail(Arc::new(Tail(self.owner.clone())));
        }
        let (_, wm) = fh.add_remote(LOG_ID);
        let (tx, rx) = mpsc::unbounded_channel();
        fh.spawn_merger(rx);
        Live { fh, tx, wm }
    }

    /// Committed entries `(after, upto]`, in order. The batch goes in before
    /// the watermark moves: the merger reads the watermark, then drains, so
    /// it never holds a watermark past events it hasn't been given.
    pub fn emit(&self, after: u64, upto: u64, events: Vec<(i64, Bytes)>) {
        let live = self.live.get_or_init(|| match self.serving.lock().take() {
            Some((opts, on_start)) => {
                let live = self.make(after, opts);
                on_start(&live.fh);
                live
            }
            None => self.make(after, firehose::Options { ring_bytes: self.ring_bytes, ..firehose::Options::default() }),
        });
        if let Some(tap) = &self.tap {
            let _ =
                tap.send(Emitted { node: self.node.clone(), incarnation: self.incarnation, events: events.clone() });
        }
        let ordinal = self.ordinal.fetch_add(1, Ordering::Relaxed);
        let _ = live.tx.send(LogBatch { log_id: LOG_ID.into(), ordinal, events });
        live.wm.store(upto as i64, Ordering::Release);
    }
}

/// The node is attached after the emitter is made (and maybe after its
/// firehose starts), so it's looked up on each read.
struct Tail(Arc<OnceLock<Weak<super::node::Node>>>);

impl Tail {
    fn node(&self) -> Option<Arc<super::node::Node>> {
        self.0.get().and_then(|w| w.upgrade())
    }
}

impl firehose::LocalTail for Tail {
    fn floor(&self) -> i64 {
        self.node().map_or(i64::MAX, |n| n.readable_floor() as i64)
    }

    fn read(
        &self,
        after: i64,
        until: i64,
        max_bytes: usize,
    ) -> futures::future::BoxFuture<'_, anyhow::Result<Vec<(i64, Bytes)>>> {
        Box::pin(async move {
            let n = self.node().ok_or_else(|| anyhow::anyhow!("node gone"))?;
            let es = n.committed_chunk(after as u64 + 1, until as u64, max_bytes).await?;
            Ok(es.into_iter().map(|e| (e.seq as i64, e.data)).collect())
        })
    }
}

#[derive(serde::Deserialize)]
struct SubscribeParams {
    cursor: Option<i64>,
}

#[derive(serde::Deserialize)]
struct StatusParams {
    #[serde(default)]
    reset: bool,
}

/// Who may `POST /qlog/members`.
#[derive(Clone, Debug)]
pub enum Admin {
    /// `Authorization: Bearer <token>`.
    Token(String),
    /// Anyone who can reach the port: only for an http port bound to
    /// loopback (the chaos harness, tests).
    Open,
    /// Nobody: no token and a reachable port.
    Refused,
}

impl Admin {
    /// A token if one is set; otherwise open only on a loopback address.
    pub fn for_listener(token: Option<String>, addr: std::net::SocketAddr) -> Admin {
        match token.filter(|t| !t.is_empty()) {
            Some(t) => Admin::Token(t),
            None if addr.ip().is_loopback() => Admin::Open,
            None => Admin::Refused,
        }
    }

    fn allows(&self, h: &axum::http::HeaderMap) -> bool {
        match self {
            Admin::Open => true,
            Admin::Refused => false,
            Admin::Token(t) => h
                .get(axum::http::header::AUTHORIZATION)
                .and_then(|v| v.to_str().ok())
                .and_then(|v| v.strip_prefix("Bearer "))
                .is_some_and(|got| vlatproto::xrpc::token_eq(t, got)),
        }
    }
}

/// subscribeRepos, `/qlog/status`, `POST /qlog/members` (a membership
/// change, on the leader; gated by `admin`) and `/metrics` for one node.
pub fn router(node: Arc<super::node::Node>, admin: Admin) -> axum::Router {
    axum::Router::new()
        .route("/xrpc/com.atproto.sync.subscribeRepos", axum::routing::get(subscribe))
        .route("/qlog/status", axum::routing::get(status))
        .route("/qlog/members", axum::routing::post(members).layer(axum::Extension(Arc::new(admin))))
        .route("/metrics", axum::routing::get(metrics))
        .with_state(node)
}

/// `/qlog/status` and `POST /qlog/members` alone, for a relay node that
/// serves subscribeRepos and `/metrics` itself.
pub fn control_router(node: Arc<super::node::Node>, admin: Admin) -> axum::Router {
    axum::Router::new()
        .route("/qlog/status", axum::routing::get(status))
        .route("/qlog/members", axum::routing::post(members).layer(axum::Extension(Arc::new(admin))))
        .with_state(node)
}

/// The member set wanted, and addresses for nodes the leader can't dial yet.
#[derive(serde::Deserialize, serde::Serialize)]
pub struct MembersRequest {
    pub members: Vec<String>,
    #[serde(default)]
    pub addrs: std::collections::BTreeMap<String, String>,
}

async fn members(
    State(n): State<Arc<super::node::Node>>,
    axum::Extension(admin): axum::Extension<Arc<Admin>>,
    h: axum::http::HeaderMap,
    axum::Json(r): axum::Json<MembersRequest>,
) -> Response {
    use axum::http::StatusCode;
    if !admin.allows(&h) {
        let why = match *admin {
            Admin::Refused => "membership changes are off: start the node with --admin-token",
            _ => "a bearer admin token is required",
        };
        return (StatusCode::UNAUTHORIZED, axum::Json(serde_json::json!({ "error": why }))).into_response();
    }
    match n.change_members(r.members, r.addrs).await {
        Ok(s) => axum::Json(s).into_response(),
        Err(e) => {
            let code = match e.downcast_ref::<super::node::NotLeading>() {
                Some(_) => StatusCode::CONFLICT,
                None => StatusCode::INTERNAL_SERVER_ERROR,
            };
            (code, axum::Json(serde_json::json!({ "error": format!("{e:#}") }))).into_response()
        }
    }
}

async fn subscribe(
    State(n): State<Arc<super::node::Node>>,
    Query(q): Query<SubscribeParams>,
    req: Request,
) -> Response {
    match n.emit.firehose() {
        Some(fh) => fh.upgrade(req, q.cursor, None, None, None),
        None => (axum::http::StatusCode::SERVICE_UNAVAILABLE, "nothing committed yet").into_response(),
    }
}

async fn metrics() -> Response {
    use prometheus::Encoder;
    let mut buf = Vec::new();
    match prometheus::TextEncoder::new().encode(&prometheus::gather(), &mut buf) {
        Ok(()) => ([(axum::http::header::CONTENT_TYPE, "text/plain; version=0.0.4")], buf).into_response(),
        Err(e) => (axum::http::StatusCode::INTERNAL_SERVER_ERROR, e.to_string()).into_response(),
    }
}

async fn status(State(n): State<Arc<super::node::Node>>, Query(q): Query<StatusParams>) -> Response {
    let s = n.status_and(q.reset);
    if q.reset {
        n.stats.commit_us.lock().reset();
    }
    axum::Json(s).into_response()
}

#[cfg(test)]
mod admin_tests {
    use super::Admin;
    use axum::http::{HeaderMap, HeaderValue, header::AUTHORIZATION};

    fn with(auth: Option<&str>) -> HeaderMap {
        let mut h = HeaderMap::new();
        if let Some(a) = auth {
            h.insert(AUTHORIZATION, HeaderValue::from_str(a).unwrap());
        }
        h
    }

    #[test]
    fn membership_changes_need_the_token_off_loopback() {
        let lo = "127.0.0.1:3161".parse().unwrap();
        let public = "0.0.0.0:3161".parse().unwrap();
        assert!(Admin::for_listener(None, lo).allows(&with(None)));
        assert!(!Admin::for_listener(None, public).allows(&with(None)));
        assert!(!Admin::for_listener(Some(String::new()), public).allows(&with(Some("Bearer "))));
        let t = Admin::for_listener(Some("s3cret".into()), public);
        assert!(t.allows(&with(Some("Bearer s3cret"))));
        assert!(!t.allows(&with(Some("Bearer s3cre"))));
        assert!(!t.allows(&with(Some("Basic s3cret"))));
        assert!(!t.allows(&with(None)));
        // a token set binds loopback too
        assert!(!Admin::for_listener(Some("s3cret".into()), lo).allows(&with(None)));
    }
}
