//! Serving the quorum log through vlpds's firehose (docs/quorum.md,
//! "Implementation notes"): one followed log whose watermark is the commit
//! index, fed only committed entries, with seqs counted from the log's base
//! rather than the clock (`firehose::Options::start_floor`).

use axum::extract::{Query, Request, State};
use axum::response::{IntoResponse, Response};
use bytes::Bytes;
use std::sync::atomic::{AtomicI64, AtomicU64, Ordering};
use std::sync::{Arc, OnceLock};
use tokio::sync::mpsc;
use vlpds::firehose::{self, Firehose};
use vlpds::nodelog::LogBatch;

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
}

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
        })
    }

    /// The firehose, once this node has emitted anything (its start floor
    /// is where its first emission began).
    pub fn firehose(&self) -> Option<&Arc<Firehose>> {
        self.live.get().map(|l| &l.fh)
    }

    /// Committed entries `(after, upto]`, in order. The batch goes in before
    /// the watermark moves: the merger reads the watermark, then drains, so
    /// it never holds a watermark past events it hasn't been given.
    pub fn emit(&self, after: u64, upto: u64, events: Vec<(i64, Bytes)>) {
        let live = self.live.get_or_init(|| {
            let fh = Firehose::new(firehose::Options {
                ring_bytes: self.ring_bytes,
                start_floor: Some(after as i64),
                ..firehose::Options::default()
            });
            let (_, wm) = fh.add_remote(LOG_ID);
            let (tx, rx) = mpsc::unbounded_channel();
            fh.spawn_merger(rx);
            Live { fh, tx, wm }
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

#[derive(serde::Deserialize)]
struct SubscribeParams {
    cursor: Option<i64>,
}

#[derive(serde::Deserialize)]
struct StatusParams {
    #[serde(default)]
    reset: bool,
}

/// subscribeRepos and `/qlog/status` for one node.
pub fn router(node: Arc<super::node::Node>) -> axum::Router {
    axum::Router::new()
        .route("/xrpc/com.atproto.sync.subscribeRepos", axum::routing::get(subscribe))
        .route("/qlog/status", axum::routing::get(status))
        .with_state(node)
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

async fn status(State(n): State<Arc<super::node::Node>>, Query(q): Query<StatusParams>) -> Response {
    let s = n.status_and(q.reset);
    if q.reset {
        n.stats.commit_us.lock().reset();
    }
    axum::Json(s).into_response()
}
