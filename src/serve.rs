//! `com.atproto.sync.subscribeRepos` for consumers.
//!
//! This is vlpds's firehose (`vlpds::firehose::Firehose`) as is. The relay's
//! node logs use vlpds's segment format and bucket layout, so everything
//! that makes the vlpds firehose work carries over without changes:
//!
//! - The merger cuts every 2 ms at the minimum watermark over the followed
//!   logs, sorts by seq and frames each batch once. Every subscriber writes
//!   slices of those same bytes, on a separate runtime.
//! - A cursor inside the in-memory ring is served from memory. An older one
//!   is backfilled from the log segments in the bucket (every log, dead ones
//!   included, merged by seq) and handed to the ring at its floor.
//! - A subscriber that falls out of the ring catches up from the bucket
//!   again while it's within `max_lag_bytes` of the head, and gets
//!   `ConsumerTooSlow` past that.
//! - A cursor below the retained floor (`retain/` reports, which `seq::prune`
//!   raises before deleting) gets `#info OutdatedCursor` and continues from
//!   the oldest event left. A cursor above both the head and the clock gets
//!   `FutureCursor`.
//!
//! What this module adds is the relay's wiring: one-node startup (fence the
//! earlier logs, start above their seqs, follow our own log), the axum
//! route, and the retention loop.

use crate::seq::{self, LogConfig, NodeLog};
use axum::extract::{ConnectInfo, Query, Request, State};
use axum::response::Response;
use serde::Deserialize;
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::mpsc;
use vlpds::firehose::{self, Firehose, Source};
use vlpds::store::Store;

#[derive(Clone, Debug)]
pub struct ServeConfig {
    /// Merged batches kept in memory for recent cursors and slow readers.
    pub ring_bytes: usize,
    /// A live subscriber further than this behind the head is cut off.
    pub max_lag_bytes: usize,
    pub readahead_bytes: usize,
    pub backfill_cache_bytes: usize,
    pub max_backfills: usize,
    pub max_per_ip: usize,
    /// Threads of the subscriber runtime.
    pub threads: usize,
    pub retention: Duration,
    pub retention_interval: Duration,
}

impl Default for ServeConfig {
    fn default() -> Self {
        ServeConfig {
            ring_bytes: 512 << 20,
            max_lag_bytes: firehose::DEFAULT_MAX_LAG_BYTES,
            readahead_bytes: vlpds::backfill::DEFAULT_READAHEAD_BYTES,
            backfill_cache_bytes: vlpds::backfill::DEFAULT_CACHE_BYTES,
            max_backfills: firehose::DEFAULT_MAX_BACKFILLS,
            max_per_ip: firehose::DEFAULT_MAX_PER_IP,
            threads: 4,
            retention: seq::DEFAULT_RETENTION,
            retention_interval: Duration::from_secs(60),
        }
    }
}

pub struct Serve {
    pub firehose: Arc<Firehose>,
    pub store: Store,
    cfg: ServeConfig,
}

impl Serve {
    /// A firehose over the logs in `store`, with no sources yet. Its start
    /// floor is the clock now: older events are served from the bucket.
    pub fn new(store: Store, cfg: ServeConfig, runtime: Option<tokio::runtime::Handle>) -> Arc<Serve> {
        let opts = firehose::Options {
            ring_bytes: cfg.ring_bytes,
            max_lag_bytes: cfg.max_lag_bytes,
            readahead_bytes: cfg.readahead_bytes,
            backfill_cache_bytes: cfg.backfill_cache_bytes,
            max_backfills: cfg.max_backfills,
            max_per_ip: cfg.max_per_ip,
            write_idle: firehose::DEFAULT_WRITE_IDLE,
            runtime,
        };
        let fh = Firehose::new(opts);
        *fh.store.write() = Some(store.clone());
        Arc::new(Serve { firehose: fh, store, cfg })
    }

    /// Follows a log of this process (its watermark is read directly).
    pub fn follow_local(&self, log: &NodeLog) {
        self.firehose.set_source(&log.log_id, Some(Source::Local(log.wm.clone())));
    }

    pub fn router(self: &Arc<Self>) -> axum::Router {
        axum::Router::new()
            .route("/xrpc/com.atproto.sync.subscribeRepos", axum::routing::get(subscribe_repos))
            .with_state(self.clone())
    }

    /// Prunes every log past the retention window, every interval.
    pub fn spawn_retention(self: &Arc<Self>, reporter: String) {
        let s = self.clone();
        tokio::spawn(async move {
            let mut tick = tokio::time::interval(s.cfg.retention_interval);
            loop {
                tick.tick().await;
                match seq::prune(&s.store, &reporter, s.cfg.retention, 10_000).await {
                    Ok(p) if p.deleted > 0 => tracing::info!(deleted = p.deleted, pruned_seq = p.pruned_seq, "log retention"),
                    Ok(_) => {}
                    Err(e) => tracing::warn!("log retention failed: {e:#}"),
                }
            }
        });
    }
}

#[derive(Deserialize)]
struct SubscribeParams {
    cursor: Option<i64>,
}

async fn subscribe_repos(State(s): State<Arc<Serve>>, Query(q): Query<SubscribeParams>, req: Request) -> Response {
    let client = req.extensions().get::<ConnectInfo<std::net::SocketAddr>>().map(|c| c.0.ip());
    s.firehose.upgrade(req, q.cursor, None, client)
}

/// A running single-node relay log with its firehose.
pub struct Started {
    pub log: Arc<NodeLog>,
    pub serve: Arc<Serve>,
    pub recovered: seq::Recovered,
}

/// One-node startup. Every log already in the bucket is an earlier
/// incarnation of this node, so it's fenced (a zombie writer fails its next
/// PUT), and the new log's seqs start above everything it holds. The new
/// log's id is `cfg.log_id`; its floor is raised as needed.
pub async fn start_single_node(
    store: Store,
    mut cfg: LogConfig,
    serve: ServeConfig,
    runtime: Option<tokio::runtime::Handle>,
    on_fatal: Option<Box<dyn FnOnce(&seq::LogError) + Send>>,
) -> anyhow::Result<Started> {
    let recovered = seq::fence_all(&store, &cfg.log_id).await?;
    // The firehose's start floor is the clock, and backfill serves only
    // events at or below it. An earlier log with seqs past the clock (a
    // clock that stepped back) would leave a gap, so wait it out.
    let deadline = std::time::Instant::now() + Duration::from_secs(10);
    while vlpds::nodelog::seq_floor(vlpds::tid::now_micros()) <= recovered.seq_floor {
        anyhow::ensure!(
            std::time::Instant::now() < deadline,
            "earlier logs hold seqs more than 10 s past the clock (seq {})",
            recovered.seq_floor
        );
        tokio::time::sleep(Duration::from_millis(5)).await;
    }
    let srv = Serve::new(store.clone(), serve, runtime);
    cfg.seq_floor = cfg.seq_floor.max(srv.firehose.position()).max(recovered.seq_floor);
    let (tx, rx) = mpsc::unbounded_channel();
    let log = NodeLog::start(store, cfg, tx, on_fatal);
    srv.follow_local(&log);
    srv.firehose.spawn_merger(rx);
    Ok(Started { log, serve: srv, recovered })
}
