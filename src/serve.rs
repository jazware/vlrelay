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
use std::collections::BTreeMap;
use std::net::IpAddr;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Weak};
use std::time::{Duration, Instant};
use tokio::sync::mpsc;
use vlpds::firehose::{self, ConnStats, Firehose, Source};
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
    /// Spacing of the stream seq checkpoints (`seq::dense`).
    pub seq_checkpoint_every: Duration,
    /// Whether this node writes them (core nodes; edges and replicas only read).
    pub write_seq_checkpoints: bool,
    /// Frame bytes the merger may hold while it waits for the slowest
    /// log's watermark; past it a log spills to reading back from the bucket.
    pub merge_queue_bytes: usize,
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
            seq_checkpoint_every: seq::dense::DEFAULT_CHECKPOINT_EVERY,
            write_seq_checkpoints: true,
            merge_queue_bytes: MERGE_QUEUE_BYTES,
        }
    }
}

pub struct Serve {
    pub firehose: Arc<Firehose>,
    seqs: seq::dense::DenseSeqs,
    pub store: Store,
    cfg: ServeConfig,
    consumers: parking_lot::Mutex<BTreeMap<u64, Consumer>>,
    next_consumer: AtomicU64,
}

/// A connected subscribeRepos consumer, as the operator sees it.
#[derive(Clone, Debug, serde::Serialize)]
pub struct ConsumerSnapshot {
    pub id: u64,
    pub ip: IpAddr,
    pub user_agent: String,
    pub connected_since_ms: i64,
    pub start_cursor: Option<i64>,
    /// Newest seq it has been sent (0 = none yet).
    pub last_seq: i64,
    /// How far its position trails the stream head, in seq time.
    pub lag_ms: f64,
    pub events_per_sec: f64,
    pub bytes_per_sec: f64,
    pub backfilling: bool,
}

struct Consumer {
    ip: IpAddr,
    user_agent: String,
    connected_since_ms: i64,
    start_cursor: Option<i64>,
    stats: Arc<ConnStats>,
    /// (events, bytes, when) at the last sample
    sampled: (u64, u64, Instant),
    events_per_sec: f64,
    bytes_per_sec: f64,
}

const USER_AGENT_MAX: usize = 200;
/// vlpds's 256 MiB is half a second of a 100k/s stream of ~5.4 KB frames,
/// less than a linger plus one slow PUT. A cluster merges three logs, each
/// that far behind at times, and at 90k/s spilled every few seconds; the
/// read-back couldn't keep up and the stream fell 25 s behind.
pub const MERGE_QUEUE_BYTES: usize = 1 << 30;

const CONSUMER_SAMPLE: Duration = Duration::from_secs(1);

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
        fh.set_max_queue_bytes(cfg.merge_queue_bytes);
        *fh.store.write() = Some(store.clone());
        // JavaScript consumers need seqs below 2^53, and indigo's are dense
        let seqs = seq::dense::DenseSeqs::new(store.clone(), cfg.seq_checkpoint_every, cfg.write_seq_checkpoints);
        fh.set_renumber(Arc::new(seqs.clone()));
        let s = Arc::new(Serve {
            firehose: fh,
            seqs,
            store,
            cfg,
            consumers: parking_lot::Mutex::new(BTreeMap::new()),
            next_consumer: AtomicU64::new(1),
        });
        tokio::spawn(sample_consumers(Arc::downgrade(&s)));
        s
    }

    /// The newest `n` stream seq checkpoints this node knows (docs/seq.md).
    pub fn seq_checkpoints(&self, n: usize) -> Vec<(i64, i64)> {
        self.seqs.recent(n)
    }

    /// The connected consumers, by id.
    pub fn consumers(&self) -> Vec<ConsumerSnapshot> {
        let head = self.firehose.last_emitted.load(Ordering::Acquire);
        let m = self.consumers.lock();
        m.iter()
            .filter(|(_, c)| !c.stats.is_closed())
            .map(|(&id, c)| {
                let last_seq = c.stats.last_seq.load(Ordering::Relaxed);
                let backfilling = c.stats.backfilling.load(Ordering::Relaxed);
                let pos = match (last_seq, backfilling) {
                    (0, true) => c.start_cursor.unwrap_or(head),
                    // live and sent nothing yet: nothing has been emitted since it joined at the head
                    (0, false) => head,
                    (s, _) => s,
                };
                // stream seqs are counts; their log keys are time (key >> 8 is unix µs)
                let key = |s: i64| self.firehose.key_at(s).unwrap_or(0) >> 8;
                let lag_ms = if pos < head { (key(head) - key(pos)).max(0) as f64 / 1000.0 } else { 0.0 };
                ConsumerSnapshot {
                    id,
                    ip: c.ip,
                    user_agent: c.user_agent.clone(),
                    connected_since_ms: c.connected_since_ms,
                    start_cursor: c.start_cursor,
                    last_seq,
                    lag_ms,
                    events_per_sec: c.events_per_sec,
                    bytes_per_sec: c.bytes_per_sec,
                    backfilling,
                }
            })
            .collect()
    }

    /// Disconnects consumer `id`; false if it's unknown or already gone.
    pub fn kick(&self, id: u64) -> bool {
        match self.consumers.lock().get(&id) {
            Some(c) if !c.stats.is_closed() => {
                c.stats.kick();
                true
            }
            _ => false,
        }
    }

    fn register(&self, ip: IpAddr, user_agent: String, start_cursor: Option<i64>) -> Arc<ConnStats> {
        let stats = Arc::new(ConnStats::default());
        let id = self.next_consumer.fetch_add(1, Ordering::Relaxed);
        let c = Consumer {
            ip,
            user_agent,
            connected_since_ms: chrono::Utc::now().timestamp_millis(),
            start_cursor,
            stats: stats.clone(),
            sampled: (0, 0, Instant::now()),
            events_per_sec: 0.0,
            bytes_per_sec: 0.0,
        };
        self.consumers.lock().insert(id, c);
        stats
    }

    /// Follows a log of this process (its watermark is read directly).
    pub fn follow_local(&self, log: &NodeLog) {
        self.firehose.set_source(&log.log_id, Some(Source::Local(log.wm.clone())));
        self.seqs.set_own_log(&log.log_id);
    }

    pub fn router(self: &Arc<Self>) -> axum::Router {
        axum::Router::new()
            .route("/xrpc/com.atproto.sync.subscribeRepos", axum::routing::get(subscribe_repos))
            .with_state(self.clone())
    }

    /// Prunes every log past the retention window, every interval.
    pub fn spawn_retention(self: &Arc<Self>, reporter: String) {
        if self.cfg.retention_interval.is_zero() {
            return;
        }
        let s = self.clone();
        tokio::spawn(async move {
            let mut tick = tokio::time::interval(s.cfg.retention_interval);
            loop {
                tick.tick().await;
                match seq::prune(&s.store, &reporter, s.cfg.retention, 10_000).await {
                    Ok(p) if p.deleted > 0 => {
                        tracing::info!(deleted = p.deleted, pruned_seq = p.pruned_seq, "log retention")
                    }
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
    let ua = req.headers().get(axum::http::header::USER_AGENT).map(|v| String::from_utf8_lossy(v.as_bytes()));
    let ua = ua.map(|u| u.chars().take(USER_AGENT_MAX).collect()).unwrap_or_default();
    let ip = client.unwrap_or(IpAddr::V4(std::net::Ipv4Addr::UNSPECIFIED));
    let stats = s.register(ip, ua, q.cursor);
    s.firehose.upgrade_tracked(req, q.cursor, None, client, Some(stats))
}

/// Per-consumer rates from the counters' deltas; forgets closed consumers.
async fn sample_consumers(serve: Weak<Serve>) {
    let mut tick = tokio::time::interval(CONSUMER_SAMPLE);
    tick.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
    loop {
        tick.tick().await;
        let Some(s) = serve.upgrade() else { return };
        let now = Instant::now();
        s.consumers.lock().retain(|_, c| {
            if c.stats.is_closed() {
                return false;
            }
            let (events, bytes) = (c.stats.events.load(Ordering::Relaxed), c.stats.bytes.load(Ordering::Relaxed));
            let secs = now.duration_since(c.sampled.2).as_secs_f64();
            if secs > 0.0 {
                c.events_per_sec = (events - c.sampled.0) as f64 / secs;
                c.bytes_per_sec = (bytes - c.sampled.1) as f64 / secs;
            }
            c.sampled = (events, bytes, now);
            true
        });
    }
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
/// log's id is `cfg.log_id`; its floor is raised as needed. A prefix with
/// cluster leases is refused: those logs aren't ours to fence.
pub async fn start_single_node(
    store: Store,
    mut cfg: LogConfig,
    serve: ServeConfig,
    runtime: Option<tokio::runtime::Handle>,
    on_fatal: Option<seq::OnFatal>,
) -> anyhow::Result<Started> {
    // fencing every log would kill a cluster's live nodes
    {
        use futures::StreamExt;
        let nodes = object_store::path::Path::from(format!("{}/nodes", store.prefix));
        if let Some(m) = store.raw.list(Some(&nodes)).next().await {
            let m = m?;
            anyhow::bail!(
                "this prefix holds cluster node leases ({}): start it as a cluster node (cluster::ClusterNode)",
                m.location
            );
        }
    }
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
    srv.spawn_retention(log.log_id.to_string());
    Ok(Started { log, serve: srv, recovered })
}

#[cfg(test)]
mod tests {
    use super::*;
    use futures::StreamExt;
    use std::net::SocketAddr;
    use tokio_tungstenite::tungstenite::client::IntoClientRequest;

    /// A consumer shows up with its address and user agent, and a kick
    /// disconnects it while it's idle (nothing is emitted) and forgets it.
    #[tokio::test]
    async fn consumers_are_listed_and_kicked() {
        let s = Serve::new(Store::memory(None), ServeConfig::default(), None);
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let app = s.router();
        tokio::spawn(async move {
            axum::serve(listener, app.into_make_service_with_connect_info::<SocketAddr>()).await.unwrap();
        });
        let mut req = format!("ws://{addr}/xrpc/com.atproto.sync.subscribeRepos").into_client_request().unwrap();
        req.headers_mut().insert("user-agent", "x".repeat(300).parse().unwrap());
        let (mut ws, _) = tokio_tungstenite::connect_async(req).await.unwrap();
        let c = s.consumers();
        assert_eq!(c.len(), 1);
        assert_eq!(c[0].id, 1);
        assert_eq!(c[0].ip, addr.ip());
        assert_eq!(c[0].user_agent, "x".repeat(USER_AGENT_MAX));
        assert_eq!((c[0].start_cursor, c[0].last_seq, c[0].lag_ms, c[0].backfilling), (None, 0, 0.0, false));
        assert!(!s.kick(2));
        assert!(s.kick(1));
        let closed = tokio::time::timeout(Duration::from_secs(5), async {
            loop {
                match ws.next().await {
                    None | Some(Err(_)) => return,
                    Some(Ok(m)) if m.is_close() => return,
                    Some(Ok(_)) => {}
                }
            }
        });
        closed.await.expect("the kicked socket stayed open");
        let t = Instant::now();
        while !s.consumers().is_empty() {
            assert!(t.elapsed() < Duration::from_secs(5), "the kicked consumer is still listed");
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
        assert!(!s.kick(1));
    }
}
