//! Following every node's log for the merged firehose.
//!
//! The wire is vlpds's (`vlpds::remote`): a node streams its durable batches
//! and 5 ms watermark heartbeats at `/internal/v1/log/stream` over peer mTLS,
//! and a follower catches up from the bucket on every (re)connect, then
//! dedupes against the stream. So `vlpds::remote::follow_log` follows a relay
//! log unchanged, including draining a dead node's log from the bucket up to
//! its fence and retiring.
//!
//! Core and edge nodes follow over mTLS. A replica has no peer certificate,
//! so its followers only read the bucket: a segment shows up one poll after
//! its PUT, and an idle log holds the replica's merge until its next segment.
//!
//! Core nodes learn about a joiner through its greeting (vlpds's join
//! protocol makes the joiner wait until every core node follows its log).
//! Edges and replicas hold no lease, so nobody greets them: they list
//! `nodes/` every poll and follow each new log from their merger's position.
//! To keep a log they haven't listed yet from starting below that position,
//! their merge never settles past the start of their last listing minus
//! `guard` (a "membership" watermark source). A joiner's first seq comes
//! after its lease write by at least the join itself, so this holds unless
//! clocks disagree by more than `guard`.

use crate::seq::NodeLog;
use axum::extract::ws::{Message, WebSocket};
use parking_lot::Mutex;
use std::collections::{BTreeMap, HashMap};
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicI64, Ordering};
use std::time::{Duration, Instant};
use tokio::sync::{broadcast, mpsc};
use vlpds::cluster::NodeLease;
use vlpds::firehose::{Firehose, Source};
use vlpds::nodelog::LogBatch;
use vlpds::remote::{self, Follower};
use vlpds::store::Store;

const HEARTBEAT: Duration = Duration::from_millis(5);
const SEND_TIMEOUT: Duration = Duration::from_secs(10);
pub const MEMBERSHIP_SOURCE: &str = "~membership";

async fn send(ws: &mut WebSocket, m: Message) -> bool {
    matches!(tokio::time::timeout(SEND_TIMEOUT, ws.send(m)).await, Ok(Ok(())))
}

/// Owner side of a log stream. `closed` ends it (the node is leaving: its
/// followers drain the fenced log from the bucket instead).
pub async fn serve_stream(mut ws: WebSocket, log: Arc<NodeLog>, closed: Arc<AtomicBool>) {
    let mut rx = log.live();
    let mut tick = tokio::time::interval(HEARTBEAT);
    tick.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
    loop {
        tick.tick().await;
        if closed.load(Ordering::Acquire) {
            let _ = tokio::time::timeout(SEND_TIMEOUT, ws.send(Message::Close(None))).await;
            return;
        }
        // the watermark first: batches it covers were broadcast before it
        // moved, so they're already queued here
        let w = log.wm.get();
        loop {
            match rx.try_recv() {
                Ok(b) => {
                    if !send(&mut ws, Message::Binary(remote::encode_batch(&b))).await {
                        return;
                    }
                }
                Err(broadcast::error::TryRecvError::Empty) => break,
                Err(broadcast::error::TryRecvError::Lagged(_)) | Err(broadcast::error::TryRecvError::Closed) => {
                    tracing::warn!(log_id = %log.log_id, "peer fell behind our log stream: dropping it (it catches up from the bucket)");
                    let _ = tokio::time::timeout(SEND_TIMEOUT, ws.send(Message::Close(None))).await;
                    return;
                }
            }
        }
        if !send(&mut ws, Message::Binary(remote::encode_watermark(w))).await {
            return;
        }
    }
}

/// A live log to follow: whose it is and where it streams from.
#[derive(Clone, Debug)]
pub struct LiveLog {
    pub log_id: String,
    pub node_id: String,
    pub addr: String,
}

impl From<&NodeLease> for LiveLog {
    fn from(l: &NodeLease) -> Self {
        LiveLog { log_id: l.log_id.clone(), node_id: l.node_id.clone(), addr: l.addr.clone() }
    }
}

/// This node's followers of other nodes' logs.
pub struct Followers {
    pub fh: Arc<Firehose>,
    store: Store,
    merger_tx: mpsc::UnboundedSender<LogBatch>,
    token: String,
    /// None: bucket only (a replica).
    http: Option<vlpds::http::PeerClient>,
    own_log: Option<String>,
    map: Mutex<HashMap<String, Follower>>,
    /// The live logs as last synced, for the followers' address lookups.
    live: Arc<parking_lot::RwLock<HashMap<String, String>>>,
}

impl Followers {
    pub fn new(
        fh: Arc<Firehose>,
        store: Store,
        merger_tx: mpsc::UnboundedSender<LogBatch>,
        token: String,
        http: Option<vlpds::http::PeerClient>,
        own_log: Option<String>,
    ) -> Arc<Followers> {
        Arc::new(Followers {
            fh,
            store,
            merger_tx,
            token,
            http,
            own_log,
            map: Mutex::new(HashMap::new()),
            live: Default::default(),
        })
    }

    /// Follows every log in `live` not followed yet, and retires the
    /// followers of dead logs that have drained to their fence.
    pub fn sync(&self, live: &[LiveLog]) {
        *self.live.write() = live.iter().map(|l| (l.log_id.clone(), l.addr.clone())).collect();
        let mut f = self.map.lock();
        for l in live {
            if Some(&l.log_id) == self.own_log.as_ref() || f.contains_key(&l.log_id) {
                continue;
            }
            let (addr, tls): (Arc<dyn Fn() -> Option<String> + Send + Sync>, _) = match &self.http {
                Some(http) => {
                    let (map, id) = (self.live.clone(), l.log_id.clone());
                    (Arc::new(move || map.read().get(&id).cloned()), http.ws_connector(&l.node_id))
                }
                None => (Arc::new(|| None), None),
            };
            let fl = remote::follow_log(
                &l.log_id,
                &self.fh,
                self.store.clone(),
                addr,
                self.token.clone(),
                self.merger_tx.clone(),
                tls,
            );
            tracing::info!(log_id = %l.log_id, node = %l.node_id, floor = fl.floor, "following log");
            f.insert(l.log_id.clone(), fl);
        }
        let done: Vec<String> =
            f.iter().filter(|(_, fl)| fl.done.load(Ordering::Acquire)).map(|(k, _)| k.clone()).collect();
        for log_id in done {
            self.fh.set_source(&log_id, None);
            f.remove(&log_id);
            tracing::info!(%log_id, "dead log drained to its fence");
        }
    }

    /// Log -> the floor its follower delivers every event above.
    pub fn floors(&self) -> BTreeMap<String, i64> {
        self.map.lock().iter().map(|(k, f)| (k.clone(), f.floor)).collect()
    }

    pub fn followed(&self) -> Vec<String> {
        let mut v: Vec<String> = self.map.lock().keys().cloned().collect();
        v.sort();
        v
    }

    pub fn stop_all(&self) {
        for (_, f) in self.map.lock().drain() {
            f.stop.store(true, Ordering::Release);
        }
    }
}

struct Seen {
    renewals: u64,
    changed_at: Instant,
    lease: NodeLease,
}

/// Lease liveness for nodes without a lease of their own (edges and
/// replicas): the same rule as vlpds's, a lease unchanged for TTL + skew of
/// our monotonic time is dead.
pub struct LeaseWatch {
    store: Store,
    ttl: Duration,
    skew: Duration,
    seen: Mutex<HashMap<String, Seen>>,
}

impl LeaseWatch {
    pub fn new(store: Store, ttl: Duration, skew: Duration) -> LeaseWatch {
        LeaseWatch { store, ttl, skew, seen: Mutex::new(HashMap::new()) }
    }

    pub async fn live(&self) -> anyhow::Result<Vec<NodeLease>> {
        use futures::StreamExt;
        use object_store::ObjectStoreExt;
        let prefix = object_store::path::Path::from(format!("{}/nodes", self.store.prefix));
        let mut names = Vec::new();
        let mut list = self.store.raw.list(Some(&prefix));
        while let Some(m) = list.next().await {
            names.push(m?.location);
        }
        let leases: Vec<Option<NodeLease>> = futures::stream::iter(names)
            .map(|p| async move {
                match self.store.raw.get(&p).await {
                    Ok(r) => Ok(serde_json::from_slice::<NodeLease>(&r.bytes().await?).ok()),
                    Err(object_store::Error::NotFound { .. }) => Ok(None),
                    Err(e) => Err(anyhow::Error::from(e)),
                }
            })
            .buffered(16)
            .collect::<Vec<_>>()
            .await
            .into_iter()
            .collect::<anyhow::Result<_>>()?;
        let now = Instant::now();
        let mut seen = self.seen.lock();
        let mut present = std::collections::HashSet::new();
        for l in leases.into_iter().flatten() {
            present.insert(l.log_id.clone());
            let s = seen.entry(l.log_id.clone()).or_insert(Seen {
                renewals: l.renewals,
                changed_at: now,
                lease: l.clone(),
            });
            if s.renewals != l.renewals {
                s.renewals = l.renewals;
                s.changed_at = now;
            }
            s.lease = l;
        }
        seen.retain(|k, _| present.contains(k));
        Ok(seen
            .values()
            .filter(|s| now.saturating_duration_since(s.changed_at) <= self.ttl + self.skew)
            .map(|s| s.lease.clone())
            .collect())
    }
}

/// Edges and replicas: list the leases every `poll`, follow what's live,
/// and hold the merge below the last listing (see the module docs).
pub fn spawn_membership(
    followers: Arc<Followers>,
    watch: LeaseWatch,
    poll: Duration,
    guard: Duration,
    stop: Arc<AtomicBool>,
) {
    let wm = Arc::new(AtomicI64::new(followers.fh.position()));
    followers.fh.set_source(MEMBERSHIP_SOURCE, Some(Source::Remote(wm.clone())));
    tokio::spawn(async move {
        let mut tick = tokio::time::interval(poll);
        tick.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
        while !stop.load(Ordering::Acquire) {
            tick.tick().await;
            let started = vlpds::tid::now_micros();
            match watch.live().await {
                Ok(live) => {
                    let logs: Vec<LiveLog> = live.iter().map(LiveLog::from).collect();
                    followers.sync(&logs);
                    let bound = vlpds::nodelog::seq_floor(started.saturating_sub(guard.as_micros() as u64));
                    wm.fetch_max(bound, Ordering::AcqRel);
                }
                Err(e) => tracing::warn!("listing node leases failed: {e:#}"),
            }
        }
        followers.stop_all();
    });
}
