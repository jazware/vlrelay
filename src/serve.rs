//! `com.atproto.sync.subscribeRepos` for consumers.
//!
//! This is vlpds's firehose (`vlpds::firehose::Firehose`) as is, fed the
//! quorum log's committed entries (`qlog::emit`) as one counted stream:
//!
//! - The merger cuts every 2 ms at the commit index and frames each batch
//!   once. Every subscriber writes slices of those same bytes, on a separate
//!   runtime.
//! - A cursor inside the in-memory ring is served from memory. An older one
//!   is backfilled from the log segments in the bucket, then the node's own
//!   log, and handed to the ring at its floor.
//! - A subscriber that falls out of the ring catches up the same way while
//!   it's within `max_lag_bytes` of the head, and gets `ConsumerTooSlow`
//!   past that.
//! - A cursor below the retained floor gets `#info OutdatedCursor` and
//!   continues from the oldest event left.
//!
//! What this module adds is the relay's wiring: the takedown filter, the
//! consumers' rates, the axum route, and the client address behind a
//! proxy ([`real_ip`]) that the per-IP limits key on.

use crate::policy::takedowns::TakedownSet;
use axum::extract::{ConnectInfo, Query, Request, State};
use axum::middleware::Next;
use axum::response::{IntoResponse, Response};
use serde::Deserialize;
use std::collections::HashMap;
use std::net::{IpAddr, SocketAddr};
use std::sync::atomic::Ordering;
use std::sync::{Arc, Weak};
use std::time::{Duration, Instant};
use vlpds::firehose::{self, Firehose, SubscriberView};
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
    /// How often the takedown set re-reads `policy/takedowns/current/`
    /// (zero: never; [`Serve::load_takedowns`] still reads it once).
    pub takedown_poll: Duration,
}

impl ServeConfig {
    pub fn firehose_options(&self, runtime: Option<tokio::runtime::Handle>) -> firehose::Options {
        firehose::Options {
            ring_bytes: self.ring_bytes,
            max_lag_bytes: self.max_lag_bytes,
            readahead_bytes: self.readahead_bytes,
            backfill_cache_bytes: self.backfill_cache_bytes,
            max_backfills: self.max_backfills,
            max_per_ip: self.max_per_ip,
            write_idle: firehose::DEFAULT_WRITE_IDLE,
            runtime,
            max_labelled: firehose::DEFAULT_MAX_LABELLED,
            start_floor: None,
        }
    }
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
            takedown_poll: crate::policy::REFRESH_EVERY,
        }
    }
}

pub struct Serve {
    /// Made at the node's first emission (`qlog::emit`): its floor is where
    /// that emission starts, which a node reset to the leader's log only
    /// learns then. Until it exists, subscribeRepos answers 503.
    firehose: std::sync::OnceLock<Arc<Firehose>>,
    /// The firehose's filter: taken-down accounts' commits and syncs.
    pub takedowns: Arc<TakedownSet>,
    pub store: Store,
    cfg: ServeConfig,
    /// Per-consumer rates by firehose connection id, sampled every second.
    rates: parking_lot::Mutex<HashMap<u64, Rate>>,
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
    /// Where it reads next from: its cursor while it has been sent
    /// nothing, else `last_seq`.
    pub pos: i64,
    /// How far its position trails the stream head, in seq time.
    pub lag_ms: f64,
    pub events_per_sec: f64,
    pub bytes_per_sec: f64,
    pub backfilling: bool,
}

struct Rate {
    /// (events, bytes, when) at the last sample
    sampled: (u64, u64, Instant),
    events_per_sec: f64,
    bytes_per_sec: f64,
}

const CONSUMER_SAMPLE: Duration = Duration::from_secs(1);

impl Serve {
    /// For the quorum log's firehose (`qlog::emit`), which counts its own
    /// seqs: the relay's takedown filter and consumer views go on it once
    /// it's made ([`Serve::attach`]).
    pub fn counted(store: Store, cfg: ServeConfig) -> Arc<Serve> {
        let takedowns = Arc::new(TakedownSet::default());
        let s = Arc::new(Serve {
            firehose: Default::default(),
            takedowns,
            store,
            cfg,
            rates: parking_lot::Mutex::new(HashMap::new()),
        });
        tokio::spawn(sample_consumers(Arc::downgrade(&s)));
        if !s.cfg.takedown_poll.is_zero() {
            tokio::spawn(poll_takedowns(Arc::downgrade(&s), s.cfg.takedown_poll));
        }
        s
    }

    /// Serves `fh` from now on (the first call wins).
    pub fn attach(&self, fh: &Arc<Firehose>) {
        fh.set_filter(self.takedowns.clone());
        let _ = self.firehose.set(fh.clone());
    }

    pub fn firehose(&self) -> Option<&Arc<Firehose>> {
        self.firehose.get()
    }

    /// The newest seq emitted (0 before the first emission).
    pub fn head(&self) -> i64 {
        self.firehose().map_or(0, |f| f.last_emitted.load(Ordering::Acquire))
    }

    /// Reads the takedown list once. Call it before serving: until the first
    /// read, taken-down accounts' old frames would replay.
    pub async fn load_takedowns(&self) {
        if let Err(e) = self.takedowns.poll(&self.store).await {
            tracing::warn!("loading the takedown list failed (retrying every poll): {e:#}");
        }
    }

    /// The connected consumers, by id.
    pub fn consumers(&self) -> Vec<ConsumerSnapshot> {
        let Some(fh) = self.firehose() else { return Vec::new() };
        let head = fh.last_emitted.load(Ordering::Acquire);
        let rates = self.rates.lock();
        fh.subscribers()
            .0
            .into_iter()
            .filter_map(|v| {
                let id: u64 = v.conn.parse().ok()?;
                let last_seq: i64 = v.last_seq.parse().unwrap_or(0);
                let backfilling = v.state == "backfilling";
                let start_cursor = v.cursor.as_deref().and_then(|c| c.parse().ok());
                let pos = match (last_seq, backfilling) {
                    (0, true) => start_cursor.unwrap_or(head),
                    // live and sent nothing yet: nothing has been emitted since it joined at the head
                    (0, false) => head,
                    (s, _) => s,
                };
                // stream seqs are counts; their log keys are time (key >> 8 is unix µs)
                // a counted stream's keys are its seqs, not time: lag is in events
                let lag_ms = (head - pos).max(0) as f64;
                let rate = rates.get(&id);
                Some(ConsumerSnapshot {
                    id,
                    ip: v
                        .ip
                        .as_deref()
                        .and_then(|ip| ip.parse().ok())
                        .unwrap_or(IpAddr::V4(std::net::Ipv4Addr::UNSPECIFIED)),
                    user_agent: v.user_agent,
                    connected_since_ms: v.connected_at as i64,
                    start_cursor,
                    last_seq,
                    pos,
                    lag_ms,
                    events_per_sec: rate.map_or(0.0, |r| r.events_per_sec),
                    bytes_per_sec: rate.map_or(0.0, |r| r.bytes_per_sec),
                    backfilling,
                })
            })
            .collect()
    }

    /// Disconnects consumer `id`; false if it's unknown or already gone.
    pub fn kick(&self, id: u64) -> bool {
        self.firehose().is_some_and(|f| f.kick(id))
    }

    pub fn router(self: &Arc<Self>) -> axum::Router {
        axum::Router::new()
            .route("/xrpc/com.atproto.sync.subscribeRepos", axum::routing::get(subscribe_repos))
            .with_state(self.clone())
    }
}

#[derive(Deserialize)]
struct SubscribeParams {
    cursor: Option<i64>,
}

/// The client's address as [`real_ip`] found it, set on requests from a
/// trusted proxy.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct ClientIp(pub IpAddr);

/// The address per-IP limits key on: [`ClientIp`] when a trusted proxy
/// named one, else the socket's peer.
pub fn client_ip(ext: &axum::http::Extensions) -> Option<IpAddr> {
    ext.get::<ClientIp>().map(|c| c.0).or_else(|| ext.get::<ConnectInfo<SocketAddr>>().map(|c| c.0.ip().to_canonical()))
}

/// An address block, `10.0.0.0/8` or `fd00::/8` (a bare address is one host).
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct Cidr {
    net: IpAddr,
    bits: u8,
}

impl std::str::FromStr for Cidr {
    type Err = String;
    fn from_str(s: &str) -> Result<Cidr, String> {
        let (a, b) = s.trim().split_once('/').map_or((s.trim(), None), |(a, b)| (a, Some(b)));
        let net: IpAddr = a.parse().map_err(|_| format!("{s:?}: not an address"))?;
        let max = if net.is_ipv4() { 32 } else { 128 };
        let bits = match b {
            Some(b) => b.parse::<u8>().ok().filter(|n| *n <= max).ok_or_else(|| format!("{s:?}: bad prefix length"))?,
            None => max,
        };
        Ok(Cidr { net, bits })
    }
}

impl Cidr {
    pub fn contains(&self, ip: IpAddr) -> bool {
        let mask = |bits: u8, width: u32| if bits == 0 { 0 } else { u128::MAX << (width - bits as u32) };
        match (self.net, ip.to_canonical()) {
            (IpAddr::V4(n), IpAddr::V4(a)) => {
                let m = mask(self.bits, 32) as u32;
                u32::from(n) & m == u32::from(a) & m
            }
            (IpAddr::V6(n), IpAddr::V6(a)) => {
                let m = mask(self.bits, 128);
                u128::from(n) & m == u128::from(a) & m
            }
            _ => false,
        }
    }
}

/// The client behind `peer`: when the peer is a trusted proxy, the
/// rightmost `X-Forwarded-For` address that isn't one. Addresses left of
/// it are the client's to write, so they're never believed.
pub fn forwarded_client(peer: IpAddr, xff: &[&str], trusted: &[Cidr]) -> IpAddr {
    let peer = peer.to_canonical();
    if !trusted.iter().any(|c| c.contains(peer)) {
        return peer;
    }
    let mut last = peer;
    for part in xff.iter().rev().flat_map(|h| h.rsplit(',')) {
        let Ok(ip) = part.trim().parse::<IpAddr>() else { return last };
        let ip = ip.to_canonical();
        if !trusted.iter().any(|c| c.contains(ip)) {
            return ip;
        }
        last = ip;
    }
    last
}

/// Middleware for `--trusted-proxy`: sets [`ClientIp`] from
/// `X-Forwarded-For` on requests whose peer is one of `trusted`.
pub async fn real_ip(State(trusted): State<Arc<Vec<Cidr>>>, mut req: Request, next: Next) -> Response {
    if let Some(peer) = req.extensions().get::<ConnectInfo<SocketAddr>>().map(|c| c.0.ip()) {
        let xff: Vec<&str> = req.headers().get_all("x-forwarded-for").iter().filter_map(|v| v.to_str().ok()).collect();
        let ip = forwarded_client(peer, &xff, &trusted);
        req.extensions_mut().insert(ClientIp(ip));
    }
    next.run(req).await
}

async fn subscribe_repos(State(s): State<Arc<Serve>>, Query(q): Query<SubscribeParams>, req: Request) -> Response {
    let client = client_ip(req.extensions());
    match s.firehose() {
        Some(fh) => fh.upgrade(req, q.cursor, None, client, None),
        None => (axum::http::StatusCode::SERVICE_UNAVAILABLE, "nothing committed yet").into_response(),
    }
}

/// Per-consumer rates from the counters' deltas; forgets gone consumers.
async fn sample_consumers(serve: Weak<Serve>) {
    let mut tick = tokio::time::interval(CONSUMER_SAMPLE);
    tick.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
    loop {
        tick.tick().await;
        let Some(s) = serve.upgrade() else { return };
        let live: Vec<SubscriberView> = s.firehose().map(|f| f.subscribers().0).unwrap_or_default();
        let now = Instant::now();
        let mut rates = s.rates.lock();
        let mut next = HashMap::with_capacity(live.len());
        for v in live {
            let Ok(id) = v.conn.parse::<u64>() else { continue };
            let mut r = rates.remove(&id).unwrap_or(Rate {
                sampled: (v.events, v.bytes, now),
                events_per_sec: 0.0,
                bytes_per_sec: 0.0,
            });
            let secs = now.duration_since(r.sampled.2).as_secs_f64();
            if secs > 0.0 {
                r.events_per_sec = v.events.saturating_sub(r.sampled.0) as f64 / secs;
                r.bytes_per_sec = v.bytes.saturating_sub(r.sampled.1) as f64 / secs;
            }
            r.sampled = (v.events, v.bytes, now);
            next.insert(id, r);
        }
        *rates = next;
    }
}

async fn poll_takedowns(serve: Weak<Serve>, every: Duration) {
    let mut tick = tokio::time::interval_at(tokio::time::Instant::now() + every, every);
    tick.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
    loop {
        tick.tick().await;
        let Some(s) = serve.upgrade() else { return };
        if let Err(e) = s.takedowns.poll(&s.store).await {
            tracing::warn!("takedown list poll failed (retrying): {e:#}");
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use futures::StreamExt;
    use tokio_tungstenite::tungstenite::client::IntoClientRequest;

    /// Behind a proxy every consumer's socket comes from the proxy, so the
    /// per-IP cap keys on X-Forwarded-For, believed only from the proxies
    /// named in --trusted-proxy, rightmost untrusted entry first.
    #[test]
    fn forwarded_for_is_believed_only_from_trusted_proxies() {
        let trusted: Vec<Cidr> = vec!["10.0.0.0/8".parse().unwrap(), "fd00::/8".parse().unwrap()];
        let ip = |s: &str| s.parse::<IpAddr>().unwrap();
        let f = |peer: &str, xff: &[&str]| forwarded_client(ip(peer), xff, &trusted);
        // not from a proxy: the header is the client's own
        assert_eq!(f("203.0.113.9", &["198.51.100.1"]), ip("203.0.113.9"));
        // from a proxy: the rightmost address it didn't add, not a spoofed leftmost one
        assert_eq!(f("10.1.2.3", &["1.1.1.1, 198.51.100.7, 10.9.9.9"]), ip("198.51.100.7"));
        assert_eq!(f("10.1.2.3", &["1.1.1.1", "198.51.100.7"]), ip("198.51.100.7"));
        assert_eq!(f("::ffff:10.1.2.3", &["198.51.100.7"]), ip("198.51.100.7"));
        assert_eq!(f("fd00::1", &["2001:db8::5"]), ip("2001:db8::5"));
        // no header, or garbage: the nearest address we have
        assert_eq!(f("10.1.2.3", &[]), ip("10.1.2.3"));
        assert_eq!(f("10.1.2.3", &["junk, 10.4.4.4"]), ip("10.4.4.4"));
        assert!("10.0.0.0/33".parse::<Cidr>().is_err());
        assert!("0.0.0.0/0".parse::<Cidr>().unwrap().contains(ip("8.8.8.8")));
        assert!(!"192.168.1.0/24".parse::<Cidr>().unwrap().contains(ip("192.168.2.1")));
    }

    /// A consumer shows up with its address and user agent, and a kick
    /// disconnects it while it's idle (nothing is emitted) and forgets it.
    #[tokio::test]
    async fn consumers_are_listed_and_kicked() {
        let cfg = ServeConfig::default();
        let fh = Firehose::new(firehose::Options { start_floor: Some(0), ..cfg.firehose_options(None) });
        let s = Serve::counted(Store::memory(None), cfg);
        s.attach(&fh);
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let app = s.router();
        tokio::spawn(async move {
            axum::serve(listener, app.into_make_service_with_connect_info::<SocketAddr>()).await.unwrap();
        });
        let mut req = format!("ws://{addr}/xrpc/com.atproto.sync.subscribeRepos").into_client_request().unwrap();
        req.headers_mut().insert("user-agent", "x".repeat(300).parse().unwrap());
        let (mut ws, _) = tokio_tungstenite::connect_async(req).await.unwrap();
        let t = Instant::now();
        let c = loop {
            let c = s.consumers();
            if !c.is_empty() || t.elapsed() > Duration::from_secs(5) {
                break c;
            }
            tokio::time::sleep(Duration::from_millis(5)).await;
        };
        assert_eq!(c.len(), 1);
        let id = c[0].id;
        assert_eq!(c[0].ip, addr.ip());
        assert!(!c[0].user_agent.is_empty() && c[0].user_agent.len() < 300, "trimmed");
        assert!(c[0].user_agent.bytes().all(|b| b == b'x'));
        assert_eq!((c[0].start_cursor, c[0].last_seq, c[0].lag_ms, c[0].backfilling), (None, 0, 0.0, false));
        assert!(!s.kick(id + 1));
        assert!(s.kick(id));
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
        assert!(!s.kick(id));
    }
}
