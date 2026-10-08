//! One task per host: connect, read frames into the host's fair-queue slot,
//! and reconnect from the durable cursor when anything goes wrong.

use super::fair::HostQueue;
use super::flow::Flow;
use super::frame::{Peek, peek};
use super::host::{Backpressure, HostEntry, HostStatus};
use super::limits::{HostLimiter, TokenBucket};
use super::{ConnectFn, CursorSource, UpstreamConfig};
use crate::types::{Host, UpstreamFrame};
use futures::{SinkExt, StreamExt};
use std::sync::Arc;
use std::sync::atomic::Ordering;
use std::time::Duration;
use tokio::net::TcpStream;
use tokio::sync::{Notify, watch};
use tokio::time::Instant;
use tokio_tungstenite::tungstenite::Message;
use tokio_tungstenite::tungstenite::client::IntoClientRequest;
use tokio_tungstenite::tungstenite::protocol::WebSocketConfig;
use tokio_tungstenite::{Connector, MaybeTlsStream, WebSocketStream};

pub type Socket = WebSocketStream<MaybeTlsStream<TcpStream>>;

pub(crate) struct HostTask {
    pub cfg: Arc<UpstreamConfig>,
    pub entry: Arc<HostEntry>,
    pub queue: Arc<HostQueue>,
    pub cursor: Arc<dyn CursorSource>,
    pub stop: watch::Receiver<bool>,
    /// Drops the current socket and reconnects from the durable cursor.
    pub kick: Arc<Notify>,
    /// Cuts a backoff short (a requestCrawl for a known host).
    pub wake: Arc<Notify>,
    pub on_refused: Option<super::OnRefused>,
    pub on_connect: Option<ConnectFn>,
    pub flow: Arc<Flow>,
}

/// A host we won't subscribe to however often we retry (it's a relay).
#[derive(Debug, thiserror::Error)]
#[error("{0}")]
pub struct Refused(pub String);

/// Why a connection ended, and so how long to wait before the next.
enum End {
    Stopped,
    Kicked,
    /// Retry right away: the remote asked us to (ConsumerTooSlow) or reset.
    Soon,
    Failed,
}

impl HostTask {
    pub async fn run(mut self) {
        let mut attempt: u32 = 0;
        let mut restarted = false;
        let reconnects = |e: &HostEntry, cfg: &UpstreamConfig| e.limits(&cfg.limits).reconnects_per_hour;
        let mut dials = TokenBucket::windowed(reconnects(&self.entry, &self.cfg), 3_600.0, std::time::Instant::now());
        let mut dials_gen = self.entry.limits_gen();
        loop {
            // a dropped manager closes the channel without sending true
            if *self.stop.borrow() || self.stop.has_changed().is_err() {
                break;
            }
            let g = self.entry.limits_gen();
            if g != dials_gen {
                dials_gen = g;
                let per_hour = reconnects(&self.entry, &self.cfg);
                let (rate, burst) = if per_hour > 0.0 { (per_hour / 3_600.0, per_hour) } else { (f64::INFINITY, 1.0) };
                dials.retune(rate, burst, std::time::Instant::now());
            }
            let owed = dials.take(1.0, std::time::Instant::now());
            if owed > Duration::ZERO {
                self.entry.set_status(HostStatus::Backoff);
                tokio::select! {
                    _ = tokio::time::sleep(owed) => {}
                    _ = self.stop.changed() => {}
                }
                continue;
            }
            // after a sequence restart the durable cursor is 0: the new
            // sequence from its first event
            let cursor = self.cursor.durable_cursor(&self.entry.host);
            let was_restarted = std::mem::take(&mut restarted);
            // a fresh socket replays from the durable cursor: anything still
            // queued from the old one would arrive twice
            self.queue.clear();
            self.entry.set_status(HostStatus::Connecting);
            let started = Instant::now();
            let end = match tokio::time::timeout(self.cfg.connect_timeout, connect(&self.cfg, &self.entry.host, cursor))
                .await
            {
                Ok(Ok(ws)) => {
                    let epoch = self.entry.note_connected();
                    if let Some(f) = &self.on_connect {
                        f(&self.entry.host, epoch, cursor, was_restarted);
                    }
                    self.entry.set_status(HostStatus::Active);
                    if let Some(c) = cursor {
                        self.entry.set_received_seq(c);
                    }
                    let (end, got_frames) = self.read(ws, epoch, cursor, &mut restarted).await;
                    if got_frames || started.elapsed() > self.cfg.backoff_max {
                        attempt = 0;
                    }
                    end
                }
                Ok(Err(e)) => {
                    self.entry.count_error(|c| c.connect += 1);
                    if let Some(Refused(why)) = e.downcast_ref::<Refused>() {
                        // permanent: ban it (as indigo does) rather than retry forever
                        tracing::warn!(host = %self.entry.host.0, "upstream refused for good: {why}");
                        self.entry.set_tier(super::Tier::Banned);
                        if let Some(f) = &self.on_refused {
                            f(&self.entry.host, why);
                        }
                        break;
                    }
                    tracing::debug!(host = %self.entry.host.0, "connect failed: {e:#}");
                    End::Failed
                }
                Err(_) => {
                    tracing::debug!(host = %self.entry.host.0, "connect timed out");
                    self.entry.count_error(|c| c.connect += 1);
                    End::Failed
                }
            };
            let wait = match end {
                End::Stopped => break,
                End::Kicked => Duration::ZERO,
                End::Soon => self.cfg.backoff_base,
                End::Failed => {
                    attempt = attempt.saturating_add(1);
                    backoff(self.cfg.backoff_base, self.cfg.backoff_max, attempt)
                }
            };
            if wait > Duration::ZERO {
                self.entry.set_status(HostStatus::Backoff);
                tokio::select! {
                    _ = tokio::time::sleep(wait) => {}
                    _ = self.kick.notified() => {}
                    _ = self.wake.notified() => {}
                    _ = self.stop.changed() => {}
                }
            }
        }
        self.queue.clear();
        self.entry.set_status(HostStatus::Idle);
    }

    async fn read(&mut self, mut ws: Socket, epoch: u64, cursor: Option<i64>, restarted: &mut bool) -> (End, bool) {
        let cfg = self.cfg.clone();
        let mut limiter =
            HostLimiter::new(&self.entry.limits(&cfg.limits), self.entry.limits_gen(), std::time::Instant::now());
        let mut last_rx = Instant::now();
        let mut ping = tokio::time::interval_at(Instant::now() + cfg.ping_interval, cfg.ping_interval);
        let mut got_frames = false;
        let mut last_seq = self.entry.received_seq();
        let horizon_ms = cfg.event_horizon.as_millis() as i64;
        // the limiter's buckets run on the host's clock (`super::clock`),
        // as an Instant this far along from where this socket's first frame
        // put it
        let mut origin: Option<(std::time::Instant, i64)> = None;
        let end = loop {
            if let Some(why) = self.flow.backpressure(&self.entry.flow) {
                self.entry.set_backpressure(why);
                tokio::select! {
                    biased;
                    _ = self.stop.changed() => break End::Stopped,
                    _ = self.kick.notified() => break End::Kicked,
                    _ = self.flow.wait_room(&self.entry.flow) => {}
                }
                self.entry.set_status(HostStatus::Active);
                last_rx = Instant::now();
            }
            let msg = tokio::select! {
                biased;
                _ = self.stop.changed() => break End::Stopped,
                _ = self.kick.notified() => break End::Kicked,
                _ = ping.tick() => {
                    if last_rx.elapsed() < cfg.ping_interval {
                        continue;
                    }
                    if ws.send(Message::Ping(Default::default())).await.is_err() {
                        self.entry.count_error(|c| c.dropped += 1);
                        break End::Failed;
                    }
                    continue;
                }
                _ = tokio::time::sleep_until(last_rx + cfg.stall_timeout) => {
                    tracing::info!(host = %self.entry.host.0, "upstream stalled");
                    self.entry.count_error(|c| c.stalls += 1);
                    break End::Failed;
                }
                m = ws.next() => m,
            };
            last_rx = Instant::now();
            let data = match msg {
                Some(Ok(Message::Binary(b))) => b,
                Some(Ok(Message::Ping(_) | Message::Pong(_) | Message::Frame(_))) => continue,
                Some(Ok(Message::Text(_))) => {
                    self.entry.count_error(|c| c.protocol += 1);
                    continue;
                }
                Some(Ok(Message::Close(_))) | None => {
                    self.entry.count_error(|c| c.dropped += 1);
                    break End::Failed;
                }
                Some(Err(e)) => {
                    tracing::debug!(host = %self.entry.host.0, "read failed: {e}");
                    self.entry.count_error(|c| c.dropped += 1);
                    break End::Failed;
                }
            };
            let (seq, clock_ms) = match peek(&data) {
                Ok(Peek::Message { seq, time, .. }) => {
                    (seq, self.entry.note_frame(time.and_then(event_time_ms), horizon_ms))
                }
                Ok(Peek::Info { name, message }) => {
                    if name == "OutdatedCursor" {
                        self.entry.count_error(|c| c.outdated_cursor += 1);
                    }
                    tracing::info!(host = %self.entry.host.0, name, message, "upstream info");
                    continue;
                }
                Ok(Peek::Error { error, message }) => {
                    tracing::info!(host = %self.entry.host.0, error, message, "upstream error frame");
                    match error {
                        "FutureCursor" => {
                            // The host's sequence restarted below our cursor (a
                            // wiped or restored PDS). Resuming live would skip
                            // what it emitted since the restart and
                            // desynchronize those accounts, so it replays its
                            // new sequence from the start: commits it re-sends
                            // are caught by rev, and the rest by the restart
                            // dedupe. Resyncing the touched accounts instead
                            // would need the very events we missed to know
                            // which they are.
                            self.entry.count_error(|c| c.future_cursor += 1);
                            if cursor.is_some_and(|c| c <= 0) {
                                // can't be ahead of anything: a broken host
                                self.entry.count_error(|c| c.protocol += 1);
                                break End::Failed;
                            }
                            self.entry.reset_cursor();
                            self.cursor.on_future_cursor(&self.entry.host);
                            *restarted = true;
                            break End::Soon;
                        }
                        "ConsumerTooSlow" => {
                            self.entry.count_error(|c| c.consumer_too_slow += 1);
                            break End::Soon;
                        }
                        _ => {
                            self.entry.count_error(|c| c.protocol += 1);
                            break End::Failed;
                        }
                    }
                }
                Err(e) => {
                    tracing::debug!(host = %self.entry.host.0, "unparseable frame: {e}");
                    self.entry.count_error(|c| c.protocol += 1);
                    continue;
                }
            };
            let upstream_seq = match seq {
                Some(s) => {
                    if last_seq.is_some_and(|l| s <= l) {
                        self.entry.count_error(|c| c.seq_regressions += 1);
                        continue;
                    }
                    last_seq = Some(s);
                    self.entry.set_received_seq(s);
                    s
                }
                None => 0,
            };
            got_frames = true;
            let len = data.len();
            self.entry.frames.fetch_add(1, Ordering::Relaxed);
            self.entry.bytes.fetch_add(len as u64, Ordering::Relaxed);
            let permit = Some(self.flow.acquire(&self.entry.flow, len));
            let frame =
                UpstreamFrame { host: self.entry.host.clone(), upstream_seq, frame: data, epoch, permit, clock_ms };

            let (o_at, o_clock) = *origin.get_or_insert_with(|| (std::time::Instant::now(), clock_ms));
            let now = o_at + Duration::from_millis((clock_ms - o_clock).max(0) as u64);
            let g = self.entry.limits_gen();
            if g != limiter.generation() {
                limiter.retune(&self.entry.limits(&cfg.limits), g, now);
            }
            let pause = limiter.take(len, now);
            let full = self.queue.len() >= self.queue_capacity_hint();
            if full {
                self.entry.set_backpressure(Backpressure::QueueFull);
            }
            tokio::select! {
                biased;
                _ = self.stop.changed() => break End::Stopped,
                _ = self.kick.notified() => break End::Kicked,
                _ = async {
                    self.queue.push(frame).await;
                    if pause > Duration::ZERO {
                        self.entry.set_status(HostStatus::Throttled);
                        tokio::time::sleep(pause).await;
                        self.entry.clock_paused(pause);
                    }
                } => {}
            }
            if pause > Duration::ZERO || full {
                self.entry.set_status(HostStatus::Active);
                // time spent not reading isn't the host's silence
                last_rx = Instant::now();
            }
        };
        if !matches!(end, End::Failed) {
            let _ = tokio::time::timeout(Duration::from_secs(1), ws.close(None)).await;
        }
        (end, got_frames)
    }

    fn queue_capacity_hint(&self) -> usize {
        self.cfg.host_queue_frames
    }
}

/// An event's `time` as unix ms. Anything unparseable is ignored: the lag
/// is a gauge for operators, not a check.
fn event_time_ms(t: &str) -> Option<i64> {
    chrono::DateTime::parse_from_rfc3339(t).ok().map(|d| d.timestamp_millis())
}

/// Exponential with equal jitter: half the step fixed, half random, so a
/// thousand hosts that dropped together don't come back together.
pub(crate) fn backoff(base: Duration, max: Duration, attempt: u32) -> Duration {
    let step = base.saturating_mul(1u32 << attempt.saturating_sub(1).min(20)).min(max);
    let half = step / 2;
    half + half.mul_f64(rand::random::<f64>())
}

fn ws_url(cfg: &UpstreamConfig, host: &Host, cursor: Option<i64>) -> String {
    let base = (cfg.endpoint)(host);
    let base = if let Some(r) = base.strip_prefix("https://") {
        format!("wss://{r}")
    } else if let Some(r) = base.strip_prefix("http://") {
        format!("ws://{r}")
    } else {
        base
    };
    let base = base.trim_end_matches('/');
    match cursor {
        Some(c) => format!("{base}/xrpc/com.atproto.sync.subscribeRepos?cursor={c}"),
        None => format!("{base}/xrpc/com.atproto.sync.subscribeRepos"),
    }
}

/// Resolves and connects ourselves, so that outside dev mode a hostname
/// can't point the relay at a private address (the same rule vlpds's
/// guarded HTTP client applies).
pub(crate) async fn connect(cfg: &UpstreamConfig, host: &Host, cursor: Option<i64>) -> anyhow::Result<Socket> {
    let url = ws_url(cfg, host, cursor);
    let mut req = url.as_str().into_client_request()?;
    req.headers_mut().insert("user-agent", concat!("vlrelay/", env!("CARGO_PKG_VERSION")).parse()?);
    let uri = req.uri().clone();
    let tls = match uri.scheme_str() {
        Some("wss") => true,
        Some("ws") if cfg.dev_mode => false,
        _ => anyhow::bail!("refusing {url}: wss only outside dev mode"),
    };
    let name = uri.host().ok_or_else(|| anyhow::anyhow!("no host in {url}"))?;
    let bare = name.trim_start_matches('[').trim_end_matches(']');
    let port = uri.port_u16().unwrap_or(if tls { 443 } else { 80 });
    let mut addrs: Vec<std::net::SocketAddr> = tokio::net::lookup_host((bare, port)).await?.collect();
    if !cfg.dev_mode {
        addrs.retain(|a| vlsync_atproto::did_resolver::is_public_ip(a.ip()));
    }
    anyhow::ensure!(!addrs.is_empty(), "{bare} has no usable address");
    let mut last_err = None;
    let mut stream = None;
    for a in addrs {
        match TcpStream::connect(a).await {
            Ok(s) => {
                stream = Some(s);
                break;
            }
            Err(e) => last_err = Some(e),
        }
    }
    let stream = match stream {
        Some(s) => s,
        None => return Err(last_err.map(anyhow::Error::from).unwrap_or_else(|| anyhow::anyhow!("connect failed"))),
    };
    stream.set_nodelay(true)?;
    let connector = if tls { Connector::Rustls(tls_config()) } else { Connector::Plain };
    let ws_cfg = WebSocketConfig::default()
        .read_buffer_size(cfg.read_buffer_bytes)
        .write_buffer_size(0)
        .max_message_size(Some(cfg.max_frame_bytes))
        .max_frame_size(Some(cfg.max_frame_bytes));
    let (ws, resp) =
        tokio_tungstenite::client_async_tls_with_config(req, stream, Some(ws_cfg), Some(connector)).await?;
    if let Some(server) = relay_server(resp.headers()) {
        return Err(Refused(format!("it's a relay (Server: {server}), not a PDS")).into());
    }
    Ok(ws)
}

/// Relays mark themselves with `atproto-relay` in `Server`, and indigo bans
/// a host that sends it. A relay's stream carries every other host's
/// accounts, which the host authority check would refuse one event at a time.
fn relay_server(h: &tokio_tungstenite::tungstenite::http::HeaderMap) -> Option<&str> {
    h.get_all("server").iter().filter_map(|v| v.to_str().ok()).find(|s| s.contains("atproto-relay"))
}

fn tls_config() -> Arc<rustls::ClientConfig> {
    static C: std::sync::LazyLock<Arc<rustls::ClientConfig>> = std::sync::LazyLock::new(|| {
        let roots = rustls::RootCertStore { roots: webpki_roots::TLS_SERVER_ROOTS.to_vec() };
        let c = rustls::ClientConfig::builder_with_provider(vlsync_atproto::http::tls_provider())
            .with_safe_default_protocol_versions()
            .expect("rustls protocol versions")
            .with_root_certificates(roots)
            .with_no_client_auth();
        Arc::new(c)
    });
    C.clone()
}

#[cfg(test)]
// tungstenite fixes the handshake callback's error type (a full HTTP response).
#[allow(clippy::result_large_err)]
pub(crate) mod tests {
    use super::*;

    #[test]
    fn backoff_grows_and_caps() {
        let base = Duration::from_millis(100);
        let max = Duration::from_secs(10);
        for _ in 0..100 {
            let d1 = backoff(base, max, 1);
            assert!(d1 >= Duration::from_millis(50) && d1 <= base);
            let d4 = backoff(base, max, 4);
            assert!(d4 >= Duration::from_millis(400) && d4 <= Duration::from_millis(800));
            let d30 = backoff(base, max, 30);
            assert!(d30 >= max / 2 && d30 <= max);
        }
    }

    /// A websocket server on loopback answering every upgrade with `server`.
    pub(crate) async fn ws_server(server: Option<&'static str>) -> Host {
        let l = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = l.local_addr().unwrap();
        tokio::spawn(async move {
            while let Ok((s, _)) = l.accept().await {
                tokio::spawn(async move {
                    let cb =
                        |_: &tokio_tungstenite::tungstenite::handshake::server::Request,
                         mut r: tokio_tungstenite::tungstenite::handshake::server::Response| {
                            if let Some(v) = server {
                                r.headers_mut().insert("server", v.parse().unwrap());
                            }
                            Ok(r)
                        };
                    let _ws = tokio_tungstenite::accept_hdr_async(s, cb).await;
                    tokio::time::sleep(Duration::from_secs(5)).await;
                });
            }
        });
        Host(format!("127.0.0.1:{}", addr.port()))
    }

    #[tokio::test]
    async fn refuses_an_upstream_that_says_its_a_relay() {
        let mut cfg = UpstreamConfig::new(true);
        cfg.endpoint = Arc::new(|h: &Host| format!("http://{}", h.0));

        let relay = ws_server(Some("indigo-relay/v0.0.0 (atproto-relay)")).await;
        let err = connect(&cfg, &relay, None).await.expect_err("a relay upstream is refused");
        assert!(err.to_string().contains("it's a relay"), "{err}");

        let pds = ws_server(Some("vlpds/1.0")).await;
        connect(&cfg, &pds, None).await.expect("a PDS connects");
        let bare = ws_server(None).await;
        connect(&cfg, &bare, Some(5)).await.expect("no Server header connects");
    }

    /// A newly admitted host is subscribed at its live head, as indigo does:
    /// no cursor, so the PDS sends nothing it emitted before.
    #[tokio::test]
    async fn a_new_host_is_subscribed_without_a_cursor() {
        let l = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let host = Host(format!("127.0.0.1:{}", l.local_addr().unwrap().port()));
        let (tx, mut rx) = tokio::sync::mpsc::unbounded_channel();
        tokio::spawn(async move {
            let (s, _) = l.accept().await.unwrap();
            let cb = |r: &tokio_tungstenite::tungstenite::handshake::server::Request,
                      resp: tokio_tungstenite::tungstenite::handshake::server::Response| {
                let _ = tx.send(r.uri().to_string());
                Ok(resp)
            };
            let _ws = tokio_tungstenite::accept_hdr_async(s, cb).await;
            tokio::time::sleep(Duration::from_secs(5)).await;
        });
        let mut cfg = UpstreamConfig::new(true);
        cfg.endpoint = Arc::new(|h: &Host| format!("http://{}", h.0));
        let (m, _rx) = super::super::Manager::new(cfg, Arc::new(super::super::MemHostStore::default()), None);
        m.start().await.unwrap();
        m.admit(&host, super::super::Tier::Default).await.unwrap();
        let uri = tokio::time::timeout(Duration::from_secs(5), rx.recv()).await.unwrap().unwrap();
        assert_eq!(uri, "/xrpc/com.atproto.sync.subscribeRepos");
        m.shutdown().await.unwrap();
    }

    /// A host whose sequence restarted answers our cursor with FutureCursor.
    /// Resuming live would lose everything it emitted since the restart, so
    /// it must replay its new sequence from 0.
    #[tokio::test]
    async fn future_cursor_replays_the_new_sequence_from_zero() {
        let l = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let host = Host(format!("127.0.0.1:{}", l.local_addr().unwrap().port()));
        let (seen_tx, mut seen) = tokio::sync::mpsc::unbounded_channel::<String>();
        tokio::spawn(async move {
            while let Ok((s, _)) = l.accept().await {
                let seen_tx = seen_tx.clone();
                tokio::spawn(async move {
                    let mut query = String::new();
                    let cb =
                        |req: &tokio_tungstenite::tungstenite::handshake::server::Request,
                         r: tokio_tungstenite::tungstenite::handshake::server::Response| {
                            query = req.uri().query().unwrap_or("").to_string();
                            Ok(r)
                        };
                    let Ok(mut ws) = tokio_tungstenite::accept_hdr_async(s, cb).await else { return };
                    let _ = seen_tx.send(query.clone());
                    if query.contains("cursor=500") {
                        let f = vlsync_atproto::events::error_frame("FutureCursor", "cursor in the future");
                        let _ = ws.send(Message::Binary(f.into())).await;
                        let _ = ws.close(None).await;
                    } else {
                        tokio::time::sleep(Duration::from_secs(5)).await;
                    }
                });
            }
        });
        let store = Arc::new(super::super::MemHostStore::default());
        let mut rec = super::super::HostRecord::new(&host, super::super::Tier::Trusted);
        rec.acked_seq = Some(500);
        super::super::HostStore::put(&*store, vec![rec]).await.unwrap();
        let mut cfg = UpstreamConfig::new(true);
        cfg.endpoint = Arc::new(|h: &Host| format!("http://{}", h.0));
        cfg.backoff_base = Duration::from_millis(10);
        let (m, _rx) = super::super::Manager::new(cfg, store, None);
        let connects = Arc::new(parking_lot::Mutex::new(Vec::new()));
        let c = connects.clone();
        m.on_connect(Arc::new(move |_: &Host, epoch, cursor, restarted| c.lock().push((epoch, cursor, restarted))));
        m.start().await.unwrap();
        let first = tokio::time::timeout(Duration::from_secs(5), seen.recv()).await.unwrap().unwrap();
        let second = tokio::time::timeout(Duration::from_secs(5), seen.recv()).await.unwrap().unwrap();
        assert_eq!((first.as_str(), second.as_str()), ("cursor=500", "cursor=0"));
        for _ in 0..100 {
            if connects.lock().len() >= 2 {
                break;
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
        assert_eq!(*connects.lock(), vec![(1, Some(500), false), (2, Some(0), true)]);
        assert_eq!(m.host(&host).unwrap().record.acked_seq, Some(0));
        m.shutdown().await.unwrap();
    }
}
