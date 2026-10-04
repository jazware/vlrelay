//! One task per host: connect, read frames into the host's fair-queue slot,
//! and reconnect from the durable cursor when anything goes wrong.

use super::fair::HostQueue;
use super::frame::{Peek, peek};
use super::host::{HostEntry, HostStatus};
use super::limits::HostLimiter;
use super::{CursorSource, UpstreamConfig};
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
}

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
        let mut skip_cursor = false;
        loop {
            // a dropped manager closes the channel without sending true
            if *self.stop.borrow() || self.stop.has_changed().is_err() {
                break;
            }
            let cursor = if skip_cursor { None } else { self.cursor.durable_cursor(&self.entry.host) };
            skip_cursor = false;
            // a fresh socket replays from the durable cursor: anything still
            // queued from the old one would arrive twice
            self.queue.clear();
            self.entry.set_status(HostStatus::Connecting);
            let started = Instant::now();
            let end = match tokio::time::timeout(self.cfg.connect_timeout, connect(&self.cfg, &self.entry.host, cursor))
                .await
            {
                Ok(Ok(ws)) => {
                    self.entry.note_connected();
                    self.entry.set_status(HostStatus::Active);
                    if let Some(c) = cursor {
                        self.entry.set_received_seq(c);
                    }
                    let (end, got_frames) = self.read(ws, &mut skip_cursor).await;
                    if got_frames || started.elapsed() > self.cfg.backoff_max {
                        attempt = 0;
                    }
                    end
                }
                Ok(Err(e)) => {
                    tracing::debug!(host = %self.entry.host.0, "connect failed: {e:#}");
                    self.entry.count_error(|c| c.connect += 1);
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

    async fn read(&mut self, mut ws: Socket, skip_cursor: &mut bool) -> (End, bool) {
        let cfg = self.cfg.clone();
        let mut limiter = HostLimiter::new(self.entry.tier(), &cfg.limits, std::time::Instant::now());
        let mut last_rx = Instant::now();
        let mut ping = tokio::time::interval_at(Instant::now() + cfg.ping_interval, cfg.ping_interval);
        let mut got_frames = false;
        let mut last_seq = self.entry.received_seq();
        let end = loop {
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
            let seq = match peek(&data) {
                Ok(Peek::Message { seq, .. }) => seq,
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
                            // the host's sequence restarted below ours (a reset
                            // or restored PDS): resume live and let the DID
                            // owners' rev checks sort out what it re-sends
                            self.entry.count_error(|c| c.future_cursor += 1);
                            self.entry.reset_cursor();
                            self.cursor.on_future_cursor(&self.entry.host);
                            *skip_cursor = true;
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
            let frame = UpstreamFrame { host: self.entry.host.clone(), upstream_seq, frame: data };

            let pause = limiter.take(self.entry.tier(), &cfg.limits, len, std::time::Instant::now());
            let full = self.queue.len() >= self.queue_capacity_hint();
            if pause > Duration::ZERO || full {
                self.entry.set_status(HostStatus::Throttled);
            }
            tokio::select! {
                biased;
                _ = self.stop.changed() => break End::Stopped,
                _ = self.kick.notified() => break End::Kicked,
                _ = async {
                    self.queue.push(frame).await;
                    if pause > Duration::ZERO {
                        tokio::time::sleep(pause).await;
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
        addrs.retain(|a| vlpds::did_resolver::is_public_ip(a.ip()));
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
    let (ws, _) = tokio_tungstenite::client_async_tls_with_config(req, stream, Some(ws_cfg), Some(connector)).await?;
    Ok(ws)
}

fn tls_config() -> Arc<rustls::ClientConfig> {
    static C: std::sync::LazyLock<Arc<rustls::ClientConfig>> = std::sync::LazyLock::new(|| {
        let roots = rustls::RootCertStore { roots: webpki_roots::TLS_SERVER_ROOTS.to_vec() };
        let c = rustls::ClientConfig::builder_with_provider(vlpds::peer_tls::provider())
            .with_safe_default_protocol_versions()
            .expect("rustls protocol versions")
            .with_root_certificates(roots)
            .with_no_client_auth();
        Arc::new(c)
    });
    C.clone()
}

#[cfg(test)]
mod tests {
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
}
