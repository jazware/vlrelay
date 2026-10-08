//! What the upstream tests share (tests/upstream and interop/tests/vlpds).
#![allow(dead_code)]

use crate::fan::Fan;
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::sync::mpsc;
use vlrelay::types::{Host, UpstreamFrame};
use vlrelay::upstream::{Crawler, HostRecord, HostStore, MemHostStore, Tier, UpstreamConfig};

pub fn dev_config() -> UpstreamConfig {
    let mut c = UpstreamConfig::new(true);
    c.endpoint = Arc::new(|h: &Host| format!("http://{}", h.0));
    c.backoff_base = Duration::from_millis(50);
    c.backoff_max = Duration::from_millis(400);
    c.connect_timeout = Duration::from_secs(2);
    c.flush_interval = Duration::from_millis(100);
    c
}

/// Routes `*.fan.test` to the fan, anything else to `http://{host}`.
pub fn fan_config(fan: &Arc<Fan>) -> UpstreamConfig {
    let mut c = dev_config();
    let f = fan.clone();
    c.endpoint = Arc::new(move |h: &Host| match h.0.strip_suffix(".fan.test") {
        Some(name) => f.url(name),
        None => format!("http://{}", h.0),
    });
    c
}

/// A store holding `host` at `tier` with an acked cursor.
pub async fn seeded(host: &Host, tier: Tier, acked: Option<i64>) -> Arc<MemHostStore> {
    let store = Arc::new(MemHostStore::default());
    let mut r = HostRecord::new(host, tier);
    r.acked_seq = acked;
    store.put(vec![r]).await.unwrap();
    store
}

pub async fn wait_for(what: &str, timeout: Duration, mut f: impl FnMut() -> bool) {
    let t = Instant::now();
    while !f() {
        assert!(t.elapsed() < timeout, "timed out waiting for {what}");
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
}

/// Frames until none arrive for `quiet`.
pub async fn drain(rx: &mut mpsc::Receiver<UpstreamFrame>, quiet: Duration) -> Vec<UpstreamFrame> {
    let mut out = Vec::new();
    while let Ok(Some(f)) = tokio::time::timeout(quiet, rx.recv()).await {
        out.push(f);
    }
    out
}

/// Frames for `d`, for streams that never go quiet.
pub async fn collect_for(rx: &mut mpsc::Receiver<UpstreamFrame>, d: Duration) -> Vec<UpstreamFrame> {
    let mut out = Vec::new();
    let end = tokio::time::Instant::now() + d;
    while let Ok(Some(f)) = tokio::time::timeout_at(end, rx.recv()).await {
        out.push(f);
    }
    out
}

pub fn seqs(frames: &[UpstreamFrame]) -> Vec<i64> {
    frames.iter().map(|f| f.upstream_seq).collect()
}

pub async fn serve_crawler(c: &Arc<Crawler>) -> String {
    let l = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let url = format!("http://{}", l.local_addr().unwrap());
    let r = c.router();
    tokio::spawn(async move { axum::serve(l, r).await.unwrap() });
    url
}

pub async fn crawl(url: &str, hostname: &str) -> (u16, serde_json::Value) {
    let r = reqwest::Client::new()
        .post(format!("{url}/xrpc/com.atproto.sync.requestCrawl"))
        .json(&serde_json::json!({"hostname": hostname}))
        .send()
        .await
        .unwrap();
    (r.status().as_u16(), r.json().await.unwrap_or_default())
}
