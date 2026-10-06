//! `GET /api/public/stats`: the public dashboard's numbers, no token.
//!
//! [`PublicStats`] is an allow-list: every field is an aggregate that says
//! nothing about a host, an account, a consumer or a node. It's built by
//! copying named fields out of the operator [`Overview`], never by
//! filtering it, so a field added to `Overview` stays private until
//! someone adds it here on purpose.

use super::{AdminResult, AdminSource, History, Overview, QuorumView};
use axum::{
    Router,
    body::Bytes,
    extract::State,
    http::{HeaderValue, StatusCode, header},
    response::{IntoResponse, Response},
    routing::get,
};
use serde::{Deserialize, Serialize};
use std::sync::{Arc, OnceLock};
use std::time::{Duration, Instant};

/// Samples of history the public page gets (5 minutes at 1 s).
pub const PUBLIC_HISTORY: usize = 300;
/// One overview per second at most, however many people have the page open.
const CACHE_TTL: Duration = Duration::from_secs(1);

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct PublicStats {
    pub time_ms: i64,
    pub version: String,
    pub uptime_secs: u64,
    pub events_in_per_sec: f64,
    /// Every node's emits summed (each consumer counts).
    pub events_out_per_sec: f64,
    /// The merged stream's own rate: what one consumer of the whole firehose gets.
    pub stream_events_per_sec: f64,
    pub time_to_firehose_p50_ms: f64,
    pub time_to_firehose_p99_ms: f64,
    pub hosts_connected: u32,
    pub consumers: u32,
    /// The firehose's newest seq, which any consumer can see anyway.
    pub last_seq: i64,
    pub nodes: u32,
    pub nodes_healthy: u32,
    pub health: Health,
    /// The quorum log's health; None when the relay doesn't run one.
    pub quorum: Option<Health>,
    pub history: PublicHistory,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub enum Health {
    Ok,
    /// Serving, with a node or a member down.
    Degraded,
    Down,
}

#[derive(Clone, Debug, Default, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct PublicHistory {
    pub sample_secs: u32,
    pub t: Vec<i64>,
    pub events_in: Vec<f64>,
    pub events_out: Vec<f64>,
    pub ttf_p50_ms: Vec<f64>,
    pub ttf_p99_ms: Vec<f64>,
}

fn tail<T: Clone>(v: &[T]) -> Vec<T> {
    v[v.len().saturating_sub(PUBLIC_HISTORY)..].to_vec()
}

impl PublicHistory {
    fn of(h: &History) -> PublicHistory {
        PublicHistory {
            sample_secs: h.sample_secs,
            t: tail(&h.t),
            events_in: tail(&h.events_in),
            events_out: tail(&h.events_out),
            ttf_p50_ms: tail(&h.ttf_p50_ms),
            ttf_p99_ms: tail(&h.ttf_p99_ms),
        }
    }
}

/// When this process started serving (the first call wins).
pub fn started() -> Instant {
    static AT: OnceLock<Instant> = OnceLock::new();
    *AT.get_or_init(Instant::now)
}

/// The quorum log's health from every node's `/qlog/status`: a leader and a
/// majority of members answering is serving; all of them is ok.
pub fn quorum_health(q: &QuorumView) -> Health {
    let live: Vec<&serde_json::Value> = q.nodes.iter().filter(|n| !n.stale).filter_map(|n| n.status.as_ref()).collect();
    let leader = live.iter().find(|s| s.get("role").and_then(|r| r.as_str()) == Some("leader"));
    let Some(leader) = leader else { return Health::Down };
    let members: Vec<&str> = leader
        .get("members")
        .and_then(|m| m.as_array())
        .map(|m| m.iter().filter_map(|x| x.as_str()).collect())
        .unwrap_or_default();
    if members.is_empty() {
        return Health::Down;
    }
    let id = |s: &&serde_json::Value| s.get("id").and_then(|x| x.as_str()).map(str::to_string);
    let up: Vec<String> = live.iter().filter_map(id).collect();
    let answering = members.iter().filter(|m| up.iter().any(|u| u == *m)).count();
    if answering * 2 <= members.len() {
        Health::Down
    } else if answering < members.len() {
        Health::Degraded
    } else {
        Health::Ok
    }
}

/// The allow-list projection. `quorum` is the quorum log's view, if any.
pub fn project(o: &Overview, quorum: Option<&QuorumView>) -> PublicStats {
    let quorum = quorum.filter(|q| !q.nodes.is_empty());
    let (nodes, healthy) = match quorum {
        _ if !o.by_node.is_empty() => {
            (o.by_node.len() as u32, o.by_node.iter().filter(|n| !n.stale).count() as u32)
        }
        // no fleet numbers: the quorum log's members are the nodes
        Some(q) => {
            let current = q.nodes.iter().filter(|n| n.status.as_ref().is_none_or(|s| s["retired"] != true));
            let (all, up) = current.fold((0, 0), |(a, u), n| (a + 1, u + u32::from(!n.stale)));
            (all, up)
        }
        None => (1, 1),
    };
    let quorum = quorum.map(quorum_health);
    let health = match (healthy, quorum) {
        (0, _) | (_, Some(Health::Down)) => Health::Down,
        (h, q) if h < nodes || q == Some(Health::Degraded) => Health::Degraded,
        _ => Health::Ok,
    };
    PublicStats {
        time_ms: o.time_ms,
        version: env!("CARGO_PKG_VERSION").into(),
        uptime_secs: started().elapsed().as_secs(),
        events_in_per_sec: o.events_in_per_sec,
        events_out_per_sec: o.events_out_per_sec,
        stream_events_per_sec: o.stream_events_per_sec,
        time_to_firehose_p50_ms: o.time_to_firehose_p50_ms,
        time_to_firehose_p99_ms: o.time_to_firehose_p99_ms,
        hosts_connected: o.hosts_connected,
        consumers: o.consumers,
        last_seq: o.last_seq,
        nodes,
        nodes_healthy: healthy,
        health,
        quorum,
        history: PublicHistory::of(&o.history),
    }
}

struct Cache<S> {
    src: Arc<S>,
    last: parking_lot::Mutex<Option<(Instant, Bytes)>>,
    /// One refresh at a time: a burst of requests after expiry waits for it
    /// rather than each running an overview.
    refresh: tokio::sync::Mutex<()>,
}

impl<S: AdminSource> Cache<S> {
    fn fresh(&self) -> Option<Bytes> {
        self.last.lock().as_ref().filter(|(at, _)| at.elapsed() < CACHE_TTL).map(|(_, b)| b.clone())
    }

    async fn get(&self) -> AdminResult<Bytes> {
        if let Some(b) = self.fresh() {
            return Ok(b);
        }
        let _g = self.refresh.lock().await;
        if let Some(b) = self.fresh() {
            return Ok(b);
        }
        let stats = self.src.public_stats().await?;
        let body = Bytes::from(serde_json::to_vec(&stats).map_err(anyhow::Error::from)?);
        *self.last.lock() = Some((Instant::now(), body.clone()));
        Ok(body)
    }
}

pub fn public_routes<S: AdminSource>(src: Arc<S>) -> Router {
    started();
    let cache = Arc::new(Cache { src, last: parking_lot::Mutex::new(None), refresh: tokio::sync::Mutex::new(()) });
    Router::new().route("/api/public/stats", get(stats::<S>)).with_state(cache)
}

async fn stats<S: AdminSource>(State(c): State<Arc<Cache<S>>>) -> Response {
    match c.get().await {
        Ok(body) => (
            [
                (header::CONTENT_TYPE, HeaderValue::from_static("application/json")),
                (header::CACHE_CONTROL, HeaderValue::from_static("public, max-age=1")),
                (header::ACCESS_CONTROL_ALLOW_ORIGIN, HeaderValue::from_static("*")),
            ],
            body,
        )
            .into_response(),
        Err(e) => {
            tracing::warn!(error = %e, "public stats");
            (
                StatusCode::SERVICE_UNAVAILABLE,
                axum::Json(serde_json::json!({"error": "Unavailable", "message": "stats are unavailable right now"})),
            )
                .into_response()
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::admin::demo::Demo;
    use axum::body::Body;
    use axum::http::Request;
    use tower::ServiceExt;

    /// Every key the public endpoint may send. A new one fails here until
    /// someone decides it's safe.
    const ALLOWED: [&str; 16] = [
        "timeMs",
        "version",
        "uptimeSecs",
        "eventsInPerSec",
        "eventsOutPerSec",
        "streamEventsPerSec",
        "timeToFirehoseP50Ms",
        "timeToFirehoseP99Ms",
        "hostsConnected",
        "consumers",
        "lastSeq",
        "nodes",
        "nodesHealthy",
        "health",
        "quorum",
        "history",
    ];

    #[tokio::test]
    async fn public_stats_are_an_allow_list() {
        let d = Demo::start(7);
        let o = d.overview().await.unwrap();
        let v = serde_json::to_value(d.public_stats().await.unwrap()).unwrap();
        let mut keys: Vec<&str> = v.as_object().unwrap().keys().map(String::as_str).collect();
        keys.sort();
        let mut want = ALLOWED.to_vec();
        want.sort();
        assert_eq!(keys, want);
        let mut hist: Vec<&str> = v["history"].as_object().unwrap().keys().map(String::as_str).collect();
        hist.sort();
        assert_eq!(hist, ["eventsIn", "eventsOut", "sampleSecs", "t", "ttfP50Ms", "ttfP99Ms"]);
        assert!(v["history"]["t"].as_array().unwrap().len() <= PUBLIC_HISTORY);
        let text = v.to_string();
        for h in &o.top_hosts {
            assert!(!text.contains(&h.host), "{} leaked", h.host);
        }
        for leak in ["did:", "relay-a", "10.0.7.", "addr"] {
            assert!(!text.contains(leak), "{leak} leaked");
        }
        assert_eq!(v["quorum"], "ok");
    }

    #[tokio::test]
    async fn public_route_needs_no_token_and_the_api_does() {
        let d = Demo::start(7);
        let app = crate::admin::api_routes(d.clone(), "t".into()).merge(public_routes(d));
        let get = |uri: &str| Request::builder().uri(uri).body(Body::empty()).unwrap();
        let r = app.clone().oneshot(get("/api/public/stats")).await.unwrap();
        assert_eq!(r.status(), StatusCode::OK);
        let r = app.oneshot(get("/admin/api/cluster/quorum")).await.unwrap();
        assert_eq!(r.status(), StatusCode::UNAUTHORIZED);
    }

    fn node(id: &str, role: &str, members: &[&str], stale: bool) -> crate::admin::QuorumNode {
        crate::admin::QuorumNode {
            node: id.into(),
            stale,
            status: (!stale).then(|| serde_json::json!({"id": id, "role": role, "members": members})),
            ..Default::default()
        }
    }

    #[test]
    fn quorum_health_needs_a_leader_and_a_majority() {
        let m = ["a", "b", "c"];
        let q = |nodes| QuorumView { nodes };
        let ok =
            q(vec![node("a", "leader", &m, false), node("b", "follower", &m, false), node("c", "follower", &m, false)]);
        assert_eq!(quorum_health(&ok), Health::Ok);
        let one_down =
            q(vec![node("a", "leader", &m, false), node("b", "follower", &m, false), node("c", "", &m, true)]);
        assert_eq!(quorum_health(&one_down), Health::Degraded);
        let two_down = q(vec![node("a", "leader", &m, false), node("b", "", &m, true), node("c", "", &m, true)]);
        assert_eq!(quorum_health(&two_down), Health::Down);
        let leaderless = q(vec![node("a", "follower", &m, false), node("b", "follower", &m, false)]);
        assert_eq!(quorum_health(&leaderless), Health::Down);
    }
}
