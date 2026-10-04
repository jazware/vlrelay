//! The peer admin RPC: each node's own numbers and lists, so the node an
//! operator's dashboard talks to can show the whole cluster
//! (`admin::fleet` adds them up).
//!
//! Core nodes serve it on the peer listener under [`CORE_PREFIX`], behind
//! the internal token and, like every peer route that changes state, only
//! to a leased core's certificate (`cluster::peer`). Edges and replicas serve it on
//! their public listener under [`FOLLOWER_PREFIX`], behind the admin token:
//! a replica has no peer listener at all, and a core's peer client only
//! trusts the origins of leased cores, which an edge's isn't. Cores find
//! their peers in the leases and the followers in `--admin-follower`.
//!
//! A member that errors or takes longer than [`TIMEOUT`] is reported stale
//! for that round instead of holding the page.

use super::admin::NodeAdmin;
use crate::admin::fleet::{Member, NodeReport};
use crate::admin::{self, AdminError, AdminResult, History};
use crate::cluster::{ClusterNode, Role};
use axum::extract::{Query, Request, State};
use axum::http::{StatusCode, header};
use axum::middleware::{self, Next};
use axum::response::{IntoResponse, Response};
use axum::routing::{get, post};
use axum::{Json, Router};
use parking_lot::Mutex;
use serde::de::DeserializeOwned;
use serde::{Deserialize, Serialize};
use std::collections::{BTreeMap, HashMap, VecDeque};
use std::sync::{Arc, OnceLock, Weak};
use std::time::{Duration, Instant};

pub const CORE_PREFIX: &str = "/internal/relay/v1/admin";
pub const FOLLOWER_PREFIX: &str = "/admin/api/node";
pub const TIMEOUT: Duration = Duration::from_millis(1500);
/// Gathered reports are shared by the dashboard requests of one refresh.
const CACHE: Duration = Duration::from_millis(800);

/// Where a core's peer router finds the node's admin once `main` built it.
pub type Slot = Arc<OnceLock<Weak<NodeAdmin>>>;

pub fn now_ms() -> i64 {
    crate::upstream::host::now_ms() as i64
}

pub fn role_str(r: Role) -> &'static str {
    match r {
        Role::Core => "core",
        Role::Edge => "edge",
        Role::Replica => "replica",
    }
}

// ---------------------------------------------------------------- process

/// A per-second rate of a counter, between two reads at least a second apart.
#[derive(Default)]
pub struct Rate(Mutex<Option<(Instant, f64, f64)>>);

impl Rate {
    pub fn update(&self, total: f64) -> f64 {
        let mut g = self.0.lock();
        let now = Instant::now();
        match *g {
            Some((at, _, rate)) if now.duration_since(at) < Duration::from_secs(1) => rate,
            Some((at, prev, _)) => {
                let r = ((total - prev) / now.duration_since(at).as_secs_f64()).max(0.0);
                *g = Some((now, total, r));
                r
            }
            None => {
                *g = Some((now, total, 0.0));
                0.0
            }
        }
    }
}

/// CPU seconds this process has used (user + system).
fn cpu_seconds() -> f64 {
    // SAFETY: getrusage fills the struct it's handed.
    unsafe {
        let mut r: libc::rusage = std::mem::zeroed();
        if libc::getrusage(libc::RUSAGE_SELF, &mut r) != 0 {
            return 0.0;
        }
        let s = |t: libc::timeval| t.tv_sec as f64 + t.tv_usec as f64 / 1e6;
        s(r.ru_utime) + s(r.ru_stime)
    }
}

#[derive(Default)]
pub struct Process {
    cpu: Rate,
}

impl Process {
    /// (cores busy, resident bytes).
    pub fn sample(&self) -> (f64, u64) {
        (self.cpu.update(cpu_seconds()), vlpds::metrics::resident_bytes().unwrap_or(0))
    }
}

/// Every relay gauge about in-flight work, queues, backlogs, caps and
/// pauses, by name and labels. Read by name so the ones the pipeline adds
/// later show up without a change here.
pub fn pipeline_gauges() -> BTreeMap<String, f64> {
    const WORDS: [&str; 9] =
        ["inflight", "in_flight", "pending", "queued", "backlog", "paused", "cap", "dedupe", "lag"];
    let mut out = BTreeMap::new();
    for fam in prometheus::gather() {
        let name = fam.name();
        if !name.starts_with("vlrelay_") || !WORDS.iter().any(|w| name.contains(w)) {
            continue;
        }
        if fam.get_field_type() != prometheus::proto::MetricType::GAUGE {
            continue;
        }
        for m in fam.get_metric() {
            let labels: Vec<String> = m.get_label().iter().map(|l| format!("{}=\"{}\"", l.name(), l.value())).collect();
            let key = if labels.is_empty() { name.to_string() } else { format!("{name}{{{}}}", labels.join(",")) };
            out.insert(key, m.get_gauge().get_value());
            if out.len() >= 200 {
                return out;
            }
        }
    }
    out
}

// ---------------------------------------------------------------- core routes

#[derive(Deserialize)]
struct HostQ {
    host: String,
}
#[derive(Deserialize)]
struct IdQ {
    id: u64,
}
#[derive(Deserialize)]
struct DidQ {
    did: String,
}
#[derive(Deserialize)]
struct SearchQ {
    q: String,
}

#[derive(Serialize, Deserialize)]
pub struct TakedownIn {
    pub did: String,
    pub takedown: bool,
    pub by: String,
    pub reason: String,
}

struct CoreCtx {
    slot: Slot,
    token: String,
}

/// The core's routes, merged into its peer listener.
pub fn core_router(slot: Slot, token: String) -> Router {
    let ctx = Arc::new(CoreCtx { slot, token });
    Router::new()
        .route(&format!("{CORE_PREFIX}/report"), get(c_report))
        .route(&format!("{CORE_PREFIX}/hosts"), get(c_hosts))
        .route(&format!("{CORE_PREFIX}/host"), get(c_host))
        .route(&format!("{CORE_PREFIX}/reconnect"), post(c_reconnect))
        .route(&format!("{CORE_PREFIX}/consumers"), get(c_consumers))
        .route(&format!("{CORE_PREFIX}/kick"), post(c_kick))
        .route(&format!("{CORE_PREFIX}/account"), get(c_account))
        .route(&format!("{CORE_PREFIX}/accounts"), get(c_accounts))
        .route(&format!("{CORE_PREFIX}/takedown"), post(c_takedown))
        .route_layer(middleware::from_fn_with_state(ctx.clone(), core_auth))
        .with_state(ctx)
}

async fn core_auth(State(c): State<Arc<CoreCtx>>, req: Request, next: Next) -> Response {
    let t = req.headers().get(crate::cluster::peer::TOKEN_HEADER).and_then(|v| v.to_str().ok()).unwrap_or("");
    if t.is_empty() || !vlpds::auth::token_eq(&c.token, t) {
        return StatusCode::UNAUTHORIZED.into_response();
    }
    next.run(req).await
}

type C = State<Arc<CoreCtx>>;

fn admin_of(c: &CoreCtx) -> AdminResult<Arc<NodeAdmin>> {
    c.slot
        .get()
        .and_then(Weak::upgrade)
        .ok_or_else(|| AdminError::Internal(anyhow::anyhow!("this node's admin isn't up yet")))
}

async fn c_report(State(c): C) -> AdminResult<Json<NodeReport>> {
    Ok(Json(admin_of(&c)?.local_report().await))
}
async fn c_hosts(State(c): C) -> AdminResult<Json<Vec<admin::HostRow>>> {
    Ok(Json(admin_of(&c)?.owned_rows()))
}
async fn c_host(State(c): C, Query(q): Query<HostQ>) -> AdminResult<Json<admin::HostDetail>> {
    Ok(Json(admin_of(&c)?.local_host(&q.host).await?))
}
async fn c_reconnect(State(c): C, Query(q): Query<HostQ>) -> AdminResult<StatusCode> {
    admin_of(&c)?.local_reconnect(&q.host)?;
    Ok(StatusCode::NO_CONTENT)
}
async fn c_consumers(State(c): C) -> AdminResult<Json<Vec<admin::Consumer>>> {
    Ok(Json(admin_of(&c)?.local_consumers()))
}
async fn c_kick(State(c): C, Query(q): Query<IdQ>) -> AdminResult<StatusCode> {
    admin_of(&c)?.local_kick(q.id, "admin (peer)")?;
    Ok(StatusCode::NO_CONTENT)
}
async fn c_account(State(c): C, Query(q): Query<DidQ>) -> AdminResult<Json<admin::Account>> {
    Ok(Json(admin_of(&c)?.local_account(&q.did).await?))
}
async fn c_accounts(State(c): C, Query(q): Query<SearchQ>) -> AdminResult<Json<Vec<admin::Account>>> {
    Ok(Json(admin_of(&c)?.local_accounts(&q.q).await?))
}
async fn c_takedown(State(c): C, Json(t): Json<TakedownIn>) -> AdminResult<Json<admin::Account>> {
    Ok(Json(admin_of(&c)?.local_takedown(&t.did, t.takedown, &t.by, &t.reason).await?))
}

// ---------------------------------------------------------------- followers

/// An edge's or a replica's numbers: its stream and its consumers.
pub struct FollowerAdmin {
    pub node: Arc<ClusterNode>,
    process: Process,
    history: Mutex<VecDeque<(i64, f64, f64)>>,
}

impl FollowerAdmin {
    pub fn start(node: Arc<ClusterNode>) -> Arc<FollowerAdmin> {
        let a = Arc::new(FollowerAdmin { node, process: Process::default(), history: Mutex::new(VecDeque::new()) });
        tokio::spawn(Self::sampler(Arc::downgrade(&a)));
        a
    }

    async fn sampler(me: Weak<FollowerAdmin>) {
        let mut tick = tokio::time::interval(Duration::from_secs(1));
        tick.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
        let mut prev: Option<(u64, u64)> = None;
        loop {
            tick.tick().await;
            let Some(me) = me.upgrade() else { return };
            let now = (vlpds::metrics::FIREHOSE_EVENTS.get(), vlpds::metrics::FIREHOSE_SENT_BYTES.get());
            if let Some(p) = prev {
                let mut h = me.history.lock();
                h.push_back((now_ms() / 1000, now.0.saturating_sub(p.0) as f64, now.1.saturating_sub(p.1) as f64));
                while h.len() > super::metrics::HISTORY {
                    h.pop_front();
                }
            }
            prev = Some(now);
        }
    }

    pub fn consumers(&self) -> Vec<admin::Consumer> {
        consumers_of(&self.node.serve, &self.node.node_id)
    }

    pub fn report(&self) -> NodeReport {
        let (cpu, mem) = self.process.sample();
        let h = self.history.lock();
        let mut hist = History { sample_secs: 1, ..Default::default() };
        for (t, ev, b) in h.iter() {
            hist.t.push(*t);
            hist.events_in.push(0.0);
            hist.events_out.push(*ev);
            hist.bytes_in.push(0.0);
            hist.bytes_out.push(*b);
            hist.ttf_p50_ms.push(0.0);
            hist.ttf_p99_ms.push(0.0);
            hist.durability_lag_ms.push(0.0);
        }
        let last = h.back().copied().unwrap_or_default();
        drop(h);
        NodeReport {
            node: self.node.node_id.clone(),
            role: role_str(self.node.role).into(),
            version: env!("CARGO_PKG_VERSION").into(),
            time_ms: now_ms(),
            events_out_per_sec: last.1,
            bytes_out_per_sec: last.2,
            consumers: self.consumers().len() as u32,
            stream_seq: self.node.serve.firehose.last_emitted.load(std::sync::atomic::Ordering::Acquire),
            cpu,
            mem_bytes: mem,
            history: hist,
            seq_checkpoints: self.node.serve.seq_checkpoints(SEQ_CHECKPOINTS),
            ..Default::default()
        }
    }
}

/// Checkpoints each node reports: a minute of 10 s boundaries.
pub const SEQ_CHECKPOINTS: usize = 6;

pub fn consumers_of(serve: &crate::serve::Serve, node: &str) -> Vec<admin::Consumer> {
    serve
        .consumers()
        .into_iter()
        .map(|c| admin::Consumer {
            id: c.id,
            ip: c.ip.to_string(),
            user_agent: c.user_agent,
            node: node.to_string(),
            connected_since_ms: c.connected_since_ms,
            cursor: c.last_seq,
            lag_ms: c.lag_ms,
            events_per_sec: c.events_per_sec,
            bytes_per_sec: c.bytes_per_sec,
            backfilling: c.backfilling,
        })
        .collect()
}

struct FollowerCtx {
    admin: Arc<FollowerAdmin>,
    token: String,
}

/// An edge's or a replica's routes, on its public listener.
pub fn follower_router(admin: Arc<FollowerAdmin>, admin_token: String) -> Router {
    let ctx = Arc::new(FollowerCtx { admin, token: admin_token });
    Router::new()
        .route(&format!("{FOLLOWER_PREFIX}/report"), get(f_report))
        .route(&format!("{FOLLOWER_PREFIX}/consumers"), get(f_consumers))
        .route(&format!("{FOLLOWER_PREFIX}/kick"), post(f_kick))
        .route_layer(middleware::from_fn_with_state(ctx.clone(), follower_auth))
        .with_state(ctx)
}

async fn follower_auth(State(c): State<Arc<FollowerCtx>>, req: Request, next: Next) -> Response {
    let ok = req
        .headers()
        .get(header::AUTHORIZATION)
        .and_then(|h| h.to_str().ok())
        .and_then(|h| h.strip_prefix("Basic "))
        .is_some_and(|b| vlpds::auth::basic_admin_ok(b, &c.token));
    if !ok {
        return StatusCode::UNAUTHORIZED.into_response();
    }
    next.run(req).await
}

type F = State<Arc<FollowerCtx>>;

async fn f_report(State(c): F) -> Json<NodeReport> {
    Json(c.admin.report())
}
async fn f_consumers(State(c): F) -> Json<Vec<admin::Consumer>> {
    Json(c.admin.consumers())
}
async fn f_kick(State(c): F, Query(q): Query<IdQ>) -> StatusCode {
    if c.admin.node.serve.kick(q.id) {
        tracing::info!(target: "vlrelay::audit", consumer = q.id, by = "admin (peer)", "consumer kicked");
        StatusCode::NO_CONTENT
    } else {
        StatusCode::NOT_FOUND
    }
}

// ---------------------------------------------------------------- client

/// A member the dashboard polls.
#[derive(Clone, Debug)]
pub enum Target {
    /// A core peer, by its peer listener (`https://...`).
    Core { id: String, addr: String },
    /// An edge or a replica, by its public URL.
    Follower { url: String },
}

/// The client half: who to ask, and the last round's answers.
pub struct Fleet {
    followers: Vec<String>,
    admin_token: String,
    http: reqwest::Client,
    gathered: tokio::sync::Mutex<Option<(Instant, Arc<Vec<Member>>)>>,
    /// Follower URL -> (node id, role) once it has answered.
    names: Mutex<HashMap<String, (String, String)>>,
    last_ok: Mutex<HashMap<String, i64>>,
}

impl Fleet {
    pub fn new(followers: Vec<String>, admin_token: String) -> Fleet {
        let http = reqwest::Client::builder()
            .timeout(TIMEOUT)
            .connect_timeout(TIMEOUT)
            .user_agent(concat!("vlrelay/", env!("CARGO_PKG_VERSION"), " (admin)"))
            .build()
            .expect("reqwest client");
        Fleet {
            followers: followers
                .into_iter()
                .map(|u| u.trim_end_matches('/').to_string())
                .filter(|u| !u.is_empty())
                .collect(),
            admin_token,
            http,
            gathered: tokio::sync::Mutex::new(None),
            names: Mutex::new(HashMap::new()),
            last_ok: Mutex::new(HashMap::new()),
        }
    }

    pub fn has_followers(&self) -> bool {
        !self.followers.is_empty()
    }

    /// Every member but this node: core peers from the leases, then the
    /// configured followers.
    pub fn targets(&self, cluster: Option<&ClusterNode>) -> Vec<Target> {
        let mut out = Vec::new();
        if let Some(cl) = cluster.and_then(|c| c.cluster.as_ref()) {
            for l in cl.peers() {
                out.push(Target::Core { id: l.node_id, addr: l.addr });
            }
        }
        out.extend(self.followers.iter().map(|u| Target::Follower { url: u.clone() }));
        out
    }

    /// The member with node id `id`.
    pub fn target(&self, cluster: Option<&ClusterNode>, id: &str) -> Option<Target> {
        self.targets(cluster).into_iter().find(|t| self.id_of(t).0 == id)
    }

    /// (node id, role): a follower is known by its URL until it answers.
    pub fn id_of(&self, t: &Target) -> (String, String) {
        match t {
            Target::Core { id, .. } => (id.clone(), "core".into()),
            Target::Follower { url } => self.names.lock().get(url).cloned().unwrap_or_else(|| {
                let host = url.split("://").nth(1).unwrap_or(url).to_string();
                (host, "follower".into())
            }),
        }
    }

    pub async fn call<T: DeserializeOwned>(
        &self,
        cluster: Option<&ClusterNode>,
        t: &Target,
        method: reqwest::Method,
        path_and_query: &str,
        body: Option<serde_json::Value>,
    ) -> Result<T, String> {
        let req = match t {
            Target::Core { addr, .. } => {
                let c = cluster.ok_or("not a cluster node")?;
                let http = c.http.as_ref().ok_or("no peer client")?;
                http.request(method, format!("{}{CORE_PREFIX}{path_and_query}", addr.trim_end_matches('/')))
                    .header(crate::cluster::peer::TOKEN_HEADER, c.internal_token())
                    .timeout(TIMEOUT)
            }
            Target::Follower { url } => self
                .http
                .request(method, format!("{url}{FOLLOWER_PREFIX}{path_and_query}"))
                .basic_auth("admin", Some(&self.admin_token)),
        };
        let req = match body {
            Some(b) => req.json(&b),
            None => req,
        };
        let r = req.send().await.map_err(|e| short(&e))?;
        let status = r.status();
        if !status.is_success() {
            let text = r.text().await.unwrap_or_default();
            let msg = serde_json::from_str::<serde_json::Value>(&text)
                .ok()
                .and_then(|v| v.get("message").and_then(|m| m.as_str()).map(str::to_string))
                .unwrap_or(text);
            return Err(format!("HTTP {}: {}", status.as_u16(), msg.chars().take(200).collect::<String>()));
        }
        let body = r.bytes().await.map_err(|e| short(&e))?;
        // a 204 decodes as null, for the callers that expect nothing
        let body: &[u8] = if body.is_empty() { b"null" } else { &body };
        serde_json::from_slice(body).map_err(|e| format!("bad answer: {e}"))
    }

    /// This node's report plus every other member's, at most [`TIMEOUT`]
    /// old; repeated calls within [`CACHE`] share one round.
    pub async fn members(
        &self,
        cluster: Option<&ClusterNode>,
        local: impl std::future::Future<Output = NodeReport>,
    ) -> Arc<Vec<Member>> {
        let mut g = self.gathered.lock().await;
        if let Some((at, m)) = &*g
            && at.elapsed() < CACHE
        {
            return m.clone();
        }
        let targets = self.targets(cluster);
        let calls = targets.iter().map(|t| async move {
            let r: Result<NodeReport, String> = self.call(cluster, t, reqwest::Method::GET, "/report", None).await;
            (t, r)
        });
        let (local, answers) = tokio::join!(local, futures::future::join_all(calls));
        let mut out = vec![Member::ok(local)];
        for (t, r) in answers {
            match r {
                Ok(rep) => {
                    if let Target::Follower { url } = t {
                        self.names.lock().insert(url.clone(), (rep.node.clone(), rep.role.clone()));
                    }
                    self.last_ok.lock().insert(rep.node.clone(), rep.time_ms);
                    out.push(Member::ok(rep));
                }
                Err(e) => {
                    let (id, role) = self.id_of(t);
                    let last = self.last_ok.lock().get(&id).copied().unwrap_or(0);
                    out.push(Member::stale(&id, &role, e, last));
                }
            }
        }
        // lease listings come in any order: keep the rows still between refreshes
        out.sort_by(|a, b| a.id.cmp(&b.id));
        let out = Arc::new(out);
        *g = Some((Instant::now(), out.clone()));
        out
    }

    /// The same GET of every other member at once; failures are left out.
    pub async fn each<T: DeserializeOwned>(&self, cluster: Option<&ClusterNode>, path: &str) -> Vec<(String, T)> {
        let targets = self.targets(cluster);
        let calls = targets.iter().map(|t| async move {
            let r: Result<T, String> = self.call(cluster, t, reqwest::Method::GET, path, None).await;
            (self.id_of(t).0, r)
        });
        futures::future::join_all(calls)
            .await
            .into_iter()
            .filter_map(|(id, r)| match r {
                Ok(v) => Some((id, v)),
                Err(e) => {
                    tracing::debug!(node = %id, path, "peer admin call failed: {e}");
                    None
                }
            })
            .collect()
    }
}

fn short(e: &reqwest::Error) -> String {
    if e.is_timeout() {
        return format!("timed out after {} ms", TIMEOUT.as_millis());
    }
    if e.is_connect() {
        return "connection refused or unreachable".into();
    }
    let mut s = e.to_string();
    let mut src = std::error::Error::source(e);
    while let Some(x) = src {
        s = format!("{s}: {x}");
        src = x.source();
    }
    s.chars().take(200).collect()
}
