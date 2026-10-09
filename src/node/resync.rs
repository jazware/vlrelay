//! Resyncing desynchronized accounts (sync 1.1).
//!
//! An account whose chain broke here (an event the relay never got: a host
//! that skipped past our cursor, a long outage) has every later commit
//! rejected, since each one builds on a head the relay doesn't hold. The
//! host owner that gets such a rejection asks the account's PDS for its
//! current commit (`getLatestCommit`, then that block from `getBlocks`),
//! checks its signature like any `#sync`, and sends it to the leader as one.
//! The leader resets the account's chain to it and emits the `#sync`, so
//! consumers resync too, and the account's next commit chains again.
//!
//! Politeness: one request at a time per host, at most
//! [`PER_HOST_PER_SEC`] accounts a second each, [`CONCURRENCY`] hosts at
//! once, and an account at most once per [`COOLDOWN`].

use super::{Checked, CheckedKind, Node, Submitted, forward::Outcome};
use crate::types::Host;
use bytes::Bytes;
use parking_lot::Mutex;
use std::collections::{HashMap, VecDeque};
use std::num::NonZeroUsize;
use std::sync::{Arc, Weak};
use std::time::{Duration, Instant};
use vlatproto::cid::Cid;

/// Rejections that mean the relay holds a head the account has moved past.
pub const RESYNC_REASONS: &[&str] = &["prev_data_mismatch", "desynchronized", "chain"];

pub const COOLDOWN: Duration = Duration::from_secs(60);
pub const PER_HOST_PER_SEC: f64 = 5.0;
pub const CONCURRENCY: usize = 8;
/// Accounts waiting per host; past it the rest wait for their next
/// rejection after the cooldown.
const QUEUE_PER_HOST: usize = 1024;
const TIMEOUT: Duration = Duration::from_secs(10);
const MAX_BLOCK_BYTES: usize = 64 << 10;

#[derive(Default)]
struct Queues {
    /// A host is here while its drain runs, its queue empty or not.
    by_host: HashMap<Host, VecDeque<String>>,
    /// DID -> when it was last queued.
    recent: Option<lru::LruCache<String, Instant>>,
}

pub struct Resync {
    queues: Mutex<Queues>,
    slots: Arc<tokio::sync::Semaphore>,
    cooldown: Duration,
    per_host_gap: Duration,
}

/// How one resync ended, for `vlrelay_resyncs_total`.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Done {
    /// The leader took the `#sync`.
    Synced,
    /// The relay already held that commit.
    Unchanged,
    Rejected,
    /// The PDS didn't answer, or answered something that doesn't check out.
    Fetch,
    /// The host or the leader moved on (another node reads it now, a retry
    /// gave up).
    Dropped,
}

impl Done {
    fn label(self) -> &'static str {
        match self {
            Done::Synced => "synced",
            Done::Unchanged => "unchanged",
            Done::Rejected => "rejected",
            Done::Fetch => "fetch_failed",
            Done::Dropped => "dropped",
        }
    }
}

impl Default for Resync {
    fn default() -> Resync {
        Resync::new(COOLDOWN, PER_HOST_PER_SEC)
    }
}

impl Resync {
    pub fn new(cooldown: Duration, per_host_per_sec: f64) -> Resync {
        Resync {
            queues: Mutex::new(Queues {
                by_host: HashMap::new(),
                recent: Some(lru::LruCache::new(NonZeroUsize::new(1 << 16).expect("nonzero"))),
            }),
            slots: Arc::new(tokio::sync::Semaphore::new(CONCURRENCY)),
            cooldown,
            per_host_gap: Duration::from_secs_f64(1.0 / per_host_per_sec.max(0.01)),
        }
    }

    /// Queues `did` of `host` unless it was queued within the cooldown;
    /// true if this starts the host's queue (the caller spawns its drain).
    fn push(&self, host: &Host, did: &str) -> bool {
        let mut q = self.queues.lock();
        let recent = q.recent.as_mut().expect("set");
        if recent.get(did).is_some_and(|at| at.elapsed() < self.cooldown) {
            return false;
        }
        let waiting = q.by_host.get(host).map_or(0, |v| v.len());
        if waiting >= QUEUE_PER_HOST {
            return false;
        }
        q.recent.as_mut().expect("set").put(did.to_string(), Instant::now());
        let start = !q.by_host.contains_key(host);
        q.by_host.entry(host.clone()).or_default().push_back(did.to_string());
        start
    }

    fn pop(&self, host: &Host) -> Option<String> {
        let mut q = self.queues.lock();
        let d = q.by_host.get_mut(host)?.pop_front();
        if d.is_none() {
            q.by_host.remove(host);
        }
        d
    }

    /// Accounts waiting, all hosts together.
    pub fn waiting(&self) -> usize {
        self.queues.lock().by_host.values().map(|v| v.len()).sum()
    }
}

impl Node {
    /// `did`'s commit from `host` was rejected for a chain this relay can't
    /// follow: fetch the account's current commit and send it as a `#sync`.
    pub(super) fn want_resync(self: &Arc<Self>, host: &Host, did: &str) {
        if !self.resync.push(host, did) {
            return;
        }
        let (node, host) = (Arc::downgrade(self), host.clone());
        self.ingest.spawn(drain(node, host));
    }

    async fn resync_one(self: &Arc<Self>, host: &Host, did: &str) -> Done {
        let base = (self.manager.config().endpoint)(host);
        let frame = match fetch_sync_frame(&base, did, self.cfg.dev_mode).await {
            Ok(f) => f,
            Err(e) => {
                tracing::info!(host = %host.0, did, "resync: {e:#}");
                return Done::Fetch;
            }
        };
        let s = match crate::event::parse(frame.clone(), &crate::event::Limits::default()) {
            Ok(crate::event::Event::Sync(s)) => s,
            Ok(_) => return Done::Fetch,
            Err(e) => {
                tracing::info!(host = %host.0, did, "resync: a bad frame: {e}");
                return Done::Fetch;
            }
        };
        let opts = crate::verify::Options::default();
        let v = match self.verified(did, host, frame.len(), |k| crate::verify::verify_sync_with(&s, k, &opts)).await {
            Ok(v) => v,
            Err(r) => {
                tracing::info!(host = %host.0, did, reason = r.reason, "resync: the commit doesn't check out: {}", r.detail);
                return Done::Fetch;
            }
        };
        let c = Checked {
            did: did.to_string(),
            host: host.clone(),
            upstream_seq: 0,
            kind: CheckedKind::Sync(v),
            frame: s.frame.clone(),
            span: s.seq_span,
            received: Instant::now(),
            // a head the relay already holds needs no announcement
            first_sighting: false,
            fence: None,
        };
        match self.owner.submit(c).await {
            Submitted::Rejected(_) => Done::Rejected,
            Submitted::Forwarded(rx) => match rx.await {
                Ok(Ok(Outcome::Appended(_))) => Done::Synced,
                Ok(Ok(Outcome::Duplicate)) => Done::Unchanged,
                Ok(Ok(Outcome::Rejected(r))) => {
                    tracing::info!(host = %host.0, did, "resync: the leader refused it: {r}");
                    Done::Rejected
                }
                _ => Done::Dropped,
            },
        }
    }
}

/// Works through `host`'s queue one account at a time, paced, while this
/// node still reads the host.
async fn drain(node: Weak<Node>, host: Host) {
    let mut last: Option<Instant> = None;
    loop {
        let Some(n) = node.upgrade() else { return };
        let Some(did) = n.resync.pop(&host) else { return };
        if !n.manager.is_running(&host) {
            super::metrics::RESYNCS.with_label_values(&[Done::Dropped.label()]).inc();
            continue;
        }
        if let Some(t) = last {
            let gap = n.resync.per_host_gap.saturating_sub(t.elapsed());
            if !gap.is_zero() {
                tokio::time::sleep(gap).await;
            }
        }
        let slots = n.resync.slots.clone();
        let Ok(_permit) = slots.acquire_owned().await else { return };
        last = Some(Instant::now());
        let done = n.resync_one(&host, &did).await;
        super::metrics::RESYNCS.with_label_values(&[done.label()]).inc();
        if done == Done::Synced {
            tracing::info!(host = %host.0, did, "resync: the account's chain is back");
        }
    }
}

/// The account's current commit as a `#sync` frame (seq 0, the leader
/// writes its own): `getLatestCommit` for its CID and rev, `getBlocks` for
/// the signed block.
pub async fn fetch_sync_frame(base: &str, did: &str, dev_mode: bool) -> anyhow::Result<Bytes> {
    #[derive(serde::Deserialize)]
    struct Latest {
        cid: String,
        rev: String,
    }
    let base = base.trim_end_matches('/');
    let did_q = urlencode(did);
    let get = |url: String| async move {
        let req = vlatproto::http::guarded(dev_mode).get(&url).map_err(|e| anyhow::anyhow!("{e}"))?;
        let resp = tokio::time::timeout(TIMEOUT, req.send()).await??;
        anyhow::ensure!(resp.status().is_success(), "{url}: {}", resp.status());
        let body = tokio::time::timeout(TIMEOUT, read_capped(resp, MAX_BLOCK_BYTES)).await??;
        anyhow::Ok(body)
    };
    let latest = get(format!("{base}/xrpc/com.atproto.sync.getLatestCommit?did={did_q}")).await?;
    let latest: Latest = serde_json::from_slice(&latest)?;
    let cid = Cid::parse(&latest.cid).map_err(|e| anyhow::anyhow!("getLatestCommit's cid: {e:?}"))?;
    anyhow::ensure!(vlatproto::tid::Tid::parse(&latest.rev).is_some(), "getLatestCommit's rev {:?}", latest.rev);
    let car = get(format!("{base}/xrpc/com.atproto.sync.getBlocks?did={did_q}&cids={}", latest.cid)).await?;
    let (_, blocks) = vlatproto::car::read_car(&car)?;
    let (_, block) =
        blocks.into_iter().find(|(c, _)| *c == cid).ok_or_else(|| anyhow::anyhow!("getBlocks left out {cid}"))?;
    let mut out = Vec::with_capacity(block.len() + 96);
    vlatproto::car::write_header(&mut out, &cid);
    vlatproto::car::write_block(&mut out, &cid, block);
    let f = vlatproto::events::sync_frame(did, &latest.rev, &out, &vlatproto::events::now_rfc3339());
    let mut frame = Vec::with_capacity(f.len_hint());
    f.finish(0, &mut frame);
    Ok(frame.into())
}

async fn read_capped(mut resp: reqwest::Response, cap: usize) -> anyhow::Result<Vec<u8>> {
    let mut out = Vec::new();
    while let Some(chunk) = resp.chunk().await? {
        anyhow::ensure!(out.len() + chunk.len() <= cap, "the answer is over {cap} bytes");
        out.extend_from_slice(&chunk);
    }
    Ok(out)
}

fn urlencode(s: &str) -> String {
    s.bytes()
        .map(|b| match b {
            b'A'..=b'Z' | b'a'..=b'z' | b'0'..=b'9' | b'-' | b'.' | b'_' | b'~' => (b as char).to_string(),
            _ => format!("%{b:02X}"),
        })
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn an_account_is_queued_once_per_cooldown_and_hosts_drain_apart() {
        let r = Resync::new(Duration::from_secs(60), 5.0);
        let (a, b) = (Host("a.example".into()), Host("b.example".into()));
        assert!(r.push(&a, "did:plc:one"), "the first starts a's drain");
        assert!(!r.push(&a, "did:plc:one"), "within the cooldown");
        assert!(!r.push(&a, "did:plc:two"), "a's drain is running");
        assert!(r.push(&b, "did:plc:three"), "b drains on its own");
        assert_eq!(r.waiting(), 3);
        assert_eq!(r.pop(&a).as_deref(), Some("did:plc:one"));
        assert!(!r.push(&a, "did:plc:four"), "a's drain still runs with its queue empty");
        assert_eq!(r.pop(&a).as_deref(), Some("did:plc:two"));
        assert_eq!(r.pop(&a).as_deref(), Some("did:plc:four"));
        assert_eq!(r.pop(&a), None, "the drain ends");
        assert!(r.push(&a, "did:plc:five"), "and the next account starts another");
    }

    #[test]
    fn a_full_host_queue_drops_the_rest() {
        let r = Resync::new(Duration::from_secs(60), 5.0);
        let h = Host("busy.example".into());
        for i in 0..QUEUE_PER_HOST + 10 {
            r.push(&h, &format!("did:plc:{i}"));
        }
        assert_eq!(r.waiting(), QUEUE_PER_HOST);
        // a dropped one isn't in its cooldown: its next rejection asks again
        r.pop(&h);
        r.push(&h, &format!("did:plc:{}", QUEUE_PER_HOST + 5));
        assert_eq!(r.waiting(), QUEUE_PER_HOST);
    }

    /// A PDS of one account for the relay: its frames from a cursor, live
    /// as they're published, and its current commit by getLatestCommit and
    /// getBlocks. The PLC directory on the same port names it the PDS.
    mod pds {
        use crate::verify::synth::Repo;
        use axum::extract::ws::{Message, WebSocketUpgrade};
        use axum::extract::{Path, Query, State};
        use axum::response::{IntoResponse, Response};
        use axum::routing::get;
        use bytes::Bytes;
        use parking_lot::Mutex;
        use std::collections::HashMap;
        use std::sync::Arc;
        use tokio::sync::watch;

        pub struct Pds {
            pub url: String,
            pub repo: Mutex<Repo>,
            frames: Mutex<Vec<(i64, Bytes)>>,
            head: watch::Sender<i64>,
        }

        impl Pds {
            pub async fn start(repo: Repo) -> Arc<Pds> {
                let lis = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
                let url = format!("http://{}", lis.local_addr().unwrap());
                let p = Arc::new(Pds {
                    url,
                    repo: Mutex::new(repo),
                    frames: Mutex::new(Vec::new()),
                    head: watch::channel(0).0,
                });
                let app = axum::Router::new()
                    .route("/xrpc/com.atproto.sync.subscribeRepos", get(subscribe))
                    .route("/xrpc/com.atproto.sync.getLatestCommit", get(latest))
                    .route("/xrpc/com.atproto.sync.getBlocks", get(blocks))
                    .route("/{did}", get(doc))
                    .with_state(p.clone());
                tokio::spawn(axum::serve(lis, app).into_future());
                p
            }

            /// The account's next commit, sent (or, `false`, never sent: a
            /// gap the relay can't see past).
            pub fn commit(&self, send: bool) {
                let (seq, f) = {
                    let mut r = self.repo.lock();
                    let ops = r.mixed_ops(2);
                    let f = r.commit(&ops);
                    (r.seq, f)
                };
                if send {
                    self.frames.lock().push((seq, f));
                    self.head.send_replace(seq);
                }
            }
        }

        async fn subscribe(
            State(p): State<Arc<Pds>>,
            Query(q): Query<HashMap<String, String>>,
            ws: WebSocketUpgrade,
        ) -> Response {
            let mut at: i64 = q.get("cursor").and_then(|c| c.parse().ok()).unwrap_or(0);
            ws.on_upgrade(move |mut sock| async move {
                let mut head = p.head.subscribe();
                loop {
                    let next: Vec<(i64, Bytes)> = p.frames.lock().iter().filter(|(s, _)| *s > at).cloned().collect();
                    for (s, f) in next {
                        if sock.send(Message::Binary(f)).await.is_err() {
                            return;
                        }
                        at = s;
                    }
                    if head.changed().await.is_err() {
                        return;
                    }
                }
            })
        }

        async fn latest(State(p): State<Arc<Pds>>) -> Response {
            let r = p.repo.lock();
            axum::Json(serde_json::json!({"cid": r.commit.to_string(), "rev": r.rev.to_string()})).into_response()
        }

        async fn blocks(State(p): State<Arc<Pds>>) -> Response {
            let r = p.repo.lock();
            let mut car = Vec::new();
            vlatproto::car::write_header(&mut car, &r.commit);
            vlatproto::car::write_block(&mut car, &r.commit, r.commit_block());
            car.into_response()
        }

        async fn doc(State(p): State<Arc<Pds>>, Path(did): Path<String>) -> Response {
            let r = p.repo.lock();
            if did != r.did {
                return axum::http::StatusCode::NOT_FOUND.into_response();
            }
            axum::Json(serde_json::json!({
                "id": did,
                "alsoKnownAs": ["at://someone.test"],
                "verificationMethod": [{
                    "id": format!("{did}#atproto"),
                    "type": "Multikey",
                    "controller": did,
                    "publicKeyMultibase": r.signer.multibase(),
                }],
                "service": [{"id": "#atproto_pds", "type": "AtprotoPersonalDataServer", "serviceEndpoint": p.url}],
            }))
            .into_response()
        }
    }

    async fn until(what: &str, secs: u64, mut f: impl FnMut() -> bool) {
        let t = Instant::now();
        while !f() {
            assert!(t.elapsed() < Duration::from_secs(secs), "waiting for {what}");
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
    }

    /// A commit the relay never saw (here a PDS that skipped it; on the
    /// relay, a host that skipped past a moved host's cursor) broke the
    /// account's chain for good: every later commit was rejected as
    /// desynchronized, and nothing asked the PDS for the account's head.
    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn a_desynchronized_account_is_resynced_from_its_pds() {
        use crate::verify::synth::{Curve, Repo, Signer};
        let did = "did:plc:zzzzzzzzzzzzzzzzzzzzzzzz";
        let pds = pds::Pds::start(Repo::new(did, Signer::new(Curve::K256, 7), 3)).await;
        let addr = format!("127.0.0.1:{}", crate::qlog::tests::free_port());
        let mut cfg = crate::node::NodeConfig::new(&pds.url);
        cfg.node_id = "n1".into();
        cfg.dev_mode = true;
        cfg.lanes = 2;
        cfg.ingest_threads = 2;
        cfg.serve_threads = 1;
        cfg.hosts = vec![pds.url.clone()];
        let mut q = crate::node::quorum::QuorumSetup::new(&addr);
        q.host_poll = Duration::from_millis(100);
        q.flush = Duration::from_millis(300);
        q.retain_horizon = None;
        let node = Node::start(vlsync_store::store::Store::memory(None), cfg, q).await.unwrap();
        let host = Host(pds.url.trim_start_matches("http://").to_string());
        let passed = |n: &Node| n.passed.lock().iter().map(|p| p.upstream_seq).collect::<Vec<_>>();
        let rejected =
            |n: &Node, why: &str| n.rejects.lock().get(&host).and_then(|h| h.by_reason.get(why).copied()).unwrap_or(0);

        pds.commit(true);
        pds.commit(true);
        until("the first commits", 30, || passed(&node).len() == 2).await;
        // the third never reaches the relay
        pds.commit(false);
        pds.commit(true);
        pds.commit(true);
        until("the broken chain", 30, || rejected(&node, "prev_data_mismatch") == 1).await;
        let head = pds.repo.lock().rev;
        let t = Instant::now();
        let rec = loop {
            let rec = node.state.get(did).await.unwrap().unwrap();
            if rec.chain.map(|c| c.rev) == Some(head) {
                break rec;
            }
            assert!(t.elapsed() < Duration::from_secs(30), "the account was never resynced to its PDS's head");
            tokio::time::sleep(Duration::from_millis(50)).await;
        };
        assert_eq!(rec.status(), crate::state::AccountStatus::Active);

        // and the account's commits chain again
        pds.commit(true);
        pds.commit(true);
        until("commits after the resync", 30, || passed(&node).iter().filter(|s| **s >= 6).count() == 2).await;
        assert!(
            rejected(&node, "desynchronized") <= 1,
            "{:?}",
            node.rejects.lock().get(&host).map(|h| h.by_reason.clone())
        );
    }
}
