//! Host aliases: one PDS that the relay knows under several hostnames. Each
//! name streams the same events at the same seqs, the DID documents name
//! one of them, and the copies from the others are `wrong_host` rejects or
//! duplicates (docs/compat.md, "Host authority").
//!
//! The leader watches a pair once a host's event is rejected for naming
//! another host, or is a duplicate of an event another host sent first:
//! every event either sends is keyed by (upstream seq, DID),
//! and the pair is the same stream when enough of them match and none is
//! missing from the other side. [`AliasWatch`] only decides that; the
//! [`confirm`] task checks that both names answer `describeServer` with the
//! same service DID before it marks the sender an alias, which closes its
//! socket and lets the other name's events speak for its accounts.

use super::policy::PolicyHooks;
use crate::admin::HostAction;
use crate::policy::tiers::RELAY_ACTOR;
use crate::types::Host;
use crate::upstream::{EndpointFn, UpstreamConfig};
use parking_lot::Mutex;
use std::collections::{HashMap, VecDeque};
use std::hash::BuildHasher;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::{Duration, Instant};
use tokio::sync::mpsc;

#[derive(Clone, Debug)]
pub struct AliasConfig {
    /// Events in a watch's first stretch neither match nor miss: the other
    /// side may have sent its copy before the watch began.
    pub warmup: Duration,
    /// An event the other side hasn't sent this long after, once that
    /// side's seq has passed it, is missing there.
    pub window: Duration,
    /// Matched events (after the warm-up) that make a pair one stream.
    pub min_matches: u32,
    /// A watch that hasn't decided by then gives up (a quiet PDS).
    pub max_watch: Duration,
    pub max_watches: usize,
    /// Unmatched events a watch holds before it gives up (one side far
    /// behind the other).
    pub max_pending: usize,
    /// A pair isn't watched again for this long after a watch ends.
    pub cooldown: Duration,
}

impl Default for AliasConfig {
    fn default() -> AliasConfig {
        AliasConfig {
            warmup: Duration::from_secs(120),
            window: Duration::from_secs(120),
            min_matches: 20,
            max_watch: Duration::from_secs(2 * 3_600),
            max_watches: 16,
            max_pending: 20_000,
            cooldown: Duration::from_secs(6 * 3_600),
        }
    }
}

/// A pair the watch found to be one stream: `alias` sent events whose DID
/// documents name `of`.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Found {
    pub alias: String,
    pub of: String,
    pub matched: u32,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum Side {
    Alias,
    Of,
    /// Both sent it.
    Matched,
}

struct Entry {
    side: Side,
    at: Instant,
}

struct Watch {
    alias: String,
    of: String,
    started: Instant,
    matches: u32,
    /// (upstream seq, DID hash)
    pending: HashMap<(i64, u64), Entry>,
    order: VecDeque<((i64, u64), Instant)>,
    /// Each side's highest seq so far.
    max_seq: [i64; 2],
}

enum Verdict {
    Open,
    Same,
    Differ(&'static str),
}

impl Watch {
    fn side(&self, host: &str) -> Option<Side> {
        if host == self.alias {
            Some(Side::Alias)
        } else if host == self.of {
            Some(Side::Of)
        } else {
            None
        }
    }

    fn observe(&mut self, side: Side, key: (i64, u64), now: Instant, warm: Instant) {
        let i = (side == Side::Of) as usize;
        self.max_seq[i] = self.max_seq[i].max(key.0);
        match self.pending.get_mut(&key) {
            Some(e) if e.side != side && e.side != Side::Matched => {
                e.side = Side::Matched;
                if now >= warm {
                    self.matches += 1;
                }
            }
            // a replay of its own copy
            Some(_) => {}
            None => {
                self.pending.insert(key, Entry { side, at: now });
                self.order.push_back((key, now));
            }
        }
    }

    fn verdict(&mut self, cfg: &AliasConfig, now: Instant) -> Verdict {
        let warm = self.started + cfg.warmup;
        while let Some(&(key, at)) = self.order.front() {
            if now.duration_since(at) < cfg.window {
                break;
            }
            let Some(e) = self.pending.get(&key) else {
                self.order.pop_front();
                continue;
            };
            let other = match e.side {
                Side::Alias => self.max_seq[1],
                Side::Of => self.max_seq[0],
                Side::Matched => i64::MAX,
            };
            // the other side is behind it: wait for it to catch up
            if e.side != Side::Matched && other < key.0 {
                break;
            }
            self.order.pop_front();
            let e = self.pending.remove(&key).expect("present");
            if e.side != Side::Matched && e.at >= warm {
                return Verdict::Differ(match e.side {
                    Side::Alias => "an event the other host didn't send",
                    _ => "an event the alias didn't send",
                });
            }
        }
        if self.matches >= cfg.min_matches {
            return Verdict::Same;
        }
        if self.pending.len() > cfg.max_pending {
            return Verdict::Differ("too far behind the other host");
        }
        if now.duration_since(self.started) >= cfg.max_watch {
            return Verdict::Differ("too few events to decide");
        }
        Verdict::Open
    }
}

#[derive(Default)]
struct Inner {
    watches: Vec<Watch>,
    cooldown: HashMap<(String, String), Instant>,
}

/// Which hosts may be watched as an alias: not one already, nor one an
/// operator marked its own PDS.
pub type Eligible = Arc<dyn Fn(&str, &str) -> bool + Send + Sync>;

pub struct AliasWatch {
    cfg: AliasConfig,
    any: AtomicBool,
    inner: Mutex<Inner>,
    hasher: foldhash::fast::RandomState,
    found: mpsc::UnboundedSender<Found>,
    eligible: parking_lot::RwLock<Option<Eligible>>,
}

impl AliasWatch {
    pub fn new(cfg: AliasConfig) -> (Arc<AliasWatch>, mpsc::UnboundedReceiver<Found>) {
        let (tx, rx) = mpsc::unbounded_channel();
        let w = AliasWatch {
            cfg,
            any: AtomicBool::new(false),
            inner: Mutex::new(Inner::default()),
            hasher: Default::default(),
            found: tx,
            eligible: Default::default(),
        };
        (Arc::new(w), rx)
    }

    pub fn set_eligible(&self, f: Eligible) {
        *self.eligible.write() = Some(f);
    }

    /// `host` sent an event whose DID document names `expected`, or a copy
    /// of one `expected` sent first.
    pub fn suspect(&self, host: &str, expected: &str, now: Instant) {
        if host == expected || self.eligible.read().as_ref().is_some_and(|f| !f(host, expected)) {
            return;
        }
        let mut i = self.inner.lock();
        if i.watches.len() >= self.cfg.max_watches
            || i.watches.iter().any(|w| w.side(host).is_some() && w.side(expected).is_some())
        {
            return;
        }
        let key = (host.to_string(), expected.to_string());
        if i.cooldown.get(&key).is_some_and(|until| now < *until) {
            return;
        }
        i.cooldown.retain(|_, until| now < *until);
        tracing::debug!(host, of = expected, "alias watch: started");
        i.watches.push(Watch {
            alias: key.0,
            of: key.1,
            started: now,
            matches: 0,
            pending: HashMap::new(),
            order: VecDeque::new(),
            max_seq: [i64::MIN; 2],
        });
        super::metrics::ALIAS_WATCHES.with_label_values(&["started"]).inc();
        self.any.store(true, Ordering::Relaxed);
    }

    /// An event from `host` reached the leader, whatever became of it.
    pub fn observe(&self, host: &str, upstream_seq: i64, did: &str, now: Instant) {
        if !self.any.load(Ordering::Relaxed) {
            return;
        }
        let key = (upstream_seq, self.hasher.hash_one(did));
        let mut i = self.inner.lock();
        let cfg = &self.cfg;
        let mut ended = Vec::new();
        for (n, w) in i.watches.iter_mut().enumerate() {
            let Some(side) = w.side(host) else { continue };
            w.observe(side, key, now, w.started + cfg.warmup);
            match w.verdict(cfg, now) {
                Verdict::Open => {}
                v => ended.push((n, v)),
            }
        }
        for (n, v) in ended.into_iter().rev() {
            let w = i.watches.swap_remove(n);
            match v {
                Verdict::Same => {
                    tracing::info!(host = %w.alias, of = %w.of, matched = w.matches, "alias watch: one stream");
                    super::metrics::ALIAS_WATCHES.with_label_values(&["same"]).inc();
                    let _ = self.found.send(Found { alias: w.alias.clone(), of: w.of.clone(), matched: w.matches });
                }
                Verdict::Differ(why) => {
                    tracing::debug!(host = %w.alias, of = %w.of, matched = w.matches, why, "alias watch: not one stream");
                    super::metrics::ALIAS_WATCHES.with_label_values(&["differ"]).inc();
                }
                Verdict::Open => unreachable!(),
            }
            i.cooldown.insert((w.alias, w.of), now + cfg.cooldown);
        }
        self.any.store(!i.watches.is_empty(), Ordering::Relaxed);
    }

    /// Pairs being watched, as (alias, of).
    pub fn watching(&self) -> Vec<(String, String)> {
        self.inner.lock().watches.iter().map(|w| (w.alias.clone(), w.of.clone())).collect()
    }
}

// ---------------------------------------------------------------- confirm

/// How often the relay's aliases are checked again, and how often the
/// recheck loop looks for ones due.
pub const RECHECK_AFTER: Duration = Duration::from_secs(24 * 3_600);
const RECHECK_EVERY: Duration = Duration::from_secs(3_600);
const DESCRIBE_TIMEOUT: Duration = Duration::from_secs(10);
const DESCRIBE_MAX_BYTES: usize = 64 << 10;

/// The service DID a host's `describeServer` names.
pub async fn service_did(cfg: &UpstreamConfig, host: &str) -> anyhow::Result<String> {
    service_did_at(&cfg.endpoint, cfg.dev_mode, host).await
}

async fn service_did_at(endpoint: &EndpointFn, dev_mode: bool, host: &str) -> anyhow::Result<String> {
    let base = endpoint(&Host(host.to_string()));
    let url = format!("{}/xrpc/com.atproto.server.describeServer", base.trim_end_matches('/'));
    let req = vlatproto::http::guarded(dev_mode).get(&url).map_err(|e| anyhow::anyhow!(e))?;
    let mut resp = req.timeout(DESCRIBE_TIMEOUT).send().await?;
    anyhow::ensure!(resp.status().is_success(), "describeServer: HTTP {}", resp.status());
    let mut body = Vec::new();
    while let Some(c) = resp.chunk().await? {
        anyhow::ensure!(body.len() + c.len() <= DESCRIBE_MAX_BYTES, "describeServer: over {DESCRIBE_MAX_BYTES} bytes");
        body.extend_from_slice(&c);
    }
    let j: serde_json::Value = serde_json::from_slice(&body)?;
    match j.get("did").and_then(|d| d.as_str()) {
        Some(d) if d.starts_with("did:") => Ok(d.to_string()),
        _ => anyhow::bail!("describeServer: no service DID"),
    }
}

/// Whether two hosts answer `describeServer` with one service DID.
async fn same_service(cfg: &UpstreamConfig, a: &str, b: &str) -> anyhow::Result<Option<String>> {
    let (da, db) = tokio::join!(service_did(cfg, a), service_did(cfg, b));
    let (da, db) = (da?, db?);
    Ok((da == db).then_some(da))
}

/// Marks what the watch finds, after the `describeServer` check, and
/// rechecks the relay's aliases a day after each was last confirmed.
/// `leading` says whether this node leads: only the leader rechecks.
pub fn spawn(
    hooks: Arc<PolicyHooks>,
    upstream: Arc<UpstreamConfig>,
    mut found: mpsc::UnboundedReceiver<Found>,
    leading: Arc<dyn Fn() -> bool + Send + Sync>,
) {
    tokio::spawn(async move {
        let mut tick = tokio::time::interval(RECHECK_EVERY);
        tick.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
        loop {
            tokio::select! {
                f = found.recv() => {
                    let Some(f) = f else { return };
                    confirm(&hooks, &upstream, &f).await;
                }
                _ = tick.tick() => {
                    if leading() {
                        recheck(&hooks, &upstream, crate::state::now_secs()).await;
                    }
                }
            }
        }
    });
}

/// Marks `f.alias` an alias if it may be one and both names are one
/// service. Returns whether it did.
pub async fn confirm(hooks: &PolicyHooks, upstream: &UpstreamConfig, f: &Found) -> bool {
    let Some(of) = hooks.alias_candidate(&f.alias, &f.of) else { return false };
    match same_service(upstream, &f.alias, &of).await {
        Ok(Some(did)) => {
            let action = HostAction::Alias { of: of.clone() };
            if let Err(e) = hooks.admin.host_action(&f.alias, action, RELAY_ACTOR).await {
                tracing::warn!(host = %f.alias, of, "marking an alias failed: {e:?}");
                return false;
            }
            if let Err(e) = hooks.refresh_host(&f.alias).await {
                tracing::warn!(host = %f.alias, "applying an alias: {e:#}");
            }
            tracing::info!(
                target: "vlrelay::audit",
                host = %f.alias, of, service = did, matched = f.matched,
                "host is an alias: same events at the same seqs, same describeServer DID"
            );
            super::metrics::ALIAS_WATCHES.with_label_values(&["marked"]).inc();
            true
        }
        Ok(None) => {
            tracing::info!(host = %f.alias, of, "alias watch: one stream, but describeServer names two services");
            false
        }
        Err(e) => {
            tracing::info!(host = %f.alias, of, "alias watch: describeServer failed: {e:#}");
            false
        }
    }
}

/// Checks each of the relay's aliases a day after it was last confirmed:
/// the same service DID renews it, a different one clears it (the name is
/// another PDS now, and gets its socket back). A host that doesn't answer
/// keeps its alias.
pub async fn recheck(hooks: &PolicyHooks, upstream: &UpstreamConfig, now: u32) {
    for (host, a) in hooks.relay_aliases() {
        if (now.saturating_sub(a.at) as u64) < RECHECK_AFTER.as_secs() {
            continue;
        }
        let action = match same_service(upstream, &host, &a.of).await {
            Ok(Some(_)) => HostAction::Alias { of: a.of.clone() },
            Ok(None) => HostAction::Unalias { pin: false },
            Err(e) => {
                tracing::debug!(host, of = %a.of, "alias recheck: {e:#}");
                continue;
            }
        };
        let cleared = matches!(action, HostAction::Unalias { .. });
        if let Err(e) = hooks.admin.host_action(&host, action, RELAY_ACTOR).await {
            tracing::warn!(host, "alias recheck: {e:?}");
            continue;
        }
        if cleared {
            tracing::info!(target: "vlrelay::audit", host, of = %a.of, "alias cleared: describeServer names another service now");
            if let Err(e) = hooks.refresh_host(&host).await {
                tracing::warn!(host, "applying a cleared alias: {e:#}");
            }
        }
    }
}

#[cfg(test)]
pub(crate) mod tests {
    use super::*;

    fn cfg() -> AliasConfig {
        AliasConfig {
            warmup: Duration::from_secs(10),
            window: Duration::from_secs(10),
            min_matches: 5,
            max_watch: Duration::from_secs(600),
            max_watches: 2,
            max_pending: 100,
            cooldown: Duration::from_secs(60),
        }
    }

    fn feed(w: &AliasWatch, host: &str, seqs: std::ops::Range<i64>, at: Instant) {
        for s in seqs {
            w.observe(host, s, &format!("did:plc:{}", s % 7), at);
        }
    }

    #[tokio::test]
    async fn one_stream_under_two_names_is_found() {
        let (w, mut rx) = AliasWatch::new(cfg());
        let t0 = Instant::now();
        w.suspect("alias.example", "pds.example", t0);
        // copies from before the watch began, and the warm-up, count for nothing
        feed(&w, "alias.example", 100..103, t0);
        feed(&w, "pds.example", 103..106, t0);
        feed(&w, "alias.example", 103..106, t0);
        assert!(rx.try_recv().is_err());
        let t1 = t0 + Duration::from_secs(11);
        for s in 106..110 {
            w.observe("pds.example", s, &format!("did:plc:{}", s % 7), t1);
            w.observe("alias.example", s, &format!("did:plc:{}", s % 7), t1);
        }
        assert!(rx.try_recv().is_err(), "4 of 5 matches");
        // a replay of a copy it already sent changes nothing
        w.observe("alias.example", 109, "did:plc:4", t1);
        w.observe("alias.example", 110, "did:plc:5", t1);
        w.observe("pds.example", 110, "did:plc:5", t1);
        let f = rx.try_recv().unwrap();
        assert_eq!((f.alias.as_str(), f.of.as_str(), f.matched), ("alias.example", "pds.example", 5));
        assert!(w.watching().is_empty());
        // and the pair rests
        w.suspect("alias.example", "pds.example", t1);
        assert!(w.watching().is_empty());
        w.suspect("alias.example", "pds.example", t1 + Duration::from_secs(61));
        assert_eq!(w.watching().len(), 1);
    }

    #[tokio::test]
    async fn an_event_one_side_lacks_ends_the_watch() {
        let (w, mut rx) = AliasWatch::new(cfg());
        let t0 = Instant::now();
        w.suspect("new.example", "old.example", t0);
        let t1 = t0 + Duration::from_secs(11);
        for s in 1..4 {
            w.observe("old.example", s, "did:plc:a", t1);
            w.observe("new.example", s, "did:plc:a", t1);
        }
        // the alias's own event: the other side passes its seq without it
        w.observe("new.example", 4, "did:plc:mine", t1);
        w.observe("old.example", 5, "did:plc:a", t1);
        w.observe("new.example", 5, "did:plc:a", t1);
        assert_eq!(w.watching().len(), 1, "inside the window");
        let t2 = t1 + Duration::from_secs(11);
        w.observe("old.example", 6, "did:plc:a", t2);
        assert!(w.watching().is_empty());
        assert!(rx.try_recv().is_err());
    }

    #[tokio::test]
    async fn a_side_behind_is_waited_for() {
        let (w, mut rx) = AliasWatch::new(cfg());
        let t0 = Instant::now();
        w.suspect("slow.example", "pds.example", t0);
        let t1 = t0 + Duration::from_secs(11);
        feed(&w, "pds.example", 10..15, t1);
        // a minute later the (throttled) alias catches up
        let t2 = t1 + Duration::from_secs(60);
        w.observe("pds.example", 15, "did:plc:1", t2);
        assert_eq!(w.watching().len(), 1);
        feed(&w, "slow.example", 10..15, t2);
        assert_eq!(rx.try_recv().unwrap().matched, 5);
    }

    #[tokio::test]
    async fn two_sequences_never_match() {
        let (w, mut rx) = AliasWatch::new(cfg());
        let t0 = Instant::now();
        w.suspect("a.example", "b.example", t0);
        let t1 = t0 + Duration::from_secs(11);
        feed(&w, "a.example", 1..20, t1);
        feed(&w, "b.example", 5_000..5_020, t1);
        w.observe("b.example", 5_020, "did:plc:x", t1 + Duration::from_secs(11));
        assert!(w.watching().is_empty());
        assert!(rx.try_recv().is_err());
    }

    #[tokio::test]
    async fn watches_are_capped_and_filtered() {
        let (w, _rx) = AliasWatch::new(cfg());
        let t = Instant::now();
        w.set_eligible(Arc::new(|h: &str, _: &str| h != "pinned.example"));
        w.suspect("pinned.example", "x.example", t);
        w.suspect("a.example", "x.example", t);
        w.suspect("x.example", "a.example", t);
        w.suspect("b.example", "x.example", t);
        w.suspect("c.example", "x.example", t);
        assert_eq!(
            w.watching(),
            vec![("a.example".to_string(), "x.example".to_string()), ("b.example".into(), "x.example".into())]
        );
    }

    /// A host answering describeServer with the service DID it's told.
    async fn describing(did: &str) -> (String, Arc<Mutex<String>>) {
        use axum::extract::State;
        let lis = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let host = format!("127.0.0.1:{}", lis.local_addr().unwrap().port());
        let did = Arc::new(Mutex::new(did.to_string()));
        let app = axum::Router::new()
            .route(
                "/xrpc/com.atproto.server.describeServer",
                axum::routing::get(|State(d): State<Arc<Mutex<String>>>| async move {
                    axum::Json(serde_json::json!({"did": *d.lock()}))
                }),
            )
            .with_state(did.clone());
        tokio::spawn(axum::serve(lis, app).into_future());
        (host, did)
    }

    #[tokio::test]
    async fn marks_one_service_and_clears_it_once_the_name_moves() {
        use crate::node::policy::tests::{add_host, setup};
        use crate::state::Tier;
        let (hooks, state) = setup().await;
        let (pds, _) = describing("did:web:pds.example").await;
        let (alias, alias_did) = describing("did:web:pds.example").await;
        let (other, _) = describing("did:web:other.example").await;
        for h in [&pds, &alias, &other] {
            add_host(&*hooks.hosts, h, Tier::Default).await;
        }
        hooks.load().await.unwrap();
        let mut up = UpstreamConfig::new(true);
        up.endpoint = Arc::new(|h: &Host| format!("http://{}", h.0));
        let found = |a: &str, of: &str| Found { alias: a.into(), of: of.into(), matched: 20 };

        // two services under one stream: not an alias
        assert!(!confirm(&hooks, &up, &found(&other, &pds)).await);
        assert_eq!(hooks.alias_of(&other), None);
        assert!(confirm(&hooks, &up, &found(&alias, &pds)).await);
        assert_eq!(hooks.alias_of(&alias), Some(pds.clone()));
        assert!(!hooks.limits(&alias).unwrap().connect);
        let k = crate::state::HostKey::of;
        assert!(state.same_host(k(&alias), k(&pds)) && !state.same_host(k(&other), k(&pds)));
        // found again, or the other way round: nothing more to mark
        assert!(!confirm(&hooks, &up, &found(&alias, &pds)).await);
        assert!(!confirm(&hooks, &up, &found(&pds, &alias)).await);

        // not due yet: nothing asked
        let now = crate::state::now_secs();
        *alias_did.lock() = "did:web:someone-else.example".into();
        recheck(&hooks, &up, now + 60).await;
        assert_eq!(hooks.alias_of(&alias), Some(pds.clone()));
        // a day on, the name answers for another service: it's its own host again
        recheck(&hooks, &up, now + RECHECK_AFTER.as_secs() as u32 + 1).await;
        assert_eq!(hooks.alias_of(&alias), None);
        assert!(hooks.limits(&alias).unwrap().connect);
        assert!(!state.same_host(k(&alias), k(&pds)));
    }

    /// One PDS with two accounts on one sequence, reachable as both
    /// 127.0.0.1 and localhost; one account's DID document names each. Its
    /// PLC directory is on the same port.
    pub(crate) mod pds {
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
            pub port: u16,
            /// (repo, the hostname its DID document names)
            pub repos: Mutex<Vec<(Repo, String)>>,
            frames: Mutex<Vec<(i64, Bytes)>>,
            head: watch::Sender<i64>,
            /// Each subscription: the Host it asked for, and its cursor.
            pub subs: Mutex<Vec<(String, Option<i64>)>>,
        }

        impl Pds {
            pub fn frames_len(&self) -> usize {
                self.frames.lock().len()
            }

            pub async fn start(repos: Vec<(Repo, &str)>) -> Arc<Pds> {
                let lis = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
                let port = lis.local_addr().unwrap().port();
                let p = Arc::new(Pds {
                    port,
                    repos: Mutex::new(repos.into_iter().map(|(r, h)| (r, h.to_string())).collect()),
                    frames: Mutex::new(Vec::new()),
                    head: watch::channel(0).0,
                    subs: Mutex::new(Vec::new()),
                });
                let app = axum::Router::new()
                    .route("/xrpc/com.atproto.sync.subscribeRepos", get(subscribe))
                    .route("/xrpc/com.atproto.server.describeServer", get(describe))
                    .route("/{did}", get(doc))
                    .with_state(p.clone());
                tokio::spawn(axum::serve(lis, app).into_future());
                p
            }

            /// The PDS's sequencer starts over (a wiped one): the next
            /// commit is seq 1, and a cursor past it gets FutureCursor.
            pub fn restart_sequence(&self) {
                self.frames.lock().clear();
                self.head.send_replace(0);
            }

            /// The next commit of account `i`.
            pub fn commit(&self, i: usize) {
                let mut frames = self.frames.lock();
                let seq = frames.last().map_or(0, |(s, _)| *s);
                let f = {
                    let mut rs = self.repos.lock();
                    let r = &mut rs[i].0;
                    r.seq = seq;
                    let ops = r.mixed_ops(2);
                    r.commit(&ops)
                };
                frames.push((seq + 1, f));
                self.head.send_replace(seq + 1);
            }
        }

        async fn subscribe(
            State(p): State<Arc<Pds>>,
            Query(q): Query<HashMap<String, String>>,
            headers: axum::http::HeaderMap,
            ws: WebSocketUpgrade,
        ) -> Response {
            let cursor = q.get("cursor").and_then(|c| c.parse::<i64>().ok());
            let host = headers.get("host").and_then(|h| h.to_str().ok()).unwrap_or_default().to_string();
            p.subs.lock().push((host, cursor));
            let mut at = cursor.unwrap_or(*p.head.borrow());
            let future = at > *p.head.borrow();
            ws.on_upgrade(move |mut sock| async move {
                if future {
                    let f = vlatproto::events::error_frame("FutureCursor", "cursor in the future");
                    let _ = sock.send(Message::Binary(f.into())).await;
                    return;
                }
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

        async fn describe() -> Response {
            axum::Json(serde_json::json!({"did": "did:web:pds.example", "availableUserDomains": []})).into_response()
        }

        async fn doc(State(p): State<Arc<Pds>>, Path(did): Path<String>) -> Response {
            let rs = p.repos.lock();
            let Some((r, host)) = rs.iter().find(|(r, _)| r.did == did) else {
                return axum::http::StatusCode::NOT_FOUND.into_response();
            };
            axum::Json(serde_json::json!({
                "id": did,
                "alsoKnownAs": ["at://someone.test"],
                "verificationMethod": [{
                    "id": format!("{did}#atproto"),
                    "type": "Multikey",
                    "controller": did,
                    "publicKeyMultibase": r.signer.multibase(),
                }],
                "service": [{
                    "id": "#atproto_pds",
                    "type": "AtprotoPersonalDataServer",
                    "serviceEndpoint": format!("http://{host}:{}", p.port),
                }],
            }))
            .into_response()
        }
    }

    pub(crate) async fn until(what: &str, secs: u64, mut f: impl FnMut() -> bool) {
        let t = Instant::now();
        while !f() {
            assert!(t.elapsed() < Duration::from_secs(secs), "waiting for {what}");
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
    }

    /// Both names stream every event; the DID documents split between them.
    /// The relay finds that they're one PDS, closes one name's socket, and
    /// takes both accounts' events from the other.
    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn one_pds_under_two_hostnames_is_read_once() {
        use crate::node::{Node, NodeConfig};
        use crate::verify::synth::{Curve, Repo, Signer};
        let (a, b) = ("did:plc:aliasaliasaliasaliasaaaa", "did:plc:aliasaliasaliasaliasbbbb");
        let pds = pds::Pds::start(vec![
            (Repo::new(a, Signer::new(Curve::K256, 11), 3), "127.0.0.1"),
            (Repo::new(b, Signer::new(Curve::K256, 12), 3), "localhost"),
        ])
        .await;
        let (ip, name) = (format!("127.0.0.1:{}", pds.port), format!("localhost:{}", pds.port));
        let store = vlsync_store::store::Store::memory(None);
        let mut cfg = NodeConfig::new(&format!("http://{ip}"));
        cfg.node_id = "n1".into();
        cfg.dev_mode = true;
        cfg.lanes = 2;
        cfg.ingest_threads = 2;
        cfg.serve_threads = 1;
        cfg.hosts = vec![format!("http://{ip}"), format!("http://{name}")];
        let live: Arc<dyn crate::policy::LiveNodes> = Arc::new(crate::policy::FixedNodes::new(1));
        cfg.policy = Some(crate::node::policy::PolicyEngine(crate::policy::Engine::new(store.clone(), "n1", live)));
        let mut q = crate::node::quorum::QuorumSetup::new(&format!("127.0.0.1:{}", crate::qlog::tests::free_port()));
        q.host_poll = Duration::from_millis(100);
        q.flush = Duration::from_millis(300);
        q.retain_horizon = None;
        q.aliases = AliasConfig {
            warmup: Duration::from_millis(500),
            window: Duration::from_secs(2),
            min_matches: 6,
            ..AliasConfig::default()
        };
        let node = Node::start(store, cfg, q).await.unwrap();
        let hooks = node.policy.clone().unwrap();
        let hosts = [Host(ip.clone()), Host(name.clone())];
        until("both sockets", 30, || hosts.iter().all(|h| node.manager.is_running(h))).await;

        let mut sent = 0;
        let t = Instant::now();
        let (alias, of) = loop {
            pds.commit(sent % 2);
            sent += 1;
            tokio::time::sleep(Duration::from_millis(100)).await;
            let found = hosts.iter().find_map(|h| Some((h.0.clone(), hooks.alias_of(&h.0)?)));
            if let Some(f) = found {
                break f;
            }
            assert!(t.elapsed() < Duration::from_secs(60), "never found the alias");
        };
        assert!(hosts.iter().any(|h| h.0 == of) && alias != of, "{alias} -> {of}");
        until("the alias's socket closed", 30, || !node.manager.is_running(&Host(alias.clone()))).await;
        assert!(node.manager.is_running(&Host(of.clone())));

        // every later event of both accounts comes from the one name, accepted
        let wrong = |n: &Node| {
            n.rejects.lock().values().map(|r| r.by_reason.get("wrong_host").copied().unwrap_or(0)).sum::<u64>()
        };
        let wrong_before = wrong(&node);
        let seq_before = pds.frames_len();
        for i in 0..6 {
            pds.commit(i % 2);
        }
        let want: Vec<i64> = (seq_before as i64 + 1..=seq_before as i64 + 6).collect();
        until("the later events", 30, || {
            let p = node.passed.lock();
            want.iter().all(|s| p.iter().any(|n| n.upstream_seq == *s && n.host.0 == of))
        })
        .await;
        assert_eq!(wrong(&node), wrong_before, "no wrong_host once one name is read");
        for did in [a, b] {
            let head = pds.repos.lock().iter().find(|(r, _)| r.did == did).unwrap().0.rev;
            assert_eq!(node.state.get(did).await.unwrap().unwrap().chain.map(|c| c.rev), Some(head), "{did}");
        }

        // an operator clears it: the name reconnects at the PDS's head, not
        // from the cursor it had when it was marked
        let subs_before = pds.subs.lock().len();
        hooks.admin.host_action(&alias, HostAction::Unalias { pin: true }, "op").await.unwrap();
        hooks.refresh_host(&alias).await.unwrap();
        until("the alias's socket again", 30, || node.manager.is_running(&Host(alias.clone()))).await;
        until("its subscription", 30, || pds.subs.lock()[subs_before..].iter().any(|(h, _)| *h == alias)).await;
        let sub = pds.subs.lock()[subs_before..].iter().find(|(h, _)| *h == alias).cloned().unwrap();
        assert_eq!(sub.1, None, "started at the head");
        let rec = hooks.hosts.get_host(&alias).await.unwrap().unwrap();
        let last = crate::policy::admin::PolicyAdmin::host_actions(&rec).pop().unwrap();
        assert!(last.reason.as_deref().is_some_and(|r| r.starts_with("cursor reset to head")), "{:?}", last.reason);
        // and it reads on from there
        pds.commit(0);
        let s = pds.frames_len() as i64;
        until("the next event from both names", 30, || {
            let p = node.passed.lock();
            p.iter().any(|n| n.upstream_seq == s)
        })
        .await;
    }
}
