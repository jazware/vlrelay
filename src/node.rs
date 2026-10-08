//! One relay node: upstream sockets in, checked events to the quorum log's
//! leader, and the firehose out (docs/quorum.md, "The relay on the log").
//!
//! The pipeline, per upstream frame:
//!
//! 1. The upstream manager's fair queue hands the dispatcher a frame from a
//!    host the leader gave this node. It takes the cheap parse
//!    (`event::route`: kind and DID), notes the frame as pending for its
//!    host's cursor, and queues it on one of the lanes, picked by a hash of
//!    the DID.
//! 2. The lane runs the host owner's stage: the strict parse and the sync 1.1
//!    checks (`verify`), with the signing key from the DID document cache. A
//!    signature that fails against a cached key refreshes it and tries once
//!    more.
//! 3. The lane hands the checked event to the [`DidOwner`]: on the quorum
//!    log, `quorum::QuorumOwner`, which sends it to the leader. The leader
//!    applies it to the account's record (host authority, account status,
//!    the chain) and appends it, and the outcome comes back once a quorum
//!    holds it.
//! 4. Only then does the host's cursor move past it. Every node emits the
//!    entry once it has committed.
//!
//! Order: a lane works one event at a time, and every event of a DID goes
//! to the same lane, so a DID's events reach the leader in the order its
//! host sent them. Lanes run in parallel on their own runtime, so the CPU
//! of verification spreads over its threads. A host's events finish out of
//! order across lanes, so its cursor only moves past a seq once every
//! earlier one is done ([`acks::Tracker`]).
//!
//! Replays keep that order. Each host socket is an epoch. When an event
//! gives up, its socket is fenced (`forward::Fence`): nothing more from it
//! reaches the leader, the host is kicked, and the new socket replays
//! everything past the cursor, in order. The ack tracker only counts the
//! newest socket's copies.
//!
//! Memory: every frame read carries an `upstream::flow` permit until it's
//! done, and a host (or the node) at its in-flight cap isn't read.

pub mod acks;
pub mod adapters;
pub mod admin;
pub mod forward;
pub mod lag;
pub mod metrics;
pub mod patience;
pub mod policy;
pub mod quorum;

use crate::event::{self, Kind, SeqSpan};
use crate::identity::{HttpFetch, Identity, IdentityCache};
use crate::serve::{Serve, ServeConfig};
use crate::state::{self, StateStore};
use crate::types::{Host, UpstreamFrame};
use crate::upstream::{self, Manager, Tier};
use crate::verify::{self, Reject, SigningKey, Verified};
use adapters::VerifyChain;
use bytes::Bytes;
use futures::FutureExt;
use metrics::{Dash, HostRejects, RejectNote, Ttf};
use parking_lot::Mutex;
use std::collections::HashMap;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};
use std::time::{Duration, Instant};
use tokio::sync::{mpsc, oneshot};
use vlsync_store::store::Store;

pub type State = StateStore<VerifyChain>;

/// How often the node looks for read-lag cases it can close.
const LAG_SWEEP_EVERY: Duration = Duration::from_secs(60);

#[derive(Clone, Debug)]
pub struct NodeConfig {
    /// The member's name in the quorum log.
    pub node_id: String,
    pub dev_mode: bool,
    pub plc_url: String,
    /// The firehose's ring of recent events, in memory (None: the default);
    /// older cursors are served from the node's log and the bucket.
    pub ring_bytes: Option<usize>,
    /// None: the firehose's default.
    pub max_lag_bytes: Option<usize>,
    /// Pipeline lanes (each a task; a DID always maps to the same one).
    pub lanes: usize,
    /// Threads of the runtime the lanes run on.
    pub ingest_threads: usize,
    /// DID document lookups started ahead of the lanes at once
    /// (`IdentityCache::prefetch`); 0 leaves them to the lanes.
    pub lookup_prefetch: usize,
    /// Threads serving subscribeRepos.
    pub serve_threads: usize,
    /// Upstreams from the command line: a URL (`http://` means plain
    /// `ws://`, dev mode only) or a bare hostname.
    pub hosts: Vec<String>,
    pub identity: crate::identity::Options,
    pub upstream_limits: upstream::Limits,
    /// In-flight caps on what's read from upstreams (`upstream::flow`).
    pub inflight: upstream::flow::FlowLimits,
    /// SO_RCVBUF of each upstream socket; 0: the kernel's autotuning.
    pub upstream_rcvbuf_bytes: usize,
    /// Enforced when set (`node::policy`); without it hosts run at their
    /// tier's default limits and nothing is counted.
    pub policy: Option<policy::PolicyEngine>,
    /// The tier a `--host` upstream starts at the first time it's seen.
    pub cli_host_tier: Tier,
    /// When a host's read lag opens a case, and when the case closes.
    pub lag_cases: lag::LagCaseConfig,
    /// How far back a host's event times count on its own timeline
    /// (`upstream::clock`).
    pub event_horizon: Duration,
}

impl NodeConfig {
    pub fn new(plc_url: &str) -> NodeConfig {
        let cores = std::thread::available_parallelism().map_or(4, |n| n.get());
        NodeConfig {
            node_id: "relay".into(),
            dev_mode: false,
            plc_url: plc_url.into(),
            ring_bytes: None,
            max_lag_bytes: None,
            lanes: 64,
            ingest_threads: cores.clamp(2, 16),
            lookup_prefetch: 256,
            serve_threads: 4,
            hosts: Vec::new(),
            identity: crate::identity::Options::default(),
            upstream_limits: upstream::Limits::default(),
            inflight: upstream::flow::FlowLimits::default(),
            upstream_rcvbuf_bytes: 0,
            policy: None,
            cli_host_tier: Tier::Trusted,
            lag_cases: lag::LagCaseConfig::default(),
            event_horizon: upstream::EVENT_HORIZON,
        }
    }

    pub fn serve_config(&self) -> ServeConfig {
        let d = ServeConfig::default();
        ServeConfig {
            threads: self.serve_threads,
            max_lag_bytes: self.max_lag_bytes.unwrap_or(d.max_lag_bytes),
            ring_bytes: self.ring_bytes.unwrap_or(d.ring_bytes),
            ..d
        }
    }
}

/// An event that passed the host owner's checks, on its way to the DID owner.
pub struct Checked {
    pub did: String,
    pub host: Host,
    pub upstream_seq: i64,
    pub kind: CheckedKind,
    /// The frame as received, and where its seq is.
    pub frame: Bytes,
    pub span: SeqSpan,
    pub received: Instant,
    /// The first copy of this upstream seq (not a reconnect's replay).
    pub first_sighting: bool,
    /// The host socket it came on (None for an event the relay made).
    pub fence: Option<forward::Fence>,
}

#[derive(Clone)]
pub enum CheckedKind {
    Commit(Verified),
    Sync(Verified),
    Identity,
    Account { active: bool, status: Option<String> },
}

impl CheckedKind {
    fn label(&self) -> &'static str {
        match self {
            CheckedKind::Commit(_) => "commit",
            CheckedKind::Sync(_) => "sync",
            CheckedKind::Identity => "identity",
            CheckedKind::Account { .. } => "account",
        }
    }
}

/// Why an event was dropped: a stable reason (metrics, dashboard) and the
/// specifics (logs, the host's recent rejects).
#[derive(Clone, Debug)]
pub struct Rejection {
    pub reason: &'static str,
    pub detail: String,
}

impl Rejection {
    fn verify(r: Reject) -> Rejection {
        Rejection { reason: r.reason(), detail: r.to_string() }
    }
}

pub enum Submitted {
    Rejected(Rejection),
    /// On its way to the leader: resolves with its outcome once it has
    /// committed, or as a duplicate or rejected.
    Forwarded(oneshot::Receiver<Result<forward::Outcome, forward::ForwardError>>),
}

/// The DID owner's side of the pipeline. Calls for one DID come in the
/// order its host sent the events, and each is on its way before the call
/// returns, so a DID's events stay in order.
#[async_trait::async_trait]
pub trait DidOwner: Send + Sync + 'static {
    async fn submit(&self, ev: Checked) -> Submitted;
}

fn state_rejection(e: &state::Reject) -> Rejection {
    use state::Reject as R;
    let reason = match e {
        R::Stale { .. } => "stale",
        R::WrongHost { .. } => "wrong_host",
        R::Inactive(_) => "inactive",
        R::Desynchronized => "desynchronized",
        R::Chain(state::ChainError::RevNotNewer { .. }) => "rev_not_newer",
        R::Chain(state::ChainError::PrevDataMismatch) => "prev_data_mismatch",
        R::Chain(_) => "chain",
        R::RateLimited { .. } => "rate_limited",
        R::NewAccountDeferred => "new_account_deferred",
        R::NoIdentity => "no_identity",
        R::BadCid => "bad_cid",
        R::NotOwner(_) => "not_owner",
        R::Identity(_) | R::IdentityGaveUp(_) => "identity_unavailable",
        R::Store(_) => "store",
    };
    Rejection { reason, detail: e.to_string() }
}

/// A lane's queue: unbounded, since every frame holds its upstream
/// in-flight permit until it's done and the caps bound them. A bounded lane
/// that filled behind one slow event blocked the one dispatcher, and every
/// other lane with it.
struct Lane {
    tx: mpsc::UnboundedSender<Job>,
    queued: AtomicUsize,
}

/// The depth a lane counts as full at, for the lag cases' pressure.
const LANE_FULL: usize = 256;

struct Job {
    frame: UpstreamFrame,
    received: Instant,
    first: bool,
    fence: forward::Fence,
}

pub struct Node {
    pub cfg: NodeConfig,
    pub store: Store,
    pub manager: Arc<Manager>,
    pub crawler: Arc<upstream::Crawler>,
    pub state: Arc<State>,
    pub identity: Arc<IdentityCache<HttpFetch>>,
    pub serve: Arc<Serve>,
    pub owner: Arc<dyn DidOwner>,
    pub acks: acks::Tracker,
    pub ttf: Arc<Ttf>,
    pub dash: Mutex<Dash>,
    pub rejects: Mutex<HashMap<Host, HostRejects>>,
    /// The last events this node read that went out, for the admin tail.
    pub passed: Mutex<std::collections::VecDeque<metrics::PassedNote>>,
    pub policy: Option<Arc<policy::PolicyHooks>>,
    /// Per host: sockets below this epoch are fenced (`forward::Fence`).
    fences: Mutex<crate::types::FastMap<Host, Arc<AtomicU64>>>,
    lanes: Vec<Lane>,
    prefetch: Arc<tokio::sync::Semaphore>,
    pub ingest: tokio::runtime::Handle,
    pub started_ms: i64,
    /// The quorum log half (docs/quorum.md): the log node, its client, the
    /// leader's hooks and the host table.
    pub quorum: Arc<quorum::Glue>,
    lag: Mutex<lag::LagWatch>,
}

/// Scheme for each `--host` given as a URL, so a dev upstream on
/// `http://127.0.0.1:2984` is dialed with `ws://`.
fn endpoint_fn(dev_mode: bool, explicit: HashMap<Host, String>) -> upstream::EndpointFn {
    Arc::new(move |h: &Host| {
        if let Some(base) = explicit.get(h) {
            return base.clone();
        }
        let name = h.0.rsplit_once(':').map_or(h.0.as_str(), |(n, _)| n);
        let local = name == "localhost" || name.parse::<std::net::IpAddr>().is_ok() || name.starts_with('[');
        if dev_mode && local { format!("http://{}", h.0) } else { format!("https://{}", h.0) }
    })
}

/// The `--host` upstreams: each normalized, and its scheme kept.
fn cli_hosts(cfg: &NodeConfig) -> anyhow::Result<(HashMap<Host, String>, Vec<Host>)> {
    let mut explicit = HashMap::new();
    let mut cli_hosts = Vec::new();
    for h in &cfg.hosts {
        let host = upstream::normalize_hostname(h, cfg.dev_mode).map_err(|e| anyhow::anyhow!("--host {h}: {e}"))?;
        let base = if h.starts_with("http://") || h.starts_with("ws://") {
            anyhow::ensure!(cfg.dev_mode, "--host {h}: plain http needs --dev-mode");
            format!("http://{}", host.0)
        } else {
            format!("https://{}", host.0)
        };
        explicit.insert(host.clone(), base);
        cli_hosts.push(host);
    }
    Ok((explicit, cli_hosts))
}

impl Node {
    /// The pipeline around the parts `start` built:
    /// the ingest runtime, the lanes, the dispatcher and the background
    /// loops.
    #[allow(clippy::too_many_arguments)]
    fn assemble(
        cfg: NodeConfig,
        store: Store,
        manager: Arc<Manager>,
        crawler: Arc<upstream::Crawler>,
        state: Arc<State>,
        identity: Arc<IdentityCache<HttpFetch>>,
        srv: Arc<Serve>,
        owner: Arc<dyn DidOwner>,
        hooks: Option<Arc<policy::PolicyHooks>>,
        quorum: Arc<quorum::Glue>,
        rx: mpsc::Receiver<UpstreamFrame>,
    ) -> anyhow::Result<Arc<Node>> {
        let ingest = tokio::runtime::Builder::new_multi_thread()
            .worker_threads(cfg.ingest_threads)
            .thread_name("ingest")
            .enable_all()
            .build()?;
        let ingest_handle = ingest.handle().clone();
        // lives as long as the process
        std::mem::forget(ingest);

        let mut lane_tx = Vec::new();
        let mut lane_rx = Vec::new();
        for _ in 0..cfg.lanes.max(1) {
            let (tx, rx) = mpsc::unbounded_channel();
            lane_tx.push(Lane { tx, queued: AtomicUsize::new(0) });
            lane_rx.push(rx);
        }
        let node = Arc::new(Node {
            cfg: cfg.clone(),
            store,
            manager: manager.clone(),
            crawler,
            state,
            identity,
            serve: srv,
            owner,
            acks: acks::Tracker::default(),
            ttf: Arc::new(Ttf::default()),
            dash: Mutex::new(Dash::default()),
            rejects: Mutex::new(HashMap::new()),
            passed: Mutex::new(Default::default()),
            policy: hooks.clone(),
            fences: Mutex::new(Default::default()),
            lanes: lane_tx,
            prefetch: Arc::new(tokio::sync::Semaphore::new(cfg.lookup_prefetch)),
            ingest: ingest_handle.clone(),
            started_ms: upstream::host::now_ms() as i64,
            quorum,
            lag: Mutex::new(lag::LagWatch::new(cfg.lag_cases)),
        });
        let weak = Arc::downgrade(&node);
        manager.on_connect(Arc::new(move |host: &Host, epoch, cursor, restarted| {
            if let Some(n) = weak.upgrade() {
                n.acks.connected(host, epoch, cursor, restarted);
            }
        }));
        for (i, rx) in lane_rx.into_iter().enumerate() {
            ingest_handle.spawn(node.clone().lane(i, rx));
        }
        ingest_handle.spawn(node.clone().dispatch(rx));
        tokio::spawn(node.clone().tap());
        tokio::spawn(node.clone().sampler());
        tokio::spawn(node.clone().lag_sweeper());

        Ok(node)
    }

    async fn dispatch(self: Arc<Self>, mut rx: mpsc::Receiver<UpstreamFrame>) {
        let max = event::Limits::default().max_frame_bytes;
        while let Some(f) = rx.recv().await {
            let received = Instant::now();
            let r = match event::route(&f.frame, max) {
                Ok(r) => r,
                Err(e) => {
                    metrics::EVENTS_IN.with_label_values(&["malformed"]).inc();
                    self.acks.begin_at(&f.host, f.upstream_seq, f.epoch, received);
                    self.reject(&f.host, "", f.upstream_seq, Rejection::verify(e));
                    self.finish(&f.host, f.upstream_seq, f.epoch, None);
                    continue;
                }
            };
            metrics::IN_BY_KIND.inc(r.kind.as_str());
            let first = self.acks.begin_at(&f.host, f.upstream_seq, f.epoch, received);
            let did = match (r.kind, r.did) {
                (Kind::Commit | Kind::Sync | Kind::Identity | Kind::Account, Some(d)) => d,
                (k, _) => {
                    metrics::EVENTS_SKIPPED.with_label_values(&[k.as_str()]).inc();
                    self.finish(&f.host, f.upstream_seq, f.epoch, None);
                    continue;
                }
            };
            if matches!(r.kind, Kind::Commit | Kind::Sync) && self.cfg.lookup_prefetch > 0 {
                self.identity.prefetch(did, &self.prefetch, &self.ingest);
            }
            let lane = &self.lanes[lane_of(did, self.lanes.len())];
            let fence = self.fence(&f.host, f.epoch);
            metrics::LANE_QUEUED.inc();
            lane.queued.fetch_add(1, Ordering::Relaxed);
            if lane.tx.send(Job { frame: f, received, first, fence }).is_err() {
                return;
            }
        }
    }

    fn finish(&self, host: &Host, seq: i64, epoch: u64, ordinal: Option<u64>) {
        if let Some(acked) = self.acks.finish(host, seq, epoch, ordinal) {
            self.manager.ack(host, acked);
        }
    }

    fn fence(&self, host: &Host, epoch: u64) -> forward::Fence {
        let below = self.fences.lock().entry(host.clone()).or_default().clone();
        forward::Fence::new(epoch, below)
    }

    /// Stops what's left in the pipeline from `host`'s current and earlier
    /// sockets: none of it is forwarded, and none of it moves the cursor.
    pub fn fence_host(&self, host: &Host) {
        if let Some(e) = self.manager.registry().get(host) {
            self.fence(host, e.epoch()).trip();
        }
    }

    async fn lane(self: Arc<Self>, i: usize, mut rx: mpsc::UnboundedReceiver<Job>) {
        use futures::StreamExt;
        use futures::stream::FuturesUnordered;
        // the leader's answers to this lane's events, polled between jobs
        // instead of a task per event
        let mut acks: FuturesUnordered<futures::future::BoxFuture<'static, ()>> = FuturesUnordered::new();
        loop {
            let mut job = tokio::select! {
                biased;
                Some(()) = acks.next(), if !acks.is_empty() => continue,
                job = rx.recv() => match job {
                    Some(j) => j,
                    None => break,
                },
            };
            metrics::LANE_QUEUED.dec();
            self.lanes[i].queued.fetch_sub(1, Ordering::Relaxed);
            // held until the event is done, which bounds what's in flight
            let permit = job.frame.permit.take();
            let host = job.frame.host.clone();
            let useq = job.frame.upstream_seq;
            let epoch = job.frame.epoch;
            if !job.fence.live() {
                metrics::EVENTS_FENCED.inc();
                self.acks.fail(&host, useq, epoch);
                continue;
            }
            let checked = match self.check(job).await {
                Ok(Some(c)) => c,
                Ok(None) => {
                    self.finish(&host, useq, epoch, None);
                    continue;
                }
                Err((did, r)) => {
                    self.reject(&host, &did, useq, r);
                    self.finish(&host, useq, epoch, None);
                    continue;
                }
            };
            let did = checked.did.clone();
            let kind = checked.kind.label();
            match self.owner.submit(checked).await {
                Submitted::Rejected(r) => {
                    self.reject(&host, &did, useq, r);
                    self.finish(&host, useq, epoch, None);
                }
                Submitted::Forwarded(rx) => {
                    let node = self.clone();
                    acks.push(
                        async move {
                            let _permit = permit;
                            node.forwarded(rx, host, did, useq, epoch, kind).await;
                        }
                        .boxed(),
                    );
                }
            }
        }
        while acks.next().await.is_some() {}
    }

    /// The host owner's stage: strict parse and the stateless checks.
    async fn check(&self, job: Job) -> Result<Option<Checked>, (String, Rejection)> {
        let Job { frame: f, received, first, fence } = job;
        let t0 = Instant::now();
        let ev = cpu(f.frame.len(), || event::parse(f.frame.clone(), &event::Limits::default()))
            .map_err(|r| (String::new(), Rejection::verify(r)))?;
        let parse_us = t0.elapsed();
        let checked = |did: String, kind, frame, span| Checked {
            did,
            host: f.host.clone(),
            upstream_seq: f.upstream_seq,
            kind,
            frame,
            span,
            received,
            first_sighting: first,
            fence: Some(fence.clone()),
        };
        let opts = verify::Options::default();
        let out = match ev {
            event::Event::Commit(c) => {
                let v = self
                    .verified(&c.repo, &f.host, f.frame.len(), |k| verify::verify_commit_with(&c, k, &opts))
                    .await
                    .map_err(|r| (c.repo.clone(), r))?;
                Some(checked(c.repo.clone(), CheckedKind::Commit(v), c.frame.clone(), c.seq_span))
            }
            event::Event::Sync(s) => {
                let v = self
                    .verified(&s.did, &f.host, f.frame.len(), |k| verify::verify_sync_with(&s, k, &opts))
                    .await
                    .map_err(|r| (s.did.clone(), r))?;
                Some(checked(s.did.clone(), CheckedKind::Sync(v), s.frame.clone(), s.seq_span))
            }
            event::Event::Identity(i) => {
                if let Some(p) = &self.policy
                    && !p.take_identity_event(&f.host.0, f.clock_ms)
                {
                    let detail = "over the host's identityEventsPerHour".to_string();
                    return Err((i.did.clone(), Rejection { reason: policy::IDENTITY_RATE, detail }));
                }
                // the DID owner refreshes the document (within its host's
                // budget); dropping our copy here would spend the shared
                // budget on every #identity
                Some(checked(i.did.clone(), CheckedKind::Identity, i.frame.clone(), i.seq_span))
            }
            event::Event::Account(a) => Some(checked(
                a.did.clone(),
                CheckedKind::Account { active: a.active, status: a.status.clone() },
                a.frame.clone(),
                a.seq_span,
            )),
            _ => None,
        };
        metrics::PARSE.busy(parse_us);
        Ok(out)
    }

    /// Runs `check` against the DID's signing key, refreshing the key once
    /// when it may be stale and the host's lookup budget allows: a forged
    /// signature would otherwise buy a fresh fetch.
    async fn verified(
        &self,
        did: &str,
        host: &Host,
        len: usize,
        check: impl Fn(&SigningKey) -> Result<Verified, Reject>,
    ) -> Result<Verified, Rejection> {
        let mut fresh = false;
        loop {
            let t0 = Instant::now();
            let id = self.lookup(did, fresh).await?;
            let t1 = Instant::now();
            metrics::IDENTITY.wall(t1 - t0);
            let r = match &id.signing_key {
                Some(k) => cpu(len, || check(k)),
                None => Err(Reject::NoSigningKey),
            };
            metrics::VERIFY.busy(t1.elapsed());
            match r {
                Err(e) if e.may_be_stale_key() && !fresh && self.refresh_allowed(host) => fresh = true,
                r => return r.map_err(Rejection::verify),
            }
        }
    }

    fn refresh_allowed(&self, host: &Host) -> bool {
        self.policy.as_ref().is_none_or(|p| p.take_forced_lookup(&host.0))
    }

    async fn lookup(&self, did: &str, fresh: bool) -> Result<Arc<Identity>, Rejection> {
        // PLC trouble is waited out, holding this lane (and so the host's
        // reads through the in-flight caps), except for a DID that already
        // used up its patience (node/patience.rs)
        self.quorum.patience.lookup(did, || self.identity.lookup_paced(did, fresh)).await
    }

    fn reject(&self, host: &Host, did: &str, useq: i64, r: Rejection) {
        metrics::EVENTS_REJECTED.with_label_values(&[r.reason]).inc();
        if let Some(p) = &self.policy {
            p.on_reject(&host.0, did, r.reason, &r.detail);
        }
        tracing::debug!(host = %host.0, did, useq, reason = r.reason, "rejected: {}", r.detail);
        let mut m = self.rejects.lock();
        let h = m.entry(host.clone()).or_default();
        *h.by_reason.entry(r.reason).or_default() += 1;
        h.total += 1;
        h.recent.push_back(RejectNote {
            at_ms: upstream::host::now_ms() as i64,
            did: did.to_string(),
            reason: r.reason,
            upstream_seq: useq,
            detail: r.detail,
        });
        if h.recent.len() > 50 {
            h.recent.pop_front();
        }
    }

    /// Times each emitted event against its arrival (time to firehose).
    async fn tap(self: Arc<Self>) {
        // the firehose is made at the node's first emission
        let fh = loop {
            if let Some(f) = self.serve.firehose() {
                break f.clone();
            }
            tokio::time::sleep(Duration::from_millis(100)).await;
        };
        let mut head = fh.subscribe();
        let mut last = fh.last_emitted.load(std::sync::atomic::Ordering::Acquire);
        while head.changed().await.is_ok() {
            let now = Instant::now();
            let (batches, _) = fh.from_ring(last);
            for b in batches {
                self.ttf.emitted_batch((0..b.events.len()).map(|i| b.key(i)), now);
                metrics::EVENTS_OUT.inc_by(b.events.len() as u64);
                last = b.last;
            }
        }
    }

    /// How full this node's in-flight caps and busiest lane are.
    fn pressure(&self) -> lag::Pressure {
        let deepest = self.lanes.iter().map(|l| l.queued.load(Ordering::Relaxed)).max().unwrap_or(0);
        let lanes = (deepest as f64 / LANE_FULL as f64).min(1.0);
        lag::Pressure { inflight: self.manager.inflight_fill(), lanes }
    }

    /// Opens (or adds to) a read-lag case for each host [`lag::LagWatch`]
    /// says fell behind on its own. A host held to its limits that long is
    /// losing ground, and the PDS will cut the socket with `ConsumerTooSlow`
    /// once it's past its own outbox.
    fn lag_cases(&self, hosts: &[upstream::HostView]) {
        let Some(p) = self.policy.clone() else { return };
        let now = upstream::host::now_ms() as i64;
        let pressure = self.pressure();
        let samples: Vec<lag::Sample<'_>> = hosts
            .iter()
            .map(|h| lag::Sample {
                host: h.record.hostname.as_str(),
                lag_ms: h.host_lag_ms,
                held_at_ms: h.backpressure_at_ms,
            })
            .collect();
        let (trips, threshold) = {
            let mut w = self.lag.lock();
            (w.observe(now, pressure, &samples), w.config().threshold.as_secs_f64())
        };
        for (host, trip) in trips {
            let (node, engine) = (self.cfg.node_id.clone(), p.engine.clone());
            tokio::spawn(async move {
                let observed = trip.lag_ms as f64 / 1000.0;
                let signals = [("inflightFill", trip.pressure.inflight), ("laneFill", trip.pressure.lanes)]
                    .into_iter()
                    .map(|(k, v)| (k.to_string(), (v * 100.0).round() / 100.0))
                    .collect();
                let o = crate::policy::cases::CaseOpen {
                    kind: "read-lag".into(),
                    host: host.clone(),
                    did: None,
                    severity: crate::admin::Severity::High,
                    summary: format!("{host}: reader {} min behind the host's stream", trip.lag_ms / 60_000),
                    observed,
                    threshold,
                    auto_action: None,
                    evidence: crate::policy::cases::Evidence {
                        at_ms: upstream::host::now_ms() as i64,
                        observed,
                        threshold,
                        window_secs: 0,
                        node,
                        detail: Some("raise the host's limits or tier, or it will fall out of the PDS's outbox".into()),
                        signals,
                    },
                };
                if let Err(e) = engine.cases.open_or_update(o).await {
                    tracing::warn!(host, "read-lag case not opened: {e:#}");
                }
            });
        }
    }

    /// Closes the open read-lag cases of hosts this node reads once their
    /// lag has recovered ([`lag`]), cases from before this process
    /// included.
    async fn lag_sweeper(self: Arc<Self>) {
        let Some(p) = self.policy.clone() else { return };
        let mut tick = tokio::time::interval(LAG_SWEEP_EVERY);
        tick.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
        loop {
            tick.tick().await;
            if let Err(e) = self.sweep_lag_cases(&p.engine).await {
                tracing::warn!("read-lag sweep: {e:#}");
            }
        }
    }

    /// One pass of [`Self::lag_sweeper`]; returns the cases it closed.
    pub async fn sweep_lag_cases(&self, engine: &crate::policy::Engine) -> anyhow::Result<Vec<u64>> {
        let now = upstream::host::now_ms() as i64;
        let open: Vec<_> = engine
            .cases
            .list(None)
            .await?
            .into_iter()
            .filter(|c| c.is_open() && c.kind == "read-lag")
            .filter(|c| self.manager.is_running(&Host(c.host.clone())) && self.lag.lock().recovered(&c.host, now))
            .collect();
        let by = crate::admin::Actor::Service("relay".into()).label();
        let mut closed = Vec::new();
        for c in open {
            let Some(done) = engine.cases.resolve_open(c.id, lag::RESOLVED_NOTE, &by).await? else { continue };
            tracing::info!(host = %done.host, case = done.id, "read-lag case resolved: lag recovered");
            if let Some(f) = self.quorum.hooks.changes.get() {
                let hint = serde_json::json!({ "status": done.status });
                f.touch(crate::admin::changes::ChangeKind::Case, done.id.to_string(), Some(hint), true);
            }
            closed.push(done.id);
        }
        Ok(closed)
    }

    /// One sample per second for the dashboard and the gauges.
    async fn sampler(self: Arc<Self>) {
        let mut tick = tokio::time::interval(Duration::from_secs(1));
        tick.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
        let mut prev_in = 0u64;
        let mut prev_out = 0u64;
        let mut prev_bytes_in = 0u64;
        let mut prev_bytes_out = 0u64;
        let mut prev_rejects: HashMap<&'static str, u64> = HashMap::new();
        let mut prev_lat = (0u64, 0u64);
        let mut first = true;
        loop {
            tick.tick().await;
            let t = upstream::host::now_ms() as i64 / 1000;
            let hosts = self.manager.hosts();
            let ev_in: u64 = hosts.iter().map(|h| h.frames).sum();
            let bytes_in: u64 = hosts.iter().map(|h| h.bytes).sum();
            let ev_out = metrics::EVENTS_OUT.get();
            let bytes_out = vlsync_firehose::metrics::FIREHOSE_SENT_BYTES.get();
            let mut rejects_now: HashMap<&'static str, u64> = HashMap::new();
            let host_rejects: HashMap<Host, u64> = {
                let r = self.rejects.lock();
                for h in r.values() {
                    for (k, v) in &h.by_reason {
                        *rejects_now.entry(k).or_default() += v;
                    }
                }
                r.iter().map(|(h, x)| (h.clone(), x.total)).collect()
            };
            let (p50, p99) = self.ttf.roll();
            let lat = quorum::latency_totals(&self);
            let lag_ms = if lat.1 > prev_lat.1 {
                (lat.0 - prev_lat.0) as f64 / (lat.1 - prev_lat.1) as f64 / 1000.0
            } else {
                0.0
            };
            prev_lat = lat;
            metrics::DURABLE_LAG.set(lag_ms as i64);
            let consumers = vlsync_firehose::metrics::FIREHOSE_SUBSCRIBERS.get();
            metrics::CONSUMERS.set(consumers);
            let st = &self.identity.stats;
            for (k, v) in [
                ("hit", &st.hits),
                ("seeded", &st.seeded),
                ("fetched", &st.fetches),
                ("prefetched", &st.prefetched),
                ("prefetch_full", &st.prefetch_full),
            ] {
                metrics::IDENTITY_LOOKUPS.with_label_values(&[k]).set(v.load(Ordering::Relaxed) as i64);
            }
            metrics::IDENTITY_CACHE.set(self.identity.len() as i64);
            let lags: Vec<i64> = hosts.iter().filter_map(|h| h.read_lag_ms).collect();
            metrics::HOST_READ_LAG_MAX.set(lags.iter().map(|l| l / 1000).max().unwrap_or(0));
            metrics::HOSTS_LAGGING.set(lags.iter().filter(|&&l| l > 60_000).count() as i64);
            self.lag_cases(&hosts);
            let mut by_status: HashMap<&'static str, i64> = HashMap::new();
            for h in &hosts {
                *by_status.entry(admin::host_status_label(h)).or_default() += 1;
            }
            for s in ["connected", "idle", "backoff", "throttled", "backpressure", "suspended", "banned"] {
                metrics::HOSTS.with_label_values(&[s]).set(by_status.get(s).copied().unwrap_or(0));
            }
            upstream::flow::HOST_INFLIGHT_MAX.set(hosts.iter().map(|h| h.inflight_events).max().unwrap_or(0) as i64);
            let mut d = self.dash.lock();
            for h in &hosts {
                let host = Host(h.record.hostname.clone());
                let rj = host_rejects.get(&host).copied().unwrap_or(0);
                d.hosts.entry(host).or_default().push(t, h.frames, rj);
            }
            if !first {
                let rejects = rejects_now
                    .iter()
                    .map(|(k, v)| (*k, v.saturating_sub(prev_rejects.get(k).copied().unwrap_or(0)) as f64))
                    .collect();
                d.history.push_back(metrics::Sample {
                    t,
                    events_in: ev_in.saturating_sub(prev_in) as f64,
                    events_out: ev_out.saturating_sub(prev_out) as f64,
                    bytes_in: bytes_in.saturating_sub(prev_bytes_in) as f64,
                    bytes_out: bytes_out.saturating_sub(prev_bytes_out) as f64,
                    ttf_p50_ms: p50,
                    ttf_p99_ms: p99,
                    durable_lag_ms: lag_ms,
                    rejects,
                });
                while d.history.len() > metrics::HISTORY {
                    d.history.pop_front();
                }
            }
            first = false;
            prev_in = ev_in;
            prev_out = ev_out;
            prev_bytes_in = bytes_in;
            prev_bytes_out = bytes_out;
            prev_rejects = rejects_now;
        }
    }

    /// Stops reading upstreams. Their cursors are the log's: the hosts'
    /// next owners resume from what's committed.
    pub async fn shutdown(&self) -> anyhow::Result<()> {
        self.manager.shutdown().await
    }
}

fn lane_of(did: &str, n: usize) -> usize {
    let mut h: u64 = 0xcbf29ce484222325;
    for b in did.bytes() {
        h = (h ^ b as u64).wrapping_mul(0x100000001b3);
    }
    (h % n as u64) as usize
}

/// Big frames (a 2 MB CAR) take milliseconds to hash: off the lane's worker
/// so the runtime keeps polling the other lanes.
fn cpu<T>(len: usize, f: impl FnOnce() -> T) -> T {
    if len > 256 << 10 { tokio::task::block_in_place(f) } else { f() }
}
