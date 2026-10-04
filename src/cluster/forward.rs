//! Host owner to DID owner: verified events go to the node that owns their
//! DID's shard, in batches, and each comes back with its result once the DID
//! owner's log made it durable (or decided it was a duplicate or a reject).
//!
//! Order per DID is what matters (the DID owner checks each commit against
//! the one before), so events go through lanes keyed by the DID's slot. A
//! lane has up to `ForwardConfig::batches` batches in flight, no DID in two
//! of them at once: an event whose DID is in flight waits, in order, until
//! that batch resolves (its failed events retried in order first). So one
//! slow owner holds back only its own DIDs, not every DID sharing a lane
//! with them (one slow bucket path on one core stalled every node's
//! forwards, docs/chaos.md).
//! A batch splits by owner: the part for this node calls the stage directly
//! (no network), the rest goes over peer mTLS as one HTTP/2 POST per owner.
//!
//! An owner that answers `NotOwner` or can't be reached is looked up again
//! after a short backoff, so a takeover costs the time until the new owner
//! has opened the shard, as with vlpds's forwarded writes.
//!
//! Giving up on an event breaks its DID's order: the host owner has the
//! host replay it, but the DID's later events would reach the owner first,
//! fail `prevData` and desynchronize the account. So an event carries its
//! host socket's [`Fence`]: giving up trips it, and nothing else from that
//! socket is sent from then on (`ForwardError::Fenced`). The replay comes
//! on a new socket and brings all of them again, in order.

use crate::types::Host;
use bytes::{Buf, BufMut, Bytes};
use std::collections::{HashMap, HashSet};
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{Duration, Instant};
use tokio::sync::{mpsc, oneshot};

/// One verified event for the DID owner. `meta` is whatever the host
/// owner's stages parsed that the DID owner needs (opaque here).
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Forwarded {
    pub did: String,
    pub host: Host,
    pub upstream_seq: i64,
    pub meta: Bytes,
    pub frame: Bytes,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Outcome {
    /// Durable in the DID owner's log at this relay seq.
    Appended(i64),
    /// Already applied: nothing appended, done.
    Duplicate,
    /// Dropped by a check. Done too: the host owner acks it.
    Rejected(String),
}

#[derive(Clone, Debug, PartialEq, Eq, thiserror::Error)]
pub enum StageError {
    /// Not this node's DID: look the owner up again.
    #[error("not the DID's owner")]
    NotOwner,
    /// Couldn't decide now (log failed, shard opening): try again.
    #[error("unavailable: {0}")]
    Unavailable(String),
}

pub type StageResult = Result<Outcome, StageError>;

/// The DID-owner stage: chain checks, state, append, wait for durable.
#[async_trait::async_trait]
pub trait DidStage: Send + Sync + 'static {
    /// Called only with DIDs whose shard this node serves, in arrival
    /// order. One result per event. After a retryable error for a DID, the
    /// stage must not apply that DID's later events in the batch (answer
    /// them with the same error), or they'd land out of order.
    async fn apply(&self, batch: Vec<Forwarded>) -> Vec<StageResult>;
}

/// Runs each batch of a stage as a task of its own, so the caller going
/// away mid-batch (a peer's request dropped because its forwarder moved on
/// or the peer died, a local forward abandoned) can't cancel it. A stage
/// cancelled between an append and its durability leaves that event's
/// in-flight entry unresolved, and every later copy of the event, a
/// duplicate waiting on it, answers "the duplicated event isn't durable"
/// until its forward gives up (chaos kill9, minio-errors, crash-loop).
///
/// A caller that gives up retries, and its abandoned batch runs on: against
/// a stuck stage those would pile up without bound. Past `max_events` in
/// detached batches a new batch is answered `Unavailable` at once.
pub struct Detached<S> {
    stage: Arc<S>,
    room: Arc<tokio::sync::Semaphore>,
}

impl<S> Detached<S> {
    pub const DEFAULT_MAX_EVENTS: usize = 65_536;

    pub fn new(stage: Arc<S>, max_events: usize) -> Detached<S> {
        Detached { stage, room: Arc::new(tokio::sync::Semaphore::new(max_events.max(1))) }
    }
}

#[async_trait::async_trait]
impl<S: DidStage> DidStage for Detached<S> {
    async fn apply(&self, batch: Vec<Forwarded>) -> Vec<StageResult> {
        let n = batch.len();
        let Ok(permit) = self.room.clone().try_acquire_many_owned(n.max(1) as u32) else {
            return vec![Err(StageError::Unavailable("stage busy".into())); n];
        };
        let stage = self.stage.clone();
        let task = tokio::spawn(async move {
            let _permit = permit;
            stage.apply(batch).await
        });
        match task.await {
            Ok(r) => r,
            Err(e) => vec![Err(StageError::Unavailable(format!("stage task: {e}"))); n],
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq, thiserror::Error)]
pub enum ForwardError {
    #[error("gave up after {0:?}: {1}")]
    GaveUp(Duration, String),
    /// Not sent: an event from the same host socket gave up first, and the
    /// host replays this one too.
    #[error("fenced: an earlier event of its host socket gave up")]
    Fenced,
    #[error("forwarder stopped")]
    Stopped,
}

/// One host socket's events, as far as forwarding goes: live until one of
/// them gives up (or the host owner fences the socket). Every socket of a
/// host shares `below`: sockets older than it are fenced.
#[derive(Clone, Debug)]
pub struct Fence {
    epoch: u64,
    below: Arc<AtomicU64>,
}

impl Fence {
    pub fn new(epoch: u64, below: Arc<AtomicU64>) -> Fence {
        Fence { epoch, below }
    }

    pub fn live(&self) -> bool {
        self.epoch >= self.below.load(Ordering::Acquire)
    }

    /// Fences this socket and every older one. True if it wasn't already.
    pub fn trip(&self) -> bool {
        self.below.fetch_max(self.epoch + 1, Ordering::AcqRel) <= self.epoch
    }
}

#[derive(Clone, Debug)]
pub struct ForwardConfig {
    /// Lanes (one batch in flight each).
    pub lanes: usize,
    /// Batches in flight per lane.
    pub batches: usize,
    pub max_batch: usize,
    /// A batch stops taking events once it holds this many bytes (it
    /// always takes one).
    pub max_batch_bytes: usize,
    /// A lane holding fewer than `max_batch` events waits this long for more.
    pub window: Duration,
    pub lane_queue: usize,
    /// A forward is retried against new owners for this long.
    pub retry_budget: Duration,
    pub rpc_timeout: Duration,
}

impl Default for ForwardConfig {
    fn default() -> Self {
        ForwardConfig {
            lanes: 64,
            batches: 4,
            max_batch: 512,
            max_batch_bytes: MAX_BATCH_BYTES,
            window: Duration::from_millis(1),
            lane_queue: 4096,
            retry_budget: vlpds::forward::RETRY_BUDGET,
            rpc_timeout: Duration::from_secs(10),
        }
    }
}

/// 512 production-sized frames (~5.4 KB) are ~3 MB, past axum's 2 MB
/// default body limit, and an owner that answers 413 is retried until the
/// forward gives up.
pub const MAX_BATCH_BYTES: usize = 4 << 20;
/// The peer listener's limit for a forward: a full batch plus one more
/// event at the largest upstream frame (`UpstreamConfig::max_frame_bytes`).
pub const MAX_BODY_BYTES: usize = MAX_BATCH_BYTES + (6 << 20);

fn wire_size(e: &Forwarded) -> usize {
    24 + e.did.len() + e.host.0.len() + e.meta.len() + e.frame.len()
}

/// Where a DID goes, and how to get there.
#[async_trait::async_trait]
pub trait Route: Send + Sync + 'static {
    /// None: no live owner right now. Some(None): this node.
    fn owner(&self, did: &str) -> Option<Option<String>>;
    /// Runs the batch here (the fast path).
    async fn local(&self, batch: Vec<Forwarded>) -> Vec<StageResult>;
    /// Sends the batch to the node at `addr`.
    async fn remote(&self, addr: &str, batch: Vec<Forwarded>) -> anyhow::Result<Vec<StageResult>>;
}

type Reply = oneshot::Sender<Result<Outcome, ForwardError>>;

static FORWARD_EVENTS: std::sync::LazyLock<prometheus::IntCounterVec> = std::sync::LazyLock::new(|| {
    prometheus::register_int_counter_vec!(
        "vlrelay_cluster_forward_events_total",
        "Events sent to their DID owner, by where it is (local: this node's stage, remote: a peer)",
        &["to"]
    )
    .unwrap()
});
static FORWARD_SECONDS: std::sync::LazyLock<prometheus::HistogramVec> = std::sync::LazyLock::new(|| {
    prometheus::register_histogram_vec!(
        "vlrelay_cluster_forward_batch_seconds",
        "A forwarded batch's send to its owner's answer (made durable or decided), by where the owner is",
        &["to"],
        prometheus::exponential_buckets(0.0005, 1.6, 24).unwrap()
    )
    .unwrap()
});
/// Request body bytes of forwards to peers.
pub(crate) static FORWARD_BYTES: std::sync::LazyLock<prometheus::IntCounter> = std::sync::LazyLock::new(|| {
    prometheus::register_int_counter!("vlrelay_cluster_forward_bytes_total", "Request body bytes forwarded to peers")
        .unwrap()
});

struct Item {
    ev: Forwarded,
    reply: Reply,
    since: Instant,
    fence: Option<Fence>,
}

impl Item {
    fn live(&self) -> bool {
        self.fence.as_ref().is_none_or(|f| f.live())
    }
}

pub struct Forwarder {
    lanes: Vec<mpsc::Sender<Item>>,
}

impl Forwarder {
    pub fn start(cfg: ForwardConfig, route: Arc<dyn Route>) -> Arc<Forwarder> {
        let mut lanes = Vec::with_capacity(cfg.lanes.max(1));
        for _ in 0..cfg.lanes.max(1) {
            let (tx, rx) = mpsc::channel(cfg.lane_queue.max(1));
            tokio::spawn(lane(cfg.clone(), route.clone(), rx));
            lanes.push(tx);
        }
        Arc::new(Forwarder { lanes })
    }

    /// Queues an event (waiting while its lane is full). The receiver
    /// resolves with its outcome.
    pub async fn submit(&self, ev: Forwarded) -> oneshot::Receiver<Result<Outcome, ForwardError>> {
        self.submit_fenced(ev, None).await
    }

    /// [`Self::submit`] for an event of the host socket `fence` stands for.
    pub async fn submit_fenced(
        &self,
        ev: Forwarded,
        fence: Option<Fence>,
    ) -> oneshot::Receiver<Result<Outcome, ForwardError>> {
        let (reply, rx) = oneshot::channel();
        let lane = vlpds::slots::slot_of(&ev.did) as usize % self.lanes.len();
        let item = Item { ev, reply, since: Instant::now(), fence };
        if let Err(mpsc::error::SendError(item)) = self.lanes[lane].send(item).await {
            let _ = item.reply.send(Err(ForwardError::Stopped));
        }
        rx
    }

    pub async fn forward(&self, ev: Forwarded) -> Result<Outcome, ForwardError> {
        self.submit(ev).await.await.unwrap_or(Err(ForwardError::Stopped))
    }
}

async fn lane(cfg: ForwardConfig, route: Arc<dyn Route>, mut rx: mpsc::Receiver<Item>) {
    use futures::StreamExt;
    use std::collections::{HashMap, HashSet, VecDeque};
    let mut inflight = futures::stream::FuturesUnordered::new();
    // DID -> its events in flight
    let mut busy: HashMap<String, usize> = HashMap::new();
    // waiting on a DID in flight (or behind an earlier held event of theirs)
    let mut held: VecDeque<Item> = VecDeque::new();
    let mut open = true;
    loop {
        // what's held and free to go, in order
        let mut batch = Vec::new();
        let mut bytes = 0;
        if !held.is_empty() && inflight.len() < cfg.batches.max(1) {
            let mut blocked: HashSet<String> = HashSet::new();
            let mut keep = VecDeque::new();
            for it in held.drain(..) {
                if batch.len() < cfg.max_batch
                    && bytes < cfg.max_batch_bytes
                    && !busy.contains_key(&it.ev.did)
                    && !blocked.contains(&it.ev.did)
                {
                    bytes += wire_size(&it.ev);
                    batch.push(it);
                } else {
                    blocked.insert(it.ev.did.clone());
                    keep.push_back(it);
                }
            }
            held = keep;
        }
        if batch.is_empty() {
            let can_take = open && inflight.len() < cfg.batches.max(1) && held.len() < cfg.lane_queue.max(1);
            if !can_take && inflight.is_empty() {
                return;
            }
            tokio::select! {
                Some(dids) = inflight.next(), if !inflight.is_empty() => {
                    for d in dids {
                        release(&mut busy, d);
                    }
                    continue;
                }
                first = rx.recv(), if can_take => {
                    let Some(first) = first else {
                        open = false;
                        continue;
                    };
                    let mut taken_bytes = wire_size(&first.ev);
                    let mut taken = vec![first];
                    let deadline = tokio::time::Instant::now() + cfg.window;
                    while taken.len() < cfg.max_batch && taken_bytes < cfg.max_batch_bytes {
                        match tokio::time::timeout_at(deadline, rx.recv()).await {
                            Ok(Some(it)) => {
                                taken_bytes += wire_size(&it.ev);
                                taken.push(it);
                            }
                            Ok(None) | Err(_) => break,
                        }
                    }
                    let mut blocked: HashSet<String> = held.iter().map(|i| i.ev.did.clone()).collect();
                    for it in taken {
                        if busy.contains_key(&it.ev.did) || blocked.contains(&it.ev.did) {
                            blocked.insert(it.ev.did.clone());
                            held.push_back(it);
                        } else {
                            batch.push(it);
                        }
                    }
                }
            }
        }
        // one batch per owner, so a slow owner's answer doesn't hold the rest
        let mut groups: Vec<(Option<Option<String>>, Vec<Item>)> = Vec::new();
        // one lookup per DID: two of its events in different batches would
        // be in flight at once, and could land out of order
        let mut owners: HashMap<String, Option<Option<String>>> = HashMap::new();
        for it in batch {
            let o = owners.entry(it.ev.did.clone()).or_insert_with(|| route.owner(&it.ev.did)).clone();
            match groups.iter_mut().find(|(g, _)| *g == o) {
                Some((_, v)) => v.push(it),
                None => groups.push((o, vec![it])),
            }
        }
        for (_, batch) in groups {
            let dids: Vec<String> = batch.iter().map(|i| i.ev.did.clone()).collect();
            for d in &dids {
                *busy.entry(d.clone()).or_default() += 1;
            }
            let (cfg, route) = (cfg.clone(), route.clone());
            inflight.push(async move {
                run_batch(&cfg, &*route, batch).await;
                dids
            });
        }
    }
}

fn release(busy: &mut std::collections::HashMap<String, usize>, did: String) {
    if let std::collections::hash_map::Entry::Occupied(mut e) = busy.entry(did) {
        *e.get_mut() -= 1;
        if *e.get() == 0 {
            e.remove();
        }
    }
}

/// Resolves once any of `items` is no longer owned by `owner`.
async fn moved_off(route: &dyn Route, items: &[Item], owner: &Option<String>) {
    loop {
        tokio::time::sleep(Duration::from_millis(100)).await;
        if items.iter().any(|i| route.owner(&i.ev.did).as_ref() != Some(owner)) {
            return;
        }
    }
}

/// Until every event is resolved: split by owner, send, keep the retryable
/// failures in order, back off, look owners up again.
async fn run_batch(cfg: &ForwardConfig, route: &dyn Route, mut pending: Vec<Item>) {
    let mut backoff = Duration::from_millis(20);
    loop {
        let mut groups: Vec<(Option<String>, Vec<Item>)> = Vec::new();
        let mut unrouted: Vec<Item> = Vec::new();
        // one lookup per DID per pass: an owner appearing mid-pass must not
        // send a DID's later event while its earlier one waits unrouted
        let mut owners: HashMap<String, Option<Option<String>>> = HashMap::new();
        for it in pending.drain(..) {
            if !it.live() {
                let _ = it.reply.send(Err(ForwardError::Fenced));
                continue;
            }
            let owner = owners.entry(it.ev.did.clone()).or_insert_with(|| route.owner(&it.ev.did)).clone();
            match owner {
                None => unrouted.push(it),
                Some(o) => match groups.iter_mut().find(|(g, _)| *g == o) {
                    Some((_, v)) => v.push(it),
                    None => groups.push((o, vec![it])),
                },
            }
        }
        if groups.is_empty() && unrouted.is_empty() {
            return;
        }
        let sends = groups.into_iter().map(|(owner, items)| async move {
            let evs: Vec<Forwarded> = items.iter().map(|i| i.ev.clone()).collect();
            let to = if owner.is_none() { "local" } else { "remote" };
            FORWARD_EVENTS.with_label_values(&[to]).inc_by(evs.len() as u64);
            let t0 = Instant::now();
            let res = match &owner {
                None => Ok(route.local(evs).await),
                // A hung owner (SIGSTOP, a GC pause, a blackholed link) keeps
                // the socket open, so the request would sit out the whole
                // timeout with the lane blocked behind it, even after the
                // DID's shard moved. Once it moved, the old owner can't make
                // the event durable or answer for it, so asking the new one
                // is safe.
                Some(addr) => tokio::select! {
                    r = tokio::time::timeout(cfg.rpc_timeout, route.remote(addr, evs)) => match r {
                        Ok(r) => r,
                        Err(_) => Err(anyhow::anyhow!("forward to {addr} timed out")),
                    },
                    _ = moved_off(route, &items, &owner) => Err(anyhow::anyhow!("a DID's owner moved off {addr} mid-forward")),
                },
            };
            FORWARD_SECONDS.with_label_values(&[to]).observe(t0.elapsed().as_secs_f64());
            (items, res)
        });
        let mut why = String::from("no live owner");
        let mut retry: Vec<Item> = unrouted;
        for (items, res) in futures::future::join_all(sends).await {
            match res {
                Ok(results) if results.len() == items.len() => {
                    for (it, r) in items.into_iter().zip(results) {
                        match r {
                            Ok(o) => {
                                let _ = it.reply.send(Ok(o));
                            }
                            Err(e) => {
                                why = e.to_string();
                                retry.push(it);
                            }
                        }
                    }
                }
                Ok(results) => {
                    why = format!("{} results for {} events", results.len(), items.len());
                    retry.extend(items);
                }
                Err(e) => {
                    why = format!("{e:#}");
                    retry.extend(items);
                }
            }
        }
        if retry.is_empty() {
            return;
        }
        // the lane's order: arrival order within the batch
        retry.sort_by_key(|i| i.since);
        // A DID whose event gives up gives up the rest of its events too;
        // the fence stops the rest of the host socket's (from this batch and
        // every later one) before anything else is sent.
        let mut gave_up: HashSet<String> = HashSet::new();
        let (expired, live): (Vec<Item>, Vec<Item>) = retry.into_iter().partition(|i| {
            if i.since.elapsed() >= cfg.retry_budget || gave_up.contains(&i.ev.did) {
                gave_up.insert(i.ev.did.clone());
                true
            } else {
                false
            }
        });
        for it in expired {
            if let Some(f) = &it.fence {
                f.trip();
            }
            let _ = it.reply.send(Err(ForwardError::GaveUp(cfg.retry_budget, why.clone())));
        }
        if live.is_empty() {
            return;
        }
        tracing::debug!(events = live.len(), "forward retrying: {why}");
        tokio::time::sleep(backoff).await;
        backoff = (backoff * 2).min(Duration::from_millis(500));
        pending = live;
    }
}

// ---- wire format ----
//
// request:  count u32 | (did u16+bytes | host u16+bytes | upstream_seq i64 |
//           meta u32+bytes | frame u32+bytes)*
// response: count u32 | (tag u8 | payload)*
//           0 appended (seq i64), 1 duplicate, 2 rejected (u16+msg),
//           3 not owner, 4 unavailable (u16+msg)

pub fn encode_batch(evs: &[Forwarded]) -> Bytes {
    let size: usize = evs.iter().map(wire_size).sum();
    let mut b = Vec::with_capacity(4 + size);
    b.put_u32(evs.len() as u32);
    for e in evs {
        b.put_u16(e.did.len() as u16);
        b.put_slice(e.did.as_bytes());
        b.put_u16(e.host.0.len() as u16);
        b.put_slice(e.host.0.as_bytes());
        b.put_i64(e.upstream_seq);
        b.put_u32(e.meta.len() as u32);
        b.put_slice(&e.meta);
        b.put_u32(e.frame.len() as u32);
        b.put_slice(&e.frame);
    }
    b.into()
}

fn take_str(r: &mut Bytes, n: usize) -> anyhow::Result<String> {
    anyhow::ensure!(r.remaining() >= n, "short string");
    Ok(String::from_utf8(r.split_to(n).to_vec())?)
}

pub fn decode_batch(mut r: Bytes) -> anyhow::Result<Vec<Forwarded>> {
    anyhow::ensure!(r.remaining() >= 4, "short batch");
    let n = r.get_u32() as usize;
    let mut out = Vec::with_capacity(n.min(r.remaining() / 24));
    for _ in 0..n {
        anyhow::ensure!(r.remaining() >= 2, "short event");
        let l = r.get_u16() as usize;
        let did = take_str(&mut r, l)?;
        anyhow::ensure!(r.remaining() >= 2, "short event");
        let l = r.get_u16() as usize;
        let host = Host(take_str(&mut r, l)?);
        anyhow::ensure!(r.remaining() >= 12, "short event");
        let upstream_seq = r.get_i64();
        let l = r.get_u32() as usize;
        anyhow::ensure!(r.remaining() >= l, "short meta");
        let meta = r.split_to(l);
        anyhow::ensure!(r.remaining() >= 4, "short event");
        let l = r.get_u32() as usize;
        anyhow::ensure!(r.remaining() >= l, "short frame");
        let frame = r.split_to(l);
        out.push(Forwarded { did, host, upstream_seq, meta, frame });
    }
    anyhow::ensure!(!r.has_remaining(), "trailing bytes");
    Ok(out)
}

fn put_msg(b: &mut Vec<u8>, m: &str) {
    let m = &m.as_bytes()[..m.len().min(u16::MAX as usize)];
    b.put_u16(m.len() as u16);
    b.put_slice(m);
}

pub fn encode_results(rs: &[StageResult]) -> Bytes {
    let mut b = Vec::with_capacity(4 + rs.len() * 9);
    b.put_u32(rs.len() as u32);
    for r in rs {
        match r {
            Ok(Outcome::Appended(seq)) => {
                b.put_u8(0);
                b.put_i64(*seq);
            }
            Ok(Outcome::Duplicate) => b.put_u8(1),
            Ok(Outcome::Rejected(m)) => {
                b.put_u8(2);
                put_msg(&mut b, m);
            }
            Err(StageError::NotOwner) => b.put_u8(3),
            Err(StageError::Unavailable(m)) => {
                b.put_u8(4);
                put_msg(&mut b, m);
            }
        }
    }
    b.into()
}

pub fn decode_results(mut r: Bytes) -> anyhow::Result<Vec<StageResult>> {
    anyhow::ensure!(r.remaining() >= 4, "short results");
    let n = r.get_u32() as usize;
    let mut out = Vec::with_capacity(n.min(r.remaining()));
    for _ in 0..n {
        anyhow::ensure!(r.has_remaining(), "short result");
        let msg = |r: &mut Bytes| -> anyhow::Result<String> {
            anyhow::ensure!(r.remaining() >= 2, "short message");
            let l = r.get_u16() as usize;
            take_str(r, l)
        };
        out.push(match r.get_u8() {
            0 => {
                anyhow::ensure!(r.remaining() >= 8, "short seq");
                Ok(Outcome::Appended(r.get_i64()))
            }
            1 => Ok(Outcome::Duplicate),
            2 => Ok(Outcome::Rejected(msg(&mut r)?)),
            3 => Err(StageError::NotOwner),
            4 => Err(StageError::Unavailable(msg(&mut r)?)),
            t => anyhow::bail!("unknown result tag {t}"),
        });
    }
    Ok(out)
}

#[cfg(test)]
mod tests {
    use super::*;
    use parking_lot::Mutex;
    use std::collections::HashMap;

    fn ev(did: &str, seq: i64) -> Forwarded {
        Forwarded {
            did: did.into(),
            host: Host("pds.example".into()),
            upstream_seq: seq,
            meta: Bytes::from_static(b"m"),
            frame: Bytes::from(vec![seq as u8; 3]),
        }
    }

    #[test]
    fn wire_round_trips() {
        let evs = vec![ev("did:plc:a", 1), ev("did:plc:b", 2)];
        assert_eq!(decode_batch(encode_batch(&evs)).unwrap(), evs);
        let rs: Vec<StageResult> = vec![
            Ok(Outcome::Appended(77)),
            Ok(Outcome::Duplicate),
            Ok(Outcome::Rejected("bad sig".into())),
            Err(StageError::NotOwner),
            Err(StageError::Unavailable("opening".into())),
        ];
        assert_eq!(decode_results(encode_results(&rs)).unwrap(), rs);
        let mut cut = encode_batch(&evs).to_vec();
        cut.truncate(cut.len() - 1);
        assert!(decode_batch(Bytes::from(cut)).is_err());
    }

    /// Owner of each DID flips after a "takeover"; events keep their order
    /// per DID and land exactly once.
    struct Flaky {
        owner: Mutex<HashMap<String, Option<String>>>,
        applied: Mutex<Vec<(String, String, i64)>>,
        refuse_on: Mutex<String>,
    }

    #[async_trait::async_trait]
    impl Route for Flaky {
        fn owner(&self, did: &str) -> Option<Option<String>> {
            self.owner.lock().get(did).cloned()
        }
        async fn local(&self, batch: Vec<Forwarded>) -> Vec<StageResult> {
            self.apply_at("local", batch)
        }
        async fn remote(&self, addr: &str, batch: Vec<Forwarded>) -> anyhow::Result<Vec<StageResult>> {
            Ok(self.apply_at(addr, batch))
        }
    }

    impl Flaky {
        fn apply_at(&self, at: &str, batch: Vec<Forwarded>) -> Vec<StageResult> {
            let refuse = self.refuse_on.lock().clone();
            batch
                .into_iter()
                .map(|e| {
                    if at == refuse {
                        return Err(StageError::NotOwner);
                    }
                    self.applied.lock().push((at.to_string(), e.did.clone(), e.upstream_seq));
                    Ok(Outcome::Appended(e.upstream_seq))
                })
                .collect()
        }
    }

    #[tokio::test]
    async fn retries_follow_the_new_owner_in_order() {
        let r = Arc::new(Flaky {
            owner: Mutex::new(HashMap::from([
                ("did:a".to_string(), Some("https://b".to_string())),
                ("did:b".to_string(), None),
            ])),
            applied: Mutex::new(Vec::new()),
            refuse_on: Mutex::new("https://b".into()),
        });
        let f = Forwarder::start(ForwardConfig { lanes: 4, ..Default::default() }, r.clone());
        let mut waits = Vec::new();
        for i in 0..20 {
            waits.push(f.submit(ev("did:a", i)).await);
            waits.push(f.submit(ev("did:b", i)).await);
        }
        tokio::time::sleep(Duration::from_millis(60)).await;
        // the takeover: did:a now lives at c, and b answers NotOwner meanwhile
        r.owner.lock().insert("did:a".into(), Some("https://c".into()));
        for w in waits {
            assert!(matches!(w.await.unwrap(), Ok(Outcome::Appended(_))));
        }
        let applied = r.applied.lock().clone();
        let a: Vec<i64> = applied.iter().filter(|x| x.1 == "did:a").map(|x| x.2).collect();
        let b: Vec<i64> = applied.iter().filter(|x| x.1 == "did:b").map(|x| x.2).collect();
        assert_eq!(a, (0..20).collect::<Vec<_>>());
        assert_eq!(b, (0..20).collect::<Vec<_>>());
        assert!(applied.iter().filter(|x| x.1 == "did:a").all(|x| x.0 == "https://c"));
        assert!(applied.iter().filter(|x| x.1 == "did:b").all(|x| x.0 == "local"));
    }

    /// An owner that accepts the request and never answers (SIGSTOPped).
    struct Hung {
        owner: Mutex<Option<String>>,
    }

    #[async_trait::async_trait]
    impl Route for Hung {
        fn owner(&self, _did: &str) -> Option<Option<String>> {
            Some(self.owner.lock().clone())
        }
        async fn local(&self, batch: Vec<Forwarded>) -> Vec<StageResult> {
            batch.iter().map(|e| Ok(Outcome::Appended(e.upstream_seq))).collect()
        }
        async fn remote(&self, _addr: &str, _batch: Vec<Forwarded>) -> anyhow::Result<Vec<StageResult>> {
            std::future::pending().await
        }
    }

    /// The zombie scenario's 10 s stall: a forward to a hung owner is
    /// abandoned once the DID moves, not after the RPC timeout.
    #[tokio::test]
    async fn a_hung_owner_is_abandoned_when_the_did_moves() {
        let r = Arc::new(Hung { owner: Mutex::new(Some("https://zombie".into())) });
        let f = Forwarder::start(ForwardConfig { lanes: 1, ..Default::default() }, r.clone());
        let t0 = Instant::now();
        let w = f.submit(ev("did:a", 1)).await;
        tokio::time::sleep(Duration::from_millis(150)).await;
        *r.owner.lock() = None;
        assert!(matches!(w.await.unwrap(), Ok(Outcome::Appended(1))));
        assert!(t0.elapsed() < Duration::from_secs(2), "took {:?}", t0.elapsed());
    }

    /// One owner answers in 300 ms (its bucket path is slow), the other at
    /// once, all in one lane.
    struct Slow {
        applied: Mutex<Vec<(String, i64)>>,
    }

    #[async_trait::async_trait]
    impl Route for Slow {
        fn owner(&self, did: &str) -> Option<Option<String>> {
            Some(did.starts_with("did:slow").then(|| "https://slow".to_string()))
        }
        async fn local(&self, batch: Vec<Forwarded>) -> Vec<StageResult> {
            self.applied.lock().extend(batch.iter().map(|e| (e.did.clone(), e.upstream_seq)));
            batch.iter().map(|e| Ok(Outcome::Appended(e.upstream_seq))).collect()
        }
        async fn remote(&self, _addr: &str, batch: Vec<Forwarded>) -> anyhow::Result<Vec<StageResult>> {
            tokio::time::sleep(Duration::from_millis(300)).await;
            Ok(self.local(batch).await)
        }
    }

    /// The slow owner holds back only its own DIDs, each still in order.
    #[tokio::test]
    async fn a_slow_owner_holds_back_only_its_dids() {
        let r = Arc::new(Slow { applied: Mutex::new(Vec::new()) });
        let f = Forwarder::start(ForwardConfig { lanes: 1, ..Default::default() }, r.clone());
        let t0 = Instant::now();
        let mut slow = Vec::new();
        let mut fast = Vec::new();
        for i in 0..10 {
            slow.push(f.submit(ev("did:slow", i)).await);
            fast.push(f.submit(ev("did:fast", i)).await);
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
        for w in fast {
            assert!(matches!(w.await.unwrap(), Ok(Outcome::Appended(_))));
        }
        assert!(t0.elapsed() < Duration::from_millis(250), "fast DIDs waited on the slow owner: {:?}", t0.elapsed());
        for w in slow {
            assert!(matches!(w.await.unwrap(), Ok(Outcome::Appended(_))));
        }
        let applied = r.applied.lock().clone();
        for did in ["did:slow", "did:fast"] {
            let got: Vec<i64> = applied.iter().filter(|x| x.0 == did).map(|x| x.1).collect();
            assert_eq!(got, (0..10).collect::<Vec<_>>(), "{did}");
        }
    }

    /// Issue 2 in docs/chaos.md: N gives up while N+1 waits behind it; once
    /// an owner appears N+1 must not reach it ahead of N's replay, or the
    /// account desynchronizes. Neither may anything else from that socket.
    #[tokio::test]
    async fn a_give_up_fences_the_rest_of_its_socket() {
        let r = Arc::new(Flaky {
            owner: Mutex::new(HashMap::new()),
            applied: Mutex::new(Vec::new()),
            refuse_on: Mutex::new(String::new()),
        });
        let cfg = ForwardConfig { lanes: 1, retry_budget: Duration::from_millis(200), ..Default::default() };
        let f = Forwarder::start(cfg, r.clone());
        let below = Arc::new(AtomicU64::new(0));
        let old = Fence::new(1, below.clone());
        let n = f.submit_fenced(ev("did:x", 1), Some(old.clone())).await;
        tokio::time::sleep(Duration::from_millis(120)).await;
        let n1 = f.submit_fenced(ev("did:x", 2), Some(old.clone())).await;
        let other = f.submit_fenced(ev("did:y", 3), Some(old.clone())).await;
        assert!(matches!(n.await.unwrap(), Err(ForwardError::GaveUp(..))));
        r.owner.lock().insert("did:x".into(), None);
        r.owner.lock().insert("did:y".into(), None);
        assert_eq!(n1.await.unwrap(), Err(ForwardError::Fenced));
        assert_eq!(other.await.unwrap(), Err(ForwardError::Fenced));
        assert!(r.applied.lock().is_empty());
        // the replay comes on the next socket
        let replay = Fence::new(2, below);
        assert!(replay.live());
        for s in [1, 2] {
            let got = f.submit_fenced(ev("did:x", s), Some(replay.clone())).await.await.unwrap();
            assert_eq!(got, Ok(Outcome::Appended(s)));
        }
        assert_eq!(r.applied.lock().iter().map(|x| x.2).collect::<Vec<_>>(), vec![1, 2]);
    }

    /// Within one batch: a DID's later event goes down with its earlier one.
    #[tokio::test]
    async fn a_give_up_takes_the_dids_later_events_in_the_batch() {
        let r = Arc::new(Flaky {
            owner: Mutex::new(HashMap::new()),
            applied: Mutex::new(Vec::new()),
            refuse_on: Mutex::new(String::new()),
        });
        let cfg = ForwardConfig {
            lanes: 1,
            retry_budget: Duration::from_millis(200),
            window: Duration::from_millis(150),
            ..Default::default()
        };
        let f = Forwarder::start(cfg, r.clone());
        let n = f.submit(ev("did:x", 1)).await;
        tokio::time::sleep(Duration::from_millis(100)).await;
        let n1 = f.submit(ev("did:x", 2)).await;
        assert!(matches!(n.await.unwrap(), Err(ForwardError::GaveUp(..))));
        r.owner.lock().insert("did:x".into(), None);
        assert!(matches!(n1.await.unwrap(), Err(ForwardError::GaveUp(..))));
        assert!(r.applied.lock().is_empty());
    }

    /// A stage that takes a claim, then waits for durability, then settles
    /// it: the shape of the DID owner's stage.
    struct Claims {
        held: Mutex<HashMap<String, bool>>,
        durable: tokio::sync::Notify,
    }

    #[async_trait::async_trait]
    impl DidStage for Claims {
        async fn apply(&self, batch: Vec<Forwarded>) -> Vec<StageResult> {
            for e in &batch {
                self.held.lock().insert(e.did.clone(), false);
            }
            self.durable.notified().await;
            for e in &batch {
                self.held.lock().insert(e.did.clone(), true);
            }
            batch.iter().map(|e| Ok(Outcome::Appended(e.upstream_seq))).collect()
        }
    }

    /// The caller of a batch goes away between the claim and durability
    /// (its peer died, or its forwarder moved on). Without `Detached` the
    /// claim was never settled, and every later copy of the event waited on
    /// it until its forward gave up.
    #[tokio::test]
    async fn a_dropped_caller_does_not_cancel_the_stage() {
        let inner = Arc::new(Claims { held: Mutex::new(HashMap::new()), durable: tokio::sync::Notify::new() });
        let stage = Detached::new(inner.clone(), 2);
        let call = stage.apply(vec![ev("did:a", 1)]);
        assert!(tokio::time::timeout(Duration::from_millis(50), call).await.is_err(), "dropped mid-batch");
        assert_eq!(inner.held.lock().get("did:a"), Some(&false));
        // abandoned batches still count: past the bound a retry is turned away
        let again = stage.apply(vec![ev("did:b", 2)]);
        assert!(tokio::time::timeout(Duration::from_millis(50), again).await.is_err());
        let busy = stage.apply(vec![ev("did:c", 3)]).await;
        assert_eq!(busy, vec![Err(StageError::Unavailable("stage busy".into()))]);
        inner.durable.notify_waiters();
        tokio::time::sleep(Duration::from_millis(50)).await;
        assert_eq!(inner.held.lock().get("did:a"), Some(&true), "the batch ran to its end");
        assert_eq!(inner.held.lock().get("did:b"), Some(&true));
    }

    #[tokio::test]
    async fn gives_up_after_the_budget() {
        let r = Arc::new(Flaky {
            owner: Mutex::new(HashMap::new()),
            applied: Mutex::new(Vec::new()),
            refuse_on: Mutex::new(String::new()),
        });
        let cfg = ForwardConfig { lanes: 1, retry_budget: Duration::from_millis(100), ..Default::default() };
        let f = Forwarder::start(cfg, r);
        let got = f.forward(ev("did:x", 1)).await;
        assert!(matches!(got, Err(ForwardError::GaveUp(..))), "{got:?}");
    }

    /// Records each remote request's body size.
    struct Sizes(Mutex<Vec<(usize, usize)>>);

    #[async_trait::async_trait]
    impl Route for Sizes {
        fn owner(&self, _did: &str) -> Option<Option<String>> {
            Some(Some("https://peer".into()))
        }
        async fn local(&self, batch: Vec<Forwarded>) -> Vec<StageResult> {
            batch.iter().map(|e| Ok(Outcome::Appended(e.upstream_seq))).collect()
        }
        async fn remote(&self, _addr: &str, batch: Vec<Forwarded>) -> anyhow::Result<Vec<StageResult>> {
            self.0.lock().push((batch.len(), encode_batch(&batch).len()));
            tokio::time::sleep(Duration::from_millis(5)).await;
            Ok(batch.iter().map(|e| Ok(Outcome::Appended(e.upstream_seq))).collect())
        }
    }

    /// Production-sized frames would fill a 512-event batch past the peer
    /// listener's body limit.
    #[tokio::test]
    async fn batches_stay_under_the_body_limit() {
        let r = Arc::new(Sizes(Mutex::new(Vec::new())));
        let cfg = ForwardConfig { lanes: 1, window: Duration::from_millis(50), ..Default::default() };
        let f = Forwarder::start(cfg, r.clone());
        let mut waits = Vec::new();
        for i in 0..600 {
            let mut e = ev(&format!("did:{i}"), i);
            e.frame = Bytes::from(vec![0u8; 20_000]);
            waits.push(f.submit(e).await);
        }
        for w in waits {
            assert!(matches!(w.await.unwrap(), Ok(Outcome::Appended(_))));
        }
        let sizes = r.0.lock().clone();
        assert_eq!(sizes.iter().map(|s| s.0).sum::<usize>(), 600);
        assert!(sizes.iter().all(|s| s.1 <= MAX_BATCH_BYTES + 20_100), "{sizes:?}");
        assert!(sizes.iter().any(|s| s.0 > 100), "{sizes:?}");
    }
}
