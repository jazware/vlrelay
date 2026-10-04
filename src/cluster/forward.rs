//! Host owner to DID owner: verified events go to the node that owns their
//! DID's shard, in batches, and each comes back with its result once the DID
//! owner's log made it durable (or decided it was a duplicate or a reject).
//!
//! Order per DID is what matters (the DID owner checks each commit against
//! the one before), so events go through lanes keyed by the DID's slot. A
//! lane has one batch in flight, which is the in-flight limit, and a batch's
//! failed events are retried, in order, before the lane takes anything new.
//! A batch splits by owner: the part for this node calls the stage directly
//! (no network), the rest goes over peer mTLS as one HTTP/2 POST per owner.
//!
//! An owner that answers `NotOwner` or can't be reached is looked up again
//! after a short backoff, so a takeover costs the time until the new owner
//! has opened the shard, as with vlpds's forwarded writes.

use crate::types::Host;
use bytes::{Buf, BufMut, Bytes};
use std::sync::Arc;
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

#[derive(Clone, Debug, PartialEq, Eq, thiserror::Error)]
pub enum ForwardError {
    #[error("gave up after {0:?}: {1}")]
    GaveUp(Duration, String),
    #[error("forwarder stopped")]
    Stopped,
}

#[derive(Clone, Debug)]
pub struct ForwardConfig {
    /// Lanes (one batch in flight each).
    pub lanes: usize,
    pub max_batch: usize,
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
            max_batch: 512,
            window: Duration::from_millis(1),
            lane_queue: 4096,
            retry_budget: vlpds::forward::RETRY_BUDGET,
            rpc_timeout: Duration::from_secs(10),
        }
    }
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

struct Item {
    ev: Forwarded,
    reply: Reply,
    since: Instant,
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
        let (reply, rx) = oneshot::channel();
        let lane = vlpds::slots::slot_of(&ev.did) as usize % self.lanes.len();
        let item = Item { ev, reply, since: Instant::now() };
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
    while let Some(first) = rx.recv().await {
        let mut batch = vec![first];
        let deadline = tokio::time::Instant::now() + cfg.window;
        while batch.len() < cfg.max_batch {
            match tokio::time::timeout_at(deadline, rx.recv()).await {
                Ok(Some(it)) => batch.push(it),
                Ok(None) | Err(_) => break,
            }
        }
        run_batch(&cfg, &*route, batch).await;
    }
}

/// Until every event is resolved: split by owner, send, keep the retryable
/// failures in order, back off, look owners up again.
async fn run_batch(cfg: &ForwardConfig, route: &dyn Route, mut pending: Vec<Item>) {
    let mut backoff = Duration::from_millis(20);
    loop {
        let mut groups: Vec<(Option<String>, Vec<Item>)> = Vec::new();
        let mut unrouted: Vec<Item> = Vec::new();
        for it in pending.drain(..) {
            match route.owner(&it.ev.did) {
                None => unrouted.push(it),
                Some(o) => match groups.iter_mut().find(|(g, _)| *g == o) {
                    Some((_, v)) => v.push(it),
                    None => groups.push((o, vec![it])),
                },
            }
        }
        let sends = groups.into_iter().map(|(owner, items)| async move {
            let evs: Vec<Forwarded> = items.iter().map(|i| i.ev.clone()).collect();
            let res = match &owner {
                None => Ok(route.local(evs).await),
                Some(addr) => match tokio::time::timeout(cfg.rpc_timeout, route.remote(addr, evs)).await {
                    Ok(r) => r,
                    Err(_) => Err(anyhow::anyhow!("forward to {addr} timed out")),
                },
            };
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
        let (expired, live): (Vec<Item>, Vec<Item>) =
            retry.into_iter().partition(|i| i.since.elapsed() >= cfg.retry_budget);
        for it in expired {
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
    let size: usize = evs.iter().map(|e| 24 + e.did.len() + e.host.0.len() + e.meta.len() + e.frame.len()).sum();
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
}
