//! Dense stream seqs (1, 2, 3, ...) over the merged log stream.
//!
//! The logs and the merger stay in merge keys (`unix_micros << 8 | writer`).
//! A consumer sees seq N for the N-th event of the merged stream, counted
//! from the first event the bucket ever held. Every node merges the same
//! events in the same order, so every node, edge and replica numbers them
//! the same without talking to each other. docs/seq.md has the reasoning.
//!
//! What makes it recoverable is checkpoints: `seqck/{key:020}-{seq:020}`,
//! an empty object whose name says that exactly `seq` events of the merged
//! stream have a key <= `key`. Keys are boundaries every `every` of key
//! time, so every node computes the same pairs, and core nodes write them
//! with If-None-Match (the first writer wins; a different pair already there
//! would be a numbering bug, and is logged as one). A node anchors its count
//! at its start floor from the newest checkpoint at or below it plus a count
//! of the events in between, read from the bucket. A cursor older than the
//! ring is located the same way: the newest checkpoint at or below it, then
//! the backfill skips to it. Retention never prunes past the newest
//! checkpoint (`seq::prune`), so one always covers what's left.

use bytes::Bytes;
use futures::StreamExt;
use futures::future::BoxFuture;
use object_store::path::Path;
use object_store::{ObjectStoreExt, PutMode, PutOptions, PutPayload};
use std::collections::BTreeMap;
use std::sync::Arc;
use std::time::Duration;
use vlpds::firehose::Renumber;
use vlpds::store::Store;

pub const DEFAULT_CHECKPOINT_EVERY: Duration = Duration::from_secs(10);
const PREFIX: &str = "seqck";

#[derive(Clone)]
pub struct DenseSeqs(Arc<Inner>);

struct Inner {
    store: Store,
    /// Boundary spacing in key units.
    every: i64,
    /// Whether this node writes checkpoints (core nodes).
    write: bool,
    /// This node's log. A node whose log a successor fenced is a zombie: its
    /// merge no longer sees every log, so it must not write checkpoints.
    own_log: parking_lot::Mutex<Option<String>>,
    st: parking_lot::Mutex<State>,
}

#[derive(Default)]
struct State {
    /// The next boundary to record (set once anchored).
    next: Option<i64>,
    /// Known (key, seq) pairs: listed from the bucket, or our own.
    known: BTreeMap<i64, i64>,
    /// The newest key `known` has been listed up to.
    listed: Option<i64>,
}

fn key_path(store: &Store, key: i64, seq: i64) -> Path {
    Path::from(format!("{}/{PREFIX}/{key:020}-{seq:020}", store.prefix))
}

fn parse_name(name: &str) -> Option<(i64, i64)> {
    let (k, s) = name.split_once('-')?;
    Some((k.parse().ok()?, s.parse().ok()?))
}

/// The first boundary strictly above `key`.
fn boundary_above(key: i64, every: i64) -> i64 {
    let t = (key >> 8) / every;
    let b = ((t * every) << 8) | 0xff;
    if b > key { b } else { (((t + 1) * every) << 8) | 0xff }
}

/// Every checkpoint in the bucket, oldest first (from `offset` on, if given).
pub async fn list(store: &Store, offset: Option<i64>) -> anyhow::Result<Vec<(i64, i64)>> {
    let prefix = Path::from(format!("{}/{PREFIX}", store.prefix));
    let mut s = match offset {
        Some(k) => store.raw.list_with_offset(Some(&prefix), &key_path(store, k, 0)),
        None => store.raw.list(Some(&prefix)),
    };
    let mut out = Vec::new();
    while let Some(m) = s.next().await {
        if let Some(p) = m?.location.filename().and_then(parse_name) {
            out.push(p);
        }
    }
    out.sort_unstable();
    Ok(out)
}

/// Deletes the checkpoints below `key` (retention pruned what they count from).
pub async fn prune_below(store: &Store, key: i64, max: usize) -> anyhow::Result<usize> {
    let prefix = Path::from(format!("{}/{PREFIX}", store.prefix));
    let mut s = store.raw.list(Some(&prefix)).take(max);
    let mut doomed = Vec::new();
    while let Some(m) = s.next().await {
        let m = m?;
        match m.location.filename().and_then(parse_name) {
            Some((k, _)) if k < key => doomed.push(m.location),
            _ => break,
        }
    }
    drop(s);
    let n = doomed.len();
    for p in doomed {
        match store.raw.delete(&p).await {
            Ok(()) | Err(object_store::Error::NotFound { .. }) => {}
            Err(e) => return Err(e.into()),
        }
    }
    Ok(n)
}

/// The newest checkpoint key, if any.
pub async fn newest(store: &Store) -> anyhow::Result<Option<i64>> {
    Ok(list(store, None).await?.last().map(|c| c.0))
}

/// The newest checkpoint key at or below `key`, if any.
pub async fn newest_at_or_below(store: &Store, key: i64) -> anyhow::Result<Option<i64>> {
    Ok(list(store, None).await?.into_iter().map(|c| c.0).take_while(|k| *k <= key).last())
}

/// Events of the merged stream with `after < key <= until`.
async fn count(store: &Store, after: i64, until: i64) -> anyhow::Result<i64> {
    if until <= after {
        return Ok(0);
    }
    let (tx, mut rx) = tokio::sync::mpsc::channel(4096);
    let r = vlpds::backfill::Reader::new(store.clone());
    let job = tokio::spawn(async move { vlpds::backfill::backfill_with(&r, after, until, &tx).await });
    let mut n = 0i64;
    let mut buf = Vec::with_capacity(4096);
    while rx.recv_many(&mut buf, 4096).await > 0 {
        n += buf.len() as i64;
        buf.clear();
    }
    job.await??;
    Ok(n)
}

impl DenseSeqs {
    pub fn new(store: Store, every: Duration, write: bool) -> DenseSeqs {
        DenseSeqs(Arc::new(Inner {
            store,
            every: (every.as_micros() as i64).max(1),
            write,
            own_log: Default::default(),
            st: Default::default(),
        }))
    }

    pub fn set_own_log(&self, log_id: &str) {
        *self.0.own_log.lock() = Some(log_id.to_string());
    }

    /// Brings `known` up to date with the bucket.
    async fn refresh(&self) -> anyhow::Result<()> {
        let from = self.0.st.lock().listed;
        let got = list(&self.0.store, from).await?;
        let mut st = self.0.st.lock();
        for (k, s) in got {
            st.known.insert(k, s);
            st.listed = Some(st.listed.map_or(k, |l| l.max(k)));
        }
        Ok(())
    }

    /// The newest known (key, seq) with key <= `key` and key >= `floor`.
    fn at_or_below_key(&self, key: i64, floor: i64) -> Option<(i64, i64)> {
        let st = self.0.st.lock();
        st.known.range(floor..=key).next_back().map(|(k, s)| (*k, *s))
    }

    async fn anchor_at(self, key: i64) -> anyhow::Result<i64> {
        self.refresh().await?;
        let pruned = vlpds::retention::retained_floor(&self.0.store).await?;
        let (from, base) = match self.at_or_below_key(key, pruned) {
            Some(c) => c,
            None if pruned == 0 => (0, 0),
            // a bucket pruned before it had checkpoints: number what's left
            None => {
                tracing::warn!(pruned, "no seq checkpoint above the retained floor: numbering from it");
                (pruned, 0)
            }
        };
        let n = base + count(&self.0.store, from, key).await?;
        let mut st = self.0.st.lock();
        st.known.insert(key, n);
        st.next = Some(boundary_above(key, self.0.every));
        Ok(n)
    }

    async fn locate_seq(self, after: i64) -> anyhow::Result<(i64, i64)> {
        let pruned = vlpds::retention::retained_floor(&self.0.store).await?;
        let pick = |st: &State| -> Option<(i64, i64)> {
            // seqs ascend with keys: the newest pair at or below `after`
            // that retention left whole, else the oldest one it did. Until
            // something is pruned, the start of the bucket is such a pair.
            let origin = (pruned == 0).then_some((&0, &0));
            let usable = origin.into_iter().chain(st.known.range(pruned..));
            usable
                .clone()
                .take_while(|(_, s)| **s <= after)
                .last()
                .or_else(|| usable.clone().next())
                .map(|(k, s)| (*k, *s))
        };
        if let Some(c) = pick(&self.0.st.lock()) {
            return Ok(c);
        }
        self.refresh().await?;
        if let Some(c) = pick(&self.0.st.lock()) {
            return Ok(c);
        }
        anyhow::ensure!(pruned == 0, "no seq checkpoint at or above the retained floor {pruned}");
        Ok((0, 0))
    }

    fn put(&self, key: i64, seq: i64) {
        let store = self.0.store.clone();
        let own = self.0.own_log.lock().clone();
        tokio::spawn(async move {
            if let Some(log) = own {
                match vlpds::nodelog::first_free(&store, &log).await {
                    Ok((_, false)) => {}
                    Ok((_, true)) => return tracing::debug!(log, "our log is fenced: no more seq checkpoints"),
                    Err(e) => return tracing::warn!(log, "checking our log before a seq checkpoint: {e:#}"),
                }
            }
            // another core may have written this boundary: it must be the same pair
            match list(&store, Some(key - 1)).await {
                Ok(v) => match v.iter().find(|(k, _)| *k == key) {
                    Some((_, other)) if *other != seq => {
                        return tracing::error!(
                            key,
                            seq,
                            other,
                            "seq checkpoints disagree: nodes numbered the stream differently"
                        );
                    }
                    Some(_) => return,
                    None => {}
                },
                Err(e) => return tracing::warn!("listing seq checkpoints: {e:#}"),
            }
            let path = key_path(&store, key, seq);
            let opts = PutOptions { mode: PutMode::Create, ..Default::default() };
            match store.raw.put_opts(&path, PutPayload::from_bytes(Bytes::new()), opts).await {
                Ok(_) | Err(object_store::Error::AlreadyExists { .. }) => {}
                Err(e) => tracing::warn!(key, seq, "writing a seq checkpoint failed: {e}"),
            }
        });
    }
}

impl Renumber for DenseSeqs {
    fn anchor(&self, key: i64) -> BoxFuture<'static, anyhow::Result<i64>> {
        Box::pin(self.clone().anchor_at(key))
    }

    fn locate(&self, after: i64) -> BoxFuture<'static, anyhow::Result<(i64, i64)>> {
        Box::pin(self.clone().locate_seq(after))
    }

    fn splice(&self, frame: &[u8], seq: i64, out: &mut Vec<u8>) {
        match super::find_seq(frame) {
            Some(at) => {
                out.extend_from_slice(&frame[..at.start]);
                super::write_int(out, seq);
                out.extend_from_slice(&frame[at.end..]);
            }
            // every logged frame has a seq (`SeqSplice::parse` checks)
            None => out.extend_from_slice(frame),
        }
    }

    fn emitted(&self, keys: &[i64], first: i64, bound: i64) {
        let mut st = self.0.st.lock();
        let Some(mut next) = st.next else { return };
        let mut last = None;
        while next <= bound {
            let n = first - 1 + keys.partition_point(|k| *k <= next) as i64;
            st.known.insert(next, n);
            last = Some((next, n));
            next = boundary_above(next, self.0.every);
        }
        st.next = Some(next);
        drop(st);
        // one write per call: after a stall, the newest boundary covers the rest
        if let Some((k, n)) = last
            && self.0.write
        {
            self.put(k, n);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn boundaries() {
        let every = 10_000_000;
        let b = boundary_above(5 << 8, every);
        assert_eq!(b, (every << 8) | 0xff);
        assert_eq!(boundary_above(b, every), ((2 * every) << 8) | 0xff);
        assert_eq!(boundary_above(b - 1, every), b);
        assert_eq!(parse_name(&format!("{:020}-{:020}", b, 7)), Some((b, 7)));
    }
}
