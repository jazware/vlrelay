//! Archival mode: a full mirror of each archived account's repo, kept in its
//! DID shard's SlateDB next to the sync state (docs/archival.md).
//!
//! The layout is vlpds's as is (`vlpds::state`): records (`R/`) are the
//! source of truth, the record CID index (`c/`) serves getBlocks, interior
//! MST nodes (`M/`) are stored and leaves are rebuilt from records, and the
//! head (`h/`) holds the signed commit. Each mirror's rows sit under a
//! generation, so a bootstrap stages a whole repo where no reader looks and
//! switches to it in one write. `V/{did}` ([`mirror::Meta`]) names the live
//! generation, one being staged and the ones left to sweep.
//!
//! A live commit is applied by the DID owner after `check_chain`, on the
//! repo's stored tree (`mirror::apply_frame`, vlpds's `LazyTree` and the
//! mutations vlpds's own replay derives from a #commit frame). Its rows ride
//! the event's state ticket: staged with it, written when its log entry is
//! durable. The tree stays in memory while it has uncommitted commits, so
//! the next commit of the same account builds on it.
//!
//! A repo that needs a full copy (new to the mirror, archiving switched on
//! for it, or a broken chain) goes to the fetch queue (`fetch`).

pub mod admin;
pub mod fetch;
pub mod mirror;
pub mod read;
pub mod sweep;
pub mod wiring;

#[cfg(test)]
mod tests;

pub use fetch::{FetchLimits, Queue, Resolved, Resolver};
pub use mirror::{Meta, ShardMirror};

use crate::state::{Chain, ShardState, StateStore};
use bytes::Bytes;
use parking_lot::RwLock;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering::Relaxed};
use vlpds::segment::Mutation;

/// The policy's say on archiving, read on every event.
pub trait Gate: Send + Sync {
    /// Whether accounts on `host` are mirrored.
    fn wants(&self, host: &str) -> bool;
    /// Changes when the policy does (the sweeper rescans on a change).
    fn version(&self) -> u64;
    fn takedown_retention_secs(&self) -> u32;
    fn limits(&self, host: &str) -> FetchLimits;
}

/// A fixed answer, for tests and tools.
pub struct StaticGate {
    pub on: bool,
    pub limits: FetchLimits,
    pub retention_secs: u32,
}

impl Gate for StaticGate {
    fn wants(&self, _host: &str) -> bool {
        self.on
    }
    fn version(&self) -> u64 {
        self.on as u64
    }
    fn takedown_retention_secs(&self) -> u32 {
        self.retention_secs
    }
    fn limits(&self, _host: &str) -> FetchLimits {
        self.limits
    }
}

#[derive(Default)]
pub struct Stats {
    pub applied: AtomicU64,
    pub applied_us: AtomicU64,
    pub skipped: AtomicU64,
    pub buffered: AtomicU64,
    /// The stored tree disagreed with a commit that passed the sync 1.1
    /// checks.
    pub mismatches: AtomicU64,
    pub stale: AtomicU64,
    pub replayed: AtomicU64,
    pub deleted_repos: AtomicU64,
    pub deleted_rows: AtomicU64,
    /// Live mirrors on this node's shards, as the last sweep counted them.
    pub mirrors: AtomicU64,
    /// When that sweep finished (unix ms; 0: none yet).
    pub swept_at_ms: AtomicU64,
}

#[derive(Default)]
pub struct ReadStats {
    pub exports: AtomicU64,
    pub export_us: AtomicU64,
}

pub struct Archive {
    gate: RwLock<Arc<dyn Gate>>,
    pub queue: Arc<Queue>,
    pub stats: Stats,
    pub reads: ReadStats,
}

/// What one event did to the mirror. Rows ride the event's ticket.
pub enum Step {
    Rows(Vec<Mutation>),
    None,
}

impl Archive {
    pub fn new(gate: Arc<dyn Gate>, resolver: Arc<dyn Resolver>) -> Arc<Archive> {
        Arc::new(Archive {
            gate: RwLock::new(gate),
            queue: Queue::new(resolver),
            stats: Stats::default(),
            reads: ReadStats::default(),
        })
    }

    pub fn gate(&self) -> Arc<dyn Gate> {
        self.gate.read().clone()
    }

    pub fn set_gate(&self, g: Arc<dyn Gate>) {
        *self.gate.write() = g;
    }

    /// Starts the fetch workers and the sweeper.
    pub fn spawn<C: Chain>(self: &Arc<Self>, state: Arc<StateStore<C>>) {
        fetch::spawn_workers(self.clone(), state.clone());
        sweep::spawn(self.clone(), state);
    }
}

impl<C: Chain> StateStore<C> {
    pub fn set_archive(&self, a: Arc<Archive>) {
        let _ = self.archive_cell().set(a);
    }

    pub fn archive(&self) -> Option<&Arc<Archive>> {
        self.archive_cell().get()
    }

    /// The mirror's part of an accepted #commit, after `check_chain`. The
    /// caller holds the DID's lock and stages the rows with the event's
    /// ticket.
    pub(crate) async fn archive_commit(&self, shard: &ShardState, did: &str, host: &str, frame: &Bytes) -> Step {
        let Some(a) = self.archive() else { return Step::None };
        if !a.gate().wants(host) {
            return Step::None;
        }
        if a.queue.buffer(did, frame) {
            a.stats.buffered.fetch_add(1, Relaxed);
            return Step::None;
        }
        let t0 = std::time::Instant::now();
        let r = mirror::apply_live(shard, did, frame.clone()).await;
        match r {
            Ok(mirror::Applied::Rows(rows)) => {
                a.stats.applied.fetch_add(1, Relaxed);
                a.stats.applied_us.fetch_add(t0.elapsed().as_micros() as u64, Relaxed);
                Step::Rows(rows)
            }
            Ok(mirror::Applied::Skip) => {
                a.stats.skipped.fetch_add(1, Relaxed);
                Step::None
            }
            Ok(mirror::Applied::Absent) => {
                a.queue.enqueue(did, host, fetch::Why::New);
                a.queue.buffer(did, frame);
                Step::None
            }
            Ok(mirror::Applied::Stale) => {
                a.stats.stale.fetch_add(1, Relaxed);
                a.queue.enqueue(did, host, fetch::Why::Chain);
                a.queue.buffer(did, frame);
                Step::None
            }
            Err(e) => {
                a.stats.mismatches.fetch_add(1, Relaxed);
                tracing::warn!(%did, "archive: {e:#}; re-fetching");
                a.queue.enqueue(did, host, fetch::Why::Mismatch);
                Step::None
            }
        }
    }

    /// A #sync for an archived account: a new head for the same tree is
    /// written with the event; anything else needs a fresh copy.
    pub(crate) async fn archive_sync(&self, shard: &ShardState, did: &str, host: &str, frame: &Bytes) -> Step {
        let Some(a) = self.archive() else { return Step::None };
        if !a.gate().wants(host) {
            return Step::None;
        }
        if a.queue.buffer(did, frame) {
            return Step::None;
        }
        match mirror::apply_sync(shard, did, frame).await {
            Ok(Some(rows)) => Step::Rows(rows),
            Ok(None) => Step::None,
            Err(e) => {
                tracing::debug!(%did, "archive: #sync: {e:#}");
                a.queue.enqueue(did, host, fetch::Why::Sync);
                Step::None
            }
        }
    }

    /// A broken chain (prevData mismatch, or a commit from a desynchronized
    /// account): an archiving relay fetches the repo, which also heals the
    /// sync state.
    pub(crate) fn archive_chain_broken(&self, did: &str, host: &str) {
        if let Some(a) = self.archive()
            && a.gate().wants(host)
        {
            a.queue.enqueue(did, host, fetch::Why::Chain);
        }
    }

    /// Replays logged #commit and #sync frames into the mirrors of shard
    /// `s` (a new owner's recovery). Writes directly: nothing else runs on
    /// the shard yet.
    pub(crate) async fn archive_replay(&self, s: &ShardState, frames: Vec<(String, Bytes)>) -> anyhow::Result<usize> {
        let Some(a) = self.archive() else { return Ok(0) };
        let mut n = 0;
        for (did, frame) in frames {
            let _g = s.lock_did(&did).await;
            match mirror::replay_frame(s, &did, frame).await {
                Ok(true) => n += 1,
                Ok(false) => {}
                Err(e) => {
                    tracing::debug!(%did, "archive replay: {e:#}; re-fetching");
                    let host = self.host_of(s, &did).await.unwrap_or_default();
                    a.queue.enqueue(&did, &host, fetch::Why::Chain);
                }
            }
        }
        a.stats.replayed.fetch_add(n as u64, Relaxed);
        Ok(n)
    }

    /// The account's host name, from its sync record.
    pub(crate) async fn host_of(&self, s: &ShardState, did: &str) -> Option<String> {
        let rec = s.load(did).await.ok()??;
        self.host_name(rec.pds.unwrap_or(rec.host)).map(|h| h.to_string())
    }
}
