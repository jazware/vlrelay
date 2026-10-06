//! The quorum leader's view of the records: the state's SlateDB as the
//! log's applier wrote it, plus what this term decided and the applier
//! hasn't reached.
//!
//! A decided event changes the in-memory record at once, so the DID's next
//! event is checked against it. Nothing here writes the database: the
//! applier writes each committed entry's record, and the staged one is
//! then released to the cache (`release`).

use super::record::{self, Record};
use crate::types::FastMap;
use bytes::Bytes;
use parking_lot::Mutex;
use slatedb::Db;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering::Relaxed};
use vlpds::slots::ShardId;

/// Names one decided event's staged record, until the applier has it.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub struct Ticket {
    pub shard: ShardId,
    pub n: u64,
}

struct Slot {
    rec: Arc<Record>,
    /// Logged changes not yet committed. While any are, the record holds
    /// state the log may still lose, so only their snapshots are written.
    outstanding: u32,
    /// Holds a change no log entry carries (a failed-check counter, a
    /// desync mark), written once nothing is outstanding.
    dirty: bool,
}

#[derive(Default)]
struct Pending {
    by_did: FastMap<Arc<str>, Slot>,
    by_ticket: FastMap<u64, (Arc<str>, Arc<Record>)>,
}

type RecordLru = lru::LruCache<Arc<str>, Arc<Record>>;

const CACHE_WAYS: usize = 16;
const STRIPES: usize = 1024;

pub struct ShardState {
    pub id: ShardId,
    /// Slots [lo, hi).
    pub lo: u32,
    pub hi: u32,
    pub db: Arc<Db>,
    cache: Box<[Mutex<RecordLru>]>,
    pending: Mutex<Pending>,
    /// Serializes each DID's applies (a DID's events must apply in order,
    /// and the identity lookup inside one is async).
    stripes: Box<[Arc<tokio::sync::Mutex<()>>]>,
    next_ticket: AtomicU64,
    pub stats: ShardStats,
}

#[derive(Default)]
pub struct ShardStats {
    pub loads: AtomicU64,
    pub load_misses: AtomicU64,
    pub cache_hits: AtomicU64,
    pub committed: AtomicU64,
}

impl ShardState {
    pub fn new(id: ShardId, lo: u32, hi: u32, db: Arc<Db>, cache_entries: usize) -> ShardState {
        let per_way = std::num::NonZeroUsize::new((cache_entries / CACHE_WAYS).max(1)).expect("nonzero");
        ShardState {
            id,
            lo,
            hi,
            db,
            cache: (0..CACHE_WAYS).map(|_| Mutex::new(lru::LruCache::new(per_way))).collect(),
            pending: Mutex::new(Pending::default()),
            stripes: (0..STRIPES).map(|_| Arc::new(tokio::sync::Mutex::new(()))).collect(),
            next_ticket: AtomicU64::new(1),
            stats: ShardStats::default(),
        }
    }

    fn way(did: &str) -> usize {
        // FxHash-like: the slot hash already spread DIDs across shards, this
        // only spreads them across a shard's locks.
        let mut h: u64 = 0xcbf29ce484222325;
        for b in did.bytes() {
            h = (h ^ b as u64).wrapping_mul(0x100000001b3);
        }
        h as usize
    }

    pub async fn lock_did(&self, did: &str) -> tokio::sync::MutexGuard<'_, ()> {
        self.stripes[Self::way(did) % STRIPES].lock().await
    }

    /// The locks of every DID in `dids`, taken in stripe order (so two
    /// callers can't deadlock), held until the guards drop: the quorum
    /// log's leader holds a batch's DIDs from deciding to appending.
    pub async fn lock_dids_owned<'a>(
        &self,
        dids: impl IntoIterator<Item = &'a str>,
    ) -> Vec<tokio::sync::OwnedMutexGuard<()>> {
        let mut idx: Vec<usize> = dids.into_iter().map(|d| Self::way(d) % STRIPES).collect();
        idx.sort_unstable();
        idx.dedup();
        let mut out = Vec::with_capacity(idx.len());
        for i in idx {
            out.push(self.stripes[i].clone().lock_owned().await);
        }
        out
    }

    /// The current record: pending, then cache, then SlateDB.
    pub async fn load(&self, did: &str) -> Result<Option<Arc<Record>>, slatedb::Error> {
        if let Some(s) = self.pending.lock().by_did.get(did) {
            return Ok(Some(s.rec.clone()));
        }
        let way = &self.cache[Self::way(did) % CACHE_WAYS];
        if let Some(r) = way.lock().get(did) {
            self.stats.cache_hits.fetch_add(1, Relaxed);
            return Ok(Some(r.clone()));
        }
        self.stats.loads.fetch_add(1, Relaxed);
        let Some(raw) = self.db.get(record::did_key(did)).await? else {
            self.stats.load_misses.fetch_add(1, Relaxed);
            return Ok(None);
        };
        let rec = match Record::decode(&raw) {
            Ok(r) => Arc::new(r),
            Err(e) => {
                tracing::error!(%did, shard = %self.id, "{e}");
                return Err(slatedb::Error::data(format!("{e} for {did}")));
            }
        };
        // a concurrent apply may have staged a newer one meanwhile
        if let Some(s) = self.pending.lock().by_did.get(did) {
            return Ok(Some(s.rec.clone()));
        }
        way.lock().put(Arc::from(did), rec.clone());
        Ok(Some(rec))
    }

    /// Stages a change the log will carry. The caller holds the DID's lock.
    pub fn stage_logged(&self, did: &str, rec: Record) -> Ticket {
        let n = self.next_ticket.fetch_add(1, Relaxed);
        let rec = Arc::new(rec);
        let mut p = self.pending.lock();
        let did: Arc<str> = match p.by_did.get_key_value(did) {
            Some((k, _)) => k.clone(),
            None => Arc::from(did),
        };
        p.by_ticket.insert(n, (did.clone(), rec.clone()));
        let slot = p.by_did.entry(did).or_insert_with(|| Slot { rec: rec.clone(), outstanding: 0, dirty: false });
        slot.rec = rec;
        slot.outstanding += 1;
        Ticket { shard: self.id, n }
    }

    /// Stages a change no log entry carries; `flush_unlogged` writes it.
    pub fn stage_unlogged(&self, did: &str, rec: Record) {
        let rec = Arc::new(rec);
        let mut p = self.pending.lock();
        match p.by_did.get_mut(did) {
            Some(s) => {
                s.rec = rec;
                s.dirty = true;
            }
            None => {
                p.by_did.insert(Arc::from(did), Slot { rec, outstanding: 0, dirty: true });
            }
        }
    }

    /// The quorum log's leader: these tickets' entries are applied, by the
    /// log's applier from the records their meta carried, so their records
    /// leave pending for the cache without being written here. A DID with
    /// an unlogged change on top keeps it in memory only. The cache takes
    /// a record before pending lets it go, under pending's lock: a load
    /// between the two would find neither and read an older one.
    pub fn release(&self, tickets: impl IntoIterator<Item = u64>) {
        let mut p = self.pending.lock();
        for t in tickets {
            let Some((did, _)) = p.by_ticket.remove(&t) else { continue };
            let slot = p.by_did.get_mut(&did).expect("a ticket's DID is pending");
            slot.outstanding -= 1;
            if slot.outstanding == 0 {
                let rec = slot.rec.clone();
                self.cache[Self::way(&did) % CACHE_WAYS].lock().put(did.clone(), rec);
                p.by_did.remove(&did);
            }
        }
    }

    /// The quorum log's leader: unlogged changes (failed-check counts,
    /// desync marks) of DIDs with nothing outstanding go to the cache, not
    /// the database: only the log's applier writes there.
    pub fn settle_unlogged_in_memory(&self) {
        let mut p = self.pending.lock();
        let ds: Vec<Arc<str>> = p.by_did.iter().filter(|(_, s)| s.outstanding == 0).map(|(d, _)| d.clone()).collect();
        for d in ds {
            if let Some(s) = p.by_did.remove(&d) {
                self.cache[Self::way(&d) % CACHE_WAYS].lock().put(d, s.rec);
            }
        }
    }

    pub fn pending_len(&self) -> (usize, usize) {
        let p = self.pending.lock();
        (p.by_did.len(), p.by_ticket.len())
    }

    /// The live SSTs' bytes in the bucket (L0 plus compacted).
    pub fn sst_bytes(&self) -> u64 {
        let m = self.db.manifest();
        m.l0().iter().map(|v| v.estimate_size()).sum::<u64>()
            + m.compacted().iter().flat_map(|r| r.sst_views().iter()).map(|v| v.estimate_size()).sum::<u64>()
    }

    pub fn range_keys(&self) -> (Bytes, Bytes) {
        vlpds::state::slot_range_keys(self.lo, self.hi)
    }

    pub fn contains_slot(&self, slot: u16) -> bool {
        (self.lo..self.hi).contains(&(slot as u32))
    }
}
