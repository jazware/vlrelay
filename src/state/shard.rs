//! One DID shard's SlateDB plus the in-memory view the DID owner checks
//! against.
//!
//! An applied event changes the in-memory record at once, so the DID's next
//! event is checked against it, but reaches SlateDB only when its log entry
//! is durable (`commit`). Writing earlier would let a memtable flush persist
//! state for an event the log never got: after a crash the host replays it,
//! the DID owner sees "same commit at the current rev", acks it as a
//! duplicate, and the event never reaches the firehose.

use super::record::{self, Record};
use crate::types::FastMap;
use bytes::Bytes;
use parking_lot::Mutex;
use slatedb::Db;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering::Relaxed};
use vlpds::slots::ShardId;

/// Names one applied event's pending write; the log carries it to `commit`.
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
    /// Archival mode's in-memory side (`crate::archive`).
    pub mirror: crate::archive::ShardMirror,
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
            mirror: Default::default(),
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

    /// Writes the records of `tickets` (all durable in the log, in log
    /// order) to the memtable in one batch: what the log finalizer calls
    /// before it acks them. Unknown tickets (already committed) are skipped.
    pub async fn commit(&self, tickets: impl IntoIterator<Item = u64>) -> Result<usize, slatedb::Error> {
        let mut rows: FastMap<Arc<str>, Arc<Record>> = FastMap::default();
        let mut settled = Vec::new();
        let mut mirror_rows = Vec::new();
        let mut mirrored = Vec::new();
        {
            let mut p = self.pending.lock();
            for t in tickets {
                if let Some((did, m)) = self.mirror.take_ticket(t) {
                    mirror_rows.extend(m);
                    mirrored.push(did);
                }
                let Some((did, snap)) = p.by_ticket.remove(&t) else { continue };
                let slot = p.by_did.get_mut(&did).expect("a ticket's DID is pending");
                slot.outstanding -= 1;
                if slot.outstanding == 0 {
                    rows.insert(did.clone(), slot.rec.clone());
                    settled.push((did, slot.rec.clone()));
                } else {
                    rows.insert(did, snap);
                }
            }
        }
        let n = rows.len();
        if n == 0 && mirror_rows.is_empty() {
            return Ok(0);
        }
        self.write_rows_with(rows, mirror_rows).await?;
        for did in mirrored {
            self.mirror.settle(&did);
        }
        self.settle(settled);
        self.stats.committed.fetch_add(n as u64, Relaxed);
        Ok(n)
    }

    /// Writes staged changes that no log entry carries, for DIDs with no
    /// uncommitted logged change.
    pub async fn flush_unlogged(&self) -> Result<usize, slatedb::Error> {
        let rows: FastMap<Arc<str>, Arc<Record>> = {
            let p = self.pending.lock();
            p.by_did
                .iter()
                .filter(|(_, s)| s.outstanding == 0 && s.dirty)
                .map(|(d, s)| (d.clone(), s.rec.clone()))
                .collect()
        };
        let n = rows.len();
        if n == 0 {
            return Ok(0);
        }
        let settled: Vec<_> = rows.iter().map(|(d, r)| (d.clone(), r.clone())).collect();
        self.write_rows(rows).await?;
        self.settle(settled);
        Ok(n)
    }

    async fn write_rows(&self, rows: FastMap<Arc<str>, Arc<Record>>) -> Result<(), slatedb::Error> {
        self.write_rows_with(rows, Vec::new()).await
    }

    /// Sync records and mirror rows in one batch, the mirror's in log order.
    async fn write_rows_with(
        &self,
        rows: FastMap<Arc<str>, Arc<Record>>,
        mirror: Vec<vlpds::segment::Mutation>,
    ) -> Result<(), slatedb::Error> {
        let mut wb = slatedb::WriteBatch::new();
        for (did, rec) in rows {
            wb.put(record::did_key(&did), rec.encode());
        }
        for m in mirror {
            match m.val {
                Some(v) => wb.put(&m.key, &v),
                None => wb.delete(&m.key),
            }
        }
        self.db.write(wb).await.map(|_| ())
    }

    /// Moves written records from pending to the cache, unless a newer
    /// change was staged while the batch was in flight.
    fn settle(&self, written: Vec<(Arc<str>, Arc<Record>)>) {
        let mut p = self.pending.lock();
        for (did, rec) in written {
            let done = p.by_did.get(&did).is_some_and(|s| s.outstanding == 0 && Arc::ptr_eq(&s.rec, &rec));
            if done {
                p.by_did.remove(&did);
                self.cache[Self::way(&did) % CACHE_WAYS].lock().put(did, rec);
            } else if let Some(s) = p.by_did.get_mut(&did)
                && s.outstanding == 0
                && !Arc::ptr_eq(&s.rec, &rec)
            {
                s.dirty = true;
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

    /// Writes `rec` straight to the memtable (replay and operator edits:
    /// nothing outstanding to order against). Drops any cached copy.
    pub async fn put_direct(&self, did: &str, rec: Record) -> Result<(), slatedb::Error> {
        let rec = Arc::new(rec);
        self.db.put(record::did_key(did), rec.encode()).await?;
        self.cache[Self::way(did) % CACHE_WAYS].lock().put(Arc::from(did), rec);
        Ok(())
    }

    pub async fn put_raw(&self, key: Vec<u8>, value: Bytes) -> Result<(), slatedb::Error> {
        self.db.put(key, value).await.map(|_| ())
    }

    pub async fn flush_memtable(&self) -> Result<(), slatedb::Error> {
        self.db
            .flush_with_options(slatedb::config::FlushOptions { flush_type: slatedb::config::FlushType::MemTable })
            .await
    }

    /// The last log ordinal of `log_id` applied before the last checkpoint.
    pub async fn applied_marker(&self, log_id: &str) -> Result<Option<u64>, slatedb::Error> {
        Ok(self
            .db
            .get(record::applied_key(log_id))
            .await?
            .and_then(|b| b.as_ref().try_into().ok().map(u64::from_be_bytes)))
    }

    /// Records that every entry of `log_id` up to `ord` is committed here,
    /// then flushes, so the next owner replays from just after it. The
    /// caller has committed every ticket of those entries.
    pub async fn checkpoint(&self, log_id: &str, ord: u64) -> Result<(), slatedb::Error> {
        self.flush_unlogged().await?;
        self.db.put(record::applied_key(log_id), ord.to_be_bytes().to_vec()).await?;
        self.flush_memtable().await
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
