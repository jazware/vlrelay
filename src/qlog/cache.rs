//! One SlateDB block and metadata cache for every database a node opens:
//! the quorum log's state, and the PLC seeds' writer and reader. Left to
//! itself SlateDB gives each its own (512 MiB of blocks and 128 MiB of
//! metadata), ~2 GiB for three, more than a small box has.
//! `--slatedb-cache-mb` (default 320) is the total, split four to one
//! between blocks and metadata (indexes, filters, stats), which a node
//! touches far more often per byte; `--slatedb-meta-mb` moves the split.
//!
//! With `--slatedb-disk-cache-dir` the SSTs themselves are also kept on
//! local disk (SlateDB's object-store cache, as vlpds runs it): a block or
//! filter the memory cache doesn't hold is a local read instead of a
//! bucket GET. Each database gets its own folder, since every cache runs
//! its own evictor.

use slatedb::db_cache::{DbCache, SplitCache, foyer::FoyerCache, foyer::FoyerCacheOptions};
use std::sync::{Arc, OnceLock};

pub const DEFAULT_MB: u64 = 320;

pub struct Shared {
    pub cache: Arc<dyn DbCache>,
    pub block_bytes: u64,
    pub meta_bytes: u64,
}

impl Shared {
    pub fn new(total_mb: u64) -> Shared {
        Shared::with_meta(total_mb, None)
    }

    /// `meta_mb` of the total for indexes and filters (None: a fifth).
    pub fn with_meta(total_mb: u64, meta_mb: Option<u64>) -> Shared {
        let total = total_mb.max(8) << 20;
        let meta_bytes = meta_mb.map_or(total / 5, |m| (m.max(1) << 20).min(total - (1 << 20)));
        let block_bytes = total - meta_bytes;
        let foyer = |bytes: u64| -> Arc<dyn DbCache> {
            Arc::new(FoyerCache::new_with_opts(FoyerCacheOptions { max_capacity: bytes, ..Default::default() }))
        };
        let cache = Arc::new(
            SplitCache::new().with_block_cache(Some(foyer(block_bytes))).with_meta_cache(Some(foyer(meta_bytes))),
        );
        Shared { cache, block_bytes, meta_bytes }
    }
}

static SHARED: OnceLock<Shared> = OnceLock::new();

/// Sizes the node's cache; only before the first database opens (later
/// calls, and opens before any call, get the default).
pub fn configure(total_mb: u64) -> &'static Shared {
    configure_split(total_mb, None)
}

/// [`configure`] with the metadata share in MiB.
pub fn configure_split(total_mb: u64, meta_mb: Option<u64>) -> &'static Shared {
    SHARED.get_or_init(|| exported(Shared::with_meta(total_mb, meta_mb)))
}

/// Where SSTs are kept on local disk, and how much of it each database
/// may fill.
#[derive(Clone, Debug)]
pub struct Disk {
    pub dir: std::path::PathBuf,
    pub total_bytes: u64,
}

/// The databases a node keeps on disk, with their share of the total. The
/// state is ~5 B a DID the relay has seen against the seeds' ~85 B a PLC op,
/// so it gets the small share.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum DiskDb {
    State,
    Seeds,
}

impl DiskDb {
    fn folder(self) -> &'static str {
        match self {
            DiskDb::State => "qlog_state",
            DiskDb::Seeds => "plc_seeds",
        }
    }

    fn share(self, total: u64) -> u64 {
        let state = (total / 8).max(64 << 20);
        match self {
            DiskDb::State => state,
            DiskDb::Seeds => total.saturating_sub(state).max(64 << 20),
        }
    }
}

static DISK: OnceLock<Option<Disk>> = OnceLock::new();

/// Keeps SSTs under `dir` from the next database opened on; only before
/// the first opens (later calls are ignored).
pub fn configure_disk(dir: Option<std::path::PathBuf>, total_mb: u64) {
    let _ = DISK.set(dir.map(|dir| Disk { dir, total_bytes: total_mb.max(128) << 20 }));
}

/// SlateDB's object-store cache options for `db`: disabled without
/// [`configure_disk`].
pub fn disk_options(db: DiskDb) -> slatedb::config::ObjectStoreCacheOptions {
    let mut o = slatedb::config::ObjectStoreCacheOptions::default();
    if let Some(Some(d)) = DISK.get() {
        o.root_folder = Some(d.dir.join(db.folder()));
        o.max_cache_size_bytes = Some(db.share(d.total_bytes) as usize);
        // what a node writes it reads next; caching it skips the GET
        o.cache_on_flush = true;
        o.cache_on_compaction = true;
    }
    o
}

pub fn shared() -> &'static Shared {
    SHARED.get_or_init(|| exported(Shared::new(DEFAULT_MB)))
}

fn exported(s: Shared) -> Shared {
    slate_metrics::register_cache("node", s.cache.clone());
    s
}

/// The shared cache and the id that keeps `path`'s entries apart from
/// the other databases' in it.
pub fn for_db(path: &str) -> (Arc<dyn DbCache>, u64) {
    let c = shared().cache.clone();
    #[cfg(test)]
    OPENED.lock().push((path.to_string(), Arc::as_ptr(&c) as *const () as usize));
    (c, id(path))
}

/// Tests: (path, cache address) of every database opened.
#[cfg(test)]
pub static OPENED: parking_lot::Mutex<Vec<(String, usize)>> = parking_lot::Mutex::new(Vec::new());

fn id(path: &str) -> u64 {
    use std::hash::{Hash, Hasher};
    let mut h = std::hash::DefaultHasher::new();
    path.hash(&mut h);
    h.finish()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn the_total_is_the_flag_split_four_to_one() {
        let s = Shared::new(100);
        assert_eq!(s.block_bytes + s.meta_bytes, 100 << 20);
        assert_eq!(s.meta_bytes, 20 << 20);
        let d = Shared::new(DEFAULT_MB);
        assert_eq!((d.block_bytes >> 20, d.meta_bytes >> 20), (256, 64));
        let m = Shared::with_meta(320, Some(224));
        assert_eq!((m.block_bytes >> 20, m.meta_bytes >> 20), (96, 224));
        let clamped = Shared::with_meta(64, Some(1000));
        assert_eq!((clamped.block_bytes >> 20, clamped.meta_bytes >> 20), (1, 63));
    }

    #[test]
    fn the_disk_shares_add_up() {
        let t = 16u64 << 30;
        assert_eq!(DiskDb::State.share(t) + DiskDb::Seeds.share(t), t);
        assert_eq!(DiskDb::State.share(t), 2 << 30);
    }

    /// The state, the seeds' writer and their reader all open with the
    /// one shared cache.
    #[tokio::test]
    async fn every_database_shares_the_one_cache() {
        let store = vlsync_store::store::Store::memory(None);
        let st = crate::qlog::state::State::open(&store, "qlog/state-cachetest").await.unwrap();
        let w = crate::plc_seed::SeedWriter::open(&store).await.unwrap();
        w.flush().await.unwrap();
        let r = crate::plc_seed::SeedReader::new(store.clone());
        let _ = r.get("did:plc:aaaaaaaaaaaaaaaaaaaaaaaa").await;
        let want = Arc::as_ptr(&shared().cache) as *const () as usize;
        let opened = OPENED.lock().clone();
        let mine: Vec<&(String, usize)> =
            opened.iter().filter(|(p, _)| p.contains("cachetest") || p.contains("plc/seeds")).collect();
        assert!(mine.len() >= 3, "{opened:?}");
        assert!(mine.iter().all(|(_, a)| *a == want), "{opened:?}");
        drop((st, w));
    }
}
