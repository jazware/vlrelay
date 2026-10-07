//! One SlateDB block and metadata cache for every database a node opens:
//! the quorum log's state, and the PLC seeds' writer and reader. Left to
//! itself SlateDB gives each its own (512 MiB of blocks and 128 MiB of
//! metadata), ~2 GiB for three, more than a small box has.
//! `--slatedb-cache-mb` (default 320) is the total, split four to one
//! between blocks and metadata (indexes, filters, stats), which a node
//! touches far more often per byte.

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
        let total = total_mb.max(8) << 20;
        let meta_bytes = total / 5;
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
    SHARED.get_or_init(|| Shared::new(total_mb))
}

pub fn shared() -> &'static Shared {
    SHARED.get_or_init(|| Shared::new(DEFAULT_MB))
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
    }

    /// The state, the seeds' writer and their reader all open with the
    /// one shared cache.
    #[tokio::test]
    async fn every_database_shares_the_one_cache() {
        let store = vlpds::store::Store::memory(None);
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
