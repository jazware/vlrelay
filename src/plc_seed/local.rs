//! A member's local copy of the seeds ([`SeedTable`]), so a seed lookup is
//! a local read instead of an LSM's filters, indexes and blocks from the
//! bucket.
//!
//! The seed database in the bucket stays the one durable, shared copy: the
//! leader writes it as before, and every row it writes is also kept for
//! [`CHANGELOG_TTL`] under the time it was written. Each member (the leader
//! too) builds its table from one scan of the database, then follows the
//! changelog from a cursor. A member that finds its cursor older than the
//! changelog keeps rebuilds. Until its table is ready, lookups read the
//! database as before.

use super::table::{Builder, META_VERSION, Meta, SeedTable};
use super::{CHANGELOG_TTL, Seed, SeedReader, changelog_key, split_changelog_key};
use parking_lot::RwLock;
use std::path::PathBuf;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering::Relaxed};
use std::time::{Duration, Instant};

/// How often a member reads the changelog.
pub const TAIL_EVERY: Duration = Duration::from_secs(10);
/// How far before its cursor a member reads again: a row becomes visible
/// to a reader only once the leader has flushed it and the reader has
/// polled the manifest (~10 s and 30 s), and its time is the leader's
/// clock. Rows read twice are harmless (newest wins).
pub const OVERLAP: Duration = Duration::from_secs(600);

/// Called with each changelog row a member applies (the identity cache
/// drops documents an op made stale).
pub type OnRow = Arc<dyn Fn(&str, &Seed) + Send + Sync>;

#[derive(Default, Debug)]
pub struct Stats {
    pub builds: AtomicU64,
    pub build_rows: AtomicU64,
    pub build_ms: AtomicU64,
    pub tailed: AtomicU64,
    pub cursor_ms: AtomicU64,
    pub errors: AtomicU64,
}

pub struct Local {
    pub dir: PathBuf,
    /// The seed database's bucket path: a table built from another is
    /// rebuilt.
    pub source: String,
    table: RwLock<Option<Arc<SeedTable>>>,
    pub stats: Stats,
}

impl Local {
    pub fn new(dir: PathBuf, source: String) -> Arc<Local> {
        Arc::new(Local { dir, source, table: Default::default(), stats: Default::default() })
    }

    pub fn table(&self) -> Option<Arc<SeedTable>> {
        self.table.read().clone()
    }

    /// Opens the table already in the directory, whatever its cursor.
    pub async fn open_existing(&self) -> anyhow::Result<()> {
        let dir = self.dir.clone();
        let t = tokio::task::spawn_blocking(move || SeedTable::open(&dir)).await??;
        *self.table.write() = Some(Arc::new(t));
        Ok(())
    }

    /// The rows the leader just applied, so its own lookups see them before
    /// the changelog comes round.
    pub async fn apply(&self, rows: Vec<(String, Seed)>) {
        let Some(t) = self.table() else { return };
        let r = tokio::task::spawn_blocking(move || {
            for (did, s) in &rows {
                t.put(did, s)?;
            }
            std::io::Result::Ok(())
        })
        .await;
        if !matches!(r, Ok(Ok(()))) {
            self.stats.errors.fetch_add(1, Relaxed);
        }
    }

    fn usable_meta(&self, now_ms: u64) -> Option<Meta> {
        Meta::load(&self.dir).filter(|m| {
            m.source == self.source
                && now_ms.saturating_sub(m.cursor_ms) + OVERLAP.as_millis() as u64 * 2
                    < CHANGELOG_TTL.as_millis() as u64
        })
    }

    /// Builds or opens the table, then follows the changelog until the
    /// process ends.
    pub async fn run(self: Arc<Self>, seeds: Arc<SeedReader>, on_row: Option<OnRow>) {
        loop {
            match self.clone().step(&seeds, on_row.clone()).await {
                Ok(()) => tokio::time::sleep(TAIL_EVERY).await,
                Err(e) => {
                    self.stats.errors.fetch_add(1, Relaxed);
                    tracing::warn!("PLC seed table: {e:#}");
                    tokio::time::sleep(TAIL_EVERY * 3).await;
                }
            }
        }
    }

    /// One round: build or open the table if it has none, else read the
    /// changelog once.
    pub async fn step(self: Arc<Self>, seeds: &SeedReader, on_row: Option<OnRow>) -> anyhow::Result<()> {
        let now = crate::policy::store::now_ms() as u64;
        let Some(t) = self.table() else {
            match self.usable_meta(now) {
                Some(m) => {
                    let dir = self.dir.clone();
                    let t = tokio::task::spawn_blocking(move || SeedTable::open(&dir)).await??;
                    tracing::info!(rows = t.len(), cursor_ms = m.cursor_ms, "PLC seed table: opened");
                    self.stats.cursor_ms.store(m.cursor_ms, Relaxed);
                    *self.table.write() = Some(Arc::new(t));
                }
                None => self.build(seeds).await?,
            }
            return Ok(());
        };
        if self.usable_meta(now).is_none() {
            // fell behind the changelog: what it missed is gone from it
            tracing::warn!("PLC seed table: behind the changelog; rebuilding");
            *self.table.write() = None;
            drop(t);
            return self.build(seeds).await;
        }
        self.tail(t, seeds, on_row).await
    }

    async fn tail(&self, t: Arc<SeedTable>, seeds: &SeedReader, on_row: Option<OnRow>) -> anyhow::Result<()> {
        let cursor = self.stats.cursor_ms.load(Relaxed);
        let from = changelog_key(cursor.saturating_sub(OVERLAP.as_millis() as u64), b"");
        let Some(mut it) = seeds.scan(from..b"d".to_vec()).await? else { return Ok(()) };
        let mut rows = Vec::new();
        let mut seen = cursor;
        while let Some(kv) = it.next().await? {
            let Some((ms, key)) = split_changelog_key(&kv.key) else { continue };
            let Ok(seed) = Seed::decode(&kv.value) else { continue };
            seen = seen.max(ms);
            rows.push((key.to_vec(), seed));
        }
        let n = rows.len() as u64;
        if let Some(f) = &on_row {
            for (k, s) in &rows {
                if let Some(did) = did_of(k) {
                    f(&did, s);
                }
            }
        }
        let t2 = t.clone();
        tokio::task::spawn_blocking(move || {
            for (k, s) in &rows {
                t2.put_key(k, s)?;
            }
            t2.sync()
        })
        .await??;
        let m = Meta { version: META_VERSION, source: self.source.clone(), cursor_ms: seen, built_ms: 0 };
        let dir = self.dir.clone();
        let built = Meta::load(&dir).map_or(0, |m| m.built_ms);
        tokio::task::spawn_blocking(move || Meta { built_ms: built, ..m }.store(&dir)).await??;
        self.stats.cursor_ms.store(seen, Relaxed);
        self.stats.tailed.fetch_add(n, Relaxed);
        Ok(())
    }

    /// A new table from one scan of the database. The old one is removed
    /// first: two copies at once would double the disk the table needs.
    pub async fn build(&self, seeds: &SeedReader) -> anyhow::Result<()> {
        let started = Instant::now();
        // rows written from here on are in the changelog
        let cursor = crate::policy::store::now_ms() as u64;
        let reader = seeds.scan_reader().await?;
        let p = reader.scan_with_options(b"p".to_vec()..b"q".to_vec(), &super::scan_options()).await?;
        let w = reader.scan_with_options(b"w".to_vec()..b"x".to_vec(), &super::scan_options()).await?;
        let dir = self.dir.clone();
        let _ = std::fs::remove_dir_all(&dir);
        std::fs::create_dir_all(&dir)?;
        let (tx, mut rx) = tokio::sync::mpsc::channel::<Vec<(Vec<u8>, Seed)>>(16);
        let builder = tokio::task::spawn_blocking(move || -> std::io::Result<SeedTable> {
            let mut b = Builder::new(&dir)?;
            while let Some(batch) = rx.blocking_recv() {
                for (k, s) in &batch {
                    b.push(k, s)?;
                }
            }
            b.finish()
        });
        let mut batch = Vec::with_capacity(4096);
        let mut rows = 0u64;
        for mut it in [p, w] {
            while let Some(kv) = it.next().await? {
                if let Ok(s) = Seed::decode(&kv.value) {
                    batch.push((kv.key.to_vec(), s));
                }
                if batch.len() >= 4096 {
                    rows += batch.len() as u64;
                    if tx.send(std::mem::take(&mut batch)).await.is_err() {
                        break;
                    }
                }
            }
        }
        rows += batch.len() as u64;
        let _ = tx.send(batch).await;
        drop(tx);
        let t = builder.await??;
        let _ = reader.close().await;
        let m = Meta { version: META_VERSION, source: self.source.clone(), cursor_ms: cursor, built_ms: cursor };
        let dir = self.dir.clone();
        tokio::task::spawn_blocking(move || m.store(&dir)).await??;
        let ms = started.elapsed().as_millis() as u64;
        tracing::info!(rows, table_rows = t.len(), disk_mb = t.disk_bytes() >> 20, ms, "PLC seed table: built");
        self.stats.builds.fetch_add(1, Relaxed);
        self.stats.build_rows.store(rows, Relaxed);
        self.stats.build_ms.store(ms, Relaxed);
        self.stats.cursor_ms.store(cursor, Relaxed);
        *self.table.write() = Some(Arc::new(t));
        Ok(())
    }
}

/// The DID a seed key names.
pub fn did_of(key: &[u8]) -> Option<String> {
    match key.split_first()? {
        (b'p', id) if id.len() == 15 => {
            const B32: &[u8] = b"abcdefghijklmnopqrstuvwxyz234567";
            let mut acc: u128 = 0;
            for &b in id {
                acc = (acc << 8) | b as u128;
            }
            let s: String = (0..24).map(|i| B32[((acc >> (5 * (23 - i))) & 31) as usize] as char).collect();
            Some(format!("did:plc:{s}"))
        }
        (b'w', rest) => Some(format!("did:{}", std::str::from_utf8(rest).ok()?)),
        _ => None,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::plc_seed::{SeedReader, SeedWriter};
    use bytes::Bytes;

    fn seed(i: u8, ms: u64) -> Seed {
        let mut k = vec![0xe7, 0x01, 2];
        k.extend([i; 32]);
        Seed {
            created_ms: ms,
            tombstone: false,
            key: Some(Bytes::from(k)),
            pds: Some(format!("pds{i}.example.com")),
            pds_http: false,
            lookup: false,
        }
    }

    fn did(i: u8) -> String {
        format!("did:plc:{}{}", "abcdefghijklmnopqrstuvwxyz".chars().nth(i as usize % 26).unwrap(), "a".repeat(23))
    }

    /// A member builds its table from the database, follows rows written
    /// after, and reads them locally; a reopen resumes from its cursor.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn a_member_builds_then_follows_the_changelog() {
        let store = vlsync_store::store::Store::memory(None);
        let w = Arc::new(SeedWriter::open(&store).await.unwrap());
        w.apply((0..10).map(|i| (did(i), seed(i, 1000 + i as u64))).collect()).await.unwrap();
        w.apply(vec![("did:web:a.example".into(), seed(99, 5))]).await.unwrap();
        w.flush().await.unwrap();

        let dir = tempfile::tempdir().unwrap();
        let seeds = SeedReader::new(store.clone());
        seeds.set_writer(w.clone()).await;
        let local = Local::new(dir.path().join("seeds"), seeds.source());
        let _ = seeds.local.set(local.clone());
        local.clone().step(&seeds, None).await.unwrap();
        let t = local.table().expect("built");
        assert_eq!(t.len(), 11);
        assert_eq!(seeds.get(&did(3)).await, Some(seed(3, 1003)));
        assert_eq!(seeds.get("did:web:a.example").await, Some(seed(99, 5)));

        // a newer op and a new DID, through the changelog
        w.apply(vec![(did(3), seed(33, 9000)), (did(20), seed(20, 9001))]).await.unwrap();
        w.flush().await.unwrap();
        let seen = Arc::new(parking_lot::Mutex::new(Vec::new()));
        let s2 = seen.clone();
        let on_row: OnRow = Arc::new(move |d, _| s2.lock().push(d.to_string()));
        local.clone().step(&seeds, Some(on_row)).await.unwrap();
        assert_eq!(seeds.get(&did(3)).await, Some(seed(33, 9000)));
        assert_eq!(seeds.get(&did(20)).await, Some(seed(20, 9001)));
        assert!(seen.lock().contains(&did(20)));
        assert!(local.stats.cursor_ms.load(Relaxed) > 0);

        // a restart opens the table where it was
        let again = Local::new(dir.path().join("seeds"), seeds.source());
        again.clone().step(&seeds, None).await.unwrap();
        assert_eq!(again.stats.builds.load(Relaxed), 0);
        assert_eq!(again.table().unwrap().get(&did(20)).unwrap(), Some(seed(20, 9001)));

        // another database's table is rebuilt
        let other = Local::new(dir.path().join("seeds"), "elsewhere".into());
        other.clone().step(&seeds, None).await.unwrap();
        assert_eq!(other.stats.builds.load(Relaxed), 1);
        w.close().await;
    }
    #[test]
    fn a_seed_key_names_its_did() {
        for d in ["did:plc:ragtjsm2j2vknwkz3zp4oxrd", "did:plc:aaaaaaaaaaaaaaaaaaaaaaaa", "did:web:example.com"] {
            assert_eq!(super::did_of(&super::super::seed_key(d)).as_deref(), Some(d));
        }
    }
}
