//! The state the leader applies committed entries to (docs/quorum.md §2),
//! sealed at exactly a flush point F: one SlateDB over the bucket at
//! `qlog/state`, WAL off, written only by the current leader (opening it
//! fences the previous one's writer).
//!
//! What an entry writes is the leader's to decide (`log::Meta::writes`): the
//! relay's DID records and host table, keyed as the relay reads them. A
//! bare test frame (no meta) writes the seq and content of its DID's last
//! event (`d/{did}`). Every entry's host cursors are kept per host
//! (`c/{host}`), and the last seq applied as `_applied`. Each applied batch
//! is written with SlateDB seqnum = its last log seq, so a manifest's
//! `last_l0_seq` *is* the log seq its state reaches.
//!
//! Exactly F: SlateDB only promises a flush or checkpoint holds *at least*
//! the writes issued before it. The flush tracker targets the newest frozen
//! memtable when it runs, and the manifest writer folds every contiguous
//! uploaded memtable into one manifest, so writes after F can share F's
//! manifest. The applier is this state's only writer and it stops at F
//! until the checkpoint exists: nothing above F is in the database while
//! the checkpoint is taken. [`State::seal`] then reads the checkpoint's
//! manifest back and refuses one whose `last_l0_seq` isn't F.

use super::check::content_id;
use super::client::parse_test_frame;
use super::log::{Entry, Meta, decode_cursors};
use bytes::Bytes;
use serde::{Deserialize, Serialize};
use slatedb::config::{CheckpointOptions, CheckpointScope, WriteOptions};
use slatedb::{Db, DbReader, DbReaderMode, WriteBatch};
use std::collections::BTreeMap;
use std::time::Duration;
use vlsync_store::store::Store;

const APPLIED: &[u8] = b"_applied";

/// Where the state lives until a bucket recovery clones it elsewhere.
pub const DEFAULT_PATH: &str = "qlog/state";

fn default_path() -> String {
    DEFAULT_PATH.into()
}

/// The state a manifest names: a checkpoint of the SlateDB at `path`
/// (under the store's prefix) at `seq`.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct StateRef {
    #[serde(default = "default_path")]
    pub path: String,
    pub checkpoint: String,
    pub manifest_id: u64,
    pub seq: u64,
}

pub fn db_path(store: &Store, rel: &str) -> String {
    format!("{}/{rel}", store.prefix)
}

/// The path a bucket recovery by `epoch` clones the state to: one per
/// attempt, so a recovery cut short is resumed (SlateDB's clone is
/// idempotent for the same source) or left for the retention report.
pub fn recovery_path(epoch: u64) -> String {
    format!("qlog/state-e{epoch}")
}

/// The compactor's and its worker's poll, in ms. SlateDB's default (5 s
/// each) made them most of the state's GETs in the hour run at today's
/// rate (docs/quorum.md, Phase 6). Nothing waits on a poll here: the
/// applier is the only writer, a flush adds one L0 per interval, and
/// `l0_max_ssts` is far above the L0s one poll interval brings. vlpds polls
/// every 30 s too.
static COMPACTOR_POLL_MS: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(30_000);

pub fn set_compactor_poll(d: Duration) {
    COMPACTOR_POLL_MS.store((d.as_millis() as u64).max(100), std::sync::atomic::Ordering::Relaxed);
}

fn compactor_poll() -> Duration {
    if cfg!(test) {
        // tests flush every 150 ms: a 30 s poll would fill L0
        return Duration::from_secs(5);
    }
    Duration::from_millis(COMPACTOR_POLL_MS.load(std::sync::atomic::Ordering::Relaxed))
}

/// How much memory a SlateDB's memtables and compactor may hold, as
/// `--qlog-state-slatedb` / `--plc-seeds-slatedb` spell it
/// (`compactions=1,subcompactions=1,fetch-tasks=2,fetch-kb=1024,sst-mb=64,memtable-mb=128,codec=zstd`;
/// a key left out keeps its default).
///
/// SlateDB's own defaults are sized for a big box: 4 compactions of 4
/// subcompactions each, every one reading up to 8 sources 4 x 2 MiB ahead
/// and streaming its output through a multipart writer that buffers parts
/// while the bucket is slow, plus 256 MiB of memtables per database with
/// the WAL off. On a 4 GB node the seeds' compactions alone took the
/// process past its memory limit. A compaction's peak is roughly
/// `subcompactions x (sources x fetch-tasks x fetch-kb + its output's
/// buffered parts)`, so these bound it per database.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct Bounds {
    /// Compactions at once (the coordinator's and its worker's).
    pub compactions: usize,
    /// Parts one compaction is split into, run side by side (1: none).
    pub subcompactions: usize,
    /// Read-ahead requests in flight per input SST.
    pub fetch_tasks: usize,
    /// Bytes per read-ahead request.
    pub fetch_bytes: usize,
    /// A compaction's output SSTs roll at this size.
    pub sst_bytes: usize,
    /// Memtables, frozen ones included, not yet in the bucket: past it
    /// writes wait (the WAL is off).
    pub memtable_bytes: usize,
    /// SST compression for new SSTs. Each SST records its own codec, so a
    /// change applies as SSTs are rewritten and old ones stay readable.
    pub codec: Codec,
}

/// zstd over lz4: a point read decompresses one ~4 KiB block, microseconds
/// either way, so on a small box the cost that matters is compaction's
/// compress (level 3: a few seconds of CPU per GB rewritten), and zstd's
/// better ratio cuts the bucket bytes the seeds' readers fetch.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Codec {
    None,
    Lz4,
    Zstd,
}

impl Codec {
    fn slatedb(self) -> Option<slatedb::config::CompressionCodec> {
        match self {
            Codec::None => None,
            Codec::Lz4 => Some(slatedb::config::CompressionCodec::Lz4),
            Codec::Zstd => Some(slatedb::config::CompressionCodec::Zstd),
        }
    }

    fn name(self) -> &'static str {
        match self {
            Codec::None => "none",
            Codec::Lz4 => "lz4",
            Codec::Zstd => "zstd",
        }
    }
}

impl Bounds {
    /// The state takes one L0 per seal and stalls seals at `l0_max_ssts`:
    /// a second compaction keeps L0s draining while a long sorted-run merge
    /// holds the first. Its memtables keep their old cap: a flush
    /// interval's writes fit one, and a smaller cap would hold the applier
    /// back during a catch-up.
    pub const STATE: Bounds = Bounds {
        compactions: 2,
        subcompactions: 1,
        fetch_tasks: 2,
        fetch_bytes: 1 << 20,
        sst_bytes: 256 << 20,
        memtable_bytes: 256 << 20,
        codec: Codec::Zstd,
    };

    /// The seeds are a cache filled in the background: one compaction at a
    /// time is enough, and a slower fill costs nothing but time.
    pub const SEEDS: Bounds = Bounds {
        compactions: 1,
        subcompactions: 1,
        fetch_tasks: 2,
        fetch_bytes: 1 << 20,
        sst_bytes: 64 << 20,
        memtable_bytes: 128 << 20,
        codec: Codec::Zstd,
    };

    /// SlateDB's own, as every database ran before the bounds (benches).
    pub const SLATEDB_DEFAULT: Bounds = Bounds {
        compactions: 4,
        subcompactions: 4,
        fetch_tasks: 4,
        fetch_bytes: 2 << 20,
        sst_bytes: 256 << 20,
        memtable_bytes: 256 << 20,
        codec: Codec::None,
    };

    /// `self` with the keys `spec` names changed.
    pub fn parse(self, spec: &str) -> Result<Bounds, String> {
        let mut b = self;
        for kv in spec.split(',').map(str::trim).filter(|s| !s.is_empty()) {
            let (k, v) = kv.split_once('=').ok_or_else(|| format!("{kv}: want key=value"))?;
            if k.trim() == "codec" {
                b.codec = match v.trim() {
                    "none" => Codec::None,
                    "lz4" => Codec::Lz4,
                    "zstd" => Codec::Zstd,
                    _ => return Err(format!("{kv}: want none, lz4 or zstd")),
                };
                continue;
            }
            let n: usize = v.trim().parse().map_err(|e| format!("{kv}: {e}"))?;
            if n == 0 {
                return Err(format!("{kv}: must be at least 1"));
            }
            match k.trim() {
                "compactions" => b.compactions = n,
                "subcompactions" => b.subcompactions = n,
                "fetch-tasks" => b.fetch_tasks = n,
                "fetch-kb" => b.fetch_bytes = n << 10,
                "sst-mb" => b.sst_bytes = n << 20,
                "memtable-mb" => b.memtable_bytes = n << 20,
                k => {
                    return Err(format!(
                        "unknown key {k} (compactions, subcompactions, fetch-tasks, fetch-kb, sst-mb, memtable-mb, codec)"
                    ));
                }
            }
        }
        Ok(b)
    }
}

impl std::fmt::Display for Bounds {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "compactions={},subcompactions={},fetch-tasks={},fetch-kb={},sst-mb={},memtable-mb={},codec={}",
            self.compactions,
            self.subcompactions,
            self.fetch_tasks,
            self.fetch_bytes >> 10,
            self.sst_bytes >> 20,
            self.memtable_bytes >> 20,
            self.codec.name()
        )
    }
}

static STATE_BOUNDS: parking_lot::RwLock<Bounds> = parking_lot::RwLock::new(Bounds::STATE);
static SEED_BOUNDS: parking_lot::RwLock<Bounds> = parking_lot::RwLock::new(Bounds::SEEDS);

/// Sets the bounds every later open of the state uses.
pub fn set_state_bounds(b: Bounds) {
    *STATE_BOUNDS.write() = b;
}

pub fn state_bounds() -> Bounds {
    *STATE_BOUNDS.read()
}

/// Sets the bounds every later open of the PLC seeds' writer uses.
pub fn set_seed_bounds(b: Bounds) {
    *SEED_BOUNDS.write() = b;
}

pub fn seed_bounds() -> Bounds {
    *SEED_BOUNDS.read()
}

pub(crate) fn settings(l0_bytes: usize, b: Bounds) -> slatedb::Settings {
    let poll = compactor_poll();
    slatedb::Settings {
        wal_enabled: false,
        l0_sst_size_bytes: l0_bytes,
        // above the L0 size: with the WAL off, a cap below it stalls writes
        max_unflushed_bytes: b.memtable_bytes.max(l0_bytes * 2),
        compression_codec: b.codec.slatedb(),
        // every seal uploads an L0, and an upload past this many waits for
        // the compactor (seen as 1-5 s seals at 2 s flushes with the
        // default 8); 32 is minutes of flushes at any interval used here
        l0_max_ssts: 32,
        l0_max_ssts_per_key: 32,
        // the default 1 s poll is most of the state's GETs (~4/s measured);
        // vlpds polls every 10 s too
        manifest_poll_interval: std::time::Duration::from_secs(10),
        compactor_options: Some(slatedb::config::CompactorOptions {
            poll_interval: poll,
            max_concurrent_compactions: b.compactions,
            worker: Some(slatedb::config::CompactionWorkerOptions {
                compactions_poll_interval: poll,
                max_concurrent_compactions: b.compactions,
                max_subcompactions: b.subcompactions,
                max_fetch_tasks: b.fetch_tasks,
                bytes_to_fetch: b.fetch_bytes,
                max_sst_size: b.sst_bytes,
                ..Default::default()
            }),
            ..Default::default()
        }),
        ..Default::default()
    }
}

pub fn did_key(did: &str) -> Vec<u8> {
    [b"d/", did.as_bytes()].concat()
}

pub fn cursor_key(host: &str) -> Vec<u8> {
    [b"c/", host.as_bytes()].concat()
}

pub fn did_value(seq: u64, content: u64) -> [u8; 16] {
    let mut v = [0u8; 16];
    v[..8].copy_from_slice(&seq.to_be_bytes());
    v[8..].copy_from_slice(&content.to_be_bytes());
    v
}

/// What one entry writes to the state (the verifier replays the same): its
/// meta's writes, or for a bare test frame its DID's `d/` row; and the host
/// cursors it carries.
/// An entry's state writes and the host cursors it carries.
pub type Effects = (Vec<(Bytes, Bytes)>, Vec<(String, u64)>);

pub fn effects(e: &Entry) -> Effects {
    let writes = if e.meta.is_empty() {
        parse_test_frame(&e.data)
            .map(|(_, did, _)| {
                vec![(Bytes::from(did_key(&did)), Bytes::copy_from_slice(&did_value(e.seq, content_id(&e.data))))]
            })
            .unwrap_or_default()
    } else {
        Meta::decode(&e.meta).map(|m| m.writes).unwrap_or_default()
    };
    (writes, decode_cursors(&e.cursors))
}

pub struct State {
    db: Db,
    rel: String,
    path: String,
    store: Store,
    applied: u64,
    cursors: BTreeMap<String, u64>,
}

/// The `db` label of the state's SlateDB series and `/admin/api/store` shape.
pub const DB_LABEL: &str = "qlog_state";
/// A checkpoint read whole (a recovering leader's), while it runs.
pub const CHECKPOINT_LABEL: &str = "qlog_state_checkpoint";

impl State {
    /// Opens `qlog/state` as its writer, at whatever its last durable flush
    /// reached (at or past the last manifest's F: the leader seals before it
    /// writes a manifest, and applies only committed entries).
    pub async fn open(store: &Store, rel: &str) -> anyhow::Result<State> {
        // a flush interval's state changes fit one memtable at today's rates
        // (~100 B a DID), so most seals upload one L0
        State::open_with(store, rel, 64 << 20).await
    }

    /// With L0s cut at `l0_bytes` (tests: small, so memtables freeze and
    /// upload on their own between seals).
    pub async fn open_with(store: &Store, rel: &str, l0_bytes: usize) -> anyhow::Result<State> {
        let path = db_path(store, rel);
        let (cache, id) = super::cache::for_db(path.as_ref());
        let mut st = settings(l0_bytes, state_bounds());
        st.object_store_cache_options = super::cache::disk_options(super::cache::DiskDb::State);
        let db = Db::builder(path.clone(), store.raw.clone())
            .with_settings(st)
            .with_db_cache(cache, id)
            .with_metrics_recorder(slate_metrics::recorder(DB_LABEL))
            .build()
            .await?;
        slate_metrics::register(DB_LABEL, &db);
        let applied = match db.get(APPLIED).await? {
            Some(v) => u64::from_be_bytes(v.as_ref().try_into()?),
            None => 0,
        };
        let mut cursors = BTreeMap::new();
        let mut it = db.scan_prefix(b"c/", ..).await?;
        while let Some(kv) = it.next().await? {
            let host = String::from_utf8_lossy(&kv.key[2..]).into_owned();
            cursors.insert(host, u64::from_be_bytes(kv.value.as_ref().try_into()?));
        }
        Ok(State { db, rel: rel.to_string(), path, store: store.clone(), applied, cursors })
    }

    /// The state at exactly `from` (a manifest's checkpoint), as a new
    /// database at `rel` that this process writes: SlateDB has no restore,
    /// and the source's latest state can be past the checkpoint (the old
    /// leader kept applying, and memtables flush on their own). A clone is
    /// a new manifest over the checkpoint's SSTs, O(1) in the state's size;
    /// rewriting every key changed past F in place would scan the whole
    /// state. The clone pins its source with a checkpoint of its own, so
    /// the source's SSTs stay until that's released. `None`: no flush ever
    /// sealed a state, so it starts empty.
    pub async fn recover(store: &Store, from: Option<&StateRef>, rel: &str) -> anyhow::Result<State> {
        if let Some(r) = from {
            anyhow::ensure!(r.path != rel, "qlog state: recovering {rel} onto itself");
            slatedb::admin::Admin::builder(db_path(store, rel), store.raw.clone())
                .build()
                .create_clone_builder_from_source(slatedb::admin::CloneSourceSpec::with_checkpoint(
                    db_path(store, &r.path),
                    r.checkpoint.parse()?,
                ))
                .build()
                .await?;
        }
        let st = State::open(store, rel).await?;
        let want = from.map_or(0, |r| r.seq);
        anyhow::ensure!(st.applied == want, "qlog state: the clone at {rel} is at {}, not {want}", st.applied);
        Ok(st)
    }

    /// The path under the store's prefix.
    pub fn rel(&self) -> &str {
        &self.rel
    }

    /// Moves the applied point to `seq` with nothing applied in between:
    /// the seqs skipped by a bucket recovery, which no entry will ever hold.
    pub async fn jump(&mut self, seq: u64) -> anyhow::Result<()> {
        anyhow::ensure!(seq >= self.applied, "qlog state: jumping back from {} to {seq}", self.applied);
        if seq == self.applied {
            return Ok(());
        }
        let mut b = WriteBatch::new();
        b.put(APPLIED, seq.to_be_bytes());
        self.db.write_with_options(b, &WriteOptions { seqnum: seq }).await?;
        self.applied = seq;
        Ok(())
    }

    pub fn applied(&self) -> u64 {
        self.applied
    }

    pub fn cursors(&self) -> &BTreeMap<String, u64> {
        &self.cursors
    }

    /// The database, for readers of what's applied (the relay's leader
    /// reads its DID records here); only `apply` and `jump` write it.
    pub fn db(&self) -> Db {
        self.db.clone()
    }

    /// Applies `entries` (consecutive, from `applied + 1`) as one batch.
    pub async fn apply(&mut self, entries: &[Entry]) -> anyhow::Result<()> {
        let Some(last) = entries.last() else { return Ok(()) };
        anyhow::ensure!(
            entries[0].seq == self.applied + 1,
            "qlog state: applying {} after {}",
            entries[0].seq,
            self.applied
        );
        let mut b = WriteBatch::new();
        for e in entries {
            let (writes, cursors) = effects(e);
            for (k, v) in writes {
                b.put(k, v);
            }
            for (h, c) in cursors {
                let cur = self.cursors.entry(h.clone()).or_default();
                if c > *cur {
                    *cur = c;
                    b.put(cursor_key(&h), c.to_be_bytes());
                }
            }
        }
        b.put(APPLIED, last.seq.to_be_bytes());
        self.db.write_with_options(b, &WriteOptions { seqnum: last.seq }).await?;
        self.applied = last.seq;
        Ok(())
    }

    /// A checkpoint of the state at exactly `applied` (see the module docs:
    /// the caller writes nothing until this returns).
    pub async fn seal(&self) -> anyhow::Result<StateRef> {
        let cp = self
            .db
            .create_checkpoint(
                CheckpointScope::All,
                &CheckpointOptions { lifetime: None, name: Some(format!("qlog-{}", self.applied)), source: None },
            )
            .await?;
        let admin = slatedb::admin::Admin::builder(self.path.clone(), self.store.raw.clone()).build();
        let m = admin
            .read_manifest(Some(cp.manifest_id))
            .await?
            .ok_or_else(|| anyhow::anyhow!("qlog state: checkpoint manifest {} missing", cp.manifest_id))?;
        if m.last_l0_seq() != self.applied {
            let _ = admin.delete_checkpoint(cp.id).await;
            anyhow::bail!(
                "qlog state: checkpoint {} holds seq {}, not the seal point {}",
                cp.id,
                m.last_l0_seq(),
                self.applied
            );
        }
        Ok(StateRef {
            path: self.rel.clone(),
            checkpoint: cp.id.to_string(),
            manifest_id: cp.manifest_id,
            seq: self.applied,
        })
    }

    pub async fn close(self) {
        let _ = self.db.close().await;
    }
}

/// Deletes every `qlog-*` checkpoint of the state at `rel` but `keep`'s.
pub async fn delete_checkpoints_except(store: &Store, rel: &str, keep: Option<&str>) -> anyhow::Result<usize> {
    let admin = slatedb::admin::Admin::builder(db_path(store, rel), store.raw.clone()).build();
    let mut n = 0;
    for c in admin.list_checkpoints(None).await? {
        if c.name.as_deref().is_some_and(|x| x.starts_with("qlog-")) && Some(c.id.to_string().as_str()) != keep {
            admin.delete_checkpoint(c.id).await?;
            n += 1;
        }
    }
    Ok(n)
}

/// Ids of the `qlog-*` checkpoints of the state at `rel`.
pub async fn list_checkpoints(store: &Store, rel: &str) -> anyhow::Result<Vec<String>> {
    let admin = slatedb::admin::Admin::builder(db_path(store, rel), store.raw.clone()).build();
    Ok(admin
        .list_checkpoints(None)
        .await?
        .into_iter()
        .filter(|c| c.name.as_deref().is_some_and(|x| x.starts_with("qlog-")))
        .map(|c| c.id.to_string())
        .collect())
}

pub async fn delete_checkpoint(store: &Store, r: &StateRef) -> anyhow::Result<()> {
    let admin = slatedb::admin::Admin::builder(db_path(store, &r.path), store.raw.clone()).build();
    let id = &r.checkpoint;
    admin.delete_checkpoint(id.parse()?).await?;
    Ok(())
}

/// The state a checkpoint holds: every key and value, plus the checkpoint
/// manifest's `last_l0_seq` (what a restart from it would hold).
pub async fn read_checkpoint(store: &Store, r: &StateRef) -> anyhow::Result<(BTreeMap<Bytes, Bytes>, u64)> {
    let admin = slatedb::admin::Admin::builder(db_path(store, &r.path), store.raw.clone()).build();
    let m = admin
        .read_manifest(Some(r.manifest_id))
        .await?
        .ok_or_else(|| anyhow::anyhow!("checkpoint manifest {} is gone", r.manifest_id))?;
    let path = db_path(store, &r.path);
    let (cache, id) = super::cache::for_db(path.as_ref());
    let reader = DbReader::builder(path, store.raw.clone())
        .with_db_cache(cache, id)
        .with_reader_mode(DbReaderMode::Checkpoint(r.checkpoint.parse()?))
        .with_metrics_recorder(slate_metrics::recorder(CHECKPOINT_LABEL))
        .build()
        .await?;
    slate_metrics::register_reader(CHECKPOINT_LABEL, &reader);
    let mut out = BTreeMap::new();
    let mut it = reader.scan::<std::ops::RangeFull>(..).await?;
    while let Some(kv) = it.next().await? {
        out.insert(kv.key, kv.value);
    }
    let _ = tokio::time::timeout(Duration::from_secs(5), reader.close()).await;
    Ok((out, m.last_l0_seq()))
}

pub fn applied_key() -> &'static [u8] {
    APPLIED
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::qlog::client::test_frame;
    use crate::qlog::log::encode_cursors;

    fn entry(seq: u64, pad: usize) -> Entry {
        let (p, s) = test_frame(&format!("did:q:h{}:{}", seq % 3, seq / 3 + 1), pad, 0);
        let mut e = Entry::new(1, seq, crate::qlog::wire::splice_seq(&p, &s, seq));
        if seq.is_multiple_of(10) {
            e.cursors = encode_cursors(&[(format!("h{}", seq % 3), seq / 3)].into());
        }
        e
    }

    /// Memtables freeze and upload on their own (tiny L0s) before and after
    /// the seal, and writes go on right after it: the checkpoint still holds
    /// exactly the seal point, by its manifest and by its contents.
    #[test]
    fn bounds_parse_over_their_defaults_and_reach_the_settings() {
        let b = Bounds::SEEDS.parse("compactions=3, fetch-kb=512,memtable-mb=96").unwrap();
        assert_eq!(b, Bounds { compactions: 3, fetch_bytes: 512 << 10, memtable_bytes: 96 << 20, ..Bounds::SEEDS });
        assert_eq!(Bounds::STATE.parse(&Bounds::SEEDS.to_string()).unwrap(), Bounds::SEEDS);
        assert_eq!(Bounds::SEEDS.parse("").unwrap(), Bounds::SEEDS);
        let lz4 = Bounds::SEEDS.parse("codec=lz4").unwrap();
        assert_eq!(lz4.codec, Codec::Lz4);
        assert_eq!(Bounds::SEEDS.parse(&lz4.to_string()).unwrap(), lz4);
        assert_eq!(settings(64 << 20, Bounds::SEEDS).compression_codec, Some(slatedb::config::CompressionCodec::Zstd));
        assert_eq!(settings(64 << 20, Bounds::SEEDS.parse("codec=none").unwrap()).compression_codec, None);
        for bad in ["compactions", "compactions=0", "compactions=x", "zstd=1", "codec=snappy"] {
            assert!(Bounds::SEEDS.parse(bad).is_err(), "{bad}");
        }
        let s = settings(64 << 20, Bounds::SEEDS);
        let c = s.compactor_options.unwrap();
        let w = c.worker.unwrap();
        assert_eq!(c.max_concurrent_compactions, 1);
        assert_eq!(
            (w.max_concurrent_compactions, w.max_subcompactions, w.max_fetch_tasks, w.bytes_to_fetch, w.max_sst_size),
            (1, 1, 2, 1 << 20, 64 << 20)
        );
        assert_eq!(s.max_unflushed_bytes, 128 << 20);
        // never below two L0s: with the WAL off a smaller cap stalls writes
        assert_eq!(settings(96 << 20, Bounds::SEEDS).max_unflushed_bytes, 192 << 20);
    }

    #[tokio::test]
    async fn a_seal_holds_exactly_its_point_across_automatic_flushes() {
        let store = Store::memory(None);
        let mut st = State::open_with(&store, DEFAULT_PATH, 16 << 10).await.unwrap();
        let mut seq = 0;
        let mut refs = Vec::new();
        for round in 0..4 {
            for _ in 0..40 {
                let es: Vec<Entry> = (seq + 1..=seq + 25).map(|s| entry(s, 400)).collect();
                st.apply(&es).await.unwrap();
                seq += 25;
            }
            let r = st.seal().await.unwrap();
            assert_eq!(r.seq, seq, "round {round}");
            refs.push((r, st.cursors().clone()));
        }
        let m = slatedb::admin::Admin::builder(db_path(&store, DEFAULT_PATH), store.raw.clone())
            .build()
            .read_manifest(None)
            .await
            .unwrap()
            .unwrap();
        assert!(m.l0().len() + m.compacted().len() > 4, "no automatic L0 flushes happened");
        for (r, cursors) in &refs {
            let (kv, l0) = read_checkpoint(&store, r).await.unwrap();
            assert_eq!(l0, r.seq);
            assert_eq!(kv.get(APPLIED).map(|v| u64::from_be_bytes(v[..].try_into().unwrap())), Some(r.seq));
            let mut dids = 0;
            for (k, v) in &kv {
                if k.starts_with(b"d/") {
                    dids += 1;
                    let s = u64::from_be_bytes(v[..8].try_into().unwrap());
                    assert!(s <= r.seq, "seq {s} past the seal at {}", r.seq);
                } else if let Some(h) = k.strip_prefix(b"c/") {
                    let h = String::from_utf8_lossy(h).into_owned();
                    assert_eq!(cursors.get(&h).copied(), Some(u64::from_be_bytes(v[..].try_into().unwrap())));
                }
            }
            assert_eq!(dids, r.seq, "every seq names its own DID");
        }
        st.close().await;
        // a writer opened afterwards sees the last applied point and goes on
        let st = State::open(&store, DEFAULT_PATH).await.unwrap();
        assert!(st.applied() >= refs.last().unwrap().0.seq);
        st.close().await;
    }

    /// The state's SlateDB is in /metrics under `db="qlog_state"` and in
    /// `GET /admin/api/store`'s `dbs`.
    #[tokio::test]
    async fn the_state_db_exports_its_metrics_and_shape() {
        let store = Store::memory(None);
        let mut st = State::open(&store, DEFAULT_PATH).await.unwrap();
        let es: Vec<Entry> = (1..=50).map(|s| entry(s, 300)).collect();
        st.apply(&es).await.unwrap();
        st.seal().await.unwrap();
        let shape = slate_metrics::shapes().into_iter().find(|d| d.db == DB_LABEL).unwrap();
        assert_eq!(shape.role, "writer");
        assert!(shape.memtable_bytes.is_some());
        let fams = prometheus::gather();
        let dbs = |name: &str| -> Vec<String> {
            fams.iter()
                .filter(|f| f.name() == name)
                .flat_map(|f| f.get_metric())
                .flat_map(|m| m.get_label().iter().filter(|l| l.name() == "db").map(|l| l.value().to_string()))
                .collect()
        };
        assert!(dbs("slatedb_db_write_ops_total").iter().any(|d| d == DB_LABEL));
        assert!(dbs("slatedb_lsm_ssts").iter().any(|d| d == DB_LABEL));
        st.close().await;
    }

    /// Recovery from a checkpoint while the source has moved on past it:
    /// the clone holds exactly the checkpoint (not the source's latest),
    /// takes writes of its own, jumps, and seals at the jump; the source's
    /// later writes never show up in it.
    #[tokio::test]
    async fn a_recovered_state_is_its_checkpoint_not_the_latest() {
        let store = Store::memory(None);
        let mut st = State::open_with(&store, DEFAULT_PATH, 16 << 10).await.unwrap();
        let es: Vec<Entry> = (1..=500).map(|s| entry(s, 300)).collect();
        st.apply(&es).await.unwrap();
        let r = st.seal().await.unwrap();
        let cursors_at = st.cursors().clone();
        // the old leader keeps applying, and its memtables reach the bucket
        let es: Vec<Entry> = (501..=900).map(|s| entry(s, 300)).collect();
        st.apply(&es).await.unwrap();
        st.close().await;
        let rel = recovery_path(7);
        let mut rec = State::recover(&store, Some(&r), &rel).await.unwrap();
        assert_eq!(rec.applied(), 500);
        assert_eq!(rec.cursors(), &cursors_at);
        let es: Vec<Entry> = (501..=520).map(|s| entry(s, 300)).collect();
        rec.apply(&es).await.unwrap();
        rec.jump(10_000).await.unwrap();
        let sealed = rec.seal().await.unwrap();
        assert_eq!((sealed.seq, sealed.path.as_str()), (10_000, rel.as_str()));
        let (kv, l0) = read_checkpoint(&store, &sealed).await.unwrap();
        assert_eq!(l0, 10_000);
        let dids = kv.keys().filter(|k| k.starts_with(b"d/")).count();
        assert_eq!(dids, 520, "the clone holds the source's state past its checkpoint");
        for (k, v) in &kv {
            if k.starts_with(b"d/") {
                assert!(u64::from_be_bytes(v[..8].try_into().unwrap()) <= 520);
            }
        }
        rec.close().await;
        // a clone that finished is opened again as it is
        let again = State::open(&store, &rel).await.unwrap();
        assert_eq!(again.applied(), 10_000);
        again.close().await;
    }
}
