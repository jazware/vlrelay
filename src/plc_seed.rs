//! DID documents in bulk from the PLC directory's `/export`, so a cold relay
//! doesn't resolve its ~56M accounts one lookup at a time.
//!
//! The quorum log's leader reads the export as a background job ([`job`]):
//! the history in a few windows side by side, then the live tail
//! ([`ingest`]). Per did:plc it keeps the latest op's `#atproto` signing key,
//! its `atproto_pds` endpoint, whether it's a tombstone, and the op's
//! `createdAt`, in a SlateDB of its own in the bucket (`plc/seeds`, ~3 GB
//! for 56M DIDs), with the export's cursors checkpointed beside it
//! (`plc/export-checkpoint.json`) once the rows before them are flushed. A
//! new leader opens the database as its writer (which fences the old
//! leader's) and resumes from the checkpoint; ops read after it are read
//! again, and newest-wins makes that harmless.
//!
//! The seeds are a cache, not the log's state: they stay out of
//! `qlog/state`, so flushes, checkpoints, `verify` and bucket recovery never
//! carry 3 GB of documents, and losing the last seconds of them at a
//! takeover only costs a few PLC lookups.
//!
//! Rows are written as merges: the database's merge operator ([`NewestWins`])
//! keeps each DID's newest, so writing a page reads nothing first.
//!
//! The leader also keeps every document its cache fetches ([`Seeder`]'s
//! `learned`, batched by [`SeedReader::write_learned`]), stamped with when
//! it was fetched, so a restart doesn't resolve those accounts again and an
//! export op created after the fetch still wins.
//!
//! Every member reads the database ([`SeedReader`]). On a DID document cache
//! miss the seed fills the cache without spending the PLC lookup budget;
//! the leader also weighs it against the account's record ([`choose`]). A
//! forced refresh (an `#identity`, a signature that fails against the
//! seeded key, a host that doesn't match) still goes to PLC.
//!
//! Validation is deliberately thin: each line must be a well-formed op of a
//! known type (vlpds's `plc::op_type`) for a valid did:plc, and nullified
//! ops are skipped. The op chain isn't checked: every commit's signature is
//! still checked against the seeded key, and a failure re-resolves from PLC.

pub mod ingest;
pub mod job;

use crate::identity::{self, Identity};
use crate::state::record::{HostKey, Record};
use crate::types::Host;
use crate::verify::SigningKey;
use bytes::Bytes;
use serde_json::Value as J;
use std::sync::Arc;
use std::time::{Duration, Instant};
use vlsync_store::store::Store;

/// The seed database, under the relay's prefix.
pub const SEEDS_PATH: &str = "plc/seeds";

pub fn seed_key(did: &str) -> Vec<u8> {
    match crate::state::record::plc_bytes(did) {
        Some(b) => {
            let mut k = Vec::with_capacity(16);
            k.push(b'p');
            k.extend_from_slice(&b);
            k
        }
        None => [b"w".as_slice(), did.strip_prefix("did:").unwrap_or(did).as_bytes()].concat(),
    }
}

/// One DID's latest export op, as kept.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Seed {
    /// The op's `createdAt`, unix ms: an older op never replaces a newer one.
    pub created_ms: u64,
    pub tombstone: bool,
    /// Multicodec bytes of the `#atproto` key, as the state record keeps it.
    pub key: Option<Bytes>,
    /// The `atproto_pds` endpoint's host ([`identity::normalize_host`]).
    pub pds: Option<String>,
    /// The endpoint is `http://`: the host alone would read back as https,
    /// and a local PDS (the dev network's) serves plain http only.
    pub pds_http: bool,
    /// A document this relay fetched, not an export op: `created_ms` is
    /// when it was fetched, less [`LOOKUP_SKEW`].
    pub lookup: bool,
}

const VERSION: u8 = 1;
const F_TOMBSTONE: u8 = 1;
const F_KEY: u8 = 2;
const F_PDS: u8 = 4;
const F_PDS_HTTP: u8 = 8;
const F_LOOKUP: u8 = 16;

#[derive(Debug, Clone, Copy, thiserror::Error)]
#[error("corrupt seed row")]
pub struct DecodeError;

impl Seed {
    pub fn encode(&self) -> Bytes {
        let mut b = Vec::with_capacity(64);
        let mut flags = 0;
        if self.tombstone {
            flags |= F_TOMBSTONE;
        }
        if self.key.is_some() {
            flags |= F_KEY;
        }
        if self.pds.is_some() {
            flags |= F_PDS;
        }
        if self.pds_http {
            flags |= F_PDS_HTTP;
        }
        if self.lookup {
            flags |= F_LOOKUP;
        }
        b.push(VERSION);
        b.push(flags);
        crate::state::record::put_varint(&mut b, self.created_ms);
        if let Some(k) = &self.key {
            b.push(k.len() as u8);
            b.extend_from_slice(k);
        }
        if let Some(p) = &self.pds {
            let p = &p.as_bytes()[..p.len().min(255)];
            b.push(p.len() as u8);
            b.extend_from_slice(p);
        }
        Bytes::from(b)
    }

    pub fn decode(b: &[u8]) -> Result<Seed, DecodeError> {
        let mut r = crate::state::record::Reader(b);
        let d = |_| DecodeError;
        if r.u8().map_err(d)? != VERSION {
            return Err(DecodeError);
        }
        let flags = r.u8().map_err(d)?;
        let created_ms = r.varint().map_err(d)?;
        let key = if flags & F_KEY != 0 {
            let n = r.u8().map_err(d)? as usize;
            Some(Bytes::copy_from_slice(r.take(n).map_err(d)?))
        } else {
            None
        };
        let pds = if flags & F_PDS != 0 {
            let n = r.u8().map_err(d)? as usize;
            Some(std::str::from_utf8(r.take(n).map_err(d)?).map_err(|_| DecodeError)?.to_string())
        } else {
            None
        };
        Ok(Seed {
            created_ms,
            tombstone: flags & F_TOMBSTONE != 0,
            key,
            pds,
            pds_http: flags & F_PDS_HTTP != 0,
            lookup: flags & F_LOOKUP != 0,
        })
    }

    fn usable(&self) -> bool {
        !self.tombstone && self.key.is_some() && self.pds.is_some()
    }

    pub fn identity(&self, did: &str) -> Option<Identity> {
        if !self.usable() {
            return None;
        }
        let pds = self.pds.as_ref()?;
        let mb = format!("z{}", bs58::encode(self.key.as_ref()?).into_string());
        let k = SigningKey::from_multibase(&mb).ok()?;
        Some(Identity {
            did: did.to_string(),
            signing_key: Some(k),
            signing_key_multibase: Some(mb),
            pds: Some(format!("{}://{pds}", if self.pds_http { "http" } else { "https" })),
            pds_host: Some(Host(pds.clone())),
            handle: None,
        })
    }

    /// Whether `id` (a cached document) says something else than `self`.
    fn differs_from(&self, id: &Identity) -> bool {
        if self.tombstone {
            return true;
        }
        let key = id.signing_key_multibase.as_deref().and_then(multikey_bytes);
        key != self.key
            || id.pds_host.as_ref().map(|h| h.0.as_str()) != self.pds.as_deref()
            || id.pds.as_deref().is_some_and(is_http) != self.pds_http
    }

    /// A fetched document as a row, stamped with when it was fetched.
    pub fn from_lookup(id: Option<&Identity>, fetched_ms: u64) -> Seed {
        let created_ms = fetched_ms.saturating_sub(LOOKUP_SKEW.as_millis() as u64);
        match id {
            Some(id) => Seed {
                created_ms,
                tombstone: false,
                key: id.signing_key_multibase.as_deref().and_then(multikey_bytes),
                pds: id.pds_host.as_ref().map(|h| h.0.clone()),
                pds_http: id.pds.as_deref().is_some_and(is_http),
                lookup: true,
            },
            None => Seed { created_ms, tombstone: true, key: None, pds: None, pds_http: false, lookup: true },
        }
    }

    /// Whether row `a` wins over row `b`: the newer op, with the encodings
    /// breaking a tie, so every member and every compaction picks the same
    /// one. A row that doesn't decode loses.
    fn wins(a: &[u8], b: &[u8]) -> bool {
        let at = |r: &[u8]| Seed::decode(r).ok().map(|s| s.created_ms);
        at(a).cmp(&at(b)).then_with(|| a.cmp(b)).is_ge()
    }
}

/// How far a lookup's stamp is set back. PLC stamps an op's `createdAt`
/// itself, so a document fetched at T reflects every op before T by its
/// clock; ours may run ahead of it. Setting the stamp back keeps an op
/// made just after the fetch from looking older than it: an op inside the
/// margin replaces the fetched document, which costs nothing when it was
/// already in it.
pub const LOOKUP_SKEW: Duration = Duration::from_secs(60);

fn is_http(url: &str) -> bool {
    url.trim().get(..7).is_some_and(|s| s.eq_ignore_ascii_case("http://"))
}

/// A `publicKeyMultibase` as the multicodec bytes a row keeps.
fn multikey_bytes(mb: &str) -> Option<Bytes> {
    let raw = bs58::decode(mb.strip_prefix('z')?).into_vec().ok()?;
    (raw.len() <= crate::state::record::MAX_KEY_LEN).then(|| Bytes::from(raw))
}

/// The seed database's merge: a DID's newest row wins, so the export's
/// windows and the lookups write without reading what's there.
pub struct NewestWins;

impl slatedb::MergeOperator for NewestWins {
    fn merge(&self, _key: &Bytes, existing: Option<Bytes>, value: Bytes) -> Result<Bytes, slatedb::MergeOperatorError> {
        Ok(match existing {
            Some(old) if Seed::wins(&old, &value) => old,
            _ => value,
        })
    }
}

fn merge_operator() -> Arc<dyn slatedb::MergeOperator + Send + Sync> {
    Arc::new(NewestWins)
}

/// One line of the export, parsed.
#[derive(Clone, Debug)]
pub struct ExportOp {
    pub did: String,
    pub created_at: String,
    pub seed: Seed,
}

#[derive(Debug, PartialEq, Eq)]
pub enum LineError {
    Json,
    Did,
    CreatedAt,
    Op,
    /// Valid, but nullified by a later recovery op: not the DID's state.
    Nullified,
}

/// Parses one export line. Only the checks the module doc names.
pub fn parse_line(line: &[u8]) -> Result<ExportOp, LineError> {
    let v: J = serde_json::from_slice(line).map_err(|_| LineError::Json)?;
    let did = v.get("did").and_then(J::as_str).ok_or(LineError::Did)?;
    if !vlsync_atproto::plc::valid_plc_did(did) {
        return Err(LineError::Did);
    }
    let created_at = v.get("createdAt").and_then(J::as_str).ok_or(LineError::CreatedAt)?;
    let created_ms = parse_ms(created_at).ok_or(LineError::CreatedAt)?;
    if v.get("nullified").and_then(J::as_bool) == Some(true) {
        return Err(LineError::Nullified);
    }
    let op = v.get("operation").ok_or(LineError::Op)?;
    let ty = vlsync_atproto::plc::op_type(op, true).ok_or(LineError::Op)?;
    let seed = match ty {
        vlsync_atproto::plc::OpType::Tombstone => {
            Seed { created_ms, tombstone: true, key: None, pds: None, pds_http: false, lookup: false }
        }
        vlsync_atproto::plc::OpType::Operation | vlsync_atproto::plc::OpType::LegacyCreate => {
            let (key, pds) = if ty == vlsync_atproto::plc::OpType::LegacyCreate {
                (op.get("signingKey"), op.get("service"))
            } else {
                (
                    op.pointer("/verificationMethods/atproto"),
                    op.pointer("/services/atproto_pds")
                        .filter(|s| s.get("type").and_then(J::as_str) == Some("AtprotoPersonalDataServer"))
                        .and_then(|s| s.get("endpoint")),
                )
            };
            let pds = pds.and_then(J::as_str);
            Seed {
                created_ms,
                tombstone: false,
                key: key.and_then(J::as_str).and_then(did_key_bytes),
                pds: pds.and_then(identity::normalize_host).map(|h| h.0),
                pds_http: pds.is_some_and(is_http),
                lookup: false,
            }
        }
    };
    Ok(ExportOp { did: did.to_string(), created_at: created_at.to_string(), seed })
}

/// A `did:key:z…` as multicodec bytes, if it's a key the relay can verify with.
fn did_key_bytes(k: &str) -> Option<Bytes> {
    let mb = k.strip_prefix("did:key:")?;
    SigningKey::from_multibase(mb).ok()?;
    let raw = bs58::decode(mb.strip_prefix('z')?).into_vec().ok()?;
    (raw.len() <= crate::state::record::MAX_KEY_LEN).then(|| Bytes::from(raw))
}

pub fn parse_ms(s: &str) -> Option<u64> {
    chrono::DateTime::parse_from_rfc3339(s).ok().and_then(|t| u64::try_from(t.timestamp_millis()).ok())
}

pub fn format_ms(ms: u64) -> String {
    chrono::DateTime::from_timestamp_millis(ms as i64)
        .unwrap_or_default()
        .to_rfc3339_opts(chrono::SecondsFormat::Millis, true)
}

/// What fills the cache on a miss.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Pick {
    /// The record's own document is fresher: fetch it (the record keeps a
    /// host's hash, not its name).
    Record,
    Seed,
    /// Ask PLC.
    Resolve,
}

/// The state record against the seed. A record resolved within `ttl` wins
/// unless the seed's op is newer. A seed of any age is used, unless the
/// account shows a change since its op and the op is older than `ttl`: an
/// `#identity` not resolved since (`fetched_at` 0), or a later resolve that
/// found another key or PDS.
pub fn choose(rec: Option<&Record>, seed: Option<&Seed>, now_s: u32, ttl_s: u32) -> Pick {
    let rec_ok = rec.filter(|r| r.key.is_some() && r.pds.is_some());
    let rec_fresh = rec_ok.filter(|r| r.fetched_at != 0 && now_s.saturating_sub(r.fetched_at) < ttl_s);
    let Some(seed) = seed.filter(|s| s.usable()) else {
        return if rec_fresh.is_some() { Pick::Record } else { Pick::Resolve };
    };
    let seed_s = (seed.created_ms / 1000) as u32;
    if let Some(r) = rec_fresh {
        // an op in the resolve's own second may postdate it
        return if r.fetched_at > seed_s { Pick::Record } else { Pick::Seed };
    }
    let seed_old = now_s.saturating_sub(seed_s) >= ttl_s;
    if let Some(r) = rec
        && seed_old
    {
        if r.fetched_at == 0 {
            return Pick::Resolve;
        }
        let moved = r.key.as_ref().map(|k| &k.0) != seed.key.as_ref() || r.pds != seed.pds.as_deref().map(HostKey::of);
        if r.fetched_at > seed_s && moved {
            return Pick::Resolve;
        }
    }
    Pick::Seed
}

fn db_path(store: &Store) -> object_store::path::Path {
    object_store::path::Path::from(format!("{}/{SEEDS_PATH}", store.prefix))
}

/// The `db` labels of the seed database's SlateDB series and shapes.
pub const WRITER_LABEL: &str = "plc_seeds";
pub const READER_LABEL: &str = "plc_seeds_reader";

/// The leader's handle on the seed database: the only writer, fenced by
/// the next leader's open.
pub struct SeedWriter {
    db: slatedb::Db,
}

impl SeedWriter {
    pub async fn open(store: &Store) -> anyhow::Result<SeedWriter> {
        // a few seconds of the tail fit one memtable; the backfill seals
        // 64 MiB L0s as it goes
        let path = db_path(store);
        let (cache, id) = crate::qlog::cache::for_db(path.as_ref());
        let db = slatedb::Db::builder(path, store.raw.clone())
            .with_settings(crate::qlog::state::settings(64 << 20, crate::qlog::state::seed_bounds()))
            .with_db_cache(cache, id)
            .with_merge_operator(merge_operator())
            .with_metrics_recorder(slate_metrics::recorder(WRITER_LABEL))
            .build()
            .await?;
        slate_metrics::register(WRITER_LABEL, &db);
        Ok(SeedWriter { db })
    }

    pub async fn get(&self, did: &str) -> anyhow::Result<Option<Seed>> {
        match self.db.get(seed_key(did)).await? {
            Some(b) => Ok(Some(Seed::decode(&b)?)),
            None => Ok(None),
        }
    }

    /// Merges the rows into the memtable, each DID keeping its newest
    /// ([`NewestWins`]): no reads, so a page costs no bucket requests.
    /// [`Self::flush`] makes them durable.
    pub async fn apply(&self, rows: Vec<(String, Seed)>) -> anyhow::Result<usize> {
        // SlateDB refuses an empty batch
        if rows.is_empty() {
            return Ok(0);
        }
        let mut wb = slatedb::WriteBatch::new();
        for (did, seed) in &rows {
            wb.merge(seed_key(did), seed.encode());
        }
        self.db.write(wb).await?;
        Ok(rows.len())
    }

    /// Makes every applied entry durable (the database runs without a WAL).
    pub async fn flush(&self) -> anyhow::Result<()> {
        self.db
            .flush_with_options(slatedb::config::FlushOptions { flush_type: slatedb::config::FlushType::MemTable })
            .await?;
        Ok(())
    }

    pub async fn close(&self) {
        if let Err(e) = self.db.close().await {
            tracing::debug!("closing the PLC seed database: {e:#}");
        }
    }
}

/// Every member's read side of the seed database, opened once the leader
/// has made it, following its latest manifest (seeds a compaction removed
/// under a read come back as a miss, and the cache fetches).
pub struct SeedReader {
    store: Store,
    /// Seed reads in flight at once ([`set_read_slots`]).
    reads: tokio::sync::Semaphore,
    /// The export's memory budget stopped its term: no seed reads (each can
    /// load megabytes of filters and indexes) until it starts the next.
    pub paused: std::sync::atomic::AtomicBool,
    reader: tokio::sync::RwLock<Option<Arc<slatedb::DbReader>>>,
    tried: parking_lot::Mutex<Option<Instant>>,
    /// The leader's writer, for its own fresh rows.
    pub writer: parking_lot::RwLock<Option<Arc<SeedWriter>>>,
    /// Fetched documents waiting for the leader's next write batch
    /// ([`SeedReader::write_learned`]).
    learned: parking_lot::Mutex<Vec<(String, Seed)>>,
    learned_full: tokio::sync::Notify,
    pub learned_written: std::sync::atomic::AtomicU64,
    pub learned_batches: std::sync::atomic::AtomicU64,
    pub learned_dropped: std::sync::atomic::AtomicU64,
}

/// Fetched documents buffered before a write batch is due anyway.
pub const LEARN_BATCH: usize = 1024;
/// How often the buffer is written when it doesn't fill.
pub const LEARN_EVERY: Duration = Duration::from_secs(1);
/// What the buffer holds at most (a writer stuck on the bucket): past it a
/// document is only in memory.
const LEARN_MAX: usize = 64 * LEARN_BATCH;

/// Seed reads a [`SeedReader`] runs at once; the rest wait their turn.
/// Once the seeds' filters and indexes outgrow the metadata cache (~120 MiB
/// of them at 58M rows against 64 MiB), a read loads megabytes of them from
/// the bucket: on a small box the identity cache's ~128 lookups in flight
/// pulled 40-55 MiB/s, past what the rest of the node could share.
static READ_SLOTS: std::sync::atomic::AtomicUsize = std::sync::atomic::AtomicUsize::new(DEFAULT_READ_SLOTS);
pub const DEFAULT_READ_SLOTS: usize = 32;

/// Sets [`READ_SLOTS`] for the readers made after it.
pub fn set_read_slots(n: usize) {
    READ_SLOTS.store(n.max(1), std::sync::atomic::Ordering::Relaxed);
}

/// How often a member retries opening the database before it exists, and
/// how often an open reader looks for the writer's new manifests.
const READER_POLL: Duration = Duration::from_secs(30);

impl SeedReader {
    pub fn new(store: Store) -> Arc<SeedReader> {
        Arc::new(SeedReader {
            store,
            reads: tokio::sync::Semaphore::new(READ_SLOTS.load(std::sync::atomic::Ordering::Relaxed).max(1)),
            paused: Default::default(),
            reader: Default::default(),
            tried: Default::default(),
            writer: Default::default(),
            learned: Default::default(),
            learned_full: Default::default(),
            learned_written: Default::default(),
            learned_batches: Default::default(),
            learned_dropped: Default::default(),
        })
    }

    /// Keeps a fetched document for the seed database. Only the leader
    /// writes it, so on any other member this is a no-op: its own lookups
    /// stay in its memory, and the leader resolves every account it applies
    /// anyway.
    pub fn learn(&self, did: &str, seed: Seed) {
        use std::sync::atomic::Ordering::Relaxed;
        if self.writer.read().is_none() {
            return;
        }
        let mut l = self.learned.lock();
        if l.len() >= LEARN_MAX {
            self.learned_dropped.fetch_add(1, Relaxed);
            return;
        }
        l.push((did.to_string(), seed));
        if l.len() >= LEARN_BATCH {
            self.learned_full.notify_one();
        }
    }

    /// The leader's loop writing [`Self::learn`]'s buffer, one batch per
    /// [`LEARN_BATCH`] documents or [`LEARN_EVERY`], into the memtable: the
    /// export's checkpoints flush it, so persisting lookups adds no bucket
    /// writes of its own. Returns when `keep` turns false, after a last
    /// batch.
    pub async fn write_learned(&self, w: &SeedWriter, keep: &(dyn Fn() -> bool + Send + Sync)) {
        use std::sync::atomic::Ordering::Relaxed;
        loop {
            let going = keep();
            if going {
                tokio::select! {
                    _ = self.learned_full.notified() => {}
                    _ = tokio::time::sleep(LEARN_EVERY) => {}
                }
            }
            let rows = std::mem::take(&mut *self.learned.lock());
            if !rows.is_empty() {
                let n = rows.len() as u64;
                match w.apply(rows).await {
                    Ok(_) => {
                        self.learned_written.fetch_add(n, Relaxed);
                        self.learned_batches.fetch_add(1, Relaxed);
                    }
                    Err(e) => {
                        self.learned_dropped.fetch_add(n, Relaxed);
                        tracing::debug!("writing fetched DID documents to the seeds: {e:#}");
                    }
                }
            }
            if !going {
                return;
            }
        }
    }

    async fn reader(&self) -> Option<Arc<slatedb::DbReader>> {
        if let Some(r) = self.reader.read().await.clone() {
            return Some(r);
        }
        {
            let mut t = self.tried.lock();
            if t.is_some_and(|at| at.elapsed() < READER_POLL) {
                return None;
            }
            *t = Some(Instant::now());
        }
        let mut w = self.reader.write().await;
        if let Some(r) = w.clone() {
            return Some(r);
        }
        let opts = slatedb::config::DbReaderOptions {
            manifest_poll_interval: READER_POLL,
            skip_wal_replay: true,
            ..Default::default()
        };
        let path = db_path(&self.store);
        let (cache, id) = crate::qlog::cache::for_db(path.as_ref());
        match slatedb::DbReader::builder(path, self.store.raw.clone())
            .with_db_cache(cache, id)
            .with_merge_operator(merge_operator())
            .with_options(opts)
            .with_reader_mode(slatedb::DbReaderMode::FollowLatest)
            .with_metrics_recorder(slate_metrics::recorder(READER_LABEL))
            .build()
            .await
        {
            Ok(r) => {
                slate_metrics::register_reader(READER_LABEL, &r);
                let r = Arc::new(r);
                *w = Some(r.clone());
                Some(r)
            }
            Err(e) => {
                tracing::debug!("the PLC seed database isn't readable yet: {e}");
                None
            }
        }
    }

    pub async fn get(&self, did: &str) -> Option<Seed> {
        if self.paused.load(std::sync::atomic::Ordering::Relaxed) {
            return None;
        }
        let _slot = self.reads.acquire().await.ok()?;
        let writer = self.writer.read().clone();
        if let Some(w) = writer {
            return w.get(did).await.ok().flatten();
        }
        let r = self.reader().await?;
        match r.get(seed_key(did)).await {
            Ok(Some(b)) => Seed::decode(&b).ok(),
            Ok(None) => None,
            Err(e) => {
                tracing::debug!(did, "reading a PLC seed: {e}");
                None
            }
        }
    }
}

/// The identity cache's seeder on every member: the seed, weighed against
/// the account's record where this node holds it (the leader). It also
/// hands what the cache fetches to the seeds ([`SeedReader::learn`]).
pub struct Seeder {
    pub seeds: Arc<SeedReader>,
    pub state: Arc<crate::node::State>,
    pub ttl: Duration,
    /// A did:web row older than this is fetched again: no export follows
    /// did:web, so nothing else would replace it.
    pub web_ttl: Duration,
}

impl identity::Seeder for Seeder {
    fn seed<'a>(&'a self, did: &'a str) -> futures::future::BoxFuture<'a, Option<Identity>> {
        Box::pin(async move {
            // both are bucket reads when cold: side by side, a miss costs one
            // round trip before the fetch instead of two
            let (seed, rec) = tokio::join!(self.seeds.get(did), self.state.get(did));
            let seed = seed?;
            let now_ms = crate::policy::store::now_ms() as u64;
            if seed.lookup
                && !did.starts_with("did:plc:")
                && now_ms.saturating_sub(seed.created_ms) >= self.web_ttl.as_millis() as u64
            {
                return None;
            }
            let rec = rec.ok().flatten();
            let now = crate::state::now_secs();
            match choose(rec.as_deref(), Some(&seed), now, self.ttl.as_secs() as u32) {
                Pick::Seed => seed.identity(did),
                Pick::Record | Pick::Resolve => None,
            }
        })
    }

    fn learned(&self, did: &str, outcome: Result<&Identity, &identity::LookupError>, fetched_ms: u64) {
        let seed = match outcome {
            Ok(id) => Seed::from_lookup(Some(id), fetched_ms),
            Err(identity::LookupError::NotFound) => Seed::from_lookup(None, fetched_ms),
            Err(_) => return,
        };
        self.seeds.learn(did, seed);
    }
}

/// Drops `did`'s cached document when an op created after it was fetched
/// says something else.
pub fn invalidate_if_stale<F: identity::Fetch>(cache: &identity::IdentityCache<F>, did: &str, seed: &Seed) {
    let skew = LOOKUP_SKEW.as_millis() as u64;
    cache.invalidate_if(did, |fetched_ms, cached| {
        seed.created_ms + skew > fetched_ms && cached.is_none_or(|id| seed.differs_from(id))
    });
}

#[cfg(test)]
pub(crate) mod tests;
