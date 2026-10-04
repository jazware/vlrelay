//! DID documents in bulk from the PLC directory's `/export`, so a cold relay
//! doesn't resolve its ~56M accounts one lookup at a time.
//!
//! The ingester ([`ingest`]) streams the export's operations and keeps, per
//! did:plc, the latest op's `#atproto` signing key, its `atproto_pds`
//! endpoint, whether it's a tombstone, and the op's `createdAt`. Entries live
//! in their DID shard's SlateDB under their own tag, beside the state
//! records (see [`seed_key`]), written by the shard's owner.
//!
//! On a DID document cache miss, [`LocalSeeds::pick`] weighs the seed
//! against the DID's state record ([`choose`]), and a usable one fills the
//! cache without spending the PLC lookup budget. A forced refresh (an
//! `#identity`, a signature that fails against the seeded key, a host that
//! doesn't match) still goes to PLC: the export is PLC's own view, but a
//! signed event and a fresh resolve win over it.
//!
//! Validation is deliberately thin: each line must be a well-formed op of a
//! known type (vlpds's `plc::op_type`) for a valid did:plc, and nullified ops
//! are skipped. The op chain (each op's signature by a rotation key of the
//! op before it, and forks inside the recovery window) isn't checked: that
//! needs every DID's previous rotation keys (~70 bytes more per DID) and the
//! nullification rules, and buys little when every commit's signature is
//! still checked against the seeded key, and a failure re-resolves from PLC.

pub mod ingest;
pub mod peer;

use crate::identity::{self, Identity};
use crate::state::record::{HostKey, Reader, Record, put_str, put_varint};
use crate::state::{Chain, StateStore, StoreError};
use crate::types::Host;
use crate::verify::SigningKey;
use bytes::Bytes;
use serde_json::Value as J;
use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;

/// Seed rows: `0x03 ‖ slot ‖ DID`, outside the `0x01` state records so
/// `listRepos` scans never walk them. A split or merge doesn't carry them
/// yet ([`crate::state::RESHARD_FAMILIES`]).
pub const SEED_TAG: u8 = 0x03;

/// Seed rows of slots [lo, hi).
pub fn seed_range_keys(lo: u32, hi: u32) -> (Bytes, Bytes) {
    let at = |s: u32| -> Bytes {
        if s >= vlpds::slots::SLOTS {
            Bytes::from_static(&[SEED_TAG + 1])
        } else {
            let [a, b] = (s as u16).to_be_bytes();
            Bytes::copy_from_slice(&[SEED_TAG, a, b])
        }
    };
    (at(lo), at(hi))
}

pub fn seed_key(did: &str) -> Vec<u8> {
    let slot = vlpds::slots::slot_of(did);
    let mut k = Vec::with_capacity(4 + 15);
    k.push(SEED_TAG);
    k.extend_from_slice(&slot.to_be_bytes());
    match crate::state::record::plc_bytes(did) {
        Some(b) => {
            k.push(b'p');
            k.extend_from_slice(&b);
        }
        None => {
            k.push(b'w');
            k.extend_from_slice(did.strip_prefix("did:").unwrap_or(did).as_bytes());
        }
    }
    k
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
}

const VERSION: u8 = 1;
const F_TOMBSTONE: u8 = 1;
const F_KEY: u8 = 2;
const F_PDS: u8 = 4;
const F_PDS_HTTP: u8 = 8;

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
        b.push(VERSION);
        b.push(flags);
        put_varint(&mut b, self.created_ms);
        if let Some(k) = &self.key {
            b.push(k.len() as u8);
            b.extend_from_slice(k);
        }
        if let Some(p) = &self.pds {
            put_str(&mut b, p);
        }
        Bytes::from(b)
    }

    pub fn decode(b: &[u8]) -> Result<Seed, DecodeError> {
        let mut r = Reader(b);
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
        let pds = if flags & F_PDS != 0 { Some(r.str().map_err(d)?.to_string()) } else { None };
        Ok(Seed { created_ms, tombstone: flags & F_TOMBSTONE != 0, key, pds, pds_http: flags & F_PDS_HTTP != 0 })
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
            pds: Some(self.endpoint(pds)),
            pds_host: Some(Host(pds.clone())),
            handle: None,
        })
    }

    fn endpoint(&self, host: &str) -> String {
        format!("{}://{host}", if self.pds_http { "http" } else { "https" })
    }

    /// Whether a cached document made from `self` would differ from one
    /// made from `other`.
    fn differs(&self, other: &Seed) -> bool {
        self.tombstone != other.tombstone
            || self.key != other.key
            || self.pds != other.pds
            || self.pds_http != other.pds_http
    }
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
    if !vlpds::plc::valid_plc_did(did) {
        return Err(LineError::Did);
    }
    let created_at = v.get("createdAt").and_then(J::as_str).ok_or(LineError::CreatedAt)?;
    let created_ms = parse_ms(created_at).ok_or(LineError::CreatedAt)?;
    if v.get("nullified").and_then(J::as_bool) == Some(true) {
        return Err(LineError::Nullified);
    }
    let op = v.get("operation").ok_or(LineError::Op)?;
    let ty = vlpds::plc::op_type(op, true).map_err(|_| LineError::Op)?;
    let seed = match ty {
        vlpds::plc::OpType::Tombstone => Seed { created_ms, tombstone: true, key: None, pds: None, pds_http: false },
        vlpds::plc::OpType::Operation | vlpds::plc::OpType::LegacyCreate => {
            let (key, pds) = if ty == vlpds::plc::OpType::LegacyCreate {
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
                pds_http: pds.is_some_and(|p| p.trim().get(..7).is_some_and(|s| s.eq_ignore_ascii_case("http://"))),
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

/// The DID shards this node holds, as a seed store.
pub struct LocalSeeds<C: Chain> {
    pub state: Arc<StateStore<C>>,
    pub ttl: Duration,
    /// Ops created after this (unix ms) may postdate a cached document of a
    /// DID that had no seed yet, so writing them drops the cached copy.
    pub recent_after_ms: u64,
}

#[derive(Default, Debug)]
pub struct Applied {
    pub written: usize,
    /// DIDs whose cached documents may now be stale.
    pub changed: Vec<String>,
}

impl<C: Chain> LocalSeeds<C> {
    pub fn new(state: Arc<StateStore<C>>, ttl: Duration) -> LocalSeeds<C> {
        let recent_after_ms = crate::policy::store::now_ms().saturating_sub(ttl.as_millis() as i64) as u64;
        LocalSeeds { state, ttl, recent_after_ms }
    }

    pub async fn get(&self, did: &str) -> Result<Option<Seed>, StoreError> {
        let s = self.state.shard_for(did)?;
        match s.db.get(seed_key(did)).await? {
            Some(b) => Ok(Some(Seed::decode(&b).map_err(|e| StoreError::Other(format!("{e} for {did}")))?)),
            None => Ok(None),
        }
    }

    /// What the cache should hold for `did`, without a PLC fetch. None
    /// means resolve. `NotOwner` when another node holds the DID.
    pub async fn pick(&self, did: &str) -> Result<Option<Identity>, StoreError> {
        let rec = self.state.get(did).await?;
        let seed = self.get(did).await?;
        let now = crate::state::now_secs();
        Ok(match choose(rec.as_deref(), seed.as_ref(), now, self.ttl.as_secs() as u32) {
            Pick::Resolve => None,
            Pick::Seed => seed.and_then(|s| s.identity(did)),
            Pick::Record => rec.and_then(|r| self.record_identity(did, &r, seed.as_ref())),
        })
    }

    /// The record keeps only the PDS's host, so the scheme comes from the
    /// seed when it names the same host.
    fn record_identity(&self, did: &str, rec: &Record, seed: Option<&Seed>) -> Option<Identity> {
        let (key, pds) = (rec.key.as_ref()?, rec.pds?);
        let pds_host = self.state.host_name(pds)?;
        let http = seed.is_some_and(|s| s.pds_http && s.pds.as_deref() == Some(&*pds_host));
        let mb = format!("z{}", bs58::encode(&key.0).into_string());
        let k = SigningKey::from_multibase(&mb).ok()?;
        Some(Identity {
            did: did.to_string(),
            signing_key: Some(k),
            signing_key_multibase: Some(mb),
            pds: Some(format!("{}://{pds_host}", if http { "http" } else { "https" })),
            pds_host: Some(Host(pds_host.to_string())),
            handle: None,
        })
    }

    /// Writes the entries newer than what each DID has, to the memtable:
    /// [`Self::flush`] makes them durable. Every DID must be in a shard
    /// held here (else `NotOwner`, and nothing of the batch's later DIDs
    /// is written).
    pub async fn apply(&self, ops: Vec<(String, Seed)>) -> Result<Applied, StoreError> {
        let mut by: HashMap<vlpds::slots::ShardId, Vec<(String, Seed)>> = HashMap::new();
        for (did, seed) in ops {
            let s = self.state.shard_for(&did)?;
            by.entry(s.id).or_default().push((did, seed));
        }
        let mut out = Applied::default();
        for (id, ops) in by {
            let s = self.state.shard(id).ok_or(StoreError::NotOwner(id))?;
            // Unlocked: only the reader writes seed rows, and two readers
            // (a leadership handover) write the same ops, so the worst race
            // briefly keeps an op a few seconds older.
            let mut wb = slatedb::WriteBatch::new();
            let before = out.written;
            for (did, seed) in &ops {
                let k = seed_key(did);
                let prev = match s.db.get(&k).await? {
                    Some(b) => Seed::decode(&b).ok(),
                    None => None,
                };
                if prev.as_ref().is_some_and(|p| p.created_ms >= seed.created_ms) {
                    continue;
                }
                let changed = match &prev {
                    Some(p) => p.differs(seed),
                    None => seed.created_ms > self.recent_after_ms,
                };
                if changed {
                    out.changed.push(did.clone());
                }
                wb.put(k, seed.encode());
                out.written += 1;
            }
            // SlateDB refuses an empty batch
            if out.written > before {
                s.db.write(wb).await?;
            }
        }
        Ok(out)
    }

    /// Flushes every held shard's memtable, so what [`Self::apply`] wrote
    /// survives a crash (the shards run without a WAL).
    pub async fn flush(&self) -> Result<(), StoreError> {
        for s in self.state.shards() {
            s.flush_memtable().await?;
        }
        Ok(())
    }
}

/// [`LocalSeeds`] as the cache's seeder on a single node.
pub struct SingleNode<C: Chain>(pub Arc<LocalSeeds<C>>);

impl<C: Chain> identity::Seeder for SingleNode<C> {
    fn seed<'a>(&'a self, did: &'a str) -> futures::future::BoxFuture<'a, Option<Identity>> {
        Box::pin(async move { self.0.pick(did).await.ok().flatten() })
    }
}

/// The ingester's sink on a single node: the local shards, and the local
/// cache dropping DIDs whose documents changed.
pub struct LocalSink<C: Chain, F: identity::Fetch> {
    pub seeds: Arc<LocalSeeds<C>>,
    pub cache: Arc<identity::IdentityCache<F>>,
}

#[async_trait::async_trait]
impl<C: Chain, F: identity::Fetch> ingest::Sink for LocalSink<C, F> {
    async fn apply(&self, ops: Vec<(String, Seed)>) -> anyhow::Result<usize> {
        let a = self.seeds.apply(ops).await?;
        for d in &a.changed {
            self.cache.invalidate(d);
        }
        Ok(a.written)
    }

    async fn flush(&self) -> anyhow::Result<()> {
        Ok(self.seeds.flush().await?)
    }
}

#[cfg(test)]
mod tests;
