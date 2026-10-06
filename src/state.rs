//! Per-DID sync state and host records, one SlateDB per DID shard.
//!
//! vlpds's layout carries over: 65,536 slots by DID hash, contiguous slot
//! ranges per shard, slot-major keys, a shard DB at `{prefix}/state/{id}`
//! opened with vlpds's settings (WAL off; durability comes from the relay
//! log, and a new owner rebuilds by replaying the log tail past the shard's
//! applied marker). Hosts hash into the same slots, so a host's record sits
//! in the shard whose owner subscribes to it.
//!
//! `listRepos` pages walk keys in (slot, DID) order: every page is one range
//! scan, and the cursor (the last DID) names its own slot, so it crosses
//! shards without any per-shard bookkeeping.

pub mod apply;
pub mod host;
pub mod record;
pub mod shard;

pub use apply::{
    Accepted, AccountGate, Applied, ApplyConfig, Arrival, Chain, ChainError, ChangeKind, CommitClaim, EventKind,
    Identity, IdentityError, IdentitySource, Incoming, NewAccount, Reject, StateDelta, StubChain,
};
pub use host::{Conn, HostCounts, HostPage, HostRecord, HostStore, HostUpdate, Tier};
pub use record::{AccountStatus, ChainState, HostKey, Record, SigningKey, Upstream};
pub use shard::{ShardState, Ticket};

use parking_lot::{Mutex, RwLock};
use std::collections::HashMap;
use std::sync::Arc;
use vlpds::slots::{ShardId, ShardRange};
use vlpds::store::Store;

#[derive(Debug, thiserror::Error)]
pub enum StoreError {
    #[error("shard {0} is not owned by this node")]
    NotOwner(ShardId),
    #[error("bad cursor")]
    BadCursor,
    #[error(transparent)]
    Slate(#[from] slatedb::Error),
    #[error(transparent)]
    Decode(#[from] record::DecodeError),
    #[error("{0}")]
    Other(String),
}

/// One `listRepos` row.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct RepoRow {
    pub did: String,
    pub head: vlpds::cid::Cid,
    pub rev: vlpds::tid::Tid,
    pub status: AccountStatus,
}

pub struct RepoPage {
    pub repos: Vec<RepoRow>,
    pub cursor: Option<String>,
}

/// The log tail a shard's new owner replays: entries of `log_id` after
/// `after`, each as the deltas it carried. The log-serve workstream
/// implements it over a node log's segments.
#[async_trait::async_trait]
pub trait ReplaySource: Send + Sync {
    async fn tail(
        &self,
        log_id: &str,
        shard: ShardId,
        after: Option<u64>,
    ) -> anyhow::Result<Vec<(u64, Vec<StateDelta>)>>;

    /// The same entries' (DID, frame) of the shard, for archival mode's
    /// mirrors. Sources that don't carry frames give none.
    async fn frames(
        &self,
        _log_id: &str,
        _shard: ShardId,
        _after: Option<u64>,
    ) -> anyhow::Result<Vec<(u64, Vec<(String, bytes::Bytes)>)>> {
        Ok(Vec::new())
    }
}

/// The slot-keyed key families of a DID shard's SlateDB, each one
/// contiguous key range per slot range, that a split or merge should carry
/// into the children (`vlpds::partition::clone_db_families`):
///
/// - `0x01 ‖ slot`: sync records, and archival mode's mirror rows (`V/` and
///   vlpds's generation-keyed record, MST and backlink families);
/// - `0x02 ‖ slot`: host records (single-node mode only; a cluster keeps
///   them in the bucket);
/// - `0x03 ‖ slot`: PLC export seeds.
///
/// `meta/applied/{log}` is per shard and stays behind: a child starts with
/// no log history, so it has nothing to replay from a marker.
pub const CLONE_FAMILIES: &[vlpds::partition::FamilyRange] =
    &[vlpds::state::slot_range_keys, record::host_range_keys, crate::plc_seed::seed_range_keys];

/// What a live split or merge carries: every family. The child then holds
/// each parent L0 SST once per family, which SlateDB's L0 merge needs fork
/// patch 5 for (vlpds's Cargo.toml). docs/cluster.md, "Resharding".
pub const RESHARD_FAMILIES: &[vlpds::partition::FamilyRange] = CLONE_FAMILIES;

pub struct StateStore<C: Chain = StubChain> {
    archive: std::sync::OnceLock<Arc<crate::archive::Archive>>,
    pub store: Store,
    pub chain: C,
    pub identity: Arc<dyn IdentitySource>,
    pub config: ApplyConfig,
    layout: RwLock<Vec<ShardRange>>,
    shards: RwLock<HashMap<ShardId, Arc<ShardState>>>,
    host_names: RwLock<HashMap<HostKey, Arc<str>>>,
    host_counts: Mutex<HashMap<HostKey, HostCounts>>,
    gate: RwLock<Option<Arc<dyn AccountGate>>>,
}

impl<C: Chain> StateStore<C> {
    pub fn new(
        store: Store,
        layout: Vec<ShardRange>,
        chain: C,
        identity: Arc<dyn IdentitySource>,
        config: ApplyConfig,
    ) -> StateStore<C> {
        StateStore {
            archive: Default::default(),
            store,
            chain,
            identity,
            config,
            layout: RwLock::new(layout),
            shards: Default::default(),
            host_names: Default::default(),
            host_counts: Default::default(),
            gate: Default::default(),
        }
    }

    pub(crate) fn archive_cell(&self) -> &std::sync::OnceLock<Arc<crate::archive::Archive>> {
        &self.archive
    }

    /// The policy's say on new accounts. Without one every account is
    /// created active.
    pub fn set_account_gate(&self, gate: Arc<dyn AccountGate>) {
        *self.gate.write() = Some(gate);
    }

    pub(crate) fn account_gate(&self) -> Option<Arc<dyn AccountGate>> {
        self.gate.read().clone()
    }

    pub fn set_layout(&self, layout: Vec<ShardRange>) {
        *self.layout.write() = layout;
    }

    pub fn shard_id_of_slot(&self, slot: u16) -> ShardId {
        let l = self.layout.read();
        let i = l.partition_point(|r| r.hi <= slot as u32).min(l.len() - 1);
        l[i].id
    }

    /// Opens shard `id`'s SlateDB and starts serving it. The caller then
    /// replays the log tail (`recover`) before routing events to it.
    pub async fn open_shard(
        &self,
        id: ShardId,
        cache: Option<&vlpds::partition::DiskCache>,
    ) -> anyhow::Result<Arc<ShardState>> {
        let range = self
            .layout
            .read()
            .iter()
            .find(|r| r.id == id)
            .copied()
            .ok_or_else(|| anyhow::anyhow!("shard {id} not in the layout"))?;
        let db = vlpds::partition::open_db(&self.store, id, cache).await?;
        let s = Arc::new(ShardState::new(id, range.lo, range.hi, Arc::new(db), self.config.cache_entries_per_shard));
        self.load_host_names(&s).await?;
        self.shards.write().insert(id, s.clone());
        Ok(s)
    }

    /// Serves a shard whose database someone else opened and writes (the
    /// quorum log's state, `node::quorum`): replaces any shard of that id.
    pub fn attach_shard(&self, s: Arc<ShardState>) {
        self.shards.write().insert(s.id, s);
    }

    /// Stops serving `id` if it's `s`, without touching its database.
    pub fn detach_shard(&self, s: &Arc<ShardState>) {
        let mut m = self.shards.write();
        if m.get(&s.id).is_some_and(|x| Arc::ptr_eq(x, s)) {
            m.remove(&s.id);
        }
    }

    /// Stops serving `id` and closes its DB. Logged changes still pending
    /// are dropped: the next owner replays them from the log.
    pub async fn close_shard(&self, id: ShardId) -> anyhow::Result<()> {
        let s = self.shards.write().remove(&id);
        if let Some(s) = s {
            s.flush_unlogged().await?;
            s.db.close().await?;
        }
        Ok(())
    }

    pub fn shard(&self, id: ShardId) -> Option<Arc<ShardState>> {
        self.shards.read().get(&id).cloned()
    }

    pub fn shards(&self) -> Vec<Arc<ShardState>> {
        let mut v: Vec<_> = self.shards.read().values().cloned().collect();
        v.sort_by_key(|s| s.lo);
        v
    }

    pub fn shard_for(&self, routing: &str) -> Result<Arc<ShardState>, StoreError> {
        self.shard_for_slot(vlpds::slots::slot_of(routing))
    }

    fn shard_for_slot(&self, slot: u16) -> Result<Arc<ShardState>, StoreError> {
        let id = self.shard_id_of_slot(slot);
        self.shard(id).ok_or(StoreError::NotOwner(id))
    }

    /// Replays `log_id`'s tail past `shard`'s applied marker, then
    /// checkpoints, so the shard's SlateDB holds everything the log does.
    pub async fn recover(
        &self,
        shard: ShardId,
        log_id: &str,
        src: &dyn ReplaySource,
        now: u32,
    ) -> anyhow::Result<usize> {
        let s = self.shard(shard).ok_or(StoreError::NotOwner(shard))?;
        let after = s.applied_marker(log_id).await?;
        let mut n = 0;
        let mut last = after;
        for (ord, deltas) in src.tail(log_id, shard, after).await? {
            if after.is_some_and(|a| ord <= a) {
                continue;
            }
            n += self.replay(&deltas, now).await.map_err(|e| anyhow::anyhow!("{e}"))?;
            last = Some(ord);
        }
        if self.archive().is_some() {
            let mut frames = Vec::new();
            for (ord, evs) in src.frames(log_id, shard, after).await? {
                if after.is_some_and(|a| ord <= a) {
                    continue;
                }
                frames.extend(evs);
                last = last.max(Some(ord));
            }
            self.archive_replay(&s, frames).await?;
        }
        if let Some(ord) = last {
            s.checkpoint(log_id, ord).await?;
        }
        Ok(n)
    }

    /// Commits tickets (durable in the log) grouped by shard. What the log
    /// finalizer calls before acking.
    pub async fn commit(&self, tickets: &[Ticket]) -> Result<usize, StoreError> {
        let mut by: HashMap<ShardId, Vec<u64>> = HashMap::new();
        for t in tickets {
            by.entry(t.shard).or_default().push(t.n);
        }
        let mut n = 0;
        for (id, ts) in by {
            let s = self.shard(id).ok_or(StoreError::NotOwner(id))?;
            n += s.commit(ts).await?;
        }
        Ok(n)
    }

    pub async fn get(&self, did: &str) -> Result<Option<Arc<Record>>, StoreError> {
        Ok(self.shard_for(did)?.load(did).await?)
    }

    /// Operator takedown (or its reversal). Written directly: the caller
    /// logs the `#account` event that announces it.
    pub async fn set_relay_takedown(&self, did: &str, takedown: bool) -> Result<Option<AccountStatus>, StoreError> {
        let s = self.shard_for(did)?;
        let _g = s.lock_did(did).await;
        let Some(cur) = s.load(did).await? else { return Ok(None) };
        let mut rec = (*cur).clone();
        rec.relay_takedown = takedown;
        // lifting a takedown is how an operator also lifts a relay throttle
        if !takedown {
            rec.relay_throttled = false;
        }
        let st = rec.status();
        s.stage_unlogged(did, rec);
        s.flush_unlogged().await?;
        Ok(Some(st))
    }

    pub async fn list_repos(&self, cursor: Option<&str>, limit: usize) -> Result<RepoPage, StoreError> {
        let limit = limit.max(1);
        let mut start = match cursor {
            Some(did) if !did.is_empty() => {
                let mut k = record::did_key(did);
                k.push(0);
                k
            }
            _ => vlpds::state::slot_prefix(0).to_vec(),
        };
        let mut repos = Vec::with_capacity(limit);
        loop {
            let slot = vlpds::state::key_slot(&start).ok_or(StoreError::BadCursor)?;
            let s = self.shard_for_slot(slot)?;
            let (_, end) = s.range_keys();
            let mut it = vlpds::state::BatchedScan::new(s.db.scan(start.clone()..end.to_vec()).await?);
            while let Some(kv) = it.next().await? {
                let Some(did) = record::did_from_key(&kv.key) else { continue };
                let rec = Record::decode(&kv.value)?;
                let Some(c) = rec.chain else { continue };
                repos.push(RepoRow { did, head: c.commit, rev: c.rev, status: rec.status() });
                if repos.len() == limit {
                    let cursor = repos.last().map(|r| r.did.clone());
                    return Ok(RepoPage { repos, cursor });
                }
            }
            if s.hi >= vlpds::slots::SLOTS {
                return Ok(RepoPage { repos, cursor: None });
            }
            start = vlpds::state::slot_prefix(s.hi as u16).to_vec();
        }
    }

    pub fn note_host(&self, k: HostKey, name: &str) {
        if self.host_names.read().contains_key(&k) {
            return;
        }
        self.host_names.write().insert(k, Arc::from(name));
    }

    pub fn host_name(&self, k: HostKey) -> Option<Arc<str>> {
        self.host_names.read().get(&k).cloned()
    }

    async fn load_host_names(&self, s: &ShardState) -> anyhow::Result<()> {
        let range = record::host_slot_key(s.lo)..record::host_slot_key(s.hi);
        let mut it = vlpds::state::BatchedScan::new(s.db.scan(range).await?);
        let mut names = Vec::new();
        while let Some(kv) = it.next().await? {
            if let Some(h) = record::host_from_key(&kv.key) {
                names.push(h.to_string());
            }
        }
        for h in names {
            self.note_host(HostKey::of(&h), &h);
        }
        Ok(())
    }

    pub(crate) fn add_host_counts(&self, k: HostKey, c: HostCounts) {
        self.host_counts.lock().entry(k).or_default().add(&c);
    }

    /// Hands the batched per-host counters to `hosts` (the registry, or this
    /// store itself on one node). Counters of hosts without a known name
    /// stay batched.
    pub async fn flush_host_counts(&self, hosts: &dyn HostStore) -> anyhow::Result<()> {
        let taken: Vec<(HostKey, HostCounts)> = self.host_counts.lock().drain().collect();
        let mut named = Vec::new();
        for (k, c) in taken {
            match self.host_name(k) {
                Some(n) => named.push((n.to_string(), c)),
                None => self.add_host_counts(k, c),
            }
        }
        if let Err(e) = hosts.add_counts(&named).await {
            for (n, c) in named {
                self.add_host_counts(HostKey::of(&n), c);
            }
            return Err(e);
        }
        Ok(())
    }

    async fn read_host(&self, s: &ShardState, hostname: &str) -> anyhow::Result<Option<HostRecord>> {
        Ok(match s.db.get(record::host_key(hostname)).await? {
            Some(b) => Some(serde_json::from_slice(&b)?),
            None => None,
        })
    }

    async fn write_host(&self, s: &ShardState, rec: &HostRecord) -> anyhow::Result<()> {
        s.put_raw(record::host_key(&rec.hostname), serde_json::to_vec(rec)?.into()).await?;
        self.note_host(HostKey::of(&rec.hostname), &rec.hostname);
        Ok(())
    }

    /// Serializes read-modify-writes of one host's record; the per-DID
    /// stripes double as host stripes (hostnames and DIDs never collide).
    async fn modify_host(
        &self,
        hostname: &str,
        create: bool,
        f: impl FnOnce(&mut HostRecord),
    ) -> anyhow::Result<Option<Arc<ShardState>>> {
        let s = self.shard_for(hostname)?;
        let _g = s.lock_did(hostname).await;
        let rec = match self.read_host(&s, hostname).await? {
            Some(r) => Some(r),
            None if create => Some(HostRecord::new(hostname, Tier::New, now_secs())),
            None => None,
        };
        let Some(mut rec) = rec else { return Ok(None) };
        f(&mut rec);
        self.write_host(&s, &rec).await?;
        drop(_g);
        Ok(Some(s))
    }
}

pub fn now_secs() -> u32 {
    std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH).map(|d| d.as_secs() as u32).unwrap_or(0)
}

#[async_trait::async_trait]
impl<C: Chain> HostStore for StateStore<C> {
    async fn get_host(&self, hostname: &str) -> anyhow::Result<Option<HostRecord>> {
        let s = self.shard_for(hostname)?;
        self.read_host(&s, hostname).await
    }

    async fn put_host(&self, rec: &HostRecord) -> anyhow::Result<()> {
        let s = self.shard_for(&rec.hostname)?;
        let _g = s.lock_did(&rec.hostname).await;
        self.write_host(&s, rec).await
    }

    async fn checkpoint_cursors(&self, cursors: &[(String, i64)]) -> anyhow::Result<()> {
        let mut touched: HashMap<ShardId, Arc<ShardState>> = HashMap::new();
        for (h, seq) in cursors {
            if let Some(s) = self.modify_host(h, false, |r| r.cursor = r.cursor.max(*seq)).await? {
                touched.insert(s.id, s);
            }
        }
        for s in touched.values() {
            s.flush_memtable().await?;
        }
        Ok(())
    }

    async fn add_counts(&self, counts: &[(String, HostCounts)]) -> anyhow::Result<()> {
        for (h, c) in counts {
            if c.is_zero() {
                continue;
            }
            self.modify_host(h, true, |r| {
                r.account_count += c.accounts;
                r.events += c.events;
                r.failed_checks += c.failed_checks;
                r.dropped += c.dropped;
            })
            .await?;
        }
        Ok(())
    }

    async fn update_host(&self, hostname: &str, f: HostUpdate<'_>) -> anyhow::Result<Option<HostRecord>> {
        let s = self.shard_for(hostname)?;
        let _g = s.lock_did(hostname).await;
        let cur = self.read_host(&s, hostname).await?;
        let Some(rec) = f(cur) else { return Ok(None) };
        anyhow::ensure!(rec.hostname == hostname, "update_host({hostname}) returned {}", rec.hostname);
        self.write_host(&s, &rec).await?;
        Ok(Some(rec))
    }

    async fn list_hosts(&self, cursor: Option<&str>, limit: usize) -> anyhow::Result<HostPage> {
        let limit = limit.max(1);
        let mut start = match cursor {
            Some(h) if !h.is_empty() => {
                let mut k = record::host_key(h);
                k.push(0);
                k
            }
            _ => record::host_slot_key(0),
        };
        let mut hosts = Vec::new();
        loop {
            let slot = u16::from_be_bytes([start[1], start[2]]);
            let s = self.shard_for_slot(slot)?;
            let end = record::host_slot_key(s.hi);
            let mut it = vlpds::state::BatchedScan::new(s.db.scan(start.clone()..end).await?);
            while let Some(kv) = it.next().await? {
                hosts.push(serde_json::from_slice::<HostRecord>(&kv.value)?);
                if hosts.len() == limit {
                    let cursor = hosts.last().map(|h| h.hostname.clone());
                    return Ok(HostPage { hosts, cursor });
                }
            }
            if s.hi >= vlpds::slots::SLOTS {
                return Ok(HostPage { hosts, cursor: None });
            }
            start = record::host_slot_key(s.hi);
        }
    }
}

#[cfg(test)]
pub(crate) mod tests;
