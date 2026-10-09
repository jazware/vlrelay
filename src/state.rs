//! Per-DID sync state: each account's record, as the quorum log's leader
//! sees it (docs/quorum.md, "The relay on the log").
//!
//! The records live in the quorum state's one SlateDB, written only by the
//! log's applier from committed entries. The leader decides each event
//! against a [`ShardState`] over that database (its records as applied,
//! plus what its term appended and the applier hasn't reached), attached
//! here for the term. Keys are vlpds's slot-major layout (65,536 slots by
//! DID hash), so `listRepos` pages are one range scan each.

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
use vlsync_store::slots::ShardId;

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
    pub head: vlatproto::cid::Cid,
    pub rev: vlatproto::tid::Tid,
    pub status: AccountStatus,
}

pub struct RepoPage {
    pub repos: Vec<RepoRow>,
    pub cursor: Option<String>,
}

pub struct StateStore<C: Chain = StubChain> {
    pub chain: C,
    pub identity: Arc<dyn IdentitySource>,
    pub config: ApplyConfig,
    /// The leader's view for its term; None on a follower.
    shard: RwLock<Option<Arc<ShardState>>>,
    host_names: RwLock<HashMap<HostKey, Arc<str>>>,
    host_counts: Mutex<HashMap<HostKey, HostCounts>>,
    gate: RwLock<Option<Arc<dyn AccountGate>>>,
    /// Alias host -> the host the relay reads it as (the policy's host
    /// aliases, resolved to the end of each chain).
    aliases: RwLock<HashMap<HostKey, HostKey>>,
}

impl<C: Chain> StateStore<C> {
    pub fn new(chain: C, identity: Arc<dyn IdentitySource>, config: ApplyConfig) -> StateStore<C> {
        StateStore {
            chain,
            identity,
            config,
            shard: Default::default(),
            host_names: Default::default(),
            host_counts: Default::default(),
            gate: Default::default(),
            aliases: Default::default(),
        }
    }

    /// Replaces the host aliases: a DID document naming an alias is
    /// answered by the host it's an alias of, and the other way round.
    pub fn set_host_aliases(&self, m: HashMap<HostKey, HostKey>) {
        *self.aliases.write() = m;
    }

    /// Whether `a` and `b` are one PDS: the same host, or aliases of one.
    pub fn same_host(&self, a: HostKey, b: HostKey) -> bool {
        if a == b {
            return true;
        }
        let m = self.aliases.read();
        if m.is_empty() {
            return false;
        }
        m.get(&a).copied().unwrap_or(a) == m.get(&b).copied().unwrap_or(b)
    }

    /// The policy's say on new accounts. Without one every account is
    /// created active.
    pub fn set_account_gate(&self, gate: Arc<dyn AccountGate>) {
        *self.gate.write() = Some(gate);
    }

    pub(crate) fn account_gate(&self) -> Option<Arc<dyn AccountGate>> {
        self.gate.read().clone()
    }

    /// Serves the records of a database someone else writes (the quorum
    /// log's state, `node::quorum`), for a leader's term.
    pub fn attach_shard(&self, s: Arc<ShardState>) {
        *self.shard.write() = Some(s);
    }

    /// Stops serving `s`, if it's still the one served.
    pub fn detach_shard(&self, s: &Arc<ShardState>) {
        let mut cur = self.shard.write();
        if cur.as_ref().is_some_and(|x| Arc::ptr_eq(x, s)) {
            *cur = None;
        }
    }

    pub fn shard(&self) -> Option<Arc<ShardState>> {
        self.shard.read().clone()
    }

    pub fn shard_for(&self, _routing: &str) -> Result<Arc<ShardState>, StoreError> {
        self.shard().ok_or(StoreError::NotOwner(ShardId(0)))
    }

    pub async fn get(&self, did: &str) -> Result<Option<Arc<Record>>, StoreError> {
        Ok(self.shard_for(did)?.load(did).await?)
    }

    /// Repos as applied (what the last committed entries left), in (slot,
    /// DID) order.
    pub async fn list_repos(&self, cursor: Option<&str>, limit: usize) -> Result<RepoPage, StoreError> {
        let limit = limit.max(1);
        let start = match cursor {
            Some(did) if !did.is_empty() => {
                let mut k = record::did_key(did);
                k.push(0);
                k
            }
            _ => vlsync_store::keys::slot_prefix(0).to_vec(),
        };
        let s = self.shard_for("")?;
        let (_, end) = s.range_keys();
        let mut repos = Vec::with_capacity(limit);
        let mut it = vlsync_store::keys::BatchedScan::new(s.db.scan(start..end.to_vec()).await?);
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
        Ok(RepoPage { repos, cursor: None })
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

    pub(crate) fn add_host_counts(&self, k: HostKey, c: HostCounts) {
        self.host_counts.lock().entry(k).or_default().add(&c);
    }

    /// Hands the batched per-host counters to `hosts`. Counters of hosts without a known name
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
}

pub fn now_secs() -> u32 {
    std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH).map(|d| d.as_secs() as u32).unwrap_or(0)
}

#[cfg(test)]
pub(crate) mod tests;
