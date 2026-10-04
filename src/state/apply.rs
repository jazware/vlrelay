//! `apply`: the DID owner's stateful step for one event.
//!
//! Order of checks for a commit: duplicate/stale (so host-shard takeover
//! replays ack cheaply), host authority, account status, the per-DID rate,
//! then the chain (rev and prevData) via the verify workstream's
//! `check_chain`.

use super::record::{AccountStatus, ChainState, DesyncReason, HostKey, Record, SigningKey, Upstream};
use super::shard::{ShardState, Ticket};
use super::{StateStore, StoreError};
use crate::types::Host;
use vlpds::cid::{CODEC_DAG_CBOR, Cid};
use vlpds::tid::Tid;

/// The verify workstream's chain check, behind a trait until
/// `verify::check_chain(prev, &Verified) -> Result<ChainState, ChainError>`
/// lands.
pub trait Chain: Send + Sync + 'static {
    type Verified: Send + Sync;
    /// The head the commit claims to produce.
    fn claimed(&self, v: &Self::Verified) -> ChainState;
    fn check_chain(&self, prev: Option<&ChainState>, v: &Self::Verified) -> Result<ChainState, ChainError>;
}

#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum ChainError {
    #[error("rev {rev} is not after {prev}")]
    RevNotNewer { rev: Tid, prev: Tid },
    #[error("prevData does not match the stored data CID")]
    PrevDataMismatch,
    #[error("{0}")]
    Other(String),
}

impl ChainError {
    fn reason(&self) -> DesyncReason {
        match self {
            ChainError::RevNotNewer { .. } => DesyncReason::RevNotNewer,
            ChainError::PrevDataMismatch => DesyncReason::PrevDataMismatch,
            ChainError::Other(_) => DesyncReason::Chain,
        }
    }
}

/// What a sync 1.1 commit claims, for [`StubChain`]: rev, commit, data, and
/// prevData.
#[derive(Clone, Copy, Debug)]
pub struct CommitClaim {
    pub rev: Tid,
    pub commit: Cid,
    pub data: Cid,
    pub prev_data: Option<Cid>,
}

/// Rev must move forward and prevData must match. A missing prevData (a
/// pre-1.1 host) passes.
pub struct StubChain;

impl Chain for StubChain {
    type Verified = CommitClaim;
    fn claimed(&self, v: &CommitClaim) -> ChainState {
        ChainState { rev: v.rev, commit: v.commit, data: v.data }
    }
    fn check_chain(&self, prev: Option<&ChainState>, v: &CommitClaim) -> Result<ChainState, ChainError> {
        if let Some(p) = prev {
            if v.rev <= p.rev {
                return Err(ChainError::RevNotNewer { rev: v.rev, prev: p.rev });
            }
            if v.prev_data.is_some_and(|d| d != p.data) {
                return Err(ChainError::PrevDataMismatch);
            }
        }
        Ok(self.claimed(v))
    }
}

/// A DID document's parts the relay keeps.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Identity {
    /// The `#atproto_pds` service's hostname.
    pub pds: Option<Host>,
    pub signing_key: Option<SigningKey>,
}

#[derive(Debug, Clone, thiserror::Error)]
#[error("identity lookup failed: {0}")]
pub struct IdentityError(pub String);

/// The identity cache (verify workstream's `identity.rs`), behind a trait
/// until it lands. `fresh` bypasses the cache.
#[async_trait::async_trait]
pub trait IdentitySource: Send + Sync {
    async fn resolve(&self, did: &str, fresh: bool) -> Result<Option<Identity>, IdentityError>;
}

/// The policy engine's decision on a DID the relay hasn't seen before.
pub trait AccountGate: Send + Sync {
    /// Called once per new account, after its host checked out. False:
    /// create it throttled (the host is at its account cap, or the
    /// cluster's new-account budget is spent).
    fn admit_account(&self, host: &str, did: &str) -> bool;
}

pub enum EventKind<V> {
    Commit(V),
    Sync { rev: Tid, commit: Cid, data: Cid },
    Identity,
    Account { active: bool, status: Option<String> },
}

pub struct Incoming<'a, V> {
    pub did: &'a str,
    pub host: &'a Host,
    /// Unix seconds.
    pub now: u32,
    pub kind: EventKind<V>,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
#[repr(u8)]
pub enum ChangeKind {
    Commit = 0,
    Sync = 1,
    Identity = 2,
    Account = 3,
}

/// What an accepted event did, compact enough to ride in its log entry:
/// replaying these rebuilds the shard's state (see `replay`).
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct StateDelta {
    pub did: String,
    pub host: HostKey,
    pub kind: ChangeKind,
    pub chain: Option<ChainState>,
    pub upstream: Upstream,
}

impl StateDelta {
    pub fn encode(&self) -> Vec<u8> {
        use bytes::BufMut;
        let mut b = Vec::with_capacity(self.did.len() + 96);
        super::record::put_str(&mut b, &self.did);
        b.put_u64(self.host.0);
        b.put_u8(self.kind as u8);
        b.put_u8(self.upstream as u8);
        match &self.chain {
            Some(c) => {
                b.put_u8(1);
                b.put_u64(c.rev.0);
                b.put_slice(&c.commit.digest);
                b.put_slice(&c.data.digest);
            }
            None => b.put_u8(0),
        }
        b
    }

    pub fn decode(b: &[u8]) -> Result<StateDelta, super::record::DecodeError> {
        use super::record::{DecodeError, Reader};
        let mut r = Reader(b);
        let did = r.str()?.to_string();
        let host = HostKey(r.u64()?);
        let kind = match r.u8()? {
            0 => ChangeKind::Commit,
            1 => ChangeKind::Sync,
            2 => ChangeKind::Identity,
            3 => ChangeKind::Account,
            _ => return Err(DecodeError),
        };
        let upstream = upstream_from(r.u8()?).ok_or(DecodeError)?;
        let chain = match r.u8()? {
            0 => None,
            _ => Some(ChainState {
                rev: Tid(r.u64()?),
                commit: Cid { codec: CODEC_DAG_CBOR, digest: r.array()? },
                data: Cid { codec: CODEC_DAG_CBOR, digest: r.array()? },
            }),
        };
        Ok(StateDelta { did, host, kind, chain, upstream })
    }
}

fn upstream_from(b: u8) -> Option<Upstream> {
    use Upstream::*;
    [Active, Takendown, Suspended, Deleted, Deactivated, Desynchronized, Throttled, Inactive].get(b as usize).copied()
}

#[derive(Debug)]
pub struct Accepted {
    pub ticket: Ticket,
    pub delta: StateDelta,
    pub status: AccountStatus,
    /// The status before, when this event changed it (emit `#account`).
    pub status_was: Option<AccountStatus>,
    /// The signing key changed: push it to host owners' key caches.
    pub key_changed: bool,
    pub new_account: bool,
}

#[derive(Debug)]
pub enum Applied {
    /// Append the event to the log, then `commit` its ticket once durable.
    Append(Accepted),
    /// Already applied (same commit at the current rev): ack, don't append.
    Duplicate,
}

#[derive(Debug, thiserror::Error)]
pub enum Reject {
    /// Older than the current rev: a replay of something already applied.
    #[error("rev {rev} is older than the current {current}")]
    Stale { rev: Tid, current: Tid },
    #[error("host {got} is not the DID's PDS ({expected:?})")]
    WrongHost { got: String, expected: Option<HostKey> },
    #[error("account is {0:?}")]
    Inactive(AccountStatus),
    #[error("account is desynchronized; waiting for #sync")]
    Desynchronized,
    #[error("chain check failed: {0}")]
    Chain(ChainError),
    #[error("over {limit} commits per minute")]
    RateLimited { limit: u32 },
    #[error("DID document not found or has no PDS")]
    NoIdentity,
    #[error("CID is not dag-cbor")]
    BadCid,
    #[error("shard {0} is not owned here")]
    NotOwner(vlpds::slots::ShardId),
    #[error(transparent)]
    Identity(#[from] IdentityError),
    #[error("state store: {0}")]
    Store(String),
}

impl Reject {
    /// Retrying later may succeed: don't count the upstream event as done.
    pub fn retryable(&self) -> bool {
        matches!(self, Reject::Identity(_) | Reject::Store(_) | Reject::NotOwner(_))
    }
}

impl From<StoreError> for Reject {
    fn from(e: StoreError) -> Reject {
        match e {
            StoreError::NotOwner(s) => Reject::NotOwner(s),
            e => Reject::Store(e.to_string()),
        }
    }
}

impl From<slatedb::Error> for Reject {
    fn from(e: slatedb::Error) -> Reject {
        Reject::Store(e.to_string())
    }
}

#[derive(Clone, Debug)]
pub struct ApplyConfig {
    /// A host mismatch re-resolves the DID document only if the stored one
    /// is at least this old, so a host claiming DIDs it doesn't serve can't
    /// turn every event into a PLC lookup.
    pub reresolve_after_secs: u32,
    pub max_commits_per_minute: Option<u32>,
    pub cache_entries_per_shard: usize,
}

impl Default for ApplyConfig {
    fn default() -> ApplyConfig {
        ApplyConfig { reresolve_after_secs: 30, max_commits_per_minute: None, cache_entries_per_shard: 1 << 20 }
    }
}

enum Authority {
    Ok,
    Wrong,
}

impl<C: Chain> StateStore<C> {
    pub async fn apply(&self, ev: Incoming<'_, C::Verified>) -> Result<Applied, Reject> {
        let shard = self.shard_for(ev.did)?;
        let _did_lock = shard.lock_did(ev.did).await;
        let prev = shard.load(ev.did).await?;
        let r = self.apply_locked(&shard, &ev, prev.as_deref()).await;
        let hk = HostKey::of(&ev.host.0);
        match &r {
            Ok(Applied::Append(a)) => {
                self.note_host(hk, &ev.host.0);
                let mut c = super::host::HostCounts { events: 1, ..Default::default() };
                if a.new_account {
                    c.accounts = 1;
                }
                self.add_host_counts(hk, c);
                if !a.new_account
                    && let Some(old) = prev.as_ref().map(|p| p.host)
                    && old != hk
                {
                    self.add_host_counts(hk, super::host::HostCounts { accounts: 1, ..Default::default() });
                    self.add_host_counts(old, super::host::HostCounts { accounts: -1, ..Default::default() });
                }
            }
            Ok(Applied::Duplicate) => {}
            Err(e) if !e.retryable() => {
                let mut c = super::host::HostCounts { dropped: 1, ..Default::default() };
                if matches!(e, Reject::Chain(_) | Reject::WrongHost { .. }) {
                    c.failed_checks = 1;
                }
                self.add_host_counts(hk, c);
            }
            Err(_) => {}
        }
        r
    }

    async fn apply_locked(
        &self,
        shard: &ShardState,
        ev: &Incoming<'_, C::Verified>,
        prev: Option<&Record>,
    ) -> Result<Applied, Reject> {
        let hk = HostKey::of(&ev.host.0);
        let new_account = prev.is_none();
        let mut rec = prev.cloned().unwrap_or_else(|| Record::new(hk, ev.now));
        let status_before = prev.map(|p| p.status());
        let key_before = rec.key.clone();

        // duplicates and stale replays first: they must ack without a lookup
        let claimed = match &ev.kind {
            EventKind::Commit(v) => Some(self.chain.claimed(v)),
            EventKind::Sync { rev, commit, data } => Some(ChainState { rev: *rev, commit: *commit, data: *data }),
            _ => None,
        };
        if let (Some(c), Some(cur)) = (claimed, rec.chain) {
            if c.rev == cur.rev && c.commit == cur.commit {
                return Ok(Applied::Duplicate);
            }
            // a #sync may restate the current rev with a new commit
            let stale = match ev.kind {
                EventKind::Sync { .. } => c.rev < cur.rev,
                _ => c.rev <= cur.rev,
            };
            if stale {
                return Err(Reject::Stale { rev: c.rev, current: cur.rev });
            }
        }
        if let Some(c) = claimed
            && (c.commit.codec != CODEC_DAG_CBOR || c.data.codec != CODEC_DAG_CBOR)
        {
            return Err(Reject::BadCid);
        }

        let fresh_identity = matches!(ev.kind, EventKind::Identity);
        match self.check_authority(&mut rec, ev, new_account, fresh_identity).await {
            Ok(Authority::Ok) => {}
            Ok(Authority::Wrong) => {
                let expected = rec.pds;
                rec.failed_checks = rec.failed_checks.saturating_add(1);
                if !new_account {
                    shard.stage_unlogged(ev.did, rec);
                }
                return Err(Reject::WrongHost { got: ev.host.0.clone(), expected });
            }
            Err(e) => return Err(e),
        }
        if new_account
            && let Some(g) = self.account_gate()
            && !g.admit_account(&ev.host.0, ev.did)
        {
            rec.relay_throttled = true;
        }

        let kind = match &ev.kind {
            EventKind::Commit(v) => {
                if rec.drops_commits() {
                    // kept, so the next event doesn't count as a new account again
                    if new_account {
                        shard.stage_unlogged(ev.did, rec.clone());
                    }
                    return Err(Reject::Inactive(rec.status()));
                }
                let minute = ev.now / 60;
                if rec.minute != minute {
                    rec.minute = minute;
                    rec.minute_commits = 0;
                }
                if let Some(limit) = self.config.max_commits_per_minute
                    && rec.minute_commits >= limit
                {
                    return Err(Reject::RateLimited { limit });
                }
                match self.chain.check_chain(rec.chain.as_ref(), v) {
                    Ok(cs) => {
                        rec.chain = Some(cs);
                        rec.desync = None;
                        rec.minute_commits += 1;
                    }
                    Err(e) => {
                        let was_desync = rec.desync.is_some();
                        rec.failed_checks = rec.failed_checks.saturating_add(1);
                        rec.desync.get_or_insert(e.reason());
                        shard.stage_unlogged(ev.did, rec);
                        return Err(if was_desync { Reject::Desynchronized } else { Reject::Chain(e) });
                    }
                }
                ChangeKind::Commit
            }
            EventKind::Sync { rev, commit, data } => {
                if rec.drops_commits() {
                    if new_account {
                        shard.stage_unlogged(ev.did, rec.clone());
                    }
                    return Err(Reject::Inactive(rec.status()));
                }
                rec.chain = Some(ChainState { rev: *rev, commit: *commit, data: *data });
                rec.desync = None;
                ChangeKind::Sync
            }
            EventKind::Identity => ChangeKind::Identity,
            EventKind::Account { active, status } => {
                rec.upstream = Upstream::from_event(*active, status.as_deref());
                ChangeKind::Account
            }
        };
        rec.host = hk;
        let status = rec.status();
        let delta = StateDelta { did: ev.did.to_string(), host: hk, kind, chain: rec.chain, upstream: rec.upstream };
        let key_changed = rec.key != key_before;
        let ticket = shard.stage_logged(ev.did, rec);
        Ok(Applied::Append(Accepted {
            ticket,
            delta,
            status,
            status_was: status_before.filter(|s| *s != status),
            key_changed,
            new_account,
        }))
    }

    /// The event's host must be the PDS the DID document names. A mismatch
    /// re-resolves once (if the stored document isn't brand new); `#identity`
    /// always re-resolves.
    async fn check_authority(
        &self,
        rec: &mut Record,
        ev: &Incoming<'_, C::Verified>,
        new_account: bool,
        force: bool,
    ) -> Result<Authority, Reject> {
        let hk = HostKey::of(&ev.host.0);
        if !force && !new_account && rec.fetched_at != 0 && rec.pds == Some(hk) {
            return Ok(Authority::Ok);
        }
        let recent = rec.fetched_at != 0 && ev.now.saturating_sub(rec.fetched_at) < self.config.reresolve_after_secs;
        if !force && !new_account && recent {
            return Ok(Authority::Wrong);
        }
        // a new DID tries the cache first, then one fresh lookup
        let mut fresh = force || !new_account;
        loop {
            let id = self.identity.resolve(ev.did, fresh).await?;
            let Some(id) = id else {
                return Err(Reject::NoIdentity);
            };
            rec.pds = id.pds.as_ref().map(|h| HostKey::of(&h.0));
            if let Some(h) = &id.pds {
                self.note_host(HostKey::of(&h.0), &h.0);
            }
            rec.key = id.signing_key;
            rec.fetched_at = ev.now.max(1);
            if rec.pds.is_none() {
                return Err(Reject::NoIdentity);
            }
            if rec.pds == Some(hk) {
                return Ok(Authority::Ok);
            }
            if fresh {
                return Ok(Authority::Wrong);
            }
            fresh = true;
        }
    }

    /// Rebuilds state from log entries the shard's SlateDB may not have
    /// (those after its applied marker). Idempotent: a chain only moves to a
    /// newer rev, and statuses are replayed in log order.
    pub async fn replay(&self, deltas: &[StateDelta], now: u32) -> Result<usize, Reject> {
        let mut n = 0;
        for d in deltas {
            let shard = self.shard_for(&d.did)?;
            let _g = shard.lock_did(&d.did).await;
            let prev = shard.load(&d.did).await?;
            let mut rec = prev.as_deref().cloned().unwrap_or_else(|| Record::new(d.host, now));
            let before = rec.clone();
            match d.kind {
                ChangeKind::Commit | ChangeKind::Sync => {
                    if let Some(c) = d.chain {
                        let newer = rec.chain.is_none_or(|cur| c.rev > cur.rev);
                        let resync = d.kind == ChangeKind::Sync && rec.chain.is_some_and(|cur| c.rev == cur.rev);
                        if newer || resync {
                            rec.chain = Some(c);
                            rec.desync = None;
                            rec.host = d.host;
                        }
                    }
                }
                ChangeKind::Account => {
                    rec.upstream = d.upstream;
                    rec.host = d.host;
                }
                // the key in the log entry's DID doc isn't carried: look it
                // up again before trusting it
                ChangeKind::Identity => rec.fetched_at = 0,
            }
            if prev.is_none() || rec != before {
                shard.stage_unlogged(&d.did, rec);
                n += 1;
            }
        }
        Ok(n)
    }
}
