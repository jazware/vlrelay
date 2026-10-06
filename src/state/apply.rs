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
use std::sync::Arc;
use vlpds::cid::{CODEC_DAG_CBOR, Cid};
use vlpds::tid::Tid;

/// The verify workstream's chain check, behind a trait until
/// `verify::check_chain(prev, &Verified) -> Result<ChainState, ChainError>`
/// lands.
pub trait Chain: Send + Sync + 'static {
    type Verified: Send + Sync;
    /// The head the commit claims to produce.
    fn claimed(&self, v: &Self::Verified) -> ChainState;
    /// The repo's first commit (see `verify::Verified::created`).
    fn created(&self, v: &Self::Verified) -> bool;
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

/// What a sync 1.1 commit claims, for [`StubChain`]: rev, commit, data,
/// prevData and since.
#[derive(Clone, Copy, Debug)]
pub struct CommitClaim {
    pub rev: Tid,
    pub commit: Cid,
    pub data: Cid,
    pub prev_data: Option<Cid>,
    pub since: Option<Tid>,
}

/// Rev must move forward and prevData must match. A missing prevData (a
/// pre-1.1 host) passes.
pub struct StubChain;

impl Chain for StubChain {
    type Verified = CommitClaim;
    fn claimed(&self, v: &CommitClaim) -> ChainState {
        ChainState { rev: v.rev, commit: v.commit, data: v.data }
    }
    fn created(&self, v: &CommitClaim) -> bool {
        v.since.is_none() && v.prev_data.is_none()
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

/// The policy engine's decision on an account it hasn't seen before.
pub trait AccountGate: Send + Sync {
    /// Called once the account's host checked out.
    fn admit_account(&self, host: &str, did: &str, how: Arrival) -> NewAccount;

    /// Takes one fresh DID document fetch from `host`'s own budget. An event
    /// whose host has none left is checked against the cached document, so
    /// one host's `#identity` stream can't spend everyone's PLC budget.
    fn forced_lookup(&self, _host: &str) -> bool {
        true
    }
}

/// How an account reaches the [`AccountGate`]. A relay starting cold sees
/// every established account for the first time, so only a repo's first
/// commit marks a newly created one: the rate caps and the new-account spam
/// signal are for those, the host's account cap for every account.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Arrival {
    /// Unknown here, and its event isn't its repo's first commit.
    FirstSeen,
    /// Unknown here, and its event is its repo's first commit.
    Created,
    /// Known (an `#identity` or `#account` came first) with no commit yet:
    /// this is its first. `created` when it's the repo's first.
    FirstCommit { created: bool },
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum NewAccount {
    Admit,
    /// Create it throttled: the host is at its account cap.
    Throttle,
    /// Drop this event and create nothing: a rate budget is spent for now,
    /// and the account's next event asks again.
    Defer,
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
    /// The record staged under `ticket`: what the quorum log's entry
    /// carries for its state.
    pub record: Box<Record>,
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
    /// Append the event with no state change: an `#identity` another host
    /// relayed for an account unknown here. The account is created, and
    /// gated, at its own PDS's first event.
    Pass,
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
    #[error("new account deferred: its host's or the cluster's new-account budget is spent")]
    NewAccountDeferred,
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
        self.apply_with_frame(ev, None).await
    }

    /// [`apply`](Self::apply) with the frame as received, which archival
    /// mode applies to the account's mirror.
    pub async fn apply_with_frame(
        &self,
        ev: Incoming<'_, C::Verified>,
        frame: Option<&bytes::Bytes>,
    ) -> Result<Applied, Reject> {
        let shard = self.shard_for(ev.did)?;
        self.apply_in(&shard, ev, frame).await
    }

    /// [`apply_with_frame`](Self::apply_with_frame) against `shard`, which
    /// needn't be the one this store routes the DID to: the quorum log's
    /// leader applies against its own term's view.
    pub async fn apply_in(
        &self,
        shard: &Arc<ShardState>,
        ev: Incoming<'_, C::Verified>,
        frame: Option<&bytes::Bytes>,
    ) -> Result<Applied, Reject> {
        let _did_lock = shard.lock_did(ev.did).await;
        self.apply_held(shard, ev, frame).await
    }

    /// [`apply_in`](Self::apply_in) with the DID's lock already held by the
    /// caller (`ShardState::lock_dids_owned`).
    pub async fn apply_held(
        &self,
        shard: &Arc<ShardState>,
        ev: Incoming<'_, C::Verified>,
        frame: Option<&bytes::Bytes>,
    ) -> Result<Applied, Reject> {
        let prev = shard.load(ev.did).await?;
        let r = self.apply_locked(shard, &ev, prev.as_deref(), frame).await;
        let hk = HostKey::of(&ev.host.0);
        match &r {
            Ok(Applied::Append(a)) => {
                self.note_host(hk, &ev.host.0);
                self.add_host_counts(hk, super::host::HostCounts { events: 1, ..Default::default() });
                // the account's host: the sender, except for an #identity
                // relayed by another host
                let owner = a.delta.host;
                if a.new_account {
                    self.add_host_counts(owner, super::host::HostCounts { accounts: 1, ..Default::default() });
                }
                if !a.new_account
                    && let Some(old) = prev.as_ref().map(|p| p.host)
                    && old != owner
                {
                    self.add_host_counts(owner, super::host::HostCounts { accounts: 1, ..Default::default() });
                    self.add_host_counts(old, super::host::HostCounts { accounts: -1, ..Default::default() });
                }
            }
            Ok(Applied::Pass) => {
                self.note_host(hk, &ev.host.0);
                self.add_host_counts(hk, super::host::HostCounts { events: 1, ..Default::default() });
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
        frame: Option<&bytes::Bytes>,
    ) -> Result<Applied, Reject> {
        let hk = HostKey::of(&ev.host.0);
        let mut mirror_rows = None;
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
        // the DID document is the authority on identity, not the host that
        // sent it (indigo passes #identity from any host): the re-resolve
        // above refreshed it, so emit it whoever sent it
        let mut from_owner = true;
        match self.check_authority(&mut rec, ev, new_account, fresh_identity).await {
            Ok(Authority::Ok) => {}
            Ok(Authority::Wrong) if fresh_identity => from_owner = false,
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
        // a record here would credit the account to the PDS its DID
        // document names without that host's cap or rate check, and its
        // first commit would then pass as FirstCommit: anyone could fill
        // any host's cap, or skirt their own
        if !from_owner && new_account {
            return Ok(Applied::Pass);
        }
        let created = matches!(&ev.kind, EventKind::Commit(v) if self.chain.created(v));
        let arrival = if !from_owner {
            // another host's #identity spends nothing of that host's
            // new-account budget
            None
        } else if new_account {
            Some(if created { Arrival::Created } else { Arrival::FirstSeen })
        } else if rec.chain.is_none()
            && matches!(ev.kind, EventKind::Commit(_) | EventKind::Sync { .. })
            && (created || !rec.drops_commits())
        {
            // a creation usually announces itself with #identity and
            // #account before its first commit; a throttled one still
            // counts as created, for the spam signal
            Some(Arrival::FirstCommit { created })
        } else {
            None
        };
        if let Some(how) = arrival
            && let Some(g) = self.account_gate()
        {
            match g.admit_account(&ev.host.0, ev.did, how) {
                NewAccount::Admit => {}
                NewAccount::Throttle => rec.relay_throttled = true,
                NewAccount::Defer => return Err(Reject::NewAccountDeferred),
            }
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
                        if let Some(f) = frame
                            && let crate::archive::Step::Rows(r) =
                                self.archive_commit(shard, ev.did, &ev.host.0, f).await
                        {
                            mirror_rows = Some(r);
                        }
                        rec.chain = Some(cs);
                        rec.desync = None;
                        rec.minute_commits += 1;
                    }
                    Err(e) => {
                        let was_desync = rec.desync.is_some();
                        if was_desync || e == ChainError::PrevDataMismatch {
                            self.archive_chain_broken(ev.did, &ev.host.0);
                        }
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
                if let Some(f) = frame
                    && let crate::archive::Step::Rows(r) = self.archive_sync(shard, ev.did, &ev.host.0, f).await
                {
                    mirror_rows = Some(r);
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
        if from_owner {
            rec.host = hk;
        }
        let status = rec.status();
        let delta =
            StateDelta { did: ev.did.to_string(), host: rec.host, kind, chain: rec.chain, upstream: rec.upstream };
        let key_changed = rec.key != key_before;
        let record = Box::new(rec.clone());
        let ticket = shard.stage_logged(ev.did, rec);
        if let Some(r) = mirror_rows {
            shard.mirror.attach(ticket.n, ev.did, r);
        }
        Ok(Applied::Append(Accepted {
            ticket,
            record,
            delta,
            status,
            status_was: status_before.filter(|s| *s != status),
            key_changed,
            new_account,
        }))
    }

    /// The event's host must be the PDS the DID document names. A mismatch
    /// re-resolves once (if the stored document isn't brand new). An
    /// `#identity` re-resolves when its host is the account's PDS, or the
    /// stored document isn't brand new. Fresh lookups spend the sending
    /// host's budget ([`AccountGate::forced_lookup`]).
    async fn check_authority(
        &self,
        rec: &mut Record,
        ev: &Incoming<'_, C::Verified>,
        new_account: bool,
        identity: bool,
    ) -> Result<Authority, Reject> {
        let hk = HostKey::of(&ev.host.0);
        let owner = rec.fetched_at != 0 && rec.pds == Some(hk);
        let recent = rec.fetched_at != 0 && ev.now.saturating_sub(rec.fetched_at) < self.config.reresolve_after_secs;
        let force = identity && (owner || !recent);
        if !force && !new_account && owner {
            return Ok(Authority::Ok);
        }
        if !force && !new_account && recent {
            return Ok(Authority::Wrong);
        }
        let budget = |host: &str| self.account_gate().is_none_or(|g| g.forced_lookup(host));
        // a new DID tries the cache first, then one fresh lookup
        let mut fresh = (force || !new_account) && budget(&ev.host.0);
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
            if fresh || !budget(&ev.host.0) {
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
