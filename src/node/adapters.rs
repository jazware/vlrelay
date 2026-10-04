//! The seams between modules that were built apart: the verify workstream's
//! chain check and DID cache behind the state store's traits, the state
//! store's host records behind the upstream registry, and the node log
//! behind the state store's replay.

use crate::identity::{Fetch, HttpFetch, IdentityCache, LookupError};
use crate::seq::{self, Logged};
use crate::state::{self, Chain, HostStore as _, IdentityError, IdentitySource, ReplaySource, StateDelta, StateStore};
use crate::types::Host;
use crate::upstream::{self, ErrorCounters, HostStatus};
use bytes::Bytes;
use parking_lot::Mutex;
use std::collections::HashMap;
use std::sync::Arc;
use vlpds::slots::ShardId;
use vlpds::store::Store;

/// `verify::check_chain` as the state store's [`Chain`].
pub struct VerifyChain;

impl Chain for VerifyChain {
    type Verified = crate::verify::Verified;

    fn claimed(&self, v: &Self::Verified) -> state::ChainState {
        state::ChainState { rev: v.rev, commit: v.commit, data: v.data }
    }

    fn created(&self, v: &Self::Verified) -> bool {
        v.created
    }

    fn check_chain(
        &self,
        prev: Option<&state::ChainState>,
        v: &Self::Verified,
    ) -> Result<state::ChainState, state::ChainError> {
        use crate::verify::{ChainError, ChainState, check_chain};
        let p = prev.map(|p| ChainState { rev: p.rev, data: p.data, commit: p.commit });
        match check_chain(p.as_ref(), v) {
            Ok(c) => Ok(state::ChainState { rev: c.rev, commit: c.commit, data: c.data }),
            // apply answers exact duplicates before it asks the chain
            Err(ChainError::Duplicate | ChainError::RevNotForward) => {
                Err(state::ChainError::RevNotNewer { rev: v.rev, prev: prev.map_or(v.rev, |p| p.rev) })
            }
            Err(ChainError::PrevDataMismatch { .. }) => Err(state::ChainError::PrevDataMismatch),
        }
    }
}

/// The DID document cache as the state store's [`IdentitySource`].
pub struct CacheIdentity<F: Fetch = HttpFetch>(pub Arc<IdentityCache<F>>);

#[async_trait::async_trait]
impl<F: Fetch> IdentitySource for CacheIdentity<F> {
    async fn resolve(&self, did: &str, fresh: bool) -> Result<Option<state::Identity>, IdentityError> {
        match self.0.lookup_paced(did, fresh).await {
            Ok(id) => Ok(Some(state::Identity {
                pds: id.pds_host.clone(),
                signing_key: id.signing_key_multibase.as_deref().and_then(multikey_bytes).map(state::SigningKey),
            })),
            Err(LookupError::NotFound | LookupError::BadDid) => Ok(None),
            Err(e) => Err(IdentityError(e.to_string())),
        }
    }
}

/// A `publicKeyMultibase` as the multicodec bytes the state record keeps.
fn multikey_bytes(mb: &str) -> Option<Bytes> {
    let raw = bs58::decode(mb.strip_prefix('z')?).into_vec().ok()?;
    (raw.len() <= state::record::MAX_KEY_LEN).then(|| Bytes::from(raw))
}

/// The upstream registry's rows kept in the state store's host records. The
/// upstream-only fields ride in the record's `extra` map under `upstream`.
pub struct StateHosts<C: Chain>(pub Arc<StateStore<C>>);

#[derive(serde::Serialize, serde::Deserialize, Default)]
#[serde(rename_all = "camelCase", default)]
struct UpstreamExtra {
    admitted_ms: u64,
    last_connected_ms: Option<u64>,
    errors: ErrorCounters,
}

pub(crate) fn tier_to_state(t: upstream::Tier) -> state::Tier {
    match t {
        upstream::Tier::Trusted => state::Tier::Trusted,
        upstream::Tier::Default => state::Tier::Default,
        upstream::Tier::New => state::Tier::New,
        upstream::Tier::Throttled => state::Tier::Throttled,
        upstream::Tier::Suspended => state::Tier::Suspended,
        upstream::Tier::Banned => state::Tier::Banned,
    }
}

fn tier_from_state(t: state::Tier) -> upstream::Tier {
    match t {
        state::Tier::Trusted => upstream::Tier::Trusted,
        state::Tier::Default => upstream::Tier::Default,
        state::Tier::New => upstream::Tier::New,
        state::Tier::Throttled => upstream::Tier::Throttled,
        state::Tier::Suspended => upstream::Tier::Suspended,
        state::Tier::Banned => upstream::Tier::Banned,
    }
}

pub(crate) fn to_upstream(r: &state::HostRecord) -> upstream::HostRecord {
    let x: UpstreamExtra =
        r.extra.get("upstream").and_then(|v| serde_json::from_value(v.clone()).ok()).unwrap_or_default();
    upstream::HostRecord {
        hostname: r.hostname.clone(),
        tier: tier_from_state(r.tier),
        status: HostStatus::Idle,
        // a cursor of 0 is the record's default: nothing acked yet
        acked_seq: (r.cursor > 0).then_some(r.cursor),
        last_connected_ms: x.last_connected_ms,
        admitted_ms: if x.admitted_ms > 0 { x.admitted_ms } else { r.first_seen as u64 * 1000 },
        account_count: r.account_count.max(0) as u64,
        errors: x.errors,
    }
}

/// A registry row's connection state, upstream-only fields and acked
/// cursor, onto the host's record (the tier is the caller's call).
pub(crate) fn apply_upstream(rec: &mut state::HostRecord, r: &upstream::HostRecord) {
    rec.conn = match r.status {
        HostStatus::Active | HostStatus::Throttled => state::Conn::Active,
        HostStatus::Idle => state::Conn::Idle,
        HostStatus::Connecting | HostStatus::Backoff => state::Conn::Offline,
    };
    let x =
        UpstreamExtra { admitted_ms: r.admitted_ms, last_connected_ms: r.last_connected_ms, errors: r.errors.clone() };
    if let Ok(v) = serde_json::to_value(x) {
        rec.extra.insert("upstream".into(), v);
    }
    if let Some(c) = r.acked_seq {
        rec.cursor = rec.cursor.max(c);
    }
}

impl<C: Chain> upstream::HostStore for StateHosts<C> {
    fn load(&self) -> upstream::host::StoreFuture<'_, Vec<upstream::HostRecord>> {
        Box::pin(async move {
            let mut out = Vec::new();
            let mut cursor: Option<String> = None;
            loop {
                let page = self.0.list_hosts(cursor.as_deref(), 1000).await?;
                out.extend(page.hosts.iter().map(to_upstream));
                match page.cursor {
                    Some(c) => cursor = Some(c),
                    None => return Ok(out),
                }
            }
        })
    }

    /// The registry's tier only seeds a new record: after that the policy
    /// engine owns it (operator actions, the driver's throttles), and the
    /// registry follows the record, never the other way round.
    fn put(&self, records: Vec<upstream::HostRecord>) -> upstream::host::StoreFuture<'_, ()> {
        Box::pin(async move {
            for r in &records {
                let conn = match r.status {
                    HostStatus::Active | HostStatus::Throttled => state::Conn::Active,
                    HostStatus::Idle => state::Conn::Idle,
                    HostStatus::Connecting | HostStatus::Backoff => state::Conn::Offline,
                };
                let x = serde_json::to_value(UpstreamExtra {
                    admitted_ms: r.admitted_ms,
                    last_connected_ms: r.last_connected_ms,
                    errors: r.errors.clone(),
                })?;
                let tier = tier_to_state(r.tier);
                let hostname = r.hostname.clone();
                self.0
                    .update_host(
                        &r.hostname,
                        Box::new(move |cur| {
                            let mut rec =
                                cur.unwrap_or_else(|| state::HostRecord::new(&hostname, tier, state::now_secs()));
                            rec.conn = conn;
                            rec.extra.insert("upstream".into(), x);
                            Some(rec)
                        }),
                    )
                    .await?;
            }
            // every row, cursor or not: checkpoint_cursors flushes the
            // memtable of each shard it touches, and nothing else does
            let cursors: Vec<(String, i64)> =
                records.iter().map(|r| (r.hostname.clone(), r.acked_seq.unwrap_or(0))).collect();
            self.0.checkpoint_cursors(&cursors).await
        })
    }
}

/// One log's segments read once from the lowest ordinal any shard needs,
/// then served to every shard's `recover`.
pub type Tail = Arc<Vec<(u64, Vec<Logged>)>>;

/// (from, end: None = to the log's end, the segments read)
type Cached = (u64, Option<u64>, Tail);

pub struct LogReplay {
    store: Store,
    cache: Mutex<HashMap<String, Cached>>,
    /// Per log: a takeover opens many shards of one dead log at once, and
    /// each would otherwise read it in full.
    reading: Mutex<HashMap<String, Arc<tokio::sync::Mutex<()>>>>,
}

impl LogReplay {
    pub fn new(store: Store) -> LogReplay {
        LogReplay { store, cache: Mutex::new(HashMap::new()), reading: Mutex::new(HashMap::new()) }
    }

    pub async fn read(&self, log_id: &str, from: u64) -> anyhow::Result<Tail> {
        self.read_span(log_id, from, None).await
    }

    /// The segments of `log_id` in `[from, end)`. A bounded span (an
    /// earlier owner's, closed by its release or the takeover's fence) is
    /// read without a LIST and never past its end: a log that lived on
    /// long after it held the shard made every open read the rest of it.
    pub async fn read_span(&self, log_id: &str, from: u64, end: Option<u64>) -> anyhow::Result<Tail> {
        let hit = |c: &Cached| c.0 <= from && (c.1.is_none() || end.is_some_and(|e| Some(e) <= c.1));
        if let Some(c) = self.cache.lock().get(log_id).filter(|c| hit(c)) {
            return Ok(c.2.clone());
        }
        if end.is_some_and(|e| e <= from) {
            return Ok(Arc::default());
        }
        let lock = self.reading.lock().entry(log_id.to_string()).or_default().clone();
        let _one = lock.lock().await;
        if let Some(c) = self.cache.lock().get(log_id).filter(|c| hit(c)) {
            return Ok(c.2.clone());
        }
        use futures::StreamExt;
        let to = match end {
            Some(e) => e,
            None => vlpds::nodelog::first_free(&self.store, log_id).await?.0,
        };
        let segs: Vec<anyhow::Result<(u64, Option<Vec<Logged>>)>> = futures::stream::iter(from..to)
            .map(|ord| async move { Ok((ord, seq::read_segment(&self.store, log_id, ord).await?)) })
            .buffered(16)
            .collect()
            .await;
        let mut out = Vec::new();
        for s in segs {
            let (ord, evs) = s?;
            if let Some(evs) = evs {
                out.push((ord, evs));
            }
        }
        let t: Tail = Arc::new(out);
        self.cache.lock().insert(log_id.to_string(), (from, end, t.clone()));
        Ok(t)
    }

    /// Every (host, upstream seq) the cached tails hold, for hosts whose
    /// durable cursor is below it.
    /// (upstream seq, `did_key`) of each logged event past its host's cursor.
    pub fn logged_above(&self, cursor: impl Fn(&Host) -> i64) -> HashMap<Host, std::collections::HashSet<(i64, u64)>> {
        let mut out: HashMap<Host, std::collections::HashSet<(i64, u64)>> = HashMap::new();
        for (_, _, t) in self.cache.lock().values() {
            for (_, evs) in t.iter() {
                for e in evs {
                    if e.meta.upstream_seq > 0 && e.meta.upstream_seq > cursor(&e.meta.host) {
                        let k = (e.meta.upstream_seq, super::cluster::did_key(&e.meta.did));
                        out.entry(e.meta.host.clone()).or_default().insert(k);
                    }
                }
            }
        }
        out
    }

    pub fn clear(&self) {
        self.cache.lock().clear();
    }
}

#[async_trait::async_trait]
impl ReplaySource for LogReplay {
    async fn tail(
        &self,
        log_id: &str,
        shard: ShardId,
        after: Option<u64>,
    ) -> anyhow::Result<Vec<(u64, Vec<StateDelta>)>> {
        let from = after.map_or(0, |a| a + 1);
        let t = self.read(log_id, from).await?;
        deltas_of(&t, log_id, shard, from)
    }
    async fn frames(
        &self,
        log_id: &str,
        shard: ShardId,
        after: Option<u64>,
    ) -> anyhow::Result<Vec<(u64, Vec<(String, bytes::Bytes)>)>> {
        let from = after.map_or(0, |a| a + 1);
        let t = self.read(log_id, from).await?;
        Ok(frames_of(&t, shard, from))
    }
}

/// `shard`'s state deltas in `t` from ordinal `from` on.
pub fn deltas_of(t: &Tail, log_id: &str, shard: ShardId, from: u64) -> anyhow::Result<Vec<(u64, Vec<StateDelta>)>> {
    let mut out = Vec::new();
    for (ord, evs) in t.iter() {
        if *ord < from {
            continue;
        }
        let deltas: Vec<StateDelta> = evs
            .iter()
            .filter(|e| e.meta.shard == shard.0)
            .filter_map(|e| e.delta.as_ref())
            .map(|d| StateDelta::decode(d).map_err(|e| anyhow::anyhow!("{log_id}/{ord}: bad state delta: {e}")))
            .collect::<anyhow::Result<_>>()?;
        if !deltas.is_empty() {
            out.push((*ord, deltas));
        }
    }
    Ok(out)
}

/// `shard`'s frames in `t` from ordinal `from` on.
pub fn frames_of(t: &Tail, shard: ShardId, from: u64) -> Vec<(u64, Vec<(String, bytes::Bytes)>)> {
    let mut out = Vec::new();
    for (ord, evs) in t.iter() {
        if *ord < from {
            continue;
        }
        let frames: Vec<(String, bytes::Bytes)> =
            evs.iter().filter(|e| e.meta.shard == shard.0).map(|e| (e.meta.did.clone(), e.frame.clone())).collect();
        if !frames.is_empty() {
            out.push((*ord, frames));
        }
    }
    out
}
