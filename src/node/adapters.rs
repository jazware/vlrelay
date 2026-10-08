//! The seams between modules that were built apart: the verify workstream's
//! chain check and DID cache behind the state store's traits, and the
//! upstream registry's rows as host records.

use crate::identity::{Fetch, HttpFetch, IdentityCache, LookupError};
use crate::state::{self, Chain, IdentityError, IdentitySource};
use crate::upstream::{self, ErrorCounters, HostStatus};
use bytes::Bytes;
use std::sync::Arc;

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
pub struct CacheIdentity<F: Fetch = HttpFetch>(pub Arc<IdentityCache<F>>, pub Arc<super::patience::Patience>);

#[async_trait::async_trait]
impl<F: Fetch> IdentitySource for CacheIdentity<F> {
    async fn resolve(&self, did: &str, fresh: bool) -> Result<Option<state::Identity>, IdentityError> {
        match self.0.lookup_paced(did, fresh).await {
            Ok(id) => {
                self.1.succeeded(did);
                Ok(Some(state::Identity {
                    pds: id.pds_host.clone(),
                    signing_key: id.signing_key_multibase.as_deref().and_then(multikey_bytes).map(state::SigningKey),
                }))
            }
            Err(LookupError::NotFound | LookupError::BadDid) => Ok(None),
            Err(e @ LookupError::Failed(_)) => {
                Err(IdentityError { gave_up: self.1.leader_failed(did), msg: e.to_string() })
            }
            Err(e) => Err(IdentityError { msg: e.to_string(), gave_up: false }),
        }
    }
}

/// A `publicKeyMultibase` as the multicodec bytes the state record keeps.
fn multikey_bytes(mb: &str) -> Option<Bytes> {
    let raw = bs58::decode(mb.strip_prefix('z')?).into_vec().ok()?;
    (raw.len() <= state::record::MAX_KEY_LEN).then(|| Bytes::from(raw))
}

/// The upstream-only fields of a registry row, kept in a host record's
/// `extra` map under `upstream`.
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
        HostStatus::Active | HostStatus::Throttled | HostStatus::Backpressure => state::Conn::Active,
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
