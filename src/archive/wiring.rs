//! The archive's ties to the rest of the node: the policy object as its
//! [`Gate`], and the DID document cache as its [`Resolver`].

use super::{Archive, FetchLimits, Gate, Resolved, Resolver};
use crate::identity::{HttpFetch, IdentityCache};
use crate::node::policy::PolicyHooks;
use crate::policy::Engine;
use crate::policy::budget::BudgetKind;
use crate::policy::doc::ArchiveMode;
use crate::state::{Chain, StateStore, Tier};
use std::sync::{Arc, OnceLock};

pub struct PolicyGate {
    pub engine: Arc<Engine>,
    /// The host tiers (`archive.mode: tiers`); set once the hooks exist.
    pub hooks: OnceLock<Arc<PolicyHooks>>,
}

impl PolicyGate {
    fn tier(&self, host: &str) -> Option<Tier> {
        self.hooks.get().and_then(|h| h.limits(host)).map(|l| l.tier)
    }
}

impl Gate for PolicyGate {
    fn wants(&self, host: &str) -> bool {
        let snap = self.engine.snapshot();
        let a = &snap.policy.body.archive;
        match a.mode {
            ArchiveMode::Off => false,
            ArchiveMode::All => true,
            ArchiveMode::Tiers => a.wants(host, self.tier(host)),
            ArchiveMode::Hosts => a.wants(host, None),
        }
    }

    fn version(&self) -> u64 {
        self.engine.snapshot().policy.version
    }

    fn takedown_retention_secs(&self) -> u32 {
        self.engine.snapshot().policy.body.archive.takedown_retention_hours.saturating_mul(3600)
    }

    fn limits(&self, host: &str) -> FetchLimits {
        let snap = self.engine.snapshot();
        let tiers = &snap.policy.body.tiers;
        let per_host = if host.is_empty() {
            1.0
        } else {
            let t = self.tier(host).unwrap_or(Tier::Default);
            tiers.get(t).unwrap_or(&tiers.throttled).archival_fetches_per_host
        };
        FetchLimits {
            per_host_per_sec: per_host,
            concurrency: self.engine.budget(BudgetKind::ArchivalFetchConcurrency) as usize,
            bytes_per_sec: self.engine.budget(BudgetKind::ArchivalFetchBytesPerSec),
        }
    }
}

pub struct IdentityResolver(pub Arc<IdentityCache<HttpFetch>>);

#[async_trait::async_trait]
impl Resolver for IdentityResolver {
    async fn resolve(&self, did: &str) -> anyhow::Result<Resolved> {
        let id = self.0.resolve(did).await.map_err(|e| anyhow::anyhow!("DID document: {e}"))?;
        let endpoint = id.pds.clone().ok_or_else(|| anyhow::anyhow!("DID document names no PDS"))?;
        let key = id.signing_key.clone().ok_or_else(|| anyhow::anyhow!("DID document has no signing key"))?;
        Ok(Resolved { endpoint, key })
    }
}

/// Installs archival mode on `state` (inert until the policy turns it on)
/// and starts its workers. Call before the shards' recovery, so replay
/// rebuilds the mirrors too.
pub fn install<C: Chain>(
    state: &Arc<StateStore<C>>,
    engine: Arc<Engine>,
    identity: Arc<IdentityCache<HttpFetch>>,
) -> (Arc<Archive>, Arc<PolicyGate>) {
    let gate = Arc::new(PolicyGate { engine, hooks: OnceLock::new() });
    let a = Archive::new(gate.clone(), Arc::new(IdentityResolver(identity)));
    state.set_archive(a.clone());
    a.spawn(state.clone());
    (a, gate)
}
