//! The archive's ties to the rest of the node: the policy object as its
//! [`Gate`], and the DID document cache as its [`Resolver`].

use super::{Archive, FetchLimits, Gate, Resolved, Resolver};
use crate::identity::{HttpFetch, IdentityCache};
use crate::node::policy::PolicyHooks;
use crate::policy::Engine;
use crate::policy::budget::BudgetKind;
use crate::policy::doc::ArchiveMode;
use crate::state::{Chain, StateStore, Tier};
use axum::response::IntoResponse as _;
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

/// Archival reads from peers: the owner's half of [`PeerForward`].
pub const PEER_READS: &str = "/internal/relay/v1/archive";

pub fn peer_reads<C: Chain>(state: Arc<StateStore<C>>, token: String) -> axum::Router {
    use axum::extract::Request;
    use axum::middleware::{self, Next};
    let inner = super::read::router(state, None);
    let check = move |req: Request, next: Next| {
        let token = token.clone();
        async move {
            let t = req.headers().get(crate::cluster::peer::TOKEN_HEADER).and_then(|v| v.to_str().ok()).unwrap_or("");
            if t.is_empty() || !vlpds::auth::token_eq(&token, t) {
                return axum::http::StatusCode::UNAUTHORIZED.into_response();
            }
            next.run(req).await
        }
    };
    axum::Router::new().nest(PEER_READS, inner).layer(middleware::from_fn(check))
}

/// A read for a DID another core node owns goes to that node's peer
/// listener, and its answer (a streamed CAR or an error) comes back as is.
pub struct PeerForward(pub std::sync::Weak<crate::cluster::ClusterNode>);

#[async_trait::async_trait]
impl super::read::Forward for PeerForward {
    async fn forward(&self, did: &str, path_and_query: &str) -> Option<axum::response::Response> {
        let n = self.0.upgrade()?;
        let owner = n.owner_of_did(did)?;
        if owner.node_id == n.node_id {
            return None;
        }
        let http = n.http.as_ref()?;
        let url = format!("{}{PEER_READS}{path_and_query}", owner.addr.trim_end_matches('/'));
        let r = http.get(url).header(crate::cluster::peer::TOKEN_HEADER, n.internal_token()).send().await.ok()?;
        let status = axum::http::StatusCode::from_u16(r.status().as_u16()).ok()?;
        let ctype = r.headers().get(reqwest::header::CONTENT_TYPE).and_then(|v| v.to_str().ok()).map(str::to_string);
        let body = axum::body::Body::from_stream(r.bytes_stream());
        let mut resp = (status, body).into_response();
        if let Some(c) = ctype.and_then(|c| axum::http::HeaderValue::from_str(&c).ok()) {
            resp.headers_mut().insert(axum::http::header::CONTENT_TYPE, c);
        }
        Some(resp)
    }
}
