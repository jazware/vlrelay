//! Account takedowns on the policy's audit trail. The flag that drops an
//! account's events lives in its state record; this keeps who did it, when
//! and why, which the record has no room for:
//!
//! - `policy/takedowns/current/{sha256(did)}.json`: the latest action per
//!   account, for the account page.
//! - `policy/takedowns/audit/{at_ms:020}-{sha256(did)[..8]}.json`: one
//!   object per action, created with If-None-Match, so the log only grows.

use super::store::{get, now_ms, path, put};
use object_store::PutMode;
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use vlpds::store::Store;

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct TakedownEntry {
    pub did: String,
    /// True for a takedown, false for its reversal.
    pub takedown: bool,
    pub at_ms: i64,
    pub by: String,
    #[serde(default)]
    pub reason: String,
}

fn did_hash(did: &str) -> String {
    Sha256::digest(did.as_bytes())[..16].iter().map(|b| format!("{b:02x}")).collect()
}

pub struct Takedowns {
    store: Store,
}

impl Takedowns {
    pub fn new(store: Store) -> Takedowns {
        Takedowns { store }
    }

    /// Appends the action to the audit log, then records it as the
    /// account's latest. Call it before changing the account, so a failed
    /// write leaves no unaudited takedown.
    pub async fn record(&self, did: &str, takedown: bool, by: &str, reason: &str) -> anyhow::Result<TakedownEntry> {
        let e = TakedownEntry {
            did: did.to_string(),
            takedown,
            at_ms: now_ms(),
            by: by.to_string(),
            reason: reason.to_string(),
        };
        let h = did_hash(did);
        let body = serde_json::to_vec_pretty(&e)?;
        let audit = path(&self.store, &format!("policy/takedowns/audit/{:020}-{}.json", e.at_ms, &h[..8]));
        put(&self.store, &audit, body.clone(), PutMode::Create).await?;
        let cur = path(&self.store, &format!("policy/takedowns/current/{h}.json"));
        put(&self.store, &cur, body, PutMode::Overwrite).await?;
        tracing::info!(
            target: "vlrelay::audit",
            did,
            by,
            takedown,
            reason,
            "account takedown"
        );
        Ok(e)
    }

    pub async fn latest(&self, did: &str) -> anyhow::Result<Option<TakedownEntry>> {
        let p = path(&self.store, &format!("policy/takedowns/current/{}.json", did_hash(did)));
        Ok(match get(&self.store, &p, None).await? {
            Some((b, _)) => Some(serde_json::from_slice(&b)?),
            None => None,
        })
    }

    /// The newest `limit` actions, newest first.
    pub async fn audit(&self, limit: usize) -> anyhow::Result<Vec<TakedownEntry>> {
        use futures::TryStreamExt;
        use object_store::ObjectStore;
        let prefix = path(&self.store, "policy/takedowns/audit");
        let mut metas: Vec<_> = self.store.raw.list(Some(&prefix)).try_collect().await?;
        metas.sort_by(|a, b| b.location.cmp(&a.location));
        let mut out = Vec::new();
        for m in metas.into_iter().take(limit) {
            if let Some((b, _)) = get(&self.store, &m.location, None).await? {
                out.push(serde_json::from_slice(&b)?);
            }
        }
        Ok(out)
    }
}
