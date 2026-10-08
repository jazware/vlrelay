//! Account takedowns on the policy's audit trail. The flag that drops an
//! account's events lives in its state record; this keeps who did it, when
//! and why, which the record has no room for:
//!
//! - `policy/takedowns/current/{sha256(did)}.json`: the latest action per
//!   account, for the account page and every serving node's
//!   [`TakedownSet`].
//! - `policy/takedowns/audit/{at_ms:020}-{sha256(did)[..8]}.json`: one
//!   object per action, created with If-None-Match, so the log only grows.

use super::store::{get, now_ms, path, put};
use object_store::PutMode;
use parking_lot::{Mutex, RwLock};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use std::collections::{HashMap, HashSet};
use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};
use vlatproto::frame::{FrameKind, FrameMeta};
use vlsync_firehose::firehose::FrameFilter;
use vlsync_store::store::Store;

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

const CURRENT: &str = "policy/takedowns/current";
const POLL_GETS: usize = 16;

/// The DIDs under a relay takedown, kept from `policy/takedowns/current/`,
/// so subscribeRepos leaves their `#commit` and `#sync` frames out of the
/// replay window (the ring and cursor backfill) as well as the live stream.
/// `#account` and `#identity` pass, so consumers see the status change.
///
/// Every serving node (core, edge, replica) [polls](TakedownSet::poll) the
/// prefix. The core that takes an account down also
/// [applies it](TakedownSet::apply_local) before it emits the account's
/// `#account`, so its own consumers never see the two disagree.
#[derive(Default)]
pub struct TakedownSet {
    dids: RwLock<HashSet<Box<[u8]>, foldhash::fast::RandomState>>,
    /// `dids.len()`, read without the lock: an empty set filters nothing
    /// and parses no frames.
    len: AtomicUsize,
    /// Bumped after every change to `dids`; vlpds caches each ring batch's
    /// verdicts per value.
    generation: AtomicU64,
    /// Each account's latest action as this node knows it, and the local
    /// apply that set it (0: a poll did).
    latest: Mutex<HashMap<Box<str>, (bool, u64)>>,
    local_applies: AtomicU64,
    /// The version of each current object a poll has applied.
    seen: Mutex<HashMap<String, String>>,
    /// Each account's latest action in full, for the admin API's list.
    entries: Mutex<HashMap<Box<str>, TakedownEntry>>,
    polling: tokio::sync::Mutex<()>,
}

impl TakedownSet {
    pub fn contains(&self, did: &str) -> bool {
        self.dids.read().contains(did.as_bytes())
    }

    pub fn len(&self) -> usize {
        self.len.load(Ordering::Acquire)
    }

    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    /// Every account under a takedown as this node knows them, newest
    /// first.
    pub fn list(&self) -> Vec<TakedownEntry> {
        let mut v: Vec<TakedownEntry> = self.entries.lock().values().filter(|e| e.takedown).cloned().collect();
        v.sort_by(|a, b| b.at_ms.cmp(&a.at_ms).then_with(|| a.did.cmp(&b.did)));
        v
    }

    /// The full entry of an action this node recorded.
    pub fn note(&self, e: TakedownEntry) {
        let mut m = self.entries.lock();
        if m.get(e.did.as_str()).is_none_or(|p| p.at_ms <= e.at_ms) {
            m.insert(e.did.as_str().into(), e);
        }
    }

    /// An action this node just recorded ([`Takedowns::record`]). It wins
    /// over a poll that was already in flight, which may hold the object
    /// from before it.
    pub fn apply_local(&self, did: &str, takedown: bool) {
        let n = self.local_applies.fetch_add(1, Ordering::AcqRel) + 1;
        self.set(did, takedown, n, |_| true);
    }

    /// Lists the current objects and fetches the ones that changed since
    /// the last poll. An object it couldn't fetch is fetched again next
    /// time; the error is returned after everything else is applied.
    pub async fn poll(&self, store: &Store) -> anyhow::Result<()> {
        use futures::{StreamExt, TryStreamExt};
        use object_store::ObjectStore;
        let _one = self.polling.lock().await;
        let started = self.local_applies.load(Ordering::Acquire);
        let prefix = path(store, CURRENT);
        let metas: Vec<object_store::ObjectMeta> = store.raw.list(Some(&prefix)).try_collect().await?;
        let version = |m: &object_store::ObjectMeta| {
            m.e_tag.clone().unwrap_or_else(|| format!("{}-{}", m.last_modified.timestamp_micros(), m.size))
        };
        let changed: Vec<_> = {
            let seen = self.seen.lock();
            metas.into_iter().filter(|m| seen.get(m.location.as_ref()) != Some(&version(m))).collect()
        };
        let fetched: Vec<_> = futures::stream::iter(changed)
            .map(|m| async move { (get(store, &m.location, None).await, m) })
            .buffer_unordered(POLL_GETS)
            .collect()
            .await;
        let mut failed = None;
        for (got, m) in fetched {
            let applied = match got {
                Ok(Some((body, _))) => match serde_json::from_slice::<TakedownEntry>(&body) {
                    // a local apply since the list started is newer than anything it saw
                    Ok(e) => {
                        let applied = self.set(&e.did, e.takedown, 0, |by| by <= started);
                        if applied {
                            self.note(e);
                        }
                        applied
                    }
                    Err(e) => {
                        tracing::warn!(object = %m.location, "unreadable takedown object (skipped until it changes): {e}");
                        true
                    }
                },
                Ok(None) => false,
                Err(e) => {
                    failed = Some(e);
                    false
                }
            };
            if applied {
                self.seen.lock().insert(m.location.to_string(), version(&m));
            }
        }
        match failed {
            Some(e) => Err(anyhow::anyhow!("fetching takedown objects: {e}")),
            None => Ok(()),
        }
    }

    /// Records the action unless `allow` refuses the local apply that set
    /// the account's current one; false when refused.
    fn set(&self, did: &str, takedown: bool, by: u64, allow: impl FnOnce(u64) -> bool) -> bool {
        let mut latest = self.latest.lock();
        let was = match latest.get(did) {
            Some(&(t, prev_by)) => {
                if !allow(prev_by) {
                    return false;
                }
                t
            }
            None => false,
        };
        latest.insert(did.into(), (takedown, by));
        if was != takedown {
            let mut dids = self.dids.write();
            if takedown {
                dids.insert(did.as_bytes().into());
            } else {
                dids.remove(did.as_bytes());
            }
            self.len.store(dids.len(), Ordering::Release);
            drop(dids);
            self.generation.fetch_add(1, Ordering::AcqRel);
            tracing::info!(did, takedown, "takedown set changed");
        }
        true
    }
}

impl FrameFilter for TakedownSet {
    fn generation(&self) -> Option<u64> {
        (self.len.load(Ordering::Acquire) > 0).then(|| self.generation.load(Ordering::Acquire))
    }

    fn skip(&self, f: &FrameMeta<'_>) -> bool {
        matches!(f.kind, FrameKind::Commit | FrameKind::Sync) && f.did.is_some_and(|d| self.dids.read().contains(d))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn meta(kind: FrameKind, did: &str) -> FrameMeta<'_> {
        FrameMeta { kind, did: Some(did.as_bytes()) }
    }

    /// Audit objects are named by the millisecond: two actions on one DID
    /// in the same one collide.
    async fn record(t: &Takedowns, did: &str, takedown: bool) {
        tokio::time::sleep(std::time::Duration::from_millis(2)).await;
        t.record(did, takedown, "op", if takedown { "spam" } else { "" }).await.unwrap();
    }

    /// Another node's takedowns and reversals arrive by polling the current
    /// objects; only #commit and #sync of a listed DID are skipped, and an
    /// empty set asks vlpds to parse nothing.
    #[tokio::test]
    async fn polling_follows_the_current_objects() {
        let store = Store::memory(None);
        let (writer, set) = (Takedowns::new(store.clone()), TakedownSet::default());
        let (a, b) = ("did:plc:aaaaaaaaaaaaaaaaaaaaaaaa", "did:plc:bbbbbbbbbbbbbbbbbbbbbbbb");
        set.poll(&store).await.unwrap();
        assert_eq!(set.generation(), None);
        record(&writer, a, true).await;
        record(&writer, b, false).await;
        set.poll(&store).await.unwrap();
        assert!(set.contains(a) && !set.contains(b));
        let g = set.generation().expect("non-empty");
        assert!(set.skip(&meta(FrameKind::Commit, a)) && set.skip(&meta(FrameKind::Sync, a)));
        for k in [FrameKind::Account, FrameKind::Identity, FrameKind::Other] {
            assert!(!set.skip(&meta(k, a)), "{k:?} passes");
        }
        assert!(!set.skip(&meta(FrameKind::Commit, b)));
        assert!(!set.skip(&FrameMeta { kind: FrameKind::Commit, did: None }));
        // nothing changed: same generation, so batches keep their verdicts
        set.poll(&store).await.unwrap();
        assert_eq!(set.generation(), Some(g));
        record(&writer, b, true).await;
        set.poll(&store).await.unwrap();
        assert!(set.contains(b));
        assert!(set.generation().unwrap() > g);
        record(&writer, a, false).await;
        record(&writer, b, false).await;
        set.poll(&store).await.unwrap();
        assert!(set.is_empty());
        assert_eq!(set.generation(), None, "lifted: nothing filtered");
    }

    /// A local apply wins over a poll that listed the objects before it:
    /// the poll's older copy doesn't undo it, and the next poll fetches the
    /// object again.
    #[tokio::test]
    async fn a_local_apply_beats_an_in_flight_poll() {
        let store = Store::memory(None);
        let (writer, set) = (Takedowns::new(store.clone()), TakedownSet::default());
        let d = "did:plc:aaaaaaaaaaaaaaaaaaaaaaaa";
        record(&writer, d, false).await;
        set.poll(&store).await.unwrap();
        let started = set.local_applies.load(Ordering::Acquire);
        record(&writer, d, true).await;
        set.apply_local(d, true);
        assert!(set.contains(d));
        // the in-flight poll's copy from before the takedown
        assert!(!set.set(d, false, 0, |by| by <= started), "refused");
        assert!(set.contains(d));
        set.poll(&store).await.unwrap();
        assert!(set.contains(d));
        // and a reversal applied locally isn't undone either
        let started = set.local_applies.load(Ordering::Acquire);
        record(&writer, d, false).await;
        set.apply_local(d, false);
        assert!(!set.set(d, true, 0, |by| by <= started));
        assert!(!set.contains(d));
    }
}
