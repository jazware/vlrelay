//! Moderation cases in the bucket.
//!
//! - `cases/c/{id:016}.json`: one object per case, updated by CAS.
//! - `cases/open/{key hash}.json`: `{id}` of the open case for a dedupe key
//!   (rule + host + DID). Created with If-None-Match, so two nodes tripping
//!   the same key at once end up on one case. It's deleted when the case is
//!   resolved or dismissed, and the next trip opens a fresh case.
//! - `cases/next-id.json`: the id counter, by CAS.
//!
//! A trip on a key with an open case appends evidence to that case (the
//! newest [`EVIDENCE_KEPT`]) instead of opening another.

use super::store::{bounded, get, if_match, is_conflict, now_ms, path, put};
use crate::admin::{CaseNote, CaseStatus, Severity};
use object_store::{ObjectStore, ObjectStoreExt, PutMode, path::Path};
use parking_lot::Mutex;
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use std::collections::{BTreeMap, HashMap};
use vlpds::store::Store;

pub const EVIDENCE_KEPT: usize = 20;
const CAS_RETRIES: usize = 8;

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct Evidence {
    pub at_ms: i64,
    pub observed: f64,
    pub threshold: f64,
    pub window_secs: u32,
    pub node: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub detail: Option<String>,
    /// Every signal's count for the same host (and DID) at the time.
    #[serde(default)]
    pub signals: BTreeMap<String, f64>,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct StoredCase {
    pub id: u64,
    pub key: String,
    pub host: String,
    pub did: Option<String>,
    pub kind: String,
    pub severity: Severity,
    pub status: CaseStatus,
    pub opened_at_ms: i64,
    pub updated_at_ms: i64,
    pub summary: String,
    pub observed: f64,
    pub threshold: f64,
    pub auto_action: Option<String>,
    #[serde(default)]
    pub notes: Vec<CaseNote>,
    /// Trips folded into this case, including the first.
    #[serde(default)]
    pub trips: u32,
    #[serde(default)]
    pub evidence: Vec<Evidence>,
}

impl StoredCase {
    pub fn is_open(&self) -> bool {
        matches!(self.status, CaseStatus::Open | CaseStatus::Acknowledged)
    }

    pub fn to_wire(&self) -> crate::admin::Case {
        crate::admin::Case {
            id: self.id,
            host: self.host.clone(),
            did: self.did.clone(),
            kind: self.kind.clone(),
            severity: self.severity,
            status: self.status,
            opened_at_ms: self.opened_at_ms,
            updated_at_ms: self.updated_at_ms,
            summary: self.summary.clone(),
            observed: self.observed,
            threshold: self.threshold,
            auto_action: self.auto_action.clone(),
            notes: self.notes.clone(),
        }
    }
}

/// What a trip wants on record.
#[derive(Clone, Debug)]
pub struct CaseOpen {
    pub kind: String,
    pub host: String,
    pub did: Option<String>,
    pub severity: Severity,
    pub summary: String,
    pub observed: f64,
    pub threshold: f64,
    pub auto_action: Option<String>,
    pub evidence: Evidence,
}

impl CaseOpen {
    pub fn key(&self) -> String {
        format!("{}|{}|{}", self.kind, self.host, self.did.as_deref().unwrap_or(""))
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Opened {
    Created(u64),
    Updated(u64),
}

impl Opened {
    pub fn id(self) -> u64 {
        match self {
            Opened::Created(id) | Opened::Updated(id) => id,
        }
    }
}

#[derive(Serialize, Deserialize)]
struct OpenIndex {
    id: u64,
}

#[derive(Serialize, Deserialize, Default)]
struct NextId {
    next: u64,
}

pub struct CaseStore {
    store: Store,
    /// Cases by id with the ETag they were read at, so a list re-reads only
    /// what changed.
    cache: Mutex<HashMap<u64, (Option<String>, StoredCase)>>,
}

impl CaseStore {
    pub fn new(store: Store) -> CaseStore {
        CaseStore { store, cache: Default::default() }
    }

    fn case_path(&self, id: u64) -> Path {
        path(&self.store, &format!("cases/c/{id:016}.json"))
    }

    fn index_path(&self, key: &str) -> Path {
        let h = Sha256::digest(key.as_bytes());
        let hex: String = h[..16].iter().map(|b| format!("{b:02x}")).collect();
        path(&self.store, &format!("cases/open/{hex}.json"))
    }

    async fn read(&self, id: u64) -> anyhow::Result<Option<(StoredCase, Option<String>)>> {
        Ok(match get(&self.store, &self.case_path(id), None).await? {
            Some((b, e)) => {
                let c: StoredCase = serde_json::from_slice(&b)?;
                self.cache.lock().insert(id, (e.clone(), c.clone()));
                Some((c, e))
            }
            None => None,
        })
    }

    async fn write(&self, c: &StoredCase, mode: PutMode) -> object_store::Result<Option<String>> {
        let body = serde_json::to_vec_pretty(c).expect("serializable");
        let e = put(&self.store, &self.case_path(c.id), body, mode).await?;
        self.cache.lock().insert(c.id, (e.clone(), c.clone()));
        Ok(e)
    }

    async fn next_id(&self) -> anyhow::Result<u64> {
        let p = path(&self.store, "cases/next-id.json");
        for _ in 0..CAS_RETRIES {
            let (cur, etag) = match get(&self.store, &p, None).await? {
                Some((b, e)) => (serde_json::from_slice::<NextId>(&b).unwrap_or_default(), e),
                None => (NextId::default(), None),
            };
            let id = cur.next.max(1);
            let body = serde_json::to_vec(&NextId { next: id + 1 })?;
            match put(&self.store, &p, body, if_match(etag)).await {
                Ok(_) => return Ok(id),
                Err(e) if is_conflict(&e) || matches!(e, object_store::Error::NotFound { .. }) => {}
                Err(e) => return Err(e.into()),
            }
        }
        anyhow::bail!("case id counter still contended after {CAS_RETRIES} tries")
    }

    /// Opens a case for `o`, or adds its evidence to the open one with the
    /// same key.
    pub async fn open_or_update(&self, o: CaseOpen) -> anyhow::Result<Opened> {
        let key = o.key();
        let ip = self.index_path(&key);
        for _ in 0..CAS_RETRIES {
            let idx = get(&self.store, &ip, None).await?;
            let idx_etag = idx.as_ref().and_then(|(_, e)| e.clone());
            if let Some((b, _)) = &idx
                && let Ok(OpenIndex { id }) = serde_json::from_slice(b)
                && let Some((mut c, etag)) = self.read(id).await?
                && c.is_open()
            {
                c.updated_at_ms = o.evidence.at_ms;
                c.observed = o.observed;
                c.threshold = o.threshold;
                c.summary = o.summary.clone();
                c.severity = c.severity.max(o.severity);
                if o.auto_action.is_some() {
                    c.auto_action = o.auto_action.clone();
                }
                c.trips = c.trips.saturating_add(1);
                c.evidence.push(o.evidence.clone());
                let drop = c.evidence.len().saturating_sub(EVIDENCE_KEPT);
                c.evidence.drain(..drop);
                match self.write(&c, if_match(etag)).await {
                    Ok(_) => return Ok(Opened::Updated(id)),
                    Err(e) if is_conflict(&e) => continue,
                    Err(e) => return Err(e.into()),
                }
            }
            // No open case for the key (none, closed, or the index points
            // at a case a crash never wrote).
            let id = self.next_id().await?;
            let body = serde_json::to_vec(&OpenIndex { id })?;
            match put(&self.store, &ip, body, if_match(idx_etag)).await {
                Ok(_) => {}
                Err(e) if is_conflict(&e) || matches!(e, object_store::Error::NotFound { .. }) => {
                    continue;
                }
                Err(e) => return Err(e.into()),
            }
            let c = StoredCase {
                id,
                key: key.clone(),
                host: o.host.clone(),
                did: o.did.clone(),
                kind: o.kind.clone(),
                severity: o.severity,
                status: CaseStatus::Open,
                opened_at_ms: o.evidence.at_ms,
                updated_at_ms: o.evidence.at_ms,
                summary: o.summary.clone(),
                observed: o.observed,
                threshold: o.threshold,
                auto_action: o.auto_action.clone(),
                notes: Vec::new(),
                trips: 1,
                evidence: vec![o.evidence.clone()],
            };
            self.write(&c, PutMode::Create).await?;
            return Ok(Opened::Created(id));
        }
        anyhow::bail!("case for {key} still contended after {CAS_RETRIES} tries")
    }

    pub async fn get_case(&self, id: u64) -> anyhow::Result<Option<StoredCase>> {
        Ok(self.read(id).await?.map(|(c, _)| c))
    }

    /// Every case, re-reading only the ones whose ETag changed.
    pub async fn list(&self, status: Option<CaseStatus>) -> anyhow::Result<Vec<StoredCase>> {
        use futures::TryStreamExt;
        let prefix = path(&self.store, "cases/c");
        let metas: Vec<_> = bounded(self.store.raw.list(Some(&prefix)).try_collect()).await?;
        let mut out = Vec::with_capacity(metas.len());
        for m in metas {
            let id = m.location.filename().and_then(|f| f.strip_suffix(".json")).and_then(|f| f.parse::<u64>().ok());
            let Some(id) = id else { continue };
            let cached =
                self.cache.lock().get(&id).filter(|(e, _)| e.is_some() && *e == m.e_tag).map(|(_, c)| c.clone());
            let c = match cached {
                Some(c) => c,
                None => match self.read(id).await? {
                    Some((c, _)) => c,
                    None => continue,
                },
            };
            if status.is_none_or(|s| c.status == s) {
                out.push(c);
            }
        }
        out.sort_by(|a, b| b.severity.cmp(&a.severity).then(b.opened_at_ms.cmp(&a.opened_at_ms)));
        Ok(out)
    }

    /// An operator's status change and note. Closing a case frees its key.
    pub async fn update(
        &self,
        id: u64,
        status: Option<CaseStatus>,
        note: &str,
        by: &str,
    ) -> anyhow::Result<Option<StoredCase>> {
        for _ in 0..CAS_RETRIES {
            let Some((mut c, etag)) = self.read(id).await? else {
                return Ok(None);
            };
            let now = now_ms();
            if let Some(s) = status {
                c.status = s;
            }
            if !note.trim().is_empty() {
                c.notes.push(CaseNote { at_ms: now, by: by.to_string(), text: note.trim().to_string() });
            }
            c.updated_at_ms = now;
            match self.write(&c, if_match(etag)).await {
                Ok(_) => {}
                Err(e) if is_conflict(&e) => continue,
                Err(e) => return Err(e.into()),
            }
            if !c.is_open() {
                self.release_key(&c).await?;
            }
            return Ok(Some(c));
        }
        anyhow::bail!("case {id} still contended after {CAS_RETRIES} tries")
    }

    /// Resolves case `id` with `note` if it's still open (open or
    /// acknowledged); None if it's gone or someone closed it first.
    pub async fn resolve_open(&self, id: u64, note: &str, by: &str) -> anyhow::Result<Option<StoredCase>> {
        for _ in 0..CAS_RETRIES {
            let Some((mut c, etag)) = self.read(id).await? else { return Ok(None) };
            if !c.is_open() {
                return Ok(None);
            }
            let now = now_ms();
            c.status = CaseStatus::Resolved;
            c.notes.push(CaseNote { at_ms: now, by: by.to_string(), text: note.to_string() });
            c.updated_at_ms = now;
            match self.write(&c, if_match(etag)).await {
                Ok(_) => {}
                Err(e) if is_conflict(&e) => continue,
                Err(e) => return Err(e.into()),
            }
            self.release_key(&c).await?;
            return Ok(Some(c));
        }
        anyhow::bail!("case {id} still contended after {CAS_RETRIES} tries")
    }

    async fn release_key(&self, c: &StoredCase) -> anyhow::Result<()> {
        let ip = self.index_path(&c.key);
        if let Some((b, _)) = get(&self.store, &ip, None).await?
            && serde_json::from_slice::<OpenIndex>(&b).is_ok_and(|i| i.id == c.id)
        {
            // A racing trip that already re-pointed the index keeps it.
            match bounded(self.store.raw.delete(&ip)).await {
                Ok(()) | Err(object_store::Error::NotFound { .. }) => {}
                Err(e) => return Err(e.into()),
            }
        }
        Ok(())
    }
}
