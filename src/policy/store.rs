//! One versioned JSON object in the bucket with compare-and-swap saves and
//! an append-only audit log beside it. The policy and the domain rules are
//! both stored this way (vlpds's rate-limit config, `ratelimit/runtime.rs`,
//! is the model).
//!
//! A save checks the caller's base version against the stored one, then
//! PUTs version + 1 with If-Match on the ETag it read, so two operators on
//! two nodes can't overwrite each other. The audit log is one object per
//! version (`{audit}/{version:020}.json`, created with If-None-Match), so
//! it can only grow. The stored object repeats its own audit entry, and the
//! next save writes it if the first attempt was lost to a crash.

use object_store::{GetOptions, ObjectStore, PutMode, PutOptions, PutPayload, UpdateVersion, path::Path};
use serde::{Deserialize, Serialize, de::DeserializeOwned};
use std::time::Duration;
use vlsync_store::store::Store;

const CALL_DEADLINE: Duration = Duration::from_secs(5);

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct Stored<T> {
    pub version: u64,
    pub updated_at_ms: i64,
    pub updated_by: String,
    #[serde(default)]
    pub note: String,
    /// This version's audit entry (one line per changed leaf).
    #[serde(default)]
    pub changes: Vec<String>,
    pub body: T,
}

impl<T: Default> Stored<T> {
    /// What a node runs before anything is stored.
    pub fn initial() -> Stored<T> {
        Stored {
            version: 0,
            updated_at_ms: 0,
            updated_by: String::new(),
            note: String::new(),
            changes: Vec::new(),
            body: T::default(),
        }
    }
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct AuditEntry {
    pub version: u64,
    pub at_ms: i64,
    pub by: String,
    pub note: String,
    pub changes: Vec<String>,
}

#[derive(Debug, thiserror::Error)]
pub enum SaveError {
    #[error("invalid: {}", .0.join("; "))]
    Invalid(Vec<String>),
    #[error("changed since version {expected} (now version {current}): reload and reapply your edit")]
    Conflict { expected: u64, current: u64 },
    #[error("nothing changed")]
    NoChange,
    #[error("store: {0}")]
    Store(String),
    /// The bucket didn't answer in time or the request failed on the way:
    /// the same save may well work in a moment.
    #[error("the bucket is unavailable: {0}")]
    Unavailable(String),
}

impl SaveError {
    pub(crate) fn from_store(e: object_store::Error) -> SaveError {
        use object_store::Error as E;
        let config = matches!(
            e,
            E::PermissionDenied { .. }
                | E::Unauthenticated { .. }
                | E::NotSupported { .. }
                | E::NotImplemented { .. }
                | E::InvalidPath { .. }
        );
        if config { SaveError::Store(e.to_string()) } else { SaveError::Unavailable(e.to_string()) }
    }
}

pub enum Fetched<T> {
    NotModified,
    Absent,
    Got {
        doc: Stored<T>,
        etag: Option<String>,
    },
    /// The object is there but doesn't parse: callers keep what they have.
    Invalid {
        version: Option<u64>,
        message: String,
        etag: Option<String>,
    },
}

pub struct Versioned {
    store: Store,
    pub path: Path,
    audit: String,
}

pub(crate) async fn bounded<T>(f: impl Future<Output = object_store::Result<T>>) -> object_store::Result<T> {
    match tokio::time::timeout(CALL_DEADLINE, f).await {
        Ok(r) => r,
        Err(_) => Err(object_store::Error::Generic { store: "policy", source: "object store call timed out".into() }),
    }
}

pub(crate) fn is_conflict(e: &object_store::Error) -> bool {
    matches!(e, object_store::Error::Precondition { .. } | object_store::Error::AlreadyExists { .. })
}

pub(crate) fn if_match(etag: Option<String>) -> PutMode {
    match etag {
        Some(e) => PutMode::Update(UpdateVersion { e_tag: Some(e), version: None }),
        None => PutMode::Create,
    }
}

pub(crate) fn now_ms() -> i64 {
    std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH).map(|d| d.as_millis() as i64).unwrap_or(0)
}

/// `{prefix}/{rel}`.
pub(crate) fn path(store: &Store, rel: &str) -> Path {
    if store.prefix.is_empty() { Path::from(rel) } else { Path::from(format!("{}/{rel}", store.prefix)) }
}

/// GET with an optional If-None-Match. None when absent.
pub(crate) async fn get(
    store: &Store,
    p: &Path,
    etag: Option<String>,
) -> object_store::Result<Option<(bytes::Bytes, Option<String>)>> {
    let got = bounded(async {
        let r = store.raw.get_opts(p, GetOptions { if_none_match: etag, ..Default::default() }).await?;
        let e = r.meta.e_tag.clone();
        Ok((r.bytes().await?, e))
    })
    .await;
    match got {
        Ok(x) => Ok(Some(x)),
        Err(object_store::Error::NotFound { .. }) => Ok(None),
        Err(e) => Err(e),
    }
}

pub(crate) async fn put(store: &Store, p: &Path, body: Vec<u8>, mode: PutMode) -> object_store::Result<Option<String>> {
    let r = bounded(store.raw.put_opts(p, PutPayload::from(body), PutOptions { mode, ..Default::default() })).await?;
    Ok(r.e_tag)
}

impl Versioned {
    /// `rel` and `audit_rel` are relative to the store's prefix.
    pub fn new(store: Store, rel: &str, audit_rel: &str) -> Versioned {
        let path = path(&store, rel);
        let audit = match store.prefix.as_str() {
            "" => audit_rel.to_string(),
            p => format!("{p}/{audit_rel}"),
        };
        Versioned { store, path, audit }
    }

    fn audit_path(&self, version: u64) -> Path {
        Path::from(format!("{}/{version:020}.json", self.audit))
    }

    pub async fn fetch<T: DeserializeOwned>(&self, etag: Option<String>) -> object_store::Result<Fetched<T>> {
        let got = match get(&self.store, &self.path, etag).await {
            Ok(g) => g,
            Err(object_store::Error::NotModified { .. }) => return Ok(Fetched::NotModified),
            Err(e) => return Err(e),
        };
        let Some((bytes, etag)) = got else {
            return Ok(Fetched::Absent);
        };
        Ok(match serde_json::from_slice::<Stored<T>>(&bytes) {
            Ok(doc) => Fetched::Got { doc, etag },
            Err(e) => Fetched::Invalid {
                version: serde_json::from_slice::<serde_json::Value>(&bytes).ok().and_then(|v| v["version"].as_u64()),
                message: e.to_string(),
                etag,
            },
        })
    }

    /// Stores `body` as version `base_version + 1` if `base_version` is
    /// still current. `validate` runs before anything is written.
    pub async fn save<T: Serialize + DeserializeOwned + Clone + Default>(
        &self,
        base_version: u64,
        body: T,
        by: &str,
        note: &str,
        validate: impl FnOnce(&T) -> Result<(), Vec<String>>,
    ) -> Result<Stored<T>, SaveError> {
        validate(&body).map_err(SaveError::Invalid)?;
        if note.chars().count() > 280 {
            return Err(SaveError::Invalid(vec!["note: longer than 280 characters".into()]));
        }
        let st = SaveError::from_store;
        let cur = get(&self.store, &self.path, None).await.map_err(st)?;
        // An unreadable object can still be replaced: its version (if any)
        // is what the caller must have seen.
        let (cur_raw, cur_version, etag) = match &cur {
            None => (None, 0, None),
            Some((b, e)) => {
                let v: Option<serde_json::Value> = serde_json::from_slice(b).ok();
                let ver = v.as_ref().and_then(|v| v["version"].as_u64()).unwrap_or(0);
                (v, ver, e.clone())
            }
        };
        if cur_version != base_version {
            return Err(SaveError::Conflict { expected: base_version, current: cur_version });
        }
        if let Some(prev) =
            cur_raw.as_ref().and_then(|v| serde_json::from_value::<Stored<serde_json::Value>>(v.clone()).ok())
        {
            self.write_audit(&prev).await;
        }
        // The first save is diffed against the defaults every node ran on.
        let old_body = match &cur_raw {
            Some(v) => v["body"].clone(),
            None => serde_json::to_value(T::default()).map_err(|e| SaveError::Store(e.to_string()))?,
        };
        let new_body = serde_json::to_value(&body).map_err(|e| SaveError::Store(e.to_string()))?;
        let changes = crate::admin::diff_json(&old_body, &new_body);
        if changes.is_empty() {
            return Err(SaveError::NoChange);
        }
        let doc = Stored {
            version: cur_version + 1,
            updated_at_ms: now_ms(),
            updated_by: by.to_string(),
            note: note.trim().to_string(),
            changes,
            body,
        };
        let bytes = serde_json::to_vec_pretty(&doc).map_err(|e| SaveError::Store(e.to_string()))?;
        match put(&self.store, &self.path, bytes, if_match(etag)).await {
            Ok(_) => {}
            Err(e) if is_conflict(&e) || matches!(e, object_store::Error::NotFound { .. }) => {
                return Err(SaveError::Conflict { expected: base_version, current: base_version + 1 });
            }
            Err(e) => return Err(st(e)),
        }
        self.write_audit(&doc).await;
        tracing::info!(
            target: "vlrelay::audit",
            object = %self.path,
            version = doc.version,
            by,
            changes = %doc.changes.join("; "),
            "policy object updated"
        );
        Ok(doc)
    }

    async fn write_audit<B>(&self, d: &Stored<B>) {
        if d.version == 0 {
            return;
        }
        let e = AuditEntry {
            version: d.version,
            at_ms: d.updated_at_ms,
            by: d.updated_by.clone(),
            note: d.note.clone(),
            changes: d.changes.clone(),
        };
        let body = serde_json::to_vec(&e).expect("serializable");
        match put(&self.store, &self.audit_path(d.version), body, PutMode::Create).await {
            Ok(_) => {}
            Err(e) if is_conflict(&e) => {}
            Err(e) => {
                tracing::warn!(version = d.version, "audit entry not written (the next save retries): {e}")
            }
        }
    }

    /// Newest first.
    pub async fn audit(&self, limit: usize) -> anyhow::Result<Vec<AuditEntry>> {
        use futures::TryStreamExt;
        let prefix = Path::from(self.audit.clone());
        let mut metas: Vec<_> = self.store.raw.list(Some(&prefix)).try_collect().await?;
        metas.sort_by(|a, b| b.location.cmp(&a.location));
        metas.truncate(limit);
        let mut out = Vec::with_capacity(metas.len());
        for m in metas {
            if let Some((b, _)) = get(&self.store, &m.location, None).await? {
                match serde_json::from_slice(&b) {
                    Ok(e) => out.push(e),
                    Err(e) => tracing::warn!(path = %m.location, "unreadable audit entry: {e}"),
                }
            }
        }
        Ok(out)
    }
}
