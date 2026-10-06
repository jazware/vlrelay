//! What bucket retention could delete (docs/quorum.md, "Bucket retention"),
//! reported before anything is: `qlog retain` writes it to `retain/qlog`,
//! the object vlpds's retention keeps per log (`retention::Report`; its
//! `pruned_seq` is what `OutdatedCursor` answers from, and stays as it is
//! until something deletes).
//!
//! - Log segments: vlpds deletes a log's segments oldest first, so what
//!   could go is the longest prefix of ordinals whose objects are all older
//!   than the horizon. Every one of them is at or below F (named by a
//!   manifest), so no restart needs it: the state checkpoint covers it.
//! - Segments past the manifest that a deposed leader wrote below F
//!   (stale): nobody's; the next flush or recovery deletes them.
//! - State checkpoints: only the manifest's is needed; any other `qlog-*`
//!   one is a flush that died between its seal and its CAS.
//! - State paths: a bucket recovery clones the state to `qlog/state-e{epoch}`.
//!   An older path is still needed while the current one's SlateDB manifest
//!   lists it as an external database (the clone reads its SSTs until
//!   compaction rewrites them); after that it can go whole.

use super::flush::{self, LOG_ID};
use super::state;
use futures::StreamExt;
use object_store::ObjectStoreExt;
use object_store::path::Path;
use serde::Serialize;
use std::collections::{BTreeMap, BTreeSet};
use std::time::Duration;
use vlpds::nodelog::{self, Head};
use vlpds::store::Store;

#[derive(Debug, Default, Serialize)]
pub struct SegmentPlan {
    pub ordinal: u64,
    pub first: u64,
    pub last: u64,
    pub bytes: u64,
    pub age_secs: u64,
}

#[derive(Debug, Default, Serialize)]
pub struct StatePath {
    pub path: String,
    /// The manifest's state.
    pub current: bool,
    /// Listed as an external database by the current state (its SSTs are
    /// still read through the clone).
    pub referenced: bool,
    pub objects: u64,
    pub bytes: u64,
    /// `qlog-*` checkpoints the manifest doesn't name.
    pub stale_checkpoints: Vec<String>,
    /// Checkpoints SlateDB took for a clone of this path.
    pub clone_checkpoints: u64,
    pub deletable: bool,
}

#[derive(Debug, Default, Serialize)]
pub struct Plan {
    pub at_ms: i64,
    pub horizon_secs: u64,
    pub flushed: u64,
    pub reserve: u64,
    pub next_ordinal: u64,
    pub gaps: Vec<(u64, u64)>,
    pub segments: u64,
    pub segment_bytes: u64,
    /// The oldest segments, all past the horizon: what a pass could delete.
    pub deletable: Vec<SegmentPlan>,
    pub deletable_bytes: u64,
    /// The retained floor a delete of them would raise `pruned_seq` to.
    pub pruned_seq_after: u64,
    pub stale_segments: Vec<u64>,
    pub states: Vec<StatePath>,
}

/// `retain/qlog`: vlpds's report for the log (nothing deleted yet, so its
/// `pruned_seq` stays), with the plan alongside.
#[derive(Debug, Default, Serialize)]
pub struct Report {
    pub opened: BTreeMap<String, u64>,
    pub pruned_seq: i64,
    pub plan: Plan,
}

async fn list(store: &Store, prefix: &str) -> anyhow::Result<Vec<object_store::ObjectMeta>> {
    let p = Path::from(format!("{}/{prefix}", store.prefix));
    let mut out = Vec::new();
    let mut s = store.raw.list(Some(&p));
    while let Some(m) = s.next().await {
        out.push(m?);
    }
    Ok(out)
}

pub async fn plan(store: &Store, horizon: Duration) -> anyhow::Result<Option<Plan>> {
    let Some((m, _)) = flush::read_manifest(store).await? else { return Ok(None) };
    let now = chrono::Utc::now();
    let mut p = Plan {
        at_ms: now.timestamp_millis(),
        horizon_secs: horizon.as_secs(),
        flushed: m.flushed,
        reserve: m.reserve,
        next_ordinal: m.next_ordinal,
        gaps: m.gaps.clone(),
        ..Default::default()
    };
    let segs = list(store, &format!("log/{LOG_ID}")).await?;
    let mut by_ord: BTreeMap<u64, &object_store::ObjectMeta> = BTreeMap::new();
    for o in &segs {
        if let Some(ord) = o.location.filename().and_then(|f| f.strip_suffix(".seg")).and_then(|f| f.parse().ok()) {
            by_ord.insert(ord, o);
        }
    }
    p.segments = by_ord.len() as u64;
    p.segment_bytes = by_ord.values().map(|o| o.size).sum();
    let mut prefix_open = true;
    for (&ord, o) in &by_ord {
        let age = (now - o.last_modified).num_seconds().max(0) as u64;
        if ord >= m.next_ordinal {
            if let Head::Segment(h) = nodelog::read_head(store, LOG_ID, ord).await?
                && (h.first_seq as u64) <= m.flushed
            {
                p.stale_segments.push(ord);
            }
            continue;
        }
        if !prefix_open || age < horizon.as_secs() {
            prefix_open = false;
            continue;
        }
        let Head::Segment(h) = nodelog::read_head(store, LOG_ID, ord).await? else {
            prefix_open = false;
            continue;
        };
        p.deletable_bytes += o.size;
        p.pruned_seq_after = h.last_seq as u64;
        p.deletable.push(SegmentPlan {
            ordinal: ord,
            first: h.first_seq as u64,
            last: h.last_seq as u64,
            bytes: o.size,
            age_secs: age,
        });
    }
    let current = m.state_path().to_string();
    let named = m.state.as_ref().map(|s| s.checkpoint.clone());
    let mut external = BTreeSet::new();
    let admin = slatedb::admin::Admin::builder(state::db_path(store, &current), store.raw.clone()).build();
    if let Some(vm) = admin.read_manifest(None).await? {
        for e in vm.external_dbs() {
            external.insert(e.path.clone());
        }
    }
    let objs = list(store, "qlog").await?;
    let mut paths: BTreeMap<String, (u64, u64)> = BTreeMap::new();
    let base = Path::from(format!("{}/qlog", store.prefix));
    for o in &objs {
        let mut parts = o.location.prefix_match(&base).into_iter().flatten();
        if let Some(first) = parts.next()
            && first.as_ref().starts_with("state")
            && parts.next().is_some()
        {
            let e = paths.entry(format!("qlog/{}", first.as_ref())).or_default();
            e.0 += 1;
            e.1 += o.size;
        }
    }
    for (path, (objects, bytes)) in paths {
        let is_current = path == current;
        let referenced = external.contains(&state::db_path(store, &path));
        let admin = slatedb::admin::Admin::builder(state::db_path(store, &path), store.raw.clone()).build();
        let cps = admin.list_checkpoints(None).await.unwrap_or_default();
        let stale: Vec<String> = cps
            .iter()
            .filter(|c| c.name.as_deref().is_some_and(|n| n.starts_with("qlog-")))
            .map(|c| c.id.to_string())
            .filter(|id| !is_current || Some(id) != named.as_ref())
            .collect();
        let clone_checkpoints = cps.iter().filter(|c| c.name.is_none()).count() as u64;
        p.states.push(StatePath {
            deletable: !is_current && !referenced,
            path,
            current: is_current,
            referenced,
            objects,
            bytes,
            stale_checkpoints: stale,
            clone_checkpoints,
        });
    }
    Ok(Some(p))
}

/// Writes the plan as `retain/qlog`, keeping any `pruned_seq` already
/// there (nothing here deletes, so the retained floor doesn't move).
pub async fn write(store: &Store, plan: Plan) -> anyhow::Result<Report> {
    let path = Path::from(format!("{}/retain/{LOG_ID}", store.prefix));
    let pruned_seq = match store.raw.get(&path).await {
        Ok(r) => serde_json::from_slice::<serde_json::Value>(&r.bytes().await?)?["pruned_seq"].as_i64().unwrap_or(0),
        Err(object_store::Error::NotFound { .. }) => 0,
        Err(e) => return Err(e.into()),
    };
    let r = Report { opened: BTreeMap::new(), pruned_seq, plan };
    store.raw.put(&path, serde_json::to_vec_pretty(&r)?.into()).await?;
    Ok(r)
}
