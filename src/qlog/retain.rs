//! What bucket retention may delete (docs/quorum.md, "Bucket retention"):
//! `qlog retain` writes the plan to `retain/qlog`, the object vlpds's
//! retention keeps per log (`retention::Report`; its `pruned_seq` is what
//! `OutdatedCursor` answers from). `qlog retain --apply` then deletes what
//! [`apply`] still finds deletable, raising `pruned_seq` first.
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
//!   lists it as an external database, directly or through another clone
//!   (the clone reads its SSTs until compaction rewrites them); after that
//!   it can go whole. A newer epoch's path is never deletable: it may be a
//!   recovery's clone in progress.

use super::flush::{self, LOG_ID};
use super::state;
use futures::StreamExt;
use object_store::ObjectStoreExt;
use object_store::path::Path;
use serde::Serialize;
use std::collections::{BTreeMap, BTreeSet};
use std::time::Duration;
use vlsync_firehose::log;
use vlsync_firehose::log::Head;
use vlsync_store::store::Store;

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

/// `retain/qlog`: vlpds's report for the log, with the plan alongside and
/// what the last `--apply` deleted.
#[derive(Debug, Default, Serialize)]
pub struct Report {
    pub opened: BTreeMap<String, u64>,
    pub pruned_seq: i64,
    pub plan: Plan,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub applied: Option<Applied>,
}

/// What [`apply`] deleted.
#[derive(Debug, Default, Serialize)]
pub struct Applied {
    pub segments: u64,
    pub segment_bytes: u64,
    pub pruned_seq: u64,
    pub state_paths: Vec<String>,
    pub state_objects: u64,
    pub state_bytes: u64,
    /// Marked deletable by the plan and kept on a second look, with why.
    pub kept: Vec<String>,
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
            if let Head::Segment(h) = log::read_head(store, LOG_ID, ord).await?
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
        let Head::Segment(h) = log::read_head(store, LOG_ID, ord).await? else {
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
    let external = referenced(store, &current).await?;
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
            deletable: !is_current
                && !referenced
                && matches!((path_epoch(&path), path_epoch(&current)), (Some(e), Some(c)) if e < c),
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

fn report_path(store: &Store) -> Path {
    Path::from(format!("{}/retain/{LOG_ID}", store.prefix))
}

/// `retain/qlog` as the last pass wrote it, None before the first.
pub async fn read_report(store: &Store) -> anyhow::Result<Option<serde_json::Value>> {
    match store.raw.get(&report_path(store)).await {
        Ok(r) => Ok(Some(serde_json::from_slice(&r.bytes().await?)?)),
        Err(object_store::Error::NotFound { .. }) => Ok(None),
        Err(e) => Err(e.into()),
    }
}

/// The retained floor `retain/qlog` publishes: every seq at or below it may
/// be gone from the bucket.
pub async fn pruned_seq(store: &Store) -> anyhow::Result<u64> {
    match store.raw.get(&report_path(store)).await {
        Ok(r) => {
            Ok(serde_json::from_slice::<serde_json::Value>(&r.bytes().await?)?["pruned_seq"].as_u64().unwrap_or(0))
        }
        Err(object_store::Error::NotFound { .. }) => Ok(0),
        Err(e) => Err(e.into()),
    }
}

/// Writes the plan (and what `apply` did with it) as `retain/qlog`,
/// keeping the `pruned_seq` already there.
pub async fn write(store: &Store, plan: Plan, applied: Option<Applied>) -> anyhow::Result<Report> {
    let pruned_seq = pruned_seq(store).await? as i64;
    let r = Report { opened: BTreeMap::new(), pruned_seq, plan, applied };
    store.raw.put(&report_path(store), serde_json::to_vec_pretty(&r)?.into()).await?;
    Ok(r)
}

/// 0 for the original `qlog/state`, N for a recovery's `qlog/state-eN`.
fn path_epoch(path: &str) -> Option<u64> {
    match path.strip_prefix(state::DEFAULT_PATH)? {
        "" => Some(0),
        e => e.strip_prefix("-e")?.parse().ok(),
    }
}

/// Every state path the current one reads SSTs from, following each
/// external database's own `external_dbs` too (a clone of a clone).
async fn referenced(store: &Store, current: &str) -> anyhow::Result<BTreeSet<String>> {
    let mut seen = BTreeSet::new();
    let mut todo = vec![state::db_path(store, current)];
    while let Some(p) = todo.pop() {
        let admin = slatedb::admin::Admin::builder(p.clone(), store.raw.clone()).build();
        if let Some(vm) = admin.read_manifest(None).await? {
            for e in vm.external_dbs() {
                if seen.insert(e.path.clone()) {
                    todo.push(e.path.clone());
                }
            }
        }
    }
    Ok(seen)
}

/// Deletes what `plan` marked deletable, after checking each again against
/// the manifest as it is now:
///
/// - Segments: `pruned_seq` is raised to the last one's end and published
///   first (as vlpds's retention does, so a reader that checked the floor
///   never misses data silently), then they go oldest first, so a pass cut
///   short leaves a dense suffix. Each is at or below F and below the
///   manifest's next ordinal, so neither a restart nor a recovery reads it:
///   the state checkpoint covers it.
/// - State paths: one goes whole only if it isn't the manifest's, nothing
///   the current state reads lists it (`external_dbs`, transitively), and
///   it's an older epoch's than the current one. A newer path may be a
///   recovery's clone in progress, and no older one can become current
///   again: a recovery's manifest CAS is against the manifest it read,
///   which the current one replaced.
///
/// Not deleted here: stale segments past the manifest (the flush or a
/// recovery deletes them when it reaches their ordinal, and a delete here
/// could race it and take its fresh segment at the same key) and stale
/// `qlog-*` checkpoints (a flush between its seal and its CAS holds one
/// the manifest doesn't name yet; the leader clears them at its fence).
pub async fn apply(store: &Store, plan: &Plan) -> anyhow::Result<Applied> {
    let mut a = Applied::default();
    let Some((m, _)) = flush::read_manifest(store).await? else { anyhow::bail!("qlog retain: no manifest") };
    anyhow::ensure!(
        m.flushed >= plan.flushed && m.next_ordinal >= plan.next_ordinal,
        "qlog retain: the manifest went back (F {} next ordinal {}, the plan's {} {})",
        m.flushed,
        m.next_ordinal,
        plan.flushed,
        plan.next_ordinal
    );
    let segs: Vec<&SegmentPlan> =
        plan.deletable.iter().filter(|s| s.ordinal < m.next_ordinal && s.last <= m.flushed).collect();
    if let Some(top) = segs.last() {
        let floor = pruned_seq(store).await?.max(top.last);
        let body = serde_json::json!({ "opened": {}, "pruned_seq": floor, "plan": plan });
        store.raw.put(&report_path(store), serde_json::to_vec_pretty(&body)?.into()).await?;
        a.pruned_seq = floor;
        for s in segs {
            match store.raw.delete(&log::segment_path(store, LOG_ID, s.ordinal)).await {
                Ok(()) | Err(object_store::Error::NotFound { .. }) => {}
                Err(e) => return Err(e.into()),
            }
            a.segments += 1;
            a.segment_bytes += s.bytes;
        }
    }
    let current = m.state_path().to_string();
    let cur_epoch = path_epoch(&current);
    let refs = referenced(store, &current).await?;
    for sp in plan.states.iter().filter(|s| s.deletable) {
        let why = if sp.path == current {
            Some("current")
        } else if refs.contains(&state::db_path(store, &sp.path)) {
            Some("referenced")
        } else if !matches!((path_epoch(&sp.path), cur_epoch), (Some(e), Some(c)) if e < c) {
            Some("not an older epoch's")
        } else {
            None
        };
        if let Some(why) = why {
            a.kept.push(format!("{}: {why}", sp.path));
            continue;
        }
        let objs = list(store, &sp.path).await?;
        let (n, bytes) = (objs.len() as u64, objs.iter().map(|o| o.size).sum::<u64>());
        let locs = futures::stream::iter(objs.into_iter().map(|o| Ok(o.location))).boxed();
        let mut del = store.raw.delete_stream(locs);
        while let Some(r) = del.next().await {
            match r {
                Ok(_) | Err(object_store::Error::NotFound { .. }) => {}
                Err(e) => return Err(e.into()),
            }
        }
        a.state_paths.push(sp.path.clone());
        (a.state_objects, a.state_bytes) = (a.state_objects + n, a.state_bytes + bytes);
    }
    Ok(a)
}
