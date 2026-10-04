//! The background half of the mirror: deleting what shouldn't be kept
//! (archiving switched off for an account, a takedown past its retention,
//! a deleted account), sweeping replaced generations, and queueing accounts
//! archiving was switched on for.

use super::Archive;
use super::fetch::Why;
use super::mirror::{self, META_FAMILY, Meta};
use crate::state::{Chain, ShardState, StateStore, Upstream};
use std::collections::HashSet;
use std::sync::Arc;
use std::sync::atomic::Ordering::Relaxed;
use std::time::Duration;
use vlpds::segment::Mutation;
use vlpds::state::{self as vs};

pub const INTERVAL: Duration = Duration::from_secs(10);
const DELETES_PER_BATCH: usize = 4096;

pub fn spawn<C: Chain>(a: Arc<Archive>, state: Arc<StateStore<C>>) {
    tokio::spawn(async move {
        let mut seen_version = None;
        loop {
            // the first pass waits for the shards' recovery
            tokio::time::sleep(INTERVAL).await;
            let v = a.gate().version();
            let rescan = seen_version != Some(v);
            match sweep_once(&a, &state, rescan).await {
                Ok(r) => {
                    seen_version = Some(v);
                    if r.deleted + r.queued + r.swept > 0 {
                        tracing::info!(?r, "archive sweep");
                    }
                }
                Err(e) => tracing::warn!("archive sweep: {e:#}"),
            }
        }
    });
}

#[derive(Debug, Default, Clone, Copy, serde::Serialize)]
pub struct SweepReport {
    pub mirrors: usize,
    pub deleted: usize,
    pub swept: usize,
    pub queued: usize,
}

/// One pass over every open shard. `rescan` also walks every account to
/// queue the ones that should be mirrored and aren't (after a policy
/// change, and at startup).
pub async fn sweep_once<C: Chain>(a: &Archive, state: &StateStore<C>, rescan: bool) -> anyhow::Result<SweepReport> {
    let gate = a.gate();
    let now = crate::state::now_secs();
    let mut r = SweepReport::default();
    for s in state.shards() {
        let mut metas: Vec<(String, Meta)> = Vec::new();
        let opts = slatedb::config::ScanOptions::default();
        let mut it = vs::FamilyScan::new(&*s.db, META_FAMILY, None, &opts).await?;
        while let Some(kv) = it.next().await? {
            let Some(did) = mirror::did_from_meta_key(&kv.key) else { continue };
            metas.push((did.to_string(), Meta::decode(&kv.value)?));
        }
        let mut live = HashSet::new();
        for (did, meta) in metas {
            if meta.live.is_some() {
                r.mirrors += 1;
            }
            let rec = s.load(&did).await?;
            let host = state.host_of(&s, &did).await;
            let taken_down = rec.as_ref().is_some_and(|r| r.relay_takedown || r.upstream == Upstream::Takendown);
            let keep = rec.as_ref().is_some_and(|r| r.upstream != Upstream::Deleted)
                && host.as_deref().is_some_and(|h| gate.wants(h))
                && !(taken_down
                    && meta.takedown_at != 0
                    && now.saturating_sub(meta.takedown_at) >= gate.takedown_retention_secs());
            let mut want = meta.clone();
            if taken_down && want.takedown_at == 0 {
                want.takedown_at = now.max(1);
            } else if !taken_down {
                want.takedown_at = 0;
            }
            if want.staging.is_some() && !a.queue.contains(&did) {
                want.garbage.extend(want.staging.take());
            }
            if meta.live.is_some() && !keep {
                if delete_mirror(&s, &did).await? {
                    r.deleted += 1;
                    a.stats.deleted_repos.fetch_add(1, Relaxed);
                }
            } else if want != meta {
                let _g = s.lock_did(&did).await;
                let mut cur = mirror::read_meta(&s.db, &did).await?.unwrap_or_default();
                cur.takedown_at = want.takedown_at;
                if want.staging.is_none() && cur.staging == meta.staging {
                    cur.garbage.extend(cur.staging.take());
                }
                mirror::write_rows(&s.db, [cur.mutation(&did)]).await?;
            }
            if meta.live.is_some() && keep {
                live.insert(did.clone());
            }
            let n = sweep_garbage(&s, &did).await?;
            r.swept += n;
            a.stats.deleted_rows.fetch_add(n as u64, Relaxed);
        }
        if rescan {
            r.queued += queue_unmirrored(a, state, &s, &live).await?;
        }
    }
    Ok(r)
}

/// Queues every active account of shard `s` that should be mirrored and
/// isn't.
async fn queue_unmirrored<C: Chain>(
    a: &Archive,
    state: &StateStore<C>,
    s: &ShardState,
    live: &HashSet<String>,
) -> anyhow::Result<usize> {
    let gate = a.gate();
    let mut n = 0;
    let opts = slatedb::config::ScanOptions::default();
    let mut it = vs::FamilyScan::new(&*s.db, &[crate::state::record::DID_FAMILY], None, &opts).await?;
    while let Some(kv) = it.next().await? {
        let Some(did) = crate::state::record::did_from_key(&kv.key) else { continue };
        if live.contains(&did) {
            continue;
        }
        let rec = crate::state::Record::decode(&kv.value)?;
        if rec.chain.is_none() || !rec.status().is_active() {
            continue;
        }
        let Some(host) = state.host_name(rec.pds.unwrap_or(rec.host)) else { continue };
        if gate.wants(&host) && a.queue.enqueue(&did, &host, Why::Switch) {
            n += 1;
        }
    }
    Ok(n)
}

/// Stops serving `did`'s mirror and moves its rows to the garbage. False:
/// it wasn't mirrored.
pub async fn delete_mirror(s: &ShardState, did: &str) -> anyhow::Result<bool> {
    let mut g = s.lock_did(did).await;
    let deadline = std::time::Instant::now() + Duration::from_secs(30);
    while s.mirror.outstanding(did) > 0 {
        anyhow::ensure!(std::time::Instant::now() < deadline, "commits still uncommitted");
        drop(g);
        tokio::time::sleep(Duration::from_millis(5)).await;
        g = s.lock_did(did).await;
    }
    let mut meta = mirror::read_meta(&s.db, did).await?.unwrap_or_default();
    let Some(generation) = meta.live.take() else { return Ok(false) };
    meta.garbage.push(generation);
    meta.takedown_at = 0;
    mirror::write_rows(&s.db, [Mutation { key: vs::head_key(did).into(), val: None }, meta.mutation(did)]).await?;
    s.mirror.forget(did);
    drop(g);
    Ok(true)
}

/// Deletes the rows of every generation in `did`'s garbage, then drops them
/// from its meta. Returns the rows deleted.
pub async fn sweep_garbage(s: &ShardState, did: &str) -> anyhow::Result<usize> {
    let Some(meta) = mirror::read_meta(&s.db, did).await? else { return Ok(0) };
    if meta.garbage.is_empty() {
        return Ok(0);
    }
    let mut n = 0;
    for &generation in &meta.garbage {
        for fam in vs::GEN_FAMILIES {
            let prefix = vs::gen_prefix(fam, did, generation);
            let mut it = vs::BatchedScan::new(s.db.scan(prefix.clone()..vs::prefix_end(&prefix)).await?);
            let mut batch = Vec::new();
            while let Some(kv) = it.next().await? {
                batch.push(Mutation { key: kv.key, val: None });
                if batch.len() == DELETES_PER_BATCH {
                    n += batch.len();
                    mirror::write_rows(&s.db, std::mem::take(&mut batch)).await?;
                }
            }
            n += batch.len();
            mirror::write_rows(&s.db, batch).await?;
        }
    }
    let _g = s.lock_did(did).await;
    let mut cur = mirror::read_meta(&s.db, did).await?.unwrap_or_default();
    cur.garbage.retain(|g| !meta.garbage.contains(g));
    mirror::write_rows(&s.db, [cur.mutation(did)]).await?;
    Ok(n)
}
