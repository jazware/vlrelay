//! Cluster-wide budgets. The fast ones (PLC lookups/s, new accounts/min)
//! are split evenly: each node gets budget ÷ live nodes and enforces its
//! share with a local token bucket, so nothing crosses the network per
//! event. The slow one (new hosts per day) is a counter object in the bucket
//! updated by compare-and-swap, like vlpds's mail budget.

use super::doc::Cluster;
use super::store::{get, if_match, is_conflict, path, put};
use parking_lot::Mutex;
use serde::{Deserialize, Serialize};
use std::sync::atomic::{AtomicUsize, Ordering};
use vlpds::store::Store;

#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub enum BudgetKind {
    PlcLookupsPerSec,
    NewAccountsPerMin,
    /// Shared through the bucket, not split.
    NewHostsPerDay,
    /// At least one per node.
    ArchivalFetchConcurrency,
    ArchivalFetchBytesPerSec,
}

/// How many nodes are live right now. The cluster module implements it over
/// the node leases; [`FixedNodes`] is for tests and single-node runs.
pub trait LiveNodes: Send + Sync {
    fn live_nodes(&self) -> usize;
}

pub struct FixedNodes(pub AtomicUsize);

impl FixedNodes {
    pub fn new(n: usize) -> FixedNodes {
        FixedNodes(AtomicUsize::new(n))
    }
    pub fn set(&self, n: usize) {
        self.0.store(n, Ordering::Relaxed);
    }
}

impl LiveNodes for FixedNodes {
    fn live_nodes(&self) -> usize {
        self.0.load(Ordering::Relaxed)
    }
}

/// This node's share. A node that sees no live nodes (its own lease isn't
/// up yet) takes the whole budget, which errs toward serving.
pub fn share(c: &Cluster, kind: BudgetKind, live: usize) -> f64 {
    let n = live.max(1) as f64;
    match kind {
        BudgetKind::PlcLookupsPerSec => c.plc_lookups_per_sec / n,
        BudgetKind::NewAccountsPerMin => c.new_accounts_per_min / n,
        BudgetKind::NewHostsPerDay => c.new_hosts_per_day as f64,
        BudgetKind::ArchivalFetchConcurrency => (c.archival_fetch_concurrency as f64 / n).ceil().max(1.0),
        BudgetKind::ArchivalFetchBytesPerSec => c.archival_fetch_bytes_per_sec as f64 / n,
    }
}

/// A token bucket whose rate is re-read on every take, so a node joining or
/// leaving changes everyone's share within one refill. Burst is one second
/// of the rate (at least one token).
#[derive(Default)]
pub struct Bucket {
    /// (tokens, at ms). None until the first take, which starts full.
    st: Mutex<Option<(f64, i64)>>,
}

impl Bucket {
    /// `per_sec` tokens a second.
    pub fn try_take(&self, per_sec: f64, n: f64, now_ms: i64) -> bool {
        let burst = per_sec.max(1.0);
        let mut st = self.st.lock();
        let tokens = match *st {
            None => burst,
            Some((t, at)) => (t + (now_ms - at).max(0) as f64 / 1000.0 * per_sec).min(burst),
        };
        let ok = tokens >= n;
        *st = Some((if ok { tokens - n } else { tokens }, now_ms));
        ok
    }
}

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct DayCount {
    /// Unix day (UTC).
    pub day: u32,
    pub used: u32,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Spend {
    Spent(u32),
    /// Nothing written.
    Exhausted(u32),
}

const CAS_RETRIES: usize = 8;

/// A per-day counter object shared by every node (`policy/counters/...`).
pub struct DailyCounter {
    store: Store,
    path: object_store::path::Path,
    io: tokio::sync::Mutex<()>,
}

impl DailyCounter {
    pub fn new(store: Store, rel: &str) -> DailyCounter {
        DailyCounter { path: path(&store, rel), store, io: Default::default() }
    }

    pub async fn read(&self, now_ms: i64) -> anyhow::Result<u32> {
        let day = (now_ms / 86_400_000) as u32;
        Ok(match get(&self.store, &self.path, None).await? {
            Some((b, _)) => serde_json::from_slice::<DayCount>(&b).ok().filter(|c| c.day == day).map_or(0, |c| c.used),
            None => 0,
        })
    }

    pub async fn spend(&self, limit: u32, now_ms: i64) -> anyhow::Result<Spend> {
        let day = (now_ms / 86_400_000) as u32;
        let _io = self.io.lock().await;
        for _ in 0..CAS_RETRIES {
            let (cur, etag) = match get(&self.store, &self.path, None).await? {
                // unreadable: overwritten by this spend
                Some((b, e)) => (serde_json::from_slice::<DayCount>(&b).unwrap_or_default(), e),
                None => (DayCount::default(), None),
            };
            let used = if cur.day == day { cur.used } else { 0 };
            if used >= limit {
                return Ok(Spend::Exhausted(used));
            }
            let next = DayCount { day, used: used + 1 };
            let body = serde_json::to_vec(&next)?;
            match put(&self.store, &self.path, body, if_match(etag)).await {
                Ok(_) => return Ok(Spend::Spent(next.used)),
                Err(e) if is_conflict(&e) || matches!(e, object_store::Error::NotFound { .. }) => {
                    continue;
                }
                Err(e) => return Err(e.into()),
            }
        }
        anyhow::bail!("daily counter still contended after {CAS_RETRIES} tries")
    }
}
