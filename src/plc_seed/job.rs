//! The export reader as the quorum log leader's background job: started
//! when this node leads, stopped when its term ends. Each term opens the
//! seed database as its writer and resumes the export from the bucket's
//! checkpoint.

use super::ingest::{Checkpoint, Config, Ingester, Sink, Stats};
use super::{SeedReader, SeedWriter};
use crate::admin::fleet::PlcReport;
use crate::identity::{HttpFetch, IdentityCache};
use crate::qlog::node::{Node as QNode, Role};
use std::sync::Arc;
use std::sync::atomic::Ordering::Relaxed;
use std::time::{Duration, Instant};
use vlpds::store::Store;

/// The ingester's sink on the leader: the seed database, and this node's
/// cache dropping DIDs whose documents changed (the members' copies age out
/// with the cache's TTL, and an `#identity` refreshes them sooner).
struct WriterSink {
    w: Arc<SeedWriter>,
    cache: Arc<IdentityCache<HttpFetch>>,
    feed: Option<Arc<crate::discovery::Feed>>,
}

#[async_trait::async_trait]
impl Sink for WriterSink {
    async fn apply(&self, ops: Vec<(String, super::Seed)>) -> anyhow::Result<usize> {
        if let Some(f) = &self.feed {
            for (_, s) in &ops {
                if let Some(p) = s.pds.as_deref().filter(|_| !s.tombstone) {
                    f.push(p);
                }
            }
        }
        for (did, seed) in &ops {
            super::invalidate_if_stale(&self.cache, did, seed);
        }
        self.w.apply(ops).await
    }

    async fn flush(&self) -> anyhow::Result<()> {
        self.w.flush().await
    }
}

pub struct PlcJob {
    pub cfg: Config,
    /// The bucket, counted as `qlog_plc`.
    pub store: Store,
    pub seeds: Arc<SeedReader>,
    pub cache: Arc<IdentityCache<HttpFetch>>,
    /// The current term's numbers (None while this node doesn't lead).
    term: parking_lot::Mutex<Option<(u64, Arc<Stats>)>>,
    /// The checkpoint as last read, with when.
    checkpoint: parking_lot::Mutex<Option<(Instant, Option<Checkpoint>)>>,
    pub restarts: std::sync::atomic::AtomicU64,
    stopped: std::sync::atomic::AtomicBool,
    /// Discovery's feed of the PDS hosts the documents name.
    pub feed: parking_lot::Mutex<Option<Arc<crate::discovery::Feed>>>,
    /// Terms ended, or held back, by [`Config::mem_budget_mb`].
    pub mem_pauses: std::sync::atomic::AtomicU64,
    /// jemalloc's allocated bytes, MiB (a test sets its own).
    allocated_mb: fn() -> Option<u64>,
}

/// jemalloc's allocated bytes, MiB; None when it can't be read.
pub fn jemalloc_allocated_mb() -> Option<u64> {
    tikv_jemalloc_ctl::epoch::advance().ok()?;
    tikv_jemalloc_ctl::stats::allocated::read().ok().map(|b| (b >> 20) as u64)
}

/// A term resumes below this share of the budget, so a node hovering at the
/// limit doesn't reopen the seeds every few seconds.
const MEM_RESUME: f64 = 0.85;

/// How often a member checks whether it leads.
const WATCH: Duration = Duration::from_millis(500);
const CHECKPOINT_TTL: Duration = Duration::from_secs(10);

impl PlcJob {
    pub fn new(cfg: Config, store: Store, seeds: Arc<SeedReader>, cache: Arc<IdentityCache<HttpFetch>>) -> Arc<PlcJob> {
        Arc::new(PlcJob {
            cfg,
            store,
            seeds,
            cache,
            term: Default::default(),
            checkpoint: Default::default(),
            restarts: Default::default(),
            stopped: Default::default(),
            feed: Default::default(),
            mem_pauses: Default::default(),
            allocated_mb: jemalloc_allocated_mb,
        })
    }

    #[cfg(test)]
    pub fn with_allocated(mut self: Arc<Self>, f: fn() -> Option<u64>) -> Arc<Self> {
        Arc::get_mut(&mut self).expect("before the job is shared").allocated_mb = f;
        self
    }

    /// Whether the process holds more than `share` of the export's memory
    /// budget. The export is the one thing on a node that can wait: on a
    /// small box, with the seeds past ~35M rows, it took a relay that sat
    /// at 1.3 GB past its 2.3 GB limit within minutes.
    pub fn over_budget(&self, share: f64) -> Option<u64> {
        let budget = self.cfg.mem_budget_mb;
        if budget == 0 {
            return None;
        }
        let mb = (self.allocated_mb)()?;
        (mb as f64 > budget as f64 * share).then_some(mb)
    }

    /// Ends the job (its term ends at its next check).
    pub fn stop(&self) {
        self.stopped.store(true, Relaxed);
    }

    /// The current term's numbers, if this node leads.
    pub fn stats(&self) -> Option<Arc<Stats>> {
        self.term.lock().as_ref().map(|(_, s)| s.clone())
    }

    fn leads(q: &QNode, epoch: u64) -> bool {
        let st = q.status();
        st.role == Role::Leader && st.epoch == epoch
    }

    /// Runs for the node's life: a term of the export for each term this
    /// node leads.
    pub async fn run(self: Arc<Self>, qnode: std::sync::Weak<QNode>) {
        let mut tick = tokio::time::interval(WATCH);
        let mut held = false;
        loop {
            tick.tick().await;
            if self.stopped.load(Relaxed) {
                return;
            }
            let Some(q) = qnode.upgrade() else { return };
            let st = q.status();
            if st.role != Role::Leader {
                continue;
            }
            let epoch = st.epoch;
            drop(q);
            if let Some(mb) = self.over_budget(MEM_RESUME) {
                self.seeds.paused.store(true, Relaxed);
                if !held {
                    held = true;
                    self.mem_pauses.fetch_add(1, Relaxed);
                    tracing::warn!(
                        epoch,
                        allocated_mb = mb,
                        budget_mb = self.cfg.mem_budget_mb,
                        "PLC export: held back, the node is over its memory budget"
                    );
                }
                continue;
            }
            held = false;
            if let Err(e) = self.clone().term(qnode.clone(), epoch).await {
                self.restarts.fetch_add(1, Relaxed);
                tracing::warn!(epoch, "PLC export: the term's reader stopped: {e:#}");
                tokio::time::sleep(Duration::from_secs(1)).await;
            }
        }
    }

    async fn term(self: Arc<Self>, qnode: std::sync::Weak<QNode>, epoch: u64) -> anyhow::Result<()> {
        let keep: Arc<dyn Fn() -> bool + Send + Sync> = {
            let (qnode, me) = (qnode.clone(), self.clone());
            Arc::new(move || {
                if let Some(mb) = me.over_budget(1.0) {
                    if !me.seeds.paused.swap(true, Relaxed) {
                        me.mem_pauses.fetch_add(1, Relaxed);
                        tracing::warn!(
                            epoch,
                            allocated_mb = mb,
                            budget_mb = me.cfg.mem_budget_mb,
                            "PLC export: pausing, the node is over its memory budget"
                        );
                    }
                    return false;
                }
                !me.stopped.load(Relaxed) && qnode.upgrade().is_some_and(|q| PlcJob::leads(&q, epoch))
            })
        };
        let w = loop {
            if !keep() {
                return Ok(());
            }
            match SeedWriter::open(&self.store).await {
                Ok(w) => break Arc::new(w),
                Err(e) => {
                    tracing::warn!(epoch, "PLC export: opening the seed database: {e:#}");
                    tokio::time::sleep(Duration::from_secs(2)).await;
                }
            }
        };
        tracing::info!(epoch, url = %self.cfg.url, "PLC export: this node leads; reading the export");
        self.seeds.paused.store(false, Relaxed);
        *self.seeds.writer.write() = Some(w.clone());
        let sink = Arc::new(WriterSink { w: w.clone(), cache: self.cache.clone(), feed: self.feed.lock().clone() });
        let ing = Ingester::new(self.cfg.clone(), self.store.clone(), sink);
        *self.term.lock() = Some((epoch, ing.stats.clone()));
        let learned = {
            let (seeds, w, keep) = (self.seeds.clone(), w.clone(), keep.clone());
            tokio::spawn(async move { seeds.write_learned(&w, &*keep).await })
        };
        ing.supervise(keep).await;
        *self.seeds.writer.write() = None;
        let _ = learned.await;
        // the export's last checkpoint ran before the last lookups' batch
        if let Err(e) = w.flush().await {
            tracing::debug!(epoch, "PLC export: flushing the last fetched documents: {e:#}");
        }
        *self.term.lock() = None;
        w.close().await;
        tracing::info!(epoch, "PLC export: the term ended");
        Ok(())
    }

    async fn stored_checkpoint(&self) -> Option<Checkpoint> {
        if let Some((at, c)) = self.checkpoint.lock().clone()
            && at.elapsed() < CHECKPOINT_TTL
        {
            return c;
        }
        let c = Checkpoint::load(&self.store).await.ok().flatten();
        *self.checkpoint.lock() = Some((Instant::now(), c.clone()));
        c
    }

    /// This node's part, as the admin API shows it; None when it doesn't
    /// lead (the leader's report is the cluster's).
    pub async fn report(&self) -> Option<PlcReport> {
        let (_, s) = self.term.lock().clone()?;
        let ck = self.stored_checkpoint().await;
        let ops = s.ops.load(Relaxed);
        let (windows, checkpoint_ms) = match ck {
            Some(c) => (super::ingest::windows_view(&c, self.cfg.start_ms), c.updated_ms),
            None => (Vec::new(), 0),
        };
        Some(PlcReport {
            leader: true,
            caught_up: s.caught_up.load(Relaxed),
            ops,
            ops_per_sec: s.rate(),
            written: s.written.load(Relaxed),
            requests: s.requests.load(Relaxed),
            throttled: s.throttled.load(Relaxed),
            errors: s.errors.load(Relaxed),
            restarts: s.restarts.load(Relaxed) + self.restarts.load(Relaxed),
            newest_ms: s.newest_ms.load(Relaxed) as i64,
            windows,
            checkpoint_ms,
            learned: self.seeds.learned_written.load(Relaxed),
            learned_dropped: self.seeds.learned_dropped.load(Relaxed),
        })
    }
}
