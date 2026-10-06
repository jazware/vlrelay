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
}

#[async_trait::async_trait]
impl Sink for WriterSink {
    async fn apply(&self, ops: Vec<(String, super::Seed)>) -> anyhow::Result<usize> {
        let a = self.w.apply(ops).await?;
        for d in &a.changed {
            self.cache.invalidate(d);
        }
        Ok(a.written)
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
}

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
        })
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
            Arc::new(move || !me.stopped.load(Relaxed) && qnode.upgrade().is_some_and(|q| PlcJob::leads(&q, epoch)))
        };
        let w = loop {
            if !keep() {
                return Ok(());
            }
            match SeedWriter::open(&self.store, self.cache_ttl()).await {
                Ok(w) => break Arc::new(w),
                Err(e) => {
                    tracing::warn!(epoch, "PLC export: opening the seed database: {e:#}");
                    tokio::time::sleep(Duration::from_secs(2)).await;
                }
            }
        };
        tracing::info!(epoch, url = %self.cfg.url, "PLC export: this node leads; reading the export");
        *self.seeds.writer.write() = Some(w.clone());
        let sink = Arc::new(WriterSink { w: w.clone(), cache: self.cache.clone() });
        let ing = Ingester::new(self.cfg.clone(), self.store.clone(), sink);
        *self.term.lock() = Some((epoch, ing.stats.clone()));
        ing.supervise(keep).await;
        *self.term.lock() = None;
        *self.seeds.writer.write() = None;
        w.close().await;
        tracing::info!(epoch, "PLC export: the term ended");
        Ok(())
    }

    fn cache_ttl(&self) -> Duration {
        crate::identity::Options::default().ttl
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
        })
    }
}
