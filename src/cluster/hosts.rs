//! Host shards: which node keeps the websockets to which PDSes.
//!
//! A hostname hashes to one of vlpds's 65,536 slots (the same hash as DIDs),
//! and a fixed uniform layout groups the slots into host shards. Each shard
//! has a CAS-written assignment under `assign-hosts/`, next to vlpds's
//! `assign/` for DID shards, with the same rules: a node takes free shards
//! and the shards of owners that aren't live, up to its fair share
//! (ceil(shards / live nodes)), and hands extras straight to a node that's
//! short. Liveness is the DID cluster's: one node lease per node, judged by
//! vlpds's `Cluster`.
//!
//! Host shards need no fencing. Two nodes subscribed to one host for a
//! moment only send the DID owner the same events twice, and `check_chain`
//! acks the second copy as a duplicate.
//!
//! A planned handoff closes the sockets and writes their acked cursors to
//! `hostck/` before the assignment names the new owner, which reads them
//! and connects at once (it's nudged). A crash takeover waits for the
//! owner's lease to lapse (TTL + skew), then resumes from the last periodic
//! checkpoint, and the PDS replays what came after it.

use crate::types::Host;
use crate::upstream::HostFilter;
use object_store::path::Path;
use object_store::{ObjectStoreExt, PutMode, PutOptions, PutPayload};
use parking_lot::{Mutex, RwLock};
use serde::{Deserialize, Serialize};
use std::collections::{BTreeMap, BTreeSet, HashMap, HashSet};
use std::sync::Arc;
use tokio::sync::watch;
use vlpds::cluster::if_match;
use vlpds::slots::{Layout, ShardId};
use vlpds::store::Store;

pub const DEFAULT_HOST_SHARDS: u32 = 64;
const DIR: &str = "assign-hosts";
const CHECKPOINTS: &str = "hostck";

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Default)]
#[serde(default)]
pub struct HostAssignment {
    pub owner: Option<String>,
    pub log_id: Option<String>,
    pub addr: Option<String>,
    pub epoch: u64,
    #[serde(flatten)]
    pub extra: serde_json::Map<String, serde_json::Value>,
}

/// The host shards this node holds, as the upstream manager follows them.
#[derive(Clone)]
pub struct HostOwnership {
    pub layout: Arc<Layout>,
    pub owned: Arc<BTreeSet<ShardId>>,
}

impl HostOwnership {
    pub fn shard_of(&self, host: &Host) -> ShardId {
        self.layout.shard_of(&host.0)
    }

    pub fn owns(&self, host: &Host) -> bool {
        self.owned.contains(&self.shard_of(host))
    }

    pub fn filter(&self) -> HostFilter {
        let me = self.clone();
        Arc::new(move |h: &Host| me.owns(h))
    }
}

/// A node that counts toward host shards: a live core node.
#[derive(Clone, Debug)]
pub struct Member {
    pub node_id: String,
    pub log_id: String,
    pub addr: String,
    pub draining: bool,
}

/// What the cluster asks of the upstream side when host shards move.
#[async_trait::async_trait]
pub trait HostHandler: Send + Sync + 'static {
    /// Stops subscribing to every host `keep` rejects. Returns the acked
    /// cursor of each host it stopped.
    async fn release(&self, keep: HostFilter) -> Vec<(Host, i64)>;
    /// The acked cursor of every host this node subscribes to.
    fn acked(&self) -> Vec<(Host, i64)>;
}

/// Upstream cursors in the bucket, one object per host shard. Only a
/// shard's owner writes it, but a zombie owner may still be writing, so
/// every write is a CAS that carries the writer's host assignment epoch:
/// an owner claims the object with its epoch when it takes the shard, and
/// a write from a lower epoch is refused. Within an epoch writes merge by
/// max.
///
/// Each host's cursor carries the generation of its sequence, bumped when
/// the host restarted it (FutureCursor). The max only merges cursors of one
/// generation: a node that still holds a cursor of the old sequence (one
/// that owned the host before another node saw the restart) can't push the
/// stored cursor back up into it, and taking a shard replaces what we held
/// with what's stored.
pub struct Checkpoints {
    store: Store,
    /// Read when a shard was taken (and our own writes since).
    cursors: RwLock<HashMap<Host, i64>>,
    /// The sequence generation each cursor in `cursors` belongs to.
    gens: RwLock<HashMap<Host, u64>>,
    /// Hosts that restarted their sequence (FutureCursor): their next write
    /// starts a new generation with the cursor it carries.
    resets: Mutex<HashSet<Host>>,
    /// What we last wrote per shard, and at which assignment epoch.
    written: Mutex<HashMap<ShardId, (u64, BTreeMap<String, i64>)>>,
    /// The upstream registry, whose acked cursors a shard's take replaces.
    pub(crate) registry: std::sync::OnceLock<Arc<crate::upstream::Registry>>,
    /// Hosts taken before their registry entry existed (a shard taken while
    /// starting): the entry, seeded from its host record, gets the stored
    /// cursor on its first connect.
    unrestored: Mutex<HashSet<Host>>,
}

#[derive(Serialize, Deserialize, Default)]
struct CheckpointDoc {
    cursors: BTreeMap<String, i64>,
    #[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
    gens: BTreeMap<String, u64>,
    /// The host assignment epoch of the owner that last claimed it.
    #[serde(default)]
    epoch: u64,
}

impl Checkpoints {
    pub fn new(store: Store) -> Arc<Checkpoints> {
        Arc::new(Checkpoints {
            store,
            cursors: Default::default(),
            gens: Default::default(),
            resets: Default::default(),
            written: Default::default(),
            registry: Default::default(),
            unrestored: Default::default(),
        })
    }

    fn path(&self, shard: ShardId) -> Path {
        Path::from(format!("{}/{CHECKPOINTS}/{}", self.store.prefix, shard.key()))
    }

    async fn read(&self, shard: ShardId) -> anyhow::Result<(CheckpointDoc, Option<String>)> {
        match self.store.raw.get(&self.path(shard)).await {
            Ok(r) => {
                let etag = r.meta.e_tag.clone();
                Ok((serde_json::from_slice(&r.bytes().await?)?, etag))
            }
            Err(object_store::Error::NotFound { .. }) => Ok((CheckpointDoc::default(), None)),
            Err(e) => Err(e.into()),
        }
    }

    /// Reads a shard's cursors into memory (taking it over). What's stored
    /// replaces what we held, here and in the registry: while another node
    /// owned the host its sequence may have restarted, and our old cursor
    /// would skip the new sequence up to it.
    pub async fn load(&self, shard: ShardId) -> anyhow::Result<usize> {
        let (doc, _) = self.read(shard).await?;
        let reg = self.registry.get();
        let (mut c, mut g) = (self.cursors.write(), self.gens.write());
        let mut r = self.resets.lock();
        let mut later = self.unrestored.lock();
        for (h, seq) in &doc.cursors {
            let host = Host(h.clone());
            match reg.and_then(|reg| reg.get(&host)) {
                Some(e) => {
                    e.restore_cursor(*seq);
                    later.remove(&host);
                }
                None => {
                    later.insert(host.clone());
                }
            }
            r.remove(&host);
            g.insert(host.clone(), doc.gens.get(h).copied().unwrap_or(0));
            c.insert(host, *seq);
        }
        drop((c, g, r, later));
        self.written.lock().remove(&shard);
        Ok(doc.cursors.len())
    }

    pub fn get(&self, host: &Host) -> Option<i64> {
        self.cursors.read().get(host).copied()
    }

    pub fn reset(&self, host: &Host) {
        self.cursors.write().remove(host);
        self.resets.lock().insert(host.clone());
    }

    fn generation(&self, host: &Host) -> u64 {
        self.gens.read().get(host).copied().unwrap_or(0)
    }

    /// Stamps the shard's object with the epoch of the assignment that
    /// gave it to us, so the previous owner's writes are refused from now
    /// on. False if a later owner already claimed it.
    pub async fn claim(&self, shard: ShardId, epoch: u64) -> anyhow::Result<bool> {
        self.write(shard, &[], epoch).await
    }

    /// Merges `cursors` into the shard's object as the owner at assignment
    /// `epoch`. Skips the PUT when nothing moved since our last write, and
    /// refuses (false) when the object belongs to a later epoch.
    pub async fn write(&self, shard: ShardId, cursors: &[(Host, i64)], epoch: u64) -> anyhow::Result<bool> {
        let resets: HashSet<Host> = {
            let r = self.resets.lock();
            cursors.iter().filter(|(h, _)| r.contains(h)).map(|(h, _)| h.clone()).collect()
        };
        if resets.is_empty()
            && let Some((at, prev)) = self.written.lock().get(&shard)
            && *at == epoch
            && cursors.iter().all(|(h, s)| prev.get(&h.0).is_some_and(|p| p >= s))
        {
            return Ok(false);
        }
        loop {
            let (mut doc, etag) = self.read(shard).await?;
            if doc.epoch > epoch {
                SUPERSEDED.inc();
                tracing::warn!(
                    shard = shard.0,
                    ours = epoch,
                    theirs = doc.epoch,
                    "host checkpoint refused: a later owner claimed the shard"
                );
                return Ok(false);
            }
            doc.epoch = epoch;
            for (h, seq) in cursors {
                let (ours, stored) = (self.generation(h), doc.gens.get(&h.0).copied().unwrap_or(0));
                if resets.contains(h) {
                    doc.gens.insert(h.0.clone(), ours.max(stored) + 1);
                    doc.cursors.insert(h.0.clone(), *seq);
                } else if ours < stored {
                    // a cursor of a sequence the host has since restarted
                    continue;
                } else {
                    if ours > stored {
                        doc.gens.insert(h.0.clone(), ours);
                    }
                    let e = doc.cursors.entry(h.0.clone()).or_insert(*seq);
                    *e = (*e).max(*seq);
                }
            }
            let mode = if etag.is_some() { if_match(etag) } else { PutMode::Create };
            let body = PutPayload::from(serde_json::to_vec(&doc)?);
            match self.store.raw.put_opts(&self.path(shard), body, PutOptions { mode, ..Default::default() }).await {
                Ok(_) => {
                    {
                        let mut r = self.resets.lock();
                        for h in &resets {
                            r.remove(h);
                        }
                    }
                    let (mut c, mut g) = (self.cursors.write(), self.gens.write());
                    for (h, s) in &doc.cursors {
                        c.insert(Host(h.clone()), *s);
                        g.insert(Host(h.clone()), doc.gens.get(h).copied().unwrap_or(0));
                    }
                    drop((c, g));
                    self.written.lock().insert(shard, (epoch, doc.cursors));
                    return Ok(true);
                }
                Err(e) if is_conflict(&e) => continue,
                Err(e) => return Err(e.into()),
            }
        }
    }
}

static SUPERSEDED: std::sync::LazyLock<prometheus::IntCounter> = std::sync::LazyLock::new(|| {
    prometheus::register_int_counter!(
        "vlrelay_host_checkpoints_superseded_total",
        "Host checkpoint writes refused because a later owner of the host shard claimed its hostck object"
    )
    .unwrap()
});

fn is_conflict(e: &object_store::Error) -> bool {
    matches!(e, object_store::Error::Precondition { .. } | object_store::Error::AlreadyExists { .. })
}

/// Lets the upstream manager resume from the newer of its own acked cursor
/// and the checkpoint a previous owner left.
pub struct ClusterCursors {
    pub checkpoints: Arc<Checkpoints>,
}

impl ClusterCursors {
    pub fn set_registry(&self, r: Arc<crate::upstream::Registry>) {
        let _ = self.checkpoints.registry.set(r);
    }
}

impl crate::upstream::CursorSource for ClusterCursors {
    fn durable_cursor(&self, host: &Host) -> Option<i64> {
        let entry = self.checkpoints.registry.get().and_then(|r| r.get(host));
        if let Some(e) = &entry
            && self.checkpoints.unrestored.lock().remove(host)
            && let Some(c) = self.checkpoints.get(host)
        {
            e.restore_cursor(c);
            return Some(c);
        }
        let local = entry.and_then(|e| e.acked_seq());
        local.max(self.checkpoints.get(host))
    }

    fn on_future_cursor(&self, host: &Host) {
        self.checkpoints.reset(host);
    }
}

type Versioned = (HostAssignment, Option<String>);

/// What one step did, for tests and logs.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct StepReport {
    pub acquired: Vec<ShardId>,
    pub adopted: Vec<ShardId>,
    pub handed: Vec<(ShardId, String)>,
    pub lost: Vec<ShardId>,
}

pub struct HostShards {
    store: Store,
    node_id: String,
    log_id: String,
    addr: String,
    layout: Arc<Layout>,
    /// shard -> (assignment, ETag)
    assigns: RwLock<BTreeMap<ShardId, (HostAssignment, Option<String>)>>,
    owned: RwLock<BTreeSet<ShardId>>,
    tx: watch::Sender<HostOwnership>,
    filter_tx: watch::Sender<HostFilter>,
    pub checkpoints: Arc<Checkpoints>,
    handler: RwLock<Option<Arc<dyn HostHandler>>>,
    step_lock: tokio::sync::Mutex<()>,
}

impl HostShards {
    /// Reads (or creates) the host layout.
    pub async fn new(store: Store, node_id: &str, log_id: &str, addr: &str, shards: u32) -> anyhow::Result<Arc<Self>> {
        let layout = Arc::new(ensure_layout(&store, shards).await?);
        let own = HostOwnership { layout: layout.clone(), owned: Default::default() };
        let filter = own.filter();
        Ok(Arc::new(HostShards {
            checkpoints: Checkpoints::new(store.clone()),
            store,
            node_id: node_id.to_string(),
            log_id: log_id.to_string(),
            addr: addr.to_string(),
            layout,
            assigns: Default::default(),
            owned: Default::default(),
            tx: watch::channel(own).0,
            filter_tx: watch::channel(filter).0,
            handler: RwLock::new(None),
            step_lock: tokio::sync::Mutex::new(()),
        }))
    }

    pub fn set_handler(&self, h: Arc<dyn HostHandler>) {
        *self.handler.write() = Some(h);
    }

    pub fn layout(&self) -> Arc<Layout> {
        self.layout.clone()
    }

    pub fn watch(&self) -> watch::Receiver<HostOwnership> {
        self.tx.subscribe()
    }

    /// The same, as the filter `upstream::Manager::follow_filter` takes.
    pub fn filter_watch(&self) -> watch::Receiver<HostFilter> {
        self.filter_tx.subscribe()
    }

    pub fn ownership(&self) -> HostOwnership {
        self.tx.borrow().clone()
    }

    pub fn owns(&self, host: &Host) -> bool {
        self.owned.read().contains(&self.layout.shard_of(&host.0))
    }

    pub fn owned(&self) -> Vec<ShardId> {
        self.owned.read().iter().copied().collect()
    }

    /// (node id, addr) of the live owner as last read.
    pub fn owner_of(&self, host: &Host) -> Option<(String, String)> {
        let s = self.layout.shard_of(&host.0);
        let a = self.assigns.read();
        let (a, _) = a.get(&s)?;
        Some((a.owner.clone()?, a.addr.clone().unwrap_or_default()))
    }

    /// Each host shard's owner as last read, in layout order.
    pub fn owners(&self) -> Vec<Option<String>> {
        let a = self.assigns.read();
        self.layout.ids().iter().map(|s| a.get(s).and_then(|(x, _)| x.owner.clone())).collect()
    }

    fn path(&self, shard: ShardId) -> Path {
        Path::from(format!("{}/{DIR}/{}", self.store.prefix, shard.key()))
    }

    fn names_us(&self, a: &HostAssignment) -> bool {
        a.owner.as_deref() == Some(&self.node_id) && a.log_id.as_deref() == Some(&self.log_id)
    }

    fn publish(&self) {
        let own = HostOwnership { layout: self.layout.clone(), owned: Arc::new(self.owned.read().clone()) };
        let changed = *own.owned != *self.tx.borrow().owned;
        if changed {
            self.filter_tx.send_replace(own.filter());
            self.tx.send_replace(own);
        }
    }

    /// One LIST, then a GET of each assignment whose ETag changed.
    async fn read_assignments(&self) -> anyhow::Result<()> {
        use futures::StreamExt;
        let prefix = Path::from(format!("{}/{DIR}", self.store.prefix));
        let mut listed: Vec<(ShardId, Option<String>)> = Vec::new();
        let mut list = self.store.raw.list(Some(&prefix));
        while let Some(m) = list.next().await {
            let m = m?;
            if let Some(id) = m.location.filename().and_then(ShardId::from_key) {
                listed.push((id, m.e_tag.clone()));
            }
        }
        let stale: Vec<ShardId> = {
            let a = self.assigns.read();
            listed
                .iter()
                .filter(|(id, etag)| etag.is_none() || a.get(id).is_none_or(|(_, e)| e != etag))
                .map(|(id, _)| *id)
                .collect()
        };
        let fetched: Vec<(ShardId, Option<Versioned>)> = futures::stream::iter(stale)
            .map(|id| async move { (id, self.get(id).await) })
            .buffered(16)
            .map(|(id, r)| r.map(|v| (id, v)))
            .collect::<Vec<_>>()
            .await
            .into_iter()
            .collect::<anyhow::Result<_>>()?;
        let mut a = self.assigns.write();
        for (id, got) in fetched {
            match got {
                Some(v) => {
                    a.insert(id, v);
                }
                None => {
                    a.remove(&id);
                }
            }
        }
        Ok(())
    }

    async fn get(&self, id: ShardId) -> anyhow::Result<Option<(HostAssignment, Option<String>)>> {
        match self.store.raw.get(&self.path(id)).await {
            Ok(r) => {
                let etag = r.meta.e_tag.clone();
                Ok(Some((serde_json::from_slice(&r.bytes().await?)?, etag)))
            }
            Err(object_store::Error::NotFound { .. }) => Ok(None),
            Err(e) => Err(e.into()),
        }
    }

    /// CAS from the version we read. False if someone else wrote first.
    async fn cas(&self, id: ShardId, new: HostAssignment) -> anyhow::Result<bool> {
        let etag = self.assigns.read().get(&id).and_then(|(_, e)| e.clone());
        let known = self.assigns.read().contains_key(&id);
        let mode = if known { if_match(etag) } else { PutMode::Create };
        let body = PutPayload::from(serde_json::to_vec(&new)?);
        match self.store.raw.put_opts(&self.path(id), body, PutOptions { mode, ..Default::default() }).await {
            Ok(r) => {
                self.assigns.write().insert(id, (new, r.e_tag));
                Ok(true)
            }
            Err(e) if is_conflict(&e) || matches!(e, object_store::Error::NotFound { .. }) => {
                if let Some(v) = self.get(id).await? {
                    self.assigns.write().insert(id, v);
                }
                Ok(false)
            }
            Err(e) => Err(e.into()),
        }
    }

    /// One round: adopt shards handed to us, drop ones reassigned under us,
    /// take free and orphaned shards up to the fair share, hand extras to
    /// nodes short of it. `members` are the live core nodes, us included.
    /// Returns the addresses to nudge (recipients of handoffs).
    pub async fn step(&self, members: &[Member]) -> anyhow::Result<(StepReport, Vec<String>)> {
        let _g = self.step_lock.lock().await;
        let mut report = StepReport::default();
        self.read_assignments().await?;
        let live: HashMap<&str, &Member> = members.iter().map(|m| (m.node_id.as_str(), m)).collect();
        let ids = self.layout.ids();
        let me_draining = live.get(self.node_id.as_str()).is_none_or(|m| m.draining);
        {
            let a = self.assigns.read();
            let mut owned = self.owned.write();
            for s in owned.clone() {
                if a.get(&s).is_none_or(|(x, _)| !self.names_us(x)) {
                    owned.remove(&s);
                    report.lost.push(s);
                }
            }
            for s in &ids {
                if !owned.contains(s) && a.get(s).is_some_and(|(x, _)| self.names_us(x)) {
                    owned.insert(*s);
                    report.adopted.push(*s);
                }
            }
        }
        for s in &report.adopted {
            if let Err(e) = self.checkpoints.load(*s).await {
                tracing::warn!(shard = s.0, "reading host checkpoints failed: {e:#}");
            }
            self.claim(*s).await;
        }
        if !report.adopted.is_empty() {
            tracing::info!(shards = ?report.adopted, "adopted host shards handed to us");
        }
        if !report.lost.is_empty() {
            tracing::warn!(shards = ?report.lost, "host shards reassigned under us");
        }
        self.publish();
        let counted = members.iter().filter(|m| !m.draining).count().max(1);
        let fair = ids.len().div_ceil(counted);
        let held = |a: &HostAssignment| {
            a.owner.as_deref().and_then(|o| live.get(o)).is_some_and(|m| a.log_id.as_deref() == Some(&m.log_id))
        };
        let owned_n = self.owned.read().len();
        if !me_draining && owned_n < fair {
            let want = fair - owned_n;
            let free: Vec<(ShardId, HostAssignment)> = {
                let a = self.assigns.read();
                ids.iter()
                    .filter(|s| a.get(s).is_none_or(|(x, _)| !held(x)))
                    .map(|s| (*s, a.get(s).map(|(x, _)| x.clone()).unwrap_or_default()))
                    .collect()
            };
            // spread simultaneous joiners over different shards
            let start = (vlpds::slots::slot_of(&self.node_id) as usize) % free.len().max(1);
            for (s, cur) in free.iter().cycle().skip(start).take(free.len()) {
                if report.acquired.len() == want {
                    break;
                }
                let new = HostAssignment {
                    owner: Some(self.node_id.clone()),
                    log_id: Some(self.log_id.clone()),
                    addr: Some(self.addr.clone()),
                    epoch: cur.epoch + 1,
                    extra: cur.extra.clone(),
                };
                if self.cas(*s, new).await? {
                    if let Err(e) = self.checkpoints.load(*s).await {
                        tracing::warn!(shard = s.0, "reading host checkpoints failed: {e:#}");
                    }
                    self.claim(*s).await;
                    self.owned.write().insert(*s);
                    report.acquired.push(*s);
                }
            }
            if !report.acquired.is_empty() {
                tracing::info!(shards = ?report.acquired, fair, "acquired host shards");
            }
            self.publish();
        } else {
            let target = if me_draining { 0 } else { fair };
            report.handed = self.hand_off(members, target, &held).await?;
        }
        let nudges: Vec<String> = {
            let mut v: Vec<String> =
                report.handed.iter().filter_map(|(_, n)| live.get(n.as_str()).map(|m| m.addr.clone())).collect();
            v.sort();
            v.dedup();
            v
        };
        Ok((report, nudges))
    }

    /// Our assignment epoch for `shard`, if the assignment as last read
    /// names us.
    fn our_epoch(&self, shard: ShardId) -> Option<u64> {
        self.assigns.read().get(&shard).filter(|(a, _)| self.names_us(a)).map(|(a, _)| a.epoch)
    }

    /// Best effort: until it lands the previous owner's late writes still
    /// merge by max, as before epochs, and the next checkpoint retries it.
    async fn claim(&self, shard: ShardId) {
        let Some(epoch) = self.our_epoch(shard) else { return };
        if let Err(e) = self.checkpoints.claim(shard, epoch).await {
            tracing::warn!(shard = shard.0, "claiming host checkpoints failed: {e:#}");
        }
    }

    /// Hands shards above `target` to the members furthest below the fair
    /// share: closes their sockets and checkpoints first.
    async fn hand_off(
        &self,
        members: &[Member],
        target: usize,
        held: &(dyn Fn(&HostAssignment) -> bool + Sync),
    ) -> anyhow::Result<Vec<(ShardId, String)>> {
        let owned: Vec<ShardId> = self.owned.read().iter().copied().collect();
        if owned.len() <= target {
            return Ok(Vec::new());
        }
        let counted = members.iter().filter(|m| !m.draining).count().max(1);
        let fair = self.layout.shards.len().div_ceil(counted);
        let mut short: Vec<(String, usize)> = {
            let a = self.assigns.read();
            members
                .iter()
                .filter(|m| !m.draining && m.node_id != self.node_id)
                .map(|m| {
                    let n = a.values().filter(|(x, _)| held(x) && x.owner.as_deref() == Some(&m.node_id)).count();
                    (m.node_id.clone(), n)
                })
                .filter(|(_, n)| *n < fair)
                .collect()
        };
        if short.is_empty() {
            return Ok(Vec::new());
        }
        let mut plan: Vec<(ShardId, String)> = Vec::new();
        let mut give = owned.len() - target;
        // newest-numbered first; round-robin so each short node gets a slice
        let mut it = owned.iter().rev();
        while give > 0 {
            short.sort_by_key(|(_, n)| *n);
            let Some((node, n)) = short.first_mut().filter(|(_, n)| *n < fair) else {
                break;
            };
            let Some(s) = it.next() else { break };
            plan.push((*s, node.clone()));
            *n += 1;
            give -= 1;
        }
        if plan.is_empty() {
            return Ok(plan);
        }
        let giving: HashSet<ShardId> = plan.iter().map(|(s, _)| *s).collect();
        {
            let mut o = self.owned.write();
            for s in &giving {
                o.remove(s);
            }
        }
        self.publish();
        let handler = self.handler.read().clone();
        if let Some(h) = handler {
            let keep = self.ownership().filter();
            let stopped = h.release(keep).await;
            let mut by: HashMap<ShardId, Vec<(Host, i64)>> = HashMap::new();
            for (host, seq) in stopped {
                by.entry(self.layout.shard_of(&host.0)).or_default().push((host, seq));
            }
            for (s, cursors) in by {
                if giving.contains(&s)
                    && let Some(epoch) = self.our_epoch(s)
                {
                    self.checkpoints.write(s, &cursors, epoch).await?;
                }
            }
        }
        let addrs: HashMap<&str, &str> = members.iter().map(|m| (m.node_id.as_str(), m.addr.as_str())).collect();
        let mut done = Vec::new();
        for (s, node) in plan {
            let Some(m) = members.iter().find(|m| m.node_id == node) else {
                continue;
            };
            let cur = self.assigns.read().get(&s).map(|(a, _)| a.clone()).unwrap_or_default();
            let new = HostAssignment {
                owner: Some(node.clone()),
                log_id: Some(m.log_id.clone()),
                addr: addrs.get(node.as_str()).map(|a| a.to_string()),
                epoch: cur.epoch + 1,
                extra: cur.extra,
            };
            if self.cas(s, new).await? {
                done.push((s, node));
            }
        }
        if !done.is_empty() {
            tracing::info!(handed = ?done, "handed host shards over");
        }
        Ok(done)
    }

    /// Writes the acked cursors of the hosts we subscribe to, per shard.
    pub async fn checkpoint(&self) -> anyhow::Result<usize> {
        let Some(h) = self.handler.read().clone() else {
            return Ok(0);
        };
        let owned = self.owned.read().clone();
        let mut by: HashMap<ShardId, Vec<(Host, i64)>> = HashMap::new();
        for (host, seq) in h.acked() {
            let s = self.layout.shard_of(&host.0);
            if owned.contains(&s) {
                by.entry(s).or_default().push((host, seq));
            }
        }
        let mut n = 0;
        for (s, c) in by {
            let Some(epoch) = self.our_epoch(s) else { continue };
            if self.checkpoints.write(s, &c, epoch).await? {
                n += 1;
            }
        }

        Ok(n)
    }

    /// Tests: forget everything as a crash would (no release, no checkpoint).
    pub fn halt(&self) {
        self.owned.write().clear();
        self.publish();
    }
}

async fn ensure_layout(store: &Store, shards: u32) -> anyhow::Result<Layout> {
    let path = Path::from(format!("{}/{DIR}/layout", store.prefix));
    loop {
        match store.raw.get(&path).await {
            Ok(r) => {
                let l: Layout = serde_json::from_slice(&r.bytes().await?)?;
                l.validate()?;
                return Ok(l);
            }
            Err(object_store::Error::NotFound { .. }) => {}
            Err(e) => return Err(e.into()),
        }
        let l = Layout::uniform(shards);
        let body = PutPayload::from(serde_json::to_vec(&l)?);
        match store.raw.put_opts(&path, body, PutOptions { mode: PutMode::Create, ..Default::default() }).await {
            Ok(_) => return Ok(l),
            Err(e) if is_conflict(&e) => continue,
            Err(e) => return Err(e.into()),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// A host owner that lost its shard keeps a checkpoint loop running
    /// until it notices. Once the new owner claimed the object, those
    /// writes are refused: they would raise cursors past what the new
    /// owner's dedupe pruning saw, or undo its FutureCursor reset.
    #[tokio::test]
    async fn a_previous_owners_checkpoints_are_refused_once_the_new_owner_claims() {
        let store = Store::memory(None);
        let (old, new) = (Checkpoints::new(store.clone()), Checkpoints::new(store.clone()));
        let s = ShardId(3);
        let h = Host("pds.test".into());
        assert!(old.write(s, &[(h.clone(), 100)], 1).await.unwrap());
        new.load(s).await.unwrap();
        assert!(new.claim(s, 2).await.unwrap());
        new.reset(&h);
        assert!(new.write(s, &[(h.clone(), 7)], 2).await.unwrap(), "a reset replaces the cursor");
        assert!(!old.write(s, &[(h.clone(), 150)], 1).await.unwrap(), "the old epoch is refused");
        let fresh = Checkpoints::new(store.clone());
        fresh.load(s).await.unwrap();
        assert_eq!(fresh.get(&h), Some(7));
        assert!(new.write(s, &[(h.clone(), 9)], 2).await.unwrap());
        assert!(!new.claim(s, 1).await.unwrap(), "a lower claim never takes it back");
    }
}
