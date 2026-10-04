//! The bootstrap queue: repos that need a full copy, fetched with `getRepo`
//! from the account's PDS (the DID document's endpoint), checked, and
//! imported under a fresh generation.
//!
//! Politeness lives here. Each host has a token bucket at its tier's
//! `archivalFetchesPerHost`, and the node's share of the cluster's
//! `archivalFetchConcurrency` and `archivalFetchBytesPerSec` caps the rest.
//! Hosts take turns, so one big PDS's backlog doesn't hold up the others.
//!
//! While a repo is queued or being fetched, its live commits keep being
//! checked, sequenced and emitted, and their frames wait in the queue entry.
//! The import applies the ones past the fetched rev, under the DID's lock,
//! before the mirror goes live.

use super::Archive;
use super::mirror::{self, HeadLite, PERSIST_MIN};
use crate::state::{Chain, StateStore};
use crate::verify::SigningKey;
use anyhow::Context as _;
use bytes::Bytes;
use parking_lot::Mutex;
use std::collections::{HashMap, VecDeque};
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering::Relaxed};
use std::time::{Duration, Instant};
use vlpds::cid::Cid;
use vlpds::mst_lazy::LazyTree;
use vlpds::mst_store::DbSource;
use vlpds::segment::Mutation;
use vlpds::state::{self as vs, Head};
use vlpds::tid::Tid;

#[derive(Clone, Copy, Debug, PartialEq)]
pub struct FetchLimits {
    pub per_host_per_sec: f64,
    /// This node's share.
    pub concurrency: usize,
    pub bytes_per_sec: f64,
}

impl Default for FetchLimits {
    fn default() -> Self {
        FetchLimits { per_host_per_sec: 1.0, concurrency: 8, bytes_per_sec: 50.0 * 1024.0 * 1024.0 }
    }
}

/// Where an account's repo lives and the key its commits are signed with.
pub struct Resolved {
    /// The `#atproto_pds` endpoint (`https://...`).
    pub endpoint: String,
    pub key: SigningKey,
}

#[async_trait::async_trait]
pub trait Resolver: Send + Sync {
    async fn resolve(&self, did: &str) -> anyhow::Result<Resolved>;
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Why {
    /// First seen by an archiving relay.
    New,
    /// prevData mismatch or a desynchronized account.
    Chain,
    /// A #sync the mirror can't link.
    Sync,
    /// The stored tree disagreed with a commit.
    Mismatch,
    /// Archiving was switched on for it.
    Switch,
    Admin,
}

impl Why {
    pub fn as_str(self) -> &'static str {
        match self {
            Why::New => "new",
            Why::Chain => "chain",
            Why::Sync => "sync",
            Why::Mismatch => "mismatch",
            Why::Switch => "switch",
            Why::Admin => "admin",
        }
    }
}

/// A repo bigger than this isn't mirrored (the import holds it in memory).
pub const MAX_REPO_BYTES: usize = 256 << 20;
const MAX_TRIES: u32 = 5;
/// Frames held per queued repo; past either cap the import fetches again.
const BUFFER_FRAMES: usize = 1024;
const BUFFER_BYTES: usize = 16 << 20;
const ROWS_PER_BATCH: usize = 4096;

struct Entry {
    host: String,
    running: bool,
    not_before: Option<Instant>,
    tries: u32,
    buf: Vec<Bytes>,
    buf_bytes: usize,
    overflow: bool,
}

struct HostQ {
    dids: VecDeque<Arc<str>>,
    tokens: f64,
    at: Instant,
}

struct Inner {
    entries: HashMap<Arc<str>, Entry>,
    hosts: HashMap<String, HostQ>,
    /// Hosts with queued repos, in turn order.
    turn: VecDeque<String>,
    running: usize,
    bytes_tokens: f64,
    bytes_at: Instant,
}

#[derive(Default)]
pub struct FetchStats {
    pub queued: AtomicU64,
    pub done: AtomicU64,
    pub failed: AtomicU64,
    pub retried: AtomicU64,
    pub bytes: AtomicU64,
    pub records: AtomicU64,
    pub fetch_us: AtomicU64,
    pub import_us: AtomicU64,
    pub replayed_frames: AtomicU64,
    pub healed: AtomicU64,
    pub by_why: Mutex<HashMap<&'static str, u64>>,
}

pub struct Queue {
    inner: Mutex<Inner>,
    notify: tokio::sync::Notify,
    resolver: Arc<dyn Resolver>,
    http: reqwest::Client,
    pub stats: FetchStats,
    /// The last few failures, for the operator.
    pub errors: Mutex<VecDeque<(String, String)>>,
}

impl Queue {
    pub fn new(resolver: Arc<dyn Resolver>) -> Arc<Queue> {
        let http = reqwest::Client::builder()
            .timeout(Duration::from_secs(300))
            .connect_timeout(Duration::from_secs(10))
            .user_agent(concat!("vlrelay/", env!("CARGO_PKG_VERSION"), " (atproto-relay archival)"))
            .build()
            .expect("reqwest client");
        Arc::new(Queue {
            inner: Mutex::new(Inner {
                entries: HashMap::new(),
                hosts: HashMap::new(),
                turn: VecDeque::new(),
                running: 0,
                bytes_tokens: 0.0,
                bytes_at: Instant::now(),
            }),
            notify: tokio::sync::Notify::new(),
            resolver,
            http,
            stats: FetchStats::default(),
            errors: Mutex::new(VecDeque::new()),
        })
    }

    /// Queues `did` unless it's queued or being fetched already.
    pub fn enqueue(&self, did: &str, host: &str, why: Why) -> bool {
        let mut g = self.inner.lock();
        if g.entries.contains_key(did) {
            return false;
        }
        let did: Arc<str> = Arc::from(did);
        g.entries.insert(
            did.clone(),
            Entry {
                host: host.to_string(),
                running: false,
                not_before: None,
                tries: 0,
                buf: Vec::new(),
                buf_bytes: 0,
                overflow: false,
            },
        );
        push_host(&mut g, host, did);
        drop(g);
        self.stats.queued.fetch_add(1, Relaxed);
        *self.stats.by_why.lock().entry(why.as_str()).or_default() += 1;
        self.notify.notify_one();
        true
    }

    /// Holds a live frame for a queued repo. False: not queued.
    pub fn buffer(&self, did: &str, frame: &Bytes) -> bool {
        let mut g = self.inner.lock();
        let Some(e) = g.entries.get_mut(did) else { return false };
        if e.buf.len() >= BUFFER_FRAMES || e.buf_bytes + frame.len() > BUFFER_BYTES {
            e.overflow = true;
        } else {
            e.buf_bytes += frame.len();
            e.buf.push(frame.clone());
        }
        true
    }

    pub fn contains(&self, did: &str) -> bool {
        self.inner.lock().entries.contains_key(did)
    }

    /// (queued, running).
    pub fn depth(&self) -> (usize, usize) {
        let g = self.inner.lock();
        (g.entries.len() - g.running, g.running)
    }

    /// The buffered frames, once the import holds the DID's lock.
    fn take_buffer(&self, did: &str) -> (Vec<Bytes>, bool) {
        let mut g = self.inner.lock();
        match g.entries.get_mut(did) {
            Some(e) => {
                e.buf_bytes = 0;
                (std::mem::take(&mut e.buf), std::mem::take(&mut e.overflow))
            }
            None => (Vec::new(), false),
        }
    }

    /// The import went live: later frames go to the mirror directly.
    fn finish(&self, did: &str) {
        let mut g = self.inner.lock();
        if g.entries.remove(did).is_some_and(|e| e.running) {
            g.running -= 1;
        }
    }

    /// The fetch failed: try again later, or give up.
    fn failed(&self, did: &str, err: &anyhow::Error) {
        let mut g = self.inner.lock();
        let Some(e) = g.entries.get_mut(did) else { return };
        if e.running {
            e.running = false;
            g.running -= 1;
        }
        let e = g.entries.get_mut(did).expect("present");
        e.tries += 1;
        let host = e.host.clone();
        if e.tries >= MAX_TRIES {
            g.entries.remove(did);
            self.stats.failed.fetch_add(1, Relaxed);
        } else {
            e.not_before = Some(Instant::now() + Duration::from_secs(2u64 << e.tries));
            self.stats.retried.fetch_add(1, Relaxed);
            push_host(&mut g, &host, Arc::from(did));
        }
        drop(g);
        let mut errs = self.errors.lock();
        if errs.len() >= 32 {
            errs.pop_front();
        }
        errs.push_back((did.to_string(), format!("{err:#}")));
    }

    /// The next repo to fetch now, or how long to wait for one.
    fn next(&self, gate: &dyn super::Gate) -> Result<Arc<str>, Duration> {
        let idle = Duration::from_millis(250);
        let mut g = self.inner.lock();
        let now = Instant::now();
        let lim = gate.limits("");
        if g.running >= lim.concurrency.max(1) {
            return Err(idle);
        }
        let dt = now.duration_since(g.bytes_at).as_secs_f64();
        g.bytes_at = now;
        g.bytes_tokens = (g.bytes_tokens + dt * lim.bytes_per_sec).min(lim.bytes_per_sec);
        if g.bytes_tokens < 0.0 {
            return Err(Duration::from_secs_f64(-g.bytes_tokens / lim.bytes_per_sec.max(1.0)).min(idle));
        }
        let mut wait = idle;
        for _ in 0..g.turn.len() {
            let Some(host) = g.turn.pop_front() else { break };
            let rate = gate.limits(&host).per_host_per_sec.max(0.001);
            let Inner { hosts, entries, .. } = &mut *g;
            let Some(q) = hosts.get_mut(&host) else { continue };
            q.tokens = (q.tokens + now.duration_since(q.at).as_secs_f64() * rate).min(rate.max(1.0));
            q.at = now;
            // skip entries that left or wait for a retry
            let mut pick = None;
            for _ in 0..q.dids.len() {
                let d = q.dids.pop_front().expect("len");
                match entries.get(&d) {
                    None => continue,
                    Some(e) if e.running => continue,
                    Some(e) if e.not_before.is_some_and(|t| t > now) => q.dids.push_back(d),
                    Some(_) => {
                        pick = Some(d);
                        break;
                    }
                }
            }
            let empty = q.dids.is_empty();
            match pick {
                Some(d) if q.tokens >= 1.0 => {
                    q.tokens -= 1.0;
                    if !empty {
                        g.turn.push_back(host.clone());
                    } else {
                        g.hosts.remove(&host);
                    }
                    g.running += 1;
                    g.entries.get_mut(&d).expect("present").running = true;
                    return Ok(d);
                }
                Some(d) => {
                    wait = wait.min(Duration::from_secs_f64((1.0 - q.tokens) / rate));
                    q.dids.push_front(d);
                    g.turn.push_back(host);
                }
                None if empty => {
                    g.hosts.remove(&host);
                }
                None => g.turn.push_back(host),
            }
        }
        Err(wait)
    }

    fn spend_bytes(&self, n: usize) {
        self.inner.lock().bytes_tokens -= n as f64;
    }

    async fn get_latest(&self, endpoint: &str, did: &str) -> anyhow::Result<(Cid, Tid)> {
        #[derive(serde::Deserialize)]
        struct Latest {
            cid: String,
            rev: String,
        }
        let url = format!("{}/xrpc/com.atproto.sync.getLatestCommit?did={did}", endpoint.trim_end_matches('/'));
        let r = self.http.get(&url).send().await?;
        anyhow::ensure!(r.status().is_success(), "getLatestCommit: {}", r.status());
        let l: Latest = r.json().await?;
        Ok((Cid::parse(&l.cid)?, Tid::parse(&l.rev).context("getLatestCommit: bad rev")?))
    }

    async fn get_repo(&self, endpoint: &str, did: &str) -> anyhow::Result<Bytes> {
        let url = format!("{}/xrpc/com.atproto.sync.getRepo?did={did}", endpoint.trim_end_matches('/'));
        let mut r = self.http.get(&url).send().await?;
        anyhow::ensure!(r.status().is_success(), "getRepo: {}", r.status());
        if r.content_length().is_some_and(|n| n as usize > MAX_REPO_BYTES) {
            anyhow::bail!("repo over {MAX_REPO_BYTES} bytes");
        }
        let mut body = Vec::new();
        while let Some(c) = r.chunk().await? {
            body.extend_from_slice(&c);
            anyhow::ensure!(body.len() <= MAX_REPO_BYTES, "repo over {MAX_REPO_BYTES} bytes");
        }
        Ok(body.into())
    }
}

fn push_host(g: &mut Inner, host: &str, did: Arc<str>) {
    match g.hosts.get_mut(host) {
        Some(q) => q.dids.push_back(did),
        None => {
            g.hosts.insert(host.to_string(), HostQ { dids: VecDeque::from([did]), tokens: 1.0, at: Instant::now() });
            g.turn.push_back(host.to_string());
        }
    }
}

pub fn spawn_workers<C: Chain>(a: Arc<Archive>, state: Arc<StateStore<C>>) {
    tokio::spawn(async move {
        loop {
            let gate = a.gate();
            match a.queue.next(&*gate) {
                Ok(did) => {
                    let (a, state) = (a.clone(), state.clone());
                    tokio::spawn(async move {
                        let t0 = Instant::now();
                        match fetch_and_import(&a, &state, &did).await {
                            Ok(()) => {
                                a.queue.stats.done.fetch_add(1, Relaxed);
                                a.queue.stats.import_us.fetch_add(t0.elapsed().as_micros() as u64, Relaxed);
                            }
                            Err(e) => {
                                tracing::debug!(%did, "archive fetch: {e:#}");
                                a.queue.failed(&did, &e);
                            }
                        }
                        a.queue.notify.notify_one();
                    });
                }
                Err(wait) => {
                    let _ = tokio::time::timeout(wait.max(Duration::from_millis(1)), a.queue.notify.notified()).await;
                }
            }
        }
    });
}

/// What a fetched CAR gives, checked.
pub struct Fetched {
    pub head: Head,
    pub records: Vec<vlpds::xrpc::ImportedRecord>,
    pub tree: vlpds::mst::Tree,
}

/// Checks a getRepo CAR: block hashes and the complete canonical tree
/// (vlpds's import parse), then the commit's DID, version and signature.
pub fn check_car(did: &str, body: &Bytes, key: &SigningKey) -> anyhow::Result<Fetched> {
    let (records, tree) = vlpds::xrpc::parse_import(body).map_err(|e| anyhow::anyhow!("{}: {}", e.error, e.message))?;
    let (roots, blocks) = vlpds::car::read_car(body)?;
    let commit = roots[0];
    let block = blocks.iter().find(|(c, _)| *c == commit).map(|(_, b)| *b).context("no commit block")?;
    let rev = match vlpds::cbor::ValueRef::decode(block)?.get("rev") {
        Some(vlpds::cbor::ValueRef::Text(s)) => Tid::parse(s).context("commit rev is not a TID")?,
        _ => anyhow::bail!("commit without rev"),
    };
    let obj = crate::verify::check_commit_block(block, did, rev, key).map_err(|e| anyhow::anyhow!("commit: {e}"))?;
    Ok(Fetched {
        head: Head { commit, data: obj.data, rev, commit_block: Bytes::copy_from_slice(block) },
        records,
        tree,
    })
}

async fn fetch_and_import<C: Chain>(a: &Archive, state: &StateStore<C>, did: &str) -> anyhow::Result<()> {
    let q = &a.queue;
    let t0 = Instant::now();
    let id = q.resolver.resolve(did).await?;
    let (_, latest_rev) = q.get_latest(&id.endpoint, did).await?;
    let body = q.get_repo(&id.endpoint, did).await?;
    q.spend_bytes(body.len());
    q.stats.bytes.fetch_add(body.len() as u64, Relaxed);
    q.stats.fetch_us.fetch_add(t0.elapsed().as_micros() as u64, Relaxed);
    let d = did.to_string();
    let f = tokio::task::spawn_blocking(move || check_car(&d, &body, &id.key)).await??;
    anyhow::ensure!(f.head.rev >= latest_rev, "getRepo gave rev {} behind getLatestCommit's {latest_rev}", f.head.rev);
    q.stats.records.fetch_add(f.records.len() as u64, Relaxed);
    import(a, state, did, f).await
}

/// Stages a checked repo under a fresh generation, then (under the DID's
/// lock) applies the frames that arrived meanwhile and switches the mirror
/// to it. Also heals the account's sync state from the signed head.
pub async fn import<C: Chain>(a: &Archive, state: &StateStore<C>, did: &str, f: Fetched) -> anyhow::Result<()> {
    let s = state.shard_for(did)?;
    let generation = {
        let _g = s.lock_did(did).await;
        let mut meta = mirror::read_meta(&s.db, did).await?.unwrap_or_default();
        if let Some(old) = meta.staging.take() {
            meta.garbage.push(old);
        }
        let generation = meta.next_gen();
        meta.staging = Some(generation);
        mirror::write_rows(&s.db, [meta.mutation(did)]).await?;
        generation
    };
    let nodes: Vec<(Cid, Arc<[u8]>)> = vlpds::mst_lazy::persisted_nodes(&f.tree, PERSIST_MIN).into_iter().collect();
    let (rows, _, _) = vlpds::xrpc::import_rows(did, generation, f.head.rev, f.records, nodes);
    let mut batch = Vec::with_capacity(ROWS_PER_BATCH);
    for m in rows {
        // listBlobs isn't served: blobs stay on the PDS
        if vs::key_body(&m.key).starts_with(vs::BLOB_REF_FAMILY) {
            continue;
        }
        batch.push(m);
        if batch.len() == ROWS_PER_BATCH {
            mirror::write_rows(&s.db, std::mem::take(&mut batch)).await?;
        }
    }
    mirror::write_rows(&s.db, batch).await?;

    // the frames applied before this fetch must be written before the switch
    let mut g = s.lock_did(did).await;
    let deadline = Instant::now() + Duration::from_secs(30);
    while s.mirror.outstanding(did) > 0 {
        anyhow::ensure!(Instant::now() < deadline, "earlier commits still uncommitted");
        drop(g);
        tokio::time::sleep(Duration::from_millis(5)).await;
        g = s.lock_did(did).await;
    }
    let (frames, overflow) = a.queue.take_buffer(did);
    let mut head = f.head.clone();
    let tree = LazyTree::loaded(f.tree, PERSIST_MIN);
    let (db, d, h0) = (s.db.clone(), did.to_string(), head.clone());
    let (mut rows, head2, applied, broke) = tokio::task::spawn_blocking(move || {
        let rt = tokio::runtime::Handle::current();
        let src = DbSource::new(&*db, &d, generation, &rt);
        replay_buffered(tree, &src, &d, generation, h0, &frames)
    })
    .await??;
    head = head2;
    a.queue.stats.replayed_frames.fetch_add(applied as u64, Relaxed);
    rows.push(Mutation { key: vs::head_key(did).into(), val: Some(head.encode()) });
    let mut meta = mirror::read_meta(&s.db, did).await?.unwrap_or_default();
    if let Some(old) = meta.live.replace(generation) {
        meta.garbage.push(old);
    }
    meta.staging = None;
    rows.push(meta.mutation(did));
    mirror::write_rows(&s.db, rows).await?;
    s.mirror.forget(did);
    heal(state, &s, did, &head).await?;
    if broke || overflow {
        // the frames didn't chain from the fetched head: fetch again
        a.queue.finish(did);
        let host = state.host_of(&s, did).await.unwrap_or_default();
        a.queue.enqueue(did, &host, Why::Chain);
    } else {
        a.queue.finish(did);
    }
    drop(g);
    s.flush_memtable().await?;
    if !meta.garbage.is_empty() {
        let (s2, d2) = (s.clone(), did.to_string());
        tokio::spawn(async move {
            if let Err(e) = super::sweep::sweep_garbage(&s2, &d2).await {
                tracing::warn!(did = %d2, "archive: sweeping old generations: {e:#}");
            }
        });
    }
    Ok(())
}

type Replayed = (Vec<Mutation>, Head, usize, bool);

/// Applies buffered frames past the fetched head. Stops at the first one
/// that doesn't chain (true in the result).
fn replay_buffered(
    mut tree: LazyTree,
    src: &dyn vlpds::mst_lazy::Source,
    did: &str,
    generation: u64,
    mut head: Head,
    frames: &[Bytes],
) -> anyhow::Result<Replayed> {
    let mut rows = Vec::new();
    let mut applied = 0;
    for f in frames {
        let (h, n) = vlpds::cbor::ValueRef::decode_prefix(f)?;
        let kind = h.get("t").and_then(vlpds::cbor::ValueRef::as_str).unwrap_or_default().to_string();
        let body = vlpds::cbor::ValueRef::decode(&f[n..])?;
        let rev = body.get("rev").and_then(vlpds::cbor::ValueRef::as_str).and_then(Tid::parse).context("no rev")?;
        if rev <= head.rev {
            continue;
        }
        match kind.as_str() {
            "#commit" => {
                if let Some(vlpds::cbor::ValueRef::Link(p)) = body.get("prevData")
                    && *p != head.data
                {
                    return Ok((rows, head, applied, true));
                }
                let backup = tree.clone();
                match mirror::apply_frame(&mut tree, src, did, generation, f) {
                    Ok((r, HeadLite { .. })) => {
                        // the head rides in the rows; keep ours current for
                        // the final write
                        for m in &r {
                            if m.key[..] == vs::head_key(did)[..]
                                && let Some(v) = &m.val
                            {
                                head = Head::decode(v)?;
                            }
                        }
                        rows.extend(r);
                        applied += 1;
                    }
                    Err(_) => {
                        drop(backup);
                        return Ok((rows, head, applied, true));
                    }
                }
            }
            _ => return Ok((rows, head, applied, true)),
        }
    }
    Ok((rows, head, applied, false))
}

/// A desynchronized account takes the
/// fetched head as its chain: it's signed by the account's key and checked
/// against the PDS's getLatestCommit, which is what a #sync would say.
async fn heal<C: Chain>(
    state: &StateStore<C>,
    s: &crate::state::ShardState,
    did: &str,
    head: &Head,
) -> anyhow::Result<()> {
    let Some(cur) = s.load(did).await? else { return Ok(()) };
    // a synchronized account keeps its chain: commits between it and the
    // fetched head are still on their way and must not read as stale
    if cur.desync.is_none() || cur.chain.is_some_and(|c| c.rev > head.rev) {
        return Ok(());
    }
    let mut rec = (*cur).clone();
    rec.chain = Some(crate::state::ChainState { rev: head.rev, commit: head.commit, data: head.data });
    rec.desync = None;
    s.stage_unlogged(did, rec);
    s.flush_unlogged().await?;
    if let Some(a) = state.archive() {
        a.queue.stats.healed.fetch_add(1, Relaxed);
    }
    Ok(())
}

impl<C: Chain> StateStore<C> {
    /// Fetches and imports `did` now (tools and tests), bypassing the queue.
    pub async fn archive_fetch_now(&self, did: &str) -> anyhow::Result<()> {
        let a = self.archive().context("archive off")?.clone();
        a.queue.enqueue(did, "", Why::Admin);
        let r = fetch_and_import(&a, self, did).await;
        if let Err(e) = &r {
            a.queue.failed(did, e);
        }
        r
    }
}
