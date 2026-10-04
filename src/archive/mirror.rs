//! One shard's mirrors: the per-DID meta row, applying commits to the
//! stored tree, and the in-memory trees of repos with uncommitted commits.

use crate::state::ShardState;
use anyhow::Context as _;
use bytes::{BufMut, Bytes};
use parking_lot::Mutex;
use slatedb::Db;
use std::collections::HashMap;
use std::sync::Arc;
use vlpds::cbor::ValueRef;
use vlpds::cid::Cid;
use vlpds::mst_lazy::LazyTree;
use vlpds::mst_store::DbSource;
use vlpds::segment::Mutation;
use vlpds::state::{self as vs, Head};
use vlpds::tid::Tid;

pub const META_FAMILY: &[u8] = b"V/";
/// Interior nodes (height >= 1) are stored; leaves are rebuilt from records.
pub const PERSIST_MIN: i32 = 1;

pub fn meta_key(did: &str) -> Vec<u8> {
    [&vs::slot_prefix(vlpds::slots::slot_of(did))[..], META_FAMILY, did.as_bytes()].concat()
}

pub fn did_from_meta_key(key: &[u8]) -> Option<&str> {
    std::str::from_utf8(vs::key_body(key).strip_prefix(META_FAMILY)?).ok()
}

/// `V/{did}`: which generation of the account's rows readers use, one being
/// staged by a fetch, and ones left to delete.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct Meta {
    pub live: Option<u64>,
    pub staging: Option<u64>,
    pub garbage: Vec<u64>,
    /// Unix seconds when the sweeper first saw the account taken down (0:
    /// not taken down).
    pub takedown_at: u32,
}

impl Meta {
    pub fn is_empty(&self) -> bool {
        self.live.is_none() && self.staging.is_none() && self.garbage.is_empty() && self.takedown_at == 0
    }

    /// A generation no row can be under.
    pub fn next_gen(&self) -> u64 {
        let top = self.garbage.iter().copied().chain(self.staging).chain(self.live).max();
        top.map_or(0, |g| g + 1)
    }

    pub fn encode(&self) -> Bytes {
        let mut b = Vec::with_capacity(32 + 8 * self.garbage.len());
        b.put_u8(self.live.is_some() as u8 | (self.staging.is_some() as u8) << 1);
        b.put_u64(self.live.unwrap_or(0));
        b.put_u64(self.staging.unwrap_or(0));
        b.put_u32(self.takedown_at);
        b.put_u32(self.garbage.len() as u32);
        for g in &self.garbage {
            b.put_u64(*g);
        }
        b.into()
    }

    pub fn decode(b: &[u8]) -> anyhow::Result<Meta> {
        anyhow::ensure!(b.len() >= 25, "short mirror meta");
        let u64_at = |i: usize| u64::from_be_bytes(b[i..i + 8].try_into().expect("8 bytes"));
        let flags = b[0];
        let n = u32::from_be_bytes(b[21..25].try_into().expect("4 bytes")) as usize;
        anyhow::ensure!(b.len() == 25 + 8 * n, "mirror meta of {} bytes", b.len());
        Ok(Meta {
            live: (flags & 1 != 0).then(|| u64_at(1)),
            staging: (flags & 2 != 0).then(|| u64_at(9)),
            takedown_at: u32::from_be_bytes(b[17..21].try_into().expect("4 bytes")),
            garbage: (0..n).map(|i| u64_at(25 + 8 * i)).collect(),
        })
    }

    pub fn mutation(&self, did: &str) -> Mutation {
        Mutation { key: meta_key(did).into(), val: (!self.is_empty()).then(|| self.encode()) }
    }
}

pub async fn read_meta(db: &Db, did: &str) -> anyhow::Result<Option<Meta>> {
    match db.get(meta_key(did)).await? {
        Some(b) => Ok(Some(Meta::decode(&b)?)),
        None => Ok(None),
    }
}

pub async fn read_head(db: &Db, did: &str) -> anyhow::Result<Option<Head>> {
    match db.get(vs::head_key(did)).await? {
        Some(b) => Ok(Some(Head::decode(&b)?)),
        None => Ok(None),
    }
}

/// The live generation and its head, if the account is mirrored.
pub async fn live(db: &Db, did: &str) -> anyhow::Result<Option<(u64, Head)>> {
    let Some(generation) = read_meta(db, did).await?.and_then(|m| m.live) else { return Ok(None) };
    Ok(read_head(db, did).await?.map(|h| (generation, h)))
}

pub async fn write_rows(db: &Db, rows: impl IntoIterator<Item = Mutation>) -> Result<(), slatedb::Error> {
    let mut wb = slatedb::WriteBatch::new();
    let mut any = false;
    for m in rows {
        any = true;
        match m.val {
            Some(v) => wb.put(&m.key, &v),
            None => wb.delete(&m.key),
        }
    }
    if any {
        db.write(wb).await?;
    }
    Ok(())
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct HeadLite {
    pub rev: Tid,
    pub data: Cid,
}

struct Slot {
    generation: u64,
    head: HeadLite,
    /// None while an apply has it out (or before the first load).
    tree: Option<LazyTree>,
    /// Applied commits whose rows aren't written yet.
    outstanding: u32,
    used: u64,
}

/// Trees with nothing outstanding kept per shard, so an active account's
/// next commit doesn't reopen its tree from the DB (most of the apply cost).
pub static IDLE_TREES: std::sync::atomic::AtomicUsize = std::sync::atomic::AtomicUsize::new(4096);
/// Loaded levels an idle tree keeps: the top of the tree is shared by every
/// path, the rest is one commit's paths.
const IDLE_DEPTH: usize = 3;

/// The shard's in-memory side of its mirrors: the trees of repos with
/// uncommitted commits (so the next commit builds on them), and each
/// ticket's rows until its log entry is durable.
#[derive(Default)]
pub struct ShardMirror {
    slots: Mutex<HashMap<Arc<str>, Slot>>,
    tick: std::sync::atomic::AtomicU64,
    by_ticket: Mutex<HashMap<u64, (Arc<str>, Vec<Mutation>)>>,
}

impl ShardMirror {
    fn head(&self, did: &str) -> Option<(u64, HeadLite)> {
        self.slots.lock().get(did).map(|s| (s.generation, s.head))
    }

    fn take_tree(&self, did: &str) -> Option<LazyTree> {
        self.slots.lock().get_mut(did).and_then(|s| s.tree.take())
    }

    /// Puts the tree back after an apply; `added` counts one more
    /// uncommitted commit.
    fn finish(&self, did: &str, generation: u64, head: HeadLite, tree: Option<LazyTree>, added: bool) {
        let used = self.tick.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        let mut m = self.slots.lock();
        let s = m.entry(Arc::from(did)).or_insert(Slot { generation, head, tree: None, outstanding: 0, used });
        s.generation = generation;
        s.head = head;
        s.tree = tree;
        s.used = used;
        if added {
            s.outstanding += 1;
        }
        if s.outstanding == 0 && s.tree.is_none() {
            m.remove(did);
        }
        Self::trim(&mut m);
    }

    /// Evicts the least recently used idle trees past [`IDLE_TREES`], down
    /// to three quarters of it so the scan is rare.
    fn trim(m: &mut HashMap<Arc<str>, Slot>) {
        let cap = IDLE_TREES.load(std::sync::atomic::Ordering::Relaxed);
        let idle = |s: &Slot| s.outstanding == 0 && s.tree.is_some();
        if m.values().filter(|s| idle(s)).count() <= cap {
            return;
        }
        let mut ages: Vec<u64> = m.values().filter(|s| idle(s)).map(|s| s.used).collect();
        ages.sort_unstable();
        let keep = cap * 3 / 4;
        let cut = ages.len().saturating_sub(keep);
        let below = if cut == 0 { 0 } else { ages[cut - 1] + 1 };
        m.retain(|_, s| !idle(s) || s.used >= below);
    }

    pub fn attach(&self, ticket: u64, did: &str, rows: Vec<Mutation>) {
        self.by_ticket.lock().insert(ticket, (Arc::from(did), rows));
    }

    pub fn take_ticket(&self, ticket: u64) -> Option<(Arc<str>, Vec<Mutation>)> {
        self.by_ticket.lock().remove(&ticket)
    }

    /// A ticket's rows are written.
    pub fn settle(&self, did: &str) {
        let mut m = self.slots.lock();
        if let Some(s) = m.get_mut(did) {
            s.outstanding = s.outstanding.saturating_sub(1);
            if s.outstanding == 0
                && let Some(t) = s.tree.as_mut()
            {
                t.unload(IDLE_DEPTH);
            }
        }
        Self::trim(&mut m);
    }

    pub fn outstanding(&self, did: &str) -> u32 {
        self.slots.lock().get(did).map_or(0, |s| s.outstanding)
    }

    /// Drops the in-memory tree (an import or a delete replaced the rows).
    pub fn forget(&self, did: &str) {
        self.slots.lock().remove(did);
    }

    pub fn len(&self) -> (usize, usize) {
        (self.slots.lock().len(), self.by_ticket.lock().len())
    }
}

pub enum Applied {
    Rows(Vec<Mutation>),
    /// At or behind the mirror's rev (a bootstrap already holds it).
    Skip,
    /// Not mirrored.
    Absent,
    /// Its prevData isn't the mirror's head: the mirror missed commits.
    Stale,
}

struct Info {
    rev: Tid,
    prev_data: Option<Cid>,
}

fn header_kind(frame: &[u8]) -> anyhow::Result<(String, usize)> {
    let (h, n) = ValueRef::decode_prefix(frame)?;
    let t = h.get("t").and_then(ValueRef::as_str).context("frame without t")?.to_string();
    Ok((t, n))
}

fn commit_info(frame: &[u8]) -> anyhow::Result<Info> {
    let (_, n) = ValueRef::decode_prefix(frame)?;
    let body = ValueRef::decode(&frame[n..])?;
    let rev = body.get("rev").and_then(ValueRef::as_str).and_then(Tid::parse).context("#commit without rev")?;
    let prev_data = match body.get("prevData") {
        Some(ValueRef::Link(c)) => Some(*c),
        _ => None,
    };
    Ok(Info { rev, prev_data })
}

/// The new head a #sync frame carries: its CAR's root commit.
fn sync_head(frame: &[u8]) -> anyhow::Result<Head> {
    let (_, n) = ValueRef::decode_prefix(frame)?;
    let body = ValueRef::decode(&frame[n..])?;
    let rev = body.get("rev").and_then(ValueRef::as_str).and_then(Tid::parse).context("#sync without rev")?;
    let Some(ValueRef::Bytes(car)) = body.get("blocks") else { anyhow::bail!("#sync without blocks") };
    let (roots, blocks) = vlpds::car::read_car(car)?;
    let commit = *roots.first().context("#sync CAR without a root")?;
    let block = blocks.iter().find(|(c, _)| *c == commit).map(|(_, b)| *b).context("#sync without its commit")?;
    let data = match ValueRef::decode(block)?.get("data") {
        Some(ValueRef::Link(c)) => *c,
        _ => anyhow::bail!("commit without data"),
    };
    Ok(Head { commit, data, rev, commit_block: Bytes::copy_from_slice(block) })
}

fn is_backlink(key: &[u8]) -> bool {
    vs::key_body(key).starts_with(vs::BACKLINK_FAMILY)
}

/// A #commit's rows on the tree at the mirror's head: the records, their CID
/// index and the head as vlpds's replay derives them from the frame (no
/// backlinks: a relay doesn't serve them), then the `M/` nodes the ops wrote
/// and replaced. Fails if the tree the ops give isn't the commit's `data`.
pub fn apply_frame(
    tree: &mut LazyTree,
    src: &dyn vlpds::mst_lazy::Source,
    did: &str,
    generation: u64,
    frame: &[u8],
) -> anyhow::Result<(Vec<Mutation>, HeadLite)> {
    let muts = vlpds::segment::derive_commit_muts(frame, generation)?;
    let rp = vs::record_prefix(did, generation);
    let hk = vs::head_key(did);
    let mut rows = Vec::with_capacity(muts.len() + 8);
    let mut claimed = None;
    for m in muts {
        if m.key.starts_with(&rp) {
            let path = &m.key[rp.len()..];
            match &m.val {
                Some(v) => {
                    let (cid, _) = vs::record_value_parts(v)?;
                    tree.insert(path, cid, src)?;
                }
                None => {
                    tree.remove(path, src)?;
                }
            }
        } else if m.key[..] == hk[..] {
            claimed = Some(Head::decode(m.val.as_ref().context("head delete")?)?);
        } else if is_backlink(&m.key) {
            continue;
        }
        rows.push(m);
    }
    let head = claimed.context("#commit without a head")?;
    let mut blocks = Vec::new();
    let (root, persist) = tree.write_diff_blocks(&mut blocks)?;
    anyhow::ensure!(root == head.data, "the stored tree gives {root}, the commit says {}", head.data);
    for c in &persist.deletes {
        rows.push(Mutation { key: vs::mst_node_key(did, generation, c).into(), val: None });
    }
    for (c, b) in persist.puts {
        rows.push(Mutation { key: vs::mst_node_key(did, generation, &c).into(), val: Some(Bytes::from(b)) });
    }
    Ok((rows, HeadLite { rev: head.rev, data: head.data }))
}

async fn current(s: &ShardState, did: &str) -> anyhow::Result<Option<(u64, HeadLite)>> {
    if let Some(h) = s.mirror.head(did) {
        return Ok(Some(h));
    }
    Ok(live(&s.db, did).await?.map(|(g, h)| (g, HeadLite { rev: h.rev, data: h.data })))
}

/// Applies a live #commit to the account's mirror. The caller holds the
/// DID's lock and, on `Rows`, attaches them to the event's ticket.
pub async fn apply_live(s: &ShardState, did: &str, frame: Bytes) -> anyhow::Result<Applied> {
    let Some((generation, head)) = current(s, did).await? else { return Ok(Applied::Absent) };
    let info = commit_info(&frame)?;
    if info.rev <= head.rev {
        return Ok(Applied::Skip);
    }
    if info.prev_data.is_some_and(|p| p != head.data) {
        return Ok(Applied::Stale);
    }
    let tree = s.mirror.take_tree(did);
    let (db, d) = (s.db.clone(), did.to_string());
    let (r, tree) = tokio::task::spawn_blocking(move || {
        let rt = tokio::runtime::Handle::current();
        let src = DbSource::new(&*db, &d, generation, &rt);
        let mut tree = match tree {
            Some(t) => t,
            None => match LazyTree::open(head.data, PERSIST_MIN, &src) {
                Ok(t) => t,
                Err(e) => return (Err(anyhow::anyhow!("open the stored tree: {e}")), None),
            },
        };
        let backup = tree.clone();
        match apply_frame(&mut tree, &src, &d, generation, &frame) {
            Ok(r) => (Ok(r), Some(tree)),
            Err(e) => (Err(e), Some(backup)),
        }
    })
    .await?;
    match r {
        Ok((rows, new_head)) => {
            s.mirror.finish(did, generation, new_head, tree, true);
            Ok(Applied::Rows(rows))
        }
        Err(e) => {
            s.mirror.finish(did, generation, head, tree, false);
            Err(e)
        }
    }
}

/// A #sync on a mirrored account: Some(rows) when it restates the mirror's
/// tree (a new commit for the same data), Ok(None) when it isn't mirrored,
/// Err when the mirror needs a fresh copy.
pub async fn apply_sync(s: &ShardState, did: &str, frame: &Bytes) -> anyhow::Result<Option<Vec<Mutation>>> {
    let Some((generation, head)) = current(s, did).await? else { anyhow::bail!("not mirrored") };
    let new = sync_head(frame)?;
    anyhow::ensure!(new.data == head.data, "#sync to another tree");
    if new.rev < head.rev {
        return Ok(None);
    }
    let rows = vec![Mutation { key: vs::head_key(did).into(), val: Some(new.encode()) }];
    let tree = s.mirror.take_tree(did);
    s.mirror.finish(did, generation, HeadLite { rev: new.rev, data: new.data }, tree, true);
    Ok(Some(rows))
}

/// A logged frame on recovery, written at once. Ok(false): nothing to do.
pub async fn replay_frame(s: &ShardState, did: &str, frame: Bytes) -> anyhow::Result<bool> {
    let (kind, _) = header_kind(&frame)?;
    let Some((generation, head)) = live(&s.db, did).await? else { return Ok(false) };
    match kind.as_str() {
        "#commit" => {
            let info = commit_info(&frame)?;
            if info.rev <= head.rev {
                return Ok(false);
            }
            anyhow::ensure!(info.prev_data.is_none_or(|p| p == head.data), "the mirror missed commits");
            let (db, d) = (s.db.clone(), did.to_string());
            let rows = tokio::task::spawn_blocking(move || {
                let rt = tokio::runtime::Handle::current();
                let src = DbSource::new(&*db, &d, generation, &rt);
                let mut tree = LazyTree::open(head.data, PERSIST_MIN, &src).map_err(|e| anyhow::anyhow!("{e}"))?;
                apply_frame(&mut tree, &src, &d, generation, &frame).map(|(rows, _)| rows)
            })
            .await??;
            write_rows(&s.db, rows).await?;
            Ok(true)
        }
        "#sync" => {
            let new = sync_head(&frame)?;
            if new.rev < head.rev || (new.rev == head.rev && new.commit == head.commit) {
                return Ok(false);
            }
            anyhow::ensure!(new.data == head.data, "#sync to another tree");
            write_rows(&s.db, [Mutation { key: vs::head_key(did).into(), val: Some(new.encode()) }]).await?;
            Ok(true)
        }
        _ => Ok(false),
    }
}
