//! Event generation: per-account MSTs, signed sync 1.1 commits, the size mix
//! and the generation-side faults. Each generator thread owns a disjoint set
//! of accounts (index mod threads), so per-account order needs no locks and
//! survives the shared per-host queues.

use super::fleet::Layout;
use parking_lot::Mutex;
use rand::rngs::StdRng;
use rand::{Rng, SeedableRng};
use sha2::{Digest, Sha256};
use std::collections::{HashMap, VecDeque};
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicU64, AtomicUsize, Ordering};
use std::time::{Duration, Instant};
use vlpds::car;
use vlpds::cbor::{write_map_head, write_text};
use vlpds::cid::Cid;
use vlpds::crypto::Keypair;
use vlpds::events::{self, Frame, RepoOp};
use vlpds::mst::{Entry, Node, Tree};
use vlpds::tid::{self, Tid};

/// `now_rfc3339()` is always this long (micros, `Z`), which lets the emitter
/// overwrite a pre-built frame's `time` in place.
pub const TIME_LEN: usize = 27;

#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub enum Label {
    Commit,
    Sync,
    Identity,
    Account,
    /// An account's first commit (spam bursts).
    Create,
    BadSig,
    /// The first commit after one that was never emitted: its `prevData`
    /// and `since` don't match what a consumer holds.
    AfterGap,
    /// Signed by the right key, but for an account whose DID document names
    /// another host.
    Foreign,
}

impl Label {
    pub fn name(self) -> &'static str {
        match self {
            Label::Commit => "commit",
            Label::Sync => "sync",
            Label::Identity => "identity",
            Label::Account => "account",
            Label::Create => "create",
            Label::BadSig => "badsig",
            Label::AfterGap => "aftergap",
            Label::Foreign => "foreign",
        }
    }
}

pub struct Pending {
    pub frame: Frame,
    /// Where the 27 `time` bytes sit in `frame.suffix`.
    pub time_off: usize,
    pub label: Label,
}

impl Pending {
    pub fn finish(&self, seq: i64, now: &str, out: &mut Vec<u8>) {
        let f = &self.frame;
        out.reserve(f.len_hint());
        out.extend_from_slice(&f.prefix);
        write_text(out, "seq");
        vlpds::cbor::write_int(out, seq);
        out.extend_from_slice(&f.suffix[..self.time_off]);
        out.extend_from_slice(now.as_bytes());
        out.extend_from_slice(&f.suffix[self.time_off + TIME_LEN..]);
    }
}

#[derive(Clone, Debug)]
pub struct SizeMix {
    pub p50: f64,
    pub p99: f64,
    pub max: f64,
    /// Share of commits that are big multi-op ones, `max..big_max` bytes.
    pub big_share: f64,
    pub big_max: f64,
}

impl Default for SizeMix {
    fn default() -> Self {
        // docs/reference-notes.md: #commit p50 5.2 KB, p99 9.9 KB, mean ~5.3 KB
        SizeMix { p50: 5200.0, p99: 9600.0, max: 16000.0, big_share: 0.0005, big_max: 200_000.0 }
    }
}

impl SizeMix {
    /// (target frame bytes, op count)
    fn sample(&self, rng: &mut StdRng) -> (usize, usize) {
        if rng.r#gen::<f64>() < self.big_share {
            let size = (self.max.ln() + rng.r#gen::<f64>() * (self.big_max / self.max).ln()).exp();
            return (size as usize, rng.gen_range(10..=50));
        }
        let sigma = (self.p99 / self.p50).ln() / 2.326;
        let z = normal(rng);
        let size = (self.p50 * (sigma * z).exp()).clamp(300.0, self.max);
        let r: f64 = rng.r#gen();
        let nops = if r < 0.92 {
            1
        } else if r < 0.97 {
            2
        } else if r < 0.99 {
            rng.gen_range(3..=5)
        } else {
            rng.gen_range(6..=10)
        };
        (size as usize, nops)
    }
}

fn normal(rng: &mut StdRng) -> f64 {
    let u1: f64 = rng.r#gen::<f64>().max(1e-12);
    let u2: f64 = rng.r#gen();
    (-2.0 * u1.ln()).sqrt() * (std::f64::consts::TAU * u2).cos()
}

#[derive(Clone, Debug)]
pub struct KindMix {
    pub identity: f64,
    pub account: f64,
    pub sync: f64,
}

impl Default for KindMix {
    fn default() -> Self {
        KindMix { identity: 0.002, account: 0.002, sync: 0.001 }
    }
}

#[derive(Clone, Debug, Default)]
pub struct Spam {
    /// New accounts per second while a burst lasts.
    pub rate: f64,
    pub secs: f64,
    pub every: f64,
    /// First burst starts this long after emission starts.
    pub delay: f64,
}

impl Spam {
    /// Accounts that should exist `t` seconds in.
    pub fn due(&self, t: f64) -> u64 {
        let t = t - self.delay;
        if t < 0.0 || self.every <= 0.0 {
            return 0;
        }
        let c = (t / self.every).floor();
        let p = t - c * self.every;
        (self.rate * (c * self.secs + p.min(self.secs))) as u64
    }
}

/// Faults for one host. Generation-side ones live here, the stream-side
/// ones (stall, disconnect, replay) are read by the emitter and server.
#[derive(Clone, Debug, Default)]
pub struct HostFaults {
    pub badsig: f64,
    pub gap: f64,
    /// Commits after a gap before the account emits `#sync` (0: never).
    pub gap_heal: u32,
    pub foreign: f64,
    pub spam: Option<Spam>,
    /// (seconds stalled, every seconds)
    pub stall: Option<(f64, f64)>,
    /// (every seconds, seconds refusing connections)
    pub disconnect: Option<(f64, f64)>,
    /// (every seconds, frames re-sent)
    pub replay: Option<(f64, usize)>,
    /// Every seconds: the host's sequence starts over at 1 (a restored or
    /// reset PDS), so a cursor from before is in its future.
    pub restart: Option<f64>,
}

/// `kind:hosts[:k=v,...]`, hosts as `all` or `0,3-5` (global indexes).
pub fn parse_fault(spec: &str, host_base: u32, hosts: u32, out: &mut [HostFaults]) -> anyhow::Result<()> {
    let mut parts = spec.splitn(3, ':');
    let kind = parts.next().unwrap_or("");
    let which = parts.next().ok_or_else(|| anyhow::anyhow!("fault {spec}: missing hosts"))?;
    let mut kv = std::collections::HashMap::new();
    for p in parts.next().unwrap_or("").split(',').filter(|s| !s.is_empty()) {
        let (k, v) = p.split_once('=').ok_or_else(|| anyhow::anyhow!("fault {spec}: bad param {p}"))?;
        kv.insert(k.to_string(), v.parse::<f64>()?);
    }
    let get = |k: &str, d: f64| kv.get(k).copied().unwrap_or(d);
    let mut sel = Vec::new();
    if which == "all" {
        sel.extend(host_base..host_base + hosts);
    } else {
        for r in which.split(',') {
            match r.split_once('-') {
                Some((a, b)) => sel.extend(a.parse::<u32>()?..=b.parse::<u32>()?),
                None => sel.push(r.parse()?),
            }
        }
    }
    for g in sel {
        let Some(f) = g.checked_sub(host_base).and_then(|l| out.get_mut(l as usize)) else {
            continue;
        };
        match kind {
            "badsig" => f.badsig = get("rate", 0.01),
            "gap" => {
                f.gap = get("rate", 0.01);
                f.gap_heal = get("heal", 3.0) as u32;
            }
            "foreign" => f.foreign = get("rate", 0.01),
            "spam" => {
                f.spam = Some(Spam {
                    rate: get("rate", 100.0),
                    secs: get("secs", 10.0),
                    every: get("every", 60.0),
                    delay: get("delay", 5.0),
                })
            }
            "stall" => f.stall = Some((get("secs", 10.0), get("every", 60.0))),
            "disconnect" => f.disconnect = Some((get("every", 30.0), get("down", 5.0))),
            "replay" => f.replay = Some((get("every", 20.0), get("count", 100.0) as usize)),
            "restart" => f.restart = Some(get("every", 60.0)),
            _ => anyhow::bail!(
                "unknown fault kind {kind} (badsig, gap, foreign, spam, stall, disconnect, replay, restart)"
            ),
        }
    }
    Ok(())
}

pub struct GenCfg {
    pub layout: Layout,
    pub host_base: u32,
    pub hosts: u32,
    pub dids: u32,
    pub threads: usize,
    pub initial_records: usize,
    pub target_records: usize,
    pub size: SizeMix,
    pub mix: KindMix,
    pub faults: Vec<HostFaults>,
    /// Relative event rate per local host.
    pub weights: Vec<f64>,
    /// Per local host: where the generators publish each account's repo.
    pub repos: Vec<Arc<Repos>>,
}

/// A queue of pre-built events for one host, filled by every generator
/// thread and drained by that host's emitter.
pub struct HostQueue {
    pub q: Mutex<VecDeque<Pending>>,
    pub bytes: AtomicUsize,
    pub cap: AtomicUsize,
}

impl HostQueue {
    pub fn new(cap: usize) -> HostQueue {
        HostQueue { q: Mutex::new(VecDeque::new()), bytes: AtomicUsize::new(0), cap: AtomicUsize::new(cap) }
    }

    pub fn pop(&self, n: usize, out: &mut Vec<Pending>) {
        let mut q = self.q.lock();
        let n = n.min(q.len());
        let mut b = 0;
        for p in q.drain(..n) {
            b += p.frame.len_hint();
            out.push(p);
        }
        self.bytes.fetch_sub(b, Ordering::Relaxed);
    }

    fn push(&self, items: &mut Vec<Pending>) {
        let b: usize = items.iter().map(|p| p.frame.len_hint()).sum();
        self.q.lock().extend(items.drain(..));
        self.bytes.fetch_add(b, Ordering::Relaxed);
    }

    fn full(&self) -> bool {
        self.bytes.load(Ordering::Relaxed) >= self.cap.load(Ordering::Relaxed)
    }
}

#[derive(Default)]
pub struct GenStats {
    pub events: AtomicU64,
    pub bytes: AtomicU64,
    pub busy_ns: AtomicU64,
    pub ready: AtomicUsize,
}

/// A record's bytes are a pure function of these fields, so nobody keeps
/// them: the generator builds them once for the commit, and getRepo
/// rebuilds them from here.
#[derive(Clone, Copy, Debug)]
pub struct Rec {
    pub seed: u64,
    /// `Tid(0)` for the profile, whose rkey is `self`.
    pub rkey: Tid,
    pub cid: Cid,
    /// Target encoded size.
    pub size: u32,
    /// `createdAt` in unix seconds. The micros come from the seed.
    pub at: u32,
    /// Index into [`COLLECTIONS`], or [`PROFILE`].
    pub coll: u8,
}

const PROFILE: u8 = COLLECTIONS.len() as u8;

fn coll_name(c: u8) -> &'static str {
    COLLECTIONS.get(c as usize).map_or("app.bsky.actor.profile", |x| x.0)
}

fn rec_path(coll: u8, rkey: Tid) -> String {
    if coll == PROFILE { format!("{}/self", coll_name(coll)) } else { format!("{}/{rkey}", coll_name(coll)) }
}

impl Rec {
    fn build(coll: u8, rkey: Tid, seed: u64, size: usize, at: u32) -> (Rec, Vec<u8>) {
        let cid = Cid { codec: vlpds::cid::CODEC_DAG_CBOR, digest: [0; 32] };
        let mut r = Rec { seed, rkey, cid, size: size as u32, at, coll };
        let mut bytes = Vec::with_capacity(size + 16);
        r.write(&mut bytes);
        r.cid = Cid::dag_cbor(&bytes);
        (r, bytes)
    }

    pub fn path(&self) -> String {
        rec_path(self.coll, self.rkey)
    }

    pub fn write(&self, out: &mut Vec<u8>) {
        record(coll_name(self.coll), self.size as usize, self.seed, self.at, out)
    }
}

const GOLDEN: u64 = 0x9e37_79b9_7f4a_7c15;

/// The splitmix64 finalizer.
fn mix(mut z: u64) -> u64 {
    z = (z ^ (z >> 30)).wrapping_mul(0xbf58_476d_1ce4_e5b9);
    z = (z ^ (z >> 27)).wrapping_mul(0x94d0_49bb_1331_11eb);
    z ^ (z >> 31)
}

/// An account's records: the tree, what each leaf's bytes are made from,
/// and the counter that seeds the next record.
#[derive(Clone)]
struct Repo {
    tree: Tree,
    /// Shared with the published snapshot until the next write copies it.
    recs: Arc<Vec<Rec>>,
    last_rkey: Tid,
    writes: u64,
    base: u64,
}

impl Repo {
    fn new(did: &str, last_rkey: Tid, cap: usize) -> Repo {
        let h = Sha256::digest(format!("fakepds records\0{did}"));
        let base = u64::from_le_bytes(h[..8].try_into().expect("8 bytes"));
        Repo { tree: Tree::new(), recs: Arc::new(Vec::with_capacity(cap)), last_rkey, writes: 0, base }
    }
}

fn next_seed(base: u64, writes: &mut u64) -> u64 {
    *writes += 1;
    mix(base.wrapping_add(writes.wrapping_mul(GOLDEN)))
}

/// The generator's own record list, copied first if a snapshot still holds
/// it, with room for `extra` creates.
fn recs_mut(r: &mut Arc<Vec<Rec>>, extra: usize) -> &mut Vec<Rec> {
    if Arc::get_mut(r).is_none() {
        let mut v = Vec::with_capacity(r.len() + extra);
        v.extend_from_slice(r);
        *r = Arc::new(v);
    }
    Arc::get_mut(r).expect("unique")
}

/// An account's repo as of its last applied commit (emitted or not), for
/// getRepo and the head endpoints. The tree shares its nodes with the
/// generator's, so taking one costs a few refcounts, and the next commit
/// copies only the nodes on its paths.
pub struct Snapshot {
    pub commit: Cid,
    pub signed: Vec<u8>,
    pub rev: Tid,
    pub tree: Tree,
    pub recs: Arc<Vec<Rec>>,
}

impl Snapshot {
    /// The whole repo as a CAR in vlpds's streamable order
    /// (`vlpds::car_order`): the commit, then the MST in preorder, each
    /// record right after its entry. Record bytes are rebuilt here.
    pub fn car(&self) -> anyhow::Result<Vec<u8>> {
        let by_cid: HashMap<Cid, &Rec> = self.recs.iter().map(|r| (r.cid, r)).collect();
        let est = 256 + self.signed.len() + self.recs.iter().map(|r| r.size as usize + 160).sum::<usize>();
        let mut out = Vec::with_capacity(est);
        car::write_header(&mut out, &self.commit);
        car::write_block(&mut out, &self.commit, &self.signed);
        let mut buf = Vec::with_capacity(4096);
        write_node(&self.tree.root, &by_cid, &mut buf, &mut out, 0)?;
        Ok(out)
    }
}

fn write_node(
    n: &Node,
    recs: &HashMap<Cid, &Rec>,
    buf: &mut Vec<u8>,
    out: &mut Vec<u8>,
    depth: usize,
) -> anyhow::Result<()> {
    anyhow::ensure!(depth <= vlpds::mst::MAX_DEPTH, "tree too deep");
    let c = n.cid.ok_or_else(|| anyhow::anyhow!("unwritten MST node"))?;
    match &n.bytes {
        Some(b) if !n.dirty => car::write_block(out, &c, b),
        _ => {
            buf.clear();
            vlpds::mst::encode_node(n, buf)?;
            car::write_block(out, &c, buf);
        }
    }
    for e in &n.entries {
        match e {
            Entry::Value { val, .. } => {
                let r = recs.get(val).ok_or_else(|| anyhow::anyhow!("no record state for {val}"))?;
                buf.clear();
                r.write(buf);
                car::write_block(out, val, buf);
            }
            Entry::Child { node: Some(c), .. } => write_node(c, recs, buf, out, depth + 1)?,
            Entry::Child { node: None, .. } => anyhow::bail!("partial tree"),
        }
    }
    Ok(())
}

/// One host's latest snapshot per account index, written by the generator
/// threads and read by the host's xrpc handlers.
#[derive(Default)]
pub struct Repos {
    map: Mutex<HashMap<u32, Arc<Snapshot>>>,
}

impl Repos {
    pub fn get(&self, i: u32) -> Option<Arc<Snapshot>> {
        self.map.lock().get(&i).cloned()
    }

    /// (account, commit, rev) for every account with a commit.
    pub fn heads(&self) -> Vec<(u32, Cid, Tid)> {
        self.map.lock().iter().map(|(i, s)| (*i, s.commit, s.rev)).collect()
    }

    fn put(&self, i: u32, s: Snapshot) {
        let old = self.map.lock().insert(i, Arc::new(s));
        // the old tree's freed nodes are dropped outside the lock
        drop(old);
    }
}

struct Did {
    i: u32,
    did: String,
    key: Keypair,
    clock: u64,
    repo: Repo,
    rev: Tid,
    data: Cid,
    mst_ewma: f64,
    /// Commits left before this account emits `#sync` to heal a gap.
    heal: Option<u32>,
    gapped: bool,
}

struct HostGen {
    g: u32,
    dids: Vec<Did>,
    batch: usize,
    /// Spam accounts this thread has created (its own k = t mod threads).
    spam_made: u64,
}

pub struct Gen {
    cfg: Arc<GenCfg>,
    t: usize,
    hosts: Vec<HostGen>,
    rng: StdRng,
    empty_root: Cid,
}

const COLLECTIONS: &[(&str, f64)] = &[
    ("app.bsky.feed.like", 0.40),
    ("app.bsky.feed.post", 0.35),
    ("app.bsky.feed.repost", 0.10),
    ("app.bsky.graph.follow", 0.10),
    ("app.bsky.graph.block", 0.03),
    ("app.bsky.feed.threadgate", 0.02),
];

const VOCAB: &[&str] = &[
    "the",
    "a",
    "relay",
    "firehose",
    "repo",
    "commit",
    "post",
    "and",
    "of",
    "to",
    "in",
    "is",
    "that",
    "it",
    "for",
    "on",
    "with",
    "as",
    "this",
    "was",
    "at",
    "by",
    "be",
    "have",
    "from",
    "or",
    "one",
    "had",
    "not",
    "but",
    "what",
    "all",
    "were",
    "when",
    "we",
    "there",
    "can",
    "an",
    "your",
    "which",
    "their",
    "said",
    "if",
    "do",
    "will",
    "each",
    "about",
    "how",
    "up",
    "out",
    "them",
    "then",
    "she",
    "many",
    "some",
    "so",
    "these",
    "would",
    "other",
    "into",
    "has",
    "more",
    "her",
    "two",
    "like",
    "him",
    "see",
    "time",
    "could",
    "no",
    "make",
    "than",
    "first",
    "been",
    "its",
    "who",
    "now",
    "people",
    "my",
    "made",
    "over",
    "did",
    "down",
    "only",
    "way",
    "find",
    "use",
    "may",
    "water",
    "long",
    "little",
    "very",
    "after",
    "words",
    "called",
    "just",
    "where",
    "most",
    "know",
    "skyline",
    "mountain",
    "coffee",
    "morning",
    "garden",
    "bicycle",
    "atproto",
    "bluesky",
    "signature",
    "merkle",
    "tree",
];

fn pick_collection(rng: &mut StdRng) -> u8 {
    let mut r: f64 = rng.r#gen();
    for (i, (_, w)) in COLLECTIONS.iter().enumerate() {
        if r < *w {
            return i as u8;
        }
        r -= w;
    }
    0
}

fn words(n: usize, mut x: u64) -> String {
    let mut s = String::with_capacity(n + 48);
    while s.len() < n {
        x = x.wrapping_add(GOLDEN);
        let mut r = mix(x);
        for _ in 0..4 {
            s.push_str(VOCAB[((r & 0xffff) as usize * VOCAB.len()) >> 16]);
            s.push(' ');
            r >>= 16;
        }
    }
    s.truncate(n);
    s
}

fn record(coll: &str, size: usize, seed: u64, at: u32, out: &mut Vec<u8>) {
    let micros = at as i64 * 1_000_000 + (mix(seed ^ GOLDEN) % 1_000_000) as i64;
    let created = chrono::DateTime::from_timestamp_micros(micros)
        .map(|t| t.to_rfc3339_opts(chrono::SecondsFormat::Micros, true))
        .unwrap_or_default();
    let text = words(size.saturating_sub(48 + coll.len()).max(1), seed);
    // DAG-CBOR key order: length first, then bytes
    write_map_head(out, 3);
    write_text(out, "text");
    write_text(out, &text);
    write_text(out, "$type");
    write_text(out, coll);
    write_text(out, "createdAt");
    write_text(out, &created);
}

fn text_len(n: usize) -> usize {
    n + match n {
        0..=23 => 1,
        24..=255 => 2,
        256..=65535 => 3,
        _ => 5,
    }
}

/// `time` offset in a `#commit` suffix: repo, then time.
fn commit_time_off(did: &str) -> usize {
    text_len(4) + text_len(did.len()) + text_len(4) + 2
}

/// `#sync`, `#identity`, `#account` suffixes start with time.
const TIME_FIRST_OFF: usize = 5 + 2;

fn sign(did: &str, rev: &str, data: &Cid, key: &Keypair, corrupt: bool) -> (Cid, Vec<u8>) {
    let unsigned = events::encode_commit(did, rev, data, None);
    let mut sig = key.sign(&unsigned);
    if corrupt {
        sig[17] ^= 0x01;
    }
    let signed = events::encode_commit(did, rev, data, Some(&sig));
    (Cid::dag_cbor(&signed), signed)
}

enum Plan {
    Create { coll: u8, size: usize },
    Update { size: usize },
    Delete,
}

struct Built {
    frame: Frame,
    time_off: usize,
    commit: Cid,
    signed: Vec<u8>,
    rev: Tid,
    data: Cid,
    mst_bytes: usize,
}

#[allow(clippy::too_many_arguments)]
fn build_commit(
    did: &str,
    key: &Keypair,
    clock: u64,
    repo: &mut Repo,
    since: Option<Tid>,
    prev_data: Option<Cid>,
    plan: &[Plan],
    corrupt: bool,
    rng: &mut StdRng,
) -> anyhow::Result<Built> {
    struct Op {
        action: &'static str,
        path: String,
        cid: Option<Cid>,
        prev: Option<Cid>,
    }
    let mut ops: Vec<Op> = Vec::with_capacity(plan.len());
    let mut records: Vec<(Cid, Vec<u8>)> = Vec::new();
    let touched = |ops: &[Op], p: &str| ops.iter().any(|o| o.path == p);
    let at = (tid::now_micros() / 1_000_000) as u32;
    let recs = recs_mut(&mut repo.recs, plan.len());
    for p in plan {
        match p {
            Plan::Create { coll, size } => {
                repo.last_rkey = tid::next_rev(Some(repo.last_rkey), clock);
                let rkey = if *coll == PROFILE { Tid(0) } else { repo.last_rkey };
                let path = rec_path(*coll, rkey);
                if touched(&ops, &path) {
                    continue;
                }
                let (rec, bytes) = Rec::build(*coll, rkey, next_seed(repo.base, &mut repo.writes), *size, at);
                let prev = repo.tree.insert(path.as_bytes(), rec.cid)?;
                match recs.iter_mut().find(|r| r.coll == rec.coll && r.rkey == rec.rkey) {
                    Some(r) => *r = rec,
                    None => recs.push(rec),
                }
                ops.push(Op {
                    action: if prev.is_some() { "update" } else { "create" },
                    path,
                    cid: Some(rec.cid),
                    prev,
                });
                records.push((rec.cid, bytes));
            }
            Plan::Update { size } => {
                if recs.is_empty() {
                    continue;
                }
                let idx = rng.gen_range(0..recs.len());
                let old = recs[idx];
                let path = old.path();
                if touched(&ops, &path) {
                    continue;
                }
                let (rec, bytes) = Rec::build(old.coll, old.rkey, next_seed(repo.base, &mut repo.writes), *size, at);
                repo.tree.insert(path.as_bytes(), rec.cid)?;
                recs[idx] = rec;
                ops.push(Op { action: "update", path, cid: Some(rec.cid), prev: Some(old.cid) });
                records.push((rec.cid, bytes));
            }
            Plan::Delete => {
                if recs.is_empty() {
                    continue;
                }
                let idx = rng.gen_range(0..recs.len());
                let path = recs[idx].path();
                if touched(&ops, &path) {
                    continue;
                }
                let old = recs.swap_remove(idx);
                repo.tree.remove(path.as_bytes())?;
                ops.push(Op { action: "delete", path, cid: None, prev: Some(old.cid) });
            }
        }
    }
    if ops.is_empty() {
        return build_commit(
            did,
            key,
            clock,
            repo,
            since,
            prev_data,
            &[Plan::Create { coll: 0, size: 300 }],
            corrupt,
            rng,
        );
    }
    let tree = &mut repo.tree;
    let mut mst_blocks = Vec::with_capacity(8);
    let data = tree.write_diff_blocks(&mut mst_blocks)?;
    // a commit that nets to no change still has to carry its root node
    if !mst_blocks.iter().any(|(c, _)| *c == data) {
        mst_blocks.push(tree.root_block()?);
    }
    let rev = tid::next_rev(since, clock);
    let rev_s = rev.to_string();
    let (commit, signed) = sign(did, &rev_s, &data, key, corrupt);
    let mst_bytes: usize = mst_blocks.iter().map(|(_, b)| b.len() + 40).sum();
    let mut car_bytes =
        Vec::with_capacity(128 + signed.len() + mst_bytes + records.iter().map(|(_, b)| b.len() + 40).sum::<usize>());
    car::write_header(&mut car_bytes, &commit);
    car::write_block(&mut car_bytes, &commit, &signed);
    for (c, b) in &mst_blocks {
        car::write_block(&mut car_bytes, c, b);
    }
    for (c, b) in &records {
        car::write_block(&mut car_bytes, c, b);
    }
    let since_s = since.map(|t| t.to_string());
    let time = events::now_rfc3339();
    let repo_ops: Vec<RepoOp> =
        ops.iter().map(|o| RepoOp { action: o.action, path: &o.path, cid: o.cid, prev: o.prev }).collect();
    let frame = events::commit_frame(&events::CommitFrame {
        repo: did,
        rev: &rev_s,
        since: since_s.as_deref(),
        commit,
        prev_data,
        blocks: &car_bytes,
        ops: &repo_ops,
        time: &time,
    });
    let time_off = commit_time_off(did);
    debug_assert_eq!(&frame.suffix[time_off..time_off + TIME_LEN], time.as_bytes());
    Ok(Built { frame, time_off, commit, signed, rev, data, mst_bytes })
}

fn snapshot(b: &Built, repo: &Repo) -> Snapshot {
    Snapshot {
        commit: b.commit,
        signed: b.signed.clone(),
        rev: b.rev,
        tree: repo.tree.clone(),
        recs: repo.recs.clone(),
    }
}

impl Gen {
    /// Builds this thread's accounts: initial records and a signed head each.
    pub fn new(cfg: Arc<GenCfg>, t: usize) -> anyhow::Result<Gen> {
        let mut rng = StdRng::from_entropy();
        let empty_root = Tree::new().root_cid()?;
        let max_w = cfg.weights.iter().cloned().fold(f64::MIN, f64::max).max(1e-9);
        let mut hosts = Vec::with_capacity(cfg.hosts as usize);
        for l in 0..cfg.hosts {
            let g = cfg.host_base + l;
            let mut dids = Vec::new();
            for i in (t as u32..cfg.dids).step_by(cfg.threads) {
                let did = cfg.layout.did(g, i);
                let key = cfg.layout.key(g, i);
                let clock = rng.gen_range(0..1024);
                let start = Tid::from_parts(tid::now_micros() - 86_400_000_000, clock);
                let mut repo = Repo::new(&did, start, cfg.initial_records);
                let at = (start.micros() / 1_000_000) as u32;
                let recs = Arc::get_mut(&mut repo.recs).expect("unique");
                for _ in 0..cfg.initial_records {
                    repo.last_rkey = tid::next_rev(Some(repo.last_rkey), clock);
                    let coll = pick_collection(&mut rng);
                    let seed = next_seed(repo.base, &mut repo.writes);
                    let (rec, _) = Rec::build(coll, repo.last_rkey, seed, rng.gen_range(150..600), at);
                    repo.tree.insert_no_proof(rec.path().as_bytes(), rec.cid)?;
                    recs.push(rec);
                }
                let data = repo.tree.root_cid()?;
                let rev = tid::next_rev(None, clock);
                dids.push(Did { i, did, key, clock, repo, rev, data, mst_ewma: 400.0, heal: None, gapped: false });
            }
            let batch = ((8.0 * cfg.weights[l as usize] / max_w).round() as usize).max(1);
            hosts.push(HostGen { g, dids, batch, spam_made: 0 });
        }
        Ok(Gen { cfg, t, hosts, rng, empty_root })
    }

    pub fn run(
        mut self,
        queues: Arc<Vec<Arc<HostQueue>>>,
        started: Arc<parking_lot::RwLock<Option<Instant>>>,
        stop: Arc<AtomicBool>,
        stats: Arc<GenStats>,
    ) {
        let mut buf = Vec::with_capacity(64);
        while !stop.load(Ordering::Relaxed) {
            let mut worked = false;
            let elapsed = started.read().map(|s| s.elapsed().as_secs_f64());
            for h in 0..self.hosts.len() {
                let q = &queues[h];
                if q.full() {
                    continue;
                }
                let t0 = Instant::now();
                if let Some(e) = elapsed {
                    self.spam(h, e, &mut buf);
                }
                for _ in 0..self.hosts[h].batch {
                    if let Err(e) = self.next(h, &mut buf) {
                        tracing::error!(error = %e, "generate");
                    }
                }
                stats.events.fetch_add(buf.len() as u64, Ordering::Relaxed);
                stats.bytes.fetch_add(buf.iter().map(|p| p.frame.len_hint() as u64).sum(), Ordering::Relaxed);
                stats.busy_ns.fetch_add(t0.elapsed().as_nanos() as u64, Ordering::Relaxed);
                q.push(&mut buf);
                worked = true;
            }
            if !worked {
                std::thread::sleep(Duration::from_millis(2));
            }
        }
    }

    /// Spam accounts due by `elapsed` seconds: each is `#identity`,
    /// `#account` and a first commit.
    pub fn spam(&mut self, h: usize, elapsed: f64, out: &mut Vec<Pending>) {
        let Some(sp) = &self.cfg.faults[h].spam else {
            return;
        };
        let due = sp.due(elapsed);
        let threads = self.cfg.threads as u64;
        loop {
            let k = self.hosts[h].spam_made * threads + self.t as u64;
            if k >= due {
                break;
            }
            self.hosts[h].spam_made += 1;
            let g = self.hosts[h].g;
            let i = self.cfg.dids + k as u32;
            let did = self.cfg.layout.did(g, i);
            let key = self.cfg.layout.key(g, i);
            let time = events::now_rfc3339();
            out.push(Pending {
                frame: events::identity_frame(&did, &self.cfg.layout.handle(g, i), &time),
                time_off: TIME_FIRST_OFF,
                label: Label::Identity,
            });
            out.push(Pending {
                frame: events::account_frame(&did, true, None, &time),
                time_off: TIME_FIRST_OFF,
                label: Label::Account,
            });
            let clock = self.rng.gen_range(0..1024);
            let mut repo = Repo::new(&did, Tid::from_parts(tid::now_micros(), clock), 1);
            let plan = [Plan::Create { coll: PROFILE, size: 400 }];
            match build_commit(&did, &key, clock, &mut repo, None, Some(self.empty_root), &plan, false, &mut self.rng) {
                Ok(b) => {
                    self.cfg.repos[h].put(i, snapshot(&b, &repo));
                    out.push(Pending { frame: b.frame, time_off: b.time_off, label: Label::Create })
                }
                Err(e) => tracing::error!(error = %e, "spam commit"),
            }
        }
    }

    /// One event for local host `h` (none when a gap swallows a commit).
    pub fn next(&mut self, h: usize, out: &mut Vec<Pending>) -> anyhow::Result<()> {
        let n = self.hosts[h].dids.len();
        if n == 0 {
            return Ok(());
        }
        let di = self.rng.gen_range(0..n);
        let faults = &self.cfg.faults[h];
        let mix = &self.cfg.mix;
        let r: f64 = self.rng.r#gen();
        let g = self.hosts[h].g;
        let d = &mut self.hosts[h].dids[di];
        let time = events::now_rfc3339();

        if d.heal == Some(0) || r < mix.sync {
            d.heal = None;
            d.gapped = false;
            let rev = tid::next_rev(Some(d.rev), d.clock);
            let rev_s = rev.to_string();
            let (commit, signed) = sign(&d.did, &rev_s, &d.data, &d.key, false);
            let mut blocks = Vec::with_capacity(signed.len() + 100);
            car::write_header(&mut blocks, &commit);
            car::write_block(&mut blocks, &commit, &signed);
            d.rev = rev;
            self.cfg.repos[h]
                .put(d.i, Snapshot { commit, signed, rev, tree: d.repo.tree.clone(), recs: d.repo.recs.clone() });
            out.push(Pending {
                frame: events::sync_frame(&d.did, &rev_s, &blocks, &time),
                time_off: TIME_FIRST_OFF,
                label: Label::Sync,
            });
            return Ok(());
        }
        if r < mix.sync + mix.identity {
            out.push(Pending {
                frame: events::identity_frame(&d.did, &self.cfg.layout.handle(g, d.i), &time),
                time_off: TIME_FIRST_OFF,
                label: Label::Identity,
            });
            return Ok(());
        }
        if r < mix.sync + mix.identity + mix.account {
            out.push(Pending {
                frame: events::account_frame(&d.did, true, None, &time),
                time_off: TIME_FIRST_OFF,
                label: Label::Account,
            });
            return Ok(());
        }

        let fr: f64 = self.rng.r#gen();
        if fr < faults.foreign {
            // an account of the next host, which this one doesn't own
            let (fg, fi) = (g + 1, self.rng.gen_range(0..self.cfg.dids));
            let did = self.cfg.layout.did(fg, fi);
            let key = self.cfg.layout.key(fg, fi);
            let mut repo = Repo::new(&did, Tid::from_parts(tid::now_micros(), 0), 1);
            let plan = [Plan::Create { coll: 1, size: 2000 }];
            let b = build_commit(&did, &key, 0, &mut repo, None, Some(self.empty_root), &plan, false, &mut self.rng)?;
            out.push(Pending { frame: b.frame, time_off: b.time_off, label: Label::Foreign });
            return Ok(());
        }

        let (size, nops) = self.cfg.size.sample(&mut self.rng);
        let nrec = d.repo.recs.len();
        let target = self.cfg.target_records;
        let mut plan = Vec::with_capacity(nops);
        let mut puts = 0usize;
        for _ in 0..nops {
            let x: f64 = self.rng.r#gen();
            let (pc, pu) = if nrec < 4 {
                (1.0, 0.0)
            } else if nrec >= target {
                (0.45, 0.05)
            } else {
                (0.85, 0.05)
            };
            if x < pc {
                plan.push(Plan::Create { coll: pick_collection(&mut self.rng), size: 0 });
                puts += 1;
            } else if x < pc + pu {
                plan.push(Plan::Update { size: 0 });
                puts += 1;
            } else {
                plan.push(Plan::Delete);
            }
        }
        // the record carries whatever the commit, CAR and MST don't
        let per = if puts == 0 {
            0
        } else {
            ((size as f64 - 330.0 - d.mst_ewma - 60.0 * nops as f64) / puts as f64).max(80.0) as usize
        };
        for p in plan.iter_mut() {
            match p {
                Plan::Create { size, .. } | Plan::Update { size } => *size = per,
                Plan::Delete => {}
            }
        }

        if fr < faults.foreign + faults.badsig {
            // built on a copy: the account's chain doesn't move
            let mut repo = d.repo.clone();
            let b = build_commit(
                &d.did,
                &d.key,
                d.clock,
                &mut repo,
                Some(d.rev),
                Some(d.data),
                &plan,
                true,
                &mut self.rng,
            )?;
            out.push(Pending { frame: b.frame, time_off: b.time_off, label: Label::BadSig });
            return Ok(());
        }

        let b =
            build_commit(&d.did, &d.key, d.clock, &mut d.repo, Some(d.rev), Some(d.data), &plan, false, &mut self.rng)?;
        d.rev = b.rev;
        d.data = b.data;
        d.mst_ewma = 0.9 * d.mst_ewma + 0.1 * b.mst_bytes as f64;
        self.cfg.repos[h].put(d.i, snapshot(&b, &d.repo));
        if fr < faults.foreign + faults.badsig + faults.gap && d.heal.is_none() {
            // never emitted: the next commit's prevData/since skip it
            d.gapped = true;
            if faults.gap_heal > 0 {
                d.heal = Some(faults.gap_heal);
            }
            return Ok(());
        }
        let label = if std::mem::take(&mut d.gapped) { Label::AfterGap } else { Label::Commit };
        if let Some(h) = d.heal.as_mut() {
            *h = h.saturating_sub(1);
        }
        out.push(Pending { frame: b.frame, time_off: b.time_off, label });
        Ok(())
    }
}
