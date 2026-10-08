//! The relay on the quorum log (docs/quorum.md, "The relay on the log").
//!
//! Every node runs the same pipeline (upstream sockets, verify) for the
//! hosts it owns, and one quorum log (`crate::qlog`) holds the firehose:
//!
//! - Host owner to leader: the lane's [`DidOwner`] is [`QuorumOwner`], which
//!   hands each checked event to the leader with `Client::submit_events`,
//!   one batch in flight per slot (a DID always maps to one slot, so its
//!   events reach the leader in order), and answers the lane with the
//!   leader's outcome once it has committed.
//! - The leader decides (`Hooks::admit`): the relay's own state apply
//!   (`StateStore::apply`: host authority, account status, `check_chain`)
//!   against the DID records as of everything this term has appended. An
//!   accepted event is appended with its new record in the entry's meta;
//!   duplicates and rejections answer without an entry. Nothing is
//!   emitted before its entry commits.
//! - The state: the quorum log's applier (`qlog::flush`) writes each
//!   committed entry's records to the one SlateDB the flush seals at F, so
//!   the state at F is exactly the log replayed to F. The leader's view of
//!   a DID is that database plus what this term has appended and not yet
//!   applied ([`Term`]): a `ShardState` over the same database whose
//!   tickets are released, never written, once the applier has them.
//! - Hosts: the leader keeps the host table (every host, its tier and the
//!   member that owns it), changes ride the next entry's meta into the
//!   state, and members read it from the leader every `host_poll`. Owners
//!   are the live members by rendezvous hash: a dead member's hosts move,
//!   nothing else does.
//! - Cursors: each node sends its owned hosts' acked cursors every second;
//!   they ride the log as in Phases 3-4, and a bucket recovery rewinds the
//!   hosts to the recovery's cursors before a node's cursors count again.
//! - Membership is `qlog/leader`'s.

use super::forward::{ForwardError, Outcome as FwdOutcome};
use super::{Checked, CheckedKind, DidOwner, Node, NodeConfig, Rejection, State, Submitted};
use crate::qlog::client::{Client, Decided};
use crate::qlog::log::{Entry, Meta, decode_cursors, encode_cursors};
use crate::qlog::node::{Admission, Hooks, Verdict};
use crate::qlog::wire::{Item, Outcome};
use crate::state::{self, Applied, EventKind, Incoming, Record, ShardState};
use crate::types::Host;
use crate::upstream::{self, HostFilter, Manager, UpstreamConfig};
use bytes::{Buf, BufMut, Bytes};
use futures::future::BoxFuture;
use parking_lot::{Mutex, RwLock};
use serde::{Deserialize, Serialize};
use std::collections::{BTreeMap, HashMap, HashSet, VecDeque};
use std::path::PathBuf;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{Duration, Instant};
use tokio::sync::{mpsc, oneshot, watch};
use vlsync_store::slots::ShardId;
use vlsync_store::store::Store;

/// What `main` gives a node on the quorum log.
#[derive(Clone)]
pub struct QuorumSetup {
    /// The peer protocol (replication, submits, asks).
    pub listen: String,
    /// Where to dial each other node.
    pub peers: HashMap<String, String>,
    /// The bootstrap member set (default: this node and its peers).
    pub members: Vec<String>,
    /// The commitlog's directory; None keeps the log in memory only.
    pub commitlog: Option<PathBuf>,
    pub flush: Duration,
    pub headroom: u64,
    pub flush_segment_bytes: usize,
    pub commitlog_segment_bytes: u64,
    pub disk_retain_bytes: u64,
    /// Committed log kept in memory (default: 64 MiB with a commitlog).
    pub memory_bytes: Option<usize>,
    pub auto_recover: bool,
    /// `--qlog-admin-token`: membership changes need it.
    pub admin_token: Option<String>,
    /// Bucket retention, run by the leader: delete segments older than
    /// this and state paths nothing reads. None: never.
    pub retain_horizon: Option<Duration>,
    pub retain_every: Duration,
    /// A member the leader hasn't heard from in this long loses its hosts.
    pub host_failover: Duration,
    /// How often members read the leader's host table.
    pub host_poll: Duration,
    /// How often a node sends its hosts' acked cursors.
    pub cursor_every: Duration,
    pub election_timeout: Duration,
    pub heartbeat: Duration,
    /// Chaos: cut the flush short at a step (`qlog::flush::Step`).
    pub crash: Option<crate::qlog::flush::CrashHook>,
    /// Chaos: SIGUSR1 loses part of the commitlog's unsynced tail, tears a
    /// record and kills the process, as a power cut would.
    pub power_cut_on_usr1: bool,
    /// Chaos: sleep this long before each commitlog fsync.
    pub fsync_delay: Option<Duration>,
    /// When an entry counts here (`--durability`).
    pub durability: crate::qlog::commitlog::DurabilityMode,
    /// Mutation tests only (`--qlog-unsafe-trust-log`).
    pub trust_after_power_loss: bool,
    /// The leader reads the PLC directory's export into the seed database
    /// (`plc_seed`), and every member seeds its DID documents from it.
    pub plc_export: Option<crate::plc_seed::ingest::Config>,
    /// With `plc_export`: this member's copy of the seeds on local disk
    /// (`plc_seed::local`).
    pub plc_seeds_dir: Option<std::path::PathBuf>,
}

impl QuorumSetup {
    pub fn new(listen: &str) -> QuorumSetup {
        QuorumSetup {
            listen: listen.to_string(),
            peers: HashMap::new(),
            members: Vec::new(),
            commitlog: None,
            flush: Duration::from_secs(30),
            headroom: 8_640_000,
            flush_segment_bytes: 64 << 20,
            commitlog_segment_bytes: 64 << 20,
            disk_retain_bytes: 4 << 30,
            memory_bytes: None,
            auto_recover: true,
            admin_token: None,
            retain_horizon: Some(Duration::from_secs(72 * 3600)),
            retain_every: Duration::from_secs(600),
            host_failover: Duration::from_secs(2),
            host_poll: Duration::from_millis(500),
            cursor_every: Duration::from_secs(1),
            election_timeout: Duration::from_millis(1000),
            heartbeat: Duration::from_millis(100),
            crash: None,
            power_cut_on_usr1: false,
            fsync_delay: None,
            durability: crate::qlog::commitlog::DurabilityMode::Sync(crate::qlog::commitlog::SyncMode::Fsync),
            trust_after_power_loss: false,
            plc_export: None,
            plc_seeds_dir: None,
        }
    }
}

// ---------------------------------------------------------------- codecs

const KIND_COMMIT: u8 = 0;
const KIND_SYNC: u8 = 1;
const KIND_IDENTITY: u8 = 2;
const KIND_ACCOUNT: u8 = 3;
/// An operator's relay takedown (or its lifting), made on the leader.
const KIND_TAKEDOWN: u8 = 4;
/// An operator lifting a relay throttle, made on the leader.
const KIND_RELEASE: u8 = 5;

/// Operator items, after the item's upstream seq: `0xff | op`.
const OP_UNTAKEDOWN: u8 = 0;
const OP_TAKEDOWN: u8 = 1;
const OP_RELEASE: u8 = 2;

fn put_str16(b: &mut Vec<u8>, s: &str) {
    let s = &s.as_bytes()[..s.len().min(u16::MAX as usize)];
    b.put_u16(s.len() as u16);
    b.put_slice(s);
}

fn get_str16(r: &mut Bytes) -> Option<String> {
    if r.remaining() < 2 {
        return None;
    }
    let n = r.get_u16() as usize;
    if r.remaining() < n {
        return None;
    }
    String::from_utf8(r.split_to(n).to_vec()).ok()
}

/// A submitted event as the leader reads it: `did | host | the node that
/// read it | upstream seq | the host owner's checks`
/// (`cluster::encode_meta`), or a takedown.
struct ItemMeta {
    did: String,
    host: Host,
    from: String,
    useq: i64,
    kind: ItemKind,
}

enum ItemKind {
    Event {
        kind: CheckedKind,
        first_sighting: bool,
    },
    Takedown(bool),
    /// Lift the relay throttle of an account `host` created; the status its
    /// `#account` announces.
    Release {
        active: bool,
        status: Option<String>,
    },
}

fn encode_item(did: &str, host: &Host, from: &str, useq: i64, rest: &[u8]) -> Bytes {
    let mut b = Vec::with_capacity(did.len() + host.0.len() + from.len() + 14 + rest.len());
    put_str16(&mut b, did);
    put_str16(&mut b, &host.0);
    put_str16(&mut b, from);
    b.put_i64(useq);
    b.put_slice(rest);
    b.into()
}

fn decode_item(meta: &Bytes) -> anyhow::Result<ItemMeta> {
    let mut r = meta.clone();
    let did = get_str16(&mut r).ok_or_else(|| anyhow::anyhow!("short item"))?;
    let host = Host(get_str16(&mut r).ok_or_else(|| anyhow::anyhow!("short item"))?);
    let from = get_str16(&mut r).ok_or_else(|| anyhow::anyhow!("short item"))?;
    anyhow::ensure!(r.remaining() >= 9, "short item");
    let useq = r.get_i64();
    if r[0] == 0xff {
        anyhow::ensure!(r.remaining() >= 2, "short operator item");
        let kind = match r[1] {
            OP_UNTAKEDOWN => ItemKind::Takedown(false),
            OP_TAKEDOWN => ItemKind::Takedown(true),
            OP_RELEASE => {
                r.advance(2);
                anyhow::ensure!(r.remaining() >= 1, "short release");
                let active = r.get_u8() != 0;
                let status = get_str16(&mut r).filter(|s| !s.is_empty());
                ItemKind::Release { active, status }
            }
            op => anyhow::bail!("unknown operator item {op}"),
        };
        return Ok(ItemMeta { did, host, from, useq, kind });
    }
    // the frame here is the prefix and suffix, without the seq: the span
    // check is the host owner's
    let m = super::forward::decode_meta(&did, r)?;
    Ok(ItemMeta { did, host, from, useq, kind: ItemKind::Event { kind: m.kind, first_sighting: m.first_sighting } })
}

/// What an entry's meta carries besides its state writes: who sent the
/// event, for the duplicate check of events with no rev, and whether its
/// DID's key changed (every node drops its cached key).
#[derive(Clone, Debug, PartialEq, Eq)]
struct Ext {
    kind: u8,
    key_changed: bool,
    host: String,
    useq: i64,
    did: String,
}

impl Ext {
    fn encode(&self) -> Bytes {
        let mut b = Vec::with_capacity(self.host.len() + self.did.len() + 16);
        b.put_u8(self.kind);
        b.put_u8(self.key_changed as u8);
        put_str16(&mut b, &self.host);
        b.put_i64(self.useq);
        put_str16(&mut b, &self.did);
        b.into()
    }

    fn decode(b: &Bytes) -> Option<Ext> {
        let mut r = b.clone();
        if r.remaining() < 2 {
            return None;
        }
        let kind = r.get_u8();
        let key_changed = r.get_u8() != 0;
        let host = get_str16(&mut r)?;
        if r.remaining() < 8 {
            return None;
        }
        let useq = r.get_i64();
        let did = get_str16(&mut r)?;
        Some(Ext { kind, key_changed, host, useq, did })
    }

    /// Events with no rev to catch a second copy by: the leader catches
    /// them by (host, upstream seq) instead.
    fn deduped(&self) -> bool {
        matches!(self.kind, KIND_SYNC | KIND_IDENTITY | KIND_ACCOUNT) && self.useq > 0
    }
}

/// What an operator's committed entry changed, for the admin change feed:
/// a takedown or its reversal, or a lifted relay throttle.
fn logged_change(x: &Ext, m: &Meta) -> Option<(crate::admin::changes::ChangeKind, String, serde_json::Value)> {
    use crate::admin::changes::ChangeKind;
    match x.kind {
        KIND_TAKEDOWN => {
            let key = state::record::did_key(&x.did);
            let rec = m.writes.iter().find(|(k, _)| k[..] == key[..]).and_then(|(_, v)| Record::decode(v).ok())?;
            Some((ChangeKind::Takedown, x.did.clone(), serde_json::json!({ "takedown": rec.relay_takedown })))
        }
        KIND_RELEASE => {
            let host = m.writes.iter().find_map(|(k, _)| throttled_from_key(k)).map(|(h, _)| h);
            Some((ChangeKind::Account, x.did.clone(), serde_json::json!({ "host": host })))
        }
        _ => None,
    }
}

/// The host table's state key.
pub const HOST_PREFIX: &[u8] = b"h/";

fn host_key(host: &str) -> Bytes {
    [HOST_PREFIX, host.as_bytes()].concat().into()
}

/// `t/{host}/{did}`: an account `host` created that the relay throttled
/// ("1"), or whose throttle was lifted ("0"). The record is the truth; this
/// is how the leader finds a host's throttled accounts without a scan.
pub const THROTTLED_PREFIX: &[u8] = b"t/";

fn throttled_key(host: &str, did: &str) -> Bytes {
    [THROTTLED_PREFIX, host.as_bytes(), b"/", did.as_bytes()].concat().into()
}

fn throttled_from_key(k: &[u8]) -> Option<(String, String)> {
    let rest = std::str::from_utf8(k.strip_prefix(THROTTLED_PREFIX)?).ok()?;
    let (h, d) = rest.split_once('/')?;
    Some((h.to_string(), d.to_string()))
}

/// One throttled account, as `leader:throttled` lists it: the status its
/// `#account` announces once the throttle is lifted.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct Throttled {
    pub did: String,
    pub active: bool,
    pub status: Option<String>,
}

/// One host as the log keeps it: its record's durable fields and the
/// member that owns it.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct HostRow {
    pub hostname: String,
    #[serde(default)]
    pub tier: state::Tier,
    #[serde(default)]
    pub first_seen: u32,
    #[serde(default)]
    pub owner: Option<String>,
    /// How it was found: `requestCrawl`, `bootstrap:<relay>`, `plc`, `cli`.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub source: Option<String>,
    /// The record's fields the log doesn't name (the policy's operator
    /// throttle, account cap, restore tier and action trail), so every
    /// member enforces and shows the same ones, and they outlive a restart.
    #[serde(default, skip_serializing_if = "serde_json::Map::is_empty")]
    pub extra: serde_json::Map<String, serde_json::Value>,
}

impl HostRow {
    fn record(&self) -> state::HostRecord {
        let mut r = state::HostRecord::new(&self.hostname, self.tier, self.first_seen);
        r.extra = self.extra.clone();
        r
    }
}

/// The leader's table as members read it (`Ask leader:hosts`).
#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct HostTable {
    pub epoch: u64,
    pub version: u64,
    pub rows: BTreeMap<String, HostRow>,
    /// The newest cursor per host the leader has seen committed.
    pub cursors: BTreeMap<String, u64>,
    /// Accounts each host created that the relay throttled (the leader's
    /// count, sent with the table).
    #[serde(default)]
    pub throttled: BTreeMap<String, u64>,
    /// `rows` left out: the reader said it holds this epoch and version
    /// ([`HostsAsk`]). At thousands of hosts with their policy fields the
    /// rows are megabytes, and only the cursors move between versions.
    #[serde(default, skip_serializing_if = "std::ops::Not::not")]
    pub same_rows: bool,
}

/// A member's `leader:hosts` ask: the (epoch, version) of the table it
/// holds. An empty ask (an older member) gets the whole table.
#[derive(Serialize, Deserialize)]
struct HostsAsk {
    have: (u64, u64),
}

/// A host's owner among `live`: the highest hash of (member, host), so a
/// member's departure moves only its own hosts.
fn rendezvous<'a>(host: &str, live: &'a [String]) -> Option<&'a String> {
    live.iter().max_by_key(|m| {
        let mut h: u64 = 0xcbf29ce484222325;
        for b in m.bytes().chain([0xff]).chain(host.bytes()) {
            h = (h ^ b as u64).wrapping_mul(0x100000001b3);
        }
        // a final mix: FNV's low bits barely move for a one-byte change
        h ^= h >> 33;
        h = h.wrapping_mul(0xff51afd7ed558ccd);
        h ^ (h >> 33)
    })
}

// ---------------------------------------------------------------- the leader's term

/// The leader's view for one term: the state's database as the applier
/// has written it, plus everything this term appended and the applier
/// hasn't reached (staged in `shard`, released by seq).
struct Term {
    epoch: u64,
    shard: Arc<ShardState>,
    applied: AtomicU64,
    inner: Mutex<TermInner>,
}

#[derive(Default)]
struct TermInner {
    /// The hooks' own tickets, from admission to `appended`.
    next: u64,
    staged: HashMap<u64, Staged>,
    /// seq -> the shard's ticket, until applied.
    by_seq: BTreeMap<u64, u64>,
    /// DID -> the seq of its newest entry this term holds.
    last_seq: HashMap<String, u64>,
    /// (host, upstream seq, DID) -> seq, for entries this term holds that
    /// the node hasn't seen commit.
    recent: HashMap<(String, i64, String), u64>,
    hosts: HostTable,
    /// Host rows to ride the next appended entry.
    pending_rows: BTreeMap<String, HostRow>,
    /// host -> accounts it created that this term saw throttled, logged or
    /// not (a commit-first account's throttle stays in memory).
    throttled: HashMap<String, HashSet<String>>,
}

struct Staged {
    shard_ticket: Option<u64>,
    did: String,
    dedupe: Option<(String, i64)>,
}

impl Term {
    fn release_upto(&self, upto: u64) {
        let tickets: Vec<u64> = {
            let mut i = self.inner.lock();
            let rest = i.by_seq.split_off(&(upto + 1));
            let done = std::mem::replace(&mut i.by_seq, rest);
            done.into_values().collect()
        };
        if !tickets.is_empty() {
            self.shard.release(tickets);
        }
    }
}

// ---------------------------------------------------------------- hooks

/// What the relay plugs into the quorum log.
pub struct RelayHooks {
    state: Arc<State>,
    /// The node's DID document cache: a committed key change drops the
    /// DID from it.
    identity: Option<Arc<crate::identity::IdentityCache<crate::identity::HttpFetch>>>,
    term: RwLock<Option<Arc<Term>>>,
    /// (host, upstream seq, DID) of committed events with no rev, until
    /// their host's cursor passes them: a second copy is a duplicate.
    recent: Mutex<HashMap<String, BTreeMap<i64, HashSet<String>>>>,
    /// The highest cursor per host this node has seen committed.
    cursors: Mutex<BTreeMap<String, u64>>,
    cfg: QuorumSetup,
    node: std::sync::OnceLock<std::sync::Weak<crate::qlog::node::Node>>,
    /// Itself, for the loops each term spawns.
    me: std::sync::Weak<RelayHooks>,
    pub stats: HookStats,
    /// This node's own numbers (rates, hosts read, consumers, CPU), for the
    /// status other members' dashboards read.
    local: Mutex<serde_json::Value>,
    /// The PLC export job, for `leader:plc`.
    pub plc: std::sync::OnceLock<Arc<crate::plc_seed::job::PlcJob>>,
    /// Host discovery, for `leader:discovery`.
    pub discovery: std::sync::OnceLock<Arc<crate::discovery::DiscoveryJob>>,
    /// The node's own answers (`node:*`: its consumers, settings, kicks),
    /// from its admin source.
    pub answers: std::sync::OnceLock<Arc<dyn LocalAsk>>,
    /// The admin change feed: committed takedowns and throttle lifts.
    pub changes: std::sync::OnceLock<Arc<crate::admin::changes::ChangeFeed>>,
    /// Tests: each decision of every other batch (at random) takes this
    /// long (µs), as a batch with slow identity lookups does.
    #[cfg(test)]
    slow_us: AtomicU64,
}

/// The leader's answer to an event from a node that doesn't own its host.
const NOT_OWNER: &str = "not_owner";

#[derive(Default)]
pub struct HookStats {
    pub not_owner: AtomicU64,
    /// Duplicates by how they were caught: a second copy of an event with
    /// no rev, the head already at the event's rev and commit, an older rev.
    pub dup_seen: AtomicU64,
    pub dup_head: AtomicU64,
    pub dup_stale: AtomicU64,
    pub admitted: AtomicU64,
    pub duplicates: AtomicU64,
    pub rejected: AtomicU64,
    pub retried: AtomicU64,
    pub not_ready: AtomicU64,
    pub host_moves: AtomicU64,
    pub terms: AtomicU64,
    pub retain_runs: AtomicU64,
    pub retain_deleted: AtomicU64,
}

impl RelayHooks {
    pub fn new(
        state: Arc<State>,
        identity: Option<Arc<crate::identity::IdentityCache<crate::identity::HttpFetch>>>,
        cfg: QuorumSetup,
    ) -> Arc<RelayHooks> {
        Arc::new_cyclic(|me| RelayHooks {
            state,
            identity,
            term: RwLock::new(None),
            recent: Mutex::new(HashMap::new()),
            cursors: Mutex::new(BTreeMap::new()),
            cfg,
            node: Default::default(),
            me: me.clone(),
            stats: HookStats::default(),
            local: Mutex::new(serde_json::Value::Null),
            plc: std::sync::OnceLock::new(),
            discovery: std::sync::OnceLock::new(),
            answers: std::sync::OnceLock::new(),
            changes: std::sync::OnceLock::new(),
            #[cfg(test)]
            slow_us: AtomicU64::new(0),
        })
    }

    fn current(&self, epoch: u64) -> Option<Arc<Term>> {
        self.term.read().as_ref().filter(|t| t.epoch == epoch).cloned()
    }

    fn qnode(&self) -> Option<Arc<crate::qlog::node::Node>> {
        self.node.get().and_then(|w| w.upgrade())
    }

    /// The term's host table, without its rows for a reader that holds
    /// `have`. None when this node isn't leading.
    fn host_table(&self, have: Option<(u64, u64)>) -> Option<HostTable> {
        let t = self.term.read().clone()?;
        let i = t.inner.lock();
        let same_rows = have == Some((i.hosts.epoch, i.hosts.version));
        Some(HostTable {
            epoch: i.hosts.epoch,
            version: i.hosts.version,
            rows: if same_rows { BTreeMap::new() } else { i.hosts.rows.clone() },
            cursors: i.hosts.cursors.clone(),
            throttled: i
                .throttled
                .iter()
                .filter(|(_, d)| !d.is_empty())
                .map(|(h, d)| (h.clone(), d.len() as u64))
                .collect(),
            same_rows,
        })
    }

    fn committed_dup(&self, host: &str, useq: i64, did: &str) -> bool {
        self.recent.lock().get(host).and_then(|m| m.get(&useq)).is_some_and(|s| s.contains(did))
    }

    async fn admit_one(&self, term: &Term, it: &Item, m: ItemMeta) -> (Verdict, Option<String>) {
        let ItemMeta { did, host, from, useq, kind } = m;
        let (kind, first_sighting) = match kind {
            ItemKind::Takedown(t) => return (self.takedown(term, &did, t).await, None),
            ItemKind::Release { active, status } => {
                return (self.release(term, &host.0, &did, active, status.as_deref()).await, None);
            }
            ItemKind::Event { kind, first_sighting } => (kind, first_sighting),
        };
        // One reader per host: a DID's events must reach the state in its
        // host's order, and two sockets on one host (an owner that hasn't
        // seen it moved, a node that just started) interleave them.
        let owner = term.inner.lock().hosts.rows.get(&host.0).and_then(|r| r.owner.clone());
        if owner.as_deref() != Some(from.as_str()) {
            self.stats.not_owner.fetch_add(1, Ordering::Relaxed);
            let why = format!("{NOT_OWNER}: {} reads {}", owner.as_deref().unwrap_or("nobody"), host.0);
            return (Verdict::Answer { outcome: Outcome::Retry(why), after: None }, None);
        }
        let tag = match &kind {
            CheckedKind::Commit(_) => KIND_COMMIT,
            CheckedKind::Sync(_) => KIND_SYNC,
            CheckedKind::Identity => KIND_IDENTITY,
            CheckedKind::Account { .. } => KIND_ACCOUNT,
        };
        let mut ext = Ext { kind: tag, key_changed: false, host: host.0.clone(), useq, did: did.clone() };
        if ext.deduped() {
            if self.committed_dup(&host.0, useq, &did) {
                self.stats.dup_seen.fetch_add(1, Ordering::Relaxed);
                return (Verdict::Answer { outcome: Outcome::Duplicate, after: None }, None);
            }
            if let Some(seq) = term.inner.lock().recent.get(&(host.0.clone(), useq, did.clone())).copied() {
                self.stats.dup_seen.fetch_add(1, Ordering::Relaxed);
                return (Verdict::Answer { outcome: Outcome::Duplicate, after: Some(seq) }, None);
            }
        }
        let ev_kind = match &kind {
            CheckedKind::Commit(v) => EventKind::Commit(v.clone()),
            CheckedKind::Sync(v) => EventKind::Sync { rev: v.rev, commit: v.commit, data: v.data },
            CheckedKind::Identity => EventKind::Identity,
            CheckedKind::Account { active, status } => EventKind::Account { active: *active, status: status.clone() },
        };
        let t0 = Instant::now();
        let mut tries = 0;
        let r = loop {
            let ev = Incoming { did: &did, host: &host, now: state::now_secs(), kind: clone_kind(&ev_kind) };
            match self.state.apply_held(&term.shard, ev).await {
                Err(e) if e.retryable() && tries < 2 && t0.elapsed() < Duration::from_millis(300) => {
                    tries += 1;
                    tokio::time::sleep(Duration::from_millis(50 << tries)).await;
                }
                r => break r,
            }
        };
        let dup_after = |term: &Term| {
            let applied = term.applied.load(Ordering::Acquire);
            term.inner.lock().last_seq.get(&did).copied().filter(|s| *s > applied)
        };
        let append = |term: &Term, writes: Vec<(Bytes, Bytes)>, shard_ticket: Option<u64>, ext: &Ext| {
            let mut i = term.inner.lock();
            i.next += 1;
            let tk = i.next;
            let dedupe = ext.deduped().then(|| (ext.host.clone(), ext.useq));
            i.staged.insert(tk, Staged { shard_ticket, did: did.clone(), dedupe });
            Verdict::Append { meta: Meta { writes, ext: ext.encode() }.encode(), ticket: tk }
        };
        let _ = it;
        match r {
            Ok(Applied::Append(a)) => {
                ext.key_changed = a.key_changed;
                let mut writes = vec![(Bytes::from(state::record::did_key(&did)), a.record.encode())];
                if a.record.relay_throttled {
                    writes.push((throttled_key(&host.0, &did), Bytes::from_static(b"1")));
                    term.inner.lock().throttled.entry(host.0.clone()).or_default().insert(did.clone());
                }
                (append(term, writes, Some(a.ticket.n), &ext), None)
            }
            Ok(Applied::Pass) => (append(term, Vec::new(), None, &ext), None),
            // A #sync may restate the head the last #commit left (a
            // reactivation does): news to consumers unless this very
            // upstream seq was seen before.
            Ok(Applied::Duplicate) if matches!(kind, CheckedKind::Sync(_)) && first_sighting => {
                (append(term, Vec::new(), None, &ext), None)
            }
            Ok(Applied::Duplicate) => {
                self.stats.dup_head.fetch_add(1, Ordering::Relaxed);
                (Verdict::Answer { outcome: Outcome::Duplicate, after: dup_after(term) }, None)
            }
            Err(state::Reject::Stale { .. }) => {
                self.stats.dup_stale.fetch_add(1, Ordering::Relaxed);
                (Verdict::Answer { outcome: Outcome::Duplicate, after: dup_after(term) }, None)
            }
            Err(e) if e.retryable() => {
                let r = super::state_rejection(&e);
                (
                    Verdict::Answer { outcome: Outcome::Retry(format!("{}: {}", r.reason, r.detail)), after: None },
                    Some(did),
                )
            }
            Err(e) => {
                if matches!(e, state::Reject::Inactive(state::AccountStatus::Throttled)) {
                    term.inner.lock().throttled.entry(host.0.clone()).or_default().insert(did.clone());
                }
                let r = super::state_rejection(&e);
                (
                    Verdict::Answer { outcome: Outcome::Rejected(format!("{}: {}", r.reason, r.detail)), after: None },
                    None,
                )
            }
        }
    }

    /// The accounts `host` created that are throttled now, with the status
    /// each announces once lifted.
    async fn throttled_of(&self, term: &Term, host: &str) -> anyhow::Result<Vec<Throttled>> {
        let mut dids: std::collections::BTreeSet<String> =
            term.inner.lock().throttled.get(host).map(|s| s.iter().cloned().collect()).unwrap_or_default();
        let prefix = [THROTTLED_PREFIX, host.as_bytes(), b"/"].concat();
        let mut it = term.shard.db.scan_prefix(&prefix, ..).await?;
        while let Some(kv) = it.next().await? {
            if let Some((_, d)) = throttled_from_key(&kv.key) {
                dids.insert(d);
            }
        }
        let mut out = Vec::new();
        for did in dids {
            let Some(rec) = term.shard.load(&did).await? else { continue };
            if !rec.relay_throttled {
                continue;
            }
            let mut lifted = (*rec).clone();
            lifted.relay_throttled = false;
            let st = lifted.status();
            out.push(Throttled { did, active: st.is_active(), status: st.as_str().map(str::to_string) });
        }
        Ok(out)
    }

    /// An operator lifting a relay throttle: the record's flag, and the
    /// `#account` the submitter built from `leader:throttled`'s status.
    async fn release(&self, term: &Term, host: &str, did: &str, active: bool, status: Option<&str>) -> Verdict {
        let prev = match term.shard.load(did).await {
            Ok(Some(r)) if r.relay_throttled => r,
            Ok(_) => return Verdict::Answer { outcome: Outcome::Duplicate, after: None },
            Err(e) => return Verdict::Answer { outcome: Outcome::Retry(format!("store: {e}")), after: None },
        };
        let mut rec: Record = (*prev).clone();
        rec.relay_throttled = false;
        let st = rec.status();
        // the frame announces what the record said when it was listed
        if (st.is_active(), st.as_str()) != (active, status) {
            return Verdict::Answer {
                outcome: Outcome::Retry("status_changed: list the throttled again".into()),
                after: None,
            };
        }
        let record = rec.clone();
        let t = term.shard.stage_logged(did, rec);
        let ext = Ext { kind: KIND_RELEASE, key_changed: false, host: String::new(), useq: 0, did: did.to_string() };
        let mut i = term.inner.lock();
        if let Some(s) = i.throttled.get_mut(host) {
            s.remove(did);
        }
        i.next += 1;
        let tk = i.next;
        i.staged.insert(tk, Staged { shard_ticket: Some(t.n), did: did.to_string(), dedupe: None });
        Verdict::Append {
            meta: Meta {
                writes: vec![
                    (Bytes::from(state::record::did_key(did)), record.encode()),
                    (throttled_key(host, did), Bytes::from_static(b"0")),
                ],
                ext: ext.encode(),
            }
            .encode(),
            ticket: tk,
        }
    }

    /// An operator's takedown: the record's flag, and an `#account` the
    /// relay makes announcing the status it leaves.
    async fn takedown(&self, term: &Term, did: &str, takedown: bool) -> Verdict {
        let shard = &term.shard;
        let prev = match shard.load(did).await {
            Ok(Some(r)) => r,
            Ok(None) => {
                return Verdict::Answer {
                    outcome: Outcome::Rejected(format!("no_account: no account {did}")),
                    after: None,
                };
            }
            Err(e) => return Verdict::Answer { outcome: Outcome::Retry(format!("store: {e}")), after: None },
        };
        let mut rec: Record = (*prev).clone();
        rec.relay_takedown = takedown;
        // lifting a takedown is how an operator also lifts a relay throttle
        if !takedown && rec.relay_throttled {
            rec.relay_throttled = false;
            for s in term.inner.lock().throttled.values_mut() {
                s.remove(did);
            }
        }
        let st = rec.status();
        let record = rec.clone();
        let t = shard.stage_logged(did, rec);
        let ext = Ext { kind: KIND_TAKEDOWN, key_changed: false, host: String::new(), useq: 0, did: did.to_string() };
        let mut i = term.inner.lock();
        i.next += 1;
        let tk = i.next;
        i.staged.insert(tk, Staged { shard_ticket: Some(t.n), did: did.to_string(), dedupe: None });
        let _ = st;
        Verdict::Append {
            meta: Meta { writes: vec![(Bytes::from(state::record::did_key(did)), record.encode())], ext: ext.encode() }
                .encode(),
            ticket: tk,
        }
    }

    /// The leader's host table: rows from submitters merge into it (the
    /// owner and cursor stay the leader's), and changes ride the next entry.
    fn merge_rows(&self, term: &Term, rows: Vec<HostRow>) {
        let mut i = term.inner.lock();
        let mut changed = false;
        for r in rows {
            let cur = i.hosts.rows.get(&r.hostname).cloned();
            let next = match cur {
                Some(c) if c.tier == r.tier && c.extra == r.extra && (c.source.is_some() || r.source.is_none()) => {
                    continue;
                }
                Some(c) => HostRow { tier: r.tier, extra: r.extra, source: c.source.clone().or(r.source), ..c },
                None => HostRow { owner: None, ..r },
            };
            i.hosts.rows.insert(next.hostname.clone(), next.clone());
            i.pending_rows.insert(next.hostname.clone(), next);
            changed = true;
        }
        if changed {
            i.hosts.version += 1;
        }
    }

    /// Owners for every host among the live members; the moves ride the
    /// next entry.
    fn assign(&self, term: &Term, live: &[String]) {
        if live.is_empty() {
            return;
        }
        let mut i = term.inner.lock();
        let mut moved = Vec::new();
        for (h, row) in &i.hosts.rows {
            let keep = row.owner.as_ref().is_some_and(|o| live.contains(o));
            if keep {
                continue;
            }
            if let Some(o) = rendezvous(h, live)
                && row.owner.as_ref() != Some(o)
            {
                moved.push(HostRow { owner: Some(o.clone()), ..row.clone() });
            }
        }
        if moved.is_empty() {
            return;
        }
        self.stats.host_moves.fetch_add(moved.len() as u64, Ordering::Relaxed);
        for r in moved {
            tracing::info!(host = %r.hostname, owner = ?r.owner, epoch = term.epoch, "quorum: host assigned");
            i.hosts.rows.insert(r.hostname.clone(), r.clone());
            i.pending_rows.insert(r.hostname.clone(), r);
        }
        i.hosts.version += 1;
    }

    /// The term's assignment loop: while it leads, every `host_poll`.
    async fn assign_loop(self: Arc<Self>, term: Arc<Term>) {
        loop {
            let Some(n) = self.qnode() else { return };
            let Some(live) = n.live_members(term.epoch, self.cfg.host_failover) else { return };
            if self.current(term.epoch).is_none() {
                return;
            }
            self.assign(&term, &live);
            tokio::time::sleep(self.cfg.host_poll.min(Duration::from_millis(500))).await;
        }
    }

    /// Bucket retention as a loop (Phase 7): the leader runs `qlog retain
    /// --apply`'s plan and deletes every `retain_every`, with the same
    /// re-checks against the manifest, billed to `qlog_retain`.
    async fn retain_loop(self: Arc<Self>, term: Arc<Term>, store: Store) {
        let Some(horizon) = self.cfg.retain_horizon else { return };
        let mut tick = tokio::time::interval(self.cfg.retain_every.max(Duration::from_secs(1)));
        tick.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
        tick.tick().await;
        loop {
            tick.tick().await;
            if self.current(term.epoch).is_none() {
                return;
            }
            match retain_once(&store, horizon).await {
                Ok(n) => {
                    self.stats.retain_runs.fetch_add(1, Ordering::Relaxed);
                    self.stats.retain_deleted.fetch_add(n, Ordering::Relaxed);
                }
                Err(e) => tracing::warn!(epoch = term.epoch, "quorum: bucket retention failed: {e:#}"),
            }
        }
    }
}

/// One retention pass: the plan, the deletes, the report. Returns the
/// objects deleted.
pub async fn retain_once(store: &Store, horizon: Duration) -> anyhow::Result<u64> {
    use crate::qlog::retain;
    let Some(plan) = retain::plan(store, horizon).await? else { return Ok(0) };
    let applied = retain::apply(store, &plan).await?;
    let n = applied.segments + applied.state_objects;
    retain::write(store, plan, Some(applied)).await?;
    if n > 0 {
        tracing::info!(deleted = n, "quorum: bucket retention");
    }
    Ok(n)
}

fn clone_kind(k: &EventKind<crate::verify::Verified>) -> EventKind<crate::verify::Verified> {
    match k {
        EventKind::Commit(v) => EventKind::Commit(v.clone()),
        EventKind::Sync { rev, commit, data } => EventKind::Sync { rev: *rev, commit: *commit, data: *data },
        EventKind::Identity => EventKind::Identity,
        EventKind::Account { active, status } => EventKind::Account { active: *active, status: status.clone() },
    }
}

/// What a submitter sends besides events (`SubmitEvents::control`).
#[derive(Default, Serialize, Deserialize)]
struct Control {
    #[serde(default)]
    rows: Vec<HostRow>,
}

impl Hooks for RelayHooks {
    /// Each DID's events in order, DIDs in parallel, all of the batch's DIDs
    /// locked from here until the batch is appended: a resent copy of a
    /// batch, or another host's event for one of its DIDs, can't decide in
    /// between and reach the log in the other order (the state takes each
    /// entry's record in log order).
    fn admit<'a>(&'a self, epoch: u64, items: &'a [Item], control: Bytes) -> BoxFuture<'a, Admission> {
        Box::pin(async move {
            let Some(term) = self.current(epoch) else {
                self.stats.not_ready.fetch_add(1, Ordering::Relaxed);
                let verdicts = items
                    .iter()
                    .map(|_| Verdict::Answer { outcome: Outcome::Retry("leader_not_ready".into()), after: None })
                    .collect();
                return Admission { verdicts, hold: None };
            };
            if !control.is_empty()
                && let Ok(c) = serde_json::from_slice::<Control>(&control)
            {
                self.merge_rows(&term, c.rows);
            }
            let mut out: Vec<Option<Verdict>> = (0..items.len()).map(|_| None).collect();
            let mut groups: HashMap<String, Vec<(usize, ItemMeta)>> = HashMap::new();
            for (i, it) in items.iter().enumerate() {
                match decode_item(&it.meta) {
                    Ok(m) => groups.entry(m.did.clone()).or_default().push((i, m)),
                    Err(e) => {
                        out[i] = Some(Verdict::Answer {
                            outcome: Outcome::Rejected(format!("bad_meta: {e:#}")),
                            after: None,
                        })
                    }
                }
            }
            let hold = term.shard.lock_dids_owned(groups.keys().map(|d| d.as_str())).await;
            #[cfg(test)]
            let slow = rand::random::<bool>();
            let runs = groups.into_values().map(|evs| {
                let term = &term;
                async move {
                    let mut done = Vec::with_capacity(evs.len());
                    let mut blocked = false;
                    for (i, m) in evs {
                        // a DID's later events wait for one that must be tried again
                        if blocked {
                            let r = Outcome::Retry("blocked: an earlier event of the DID".into());
                            done.push((i, Verdict::Answer { outcome: r, after: None }));
                            continue;
                        }
                        let (v, block) = self.admit_one(term, &items[i], m).await;
                        #[cfg(test)]
                        {
                            let us = self.slow_us.load(Ordering::Relaxed);
                            if us > 0 && slow {
                                tokio::time::sleep(Duration::from_micros(us)).await;
                            }
                        }
                        blocked = block.is_some();
                        done.push((i, v));
                    }
                    done
                }
            });
            for (i, v) in futures::future::join_all(runs).await.into_iter().flatten() {
                match &v {
                    Verdict::Append { .. } => self.stats.admitted.fetch_add(1, Ordering::Relaxed),
                    Verdict::Answer { outcome: Outcome::Duplicate, .. } => {
                        self.stats.duplicates.fetch_add(1, Ordering::Relaxed)
                    }
                    Verdict::Answer { outcome: Outcome::Retry(_), .. } => {
                        self.stats.retried.fetch_add(1, Ordering::Relaxed)
                    }
                    Verdict::Answer { .. } => self.stats.rejected.fetch_add(1, Ordering::Relaxed),
                };
                out[i] = Some(v);
            }
            let mut verdicts: Vec<Verdict> = out
                .into_iter()
                .map(|v| v.unwrap_or(Verdict::Answer { outcome: Outcome::Retry("undecided".into()), after: None }))
                .collect();
            // host table changes ride the first entry appended
            if let Some(Verdict::Append { meta, .. }) =
                verdicts.iter_mut().find(|v| matches!(v, Verdict::Append { .. }))
            {
                let rows = std::mem::take(&mut term.inner.lock().pending_rows);
                if !rows.is_empty() {
                    let mut m = Meta::decode(meta).unwrap_or_default();
                    for (h, r) in rows {
                        m.writes.push((host_key(&h), serde_json::to_vec(&r).unwrap_or_default().into()));
                    }
                    *meta = m.encode();
                }
            }
            Admission { verdicts, hold: Some(Box::new(hold)) }
        })
    }

    fn appended(&self, epoch: u64, seqs: Vec<(u64, u64)>) {
        let Some(term) = self.current(epoch) else { return };
        let applied = term.applied.load(Ordering::Acquire);
        let mut release = Vec::new();
        {
            let mut i = term.inner.lock();
            for (tk, seq) in seqs {
                let Some(st) = i.staged.remove(&tk) else { continue };
                i.last_seq.insert(st.did.clone(), seq);
                if let Some((h, u)) = st.dedupe {
                    i.recent.insert((h, u, st.did.clone()), seq);
                }
                if let Some(t) = st.shard_ticket {
                    if seq <= applied {
                        release.push(t);
                    } else {
                        i.by_seq.insert(seq, t);
                    }
                }
            }
        }
        if !release.is_empty() {
            term.shard.release(release);
        }
        // the applier may have passed these before they were registered
        term.release_upto(term.applied.load(Ordering::Acquire));
    }

    fn state_opened<'a>(
        &'a self,
        node: &'a Arc<crate::qlog::node::Node>,
        epoch: u64,
        db: slatedb::Db,
        applied: u64,
    ) -> BoxFuture<'a, anyhow::Result<()>> {
        Box::pin(async move {
            let _ = self.node.set(Arc::downgrade(node));
            let shard = Arc::new(ShardState::new(
                ShardId(0),
                0,
                vlsync_store::slots::SLOTS,
                Arc::new(db.clone()),
                self.state.config.cache_entries_per_shard,
            ));
            let term = Arc::new(Term {
                epoch,
                shard: shard.clone(),
                applied: AtomicU64::new(applied),
                inner: Default::default(),
            });
            // the table as applied, then the tail above it
            {
                let mut rows = BTreeMap::new();
                let mut it = db.scan_prefix(HOST_PREFIX, ..).await?;
                while let Some(kv) = it.next().await? {
                    if let Ok(r) = serde_json::from_slice::<HostRow>(&kv.value) {
                        rows.insert(r.hostname.clone(), r);
                    }
                }
                let mut cursors = BTreeMap::new();
                let mut it = db.scan_prefix(b"c/", ..).await?;
                while let Some(kv) = it.next().await? {
                    if let Ok(b) = <[u8; 8]>::try_from(kv.value.as_ref()) {
                        cursors.insert(String::from_utf8_lossy(&kv.key[2..]).into_owned(), u64::from_be_bytes(b));
                    }
                }
                let mut throttled: HashMap<String, HashSet<String>> = HashMap::new();
                let mut it = db.scan_prefix(THROTTLED_PREFIX, ..).await?;
                while let Some(kv) = it.next().await? {
                    if kv.value.as_ref() == b"1"
                        && let Some((h, d)) = throttled_from_key(&kv.key)
                    {
                        throttled.entry(h).or_default().insert(d);
                    }
                }
                let mut i = term.inner.lock();
                i.hosts = HostTable { epoch, version: 1, rows, cursors, throttled: BTreeMap::new(), same_rows: false };
                i.throttled = throttled;
            }
            let emitted = node.emitted();
            let mut next = applied + 1;
            let mut n = 0u64;
            loop {
                let es = node.read_tail(next, 8 << 20).await?;
                let Some(last) = es.last().map(|e| e.seq) else { break };
                for e in &es {
                    self.stage_tail(&term, e, emitted).await;
                }
                n += es.len() as u64;
                next = last + 1;
            }
            {
                let mut i = term.inner.lock();
                for (h, c) in self.cursors.lock().iter() {
                    let e = i.hosts.cursors.entry(h.clone()).or_default();
                    *e = (*e).max(*c);
                }
            }
            self.state.attach_shard(shard);
            let old = self.term.write().replace(term.clone());
            if let Some(o) = old {
                self.state.detach_shard(&o.shard);
            }
            self.stats.terms.fetch_add(1, Ordering::Relaxed);
            tracing::info!(
                epoch,
                applied,
                tail = n,
                hosts = term.inner.lock().hosts.rows.len(),
                "quorum: leader state ready"
            );
            let me = self.self_arc();
            if let Some(me) = me {
                tokio::spawn(me.clone().assign_loop(term.clone()));
                tokio::spawn(me.retain_loop(term, node.bucket().retain.clone()));
            }
            Ok(())
        })
    }

    fn state_applied(&self, epoch: u64, upto: u64) {
        let Some(term) = self.current(epoch) else { return };
        term.applied.fetch_max(upto, Ordering::AcqRel);
        term.release_upto(upto);
        let mut i = term.inner.lock();
        if i.last_seq.len() > 100_000 {
            i.last_seq.retain(|_, s| *s > upto);
        }
        drop(i);
        term.shard.settle_unlogged_in_memory();
    }

    fn term_ended(&self, epoch: u64) {
        let mut t = self.term.write();
        if t.as_ref().is_some_and(|x| x.epoch == epoch) {
            let old = t.take().expect("checked");
            self.state.detach_shard(&old.shard);
            tracing::info!(epoch, "quorum: leader term over");
        }
    }

    fn committed(&self, entries: &[Entry]) {
        let term = self.term.read().clone();
        let mut recent = self.recent.lock();
        for e in entries {
            for (h, c) in decode_cursors(&e.cursors) {
                {
                    let mut cs = self.cursors.lock();
                    let x = cs.entry(h.clone()).or_default();
                    *x = (*x).max(c);
                }
                if let Some(m) = recent.get_mut(&h) {
                    let rest = m.split_off(&(c as i64 + 1));
                    *m = rest;
                }
                if let Some(t) = &term {
                    let mut i = t.inner.lock();
                    let x = i.hosts.cursors.entry(h.clone()).or_default();
                    *x = (*x).max(c);
                }
            }
            if e.meta.is_empty() {
                continue;
            }
            let Some(m) = Meta::decode(&e.meta) else { continue };
            let Some(x) = Ext::decode(&m.ext) else { continue };
            if x.deduped() {
                recent.entry(x.host.clone()).or_default().entry(x.useq).or_default().insert(x.did.clone());
                if let Some(t) = &term {
                    t.inner.lock().recent.remove(&(x.host.clone(), x.useq, x.did.clone()));
                }
            }
            if (x.key_changed || x.kind == KIND_IDENTITY)
                && let Some(id) = &self.identity
            {
                id.invalidate(&x.did);
            }
            if let Some(f) = self.changes.get()
                && let Some((kind, id, hint)) = logged_change(&x, &m)
            {
                f.publish_versioned(kind, id, e.seq.to_string(), Some(hint), false);
            }
        }
    }

    fn report(&self) -> serde_json::Value {
        let s = &self.stats;
        let term = self.term.read().clone();
        let (hosts, owners, pending) = match &term {
            Some(t) => {
                let i = t.inner.lock();
                let mut owners: BTreeMap<String, u64> = BTreeMap::new();
                for r in i.hosts.rows.values() {
                    *owners.entry(r.owner.clone().unwrap_or_else(|| "-".into())).or_default() += 1;
                }
                (i.hosts.rows.len(), Some(owners), i.by_seq.len())
            }
            None => (0, None, 0),
        };
        serde_json::json!({
            "leading": term.as_ref().map(|t| t.epoch),
            "admitted": s.admitted.load(Ordering::Relaxed),
            "duplicates": s.duplicates.load(Ordering::Relaxed),
            "rejected": s.rejected.load(Ordering::Relaxed),
            "retried": s.retried.load(Ordering::Relaxed),
            "not_ready": s.not_ready.load(Ordering::Relaxed),
            "not_owner": s.not_owner.load(Ordering::Relaxed),
            "dup_seen": s.dup_seen.load(Ordering::Relaxed),
            "dup_head": s.dup_head.load(Ordering::Relaxed),
            "dup_stale": s.dup_stale.load(Ordering::Relaxed),
            "host_moves": s.host_moves.load(Ordering::Relaxed),
            "terms": s.terms.load(Ordering::Relaxed),
            "retain_runs": s.retain_runs.load(Ordering::Relaxed),
            "retain_deleted": s.retain_deleted.load(Ordering::Relaxed),
            "hosts": hosts,
            "owners": owners,
            "unapplied": pending,
            "recent": self.recent.lock().values().map(|m| m.len()).sum::<usize>(),
            "node": self.local.lock().clone(),
        })
    }

    fn answer<'a>(&'a self, topic: &'a str, body: Bytes) -> BoxFuture<'a, Option<Bytes>> {
        Box::pin(async move {
            match topic {
                "leader:hosts" => {
                    let have = serde_json::from_slice::<HostsAsk>(&body).ok().map(|a| a.have);
                    serde_json::to_vec(&self.host_table(have)?).ok().map(Bytes::from)
                }
                "leader:flush" => {
                    let want = self.cfg.admin_token.as_deref().filter(|t| !t.is_empty())?;
                    let v: serde_json::Value = serde_json::from_slice(&body).ok()?;
                    if v["token"].as_str() != Some(want) {
                        return serde_json::to_vec(&serde_json::json!({"error": "unauthorized"})).ok().map(Bytes::from);
                    }
                    let q = self.qnode()?;
                    let top = q.status().commit;
                    q.flush.request(top);
                    let t = Instant::now();
                    while q.status().flushed < top && t.elapsed() < Duration::from_secs(30) {
                        tokio::time::sleep(Duration::from_millis(50)).await;
                    }
                    serde_json::to_vec(&q.status()).ok().map(Bytes::from)
                }
                "leader:discovery" => {
                    let j = self.discovery.get()?;
                    serde_json::to_vec(&j.view()).ok().map(Bytes::from)
                }
                "leader:discovery-run" => {
                    let j = self.discovery.get()?;
                    let source = serde_json::from_slice::<serde_json::Value>(&body)
                        .ok()
                        .and_then(|v| v["source"].as_str().map(str::to_string));
                    j.request(source);
                    serde_json::to_vec(&j.view()).ok().map(Bytes::from)
                }
                "leader:plc" => {
                    let r = match self.plc.get() {
                        Some(j) => j.report().await,
                        None => None,
                    };
                    serde_json::to_vec(&r).ok().map(Bytes::from)
                }
                "leader:throttled" => {
                    let t = self.term.read().clone()?;
                    let host = std::str::from_utf8(&body).ok()?;
                    let r = match self.throttled_of(&t, host).await {
                        Ok(list) => serde_json::json!({ "accounts": list }),
                        Err(e) => serde_json::json!({ "error": format!("{e:#}") }),
                    };
                    serde_json::to_vec(&r).ok().map(Bytes::from)
                }
                t if t.starts_with("node:") => match self.answers.get() {
                    Some(l) => l.answer(t, body).await,
                    None => None,
                },
                _ => None,
            }
        })
    }
}

/// What a member answers about itself over the peer protocol.
pub trait LocalAsk: Send + Sync {
    fn answer<'a>(&'a self, topic: &'a str, body: Bytes) -> BoxFuture<'a, Option<Bytes>>;
}

impl RelayHooks {
    async fn stage_tail(&self, term: &Term, e: &Entry, emitted: u64) {
        if e.meta.is_empty() {
            return;
        }
        let Some(m) = Meta::decode(&e.meta) else { return };
        let ext = Ext::decode(&m.ext);
        for (k, v) in &m.writes {
            if let Some(h) = k.strip_prefix(HOST_PREFIX) {
                if let Ok(r) = serde_json::from_slice::<HostRow>(v) {
                    let mut i = term.inner.lock();
                    i.hosts.rows.insert(String::from_utf8_lossy(h).into_owned(), r);
                }
                continue;
            }
            if let Some((h, d)) = throttled_from_key(k) {
                let mut i = term.inner.lock();
                if v.as_ref() == b"1" {
                    i.throttled.entry(h).or_default().insert(d);
                } else if let Some(s) = i.throttled.get_mut(&h) {
                    s.remove(&d);
                }
                continue;
            }
            let Some(did) = state::record::did_from_key(k) else { continue };
            let Ok(rec) = Record::decode(v) else { continue };
            let _g = term.shard.lock_did(&did).await;
            let t = term.shard.stage_logged(&did, rec);
            let mut i = term.inner.lock();
            i.by_seq.insert(e.seq, t.n);
            i.last_seq.insert(did, e.seq);
        }
        if let Some(x) = ext
            && x.deduped()
            && e.seq > emitted
        {
            term.inner.lock().recent.insert((x.host, x.useq, x.did), e.seq);
        }
    }

    fn self_arc(&self) -> Option<Arc<RelayHooks>> {
        self.me.upgrade()
    }
}

// ---------------------------------------------------------------- the host owner's side

/// How long an event the leader keeps answering "try again" (its state not
/// ready, PLC trouble) is retried before the host is asked to send it again.
pub(super) const GIVE_UP: Duration = Duration::from_secs(20);
const SLOTS: usize = 16;
const MAX_BATCH: usize = 512;
const MAX_BATCH_BYTES: usize = 4 << 20;

struct Pending {
    item: Item,
    fence: Option<super::forward::Fence>,
    since: Instant,
    tx: oneshot::Sender<Result<FwdOutcome, ForwardError>>,
}

/// What rides the next submit besides events.
#[derive(Default)]
struct Outbox {
    cursors: BTreeMap<String, u64>,
    rows: Vec<HostRow>,
    /// When something was put in that no submit has taken yet.
    since: Option<Instant>,
}

/// The node's half shared by its submit slots, cursor ticker, rewinds and
/// host poller.
pub struct Shared {
    client: Arc<Client>,
    outbox: Mutex<Outbox>,
    /// Read while cursors and their generation are taken together; written
    /// by a rewind, so no cursor from before it counts after.
    rewind: tokio::sync::RwLock<()>,
    rewinding: AtomicU64,
    /// host -> the cursor a recovery left it at, until its socket resumes
    /// from there.
    pending_rewind: Mutex<HashMap<Host, i64>>,
    /// Submit to answer, for the dashboard's durable lag.
    lat_us: AtomicU64,
    lat_n: AtomicU64,
    /// Tests: host rows stay in the outbox, as if the leader never got them.
    #[cfg(test)]
    pub(crate) hold_rows: std::sync::atomic::AtomicBool,
}

impl Shared {
    /// The outbox's cursors and rows, and the generation they're under.
    /// While a rewind runs, cursors stay behind (they may count lost
    /// events) and only rows go.
    fn take(&self) -> (Bytes, Bytes, u64) {
        let guard = self.rewind.try_read();
        let mut o = self.outbox.lock();
        o.since = None;
        let control = self.rows_control(&mut o);
        match guard {
            Ok(_g) => (encode_cursors(&std::mem::take(&mut o.cursors)), control, self.client.generation()),
            Err(_) => (Bytes::new(), control, self.client.generation()),
        }
    }

    fn rows_control(&self, o: &mut Outbox) -> Bytes {
        #[cfg(test)]
        if self.hold_rows.load(Ordering::Relaxed) {
            return Bytes::new();
        }
        let rows = std::mem::take(&mut o.rows);
        if rows.is_empty() {
            return Bytes::new();
        }
        serde_json::to_vec(&Control { rows }).map(Bytes::from).unwrap_or_default()
    }

    /// The outbox's host rows alone, as a submit's control.
    fn take_rows(&self) -> Bytes {
        self.rows_control(&mut self.outbox.lock())
    }
}

/// The lane's DID owner on the quorum log: every event goes to the leader.
pub struct QuorumOwner {
    id: String,
    slots: Vec<mpsc::UnboundedSender<Pending>>,
}

impl QuorumOwner {
    fn start(id: &str, shared: Arc<Shared>, glue: std::sync::Weak<Glue>) -> Arc<QuorumOwner> {
        let mut slots = Vec::new();
        for _ in 0..SLOTS {
            let (tx, rx) = mpsc::unbounded_channel();
            tokio::spawn(slot(shared.clone(), glue.clone(), rx));
            slots.push(tx);
        }
        Arc::new(QuorumOwner { id: id.to_string(), slots })
    }
}

#[async_trait::async_trait]
impl DidOwner for QuorumOwner {
    async fn submit(&self, c: Checked) -> Submitted {
        let (s, e) = (c.span.start as usize, c.span.end as usize);
        // the leader writes the seq key and its own value: the frame's
        // own `seq` key comes off with its value
        if s < 4 || &c.frame[s - 4..s] != b"\x63seq" || e > c.frame.len() {
            return Submitted::Rejected(Rejection { reason: "bad_frame", detail: "no seq key before the seq".into() });
        }
        let item = Item {
            prefix: c.frame.slice(..s - 4),
            suffix: c.frame.slice(e..),
            meta: encode_item(&c.did, &c.host, &self.id, c.upstream_seq, &super::forward::encode_meta(&c)),
        };
        let (tx, rx) = oneshot::channel();
        let i = super::lane_of(&c.did, self.slots.len());
        let _ = self.slots[i].send(Pending { item, fence: c.fence, since: Instant::now(), tx });
        Submitted::Forwarded(rx)
    }
}

async fn slot(shared: Arc<Shared>, glue: std::sync::Weak<Glue>, mut rx: mpsc::UnboundedReceiver<Pending>) {
    let mut again: VecDeque<Pending> = VecDeque::new();
    loop {
        let mut batch: Vec<Pending> = Vec::new();
        let mut bytes = 0;
        while batch.len() < MAX_BATCH && bytes < MAX_BATCH_BYTES {
            let Some(p) = again.pop_front() else { break };
            bytes += p.item.prefix.len() + p.item.suffix.len();
            batch.push(p);
        }
        if batch.is_empty() {
            match rx.recv().await {
                Some(p) => batch.push(p),
                None => return,
            }
        }
        while batch.len() < MAX_BATCH && bytes < MAX_BATCH_BYTES {
            match rx.try_recv() {
                Ok(p) => {
                    bytes += p.item.prefix.len() + p.item.suffix.len();
                    batch.push(p);
                }
                Err(_) => break,
            }
        }
        // a socket that gave up on an event sends nothing more of its own
        batch.retain_mut(|p| {
            if p.fence.as_ref().is_some_and(|f| !f.live()) {
                let (tx, _) = oneshot::channel();
                let _ = std::mem::replace(&mut p.tx, tx).send(Err(ForwardError::Fenced));
                false
            } else {
                true
            }
        });
        if batch.is_empty() {
            continue;
        }
        let (cursors, control, cgen) = shared.take();
        let items: Vec<Item> = batch.iter().map(|p| p.item.clone()).collect();
        let t0 = Instant::now();
        let d: Decided = shared.client.submit_events(items, cursors, control, cgen).await;
        let took = t0.elapsed().as_micros() as u64;
        shared.lat_us.fetch_add(took * batch.len() as u64, Ordering::Relaxed);
        shared.lat_n.fetch_add(batch.len() as u64, Ordering::Relaxed);
        if d.generation > shared.client.generation()
            && let Some(g) = glue.upgrade()
        {
            g.rewind(d.generation);
        }
        let mut retry = false;
        for (p, o) in batch.into_iter().zip(d.outcomes) {
            let r = match o {
                Outcome::Appended(seq) => Ok(FwdOutcome::Appended(seq as i64)),
                Outcome::Duplicate => Ok(FwdOutcome::Duplicate),
                Outcome::Rejected(r) => Ok(FwdOutcome::Rejected(r)),
                // the host is another node's now: this socket sends nothing
                // more, and its cursor stays where it is
                Outcome::Retry(r) if r.starts_with(NOT_OWNER) => {
                    if let Some(f) = &p.fence {
                        f.trip();
                    }
                    Err(ForwardError::Fenced)
                }
                Outcome::Retry(_) if p.since.elapsed() < GIVE_UP => {
                    retry = true;
                    again.push_back(p);
                    continue;
                }
                Outcome::Retry(r) => {
                    if let Some(f) = &p.fence {
                        f.trip();
                    }
                    Err(ForwardError::GaveUp(p.since.elapsed(), r))
                }
            };
            let _ = p.tx.send(r);
        }
        if retry {
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
    }
}

/// Where a host's socket resumes: a rewind's cursor once, then the newer
/// of what this node acked and what the leader has committed for it.
pub struct QuorumCursors {
    shared: Arc<Shared>,
    hosts: Arc<QuorumHosts>,
    registry: std::sync::OnceLock<Arc<upstream::Registry>>,
}

impl upstream::CursorSource for QuorumCursors {
    fn durable_cursor(&self, host: &Host) -> Option<i64> {
        let entry = self.registry.get().and_then(|r| r.get(host));
        if let Some(c) = self.shared.pending_rewind.lock().remove(host) {
            if let Some(e) = &entry {
                e.restore_cursor(c);
            }
            return Some(c);
        }
        let committed = self.hosts.table.read().cursors.get(&host.0).map(|c| *c as i64);
        entry.and_then(|e| e.acked_seq()).max(committed)
    }
}

// ---------------------------------------------------------------- hosts

/// The host records on the quorum log: the leader's table (tier, owner,
/// committed cursor) as last read, plus what this node knows and the log
/// doesn't keep (connection state, counts), which stays in memory.
pub struct QuorumHosts {
    table: RwLock<HostTable>,
    local: RwLock<BTreeMap<String, state::HostRecord>>,
    shared: Arc<Shared>,
    /// Sources of hosts this node admitted, until the table has them.
    sources: Mutex<HashMap<String, String>>,
    /// Told each host whose row a new table moved (the policy's sync loop).
    changed: std::sync::OnceLock<mpsc::UnboundedSender<String>>,
}

impl QuorumHosts {
    /// Accounts `host` created that the relay throttled, as the leader
    /// counted them at the last poll.
    pub fn throttled(&self, host: &str) -> u64 {
        self.table.read().throttled.get(host).copied().unwrap_or(0)
    }

    /// Where `host` came from, for the row this node is about to propose.
    pub fn note_source(&self, host: &str, source: &str) {
        let mut m = self.sources.lock();
        if m.len() < 100_000 {
            m.insert(host.to_string(), source.to_string());
        }
    }

    /// Where the host table says `host` came from.
    pub fn source(&self, host: &str) -> Option<String> {
        self.table.read().rows.get(host).and_then(|r| r.source.clone())
    }

    fn record(&self, h: &str) -> Option<state::HostRecord> {
        let row = self.table.read().rows.get(h).cloned();
        let local = self.local.read().get(h).cloned();
        match (row, local) {
            (Some(r), Some(mut l)) => {
                l.tier = r.tier;
                l.extra = r.extra;
                l.cursor = self.table.read().cursors.get(h).map_or(l.cursor, |c| *c as i64);
                Some(l)
            }
            (Some(r), None) => {
                let mut rec = r.record();
                rec.cursor = self.table.read().cursors.get(h).map_or(0, |c| *c as i64);
                Some(rec)
            }
            (None, l) => l,
        }
    }

    fn all(&self) -> Vec<state::HostRecord> {
        let mut names: std::collections::BTreeSet<String> = self.table.read().rows.keys().cloned().collect();
        names.extend(self.local.read().keys().cloned());
        names.iter().filter_map(|h| self.record(h)).collect()
    }

    /// A row the table doesn't have yet, or a new tier or policy: rides
    /// the next submit to the leader (a missing row again on every poll
    /// until the table has it).
    fn propose(&self, rec: &state::HostRecord) {
        let cur = self
            .table
            .read()
            .rows
            .get(&rec.hostname)
            .map(|r| (r.tier == rec.tier && r.extra == rec.extra, r.source.is_some()));
        let source = self.sources.lock().get(&rec.hostname).cloned();
        if cur.is_some_and(|(same, has)| same && (has || source.is_none())) {
            if cur.is_some_and(|(_, has)| has) {
                self.sources.lock().remove(&rec.hostname);
            }
            return;
        }
        let mut o = self.shared.outbox.lock();
        o.rows.retain(|r| r.hostname != rec.hostname);
        o.rows.push(HostRow {
            hostname: rec.hostname.clone(),
            tier: rec.tier,
            first_seen: rec.first_seen,
            owner: None,
            source,
            extra: rec.extra.clone(),
        });
        o.since.get_or_insert_with(Instant::now);
    }

    /// host -> the member that reads it, as the leader's table last said.
    pub fn owners(&self) -> HashMap<String, String> {
        self.table.read().rows.values().filter_map(|r| Some((r.hostname.clone(), r.owner.clone()?))).collect()
    }

    fn owned(&self, me: &str) -> HashSet<Host> {
        self.table
            .read()
            .rows
            .values()
            .filter(|r| r.owner.as_deref() == Some(me))
            .map(|r| Host(r.hostname.clone()))
            .collect()
    }
}

impl upstream::HostStore for QuorumHosts {
    fn load(&self) -> upstream::host::StoreFuture<'_, Vec<upstream::HostRecord>> {
        Box::pin(async move { Ok(self.all().iter().map(super::adapters::to_upstream).collect()) })
    }

    fn put(&self, records: Vec<upstream::HostRecord>) -> upstream::host::StoreFuture<'_, ()> {
        Box::pin(async move {
            for r in &records {
                let rec = {
                    let mut l = self.local.write();
                    let tier = super::adapters::tier_to_state(r.tier);
                    let rec = l
                        .entry(r.hostname.clone())
                        .or_insert_with(|| state::HostRecord::new(&r.hostname, tier, state::now_secs()));
                    super::adapters::apply_upstream(rec, r);
                    rec.clone()
                };
                // the registry's tier only seeds a new row: the policy owns it after
                if !self.table.read().rows.contains_key(&r.hostname) {
                    self.propose(&rec);
                }
            }
            Ok(())
        })
    }
}

#[async_trait::async_trait]
impl state::HostStore for QuorumHosts {
    async fn get_host(&self, hostname: &str) -> anyhow::Result<Option<state::HostRecord>> {
        Ok(self.record(hostname))
    }

    async fn put_host(&self, rec: &state::HostRecord) -> anyhow::Result<()> {
        self.local.write().insert(rec.hostname.clone(), rec.clone());
        self.propose(rec);
        Ok(())
    }

    async fn update_host(&self, hostname: &str, f: state::HostUpdate<'_>) -> anyhow::Result<Option<state::HostRecord>> {
        let cur = self.record(hostname);
        match f(cur) {
            Some(rec) => {
                self.put_host(&rec).await?;
                Ok(Some(rec))
            }
            None => Ok(None),
        }
    }

    async fn checkpoint_cursors(&self, _cursors: &[(String, i64)]) -> anyhow::Result<()> {
        Ok(())
    }

    async fn add_counts(&self, counts: &[(String, state::HostCounts)]) -> anyhow::Result<()> {
        let mut l = self.local.write();
        for (h, c) in counts {
            let rec =
                l.entry(h.clone()).or_insert_with(|| state::HostRecord::new(h, state::Tier::New, state::now_secs()));
            rec.account_count += c.accounts;
            rec.events += c.events;
            rec.failed_checks += c.failed_checks;
            rec.dropped += c.dropped;
        }
        Ok(())
    }

    async fn list_hosts(&self, cursor: Option<&str>, limit: usize) -> anyhow::Result<state::HostPage> {
        let mut rows = self.all();
        let key = |h: &str| (vlsync_store::slots::slot_of(h), h.to_string());
        rows.sort_by_key(|r| key(&r.hostname));
        if let Some(c) = cursor.filter(|c| !c.is_empty()) {
            let k = key(c);
            rows.retain(|r| key(&r.hostname) > k);
        }
        let limit = limit.max(1);
        let more = rows.len() > limit;
        rows.truncate(limit);
        let cursor = more.then(|| rows.last().map(|r| r.hostname.clone())).flatten();
        Ok(state::HostPage { hosts: rows, cursor })
    }
}

/// The sync API's reads: the leader's records (as applied), and the host
/// table on any node.
pub struct QuorumSync {
    pub state: Arc<State>,
    pub hosts: Arc<QuorumHosts>,
}

#[async_trait::async_trait]
impl crate::sync_api::SyncSource for QuorumSync {
    async fn list_repos(&self, cursor: Option<&str>, limit: usize) -> Result<state::RepoPage, state::StoreError> {
        self.state.list_repos(cursor, limit).await
    }
    async fn repo(&self, did: &str) -> Result<Option<Arc<Record>>, state::StoreError> {
        self.state.get(did).await
    }
    async fn host(&self, hostname: &str) -> anyhow::Result<Option<state::HostRecord>> {
        state::HostStore::get_host(&*self.hosts, hostname).await
    }
    async fn list_hosts(&self, cursor: Option<&str>, limit: usize) -> anyhow::Result<state::HostPage> {
        state::HostStore::list_hosts(&*self.hosts, cursor, limit).await
    }
}

// ---------------------------------------------------------------- glue

/// A node's quorum log half: the log node, its client, the hooks and the
/// host table.
pub struct Glue {
    pub id: String,
    pub qnode: Arc<crate::qlog::node::Node>,
    pub client: Arc<Client>,
    pub hooks: Arc<RelayHooks>,
    pub hosts: Arc<QuorumHosts>,
    pub shared: Arc<Shared>,
    pub setup: QuorumSetup,
    pub plc: Option<Arc<crate::plc_seed::job::PlcJob>>,
    /// Which DIDs' lookups have failed long enough to be final, for the
    /// host stage and the leader alike.
    pub patience: Arc<super::patience::Patience>,
    manager: std::sync::OnceLock<Arc<Manager>>,
    filter: watch::Sender<HostFilter>,
    owned: Mutex<HashSet<Host>>,
}

impl Glue {
    /// Rewinds every host to where bucket recovery `g` left it (once per
    /// generation): their sockets reconnect from there, and this node's
    /// cursors count again only once each has (`Client::rewound`).
    fn rewind(self: &Arc<Self>, g: u64) {
        if self.shared.rewinding.fetch_max(g, Ordering::AcqRel) >= g {
            return;
        }
        let me = self.clone();
        tokio::spawn(async move {
            let _w = me.shared.rewind.write().await;
            let (g, cursors) = me.client.recovery_cursors(g).await;
            let Some(m) = me.manager.get().cloned() else { return };
            let mut kicked = Vec::new();
            for e in m.registry().all() {
                let c = cursors.get(&e.host.0).copied().unwrap_or(0) as i64;
                if m.is_running(&e.host) {
                    me.shared.pending_rewind.lock().insert(e.host.clone(), c);
                    kicked.push((e.host.clone(), e.epoch()));
                    m.kick(&e.host);
                } else {
                    e.restore_cursor(c);
                }
            }
            let until = Instant::now() + Duration::from_secs(10);
            for (h, epoch) in kicked {
                while Instant::now() < until
                    && m.is_running(&h)
                    && m.registry().get(&h).is_some_and(|e| e.epoch() <= epoch)
                {
                    tokio::time::sleep(Duration::from_millis(20)).await;
                }
            }
            me.hooks.recent.lock().clear();
            me.shared.outbox.lock().cursors.clear();
            me.client.rewound(g);
            tracing::warn!(
                generation = g,
                hosts = cursors.len(),
                "quorum: hosts rewound to a bucket recovery's cursors"
            );
        });
    }

    /// Reads the leader's host table every `host_poll` and follows it.
    async fn poll_hosts(self: Arc<Self>) {
        let mut tick = tokio::time::interval(self.setup.host_poll);
        tick.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
        loop {
            tick.tick().await;
            if let Some(t) = self.read_table(Duration::from_secs(1)).await {
                self.install_table(t);
            }
        }
    }

    async fn read_table(&self, patience: Duration) -> Option<HostTable> {
        let have = {
            let c = self.hosts.table.read();
            (c.epoch > 0).then_some((c.epoch, c.version))
        };
        // the leader's own member has the table in memory: asking through
        // the log's client would be JSON both ways every poll
        if let Some(t) = self.local_table(have) {
            return Some(t);
        }
        let ask = have.map(|have| Bytes::from(serde_json::to_vec(&HostsAsk { have }).expect("serializable")));
        let b = self.client.ask_leader("leader:hosts", ask.unwrap_or_default(), patience).await.ok()?;
        serde_json::from_slice::<HostTable>(&b).map_err(|e| tracing::warn!("quorum: a bad host table: {e}")).ok()
    }

    /// This node's own term's table, while the log has it leading that
    /// term (a term the node lost waits for `term_ended`; the log knows
    /// first).
    fn local_table(&self, have: Option<(u64, u64)>) -> Option<HostTable> {
        let epoch = self.hooks.term.read().as_ref()?.epoch;
        self.qnode.leading(epoch)?;
        self.hooks.host_table(have)
    }

    /// Follows a table read from the leader: the hosts it gives this node
    /// are the manager's, and each host whose tier or policy it moved is
    /// re-applied to its socket and limits now. An older table than the
    /// one held (a settle and the poll read concurrently) is dropped.
    fn install_table(&self, t: HostTable) {
        let same_rows = t.same_rows;
        let moved: Vec<String> = {
            let mut cur = self.hosts.table.write();
            let (new, old) = ((t.epoch, t.version), (cur.epoch, cur.version));
            if new < old || (new == old && cur.cursors == t.cursors && cur.throttled == t.throttled) {
                return;
            }
            if same_rows {
                // the rows left out aren't the ones this node holds: the
                // next read sends them
                if new != old {
                    return;
                }
                cur.cursors = t.cursors;
                cur.throttled = t.throttled;
                Vec::new()
            } else {
                let moved = t
                    .rows
                    .values()
                    .filter(|r| cur.rows.get(&r.hostname).is_none_or(|c| c.tier != r.tier || c.extra != r.extra))
                    .map(|r| r.hostname.clone())
                    .collect();
                *cur = t;
                moved
            }
        };
        // what the leader doesn't have yet goes again
        let local: Vec<state::HostRecord> = self.hosts.local.read().values().cloned().collect();
        for r in local {
            if !self.hosts.table.read().rows.contains_key(&r.hostname) {
                self.hosts.propose(&r);
            }
        }
        let tx = self.hosts.changed.get().cloned();
        let notify = move |moved: Vec<String>| {
            for h in moved {
                if let Some(tx) = &tx {
                    let _ = tx.send(h);
                }
            }
        };
        let m = self.manager.get().cloned();
        match m {
            // a host another member admitted is in this node's registry,
            // and so its listings, before its row is applied
            Some(m) if moved.iter().any(|h| m.registry().get(&Host(h.clone())).is_none()) => {
                tokio::spawn(async move {
                    if let Err(e) = m.registry().load().await {
                        tracing::warn!("quorum: loading new hosts: {e:#}");
                    }
                    notify(moved);
                });
            }
            _ => notify(moved),
        }
        if same_rows {
            return;
        }
        let owned = self.hosts.owned(&self.id);
        let mut cur = self.owned.lock();
        if *cur != owned {
            let gained: Vec<&Host> = owned.difference(&cur).collect();
            tracing::info!(id = %self.id, owned = owned.len(), gained = gained.len(), "quorum: hosts owned");
            let set = Arc::new(owned.clone());
            let _ = self.filter.send(Arc::new(move |h: &Host| set.contains(h)));
            *cur = owned;
        }
    }

    /// Reads the leader's table now rather than at the next poll.
    pub async fn refresh_table(&self) {
        if let Some(t) = self.read_table(Duration::from_secs(1)).await {
            self.install_table(t);
        }
    }

    /// Sees a write to `host`'s record through (docs/admin-api.md, "Host
    /// actions"): sends the row to the leader now, not with the next
    /// submit, and reads the leader's table until this node's copy holds
    /// the record as this node last wrote it. False if it didn't within
    /// `within`: no leader answered, or another write to the host won.
    pub async fn settle_host(&self, host: &str, within: Duration) -> bool {
        let until = Instant::now() + within;
        loop {
            let Some(want) = self.hosts.local.read().get(host).cloned() else { return true };
            let held = |t: &HostTable| t.rows.get(host).is_some_and(|r| r.tier == want.tier && r.extra == want.extra);
            if held(&self.hosts.table.read()) {
                return true;
            }
            let left = until.saturating_duration_since(Instant::now());
            if left.is_zero() {
                return false;
            }
            // again each pass: a submit that took it may have gone to a
            // leader that lost its term
            self.hosts.propose(&want);
            let rows = self.shared.take_rows();
            if !rows.is_empty() {
                let submit = self.client.submit_events(Vec::new(), Bytes::new(), rows, self.client.generation());
                let _ = tokio::time::timeout(left, submit).await;
            }
            let left = until.saturating_duration_since(Instant::now());
            if !left.is_zero()
                && let Some(t) = self.read_table(left.min(Duration::from_secs(1))).await
            {
                self.install_table(t);
            }
            if !held(&self.hosts.table.read()) {
                tokio::time::sleep(Duration::from_millis(50).min(until.saturating_duration_since(Instant::now())))
                    .await;
            }
        }
    }

    /// Every `cursor_every`, the acked cursors that moved go out with the
    /// next submit, or on their own if nothing else is going.
    async fn send_cursors(self: Arc<Self>) {
        let mut sent: HashMap<String, i64> = HashMap::new();
        let mut tick = tokio::time::interval(self.setup.cursor_every);
        tick.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
        loop {
            tick.tick().await;
            let Some(m) = self.manager.get().cloned() else { continue };
            let owned = self.owned.lock().clone();
            {
                let mut o = self.shared.outbox.lock();
                for h in &owned {
                    let Some(c) = m.registry().get(h).and_then(|e| e.acked_seq()) else { continue };
                    if c > 0 && sent.get(&h.0).is_none_or(|s| *s != c) {
                        sent.insert(h.0.clone(), c);
                        o.cursors.insert(h.0.clone(), c as u64);
                        o.since.get_or_insert_with(Instant::now);
                    }
                }
            }
            let idle = self.shared.outbox.lock().since.is_some_and(|t| t.elapsed() >= self.setup.cursor_every / 2);
            if idle {
                let (cursors, control, g) = self.shared.take();
                if cursors.is_empty() && control.is_empty() {
                    continue;
                }
                let d = self.client.submit_events(Vec::new(), cursors, control, g).await;
                if d.generation > self.client.generation() {
                    self.rewind(d.generation);
                }
            }
        }
    }

    /// Every member's `status` over the peer protocol, for the dashboard's
    /// Quorum page.
    pub async fn view(&self) -> crate::admin::QuorumView {
        let st = self.qnode.status();
        let mut ids: Vec<String> = st.members.iter().chain(&st.learners).cloned().collect();
        ids.sort();
        ids.dedup();
        let now = chrono::Utc::now().timestamp_millis();
        let asks = ids.iter().map(|id| async move {
            let addr = if *id == self.id { None } else { self.qnode.addr_of(id) };
            let (status, error) = match &addr {
                None if *id == self.id => (serde_json::to_value(self.qnode.status()).ok(), None),
                None => (None, Some("no address".to_string())),
                Some(a) => {
                    match crate::qlog::client::ask(a, "status", Bytes::new(), Duration::from_millis(800)).await {
                        Ok(b) => (serde_json::from_slice(&b).ok(), None),
                        Err(e) => (None, Some(format!("{e:#}"))),
                    }
                }
            };
            crate::admin::QuorumNode {
                node: id.clone(),
                addr: addr.unwrap_or_else(|| self.setup.listen.clone()),
                stale: status.is_none(),
                error,
                reported_ms: now,
                status,
            }
        });
        crate::admin::QuorumView { nodes: futures::future::join_all(asks).await }
    }

    /// `topic` asked of member `id` (this node answers its own).
    pub async fn ask_member(&self, id: &str, topic: &str, body: Bytes) -> Result<Bytes, String> {
        if id == self.id {
            let l = self.hooks.answers.get().ok_or("this node answers nothing yet")?;
            return l.answer(topic, body).await.ok_or_else(|| format!("{id} has no answer to {topic}"));
        }
        let addr = self.qnode.addr_of(id).ok_or_else(|| format!("no address for {id}"))?;
        crate::qlog::client::ask(&addr, topic, body, Duration::from_millis(1500)).await.map_err(|e| format!("{e:#}"))
    }

    /// `topic` asked of every member and learner at once.
    pub async fn ask_all(&self, topic: &str, body: Bytes) -> Vec<(String, Result<Bytes, String>)> {
        let st = self.qnode.status();
        let mut ids: Vec<String> = st.members.iter().chain(&st.learners).cloned().collect();
        ids.sort();
        ids.dedup();
        let asks = ids.into_iter().map(|id| {
            let body = body.clone();
            async move {
                let r = self.ask_member(&id, topic, body).await;
                (id, r)
            }
        });
        futures::future::join_all(asks).await
    }

    /// The qlog admin token, for asks that change something.
    pub fn admin_token(&self) -> Option<&str> {
        self.setup.admin_token.as_deref().filter(|t| !t.is_empty())
    }

    /// Asks the leader to flush now (with the qlog admin token); its status
    /// once F reached the commit index it had.
    pub async fn flush_now(&self) -> anyhow::Result<serde_json::Value> {
        let token = self
            .admin_token()
            .ok_or_else(|| anyhow::anyhow!("flushes on demand are off: start the nodes with --qlog-admin-token"))?;
        let body = serde_json::json!({ "token": token });
        let b = self
            .client
            .ask_leader("leader:flush", serde_json::to_vec(&body)?.into(), Duration::from_secs(40))
            .await
            .map_err(|e| anyhow::anyhow!("{e}"))?;
        let v: serde_json::Value = serde_json::from_slice(&b)?;
        if let Some(e) = v.get("error").and_then(|e| e.as_str()) {
            anyhow::bail!("the leader refused: {e}");
        }
        Ok(v)
    }

    /// A membership change, sent to the leader with the qlog admin token.
    pub async fn change_members(&self, req: crate::admin::QuorumMembersChange) -> anyhow::Result<serde_json::Value> {
        let token =
            self.setup.admin_token.clone().filter(|t| !t.is_empty()).ok_or_else(|| {
                anyhow::anyhow!("membership changes are off: start the nodes with --qlog-admin-token")
            })?;
        let body = serde_json::json!({ "token": token, "members": req.members, "addrs": req.addrs });
        let b = self
            .client
            .ask_leader("members", serde_json::to_vec(&body)?.into(), Duration::from_secs(60))
            .await
            .map_err(|e| anyhow::anyhow!("{e}"))?;
        Ok(serde_json::from_slice(&b)?)
    }

    /// An operator's takedown, made by the leader as an `#account` on the
    /// log.
    pub async fn takedown(&self, did: &str, takedown: bool) -> anyhow::Result<FwdOutcome> {
        let frame = vlatproto::events::account_frame(
            did,
            !takedown,
            takedown.then_some("takendown"),
            &vlatproto::events::now_rfc3339(),
        );
        let item = Item {
            prefix: frame.prefix.into(),
            suffix: frame.suffix.into(),
            meta: encode_item(did, &Host(String::new()), &self.id, 0, &[0xff, takedown as u8]),
        };
        let d = self.client.submit_events(vec![item], Bytes::new(), Bytes::new(), self.client.generation()).await;
        match d.outcomes.into_iter().next() {
            Some(Outcome::Appended(s)) => Ok(FwdOutcome::Appended(s as i64)),
            Some(Outcome::Duplicate) => Ok(FwdOutcome::Duplicate),
            Some(Outcome::Rejected(r)) | Some(Outcome::Retry(r)) => anyhow::bail!("{r}"),
            None => anyhow::bail!("no answer"),
        }
    }
}

/// [`Glue::release_throttled`], from `from`.
pub async fn release_throttled(client: &Client, from: &str, host: &str) -> anyhow::Result<u64> {
    let b = client
        .ask_leader("leader:throttled", Bytes::from(host.to_string()), Duration::from_secs(30))
        .await
        .map_err(anyhow::Error::msg)?;
    let v: serde_json::Value = serde_json::from_slice(&b)?;
    if let Some(e) = v.get("error").and_then(|e| e.as_str()) {
        anyhow::bail!("the leader couldn't list them: {e}");
    }
    let list: Vec<Throttled> = serde_json::from_value(v["accounts"].clone())?;
    let mut released = 0u64;
    let time = vlatproto::events::now_rfc3339();
    for chunk in list.chunks(256) {
        let items = chunk
            .iter()
            .map(|t| {
                let frame = vlatproto::events::account_frame(&t.did, t.active, t.status.as_deref(), &time);
                let mut op = vec![0xff, OP_RELEASE, t.active as u8];
                put_str16(&mut op, t.status.as_deref().unwrap_or(""));
                Item {
                    prefix: frame.prefix.into(),
                    suffix: frame.suffix.into(),
                    meta: encode_item(&t.did, &Host(host.to_string()), from, 0, &op),
                }
            })
            .collect();
        let d = client.submit_events(items, Bytes::new(), Bytes::new(), client.generation()).await;
        for o in d.outcomes {
            match o {
                Outcome::Appended(_) => released += 1,
                Outcome::Duplicate => {}
                Outcome::Rejected(r) | Outcome::Retry(r) => tracing::info!(host, "release-throttled: {r}"),
            }
        }
    }
    Ok(released)
}

impl Glue {
    /// Host discovery as the leader runs it (`run`: start a run of
    /// `source`, or of every enabled source, first).
    pub async fn discovery(&self, run: Option<Option<String>>) -> anyhow::Result<crate::admin::DiscoveryView> {
        let (topic, body) = match run {
            Some(source) => ("leader:discovery-run", serde_json::to_vec(&serde_json::json!({ "source": source }))?),
            None => ("leader:discovery", Vec::new()),
        };
        let b = self.client.ask_leader(topic, body.into(), Duration::from_secs(5)).await.map_err(anyhow::Error::msg)?;
        let mut v: crate::admin::DiscoveryView = serde_json::from_slice(&b)?;
        v.leader = self.qnode.status().leader;
        v.leading = v.leader.as_deref() == Some(self.id.as_str());
        Ok(v)
    }

    /// The PLC export as the leader reports it (None: no export, or the
    /// leader didn't answer).
    pub async fn plc_report(&self) -> Option<crate::admin::fleet::PlcReport> {
        let j = self.plc.as_ref()?;
        if let Some(r) = j.report().await {
            return Some(r);
        }
        let b = self.client.ask_leader("leader:plc", Bytes::new(), Duration::from_secs(2)).await.ok()?;
        serde_json::from_slice::<Option<crate::admin::fleet::PlcReport>>(&b).ok().flatten()
    }

    /// Lifts the relay throttle of every account `host` created that has one
    /// (indigo lifts them when a host's cap is raised): the leader lists
    /// them, and each goes through its log with the `#account` announcing
    /// the account's status without the throttle. Returns how many.
    pub async fn release_throttled(&self, host: &str) -> anyhow::Result<u64> {
        release_throttled(&self.client, &self.id, host).await
    }

    /// Every second, this node's numbers into its status (other members'
    /// dashboards read them there).
    async fn sample(self: Arc<Self>, node: std::sync::Weak<Node>) {
        let mut tick = tokio::time::interval(Duration::from_secs(1));
        let mut cpu = (super::admin::cpu_seconds(), Instant::now());
        loop {
            tick.tick().await;
            let Some(n) = node.upgrade() else { return };
            if let Err(e) = n.state.flush_host_counts(&*self.hosts).await {
                tracing::debug!("host counts: {e:#}");
            }
            let last = n.dash.lock().history.back().cloned();
            let now = (super::admin::cpu_seconds(), Instant::now());
            let cores = (now.0 - cpu.0) / now.1.duration_since(cpu.1).as_secs_f64().max(0.001);
            cpu = now;
            *self.hooks.local.lock() = serde_json::json!({
                "hosts": n.manager.running(),
                "consumers": vlsync_firehose::metrics::FIREHOSE_SUBSCRIBERS.get().max(0),
                "events_in_per_sec": last.as_ref().map_or(0.0, |s| s.events_in),
                "events_out_per_sec": last.as_ref().map_or(0.0, |s| s.events_out),
                "bytes_out_per_sec": last.as_ref().map_or(0.0, |s| s.bytes_out),
                "durable_lag_ms": last.as_ref().map_or(0.0, |s| s.durable_lag_ms),
                "cpu": cores,
                "mem_bytes": vlsync_store::metrics::resident_bytes(),
                "stream_seq": n.serve.head(),
            });
        }
    }

    /// The dashboard's cluster view: the quorum log's members as their
    /// statuses say (role, the hosts they own, their own numbers), hosts
    /// as the leader's table assigns them, and one DID state, the leader's.
    pub async fn cluster_view(&self) -> crate::admin::ClusterView {
        let q = self.view().await;
        let table = self.hosts.table.read().clone();
        let mut owned: HashMap<&str, u32> = HashMap::new();
        for r in table.rows.values() {
            if let Some(o) = &r.owner {
                *owned.entry(o.as_str()).or_default() += 1;
            }
        }
        let mut leader = None;
        let mut epoch = 0;
        let nodes: Vec<crate::admin::NodeView> = q
            .nodes
            .into_iter()
            .map(|n| {
                let st = n.status.clone().unwrap_or(serde_json::Value::Null);
                let role = st["role"].as_str().unwrap_or("unreachable").to_string();
                if role == "leader" {
                    leader = Some(n.node.clone());
                    epoch = st["epoch"].as_u64().unwrap_or(0);
                }
                let listed = |k: &str| st[k].as_array().is_some_and(|m| m.iter().any(|x| x == n.node.as_str()));
                let local = &st["relay"]["node"];
                let f = |k: &str| local[k].as_f64().unwrap_or(0.0);
                crate::admin::NodeView {
                    id: n.node.clone(),
                    addr: n.addr.clone(),
                    version: env!("CARGO_PKG_VERSION").to_string(),
                    rev: String::new(),
                    reachable: !n.stale,
                    healthy: !n.stale && listed("members") && st["intact"].as_bool().unwrap_or(false),
                    learner: listed("learners"),
                    owned_hosts: owned.get(n.node.as_str()).copied().unwrap_or(0),
                    hosts: local["hosts"].as_u64().unwrap_or(0) as u32,
                    consumers: local["consumers"].as_u64().unwrap_or(0) as u32,
                    events_in_per_sec: f("events_in_per_sec"),
                    events_out_per_sec: f("events_out_per_sec"),
                    commit_lag_ms: f("durable_lag_ms"),
                    cpu: f("cpu"),
                    mem_bytes: local["mem_bytes"].as_u64(),
                    role,
                    stale: n.stale,
                    error: n.error.clone(),
                    reported_ms: n.reported_ms,
                    bytes_out_per_sec: f("bytes_out_per_sec"),
                    stream_seq: local["stream_seq"].as_i64().unwrap_or(0),
                }
            })
            .collect();
        let live: HashSet<&str> = nodes.iter().filter(|n| n.healthy).map(|n| n.id.as_str()).collect();
        let unowned =
            table.rows.values().filter(|r| r.owner.as_deref().is_none_or(|o| !live.contains(o))).count() as u32;
        crate::admin::ClusterView {
            leader,
            epoch,
            hosts: table.rows.len() as u32,
            unowned_hosts: unowned,
            last_seq: self.qnode.status().commit as i64,
            nodes,
        }
    }
}

/// (µs, events) submitted to answered, for the dashboard's durable lag.
pub fn latency_totals(n: &Node) -> (u64, u64) {
    (n.quorum.shared.lat_us.load(Ordering::Relaxed), n.quorum.shared.lat_n.load(Ordering::Relaxed))
}

// ---------------------------------------------------------------- start

impl Node {
    /// A node on the quorum log: its peer port, commitlog and firehose, the
    /// pipeline for the hosts the leader gives it, and the leader's half
    /// when it leads. The firehose and every API are ready on return.
    pub async fn start(store: Store, cfg: NodeConfig, q: QuorumSetup) -> anyhow::Result<Arc<Node>> {
        use crate::qlog::{bucket::Bucket, commitlog, emit, node as qn};
        let id = cfg.node_id.clone();
        let identity = Arc::new(crate::identity::IdentityCache::new(
            crate::identity::HttpFetch::new(&cfg.plc_url, cfg.dev_mode),
            cfg.identity.clone(),
        ));
        let patience = Arc::new(super::patience::Patience::default());
        let state = Arc::new(state::StateStore::new(
            super::adapters::VerifyChain,
            Arc::new(super::adapters::CacheIdentity(identity.clone(), patience.clone())),
            state::ApplyConfig::default(),
        ));
        let plc = q.plc_export.clone().map(|c| {
            let st = crate::qlog::bucket::counted(&store, "plc");
            let seeds = crate::plc_seed::SeedReader::new(st.clone());
            if let Some(dir) = q.plc_seeds_dir.clone() {
                let local = crate::plc_seed::local::Local::new(dir, seeds.source());
                let _ = seeds.local.set(local.clone());
                let cache = identity.clone();
                let on_row: crate::plc_seed::local::OnRow =
                    Arc::new(move |did, seed| crate::plc_seed::invalidate_if_stale(&cache, did, seed));
                tokio::spawn(local.run(seeds.clone(), Some(on_row)));
            }
            identity.set_seeder(Arc::new(crate::plc_seed::Seeder {
                seeds: seeds.clone(),
                state: state.clone(),
                ttl: cfg.identity.ttl,
                web_ttl: cfg.identity.web_seed_ttl,
            }));
            crate::plc_seed::job::PlcJob::new(c, st, seeds, identity.clone())
        });
        let bucket = Bucket::new(store.clone());
        let (dir, sync) = match q.durability {
            commitlog::DurabilityMode::Memory => (None, commitlog::SyncMode::Fsync),
            commitlog::DurabilityMode::Sync(s) => (q.commitlog.as_ref(), s),
        };
        let memory = q.memory_bytes.unwrap_or(if dir.is_some() { 64 << 20 } else { 512 << 20 });
        let (durability, recovered): (Arc<dyn qn::Durability>, _) = match dir {
            Some(dir) => {
                let o = commitlog::Options {
                    sync,
                    trust_after_power_loss: q.trust_after_power_loss,
                    segment_bytes: q.commitlog_segment_bytes,
                    retain_bytes: q.disk_retain_bytes,
                    memory_bytes: memory,
                    abort_on_error: true,
                    sync_delay: q.fsync_delay,
                    ..Default::default()
                };
                let (cl, r) = commitlog::CommitLog::open(dir, o)?;
                if q.power_cut_on_usr1 {
                    let cl = cl.clone();
                    let mut sig = tokio::signal::unix::signal(tokio::signal::unix::SignalKind::user_defined1())?;
                    tokio::spawn(async move {
                        sig.recv().await;
                        use rand::Rng;
                        let (keep, garbage) = {
                            let mut rng = rand::thread_rng();
                            let garbage: Vec<u8> = (0..rng.gen_range(0..40)).map(|_| rng.r#gen()).collect();
                            (rng.gen_range(0.0..1.0), garbage)
                        };
                        let r = cl.power_cut(keep, &garbage);
                        eprintln!(
                            "vlrelay: power cut (kept {keep:.2} of the unsynced tail, {} bytes torn): {r:?}",
                            garbage.len()
                        );
                        // as sudden as the power going: no unwinding, no core dump
                        unsafe { libc::kill(libc::getpid(), libc::SIGKILL) };
                    });
                }
                (Arc::new(cl), Some(r))
            }
            None => (Arc::new(qn::MemoryOnly), None),
        };
        let scfg = cfg.serve_config();
        let incarnation = chrono::Utc::now().timestamp_micros() as u64;
        let emitter = emit::Emitter::with_store(&id, incarnation, scfg.ring_bytes, None, bucket.backfill.clone());
        let srv = crate::serve::Serve::counted(store.clone(), scfg.clone());
        let s2 = srv.clone();
        emitter.set_serving(
            scfg.firehose_options(Some(vlsync_firehose::firehose::runtime(cfg.serve_threads))),
            Box::new(move |fh| s2.attach(fh)),
        );
        srv.load_takedowns().await;

        let hooks = RelayHooks::new(state.clone(), Some(identity.clone()), q.clone());
        let mut qc = qn::Config::new(&id, q.peers.clone());
        if !q.members.is_empty() {
            qc.members = q.members.clone();
            qc.members.sort();
            qc.members.dedup();
        }
        qc.heartbeat = q.heartbeat;
        qc.election_timeout = q.election_timeout;
        qc.retain_bytes = memory;
        qc.auto_recover = q.auto_recover;
        qc.flush = Some(crate::qlog::flush::Options {
            interval: q.flush,
            headroom: q.headroom,
            segment_bytes: q.flush_segment_bytes,
            crash: q.crash.clone(),
        });
        qc.hooks = qn::HooksSlot(Some(hooks.clone()));
        qc.admin_token = q.admin_token.clone();
        let listener = tokio::net::TcpListener::bind(&q.listen).await?;
        let local_addr = listener.local_addr()?;
        let qnode =
            qn::Node::start(qc, bucket, listener, emitter, Arc::new(qn::Faults::default()), durability, recovered)
                .await?;
        let _ = hooks.node.set(Arc::downgrade(&qnode));
        if let Some(j) = &plc {
            let _ = hooks.plc.set(j.clone());
            tokio::spawn(j.clone().run(Arc::downgrade(&qnode)));
        }
        let mut nodes: Vec<(String, String)> = q.peers.iter().map(|(k, v)| (k.clone(), v.clone())).collect();
        nodes.push((id.clone(), format!("127.0.0.1:{}", local_addr.port())));
        nodes.sort();
        let client = Client::new(nodes);

        let shared = Arc::new(Shared {
            client: client.clone(),
            outbox: Mutex::new(Outbox::default()),
            rewind: tokio::sync::RwLock::new(()),
            rewinding: AtomicU64::new(0),
            pending_rewind: Mutex::new(HashMap::new()),
            lat_us: AtomicU64::new(0),
            lat_n: AtomicU64::new(0),
            #[cfg(test)]
            hold_rows: Default::default(),
        });
        let hosts = Arc::new(QuorumHosts {
            table: RwLock::new(HostTable::default()),
            local: RwLock::new(BTreeMap::new()),
            shared: shared.clone(),
            sources: Mutex::new(HashMap::new()),
            changed: std::sync::OnceLock::new(),
        });
        let cursors =
            Arc::new(QuorumCursors { shared: shared.clone(), hosts: hosts.clone(), registry: Default::default() });
        let (explicit, cli_hosts) = super::cli_hosts(&cfg)?;
        let mut ucfg = UpstreamConfig::new(cfg.dev_mode);
        ucfg.endpoint = super::endpoint_fn(cfg.dev_mode, explicit);
        ucfg.limits = cfg.upstream_limits.clone();
        ucfg.inflight = cfg.inflight;
        ucfg.event_horizon = cfg.event_horizon;
        let (manager, rx) = Manager::new(ucfg, hosts.clone(), Some(cursors.clone() as Arc<dyn upstream::CursorSource>));
        let _ = cursors.registry.set(manager.registry().clone());
        let crawler = upstream::Crawler::new(manager.clone(), upstream::CrawlPolicy::default());
        {
            let hosts = hosts.clone();
            crawler.set_provenance(Arc::new(move |h: &Host, src: &str| hosts.note_source(&h.0, src)));
        }
        let policy = cfg.policy.as_ref().map(|p| {
            let raw: Arc<dyn state::HostStore> = hosts.clone();
            super::policy::PolicyHooks::new(p.0.clone(), state.clone(), raw, cfg.dev_mode)
        });
        if let Some(h) = &policy {
            let _ = hosts.changed.set(h.changed_sender());
            h.install(&manager, &crawler, &identity);
            h.load().await?;
        }
        if let Some(h) = &policy {
            let feed = Arc::new(crate::discovery::Feed::default());
            if let Some(j) = &plc {
                *j.feed.lock() = Some(feed.clone());
            }
            let st = crate::qlog::bucket::counted(&store, "discovery");
            let d = crate::discovery::DiscoveryJob::new(h.engine.clone(), crawler.clone(), st, feed);
            let _ = hooks.discovery.set(d.clone());
            tokio::spawn(d.run(Arc::downgrade(&qnode)));
        }
        let (filter_tx, filter_rx) = watch::channel::<HostFilter>(Arc::new(|_: &Host| false));
        let glue = Arc::new_cyclic(|w: &std::sync::Weak<Glue>| {
            let _ = w;
            Glue {
                id: id.clone(),
                qnode: qnode.clone(),
                client: client.clone(),
                hooks: hooks.clone(),
                hosts: hosts.clone(),
                shared: shared.clone(),
                setup: q.clone(),
                plc: plc.clone(),
                patience: patience.clone(),
                manager: std::sync::OnceLock::new(),
                filter: filter_tx,
                owned: Mutex::new(HashSet::new()),
            }
        });
        let _ = glue.manager.set(manager.clone());
        let owner = QuorumOwner::start(&id, shared.clone(), Arc::downgrade(&glue));
        let cli_tier = cfg.cli_host_tier;
        let node = Node::assemble(
            cfg,
            store,
            manager.clone(),
            crawler,
            state,
            identity,
            srv,
            owner,
            policy.clone(),
            glue.clone(),
            rx,
        )?;
        // nothing is read until the leader gives this node its hosts
        manager.set_filter(Arc::new(|_: &Host| false)).await?;
        manager.follow_filter(filter_rx);
        manager.start().await?;
        for h in cli_hosts {
            hosts.note_source(&h.0, "cli");
            manager.admit(&h, cli_tier).await?;
        }
        if let Some(h) = &policy {
            h.spawn();
        }
        tokio::spawn(glue.clone().poll_hosts());
        tokio::spawn(glue.clone().send_cursors());
        tokio::spawn(glue.clone().sample(Arc::downgrade(&node)));
        tracing::info!(node = %id, listen = %local_addr, "quorum log node started");
        Ok(node)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::qlog::client::test_frame;
    use crate::qlog::tests::{Cluster, ConfigFn, config};
    use crate::state::tests::MapIdentity;
    use crate::verify::{Verified, VerifiedKind};
    use vlatproto::cid::Cid;
    use vlatproto::tid::Tid;

    const HOST: &str = "h0";

    fn cid(did: &str, k: u64, what: &str) -> Cid {
        Cid::dag_cbor(format!("{did}/{k}/{what}").as_bytes())
    }

    /// Event `k` of `did`'s chain, read by `from`; its frame names it
    /// `{did}#{k}`.
    fn commit(did: &str, k: u64, from: &str) -> Item {
        let v = Verified {
            kind: VerifiedKind::Commit,
            did: did.to_string(),
            rev: Tid(1_000_000 + k),
            commit: cid(did, k, "c"),
            data: cid(did, k, "d"),
            prev_data: (k > 0).then(|| cid(did, k - 1, "d")),
            created: k == 0,
        };
        let c = Checked {
            did: did.to_string(),
            host: Host(HOST.into()),
            upstream_seq: 0,
            kind: CheckedKind::Commit(v),
            frame: Bytes::new(),
            span: crate::event::SeqSpan { start: 0, end: 0 },
            received: Instant::now(),
            first_sighting: true,
            fence: None,
        };
        let (prefix, suffix) = test_frame(&format!("{did}#{k}"), 32, 0);
        Item { prefix, suffix, meta: encode_item(did, &c.host, from, 0, &super::super::forward::encode_meta(&c)) }
    }

    fn identity(did: &str, from: &str) -> Item {
        let c = Checked {
            did: did.to_string(),
            host: Host(HOST.into()),
            upstream_seq: 0,
            kind: CheckedKind::Identity,
            frame: Bytes::new(),
            span: crate::event::SeqSpan { start: 0, end: 0 },
            received: Instant::now(),
            first_sighting: true,
            fence: None,
        };
        let (prefix, suffix) = test_frame(&format!("{did}#identity"), 32, 0);
        Item { prefix, suffix, meta: encode_item(did, &c.host, from, 0, &super::super::forward::encode_meta(&c)) }
    }

    fn cluster_cfg(ident: Arc<MapIdentity>, slow_us: u64) -> ConfigFn {
        cluster_cfg_with(ident, slow_us, |_| {})
    }

    fn cluster_cfg_with(
        ident: Arc<MapIdentity>,
        slow_us: u64,
        tweak: impl Fn(&mut QuorumSetup) + Send + Sync + 'static,
    ) -> ConfigFn {
        cluster_cfg_gated(ident, slow_us, None, tweak)
    }

    fn cluster_cfg_gated(
        ident: Arc<MapIdentity>,
        slow_us: u64,
        gate: Option<Arc<dyn state::AccountGate>>,
        tweak: impl Fn(&mut QuorumSetup) + Send + Sync + 'static,
    ) -> ConfigFn {
        cluster_cfg_hooked(ident, slow_us, gate, tweak, |_, _| {})
    }

    fn cluster_cfg_hooked(
        ident: Arc<MapIdentity>,
        slow_us: u64,
        gate: Option<Arc<dyn state::AccountGate>>,
        tweak: impl Fn(&mut QuorumSetup) + Send + Sync + 'static,
        on_hooks: impl Fn(&str, &Arc<RelayHooks>) + Send + Sync + 'static,
    ) -> ConfigFn {
        Arc::new(move |id: &str, addrs: &HashMap<String, String>| {
            let mut c = config(id, addrs);
            c.flush = Some(crate::qlog::flush::Options {
                interval: Duration::from_millis(300),
                headroom: 10_000_000,
                segment_bytes: 1 << 20,
                crash: None,
            });
            let state = Arc::new(state::StateStore::new(
                super::super::adapters::VerifyChain,
                ident.clone(),
                state::ApplyConfig::default(),
            ));
            if let Some(g) = &gate {
                state.set_account_gate(g.clone());
            }
            let mut q = QuorumSetup::new("127.0.0.1:0");
            q.host_poll = Duration::from_millis(100);
            q.host_failover = Duration::from_secs(2);
            q.retain_horizon = None;
            tweak(&mut q);
            let h = RelayHooks::new(state, None, q);
            h.slow_us.store(slow_us, Ordering::Relaxed);
            on_hooks(id, &h);
            c.hooks = crate::qlog::node::HooksSlot(Some(h));
            c
        })
    }

    async fn owner(client: &Client) -> String {
        let row = HostRow {
            hostname: HOST.into(),
            tier: state::Tier::Trusted,
            first_seen: 1,
            owner: None,
            source: None,
            extra: Default::default(),
        };
        let control: Bytes = serde_json::to_vec(&Control { rows: vec![row] }).unwrap().into();
        let t = Instant::now();
        loop {
            client.submit_events(Vec::new(), Bytes::new(), control.clone(), 0).await;
            if let Ok(b) = client.ask_leader("leader:hosts", Bytes::new(), Duration::from_secs(1)).await
                && let Ok(t) = serde_json::from_slice::<HostTable>(&b)
                && let Some(o) = t.rows.get(HOST).and_then(|r| r.owner.clone())
            {
                return o;
            }
            assert!(t.elapsed() < Duration::from_secs(10), "the host never got an owner");
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
    }

    /// Each DID's events, in its order, every one exactly once, in every
    /// node's stream; and the state at F holds each DID's last record.
    fn check_chains(c: &Cluster, last: &HashMap<String, u64>) {
        let mut by: HashMap<(String, String), Vec<u64>> = HashMap::new();
        for (stream, _, data) in c.emitted.lock().iter() {
            let Some((_, name, _)) = crate::qlog::client::parse_test_frame(data) else { continue };
            let (did, k) = name.rsplit_once('#').unwrap();
            by.entry((stream.clone(), did.to_string())).or_default().push(k.parse().unwrap());
        }
        assert!(!by.is_empty());
        // each node's latest incarnation runs to the end; a killed one stops
        let mut newest: HashMap<String, u64> = HashMap::new();
        for (stream, _) in by.keys() {
            let (n, i) = stream.split_once('#').unwrap();
            let e = newest.entry(n.to_string()).or_default();
            *e = (*e).max(i.parse().unwrap());
        }
        for ((stream, did), ks) in &by {
            let first = ks[0];
            for (i, k) in ks.iter().enumerate() {
                assert_eq!(*k, first + i as u64, "{stream} {did}: events {ks:?}");
            }
            // a restarted node's stream starts where its log does
            if stream.ends_with("#1") {
                assert_eq!(first, 0, "{stream} {did} starts at {first}");
            }
            let (n, i) = stream.split_once('#').unwrap();
            if newest[n] == i.parse::<u64>().unwrap() {
                assert_eq!(*ks.last().unwrap() + 1, last[did], "{stream} {did} ends early: {ks:?}");
            }
        }
    }

    /// Past its cap every new account is created throttled.
    struct ThrottleNew;
    impl state::AccountGate for ThrottleNew {
        fn admit_account(&self, _host: &str, _did: &str, how: state::Arrival) -> state::NewAccount {
            match how {
                state::Arrival::FirstCommit { .. } => state::NewAccount::Admit,
                _ => state::NewAccount::Throttle,
            }
        }
    }

    async fn submit_one(client: &Client, item: Item) -> Outcome {
        let t = Instant::now();
        loop {
            let d = client.submit_events(vec![item.clone()], Bytes::new(), Bytes::new(), 0).await;
            match d.outcomes.into_iter().next() {
                Some(Outcome::Retry(r)) => assert!(t.elapsed() < Duration::from_secs(10), "retrying: {r}"),
                Some(o) => return o,
                None => {}
            }
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
    }

    async fn throttled(client: &Client) -> Vec<String> {
        let b = client.ask_leader("leader:throttled", Bytes::from(HOST), Duration::from_secs(5)).await.unwrap();
        let v: serde_json::Value = serde_json::from_slice(&b).unwrap();
        let list: Vec<Throttled> = serde_json::from_value(v["accounts"].clone()).unwrap();
        let mut dids: Vec<String> = list.into_iter().map(|t| t.did).collect();
        dids.sort();
        dids
    }

    /// Accounts created throttled, logged (their `#identity` carried the
    /// record) or only in the leader's memory (a first commit, dropped):
    /// the leader lists the host's throttled accounts across a takeover
    /// (the logged ones), a release lifts each through the log with an
    /// `#account`, and their commits are taken again.

    #[test]
    fn operator_entries_name_their_change() {
        use crate::admin::changes::ChangeKind;
        let did = crate::state::tests::plc(3);
        let ext = |kind| Ext { kind, key_changed: false, host: String::new(), useq: 0, did: did.clone() };
        let mut rec = Record::new(state::HostKey::of("pds.example.com"), 1);
        rec.relay_takedown = true;
        let m = Meta { writes: vec![(Bytes::from(state::record::did_key(&did)), rec.encode())], ext: Bytes::new() };
        let (k, id, hint) = logged_change(&ext(KIND_TAKEDOWN), &m).unwrap();
        assert_eq!((k, id.as_str(), &hint["takedown"]), (ChangeKind::Takedown, did.as_str(), &serde_json::json!(true)));
        let m = Meta {
            writes: vec![(throttled_key("pds.example.com", &did), Bytes::from_static(b"0"))],
            ext: Bytes::new(),
        };
        let (k, _, hint) = logged_change(&ext(KIND_RELEASE), &m).unwrap();
        assert_eq!((k, &hint["host"]), (ChangeKind::Account, &serde_json::json!("pds.example.com")));
        assert!(logged_change(&ext(KIND_COMMIT), &m).is_none());
    }

    /// A takedown made through one node reaches every member's change feed
    /// as its entry commits there, versioned by the entry's seq.
    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn committed_takedowns_reach_every_members_change_feed() {
        use crate::admin::changes::{ChangeFeed, ChangeKind};
        let ident = MapIdentity::new();
        let did = crate::state::tests::plc(1);
        ident.set(&did, HOST, 1);
        let feeds: Arc<Mutex<HashMap<String, Arc<ChangeFeed>>>> = Default::default();
        let f2 = feeds.clone();
        let cfg = cluster_cfg_hooked(
            ident,
            0,
            None,
            |_| {},
            move |id, h| {
                let f = ChangeFeed::new(id);
                let _ = h.changes.set(f.clone());
                f2.lock().insert(id.to_string(), f);
            },
        );
        let c = Cluster::with_cfg(3, None, Some(cfg), 64 << 20).await;
        c.wait_leader(Duration::from_secs(5)).await;
        let client = c.client();
        let from = owner(&client).await;
        assert!(matches!(submit_one(&client, identity(&did, &from)).await, Outcome::Appended(_)));
        let operator = |takedown: bool| {
            let frame = vlatproto::events::account_frame(
                &did,
                !takedown,
                takedown.then_some("takendown"),
                &vlatproto::events::now_rfc3339(),
            );
            Item {
                prefix: frame.prefix.into(),
                suffix: frame.suffix.into(),
                meta: encode_item(&did, &Host(String::new()), &from, 0, &[0xff, takedown as u8]),
            }
        };
        let mut seqs = Vec::new();
        for takedown in [true, false] {
            match submit_one(&client, operator(takedown)).await {
                Outcome::Appended(s) => seqs.push((s, takedown)),
                o => panic!("{o:?}"),
            }
        }
        let t = Instant::now();
        loop {
            let got: Vec<(String, Vec<(String, serde_json::Value)>)> = feeds
                .lock()
                .iter()
                .map(|(id, f)| {
                    let es = f
                        .recent()
                        .into_iter()
                        .filter(|e| e.kind == ChangeKind::Takedown && e.id == did)
                        .map(|e| (e.version, e.hint.unwrap()["takedown"].clone()))
                        .collect();
                    (id.clone(), es)
                })
                .collect();
            let want: Vec<(String, serde_json::Value)> =
                seqs.iter().map(|(s, t)| (s.to_string(), serde_json::json!(t))).collect();
            if got.len() == 3 && got.iter().all(|(_, es)| *es == want) {
                break;
            }
            assert!(t.elapsed() < Duration::from_secs(10), "{got:?}, want {want:?} on each");
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
        c.shutdown();
    }
    /// A member that holds the leader's (epoch, version) reads the cursors
    /// alone; one behind, or an ask without a version, gets the rows.
    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn the_host_table_leaves_out_rows_a_member_holds() {
        let c = Cluster::with_cfg(3, None, Some(cluster_cfg(MapIdentity::new(), 0)), 64 << 20).await;
        c.wait_leader(Duration::from_secs(5)).await;
        let client = c.client();
        owner(&client).await;
        let ask = |have: Option<(u64, u64)>| {
            let client = client.clone();
            async move {
                let body = have.map(|have| Bytes::from(serde_json::to_vec(&HostsAsk { have }).unwrap()));
                let b = client.ask_leader("leader:hosts", body.unwrap_or_default(), Duration::from_secs(5)).await;
                serde_json::from_slice::<HostTable>(&b.unwrap()).unwrap()
            }
        };
        let full = ask(None).await;
        assert!(!full.same_rows && full.rows.contains_key(HOST));
        let same = ask(Some((full.epoch, full.version))).await;
        assert!(same.same_rows && same.rows.is_empty(), "{same:?}");
        assert_eq!((same.epoch, same.version), (full.epoch, full.version));
        let behind = ask(Some((full.epoch, full.version - 1))).await;
        assert!(!behind.same_rows && behind.rows.contains_key(HOST));
        c.shutdown();
    }

    /// What a member's poll of a 3,400-host table costs, rows carrying
    /// policy fields: the whole table through JSON (as every poll was) and
    /// the cursors alone (an unchanged version over the wire, or the
    /// leader's own member). `cargo test --profile dev-release
    /// host_table_poll_cost -- --ignored --nocapture`.
    #[test]
    #[ignore]
    fn host_table_poll_cost() {
        let hosts = 3_400;
        let mut t = HostTable { epoch: 7, version: 1234, ..Default::default() };
        for n in 0..hosts {
            let h = format!("pds-{n}.host.example.com");
            let trail: Vec<serde_json::Value> = (0..8)
                .map(|k| {
                    serde_json::json!({"at": 1_759_000_000_000u64 + k, "by": "operator@example.com",
                        "action": "set-tier", "tier": "throttled", "note": "spam wave from fresh accounts, see case 1234"})
                })
                .collect();
            let mut extra = serde_json::Map::new();
            extra.insert("operator_throttle".into(), serde_json::json!({"events_per_sec": 5.0, "until": 0}));
            extra.insert("account_cap".into(), serde_json::json!(10_000));
            extra.insert("restore_tier".into(), serde_json::json!("default"));
            extra.insert("actions".into(), serde_json::Value::Array(trail));
            t.rows.insert(
                h.clone(),
                HostRow {
                    hostname: h.clone(),
                    tier: state::Tier::Default,
                    first_seen: 1_700_000_000,
                    owner: Some("relay-1".into()),
                    source: Some("listHosts".into()),
                    extra,
                },
            );
            t.cursors.insert(h, 123_456_789);
        }
        let n = 50;
        let time = |f: &dyn Fn() -> usize| {
            let t0 = Instant::now();
            let mut bytes = 0;
            for _ in 0..n {
                bytes = std::hint::black_box(f());
            }
            (t0.elapsed() / n, bytes)
        };
        let (whole, whole_b) = time(&|| {
            // the leader cloned its table under the term's lock first
            let b = serde_json::to_vec(&t.clone()).unwrap();
            let back: HostTable = serde_json::from_slice(&b).unwrap();
            assert_eq!(back.rows.len(), hosts);
            b.len()
        });
        let (wire, wire_b) = time(&|| {
            let p = HostTable {
                epoch: t.epoch,
                version: t.version,
                rows: BTreeMap::new(),
                cursors: t.cursors.clone(),
                throttled: t.throttled.clone(),
                same_rows: true,
            };
            let b = serde_json::to_vec(&p).unwrap();
            let back: HostTable = serde_json::from_slice(&b).unwrap();
            assert!(back.same_rows);
            b.len()
        });
        let (local, _) = time(&|| t.cursors.clone().len());
        eprintln!(
            "POLL hosts={hosts} whole_json={whole:?} ({} KiB) cursors_json={wire:?} ({} KiB) local={local:?}; \
             at 2 polls/s: {:.1}% / {:.2}% / {:.3}% of a core",
            whole_b >> 10,
            wire_b >> 10,
            whole.as_secs_f64() * 200.0,
            wire.as_secs_f64() * 200.0,
            local.as_secs_f64() * 200.0,
        );
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn release_throttled_lifts_a_hosts_throttled_accounts_through_the_log() {
        let ident = MapIdentity::new();
        let logged: Vec<String> = (0..4).map(crate::state::tests::plc).collect();
        let dropped: Vec<String> = (4..7).map(crate::state::tests::plc).collect();
        for d in logged.iter().chain(&dropped) {
            ident.set(d, HOST, 1);
        }
        let gate: Arc<dyn state::AccountGate> = Arc::new(ThrottleNew);
        let mut c = Cluster::with_cfg(3, None, Some(cluster_cfg_gated(ident, 0, Some(gate), |_| {})), 64 << 20).await;
        c.wait_leader(Duration::from_secs(5)).await;
        let client = c.client();
        let from = owner(&client).await;
        for d in &logged {
            assert!(matches!(submit_one(&client, identity(d, &from)).await, Outcome::Appended(_)), "{d}");
        }
        for d in &dropped {
            let o = submit_one(&client, commit(d, 0, &from)).await;
            assert!(matches!(&o, Outcome::Rejected(r) if r.contains("hrottled")), "{d}: {o:?}");
        }
        let mut all: Vec<String> = logged.iter().chain(&dropped).cloned().collect();
        all.sort();
        assert_eq!(throttled(&client).await, all);

        // a takeover keeps what the log holds; the dropped commits' throttle
        // was the old leader's memory
        let l = c.wait_leader(Duration::from_secs(5)).await;
        c.kill(&l);
        c.start(&l).await;
        c.wait_leader(Duration::from_secs(5)).await;
        let from = owner(&client).await;
        let mut want = logged.clone();
        want.sort();
        let t = Instant::now();
        while throttled(&client).await != want {
            assert!(t.elapsed() < Duration::from_secs(10), "after the takeover: {:?}", throttled(&client).await);
            tokio::time::sleep(Duration::from_millis(100)).await;
        }
        assert_eq!(release_throttled(&client, &from, HOST).await.unwrap(), logged.len() as u64);
        assert!(throttled(&client).await.is_empty());
        assert_eq!(release_throttled(&client, &from, HOST).await.unwrap(), 0, "a second release finds none");

        // the dropped ones are new to this leader: throttled again, released
        for d in &dropped {
            assert!(matches!(submit_one(&client, commit(d, 0, &from)).await, Outcome::Rejected(_)));
        }
        assert_eq!(release_throttled(&client, &from, HOST).await.unwrap(), dropped.len() as u64);
        for d in logged.iter().chain(&dropped) {
            let o = submit_one(&client, commit(d, 0, &from)).await;
            assert!(matches!(o, Outcome::Appended(_)), "{d}'s commit after the release: {o:?}");
        }
        c.converge(Duration::from_secs(15)).await;
        let accounts = c
            .emitted
            .lock()
            .iter()
            .filter(|(s, _, data)| s.starts_with(&from) && data.windows(8).any(|w| w == b"#account"))
            .count();
        assert_eq!(accounts, logged.len() + dropped.len(), "one #account per release");
        c.shutdown();
    }

    /// Hosts owners submit chains to the leader through kill -9s of the
    /// leader, with every batch sent twice at once now and then (a resend
    /// racing the original): no event is lost, repeated or reordered on any
    /// node, and the flushed state is the log replayed.
    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn the_leader_admits_each_dids_events_once_and_in_order() {
        let ident = MapIdentity::new();
        let dids: Vec<String> = (0..24).map(crate::state::tests::plc).collect();
        for d in &dids {
            ident.set(d, HOST, 1);
        }
        let o = crate::qlog::commitlog::Options {
            segment_bytes: 1 << 20,
            retain_bytes: 64 << 20,
            memory_bytes: 1 << 20,
            ..crate::qlog::commitlog::Options::default()
        };
        let mut c = Cluster::with_cfg(
            3,
            Some((tempfile::tempdir().unwrap(), o)),
            Some(cluster_cfg(ident.clone(), 20_000)),
            64 << 20,
        )
        .await;
        c.wait_leader(Duration::from_secs(5)).await;
        let client = c.client();
        let from = owner(&client).await;
        let next: Arc<Mutex<HashMap<String, u64>>> =
            Arc::new(Mutex::new(dids.iter().map(|d| (d.clone(), 0)).collect()));
        let stop = Arc::new(std::sync::atomic::AtomicBool::new(false));
        let mut tasks = Vec::new();
        for (w, group) in dids.chunks(6).enumerate() {
            let (client, next, stop, group, from) =
                (client.clone(), next.clone(), stop.clone(), group.to_vec(), from.clone());
            tasks.push(tokio::spawn(async move {
                let mut round = 0u64;
                while !stop.load(Ordering::Acquire) {
                    round += 1;
                    let items: Vec<Item> = {
                        let n = next.lock();
                        group.iter().map(|d| commit(d, n[d], &from)).collect()
                    };
                    // now and then a second copy races the first, and the
                    // next batch goes as soon as either answers (a resend
                    // whose original is still deciding)
                    let d = if round % 3 == w as u64 % 3 {
                        let (tx, mut rx) = mpsc::unbounded_channel();
                        for copy in [items.clone(), items] {
                            let (client, tx) = (client.clone(), tx.clone());
                            tokio::spawn(async move {
                                let _ = tx.send(client.submit_events(copy, Bytes::new(), Bytes::new(), 0).await);
                            });
                        }
                        let first = rx.recv().await.unwrap();
                        first.outcomes
                    } else {
                        client.submit_events(items, Bytes::new(), Bytes::new(), 0).await.outcomes
                    };
                    let mut n = next.lock();
                    for (did, o) in group.iter().zip(d) {
                        match o {
                            Outcome::Appended(_) | Outcome::Duplicate => *n.get_mut(did).unwrap() += 1,
                            Outcome::Retry(_) => {}
                            Outcome::Rejected(r) => panic!("{did}: rejected: {r}"),
                        }
                    }
                }
            }));
        }
        for _ in 0..2 {
            tokio::time::sleep(Duration::from_millis(1500)).await;
            let l = c.wait_leader(Duration::from_secs(5)).await;
            c.kill(&l);
            tokio::time::sleep(Duration::from_millis(300)).await;
            c.start(&l).await;
        }
        tokio::time::sleep(Duration::from_millis(1500)).await;
        stop.store(true, Ordering::Release);
        for t in tasks {
            tokio::time::timeout(Duration::from_secs(30), t).await.expect("a submitter is stuck").unwrap();
        }
        c.converge(Duration::from_secs(15)).await;
        let last = next.lock().clone();
        assert!(last.values().all(|k| *k > 5), "too little got through: {last:?}");
        check_chains(&c, &last);
        let r = c.finish(&[]);
        assert!(r.ok && r.holes == 0, "{r:#?}");
        // a flush covers everything, and the state at F is the log replayed:
        // each DID's record at its last event
        let l = c.wait_leader(Duration::from_secs(5)).await;
        let top = c.nodes[&l].node.status().commit;
        let t = Instant::now();
        loop {
            c.nodes[&l].node.flush.request(top);
            if crate::qlog::flush::read_manifest(&c.store).await.unwrap().is_some_and(|(m, _)| m.flushed >= top) {
                break;
            }
            assert!(t.elapsed() < Duration::from_secs(15), "no flush to {top}");
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
        let v = crate::qlog::flush::verify(&c.store).await.unwrap();
        assert!(v.ok, "{v:#?}");
        assert_eq!(v.relay_entries, v.entries);
        let (m, _) = crate::qlog::flush::read_manifest(&c.store).await.unwrap().unwrap();
        let (kv, _) = crate::qlog::state::read_checkpoint(&c.store, m.state.as_ref().unwrap()).await.unwrap();
        for d in &dids {
            let rec = Record::decode(&kv[&Bytes::from(state::record::did_key(d))]).unwrap();
            assert_eq!(rec.chain.unwrap().rev, Tid(1_000_000 + last[d] - 1), "{d}");
        }
        assert!(kv.contains_key(&host_key(HOST)), "the host table isn't in the state");
        c.shutdown();
    }

    /// The leader deletes segments past the horizon on its own, with the
    /// same re-checks as `qlog retain --apply`, and what's left still
    /// verifies.
    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn the_leader_runs_bucket_retention_on_a_loop() {
        let ident = MapIdentity::new();
        let dids: Vec<String> = (0..8).map(crate::state::tests::plc).collect();
        for d in &dids {
            ident.set(d, HOST, 1);
        }
        let cfg = cluster_cfg_with(ident, 0, |q| {
            q.retain_horizon = Some(Duration::from_millis(500));
            q.retain_every = Duration::from_millis(400);
        });
        let c = Cluster::with_cfg(3, None, Some(cfg), 64 << 20).await;
        c.wait_leader(Duration::from_secs(5)).await;
        let client = c.client();
        let from = owner(&client).await;
        let mut next: HashMap<String, u64> = dids.iter().map(|d| (d.clone(), 0)).collect();
        let t = Instant::now();
        while t.elapsed() < Duration::from_secs(4) {
            let items: Vec<Item> = dids.iter().map(|d| commit(d, next[d], &from)).collect();
            let r = client.submit_events(items, Bytes::new(), Bytes::new(), 0).await;
            for (d, o) in dids.iter().zip(r.outcomes) {
                if matches!(o, Outcome::Appended(_) | Outcome::Duplicate) {
                    *next.get_mut(d).unwrap() += 1;
                }
            }
        }
        let t = Instant::now();
        loop {
            let pruned = crate::qlog::retain::pruned_seq(&c.store).await.unwrap();
            if pruned > 0 {
                break;
            }
            assert!(t.elapsed() < Duration::from_secs(10), "retention never ran");
            tokio::time::sleep(Duration::from_millis(100)).await;
        }
        let l = c.wait_leader(Duration::from_secs(5)).await;
        let st = c.nodes[&l].node.status();
        let relay = st.relay.unwrap();
        assert!(relay["retain_runs"].as_u64().unwrap() > 0 && relay["retain_deleted"].as_u64().unwrap() > 0, "{relay}");
        let v = crate::qlog::flush::verify(&c.store).await.unwrap();
        assert!(v.ok && v.pruned > 0, "{v:#?}");
        // the requests went to retention's own counter
        let req = crate::qlog::bucket::requests();
        assert!(req.by_purpose.keys().any(|p| p.contains("retain")), "{:?}", req.by_purpose.keys());
        c.shutdown();
    }
}
