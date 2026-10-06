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
use vlpds::slots::ShardId;
use vlpds::store::Store;

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
    Event { kind: CheckedKind, first_sighting: bool },
    Takedown(bool),
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
        anyhow::ensure!(r.remaining() >= 2, "short takedown");
        return Ok(ItemMeta { did, host, from, useq, kind: ItemKind::Takedown(r[1] != 0) });
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

/// The host table's state key.
pub const HOST_PREFIX: &[u8] = b"h/";

fn host_key(host: &str) -> Bytes {
    [HOST_PREFIX, host.as_bytes()].concat().into()
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
}

impl HostRow {
    fn record(&self) -> state::HostRecord {
        state::HostRecord::new(&self.hostname, self.tier, self.first_seen)
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

    fn committed_dup(&self, host: &str, useq: i64, did: &str) -> bool {
        self.recent.lock().get(host).and_then(|m| m.get(&useq)).is_some_and(|s| s.contains(did))
    }

    async fn admit_one(&self, term: &Term, it: &Item, m: ItemMeta) -> (Verdict, Option<String>) {
        let ItemMeta { did, host, from, useq, kind } = m;
        let (kind, first_sighting) = match kind {
            ItemKind::Takedown(t) => return (self.takedown(term, &did, t).await, None),
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
                let writes = vec![(Bytes::from(state::record::did_key(&did)), a.record.encode())];
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
                let r = super::state_rejection(&e);
                (
                    Verdict::Answer { outcome: Outcome::Rejected(format!("{}: {}", r.reason, r.detail)), after: None },
                    None,
                )
            }
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
        if !takedown {
            rec.relay_throttled = false;
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
                Some(c) if c.tier == r.tier => continue,
                Some(c) => HostRow { tier: r.tier, ..c },
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
                vlpds::slots::SLOTS,
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
                let mut i = term.inner.lock();
                i.hosts = HostTable { epoch, version: 1, rows, cursors };
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

    fn answer<'a>(&'a self, topic: &'a str, _body: Bytes) -> BoxFuture<'a, Option<Bytes>> {
        Box::pin(async move {
            match topic {
                "leader:hosts" => {
                    let t = self.term.read().clone()?;
                    let table = t.inner.lock().hosts.clone();
                    serde_json::to_vec(&table).ok().map(Bytes::from)
                }
                _ => None,
            }
        })
    }
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
const GIVE_UP: Duration = Duration::from_secs(20);
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
}

impl Shared {
    /// The outbox's cursors and rows, and the generation they're under.
    /// While a rewind runs, cursors stay behind (they may count lost
    /// events) and only rows go.
    fn take(&self) -> (Bytes, Bytes, u64) {
        let guard = self.rewind.try_read();
        let mut o = self.outbox.lock();
        o.since = None;
        let rows = std::mem::take(&mut o.rows);
        let control = if rows.is_empty() {
            Bytes::new()
        } else {
            serde_json::to_vec(&Control { rows }).map(Bytes::from).unwrap_or_default()
        };
        match guard {
            Ok(_g) => (encode_cursors(&std::mem::take(&mut o.cursors)), control, self.client.generation()),
            Err(_) => (Bytes::new(), control, self.client.generation()),
        }
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
}

impl QuorumHosts {
    fn record(&self, h: &str) -> Option<state::HostRecord> {
        let row = self.table.read().rows.get(h).cloned();
        let local = self.local.read().get(h).cloned();
        match (row, local) {
            (Some(r), Some(mut l)) => {
                l.tier = r.tier;
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

    /// A row the table doesn't have yet, or a new tier: rides the next
    /// submit to the leader (again on every poll until the table has it).
    fn propose(&self, rec: &state::HostRecord) {
        let cur = self.table.read().rows.get(&rec.hostname).map(|r| r.tier);
        if cur == Some(rec.tier) {
            return;
        }
        let mut o = self.shared.outbox.lock();
        o.rows.retain(|r| r.hostname != rec.hostname);
        o.rows.push(HostRow {
            hostname: rec.hostname.clone(),
            tier: rec.tier,
            first_seen: rec.first_seen,
            owner: None,
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
        let key = |h: &str| (vlpds::slots::slot_of(h), h.to_string());
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

    /// Reads the leader's host table every `host_poll` and follows it:
    /// the hosts it gives this node are the manager's.
    async fn poll_hosts(self: Arc<Self>) {
        let mut tick = tokio::time::interval(self.setup.host_poll);
        tick.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
        loop {
            tick.tick().await;
            let t = match self.client.ask_leader("leader:hosts", Bytes::new(), Duration::from_secs(1)).await {
                Ok(b) => match serde_json::from_slice::<HostTable>(&b) {
                    Ok(t) => t,
                    Err(e) => {
                        tracing::warn!("quorum: a bad host table: {e}");
                        continue;
                    }
                },
                Err(_) => continue,
            };
            let changed = {
                let cur = self.hosts.table.read();
                (cur.epoch, cur.version) != (t.epoch, t.version) || cur.cursors != t.cursors
            };
            if !changed {
                continue;
            }
            // what the leader doesn't have yet goes again
            {
                let local: Vec<state::HostRecord> = self.hosts.local.read().values().cloned().collect();
                *self.hosts.table.write() = t;
                for r in local {
                    if !self.hosts.table.read().rows.contains_key(&r.hostname) {
                        self.hosts.propose(&r);
                    }
                }
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
        let frame = vlpds::events::account_frame(
            did,
            !takedown,
            takedown.then_some("takendown"),
            &vlpds::events::now_rfc3339(),
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

impl Glue {
    /// Every second, this node's numbers into its status (other members'
    /// dashboards read them there).
    async fn sample(self: Arc<Self>, node: std::sync::Weak<Node>) {
        let mut tick = tokio::time::interval(Duration::from_secs(1));
        let mut cpu = (process_cpu_secs(), Instant::now());
        loop {
            tick.tick().await;
            let Some(n) = node.upgrade() else { return };
            if let Err(e) = n.state.flush_host_counts(&*self.hosts).await {
                tracing::debug!("host counts: {e:#}");
            }
            let last = n.dash.lock().history.back().cloned();
            let now = (process_cpu_secs(), Instant::now());
            let cores = (now.0 - cpu.0) / now.1.duration_since(cpu.1).as_secs_f64().max(0.001);
            cpu = now;
            *self.hooks.local.lock() = serde_json::json!({
                "hosts": n.manager.running(),
                "consumers": vlpds::metrics::FIREHOSE_SUBSCRIBERS.get().max(0),
                "events_in_per_sec": last.as_ref().map_or(0.0, |s| s.events_in),
                "events_out_per_sec": last.as_ref().map_or(0.0, |s| s.events_out),
                "bytes_out_per_sec": last.as_ref().map_or(0.0, |s| s.bytes_out),
                "durable_lag_ms": last.as_ref().map_or(0.0, |s| s.durable_lag_ms),
                "cpu": cores,
                "mem_bytes": process_rss_bytes(),
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
        let host_shards: Vec<Option<String>> = table.rows.values().map(|r| r.owner.clone()).collect();
        let mut leader = None;
        let nodes = q
            .nodes
            .into_iter()
            .map(|n| {
                let st = n.status.clone().unwrap_or(serde_json::Value::Null);
                let role = st["role"].as_str().unwrap_or("unreachable").to_string();
                if role == "leader" {
                    leader = Some(n.node.clone());
                }
                let local = &st["relay"]["node"];
                let f = |k: &str| local[k].as_f64().unwrap_or(0.0);
                let owned = host_shards.iter().filter(|o| o.as_deref() == Some(n.node.as_str())).count() as u32;
                crate::admin::NodeView {
                    id: n.node.clone(),
                    addr: n.addr.clone(),
                    version: env!("CARGO_PKG_VERSION").to_string(),
                    rev: String::new(),
                    reachable: !n.stale,
                    lease_valid: st["members"].as_array().is_some_and(|m| m.iter().any(|x| x == n.node.as_str())),
                    lease_expires_ms: 0,
                    host_shards: owned,
                    did_shards: (role == "leader") as u32,
                    hosts: local["hosts"].as_u64().unwrap_or(0) as u32,
                    consumers: local["consumers"].as_u64().unwrap_or(0) as u32,
                    events_in_per_sec: f("events_in_per_sec"),
                    events_out_per_sec: f("events_out_per_sec"),
                    log_durability_lag_ms: f("durable_lag_ms"),
                    cpu: f("cpu"),
                    mem_bytes: local["mem_bytes"].as_u64().unwrap_or(0),
                    role,
                    stale: n.stale,
                    error: n.error.clone(),
                    reported_ms: n.reported_ms,
                    bytes_out_per_sec: f("bytes_out_per_sec"),
                    stream_seq: local["stream_seq"].as_i64().unwrap_or(0),
                }
            })
            .collect();
        crate::admin::ClusterView {
            nodes,
            host_shards,
            did_shards: vec![leader],
            last_seq: self.qnode.status().commit as i64,
        }
    }
}

fn process_cpu_secs() -> f64 {
    // utime + stime in clock ticks (fields 14 and 15 of /proc/self/stat)
    let s = std::fs::read_to_string("/proc/self/stat").unwrap_or_default();
    let rest = s.rsplit_once(')').map_or("", |(_, r)| r);
    let f: Vec<&str> = rest.split_whitespace().collect();
    let ticks = |i: usize| f.get(i).and_then(|x| x.parse::<f64>().ok()).unwrap_or(0.0);
    (ticks(11) + ticks(12)) / 100.0
}

fn process_rss_bytes() -> u64 {
    let s = std::fs::read_to_string("/proc/self/statm").unwrap_or_default();
    s.split_whitespace().nth(1).and_then(|x| x.parse::<u64>().ok()).unwrap_or(0) * 4096
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
        let state = Arc::new(state::StateStore::new(
            super::adapters::VerifyChain,
            Arc::new(super::adapters::CacheIdentity(identity.clone())),
            state::ApplyConfig::default(),
        ));
        let bucket = Bucket::new(store.clone());
        let memory = q.memory_bytes.unwrap_or(if q.commitlog.is_some() { 64 << 20 } else { 512 << 20 });
        let (durability, recovered): (Arc<dyn qn::Durability>, _) = match &q.commitlog {
            Some(dir) => {
                let o = commitlog::Options {
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
            scfg.firehose_options(Some(vlpds::firehose::runtime(cfg.serve_threads))),
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
        });
        let hosts = Arc::new(QuorumHosts {
            table: RwLock::new(HostTable::default()),
            local: RwLock::new(BTreeMap::new()),
            shared: shared.clone(),
        });
        let cursors =
            Arc::new(QuorumCursors { shared: shared.clone(), hosts: hosts.clone(), registry: Default::default() });
        let (explicit, cli_hosts) = super::cli_hosts(&cfg)?;
        let mut ucfg = UpstreamConfig::new(cfg.dev_mode);
        ucfg.endpoint = super::endpoint_fn(cfg.dev_mode, explicit);
        ucfg.limits = cfg.upstream_limits.clone();
        ucfg.inflight = cfg.inflight;
        let (manager, rx) = Manager::new(ucfg, hosts.clone(), Some(cursors.clone() as Arc<dyn upstream::CursorSource>));
        let _ = cursors.registry.set(manager.registry().clone());
        let crawler = upstream::Crawler::new(manager.clone(), upstream::CrawlPolicy::default());
        let policy = cfg.policy.as_ref().map(|p| {
            let raw: Arc<dyn state::HostStore> = hosts.clone();
            super::policy::PolicyHooks::new(p.0.clone(), state.clone(), raw, cfg.dev_mode)
        });
        if let Some(h) = &policy {
            h.install(&manager, &crawler, &identity);
            h.load().await?;
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
    use vlpds::cid::Cid;
    use vlpds::tid::Tid;

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

    fn cluster_cfg(ident: Arc<MapIdentity>, slow_us: u64) -> ConfigFn {
        cluster_cfg_with(ident, slow_us, |_| {})
    }

    fn cluster_cfg_with(
        ident: Arc<MapIdentity>,
        slow_us: u64,
        tweak: impl Fn(&mut QuorumSetup) + Send + Sync + 'static,
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
            let mut q = QuorumSetup::new("127.0.0.1:0");
            q.host_poll = Duration::from_millis(100);
            q.host_failover = Duration::from_secs(2);
            q.retain_horizon = None;
            tweak(&mut q);
            let h = RelayHooks::new(state, None, q);
            h.slow_us.store(slow_us, Ordering::Relaxed);
            c.hooks = crate::qlog::node::HooksSlot(Some(h));
            c
        })
    }

    async fn owner(client: &Client) -> String {
        let row = HostRow { hostname: HOST.into(), tier: state::Tier::Trusted, first_seen: 1, owner: None };
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
