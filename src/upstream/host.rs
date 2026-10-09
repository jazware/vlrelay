//! Hostnames, tiers and the host registry.

use crate::types::Host;
use parking_lot::{Mutex, RwLock};
use serde::{Deserialize, Serialize};
use std::collections::{BTreeMap, HashMap};
use std::future::Future;
use std::net::IpAddr;
use std::pin::Pin;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicI64, AtomicU8, AtomicU64, Ordering};

#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum Tier {
    Trusted,
    Default,
    New,
    Throttled,
    Suspended,
    Banned,
}

impl Tier {
    pub const ALL: [Tier; 6] =
        [Tier::Trusted, Tier::Default, Tier::New, Tier::Throttled, Tier::Suspended, Tier::Banned];

    /// Suspended and banned hosts keep their registry row but get no socket.
    pub fn connects(self) -> bool {
        !matches!(self, Tier::Suspended | Tier::Banned)
    }

    pub fn as_str(self) -> &'static str {
        match self {
            Tier::Trusted => "trusted",
            Tier::Default => "default",
            Tier::New => "new",
            Tier::Throttled => "throttled",
            Tier::Suspended => "suspended",
            Tier::Banned => "banned",
        }
    }

    pub fn parse(s: &str) -> Option<Tier> {
        Tier::ALL.into_iter().find(|t| t.as_str() == s)
    }

    fn to_u8(self) -> u8 {
        self as u8
    }

    fn from_u8(n: u8) -> Tier {
        Tier::ALL[n as usize]
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum HostStatus {
    /// Registered, no task running (not started yet, or its tier doesn't connect).
    Idle,
    Connecting,
    Active,
    /// Waiting out a backoff after a failed or dropped connection.
    Backoff,
    /// Held off by its own limits (its tier's, a domain rule's or an
    /// operator's throttle): the socket isn't being read.
    Throttled,
    /// Held off by the relay, not by anything about the host: the pipeline
    /// behind it is full ([`HostEntry::backpressure`] says which part).
    Backpressure,
}

impl HostStatus {
    const ALL: [HostStatus; 6] = [
        HostStatus::Idle,
        HostStatus::Connecting,
        HostStatus::Active,
        HostStatus::Backoff,
        HostStatus::Throttled,
        HostStatus::Backpressure,
    ];
}

/// Which part of the relay is full while a host is in [`HostStatus::Backpressure`].
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Backpressure {
    /// The host's own in-flight cap: frames it sent are read and not yet
    /// durable (`--host-inflight-events`, `--host-inflight-mb`).
    InflightFull,
    /// The node's in-flight cap over every host (`--inflight-events`,
    /// `--inflight-mb`).
    NodeInflightFull,
    /// Its fair-queue slot is full: the lanes aren't taking frames, usually
    /// while they wait on identity lookups.
    QueueFull,
    /// The process is over its memory budget (`--ingest-mem-mb`) and the
    /// pipeline holds its share of the in-flight caps.
    MemoryFull,
}

impl Backpressure {
    const ALL: [Backpressure; 4] =
        [Backpressure::InflightFull, Backpressure::NodeInflightFull, Backpressure::QueueFull, Backpressure::MemoryFull];
}

#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(default)]
pub struct ErrorCounters {
    pub connect: u64,
    /// Frames we couldn't parse, unknown error frames, text messages.
    pub protocol: u64,
    pub stalls: u64,
    /// Remote closes and read errors on an established socket.
    pub dropped: u64,
    pub outdated_cursor: u64,
    pub future_cursor: u64,
    pub consumer_too_slow: u64,
    /// Frames whose seq didn't move past the last one on the same socket.
    pub seq_regressions: u64,
}

/// What [`HostStore`] persists per host.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct HostRecord {
    pub hostname: String,
    pub tier: Tier,
    pub status: HostStatus,
    /// Highest upstream seq whose events are all durable: the resume cursor.
    pub acked_seq: Option<i64>,
    pub last_connected_ms: Option<u64>,
    pub admitted_ms: u64,
    pub account_count: u64,
    #[serde(default)]
    pub errors: ErrorCounters,
}

impl HostRecord {
    pub fn new(host: &Host, tier: Tier) -> HostRecord {
        HostRecord {
            hostname: host.0.clone(),
            tier,
            status: HostStatus::Idle,
            acked_seq: None,
            last_connected_ms: None,
            admitted_ms: now_ms(),
            account_count: 0,
            errors: ErrorCounters::default(),
        }
    }
}

pub type StoreFuture<'a, T> = Pin<Box<dyn Future<Output = anyhow::Result<T>> + Send + 'a>>;

/// Where the registry persists. The state workstream backs this with
/// SlateDB; [`MemHostStore`] serves tests and dev.
pub trait HostStore: Send + Sync + 'static {
    fn load(&self) -> StoreFuture<'_, Vec<HostRecord>>;
    /// Upserts by hostname.
    fn put(&self, records: Vec<HostRecord>) -> StoreFuture<'_, ()>;
}

#[derive(Default)]
pub struct MemHostStore {
    rows: Mutex<BTreeMap<String, HostRecord>>,
}

impl MemHostStore {
    pub fn get(&self, hostname: &str) -> Option<HostRecord> {
        self.rows.lock().get(hostname).cloned()
    }
}

impl HostStore for MemHostStore {
    fn load(&self) -> StoreFuture<'_, Vec<HostRecord>> {
        let rows = self.rows.lock().values().cloned().collect();
        Box::pin(async move { Ok(rows) })
    }

    fn put(&self, records: Vec<HostRecord>) -> StoreFuture<'_, ()> {
        let mut rows = self.rows.lock();
        for r in records {
            rows.insert(r.hostname.clone(), r);
        }
        Box::pin(async { Ok(()) })
    }
}

const NO_SEQ: i64 = i64::MIN;

fn opt_seq(n: i64) -> Option<i64> {
    (n != NO_SEQ).then_some(n)
}

/// One host's live state. The hot counters are atomics so the host task
/// never takes a lock per frame.
pub struct HostEntry {
    pub host: Host,
    tier: AtomicU8,
    status: AtomicU8,
    received_seq: AtomicI64,
    acked_seq: AtomicI64,
    pub(crate) frames: AtomicU64,
    pub(crate) bytes: AtomicU64,
    pub(crate) connects: AtomicU64,
    /// The current socket's epoch (see `UpstreamFrame::epoch`).
    epoch: AtomicU64,
    /// Frames read and not yet done.
    pub flow: Arc<super::flow::HostFlow>,
    last_connected_ms: AtomicU64,
    account_count: AtomicU64,
    admitted_ms: u64,
    errors: Mutex<ErrorCounters>,
    dirty: AtomicBool,
    /// Limits from the policy engine, in place of the tier's defaults.
    limits: Mutex<Option<super::limits::TierLimits>>,
    /// Bumped on every tier or limit change; the host task retunes its
    /// buckets when it sees a new one.
    limits_gen: AtomicU64,
    /// When the newest frame was read, and the `time` it carried (unix ms,
    /// 0 for none yet).
    read_at_ms: AtomicI64,
    read_event_ms: AtomicI64,
    /// A [`Backpressure`] as u8; read only while the status says so.
    backpressure: AtomicU8,
    /// When the status last left `backpressure` (unix ms, 0 for never).
    backpressure_left_ms: AtomicI64,
    /// The host's own timeline (`super::clock`), across its sockets.
    clock: Mutex<super::clock::EventClock>,
    /// Until then (unix ms), the next connect skips the cursor and starts
    /// at the host's head.
    head_until_ms: AtomicU64,
}

/// A reader that has waited this long on its socket with nothing to read
/// has everything the host has sent: it isn't behind, whatever the age of
/// the last frame it read.
pub const CAUGHT_UP_MS: i64 = 10_000;

impl HostEntry {
    fn from_record(r: &HostRecord) -> HostEntry {
        HostEntry {
            host: Host(r.hostname.clone()),
            tier: AtomicU8::new(r.tier.to_u8()),
            status: AtomicU8::new(HostStatus::Idle as u8),
            received_seq: AtomicI64::new(r.acked_seq.unwrap_or(NO_SEQ)),
            acked_seq: AtomicI64::new(r.acked_seq.unwrap_or(NO_SEQ)),
            frames: AtomicU64::new(0),
            bytes: AtomicU64::new(0),
            connects: AtomicU64::new(0),
            epoch: AtomicU64::new(0),
            flow: Arc::default(),
            last_connected_ms: AtomicU64::new(r.last_connected_ms.unwrap_or(0)),
            account_count: AtomicU64::new(r.account_count),
            admitted_ms: r.admitted_ms,
            errors: Mutex::new(r.errors.clone()),
            dirty: AtomicBool::new(false),
            limits: Mutex::new(None),
            limits_gen: AtomicU64::new(0),
            read_at_ms: AtomicI64::new(0),
            read_event_ms: AtomicI64::new(0),
            backpressure: AtomicU8::new(0),
            backpressure_left_ms: AtomicI64::new(0),
            head_until_ms: AtomicU64::new(0),
            clock: Mutex::new(Default::default()),
        }
    }

    /// A frame read off the socket, stamped `event_ms` by the host (None:
    /// no usable `time`). Returns the host's clock after it.
    pub(crate) fn note_frame(&self, event_ms: Option<i64>, horizon_ms: i64) -> i64 {
        let now = now_ms() as i64;
        if let Some(ev) = event_ms {
            self.read_event_ms.store(ev, Ordering::Relaxed);
            self.read_at_ms.store(now, Ordering::Relaxed);
        }
        self.clock.lock().on_frame(event_ms, now, horizon_ms)
    }

    /// A limiter pause the reader sat out, which moves the host's clock.
    pub(crate) fn clock_paused(&self, pause: std::time::Duration) {
        self.clock.lock().paused(pause.as_millis() as i64, now_ms() as i64);
    }

    /// The host's own timeline, unix ms (0 before its first frame).
    pub fn clock_ms(&self) -> i64 {
        self.clock.lock().clock_ms()
    }

    /// Host seconds per wall second: 1 when live, more while catching up.
    pub fn pace(&self) -> f64 {
        self.clock.lock().pace(now_ms() as i64)
    }

    /// The newest frame's age when it was read, or 0 once the reader has
    /// sat on an empty socket for [`CAUGHT_UP_MS`]. None while not live.
    fn frame_lag(&self, now: i64) -> Option<(i64, i64)> {
        let status = self.status();
        if !matches!(status, HostStatus::Active | HostStatus::Throttled | HostStatus::Backpressure) {
            return None;
        }
        let (at, ev) = (self.read_at_ms.load(Ordering::Relaxed), self.read_event_ms.load(Ordering::Relaxed));
        if at == 0 || (status == HostStatus::Active && now - at >= CAUGHT_UP_MS) {
            return Some((0, at));
        }
        Some(((at - ev).max(0), at))
    }

    /// How far behind the host's own stream the reader is: the newest
    /// frame's age when it was read, plus the time since while the reader
    /// is held back (by its limits or by the relay). A reader that is
    /// waiting on its socket is caught up, so a quiet host reads 0, not the
    /// age of its last frame. PDS clock skew is in it too, so read it in
    /// seconds and minutes, not ms.
    pub fn read_lag_ms(&self) -> Option<i64> {
        let now = now_ms() as i64;
        let (lag, at) = self.frame_lag(now)?;
        let held = match self.status() {
            HostStatus::Throttled | HostStatus::Backpressure if at != 0 => now - at,
            _ => 0,
        };
        Some((lag + held).max(0))
    }

    /// The lag the host answers for (read-lag cases): [`Self::read_lag_ms`]
    /// without the time the relay held the reader back. Time held by the
    /// host's own limits still counts.
    pub fn host_lag_ms(&self) -> Option<i64> {
        let now = now_ms() as i64;
        let (lag, at) = self.frame_lag(now)?;
        let held = if self.status() == HostStatus::Throttled && at != 0 { now - at } else { 0 };
        Some((lag + held).max(0))
    }

    /// When the relay last held this host back (now, while it does).
    pub fn backpressure_at_ms(&self) -> Option<i64> {
        if self.status() == HostStatus::Backpressure {
            return Some(now_ms() as i64);
        }
        let left = self.backpressure_left_ms.load(Ordering::Relaxed);
        (left != 0).then_some(left)
    }

    /// The limits the host task enforces.
    pub fn limits(&self, defaults: &super::limits::Limits) -> super::limits::TierLimits {
        (*self.limits.lock()).unwrap_or_else(|| defaults.for_tier(self.tier()))
    }

    pub fn limits_gen(&self) -> u64 {
        self.limits_gen.load(Ordering::Acquire)
    }

    pub(crate) fn set_limits(&self, l: Option<super::limits::TierLimits>) {
        let mut cur = self.limits.lock();
        if *cur != l {
            *cur = l;
            self.limits_gen.fetch_add(1, Ordering::AcqRel);
        }
    }

    pub fn tier(&self) -> Tier {
        Tier::from_u8(self.tier.load(Ordering::Relaxed))
    }

    pub(crate) fn set_tier(&self, t: Tier) {
        if self.tier.swap(t.to_u8(), Ordering::Relaxed) != t.to_u8() {
            self.limits_gen.fetch_add(1, Ordering::AcqRel);
        }
        self.dirty.store(true, Ordering::Relaxed);
    }

    pub fn status(&self) -> HostStatus {
        HostStatus::ALL[self.status.load(Ordering::Relaxed) as usize]
    }

    pub(crate) fn set_status(&self, s: HostStatus) {
        let was = self.status.swap(s as u8, Ordering::Relaxed);
        if was != s as u8 {
            if was == HostStatus::Backpressure as u8 {
                self.backpressure_left_ms.store(now_ms() as i64, Ordering::Relaxed);
            }
            self.dirty.store(true, Ordering::Relaxed);
        }
    }

    /// Paused by the relay: the reason first, so a reader that sees the
    /// status sees its reason.
    pub(crate) fn set_backpressure(&self, why: Backpressure) {
        self.backpressure.store(why as u8, Ordering::Release);
        self.set_status(HostStatus::Backpressure);
    }

    /// What's full while the status is [`HostStatus::Backpressure`].
    pub fn backpressure(&self) -> Option<Backpressure> {
        (self.status() == HostStatus::Backpressure)
            .then(|| Backpressure::ALL[self.backpressure.load(Ordering::Acquire) as usize])
    }

    /// Last seq read off the socket (not necessarily durable).
    pub fn received_seq(&self) -> Option<i64> {
        opt_seq(self.received_seq.load(Ordering::Relaxed))
    }

    pub(crate) fn set_received_seq(&self, seq: i64) {
        self.received_seq.store(seq, Ordering::Relaxed);
    }

    pub fn acked_seq(&self) -> Option<i64> {
        opt_seq(self.acked_seq.load(Ordering::Relaxed))
    }

    /// Only moves forward: acks may arrive out of order from parallel paths.
    pub(crate) fn ack(&self, seq: i64) {
        if self.acked_seq.fetch_max(seq, Ordering::Relaxed) < seq {
            self.dirty.store(true, Ordering::Relaxed);
        }
    }

    /// The next connect within `for_ms` starts at the host's head, with no
    /// cursor: what it sent before is known to be read already.
    pub fn start_at_head(&self, for_ms: u64) {
        self.head_until_ms.store(now_ms() + for_ms, Ordering::Relaxed);
    }

    /// Whether this connect starts at the head (once). Its acked cursor
    /// starts over, so the new socket's acks set it.
    pub(crate) fn take_start_at_head(&self) -> bool {
        let until = self.head_until_ms.swap(0, Ordering::Relaxed);
        let now = until > now_ms();
        if now {
            self.reset_cursor();
        }
        now
    }

    /// A host that reset its sequence (FutureCursor) starts over, from the
    /// new sequence's first event.
    pub(crate) fn reset_cursor(&self) {
        self.acked_seq.store(0, Ordering::Relaxed);
        self.received_seq.store(0, Ordering::Relaxed);
        self.dirty.store(true, Ordering::Relaxed);
    }

    /// The cursor a host shard's checkpoint holds, taken over from another
    /// node: it replaces ours, which may belong to a sequence the host has
    /// since restarted.
    pub(crate) fn restore_cursor(&self, seq: i64) {
        self.acked_seq.store(seq, Ordering::Relaxed);
        self.received_seq.store(seq, Ordering::Relaxed);
        self.dirty.store(true, Ordering::Relaxed);
    }

    /// The current socket's epoch.
    pub fn epoch(&self) -> u64 {
        self.epoch.load(Ordering::Acquire)
    }

    /// A new socket: returns its epoch.
    pub(crate) fn note_connected(&self) -> u64 {
        let epoch = self.epoch.fetch_add(1, Ordering::AcqRel) + 1;
        self.connects.fetch_add(1, Ordering::Relaxed);
        self.last_connected_ms.store(now_ms(), Ordering::Relaxed);
        self.dirty.store(true, Ordering::Relaxed);
        epoch
    }

    pub(crate) fn set_account_count(&self, n: u64) {
        if self.account_count.swap(n, Ordering::Relaxed) != n {
            self.dirty.store(true, Ordering::Relaxed);
        }
    }

    pub(crate) fn count_error(&self, f: impl FnOnce(&mut ErrorCounters)) {
        f(&mut self.errors.lock());
        self.dirty.store(true, Ordering::Relaxed);
    }

    pub fn errors(&self) -> ErrorCounters {
        self.errors.lock().clone()
    }

    pub fn record(&self) -> HostRecord {
        let last = self.last_connected_ms.load(Ordering::Relaxed);
        HostRecord {
            hostname: self.host.0.clone(),
            tier: self.tier(),
            status: self.status(),
            acked_seq: self.acked_seq(),
            last_connected_ms: (last != 0).then_some(last),
            admitted_ms: self.admitted_ms,
            account_count: self.account_count.load(Ordering::Relaxed),
            errors: self.errors(),
        }
    }

    pub fn view(&self) -> HostView {
        HostView {
            record: self.record(),
            received_seq: self.received_seq(),
            frames: self.frames.load(Ordering::Relaxed),
            bytes: self.bytes.load(Ordering::Relaxed),
            connects: self.connects.load(Ordering::Relaxed),
            read_lag_ms: self.read_lag_ms(),
            host_lag_ms: self.host_lag_ms(),
            backpressure_at_ms: self.backpressure_at_ms(),
            pace: self.pace(),
            inflight_events: self.flow.events() as u64,
            inflight_bytes: self.flow.bytes() as u64,
            paused: self.flow.paused(),
            backpressure: self.backpressure(),
        }
    }
}

/// A host as the console and tests see it: the persisted record plus the
/// live counters that aren't worth persisting.
#[derive(Clone, Debug, Serialize)]
pub struct HostView {
    #[serde(flatten)]
    pub record: HostRecord,
    pub received_seq: Option<i64>,
    pub frames: u64,
    pub bytes: u64,
    pub connects: u64,
    pub read_lag_ms: Option<i64>,
    /// [`HostEntry::host_lag_ms`].
    pub host_lag_ms: Option<i64>,
    /// [`HostEntry::backpressure_at_ms`].
    pub backpressure_at_ms: Option<i64>,
    /// [`HostEntry::pace`].
    pub pace: f64,
    /// Frames (and their bytes) read and not yet durable, rejected or dropped.
    pub inflight_events: u64,
    pub inflight_bytes: u64,
    /// Not read: at its in-flight cap or the node's.
    pub paused: bool,
    /// Set while the status is `backpressure`.
    pub backpressure: Option<Backpressure>,
}

pub struct Registry {
    hosts: RwLock<HashMap<Host, Arc<HostEntry>>>,
    store: Arc<dyn HostStore>,
}

impl Registry {
    pub fn new(store: Arc<dyn HostStore>) -> Registry {
        Registry { hosts: RwLock::new(HashMap::new()), store }
    }

    pub async fn load(&self) -> anyhow::Result<usize> {
        let rows = self.store.load().await?;
        let mut hosts = self.hosts.write();
        for r in &rows {
            hosts.entry(Host(r.hostname.clone())).or_insert_with(|| Arc::new(HostEntry::from_record(r)));
        }
        Ok(rows.len())
    }

    pub fn get(&self, host: &Host) -> Option<Arc<HostEntry>> {
        self.hosts.read().get(host).cloned()
    }

    pub fn len(&self) -> usize {
        self.hosts.read().len()
    }

    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    pub fn all(&self) -> Vec<Arc<HostEntry>> {
        self.hosts.read().values().cloned().collect()
    }

    /// Inserts `host` at `tier` unless present, and persists a new row
    /// before returning so an admission survives a crash. Returns the entry
    /// and whether it was new.
    pub async fn admit(&self, host: &Host, tier: Tier) -> anyhow::Result<(Arc<HostEntry>, bool)> {
        if let Some(e) = self.get(host) {
            return Ok((e, false));
        }
        let rec = HostRecord::new(host, tier);
        self.store.put(vec![rec.clone()]).await?;
        let mut hosts = self.hosts.write();
        if let Some(e) = hosts.get(host) {
            return Ok((e.clone(), false));
        }
        let e = Arc::new(HostEntry::from_record(&rec));
        hosts.insert(host.clone(), e.clone());
        Ok((e, true))
    }

    /// Writes every row changed since the last flush.
    pub async fn flush(&self) -> anyhow::Result<usize> {
        let dirty: Vec<Arc<HostEntry>> =
            self.hosts.read().values().filter(|e| e.dirty.swap(false, Ordering::Relaxed)).cloned().collect();
        if dirty.is_empty() {
            return Ok(0);
        }
        let n = dirty.len();
        let rows = dirty.iter().map(|e| e.record()).collect();
        if let Err(e) = self.store.put(rows).await {
            for d in dirty {
                d.dirty.store(true, Ordering::Relaxed);
            }
            return Err(e);
        }
        Ok(n)
    }
}

pub(crate) fn now_ms() -> u64 {
    std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH).map_or(0, |d| d.as_millis() as u64)
}

#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum HostnameError {
    #[error("hostname is empty")]
    Empty,
    #[error("hostname is too long")]
    TooLong,
    #[error("hostname must be a bare host, without a path, query or credentials")]
    NotBare,
    #[error("IP addresses aren't accepted as hosts")]
    IpAddress,
    #[error("local and reserved names aren't accepted as hosts")]
    Reserved,
    #[error("ports aren't accepted outside dev mode")]
    Port,
    #[error("invalid hostname label")]
    BadLabel,
    #[error("hostname needs a domain and a TLD")]
    TooFewLabels,
}

/// Special-use and private-network TLDs a public PDS can't live under.
const RESERVED_TLDS: &[&str] = &[
    "local",
    "localhost",
    "internal",
    "arpa",
    "invalid",
    "test",
    "example",
    "onion",
    "lan",
    "home",
    "corp",
    "intranet",
];

/// Normalizes what a requestCrawl caller or operator typed into a [`Host`]:
/// lowercase, scheme and trailing slash or dot dropped, `:443` dropped.
/// Outside dev mode it refuses IPs, localhost, reserved TLDs and ports.
pub fn normalize_hostname(input: &str, dev_mode: bool) -> Result<Host, HostnameError> {
    let mut s = input.trim().to_ascii_lowercase();
    for scheme in ["https://", "wss://", "http://", "ws://"] {
        if let Some(rest) = s.strip_prefix(scheme) {
            s = rest.to_string();
            break;
        }
    }
    let s = s.strip_suffix('/').unwrap_or(&s);
    if s.is_empty() {
        return Err(HostnameError::Empty);
    }
    if s.len() > 260 {
        return Err(HostnameError::TooLong);
    }
    if s.chars().any(|c| matches!(c, '/' | '?' | '#' | '@' | '\\' | '%') || c.is_whitespace() || c.is_control()) {
        return Err(HostnameError::NotBare);
    }

    if let Some(v6) = s.strip_prefix('[') {
        let (addr, port) = v6.split_once(']').ok_or(HostnameError::NotBare)?;
        addr.parse::<std::net::Ipv6Addr>().map_err(|_| HostnameError::BadLabel)?;
        if !dev_mode {
            return Err(HostnameError::IpAddress);
        }
        if !port.is_empty() {
            let p = port.strip_prefix(':').ok_or(HostnameError::NotBare)?;
            p.parse::<u16>().map_err(|_| HostnameError::Port)?;
        }
        return Ok(Host(s.to_string()));
    }
    if s.parse::<IpAddr>().is_ok() {
        // a bare IPv6 address without brackets
        return if dev_mode { Ok(Host(s.to_string())) } else { Err(HostnameError::IpAddress) };
    }

    let (name, port) = match s.rsplit_once(':') {
        Some((n, p)) => (n, Some(p.parse::<u16>().map_err(|_| HostnameError::Port)?)),
        None => (s, None),
    };
    let name = name.strip_suffix('.').unwrap_or(name);
    if name.is_empty() {
        return Err(HostnameError::Empty);
    }
    if name.len() > 253 {
        return Err(HostnameError::TooLong);
    }
    let port = match port {
        Some(443) | None => None,
        Some(p) if dev_mode => Some(p),
        Some(_) => return Err(HostnameError::Port),
    };
    let render = |n: &str| match port {
        Some(p) => format!("{n}:{p}"),
        None => n.to_string(),
    };

    if name.parse::<std::net::Ipv4Addr>().is_ok() {
        return if dev_mode { Ok(Host(render(name))) } else { Err(HostnameError::IpAddress) };
    }
    let labels: Vec<&str> = name.split('.').collect();
    for l in &labels {
        let ok = !l.is_empty()
            && l.len() <= 63
            && !l.starts_with('-')
            && !l.ends_with('-')
            && l.bytes().all(|b| b.is_ascii_lowercase() || b.is_ascii_digit() || b == b'-');
        if !ok {
            return Err(HostnameError::BadLabel);
        }
    }
    if !dev_mode {
        let tld = labels[labels.len() - 1];
        if RESERVED_TLDS.contains(&tld) {
            return Err(HostnameError::Reserved);
        }
        if labels.len() < 2 {
            return Err(HostnameError::TooFewLabels);
        }
        if tld.bytes().all(|b| b.is_ascii_digit()) {
            return Err(HostnameError::BadLabel);
        }
    }
    Ok(Host(render(name)))
}

#[cfg(test)]
mod tests {
    use super::*;

    const H: i64 = 24 * 3_600_000;

    #[test]
    fn a_start_at_head_is_taken_once_and_expires() {
        let mut r = HostRecord::new(&Host("pds.example.com".into()), Tier::Default);
        r.acked_seq = Some(500);
        let e = HostEntry::from_record(&r);
        assert!(!e.take_start_at_head());
        assert_eq!(e.acked_seq(), Some(500));
        e.start_at_head(60_000);
        assert!(e.take_start_at_head());
        assert_eq!(e.acked_seq(), Some(0), "the new socket's acks set it");
        assert!(!e.take_start_at_head(), "once");
        // a mark nobody connected under in time does nothing
        e.ack(700);
        e.start_at_head(0);
        assert!(!e.take_start_at_head());
        assert_eq!(e.acked_seq(), Some(700));
    }

    #[test]
    fn read_lag_counts_held_time_only_while_held() {
        let e = HostEntry::from_record(&HostRecord::new(&Host("pds.example.com".into()), Tier::Default));
        e.note_frame(Some(now_ms() as i64 - 30_000), H);
        // not live: no lag to speak of
        assert_eq!(e.read_lag_ms(), None);
        e.set_status(HostStatus::Active);
        let lag = e.read_lag_ms().unwrap();
        assert!((30_000..31_000).contains(&lag), "{lag}");
        // a reader held back by its limits keeps falling behind between frames
        e.read_at_ms.fetch_sub(60_000, Ordering::Relaxed);
        e.read_event_ms.fetch_sub(60_000, Ordering::Relaxed);
        e.set_status(HostStatus::Throttled);
        assert!(e.read_lag_ms().unwrap() >= 90_000);
        assert!(e.host_lag_ms().unwrap() >= 90_000);
    }

    #[test]
    fn a_reader_waiting_on_its_socket_is_caught_up() {
        let e = HostEntry::from_record(&HostRecord::new(&Host("pds.example.com".into()), Tier::Default));
        e.set_status(HostStatus::Active);
        // connected, nothing read yet
        assert_eq!(e.read_lag_ms(), Some(0));
        // a replayed frame nine days old, then nothing more from the host
        e.note_frame(Some(now_ms() as i64 - 9 * H), H);
        assert!(e.read_lag_ms().unwrap() >= 9 * H - 1_000);
        e.read_at_ms.fetch_sub(CAUGHT_UP_MS, Ordering::Relaxed);
        e.read_event_ms.fetch_sub(CAUGHT_UP_MS, Ordering::Relaxed);
        assert_eq!((e.read_lag_ms(), e.host_lag_ms()), (Some(0), Some(0)));
    }

    #[test]
    fn the_relay_holding_a_reader_is_not_the_hosts_lag() {
        let e = HostEntry::from_record(&HostRecord::new(&Host("pds.example.com".into()), Tier::Default));
        e.set_status(HostStatus::Active);
        assert_eq!(e.backpressure_at_ms(), None);
        e.note_frame(Some(now_ms() as i64 - 1_000), H);
        e.read_at_ms.fetch_sub(60_000, Ordering::Relaxed);
        e.read_event_ms.fetch_sub(60_000, Ordering::Relaxed);
        e.set_backpressure(Backpressure::QueueFull);
        assert!(e.read_lag_ms().unwrap() >= 60_000);
        assert!(e.host_lag_ms().unwrap() < 2_000);
        assert!(e.backpressure_at_ms().is_some_and(|t| t >= now_ms() as i64 - 1_000));
        e.set_status(HostStatus::Active);
        let left = e.backpressure_at_ms().unwrap();
        assert!(left > now_ms() as i64 - 1_000 && left <= now_ms() as i64);
    }

    #[test]
    fn backpressure_carries_its_reason_until_the_status_moves() {
        let e = HostEntry::from_record(&HostRecord::new(&Host("pds.example.com".into()), Tier::Trusted));
        e.set_status(HostStatus::Active);
        assert_eq!(e.backpressure(), None);
        e.set_backpressure(Backpressure::QueueFull);
        assert_eq!((e.status(), e.backpressure()), (HostStatus::Backpressure, Some(Backpressure::QueueFull)));
        assert_eq!(e.view().backpressure, Some(Backpressure::QueueFull));
        e.set_backpressure(Backpressure::InflightFull);
        assert_eq!(e.backpressure(), Some(Backpressure::InflightFull));
        // the limiter's pause is the host's own limits, not the relay
        e.set_status(HostStatus::Throttled);
        assert_eq!(e.backpressure(), None);
        e.set_backpressure(Backpressure::NodeInflightFull);
        e.set_status(HostStatus::Active);
        assert_eq!((e.status(), e.view().backpressure), (HostStatus::Active, None));
        let r = e.record();
        assert_eq!(serde_json::to_value(HostStatus::Backpressure).unwrap(), "backpressure");
        assert_eq!(serde_json::to_value(Backpressure::NodeInflightFull).unwrap(), "node_inflight_full");
        assert_eq!(serde_json::from_value::<HostRecord>(serde_json::to_value(&r).unwrap()).unwrap(), r);
    }

    fn ok(s: &str) -> String {
        normalize_hostname(s, false).unwrap().0
    }

    #[test]
    fn normalizes() {
        assert_eq!(ok("PDS.Example.com"), "pds.example.com");
        assert_eq!(ok("https://pds.example.com/"), "pds.example.com");
        assert_eq!(ok("wss://pds.example.com:443"), "pds.example.com");
        assert_eq!(ok("pds.example.com."), "pds.example.com");
        assert_eq!(ok("  morel.us-east.host.bsky.network "), "morel.us-east.host.bsky.network");
        assert_eq!(ok("xn--bcher-kva.com"), "xn--bcher-kva.com");
    }

    #[test]
    fn refuses_outside_dev() {
        use HostnameError::*;
        let err = |s: &str| normalize_hostname(s, false).unwrap_err();
        assert_eq!(err(""), Empty);
        assert_eq!(err("https://"), Empty);
        assert_eq!(err("127.0.0.1"), IpAddress);
        assert_eq!(err("10.0.0.1:8080"), Port);
        assert_eq!(err("[::1]"), IpAddress);
        assert_eq!(err("::1"), IpAddress);
        assert_eq!(err("localhost"), Reserved);
        assert_eq!(err("pds.localhost"), Reserved);
        assert_eq!(err("pds.internal"), Reserved);
        assert_eq!(err("pds.example.com:8080"), Port);
        assert_eq!(err("pds.example.com/xrpc"), NotBare);
        assert_eq!(err("user@pds.example.com"), NotBare);
        assert_eq!(err("pds"), TooFewLabels);
        assert_eq!(err("-pds.example.com"), BadLabel);
        assert_eq!(err("pds..example.com"), BadLabel);
        assert_eq!(err("pds_1.example.com"), BadLabel);
        assert_eq!(err(&format!("{}.com", "a".repeat(64))), BadLabel);
        let long = format!("{}com", "abcdefghi.".repeat(26));
        assert_eq!(err(&long), TooLong);
    }

    #[test]
    fn dev_mode_allows_local() {
        let dev = |s: &str| normalize_hostname(s, true).unwrap().0;
        assert_eq!(dev("127.0.0.1:2583"), "127.0.0.1:2583");
        assert_eq!(dev("localhost:2583"), "localhost:2583");
        assert_eq!(dev("http://[::1]:80"), "[::1]:80");
        assert_eq!(dev("pds.test"), "pds.test");
    }

    #[tokio::test]
    async fn registry_persists_dirty_rows() {
        let store = Arc::new(MemHostStore::default());
        let reg = Registry::new(store.clone());
        let h = Host("pds.example.com".into());
        let (e, new) = reg.admit(&h, Tier::New).await.unwrap();
        assert!(new);
        assert_eq!(store.get("pds.example.com").unwrap().tier, Tier::New);
        e.ack(41);
        e.ack(40);
        e.set_tier(Tier::Default);
        assert_eq!(reg.flush().await.unwrap(), 1);
        assert_eq!(reg.flush().await.unwrap(), 0);
        let row = store.get("pds.example.com").unwrap();
        assert_eq!((row.tier, row.acked_seq), (Tier::Default, Some(41)));

        let reg2 = Registry::new(store.clone());
        assert_eq!(reg2.load().await.unwrap(), 1);
        let e2 = reg2.get(&h).unwrap();
        assert_eq!((e2.tier(), e2.acked_seq(), e2.received_seq()), (Tier::Default, Some(41), Some(41)));
        assert!(!reg2.admit(&h, Tier::New).await.unwrap().1);
    }
}
