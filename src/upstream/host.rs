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
    pub const ALL: [Tier; 6] = [Tier::Trusted, Tier::Default, Tier::New, Tier::Throttled, Tier::Suspended, Tier::Banned];

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
    /// Held off by its rate limit or a full queue: the socket isn't being read.
    Throttled,
}

impl HostStatus {
    const ALL: [HostStatus; 5] =
        [HostStatus::Idle, HostStatus::Connecting, HostStatus::Active, HostStatus::Backoff, HostStatus::Throttled];
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
    last_connected_ms: AtomicU64,
    account_count: AtomicU64,
    admitted_ms: u64,
    errors: Mutex<ErrorCounters>,
    dirty: AtomicBool,
}

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
            last_connected_ms: AtomicU64::new(r.last_connected_ms.unwrap_or(0)),
            account_count: AtomicU64::new(r.account_count),
            admitted_ms: r.admitted_ms,
            errors: Mutex::new(r.errors.clone()),
            dirty: AtomicBool::new(false),
        }
    }

    pub fn tier(&self) -> Tier {
        Tier::from_u8(self.tier.load(Ordering::Relaxed))
    }

    pub(crate) fn set_tier(&self, t: Tier) {
        self.tier.store(t.to_u8(), Ordering::Relaxed);
        self.dirty.store(true, Ordering::Relaxed);
    }

    pub fn status(&self) -> HostStatus {
        HostStatus::ALL[self.status.load(Ordering::Relaxed) as usize]
    }

    pub(crate) fn set_status(&self, s: HostStatus) {
        if self.status.swap(s as u8, Ordering::Relaxed) != s as u8 {
            self.dirty.store(true, Ordering::Relaxed);
        }
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

    /// A host that reset its sequence (FutureCursor) starts over.
    pub(crate) fn reset_cursor(&self) {
        self.acked_seq.store(NO_SEQ, Ordering::Relaxed);
        self.received_seq.store(NO_SEQ, Ordering::Relaxed);
        self.dirty.store(true, Ordering::Relaxed);
    }

    pub(crate) fn note_connected(&self) {
        self.connects.fetch_add(1, Ordering::Relaxed);
        self.last_connected_ms.store(now_ms(), Ordering::Relaxed);
        self.dirty.store(true, Ordering::Relaxed);
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
const RESERVED_TLDS: &[&str] =
    &["local", "localhost", "internal", "arpa", "invalid", "test", "example", "onion", "lan", "home", "corp", "intranet"];

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
