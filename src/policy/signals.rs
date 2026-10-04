//! Spam signals, counted per host and per DID in fixed memory.
//!
//! Each threshold gets a [`TopK`]: a Space-Saving table (Metwally et al.)
//! over sliding windows. It holds at most `capacity` keys. A new key takes
//! the place of the lightest one and inherits its count as error, so a key
//! whose real count is above total/capacity is always in the table, and
//! `count - error` never overstates it. Thresholds are checked against that
//! lower bound, so a trip is never a false positive however many keys churn
//! through. vlpds's busiest-keys tracker (`ratelimit.rs`, `track`) works the
//! same way, but its insert is private to vlpds and isn't capped per table.
//!
//! The window is a sliding one built from two fixed windows: the estimate is
//! `current + previous × (1 - elapsed fraction)`.

use super::doc::{Spam, SpamAction, Threshold};
use parking_lot::Mutex;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::hash::BuildHasher;

/// Longest key kept (a hostname is ≤ 253, a did:web can be longer).
const KEY_MAX: usize = 256;
const DETAIL_MAX: usize = 160;

#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub enum SignalKind {
    /// First event from a DID the relay hadn't seen on this host.
    NewAccount,
    /// One `#commit` (count = ops if the caller prefers records).
    Record,
    /// A frame dropped by a sync or signature check.
    FailedValidation,
    /// `#identity`, or a handle change.
    IdentityChange,
    /// A commit over the size or op limits.
    OversizedCommit,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, Serialize, Deserialize, PartialOrd, Ord)]
#[serde(rename_all = "kebab-case")]
pub enum SpamRule {
    HostNewAccounts,
    AccountRecords,
    HostFailedValidation,
    AccountFailedValidation,
    HostIdentityChurn,
    AccountIdentityChurn,
    HostOversizedCommits,
}

impl SpamRule {
    pub const ALL: [SpamRule; 7] = [
        SpamRule::HostNewAccounts,
        SpamRule::AccountRecords,
        SpamRule::HostFailedValidation,
        SpamRule::AccountFailedValidation,
        SpamRule::HostIdentityChurn,
        SpamRule::AccountIdentityChurn,
        SpamRule::HostOversizedCommits,
    ];

    pub fn name(self) -> &'static str {
        match self {
            SpamRule::HostNewAccounts => "new-accounts",
            SpamRule::AccountRecords => "account-records",
            SpamRule::HostFailedValidation => "failed-validation",
            SpamRule::AccountFailedValidation => "account-failed-validation",
            SpamRule::HostIdentityChurn => "identity-churn",
            SpamRule::AccountIdentityChurn => "account-identity-churn",
            SpamRule::HostOversizedCommits => "oversized-commits",
        }
    }

    pub fn per_account(self) -> bool {
        matches!(
            self,
            SpamRule::AccountRecords
                | SpamRule::AccountFailedValidation
                | SpamRule::AccountIdentityChurn
        )
    }

    pub fn kind(self) -> SignalKind {
        match self {
            SpamRule::HostNewAccounts => SignalKind::NewAccount,
            SpamRule::AccountRecords => SignalKind::Record,
            SpamRule::HostFailedValidation | SpamRule::AccountFailedValidation => {
                SignalKind::FailedValidation
            }
            SpamRule::HostIdentityChurn | SpamRule::AccountIdentityChurn => {
                SignalKind::IdentityChange
            }
            SpamRule::HostOversizedCommits => SignalKind::OversizedCommit,
        }
    }

    pub fn threshold(self, s: &Spam) -> &Threshold {
        match self {
            SpamRule::HostNewAccounts => &s.host_new_accounts,
            SpamRule::AccountRecords => &s.account_records,
            SpamRule::HostFailedValidation => &s.host_failed_validation,
            SpamRule::AccountFailedValidation => &s.account_failed_validation,
            SpamRule::HostIdentityChurn => &s.host_identity_churn,
            SpamRule::AccountIdentityChurn => &s.account_identity_churn,
            SpamRule::HostOversizedCommits => &s.host_oversized_commits,
        }
    }
}

/// One observation. `did` is required for the per-account rules to count.
#[derive(Clone, Copy, Debug)]
pub struct Signal<'a> {
    pub kind: SignalKind,
    pub host: &'a str,
    pub did: Option<&'a str>,
    pub count: u32,
    /// Shown in a case's evidence (a reject reason, a frame size).
    pub detail: Option<&'a str>,
}

impl<'a> Signal<'a> {
    pub fn new(kind: SignalKind, host: &'a str, did: Option<&'a str>) -> Signal<'a> {
        Signal {
            kind,
            host,
            did,
            count: 1,
            detail: None,
        }
    }
}

/// A threshold crossing, at most once per key per window.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct Trip {
    pub rule: SpamRule,
    pub host: String,
    pub did: Option<String>,
    /// Lower bound of the count over the sliding window.
    pub observed: f64,
    pub threshold: f64,
    pub window_secs: u32,
    pub action: SpamAction,
    pub at_ms: i64,
    pub detail: Option<String>,
}

#[derive(Clone, Debug)]
struct Entry {
    key: Box<str>,
    /// For per-account tables: the host the DID's events came from.
    host: Box<str>,
    detail: Option<Box<str>>,
    hash: u64,
    cur: u64,
    cur_err: u64,
    prev: u64,
    prev_err: u64,
    tripped_window: u64,
}

impl Entry {
    fn estimate(&self, keep_prev: f64) -> f64 {
        self.cur as f64 + self.prev as f64 * keep_prev
    }
    fn lower(&self, keep_prev: f64) -> f64 {
        (self.cur - self.cur_err) as f64 + (self.prev - self.prev_err) as f64 * keep_prev
    }
}

/// One Space-Saving table over a sliding window. Not thread-safe by
/// itself: [`Tracker`] shards it behind mutexes.
#[derive(Debug)]
pub struct TopK {
    capacity: usize,
    window_secs: u64,
    window: u64,
    entries: Vec<Entry>,
    index: HashMap<u64, usize>,
    /// Eviction candidates, lightest last: the lightest `capacity / VICTIMS`
    /// entries as of the last scan. With many more keys than room (every DID
    /// on a busy relay) nearly every add evicts, and a scan per add was ~2%
    /// of a loaded node's CPU.
    victims: Vec<usize>,
    /// The heaviest victim's estimate at that scan: a victim that has grown
    /// past it since is skipped.
    victim_cutoff: f64,
}

/// One scan finds candidates for this fraction of the table's evictions.
const VICTIMS: usize = 16;

pub struct Added {
    pub lower: f64,
    pub newly_tripped: bool,
}

impl TopK {
    pub fn new(capacity: usize, window_secs: u32) -> TopK {
        TopK {
            capacity: capacity.max(1),
            window_secs: window_secs.max(1) as u64,
            window: 0,
            entries: Vec::with_capacity(capacity.max(1)),
            index: HashMap::with_capacity(capacity.max(1)),
            victims: Vec::new(),
            victim_cutoff: 0.0,
        }
    }

    pub fn len(&self) -> usize {
        self.entries.len()
    }

    pub fn is_empty(&self) -> bool {
        self.entries.is_empty()
    }

    /// Heap bytes held, for the memory-bound test and a gauge.
    pub fn heap_bytes(&self) -> usize {
        self.entries.capacity() * std::mem::size_of::<Entry>()
            + self
                .entries
                .iter()
                .map(|e| e.key.len() + e.host.len() + e.detail.as_ref().map_or(0, |d| d.len()))
                .sum::<usize>()
            + self.index.capacity() * (std::mem::size_of::<(u64, usize)>() + 1)
            + self.victims.capacity() * std::mem::size_of::<usize>()
    }

    fn keep_prev(&self, now_s: u64) -> f64 {
        1.0 - (now_s % self.window_secs) as f64 / self.window_secs as f64
    }

    fn rotate(&mut self, now_s: u64) {
        let w = now_s / self.window_secs;
        if w == self.window {
            return;
        }
        let adjacent = w == self.window + 1;
        self.victims.clear();
        for e in &mut self.entries {
            if adjacent {
                e.prev = e.cur;
                e.prev_err = e.cur_err;
            } else {
                e.prev = 0;
                e.prev_err = 0;
            }
            e.cur = 0;
            e.cur_err = 0;
        }
        self.window = w;
    }

    /// Counts `n` for `key` and says whether this pushed its lower bound
    /// to `limit` for the first time this window (`limit` ≤ 0: never).
    #[allow(clippy::too_many_arguments)]
    pub fn add(
        &mut self,
        key: &str,
        hash: u64,
        host: &str,
        n: u64,
        detail: Option<&str>,
        limit: f64,
        now_s: u64,
    ) -> Added {
        self.rotate(now_s);
        let keep = self.keep_prev(now_s);
        let i = match self.index.get(&hash) {
            Some(&i) if &*self.entries[i].key == trunc(key, KEY_MAX) => i,
            _ => self.insert(key, hash, host, keep),
        };
        let window = self.window;
        let e = &mut self.entries[i];
        e.cur += n;
        if let Some(d) = detail {
            e.detail = Some(trunc(d, DETAIL_MAX).into());
        }
        if e.host.as_ref() != host {
            e.host = trunc(host, KEY_MAX).into();
        }
        let lower = e.lower(keep);
        // tripped_window is stored + 1 so 0 means never
        let newly_tripped = limit > 0.0 && lower >= limit && e.tripped_window != window + 1;
        if newly_tripped {
            e.tripped_window = window + 1;
        }
        Added {
            lower,
            newly_tripped,
        }
    }

    fn insert(&mut self, key: &str, hash: u64, host: &str, keep: f64) -> usize {
        let fresh = Entry {
            key: trunc(key, KEY_MAX).into(),
            host: trunc(host, KEY_MAX).into(),
            detail: None,
            hash,
            cur: 0,
            cur_err: 0,
            prev: 0,
            prev_err: 0,
            tripped_window: 0,
        };
        if self.entries.len() < self.capacity {
            self.entries.push(fresh);
            let i = self.entries.len() - 1;
            self.index.insert(hash, i);
            return i;
        }
        let i = self.victim(keep);
        let old = &self.entries[i];
        self.index.remove(&old.hash);
        let (cur, prev) = (old.cur, old.prev);
        self.entries[i] = Entry {
            cur,
            cur_err: cur,
            prev,
            prev_err: prev,
            ..fresh
        };
        self.index.insert(hash, i);
        i
    }

    /// A light entry to evict: one of the lightest at the last scan that
    /// hasn't grown since. Evicting it rather than the exact minimum keeps
    /// the lower bounds honest (the newcomer inherits its count as error).
    fn victim(&mut self, keep: f64) -> usize {
        loop {
            while let Some(i) = self.victims.pop() {
                if self.entries[i].estimate(keep) <= self.victim_cutoff {
                    return i;
                }
            }
            let mut est: Vec<(f64, usize)> =
                self.entries.iter().enumerate().map(|(i, e)| (e.estimate(keep), i)).collect();
            let n = (est.len() / VICTIMS).max(1);
            est.select_nth_unstable_by(n - 1, |a, b| a.0.total_cmp(&b.0));
            est.truncate(n);
            est.sort_unstable_by(|a, b| b.0.total_cmp(&a.0));
            self.victim_cutoff = est[0].0;
            self.victims = est.into_iter().map(|(_, i)| i).collect();
        }
    }

    /// Lower bound for `key`, 0 if it isn't tracked.
    pub fn lower_bound(&self, key: &str, hash: u64, now_s: u64) -> f64 {
        let Some(&i) = self.index.get(&hash) else {
            return 0.0;
        };
        let e = &self.entries[i];
        if &*e.key != trunc(key, KEY_MAX) {
            return 0.0;
        }
        let w = now_s / self.window_secs;
        let keep = 1.0 - (now_s % self.window_secs) as f64 / self.window_secs as f64;
        if w == self.window {
            e.lower(keep)
        } else if w == self.window + 1 {
            (e.cur - e.cur_err) as f64 * keep
        } else {
            0.0
        }
    }

    /// The heaviest keys by estimate: (key, host, estimate, lower bound).
    pub fn top(&self, n: usize, now_s: u64) -> Vec<(String, String, f64, f64)> {
        let w = now_s / self.window_secs;
        let keep = 1.0 - (now_s % self.window_secs) as f64 / self.window_secs as f64;
        let mut v: Vec<_> = self
            .entries
            .iter()
            .filter_map(|e| {
                let (est, low) = if w == self.window {
                    (e.estimate(keep), e.lower(keep))
                } else if w == self.window + 1 {
                    (e.cur as f64 * keep, (e.cur - e.cur_err) as f64 * keep)
                } else {
                    return None;
                };
                (est > 0.0).then(|| (e.key.to_string(), e.host.to_string(), est, low))
            })
            .collect();
        v.sort_by(|a, b| b.2.total_cmp(&a.2).then_with(|| a.0.cmp(&b.0)));
        v.truncate(n);
        v
    }
}

fn trunc(s: &str, max: usize) -> &str {
    if s.len() <= max {
        return s;
    }
    let mut end = max;
    while !s.is_char_boundary(end) {
        end -= 1;
    }
    &s[..end]
}

const SHARDS: usize = 16;

/// One rule's table, sharded by key hash. A key always lands in the same
/// shard, so the Space-Saving bound holds per shard with capacity/SHARDS.
pub struct Tracker {
    pub rule: SpamRule,
    pub threshold: Threshold,
    shards: Box<[Mutex<TopK>]>,
    hasher: std::collections::hash_map::RandomState,
}

impl Tracker {
    pub fn new(rule: SpamRule, threshold: Threshold, capacity: usize) -> Tracker {
        let per = capacity.div_ceil(SHARDS).max(1);
        Tracker {
            rule,
            shards: (0..SHARDS)
                .map(|_| Mutex::new(TopK::new(per, threshold.window_secs)))
                .collect(),
            threshold,
            hasher: Default::default(),
        }
    }

    fn hash(&self, key: &str) -> u64 {
        self.hasher.hash_one(key)
    }

    pub fn len(&self) -> usize {
        self.shards.iter().map(|s| s.lock().len()).sum()
    }

    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    pub fn heap_bytes(&self) -> usize {
        self.shards.iter().map(|s| s.lock().heap_bytes()).sum()
    }

    pub fn add(&self, s: &Signal<'_>, now_ms: i64) -> Option<Trip> {
        let key = if self.rule.per_account() {
            s.did?
        } else {
            s.host
        };
        let h = self.hash(key);
        let now_s = (now_ms / 1000) as u64;
        let limit = if self.threshold.enabled() {
            self.threshold.limit
        } else {
            0.0
        };
        let a = self.shards[(h >> 60) as usize % SHARDS].lock().add(
            key,
            h,
            s.host,
            s.count as u64,
            s.detail,
            limit,
            now_s,
        );
        a.newly_tripped.then(|| Trip {
            rule: self.rule,
            host: s.host.to_string(),
            did: s
                .did
                .filter(|_| self.rule.per_account())
                .map(str::to_string),
            observed: a.lower,
            threshold: self.threshold.limit,
            window_secs: self.threshold.window_secs,
            action: self.threshold.action,
            at_ms: now_ms,
            detail: s.detail.map(|d| trunc(d, DETAIL_MAX).to_string()),
        })
    }

    pub fn lower_bound(&self, key: &str, now_ms: i64) -> f64 {
        let h = self.hash(key);
        self.shards[(h >> 60) as usize % SHARDS]
            .lock()
            .lower_bound(key, h, (now_ms / 1000) as u64)
    }

    pub fn top(&self, n: usize, now_ms: i64) -> Vec<(String, String, f64, f64)> {
        let now_s = (now_ms / 1000) as u64;
        let mut all: Vec<_> = self
            .shards
            .iter()
            .flat_map(|s| s.lock().top(n, now_s))
            .collect();
        all.sort_by(|a, b| b.2.total_cmp(&a.2).then_with(|| a.0.cmp(&b.0)));
        all.truncate(n);
        all
    }
}

/// Every rule's tracker, rebuilt when the policy changes a window or a
/// capacity (counts restart then, which is fine for minute-to-hour windows).
pub struct Signals {
    trackers: parking_lot::RwLock<Vec<Tracker>>,
    trips: Mutex<std::collections::VecDeque<Trip>>,
    pub dropped_trips: std::sync::atomic::AtomicU64,
}

/// Trips wait here for the driver. Past this many, new ones are counted and
/// dropped (the driver runs every second, so it means the driver is stuck).
const TRIP_QUEUE: usize = 4_096;

impl Signals {
    pub fn new(spam: &Spam) -> Signals {
        Signals {
            trackers: parking_lot::RwLock::new(build(spam)),
            trips: Mutex::new(Default::default()),
            dropped_trips: Default::default(),
        }
    }

    pub fn reconfigure(&self, spam: &Spam) {
        let mut t = self.trackers.write();
        let same_shape = t.iter().all(|tr| {
            let th = tr.rule.threshold(spam);
            th.window_secs == tr.threshold.window_secs
                && tr.shards[0].lock().capacity == capacity(tr.rule, spam).div_ceil(SHARDS).max(1)
        });
        if same_shape {
            // limits and actions change in place, counts kept
            for tr in t.iter_mut() {
                tr.threshold = tr.rule.threshold(spam).clone();
            }
        } else {
            *t = build(spam);
        }
    }

    pub fn record(&self, s: &Signal<'_>, now_ms: i64) -> Vec<Trip> {
        let t = self.trackers.read();
        let trips: Vec<Trip> = t
            .iter()
            .filter(|tr| tr.rule.kind() == s.kind)
            .filter_map(|tr| tr.add(s, now_ms))
            .collect();
        drop(t);
        if !trips.is_empty() {
            let mut q = self.trips.lock();
            for tr in &trips {
                if q.len() >= TRIP_QUEUE {
                    self.dropped_trips
                        .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                } else {
                    q.push_back(tr.clone());
                }
            }
        }
        trips
    }

    pub fn drain(&self) -> Vec<Trip> {
        self.trips.lock().drain(..).collect()
    }

    /// Every rule's current lower bound for a host (and DID), for evidence.
    pub fn snapshot(
        &self,
        host: &str,
        did: Option<&str>,
        now_ms: i64,
    ) -> std::collections::BTreeMap<String, f64> {
        let t = self.trackers.read();
        t.iter()
            .filter_map(|tr| {
                let key = if tr.rule.per_account() { did? } else { host };
                let v = tr.lower_bound(key, now_ms);
                (v > 0.0).then(|| (tr.rule.name().to_string(), v))
            })
            .collect()
    }

    pub fn top(&self, rule: SpamRule, n: usize, now_ms: i64) -> Vec<(String, String, f64, f64)> {
        let t = self.trackers.read();
        t.iter()
            .find(|tr| tr.rule == rule)
            .map(|tr| tr.top(n, now_ms))
            .unwrap_or_default()
    }

    pub fn heap_bytes(&self) -> usize {
        self.trackers.read().iter().map(|t| t.heap_bytes()).sum()
    }

    pub fn tracked(&self, rule: SpamRule) -> usize {
        self.trackers
            .read()
            .iter()
            .find(|t| t.rule == rule)
            .map_or(0, |t| t.len())
    }
}

fn capacity(rule: SpamRule, spam: &Spam) -> usize {
    if rule.per_account() {
        spam.track_accounts as usize
    } else {
        spam.track_hosts as usize
    }
}

fn build(spam: &Spam) -> Vec<Tracker> {
    SpamRule::ALL
        .iter()
        .map(|&r| Tracker::new(r, r.threshold(spam).clone(), capacity(r, spam)))
        .collect()
}
