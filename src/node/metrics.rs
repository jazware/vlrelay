//! Relay metrics: Prometheus series (rendered with vlpds's, which share the
//! default registry) and the 1 s samples the operator dashboard charts.

use crate::types::Host;
use parking_lot::Mutex;
use prometheus::{
    Histogram, HistogramVec, IntCounter, IntCounterVec, IntGauge, IntGaugeVec, exponential_buckets, register_histogram,
    register_histogram_vec, register_int_counter, register_int_counter_vec, register_int_gauge, register_int_gauge_vec,
};
use std::collections::{BTreeMap, HashMap, VecDeque};
use std::sync::LazyLock;
use std::time::{Duration, Instant};

macro_rules! lazy {
    ($name:ident: $t:ty = $e:expr) => {
        pub static $name: LazyLock<$t> = LazyLock::new(|| $e.unwrap());
    };
}

fn latency_buckets() -> Vec<f64> {
    exponential_buckets(0.0005, 1.6, 24).unwrap()
}

lazy!(EVENTS_IN: IntCounterVec = register_int_counter_vec!("vlrelay_events_in_total", "Frames read off upstream sockets, by kind", &["kind"]));
lazy!(EVENTS_ACCEPTED: IntCounterVec = register_int_counter_vec!("vlrelay_events_accepted_total", "Events appended to the node log, by kind", &["kind"]));
lazy!(EVENTS_OUT: IntCounter = register_int_counter!("vlrelay_events_out_total", "Events emitted on subscribeRepos by the merger"));
lazy!(EVENTS_REJECTED: IntCounterVec = register_int_counter_vec!("vlrelay_events_rejected_total", "Upstream events dropped, by reason", &["reason"]));
lazy!(EVENTS_DUPLICATE: IntCounterVec = register_int_counter_vec!("vlrelay_events_duplicate_total", "Upstream events already applied (replays after a reconnect or restart), by where they were caught", &["at"]));
lazy!(EVENTS_FENCED: IntCounter = register_int_counter!("vlrelay_events_fenced_total", "Events dropped unsent because an earlier event of their host socket gave up or the host moved; the host replays them"));
lazy!(EVENTS_SKIPPED: IntCounterVec = register_int_counter_vec!("vlrelay_events_skipped_total", "Upstream frames not relayed by design (#info, unknown types)", &["kind"]));
lazy!(IDENTITY_LOOKUPS: IntGaugeVec = register_int_gauge_vec!("vlrelay_identity_lookups", "DID document cache lookups since start: hits, misses the seeder filled (state record or PLC export entry) and PLC/did:web fetches", &["outcome"]));
lazy!(TIME_TO_FIREHOSE: Histogram = register_histogram!("vlrelay_time_to_firehose_seconds", "Upstream frame received -> emitted on subscribeRepos", latency_buckets()));
lazy!(STAGE: HistogramVec = register_histogram_vec!("vlrelay_stage_seconds", "Wall time per event in a pipeline stage (parse+verify is CPU only; identity includes DID lookups; apply includes the DID owner's lookups)", &["stage"], latency_buckets()));
lazy!(STAGE_CPU: IntCounterVec = register_int_counter_vec!("vlrelay_stage_busy_us_total", "Microseconds spent in a pipeline stage, summed over events", &["stage"]));
lazy!(DURABLE_LAG: IntGauge = register_int_gauge!("vlrelay_durable_lag_ms", "Mean append -> durable time over the last second"));
lazy!(HOSTS: IntGaugeVec = register_int_gauge_vec!("vlrelay_hosts", "Upstream hosts by status", &["status"]));
lazy!(CONSUMERS: IntGauge = register_int_gauge!("vlrelay_consumers", "Connected subscribeRepos consumers"));
lazy!(HOST_READ_LAG_MAX: IntGauge = register_int_gauge!("vlrelay_host_read_lag_max_seconds", "The furthest any host reader is behind its host's stream: newest frame's age when read, plus the time since while held back by limits"));
lazy!(HOSTS_LAGGING: IntGauge = register_int_gauge!("vlrelay_hosts_lagging", "Hosts whose reader is more than a minute behind"));
lazy!(IDENTITY_CACHE: IntGauge = register_int_gauge!("vlrelay_identity_cache_entries", "DID documents in the identity cache"));
lazy!(ACCOUNTS_THROTTLED: IntCounterVec = register_int_counter_vec!("vlrelay_accounts_throttled_total", "New accounts created throttled by policy, by why (host_cap)", &["why"]));
lazy!(ACCOUNTS_DEFERRED: IntCounterVec = register_int_counter_vec!("vlrelay_accounts_deferred_total", "Events of new accounts dropped while a new-account budget was spent, by which (host_rate, cluster_budget)", &["why"]));
lazy!(FORCED_LOOKUPS_REFUSED: IntCounter = register_int_counter!("vlrelay_forced_lookups_refused_total", "Fresh DID document fetches an event asked for that its host's budget refused (the cached document was used)"));
lazy!(LANE_QUEUED: IntGauge = register_int_gauge!("vlrelay_lane_queued", "Events queued in front of the pipeline lanes"));

/// One pipeline stage's series, resolved once: `with_label_values` hashes
/// its labels on every call, several times per event.
pub struct Stage {
    seconds: Histogram,
    busy_us: IntCounter,
}

impl Stage {
    fn of(name: &str) -> Stage {
        Stage { seconds: STAGE.with_label_values(&[name]), busy_us: STAGE_CPU.with_label_values(&[name]) }
    }

    /// Wall time only (a stage that waits, like a DID lookup).
    pub fn wall(&self, d: Duration) {
        self.seconds.observe(d.as_secs_f64());
    }

    /// Wall time that is also busy time.
    pub fn busy(&self, d: Duration) {
        self.seconds.observe(d.as_secs_f64());
        self.busy_us.inc_by(d.as_micros() as u64);
    }
}

pub static PARSE: LazyLock<Stage> = LazyLock::new(|| Stage::of("parse"));
pub static IDENTITY: LazyLock<Stage> = LazyLock::new(|| Stage::of("identity"));
pub static VERIFY: LazyLock<Stage> = LazyLock::new(|| Stage::of("verify"));
pub static APPLY: LazyLock<Stage> = LazyLock::new(|| Stage::of("apply"));

const KINDS: [&str; 4] = ["commit", "sync", "identity", "account"];

/// A per-kind counter of `vec`, resolved once for the four event kinds.
pub struct ByKind([IntCounter; 4], &'static IntCounterVec);

impl ByKind {
    fn of(vec: &'static IntCounterVec) -> ByKind {
        ByKind(KINDS.map(|k| vec.with_label_values(&[k])), vec)
    }

    pub fn inc(&self, kind: &str) {
        match KINDS.iter().position(|k| *k == kind) {
            Some(i) => self.0[i].inc(),
            None => self.1.with_label_values(&[kind]).inc(),
        }
    }
}

pub static IN_BY_KIND: LazyLock<ByKind> = LazyLock::new(|| ByKind::of(&EVENTS_IN));
pub static ACCEPTED_BY_KIND: LazyLock<ByKind> = LazyLock::new(|| ByKind::of(&EVENTS_ACCEPTED));

/// Marks the time from when a frame arrived to when the merger emitted it.
/// Durability (where the receive time is known) and emission (seen by a tap
/// on the firehose ring) race, so whichever comes second records it.
#[derive(Default)]
pub struct Ttf {
    inner: Mutex<TtfInner>,
}

#[derive(Default)]
struct TtfInner {
    received: crate::types::FastMap<i64, Instant>,
    emitted: crate::types::FastMap<i64, Instant>,
    window: Option<hdrhistogram::Histogram<u64>>,
}

impl Ttf {
    fn record(i: &mut TtfInner, d: Duration) {
        TIME_TO_FIREHOSE.observe(d.as_secs_f64());
        let h = i.window.get_or_insert_with(|| hdrhistogram::Histogram::new_with_bounds(1, 600_000_000, 2).unwrap());
        let _ = h.record((d.as_micros() as u64).max(1));
    }

    /// Durable events: each one's relay seqs and when its frame arrived.
    pub fn durable_batch<'a>(&self, done: impl IntoIterator<Item = (&'a [i64], Instant)>) {
        let mut i = self.inner.lock();
        for (seqs, received) in done {
            for &seq in seqs {
                match i.emitted.remove(&seq) {
                    Some(at) => Self::record(&mut i, at.saturating_duration_since(received)),
                    None => {
                        i.received.insert(seq, received);
                    }
                }
            }
        }
    }

    /// Events the merger emitted at `at`.
    pub fn emitted_batch(&self, seqs: impl IntoIterator<Item = i64>, at: Instant) {
        let mut i = self.inner.lock();
        for seq in seqs {
            match i.received.remove(&seq) {
                Some(r) => Self::record(&mut i, at.saturating_duration_since(r)),
                None => {
                    i.emitted.insert(seq, at);
                }
            }
        }
    }

    /// The window's p50 and p99 in ms, and starts a new window. Also drops
    /// halves that never met their pair (events from before a restart).
    pub fn roll(&self) -> (f64, f64) {
        let mut i = self.inner.lock();
        let old = Instant::now() - Duration::from_secs(30);
        i.received.retain(|_, t| *t > old);
        i.emitted.retain(|_, t| *t > old);
        match i.window.take() {
            Some(h) if !h.is_empty() => {
                (h.value_at_quantile(0.5) as f64 / 1000.0, h.value_at_quantile(0.99) as f64 / 1000.0)
            }
            _ => (0.0, 0.0),
        }
    }
}

/// Per-host rejects, for the dashboard's host detail.
#[derive(Default)]
pub struct HostRejects {
    pub by_reason: BTreeMap<&'static str, u64>,
    pub total: u64,
    pub recent: VecDeque<RejectNote>,
}

#[derive(Clone)]
pub struct RejectNote {
    pub at_ms: i64,
    pub did: String,
    pub reason: &'static str,
    pub upstream_seq: i64,
    pub detail: String,
}

/// An event this node read that the leader appended.
#[derive(Clone)]
pub struct PassedNote {
    pub at_ms: i64,
    pub host: Host,
    pub did: String,
    pub seq: i64,
    pub upstream_seq: i64,
    pub kind: &'static str,
}

/// Passed events the admin tail keeps (a few seconds at today's rates).
pub const PASSED_KEPT: usize = 8192;

/// Seconds of history the overview keeps (5 minutes), and per host (2).
pub const HISTORY: usize = 300;
pub const HOST_HISTORY: usize = 120;

#[derive(Default, Clone)]
pub struct Sample {
    pub t: i64,
    pub events_in: f64,
    pub events_out: f64,
    pub bytes_in: f64,
    pub bytes_out: f64,
    pub ttf_p50_ms: f64,
    pub ttf_p99_ms: f64,
    pub durable_lag_ms: f64,
    pub rejects: BTreeMap<&'static str, f64>,
}

#[derive(Default)]
pub struct HostSeries {
    pub t: VecDeque<i64>,
    pub events: VecDeque<f64>,
    pub rejects: VecDeque<f64>,
    last_frames: u64,
    last_rejects: u64,
    primed: bool,
}

impl HostSeries {
    pub fn push(&mut self, t: i64, frames: u64, rejects: u64) {
        let ev = frames.saturating_sub(self.last_frames) as f64;
        let rj = rejects.saturating_sub(self.last_rejects) as f64;
        // the first sample has no previous one to diff against
        let first = !std::mem::replace(&mut self.primed, true);
        self.last_frames = frames;
        self.last_rejects = rejects;
        if first {
            return;
        }
        self.t.push_back(t);
        self.events.push_back(ev);
        self.rejects.push_back(rj);
        while self.t.len() > HOST_HISTORY {
            self.t.pop_front();
            self.events.pop_front();
            self.rejects.pop_front();
        }
    }

    pub fn rate(&self) -> f64 {
        let n = self.events.len().min(10);
        if n == 0 {
            return 0.0;
        }
        self.events.iter().rev().take(n).sum::<f64>() / n as f64
    }

    pub fn reject_ratio(&self) -> f64 {
        let n = self.events.len().min(60);
        let ev: f64 = self.events.iter().rev().take(n).sum();
        let rj: f64 = self.rejects.iter().rev().take(n).sum();
        if ev > 0.0 { rj / ev } else { 0.0 }
    }
}

#[derive(Default)]
pub struct Dash {
    pub history: VecDeque<Sample>,
    pub hosts: HashMap<Host, HostSeries>,
}
