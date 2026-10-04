//! Per-host rate limits.
//!
//! vlpds's `ratelimit` module counts requests in fixed windows keyed by IP
//! or DID, which suits "N calls per hour" but not a byte rate on a stream.
//! These are plain token buckets with debt: a frame always goes through, and
//! the host then waits until its bucket is back to zero. Waiting means not
//! reading the socket, so the PDS holds the events instead of us dropping them.

use super::host::Tier;
use serde::{Deserialize, Serialize};
use std::time::{Duration, Instant};

#[derive(Clone, Copy, Debug, PartialEq, Serialize, Deserialize)]
pub struct TierLimits {
    pub events_per_sec: f64,
    pub bytes_per_sec: f64,
    /// Bucket depth, in seconds of rate.
    pub burst_secs: f64,
    /// Share of the fair queue relative to other tiers.
    pub weight: u32,
    /// Caps over longer windows (indigo's hourly and daily host limits),
    /// each a bucket as deep as the cap. 0: none.
    #[serde(default)]
    pub events_per_hour: f64,
    #[serde(default)]
    pub events_per_day: f64,
    /// Dials per hour, so a flapping host can't keep a task busy. 0: none.
    #[serde(default)]
    pub reconnects_per_hour: f64,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct Limits {
    pub trusted: TierLimits,
    pub default: TierLimits,
    pub new: TierLimits,
    pub throttled: TierLimits,
}

impl Default for Limits {
    /// Sized from today's network: the busiest Bluesky PDS shards run a few
    /// hundred events/s, and events average ~4.5 KB with the occasional
    /// multi-MB commit, hence the bytes headroom.
    fn default() -> Limits {
        let t = |events_per_sec: f64, mb: f64, weight| TierLimits {
            events_per_sec,
            bytes_per_sec: mb * 1e6,
            burst_secs: 5.0,
            weight,
            events_per_hour: 0.0,
            events_per_day: 0.0,
            reconnects_per_hour: 0.0,
        };
        Limits {
            trusted: t(10_000.0, 100.0, 8),
            default: t(1_000.0, 10.0, 4),
            new: t(50.0, 1.0, 1),
            throttled: t(5.0, 0.25, 1),
        }
    }
}

impl Limits {
    /// No limit and equal weights, for tests that measure something else.
    pub fn unlimited() -> Limits {
        let u = TierLimits {
            events_per_sec: f64::INFINITY,
            bytes_per_sec: f64::INFINITY,
            burst_secs: 1.0,
            weight: 1,
            events_per_hour: 0.0,
            events_per_day: 0.0,
            reconnects_per_hour: 0.0,
        };
        Limits { trusted: u, default: u, new: u, throttled: u }
    }

    pub fn for_tier(&self, t: Tier) -> TierLimits {
        match t {
            Tier::Trusted => self.trusted,
            Tier::Default => self.default,
            Tier::New => self.new,
            // never connected, so only the weight could matter
            Tier::Throttled | Tier::Suspended | Tier::Banned => self.throttled,
        }
    }
}

#[derive(Debug)]
pub struct TokenBucket {
    rate: f64,
    burst: f64,
    tokens: f64,
    last: Instant,
}

impl TokenBucket {
    pub fn new(rate: f64, burst: f64, now: Instant) -> TokenBucket {
        let burst = burst.max(1.0);
        TokenBucket { rate, burst, tokens: burst, last: now }
    }

    /// A bucket for `per_window` per `window_secs`, as deep as the window's
    /// allowance; 0 means no limit.
    pub fn windowed(per_window: f64, window_secs: f64, now: Instant) -> TokenBucket {
        let (rate, burst) = windowed(per_window, window_secs);
        TokenBucket::new(rate, burst, now)
    }

    /// New rate and depth, keeping what the bucket holds (or owes), so a
    /// policy change doesn't hand a host a fresh burst.
    pub fn retune(&mut self, rate: f64, burst: f64, now: Instant) {
        self.take(0.0, now);
        self.rate = rate;
        self.burst = burst.max(1.0);
        if !self.rate.is_infinite() {
            self.tokens = self.tokens.min(self.burst);
        } else {
            self.tokens = self.burst;
        }
    }

    /// Takes `n` and returns how long until the bucket is out of debt.
    pub fn take(&mut self, n: f64, now: Instant) -> Duration {
        if self.rate.is_infinite() {
            return Duration::ZERO;
        }
        let dt = now.saturating_duration_since(self.last).as_secs_f64();
        self.last = now;
        self.tokens = (self.tokens + dt * self.rate).min(self.burst) - n;
        if self.tokens >= 0.0 || self.rate <= 0.0 {
            Duration::ZERO
        } else {
            Duration::from_secs_f64(-self.tokens / self.rate)
        }
    }
}

fn windowed(per_window: f64, window_secs: f64) -> (f64, f64) {
    if per_window > 0.0 && per_window.is_finite() {
        (per_window / window_secs, per_window)
    } else {
        (f64::INFINITY, 1.0)
    }
}

/// A host's buckets, retuned in place when its limits change (a tier move,
/// a policy edit, an operator throttle) so the socket stays up.
pub(crate) struct HostLimiter {
    generation: u64,
    events: TokenBucket,
    bytes: TokenBucket,
    hour: TokenBucket,
    day: TokenBucket,
}

impl HostLimiter {
    pub fn new(l: &TierLimits, generation: u64, now: Instant) -> HostLimiter {
        HostLimiter {
            generation,
            events: TokenBucket::new(l.events_per_sec, l.events_per_sec * l.burst_secs, now),
            bytes: TokenBucket::new(l.bytes_per_sec, l.bytes_per_sec * l.burst_secs, now),
            hour: TokenBucket::windowed(l.events_per_hour, 3_600.0, now),
            day: TokenBucket::windowed(l.events_per_day, 86_400.0, now),
        }
    }

    pub fn generation(&self) -> u64 {
        self.generation
    }

    pub fn retune(&mut self, l: &TierLimits, generation: u64, now: Instant) {
        self.generation = generation;
        self.events.retune(l.events_per_sec, l.events_per_sec * l.burst_secs, now);
        self.bytes.retune(l.bytes_per_sec, l.bytes_per_sec * l.burst_secs, now);
        let (r, b) = windowed(l.events_per_hour, 3_600.0);
        self.hour.retune(r, b, now);
        let (r, b) = windowed(l.events_per_day, 86_400.0);
        self.day.retune(r, b, now);
    }

    /// The pause owed for one frame of `len` bytes.
    pub fn take(&mut self, len: usize, now: Instant) -> Duration {
        self.events
            .take(1.0, now)
            .max(self.bytes.take(len as f64, now))
            .max(self.hour.take(1.0, now))
            .max(self.day.take(1.0, now))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn bucket_rate_and_debt() {
        let t0 = Instant::now();
        let mut b = TokenBucket::new(100.0, 10.0, t0);
        for _ in 0..10 {
            assert_eq!(b.take(1.0, t0), Duration::ZERO);
        }
        // the 11th goes into debt by one token: 10 ms at 100/s
        let w = b.take(1.0, t0);
        assert!((w.as_secs_f64() - 0.01).abs() < 1e-9, "{w:?}");
        // after the wait it's square again
        assert_eq!(b.take(0.0, t0 + w), Duration::ZERO);
        // a long idle refills only to the burst
        let later = t0 + Duration::from_secs(60);
        for _ in 0..10 {
            assert_eq!(b.take(1.0, later), Duration::ZERO);
        }
        assert!(b.take(1.0, later) > Duration::ZERO);
    }

    #[test]
    fn oversize_frame_pays_its_bytes() {
        let t0 = Instant::now();
        let limits = Limits::default();
        let mut l = HostLimiter::new(&limits.for_tier(Tier::New), 0, t0);
        // 1 MB/s with 5 s of burst: a 6 MB frame owes ~1 s
        let w = l.take(6_000_000, t0);
        assert!((w.as_secs_f64() - 1.0).abs() < 0.01, "{w:?}");
        // a retune keeps the debt: a higher byte rate pays it off sooner
        l.retune(&limits.for_tier(Tier::Trusted), 1, t0);
        let w = l.take(1000, t0);
        assert!(w > Duration::ZERO && w < Duration::from_millis(20), "{w:?}");
    }

    #[test]
    fn hourly_cap_holds_past_the_per_second_burst() {
        let t0 = Instant::now();
        let l = TierLimits { events_per_hour: 100.0, ..Limits::unlimited().default };
        let mut h = HostLimiter::new(&l, 0, t0);
        for _ in 0..100 {
            assert_eq!(h.take(10, t0), Duration::ZERO);
        }
        // the 101st waits for a 36 s refill
        let w = h.take(10, t0);
        assert!((w.as_secs_f64() - 36.0).abs() < 0.1, "{w:?}");
        // lifting the cap (0 = none) clears the wait
        h.retune(&Limits::unlimited().default, 1, t0);
        assert_eq!(h.take(10, t0), Duration::ZERO);
    }
}
