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
        let u = TierLimits { events_per_sec: f64::INFINITY, bytes_per_sec: f64::INFINITY, burst_secs: 1.0, weight: 1 };
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

/// A host's two buckets, rebuilt when its tier changes.
pub(crate) struct HostLimiter {
    tier: Tier,
    events: TokenBucket,
    bytes: TokenBucket,
}

impl HostLimiter {
    pub fn new(tier: Tier, limits: &Limits, now: Instant) -> HostLimiter {
        let l = limits.for_tier(tier);
        HostLimiter {
            tier,
            events: TokenBucket::new(l.events_per_sec, l.events_per_sec * l.burst_secs, now),
            bytes: TokenBucket::new(l.bytes_per_sec, l.bytes_per_sec * l.burst_secs, now),
        }
    }

    /// The pause owed for one frame of `len` bytes.
    pub fn take(&mut self, tier: Tier, limits: &Limits, len: usize, now: Instant) -> Duration {
        if tier != self.tier {
            *self = HostLimiter::new(tier, limits, now);
        }
        self.events.take(1.0, now).max(self.bytes.take(len as f64, now))
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
        let mut l = HostLimiter::new(Tier::New, &limits, t0);
        // 1 MB/s with 5 s of burst: a 6 MB frame owes ~1 s
        let w = l.take(Tier::New, &limits, 6_000_000, t0);
        assert!((w.as_secs_f64() - 1.0).abs() < 0.01, "{w:?}");
        // a tier change starts fresh buckets
        assert_eq!(l.take(Tier::Trusted, &limits, 1000, t0), Duration::ZERO);
    }
}
