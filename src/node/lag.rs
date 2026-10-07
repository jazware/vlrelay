//! Read-lag cases: a host whose reader falls behind the host's own stream
//! while the relay has room for it, and their closing once it has caught up.
//!
//! The lag a case looks at is [`HostEntry::host_lag_ms`]: the newest frame's
//! age when read, 0 once the reader waits on an empty socket, plus time held
//! by the host's own limits, never time the relay held it. A frame read
//! right after the relay held the reader is old because of the relay too,
//! so a case also waits out:
//!
//! - the host in `backpressure`, and [`LagCaseConfig::grace`] after;
//! - this node's in-flight caps or a pipeline lane more than
//!   [`LagCaseConfig::pressure`] full, and the grace after.
//!
//! Past those, the lag has to stay over the threshold for
//! [`LagCaseConfig::sustain`] without the reader catching up on it.
//!
//! The node that reads a host resolves its open read-lag cases once its lag
//! has stayed under the threshold for [`LagCaseConfig::resolve_after`]. Only
//! that node knows the lag, and cases are shared objects in the bucket
//! written by CAS, so a host changing owners at worst has two nodes try and
//! the second find it already closed.
//!
//! [`HostEntry::host_lag_ms`]: crate::upstream::HostEntry::host_lag_ms

use std::collections::HashMap;
use std::time::Duration;

#[derive(Clone, Copy, Debug, PartialEq)]
pub struct LagCaseConfig {
    /// A host's lag over this opens a case.
    pub threshold: Duration,
    /// ...once it has stayed over it this long.
    pub sustain: Duration,
    /// How long after the relay last held the host (or this node was under
    /// pressure) its lag doesn't count.
    pub grace: Duration,
    /// This node's in-flight caps or busiest lane this full (0-1) is
    /// pressure.
    pub pressure: f64,
    /// An open read-lag case closes once its host's lag has been under the
    /// threshold this long.
    pub resolve_after: Duration,
}

impl Default for LagCaseConfig {
    fn default() -> Self {
        LagCaseConfig {
            threshold: Duration::from_secs(600),
            sustain: Duration::from_secs(120),
            grace: Duration::from_secs(180),
            pressure: 0.5,
            resolve_after: Duration::from_secs(600),
        }
    }
}

/// A trip is noted at most this often per host.
pub const TRIP_EVERY: Duration = Duration::from_secs(300);

/// The note and the audit name an auto-resolve leaves on a case.
pub const RESOLVED_NOTE: &str = "resolved: lag recovered";

/// One host as the sampler saw it this second.
#[derive(Clone, Copy, Debug)]
pub struct Sample<'a> {
    pub host: &'a str,
    /// [`crate::upstream::HostEntry::host_lag_ms`]; None while not live.
    pub lag_ms: Option<i64>,
    /// When the relay last held it back (now, while it does).
    pub held_at_ms: Option<i64>,
}

/// This node's own load, 0-1 of each cap.
#[derive(Clone, Copy, Debug, Default, PartialEq)]
pub struct Pressure {
    pub inflight: f64,
    pub lanes: f64,
}

impl Pressure {
    pub fn max(&self) -> f64 {
        self.inflight.max(self.lanes)
    }
}

#[derive(Clone, Copy, Debug, PartialEq)]
pub struct Trip {
    pub lag_ms: i64,
    pub pressure: Pressure,
}

#[derive(Default)]
struct Track {
    /// (since, lag then) while over the threshold and counted.
    over: Option<(i64, i64)>,
    /// Since when the lag has been under the threshold.
    under: Option<i64>,
    last_trip: Option<i64>,
}

#[derive(Default)]
pub struct LagWatch {
    cfg: LagCaseConfig,
    hosts: HashMap<String, Track>,
    pressured_at: Option<i64>,
}

impl LagWatch {
    pub fn new(cfg: LagCaseConfig) -> LagWatch {
        LagWatch { cfg, ..Default::default() }
    }

    pub fn config(&self) -> &LagCaseConfig {
        &self.cfg
    }

    /// One second's samples (every host this node reads); returns the hosts
    /// that trip. Hosts missing from `samples` are forgotten.
    pub fn observe(&mut self, now_ms: i64, pressure: Pressure, samples: &[Sample<'_>]) -> Vec<(String, Trip)> {
        let ms = |d: Duration| d.as_millis() as i64;
        let (threshold, grace) = (ms(self.cfg.threshold), ms(self.cfg.grace));
        if pressure.max() > self.cfg.pressure {
            self.pressured_at = Some(now_ms);
        }
        let node_held = self.pressured_at.is_some_and(|t| now_ms - t < grace);
        let mut trips = Vec::new();
        let mut seen = std::collections::HashSet::with_capacity(samples.len());
        for s in samples {
            seen.insert(s.host);
            let t = self.hosts.entry(s.host.to_string()).or_default();
            let Some(lag) = s.lag_ms else {
                *t = Track { last_trip: t.last_trip, ..Default::default() };
                continue;
            };
            if lag < threshold {
                t.over = None;
                t.under.get_or_insert(now_ms);
                continue;
            }
            t.under = None;
            let relay_held = node_held || s.held_at_ms.is_some_and(|h| now_ms - h < grace);
            if relay_held {
                t.over = None;
                continue;
            }
            let (since, lag0) = *t.over.get_or_insert((now_ms, lag));
            let elapsed = now_ms - since;
            // catching up faster than a tenth of real time: not falling behind
            if lag0 - lag > elapsed / 10 {
                t.over = Some((now_ms, lag));
                continue;
            }
            if elapsed >= ms(self.cfg.sustain) && t.last_trip.is_none_or(|l| now_ms - l >= ms(TRIP_EVERY)) {
                t.last_trip = Some(now_ms);
                trips.push((s.host.to_string(), Trip { lag_ms: lag, pressure }));
            }
        }
        self.hosts.retain(|h, _| seen.contains(h.as_str()));
        trips
    }

    /// The host's lag has been under the threshold for
    /// [`LagCaseConfig::resolve_after`] as of `now_ms`.
    pub fn recovered(&self, host: &str, now_ms: i64) -> bool {
        let after = self.cfg.resolve_after.as_millis() as i64;
        self.hosts.get(host).and_then(|t| t.under).is_some_and(|u| now_ms - u >= after)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    const MIN: i64 = 60_000;
    const T0: i64 = 1_800_000_000_000;

    fn s(lag: i64) -> Sample<'static> {
        Sample { host: "pds.example.com", lag_ms: Some(lag), held_at_ms: None }
    }

    /// Runs `secs` one-second samples from `from`; returns the trip times.
    fn run(w: &mut LagWatch, from: i64, secs: i64, p: Pressure, f: impl Fn(i64) -> Sample<'static>) -> Vec<i64> {
        let mut out = Vec::new();
        for i in 0..secs {
            let now = from + i * 1000;
            if !w.observe(now, p, &[f(now)]).is_empty() {
                out.push(now);
            }
        }
        out
    }

    #[test]
    fn a_host_behind_with_room_trips_after_the_sustain() {
        let mut w = LagWatch::new(LagCaseConfig::default());
        let trips = run(&mut w, T0, 400, Pressure::default(), |_| s(15 * MIN));
        // two minutes in, then not again inside TRIP_EVERY
        assert_eq!(trips, vec![T0 + 2 * MIN]);
        // falling further behind trips too
        let mut w = LagWatch::new(LagCaseConfig::default());
        assert_eq!(run(&mut w, T0, 130, Pressure::default(), |n| s(10 * MIN + (n - T0))).len(), 1);
    }

    #[test]
    fn a_reader_catching_up_doesnt_trip() {
        let mut w = LagWatch::new(LagCaseConfig::default());
        // a day behind, closing at 5 s a second
        let trips = run(&mut w, T0, 600, Pressure::default(), |n| s(24 * 60 * MIN - (n - T0) * 5));
        assert!(trips.is_empty());
    }

    #[test]
    fn relay_side_lag_doesnt_trip_and_the_grace_holds_after() {
        let cfg = LagCaseConfig::default();
        // the host held by the relay, then released
        let mut w = LagWatch::new(cfg);
        let held = |n: i64| Sample { held_at_ms: Some(n.min(T0 + 5 * MIN)), ..s(15 * MIN) };
        let trips = run(&mut w, T0, 15 * 60, Pressure::default(), held);
        // released at 5 min: 3 min of grace, then the 2 min sustain
        assert_eq!(trips, vec![T0 + 10 * MIN]);
        // the node itself over its pressure line
        let mut w = LagWatch::new(cfg);
        let busy = Pressure { inflight: 0.9, lanes: 0.1 };
        assert!(run(&mut w, T0, 10 * 60, busy, |_| s(15 * MIN)).is_empty());
        let from = T0 + 10 * MIN;
        let calm = Pressure { inflight: 0.4, lanes: 0.2 };
        assert_eq!(
            run(&mut w, from, 10 * 60, calm, |_| s(15 * MIN)),
            vec![from + 5 * MIN - 1000, from + 10 * MIN - 1000]
        );
    }

    #[test]
    fn recovery_takes_the_whole_window_under_the_threshold() {
        let mut w = LagWatch::new(LagCaseConfig::default());
        run(&mut w, T0, 60, Pressure::default(), |_| s(15 * MIN));
        assert!(!w.recovered("pds.example.com", T0 + MIN));
        run(&mut w, T0 + MIN, 9 * 60, Pressure::default(), |_| s(0));
        assert!(!w.recovered("pds.example.com", T0 + 10 * MIN - 1000));
        // a blip over the threshold starts it again
        run(&mut w, T0 + 10 * MIN, 1, Pressure::default(), |_| s(11 * MIN));
        run(&mut w, T0 + 10 * MIN + 1000, 11 * 60, Pressure::default(), |_| s(0));
        assert!(!w.recovered("pds.example.com", T0 + 20 * MIN));
        assert!(w.recovered("pds.example.com", T0 + 20 * MIN + 1000));
        // a host that isn't live hasn't recovered, and one no longer read is forgotten
        w.observe(T0 + 30 * MIN, Pressure::default(), &[Sample { lag_ms: None, ..s(0) }]);
        assert!(!w.recovered("pds.example.com", T0 + 30 * MIN));
        w.observe(T0 + 30 * MIN, Pressure::default(), &[]);
        assert!(w.hosts.is_empty());
    }
}
