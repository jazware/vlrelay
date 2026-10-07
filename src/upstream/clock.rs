//! A host's own timeline, for its limits and spam signals.
//!
//! Limits count a host's events against the `time` the host stamped on them,
//! not when the relay read them, so a backlog replayed after a relay
//! restart, a reconnect or a cursor resume costs what the original traffic
//! cost instead of an hour's events landing in a few minutes. The clock is
//! the newest event time seen, clamped:
//!
//! - never past now, so a future-dated event can't buy room;
//! - never back (it's a max), so a host can't spend the same stretch twice;
//! - never further back than [`EventClock::horizon`], so a stale or bogus
//!   `time` counts at the horizon;
//! - forward by a limiter pause's length (still never past now), so a host
//!   paying off debt pays it in real time and an over-limit backlog isn't
//!   stuck behind debt that only event time could clear.
//!
//! Over any stretch the clock advances at most by the wall time plus how far
//! behind now it started, which is at most the horizon the first time a
//! process sees the host and the real time it was away after that (the
//! clock outlives the host's sockets).

/// The fastest pace a host's timeline is taken to move at. Past it events
/// still count, just not any lighter.
pub const PACE_MAX: f64 = 1_000.0;

/// How often the pace is re-measured.
const PACE_EVERY_MS: i64 = 1_000;

#[derive(Clone, Debug)]
pub struct EventClock {
    /// Unix ms; 0 until the first frame.
    clock_ms: i64,
    /// (wall, clock) at the last pace measurement.
    mark: (i64, i64),
    /// Host time per wall time over the last measurements, at least 1.
    pace: f64,
}

impl Default for EventClock {
    fn default() -> Self {
        EventClock { clock_ms: 0, mark: (0, 0), pace: 1.0 }
    }
}

impl EventClock {
    /// The clock after a frame stamped `event_ms` (None: no usable `time`)
    /// read at `now_ms`.
    pub fn on_frame(&mut self, event_ms: Option<i64>, now_ms: i64, horizon_ms: i64) -> i64 {
        let floor = now_ms - horizon_ms.max(0);
        let at = event_ms.map_or(self.clock_ms, |e| e.clamp(floor, now_ms));
        if self.clock_ms == 0 {
            self.clock_ms = at.max(floor);
            self.mark = (now_ms, self.clock_ms);
        } else {
            self.clock_ms = self.clock_ms.max(at).max(floor);
        }
        self.measure(now_ms);
        self.clock_ms
    }

    /// A limiter pause of `ms` the reader sat out.
    pub fn paused(&mut self, ms: i64, now_ms: i64) {
        if self.clock_ms != 0 {
            self.clock_ms = self.clock_ms.max((self.clock_ms + ms).min(now_ms));
        }
    }

    fn measure(&mut self, now_ms: i64) {
        let dt = now_ms - self.mark.0;
        if dt < PACE_EVERY_MS {
            return;
        }
        let inst = (self.clock_ms - self.mark.1) as f64 / dt as f64;
        self.pace = (0.5 * self.pace + 0.5 * inst).clamp(1.0, PACE_MAX);
        self.mark = (now_ms, self.clock_ms);
    }

    pub fn clock_ms(&self) -> i64 {
        self.clock_ms
    }

    /// How many seconds of the host's timeline go by per wall second: 1
    /// when live, more while it catches up. A quiet host's pace is stale,
    /// so past a couple of measurement periods it reads as 1.
    pub fn pace(&self, now_ms: i64) -> f64 {
        if now_ms - self.mark.0 > 3 * PACE_EVERY_MS { 1.0 } else { self.pace }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    const H: i64 = 24 * 3_600_000;
    const T0: i64 = 1_800_000_000_000;

    #[test]
    fn future_dated_events_count_now_and_the_clock_never_goes_back() {
        let mut c = EventClock::default();
        assert_eq!(c.on_frame(Some(T0 + 60_000), T0, H), T0);
        assert_eq!(c.on_frame(Some(T0 - 30_000), T0 + 10, H), T0);
        assert_eq!(c.on_frame(None, T0 + 20, H), T0);
        assert_eq!(c.on_frame(Some(T0 + 15), T0 + 20, H), T0 + 15);
    }

    #[test]
    fn stale_times_count_at_the_horizon() {
        let mut c = EventClock::default();
        assert_eq!(c.on_frame(Some(T0 - 9 * H), T0, H), T0 - H);
        // the floor moves with the wall clock
        assert_eq!(c.on_frame(Some(T0 - 9 * H), T0 + 5_000, H), T0 - H + 5_000);
    }

    #[test]
    fn a_replay_runs_fast_and_a_live_host_at_one() {
        let mut c = EventClock::default();
        // an hour of backlog read in a minute: 60× its own pace
        for i in 0..=60 {
            c.on_frame(Some(T0 - 3_600_000 + i * 60_000), T0 + i * 1_000, H);
        }
        let p = c.pace(T0 + 60_000);
        assert!(p > 30.0, "{p}");
        // then live: back toward 1
        for i in 61..=80 {
            c.on_frame(Some(T0 + i * 1_000), T0 + i * 1_000, H);
        }
        assert!(c.pace(T0 + 80_000) < 1.01);
        // and a host gone quiet reads as 1
        let mut q = EventClock::default();
        for i in 0..=5 {
            q.on_frame(Some(T0 - 3_600_000 + i * 600_000), T0 + i * 1_000, H);
        }
        assert!(q.pace(T0 + 5_000) > 1.0);
        assert_eq!(q.pace(T0 + 60_000), 1.0);
    }

    #[test]
    fn pauses_move_the_clock_up_to_now() {
        let mut c = EventClock::default();
        c.on_frame(Some(T0 - 10_000), T0, H);
        c.paused(4_000, T0 + 4_000);
        assert_eq!(c.clock_ms(), T0 - 6_000);
        c.paused(60_000, T0 + 5_000);
        assert_eq!(c.clock_ms(), T0 + 5_000);
    }
}
