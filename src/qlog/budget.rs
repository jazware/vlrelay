//! A hard request budget for runs against a billed bucket (R2): the node
//! fail-stops (exits with [`TRIPPED`], no unwinding, so nothing more is
//! sent) once its bucket requests pass a cumulative budget, a sustained
//! rate, or a burst. It reads the same counters as `/qlog/status`'s
//! `requests` (`bucket::requests`), so every request through a counted
//! client counts, retries and SlateDB's own traffic included.
//!
//! The counters are per process. A node only sees its own requests, so the
//! cluster total is the harness's and `tests/qlog/r2_watchdog.py`'s job;
//! this is the backstop that still works when they don't. With
//! `--budget-state` the cumulative count survives restarts (the supervisor
//! restarting a node would otherwise hand it a fresh budget), and a node
//! whose saved count is already over budget trips before its first request.

use super::bucket::{self, Counts};
use std::collections::VecDeque;
use std::path::PathBuf;
use std::time::{Duration, Instant};

/// The exit code of a tripped node: a supervisor must not restart it.
pub const TRIPPED: i32 = 86;

const TICK: Duration = Duration::from_millis(250);

#[derive(clap::Args, Clone, Debug, Default)]
pub struct BudgetArgs {
    /// Fail-stop once this process (plus --budget-state) has sent this many
    /// Class A requests.
    #[arg(long)]
    pub budget_a: Option<u64>,
    #[arg(long)]
    pub budget_b: Option<u64>,
    /// Fail-stop when the mean Class A rate over the last --budget-window-s
    /// passes this (per second).
    #[arg(long)]
    pub budget_rate_a: Option<f64>,
    #[arg(long)]
    pub budget_rate_b: Option<f64>,
    #[arg(long, default_value_t = 30)]
    pub budget_window_s: u64,
    /// Fail-stop when any --budget-burst-s window holds more than this many
    /// Class A requests.
    #[arg(long)]
    pub budget_burst_a: Option<u64>,
    #[arg(long)]
    pub budget_burst_b: Option<u64>,
    #[arg(long, default_value_t = 10)]
    pub budget_burst_s: u64,
    /// Carries the cumulative count across restarts; a trip's reason goes
    /// to `<this>.tripped`.
    #[arg(long)]
    pub budget_state: Option<PathBuf>,
    /// Tests of the guards: send `get` or `put` requests to `qlog/runaway`
    /// at RATE a second (`get:200`), as a runaway would.
    #[arg(long, hide = true)]
    pub runaway: Option<String>,
}

impl BudgetArgs {
    fn limits(&self) -> Option<Limits> {
        let l = Limits {
            a: self.budget_a,
            b: self.budget_b,
            rate_a: self.budget_rate_a,
            rate_b: self.budget_rate_b,
            window: Duration::from_secs(self.budget_window_s.max(1)),
            burst_a: self.budget_burst_a,
            burst_b: self.budget_burst_b,
            burst_window: Duration::from_secs(self.budget_burst_s.max(1)),
        };
        let any = l.a.is_some()
            || l.b.is_some()
            || l.rate_a.is_some()
            || l.rate_b.is_some()
            || l.burst_a.is_some()
            || l.burst_b.is_some();
        any.then_some(l)
    }
}

#[derive(Clone, Debug)]
pub struct Limits {
    pub a: Option<u64>,
    pub b: Option<u64>,
    pub rate_a: Option<f64>,
    pub rate_b: Option<f64>,
    pub window: Duration,
    pub burst_a: Option<u64>,
    pub burst_b: Option<u64>,
    pub burst_window: Duration,
}

pub struct Guard {
    limits: Limits,
    /// Earlier incarnations' requests (from the state file).
    carried: Counts,
    history: VecDeque<(Instant, u64, u64)>,
}

impl Guard {
    pub fn new(limits: Limits, carried: Counts) -> Guard {
        Guard { limits, carried, history: VecDeque::new() }
    }

    pub fn total(&self, now: &Counts) -> Counts {
        Counts { a: self.carried.a + now.a, b: self.carried.b + now.b, free: self.carried.free + now.free }
    }

    /// `process` is this process's own count; the reason on a breach.
    pub fn check(&mut self, at: Instant, process: &Counts) -> Option<String> {
        let t = self.total(process);
        let l = &self.limits;
        if let Some(n) = l.a.filter(|&n| t.a >= n) {
            return Some(format!("Class A budget: {} requests >= {n}", t.a));
        }
        if let Some(n) = l.b.filter(|&n| t.b >= n) {
            return Some(format!("Class B budget: {} requests >= {n}", t.b));
        }
        self.history.push_back((at, process.a, process.b));
        let keep = l.window.max(l.burst_window) + TICK * 2;
        while self.history.front().is_some_and(|&(t0, ..)| at.duration_since(t0) > keep) {
            self.history.pop_front();
        }
        // the oldest sample at least `w` old, so the span covers the window
        let since = |w: Duration| self.history.iter().rev().find(|&&(t0, ..)| at.duration_since(t0) >= w).copied();
        if let Some((t0, a0, b0)) = since(l.window) {
            let secs = at.duration_since(t0).as_secs_f64();
            let (ra, rb) = ((process.a - a0) as f64 / secs, (process.b - b0) as f64 / secs);
            if let Some(x) = l.rate_a.filter(|&x| ra > x) {
                return Some(format!("Class A rate: {ra:.2}/s over {secs:.0} s > {x}/s"));
            }
            if let Some(x) = l.rate_b.filter(|&x| rb > x) {
                return Some(format!("Class B rate: {rb:.2}/s over {secs:.0} s > {x}/s"));
            }
        }
        // a burst is caught before a full window has passed too: anything
        // since the oldest sample within the window counts
        let (_, a0, b0) = self
            .history
            .iter()
            .find(|&&(t0, ..)| at.duration_since(t0) <= l.burst_window)
            .copied()
            .unwrap_or((at, process.a, process.b));
        let base = if self.history.len() == 1 { (0, 0) } else { (a0, b0) };
        let (da, db) = (process.a - base.0, process.b - base.1);
        if let Some(n) = l.burst_a.filter(|&n| da > n) {
            return Some(format!("Class A burst: {da} requests in {} s > {n}", l.burst_window.as_secs()));
        }
        if let Some(n) = l.burst_b.filter(|&n| db > n) {
            return Some(format!("Class B burst: {db} requests in {} s > {n}", l.burst_window.as_secs()));
        }
        None
    }
}

fn load(path: &PathBuf) -> anyhow::Result<Counts> {
    match std::fs::read(path) {
        Ok(b) => Ok(serde_json::from_slice(&b)?),
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => Ok(Counts::default()),
        Err(e) => Err(e.into()),
    }
}

fn save(path: &PathBuf, c: &Counts) {
    let tmp = path.with_extension("tmp");
    let ok = std::fs::write(&tmp, serde_json::to_vec(c).unwrap_or_default()).and_then(|_| std::fs::rename(&tmp, path));
    if let Err(e) = ok {
        tracing::warn!("qlog budget: saving {}: {e}", path.display());
    }
}

fn trip(why: &str, state: Option<&PathBuf>, total: &Counts) -> ! {
    eprintln!("qlog budget: TRIPPED, stopping: {why} (A {} B {})", total.a, total.b);
    if let Some(p) = state {
        save(p, total);
        let _ = std::fs::write(p.with_extension("tripped"), format!("{why}\n"));
    }
    // fail-stop: no destructors, no last flush, no more requests
    unsafe { libc::_exit(TRIPPED) }
}

/// Checks the saved count before the node sends anything, then watches the
/// counters on a thread of its own (a stalled runtime can't hold it up).
/// Call before the node starts.
pub fn start(args: &BudgetArgs, store: vlpds::store::Store) -> anyhow::Result<()> {
    if let Some(spec) = &args.runaway {
        runaway(spec, store)?;
    }
    let Some(limits) = args.limits() else { return Ok(()) };
    let state = args.budget_state.clone();
    let carried = match &state {
        Some(p) => load(p)?,
        None => Counts::default(),
    };
    let mut g = Guard::new(limits.clone(), carried);
    if let Some(why) = g.check(Instant::now(), &bucket::requests().total) {
        trip(&why, state.as_ref(), &g.total(&bucket::requests().total));
    }
    tracing::info!(?limits, carried_a = g.carried.a, carried_b = g.carried.b, "qlog budget: armed");
    std::thread::Builder::new().name("qlog-budget".into()).spawn(move || {
        let mut saved = g.total(&Counts::default());
        let mut logged = Instant::now();
        loop {
            std::thread::sleep(TICK);
            let now = bucket::requests().total;
            let total = g.total(&now);
            if let Some(why) = g.check(Instant::now(), &now) {
                trip(&why, state.as_ref(), &total);
            }
            if let Some(p) = &state
                && total != saved
            {
                save(p, &total);
                saved = total.clone();
            }
            if logged.elapsed() >= Duration::from_secs(60) {
                tracing::info!(a = total.a, b = total.b, "qlog budget: requests so far");
                logged = Instant::now();
            }
        }
    })?;
    Ok(())
}

fn runaway(spec: &str, store: vlpds::store::Store) -> anyhow::Result<()> {
    use object_store::{ObjectStoreExt, PutPayload, path::Path};
    let (op, rate) = spec.split_once(':').ok_or_else(|| anyhow::anyhow!("--runaway get:RATE or put:RATE"))?;
    let rate: f64 = rate.parse()?;
    let put = match op {
        "put" => true,
        "get" => false,
        _ => anyhow::bail!("--runaway: get or put, not {op}"),
    };
    tracing::warn!("qlog budget: --runaway {op} at {rate}/s");
    let path = Path::from(format!("{}/qlog/runaway", store.prefix));
    tokio::spawn(async move {
        let mut tick = tokio::time::interval(Duration::from_secs_f64(1.0 / rate));
        loop {
            tick.tick().await;
            // not awaited in turn: the rate holds whatever the latency
            let (store, path) = (store.clone(), path.clone());
            tokio::spawn(async move {
                if put {
                    let _ = store.raw.put(&path, PutPayload::from_static(b"x")).await;
                } else {
                    let _ = store.raw.get(&path).await;
                }
            });
        }
    });
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    fn limits() -> Limits {
        Limits {
            a: Some(3500),
            b: Some(12000),
            rate_a: Some(2.4),
            rate_b: Some(8.3),
            window: Duration::from_secs(30),
            burst_a: Some(100),
            burst_b: Some(300),
            burst_window: Duration::from_secs(10),
        }
    }

    fn c(a: u64, b: u64) -> Counts {
        Counts { a, b, free: 0 }
    }

    /// Feeds `rate(t)` (A, B a second) in 250 ms ticks for `secs`; the
    /// second the guard tripped at, and why.
    fn run(g: &mut Guard, secs: f64, rate: impl Fn(f64) -> (f64, f64)) -> Option<(f64, String)> {
        let t0 = Instant::now();
        let (mut a, mut b) = (0.0, 0.0);
        let mut t = 0.0;
        while t < secs {
            t += 0.25;
            let (ra, rb) = rate(t);
            a += ra * 0.25;
            b += rb * 0.25;
            if let Some(why) = g.check(t0 + Duration::from_secs_f64(t), &c(a as u64, b as u64)) {
                return Some((t, why));
            }
        }
        None
    }

    #[test]
    fn an_hour_at_the_expected_rate_with_flush_spikes_passes() {
        // Phase 6: 0.478 A and 1.66 B a second, the A arriving as ~14 at
        // each 30 s flush
        let mut g = Guard::new(limits(), Counts::default());
        let r = run(&mut g, 3600.0, |t| (if t % 30.0 < 0.25 { 14.3 * 4.0 } else { 0.0 }, 1.66));
        assert_eq!(r, None);
    }

    #[test]
    fn the_cumulative_budget_trips_and_counts_earlier_incarnations() {
        let mut g = Guard::new(limits(), c(3490, 0));
        let (t, why) = run(&mut g, 60.0, |_| (1.0, 0.0)).unwrap();
        assert!(why.starts_with("Class A budget"), "{why}");
        assert!((9.0..=11.0).contains(&t), "{t}");
        let mut g = Guard::new(limits(), c(0, 12000));
        assert!(g.check(Instant::now(), &c(0, 0)).unwrap().starts_with("Class B budget"));
    }

    #[test]
    fn a_sustained_rate_trips_after_the_window() {
        let mut g = Guard::new(limits(), Counts::default());
        // 3 A/s is under the burst limit (30 a 10 s) but over 2.4/s
        let (t, why) = run(&mut g, 120.0, |_| (3.0, 0.0)).unwrap();
        assert!(why.starts_with("Class A rate"), "{why}");
        assert!((30.0..=31.0).contains(&t), "{t}");
        let mut g = Guard::new(limits(), Counts::default());
        let (t, why) = run(&mut g, 120.0, |_| (0.0, 9.0)).unwrap();
        assert!(why.starts_with("Class B rate"), "{why}");
        assert!((30.0..=31.0).contains(&t), "{t}");
    }

    #[test]
    fn a_burst_trips_at_once() {
        let mut g = Guard::new(limits(), Counts::default());
        let (t, why) = run(&mut g, 60.0, |t| if t > 20.0 { (0.0, 1000.0) } else { (0.0, 1.0) }).unwrap();
        assert!(why.starts_with("Class B burst"), "{why}");
        assert!(t <= 20.75, "{t}");
        // from the very first sample too
        let mut g = Guard::new(limits(), Counts::default());
        assert!(g.check(Instant::now(), &c(101, 0)).unwrap().starts_with("Class A burst"));
    }
}
