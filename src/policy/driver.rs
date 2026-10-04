//! Applies the tier state machine and the spam trips to host records.
//!
//! - [`Driver::process_trips`] (every second): drains the engine's trips,
//!   logs alerts, throttles hosts whose trip says so and opens or updates
//!   cases.
//! - [`Driver::sweep`] (every 30 s): steps every host this node's
//!   `HostStore` lists, with the counter deltas since the last sweep, for
//!   the error budget, promotion and recovery.
//!
//! Records are only written when the tier or the policy state changes.

use super::Engine;
use super::cases::{CaseOpen, Evidence, Opened};
use super::doc::{SpamAction, tier_name};
use super::signals::Trip;
use super::tiers::{self, Obs};
use crate::admin::Severity;
use crate::state::{HostRecord, HostStore};
use parking_lot::Mutex;
use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;

pub const TRIPS_EVERY: Duration = Duration::from_secs(1);
pub const SWEEP_EVERY: Duration = Duration::from_secs(30);
const PAGE: usize = 1_000;

#[derive(Clone, Debug, PartialEq)]
pub struct Moved {
    pub host: String,
    pub from: crate::state::Tier,
    pub to: crate::state::Tier,
    pub reason: String,
}

#[derive(Debug, Default)]
pub struct Report {
    pub scanned: usize,
    pub written: usize,
    pub moved: Vec<Moved>,
    pub alerts: usize,
    pub cases: Vec<Opened>,
    /// Trips that hit a store error (logged, then dropped: the signal trips
    /// again next window if it keeps up).
    pub errors: usize,
}

pub struct Driver {
    engine: Arc<Engine>,
    hosts: Arc<dyn HostStore>,
    /// (events, failed) at the last sweep. Kept in memory only: after a
    /// restart (or a host moving here) the first sweep just takes a baseline.
    seen: Mutex<HashMap<String, (u64, u64)>>,
}

pub fn severity(t: &Trip) -> Severity {
    let over = if t.threshold > 0.0 {
        t.observed / t.threshold
    } else {
        1.0
    };
    match t.action {
        _ if over >= 5.0 => Severity::Critical,
        SpamAction::ThrottleAndCase | SpamAction::Throttle => Severity::High,
        SpamAction::Case => Severity::Warn,
        SpamAction::Alert => Severity::Info,
    }
}

pub fn summary(t: &Trip) -> String {
    let per = match t.window_secs {
        60 => "/min".to_string(),
        3_600 => "/h".to_string(),
        s => format!(" per {s} s"),
    };
    let who = match &t.did {
        Some(d) => format!("{d} on {}", t.host),
        None => t.host.clone(),
    };
    format!(
        "{who}: {} {:.0}{per} (threshold {:.0})",
        t.rule.name(),
        t.observed,
        t.threshold
    )
}

impl Driver {
    pub fn new(engine: Arc<Engine>, hosts: Arc<dyn HostStore>) -> Driver {
        Driver {
            engine,
            hosts,
            seen: Default::default(),
        }
    }

    /// Steps one host and writes it if anything changed.
    async fn step_host(
        &self,
        mut rec: HostRecord,
        obs: &Obs,
        now: u32,
        report: &mut Report,
    ) -> anyhow::Result<()> {
        let p = self.engine.snapshot().policy.body.clone();
        let st = tiers::host_policy(&rec);
        let Some(ch) = tiers::step(rec.tier, rec.first_seen, &st, obs, &p, now) else {
            return Ok(());
        };
        let from = rec.tier;
        rec.tier = ch.tier;
        tiers::set_host_policy(&mut rec, &ch.state);
        self.hosts.put_host(&rec).await?;
        report.written += 1;
        if let Some(reason) = ch.reason {
            tracing::info!(
                target: "vlrelay::audit",
                host = %rec.hostname,
                from = tier_name(from),
                to = tier_name(ch.tier),
                %reason,
                "host tier changed"
            );
            report.moved.push(Moved {
                host: rec.hostname.clone(),
                from,
                to: ch.tier,
                reason,
            });
        }
        Ok(())
    }

    pub async fn process_trips(&self) -> anyhow::Result<Report> {
        self.process(self.engine.signals.drain()).await
    }

    pub async fn process(&self, trips: Vec<Trip>) -> anyhow::Result<Report> {
        let mut report = Report::default();
        let now = crate::state::now_secs();
        for t in trips {
            if let Err(e) = self.process_one(&t, now, &mut report).await {
                report.errors += 1;
                tracing::warn!(host = %t.host, rule = t.rule.name(), "spam trip not applied: {e:#}");
            }
        }
        Ok(report)
    }

    async fn process_one(&self, t: &Trip, now: u32, report: &mut Report) -> anyhow::Result<()> {
        {
            let throttles = t.action.throttles();
            if t.action == SpamAction::Alert {
                report.alerts += 1;
                tracing::warn!(host = %t.host, did = ?t.did, rule = t.rule.name(), observed = t.observed, threshold = t.threshold, "spam threshold crossed");
            }
            let mut auto_action = None;
            if throttles && let Some(rec) = self.hosts.get_host(&t.host).await? {
                let obs = Obs {
                    spam_trip: Some(t.rule.name().to_string()),
                    ..Default::default()
                };
                let before = report.moved.len();
                self.step_host(rec, &obs, now, report).await?;
                if report.moved.len() > before {
                    auto_action = Some("throttled".to_string());
                }
            }
            if t.action.opens_case() {
                let ev = Evidence {
                    at_ms: t.at_ms,
                    observed: t.observed,
                    threshold: t.threshold,
                    window_secs: t.window_secs,
                    node: self.engine.node.clone(),
                    detail: t.detail.clone(),
                    signals: self
                        .engine
                        .signals
                        .snapshot(&t.host, t.did.as_deref(), t.at_ms),
                };
                let o = CaseOpen {
                    kind: t.rule.name().to_string(),
                    host: t.host.clone(),
                    did: t.did.clone(),
                    severity: severity(t),
                    summary: summary(t),
                    observed: t.observed,
                    threshold: t.threshold,
                    auto_action,
                    evidence: ev,
                };
                report
                    .cases
                    .push(self.engine.cases.open_or_update(o).await?);
            }
        }
        Ok(())
    }

    /// Steps every host with its counter deltas since the last sweep.
    pub async fn sweep(&self) -> anyhow::Result<Report> {
        self.sweep_at(crate::state::now_secs()).await
    }

    pub async fn sweep_at(&self, now: u32) -> anyhow::Result<Report> {
        let mut report = Report::default();
        let mut cursor: Option<String> = None;
        let mut live = HashMap::new();
        loop {
            let page = self.hosts.list_hosts(cursor.as_deref(), PAGE).await?;
            for rec in page.hosts {
                report.scanned += 1;
                let cur = (rec.events, rec.failed_checks);
                let prev = self.seen.lock().get(&rec.hostname).copied();
                live.insert(rec.hostname.clone(), cur);
                let obs = match prev {
                    Some((e, f)) => Obs {
                        events: cur.0.saturating_sub(e),
                        failed: cur.1.saturating_sub(f),
                        spam_trip: None,
                    },
                    None => Obs::default(),
                };
                self.step_host(rec, &obs, now, &mut report).await?;
            }
            match page.cursor {
                Some(c) => cursor = Some(c),
                None => break,
            }
        }
        // hosts that left this node drop out here
        *self.seen.lock() = live;
        Ok(report)
    }

    /// Runs both loops until the driver is dropped.
    pub fn spawn(self: &Arc<Self>) {
        let weak = Arc::downgrade(self);
        tokio::spawn(async move {
            let mut trips = tokio::time::interval(TRIPS_EVERY);
            let mut sweep = tokio::time::interval(SWEEP_EVERY);
            trips.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
            sweep.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
            loop {
                tokio::select! {
                    _ = trips.tick() => {
                        let Some(d) = weak.upgrade() else { return };
                        if let Err(e) = d.process_trips().await {
                            tracing::warn!("policy trips: {e:#}");
                        }
                    }
                    _ = sweep.tick() => {
                        let Some(d) = weak.upgrade() else { return };
                        if let Err(e) = d.sweep().await {
                            tracing::warn!("policy sweep: {e:#}");
                        }
                    }
                }
            }
        });
    }
}
