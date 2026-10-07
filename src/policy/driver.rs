//! Applies the tier state machine and the spam trips to host records.
//!
//! - [`Driver::process_trips`] (every second): drains the engine's trips,
//!   logs alerts, throttles hosts whose trip says so and opens or updates
//!   cases.
//! - [`Driver::sweep`] (every 30 s): steps every host this node's
//!   `HostStore` lists, with the counter deltas since the last sweep, for
//!   the error budget, promotion and recovery. An error-budget throttle
//!   opens or updates an `error-budget` case.
//!
//! Every tier change the relay makes goes on the host's action trail as
//! `relay (service)`, with its reason and the case it opened, if any.
//!
//! Records are only written when the tier or the policy state changes.

use super::Engine;
use super::cases::{CaseOpen, Evidence, Opened};
use super::doc::{SpamAction, tier_name};
use super::signals::Trip;
use super::store::now_ms;
use super::tiers::{self, Obs};
use crate::admin::Severity;
use crate::state::{HostStore, Tier};
use parking_lot::Mutex;
use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;

/// The case kind the error budget's auto-throttle opens.
pub const ERROR_BUDGET_CASE: &str = "error-budget";
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

/// A tier change [`Driver::step_host`] wrote.
struct Stepped {
    from: Tier,
    to: Tier,
    /// Its trail entry's time.
    at_ms: i64,
}

impl Stepped {
    fn auto_action(&self) -> String {
        format!("{} from {}", tier_name(self.to), tier_name(self.from))
    }
}

pub struct Driver {
    engine: Arc<Engine>,
    hosts: Arc<dyn HostStore>,
    /// (events, failed) at the last sweep. Kept in memory only: after a
    /// restart (or a host moving here) the first sweep just takes a baseline.
    seen: Mutex<HashMap<String, (u64, u64)>>,
}

pub fn severity(t: &Trip) -> Severity {
    let over = if t.threshold > 0.0 { t.observed / t.threshold } else { 1.0 };
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
    format!("{who}: {} {:.0}{per} (threshold {:.0})", t.rule.name(), t.observed, t.threshold)
}

impl Driver {
    pub fn new(engine: Arc<Engine>, hosts: Arc<dyn HostStore>) -> Driver {
        Driver { engine, hosts, seen: Default::default() }
    }

    /// Steps one host and writes it if anything changed. The step runs on
    /// the record as it is under the host's lock, so a counter flush or an
    /// operator action landing meanwhile isn't overwritten. A tier change
    /// goes on the host's action trail as the relay's.
    async fn step_host(
        &self,
        hostname: &str,
        obs: &Obs,
        now: u32,
        report: &mut Report,
    ) -> anyhow::Result<Option<Stepped>> {
        let p = self.engine.snapshot().policy.body.clone();
        let at_ms = now_ms();
        let mut moved: Option<(crate::state::Tier, tiers::Change)> = None;
        let out = &mut moved;
        let written = self
            .hosts
            .update_host(
                hostname,
                Box::new(move |cur| {
                    let mut rec = cur?;
                    let st = tiers::host_policy(&rec);
                    let ch = tiers::step(rec.tier, rec.first_seen, &st, obs, &p, now)?;
                    let from = rec.tier;
                    rec.tier = ch.tier;
                    tiers::set_host_policy(&mut rec, &ch.state);
                    if let Some(reason) = &ch.reason {
                        tiers::record_action(&mut rec, &tiers::relay_action(ch.tier, reason, at_ms));
                    }
                    *out = Some((from, ch));
                    Some(rec)
                }),
            )
            .await?;
        let (Some(rec), Some((from, ch))) = (written, moved) else {
            return Ok(None);
        };
        report.written += 1;
        let Some(reason) = ch.reason else {
            return Ok(None);
        };
        tracing::info!(
            target: "vlrelay::audit",
            host = %rec.hostname,
            from = tier_name(from),
            to = tier_name(ch.tier),
            by = tiers::RELAY_ACTOR,
            %reason,
            "host tier changed"
        );
        report.moved.push(Moved { host: rec.hostname.clone(), from, to: ch.tier, reason });
        Ok(Some(Stepped { from, to: ch.tier, at_ms }))
    }

    /// Puts the case on the trail entry of the tier change that opened or
    /// updated it. Best effort: the case already names the action.
    async fn link_case(&self, host: &str, s: &Stepped, case: u64) {
        let at_ms = s.at_ms;
        let r = self
            .hosts
            .update_host(
                host,
                Box::new(move |cur| {
                    let mut rec = cur?;
                    tiers::link_case(&mut rec, at_ms, case).then_some(rec)
                }),
            )
            .await;
        if let Err(e) = r {
            tracing::warn!(host, case, "auto-throttle's case not linked on its trail: {e:#}");
        }
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
        if t.action == SpamAction::Alert {
            report.alerts += 1;
            tracing::warn!(host = %t.host, did = ?t.did, rule = t.rule.name(), observed = t.observed, threshold = t.threshold, "spam threshold crossed");
        }
        let mut stepped = None;
        if t.action.throttles() {
            let obs = Obs { spam_trip: Some(t.rule.name().to_string()), ..Default::default() };
            stepped = self.step_host(&t.host, &obs, now, report).await?;
        }
        if t.action.opens_case() {
            let ev = Evidence {
                at_ms: t.at_ms,
                observed: t.observed,
                threshold: t.threshold,
                window_secs: t.window_secs,
                node: self.engine.node.clone(),
                detail: t.detail.clone(),
                signals: self.engine.signals.snapshot(&t.host, t.did.as_deref(), t.at_ms),
            };
            let o = CaseOpen {
                kind: t.rule.name().to_string(),
                host: t.host.clone(),
                did: t.did.clone(),
                severity: severity(t),
                summary: summary(t),
                observed: t.observed,
                threshold: t.threshold,
                auto_action: stepped.as_ref().map(Stepped::auto_action),
                evidence: ev,
            };
            let opened = self.engine.cases.open_or_update(o).await?;
            report.cases.push(opened);
            if let Some(s) = &stepped {
                self.link_case(&t.host, s, opened.id()).await;
            }
        }
        Ok(())
    }

    /// The error budget throttles without a spam rule, so it opens its own
    /// case: an operator looking at the host's cases sees why it moved.
    async fn error_budget_case(&self, host: &str, obs: &Obs, s: &Stepped) -> anyhow::Result<Opened> {
        let tr = self.engine.snapshot().policy.body.transitions.clone();
        let total = obs.events + obs.failed;
        let pct = if total == 0 { 0.0 } else { obs.failed as f64 * 100.0 / total as f64 };
        let budget = tr.error_ratio * 100.0;
        let o = CaseOpen {
            kind: ERROR_BUDGET_CASE.into(),
            host: host.to_string(),
            did: None,
            severity: Severity::High,
            summary: format!("{host}: {pct:.0}% of {total} frames failed checks (budget {budget:.0}%)"),
            observed: pct,
            threshold: budget,
            auto_action: Some(s.auto_action()),
            evidence: Evidence {
                at_ms: s.at_ms,
                observed: pct,
                threshold: budget,
                window_secs: SWEEP_EVERY.as_secs() as u32,
                node: self.engine.node.clone(),
                detail: Some(format!("{} of {total} frames failed checks", obs.failed)),
                signals: self.engine.signals.snapshot(host, None, s.at_ms),
            },
        };
        self.engine.cases.open_or_update(o).await
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
                    Some((e, f)) => {
                        Obs { events: cur.0.saturating_sub(e), failed: cur.1.saturating_sub(f), spam_trip: None }
                    }
                    None => Obs::default(),
                };
                let Some(s) = self.step_host(&rec.hostname, &obs, now, &mut report).await? else { continue };
                if s.to != Tier::Throttled {
                    continue;
                }
                match self.error_budget_case(&rec.hostname, &obs, &s).await {
                    Ok(opened) => {
                        report.cases.push(opened);
                        self.link_case(&rec.hostname, &s, opened.id()).await;
                    }
                    Err(e) => {
                        report.errors += 1;
                        tracing::warn!(host = %rec.hostname, "error-budget case not opened: {e:#}");
                    }
                }
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
