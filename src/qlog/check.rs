//! The emission checker: every emitted seq survives every takeover.
//!
//! It watches every node's emitted stream (in-process taps, or websocket
//! consumers in the chaos harness) and holds:
//!
//! - one content per seq across every node and every incarnation: a seq
//!   emitted with two contents was emitted, lost and reissued;
//! - each stream strictly increasing and dense: a repeat or a step back is a
//!   seq emitted twice to one consumer, a hole is a seq lost to it (a skip a
//!   consumer was told about, like `OutdatedCursor`, is counted apart);
//! - at the end, every seq a submitter was acked for was emitted with the
//!   content it was acked with, and every node's committed log agrees with
//!   everything emitted.
//!
//! After a bucket recovery (a lost quorum) the log skips the seqs in
//! `(S, R]` (`flush::Manifest::gaps`). The end checks then also hold:
//!
//! - a stream may jump only across gaps: every seq it skipped without
//!   notice is in one;
//! - an acked seq in a gap was lost, and its event (by DID) must have been
//!   emitted again above that gap (re-ingested);
//! - every event the hosts sent is emitted at a seq outside every gap.
//!
//! An event emitted at two seqs isn't a violation: a host owner resends a
//! batch whose ack it never got, and a re-ingest resends what follows the
//! cursors. Both are counted (`duplicates`, and `duplicates_across_gaps`
//! for the pairs a recovery separates).

use std::collections::HashMap;

/// FNV-1a: stable across processes, so a load generator and a checker
/// agree on an event's content id.
pub fn content_id(b: &[u8]) -> u64 {
    let mut h: u64 = 0xcbf29ce484222325;
    for &x in b {
        h ^= x as u64;
        h = h.wrapping_mul(0x100000001b3);
    }
    h
}

const KEEP_MESSAGES: usize = 50;

#[derive(Default, Debug)]
pub struct Checker {
    by_seq: HashMap<u64, u64>,
    streams: HashMap<String, u64>,
    pub violations: u64,
    pub messages: Vec<String>,
    /// Events observed across all streams.
    pub observed: u64,
    pub max_seq: u64,
    /// Seqs skipped in a stream without notice (violations) and with it.
    pub holes: u64,
    pub skipped: u64,
    acked: Vec<(u64, u64, Option<u64>)>,
    /// Skips without notice, judged at the end against the gaps:
    /// (stream, first skipped, last skipped).
    jumps: Vec<(String, u64, u64)>,
    /// Event (DID content id) -> the seqs it was emitted at.
    dids: HashMap<u64, Vec<u64>>,
}

#[derive(Debug, serde::Serialize)]
pub struct Report {
    pub ok: bool,
    pub violations: u64,
    pub messages: Vec<String>,
    pub observed: u64,
    pub distinct_seqs: u64,
    pub max_seq: u64,
    pub streams: usize,
    pub holes: u64,
    pub skipped_with_notice: u64,
    /// Seqs streams skipped across recovery gaps (the R + 1 jump).
    pub jumped: u64,
    pub acked: u64,
    pub acked_missing: u64,
    /// Acked seqs a recovery lost whose events were emitted again above
    /// the gap.
    pub reingested: u64,
    /// Events emitted at more than one seq.
    pub duplicates: u64,
    /// Of them, with a recovery gap between two of the seqs.
    pub duplicates_across_gaps: u64,
    /// Events the hosts sent that were never emitted outside a gap.
    pub events_lost: u64,
    pub log_mismatches: u64,
}

impl Checker {
    pub fn new() -> Checker {
        Checker::default()
    }

    fn violation(&mut self, m: String) {
        self.violations += 1;
        if self.messages.len() < KEEP_MESSAGES {
            tracing::error!("qlog check: {m}");
            self.messages.push(m);
        }
    }

    /// `stream` (a node incarnation, or one consumer connection) emitted `seq`.
    pub fn observe(&mut self, stream: &str, seq: u64, content: u64) {
        self.observed += 1;
        self.max_seq = self.max_seq.max(seq);
        match self.by_seq.get(&seq) {
            Some(&c) if c != content => self.violation(format!(
                "seq {seq} emitted with two contents (emitted, lost and reissued), seen on {stream}"
            )),
            Some(_) => {}
            None => {
                self.by_seq.insert(seq, content);
            }
        }
        match self.streams.get(stream).copied() {
            Some(last) if seq <= last => {
                self.violation(format!("{stream} went from seq {last} to {seq}: a repeat or a step back"))
            }
            Some(last) if seq > last + 1 => self.jumps.push((stream.to_string(), last + 1, seq - 1)),
            _ => {}
        }
        if self.streams.get(stream).is_none_or(|&l| seq > l) {
            self.streams.insert(stream.to_string(), seq);
        }
    }

    /// As `observe`, for an event identified by `did` (its DID's content id).
    pub fn observe_event(&mut self, stream: &str, seq: u64, content: u64, did: u64) {
        if !self.by_seq.contains_key(&seq) {
            self.dids.entry(did).or_default().push(seq);
        }
        self.observe(stream, seq, content);
    }

    /// The stream was told it skipped to `to` (e.g. `OutdatedCursor`).
    pub fn skip(&mut self, stream: &str, to: u64) {
        let last = self.streams.get(stream).copied().unwrap_or(0);
        if to > last {
            self.skipped += to - last;
            self.streams.insert(stream.to_string(), to);
        }
    }

    /// A stream that starts over (a reconnect without a cursor, a restart).
    pub fn restart(&mut self, stream: &str) {
        self.streams.remove(stream);
    }

    /// A submitter was told `seq` committed with this content.
    pub fn acked(&mut self, seq: u64, content: u64) {
        self.acked.push((seq, content, None));
    }

    /// As `acked`, for an event identified by `did`.
    pub fn acked_event(&mut self, seq: u64, content: u64, did: u64) {
        self.acked.push((seq, content, Some(did)));
    }

    pub fn last(&self, stream: &str) -> Option<u64> {
        self.streams.get(stream).copied()
    }

    /// Final checks against the acks and each node's committed log
    /// ((seq, content) pairs).
    pub fn finish(&mut self, logs: &[(String, Vec<(u64, u64)>)]) -> Report {
        self.finish_with(logs, &[], None)
    }

    /// As `finish`, after recoveries that skipped `gaps` (`(after, upto]`),
    /// and with `expected` the content ids of every event the hosts sent.
    pub fn finish_with(
        &mut self,
        logs: &[(String, Vec<(u64, u64)>)],
        gaps: &[(u64, u64)],
        expected: Option<&[u64]>,
    ) -> Report {
        let gap_of = |seq: u64| gaps.iter().copied().find(|&(a, u)| seq > a && seq <= u);
        let mut jumped = 0;
        for (stream, from, to) in std::mem::take(&mut self.jumps) {
            // the skipped range must be covered by gaps, one after another
            let mut s = from;
            while s <= to {
                match gap_of(s) {
                    Some((_, u)) => s = u + 1,
                    None => break,
                }
            }
            if s > to {
                jumped += to - from + 1;
            } else {
                self.holes += to - from + 1;
                self.violation(format!("{stream} skipped seqs {from}..={to} ({s} isn't in a recovery gap)"));
            }
        }
        let emitted_above = |dids: &HashMap<u64, Vec<u64>>, did: u64, u: u64| {
            dids.get(&did).is_some_and(|ss| ss.iter().any(|&x| x > u))
        };
        let acked = std::mem::take(&mut self.acked);
        let mut acked_missing = 0;
        let mut reingested = 0;
        for &(seq, content, did) in &acked {
            if let Some((a, u)) = gap_of(seq) {
                match did {
                    Some(d) if emitted_above(&self.dids, d, u) => reingested += 1,
                    _ => {
                        acked_missing += 1;
                        self.violation(format!(
                            "acked seq {seq} was lost in the recovery gap ({a}, {u}] and never re-ingested"
                        ));
                    }
                }
                continue;
            }
            match self.by_seq.get(&seq) {
                Some(&c) if c == content => {}
                Some(_) => self.violation(format!("acked seq {seq} was emitted with other content")),
                None => {
                    acked_missing += 1;
                    self.violation(format!("acked seq {seq} was never emitted"));
                }
            }
        }
        let mut events_lost = 0;
        if let Some(exp) = expected {
            for d in exp {
                let ok = self.dids.get(d).is_some_and(|ss| ss.iter().any(|&x| gap_of(x).is_none()));
                if !ok {
                    events_lost += 1;
                    self.violation(format!("event {d:016x} was never emitted outside a recovery gap"));
                }
            }
        }
        let (mut duplicates, mut duplicates_across_gaps) = (0, 0);
        for ss in self.dids.values() {
            if ss.len() < 2 {
                continue;
            }
            duplicates += 1;
            let (lo, hi) = (ss.iter().min().expect("two"), ss.iter().max().expect("two"));
            if gaps.iter().any(|&(_, u)| *lo <= u && *hi > u) {
                duplicates_across_gaps += 1;
            }
        }
        let mut log_mismatches = 0;
        for (node, log) in logs {
            for &(seq, content) in log {
                if let Some(&c) = self.by_seq.get(&seq)
                    && c != content
                {
                    log_mismatches += 1;
                    self.violation(format!(
                        "{node} holds seq {seq} committed with content other than what was emitted"
                    ));
                }
            }
            // everything emitted that this node's log covers must be in it
            if let (Some(lo), Some(hi)) = (log.first().map(|x| x.0), log.last().map(|x| x.0)) {
                let held: std::collections::HashSet<u64> = log.iter().map(|x| x.0).collect();
                for &seq in self.by_seq.keys().filter(|&&s| s >= lo && s <= hi && gap_of(s).is_none()) {
                    if !held.contains(&seq) {
                        log_mismatches += 1;
                        if log_mismatches < 5 {
                            self.messages.push(format!("{node}'s committed log lacks emitted seq {seq}"));
                        }
                    }
                }
                if self.max_seq > hi {
                    self.messages
                        .push(format!("{node}'s commit index {hi} is below the highest emitted seq {}", self.max_seq));
                    log_mismatches += 1;
                }
            }
        }
        self.violations += log_mismatches;
        Report {
            ok: self.violations == 0,
            violations: self.violations,
            messages: self.messages.clone(),
            observed: self.observed,
            distinct_seqs: self.by_seq.len() as u64,
            max_seq: self.max_seq,
            streams: self.streams.len(),
            holes: self.holes,
            skipped_with_notice: self.skipped,
            jumped,
            acked: acked.len() as u64,
            acked_missing,
            reingested,
            duplicates,
            duplicates_across_gaps,
            events_lost,
            log_mismatches,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn catches_reissue_repeat_hole_and_missing_ack() {
        let mut c = Checker::new();
        c.observe("a", 1, 10);
        c.observe("a", 2, 20);
        c.observe("b", 1, 10);
        assert_eq!(c.violations, 0);
        c.observe("b", 2, 21);
        assert_eq!(c.violations, 1, "reissued seq");
        c.observe("a", 2, 20);
        assert_eq!(c.violations, 2, "repeat");
        c.observe("a", 5, 50);
        c.skip("a", 9);
        c.observe("a", 10, 100);
        assert_eq!(c.violations, 2, "a skip with notice is no hole");
        c.acked(10, 100);
        c.acked(11, 110);
        let r = c.finish(&[]);
        assert_eq!((r.acked_missing, r.holes, r.violations), (1, 2, 4), "{r:#?}");
        assert!(!r.ok);
    }

    /// A recovery that kept 1..=3 and resumed at 11: streams may jump
    /// across (3, 10], an acked seq lost there is fine once its event is
    /// emitted again above 10, and a jump or duplicate anywhere else isn't.
    #[test]
    fn jumps_and_reingest_across_a_recovery_gap() {
        let gaps = [(3, 10)];
        let mut c = Checker::new();
        for (s, d) in [(1, 1), (2, 2), (3, 3), (4, 4), (5, 5)] {
            c.observe_event("a", s, 100 + s, d);
        }
        // the old stream ran to 5; the recovered one starts at 11 and
        // re-ingests events 4 and 5
        for (s, d) in [(11, 4), (12, 5), (13, 6)] {
            c.observe_event("a", s, 100 + s, d);
        }
        for (s, d) in [(1, 1), (2, 2), (3, 3), (11, 4), (12, 5), (13, 6)] {
            c.observe_event("b", s, 100 + s, d);
        }
        c.acked_event(5, 105, 5);
        c.acked_event(13, 113, 6);
        let r = c.finish_with(&[], &gaps, Some(&[1, 2, 3, 4, 5, 6]));
        assert!(r.ok, "{r:#?}");
        assert_eq!((r.jumped, r.reingested, r.duplicates, r.duplicates_across_gaps), (12, 1, 2, 2), "{r:#?}");

        let mut c = Checker::new();
        c.observe_event("a", 1, 101, 1);
        c.observe_event("a", 2, 102, 2);
        c.observe_event("a", 5, 105, 1);
        c.observe_event("a", 11, 111, 2);
        c.acked_event(9, 109, 9);
        let r = c.finish_with(&[], &gaps, Some(&[1, 2, 9]));
        // 3..=4 isn't all gap; acked 9 lost, never re-sent, and event 9
        // never emitted; event 1 twice below the gap is a resend
        assert_eq!((r.holes, r.duplicates, r.duplicates_across_gaps), (2, 2, 1), "{r:#?}");
        assert_eq!((r.acked_missing, r.events_lost), (1, 1), "{r:#?}");
        assert_eq!(r.violations, 3, "{r:#?}");
    }
}
