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
    acked: Vec<(u64, u64)>,
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
    pub acked: u64,
    pub acked_missing: u64,
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
            Some(last) if seq > last + 1 => {
                self.holes += seq - last - 1;
                self.violation(format!("{stream} skipped seqs {}..={}", last + 1, seq - 1));
            }
            _ => {}
        }
        if self.streams.get(stream).is_none_or(|&l| seq > l) {
            self.streams.insert(stream.to_string(), seq);
        }
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
        self.acked.push((seq, content));
    }

    pub fn last(&self, stream: &str) -> Option<u64> {
        self.streams.get(stream).copied()
    }

    /// Final checks against the acks and each node's committed log
    /// ((seq, content) pairs).
    pub fn finish(&mut self, logs: &[(String, Vec<(u64, u64)>)]) -> Report {
        let acked = std::mem::take(&mut self.acked);
        let mut acked_missing = 0;
        for &(seq, content) in &acked {
            match self.by_seq.get(&seq) {
                Some(&c) if c == content => {}
                Some(_) => self.violation(format!("acked seq {seq} was emitted with other content")),
                None => {
                    acked_missing += 1;
                    self.violation(format!("acked seq {seq} was never emitted"));
                }
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
                for &seq in self.by_seq.keys().filter(|&&s| s >= lo && s <= hi) {
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
            acked: acked.len() as u64,
            acked_missing,
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
        assert_eq!((c.violations, c.holes), (3, 2), "hole");
        c.skip("a", 9);
        c.observe("a", 10, 100);
        assert_eq!(c.violations, 3, "a skip with notice is no hole");
        c.acked(10, 100);
        c.acked(11, 110);
        let r = c.finish(&[]);
        assert_eq!((r.acked_missing, r.violations), (1, 4));
        assert!(!r.ok);
    }
}
