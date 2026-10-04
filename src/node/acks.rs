//! Per-host ack tracking. Lanes finish a host's events out of order, but its
//! cursor may only move past a seq once that seq and every earlier one is
//! done: durable in the log, rejected, or skipped.

use crate::types::Host;
use parking_lot::Mutex;
use std::collections::{BTreeMap, HashMap};
use std::time::Instant;

#[derive(Default)]
struct Entry {
    /// Copies still in the pipeline (a reconnect can replay a seq that's
    /// still in flight from the old socket).
    pending: u32,
    since: Option<Instant>,
    /// Lowest log ordinal a durable copy landed in.
    ordinal: Option<u64>,
}

#[derive(Default)]
struct HostAcks {
    seqs: BTreeMap<i64, Entry>,
}

#[derive(Default)]
pub struct Tracker {
    hosts: Mutex<HashMap<Host, HostAcks>>,
}

#[derive(Debug, Default)]
pub struct Snapshot {
    /// Lowest ordinal of a durable event its host's cursor hasn't passed.
    pub min_ordinal_above_ack: Option<u64>,
    pub oldest_pending: Option<Instant>,
    pub pending: usize,
}

impl Tracker {
    /// A frame read off `host`'s socket (seq 0: one without a seq).
    /// Returns false when this seq is already in the pipeline or done but
    /// not yet acked: a replay after a reconnect.
    pub fn begin(&self, host: &Host, seq: i64) -> bool {
        if seq <= 0 {
            return true;
        }
        let mut m = self.hosts.lock();
        let seqs = &mut m.entry(host.clone()).or_default().seqs;
        let first = !seqs.contains_key(&seq);
        let e = seqs.entry(seq).or_default();
        e.pending += 1;
        e.since.get_or_insert_with(Instant::now);
        first
    }

    /// The frame is done. Returns the host's new ack cursor, if it moved.
    pub fn finish(&self, host: &Host, seq: i64, ordinal: Option<u64>) -> Option<i64> {
        if seq <= 0 {
            return None;
        }
        let mut m = self.hosts.lock();
        let h = m.get_mut(host)?;
        let e = h.seqs.get_mut(&seq)?;
        e.pending = e.pending.saturating_sub(1);
        if let Some(o) = ordinal {
            e.ordinal = Some(e.ordinal.map_or(o, |x| x.min(o)));
        }
        let mut acked = None;
        while let Some(first) = h.seqs.first_entry() {
            if first.get().pending > 0 {
                break;
            }
            acked = Some(*first.key());
            first.remove();
        }
        acked
    }

    pub fn snapshot(&self) -> Snapshot {
        let m = self.hosts.lock();
        let mut s = Snapshot::default();
        for h in m.values() {
            for e in h.seqs.values() {
                if e.pending > 0 {
                    s.pending += 1;
                    if let Some(t) = e.since {
                        s.oldest_pending = Some(s.oldest_pending.map_or(t, |o: Instant| o.min(t)));
                    }
                } else if let Some(o) = e.ordinal {
                    s.min_ordinal_above_ack = Some(s.min_ordinal_above_ack.map_or(o, |x: u64| x.min(o)));
                }
            }
        }
        s
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn acks_only_a_done_prefix() {
        let t = Tracker::default();
        let h = Host("pds".into());
        for s in [1, 2, 3, 4] {
            t.begin(&h, s);
        }
        assert_eq!(t.finish(&h, 2, Some(7)), None);
        assert_eq!(t.finish(&h, 4, None), None);
        assert_eq!(t.snapshot().min_ordinal_above_ack, Some(7));
        assert_eq!(t.finish(&h, 1, Some(8)), Some(2));
        assert_eq!(t.finish(&h, 3, None), Some(4));
        let s = t.snapshot();
        assert_eq!((s.pending, s.min_ordinal_above_ack), (0, None));
    }

    #[test]
    fn a_replayed_seq_needs_both_copies() {
        let t = Tracker::default();
        let h = Host("pds".into());
        assert!(t.begin(&h, 5));
        assert!(!t.begin(&h, 5));
        assert_eq!(t.finish(&h, 5, None), None);
        assert_eq!(t.finish(&h, 5, None), Some(5));
    }
}
