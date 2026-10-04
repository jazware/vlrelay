//! Per-host ack tracking. Lanes finish a host's events out of order, but its
//! cursor may only move past a seq once that seq and every earlier one is
//! done: durable in the log, rejected, or skipped.
//!
//! Each socket of a host is an epoch ([`Tracker::connected`]). A new socket
//! resumes from a cursor, so what the tracker held at or below it is done
//! and everything above it comes again on the new socket. From then on only
//! the new socket's copies count: an earlier socket's copy still in the
//! pipeline can't move the cursor, which matters after a sequence restart
//! (FutureCursor), where its seqs belong to another sequence.

use crate::types::FastMap;
use crate::types::Host;
use parking_lot::Mutex;
use std::collections::BTreeMap;
use std::time::Instant;

#[derive(Default)]
struct Entry {
    /// Copies still in the pipeline (a reconnect can replay a seq that's
    /// still in flight from the old socket).
    pending: u32,
    since: Option<Instant>,
    /// Lowest log ordinal a durable copy landed in.
    ordinal: Option<u64>,
    /// A copy was lost (the cluster couldn't reach the DID's owner, or the
    /// socket it came on was replaced): the cursor waits here until a
    /// replayed copy finishes.
    failed: bool,
}

#[derive(Default)]
struct HostAcks {
    seqs: BTreeMap<i64, Entry>,
    /// The newest socket's epoch: copies from older ones are ignored.
    epoch: u64,
}

#[derive(Default)]
pub struct Tracker {
    hosts: Mutex<FastMap<Host, HostAcks>>,
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
        self.begin_at(host, seq, 0, Instant::now())
    }

    /// [`Self::begin`] for a frame read on socket `epoch` at `at`.
    pub fn begin_at(&self, host: &Host, seq: i64, epoch: u64, at: Instant) -> bool {
        if seq <= 0 {
            return true;
        }
        let mut m = self.hosts.lock();
        if !m.contains_key(host) {
            m.insert(host.clone(), HostAcks::default());
        }
        let h = m.get_mut(host).expect("inserted above");
        let first = !h.seqs.contains_key(&seq);
        if epoch < h.epoch {
            return first;
        }
        let e = h.seqs.entry(seq).or_default();
        e.pending += 1;
        e.since.get_or_insert(at);
        first
    }

    /// `host` has a new socket, `epoch`, resuming after `cursor`. Seqs at or
    /// below it are done; those above it wait for their replayed copies.
    /// `restarted`: the host's sequence started over, so nothing held
    /// belongs to the new one.
    pub fn connected(&self, host: &Host, epoch: u64, cursor: Option<i64>, restarted: bool) {
        let mut m = self.hosts.lock();
        let h = m.entry(host.clone()).or_default();
        if epoch < h.epoch {
            return;
        }
        h.epoch = epoch;
        match cursor {
            Some(c) if !restarted => {
                h.seqs = h.seqs.split_off(&(c + 1));
                for e in h.seqs.values_mut() {
                    e.pending = 0;
                    e.failed = true;
                }
            }
            _ => h.seqs.clear(),
        }
    }

    /// The frame is done. Returns the host's new ack cursor, if it moved.
    pub fn finish(&self, host: &Host, seq: i64, epoch: u64, ordinal: Option<u64>) -> Option<i64> {
        if seq <= 0 {
            return None;
        }
        let mut m = self.hosts.lock();
        let h = m.get_mut(host)?;
        if epoch < h.epoch {
            return None;
        }
        let e = h.seqs.get_mut(&seq)?;
        e.pending = e.pending.saturating_sub(1);
        e.failed = false;
        if let Some(o) = ordinal {
            e.ordinal = Some(e.ordinal.map_or(o, |x| x.min(o)));
        }
        let mut acked = None;
        while let Some(first) = h.seqs.first_entry() {
            if first.get().pending > 0 || first.get().failed {
                break;
            }
            acked = Some(*first.key());
            first.remove();
        }
        acked
    }

    /// A copy that will never finish: the host's cursor stays below `seq`
    /// until the host replays it and that copy finishes.
    pub fn fail(&self, host: &Host, seq: i64, epoch: u64) {
        if seq <= 0 {
            return;
        }
        let mut m = self.hosts.lock();
        let Some(h) = m.get_mut(host) else { return };
        if epoch < h.epoch {
            return;
        }
        if let Some(e) = h.seqs.get_mut(&seq) {
            e.pending = e.pending.saturating_sub(1);
            e.failed = true;
        }
    }

    /// Frames of `host` still in the pipeline.
    pub fn pending_for(&self, host: &Host) -> usize {
        self.hosts.lock().get(host).map_or(0, |h| h.seqs.values().filter(|e| e.pending > 0).count())
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
        assert_eq!(t.finish(&h, 2, 0, Some(7)), None);
        assert_eq!(t.finish(&h, 4, 0, None), None);
        assert_eq!(t.snapshot().min_ordinal_above_ack, Some(7));
        assert_eq!(t.finish(&h, 1, 0, Some(8)), Some(2));
        assert_eq!(t.finish(&h, 3, 0, None), Some(4));
        let s = t.snapshot();
        assert_eq!((s.pending, s.min_ordinal_above_ack), (0, None));
    }

    #[test]
    fn a_replayed_seq_needs_both_copies() {
        let t = Tracker::default();
        let h = Host("pds".into());
        assert!(t.begin(&h, 5));
        assert!(!t.begin(&h, 5));
        assert_eq!(t.finish(&h, 5, 0, None), None);
        assert_eq!(t.finish(&h, 5, 0, None), Some(5));
    }

    /// A failed copy from an earlier socket at or below the new socket's
    /// cursor used to pin the cursor for good: nothing would replay it.
    #[test]
    fn a_new_socket_settles_the_old_ones_entries() {
        let t = Tracker::default();
        let h = Host("pds".into());
        let at = Instant::now();
        t.connected(&h, 1, Some(0), false);
        for s in 1..=6 {
            t.begin_at(&h, s, 1, at);
        }
        t.fail(&h, 2, 1);
        assert_eq!(t.finish(&h, 1, 1, None), Some(1));
        // a takeover elsewhere checkpointed 3; this node gets the host back
        t.connected(&h, 2, Some(3), false);
        assert_eq!(t.finish(&h, 4, 1, None), None, "the old socket's copies no longer count");
        assert_eq!(t.pending_for(&h), 0);
        assert!(!t.begin_at(&h, 4, 2, at), "4 was seen: a replay");
        assert_eq!(t.finish(&h, 4, 2, None), Some(4));
        t.begin_at(&h, 5, 2, at);
        t.begin_at(&h, 6, 2, at);
        assert_eq!(t.finish(&h, 6, 2, None), None);
        assert_eq!(t.finish(&h, 5, 2, None), Some(6));
    }

    /// After FutureCursor the old sequence's acks must not push the new
    /// sequence's cursor up.
    #[test]
    fn a_restarted_sequence_drops_the_old_one() {
        let t = Tracker::default();
        let h = Host("pds".into());
        let at = Instant::now();
        t.connected(&h, 1, Some(0), false);
        for s in 500..510 {
            t.begin_at(&h, s, 1, at);
        }
        t.connected(&h, 2, Some(0), true);
        assert_eq!(t.finish(&h, 505, 1, None), None);
        t.begin_at(&h, 1, 2, at);
        t.begin_at(&h, 600, 1, at);
        assert_eq!(t.finish(&h, 1, 2, None), Some(1));
        assert_eq!(t.snapshot().pending, 0);
    }
}
