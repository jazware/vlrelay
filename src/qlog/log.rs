//! One node's copy of the stream log, in memory: entries with their
//! `(epoch, seq)`, the commit index, and Raft's log matching rule.
//!
//! Seqs are dense from `base + 1`. Everything at or below `base` was
//! committed and trimmed (or never held: a node reset to a leader's base).
//! Nothing at or below `commit` is ever truncated; [`Log::try_append`] and
//! [`Log::truncate_after`] panic rather than do it, since that would take
//! back an entry a consumer may have seen.

use bytes::Bytes;
use std::collections::VecDeque;

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Entry {
    pub epoch: u64,
    pub seq: u64,
    pub data: Bytes,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Mismatch;

#[derive(Debug, Default)]
pub struct Log {
    base_epoch: u64,
    base_seq: u64,
    entries: VecDeque<Entry>,
    commit: u64,
    bytes: usize,
}

impl Log {
    pub fn new() -> Log {
        Log::default()
    }

    pub fn base(&self) -> (u64, u64) {
        (self.base_epoch, self.base_seq)
    }

    pub fn last(&self) -> (u64, u64) {
        self.entries.back().map_or((self.base_epoch, self.base_seq), |e| (e.epoch, e.seq))
    }

    pub fn last_seq(&self) -> u64 {
        self.last().1
    }

    pub fn commit(&self) -> u64 {
        self.commit
    }

    pub fn bytes(&self) -> usize {
        self.bytes
    }

    pub fn len(&self) -> usize {
        self.entries.len()
    }

    pub fn is_empty(&self) -> bool {
        self.entries.is_empty()
    }

    /// The epoch of the entry at `seq`, if this log can tell (`base` counts).
    pub fn epoch_at(&self, seq: u64) -> Option<u64> {
        if seq == self.base_seq {
            return Some(self.base_epoch);
        }
        if seq < self.base_seq {
            return None;
        }
        self.entries.get((seq - self.base_seq - 1) as usize).map(|e| e.epoch)
    }

    pub fn get(&self, seq: u64) -> Option<&Entry> {
        if seq <= self.base_seq {
            return None;
        }
        self.entries.get((seq - self.base_seq - 1) as usize)
    }

    /// The leader's append: the next seq, under `epoch`.
    pub fn append(&mut self, epoch: u64, data: Bytes) -> u64 {
        let seq = self.last_seq() + 1;
        self.bytes += data.len();
        self.entries.push_back(Entry { epoch, seq, data });
        seq
    }

    /// A follower's append: `entries` follow `(prev_epoch, prev_seq)` in the
    /// leader's log. Refused unless this log holds that entry with that
    /// epoch (anything at or below the commit index matches: committed
    /// entries are the same everywhere). An entry that differs from the one
    /// held at its seq truncates the log there; one that matches is kept, so
    /// a stale, shorter append never drops later entries. Returns the last
    /// seq known to match the leader's log.
    pub fn try_append(&mut self, prev_epoch: u64, prev_seq: u64, entries: Vec<Entry>) -> Result<u64, Mismatch> {
        if prev_seq > self.last_seq() {
            return Err(Mismatch);
        }
        if prev_seq > self.commit.max(self.base_seq) && self.epoch_at(prev_seq) != Some(prev_epoch) {
            return Err(Mismatch);
        }
        let matched = prev_seq + entries.len() as u64;
        for (i, e) in entries.into_iter().enumerate() {
            debug_assert_eq!(e.seq, prev_seq + 1 + i as u64, "entries are consecutive");
            if e.seq <= self.base_seq || e.seq <= self.commit {
                continue;
            }
            match self.epoch_at(e.seq) {
                Some(ep) if ep == e.epoch => continue,
                Some(_) => self.truncate_after(e.seq - 1),
                None => {}
            }
            self.bytes += e.data.len();
            self.entries.push_back(e);
        }
        Ok(matched)
    }

    /// Drops every entry above `seq`. Only uncommitted entries can go.
    pub fn truncate_after(&mut self, seq: u64) {
        if seq >= self.last_seq() {
            return;
        }
        assert!(
            seq >= self.commit,
            "qlog: truncating committed entries (after {seq}, commit {}): a committed entry would be lost",
            self.commit
        );
        let keep = seq.saturating_sub(self.base_seq) as usize;
        while self.entries.len() > keep {
            let e = self.entries.pop_back().expect("len checked");
            self.bytes -= e.data.len();
        }
    }

    /// Raises the commit index to `c`, capped at the last entry held.
    pub fn set_commit(&mut self, c: u64) -> bool {
        let c = c.min(self.last_seq());
        if c > self.commit {
            self.commit = c;
            true
        } else {
            false
        }
    }

    /// Starts the log over at `(epoch, seq)`, everything at or below it
    /// committed: a follower behind the leader's base, or one whose log
    /// can't be matched any other way. Only ever moves forward.
    pub fn reset(&mut self, epoch: u64, seq: u64) {
        assert!(seq >= self.commit, "qlog: reset to {seq} below the commit index {}", self.commit);
        self.entries.clear();
        self.bytes = 0;
        self.base_epoch = epoch;
        self.base_seq = seq;
        self.commit = seq;
    }

    /// Re-tags every entry above `seq` with `epoch`: a new leader takes the
    /// adopted tail as its own term's entries, so it commits them by
    /// counting acks like any other (Raft's rule against committing an
    /// older term's entries by count). Seqs and data are unchanged.
    pub fn restamp_after(&mut self, seq: u64, epoch: u64) {
        assert!(seq >= self.commit, "qlog: restamping committed entries");
        let from = seq.saturating_sub(self.base_seq) as usize;
        for e in self.entries.iter_mut().skip(from) {
            e.epoch = epoch;
        }
    }

    /// Up to `max_bytes` of entries from `from` on (at least one if any).
    pub fn entries_from(&self, from: u64, max_bytes: usize) -> Vec<Entry> {
        let from = from.max(self.base_seq + 1);
        let mut out = Vec::new();
        let mut n = 0;
        let start = (from - self.base_seq - 1) as usize;
        for e in self.entries.iter().skip(start) {
            if !out.is_empty() && n + e.data.len() > max_bytes {
                break;
            }
            n += e.data.len();
            out.push(e.clone());
        }
        out
    }

    /// Entries in `(after, upto]`, all of them.
    pub fn range(&self, after: u64, upto: u64) -> impl Iterator<Item = &Entry> {
        let after = after.max(self.base_seq);
        let start = (after - self.base_seq) as usize;
        let n = upto.saturating_sub(after) as usize;
        self.entries.iter().skip(start).take(n)
    }

    /// Drops the oldest entries while the log holds more than `keep_bytes`,
    /// never past `upto` (at most the commit index; the caller passes what
    /// has been emitted too).
    pub fn trim(&mut self, keep_bytes: usize, upto: u64) {
        let upto = upto.min(self.commit);
        while self.bytes > keep_bytes {
            match self.entries.front() {
                Some(e) if e.seq <= upto => {
                    let e = self.entries.pop_front().expect("front checked");
                    self.bytes -= e.data.len();
                    self.base_epoch = e.epoch;
                    self.base_seq = e.seq;
                }
                _ => break,
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use rand::{Rng, SeedableRng};

    fn e(epoch: u64, seq: u64) -> Entry {
        Entry { epoch, seq, data: Bytes::from(format!("{epoch}/{seq}")) }
    }

    #[test]
    fn matching_and_truncation() {
        let mut l = Log::new();
        assert_eq!(l.try_append(0, 0, vec![e(1, 1), e(1, 2), e(1, 3)]), Ok(3));
        // a gap is refused
        assert_eq!(l.try_append(1, 5, vec![e(1, 6)]), Err(Mismatch));
        // a different epoch at prev is refused
        assert_eq!(l.try_append(2, 3, vec![e(2, 4)]), Err(Mismatch));
        // a stale, shorter append keeps what follows
        assert_eq!(l.try_append(1, 1, vec![e(1, 2)]), Ok(2));
        assert_eq!(l.last(), (1, 3));
        // a conflicting entry truncates from there
        assert_eq!(l.try_append(1, 2, vec![e(2, 3), e(2, 4)]), Ok(4));
        assert_eq!(l.last(), (2, 4));
        assert_eq!(l.get(3).unwrap().epoch, 2);
        l.set_commit(4);
        // below the commit index, prev always matches
        assert_eq!(l.try_append(9, 2, vec![e(2, 3), e(2, 4), e(2, 5)]), Ok(5));
        assert_eq!(l.last(), (2, 5));
    }

    #[test]
    #[should_panic(expected = "truncating committed entries")]
    fn committed_entries_are_never_truncated() {
        let mut l = Log::new();
        l.try_append(0, 0, vec![e(1, 1), e(1, 2)]).unwrap();
        l.set_commit(2);
        l.truncate_after(1);
    }

    #[test]
    fn trim_and_reset() {
        let mut l = Log::new();
        for _ in 0..10 {
            l.append(1, Bytes::from_static(b"0123456789"));
        }
        l.set_commit(6);
        l.trim(30, 4);
        assert_eq!(l.base(), (1, 4));
        assert_eq!(l.entries_from(1, 1 << 20).first().unwrap().seq, 5);
        // a prev below the base is committed, so it matches
        assert_eq!(l.try_append(1, 2, vec![e(1, 3), e(1, 4), e(1, 5)]), Ok(5));
        assert_eq!(l.range(4, 7).map(|e| e.seq).collect::<Vec<_>>(), vec![5, 6, 7]);
        l.reset(3, 20);
        assert_eq!((l.base(), l.last(), l.commit()), ((3, 20), (3, 20), 20));
        assert_eq!(l.try_append(3, 20, vec![e(3, 21)]), Ok(21));
    }

    /// Random leaders over a shared model: whatever sequence of appends,
    /// stale appends, truncations and commits a follower sees, its log is
    /// always a prefix-consistent copy of the leader's at every matched
    /// point, and the commit index never moves back.
    #[test]
    fn follower_tracks_leader_under_reordering() {
        for seed in 0..200u64 {
            let mut rng = rand::rngs::StdRng::seed_from_u64(seed);
            let mut leader = Log::new();
            let mut f = Log::new();
            let mut epoch = 1;
            let mut sent: Vec<(u64, u64, Vec<Entry>)> = Vec::new();
            for _ in 0..300 {
                match rng.gen_range(0..10) {
                    0..=4 => {
                        leader.append(epoch, Bytes::from(vec![epoch as u8]));
                    }
                    5..=7 => {
                        let from = rng.gen_range(f.commit() + 1..=leader.last_seq() + 1);
                        let prev = from - 1;
                        let pe = leader.epoch_at(prev).unwrap_or(0);
                        let ents = leader.entries_from(from, rng.gen_range(1..64));
                        sent.push((pe, prev, ents));
                    }
                    8 => {
                        // a new leader takes over and drops the uncommitted tail
                        epoch += 1;
                        let keep = rng.gen_range(f.commit().max(leader.commit())..=leader.last_seq());
                        leader.truncate_after(keep);
                        let c = leader.commit();
                        leader.restamp_after(c, epoch);
                    }
                    _ => {
                        // deliver a random earlier message (stale or reordered)
                        if !sent.is_empty() {
                            let i = rng.gen_range(0..sent.len());
                            let (pe, ps, ents) = sent.swap_remove(i);
                            // a message from an older term is refused by epoch upstream
                            if ents.iter().all(|x| leader.epoch_at(x.seq) == Some(x.epoch)) {
                                let before = f.commit();
                                if let Ok(m) = f.try_append(pe, ps, ents) {
                                    for s in f.commit() + 1..=m {
                                        assert_eq!(f.get(s), leader.get(s), "seed {seed}: matched entry {s} differs");
                                    }
                                    let c = m.min(leader.last_seq()).min(f.last_seq());
                                    // the leader commits what it holds; the follower learns it
                                    leader.set_commit(c);
                                    f.set_commit(c);
                                }
                                assert!(f.commit() >= before);
                            }
                        }
                    }
                }
            }
            for s in 1..=f.commit() {
                assert_eq!(f.get(s), leader.get(s), "seed {seed}: committed entry {s} differs");
            }
        }
    }
}
