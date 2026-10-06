//! The state the leader applies committed entries to (docs/quorum.md §2),
//! sealed at exactly a flush point F: one SlateDB over the bucket at
//! `qlog/state`, WAL off, written only by the current leader (opening it
//! fences the previous one's writer).
//!
//! It stands in for the relay's DID state: per DID the seq and content of
//! its last event (`d/{did}`), per host the highest cursor carried in the
//! log (`c/{host}`), and the last seq applied (`_applied`). Each applied
//! batch is written with SlateDB seqnum = its last log seq, so a manifest's
//! `last_l0_seq` *is* the log seq its state reaches.
//!
//! Exactly F: SlateDB only promises a flush or checkpoint holds *at least*
//! the writes issued before it. The flush tracker targets the newest frozen
//! memtable when it runs, and the manifest writer folds every contiguous
//! uploaded memtable into one manifest, so writes after F can share F's
//! manifest. The applier is this state's only writer and it stops at F
//! until the checkpoint exists: nothing above F is in the database while
//! the checkpoint is taken. [`State::seal`] then reads the checkpoint's
//! manifest back and refuses one whose `last_l0_seq` isn't F.

use super::check::content_id;
use super::client::parse_test_frame;
use super::log::{Entry, decode_cursors};
use bytes::Bytes;
use serde::{Deserialize, Serialize};
use slatedb::config::{CheckpointOptions, CheckpointScope, WriteOptions};
use slatedb::{Db, DbReader, DbReaderMode, WriteBatch};
use std::collections::BTreeMap;
use std::time::Duration;
use vlpds::store::Store;

const APPLIED: &[u8] = b"_applied";

/// Where the state lives until a bucket recovery clones it elsewhere.
pub const DEFAULT_PATH: &str = "qlog/state";

fn default_path() -> String {
    DEFAULT_PATH.into()
}

/// The state a manifest names: a checkpoint of the SlateDB at `path`
/// (under the store's prefix) at `seq`.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct StateRef {
    #[serde(default = "default_path")]
    pub path: String,
    pub checkpoint: String,
    pub manifest_id: u64,
    pub seq: u64,
}

pub fn db_path(store: &Store, rel: &str) -> String {
    format!("{}/{rel}", store.prefix)
}

/// The path a bucket recovery by `epoch` clones the state to: one per
/// attempt, so a recovery cut short is resumed (SlateDB's clone is
/// idempotent for the same source) or left for the retention report.
pub fn recovery_path(epoch: u64) -> String {
    format!("qlog/state-e{epoch}")
}

fn settings(l0_bytes: usize) -> slatedb::Settings {
    slatedb::Settings {
        wal_enabled: false,
        l0_sst_size_bytes: l0_bytes,
        // above the L0 size: with the WAL off, a cap below it stalls writes
        max_unflushed_bytes: (l0_bytes * 4).max(256 << 20),
        // every seal uploads an L0, and an upload past this many waits for
        // the compactor (seen as 1-5 s seals at 2 s flushes with the
        // default 8); 32 is minutes of flushes at any interval used here
        l0_max_ssts: 32,
        l0_max_ssts_per_key: 32,
        // the default 1 s poll is most of the state's GETs (~4/s measured);
        // vlpds polls every 10 s too
        manifest_poll_interval: std::time::Duration::from_secs(10),
        ..Default::default()
    }
}

pub fn did_key(did: &str) -> Vec<u8> {
    [b"d/", did.as_bytes()].concat()
}

pub fn cursor_key(host: &str) -> Vec<u8> {
    [b"c/", host.as_bytes()].concat()
}

pub fn did_value(seq: u64, content: u64) -> [u8; 16] {
    let mut v = [0u8; 16];
    v[..8].copy_from_slice(&seq.to_be_bytes());
    v[8..].copy_from_slice(&content.to_be_bytes());
    v
}

/// What one entry does to the state (the verifier replays the same).
pub fn effects(e: &Entry) -> (Option<(String, [u8; 16])>, Vec<(String, u64)>) {
    let did = parse_test_frame(&e.data).map(|(_, did, _)| (did, did_value(e.seq, content_id(&e.data))));
    (did, decode_cursors(&e.cursors))
}

pub struct State {
    db: Db,
    rel: String,
    path: String,
    store: Store,
    applied: u64,
    cursors: BTreeMap<String, u64>,
}

impl State {
    /// Opens `qlog/state` as its writer, at whatever its last durable flush
    /// reached (at or past the last manifest's F: the leader seals before it
    /// writes a manifest, and applies only committed entries).
    pub async fn open(store: &Store, rel: &str) -> anyhow::Result<State> {
        // a flush interval's state changes fit one memtable at today's rates
        // (~100 B a DID), so most seals upload one L0
        State::open_with(store, rel, 64 << 20).await
    }

    /// With L0s cut at `l0_bytes` (tests: small, so memtables freeze and
    /// upload on their own between seals).
    pub async fn open_with(store: &Store, rel: &str, l0_bytes: usize) -> anyhow::Result<State> {
        let path = db_path(store, rel);
        let db = Db::builder(path.clone(), store.raw.clone()).with_settings(settings(l0_bytes)).build().await?;
        let applied = match db.get(APPLIED).await? {
            Some(v) => u64::from_be_bytes(v.as_ref().try_into()?),
            None => 0,
        };
        let mut cursors = BTreeMap::new();
        let mut it = db.scan_prefix(b"c/", ..).await?;
        while let Some(kv) = it.next().await? {
            let host = String::from_utf8_lossy(&kv.key[2..]).into_owned();
            cursors.insert(host, u64::from_be_bytes(kv.value.as_ref().try_into()?));
        }
        Ok(State { db, rel: rel.to_string(), path, store: store.clone(), applied, cursors })
    }

    /// The state at exactly `from` (a manifest's checkpoint), as a new
    /// database at `rel` that this process writes: SlateDB has no restore,
    /// and the source's latest state can be past the checkpoint (the old
    /// leader kept applying, and memtables flush on their own). A clone is
    /// a new manifest over the checkpoint's SSTs, O(1) in the state's size;
    /// rewriting every key changed past F in place would scan the whole
    /// state. The clone pins its source with a checkpoint of its own, so
    /// the source's SSTs stay until that's released. `None`: no flush ever
    /// sealed a state, so it starts empty.
    pub async fn recover(store: &Store, from: Option<&StateRef>, rel: &str) -> anyhow::Result<State> {
        if let Some(r) = from {
            anyhow::ensure!(r.path != rel, "qlog state: recovering {rel} onto itself");
            slatedb::admin::Admin::builder(db_path(store, rel), store.raw.clone())
                .build()
                .create_clone_builder_from_source(slatedb::admin::CloneSourceSpec::with_checkpoint(
                    db_path(store, &r.path),
                    r.checkpoint.parse()?,
                ))
                .build()
                .await?;
        }
        let st = State::open(store, rel).await?;
        let want = from.map_or(0, |r| r.seq);
        anyhow::ensure!(st.applied == want, "qlog state: the clone at {rel} is at {}, not {want}", st.applied);
        Ok(st)
    }

    /// The path under the store's prefix.
    pub fn rel(&self) -> &str {
        &self.rel
    }

    /// Moves the applied point to `seq` with nothing applied in between:
    /// the seqs skipped by a bucket recovery, which no entry will ever hold.
    pub async fn jump(&mut self, seq: u64) -> anyhow::Result<()> {
        anyhow::ensure!(seq >= self.applied, "qlog state: jumping back from {} to {seq}", self.applied);
        if seq == self.applied {
            return Ok(());
        }
        let mut b = WriteBatch::new();
        b.put(APPLIED, seq.to_be_bytes());
        self.db.write_with_options(b, &WriteOptions { seqnum: seq, ..Default::default() }).await?;
        self.applied = seq;
        Ok(())
    }

    pub fn applied(&self) -> u64 {
        self.applied
    }

    pub fn cursors(&self) -> &BTreeMap<String, u64> {
        &self.cursors
    }

    /// Applies `entries` (consecutive, from `applied + 1`) as one batch.
    pub async fn apply(&mut self, entries: &[Entry]) -> anyhow::Result<()> {
        let Some(last) = entries.last() else { return Ok(()) };
        anyhow::ensure!(
            entries[0].seq == self.applied + 1,
            "qlog state: applying {} after {}",
            entries[0].seq,
            self.applied
        );
        let mut b = WriteBatch::new();
        for e in entries {
            let (did, cursors) = effects(e);
            if let Some((did, v)) = did {
                b.put(did_key(&did), v);
            }
            for (h, c) in cursors {
                let cur = self.cursors.entry(h.clone()).or_default();
                if c > *cur {
                    *cur = c;
                    b.put(cursor_key(&h), c.to_be_bytes());
                }
            }
        }
        b.put(APPLIED, last.seq.to_be_bytes());
        self.db.write_with_options(b, &WriteOptions { seqnum: last.seq, ..Default::default() }).await?;
        self.applied = last.seq;
        Ok(())
    }

    /// A checkpoint of the state at exactly `applied` (see the module docs:
    /// the caller writes nothing until this returns).
    pub async fn seal(&self) -> anyhow::Result<StateRef> {
        let cp = self
            .db
            .create_checkpoint(
                CheckpointScope::All,
                &CheckpointOptions { lifetime: None, name: Some(format!("qlog-{}", self.applied)), source: None },
            )
            .await?;
        let admin = slatedb::admin::Admin::builder(self.path.clone(), self.store.raw.clone()).build();
        let m = admin
            .read_manifest(Some(cp.manifest_id))
            .await?
            .ok_or_else(|| anyhow::anyhow!("qlog state: checkpoint manifest {} missing", cp.manifest_id))?;
        if m.last_l0_seq() != self.applied {
            let _ = admin.delete_checkpoint(cp.id).await;
            anyhow::bail!(
                "qlog state: checkpoint {} holds seq {}, not the seal point {}",
                cp.id,
                m.last_l0_seq(),
                self.applied
            );
        }
        Ok(StateRef {
            path: self.rel.clone(),
            checkpoint: cp.id.to_string(),
            manifest_id: cp.manifest_id,
            seq: self.applied,
        })
    }

    pub async fn close(self) {
        let _ = self.db.close().await;
    }
}

/// Deletes every `qlog-*` checkpoint of the state at `rel` but `keep`'s.
pub async fn delete_checkpoints_except(store: &Store, rel: &str, keep: Option<&str>) -> anyhow::Result<usize> {
    let admin = slatedb::admin::Admin::builder(db_path(store, rel), store.raw.clone()).build();
    let mut n = 0;
    for c in admin.list_checkpoints(None).await? {
        if c.name.as_deref().is_some_and(|x| x.starts_with("qlog-")) && Some(c.id.to_string().as_str()) != keep {
            admin.delete_checkpoint(c.id).await?;
            n += 1;
        }
    }
    Ok(n)
}

/// Ids of the `qlog-*` checkpoints of the state at `rel`.
pub async fn list_checkpoints(store: &Store, rel: &str) -> anyhow::Result<Vec<String>> {
    let admin = slatedb::admin::Admin::builder(db_path(store, rel), store.raw.clone()).build();
    Ok(admin
        .list_checkpoints(None)
        .await?
        .into_iter()
        .filter(|c| c.name.as_deref().is_some_and(|x| x.starts_with("qlog-")))
        .map(|c| c.id.to_string())
        .collect())
}

pub async fn delete_checkpoint(store: &Store, r: &StateRef) -> anyhow::Result<()> {
    let admin = slatedb::admin::Admin::builder(db_path(store, &r.path), store.raw.clone()).build();
    let id = &r.checkpoint;
    admin.delete_checkpoint(id.parse()?).await?;
    Ok(())
}

/// The state a checkpoint holds: every key and value, plus the checkpoint
/// manifest's `last_l0_seq` (what a restart from it would hold).
pub async fn read_checkpoint(store: &Store, r: &StateRef) -> anyhow::Result<(BTreeMap<Bytes, Bytes>, u64)> {
    let admin = slatedb::admin::Admin::builder(db_path(store, &r.path), store.raw.clone()).build();
    let m = admin
        .read_manifest(Some(r.manifest_id))
        .await?
        .ok_or_else(|| anyhow::anyhow!("checkpoint manifest {} is gone", r.manifest_id))?;
    let reader = DbReader::builder(db_path(store, &r.path), store.raw.clone())
        .with_reader_mode(DbReaderMode::Checkpoint(r.checkpoint.parse()?))
        .build()
        .await?;
    let mut out = BTreeMap::new();
    let mut it = reader.scan::<std::ops::RangeFull>(..).await?;
    while let Some(kv) = it.next().await? {
        out.insert(kv.key, kv.value);
    }
    let _ = tokio::time::timeout(Duration::from_secs(5), reader.close()).await;
    Ok((out, m.last_l0_seq()))
}

pub fn applied_key() -> &'static [u8] {
    APPLIED
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::qlog::client::test_frame;
    use crate::qlog::log::encode_cursors;

    fn entry(seq: u64, pad: usize) -> Entry {
        let (p, s) = test_frame(&format!("did:q:h{}:{}", seq % 3, seq / 3 + 1), pad, 0);
        let mut e = Entry::new(1, seq, crate::qlog::wire::splice_seq(&p, &s, seq));
        if seq % 10 == 0 {
            e.cursors = encode_cursors(&[(format!("h{}", seq % 3), seq / 3)].into());
        }
        e
    }

    /// Memtables freeze and upload on their own (tiny L0s) before and after
    /// the seal, and writes go on right after it: the checkpoint still holds
    /// exactly the seal point, by its manifest and by its contents.
    #[tokio::test]
    async fn a_seal_holds_exactly_its_point_across_automatic_flushes() {
        let store = Store::memory(None);
        let mut st = State::open_with(&store, DEFAULT_PATH, 16 << 10).await.unwrap();
        let mut seq = 0;
        let mut refs = Vec::new();
        for round in 0..4 {
            for _ in 0..40 {
                let es: Vec<Entry> = (seq + 1..=seq + 25).map(|s| entry(s, 400)).collect();
                st.apply(&es).await.unwrap();
                seq += 25;
            }
            let r = st.seal().await.unwrap();
            assert_eq!(r.seq, seq, "round {round}");
            refs.push((r, st.cursors().clone()));
        }
        let m = slatedb::admin::Admin::builder(db_path(&store, DEFAULT_PATH), store.raw.clone())
            .build()
            .read_manifest(None)
            .await
            .unwrap()
            .unwrap();
        assert!(m.l0().len() + m.compacted().len() > 4, "no automatic L0 flushes happened");
        for (r, cursors) in &refs {
            let (kv, l0) = read_checkpoint(&store, r).await.unwrap();
            assert_eq!(l0, r.seq);
            assert_eq!(kv.get(APPLIED).map(|v| u64::from_be_bytes(v[..].try_into().unwrap())), Some(r.seq));
            let mut dids = 0;
            for (k, v) in &kv {
                if k.starts_with(b"d/") {
                    dids += 1;
                    let s = u64::from_be_bytes(v[..8].try_into().unwrap());
                    assert!(s <= r.seq, "seq {s} past the seal at {}", r.seq);
                } else if let Some(h) = k.strip_prefix(b"c/") {
                    let h = String::from_utf8_lossy(h).into_owned();
                    assert_eq!(cursors.get(&h).copied(), Some(u64::from_be_bytes(v[..].try_into().unwrap())));
                }
            }
            assert_eq!(dids, r.seq, "every seq names its own DID");
        }
        st.close().await;
        // a writer opened afterwards sees the last applied point and goes on
        let st = State::open(&store, DEFAULT_PATH).await.unwrap();
        assert!(st.applied() >= refs.last().unwrap().0.seq);
        st.close().await;
    }

    /// Recovery from a checkpoint while the source has moved on past it:
    /// the clone holds exactly the checkpoint (not the source's latest),
    /// takes writes of its own, jumps, and seals at the jump; the source's
    /// later writes never show up in it.
    #[tokio::test]
    async fn a_recovered_state_is_its_checkpoint_not_the_latest() {
        let store = Store::memory(None);
        let mut st = State::open_with(&store, DEFAULT_PATH, 16 << 10).await.unwrap();
        let es: Vec<Entry> = (1..=500).map(|s| entry(s, 300)).collect();
        st.apply(&es).await.unwrap();
        let r = st.seal().await.unwrap();
        let cursors_at = st.cursors().clone();
        // the old leader keeps applying, and its memtables reach the bucket
        let es: Vec<Entry> = (501..=900).map(|s| entry(s, 300)).collect();
        st.apply(&es).await.unwrap();
        st.close().await;
        let rel = recovery_path(7);
        let mut rec = State::recover(&store, Some(&r), &rel).await.unwrap();
        assert_eq!(rec.applied(), 500);
        assert_eq!(rec.cursors(), &cursors_at);
        let es: Vec<Entry> = (501..=520).map(|s| entry(s, 300)).collect();
        rec.apply(&es).await.unwrap();
        rec.jump(10_000).await.unwrap();
        let sealed = rec.seal().await.unwrap();
        assert_eq!((sealed.seq, sealed.path.as_str()), (10_000, rel.as_str()));
        let (kv, l0) = read_checkpoint(&store, &sealed).await.unwrap();
        assert_eq!(l0, 10_000);
        let dids = kv.keys().filter(|k| k.starts_with(b"d/")).count();
        assert_eq!(dids, 520, "the clone holds the source's state past its checkpoint");
        for (k, v) in &kv {
            if k.starts_with(b"d/") {
                assert!(u64::from_be_bytes(v[..8].try_into().unwrap()) <= 520);
            }
        }
        rec.close().await;
        // a clone that finished is opened again as it is
        let again = State::open(&store, &rel).await.unwrap();
        assert_eq!(again.applied(), 10_000);
        again.close().await;
    }
}
