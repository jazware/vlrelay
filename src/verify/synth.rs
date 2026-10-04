//! Synthetic, correctly signed sync 1.1 events from a real MST (vlpds's), for
//! tests and benches: k256 or P-256 keys, any mix of creates, updates and
//! deletes, with the proof blocks vlpds's PDS would ship.

use super::SigningKey;
use bytes::Bytes;
use std::collections::BTreeMap;
use vlpds::cbor::Value;
use vlpds::cid::Cid;
use vlpds::events::{CommitFrame, RepoOp, commit_frame, encode_commit, sync_frame};
use vlpds::mst::Tree;
use vlpds::tid::Tid;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Curve {
    K256,
    P256,
}

pub enum Signer {
    K256(vlpds::crypto::Keypair),
    P256(p256::ecdsa::SigningKey),
}

impl Signer {
    /// Deterministic per `seed`.
    pub fn new(curve: Curve, seed: u64) -> Signer {
        use sha2::{Digest, Sha256};
        let sk: [u8; 32] = Sha256::digest(seed.to_be_bytes()).into();
        match curve {
            Curve::K256 => Signer::K256(vlpds::crypto::Keypair::from_bytes(&sk).expect("k256 key")),
            Curve::P256 => Signer::P256(p256::ecdsa::SigningKey::from_bytes(&sk.into()).expect("p256 key")),
        }
    }

    pub fn sign(&self, msg: &[u8]) -> [u8; 64] {
        match self {
            Signer::K256(k) => k.sign(msg),
            Signer::P256(k) => {
                use p256::ecdsa::signature::Signer as _;
                let s: p256::ecdsa::Signature = k.sign(msg);
                s.normalize_s().unwrap_or(s).to_bytes().into()
            }
        }
    }

    pub fn multibase(&self) -> String {
        let mut b = match self {
            Signer::K256(_) => vec![0xe7, 0x01],
            Signer::P256(_) => vec![0x80, 0x24],
        };
        match self {
            Signer::K256(k) => b.extend_from_slice(&k.public_key_sec1()),
            Signer::P256(k) => b.extend_from_slice(k.verifying_key().to_encoded_point(true).as_bytes()),
        }
        format!("z{}", bs58::encode(b).into_string())
    }

    pub fn public(&self) -> SigningKey {
        SigningKey::from_multibase(&self.multibase()).expect("own key parses")
    }
}

pub fn record(path: &str, n: u64) -> Vec<u8> {
    let coll = path.split('/').next().unwrap_or("");
    Value::Map(vec![
        ("text".into(), Value::Text(format!("synthetic record {n} at {path}"))),
        ("$type".into(), Value::Text(coll.into())),
        ("createdAt".into(), Value::Text("2026-01-01T00:00:00.000Z".into())),
    ])
    .to_cbor()
}

/// One account's repo, emitting #commit and #sync frames.
pub struct Repo {
    pub did: String,
    pub signer: Signer,
    pub tree: Tree,
    pub live: BTreeMap<String, Cid>,
    pub rev: Tid,
    pub seq: i64,
    pub commit: Cid,
    commit_block: Vec<u8>,
    n: u64,
}

pub enum Op {
    Put(String),
    Delete(String),
}

impl Repo {
    /// `initial` records, written without proofs (the starting state).
    pub fn new(did: &str, signer: Signer, initial: usize) -> Repo {
        let mut tree = Tree::new();
        let mut live = BTreeMap::new();
        for i in 0..initial {
            let p = format!("app.bsky.feed.post/{}", Tid::from_parts(1_700_000_000_000_000 + i as u64 * 7919, 0));
            let c = Cid::dag_cbor(&record(&p, i as u64));
            tree.insert_no_proof(p.as_bytes(), c).expect("insert");
            live.insert(p, c);
        }
        tree.root_cid().expect("root");
        let rev = Tid::from_parts(vlpds::tid::now_micros() - 60_000_000, 0);
        let mut r = Repo {
            did: did.into(),
            signer,
            tree,
            live,
            rev,
            seq: 0,
            commit: Cid::raw(b""),
            commit_block: vec![],
            n: 0,
        };
        let data = r.tree.root_cid().expect("root");
        r.sign(data);
        r
    }

    fn sign(&mut self, data: Cid) {
        let rev = self.rev.to_string();
        let sig = self.signer.sign(&encode_commit(&self.did, &rev, &data, None));
        self.commit_block = encode_commit(&self.did, &rev, &data, Some(&sig));
        self.commit = Cid::dag_cbor(&self.commit_block);
    }

    pub fn commit_block(&self) -> &[u8] {
        &self.commit_block
    }

    pub fn new_path(&mut self) -> String {
        self.n += 1;
        format!("app.bsky.feed.like/{}", Tid::from_parts(1_750_000_000_000_000 + self.n * 104_729, 1))
    }

    /// A random-ish mix: mostly creates, some updates and deletes of live keys.
    pub fn mixed_ops(&mut self, n: usize) -> Vec<Op> {
        let mut ops = Vec::with_capacity(n);
        let keys: Vec<String> = self.live.keys().cloned().collect();
        for i in 0..n {
            self.n += 1;
            let pick = keys.get((self.n as usize * 2_654_435_761) % keys.len().max(1)).cloned();
            match (i % 5, pick) {
                (3, Some(k)) if !ops.iter().any(|o: &Op| matches!(o, Op::Put(p) | Op::Delete(p) if *p == k)) => {
                    ops.push(Op::Put(k))
                }
                (4, Some(k)) if !ops.iter().any(|o: &Op| matches!(o, Op::Put(p) | Op::Delete(p) if *p == k)) => {
                    ops.push(Op::Delete(k))
                }
                _ => ops.push(Op::Put(self.new_path())),
            }
        }
        ops
    }

    /// Applies `ops` and returns the #commit frame.
    pub fn commit(&mut self, ops: &[Op]) -> Bytes {
        let before = self.tree.root_cid().expect("root");
        let mut records: Vec<(Cid, Vec<u8>)> = Vec::new();
        let mut fops: Vec<(String, &'static str, Option<Cid>, Option<Cid>)> = Vec::new();
        for op in ops {
            match op {
                Op::Put(p) => {
                    self.n += 1;
                    let rec = record(p, self.n);
                    let c = Cid::dag_cbor(&rec);
                    let prev = self.tree.insert(p.as_bytes(), c).expect("insert");
                    self.live.insert(p.clone(), c);
                    records.push((c, rec));
                    fops.push((p.clone(), if prev.is_some() { "update" } else { "create" }, Some(c), prev));
                }
                Op::Delete(p) => {
                    let prev = self.tree.remove(p.as_bytes()).expect("remove");
                    self.live.remove(p);
                    fops.push((p.clone(), "delete", None, prev));
                }
            }
        }
        let mut nodes = Vec::new();
        let data = self.tree.write_diff_blocks(&mut nodes).expect("diff");
        self.rev = Tid::from_parts(self.rev.micros().max(vlpds::tid::now_micros() - 30_000_000) + 1, 0);
        self.sign(data);
        let mut car = Vec::new();
        vlpds::car::write_header(&mut car, &self.commit);
        vlpds::car::write_block(&mut car, &self.commit, &self.commit_block);
        for (c, b) in nodes.iter().chain(records.iter()) {
            vlpds::car::write_block(&mut car, c, b);
        }
        let rops: Vec<RepoOp> = fops
            .iter()
            .map(|(p, a, c, prev)| RepoOp {
                action: a,
                path: p,
                cid: *c,
                prev: if *a == "create" { None } else { *prev },
            })
            .collect();
        let rev = self.rev.to_string();
        let f = commit_frame(&CommitFrame {
            repo: &self.did,
            rev: &rev,
            since: None,
            commit: self.commit,
            prev_data: Some(before),
            blocks: &car,
            ops: &rops,
            time: "2026-01-01T00:00:00.000Z",
        });
        self.seq += 1;
        let mut out = Vec::with_capacity(f.len_hint());
        f.finish(self.seq, &mut out);
        Bytes::from(out)
    }

    /// A #sync of the current state.
    pub fn sync(&mut self) -> Bytes {
        let mut car = Vec::new();
        vlpds::car::write_header(&mut car, &self.commit);
        vlpds::car::write_block(&mut car, &self.commit, &self.commit_block);
        let f = sync_frame(&self.did, &self.rev.to_string(), &car, "2026-01-01T00:00:00.000Z");
        self.seq += 1;
        let mut out = Vec::new();
        f.finish(self.seq, &mut out);
        Bytes::from(out)
    }
}
