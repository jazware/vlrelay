use super::synth::{Curve, Op, Repo, Signer};
use super::*;
use crate::event::{Event, Limits, ParsedCommit, ParsedSync, parse};

fn commit_of(frame: Bytes) -> ParsedCommit {
    match parse(frame, &Limits::default()).expect("parse") {
        Event::Commit(c) => c,
        e => panic!("not a commit: {e:?}"),
    }
}

fn sync_of(frame: Bytes) -> ParsedSync {
    match parse(frame, &Limits::default()).expect("parse") {
        Event::Sync(s) => s,
        e => panic!("not a sync: {e:?}"),
    }
}

/// Replaces the commit block (and the event's pointers to it).
fn set_commit_block(c: &mut ParsedCommit, block: Vec<u8>) {
    let old = c.commit;
    let new = Cid::dag_cbor(&block);
    for (cid, b) in c.blocks.iter_mut() {
        if *cid == old {
            *cid = new;
            *b = Bytes::from(block.clone());
        }
    }
    c.commit = new;
    c.car_roots[0] = new;
}

/// Where the 64 signature bytes start in a signed commit block.
pub(crate) fn sig_offset(block: &[u8]) -> usize {
    let (_, sig) = crate::event::split_signed_commit(block).unwrap();
    sig.as_ptr() as usize - block.as_ptr() as usize
}

fn commit_block(c: &ParsedCommit) -> Vec<u8> {
    c.blocks.iter().find(|(cid, _)| *cid == c.commit).unwrap().1.to_vec()
}

#[test]
fn synthetic_histories_verify_and_chain() {
    for curve in [Curve::K256, Curve::P256] {
        for initial in [0usize, 1, 10, 300] {
            let mut r = Repo::new("did:plc:synthsynthsynthsynth", Signer::new(curve, initial as u64), initial);
            let key = r.signer.public();
            let mut state: Option<ChainState> = None;
            for round in 0..40 {
                let n = [1, 1, 2, 5, 13, 50][round % 6];
                let ops = r.mixed_ops(n);
                let c = commit_of(r.commit(&ops));
                let v = verify_commit(&c, &key)
                    .unwrap_or_else(|e| panic!("{curve:?} initial={initial} round={round}: {e}"));
                assert_eq!(v.prev_data, c.prev_data);
                // synth commits carry no `since`: only an empty repo's first is a creation
                assert_eq!(v.created, initial == 0 && round == 0, "initial={initial} round={round}");
                state = Some(check_chain(state.as_ref(), &v).expect("chain"));
                assert_eq!(check_chain(state.as_ref(), &v), Err(ChainError::Duplicate));
            }
            let s = sync_of(r.sync());
            let v = verify_sync(&s, &key).expect("sync");
            assert_eq!(v.data, state.unwrap().data);
        }
    }
}

#[test]
fn deletes_and_updates_only() {
    let mut r = Repo::new("did:plc:deleteupdatedelete", Signer::new(Curve::K256, 7), 200);
    let key = r.signer.public();
    let keys: Vec<String> = r.live.keys().take(60).cloned().collect();
    for chunk in keys.chunks(6) {
        let ops: Vec<Op> = chunk
            .iter()
            .enumerate()
            .map(|(i, k)| if i % 2 == 0 { Op::Delete(k.clone()) } else { Op::Put(k.clone()) })
            .collect();
        let c = commit_of(r.commit(&ops));
        verify_commit(&c, &key).expect("verify");
    }
    // emptying the repo entirely
    let all: Vec<Op> = r.live.keys().cloned().map(Op::Delete).collect();
    for chunk in all.chunks(150) {
        let ops: Vec<Op> = chunk
            .iter()
            .map(|o| match o {
                Op::Delete(k) => Op::Delete(k.clone()),
                _ => unreachable!(),
            })
            .collect();
        verify_commit(&commit_of(r.commit(&ops)), &key).expect("verify");
    }
    // and an empty commit
    verify_commit(&commit_of(r.commit(&[])), &key).expect("empty commit");
}

fn sample(curve: Curve) -> (ParsedCommit, SigningKey) {
    let mut r = Repo::new("did:plc:mutantmutantmutant", Signer::new(curve, 99), 500);
    let ops = r.mixed_ops(8);
    (commit_of(r.commit(&ops)), r.signer.public())
}

#[test]
fn flipped_signature_bytes_reject() {
    for curve in [Curve::K256, Curve::P256] {
        let (c, key) = sample(curve);
        let block = commit_block(&c);
        let (_, sig) = crate::event::split_signed_commit(&block).unwrap();
        let sig_at = sig_offset(&block);
        assert_eq!(&block[sig_at..sig_at + 64], sig);
        for i in 0..64 {
            let mut m = c.clone();
            let mut b = block.clone();
            b[sig_at + i] ^= 1 << (i % 8);
            set_commit_block(&mut m, b);
            assert_eq!(verify_commit(&m, &key), Err(Reject::BadSignature), "{curve:?} byte {i}");
        }
        // the signed content, too
        let mut m = c.clone();
        let mut b = block.clone();
        let at = b.windows(3).position(|w| w == b"rev").unwrap() + 4;
        b[at + 12] = if b[at + 12] == b'2' { b'3' } else { b'2' };
        set_commit_block(&mut m, b);
        assert!(matches!(verify_commit(&m, &key), Err(Reject::CommitRevMismatch | Reject::BadSignature)));
    }
}

#[test]
fn wrong_key_rejects() {
    let (c, _) = sample(Curve::K256);
    for other in [Signer::new(Curve::K256, 1).public(), Signer::new(Curve::P256, 1).public()] {
        assert_eq!(verify_commit(&c, &other), Err(Reject::BadSignature));
    }
}

#[test]
fn high_s_rejects() {
    for curve in [Curve::K256, Curve::P256] {
        let (c, key) = sample(curve);
        let mut block = commit_block(&c);
        let at = sig_offset(&block) + 32;
        // s -> n - s: still a valid signature, but high-S
        let n: [u8; 32] = match curve {
            Curve::K256 => hex32("fffffffffffffffffffffffffffffffebaaedce6af48a03bbfd25e8cd0364141"),
            Curve::P256 => hex32("ffffffff00000000ffffffffffffffffbce6faada7179e84f3b9cac2fc632551"),
        };
        let s: [u8; 32] = block[at..at + 32].try_into().unwrap();
        block[at..at + 32].copy_from_slice(&sub_be(&n, &s));
        let mut m = c.clone();
        set_commit_block(&mut m, block);
        assert_eq!(verify_commit(&m, &key), Err(Reject::BadSignature), "{curve:?}");
    }
}

fn hex32(s: &str) -> [u8; 32] {
    let mut o = [0u8; 32];
    for i in 0..32 {
        o[i] = u8::from_str_radix(&s[2 * i..2 * i + 2], 16).unwrap();
    }
    o
}

fn sub_be(a: &[u8; 32], b: &[u8; 32]) -> [u8; 32] {
    let mut o = [0u8; 32];
    let mut borrow = 0i16;
    for i in (0..32).rev() {
        let d = a[i] as i16 - b[i] as i16 - borrow;
        o[i] = d.rem_euclid(256) as u8;
        borrow = (d < 0) as i16;
    }
    o
}

#[test]
fn every_dropped_block_rejects() {
    for curve in [Curve::K256, Curve::P256] {
        let (c, key) = sample(curve);
        assert!(verify_commit(&c, &key).is_ok());
        let records: Vec<Cid> = c.ops.iter().filter_map(|o| o.cid).collect();
        for i in 0..c.blocks.len() {
            let mut m = c.clone();
            let (cid, _) = m.blocks.remove(i);
            let want = if cid == c.commit {
                Reject::MissingCommitBlock
            } else if records.contains(&cid) {
                Reject::MissingRecordBlock
            } else {
                Reject::MissingMstBlocks
            };
            assert_eq!(verify_commit(&m, &key), Err(want), "{curve:?} dropping block {i} ({cid})");
        }
    }
}

#[test]
fn proof_mutations_reject() {
    let (c, key) = sample(Curve::K256);
    let other = Cid::dag_cbor(b"something else");

    let mut m = c.clone();
    m.prev_data = Some(other);
    assert_eq!(verify_commit(&m, &key), Err(Reject::InversionMismatch));

    let i = c.ops.iter().position(|o| o.action == Action::Create).unwrap();
    let mut m = c.clone();
    m.ops[i].cid = Some(other);
    assert_eq!(
        verify_commit_with(&m, &key, &Options { require_record_blocks: false, ..Options::default() }),
        Err(Reject::OpMismatch)
    );

    let mut m = c.clone();
    m.ops.remove(i);
    assert_eq!(verify_commit(&m, &key), Err(Reject::InversionMismatch), "an op left out");

    let mut m = c.clone();
    let dup = m.ops[i].clone();
    m.ops.push(dup);
    assert_eq!(verify_commit(&m, &key), Err(Reject::DuplicatePath));

    if let Some(j) = c.ops.iter().position(|o| o.action != Action::Create) {
        let mut m = c.clone();
        m.ops[j].prev = None;
        assert_eq!(verify_commit(&m, &key), Err(Reject::MissingOpPrev));
        let mut m = c.clone();
        m.ops[j].prev = Some(other);
        assert_eq!(verify_commit(&m, &key), Err(Reject::InversionMismatch));
    } else {
        panic!("sample has no update/delete");
    }

    // a create relabelled as an update of a key that didn't exist
    let mut m = c.clone();
    m.ops[i].action = Action::Update;
    m.ops[i].prev = Some(other);
    assert_eq!(verify_commit(&m, &key), Err(Reject::InversionMismatch));

    let mut m = c.clone();
    let (_, b) = &mut m.blocks[1];
    let mut v = b.to_vec();
    *v.last_mut().unwrap() ^= 1;
    *b = Bytes::from(v);
    assert_eq!(verify_commit(&m, &key), Err(Reject::BlockHashMismatch));

    let mut m = c.clone();
    m.car_roots[0] = other;
    assert_eq!(verify_commit(&m, &key), Err(Reject::CarRootMismatch));

    let mut m = c.clone();
    m.repo = "did:plc:someoneelseentirely".into();
    assert_eq!(verify_commit(&m, &key), Err(Reject::CommitDidMismatch));

    let mut m = c.clone();
    m.rev = Tid(m.rev.0 + 1);
    assert_eq!(verify_commit(&m, &key), Err(Reject::CommitRevMismatch));

    let mut m = c.clone();
    m.rev = Tid::from_parts(vlpds::tid::now_micros() + 3_600_000_000, 0);
    assert_eq!(verify_commit(&m, &key), Err(Reject::FutureRev));

    let mut m = c.clone();
    m.prev_data = None;
    assert!(verify_commit(&m, &key).is_ok(), "sync 1.0 shape passes by default");
    assert_eq!(
        verify_commit_with(&m, &key, &Options { require_prev_data: true, ..Options::default() }),
        Err(Reject::MissingPrevData)
    );
}

#[test]
fn sync_mutations_reject() {
    let mut r = Repo::new("did:plc:syncsyncsyncsync", Signer::new(Curve::P256, 3), 20);
    let key = r.signer.public();
    let s = sync_of(r.sync());
    verify_sync(&s, &key).unwrap();

    let mut m = s.clone();
    let mut b = m.blocks[0].1.to_vec();
    let at = sig_offset(&b);
    b[at + 63] ^= 1;
    let c = Cid::dag_cbor(&b);
    m.blocks[0] = (c, Bytes::from(b));
    m.car_roots[0] = c;
    assert_eq!(verify_sync(&m, &key), Err(Reject::BadSignature));

    let mut m = s.clone();
    m.blocks.clear();
    assert_eq!(verify_sync(&m, &key), Err(Reject::MissingCommitBlock));

    let mut m = s.clone();
    m.did = "did:plc:notthesameaccount".into();
    assert_eq!(verify_sync(&m, &key), Err(Reject::CommitDidMismatch));

    assert_eq!(verify_sync(&s, &Signer::new(Curve::P256, 4).public()), Err(Reject::BadSignature));
}

#[test]
fn chain_rules() {
    let c1 = Cid::dag_cbor(b"c1");
    let d1 = Cid::dag_cbor(b"d1");
    let c2 = Cid::dag_cbor(b"c2");
    let d2 = Cid::dag_cbor(b"d2");
    let st = ChainState { rev: Tid(100), data: d1, commit: c1 };
    let v = |kind, rev, commit, data, prev_data| Verified {
        kind,
        did: "did:plc:x".into(),
        rev: Tid(rev),
        commit,
        data,
        prev_data,
        created: false,
    };
    use VerifiedKind::*;

    assert_eq!(
        check_chain(None, &v(Commit, 5, c2, d2, Some(d1))),
        Ok(ChainState { rev: Tid(5), data: d2, commit: c2 })
    );
    assert_eq!(
        check_chain(Some(&st), &v(Commit, 101, c2, d2, Some(d1))),
        Ok(ChainState { rev: Tid(101), data: d2, commit: c2 })
    );
    assert_eq!(check_chain(Some(&st), &v(Commit, 99, c2, d2, Some(d1))), Err(ChainError::RevNotForward));
    assert_eq!(check_chain(Some(&st), &v(Commit, 100, c2, d2, Some(d1))), Err(ChainError::RevNotForward));
    assert_eq!(check_chain(Some(&st), &v(Commit, 100, c1, d1, Some(d1))), Err(ChainError::Duplicate));
    assert_eq!(
        check_chain(Some(&st), &v(Commit, 101, c2, d2, Some(d2))),
        Err(ChainError::PrevDataMismatch { expected: d1, got: d2 })
    );
    assert!(check_chain(Some(&st), &v(Commit, 101, c2, d2, None)).is_ok(), "sync 1.0 commit");
    // #sync resets regardless of data, but not backwards
    assert_eq!(
        check_chain(Some(&st), &v(Sync, 101, c2, d2, None)),
        Ok(ChainState { rev: Tid(101), data: d2, commit: c2 })
    );
    assert_eq!(check_chain(Some(&st), &v(Sync, 50, c2, d2, None)), Err(ChainError::RevNotForward));

    assert_eq!(ChainState::from_bytes(&st.to_bytes()), Some(st));
}

#[test]
fn rev_going_backwards_on_real_chain() {
    let mut r = Repo::new("did:plc:backwardsbackwards", Signer::new(Curve::K256, 11), 30);
    let key = r.signer.public();
    let ops = r.mixed_ops(3);
    let a = verify_commit(&commit_of(r.commit(&ops)), &key).unwrap();
    let ops = r.mixed_ops(3);
    let b = verify_commit(&commit_of(r.commit(&ops)), &key).unwrap();
    let after_b = check_chain(Some(&check_chain(None, &a).unwrap()), &b).unwrap();
    assert_eq!(check_chain(Some(&after_b), &a), Err(ChainError::RevNotForward));
}

#[test]
fn multikeys() {
    assert!(matches!(
        SigningKey::from_did_key("did:key:zQ3shQKrVD2bzxqAqhawWyK3LkKGbWAi3E1fAfQnJ35V1ZWke"),
        Ok(SigningKey::K256(_))
    ));
    assert!(matches!(
        SigningKey::from_did_key("did:key:zDnaembgSGUhZULN2Caob4HLJPaxBh92N7rtH21TErzqf8HQo"),
        Ok(SigningKey::P256(_))
    ));
    for bad in ["", "z", "zQ3sh", "did:key:abc", "mABC", "z6MkhaXgBZDvotDkL5257faiztiGiC2QtKLGpbnnEGta2doK"] {
        assert!(SigningKey::from_multibase(bad).is_err() && SigningKey::from_did_key(bad).is_err(), "{bad}");
    }
}

// real bsky.network #commits (sync 1.0 era: no prevData)

fn fixture_frames() -> Vec<(String, Bytes)> {
    let dir = concat!(env!("CARGO_MANIFEST_DIR"), "/../vlpds/testdata/shrike/firehose_commits");
    let mut out = Vec::new();
    for e in std::fs::read_dir(dir).expect("fixtures") {
        let p = e.unwrap().path();
        let j: serde_json::Value = serde_json::from_slice(&std::fs::read(&p).unwrap()).unwrap();
        out.push((p.display().to_string(), frame_from_json("#commit", &j)));
    }
    assert!(out.len() >= 4);
    out
}

pub(crate) fn frame_from_json(t: &str, body: &serde_json::Value) -> Bytes {
    use vlpds::cbor::Value;
    let header = Value::Map(vec![("t".into(), Value::Text(t.into())), ("op".into(), Value::Int(1))]);
    // Value::from_json sorts keys canonically; null blobs are an older PDS's
    let mut body = body.clone();
    if body["blobs"].is_null() {
        body["blobs"] = serde_json::json!([]);
    }
    let mut out = header.to_cbor();
    Value::from_json(&body).expect("lex json").encode(&mut out);
    Bytes::from(out)
}

pub(crate) fn fixture_keys() -> HashMap<String, SigningKey> {
    let p = concat!(env!("CARGO_MANIFEST_DIR"), "/testdata/firehose_commit_keys.json");
    let j: serde_json::Value = serde_json::from_slice(&std::fs::read(p).unwrap()).unwrap();
    j.as_object()
        .unwrap()
        .iter()
        .filter(|(k, _)| k.starts_with("did:"))
        .map(|(k, v)| (k.clone(), SigningKey::from_did_key(v.as_str().unwrap()).unwrap()))
        .collect()
}

#[test]
fn real_firehose_commits_verify() {
    let keys = fixture_keys();
    for (name, frame) in fixture_frames() {
        let c = commit_of(frame);
        let v = verify_commit(&c, &keys[&c.repo]).unwrap_or_else(|e| panic!("{name}: {e}"));
        assert_eq!(v.prev_data, None);
        // and each mutation of it fails
        let mut m = c.clone();
        let mut b = commit_block(&c);
        let at = sig_offset(&b);
        b[at + 5] ^= 0x10;
        set_commit_block(&mut m, b);
        assert_eq!(verify_commit(&m, &keys[&c.repo]), Err(Reject::BadSignature), "{name}");
        // sync 1.0 CARs may carry blocks no check needs (the differential
        // test holds the rest to shrike's verdict); these must be there
        let data = verify_commit(&c, &keys[&c.repo]).unwrap().data;
        let needed: Vec<Cid> = c.ops.iter().filter_map(|o| o.cid).chain([c.commit, data]).collect();
        for cid in needed {
            let mut m = c.clone();
            m.blocks.retain(|(x, _)| *x != cid);
            assert!(verify_commit(&m, &keys[&c.repo]).is_err(), "{name}: dropped {cid}");
        }
    }
}

/// eurosky.social, Oct 2026: one commit deletes a post and re-creates it at
/// the same path, the create carrying the old record as `prev`. indigo
/// forwards it; we dropped it (`bad_op`), and then the account's next commit
/// as a chain break (`prev_data_mismatch`).
#[test]
fn delete_then_create_on_one_path() {
    let b = std::fs::read(concat!(env!("CARGO_MANIFEST_DIR"), "/testdata/regress/eurosky_delete_create.bin")).unwrap();
    let (n, rest) = b.split_at(4);
    let n = u32::from_le_bytes(n.try_into().unwrap()) as usize;
    let (first, rest) = rest.split_at(n);
    let next = &rest[4..];
    let key = SigningKey::from_multibase("zQ3shgYv5ut5siX3dhNU82fg225BqaPD2XyXVNKE3isgn76HG").unwrap();
    let c = commit_of(Bytes::copy_from_slice(first));
    assert_eq!(c.ops.len(), 2);
    assert_eq!(c.ops[0].path, c.ops[1].path);
    assert_eq!((c.ops[0].action, c.ops[1].action), (Action::Delete, Action::Create));
    assert!(c.ops[1].prev.is_some());
    let v = verify_commit(&c, &key).expect("delete + create verifies");
    let state = check_chain(None, &v).unwrap();
    let v2 = verify_commit(&commit_of(Bytes::copy_from_slice(next)), &key).expect("next commit");
    check_chain(Some(&state), &v2).expect("the chain holds");

    let other = Cid::dag_cbor(b"other");
    let mut m = c.clone();
    m.ops[0].prev = Some(other);
    assert_eq!(verify_commit(&m, &key), Err(Reject::InversionMismatch));
    let mut m = c.clone();
    m.ops.remove(0);
    assert!(verify_commit(&m, &key).is_ok(), "a create with prev acts as an update");
    m.ops[0].prev = None;
    assert_eq!(verify_commit(&m, &key), Err(Reject::InversionMismatch));
    // ops that contradict each other on one path
    let mut m = c.clone();
    m.ops.swap(0, 1);
    assert_eq!(verify_commit(&m, &key), Err(Reject::DuplicatePath));
    let mut m = c.clone();
    let dup = m.ops[1].clone();
    m.ops.push(dup);
    assert_eq!(verify_commit(&m, &key), Err(Reject::DuplicatePath));
}
