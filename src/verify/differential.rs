//! Differential tests against shrike's sync 1.1 checks (the same oracle
//! vlpds's tests/all/differential_shrike.rs uses): on real and synthetic
//! commits, semantic mutations and random byte flips, both sides must accept
//! and reject the same frames.

use super::synth::{Curve, Op, Repo, Signer};
use super::tests::frame_from_json;
use super::*;
use crate::event::{Event, Limits, ParsedCommit, parse};
use rand::rngs::StdRng;
use rand::{Rng, SeedableRng};
use shrike::sync::RawSyncEvent;
use shrike::sync::invert::{
    check_op_cids, decode_commit_car, decode_sync_commit, find_duplicate_path, invert_decoded_commit,
};

fn shrike_verdict(frame: &[u8], did_key: &str) -> Result<(), String> {
    let key = shrike::crypto::parse_did_key(did_key).map_err(|e| e.to_string())?;
    match shrike::sync::raw::parse_raw_sync_frame(frame).map_err(|e| e.to_string())? {
        RawSyncEvent::Commit(raw) => {
            if raw.blocks.len() > 2_000_000 || raw.ops.len() > 200 {
                return Err("oversized".into());
            }
            if find_duplicate_path(&raw.ops).is_some() {
                return Err("duplicate path".into());
            }
            let d = decode_commit_car(&raw).map_err(|e| e.to_string())?;
            if d.inner.did != raw.repo || d.inner.rev != raw.rev || d.inner.version != 3 {
                return Err("field mismatch".into());
            }
            d.inner.verify(key.as_ref()).map_err(|e| e.to_string())?;
            check_op_cids(&raw, d.inner.data, &d.store).map_err(|e| e.to_string())?;
            if let Some(pd) = raw.prev_data {
                let inv = invert_decoded_commit(&raw, &d.inner, &d.store).map_err(|e| e.to_string())?;
                if inv != pd {
                    return Err("inversion mismatch".into());
                }
            }
            Ok(())
        }
        RawSyncEvent::Sync(raw) => {
            let c = decode_sync_commit(&raw.did, &raw.rev, &raw.blocks).map_err(|e| e.to_string())?;
            if c.did != raw.did || c.rev.to_string() != raw.rev || c.version != 3 {
                return Err("field mismatch".into());
            }
            c.verify(key.as_ref()).map_err(|e| e.to_string())
        }
        _ => Err("not a commit or sync".into()),
    }
}

/// Shrike doesn't require record blocks (it counts them), so neither do we here.
fn our_verdict(frame: &Bytes, did_key: &str) -> Result<(), String> {
    let key = SigningKey::from_did_key(did_key).map_err(|e| e.to_string())?;
    let opts = Options { require_record_blocks: false, ..Options::default() };
    match parse(frame.clone(), &Limits::default()).map_err(|e| e.reason().to_string())? {
        Event::Commit(c) => verify_commit_with(&c, &key, &opts).map(drop),
        Event::Sync(s) => verify_sync_with(&s, &key, &opts).map(drop),
        _ => Err(Reject::BadFrame),
    }
    .map_err(|e| e.reason().to_string())
}

fn reencode(c: &ParsedCommit) -> Bytes {
    use vlpds::events::{CommitFrame, RepoOp, commit_frame};
    let mut car = Vec::new();
    vlpds::car::write_header(&mut car, &c.car_roots[0]);
    for (cid, b) in &c.blocks {
        vlpds::car::write_block(&mut car, cid, b);
    }
    let ops: Vec<RepoOp> = c
        .ops
        .iter()
        .map(|o| RepoOp {
            action: match o.action {
                Action::Create => "create",
                Action::Update => "update",
                Action::Delete => "delete",
            },
            path: &o.path,
            cid: o.cid,
            prev: o.prev,
        })
        .collect();
    let rev = c.rev.to_string();
    let f = commit_frame(&CommitFrame {
        repo: &c.repo,
        rev: &rev,
        since: c.since.as_deref(),
        commit: c.commit,
        prev_data: c.prev_data,
        blocks: &car,
        ops: &ops,
        time: &c.time,
    });
    let mut out = Vec::new();
    f.finish(c.seq, &mut out);
    Bytes::from(out)
}

fn commit_of(f: &Bytes) -> ParsedCommit {
    match parse(f.clone(), &Limits::default()).unwrap() {
        Event::Commit(c) => c,
        _ => panic!("not a commit"),
    }
}

/// Semantic mutations of a valid commit, each re-encoded as a frame.
fn mutations(c: &ParsedCommit) -> Vec<(String, Bytes)> {
    let mut out = vec![("original".to_string(), reencode(c))];
    let other = Cid::dag_cbor(b"other");
    let mut push = |name: String, m: ParsedCommit| out.push((name, reencode(&m)));
    for i in 0..c.blocks.len() {
        let mut m = c.clone();
        m.blocks.remove(i);
        push(format!("drop block {i}"), m);
    }
    let mut m = c.clone();
    m.prev_data = Some(other);
    push("wrong prevData".into(), m);
    let mut m = c.clone();
    m.prev_data = None;
    push("no prevData".into(), m);
    for i in 0..c.ops.len() {
        let mut m = c.clone();
        m.ops.remove(i);
        push(format!("drop op {i}"), m);
        let mut m = c.clone();
        if m.ops[i].action != Action::Delete {
            m.ops[i].cid = Some(other);
            push(format!("op {i} cid"), m);
        }
        let mut m = c.clone();
        if m.ops[i].action != Action::Create {
            m.ops[i].prev = Some(other);
            push(format!("op {i} prev"), m.clone());
            m.ops[i].prev = None;
            push(format!("op {i} no prev"), m);
        }
    }
    if let Some(o) = c.ops.first() {
        let mut m = c.clone();
        m.ops.push(o.clone());
        push("duplicate op".into(), m);
    }
    let mut m = c.clone();
    m.repo = "did:plc:aaaaaaaaaaaaaaaaaaaaaaaa".into();
    push("other did".into(), m);
    let mut m = c.clone();
    m.rev = Tid(m.rev.0 - 1);
    push("other rev".into(), m);
    let mut m = c.clone();
    let i = m.blocks.iter().position(|(x, _)| *x == c.commit).unwrap();
    let mut b = m.blocks[i].1.to_vec();
    let at = super::tests::sig_offset(&b);
    b[at + 7] ^= 0x40;
    let nc = Cid::dag_cbor(&b);
    m.blocks[i] = (nc, Bytes::from(b));
    m.commit = nc;
    m.car_roots[0] = nc;
    push("flipped sig".into(), m);
    out
}

fn compare(name: &str, frame: &Bytes, did_key: &str, diffs: &mut Vec<String>) -> bool {
    let (ours, theirs) = (our_verdict(frame, did_key), shrike_verdict(frame, did_key));
    if ours.is_ok() != theirs.is_ok() {
        diffs.push(format!("{name}: ours {ours:?}, shrike {theirs:?}"));
    }
    ours.is_ok()
}

#[test]
fn synthetic_commits_and_mutations_agree() {
    let mut diffs = Vec::new();
    let (mut accepted, mut rejected) = (0, 0);
    for (curve, seed, initial) in
        [(Curve::K256, 1, 0usize), (Curve::K256, 2, 40), (Curve::P256, 3, 300), (Curve::K256, 4, 2000)]
    {
        let mut r = Repo::new("did:plc:diffdiffdiffdiffdiff", Signer::new(curve, seed), initial);
        let dk = format!("did:key:{}", r.signer.multibase());
        for round in 0..8 {
            let ops = if round == 7 && initial > 0 {
                r.live.keys().take(3).cloned().map(Op::Delete).collect()
            } else {
                r.mixed_ops([1, 2, 6, 20][round % 4])
            };
            let c = commit_of(&r.commit(&ops));
            for (name, f) in mutations(&c) {
                if compare(&format!("{curve:?}/{initial}/{round}/{name}"), &f, &dk, &mut diffs) {
                    accepted += 1;
                } else {
                    rejected += 1;
                }
            }
        }
        let s = r.sync();
        assert!(compare("sync", &s, &dk, &mut diffs));
    }
    let unexplained: Vec<&String> = diffs.iter().filter(|d| !known_split(d)).collect();
    assert!(
        unexplained.is_empty(),
        "{} disagreements:\n{}",
        unexplained.len(),
        unexplained.iter().map(|s| s.as_str()).collect::<Vec<_>>().join("\n")
    );
    assert!(accepted > 30 && rejected > 300, "accepted {accepted}, rejected {rejected}");
}

#[test]
fn real_commits_and_mutations_agree() {
    let keys: HashMap<String, String> = {
        let p = concat!(env!("CARGO_MANIFEST_DIR"), "/testdata/firehose_commit_keys.json");
        let j: serde_json::Value = serde_json::from_slice(&std::fs::read(p).unwrap()).unwrap();
        j.as_object().unwrap().iter().filter_map(|(k, v)| Some((k.clone(), v.as_str()?.to_string()))).collect()
    };
    let dir = concat!(env!("CARGO_MANIFEST_DIR"), "/testdata/shrike/firehose_commits");
    let mut diffs = Vec::new();
    for e in std::fs::read_dir(dir).unwrap() {
        let p = e.unwrap().path();
        let j: serde_json::Value = serde_json::from_slice(&std::fs::read(&p).unwrap()).unwrap();
        let f = frame_from_json("#commit", &j);
        let c = commit_of(&f);
        let dk = &keys[&c.repo];
        assert!(compare("as received", &f, dk, &mut diffs));
        for (name, m) in mutations(&c) {
            compare(&format!("{}: {name}", p.display()), &m, dk, &mut diffs);
        }
    }
    assert!(diffs.is_empty(), "{} disagreements:\n{}", diffs.len(), diffs.join("\n"));
}

#[test]
fn random_byte_flips_agree() {
    let mut rng = StdRng::seed_from_u64(0x5eed);
    let mut r = Repo::new("did:plc:flipflipflipflipflip", Signer::new(Curve::K256, 9), 100);
    let dk = format!("did:key:{}", r.signer.multibase());
    let mut diffs = Vec::new();
    let mut n = 0;
    for _ in 0..30 {
        let ops = r.mixed_ops(rng.gen_range(1..6));
        let f = r.commit(&ops);
        for _ in 0..60 {
            let mut v = f.to_vec();
            for _ in 0..rng.gen_range(1..3) {
                let i = rng.gen_range(0..v.len());
                v[i] ^= 1 << rng.gen_range(0..8);
            }
            compare(&format!("flip {n}"), &Bytes::from(v), &dk, &mut diffs);
            n += 1;
        }
    }
    // Known policy splits are filtered in `known_split`; anything else fails.
    let unexplained: Vec<&String> = diffs.iter().filter(|d| !known_split(d)).collect();
    assert!(
        unexplained.is_empty(),
        "{} of {n} flips disagree:\n{}",
        unexplained.len(),
        unexplained.iter().map(|s| s.as_str()).collect::<Vec<_>>().join("\n")
    );
}

/// Where the two sides differ on purpose. Ours is stricter on the frame:
/// strict DAG-CBOR and syntax checks throughout (shrike ignores unknown
/// fields and doesn't check e.g. `time`). Shrike is stricter in two places:
/// it requires the deprecated `tooBig` field, and inverting an update loads
/// both neighbour subtrees of the key, which indigo, the reference and vlpds
/// don't ship (vlpds's `shrike_update_inversion_overfetch_pinned`).
fn known_split(d: &str) -> bool {
    let shrike_only = d.contains("ours Ok(())")
        && (d.contains("tooBig") || (d.contains("inversion failed") && d.contains("block not found")));
    shrike_only
        || d.contains("ours Err(\"bad_frame\")")
        || d.contains("ours Err(\"bad_header\")")
        || d.contains("ours Err(\"bad_field\")")
        || d.contains("ours Err(\"missing_field\")")
        || d.contains("ours Err(\"bad_op\")")
        || d.contains("ours Err(\"bad_car\")")
        || d.contains("ours Err(\"future_rev\")")
}

/// Live frames from `verify_bench capture` + `verify` (gitignored, so this
/// skips when they're absent): every frame, and its mutations, judged the
/// same way by both sides.
#[test]
fn live_frames_agree() {
    let dir = concat!(env!("CARGO_MANIFEST_DIR"), "/testdata/live");
    let (Ok(frames), Ok(keys)) =
        (std::fs::read(format!("{dir}/frames.bin")), std::fs::read(format!("{dir}/keys.json")))
    else {
        eprintln!("no live frames in {dir}; skipping");
        return;
    };
    let keys: HashMap<String, String> = serde_json::from_slice(&keys).unwrap();
    let (mut i, mut n, mut ok) = (0, 0, 0);
    let mut diffs = Vec::new();
    while i + 4 <= frames.len() {
        let len = u32::from_le_bytes(frames[i..i + 4].try_into().unwrap()) as usize;
        let f = Bytes::copy_from_slice(&frames[i + 4..i + 4 + len]);
        i += 4 + len;
        let Ok(Event::Commit(c)) = parse(f.clone(), &Limits::default()) else {
            continue;
        };
        let Some(mb) = keys.get(&c.repo).filter(|k| !k.is_empty()) else {
            continue;
        };
        let dk = format!("did:key:{mb}");
        ok += compare(&format!("live seq {}", c.seq), &f, &dk, &mut diffs) as usize;
        if n % 10 == 0 {
            for (name, m) in mutations(&c) {
                compare(&format!("live seq {}: {name}", c.seq), &m, &dk, &mut diffs);
            }
        }
        n += 1;
    }
    let unexplained: Vec<&String> = diffs.iter().filter(|d| !known_split(d)).collect();
    assert!(
        unexplained.is_empty(),
        "{} disagreements:\n{}",
        unexplained.len(),
        unexplained.iter().map(|s| s.as_str()).collect::<Vec<_>>().join("\n")
    );
    eprintln!("{n} live commits, {ok} accepted by both; {} known splits", diffs.len());
}
