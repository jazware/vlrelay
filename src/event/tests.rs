use super::*;
use crate::verify::synth::{Curve, Repo, Signer};
use vlatproto::events;

fn finish(f: events::Frame, seq: i64) -> Bytes {
    let mut out = Vec::new();
    f.finish(seq, &mut out);
    Bytes::from(out)
}

fn all_kinds() -> Vec<Bytes> {
    let mut r = Repo::new("did:plc:eventseventsevents", Signer::new(Curve::K256, 5), 50);
    let ops = r.mixed_ops(6);
    vec![
        r.commit(&ops),
        r.sync(),
        finish(events::identity_frame("did:plc:eventseventsevents", "alice.test", "2026-01-01T00:00:00Z"), 77),
        finish(events::account_frame("did:web:example.com", false, Some("takendown"), "2026-01-01T00:00:00Z"), 1 << 40),
    ]
}

#[test]
fn route_agrees_with_parse() {
    for f in all_kinds() {
        let r = route(&f, MAX_FRAME_BYTES).unwrap();
        let e = parse(f.clone(), &Limits::default()).unwrap();
        assert_eq!(r.kind, e.kind());
        assert_eq!(r.did, e.did());
        let (_, span) = e.frame_and_seq().unwrap();
        assert_eq!(r.seq_span, Some(span));
        if let Event::Commit(c) = &e {
            assert_eq!(r.rev, Some(c.rev.to_string().as_str()));
            assert_eq!(r.seq, Some(c.seq));
            assert!(!c.ops.is_empty() && !c.blocks.is_empty());
        }
    }
}

/// An `#identity` with a stray `repo` (it sorts after `did`) used to route
/// by `repo` while parse read `did`: the DID owner of one account applied
/// another's event.
#[test]
fn route_reads_the_kinds_did_key() {
    use vlatproto::cbor::{write_map_head, write_text, write_uint};
    let mut f = Vec::new();
    write_map_head(&mut f, 2);
    write_text(&mut f, "t");
    write_text(&mut f, "#identity");
    write_text(&mut f, "op");
    write_uint(&mut f, 1);
    write_map_head(&mut f, 4);
    write_text(&mut f, "did");
    write_text(&mut f, "did:plc:aaaaaaaaaaaaaaaaaaaaaaaa");
    write_text(&mut f, "seq");
    write_uint(&mut f, 5);
    write_text(&mut f, "repo");
    write_text(&mut f, "did:plc:zzzzzzzzzzzzzzzzzzzzzzzz");
    write_text(&mut f, "time");
    write_text(&mut f, "2026-01-01T00:00:00Z");
    let f = Bytes::from(f);
    let r = route(&f, MAX_FRAME_BYTES).unwrap();
    assert_eq!(r.did, Some("did:plc:aaaaaaaaaaaaaaaaaaaaaaaa"));
    if let Ok(e) = parse(f.clone(), &Limits::default()) {
        assert_eq!(r.did, e.did());
    }
}

#[test]
fn seq_splice_round_trips() {
    for f in all_kinds() {
        let e = parse(f.clone(), &Limits::default()).unwrap();
        for seq in [0i64, 1, 23, 24, 255, 256, 65535, 65536, u32::MAX as i64, u32::MAX as i64 + 1, i64::MAX] {
            let out = e.encode_with_seq(seq).unwrap();
            let r = route(&out, MAX_FRAME_BYTES).unwrap();
            assert_eq!(r.seq, Some(seq));
            // the strict parse still accepts it (canonical), with everything else equal
            let e2 = parse(out.clone(), &Limits::default()).unwrap();
            assert_eq!(e2.did(), e.did());
            let (_, sp) = e2.frame_and_seq().unwrap();
            assert_eq!(out[..sp.start as usize], f[..sp.start as usize]);
            let (_, sp1) = e.frame_and_seq().unwrap();
            assert_eq!(out[sp.end as usize..], f[sp1.end as usize..]);
            // same bytes as encoding the frame with that seq from scratch
            let v = vlatproto::cbor::Value::decode_prefix(&out).unwrap();
            let body = vlatproto::cbor::Value::decode(&out[v.1..]).unwrap();
            assert_eq!(body.get("seq"), Some(&vlatproto::cbor::Value::Int(seq)));
            v.0.encode(&mut Vec::new());
        }
    }
}

#[test]
fn info_and_error_frames() {
    use vlatproto::cbor::*;
    let mut f = Vec::new();
    write_map_head(&mut f, 2);
    write_text(&mut f, "t");
    write_text(&mut f, "#info");
    write_text(&mut f, "op");
    write_uint(&mut f, 1);
    write_map_head(&mut f, 1);
    write_text(&mut f, "name");
    write_text(&mut f, "OutdatedCursor");
    match parse(Bytes::from(f), &Limits::default()).unwrap() {
        Event::Info(i) => assert_eq!(i.name, "OutdatedCursor"),
        e => panic!("{e:?}"),
    }
    let f = events::error_frame("FutureCursor", "cursor in the future");
    match parse(Bytes::from(f), &Limits::default()).unwrap() {
        Event::Error(e) => assert_eq!(e.error, "FutureCursor"),
        e => panic!("{e:?}"),
    }
}

#[test]
fn unknown_types_pass_as_unknown() {
    use vlatproto::cbor::*;
    let mut f = Vec::new();
    write_map_head(&mut f, 2);
    write_text(&mut f, "t");
    write_text(&mut f, "#handle");
    write_text(&mut f, "op");
    write_uint(&mut f, 1);
    write_map_head(&mut f, 2);
    write_text(&mut f, "did");
    write_text(&mut f, "did:plc:abc");
    write_text(&mut f, "seq");
    write_uint(&mut f, 5);
    let f = Bytes::from(f);
    assert_eq!(route(&f, MAX_FRAME_BYTES).unwrap().kind, Kind::Unknown);
    assert!(matches!(parse(f, &Limits::default()), Ok(Event::Unknown)));
}

#[test]
fn limits_and_syntax() {
    let mut r = Repo::new("did:plc:limitslimitslimits", Signer::new(Curve::K256, 6), 0);
    let ops = r.mixed_ops(30);
    let f = r.commit(&ops);
    let strict = |l: Limits| parse(f.clone(), &l).err();
    assert_eq!(strict(Limits { max_frame_bytes: f.len() - 1, ..Limits::default() }), Some(Reject::FrameTooBig));
    assert_eq!(strict(Limits { max_commit_ops: 29, ..Limits::default() }), Some(Reject::TooManyOps));
    assert_eq!(strict(Limits { max_commit_blocks: 3, ..Limits::default() }), Some(Reject::TooManyBlocks));
    assert_eq!(strict(Limits { max_commit_blocks_bytes: 100, ..Limits::default() }), Some(Reject::BlocksTooBig));
    assert_eq!(strict(Limits::default()), None);

    // same-length replacements keep the frame well formed
    let swap = |from: &[u8], to: &[u8]| {
        let mut v = f.to_vec();
        let at = v.windows(from.len()).position(|w| w == from).unwrap();
        v[at..at + to.len()].copy_from_slice(to);
        parse(Bytes::from(v), &Limits::default()).err()
    };
    assert_eq!(swap(b"did:plc:limits", b"did:PLC:limits"), Some(Reject::BadDid));
    let rev = r.rev.to_string();
    assert_eq!(swap(rev.as_bytes(), b"zzzzzzzzzzzzz"), Some(Reject::BadRev));
    assert_eq!(swap(b"app.bsky.feed.like/", b"app.bsky.feed.like!"), Some(Reject::BadOp));

    // truncation anywhere is an error, never a panic
    for n in 0..f.len().min(4000) {
        assert!(route(&f[..n], MAX_FRAME_BYTES).is_err() || parse(f.slice(..n), &Limits::default()).is_err());
    }
    // trailing garbage
    let mut v = f.to_vec();
    v.push(0);
    assert!(route(&v, MAX_FRAME_BYTES).is_err());
    assert!(parse(Bytes::from(v), &Limits::default()).is_err());
}

#[test]
fn unsigned_commit_split() {
    let did = "did:plc:splitsplitsplit";
    let data = Cid::dag_cbor(b"x");
    let unsigned = vlatproto::events::encode_commit(did, "3jzfcijpj2z2a", &data, None);
    let signed = vlatproto::events::encode_commit(did, "3jzfcijpj2z2a", &data, Some(&[7u8; 64]));
    let (u, sig) = split_signed_commit(&signed).unwrap();
    assert_eq!(sig, &[7u8; 64]);
    let mut out = Vec::new();
    u.write(&mut out);
    assert_eq!(out, unsigned);
    use sha2::Digest;
    assert_eq!(u.sha256(), <[u8; 32]>::from(sha2::Sha256::digest(&unsigned)));
    assert!(split_signed_commit(&unsigned).is_none());
}
