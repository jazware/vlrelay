//! The one-create fast path in `check_ops` against the tree path, on
//! randomized repos and mutated commits: same verdict and reason always.

use super::synth::{Curve, Op, Repo, Signer};
use super::*;
use crate::event::{Event, Limits, ParsedCommit, parse};
use rand::{Rng, SeedableRng, rngs::StdRng};

fn commit_of(frame: Bytes) -> ParsedCommit {
    match parse(frame, &Limits::default()).expect("parse") {
        Event::Commit(c) => c,
        e => panic!("not a commit: {e:?}"),
    }
}

fn both(c: &ParsedCommit, data: Cid, opts: &Options) -> Result<(), Reject> {
    let blocks: BlockMap<'_> = c.blocks.iter().map(|(k, v)| (*k, &v[..])).collect();
    set_fast_path(false);
    let slow = check_ops(c, data, &blocks, opts);
    set_fast_path(true);
    let fast = check_ops(c, data, &blocks, opts);
    assert_eq!(fast, slow, "fast and tree paths disagree");
    fast
}

fn data_of(c: &ParsedCommit) -> Cid {
    let b = c.blocks.iter().find(|(k, _)| *k == c.commit).unwrap();
    decode_commit(&b.1).unwrap().data
}

/// A create at a random key, in one of a few collections so that keys
/// land at every height and on both sides of the existing ones.
fn random_create(rng: &mut StdRng) -> Op {
    const COLLECTIONS: [&str; 4] = ["app.bsky.feed.like", "app.bsky.feed.post", "app.bsky.graph.follow", "a.b.c"];
    let col = COLLECTIONS[rng.gen_range(0..COLLECTIONS.len())];
    let tid = vlsync_atproto::tid::Tid::from_parts(rng.gen_range(1_600_000_000_000_000..1_800_000_000_000_000), 0);
    Op::Put(format!("{col}/{tid}"))
}

#[test]
fn one_create_fast_path_matches_tree_path() {
    let opts = Options::default();
    let mut rng = StdRng::seed_from_u64(0x5eed);
    let (mut fast_hits, mut total) = (0, 0);
    for initial in [0usize, 1, 2, 3, 5, 17, 100, 1000] {
        let mut r = Repo::new("did:plc:fastfastfastfastfast", Signer::new(Curve::K256, 3), initial);
        for _ in 0..60 {
            let op = random_create(&mut rng);
            if let Op::Put(p) = &op
                && r.live.contains_key(p)
            {
                continue;
            }
            let c = commit_of(r.commit(&[op]));
            let data = data_of(&c);
            total += 1;
            assert_eq!(both(&c, data, &opts), Ok(()));
            let blocks: BlockMap<'_> = c.blocks.iter().map(|(k, v)| (*k, &v[..])).collect();
            let op = &c.ops[0];
            if vlsync_atproto::mst::single_create::undo_single_create(
                &blocks,
                data,
                op.path.as_bytes(),
                op.cid.unwrap(),
                true,
            ) == Some(c.prev_data)
            {
                fast_hits += 1;
            }

            // sync 1.0 shape, wrong prevData, wrong record, every block dropped
            let mut m = c.clone();
            m.prev_data = None;
            assert_eq!(both(&m, data, &opts), Ok(()));
            let mut m = c.clone();
            m.prev_data = Some(Cid::dag_cbor(b"elsewhere"));
            assert!(both(&m, data, &opts).is_err());
            let mut m = c.clone();
            m.ops[0].cid = Some(Cid::dag_cbor(b"another record"));
            let lax = Options { require_record_blocks: false, ..Options::default() };
            assert!(both(&m, data, &lax).is_err());
            for i in 0..c.blocks.len() {
                let mut m = c.clone();
                m.blocks.remove(i);
                let _ = both(&m, data, &lax);
            }
            // the create pointed at another live key (found, but undoing it
            // removes the wrong key)
            if let Some((k, v)) = r.live.iter().nth(rng.gen_range(0..r.live.len())) {
                let mut m = c.clone();
                m.ops[0].path = k.clone();
                m.ops[0].cid = Some(*v);
                let _ = both(&m, data, &lax);
            }
        }
    }
    // the fast path must carry the common case, not just fall back
    assert!(fast_hits * 10 >= total * 7, "fast path took {fast_hits} of {total}");
}

/// Node blocks rewritten (and re-hashed, so they pass the block check):
/// each change must get the same verdict from both paths.
#[test]
fn rewritten_nodes_agree() {
    let opts = Options { require_record_blocks: false, ..Options::default() };
    let mut rng = StdRng::seed_from_u64(42);
    for initial in [2usize, 40, 600] {
        let mut r = Repo::new("did:plc:rewriterewriterewrite", Signer::new(Curve::K256, 5), initial);
        for _ in 0..25 {
            let c = commit_of(r.commit(&[random_create(&mut rng)]));
            let data = data_of(&c);
            for i in 0..c.blocks.len() {
                if c.blocks[i].0 == c.commit {
                    continue;
                }
                for _ in 0..12 {
                    let mut b = c.blocks[i].1.to_vec();
                    let at = rng.gen_range(0..b.len());
                    b[at] ^= 1 << rng.gen_range(0..8);
                    let new = Cid::dag_cbor(&b);
                    let old = c.blocks[i].0;
                    let mut m = c.clone();
                    m.blocks[i] = (new, Bytes::from(b));
                    // point the parent (or the root) at the rewritten node
                    let mut root = data;
                    if old == data {
                        root = new;
                    } else {
                        for (_, pb) in m.blocks.iter_mut() {
                            let ob = old.to_bytes();
                            if let Some(p) = pb.windows(ob.len()).position(|w| w == ob) {
                                let mut v = pb.to_vec();
                                v[p..p + ob.len()].copy_from_slice(&new.to_bytes());
                                *pb = Bytes::from(v);
                            }
                        }
                        // parents changed bytes too: re-key them, root last
                        let mut changed = true;
                        while changed {
                            changed = false;
                            for j in 0..m.blocks.len() {
                                let h = Cid::dag_cbor(&m.blocks[j].1);
                                if m.blocks[j].0 != h && m.blocks[j].0 != m.commit {
                                    let was = m.blocks[j].0;
                                    m.blocks[j].0 = h;
                                    if was == root {
                                        root = h;
                                    }
                                    let (wb, hb) = (was.to_bytes(), h.to_bytes());
                                    for k in 0..m.blocks.len() {
                                        let kb = m.blocks[k].1.clone();
                                        if let Some(p) = kb.windows(wb.len()).position(|w| w == wb) {
                                            let mut v = kb.to_vec();
                                            v[p..p + wb.len()].copy_from_slice(&hb);
                                            m.blocks[k].1 = Bytes::from(v);
                                            changed = true;
                                        }
                                    }
                                }
                            }
                        }
                    }
                    let _ = both(&m, root, &opts);
                }
            }
        }
    }
}
