use super::fetch::{self, Fetched};
use super::*;
use crate::state::tests::{MapIdentity, open, plc};
use crate::state::{Applied, CommitClaim, EventKind, Incoming, ReplaySource, StateDelta, StubChain, Ticket};
use crate::types::Host;
use crate::verify::synth::{Curve, Op, Repo, Signer, record};
use std::collections::{BTreeMap, HashMap};
use vlpds::cid::Cid;
use vlpds::slots::ShardId;
use vlpds::state as vs;

const NOW: u32 = 1_800_000_000;

/// The process-wide MST node cache is content-addressed, and synthetic repos
/// share subtrees: the test that empties it to force reads runs alone.
static NODE_CACHE_USE: tokio::sync::RwLock<()> = tokio::sync::RwLock::const_new(());

struct NoResolver;

#[async_trait::async_trait]
impl Resolver for NoResolver {
    async fn resolve(&self, _did: &str) -> anyhow::Result<Resolved> {
        anyhow::bail!("no network in tests")
    }
}

fn gate(on: bool, retention_secs: u32) -> Arc<StaticGate> {
    Arc::new(StaticGate { on, limits: FetchLimits::default(), retention_secs })
}

/// A synthetic account and every record block it has written.
struct Acct {
    repo: Repo,
    blocks: HashMap<Cid, Vec<u8>>,
}

impl Acct {
    fn new(did: &str, seed: u64, initial: usize) -> Acct {
        let repo = Repo::new(did, Signer::new(Curve::K256, seed), initial);
        let mut blocks = HashMap::new();
        for (i, p) in repo.live.keys().enumerate() {
            let b = record(p, i as u64);
            blocks.insert(Cid::dag_cbor(&b), b);
        }
        // `Repo::new` numbers its records in creation order, which is key
        // order for its TID keys
        assert!(repo.live.values().all(|c| blocks.contains_key(c)), "initial record blocks");
        Acct { repo, blocks }
    }

    /// A commit's frame and what StubChain needs to accept it.
    fn commit(&mut self, ops: &[Op]) -> (Bytes, CommitClaim) {
        let before = self.repo.tree.root_cid().unwrap();
        let frame = self.repo.commit(ops);
        let crate::event::Event::Commit(c) = crate::event::parse(frame.clone(), &Default::default()).unwrap() else {
            panic!("not a commit")
        };
        for (cid, b) in &c.blocks {
            self.blocks.insert(*cid, b.to_vec());
        }
        let data = self.repo.tree.root_cid().unwrap();
        (frame, CommitClaim { rev: self.repo.rev, commit: self.repo.commit, data, prev_data: Some(before) })
    }

    /// The whole repo as a getRepo CAR (commit, nodes, records).
    fn car(&mut self) -> Bytes {
        let mut out = Vec::new();
        vlpds::car::write_header(&mut out, &self.repo.commit);
        vlpds::car::write_block(&mut out, &self.repo.commit, self.repo.commit_block());
        self.repo.tree.root_cid().unwrap();
        self.repo.tree.walk_blocks(&mut |c, b| vlpds::car::write_block(&mut out, &c, b)).unwrap();
        for c in self.repo.live.values() {
            vlpds::car::write_block(&mut out, c, &self.blocks[c]);
        }
        Bytes::from(out)
    }

    fn fetched(&mut self) -> Fetched {
        let car = self.car();
        fetch::check_car(&self.repo.did, &car, &self.repo.signer.public()).unwrap()
    }
}

async fn store(on: bool) -> (Arc<StateStore>, Arc<Archive>, Arc<MapIdentity>) {
    let id = MapIdentity::new();
    let st = open(2, id.clone(), Default::default()).await;
    let a = Archive::new(gate(on, 0), Arc::new(NoResolver));
    st.set_archive(a.clone());
    (st, a, id)
}

async fn apply(st: &StateStore, did: &str, frame: &Bytes, c: CommitClaim) -> Ticket {
    let h = Host("pds.a".into());
    let ev = Incoming { did, host: &h, now: NOW, kind: EventKind::Commit(c) };
    match st.apply_with_frame(ev, Some(frame)).await.unwrap() {
        Applied::Append(a) => a.ticket,
        Applied::Duplicate => panic!("duplicate"),
    }
}

/// The mirror's records and its exported CAR, checked against the account.
async fn assert_mirror(st: &StateStore, acct: &mut Acct) {
    let did = acct.repo.did.clone();
    let s = st.shard_for(&did).unwrap();
    let (generation, head) = mirror::live(&s.db, &did).await.unwrap().expect("mirrored");
    let want_root = acct.repo.tree.root_cid().unwrap();
    assert_eq!(head.data, want_root, "head");
    assert_eq!(head.commit, acct.repo.commit);
    // records
    let prefix = vs::record_prefix(&did, generation);
    let mut it = vs::BatchedScan::new(s.db.scan(prefix.clone()..vs::prefix_end(&prefix)).await.unwrap());
    let mut got = BTreeMap::new();
    while let Some(kv) = it.next().await.unwrap() {
        let (c, b) = vs::decode_record_value(&kv.value).unwrap();
        assert_eq!(Cid::dag_cbor(&b), c);
        got.insert(String::from_utf8(kv.key[prefix.len()..].to_vec()).unwrap(), c);
    }
    assert_eq!(got, acct.repo.live, "records");
    // `M/` holds exactly the tree's interior nodes
    let want: std::collections::HashSet<Cid> =
        vlpds::mst_lazy::persisted_nodes(&acct.repo.tree, mirror::PERSIST_MIN).into_keys().collect();
    let np = vs::mst_node_prefix(&did, generation);
    let mut it = vs::BatchedScan::new(s.db.scan(np.clone()..vs::prefix_end(&np)).await.unwrap());
    let mut nodes = std::collections::HashSet::new();
    while let Some(kv) = it.next().await.unwrap() {
        nodes.insert(Cid::dag_cbor(&kv.value));
    }
    assert_eq!(nodes, want, "M/ nodes");
    // the export streams the same blocks
    let car = export(&s, &did, generation, head).await;
    let (roots, blocks) = vlpds::car::read_car(&car).unwrap();
    assert_eq!(roots, vec![acct.repo.commit]);
    let mine: HashMap<Cid, Vec<u8>> = blocks.iter().map(|(c, b)| (*c, b.to_vec())).collect();
    let theirs = acct.car();
    let (_, tb) = vlpds::car::read_car(&theirs).unwrap();
    let theirs: HashMap<Cid, Vec<u8>> = tb.iter().map(|(c, b)| (*c, b.to_vec())).collect();
    assert_eq!(mine.len(), theirs.len(), "block count");
    assert!(mine == theirs, "exported blocks differ");
}

async fn export(s: &ShardState, did: &str, generation: u64, head: vlpds::state::Head) -> Vec<u8> {
    let snap = s.db.snapshot().await.unwrap();
    let (tx, mut rx) = tokio::sync::mpsc::channel(4);
    let did: Arc<str> = did.into();
    let h = tokio::spawn(async move {
        vlpds::xrpc::stream_export(snap, did, generation, head, None, &tx, std::time::Duration::from_secs(10)).await
    });
    let mut out = Vec::new();
    while let Some(c) = rx.recv().await {
        out.extend_from_slice(&c.unwrap());
    }
    h.await.unwrap().unwrap();
    out
}

#[tokio::test]
async fn live_commits_apply_to_the_stored_tree() {
    let _cache = NODE_CACHE_USE.read().await;
    let (st, a, id) = store(true).await;
    let did = plc(1);
    id.set(&did, "pds.a", 1);
    let mut acct = Acct::new(&did, 1, 300);
    fetch::import(&a, &st, &did, acct.fetched()).await.unwrap();
    assert_mirror(&st, &mut acct).await;

    let mut tickets = Vec::new();
    for i in 0..60 {
        let ops = acct.repo.mixed_ops(1 + i % 4);
        let (f, c) = acct.commit(&ops);
        tickets.push(apply(&st, &did, &f, c).await);
        // commit in uneven batches: the next commits build on the
        // in-memory tree meanwhile
        if i % 7 == 6 {
            st.commit(&std::mem::take(&mut tickets)).await.unwrap();
        }
    }
    st.commit(&tickets).await.unwrap();
    assert_eq!(a.stats.mismatches.load(Relaxed), 0);
    assert_eq!(a.stats.applied.load(Relaxed), 60);
    assert_eq!(st.shard_for(&did).unwrap().mirror.len().1, 0, "every ticket's rows written");
    assert_mirror(&st, &mut acct).await;
}

struct Frames {
    deltas: Vec<(u64, Vec<StateDelta>)>,
    frames: Vec<(u64, Vec<(String, Bytes)>)>,
}

#[async_trait::async_trait]
impl ReplaySource for Frames {
    async fn tail(&self, _: &str, _: ShardId, after: Option<u64>) -> anyhow::Result<Vec<(u64, Vec<StateDelta>)>> {
        Ok(self.deltas.iter().filter(|(o, _)| after.is_none_or(|a| *o > a)).cloned().collect())
    }
    async fn frames(
        &self,
        _: &str,
        _: ShardId,
        after: Option<u64>,
    ) -> anyhow::Result<Vec<(u64, Vec<(String, Bytes)>)>> {
        Ok(self.frames.iter().filter(|(o, _)| after.is_none_or(|a| *o > a)).cloned().collect())
    }
}

#[tokio::test]
async fn replay_rebuilds_the_mirror() {
    let _cache = NODE_CACHE_USE.read().await;
    let (st, a, id) = store(true).await;
    let did = plc(2);
    id.set(&did, "pds.a", 1);
    let mut acct = Acct::new(&did, 2, 120);
    fetch::import(&a, &st, &did, acct.fetched()).await.unwrap();
    let h = Host("pds.a".into());
    let mut src = Frames { deltas: Vec::new(), frames: Vec::new() };
    for i in 0..25u64 {
        let ops = acct.repo.mixed_ops(2);
        let (f, c) = acct.commit(&ops);
        let ev = Incoming { did: &did, host: &h, now: NOW, kind: EventKind::Commit(c) };
        let Applied::Append(x) = st.apply_with_frame(ev, Some(&f)).await.unwrap() else { panic!() };
        // the first 10 reach the DB; the rest die with the process
        if i < 10 {
            st.commit(&[x.ticket]).await.unwrap();
        }
        src.deltas.push((i, vec![x.delta]));
        src.frames.push((i, vec![(did.clone(), f)]));
    }
    let s = st.shard_for(&did).unwrap();
    s.checkpoint("log-a", 9).await.unwrap();

    // a new owner: the same bucket, the log tail past the marker
    let st2 = Arc::new(StateStore::new(
        st.store.clone(),
        vlpds::slots::Layout::uniform(2).shards,
        StubChain,
        id.clone(),
        Default::default(),
    ));
    let a2 = Archive::new(gate(true, 0), Arc::new(NoResolver));
    st2.set_archive(a2.clone());
    let sid = st2.shard_id_of_slot(vlpds::slots::slot_of(&did));
    for l in vlpds::slots::Layout::uniform(2).shards {
        st2.open_shard(l.id, None).await.unwrap();
    }
    st2.recover(sid, "log-a", &src, NOW).await.unwrap();
    assert_eq!(a2.stats.replayed.load(Relaxed), 15);
    assert!(!a2.queue.contains(&did));
    assert_mirror(&st2, &mut acct).await;
}

#[tokio::test]
async fn bootstrap_applies_commits_that_arrived_meanwhile() {
    let _cache = NODE_CACHE_USE.read().await;
    let (st, a, id) = store(true).await;
    let did = plc(3);
    id.set(&did, "pds.a", 1);
    let mut acct = Acct::new(&did, 3, 80);
    // the first commit finds no mirror and queues a fetch
    let (f, c) = {
        let p = acct.repo.new_path();
        acct.commit(&[Op::Put(p)])
    };
    let t = apply(&st, &did, &f, c).await;
    st.commit(&[t]).await.unwrap();
    assert!(a.queue.contains(&did));
    // the fetch reads the repo here...
    let fetched = acct.fetched();
    // ...while more commits arrive, are emitted, and wait in the queue
    let mut ts = Vec::new();
    for _ in 0..12 {
        let ops = acct.repo.mixed_ops(2);
        let (f, c) = acct.commit(&ops);
        ts.push(apply(&st, &did, &f, c).await);
    }
    st.commit(&ts).await.unwrap();
    assert_eq!(a.stats.buffered.load(Relaxed), 12);
    fetch::import(&a, &st, &did, fetched).await.unwrap();
    assert!(!a.queue.contains(&did));
    assert_eq!(a.queue.stats.replayed_frames.load(Relaxed), 12);
    assert_mirror(&st, &mut acct).await;
    // and live from here
    let (f, c) = {
        let p = acct.repo.new_path();
        acct.commit(&[Op::Put(p)])
    };
    let t = apply(&st, &did, &f, c).await;
    st.commit(&[t]).await.unwrap();
    assert_mirror(&st, &mut acct).await;
}

#[tokio::test]
async fn a_corrupt_mirror_is_refetched_not_trusted() {
    let _cache = NODE_CACHE_USE.write().await;
    let (st, a, id) = store(true).await;
    let did = plc(4);
    id.set(&did, "pds.a", 1);
    let mut acct = Acct::new(&did, 4, 400);
    fetch::import(&a, &st, &did, acct.fetched()).await.unwrap();
    // lose every record: leaves rebuilt from them no longer match
    let s = st.shard_for(&did).unwrap();
    let (generation, _) = mirror::live(&s.db, &did).await.unwrap().unwrap();
    let prefix = vs::record_prefix(&did, generation);
    let mut it = vs::BatchedScan::new(s.db.scan(prefix.clone()..vs::prefix_end(&prefix)).await.unwrap());
    let mut dels = Vec::new();
    while let Some(kv) = it.next().await.unwrap() {
        dels.push(vlpds::segment::Mutation { key: kv.key, val: None });
    }
    mirror::write_rows(&s.db, dels).await.unwrap();
    // nodes are content-addressed, so cached leaves would still be right:
    // make the walk rebuild them from the (now missing) records
    vlpds::mst_store::NODE_CACHE.clear();
    let (f, c) = {
        let ops = acct.repo.mixed_ops(1);
        acct.commit(&ops)
    };
    let t = apply(&st, &did, &f, c).await;
    st.commit(&[t]).await.unwrap();
    assert_eq!(a.stats.mismatches.load(Relaxed), 1);
    assert!(a.queue.contains(&did), "queued for a fresh copy");
    // the fresh copy (with the commit in the buffer) converges
    fetch::import(&a, &st, &did, acct.fetched()).await.unwrap();
    assert_mirror(&st, &mut acct).await;
}

#[tokio::test]
async fn a_broken_chain_queues_a_fetch_that_heals_the_account() {
    let _cache = NODE_CACHE_USE.read().await;
    let (st, a, id) = store(true).await;
    let did = plc(5);
    id.set(&did, "pds.a", 1);
    let mut acct = Acct::new(&did, 5, 50);
    fetch::import(&a, &st, &did, acct.fetched()).await.unwrap();
    let (f, c) = {
        let ops = acct.repo.mixed_ops(1);
        acct.commit(&ops)
    };
    let t = apply(&st, &did, &f, c).await;
    st.commit(&[t]).await.unwrap();
    // a commit the relay never sees
    let _ = {
        let ops = acct.repo.mixed_ops(1);
        acct.commit(&ops)
    };
    let (f, c) = {
        let ops = acct.repo.mixed_ops(1);
        acct.commit(&ops)
    };
    let h = Host("pds.a".into());
    let ev = Incoming { did: &did, host: &h, now: NOW, kind: EventKind::Commit(c) };
    assert!(st.apply_with_frame(ev, Some(&f)).await.is_err(), "prevData mismatch");
    assert!(a.queue.contains(&did));
    let rec = st.get(&did).await.unwrap().unwrap();
    assert!(rec.desync.is_some());
    fetch::import(&a, &st, &did, acct.fetched()).await.unwrap();
    let rec = st.get(&did).await.unwrap().unwrap();
    assert!(rec.desync.is_none(), "healed");
    assert_eq!(rec.chain.unwrap().rev, acct.repo.rev);
    assert_eq!(a.queue.stats.healed.load(Relaxed), 1);
    assert_mirror(&st, &mut acct).await;
    // the next commit chains again
    let (f, c) = {
        let ops = acct.repo.mixed_ops(1);
        acct.commit(&ops)
    };
    let t = apply(&st, &did, &f, c).await;
    st.commit(&[t]).await.unwrap();
    assert_mirror(&st, &mut acct).await;
}

#[tokio::test]
async fn switching_off_and_takedowns_delete_in_the_background() {
    let _cache = NODE_CACHE_USE.read().await;
    let (st, a, id) = store(true).await;
    let (d1, d2) = (plc(6), plc(7));
    let mut accts = Vec::new();
    for (i, d) in [&d1, &d2].into_iter().enumerate() {
        id.set(d, "pds.a", 1);
        let mut acct = Acct::new(d, 6 + i as u64, 60);
        let (f, c) = {
            let ops = acct.repo.mixed_ops(1);
            acct.commit(&ops)
        };
        let t = apply(&st, d, &f, c).await;
        st.commit(&[t]).await.unwrap();
        fetch::import(&a, &st, d, acct.fetched()).await.unwrap();
        accts.push(acct);
    }
    let rows = |s: Arc<ShardState>, d: String| async move {
        let mut n = 0;
        for fam in vs::GEN_FAMILIES {
            for generation in 0..4 {
                let p = vs::gen_prefix(fam, &d, generation);
                let mut it = vs::BatchedScan::new(s.db.scan(p.clone()..vs::prefix_end(&p)).await.unwrap());
                while it.next().await.unwrap().is_some() {
                    n += 1;
                }
            }
        }
        n
    };
    let s1 = st.shard_for(&d1).unwrap();
    let s2 = st.shard_for(&d2).unwrap();
    assert!(rows(s1.clone(), d1.clone()).await > 60);
    // a takedown stops reads at once (the sync record says so) and deletes
    // after the retention (0 here) on the sweep after the one that saw it
    st.set_relay_takedown(&d1, true).await.unwrap();
    sweep::sweep_once(&a, &st, false).await.unwrap();
    assert!(mirror::live(&s1.db, &d1).await.unwrap().is_some(), "kept until the retention passes");
    sweep::sweep_once(&a, &st, false).await.unwrap();
    assert!(mirror::live(&s1.db, &d1).await.unwrap().is_none());
    assert_eq!(rows(s1.clone(), d1.clone()).await, 0);
    assert!(mirror::read_meta(&s1.db, &d1).await.unwrap().is_none(), "meta gone");
    assert!(mirror::live(&s2.db, &d2).await.unwrap().is_some(), "others untouched");
    // archiving off: everything goes
    a.set_gate(gate(false, 0));
    let r = sweep::sweep_once(&a, &st, true).await.unwrap();
    assert_eq!(r.deleted, 1);
    assert_eq!(rows(s2.clone(), d2.clone()).await, 0);
    // and on again: the rescan queues every active account
    a.set_gate(gate(true, 0));
    let r = sweep::sweep_once(&a, &st, true).await.unwrap();
    assert_eq!(r.queued, 1, "the taken-down account isn't queued");
    assert!(a.queue.contains(&d2));
}

#[tokio::test]
async fn reads_follow_the_account_status() {
    let _cache = NODE_CACHE_USE.read().await;
    use tower::ServiceExt;
    let (st, a, id) = store(true).await;
    let did = plc(8);
    id.set(&did, "pds.a", 1);
    let mut acct = Acct::new(&did, 8, 40);
    let (f, c) = {
        let ops = acct.repo.mixed_ops(1);
        acct.commit(&ops)
    };
    let t = apply(&st, &did, &f, c).await;
    st.commit(&[t]).await.unwrap();
    let app = read::router(st.clone(), None);
    let get = |uri: String| {
        let app = app.clone();
        async move {
            let r = app.oneshot(axum::http::Request::get(uri).body(axum::body::Body::empty()).unwrap()).await.unwrap();
            let status = r.status();
            let body = axum::body::to_bytes(r.into_body(), 1 << 26).await.unwrap();
            (status, body)
        }
    };
    let (s, b) = get(format!("/xrpc/com.atproto.sync.getRepo?did={did}")).await;
    assert_eq!(s, 400);
    assert!(String::from_utf8_lossy(&b).contains("RepoNotFound"), "not archived yet");
    fetch::import(&a, &st, &did, acct.fetched()).await.unwrap();
    let (s, b) = get(format!("/xrpc/com.atproto.sync.getRepo?did={did}")).await;
    assert_eq!(s, 200);
    let (roots, _) = vlpds::car::read_car(&b).unwrap();
    assert_eq!(roots, vec![acct.repo.commit]);
    let path = acct.repo.live.keys().nth(3).unwrap().clone();
    let (coll, rkey) = path.split_once('/').unwrap();
    let (s, b) = get(format!("/xrpc/com.atproto.sync.getRecord?did={did}&collection={coll}&rkey={rkey}")).await;
    assert_eq!(s, 200);
    let (_, blocks) = vlpds::car::read_car(&b).unwrap();
    assert!(blocks.iter().any(|(c, _)| *c == acct.repo.live[&path]), "the record");
    let rc = acct.repo.live[&path];
    let leaf = acct.repo.tree.proof_blocks(path.as_bytes()).unwrap().last().unwrap().0;
    let (s, b) =
        get(format!("/xrpc/com.atproto.sync.getBlocks?did={did}&cids={rc}&cids={leaf}&cids={}", acct.repo.commit))
            .await;
    assert_eq!(s, 200, "{}", String::from_utf8_lossy(&b));
    let (_, blocks) = vlpds::car::read_car(&b).unwrap();
    assert_eq!(blocks.len(), 3);
    let (s, _) = get(format!("/xrpc/com.atproto.sync.listBlobs?did={did}")).await;
    assert_eq!(s, 501);
    st.set_relay_takedown(&did, true).await.unwrap();
    let (s, b) = get(format!("/xrpc/com.atproto.sync.getRepo?did={did}")).await;
    assert_eq!(s, 400);
    assert!(String::from_utf8_lossy(&b).contains("RepoTakendown"));
}

#[tokio::test]
async fn fetches_over_http_from_the_pds() {
    let _cache = NODE_CACHE_USE.read().await;
    let id = MapIdentity::new();
    let did = plc(9);
    id.set(&did, "pds.a", 1);
    let mut acct = Acct::new(&did, 9, 200);
    let car = acct.car();
    let (commit, rev) = (acct.repo.commit.to_string(), acct.repo.rev.to_string());
    let pds = axum::Router::new()
        .route(
            "/xrpc/com.atproto.sync.getLatestCommit",
            axum::routing::get(move || async move { axum::Json(serde_json::json!({"cid": commit, "rev": rev})) }),
        )
        .route("/xrpc/com.atproto.sync.getRepo", axum::routing::get(move || async move { car.clone() }));
    let l = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let endpoint = format!("http://{}", l.local_addr().unwrap());
    tokio::spawn(async move { axum::serve(l, pds).await });
    struct Fixed(String, crate::verify::SigningKey);
    #[async_trait::async_trait]
    impl Resolver for Fixed {
        async fn resolve(&self, _did: &str) -> anyhow::Result<Resolved> {
            Ok(Resolved { endpoint: self.0.clone(), key: self.1.clone() })
        }
    }
    let a = Archive::new(gate(true, 0), Arc::new(Fixed(endpoint, acct.repo.signer.public())));
    let st = open(2, id.clone(), Default::default()).await;
    st.set_archive(a.clone());
    st.archive_fetch_now(&did).await.unwrap();
    assert_mirror(&st, &mut acct).await;
    assert_eq!(a.queue.stats.records.load(Relaxed), 200);
    // a PDS that doesn't answer fails the fetch, and the queue says why
    let did2 = plc(10);
    let a_bad = Archive::new(gate(true, 0), Arc::new(Fixed("http://127.0.0.1:1".into(), acct.repo.signer.public())));
    let st3 = open(1, id, Default::default()).await;
    st3.set_archive(a_bad.clone());
    assert!(st3.archive_fetch_now(&did2).await.is_err());
    assert_eq!(a_bad.queue.errors.lock().len(), 1);
}

#[test]
fn meta_round_trips() {
    for m in [
        Meta::default(),
        Meta { live: Some(3), staging: None, garbage: vec![1, 2], takedown_at: 0 },
        Meta { live: None, staging: Some(9), garbage: vec![], takedown_at: 77 },
    ] {
        assert_eq!(Meta::decode(&m.encode()).unwrap(), m);
    }
    assert_eq!(Meta { live: Some(3), staging: Some(5), garbage: vec![7], takedown_at: 0 }.next_gen(), 8);
}

fn cpu_us() -> u64 {
    let mut ru: libc::rusage = unsafe { std::mem::zeroed() };
    unsafe { libc::getrusage(libc::RUSAGE_SELF, &mut ru) };
    let tv = |t: libc::timeval| t.tv_sec as u64 * 1_000_000 + t.tv_usec as u64;
    tv(ru.ru_utime) + tv(ru.ru_stime)
}

async fn bootstrap_one(a: Arc<Archive>, st: Arc<StateStore>, did: String, key: crate::verify::SigningKey, car: Bytes) {
    let d = did.clone();
    let f = tokio::task::spawn_blocking(move || fetch::check_car(&d, &car, &key)).await.unwrap().unwrap();
    fetch::import(&a, &st, &did, f).await.unwrap();
}

/// Bytes per record, apply cost with archival on and off, bootstrap and
/// getRepo throughput (docs/archival.md). Run with
/// `cargo test --profile dev-release --lib bench_archival -- --ignored --nocapture`.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore]
async fn bench_archival() {
    let _cache = NODE_CACHE_USE.read().await;
    let env = |k: &str, d: usize| std::env::var(k).ok().and_then(|v| v.parse().ok()).unwrap_or(d);
    let (n, recs, commits) = (env("REPOS", 200), env("RECORDS", 300), env("COMMITS", 20));
    let mut accts: Vec<Acct> = (0..n).map(|i| Acct::new(&plc(10_000 + i as u64), 100 + i as u64, recs)).collect();
    let cars: Vec<Bytes> = accts.iter_mut().map(|a| a.car()).collect();
    let car_bytes: usize = cars.iter().map(|c| c.len()).sum();
    let record_bytes: usize =
        accts.iter().map(|a| a.repo.live.values().map(|c| a.blocks[c].len()).sum::<usize>()).sum();
    let (st, a, id) = store(true).await;
    for acct in &accts {
        id.set(&acct.repo.did, "pds.a", 1);
    }

    // bootstrap: check and import, 8 at a time
    let (t0, c0) = (std::time::Instant::now(), cpu_us());
    let mut work = accts.iter().map(|a| (a.repo.did.clone(), a.repo.signer.public())).zip(cars.iter().cloned());
    let mut set = tokio::task::JoinSet::new();
    loop {
        while set.len() < 8 {
            let Some(((did, key), car)) = work.next() else { break };
            set.spawn(bootstrap_one(a.clone(), st.clone(), did, key, car));
        }
        if set.join_next().await.is_none() {
            break;
        }
    }
    let (dt, dc) = (t0.elapsed().as_secs_f64(), cpu_us() - c0);
    println!(
        "bootstrap: {n} repos x {recs} records, {:.1} MB of CAR: {:.0} repos/s, {:.1} MB/s, {:.0} records/s, {:.2} ms CPU per repo",
        car_bytes as f64 / 1e6,
        n as f64 / dt,
        car_bytes as f64 / 1e6 / dt,
        (n * recs) as f64 / dt,
        dc as f64 / 1e3 / n as f64
    );
    let mut sst = 0;
    for s in st.shards() {
        s.flush_memtable().await.unwrap();
        sst += s.sst_bytes();
    }
    let total = (n * recs) as f64;
    println!(
        "bucket: {:.1} MB of SSTs for {} records ({:.0} B per record block): {:.0} B/record, {:.0} B/record over the block itself",
        sst as f64 / 1e6,
        n * recs,
        record_bytes as f64 / total,
        sst as f64 / total,
        (sst as f64 - record_bytes as f64) / total
    );

    // the same commits through a store with archival on and one with it off
    let mut evs = Vec::new();
    for _ in 0..commits {
        for acct in accts.iter_mut() {
            let ops = acct.repo.mixed_ops(1);
            let (f, c) = acct.commit(&ops);
            evs.push((acct.repo.did.clone(), f, c));
        }
    }
    let (off, _, id_off) = store(false).await;
    for acct in &accts {
        id_off.set(&acct.repo.did, "pds.a", 1);
    }
    // the commits go round the accounts, so "cold" (no idle trees kept)
    // reopens every tree and "warm" finds each one kept from its last commit
    let half = evs.len() / 2;
    let runs: [(&str, &Arc<StateStore>, &[_], usize); 3] =
        [("off", &off, &evs[..], 4096), ("on, cold", &st, &evs[..half], 0), ("on, warm", &st, &evs[half..], 4096)];
    for (label, s, evs, idle) in runs {
        mirror::IDLE_TREES.store(idle, Relaxed);
        let (t0, c0) = (std::time::Instant::now(), cpu_us());
        let mut tickets = Vec::new();
        for (did, f, c) in evs {
            tickets.push(apply(s, did, f, *c).await);
            if tickets.len() == 64 {
                s.commit(&std::mem::take(&mut tickets)).await.unwrap();
            }
        }
        s.commit(&tickets).await.unwrap();
        let (dt, dc) = (t0.elapsed().as_secs_f64(), cpu_us() - c0);
        println!(
            "apply, archival {label}: {} commits, {:.1} us CPU and {:.1} us wall per commit",
            evs.len(),
            dc as f64 / evs.len() as f64,
            dt * 1e6 / evs.len() as f64
        );
    }
    assert_eq!(a.stats.mismatches.load(Relaxed), 0);
    assert_eq!(a.stats.applied.load(Relaxed) as usize, evs.len());

    // getRepo: every repo streamed from storage, one at a time
    let (t0, c0) = (std::time::Instant::now(), cpu_us());
    let mut bytes = 0;
    for acct in &accts {
        let s = st.shard_for(&acct.repo.did).unwrap();
        let (g, head) = mirror::live(&s.db, &acct.repo.did).await.unwrap().unwrap();
        bytes += export(&s, &acct.repo.did, g, head).await.len();
    }
    let (dt, dc) = (t0.elapsed().as_secs_f64(), cpu_us() - c0);
    println!(
        "getRepo: {n} repos, {:.1} MB: {:.0} repos/s, {:.1} MB/s, {:.2} ms CPU per repo",
        bytes as f64 / 1e6,
        n as f64 / dt,
        bytes as f64 / 1e6 / dt,
        dc as f64 / 1e3 / n as f64
    );
}
