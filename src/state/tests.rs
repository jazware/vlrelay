use super::*;
use crate::types::Host;
use bytes::Bytes;
use std::sync::atomic::{AtomicU32, Ordering::Relaxed};
use vlpds::cid::Cid;
use vlpds::slots::Layout;
use vlpds::tid::Tid;

pub(crate) struct MapIdentity {
    pub docs: Mutex<HashMap<String, Identity>>,
    pub fresh: AtomicU32,
    pub cached: AtomicU32,
}

impl MapIdentity {
    pub fn new() -> Arc<MapIdentity> {
        Arc::new(MapIdentity { docs: Default::default(), fresh: AtomicU32::new(0), cached: AtomicU32::new(0) })
    }
    pub fn set(&self, did: &str, pds: &str, key: u8) {
        self.docs.lock().insert(
            did.to_string(),
            Identity { pds: Some(Host(pds.into())), signing_key: Some(SigningKey(Bytes::from(vec![0xe7, 0x01, key]))) },
        );
    }
    pub fn lookups(&self) -> u32 {
        self.fresh.load(Relaxed) + self.cached.load(Relaxed)
    }
}

#[async_trait::async_trait]
impl IdentitySource for MapIdentity {
    async fn resolve(&self, did: &str, fresh: bool) -> Result<Option<Identity>, IdentityError> {
        if fresh { &self.fresh } else { &self.cached }.fetch_add(1, Relaxed);
        Ok(self.docs.lock().get(did).cloned())
    }
}

pub(crate) async fn open(shards: u32, id: Arc<MapIdentity>, config: ApplyConfig) -> Arc<StateStore> {
    let store = Store::memory(None);
    let st = Arc::new(StateStore::new(store, Layout::uniform(shards).shards, StubChain, id, config));
    for s in Layout::uniform(shards).shards {
        st.open_shard(s.id, None).await.unwrap();
    }
    st
}

pub(crate) fn plc(n: u64) -> String {
    const B32: &[u8] = b"abcdefghijklmnopqrstuvwxyz234567";
    let mut x = n.wrapping_mul(0x9e3779b97f4a7c15) as u128 | ((n as u128) << 64);
    let mut s = String::from("did:plc:");
    for _ in 0..24 {
        s.push(B32[(x & 31) as usize] as char);
        x >>= 5;
    }
    s
}

pub(crate) fn cid(tag: &str) -> Cid {
    Cid::dag_cbor(tag.as_bytes())
}

pub(crate) fn rev(n: u64) -> Tid {
    Tid::from_parts(1_700_000_000_000_000 + n, 0)
}

/// Commit `n` of a repo: data `d{n}`, prevData `d{n-1}`.
pub(crate) fn claim(did: &str, n: u64) -> CommitClaim {
    CommitClaim {
        rev: rev(n),
        commit: cid(&format!("{did}c{n}")),
        data: cid(&format!("{did}d{n}")),
        prev_data: (n > 0).then(|| cid(&format!("{did}d{}", n - 1))),
    }
}

const NOW: u32 = 1_800_000_000;

fn host(h: &str) -> Host {
    Host(h.into())
}

async fn commit(st: &StateStore, did: &str, h: &Host, c: CommitClaim, now: u32) -> Result<Applied, Reject> {
    st.apply(Incoming { did, host: h, now, kind: EventKind::Commit(c) }).await
}

fn ticket(a: &Applied) -> Ticket {
    match a {
        Applied::Append(a) => a.ticket,
        Applied::Duplicate => panic!("duplicate"),
    }
}

async fn db_record(st: &StateStore, did: &str) -> Option<Record> {
    let s = st.shard_for(did).unwrap();
    s.db.get(record::did_key(did)).await.unwrap().map(|b| Record::decode(&b).unwrap())
}

#[test]
fn did_keys_round_trip() {
    for did in [plc(1), plc(2), "did:web:example.com".into(), "did:plc:short".into(), "did:plc:ABCDEFGHIJKLMNOPQRSTUVWX".into()] {
        let k = record::did_key(&did);
        assert_eq!(record::did_from_key(&k).as_deref(), Some(did.as_str()), "{did}");
    }
    assert_eq!(record::did_key(&plc(1)).len(), 3 + 2 + 15);
}

#[test]
fn record_round_trips_compactly() {
    let mut r = Record::new(HostKey::of("pds.example"), NOW);
    r.pds = Some(r.host);
    r.chain = Some(ChainState { rev: rev(5), commit: cid("c"), data: cid("d") });
    r.key = Some(SigningKey(Bytes::from(vec![7u8; 35])));
    r.fetched_at = NOW;
    r.minute = NOW / 60;
    r.minute_commits = 3;
    let enc = r.encode();
    assert_eq!(Record::decode(&enc).unwrap(), r);
    // 4 header + 8 host + 72 chain + 36 key + ~16 varints
    assert!(enc.len() <= 140, "{}", enc.len());

    let mut r2 = r.clone();
    r2.pds = Some(HostKey::of("other"));
    r2.upstream = Upstream::Deactivated;
    r2.relay_takedown = true;
    r2.desync = Some(record::DesyncReason::PrevDataMismatch);
    r2.chain = None;
    r2.key = None;
    assert_eq!(Record::decode(&r2.encode()).unwrap(), r2);
    assert!(Record::decode(&enc[..enc.len() - 1]).is_err());
}

#[test]
fn delta_round_trips() {
    let d = StateDelta {
        did: plc(9),
        host: HostKey::of("h"),
        kind: ChangeKind::Sync,
        chain: Some(ChainState { rev: rev(1), commit: cid("a"), data: cid("b") }),
        upstream: Upstream::Throttled,
    };
    assert_eq!(StateDelta::decode(&d.encode()).unwrap(), d);
}

#[tokio::test]
async fn first_commit_creates_and_commit_persists() {
    let id = MapIdentity::new();
    let did = plc(1);
    id.set(&did, "pds.a", 1);
    let st = open(4, id.clone(), ApplyConfig::default()).await;
    let a = commit(&st, &did, &host("pds.a"), claim(&did, 1), NOW).await.unwrap();
    let Applied::Append(acc) = &a else { panic!() };
    assert!(acc.new_account);
    assert_eq!(acc.status, AccountStatus::Active);
    assert_eq!(acc.delta.chain.unwrap().rev, rev(1));
    // staged, not yet in SlateDB
    assert!(db_record(&st, &did).await.is_none());
    assert_eq!(st.get(&did).await.unwrap().unwrap().chain.unwrap().rev, rev(1));
    assert_eq!(st.commit(&[ticket(&a)]).await.unwrap(), 1);
    let r = db_record(&st, &did).await.unwrap();
    assert_eq!(r.chain.unwrap().commit, claim(&did, 1).commit);
    assert_eq!(r.created_at, NOW);
    assert_eq!(r.pds, Some(HostKey::of("pds.a")));
    assert_eq!(st.shard_for(&did).unwrap().pending_len(), (0, 0));
    // committing again is a no-op
    assert_eq!(st.commit(&[ticket(&a)]).await.unwrap(), 0);
    assert_eq!(id.lookups(), 1);
}

#[tokio::test]
async fn duplicates_ack_and_stale_revs_drop() {
    let id = MapIdentity::new();
    let did = plc(2);
    id.set(&did, "pds.a", 1);
    let st = open(2, id.clone(), ApplyConfig::default()).await;
    let h = host("pds.a");
    for n in 1..=3 {
        commit(&st, &did, &h, claim(&did, n), NOW).await.unwrap();
    }
    assert!(matches!(commit(&st, &did, &h, claim(&did, 3), NOW).await, Ok(Applied::Duplicate)));
    assert!(matches!(commit(&st, &did, &h, claim(&did, 2), NOW).await, Err(Reject::Stale { .. })));
    // a different commit at the current rev is stale too, not a desync
    let mut c = claim(&did, 3);
    c.commit = cid("other");
    assert!(matches!(commit(&st, &did, &h, c, NOW).await, Err(Reject::Stale { .. })));
    assert_eq!(st.get(&did).await.unwrap().unwrap().status(), AccountStatus::Active);
    // a duplicate never needs a lookup, even from the wrong host
    assert!(matches!(commit(&st, &did, &host("evil"), claim(&did, 3), NOW).await, Ok(Applied::Duplicate)));
    assert_eq!(id.lookups(), 1);
}

#[tokio::test]
async fn wrong_host_reresolves_once_then_follows_migration() {
    let id = MapIdentity::new();
    let did = plc(3);
    id.set(&did, "pds.a", 1);
    let st = open(2, id.clone(), ApplyConfig { reresolve_after_secs: 30, ..Default::default() }).await;
    let a = commit(&st, &did, &host("pds.a"), claim(&did, 1), NOW).await.unwrap();
    st.commit(&[ticket(&a)]).await.unwrap();

    // a stranger, within the re-resolve window: rejected with no lookup
    let r = commit(&st, &did, &host("pds.b"), claim(&did, 2), NOW + 5).await;
    assert!(matches!(r, Err(Reject::WrongHost { .. })), "{r:?}");
    assert_eq!(id.lookups(), 1);
    assert_eq!(st.get(&did).await.unwrap().unwrap().failed_checks, 1);

    // the account moves; past the window the mismatch re-resolves
    id.set(&did, "pds.b", 2);
    let a = commit(&st, &did, &host("pds.b"), claim(&did, 2), NOW + 60).await.unwrap();
    let Applied::Append(acc) = &a else { panic!() };
    assert!(acc.key_changed);
    assert_eq!(id.fresh.load(Relaxed), 1);
    let r = st.get(&did).await.unwrap().unwrap();
    assert_eq!(r.host, HostKey::of("pds.b"));
    // and the old host is now the stranger
    let r = commit(&st, &did, &host("pds.a"), claim(&did, 3), NOW + 61).await;
    assert!(matches!(r, Err(Reject::WrongHost { .. })));

    // host account counts followed the move
    st.flush_host_counts(&*st).await.unwrap();
    assert_eq!(st.get_host("pds.a").await.unwrap().unwrap().account_count, 0);
    assert_eq!(st.get_host("pds.b").await.unwrap().unwrap().account_count, 1);
    assert_eq!(st.get_host("pds.b").await.unwrap().unwrap().events, 1);
    assert_eq!(st.get_host("pds.a").await.unwrap().unwrap().failed_checks, 1);
}

#[tokio::test]
async fn new_did_from_wrong_host_tries_fresh_and_creates_nothing() {
    let id = MapIdentity::new();
    let did = plc(4);
    id.set(&did, "pds.a", 1);
    let st = open(2, id.clone(), ApplyConfig::default()).await;
    let r = commit(&st, &did, &host("pds.x"), claim(&did, 1), NOW).await;
    assert!(matches!(r, Err(Reject::WrongHost { .. })));
    assert_eq!((id.cached.load(Relaxed), id.fresh.load(Relaxed)), (1, 1));
    assert!(st.get(&did).await.unwrap().is_none());
    let unknown = plc(5);
    assert!(matches!(commit(&st, &unknown, &host("pds.a"), claim(&unknown, 1), NOW).await, Err(Reject::NoIdentity)));
}

async fn account(st: &StateStore, did: &str, h: &Host, active: bool, status: Option<&str>) -> Result<Applied, Reject> {
    st.apply(Incoming {
        did,
        host: h,
        now: NOW,
        kind: EventKind::Account { active, status: status.map(String::from) },
    })
    .await
}

#[tokio::test]
async fn inactive_accounts_drop_commits() {
    let id = MapIdentity::new();
    let did = plc(6);
    id.set(&did, "pds.a", 1);
    let st = open(2, id, ApplyConfig::default()).await;
    let h = host("pds.a");
    commit(&st, &did, &h, claim(&did, 1), NOW).await.unwrap();
    for (status, want) in [
        ("takendown", AccountStatus::Takendown),
        ("deactivated", AccountStatus::Deactivated),
        ("suspended", AccountStatus::Suspended),
        ("deleted", AccountStatus::Deleted),
    ] {
        let a = account(&st, &did, &h, false, Some(status)).await.unwrap();
        let Applied::Append(acc) = a else { panic!() };
        assert_eq!(acc.status, want);
        let r = commit(&st, &did, &h, claim(&did, 2), NOW).await;
        assert!(matches!(r, Err(Reject::Inactive(s)) if s == want), "{status}: {r:?}");
    }
    // unknown inactive status: no claim, still dropped
    account(&st, &did, &h, false, Some("weird")).await.unwrap();
    assert_eq!(st.get(&did).await.unwrap().unwrap().status(), AccountStatus::Inactive);
    // throttled hosts' accounts still flow
    account(&st, &did, &h, false, Some("throttled")).await.unwrap();
    commit(&st, &did, &h, claim(&did, 2), NOW).await.unwrap();
    let a = account(&st, &did, &h, true, None).await.unwrap();
    let Applied::Append(acc) = a else { panic!() };
    assert_eq!((acc.status, acc.status_was), (AccountStatus::Active, Some(AccountStatus::Throttled)));
    commit(&st, &did, &h, claim(&did, 3), NOW).await.unwrap();

    // a relay takedown outlives upstream "active"
    assert_eq!(st.set_relay_takedown(&did, true).await.unwrap(), Some(AccountStatus::Takendown));
    account(&st, &did, &h, true, None).await.unwrap();
    assert!(matches!(commit(&st, &did, &h, claim(&did, 4), NOW).await, Err(Reject::Inactive(AccountStatus::Takendown))));
    st.set_relay_takedown(&did, false).await.unwrap();
    commit(&st, &did, &h, claim(&did, 4), NOW).await.unwrap();
}

#[tokio::test]
async fn broken_chain_desyncs_until_sync() {
    let id = MapIdentity::new();
    let did = plc(7);
    id.set(&did, "pds.a", 1);
    let st = open(2, id, ApplyConfig::default()).await;
    let h = host("pds.a");
    let t1 = ticket(&commit(&st, &did, &h, claim(&did, 1), NOW).await.unwrap());
    st.commit(&[t1]).await.unwrap();
    // commit 3 arrives without 2: prevData mismatch
    let r = commit(&st, &did, &h, claim(&did, 3), NOW).await;
    assert!(matches!(r, Err(Reject::Chain(ChainError::PrevDataMismatch))), "{r:?}");
    let rec = st.get(&did).await.unwrap().unwrap();
    assert_eq!(rec.status(), AccountStatus::Desynchronized);
    assert_eq!(rec.chain.unwrap().rev, rev(1));
    let r = commit(&st, &did, &h, claim(&did, 4), NOW).await;
    assert!(matches!(r, Err(Reject::Desynchronized)), "{r:?}");
    // the desync mark is written without any log entry
    assert_eq!(st.shard_for(&did).unwrap().flush_unlogged().await.unwrap(), 1);
    assert_eq!(db_record(&st, &did).await.unwrap().status(), AccountStatus::Desynchronized);

    // #sync resets the chain
    let c = claim(&did, 4);
    let a = st
        .apply(Incoming { did: &did, host: &h, now: NOW, kind: EventKind::Sync { rev: c.rev, commit: c.commit, data: c.data } })
        .await
        .unwrap();
    let Applied::Append(acc) = a else { panic!() };
    assert_eq!((acc.status, acc.status_was), (AccountStatus::Active, Some(AccountStatus::Desynchronized)));
    commit(&st, &did, &h, claim(&did, 5), NOW).await.unwrap();
}

#[tokio::test]
async fn missed_commit_replayed_relinks_the_chain() {
    let id = MapIdentity::new();
    let did = plc(8);
    id.set(&did, "pds.a", 1);
    let st = open(2, id, ApplyConfig::default()).await;
    let h = host("pds.a");
    commit(&st, &did, &h, claim(&did, 1), NOW).await.unwrap();
    assert!(commit(&st, &did, &h, claim(&did, 3), NOW).await.is_err());
    // the PDS resends 2: it links to the stored data, so it's accepted
    commit(&st, &did, &h, claim(&did, 2), NOW).await.unwrap();
    assert_eq!(st.get(&did).await.unwrap().unwrap().status(), AccountStatus::Active);
    commit(&st, &did, &h, claim(&did, 3), NOW).await.unwrap();
}

#[tokio::test]
async fn identity_event_refreshes_the_key() {
    let id = MapIdentity::new();
    let did = plc(9);
    id.set(&did, "pds.a", 1);
    let st = open(2, id.clone(), ApplyConfig::default()).await;
    let h = host("pds.a");
    commit(&st, &did, &h, claim(&did, 1), NOW).await.unwrap();
    id.set(&did, "pds.a", 2);
    let a = st.apply(Incoming { did: &did, host: &h, now: NOW + 1, kind: EventKind::Identity }).await.unwrap();
    let Applied::Append(acc) = a else { panic!() };
    assert!(acc.key_changed);
    assert_eq!(acc.delta.kind, ChangeKind::Identity);
    assert_eq!(id.fresh.load(Relaxed), 1);
    assert_eq!(st.get(&did).await.unwrap().unwrap().key.as_ref().unwrap().0[2], 2);
    // #identity from a host the fresh document doesn't name: rejected
    let r = st.apply(Incoming { did: &did, host: &host("pds.z"), now: NOW + 2, kind: EventKind::Identity }).await;
    assert!(matches!(r, Err(Reject::WrongHost { .. })));
}

#[tokio::test]
async fn per_minute_commit_limit() {
    let id = MapIdentity::new();
    let did = plc(10);
    id.set(&did, "pds.a", 1);
    let st = open(1, id, ApplyConfig { max_commits_per_minute: Some(3), ..Default::default() }).await;
    let h = host("pds.a");
    let t0 = (NOW / 60) * 60;
    for n in 1..=3 {
        commit(&st, &did, &h, claim(&did, n), t0 + n as u32).await.unwrap();
    }
    assert!(matches!(commit(&st, &did, &h, claim(&did, 4), t0 + 10).await, Err(Reject::RateLimited { limit: 3 })));
    commit(&st, &did, &h, claim(&did, 4), t0 + 60).await.unwrap();
    assert_eq!(st.get(&did).await.unwrap().unwrap().minute_commits, 1);
}

#[tokio::test]
async fn only_committed_tickets_reach_slatedb() {
    let id = MapIdentity::new();
    let did = plc(11);
    id.set(&did, "pds.a", 1);
    let st = open(1, id, ApplyConfig::default()).await;
    let h = host("pds.a");
    let t1 = ticket(&commit(&st, &did, &h, claim(&did, 1), NOW).await.unwrap());
    let t2 = ticket(&commit(&st, &did, &h, claim(&did, 2), NOW).await.unwrap());
    // an unlogged change after both: must not be written ahead of t2
    assert!(commit(&st, &did, &h, claim(&did, 9), NOW).await.is_err());
    st.shard_for(&did).unwrap().flush_unlogged().await.unwrap();
    assert!(db_record(&st, &did).await.is_none());
    st.commit(&[t1]).await.unwrap();
    assert_eq!(db_record(&st, &did).await.unwrap().chain.unwrap().rev, rev(1));
    assert_eq!(st.get(&did).await.unwrap().unwrap().chain.unwrap().rev, rev(2));
    st.commit(&[t2]).await.unwrap();
    let r = db_record(&st, &did).await.unwrap();
    assert_eq!(r.chain.unwrap().rev, rev(2));
    // the last record written carries the unlogged desync mark too
    assert_eq!(r.status(), AccountStatus::Desynchronized);
    assert_eq!(st.shard_for(&did).unwrap().pending_len(), (0, 0));
}

struct VecSource(Vec<(u64, Vec<StateDelta>)>);

#[async_trait::async_trait]
impl ReplaySource for VecSource {
    async fn tail(&self, _log: &str, _shard: ShardId, after: Option<u64>) -> anyhow::Result<Vec<(u64, Vec<StateDelta>)>> {
        Ok(self.0.iter().filter(|(o, _)| after.is_none_or(|a| *o > a)).cloned().collect())
    }
}

#[tokio::test]
async fn replay_rebuilds_state_idempotently() {
    let id = MapIdentity::new();
    let dids: Vec<String> = (20..30).map(plc).collect();
    for d in &dids {
        id.set(d, "pds.a", 1);
    }
    let st = open(1, id.clone(), ApplyConfig::default()).await;
    let h = host("pds.a");
    let mut log = Vec::new();
    for (i, d) in dids.iter().enumerate() {
        for n in 1..=3 {
            let Applied::Append(a) = commit(&st, d, &h, claim(d, n), NOW).await.unwrap() else { panic!() };
            log.push(((i * 3 + n as usize) as u64, vec![a.delta]));
        }
    }
    let Applied::Append(a) = account(&st, &dids[0], &h, false, Some("deactivated")).await.unwrap() else { panic!() };
    log.push((100, vec![a.delta]));
    // the node dies: nothing committed. A fresh store over the same bucket
    // replays the log.
    let store = st.store.clone();
    let id0 = st.shards()[0].id;
    st.close_shard(id0).await.unwrap();
    let st2 = Arc::new(StateStore::new(store, Layout::uniform(1).shards, StubChain, id.clone(), ApplyConfig::default()));
    st2.open_shard(id0, None).await.unwrap();
    let src = VecSource(log.clone());
    assert_eq!(st2.recover(id0, "node-a", &src, NOW).await.unwrap(), 10 * 3 + 1);
    let check = |st: Arc<StateStore>| {
        let dids = dids.clone();
        async move {
            for (i, d) in dids.iter().enumerate() {
                let r = db_record(&st, d).await.unwrap();
                assert_eq!(r.chain.unwrap().commit, claim(d, 3).commit);
                assert_eq!(r.status(), if i == 0 { AccountStatus::Deactivated } else { AccountStatus::Active });
            }
        }
    };
    check(st2.clone()).await;
    assert_eq!(st2.shard(id0).unwrap().applied_marker("node-a").await.unwrap(), Some(100));
    // again: the marker skips everything
    assert_eq!(st2.recover(id0, "node-a", &src, NOW).await.unwrap(), 0);
    // replaying the whole log over the state changes nothing
    assert_eq!(st2.replay(&log.iter().flat_map(|(_, d)| d.clone()).collect::<Vec<_>>(), NOW).await.unwrap(), 0);
    check(st2.clone()).await;
    // and the replayed head acks the PDS's replays as duplicates
    assert!(matches!(commit(&st2, &dids[1], &h, claim(&dids[1], 3), NOW).await, Ok(Applied::Duplicate)));
}

#[tokio::test]
async fn list_repos_pages_across_shards() {
    let id = MapIdentity::new();
    let st = open(4, id.clone(), ApplyConfig::default()).await;
    let h = host("pds.a");
    let mut want = Vec::new();
    let mut tickets = Vec::new();
    for n in 0..200u64 {
        let d = plc(1000 + n);
        id.set(&d, "pds.a", 1);
        if n % 10 == 0 {
            // identity only: no head, so not listed
            let a = st.apply(Incoming { did: &d, host: &h, now: NOW, kind: EventKind::Identity }).await.unwrap();
            tickets.push(ticket(&a));
            continue;
        }
        tickets.push(ticket(&commit(&st, &d, &h, claim(&d, 1), NOW).await.unwrap()));
        want.push(d);
    }
    let taken = want[0].clone();
    st.set_relay_takedown(&taken, true).await.unwrap();
    st.commit(&tickets).await.unwrap();
    let mut got = Vec::new();
    let mut cursor: Option<String> = None;
    let mut pages = 0;
    loop {
        let p = st.list_repos(cursor.as_deref(), 7).await.unwrap();
        pages += 1;
        assert!(p.repos.len() <= 7);
        got.extend(p.repos);
        match p.cursor {
            Some(c) => cursor = Some(c),
            None => break,
        }
    }
    assert_eq!(pages, 180usize.div_ceil(7) + (180 % 7 == 0) as usize);
    let mut names: Vec<_> = got.iter().map(|r| r.did.clone()).collect();
    let keys: Vec<_> = names.iter().map(|d| record::did_key(d)).collect();
    assert!(keys.windows(2).all(|w| w[0] < w[1]), "pages are in key order with no repeats");
    names.sort();
    want.sort();
    assert_eq!(names, want);
    let td = got.iter().find(|r| r.did == taken).unwrap();
    assert_eq!(td.status, AccountStatus::Takendown);
    // a big page in one go, and a cursor past the end
    assert_eq!(st.list_repos(None, 1000).await.unwrap().repos.len(), 180);
    let last = keys.last().map(|k| record::did_from_key(k).unwrap()).unwrap();
    let p = st.list_repos(Some(&last), 10).await.unwrap();
    assert!(p.repos.is_empty() && p.cursor.is_none());
}

#[tokio::test]
async fn host_records() {
    let st = open(4, MapIdentity::new(), ApplyConfig::default()).await;
    assert!(st.get_host("nope").await.unwrap().is_none());
    for i in 0..25 {
        st.put_host(&HostRecord::new(&format!("pds{i}.example"), Tier::Default, NOW)).await.unwrap();
    }
    st.checkpoint_cursors(&[("pds3.example".into(), 500), ("unknown.example".into(), 9)]).await.unwrap();
    st.checkpoint_cursors(&[("pds3.example".into(), 400)]).await.unwrap();
    assert_eq!(st.get_host("pds3.example").await.unwrap().unwrap().cursor, 500);
    assert!(st.get_host("unknown.example").await.unwrap().is_none());
    let mut seen = Vec::new();
    let mut cursor = None;
    loop {
        let p = HostStore::list_hosts(&*st, cursor.as_deref(), 4).await.unwrap();
        seen.extend(p.hosts.into_iter().map(|h| h.hostname));
        match p.cursor {
            Some(c) => cursor = Some(c),
            None => break,
        }
    }
    seen.sort();
    seen.dedup();
    assert_eq!(seen.len(), 25);
    let mut r = st.get_host("pds4.example").await.unwrap().unwrap();
    r.tier = Tier::Banned;
    st.put_host(&r).await.unwrap();
    assert_eq!(st.get_host("pds4.example").await.unwrap().unwrap().lexicon_status(), "banned");
    // host names survive a reopen (they come back from the host rows)
    let sid = st.shard_for("pds4.example").unwrap().id;
    st.close_shard(sid).await.unwrap();
    st.open_shard(sid, None).await.unwrap();
    assert!(st.host_name(HostKey::of("pds4.example")).is_some());
}

#[tokio::test]
async fn concurrent_applies_for_one_did_stay_ordered() {
    let id = MapIdentity::new();
    let did = plc(42);
    id.set(&did, "pds.a", 1);
    let st = open(1, id, ApplyConfig::default()).await;
    let h = host("pds.a");
    // the same three events delivered twice concurrently (a takeover replay)
    let mut tasks = Vec::new();
    for _ in 0..2 {
        let (st, did, h) = (st.clone(), did.clone(), h.clone());
        tasks.push(tokio::spawn(async move {
            let mut out = Vec::new();
            for n in 1..=3 {
                out.push(commit(&st, &did, &h, claim(&did, n), NOW).await);
            }
            out
        }));
    }
    let mut appended = 0;
    for t in tasks {
        for r in t.await.unwrap() {
            if let Ok(Applied::Append(_)) = r {
                appended += 1;
            }
        }
    }
    assert_eq!(appended, 3);
    assert_eq!(st.get(&did).await.unwrap().unwrap().chain.unwrap().rev, rev(3));
}
