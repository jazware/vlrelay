use super::*;
use crate::types::Host;
use bytes::Bytes;
use std::sync::atomic::{AtomicU32, Ordering::Relaxed};
use vlsync_atproto::cid::Cid;
use vlsync_atproto::tid::Tid;
use vlsync_store::store::Store;

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

/// A store serving one view over an in-memory database, as a leader's term
/// does over the quorum state's.
pub(crate) async fn open(_shards: u32, id: Arc<MapIdentity>, config: ApplyConfig) -> Arc<StateStore> {
    let st = Arc::new(StateStore::new(StubChain, id, config));
    attach_memory_shard(&st).await;
    st
}

pub(crate) async fn attach_memory_shard<C: Chain>(st: &StateStore<C>) {
    let store = Store::memory(None);
    let db = slatedb::Db::builder("state", store.raw.clone()).build().await.unwrap();
    st.attach_shard(Arc::new(ShardState::new(
        ShardId(0),
        0,
        vlsync_store::slots::SLOTS,
        Arc::new(db),
        st.config.cache_entries_per_shard,
    )));
}

/// What the quorum log's applier does once an entry commits: the record its
/// meta carried goes to the database, and the staged one is released.
pub(crate) async fn persist(st: &StateStore, accepted: &[&Accepted]) {
    let s = st.shard_for("").unwrap();
    for a in accepted {
        s.db.put(record::did_key(&a.delta.did), a.record.encode()).await.unwrap();
    }
    s.release(accepted.iter().map(|a| a.ticket.n));
}

/// An operator's takedown or its lifting, as the quorum leader stages it
/// (lifting it also lifts a relay throttle).
async fn set_relay_takedown(st: &StateStore, did: &str, takedown: bool) -> Option<AccountStatus> {
    let s = st.shard_for(did).unwrap();
    let _g = s.lock_did(did).await;
    let mut rec = (*s.load(did).await.unwrap()?).clone();
    rec.relay_takedown = takedown;
    if !takedown {
        rec.relay_throttled = false;
    }
    let st = rec.status();
    s.stage_unlogged(did, rec);
    Some(st)
}

fn accepted(a: &Applied) -> &Accepted {
    match a {
        Applied::Append(a) => a,
        a => panic!("{a:?}"),
    }
}

#[derive(Default)]
pub(crate) struct MemHosts(Mutex<HashMap<String, HostRecord>>);

#[async_trait::async_trait]
impl HostStore for MemHosts {
    async fn get_host(&self, hostname: &str) -> anyhow::Result<Option<HostRecord>> {
        Ok(self.0.lock().get(hostname).cloned())
    }
    async fn put_host(&self, rec: &HostRecord) -> anyhow::Result<()> {
        self.0.lock().insert(rec.hostname.clone(), rec.clone());
        Ok(())
    }
    async fn checkpoint_cursors(&self, _: &[(String, i64)]) -> anyhow::Result<()> {
        Ok(())
    }
    async fn add_counts(&self, counts: &[(String, HostCounts)]) -> anyhow::Result<()> {
        let mut m = self.0.lock();
        for (h, c) in counts {
            let r = m.entry(h.clone()).or_insert_with(|| HostRecord::new(h, Tier::New, 0));
            r.account_count += c.accounts;
            r.events += c.events;
            r.failed_checks += c.failed_checks;
            r.dropped += c.dropped;
        }
        Ok(())
    }
    async fn list_hosts(&self, _: Option<&str>, _: usize) -> anyhow::Result<HostPage> {
        Ok(HostPage { hosts: self.0.lock().values().cloned().collect(), cursor: None })
    }
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
        since: (n > 0).then(|| rev(n - 1)),
    }
}

const NOW: u32 = 1_800_000_000;

fn host(h: &str) -> Host {
    Host(h.into())
}

async fn commit(st: &StateStore, did: &str, h: &Host, c: CommitClaim, now: u32) -> Result<Applied, Reject> {
    st.apply(Incoming { did, host: h, now, kind: EventKind::Commit(c) }).await
}

async fn db_record(st: &StateStore, did: &str) -> Option<Record> {
    let s = st.shard_for(did).unwrap();
    s.db.get(record::did_key(did)).await.unwrap().map(|b| Record::decode(&b).unwrap())
}

#[test]
fn did_keys_round_trip() {
    for did in [
        plc(1),
        plc(2),
        "did:web:example.com".into(),
        "did:plc:short".into(),
        "did:plc:ABCDEFGHIJKLMNOPQRSTUVWX".into(),
    ] {
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
    persist(&st, &[accepted(&a)]).await;
    let r = db_record(&st, &did).await.unwrap();
    assert_eq!(r.chain.unwrap().commit, claim(&did, 1).commit);
    assert_eq!(r.created_at, NOW);
    assert_eq!(r.pds, Some(HostKey::of("pds.a")));
    assert_eq!(st.shard_for(&did).unwrap().pending_len(), (0, 0));
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
    persist(&st, &[accepted(&a)]).await;

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
    let hosts = MemHosts::default();
    st.flush_host_counts(&hosts).await.unwrap();
    assert_eq!(hosts.get_host("pds.a").await.unwrap().unwrap().account_count, 0);
    assert_eq!(hosts.get_host("pds.b").await.unwrap().unwrap().account_count, 1);
    assert_eq!(hosts.get_host("pds.b").await.unwrap().unwrap().events, 1);
    assert_eq!(hosts.get_host("pds.a").await.unwrap().unwrap().failed_checks, 1);
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
    st.apply(Incoming { did, host: h, now: NOW, kind: EventKind::Account { active, status: status.map(String::from) } })
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
    assert_eq!(set_relay_takedown(&st, &did, true).await, Some(AccountStatus::Takendown));
    account(&st, &did, &h, true, None).await.unwrap();
    assert!(matches!(
        commit(&st, &did, &h, claim(&did, 4), NOW).await,
        Err(Reject::Inactive(AccountStatus::Takendown))
    ));
    set_relay_takedown(&st, &did, false).await;
    commit(&st, &did, &h, claim(&did, 4), NOW).await.unwrap();
}

#[tokio::test]
async fn broken_chain_desyncs_until_sync() {
    let id = MapIdentity::new();
    let did = plc(7);
    id.set(&did, "pds.a", 1);
    let st = open(2, id, ApplyConfig::default()).await;
    let h = host("pds.a");
    let a1 = commit(&st, &did, &h, claim(&did, 1), NOW).await.unwrap();
    persist(&st, &[accepted(&a1)]).await;
    // commit 3 arrives without 2: prevData mismatch
    let r = commit(&st, &did, &h, claim(&did, 3), NOW).await;
    assert!(matches!(r, Err(Reject::Chain(ChainError::PrevDataMismatch))), "{r:?}");
    let rec = st.get(&did).await.unwrap().unwrap();
    assert_eq!(rec.status(), AccountStatus::Desynchronized);
    assert_eq!(rec.chain.unwrap().rev, rev(1));
    let r = commit(&st, &did, &h, claim(&did, 4), NOW).await;
    assert!(matches!(r, Err(Reject::Desynchronized)), "{r:?}");
    // the desync mark has no log entry: it's the leader's memory only, never
    // the database (the log's applier is its only writer)
    st.shard_for(&did).unwrap().settle_unlogged_in_memory();
    assert_eq!(st.get(&did).await.unwrap().unwrap().status(), AccountStatus::Desynchronized);
    assert_eq!(db_record(&st, &did).await.unwrap().status(), AccountStatus::Active);

    // #sync resets the chain
    let c = claim(&did, 4);
    let a = st
        .apply(Incoming {
            did: &did,
            host: &h,
            now: NOW,
            kind: EventKind::Sync { rev: c.rev, commit: c.commit, data: c.data },
        })
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
    // #identity from a host the document doesn't name: emitted all the
    // same (the document is the authority), and the account stays with its
    // PDS. A brand new document isn't fetched again for it.
    id.set(&did, "pds.a", 3);
    let r = st.apply(Incoming { did: &did, host: &host("pds.z"), now: NOW + 2, kind: EventKind::Identity }).await;
    let Ok(Applied::Append(acc)) = r else { panic!("{r:?}") };
    assert!(!acc.key_changed);
    assert_eq!(acc.delta.host, HostKey::of("pds.a"));
    assert_eq!(id.fresh.load(Relaxed), 1);
    // an older one is
    let r = st.apply(Incoming { did: &did, host: &host("pds.z"), now: NOW + 40, kind: EventKind::Identity }).await;
    let Ok(Applied::Append(acc)) = r else { panic!("{r:?}") };
    assert!(acc.key_changed);
    assert_eq!(id.fresh.load(Relaxed), 2);
    let rec = st.get(&did).await.unwrap().unwrap();
    assert_eq!((rec.host, rec.key.as_ref().unwrap().0[2]), (HostKey::of("pds.a"), 3));
    // so is one for a DID nothing was heard from yet, but it creates no
    // account (its own PDS's first event does)
    let other = plc(19);
    id.set(&other, "pds.a", 1);
    let r = st.apply(Incoming { did: &other, host: &host("pds.z"), now: NOW + 3, kind: EventKind::Identity }).await;
    assert!(matches!(r, Ok(Applied::Pass)), "{r:?}");
    assert!(st.get(&other).await.unwrap().is_none());
    // commits from that host still aren't
    let r = commit(&st, &did, &host("pds.z"), claim(&did, 2), NOW + 4).await;
    assert!(matches!(r, Err(Reject::WrongHost { .. })), "{r:?}");
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
async fn only_committed_entries_reach_slatedb() {
    let id = MapIdentity::new();
    let did = plc(11);
    id.set(&did, "pds.a", 1);
    let st = open(1, id, ApplyConfig::default()).await;
    let h = host("pds.a");
    let a1 = commit(&st, &did, &h, claim(&did, 1), NOW).await.unwrap();
    let a2 = commit(&st, &did, &h, claim(&did, 2), NOW).await.unwrap();
    // an unlogged change after both
    assert!(commit(&st, &did, &h, claim(&did, 9), NOW).await.is_err());
    st.shard_for(&did).unwrap().settle_unlogged_in_memory();
    assert!(db_record(&st, &did).await.is_none());
    persist(&st, &[accepted(&a1)]).await;
    assert_eq!(db_record(&st, &did).await.unwrap().chain.unwrap().rev, rev(1));
    assert_eq!(st.get(&did).await.unwrap().unwrap().chain.unwrap().rev, rev(2));
    persist(&st, &[accepted(&a2)]).await;
    let r = db_record(&st, &did).await.unwrap();
    assert_eq!(r.chain.unwrap().rev, rev(2));
    // the database has the entry's record; the desync mark stays in memory
    assert_eq!(r.status(), AccountStatus::Active);
    assert_eq!(st.get(&did).await.unwrap().unwrap().status(), AccountStatus::Desynchronized);
    assert_eq!(st.shard_for(&did).unwrap().pending_len(), (0, 0));
}

#[tokio::test]
async fn list_repos_pages_in_key_order() {
    let id = MapIdentity::new();
    let st = open(4, id.clone(), ApplyConfig::default()).await;
    let h = host("pds.a");
    let mut want = Vec::new();
    let mut applied = Vec::new();
    for n in 0..200u64 {
        let d = plc(1000 + n);
        id.set(&d, "pds.a", 1);
        if n % 10 == 0 {
            // identity only: no head, so not listed
            applied.push(st.apply(Incoming { did: &d, host: &h, now: NOW, kind: EventKind::Identity }).await.unwrap());
            continue;
        }
        applied.push(commit(&st, &d, &h, claim(&d, 1), NOW).await.unwrap());
        want.push(d);
    }
    let mut taken_rec = None;
    let taken = want[0].clone();
    let mut acc: Vec<&Accepted> = applied.iter().map(accepted).collect();
    for a in &mut acc {
        if a.delta.did == taken {
            let mut r = (*a.record).clone();
            r.relay_takedown = true;
            taken_rec = Some(r);
        }
    }
    persist(&st, &acc).await;
    let s = st.shard_for("").unwrap();
    s.db.put(record::did_key(&taken), taken_rec.unwrap().encode()).await.unwrap();
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

struct CapGate(AtomicU32);

impl AccountGate for CapGate {
    fn admit_account(&self, _host: &str, did: &str, how: Arrival) -> NewAccount {
        if did == plc(9) {
            return NewAccount::Defer;
        }
        if matches!(how, Arrival::FirstCommit { .. }) {
            return NewAccount::Admit;
        }
        if self.0.fetch_add(1, Relaxed) < 1 { NewAccount::Admit } else { NewAccount::Throttle }
    }
}

#[tokio::test]
async fn accounts_past_the_gate_are_created_throttled() {
    let id = MapIdentity::new();
    let st = open(1, id.clone(), ApplyConfig::default()).await;
    let gate = Arc::new(CapGate(AtomicU32::new(0)));
    st.set_account_gate(gate.clone());
    let h = host("pds.example");
    let (a, b) = (plc(1), plc(2));
    id.set(&a, "pds.example", 1);
    id.set(&b, "pds.example", 2);
    // the first is admitted, the second is over the cap
    commit(&st, &a, &h, claim(&a, 1), NOW).await.unwrap();
    let r = commit(&st, &b, &h, claim(&b, 1), NOW).await;
    assert!(matches!(r, Err(Reject::Inactive(AccountStatus::Throttled))), "{r:?}");
    let rec = st.get(&b).await.unwrap().unwrap();
    assert!(rec.relay_throttled && rec.drops_commits());
    // kept: the next event isn't a new account, so the gate isn't asked again
    let r = commit(&st, &b, &h, claim(&b, 2), NOW).await;
    assert!(matches!(r, Err(Reject::Inactive(AccountStatus::Throttled))), "{r:?}");
    assert_eq!(gate.0.load(Relaxed), 2);
    // an upstream #account doesn't lift it; an operator's untakedown does
    account(&st, &b, &h, true, None).await.unwrap();
    assert_eq!(st.get(&b).await.unwrap().unwrap().status(), AccountStatus::Throttled);
    set_relay_takedown(&st, &b, false).await;
    commit(&st, &b, &h, claim(&b, 3), NOW).await.unwrap();
    // a deferred account isn't created: its next event asks again
    let d = plc(9);
    id.set(&d, "pds.example", 9);
    let r = commit(&st, &d, &h, claim(&d, 1), NOW).await;
    assert!(matches!(r, Err(Reject::NewAccountDeferred)), "{r:?}");
    assert!(st.get(&d).await.unwrap().is_none());
    // the flag survives the record's encoding
    let mut r = Record::new(HostKey::of("pds.example"), NOW);
    r.relay_throttled = true;
    assert_eq!(Record::decode(&r.encode()).unwrap(), r);
}

#[derive(Default)]
struct LogGate(Mutex<Vec<(String, Arrival)>>);

impl AccountGate for LogGate {
    fn admit_account(&self, _host: &str, did: &str, how: Arrival) -> NewAccount {
        self.0.lock().push((did.to_string(), how));
        NewAccount::Admit
    }
}

#[tokio::test]
async fn only_a_repos_first_commit_is_a_creation() {
    let id = MapIdentity::new();
    let st = open(1, id.clone(), ApplyConfig::default()).await;
    let gate = Arc::new(LogGate::default());
    st.set_account_gate(gate.clone());
    let h = host("pds.example");
    let dids: Vec<String> = (1..=4).map(plc).collect();
    for (i, d) in dids.iter().enumerate() {
        id.set(d, "pds.example", i as u8);
    }
    let ident = |d: &str| {
        let (st, h) = (st.clone(), h.clone());
        let d = d.to_string();
        async move { st.apply(Incoming { did: &d, host: &h, now: NOW, kind: EventKind::Identity }).await }
    };
    // an established repo's commit (prevData and since set), then a brand-new repo's
    commit(&st, &dids[0], &h, claim(&dids[0], 7), NOW).await.unwrap();
    commit(&st, &dids[1], &h, claim(&dids[1], 0), NOW).await.unwrap();
    // #identity first, the way a PDS announces a new account, then the first commit
    ident(&dids[2]).await.unwrap();
    commit(&st, &dids[2], &h, claim(&dids[2], 0), NOW).await.unwrap();
    ident(&dids[3]).await.unwrap();
    commit(&st, &dids[3], &h, claim(&dids[3], 5), NOW).await.unwrap();
    // later commits don't ask
    for (d, n) in dids.iter().zip([8, 1, 1, 6]) {
        commit(&st, d, &h, claim(d, n), NOW).await.unwrap();
    }
    use Arrival::*;
    assert_eq!(
        *gate.0.lock(),
        vec![
            (dids[0].clone(), FirstSeen),
            (dids[1].clone(), Created),
            (dids[2].clone(), FirstSeen),
            (dids[2].clone(), FirstCommit { created: true }),
            (dids[3].clone(), FirstSeen),
            (dids[3].clone(), FirstCommit { created: false }),
        ]
    );
}

/// No lookup budget left for the host: fresh fetches its events ask for
/// fall back to the cached document.
#[tokio::test]
async fn fresh_lookups_spend_the_senders_budget() {
    struct NoBudget;
    impl AccountGate for NoBudget {
        fn admit_account(&self, _host: &str, _did: &str, _how: Arrival) -> NewAccount {
            NewAccount::Admit
        }
        fn forced_lookup(&self, _host: &str) -> bool {
            false
        }
    }
    let id = MapIdentity::new();
    let did = plc(30);
    id.set(&did, "pds.a", 1);
    let st = open(1, id.clone(), ApplyConfig::default()).await;
    st.set_account_gate(Arc::new(NoBudget));
    let h = host("pds.a");
    commit(&st, &did, &h, claim(&did, 1), NOW).await.unwrap();
    for (i, sender) in ["pds.a", "pds.z", "pds.a", "pds.z"].into_iter().enumerate() {
        let now = NOW + 100 * (i as u32 + 1);
        let r = st.apply(Incoming { did: &did, host: &host(sender), now, kind: EventKind::Identity }).await;
        assert!(matches!(r, Ok(Applied::Append(_))), "{r:?}");
    }
    // nor does a commit from a host the document doesn't name
    let r = commit(&st, &did, &host("pds.z"), claim(&did, 2), NOW + 1000).await;
    assert!(matches!(r, Err(Reject::WrongHost { .. })), "{r:?}");
    assert_eq!(id.fresh.load(Relaxed), 0);
}

/// Another host's #identity for an unknown DID must not create the account
/// under the PDS its document names: that skipped the PDS's cap, and its
/// first commit then passed as `FirstCommit { created: false }`, which the
/// cap doesn't check.
#[tokio::test]
async fn foreign_identity_creates_no_account() {
    let id = MapIdentity::new();
    let st = open(1, id.clone(), ApplyConfig::default()).await;
    let gate = Arc::new(LogGate::default());
    st.set_account_gate(gate.clone());
    let (victim, other) = (host("victim.example"), host("relayer.example"));
    let hosts = MemHosts::default();
    hosts.put_host(&HostRecord::new("victim.example", Tier::Default, NOW)).await.unwrap();
    let dids: Vec<String> = (40..43).map(plc).collect();
    for d in &dids {
        id.set(d, "victim.example", 1);
        let r = st.apply(Incoming { did: d, host: &other, now: NOW, kind: EventKind::Identity }).await;
        assert!(matches!(r, Ok(Applied::Pass)), "{r:?}");
        assert!(st.get(d).await.unwrap().is_none());
    }
    assert!(gate.0.lock().is_empty());
    st.flush_host_counts(&hosts).await.unwrap();
    assert_eq!(hosts.get_host("victim.example").await.unwrap().unwrap().account_count, 0);
    // the account's own PDS meets the gate as for any unknown account
    commit(&st, &dids[0], &victim, claim(&dids[0], 3), NOW).await.unwrap();
    assert_eq!(*gate.0.lock(), vec![(dids[0].clone(), Arrival::FirstSeen)]);
}
