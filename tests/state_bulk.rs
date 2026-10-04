//! Bulk numbers for the per-DID state: bytes per DID in the bucket, apply
//! throughput, listRepos page latency. Ignored by default; run with
//!
//! ```text
//! BULK_DIDS=1000000 BULK_SHARDS=8 cargo test --release --test state_bulk -- --ignored --nocapture
//! ```
//!
//! With `BULK_S3_ENDPOINT` (plus `BULK_S3_BUCKET`, `BULK_S3_KEY`,
//! `BULK_S3_SECRET`) it runs against S3/MinIO instead of memory.

use bytes::Bytes;
use sha2::{Digest, Sha256};
use std::sync::Arc;
use std::time::{Duration, Instant};
use vlpds::cid::Cid;
use vlpds::slots::Layout;
use vlpds::store::{S3Config, Store};
use vlpds::tid::Tid;
use vlrelay::state::*;
use vlrelay::types::Host;

fn env(k: &str, d: usize) -> usize {
    std::env::var(k).ok().and_then(|v| v.parse().ok()).unwrap_or(d)
}

fn did(n: u64) -> String {
    const B32: &[u8] = b"abcdefghijklmnopqrstuvwxyz234567";
    let h = Sha256::digest(n.to_be_bytes());
    let mut x = u128::from_be_bytes(h[..16].try_into().unwrap());
    let mut s = String::from("did:plc:");
    for _ in 0..24 {
        s.push(B32[(x & 31) as usize] as char);
        x >>= 5;
    }
    s
}

fn pds_of(did: &str) -> String {
    let h = Sha256::digest(did.as_bytes());
    format!("pds{}.us-east.host.bsky.network", h[0] % 100)
}

struct Docs;

#[async_trait::async_trait]
impl IdentitySource for Docs {
    async fn resolve(&self, did: &str, _fresh: bool) -> Result<Option<Identity>, IdentityError> {
        let h = Sha256::digest(did.as_bytes());
        let mut key = vec![0xe7, 0x01, 0x02];
        key.extend_from_slice(&h[..32]);
        Ok(Some(Identity { pds: Some(Host(pds_of(did))), signing_key: Some(SigningKey(Bytes::from(key))) }))
    }
}

fn claim(did: &str, n: u64) -> CommitClaim {
    let c = |t: &str| Cid::dag_cbor(format!("{did}{t}{n}").as_bytes());
    CommitClaim {
        rev: Tid::from_parts(1_760_000_000_000_000 + n * 1000, 0),
        commit: c("c"),
        data: c("d"),
        prev_data: (n > 0).then(|| Cid::dag_cbor(format!("{did}d{}", n - 1).as_bytes())),
        since: (n > 0).then(|| Tid::from_parts(1_760_000_000_000_000 + (n - 1) * 1000, 0)),
    }
}

fn pct(v: &mut [Duration], p: f64) -> Duration {
    v.sort();
    v[((v.len() - 1) as f64 * p) as usize]
}

/// Applies commit `round` for DIDs [lo, hi) of `ids`, committing tickets
/// every `batch` applies the way the log finalizer would per segment.
async fn run(st: Arc<StateStore>, ids: Arc<Vec<String>>, workers: usize, round: u64, batch: usize) -> (usize, Duration) {
    let t = Instant::now();
    let mut tasks = Vec::new();
    for w in 0..workers {
        let (st, ids) = (st.clone(), ids.clone());
        tasks.push(tokio::spawn(async move {
            let mut tickets = Vec::with_capacity(batch);
            let mut n = 0;
            for i in (w..ids.len()).step_by(workers) {
                let d = &ids[i];
                let h = Host(pds_of(d));
                match st.apply(Incoming { did: d, host: &h, now: 1_800_000_000, kind: EventKind::Commit(claim(d, round)) }).await {
                    Ok(Applied::Append(a)) => tickets.push(a.ticket),
                    r => panic!("{d}: {r:?}"),
                }
                n += 1;
                if tickets.len() >= batch {
                    st.commit(&tickets).await.unwrap();
                    tickets.clear();
                }
            }
            st.commit(&tickets).await.unwrap();
            n
        }));
    }
    let mut n = 0;
    for t in tasks {
        n += t.await.unwrap();
    }
    (n, t.elapsed())
}

#[tokio::test(flavor = "multi_thread")]
#[ignore]
async fn bulk() {
    let n = env("BULK_DIDS", 1_000_000);
    let shards = env("BULK_SHARDS", 8) as u32;
    let workers = env("BULK_WORKERS", 64);
    let batch = env("BULK_BATCH", 512);
    let cache = env("BULK_CACHE_PER_SHARD", 1 << 20);
    let store = match std::env::var("BULK_S3_ENDPOINT") {
        Ok(endpoint) => {
            let cfg = S3Config {
                endpoint,
                bucket: std::env::var("BULK_S3_BUCKET").unwrap_or("vlrelay".into()),
                access_key: std::env::var("BULK_S3_KEY").unwrap_or("minioadmin".into()),
                secret_key: std::env::var("BULK_S3_SECRET").unwrap_or("minioadmin".into()),
                region: "us-east-1".into(),
            };
            let prefix = format!("bulk-{}", std::process::id());
            Store::s3(&cfg, &prefix, None, 256).unwrap()
        }
        Err(_) => Store::memory(None),
    };
    let layout = Layout::uniform(shards).shards;
    let config = ApplyConfig { cache_entries_per_shard: cache, ..Default::default() };
    let st = Arc::new(StateStore::new(store.clone(), layout.clone(), StubChain, Arc::new(Docs), config));
    for s in &layout {
        st.open_shard(s.id, None).await.unwrap();
    }
    let ids: Arc<Vec<String>> = Arc::new((0..n as u64).map(did).collect());
    println!("bulk: {n} DIDs, {shards} shards, {workers} workers, commit every {batch}, cache {cache}/shard");

    let (k, el) = run(st.clone(), ids.clone(), workers, 1, batch).await;
    let rate = k as f64 / el.as_secs_f64();
    println!("create: {k} in {:.2}s = {:.0}/s node, {:.0}/s per shard", el.as_secs_f64(), rate, rate / shards as f64);
    // one version of every record in the SSTs: the per-DID cost
    for s in st.shards() {
        s.flush_memtable().await.unwrap();
    }
    let bytes: u64 = st.shards().iter().map(|s| s.sst_bytes()).sum();
    println!("bucket after create: {bytes} SST bytes = {:.1} bytes/DID", bytes as f64 / n as f64);

    let (k, el) = run(st.clone(), ids.clone(), workers, 2, batch).await;
    let rate = k as f64 / el.as_secs_f64();
    println!("update: {k} in {:.2}s = {:.0}/s node, {:.0}/s per shard", el.as_secs_f64(), rate, rate / shards as f64);
    let (mut loads, mut hits) = (0, 0);
    for s in st.shards() {
        loads += s.stats.loads.load(std::sync::atomic::Ordering::Relaxed);
        hits += s.stats.cache_hits.load(std::sync::atomic::Ordering::Relaxed);
    }
    println!("record loads from SlateDB: {loads}, cache hits: {hits}");

    let t = Instant::now();
    for s in st.shards() {
        s.flush_memtable().await.unwrap();
    }
    println!("flush: {:.2}s", t.elapsed().as_secs_f64());
    let bytes: u64 = st.shards().iter().map(|s| s.sst_bytes()).sum();
    println!("bucket after update (two versions until compaction): {bytes} SST bytes = {:.1} bytes/DID", bytes as f64 / n as f64);
    let raw: usize = ids.iter().take(10_000).map(|d| record::did_key(d).len()).sum::<usize>()
        + futures_rec_len(&st, &ids[..10_000.min(n)]).await;
    println!("raw key+value: {:.1} bytes/DID (first 10k)", raw as f64 / 10_000.min(n) as f64);

    // cold reads: a fresh store over the same bucket, every shard reopened
    for s in &layout {
        st.close_shard(s.id).await.unwrap();
    }
    let st = Arc::new(StateStore::new(store, layout.clone(), StubChain, Arc::new(Docs), ApplyConfig::default()));
    for s in &layout {
        st.open_shard(s.id, None).await.unwrap();
    }
    let mut lat = Vec::new();
    let mut cursor: Option<String> = None;
    let mut total = 0;
    let t = Instant::now();
    loop {
        let t0 = Instant::now();
        let p = st.list_repos(cursor.as_deref(), 1000).await.unwrap();
        lat.push(t0.elapsed());
        total += p.repos.len();
        match p.cursor {
            Some(c) => cursor = Some(c),
            None => break,
        }
    }
    assert_eq!(total, n);
    println!(
        "listRepos: {} pages of 1000 in {:.2}s; page p50 {:?} p99 {:?} max {:?}",
        lat.len(),
        t.elapsed().as_secs_f64(),
        pct(&mut lat, 0.5),
        pct(&mut lat, 0.99),
        pct(&mut lat, 1.0)
    );

    // one cold round of applies after the reopen (reads come from SSTs)
    let sample: Arc<Vec<String>> = Arc::new(ids.iter().take(n.min(200_000)).cloned().collect());
    let (k, el) = run(st.clone(), sample, workers, 3, batch).await;
    let rate = k as f64 / el.as_secs_f64();
    println!("cold update: {k} in {:.2}s = {:.0}/s node, {:.0}/s per shard", el.as_secs_f64(), rate, rate / shards as f64);
    for s in &layout {
        st.close_shard(s.id).await.unwrap();
    }
}

async fn futures_rec_len(st: &StateStore, ids: &[String]) -> usize {
    let mut n = 0;
    for d in ids {
        n += st.get(d).await.unwrap().unwrap().encode().len();
    }
    n
}
