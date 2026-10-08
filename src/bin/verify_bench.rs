//! Live checks and per-stage timings for event parsing and verification.
//!
//!   verify_bench capture --n 1000 --out testdata/live/frames.bin
//!   verify_bench verify  --frames testdata/live/frames.bin --keys testdata/live/keys.json
//!   verify_bench bench   --frames testdata/live/frames.bin --keys testdata/live/keys.json
//!
//! `capture` reads a few seconds of a relay's firehose (read-only). `verify`
//! resolves each DID's key politely (cached in --keys, at most --rate
//! lookups/s) and verifies every frame, printing reject reasons. `bench`
//! times each stage on one core.

use bytes::Bytes;
use clap::{Parser, Subcommand};
use std::collections::{BTreeMap, HashMap};
use std::hint::black_box;
use std::path::PathBuf;
use std::time::{Duration, Instant};
use vlrelay::event::{self, Event, Limits};
use vlrelay::identity::{HttpFetch, IdentityCache, Options as IdOptions};
use vlrelay::verify::synth::{Curve, Repo, Signer};
use vlrelay::verify::{self, SigningKey};

#[derive(Parser)]
struct Cli {
    #[command(subcommand)]
    cmd: Cmd,
}

#[derive(Subcommand)]
enum Cmd {
    Capture {
        #[arg(long, default_value = "wss://bsky.network/xrpc/com.atproto.sync.subscribeRepos")]
        url: String,
        #[arg(long, default_value_t = 1000)]
        n: usize,
        #[arg(long)]
        out: PathBuf,
    },
    Verify {
        #[arg(long)]
        frames: PathBuf,
        #[arg(long)]
        keys: PathBuf,
        #[arg(long, default_value = "https://plc.directory")]
        plc: String,
        #[arg(long, default_value_t = 5.0)]
        rate: f64,
    },
    Bench {
        #[arg(long)]
        frames: PathBuf,
        #[arg(long)]
        keys: PathBuf,
        /// Passes over the frames per stage.
        #[arg(long, default_value_t = 20)]
        passes: usize,
    },
    /// Reads PDS firehoses (read-only, from `--back` events ago) and saves
    /// every frame the relay would reject, with the reason.
    Hunt {
        /// PDS hostnames.
        #[arg(long, num_args = 1..)]
        hosts: Vec<String>,
        /// Events per host to replay from before the live head.
        #[arg(long, default_value_t = 5000)]
        back: i64,
        /// Replay from this seq instead (one host).
        #[arg(long)]
        cursor: Option<i64>,
        #[arg(long)]
        out: PathBuf,
        #[arg(long)]
        keys: PathBuf,
        #[arg(long, default_value = "https://plc.directory")]
        plc: String,
        #[arg(long, default_value_t = 5.0)]
        rate: f64,
        #[arg(long, default_value_t = 300)]
        secs: u64,
    },
    /// Loops one stage for a profiler (`samply record`, `perf record`).
    Profile {
        #[arg(long)]
        frames: PathBuf,
        #[arg(long)]
        keys: PathBuf,
        /// all (parse + verify_commit), mst, sig or hashes.
        #[arg(long, default_value = "all")]
        stage: String,
        #[arg(long, default_value_t = 10)]
        secs: u64,
    },
}

fn read_frames(p: &PathBuf) -> anyhow::Result<Vec<Bytes>> {
    let b = std::fs::read(p)?;
    let mut out = Vec::new();
    let mut i = 0;
    while i + 4 <= b.len() {
        let n = u32::from_le_bytes(b[i..i + 4].try_into()?) as usize;
        out.push(Bytes::copy_from_slice(&b[i + 4..i + 4 + n]));
        i += 4 + n;
    }
    Ok(out)
}

/// DID -> multibase key ("" when the DID has none / didn't resolve).
fn read_keys(p: &PathBuf) -> BTreeMap<String, String> {
    std::fs::read(p).ok().and_then(|b| serde_json::from_slice(&b).ok()).unwrap_or_default()
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    // the tree enables both rustls providers, so name one
    let _ = rustls::crypto::ring::default_provider().install_default();
    match Cli::parse().cmd {
        Cmd::Capture { url, n, out } => capture(&url, n, &out).await,
        Cmd::Verify { frames, keys, plc, rate } => verify_all(&frames, &keys, &plc, rate).await,
        Cmd::Bench { frames, keys, passes } => {
            bench(&frames, &keys, passes);
            Ok(())
        }
        Cmd::Hunt { hosts, back, cursor, out, keys, plc, rate, secs } => {
            hunt(hosts, back, cursor, &out, &keys, &plc, rate, secs).await
        }
        Cmd::Profile { frames, keys, stage, secs } => {
            profile(&frames, &keys, &stage, secs);
            Ok(())
        }
    }
}

async fn capture(url: &str, n: usize, out: &PathBuf) -> anyhow::Result<()> {
    use futures::StreamExt;
    let (mut ws, _) = tokio_tungstenite::connect_async(url).await?;
    let mut buf = Vec::new();
    let mut got = 0;
    let t0 = Instant::now();
    while got < n && t0.elapsed() < Duration::from_secs(60) {
        match tokio::time::timeout(Duration::from_secs(10), ws.next()).await? {
            Some(Ok(tokio_tungstenite::tungstenite::Message::Binary(b))) => {
                buf.extend_from_slice(&(b.len() as u32).to_le_bytes());
                buf.extend_from_slice(&b);
                got += 1;
            }
            Some(Ok(_)) => {}
            Some(Err(e)) => return Err(e.into()),
            None => break,
        }
    }
    let _ = ws.close(None).await;
    if let Some(d) = out.parent() {
        std::fs::create_dir_all(d)?;
    }
    std::fs::write(out, &buf)?;
    println!("captured {got} frames ({} bytes) in {:?}", buf.len(), t0.elapsed());
    Ok(())
}

async fn verify_all(frames: &PathBuf, keys_path: &PathBuf, plc: &str, rate: f64) -> anyhow::Result<()> {
    let frames = read_frames(frames)?;
    let mut keys = read_keys(keys_path);
    let ids = IdentityCache::new(
        HttpFetch::new(plc, false),
        IdOptions {
            lookups_per_sec: rate,
            burst: 1.0,
            max_budget_wait: Duration::from_secs(3600),
            ..IdOptions::default()
        },
    );
    let limits = Limits::default();
    let mut kinds: BTreeMap<&str, usize> = BTreeMap::new();
    let mut rejects: BTreeMap<String, Vec<String>> = BTreeMap::new();
    let mut ok = 0;
    let (mut with_prev_data, mut ops_total) = (0, 0);
    let mut curves: BTreeMap<&str, usize> = BTreeMap::new();
    for f in &frames {
        let e = match event::parse(f.clone(), &limits) {
            Ok(e) => e,
            Err(r) => {
                rejects.entry(format!("parse:{}", r.reason())).or_default().push(format!("{r}"));
                continue;
            }
        };
        *kinds.entry(e.kind().as_str()).or_default() += 1;
        let Some(did) = e.did().map(String::from) else {
            continue;
        };
        if !matches!(e, Event::Commit(_) | Event::Sync(_)) {
            continue;
        }
        if !keys.contains_key(&did) {
            let mb = match ids.resolve(&did).await {
                Ok(id) => id.signing_key_multibase.clone().unwrap_or_default(),
                Err(err) => {
                    eprintln!("{did}: {err}");
                    String::new()
                }
            };
            keys.insert(did.clone(), mb);
        }
        let Ok(key) = SigningKey::from_multibase(&keys[&did]) else {
            rejects.entry("no_key".into()).or_default().push(did.clone());
            continue;
        };
        *curves.entry(key.curve()).or_default() += 1;
        let r = match &e {
            Event::Commit(c) => {
                ops_total += c.ops.len();
                with_prev_data += c.prev_data.is_some() as usize;
                verify::verify_commit(c, &key)
            }
            Event::Sync(s) => verify::verify_sync(s, &key),
            _ => unreachable!(),
        };
        match r {
            Ok(_) => ok += 1,
            Err(r) => {
                let seq = match &e {
                    Event::Commit(c) => c.seq,
                    Event::Sync(s) => s.seq,
                    _ => 0,
                };
                rejects.entry(r.reason().to_string()).or_default().push(format!("{did} seq={seq}"))
            }
        }
    }
    if let Some(d) = keys_path.parent() {
        std::fs::create_dir_all(d)?;
    }
    std::fs::write(keys_path, serde_json::to_vec_pretty(&keys)?)?;
    println!("frames {}  kinds {kinds:?}", frames.len());
    println!(
        "commits+syncs verified ok: {ok}; commits with prevData: {with_prev_data}; ops {ops_total}; key curves {curves:?}"
    );
    println!("plc fetches {}", ids.stats.fetches.load(std::sync::atomic::Ordering::Relaxed));
    for (r, v) in &rejects {
        println!("reject {r}: {} (e.g. {:?})", v.len(), &v[..v.len().min(3)]);
    }
    Ok(())
}

/// The host's newest seq, from one live frame.
async fn head_seq(host: &str) -> anyhow::Result<i64> {
    use futures::StreamExt;
    let url = format!("wss://{host}/xrpc/com.atproto.sync.subscribeRepos");
    let (mut ws, _) = tokio_tungstenite::connect_async(&url).await?;
    loop {
        match tokio::time::timeout(Duration::from_secs(120), ws.next()).await? {
            Some(Ok(tokio_tungstenite::tungstenite::Message::Binary(b))) => {
                if let Ok(r) = event::route(&b, event::MAX_FRAME_BYTES)
                    && let Some(s) = r.seq
                {
                    return Ok(s);
                }
            }
            Some(Ok(_)) => {}
            Some(Err(e)) => return Err(e.into()),
            None => anyhow::bail!("{host} closed"),
        }
    }
}

#[allow(clippy::too_many_arguments)]
async fn hunt(
    hosts: Vec<String>,
    back: i64,
    from: Option<i64>,
    out: &PathBuf,
    keys_path: &PathBuf,
    plc: &str,
    rate: f64,
    secs: u64,
) -> anyhow::Result<()> {
    use futures::StreamExt;
    let ids = IdentityCache::new(
        HttpFetch::new(plc, false),
        IdOptions {
            lookups_per_sec: rate,
            burst: 1.0,
            max_budget_wait: Duration::from_secs(3600),
            ..IdOptions::default()
        },
    );
    let (tx, mut rx) = tokio::sync::mpsc::channel::<(String, Bytes)>(1024);
    for host in hosts {
        let tx = tx.clone();
        tokio::spawn(async move {
            let r: anyhow::Result<()> = async {
                let cursor = match from {
                    Some(c) => c,
                    None => head_seq(&host).await? - back,
                };
                let url = format!("wss://{host}/xrpc/com.atproto.sync.subscribeRepos?cursor={}", cursor.max(0));
                eprintln!("{host}: from seq {cursor}");
                let (mut ws, _) = tokio_tungstenite::connect_async(&url).await?;
                while let Some(m) = ws.next().await {
                    if let tokio_tungstenite::tungstenite::Message::Binary(b) = m?
                        && tx.send((host.clone(), b)).await.is_err()
                    {
                        break;
                    }
                }
                Ok(())
            }
            .await;
            eprintln!("{host}: ended {r:?}");
        });
    }
    drop(tx);
    let mut keys = read_keys(keys_path);
    let limits = Limits::default();
    let mut chains: HashMap<String, verify::ChainState> = HashMap::new();
    let mut saved = Vec::new();
    let mut counts: BTreeMap<String, usize> = BTreeMap::new();
    let deadline = tokio::time::Instant::now() + Duration::from_secs(secs);
    // timeout_at polls the receiver first, so a busy stream never times out
    while tokio::time::Instant::now() < deadline
        && let Ok(Some((host, f))) = tokio::time::timeout_at(deadline, rx.recv()).await
    {
        *counts.entry(format!("{host} frames")).or_default() += 1;
        let reason = match event::parse(f.clone(), &limits) {
            Err(r) => Some(format!("parse:{}", r.reason())),
            Ok(e @ (Event::Commit(_) | Event::Sync(_))) => {
                let did = e.did().unwrap().to_string();
                if !keys.contains_key(&did) {
                    let mb = match ids.resolve(&did).await {
                        Ok(id) => id.signing_key_multibase.clone().unwrap_or_default(),
                        Err(_) => String::new(),
                    };
                    keys.insert(did.clone(), mb);
                }
                match SigningKey::from_multibase(&keys[&did]) {
                    Err(_) => None,
                    Ok(key) => {
                        let v = match &e {
                            Event::Commit(c) => verify::verify_commit(c, &key),
                            Event::Sync(s) => verify::verify_sync(s, &key),
                            _ => unreachable!(),
                        };
                        match v {
                            Err(r) => Some(r.reason().to_string()),
                            Ok(v) => match verify::check_chain(chains.get(&did), &v) {
                                Ok(st) => {
                                    chains.insert(did, st);
                                    None
                                }
                                Err(verify::ChainError::Duplicate) => None,
                                Err(c) => {
                                    if let verify::ChainError::PrevDataMismatch { .. } = c {
                                        chains.insert(
                                            did,
                                            verify::ChainState { rev: v.rev, data: v.data, commit: v.commit },
                                        );
                                    }
                                    Some(format!("chain:{}", c.reason()))
                                }
                            },
                        }
                    }
                }
            }
            Ok(_) => None,
        };
        if let Some(r) = reason {
            let seq = event::route(&f, limits.max_frame_bytes).ok().and_then(|r| r.seq).unwrap_or(-1);
            println!("{host} seq={seq} {r}");
            *counts.entry(r).or_default() += 1;
            saved.extend_from_slice(&(f.len() as u32).to_le_bytes());
            saved.extend_from_slice(&f);
            if let Some(d) = out.parent() {
                std::fs::create_dir_all(d)?;
            }
            std::fs::write(out, &saved)?;
        }
    }
    std::fs::write(keys_path, serde_json::to_vec_pretty(&keys)?)?;
    println!("{counts:?}");
    Ok(())
}

/// ns per item over `passes` passes of `f` on each item, best of 3 runs.
fn time<T>(items: &[T], passes: usize, mut f: impl FnMut(&T)) -> f64 {
    let mut best = f64::MAX;
    for _ in 0..3 {
        let t0 = Instant::now();
        for _ in 0..passes {
            for it in items {
                f(it);
            }
        }
        best = best.min(t0.elapsed().as_nanos() as f64 / (passes * items.len()).max(1) as f64);
    }
    best
}

type Commits = Vec<(event::ParsedCommit, SigningKey)>;

fn load_commits(frames: &[Bytes], keys_path: &PathBuf) -> (HashMap<String, SigningKey>, Commits) {
    let keys: HashMap<String, SigningKey> = read_keys(keys_path)
        .into_iter()
        .filter_map(|(d, k)| SigningKey::from_multibase(&k).ok().map(|k| (d, k)))
        .collect();
    let limits = Limits::default();
    let commits = frames
        .iter()
        .filter_map(|f| match event::parse(f.clone(), &limits) {
            Ok(Event::Commit(c)) => keys.get(&c.repo).cloned().map(|k| (c, k)),
            _ => None,
        })
        .filter(|(c, k)| verify::verify_commit(c, k).is_ok())
        .collect();
    (keys, commits)
}

fn profile(frames: &PathBuf, keys_path: &PathBuf, stage: &str, secs: u64) {
    let frames = read_frames(frames).expect("frames");
    let (_, commits) = load_commits(&frames, keys_path);
    let limits = Limits::default();
    let opts = verify::Options::default();
    let staged: Vec<_> = commits
        .iter()
        .map(|(c, k)| {
            let m = verify::block_map(&c.blocks).unwrap();
            let data = verify::check_commit_block(m[&c.commit], &c.repo, c.rev, k).unwrap().data;
            (c, k, m, data)
        })
        .collect();
    let t0 = Instant::now();
    let mut n = 0u64;
    while t0.elapsed() < Duration::from_secs(secs) {
        for (c, k, m, data) in &staged {
            match stage {
                "mst" => drop(black_box(verify::check_ops(c, *data, m, &opts))),
                "sig" => drop(black_box(verify::check_commit_block(m[&c.commit], &c.repo, c.rev, k))),
                "hashes" => drop(black_box(verify::block_map(&c.blocks))),
                _ => {
                    if let Ok(Event::Commit(p)) = event::parse(c.frame.clone(), &limits) {
                        drop(black_box(verify::verify_commit(&p, k)));
                    }
                }
            }
            n += 1;
        }
    }
    println!("{stage}: {n} iterations, {:.2} µs each", t0.elapsed().as_secs_f64() * 1e6 / n as f64);
}

fn bench(frames: &PathBuf, keys_path: &PathBuf, passes: usize) {
    let frames = read_frames(frames).expect("frames");
    let (keys, commits) = load_commits(&frames, keys_path);
    let limits = Limits::default();
    let bytes: usize = commits.iter().map(|(c, _)| c.frame.len()).sum();
    println!(
        "{} frames, {} verifiable commits (mean frame {} B, mean ops {:.2}, mean blocks {:.1})",
        frames.len(),
        commits.len(),
        bytes / commits.len().max(1),
        commits.iter().map(|(c, _)| c.ops.len()).sum::<usize>() as f64 / commits.len().max(1) as f64,
        commits.iter().map(|(c, _)| c.blocks.len()).sum::<usize>() as f64 / commits.len().max(1) as f64,
    );
    let mut rows: Vec<(&str, f64)> = Vec::new();
    rows.push((
        "route (cheap parse), all frames",
        time(&frames, passes, |f| {
            black_box(event::route(f, limits.max_frame_bytes).ok());
        }),
    ));
    rows.push((
        "parse (strict), all frames",
        time(&frames, passes, |f| {
            black_box(event::parse(f.clone(), &limits).ok());
        }),
    ));
    let cframes: Vec<Bytes> = commits.iter().map(|(c, _)| c.frame.clone()).collect();
    rows.push((
        "parse (strict), commits",
        time(&cframes, passes, |f| {
            black_box(event::parse(f.clone(), &limits).ok());
        }),
    ));
    rows.push((
        "block hashes + map",
        time(&commits, passes, |(c, _)| {
            black_box(verify::block_map(&c.blocks).ok());
        }),
    ));
    // the later stages alone, on block maps and commit objects built up front
    let staged: Vec<_> = commits
        .iter()
        .map(|(c, k)| {
            let m = verify::block_map(&c.blocks).unwrap();
            let data = verify::check_commit_block(m[&c.commit], &c.repo, c.rev, k).unwrap().data;
            (c, k, m, data)
        })
        .collect();
    rows.push((
        "commit decode + fields + signature",
        time(&staged, passes, |(c, k, m, _)| {
            black_box(verify::check_commit_block(m[&c.commit], &c.repo, c.rev, k).ok());
        }),
    ));
    let opts = verify::Options::default();
    rows.push((
        "MST load + op checks + inductive proof",
        time(&staged, passes, |(c, _, m, data)| {
            black_box(verify::check_ops(c, *data, m, &opts).ok());
        }),
    ));
    rows.push((
        "verify_commit (all of the above but parse)",
        time(&commits, passes, |(c, k)| {
            black_box(verify::verify_commit(c, k).ok());
        }),
    ));
    rows.push((
        "parse + verify_commit",
        time(&cframes, passes, |f| {
            if let Ok(Event::Commit(c)) = event::parse(f.clone(), &limits) {
                black_box(verify::verify_commit(&c, &keys[&c.repo]).ok());
            }
        }),
    ));
    rows.push((
        "encode_with_seq",
        time(&commits, passes, |(c, _)| {
            black_box(event::encode_with_seq(&c.frame, c.seq_span, 1 << 40));
        }),
    ));

    // signatures alone, k256 vs P-256, on synthetic commits of each curve
    for curve in [Curve::K256, Curve::P256] {
        let mut r = Repo::new("did:plc:benchbenchbenchbench", Signer::new(curve, 1), 1000);
        let key = r.signer.public();
        let sc: Vec<event::ParsedCommit> = (0..200)
            .map(|_| {
                let ops = r.mixed_ops(2);
                match event::parse(r.commit(&ops), &limits) {
                    Ok(Event::Commit(c)) => c,
                    _ => unreachable!(),
                }
            })
            .collect();
        let blocks: Vec<(Vec<u8>, String, vlsync_atproto::tid::Tid)> = sc
            .iter()
            .map(|c| (c.blocks.iter().find(|(x, _)| *x == c.commit).unwrap().1.to_vec(), c.repo.clone(), c.rev))
            .collect();
        let label =
            if curve == Curve::K256 { "signature only, k256 (libsecp256k1)" } else { "signature only, P-256 (ring)" };
        rows.push((
            label,
            time(&blocks, passes, |(b, did, rev)| {
                black_box(verify::check_commit_block(b, did, *rev, &key).ok());
            }),
        ));
        if curve == Curve::P256 {
            let vk = match &key {
                SigningKey::P256(pt) => p256::ecdsa::VerifyingKey::from_sec1_bytes(&pt[..]).unwrap(),
                _ => unreachable!(),
            };
            rows.push((
                "signature only, P-256 (RustCrypto, for comparison)",
                time(&blocks, passes, |(b, _, _)| {
                    use p256::ecdsa::signature::hazmat::PrehashVerifier;
                    let (u, sig) = event::split_signed_commit(b).unwrap();
                    let s = p256::ecdsa::Signature::from_slice(sig).unwrap();
                    black_box(vk.verify_prehash(&u.sha256(), &s).is_ok());
                }),
            ));
        }
        rows.push((
            if curve == Curve::K256 {
                "verify_commit, synthetic k256, 2 ops, 1k-record repo"
            } else {
                "verify_commit, synthetic P-256, 2 ops, 1k-record repo"
            },
            time(&sc, passes, |c| {
                black_box(verify::verify_commit(c, &key).ok());
            }),
        ));
    }
    println!("{:<56} {:>10}", "stage (single core)", "µs/event");
    for (name, ns) in rows {
        println!("{name:<56} {:>10.2}", ns / 1000.0);
    }
}
