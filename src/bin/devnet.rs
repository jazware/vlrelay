//! devnet: accounts and traffic for the local network in dev/ (docs/devloop.md).
//!
//!   devnet seed --host http://127.0.0.1:2984 --host http://localhost:2983 --accounts 30
//!   devnet load --rate 50 [--duration 60]
//!
//! `load` is open loop: operations start on a fixed schedule whether or not
//! earlier ones have finished, so a slow PDS shows up as errors and
//! latency, not as a quietly lower rate. Besides record writes (posts,
//! likes, reposts, follows, unfollows, deletes, profile edits) it changes a
//! handle every --identity-every seconds and deactivates an account for a
//! few seconds every --deactivate-every, so every firehose event type
//! (#commit, #identity, #account; #sync from the PDSes that emit one on
//! activation) shows up.

use clap::{Parser, Subcommand};
use parking_lot::Mutex;
use rand::Rng;
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};
use std::collections::{BTreeMap, VecDeque};
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::time::{Duration, Instant};

#[derive(Parser)]
struct Args {
    #[arg(long, default_value = "dev/state/accounts.json")]
    accounts_file: String,
    #[command(subcommand)]
    cmd: Cmd,
}

#[derive(Subcommand)]
enum Cmd {
    /// Create accounts round-robin across the hosts (appends to --accounts-file).
    Seed {
        /// A PDS origin. Repeatable.
        #[arg(long = "host", required = true)]
        hosts: Vec<String>,
        #[arg(long, default_value_t = 30)]
        accounts: usize,
        /// Records (posts) per new account, besides its profile.
        #[arg(long, default_value_t = 3)]
        records: usize,
        #[arg(long, default_value_t = 8)]
        concurrency: usize,
    },
    /// Continuous mixed writes over the seeded accounts.
    Load {
        /// Record writes per second, fleet-wide.
        #[arg(long, default_value_t = 20.0)]
        rate: f64,
        /// Seconds (0 = until interrupted).
        #[arg(long, default_value_t = 0)]
        duration: u64,
        /// Seconds between handle changes (0 = never).
        #[arg(long, default_value_t = 20)]
        identity_every: u64,
        /// Seconds between deactivate/reactivate cycles (0 = never).
        #[arg(long, default_value_t = 45)]
        deactivate_every: u64,
        /// Seconds an account stays deactivated.
        #[arg(long, default_value_t = 5)]
        deactivated_for: u64,
        #[arg(long, default_value_t = 512)]
        max_inflight: usize,
        #[arg(long, default_value_t = 5)]
        report_secs: u64,
    },
}

#[derive(Serialize, Deserialize, Clone)]
struct Acct {
    host: String,
    did: String,
    handle: String,
    /// The first label of the seeded handle; handle changes append to it.
    base: String,
    /// The host's handle suffix (".test", ".pds1.test").
    domain: String,
    password: String,
    #[serde(default)]
    access: String,
    #[serde(default)]
    refresh: String,
}

fn client() -> reqwest::Client {
    reqwest::Client::builder().timeout(Duration::from_secs(30)).pool_max_idle_per_host(64).build().unwrap()
}

#[derive(Debug)]
struct XErr {
    status: u16,
    error: String,
    body: String,
}

impl std::fmt::Display for XErr {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{} {}: {}", self.status, self.error, self.body.chars().take(300).collect::<String>())
    }
}

async fn xrpc(c: &reqwest::Client, host: &str, nsid: &str, token: Option<&str>, body: Option<&Value>) -> Result<Value, XErr> {
    let url = format!("{host}/xrpc/{nsid}");
    let mut rq = match body {
        Some(b) => c.post(url).json(b),
        None => c.get(url),
    };
    if let Some(t) = token {
        rq = rq.bearer_auth(t);
    }
    let r = rq.send().await.map_err(|e| XErr { status: 0, error: "Transport".into(), body: e.to_string() })?;
    let status = r.status().as_u16();
    let text = r.text().await.unwrap_or_default();
    if !(200..300).contains(&status) {
        let error = serde_json::from_str::<Value>(&text).ok().and_then(|v| v["error"].as_str().map(str::to_string));
        return Err(XErr { status, error: error.unwrap_or_default(), body: text });
    }
    Ok(if text.is_empty() { Value::Null } else { serde_json::from_str(&text).unwrap_or(Value::Null) })
}

fn now() -> String {
    chrono::Utc::now().format("%Y-%m-%dT%H:%M:%S%.3fZ").to_string()
}

fn post(text: String) -> Value {
    json!({"$type": "app.bsky.feed.post", "text": text, "createdAt": now()})
}

fn load_accounts(path: &str) -> anyhow::Result<Vec<Acct>> {
    match std::fs::read(path) {
        Ok(b) => Ok(serde_json::from_slice(&b)?),
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => Ok(Vec::new()),
        Err(e) => Err(e.into()),
    }
}

fn save_accounts(path: &str, accts: &[Acct]) -> anyhow::Result<()> {
    if let Some(dir) = std::path::Path::new(path).parent() {
        std::fs::create_dir_all(dir)?;
    }
    let tmp = format!("{path}.tmp");
    std::fs::write(&tmp, serde_json::to_vec_pretty(accts)?)?;
    std::fs::rename(tmp, path)?;
    Ok(())
}

async fn seed(args: &Args, hosts: &[String], n: usize, records: usize, concurrency: usize) -> anyhow::Result<()> {
    use futures::StreamExt;
    let c = client();
    let mut domains = Vec::new();
    for h in hosts {
        let d = xrpc(&c, h, "com.atproto.server.describeServer", None, None).await.map_err(|e| anyhow::anyhow!("{h}: {e}"))?;
        let dom = d["availableUserDomains"][0].as_str().ok_or_else(|| anyhow::anyhow!("{h}: no availableUserDomains"))?;
        domains.push(dom.to_string());
    }
    // a run tag keeps handles unique across seeds of a long-lived network
    let tag = format!("{:x}", chrono::Utc::now().timestamp() % 0xfffff);
    let t0 = Instant::now();
    let made: Vec<anyhow::Result<Acct>> = futures::stream::iter(0..n)
        .map(|i| {
            let c = c.clone();
            let host = hosts[i % hosts.len()].clone();
            let domain = domains[i % hosts.len()].clone();
            let base = format!("dev{tag}x{i}");
            async move {
                let handle = format!("{base}{domain}");
                let password = "hunter2hunter2".to_string();
                let r = xrpc(
                    &c,
                    &host,
                    "com.atproto.server.createAccount",
                    None,
                    Some(&json!({"handle": handle, "password": password, "email": format!("{base}@example.com")})),
                )
                .await
                .map_err(|e| anyhow::anyhow!("{host} createAccount {handle}: {e}"))?;
                let a = Acct {
                    did: r["did"].as_str().unwrap_or_default().to_string(),
                    access: r["accessJwt"].as_str().unwrap_or_default().to_string(),
                    refresh: r["refreshJwt"].as_str().unwrap_or_default().to_string(),
                    host,
                    handle,
                    base,
                    domain,
                    password,
                };
                let profile = json!({"$type": "app.bsky.actor.profile", "displayName": format!("dev {i}"), "description": "devnet account"});
                xrpc(&c, &a.host, "com.atproto.repo.putRecord", Some(&a.access),
                    Some(&json!({"repo": a.did, "collection": "app.bsky.actor.profile", "rkey": "self", "record": profile})))
                    .await
                    .map_err(|e| anyhow::anyhow!("{} profile: {e}", a.host))?;
                for k in 0..records {
                    xrpc(&c, &a.host, "com.atproto.repo.createRecord", Some(&a.access),
                        Some(&json!({"repo": a.did, "collection": "app.bsky.feed.post", "record": post(format!("seed post {k}"))})))
                        .await
                        .map_err(|e| anyhow::anyhow!("{} post: {e}", a.host))?;
                }
                Ok(a)
            }
        })
        .buffer_unordered(concurrency)
        .collect()
        .await;
    let mut accts = load_accounts(&args.accounts_file)?;
    let mut errs = 0;
    for m in made {
        match m {
            Ok(a) => accts.push(a),
            Err(e) => {
                errs += 1;
                eprintln!("devnet seed: {e}");
            }
        }
    }
    save_accounts(&args.accounts_file, &accts)?;
    let mut per: BTreeMap<&str, usize> = BTreeMap::new();
    for a in &accts {
        *per.entry(a.host.as_str()).or_default() += 1;
    }
    eprintln!("devnet seed: {} new accounts in {:.1}s ({errs} failed); {} total in {}: {per:?}", n - errs, t0.elapsed().as_secs_f64(), accts.len(), args.accounts_file);
    anyhow::ensure!(errs == 0, "{errs} accounts failed");
    Ok(())
}

struct Live {
    acct: Mutex<Acct>,
    /// Records this run created: (collection, rkey).
    mine: Mutex<Vec<(String, String)>>,
    follows: Mutex<Vec<String>>,
    inactive: AtomicBool,
    handle_n: AtomicU64,
}

struct Load {
    c: reqwest::Client,
    accts: Vec<Live>,
    posts: Mutex<VecDeque<(String, String)>>,
    stats: Mutex<BTreeMap<&'static str, (u64, u64)>>,
    errors: Mutex<BTreeMap<String, u64>>,
}

impl Load {
    fn record(&self, op: &'static str, r: &Result<Value, XErr>) {
        let mut s = self.stats.lock();
        let e = s.entry(op).or_default();
        match r {
            Ok(_) => e.0 += 1,
            Err(err) => {
                e.1 += 1;
                *self.errors.lock().entry(format!("{op}: {} {}", err.status, if err.error.is_empty() { &err.body } else { &err.error })).or_default() += 1;
            }
        }
    }

    /// An authed call, logging in again once if the access token expired.
    async fn call(&self, i: usize, nsid: &str, body: Value) -> Result<Value, XErr> {
        let (host, tok) = {
            let a = self.accts[i].acct.lock();
            (a.host.clone(), a.access.clone())
        };
        match xrpc(&self.c, &host, nsid, Some(&tok), Some(&body)).await {
            Err(e) if e.status == 401 || e.error == "ExpiredToken" || e.error == "InvalidToken" => {
                self.login(i).await?;
                let tok = self.accts[i].acct.lock().access.clone();
                xrpc(&self.c, &host, nsid, Some(&tok), Some(&body)).await
            }
            r => r,
        }
    }

    async fn login(&self, i: usize) -> Result<(), XErr> {
        let (host, did, pw) = {
            let a = self.accts[i].acct.lock();
            (a.host.clone(), a.did.clone(), a.password.clone())
        };
        let r = xrpc(&self.c, &host, "com.atproto.server.createSession", None, Some(&json!({"identifier": did, "password": pw}))).await?;
        let mut a = self.accts[i].acct.lock();
        a.access = r["accessJwt"].as_str().unwrap_or_default().to_string();
        a.refresh = r["refreshJwt"].as_str().unwrap_or_default().to_string();
        Ok(())
    }

    fn pick_active(&self) -> Option<usize> {
        let mut rng = rand::thread_rng();
        for _ in 0..16 {
            let i = rng.gen_range(0..self.accts.len());
            if !self.accts[i].inactive.load(Ordering::Acquire) {
                return Some(i);
            }
        }
        None
    }

    async fn write(&self, n: u64) {
        let Some(i) = self.pick_active() else { return };
        let did = self.accts[i].acct.lock().did.clone();
        let roll = rand::thread_rng().gen_range(0..100);
        let create = |coll: &str, rec: Value| json!({"repo": did, "collection": coll, "record": rec});
        let subject = || {
            let p = self.posts.lock();
            let k = rand::thread_rng().gen_range(0..p.len().max(1));
            p.get(k).cloned()
        };
        let (op, r, coll) = match roll {
            0..35 => ("post", self.call(i, "com.atproto.repo.createRecord", create("app.bsky.feed.post", post(format!("load post {n}")))).await, "app.bsky.feed.post"),
            35..60 => {
                let Some((uri, cid)) = subject() else { return };
                let rec = json!({"$type": "app.bsky.feed.like", "subject": {"uri": uri, "cid": cid}, "createdAt": now()});
                ("like", self.call(i, "com.atproto.repo.createRecord", create("app.bsky.feed.like", rec)).await, "app.bsky.feed.like")
            }
            60..68 => {
                let Some((uri, cid)) = subject() else { return };
                let rec = json!({"$type": "app.bsky.feed.repost", "subject": {"uri": uri, "cid": cid}, "createdAt": now()});
                ("repost", self.call(i, "com.atproto.repo.createRecord", create("app.bsky.feed.repost", rec)).await, "app.bsky.feed.repost")
            }
            68..78 => {
                let j = rand::thread_rng().gen_range(0..self.accts.len());
                let target = self.accts[j].acct.lock().did.clone();
                let rec = json!({"$type": "app.bsky.graph.follow", "subject": target, "createdAt": now()});
                ("follow", self.call(i, "com.atproto.repo.createRecord", create("app.bsky.graph.follow", rec)).await, "app.bsky.graph.follow")
            }
            78..93 => {
                let victim = {
                    let mut m = self.accts[i].mine.lock();
                    if m.is_empty() {
                        None
                    } else {
                        let k = rand::thread_rng().gen_range(0..m.len());
                        Some(m.swap_remove(k))
                    }
                };
                let Some((coll, rkey)) = victim else { return };
                let op = if coll == "app.bsky.graph.follow" { "unfollow" } else { "delete" };
                let r = self.call(i, "com.atproto.repo.deleteRecord", json!({"repo": did, "collection": coll, "rkey": rkey})).await;
                self.record(op, &r);
                return;
            }
            _ => {
                let rec = json!({"$type": "app.bsky.actor.profile", "displayName": format!("dev edit {n}"), "description": format!("edited at {}", now())});
                let r = self.call(i, "com.atproto.repo.putRecord", json!({"repo": did, "collection": "app.bsky.actor.profile", "rkey": "self", "record": rec})).await;
                self.record("profile", &r);
                return;
            }
        };
        self.record(op, &r);
        if let Ok(v) = &r {
            if let (Some(uri), Some(cid)) = (v["uri"].as_str(), v["cid"].as_str()) {
                let rkey = uri.rsplit('/').next().unwrap_or_default().to_string();
                self.accts[i].mine.lock().push((coll.to_string(), rkey));
                if coll == "app.bsky.feed.post" {
                    let mut p = self.posts.lock();
                    p.push_back((uri.to_string(), cid.to_string()));
                    if p.len() > 2000 {
                        p.pop_front();
                    }
                }
                if coll == "app.bsky.graph.follow" {
                    self.accts[i].follows.lock().push(uri.to_string());
                }
            }
        }
    }

    async fn change_handle(&self) {
        let Some(i) = self.pick_active() else { return };
        let k = self.accts[i].handle_n.fetch_add(1, Ordering::Relaxed) + 1;
        let (base, domain) = {
            let a = self.accts[i].acct.lock();
            (a.base.clone(), a.domain.clone())
        };
        let handle = format!("{base}h{k}{domain}");
        let r = self.call(i, "com.atproto.identity.updateHandle", json!({"handle": handle})).await;
        if r.is_ok() {
            self.accts[i].acct.lock().handle = handle;
        }
        self.record("handle", &r);
    }

    async fn deactivate_cycle(&self, hold: Duration) {
        let Some(i) = self.pick_active() else { return };
        self.accts[i].inactive.store(true, Ordering::Release);
        let r = self.call(i, "com.atproto.server.deactivateAccount", json!({})).await;
        self.record("deactivate", &r);
        tokio::time::sleep(hold).await;
        let r = self.call(i, "com.atproto.server.activateAccount", json!({})).await;
        self.record("activate", &r);
        if r.is_ok() {
            self.accts[i].inactive.store(false, Ordering::Release);
        }
    }
}

#[allow(clippy::too_many_arguments)]
async fn load(
    args: &Args,
    rate: f64,
    duration: u64,
    identity_every: u64,
    deactivate_every: u64,
    deactivated_for: u64,
    max_inflight: usize,
    report_secs: u64,
) -> anyhow::Result<()> {
    let accts = load_accounts(&args.accounts_file)?;
    anyhow::ensure!(!accts.is_empty(), "no accounts in {}: run `just dev-seed` first", args.accounts_file);
    let l = Arc::new(Load {
        c: client(),
        accts: accts
            .into_iter()
            .map(|a| Live { acct: Mutex::new(a), mine: Default::default(), follows: Default::default(), inactive: AtomicBool::new(false), handle_n: AtomicU64::new(0) })
            .collect(),
        posts: Default::default(),
        stats: Default::default(),
        errors: Default::default(),
    });
    // fresh tokens up front; an account deactivated by an interrupted run is reactivated
    for i in 0..l.accts.len() {
        if let Err(e) = l.login(i).await {
            eprintln!("devnet load: login {}: {e}", l.accts[i].acct.lock().did);
            l.accts[i].inactive.store(true, Ordering::Release);
            continue;
        }
        let _ = l.call(i, "com.atproto.server.activateAccount", json!({})).await;
        let (host, did) = {
            let a = l.accts[i].acct.lock();
            (a.host.clone(), a.did.clone())
        };
        // a like needs a subject: seed the ring from each account's own posts
        let url = format!("com.atproto.repo.listRecords?repo={did}&collection=app.bsky.feed.post&limit=5");
        if let Ok(v) = xrpc(&l.c, &host, &url, None, None).await {
            for r in v["records"].as_array().into_iter().flatten() {
                if let (Some(u), Some(c)) = (r["uri"].as_str(), r["cid"].as_str()) {
                    l.posts.lock().push_back((u.to_string(), c.to_string()));
                }
            }
        }
    }
    eprintln!("devnet load: {} accounts, {rate}/s writes, handle change every {identity_every}s, deactivation every {deactivate_every}s", l.accts.len());

    let inflight = Arc::new(tokio::sync::Semaphore::new(max_inflight));
    let dropped = Arc::new(AtomicU64::new(0));
    let start = Instant::now();
    let end = (duration > 0).then(|| start + Duration::from_secs(duration));
    if identity_every > 0 {
        let l = l.clone();
        tokio::spawn(async move {
            let mut t = tokio::time::interval(Duration::from_secs(identity_every));
            t.tick().await;
            loop {
                t.tick().await;
                l.change_handle().await;
            }
        });
    }
    if deactivate_every > 0 {
        let l = l.clone();
        tokio::spawn(async move {
            let mut t = tokio::time::interval(Duration::from_secs(deactivate_every));
            t.tick().await;
            loop {
                t.tick().await;
                let l = l.clone();
                tokio::spawn(async move { l.deactivate_cycle(Duration::from_secs(deactivated_for)).await });
            }
        });
    }
    {
        let l = l.clone();
        let dropped = dropped.clone();
        tokio::spawn(async move {
            let mut t = tokio::time::interval(Duration::from_secs(report_secs.max(1)));
            t.tick().await;
            loop {
                t.tick().await;
                let s = l.stats.lock().clone();
                let line: Vec<String> = s.iter().map(|(k, (ok, err))| if *err > 0 { format!("{k} {ok}/{err}err") } else { format!("{k} {ok}") }).collect();
                eprintln!("devnet load: t={:.0}s {} dropped {}", start.elapsed().as_secs_f64(), line.join(" "), dropped.load(Ordering::Relaxed));
            }
        });
    }

    let period = Duration::from_secs_f64(1.0 / rate.max(0.001));
    let mut next = Instant::now();
    let mut n = 0u64;
    let stop = tokio::signal::ctrl_c();
    tokio::pin!(stop);
    loop {
        if end.is_some_and(|e| Instant::now() >= e) {
            break;
        }
        tokio::select! {
            _ = &mut stop => break,
            _ = tokio::time::sleep_until(next.into()) => {}
        }
        next += period;
        n += 1;
        match inflight.clone().try_acquire_owned() {
            Ok(permit) => {
                let l = l.clone();
                tokio::spawn(async move {
                    l.write(n).await;
                    drop(permit);
                });
            }
            Err(_) => {
                dropped.fetch_add(1, Ordering::Relaxed);
            }
        }
    }
    let _ = tokio::time::timeout(Duration::from_secs(10), inflight.acquire_many(max_inflight as u32)).await;
    // leave no account deactivated
    for i in 0..l.accts.len() {
        if l.accts[i].inactive.load(Ordering::Acquire) {
            let r = l.call(i, "com.atproto.server.activateAccount", json!({})).await;
            l.record("activate", &r);
        }
    }
    let accts: Vec<Acct> = l.accts.iter().map(|a| a.acct.lock().clone()).collect();
    save_accounts(&args.accounts_file, &accts)?;
    let s = l.stats.lock().clone();
    let (ok, err): (u64, u64) = s.values().fold((0, 0), |a, b| (a.0 + b.0, a.1 + b.1));
    eprintln!("devnet load: done in {:.1}s: {ok} ok, {err} errors, {} dropped; {s:?}", start.elapsed().as_secs_f64(), dropped.load(Ordering::Relaxed));
    for (e, c) in l.errors.lock().iter().take(20) {
        eprintln!("  {c}x {e}");
    }
    Ok(())
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let _ = rustls::crypto::ring::default_provider().install_default();
    let args = Args::parse();
    match &args.cmd {
        Cmd::Seed { hosts, accounts, records, concurrency } => seed(&args, hosts, *accounts, *records, *concurrency).await,
        Cmd::Load { rate, duration, identity_every, deactivate_every, deactivated_for, max_inflight, report_secs } => {
            load(&args, *rate, *duration, *identity_every, *deactivate_every, *deactivated_for, *max_inflight, *report_secs).await
        }
    }
}
