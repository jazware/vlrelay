//! refdiff: subscribe read-only to a relay and a few PDS hosts at once, then
//! report what one side saw and the other didn't, per-DID ordering, PDS→relay
//! latency, and the relay's frame-size mix.

use std::collections::{HashMap, HashSet};
use std::io::Cursor;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use anyhow::{Context, Result, anyhow};
use ciborium::Value;
use clap::Parser;
use futures_util::StreamExt;
use hdrhistogram::Histogram;
use serde_json::json;
use tokio::sync::{mpsc, watch};
use tokio_tungstenite::tungstenite::Message;

#[derive(Parser, Debug)]
#[command(about = "Diff a relay's firehose against PDS firehoses (read-only)")]
struct Args {
    /// Relay host (wss, no path), or a ws:// or wss:// origin.
    #[arg(long, default_value = "relay1.us-east.bsky.network")]
    relay: String,
    /// PDS hosts to subscribe to directly. Repeatable.
    #[arg(long = "pds")]
    pds: Vec<String>,
    /// How long to record from all sides, in seconds.
    #[arg(long, default_value_t = 600)]
    secs: u64,
    /// Events received in the first this-many seconds are only used for
    /// matching, since their counterpart may predate our connection.
    #[arg(long, default_value_t = 30)]
    warmup_secs: u64,
    /// Keep reading the relay this long after the PDS sockets close, so slow
    /// relay deliveries of the last PDS events still match.
    #[arg(long, default_value_t = 60)]
    grace_secs: u64,
    /// Write the JSON report here.
    #[arg(long)]
    out: Option<String>,
    /// How many example DIDs to list per mismatch class.
    #[arg(long, default_value_t = 10)]
    examples: usize,
}

#[derive(Clone, Copy, PartialEq, Eq, Hash, Debug)]
enum Kind {
    Commit,
    Sync,
    Identity,
    Account,
    Info,
    Error,
    Other,
}

const KINDS: [Kind; 7] = [Kind::Commit, Kind::Sync, Kind::Identity, Kind::Account, Kind::Info, Kind::Error, Kind::Other];

impl Kind {
    fn name(self) -> &'static str {
        match self {
            Kind::Commit => "#commit",
            Kind::Sync => "#sync",
            Kind::Identity => "#identity",
            Kind::Account => "#account",
            Kind::Info => "#info",
            Kind::Error => "error",
            Kind::Other => "other",
        }
    }
    fn idx(self) -> usize {
        KINDS.iter().position(|k| *k == self).unwrap()
    }
}

#[derive(Clone, Debug)]
struct Ev {
    side: usize,
    kind: Kind,
    did: u64,
    rev: u64,
    cid: u64,
    seq: i64,
    recv_us: i64,
    time_us: i64,
    size: u32,
    ops: u16,
    blocks: u32,
    too_big: bool,
    has_handle: bool,
}

enum Msg {
    Ev(Ev, Option<String>),
    Disconnect { side: usize, at_us: i64, why: String },
}

struct Clock {
    base_us: i64,
    start: Instant,
}

impl Clock {
    fn new() -> Self {
        let base_us = SystemTime::now().duration_since(UNIX_EPOCH).unwrap().as_micros() as i64;
        Clock { base_us, start: Instant::now() }
    }
    fn now_us(&self) -> i64 {
        self.base_us + self.start.elapsed().as_micros() as i64
    }
}

fn fnv(b: &[u8]) -> u64 {
    let mut h: u64 = 0xcbf29ce484222325;
    for x in b {
        h ^= *x as u64;
        h = h.wrapping_mul(0x100000001b3);
    }
    h
}

/// Revs are TIDs, so decoding them keeps their order and fits a u64. Anything
/// else falls back to a hash, which is fine for matching.
fn rev_key(s: &str) -> u64 {
    const ALPHA: &[u8] = b"234567abcdefghijklmnopqrstuvwxyz";
    if s.len() != 13 {
        return fnv(s.as_bytes());
    }
    let mut v: u128 = 0;
    for c in s.bytes() {
        match ALPHA.iter().position(|a| *a == c) {
            Some(i) => v = (v << 5) | i as u128,
            None => return fnv(s.as_bytes()),
        }
    }
    v as u64
}

fn map_get<'a>(m: &'a [(Value, Value)], k: &str) -> Option<&'a Value> {
    m.iter().find(|(key, _)| matches!(key, Value::Text(t) if t == k)).map(|(_, v)| v)
}

fn text<'a>(m: &'a [(Value, Value)], k: &str) -> Option<&'a str> {
    match map_get(m, k)? {
        Value::Text(t) => Some(t.as_str()),
        _ => None,
    }
}

fn int(m: &[(Value, Value)], k: &str) -> Option<i64> {
    match map_get(m, k)? {
        Value::Integer(i) => i64::try_from(i128::from(*i)).ok(),
        _ => None,
    }
}

fn parse_time_us(s: &str) -> i64 {
    chrono::DateTime::parse_from_rfc3339(s).map(|t| t.timestamp_micros()).unwrap_or(0)
}

fn parse_frame(side: usize, frame: &[u8], recv_us: i64) -> Result<(Ev, Option<String>)> {
    let mut cur = Cursor::new(frame);
    let hdr: Value = ciborium::de::from_reader(&mut cur).context("header")?;
    let hdr = hdr.as_map().ok_or_else(|| anyhow!("header not a map"))?;
    let op = int(hdr, "op").unwrap_or(0);
    let t = text(hdr, "t").unwrap_or("");
    let kind = if op == -1 {
        Kind::Error
    } else {
        match t {
            "#commit" => Kind::Commit,
            "#sync" => Kind::Sync,
            "#identity" => Kind::Identity,
            "#account" => Kind::Account,
            "#info" => Kind::Info,
            _ => Kind::Other,
        }
    };
    let mut ev = Ev {
        side,
        kind,
        did: 0,
        rev: 0,
        cid: 0,
        seq: 0,
        recv_us,
        time_us: 0,
        size: frame.len() as u32,
        ops: 0,
        blocks: 0,
        too_big: false,
        has_handle: false,
    };
    if matches!(kind, Kind::Error | Kind::Info | Kind::Other) {
        return Ok((ev, None));
    }
    let body: Value = ciborium::de::from_reader(&mut cur).context("body")?;
    let body = body.as_map().ok_or_else(|| anyhow!("body not a map"))?;
    let did = text(body, "repo").or_else(|| text(body, "did")).unwrap_or("").to_string();
    ev.did = fnv(did.as_bytes());
    ev.seq = int(body, "seq").unwrap_or(0);
    ev.time_us = text(body, "time").map(parse_time_us).unwrap_or(0);
    if let Some(r) = text(body, "rev") {
        ev.rev = rev_key(r);
    }
    if let Some(Value::Tag(42, b)) = map_get(body, "commit")
        && let Value::Bytes(b) = b.as_ref()
    {
        ev.cid = fnv(b);
    }
    if let Some(Value::Bytes(b)) = map_get(body, "blocks") {
        ev.blocks = b.len() as u32;
    }
    if let Some(Value::Array(a)) = map_get(body, "ops") {
        ev.ops = a.len().min(u16::MAX as usize) as u16;
    }
    ev.has_handle = text(body, "handle").is_some();
    if let Some(Value::Bool(b)) = map_get(body, "tooBig") {
        ev.too_big = *b;
    }
    Ok((ev, Some(did)))
}

async fn run_socket(side: usize, url: String, clock: std::sync::Arc<Clock>, tx: mpsc::UnboundedSender<Msg>, mut stop: watch::Receiver<bool>) {
    let mut parse_errors = 0u64;
    loop {
        if *stop.borrow() {
            return;
        }
        let conn = tokio::time::timeout(Duration::from_secs(15), tokio_tungstenite::connect_async(url.as_str())).await;
        let mut ws = match conn {
            Ok(Ok((ws, _))) => ws,
            Ok(Err(e)) => {
                let _ = tx.send(Msg::Disconnect { side, at_us: clock.now_us(), why: format!("connect: {e}") });
                tokio::time::sleep(Duration::from_secs(5)).await;
                continue;
            }
            Err(_) => {
                let _ = tx.send(Msg::Disconnect { side, at_us: clock.now_us(), why: "connect timeout".into() });
                continue;
            }
        };
        eprintln!("connected side={side} {url}");
        let why = loop {
            tokio::select! {
                _ = stop.changed() => {
                    let _ = ws.close(None).await;
                    return;
                }
                m = tokio::time::timeout(Duration::from_secs(60), ws.next()) => match m {
                    Err(_) => break "no frames for 60 s".to_string(),
                    Ok(None) => break "closed".to_string(),
                    Ok(Some(Err(e))) => break format!("read: {e}"),
                    Ok(Some(Ok(Message::Binary(b)))) => {
                        let now = clock.now_us();
                        match parse_frame(side, &b, now) {
                            Ok((ev, did)) => { let _ = tx.send(Msg::Ev(ev, did)); }
                            Err(e) => {
                                parse_errors += 1;
                                if parse_errors <= 5 { eprintln!("side={side} parse error: {e:#}"); }
                            }
                        }
                    }
                    Ok(Some(Ok(_))) => {}
                }
            }
        };
        let _ = tx.send(Msg::Disconnect { side, at_us: clock.now_us(), why });
        tokio::time::sleep(Duration::from_secs(2)).await;
    }
}

fn hist() -> Histogram<u64> {
    Histogram::new_with_bounds(1, 24 * 3600 * 1_000_000, 3).unwrap()
}

fn pct(h: &Histogram<u64>) -> serde_json::Value {
    if h.is_empty() {
        return json!(null);
    }
    json!({
        "n": h.len(),
        "min": h.min(), "p50": h.value_at_quantile(0.5), "p90": h.value_at_quantile(0.9),
        "p99": h.value_at_quantile(0.99), "p999": h.value_at_quantile(0.999), "max": h.max(),
        "mean": h.mean().round(),
    })
}

/// Latencies can be negative: the relay's edge may be closer to us than the
/// PDS is, so these are exact percentiles over signed values.
struct Signed(Vec<i64>);

impl Signed {
    fn q(&self, q: f64) -> i64 {
        if self.0.is_empty() {
            return 0;
        }
        self.0[((self.0.len() - 1) as f64 * q).round() as usize]
    }
    fn line(&mut self) -> String {
        self.0.sort_unstable();
        let neg = self.0.iter().filter(|v| **v < 0).count();
        format!(
            "n={:<7} p50 {:>9} p90 {:>9} p99 {:>9} p99.9 {:>9} max {:>9} min {:>9} ({} negative)",
            self.0.len(),
            sms(self.q(0.5)),
            sms(self.q(0.9)),
            sms(self.q(0.99)),
            sms(self.q(0.999)),
            sms(self.q(1.0)),
            sms(self.q(0.0)),
            neg
        )
    }
    fn json(&mut self) -> serde_json::Value {
        self.0.sort_unstable();
        json!({"n": self.0.len(), "min": self.q(0.0), "p10": self.q(0.1), "p50": self.q(0.5), "p90": self.q(0.9),
            "p99": self.q(0.99), "p999": self.q(0.999), "max": self.q(1.0),
            "negative": self.0.iter().filter(|v| **v < 0).count()})
    }
}

fn sms(us: i64) -> String {
    format!("{:.1} ms", us as f64 / 1000.0)
}

fn ms(us: u64) -> String {
    format!("{:.1} ms", us as f64 / 1000.0)
}

#[tokio::main]
async fn main() -> Result<()> {
    let args = Args::parse();
    let _ = rustls::crypto::ring::default_provider().install_default();
    let clock = std::sync::Arc::new(Clock::new());
    let t0 = clock.now_us();

    let mut names = vec![args.relay.clone()];
    names.extend(args.pds.iter().cloned());
    let (tx, mut rx) = mpsc::unbounded_channel();
    let (stop_relay_tx, stop_relay) = watch::channel(false);
    let (stop_pds_tx, stop_pds) = watch::channel(false);
    let mut tasks = Vec::new();
    for (side, host) in names.iter().enumerate() {
        // a bare host is wss; ws:// or wss:// as given (a local relay)
        let origin = if host.contains("://") { host.clone() } else { format!("wss://{host}") };
        let url = format!("{}/xrpc/com.atproto.sync.subscribeRepos", origin.trim_end_matches('/'));
        let stop = if side == 0 { stop_relay.clone() } else { stop_pds.clone() };
        tasks.push(tokio::spawn(run_socket(side, url, clock.clone(), tx.clone(), stop)));
    }
    drop(tx);

    let pds_end = t0 + (args.secs as i64) * 1_000_000;
    let relay_end = pds_end + (args.grace_secs as i64) * 1_000_000;
    {
        let clock = clock.clone();
        tokio::spawn(async move {
            let wait = |until: i64| Duration::from_micros((until - clock.now_us()).max(0) as u64);
            tokio::select! {
                _ = tokio::time::sleep(wait(pds_end)) => {}
                _ = tokio::signal::ctrl_c() => { eprintln!("interrupted, stopping now"); }
            }
            let _ = stop_pds_tx.send(true);
            eprintln!("PDS sockets closed, reading the relay for the grace period");
            tokio::select! {
                _ = tokio::time::sleep(wait(relay_end)) => {}
                _ = tokio::signal::ctrl_c() => {}
            }
            let _ = stop_relay_tx.send(true);
        });
    }

    let mut evs: Vec<Ev> = Vec::with_capacity(1 << 20);
    let mut did_names: HashMap<u64, String> = HashMap::new();
    let mut disconnects: Vec<(usize, i64, String)> = Vec::new();
    let mut last_report = t0;
    let mut counts = vec![0u64; names.len()];
    while let Some(m) = rx.recv().await {
        match m {
            Msg::Ev(ev, did) => {
                counts[ev.side] += 1;
                if ev.side != 0
                    && let Some(d) = did
                {
                    did_names.entry(ev.did).or_insert(d);
                }
                if ev.recv_us - last_report > 30_000_000 {
                    last_report = ev.recv_us;
                    eprintln!("t+{}s events: {:?}", (ev.recv_us - t0) / 1_000_000, counts);
                }
                evs.push(ev);
            }
            Msg::Disconnect { side, at_us, why } => {
                eprintln!("side={side} ({}) disconnected at t+{}s: {why}", names[side], (at_us - t0) / 1_000_000);
                disconnects.push((side, at_us, why));
            }
        }
    }
    for t in tasks {
        let _ = t.await;
    }

    let report = analyze(&args, &names, &evs, &did_names, &disconnects, t0, pds_end);
    if let Some(out) = &args.out {
        std::fs::write(out, serde_json::to_vec_pretty(&report)?)?;
        eprintln!("wrote {out}");
    }
    Ok(())
}

fn analyze(
    args: &Args,
    names: &[String],
    evs: &[Ev],
    did_names: &HashMap<u64, String>,
    disconnects: &[(usize, i64, String)],
    t0: i64,
    pds_end: i64,
) -> serde_json::Value {
    let warm_end = t0 + (args.warmup_secs as i64) * 1_000_000;
    let relay: Vec<usize> = (0..evs.len()).filter(|i| evs[*i].side == 0).collect();

    // Relay stream shape: sizes, rates, seq continuity.
    let mut size_h: Vec<Histogram<u64>> = KINDS.iter().map(|_| hist()).collect();
    let mut bytes_by_kind = [0u64; 7];
    let mut ops_h = hist();
    let mut blocks_h = hist();
    let mut too_big = 0u64;
    let mut per_sec: HashMap<i64, [u64; 7]> = HashMap::new();
    let mut per_sec_bytes: HashMap<i64, u64> = HashMap::new();
    let (mut seq_gaps, mut seq_missing, mut seq_backwards) = (0u64, 0u64, 0u64);
    let mut last_seq = 0i64;
    let mut relay_age_h = hist();
    let mut identity_with_handle = 0u64;
    for &i in &relay {
        let e = &evs[i];
        identity_with_handle += (e.kind == Kind::Identity && e.has_handle) as u64;
        let k = e.kind.idx();
        size_h[k].record(e.size.max(1) as u64).ok();
        bytes_by_kind[k] += e.size as u64;
        per_sec.entry((e.recv_us - t0) / 1_000_000).or_default()[k] += 1;
        *per_sec_bytes.entry((e.recv_us - t0) / 1_000_000).or_default() += e.size as u64;
        if e.kind == Kind::Commit {
            ops_h.record(e.ops.max(1) as u64).ok();
            blocks_h.record(e.blocks.max(1) as u64).ok();
            too_big += e.too_big as u64;
        }
        if e.seq > 0 {
            if last_seq > 0 {
                if e.seq > last_seq + 1 {
                    seq_gaps += 1;
                    seq_missing += (e.seq - last_seq - 1) as u64;
                } else if e.seq <= last_seq {
                    seq_backwards += 1;
                }
            }
            last_seq = e.seq;
        }
        if e.time_us > 0 && e.recv_us > e.time_us {
            relay_age_h.record((e.recv_us - e.time_us) as u64).ok();
        }
    }
    // Only whole seconds inside the PDS window count toward rates.
    let first_sec = (warm_end - t0) / 1_000_000;
    let last_sec = (pds_end - t0) / 1_000_000 - 1;
    let secs: Vec<[u64; 7]> = (first_sec..=last_sec).map(|s| per_sec.get(&s).copied().unwrap_or_default()).collect();
    let mut total_rate_h = hist();
    for s in &secs {
        total_rate_h.record(s.iter().sum::<u64>().max(1)).ok();
    }
    let nsecs = secs.len().max(1) as f64;
    let total_relay = relay.len() as f64;
    let total_bytes: u64 = bytes_by_kind.iter().sum();
    let mut by_kind = serde_json::Map::new();
    println!("== relay stream: {} ==", names[0]);
    println!(
        "{} events, {:.1} MB, avg {:.0} B/event; {:.0} events/s and {:.2} MB/s over {} s",
        relay.len(),
        total_bytes as f64 / 1e6,
        total_bytes as f64 / total_relay.max(1.0),
        secs.iter().flatten().sum::<u64>() as f64 / nsecs,
        (first_sec..=last_sec).map(|s| per_sec_bytes.get(&s).copied().unwrap_or(0)).sum::<u64>() as f64 / 1e6 / nsecs,
        secs.len()
    );
    println!(
        "per-second total events: p50 {} p99 {} max {}",
        total_rate_h.value_at_quantile(0.5),
        total_rate_h.value_at_quantile(0.99),
        total_rate_h.max()
    );
    for k in KINDS {
        let h = &size_h[k.idx()];
        if h.is_empty() {
            continue;
        }
        let rate = secs.iter().map(|s| s[k.idx()]).sum::<u64>() as f64 / nsecs;
        println!(
            "  {:10} {:>9} events ({:5.2}%) {:8.1}/s  size p50 {:>6} p90 {:>6} p99 {:>7} max {:>8} mean {:>6.0} B  ({:.1}% of bytes)",
            k.name(),
            h.len(),
            100.0 * h.len() as f64 / total_relay.max(1.0),
            rate,
            h.value_at_quantile(0.5),
            h.value_at_quantile(0.9),
            h.value_at_quantile(0.99),
            h.max(),
            h.mean(),
            100.0 * bytes_by_kind[k.idx()] as f64 / (total_bytes.max(1)) as f64
        );
        by_kind.insert(k.name().into(), json!({"count": h.len(), "per_sec": rate, "bytes": bytes_by_kind[k.idx()], "size": pct(h)}));
    }
    if !ops_h.is_empty() {
        println!(
            "  #commit ops p50 {} p99 {} max {}; blocks p50 {} B p99 {} B max {} B; tooBig {}",
            ops_h.value_at_quantile(0.5),
            ops_h.value_at_quantile(0.99),
            ops_h.max(),
            blocks_h.value_at_quantile(0.5),
            blocks_h.value_at_quantile(0.99),
            blocks_h.max(),
            too_big
        );
    }
    println!("  #identity carrying a handle: {identity_with_handle} of {}", size_h[Kind::Identity.idx()].len());
    println!("  relay seq: {seq_gaps} gaps ({seq_missing} seqs skipped), {seq_backwards} backwards");
    if !relay_age_h.is_empty() {
        println!(
            "  relay recv - event `time`: p50 {} p90 {} p99 {} (clock skew applies)",
            ms(relay_age_h.value_at_quantile(0.5)),
            ms(relay_age_h.value_at_quantile(0.9)),
            ms(relay_age_h.value_at_quantile(0.99))
        );
    }

    // Index the relay's events for matching.
    let mut by_rev: HashMap<(u64, Kind, u64), Vec<usize>> = HashMap::new();
    let mut by_did: HashMap<(u64, Kind), Vec<usize>> = HashMap::new();
    for &i in &relay {
        let e = &evs[i];
        match e.kind {
            Kind::Commit | Kind::Sync => by_rev.entry((e.did, e.kind, e.rev)).or_default().push(i),
            Kind::Identity | Kind::Account => by_did.entry((e.did, e.kind)).or_default().push(i),
            _ => {}
        }
    }
    let mut matched: HashSet<usize> = HashSet::new();
    let mut hosts = Vec::new();
    let mut all_lat = Signed(Vec::new());
    let mut did_side: HashMap<u64, usize> = HashMap::new();
    for side in 1..names.len() {
        let pds: Vec<usize> = (0..evs.len()).filter(|i| evs[*i].side == side).collect();
        for &i in &pds {
            did_side.insert(evs[i].did, side);
        }
        let mut lat_h: HashMap<Kind, Signed> = HashMap::new();
        let mut missing: HashMap<Kind, Vec<usize>> = HashMap::new();
        let mut cid_mismatch = 0u64;
        let mut dup_at_relay = 0u64;
        let mut in_window = 0u64;
        let mut pds_age_h = hist();
        // #identity events matched at the relay: (PDS sent a handle, relay kept it)
        let (mut id_handle_pds, mut id_handle_kept) = (0u64, 0u64);
        // did -> (pds seq, relay seq) of matched events, for order checks
        let mut pairs: HashMap<u64, Vec<(i64, i64)>> = HashMap::new();
        for &i in &pds {
            let e = &evs[i];
            let counted = e.recv_us >= warm_end && e.recv_us < pds_end;
            if counted && matches!(e.kind, Kind::Commit | Kind::Sync | Kind::Identity | Kind::Account) {
                in_window += 1;
                if e.time_us > 0 && e.recv_us > e.time_us {
                    pds_age_h.record((e.recv_us - e.time_us) as u64).ok();
                }
            }
            let hit = match e.kind {
                Kind::Commit | Kind::Sync => by_rev.get(&(e.did, e.kind, e.rev)).and_then(|v| {
                    if v.len() > 1 && counted {
                        dup_at_relay += 1;
                    }
                    v.iter().copied().find(|j| !matched.contains(j))
                }),
                Kind::Identity | Kind::Account => by_did
                    .get(&(e.did, e.kind))
                    .and_then(|v| v.iter().copied().find(|j| !matched.contains(j) && evs[*j].recv_us >= e.recv_us - 2_000_000)),
                _ => continue,
            };
            match hit {
                Some(j) => {
                    matched.insert(j);
                    let r = &evs[j];
                    if e.kind == Kind::Identity && e.has_handle {
                        id_handle_pds += 1;
                        id_handle_kept += r.has_handle as u64;
                    }
                    if e.kind == Kind::Commit && r.cid != e.cid {
                        cid_mismatch += 1;
                    }
                    pairs.entry(e.did).or_default().push((e.seq, r.seq));
                    if counted {
                        let d = r.recv_us - e.recv_us;
                        lat_h.entry(e.kind).or_insert_with(|| Signed(Vec::new())).0.push(d);
                        all_lat.0.push(d);
                    }
                }
                None if counted => missing.entry(e.kind).or_default().push(i),
                None => {}
            }
        }
        let mut order_dids = 0u64;
        let mut order_pairs = 0u64;
        for v in pairs.values_mut() {
            v.sort();
            let inv = v.windows(2).filter(|w| w[1].1 < w[0].1).count() as u64;
            if inv > 0 {
                order_dids += 1;
                order_pairs += inv;
            }
        }
        let host = &names[side];
        println!("\n== {host} (PDS) vs relay ==");
        println!(
            "{} PDS events in window ({} DIDs seen); pds recv - event `time` p50 {} p99 {}",
            in_window,
            pds.iter().map(|i| evs[*i].did).collect::<HashSet<_>>().len(),
            ms(pds_age_h.value_at_quantile(0.5)),
            ms(pds_age_h.value_at_quantile(0.99))
        );
        let mut lat_json = serde_json::Map::new();
        for k in [Kind::Commit, Kind::Sync, Kind::Identity, Kind::Account] {
            if let Some(h) = lat_h.get_mut(&k) {
                println!("  {:10} PDS→relay {}", k.name(), h.line());
                lat_json.insert(k.name().into(), h.json());
            }
        }
        let mut missing_json = serde_json::Map::new();
        for (k, v) in &missing {
            let ex: Vec<String> = v
                .iter()
                .take(args.examples)
                .map(|i| format!("{} seq={} t+{}s", did_names.get(&evs[*i].did).cloned().unwrap_or_default(), evs[*i].seq, (evs[*i].recv_us - t0) / 1_000_000))
                .collect();
            println!("  missing at relay: {} {}  e.g. {:?}", v.len(), k.name(), &ex[..ex.len().min(3)]);
            missing_json.insert(k.name().into(), json!({"count": v.len(), "examples": ex}));
        }
        println!("  commit cid mismatches: {cid_mismatch}; relay duplicates: {dup_at_relay}");
        println!("  #identity with a handle at the PDS: {id_handle_pds}, still carrying it at the relay: {id_handle_kept}");
        println!("  per-DID order: {order_dids} DIDs with relay order != PDS order ({order_pairs} inverted adjacent pairs)");
        hosts.push(json!({
            "host": host,
            "events_in_window": in_window,
            "latency_us": lat_json,
            "missing_at_relay": missing_json,
            "commit_cid_mismatch": cid_mismatch,
            "relay_duplicates": dup_at_relay,
            "order_inverted_dids": order_dids,
            "order_inverted_pairs": order_pairs,
            "pds_age_us": pct(&pds_age_h),
            "identity_handle_at_pds": id_handle_pds,
            "identity_handle_kept_by_relay": id_handle_kept,
        }));
    }

    // Relay events for DIDs we watched on a PDS that no PDS event explains.
    let relay_only_from = warm_end + 10_000_000;
    let mut relay_only: HashMap<(usize, Kind), Vec<usize>> = HashMap::new();
    for &i in &relay {
        let e = &evs[i];
        if e.recv_us < relay_only_from || e.recv_us >= pds_end || matched.contains(&i) {
            continue;
        }
        if let Some(&side) = did_side.get(&e.did)
            && matches!(e.kind, Kind::Commit | Kind::Sync | Kind::Identity | Kind::Account)
        {
            relay_only.entry((side, e.kind)).or_default().push(i);
        }
    }
    let mut relay_only_json = Vec::new();
    if relay_only.is_empty() {
        println!("relay-only events for watched DIDs: 0");
    }
    for ((side, k), v) in &relay_only {
        let ex: Vec<String> = v
            .iter()
            .take(args.examples)
            .map(|i| format!("{} relay seq={} t+{}s", did_names.get(&evs[*i].did).cloned().unwrap_or_default(), evs[*i].seq, (evs[*i].recv_us - t0) / 1_000_000))
            .collect();
        println!("relay-only for {} DIDs: {} {}  e.g. {:?}", names[*side], v.len(), k.name(), &ex[..ex.len().min(3)]);
        relay_only_json.push(json!({"host": names[*side], "kind": k.name(), "count": v.len(), "examples": ex}));
    }
    println!("\nall hosts PDS→relay: {}", all_lat.line());
    println!("disconnects: {}", disconnects.len());
    json!({
        "relay": names[0],
        "pds": &names[1..],
        "started_unix_us": t0,
        "window_secs": (pds_end - warm_end) / 1_000_000,
        "relay_stream": {
            "events": relay.len(),
            "bytes": total_bytes,
            "events_per_sec_mean": secs.iter().flatten().sum::<u64>() as f64 / nsecs,
            "events_per_sec": pct(&total_rate_h),
            "by_kind": by_kind,
            "commit_ops": pct(&ops_h),
            "commit_blocks_bytes": pct(&blocks_h),
            "too_big": too_big,
            "identity_with_handle": identity_with_handle,
            "seq_gaps": seq_gaps, "seq_missing": seq_missing, "seq_backwards": seq_backwards,
            "recv_minus_time_us": pct(&relay_age_h),
        },
        "hosts": hosts,
        "relay_only": relay_only_json,
        "latency_all_us": all_lat.json(),
        "disconnects": disconnects.iter().map(|(s, at, why)| json!({"host": names[*s], "t_s": (at - t0) / 1_000_000, "why": why})).collect::<Vec<_>>(),
    })
}
