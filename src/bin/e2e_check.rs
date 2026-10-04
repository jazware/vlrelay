//! e2e_check: compares a relay's `subscribeRepos` with its upstreams' own.
//!
//!   e2e_check --upstream http://127.0.0.1:2984 --upstream http://localhost:2983 \
//!             --relay http://127.0.0.1:2980 --duration 60
//!
//! Every event is keyed by DID plus what identifies it across a relay:
//! `#commit`/`#sync` by (rev, commit CID); `#identity` by handle and
//! `#account` by (active, status), each with its occurrence count, because
//! those carry no rev. Both sides are timestamped on receipt here, so the
//! latency (upstream emit -> relay emit) needs no clock agreement with
//! either server. Upstream events seen in [warmup, duration] are expected on
//! the relay; the relay gets `settle` more seconds to deliver them.
//!
//! `--relay` is repeatable and the relay side is the union of its streams,
//! so `--upstream A --upstream B --relay A --relay B` checks the checker.

use clap::Parser;
use futures::{SinkExt, StreamExt};
use hdrhistogram::Histogram;
use std::collections::HashMap;
use std::time::{Duration, Instant};
use tokio::sync::mpsc;
use tokio_tungstenite::tungstenite::Message;
use vlpds::cbor::ValueRef;

#[derive(Parser, Debug)]
struct Args {
    /// An upstream PDS (http(s)/ws(s) origin, bare hostname = wss, or a full
    /// subscribeRepos URL). Repeatable.
    #[arg(long = "upstream", required = true)]
    upstreams: Vec<String>,
    /// A relay to compare against the upstreams, same forms. Repeatable (the
    /// relay side is the union). None: only the upstreams are watched.
    #[arg(long = "relay")]
    relays: Vec<String>,
    /// Seconds of upstream events to expect on the relay.
    #[arg(long, default_value_t = 30)]
    duration: u64,
    /// Seconds at the start whose events are neither expected nor extra
    /// (in flight when the sockets opened).
    #[arg(long, default_value_t = 2)]
    warmup: u64,
    /// Seconds after `duration` the relay gets to deliver the last events.
    #[arg(long, default_value_t = 10)]
    settle: u64,
    /// all: every relay event is in scope (a relay that crawls only these
    /// upstreams). hosted: only DIDs the upstreams host (listRepos at start,
    /// plus any DID they emit), for a relay carrying a whole network. seen:
    /// like hosted without the listRepos (a big PDS takes minutes to list),
    /// so a DID counts from its first upstream event on.
    #[arg(long, default_value = "all")]
    scope: String,
    /// Write the report as JSON here.
    #[arg(long)]
    json_out: Option<String>,
    /// Exit 0 even when events are missing, extra or out of order.
    #[arg(long)]
    report_only: bool,
    #[arg(long, default_value_t = 5)]
    report_secs: u64,
    /// Examples of each kind of discrepancy to print.
    #[arg(long, default_value_t = 10)]
    show: usize,
    /// Write every relay event as `seq did key` lines, in arrival order, to
    /// compare streams from different nodes of one cluster.
    #[arg(long)]
    seq_out: Option<String>,
    /// Write `unix_ms latency_ms` per matched event (at the later side's
    /// arrival), to find the pauses around a failover.
    #[arg(long)]
    lat_out: Option<String>,
}

#[derive(Clone, Copy, PartialEq, Eq, Debug)]
enum Side {
    Up,
    Relay,
}

#[derive(Clone, Copy, PartialEq, Eq, Hash, Debug, PartialOrd, Ord)]
enum Kind {
    Commit,
    Sync,
    Identity,
    Account,
}

impl Kind {
    fn name(self) -> &'static str {
        match self {
            Kind::Commit => "#commit",
            Kind::Sync => "#sync",
            Kind::Identity => "#identity",
            Kind::Account => "#account",
        }
    }
    const ALL: [Kind; 4] = [Kind::Commit, Kind::Sync, Kind::Identity, Kind::Account];
}

enum Msg {
    Event {
        side: Side,
        src: usize,
        did: String,
        kind: Kind,
        base: String,
        rev: Option<String>,
        seq: i64,
        at: Instant,
    },
    Other {
        side: Side,
        src: usize,
        what: String,
    },
    Status {
        side: Side,
        src: usize,
        what: String,
    },
}

fn subscribe_url(s: &str) -> String {
    let s = s.trim().trim_end_matches('/');
    let s = if let Some(r) = s.strip_prefix("https://") {
        format!("wss://{r}")
    } else if let Some(r) = s.strip_prefix("http://") {
        format!("ws://{r}")
    } else if s.starts_with("ws://") || s.starts_with("wss://") {
        s.to_string()
    } else {
        format!("wss://{s}")
    };
    if s.contains("/xrpc/") {
        s
    } else {
        format!("{s}/xrpc/com.atproto.sync.subscribeRepos")
    }
}

fn http_origin(s: &str) -> String {
    let u = subscribe_url(s);
    let u = u.split("/xrpc/").next().unwrap_or(&u).to_string();
    u.replacen("wss://", "https://", 1)
        .replacen("ws://", "http://", 1)
}

/// What identifies an event across a relay, or None for frames that don't
/// take part (#info, unknown types).
/// (did, kind, cross-relay key, rev, seq)
type Classified = (String, Kind, String, Option<String>, i64);

fn classify(frame: &[u8]) -> Result<Option<Classified>, String> {
    let (hdr, n) = ValueRef::decode_prefix(frame).map_err(|e| format!("header: {e}"))?;
    let op = match hdr.get("op") {
        Some(ValueRef::Int(i)) => *i,
        _ => return Err("header without op".into()),
    };
    let body = ValueRef::decode(&frame[n..]).map_err(|e| format!("body: {e}"))?;
    if op != 1 {
        let err = body.get("error").and_then(|v| v.as_str()).unwrap_or("?");
        let msg = body.get("message").and_then(|v| v.as_str()).unwrap_or("");
        return Err(format!("error frame: {err} {msg}"));
    }
    let t = hdr.get("t").and_then(|v| v.as_str()).unwrap_or("");
    let seq = match body.get("seq") {
        Some(ValueRef::Int(i)) => *i,
        _ => 0,
    };
    let s = |k: &str| body.get(k).and_then(|v| v.as_str()).map(str::to_string);
    let Some(did) = s("repo").or_else(|| s("did")) else {
        return Ok(None);
    };
    Ok(Some(match t {
        "#commit" => {
            let rev = s("rev").unwrap_or_default();
            let cid = match body.get("commit") {
                Some(ValueRef::Link(c)) => c.to_string(),
                _ => "-".into(),
            };
            (did, Kind::Commit, format!("c:{rev}:{cid}"), Some(rev), seq)
        }
        "#sync" => {
            let rev = s("rev").unwrap_or_default();
            let root = match body.get("blocks") {
                Some(ValueRef::Bytes(b)) => vlpds::car::read_car(b)
                    .ok()
                    .and_then(|(roots, _)| roots.first().map(|c| c.to_string()))
                    .unwrap_or_else(|| "-".into()),
                _ => "-".into(),
            };
            (did, Kind::Sync, format!("s:{rev}:{root}"), Some(rev), seq)
        }
        "#identity" => {
            let handle = s("handle").unwrap_or_else(|| "-".into());
            (did, Kind::Identity, format!("i:{handle}"), None, seq)
        }
        "#account" => {
            let active = matches!(body.get("active"), Some(ValueRef::Bool(true)));
            let status = s("status").unwrap_or_else(|| "-".into());
            (
                did,
                Kind::Account,
                format!("a:{active}:{status}"),
                None,
                seq,
            )
        }
        _ => return Ok(None),
    }))
}

/// `base` may list several servers separated by commas (nodes of one
/// cluster): a socket that closes or can't connect moves to the next one,
/// resuming from its cursor.
async fn subscribe(side: Side, src: usize, base: String, tx: mpsc::UnboundedSender<Msg>) {
    let urls: Vec<String> = base.split(',').map(subscribe_url).collect();
    let mut at = 0usize;
    let mut cursor: Option<i64> = None;
    let mut backoff = Duration::from_millis(250);
    loop {
        let url = urls[at % urls.len()].clone();
        let u = match cursor {
            Some(c) => format!("{url}?cursor={c}"),
            None => url.clone(),
        };
        let tls = tokio_tungstenite::Connector::Rustls(tls_config());
        match tokio_tungstenite::connect_async_tls_with_config(&u, None, false, Some(tls)).await {
            Ok((mut ws, _)) => {
                let _ = tx.send(Msg::Status {
                    side,
                    src,
                    what: format!("connected {u}"),
                });
                backoff = Duration::from_millis(250);
                while let Some(m) = ws.next().await {
                    let at = Instant::now();
                    let b = match m {
                        Ok(Message::Binary(b)) => b,
                        Ok(Message::Ping(p)) => {
                            let _ = ws.send(Message::Pong(p)).await;
                            continue;
                        }
                        Ok(Message::Close(f)) => {
                            let _ = tx.send(Msg::Status {
                                side,
                                src,
                                what: format!("closed: {f:?}"),
                            });
                            break;
                        }
                        Ok(_) => continue,
                        Err(e) => {
                            let _ = tx.send(Msg::Status {
                                side,
                                src,
                                what: format!("read error: {e}"),
                            });
                            break;
                        }
                    };
                    match classify(&b) {
                        Ok(Some((did, kind, base, rev, seq))) => {
                            if seq > 0 {
                                cursor = Some(seq);
                            }
                            if tx
                                .send(Msg::Event {
                                    side,
                                    src,
                                    did,
                                    kind,
                                    base,
                                    rev,
                                    seq,
                                    at,
                                })
                                .is_err()
                            {
                                return;
                            }
                        }
                        Ok(None) => {}
                        Err(e) => {
                            let _ = tx.send(Msg::Other { side, src, what: e });
                        }
                    }
                }
            }
            Err(e) => {
                let _ = tx.send(Msg::Status {
                    side,
                    src,
                    what: format!("connect {u}: {e}"),
                });
            }
        }
        if urls.len() > 1 {
            at += 1;
            tokio::time::sleep(Duration::from_millis(50)).await;
            continue;
        }
        tokio::time::sleep(backoff).await;
        backoff = (backoff * 2).min(Duration::from_secs(5));
    }
}

/// Every DID an upstream hosts (com.atproto.sync.listRepos), for --scope hosted.
async fn list_repos(origin: &str) -> anyhow::Result<Vec<String>> {
    let c = reqwest::Client::builder()
        .timeout(Duration::from_secs(30))
        .build()?;
    let mut out = Vec::new();
    let mut cursor: Option<String> = None;
    loop {
        let mut u = format!("{origin}/xrpc/com.atproto.sync.listRepos?limit=1000");
        if let Some(c) = &cursor {
            u.push_str(&format!("&cursor={c}"));
        }
        let r: serde_json::Value = c.get(&u).send().await?.error_for_status()?.json().await?;
        for repo in r["repos"].as_array().into_iter().flatten() {
            if let Some(d) = repo["did"].as_str() {
                out.push(d.to_string());
            }
        }
        match r["cursor"].as_str() {
            Some(c) if !r["repos"].as_array().is_none_or(|a| a.is_empty()) => {
                cursor = Some(c.to_string())
            }
            _ => return Ok(out),
        }
    }
}

struct Waiting {
    side: Side,
    at: Instant,
    kind: Kind,
    /// The event's position in its DID's upstream stream (Up side only).
    pos: u64,
}

#[derive(Default)]
struct DidState {
    up_pos: u64,
    up_occ: HashMap<String, u32>,
    relay_occ: HashMap<String, u32>,
    relay_max_pos: Option<u64>,
    relay_last_rev: Option<String>,
}

#[derive(Default, Clone, Copy)]
struct KindCounts {
    up: u64,
    relay: u64,
    matched: u64,
    missing: u64,
    extra: u64,
}

fn hist() -> Histogram<u64> {
    Histogram::new_with_bounds(1, 600_000_000, 3).unwrap()
}

fn ms(us: u64) -> f64 {
    us as f64 / 1000.0
}

fn rustls_provider() {
    let _ = rustls::crypto::ring::default_provider().install_default();
}

/// webpki roots set here rather than through a tungstenite feature: a
/// feature change would rebuild tungstenite and, behind it, vlpds.
fn tls_config() -> std::sync::Arc<rustls::ClientConfig> {
    static CFG: std::sync::OnceLock<std::sync::Arc<rustls::ClientConfig>> =
        std::sync::OnceLock::new();
    CFG.get_or_init(|| {
        let roots = rustls::RootCertStore {
            roots: webpki_roots::TLS_SERVER_ROOTS.to_vec(),
        };
        std::sync::Arc::new(
            rustls::ClientConfig::builder()
                .with_root_certificates(roots)
                .with_no_client_auth(),
        )
    })
    .clone()
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    rustls_provider();
    let args = Args::parse();
    let hosted = match args.scope.as_str() {
        "all" => false,
        "hosted" | "seen" => true,
        s => anyhow::bail!("--scope {s}: want all, hosted or seen"),
    };
    let mut in_scope: std::collections::HashSet<String> = Default::default();
    if args.scope == "hosted" {
        for u in &args.upstreams {
            let dids = list_repos(&http_origin(u)).await?;
            eprintln!("e2e_check: {} hosts {} repos", http_origin(u), dids.len());
            in_scope.extend(dids);
        }
    }

    let (tx, mut rx) = mpsc::unbounded_channel();
    for (i, u) in args.upstreams.iter().enumerate() {
        tokio::spawn(subscribe(Side::Up, i, u.clone(), tx.clone()));
    }
    for (i, u) in args.relays.iter().enumerate() {
        tokio::spawn(subscribe(Side::Relay, i, u.clone(), tx.clone()));
    }
    drop(tx);
    let relay_on = !args.relays.is_empty();
    let name = |side: Side, src: usize| match side {
        Side::Up => format!("upstream {}", args.upstreams[src]),
        Side::Relay => format!("relay {}", args.relays[src]),
    };

    let start = Instant::now();
    let start_ms = std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH)?.as_millis();
    let warm_end = start + Duration::from_secs(args.warmup);
    let collect_end = start + Duration::from_secs(args.duration);
    let end = collect_end
        + if relay_on {
            Duration::from_secs(args.settle)
        } else {
            Duration::ZERO
        };

    let mut dids: HashMap<String, DidState> = HashMap::new();
    let mut waiting: HashMap<(String, String), Waiting> = HashMap::new();
    let mut counts: HashMap<Kind, KindCounts> = HashMap::new();
    let mut lat = hist();
    let mut lat_kind: HashMap<Kind, Histogram<u64>> = HashMap::new();
    let mut last_seq: HashMap<(bool, usize), i64> = HashMap::new();
    let mut seq_regressions = 0u64;
    let mut duplicates = 0u64;
    let mut out_of_order = 0u64;
    let mut rev_regressions = 0u64;
    let mut relay_first = 0u64;
    let mut out_of_scope = 0u64;
    let mut bad_frames = 0u64;
    let mut examples: HashMap<&'static str, Vec<String>> = HashMap::new();
    let mut note = |what: &'static str, s: String, show: usize| {
        let v = examples.entry(what).or_default();
        if v.len() < show {
            v.push(s);
        }
    };

    use std::io::Write;
    let open = |p: &Option<String>| -> anyhow::Result<Option<std::io::BufWriter<std::fs::File>>> {
        Ok(match p {
            Some(p) => Some(std::io::BufWriter::new(std::fs::File::create(p)?)),
            None => None,
        })
    };
    let mut seq_out = open(&args.seq_out)?;
    let mut lat_out = open(&args.lat_out)?;
    let mut tick = tokio::time::interval(Duration::from_secs(args.report_secs.max(1)));
    tick.tick().await;
    let deadline = tokio::time::sleep_until(end.into());
    tokio::pin!(deadline);
    let (mut last_up, mut last_relay, mut last_t) = (0u64, 0u64, Instant::now());
    loop {
        let msg = tokio::select! {
            _ = &mut deadline => break,
            _ = tick.tick() => {
                let up: u64 = counts.values().map(|c| c.up).sum();
                let re: u64 = counts.values().map(|c| c.relay).sum();
                let dt = last_t.elapsed().as_secs_f64();
                let pending = waiting.values().filter(|w| w.side == Side::Up).count();
                eprintln!(
                    "e2e_check: t={:>4.0}s upstream {up} ({:.0}/s) relay {re} ({:.0}/s) awaiting relay {pending} p50 {:.1} ms p99 {:.1} ms",
                    start.elapsed().as_secs_f64(), (up - last_up) as f64 / dt, (re - last_relay) as f64 / dt,
                    ms(lat.value_at_quantile(0.5)), ms(lat.value_at_quantile(0.99)),
                );
                (last_up, last_relay, last_t) = (up, re, Instant::now());
                continue;
            }
            m = rx.recv() => match m { Some(m) => m, None => break },
        };
        let (side, src, did, kind, base, rev, seq, at) = match msg {
            Msg::Status { side, src, what } => {
                eprintln!("e2e_check: {}: {what}", name(side, src));
                continue;
            }
            Msg::Other { side, src, what } => {
                bad_frames += 1;
                note(
                    "bad frames",
                    format!("{}: {what}", name(side, src)),
                    args.show,
                );
                continue;
            }
            Msg::Event {
                side,
                src,
                did,
                kind,
                base,
                rev,
                seq,
                at,
            } => (side, src, did, kind, base, rev, seq, at),
        };
        if seq > 0 {
            let k = (side == Side::Up, src);
            if let Some(&prev) = last_seq.get(&k)
                && seq <= prev
            {
                seq_regressions += 1;
                note(
                    "seq regressions",
                    format!("{}: seq {seq} after {prev}", name(side, src)),
                    args.show,
                );
            }
            last_seq.insert(k, seq);
        }
        match side {
            Side::Up => {
                if hosted {
                    in_scope.insert(did.clone());
                }
            }
            Side::Relay => {
                if hosted && !in_scope.contains(&did) {
                    out_of_scope += 1;
                    continue;
                }
            }
        }
        let st = dids.entry(did.clone()).or_default();
        let occ_map = if side == Side::Up {
            &mut st.up_occ
        } else {
            &mut st.relay_occ
        };
        let occ = occ_map.entry(base.clone()).or_insert(0);
        let seen_before = *occ > 0;
        *occ += 1;
        if seen_before && matches!(kind, Kind::Commit | Kind::Sync) {
            if side == Side::Relay {
                duplicates += 1;
                note("duplicates", format!("{did} {base}"), args.show);
            }
            continue;
        }
        let key = if matches!(kind, Kind::Commit | Kind::Sync) {
            base.clone()
        } else {
            format!("{base}#{}", *occ - 1)
        };
        let c = counts.entry(kind).or_default();
        match side {
            Side::Up => c.up += 1,
            Side::Relay => c.relay += 1,
        }
        if side == Side::Relay
            && let Some(rev) = &rev
        {
            if let Some(prev) = &st.relay_last_rev {
                // a #sync restates the current rev (e.g. on reactivation)
                if rev < prev || (rev == prev && kind == Kind::Commit) {
                    rev_regressions += 1;
                    note(
                        "rev regressions",
                        format!("{did}: rev {rev} after {prev}"),
                        args.show,
                    );
                }
            }
            st.relay_last_rev = Some(rev.clone());
        }
        let pos = if side == Side::Up {
            st.up_pos += 1;
            st.up_pos
        } else {
            0
        };
        if !relay_on {
            continue;
        }
        if side == Side::Relay
            && let Some(w) = seq_out.as_mut()
        {
            writeln!(w, "{seq} {did} {key}")?;
        }
        let wk = (did.clone(), key);
        match waiting.remove(&wk) {
            Some(w) if w.side != side => {
                let (up_at, up_pos) = if side == Side::Up {
                    (at, pos)
                } else {
                    (w.at, w.pos)
                };
                let re_at = if side == Side::Up { w.at } else { at };
                if up_at >= warm_end {
                    let us = if re_at >= up_at {
                        re_at.duration_since(up_at).as_micros() as u64
                    } else {
                        relay_first += 1;
                        0
                    };
                    lat.saturating_record(us.max(1));
                    if let Some(w) = lat_out.as_mut() {
                        let t = start_ms + re_at.max(up_at).duration_since(start).as_millis();
                        writeln!(w, "{t} {:.1}", us as f64 / 1000.0)?;
                    }
                    lat_kind
                        .entry(kind)
                        .or_insert_with(hist)
                        .saturating_record(us.max(1));
                }
                counts.entry(kind).or_default().matched += 1;
                if side == Side::Relay {
                    if st.relay_max_pos.is_some_and(|m| up_pos < m) {
                        out_of_order += 1;
                        note("out of order", format!("{did} {}", wk.1), args.show);
                    }
                    st.relay_max_pos = Some(st.relay_max_pos.map_or(up_pos, |m| m.max(up_pos)));
                }
            }
            Some(w) => {
                // the same side again (two relay streams carrying one event)
                waiting.insert(wk, w);
            }
            None => {
                waiting.insert(
                    wk,
                    Waiting {
                        side,
                        at,
                        kind,
                        pos,
                    },
                );
            }
        }
    }

    for w in [seq_out.as_mut(), lat_out.as_mut()].into_iter().flatten() {
        w.flush()?;
    }
    for ((did, key), w) in &waiting {
        let c = counts.entry(w.kind).or_default();
        match w.side {
            Side::Up if w.at >= warm_end && w.at <= collect_end => {
                c.missing += 1;
                note("missing", format!("{did} {key}"), args.show);
            }
            Side::Relay if w.at >= warm_end => {
                c.extra += 1;
                note("extra", format!("{did} {key}"), args.show);
            }
            _ => {}
        }
    }

    let tot = |f: fn(&KindCounts) -> u64| counts.values().map(f).sum::<u64>();
    let (missing, extra) = (tot(|c| c.missing), tot(|c| c.extra));
    let commit_extra = counts.get(&Kind::Commit).map_or(0, |c| c.extra)
        + counts.get(&Kind::Sync).map_or(0, |c| c.extra);
    let secs = args.duration as f64;
    println!(
        "e2e_check: {} upstream(s), {} relay stream(s), {secs:.0}s + {}s settle",
        args.upstreams.len(),
        args.relays.len(),
        if relay_on { args.settle } else { 0 }
    );
    println!(
        "  {:<10} {:>9} {:>9} {:>9} {:>8} {:>8}",
        "kind", "upstream", "relay", "matched", "missing", "extra"
    );
    for k in Kind::ALL {
        let c = counts.get(&k).copied().unwrap_or_default();
        println!(
            "  {:<10} {:>9} {:>9} {:>9} {:>8} {:>8}",
            k.name(),
            c.up,
            c.relay,
            c.matched,
            c.missing,
            c.extra
        );
    }
    println!(
        "  upstream rate {:.0} ev/s, DIDs {}",
        tot(|c| c.up) as f64 / secs,
        dids.len()
    );
    if relay_on {
        println!(
            "  latency upstream->relay: p50 {:.2} ms  p90 {:.2} ms  p99 {:.2} ms  max {:.2} ms  (n={}, relay-first {relay_first})",
            ms(lat.value_at_quantile(0.5)),
            ms(lat.value_at_quantile(0.9)),
            ms(lat.value_at_quantile(0.99)),
            ms(lat.max()),
            lat.len()
        );
        println!(
            "  out of order per DID {out_of_order}, rev regressions {rev_regressions}, duplicates {duplicates}, seq regressions {seq_regressions}, out of scope {out_of_scope}, bad frames {bad_frames}"
        );
    } else {
        println!(
            "  (no --relay: upstreams only) seq regressions {seq_regressions}, bad frames {bad_frames}"
        );
    }
    let mut keys: Vec<_> = examples.keys().copied().collect();
    keys.sort();
    for k in keys {
        println!("  {k}:");
        for e in &examples[k] {
            println!("    {e}");
        }
    }

    if let Some(path) = &args.json_out {
        let per_kind: serde_json::Map<String, serde_json::Value> = Kind::ALL
            .iter()
            .map(|k| {
                let c = counts.get(k).copied().unwrap_or_default();
                let l = lat_kind.get(k);
                (
                    k.name().to_string(),
                    serde_json::json!({
                        "upstream": c.up, "relay": c.relay, "matched": c.matched, "missing": c.missing, "extra": c.extra,
                        "p50_ms": l.map(|h| ms(h.value_at_quantile(0.5))), "p99_ms": l.map(|h| ms(h.value_at_quantile(0.99))),
                    }),
                )
            })
            .collect();
        let j = serde_json::json!({
            "upstreams": args.upstreams, "relays": args.relays, "duration_s": args.duration, "settle_s": args.settle,
            "kinds": per_kind, "missing": missing, "extra": extra,
            "out_of_order": out_of_order, "rev_regressions": rev_regressions, "duplicates": duplicates,
            "seq_regressions": seq_regressions, "bad_frames": bad_frames, "relay_first": relay_first, "out_of_scope": out_of_scope,
            "latency_ms": {"p50": ms(lat.value_at_quantile(0.5)), "p90": ms(lat.value_at_quantile(0.9)),
                           "p99": ms(lat.value_at_quantile(0.99)), "max": ms(lat.max()), "n": lat.len()},
            "examples": examples,
        });
        std::fs::write(path, serde_json::to_vec_pretty(&j)?)?;
    }

    let failed = relay_on
        && (missing > 0
            || commit_extra > 0
            || out_of_order > 0
            || rev_regressions > 0
            || duplicates > 0);
    let empty = tot(|c| c.up) == 0;
    if empty {
        eprintln!("e2e_check: no upstream events at all (is anything writing?)");
    }
    if (failed || empty || seq_regressions > 0) && !args.report_only {
        std::process::exit(1);
    }
    Ok(())
}
