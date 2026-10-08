//! e2e_check: compares a relay's `subscribeRepos` with its upstreams' own.
//!
//!   e2e_check --upstream http://127.0.0.1:2984 --upstream http://localhost:2983 \
//!             --relay http://127.0.0.1:2980 --duration 60
//!
//! Every event is keyed by DID plus what identifies it across a relay:
//! `#commit`/`#sync` by (rev, commit CID); `#identity` by handle and
//! `#account` by (active, status), plus the frame's `time` (or else their
//! occurrence count), because those carry no rev. Both sides are timestamped on receipt here, so the
//! latency (upstream emit -> relay emit) needs no clock agreement with
//! either server. Upstream events seen in [warmup, duration] are expected on
//! the relay; the relay gets `settle` more seconds to deliver them.
//!
//! `--relay` is repeatable and the relay side is the union of its streams,
//! so `--upstream A --upstream B --relay A --relay B` checks the checker.
//! `--separate` instead compares each relay with the same upstream sockets,
//! so two relays are measured under the same conditions with one socket per
//! upstream.

use clap::Parser;
use futures::{SinkExt, StreamExt};
use hdrhistogram::Histogram;
use std::collections::{HashMap, HashSet};
use std::time::{Duration, Instant};
use tokio::sync::mpsc;
use tokio_tungstenite::tungstenite::Message;
use vlsync_atproto::cbor::ValueRef;

#[derive(Parser, Debug)]
struct Args {
    /// An upstream PDS (http(s)/ws(s) origin, bare hostname = wss, or a full
    /// subscribeRepos URL). Repeatable.
    #[arg(long = "upstream", required = true)]
    upstreams: Vec<String>,
    /// A relay to compare against the upstreams, same forms. Repeatable (the
    /// relay side is the union, unless --separate). None: only the upstreams
    /// are watched.
    #[arg(long = "relay")]
    relays: Vec<String>,
    /// Compare each --relay with the upstreams on its own, over one set of
    /// upstream sockets, and report each (JSON: `{"comparisons": [...]}`).
    #[arg(long)]
    separate: bool,
    /// With --separate: the scope of the n-th --relay (default --scope).
    #[arg(long = "relay-scope")]
    relay_scopes: Vec<String>,
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
    /// The upstream restarted its sequence (FutureCursor): its seqs start over.
    Restarted {
        src: usize,
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
    if s.contains("/xrpc/") { s } else { format!("{s}/xrpc/com.atproto.sync.subscribeRepos") }
}

fn http_origin(s: &str) -> String {
    let u = subscribe_url(s);
    let u = u.split("/xrpc/").next().unwrap_or(&u).to_string();
    u.replacen("wss://", "https://", 1).replacen("ws://", "http://", 1)
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
                Some(ValueRef::Bytes(b)) => vlsync_atproto::car::read_car(b)
                    .ok()
                    .and_then(|(roots, _)| roots.first().map(|c| c.to_string()))
                    .unwrap_or_else(|| "-".into()),
                _ => "-".into(),
            };
            (did, Kind::Sync, format!("s:{rev}:{root}"), Some(rev), seq)
        }
        // An event's `time` names it across a relay: counting occurrences
        // instead pairs the wrong copies when the two sides' streams start
        // a few events apart.
        "#identity" => {
            let handle = s("handle").unwrap_or_else(|| "-".into());
            let at = s("time").map(|t| format!("@{t}")).unwrap_or_default();
            (did, Kind::Identity, format!("i:{handle}{at}"), None, seq)
        }
        "#account" => {
            let active = matches!(body.get("active"), Some(ValueRef::Bool(true)));
            let status = s("status").unwrap_or_else(|| "-".into());
            let at = s("time").map(|t| format!("@{t}")).unwrap_or_default();
            (did, Kind::Account, format!("a:{active}:{status}{at}"), None, seq)
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
                let _ = tx.send(Msg::Status { side, src, what: format!("connected {u}") });
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
                            let _ = tx.send(Msg::Status { side, src, what: format!("closed: {f:?}") });
                            break;
                        }
                        Ok(_) => continue,
                        Err(e) => {
                            let _ = tx.send(Msg::Status { side, src, what: format!("read error: {e}") });
                            break;
                        }
                    };
                    match classify(&b) {
                        Ok(Some((did, kind, base, rev, seq))) => {
                            if seq > 0 {
                                cursor = Some(seq);
                            }
                            if tx.send(Msg::Event { side, src, did, kind, base, rev, seq, at }).is_err() {
                                return;
                            }
                        }
                        Ok(None) => {}
                        // a host whose sequence restarted: everything it has
                        // now is new, so replay all of it
                        Err(e) if side == Side::Up && e.starts_with("error frame: FutureCursor") => {
                            cursor = Some(0);
                            let _ = tx.send(Msg::Restarted { src });
                            break;
                        }
                        Err(e) => {
                            let _ = tx.send(Msg::Other { side, src, what: e });
                        }
                    }
                }
            }
            Err(e) => {
                let _ = tx.send(Msg::Status { side, src, what: format!("connect {u}: {e}") });
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
    let c = reqwest::Client::builder().timeout(Duration::from_secs(30)).build()?;
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
            Some(c) if !r["repos"].as_array().is_none_or(|a| a.is_empty()) => cursor = Some(c.to_string()),
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
    /// (upstream index, seq) of the upstream copy (Up side only).
    from: (usize, i64),
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

#[derive(Clone, Copy, PartialEq, Eq)]
enum Scope {
    All,
    Hosted,
    Seen,
}

impl Scope {
    fn parse(s: &str) -> anyhow::Result<Self> {
        Ok(match s {
            "all" => Scope::All,
            "hosted" => Scope::Hosted,
            "seen" => Scope::Seen,
            s => anyhow::bail!("scope {s}: want all, hosted or seen"),
        })
    }
}

/// A relay event held until its DID's first upstream event (`--scope
/// seen`): the relay often beats the checker's own PDS socket to a DID's
/// first event, which would otherwise count it out of scope and then the
/// upstream copy as missing.
struct Held {
    kind: Kind,
    base: String,
    rev: Option<String>,
    seq: i64,
    at: Instant,
}

/// Held relay events older than this never get an upstream copy.
const HOLD: Duration = Duration::from_secs(60);

/// One relay side (one `--relay`, or their union) against the upstreams.
struct Cmp {
    relays: Vec<String>,
    scope: Scope,
    in_scope: std::collections::HashSet<String>,
    held: HashMap<String, Vec<Held>>,
    held_since_prune: Instant,
    dids: HashMap<String, DidState>,
    waiting: HashMap<(String, String), Waiting>,
    counts: HashMap<Kind, KindCounts>,
    lat: Histogram<u64>,
    lat_kind: HashMap<Kind, Histogram<u64>>,
    last_seq: HashMap<usize, i64>,
    seq_regressions: u64,
    duplicates: u64,
    out_of_order: u64,
    rev_regressions: u64,
    relay_first: u64,
    out_of_scope: u64,
    bad_frames: u64,
    examples: HashMap<&'static str, Vec<String>>,
    /// Every missing or extra #sync, #identity and #account (they're rare
    /// enough to list in full).
    diffs: Vec<serde_json::Value>,
    /// Every missing event with the upstream and seq it came from (capped).
    missing_events: Vec<serde_json::Value>,
    /// (upstream index, seq) of the Up event being handled.
    cur_from: (usize, i64),
    /// Per --upstream: latency of matched events, and missing count.
    by_up: HashMap<usize, (Histogram<u64>, u64)>,
}

struct Window {
    start: Instant,
    start_ms: u128,
    warm_end: Instant,
    collect_end: Instant,
    show: usize,
}

type Out = Option<std::io::BufWriter<std::fs::File>>;

impl Cmp {
    fn new(relays: Vec<String>, scope: Scope, in_scope: std::collections::HashSet<String>) -> Self {
        Cmp {
            relays,
            scope,
            in_scope,
            held: HashMap::new(),
            held_since_prune: Instant::now(),
            dids: HashMap::new(),
            waiting: HashMap::new(),
            counts: HashMap::new(),
            lat: hist(),
            lat_kind: HashMap::new(),
            last_seq: HashMap::new(),
            seq_regressions: 0,
            duplicates: 0,
            out_of_order: 0,
            rev_regressions: 0,
            relay_first: 0,
            out_of_scope: 0,
            bad_frames: 0,
            examples: HashMap::new(),
            diffs: Vec::new(),
            missing_events: Vec::new(),
            cur_from: (0, 0),
            by_up: HashMap::new(),
        }
    }

    fn note(&mut self, what: &'static str, s: String, show: usize) {
        let v = self.examples.entry(what).or_default();
        if v.len() < show {
            v.push(s);
        }
    }

    fn relay_name(&self, src: usize) -> String {
        format!("relay {}", self.relays[src])
    }

    #[allow(clippy::too_many_arguments)]
    fn on_up(
        &mut self,
        w: &Window,
        from: (usize, i64),
        did: String,
        kind: Kind,
        base: String,
        at: Instant,
        seq_out: &mut Out,
        lat_out: &mut Out,
    ) -> anyhow::Result<()> {
        if self.scope != Scope::All
            && self.in_scope.insert(did.clone())
            && let Some(held) = self.held.remove(&did)
        {
            for h in held {
                self.on_event(w, Side::Relay, did.clone(), h.kind, h.base, h.rev, h.seq, h.at, seq_out, lat_out)?;
            }
        }
        self.cur_from = from;
        self.on_event(w, Side::Up, did, kind, base, None, 0, at, seq_out, lat_out)
    }

    #[allow(clippy::too_many_arguments)]
    fn on_relay(
        &mut self,
        w: &Window,
        src: usize,
        did: String,
        kind: Kind,
        base: String,
        rev: Option<String>,
        seq: i64,
        at: Instant,
        seq_out: &mut Out,
        lat_out: &mut Out,
    ) -> anyhow::Result<()> {
        if seq > 0 {
            if let Some(&prev) = self.last_seq.get(&src)
                && seq <= prev
            {
                self.seq_regressions += 1;
                let s = format!("{}: seq {seq} after {prev}", self.relay_name(src));
                self.note("seq regressions", s, w.show);
            }
            self.last_seq.insert(src, seq);
        }
        match self.scope {
            Scope::All => {}
            _ if self.in_scope.contains(&did) => {}
            Scope::Hosted => {
                self.out_of_scope += 1;
                return Ok(());
            }
            Scope::Seen => {
                self.held.entry(did).or_default().push(Held { kind, base, rev, seq, at });
                if self.held_since_prune.elapsed() > HOLD {
                    self.prune_held(Instant::now());
                }
                return Ok(());
            }
        }
        self.on_event(w, Side::Relay, did, kind, base, rev, seq, at, seq_out, lat_out)
    }

    fn prune_held(&mut self, now: Instant) {
        let mut dropped = 0;
        self.held.retain(|_, v| {
            let n = v.len();
            v.retain(|h| now.duration_since(h.at) < HOLD);
            dropped += n - v.len();
            !v.is_empty()
        });
        self.out_of_scope += dropped as u64;
        self.held_since_prune = now;
    }

    #[allow(clippy::too_many_arguments)]
    fn on_event(
        &mut self,
        w: &Window,
        side: Side,
        did: String,
        kind: Kind,
        base: String,
        rev: Option<String>,
        seq: i64,
        at: Instant,
        seq_out: &mut Out,
        lat_out: &mut Out,
    ) -> anyhow::Result<()> {
        use std::io::Write;
        let st = self.dids.entry(did.clone()).or_default();
        let occ_map = if side == Side::Up { &mut st.up_occ } else { &mut st.relay_occ };
        let occ = occ_map.entry(base.clone()).or_insert(0);
        let seen_before = *occ > 0;
        *occ += 1;
        let occ = *occ;
        let unique = matches!(kind, Kind::Commit | Kind::Sync) || base.contains('@');
        if seen_before && unique {
            if side == Side::Relay {
                self.duplicates += 1;
                self.note("duplicates", format!("{did} {base}"), w.show);
            }
            return Ok(());
        }
        let key = if unique { base.clone() } else { format!("{base}#{}", occ - 1) };
        let c = self.counts.entry(kind).or_default();
        match side {
            Side::Up => c.up += 1,
            Side::Relay => c.relay += 1,
        }
        let st = self.dids.get_mut(&did).expect("inserted above");
        let mut regression = None;
        if side == Side::Relay
            && let Some(rev) = &rev
        {
            if let Some(prev) = &st.relay_last_rev {
                // a #sync restates the current rev (e.g. on reactivation)
                if rev < prev || (rev == prev && kind == Kind::Commit) {
                    regression = Some(format!("{did}: rev {rev} after {prev}"));
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
        if let Some(r) = regression {
            self.rev_regressions += 1;
            self.note("rev regressions", r, w.show);
        }
        if side == Side::Relay
            && let Some(o) = seq_out.as_mut()
        {
            writeln!(o, "{seq} {did} {key}")?;
        }
        let wk = (did.clone(), key);
        match self.waiting.remove(&wk) {
            Some(wt) if wt.side != side => {
                let (up_at, up_pos, up) =
                    if side == Side::Up { (at, pos, self.cur_from.0) } else { (wt.at, wt.pos, wt.from.0) };
                let re_at = if side == Side::Up { wt.at } else { at };
                if up_at >= w.warm_end {
                    let us = if re_at >= up_at {
                        re_at.duration_since(up_at).as_micros() as u64
                    } else {
                        self.relay_first += 1;
                        0
                    };
                    self.lat.saturating_record(us.max(1));
                    if let Some(o) = lat_out.as_mut() {
                        let t = w.start_ms + re_at.max(up_at).duration_since(w.start).as_millis();
                        writeln!(o, "{t} {:.1}", us as f64 / 1000.0)?;
                    }
                    self.lat_kind.entry(kind).or_insert_with(hist).saturating_record(us.max(1));
                    self.by_up.entry(up).or_insert_with(|| (hist(), 0)).0.saturating_record(us.max(1));
                }
                self.counts.entry(kind).or_default().matched += 1;
                if side == Side::Relay {
                    let st = self.dids.get_mut(&did).expect("inserted above");
                    let bad = st.relay_max_pos.is_some_and(|m| up_pos < m);
                    st.relay_max_pos = Some(st.relay_max_pos.map_or(up_pos, |m| m.max(up_pos)));
                    if bad {
                        self.out_of_order += 1;
                        self.note("out of order", format!("{did} {}", wk.1), w.show);
                    }
                }
            }
            Some(wt) => {
                // the same side again (two relay streams carrying one event)
                self.waiting.insert(wk, wt);
            }
            None => {
                self.waiting.insert(wk, Waiting { side, at, kind, pos, from: self.cur_from });
            }
        }
        Ok(())
    }

    fn finish(&mut self, w: &Window, upstreams: &[String]) {
        self.prune_held(Instant::now() + HOLD);
        let waiting = std::mem::take(&mut self.waiting);
        let ms_at = |at: Instant| w.start_ms + at.duration_since(w.start).as_millis();
        for ((did, key), wt) in &waiting {
            let which = match wt.side {
                Side::Up if wt.at >= w.warm_end && wt.at <= w.collect_end => "missing",
                Side::Relay if wt.at >= w.warm_end => "extra",
                _ => continue,
            };
            let c = self.counts.entry(wt.kind).or_default();
            if which == "missing" {
                c.missing += 1;
                self.by_up.entry(wt.from.0).or_insert_with(|| (hist(), 0)).1 += 1;
                if self.missing_events.len() < 10_000 {
                    self.missing_events.push(serde_json::json!({
                        "upstream": upstreams[wt.from.0], "seq": wt.from.1, "did": did, "key": key,
                    }));
                }
            } else {
                c.extra += 1;
            }
            self.note(which, format!("{did} {key}"), w.show);
            if wt.kind != Kind::Commit {
                self.diffs.push(serde_json::json!({
                    "kind": wt.kind.name(), "what": which, "did": did, "key": key, "at_ms": ms_at(wt.at),
                }));
            }
        }
        self.diffs.sort_by_key(|d| d["at_ms"].as_u64());
    }

    fn tot(&self, f: fn(&KindCounts) -> u64) -> u64 {
        self.counts.values().map(f).sum()
    }

    fn print(&self, upstreams: usize, duration: u64, settle: u64) {
        let secs = duration as f64;
        println!(
            "e2e_check: {upstreams} upstream(s), {} relay stream(s) [{}], {secs:.0}s + {settle}s settle",
            self.relays.len(),
            self.relays.join(" "),
        );
        println!("  {:<10} {:>9} {:>9} {:>9} {:>8} {:>8}", "kind", "upstream", "relay", "matched", "missing", "extra");
        for k in Kind::ALL {
            let c = self.counts.get(&k).copied().unwrap_or_default();
            println!("  {:<10} {:>9} {:>9} {:>9} {:>8} {:>8}", k.name(), c.up, c.relay, c.matched, c.missing, c.extra);
        }
        println!("  upstream rate {:.0} ev/s, DIDs {}", self.tot(|c| c.up) as f64 / secs, self.dids.len());
        println!(
            "  latency upstream->relay: p50 {:.2} ms  p90 {:.2} ms  p99 {:.2} ms  max {:.2} ms  (n={}, relay-first {})",
            ms(self.lat.value_at_quantile(0.5)),
            ms(self.lat.value_at_quantile(0.9)),
            ms(self.lat.value_at_quantile(0.99)),
            ms(self.lat.max()),
            self.lat.len(),
            self.relay_first,
        );
        println!(
            "  out of order per DID {}, rev regressions {}, duplicates {}, seq regressions {}, out of scope {}, bad frames {}",
            self.out_of_order,
            self.rev_regressions,
            self.duplicates,
            self.seq_regressions,
            self.out_of_scope,
            self.bad_frames
        );
        let mut keys: Vec<_> = self.examples.keys().copied().collect();
        keys.sort();
        for k in keys {
            println!("  {k}:");
            for e in &self.examples[k] {
                println!("    {e}");
            }
        }
    }

    fn json(&self, args: &Args) -> serde_json::Value {
        let per_kind: serde_json::Map<String, serde_json::Value> = Kind::ALL
            .iter()
            .map(|k| {
                let c = self.counts.get(k).copied().unwrap_or_default();
                let l = self.lat_kind.get(k);
                (
                    k.name().to_string(),
                    serde_json::json!({
                        "upstream": c.up, "relay": c.relay, "matched": c.matched, "missing": c.missing, "extra": c.extra,
                        "p50_ms": l.map(|h| ms(h.value_at_quantile(0.5))), "p99_ms": l.map(|h| ms(h.value_at_quantile(0.99))),
                    }),
                )
            })
            .collect();
        let lat = &self.lat;
        serde_json::json!({
            "upstreams": args.upstreams, "relays": self.relays, "duration_s": args.duration, "settle_s": args.settle,
            "kinds": per_kind, "missing": self.tot(|c| c.missing), "extra": self.tot(|c| c.extra),
            "out_of_order": self.out_of_order, "rev_regressions": self.rev_regressions, "duplicates": self.duplicates,
            "seq_regressions": self.seq_regressions, "bad_frames": self.bad_frames, "relay_first": self.relay_first,
            "out_of_scope": self.out_of_scope,
            "latency_ms": {"p50": ms(lat.value_at_quantile(0.5)), "p90": ms(lat.value_at_quantile(0.9)),
                           "p99": ms(lat.value_at_quantile(0.99)), "max": ms(lat.max()), "n": lat.len()},
            "examples": self.examples,
            "diffs": self.diffs,
            "missing_events": self.missing_events,
            "by_upstream": args.upstreams.iter().enumerate().map(|(i, u)| {
                let (h, missing) = self.by_up.get(&i).map_or((None, 0), |(h, m)| (Some(h), *m));
                serde_json::json!({
                    "upstream": u, "matched": h.map_or(0, |h| h.len()), "missing": missing,
                    "p50_ms": h.map(|h| ms(h.value_at_quantile(0.5))), "p99_ms": h.map(|h| ms(h.value_at_quantile(0.99))),
                    "max_ms": h.map(|h| ms(h.max())),
                })
            }).collect::<Vec<_>>(),
        })
    }

    fn failed(&self) -> bool {
        let commit_extra =
            self.counts.get(&Kind::Commit).map_or(0, |c| c.extra) + self.counts.get(&Kind::Sync).map_or(0, |c| c.extra);
        self.tot(|c| c.missing) > 0
            || commit_extra > 0
            || self.out_of_order > 0
            || self.rev_regressions > 0
            || self.duplicates > 0
            || self.seq_regressions > 0
    }
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
    static CFG: std::sync::OnceLock<std::sync::Arc<rustls::ClientConfig>> = std::sync::OnceLock::new();
    CFG.get_or_init(|| {
        let roots = rustls::RootCertStore { roots: webpki_roots::TLS_SERVER_ROOTS.to_vec() };
        std::sync::Arc::new(rustls::ClientConfig::builder().with_root_certificates(roots).with_no_client_auth())
    })
    .clone()
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    rustls_provider();
    let args = Args::parse();
    let scope = Scope::parse(&args.scope)?;
    if args.separate && (args.seq_out.is_some() || args.lat_out.is_some()) {
        anyhow::bail!("--seq-out and --lat-out take one relay side, not --separate");
    }
    if args.relay_scopes.len() > args.relays.len() {
        anyhow::bail!("more --relay-scope than --relay");
    }
    let scope_of =
        |i: usize| -> anyhow::Result<Scope> { args.relay_scopes.get(i).map_or(Ok(scope), |s| Scope::parse(s)) };
    let mut listed: std::collections::HashSet<String> = Default::default();
    let any_hosted = (0..args.relays.len().max(1)).any(|i| scope_of(i).ok() == Some(Scope::Hosted));
    if any_hosted {
        for u in &args.upstreams {
            let dids = list_repos(&http_origin(u)).await?;
            eprintln!("e2e_check: {} hosts {} repos", http_origin(u), dids.len());
            listed.extend(dids);
        }
    }

    // cmp_of[relay src] = (comparison, its src index within it)
    let mut cmps: Vec<Cmp> = Vec::new();
    let mut cmp_of: Vec<(usize, usize)> = Vec::new();
    if args.separate {
        for (i, r) in args.relays.iter().enumerate() {
            let sc = scope_of(i)?;
            let seed = if sc == Scope::Hosted { listed.clone() } else { Default::default() };
            cmps.push(Cmp::new(vec![r.clone()], sc, seed));
            cmp_of.push((i, 0));
        }
    } else {
        if !args.relay_scopes.is_empty() {
            anyhow::bail!("--relay-scope needs --separate");
        }
        cmps.push(Cmp::new(args.relays.clone(), scope, listed));
        cmp_of = (0..args.relays.len()).map(|i| (0, i)).collect();
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
    let w = Window {
        start,
        start_ms: std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH)?.as_millis(),
        warm_end: start + Duration::from_secs(args.warmup),
        collect_end: start + Duration::from_secs(args.duration),
        show: args.show,
    };
    let end = w.collect_end + if relay_on { Duration::from_secs(args.settle) } else { Duration::ZERO };

    let mut up_counts: HashMap<Kind, u64> = HashMap::new();
    let mut up_dids: std::collections::HashSet<String> = Default::default();
    let mut up_last_seq: HashMap<usize, i64> = HashMap::new();
    let mut upstream_replays = 0u64;
    let mut upstream_restarts = 0u64;
    let mut up_commits: HashSet<(String, String)> = HashSet::new();
    let mut up_bad_frames = 0u64;

    let open = |p: &Option<String>| -> anyhow::Result<Out> {
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
    let (mut last_up, mut last_relay, mut last_t) = (0u64, vec![0u64; cmps.len()], Instant::now());
    loop {
        let msg = tokio::select! {
            _ = &mut deadline => break,
            _ = tick.tick() => {
                let up: u64 = up_counts.values().sum();
                let dt = last_t.elapsed().as_secs_f64();
                let mut line = format!(
                    "e2e_check: t={:>4.0}s upstream {up} ({:.0}/s)",
                    start.elapsed().as_secs_f64(), (up - last_up) as f64 / dt,
                );
                for (i, c) in cmps.iter().enumerate() {
                    let re = c.tot(|k| k.relay);
                    let pending = c.waiting.values().filter(|w| w.side == Side::Up).count();
                    line.push_str(&format!(
                        " | relay{} {re} ({:.0}/s) awaiting {pending} p50 {:.1} ms p99 {:.1} ms",
                        if cmps.len() > 1 { format!(" {}", c.relays.join(",")) } else { String::new() },
                        (re - last_relay[i]) as f64 / dt,
                        ms(c.lat.value_at_quantile(0.5)), ms(c.lat.value_at_quantile(0.99)),
                    ));
                    last_relay[i] = re;
                }
                eprintln!("{line}");
                (last_up, last_t) = (up, Instant::now());
                continue;
            }
            m = rx.recv() => match m { Some(m) => m, None => break },
        };
        match msg {
            Msg::Restarted { src } => {
                eprintln!("e2e_check: {}: sequence restarted", name(Side::Up, src));
                up_last_seq.remove(&src);
                upstream_restarts += 1;
            }
            Msg::Status { side, src, what } => {
                eprintln!("e2e_check: {}: {what}", name(side, src));
            }
            Msg::Other { side, src, what } => {
                let s = format!("{}: {what}", name(side, src));
                match side {
                    Side::Up => {
                        up_bad_frames += 1;
                        for c in &mut cmps {
                            c.bad_frames += 1;
                            c.note("bad frames", s.clone(), args.show);
                        }
                    }
                    Side::Relay => {
                        let c = &mut cmps[cmp_of[src].0];
                        c.bad_frames += 1;
                        c.note("bad frames", s, args.show);
                    }
                }
            }
            Msg::Event { side: Side::Up, src, did, kind, base, seq, at, .. } => {
                let commit = matches!(kind, Kind::Commit | Kind::Sync);
                if seq > 0 {
                    // A commit this upstream never sent, far below its last
                    // seq: its sequence restarted under a socket that never got
                    // FutureCursor (it reconnected past the new head). A replay
                    // fault goes back a few dozen frames, to commits already seen.
                    if commit
                        && up_last_seq.get(&src).is_some_and(|&prev| seq.saturating_mul(2) < prev)
                        && !up_commits.contains(&(did.clone(), base.clone()))
                    {
                        eprintln!("e2e_check: {}: sequence restarted (seq {seq} is a new commit)", name(Side::Up, src));
                        up_last_seq.remove(&src);
                        upstream_restarts += 1;
                    }
                    if up_last_seq.get(&src).is_some_and(|&prev| seq <= prev) {
                        // the upstream sent these again (a replay fault): they're
                        // not new events, and the relay shouldn't carry them twice
                        upstream_replays += 1;
                        continue;
                    }
                    up_last_seq.insert(src, seq);
                }
                if commit {
                    up_commits.insert((did.clone(), base.clone()));
                }
                *up_counts.entry(kind).or_default() += 1;
                up_dids.insert(did.clone());
                if relay_on {
                    for c in &mut cmps {
                        c.on_up(&w, (src, seq), did.clone(), kind, base.clone(), at, &mut seq_out, &mut lat_out)?;
                    }
                }
            }
            Msg::Event { side: Side::Relay, src, did, kind, base, rev, seq, at } => {
                let (ci, csrc) = cmp_of[src];
                cmps[ci].on_relay(&w, csrc, did, kind, base, rev, seq, at, &mut seq_out, &mut lat_out)?;
            }
        }
    }

    for o in [seq_out.as_mut(), lat_out.as_mut()].into_iter().flatten() {
        std::io::Write::flush(o)?;
    }
    let empty = up_counts.values().sum::<u64>() == 0;
    if empty {
        eprintln!("e2e_check: no upstream events at all (is anything writing?)");
    }
    if !relay_on {
        println!(
            "e2e_check: {} upstream(s), upstreams only: {} events, DIDs {}, replays {upstream_replays}, restarts {upstream_restarts}, bad frames {up_bad_frames}",
            args.upstreams.len(),
            up_counts.values().sum::<u64>(),
            up_dids.len()
        );
        if empty && !args.report_only {
            std::process::exit(1);
        }
        return Ok(());
    }
    for c in &mut cmps {
        c.finish(&w, &args.upstreams);
        c.print(args.upstreams.len(), args.duration, args.settle);
    }

    if let Some(path) = &args.json_out {
        let with_up = |c: &Cmp| {
            let mut j = c.json(&args);
            j["upstream_replays"] = upstream_replays.into();
            j["upstream_restarts"] = upstream_restarts.into();
            j
        };
        let j = if args.separate {
            serde_json::json!({ "comparisons": cmps.iter().map(with_up).collect::<Vec<_>>() })
        } else {
            with_up(&cmps[0])
        };
        std::fs::write(path, serde_json::to_vec_pretty(&j)?)?;
    }

    let failed = cmps.iter().any(Cmp::failed);
    if (failed || empty) && !args.report_only {
        std::process::exit(1);
    }
    Ok(())
}
