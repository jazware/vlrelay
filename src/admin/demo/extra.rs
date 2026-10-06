//! The demo's quorum log, effective config and full policy document.
//!
//! The quorum statuses are built as JSON in the shape `qlog::node::Status`
//! serializes to, not from the struct itself, so the demo keeps compiling
//! while the quorum log's fields move.

use super::super::*;
use serde_json::{Value, json};

const MEMBERS: [&str; 3] = ["relay-a", "relay-b", "relay-c"];

struct Switch {
    from_epoch: u64,
    epoch: u64,
    from: Vec<String>,
    to: Vec<String>,
    leader: String,
    timings: [u64; 7],
    flushed: u64,
    at_ms: i64,
}

struct Recovery {
    generation: u64,
    epoch: u64,
    from: u64,
    to: u64,
    took_ms: u64,
}

pub(in crate::admin) struct Extra {
    epoch: u64,
    leader: String,
    members: Vec<String>,
    learners: Vec<String>,
    retired: Vec<String>,
    addrs: BTreeMap<String, String>,
    /// When each learner joined (unix ms): it catches up over ~20 s.
    joining: BTreeMap<String, i64>,
    members_since: u64,
    switches: Vec<Switch>,
    recoveries: Vec<Recovery>,
    generation: u64,
    started_ms: i64,
    pub full: Value,
    pub full_by: String,
    pub full_at_ms: i64,
    pub full_note: String,
}

impl Extra {
    pub fn new(now: i64, seq: i64) -> Extra {
        let seq = seq as u64;
        let mut full = serde_json::to_value(crate::policy::doc::PolicyBody::default()).unwrap_or_default();
        // a couple of operator edits, so the Tuning page shows values off their defaults
        full["consumers"]["connectionsPerIp"] = json!(24);
        full["cluster"]["newHostsPerDay"] = json!(80);
        full["transitions"]["promoteAfterDays"] = json!(5);
        let addrs =
            MEMBERS.iter().enumerate().map(|(i, m)| (m.to_string(), format!("10.0.7.{}:2981", 11 + i))).collect();
        let hour = 3_600_000;
        Extra {
            epoch: 14,
            leader: "relay-a".into(),
            members: MEMBERS.iter().map(|s| s.to_string()).collect(),
            learners: Vec::new(),
            retired: Vec::new(),
            addrs,
            joining: BTreeMap::new(),
            members_since: seq - 41_882_310,
            switches: vec![
                Switch {
                    from_epoch: 11,
                    epoch: 12,
                    from: vec!["relay-a".into(), "relay-b".into(), "relay-d".into()],
                    to: MEMBERS.iter().map(|s| s.to_string()).collect(),
                    leader: "relay-a".into(),
                    timings: [38, 21_400, 610, 84, 702, 41, 836],
                    flushed: seq - 41_882_310,
                    at_ms: now - 26 * hour,
                },
                Switch {
                    from_epoch: 9,
                    epoch: 10,
                    from: vec!["relay-a".into(), "relay-b".into()],
                    to: vec!["relay-a".into(), "relay-b".into(), "relay-d".into()],
                    leader: "relay-a".into(),
                    timings: [41, 18_900, 540, 77, 655, 39, 780],
                    flushed: seq - 96_204_118,
                    at_ms: now - 74 * hour,
                },
            ],
            recoveries: vec![Recovery {
                generation: 3,
                epoch: 12,
                from: seq - 55_120_400,
                to: seq - 55_118_212,
                took_ms: 2_740,
            }],
            generation: 3,
            started_ms: now - 26 * hour,
            full,
            full_by: "admin".into(),
            full_at_ms: now - 9 * hour,
            full_note: "more headroom for consumers behind CGNAT".into(),
        }
    }

    fn node_status(&self, id: &str, seq: u64, now: i64, ev: f64) -> Value {
        let leading = id == self.leader;
        let learner = self.learners.iter().any(|l| l == id);
        let retired = self.retired.iter().any(|r| r == id);
        let wobble = |k: i64| ((now / 1000 + k) % 7) as u64;
        let head = seq + 18 + wobble(0) * 4;
        let (last, commit, emitted) = if leading {
            (head, seq, seq)
        } else if learner {
            let since = now - self.joining.get(id).copied().unwrap_or(now);
            let behind = (900_000.0 * (1.0 - (since as f64 / 20_000.0)).max(0.0)) as u64;
            (head - behind.min(head), seq - behind.min(seq), seq - behind.min(seq))
        } else if retired {
            let at = self.switches.first().map_or(seq, |s| s.flushed);
            (at, at, at)
        } else {
            // relay-c trails a little more: it's the one across the rack
            let lag = if id == "relay-c" { 40 + wobble(3) * 22 } else { wobble(1) * 3 };
            (head - lag, seq - lag / 2, seq - lag / 2 - wobble(2))
        };
        let flushed = seq - (seq % 4_096) - 8_192;
        let base = flushed.saturating_sub(250_000);
        let up_secs = ((now - self.started_ms) / 1000).max(1) as u64;
        let appended = (ev * up_secs as f64) as u64;
        let q = |p50: u64, p99: u64| json!({"count": appended / 3, "p50": p50, "p90": p50 * 16 / 10, "p99": p99, "p999": p99 * 2, "max": p99 * 5});
        let mut s = json!({
            "id": id,
            "role": if leading { "leader" } else { "follower" },
            "epoch": self.epoch,
            "promised": self.epoch,
            "leader": self.leader,
            "base": base,
            "last": last,
            "commit": commit,
            "emitted": emitted,
            "intact": !learner || now - self.joining.get(id).copied().unwrap_or(0) > 20_000,
            "log_bytes": last.saturating_sub(base) * 4_600,
            "appended": if leading { appended } else { 0 },
            "takeovers": if leading { 2 } else { 0 },
            "step_downs": if id == "relay-b" { 1 } else { 0 },
            "resets": 0,
            "emit_gaps": 0,
            "promise_rounds": if leading { 3 } else { 0 },
            "disk_reads": if id == "relay-c" { 14 } else { 2 },
            "bucket_reads": if id == "relay-c" { 1 } else { 0 },
            "commit_us": q(1_850 + wobble(4) * 40, 4_300 + wobble(5) * 300),
            "disk": {
                "fsyncs": appended / 40,
                "fsync_us": q(610, 2_100),
                "batch_ops": q(38, 160),
                "bytes_written": appended * 4_700,
                "disk_bytes": 18_400_000_000u64 + wobble(6) * 9_000_000,
                "rollovers": up_secs / 90,
                "deleted": up_secs / 95,
            },
            "flushed": flushed,
            "reserve": flushed + 400_000,
            "flush": Value::Null,
            "generation": self.generation,
            "recoveries": if leading { self.recoveries.len() } else { 0 },
            "lost_quorums": 1,
            "recovered": [],
            "members": self.members,
            "learners": self.learners,
            "members_since": self.members_since,
            "retired": retired,
            "paused": false,
            "last_epoch": self.epoch,
            "switches": [],
        });
        if leading {
            let flushes = up_secs / 2;
            s["flush"] = json!({
                "flushes": flushes,
                "aborted": 3,
                "failed": 0,
                "fences": 1,
                "adopted": 0,
                "segments": flushes,
                "segment_bytes": flushes * 3_900_000,
                "raw_bytes": flushes * 9_200_000,
                "entries": appended,
                "duration_us": q(182_000, 640_000),
                "seal_us": q(900, 4_800),
                "requests": {"PutObject": flushes * 2, "GetObject": flushes / 10, "ListObjectsV2": 40},
                "requests_total": {"PutObject": flushes * 2 + 812, "GetObject": flushes / 9, "ListObjectsV2": 210, "DeleteObject": 1_840},
                "applied": flushed,
                "last_flushed": flushed,
                "last_reserve": flushed + 400_000,
            });
            s["recovered"] = self
                .recoveries
                .iter()
                .map(|r| {
                    let t = r.took_ms;
                    json!({"generation": r.generation, "epoch": r.epoch, "manifest_flushed": r.from,
                           "orphans_to": r.from + 1_204, "after": r.to, "base": r.from.saturating_sub(250_000),
                           "orphan_segments": 1, "salvaged": r.to - r.from - 1_204,
                           "read_ms": t / 9, "clone_ms": t / 4, "apply_seal_ms": t / 3, "segments_ms": t / 6,
                           "manifest_ms": t / 20, "total_ms": t})
                })
                .collect();
            s["switches"] = self
                .switches
                .iter()
                .map(|w| {
                    let t = w.timings;
                    json!({"from_epoch": w.from_epoch, "epoch": w.epoch, "from": w.from, "to": w.to,
                           "leader": w.leader, "record_ms": t[0], "catch_up_ms": t[1], "pre_flush_ms": t[2],
                           "drain_ms": t[3], "flush_ms": t[4], "cas_ms": t[5], "paused_ms": t[6],
                           "flushed": w.flushed, "at_ms": w.at_ms})
                })
                .collect();
        }
        s
    }

    pub fn view(&mut self, seq: i64, now: i64, ev: f64) -> QuorumView {
        // learners that have caught up become members, as the leader's switch would
        let done: Vec<String> =
            self.joining.iter().filter(|(_, at)| now - **at > 20_000).map(|(id, _)| id.clone()).collect();
        if !done.is_empty() {
            let from = self.members.clone();
            self.learners.retain(|l| !done.contains(l));
            for d in &done {
                self.joining.remove(d);
            }
            let to: Vec<String> = from.iter().chain(done.iter()).cloned().collect();
            self.switch(from, to, seq as u64, now);
        }
        let mut ids: Vec<String> = self.members.iter().chain(&self.learners).chain(&self.retired).cloned().collect();
        ids.dedup();
        QuorumView {
            nodes: ids
                .iter()
                .map(|id| QuorumNode {
                    node: id.clone(),
                    addr: self.addrs.get(id).cloned().unwrap_or_default(),
                    stale: false,
                    error: None,
                    reported_ms: now - ((now / 1000) % 3) * 140,
                    status: Some(self.node_status(id, seq as u64, now, ev)),
                })
                .collect(),
        }
    }

    fn switch(&mut self, from: Vec<String>, to: Vec<String>, seq: u64, now: i64) {
        let removed: Vec<String> = from.iter().filter(|m| !to.contains(m)).cloned().collect();
        self.switches.insert(
            0,
            Switch {
                from_epoch: self.epoch,
                epoch: self.epoch + 1,
                from,
                to: to.clone(),
                leader: self.leader.clone(),
                timings: [36, 19_800, 590, 81, 688, 44, 813],
                flushed: seq,
                at_ms: now,
            },
        );
        self.epoch += 1;
        self.members = to;
        self.members_since = seq;
        self.retired.retain(|r| !self.members.contains(r));
        for r in removed {
            if !self.retired.contains(&r) {
                self.retired.push(r);
            }
        }
    }

    pub fn change(&mut self, req: QuorumMembersChange, seq: i64, now: i64) -> AdminResult<Value> {
        let mut want = req.members.clone();
        want.sort();
        want.dedup();
        if want.len() < 3 {
            return Err(AdminError::BadRequest("a quorum needs at least 3 members".into()));
        }
        for m in &want {
            if !self.addrs.contains_key(m) {
                match req.addrs.get(m) {
                    Some(a) => {
                        self.addrs.insert(m.clone(), a.clone());
                    }
                    None => return Err(AdminError::BadRequest(format!("the leader can't dial {m}: give its address"))),
                }
            }
        }
        if !want.contains(&self.leader) {
            self.leader = want[0].clone();
        }
        let new: Vec<String> = want.iter().filter(|m| !self.members.contains(m)).cloned().collect();
        let keep: Vec<String> = want.iter().filter(|m| self.members.contains(m)).cloned().collect();
        if new.is_empty() && keep.len() == self.members.len() {
            return Err(AdminError::BadRequest("that's the current member set".into()));
        }
        // a removal applies at once; a new node joins as a learner first
        if keep.len() != self.members.len() {
            self.switch(self.members.clone(), keep, seq as u64, now);
        }
        for n in new {
            self.retired.retain(|r| *r != n);
            self.joining.insert(n.clone(), now);
            self.learners.push(n);
        }
        Ok(self.node_status(&self.leader.clone(), seq as u64, now, 0.0))
    }

    pub fn settings(&self) -> SettingsView {
        let e =
            |flag: &str, env: Option<&str>, value: Option<&str>, source: &str, default: Option<&str>, help: &str| {
                ConfigEntry {
                    flag: flag.into(),
                    env: env.map(str::to_string),
                    value: value.map(str::to_string),
                    source: source.into(),
                    default: default.map(str::to_string),
                    secret: false,
                    set: value.is_some(),
                    help: help.into(),
                }
            };
        let secret = |flag: &str, env: &str, set: bool, source: &str, help: &str| ConfigEntry {
            flag: flag.into(),
            env: Some(env.into()),
            value: None,
            source: source.into(),
            default: None,
            secret: true,
            set,
            help: help.into(),
        };
        let entries = vec![
            e(
                "--listen",
                Some("VLRELAY_LISTEN"),
                Some("0.0.0.0:2980"),
                "env",
                Some("127.0.0.1:2980"),
                "Serves subscribeRepos, the sync API, requestCrawl, /admin and /metrics.",
            ),
            e(
                "--memory",
                None,
                Some("false"),
                "default",
                Some("false"),
                "Everything in memory: nothing survives a restart.",
            ),
            e(
                "--s3-endpoint",
                Some("VLRELAY_S3_ENDPOINT"),
                Some("https://<account>.r2.cloudflarestorage.com"),
                "env",
                None,
                "",
            ),
            e("--s3-bucket", Some("VLRELAY_S3_BUCKET"), Some("vlrelay-prod"), "env", None, ""),
            secret("--s3-access-key", "VLRELAY_S3_ACCESS_KEY", true, "env", ""),
            secret("--s3-secret-key", "VLRELAY_S3_SECRET_KEY", true, "env", ""),
            e("--s3-region", Some("VLRELAY_S3_REGION"), Some("auto"), "default", Some("auto"), ""),
            e(
                "--s3-unsigned-payload",
                Some("VLRELAY_S3_UNSIGNED_PAYLOAD"),
                None,
                "unset",
                None,
                "Send PUT bodies as SigV4 UNSIGNED-PAYLOAD instead of hashing each one (default: on for an https endpoint, where TLS covers the body).",
            ),
            e(
                "--prefix",
                Some("VLRELAY_PREFIX"),
                Some("vlrelay"),
                "default",
                Some("vlrelay"),
                "Key prefix in the bucket: one relay per prefix.",
            ),
            e(
                "--plc-url",
                Some("VLRELAY_PLC_URL"),
                Some("https://plc.directory"),
                "default",
                Some("https://plc.directory"),
                "",
            ),
            e(
                "--log-compression",
                None,
                Some("-1"),
                "default",
                Some("-1"),
                "zstd level for log segments: 0 stores them uncompressed, negative levels are zstd's fast ones.",
            ),
            e("--host", None, None, "unset", None, "An upstream to subscribe to (repeatable)."),
            e("--crawl", None, Some("true"), "flag", Some("false"), "Accept com.atproto.sync.requestCrawl."),
            e(
                "--host-tier",
                None,
                Some("trusted"),
                "default",
                Some("trusted"),
                "The tier a --host upstream starts at the first time it's seen.",
            ),
            secret(
                "--admin-token",
                "VLRELAY_ADMIN_TOKEN",
                true,
                "env",
                "Turns on /admin (dashboard and API) with this token.",
            ),
            e("--ui-dir", None, None, "unset", None, "A built dashboard (ui/dist); default: this tree's, if built."),
            e(
                "--trusted-proxy",
                Some("VLRELAY_TRUSTED_PROXIES"),
                Some("10.0.0.0/8"),
                "env",
                None,
                "Proxies whose X-Forwarded-For names the client.",
            ),
            e(
                "--dev-mode",
                None,
                Some("false"),
                "default",
                Some("false"),
                "Allows plain ws://, IPs, localhost and ports for upstreams and DID documents.",
            ),
            e("--lanes", None, Some("64"), "default", Some("64"), "Pipeline lanes; a DID always maps to the same one."),
            e(
                "--ingest-threads",
                None,
                Some("12"),
                "flag",
                None,
                "Threads verifying events (default: the core count, at most 16).",
            ),
            e(
                "--host-inflight-events",
                None,
                Some("8192"),
                "default",
                Some("8192"),
                "Upstream frames one host may have read and not yet durable.",
            ),
            e("--host-inflight-mb", None, Some("64"), "default", Some("64"), ""),
            e("--inflight-events", None, Some("32768"), "default", Some("32768"), "The same over every host together."),
            e("--inflight-mb", None, Some("384"), "default", Some("384"), ""),
            e(
                "--did-lookups-per-sec",
                None,
                Some("50"),
                "default",
                Some("50"),
                "DID document fetches per second, all DIDs together.",
            ),
            e(
                "--node-id",
                Some("VLRELAY_NODE_ID"),
                Some("relay-a"),
                "env",
                Some("relay"),
                "Node id: the node log's id prefix, and the cluster member name.",
            ),
            secret("--internal-token", "VLRELAY_INTERNAL_TOKEN", true, "env", "Shared secret on every peer request."),
        ];
        SettingsView { binary: "vlrelay".into(), version: env!("CARGO_PKG_VERSION").into(), entries }
    }
}
