//! A simulated busy relay behind [`AdminSource`], so the dashboard can be
//! built and screenshotted before the relay's state exists.
//!
//! ~5,000 hosts: two dozen big hosts carrying most of the traffic, a long
//! tail of self-hosted PDSes (mostly idle), a few buggy implementations with
//! high reject rates and a few spam farms that trip the policy's thresholds.
//! A 1 s tick moves every rate, so charts and tables change like a live relay.

use super::*;
use parking_lot::Mutex;
use std::collections::{HashMap, VecDeque};
use std::time::Duration;

mod extra;

const HOST_SHARDS: usize = 64;
const DID_SHARDS: usize = 256;
const NODES: [&str; 3] = ["relay-a", "relay-b", "relay-c"];
const HISTORY: usize = 300;
const HOST_HISTORY: usize = 120;
const RECENT_REJECTS: usize = 40;
const EVENT_BYTES: f64 = 4_600.0;

pub struct Demo {
    sim: Mutex<Sim>,
    feed: Arc<changes::ChangeFeed>,
    /// Always locked after `sim` when both are held.
    extra: Mutex<extra::Extra>,
}

impl Demo {
    /// Builds the simulation and starts its tick (call inside a tokio runtime).
    pub fn start(seed: u64) -> Arc<Demo> {
        let mut sim = Sim::new(seed, now_ms());
        // fill the history so charts aren't empty on first load
        let start = sim.now_ms;
        for i in (1..=HISTORY as i64).rev() {
            sim.tick(start - i * 1000);
        }
        sim.tick(start);
        let extra = extra::Extra::new(start, sim.last_seq);
        let feed = changes::ChangeFeed::new(NODES[0]);
        feed.spawn_flusher();
        let demo = Arc::new(Demo { sim: Mutex::new(sim), feed, extra: Mutex::new(extra) });
        let weak = Arc::downgrade(&demo);
        tokio::spawn(async move {
            let mut iv = tokio::time::interval(Duration::from_secs(1));
            iv.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
            let mut n: u64 = 0;
            loop {
                iv.tick().await;
                let Some(d) = weak.upgrade() else { break };
                d.step(now_ms(), n);
                n += 1;
            }
        });
        demo
    }

    /// One tick of the simulation, and the change events it makes.
    fn step(&self, now: i64, n: u64) {
        let mut g = self.sim.lock();
        let s = &mut *g;
        let before: Vec<HostSig> = s.hosts.iter().map(sig).collect();
        let cases = s.cases.iter().map(|c| (c.id, c.updated_at_ms)).collect::<HashMap<_, _>>();
        s.tick(now);
        // a host flapping in and out of backpressure every few seconds
        if n.is_multiple_of(3) {
            let i = s.rng.below(s.hosts.len().min(400));
            let reason = *s.rng.pick(&[
                BackpressureReason::InflightFull,
                BackpressureReason::NodeInflightFull,
                BackpressureReason::QueueFull,
            ]);
            let h = &mut s.hosts[i];
            match h.status {
                HostStatus::Connected => {
                    h.status = HostStatus::Backpressure;
                    h.backpressure = Some(reason);
                }
                HostStatus::Backpressure => {
                    h.status = HostStatus::Connected;
                    h.backpressure = None;
                }
                _ => {}
            }
        }
        // a consumer leaving and another arriving
        if n % 10 == 7 && s.consumers.len() > 1 {
            let i = 1 + s.rng.below(s.consumers.len() - 1);
            let gone = s.consumers.remove(i);
            self.consumer_change(&gone, "disconnect");
            let mut c = gone.clone();
            c.id = s.next_consumer;
            s.next_consumer += 1;
            c.connected_since_ms = now;
            c.node = NODES[s.rng.below(NODES.len())].into();
            self.consumer_change(&c, "connect");
            s.consumers.push(c);
        }
        if n % 5 == 2 {
            let hint = serde_json::json!({ "inProgress": n % 20 < 10 });
            self.feed.touch_as(NODES[0], changes::ChangeKind::Discovery, "plc", Some(hint), true);
        }
        if n % 4 == 1 {
            let hint = serde_json::json!({ "caughtUp": n % 40 >= 20 });
            self.feed.touch_as(NODES[0], changes::ChangeKind::Plc, "export", Some(hint), true);
        }
        for (h, was) in s.hosts.iter_mut().zip(&before) {
            if sig(h) != *was {
                self.host_changed(h, now, false);
            }
        }
        let opened: Vec<(u64, CaseStatus)> =
            s.cases.iter().filter(|c| cases.get(&c.id) != Some(&c.updated_at_ms)).map(|c| (c.id, c.status)).collect();
        for (id, st) in opened {
            self.feed.touch(changes::ChangeKind::Case, id.to_string(), Some(serde_json::json!({ "status": st })), true);
        }
    }

    /// The row's new version, made now (an action) or at the next flush.
    fn host_changed(&self, h: &mut SimHost, now: i64, at_once: bool) {
        let hint = serde_json::json!({
            "status": h.status,
            "backpressureReason": h.backpressure.filter(|_| h.status == HostStatus::Backpressure),
            "tier": h.tier,
        });
        let kind = changes::ChangeKind::Host;
        h.version = Some(match at_once {
            true => self.feed.emit(kind, h.name.clone(), Some(hint), true),
            false => self.feed.touch(kind, h.name.clone(), Some(hint), true),
        });
        h.updated_at = Some(now);
    }

    fn consumer_change(&self, c: &Consumer, event: &str) {
        let id = format!("{}/{}", c.node, c.id);
        let hint = serde_json::json!({ "event": event });
        self.feed.touch_as(&c.node, changes::ChangeKind::Consumer, id, Some(hint), true);
    }

    fn rules_changed(&self, s: &mut Sim) {
        s.rules_version += 1;
        self.feed.publish_versioned(changes::ChangeKind::Rules, "rules", s.rules_version.to_string(), None, true);
    }

    fn policy_changed(&self, version: u64) {
        self.feed.publish_versioned(changes::ChangeKind::Policy, "policy", version.to_string(), None, true);
    }

    /// A takedown or a lift committed at the simulated log's next seq.
    fn logged(&self, s: &mut Sim, kind: changes::ChangeKind, id: &str, hint: serde_json::Value) {
        s.last_seq += 1;
        self.feed.publish_versioned(kind, id, s.last_seq.to_string(), Some(hint), false);
    }
}

type HostSig = (HostStatus, String, Option<u64>, Option<BackpressureReason>);

fn sig(h: &SimHost) -> HostSig {
    (h.status, h.tier.clone(), h.throttle.map(f64::to_bits), h.backpressure)
}

fn now_ms() -> i64 {
    std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH).map(|d| d.as_millis() as i64).unwrap_or(0)
}

// ---------------------------------------------------------------- rng

struct Rng(u64);

impl Rng {
    fn next(&mut self) -> u64 {
        self.0 = self.0.wrapping_add(0x9E37_79B9_7F4A_7C15);
        let mut z = self.0;
        z = (z ^ (z >> 30)).wrapping_mul(0xBF58_476D_1CE4_E5B9);
        z = (z ^ (z >> 27)).wrapping_mul(0x94D0_49BB_1331_11EB);
        z ^ (z >> 31)
    }
    fn f(&mut self) -> f64 {
        (self.next() >> 11) as f64 / (1u64 << 53) as f64
    }
    fn range(&mut self, lo: f64, hi: f64) -> f64 {
        lo + (hi - lo) * self.f()
    }
    fn below(&mut self, n: usize) -> usize {
        (self.next() % n.max(1) as u64) as usize
    }
    fn chance(&mut self, p: f64) -> bool {
        self.f() < p
    }
    fn pick<'a, T>(&mut self, xs: &'a [T]) -> &'a T {
        &xs[self.below(xs.len())]
    }
    /// ~N(0,1), Box-Muller.
    fn normal(&mut self) -> f64 {
        let u = self.f().max(1e-12);
        let v = self.f();
        (-2.0 * u.ln()).sqrt() * (std::f64::consts::TAU * v).cos()
    }
    /// Multiplicative noise with median 1.
    fn jitter(&mut self, sigma: f64) -> f64 {
        (self.normal() * sigma).exp()
    }
}

fn hash(s: &str) -> u64 {
    // FNV-1a: stable across runs, which std's hasher isn't
    s.bytes().fold(0xcbf2_9ce4_8422_2325u64, |h, b| (h ^ b as u64).wrapping_mul(0x0000_0100_0000_01b3))
}

// ---------------------------------------------------------------- model

#[derive(Clone, Copy, PartialEq)]
enum Profile {
    Big,
    Community,
    SelfHosted,
    /// A buggy implementation: lots of invalid commits and out-of-order revs.
    Buggy,
    /// Account farm: new accounts far over threshold.
    SpamAccounts,
    /// Forged or broken signatures.
    SpamSigs,
    /// One account flooding.
    Flood,
}

struct SimHost {
    name: String,
    tier: String,
    status: HostStatus,
    profile: Profile,
    base_rate: f64,
    err_base: f64,
    accounts: u64,
    new_accounts_per_hour: f64,
    seq: i64,
    connected_since: Option<i64>,
    redial_at: Option<i64>,
    lag_base: f64,
    throttle: Option<f64>,
    /// Why the relay pauses it, while its status is `backpressure`.
    backpressure: Option<BackpressureReason>,
    rate: f64,
    err: f64,
    lag: f64,
    series: VecDeque<(i64, f32, f32)>,
    recent: VecDeque<RejectSample>,
    by_reason: BTreeMap<RejectReason, u64>,
    actions: Vec<HostActionRecord>,
    shard: usize,
    version: Option<String>,
    updated_at: Option<i64>,
}

struct Sample {
    t: i64,
    ev_in: f64,
    ev_out: f64,
    b_in: f64,
    b_out: f64,
    p50: f64,
    p99: f64,
    dur: f64,
    rejects: BTreeMap<RejectReason, f64>,
}

struct Sim {
    rng: Rng,
    now_ms: i64,
    hosts: Vec<SimHost>,
    by_name: HashMap<String, usize>,
    history: VecDeque<Sample>,
    last_seq: i64,
    consumers: Vec<Consumer>,
    next_consumer: u64,
    policy: PolicyDoc,
    audit: Vec<PolicyAudit>,
    rules: Vec<DomainRule>,
    next_rule: u64,
    rules_version: u64,
    cases: Vec<Case>,
    next_case: u64,
    accounts: Vec<Account>,
    takedowns: HashMap<String, Takedown>,
    host_shards: Vec<Option<String>>,
    did_shards: Vec<Option<String>>,
    surge: f64,
}

const MUSHROOMS: [&str; 24] = [
    "amanita",
    "boletus",
    "chanterelle",
    "morel",
    "enoki",
    "shiitake",
    "oyster",
    "puffball",
    "porcini",
    "truffle",
    "matsutake",
    "maitake",
    "lionsmane",
    "cordyceps",
    "reishi",
    "inkcap",
    "blewit",
    "russula",
    "lactarius",
    "hydnum",
    "agaric",
    "coprinus",
    "pholiota",
    "suillus",
];
const WORDS: [&str; 48] = [
    "moss", "fern", "lichen", "cedar", "harbor", "signal", "static", "pixel", "kettle", "orbit", "quartz", "ember",
    "tidal", "basalt", "copper", "willow", "hollow", "lantern", "cobalt", "meadow", "falcon", "otter", "badger",
    "heron", "juniper", "sparrow", "nimbus", "delta", "granite", "rowan", "thistle", "aurora", "cinder", "drift",
    "fable", "glade", "haze", "ivory", "jetty", "knoll", "lumen", "marsh", "nectar", "onyx", "prairie", "quill",
    "raven", "sable",
];
const NAMES: [&str; 24] = [
    "alex", "sam", "jo", "kai", "rin", "max", "lee", "ana", "noa", "eli", "mia", "tom", "ivy", "zoe", "ben", "lou",
    "ada", "ray", "sol", "ola", "jun", "kit", "pia", "rex",
];
const TLDS: [&str; 10] = ["com", "dev", "social", "net", "org", "xyz", "io", "blue", "cloud", "me"];

/// The real relay's defaults, so the demo's tiers are the relay's.
fn default_policy() -> Policy {
    crate::policy::admin::to_wire(&crate::policy::doc::PolicyBody::default())
}

impl Sim {
    fn new(seed: u64, now: i64) -> Sim {
        let mut rng = Rng(seed);
        let mut hosts = Vec::with_capacity(5_000);
        let add = |hosts: &mut Vec<SimHost>,
                   rng: &mut Rng,
                   name: String,
                   tier: &str,
                   profile: Profile,
                   base_rate: f64,
                   accounts: u64| {
            let (err_base, newh) = match profile {
                Profile::Big => (rng.range(0.0001, 0.0006), accounts as f64 * 0.0004),
                Profile::Community => (rng.range(0.0005, 0.004), accounts as f64 * 0.001),
                Profile::SelfHosted => {
                    (if rng.chance(0.03) { rng.range(0.01, 0.06) } else { rng.range(0.0, 0.003) }, 0.0)
                }
                Profile::Buggy => (rng.range(0.25, 0.45), rng.range(0.0, 3.0)),
                Profile::SpamAccounts => (rng.range(0.02, 0.06), rng.range(900.0, 2400.0)),
                Profile::SpamSigs => (rng.range(0.3, 0.6), rng.range(10.0, 60.0)),
                Profile::Flood => (rng.range(0.01, 0.03), rng.range(0.0, 2.0)),
            };
            let since = now - (rng.range(60.0, 86_400.0 * 9.0) * 1000.0) as i64;
            hosts.push(SimHost {
                shard: (hash(&name) % HOST_SHARDS as u64) as usize,
                version: None,
                updated_at: None,
                name,
                tier: tier.into(),
                status: HostStatus::Connected,
                profile,
                base_rate,
                err_base,
                accounts,
                new_accounts_per_hour: newh,
                seq: (rng.range(1e3, 1e6) * (1.0 + base_rate)) as i64,
                connected_since: Some(since),
                redial_at: None,
                lag_base: match profile {
                    Profile::Big => rng.range(4.0, 12.0),
                    _ => rng.range(15.0, 180.0),
                },
                throttle: None,
                backpressure: None,
                rate: base_rate,
                err: err_base,
                lag: 0.0,
                series: VecDeque::with_capacity(HOST_HISTORY),
                recent: VecDeque::with_capacity(RECENT_REJECTS),
                by_reason: BTreeMap::new(),
                actions: Vec::new(),
            });
        };
        for (i, m) in MUSHROOMS.iter().enumerate() {
            let region = if i % 3 == 2 { "us-west" } else { "us-east" };
            let accounts = rng.range(1.1e6, 1.7e6) as u64;
            let rate = rng.range(1_400.0, 2_300.0);
            add(
                &mut hosts,
                &mut rng,
                format!("{m}.{region}.host.bsky.network"),
                "trusted",
                Profile::Big,
                rate,
                accounts,
            );
        }
        for i in 0..60 {
            let w = WORDS[i % WORDS.len()];
            let name = match i % 4 {
                0 => format!("pds.{w}.social"),
                1 => format!("{w}{}.blue", rng.pick(&["sky", "town", "club", "space"])),
                2 => format!("pds.{w}-{}.org", rng.pick(&["collective", "coop", "commons", "guild"])),
                _ => format!("bsky.{w}.{}", rng.pick(&["net", "dev", "community"])),
            };
            // WORDS wraps around before the 60th
            let name =
                if hosts.iter().any(|h| h.name == name) { name.replacen('.', &format!("{i}."), 1) } else { name };
            let accounts = (rng.range(7.5, 11.3)).exp() as u64;
            let rate = accounts as f64 * rng.range(0.0006, 0.0018);
            let tier = if accounts > 20_000 { "trusted" } else { "default" };
            add(&mut hosts, &mut rng, name, tier, Profile::Community, rate, accounts);
        }
        for (i, name) in
            ["pds.patchwork-proto.dev", "atp.homebrew-pds.net", "pds.experimental-sync.org", "repo.toy-pds.xyz"]
                .iter()
                .enumerate()
        {
            let accounts = 20 + i as u64 * 37;
            add(&mut hosts, &mut rng, name.to_string(), "default", Profile::Buggy, 0.8 + i as f64 * 1.1, accounts);
        }
        let spam: [(&str, Profile, f64, u64); 7] = [
            ("pds-7f3a.fastvps.cloud", Profile::SpamAccounts, 38.0, 14_200),
            ("pds-91c2.fastvps.cloud", Profile::SpamAccounts, 22.0, 8_900),
            ("social-boost.click", Profile::SpamAccounts, 30.0, 11_400),
            ("free-followers.xyz", Profile::SpamSigs, 18.0, 2_300),
            ("pds.cryptoairdrop.live", Profile::SpamSigs, 9.0, 640),
            ("reply-guy.network", Profile::Flood, 48.0, 3),
            ("autopost.megabot.io", Profile::Flood, 31.0, 12),
        ];
        for (name, p, rate, accounts) in spam {
            add(&mut hosts, &mut rng, name.to_string(), "new", p, rate, accounts);
        }
        // a shared PDS host's customers, and its own PDS, which an exact rule lifts out of the
        // wildcard's tier
        for (i, name) in ["demo", "pds-1", "pds-2", "pds-3", "eu.pds-4"].iter().enumerate() {
            let accounts = 40 + i as u64 * 130;
            add(
                &mut hosts,
                &mut rng,
                format!("{name}.example.social"),
                "default",
                Profile::Community,
                accounts as f64 * 0.001,
                accounts,
            );
        }
        while hosts.len() < 5_000 {
            let name = match rng.below(6) {
                0 => format!("pds.{}{}.{}", rng.pick(&NAMES), rng.pick(&WORDS), rng.pick(&TLDS)),
                1 => format!("{}.{}.{}", rng.pick(&WORDS), rng.pick(&NAMES), rng.pick(&TLDS)),
                2 => format!("bsky.{}{}.{}", rng.pick(&NAMES), rng.below(100), rng.pick(&TLDS)),
                3 => format!("pds.{}-{}.{}", rng.pick(&WORDS), rng.pick(&WORDS), rng.pick(&TLDS)),
                4 => format!("{}.pds.{}{}.{}", rng.pick(&NAMES), rng.pick(&WORDS), rng.below(10), rng.pick(&TLDS)),
                _ => format!("atproto.{}{}.{}", rng.pick(&WORDS), rng.pick(&NAMES), rng.pick(&TLDS)),
            };
            if hosts.iter().any(|h| h.name == name) {
                continue;
            }
            // most self-hosted PDSes hold one or two accounts
            let accounts = if rng.chance(0.7) { 1 + rng.below(3) as u64 } else { rng.range(1.0, 7.0).exp() as u64 };
            let rate = accounts as f64 * rng.range(0.0002, 0.004) * rng.jitter(0.8);
            let tier = if rng.chance(0.3) { "new" } else { "default" };
            add(&mut hosts, &mut rng, name, tier, Profile::SelfHosted, rate, accounts);
        }
        // the long tail's connection churn
        for h in hosts.iter_mut().skip(80) {
            if h.profile != Profile::SelfHosted {
                continue;
            }
            let r = rng.f();
            if r < 0.035 {
                h.status = HostStatus::Offline;
                h.connected_since = None;
            } else if r < 0.05 {
                h.status = HostStatus::Backoff;
                h.connected_since = None;
                h.redial_at = Some(now + (rng.range(2.0, 40.0) * 1000.0) as i64);
            }
        }
        let mut by_name = HashMap::new();
        for (i, h) in hosts.iter().enumerate() {
            by_name.insert(h.name.clone(), i);
        }

        let assign = |n: usize, skew: usize| -> Vec<Option<String>> {
            (0..n).map(|i| Some(NODES[(i * 7 + i / skew) % NODES.len()].to_string())).collect()
        };
        let host_shards = assign(HOST_SHARDS, 9);
        let did_shards = assign(DID_SHARDS, 11);

        let policy = default_policy();
        let mut sim = Sim {
            rng,
            now_ms: now,
            hosts,
            by_name,
            history: VecDeque::with_capacity(HISTORY + 1),
            last_seq: 24_811_204_377,
            consumers: Vec::new(),
            next_consumer: 1,
            policy: PolicyDoc { version: 0, policy: policy.clone(), updated_at_ms: now, updated_by: "admin".into() },
            audit: Vec::new(),
            rules: Vec::new(),
            next_rule: 1,
            rules_version: 1,
            cases: Vec::new(),
            next_case: 1,
            accounts: Vec::new(),
            takedowns: HashMap::new(),
            host_shards,
            did_shards,
            surge: 0.0,
        };
        sim.seed_policy_history(now);
        sim.seed_rules(now);
        sim.seed_consumers(now);
        sim.seed_accounts(now);
        sim.seed_moderation(now);
        sim
    }

    fn seed_policy_history(&mut self, now: i64) {
        let day = 86_400_000;
        let mut p = default_policy();
        p.tiers.get_mut("default").unwrap().events_per_sec = 30.0;
        p.spam.new_accounts_per_hour = 500;
        type Step = (i64, &'static str, Box<dyn Fn(&mut Policy)>);
        let steps: [Step; 3] = [
            (now - 21 * day, "initial limits", Box::new(|_| {})),
            (
                now - 9 * day,
                "default tier was clipping small community PDSes at peak",
                Box::new(|p| {
                    p.tiers.get_mut("default").unwrap().events_per_sec = 51.0;
                }),
            ),
            (
                now - 2 * day - 3_600_000,
                "account farms on fastvps were staying under 500/h",
                Box::new(|p| {
                    p.spam.new_accounts_per_hour = 300;
                }),
            ),
        ];
        let mut prev: Option<Policy> = None;
        for (at, note, f) in steps {
            f(&mut p);
            let changes = match &prev {
                None => vec!["created".to_string()],
                Some(old) => diff_json(&serde_json::to_value(old).unwrap(), &serde_json::to_value(&p).unwrap()),
            };
            self.policy.version += 1;
            self.audit.push(PolicyAudit {
                version: self.policy.version,
                at_ms: at,
                by: "admin".into(),
                note: note.into(),
                changes,
            });
            self.policy.updated_at_ms = at;
            prev = Some(p.clone());
        }
        self.policy.policy = p;
    }

    fn seed_rules(&mut self, now: i64) {
        let rules = [
            ("*.cryptoairdrop.live", RuleEffect::Ban, "forged commits, every subdomain is the same operator", 6),
            ("*.fastvps.cloud", RuleEffect::Tier { tier: "new".into() }, "cheap VPS range used by account farms", 3),
            ("*.host.bsky.network", RuleEffect::Tier { tier: "trusted".into() }, "Bluesky's PDS fleet", 30),
            ("*.megabot.io", RuleEffect::Throttle { events_per_sec: 5.0 }, "bot platform, fine at low volume", 1),
            ("*.example.social", RuleEffect::Tier { tier: "new".into() }, "shared PDS host: customers start new", 2),
            ("demo.example.social", RuleEffect::Tier { tier: "trusted".into() }, "the host's own PDS", 1),
        ];
        for (pattern, effect, note, days) in rules {
            let id = self.next_rule;
            self.next_rule += 1;
            self.rules.push(DomainRule {
                id,
                pattern: pattern.into(),
                effect: effect.clone(),
                note: note.into(),
                created_at_ms: now - days * 86_400_000,
                created_by: "admin".into(),
                matches: 0,
                version: 0,
            });
            self.apply_rule_effect(pattern, &effect);
        }
        for i in 0..self.hosts.len() {
            if self.hosts[i].profile == Profile::SelfHosted && self.rng.chance(0.0016) {
                self.hosts[i].status = HostStatus::Banned;
                self.hosts[i].connected_since = None;
            }
        }
    }

    fn seed_consumers(&mut self, now: i64) {
        let uas = [
            ("go-indigo/0.0 (jetstream)", 0.0),
            ("bsky-appview/1.0", 0.0),
            ("Go-http-client/1.1", 0.0),
            ("python-websockets/13.1", 0.0),
            ("atproto-firehose-rs/0.4", 0.0),
            ("skyfeed-builder/2", 0.0),
            ("graze-social/1.3", 0.0),
            ("node-ws/8.18", 0.0),
            ("labeler-ozone/0.1", 0.0),
            ("search-indexer/0.9", 0.0),
        ];
        for i in 0..23 {
            let (ua, _) = uas[i % uas.len()];
            let backfilling = i % 13 == 5;
            let ip = if i % 5 == 0 {
                format!("2a01:4f8:{:x}:{:x}::1", 0x1000 + self.rng.below(0xeff), self.rng.below(0xffff))
            } else {
                format!(
                    "{}.{}.{}.{}",
                    self.rng.pick(&[5, 23, 34, 45, 65, 88, 104, 138, 147, 157, 172, 185, 203]),
                    self.rng.below(255),
                    self.rng.below(255),
                    1 + self.rng.below(253)
                )
            };
            self.consumers.push(Consumer {
                id: self.next_consumer,
                ip,
                user_agent: ua.into(),
                node: NODES[i % NODES.len()].into(),
                connected_since_ms: now - (self.rng.range(30.0, 86_400.0 * 6.0) * 1000.0) as i64,
                cursor: 0,
                lag_ms: if backfilling { self.rng.range(3.6e6, 4.0e7) } else { 0.0 },
                events_per_sec: 0.0,
                bytes_per_sec: 0.0,
                backfilling,
                read_tier: if !backfilling {
                    "ring"
                } else if i % 2 == 0 {
                    "disk"
                } else {
                    "bucket"
                }
                .into(),
            });
            self.next_consumer += 1;
        }
    }

    fn seed_accounts(&mut self, now: i64) {
        const B32: &[u8] = b"abcdefghijklmnopqrstuvwxyz234567";
        for i in 0..600 {
            // weight by activity: most sampled accounts live on the big hosts
            let hi = if i < 420 {
                self.rng.below(24)
            } else if i < 470 {
                24 + self.rng.below(60)
            } else if i < 520 {
                84 + self.rng.below(11)
            } else {
                95 + self.rng.below(self.hosts.len() - 95)
            };
            let did = format!("did:plc:{}", (0..24).map(|_| B32[self.rng.below(32)] as char).collect::<String>());
            let host = self.hosts[hi].name.clone();
            let handle = if hi < 24 {
                format!("{}{}.bsky.social", self.rng.pick(&WORDS), self.rng.below(1000))
            } else {
                let base = host.trim_start_matches("pds.").trim_start_matches("bsky.").to_string();
                format!("{}.{base}", self.rng.pick(&NAMES))
            };
            let a = self.synth_account(did, Some(handle), hi, now);
            self.accounts.push(a);
        }
        let did = self.accounts[3].did.clone();
        self.takedowns.insert(
            did,
            Takedown {
                at_ms: now - 5 * 86_400_000,
                by: "admin".into(),
                reason: "impersonation of a news org (report #4417)".into(),
            },
        );
    }

    fn synth_account(&mut self, did: String, handle: Option<String>, hi: usize, now: i64) -> Account {
        let h = &self.hosts[hi];
        let spam = matches!(h.profile, Profile::SpamAccounts | Profile::SpamSigs | Profile::Flood);
        let per_acct = h.base_rate / h.accounts.max(1) as f64;
        let shard = (hash(&did) % DID_SHARDS as u64) as u32;
        let upstream = if self.rng.chance(0.02) { "deactivated" } else { "active" };
        Account {
            handle,
            host: h.name.clone(),
            status: if spam { "throttled".into() } else { upstream.into() },
            upstream_status: upstream.into(),
            takedown: None,
            rev: format!("3m{}", (0..11).map(|_| (b'a' + self.rng.below(26) as u8) as char).collect::<String>()),
            last_seq: self.last_seq - self.rng.below(5_000_000) as i64,
            last_event_ms: now - (self.rng.range(0.5, 3.0 * 86_400.0) * 1000.0) as i64,
            events_last_hour: (per_acct * 3600.0 * self.rng.jitter(1.0)) as u64,
            rejects_last_hour: if spam { self.rng.below(400) as u64 } else { 0 },
            did_shard: shard,
            node: self.did_shards[shard as usize].clone().unwrap_or_default(),
            did,
        }
    }

    fn seed_moderation(&mut self, now: i64) {
        // a resolved and a dismissed case, so the list shows history
        let old = [
            (
                "pds.cryptoairdrop.live",
                "bad-signatures",
                Severity::Critical,
                CaseStatus::Resolved,
                6.0,
                412.0,
                60.0,
                "Banned *.cryptoairdrop.live (rule 1).",
            ),
            (
                "pds.moss-harbor.dev",
                "reject-ratio",
                Severity::Warn,
                CaseStatus::Dismissed,
                3.0,
                0.31,
                0.2,
                "Bad deploy on their side, fixed within the hour.",
            ),
        ];
        for (host, kind, sev, status, days, obs, thr, note) in old {
            let id = self.next_case;
            self.next_case += 1;
            let opened = now - (days * 86_400_000.0) as i64;
            self.cases.push(Case {
                id,
                host: host.into(),
                did: None,
                kind: kind.into(),
                severity: sev,
                status,
                opened_at_ms: opened,
                updated_at_ms: opened + 2_400_000,
                summary: case_summary(kind, obs, thr),
                observed: obs,
                threshold: thr,
                auto_action: None,
                notes: vec![CaseNote { at_ms: opened + 2_400_000, by: "admin".into(), text: note.into() }],
            });
        }
    }

    /// Applies to the hosts the rule decides, not those a more specific rule takes.
    fn apply_rule_effect(&mut self, pattern: &str, effect: &RuleEffect) -> u32 {
        let mut n = 0;
        let decides: Vec<bool> =
            self.hosts.iter().map(|h| self.rule_for(&h.name).is_some_and(|r| r.pattern == pattern)).collect();
        for (h, _) in self.hosts.iter_mut().zip(decides).filter(|(_, d)| *d) {
            n += 1;
            match effect {
                RuleEffect::Ban => {
                    h.status = HostStatus::Banned;
                    h.connected_since = None;
                }
                RuleEffect::Tier { tier } => h.tier = tier.clone(),
                RuleEffect::Throttle { events_per_sec } => h.throttle = Some(*events_per_sec),
                RuleEffect::Allow => {}
            }
        }
        n
    }

    // ------------------------------------------------------------ tick

    fn tick(&mut self, now: i64) {
        self.now_ms = now;
        let t = now as f64 / 1000.0;
        // a compressed "day" (20 min) so the demo visibly moves, plus a slow wander
        let diurnal =
            1.0 + 0.16 * (t * std::f64::consts::TAU / 1200.0).sin() + 0.05 * (t * std::f64::consts::TAU / 173.0).sin();
        if self.surge <= 0.0 && self.rng.chance(0.006) {
            self.surge = self.rng.range(0.15, 0.45);
        }
        self.surge = (self.surge - 0.01).max(0.0);
        let level = diurnal * (1.0 + self.surge);
        let policy = self.policy.policy.clone();

        let mut ev_in = 0.0;
        let mut rejects: BTreeMap<RejectReason, f64> = BTreeMap::new();
        let mut lag_acc = 0.0;
        for i in 0..self.hosts.len() {
            let jit = self.rng.jitter(0.12);
            let ejit = self.rng.jitter(0.35);
            let roll = self.rng.f();
            let (coin, coin2, pick, acct, back) =
                (self.rng.f(), self.rng.f(), self.rng.next(), self.rng.next(), self.rng.below(900) as i64);
            let h = &mut self.hosts[i];
            match h.status {
                HostStatus::Backoff if h.redial_at.is_some_and(|r| r <= now) => {
                    h.status = HostStatus::Connected;
                    h.connected_since = Some(now);
                    h.redial_at = None;
                }
                HostStatus::Connected | HostStatus::Idle if h.profile == Profile::SelfHosted && roll < 0.00004 => {
                    h.status = HostStatus::Backoff;
                    h.connected_since = None;
                    h.redial_at = Some(now + 5_000 + (roll * 1e9) as i64 % 30_000);
                }
                HostStatus::Offline if roll < 0.0004 => {
                    h.status = HostStatus::Connected;
                    h.connected_since = Some(now);
                }
                _ => {}
            }
            let live = matches!(
                h.status,
                HostStatus::Connected | HostStatus::Idle | HostStatus::Throttled | HostStatus::Backpressure
            );
            let (rate, err) = if live {
                let burst = match h.profile {
                    Profile::SpamAccounts | Profile::Flood => 1.0 + 0.6 * ((t / 37.0 + i as f64).sin()).max(0.0),
                    _ => 1.0,
                };
                let want = h.base_rate * level * jit * burst;
                let tier_cap = policy.tiers.get(&h.tier).map(|l| l.events_per_sec).unwrap_or(f64::INFINITY);
                let cap = h.throttle.unwrap_or(f64::INFINITY).min(tier_cap);
                let rate = want.min(cap);
                // a few hosts at a time wait on the relay (an identity backlog, a full in-flight
                // cap), so the console's backpressure states always have someone in them
                h.backpressure = match i % 211 {
                    5 => Some(BackpressureReason::QueueFull),
                    18 if (t / 47.0 + i as f64).sin() > 0.2 => Some(BackpressureReason::QueueFull),
                    29 if (t / 61.0 + i as f64).sin() > 0.4 => Some(BackpressureReason::InflightFull),
                    _ => None,
                };
                h.status = if want > cap * 1.001 {
                    HostStatus::Throttled
                } else if h.backpressure.is_some() {
                    HostStatus::Backpressure
                } else if h.profile == Profile::SelfHosted
                    // a band, so a host near the line doesn't flap every tick
                    && (rate < 0.002 || (h.status == HostStatus::Idle && rate < 0.01))
                {
                    HostStatus::Idle
                } else {
                    HostStatus::Connected
                };
                (rate, (h.err_base * ejit).min(0.95))
            } else {
                (0.0, 0.0)
            };
            h.rate = rate;
            h.err = err;
            h.lag = if live { h.lag_base * jit * (1.0 + self.surge) } else { 0.0 };
            h.seq += (rate + if rate > 0.0 { 0.5 } else { 0.0 }) as i64;
            if h.series.len() == HOST_HISTORY {
                h.series.pop_front();
            }
            let rej = rate * err;
            h.series.push_back((now / 1000, rate as f32, rej as f32));
            ev_in += rate;
            lag_acc += h.lag * rate;
            if rej > 0.0 {
                let reasons = reasons_for(h.profile);
                let mut left = rej;
                for (k, (reason, share)) in reasons.iter().enumerate() {
                    let v = if k + 1 == reasons.len() { left } else { rej * share };
                    left -= v;
                    *rejects.entry(*reason).or_default() += v;
                    *h.by_reason.entry(*reason).or_default() += v.trunc() as u64 + u64::from(coin < v.fract());
                }
                // a sample for the detail page's recent list, not one per reject
                if coin2 < rej.min(1.0) {
                    let (reason, _) = reasons[(pick % reasons.len() as u64) as usize];
                    let did = fake_did(&format!("{}{}", h.name, acct % h.accounts.max(1)));
                    let sample = RejectSample {
                        at_ms: now - back,
                        did,
                        reason,
                        upstream_seq: h.seq,
                        detail: reject_detail(reason).into(),
                    };
                    if h.recent.len() == RECENT_REJECTS {
                        h.recent.pop_front();
                    }
                    h.recent.push_back(sample);
                }
            }
        }
        let rej_total: f64 = rejects.values().sum();
        let ev_out = (ev_in - rej_total).max(0.0);
        self.last_seq += ev_out as i64;

        // consumers: live ones get everything; backfillers replay faster and close the gap
        let mut b_out = 0.0;
        for c in self.consumers.iter_mut() {
            if c.backfilling {
                c.events_per_sec = ev_out * self.rng.range(2.5, 4.0);
                c.lag_ms = (c.lag_ms - 1000.0 * (c.events_per_sec / ev_out.max(1.0) - 1.0)).max(0.0);
                if c.lag_ms < 50.0 {
                    c.backfilling = false;
                }
            } else {
                c.events_per_sec = ev_out * self.rng.jitter(0.01);
                c.lag_ms = self.rng.range(1.0, 25.0) * if self.rng.chance(0.03) { 20.0 } else { 1.0 };
            }
            c.bytes_per_sec = c.events_per_sec * EVENT_BYTES * self.rng.jitter(0.05);
            c.cursor = self.last_seq - (c.lag_ms / 1000.0 * ev_out) as i64;
            b_out += c.bytes_per_sec;
        }
        if self.rng.chance(0.01) && self.consumers.len() < 30 {
            let id = self.next_consumer;
            self.next_consumer += 1;
            self.consumers.push(Consumer {
                id,
                ip: format!(
                    "{}.{}.{}.{}",
                    100 + self.rng.below(100),
                    self.rng.below(255),
                    self.rng.below(255),
                    1 + self.rng.below(253)
                ),
                user_agent: self
                    .rng
                    .pick(&["python-websockets/13.1", "Go-http-client/1.1", "node-ws/8.18"])
                    .to_string(),
                node: self.rng.pick(&NODES).to_string(),
                connected_since_ms: now,
                cursor: self.last_seq,
                lag_ms: 0.0,
                events_per_sec: 0.0,
                bytes_per_sec: 0.0,
                backfilling: false,
                read_tier: "ring".into(),
            });
        } else if self.rng.chance(0.008) && self.consumers.len() > 16 {
            let i = self.rng.below(self.consumers.len());
            self.consumers.remove(i);
        }

        let busy = ev_in / 60_000.0;
        let p50 = 31.0 + 6.0 * busy + self.rng.range(-2.0, 2.0) + 20.0 * self.surge;
        let spike = if self.rng.chance(0.02) { self.rng.range(80.0, 260.0) } else { 0.0 };
        let p99 = p50 * self.rng.range(2.4, 3.1) + spike;
        let dur = 22.0 + 10.0 * busy + self.rng.range(-3.0, 6.0) + spike * 0.4;
        let _ = lag_acc;
        if self.history.len() == HISTORY {
            self.history.pop_front();
        }
        self.history.push_back(Sample {
            t: now / 1000,
            ev_in,
            ev_out,
            b_in: ev_in * EVENT_BYTES * self.rng.jitter(0.04),
            b_out,
            p50,
            p99,
            dur,
            rejects,
        });
        self.check_thresholds(now);
    }

    fn check_thresholds(&mut self, now: i64) {
        let spam = self.policy.policy.spam.clone();
        let tiers = self.policy.policy.tiers.clone();
        let tier_newh = |t: &str| tiers.get(t).map(|l| l.new_accounts_per_hour as f64).unwrap_or(0.0);
        let mut open = Vec::new();
        for (i, h) in self.hosts.iter().enumerate() {
            if !matches!(
                h.status,
                HostStatus::Connected | HostStatus::Throttled | HostStatus::Backpressure | HostStatus::Idle
            ) {
                continue;
            }
            let sigs_per_min = h.rate * h.err * 60.0 * if h.profile == Profile::SpamSigs { 0.8 } else { 0.0 };
            let top_acct = if h.profile == Profile::Flood { h.rate * 0.9 } else { h.rate / h.accounts.max(1) as f64 };
            let checks = [
                // a tier that allows more sign-ups (trusted) raises the bar with it
                ("new-accounts", h.new_accounts_per_hour, (spam.new_accounts_per_hour as f64).max(tier_newh(&h.tier))),
                ("reject-ratio", if h.rate > 0.05 { h.err } else { 0.0 }, spam.reject_ratio),
                ("bad-signatures", sigs_per_min, spam.bad_signatures_per_min as f64),
                ("account-rate", top_acct, spam.account_events_per_sec),
            ];
            for (kind, obs, thr) in checks {
                if obs > thr && thr > 0.0 {
                    open.push((i, kind, obs, thr));
                }
            }
        }
        for (i, kind, obs, thr) in open {
            let name = self.hosts[i].name.clone();
            if self.cases.iter().any(|c| {
                c.host == name && c.kind == kind && matches!(c.status, CaseStatus::Open | CaseStatus::Acknowledged)
            }) {
                continue;
            }
            let ratio = obs / thr;
            let severity = if ratio > 4.0 {
                Severity::Critical
            } else if ratio > 2.0 {
                Severity::High
            } else if ratio > 1.3 {
                Severity::Warn
            } else {
                Severity::Info
            };
            let did = (kind == "account-rate").then(|| fake_did(&name));
            let mut auto_action = None;
            if spam.auto_throttle && severity >= Severity::High {
                let cap = self.policy.policy.tiers.get("new").map(|t| t.events_per_sec).unwrap_or(10.0);
                self.hosts[i].throttle = Some(cap);
                auto_action = Some(format!("throttled to {cap} events/s"));
            }
            let id = self.next_case;
            self.next_case += 1;
            // the first batch (at startup) is backdated so the list has a spread of ages
            let opened =
                if self.history.len() < 5 { now - (self.rng.range(120.0, 30_000.0) * 1000.0) as i64 } else { now };
            self.cases.push(Case {
                id,
                host: name,
                did,
                kind: kind.into(),
                severity,
                status: CaseStatus::Open,
                opened_at_ms: opened,
                updated_at_ms: opened,
                summary: case_summary(kind, obs, thr),
                observed: obs,
                threshold: thr,
                auto_action,
                notes: Vec::new(),
            });
        }
    }

    // ------------------------------------------------------------ views

    fn row(&self, h: &SimHost) -> HostRow {
        HostRow {
            host: h.name.clone(),
            tier: h.tier.clone(),
            status: h.status,
            backpressure_reason: h.backpressure.filter(|_| h.status == HostStatus::Backpressure),
            events_per_sec: round2(h.rate),
            error_rate: (h.err * 10_000.0).round() / 10_000.0,
            accounts: h.accounts,
            last_upstream_seq: h.seq,
            connected_since_ms: h.connected_since,
            lag_ms: round2(h.lag),
            catch_up_pace: (h.lag > 60_000.0).then_some(12.0),
            throttle: h.throttle,
            max_accounts: if h.tier == "trusted" { 10_000_000 } else { 100 },
            history: Vec::new(),
            throttled_accounts: if h.tier == "trusted" { 0 } else { h.accounts.saturating_sub(100).min(5_000) },
            top_reason: h.by_reason.iter().filter(|(_, n)| **n > 0).max_by_key(|(_, n)| **n).map(|(r, _)| *r),
            source: Some(match hash(&h.name) % 5 {
                0 => "requestCrawl".into(),
                1 => "plc".into(),
                _ => "bootstrap:relay1.us-east.bsky.network".into(),
            }),
            rule: self.rule_for(&h.name).map(|r| r.id),
            node: self.host_shards[h.shard].clone().unwrap_or_default(),
            version: h.version.clone(),
            updated_at_ms: h.updated_at,
            owner_version: h.version.clone(),
            pending: false,
        }
    }

    /// The most specific rule, as the relay picks it: the longest name, an exact one first.
    fn rule_for(&self, host: &str) -> Option<&DomainRule> {
        self.rules
            .iter()
            .filter(|r| rule_matches(&r.pattern, host))
            .max_by_key(|r| (r.pattern.trim_start_matches("*.").len(), !r.pattern.starts_with("*.")))
    }

    fn host_idx(&self, host: &str) -> AdminResult<usize> {
        self.by_name
            .get(&host.to_ascii_lowercase())
            .copied()
            .ok_or_else(|| AdminError::NotFound(format!("no host {host}")))
    }

    fn rule_view(&self, r: &DomainRule) -> DomainRule {
        DomainRule {
            matches: self.hosts.iter().filter(|h| self.rule_for(&h.name).is_some_and(|w| w.id == r.id)).count() as u32,
            version: self.rules_version,
            ..r.clone()
        }
    }

    fn account_view(&self, a: &Account) -> Account {
        let mut a = a.clone();
        if let Some(t) = self.takedowns.get(&a.did) {
            a.takedown = Some(t.clone());
            a.status = "takendown".into();
        }
        a
    }

    fn find_account(&mut self, did: &str) -> AdminResult<usize> {
        if let Some(i) = self.accounts.iter().position(|a| a.did == did) {
            return Ok(i);
        }
        let valid = did
            .strip_prefix("did:plc:")
            .is_some_and(|s| s.len() == 24 && s.bytes().all(|b| b.is_ascii_lowercase() || (b'2'..=b'7').contains(&b)))
            || did.strip_prefix("did:web:").is_some_and(|s| !s.is_empty());
        if !valid {
            return Err(AdminError::NotFound(format!("{did} is not a DID this relay has seen")));
        }
        // any well-formed DID resolves to a stable synthetic account
        let hi = (hash(did) % 24) as usize;
        let a = self.synth_account(did.to_string(), None, hi, self.now_ms);
        self.accounts.push(a);
        Ok(self.accounts.len() - 1)
    }
}

/// A stable, well-formed did:plc for a seed string.
fn fake_did(seed: &str) -> String {
    const B32: &[u8] = b"abcdefghijklmnopqrstuvwxyz234567";
    let (mut a, mut b) = (hash(seed), hash(&format!("{seed}#")));
    let mut s = String::from("did:plc:");
    for i in 0..24 {
        let x = if i < 12 { &mut a } else { &mut b };
        s.push(B32[(*x & 31) as usize] as char);
        *x >>= 5;
    }
    s
}

fn round2(x: f64) -> f64 {
    (x * 100.0).round() / 100.0
}

pub fn rule_matches(pattern: &str, host: &str) -> bool {
    match pattern.strip_prefix("*.") {
        Some(base) => host == base || host.strip_suffix(base).is_some_and(|p| p.ends_with('.')),
        None => host == pattern,
    }
}

fn reasons_for(p: Profile) -> &'static [(RejectReason, f64)] {
    use RejectReason::*;
    match p {
        Profile::Buggy => &[(InvalidCommit, 0.45), (RevOutOfOrder, 0.3), (PrevDataMismatch, 0.2), (Malformed, 0.05)],
        Profile::SpamSigs => &[(BadSignature, 0.8), (WrongHost, 0.15), (UnknownDid, 0.05)],
        Profile::SpamAccounts => &[(UnknownDid, 0.5), (RateLimited, 0.4), (BadSignature, 0.1)],
        Profile::Flood => &[(RateLimited, 0.85), (TooLarge, 0.15)],
        Profile::Big => &[(WrongHost, 0.55), (PrevDataMismatch, 0.25), (Takendown, 0.2)],
        Profile::Community | Profile::SelfHosted => {
            &[(RevOutOfOrder, 0.3), (WrongHost, 0.3), (InvalidCommit, 0.2), (TooLarge, 0.2)]
        }
    }
}

fn reject_detail(r: RejectReason) -> &'static str {
    match r {
        RejectReason::BadSignature => "commit signature doesn't verify against the DID document's #atproto key",
        RejectReason::InvalidCommit => "MST root in the CAR doesn't match the commit's data CID",
        RejectReason::RevOutOfOrder => "rev is not after the last rev the relay accepted",
        RejectReason::PrevDataMismatch => "prevData doesn't match the last accepted commit's data",
        RejectReason::WrongHost => "DID document names a different PDS",
        RejectReason::UnknownDid => "DID didn't resolve (PLC 404)",
        RejectReason::TooLarge => "frame over 2 MiB",
        RejectReason::RateLimited => "host over its tier's events/s",
        RejectReason::Takendown => "account is taken down on this relay",
        RejectReason::Inactive => "account is throttled, deactivated or suspended on this relay",
        RejectReason::Malformed => "frame header isn't valid DAG-CBOR",
    }
}

fn case_summary(kind: &str, obs: f64, thr: f64) -> String {
    match kind {
        "new-accounts" => format!("{obs:.0} new accounts/h (threshold {thr:.0})"),
        "reject-ratio" => format!("{:.0}% of frames rejected (threshold {:.0}%)", obs * 100.0, thr * 100.0),
        "bad-signatures" => format!("{obs:.0} bad signatures/min (threshold {thr:.0})"),
        "account-rate" => format!("one account at {obs:.1} events/s (threshold {thr:.0})"),
        _ => format!("{obs:.2} over {thr:.2}"),
    }
}

fn validate_pattern(p: &str) -> AdminResult<String> {
    let p = p.trim().to_ascii_lowercase();
    let base = p.strip_prefix("*.").unwrap_or(&p);
    let ok = base.contains('.')
        && base
            .split('.')
            .all(|l| !l.is_empty() && l.len() <= 63 && l.bytes().all(|b| b.is_ascii_alphanumeric() || b == b'-'));
    if !ok {
        return Err(AdminError::BadRequest(format!("{p:?} isn't a hostname or *.domain pattern")));
    }
    Ok(p)
}

// ---------------------------------------------------------------- AdminSource

impl AdminSource for Demo {
    fn changes(&self) -> Option<Arc<changes::ChangeFeed>> {
        Some(self.feed.clone())
    }

    async fn overview(&self) -> AdminResult<Overview> {
        let s = self.sim.lock();
        let last = s.history.back().expect("seeded");
        let mut by_status = BTreeMap::new();
        for h in &s.hosts {
            *by_status.entry(h.status).or_insert(0u32) += 1;
        }
        let window = s.history.iter().rev().take(60).collect::<Vec<_>>();
        let mut rejects_by_reason: BTreeMap<RejectReason, f64> = BTreeMap::new();
        for x in &window {
            for (r, v) in &x.rejects {
                *rejects_by_reason.entry(*r).or_default() += v / window.len() as f64;
            }
        }
        let mut idx: Vec<usize> = (0..s.hosts.len()).collect();
        idx.sort_by(|a, b| s.hosts[*b].rate.total_cmp(&s.hosts[*a].rate));
        let mut hist = History { sample_secs: 1, ..Default::default() };
        for x in &s.history {
            hist.t.push(x.t);
            hist.events_in.push(round2(x.ev_in));
            hist.events_out.push(round2(x.ev_out));
            hist.bytes_in.push(x.b_in.round());
            hist.bytes_out.push(x.b_out.round());
            hist.ttf_p50_ms.push(round2(x.p50));
            hist.ttf_p99_ms.push(round2(x.p99));
            hist.durability_lag_ms.push(round2(x.dur));
            for r in RejectReason::ALL {
                hist.rejects.entry(r).or_default().push(round2(x.rejects.get(&r).copied().unwrap_or(0.0)));
            }
        }
        let connected = s
            .hosts
            .iter()
            .filter(|h| {
                matches!(
                    h.status,
                    HostStatus::Connected | HostStatus::Idle | HostStatus::Throttled | HostStatus::Backpressure
                )
            })
            .count();
        Ok(Overview {
            time_ms: s.now_ms,
            events_in_per_sec: round2(last.ev_in),
            events_out_per_sec: round2(last.ev_out),
            bytes_in_per_sec: last.b_in.round(),
            bytes_out_per_sec: last.b_out.round(),
            consumers: s.consumers.len() as u32,
            hosts_connected: connected as u32,
            hosts_total: s.hosts.len() as u32,
            hosts_by_status: by_status,
            rejects_per_sec: round2(last.rejects.values().sum()),
            rejects_by_reason: rejects_by_reason.into_iter().map(|(k, v)| (k, round2(v))).collect(),
            time_to_firehose_p50_ms: round2(last.p50),
            time_to_firehose_p99_ms: round2(last.p99),
            commit_lag_ms: round2(last.dur),
            last_seq: s.last_seq,
            open_cases: s.cases.iter().filter(|c| c.status == CaseStatus::Open).count() as u32,
            top_hosts: idx
                .iter()
                .take(12)
                .map(|&i| {
                    let h = &s.hosts[i];
                    let history = h.series.iter().rev().take(60).rev().map(|x| round2(x.1 as f64)).collect();
                    HostRow { history, ..s.row(h) }
                })
                .collect(),
            history: hist,
            stream_events_per_sec: round2(last.ev_in),
            by_node: Vec::new(),
        })
    }

    async fn hosts(&self, q: HostQuery) -> AdminResult<HostList> {
        let s = self.sim.lock();
        let needle = q.q.as_deref().map(str::to_ascii_lowercase).filter(|x| !x.is_empty());
        let mut rows: Vec<HostRow> = s
            .hosts
            .iter()
            .filter(|h| needle.as_deref().is_none_or(|n| h.name.contains(n)))
            .filter(|h| q.tier.as_deref().is_none_or(|t| h.tier == t))
            .filter(|h| q.status.is_none_or(|st| h.status == st))
            .map(|h| s.row(h))
            .filter(|r| q.keeps(r))
            .collect();
        let key = q.sort.as_deref().unwrap_or("events");
        rows.sort_by(|a, b| {
            let o = match key {
                "host" => a.host.cmp(&b.host),
                "tier" => a.tier.cmp(&b.tier),
                "status" => a.status.cmp(&b.status),
                "errors" => a.error_rate.total_cmp(&b.error_rate),
                "accounts" => a.accounts.cmp(&b.accounts),
                "seq" => a.last_upstream_seq.cmp(&b.last_upstream_seq),
                "since" => a.connected_since_ms.cmp(&b.connected_since_ms),
                "lag" => a.lag_ms.total_cmp(&b.lag_ms),
                "throttled" => a.throttled_accounts.cmp(&b.throttled_accounts),
                "source" => a.source.cmp(&b.source),
                _ => a.events_per_sec.total_cmp(&b.events_per_sec),
            };
            if q.desc { o.reverse() } else { o }
        });
        let total = rows.len();
        let rows = rows.into_iter().skip(q.offset.unwrap_or(0)).take(q.limit.unwrap_or(10_000)).collect();
        Ok(HostList { total, hosts: rows })
    }

    async fn host(&self, host: &str) -> AdminResult<HostDetail> {
        let s = self.sim.lock();
        let h = &s.hosts[s.host_idx(host)?];
        let mut limits = s
            .policy
            .policy
            .tiers
            .get(&h.tier)
            .cloned()
            .unwrap_or_else(|| s.policy.policy.tiers[&s.policy.policy.default_tier].clone());
        if let Some(t) = h.throttle {
            limits.events_per_sec = limits.events_per_sec.min(t);
        }
        let mut series = HostSeries { sample_secs: 1, ..Default::default() };
        for (t, e, r) in &h.series {
            series.t.push(*t);
            series.events.push(round2(*e as f64));
            series.rejects.push(round2(*r as f64));
        }
        Ok(HostDetail {
            row: s.row(h),
            limits,
            new_accounts_per_hour: round2(h.new_accounts_per_hour),
            rejects_by_reason: h.by_reason.clone(),
            recent_rejects: h.recent.iter().rev().cloned().collect(),
            series,
            actions: h.actions.iter().rev().cloned().collect(),
            open_cases: s
                .cases
                .iter()
                .filter(|c| c.host == h.name && matches!(c.status, CaseStatus::Open | CaseStatus::Acknowledged))
                .map(|c| c.id)
                .collect(),
        })
    }

    async fn host_action(&self, host: &str, action: HostAction, by: &str) -> AdminResult<HostRow> {
        let mut s = self.sim.lock();
        let i = s.host_idx(host)?;
        let now = s.now_ms;
        if let HostAction::SetTier { tier } = &action
            && !s.policy.policy.tiers.contains_key(tier)
        {
            return Err(AdminError::BadRequest(format!("no tier {tier:?}")));
        }
        if let HostAction::Throttle { events_per_sec: Some(x) } = &action
            && !(x.is_finite() && *x >= 0.0)
        {
            return Err(AdminError::BadRequest("throttle must be ≥ 0 events/s".into()));
        }
        if let HostAction::SetTier { tier } = &action
            && let Some(r) = s.rule_for(&s.hosts[i].name)
        {
            let wins = match &r.effect {
                RuleEffect::Ban => Some("banned"),
                RuleEffect::Tier { tier: t } if t != tier && tier != "throttled" => Some(t.as_str()),
                _ => None,
            };
            if let Some(w) = wins {
                return Err(AdminError::tier_set_by_rule(r.id, &r.pattern, w));
            }
        }
        let h = &mut s.hosts[i];
        match &action {
            HostAction::SetTier { tier } => h.tier = tier.clone(),
            HostAction::Throttle { events_per_sec } => h.throttle = *events_per_sec,
            HostAction::Suspend { .. } => {
                h.status = HostStatus::Suspended;
                h.connected_since = None;
            }
            HostAction::Ban { .. } => {
                h.status = HostStatus::Banned;
                h.connected_since = None;
            }
            HostAction::Unban => {
                if matches!(h.status, HostStatus::Banned | HostStatus::Suspended) {
                    h.status = HostStatus::Backoff;
                    h.redial_at = Some(now + 2_000);
                }
            }
            HostAction::SetAccountLimit { .. } => {}
            HostAction::Reconnect => {
                if matches!(h.status, HostStatus::Banned | HostStatus::Suspended) {
                    return Err(AdminError::BadRequest(
                        format!("{} is {:?}: unban it first", h.name, h.status).to_lowercase(),
                    ));
                }
                h.status = HostStatus::Backoff;
                h.connected_since = None;
                h.redial_at = Some(now + 1_500);
            }
        }
        h.actions.push(HostActionRecord { at_ms: now, by: by.into(), action, reason: None, case: None });
        self.host_changed(h, now, true);
        let h = &s.hosts[i];
        Ok(s.row(h))
    }

    async fn domain_rules(&self) -> AdminResult<Vec<DomainRule>> {
        let s = self.sim.lock();
        Ok(s.rules.iter().map(|r| s.rule_view(r)).collect())
    }

    async fn create_domain_rule(&self, rule: DomainRuleInput, by: &str) -> AdminResult<DomainRule> {
        let pattern = validate_pattern(&rule.pattern)?;
        let mut s = self.sim.lock();
        check_effect(&s, &rule.effect)?;
        if s.rules.iter().any(|r| r.pattern == pattern) {
            return Err(AdminError::Conflict(format!("a rule for {pattern} already exists")));
        }
        let id = s.next_rule;
        s.next_rule += 1;
        let r = DomainRule {
            id,
            pattern: pattern.clone(),
            effect: rule.effect.clone(),
            note: rule.note,
            created_at_ms: s.now_ms,
            created_by: by.into(),
            matches: 0,
            version: 0,
        };
        s.rules.push(r.clone());
        s.apply_rule_effect(&pattern, &rule.effect);
        self.rules_changed(&mut s);
        Ok(s.rule_view(&r))
    }

    async fn update_domain_rule(&self, id: u64, rule: DomainRuleInput, _by: &str) -> AdminResult<DomainRule> {
        let pattern = validate_pattern(&rule.pattern)?;
        let mut s = self.sim.lock();
        check_effect(&s, &rule.effect)?;
        if s.rules.iter().any(|r| r.pattern == pattern && r.id != id) {
            return Err(AdminError::Conflict(format!("a rule for {pattern} already exists")));
        }
        let i = s.rules.iter().position(|r| r.id == id).ok_or_else(|| AdminError::NotFound(format!("no rule {id}")))?;
        s.rules[i].pattern = pattern.clone();
        s.rules[i].effect = rule.effect.clone();
        s.rules[i].note = rule.note;
        s.apply_rule_effect(&pattern, &rule.effect);
        self.rules_changed(&mut s);
        let r = s.rules[i].clone();
        Ok(s.rule_view(&r))
    }

    async fn delete_domain_rule(&self, id: u64, _by: &str) -> AdminResult<()> {
        let mut s = self.sim.lock();
        let i = s.rules.iter().position(|r| r.id == id).ok_or_else(|| AdminError::NotFound(format!("no rule {id}")))?;
        s.rules.remove(i);
        self.rules_changed(&mut s);
        Ok(())
    }

    async fn policy(&self) -> AdminResult<PolicyDoc> {
        Ok(self.sim.lock().policy.clone())
    }

    async fn update_policy(&self, u: PolicyUpdate, by: &str) -> AdminResult<PolicyDoc> {
        let mut s = self.sim.lock();
        if u.base_version != s.policy.version {
            return Err(AdminError::Conflict(format!(
                "the policy is at version {} (you edited {}): reload and reapply your change",
                s.policy.version, u.base_version
            )));
        }
        let changes = diff_json(
            &serde_json::to_value(&s.policy.policy).map_err(anyhow::Error::from)?,
            &serde_json::to_value(&u.policy).map_err(anyhow::Error::from)?,
        );
        if changes.is_empty() {
            return Err(AdminError::BadRequest("nothing changed".into()));
        }
        let now = s.now_ms;
        s.policy =
            PolicyDoc { version: s.policy.version + 1, policy: u.policy, updated_at_ms: now, updated_by: by.into() };
        let version = s.policy.version;
        s.audit.push(PolicyAudit { version, at_ms: now, by: by.into(), note: u.note, changes });
        self.policy_changed(version);
        Ok(s.policy.clone())
    }

    async fn policy_audit(&self) -> AdminResult<Vec<PolicyAudit>> {
        Ok(self.sim.lock().audit.iter().rev().cloned().collect())
    }

    async fn consumers(&self) -> AdminResult<Vec<Consumer>> {
        let s = self.sim.lock();
        let mut out = s.consumers.clone();
        for c in &mut out {
            c.events_per_sec = round2(c.events_per_sec);
            c.bytes_per_sec = c.bytes_per_sec.round();
            c.lag_ms = round2(c.lag_ms);
        }
        Ok(out)
    }

    async fn kick_consumer(&self, id: u64, _by: &str) -> AdminResult<()> {
        self.kick_consumer_on(None, id, _by).await
    }

    async fn kick_consumer_on(&self, node: Option<&str>, id: u64, _by: &str) -> AdminResult<()> {
        let mut s = self.sim.lock();
        let i = s
            .consumers
            .iter()
            .position(|c| c.id == id && node.is_none_or(|n| c.node == n))
            .ok_or_else(|| AdminError::NotFound(format!("no consumer {id}")))?;
        let gone = s.consumers.remove(i);
        self.consumer_change(&gone, "disconnect");
        Ok(())
    }

    async fn settings_of(&self, node: Option<&str>) -> AdminResult<SettingsView> {
        let mut v = self.settings().await?;
        if let Some(n) = node {
            let (_, _, members, learners) = self.extra.lock().roles();
            if !members.iter().chain(&learners).any(|m| m == n) {
                return Err(AdminError::NotFound(format!("no node {n}")));
            }
            // the demo's nodes differ only in their names and addresses
            for e in &mut v.entries {
                match e.flag.as_str() {
                    "--node-id" => e.value = Some(n.to_string()),
                    "--qlog-listen" => e.value = Some(format!("0.0.0.0:{}", 2978)),
                    _ => {}
                }
            }
        }
        Ok(v)
    }

    async fn policy_usage(&self) -> AdminResult<PolicyUsage> {
        let (ev, new_per_min) = {
            let s = self.sim.lock();
            let ev = s.history.back().map_or(0.0, |x| x.ev_in);
            (ev, s.hosts.iter().map(|h| h.new_accounts_per_hour).sum::<f64>() / 60.0)
        };
        let f = self.full_policy().await?.policy;
        let c = &f["cluster"];
        let today = self.admissions().await?.new_hosts_today;
        Ok(PolicyUsage {
            node: "relay-a".into(),
            plc_lookups_per_sec: round2((ev / 400.0).min(480.0)),
            plc_lookups_budget: c["plcLookupsPerSec"].as_f64().unwrap_or(500.0),
            plc_lookups_share: c["plcLookupsPerSec"].as_f64().unwrap_or(500.0) / NODES.len() as f64,
            seeded_per_sec: round2(ev / 90.0),
            new_accounts_per_min: round2(new_per_min),
            new_accounts_budget: c["newAccountsPerMin"].as_f64().unwrap_or(600.0),
            new_hosts_today: today,
            new_hosts_per_day: c["newHostsPerDay"].as_u64().unwrap_or(50) as u32,
            window_secs: 10.0,
        })
    }

    async fn policy_signals(&self) -> AdminResult<SignalsView> {
        use crate::policy::signals::SpamRule;
        let spam = crate::policy::doc::PolicyBody::default().spam;
        let s = self.sim.lock();
        let mut busy: Vec<&SimHost> = s.hosts.iter().collect();
        busy.sort_by(|a, b| b.new_accounts_per_hour.total_cmp(&a.new_accounts_per_hour));
        let signals = SpamRule::ALL
            .iter()
            .enumerate()
            .map(|(k, &r)| {
                let t = r.threshold(&spam);
                let top = busy
                    .iter()
                    .skip(k)
                    .take(5)
                    .enumerate()
                    .map(|(j, h)| {
                        let est = round2(t.limit * (0.9 - 0.15 * j as f64).max(0.05));
                        let key = if r.per_account() { fake_did(&format!("{}/{j}", h.name)) } else { h.name.clone() };
                        SignalKey { key, host: h.name.clone(), estimate: est, lower: round2(est * 0.97) }
                    })
                    .collect();
                SignalTop {
                    rule: r.name().into(),
                    per: if r.per_account() { "account" } else { "host" }.into(),
                    limit: t.limit,
                    window_secs: t.window_secs,
                    enabled: t.enabled(),
                    top,
                }
            })
            .collect();
        Ok(SignalsView { node: "relay-a".into(), signals })
    }

    async fn takedowns(&self) -> AdminResult<Vec<crate::policy::takedowns::TakedownEntry>> {
        let s = self.sim.lock();
        let mut by: BTreeMap<String, crate::policy::takedowns::TakedownEntry> = s
            .accounts
            .iter()
            .filter_map(|a| {
                let t = a.takedown.as_ref()?;
                Some((a.did.clone(), (t, a.did.clone())))
            })
            .chain(s.takedowns.iter().map(|(d, t)| (d.clone(), (t, d.clone()))))
            .map(|(d, (t, did))| {
                let e = crate::policy::takedowns::TakedownEntry {
                    did,
                    takedown: true,
                    at_ms: t.at_ms,
                    by: t.by.clone(),
                    reason: t.reason.clone(),
                };
                (d, e)
            })
            .collect();
        // an operator's untakedown in the demo removes it from `takedowns`
        // but not from a seeded account's own field
        by.retain(|d, _| s.takedowns.contains_key(d) || s.accounts.iter().any(|a| a.did == *d && a.takedown.is_some()));
        let mut v: Vec<_> = by.into_values().collect();
        v.sort_by_key(|e| std::cmp::Reverse(e.at_ms));
        Ok(v)
    }

    async fn quorum_history(&self) -> AdminResult<QuorumHistory> {
        // sim before extra, as everywhere else
        let now = self.sim.lock().now_ms;
        self.extra.lock().history(now)
    }

    async fn flush_now(&self, _by: &str) -> AdminResult<serde_json::Value> {
        let q = self.quorum().await?;
        Ok(q.nodes.into_iter().filter_map(|n| n.status).find(|s| s["role"] == "leader").unwrap_or_default())
    }

    async fn quorum(&self) -> AdminResult<QuorumView> {
        let s = self.sim.lock();
        let ev = s.history.back().map_or(0.0, |x| x.ev_in);
        Ok(self.extra.lock().view(s.last_seq, s.now_ms, ev))
    }

    async fn change_quorum_members(&self, req: QuorumMembersChange, _by: &str) -> AdminResult<serde_json::Value> {
        let mut s = self.sim.lock();
        let out = self.extra.lock().change(req, s.last_seq, s.now_ms)?;
        let (leader, epoch, _, _) = self.extra.lock().roles();
        let hint = serde_json::json!({ "epoch": epoch, "leader": leader });
        self.logged(&mut s, changes::ChangeKind::Cluster, "quorum", hint);
        Ok(out)
    }

    async fn settings(&self) -> AdminResult<SettingsView> {
        Ok(self.extra.lock().settings())
    }

    async fn full_policy(&self) -> AdminResult<FullPolicyDoc> {
        let s = self.sim.lock();
        let x = self.extra.lock();
        Ok(FullPolicyDoc {
            version: s.policy.version,
            updated_at_ms: x.full_at_ms.max(s.policy.updated_at_ms),
            updated_by: x.full_by.clone(),
            note: x.full_note.clone(),
            policy: x.full.clone(),
        })
    }

    async fn update_full_policy(&self, u: FullPolicyUpdate, by: &str) -> AdminResult<FullPolicyDoc> {
        let body: crate::policy::doc::PolicyBody =
            serde_json::from_value(u.policy).map_err(|e| AdminError::BadRequest(format!("policy: {e}")))?;
        crate::policy::doc::validate(&body).map_err(|e| AdminError::BadRequest(e.join("; ")))?;
        let new = serde_json::to_value(&body).map_err(anyhow::Error::from)?;
        let mut s = self.sim.lock();
        let mut x = self.extra.lock();
        if u.base_version != s.policy.version {
            return Err(AdminError::Conflict(format!(
                "the policy is at version {} (you edited {}): reload and reapply your change",
                s.policy.version, u.base_version
            )));
        }
        let changes = diff_json(&x.full, &new);
        if changes.is_empty() {
            return Err(AdminError::BadRequest("nothing changed".into()));
        }
        let now = s.now_ms;
        s.policy.version += 1;
        s.policy.updated_at_ms = now;
        s.policy.updated_by = by.into();
        let version = s.policy.version;
        s.audit.push(PolicyAudit { version, at_ms: now, by: by.into(), note: u.note.clone(), changes });
        x.full = new;
        x.full_at_ms = now;
        x.full_by = by.into();
        x.full_note = u.note;
        self.policy_changed(version);
        Ok(FullPolicyDoc {
            version,
            updated_at_ms: now,
            updated_by: by.into(),
            note: x.full_note.clone(),
            policy: x.full.clone(),
        })
    }

    async fn pipeline_view(&self) -> AdminResult<PipelineView> {
        let s = self.sim.lock();
        let owner = |h: &SimHost| s.host_shards[h.shard].clone().unwrap_or_else(|| NODES[0].to_string());
        // ~40 ms of events in flight per host, so the busy hosts lead
        let mut hosts: Vec<PipelineHost> = s
            .hosts
            .iter()
            .filter(|h| h.connected_since.is_some() && h.rate > 0.0)
            .map(|h| PipelineHost {
                host: h.name.clone(),
                node: owner(h),
                inflight: (h.rate * 0.04).ceil() as u64,
                inflight_cap: Some(512),
                paused: h.throttle.is_some_and(|t| h.rate >= t),
                status: Some(h.status),
                events_per_sec: round2(h.rate),
            })
            .collect();
        hosts.sort_by(|a, b| b.inflight.cmp(&a.inflight).then_with(|| a.host.cmp(&b.host)));
        hosts.truncate(50);
        let nodes = NODES
            .iter()
            .map(|id| {
                let mine: Vec<&PipelineHost> = hosts.iter().filter(|h| h.node == *id).collect();
                let pending: u64 = mine.iter().map(|h| h.inflight).sum();
                let mut gauges = BTreeMap::new();
                gauges.insert("relay_ack_pending".into(), pending as f64);
                gauges.insert("relay_lane_queued".into(), (pending / 4) as f64);
                PipelineNode {
                    node: id.to_string(),
                    stale: false,
                    ack_pending: pending,
                    oldest_pending_ms: if pending > 0 { 38.0 } else { 0.0 },
                    lane_queued: pending / 4,
                    dedupe_entries: 0,
                    paused_hosts: mine.iter().filter(|h| h.paused).count() as u32,
                    gauges,
                }
            })
            .collect();
        Ok(PipelineView { nodes, hosts })
    }

    async fn admissions(&self) -> AdminResult<AdmissionLog> {
        let per_day = self.full_policy().await?.policy["cluster"]["newHostsPerDay"].as_u64().unwrap_or(50) as u32;
        let s = self.sim.lock();
        let now = s.now_ms;
        let mut entries = Vec::new();
        for (i, h) in s.hosts.iter().rev().filter(|h| h.tier != "trusted").take(14).enumerate() {
            entries.push(crate::upstream::crawl::CrawlAdmission {
                at_ms: now - 1_000 * (90 + 1_400 * i as i64 + (hash(&h.name) % 600) as i64),
                host: h.name.clone(),
                outcome: "admitted".into(),
                tier: Some(h.tier.clone()),
                reason: "new host".into(),
                source: if i % 3 == 0 { "requestCrawl".into() } else { "bootstrap:relay1.us-east.bsky.network".into() },
            });
        }
        let refused = [
            ("pds.spam-farm.example", "banned", "host is banned"),
            ("10.0.0.7", "refused", "host check failed: not a public hostname"),
            ("pds-31.fastvps.example", "rate-limited", "new-host budget is spent"),
            ("bsky.unreachable.example", "refused", "host check failed: describeServer timed out"),
            ("pds-32.fastvps.example", "rate-limited", "new-host budget is spent"),
        ];
        for (i, (h, o, why)) in refused.iter().enumerate() {
            entries.push(crate::upstream::crawl::CrawlAdmission {
                at_ms: now - 1_000 * (400 + 2_300 * i as i64),
                host: h.to_string(),
                outcome: o.to_string(),
                tier: None,
                reason: why.to_string(),
                source: "requestCrawl".into(),
            });
        }
        entries.sort_by_key(|a| std::cmp::Reverse(a.at_ms));
        let today = entries.iter().filter(|e| e.outcome == "admitted").count() as u32 + 9;
        Ok(AdmissionLog { new_hosts_today: today.min(per_day), new_hosts_per_day: per_day, entries })
    }

    async fn tail(&self, q: TailQuery) -> AdminResult<Vec<TailFrame>> {
        let s = self.sim.lock();
        let host = q.host.as_deref().filter(|h| !h.is_empty());
        let since = q.since_ms.unwrap_or(i64::MIN);
        let limit = q.limit.unwrap_or(200).clamp(1, 2000);
        let mut out = Vec::new();
        for h in s.hosts.iter().filter(|h| host.is_none_or(|w| w == h.name)) {
            if q.rejects.unwrap_or(0) != 0 {
                for r in h.recent.iter().filter(|r| r.at_ms > since) {
                    let held = r.reason == RejectReason::Inactive && h.tier == "throttled";
                    out.push(TailFrame {
                        at_ms: r.at_ms,
                        host: h.name.clone(),
                        did: r.did.clone(),
                        kind: if held { "held" } else { "reject" }.into(),
                        reason: serde_json::to_value(r.reason).ok().and_then(|v| v.as_str().map(str::to_string)),
                        detail: Some(r.detail.clone()),
                        upstream_seq: Some(r.upstream_seq),
                        seq: None,
                        event: None,
                    });
                }
            }
            if host.is_some() && h.rate > 0.0 {
                // the last two seconds at the host's rate, spread evenly
                let n = ((h.rate * 2.0).round() as usize).min(limit);
                for k in 0..n {
                    let at = s.now_ms - (k as f64 * 2_000.0 / n as f64) as i64;
                    if at <= since {
                        break;
                    }
                    out.push(TailFrame {
                        at_ms: at,
                        host: h.name.clone(),
                        did: fake_did(&format!("{}/{}", h.name, (h.seq as usize + k) % 40)),
                        kind: "passed".into(),
                        reason: None,
                        detail: None,
                        upstream_seq: Some(h.seq - k as i64),
                        seq: Some(s.last_seq - (k * 7) as i64),
                        event: Some(if k % 9 == 0 { "identity" } else { "commit" }.into()),
                    });
                }
            }
        }
        out.sort_by_key(|a| std::cmp::Reverse(a.at_ms));
        out.truncate(limit);
        Ok(out)
    }

    async fn release_throttled(&self, host: &str, _by: &str) -> AdminResult<Released> {
        let mut s = self.sim.lock();
        let i = s
            .hosts
            .iter()
            .position(|h| h.name == host)
            .ok_or_else(|| AdminError::NotFound(format!("no host {host}")))?;
        let h = &s.hosts[i];
        let released = if h.accounts > 100 { (h.accounts - 100).min(400) } else { 0 };
        let (name, now) = (h.name.clone(), s.now_ms);
        for k in 0..released.min(3) {
            let did = fake_did(&format!("{name}/{k}"));
            self.logged(&mut s, changes::ChangeKind::Account, &did, serde_json::json!({ "host": name }));
        }
        self.host_changed(&mut s.hosts[i], now, true);
        Ok(Released { released })
    }

    async fn store(&self) -> AdminResult<StoreView> {
        let s = self.sim.lock();
        let now = s.now_ms;
        let up = (s.history.len().max(1) as f64) * 40.0;
        let ev = s.history.back().map_or(0.0, |x| x.ev_in);
        let p = |name: &str, a: f64, b: f64, free: f64, up_b: f64, down_b: f64| StorePurpose {
            purpose: name.into(),
            requests: ClassCounts { a: (a * up).round(), b: (b * up).round(), free: (free * up).round() },
            per_sec: ClassCounts { a: round2(a), b: round2(b), free: round2(free) },
            bytes_up: (up_b * up) as u64,
            bytes_down: (down_b * up) as u64,
        };
        let purposes = vec![
            p("flush", 0.07, 0.0, 0.0, ev * 4_600.0 / 3.0, 0.0),
            p("state", 0.31, 1.2, 0.04, 3_800.0, 22_000.0),
            p("leader", 0.0, 0.0, 0.0, 0.0, 0.0),
            p("backfill", 0.0, 0.42, 0.0, 0.0, 1_900_000.0),
            p("retain", 0.003, 0.002, 0.012, 0.0, 120.0),
        ];
        let sum = |f: &dyn Fn(&StorePurpose) -> f64| purposes.iter().map(f).sum::<f64>();
        let total = StorePurpose {
            purpose: "total".into(),
            requests: ClassCounts {
                a: sum(&|x| x.requests.a),
                b: sum(&|x| x.requests.b),
                free: sum(&|x| x.requests.free),
            },
            per_sec: ClassCounts {
                a: round2(sum(&|x| x.per_sec.a)),
                b: round2(sum(&|x| x.per_sec.b)),
                free: round2(sum(&|x| x.per_sec.free)),
            },
            bytes_up: purposes.iter().map(|x| x.bytes_up).sum(),
            bytes_down: purposes.iter().map(|x| x.bytes_down).sum(),
        };
        let lat = |op: &str, n: f64, mean: f64, p50: f64, p99: f64| StoreLatency {
            op: op.into(),
            count: (n * up) as u64,
            mean_ms: mean,
            p50_ms: p50,
            p99_ms: p99,
        };
        let seg = 64u64 << 20;
        let segments = 3 * 24 * 40;
        let deletable: Vec<serde_json::Value> = (0..3u64)
            .map(|k| {
                let first = s.last_seq.saturating_sub(90_000_000) as u64 + k * 160_000;
                serde_json::json!({"ordinal": 4_100 + k, "first": first, "last": first + 159_999,
                                   "bytes": seg, "age_secs": 72 * 3600 + 600 - k * 200})
            })
            .collect();
        let retention = serde_json::json!({
            "opened": {},
            "pruned_seq": s.last_seq.saturating_sub(90_000_000),
            "plan": {
                "at_ms": now - 212_000,
                "horizon_secs": 72 * 3600,
                "flushed": s.last_seq - 8_000,
                "reserve": s.last_seq - 8_000 + 8_640_000,
                "next_ordinal": 4_100 + segments,
                "gaps": [[s.last_seq - 55_120_400, s.last_seq - 46_480_400]],
                "segments": segments,
                "segment_bytes": segments * seg,
                "deletable": deletable,
                "deletable_bytes": 3 * seg,
                "pruned_seq_after": s.last_seq.saturating_sub(89_520_000),
                "stale_segments": [],
                "states": [
                    {"path": "qlog/state-e12", "current": true, "referenced": false, "objects": 412,
                     "bytes": 2_900_000_000u64, "stale_checkpoints": [], "clone_checkpoints": 0, "deletable": false},
                    {"path": "qlog/state", "current": false, "referenced": true, "objects": 96,
                     "bytes": 610_000_000u64, "stale_checkpoints": [], "clone_checkpoints": 1, "deletable": false},
                ],
            },
            "applied": {"segments": 3, "segment_bytes": 3 * seg, "pruned_seq": s.last_seq.saturating_sub(90_480_000),
                        "state_paths": [], "state_objects": 0, "state_bytes": 0, "kept": []},
        });
        Ok(StoreView {
            node: "relay-a".into(),
            at_ms: now,
            window_secs: 10.0,
            total,
            purposes,
            latency: vec![
                lat("get", 1.6, 21.0, 25.0, 100.0),
                lat("get_range", 0.4, 34.0, 50.0, 250.0),
                lat("put", 0.3, 88.0, 100.0, 500.0),
                lat("put_cas", 0.03, 61.0, 50.0, 250.0),
                lat("list", 0.05, 40.0, 50.0, 100.0),
            ],
            retention: Some(retention),
        })
    }

    async fn plc_view(&self) -> AdminResult<PlcView> {
        let (leader, ..) = self.extra.lock().roles();
        let now = self.sim.lock().now_ms;
        let start = 1_668_643_200_000i64;
        let windows: Vec<PlcWindow> = (0..4)
            .map(|k| {
                let from = start + (now - start) * k / 4;
                let until = (k < 3).then(|| start + (now - start) * (k + 1) / 4 - 1);
                PlcWindow {
                    from_ms: from,
                    after_ms: until.unwrap_or(now - 1_200),
                    until_ms: until,
                    ops: 19_000_000 + 1_300_000 * k as u64,
                    done: k < 3,
                    progress: 1.0,
                }
            })
            .collect();
        let ops = windows.iter().map(|w| w.ops).sum();
        Ok(PlcView {
            enabled: true,
            leader: Some(leader.clone()),
            caught_up: true,
            ops,
            ops_per_sec: round2(9.0 + (now / 1000 % 7) as f64),
            written: ops - ops / 9,
            requests: ops / 1000 + 4_200,
            throttled: 37,
            errors: 2,
            restarts: 1,
            newest_ms: now - 1_200,
            windows,
            checkpoint_ms: now - (now % 10_000),
            learned: 48_000 + (now / 1000 % 600) as u64,
            learned_dropped: 0,
            nodes: vec![PlcNode {
                node: leader,
                stale: false,
                leader: true,
                ops,
                ops_per_sec: 11.0,
                throttled: 37,
                errors: 2,
            }],
        })
    }

    async fn discovery(&self) -> AdminResult<DiscoveryView> {
        let d: crate::policy::doc::Discovery =
            serde_json::from_value(self.full_policy().await?.policy["discovery"].clone()).unwrap_or_default();
        let now = self.sim.lock().now_ms;
        let (leader, ..) = self.extra.lock().roles();
        let hour = 3_600_000i64;
        let mut sources: Vec<DiscoverySource> = d
            .seed_relays
            .iter()
            .enumerate()
            .map(|(i, r)| {
                let k = 1 + (hash(&r.url) % 40);
                let state = crate::discovery::SourceState {
                    url: Some(r.url.clone()),
                    runs: 5,
                    last_started_ms: Some(now - 2 * hour - 340_000 - i as i64 * 60_000),
                    last_finished_ms: Some(now - 2 * hour - i as i64 * 60_000),
                    hosts_seen: 2_400 + k,
                    known: 2_360,
                    new: 40 + k,
                    admitted: 32 + k,
                    refused: 8,
                    throttled: 2,
                    pages: 3,
                    ..Default::default()
                };
                DiscoverySource {
                    key: crate::discovery::source_key(&r.url),
                    enabled: r.enabled,
                    refresh_interval_secs: Some(r.refresh_interval_secs),
                    next_run_ms: r
                        .enabled
                        .then(|| state.last_finished_ms.unwrap_or(now) + r.refresh_interval_secs as i64 * 1000),
                    pending: 0,
                    state,
                }
            })
            .collect();
        sources.push(DiscoverySource {
            key: crate::discovery::PLC_SOURCE.into(),
            enabled: d.plc,
            refresh_interval_secs: None,
            next_run_ms: None,
            pending: if d.plc { 3 } else { 0 },
            state: crate::discovery::SourceState {
                runs: 1,
                last_started_ms: d.plc.then_some(now - 26 * hour),
                last_finished_ms: d.plc.then_some(now - 40_000),
                hosts_seen: if d.plc { 1_960 } else { 0 },
                known: if d.plc { 1_902 } else { 0 },
                new: if d.plc { 58 } else { 0 },
                admitted: if d.plc { 44 } else { 0 },
                refused: if d.plc { 14 } else { 0 },
                ..Default::default()
            },
        });
        Ok(DiscoveryView {
            leader: Some(leader),
            leading: true,
            connects_per_min: d.connects_per_min,
            requests_per_sec: d.requests_per_sec,
            sources,
        })
    }

    async fn discovery_run(&self, req: DiscoveryRun, _by: &str) -> AdminResult<DiscoveryView> {
        let mut v = self.discovery().await?;
        let now = self.sim.lock().now_ms;
        let mut hit = false;
        for s in &mut v.sources {
            if req.source.as_deref().is_none_or(|k| k == s.key) && s.enabled {
                s.state.in_progress = true;
                s.state.run_requested = true;
                s.next_run_ms = Some(now);
                hit = true;
            }
        }
        if !hit {
            return Err(AdminError::NotFound(format!("no enabled source {:?}", req.source)));
        }
        Ok(v)
    }

    async fn rejects_top(&self, q: RejectTopQuery) -> AdminResult<Vec<RejectTop>> {
        let s = self.sim.lock();
        let limit = q.limit.unwrap_or(10).clamp(1, 500);
        let mut v: Vec<RejectTop> = s
            .hosts
            .iter()
            .filter_map(|h| {
                let all: u64 = h.by_reason.values().sum();
                let total = match q.reason {
                    Some(r) => h.by_reason.get(&r).copied().unwrap_or(0),
                    None => all,
                };
                if total == 0 {
                    return None;
                }
                let share = total as f64 / all.max(1) as f64;
                let sample = h.recent.iter().rev().find(|x| q.reason.is_none_or(|r| r == x.reason)).cloned();
                Some(RejectTop {
                    host: h.name.clone(),
                    rejects_per_sec: round2(h.rate * h.err * share),
                    total,
                    last_at_ms: sample.as_ref().map(|x| x.at_ms),
                    sample,
                })
            })
            .collect();
        v.sort_by(|a, b| b.rejects_per_sec.total_cmp(&a.rejects_per_sec).then(b.total.cmp(&a.total)));
        v.truncate(limit);
        Ok(v)
    }

    async fn cluster(&self) -> AdminResult<ClusterView> {
        let s = self.sim.lock();
        let (leader, epoch, members, learners) = self.extra.lock().roles();
        let now = s.now_ms;
        let ids: Vec<String> = members.iter().chain(&learners).cloned().collect();
        let nodes = ids
            .iter()
            .enumerate()
            .map(|(i, id)| {
                let hosts: Vec<&SimHost> =
                    s.hosts.iter().filter(|h| s.host_shards[h.shard].as_deref() == Some(id.as_str())).collect();
                let ev_in: f64 = hosts.iter().map(|h| h.rate).sum();
                let consumers: Vec<&Consumer> = s.consumers.iter().filter(|c| c.node == *id).collect();
                let ev_out: f64 = consumers.iter().map(|c| c.events_per_sec).sum();
                let learner = learners.contains(id);
                NodeView {
                    id: id.clone(),
                    addr: format!("10.0.7.{}:2978", 11 + i),
                    version: env!("CARGO_PKG_VERSION").into(),
                    rev: "530a3e45".into(),
                    reachable: true,
                    healthy: !learner,
                    role: if *id == leader { "leader" } else { "follower" }.into(),
                    learner,
                    owned_hosts: hosts.len() as u32,
                    hosts: hosts.iter().filter(|h| h.connected_since.is_some()).count() as u32,
                    consumers: consumers.len() as u32,
                    events_in_per_sec: round2(ev_in),
                    events_out_per_sec: round2(ev_out),
                    commit_lag_ms: round2(s.history.back().map(|x| x.dur).unwrap_or(0.0) * (0.85 + 0.1 * i as f64)),
                    cpu: round2((ev_in / 6_000.0 + ev_out / 400_000.0).min(7.6)),
                    mem_bytes: Some((9.5e9 + ev_in * 6.0e4) as u64),
                    stale: false,
                    error: None,
                    reported_ms: now,
                    bytes_out_per_sec: consumers.iter().map(|c| c.bytes_per_sec).sum(),
                    stream_seq: s.last_seq,
                }
            })
            .collect();
        let unowned =
            s.hosts.iter().filter(|h| s.host_shards[h.shard].as_ref().is_none_or(|o| !members.contains(o))).count()
                as u32;
        Ok(ClusterView {
            nodes,
            leader: Some(leader),
            epoch,
            hosts: s.hosts.len() as u32,
            unowned_hosts: unowned,
            last_seq: s.last_seq,
        })
    }

    async fn accounts(&self, q: AccountQuery) -> AdminResult<Vec<Account>> {
        let mut s = self.sim.lock();
        let q = q.q.unwrap_or_default().trim().to_ascii_lowercase();
        let q = q.trim_start_matches('@').to_string();
        if q.starts_with("did:") && !s.accounts.iter().any(|a| a.did.starts_with(&q)) {
            let i = s.find_account(&q)?;
            return Ok(vec![s.account_view(&s.accounts[i])]);
        }
        let mut out: Vec<Account> = s
            .accounts
            .iter()
            .filter(|a| {
                q.is_empty()
                    || a.did.starts_with(&q)
                    || a.handle.as_deref().is_some_and(|h| h.starts_with(&q) || h.contains(&q))
            })
            .take(100)
            .map(|a| s.account_view(a))
            .collect();
        if q.is_empty() {
            out.sort_by_key(|a| std::cmp::Reverse(a.last_event_ms));
        }
        Ok(out)
    }

    async fn account(&self, did: &str) -> AdminResult<Account> {
        let mut s = self.sim.lock();
        let i = s.find_account(did)?;
        Ok(s.account_view(&s.accounts[i]))
    }

    async fn takedown(&self, did: &str, reason: String, by: &str) -> AdminResult<Account> {
        let mut s = self.sim.lock();
        let i = s.find_account(did)?;
        let now = s.now_ms;
        s.takedowns.insert(did.to_string(), Takedown { at_ms: now, by: by.into(), reason });
        self.logged(&mut s, changes::ChangeKind::Takedown, did, serde_json::json!({ "takedown": true }));
        Ok(s.account_view(&s.accounts[i]))
    }

    async fn untakedown(&self, did: &str, _by: &str) -> AdminResult<Account> {
        let mut s = self.sim.lock();
        let i = s.find_account(did)?;
        s.takedowns.remove(did);
        self.logged(&mut s, changes::ChangeKind::Takedown, did, serde_json::json!({ "takedown": false }));
        Ok(s.account_view(&s.accounts[i]))
    }

    async fn cases(&self, q: CaseQuery) -> AdminResult<Vec<Case>> {
        let s = self.sim.lock();
        let mut out: Vec<Case> = s.cases.iter().filter(|c| q.status.is_none_or(|st| c.status == st)).cloned().collect();
        out.sort_by(|a, b| b.severity.cmp(&a.severity).then(b.opened_at_ms.cmp(&a.opened_at_ms)));
        Ok(out)
    }

    async fn case(&self, id: u64) -> AdminResult<Case> {
        let s = self.sim.lock();
        s.cases.iter().find(|c| c.id == id).cloned().ok_or_else(|| AdminError::NotFound(format!("no case {id}")))
    }

    async fn update_case(&self, id: u64, u: CaseUpdate, by: &str) -> AdminResult<Case> {
        let mut s = self.sim.lock();
        let now = s.now_ms;
        let c = s.cases.iter_mut().find(|c| c.id == id).ok_or_else(|| AdminError::NotFound(format!("no case {id}")))?;
        if let Some(st) = u.status {
            c.status = st;
        }
        if !u.note.trim().is_empty() {
            c.notes.push(CaseNote { at_ms: now, by: by.into(), text: u.note });
        }
        c.updated_at_ms = now;
        let hint = serde_json::json!({ "status": c.status });
        self.feed.emit(changes::ChangeKind::Case, id.to_string(), Some(hint), true);
        Ok(c.clone())
    }

    async fn bulk_update_cases(&self, b: CaseBulkUpdate, by: &str) -> AdminResult<CaseBulkResult> {
        let mut s = self.sim.lock();
        let now = s.now_ms;
        let (targets, u) = bulk_targets(&s.cases, &b)?;
        let targets: std::collections::HashSet<u64> = targets.into_iter().collect();
        let mut ids = Vec::new();
        for c in s.cases.iter_mut().filter(|c| targets.contains(&c.id)) {
            if let Some(st) = u.status {
                c.status = st;
            }
            c.notes.push(CaseNote { at_ms: now, by: by.into(), text: u.note.clone() });
            c.updated_at_ms = now;
            let hint = serde_json::json!({ "status": c.status });
            self.feed.touch(changes::ChangeKind::Case, c.id.to_string(), Some(hint), true);
            ids.push(c.id);
        }
        Ok(CaseBulkResult { updated: ids.len(), ids })
    }
}

fn check_effect(s: &Sim, e: &RuleEffect) -> AdminResult<()> {
    match e {
        RuleEffect::Tier { tier } if !s.policy.policy.tiers.contains_key(tier) => {
            Err(AdminError::BadRequest(format!("no tier {tier:?}")))
        }
        RuleEffect::Throttle { events_per_sec } if !(events_per_sec.is_finite() && *events_per_sec >= 0.0) => {
            Err(AdminError::BadRequest("throttle must be ≥ 0 events/s".into()))
        }
        _ => Ok(()),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn patterns() {
        assert!(rule_matches("*.fastvps.cloud", "pds-1.fastvps.cloud"));
        assert!(rule_matches("*.fastvps.cloud", "fastvps.cloud"));
        assert!(!rule_matches("*.fastvps.cloud", "notfastvps.cloud"));
        assert!(rule_matches("a.b", "a.b"));
        assert!(!rule_matches("a.b", "x.a.b"));
    }

    #[tokio::test]
    async fn simulates_a_busy_relay() {
        let d = Demo::start(7);
        let o = d.overview().await.unwrap();
        assert_eq!(o.hosts_total, 5_000);
        assert!(o.events_in_per_sec > 20_000.0, "{}", o.events_in_per_sec);
        assert!(o.history.t.len() >= HISTORY - 1);
        assert!(o.open_cases > 0);
        let l = d.hosts(HostQuery { q: Some("fastvps".into()), ..Default::default() }).await.unwrap();
        assert_eq!(l.total, 2);
        let det = d.host("pds-7f3a.fastvps.cloud").await.unwrap();
        assert_eq!(det.row.tier, "new");

        let p = d.policy().await.unwrap();
        let mut np = p.policy.clone();
        np.spam.reject_ratio = 0.3;
        let u = d
            .update_policy(PolicyUpdate { base_version: p.version, policy: np.clone(), note: "t".into() }, "admin")
            .await
            .unwrap();
        assert_eq!(u.version, p.version + 1);
        let stale =
            d.update_policy(PolicyUpdate { base_version: p.version, policy: np, note: String::new() }, "admin").await;
        assert!(matches!(stale, Err(AdminError::Conflict(_))));
        assert_eq!(d.policy_audit().await.unwrap()[0].changes, vec!["spam.rejectRatio: 0.5 → 0.3".to_string()]);
    }

    /// `POST cases/bulk` through the router: scoped by a filter, every matched case gets the
    /// status and a note naming the caller, and an unscoped one is refused.
    #[tokio::test]
    async fn bulk_case_updates() {
        use axum::body::Body;
        use axum::http::{Request, StatusCode};
        use tower::ServiceExt;
        let d = Demo::start(7);
        let app = crate::admin::api_routes(d.clone(), "t".into());
        let post = |body: serde_json::Value| {
            let req = Request::post("/admin/api/cases/bulk")
                .header("authorization", "Basic YWRtaW46dA==")
                .header("content-type", "application/json")
                .body(Body::from(body.to_string()))
                .unwrap();
            app.clone().oneshot(req)
        };
        let open = d.cases(CaseQuery { status: Some(CaseStatus::Open), ..Default::default() }).await.unwrap();
        let kind = open[0].kind.clone();
        let want: Vec<u64> = open.iter().filter(|c| c.kind == kind).map(|c| c.id).collect();
        let r =
            post(serde_json::json!({"filter": {"kind": kind, "status": "open"}, "status": "resolved"})).await.unwrap();
        assert_eq!(r.status(), StatusCode::OK);
        let b = axum::body::to_bytes(r.into_body(), 1 << 20).await.unwrap();
        let res: CaseBulkResult = serde_json::from_slice(&b).unwrap();
        let mut got = res.ids.clone();
        got.sort();
        let mut want = want;
        want.sort();
        assert_eq!((res.updated, got), (want.len(), want.clone()));
        for id in &want {
            let c = d.case(*id).await.unwrap();
            assert_eq!(c.status, CaseStatus::Resolved);
            let n = c.notes.last().unwrap();
            assert_eq!((n.by.as_str(), n.text.as_str()), ("admin (token)", "bulk: resolved"));
        }
        // ids narrow the filter, and nothing left open of that kind matches
        let r =
            post(serde_json::json!({"ids": want, "filter": {"status": "open"}, "status": "dismissed"})).await.unwrap();
        let b = axum::body::to_bytes(r.into_body(), 1 << 20).await.unwrap();
        assert_eq!(serde_json::from_slice::<CaseBulkResult>(&b).unwrap().updated, 0);
        assert_eq!(post(serde_json::json!({"status": "resolved"})).await.unwrap().status(), StatusCode::BAD_REQUEST);
        assert_eq!(post(serde_json::json!({"ids": [1]})).await.unwrap().status(), StatusCode::BAD_REQUEST);

        // GET cases: a page, the total, and facet counts that leave out the filter they count
        let get = |q: String| {
            let req = Request::get(format!("/admin/api/cases?{q}"))
                .header("authorization", "Basic YWRtaW46dA==")
                .body(Body::empty())
                .unwrap();
            let app = app.clone();
            async move {
                let r = app.oneshot(req).await.unwrap();
                assert_eq!(r.status(), StatusCode::OK);
                let b = axum::body::to_bytes(r.into_body(), 1 << 24).await.unwrap();
                serde_json::from_slice::<CaseList>(&b).unwrap()
            }
        };
        let all = d.cases(CaseQuery::default()).await.unwrap();
        let l = get(String::new()).await;
        assert_eq!((l.total, l.cases.len()), (all.len(), all.len()));
        assert_eq!(l.counts.by_status.values().sum::<usize>(), all.len());
        let l = get(format!("kind={kind}&status=resolved&limit=2&offset=1")).await;
        let resolved = all.iter().filter(|c| c.kind == kind && c.status == CaseStatus::Resolved).count();
        assert_eq!((l.total, l.cases.len()), (resolved, resolved.saturating_sub(1).min(2)));
        assert!(l.cases.iter().all(|c| c.kind == kind && c.status == CaseStatus::Resolved));
        assert_eq!(l.counts.by_status["resolved"], resolved);
        let resolved_any = all.iter().filter(|c| c.status == CaseStatus::Resolved).count();
        assert_eq!(l.counts.by_kind.values().sum::<usize>(), resolved_any);
    }

    /// A rule's `matches` and `hosts?rule=` count the hosts it decides: an exact rule takes its
    /// host from the wildcard above it, as the relay's lookup does.
    #[tokio::test]
    async fn an_exact_rule_overrides_its_wildcard() {
        let d = Demo::start(7);
        let rules = d.domain_rules().await.unwrap();
        let rule = |p: &str| rules.iter().find(|r| r.pattern == p).unwrap().clone();
        let (wild, exact) = (rule("*.example.social"), rule("demo.example.social"));
        assert_eq!((wild.matches, exact.matches), (4, 1));
        let by_rule = |id| d.hosts(HostQuery { rule: Some(id), sort: Some("host".into()), ..Default::default() });
        let l = by_rule(wild.id).await.unwrap();
        assert_eq!(l.total, 4);
        assert!(l.hosts.iter().all(|h| h.rule == Some(wild.id) && h.tier == "new" && h.host != "demo.example.social"));
        let l = by_rule(exact.id).await.unwrap();
        assert_eq!(
            (l.total, l.hosts[0].host.as_str(), l.hosts[0].tier.as_str()),
            (1, "demo.example.social", "trusted")
        );
        assert_eq!(d.hosts(HostQuery { rule: Some(999), ..Default::default() }).await.unwrap().total, 0);
    }

    /// A tier a domain rule decides can't be set on the host: a 409 naming the rule, and no
    /// operator action recorded.
    #[tokio::test]
    async fn set_tier_under_a_tier_rule_is_refused() {
        use axum::body::Body;
        use axum::http::{Request, StatusCode};
        use tower::ServiceExt;
        let d = Demo::start(7);
        let app = crate::admin::api_routes(d.clone(), "t".into());
        let set = |host: &str, tier: &str| {
            let req = Request::post(format!("/admin/api/hosts/{host}/action"))
                .header("authorization", "Basic YWRtaW46dA==")
                .header("content-type", "application/json")
                .body(Body::from(format!(r#"{{"action":"set-tier","tier":"{tier}"}}"#)))
                .unwrap();
            app.clone().oneshot(req)
        };
        let host = "pds-7f3a.fastvps.cloud";
        let before = d.host(host).await.unwrap().actions.len();
        let r = set(host, "trusted").await.unwrap();
        assert_eq!(r.status(), StatusCode::CONFLICT);
        let b = axum::body::to_bytes(r.into_body(), 1 << 20).await.unwrap();
        let v: serde_json::Value = serde_json::from_slice(&b).unwrap();
        assert_eq!(v["error"], "TierSetByRule");
        assert!(v["message"].as_str().unwrap().contains("rule 2 (*.fastvps.cloud)"), "{v}");
        let det = d.host(host).await.unwrap();
        assert_eq!((det.row.tier.as_str(), det.actions.len()), ("new", before));
        // the rule's own tier and throttled still land; a ban rule refuses every tier
        assert_eq!(set(host, "new").await.unwrap().status(), StatusCode::OK);
        assert_eq!(set(host, "throttled").await.unwrap().status(), StatusCode::OK);
        assert_eq!(set("pds.cryptoairdrop.live", "trusted").await.unwrap().status(), StatusCode::CONFLICT);
    }

    #[tokio::test]
    async fn the_pipeline_view_is_served() {
        let d = Demo::start(7);
        let p = d.pipeline_view().await.unwrap();
        assert_eq!(p.nodes.len(), NODES.len());
        assert!(!p.hosts.is_empty());
        assert!(p.hosts.windows(2).all(|w| w[0].inflight >= w[1].inflight));
        let pending: u64 = p.nodes.iter().map(|n| n.ack_pending).sum();
        assert_eq!(pending, p.hosts.iter().map(|h| h.inflight).sum::<u64>());
    }

    /// The console's endpoints over HTTP, behind the token.
    #[tokio::test]
    async fn the_console_endpoints_answer() {
        use axum::body::Body;
        use axum::http::{Request, StatusCode};
        use tower::ServiceExt;
        let d = Demo::start(7);
        let app = crate::admin::api_routes(d.clone(), "t".into());
        // base64("admin:t")
        let auth = "Basic YWRtaW46dA==".to_string();
        let call = |method: &str, uri: &str, authed: bool| {
            let mut b = Request::builder().method(method).uri(uri);
            if authed {
                b = b.header("authorization", auth.clone());
            }
            app.clone().oneshot(b.body(Body::empty()).unwrap())
        };
        let json = |r: axum::response::Response| async move {
            let b = axum::body::to_bytes(r.into_body(), 1 << 24).await.unwrap();
            serde_json::from_slice::<serde_json::Value>(&b).unwrap()
        };

        let r = call("GET", "/admin/api/hosts/admissions", true).await.unwrap();
        assert_eq!(r.status(), StatusCode::OK);
        let a = json(r).await;
        assert!(a["newHostsPerDay"].as_u64().unwrap() > 0);
        let e = a["entries"].as_array().unwrap();
        assert!(e.iter().any(|x| x["outcome"] == "admitted" && x["tier"].is_string()));
        assert!(e.iter().any(|x| x["outcome"] == "rate-limited"));
        assert!(e.windows(2).all(|w| w[0]["atMs"].as_i64() >= w[1]["atMs"].as_i64()));

        assert_eq!(call("GET", "/admin/api/ops/tail", true).await.unwrap().status(), StatusCode::BAD_REQUEST);
        let r = call("GET", "/admin/api/ops/tail?rejects=1", true).await.unwrap();
        let frames = json(r).await;
        assert!(frames.as_array().unwrap().iter().all(|f| f["kind"] == "reject" || f["kind"] == "held"));
        let o = d.overview().await.unwrap();
        let busy = &o.top_hosts[0];
        assert!(!busy.history.is_empty(), "top hosts carry a rate history");
        let r = call("GET", &format!("/admin/api/ops/tail?host={}&limit=50", busy.host), true).await.unwrap();
        let frames = json(r).await;
        let frames = frames.as_array().unwrap();
        assert!(!frames.is_empty() && frames.len() <= 50);
        assert!(frames.iter().all(|f| f["host"] == busy.host.as_str()));
        assert!(frames.iter().any(|f| f["kind"] == "passed" && f["seq"].is_i64()));

        let uri = format!("/admin/api/hosts/{}/release-throttled", busy.host);
        assert_eq!(call("POST", &uri, false).await.unwrap().status(), StatusCode::UNAUTHORIZED);
        let r = call("POST", &uri, true).await.unwrap();
        assert_eq!(r.status(), StatusCode::OK);
        assert!(json(r).await["released"].is_u64());
        let r = call("POST", "/admin/api/hosts/nope.example/release-throttled", true).await.unwrap();
        assert_eq!(r.status(), StatusCode::NOT_FOUND);

        let r = call("GET", "/admin/api/store", true).await.unwrap();
        let st = json(r).await;
        assert!(st["total"]["perSec"]["a"].as_f64().unwrap() > 0.0);
        assert!(st["purposes"].as_array().unwrap().iter().any(|p| p["purpose"] == "flush"));
        assert!(!st["latency"].as_array().unwrap().is_empty());
        assert!(st["retention"]["plan"]["segment_bytes"].as_u64().unwrap() > 0);
        assert!(st.to_string().find('$').is_none());

        let c = json(call("GET", "/admin/api/cluster", true).await.unwrap()).await;
        let leader = c["leader"].as_str().unwrap();
        assert!(c["nodes"].as_array().unwrap().iter().any(|n| n["id"] == leader && n["role"] == "leader"));
        assert!(
            c["nodes"].as_array().unwrap().iter().all(|n| n.get("leaseValid").is_none() && n["ownedHosts"].is_u64())
        );
        let q = d.quorum().await.unwrap();
        let st = q.nodes.iter().filter_map(|n| n.status.as_ref()).find(|s| s["role"] == "leader").unwrap();
        assert!(st["flush"]["last_at_ms"].as_i64().unwrap() > 0);
        assert!(!st["flush"]["recent"].as_array().unwrap().is_empty());
        assert!(st["history"].as_array().unwrap().iter().any(|e| e["kind"] == "lead"));
        let mut names: Vec<&str> = Vec::new();
        let all = d.hosts(HostQuery { limit: Some(10_000), ..Default::default() }).await.unwrap();
        names.extend(all.hosts.iter().map(|h| h.host.as_str()));
        let n = names.len();
        names.sort();
        names.dedup();
        assert_eq!(names.len(), n, "a hostname twice");

        let u = json(call("GET", "/admin/api/policy/usage", true).await.unwrap()).await;
        assert!(u["plcLookupsBudget"].as_f64().unwrap() > 0.0 && u["newHostsPerDay"].as_u64().unwrap() > 0);
        let sg = json(call("GET", "/admin/api/policy/signals", true).await.unwrap()).await;
        assert_eq!(sg["signals"].as_array().unwrap().len(), 7);
        assert!(
            sg["signals"][0]["top"].as_array().unwrap().iter().all(|k| k["estimate"].as_f64() >= k["lower"].as_f64())
        );
        let did = d.sim.lock().accounts[0].did.clone();
        d.takedown(&did, "spam".into(), "admin").await.unwrap();
        let td = json(call("GET", "/admin/api/takedowns", true).await.unwrap()).await;
        assert!(td.as_array().unwrap().iter().any(|t| t["did"] == did.as_str() && t["reason"] == "spam"));
        assert!(td.as_array().unwrap().iter().all(|t| t["takedown"] == true));
        let hist = json(call("GET", "/admin/api/cluster/quorum/history", true).await.unwrap()).await;
        let ev = hist["events"].as_array().unwrap();
        assert!(ev.iter().any(|e| e["kind"] == "lead" && e["why"] == "election"));
        assert!(ev.windows(2).all(|w| w[0]["atMs"].as_i64() >= w[1]["atMs"].as_i64()));
        let st = json(call("GET", "/admin/api/settings?node=relay-b", true).await.unwrap()).await;
        assert!(st["entries"].as_array().unwrap().iter().any(|e| e["flag"] == "--node-id" && e["value"] == "relay-b"));
        assert_eq!(call("GET", "/admin/api/settings?node=nope", true).await.unwrap().status(), StatusCode::NOT_FOUND);
        let f = json(call("POST", "/admin/api/cluster/quorum/flush", true).await.unwrap()).await;
        assert_eq!(f["role"], "leader");
        let q = d.quorum().await.unwrap();
        assert!(q.nodes.iter().filter_map(|n| n.status.as_ref()).all(|s| s["requests"]["total"].is_object()));
        let cs = d.consumers().await.unwrap();
        let other = cs.iter().find(|c| c.node != "relay-a").unwrap();
        let uri = format!("/admin/api/consumers/{}/kick?node={}", other.id, other.node);
        assert_eq!(call("POST", &uri, true).await.unwrap().status(), StatusCode::NO_CONTENT);
        assert!(cs.iter().all(|c| !c.read_tier.is_empty()));

        let dv = json(call("GET", "/admin/api/discovery", true).await.unwrap()).await;
        let typed: DiscoveryView = serde_json::from_value(dv.clone()).unwrap();
        assert!(typed.sources.iter().any(|s| s.state.url.is_some()), "{typed:?}");
        assert!(dv["sources"].as_array().unwrap().iter().any(|s| s["key"] == "plc" && s["hostsSeen"].is_u64()));
        let r = app
            .clone()
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/admin/api/discovery/run")
                    .header("authorization", auth.clone())
                    .header("content-type", "application/json")
                    .body(Body::from(r#"{"source":"plc"}"#))
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(r.status(), StatusCode::OK);
        let dv = json(r).await;
        assert!(dv["sources"].as_array().unwrap().iter().any(|s| s["key"] == "plc" && s["inProgress"] == true));
        assert!(all.hosts.iter().all(|h| h.source.is_some()));
        assert!(all.hosts.iter().any(|h| h.top_reason.is_some()));
        let busy = &all.hosts.iter().find(|h| h.host == "pds-7f3a.fastvps.cloud").unwrap();
        let det = d.host(&busy.host).await.unwrap();
        let most = det.rejects_by_reason.iter().max_by_key(|(_, n)| **n).map(|(r, _)| *r);
        assert_eq!(busy.top_reason, most);
        let b = json(
            call("GET", "/admin/api/hosts?source=bootstrap:&throttled=true&sort=throttled&desc=true", true)
                .await
                .unwrap(),
        )
        .await;
        let hs = b["hosts"].as_array().unwrap();
        assert!(!hs.is_empty());
        assert!(
            hs.iter().all(|h| h["source"].as_str().unwrap().starts_with("bootstrap:")
                && h["throttledAccounts"].as_u64().unwrap() > 0)
        );
        assert!(hs.windows(2).all(|w| w[0]["throttledAccounts"].as_u64() >= w[1]["throttledAccounts"].as_u64()));
        let held = json(call("GET", "/admin/api/hosts?flag=throttledOrAtCap", true).await.unwrap()).await;
        let hs = held["hosts"].as_array().unwrap();
        assert!(!hs.is_empty());
        assert!(hs.iter().all(|h| {
            let (n, cap) = (h["accounts"].as_u64().unwrap(), h["maxAccounts"].as_u64().unwrap());
            h["throttledAccounts"].as_u64().unwrap() > 0 || (cap > 0 && n >= cap)
        }));
        assert_eq!(call("GET", "/admin/api/hosts?flag=nope", true).await.unwrap().status(), StatusCode::BAD_REQUEST);

        // some hosts wait on the relay, each saying what's full; nobody else carries a reason
        let bp = json(call("GET", "/admin/api/hosts?status=backpressure", true).await.unwrap()).await;
        let hs = bp["hosts"].as_array().unwrap();
        assert!(!hs.is_empty(), "no demo host in backpressure");
        assert!(hs.iter().all(|h| h["status"] == "backpressure"
            && matches!(
                h["backpressureReason"].as_str(),
                Some("inflight_full" | "node_inflight_full" | "queue_full")
            )));
        assert!(all.hosts.iter().all(|h| (h.status == HostStatus::Backpressure) == h.backpressure_reason.is_some()));

        // the tiers are one set wherever the console reads them
        let tiers = |v: &serde_json::Value| -> std::collections::BTreeSet<String> {
            v.as_object().unwrap().keys().cloned().collect()
        };
        let pol = json(call("GET", "/admin/api/policy", true).await.unwrap()).await;
        let full = json(call("GET", "/admin/api/policy/full", true).await.unwrap()).await;
        assert_eq!(tiers(&pol["policy"]["tiers"]), tiers(&full["policy"]["tiers"]));
        let names = tiers(&pol["policy"]["tiers"]);
        assert!(all.hosts.iter().all(|h| names.contains(&h.tier)), "a host in a tier the policy doesn't have");

        let top =
            json(call("GET", "/admin/api/ops/rejects/top?reason=bad-signature&limit=3", true).await.unwrap()).await;
        let top = top.as_array().unwrap();
        assert!(!top.is_empty() && top.len() <= 3);
        assert!(
            top.iter()
                .all(|t| t["host"].is_string() && t["rejectsPerSec"].is_number() && t["total"].as_u64() > Some(0))
        );
        assert!(top.iter().all(|t| t["sample"].is_null() || t["sample"]["reason"] == "bad-signature"));
        assert!(top.windows(2).all(|w| w[0]["rejectsPerSec"].as_f64() >= w[1]["rejectsPerSec"].as_f64()));

        let p = json(call("GET", "/admin/api/ops/plc", true).await.unwrap()).await;
        assert_eq!(p["enabled"], true);
        assert_eq!(p["windows"].as_array().unwrap().len(), 4);
    }

    #[tokio::test]
    async fn quorum_members_and_tuning() {
        let d = Demo::start(7);
        let q = d.quorum().await.unwrap();
        assert_eq!(q.nodes.len(), 3);
        let leader = q.nodes.iter().filter_map(|n| n.status.as_ref()).find(|s| s["role"] == "leader").unwrap();
        assert_eq!(leader["members"].as_array().unwrap().len(), 3);
        let add = |m: &[&str], addrs: &[(&str, &str)]| QuorumMembersChange {
            members: m.iter().map(|s| s.to_string()).collect(),
            addrs: addrs.iter().map(|(k, v)| (k.to_string(), v.to_string())).collect(),
        };
        let want = ["relay-a", "relay-b", "relay-c", "relay-d"];
        assert!(matches!(d.change_quorum_members(add(&want, &[]), "admin").await, Err(AdminError::BadRequest(_))));
        d.change_quorum_members(add(&want, &[("relay-d", "10.0.7.14:2981")]), "admin").await.unwrap();
        let q = d.quorum().await.unwrap();
        assert!(q.nodes.iter().any(|n| n.node == "relay-d"));

        let f = d.full_policy().await.unwrap();
        let mut p = f.policy.clone();
        p["consumers"]["connectionsPerIp"] = serde_json::json!(32);
        let u = FullPolicyUpdate { base_version: f.version, policy: p.clone(), note: "t".into() };
        let n = d.update_full_policy(u, "admin").await.unwrap();
        assert_eq!(n.version, f.version + 1);
        assert_eq!(d.policy().await.unwrap().version, n.version);
        let stale = FullPolicyUpdate { base_version: f.version, policy: p, note: String::new() };
        assert!(matches!(d.update_full_policy(stale, "admin").await, Err(AdminError::Conflict(_))));

        let s = d.settings().await.unwrap();
        assert!(s.entries.iter().filter(|e| e.secret).all(|e| e.value.is_none()));
    }

    /// SSE frames off a response body as (event, id, data).
    pub(crate) async fn sse_frames(
        body: axum::body::Body,
        want: impl Fn(&[(String, Option<String>, serde_json::Value)]) -> bool,
    ) -> Vec<(String, Option<String>, serde_json::Value)> {
        use futures::StreamExt;
        let mut stream = body.into_data_stream();
        let mut buf = String::new();
        let mut out = Vec::new();
        let deadline = tokio::time::Instant::now() + Duration::from_secs(10);
        while !want(&out) {
            let chunk = tokio::time::timeout_at(deadline, stream.next()).await.expect("events in time");
            buf.push_str(std::str::from_utf8(&chunk.expect("open").expect("chunk")).unwrap());
            while let Some(end) = buf.find("\n\n") {
                let frame: String = buf.drain(..end + 2).collect();
                let (mut ev, mut id, mut data) = ("message".to_string(), None, String::new());
                for line in frame.lines() {
                    if let Some(v) = line.strip_prefix("event: ") {
                        ev = v.into();
                    } else if let Some(v) = line.strip_prefix("id: ") {
                        id = Some(v.to_string());
                    } else if let Some(v) = line.strip_prefix("data: ") {
                        data.push_str(v);
                    }
                }
                if !data.is_empty() {
                    out.push((ev, id, serde_json::from_str(&data).unwrap()));
                }
            }
        }
        out
    }

    #[tokio::test]
    async fn the_change_feed_names_what_the_actions_changed() {
        use axum::body::Body;
        use axum::http::{Request, StatusCode};
        use tower::ServiceExt;
        let d = Demo::start(7);
        let app = crate::admin::api_routes(d.clone(), "t".into());
        let auth = "Basic YWRtaW46dA==";
        let open = |since: Option<String>| {
            let mut b = Request::get("/admin/api/changes").header("authorization", auth);
            if let Some(s) = since {
                b = b.header("last-event-id", s);
            }
            app.clone().oneshot(b.body(Body::empty()).unwrap())
        };
        let r = app.clone().oneshot(Request::get("/admin/api/changes").body(Body::empty()).unwrap()).await.unwrap();
        assert_eq!(r.status(), StatusCode::UNAUTHORIZED);
        let r = open(None).await.unwrap();
        assert_eq!(r.status(), StatusCode::OK);
        assert_eq!(r.headers()["content-type"], "text/event-stream");

        let host = d.hosts(HostQuery::default()).await.unwrap().hosts[0].host.clone();
        let row = d.host_action(&host, HostAction::Throttle { events_per_sec: Some(5.0) }, "t").await.unwrap();
        let did = d.sim.lock().accounts[0].did.clone();
        d.takedown(&did, "spam".into(), "t").await.unwrap();
        let p = d.policy().await.unwrap();
        let mut pol = p.policy.clone();
        pol.spam.reject_ratio = (pol.spam.reject_ratio * 0.5).max(0.01);
        let u = PolicyUpdate { base_version: p.version, policy: pol, note: String::new() };
        let p2 = d.update_policy(u, "t").await.unwrap();
        let c = d.consumers().await.unwrap()[0].clone();
        d.kick_consumer_on(Some(&c.node), c.id, "t").await.unwrap();

        let has = |fs: &[(String, Option<String>, serde_json::Value)], kind: &str, id: &str| {
            fs.iter().any(|(e, _, v)| e == "change" && v["kind"] == kind && v["id"] == id)
        };
        let cid = format!("{}/{}", c.node, c.id);
        let frames = sse_frames(r.into_body(), |fs| {
            has(fs, "host", &host)
                && has(fs, "takedown", &did)
                && has(fs, "policy", "policy")
                && has(fs, "consumer", &cid)
        })
        .await;
        assert_eq!(frames[0].0, "hello");
        assert_eq!(frames[0].2["resumed"], false);
        let find = |kind: &str| frames.iter().find(|(e, _, v)| e == "change" && v["kind"] == kind).unwrap();
        let h = find("host");
        assert_eq!(h.2["version"].as_str(), row.version.as_deref(), "the row carries the event's version");
        assert_eq!(h.2["hint"]["status"], serde_json::to_value(row.status).unwrap());
        assert_eq!(find("policy").2["version"], p2.version.to_string());
        assert_eq!(find("takedown").2["hint"]["takedown"], true);

        // resuming after the host event replays what followed it
        let after = h.1.clone().unwrap();
        let r = open(Some(after)).await.unwrap();
        let again = sse_frames(r.into_body(), |fs| has(fs, "policy", "policy")).await;
        assert_eq!(again[0].2["resumed"], true);
        assert!(!has(&again, "host", &host));
        let r = open(Some("nope.1".into())).await.unwrap();
        let again = sse_frames(r.into_body(), |fs| fs.len() >= 2).await;
        assert_eq!((again[1].0.as_str(), &again[1].2["reason"]), ("resync", &serde_json::json!("unknown-cursor")));
    }
}
