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

const HOST_SHARDS: usize = 64;
const DID_SHARDS: usize = 256;
const NODES: [&str; 3] = ["relay-a", "relay-b", "relay-c"];
const HISTORY: usize = 300;
const HOST_HISTORY: usize = 120;
const RECENT_REJECTS: usize = 40;
const EVENT_BYTES: f64 = 4_600.0;

pub struct Demo {
    sim: Mutex<Sim>,
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
        let demo = Arc::new(Demo {
            sim: Mutex::new(sim),
        });
        let weak = Arc::downgrade(&demo);
        tokio::spawn(async move {
            let mut iv = tokio::time::interval(Duration::from_secs(1));
            iv.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
            loop {
                iv.tick().await;
                let Some(d) = weak.upgrade() else { break };
                d.sim.lock().tick(now_ms());
            }
        });
        demo
    }
}

fn now_ms() -> i64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_millis() as i64)
        .unwrap_or(0)
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
    s.bytes().fold(0xcbf2_9ce4_8422_2325u64, |h, b| {
        (h ^ b as u64).wrapping_mul(0x0000_0100_0000_01b3)
    })
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
    rate: f64,
    err: f64,
    lag: f64,
    series: VecDeque<(i64, f32, f32)>,
    recent: VecDeque<RejectSample>,
    by_reason: BTreeMap<RejectReason, u64>,
    actions: Vec<HostActionRecord>,
    shard: usize,
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
    "moss", "fern", "lichen", "cedar", "harbor", "signal", "static", "pixel", "kettle", "orbit",
    "quartz", "ember", "tidal", "basalt", "copper", "willow", "hollow", "lantern", "cobalt",
    "meadow", "falcon", "otter", "badger", "heron", "juniper", "sparrow", "nimbus", "delta",
    "granite", "rowan", "thistle", "aurora", "cinder", "drift", "fable", "glade", "haze", "ivory",
    "jetty", "knoll", "lumen", "marsh", "nectar", "onyx", "prairie", "quill", "raven", "sable",
];
const NAMES: [&str; 24] = [
    "alex", "sam", "jo", "kai", "rin", "max", "lee", "ana", "noa", "eli", "mia", "tom", "ivy",
    "zoe", "ben", "lou", "ada", "ray", "sol", "ola", "jun", "kit", "pia", "rex",
];
const TLDS: [&str; 10] = [
    "com", "dev", "social", "net", "org", "xyz", "io", "blue", "cloud", "me",
];

fn default_policy() -> Policy {
    let tier = |eps: f64, hour: u64, day: u64, max: u64, newh: u64| TierLimits {
        events_per_sec: eps,
        events_per_hour: hour,
        events_per_day: day,
        max_accounts: max,
        new_accounts_per_hour: newh,
    };
    Policy {
        tiers: BTreeMap::from([
            (
                "trusted".to_string(),
                tier(5_000.0, 15_000_000, 300_000_000, 5_000_000, 50_000),
            ),
            (
                "standard".to_string(),
                tier(50.0, 150_000, 2_000_000, 100_000, 500),
            ),
            (
                "probation".to_string(),
                tier(10.0, 20_000, 200_000, 1_000, 50),
            ),
        ]),
        default_tier: "probation".into(),
        spam: SpamThresholds {
            new_accounts_per_hour: 300,
            reject_ratio: 0.2,
            bad_signatures_per_min: 60,
            account_events_per_sec: 20.0,
            auto_throttle: false,
        },
    }
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
                Profile::SelfHosted => (
                    if rng.chance(0.03) {
                        rng.range(0.01, 0.06)
                    } else {
                        rng.range(0.0, 0.003)
                    },
                    0.0,
                ),
                Profile::Buggy => (rng.range(0.25, 0.45), rng.range(0.0, 3.0)),
                Profile::SpamAccounts => (rng.range(0.02, 0.06), rng.range(900.0, 2400.0)),
                Profile::SpamSigs => (rng.range(0.3, 0.6), rng.range(10.0, 60.0)),
                Profile::Flood => (rng.range(0.01, 0.03), rng.range(0.0, 2.0)),
            };
            let since = now - (rng.range(60.0, 86_400.0 * 9.0) * 1000.0) as i64;
            hosts.push(SimHost {
                shard: (hash(&name) % HOST_SHARDS as u64) as usize,
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
                2 => format!(
                    "pds.{w}-{}.org",
                    rng.pick(&["collective", "coop", "commons", "guild"])
                ),
                _ => format!("bsky.{w}.{}", rng.pick(&["net", "dev", "community"])),
            };
            let accounts = (rng.range(7.5, 11.3)).exp() as u64;
            let rate = accounts as f64 * rng.range(0.0006, 0.0018);
            let tier = if accounts > 20_000 {
                "trusted"
            } else {
                "standard"
            };
            add(
                &mut hosts,
                &mut rng,
                name,
                tier,
                Profile::Community,
                rate,
                accounts,
            );
        }
        for (i, name) in [
            "pds.patchwork-proto.dev",
            "atp.homebrew-pds.net",
            "pds.experimental-sync.org",
            "repo.toy-pds.xyz",
        ]
        .iter()
        .enumerate()
        {
            let accounts = 20 + i as u64 * 37;
            add(
                &mut hosts,
                &mut rng,
                name.to_string(),
                "standard",
                Profile::Buggy,
                0.8 + i as f64 * 1.1,
                accounts,
            );
        }
        let spam: [(&str, Profile, f64, u64); 7] = [
            (
                "pds-7f3a.fastvps.cloud",
                Profile::SpamAccounts,
                38.0,
                14_200,
            ),
            ("pds-91c2.fastvps.cloud", Profile::SpamAccounts, 22.0, 8_900),
            ("social-boost.click", Profile::SpamAccounts, 30.0, 11_400),
            ("free-followers.xyz", Profile::SpamSigs, 18.0, 2_300),
            ("pds.cryptoairdrop.live", Profile::SpamSigs, 9.0, 640),
            ("reply-guy.network", Profile::Flood, 48.0, 3),
            ("autopost.megabot.io", Profile::Flood, 31.0, 12),
        ];
        for (name, p, rate, accounts) in spam {
            add(
                &mut hosts,
                &mut rng,
                name.to_string(),
                "probation",
                p,
                rate,
                accounts,
            );
        }
        while hosts.len() < 5_000 {
            let name = match rng.below(6) {
                0 => format!(
                    "pds.{}{}.{}",
                    rng.pick(&NAMES),
                    rng.pick(&WORDS),
                    rng.pick(&TLDS)
                ),
                1 => format!(
                    "{}.{}.{}",
                    rng.pick(&WORDS),
                    rng.pick(&NAMES),
                    rng.pick(&TLDS)
                ),
                2 => format!(
                    "bsky.{}{}.{}",
                    rng.pick(&NAMES),
                    rng.below(100),
                    rng.pick(&TLDS)
                ),
                3 => format!(
                    "pds.{}-{}.{}",
                    rng.pick(&WORDS),
                    rng.pick(&WORDS),
                    rng.pick(&TLDS)
                ),
                4 => format!(
                    "{}.pds.{}{}.{}",
                    rng.pick(&NAMES),
                    rng.pick(&WORDS),
                    rng.below(10),
                    rng.pick(&TLDS)
                ),
                _ => format!(
                    "atproto.{}{}.{}",
                    rng.pick(&WORDS),
                    rng.pick(&NAMES),
                    rng.pick(&TLDS)
                ),
            };
            if hosts.iter().any(|h| h.name == name) {
                continue;
            }
            // most self-hosted PDSes hold one or two accounts
            let accounts = if rng.chance(0.7) {
                1 + rng.below(3) as u64
            } else {
                rng.range(1.0, 7.0).exp() as u64
            };
            let rate = accounts as f64 * rng.range(0.0002, 0.004) * rng.jitter(0.8);
            let tier = if rng.chance(0.3) {
                "probation"
            } else {
                "standard"
            };
            add(
                &mut hosts,
                &mut rng,
                name,
                tier,
                Profile::SelfHosted,
                rate,
                accounts,
            );
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
            (0..n)
                .map(|i| Some(NODES[(i * 7 + i / skew) % NODES.len()].to_string()))
                .collect()
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
            policy: PolicyDoc {
                version: 0,
                policy: policy.clone(),
                updated_at_ms: now,
                updated_by: "admin".into(),
            },
            audit: Vec::new(),
            rules: Vec::new(),
            next_rule: 1,
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
        p.tiers.get_mut("standard").unwrap().events_per_sec = 30.0;
        p.spam.new_accounts_per_hour = 500;
        type Step = (i64, &'static str, Box<dyn Fn(&mut Policy)>);
        let steps: [Step; 3] = [
            (now - 21 * day, "initial limits", Box::new(|_| {})),
            (
                now - 9 * day,
                "standard tier was clipping small community PDSes at peak",
                Box::new(|p| {
                    p.tiers.get_mut("standard").unwrap().events_per_sec = 50.0;
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
                Some(old) => diff_json(
                    &serde_json::to_value(old).unwrap(),
                    &serde_json::to_value(&p).unwrap(),
                ),
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
            (
                "*.cryptoairdrop.live",
                RuleEffect::Ban,
                "forged commits, every subdomain is the same operator",
                6,
            ),
            (
                "*.fastvps.cloud",
                RuleEffect::Tier {
                    tier: "probation".into(),
                },
                "cheap VPS range used by account farms",
                3,
            ),
            (
                "*.host.bsky.network",
                RuleEffect::Tier {
                    tier: "trusted".into(),
                },
                "Bluesky's PDS fleet",
                30,
            ),
            (
                "*.megabot.io",
                RuleEffect::Throttle {
                    events_per_sec: 5.0,
                },
                "bot platform, fine at low volume",
                1,
            ),
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
                format!(
                    "2a01:4f8:{:x}:{:x}::1",
                    0x1000 + self.rng.below(0xeff),
                    self.rng.below(0xffff)
                )
            } else {
                format!(
                    "{}.{}.{}.{}",
                    self.rng
                        .pick(&[5, 23, 34, 45, 65, 88, 104, 138, 147, 157, 172, 185, 203]),
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
                lag_ms: if backfilling {
                    self.rng.range(3.6e6, 4.0e7)
                } else {
                    0.0
                },
                events_per_sec: 0.0,
                bytes_per_sec: 0.0,
                backfilling,
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
            let did = format!(
                "did:plc:{}",
                (0..24)
                    .map(|_| B32[self.rng.below(32)] as char)
                    .collect::<String>()
            );
            let host = self.hosts[hi].name.clone();
            let handle = if hi < 24 {
                format!(
                    "{}{}.bsky.social",
                    self.rng.pick(&WORDS),
                    self.rng.below(1000)
                )
            } else {
                let base = host
                    .trim_start_matches("pds.")
                    .trim_start_matches("bsky.")
                    .to_string();
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

    fn synth_account(
        &mut self,
        did: String,
        handle: Option<String>,
        hi: usize,
        now: i64,
    ) -> Account {
        let h = &self.hosts[hi];
        let spam = matches!(
            h.profile,
            Profile::SpamAccounts | Profile::SpamSigs | Profile::Flood
        );
        let per_acct = h.base_rate / h.accounts.max(1) as f64;
        let shard = (hash(&did) % DID_SHARDS as u64) as u32;
        let upstream = if self.rng.chance(0.02) {
            "deactivated"
        } else {
            "active"
        };
        Account {
            handle,
            host: h.name.clone(),
            status: if spam {
                "throttled".into()
            } else {
                upstream.into()
            },
            upstream_status: upstream.into(),
            takedown: None,
            rev: format!(
                "3m{}",
                (0..11)
                    .map(|_| (b'a' + self.rng.below(26) as u8) as char)
                    .collect::<String>()
            ),
            last_seq: self.last_seq - self.rng.below(5_000_000) as i64,
            last_event_ms: now - (self.rng.range(0.5, 3.0 * 86_400.0) * 1000.0) as i64,
            events_last_hour: (per_acct * 3600.0 * self.rng.jitter(1.0)) as u64,
            rejects_last_hour: if spam { self.rng.below(400) as u64 } else { 0 },
            did_shard: shard,
            node: self.did_shards[shard as usize].clone().unwrap_or_default(),
            did,
            archive: None,
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
                notes: vec![CaseNote {
                    at_ms: opened + 2_400_000,
                    by: "admin".into(),
                    text: note.into(),
                }],
            });
        }
    }

    fn apply_rule_effect(&mut self, pattern: &str, effect: &RuleEffect) -> u32 {
        let mut n = 0;
        for h in self.hosts.iter_mut() {
            if !rule_matches(pattern, &h.name) {
                continue;
            }
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
        let diurnal = 1.0
            + 0.16 * (t * std::f64::consts::TAU / 1200.0).sin()
            + 0.05 * (t * std::f64::consts::TAU / 173.0).sin();
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
            let (coin, coin2, pick, acct, back) = (
                self.rng.f(),
                self.rng.f(),
                self.rng.next(),
                self.rng.next(),
                self.rng.below(900) as i64,
            );
            let h = &mut self.hosts[i];
            match h.status {
                HostStatus::Backoff if h.redial_at.is_some_and(|r| r <= now) => {
                    h.status = HostStatus::Connected;
                    h.connected_since = Some(now);
                    h.redial_at = None;
                }
                HostStatus::Connected | HostStatus::Idle
                    if h.profile == Profile::SelfHosted && roll < 0.00004 =>
                {
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
                HostStatus::Connected | HostStatus::Idle | HostStatus::Throttled
            );
            let (rate, err) = if live {
                let burst = match h.profile {
                    Profile::SpamAccounts | Profile::Flood => {
                        1.0 + 0.6 * ((t / 37.0 + i as f64).sin()).max(0.0)
                    }
                    _ => 1.0,
                };
                let want = h.base_rate * level * jit * burst;
                let tier_cap = policy
                    .tiers
                    .get(&h.tier)
                    .map(|l| l.events_per_sec)
                    .unwrap_or(f64::INFINITY);
                let cap = h.throttle.unwrap_or(f64::INFINITY).min(tier_cap);
                let rate = want.min(cap);
                h.status = if want > cap * 1.001 {
                    HostStatus::Throttled
                } else if rate < 0.002 && h.profile == Profile::SelfHosted {
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
            h.lag = if live {
                h.lag_base * jit * (1.0 + self.surge)
            } else {
                0.0
            };
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
                    let v = if k + 1 == reasons.len() {
                        left
                    } else {
                        rej * share
                    };
                    left -= v;
                    *rejects.entry(*reason).or_default() += v;
                    *h.by_reason.entry(*reason).or_default() +=
                        v.trunc() as u64 + u64::from(coin < v.fract());
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
                c.lag_ms =
                    (c.lag_ms - 1000.0 * (c.events_per_sec / ev_out.max(1.0) - 1.0)).max(0.0);
                if c.lag_ms < 50.0 {
                    c.backfilling = false;
                }
            } else {
                c.events_per_sec = ev_out * self.rng.jitter(0.01);
                c.lag_ms =
                    self.rng.range(1.0, 25.0) * if self.rng.chance(0.03) { 20.0 } else { 1.0 };
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
                    .pick(&[
                        "python-websockets/13.1",
                        "Go-http-client/1.1",
                        "node-ws/8.18",
                    ])
                    .to_string(),
                node: self.rng.pick(&NODES).to_string(),
                connected_since_ms: now,
                cursor: self.last_seq,
                lag_ms: 0.0,
                events_per_sec: 0.0,
                bytes_per_sec: 0.0,
                backfilling: false,
            });
        } else if self.rng.chance(0.008) && self.consumers.len() > 16 {
            let i = self.rng.below(self.consumers.len());
            self.consumers.remove(i);
        }

        let busy = ev_in / 60_000.0;
        let p50 = 31.0 + 6.0 * busy + self.rng.range(-2.0, 2.0) + 20.0 * self.surge;
        let spike = if self.rng.chance(0.02) {
            self.rng.range(80.0, 260.0)
        } else {
            0.0
        };
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
        let tier_newh = |t: &str| {
            tiers
                .get(t)
                .map(|l| l.new_accounts_per_hour as f64)
                .unwrap_or(0.0)
        };
        let mut open = Vec::new();
        for (i, h) in self.hosts.iter().enumerate() {
            if !matches!(
                h.status,
                HostStatus::Connected | HostStatus::Throttled | HostStatus::Idle
            ) {
                continue;
            }
            let sigs_per_min = h.rate
                * h.err
                * 60.0
                * if h.profile == Profile::SpamSigs {
                    0.8
                } else {
                    0.0
                };
            let top_acct = if h.profile == Profile::Flood {
                h.rate * 0.9
            } else {
                h.rate / h.accounts.max(1) as f64
            };
            let checks = [
                // a tier that allows more sign-ups (trusted) raises the bar with it
                (
                    "new-accounts",
                    h.new_accounts_per_hour,
                    (spam.new_accounts_per_hour as f64).max(tier_newh(&h.tier)),
                ),
                (
                    "reject-ratio",
                    if h.rate > 0.05 { h.err } else { 0.0 },
                    spam.reject_ratio,
                ),
                (
                    "bad-signatures",
                    sigs_per_min,
                    spam.bad_signatures_per_min as f64,
                ),
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
                c.host == name
                    && c.kind == kind
                    && matches!(c.status, CaseStatus::Open | CaseStatus::Acknowledged)
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
                let cap = self
                    .policy
                    .policy
                    .tiers
                    .get("probation")
                    .map(|t| t.events_per_sec)
                    .unwrap_or(10.0);
                self.hosts[i].throttle = Some(cap);
                auto_action = Some(format!("throttled to {cap} events/s"));
            }
            let id = self.next_case;
            self.next_case += 1;
            // the first batch (at startup) is backdated so the list has a spread of ages
            let opened = if self.history.len() < 5 {
                now - (self.rng.range(120.0, 30_000.0) * 1000.0) as i64
            } else {
                now
            };
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
            events_per_sec: round2(h.rate),
            error_rate: (h.err * 10_000.0).round() / 10_000.0,
            accounts: h.accounts,
            last_upstream_seq: h.seq,
            connected_since_ms: h.connected_since,
            lag_ms: round2(h.lag),
            throttle: h.throttle,
            rule: self
                .rules
                .iter()
                .find(|r| rule_matches(&r.pattern, &h.name))
                .map(|r| r.id),
            node: self.host_shards[h.shard].clone().unwrap_or_default(),
        }
    }

    fn host_idx(&self, host: &str) -> AdminResult<usize> {
        self.by_name
            .get(&host.to_ascii_lowercase())
            .copied()
            .ok_or_else(|| AdminError::NotFound(format!("no host {host}")))
    }

    fn rule_view(&self, r: &DomainRule) -> DomainRule {
        DomainRule {
            matches: self
                .hosts
                .iter()
                .filter(|h| rule_matches(&r.pattern, &h.name))
                .count() as u32,
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
        let valid = did.strip_prefix("did:plc:").is_some_and(|s| {
            s.len() == 24
                && s.bytes()
                    .all(|b| b.is_ascii_lowercase() || (b'2'..=b'7').contains(&b))
        }) || did.strip_prefix("did:web:").is_some_and(|s| !s.is_empty());
        if !valid {
            return Err(AdminError::NotFound(format!(
                "{did} is not a DID this relay has seen"
            )));
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
        Profile::Buggy => &[
            (InvalidCommit, 0.45),
            (RevOutOfOrder, 0.3),
            (PrevDataMismatch, 0.2),
            (Malformed, 0.05),
        ],
        Profile::SpamSigs => &[(BadSignature, 0.8), (WrongHost, 0.15), (UnknownDid, 0.05)],
        Profile::SpamAccounts => &[(UnknownDid, 0.5), (RateLimited, 0.4), (BadSignature, 0.1)],
        Profile::Flood => &[(RateLimited, 0.85), (TooLarge, 0.15)],
        Profile::Big => &[
            (WrongHost, 0.55),
            (PrevDataMismatch, 0.25),
            (Takendown, 0.2),
        ],
        Profile::Community | Profile::SelfHosted => &[
            (RevOutOfOrder, 0.3),
            (WrongHost, 0.3),
            (InvalidCommit, 0.2),
            (TooLarge, 0.2),
        ],
    }
}

fn reject_detail(r: RejectReason) -> &'static str {
    match r {
        RejectReason::BadSignature => {
            "commit signature doesn't verify against the DID document's #atproto key"
        }
        RejectReason::InvalidCommit => "MST root in the CAR doesn't match the commit's data CID",
        RejectReason::RevOutOfOrder => "rev is not after the last rev the relay accepted",
        RejectReason::PrevDataMismatch => "prevData doesn't match the last accepted commit's data",
        RejectReason::WrongHost => "DID document names a different PDS",
        RejectReason::UnknownDid => "DID didn't resolve (PLC 404)",
        RejectReason::TooLarge => "frame over 2 MiB",
        RejectReason::RateLimited => "host over its tier's events/s",
        RejectReason::Takendown => "account is taken down on this relay",
        RejectReason::Malformed => "frame header isn't valid DAG-CBOR",
    }
}

fn case_summary(kind: &str, obs: f64, thr: f64) -> String {
    match kind {
        "new-accounts" => format!("{obs:.0} new accounts/h (threshold {thr:.0})"),
        "reject-ratio" => format!(
            "{:.0}% of frames rejected (threshold {:.0}%)",
            obs * 100.0,
            thr * 100.0
        ),
        "bad-signatures" => format!("{obs:.0} bad signatures/min (threshold {thr:.0})"),
        "account-rate" => format!("one account at {obs:.1} events/s (threshold {thr:.0})"),
        _ => format!("{obs:.2} over {thr:.2}"),
    }
}

fn validate_pattern(p: &str) -> AdminResult<String> {
    let p = p.trim().to_ascii_lowercase();
    let base = p.strip_prefix("*.").unwrap_or(&p);
    let ok = base.contains('.')
        && base.split('.').all(|l| {
            !l.is_empty()
                && l.len() <= 63
                && l.bytes().all(|b| b.is_ascii_alphanumeric() || b == b'-')
        });
    if !ok {
        return Err(AdminError::BadRequest(format!(
            "{p:?} isn't a hostname or *.domain pattern"
        )));
    }
    Ok(p)
}

// ---------------------------------------------------------------- AdminSource

impl AdminSource for Demo {
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
        let mut hist = History {
            sample_secs: 1,
            ..Default::default()
        };
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
                hist.rejects
                    .entry(r)
                    .or_default()
                    .push(round2(x.rejects.get(&r).copied().unwrap_or(0.0)));
            }
        }
        let connected = s
            .hosts
            .iter()
            .filter(|h| {
                matches!(
                    h.status,
                    HostStatus::Connected | HostStatus::Idle | HostStatus::Throttled
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
            rejects_by_reason: rejects_by_reason
                .into_iter()
                .map(|(k, v)| (k, round2(v)))
                .collect(),
            time_to_firehose_p50_ms: round2(last.p50),
            time_to_firehose_p99_ms: round2(last.p99),
            log_durability_lag_ms: round2(last.dur),
            last_seq: s.last_seq,
            open_cases: s
                .cases
                .iter()
                .filter(|c| c.status == CaseStatus::Open)
                .count() as u32,
            top_hosts: idx.iter().take(12).map(|&i| s.row(&s.hosts[i])).collect(),
            history: hist,
            stream_events_per_sec: round2(last.ev_in),
            by_node: Vec::new(),
        })
    }

    async fn hosts(&self, q: HostQuery) -> AdminResult<HostList> {
        let s = self.sim.lock();
        let needle =
            q.q.as_deref()
                .map(str::to_ascii_lowercase)
                .filter(|x| !x.is_empty());
        let mut rows: Vec<HostRow> = s
            .hosts
            .iter()
            .filter(|h| needle.as_deref().is_none_or(|n| h.name.contains(n)))
            .filter(|h| q.tier.as_deref().is_none_or(|t| h.tier == t))
            .filter(|h| q.status.is_none_or(|st| h.status == st))
            .map(|h| s.row(h))
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
                _ => a.events_per_sec.total_cmp(&b.events_per_sec),
            };
            if q.desc { o.reverse() } else { o }
        });
        let total = rows.len();
        let rows = rows
            .into_iter()
            .skip(q.offset.unwrap_or(0))
            .take(q.limit.unwrap_or(10_000))
            .collect();
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
        let mut series = HostSeries {
            sample_secs: 1,
            ..Default::default()
        };
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
                .filter(|c| {
                    c.host == h.name
                        && matches!(c.status, CaseStatus::Open | CaseStatus::Acknowledged)
                })
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
        if let HostAction::Throttle {
            events_per_sec: Some(x),
        } = &action
            && !(x.is_finite() && *x >= 0.0)
        {
            return Err(AdminError::BadRequest(
                "throttle must be ≥ 0 events/s".into(),
            ));
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
        h.actions.push(HostActionRecord {
            at_ms: now,
            by: by.into(),
            action,
        });
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
            return Err(AdminError::Conflict(format!(
                "a rule for {pattern} already exists"
            )));
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
        };
        s.apply_rule_effect(&pattern, &rule.effect);
        s.rules.push(r.clone());
        Ok(s.rule_view(&r))
    }

    async fn update_domain_rule(
        &self,
        id: u64,
        rule: DomainRuleInput,
        _by: &str,
    ) -> AdminResult<DomainRule> {
        let pattern = validate_pattern(&rule.pattern)?;
        let mut s = self.sim.lock();
        check_effect(&s, &rule.effect)?;
        if s.rules.iter().any(|r| r.pattern == pattern && r.id != id) {
            return Err(AdminError::Conflict(format!(
                "a rule for {pattern} already exists"
            )));
        }
        let i = s
            .rules
            .iter()
            .position(|r| r.id == id)
            .ok_or_else(|| AdminError::NotFound(format!("no rule {id}")))?;
        s.rules[i].pattern = pattern.clone();
        s.rules[i].effect = rule.effect.clone();
        s.rules[i].note = rule.note;
        s.apply_rule_effect(&pattern, &rule.effect);
        let r = s.rules[i].clone();
        Ok(s.rule_view(&r))
    }

    async fn delete_domain_rule(&self, id: u64, _by: &str) -> AdminResult<()> {
        let mut s = self.sim.lock();
        let i = s
            .rules
            .iter()
            .position(|r| r.id == id)
            .ok_or_else(|| AdminError::NotFound(format!("no rule {id}")))?;
        s.rules.remove(i);
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
        s.policy = PolicyDoc {
            version: s.policy.version + 1,
            policy: u.policy,
            updated_at_ms: now,
            updated_by: by.into(),
        };
        let version = s.policy.version;
        s.audit.push(PolicyAudit {
            version,
            at_ms: now,
            by: by.into(),
            note: u.note,
            changes,
        });
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
        let mut s = self.sim.lock();
        let i = s
            .consumers
            .iter()
            .position(|c| c.id == id)
            .ok_or_else(|| AdminError::NotFound(format!("no consumer {id}")))?;
        s.consumers.remove(i);
        Ok(())
    }

    async fn cluster(&self) -> AdminResult<ClusterView> {
        let s = self.sim.lock();
        let now = s.now_ms;
        let nodes = NODES
            .iter()
            .enumerate()
            .map(|(i, id)| {
                let hosts: Vec<&SimHost> = s
                    .hosts
                    .iter()
                    .filter(|h| s.host_shards[h.shard].as_deref() == Some(*id))
                    .collect();
                let ev_in: f64 = hosts.iter().map(|h| h.rate).sum();
                let consumers: Vec<&Consumer> =
                    s.consumers.iter().filter(|c| c.node == *id).collect();
                let ev_out: f64 = consumers.iter().map(|c| c.events_per_sec).sum();
                NodeView {
                    id: id.to_string(),
                    addr: format!("10.0.7.{}:2700", 11 + i),
                    version: env!("CARGO_PKG_VERSION").into(),
                    rev: "530a3e45".into(),
                    reachable: true,
                    lease_valid: true,
                    // leases renew every ~3 s with a 10 s TTL
                    lease_expires_ms: now + 7_000 + ((now + i as i64 * 1_100) % 3_000),
                    host_shards: s
                        .host_shards
                        .iter()
                        .filter(|o| o.as_deref() == Some(*id))
                        .count() as u32,
                    did_shards: s
                        .did_shards
                        .iter()
                        .filter(|o| o.as_deref() == Some(*id))
                        .count() as u32,
                    hosts: hosts.len() as u32,
                    consumers: consumers.len() as u32,
                    events_in_per_sec: round2(ev_in),
                    events_out_per_sec: round2(ev_out),
                    log_durability_lag_ms: round2(
                        s.history.back().map(|x| x.dur).unwrap_or(0.0) * (0.85 + 0.1 * i as f64),
                    ),
                    cpu: round2((ev_in / 6_000.0 + ev_out / 400_000.0).min(7.6)),
                    mem_bytes: (9.5e9 + ev_in * 6.0e4) as u64,
                    role: "core".into(),
                    stale: false,
                    error: None,
                    reported_ms: now,
                    bytes_out_per_sec: consumers.iter().map(|c| c.bytes_per_sec).sum(),
                    stream_seq: s.last_seq,
                }
            })
            .collect();
        Ok(ClusterView {
            nodes,
            host_shards: s.host_shards.clone(),
            did_shards: s.did_shards.clone(),
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
                    || a.handle
                        .as_deref()
                        .is_some_and(|h| h.starts_with(&q) || h.contains(&q))
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
        s.takedowns.insert(
            did.to_string(),
            Takedown {
                at_ms: now,
                by: by.into(),
                reason,
            },
        );
        Ok(s.account_view(&s.accounts[i]))
    }

    async fn untakedown(&self, did: &str, _by: &str) -> AdminResult<Account> {
        let mut s = self.sim.lock();
        let i = s.find_account(did)?;
        s.takedowns.remove(did);
        Ok(s.account_view(&s.accounts[i]))
    }

    async fn cases(&self, q: CaseQuery) -> AdminResult<Vec<Case>> {
        let s = self.sim.lock();
        let mut out: Vec<Case> = s
            .cases
            .iter()
            .filter(|c| q.status.is_none_or(|st| c.status == st))
            .cloned()
            .collect();
        out.sort_by(|a, b| {
            b.severity
                .cmp(&a.severity)
                .then(b.opened_at_ms.cmp(&a.opened_at_ms))
        });
        Ok(out)
    }

    async fn case(&self, id: u64) -> AdminResult<Case> {
        let s = self.sim.lock();
        s.cases
            .iter()
            .find(|c| c.id == id)
            .cloned()
            .ok_or_else(|| AdminError::NotFound(format!("no case {id}")))
    }

    async fn update_case(&self, id: u64, u: CaseUpdate, by: &str) -> AdminResult<Case> {
        let mut s = self.sim.lock();
        let now = s.now_ms;
        let c = s
            .cases
            .iter_mut()
            .find(|c| c.id == id)
            .ok_or_else(|| AdminError::NotFound(format!("no case {id}")))?;
        if let Some(st) = u.status {
            c.status = st;
        }
        if !u.note.trim().is_empty() {
            c.notes.push(CaseNote {
                at_ms: now,
                by: by.into(),
                text: u.note,
            });
        }
        c.updated_at_ms = now;
        Ok(c.clone())
    }
}

fn check_effect(s: &Sim, e: &RuleEffect) -> AdminResult<()> {
    match e {
        RuleEffect::Tier { tier } if !s.policy.policy.tiers.contains_key(tier) => {
            Err(AdminError::BadRequest(format!("no tier {tier:?}")))
        }
        RuleEffect::Throttle { events_per_sec }
            if !(events_per_sec.is_finite() && *events_per_sec >= 0.0) =>
        {
            Err(AdminError::BadRequest(
                "throttle must be ≥ 0 events/s".into(),
            ))
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
        let l = d
            .hosts(HostQuery {
                q: Some("fastvps".into()),
                ..Default::default()
            })
            .await
            .unwrap();
        assert_eq!(l.total, 2);
        let det = d.host("pds-7f3a.fastvps.cloud").await.unwrap();
        assert_eq!(det.row.tier, "probation");

        let p = d.policy().await.unwrap();
        let mut np = p.policy.clone();
        np.spam.reject_ratio = 0.3;
        let u = d
            .update_policy(
                PolicyUpdate {
                    base_version: p.version,
                    policy: np.clone(),
                    note: "t".into(),
                },
                "admin",
            )
            .await
            .unwrap();
        assert_eq!(u.version, p.version + 1);
        let stale = d
            .update_policy(
                PolicyUpdate {
                    base_version: p.version,
                    policy: np,
                    note: String::new(),
                },
                "admin",
            )
            .await;
        assert!(matches!(stale, Err(AdminError::Conflict(_))));
        assert_eq!(
            d.policy_audit().await.unwrap()[0].changes,
            vec!["spam.rejectRatio: 0.2 → 0.3".to_string()]
        );
    }
}
