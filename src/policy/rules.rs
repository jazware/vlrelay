//! Domain rules (`policy/domain-rules.json`): ban, allow, force a tier or
//! throttle every host under a hostname suffix, so a spammer spinning up
//! hosts on one domain is handled as a group.
//!
//! `example.com` matches that host only. `*.example.com` matches the domain
//! and every subdomain. When several rules match, the most specific one wins
//! (the longest matching name, and an exact rule before a wildcard of the
//! same name), so one host can be allowed under a banned domain.

use crate::state::Tier;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "kebab-case")]
pub enum RuleEffect {
    /// Never connected, requestCrawl refused.
    Ban,
    /// Admitted even when requestCrawl is allow-list only, and not counted
    /// against the new-hosts-per-day budget.
    Allow,
    /// The host runs at this tier whatever its record says (unless it's
    /// suspended, banned or throttled).
    Tier { tier: Tier },
    /// Caps the host's events/s below its tier.
    Throttle {
        #[serde(rename = "eventsPerSec")]
        events_per_sec: f64,
    },
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct Rule {
    pub id: u64,
    pub pattern: String,
    pub effect: RuleEffect,
    #[serde(default)]
    pub note: String,
    pub created_at_ms: i64,
    pub created_by: String,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize, Default)]
#[serde(rename_all = "camelCase")]
pub struct RuleSet {
    /// Ids are never reused, so an audit line keeps pointing at one rule.
    pub next_id: u64,
    pub rules: Vec<Rule>,
}

pub fn validate(rs: &RuleSet) -> Result<(), Vec<String>> {
    let mut errs = Vec::new();
    let mut seen = HashMap::new();
    for r in &rs.rules {
        match normalize_pattern(&r.pattern) {
            Ok(p) if p == r.pattern => {}
            Ok(p) => errs.push(format!("rule {}: pattern should be {p:?}", r.id)),
            Err(e) => errs.push(format!("rule {}: {e}", r.id)),
        }
        if let Some(other) = seen.insert(r.pattern.clone(), r.id) {
            errs.push(format!("rules {other} and {} have the same pattern", r.id));
        }
        if r.id >= rs.next_id {
            errs.push(format!("rule {}: id isn't below nextId", r.id));
        }
        match &r.effect {
            RuleEffect::Tier { tier } if matches!(tier, Tier::Suspended | Tier::Banned) => errs
                .push(format!(
                    "rule {}: use a ban rule instead of tier {tier:?}",
                    r.id
                )),
            RuleEffect::Throttle { events_per_sec }
                if !(events_per_sec.is_finite() && *events_per_sec >= 0.0) =>
            {
                errs.push(format!("rule {}: throttle must be ≥ 0 events/s", r.id))
            }
            _ => {}
        }
    }
    if errs.is_empty() { Ok(()) } else { Err(errs) }
}

fn valid_labels(name: &str) -> bool {
    let labels: Vec<&str> = name.split('.').collect();
    name.len() <= 253
        && labels.len() >= 2
        && labels.iter().all(|l| {
            !l.is_empty()
                && l.len() <= 63
                && !l.starts_with('-')
                && !l.ends_with('-')
                && l.bytes().all(|b| b.is_ascii_alphanumeric() || b == b'-')
        })
        // a numeric TLD means an IP address, which handle syntax refuses
        && !labels.last().is_some_and(|t| t.bytes().all(|b| b.is_ascii_digit()))
}

/// `Example.COM.` → `example.com`, `*.Example.com` → `*.example.com`. An
/// exact pattern may also name one host by IPv4 address or `localhost`,
/// with a port, which is how hosts on a dev network (or a PDS on a port)
/// are known.
pub fn normalize_pattern(p: &str) -> Result<String, String> {
    let p = p.trim().trim_end_matches('.').to_ascii_lowercase();
    let bad = || format!("{p:?} isn't a hostname or *.domain pattern");
    if let Some(base) = p.strip_prefix("*.") {
        return if valid_labels(base) { Ok(p.clone()) } else { Err(bad()) };
    }
    let name = match p.rsplit_once(':') {
        Some((n, port)) if port.parse::<u16>().is_ok_and(|x| x > 0) => n,
        Some(_) => return Err(bad()),
        None => p.as_str(),
    };
    let ok = valid_labels(name) || name == "localhost" || name.parse::<std::net::Ipv4Addr>().is_ok();
    if !ok {
        return Err(bad());
    }
    Ok(p)
}

#[derive(Debug, PartialEq, Eq)]
pub enum HostnameError {
    Syntax(String),
    Scheme(String),
    Insecure,
    Port,
}

impl std::fmt::Display for HostnameError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            HostnameError::Syntax(h) => write!(f, "{h:?} isn't a valid hostname"),
            HostnameError::Scheme(s) => write!(f, "scheme {s:?} isn't https or wss"),
            HostnameError::Insecure => write!(f, "plain http/ws hosts aren't accepted"),
            HostnameError::Port => write!(f, "a port is only allowed on localhost"),
        }
    }
}

pub struct ParsedHost {
    pub hostname: String,
    /// http or ws.
    pub insecure: bool,
}

/// What requestCrawl takes (indigo's `ParseHostname`, `host.go:L143-L190`):
/// an optional https/wss (or http/ws) scheme, any path or query ignored, a
/// port only on localhost, handle syntax, lower-cased.
pub fn parse_hostname(input: &str) -> Result<ParsedHost, HostnameError> {
    let s = input.trim();
    let (insecure, rest) = match s.split_once("://") {
        None => (false, s),
        Some((scheme, rest)) => match scheme.to_ascii_lowercase().as_str() {
            "https" | "wss" => (false, rest),
            "http" | "ws" => (true, rest),
            other => return Err(HostnameError::Scheme(other.to_string())),
        },
    };
    let authority = rest
        .split(['/', '?', '#'])
        .next()
        .unwrap_or_default()
        .to_ascii_lowercase();
    let (name, port) = match authority.rsplit_once(':') {
        Some((n, p)) if !p.is_empty() && p.bytes().all(|b| b.is_ascii_digit()) => (n, Some(p)),
        Some(_) => return Err(HostnameError::Syntax(authority.clone())),
        None => (authority.as_str(), None),
    };
    let name = name.trim_end_matches('.');
    if name == "localhost" {
        let hostname = match port {
            Some(p) => format!("localhost:{p}"),
            None => "localhost".into(),
        };
        return Ok(ParsedHost { hostname, insecure });
    }
    if port.is_some() {
        return Err(HostnameError::Port);
    }
    if !valid_labels(name) {
        return Err(HostnameError::Syntax(name.to_string()));
    }
    Ok(ParsedHost {
        hostname: name.to_string(),
        insecure,
    })
}

/// Rules indexed by name, so a lookup costs one hash probe per label of the
/// hostname however many rules there are.
#[derive(Debug, Default)]
pub struct Compiled {
    pub set: RuleSet,
    exact: HashMap<String, usize>,
    wildcard: HashMap<String, usize>,
}

impl Compiled {
    pub fn new(set: RuleSet) -> Compiled {
        let mut c = Compiled {
            exact: HashMap::new(),
            wildcard: HashMap::new(),
            set,
        };
        for (i, r) in c.set.rules.iter().enumerate() {
            match r.pattern.strip_prefix("*.") {
                Some(base) => c.wildcard.insert(base.to_string(), i),
                None => c.exact.insert(r.pattern.clone(), i),
            };
        }
        c
    }

    /// The most specific rule for a normalized hostname.
    pub fn lookup(&self, host: &str) -> Option<&Rule> {
        if let Some(&i) = self.exact.get(host) {
            return Some(&self.set.rules[i]);
        }
        let host = host.split(':').next().unwrap_or(host);
        if let Some(&i) = self.exact.get(host) {
            return Some(&self.set.rules[i]);
        }
        let mut name = host;
        loop {
            if let Some(&i) = self.wildcard.get(name) {
                return Some(&self.set.rules[i]);
            }
            match name.split_once('.') {
                Some((_, parent)) if parent.contains('.') => name = parent,
                _ => return None,
            }
        }
    }
}

pub fn pattern_matches(pattern: &str, host: &str) -> bool {
    match pattern.strip_prefix("*.") {
        Some(base) => host == base || host.strip_suffix(base).is_some_and(|p| p.ends_with('.')),
        None => host == pattern,
    }
}
