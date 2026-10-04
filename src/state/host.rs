//! Host records: one row per upstream host, in the DID shard that owns the
//! hostname's slot. Host shards and DID shards share one layout and one hash,
//! so the node that subscribes to a host also owns its record.

use serde::{Deserialize, Serialize};

/// Policy tier (design doc, "Policy is data").
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize, Default)]
#[serde(rename_all = "lowercase")]
pub enum Tier {
    Trusted,
    #[default]
    Default,
    New,
    Throttled,
    Suspended,
    Banned,
}

/// The subscription's own state, as the upstream workstream last saw it.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize, Default)]
#[serde(rename_all = "lowercase")]
pub enum Conn {
    #[default]
    Active,
    Idle,
    Offline,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct HostRecord {
    pub hostname: String,
    #[serde(default)]
    pub tier: Tier,
    #[serde(default)]
    pub conn: Conn,
    /// Upstream seq whose events (and every earlier one) are durable here:
    /// where a new host owner reconnects from.
    #[serde(default)]
    pub cursor: i64,
    #[serde(default)]
    pub account_count: i64,
    #[serde(default)]
    pub first_seen: u32,
    #[serde(default)]
    pub events: u64,
    #[serde(default)]
    pub failed_checks: u64,
    #[serde(default)]
    pub dropped: u64,
    /// Fields of a newer version, kept across our rewrites.
    #[serde(default, flatten)]
    pub extra: serde_json::Map<String, serde_json::Value>,
}

impl HostRecord {
    pub fn new(hostname: &str, tier: Tier, now: u32) -> HostRecord {
        HostRecord {
            hostname: hostname.to_string(),
            tier,
            conn: Conn::Active,
            cursor: 0,
            account_count: 0,
            first_seen: now,
            events: 0,
            failed_checks: 0,
            dropped: 0,
            extra: Default::default(),
        }
    }

    /// `com.atproto.sync.defs#hostStatus`.
    pub fn lexicon_status(&self) -> &'static str {
        match (self.tier, self.conn) {
            (Tier::Banned, _) => "banned",
            (Tier::Throttled, _) => "throttled",
            (Tier::Suspended, _) => "offline",
            (_, Conn::Active) => "active",
            (_, Conn::Idle) => "idle",
            (_, Conn::Offline) => "offline",
        }
    }
}

/// Additive counters, batched by the DID owners and the host owner.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct HostCounts {
    pub accounts: i64,
    pub events: u64,
    pub failed_checks: u64,
    pub dropped: u64,
}

impl HostCounts {
    pub fn is_zero(&self) -> bool {
        *self == HostCounts::default()
    }
    pub fn add(&mut self, o: &HostCounts) {
        self.accounts += o.accounts;
        self.events += o.events;
        self.failed_checks += o.failed_checks;
        self.dropped += o.dropped;
    }
}

pub struct HostPage {
    pub hosts: Vec<HostRecord>,
    pub cursor: Option<String>,
}

/// What the upstream registry reads and writes. `StateStore` implements it
/// over the shards this node owns; a cluster wrapper routes the rest.
#[async_trait::async_trait]
pub trait HostStore: Send + Sync {
    async fn get_host(&self, hostname: &str) -> anyhow::Result<Option<HostRecord>>;
    /// Creates or replaces the record. Not flushed: tier and status changes
    /// are cheap to redo, cursors go through `checkpoint_cursors`.
    async fn put_host(&self, rec: &HostRecord) -> anyhow::Result<()>;
    /// Durable on return (one memtable flush per shard touched). Never moves
    /// a cursor backwards, and creates no record.
    async fn checkpoint_cursors(&self, cursors: &[(String, i64)]) -> anyhow::Result<()>;
    async fn add_counts(&self, counts: &[(String, HostCounts)]) -> anyhow::Result<()>;
    /// Ordered by (slot, hostname); the cursor is the last hostname.
    async fn list_hosts(&self, cursor: Option<&str>, limit: usize) -> anyhow::Result<HostPage>;
}
