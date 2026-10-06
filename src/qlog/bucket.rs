//! The bucket, one counted client per purpose, so the request bill can be
//! checked against docs/quorum.md §2 by what sent each request.
//!
//! Every request goes through vlpds's `Store::counted`, which counts it in
//! `vlpds_object_store_requests_total` by op, key component (the path:
//! `log_segment`, `qlog_manifest`, `qlog_leader`, `state_*`, ...) and
//! client. The client is `qlog_{purpose}`:
//!
//! - `flush`: the leader's fence, segments, manifest CAS and checkpoint
//!   deletes;
//! - `state`: the leader's SlateDB (memtable uploads, checkpoints, its
//!   manifest polls, compactor and GC);
//! - `leader`: `qlog/leader` reads and CASes (takeovers, membership);
//! - `recovery`: a bucket recovery (manifest, orphans, the state's clone,
//!   salvaged segments);
//! - `backfill`: segments read for a follower behind the leader's disk, a
//!   recovery's catch-up emit, and consumers' old cursors;
//! - `retain`: the retention report and its deletes;
//! - `tool`: `qlog verify` and `qlog check`.
//!
//! Classes are R2's: Class A is every write and LIST (and S3's DeleteObjects
//! POST), Class B every GET and HEAD; a single DELETE and an aborted
//! multipart upload are free.

use serde::Serialize;
use std::collections::BTreeMap;
use vlpds::store::Store;

pub const PURPOSES: [&str; 8] = ["flush", "state", "leader", "recovery", "backfill", "retain", "plc", "tool"];

const CLIENT_PREFIX: &str = "qlog_";

fn client(purpose: &str) -> &'static str {
    match purpose {
        "flush" => "qlog_flush",
        "state" => "qlog_state",
        "leader" => "qlog_leader",
        "recovery" => "qlog_recovery",
        "backfill" => "qlog_backfill",
        "retain" => "qlog_retain",
        "plc" => "qlog_plc",
        "tool" => "qlog_tool",
        p => panic!("qlog bucket: no purpose {p}"),
    }
}

/// `base` must not be counted already, or every request counts twice.
pub fn counted(base: &Store, purpose: &str) -> Store {
    base.clone().counted(client(purpose))
}

#[derive(Clone)]
pub struct Bucket {
    pub flush: Store,
    pub state: Store,
    pub leader: Store,
    pub recovery: Store,
    pub backfill: Store,
    pub retain: Store,
}

impl Bucket {
    pub fn new(base: Store) -> Bucket {
        Bucket {
            flush: counted(&base, "flush"),
            state: counted(&base, "state"),
            leader: counted(&base, "leader"),
            recovery: counted(&base, "recovery"),
            backfill: counted(&base, "backfill"),
            retain: counted(&base, "retain"),
        }
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Class {
    A,
    B,
    Free,
}

pub fn class(op: &str) -> Class {
    match op {
        "get" | "get_range" | "head" => Class::B,
        "delete" | "mpu_abort" => Class::Free,
        _ => Class::A,
    }
}

#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, serde::Deserialize)]
pub struct Counts {
    pub a: u64,
    pub b: u64,
    pub free: u64,
}

impl Counts {
    fn add(&mut self, op: &str, n: u64) {
        match class(op) {
            Class::A => self.a += n,
            Class::B => self.b += n,
            Class::Free => self.free += n,
        }
    }
}

/// Requests this process has sent through the quorum log's clients, every
/// result counted (a failed or cancelled request is billed too).
#[derive(Clone, Debug, Default, Serialize, serde::Deserialize)]
pub struct Requests {
    /// Tells a restarted process's counts (from zero again) from these.
    #[serde(default)]
    pub pid: u32,
    pub total: Counts,
    pub by_purpose: BTreeMap<String, Counts>,
    /// `purpose/component`.
    pub by_component: BTreeMap<String, Counts>,
    /// `purpose/op`.
    pub by_op: BTreeMap<String, u64>,
}

pub fn requests() -> Requests {
    use prometheus::core::Collector;
    let mut r = Requests { pid: std::process::id(), ..Default::default() };
    for mf in vlpds::metrics::OBJ_REQUESTS.collect() {
        for m in mf.get_metric() {
            let label = |k: &str| m.get_label().iter().find(|l| l.name() == k).map(|l| l.value().to_string());
            let (Some(op), Some(comp), Some(cl)) = (label("op"), label("component"), label("client")) else {
                continue;
            };
            let Some(purpose) = cl.strip_prefix(CLIENT_PREFIX) else { continue };
            let n = m.get_counter().get_value() as u64;
            if n == 0 {
                continue;
            }
            r.total.add(&op, n);
            r.by_purpose.entry(purpose.to_string()).or_default().add(&op, n);
            r.by_component.entry(format!("{purpose}/{comp}")).or_default().add(&op, n);
            *r.by_op.entry(format!("{purpose}/{op}")).or_default() += n;
        }
    }
    r
}

#[cfg(test)]
mod tests {
    use super::*;
    use object_store::path::Path;
    use object_store::{ObjectStoreExt, PutPayload};

    #[tokio::test]
    async fn counts_by_purpose_class_and_component() {
        // its own client: other tests' nodes send through the real purposes
        // concurrently, into the same process-wide counters
        let s = Store::memory(None).counted("qlog_test");
        let p = |k: &str| Path::from(format!("{}/{k}", s.prefix));
        s.raw.put(&p("qlog/leader"), PutPayload::from_static(b"{}")).await.unwrap();
        s.raw.get(&p("qlog/leader")).await.unwrap();
        s.raw.put(&p("qlog/manifest"), PutPayload::from_static(b"{}")).await.unwrap();
        s.raw.put(&p("log/qlog/000000000000.seg"), PutPayload::from_static(b"x")).await.unwrap();
        s.raw.head(&p("log/qlog/000000000000.seg")).await.unwrap();
        s.raw.delete(&p("log/qlog/000000000000.seg")).await.unwrap();
        let r = requests();
        let c = |k: &str| r.by_component.get(k).cloned().unwrap_or_default();
        assert_eq!(c("test/qlog_leader"), Counts { a: 1, b: 1, free: 0 });
        assert_eq!(c("test/qlog_manifest"), Counts { a: 1, b: 0, free: 0 });
        // object_store sends even a single delete as a DeleteObjects POST,
        // counted as a `delete_batch` (Class A) plus the free `delete`
        assert_eq!(c("test/log_segment"), Counts { a: 2, b: 1, free: 1 });
        assert_eq!(r.by_purpose["test"], Counts { a: 4, b: 2, free: 1 });
        assert_eq!(r.by_op["test/put"], 3);
    }
}
