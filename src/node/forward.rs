//! The host owner's side of an event handed to the leader: its socket's
//! fence, the leader's outcome, and the checks it ships along
//! (`encode_meta`).

use super::{Checked, CheckedKind, Node, Rejection, metrics};
use crate::types::Host;
use crate::verify::{Verified, VerifiedKind};
use bytes::{Buf, BufMut, Bytes};
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{Duration, Instant};
use vlatproto::cid::Cid;
use vlatproto::tid::Tid;

/// What the leader did with an event.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Outcome {
    /// Committed at this relay seq.
    Appended(i64),
    /// Already in the log: nothing appended, done.
    Duplicate,
    /// Dropped by a check. Done too: the host owner acks it.
    Rejected(String),
}

#[derive(Clone, Debug, PartialEq, Eq, thiserror::Error)]
pub enum ForwardError {
    #[error("gave up after {0:?}: {1}")]
    GaveUp(Duration, String),
    /// Not sent: an event from the same host socket gave up first (or the
    /// host is another node's now), and the host replays this one too.
    #[error("fenced: an earlier event of its host socket gave up")]
    Fenced,
}

/// One host socket's events, as far as forwarding goes: live until one of
/// them gives up (or the host owner fences the socket). Every socket of a
/// host shares `below`: sockets older than it are fenced.
#[derive(Clone, Debug)]
pub struct Fence {
    epoch: u64,
    below: Arc<AtomicU64>,
}

impl Fence {
    pub fn new(epoch: u64, below: Arc<AtomicU64>) -> Fence {
        Fence { epoch, below }
    }

    pub fn live(&self) -> bool {
        self.epoch >= self.below.load(Ordering::Acquire)
    }

    /// Fences this socket and every older one. True if it wasn't already.
    pub fn trip(&self) -> bool {
        self.below.fetch_max(self.epoch + 1, Ordering::AcqRel) <= self.epoch
    }
}

/// An event handed to the leader, as its host owner remembers it.
pub(super) struct Sent {
    pub host: Host,
    pub did: String,
    pub useq: i64,
    pub epoch: u64,
    pub kind: &'static str,
    /// When its frame arrived, for the time to firehose.
    pub received: Instant,
}

impl Node {
    /// The host owner's end of a forward: count it and move the host's
    /// cursor, or, if the leader never answered, hold the cursor and have
    /// the host send it again.
    pub(super) async fn forwarded(
        self: Arc<Self>,
        rx: tokio::sync::oneshot::Receiver<Result<Outcome, ForwardError>>,
        s: Sent,
    ) {
        let Sent { host, did, useq, epoch, kind, received } = s;
        match rx.await {
            Ok(Ok(Outcome::Appended(seq))) => {
                metrics::ACCEPTED_BY_KIND.inc(kind);
                self.ttf.durable_batch([(std::slice::from_ref(&seq), received)]);
                {
                    let mut p = self.passed.lock();
                    if p.len() >= metrics::PASSED_KEPT {
                        p.pop_front();
                    }
                    p.push_back(metrics::PassedNote {
                        at_ms: crate::upstream::host::now_ms() as i64,
                        host: host.clone(),
                        did: did.clone(),
                        seq,
                        upstream_seq: useq,
                        kind,
                    });
                }
                if let Some(p) = &self.policy {
                    p.on_accepted(&host.0, &did, kind);
                }
                self.finish(&host, useq, epoch, None);
            }
            Ok(Ok(Outcome::Duplicate)) => {
                metrics::EVENTS_DUPLICATE.with_label_values(&["owner"]).inc();
                self.finish(&host, useq, epoch, None);
            }
            Ok(Ok(Outcome::Rejected(m))) => {
                let (reason, detail) = m.split_once(": ").unwrap_or((m.as_str(), ""));
                let r = Rejection { reason: static_reason(reason), detail: detail.to_string() };
                self.reject(&host, &did, useq, r);
                self.finish(&host, useq, epoch, None);
            }
            Ok(Err(ForwardError::Fenced)) => {
                metrics::EVENTS_FENCED.inc();
                self.acks.fail(&host, useq, epoch);
            }
            Ok(Err(e)) => {
                tracing::warn!(host = %host.0, did, useq, "forward failed, replaying from the host: {e}");
                self.acks.fail(&host, useq, epoch);
                // the forwarder fenced the socket: one replay per socket
                self.manager.kick_epoch(&host, epoch);
            }
            Err(_) => self.acks.fail(&host, useq, epoch),
        }
    }
}

/// Reasons travel as text; metrics labels want the fixed set.
fn static_reason(r: &str) -> &'static str {
    const KNOWN: &[&str] = &[
        "stale",
        "wrong_host",
        "inactive",
        "desynchronized",
        "rev_not_newer",
        "prev_data_mismatch",
        "chain",
        "rate_limited",
        "new_account_deferred",
        "no_identity",
        "bad_cid",
        "not_owner",
        "identity_unavailable",
        "store",
        "bad_meta",
    ];
    KNOWN.iter().find(|k| **k == r).copied().unwrap_or("owner_rejected")
}

/// What the leader needs from the host owner's parse: the kind, the
/// verified chain fields, and whether this was the first copy the host
/// owner saw.
///
/// kind u8 | first u8 | commit/sync: vkind u8, rev u64,
/// commit cid, data cid, prev_data (u8 flags: 1 = a cid follows, 2 = the
/// repo's first commit; + cid) | account: active u8,
/// status (u16 len + bytes, 0xffff = none)
pub(crate) fn encode_meta(c: &Checked) -> Bytes {
    let mut b = Vec::with_capacity(120);
    let tag = match &c.kind {
        CheckedKind::Commit(_) => 0u8,
        CheckedKind::Sync(_) => 1,
        CheckedKind::Identity => 2,
        CheckedKind::Account { .. } => 3,
    };
    b.put_u8(tag);
    b.put_u8(c.first_sighting as u8);
    let cid = |b: &mut Vec<u8>, c: &Cid| {
        b.put_u8(c.codec);
        b.put_slice(&c.digest);
    };
    match &c.kind {
        CheckedKind::Commit(v) | CheckedKind::Sync(v) => {
            b.put_u8(matches!(v.kind, VerifiedKind::Sync) as u8);
            b.put_u64(v.rev.0);
            cid(&mut b, &v.commit);
            cid(&mut b, &v.data);
            let created = (v.created as u8) << 1;
            match &v.prev_data {
                Some(p) => {
                    b.put_u8(1 | created);
                    cid(&mut b, p);
                }
                None => b.put_u8(created),
            }
        }
        CheckedKind::Identity => {}
        CheckedKind::Account { active, status } => {
            b.put_u8(*active as u8);
            match status {
                Some(s) => {
                    let s = &s.as_bytes()[..s.len().min(1024)];
                    b.put_u16(s.len() as u16);
                    b.put_slice(s);
                }
                None => b.put_u16(u16::MAX),
            }
        }
    }
    b.into()
}

pub(crate) struct Meta {
    pub(crate) kind: CheckedKind,
    pub(crate) first_sighting: bool,
}

pub(crate) fn decode_meta(did: &str, mut r: Bytes) -> anyhow::Result<Meta> {
    anyhow::ensure!(r.remaining() >= 2, "short meta");
    let tag = r.get_u8();
    let first_sighting = r.get_u8() != 0;
    let cid = |r: &mut Bytes| -> anyhow::Result<Cid> {
        anyhow::ensure!(r.remaining() >= 33, "short cid");
        let codec = r.get_u8();
        let mut digest = [0u8; 32];
        r.copy_to_slice(&mut digest);
        Ok(Cid { codec, digest })
    };
    let kind = match tag {
        0 | 1 => {
            anyhow::ensure!(r.remaining() >= 9, "short verified");
            let vkind = if r.get_u8() == 1 { VerifiedKind::Sync } else { VerifiedKind::Commit };
            let rev = Tid(r.get_u64());
            let commit = cid(&mut r)?;
            let data = cid(&mut r)?;
            anyhow::ensure!(r.remaining() >= 1, "short verified");
            let flags = r.get_u8();
            let prev_data = if flags & 1 != 0 { Some(cid(&mut r)?) } else { None };
            let created = flags & 2 != 0;
            let v = Verified { kind: vkind, did: did.to_string(), rev, commit, data, prev_data, created };
            if tag == 0 { CheckedKind::Commit(v) } else { CheckedKind::Sync(v) }
        }
        2 => CheckedKind::Identity,
        3 => {
            anyhow::ensure!(r.remaining() >= 3, "short account");
            let active = r.get_u8() != 0;
            let n = r.get_u16();
            let status = if n == u16::MAX {
                None
            } else {
                anyhow::ensure!(r.remaining() >= n as usize, "short status");
                Some(String::from_utf8(r.split_to(n as usize).to_vec())?)
            };
            CheckedKind::Account { active, status }
        }
        t => anyhow::bail!("unknown kind {t}"),
    };
    Ok(Meta { kind, first_sighting })
}
