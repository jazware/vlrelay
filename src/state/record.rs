//! The per-DID record and its key, kept compact: at 56M DIDs every byte is
//! ~56 MB of keys in the bucket and in the caches.
//!
//! Keys are slot-major like vlpds's (`0x01 ‖ slot ‖ family ‖ rest`), so
//! `listRepos` pages are one range scan. The quorum state holds them beside
//! its own keys (`_applied`, `c/` cursors, `h/` the host table).
//!
//! ```text
//! 0x01 slot 'd' 'p' <15 bytes>     did:plc (the 24 base32 chars, decoded)
//! 0x01 slot 'd' 'w' <utf-8>        any other DID, minus its "did:" prefix
//! ```

use bytes::{BufMut, Bytes};
use sha2::{Digest, Sha256};
use vlpds::cid::{CODEC_DAG_CBOR, Cid};
use vlpds::state::{SLOT_PREFIX_LEN, slot_prefix};
use vlpds::tid::Tid;

pub const DID_FAMILY: u8 = b'd';
const DID_PLC: u8 = b'p';
const DID_OTHER: u8 = b'w';
const PLC_PREFIX: &str = "did:plc:";
const B32: &[u8; 32] = b"abcdefghijklmnopqrstuvwxyz234567";

pub fn did_key(did: &str) -> Vec<u8> {
    did_key_in(vlpds::slots::slot_of(did), did)
}

pub fn did_key_in(slot: u16, did: &str) -> Vec<u8> {
    let mut k = Vec::with_capacity(SLOT_PREFIX_LEN + 2 + 32);
    k.extend_from_slice(&slot_prefix(slot));
    k.push(DID_FAMILY);
    match plc_bytes(did) {
        Some(b) => {
            k.push(DID_PLC);
            k.extend_from_slice(&b);
        }
        None => {
            k.push(DID_OTHER);
            k.extend_from_slice(did.strip_prefix("did:").unwrap_or(did).as_bytes());
        }
    }
    k
}

/// The DID a [`did_key`] names, or None for any other key.
pub fn did_from_key(key: &[u8]) -> Option<String> {
    let body = key.get(SLOT_PREFIX_LEN..)?;
    let (&fam, rest) = body.split_first()?;
    if fam != DID_FAMILY {
        return None;
    }
    let (&tag, rest) = rest.split_first()?;
    match tag {
        DID_PLC if rest.len() == 15 => {
            let mut s = String::with_capacity(32);
            s.push_str(PLC_PREFIX);
            let mut acc: u128 = 0;
            for &b in rest {
                acc = (acc << 8) | b as u128;
            }
            for i in (0..24).rev() {
                s.push(B32[((acc >> (i * 5)) & 31) as usize] as char);
            }
            Some(s)
        }
        DID_OTHER => Some(format!("did:{}", std::str::from_utf8(rest).ok()?)),
        _ => None,
    }
}

pub(crate) fn plc_bytes(did: &str) -> Option<[u8; 15]> {
    let id = did.strip_prefix(PLC_PREFIX)?.as_bytes();
    if id.len() != 24 {
        return None;
    }
    let mut acc: u128 = 0;
    for &c in id {
        let d = match c {
            b'a'..=b'z' => c - b'a',
            b'2'..=b'7' => c - b'2' + 26,
            _ => return None,
        };
        acc = (acc << 5) | d as u128;
    }
    let mut out = [0u8; 15];
    for (i, o) in out.iter_mut().enumerate() {
        *o = (acc >> ((14 - i) * 8)) as u8;
    }
    Some(out)
}

/// The first key of `slot`'s DID records.
pub fn did_family_start(slot: u16) -> Vec<u8> {
    let mut k = slot_prefix(slot).to_vec();
    k.push(DID_FAMILY);
    k
}

/// A host by the first 8 bytes of sha256(hostname): fixed width in every
/// record, and nothing to allocate or coordinate. Names come back from the
/// events that carried them.
///
/// Host authority compares these, so a hostname whose key equals a real
/// PDS's would pass as it. 64 bits holds: matching one given host is a
/// second preimage, ~2^64 hashes, and the ~2^32 birthday collision only
/// pairs two names the attacker chose, which gains nothing. Widen it if
/// that margin stops being enough (the record format may change freely
/// until vlRelay ships).
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct HostKey(pub u64);

impl HostKey {
    pub fn of(hostname: &str) -> HostKey {
        let h = Sha256::digest(hostname.as_bytes());
        HostKey(u64::from_be_bytes(h[..8].try_into().expect("8 bytes")))
    }
}

/// A repo head: what the next commit's `prevData` and `rev` are checked
/// against.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct ChainState {
    pub rev: Tid,
    pub commit: Cid,
    pub data: Cid,
}

/// What the account's host last said about it (`#account`).
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
#[repr(u8)]
pub enum Upstream {
    Active = 0,
    Takendown = 1,
    Suspended = 2,
    Deleted = 3,
    Deactivated = 4,
    Desynchronized = 5,
    Throttled = 6,
    /// Inactive with a status we don't know, or none: the lexicon says to
    /// make no claim why.
    Inactive = 7,
}

impl Upstream {
    pub fn from_event(active: bool, status: Option<&str>) -> Upstream {
        if active {
            return Upstream::Active;
        }
        match status {
            Some("takendown") => Upstream::Takendown,
            Some("suspended") => Upstream::Suspended,
            Some("deleted") => Upstream::Deleted,
            Some("deactivated") => Upstream::Deactivated,
            Some("desynchronized") => Upstream::Desynchronized,
            Some("throttled") => Upstream::Throttled,
            _ => Upstream::Inactive,
        }
    }

    fn from_u8(b: u8) -> Option<Upstream> {
        use Upstream::*;
        Some(match b {
            0 => Active,
            1 => Takendown,
            2 => Suspended,
            3 => Deleted,
            4 => Deactivated,
            5 => Desynchronized,
            6 => Throttled,
            7 => Inactive,
            _ => return None,
        })
    }
}

/// Why the relay marked an account desynchronized.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
#[repr(u8)]
pub enum DesyncReason {
    PrevDataMismatch = 1,
    RevNotNewer = 2,
    Chain = 3,
}

impl DesyncReason {
    fn from_u8(b: u8) -> Option<DesyncReason> {
        Some(match b {
            1 => DesyncReason::PrevDataMismatch,
            2 => DesyncReason::RevNotNewer,
            3 => DesyncReason::Chain,
            _ => return None,
        })
    }
}

/// The status the sync endpoints and the firehose report.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum AccountStatus {
    Active,
    Takendown,
    Suspended,
    Deleted,
    Deactivated,
    Desynchronized,
    Throttled,
    /// Inactive, no claim why.
    Inactive,
}

impl AccountStatus {
    pub fn is_active(self) -> bool {
        self == AccountStatus::Active
    }

    /// The lexicon's `status` string, None when active or unclaimed.
    pub fn as_str(self) -> Option<&'static str> {
        use AccountStatus::*;
        match self {
            Active | Inactive => None,
            Takendown => Some("takendown"),
            Suspended => Some("suspended"),
            Deleted => Some("deleted"),
            Deactivated => Some("deactivated"),
            Desynchronized => Some("desynchronized"),
            Throttled => Some("throttled"),
        }
    }
}

/// A signing key as multicodec bytes (varint key type ‖ compressed point),
/// the decoded form of the DID document's `publicKeyMultibase`.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct SigningKey(pub Bytes);

pub const MAX_KEY_LEN: usize = 96;

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Record {
    /// The host whose events were last accepted for this DID.
    pub host: HostKey,
    /// The PDS its DID document named at `fetched_at`.
    pub pds: Option<HostKey>,
    pub chain: Option<ChainState>,
    pub upstream: Upstream,
    /// An operator takedown on this relay; outlives anything upstream says.
    pub relay_takedown: bool,
    /// Created past its host's account cap or the cluster's new-account
    /// budget (indigo's `host-throttled`): its commits are dropped until an
    /// operator lifts it. An upstream `#account` doesn't clear it.
    pub relay_throttled: bool,
    pub desync: Option<DesyncReason>,
    pub key: Option<SigningKey>,
    /// Unix seconds; 0 means stale (re-resolve before trusting it).
    pub fetched_at: u32,
    pub created_at: u32,
    /// Policy counters: commits in `minute` (unix minutes), and failed
    /// stateful checks over the account's life.
    pub minute: u32,
    pub minute_commits: u32,
    pub failed_checks: u32,
}

impl Record {
    pub fn new(host: HostKey, now: u32) -> Record {
        Record {
            host,
            pds: None,
            chain: None,
            upstream: Upstream::Active,
            relay_takedown: false,
            relay_throttled: false,
            desync: None,
            key: None,
            fetched_at: 0,
            created_at: now,
            minute: 0,
            minute_commits: 0,
            failed_checks: 0,
        }
    }

    pub fn status(&self) -> AccountStatus {
        if self.relay_takedown {
            return AccountStatus::Takendown;
        }
        if self.relay_throttled {
            return AccountStatus::Throttled;
        }
        match self.upstream {
            Upstream::Active => {}
            Upstream::Takendown => return AccountStatus::Takendown,
            Upstream::Suspended => return AccountStatus::Suspended,
            Upstream::Deleted => return AccountStatus::Deleted,
            Upstream::Deactivated => return AccountStatus::Deactivated,
            Upstream::Desynchronized => return AccountStatus::Desynchronized,
            Upstream::Throttled => return AccountStatus::Throttled,
            Upstream::Inactive => return AccountStatus::Inactive,
        }
        if self.desync.is_some() { AccountStatus::Desynchronized } else { AccountStatus::Active }
    }

    /// Commits from takendown, suspended, deleted or deactivated accounts are
    /// dropped. Desynchronized and throttled accounts still exist upstream.
    pub fn drops_commits(&self) -> bool {
        self.relay_takedown
            || self.relay_throttled
            || matches!(
                self.upstream,
                Upstream::Takendown
                    | Upstream::Suspended
                    | Upstream::Deleted
                    | Upstream::Deactivated
                    | Upstream::Inactive
            )
    }

    pub fn encode(&self) -> Bytes {
        let mut b = Vec::with_capacity(160);
        let mut flags = 0u8;
        if self.chain.is_some() {
            flags |= F_CHAIN;
        }
        match self.pds {
            Some(p) if p == self.host => flags |= F_PDS_IS_HOST,
            Some(_) => flags |= F_PDS,
            None => {}
        }
        if self.key.is_some() {
            flags |= F_KEY;
        }
        if self.relay_throttled {
            flags |= F_THROTTLED;
        }
        if self.relay_takedown {
            flags |= F_TAKEDOWN;
        }
        b.put_u8(VERSION);
        b.put_u8(flags);
        b.put_u8(self.upstream as u8);
        b.put_u8(self.desync.map_or(0, |d| d as u8));
        b.put_u64(self.host.0);
        if flags & F_PDS != 0 {
            b.put_u64(self.pds.expect("flagged").0);
        }
        if let Some(c) = &self.chain {
            b.put_u64(c.rev.0);
            b.put_slice(&c.commit.digest);
            b.put_slice(&c.data.digest);
        }
        if let Some(k) = &self.key {
            b.put_u8(k.0.len() as u8);
            b.put_slice(&k.0);
        }
        for v in [self.fetched_at, self.created_at, self.minute, self.minute_commits, self.failed_checks] {
            put_varint(&mut b, v as u64);
        }
        Bytes::from(b)
    }

    pub fn decode(b: &[u8]) -> Result<Record, DecodeError> {
        let mut r = Reader(b);
        if r.u8()? != VERSION {
            return Err(DecodeError);
        }
        let flags = r.u8()?;
        let upstream = Upstream::from_u8(r.u8()?).ok_or(DecodeError)?;
        let desync = match r.u8()? {
            0 => None,
            d => Some(DesyncReason::from_u8(d).ok_or(DecodeError)?),
        };
        let host = HostKey(r.u64()?);
        let pds = if flags & F_PDS != 0 {
            Some(HostKey(r.u64()?))
        } else if flags & F_PDS_IS_HOST != 0 {
            Some(host)
        } else {
            None
        };
        let chain = if flags & F_CHAIN != 0 {
            let rev = Tid(r.u64()?);
            let commit = Cid { codec: CODEC_DAG_CBOR, digest: r.array()? };
            let data = Cid { codec: CODEC_DAG_CBOR, digest: r.array()? };
            Some(ChainState { rev, commit, data })
        } else {
            None
        };
        let key = if flags & F_KEY != 0 {
            let n = r.u8()? as usize;
            Some(SigningKey(Bytes::copy_from_slice(r.take(n)?)))
        } else {
            None
        };
        let mut v = [0u32; 5];
        for x in &mut v {
            *x = r.varint()? as u32;
        }
        Ok(Record {
            host,
            pds,
            chain,
            upstream,
            relay_takedown: flags & F_TAKEDOWN != 0,
            relay_throttled: flags & F_THROTTLED != 0,
            desync,
            key,
            fetched_at: v[0],
            created_at: v[1],
            minute: v[2],
            minute_commits: v[3],
            failed_checks: v[4],
        })
    }
}

const VERSION: u8 = 1;
const F_CHAIN: u8 = 1;
const F_PDS: u8 = 2;
const F_PDS_IS_HOST: u8 = 4;
const F_KEY: u8 = 8;
const F_TAKEDOWN: u8 = 16;
const F_THROTTLED: u8 = 32;

#[derive(Debug, Clone, Copy, PartialEq, Eq, thiserror::Error)]
#[error("corrupt state record")]
pub struct DecodeError;

pub(crate) fn put_varint(b: &mut Vec<u8>, mut v: u64) {
    while v >= 0x80 {
        b.push((v as u8) | 0x80);
        v >>= 7;
    }
    b.push(v as u8);
}

pub(crate) struct Reader<'a>(pub &'a [u8]);

impl<'a> Reader<'a> {
    pub fn take(&mut self, n: usize) -> Result<&'a [u8], DecodeError> {
        if self.0.len() < n {
            return Err(DecodeError);
        }
        let (h, t) = self.0.split_at(n);
        self.0 = t;
        Ok(h)
    }
    pub fn u8(&mut self) -> Result<u8, DecodeError> {
        Ok(self.take(1)?[0])
    }
    pub fn u64(&mut self) -> Result<u64, DecodeError> {
        Ok(u64::from_be_bytes(self.array()?))
    }
    pub fn array<const N: usize>(&mut self) -> Result<[u8; N], DecodeError> {
        Ok(self.take(N)?.try_into().expect("N bytes"))
    }
    pub fn varint(&mut self) -> Result<u64, DecodeError> {
        let mut v = 0u64;
        for shift in (0..64).step_by(7) {
            let b = self.u8()?;
            v |= ((b & 0x7f) as u64) << shift;
            if b & 0x80 == 0 {
                return Ok(v);
            }
        }
        Err(DecodeError)
    }
}
