//! `com.atproto.sync.subscribeRepos` frames: a DAG-CBOR header (`op`, `t`)
//! followed by a DAG-CBOR body.
//!
//! Two parses. [`route`] walks the frame without decoding anything it doesn't
//! need (the CAR is skipped by length) and returns the fields a relay routes
//! on, plus where the body's `seq` sits so [`encode_with_seq`] can splice the
//! relay's own seq in. [`parse`] is the strict one: the whole frame must be
//! canonical DAG-CBOR, and every field the checks in `verify` use is decoded
//! and syntax-checked. Blocks stay slices of the frame's `Bytes` (no copies).

use crate::verify::Reject;
use bytes::{BufMut, Bytes, BytesMut};
use vlatproto::cbor::ValueRef;
use vlatproto::cid::Cid;
use vlatproto::syntax;
use vlatproto::tid::Tid;

/// Above the largest legal frame (a #commit with a 2 MB CAR plus 200 ops) with
/// room for the envelope. Upstream sockets should use the same limit.
pub const MAX_FRAME_BYTES: usize = 5 << 20;
/// `subscribeRepos#commit.blocks` maxLength.
pub const MAX_COMMIT_BLOCKS_BYTES: usize = 2_000_000;
/// `subscribeRepos#commit.ops` maxLength.
pub const MAX_COMMIT_OPS: usize = 200;
/// `subscribeRepos#sync.blocks` maxLength.
pub const MAX_SYNC_BLOCKS_BYTES: usize = 10_000;
/// The spec has no block-count limit. A full proof for 200 ops needs at most a
/// record and a ~10-node path (plus neighbours) per op; this leaves a wide
/// margin while capping how many tiny blocks a 2 MB CAR can make us hash.
pub const MAX_COMMIT_BLOCKS: usize = 8192;

#[derive(Clone, Debug)]
pub struct Limits {
    pub max_frame_bytes: usize,
    pub max_commit_blocks_bytes: usize,
    pub max_commit_ops: usize,
    pub max_commit_blocks: usize,
    pub max_sync_blocks_bytes: usize,
}

impl Default for Limits {
    fn default() -> Self {
        Limits {
            max_frame_bytes: MAX_FRAME_BYTES,
            max_commit_blocks_bytes: MAX_COMMIT_BLOCKS_BYTES,
            max_commit_ops: MAX_COMMIT_OPS,
            max_commit_blocks: MAX_COMMIT_BLOCKS,
            max_sync_blocks_bytes: MAX_SYNC_BLOCKS_BYTES,
        }
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub enum Kind {
    Commit,
    Sync,
    Identity,
    Account,
    Info,
    /// `op: -1`.
    Error,
    /// Deprecated (`#handle`, `#migrate`, `#tombstone`) or future types.
    /// Consumers ignore them, so the relay drops them.
    Unknown,
}

impl Kind {
    fn from_tag(op: i64, t: Option<&str>) -> Kind {
        if op == -1 {
            return Kind::Error;
        }
        match t {
            Some("#commit") => Kind::Commit,
            Some("#sync") => Kind::Sync,
            Some("#identity") => Kind::Identity,
            Some("#account") => Kind::Account,
            Some("#info") => Kind::Info,
            _ => Kind::Unknown,
        }
    }

    pub fn as_str(self) -> &'static str {
        match self {
            Kind::Commit => "commit",
            Kind::Sync => "sync",
            Kind::Identity => "identity",
            Kind::Account => "account",
            Kind::Info => "info",
            Kind::Error => "error",
            Kind::Unknown => "unknown",
        }
    }
}

/// Where the body's `seq` value is encoded in the frame: `[start, end)`.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct SeqSpan {
    pub start: u32,
    pub end: u32,
}

/// The routing fields of a frame, borrowed from it.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Routing<'a> {
    pub kind: Kind,
    /// `repo` of a #commit, `did` of the others; syntax-checked.
    pub did: Option<&'a str>,
    pub seq: Option<i64>,
    pub seq_span: Option<SeqSpan>,
    /// Unchecked: [`parse`] validates it.
    pub rev: Option<&'a str>,
    pub time: Option<&'a str>,
}

/// The cheap parse: header, then the body's top-level map, skipping values
/// (a #commit's CAR is skipped by its length). Checks frame size and DID
/// syntax, not canonical encoding.
pub fn route(frame: &[u8], max_frame_bytes: usize) -> Result<Routing<'_>, Reject> {
    if frame.len() > max_frame_bytes {
        return Err(Reject::FrameTooBig);
    }
    let mut i = 0;
    let (op, t) = walk_header(frame, &mut i)?;
    let kind = Kind::from_tag(op, t);
    let mut r = Routing { kind, did: None, seq: None, seq_span: None, rev: None, time: None };
    let (major, n) = head(frame, &mut i).ok_or(Reject::BadFrame)?;
    if major != 5 {
        return Err(Reject::BadFrame);
    }
    for _ in 0..n {
        let key = text(frame, &mut i).ok_or(Reject::BadFrame)?;
        let at = i;
        match key {
            // the key parse reads for the kind: a frame carrying both must
            // route by the DID it's checked and applied as
            b"repo" if kind == Kind::Commit => r.did = Some(text_str(frame, &mut i)?),
            b"did" if matches!(kind, Kind::Sync | Kind::Identity | Kind::Account) => {
                r.did = Some(text_str(frame, &mut i)?);
            }
            b"seq" => {
                let (major, v) = head(frame, &mut i).ok_or(Reject::BadFrame)?;
                if major != 0 || v > i64::MAX as u64 {
                    return Err(Reject::BadSeq);
                }
                r.seq = Some(v as i64);
                r.seq_span = Some(SeqSpan { start: at as u32, end: i as u32 });
            }
            b"rev" => r.rev = Some(text_str(frame, &mut i)?),
            b"time" => r.time = Some(text_str(frame, &mut i)?),
            _ => skip(frame, &mut i, 0).ok_or(Reject::BadFrame)?,
        }
    }
    if i != frame.len() {
        return Err(Reject::BadFrame);
    }
    if r.did.is_some_and(|d| !syntax::valid_did(d)) {
        return Err(Reject::BadDid);
    }
    Ok(r)
}

fn walk_header<'a>(f: &'a [u8], i: &mut usize) -> Result<(i64, Option<&'a str>), Reject> {
    let (major, n) = head(f, i).ok_or(Reject::BadHeader)?;
    if major != 5 {
        return Err(Reject::BadHeader);
    }
    let (mut op, mut t) = (None, None);
    for _ in 0..n {
        let key = text(f, i).ok_or(Reject::BadHeader)?;
        match key {
            b"op" => {
                op = Some(match head(f, i).ok_or(Reject::BadHeader)? {
                    (0, v) if v <= 1 => v as i64,
                    (1, 0) => -1,
                    _ => return Err(Reject::BadHeader),
                })
            }
            b"t" => t = Some(text_str(f, i).map_err(|_| Reject::BadHeader)?),
            _ => skip(f, i, 0).ok_or(Reject::BadHeader)?,
        }
    }
    match op {
        Some(1) if t.is_some() => Ok((1, t)),
        Some(-1) => Ok((-1, None)),
        _ => Err(Reject::BadHeader),
    }
}

/// The frame with its body's `seq` replaced by `relay_seq`. One copy of the
/// frame; the body's other bytes are untouched. Canonical key order doesn't
/// depend on values, so a spliced canonical frame stays canonical.
pub fn encode_with_seq(frame: &[u8], span: SeqSpan, relay_seq: i64) -> Bytes {
    let mut out = BytesMut::with_capacity(frame.len() + 8);
    splice_seq_into(frame, span, relay_seq, &mut out);
    out.freeze()
}

/// [`encode_with_seq`] appended to `out` (a batch or segment buffer).
pub fn splice_seq_into<B: BufMut>(frame: &[u8], span: SeqSpan, relay_seq: i64, out: &mut B) {
    assert!(relay_seq >= 0, "relay seq must be non-negative");
    let (s, e) = (span.start as usize, span.end as usize);
    out.put_slice(&frame[..s]);
    let mut h = [0u8; 9];
    out.put_slice(uint_head(&mut h, relay_seq as u64));
    out.put_slice(&frame[e..]);
}

/// The minimal (canonical) CBOR head of an unsigned int.
fn uint_head(buf: &mut [u8; 9], n: u64) -> &[u8] {
    match n {
        0..=23 => {
            buf[0] = n as u8;
            &buf[..1]
        }
        24..=0xff => {
            buf[0] = 0x18;
            buf[1] = n as u8;
            &buf[..2]
        }
        0x100..=0xffff => {
            buf[0] = 0x19;
            buf[1..3].copy_from_slice(&(n as u16).to_be_bytes());
            &buf[..3]
        }
        0x1_0000..=0xffff_ffff => {
            buf[0] = 0x1a;
            buf[1..5].copy_from_slice(&(n as u32).to_be_bytes());
            &buf[..5]
        }
        _ => {
            buf[0] = 0x1b;
            buf[1..9].copy_from_slice(&n.to_be_bytes());
            &buf[..9]
        }
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub enum Action {
    Create,
    Update,
    Delete,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct RepoOp {
    pub action: Action,
    pub path: String,
    /// The record after the op (create/update); None for delete.
    pub cid: Option<Cid>,
    /// The record before the op (update/delete), sync 1.1. Some PDSes set it
    /// on a create too, when the commit deleted the record first.
    pub prev: Option<Cid>,
}

#[derive(Clone, Debug)]
pub struct ParsedCommit {
    pub frame: Bytes,
    pub seq: i64,
    pub seq_span: SeqSpan,
    pub repo: String,
    pub rev: Tid,
    pub since: Option<String>,
    pub time: String,
    pub commit: Cid,
    pub prev_data: Option<Cid>,
    pub ops: Vec<RepoOp>,
    pub blobs: Vec<Cid>,
    pub too_big: bool,
    pub rebase: bool,
    /// The CAR's roots; the first must be `commit`.
    pub car_roots: Vec<Cid>,
    /// The CAR's blocks in order, slices of `frame`, not yet hash-checked.
    pub blocks: Vec<(Cid, Bytes)>,
    pub blocks_len: usize,
}

#[derive(Clone, Debug)]
pub struct ParsedSync {
    pub frame: Bytes,
    pub seq: i64,
    pub seq_span: SeqSpan,
    pub did: String,
    pub rev: Tid,
    pub time: String,
    pub car_roots: Vec<Cid>,
    pub blocks: Vec<(Cid, Bytes)>,
}

#[derive(Clone, Debug)]
pub struct ParsedIdentity {
    pub frame: Bytes,
    pub seq: i64,
    pub seq_span: SeqSpan,
    pub did: String,
    pub time: String,
    pub handle: Option<String>,
}

#[derive(Clone, Debug)]
pub struct ParsedAccount {
    pub frame: Bytes,
    pub seq: i64,
    pub seq_span: SeqSpan,
    pub did: String,
    pub time: String,
    pub active: bool,
    pub status: Option<String>,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Info {
    pub name: String,
    pub message: Option<String>,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ErrorFrame {
    pub error: String,
    pub message: Option<String>,
}

#[derive(Clone, Debug)]
pub enum Event {
    Commit(ParsedCommit),
    Sync(ParsedSync),
    Identity(ParsedIdentity),
    Account(ParsedAccount),
    Info(Info),
    Error(ErrorFrame),
    Unknown,
}

impl Event {
    pub fn kind(&self) -> Kind {
        match self {
            Event::Commit(_) => Kind::Commit,
            Event::Sync(_) => Kind::Sync,
            Event::Identity(_) => Kind::Identity,
            Event::Account(_) => Kind::Account,
            Event::Info(_) => Kind::Info,
            Event::Error(_) => Kind::Error,
            Event::Unknown => Kind::Unknown,
        }
    }

    pub fn did(&self) -> Option<&str> {
        match self {
            Event::Commit(c) => Some(&c.repo),
            Event::Sync(s) => Some(&s.did),
            Event::Identity(e) => Some(&e.did),
            Event::Account(e) => Some(&e.did),
            _ => None,
        }
    }

    /// The upstream frame and where its seq is, for events the relay re-emits.
    pub fn frame_and_seq(&self) -> Option<(&Bytes, SeqSpan)> {
        match self {
            Event::Commit(c) => Some((&c.frame, c.seq_span)),
            Event::Sync(s) => Some((&s.frame, s.seq_span)),
            Event::Identity(e) => Some((&e.frame, e.seq_span)),
            Event::Account(e) => Some((&e.frame, e.seq_span)),
            _ => None,
        }
    }

    /// The frame to emit at `relay_seq`.
    pub fn encode_with_seq(&self, relay_seq: i64) -> Option<Bytes> {
        self.frame_and_seq().map(|(f, s)| encode_with_seq(f, s, relay_seq))
    }
}

/// The strict parse: canonical DAG-CBOR throughout, every field the relay
/// uses present and well formed, and the spec's size limits.
pub fn parse(frame: Bytes, limits: &Limits) -> Result<Event, Reject> {
    let r = route(&frame, limits.max_frame_bytes)?;
    let (header, hlen) = ValueRef::decode_prefix(&frame).map_err(|_| Reject::BadHeader)?;
    drop(header);
    let body = ValueRef::decode(&frame[hlen..]).map_err(|_| Reject::BadFrame)?;
    if !matches!(body, ValueRef::Map(_)) {
        return Err(Reject::BadFrame);
    }
    let f = Fields(&body);
    let seq = || -> Result<(i64, SeqSpan), Reject> {
        match (r.seq, r.seq_span) {
            (Some(s), Some(sp)) => Ok((s, sp)),
            _ => Err(Reject::MissingField("seq")),
        }
    };
    match r.kind {
        Kind::Commit => {
            let (seq, seq_span) = seq()?;
            let repo = f.did("repo")?;
            let rev = f.rev("rev")?;
            let since = f.opt_text("since")?.map(String::from);
            let time = f.text("time")?.to_string();
            let commit = f.link("commit")?;
            let prev_data = f.opt_link("prevData")?;
            let too_big = f.opt_bool("tooBig")?.unwrap_or(false);
            let rebase = f.opt_bool("rebase")?.unwrap_or(false);
            let ops_v = match body.get("ops") {
                Some(ValueRef::Array(a)) => a,
                _ => return Err(Reject::MissingField("ops")),
            };
            if ops_v.len() > limits.max_commit_ops {
                return Err(Reject::TooManyOps);
            }
            let mut ops = Vec::with_capacity(ops_v.len());
            for o in ops_v {
                ops.push(parse_op(o)?);
            }
            let blobs = match body.get("blobs") {
                Some(ValueRef::Array(a)) => a
                    .iter()
                    .map(|v| if let ValueRef::Link(c) = v { Ok(*c) } else { Err(Reject::BadField("blobs")) })
                    .collect::<Result<_, _>>()?,
                None | Some(ValueRef::Null) => Vec::new(),
                _ => return Err(Reject::BadField("blobs")),
            };
            let car = f.bytes("blocks")?;
            if car.len() > limits.max_commit_blocks_bytes {
                return Err(Reject::BlocksTooBig);
            }
            let (car_roots, blocks) = read_car(&frame, car, limits.max_commit_blocks)?;
            Ok(Event::Commit(ParsedCommit {
                seq,
                seq_span,
                repo: repo.to_string(),
                rev,
                since,
                time,
                commit,
                prev_data,
                ops,
                blobs,
                too_big,
                rebase,
                car_roots,
                blocks,
                blocks_len: car.len(),
                frame,
            }))
        }
        Kind::Sync => {
            let (seq, seq_span) = seq()?;
            let did = f.did("did")?.to_string();
            let rev = f.rev("rev")?;
            let time = f.text("time")?.to_string();
            let car = f.bytes("blocks")?;
            if car.len() > limits.max_sync_blocks_bytes {
                return Err(Reject::BlocksTooBig);
            }
            let (car_roots, blocks) = read_car(&frame, car, limits.max_commit_blocks)?;
            Ok(Event::Sync(ParsedSync { seq, seq_span, did, rev, time, car_roots, blocks, frame }))
        }
        Kind::Identity => {
            let (seq, seq_span) = seq()?;
            let did = f.did("did")?.to_string();
            let time = f.text("time")?.to_string();
            let handle = f.opt_text("handle")?.map(String::from);
            Ok(Event::Identity(ParsedIdentity { seq, seq_span, did, time, handle, frame }))
        }
        Kind::Account => {
            let (seq, seq_span) = seq()?;
            let did = f.did("did")?.to_string();
            let time = f.text("time")?.to_string();
            let active = f.opt_bool("active")?.ok_or(Reject::MissingField("active"))?;
            let status = f.opt_text("status")?.map(String::from);
            Ok(Event::Account(ParsedAccount { seq, seq_span, did, time, active, status, frame }))
        }
        Kind::Info => Ok(Event::Info(Info {
            name: f.text("name")?.to_string(),
            message: f.opt_text("message")?.map(String::from),
        })),
        Kind::Error => Ok(Event::Error(ErrorFrame {
            error: f.text("error")?.to_string(),
            message: f.opt_text("message")?.map(String::from),
        })),
        Kind::Unknown => Ok(Event::Unknown),
    }
}

fn parse_op(o: &ValueRef<'_>) -> Result<RepoOp, Reject> {
    if !matches!(o, ValueRef::Map(_)) {
        return Err(Reject::BadOp);
    }
    let f = Fields(o);
    let action = match f.text("action").map_err(|_| Reject::BadOp)? {
        "create" => Action::Create,
        "update" => Action::Update,
        "delete" => Action::Delete,
        _ => return Err(Reject::BadOp),
    };
    let path = f.text("path").map_err(|_| Reject::BadOp)?;
    if !syntax::valid_record_path(path) {
        return Err(Reject::BadOp);
    }
    // `cid` is required by the lexicon but nullable; deletes carry null
    let cid = f.opt_link("cid").map_err(|_| Reject::BadOp)?;
    let prev = f.opt_link("prev").map_err(|_| Reject::BadOp)?;
    match action {
        Action::Create | Action::Update if cid.is_none() => return Err(Reject::BadOp),
        Action::Delete if cid.is_some() => return Err(Reject::BadOp),
        _ => {}
    }
    Ok(RepoOp { action, path: path.to_string(), cid, prev })
}

/// The CAR's roots and blocks, blocks as slices of `frame` (`car` borrows it).
type Car = (Vec<Cid>, Vec<(Cid, Bytes)>);

fn read_car(frame: &Bytes, car: &[u8], max_blocks: usize) -> Result<Car, Reject> {
    let (roots, raw) = vlatproto::car::read_car(car).map_err(|_| Reject::BadCar)?;
    if raw.len() > max_blocks {
        return Err(Reject::TooManyBlocks);
    }
    if roots.is_empty() {
        return Err(Reject::BadCar);
    }
    let blocks = raw.into_iter().map(|(c, b)| (c, frame.slice_ref(b))).collect();
    Ok((roots, blocks))
}

struct Fields<'v, 'a>(&'v ValueRef<'a>);

impl<'a> Fields<'_, 'a> {
    fn text(&self, k: &'static str) -> Result<&'a str, Reject> {
        match self.0.get(k) {
            Some(ValueRef::Text(s)) => Ok(s),
            None => Err(Reject::MissingField(k)),
            _ => Err(Reject::BadField(k)),
        }
    }

    fn opt_text(&self, k: &'static str) -> Result<Option<&'a str>, Reject> {
        match self.0.get(k) {
            Some(ValueRef::Text(s)) => Ok(Some(s)),
            None | Some(ValueRef::Null) => Ok(None),
            _ => Err(Reject::BadField(k)),
        }
    }

    fn did(&self, k: &'static str) -> Result<&'a str, Reject> {
        let d = self.text(k)?;
        if !syntax::valid_did(d) {
            return Err(Reject::BadDid);
        }
        Ok(d)
    }

    fn rev(&self, k: &'static str) -> Result<Tid, Reject> {
        let s = self.text(k)?;
        if !syntax::valid_tid(s) {
            return Err(Reject::BadRev);
        }
        Tid::parse(s).ok_or(Reject::BadRev)
    }

    fn link(&self, k: &'static str) -> Result<Cid, Reject> {
        self.opt_link(k)?.ok_or(Reject::MissingField(k))
    }

    fn opt_link(&self, k: &'static str) -> Result<Option<Cid>, Reject> {
        match self.0.get(k) {
            Some(ValueRef::Link(c)) => Ok(Some(*c)),
            None | Some(ValueRef::Null) => Ok(None),
            _ => Err(Reject::BadField(k)),
        }
    }

    fn opt_bool(&self, k: &'static str) -> Result<Option<bool>, Reject> {
        match self.0.get(k) {
            Some(ValueRef::Bool(b)) => Ok(Some(*b)),
            None | Some(ValueRef::Null) => Ok(None),
            _ => Err(Reject::BadField(k)),
        }
    }

    fn bytes(&self, k: &'static str) -> Result<&'a [u8], Reject> {
        match self.0.get(k) {
            Some(ValueRef::Bytes(b)) => Ok(b),
            None => Err(Reject::MissingField(k)),
            _ => Err(Reject::BadField(k)),
        }
    }
}

/// The commit block with its `sig` entry removed (what was signed), and the
/// signature. `block` must already be strict DAG-CBOR (canonical, so the
/// entry order without `sig` is the unsigned encoding's order).
pub fn split_signed_commit<'a>(block: &'a [u8]) -> Option<(UnsignedCommit<'a>, &'a [u8])> {
    let mut i = 0;
    let (major, n) = head(block, &mut i)?;
    if major != 5 || n == 0 || n > 23 {
        return None;
    }
    let body_start = i;
    for _ in 0..n {
        let k_start = i;
        let key = text(block, &mut i)?;
        if key == b"sig" {
            let (major, len) = head(block, &mut i)?;
            if major != 2 {
                return None;
            }
            let sig = block.get(i..i.checked_add(len as usize)?)?;
            i += sig.len();
            return Some((
                UnsignedCommit { head: 0xa0 | (n as u8 - 1), before: &block[body_start..k_start], after: &block[i..] },
                sig,
            ));
        }
        skip(block, &mut i, 0)?;
    }
    None
}

/// The unsigned commit's encoding, in three pieces of the signed block.
#[derive(Clone, Copy, Debug)]
pub struct UnsignedCommit<'a> {
    pub head: u8,
    pub before: &'a [u8],
    pub after: &'a [u8],
}

impl UnsignedCommit<'_> {
    pub fn write(&self, out: &mut Vec<u8>) {
        out.reserve(1 + self.before.len() + self.after.len());
        out.push(self.head);
        out.extend_from_slice(self.before);
        out.extend_from_slice(self.after);
    }

    pub fn sha256(&self) -> [u8; 32] {
        use sha2::{Digest, Sha256};
        let mut h = Sha256::new();
        h.update([self.head]);
        h.update(self.before);
        h.update(self.after);
        h.finalize().into()
    }
}

// A minimal walker over definite-length CBOR (all DAG-CBOR is), for the
// fields `route` reads. Malformed input is None, never a panic.

fn head(f: &[u8], i: &mut usize) -> Option<(u8, u64)> {
    let b = *f.get(*i)?;
    *i += 1;
    let n = match b & 0x1f {
        n @ 0..=23 => return Some((b >> 5, n as u64)),
        24 => 1,
        25 => 2,
        26 => 4,
        27 => 8,
        _ => return None,
    };
    let bytes = f.get(*i..*i + n)?;
    *i += n;
    Some((b >> 5, bytes.iter().fold(0u64, |a, x| a << 8 | *x as u64)))
}

fn text<'a>(f: &'a [u8], i: &mut usize) -> Option<&'a [u8]> {
    let (major, n) = head(f, i)?;
    if major != 3 {
        return None;
    }
    let s = f.get(*i..i.checked_add(usize::try_from(n).ok()?)?)?;
    *i += s.len();
    Some(s)
}

fn text_str<'a>(f: &'a [u8], i: &mut usize) -> Result<&'a str, Reject> {
    let b = text(f, i).ok_or(Reject::BadFrame)?;
    std::str::from_utf8(b).map_err(|_| Reject::BadFrame)
}

fn skip(f: &[u8], i: &mut usize, depth: u32) -> Option<()> {
    if depth > 64 {
        return None;
    }
    let (major, n) = head(f, i)?;
    match major {
        2 | 3 => {
            let end = i.checked_add(usize::try_from(n).ok()?)?;
            if end > f.len() {
                return None;
            }
            *i = end;
        }
        4 => {
            for _ in 0..n {
                skip(f, i, depth + 1)?;
            }
        }
        5 => {
            for _ in 0..n.checked_mul(2)? {
                skip(f, i, depth + 1)?;
            }
        }
        6 => skip(f, i, depth + 1)?,
        _ => {}
    }
    Some(())
}

#[cfg(test)]
mod tests;
