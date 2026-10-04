//! Sync 1.1 checks on parsed events.
//!
//! [`verify_commit`] and [`verify_sync`] are stateless and run on the host
//! owner: block hashes, the commit's fields, its signature, and for a
//! #commit the inductive proof (the ops, undone on the partial MST in the
//! CAR, give back `prevData`). [`check_chain`] is the stateful step, a pure
//! function the DID owner runs against its stored [`ChainState`].

use crate::event::{Action, ParsedCommit, ParsedSync, RepoOp, split_signed_commit};
use bytes::Bytes;
use std::collections::HashMap;
use std::time::{Duration, SystemTime, UNIX_EPOCH};
use vlpds::cbor::ValueRef;
use vlpds::cid::Cid;
use vlpds::mst::{MstError, Tree};
use vlpds::tid::Tid;

/// Why an event was dropped. [`Reject::reason`] is a stable label for
/// metrics and per-host error budgets.
#[derive(Clone, Debug, PartialEq, Eq, thiserror::Error)]
pub enum Reject {
    #[error("frame over the size limit")]
    FrameTooBig,
    #[error("malformed frame header")]
    BadHeader,
    #[error("malformed frame body")]
    BadFrame,
    #[error("missing field {0}")]
    MissingField(&'static str),
    #[error("malformed field {0}")]
    BadField(&'static str),
    #[error("bad seq")]
    BadSeq,
    #[error("bad DID syntax")]
    BadDid,
    #[error("bad rev (not a TID)")]
    BadRev,
    #[error("rev too far in the future")]
    FutureRev,
    #[error("more ops than allowed")]
    TooManyOps,
    #[error("blocks over the size limit")]
    BlocksTooBig,
    #[error("more CAR blocks than allowed")]
    TooManyBlocks,
    #[error("malformed op")]
    BadOp,
    #[error("two ops on one path")]
    DuplicatePath,
    #[error("malformed CAR")]
    BadCar,
    #[error("CAR root is not the commit")]
    CarRootMismatch,
    #[error("block does not hash to its CID")]
    BlockHashMismatch,
    #[error("commit block missing from the CAR")]
    MissingCommitBlock,
    #[error("malformed commit object")]
    BadCommit,
    #[error("commit did does not match the event")]
    CommitDidMismatch,
    #[error("commit rev does not match the event")]
    CommitRevMismatch,
    #[error("unsupported commit version")]
    BadCommitVersion,
    #[error("signature does not verify")]
    BadSignature,
    #[error("no usable signing key")]
    NoSigningKey,
    #[error("malformed MST node")]
    BadMst,
    #[error("the CAR lacks MST blocks the ops need")]
    MissingMstBlocks,
    #[error("an op disagrees with the new tree")]
    OpMismatch,
    #[error("update or delete op without prev")]
    MissingOpPrev,
    #[error("the CAR lacks a created or updated record")]
    MissingRecordBlock,
    #[error("undoing the ops does not give prevData")]
    InversionMismatch,
    #[error("commit has no prevData")]
    MissingPrevData,
}

impl Reject {
    pub fn reason(&self) -> &'static str {
        match self {
            Reject::FrameTooBig => "frame_too_big",
            Reject::BadHeader => "bad_header",
            Reject::BadFrame => "bad_frame",
            Reject::MissingField(_) => "missing_field",
            Reject::BadField(_) => "bad_field",
            Reject::BadSeq => "bad_seq",
            Reject::BadDid => "bad_did",
            Reject::BadRev => "bad_rev",
            Reject::FutureRev => "future_rev",
            Reject::TooManyOps => "too_many_ops",
            Reject::BlocksTooBig => "blocks_too_big",
            Reject::TooManyBlocks => "too_many_blocks",
            Reject::BadOp => "bad_op",
            Reject::DuplicatePath => "duplicate_path",
            Reject::BadCar => "bad_car",
            Reject::CarRootMismatch => "car_root_mismatch",
            Reject::BlockHashMismatch => "block_hash_mismatch",
            Reject::MissingCommitBlock => "missing_commit_block",
            Reject::BadCommit => "bad_commit",
            Reject::CommitDidMismatch => "commit_did_mismatch",
            Reject::CommitRevMismatch => "commit_rev_mismatch",
            Reject::BadCommitVersion => "bad_commit_version",
            Reject::BadSignature => "bad_signature",
            Reject::NoSigningKey => "no_signing_key",
            Reject::BadMst => "bad_mst",
            Reject::MissingMstBlocks => "missing_mst_blocks",
            Reject::OpMismatch => "op_mismatch",
            Reject::MissingOpPrev => "missing_op_prev",
            Reject::MissingRecordBlock => "missing_record_block",
            Reject::InversionMismatch => "inversion_mismatch",
            Reject::MissingPrevData => "missing_prev_data",
        }
    }

    /// A failure a fresh DID document might fix (the key rotated): the
    /// caller re-resolves once and retries before dropping the event.
    pub fn may_be_stale_key(&self) -> bool {
        matches!(self, Reject::BadSignature | Reject::NoSigningKey)
    }
}

/// A DID document's `#atproto` key, parsed once (a compressed point costs a
/// square root to decompress, about as much as hashing a whole commit).
#[derive(Clone)]
pub enum SigningKey {
    K256(secp256k1::PublicKey),
    /// The uncompressed SEC1 point, as ring wants it.
    P256(Box<[u8; 65]>),
}

impl std::fmt::Debug for SigningKey {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            SigningKey::K256(_) => f.write_str("SigningKey::K256"),
            SigningKey::P256(_) => f.write_str("SigningKey::P256"),
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq, thiserror::Error)]
#[error("unsupported or malformed key: {0}")]
pub struct KeyError(&'static str);

impl SigningKey {
    /// `publicKeyMultibase` of a Multikey verification method: `z` + base58btc
    /// of the multicodec-prefixed compressed point.
    pub fn from_multibase(mb: &str) -> Result<SigningKey, KeyError> {
        let b58 = mb
            .strip_prefix('z')
            .ok_or(KeyError("not base58btc multibase"))?;
        let raw = bs58::decode(b58)
            .into_vec()
            .map_err(|_| KeyError("bad base58"))?;
        match raw.as_slice() {
            [0xe7, 0x01, key @ ..] if key.len() == 33 => secp256k1::PublicKey::from_slice(key)
                .map(SigningKey::K256)
                .map_err(|_| KeyError("bad secp256k1 point")),
            [0x80, 0x24, key @ ..] if key.len() == 33 => {
                let pk = p256::PublicKey::from_sec1_bytes(key)
                    .map_err(|_| KeyError("bad P-256 point"))?;
                let pt = p256::elliptic_curve::sec1::ToEncodedPoint::to_encoded_point(&pk, false);
                let b: [u8; 65] = pt
                    .as_bytes()
                    .try_into()
                    .map_err(|_| KeyError("bad P-256 point"))?;
                Ok(SigningKey::P256(Box::new(b)))
            }
            _ => Err(KeyError("not a compressed secp256k1 or P-256 multikey")),
        }
    }

    pub fn from_did_key(k: &str) -> Result<SigningKey, KeyError> {
        Self::from_multibase(
            k.strip_prefix("did:key:")
                .ok_or(KeyError("not a did:key"))?,
        )
    }

    pub fn curve(&self) -> &'static str {
        match self {
            SigningKey::K256(_) => "k256",
            SigningKey::P256(_) => "p256",
        }
    }

    /// A compact low-S ECDSA signature over sha256(msg). `digest` must be
    /// sha256(msg); k256 uses only the digest, ring's P-256 only the message.
    pub fn verify(&self, msg: &[u8], digest: &[u8; 32], sig: &[u8]) -> bool {
        let Ok(sig64) = <&[u8; 64]>::try_from(sig) else {
            return false;
        };
        match self {
            SigningKey::K256(pk) => {
                let Ok(s) = secp256k1::ecdsa::Signature::from_compact(sig64) else {
                    return false;
                };
                // libsecp256k1 rejects high-S itself, as atproto requires
                secp256k1::SECP256K1
                    .verify_ecdsa(&secp256k1::Message::from_digest(*digest), &s, pk)
                    .is_ok()
            }
            SigningKey::P256(pt) => {
                if !p256_low_s(sig64) {
                    return false;
                }
                ring::signature::UnparsedPublicKey::new(
                    &ring::signature::ECDSA_P256_SHA256_FIXED,
                    &pt[..],
                )
                .verify(msg, sig64)
                .is_ok()
            }
        }
    }
}

/// s <= n/2 (big-endian compare against the P-256 order's half).
fn p256_low_s(sig: &[u8; 64]) -> bool {
    const HALF_N: [u8; 32] = [
        0x7f, 0xff, 0xff, 0xff, 0x80, 0x00, 0x00, 0x00, 0x7f, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff,
        0xff, 0xde, 0x73, 0x7d, 0x56, 0xd3, 0x8b, 0xcf, 0x42, 0x79, 0xdc, 0xe5, 0x61, 0x7e, 0x31,
        0x92, 0xa8,
    ];
    sig[32..] <= HALF_N[..]
}

#[derive(Clone, Debug)]
pub struct Options {
    /// Revs whose TID time is further ahead of our clock are rejected (they
    /// would wedge the account's chain until the clock caught up).
    pub future_rev_tolerance: Duration,
    /// Reject commits without `prevData` (sync 1.0 producers). Off while
    /// such PDSes exist: those commits get the signature and forward checks
    /// only, and [`Verified::prev_data`] is None.
    pub require_prev_data: bool,
    /// Reject commits whose CAR lacks a created or updated record's block.
    pub require_record_blocks: bool,
}

impl Default for Options {
    fn default() -> Self {
        Options {
            future_rev_tolerance: Duration::from_secs(300),
            require_prev_data: false,
            require_record_blocks: true,
        }
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum VerifiedKind {
    Commit,
    Sync,
}

/// What [`check_chain`] needs from a verified event.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Verified {
    pub kind: VerifiedKind,
    pub did: String,
    pub rev: Tid,
    pub commit: Cid,
    pub data: Cid,
    /// #commit only; None for a sync 1.0 commit or a #sync.
    pub prev_data: Option<Cid>,
}

/// The decoded commit object (repo v3).
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct CommitObj<'a> {
    pub did: &'a str,
    pub version: i64,
    pub data: Cid,
    pub rev: &'a str,
    pub prev: Option<Cid>,
}

fn decode_commit(block: &[u8]) -> Result<CommitObj<'_>, Reject> {
    let v = ValueRef::decode(block).map_err(|_| Reject::BadCommit)?;
    let text = |k| v.get(k).and_then(ValueRef::as_str).ok_or(Reject::BadCommit);
    let version = match v.get("version") {
        Some(ValueRef::Int(n)) => *n,
        _ => return Err(Reject::BadCommit),
    };
    let data = match v.get("data") {
        Some(ValueRef::Link(c)) => *c,
        _ => return Err(Reject::BadCommit),
    };
    let prev = match v.get("prev") {
        Some(ValueRef::Link(c)) => Some(*c),
        None | Some(ValueRef::Null) => None,
        _ => return Err(Reject::BadCommit),
    };
    Ok(CommitObj {
        did: text("did")?,
        version,
        data,
        rev: text("rev")?,
        prev,
    })
}

thread_local! {
    static UNSIGNED: std::cell::RefCell<Vec<u8>> = const { std::cell::RefCell::new(Vec::new()) };
}

/// The commit block's fields against the event, then its signature.
#[doc(hidden)]
pub fn check_commit_block<'a>(
    block: &'a [u8],
    did: &str,
    rev: Tid,
    key: &SigningKey,
) -> Result<CommitObj<'a>, Reject> {
    let c = decode_commit(block)?;
    if c.version != 3 {
        return Err(Reject::BadCommitVersion);
    }
    if c.did != did {
        return Err(Reject::CommitDidMismatch);
    }
    if Tid::parse(c.rev) != Some(rev) {
        return Err(Reject::CommitRevMismatch);
    }
    let (unsigned, sig) = split_signed_commit(block).ok_or(Reject::BadCommit)?;
    let ok = match key {
        // libsecp256k1 takes the digest: hash the three pieces in place
        SigningKey::K256(_) => key.verify(&[], &unsigned.sha256(), sig),
        SigningKey::P256(_) => UNSIGNED.with_borrow_mut(|buf| {
            buf.clear();
            unsigned.write(buf);
            key.verify(buf, &[0; 32], sig)
        }),
    };
    if !ok {
        return Err(Reject::BadSignature);
    }
    Ok(c)
}

fn check_future_rev(rev: Tid, opts: &Options) -> Result<(), Reject> {
    let now = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_micros() as u64;
    if rev.micros() > now.saturating_add(opts.future_rev_tolerance.as_micros() as u64) {
        return Err(Reject::FutureRev);
    }
    Ok(())
}

/// Every block hashed against its CID, as a map by CID.
#[doc(hidden)]
pub fn block_map(blocks: &[(Cid, Bytes)]) -> Result<HashMap<Cid, &[u8]>, Reject> {
    let mut m = HashMap::with_capacity(blocks.len());
    for (c, b) in blocks {
        if !vlpds::car::block_matches(c, b) {
            return Err(Reject::BlockHashMismatch);
        }
        m.insert(*c, &b[..]);
    }
    Ok(m)
}

fn mst_err(e: MstError) -> Reject {
    match e {
        MstError::Partial | MstError::NotLoaded => Reject::MissingMstBlocks,
        MstError::InvalidKey => Reject::BadOp,
        _ => Reject::BadMst,
    }
}

pub fn verify_commit(c: &ParsedCommit, key: &SigningKey) -> Result<Verified, Reject> {
    verify_commit_with(c, key, &Options::default())
}

pub fn verify_commit_with(
    c: &ParsedCommit,
    key: &SigningKey,
    opts: &Options,
) -> Result<Verified, Reject> {
    check_future_rev(c.rev, opts)?;
    if c.car_roots.first() != Some(&c.commit) {
        return Err(Reject::CarRootMismatch);
    }
    let blocks = block_map(&c.blocks)?;
    let block = blocks.get(&c.commit).ok_or(Reject::MissingCommitBlock)?;
    let obj = check_commit_block(block, &c.repo, c.rev, key)?;
    if c.prev_data.is_none() && opts.require_prev_data {
        return Err(Reject::MissingPrevData);
    }
    check_ops(c, obj.data, &blocks, opts)?;
    Ok(Verified {
        kind: VerifiedKind::Commit,
        did: c.repo.clone(),
        rev: c.rev,
        commit: c.commit,
        data: obj.data,
        prev_data: c.prev_data,
    })
}

/// The ops against the new tree, then (with prevData) the inductive proof.
#[doc(hidden)]
pub fn check_ops(
    c: &ParsedCommit,
    data: Cid,
    blocks: &HashMap<Cid, &[u8]>,
    opts: &Options,
) -> Result<(), Reject> {
    let net = net_ops(&c.ops)?;
    if opts.require_record_blocks
        && c.ops
            .iter()
            .any(|o| o.cid.is_some_and(|cid| !blocks.contains_key(&cid)))
    {
        return Err(Reject::MissingRecordBlock);
    }
    // an empty commit (rev bump) may leave the root out of its CAR
    if c.ops.is_empty() && !blocks.contains_key(&data) {
        return match c.prev_data {
            Some(p) if p != data => Err(Reject::InversionMismatch),
            _ => Ok(()),
        };
    }
    // most commits are one create: undo it on the raw nodes when that's
    // certain to agree, else (and for every reject) the tree path decides
    if let [op] = c.ops.as_slice()
        && op.action == Action::Create
        && op.prev.is_none()
        && let Some(cid) = op.cid
        && FAST_PATH.get()
    {
        match vlpds::mst::single_create::undo_single_create(
            blocks,
            data,
            op.path.as_bytes(),
            cid,
            c.prev_data.is_some(),
        ) {
            Some(prev) if prev == c.prev_data => return Ok(()),
            _ => {}
        }
    }
    check_ops_tree(c, data, blocks, &net)
}

/// One path's change across a commit's ops.
struct NetOp<'a> {
    path: &'a str,
    /// The record before the commit, from the path's first op: its `prev`
    /// (a create's too, which some PDSes set when a commit deletes and
    /// re-creates a record).
    before: Option<Cid>,
    /// The first op is an update or delete, so `before` must be known for
    /// the inductive proof.
    needs_before: bool,
    after: Option<Cid>,
    /// The path's last op, for undoing in reverse op order: on a partial
    /// tree, another order can need nodes the CAR leaves out.
    last: usize,
}

/// The ops folded per path, latest last op first. A path may take several
/// ops (a delete then a create of the same record, as applyWrites allows);
/// each must fit the one before it (a create only of a missing record, an
/// update or delete only of the record the last op left), else the ops
/// contradict each other.
fn net_ops(ops: &[RepoOp]) -> Result<Vec<NetOp<'_>>, Reject> {
    fn start(o: &RepoOp, i: usize) -> NetOp<'_> {
        NetOp {
            path: o.path.as_str(),
            before: o.prev,
            needs_before: o.action != Action::Create,
            after: o.cid,
            last: i,
        }
    }
    if ops.len() == 1 {
        return Ok(vec![start(&ops[0], 0)]);
    }
    let mut order: Vec<usize> = (0..ops.len()).collect();
    order.sort_by(|&a, &b| ops[a].path.cmp(&ops[b].path).then(a.cmp(&b)));
    let mut net: Vec<NetOp<'_>> = Vec::with_capacity(ops.len());
    for i in order {
        let o = &ops[i];
        match net.last_mut() {
            Some(n) if n.path == o.path => {
                // a create's prev, if set, is the record before the commit
                let fits = match o.action {
                    Action::Create => n.after.is_none(),
                    Action::Update | Action::Delete => {
                        n.after.is_some() && o.prev.is_none_or(|p| Some(p) == n.after)
                    }
                };
                if !fits {
                    return Err(Reject::DuplicatePath);
                }
                n.after = o.cid;
                n.last = i;
            }
            _ => net.push(start(o, i)),
        }
    }
    net.sort_unstable_by(|a, b| b.last.cmp(&a.last));
    Ok(net)
}

thread_local! {
    /// Off in tests that compare the fast path with the tree path.
    static FAST_PATH: std::cell::Cell<bool> = const { std::cell::Cell::new(true) };
}

#[cfg(test)]
pub(crate) fn set_fast_path(on: bool) {
    FAST_PATH.set(on);
}

fn check_ops_tree(
    c: &ParsedCommit,
    data: Cid,
    blocks: &HashMap<Cid, &[u8]>,
    net: &[NetOp<'_>],
) -> Result<(), Reject> {
    let mut tree = Tree::load_from_blocks(blocks, data)
        .map_err(mst_err)?
        .without_rollback();
    for n in net {
        if tree.get(n.path.as_bytes()).map_err(mst_err)? != n.after {
            return Err(Reject::OpMismatch);
        }
    }
    let Some(prev_data) = c.prev_data else {
        return Ok(());
    };
    for n in net {
        match n.before {
            Some(b) => tree.insert_no_proof(n.path.as_bytes(), b).map(drop),
            None if n.needs_before => return Err(Reject::MissingOpPrev),
            None => tree.remove_no_proof(n.path.as_bytes()).map(drop),
        }
        .map_err(mst_err)?;
    }
    if tree.root_cid().map_err(mst_err)? != prev_data {
        return Err(Reject::InversionMismatch);
    }
    Ok(())
}

pub fn verify_sync(s: &ParsedSync, key: &SigningKey) -> Result<Verified, Reject> {
    verify_sync_with(s, key, &Options::default())
}

pub fn verify_sync_with(
    s: &ParsedSync,
    key: &SigningKey,
    opts: &Options,
) -> Result<Verified, Reject> {
    check_future_rev(s.rev, opts)?;
    let commit = *s.car_roots.first().ok_or(Reject::BadCar)?;
    let (_, block) = s
        .blocks
        .iter()
        .find(|(c, _)| *c == commit)
        .ok_or(Reject::MissingCommitBlock)?;
    if !vlpds::car::block_matches(&commit, block) {
        return Err(Reject::BlockHashMismatch);
    }
    let obj = check_commit_block(block, &s.did, s.rev, key)?;
    Ok(Verified {
        kind: VerifiedKind::Sync,
        did: s.did.clone(),
        rev: s.rev,
        commit,
        data: obj.data,
        prev_data: None,
    })
}

/// What the DID owner keeps per account to check the next event.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct ChainState {
    pub rev: Tid,
    pub data: Cid,
    pub commit: Cid,
}

pub const CHAIN_STATE_BYTES: usize = 8 + 2 * vlpds::cid::CID_BYTES_LEN;

impl ChainState {
    pub fn to_bytes(&self) -> [u8; CHAIN_STATE_BYTES] {
        let mut out = [0u8; CHAIN_STATE_BYTES];
        out[..8].copy_from_slice(&self.rev.0.to_be_bytes());
        out[8..44].copy_from_slice(&self.data.to_bytes());
        out[44..].copy_from_slice(&self.commit.to_bytes());
        out
    }

    pub fn from_bytes(b: &[u8]) -> Option<ChainState> {
        if b.len() != CHAIN_STATE_BYTES {
            return None;
        }
        Some(ChainState {
            rev: Tid(u64::from_be_bytes(b[..8].try_into().ok()?)),
            data: Cid::from_bytes(&b[8..44]).ok()?,
            commit: Cid::from_bytes(&b[44..]).ok()?,
        })
    }
}

#[derive(Clone, Debug, PartialEq, Eq, thiserror::Error)]
pub enum ChainError {
    /// The same commit again (a replay after an upstream reconnect): ack it,
    /// don't emit it twice.
    #[error("duplicate of the current commit")]
    Duplicate,
    #[error("rev does not move forward")]
    RevNotForward,
    /// The chain broke: the account is desynchronized until a #sync.
    #[error("prevData {got} is not the stored data CID {expected}")]
    PrevDataMismatch { expected: Cid, got: Cid },
}

impl ChainError {
    pub fn reason(&self) -> &'static str {
        match self {
            ChainError::Duplicate => "duplicate",
            ChainError::RevNotForward => "rev_not_forward",
            ChainError::PrevDataMismatch { .. } => "prev_data_mismatch",
        }
    }
}

/// The stateful sync 1.1 check. A #commit must move the rev forward and
/// build on the stored data CID; a #sync (rev not older) resets the chain.
/// A first sighting is accepted as is. A commit without prevData (sync 1.0)
/// only has to move the rev forward.
pub fn check_chain(prev: Option<&ChainState>, v: &Verified) -> Result<ChainState, ChainError> {
    let next = ChainState {
        rev: v.rev,
        data: v.data,
        commit: v.commit,
    };
    let Some(p) = prev else { return Ok(next) };
    if v.rev <= p.rev {
        return Err(if v.rev == p.rev && v.commit == p.commit {
            ChainError::Duplicate
        } else {
            ChainError::RevNotForward
        });
    }
    if let (VerifiedKind::Commit, Some(got)) = (v.kind, v.prev_data)
        && got != p.data
    {
        return Err(ChainError::PrevDataMismatch {
            expected: p.data,
            got,
        });
    }
    Ok(next)
}

pub mod synth;

#[cfg(test)]
mod tests;

#[cfg(test)]
mod differential;

#[cfg(test)]
mod fast_tests;
