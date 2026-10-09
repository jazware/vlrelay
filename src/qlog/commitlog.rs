//! The local commitlog (docs/quorum.md, "Memory only, or a local
//! commitlog"): every change to a node's log, appended to segment files on
//! local disk and group-fsynced before the node acks (a follower) or counts
//! itself toward the quorum (the leader). It's also the single node's WAL:
//! nothing in here knows about replication.
//!
//! Records are `len u32 | crc32 u32 | type u8 | payload`, little endian,
//! the CRC over type and payload. A segment starts with a header record
//! giving the `(epoch, seq)` it follows and the promise in force. Ops are
//! written in the order the node staged them, so replaying a segment run
//! rebuilds the log exactly as it was at the last fsync:
//!
//! - an entry is an `Append` record, and a restamped entry is written again,
//!   so the last record for a seq is its current state;
//! - truncations and resets are records, never rewrites;
//! - the commit index rides along as a record after a batch (a lower bound,
//!   never needed for safety; it lets a restarted node emit at once).
//!
//! A new segment begins at the commit index and repeats the uncommitted
//! tail, so any later truncation lands inside it (nothing at or below the
//! commit index is ever truncated) and the segments before it can be
//! deleted on their own. Recovery truncates a torn tail in the last segment
//! at its first bad record. Anything past the last fsync was never acked.

use super::log::{Entry, Log, Op, decode_side, encode_side};
use bytes::Bytes;
use parking_lot::Mutex;
use std::collections::VecDeque;
use std::fs::{File, OpenOptions};
use std::io::Write;
use std::os::unix::fs::FileExt;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, Condvar};
use std::time::{Duration, Instant};
use tokio::sync::watch;

const MAGIC: u64 = u64::from_le_bytes(*b"QLOGSEG1");
const REC_HEAD: usize = 9;
const MAX_REC: usize = 256 << 20;

const T_APPEND: u8 = 1;
const T_TRUNCATE: u8 = 2;
const T_RESET: u8 = 3;
const T_PROMISE: u8 = 4;
const T_COMMIT: u8 = 5;
/// An append whose entry carries host cursors or meta: `epoch, seq, slen
/// u32, side, data`, the side being `log::encode_side`.
const T_APPEND_C: u8 = 6;
const T_HEADER: u8 = 0x10;

#[derive(Clone, Debug)]
pub struct Options {
    /// A segment rolls over once it's this big.
    pub segment_bytes: u64,
    /// Segments wholly below the trim floor are deleted while the log is
    /// bigger than this (never the active one).
    pub retain_bytes: u64,
    /// Entries loaded back into memory at recovery: the newest, at most
    /// this much (plus anything uncommitted).
    pub memory_bytes: usize,
    /// Tests: sleep this long before each fsync, to widen the window an
    /// early ack would fall in.
    pub sync_delay: Option<Duration>,
    /// Abort the process on a failed write or fsync: after a failed fsync
    /// the page cache can't be trusted to hold what was written.
    pub abort_on_error: bool,
    /// Chaos: called between two segment deletions of one trim pass (a
    /// crash there leaves the older ones gone and the newer ones in place).
    pub mid_trim: Option<Hook>,
    /// When a write counts (docs/quorum.md, "Durability modes").
    pub sync: SyncMode,
    /// Mutation tests only: don't distrust the log after a power loss in
    /// `PageCache` mode (the check this switches off is what keeps a node
    /// that lost acked entries from vouching for its log).
    pub trust_after_power_loss: bool,
}

/// When the node may act on a write: ack it as a follower, count itself
/// as the leader.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum SyncMode {
    /// After its fdatasync: a power cut loses nothing acked.
    Fsync,
    /// Once written to the page cache, fdatasync'd every `every` in the
    /// background. A process crash loses nothing (the kernel still holds
    /// the pages); a power cut can lose the last `every` of acked writes,
    /// so a node that comes back from one doesn't vouch for its log.
    /// Promises are fsynced before they're answered in every mode.
    PageCache { every: Duration },
}

impl SyncMode {
    pub fn name(self) -> &'static str {
        match self {
            SyncMode::Fsync => "fsync",
            SyncMode::PageCache { .. } => "page-cache",
        }
    }
}

/// A node's durability mode, from `--durability` and the cluster's size.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum DurabilityMode {
    Sync(SyncMode),
    /// No commitlog: a restarted node comes back empty, and a correlated
    /// loss is a bucket recovery.
    Memory,
}

impl DurabilityMode {
    /// `mode` as given (None: page-cache for three members or more, fsync
    /// below). A single node only runs fsync: it has no other copy.
    pub fn choose(mode: Option<&str>, sync: Duration, members: usize) -> anyhow::Result<DurabilityMode> {
        let m = match mode.unwrap_or(if members >= 3 { "page-cache" } else { "fsync" }) {
            "fsync" => DurabilityMode::Sync(SyncMode::Fsync),
            "page-cache" => DurabilityMode::Sync(SyncMode::PageCache { every: sync.max(Duration::from_millis(1)) }),
            "memory" => DurabilityMode::Memory,
            other => anyhow::bail!("--durability {other}: fsync, page-cache or memory"),
        };
        anyhow::ensure!(
            members > 1 || m == DurabilityMode::Sync(SyncMode::Fsync),
            "--durability {}: a single node has no other copy of its log; it runs fsync",
            m.name()
        );
        Ok(m)
    }

    pub fn name(self) -> &'static str {
        match self {
            DurabilityMode::Sync(s) => s.name(),
            DurabilityMode::Memory => "memory",
        }
    }
}

/// What the previous run of this directory recorded: the machine's boot
/// and the mode, so a reboot after page-cache writes is noticed.
const BOOT_FILE: &str = "boot";
/// Left by an emulated power cut ([`CommitLog::power_cut`]): the next open
/// treats it as a reboot.
const POWER_LOST: &str = "power-lost";

fn boot_id() -> String {
    if let Ok(s) = std::fs::read_to_string("/proc/sys/kernel/random/boot_id") {
        return s.trim().to_string();
    }
    boot_time().map_or_else(|| "unknown".into(), |t| format!("boottime-{t}"))
}

/// Without /proc (macOS), the boot time stands in for a boot id.
#[cfg(target_os = "macos")]
fn boot_time() -> Option<i64> {
    let mut tv = libc::timeval { tv_sec: 0, tv_usec: 0 };
    let mut len = std::mem::size_of::<libc::timeval>();
    let mut mib = [libc::CTL_KERN, libc::KERN_BOOTTIME];
    // SAFETY: sysctl writes at most `len` bytes into `tv`.
    let ok = unsafe {
        libc::sysctl(mib.as_mut_ptr(), 2, (&mut tv as *mut libc::timeval).cast(), &mut len, std::ptr::null_mut(), 0)
    } == 0;
    ok.then_some(tv.tv_sec)
}

#[cfg(not(target_os = "macos"))]
fn boot_time() -> Option<i64> {
    None
}

#[derive(Clone)]
pub struct Hook(pub Arc<dyn Fn() + Send + Sync>);

impl std::fmt::Debug for Hook {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("Hook")
    }
}

impl Default for Options {
    fn default() -> Self {
        Options {
            segment_bytes: 64 << 20,
            retain_bytes: 4 << 30,
            memory_bytes: 64 << 20,
            sync_delay: None,
            abort_on_error: false,
            mid_trim: None,
            sync: SyncMode::Fsync,
            trust_after_power_loss: false,
        }
    }
}

/// What a node needs back at startup.
pub struct Recovered {
    pub log: Log,
    pub promised: u64,
    /// Whom `promised` went to, if known: that candidate's retried promise
    /// round is answered again after a restart.
    pub promised_to: Option<String>,
    /// No segment existed: a new or wiped disk, which can't vouch for
    /// anything this node acked before.
    pub fresh: bool,
    /// The machine went down (a reboot since the last run, which wrote in
    /// page-cache mode): acked writes past the last fsync may be gone, so
    /// the log is kept but can't vouch for what this node acked either.
    pub power_lost: bool,
    pub segments: usize,
    pub records: u64,
    pub torn_bytes: u64,
    pub took: Duration,
}

#[derive(Clone, Copy, Debug)]
struct Loc {
    epoch: u64,
    seg: u64,
    /// Where the data starts; the side (`clen` bytes: cursors and meta) sits
    /// right before it.
    off: u64,
    len: u32,
    clen: u32,
}

/// Where every entry of the log lives on disk: the commitlog's own copy of
/// the log's shape, for reads and rollover.
#[derive(Default)]
struct Index {
    base_epoch: u64,
    base_seq: u64,
    locs: VecDeque<Loc>,
}

impl Index {
    fn last_seq(&self) -> u64 {
        self.base_seq + self.locs.len() as u64
    }
    fn epoch_at(&self, seq: u64) -> Option<u64> {
        if seq == self.base_seq {
            return Some(self.base_epoch);
        }
        if seq < self.base_seq {
            return None;
        }
        self.locs.get((seq - self.base_seq - 1) as usize).map(|l| l.epoch)
    }
    fn get(&self, seq: u64) -> Option<Loc> {
        if seq <= self.base_seq {
            return None;
        }
        self.locs.get((seq - self.base_seq - 1) as usize).copied()
    }
    fn push(&mut self, seq: u64, loc: Loc) -> bool {
        if seq != self.last_seq() + 1 {
            return false;
        }
        self.locs.push_back(loc);
        true
    }
    fn truncate_after(&mut self, seq: u64) {
        let keep = seq.saturating_sub(self.base_seq) as usize;
        self.locs.truncate(keep);
    }
    fn reset(&mut self, epoch: u64, seq: u64) {
        self.locs.clear();
        self.base_epoch = epoch;
        self.base_seq = seq;
    }
    /// Forgets everything at or below `seq`.
    fn drop_upto(&mut self, seq: u64) {
        if seq <= self.base_seq {
            return;
        }
        let Some(e) = self.epoch_at(seq) else { return };
        let n = ((seq - self.base_seq) as usize).min(self.locs.len());
        self.locs.drain(..n);
        self.base_epoch = e;
        self.base_seq = seq;
    }
    fn bytes_after(&self, seq: u64) -> u64 {
        let from = seq.saturating_sub(self.base_seq) as usize;
        self.locs.iter().skip(from).map(|l| l.len as u64).sum()
    }
}

struct Seg {
    no: u64,
    base_seq: u64,
    bytes: u64,
    /// Its length as of its last fdatasync, once it's no longer the active
    /// segment (the active one's is `Shared::durable_len`).
    durable: u64,
    /// Kept open so a read survives the file's deletion.
    file: Arc<File>,
}

struct Shared {
    index: Index,
    segs: Vec<Seg>,
    /// The commit index as last written (capped at the log's last seq, as
    /// replay caps it).
    written_commit: u64,
    /// The active segment's length as of its last fdatasync.
    durable_len: u64,
    written_len: u64,
    /// When the active segment was last fdatasync'd.
    synced_at: Instant,
}

#[derive(Default)]
struct Queue {
    ops: Vec<Op>,
    staged: u64,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
struct Durable {
    ticket: u64,
    failed: bool,
}

pub struct Stats {
    pub fsync_us: Mutex<hdrhistogram::Histogram<u64>>,
    /// Ops per group commit.
    pub batch_ops: Mutex<hdrhistogram::Histogram<u64>>,
    pub fsyncs: AtomicU64,
    pub bytes: AtomicU64,
    pub rollovers: AtomicU64,
    pub deleted: AtomicU64,
    /// Background fdatasyncs in page-cache mode (in `fsyncs` too).
    pub background_syncs: AtomicU64,
}

impl Default for Stats {
    fn default() -> Self {
        Stats {
            fsync_us: Mutex::new(hdrhistogram::Histogram::new_with_bounds(1, 60_000_000, 3).expect("bounds")),
            batch_ops: Mutex::new(hdrhistogram::Histogram::new_with_bounds(1, 10_000_000, 3).expect("bounds")),
            fsyncs: AtomicU64::new(0),
            bytes: AtomicU64::new(0),
            rollovers: AtomicU64::new(0),
            deleted: AtomicU64::new(0),
            background_syncs: AtomicU64::new(0),
        }
    }
}

pub struct CommitLog {
    dir: PathBuf,
    opts: Options,
    queue: std::sync::Mutex<Queue>,
    wake: Condvar,
    durable: watch::Sender<Durable>,
    commit: AtomicU64,
    floor: AtomicU64,
    shared: Mutex<Shared>,
    halted: AtomicBool,
    writer: Mutex<Option<std::thread::JoinHandle<()>>>,
    pub stats: Stats,
}

fn seg_path(dir: &Path, no: u64) -> PathBuf {
    dir.join(format!("{no:020}.qlog"))
}

fn rec(buf: &mut Vec<u8>, ty: u8, parts: &[&[u8]]) -> usize {
    let len = 1 + parts.iter().map(|p| p.len()).sum::<usize>();
    let mut h = crc32fast::Hasher::new();
    h.update(&[ty]);
    for p in parts {
        h.update(p);
    }
    buf.extend_from_slice(&(len as u32).to_le_bytes());
    buf.extend_from_slice(&h.finalize().to_le_bytes());
    buf.push(ty);
    let payload_at = buf.len();
    for p in parts {
        buf.extend_from_slice(p);
    }
    payload_at
}

/// Appends an entry's record; returns where its data starts in `buf`.
fn rec_append(buf: &mut Vec<u8>, e: &Entry) -> usize {
    let side = encode_side(&e.cursors, &e.meta);
    if side.is_empty() {
        return rec(buf, T_APPEND, &[&e.epoch.to_le_bytes(), &e.seq.to_le_bytes(), &e.data]) + 16;
    }
    let slen = side.len() as u32;
    rec(buf, T_APPEND_C, &[&e.epoch.to_le_bytes(), &e.seq.to_le_bytes(), &slen.to_le_bytes(), &side, &e.data])
        + 20
        + side.len()
}

fn side_len(e: &Entry) -> u32 {
    if e.cursors.is_empty() && e.meta.is_empty() { 0 } else { (4 + e.cursors.len() + e.meta.len()) as u32 }
}

fn loc_of(e: &Entry, seg: u64, off: u64) -> Loc {
    Loc { epoch: e.epoch, seg, off, len: e.data.len() as u32, clen: side_len(e) }
}

fn rec_header(buf: &mut Vec<u8>, base_epoch: u64, base_seq: u64, p: &Promised) {
    rec(
        buf,
        T_HEADER,
        &[
            &MAGIC.to_le_bytes(),
            &base_epoch.to_le_bytes(),
            &base_seq.to_le_bytes(),
            &p.epoch.to_le_bytes(),
            p.leader.as_bytes(),
        ],
    );
}

/// The promise in force, as the records give it.
#[derive(Clone, Debug, Default)]
struct Promised {
    epoch: u64,
    leader: String,
}

impl Promised {
    fn raise(&mut self, epoch: u64, leader: &[u8]) {
        if epoch > self.epoch || (epoch == self.epoch && self.leader.is_empty()) {
            self.epoch = epoch;
            self.leader = String::from_utf8_lossy(leader).into_owned();
        }
    }
}

fn u64_at(b: &[u8], i: usize) -> u64 {
    u64::from_le_bytes(b[i..i + 8].try_into().expect("8 bytes"))
}

/// The next record in `b` at `off`: `(type, payload range, next offset)`,
/// or `None` where the valid records end.
fn next_rec(b: &[u8], off: usize) -> Option<(u8, std::ops::Range<usize>, usize)> {
    if b.len() < off + REC_HEAD {
        return None;
    }
    let len = u32::from_le_bytes(b[off..off + 4].try_into().expect("4 bytes")) as usize;
    let crc = u32::from_le_bytes(b[off + 4..off + 8].try_into().expect("4 bytes"));
    if len == 0 || len > MAX_REC || b.len() < off + 8 + len {
        return None;
    }
    let body = &b[off + 8..off + 8 + len];
    if crc32fast::hash(body) != crc {
        return None;
    }
    Some((body[0], off + REC_HEAD..off + 8 + len, off + 8 + len))
}

fn fsync_dir(dir: &Path) -> std::io::Result<()> {
    File::open(dir)?.sync_all()
}

/// Whether the previous run wrote in page-cache mode and the machine went
/// down since (another boot, or an emulated power cut); records this run's
/// boot and mode for the next one.
fn power_lost_since_last_run(dir: &Path, opts: &Options) -> anyhow::Result<bool> {
    let now = boot_id();
    let prev = std::fs::read_to_string(dir.join(BOOT_FILE)).unwrap_or_default();
    let mut it = prev.split_whitespace();
    let (prev_boot, prev_mode) = (it.next().unwrap_or(""), it.next().unwrap_or(""));
    let cut = dir.join(POWER_LOST).exists();
    let lost = prev_mode == "page-cache" && (cut || (!prev_boot.is_empty() && prev_boot != now));
    if lost && opts.trust_after_power_loss {
        tracing::warn!(dir = %dir.display(), "qlog commitlog: a power loss in page-cache mode, trusted anyway (mutation test)");
    }
    let mut f = File::create(dir.join(BOOT_FILE))?;
    f.write_all(format!("{now} {}\n", opts.sync.name()).as_bytes())?;
    f.sync_all()?;
    if cut {
        std::fs::remove_file(dir.join(POWER_LOST))?;
    }
    fsync_dir(dir)?;
    Ok(lost && !opts.trust_after_power_loss)
}

impl CommitLog {
    /// Opens (or creates) the commitlog in `dir` and replays it.
    pub fn open(dir: &Path, opts: Options) -> anyhow::Result<(Arc<CommitLog>, Recovered)> {
        let t = Instant::now();
        std::fs::create_dir_all(dir)?;
        let power_lost = power_lost_since_last_run(dir, &opts)?;
        let mut nos: Vec<u64> = std::fs::read_dir(dir)?
            .filter_map(|e| e.ok())
            .filter_map(|e| e.file_name().to_str()?.strip_suffix(".qlog")?.parse().ok())
            .collect();
        nos.sort_unstable();
        let mut index = Index::default();
        let mut segs: Vec<Seg> = Vec::new();
        let mut commit = 0u64;
        let mut promised = Promised::default();
        let mut records = 0u64;
        let mut torn = 0u64;
        let n = nos.len();
        for (i, &no) in nos.iter().enumerate() {
            let last = i + 1 == n;
            let path = seg_path(dir, no);
            let b = std::fs::read(&path)?;
            let Some((T_HEADER, h, mut off)) = next_rec(&b, 0) else {
                // only a rollover cut short leaves a headless segment, and
                // the one before it was fsynced whole first
                anyhow::ensure!(last, "qlog commitlog: {} has no header", path.display());
                tracing::warn!(path = %path.display(), "qlog commitlog: dropping a segment cut short at creation");
                std::fs::remove_file(&path)?;
                torn += b.len() as u64;
                continue;
            };
            anyhow::ensure!(u64_at(&b, h.start) == MAGIC, "qlog commitlog: {} isn't a segment", path.display());
            let (base_epoch, base_seq) = (u64_at(&b, h.start + 8), u64_at(&b, h.start + 16));
            promised.raise(u64_at(&b, h.start + 24), &b[h.start + 32..h.end]);
            if segs.is_empty() {
                index.reset(base_epoch, base_seq);
                commit = base_seq;
            } else {
                anyhow::ensure!(
                    base_seq >= commit && index.epoch_at(base_seq) == Some(base_epoch),
                    "qlog commitlog: {} doesn't follow the segment before it",
                    path.display()
                );
                index.truncate_after(base_seq);
                commit = base_seq;
            }
            loop {
                let Some((ty, p, next)) = next_rec(&b, off) else {
                    if off < b.len() {
                        anyhow::ensure!(last, "qlog commitlog: {} is corrupt at {off}", path.display());
                        torn += (b.len() - off) as u64;
                    }
                    break;
                };
                let pl = &b[p.clone()];
                let bad = |what: &str| anyhow::anyhow!("qlog commitlog: {} at {off}: {what}", path.display());
                match ty {
                    T_APPEND | T_APPEND_C => {
                        let (epoch, seq) = (u64_at(pl, 0), u64_at(pl, 8));
                        let (head, clen) = if ty == T_APPEND_C {
                            let clen = u32::from_le_bytes(pl[16..20].try_into().expect("4 bytes")) as usize;
                            if 20 + clen > pl.len() {
                                return Err(bad("a side length past the record"));
                            }
                            (20 + clen, clen)
                        } else {
                            (16, 0)
                        };
                        let loc = Loc {
                            epoch,
                            seg: no,
                            off: (p.start + head) as u64,
                            len: (pl.len() - head) as u32,
                            clen: clen as u32,
                        };
                        if !index.push(seq, loc) {
                            return Err(bad("an entry out of order"));
                        }
                    }
                    T_TRUNCATE => {
                        let after = u64_at(pl, 0);
                        if after < commit {
                            return Err(bad("a truncation below the commit index"));
                        }
                        index.truncate_after(after);
                    }
                    T_RESET => {
                        let (epoch, seq) = (u64_at(pl, 0), u64_at(pl, 8));
                        index.reset(epoch, seq);
                        commit = seq;
                    }
                    T_PROMISE => promised.raise(u64_at(pl, 0), &pl[8..]),
                    T_COMMIT => commit = commit.max(u64_at(pl, 0).min(index.last_seq())),
                    _ => return Err(bad("an unknown record")),
                }
                records += 1;
                off = next;
            }
            if off < b.len() {
                let f = OpenOptions::new().write(true).open(&path)?;
                f.set_len(off as u64)?;
                f.sync_all()?;
                tracing::warn!(path = %path.display(), at = off, dropped = b.len() - off, "qlog commitlog: truncated a torn tail");
            }
            segs.push(Seg { no, base_seq, bytes: off as u64, durable: off as u64, file: Arc::new(File::open(&path)?) });
        }
        let fresh = segs.is_empty();
        if fresh {
            let no = nos.last().map_or(1, |n| n + 1);
            let path = seg_path(dir, no);
            let mut buf = Vec::new();
            rec_header(&mut buf, 0, 0, &promised);
            let mut f = OpenOptions::new().write(true).create_new(true).open(&path)?;
            f.write_all(&buf)?;
            f.sync_data()?;
            fsync_dir(dir)?;
            let len = buf.len() as u64;
            segs.push(Seg { no, base_seq: 0, bytes: len, durable: len, file: Arc::new(File::open(&path)?) });
        }
        // the newest entries back into memory, and every uncommitted one
        let mut from = index.last_seq();
        let mut bytes = 0usize;
        while from > index.base_seq {
            let l = index.get(from).expect("in range");
            if from <= commit && bytes + l.len as usize > opts.memory_bytes {
                break;
            }
            bytes += l.len as usize;
            from -= 1;
        }
        let entries = read_locs(&segs, &index, from + 1, index.last_seq())?;
        let base_epoch = index.epoch_at(from).expect("from is held");
        let log = Log::from_parts(base_epoch, from, entries, commit);
        let active_len = segs.last().expect("one segment").bytes;
        let cl = Arc::new(CommitLog {
            dir: dir.to_path_buf(),
            opts,
            queue: std::sync::Mutex::new(Queue::default()),
            wake: Condvar::new(),
            durable: watch::channel(Durable { ticket: 0, failed: false }).0,
            commit: AtomicU64::new(commit),
            floor: AtomicU64::new(0),
            shared: Mutex::new(Shared {
                index,
                segs,
                written_commit: commit,
                durable_len: active_len,
                written_len: active_len,
                synced_at: Instant::now(),
            }),
            halted: AtomicBool::new(false),
            writer: Mutex::new(None),
            stats: Stats::default(),
        });
        let w = Writer::new(&cl, promised.clone())?;
        let c2 = cl.clone();
        *cl.writer.lock() = Some(std::thread::Builder::new().name("qlog-commitlog".into()).spawn(move || w.run(c2))?);
        let r = Recovered {
            log,
            promised: promised.epoch,
            promised_to: Some(promised.leader).filter(|l| !l.is_empty()),
            fresh,
            power_lost: power_lost && !fresh,
            segments: n,
            records,
            torn_bytes: torn,
            took: t.elapsed(),
        };
        tracing::info!(
            dir = %dir.display(), segments = r.segments, records, torn_bytes = torn, fresh, power_lost = r.power_lost,
            base = r.log.base().1, last = r.log.last_seq(), commit = r.log.commit(), promised = r.promised,
            ms = r.took.as_millis() as u64, "qlog commitlog: recovered"
        );
        Ok((cl, r))
    }

    /// Queues `ops` behind every op staged before; returns the ticket that
    /// [`CommitLog::wait`] takes. Callers stage under the lock that orders
    /// the changes themselves, so the disk sees them in that order.
    pub fn stage(&self, ops: Vec<Op>) -> u64 {
        let mut q = self.queue.lock().expect("queue lock");
        q.ops.extend(ops);
        q.staged += 1;
        let t = q.staged;
        drop(q);
        self.wake.notify_one();
        t
    }

    pub fn staged(&self) -> u64 {
        self.queue.lock().expect("queue lock").staged
    }

    /// Until everything staged up to `ticket` is fsynced.
    pub async fn wait(&self, ticket: u64) -> anyhow::Result<()> {
        if ticket == 0 {
            return Ok(());
        }
        let mut rx = self.durable.subscribe();
        let d = *rx.wait_for(|d| d.failed || d.ticket >= ticket).await.map_err(|_| anyhow::anyhow!("closed"))?;
        anyhow::ensure!(d.ticket >= ticket, "qlog commitlog: a write or fsync failed");
        Ok(())
    }

    /// The commit index, written with the next batch.
    pub fn note_commit(&self, seq: u64) {
        self.commit.fetch_max(seq, Ordering::Release);
    }

    /// Segments wholly at or below `seq` may go (once over `retain_bytes`).
    pub fn set_floor(&self, seq: u64) {
        self.floor.fetch_max(seq, Ordering::Relaxed);
    }

    /// Entries above this are readable (until trimmed further).
    pub fn first_readable(&self) -> u64 {
        self.shared.lock().index.base_seq
    }

    /// The last seq written and in the index (readable).
    pub fn written_last(&self) -> u64 {
        self.shared.lock().index.last_seq()
    }

    pub fn disk_bytes(&self) -> u64 {
        self.shared.lock().segs.iter().map(|s| s.bytes).sum()
    }

    /// Entries from `from` to at most `upto`, up to `max_bytes` (at least
    /// one), with the epoch of `from - 1`; `None` if the disk doesn't reach
    /// back to `from - 1`. Only committed entries are asked for: they never
    /// change, so the read needs no lock past finding them.
    pub fn read(&self, from: u64, upto: u64, max_bytes: usize) -> Option<(u64, Vec<Entry>)> {
        let (prev, entries) = {
            let s = self.shared.lock();
            let prev = s.index.epoch_at(from.checked_sub(1)?)?;
            let upto = upto.min(s.index.last_seq());
            let mut n = 0usize;
            let mut to = from;
            while to <= upto {
                let l = s.index.get(to)?;
                if to > from && n + l.len as usize > max_bytes {
                    break;
                }
                n += l.len as usize;
                to += 1;
            }
            (prev, resolve(&s.segs, &s.index, from, to - 1).ok()?)
        };
        Some((prev, read_resolved(entries).ok()?))
    }

    /// Stops the writer; whatever is staged and not yet written is lost (a
    /// process crash: what was written stays in the page cache).
    pub fn halt(&self) {
        self.halted.store(true, Ordering::Release);
        self.wake.notify_all();
        if let Some(h) = self.writer.lock().take() {
            let _ = h.join();
        }
    }

    /// Tests and chaos: a power cut. The writer stops, older segments lose
    /// whatever was never fsynced, and the active segment loses a random
    /// part of what was written past the last fsync, ending in `garbage` (a
    /// torn record).
    pub fn power_cut(&self, keep_frac: f64, garbage: &[u8]) -> std::io::Result<()> {
        self.halt();
        let s = self.shared.lock();
        for seg in &s.segs[..s.segs.len() - 1] {
            if seg.durable < seg.bytes {
                let f = OpenOptions::new().write(true).open(seg_path(&self.dir, seg.no))?;
                f.set_len(seg.durable)?;
                f.sync_all()?;
            }
        }
        let seg = s.segs.last().expect("one segment");
        let keep = s.durable_len + ((s.written_len - s.durable_len) as f64 * keep_frac.clamp(0.0, 1.0)) as u64;
        let f = OpenOptions::new().write(true).open(seg_path(&self.dir, seg.no))?;
        f.set_len(keep)?;
        f.write_all_at(garbage, keep)?;
        f.sync_all()?;
        // the machine reboots: what the next open sees of /proc's boot id
        File::create(self.dir.join(POWER_LOST))?.sync_all()?;
        fsync_dir(&self.dir)
    }

    pub fn mode(&self) -> SyncMode {
        self.opts.sync
    }

    /// (bytes written since the last fdatasync, how long ago that was).
    pub fn unsynced(&self) -> (u64, Duration) {
        let s = self.shared.lock();
        (s.written_len.saturating_sub(s.durable_len), s.synced_at.elapsed())
    }
}

type Resolved = Vec<(u64, Loc, Arc<File>)>;

/// Where `from..=to` are, with their files held open (under the lock).
fn resolve(segs: &[Seg], index: &Index, from: u64, to: u64) -> std::io::Result<Resolved> {
    let mut out = Vec::with_capacity(to.saturating_sub(from) as usize + 1);
    for seq in from..=to {
        let l = index.get(seq).ok_or_else(|| std::io::Error::other("seq not in the index"))?;
        let seg = segs.iter().find(|s| s.no == l.seg).ok_or_else(|| std::io::Error::other("segment gone"))?;
        out.push((seq, l, seg.file.clone()));
    }
    Ok(out)
}

/// The reads themselves, outside the lock.
fn read_resolved(r: Resolved) -> std::io::Result<Vec<Entry>> {
    r.into_iter()
        .map(|(seq, l, f)| {
            let mut b = vec![0u8; (l.clen + l.len) as usize];
            f.read_exact_at(&mut b, l.off - l.clen as u64)?;
            let mut data = Bytes::from(b);
            let (cursors, meta) = decode_side(data.split_to(l.clen as usize));
            Ok(Entry { epoch: l.epoch, seq, data, cursors, meta })
        })
        .collect()
}

fn read_locs(segs: &[Seg], index: &Index, from: u64, to: u64) -> std::io::Result<Vec<Entry>> {
    read_resolved(resolve(segs, index, from, to)?)
}

struct Writer {
    file: File,
    no: u64,
    len: u64,
    promised: Promised,
    buf: Vec<u8>,
}

enum IdxOp {
    Push(u64, Loc),
    Truncate(u64),
    Reset(u64, u64),
}

impl Writer {
    fn new(cl: &CommitLog, promised: Promised) -> std::io::Result<Writer> {
        let s = cl.shared.lock();
        let seg = s.segs.last().expect("one segment");
        let file = OpenOptions::new().append(true).open(seg_path(&cl.dir, seg.no))?;
        Ok(Writer { file, no: seg.no, len: seg.bytes, promised, buf: Vec::with_capacity(8 << 20) })
    }

    fn run(mut self, cl: Arc<CommitLog>) {
        loop {
            let (ops, ticket, commit) = {
                let mut q = cl.queue.lock().expect("queue lock");
                let idle = match cl.opts.sync {
                    SyncMode::PageCache { every } => every.min(Duration::from_millis(500)),
                    SyncMode::Fsync => Duration::from_millis(500),
                };
                while q.ops.is_empty() && !cl.halted.load(Ordering::Acquire) {
                    let (g, to) = cl.wake.wait_timeout(q, idle).expect("queue lock");
                    q = g;
                    if to.timed_out() {
                        break;
                    }
                }
                if cl.halted.load(Ordering::Acquire) {
                    return;
                }
                // read before draining: every op that made entries up to
                // this commit index final was staged before it was noted
                let commit = cl.commit.load(Ordering::Acquire);
                (std::mem::take(&mut q.ops), q.staged, commit)
            };
            let r = if ops.is_empty() { self.maintain(&cl) } else { self.batch(&cl, ops, ticket, commit) };
            let r = r.and_then(|()| self.sync_due(&cl));
            if let Err(e) = r {
                tracing::error!(dir = %cl.dir.display(), "qlog commitlog: write failed, nothing more is durable: {e}");
                cl.durable.send_modify(|d| d.failed = true);
                if cl.opts.abort_on_error {
                    std::process::abort();
                }
                return;
            }
        }
    }

    fn batch(&mut self, cl: &CommitLog, ops: Vec<Op>, ticket: u64, commit: u64) -> std::io::Result<()> {
        self.buf.clear();
        let mut promise = false;
        let mut idx = Vec::with_capacity(ops.len());
        let n = ops.len();
        for op in &ops {
            match op {
                Op::Append(e) => {
                    let at = rec_append(&mut self.buf, e);
                    idx.push(IdxOp::Push(e.seq, loc_of(e, self.no, self.len + at as u64)));
                }
                Op::TruncateAfter(s) => {
                    rec(&mut self.buf, T_TRUNCATE, &[&s.to_le_bytes()]);
                    idx.push(IdxOp::Truncate(*s));
                }
                Op::Reset { epoch, seq } => {
                    rec(&mut self.buf, T_RESET, &[&epoch.to_le_bytes(), &seq.to_le_bytes()]);
                    idx.push(IdxOp::Reset(*epoch, *seq));
                }
                Op::Promise { epoch, leader } => {
                    rec(&mut self.buf, T_PROMISE, &[&epoch.to_le_bytes(), leader.as_bytes()]);
                    self.promised.raise(*epoch, leader.as_bytes());
                    promise = true;
                }
            }
        }
        drop(ops);
        self.file.write_all(&self.buf)?;
        self.len += self.buf.len() as u64;
        let mut written = self.buf.len() as u64;
        let commit_rec = {
            let mut s = cl.shared.lock();
            for op in idx {
                match op {
                    IdxOp::Push(seq, loc) => {
                        assert!(s.index.push(seq, loc), "qlog commitlog: staged entries out of order at {seq}");
                    }
                    IdxOp::Truncate(seq) => s.index.truncate_after(seq),
                    IdxOp::Reset(e, seq) => {
                        s.index.reset(e, seq);
                        s.written_commit = seq;
                    }
                }
            }
            let c = commit.min(s.index.last_seq());
            s.written_len = self.len;
            (c > s.written_commit).then(|| {
                s.written_commit = c;
                c
            })
        };
        if let Some(c) = commit_rec {
            self.buf.clear();
            rec(&mut self.buf, T_COMMIT, &[&c.to_le_bytes()]);
            self.file.write_all(&self.buf)?;
            self.len += self.buf.len() as u64;
            written += self.buf.len() as u64;
        }
        let _ = cl.stats.batch_ops.lock().record(n.max(1) as u64);
        // a promise is fsynced in every mode: a power cut must not let the
        // node promise an older epoch again
        if cl.opts.sync == SyncMode::Fsync || promise {
            self.sync(cl, false)?;
        } else {
            let mut s = cl.shared.lock();
            s.written_len = self.len;
            if let Some(seg) = s.segs.last_mut() {
                seg.bytes = self.len;
            }
        }
        cl.stats.bytes.fetch_add(written, Ordering::Relaxed);
        cl.durable.send_modify(|d| d.ticket = d.ticket.max(ticket));
        self.maintain(cl)
    }

    fn sync(&mut self, cl: &CommitLog, background: bool) -> std::io::Result<()> {
        if let Some(d) = cl.opts.sync_delay {
            std::thread::sleep(d);
        }
        let t = Instant::now();
        self.file.sync_data()?;
        let us = t.elapsed().as_micros().max(1) as u64;
        let _ = cl.stats.fsync_us.lock().record(us);
        cl.stats.fsyncs.fetch_add(1, Ordering::Relaxed);
        if background {
            cl.stats.background_syncs.fetch_add(1, Ordering::Relaxed);
        }
        let mut s = cl.shared.lock();
        s.written_len = self.len;
        s.durable_len = self.len;
        s.synced_at = Instant::now();
        if let Some(seg) = s.segs.last_mut() {
            seg.bytes = self.len;
        }
        Ok(())
    }

    /// Page-cache mode: the writes since the last fdatasync, synced once
    /// they're `every` old.
    fn sync_due(&mut self, cl: &CommitLog) -> std::io::Result<()> {
        let SyncMode::PageCache { every } = cl.opts.sync else { return Ok(()) };
        let due = {
            let s = cl.shared.lock();
            s.written_len > s.durable_len && s.synced_at.elapsed() >= every
        };
        if due { self.sync(cl, true) } else { Ok(()) }
    }

    /// Rollover and deleting old segments, off the ack path.
    fn maintain(&mut self, cl: &CommitLog) -> std::io::Result<()> {
        if self.len >= cl.opts.segment_bytes {
            self.roll(cl)?;
        }
        let mut s = cl.shared.lock();
        let floor = cl.floor.load(Ordering::Relaxed).min(s.written_commit);
        let mut total: u64 = s.segs.iter().map(|g| g.bytes).sum();
        let mut n = 0;
        while s.segs.len() > 1 && total > cl.opts.retain_bytes && s.segs[1].base_seq <= floor {
            if n > 0
                && let Some(h) = &cl.opts.mid_trim
            {
                (h.0)();
            }
            n += 1;
            let g = s.segs.remove(0);
            let next_base = s.segs[0].base_seq;
            s.index.drop_upto(next_base);
            total -= g.bytes;
            std::fs::remove_file(seg_path(&cl.dir, g.no))?;
            cl.stats.deleted.fetch_add(1, Ordering::Relaxed);
        }
        Ok(())
    }

    /// A new segment starting at the commit index, with the uncommitted
    /// tail written again so the new segment stands on its own.
    fn roll(&mut self, cl: &CommitLog) -> std::io::Result<()> {
        let (base, base_epoch, tail) = {
            let s = cl.shared.lock();
            let base = s.written_commit.clamp(s.index.base_seq, s.index.last_seq());
            if s.index.bytes_after(base) > cl.opts.segment_bytes / 2 {
                // a long uncommitted tail (an isolated node): roll later
                return Ok(());
            }
            let epoch = s.index.epoch_at(base).expect("base is held");
            (base, epoch, resolve(&s.segs, &s.index, base + 1, s.index.last_seq())?)
        };
        // page-cache mode: the new segment repeats only the uncommitted
        // tail, so the committed entries past the last fsync are only here
        let unsynced = {
            let s = cl.shared.lock();
            s.written_len > s.durable_len
        };
        if unsynced {
            self.sync(cl, false)?;
        }
        // only this thread changes the index, so the tail can't move meanwhile
        let tail = read_resolved(tail)?;
        let no = self.no + 1;
        let path = seg_path(&cl.dir, no);
        self.buf.clear();
        rec_header(&mut self.buf, base_epoch, base, &self.promised);
        let mut idx = Vec::with_capacity(tail.len());
        for e in &tail {
            let at = rec_append(&mut self.buf, e);
            idx.push((e.seq, loc_of(e, no, at as u64)));
        }
        let mut f = OpenOptions::new().append(true).create_new(true).open(&path)?;
        f.write_all(&self.buf)?;
        f.sync_data()?;
        fsync_dir(&cl.dir)?;
        let len = self.buf.len() as u64;
        {
            let mut s = cl.shared.lock();
            s.index.truncate_after(base);
            for (seq, loc) in idx {
                assert!(s.index.push(seq, loc), "qlog commitlog: rollover tail out of order");
            }
            let durable = s.durable_len;
            if let Some(old) = s.segs.last_mut() {
                old.durable = durable;
            }
            s.segs.push(Seg { no, base_seq: base, bytes: len, durable: len, file: Arc::new(File::open(&path)?) });
            s.written_len = len;
            s.durable_len = len;
            s.synced_at = Instant::now();
        }
        self.file = f;
        self.no = no;
        self.len = len;
        cl.stats.rollovers.fetch_add(1, Ordering::Relaxed);
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn e(epoch: u64, seq: u64, n: usize) -> Entry {
        Entry::new(epoch, seq, Bytes::from(vec![(seq % 251) as u8; n]))
    }

    fn opts() -> Options {
        Options { segment_bytes: 4096, retain_bytes: 1 << 30, memory_bytes: 1 << 30, ..Options::default() }
    }

    async fn apply(cl: &CommitLog, log: &mut Log, f: impl FnOnce(&mut Log)) {
        f(log);
        let t = cl.stage(log.take_journal());
        cl.wait(t).await.unwrap();
    }

    fn same(a: &Log, b: &Log) {
        assert_eq!(a.last(), b.last());
        let (_, base) = b.base();
        for s in base + 1..=a.last_seq() {
            assert_eq!(a.get(s), b.get(s), "entry {s}");
        }
    }

    #[tokio::test]
    async fn replays_appends_truncations_restamps_and_rollovers() {
        let dir = tempfile::tempdir().unwrap();
        let mut log = Log::new();
        log.journal();
        {
            let (cl, r) = CommitLog::open(dir.path(), opts()).unwrap();
            assert!(r.fresh);
            for round in 0..20u64 {
                apply(&cl, &mut log, |l| {
                    for _ in 0..5 {
                        l.append(round + 1, Bytes::from(vec![round as u8; 300]));
                    }
                })
                .await;
                // the last two stay uncommitted, then a new epoch takes over
                let c = log.last_seq() - 2;
                log.set_commit(c);
                cl.note_commit(c);
                apply(&cl, &mut log, |l| {
                    let c = l.commit();
                    l.truncate_after(c + 1);
                    l.restamp_after(c, round + 2);
                })
                .await;
            }
            apply(&cl, &mut log, |l| l.record_promise(99, "")).await;
            apply(&cl, &mut log, |l| l.record_promise(99, "n2")).await;
            apply(&cl, &mut log, |l| l.record_promise(98, "n3")).await;
            assert!(cl.stats.rollovers.load(Ordering::Relaxed) > 3);
            cl.halt();
        }
        let (cl, r) = CommitLog::open(dir.path(), opts()).unwrap();
        assert!(!r.fresh);
        assert_eq!((r.promised, r.promised_to.as_deref()), (99, Some("n2")));
        assert_eq!(r.torn_bytes, 0);
        same(&log, &r.log);
        assert!(r.log.commit() <= log.commit() && r.log.commit() >= log.commit() - 5);
        // a read from disk sees the same entries and epochs
        let (prev, got) = cl.read(3, log.commit(), 1 << 20).unwrap();
        assert_eq!(prev, log.epoch_at(2).unwrap());
        assert_eq!(got.len() as u64, log.commit() - 2);
        for g in got {
            assert_eq!(Some(&g), log.get(g.seq));
        }
        cl.halt();
    }

    #[tokio::test]
    async fn a_torn_tail_is_cut_and_fsynced_ops_survive() {
        for (keep, garbage) in [(0.0, &b""[..]), (0.5, &b"\x40\x00\x00\x00junk"[..]), (1.0, &[0u8; 7][..])] {
            let dir = tempfile::tempdir().unwrap();
            let mut log = Log::new();
            log.journal();
            let (cl, _) = CommitLog::open(dir.path(), opts()).unwrap();
            for s in 1..=10 {
                log.try_append(u64::from(s > 1), s - 1, vec![e(1, s, 100)]).unwrap();
            }
            let t = cl.stage(log.take_journal());
            cl.wait(t).await.unwrap();
            // written, maybe not fsynced: stage and cut the power right away
            for s in 11..=20 {
                log.try_append(1, s - 1, vec![e(1, s, 100)]).unwrap();
            }
            cl.stage(log.take_journal());
            cl.power_cut(keep, garbage).unwrap();
            let (cl, r) = CommitLog::open(dir.path(), opts()).unwrap();
            assert!(r.log.last_seq() >= 10, "fsynced entries lost: {:?}", r.log.last());
            for s in 1..=r.log.last_seq() {
                assert_eq!(r.log.get(s), log.get(s));
            }
            // and it appends after the cut
            let mut l2 = r.log;
            l2.journal();
            let n = l2.last_seq() + 1;
            l2.append(2, Bytes::from_static(b"after"));
            let t = cl.stage(l2.take_journal());
            cl.wait(t).await.unwrap();
            cl.halt();
            let (cl, r) = CommitLog::open(dir.path(), opts()).unwrap();
            assert_eq!(r.log.get(n).unwrap().data, Bytes::from_static(b"after"));
            assert_eq!(r.torn_bytes, 0);
            cl.halt();
        }
    }

    #[tokio::test]
    async fn old_segments_go_once_below_the_floor() {
        let dir = tempfile::tempdir().unwrap();
        let o = Options { segment_bytes: 4096, retain_bytes: 16 << 10, memory_bytes: 8 << 10, ..Options::default() };
        let mut log = Log::new();
        log.journal();
        let (cl, _) = CommitLog::open(dir.path(), o.clone()).unwrap();
        for _ in 0..400 {
            log.append(1, Bytes::from(vec![7u8; 500]));
            log.set_commit(log.last_seq());
            cl.note_commit(log.commit());
            let t = cl.stage(log.take_journal());
            cl.wait(t).await.unwrap();
        }
        assert!(cl.disk_bytes() > 100 << 10, "nothing deleted below no floor");
        cl.set_floor(300);
        log.append(1, Bytes::from(vec![7u8; 500]));
        let t = cl.stage(log.take_journal());
        cl.wait(t).await.unwrap();
        assert!(cl.disk_bytes() < 100 << 10);
        assert!(cl.stats.deleted.load(Ordering::Relaxed) > 0);
        assert!(cl.read(2, 300, 1 << 20).is_none(), "the deleted head still reads");
        cl.halt();
        let (cl, r) = CommitLog::open(dir.path(), o).unwrap();
        assert_eq!(r.log.last_seq(), 401);
        assert!(r.log.bytes() <= 8 << 10 || r.log.base().1 >= r.log.commit());
        let from = r.log.base().1 - 5;
        let (_, got) = cl.read(from, 400, 1 << 20).unwrap();
        assert_eq!(got.len() as u64, 400 - from + 1);
        cl.halt();
    }

    /// A crash between two deletions of one trim pass: what's left is a
    /// suffix of the segments, which opens and reads like any other.
    #[tokio::test]
    async fn a_crash_mid_trim_leaves_a_readable_suffix() {
        let dir = tempfile::tempdir().unwrap();
        let mut o = Options { segment_bytes: 4096, retain_bytes: 8 << 10, memory_bytes: 8 << 10, ..Options::default() };
        let mut log = Log::new();
        log.journal();
        let (cl, _) = CommitLog::open(dir.path(), o.clone()).unwrap();
        for _ in 0..200 {
            log.append(1, Bytes::from(vec![7u8; 500]));
            log.set_commit(log.last_seq());
            cl.note_commit(log.commit());
            let t = cl.stage(log.take_journal());
            cl.wait(t).await.unwrap();
        }
        cl.halt();
        let before = std::fs::read_dir(dir.path()).unwrap().count();
        o.mid_trim = Some(Hook(Arc::new(|| panic!("crash mid-trim"))));
        let (cl, _) = CommitLog::open(dir.path(), o.clone()).unwrap();
        cl.set_floor(150);
        log.append(1, Bytes::from(vec![7u8; 500]));
        cl.stage(log.take_journal());
        let t = Instant::now();
        while std::fs::read_dir(dir.path()).unwrap().count() == before {
            assert!(t.elapsed() < Duration::from_secs(5), "nothing was trimmed");
            std::thread::sleep(Duration::from_millis(5));
        }
        let left = std::fs::read_dir(dir.path()).unwrap().count();
        o.mid_trim = None;
        let (cl2, r) = CommitLog::open(dir.path(), o).unwrap();
        assert_eq!(left, before - 1, "the hook stops it after one deletion");
        assert!(r.log.last_seq() >= 200);
        let from = cl2.first_readable() + 1;
        let (_, got) = cl2.read(from, 200, 1 << 30).unwrap();
        assert_eq!(got.last().unwrap().seq, 200);
        cl2.halt();
        drop(cl);
    }

    fn page_cache(every_ms: u64) -> Options {
        Options { sync: SyncMode::PageCache { every: Duration::from_millis(every_ms) }, ..opts() }
    }

    /// Page-cache mode acks once written, before any fsync, and syncs in
    /// the background on its interval.
    #[tokio::test]
    async fn page_cache_acks_before_the_fsync_and_syncs_on_its_interval() {
        let dir = tempfile::tempdir().unwrap();
        let o = Options { sync_delay: Some(Duration::from_millis(300)), ..page_cache(600) };
        let (cl, r) = CommitLog::open(dir.path(), o).unwrap();
        let mut log = r.log;
        log.journal();
        let t = Instant::now();
        apply(&cl, &mut log, |l| {
            l.append(1, Bytes::from_static(b"one"));
        })
        .await;
        assert!(t.elapsed() < Duration::from_millis(200), "waited for an fsync: {:?}", t.elapsed());
        assert!(cl.unsynced().0 > 0);
        let t = Instant::now();
        while cl.unsynced().0 > 0 {
            assert!(t.elapsed() < Duration::from_secs(3), "no background fsync");
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
        assert!(cl.stats.background_syncs.load(Ordering::Relaxed) >= 1);
        // a promise is fsynced before it counts, in every mode
        let t = Instant::now();
        apply(&cl, &mut log, |l| l.record_promise(5, "n1")).await;
        assert!(t.elapsed() >= Duration::from_millis(300), "a promise counted before its fsync");
        assert_eq!(cl.unsynced().0, 0);
    }

    /// After a power cut in page-cache mode the next open says so (the log
    /// may have lost acked writes); after one in fsync mode, or a process
    /// crash, it doesn't. A reboot (another boot id) counts as a power
    /// loss after page-cache writes.
    #[tokio::test]
    async fn a_power_loss_after_page_cache_writes_is_noticed_at_open() {
        for (o, cut, lost) in [
            (page_cache(10_000), true, true),
            (page_cache(10_000), false, false),
            (opts(), true, false),
            (Options { trust_after_power_loss: true, ..page_cache(10_000) }, true, false),
        ] {
            let dir = tempfile::tempdir().unwrap();
            let (cl, r) = CommitLog::open(dir.path(), o.clone()).unwrap();
            assert!(!r.power_lost);
            let mut log = r.log;
            log.journal();
            for i in 0..5u8 {
                apply(&cl, &mut log, |l| {
                    l.append(1, Bytes::from(vec![i; 100]));
                })
                .await;
            }
            if cut {
                cl.power_cut(0.0, &[1, 2, 3]).unwrap();
            } else {
                cl.halt();
            }
            drop(cl);
            let (_, r) = CommitLog::open(dir.path(), o.clone()).unwrap();
            assert_eq!(r.power_lost, lost, "{:?} cut {cut}", o.sync);
            if o.sync == SyncMode::Fsync || !cut {
                assert_eq!(r.log.last_seq(), 5, "lost entries without a power loss");
            }
        }
        let dir = tempfile::tempdir().unwrap();
        let (cl, r) = CommitLog::open(dir.path(), page_cache(10_000)).unwrap();
        let mut log = r.log;
        log.journal();
        apply(&cl, &mut log, |l| {
            l.append(1, Bytes::from_static(b"x"));
        })
        .await;
        cl.halt();
        drop(cl);
        std::fs::write(dir.path().join(BOOT_FILE), "another-boot page-cache\n").unwrap();
        let (_, r) = CommitLog::open(dir.path(), page_cache(10_000)).unwrap();
        assert!(r.power_lost, "a reboot after page-cache writes went unnoticed");
    }

    /// A rollover in page-cache mode fsyncs what the old segment holds past
    /// its last fsync: the new segment starts at the commit index, so those
    /// committed entries are nowhere else, and a power cut that took them
    /// would leave a log that no longer opens.
    #[tokio::test]
    async fn a_power_cut_after_a_page_cache_rollover_keeps_the_rolled_segments() {
        let dir = tempfile::tempdir().unwrap();
        let (cl, r) = CommitLog::open(dir.path(), page_cache(10_000)).unwrap();
        let mut log = r.log;
        log.journal();
        for _ in 0..40 {
            log.append(1, Bytes::from(vec![7u8; 500]));
            log.set_commit(log.last_seq());
            cl.note_commit(log.commit());
            let t = cl.stage(log.take_journal());
            cl.wait(t).await.unwrap();
        }
        assert!(cl.stats.rollovers.load(Ordering::Relaxed) >= 2);
        let active = cl.shared.lock().segs.last().unwrap().base_seq;
        cl.power_cut(0.0, &[1, 2, 3]).unwrap();
        drop(cl);
        let (cl, r) = CommitLog::open(dir.path(), page_cache(10_000)).unwrap();
        assert!(r.log.last_seq() >= active, "lost {}..={active}", r.log.last_seq() + 1);
        for s in 1..=r.log.last_seq() {
            assert_eq!(r.log.get(s), log.get(s));
        }
        cl.halt();
    }
}
