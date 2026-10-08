//! The seeds on local disk: each DID's row in a fixed 49-byte record, in an
//! on-disk hash of 4 KiB pages, so a lookup is one `pread` and the node
//! holds nothing per DID in memory.
//!
//! A did:plc id is 120 bits of a sha256, so the id itself is the hash: its
//! first 10 bits pick one of [`SHARDS`] files, the next 56 are the record's
//! tag, and the tag's top 32 bits pick the page. Placement then follows the
//! seed database's key order (`p` + the id's bytes), which lets a full
//! build stream the database's scan into one shard at a time. Other DIDs
//! (did:web rows from lookups) are placed by a sha256 of the DID.
//!
//! Two DIDs with the same 66 bits share a record. The cost is the one a
//! stale seed has: the signature fails against the wrong key and the cache
//! refreshes from PLC. At 457M DIDs a collision anywhere is a ~1% event.
//!
//! A page that fills spills into the next ([`MAX_PROBE`] at most). A record
//! that finds no slot is dropped and its DID resolves as a miss: that bounds
//! what DIDs ground to one page can do (a few million sha256s each) to the
//! pages they land on. A shard grows by [`GROW`] once it's [`GROW_AT`]
//! full, rewriting only itself.
//!
//! Crash safety is the cache's: a page carries a CRC and one that fails
//! reads as empty (its DIDs fall back to PLC), and the follower's cursor
//! ([`Meta`]) is written only after [`SeedTable::sync`], so a restart
//! replays the changelog past it.

use super::Seed;
use bytes::Bytes;
use parking_lot::RwLock;
use std::collections::HashMap;
use std::fs::{File, OpenOptions};
use std::io::{self, Write};
use std::os::unix::fs::FileExt;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering::Relaxed};

pub const SHARDS: usize = 1024;
const SHARD_BITS: u32 = 10;
pub const PAGE: usize = 4096;
const PAGE_HDR: usize = 16;
pub const REC: usize = 49;
pub const SLOTS: usize = (PAGE - PAGE_HDR) / REC;
/// A shard file's first page is its header; pages follow.
const FILE_HDR: u64 = PAGE as u64;
/// Pages a lookup or an insert walks past its own.
pub const MAX_PROBE: u32 = 8;
/// A build fills shards to this share of their slots.
pub const BUILD_LOAD: f64 = 0.85;
/// A shard grows once this full...
pub const GROW_AT: f64 = 0.95;
/// ...by this factor (back to ~0.83).
pub const GROW: f64 = 1.15;

const MAGIC: &[u8; 8] = b"vlseedt1";
const P_OVERFLOW: u8 = 1;

const F_TOMBSTONE: u8 = 1;
const F_KEY: u8 = 2;
const F_PDS: u8 = 4;
const F_PDS_HTTP: u8 = 8;
const F_LOOKUP: u8 = 16;
const F_P256: u8 = 32;
const F_ODD: u8 = 64;

const K256_PREFIX: [u8; 2] = [0xe7, 0x01];
const P256_PREFIX: [u8; 2] = [0x80, 0x24];
const MAX_HOSTS: u32 = (1 << 24) - 1;
const TAG_MASK: u64 = (1 << 56) - 1;

/// Where a DID's record lives.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct Place {
    pub shard: usize,
    /// 56 bits.
    pub tag: u64,
}

/// `key` as the seed database spells it ([`super::seed_key`]).
pub fn place_key(key: &[u8]) -> Option<Place> {
    let bits: [u8; 9] = match key.split_first() {
        Some((b'p', id)) if id.len() == 15 => id[..9].try_into().ok()?,
        Some((b'w', _)) => {
            use sha2::Digest;
            sha2::Sha256::digest(key)[..9].try_into().ok()?
        }
        _ => return None,
    };
    let hi = u64::from_be_bytes(bits[..8].try_into().ok()?);
    let shard = (hi >> (64 - SHARD_BITS)) as usize;
    // the 56 bits after the shard's 10: the rest of `hi` and 2 of the ninth byte
    let tag = ((hi << SHARD_BITS) >> 8 | (bits[8] as u64 >> (8 - (SHARD_BITS - 8)))) & TAG_MASK;
    Some(Place { shard, tag })
}

pub fn place(did: &str) -> Option<Place> {
    place_key(&super::seed_key(did))
}

/// The page of `pages` a tag lives at: monotonic in the tag, so key order
/// is page order.
fn home(tag: u64, pages: u32) -> u32 {
    (((tag >> 24) * pages as u64) >> 32) as u32
}

#[derive(Default, Debug)]
pub struct Stats {
    pub lookups: AtomicU64,
    pub found: AtomicU64,
    /// Lookups that read past their home page.
    pub probed: AtomicU64,
    pub inserted: AtomicU64,
    pub replaced: AtomicU64,
    /// Rows older than the record they met.
    pub kept: AtomicU64,
    /// Rows with no free slot within [`MAX_PROBE`] pages.
    pub dropped: AtomicU64,
    /// Pages whose CRC failed, read as empty.
    pub corrupt: AtomicU64,
    pub grown: AtomicU64,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Put {
    Inserted,
    Replaced,
    Kept,
    Dropped,
}

struct Shard {
    file: File,
    pages: u32,
    count: u32,
    dirty: bool,
}

/// PDS hosts by number: records keep a 3-byte id. Append-only, and each new
/// name is synced before a record can point at it, so a crash can't give
/// a number to a second host.
struct Hosts {
    file: File,
    names: Vec<Arc<str>>,
    ids: HashMap<Arc<str>, u32>,
}

impl Hosts {
    fn open(path: &Path) -> io::Result<Hosts> {
        let file = OpenOptions::new().create(true).read(true).append(true).open(path)?;
        let text = std::fs::read_to_string(path)?;
        // a torn last line (a crash mid-append) was never synced, so nothing
        // points at it
        let complete = match text.rfind('\n') {
            Some(i) => &text[..=i],
            None => "",
        };
        if complete.len() != text.len() {
            file.set_len(complete.len() as u64)?;
        }
        let mut h = Hosts { file, names: vec![Arc::from("")], ids: HashMap::new() };
        for l in complete.lines() {
            let a: Arc<str> = Arc::from(l);
            h.ids.insert(a.clone(), h.names.len() as u32);
            h.names.push(a);
        }
        Ok(h)
    }

    /// `name`'s number, adding it when new. `sync`: durable before a record
    /// may point at it (a build syncs once, before its table is used).
    fn id(&mut self, name: &str, sync: bool) -> io::Result<Option<u32>> {
        if let Some(&i) = self.ids.get(name) {
            return Ok(Some(i));
        }
        let n = self.names.len() as u32;
        if n > MAX_HOSTS || name.contains('\n') || name.is_empty() {
            return Ok(None);
        }
        self.file.write_all(format!("{name}\n").as_bytes())?;
        if sync {
            self.file.sync_data()?;
        }
        let a: Arc<str> = Arc::from(name);
        self.ids.insert(a.clone(), n);
        self.names.push(a);
        Ok(Some(n))
    }

    fn name(&self, id: u32) -> Option<&str> {
        self.names.get(id as usize).map(|s| &**s).filter(|s| !s.is_empty())
    }
}

pub struct SeedTable {
    dir: PathBuf,
    shards: Vec<RwLock<Shard>>,
    hosts: RwLock<Hosts>,
    pub stats: Stats,
}

fn shard_path(dir: &Path, i: usize) -> PathBuf {
    dir.join(format!("s{i:04}.tab"))
}

fn file_header(pages: u32, count: u32) -> [u8; PAGE] {
    let mut h = [0u8; PAGE];
    h[..8].copy_from_slice(MAGIC);
    h[8..12].copy_from_slice(&pages.to_be_bytes());
    h[12..16].copy_from_slice(&count.to_be_bytes());
    let crc = crc32fast::hash(&h[..16]);
    h[16..20].copy_from_slice(&crc.to_be_bytes());
    h
}

fn read_header(f: &File) -> io::Result<(u32, u32)> {
    let mut h = [0u8; 20];
    f.read_exact_at(&mut h, 0)?;
    let crc = u32::from_be_bytes(h[16..20].try_into().unwrap());
    if &h[..8] != MAGIC || crc32fast::hash(&h[..16]) != crc {
        return Err(io::Error::new(io::ErrorKind::InvalidData, "not a seed table shard"));
    }
    Ok((u32::from_be_bytes(h[8..12].try_into().unwrap()), u32::from_be_bytes(h[12..16].try_into().unwrap())))
}

/// A page in memory.
struct Page(Box<[u8; PAGE]>);

impl Page {
    fn empty() -> Page {
        Page(Box::new([0u8; PAGE]))
    }

    /// From disk; a bad CRC reads as empty.
    fn checked(buf: &[u8], corrupt: &AtomicU64) -> Page {
        let mut p = Page::empty();
        p.0.copy_from_slice(&buf[..PAGE]);
        let want = u32::from_be_bytes(p.0[..4].try_into().unwrap());
        // never written: all zeros
        if want == 0 && p.0.iter().all(|&b| b == 0) {
            return p;
        }
        if crc32fast::hash(&p.0[4..]) != want || p.count() > SLOTS {
            corrupt.fetch_add(1, Relaxed);
            return Page::empty();
        }
        p
    }

    fn count(&self) -> usize {
        u16::from_be_bytes([self.0[4], self.0[5]]) as usize
    }

    fn set_count(&mut self, n: usize) {
        self.0[4..6].copy_from_slice(&(n as u16).to_be_bytes());
    }

    fn overflowed(&self) -> bool {
        self.0[6] & P_OVERFLOW != 0
    }

    fn set_overflowed(&mut self) {
        self.0[6] |= P_OVERFLOW;
    }

    fn rec(&self, i: usize) -> &[u8] {
        &self.0[PAGE_HDR + i * REC..PAGE_HDR + (i + 1) * REC]
    }

    fn rec_mut(&mut self, i: usize) -> &mut [u8] {
        &mut self.0[PAGE_HDR + i * REC..PAGE_HDR + (i + 1) * REC]
    }

    fn find(&self, tag: u64) -> Option<usize> {
        (0..self.count()).find(|&i| rec_tag(self.rec(i)) == tag)
    }

    fn push(&mut self, r: &[u8; REC]) -> bool {
        let n = self.count();
        if n >= SLOTS {
            return false;
        }
        self.rec_mut(n).copy_from_slice(r);
        self.set_count(n + 1);
        true
    }

    fn seal(&mut self) -> &[u8] {
        let crc = crc32fast::hash(&self.0[4..]);
        self.0[..4].copy_from_slice(&crc.to_be_bytes());
        &self.0[..]
    }
}

fn rec_tag(r: &[u8]) -> u64 {
    let mut b = [0u8; 8];
    b[1..].copy_from_slice(&r[..7]);
    u64::from_be_bytes(b)
}

fn rec_created(r: &[u8]) -> u64 {
    let mut b = [0u8; 8];
    b[2..].copy_from_slice(&r[7..13]);
    u64::from_be_bytes(b)
}

fn page_off(p: u32) -> u64 {
    FILE_HDR + p as u64 * PAGE as u64
}

/// Pages a shard of `n` records gets at `load`.
fn pages_for(n: u64, load: f64) -> u32 {
    ((n as f64 / (SLOTS as f64 * load)).ceil() as u64).clamp(1, u32::MAX as u64) as u32
}

/// `recs`, placed into `pages` pages by their tags (linear probing, with
/// the overflow bits a lookup follows). Records with no slot within
/// [`MAX_PROBE`] pages are returned.
fn lay_out(recs: &[[u8; REC]], pages: u32) -> (Vec<Page>, Vec<[u8; REC]>) {
    let mut out: Vec<Page> = (0..pages).map(|_| Page::empty()).collect();
    let mut left = Vec::new();
    for r in recs {
        let h = home(rec_tag(r), pages);
        let mut placed = false;
        for i in 0..=MAX_PROBE.min(pages - 1) {
            let p = ((h as u64 + i as u64) % pages as u64) as usize;
            if out[p].push(r) {
                for j in 0..i {
                    out[((h as u64 + j as u64) % pages as u64) as usize].set_overflowed();
                }
                placed = true;
                break;
            }
        }
        if !placed {
            left.push(*r);
        }
    }
    (out, left)
}

impl SeedTable {
    /// Opens the table in `dir`, or makes an empty one.
    pub fn open(dir: &Path) -> io::Result<SeedTable> {
        std::fs::create_dir_all(dir)?;
        let mut shards = Vec::with_capacity(SHARDS);
        for i in 0..SHARDS {
            let path = shard_path(dir, i);
            let file = OpenOptions::new().read(true).write(true).create(true).truncate(false).open(&path)?;
            let (pages, count) = if file.metadata()?.len() == 0 {
                file.write_all_at(&file_header(1, 0), 0)?;
                file.set_len(page_off(1))?;
                (1, 0)
            } else {
                read_header(&file)?
            };
            shards.push(RwLock::new(Shard { file, pages, count, dirty: false }));
        }
        Ok(SeedTable {
            dir: dir.to_path_buf(),
            shards,
            hosts: RwLock::new(Hosts::open(&dir.join("hosts"))?),
            stats: Stats::default(),
        })
    }

    pub fn dir(&self) -> &Path {
        &self.dir
    }

    pub fn len(&self) -> u64 {
        self.shards.iter().map(|s| s.read().count as u64).sum()
    }

    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    /// Pages across the shards, headers left out.
    pub fn pages(&self) -> u64 {
        self.shards.iter().map(|s| s.read().pages as u64).sum()
    }

    /// Bytes of shard files.
    pub fn disk_bytes(&self) -> u64 {
        self.shards.iter().map(|s| page_off(s.read().pages)).sum()
    }

    pub fn get(&self, did: &str) -> io::Result<Option<Seed>> {
        let Some(pl) = place(did) else { return Ok(None) };
        self.get_at(pl)
    }

    fn get_at(&self, pl: Place) -> io::Result<Option<Seed>> {
        self.stats.lookups.fetch_add(1, Relaxed);
        let sh = self.shards[pl.shard].read();
        let h = home(pl.tag, sh.pages);
        // the home page and the next in one read: a spill is usually one page
        let mut buf = [0u8; 2 * PAGE];
        let mut have: Option<(u32, usize)> = None;
        for i in 0..=MAX_PROBE.min(sh.pages - 1) {
            let p = ((h as u64 + i as u64) % sh.pages as u64) as u32;
            let at = match have {
                Some((first, n)) if p >= first && ((p - first) as usize) < n => (p - first) as usize * PAGE,
                _ => {
                    let n = if p + 1 < sh.pages { 2 } else { 1 };
                    sh.file.read_exact_at(&mut buf[..n * PAGE], page_off(p))?;
                    have = Some((p, n));
                    0
                }
            };
            let page = Page::checked(&buf[at..at + PAGE], &self.stats.corrupt);
            if i > 0 {
                self.stats.probed.fetch_add(1, Relaxed);
            }
            if let Some(j) = page.find(pl.tag) {
                self.stats.found.fetch_add(1, Relaxed);
                return Ok(self.decode(page.rec(j)));
            }
            if !page.overflowed() {
                break;
            }
        }
        Ok(None)
    }

    fn decode(&self, r: &[u8]) -> Option<Seed> {
        let flags = r[13];
        let key = (flags & F_KEY != 0).then(|| {
            let mut k = Vec::with_capacity(35);
            k.extend_from_slice(if flags & F_P256 != 0 { &P256_PREFIX } else { &K256_PREFIX });
            k.push(if flags & F_ODD != 0 { 3 } else { 2 });
            k.extend_from_slice(&r[14..46]);
            Bytes::from(k)
        });
        let host = u32::from_be_bytes([0, r[46], r[47], r[48]]);
        let pds = if flags & F_PDS != 0 { Some(self.hosts.read().name(host)?.to_string()) } else { None };
        Some(Seed {
            created_ms: rec_created(r),
            tombstone: flags & F_TOMBSTONE != 0,
            key,
            pds,
            pds_http: flags & F_PDS_HTTP != 0,
            lookup: flags & F_LOOKUP != 0,
        })
    }

    /// `seed` as a record. A key that isn't a compressed k256 or P-256
    /// multikey (the only ones the relay verifies with) is left out, and
    /// with it the seed's use: such a DID resolves from PLC, as it would
    /// anyway.
    fn encode(&self, tag: u64, seed: &Seed, sync: bool) -> io::Result<[u8; REC]> {
        let mut r = [0u8; REC];
        r[..7].copy_from_slice(&tag.to_be_bytes()[1..]);
        r[7..13].copy_from_slice(&seed.created_ms.min((1 << 48) - 1).to_be_bytes()[2..]);
        let mut flags = 0;
        if seed.tombstone {
            flags |= F_TOMBSTONE;
        }
        if seed.pds_http {
            flags |= F_PDS_HTTP;
        }
        if seed.lookup {
            flags |= F_LOOKUP;
        }
        if let Some(k) = &seed.key
            && k.len() == 35
            && matches!(k[2], 2 | 3)
            && (k[..2] == K256_PREFIX || k[..2] == P256_PREFIX)
        {
            flags |= F_KEY;
            if k[..2] == P256_PREFIX {
                flags |= F_P256;
            }
            if k[2] == 3 {
                flags |= F_ODD;
            }
            r[14..46].copy_from_slice(&k[3..]);
        }
        if let Some(p) = &seed.pds {
            let known = self.hosts.read().ids.get(p.as_str()).copied();
            let id = match known {
                Some(i) => Some(i),
                None => self.hosts.write().id(p, sync)?,
            };
            if let Some(id) = id {
                flags |= F_PDS;
                r[46..49].copy_from_slice(&id.to_be_bytes()[1..]);
            }
        }
        r[13] = flags;
        Ok(r)
    }

    /// Whether record `new` replaces record `old`: the newer op, with the
    /// bytes breaking a tie, so every member keeps the same one whatever
    /// order the rows came in.
    fn newer(new: &[u8; REC], old: &[u8]) -> bool {
        (rec_created(new), &new[7..]) > (rec_created(old), &old[7..])
    }

    /// Writes `seed` for `did` unless the record holds a newer one.
    pub fn put(&self, did: &str, seed: &Seed) -> io::Result<Put> {
        match place(did) {
            Some(pl) => self.put_at(pl, seed),
            None => Ok(Put::Dropped),
        }
    }

    pub fn put_key(&self, key: &[u8], seed: &Seed) -> io::Result<Put> {
        match place_key(key) {
            Some(pl) => self.put_at(pl, seed),
            None => Ok(Put::Dropped),
        }
    }

    fn put_at(&self, pl: Place, seed: &Seed) -> io::Result<Put> {
        let rec = self.encode(pl.tag, seed, true)?;
        let mut sh = self.shards[pl.shard].write();
        let h = home(pl.tag, sh.pages);
        let mut free: Option<u32> = None;
        let mut walked = Vec::new();
        let mut buf = [0u8; PAGE];
        for i in 0..=MAX_PROBE.min(sh.pages - 1) {
            let p = ((h as u64 + i as u64) % sh.pages as u64) as u32;
            sh.file.read_exact_at(&mut buf, page_off(p))?;
            let mut page = Page::checked(&buf, &self.stats.corrupt);
            if let Some(j) = page.find(pl.tag) {
                if !Self::newer(&rec, page.rec(j)) {
                    self.stats.kept.fetch_add(1, Relaxed);
                    return Ok(Put::Kept);
                }
                page.rec_mut(j).copy_from_slice(&rec);
                sh.file.write_all_at(page.seal(), page_off(p))?;
                sh.dirty = true;
                self.stats.replaced.fetch_add(1, Relaxed);
                return Ok(Put::Replaced);
            }
            let overflowed = page.overflowed();
            if free.is_none() && page.count() < SLOTS {
                free = Some(i);
            }
            walked.push((p, page));
            // past a page that never spilled the DID can't be, but a full
            // one still sends a new record on
            if !overflowed && free.is_some() {
                break;
            }
        }
        let Some(f) = free else {
            self.stats.dropped.fetch_add(1, Relaxed);
            return Ok(Put::Dropped);
        };
        for (i, (p, page)) in walked.iter_mut().enumerate() {
            let i = i as u32;
            if i < f && !page.overflowed() {
                page.set_overflowed();
            } else if i == f {
                page.push(&rec);
            } else {
                continue;
            }
            sh.file.write_all_at(page.seal(), page_off(*p))?;
        }
        sh.count += 1;
        sh.dirty = true;
        self.stats.inserted.fetch_add(1, Relaxed);
        if sh.count as f64 > sh.pages as f64 * SLOTS as f64 * GROW_AT {
            let pages = ((sh.pages as f64 * GROW).ceil() as u32).max(sh.pages + 1);
            self.regrow(pl.shard, &mut sh, pages)?;
        }
        Ok(Put::Inserted)
    }

    fn records(&self, sh: &Shard) -> io::Result<Vec<[u8; REC]>> {
        let mut recs = Vec::with_capacity(sh.count as usize);
        let mut buf = vec![0u8; PAGE * 256];
        let mut p = 0u32;
        while p < sh.pages {
            let n = (sh.pages - p).min(256) as usize;
            sh.file.read_exact_at(&mut buf[..n * PAGE], page_off(p))?;
            for k in 0..n {
                let page = Page::checked(&buf[k * PAGE..(k + 1) * PAGE], &self.stats.corrupt);
                for j in 0..page.count() {
                    recs.push(page.rec(j).try_into().unwrap());
                }
            }
            p += n as u32;
        }
        Ok(recs)
    }

    /// Rewrites shard `i` at `pages` pages, beside it and then over it.
    fn regrow(&self, i: usize, sh: &mut Shard, pages: u32) -> io::Result<()> {
        let mut recs = self.records(sh)?;
        recs.sort_unstable_by_key(|r| rec_tag(r));
        let (laid, left) = lay_out(&recs, pages);
        self.stats.dropped.fetch_add(left.len() as u64, Relaxed);
        let count = (recs.len() - left.len()) as u32;
        let tmp = self.dir.join(format!("s{i:04}.tab.tmp"));
        let file = write_shard(&tmp, laid, count, true)?;
        std::fs::rename(&tmp, shard_path(&self.dir, i))?;
        sync_dir(&self.dir)?;
        *sh = Shard { file, pages, count, dirty: false };
        self.stats.grown.fetch_add(1, Relaxed);
        Ok(())
    }

    /// Makes every write so far durable: shard headers and pages.
    pub fn sync(&self) -> io::Result<()> {
        for s in &self.shards {
            let mut sh = s.write();
            if sh.dirty {
                sh.file.write_all_at(&file_header(sh.pages, sh.count), 0)?;
                sh.file.sync_data()?;
                sh.dirty = false;
            }
        }
        Ok(())
    }
}

/// Writes a shard file; `sync` makes it durable before it returns (a build
/// syncs every shard once at the end instead).
fn write_shard(path: &Path, pages: Vec<Page>, count: u32, sync: bool) -> io::Result<File> {
    let file = OpenOptions::new().read(true).write(true).create(true).truncate(true).open(path)?;
    let n = pages.len() as u32;
    let mut w = io::BufWriter::with_capacity(1 << 20, &file);
    w.write_all(&file_header(n, count))?;
    for mut p in pages {
        w.write_all(p.seal())?;
    }
    w.flush()?;
    drop(w);
    if sync {
        file.sync_data()?;
    }
    Ok(file)
}

fn sync_dir(dir: &Path) -> io::Result<()> {
    File::open(dir)?.sync_all()
}

/// A table written from rows in the seed database's key order, one shard at
/// a time, into `dir` (which must not hold a table). did:web rows come
/// after every did:plc one in that order and land anywhere, so they're put
/// once the shards are written.
pub struct Builder {
    dir: PathBuf,
    table: SeedTable,
    shard: usize,
    recs: Vec<[u8; REC]>,
    later: Vec<(Vec<u8>, Seed)>,
    pub rows: u64,
}

impl Builder {
    pub fn new(dir: &Path) -> io::Result<Builder> {
        let table = SeedTable::open(dir)?;
        anyhow_empty(&table)?;
        Ok(Builder { dir: dir.to_path_buf(), table, shard: 0, recs: Vec::new(), later: Vec::new(), rows: 0 })
    }

    /// One row; `key` is the seed database's.
    pub fn push(&mut self, key: &[u8], seed: &Seed) -> io::Result<()> {
        self.rows += 1;
        if key.first() != Some(&b'p') {
            self.later.push((key.to_vec(), seed.clone()));
            return Ok(());
        }
        let Some(pl) = place_key(key) else { return Ok(()) };
        if pl.shard < self.shard {
            return Err(io::Error::new(io::ErrorKind::InvalidInput, "seed rows out of key order"));
        }
        while self.shard < pl.shard {
            self.flush_shard()?;
        }
        let r = self.table.encode(pl.tag, seed, false)?;
        self.recs.push(r);
        Ok(())
    }

    fn flush_shard(&mut self) -> io::Result<()> {
        let i = self.shard;
        let pages = pages_for(self.recs.len() as u64, BUILD_LOAD);
        let (laid, left) = lay_out(&self.recs, pages);
        self.table.stats.dropped.fetch_add(left.len() as u64, Relaxed);
        let count = (self.recs.len() - left.len()) as u32;
        let tmp = self.dir.join(format!("s{i:04}.tab.tmp"));
        let file = write_shard(&tmp, laid, count, false)?;
        std::fs::rename(&tmp, shard_path(&self.dir, i))?;
        *self.table.shards[i].write() = Shard { file, pages, count, dirty: true };
        self.recs.clear();
        self.shard += 1;
        Ok(())
    }

    pub fn finish(mut self) -> io::Result<SeedTable> {
        while self.shard < SHARDS {
            self.flush_shard()?;
        }
        self.table.hosts.read().file.sync_data()?;
        sync_dir(&self.dir)?;
        for (k, s) in std::mem::take(&mut self.later) {
            self.table.put_key(&k, &s)?;
        }
        self.table.sync()?;
        Ok(self.table)
    }
}

fn anyhow_empty(t: &SeedTable) -> io::Result<()> {
    if t.is_empty() {
        Ok(())
    } else {
        Err(io::Error::new(io::ErrorKind::AlreadyExists, "a build needs an empty directory"))
    }
}

/// What a member keeps beside its table: how far into the seed database's
/// changelog the table is.
#[derive(Clone, Debug, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct Meta {
    pub version: u32,
    /// The seed database the table was built from (its bucket path).
    pub source: String,
    /// Changelog rows from here on may be missing.
    pub cursor_ms: u64,
    pub built_ms: u64,
}

pub const META_VERSION: u32 = 1;

impl Meta {
    pub fn load(dir: &Path) -> Option<Meta> {
        let b = std::fs::read(dir.join("meta.json")).ok()?;
        serde_json::from_slice(&b).ok().filter(|m: &Meta| m.version == META_VERSION)
    }

    pub fn store(&self, dir: &Path) -> io::Result<()> {
        let tmp = dir.join("meta.json.tmp");
        let mut f = File::create(&tmp)?;
        f.write_all(&serde_json::to_vec(self).map_err(io::Error::other)?)?;
        f.sync_all()?;
        std::fs::rename(&tmp, dir.join("meta.json"))?;
        sync_dir(dir)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn did(i: u64) -> String {
        const B32: &[u8] = b"abcdefghijklmnopqrstuvwxyz234567";
        let mut x = i.wrapping_mul(0x9e37_79b9_7f4a_7c15) ^ 0x1234_5678_9abc_def0;
        let mut y = x.rotate_left(29).wrapping_mul(0xbf58_476d_1ce4_e5b9);
        let s: String = (0..24)
            .map(|n| {
                let v = if n < 12 { &mut x } else { &mut y };
                let c = B32[(*v & 31) as usize];
                *v >>= 5;
                c as char
            })
            .collect();
        format!("did:plc:{s}")
    }

    fn seed(i: u64, ms: u64) -> Seed {
        let even = i.is_multiple_of(2);
        let mut k = vec![if even { 0xe7 } else { 0x80 }, if even { 0x01 } else { 0x24 }];
        k.push(2 + even as u8);
        k.extend((0..32).map(|n| (i as u8).wrapping_add(n)));
        Seed {
            created_ms: ms,
            tombstone: false,
            key: Some(Bytes::from(k)),
            pds: Some(format!("pds{}.example.com", i % 7)),
            pds_http: i.is_multiple_of(5),
            lookup: i.is_multiple_of(3),
        }
    }

    #[test]
    fn a_record_reads_back_as_written() {
        let d = tempfile::tempdir().unwrap();
        let t = SeedTable::open(d.path()).unwrap();
        for i in 0..2000 {
            assert_eq!(t.put(&did(i), &seed(i, 1_700_000_000_000 + i)).unwrap(), Put::Inserted);
        }
        for i in 0..2000 {
            assert_eq!(t.get(&did(i)).unwrap(), Some(seed(i, 1_700_000_000_000 + i)), "{i}");
        }
        assert_eq!(t.get(&did(99_999)).unwrap(), None);
        let web = "did:web:example.com";
        t.put(web, &seed(7, 5)).unwrap();
        assert_eq!(t.get(web).unwrap(), Some(seed(7, 5)));
        assert_eq!(t.len(), 2001);
    }

    #[test]
    fn the_newest_row_wins_in_any_order() {
        let d = tempfile::tempdir().unwrap();
        let t = SeedTable::open(d.path()).unwrap();
        let (old, new) = (seed(1, 100), seed(2, 200));
        assert_eq!(t.put(&did(1), &new).unwrap(), Put::Inserted);
        assert_eq!(t.put(&did(1), &old).unwrap(), Put::Kept);
        assert_eq!(t.get(&did(1)).unwrap(), Some(new.clone()));
        let newer = seed(3, 300);
        assert_eq!(t.put(&did(1), &newer).unwrap(), Put::Replaced);
        assert_eq!(t.get(&did(1)).unwrap(), Some(newer));
    }

    #[test]
    fn a_key_the_relay_cant_verify_with_is_left_out() {
        let d = tempfile::tempdir().unwrap();
        let t = SeedTable::open(d.path()).unwrap();
        let mut s = seed(4, 10);
        s.key = Some(Bytes::from(vec![0xedu8, 0x01, 1, 2, 3]));
        t.put(&did(4), &s).unwrap();
        let got = t.get(&did(4)).unwrap().unwrap();
        assert_eq!(got.key, None);
        assert_eq!(got.pds, s.pds);
        let tomb = Seed { created_ms: 11, tombstone: true, key: None, pds: None, pds_http: false, lookup: false };
        t.put(&did(4), &tomb).unwrap();
        assert_eq!(t.get(&did(4)).unwrap(), Some(tomb));
    }

    #[test]
    fn it_survives_a_reopen() {
        let d = tempfile::tempdir().unwrap();
        {
            let t = SeedTable::open(d.path()).unwrap();
            for i in 0..500 {
                t.put(&did(i), &seed(i, i)).unwrap();
            }
            t.sync().unwrap();
        }
        let t = SeedTable::open(d.path()).unwrap();
        assert_eq!(t.len(), 500);
        for i in 0..500 {
            assert_eq!(t.get(&did(i)).unwrap(), Some(seed(i, i)));
        }
    }

    #[test]
    fn a_torn_page_reads_as_empty() {
        let d = tempfile::tempdir().unwrap();
        let t = SeedTable::open(d.path()).unwrap();
        let x = did(42);
        t.put(&x, &seed(42, 1)).unwrap();
        t.sync().unwrap();
        let pl = place(&x).unwrap();
        let f = OpenOptions::new().write(true).open(shard_path(d.path(), pl.shard)).unwrap();
        f.write_all_at(&[0xff; 64], page_off(0) + 100).unwrap();
        assert_eq!(t.get(&x).unwrap(), None);
        assert_eq!(t.stats.corrupt.load(Relaxed), 1);
    }

    #[test]
    fn a_build_from_key_order_matches_puts() {
        let d = tempfile::tempdir().unwrap();
        let mut rows: Vec<(Vec<u8>, Seed)> =
            (0..200_000).map(|i| (super::super::seed_key(&did(i)), seed(i, i))).collect();
        rows.push((super::super::seed_key("did:web:a.example"), seed(1, 1)));
        rows.sort_by(|a, b| a.0.cmp(&b.0));
        let mut b = Builder::new(d.path()).unwrap();
        for (k, s) in &rows {
            b.push(k, s).unwrap();
        }
        let t = b.finish().unwrap();
        assert_eq!(t.len(), 200_001);
        for i in 0..200_000 {
            assert_eq!(t.get(&did(i)).unwrap(), Some(seed(i, i)), "{i}");
        }
        assert_eq!(t.get("did:web:a.example").unwrap(), Some(seed(1, 1)));
        let load = t.len() as f64 / (t.pages() as f64 * SLOTS as f64);
        assert!(load > 0.7 && load <= BUILD_LOAD, "{load}");
        assert_eq!(t.stats.dropped.load(Relaxed), 0);
    }

    #[test]
    fn a_page_ground_full_spills_then_drops() {
        let d = tempfile::tempdir().unwrap();
        let t = SeedTable::open(d.path()).unwrap();
        // every tag at one home page of shard 0: the ids share their first
        // 42 bits (shard and page), not their tags
        let tries = SLOTS as u64 * (MAX_PROBE as u64 + 3);
        for i in 0..tries {
            t.put_at(Place { shard: 0, tag: i }, &seed(i, 1)).unwrap();
        }
        let found = (0..tries).filter(|&i| t.get_at(Place { shard: 0, tag: i }).unwrap().is_some()).count();
        assert_eq!(found as u64, t.len());
        assert_eq!(t.len(), SLOTS as u64 * (MAX_PROBE as u64 + 1));
        assert_eq!(t.stats.dropped.load(Relaxed), tries - t.len());
    }

    #[test]
    fn placement_follows_key_order() {
        let mut keys: Vec<Vec<u8>> = (0..5000).map(|i| super::super::seed_key(&did(i))).collect();
        keys.sort();
        let pl: Vec<Place> = keys.iter().map(|k| place_key(k).unwrap()).collect();
        assert!(pl.windows(2).all(|w| (w[0].shard, w[0].tag) <= (w[1].shard, w[1].tag)));
        assert!(pl.iter().all(|p| p.shard < SHARDS && p.tag <= TAG_MASK));
    }
}
