//! A host owner's side of the log: submits batches to whichever node leads,
//! pipelined over one connection, and resends a batch until it's acked
//! (the forwarder's retry, docs/quorum.md §1 "Leader change with a quorum
//! alive").

use super::wire::{self, Msg};
use bytes::Bytes;
use parking_lot::Mutex;
use std::collections::HashMap;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicU64, AtomicUsize, Ordering};
use std::time::Duration;
use tokio::net::TcpStream;
use tokio::sync::{mpsc, oneshot};

struct Conn {
    addr: String,
    tx: mpsc::UnboundedSender<Bytes>,
    pending: Arc<Mutex<HashMap<u64, oneshot::Sender<Msg>>>>,
    rid: AtomicU64,
    dead: Arc<AtomicBool>,
}

impl Conn {
    async fn open(addr: &str) -> std::io::Result<Arc<Conn>> {
        let s = tokio::time::timeout(Duration::from_millis(500), TcpStream::connect(addr))
            .await
            .map_err(|_| std::io::Error::new(std::io::ErrorKind::TimedOut, "connect timed out"))??;
        let _ = s.set_nodelay(true);
        let (mut rd, mut wr) = s.into_split();
        let (tx, mut rx) = mpsc::unbounded_channel::<Bytes>();
        let pending: Arc<Mutex<HashMap<u64, oneshot::Sender<Msg>>>> = Default::default();
        let dead = Arc::new(AtomicBool::new(false));
        let d = dead.clone();
        tokio::spawn(async move {
            use tokio::io::AsyncWriteExt;
            while let Some(b) = rx.recv().await {
                if wr.write_all(&b).await.is_err() {
                    break;
                }
            }
            d.store(true, Ordering::Release);
        });
        let (p, d) = (pending.clone(), dead.clone());
        tokio::spawn(async move {
            while let Ok((rid, m)) = wire::read_msg(&mut rd).await {
                if let Some(w) = p.lock().remove(&rid) {
                    let _ = w.send(m);
                }
            }
            d.store(true, Ordering::Release);
            p.lock().clear();
        });
        Ok(Arc::new(Conn { addr: addr.to_string(), tx, pending, rid: AtomicU64::new(1), dead }))
    }

    async fn call(&self, m: &Msg, timeout: Duration) -> Option<Msg> {
        let rid = self.rid.fetch_add(1, Ordering::Relaxed);
        let (tx, rx) = oneshot::channel();
        self.pending.lock().insert(rid, tx);
        if self.tx.send(wire::encode(rid, m)).is_err() {
            self.pending.lock().remove(&rid);
            return None;
        }
        match tokio::time::timeout(timeout, rx).await {
            Ok(Ok(m)) => Some(m),
            _ => {
                self.pending.lock().remove(&rid);
                None
            }
        }
    }
}

pub struct Client {
    /// node id -> address
    nodes: Vec<(String, String)>,
    cur: tokio::sync::Mutex<Option<Arc<Conn>>>,
    next: AtomicUsize,
    hint: Mutex<Option<String>>,
    /// The address that last timed out, and until when it's skipped: a hung
    /// node still accepts connections, and its peers keep naming it as the
    /// leader until they've taken over.
    avoid: Mutex<Option<(String, std::time::Instant)>>,
    pub timeout: Duration,
    pub retries: AtomicU64,
}

impl Client {
    pub fn new(nodes: Vec<(String, String)>) -> Arc<Client> {
        Arc::new(Client {
            nodes,
            cur: tokio::sync::Mutex::new(None),
            next: AtomicUsize::new(0),
            hint: Mutex::new(None),
            avoid: Mutex::new(None),
            // A hung leader (stopped, or its box gone) is only noticed by this
            // timing out; the cluster takes over after ~1 s of silence, so
            // waiting much longer than that only adds to the pause.
            timeout: Duration::from_millis(1000),
            retries: AtomicU64::new(0),
        })
    }

    async fn conn(&self) -> Option<Arc<Conn>> {
        let mut g = self.cur.lock().await;
        if let Some(c) = g.as_ref()
            && !c.dead.load(Ordering::Acquire)
        {
            return Some(c.clone());
        }
        let avoid = self.avoid.lock().clone().filter(|(_, until)| std::time::Instant::now() < *until).map(|(a, _)| a);
        let hinted =
            self.hint.lock().take().and_then(|h| self.nodes.iter().find(|(id, _)| *id == h).map(|(_, a)| a.clone()));
        let addr = match hinted.filter(|a| avoid.as_ref() != Some(a)) {
            Some(a) => a,
            None => {
                let mut a = self.nodes[self.next.fetch_add(1, Ordering::Relaxed) % self.nodes.len()].1.clone();
                if avoid.as_ref() == Some(&a) {
                    a = self.nodes[self.next.fetch_add(1, Ordering::Relaxed) % self.nodes.len()].1.clone();
                }
                a
            }
        };
        let c = Conn::open(&addr).await.ok()?;
        *g = Some(c.clone());
        Some(c)
    }

    async fn drop_conn(&self, c: &Arc<Conn>) {
        let mut g = self.cur.lock().await;
        if g.as_ref().is_some_and(|x| Arc::ptr_eq(x, c)) {
            *g = None;
        }
    }

    /// Submits `frames` until a leader commits them: (first seq, count).
    pub async fn submit(&self, frames: Vec<(Bytes, Bytes)>) -> (u64, u64) {
        self.submit_with(frames, Bytes::new()).await
    }

    /// As `submit`, carrying host cursors (`log::encode_cursors`) that
    /// count only events already acked.
    pub async fn submit_with(&self, frames: Vec<(Bytes, Bytes)>, cursors: Bytes) -> (u64, u64) {
        let m = Msg::Submit { frames, cursors };
        loop {
            let Some(c) = self.conn().await else {
                self.retries.fetch_add(1, Ordering::Relaxed);
                tokio::time::sleep(Duration::from_millis(20)).await;
                continue;
            };
            match c.call(&m, self.timeout).await {
                Some(Msg::Submitted { first, n }) => return (first, n),
                Some(Msg::NotLeader { hint }) => {
                    if !hint.is_empty() {
                        *self.hint.lock() = Some(hint);
                    }
                    self.drop_conn(&c).await;
                    tokio::time::sleep(Duration::from_millis(10)).await;
                }
                Some(_) => tokio::time::sleep(Duration::from_millis(10)).await,
                None => {
                    // timed out or the connection died: try the others
                    c.dead.store(true, Ordering::Release);
                    *self.avoid.lock() = Some((c.addr.clone(), std::time::Instant::now() + 2 * self.timeout));
                    self.drop_conn(&c).await;
                }
            }
            self.retries.fetch_add(1, Ordering::Relaxed);
        }
    }
}

/// A test event: an `#identity`-shaped frame whose `did` names it, with
/// `pad` bytes to set its size and `sent` (unix µs) for end-to-end latency.
/// Returns (prefix, suffix) around the `seq` the leader splices in.
pub fn test_frame(did: &str, pad: usize, sent_us: i64) -> (Bytes, Bytes) {
    use vlpds::cbor::*;
    let mut p = Vec::with_capacity(pad + did.len() + 48);
    write_map_head(&mut p, 2);
    write_text(&mut p, "t");
    write_text(&mut p, "#identity");
    write_text(&mut p, "op");
    write_uint(&mut p, 1);
    write_map_head(&mut p, 4);
    write_text(&mut p, "did");
    write_text(&mut p, did);
    write_text(&mut p, "pad");
    write_bytes(&mut p, &vec![b'x'; pad]);
    let mut s = Vec::with_capacity(16);
    write_text(&mut s, "sent");
    write_int(&mut s, sent_us);
    (p.into(), s.into())
}

/// (seq, did, sent µs) of a [`test_frame`] as emitted; None for anything else.
pub fn parse_test_frame(frame: &[u8]) -> Option<(u64, String, i64)> {
    use vlpds::cbor::ValueRef;
    let (hdr, n) = ValueRef::decode_prefix(frame).ok()?;
    if !matches!(hdr.get("t"), Some(ValueRef::Text(t)) if *t == "#identity") {
        return None;
    }
    let body = ValueRef::decode(&frame[n..]).ok()?;
    let seq = match body.get("seq") {
        Some(ValueRef::Int(i)) => *i as u64,
        _ => return None,
    };
    let did = match body.get("did") {
        Some(ValueRef::Text(t)) => t.to_string(),
        _ => return None,
    };
    let sent = match body.get("sent") {
        Some(ValueRef::Int(i)) => *i,
        _ => 0,
    };
    Some((seq, did, sent))
}

/// The `#info` name of an info frame (`OutdatedCursor`), if it is one.
pub fn info_name(frame: &[u8]) -> Option<String> {
    use vlpds::cbor::ValueRef;
    let (hdr, n) = ValueRef::decode_prefix(frame).ok()?;
    if !matches!(hdr.get("t"), Some(ValueRef::Text(t)) if *t == "#info") {
        return None;
    }
    match ValueRef::decode(&frame[n..]).ok()?.get("name") {
        Some(ValueRef::Text(t)) => Some(t.to_string()),
        _ => None,
    }
}
