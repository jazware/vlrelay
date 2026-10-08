//! The peer protocol: length-prefixed binary messages over TCP, each with a
//! request id its response echoes.

use super::log::Entry;
use bytes::{Buf, BufMut, Bytes, BytesMut};
use tokio::io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt};

const MAX_FRAME: usize = 256 << 20;

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Append {
    pub epoch: u64,
    pub leader: String,
    pub prev_epoch: u64,
    pub prev_seq: u64,
    pub commit: u64,
    /// The leader's last seq when it sent this: a restarted follower is
    /// whole again once it holds this much (see `Core::intact`).
    pub leader_last: u64,
    /// Start over at `(prev_epoch, prev_seq)` unless that entry matches.
    pub reset: bool,
    /// The last committed manifest's flushed seq F and reservation R, as the
    /// leader knows them: a follower keeps its log above F (a takeover
    /// flushes from there) and leads under R if it takes over.
    pub flushed: u64,
    pub reserve: u64,
    /// Bucket recoveries so far (the manifest's generation) as the leader
    /// knows it: whoever leads next answers host owners with it.
    pub generation: u64,
    pub entries: Vec<Entry>,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct AppendResp {
    pub ok: bool,
    /// The highest epoch the follower has promised.
    pub promised: u64,
    /// ok: the last seq matching the leader's log. Not ok: unused.
    pub matched: u64,
    pub commit: u64,
    pub last_seq: u64,
    pub intact: bool,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct PromiseResp {
    pub ok: bool,
    pub promised: u64,
    pub last_epoch: u64,
    pub last_seq: u64,
    pub base_seq: u64,
    pub commit: u64,
    /// The log holds everything this node ever acked (memory-only: false
    /// after a restart until it has caught up again).
    pub intact: bool,
    pub generation: u64,
}

/// One relay event submitted to the leader: the frame around its seq, and
/// what the host owner checked (opaque here; the leader's `node::Hooks`
/// read it).
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Item {
    pub prefix: Bytes,
    pub suffix: Bytes,
    pub meta: Bytes,
}

/// What became of one [`Item`].
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Outcome {
    /// Committed at this seq.
    Appended(u64),
    /// Already in the log (committed by the time this is answered): done.
    Duplicate,
    /// Dropped by a check: done too.
    Rejected(String),
    /// Not decided now (the leader's state isn't ready, an identity lookup
    /// failed): the host owner sends it again later.
    Retry(String),
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Msg {
    Append(Append),
    AppendResp(AppendResp),
    Promise {
        epoch: u64,
        from: String,
    },
    PromiseResp(PromiseResp),
    Fetch {
        epoch: u64,
        from: String,
        from_seq: u64,
        max_bytes: u64,
    },
    FetchResp {
        ok: bool,
        base_epoch: u64,
        base_seq: u64,
        last_seq: u64,
        entries: Vec<Entry>,
    },
    Ping {
        from: String,
    },
    Pong,
    /// A host owner's events, as frames with the seq left out (`prefix`,
    /// then `seq` and the leader's seq, then `suffix`).
    Submit {
        frames: Vec<(Bytes, Bytes)>,
        /// The submitter's host cursors ([`super::log::encode_cursors`]),
        /// each counting only events already acked: they ride on this
        /// submit's first entry (or the next one appended).
        cursors: Bytes,
        /// The recovery generation the submitter last rewound for: cursors
        /// from before a recovery it hasn't seen may count lost events, so
        /// the leader drops them.
        generation: u64,
    },
    Submitted {
        first: u64,
        n: u64,
        /// The leader's recovery generation: higher than the submitter's,
        /// it rewinds its hosts to `Cursors`.
        generation: u64,
    },
    /// A host owner asks where the last bucket recovery left its hosts.
    Cursors,
    CursorsResp {
        generation: u64,
        /// False: this node doesn't know that recovery's cursors yet.
        known: bool,
        cursors: Bytes,
    },
    NotLeader {
        hint: String,
    },
    Failed {
        reason: String,
    },
    /// A membership change removed its leader, which CASed `qlog/leader`
    /// to `epoch` naming the receiver: it collects promises and leads
    /// without waiting out the election timeout.
    Lead {
        epoch: u64,
        from: String,
    },
    /// A host owner's checked events, each decided by the leader's hooks
    /// before it's appended (`Node::submit_events`). `control` is the
    /// hooks' too: changes that ride the log without an event of their own.
    SubmitEvents {
        items: Vec<Item>,
        cursors: Bytes,
        control: Bytes,
        generation: u64,
    },
    SubmittedEvents {
        outcomes: Vec<Outcome>,
        generation: u64,
    },
    /// A question for a node (`status`) or its hooks (anything else).
    Ask {
        topic: String,
        body: Bytes,
    },
    Answer {
        body: Bytes,
    },
}

impl Msg {
    fn tag(&self) -> u8 {
        match self {
            Msg::Append(_) => 1,
            Msg::AppendResp(_) => 2,
            Msg::Promise { .. } => 3,
            Msg::PromiseResp(_) => 4,
            Msg::Fetch { .. } => 5,
            Msg::FetchResp { .. } => 6,
            Msg::Ping { .. } => 7,
            Msg::Pong => 8,
            Msg::Submit { .. } => 9,
            Msg::Submitted { .. } => 10,
            Msg::NotLeader { .. } => 11,
            Msg::Failed { .. } => 12,
            Msg::Cursors => 13,
            Msg::CursorsResp { .. } => 14,
            Msg::Lead { .. } => 15,
            Msg::SubmitEvents { .. } => 16,
            Msg::SubmittedEvents { .. } => 17,
            Msg::Ask { .. } => 18,
            Msg::Answer { .. } => 19,
        }
    }

    /// The sender, for peer requests (fault injection drops by it).
    pub fn from_peer(&self) -> Option<&str> {
        match self {
            Msg::Append(a) => Some(&a.leader),
            Msg::Promise { from, .. } | Msg::Fetch { from, .. } | Msg::Ping { from } | Msg::Lead { from, .. } => {
                Some(from)
            }
            _ => None,
        }
    }
}

pub fn encode(rid: u64, m: &Msg) -> Bytes {
    let mut b = BytesMut::with_capacity(64 + entries_len(m));
    b.put_u32(0);
    b.put_u8(m.tag());
    b.put_u64(rid);
    match m {
        Msg::Append(a) => {
            b.put_u64(a.epoch);
            put_str(&mut b, &a.leader);
            b.put_u64(a.prev_epoch);
            b.put_u64(a.prev_seq);
            b.put_u64(a.commit);
            b.put_u64(a.leader_last);
            b.put_u8(a.reset as u8);
            b.put_u64(a.flushed);
            b.put_u64(a.reserve);
            b.put_u64(a.generation);
            put_entries(&mut b, &a.entries);
        }
        Msg::AppendResp(r) => {
            b.put_u8(r.ok as u8);
            b.put_u64(r.promised);
            b.put_u64(r.matched);
            b.put_u64(r.commit);
            b.put_u64(r.last_seq);
            b.put_u8(r.intact as u8);
        }
        Msg::Promise { epoch, from } => {
            b.put_u64(*epoch);
            put_str(&mut b, from);
        }
        Msg::PromiseResp(r) => {
            b.put_u8(r.ok as u8);
            b.put_u64(r.promised);
            b.put_u64(r.last_epoch);
            b.put_u64(r.last_seq);
            b.put_u64(r.base_seq);
            b.put_u64(r.commit);
            b.put_u8(r.intact as u8);
            b.put_u64(r.generation);
        }
        Msg::Fetch { epoch, from, from_seq, max_bytes } => {
            b.put_u64(*epoch);
            put_str(&mut b, from);
            b.put_u64(*from_seq);
            b.put_u64(*max_bytes);
        }
        Msg::FetchResp { ok, base_epoch, base_seq, last_seq, entries } => {
            b.put_u8(*ok as u8);
            b.put_u64(*base_epoch);
            b.put_u64(*base_seq);
            b.put_u64(*last_seq);
            put_entries(&mut b, entries);
        }
        Msg::Ping { from } => put_str(&mut b, from),
        Msg::Pong => {}
        Msg::Submit { frames, cursors, generation } => {
            b.put_u32(frames.len() as u32);
            for (p, s) in frames {
                put_bytes(&mut b, p);
                put_bytes(&mut b, s);
            }
            put_bytes(&mut b, cursors);
            b.put_u64(*generation);
        }
        Msg::Submitted { first, n, generation } => {
            b.put_u64(*first);
            b.put_u64(*n);
            b.put_u64(*generation);
        }
        Msg::Cursors => {}
        Msg::CursorsResp { generation, known, cursors } => {
            b.put_u64(*generation);
            b.put_u8(*known as u8);
            put_bytes(&mut b, cursors);
        }
        Msg::NotLeader { hint } => put_str(&mut b, hint),
        Msg::Failed { reason } => put_str(&mut b, reason),
        Msg::Lead { epoch, from } => {
            b.put_u64(*epoch);
            put_str(&mut b, from);
        }
        Msg::SubmitEvents { items, cursors, control, generation } => {
            b.put_u32(items.len() as u32);
            for it in items {
                put_bytes(&mut b, &it.prefix);
                put_bytes(&mut b, &it.suffix);
                put_bytes(&mut b, &it.meta);
            }
            put_bytes(&mut b, cursors);
            put_bytes(&mut b, control);
            b.put_u64(*generation);
        }
        Msg::SubmittedEvents { outcomes, generation } => {
            b.put_u32(outcomes.len() as u32);
            for o in outcomes {
                match o {
                    Outcome::Appended(seq) => {
                        b.put_u8(0);
                        b.put_u64(*seq);
                    }
                    Outcome::Duplicate => b.put_u8(1),
                    Outcome::Rejected(r) => {
                        b.put_u8(2);
                        put_str(&mut b, r);
                    }
                    Outcome::Retry(r) => {
                        b.put_u8(3);
                        put_str(&mut b, r);
                    }
                }
            }
            b.put_u64(*generation);
        }
        Msg::Ask { topic, body } => {
            put_str(&mut b, topic);
            put_bytes(&mut b, body);
        }
        Msg::Answer { body } => put_bytes(&mut b, body),
    }
    let n = (b.len() - 4) as u32;
    b[..4].copy_from_slice(&n.to_be_bytes());
    b.freeze()
}

fn entries_len(m: &Msg) -> usize {
    match m {
        Msg::Append(a) => a.entries.iter().map(|e| e.data.len() + e.cursors.len() + e.meta.len() + 28).sum(),
        Msg::FetchResp { entries, .. } => {
            entries.iter().map(|e| e.data.len() + e.cursors.len() + e.meta.len() + 28).sum()
        }
        Msg::Submit { frames, cursors, .. } => {
            frames.iter().map(|(p, s)| p.len() + s.len() + 8).sum::<usize>() + cursors.len()
        }
        Msg::SubmitEvents { items, cursors, control, .. } => {
            items.iter().map(|i| i.prefix.len() + i.suffix.len() + i.meta.len() + 12).sum::<usize>()
                + cursors.len()
                + control.len()
        }
        Msg::Answer { body } => body.len(),
        _ => 0,
    }
}

fn put_str(b: &mut BytesMut, s: &str) {
    put_bytes(b, s.as_bytes())
}

fn put_bytes(b: &mut BytesMut, s: &[u8]) {
    b.put_u32(s.len() as u32);
    b.put_slice(s);
}

fn put_entries(b: &mut BytesMut, es: &[Entry]) {
    b.put_u32(es.len() as u32);
    for e in es {
        b.put_u64(e.epoch);
        b.put_u64(e.seq);
        put_bytes(b, &e.data);
        put_bytes(b, &e.cursors);
        put_bytes(b, &e.meta);
    }
}

struct Rd(Bytes);

type R<T> = Result<T, &'static str>;

impl Rd {
    fn need(&self, n: usize) -> R<()> {
        if self.0.remaining() < n { Err("short message") } else { Ok(()) }
    }
    fn u8(&mut self) -> R<u8> {
        self.need(1)?;
        Ok(self.0.get_u8())
    }
    fn bool(&mut self) -> R<bool> {
        Ok(self.u8()? != 0)
    }
    fn u32(&mut self) -> R<u32> {
        self.need(4)?;
        Ok(self.0.get_u32())
    }
    fn u64(&mut self) -> R<u64> {
        self.need(8)?;
        Ok(self.0.get_u64())
    }
    fn bytes(&mut self) -> R<Bytes> {
        let n = self.u32()? as usize;
        self.need(n)?;
        Ok(self.0.split_to(n))
    }
    fn string(&mut self) -> R<String> {
        String::from_utf8(self.bytes()?.to_vec()).map_err(|_| "bad utf-8")
    }
    fn entries(&mut self) -> R<Vec<Entry>> {
        let n = self.u32()? as usize;
        let mut out = Vec::with_capacity(n.min(1 << 16));
        for _ in 0..n {
            out.push(Entry {
                epoch: self.u64()?,
                seq: self.u64()?,
                data: self.bytes()?,
                cursors: self.bytes()?,
                meta: self.bytes()?,
            });
        }
        Ok(out)
    }
}

/// A message body (after the length prefix).
pub fn decode(body: Bytes) -> R<(u64, Msg)> {
    let mut r = Rd(body);
    let tag = r.u8()?;
    let rid = r.u64()?;
    let m = match tag {
        1 => Msg::Append(Append {
            epoch: r.u64()?,
            leader: r.string()?,
            prev_epoch: r.u64()?,
            prev_seq: r.u64()?,
            commit: r.u64()?,
            leader_last: r.u64()?,
            reset: r.bool()?,
            flushed: r.u64()?,
            reserve: r.u64()?,
            generation: r.u64()?,
            entries: r.entries()?,
        }),
        2 => Msg::AppendResp(AppendResp {
            ok: r.bool()?,
            promised: r.u64()?,
            matched: r.u64()?,
            commit: r.u64()?,
            last_seq: r.u64()?,
            intact: r.bool()?,
        }),
        3 => Msg::Promise { epoch: r.u64()?, from: r.string()? },
        4 => Msg::PromiseResp(PromiseResp {
            ok: r.bool()?,
            promised: r.u64()?,
            last_epoch: r.u64()?,
            last_seq: r.u64()?,
            base_seq: r.u64()?,
            commit: r.u64()?,
            intact: r.bool()?,
            generation: r.u64()?,
        }),
        5 => Msg::Fetch { epoch: r.u64()?, from: r.string()?, from_seq: r.u64()?, max_bytes: r.u64()? },
        6 => Msg::FetchResp {
            ok: r.bool()?,
            base_epoch: r.u64()?,
            base_seq: r.u64()?,
            last_seq: r.u64()?,
            entries: r.entries()?,
        },
        7 => Msg::Ping { from: r.string()? },
        8 => Msg::Pong,
        9 => {
            let n = r.u32()? as usize;
            let mut frames = Vec::with_capacity(n.min(1 << 16));
            for _ in 0..n {
                frames.push((r.bytes()?, r.bytes()?));
            }
            Msg::Submit { frames, cursors: r.bytes()?, generation: r.u64()? }
        }
        10 => Msg::Submitted { first: r.u64()?, n: r.u64()?, generation: r.u64()? },
        11 => Msg::NotLeader { hint: r.string()? },
        12 => Msg::Failed { reason: r.string()? },
        13 => Msg::Cursors,
        14 => Msg::CursorsResp { generation: r.u64()?, known: r.bool()?, cursors: r.bytes()? },
        15 => Msg::Lead { epoch: r.u64()?, from: r.string()? },
        16 => {
            let n = r.u32()? as usize;
            let mut items = Vec::with_capacity(n.min(1 << 16));
            for _ in 0..n {
                items.push(Item { prefix: r.bytes()?, suffix: r.bytes()?, meta: r.bytes()? });
            }
            Msg::SubmitEvents { items, cursors: r.bytes()?, control: r.bytes()?, generation: r.u64()? }
        }
        17 => {
            let n = r.u32()? as usize;
            let mut outcomes = Vec::with_capacity(n.min(1 << 16));
            for _ in 0..n {
                outcomes.push(match r.u8()? {
                    0 => Outcome::Appended(r.u64()?),
                    1 => Outcome::Duplicate,
                    2 => Outcome::Rejected(r.string()?),
                    3 => Outcome::Retry(r.string()?),
                    _ => return Err("unknown outcome"),
                });
            }
            Msg::SubmittedEvents { outcomes, generation: r.u64()? }
        }
        18 => Msg::Ask { topic: r.string()?, body: r.bytes()? },
        19 => Msg::Answer { body: r.bytes()? },
        _ => return Err("unknown message"),
    };
    if r.0.has_remaining() {
        return Err("trailing bytes");
    }
    Ok((rid, m))
}

pub async fn read_msg<R: AsyncRead + Unpin>(r: &mut R) -> std::io::Result<(u64, Msg)> {
    let n = r.read_u32().await? as usize;
    if n > MAX_FRAME {
        return Err(std::io::Error::new(std::io::ErrorKind::InvalidData, "qlog: frame too large"));
    }
    let mut buf = BytesMut::zeroed(n);
    r.read_exact(&mut buf).await?;
    decode(buf.freeze()).map_err(|e| std::io::Error::new(std::io::ErrorKind::InvalidData, e))
}

pub async fn write_msg<W: AsyncWrite + Unpin>(w: &mut W, rid: u64, m: &Msg) -> std::io::Result<()> {
    w.write_all(&encode(rid, m)).await?;
    w.flush().await
}

/// A frame as the leader writes it: `prefix`, the `seq` key and value, `suffix`.
pub fn splice_seq(prefix: &[u8], suffix: &[u8], seq: u64) -> Bytes {
    let mut out = Vec::with_capacity(prefix.len() + suffix.len() + 14);
    out.extend_from_slice(prefix);
    vlatproto::cbor::write_text(&mut out, "seq");
    vlatproto::cbor::write_int(&mut out, seq as i64);
    out.extend_from_slice(suffix);
    out.into()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn round_trip() {
        let ents = vec![
            Entry::new(3, 9, Bytes::from_static(b"abc")),
            Entry {
                epoch: 3,
                seq: 10,
                data: Bytes::new(),
                cursors: Bytes::from_static(b"cur"),
                meta: Bytes::from_static(b"m"),
            },
        ];
        let msgs = vec![
            Msg::Append(Append {
                epoch: 3,
                leader: "n1".into(),
                prev_epoch: 2,
                prev_seq: 8,
                commit: 7,
                leader_last: 10,
                reset: true,
                flushed: 4,
                reserve: 99,
                generation: 2,
                entries: ents.clone(),
            }),
            Msg::AppendResp(AppendResp { ok: true, promised: 3, matched: 10, commit: 7, last_seq: 10, intact: false }),
            Msg::Promise { epoch: 4, from: "n2".into() },
            Msg::PromiseResp(PromiseResp {
                ok: false,
                promised: 5,
                last_epoch: 3,
                last_seq: 10,
                base_seq: 1,
                commit: 7,
                intact: true,
                generation: 1,
            }),
            Msg::Fetch { epoch: 4, from: "n2".into(), from_seq: 8, max_bytes: 1 << 20 },
            Msg::FetchResp { ok: true, base_epoch: 1, base_seq: 2, last_seq: 10, entries: ents },
            Msg::Ping { from: "n3".into() },
            Msg::Pong,
            Msg::Submit {
                frames: vec![(Bytes::from_static(b"p"), Bytes::from_static(b"s"))],
                cursors: Bytes::from_static(b"c"),
                generation: 3,
            },
            Msg::Submitted { first: 11, n: 2, generation: 3 },
            Msg::Cursors,
            Msg::CursorsResp { generation: 3, known: true, cursors: Bytes::from_static(b"cc") },
            Msg::NotLeader { hint: "n1".into() },
            Msg::Failed { reason: "x".into() },
            Msg::Lead { epoch: 6, from: "n4".into() },
            Msg::SubmitEvents {
                items: vec![Item {
                    prefix: Bytes::from_static(b"p"),
                    suffix: Bytes::from_static(b"s"),
                    meta: Bytes::from_static(b"m"),
                }],
                cursors: Bytes::from_static(b"c"),
                control: Bytes::from_static(b"x"),
                generation: 2,
            },
            Msg::SubmittedEvents {
                outcomes: vec![
                    Outcome::Appended(7),
                    Outcome::Duplicate,
                    Outcome::Rejected("no".into()),
                    Outcome::Retry("later".into()),
                ],
                generation: 2,
            },
            Msg::Ask { topic: "status".into(), body: Bytes::from_static(b"{}") },
            Msg::Answer { body: Bytes::from_static(b"{}") },
        ];
        for (i, m) in msgs.into_iter().enumerate() {
            let b = encode(i as u64, &m);
            assert_eq!(u32::from_be_bytes(b[..4].try_into().unwrap()) as usize, b.len() - 4);
            assert_eq!(decode(b.slice(4..)).unwrap(), (i as u64, m));
            assert!(decode(b.slice(4..b.len() - 1)).is_err());
        }
    }
}
