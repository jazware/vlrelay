//! The relay log and subscribeRepos end to end: in-memory store by default,
//! or S3/MinIO with VLRELAY_TEST_S3=http://host:port (bucket VLRELAY_TEST_BUCKET,
//! default "vlrelay"; minioadmin credentials).

use bytes::Bytes;
use futures::{SinkExt, StreamExt};
use std::net::SocketAddr;
use std::sync::Arc;
use std::time::Duration;
use tokio_tungstenite::tungstenite::Message;
use vlpds::store::Store;
use vlrelay::seq::{self, Event, EventMeta, LogConfig, LogError, SeqSplice};
use vlrelay::serve::{self, ServeConfig, Started};
use vlrelay::types::Host;

fn store() -> Store {
    match std::env::var("VLRELAY_TEST_S3") {
        Ok(endpoint) => {
            let cfg = vlpds::store::S3Config {
                endpoint,
                bucket: std::env::var("VLRELAY_TEST_BUCKET").unwrap_or_else(|_| "vlrelay".into()),
                access_key: "minioadmin".into(),
                secret_key: "minioadmin".into(),
                region: "us-east-1".into(),
            };
            let prefix = format!("test-{:016x}", rand::random::<u64>());
            Store::s3(&cfg, &prefix, None, 64).unwrap()
        }
        Err(_) => Store::memory(None),
    }
}

fn event(upstream_seq: i64, size: usize) -> Event {
    let did = format!("did:plc:{:024}", upstream_seq % 1000);
    let f = vlpds::events::sync_frame(&did, "3jzfcijpj2z2a", &vec![0xab; size], "2026-10-04T00:00:00.000Z");
    let mut raw = Vec::new();
    f.finish(upstream_seq, &mut raw);
    Event {
        meta: EventMeta { did, host: Host("pds.test".into()), upstream_seq, shard: 0 },
        frame: Box::new(SeqSplice::parse(Bytes::from(raw)).unwrap()),
        delta: None,
    }
}

fn batch(from: i64, n: usize, size: usize) -> Vec<Event> {
    (0..n as i64).map(|i| event(from + i, size)).collect()
}

async fn start(store: &Store, node: &str, f: impl FnOnce(&mut LogConfig, &mut ServeConfig)) -> (Started, SocketAddr) {
    let mut cfg = LogConfig::new(seq::new_log_id(node));
    cfg.linger = Duration::from_millis(5);
    let mut sc = ServeConfig::default();
    f(&mut cfg, &mut sc);
    let started = serve::start_single_node(store.clone(), cfg, sc, None, None).await.unwrap();
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let app = started.serve.router();
    tokio::spawn(async move {
        axum::serve(listener, app.into_make_service_with_connect_info::<SocketAddr>()).await.unwrap();
    });
    (started, addr)
}

#[derive(Debug, PartialEq)]
enum Got {
    Seq(i64),
    Info(String),
    Error(String),
}

fn classify(f: &[u8]) -> Got {
    if let Some(s) = seq::frame_seq(f) {
        return Got::Seq(s);
    }
    let text = String::from_utf8_lossy(f);
    for name in ["OutdatedCursor", "FutureCursor", "ConsumerTooSlow"] {
        if text.contains(name) {
            return if text.contains("#info") { Got::Info(name.into()) } else { Got::Error(name.into()) };
        }
    }
    Got::Error(text.into_owned())
}

/// Reads until a frame with seq >= `until` (or an error frame, or a
/// timeout), returning everything read.
async fn read_until(addr: SocketAddr, cursor: Option<i64>, until: i64) -> Vec<Got> {
    let url = match cursor {
        Some(c) => format!("ws://{addr}/xrpc/com.atproto.sync.subscribeRepos?cursor={c}"),
        None => format!("ws://{addr}/xrpc/com.atproto.sync.subscribeRepos"),
    };
    let (mut ws, _) = tokio_tungstenite::connect_async(url).await.unwrap();
    let mut out = Vec::new();
    let deadline = tokio::time::Instant::now() + Duration::from_secs(20);
    loop {
        let msg = tokio::select! {
            m = ws.next() => m,
            _ = tokio::time::sleep_until(deadline) => panic!("timed out; got {} frames, last {:?}", out.len(), out.last()),
        };
        match msg {
            Some(Ok(Message::Binary(b))) => {
                let g = classify(&b);
                let done = matches!(g, Got::Seq(s) if s >= until) || matches!(g, Got::Error(_));
                out.push(g);
                if done {
                    let _ = ws.close(None).await;
                    return out;
                }
            }
            Some(Ok(Message::Ping(p))) => ws.send(Message::Pong(p)).await.unwrap(),
            Some(Ok(_)) => {}
            _ => return out,
        }
    }
}

fn seqs(got: &[Got]) -> Vec<i64> {
    got.iter().filter_map(|g| if let Got::Seq(s) = g { Some(*s) } else { None }).collect()
}

/// Stream seqs are dense: the n-th event the bucket ever held is seq n.
fn dense(from: i64, to: i64) -> Vec<i64> {
    (from..=to).collect()
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn append_durable_then_served_in_order() {
    let store = store();
    let (st, addr) = start(&store, "a", |_, _| {}).await;
    st.log.append(batch(1, 1, 100)).await.unwrap();
    // a live subscriber, then many batches submitted back to back
    let reader = tokio::spawn(read_until(addr, Some(1), 1001));
    tokio::time::sleep(Duration::from_millis(100)).await;
    let mut tickets = Vec::new();
    for b in 0..20 {
        tickets.push(st.log.submit(batch(100 + b * 50, 50, 300)).await);
    }
    let mut want = Vec::new();
    for t in tickets {
        let d = t.await.unwrap();
        assert_eq!(d.seqs.len(), 50);
        want.extend(d.seqs);
    }
    assert!(want.windows(2).all(|w| w[0] < w[1]), "seqs increase in submit order");
    let last = *want.last().unwrap();
    assert!(st.log.wm.get() >= last);
    let live = reader.await.unwrap();
    assert_eq!(seqs(&live), dense(2, 1001));
    let got = read_until(addr, Some(1), 1001).await;
    assert_eq!(seqs(&got), dense(2, 1001));
    // the log in the bucket holds the same events with their merge keys and
    // metadata
    let logged = seq::read_log(&store, &st.log.log_id, 0).await.unwrap();
    assert_eq!(logged.len(), 1 + want.len());
    assert_eq!(logged[1].seq, want[0]);
    assert_eq!(logged[1].meta.upstream_seq, 100);
    assert_eq!(seq::frame_seq(&logged[1].frame), Some(want[0]));
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn cursor_resume_has_no_gaps_or_duplicates() {
    let store = store();
    // a tiny ring, so most cursors are served from the bucket
    let (st, addr) = start(&store, "b", |c, s| {
        c.max_segment_events = 64;
        s.ring_bytes = 32 << 10;
    })
    .await;
    for b in 0..30 {
        st.log.append(batch(b * 40, 40, 200)).await.unwrap();
    }
    let last = 1200;
    let mut cursors = vec![0, 1];
    cursors.extend((1..last).step_by(97));
    cursors.push(last - 1);
    for c in cursors {
        let got = read_until(addr, Some(c), last).await;
        assert_eq!(seqs(&got), dense(c + 1, last), "cursor {c}");
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn crash_restart_drops_unacked_replays_acked_never_reuses_seqs() {
    let store = store();
    let (st, _) = start(&store, "c", |c, _| {
        c.linger = Duration::from_secs(3600);
        c.max_segment_events = 100;
    })
    .await;
    // 100 events fill a segment, and 30 more sit in the open one, unacked
    let acked = st.log.append(batch(0, 100, 500)).await.unwrap().seqs;
    let pending = st.log.submit(batch(100, 30, 500)).await;
    tokio::time::sleep(Duration::from_millis(50)).await;
    st.log.halt();
    assert_eq!(pending.await.unwrap_err(), LogError::Closed);
    let old_id = st.log.log_id.to_string();

    let (st2, addr2) = start(&store, "c", |_, _| {}).await;
    assert_eq!(st2.recovered.logs.len(), 1);
    assert_eq!(st2.recovered.seq_floor, *acked.last().unwrap());
    let after = st2.log.append(batch(100, 30, 500)).await.unwrap().seqs;
    assert!(after[0] > *acked.last().unwrap());
    // the restart counts the earlier log's 100 and carries on from there
    let got = read_until(addr2, Some(0), 130).await;
    assert_eq!(seqs(&got), dense(1, 130));
    let got = read_until(addr2, Some(100), 130).await;
    assert_eq!(seqs(&got), dense(101, 130));
    assert_eq!(seq::read_log(&store, &old_id, 0).await.unwrap().len(), 100);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn close_drains_then_fences() {
    let store = store();
    let (st, _) = start(&store, "h", |c, _| c.linger = Duration::from_millis(200)).await;
    let a = st.log.submit(batch(0, 20, 100)).await;
    let b = st.log.submit(batch(20, 20, 100)).await;
    st.log.close(&store).await.unwrap();
    let (a, b) = (a.await.unwrap(), b.await.unwrap());
    assert!(a.seqs.last() < b.seqs.first());
    assert!(st.log.append(batch(40, 1, 100)).await.is_err());
    let (free, fenced) = vlpds::nodelog::first_free(&store, &st.log.log_id).await.unwrap();
    assert!(fenced);
    assert_eq!(seq::read_log(&store, &st.log.log_id, 0).await.unwrap().len(), 40);
    assert_eq!(free, b.ordinal + 1);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_second_writer_fences_the_first() {
    let store = store();
    let fatal = Arc::new(parking_lot::Mutex::new(None));
    let f2 = fatal.clone();
    let mut cfg = LogConfig::new(seq::new_log_id("d"));
    cfg.linger = Duration::from_millis(5);
    let on_fatal: Box<dyn FnOnce(&LogError) + Send> = Box::new(move |e| *f2.lock() = Some(e.clone()));
    let one = serve::start_single_node(store.clone(), cfg, ServeConfig::default(), None, Some(on_fatal)).await.unwrap();
    let a = one.log.append(batch(0, 10, 100)).await.unwrap().seqs;

    // a new incarnation fences every earlier log, the first one's next PUT fails
    let (two, addr) = start(&store, "d", |_, _| {}).await;
    let err = one.log.append(batch(10, 10, 100)).await.unwrap_err();
    assert!(matches!(err, LogError::Fenced(_)), "{err:?}");
    assert!(matches!(*fatal.lock(), Some(LogError::Fenced(_))));
    assert!(matches!(one.log.append(batch(20, 1, 100)).await, Err(LogError::Fenced(_))));
    let b = two.log.append(batch(10, 10, 100)).await.unwrap().seqs;
    assert!(a.last() < b.first());
    let got = read_until(addr, Some(0), 20).await;
    assert_eq!(seqs(&got), dense(1, 20));

    // two writers on one log id: the loser of an ordinal stops
    let id = seq::new_log_id("e");
    let (tx, _rx) = tokio::sync::mpsc::unbounded_channel();
    let mut c1 = LogConfig::new(id.clone());
    c1.linger = Duration::from_millis(1);
    let w1 = seq::NodeLog::start(store.clone(), c1, tx.clone(), None);
    let mut c2 = LogConfig::new(id);
    c2.linger = Duration::from_millis(1);
    let w2 = seq::NodeLog::start(store.clone(), c2, tx, None);
    w1.append(batch(0, 5, 100)).await.unwrap();
    assert!(matches!(w2.append(batch(0, 5, 100)).await, Err(LogError::Fenced(_))));
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn outdated_and_future_cursors() {
    let store = store();
    let (st, _) = start(&store, "f", |c, s| {
        c.max_segment_events = 10;
        s.ring_bytes = 16 << 10;
        s.seq_checkpoint_every = Duration::from_millis(5);
    })
    .await;
    let mut all = Vec::new();
    for b in 0..10 {
        all.extend(st.log.append(batch(b * 10, 10, 200)).await.unwrap().seqs);
    }
    let last = *all.last().unwrap();
    // nothing goes past the newest seq checkpoint: none yet past the end
    while seq::dense::newest(&store).await.unwrap().is_none_or(|k| k <= last) {
        tokio::time::sleep(Duration::from_millis(5)).await;
    }
    // a zero window prunes everything but each log's newest object
    let p = seq::prune(&store, &st.log.log_id, Duration::ZERO, 3).await.unwrap();
    assert_eq!(p.deleted, 3);
    let p = seq::prune(&store, &st.log.log_id, Duration::ZERO, 1000).await.unwrap();
    assert_eq!(p.deleted, 6);
    assert_eq!(seq::prune(&store, &st.log.log_id, Duration::ZERO, 1000).await.unwrap().deleted, 0);
    // and the checkpoints below what's left
    assert!(seq::dense::list(&store, None).await.unwrap().iter().all(|(k, _)| *k > all[89]));
    // a restart has only the bucket: from the oldest checkpoint left (at or
    // past the last segment's start), then live; its count carries on
    let (st, addr) = start(&store, "f", |c, s| {
        c.max_segment_events = 10;
        s.seq_checkpoint_every = Duration::from_millis(5);
    })
    .await;
    let reader = tokio::spawn(read_until(addr, Some(0), 102));
    tokio::time::sleep(Duration::from_millis(300)).await;
    st.log.append(batch(200, 2, 200)).await.unwrap();
    let got = reader.await.unwrap();
    assert_eq!(got[0], Got::Info("OutdatedCursor".into()));
    let s = seqs(&got);
    assert!(s[0] > 90 && s == dense(s[0], 102), "{s:?}");
    let got = read_until(addr, Some(100), 102).await;
    assert_eq!(seqs(&got), dense(101, 102));

    // past the head: the stream waits a moment for it, then FutureCursor
    let t = std::time::Instant::now();
    let got = read_until(addr, Some(103), i64::MAX).await;
    assert_eq!(got, vec![Got::Error("FutureCursor".into())]);
    assert!(t.elapsed() >= Duration::from_secs(1));
    // a cursor the head reaches within that moment is served
    let pending = tokio::spawn(read_until(addr, Some(103), 104));
    tokio::time::sleep(Duration::from_millis(300)).await;
    st.log.append(batch(300, 2, 200)).await.unwrap();
    assert_eq!(seqs(&pending.await.unwrap()), vec![104]);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_stalled_consumer_is_cut_off_and_others_keep_up() {
    let store = store();
    let (st, addr) = start(&store, "g", |_, s| {
        s.max_lag_bytes = 1 << 20;
        s.ring_bytes = 64 << 20;
    })
    .await;
    st.log.append(batch(0, 1, 100)).await.unwrap();
    let first = 1;
    let url = format!("ws://{addr}/xrpc/com.atproto.sync.subscribeRepos?cursor={first}");
    let (mut stalled, _) = tokio_tungstenite::connect_async(url).await.unwrap();
    // ~40 MiB while the stalled one reads nothing and a live one reads all
    let live = tokio::spawn(read_until(addr, Some(first), i64::MAX - 1));
    let t = std::time::Instant::now();
    let mut all = Vec::new();
    for b in 0..40 {
        all.extend(st.log.append(batch(b * 100, 100, 10_000)).await.unwrap().seqs);
    }
    let appended_in = t.elapsed();
    let mut got = Vec::new();
    while let Some(Ok(m)) = stalled.next().await {
        if let Message::Binary(b) = m {
            got.push(classify(&b));
        }
    }
    assert_eq!(got.last(), Some(&Got::Error("ConsumerTooSlow".into())), "after {} frames", got.len());
    assert!(seqs(&got).len() < all.len());
    live.abort();
    // a replay that starts while the merger is still emitting may fall
    // further behind than it started, which is too slow by design
    let head = 1 + all.len() as i64;
    while st.serve.firehose.last_emitted.load(std::sync::atomic::Ordering::Acquire) < head {
        tokio::time::sleep(Duration::from_millis(5)).await;
    }
    let got = read_until(addr, Some(first), 1 + all.len() as i64).await;
    assert!(seqs(&got) == dense(2, 1 + all.len() as i64), "{} frames, last {:?}", got.len(), got.last());
    assert!(appended_in < Duration::from_secs(10), "appends took {appended_in:?}");
}
