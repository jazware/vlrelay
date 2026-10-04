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

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn append_durable_then_served_in_order() {
    let store = store();
    let (st, addr) = start(&store, "a", |_, _| {}).await;
    let first = st.log.append(batch(1, 1, 100)).await.unwrap().seqs[0];
    // a live subscriber, then many batches submitted back to back
    let reader = tokio::spawn(read_until(addr, Some(first), i64::MAX - 1));
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
    reader.abort();
    let got = read_until(addr, Some(first), last).await;
    assert_eq!(seqs(&got), want);
    // the log in the bucket holds the same events with their metadata
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
    let mut all = Vec::new();
    for b in 0..30 {
        all.extend(st.log.append(batch(b * 40, 40, 200)).await.unwrap().seqs);
    }
    let last = *all.last().unwrap();
    let mut cursors = vec![0, all[0] - 1];
    cursors.extend(all.iter().step_by(97).copied());
    cursors.push(last - 1);
    for c in cursors {
        let got = read_until(addr, Some(c), last).await;
        let want: Vec<i64> = all.iter().copied().filter(|s| *s > c).collect();
        assert_eq!(seqs(&got), want, "cursor {c}");
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
    // 100 events fill a segment; 30 more sit in the open one, unacked
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
    let got = read_until(addr2, Some(0), *after.last().unwrap()).await;
    let want: Vec<i64> = acked.iter().chain(after.iter()).copied().collect();
    assert_eq!(seqs(&got), want);
    assert_eq!(seq::read_log(&store, &old_id, 0).await.unwrap().len(), 100);
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
    let got = read_until(addr, Some(0), *b.last().unwrap()).await;
    assert_eq!(seqs(&got), a.iter().chain(b.iter()).copied().collect::<Vec<_>>());

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
    let (st, addr) = start(&store, "f", |c, s| {
        c.max_segment_events = 10;
        s.ring_bytes = 16 << 10;
    })
    .await;
    let mut all = Vec::new();
    for b in 0..10 {
        all.extend(st.log.append(batch(b * 10, 10, 200)).await.unwrap().seqs);
    }
    let last = *all.last().unwrap();
    // a zero window prunes everything but each log's newest object
    let p = seq::prune(&store, &st.log.log_id, Duration::ZERO, 1000).await.unwrap();
    assert_eq!(p.deleted, 9);
    let got = read_until(addr, Some(0), last).await;
    assert_eq!(got[0], Got::Info("OutdatedCursor".into()));
    assert_eq!(seqs(&got), all[90..].to_vec());

    let future = vlpds::nodelog::seq_floor(vlpds::tid::now_micros() + 3_600_000_000);
    let got = read_until(addr, Some(future), i64::MAX).await;
    assert_eq!(got, vec![Got::Error("FutureCursor".into())]);
}
