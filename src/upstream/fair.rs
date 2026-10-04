//! Weighted fair queueing across hosts (deficit round robin by bytes).
//!
//! Each host task pushes into its own small queue and blocks when it's full,
//! which stops it reading its socket. One scheduler task visits the hosts
//! with frames queued in turn, takes up to `quantum * weight` bytes from
//! each, and sends them into the single bounded output channel. A host that
//! bursts fills its own queue and waits for its turn; it can't push ahead of
//! anyone else's frames, so the others' latency is bounded by one round plus
//! the output channel's depth.

use crate::types::UpstreamFrame;
use parking_lot::Mutex;
use std::collections::VecDeque;
use std::sync::Arc;
use std::sync::atomic::{AtomicU32, Ordering};
use tokio::sync::{Notify, mpsc};

struct Shared {
    /// Hosts with frames queued, each at most once.
    active: Mutex<VecDeque<Arc<HostQueue>>>,
    wake: Notify,
    quantum: usize,
}

#[derive(Clone)]
pub struct FairQueue {
    shared: Arc<Shared>,
}

struct QState {
    frames: VecDeque<UpstreamFrame>,
    queued_bytes: usize,
    /// In `Shared::active` (or held by the scheduler mid-visit).
    listed: bool,
    deficit: usize,
}

pub struct HostQueue {
    state: Mutex<QState>,
    space: Notify,
    capacity: usize,
    weight: AtomicU32,
    shared: Arc<Shared>,
}

impl FairQueue {
    /// `quantum` is the bytes a weight-1 host may send per round.
    pub fn new(quantum: usize) -> FairQueue {
        FairQueue {
            shared: Arc::new(Shared {
                active: Mutex::new(VecDeque::new()),
                wake: Notify::new(),
                quantum: quantum.max(1),
            }),
        }
    }

    pub fn host_queue(&self, capacity: usize, weight: u32) -> Arc<HostQueue> {
        Arc::new(HostQueue {
            state: Mutex::new(QState { frames: VecDeque::new(), queued_bytes: 0, listed: false, deficit: 0 }),
            space: Notify::new(),
            capacity: capacity.max(1),
            weight: AtomicU32::new(weight.max(1)),
            shared: self.shared.clone(),
        })
    }

    /// Runs until `out` closes.
    pub async fn run(self, out: mpsc::Sender<UpstreamFrame>) {
        let mut batch = Vec::new();
        loop {
            let next = self.shared.active.lock().pop_front();
            let Some(hq) = next else {
                tokio::select! {
                    _ = self.shared.wake.notified() => continue,
                    _ = out.closed() => return,
                }
            };
            let requeue = {
                let mut st = hq.state.lock();
                st.deficit += self.shared.quantum * hq.weight.load(Ordering::Relaxed) as usize;
                while let Some(f) = st.frames.front() {
                    let len = f.frame.len();
                    if len > st.deficit {
                        break;
                    }
                    st.deficit -= len;
                    st.queued_bytes -= len;
                    batch.push(st.frames.pop_front().unwrap());
                }
                if st.frames.is_empty() {
                    st.listed = false;
                    st.deficit = 0;
                    false
                } else {
                    true
                }
            };
            if !batch.is_empty() {
                hq.space.notify_one();
            }
            if requeue {
                self.shared.active.lock().push_back(hq);
            }
            for f in batch.drain(..) {
                if out.send(f).await.is_err() {
                    return;
                }
            }
        }
    }
}

impl HostQueue {
    /// Waits while the queue is full.
    pub async fn push(self: &Arc<Self>, f: UpstreamFrame) {
        let mut f = Some(f);
        loop {
            {
                let mut st = self.state.lock();
                if st.frames.len() < self.capacity {
                    st.queued_bytes += f.as_ref().unwrap().frame.len();
                    st.frames.push_back(f.take().unwrap());
                    if !st.listed {
                        st.listed = true;
                        drop(st);
                        self.shared.active.lock().push_back(self.clone());
                        self.shared.wake.notify_one();
                    }
                    return;
                }
            }
            self.space.notified().await;
        }
    }

    /// Drops everything queued: frames from a socket that's gone would
    /// duplicate what its replacement replays from the durable cursor.
    pub fn clear(&self) -> usize {
        let mut st = self.state.lock();
        let n = st.frames.len();
        st.frames.clear();
        st.queued_bytes = 0;
        st.deficit = 0;
        drop(st);
        self.space.notify_one();
        n
    }

    pub fn set_weight(&self, w: u32) {
        self.weight.store(w.max(1), Ordering::Relaxed);
    }

    pub fn len(&self) -> usize {
        self.state.lock().frames.len()
    }

    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    pub fn queued_bytes(&self) -> usize {
        self.state.lock().queued_bytes
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::types::Host;
    use bytes::Bytes;

    fn f(host: &str, seq: i64, len: usize) -> UpstreamFrame {
        UpstreamFrame { host: Host(host.into()), upstream_seq: seq, frame: Bytes::from(vec![0u8; len]), epoch: 0 }
    }

    #[tokio::test]
    async fn weights_split_bytes_and_order_holds() {
        let fq = FairQueue::new(1000);
        let heavy = fq.host_queue(1000, 3);
        let light = fq.host_queue(1000, 1);
        for i in 0..300 {
            heavy.push(f("heavy", i, 100)).await;
            light.push(f("light", i, 100)).await;
        }
        let (tx, mut rx) = mpsc::channel(10_000);
        tokio::spawn(fq.run(tx));
        let mut seen = std::collections::HashMap::<String, Vec<i64>>::new();
        for _ in 0..200 {
            let fr = rx.recv().await.unwrap();
            seen.entry(fr.host.0).or_default().push(fr.upstream_seq);
        }
        // 3:1 by bytes, in per-host order
        assert_eq!(seen["heavy"].len(), 150);
        assert_eq!(seen["light"].len(), 50);
        assert!(seen.values().all(|v| v.windows(2).all(|w| w[1] == w[0] + 1)));
    }

    #[tokio::test]
    async fn full_queue_blocks_the_producer() {
        let fq = FairQueue::new(1000);
        let q = fq.host_queue(2, 1);
        q.push(f("a", 1, 10)).await;
        q.push(f("a", 2, 10)).await;
        let q2 = q.clone();
        let blocked = tokio::spawn(async move { q2.push(f("a", 3, 10)).await });
        tokio::time::sleep(std::time::Duration::from_millis(20)).await;
        assert!(!blocked.is_finished());
        assert_eq!(q.clear(), 2);
        blocked.await.unwrap();
        assert_eq!(q.len(), 1);
    }
}
