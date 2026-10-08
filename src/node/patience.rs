//! How long a failing DID lookup is waited out.
//!
//! A lookup that fails for a reason other than the DID not existing is
//! retried, holding the event: a PLC blip shouldn't cost it. The host stage
//! retries for `IDENTITY_PATIENCE`, holding its lane; the leader answers
//! "retry" and the forwarder resends until `quorum::GIVE_UP`, then replays
//! the host. A DID whose document stays unreachable (a dead did:web's
//! host) paid that on every one of its events, forever: its lane's queue
//! filled and blocked the single dispatcher (the whole relay stopped for
//! seconds every ~35 s), and every give-up replayed its host. Once a DID's
//! lookups have failed for the patience, its events get one try each and
//! a failure is final (rejected and acked) for `GAVE_UP_FOR`.

use super::Rejection;
use crate::identity::{Identity, LookupError};
use parking_lot::Mutex;
use std::future::Future;
use std::num::NonZeroUsize;
use std::sync::Arc;
use std::time::Duration;
use tokio::time::Instant;

pub const IDENTITY_PATIENCE: Duration = Duration::from_secs(30);

/// The leader's patience: under `quorum::GIVE_UP`, so the event is
/// rejected before its forwarder gives up and replays the host.
pub const LEADER_PATIENCE: Duration = Duration::from_secs(15);
const _: () = assert!(LEADER_PATIENCE.as_secs() < super::quorum::GIVE_UP.as_secs());

/// After a DID used up its patience, how long its failures are final.
pub const GAVE_UP_FOR: Duration = Duration::from_secs(300);

const KEPT: usize = 4096;

#[derive(Clone, Copy)]
struct Failing {
    since: Instant,
    gave_up: Option<Instant>,
}

pub struct Patience {
    failing: Mutex<lru::LruCache<String, Failing>>,
    patience: Duration,
    leader_patience: Duration,
    gave_up_for: Duration,
}

impl Default for Patience {
    fn default() -> Self {
        Patience::new(IDENTITY_PATIENCE, LEADER_PATIENCE, GAVE_UP_FOR)
    }
}

impl Patience {
    pub fn new(patience: Duration, leader_patience: Duration, gave_up_for: Duration) -> Patience {
        Patience {
            failing: Mutex::new(lru::LruCache::new(NonZeroUsize::new(KEPT).expect("nonzero"))),
            patience,
            leader_patience,
            gave_up_for,
        }
    }

    /// Whether `did` used up its patience within `GAVE_UP_FOR`.
    pub fn gave_up(&self, did: &str) -> bool {
        self.failing.lock().peek(did).and_then(|f| f.gave_up).is_some_and(|at| at.elapsed() < self.gave_up_for)
    }

    pub fn succeeded(&self, did: &str) {
        let mut f = self.failing.lock();
        if f.peek(did).is_some() {
            f.pop(did);
        }
    }

    /// Notes a failed lookup of `did`; true if it's now final: the DID
    /// gave up recently, or has been failing for `patience`.
    fn failed(&self, did: &str, patience: Duration) -> bool {
        let now = Instant::now();
        let mut m = self.failing.lock();
        let f = match m.get_mut(did) {
            // a failure long after the last one starts a new wait
            Some(f) if f.gave_up.is_some_and(|at| now - at >= self.gave_up_for) => {
                *f = Failing { since: now, gave_up: None };
                f
            }
            Some(f) if f.gave_up.is_none() && now - f.since >= self.gave_up_for => {
                f.since = now;
                f
            }
            Some(f) => f,
            None => {
                m.put(did.to_string(), Failing { since: now, gave_up: None });
                m.get_mut(did).expect("just put")
            }
        };
        if f.gave_up.is_some() {
            return true;
        }
        if now - f.since >= patience {
            f.gave_up = Some(now);
            tracing::info!(
                did,
                failing_s = (now - f.since).as_secs(),
                "identity: giving up on the DID's lookups for a while"
            );
            return true;
        }
        false
    }

    /// The leader's side: whether a failed lookup is final for the event.
    pub fn leader_failed(&self, did: &str) -> bool {
        self.failed(did, self.leader_patience)
    }

    /// The host stage: `lookup` until it answers, retrying failures with
    /// backoff for the patience, or trying once for a DID that gave up.
    pub async fn lookup<F, Fut>(&self, did: &str, mut lookup: F) -> Result<Arc<Identity>, Rejection>
    where
        F: FnMut() -> Fut,
        Fut: Future<Output = Result<Arc<Identity>, LookupError>>,
    {
        let t0 = Instant::now();
        let mut tries = 0u32;
        loop {
            match lookup().await {
                Ok(id) => {
                    self.succeeded(did);
                    return Ok(id);
                }
                Err(e @ (LookupError::NotFound | LookupError::BadDid)) => {
                    return Err(Rejection { reason: "unknown_did", detail: e.to_string() });
                }
                // a spent budget says nothing about the DID
                Err(e @ LookupError::OverBudget) if t0.elapsed() >= self.patience => {
                    return Err(Rejection { reason: "identity_unavailable", detail: e.to_string() });
                }
                Err(e @ LookupError::Failed(_)) if self.failed(did, self.patience) => {
                    return Err(Rejection { reason: "identity_unavailable", detail: e.to_string() });
                }
                Err(_) => {
                    tries += 1;
                    tokio::time::sleep(Duration::from_millis(100u64 << (2 * tries.min(4)))).await;
                }
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::{AtomicBool, AtomicU32, Ordering};

    fn id() -> Identity {
        Identity {
            did: "did:plc:blip".into(),
            signing_key: None,
            signing_key_multibase: None,
            pds: None,
            pds_host: None,
            handle: None,
        }
    }

    fn failing(calls: &AtomicU32) -> impl Future<Output = Result<Arc<Identity>, LookupError>> + '_ {
        calls.fetch_add(1, Ordering::Relaxed);
        async { Err(LookupError::Failed("error sending request".into())) }
    }

    /// The prod stall: a dead did:web held its lane ~35 s per event, so
    /// its queue filled and the dispatcher stopped behind it. Only its
    /// first event waits out the patience now.
    #[tokio::test(start_paused = true)]
    async fn a_dids_patience_is_spent_once_not_per_event() {
        let p = Patience::default();
        let calls = AtomicU32::new(0);
        let t = Instant::now();
        let r = p.lookup("did:web:dead.example", || failing(&calls)).await;
        assert_eq!(r.err().map(|r| r.reason), Some("identity_unavailable"));
        assert!(t.elapsed() >= IDENTITY_PATIENCE, "{:?}", t.elapsed());
        let first = calls.load(Ordering::Relaxed);
        assert!(first > 1);

        let t = Instant::now();
        let r = p.lookup("did:web:dead.example", || failing(&calls)).await;
        assert_eq!(r.err().map(|r| r.reason), Some("identity_unavailable"));
        assert_eq!(t.elapsed(), Duration::ZERO);
        assert_eq!(calls.load(Ordering::Relaxed), first + 1, "one try");
        assert!(p.leader_failed("did:web:dead.example"), "final for the leader too");

        // another DID still gets its patience
        let t = Instant::now();
        let _ = p.lookup("did:plc:other", || failing(&calls)).await;
        assert!(t.elapsed() >= IDENTITY_PATIENCE);

        // and the dead one gets it back once the window passes
        tokio::time::advance(GAVE_UP_FOR).await;
        assert!(!p.gave_up("did:web:dead.example"));
        let t = Instant::now();
        let _ = p.lookup("did:web:dead.example", || failing(&calls)).await;
        assert!(t.elapsed() >= IDENTITY_PATIENCE);
    }

    #[tokio::test(start_paused = true)]
    async fn a_blip_is_waited_out_and_unknown_dids_answer_at_once() {
        let p = Patience::default();
        let up = AtomicBool::new(false);
        let t = Instant::now();
        let r = p
            .lookup("did:plc:blip", || {
                let ok = up.swap(true, Ordering::Relaxed);
                async move { if ok { Ok(Arc::new(id())) } else { Err(LookupError::Failed("timed out".into())) } }
            })
            .await;
        assert!(r.is_ok());
        assert!(t.elapsed() < Duration::from_secs(1));
        assert!(!p.gave_up("did:plc:blip"));

        let t = Instant::now();
        let r = p.lookup("did:plc:nobody", || async { Err(LookupError::NotFound) }).await;
        assert_eq!(r.err().map(|r| r.reason), Some("unknown_did"));
        assert_eq!(t.elapsed(), Duration::ZERO);
    }

    /// The leader retries (the forwarder resends) until its patience, then
    /// the failure is final: the event is rejected and acked before the
    /// forwarder's give-up would replay the whole host.
    #[tokio::test(start_paused = true)]
    async fn the_leader_gives_up_before_the_forwarder_does() {
        let p = Patience::default();
        assert!(!p.leader_failed("did:web:dead.example"));
        tokio::time::advance(Duration::from_secs(5)).await;
        assert!(!p.leader_failed("did:web:dead.example"));
        tokio::time::advance(LEADER_PATIENCE).await;
        assert!(p.leader_failed("did:web:dead.example"));
        assert!(p.gave_up("did:web:dead.example"));
        // a later success lifts it
        p.succeeded("did:web:dead.example");
        assert!(!p.leader_failed("did:web:dead.example"));
    }

    struct DeadWeb;

    impl crate::identity::Fetch for Arc<DeadWeb> {
        async fn fetch(&self, did: &str) -> Result<serde_json::Value, LookupError> {
            if did == "did:web:dead.example" {
                return Err(LookupError::Failed(
                    "error sending request for url (https://dead.example/.well-known/did.json)".into(),
                ));
            }
            Ok(serde_json::json!({
                "id": did,
                "alsoKnownAs": [],
                "verificationMethod": [],
                "service": [{"id": "#atproto_pds", "type": "AtprotoPersonalDataServer", "serviceEndpoint": "https://pds.example"}],
            }))
        }
    }

    /// The leader's side of the prod loop: a dead did:web's events were
    /// answered "retry" until the forwarder gave up and replayed the host,
    /// over and over. Now the leader's answer turns final at its patience,
    /// under the forwarder's give-up, and stays final for the DID; the
    /// host's other accounts go through all along.
    #[tokio::test(start_paused = true)]
    async fn a_dead_did_web_is_rejected_for_good_and_its_host_flows() {
        use crate::identity::{IdentityCache, Options};
        use crate::state::{EventKind, Incoming, StateStore};
        let identity = Arc::new(IdentityCache::new(
            Arc::new(DeadWeb),
            Options { lookups_per_sec: 1e9, burst: 1e9, ..Options::default() },
        ));
        let st = StateStore::new(
            crate::node::adapters::VerifyChain,
            Arc::new(crate::node::adapters::CacheIdentity(identity, Arc::new(Patience::default()))),
            crate::state::ApplyConfig::default(),
        );
        crate::state::tests::attach_memory_shard(&st).await;
        let host = crate::types::Host("pds.example".into());
        let ev = |did: &'static str| Incoming { did, host: &host, now: 1_800_000_000, kind: EventKind::Identity };

        let r = st.apply(ev("did:web:dead.example")).await.unwrap_err();
        assert!(r.retryable(), "a first failure is waited out: {r:?}");
        assert_eq!(super::super::state_rejection(&r).reason, "identity_unavailable");
        assert!(st.apply(ev("did:plc:alive")).await.is_ok());

        tokio::time::advance(LEADER_PATIENCE).await;
        let r = st.apply(ev("did:web:dead.example")).await.unwrap_err();
        assert!(!r.retryable(), "final once the leader's patience is spent: {r:?}");
        assert_eq!(super::super::state_rejection(&r).reason, "identity_unavailable");

        // every later event of it is final at once; its host keeps flowing
        tokio::time::advance(Duration::from_secs(60)).await;
        let t = Instant::now();
        assert!(!st.apply(ev("did:web:dead.example")).await.unwrap_err().retryable());
        assert_eq!(t.elapsed(), Duration::ZERO);
        assert!(st.apply(ev("did:plc:alive2")).await.is_ok());
    }
}
