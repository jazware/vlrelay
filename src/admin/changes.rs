//! The admin change feed (`GET /admin/api/changes`, docs/admin-api.md,
//! "Change feed"): what changed, by kind, id and version, as Server-Sent
//! Events. The feed holds no data, only the names of rows to refetch.
//!
//! One [`ChangeFeed`] per serving process. Its sources publish into a ring
//! of the last [`RING`] events, each numbered by this process (`<boot>.<n>`,
//! the SSE id a client resumes from). Hot ids go through [`ChangeFeed::touch`],
//! which keeps the latest per id until the next [`ChangeFeed::flush`]. On a
//! cluster each member answers [`ChangeFeed::pull`] with the events it
//! originated, and the serving node merges them in with
//! [`ChangeFeed::absorb`].

use axum::response::sse::{Event, KeepAlive, Sse};
use parking_lot::Mutex;
use serde::{Deserialize, Serialize};
use std::collections::{BTreeMap, HashMap, HashSet, VecDeque};
use std::convert::Infallible;
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::sync::broadcast;

/// Events a node keeps for resuming clients and pulling members.
pub const RING: usize = 4096;
/// Events one client may fall behind before it gets a `resync`.
pub const CLIENT_BUFFER: usize = 1024;
pub const MAX_FEEDS: usize = 64;
/// A full node closes its oldest feed for a new one if that feed is at
/// least this old: a page reloaded behind a proxy can leave its old feed
/// counted until a write to it fails, and a client closed this way
/// reconnects and resumes from its last id.
pub const EVICT_AFTER: Duration = Duration::from_secs(15);
/// More ids of one kind than this in one flush become a single `*`.
pub const STAR_AT: usize = 256;
pub const COALESCE: Duration = Duration::from_secs(1);
pub const PING: Duration = Duration::from_secs(15);
/// How often a serving node asks members for their events.
pub const PULL_EVERY: Duration = Duration::from_secs(1);
/// A node keeps observing this long after a member last pulled, so a
/// serving node with feeds open hears about changes made anywhere.
pub const PULLED_FOR: Duration = Duration::from_secs(15);
/// Events in one pull answer.
pub const PULL_MAX: usize = 512;

#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub enum ChangeKind {
    Host,
    Policy,
    Rules,
    Takedown,
    Account,
    Cluster,
    Consumer,
    Discovery,
    Plc,
    Case,
}

impl ChangeKind {
    /// The kinds a member forwards to a serving node: what only it sees.
    pub const FORWARDED: [ChangeKind; 7] = [
        ChangeKind::Host,
        ChangeKind::Consumer,
        ChangeKind::Discovery,
        ChangeKind::Plc,
        ChangeKind::Case,
        ChangeKind::Policy,
        ChangeKind::Rules,
    ];

    /// Document versions every node sees for itself as well as hearing
    /// them forwarded, so the second copy is dropped.
    fn deduped(self) -> bool {
        matches!(self, ChangeKind::Policy | ChangeKind::Rules)
    }
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct Change {
    pub kind: ChangeKind,
    pub id: String,
    pub version: String,
    pub node: String,
    pub at_ms: i64,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub hint: Option<serde_json::Value>,
}

#[derive(Debug)]
pub struct Stored {
    pub n: u64,
    pub change: Change,
    /// Members hand it on to a serving node that pulls.
    pub forward: bool,
}

/// A member's answer to a pull.
#[derive(Clone, Debug, Default, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct PullAnswer {
    pub boot: String,
    /// The cursor to send next time.
    pub last: u64,
    pub events: Vec<Change>,
    /// The cursor sent was from another boot or older than the ring: some
    /// events are lost.
    pub gap: bool,
    pub more: bool,
}

#[derive(Clone, Debug, Default, Serialize, Deserialize)]
pub struct PullRequest {
    pub boot: Option<String>,
    pub after: Option<u64>,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize)]
#[serde(rename_all = "kebab-case")]
pub enum ResyncReason {
    UnknownCursor,
    Expired,
    Lagged,
}

struct Inner {
    ring: VecDeque<Arc<Stored>>,
    next: u64,
    seen: VecDeque<(ChangeKind, String, String)>,
    seen_set: HashSet<(ChangeKind, String, String)>,
}

struct Pending {
    hint: Option<serde_json::Value>,
    version: String,
    node: String,
    forward: bool,
}

pub struct ChangeFeed {
    node: String,
    boot: String,
    clock: Mutex<u64>,
    inner: Mutex<Inner>,
    tx: broadcast::Sender<Arc<Stored>>,
    pending: Mutex<BTreeMap<(ChangeKind, String), Pending>>,
    feeds: Mutex<Slots>,
    pulled_at: Mutex<Option<Instant>>,
    /// member -> (its boot, the cursor to send it).
    members: Mutex<HashMap<String, (String, u64)>>,
    /// node -> host -> the newest `host` version that node made.
    host_versions: Mutex<HashMap<String, HashMap<String, String>>>,
}

pub fn now_ms() -> i64 {
    chrono::Utc::now().timestamp_millis()
}

fn now_us() -> u64 {
    chrono::Utc::now().timestamp_micros().max(0) as u64
}

impl ChangeFeed {
    pub fn new(node: &str) -> Arc<ChangeFeed> {
        let (tx, _) = broadcast::channel(CLIENT_BUFFER);
        Arc::new(ChangeFeed {
            node: node.to_string(),
            boot: format!("{:x}", now_us()),
            clock: Mutex::new(0),
            inner: Mutex::new(Inner {
                ring: VecDeque::new(),
                next: 1,
                seen: VecDeque::new(),
                seen_set: HashSet::new(),
            }),
            tx,
            pending: Mutex::new(BTreeMap::new()),
            feeds: Mutex::new(Slots::default()),
            host_versions: Mutex::new(HashMap::new()),
            pulled_at: Mutex::new(None),
            members: Mutex::new(HashMap::new()),
        })
    }

    pub fn node(&self) -> &str {
        &self.node
    }

    pub fn boot(&self) -> &str {
        &self.boot
    }

    /// The next node-scoped version, `<node>:<counter>`. The counter starts
    /// from the clock in microseconds, so it keeps rising across restarts.
    pub fn next_version(&self) -> String {
        self.version_for(&self.node)
    }

    /// A node-scoped version under another node's name, from this clock:
    /// for the demo, which plays every node.
    pub fn version_for(&self, node: &str) -> String {
        let mut c = self.clock.lock();
        *c = (*c + 1).max(now_us());
        format!("{node}:{}", *c)
    }

    /// A feed is open here, or a member pulled lately: sources that cost
    /// something to watch run only then.
    pub fn wanted(&self) -> bool {
        self.open_feeds() > 0 || self.pulled_at.lock().is_some_and(|t| t.elapsed() < PULLED_FOR)
    }

    pub fn open_feeds(&self) -> usize {
        self.feeds.lock().open.len()
    }

    /// What the ring holds, oldest first.
    pub fn recent(&self) -> Vec<Change> {
        self.inner.lock().ring.iter().map(|s| s.change.clone()).collect()
    }

    /// The newest event number.
    pub fn last(&self) -> u64 {
        self.inner.lock().next - 1
    }

    /// Into the ring and out to every open feed now.
    pub fn publish(&self, change: Change, forward: bool) {
        if change.kind == ChangeKind::Host && change.id != "*" {
            let mut hv = self.host_versions.lock();
            let v = hv.entry(change.node.clone()).or_default().entry(change.id.clone()).or_default();
            if compare_versions(&change.version, v) != Some(std::cmp::Ordering::Less) {
                *v = change.version.clone();
            }
        }
        let mut i = self.inner.lock();
        if change.kind.deduped() {
            let key = (change.kind, change.id.clone(), change.version.clone());
            if !i.seen_set.insert(key.clone()) {
                return;
            }
            i.seen.push_back(key);
            if i.seen.len() > 1024
                && let Some(k) = i.seen.pop_front()
            {
                i.seen_set.remove(&k);
            }
        }
        let s = Arc::new(Stored { n: i.next, change, forward });
        i.next += 1;
        i.ring.push_back(s.clone());
        if i.ring.len() > RING {
            i.ring.pop_front();
        }
        // sent under the lock, so a subscriber's replay and its live
        // events neither overlap nor leave a gap
        let _ = self.tx.send(s);
    }

    /// A log-backed or document-versioned change, now.
    pub fn publish_versioned(
        &self,
        kind: ChangeKind,
        id: impl Into<String>,
        version: impl Into<String>,
        hint: Option<serde_json::Value>,
        forward: bool,
    ) {
        let c = Change { kind, id: id.into(), version: version.into(), node: self.node.clone(), at_ms: now_ms(), hint };
        self.publish(c, forward);
    }

    /// A node-scoped change, now (an operator's action). Returns its version.
    pub fn emit(
        &self,
        kind: ChangeKind,
        id: impl Into<String>,
        hint: Option<serde_json::Value>,
        forward: bool,
    ) -> String {
        let version = self.next_version();
        let c =
            Change { kind, id: id.into(), version: version.clone(), node: self.node.clone(), at_ms: now_ms(), hint };
        self.publish(c, forward);
        version
    }

    /// A node-scoped change, coalesced per id until the next [`Self::flush`].
    /// Returns its version, which the row it describes carries from now.
    pub fn touch(
        &self,
        kind: ChangeKind,
        id: impl Into<String>,
        hint: Option<serde_json::Value>,
        forward: bool,
    ) -> String {
        let node = self.node.clone();
        self.touch_as(&node, kind, id, hint, forward)
    }

    /// [`Self::touch`] under another node's name (the demo).
    pub fn touch_as(
        &self,
        node: &str,
        kind: ChangeKind,
        id: impl Into<String>,
        hint: Option<serde_json::Value>,
        forward: bool,
    ) -> String {
        let version = self.version_for(node);
        let p = Pending { hint, version: version.clone(), node: node.to_string(), forward };
        self.pending.lock().insert((kind, id.into()), p);
        version
    }

    /// Everything touched since the last flush, one event per id, or one
    /// `*` per kind past [`STAR_AT`] ids.
    pub fn flush(&self) {
        let pending = std::mem::take(&mut *self.pending.lock());
        if pending.is_empty() {
            return;
        }
        let mut by_kind: BTreeMap<(ChangeKind, String), Vec<(String, Pending)>> = BTreeMap::new();
        for ((k, id), p) in pending {
            by_kind.entry((k, p.node.clone())).or_default().push((id, p));
        }
        let at_ms = now_ms();
        for ((kind, node), items) in by_kind {
            if items.len() > STAR_AT {
                let forward = items.iter().any(|(_, p)| p.forward);
                let version =
                    items.iter().map(|(_, p)| p.version.clone()).max_by(|a, b| version_cmp(a, b)).unwrap_or_default();
                self.publish(Change { kind, id: "*".into(), version, node, at_ms, hint: None }, forward);
                continue;
            }
            for (id, p) in items {
                self.publish(Change { kind, id, version: p.version, node: p.node, at_ms, hint: p.hint }, p.forward);
            }
        }
    }

    /// Flushes every [`COALESCE`] for as long as the feed lives.
    pub fn spawn_flusher(self: &Arc<Self>) {
        let weak = Arc::downgrade(self);
        tokio::spawn(async move {
            let mut tick = tokio::time::interval(COALESCE);
            tick.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
            loop {
                tick.tick().await;
                let Some(f) = weak.upgrade() else { return };
                f.flush();
            }
        });
    }

    /// A member's side of a pull: the events it originated after `after`.
    pub fn pull(&self, req: &PullRequest) -> PullAnswer {
        *self.pulled_at.lock() = Some(Instant::now());
        let i = self.inner.lock();
        let last = i.next - 1;
        let mut a = PullAnswer { boot: self.boot.clone(), last, ..Default::default() };
        let (Some(boot), Some(after)) = (&req.boot, req.after) else { return a };
        if *boot != self.boot || after > last {
            a.gap = true;
            return a;
        }
        if i.ring.front().is_some_and(|f| after + 1 < f.n) {
            a.gap = true;
            return a;
        }
        let mut upto = after;
        for s in i.ring.iter().filter(|s| s.n > after) {
            if a.events.len() >= PULL_MAX {
                a.more = true;
                break;
            }
            upto = s.n;
            if s.forward && s.change.node == self.node {
                a.events.push(s.change.clone());
            }
        }
        a.last = if a.more { upto } else { last };
        a
    }

    /// What to send `member` next.
    pub fn pull_request(&self, member: &str) -> PullRequest {
        match self.members.lock().get(member) {
            Some((boot, after)) => PullRequest { boot: Some(boot.clone()), after: Some(*after) },
            None => PullRequest::default(),
        }
    }

    /// A member's answer merged in. A gap becomes a `*` per forwarded kind
    /// with the member's name, so clients refetch what they show of it.
    /// Returns whether there's more to pull now.
    pub fn absorb(&self, member: &str, a: PullAnswer) -> bool {
        if a.gap {
            for kind in ChangeKind::FORWARDED {
                let version = self.next_version();
                let c = Change { kind, id: "*".into(), version, node: member.to_string(), at_ms: now_ms(), hint: None };
                self.publish(c, false);
            }
        }
        for c in a.events {
            self.publish(c, false);
        }
        self.members.lock().insert(member.to_string(), (a.boot, a.last));
        a.more
    }

    /// The newest `host` event `node` made for `host` that this feed has.
    pub fn host_version(&self, node: &str, host: &str) -> Option<String> {
        self.host_versions.lock().get(node)?.get(host).cloned()
    }

    /// Forgets members no longer in the cluster.
    pub fn retain_members(&self, keep: &[String]) {
        self.members.lock().retain(|m, _| keep.contains(m));
        let me = self.node.as_str();
        self.host_versions.lock().retain(|m, _| m == me || keep.contains(m));
    }

    /// An open feed: the replay owed to the client's cursor, and its live
    /// events from then on.
    pub fn subscribe(self: &Arc<Self>, since: Option<&str>) -> Result<Subscription, TooManyFeeds> {
        let close = Arc::new(tokio::sync::Notify::new());
        let slot = {
            let mut f = self.feeds.lock();
            if f.open.len() >= MAX_FEEDS {
                let (&oldest, (at, _)) = f.open.iter().next().expect("full");
                if at.elapsed() < EVICT_AFTER {
                    return Err(TooManyFeeds);
                }
                if let Some((_, c)) = f.open.remove(&oldest) {
                    c.notify_one();
                }
            }
            f.next += 1;
            let n = f.next;
            f.open.insert(n, (Instant::now(), close.clone()));
            n
        };
        let guard = FeedGuard(self.clone(), slot, close);
        let i = self.inner.lock();
        let rx = self.tx.subscribe();
        let last = i.next - 1;
        let parsed = since.map(|s| s.split_once('.').and_then(|(b, n)| Some((b.to_string(), n.parse::<u64>().ok()?))));
        let (resumed, replay, resync) = match parsed {
            None => (false, Vec::new(), None),
            Some(None) => (false, Vec::new(), Some(ResyncReason::UnknownCursor)),
            Some(Some((b, n))) if b != self.boot || n > last => (false, Vec::new(), Some(ResyncReason::UnknownCursor)),
            Some(Some((_, n))) if i.ring.front().is_some_and(|f| n + 1 < f.n) => {
                (false, Vec::new(), Some(ResyncReason::Expired))
            }
            Some(Some((_, n))) => (true, i.ring.iter().filter(|s| s.n > n).cloned().collect(), None),
        };
        drop(i);
        Ok(Subscription { feed: self.clone(), resumed, replay, resync, resync_at: last, rx, guard })
    }

    /// The SSE response for one client.
    pub fn sse(
        self: &Arc<Self>,
        since: Option<&str>,
    ) -> Result<Sse<impl futures::Stream<Item = Result<Event, Infallible>> + use<>>, TooManyFeeds> {
        let sub = self.subscribe(since)?;
        Ok(Sse::new(sub.into_stream()).keep_alive(KeepAlive::new().interval(PING).text("ping")))
    }
}

/// [`MAX_FEEDS`] are open.
#[derive(Debug)]
pub struct TooManyFeeds;

/// The open feeds by when they opened, each with what closes it.
#[derive(Default)]
struct Slots {
    next: u64,
    open: BTreeMap<u64, (Instant, Arc<tokio::sync::Notify>)>,
}

struct FeedGuard(Arc<ChangeFeed>, u64, Arc<tokio::sync::Notify>);

impl Drop for FeedGuard {
    fn drop(&mut self) {
        self.0.feeds.lock().open.remove(&self.1);
    }
}

pub struct Subscription {
    feed: Arc<ChangeFeed>,
    pub resumed: bool,
    pub replay: Vec<Arc<Stored>>,
    pub resync: Option<ResyncReason>,
    /// The id a first `resync` carries: everything after it comes live.
    resync_at: u64,
    pub rx: broadcast::Receiver<Arc<Stored>>,
    guard: FeedGuard,
}

/// One SSE message, before it's framed.
#[derive(Clone, Debug, PartialEq)]
pub enum Message {
    Hello(serde_json::Value),
    Change(String, Change),
    Resync(String, ResyncReason),
}

impl Message {
    fn event(&self) -> Event {
        let json = |v: &serde_json::Value| Event::default().data(v.to_string());
        match self {
            Message::Hello(v) => json(v).event("hello"),
            Message::Change(id, c) => {
                Event::default().event("change").id(id).data(serde_json::to_string(c).unwrap_or_default())
            }
            Message::Resync(id, r) => Event::default()
                .event("resync")
                .id(id)
                .data(serde_json::json!({ "reason": r, "atMs": now_ms() }).to_string()),
        }
    }
}

impl Subscription {
    fn id(&self, n: u64) -> String {
        format!("{}.{n}", self.feed.boot)
    }

    /// The messages in order: hello, the replay or a resync, then live.
    pub fn into_messages(self) -> impl futures::Stream<Item = Message> {
        let hello = Message::Hello(serde_json::json!({
            "node": self.feed.node,
            "boot": self.feed.boot,
            "atMs": now_ms(),
            "resumed": self.resumed,
        }));
        let mut first = vec![hello];
        if let Some(r) = self.resync {
            first.push(Message::Resync(self.id(self.resync_at), r));
        }
        for s in &self.replay {
            first.push(Message::Change(self.id(s.n), s.change.clone()));
        }
        let head = futures::stream::iter(first);
        let live = futures::stream::unfold((self, false), |(mut sub, mut lagged)| async move {
            loop {
                let got = tokio::select! {
                    r = sub.rx.recv() => r,
                    _ = sub.guard.2.notified() => return None,
                };
                match got {
                    Ok(s) => {
                        let mut out = Vec::with_capacity(2);
                        if lagged {
                            out.push(Message::Resync(sub.id(s.n - 1), ResyncReason::Lagged));
                            lagged = false;
                        }
                        out.push(Message::Change(sub.id(s.n), s.change.clone()));
                        return Some((futures::stream::iter(out), (sub, lagged)));
                    }
                    Err(broadcast::error::RecvError::Lagged(_)) => lagged = true,
                    Err(broadcast::error::RecvError::Closed) => return None,
                }
            }
        });
        futures::StreamExt::chain(head, futures::StreamExt::flatten(live))
    }

    fn into_stream(self) -> impl futures::Stream<Item = Result<Event, Infallible>> {
        futures::StreamExt::map(self.into_messages(), |m| Ok(m.event()))
    }
}

/// Orders two versions that compare (both numbers, or one node's), as
/// docs/admin-api.md "Versions" says; others are equal here.
pub fn version_cmp(a: &str, b: &str) -> std::cmp::Ordering {
    compare_versions(a, b).unwrap_or(std::cmp::Ordering::Equal)
}

/// `None` when the two don't compare.
pub fn compare_versions(a: &str, b: &str) -> Option<std::cmp::Ordering> {
    let split = |v: &str| -> Option<(String, u128)> {
        match v.rsplit_once(':') {
            Some((node, n)) => Some((node.to_string(), n.parse().ok()?)),
            None => Some((String::new(), v.parse().ok()?)),
        }
    };
    let (x, y) = (split(a)?, split(b)?);
    (x.0 == y.0).then(|| x.1.cmp(&y.1))
}

#[cfg(test)]
mod tests {
    use super::*;
    use futures::StreamExt;

    fn ids(ms: &[Message]) -> Vec<String> {
        ms.iter()
            .filter_map(|m| match m {
                Message::Change(_, c) => Some(c.id.clone()),
                _ => None,
            })
            .collect()
    }

    async fn take(sub: Subscription, n: usize) -> Vec<Message> {
        let s = sub.into_messages();
        tokio::time::timeout(Duration::from_secs(2), s.take(n).collect::<Vec<_>>()).await.expect("messages")
    }

    fn cursor(m: &Message) -> String {
        match m {
            Message::Change(id, _) | Message::Resync(id, _) => id.clone(),
            Message::Hello(_) => panic!("hello has no id"),
        }
    }

    #[tokio::test]
    async fn resumes_after_the_last_event_id() {
        let f = ChangeFeed::new("n1");
        f.publish_versioned(ChangeKind::Takedown, "did:plc:a", "10", None, false);
        let sub = f.subscribe(None).unwrap();
        f.publish_versioned(ChangeKind::Takedown, "did:plc:b", "11", None, false);
        let got = take(sub, 2).await;
        assert!(matches!(&got[0], Message::Hello(h) if h["resumed"] == false));
        assert_eq!(ids(&got), ["did:plc:b"]);
        let at = cursor(&got[1]);
        f.publish_versioned(ChangeKind::Takedown, "did:plc:c", "12", None, false);
        f.publish_versioned(ChangeKind::Takedown, "did:plc:d", "13", None, false);
        let got = take(f.subscribe(Some(&at)).unwrap(), 3).await;
        assert!(matches!(&got[0], Message::Hello(h) if h["resumed"] == true));
        assert_eq!(ids(&got), ["did:plc:c", "did:plc:d"]);
    }

    #[tokio::test]
    async fn an_unknown_or_expired_cursor_is_a_resync() {
        let f = ChangeFeed::new("n1");
        for i in 0..(RING + 10) {
            f.publish_versioned(ChangeKind::Takedown, format!("did:plc:{i}"), i.to_string(), None, false);
        }
        let got = take(f.subscribe(Some("other.3")).unwrap(), 2).await;
        assert!(matches!(&got[1], Message::Resync(_, ResyncReason::UnknownCursor)));
        let old = format!("{}.3", f.boot());
        let got = take(f.subscribe(Some(&old)).unwrap(), 2).await;
        assert!(matches!(&got[1], Message::Resync(_, ResyncReason::Expired)));
        // the resync's id resumes from live
        f.publish_versioned(ChangeKind::Takedown, "did:plc:new", "x", None, false);
        let got = take(f.subscribe(Some(&cursor(&got[1]))).unwrap(), 2).await;
        assert_eq!(ids(&got), ["did:plc:new"]);
        let future = format!("{}.{}", f.boot(), f.last() + 5);
        let got = take(f.subscribe(Some(&future)).unwrap(), 2).await;
        assert!(matches!(&got[1], Message::Resync(_, ResyncReason::UnknownCursor)));
    }

    #[tokio::test]
    async fn a_client_that_falls_behind_gets_a_resync_then_live() {
        let f = ChangeFeed::new("n1");
        let sub = f.subscribe(None).unwrap();
        for i in 0..(CLIENT_BUFFER + 50) {
            f.publish_versioned(ChangeKind::Takedown, format!("did:plc:{i}"), i.to_string(), None, false);
        }
        let got = take(sub, 3).await;
        let Message::Resync(id, ResyncReason::Lagged) = &got[1] else { panic!("{got:?}") };
        let Message::Change(next, _) = &got[2] else { panic!() };
        let n = |s: &str| s.split_once('.').unwrap().1.parse::<u64>().unwrap();
        assert_eq!(n(id) + 1, n(next));
    }

    #[tokio::test]
    async fn touches_coalesce_per_id_and_collapse_past_the_bound() {
        let f = ChangeFeed::new("n1");
        let sub = f.subscribe(None).unwrap();
        let mut last = String::new();
        for i in 0..50 {
            last = f.touch(ChangeKind::Host, "a.example.com", Some(serde_json::json!({ "i": i })), true);
        }
        f.touch(ChangeKind::Host, "b.example.com", None, true);
        f.flush();
        for i in 0..(STAR_AT + 1) {
            f.touch(ChangeKind::Consumer, format!("n1/{i}"), None, true);
        }
        f.flush();
        let got = take(sub, 4).await;
        let changes: Vec<&Change> = got
            .iter()
            .filter_map(|m| match m {
                Message::Change(_, c) => Some(c),
                _ => None,
            })
            .collect();
        assert_eq!(changes.len(), 3);
        assert_eq!(changes[0].id, "a.example.com");
        assert_eq!(changes[0].version, last);
        assert_eq!(changes[0].hint.as_ref().unwrap()["i"], 49);
        assert_eq!(changes[1].id, "b.example.com");
        assert_eq!((changes[2].kind, changes[2].id.as_str()), (ChangeKind::Consumer, "*"));
    }

    #[test]
    fn versions_compare_within_a_scope() {
        use std::cmp::Ordering::*;
        assert_eq!(compare_versions("9", "10"), Some(Less));
        assert_eq!(compare_versions("n1:5", "n1:4"), Some(Greater));
        assert_eq!(compare_versions("n1:5", "n2:4"), None);
        assert_eq!(compare_versions("5", "n1:4"), None);
        let f = ChangeFeed::new("n1");
        let (a, b) = (f.next_version(), f.next_version());
        assert_eq!(compare_versions(&a, &b), Some(Less));
    }

    #[test]
    fn members_pull_what_they_originated() {
        let m = ChangeFeed::new("n2");
        let s = ChangeFeed::new("n1");
        // the first pull only learns the cursor
        assert!(!s.absorb("n2", m.pull(&s.pull_request("n2"))));
        m.emit(ChangeKind::Host, "a.example.com", None, true);
        m.emit(ChangeKind::Host, "b.example.com", None, false);
        m.publish_versioned(ChangeKind::Takedown, "did:plc:a", "7", None, false);
        m.publish_versioned(ChangeKind::Policy, "policy", "3", None, true);
        s.absorb("n2", m.pull(&s.pull_request("n2")));
        let got: Vec<(ChangeKind, String, String)> =
            s.inner.lock().ring.iter().map(|x| (x.change.kind, x.change.id.clone(), x.change.node.clone())).collect();
        assert_eq!(
            got,
            [
                (ChangeKind::Host, "a.example.com".into(), "n2".into()),
                (ChangeKind::Policy, "policy".into(), "n2".into())
            ]
        );
        // the same policy version seen here too is dropped
        s.publish_versioned(ChangeKind::Policy, "policy", "3", None, true);
        assert_eq!(s.inner.lock().ring.len(), 2);
        // forwarded events aren't forwarded again
        let back = s.pull(&PullRequest { boot: Some(s.boot().into()), after: Some(0) });
        assert!(back.events.is_empty());
        // a restarted member is a gap: a `*` per forwarded kind
        let m2 = ChangeFeed::new("n2");
        let before = s.last();
        s.absorb("n2", m2.pull(&s.pull_request("n2")));
        assert_eq!(s.last() - before, ChangeKind::FORWARDED.len() as u64);
        assert!(s.inner.lock().ring.iter().rev().take(7).all(|x| x.change.id == "*" && x.change.node == "n2"));
    }

    #[tokio::test]
    async fn feeds_are_capped() {
        let f = ChangeFeed::new("n1");
        let subs: Vec<_> = (0..MAX_FEEDS).map(|_| f.subscribe(None).unwrap()).collect();
        assert!(f.subscribe(None).is_err());
        drop(subs);
        assert!(f.subscribe(None).is_ok());
        assert_eq!(f.open_feeds(), 0);
    }

    /// A full node gives a new client the oldest feed's slot once that feed
    /// is old enough to be a reloaded page's leftover; the old one ends.
    #[tokio::test]
    async fn a_full_node_closes_its_oldest_feed_for_a_new_one() {
        let f = ChangeFeed::new("n1");
        let mut subs: Vec<_> = (0..MAX_FEEDS).map(|_| f.subscribe(None).unwrap()).collect();
        assert!(f.subscribe(None).is_err(), "every feed is new");
        if let Some((at, _)) = f.feeds.lock().open.values_mut().next() {
            *at -= EVICT_AFTER;
        }
        let new = f.subscribe(None).unwrap();
        assert_eq!(f.open_feeds(), MAX_FEEDS);
        let oldest = subs.remove(0).into_messages();
        let got: Vec<Message> = tokio::time::timeout(Duration::from_secs(2), oldest.collect()).await.unwrap();
        assert!(matches!(got.as_slice(), [Message::Hello(_)]), "{got:?}");
        assert!(f.subscribe(None).is_err(), "the next oldest is new");
        drop((subs, new));
        assert_eq!(f.open_feeds(), 0);
    }

    /// A client that goes away frees its slot at once, not at the next
    /// write that fails (a ping, up to 15 s later), so a reloaded page's
    /// new feed isn't refused while the old one is still counted.
    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn a_closed_client_frees_its_slot_at_once() {
        use tokio::io::{AsyncReadExt, AsyncWriteExt};
        let f = ChangeFeed::new("n1");
        let feed = f.clone();
        let app = axum::Router::new().route(
            "/changes",
            axum::routing::get(move || {
                let feed = feed.clone();
                async move {
                    use axum::response::IntoResponse;
                    match feed.sse(None) {
                        Ok(sse) => sse.into_response(),
                        Err(TooManyFeeds) => axum::http::StatusCode::SERVICE_UNAVAILABLE.into_response(),
                    }
                }
            }),
        );
        let lis = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = lis.local_addr().unwrap();
        tokio::spawn(axum::serve(lis, app).into_future());
        let open = || async move {
            let mut c = tokio::net::TcpStream::connect(addr).await.unwrap();
            c.write_all(b"GET /changes HTTP/1.1\r\nhost: x\r\naccept: text/event-stream\r\n\r\n").await.unwrap();
            let mut buf = vec![0u8; 512];
            let n = c.read(&mut buf).await.unwrap();
            let status = String::from_utf8_lossy(&buf[..n]).lines().next().unwrap_or_default().to_string();
            (c, status)
        };
        let mut conns = Vec::new();
        for _ in 0..MAX_FEEDS {
            let (c, status) = open().await;
            assert!(status.contains("200"), "{status}");
            conns.push(c);
        }
        assert_eq!(f.open_feeds(), MAX_FEEDS);
        assert!(open().await.1.contains("503"));
        drop(conns.pop());
        let t = Instant::now();
        while f.open_feeds() == MAX_FEEDS {
            assert!(t.elapsed() < Duration::from_millis(500), "the slot is still held");
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
        let (_c, status) = open().await;
        assert!(status.contains("200"), "{status} after {:?}", t.elapsed());
    }
}
