//! Types shared across the relay's modules. Kept small on purpose: each
//! module owns its own internals and agrees on these at the edges.

use bytes::Bytes;

/// An upstream PDS, by its normalized hostname (lowercase, no scheme, no port
/// unless non-default).
#[derive(Clone, Debug, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct Host(pub String);

/// One frame as read off a host's `subscribeRepos` socket, before any checks.
#[derive(Clone, Debug)]
pub struct UpstreamFrame {
    pub host: Host,
    /// The host's own `seq` for this event (0 for frames without one).
    pub upstream_seq: i64,
    /// The raw DAG-CBOR frame (header + body), exactly as received.
    pub frame: Bytes,
}

/// A HashMap for the per-event hot paths. SipHash was ~2% of a loaded node's
/// CPU; foldhash is seeded per process, so DID keys still can't be chosen to
/// collide.
pub type FastMap<K, V> = std::collections::HashMap<K, V, foldhash::fast::RandomState>;
