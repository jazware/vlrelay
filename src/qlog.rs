//! The quorum log (docs/quorum.md): one stream log, seqs assigned by the
//! leader at append, replicated to every member and committed once a
//! quorum holds an entry. Nothing is emitted before it commits.
//!
//! - `log`: one node's copy, with `(epoch, seq)` log matching and truncation.
//! - `wire`: the peer protocol.
//! - `node`: leader, follower and takeover (`qlog/leader` CAS, promises,
//!   adopting the longest tail), peer heartbeats for liveness.
//! - `emit`: committed entries into vlpds's firehose.
//! - `client`: a host owner's submit-and-resend side.
//! - `check`: the emission checker the tests and the chaos harness share.
//!
//! - `commitlog`: the local, group-fsynced log behind `node::Durability`
//!   (before a follower's ack, the leader counting itself, and a promise),
//!   and the single node's WAL.
//!
//! - `flush`: every interval the leader seals `state` at a committed seq F,
//!   uploads the log to F as vlpds segments and CASes `qlog/manifest`.
//!
//! Memory-only (`node::MemoryOnly`) remains for comparison.

pub mod check;
pub mod client;
pub mod commitlog;
pub mod emit;
pub mod flush;
pub mod log;
pub mod node;
pub mod state;
pub mod wire;

#[cfg(test)]
mod tests;
