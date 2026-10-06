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
//! Memory-only (`node::MemoryOnly`) remains for comparison. The bucket
//! flush comes later.

pub mod check;
pub mod client;
pub mod commitlog;
pub mod emit;
pub mod log;
pub mod node;
pub mod wire;

#[cfg(test)]
mod tests;
