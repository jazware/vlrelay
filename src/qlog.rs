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
//! Phase 1 is memory only: the commitlog plugs in at `node::Durability`
//! (before a follower's ack and before the leader counts itself), and the
//! bucket flush comes later.

pub mod check;
pub mod client;
pub mod emit;
pub mod log;
pub mod node;
pub mod wire;

#[cfg(test)]
mod tests;
