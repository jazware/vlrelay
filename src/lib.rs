//! vlRelay: an atproto relay whose only durable state is an object store.
//! Design: docs/design.md and docs/quorum.md.

pub mod admin;
pub mod event;
pub mod identity;
pub mod node;
pub mod plc_seed;
pub mod policy;
pub mod qlog;
pub mod serve;
pub mod state;
pub mod sync_api;
pub mod types;
pub mod upstream;
pub mod verify;
