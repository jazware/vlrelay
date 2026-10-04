# vlRelay cluster: plugging it into the node

`src/cluster.rs` is the multi-node layer. It owns the lease, the shard assignments, the node's log, the merged firehose, and the hop from host owner to DID owner. `node.rs` owns the pipeline (upstream, verify, state, the DID-owner stage) and hands the cluster four hooks. This page says what to call and where.

## Roles

`--role core|edge|replica`, mapped onto `cluster::Role`.

| Role | Lease | Shards | Log | Follows peers | Needs |
|---|---|---|---|---|---|
| core | yes | DID + host | its own | over mTLS | bucket read/write, peer cert |
| edge | no | none | none | over mTLS | bucket read, peer cert |
| replica | no | none | none | from the bucket | bucket read only |

A one-node deployment can stay on `serve::start_single_node`. A core node with no peers is also a fine single node, and that's the path to take once node.rs is cluster-aware, so there's one startup.

## Startup

```rust
let peer = TcpListener::bind(peer_listen).await?;            // before joining: peers greet us here
let mut o = ClusterOptions::new(&node_id, role, &advertise);  // advertise = https://host:port of `peer`
o.tls = Some(PeerTls::load(files)?);                          // None on a replica
o.internal_token = token;
o.serve = serve_cfg;
o.log.linger = linger;
let node = ClusterNode::start(o, store).await?;               // core: joins, starts the log and the merger

node.set_stage(stage);                 // the DID-owner stage (below)
node.set_did_shards(state_hooks);      // open/close DID shards' state
node.set_host_handler(upstream_hooks); // release sockets + report acked cursors
node.on_key_change(Arc::new(|dids| identity.evict(&dids)));
node.on_lost(Box::new(|why| fail_stop(why)));

cluster::peer::spawn_listener(&node, peer)?;                  // core and edge
public_router.merge(node.serve.router());                     // subscribeRepos, as today
node.run();                                                   // steps, checkpoints, lease listing
```

Shutdown is `node.shutdown().await`: host shards are handed over (sockets closed, cursors checkpointed), then vlpds's shutdown hands the DID shards over, fences our log and deletes the lease.

## Upstream

The manager subscribes only to hosts in this node's host shards:

```rust
let cursors = node.cursor_source().unwrap();               // resumes from hostck/ checkpoints
let (mgr, rx) = Manager::new(cfg, host_store, Some(cursors.clone()));
let _ = cursors.registry.set(mgr.registry().clone());
mgr.follow_filter(node.host_filter().unwrap());            // reapplied on every change
mgr.start().await?;
```

`Manager::set_filter` stops the sockets of hosts that left, flushes the registry, reloads it (for hosts another node admitted) and connects the ones that arrived. `node.host_watch()` carries the same thing as `HostOwnership` if something else wants it.

`set_host_handler` takes a `HostHandler`. Its `release(keep)` is what makes a planned handoff graceful: call `mgr.set_filter(keep)`, then return the acked cursor of each host it stopped. `acked()` returns the acked cursor of every subscribed host, and the cluster writes them to `hostck/{shard}` every `checkpoint_every`. A crash takeover resumes from there and the PDS replays the rest.

The upstream `HostStore` (the registry rows) has to be shared by all nodes, so `listHosts` and admission see every host. That isn't the cluster's: it's whatever node.rs gives the manager. Cursors no longer depend on it.

## Host owner to DID owner

After verify, the host owner calls:

```rust
let outcome = node.forward(Forwarded { did, host, upstream_seq, meta, frame }).await?;
```

`meta` is opaque bytes for whatever the host owner parsed that the DID owner needs (the commit's rev, ops, the identity fields). Encode it in node.rs. The forwarder batches by DID slot, so a DID's events reach its owner in the order they were submitted. It sends straight to the stage when this node owns the DID, and retries against the new owner after a takeover, for up to 20 s.

`Outcome` is `Appended(seq)`, `Duplicate` or `Rejected(reason)`. All three mean the host owner may count the event as done. Ack the upstream seq to the manager once every earlier event of that host is done.

On the DID owner the cluster calls the `DidStage` with only the DIDs whose shard it serves, in arrival order:

```rust
#[async_trait]
impl DidStage for Stage {
    async fn apply(&self, batch: Vec<Forwarded>) -> Vec<StageResult> {
        // check_chain + state apply per event, append the accepted ones to
        // node.log, wait until durable, commit state tickets, answer each
    }
}
```

After a retryable error (`Unavailable`) for a DID, the stage answers that DID's later events in the batch with the same error instead of applying them. `NotOwner` is the cluster's to answer. The stage only sees events for shards it holds, and a shard can't close while a batch for it is in flight.

## DID shards

`DidShards::open(shards)` gets each shard with its history (the earlier owners' log spans, oldest first). It opens the state with `StateStore::open_shard`, replays every span with `recover`, and returns. Events for a shard are only routed once `open` returned Ok for it. `close(shards)` runs once nothing for those shards is in flight and the log is drained. It checkpoints and calls `close_shard`. `on_layout` passes the layout to `StateStore::set_layout`.

Use `node.layout()` for the initial `StateStore` layout. `node.did_shard(did)` is the `shard` to write into each log entry's `EventMeta`.

## Identity

When the DID owner sees a key change (`#identity`, or a fresh DID document with a new key), call `node.invalidate_keys(dids).await`. Every live peer's `on_key_change` hook runs with them, which should evict the host owner's signing-key cache. Invalidations aren't pushes, so the next verify re-resolves.

## Lease checks

The log already refuses to PUT or ack once the lease lapses (the cluster sets `lease_ok`). Anything else that acks durably should check `node.lease_valid()` too. `node.lease_check()` gives the same check as a closure.

## What's in the bucket

| Prefix | Written by | What |
|---|---|---|
| `nodes/`, `writers/`, `assign/`, `cluster/version` | vlpds `Cluster` | leases, writer bytes, DID shard assignments, layout |
| `assign-hosts/` | `cluster::hosts` | host shard layout and assignments |
| `hostck/` | `cluster::hosts` | upstream cursors per host shard |
| `log/{log_id}/` | `seq::NodeLog` | each core node's log, fenced at its end |

## Known gaps

- An idle core log holds a replica's merge until its next segment, since a replica sees only segments. Busy logs never notice. The fix is an empty heartbeat segment from a log that's been idle for a while.
- Edges and replicas follow a new core node's log once they list its lease. They hold their merge `guard` (250 ms) behind their last listing so the joiner's first events don't land below it. That assumes clocks agree to within `guard`.
- Segments still reach replicas by polling the bucket. Pushing them to edges is already done (the mTLS stream). Push to replicas isn't, which is M5.
- The cluster doesn't start log retention. node.rs can call `node.serve.spawn_retention(log_id)` on core nodes. Running it on every core node is harmless but redundant, and it could move to the slot-0 leader (`cluster.leads_slot0()`) as vlpds does.
