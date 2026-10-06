# vlRelay cluster: how node.rs plugs in

Internal: the public page is [Cluster](cluster.md) (`/docs/cluster`). This is the API between `src/cluster.rs` and the node pipeline, and what the node does with each seam.

## What node.rs does with it

`Node::start_cluster` (`src/node/cluster.rs`) builds the same pipeline as `Node::start`, with these seams moved:

- The lane's `DidOwner` is `Forwarding`: `submit` hands the checked event (kind, verified chain fields, seq span and first-sighting flag, encoded into `Forwarded::meta`) to the forwarder and returns `Submitted::Forwarded(rx)`. The lane waits on the outcome off the lane, counts it and acks the host cursor. If the forwarder gives up (no owner for 20 s), the event's ack is held (`Tracker::fail`), its host socket is fenced (`forward::Fence`: the DID's later events in the batch give up with it, and nothing else from that socket is sent) and the host is kicked once per socket, so it replays everything past its cursor in order. A host handed to another node is fenced here too.
- The DID-owner stage (`Stage`) runs each batch through the same `LocalOwner` as a single node (apply, append to this node's log, commit once durable), DIDs in parallel, each DID's events in order, and answers once each is durable. It checks the lease before answering.
- `Shards` opens a DID shard's SlateDB and replays each earlier owner's log span in order (bounded to the span, from that log's applied marker), in parallel across shards, then flushes it and reports it `checkpointed`, so the cluster trims the history to our span ("Failure handling"). `close` checkpoints our log's marker and closes the SlateDB. `on_layout` passes layouts to the state store.
- The upstream manager uses `ClusterCursors` and follows `host_filter()`. The `HostHandler` stops the released hosts' sockets, waits (up to 5 s) for their in-flight events to land, and returns their acked cursors.
- Host records live in the bucket (`BucketHosts`, `hosts/{host shard}`: a JSON map of `state::HostRecord` per host shard, every write a CAS). It's the upstream registry's store, the `state::HostStore` behind listHosts and the policy hooks (driver, operator actions), and where host counters go. As with `StateHosts`, the registry's tier only seeds a new record. After that the policy engine owns it. `update_host` is atomic per node (one at a time) and under CAS across nodes: a conflict from another host in the same shard object is retried, one on the same host returns an error for the caller to retry.
- The policy engine runs with `PolicyHooks::with_hosts` over `BucketHosts`, and its cluster-wide budgets divide by `LiveCores` (this node plus its non-draining peers) instead of `FixedNodes(1)`.
- The admin's cluster view (`Glue::view`) is the real cluster: members from the leases, host and DID shard owners from the assignments. Every member's rates, consumers, hosts, CPU and memory come over the peer admin RPC (`node::peer_admin`, docs/admin-api.md "On a cluster"), edges and replicas included with `--admin-follower`.
- A checkpoint tick (2 s) re-reads `hostck/`, prunes the dedupe set, writes each open shard's applied marker for our log, flushes host counters, and reloads the registry.
- The sync API's repo endpoints answer for the DID shards this node holds. Hosts come from the bucket.
- With `--plc-export`, the lowest-named live core reads the PLC export and forwards each batch to the DID owners (`/internal/relay/v1/plc/apply` and `.../flush`), and a host stage that misses its DID document cache asks the DID's owner for its seed (`.../plc/pick`) before it resolves ([Policy](policy.md#plc-export-seeding)).

## Plugging it in (reference)

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

`set_host_handler` takes a `HostHandler`. Its `release(give)` is what makes a planned handoff graceful: stop every host `give` passes (the hosts of the shards being handed over), let their in-flight events land, fence them, and return each one's acked cursor. It acts on the hosts `give` names, not on whatever `mgr.set_filter` finds still running: the narrower filter is published first, and `follow_filter` may already have stopped them, which once made a SIGTERM handoff skip the drain, the fence and the final `hostck/` write. `acked()` returns the acked cursor of every subscribed host, and the cluster writes them to `hostck/{shard}` every `checkpoint_every`. A crash takeover resumes from there and the PDS replays the rest.

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

## What a DID shard's SlateDB holds (resharding families)

vlpds's clone projects a source to one key range, and vlpds keeps everything a shard owns under `0x01 ‖ slot`. vlRelay has three slot-keyed tags. vlpds now has an opt-in `partition::clone_db_families` that takes one key range per tag: it stages each tag past the first as a clone of its own and makes the child the union, all O(manifest). vlpds itself still clones with `clone_db`, unchanged. `state::CLONE_FAMILIES` lists vlRelay's three tags, and `state::tests::split_and_merge_carry_every_family` passes with all three in one process. Carrying all three first failed in a live cluster: the children's first memtable flush hit SlateDB's L0 ULID cutoff (`InvalidClockTick`) and the node fail-stopped. Every parent L0 SST carries rows of every family, so the child holds one SST behind several L0 views, and SlateDB's `merge_writer_and_compactor` cut the writer's L0 list at the first view matching the compactor's last compacted SST id, which was the wrong copy. The fork's `fix/l0-view-merge-dup-sst` (rev c7b29a06) cuts at the view id instead. The same bug could hit a plain vlpds merge of two halves that still share more than 8 parent L0s (`partition.rs` `merging_halves_that_share_many_l0s_keeps_them`). With the fix, `state::RESHARD_FAMILIES` is all three tags. The table says what that means for each family.

| Family | Key | Slot | On a split or merge |
|---|---|---|---|
| Sync records | `0x01 ‖ slot ‖ 'd' ‖ DID` | the DID's | follows the slot range |
| Archival mirrors: `V/{did}` meta, and vlpds's generation-keyed records, MST nodes and backlinks (`R/`, `c/`, `M/`, `h/`, …) | `0x01 ‖ slot ‖ family ‖ …` | the DID's | follows the slot range |
| Host records (single node only) | `0x02 ‖ slot ‖ hostname` | the hostname's | follows the slot range. Nothing in a cluster reads them (below). |
| PLC export seeds | `0x03 ‖ slot ‖ DID` | the DID's | follows the slot range. |
| Applied markers | `meta/applied/{log_id}` | none: per shard | stays with the parent. A child starts with no log history (the parents were checkpointed and closed at the freeze, so their SSTs hold everything), so it has no span to replay and no marker to start from. |

Two more things are per shard but live outside SlateDB:

- **The restart dedupe set** (`Recent`, `dedupe/{shard}/{log_id}`). A child replays none of its parents' logs, so it can't rebuild the set the way a takeover does. So a DID shard's close writes its whole set (not only the inherited entries) to `dedupe/{shard}/{our log}` and drops it from memory. The driver reads each parent's objects and writes their union to `dedupe/{child}/reshard` before the flip, and a child's open reads that along with its history's objects. Entries carry a hash of the DID, not its slot, so every child of a parent gets all of the parent's entries. One that isn't for the child's DIDs can never match an event, and it ages out with the host's checkpoint.
- **Host records and cursors.** In a cluster these are in the bucket, keyed by host shard (`hosts/`, `hostck/`), and a DID reshard doesn't touch them. The `0x02` rows are the single node's `StateHosts`, which never reshards. They stay in the DID shards because on a single node the host and DID slot spaces are the same shards, and a host's record is in the shard that serves its slot. A bucket that started on one node and later runs as a cluster leaves them in its DID shards, unread.

`tests/e2e/reshard.sh` (`just e2e-reshard`) splits and merges under load with archival on and the PLC export seeding. `state::tests::split_and_merge_carry_every_family` and vlpds's `clone_with_families_splits_and_merges_every_family` cover the families clone in one process.

