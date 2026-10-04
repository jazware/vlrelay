# vlRelay cluster: plugging it into the node

## Running a cluster

Single-node mode (no `--cluster`/`--role`) is still the default and unchanged. A cluster node takes:

| Flag | Default | What |
|---|---|---|
| `--cluster` or `--role core\|edge\|replica` | | Cluster mode. `--cluster` means `--role core`. |
| `--node-id ID` | `relay` | Member name (and the log id prefix). Unique per node. |
| `--peer-listen ADDR` | `127.0.0.1:2979` | The node-to-node mTLS listener (forwarding, log streams, nudges). Core and edge. |
| `--advertise-url URL` | `https://{peer-listen}` | Where peers reach `--peer-listen`. |
| `--peer-tls-dir DIR` | | `ca.crt`, `{node-id}.crt`, `{node-id}.key`, as `vlpds admin tls ca` / `issue` write them. With `--dev-mode` they're created as needed (vlpds's `dev_files`). Core and edge. |
| `--internal-token T` | | Shared secret on every peer request. Core and edge. |
| `--lease-ttl-ms MS` | 10000 | Node lease TTL. Renewal and skew are a fifth of it each. |
| `--did-shards N`, `--host-shards N` | 4, 64 | Only used when the bucket has no layout yet. |

Every node of a cluster points at the same bucket and `--prefix`. A replica needs only read credentials and no TLS or token. `--host` and `--crawl` work on any core node: the host registry is in the bucket, so a host admitted anywhere is admitted everywhere and connects on whichever node owns its host shard (within a checkpoint tick, 2 s).

```
vlrelay --role core --node-id n1 --listen :2980 --peer-listen 10.0.0.1:2979 \
        --advertise-url https://10.0.0.1:2979 --peer-tls-dir /etc/vlrelay/tls \
        --internal-token "$TOKEN" --s3-endpoint ... --prefix relay1 --host pds.example.com
```

A node that gets SIGTERM leaves gracefully: it marks its lease draining and tells its peers, hands its host shards over (sockets closed, in-flight events landed, cursors checkpointed, recipients nudged), then its DID shards, fences its log and deletes its lease. If it can't fence its own log, it asks a peer to (below, "Failure handling"). Its consumers keep their sockets until the process exits and then resume on another node with their cursor. kill -9 is a crash: peers take its shards once the lease lapses, or sooner if its peer port refuses connections.

Locally, `just e2e-cluster` runs three cores, an edge and a replica (docs/devloop.md, "Cluster e2e").

## What node.rs does with it

`Node::start_cluster` (`src/node/cluster.rs`) builds the same pipeline as `Node::start`, with these seams moved:

- The lane's `DidOwner` is `Forwarding`: `submit` hands the checked event (kind, verified chain fields, seq span and first-sighting flag, encoded into `Forwarded::meta`) to the forwarder and returns `Submitted::Forwarded(rx)`. The lane waits on the outcome off the lane, counts it and acks the host cursor. If the forwarder gives up (no owner for 20 s), the event's ack is held (`Tracker::fail`) and the host is kicked, so it replays from its cursor.
- The DID-owner stage (`Stage`) runs each batch through the same `LocalOwner` as a single node (apply, append to this node's log, commit once durable), DIDs in parallel, each DID's events in order, and answers once each is durable. It checks the lease before answering.
- `Shards` opens a DID shard's SlateDB and replays each earlier owner's log span in order (bounded to the span, from that log's applied marker), in parallel across shards, then flushes it and reports it `checkpointed`, so the cluster trims the history to our span ("Failure handling"). `close` checkpoints our log's marker and closes the SlateDB. `on_layout` passes layouts to the state store.
- The upstream manager uses `ClusterCursors` and follows `host_filter()`. The `HostHandler` stops the released hosts' sockets, waits (up to 5 s) for their in-flight events to land, and returns their acked cursors.
- Host records live in the bucket (`BucketHosts`, `hosts/{host shard}`: a JSON map of `state::HostRecord` per host shard, every write a CAS). It's the upstream registry's store, the `state::HostStore` behind listHosts and the policy hooks (driver, operator actions), and where host counters go. As with `StateHosts`, the registry's tier only seeds a new record. After that the policy engine owns it. `update_host` is atomic per node (one at a time) and under CAS across nodes: a conflict from another host in the same shard object is retried, one on the same host returns an error for the caller to retry.
- The policy engine runs with `PolicyHooks::with_hosts` over `BucketHosts`, and its cluster-wide budgets divide by `LiveCores` (this node plus its non-draining peers) instead of `FixedNodes(1)`.
- The admin's cluster view (`Glue::view`) is the real cluster: members from the leases, host and DID shard owners from the assignments. Rates, consumers and hosts are filled in for this node only.
- A checkpoint tick (2 s) re-reads `hostck/`, prunes the dedupe set, writes each open shard's applied marker for our log, flushes host counters, and reloads the registry.
- The sync API's repo endpoints answer for the DID shards this node holds. Hosts come from the bucket.
- With `--plc-export`, the lowest-named live core reads the PLC export and forwards each batch to the DID owners (`/internal/relay/v1/plc/apply` and `.../flush`), and a host stage that misses its DID document cache asks the DID's owner for its seed (`.../plc/pick`) before it resolves ([Policy](policy.md#plc-export-seeding)).

### Restart dedupe in a cluster

A single node drops what an upstream replays past its durable cursor by reading its own log's tail. In a cluster the host owner can't read the DID owner's log, so the DID owner does it. Commits need nothing: the chain catches a replay by its rev. For `#identity`, `#account` and `#sync`, the DID owner keeps the (host, upstream seq) of each one it appended (`Recent`) until the host's checkpointed cursor in `hostck/` has passed it, and answers a second copy as a duplicate. A copy is claimed before it's applied, so two host owners sending it at once (a zombie) can't both append it.

The set survives the DID owner's crash or handoff because the applied marker it writes for its own log never passes an entry still in the set. The shard's next owner replays from that marker, and while replaying it puts the non-commit entries back in the set.

Checkpoint markers are per log (`meta/applied/{log_id}` in each shard's SlateDB). A shard's marker for our log is the lower of what's committed (as of the previous tick) and the lowest ordinal the dedupe set still holds for that shard.

## Failure handling

What the chaos runs (docs/chaos.md) changed, and why each piece is safe.

**Shard histories stay one span long.** An open replays every span of the shard's history, one log each, and each crash adds a span. So the vlpds `Cluster` is told `set_trim_spans(1)` (vlpds keeps 8): once an open has replayed every earlier span and flushed the shard's SlateDB, `ShardHost::checkpointed` says so and vlpds's `trim_owned` drops the earlier spans (`applied_epoch`), as it does for its own crash loops. The next owner replays our span alone. Their dedupe entries don't need the spans: the open wrote them to our own `dedupe/{shard}/{log}` before routing anything. A span is read only up to its end, without a LIST (`LogReplay::read_span`), once per log for every shard opening together. Before, a span was read from its start to the end of its log, however long that log lived on.

**A lapsed lease is revalidated, not fatal.** A pause (GC, VM migration, SIGSTOP) between the end of the lease's validity and the time peers may presume us dead used to kill a node that nobody had replaced. The cluster opts into vlpds's `set_revalidate`: within 2 x skew + TTL of validity, the next renewal CAS-renews our own lease and checks our log for a fence (the heads of `durable_end..=next_ordinal`, where a peer's fence would be: a few GETs, not a LIST). The lease counts as valid again only once that check passes. Unfenced, the node carries on. Fenced (a peer presumed us dead, and it fences before it takes anything), or the lease rewritten, it fail-stops. A fence landing after the check fails our next segment PUT, as for any zombie. Meanwhile the log holds instead of failing (`LogConfig::lapse_grace`): nothing is sealed or acked until the lease is valid again. The watchdog fail-stops past the same window.

**A planned leave fences at the known end.** Quiesced, our log's end is `durable_end`, so vlpds's shutdown PUTs the fence there (create-only) instead of scanning the log first: the scan LISTs the whole log and outlasted the TTL-long budget on a long-lived log under load. If the leave can't finish (our own fence failed, or our lease lapsed under the leave for 300 ms: our bucket path is the problem), the leaving node asks its peers to fence its log (`/internal/relay/v1/cluster/fence`, 2 s). A fence by anyone ends the log at the same place, and a peer holding it fenced treats our lease as dead, so peers take over without waiting out the lease. Only if no peer can either does the node exit nonzero with its lease in place, as before.

**Reachability.** Membership is the bucket lease, so a core that reaches the bucket but not its peers (or that its peers can't reach) stays live and keeps its shards. Should reachability feed ownership? Not between nodes: a peer deciding that another is unreachable is a second failure detector racing the lease, it can't tell "I can't reach B" from "B is down" or "my own link is down", and in a symmetric two-node partition both sides would accuse each other. What's sound is a node judging itself and giving itself up, because fencing makes any self fail-stop safe. So each core probes its own advertised peer address every renewal, with a hello as a peer would send it through whatever sits in front of it. If that fails for a TTL while it has live peers, the core does a planned leave (bounded by a TTL: its shards go to peers over the links that still work, its log is fenced, or a peer is asked to fence it) and fail-stops. Its restart doesn't join while its own address doesn't answer (vlpds's `ShardHost::may_join`), so it doesn't take shards only to give them up a TTL later. Peers hand host shards only to members that have joined, or a held restart would sit on its share of hosts. The costs: a node whose own runtime is too starved to answer itself for a TTL also steps down (it isn't serving its peers either), and a path that's broken only from the peers' side (an asymmetric firewall) isn't seen, so the lease still rules there. Forwards to an owner that hangs are re-routed once its shards move (the forwarder polls owners every 100 ms while a request is out), so they wait for the step-down, not the 10 s RPC timeout.

**A slow log steps down too.** Every node's merge waits on every live log's watermark, so a core whose own bucket path crawls holds everyone's firehose until its lease gives out. The log reports how long its oldest append has waited to be durable (`NodeLog::pending_age`). Past max(TTL/3, 1 s) and past 4x the median peer log's watermark age, for a TTL, the core does the same bounded leave and fail-stop. The ratio is what keeps a bucket slow for everyone (where every core's appends wait as long) from emptying the cluster; a first version compared with a fixed bound and a core stepped down under uniform 100-500 ms latency.

**Forwarding lanes don't couple owners.** A lane keeps up to 4 batches in flight, one per owner, and never one DID in two of them: an event whose DID is in flight waits, in order, for that batch. So a slow DID owner (a slow bucket path, a pause) holds back the events of its own DIDs only. Before, a lane waited for every owner in its batch, and every lane holds DIDs of every owner, so one slow core set every node's forwarding pace.

**Replicas read the bucket through a window.** `follow::follow_bucket` keeps a window of GETs past the last segment it delivered (2 when caught up, doubling to 32 while each lands), delivers in order up to the first missing one or the fence, polls every 20 ms, and LISTs (for retention pruning) only after 5 s without a new segment. vlpds's `catch_up`, which replicas used, did one GET per segment and a LIST plus a 100 ms sleep at the end.

## Results

Local, M-series Mac, dev build, `tests/e2e/cluster.sh --duration 80` (defaults: 50 writes/s, 30 accounts, 4 upstreams with `DEV_PDS=3`, lease TTL 3 s, 8 DID shards, 15 host shards). Five checker streams: one per core, each listing all three cores to fail over to, plus the edge and the replica.

| Stream | Missing | Extra | Reordered | Dups | Matched | Same seqs as core1 |
|---|---|---|---|---|---|---|
| core1, core2, core3 | 0 | 0 | 0 | 0 | 4,173 | yes |
| edge, replica | 0 | 0 | 0 | 0 | 4,173 | yes |

That's with a kill -9 of the busiest core at 20 s, its restart at 35 s, a SIGTERM of the next busiest core at 50 s and its restart at 60 s.

Steady state, upstream emit to relay emit (before the first HA event):

| | p50 | p90 | p99 | max |
|---|---|---|---|---|
| 1 node (`just e2e --bucket`, same network and load) | 29.6 ms | 50.4 ms | 91.9 ms | 120 ms |
| 3 cores, each core's stream | 33.4 ms | 37.3 ms | 52 ms | 61 ms |
| edge | 307 ms | 347 ms | 364 ms | 372 ms |
| replica | 308 ms | 348 ms | 365 ms | 415 ms |

Most DIDs' events take one forward hop on 3 nodes, and every core merges three logs. The edge and the replica sit about 275 ms further back: the merge `guard` (250 ms behind their last lease listing) plus polling.

HA (the worst latency of any event that reached a stream after each action, until the next one):

| Action | Shards moved | Worst latency on the core streams | Edge / replica |
|---|---|---|---|
| kill -9 the busiest core | DID shards opened 0.98 s after the kill, host shards 1.1 s | 1.58-1.82 s | 1.77 / 1.67 s |
| restart it (rebalance) | | 0.18 s | 0.45 / 0.86 s |
| SIGTERM a core (planned handoff) | host shards handed at once, DID shards open 0.38-0.46 s after (SlateDB open ~30 ms, span replay ~250 ms), process gone after 0.30 s | 0.66-0.89 s | 1.0 / 1.0 s |
| restart it (rebalance) | | 1.2 s | 1.48 / 1.47 s |

The takeover after kill -9 came well inside the 3 s TTL because the dead node's peer port refused connections (`ShardHost::refused`). A node that hangs instead of dying costs TTL + skew (3.6 s here, 12 s with the defaults).

The first planned-handoff runs paused 1.3-2.4 s: peers kept handing host shards back to the leaving node until they listed its draining lease, and the shards bounced. The leave now writes its draining lease first (`Cluster::announce_drain`, new in vlpds) and nudges every peer with its log id, which they treat as draining at once.

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
| `hosts/` | `node::cluster::BucketHosts` | host records (the registry) per host shard |
| `log/{log_id}/` | `seq::NodeLog` | each core node's log, fenced at its end |
| `dedupe/{did shard}/{log_id}` | `node::cluster::DedupeStore` | a DID shard owner's inherited restart-dedupe entries, while it has any |
| `seqck/` | `seq::dense` (core nodes) | stream seq checkpoints: consumers see dense seqs, not merge keys (docs/seq.md) |

## Known gaps

- An idle core log holds a replica's merge until its next segment, since a replica sees only segments. Busy logs never notice. The fix is an empty heartbeat segment from a log that's been idle for a while.
- Edges and replicas follow a new core node's log once they list its lease. They hold their merge `guard` (250 ms) behind their last listing so the joiner's first events don't land below it. That assumes clocks agree to within `guard`.
- Segments still reach replicas by polling the bucket (a window of concurrent GETs, "Failure handling"). Pushing them to edges is already done (the mTLS stream). Push to replicas isn't, which is M5.
- Cores and edges catch a log up from the bucket on every stream (re)connect with vlpds's `remote::catch_up`, one GET per segment. Under bucket latency a reconnect after a long gap is slow; the replica's window would fix it, but it's vlpds code.
- Every core node runs log retention (`spawn_retention`). Harmless but redundant: it could move to the slot-0 leader (`cluster.leads_slot0()`) as vlpds does.
- The sync API's repo endpoints (listRepos, getRepoStatus, getLatestCommit) answer only for the DID shards the node holds, and a DID elsewhere is a NotOwner error. They should forward to the owner, or listRepos should merge across nodes.
- Admin takedowns (`append_own`) and the admin's account lookups read this node's state only, so they work only on the DID's owner. The cluster view shows peers' rates, CPU and memory as 0 until peers report them.
- Fixed since (docs/chaos.md): dedupe across two crashes in one checkpoint interval (inherited entries are persisted per shard), a duplicate answered before the first copy was durable (the zombie-plus-crash gap; bucket errors hit it without a zombie), and FutureCursor seq collisions in the dedupe set (entries carry the DID). docs/chaos.md lists what the chaos runs found open.
- Each host owner's registry flush writes every host shard object that has a dirty row, every 2 s: one PUT per active host shard per tick.
