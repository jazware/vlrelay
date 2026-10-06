---
title: Lease cluster
section: vlRelay
order: 8
summary: "Core nodes share host shards and DID shards through leases in the bucket, merge every log into one stream, and hand shards over on SIGTERM or take them over after a crash. Edges and replicas add egress."
---

```hero
diagram:
  caption: Three cores on one bucket and prefix. Each holds a lease, owns some host shards and some DID shards, writes its own log and streams it to its peers. An edge follows the cores over mTLS, and a replica follows the same logs from the bucket with read-only credentials.
  nodes:
    - { id: c1, label: core n1, sub: "lease · log n1", at: [0, 0], size: [9, 3], tone: accent }
    - { id: c2, label: core n2, sub: "lease · log n2", at: [0, 5], size: [9, 3], tone: accent }
    - { id: c3, label: core n3, sub: "lease · log n3", at: [0, 10], size: [9, 3], tone: accent }
    - { id: edge, label: Edge, sub: "no lease · no shards", at: [14, 14.5], size: [9, 3], tone: blue }
    - { id: nodes, label: "`nodes/` `assign/`", sub: leases · DID shards, at: [27, 0], size: [10, 2.6], shape: store, tone: amber }
    - { id: hosts, label: "`assign-hosts/` `hostck/`", sub: host shards · cursors, at: [27, 3.4], size: [10, 2.6], shape: store, tone: amber }
    - { id: state, label: "`state/`", sub: SlateDB per DID shard, at: [27, 6.8], size: [10, 2.6], shape: store, tone: amber }
    - { id: log, label: "`log/{log_id}/`", sub: one per core · fenced at its end, at: [27, 10.2], size: [10, 2.6], shape: store, tone: amber }
    - { id: rep, label: Replica, sub: read-only credentials, at: [27, 14.5], size: [10, 3], tone: blue }
  groups:
    - { label: peer mTLS · forwards and log streams, around: [c1, c2, c3], tone: accent }
    - { label: one bucket and prefix, around: [nodes, hosts, state, log], tone: amber }
  edges:
    - "c1 <-> c2: forward"
    - "c2 <-> c3: log streams"
    - { from: c1.r, to: nodes.l, label: lease CAS, dash: true }
    - { from: c2.r30, to: hosts.l, label: checkpoint }
    - { from: c2.r70, to: state.l, label: apply }
    - { from: c3.r, to: log.l, label: append }
    - { from: c3.b, to: edge.l, label: mTLS, tone: blue, via: [[4.5, 16]] }
    - { from: log.b, to: rep.t, label: GET window, tone: blue, dash: true }
facts:
  - { value: "10 s", label: lease TTL, note: "`--lease-ttl-ms`; renewal and clock skew are a fifth of it each", tone: accent }
  - { value: "~1 s", label: to take a crashed core's shards, note: "when its peer port refuses connections; ~12 s if it hangs", tone: violet }
  - { value: "< 0.5 s", label: planned handoff, note: "SIGTERM; host shards at once, DID shards open 0.38–0.46 s later", tone: blue }
  - { value: "0", label: events missing or reordered, note: "5 streams through a kill -9 and a SIGTERM, same seqs on all", tone: amber }
```

This is the first cluster design, kept for comparison: the [quorum cluster](quorum-cluster.md)
replaces it, and a node runs it only with `--legacy-cluster`.

A vlRelay cluster is several core nodes on one bucket and prefix. Each core holds a lease, owns
some host shards (which PDSes it subscribes to) and some DID shards (whose state it keeps and
whose events it writes to its own log), and merges every core's log into the same stream.
Membership is the lease in the bucket, and ownership changes are compare-and-swap writes there, so
there's nothing else to run. Consumers can connect to any core, edge or replica, and resume on
another with their cursor.

## Roles

| Role | Lease | Shards | Log | Follows peers | Needs |
|---|---|---|---|---|---|
| core | yes | DID and host | its own | over mTLS | bucket read and write, a peer certificate |
| edge | no | none | none | over mTLS | bucket read, a peer certificate |
| replica | no | none | none | from the bucket | bucket read only |

A core with no peers is a fine single node. A node started without `--cluster` or `--role` runs
alone, the default.

## Running a cluster

| Flag | Default | What |
|---|---|---|
| `--cluster` or `--role core\|edge\|replica` | | Cluster mode. `--cluster` means `--role core`. |
| `--node-id ID` | `relay` | Member name (and the log id prefix). Unique per node. |
| `--peer-listen ADDR` | `127.0.0.1:2979` | The node-to-node mTLS listener (forwarding, log streams, nudges). Core and edge. |
| `--advertise-url URL` | `https://{peer-listen}` | Where peers reach `--peer-listen`. |
| `--peer-tls-dir DIR` | | `ca.crt`, `{node-id}.crt`, `{node-id}.key`, as `vlpds admin tls ca` and `issue` write them. With `--dev-mode` they're created as needed. Core and edge. |
| `--internal-token T` | | Shared secret on every peer request. Core and edge. Not enough alone (see [Peer authorization](#peer-authorization)). |
| `--lease-ttl-ms MS` | 10000 | Node lease TTL. Renewal and skew are a fifth of it each. |
| `--did-shards N`, `--host-shards N` | 24 in a cluster (4 alone), 64 | Only read when the bucket has no layout yet. |

Every node of a cluster points at the same bucket and `--prefix`. A replica needs only read
credentials, and no TLS or token. `--host` and `--crawl` work on any core: the host registry is in
the bucket, so a host admitted anywhere is admitted everywhere, and it connects on whichever core
owns its host shard within a checkpoint tick (2 s).

```bash
vlrelay --role core --node-id n1 --listen :2980 --peer-listen 10.0.0.1:2979 \
        --advertise-url https://10.0.0.1:2979 --peer-tls-dir /etc/vlrelay/tls \
        --internal-token "$TOKEN" --s3-endpoint ... --prefix relay1 --host pds.example.com
```

Each core takes at most its fair share of shards, `ceil(shards / cores)`. The default of 24 DID
shards splits evenly over 2, 3, 4, 6 and 8 cores and leaves none empty at 5. [Deploy](operations/deploy.md#a-cluster)
has the rest of the setup.

### Leaving and crashing

A core that gets SIGTERM leaves gracefully. It marks its lease draining and tells its peers, hands
its host shards over (sockets closed, in-flight events landed, cursors checkpointed, recipients
nudged), then its DID shards, fences its log and deletes its lease. If it can't fence its own log,
it asks a peer to. Its consumers keep their sockets until the process exits, then resume on
another node with their cursor.

kill -9 is a crash. Peers take the dead core's shards once its lease lapses (TTL plus a fifth of
it, 12 s by default), or within about a second if its peer port refuses connections.

### Peer authorization

Every core and edge holds the internal token and a certificate from the cluster CA, and the CA
admits any of them. An edge is the most exposed member, so neither is enough on its own to change
anything. Each peer request is bound to the node its client certificate names (the
`vlpds://node/<id>` URI SAN), and the peer router checks it against the leases:

| Route | Who may call it |
|---|---|
| log stream, hello | any member (edges follow core logs, joiners greet); a hello must name its caller |
| forward, nudge, key invalidation, peer admin, archival reads, PLC seeding | a core whose lease we list as live (draining included, since a leaving node still forwards and nudges); a nudge's `leaving` must be the caller's own log |
| fence `log_id` | the node that wrote `log_id` (a leave whose own fence failed), or a leased core while the bucket shows that log's lease draining, expired, replaced or gone |

Anything else gets a 403 and counts in `vlrelay_peer_refused_total`. A core that its peers
presumed dead is refused until they list it again, which is what a zombie should get. The token
is still checked on every route.

## Restart dedupe

When a host's owner changes, the new owner resumes the PDS's stream from the last checkpointed
cursor, and the PDS replays everything after it. Some of those events were already appended. A
single node drops them by reading its own log's tail. In a cluster the host owner can't read the
DID owner's log, so the DID owner does it.

- Commits need nothing, since the account's chain catches a replay by its rev.
- For `#identity`, `#account` and `#sync`, the DID owner keeps the (host, upstream seq) of each one
  it appended until the host's checkpointed cursor in `hostck/` has passed it, and answers a
  second copy as a duplicate. Only a host with no checkpoint yet ages its entries out (15 minutes).
- A host whose sequence restarted (a new generation in `hostck/`) loses its entries at the next
  tick, since they belong to the old sequence.
- A copy is claimed before it's applied, so two host owners sending it at once (a zombie) can't
  both append it.

The set survives the DID owner's crash or handoff. The applied marker it writes for its own log
never passes an entry still in the set, the shard's next owner replays from that marker, and the
replay puts the non-commit entries back. `vlrelay_cluster_dedupe_entries` counts what's held.

## Failure handling

What the chaos runs changed, and why each piece is safe.

### Shard histories stay one span long

Opening a DID shard replays every span of the shard's history (one log each), and each crash adds
a span. So once an open has replayed every earlier span and flushed the shard's SlateDB, the
cluster drops the earlier spans. The next owner replays only the last one. Restart-dedupe
entries don't need the old spans, because the open wrote them to `dedupe/{shard}/{log}` before
routing anything. A span is read only up to its end, without a LIST, once per log for every shard
opening together.

### A lapsed lease is revalidated

A pause (GC, VM migration, SIGSTOP) between the end of a lease's validity and the time peers may
presume the node dead used to kill a node that nobody had replaced. Now, within 2 × skew + TTL of
validity, the next renewal CAS-renews the node's own lease and checks its log for a fence (a few
GETs where a peer's fence would be, no LIST). The lease counts as valid again only once that check
passes. Unfenced, the node carries on. If it's been fenced or its lease was rewritten, it
fail-stops. A fence landing after the check fails the node's next segment PUT, as it would for any
zombie. Meanwhile the log holds: nothing is sealed or acked until the lease is valid again, and
the watchdog fail-stops past the same window.

### A planned leave fences at the known end

Once a leaving node has quiesced, its log ends at its durable end, so shutdown PUTs the fence
there (create-only) instead of scanning the log first. The scan LISTs the whole log, and on a
long-lived log under load it outlasted the shutdown's budget. If the leave can't finish (its own
fence failed, or its lease lapsed during the leave for 300 ms), the leaving node asks its peers
to fence its log (2 s). A fence by anyone ends the log at the same place, and a peer that holds
it fenced treats the lease as dead, so peers take over without waiting out the lease. Only if no
peer can either does the node exit nonzero with its lease in place.

### A shard close is bounded

Closing DID shards waits for their in-flight batches, inside a step that holds a lock. A batch
whose segment PUTs retry without end would wedge every later step, so past the lapse window
(TTL + 2 × skew) the close fail-stops the node, as the watchdog would. The close's own bucket
writes (the shard's applied marker and dedupe set) are retried while the lease holds, so one
failed write doesn't fail-stop it.

### Reachability

Membership is the bucket lease. So a core that reaches the bucket but not its peers, or that its
peers can't reach, stays live and keeps its shards. Peers don't judge each other: a peer that
decides another is unreachable is a second failure detector racing the lease. It can't tell "I
can't reach B" from "B is down" or "my own link is down", and in a symmetric two-node partition
both sides would accuse each other.

What's sound is a node judging itself and giving itself up, because fencing makes any self
fail-stop safe. So each core probes its own advertised peer address every renewal, with a hello
as a peer would send it. If that fails for a TTL while it has live peers, the core does a planned
leave (bounded by a TTL) and fail-stops. Its restart doesn't join while its own address doesn't
answer, so it doesn't take shards only to give them up a TTL later. Peers hand host shards only to
members that have joined.

There are two costs. A node whose runtime is too starved to answer itself for a TTL also steps
down (it isn't serving its peers either). And a path that's broken only from the peers' side (an
asymmetric firewall) isn't seen, so the lease still rules there. Forwards to an owner that hangs
are re-routed once its shards move (the forwarder polls owners every 100 ms while a request is
out), so they wait for the step-down, not the 10 s RPC timeout.

### A slow log steps down too

Every node's merge waits on every live log's watermark, so a core whose own bucket path crawls
holds everyone's firehose until its lease gives out. So each log reports how long its oldest
append has waited to be durable. Past max(TTL/3, 1 s) and past 4× the median peer's own pending
age, for a TTL, the core does the same bounded leave and fail-stop. Each core publishes that age
in its lease, timed on its own clock, and no peer reporting one means no step-down. Comparing with
the peers is what keeps a bucket that's slow for everyone from emptying the cluster. A first
version used a fixed bound, and a core stepped down under uniform 100–500 ms latency.

### Forwarding lanes don't couple owners

A forwarding lane keeps up to 4 batches in flight, one per owner, and never one DID in two of them.
An event whose DID is in flight waits, in order, for that batch. So a slow DID owner holds back
the events of its own DIDs only. Before, a lane waited for every owner in its batch, and since
every lane holds DIDs of every owner, one slow core set every node's forwarding pace.

### Replicas read the bucket through a window

A replica keeps a window of GETs past the last segment it delivered (2 when caught up, doubling to
32 while each lands), delivers in order up to the first missing one or the fence, and polls every
20 ms. It LISTs (for retention) only after 5 s without a new segment.

## Resharding

A DID shard can be split, or two adjacent ones merged, online. It's vlpds's reshard. The layout
carries the op, the parents' owners close and freeze them, the driver clones the children from
the frozen parents and flips the layout, and the children are then ordinary free shards that any
core takes. The forwarder holds a parent's events (it retries for up to 20 s) from the freeze
until a child opens. The clone is O(manifest), so the pause is the freeze, the clone and an open,
well under a second.

There's no automatic policy. An operator triggers it on any core's admin API:

```bash
curl -u admin:$TOKEN -H 'content-type: application/json' localhost:2980/admin/api/cluster/layout
curl -u admin:$TOKEN -H 'content-type: application/json' localhost:2980/admin/api/cluster/reshard \
     -d '{"op": "split", "shard": 3, "wait": true}'          # at the midpoint, or "at": <first slot of the right half>
curl ... -d '{"op": "merge", "left": 8, "right": 9, "wait": true}'   # adjacent, left holds the lower slots
curl ... -d '{"op": "abort"}'                                # only before the flip
```

`wait` answers once the op flipped (`done: true`) or was aborted, at most 120 s. Any core can
drive, since the op is in the bucket, and a driver that dies is replaced by the lowest-named live
core.

| What a DID shard holds | Key | On a split or merge |
|---|---|---|
| Sync records | `0x01 ‖ slot ‖ 'd' ‖ DID` | follows the slot range |
| Archival mirrors (`V/{did}` and vlpds's record, MST and head rows) | `0x01 ‖ slot ‖ family ‖ …` | follows the slot range |
| Host records (single node only) | `0x02 ‖ slot ‖ hostname` | follows the slot range, and nothing in a cluster reads them |
| PLC export seeds | `0x03 ‖ slot ‖ DID` | follows the slot range |
| Applied markers | `meta/applied/{log_id}` | stay with the parent. A child starts with no log history, since the parents were checkpointed and closed at the freeze. |

Two more things are per shard but live outside SlateDB:

- The restart dedupe set. A child replays none of its parents' logs, so it can't rebuild the set
  the way a takeover does. So a shard's close writes its whole set to `dedupe/{shard}/{log}`, and
  the driver writes the union of each parent's objects to `dedupe/{child}/reshard` before the
  flip. Entries carry a hash of the DID, so every child gets all of its parent's entries, and one
  that isn't for the child's DIDs never matches an event and ages out with the host's checkpoint.
- Host records and cursors are keyed by host shard (`hosts/`, `hostck/`), so a DID reshard doesn't
  touch them.

`just e2e-reshard` splits and merges under load with archival on and the PLC export seeding.

## What's in the bucket

| Prefix | What |
|---|---|
| `nodes/`, `writers/`, `assign/`, `cluster/version` | Leases, writer bytes, DID shard assignments and the layout |
| `assign-hosts/` | The host shard layout and assignments |
| `hostck/` | Upstream cursors per host shard, each with its sequence generation (bumped on `FutureCursor`, so a stale owner's cursor of the old sequence can't win). Each object also carries the host assignment epoch of the owner that claimed it, and a write from a lower epoch is refused. Nothing is checkpointed while the writer's lease is lapsed. |
| `hosts/` | Host records (the registry), per host shard |
| `log/{log_id}/` | Each core's log, fenced at its end |
| `dedupe/{did shard}/{log_id}` | A DID shard owner's inherited restart-dedupe entries, while it has any |
| `seqck/` | Stream seq checkpoints ([Stream seqs](seq.md#checkpoints)) |
| `state/` | One SlateDB per DID shard |
| `policy/`, `cases/` | The policy, domain rules, takedowns and cases ([Policy](policy.md)) |

## Results

Local, on a laptop, dev build: `just e2e-cluster --duration 80` with 50 writes/s, 30 accounts, 4
upstreams, lease TTL 3 s, 8 DID shards and 15 host shards. Five checker streams: one per core,
each listing all three cores to fail over to, plus the edge and the replica. The run kills the
busiest core with kill -9 at 20 s and restarts it at 35 s, then sends SIGTERM to the next busiest
at 50 s and restarts it at 60 s.

| Stream | Missing | Extra | Reordered | Dups | Matched | Same seqs as core 1 |
|---|---|---|---|---|---|---|
| core 1, 2, 3 | 0 | 0 | 0 | 0 | 4,173 | yes |
| edge, replica | 0 | 0 | 0 | 0 | 4,173 | yes |

Steady state, upstream emit to relay emit, before the first HA event:

| | p50 | p90 | p99 | max |
|---|---|---|---|---|
| 1 node (same network and load) | 29.6 ms | 50.4 ms | 91.9 ms | 120 ms |
| 3 cores, each core's stream | 33.4 ms | 37.3 ms | 52 ms | 61 ms |
| edge | 307 ms | 347 ms | 364 ms | 372 ms |
| replica | 308 ms | 348 ms | 365 ms | 415 ms |

Most events take one forward hop on 3 nodes, and every core merges three logs. The edge and the
replica sit about 275 ms further back: the merge guard (250 ms behind their last lease listing)
plus polling.

The worst latency of any event that reached a stream after each action, until the next one:

| Action | Shards moved | Worst on the core streams | Edge / replica |
|---|---|---|---|
| kill -9 the busiest core | DID shards opened 0.98 s after the kill, host shards 1.1 s | 1.58–1.82 s | 1.77 / 1.67 s |
| restart it (rebalance) | | 0.18 s | 0.45 / 0.86 s |
| SIGTERM a core | host shards handed at once, DID shards open 0.38–0.46 s after (SlateDB open ~30 ms, span replay ~250 ms), process gone after 0.30 s | 0.66–0.89 s | 1.0 / 1.0 s |
| restart it (rebalance) | | 1.2 s | 1.48 / 1.47 s |

The takeover after kill -9 came well inside the 3 s TTL because the dead node's peer port refused
connections. A node that hangs instead of dying costs TTL plus skew (3.6 s here, 12 s with the
defaults).

## Known gaps

- An idle core log holds a replica's merge until its next segment, since a replica sees only
  segments. Busy logs never notice. The fix is an empty heartbeat segment from a log that's been
  idle for a while.
- Edges and replicas follow a new core's log once they list its lease. They hold their merge
  guard (250 ms) behind their last listing so the joiner's first events don't land below it, which
  assumes clocks agree to within that.
- Segments reach replicas by polling the bucket. Edges get them pushed over mTLS, and push to
  replicas isn't built.
- Cores and edges catch a log up from the bucket on every stream reconnect with one GET per
  segment, so a reconnect after a long gap is slow under bucket latency.
- Every core runs log retention. It's harmless but redundant.
- The sync API's repo endpoints (`listRepos`, `getRepoStatus`, `getLatestCommit`) answer only for
  the DID shards the node holds, and a DID elsewhere gets a `NotOwner` error. They should forward
  to the owner, or `listRepos` should merge across nodes.
- Each host owner's registry flush writes every host shard object that has a dirty row, every 2 s:
  one PUT per active host shard per tick ([Cost](cost.md)).
- Resharding leaves the retired parents' state and `dedupe/{parent}/` objects in the bucket, and
  there's no reshard policy (split by size or write rate), only the admin trigger.
