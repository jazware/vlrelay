---
title: Subscribe to the firehose
section: vlRelay
order: 2
summary: "For consumers: the subscribeRepos stream, what its seqs mean, how cursors resume on any node, and the sync endpoints next to it."
---

```hero
diagram:
  caption: Every node, core, edge or replica, serves the same events with the same seqs. Recent cursors come from each node's in-memory ring, and older ones are read back from sealed log segments in the bucket.
  nodes:
    - { id: you, label: Your consumer, sub: "`?cursor=` last seq", at: [0, 4.5], size: [9, 3] }
    - { id: lb, label: Load balancer, sub: any node, at: [12, 4.5], size: [8, 3], tone: muted }
    - { id: core, label: Core node, sub: merges every log, at: [23, 0], size: [9, 3], tone: accent }
    - { id: edge, label: Edge, sub: "~275 ms behind", at: [23, 4.5], size: [9, 3], tone: blue }
    - { id: rep, label: Replica, sub: "~275 ms behind", at: [23, 9], size: [9, 3], tone: blue }
    - { id: ring, label: Ring, sub: "512 MiB in memory", at: [36, 1.5], size: [9, 3], tone: blue }
    - { id: log, label: "`log/` segments", sub: "sealed · 72 h", at: [36, 7.5], size: [9, 3], shape: store, tone: amber }
  groups:
    - { label: on every node, around: [ring], tone: blue }
  edges:
    - "you <-> lb: subscribeRepos"
    - lb.r -> core.l
    - lb.r -> edge.l
    - lb.r -> rep.l
    - "core.r -> ring.l: recent"
    - { from: rep.r, to: log.l, label: older cursors, dash: true }
facts:
  - { value: "1, 2, 3…", label: dense seqs, note: "the same event has the same seq on every node", tone: blue }
  - { value: "72 h", label: of cursor replay, note: "older cursors get `#info OutdatedCursor`", tone: amber }
  - { value: "~5.3", unit: KB, label: per frame on average, note: "~15 Mb/s at today's ~350 events/s" }
  - { value: "2 s", label: wait before FutureCursor, note: "so a node that trails another doesn't refuse a good cursor", tone: violet }
```

vlRelay serves `com.atproto.sync.subscribeRepos` like indigo's relay, with the same sync 1.1
frames. Software written for Bluesky's relay reads it unchanged: indigo's Go consumers, `goat`,
`@atproto/sync` and Jetstream all passed against it ([Compatibility](compat.md)).


## Connecting

```bash
# the live stream, from the head
wss://relay.example.com/xrpc/com.atproto.sync.subscribeRepos

# resume after the last seq you processed
wss://relay.example.com/xrpc/com.atproto.sync.subscribeRepos?cursor=184467

# goat prints every event, checking signatures and MST proofs as it goes
goat firehose --relay-host wss://relay.example.com --verify-basic --verify-sig --verify-mst
```

The stream carries `#commit`, `#sync`, `#identity`, `#account` and `#info` frames, as binary
DAG-CBOR websocket messages. Every event has passed the relay's checks before it's sent: the
commit's signature, the MST proof against `prevData`, a `rev` that moves forward, and a host
that's allowed to speak for the account. Nothing is sent before it's durable in the bucket, so an
event you've seen is never lost in a crash.

Every node serves the stream, and `/xrpc/_health` answers on each one. Edges and replicas sit
about 275 ms behind the cores, since they follow the cores' logs over a hop and hold back a guard
for joining nodes. If you want the freshest stream, connect to a core. If you want the most
copies, put every node behind one name.

## Seqs and cursors

A seq is the event's position in the merged stream: 1, 2, 3, and so on, counted from the first
event the relay's bucket ever held. Every node computes it from the bucket's contents, so every
core, edge and replica gives the same event the same seq without talking to each other. Seqs only
go up and are never reused across restarts or takeovers. They stay well under 2^53, so a
JavaScript `number` holds them.

| Cursor | What you get |
|---|---|
| None | The live stream from the head |
| In the node's ring (the last 512 MiB) | Served from memory |
| Older, within retention (72 h) | Read back from sealed segments in the bucket, then handed to the live stream |
| Older than retention | `#info` with `OutdatedCursor`, then the stream from the oldest checkpoint left |
| Past the head | The node waits up to 2 s for its head to reach it, then sends a `FutureCursor` error and closes |

So you can resume on any node with the cursor you had. A consumer that reconnects to an edge
right after reading a core's stream may be a few hundred milliseconds ahead of it, which is why
the node waits before it calls a cursor from the future. indigo's relay ignores a future cursor
and serves live. vlRelay follows the event-stream spec and sends the error.

Backfill from the bucket doesn't touch the live path. Segments never change once written, so a
consumer catching up from yesterday reads them on its own and doesn't slow anyone else down.

## Falling behind

A consumer that reads slower than the stream falls out of the ring and catches up from the bucket.
Once it's more than 128 MiB behind the head, the node sends a `ConsumerTooSlow` error frame after
the backlog and closes the socket. Reconnect with your last seq and you continue from there,
from the bucket if need be.

At today's rate (~350 events/s, ~5.3 KB each) the whole stream is ~15 Mb/s. At 33k events/s
it's ~1.6 Gb/s, so a consumer that wants every event at high rates needs a fast link more than a
fast CPU. Each IP can hold up to 256 connections to a node.

## Account status and takedowns

`#account` frames carry `active` and `status` (`deactivated`, `takendown`, `suspended`,
`deleted`, `desynchronized`, `throttled`) as the spec describes. When an operator takes an
account down, the relay emits an `#account` with `status: takendown`. It also stops replaying the
account's earlier `#commit` and `#sync` frames to any cursor that reaches back past the takedown,
on every node. Those frames are skipped, not renumbered, so you'll see a gap in the seqs, which
the spec allows. Lifting the takedown lets the old frames replay again.

vlRelay keeps the handle in `#identity` frames. indigo's relay drops nearly every handle (it
skips handle verification), so a consumer comparing the two will see handles only on vlRelay.

## The sync endpoints

| Endpoint | Answers |
|---|---|
| `com.atproto.sync.listRepos` | Every account, with `active` and `status` (deactivated and taken-down accounts included) |
| `com.atproto.sync.getRepoStatus` | One account's status, and its `rev` while it's active |
| `com.atproto.sync.getLatestCommit` | The account's latest commit CID and rev |
| `com.atproto.sync.listHosts`, `getHostStatus` | The PDSes the relay knows, with their status and last seq |
| `com.atproto.sync.requestCrawl` | Asks the relay to subscribe to a PDS, when the operator has turned crawling on (`--crawl`) |
| `getRepo`, `getRecord`, `getBlocks` | Only on a relay running [archival mode](archival.md) |

Errors follow the PDS: `RepoNotFound` and `HostNotFound` are HTTP 400, and bad parameters are
`InvalidRequest`. In a cluster the repo endpoints (`listRepos`, `getRepoStatus`,
`getLatestCommit`) answer only for the accounts whose DID shard the answering node holds, and
return `NotOwner` for the rest. That's a known gap ([Cluster](cluster.md#known-gaps)).

## What's different from a single-box relay

- Seqs and cursors work on every node, so you can load-balance consumers and fail over without
  losing your place.
- A planned restart (a deploy) doesn't drop your stream's events. Your socket closes when the node
  you're on exits, and you resume on another node with your cursor. A crash pauses the stream
  for ~1–2 s while the dead node's accounts move.
- The relay emits sooner. On the same upstreams and load, its p50 time to firehose was ~29 ms
  against ~46–60 ms for indigo's relay, which writes to disk every 100 ms before it broadcasts.
- There's no compressed or filtered output yet. Every consumer gets the whole stream.
