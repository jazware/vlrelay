---
title: Overview
section: vlRelay
order: 1
summary: An atproto relay whose only durable state is an object store. It subscribes to PDSes, checks every event against sync 1.1, and serves one firehose with the same seqs from every node.
---

```hero
diagram:
  caption: PDSes are spread over the core nodes by host. Each event hops to the node that owns its account, lands in that node's log in the bucket, and every node merges every log into the same stream. Edges and replicas serve that stream without taking any upstreams.
  nodes:
    - { id: pds, label: PDSes, sub: subscribeRepos, at: [0, 4.5], size: [8, 3], tone: muted, stack: true }
    - { id: c1, label: core 1, sub: "host + DID shards · log", at: [12, 0], size: [9, 3], tone: accent }
    - { id: c2, label: core 2, sub: "host + DID shards · log", at: [12, 4.5], size: [9, 3], tone: accent }
    - { id: c3, label: core 3, sub: "host + DID shards · log", at: [12, 9], size: [9, 3], tone: accent }
    - { id: pol, label: "`policy/`", sub: limits · rules · cases, at: [27, 0], size: [10, 2.6], shape: store, tone: amber }
    - { id: ctl, label: "`nodes/` `hostck/`", sub: leases · host cursors, at: [27, 3.2], size: [10, 2.6], shape: store, tone: amber }
    - { id: state, label: "`state/`", sub: SlateDB per DID shard, at: [27, 6.4], size: [10, 2.6], shape: store, tone: amber }
    - { id: log, label: "`log/`", sub: "segments · 72 h", at: [27, 9.6], size: [10, 2.6], shape: store, tone: amber }
    - { id: cons, label: Consumers, sub: AppViews · Jetstream, at: [0, 15.5], size: [8, 3], tone: blue }
    - { id: edge, label: Edge, sub: follows cores over mTLS, at: [12, 15.5], size: [9, 3], tone: blue }
    - { id: rep, label: Replica, sub: follows the bucket, at: [27, 15.5], size: [10, 3], tone: blue }
  groups:
    - { label: vlRelay cores, around: [c1, c2, c3], tone: accent }
    - { label: object store · the only durable state, around: [pol, ctl, state, log], tone: amber }
  edges:
    - pds.r30 -> c1.l
    - "pds.r -> c2.l: by host"
    - pds.r70 -> c3.l
    - "c1 <-> c2: forward to DID owner"
    - c2 <-> c3
    - { from: c1.r30, to: pol.l, label: reload, dash: true }
    - { from: c1.r70, to: ctl.l, label: lease CAS, dash: true }
    - { from: c2.r, to: state.l, label: apply }
    - "c3.r -> log.l: append, then ack"
    - { from: c3.b, to: edge.t, label: log streams, tone: blue }
    - { from: log.b, to: rep.t, label: segments, tone: blue, dash: true }
    - { from: c3.l, to: cons.t, label: subscribeRepos, tone: blue }
    - { from: edge.l, to: cons.r, tone: blue }
facts:
  - { value: "1", unit: bucket, label: is the only durable state, note: "log, per-account state, leases and policy in S3, R2, GCS or MinIO" }
  - { value: "~94k", unit: events/s, label: on one 8-core node, note: "measured, every event checked; Bluesky averages ~350/s", tone: amber }
  - { value: "1", unit: stream, label: with the same seqs on every node, note: "a consumer resumes on any node with its cursor", tone: blue }
  - { value: "< 1 s", label: planned handoff, note: "on SIGTERM; a crash moves shards in ~1–2 s, ~12 s if the node hangs", tone: violet }
```

vlRelay is an [atproto](https://atproto.com) relay written in Rust. It connects to PDSes, reads
each one's `subscribeRepos` stream, checks every event against the sync 1.1 rules and re-emits
them as one combined firehose. Its only durable state is an object store, so a node keeps nothing
on local disk and losing one only costs its caches. Consumers see a normal relay: indigo's Go
consumers, `goat`, `@atproto/sync` and Jetstream read it unchanged.

It's built from the parts of [vlpds](https://github.com/jazware/vlpds) that worked: its log,
leases, firehose merger, SlateDB state and peer TLS. A relay's input is small (Bluesky's whole
network averages ~350 events a second), so the hard parts are elsewhere. They're fan-out, keeping
consumers' streams alive across restarts, cursors that work on any node, and thousands of
upstreams with their own failure modes. [Design](design.md) has the reasoning.

## The path of an event

```diagram
caption: One event, from a PDS's socket to every consumer. The host owner does the stateless checks before anything crosses the network, and the DID owner does the stateful ones. Nothing reaches the firehose before it's durable in the bucket.
nodes:
  - { id: pds, label: PDS, sub: "`#commit` frame", at: [0, 0], size: [7, 3], tone: muted }
  - { id: host, label: Host owner, sub: parse · signature · MST proof, at: [12, 0], size: [10, 3], tone: accent }
  - { id: did, label: DID owner, sub: host · rev · prevData, at: [27, 0], size: [9, 3], tone: accent }
  - { id: seg, label: "`log/…` segment", sub: If-None-Match PUT, at: [40, 0], size: [9, 3], shape: store, tone: amber }
  - { id: merge, label: Merger, sub: every node's logs, at: [40, 6.5], size: [9, 3], tone: blue }
  - { id: subs, label: Consumers, sub: dense seq, at: [27, 6.5], size: [9, 3], tone: blue }
  - { id: ack, label: Host cursor, sub: "acked · `hostck/`", at: [12, 6.5], size: [10, 3], tone: solid }
edges:
  - "pds -> host: websocket"
  - "host -> did: forward"
  - "did -> seg: append"
  - "seg -> merge: durable"
  - "merge -> subs: subscribeRepos"
  - { from: host.b, to: ack.t, label: once durable }
```

- The host owner reads the frame off the PDS's websocket. It checks the CBOR, the size limits,
  the commit's signature against the account's cached signing key and the inductive proof (the
  ops applied to the partial tree in the CAR give the commit's `data`). A signature check costs
  ~33 µs of CPU, so one core checks ~30k events a second.
- It forwards the event to the node that owns the account's DID shard. On three nodes about two
  thirds of events take that hop, and the forward path batches them.
- The DID owner runs the checks that need state. The event must come from the host the account's
  DID document names, its `rev` must move forward, and its `prevData` must match the stored data
  CID. Then it appends the event to its node's log. The log groups whatever queued up during the
  last PUT into one segment (25 ms linger by default) and writes it with `If-None-Match: *`.
- Once the segment is durable, the DID owner commits the account's new state and answers the host
  owner, which only then counts the upstream event as done. The host's cursor is checkpointed to
  the bucket every 2 s, so a takeover resumes the PDS's stream from there and the PDS replays the
  rest.
- Every node merges every node's log, and emits an event once every log's durable watermark has
  passed it. So every node sends the same events in the same order.

A failed check drops the event and counts against its host. A broken chain marks the account
desynchronized until a `#sync` resets it, as sync 1.1 expects. Details: [Design](design.md),
[Cluster](cluster.md).

## Host shards and DID shards

A node owns two kinds of shards, and each kind has one owner at a time.

| | Host shards | DID shards |
|---|---|---|
| Decide | which node subscribes to which PDS | which node keeps each account's state and writes its events |
| Keyed by | the hostname's hash, 64 shards | the DID's hash: 65,536 slots in shards, 24 in a cluster (4 alone) |
| Owner keeps | the websockets, per-host limits, cursors | one SlateDB per shard: rev, data CID, host, status, key |
| Moves when | a node joins, leaves or dies | the same, or an operator splits or merges a shard |

The two jobs have different natural keys. One websocket carries a whole host's accounts, but an
account's state has to change in exactly one place. Keeping them apart also makes account
migration boring. When a DID moves to another PDS, its events start arriving through a different
host owner, but they still land at the same DID owner, which decides whether the new host may
speak for it.

Ownership works the way it does in vlpds. Each node renews one lease, and shard assignments are
compare-and-swap writes on objects in the bucket, so there's no consensus service to run.
Details: [Cluster](cluster.md).

## One stream on every node

Consumers see seqs 1, 2, 3, … like indigo's relay. The seq of an event is its position in the
merged stream, and that's a pure function of what's in the bucket. So every core, edge and
replica gives the same event the same seq without talking to each other, and a consumer can put
the nodes behind a load balancer and resume on any of them. Old cursors are read back from sealed
segments, which never change, so a consumer catching up from yesterday doesn't slow anyone's live
stream. The log keeps 72 h (`--retention`).

Details: [Subscribe to the firehose](subscribing.md), [Stream seqs](seq.md).

## How big it gets

```facts
- { value: "~94k", unit: events/s, label: one 8-core node, note: "measured on MinIO, p99 time to firehose under 0.7 s, 0 rejects", tone: amber }
- { value: "~98k", unit: events/s, label: three nodes of 3 cores + SMT, note: "measured on one box; 100k/s on 3 × 8 cores fits with headroom", tone: violet }
- { value: "~1.6", unit: Gb/s, label: per full-firehose consumer, note: "at 33k events/s; a NIC runs out long before the CPU", tone: blue }
- { value: "~$1.0k", unit: /mo, label: one node at today's load, note: "modeled, OVH + R2, 100 consumers; 3 cores ~$3.4k", tone: rust }
```

| | Bluesky today | The design target |
|---|---|---|
| Events/s | ~350 average, ~480 in the busiest hour | 100k |
| Nodes | 1, or 3 for high availability | 3 × 8 cores, 32 GB |
| Busy cores, cluster-wide | well under 1 | ~50–60% of the cluster |
| Firehose per consumer | ~15 Mb/s | ~4.3 Gb/s |

CPU is cheap at relay rates. A node costs ~65–70 µs of CPU per event, and a third of that is the
signature check. What sizes a relay is bandwidth: every full-firehose consumer pulls the whole
stream, so egress is the bill wherever egress is metered. On unmetered hosts the bucket's
requests are the biggest line, and they follow the number of logs and host shards, not the
traffic. Details: [Performance](perf.md), [Cost](cost.md).

## Policy

Host tiers, per-host and per-account limits, domain rules, `requestCrawl` admission,
cluster-wide budgets and the spam counters that open cases are one versioned document in the
bucket. Every node reloads it within 10 s, and every change goes in an audit log. The defaults
follow indigo's relay wherever it has a number. The operator dashboard at `/admin` edits it,
throttles or bans hosts and takes down accounts. Details: [Policy](policy.md),
[Admin API](admin-api.md).

## One node or many

| | One node | A cluster |
|---|---|---|
| Run with | no `--cluster` or `--role` | `--role core` on each node, plus peer mTLS |
| DID shards | 4 | 24 (`--did-shards`, read once) |
| When a node dies | its supervisor restarts it, and it resumes from the bucket | peers take its shards in ~1–2 s (~12 s if it hangs) |
| More egress | | edges (mTLS) and replicas (read-only bucket access) |

Going from one node to several doesn't need a migration. Start a core with the same bucket and
`--prefix`, and it joins, takes its share of shards and merges every log. See
[Deploy](operations/deploy.md).

## What isn't built

- Archival mode (a full mirror of every repo, for `getRepo`) works, but it's off by default and
  has gaps. See [Archival mode](archival.md).
- There are no alert rules, Grafana dashboards or Ansible kit for vlRelay yet, and no published
  image. [Operations](operations/index.md) says what there is.
- Compressed or filtered outputs (a zstd-framed stream, Jetstream-style collection filters) and
  public segments for backfill are designed but not built.
- vlRelay is new and hasn't run in production. It has unit and differential tests, an e2e suite
  on a local network (one node, a cluster under kill -9 and SIGTERM, policy faults, resharding,
  archival) and a compatibility harness against indigo's tools ([Compatibility](compat.md)).

## Where to go next

- To read the firehose, start at [Subscribe to the firehose](subscribing.md).
- To run a relay, start at [Operations](operations/index.md), then [Deploy](operations/deploy.md).
- For why it's shaped this way, read [Design](design.md).
