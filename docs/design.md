---
title: Design
section: vlRelay
order: 3
summary: "Why vlRelay is shaped the way it is: what a relay has to do, where that's hard, what it takes from vlpds, and the decisions behind host shards, DID shards and one merged stream."
---

```hero
diagram:
  caption: What a relay does. It reads every PDS's stream, checks each event against the account's last known state, and re-emits everything as one stream with its own seqs, plus enough recent history for consumers to resume.
  nodes:
    - { id: pds, label: PDSes, sub: "thousands · one socket each", at: [0, 2], size: [9, 3], tone: muted, stack: true }
    - { id: check, label: Check, sub: "signature · proof · rev · host", at: [13, 2], size: [10, 3], tone: accent }
    - { id: state, label: Per-account state, sub: "rev · data CID · key · ~150 B", at: [13, 8], size: [10, 3], tone: amber, shape: store }
    - { id: log, label: Log, sub: "72 h · cursor replay", at: [27, 8], size: [9, 3], tone: amber, shape: store }
    - { id: fh, label: One firehose, sub: its own seqs, at: [27, 2], size: [9, 3], tone: blue }
    - { id: cons, label: Consumers, sub: "AppViews · feeds · labelers", at: [40, 2], size: [9, 3], tone: blue, stack: true }
  edges:
    - "pds -> check: events"
    - "check <-> state: compare, update"
    - "check -> fh: accepted"
    - "fh <-> log"
    - "fh -> cons: subscribeRepos"
facts:
  - { value: "~350", unit: events/s, label: Bluesky's average today, note: "~480 in the busiest hour; measured over 7 days", tone: amber }
  - { value: "56M", unit: repos, label: to keep state for, note: "~150 bytes each, ~8 GB of keys" }
  - { value: "~390", unit: GB, label: of raw events in 72 h, note: "~12 Mb/s × 72 h, before compression", tone: blue }
  - { value: "100k", unit: events/s, label: the design target, note: "on 3 nodes of 8 cores and 32 GB; ~285× today", tone: violet }
```

A relay connects to every PDS it knows about, reads each one's `subscribeRepos` stream, checks the
events and re-emits them as one firehose with its own sequence numbers. Under sync 1.1 a relay is
non-archival. It doesn't keep copies of repos, only enough per-account state to check that each
commit follows the last one, plus a window of recent events so consumers can resume from a
cursor. It also answers a handful of sync endpoints (`listRepos`, `getRepoStatus`,
`getLatestCommit`, `listHosts`, `getHostStatus` and `requestCrawl`).

The original relays were archival. They kept every repo and could serve `getRepo` for anyone,
which is what a new AppView needs to backfill without hammering every PDS. Sync 1.1 made that
optional because it's expensive. vlRelay does both from the same binary, and the archival half is
mostly vlpds's storage engine ([Archival mode](archival.md)).

## Where it's hard

The input is small. vlpds does ~60k commits/s on one 16-core node, and the whole network is about
350 events/s. Even at 10× Bluesky's load, one machine can check and sequence everything. So the
write path isn't the problem this time. These are:

| Problem | Why it hurts on a single-box relay | What vlRelay does |
|---|---|---|
| Fan-out | Every consumer gets the whole stream. A few hundred consumers is several Gb/s of egress, and that's the real bill. | Pre-framed batches shared by every subscriber, so serving is mostly copying bytes into sockets. Edges and replicas add egress without adding ingest. |
| Restarts break consumers | A restart or crash drops every websocket, and a disk loss can lose the backfill window. | Leases, fencing and replay. A planned handoff takes under a second, and the log is in the bucket. |
| Cursor stability | A cursor only means something on the box that issued it, so you can't load-balance consumers. | A deterministic merge of every node's log, so every node emits the same events with the same seqs. |
| Backfill | Catching up from an old cursor competes with live traffic for the same disk and NIC. | Immutable segments in the bucket, read without touching the live path. |
| Thousands of upstreams | Each PDS is a websocket with its own failure modes: slow, flapping, replaying or abusive. | Host shards spread them, and the policy engine keeps them fair. |

## What it takes from vlpds

| | Piece | How the relay uses it |
|---|---|---|
| take | Log segments with group commit | The relay's output stream is the log. Whatever checked events queued up during the last PUT become the next segment, written with `If-None-Match: *`. |
| take | Leases, fences and fail-stop | Node leases own host shards and DID shards. A takeover fences the dead node's log, replays its tail and reconnects its hosts from their last checkpoint. |
| take | Firehose merger and serving | Every node gets the same ordered stream, and the subscriber code (pre-framed batches, slow consumers, backfill from the bucket) is most of a relay's egress. |
| take | SlateDB for state | Per-account sync state (rev, data CID, host, status, signing key) in one SlateDB per DID shard. |
| take | Record storage, CAR import and export | Archival mode only. Mirrored repos use vlpds's layout as is. |
| adapt | Sharding | DID shards carry over. Host shards are new, because one websocket carries all of a host's accounts. |
| adapt | Sync 1.1 checking | vlpds builds and signs commits. A relay checks other people's: the signature, and the inductive proof from the ops and `prevData`. |
| leave | Repo workers, OAuth, accounts, blobs, proxying | A relay never creates commits, and none of the rest applies. |

## Two kinds of shard

```diagram
caption: Host shards and DID shards are owned independently. A DID that moves to another PDS arrives through a different host owner but still lands at the same DID owner, which decides whether the new host may speak for it.
nodes:
  - { id: h1, label: pds-a.example, sub: host shard 7, at: [0, 0], size: [9, 3], tone: muted }
  - { id: h2, label: pds-b.example, sub: host shard 41, at: [0, 5], size: [9, 3], tone: muted }
  - { id: n1, label: core 1, sub: owns host shard 7, at: [13, 0], size: [9, 3], tone: accent }
  - { id: n2, label: core 2, sub: owns host shard 41, at: [13, 5], size: [9, 3], tone: accent }
  - { id: n3, label: core 3, sub: "owns did:plc:abc's shard", at: [27, 2.5], size: [10, 3], tone: accent }
  - { id: st, label: "account state", sub: "host = pds-b.example", at: [41, 2.5], size: [9, 3], shape: store, tone: amber }
edges:
  - "h1 -> n1: old PDS"
  - "h2 -> n2: new PDS"
  - "n1.r -> n3.l30: forward"
  - "n2.r -> n3.l70: forward"
  - "n3 <-> st: host check"
```

The two jobs have different natural keys. Subscriptions shard by host, so one node holds each
PDS's socket. Sync state shards by DID, so each account's state changes in exactly one place.
The cost is one extra in-region hop for events whose host and DID owners are different nodes,
about two thirds of them on three nodes. That's a millisecond or two next to a segment PUT, and the
forward path batches.

Account migration races come out of this for free. For a while, both the old and the new PDS may
send events for a DID. The DID owner accepts only the host its fresh DID document names, and
re-resolves the document when that changes. Details: [Cluster](cluster.md).

## Where each check runs

| Where | Check |
|---|---|
| Host owner | Frame and CBOR well formed and under the size limits. The commit's signature verifies against the DID's cached signing key. The ops applied to the partial MST in the CAR give the commit's `data`. |
| DID owner | The DID's current PDS (from its DID document) is the host the event came from. `rev` moves forward. `prevData` matches the stored data CID. The account isn't taken down or deactivated. |

The stateless checks run before anything crosses the network, and they're most of the CPU (the
signature alone is ~33 µs). The host owner keeps a read-through cache of signing keys, and the DID
owner tells every peer to drop a key when it sees it change. A failed stateful check drops the
event and marks the account desynchronized until a `#sync` resets it, which is how sync 1.1
expects relays to recover.

DID documents come from PLC (and `did:web`) through a cache with a cluster-wide budget
(`cluster.plcLookupsPerSec`, 500 a second by default). A cold relay would resolve each of ~56M accounts once at that budget,
about 31 hours, so a relay can seed the cache from the PLC directory's export instead
([Policy](policy.md#plc-export-seeding)).

## Staying available

- Each host owner checkpoints, per host, the last upstream seq whose events have all been acked
  durable. It writes that to the bucket every 2 s and on handoff. When a node dies, its host
  shards go to the others, and they reconnect to each PDS from the checkpoint. The PDS replays
  the events in between, and the DID owners drop them as duplicates.
- A DID shard takeover works exactly like vlpds's. The new owner fences the dead node's log,
  replays its tail into the shard's state, then serves. Host owners retry their forwards against
  the new owner.
- Nothing is acked upstream or sent to consumers before it's durable in the bucket, so no
  consumer ever sees an event that a crash then loses.
- If a node isn't sure it may still write (its lease lapsed and a peer fenced its log, or its own
  bucket path is too slow), it steps down and exits. Being unavailable for a while is
  recoverable, but two nodes writing the same account's state isn't.

## Three kinds of node

| Node | Bucket access | Gets live data from | Does |
|---|---|---|---|
| Core | read and write | its own shards, plus its peers' log streams | subscriptions, checks, state, its log, serving |
| Edge | read | the cores' log streams over mTLS | serving only, inside the cluster |
| Replica | read only | the bucket | serving only, anywhere that can read the bucket |

Edges and replicas run the same serving code and emit the same seqs. A replica needs nothing but
read-only credentials, so it can sit next to a big consumer in another region, isolate that
consumer from everyone else, or keep serving everything up to the last durable segment while the
cores are down. It can't become a writer, since it has no write credentials and no lease.

## Scale

The target is 100k events/s on 3 nodes with 8 cores, 32 GB of RAM and NVMe each. That's about
285× Bluesky's average today and ~33k events/s per node. The design estimated CPU, memory and disk
would fit and the network would decide the box. The benches agreed:

- One 8-core node takes ~90–95k events/s cleanly, every event checked, at ~65–70 µs of CPU per
  event. Three scaled-down nodes take ~98k/s, and the extrapolation to 3 × 8 cores leaves 1.6–2×
  headroom ([Performance](perf.md)).
- Memory isn't a concern: 4–6 GB per node at 100–120k events/s.
- At 100k/s each core streams ~3.5 Gb/s each way to its peers, before consumers, and each
  full-firehose consumer is another ~4.3 Gb/s. So the cores want 25 GbE, or consumers belong on
  edges and replicas.

Past 100k/s the full mesh (every core streams its log to every other core) stops scaling, and
compressed or filtered outputs matter more than any host ([Cost](cost.md#what-1000x-would-need)).

## Decisions

| Question | Decision |
|---|---|
| Segment linger | 25 ms (`--linger-ms`). Time to firehose matters more than the PUT bill, which this costs a few hundred dollars a month. |
| Where signatures are checked | On the host owner, with a read-through signing-key cache. |
| Account migration races | The DID owner accepts events only from the host its fresh DID document names. |
| Event size at 100k/s | Plan for today's mix (~4.5–5.3 KB), so the 100k target needs 10 GbE or better. |
| Archive by default | Off. Archival reads go to the DID owner only, and edges and replicas serve streams only. |
| Compressed or filtered outputs | Not yet. Collection filtering eventually. |
| Public segments for backfill | Not for now. They'd make backfill cheap, but the segment format would become an API. |
| Relays as upstreams | No. An upstream that says it's a relay is refused and banned ([Compatibility](compat.md#relay-chaining)). |

## Failure modes

| What breaks | What happens |
|---|---|
| A node dies | Its host shards move and reconnect from checkpoints. Its DID shards move, and their new owners fence its log and replay. Consumers on other nodes see a pause of ~1–2 s, and its own consumers reconnect anywhere. |
| A node can't reach the bucket | Nothing is acked or emitted until it's durable. If its lease lapses and a peer fences it, it exits. |
| A PDS replays or sends garbage | The DID owner drops duplicates by rev and CID. Failed checks count against the host's error budget, which can auto-throttle it. |
| A spam wave from new hosts | New-host quotas cap each one, domain rules catch them as a group, and the cluster-wide new-account budget caps the total. |
| PLC is slow or down | Cached keys keep known accounts flowing. Lookups wait for the budget instead of dropping events, and the backpressure reaches the host's socket. |
| A slow consumer | It falls back to reading segments, and gets `ConsumerTooSlow` once it's too far behind. |

The original design-session document is in the repository next to these pages, with the cost
estimates and open questions as they stood before the build.
