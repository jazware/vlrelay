---
title: Overview
section: vlRelay
order: 1
summary: An atproto relay on a replicated log. It subscribes to PDSes, checks every event against sync 1.1, and serves one firehose with the same seqs from every node, with the bucket as its cheap long-term copy.
---

```hero
diagram:
  caption: Each node reads the PDSes the leader gives it, verifies their events and submits them to the leader. The leader checks each one against the account's record, gives it the next seq and replicates it, and every node emits it once two of the three hold it. Every 30 s the leader flushes the log and the records to the bucket. Three nodes stand for one to five.
  nodes:
    - { id: pds, label: PDSes, sub: subscribeRepos, at: [0, 5], size: [8, 3], tone: muted, stack: true }
    - { id: n1, label: node n1, sub: "its PDSes · verify", at: [12, 0], size: [9, 3], tone: accent }
    - { id: n2, label: leader n2, sub: "check chain · seq · append", at: [12, 5], size: [9, 3], tone: violet }
    - { id: n3, label: node n3, sub: "its PDSes · verify", at: [12, 10], size: [9, 3], tone: accent }
    - { id: pol, label: "`policy/`", sub: "limits · rules · cases", at: [27, 0], size: [10, 2.6], shape: store, tone: amber }
    - { id: state, label: "`qlog/state`", sub: "records · hosts · cursors", at: [27, 3.4], size: [10, 2.6], shape: store, tone: amber }
    - { id: log, label: "`log/qlog/`", sub: "segments · 72 h", at: [27, 6.8], size: [10, 2.6], shape: store, tone: amber }
    - { id: cons, label: Consumers, sub: any node, at: [0, 14.5], size: [8, 3], tone: blue }
  groups:
    - { label: one quorum log · commit at 2 of 3, around: [n1, n2, n3], tone: accent }
    - { label: the bucket, around: [pol, state, log], tone: amber }
  edges:
    - "pds -> n1: ws"
    - "pds -> n3: ws"
    - "n1 -> n2: submit"
    - "n3 -> n2: submit"
    - { from: n1.r, to: pol.l, label: reload, dash: true }
    - { from: n2.r, to: state.l, label: flush }
    - { from: n2.r70, to: log.l, label: segments }
    - { from: n2.b, to: cons.r, label: emit at commit, tone: blue, via: [[16.5, 16]] }
facts:
  - { value: "2 of 3", label: commit, note: "nothing reaches a consumer before a quorum holds it", tone: accent }
  - { value: "1", unit: stream, label: with the same seqs on every node, note: "a consumer resumes on any node with its cursor", tone: blue }
  - { value: "50–120", unit: ms, label: emission pause when the leader dies, note: "kill -9; ~1 s when it hangs or is cut off", tone: violet }
  - { value: "30 s", label: bucket flush, note: "`--qlog-flush-ms`; ~0.5 writes and ~1.7 reads a second today", tone: amber }
```

vlRelay is an [atproto](https://atproto.com) relay written in Rust. It connects to PDSes, reads
each one's `subscribeRepos` stream, checks every event against the sync 1.1 rules and re-emits
them as one combined firehose. Consumers see a normal relay: indigo's Go consumers, `goat`,
`@atproto/sync` and Jetstream read it unchanged.

Its nodes share one replicated log. One node leads, gives every event its seq and emits it once a
majority holds it, so every node serves the same stream and a takeover keeps everything a consumer
saw. The bucket (S3, R2, GCS or MinIO) gets the log and the accounts' records every 30 seconds,
which makes it a cheap long-term copy (a few dollars a month in requests at today's load). A
relay's input is small, since Bluesky's whole network averages ~350 events a second. So the hard
parts are fan-out, streams that survive restarts, cursors that work on any node, and thousands of
upstreams with their own failure modes. [Design](design.md) has the reasoning.

## The path of an event

```diagram
caption: One event, from a PDS's socket to every consumer. The node that reads the PDS does the stateless checks before anything crosses the network, and the leader does the stateful ones. Nothing reaches the firehose before two of the three nodes hold it.
nodes:
  - { id: pds, label: PDS, sub: "`#commit` frame", at: [0, 0], size: [7, 3], tone: muted }
  - { id: host, label: Reading node, sub: parse · signature · MST proof, at: [11, 0], size: [11, 3], tone: accent }
  - { id: lead, label: Leader, sub: host · rev · prevData · seq, at: [26, 0], size: [10, 3], tone: violet }
  - { id: fol, label: Followers, sub: commitlog, at: [40, 0], size: [9, 3], tone: accent, stack: true }
  - { id: commit, label: Committed, sub: 2 of 3 hold it, at: [26, 6.5], size: [10, 3], tone: solid }
  - { id: subs, label: Consumers, sub: every node emits, at: [40, 6.5], size: [9, 3], tone: blue }
  - { id: ack, label: Host cursor, sub: moves past it, at: [11, 6.5], size: [11, 3] }
edges:
  - "pds -> host: websocket"
  - "host -> lead: submit"
  - "lead <-> fol: replicate, ack"
  - { from: lead.b, to: commit.t, label: quorum }
  - "commit -> subs: subscribeRepos"
  - "commit -> ack: outcome"
```

- A node reads the frame off the PDS's websocket. It checks the CBOR, the size limits, the
  commit's signature against the account's signing key and the inductive proof (the ops applied
  to the partial tree in the CAR give the commit's `data`). A signature check costs ~33 µs of CPU,
  so one core checks ~30k events a second. Verifying is most of the CPU, so every node does it for
  its own PDSes.
- It submits the event to the leader in a batch. All of an account's events go through one batch
  stream, so they reach the leader in the order the PDS sent them.
- The leader runs the checks that need state. The event must come from the host the account's
  DID document names, the account must be active, its `rev` must move forward, and its `prevData`
  must match the record's data CID. Then the leader appends it under the next seq, with the
  account's new record in the same entry, and replicates it.
- Once two of the three nodes hold the entry on disk, it's committed. Every node emits it, and
  the reading node moves its host's cursor past it. The cursors ride in the log too, so a node that
  takes over a PDS resumes it from there and the PDS replays the rest.

A failed check drops the event and counts against its host. A broken chain marks the account
desynchronized until a `#sync` resets it, as sync 1.1 expects. A duplicate (the PDS sent it again
after a reconnect) is answered without an entry. Details: [Cluster](cluster.md#the-path-of-an-event).

## Hosts and the leader

The leader keeps the host table: every PDS, its tier and the member that reads it. A host's reader
is the live member with the highest rendezvous hash for it. So when a node goes quiet for 2 s
(`--qlog-host-failover-ms`), only its own hosts move, and they resume from their cursors. `--host`
and `requestCrawl` work on any node, since a host admitted anywhere goes into the leader's table.

The leader also holds every account's record (rev, data CID, host, status, signing key). The
records live in one SlateDB that only committed entries write, and each flush seals it at exactly
the flushed seq. So a new leader opens the records at the last flush and replays its own log from
there, which takes tens of milliseconds. Details: [Cluster](cluster.md).

## One stream on every node

Consumers see seqs 1, 2, 3, … like indigo's relay. The leader gives each event its seq when it
appends it, and every node emits the same committed log, so every node gives the same event the
same seq. A consumer can put the nodes behind a load balancer and resume on any of them.

A node serves recent cursors from its in-memory ring (512 MiB, `--ring-mb`) and its local log, and
older ones from the 64 MiB segments in the bucket. So a consumer catching up from yesterday doesn't
slow anyone's live stream. The bucket keeps 72 h of log (`--qlog-retain-hours`).

If two nodes lose their disks at once, the log resumes from the bucket's last flush with a jump in
the seqs, and the PDSes send the rest again. Details: [Subscribe to the firehose](subscribing.md).

## How big it gets

```facts
- { value: "~350", unit: events/s, label: Bluesky's average today, note: "the leader needs ~0.1 vCPU for it (modeled)", tone: amber }
- { value: "200k", unit: events/s, label: the quorum log's ceiling, note: "measured, 3 nodes, commitlogs on tmpfs; the leader used 1.8 cores", tone: violet }
- { value: "~1.6", unit: Gb/s, label: per full-firehose consumer, note: "at 33k events/s; a NIC runs out long before the CPU", tone: blue }
- { value: "~$19", unit: /mo, label: three nodes at today's load, note: "modeled, 3 OVH VPS-1 + R2, 30 s flush", tone: rust }
```

CPU is cheap at relay rates. Verifying an event is the biggest cost, and every node does its own
share. The log is cheap too. Replication costs the leader ~6 µs an event, and three nodes on tmpfs
commit 200k events a second. On a real disk, fsynced bandwidth sets the ceiling. Three nodes
sharing one consumer NVMe topped out at ~25k events a second, so 100× today's load wants
datacenter drives and 10× is comfortable anywhere. Every full-firehose consumer pulls the whole
stream, so egress is the bill wherever egress is metered. On small VPSes the hosts are the bill,
since the bucket costs a few dollars a month. Details: [Performance](perf.md), [Cost](cost.md).

The whole relay on the quorum log hasn't been pushed to its ceiling yet. The chaos runs drive it
at today's rate through kill -9, partitions, pauses, power cuts, wiped disks and membership
changes, and check every node's stream.

## Policy

Host tiers, per-host and per-account limits, domain rules, `requestCrawl` admission,
cluster-wide budgets and the spam counters that open cases are one versioned document in the
bucket. Every node reloads it within 10 s, and every change goes in an audit log. The defaults
follow indigo's relay wherever it has a number. The operator dashboard at `/admin` edits it,
throttles or bans hosts and takes down accounts. A takedown is an entry in the log, so every node
emits it in order. Details: [Policy](policy.md), [Admin API](admin-api.md).

## One node or three

| | One node | Three nodes |
|---|---|---|
| Run with | no `--qlog-peer` | `--qlog-peer` for each other node, and `--qlog-dir` on NVMe |
| Commit | its commitlog's fsync (the write-ahead log) | two of three on disk |
| When a node dies | its supervisor restarts it, and it replays its commitlog | another node leads in 50–120 ms (~1 s if it hangs) |
| When a disk dies | it resumes from the bucket's last flush, with a seq jump | nothing, unless two go at once |

One node is the same quorum log with one member, and it flushes to the bucket on the same
schedule. So going to three is a membership change (`qlog member …` or the dashboard's Quorum
page) with no migration. See [Cluster](cluster.md#changing-the-members) and
[Deploy](operations/deploy.md).

## What isn't built

- With `--plc-export`, a cold relay seeds its DID documents from the PLC directory's export, which
  at 2 requests a second takes about 11 hours for today's ~80M ops. Without it, each account is
  resolved once at the PLC budget.
- The sync API's repo endpoints answer on the leader, and a follower names it.
- There are no alert rules, Grafana dashboards or Ansible kit for vlRelay yet.
  [Operations](operations/index.md) says what there is.
- Compressed or filtered outputs (a zstd-framed stream, Jetstream-style collection filters) and
  public segments for backfill are designed but not built.
- vlRelay is new and hasn't run in production. It has unit and differential tests, chaos runs on a
  local network (one node and three, under kill -9, partitions, wiped disks and membership
  changes) and a compatibility harness against indigo's tools ([Compatibility](compat.md)).

## Where to go next

- To read the firehose, start at [Subscribe to the firehose](subscribing.md).
- To run a relay, start at [Operations](operations/index.md), then [Deploy](operations/deploy.md).
- For why it's shaped this way, read [Design](design.md).
