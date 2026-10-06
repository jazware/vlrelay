---
title: Design
section: vlRelay
order: 3
summary: "Why vlRelay is shaped the way it is: what a relay has to do, where that's hard, what it takes from vlpds, and the decisions behind one replicated log with one leader and a bucket written in bulk."
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
  - { value: "100×", label: what the log is sized for, note: "~35k events/s; past that the stream splits into several logs", tone: violet }
```

A relay connects to every PDS it knows about, reads each one's `subscribeRepos` stream, checks the
events and re-emits them as one firehose with its own sequence numbers. Under sync 1.1 a relay is
non-archival. It doesn't keep copies of repos, only enough per-account state to check that each
commit follows the last one, plus a window of recent events so consumers can resume from a
cursor. It also answers a handful of sync endpoints (`listRepos`, `getRepoStatus`,
`getLatestCommit`, `listHosts`, `getHostStatus` and `requestCrawl`).

vlRelay is one of those. It keeps each account's sync state and 72 h of the stream, and it
doesn't serve `getRepo`.

## Where it's hard

The input is small. vlpds does ~60k commits/s on one 16-core node, and the whole network is about
350 events/s. Even at 10× Bluesky's load, one machine can check and sequence everything. So the
write path isn't the problem this time. These are:

| Problem | Why it hurts on a single-box relay | What vlRelay does |
|---|---|---|
| Fan-out | Every consumer gets the whole stream. A few hundred consumers is several Gb/s of egress, and that's the real bill. | Pre-framed batches shared by every subscriber, so serving is mostly copying bytes into sockets. Every member serves the stream. |
| Restarts break consumers | A restart or crash drops every websocket, and a disk loss can lose the backfill window. | A replicated log. Another node leads 50–120 ms after the leader dies, with nothing committed lost, and the bucket holds a copy of the log. |
| Cursor stability | A cursor only means something on the box that issued it, so you can't load-balance consumers. | One leader gives every event its seq, and every node emits the same committed log. |
| Backfill | Catching up from an old cursor competes with live traffic for the same disk and NIC. | Recent cursors read memory and the local log, and older ones read 64 MiB segments in the bucket. |
| Thousands of upstreams | Each PDS is a websocket with its own failure modes: slow, flapping, replaying or abusive. | The leader's host table spreads them over the members, and the policy engine keeps them fair. |

## What it takes from vlpds

| | Piece | How the relay uses it |
|---|---|---|
| take | Log segments | The flush uploads the log as vlpds segments (64 MiB, zstd), which old cursors read back. |
| take | Firehose serving | The subscriber code (pre-framed batches, slow consumers, backfill from the bucket) is most of a relay's egress. It serves one counted log. |
| take | SlateDB for state | The accounts' records (rev, data CID, host, status, signing key) in one SlateDB, sealed at each flush. |
| take | Conditional writes for fencing | `qlog/leader` is written by compare-and-swap, so each leader term has its own epoch and an old leader can't commit. |
| adapt | Sync 1.1 checking | vlpds builds and signs commits. A relay checks other people's: the signature, and the inductive proof from the ops and `prevData`. |
| new | The quorum log | Replication, commit at a majority, takeover and the flush. It's the "quorum in-memory durability" idea from vlpds's TODO, built here. |
| leave | Node leases and the log merger | Liveness is peer heartbeats, and one log needs no merge. |
| leave | Repo workers, OAuth, accounts, blobs, proxying | A relay never creates commits, and none of the rest applies. |

## One log, one leader

```diagram
caption: A DID that moves to another PDS arrives through a different node, but every event goes to the one leader, which checks that the new host may speak for the account. Two nodes stand for any number.
nodes:
  - { id: h1, label: pds-a.example, sub: old PDS, at: [0, 0], size: [9, 3], tone: muted }
  - { id: h2, label: pds-b.example, sub: new PDS, at: [0, 5], size: [9, 3], tone: muted }
  - { id: n1, label: node n1, sub: reads pds-a, at: [13, 0], size: [9, 3], tone: accent }
  - { id: n2, label: node n2, sub: reads pds-b, at: [13, 5], size: [9, 3], tone: accent }
  - { id: lead, label: leader, sub: "checks did:plc:abc", at: [27, 2.5], size: [10, 3], tone: violet }
  - { id: st, label: "account record", sub: "host = pds-b.example", at: [41, 2.5], size: [9, 3], shape: store, tone: amber }
edges:
  - "h1 -> n1: old PDS"
  - "h2 -> n2: new PDS"
  - "n1.r -> lead.l30: submit"
  - "n2.r -> lead.l70: submit"
  - "lead <-> st: host check"
```

Every node reads its share of the PDSes, and one leader keeps every account's record and gives
every event its seq. Verifying is most of the CPU, so it stays spread over the members. Checking
the chain and appending is cheap, so it happens in one place.

With three nodes and every entry on all three, every node holds the whole log anyway. Per-account
leaders would only spread the leader's share of the work. One log means the seq is assigned in one
place, so there's no merge of several logs and no slowest log setting the pace. Past ~100× today's
load the stream would split into several logs with a leader each and a merge, and nothing is built
for that.

Account migration races come out of this for free. For a while, both the old and the new PDS may
send events for a DID. The leader accepts only the host the account's fresh DID document names,
and it takes a host's events only from the member its host table names, so two sockets on one PDS
can't mix up an account's order. Details: [Cluster](cluster.md).

## Where each check runs

| Where | Check |
|---|---|
| The node reading the PDS | Frame and CBOR well formed and under the size limits. The commit's signature verifies against the DID's signing key. The ops applied to the partial MST in the CAR give the commit's `data`. |
| The leader | The DID's current PDS (from its DID document) is the host the event came from. `rev` moves forward. `prevData` matches the stored data CID. The account isn't taken down or deactivated, and it's under its rate. |

The stateless checks run before anything crosses the network, and they're most of the CPU (the
signature alone is ~33 µs). Each node keeps a cache of DID documents. A signature that fails
against a cached key refreshes the document and tries once more. A failed stateful check drops the
event and marks the account desynchronized until a `#sync` resets it, which is how sync 1.1
expects relays to recover.

DID documents come from PLC (and `did:web`) through a cache with a cluster-wide budget
(`cluster.plcLookupsPerSec`, 500 a second by default). A cold relay resolves each of ~56M accounts
once at that budget, about 31 hours. Seeding the cache from the PLC directory's export would cut
that, and it isn't built yet.

## Staying available

- The leader appends each event and replicates it. Once two of the three nodes hold it on disk,
  it's committed, and only then does any node emit it or count it against its PDS's cursor. So no
  consumer ever sees an event that a takeover then loses.
- When the leader dies, the others elect a new one in 50–120 ms (kill -9) or after 1 s of silence
  (`--qlog-election-ms`) when it hangs. The new leader writes a new epoch to `qlog/leader`, opens
  the records at the last flush and replays its own log before it takes events.
- When a follower dies, consumers on other nodes don't notice. Its PDSes move to the others after
  2 s and resume from their cursors, which ride in the log.
- If two nodes lose their disks, the log resumes from the bucket's last flush. Seqs jump to the
  flush's reservation (R = F + headroom), so no seq is ever reused, and the PDSes send the rest
  again.
- If the bucket stops taking flushes, the leader stops committing at R. That bounds the unflushed
  tail. The headroom is 8.64M seqs by default (`--qlog-headroom`).

## The bucket

| Path | What | Written |
|---|---|---|
| `qlog/leader` | the epoch, the leader and the members | at a takeover or a membership change |
| `qlog/manifest` | F (the last seq flushed), R, the segments, the records' checkpoint, the cursors | every flush, last |
| `qlog/state*` | one SlateDB: each account's record, the host table, the cursors, at exactly F | every flush |
| `log/qlog/` | 64 MiB segments of the log, for old cursors | every flush |
| `policy/`, `cases/` | the policy document, domain rules, audit logs and cases | when an operator or the driver changes them |

The bucket is written in bulk every 30 s (`--qlog-flush-ms`), with the manifest written last as
the commit point. At today's rate that's about 0.5 writes and 1.7 reads a second (measured over an
hour), so requests cost ~$1 a month on R2 after its free tier. Liveness never touches the bucket,
since the members heartbeat each other every 100 ms. Details: [Cluster](cluster.md#what-s-in-the-bucket).

## Scale

The quorum log is sized for 100× today's load, about 35k events/s. What sets the ceiling at each
step is measured on the log and modeled for the relay:

- Replication is cheap. At 100× the leader spends ~6 µs an event on it, and three nodes with their
  commitlogs on tmpfs commit 200k events/s with the leader at 1.8 cores.
- Fsynced disk bandwidth is the real ceiling. Three nodes sharing one consumer NVMe topped out at
  ~25k events/s. So 100× wants datacenter drives, and two members must never share a disk. 10× is
  comfortable anywhere.
- Verifying is the biggest CPU cost (~33 µs a signature), and every node does it for its own PDSes.
  At today's load the leader needs ~0.1 vCPU (modeled).
- At today's load a node's limits are RAM (~2.5 GB) and disk (~23 GB, with ~8.5 GB of account
  state), both modeled. At 10× small VPSes run out of disk and port first.
- Each full-firehose consumer pulls the whole stream. At 100× that's ~1.6 Gb/s a consumer, so the
  network sizes the boxes long before the CPU does.

Past 100× the single log stops scaling, and compressed or filtered outputs matter more than any
host ([Cost](cost.md)).

## Decisions

| Question | Decision |
|---|---|
| One log or several | One, with one leader. Several logs and a merge only pay off past ~100×. |
| When consumers see an event | At commit, once two of three hold it. A takeover never takes back an event a consumer saw. |
| Commitlog or memory only | A commitlog on local NVMe (`--qlog-dir`), so two or three process deaths are an ordinary takeover. |
| Bucket flush | 30 s (`--qlog-flush-ms`). A longer flush only widens what PDSes resend after two disks are lost. |
| Liveness | Peer heartbeats. Bucket leases with a 10 s TTL would cost ~$56 a month on R2, more than everything else in the bucket. |
| Segment size | 64 MiB. 8 MiB segments would cost ~$230 a month more at 100×. |
| Account state | One SlateDB, written only from committed entries and sealed at exactly F. More shards multiply its requests. |
| Where signatures are checked | On the node reading the PDS, with the DID document cache. |
| Account migration races | The leader accepts events only from the host the fresh DID document names, read by the member the host table names. |
| Archival | No. vlRelay is a sync 1.1 relay and doesn't serve `getRepo`. |
| Compressed or filtered outputs | Not yet. Collection filtering eventually. |
| Public segments for backfill | Not for now. They'd make backfill cheap, but the segment format would become an API. |
| Relays as upstreams | No. An upstream that says it's a relay is refused and banned ([Compatibility](compat.md#relay-chaining)). |

## Failure modes

| What breaks | What happens |
|---|---|
| The leader dies | Another node leads in 50–120 ms, or ~1 s when the leader hangs or is cut off. Nothing committed is lost, and consumers see a short pause. |
| A follower dies | Consumers on other nodes see nothing. Its PDSes move after 2 s and resume from their cursors, and its own consumers reconnect anywhere. |
| Two nodes' disks are lost | The log resumes from the bucket's last flush with a seq jump, and the PDSes resend what came after it. |
| The bucket is slow or down | Commits go on until the reservation R, then stop until a flush lands. |
| A PDS replays or sends garbage | The leader answers duplicates without an entry. Failed checks count against the host's error budget, which can auto-throttle it. |
| A spam wave from new hosts | New-host quotas cap each one, domain rules catch them as a group, and the cluster-wide new-account budget caps the total. |
| PLC is slow or down | Cached keys keep known accounts flowing. Lookups wait for the budget instead of dropping events, and the backpressure reaches the host's socket. |
| A slow consumer | It falls back to reading the local log and then segments, and gets `ConsumerTooSlow` once it's too far behind. |

The original design-session document is in the repository next to these pages, with the cost
estimates and open questions as they stood before the build.
