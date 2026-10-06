---
title: Archival mode
section: vlRelay
order: 7
status: draft
summary: "An archiving relay keeps a full mirror of the repos it chooses and serves getRepo, getRecord and getBlocks from it. It's a policy setting, per account, and off by default."
---

```hero
diagram:
  caption: The DID owner keeps the mirror current from the commits it already checks. A repo it has never seen, or whose chain broke, is fetched from its PDS once, politely, while its live commits keep flowing to the firehose.
  nodes:
    - { id: commit, label: "`#commit`", sub: checked by the DID owner, at: [0, 0], size: [9, 3], tone: accent }
    - { id: apply, label: Apply to the mirror, sub: root must equal `data`, at: [13, 0], size: [10, 3], tone: accent }
    - { id: mirror, label: Mirror, sub: "SlateDB · vlpds's layout", at: [27, 3.5], size: [10, 3], shape: store, tone: amber }
    - { id: pds, label: PDS, sub: getLatestCommit · getRepo, at: [0, 7], size: [9, 3], tone: muted }
    - { id: fetch, label: Fetch queue, sub: per-host rate · 2 in flight, at: [13, 7], size: [10, 3], tone: violet }
    - { id: reads, label: getRepo · getRecord · getBlocks, sub: DID owner only, at: [41, 3.5], size: [11, 3], tone: blue }
  edges:
    - "commit -> apply"
    - "apply.r -> mirror.l30: with the event's state"
    - { from: fetch.l, to: pds.r, label: bootstrap }
    - "fetch.r -> mirror.l70: import"
    - { from: apply.b, to: fetch.t, label: mismatch, dash: true }
    - "mirror -> reads"
facts:
  - { value: "off", label: by default, note: "`archive.mode`: off, all, tiers or hosts", tone: muted }
  - { value: "~65–130", unit: µs, label: of CPU per archived commit, note: "2–4% of a core at Bluesky's ~330 commits/s", tone: amber }
  - { value: "~1,300", unit: repos/s, label: bootstrapped from CARs, note: "measured on a laptop, 8 at a time", tone: blue }
  - { value: "72 h", label: before a taken-down mirror is deleted, note: "`archive.takedownRetentionHours`; it stops serving at once", tone: rust }
```

An archiving relay keeps a full copy of every repo it mirrors and serves `getRepo`, `getRecord`
and `getBlocks` from it, the way the original relays did. That's what a new AppView needs to
backfill the network from one place instead of from thousands of PDSes. It's the same binary.
Archiving is a policy setting, per account, and it's off by default.

> [!NOTE]
> Archival mode works end to end and passes its e2e, but it has known gaps (below) and hasn't
> been measured at network scale.

## Turning it on

It all lives in the policy object ([Policy](policy.md)), so a PUT of `policy/full` switches it
cluster-wide within 10 s.

| Field | Default | What |
|---|---|---|
| `archive.mode` | `off` | `off`, `all`, `tiers` (accounts on hosts of `archive.tiers`) or `hosts` (accounts on `archive.hosts`) |
| `archive.takedownRetentionHours` | 72 | How long a taken-down account's mirror is kept before it's deleted |
| `tiers.<tier>.archivalFetchesPerHost` | trusted 5, default and new 1, throttled 0.1 | Bootstrap fetches per second from one host |
| `cluster.archivalFetchConcurrency` | 32 | Fetches in flight across the cluster, split over live nodes (at least 1 each) |
| `cluster.archivalFetchBytesPerSec` | 50 MiB/s | Fetched bytes per second across the cluster, split the same way |

An account is mirrored when the policy wants its host. The host is the one its events come from,
which the DID owner has already checked against the DID document. Archiving by tier (trusted and
default hosts, say, but not new ones) keeps a spam host from filling the bucket.

## What's stored

The mirror sits in the DID shard's SlateDB, next to the sync state, in vlpds's own repo layout:

| Key | What |
|---|---|
| `R/{did}\0{gen}{path}` | The record: CID, the rev that wrote it, its bytes. The source of truth. |
| `c/{did}\0{gen}{cid8}{path}` | The record CID index, for `getBlocks` |
| `M/{did}\0{gen}{cid}` | Interior MST nodes (height 1 and up). Leaves are rebuilt from records. |
| `h/{did}` | The head: commit CID, data CID, rev, the signed commit block |
| `V/{did}` | The relay's own: the live generation, one being staged, ones left to delete, and when a takedown was first seen |

Blob refs and backlinks aren't kept, since the relay serves neither. Every row but the head and
`V/` is under a generation. A bootstrap writes a whole repo under a new generation, where no reader
looks, then switches `V/` and `h/` to it in one batch. The old generation's rows are deleted in the
background.

## Keeping the mirror current

Sync 1.1 commits carry the records and MST nodes for every path they change, so the DID owner
already has what it needs to apply a commit. No fetch from the PDS is needed.

```steps
- title: Apply the ops to the stored tree
  body: After the chain checks pass, the DID owner opens the repo's stored tree at its head (loading only the paths the ops touch) and applies the commit with vlpds's own replay code.
- title: Check the new root
  body: The new root must equal the commit's `data`. That checks the commit against the relay's copy of the whole tree, not just the partial tree in the CAR.
- title: Write with the event's state
  body: The rows ride the event's state ticket, so they're written in the same batch as the sync record, once the log segment holding the event is durable. A crash can't persist mirror rows for an event the log might still lose.
- title: Keep the tree warm
  body: Until then the tree stays in memory, so the account's next commit builds on it. Idle trees stay in an LRU of 4,096 per shard.
```

A `#sync` that restates the same tree writes a new head. Anything else queues a fresh copy. When
the stored tree disagrees with a commit that passed the sync 1.1 checks, one side is wrong and the
relay can't tell which without the PDS. It keeps emitting the event (the proof passed) and queues
a re-fetch, and the fetched repo replaces the mirror. These count as `mismatches` on
`GET /admin/api/archive`.

A shard's new owner replays log entries into the mirror the same way it replays the sync state.
A frame at or behind the mirror's rev is skipped, and one whose `prevData` isn't the mirror's head
queues a re-fetch.

## Bootstrap

A repo needs a full copy when its account is new to an archiving relay, when archiving is switched
on for it, or when its chain breaks (a `prevData` mismatch, a commit from a desynchronized account,
a `#sync` the mirror can't link, or a stored-tree mismatch).

The DID owner queues a fetch. It resolves the account's PDS and key from the DID document, reads
`getLatestCommit`, then `getRepo`, and checks the CAR with vlpds's import (every block hashed, the
whole canonical tree rebuilt and matched to the commit's `data`), then the commit's DID, version
and signature. Its rev must be at or past `getLatestCommit`'s.

Live commits for the account keep being checked, sequenced and emitted while it's queued. Their
frames wait in the queue entry (up to 1,024 frames or 16 MiB) and are applied once the import
lands. A frame that doesn't chain from the fetched head, or a buffer that overflowed, queues
another fetch. A desynchronized account also takes the fetched head as its chain and loses its
desync mark, since the head is signed by the account's key and is at least what the PDS's
`getLatestCommit` says. So an archiving relay heals a broken chain without waiting for a `#sync`.

The DID document is anyone's to write, so fetches use vlpds's guarded client: https only, no
redirects followed, and only hostnames that resolve to public addresses (`--dev-mode` allows plain
http to local PDSes). Each host gets a token bucket at its tier's `archivalFetchesPerHost` and at
most 2 fetches in flight, and hosts take turns. A read idle for 30 s fails the fetch, and so does
a `getRepo` under 32 KiB/s after its first 30 s, so a PDS that trickles can't hold the node's
fetch slots. Failures retry 5 times with backoff (4 s, 8 s, … 64 s).

## Deletes and takedowns

A sweeper walks the mirrors every 10 s per open shard:

- A mirror the policy no longer wants (archiving off for its host, or the account deleted
  upstream) is deleted in batches.
- A taken-down account (by an operator, or by its PDS's `#account` status) stops serving at once,
  because the read endpoints check the sync record first. The mirror is deleted
  `takedownRetentionHours` after the sweeper first saw the takedown, and an untakedown before then
  keeps it.
- A staged generation whose fetch is gone (a crash mid-import) is deleted.

After a policy change, and at startup, it also queues every active account that should be mirrored
and isn't.

## Endpoints

| Endpoint | Served |
|---|---|
| `com.atproto.sync.getRepo` | Streamed from one snapshot in vlpds's streamable CAR order. `since` sends only the records written after that rev, with the whole current tree. 32 at once. |
| `com.atproto.sync.getRecord` | The commit, the proof path and the record. Shares 64 slots with `getBlocks` (a request waits up to 2 s for one, then `Overloaded`). |
| `com.atproto.sync.getBlocks` | The commit, `M/` nodes and records by CID, with leaves found in one walk of the tree. At most 1,000 CIDs and 2 walks at once. |
| `com.atproto.sync.listBlobs` | 501 `MethodNotImplemented`. Blobs stay on the PDS. |

Errors follow the PDS: `RepoTakendown`, `RepoSuspended`, `RepoDeactivated`, and `RepoNotFound` for
a deleted account, an unknown one, or one that isn't mirrored. Desynchronized and throttled
accounts are served. Only the DID owner answers. A core that doesn't hold the shard forwards the
request to the owner over the peer listener and streams the answer back. Edges and replicas don't
serve archival reads.

The operator routes, behind the admin token, are `GET /admin/api/archive` (counters, queue,
per-shard trees and SST bytes, recent errors) and `POST /admin/api/archive/fetch?did=` (queue a
fetch).

## Numbers

Measured on a laptop with other builds running, so treat these as upper bounds.

| What | Result |
|---|---|
| Bootstrap (check the CAR, stage, switch), 8 at a time | 1,293 repos/s, 97 MB/s of CAR, 388k records/s, 1.1 ms CPU per repo |
| Apply, archival off | 4.0 µs CPU per commit |
| Apply, archival on, tree kept from the account's last commit | 69 µs CPU, 276 µs wall |
| Apply, archival on, tree reopened from the DB | 133 µs CPU, 577 µs wall |
| `getRepo`, one at a time | 1,096 repos/s, 88 MB/s, 0.48 ms CPU per repo |

So archival adds ~65–130 µs of CPU per commit, 2–4% of one core per node at Bluesky's ~330
commits/s. The bench's synthetic records compress far better than real posts, so for bucket
bytes vlpds's measurement of the same layout is the better guide (~154 B a record plus ~323 B a
repo, [Cost](cost.md#archival-mirror-optional)).

Over HTTP against a 4-host synthetic fleet (1,000 accounts per host, 100 records each, 1,000
events/s), the relay bootstrapped 4,003 repos (401k records, 227 MB) with 0 failures, ~570 repos/s
in the first 5 s, while it applied 62,915 live commits with 0 mismatches.

`just e2e-archival` on the dev network (two vlpds and the reference PDS, 60 accounts, 100
writes/s) switches archival on at 30 s and forces one account's chain to break:

| Check | Result |
|---|---|
| Archive switched off → all at 30 s | 60 bootstraps within 4 s of the switch |
| Forced desync of one account | Re-fetched and healed. Its one commit with the broken `prevData` is the only event missing from the firehose. |
| `getRepo` vs the vlpds upstreams | 40 of 40 byte-identical |
| `getRepo` vs the reference PDS | 20 of 20 with the same root and block set |
| Mirror | 6,906 commits applied, 0 mismatches, `getRepo` in 1.1 ms each |

The reference PDS writes its CAR's blocks in its own order, which a mirror that rebuilds leaves
can't reproduce, so byte identity with it isn't possible. Its root and blocks match.

## Gaps

- A bootstrap isn't durable until the shard's next checkpoint (5 s). A crash before then loses it,
  and the account's next commit queues it again.
- A repo over 256 MiB isn't mirrored, since the import holds the CAR in memory. The fetch fails
  and gives up after 5 tries.
- `getRepo` with `since` sends the whole current tree with only the newer records, like vlpds.
  The reference PDS sends only the blocks written since.
- The fetch queue is in memory. A restart or a shard move drops it, and the sweeper's rescan
  queues everything again, in a new order.
- Healing a desynchronized account from a fetched repo doesn't emit a `#sync`, so consumers see
  the account's next commit chain from a head they never saw.
- The sweeper walks every mirror of a shard every 10 s. That's fine at dev scale, and at 56M
  accounts it wants a cursor and pacing.
- Takedown retention counts from when the sweeper first saw the takedown, not from the takedown
  itself.
- At the 100k events/s target with everything archived, the mirror alone would take ~7–13 cores of
  CPU across the cluster.
- The archive's counters are on `GET /admin/api/archive`, not in Prometheus yet.
