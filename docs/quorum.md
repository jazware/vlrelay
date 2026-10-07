---
title: The quorum log
section: vlRelay
order: 4
summary: "How vlRelay's log works: one leader, epochs fenced by the bucket, a commit at two of three, a local NVMe commitlog, and a bucket flush every 30 s with the PDS cursors in the same manifest. With what it costs and why."
---

```hero
diagram:
  caption: Host owners on every node submit checked events to the leader. The leader appends each one, replicates it to both followers and emits it once two of the three nodes hold it, and the followers emit up to the same commit index. Every 30 s the leader flushes the log, the state and the cursors to the bucket, with the manifest written last.
  nodes:
    - { id: own, label: host owners, sub: "every node · verify", at: [0, 4.5], size: [9, 3], tone: accent }
    - { id: lead, label: leader, sub: "admit · seq · append", at: [13, 4.5], size: [9, 3], tone: violet }
    - { id: bkt, label: "`qlog/manifest`", sub: "F · R · cursors", at: [13, 0], size: [9, 2.6], shape: store, tone: amber }
    - { id: fa, label: follower, sub: "commitlog · ack", at: [27, 0.5], size: [9, 3], tone: accent }
    - { id: fb, label: follower, sub: "commitlog · ack", at: [27, 8.5], size: [9, 3], tone: accent }
    - { id: cons, label: consumers, sub: any node, at: [0, 11], size: [9, 3], tone: blue }
  edges:
    - "own -> lead: submit"
    - "lead -> fa: replicate"
    - "lead -> fb: replicate"
    - { from: lead.t, to: bkt.b, label: flush, tone: amber }
    - { from: lead.b, to: cons.r, label: emit at commit, tone: blue, via: [[17.5, 12.5]] }
facts:
  - { value: "2 of 3", label: commit, note: "nothing is emitted before a quorum holds it", tone: accent }
  - { value: "~75 ms", label: emission pause when the leader is killed, note: "relay chaos at 10x, median · ~1 s when it hangs or is cut off", tone: violet }
  - { value: "~$18", unit: /mo, label: three OVH VPS-1s at today's load, note: "modeled · R2 holding 72 h of log, 30 s flushes", tone: amber }
  - { value: "$0", label: R2 requests a month after the free tier, note: "~$6 before it · checked over an hour on real R2", tone: amber }
```

vlRelay keeps its recent log in its nodes and writes the bucket rarely and in bulk. One node leads.
It gives each event the next seq, replicates it to the other two and emits it once two of the
three hold it. Every 30 seconds it flushes everything up to one seq to the bucket: the log as
segments, the accounts' state and the PDS cursors, with one manifest written last. One node is the
same log with one member, its commitlog serving as the write-ahead log.

A relay suits this better than a PDS does, because the PDSes upstream are the source of truth. If
the relay loses an unflushed tail, it asks each PDS to replay from its last flushed cursor. Losing
a quorum costs a re-ingest and some time, and never a user's data. The firehose holds every event
back until a quorum has it, so consumers only see events that a takeover keeps.

This page is the design, the cost reasoning and the safety arguments. [Cluster](cluster.md) is how
to run one, and [Quorum log measurements](quorum-measurements.md) has every test and benchmark
behind the numbers here.

## Why a quorum

An earlier design (the lease cluster, since deleted) kept the log in the bucket. Three nodes on OVH
with R2 came to ~$3.4k a month, and ~$2.8k of that was bucket requests: segments sealed every 25 ms
on every node, plus host bookkeeping in bucket objects every 2-5 s. A non-archival sync 1.1 relay
on one node costs about $10-15 a month to run, so that was the bar.

Today's load (~350 events/s), 10 full-firehose consumers, R2 after its free tier, from
`scripts/cost_model.py --quorum`:

| setup | flush | $/mo | where it goes |
|---|---|---|---|
| Old design, 3 nodes, OVH ADVANCE-2 + R2 | 25 ms linger | $3,131 | bucket requests $2,532, hosts $594 |
| Old design, one node, OVH ADVANCE-2 + R2 | 25 ms linger | $658 | bucket requests $455, hosts $198 |
| Quorum HA, 3 x OVH VPS-1, commitlog | 30 s | $18 | hosts $14, bucket requests $0, storage $5 |
| Quorum HA, 3 x OVH VPS-1, commitlog | 60 s | $18 | hosts $14, bucket requests $0, storage $5 |
| Quorum HA, 3 x OVH VPS-1, commitlog, 24 h of log in the bucket | 60 s | $15 | hosts $14, bucket requests $0, storage $2 |
| Quorum HA, 3 x Hetzner CAX21 (ARM), commitlog | 30 s | $42 | hosts $37, bucket $5 |
| Quorum HA, 3 x Hetzner AX42, commitlog | 30 s | $332 | hosts $327, bucket $5 |
| Quorum HA, 3 x OVH ADVANCE-2, commitlog | 30 s | $599 | hosts $594, bucket $5 |
| Quorum HA, 3 x AWS c7gd.large + S3, consumers outside AWS | 30 s | $4,538 | egress and cross-AZ $4,326, hosts $199, bucket $14 |
| Single node, OVH VPS-1, NVMe WAL + R2 (72 h of log) | 30 s | $9 | host $5, bucket requests $0, storage $5 |
| Single node, OVH VPS-1, NVMe WAL + R2 (72 h of log) | 60 s | $9 | host $5, bucket requests $0, storage $5 |
| Single node, OVH VPS-2, NVMe WAL + R2 for state and cursors only (not built) | 60 s | $9 | host $8, bucket $0 |
| Single node, OVH VPS-2, NVMe only (not built) |  | $8 | no bucket: losing the disk loses the cursors |
| Benchmark: a non-archival sync 1.1 relay on one node |  | $10-15 | our estimate |

- The bucket stops mattering. Requests go from ~$2.5k a month on 3 nodes to ~$6 before R2's free
  tier and $0 after it at a 30 s flush. Those rates were measured over an hour on real R2
  ([An hour on R2](quorum-measurements.md#an-hour-on-r2)). What's left in the bucket bill is
  storage for 72 h of log (~$5 on R2).
- Hosts are the bill now. At today's load the leader needs ~0.1 vCPU at the sized rate, so the
  smallest NVMe VPS carries a node. Three OVH VPS-1s come to $14 a month, and with the log kept
  24 h in the bucket the cluster lands at ~$15.
- So 3-node HA reaches the top of the $10-15 benchmark only on 4 GB VPSes with a 60 s flush and a
  24 h bucket window. On 8 GB VPSes (the size this page recommends for headroom) it's ~$30. On
  dedicated boxes it's whatever three boxes cost ($332 on AX42s, $599 on ADVANCE-2s).
- A single node gets under the benchmark: ~$9 on an OVH VPS-1 with R2 holding 72 h of log.
- AWS is still decided by egress. Ten consumers outside AWS cost ~$4.1k a month in egress, and
  replication across three AZs adds ~$260.
- At 10x the cheapest HA is ~$121 (three VPS-4s, where disk and the port run out first). At 100x
  it's ~$3.4k, and $2.4k of that is edge boxes for the 10 consumers' 14.8 Gb/s. The bucket is
  ~$500 a month of storage there, still not requests.

## The write path

```steps
- title: A host owner reads and verifies
  body: Each node holds the websockets of the PDSes the leader's host table gives it and verifies their frames. Verify is most of the CPU (~55 µs of the ~90 an event), so it stays spread over every node.
- title: It submits to the leader
  body: Checked events go to the leader in batches over sixteen slots, one batch in flight each, with a DID always on one slot so its events arrive in order. The submit also carries the owner's acked PDS cursors.
- title: The leader admits and appends
  body: The leader runs the relay's state checks (host authority, account status, rate limits, `check_chain`) against the DID's record as of everything appended this term. An accepted event gets the next seq and is appended with its new record in the entry's metadata. A duplicate or a rejection is answered without an entry.
- title: Followers hold it and ack
  body: The leader replicates each entry to both followers. A follower writes it to its commitlog (or holds it in memory, depending on the [durability mode](#durability-modes)) and acks.
- title: Commit at two of three
  body: Once two of the three nodes hold an entry (the leader counts itself once its own copy counts), it's committed. The leader answers the submitter and emits it. Followers learn the commit index on the next append, or at once when it moves, and emit up to it.
- title: Flush every 30 s
  body: The leader flushes everything up to one committed seq to the bucket (`--qlog-flush-ms`, 30 s), with the manifest written last as the commit point.
```

One log with one leader is a choice. With 3 nodes and RF 3, every node is in every replica set
anyway, so per-shard leaders would only spread the leader's work, which is ~2 cores at 100x's sized
rate. One log means the seq is assigned in one place. That drops the k-way merge, the watermarks,
the "slowest of three logs sets the pace" latency the lease cluster had, and its renumbering
checkpoints. Past ~100x the stream would split into P logs with a
leader each and a merge, and nothing here needs that.

The leader decides before it appends. It locks every DID of a batch, in stripe order, from the
decision until the append, and decides different DIDs in parallel within a batch. So a resent copy
of a batch can't slip a DID's later event in ahead of an earlier one.

## Seqs and the emit point

The leader assigns seqs. A seq is a plain counter, given at append time in log order, so it's dense
within a leader's term and the same on every node with no renumbering. Each entry carries
`(epoch, seq)`, and the frame carries the leader's seq from the moment it's appended, so every node
emits identical bytes for a seq. A follower only accepts an entry whose predecessor it already holds
with the same epoch (Raft's log matching rule).

The emit point is the commit. Nothing reaches a consumer, from the leader or a follower, until a
quorum holds it. Each node also emits only what its own log holds durably, up to
`min(commit, local durable)`. A follower can learn a commit index ahead of its own disk, and
without this rule a power cut could bring it back behind what its consumers had seen. With it, a
consumer reconnects to a restarted node and carries on from its cursor with no step back.

### The reservation

Every flush manifest carries a seq ceiling R, and the leader never commits (so no node ever emits)
a seq above the last committed manifest's R. Each flush sets R = max(previous R, F + H), where F is
the flushed seq and H is `--qlog-headroom` (8.64M by default, three 30 s intervals at 100x).
Normally that never binds. If the bucket stops taking flushes, emission stops at R, which is also
the backpressure that keeps the unflushed tail bounded. A bigger H rides out a longer bucket outage
and makes the jump after a lost quorum bigger. Seqs are cheap: 2^53 is ~3,000 years at 100k events/s.

### Leader change with a quorum alive

The new leader takes the longest tail from a quorum ([below](#takeover)) and carries on from its
last seq + 1. Entries in that tail that weren't committed yet are committed under the new epoch with
the seqs they already had. They were never emitted, so nobody has seen those seqs. Entries that
lived only on the dead leader were never committed or emitted either. Their host owners never got an
answer, so they resend them to the new leader after their 1 s timeout, and they get new seqs. So no
seq is emitted twice, no emitted seq is lost, and consumers see no gap.

### Leader change after a lost quorum

When no quorum of intact logs exists, nobody can prove which seqs were emitted after the last flush.
The new leader resumes at R + 1 of the last manifest, which is above anything any node could have
emitted. Consumers see a jump forward and never a rewind:

| Consumer's cursor | What it gets |
|---|---|
| at or below what the recovery kept | backfill from the bucket up to there, then the stream from R + 1 |
| in the skipped range, up to R | the stream from R + 1 |
| above R | `FutureCursor` after the usual 2 s wait |

Events between the last flush and the crash are gone from the relay. The re-ingest brings them back
from the PDSes under new seqs above R, so a consumer that saw them sees them again. A repeated
commit has the same rev, and a sync 1.1 consumer already drops a commit whose rev isn't newer than
the one it holds. `#identity` and `#account` events restate current state, so a repeat is harmless.
That's the price of a lost quorum, and with the commitlog it's only paid when two disks are gone (or,
in `page-cache` mode, when a majority loses power within ~100 ms).
[Bucket recovery](#bucket-recovery) has the details.

## The flush

```steps
- title: Fence
  body: A new leader first CASes `qlog/manifest` to its own epoch with the content unchanged (one GET and one PUT a takeover, off the ack path). An older leader's flush then fails its CAS because the ETag moved, and one that reads the manifest afterwards sees a newer epoch and steps down.
- title: Seal the state at exactly F
  body: The leader applies committed entries to its state as they commit. To flush, it picks F (the seq its state has applied), writes nothing past F until a SlateDB checkpoint exists, then reads the checkpoint back and refuses it unless it holds exactly F.
- title: Upload the segments
  body: "The log in (previous F, F] goes up as vlpds segments, `log/qlog/{ordinal:012}.seg`: create-only, zstd at level 1, cut at 64 MiB raw. At today's rate a 30 s flush is one ~40 MiB segment."
- title: CAS the manifest
  body: The manifest names F, R, this flush's segments, the next ordinal, the state checkpoint and the host cursors as of F. Its CAS is the commit. A crash before it leaves the previous manifest in charge.
- title: Clean up
  body: After the CAS the leader deletes the previous manifest's checkpoint. A new leader deletes any `qlog-*` checkpoint the manifest doesn't name, which covers flushes that died between the seal and the CAS.
```

The bucket holds one object that recovery trusts, `qlog/manifest`, and it's written last with a CAS
(`If-Match` on the ETag the leader read or wrote before). Only committed entries are ever flushed,
since an uncommitted one could be dropped by a takeover. A flush that lands before a new leader's
fence is harmless: everything it names is committed, and F and R only go up.

A single object can't hold it all, because SlateDB writes its own SSTs and manifest. So the state is
referenced by a SlateDB checkpoint, and the quorum manifest names it. Keeping segments as their own
objects also spreads the upload over the interval and lets backfill GET a segment by name. vlpds
keys segments by a dense ordinal and keeps the first and last seq in the header, so vlpds's backfill
reader serves them as they are.

The manifest is JSON:
`{epoch, leader, flushed: F, reserve: R, next_ordinal, segments: [{ordinal, first, last, bytes}], state: {checkpoint, manifest_id, seq}, cursors: {host: cursor}, flushes, at_ms}`,
plus `gaps` and `recovery` after a [bucket recovery](#bucket-recovery). It's a few KB with 64 hosts.
It lists only this flush's segments: ordinals are dense from 0 and headers carry seqs, so the full
list would only grow with retention.

A flush that finds its ordinal taken reads that segment's header. If the segment starts where this
flush's would and ends at or below F, the flush adopts it. That's a flush that died after a segment
PUT, or a deposed leader's, and committed entries are the same on every node. If it ends past F, the
flush retries next tick. Anything else stops the flush.

### Cursors can't get ahead of the log

The danger is a cursor that says "PDS X is done through 1,000" in a flush whose log only holds X's
events through 990. After a crash, the relay would ask X for 1,001 onwards and 991-1,000 would be
gone. So cursors get into the log the same way events do:

- Each host owner sends its hosts' acked cursors to the leader about once a second, on its next
  submit. An acked cursor C for host X means every event of X up to C is committed, a duplicate or
  refused.
- The leader puts them on the first entry it appends after receiving them (`Entry::cursors`),
  holding them if the submit had no events. They're replicated and journaled with that entry and
  never emitted. They ride on an event instead of taking a seq of their own, which would put holes
  in the dense stream consumers see.
- An event counted in C was committed before the host owner sent C, so its seq is below the entry
  that carries C. The state applies the cursors, and the manifest's cursors are the state's at F.

So every event a manifest's cursors count is at or below F, and in the flushed log. The same entries
let a new leader resume a dead node's hosts from a cursor ~1 s old, from its own log, without the
bucket.

### The state at exactly F

The state follows the same rule. If the state got ahead of the log, a re-ingested commit would hit a
rev the state already holds and be dropped as a duplicate, which loses it.

The state is one SlateDB at `qlog/state` with its WAL off, written only by the leader's applier.
Each committed entry's writes go in with SlateDB's user seqnum set to the entry's seq, which makes a
checkpoint's `last_l0_seq` the log seq its state reaches. SlateDB flushes memtables on its own (at
its L0 size), and a `flush()` or `create_checkpoint` can land in a manifest that also holds writes
made after it was asked for. It promises "at least F", never "exactly F". So the seal doesn't rely on
SlateDB staying quiet. The applier writes nothing past F until `create_checkpoint(All)` returns,
then the seal reads the checkpoint's manifest back and refuses it unless `last_l0_seq` is F.
Automatic flushes at other times are fine, since they only ever hold seqs at or below what's applied.
No change to SlateDB was needed.

The seal pauses only the applier: 20-350 ms a flush on a local MinIO, ~0.9 s p50 on R2. Acks and
emission don't wait on the state. `l0_max_ssts` is 32 here (as in vlpds), since SlateDB's default of
8 made seals wait 1-5 s for the compactor at 2 s flushes.

Followers keep no state. A new leader opens `qlog/state` at whatever the old leader last flushed (at
or past the manifest's F), then stages every entry above that from its own log, the adopted tail
included. Only then does it admit new events. Until then it answers "retry". On the relay that's
4-25k entries read in the same step as the open, 8-90 ms in all. Every node keeps its log above F,
so the replay always has what it needs. Keeping one writer also keeps the bucket at one L0 flush an
interval.

What isn't in the log: unlogged record changes (failed-check counts, a desync mark from a failed
check) stay in the leader's memory. A takeover loses them, and the next bad commit marks the account
again. `relay_throttled` and the signing key ride in the record, so they carry over. Host counters
are per node and in memory.

### What a flush costs

| load | flush | segment PUTs/s | Class A/s | Class B/s | R2 req $/mo, no free tier | R2 req $/mo | S3 req $/mo | bucket GB | R2 storage | S3 storage |
|---|---|---|---|---|---|---|---|---|---|---|
| today | 10 s | 0.10 | 1.00 | 2.46 | $14 | $7 | $16 | 323 | $5 | $7 |
| today | 30 s | 0.03 | 0.33 | 1.66 | $6 | $0 | $6 | 323 | $5 | $7 |
| today | 60 s | 0.03 | 0.18 | 1.46 | $4 | $0 | $4 | 323 | $5 | $7 |
| 10x | 10 s | 0.30 | 1.20 | 2.46 | $17 | $10 | $18 | 3,228 | $48 | $74 |
| 10x | 30 s | 0.30 | 0.60 | 1.66 | $9 | $3 | $10 | 3,228 | $48 | $74 |
| 10x | 60 s | 0.28 | 0.43 | 1.46 | $7 | $1 | $7 | 3,228 | $48 | $74 |
| 100x | 10 s | 2.80 | 3.70 | 2.46 | $46 | $39 | $51 | 32,281 | $484 | $742 |
| 100x | 30 s | 2.77 | 3.07 | 1.66 | $38 | $32 | $42 | 32,281 | $484 | $742 |
| 100x | 60 s | 2.77 | 2.92 | 1.46 | $36 | $30 | $40 | 32,281 | $484 | $742 |

A flush is a manifest CAS, its segments (cut at 64 MiB raw, the last one partial) and the state's
share: ~8 Class A and ~12 Class B a flush for its L0, the checkpoint it seals, the one it retires and
compaction. On top of that SlateDB polls ~1.26 Class B a second. Those are measured numbers for the
one SlateDB the leader keeps, less SlateDB's GC deletes, which R2 doesn't bill as Class A
([Counting requests](quorum-measurements.md#counting-requests)). Segments are one PUT a flush today
and ~2.8 a second at 100x. Against the old design:

| load | old, 3 nodes | quorum HA, 30 s | old, one node | quorum single, 30 s |
|---|---|---|---|---|
| today | $2,500 | $6 | $442 | $6 |
| 10x | $2,765 | $9 | $477 | $9 |
| 100x | $2,799 | $38 | $481 | $38 |

Nothing in this bill follows host shards any more. The old design's ~$1.3k of host bookkeeping (a
GET and a CAS per host shard every 2-5 s, plus every node re-reading them) folds into one manifest
per flush. Changes that must be seen at once (an admission, an operator ban) go into the log as
entries and land in the next manifest.

On R2 a flush takes seconds (2.35 s p50, 11.4 s max over an hour at 30 s), because it sends ~20
requests and many of them in sequence. That's fine at 30-60 s. At a 10 s flush the p99 would eat
the interval, so on R2 30-60 s is the choice.

## Leadership, epochs and fencing

vlpds already has the pieces for "one writer, decided by the bucket". The quorum log takes some of
them:

| vlpds piece | Use here | Why |
|---|---|---|
| CAS'd assignment with an epoch (`assign/{shard}`: owner, epoch) | yes, as `qlog/leader` {epoch, leader, members} | one leader per epoch, decided by the bucket's linearizable CAS, no clocks |
| create-only segment PUTs and the fence object | yes, as create-only segments and the manifest CAS | a zombie leader can't overwrite a segment or commit a flush |
| self fail-stop | yes | a leader that can't reach a quorum can't commit anyway, and it steps down after a heartbeat timeout |
| refused-probe fast path | yes | a refused peer port means the process is gone, so takeover starts at once |
| planned handoff with a barrier | yes, for moving leadership | stop appending, wait for a member to hold the whole log, CAS the epoch to it |
| node leases (`nodes/`) for liveness | no | they cost a CAS and two LISTs per node every 2 s (~$57 a month on R2 at TTL 10 s) and take TTL + skew (12 s) to notice a hung box. Peer heartbeats notice it in ~1 s for free |
| `seq_floor` commit-wait, `set_revalidate`, lapse grace | no | seqs are counters, and safety comes from the quorum promise |

The leases fence the bucket. They don't fence memory. A zombie leader whose bucket writes all fail
could still get a follower to ack an entry, and then emit it. So each member keeps a promised epoch,
the highest it has seen, and refuses entries from a lower one. That's the piece vlpds doesn't have.
Liveness comes from peer heartbeats only, so the bucket sees one GET and one CAS a takeover and
nothing in steady state.

### Takeover

```steps
- title: Detect
  body: The leader dials its followers and heartbeats every 100 ms when idle (`--qlog-heartbeat-ms`). A follower sees its inbound connection close the moment the leader's process dies, and probes the leader's port at once. A refused or reset connection starts a takeover. Otherwise 300 ms of silence triggers one probe, and 1 s (`--qlog-election-ms`) starts a takeover.
- title: Pre-vote
  body: The candidate needs pongs from enough members to make a quorum before it touches the bucket. Without that, a cut-off node would CAS a higher epoch and unseat the majority's leader when the partition healed. The pings wait only until they have a quorum.
- title: CAS the epoch
  body: The candidate CASes `qlog/leader` from epoch e to e + 1, naming itself. The lowest-ranked follower tries first and the others wait 500 ms per rank. If two try, one CAS wins.
- title: Promise round
  body: The new leader asks every member to promise e + 1. Each one that does stops accepting epoch e and answers with the last `(epoch, seq)` it holds.
- title: Adopt the longest tail
  body: With promises from a quorum of intact members (itself included), it adopts the tail with the highest last epoch, then the highest seq, and fetches what it lacks from that member (4 MiB a request). Every committed entry is on a quorum, and any two quorums share a member, so the adopted tail holds every committed entry.
- title: Re-tag and lead
  body: It re-tags every entry above its commit index with its own epoch, replicates the tail (followers truncate anything that differs), opens the state, replays, and starts admitting.
```

That's Raft's election with the bucket's CAS in place of the vote count. The term is the epoch and
the promise is the vote. Re-tagging the adopted tail is Raft's rule that only a current-term entry
commits by count, done without a no-op entry (a no-op would burn a seq that consumers would see as a
hole). Seqs and bytes don't change, and followers holding the old tag replace it through the matching
rule. What's skipped from Raft is randomized election timers and joint consensus, since the CAS
picks the leader and membership only changes at a flush barrier ([below](#membership-changes)).

A node counts toward a takeover only if it's intact, which means its log still holds everything it
ever acked. A node with a new or wiped commitlog isn't intact, and neither is a `memory` node that
restarted, or a `page-cache` node that rebooted (see [Durability modes](#durability-modes)). Such a
node still promises, but it only counts again once it holds the leader's commit index as of when it
rejoined. Without this rule, a quorum of one real log and one emptied one could elect a leader that
lacks an emitted entry.

The emission pause when the leader's process dies is ~75 ms median on the relay at 10x, and a hung or
cut-off leader takes the 1 s election timeout. On R2 the CAS adds a PUT (205-410 ms p50).

### Failover, case by case

Pauses are the relay chaos runs' medians at 10x. The multi-node ones include the test harness's 1 s
supervisor restart, so a real deploy's restart time replaces that second.

| Event | What happens | Emission pause | Lost |
|---|---|---|---|
| A follower dies | the other follower carries the quorum | none | nothing |
| The leader's process dies | refused probe, pre-vote, CAS, promise, adopt | ~75 ms (`fsync`), ~140 ms (`page-cache`), ~200 ms (`memory`) | nothing |
| The leader hangs or is cut off | election timeout, then as above | ~1.0 s | nothing |
| A partition cuts one node off | the side with two carries on (with a new leader if needed) and the lone node stops emitting | none, or as a takeover | nothing |
| Two or three processes die (a bad deploy) | `fsync` and `page-cache`: the page cache kept their writes, so a normal takeover. `memory`: bucket recovery | 1.8-2.3 s | `memory`: the tail since the last flush, re-ingested |
| Power loss on all three | `fsync`: normal takeover from any two disks. `page-cache` and `memory`: bucket recovery | 2.3-2.7 s | `page-cache` and `memory`: the tail, re-ingested |
| Two disks or boxes gone for good | bucket recovery, jump to R + 1, re-ingest | ~2.4 s | the tail since the last flush, re-ingested |
| The bucket is down | emission carries on until R, then stops | after H (3 intervals at 100x) | nothing |
| One slow disk | the quorum waits for the faster two | none | nothing |

### Placement

Replicas go on distinct physical boxes. How far apart is a latency trade (assumed RTTs):

| Placement | Follower RTT | Quorum ack | What one event takes out |
|---|---|---|---|
| one DC, three dedicated boxes | ~0.2 ms | ~2-4 ms | a DC power or network event takes all three |
| one metro, three DCs (Hetzner FSN1, NBG1, HEL1 on a private network. OVH RBX, GRA, SBG on the vRack) | ~3 ms to the nearest | ~5-7 ms | one DC |
| two regions (OVH US East and US West) | ~65 ms | ~67 ms | one region |

A quorum only waits for the nearest follower, so a far third replica costs nothing on the ack path.
Put the leader and one follower close and the third anywhere. With `fsync` durability, a single DC is
also safe against a DC-wide power cut. On VPSes "distinct boxes" needs care. Hetzner Cloud's spread
placement groups guarantee different hosts. OVH VPSes have no anti-affinity, so put them in three
locations. Don't put two members on one disk either: three nodes on one consumer NVMe fsync at 5.5 ms
instead of 2.7 and share its write bandwidth.

The transport between members matters at 10x on small boxes. A real three-host run over Tailscale
couldn't hold 3,500 events/s with the leader on a 2-vCPU VPS, because tailscaled's userspace
WireGuard used 145% of the box while the node used 27%
([Three hosts over Tailscale](quorum-measurements.md#three-hosts-over-tailscale)). The relay was fine,
the tunnel wasn't. A leader on a small VPS wants kernel WireGuard, a private network or more cores.

## Durability modes

`--durability` says when an entry counts on a node: when a follower acks it and when the leader
counts itself toward the quorum.

| mode | an entry counts | a process crash | a power cut | default for |
|---|---|---|---|---|
| `fsync` | after its commitlog fdatasync | loses nothing | loses nothing acked | one node (the only mode it runs), and two |
| `page-cache` | once written to the commitlog (the page cache), fdatasync'd in the background every `--durability-sync-ms` (100) | loses nothing, since the kernel still holds the pages | can lose the last ~100 ms of acked writes on that box | three members or more |
| `memory` | in memory, with no commitlog | the node comes back empty | the same | |

Promises are fdatasync'd before they're answered in every mode, so a node never promises an older
epoch after a newer one, power cut or not.

`page-cache` takes the fsync off the ack. With a 2 ms emulated fsync at 3,500/s, acks were 0.24 ms
p50 against 2.28 ms in `fsync` mode, and on one shared consumer NVMe its ceiling was ~2x
(~35,500/s committed against ~18,000/s). What it gives up is one case: a power cut on a majority
within ~100 ms becomes a bucket recovery with a gap, where `fsync` would make it a takeover. On
separate boxes on separate power that's a much rarer failure than a process crash, which
`page-cache` survives like `fsync`. So it's the default for three or more. One node runs `fsync`,
since its commitlog is the only copy.

### Why it's safe

Two rules hold in every mode: a seq is never reused, and nothing is emitted before a quorum holds it
(in the mode's sense: a majority's disks, page caches or memory). What changes between modes is which
failures a node survives with its log whole. So the rule that decides it is that a node counts as
intact only if its log still holds everything it ever acked.

1. Epochs. A promise is on disk before it's answered, and a takeover needs a quorum of promises and
   the `qlog/leader` CAS. So two leaders never hold one epoch.
2. Who is intact. In `fsync` mode every acked entry was synced first, so a restarted node is intact.
   In `page-cache` mode a process crash leaves the page cache, so the node is intact too. A power cut
   can drop the unsynced tail, and the node can't tell that from the file. So the commitlog
   directory records the boot id and the mode of the run that wrote it, fsynced at open. If the
   previous run wrote in `page-cache` mode and the boot id has changed, the node keeps its log to
   catch up from but starts not intact. In `memory` mode a restarted node is empty, and not intact
   for the same reason.
3. Commits and takeovers count only intact logs. A committed entry sits in the logs of a majority
   that were intact when they acked it. Any later takeover quorum of intact logs meets that majority
   in a node that's still intact, because a node that lost power since was excluded in step 2. So
   the new leader holds every committed entry, and nothing emitted is reissued.
4. When no quorum of intact logs can exist (power cut on a majority within the sync window in
   `page-cache` mode, a majority of `memory` nodes restarting), the candidate runs the bucket
   recovery. No seq above R was ever emitted while that manifest was current, so no seq is reused,
   and the events inside the gap come back from the PDSes under new seqs.

The chaos checks this with a mutation. With the step 2 check turned off (`--qlog-unsafe-trust-log`),
`power-cut-all` fails with 257 seqs "emitted with two contents". Residual risks:

- A power cut on a majority within ~100 ms in `page-cache` mode is a bucket recovery with a gap. It
  never loses anything silently.
- A disk that loses data after an fdatasync (no power-loss protection, lying firmware) breaks
  `fsync` mode the same way. That's why the cluster wants three boxes on three disks.
- A VM that's frozen and resumed later keeps its page cache. That's a pause, not a loss.

### Commitlog or memory

The study behind this design compared a quorum in memory with a quorum plus a local commitlog, as in
Scylla:

| | `memory` | commitlog (`fsync` or `page-cache`) |
|---|---|---|
| Host cost | same hosts (every host here has local NVMe) | same, the disk was already there |
| RAM | the log window and the firehose ring as two copies (~1.0 GB at 10x measured) | the ring plus a 64 MiB log window (~0.6 GB at 10x measured) |
| Ack latency | one RTT + group commit | `page-cache`: about the same. `fsync`: + 1-1.7x the device's fsync |
| A bad deploy kills all three processes | bucket recovery, jump, re-ingest | normal takeover, nothing lost |
| Power loss on all three | bucket recovery, jump, re-ingest | `fsync`: normal takeover. `page-cache`: bucket recovery |
| Two disks lost | bucket recovery, jump, re-ingest | the same |
| Disk wear | none | ~0.16 TB a day today, 1.6 at 10x, 16 at 100x |

| load | written a day (raw frames) | consumer 1 TB drive, 600 TBW | datacenter 1.92 TB, 1 DWPD for 5 years |
|---|---|---|---|
| today | 0.16 TB | 10.3 years | 59.9 years |
| 10x | 1.60 TB | 1.0 years | 6.0 years |
| 100x | 16.03 TB | 0.1 years | 0.6 years |

The correlated failures a 3-box cluster actually sees are a bad deploy, a shared kernel or OOM bug,
and a DC power event. Memory alone turns each of those into a re-ingest and a seq jump that consumers
notice. The commitlog turns them into a normal takeover, costs no extra money on these hosts, and is
the single node's WAL anyway. Two things to watch. Past ~10x a consumer drive wears out in about a
year, so a cluster at that rate wants datacenter drives (compressing the commitlog at zstd -1 would
cut writes by a third). And a VPS's fsync may be acknowledged by a cache without reaching a disk with
power-loss protection. Measure it with `tests/qlog/fsync_probe.sh`: a VPS whose fdatasync is well
under 0.1 ms is answering from a cache. One OVH VPS measured 0.6-0.7 ms, steady over two hours
([One VPS](quorum-measurements.md#one-vps-one-node)).

| device | fsync | a WAL emit adds (group commit) |
|---|---|---|
| datacenter NVMe with power-loss protection (AX42, ADVANCE-2, EC2 instance store) | 0.03-0.1 ms (assumed) | up to 2 ms + 0.03-0.1 ms |
| VPS virtual NVMe (OVH VPS, Hetzner Cloud) | 0.5-2 ms (assumed), 0.6-0.7 ms on one OVH VPS (measured) | up to 2 ms + 0.5-2 ms |
| consumer NVMe without power-loss protection | 2.7 ms p50, 5.9 p99 (measured on a 970 EVO Plus, one writer; 5.5 ms with three) | up to 2 ms + ~3 ms |

| follower placement | RTT | quorum ack, memory only | quorum ack, commitlog fsynced |
|---|---|---|---|
| one DC | 0.2 ms | 2.2 ms | 2.3 ms (dc) / 4.2 ms (vps) |
| one metro (FSN-NBG, RBX-GRA) | 3 ms | 5 ms | 5.1 ms (dc) / 7 ms (vps) |
| cross-region (US East-US West) | 65 ms | 67 ms | 67.1 ms (dc) / 69 ms (vps) |
| old design: 25 ms linger + an R2 PUT |  | ~225 ms p50 |  |

These are modeled. Measured, the `fsync` ack is about RTT + 1-1.7x the device's fsync + ~0.3 ms, and
in one DC or one metro that's 30-100x faster than the old design's linger plus an R2 PUT.

### The commitlog

`src/qlog/commitlog.rs` is the local log behind every member and the single node's WAL. It knows
nothing about replication: a caller stages ops and waits for them to count.

- What's written. The in-memory log journals every change it makes: appends (with their cursors
  and metadata), truncations, resets and restamps (written as a truncation plus the entries again,
  so the last record of a seq is always its current state). The node stages the journal under its
  lock, so the disk sees changes in the order memory made them. Promises are journaled the same way,
  with whom they went to. The commit index rides along after each batch as a lower bound, so a
  restarted node can emit at once.
- Record format. `len u32 | crc32 u32 | type u8 | payload`, little endian, the CRC over type and
  payload. That's 25 bytes over each ~5.3 KB frame (0.5%).
- Group commit. One writer thread drains everything staged, writes it with one `write` and
  `fdatasync`s, then publishes the batch's ticket. There's no linger: a batch is whatever arrived
  during the previous fsync. A failed write or fsync poisons the log, nothing more is acked, and the
  process aborts (after a failed fsync the page cache can't be trusted).
- Torn writes. Truncations are records, not rewrites, so a crash can't half-apply one. Recovery
  replays the segments in order and stops the last one at its first bad record (short, too long, a
  CRC mismatch or zeros), truncates the file there and fsyncs it. Everything past the last fsync was
  never acked. A bad record in any segment but the last is corruption, and the node refuses to start.
- Segments and trimming. A segment rolls at 64 MiB (`--qlog-segment-mb`). The new one starts at
  the commit index and repeats the uncommitted tail, so any later truncation lands inside it and
  older segments can be deleted on their own. A node's floor is min(emitted, commit, F), so only what's
  in the bucket leaves local disk. The leader also keeps its log back to any follower heard from in
  the last 10 s. Past the floor, segments go once the log is over `--qlog-disk-retain-mb` (4 GiB).
- Memory. Each node keeps `--qlog-memory-mb` of committed log in memory (64 MiB with a commitlog,
  512 without) on top of the firehose's ring, and indexes the rest on disk at 32 bytes an entry.
- Backpressure. The leader takes no new submits while 256 MiB is uncommitted. It waits up to
  500 ms (under the submitter's 1 s timeout) and then answers "busy". Without it, a quorum slower
  than its submitters grows the leader's memory without bound: an early ceiling run on one shared
  disk reached 52 GB RSS before the kernel killed two nodes.
- Recovery time grows with what's on disk, ~0.35 s a GB on the bench box (~1.35 s a GB on a 2-vCPU
  VPS). Trimming at F keeps it to about an interval plus a segment.

## Bucket recovery

```steps
- title: Notice the lost quorum
  body: A candidate that has won `qlog/leader` collects promises. If fewer than a quorum of the members that promised are intact, it counts every member that didn't answer as possibly intact. Only when even that falls short of a quorum is the quorum lost.
- title: Keep what exists
  body: "In order: the manifest's F, then orphan segments past it that continue the log densely (a flush that died before its CAS), then the longest committed prefix past those that any promising member holds. Call the end S."
- title: Clone the state
  body: The state becomes a SlateDB clone of the manifest's checkpoint at `qlog/state-e{epoch}`, with the orphans and the salvage applied, `_applied` moved to R, and a seal at R.
- title: CAS the recovery manifest
  body: "F = R, R' = R + H, the salvaged segments, the cursors at S, and a gap `(S, R]` recorded for good. Losing the CAS, or a crash before it, leaves the old manifest in charge."
- title: Lead from R + 1
  body: The node resets its log to `(epoch, R)`, followers are reset to R, and the host owners re-read every host from the recovery's cursors.
```

The trigger is automatic, because the count is the proof an operator would check. A member that
promised this epoch and isn't intact can't become intact behind the candidate's back, since it now
refuses older leaders. So "intact that promised + silent < quorum" proves that no quorum of intact
logs exists anywhere. Wiping one disk while the leader is dead leaves one intact log answering and one
member silent. That's 1 + 1 ≥ 2, so the candidate waits, and the dead leader's return makes a normal
takeover. The candidate already has a quorum's pongs before its CAS, so a minority never recovers on
its own: a node that can't reach the others can't tell "they're dead" from "I'm cut off".
`--qlog-no-auto-recover` turns the trigger into a log line and a wait, for an operator who wants to
decide.

Why a clone. The clone is a new SlateDB manifest over the checkpoint's SSTs, so it's O(1) in the
state's size (25-46 ms on a local MinIO). Rewriting every key changed past F in place would mean
diffing the whole state, ~850 GB a node at 100x. The clone pins its source with a checkpoint of its
own, and SlateDB lists the source as an external database until compaction has rewritten its SSTs.
Retention deletes an old state path only once nothing the current state reads lists it.

Salvage streams. A task fetches committed 4 MiB chunks from the best member, and the recovery applies
each to the cloned state and adds it to a segment builder, PUTting a segment at every 64 MiB raw. So
the recovering node holds one segment and two chunks however long the interval. A recovering
candidate re-sends its promise every heartbeat, so a long salvage (3-5 s at 100x with a 10 s flush)
doesn't let another member take over mid-recovery and jump a second time.

A segment past the manifest that starts at or below F is a deposed leader's flush still running.
Recovery deletes it, so it can't take an ordinal the recovery or the next flush writes. Two recoveries
in a row jump twice: an attempt that dies after its manifest CAS may have led and committed above its
R, so the next one recovers from that manifest and skips another H. At the default H that's still
~3,000 years of seqs at 100k/s for a recovery a day.

What consumers see:

| consumer | sees |
|---|---|
| live on a node that kept running | everything up to S (topped up from the bucket if needed), then R + 1 on: one jump over `(S, R]` |
| reconnecting at or below S | the bucket to S, then R + 1 on |
| reconnecting in `(S, R]` (it saw events the recovery lost) | R + 1 on |
| above R | `FutureCursor` after the 2 s grace |

No seq is ever emitted with two contents. Emitters that cross a gap check the manifest's `gaps`
first and read the bucket only outside them, so a deposed leader's segment sitting at the next
ordinal can't leak seqs from inside `(S, R]`.

Re-ingest. Each answer to a submit carries the leader's recovery generation. A host owner that sees
a higher generation asks the leader for that recovery's cursors, re-reads each host from there, and
marks everything at or below them done. The leader drops cursors from a submitter that hasn't rewound
yet, since they may count events the recovery lost. Events that come back twice are dropped by the
relay's `check_chain` against the state and by consumers' rev checks.

### The re-ingest storm

The storm is the events between the cursors and the crash. Worst case, the crash lands just before a
flush, so the window is the interval plus an upload (~2 s) plus detection and takeover (~5 s, both
assumed):

| load | flush | window | events re-requested | per bsky.network PDS | all other PDSes together | bytes | catch-up |
|---|---|---|---|---|---|---|---|
| today | 10 s | 17 s | 5,950 | 65 | 149 | 0.03 GB | 5.8 s |
| today | 30 s | 37 s | 12,950 | 142 | 324 | 0.07 GB | 12.7 s |
| today | 60 s | 67 s | 23,450 | 257 | 586 | 0.12 GB | 23.0 s |
| 10x | 10 s | 17 s | 59,500 | 652 | 1,488 | 0.32 GB | 5.8 s |
| 10x | 30 s | 37 s | 129,500 | 1,419 | 3,238 | 0.69 GB | 12.7 s |
| 10x | 60 s | 67 s | 234,500 | 2,569 | 5,863 | 1.24 GB | 23.0 s |
| 100x | 10 s | 17 s | 595,000 | 6,518 | 14,875 | 3.15 GB | 5.8 s |
| 100x | 30 s | 37 s | 1,295,000 | 14,187 | 32,375 | 6.86 GB | 12.7 s |
| 100x | 60 s | 67 s | 2,345,000 | 25,690 | 58,625 | 12.43 GB | 23.0 s |

Per PDS it's small. A bsky.network PDS (89 of them hold ~97.5% of accounts) replays ~140 events today
at a 30 s flush, and ~14k at 100x. The reference PDS serves cursors from its sequencer table for much
longer than a minute, so a 67 s window is well inside what it keeps. Catch-up is the backlog over the
CPU headroom the nodes are sized with (2x the peak hour at 70%), so 6-23 s at any load. A small PDS
that can't replay that far sends `OutdatedCursor`, and that host has a gap. Measured at 10x with a
30 s flush, a recovery re-ingested ~70-81k events, inside the 129.5k worst case.

## A single node

A node with no `--qlog-peer` is a quorum of one. Its commitlog is the WAL, an entry commits at its own
fsync, and emission follows the local durable point as for any member. It runs only `fsync` mode, and
it leads at once (it has nobody to hear from). Every N seconds it flushes segments, the state and a
manifest exactly as a leader does.

| Event | Recovery | Consumers see |
|---|---|---|
| process crash, OOM, deploy restart | replay the WAL, reconnect hosts from the cursors in it | a pause of a second or two, no gap, no jump |
| kernel crash or power loss | the same, since everything emitted was fsynced | the same |
| the disk or the box is gone | bucket recovery: manifest, state at F, seqs from R + 1, re-ingest from the cursors | a jump to R + 1, repeated commits since F |

The relay always has a bucket (`--s3-endpoint` and `--s3-bucket` are required). The cost model also
prices a node with no bucket and a bucket holding only the state and cursors, to show what the bucket
is worth. Neither mode is built. A state-and-cursors bucket would turn "losing the disk" from a gap
into a re-ingest and fit inside R2's free tier at a 60 s flush:

| load | flush | 72 h log: A/s / B/s | R2 / S3 $/mo | state and cursors only: A/s / B/s | R2 / S3 $/mo |
|---|---|---|---|---|---|
| today | 10 s | 1.00 / 2.46 | $12 / $23 | 0.90 / 2.46 | $6 / $15 |
| today | 30 s | 0.33 / 1.66 | $5 / $14 | 0.30 / 1.66 | $0 / $6 |
| today | 60 s | 0.18 / 1.46 | $5 / $11 | 0.15 / 1.46 | $0 / $4 |
| 10x | 10 s | 1.20 / 2.46 | $58 / $93 | 0.90 / 2.46 | $8 / $18 |
| 10x | 30 s | 0.60 / 1.66 | $51 / $84 | 0.30 / 1.66 | $2 / $9 |
| 10x | 60 s | 0.43 / 1.46 | $49 / $81 | 0.15 / 1.46 | $2 / $7 |
| 100x | 10 s | 3.70 / 2.46 | $523 / $794 | 0.90 / 2.46 | $28 / $48 |
| 100x | 30 s | 3.07 / 1.66 | $516 / $785 | 0.30 / 1.66 | $22 / $39 |
| 100x | 60 s | 2.92 / 1.46 | $514 / $782 | 0.15 / 1.46 | $22 / $37 |

One OVH VPS-1 with R2 holding 72 h is ~$9 a month at a 30 or 60 s flush. What dominates is the host,
then 72 h of log storage. The fit across hosts (the hours are how much of the log fits on the disk):

| host | $/mo | today | 10x | 100x |
|---|---|---|---|---|
| OVH VPS-1 | $5 | $9 (4 h on disk) | no: disk 138/40 GB, port 1.6 Gb/s/0.5 Gb/s | no: CPU 12.3/2, disk 1,291/40 GB, port 16.3 Gb/s/0.5 Gb/s |
| OVH VPS-2 | $8 | $13 (11 h on disk) | no: disk 138/75 GB, port 1.6 Gb/s/1 Gb/s | no: CPU 12.3/4, disk 1,291/75 GB, port 16.3 Gb/s/1 Gb/s |
| OVH VPS-3 | $12 | $17 (16 h on disk) | no: disk 138/100 GB, port 1.6 Gb/s/2 Gb/s | no: CPU 12.3/6, disk 1,291/100 GB, port 16.3 Gb/s/2 Gb/s |
| OVH VPS-4 | $23 | $28 (35 h on disk) | $74 (2 h on disk) | no: CPU 12.3/8, disk 1,291/200 GB, port 16.3 Gb/s/3 Gb/s |
| Hetzner CX33 | $10 | $49 (12 h on disk) | no: disk 138/80 GB, port 1.6 Gb/s/1 Gb/s | no: CPU 12.3/4, disk 1,291/80 GB, port 16.3 Gb/s/1 Gb/s |
| Hetzner CAX21 (ARM) | $12 | $52 (12 h on disk) | no: disk 138/80 GB, port 1.6 Gb/s/1 Gb/s | no: CPU 12.3/4, disk 1,291/80 GB, port 16.3 Gb/s/1 Gb/s |
| Hetzner CPX22 | $23 | $62 (12 h on disk) | no: disk 138/80 GB, port 1.6 Gb/s/1 Gb/s | no: CPU 12.3/2, disk 1,291/80 GB, port 16.3 Gb/s/1 Gb/s |
| Hetzner AX42 | $109 | $114 (72 h on disk) | $487, 3 edges (36 h on disk) | no: port 16.3 Gb/s/1 Gb/s |
| Hetzner AX42 + 10G | $157 | $196 (72 h on disk) | $769 (36 h on disk) | $3,071, 22 edges (2 h on disk) |
| OVH ADVANCE-2 | $198 | $203 (72 h on disk) | $249 (17 h on disk) | no: disk 1,291/960 GB, port 16.3 Gb/s/3 Gb/s |

The 4 GB VPS-1 runs today's load at ~2.5 GB (assumed: the [shadow run](shadow.md)'s 1.1 GB at 60
events/s, with the identity cache growing with the rate), which is tight. Hetzner's CX33 would be
~$10 for the host, but its 20 TB of included traffic runs out at four full-firehose consumers, and the
listing shows it as not orderable. Measured, one 2-vCPU VPS carried ~30,000-40,000 events/s as a single
node with the load generator and checker on the same box, about 85x today's rate
([One VPS](quorum-measurements.md#one-vps-one-node)).

## Membership changes

```steps
- title: Add learners
  body: The leader records the new members as learners in `qlog/leader` at the same epoch (a CAS) and replicates to them. A learner starts at the leader's oldest local entry, emits what's committed like any follower, and never counts toward a commit or a promise round.
- title: Pre-flush
  body: Once every learner is intact and holds the commit index, the leader flushes, so the barrier's own flush is small.
- title: Pause and drain
  body: Appends pause (submits wait up to 500 ms, then get "busy" and resend). The leader waits until everything appended has committed under the old set, and every learner and enough of the new set durably hold it. That last seq is the barrier B.
- title: Flush to B and CAS
  body: The leader flushes to B, then CASes `qlog/leader` from (e, old set) to (e + 1, new set). It leads e + 1 with the new set, or hands off if it isn't in it.
- title: Resume
  body: Commits resume under the new set. Every abort before the CAS resumes at e with the old set.
```

The member set lives in `qlog/leader` (`{epoch, leader, members, learners, addrs, since}`). Commit
counting, the pre-vote, the promise round, a candidate's rank and the lost-quorum trigger all use the
record's members. `--qlog-peer` is only an address book, and `--qlog-members` is the bootstrap set the
first CAS writes. A takeover copies the set into the next epoch. A node the record doesn't name
doesn't campaign, and a removed node retires. [Cluster](cluster.md#changing-the-members) has the
commands.

Why one step from the old set to the new is safe without joint consensus. Joint consensus exists
because, in Raft, two configurations can each make decisions during a change. Here each epoch has
exactly one set, the CAS decides which, and every decision is made by a leader of one epoch with that
epoch's set:

- No entry is committed under the new set before the CAS, because nobody leads e + 1 yet.
- No entry is committed under the old set after the CAS. The leader of e appends nothing after B,
  and it moves to e + 1 (or steps down) in the same locked step that ends the pause. Any later
  leader read the record, so it uses the new set or a later one.
- Every entry committed under the old set is in the bucket (the flush to B). Beyond that, enough of
  the new set durably holds B that every quorum of the new set includes one, so a takeover's longest
  tail holds it too.
- A removed member is never sent an append or a promise of e + 1 or later, so it's never counted
  after the switch. A takeover by it would need the record to name it, and the CAS is against the
  ETag of what was read, so a stale read can't win.

So every committed entry is held by every later quorum (or the bucket), which is the takeover rule's
invariant. The flush alone would be enough for safety. The holders condition lets the new set take
over from its own logs at once, and the learners' catch-up lets it commit at once.

Replacing the leader is a handoff. A leader that isn't in the new set CASes e + 1 naming a member of
the new set that holds B, steps down, and sends it `Lead { epoch }`. That member checks the record
names it and runs the promise round directly. If the message is lost, the members time out and take
over from the record as usual. Measured, commits pause 6-13 ms for a change on a local MinIO, and a
leader handoff pauses emission 15-49 ms. On R2 a change pauses commits ~1-1.6 s (a manifest round plus
the `qlog/leader` CAS). Replacing a box is mostly copying the leader's local log to the learner: 4 GiB
took ~6 s on loopback, which would be ~35 s on a 1 Gb/s port and ~70 s on a VPS-1's 0.5 Gb/s.

Membership is the operator's. Nothing removes members on its own, and a change that can't catch its
learners up aborts with the old set intact (its learners stay in the record until the operator
retries or sets the members back). `POST /qlog/members` needs `Authorization: Bearer <token>` matching
`--qlog-admin-token`. With no token it's open only on loopback.

## Serving and backfill

Consumers are served by vlpds's `Firehose` as it is. The quorum log feeds it as one followed log
(`qlog`) whose watermark is the commit index, and it's handed only committed entries, in seq order
(`src/qlog/emit.rs`). With one log the k-way merge degenerates to "emit up to the watermark", which is
exactly the hold-until-quorum rule. Every node runs the same merger over its own commit index, so a
promoted follower keeps the same firehose. The subscriber registry, per-consumer series,
`ConsumerTooSlow`, per-IP caps, the takedown filter and the ring all carry over unchanged.

The emitter sends a batch before it stores the watermark, and the merger reads the watermark before it
drains, so it never holds a watermark past events it hasn't been given. The commit index only moves
when a quorum's acks cover a current-epoch entry, and the log panics rather than truncate or reset
anything at or below the commit index.

Reads come from three tiers:

1. The ring in memory (512 MB, ~290 s today, ~3 s at 100x).
2. The node's own log, in memory or in its commitlog, which always reaches back to F.
3. The bucket, for anything older. Segments are 64 MiB raw, and backfill reads them with 8 MiB
   ranged GETs. A 24 h replay today is ~12k GETs (~$0.004 on R2), where the old design's 4-event
   segments took ~7.2M GETs ($3).

vlpds's backfill assumes every emitted event is already in the bucket. The quorum log emits up to a
flush interval before it flushes, so a cursor between F and the ring floor would skip silently. The
one vlpds change is `firehose::LocalTail`, opt-in and for counted streams only: backfill reads the
bucket up to the tail's floor, then the node's own log up to the ring. A node creates its firehose at
the first entry it emits, so a node wiped past a recovery gap serves old cursors from the bucket across
every gap.

A follower behind the leader's disk is served from bucket segments (one segment cached per
replicator). Only a follower behind the bucket's retention is still reset, and its stream jumps.

## The relay on the log

The relay runs only on the quorum log (`src/node/quorum.rs`, plugged in through `qlog::node::Hooks`).
Every node keeps its upstream sockets, verify and lanes.

- One writer, still exactly F. The state's SlateDB is written only by the log's applier, which writes
  each committed entry's metadata writes with seqnum = its seq. The leader's view is a cache over the
  same database whose staged records are released once the applier has their entry. So the seal at F,
  the consistency check and bucket recovery all carry over, and salvage replays into the state too.
- Host shards are log entries. The leader keeps the host table: every host, its tier and the member
  that owns it. A host's owner is the live member (heard from within `--qlog-host-failover-ms`, 2 s)
  with the highest rendezvous hash, and only a dead owner's hosts move. A changed row rides the next
  appended entry as a state write (`h/{host}`), so a new leader starts from the old table with no
  reshuffle. Members read the table from the leader every `--qlog-host-poll-ms` (500 ms), with each
  host's newest committed cursor, so a node that gains a host resumes it from there. The row also
  carries the host's policy fields (operator throttle, account cap, the tier a ban restores, the
  action trail), so every member enforces the same ones. A member re-applies a host at once when a
  new table moves its tier or policy, and an operator's action sends its row at once and waits to
  read it back ([Admin API](admin-api.md#host-actions)).
- The leader admits a host's events only from the member its table names. Without that, a moved
  host's old owner (reading until its next poll) and the new one interleaved a DID's events at the
  leader in testing.
- Retention is a loop. Each leader term runs a retention pass every `--qlog-retain-every-secs` (600)
  with `--qlog-retain-hours` (72, 0 turns it off). It deletes segments past the horizon oldest first,
  after publishing the new floor (`pruned_seq` in `retain/qlog`), and old state paths that nothing
  lists any more.
- The PLC export reader and host discovery run on the leader, one term at a time, with their progress
  in the bucket (`plc/export-checkpoint.json`, `discovery/state.json`), so a new leader resumes them.
- The admin API answers for the node it's asked: consumers, host actions and the pipeline are that
  node's, accounts are the leader's (a follower names it), and operator takedowns are entries.

## Hosts and what a node needs

CPU is tiny at today's load. One node measured 65-70 µs an event on 8 physical cores and 89-94 µs on
SMT threads, and a 3-node cluster of the old design 142-156 µs an event across all its threads
(see [perf](perf.md)). At today's ~350 events/s that's ~0.03 cores for one node. The leader carries more than a third of the cluster's work (every apply, both copies out),
assumed here at half of it. Replication itself is small: ~6 µs an event on the leader at 100x
measured. Per node:

| load | flush | HA vCPUs | single vCPUs | HA RAM, memory only | HA RAM, commitlog | disk | HA public port | leader replication out | single public port |
|---|---|---|---|---|---|---|---|---|---|
| today | 10 s | 0.10 | 0.12 | 2.5 GB (0.0 tail) | 2.5 GB | 23 GB | 54 Mb/s | 30 Mb/s | 163 Mb/s |
| today | 60 s | 0.10 | 0.12 | 2.5 GB (0.2 tail) | 2.5 GB | 23 GB | 54 Mb/s | 30 Mb/s | 163 Mb/s |
| 10x | 10 s | 1.03 | 1.23 | 2.5 GB (0.3 tail) | 2.5 GB | 138 GB | 544 Mb/s | 297 Mb/s | 1.6 Gb/s |
| 10x | 60 s | 1.03 | 1.23 | 3.6 GB (1.6 tail) | 2.6 GB | 138 GB | 544 Mb/s | 297 Mb/s | 1.6 Gb/s |
| 100x | 10 s | 10.29 | 12.34 | 5.2 GB (3.1 tail) | 2.7 GB | 1,291 GB | 5.4 Gb/s | 3.0 Gb/s | 16.3 Gb/s |
| 100x | 60 s | 10.29 | 12.34 | 18.5 GB (15.8 tail) | 3.3 GB | 1,291 GB | 5.4 Gb/s | 3.0 Gb/s | 16.3 Gb/s |

So at today's load the limits are RAM (2.5 GB) and disk (~23 GB: the OS, 8.5 GB of DID state, an hour
of log). At 10x, disk (138 GB, mostly DID state) and the port run out on small VPSes. At 100x the
leader needs ~10 vCPUs at the sized rate and replicates 3 Gb/s, and the consumers need edge boxes.

| host | $/mo each | today | 10x | 100x |
|---|---|---|---|---|
| OVH VPS-1 | $5 | $18 | no: disk 138/40 GB, port 841 Mb/s/0.5 Gb/s | no: CPU 10.3/2, disk 1,291/40 GB, port 8.4 Gb/s/0.5 Gb/s |
| OVH VPS-2 | $8 | $30 | no: disk 138/75 GB, port 841 Mb/s/1 Gb/s | no: CPU 10.3/4, disk 1,291/75 GB, port 8.4 Gb/s/1 Gb/s |
| OVH VPS-3 | $12 | $42 | no: disk 138/100 GB | no: CPU 10.3/6, disk 1,291/100 GB, port 8.4 Gb/s/2 Gb/s |
| OVH VPS-4 | $23 | $75 | $121 | no: CPU 10.3/8, disk 1,291/200 GB, port 8.4 Gb/s/3 Gb/s |
| OVH ADVANCE-2 | $198 | $599 | $645 | no: disk 1,291/960 GB, port 5.4 Gb/s/3 Gb/s |
| Hetzner CX33 | $10 | $35 | no: disk 138/80 GB, port 841 Mb/s/1 Gb/s | no: CPU 10.3/4, disk 1,291/80 GB, port 8.4 Gb/s/1 Gb/s |
| Hetzner CAX21 (ARM) | $12 | $42 | no: disk 138/80 GB, port 841 Mb/s/1 Gb/s | no: CPU 10.3/4, disk 1,291/80 GB, port 8.4 Gb/s/1 Gb/s |
| Hetzner AX42 | $109 | $332 | $705, 3 edges | no: port 8.4 Gb/s/1 Gb/s |
| Hetzner AX42 + 10G | $157 | $476 | $1,035 | $3,385, 22 edges |
| AWS c7gd.large | $66 | $4,538 | no: disk 138/118 GB, port 841 Mb/s/0.94 Gb/s | no: CPU 10.3/2, disk 1,291/118 GB, port 8.4 Gb/s/0.94 Gb/s |
| AWS c7gd.xlarge | $132 | $4,737 | $30k | no: CPU 10.3/4, disk 1,291/237 GB, port 8.4 Gb/s/1.88 Gb/s |

The cheapest that fits, per load and flush (AWS left out):

| load | flush | HA + R2 | HA + S3 | single + R2 |
|---|---|---|---|---|
| today | 10 s | $26 (OVH VPS-1) | $37 (OVH VPS-1) | $17 (OVH VPS-1) |
| today | 30 s | $18 (OVH VPS-1) | $27 (OVH VPS-1) | $9 (OVH VPS-1) |
| today | 60 s | $18 (OVH VPS-1) | $25 (OVH VPS-1) | $9 (OVH VPS-1) |
| 10x | 10 s | $128 (OVH VPS-4) | $163 (OVH VPS-4) | $81 (OVH VPS-4) |
| 10x | 30 s | $121 (OVH VPS-4) | $154 (OVH VPS-4) | $74 (OVH VPS-4) |
| 10x | 60 s | $119 (OVH VPS-4) | $152 (OVH VPS-4) | $72 (OVH VPS-4) |
| 100x | 10 s | $3,392, 22 edges (Hetzner AX42 + 10G) | $3,663, 22 edges (Hetzner AX42 + 10G) | $3,078, 22 edges (Hetzner AX42 + 10G) |
| 100x | 30 s | $3,385, 22 edges (Hetzner AX42 + 10G) | $3,654, 22 edges (Hetzner AX42 + 10G) | $3,071, 22 edges (Hetzner AX42 + 10G) |
| 100x | 60 s | $3,383, 22 edges (Hetzner AX42 + 10G) | $3,651, 22 edges (Hetzner AX42 + 10G) | $3,069, 22 edges (Hetzner AX42 + 10G) |

Can three small VPSes run HA? At today's load, yes. Three OVH VPS-1s carry it at ~$18 a month, and
three VPS-2s at ~$30 with room for 2x growth and a misbehaving identity cache. Replication is ~30 Mb/s
out of the leader, under a tenth of a VPS-1's 500 Mb/s port. The risk with shared vCPUs is a noisy
neighbour. The quorum masks a stalled follower, but a stalled leader holds up every ack. Past 10x,
small VPSes run out of disk and port before CPU.

Replication bandwidth is free where the boxes are, except across AWS AZs:

| load | peer bytes/event | cluster total | TB/mo | OVH vRack | Hetzner private network | AWS cross-AZ |
|---|---|---|---|---|---|---|
| today | 14.2 KB | 40 Mb/s | 13 | $0 | $0 | $261 |
| 10x | 14.2 KB | 397 Mb/s | 131 | $0 | $0 | $2,613 |
| 100x | 14.2 KB | 4.0 Gb/s | 1,306 | $0 | $0 | $26k |

### Cost tuning

The knobs, in the order they matter (3-node HA on R2, requests only, no free tier, to show the slope):

| knob | today, 10 s | today, 60 s | 100x, 10 s | 100x, 60 s |
|---|---|---|---|---|
| default: 64 MiB segments, one state, peers | $14 | $4 | $46 | $36 |
| 8 MiB segments | $17 | $6 | $276 | $265 |
| 4 DID shards (the study's design) | $50 | $12 | $82 | $45 |
| 24 DID shards (the old cluster's count) | $285 | $72 | $317 | $104 |
| vlpds leases kept, TTL 30 s | $33 | $22 | $65 | $55 |
| vlpds leases kept, TTL 10 s | $70 | $60 | $102 | $92 |

- Flush interval. A flush costs ~8 Class A and ~12 Class B for the state plus its segments and the
  manifest, on top of ~1.3 Class B a second of polls. At 30 or 60 s the requests fit inside R2's free
  tier (1M Class A a month), and at 10 s they're ~$7 after it. A longer flush only costs the re-ingest
  window, and with the commitlog that only matters when two disks are lost. So 30-60 s, and on R2 the
  flush's own duration rules out 10 s anyway.
- Segment size. 8 MiB segments are fine today but cost ~$230 a month more at 100x. Use 64 MiB.
  Segments are cut at 64 MiB raw, which is 1.56x the PUTs of cutting at 64 MiB of zstd above one
  segment a flush (~$12 a month more on R2 at 100x). Cutting by compressed size would close that.
- DID shards. Each one costs its flush share and its polls. The leader keeps one SlateDB. Four (the
  study's design) would cost ~3.5x its requests at a 10 s flush, and the old cluster's 24 ~20x. One is
  enough until the state outgrows one SlateDB's compactor.
- SlateDB's polls. Its default 5 s compactor and worker polls sent 0.74 Class B a second more than
  the 30 s the state now uses (`--qlog-state-compactor-poll-ms`). Nothing waits on those polls here.
- Liveness. Keeping vlpds's node leases at TTL 10 s would add ~$56 a month on R2 and ~$62 on S3, more
  than everything else on the bucket. Peer heartbeats do it for free.
- Bucket retention. 72 h of log is ~$5 a month on R2 today, and 24 h is ~$1.6. That's the last lever
  between ~$18 and ~$15 on VPS-1s.
- Host size. Hosts are the bill now. Size them by RAM, disk and port, since CPU barely registers
  below 10x.

## Risks and what's left

- Log matching and truncation are where correctness bugs would live. They're covered by a randomized
  model test, seeded chaos in process and a process-level chaos harness with mutations that prove
  the checks bite ([measurements](quorum-measurements.md#how-it-s-checked)).
- VPS fsyncs may lie. `fsync` mode's claim to survive a DC power cut only holds on disks with
  power-loss protection, which a VPS doesn't promise. Measure each host.
- Shared vCPUs. The leader's ack path is one node's, so a noisy neighbour shows up as firehose
  latency. On a 2-vCPU VPS, userspace WireGuard alone can't carry a 10x leader's replication.
- The re-ingest after a lost quorum sends repeated commits. Consumers that don't check revs (the
  reference says they should) would double-count.
- The per-PDS replay window is assumed. A PDS that keeps less than a minute or so of history turns a
  lost quorum into a gap for that host.
- Hosts are priced from list pages and one third-party listing (Hetzner Cloud), and the RAM baseline
  is assumed.

Not built yet:

- Learners that start at F and read below it from the bucket, or a rate limit on catch-up, for hosts
  on a 0.5-1 Gb/s port.
- Parallel segment PUTs for 100x flushes on R2 (and cutting segments by compressed size, if the PUT
  count matters there).
- The sync API's repo endpoints on followers (the records are the leader's).
- Real-host runs of `page-cache` and `memory` modes, a cross-host run at 350/s away from the
  WireGuard ceiling, and a real leader kill across hosts.

## Inputs

| Input | Value | Label |
|---|---|---|
| Events/s, frame size, zstd ratio, peak hour | ~350 (480 peak hour), 5.3 KB, 1.56x | measured ([cost](cost.md#inputs)) |
| CPU per event | 65-70 µs one node on cores, 89-94 on threads, 142-156 a 3-node cluster | measured ([perf](perf.md)) |
| Leader's share of the cluster's CPU | half | assumed |
| The state's share of a flush (its L0, the checkpoint sealed and the one retired, compaction) | ~8 Class A, ~12 Class B, one SlateDB | measured (an hour at 350/s, less GC's one-key `DeleteObjects`, which R2 doesn't bill as Class A). The study used vlpds's ~4.4 A and ~9 B an L0 flush, four shards |
| SlateDB polls | ~1.26 GET/s, one SlateDB (manifest 10 s, compactor and worker 30 s) | measured. The study used vlpds's ~0.4 a shard |
| vlpds lease loop | 1.5 Class A + 1 Class B per node per second at TTL 10 s | measured in vlpds |
| Per-DID state | 121.8 B a DID (243.6 B right after an update) | measured (`tests/state_bulk.rs`) |
| Hosts on the network | 6,260 listed, 1,956 active, 89 bsky.network PDSes with 23.4M of 24.0M accounts | measured (listHosts, [reference notes](reference-notes.md)) |
| RAM baseline | 2 GB a node plus the ring | assumed (shadow run: 1.1 GB at 60 events/s) |
| Flush upload, detection and takeover | ~2 s, ~5 s | assumed |
| fsync | 0.03-0.1 ms datacenter NVMe, 0.5-2 ms VPS | assumed. One OVH VPS measured 0.57-0.68 ms p50 (fio, 4-64 KiB) and 0.83-0.93 ms in the commitlog |
| fsync, consumer NVMe | 2.7 ms p50, 5.9 ms p99 (fdatasync, one appending writer; 5.5 ms with three on one disk) | measured (`tests/qlog/fsync_probe.sh`) |
| RTT | 0.2 ms one DC, 3 ms one metro, 65 ms cross-region | assumed |
| R2 PUT | ~200 ms p50 (205-410 ms p50 over an hour of the quorum log) | measured in vlpds, then here |
| Segment size, read size, DID shards, group commit | 64 MiB raw (cut per flush), 8 MiB, 1, 2 ms | code (the study: 64 MiB of zstd, 4 shards) |
| R2 free tier | 1M Class A, 10M Class B, 10 GB-month a month | developers.cloudflare.com/r2/pricing (2026-10-06) |
| OVH VPS-1 to VPS-4 | $4.54, $8.50, $12.32, $23.37 | us.ovhcloud.com/vps (2026-10-06) |
| Hetzner Cloud CX33, CAX21, CPX22 | $9.99, $12.49, $22.99 (EUR 8.49, 10.49, 19.49) | costgoat.com listing (2026-09-05), not Hetzner's own page, which didn't render |
| AWS c7gd.large, .xlarge | $0.0907/h, $0.1814/h on demand | third-party listings (2026-10-06) |
| Everything else (S3, R2 rates, OVH ADVANCE-2, Hetzner AX42, AWS egress and cross-AZ) | as in [cost](cost.md#prices) | |

The hosts behind the tables:

| host | $/mo | vCPU | RAM | NVMe | port | source |
|---|---|---|---|---|---|---|
| OVH VPS-1 | $5 | 2 | 4 GB | 40 GB | 0.5 Gb/s | us.ovhcloud.com/vps (2026-10-06): 2 vCores, 4 GB, 40 GB NVMe, 500 Mb/s, unlimited traffic |
| OVH VPS-2 | $8 | 4 | 8 GB | 75 GB | 1 Gb/s | same page: 4 vCores, 8 GB, 75 GB NVMe, 1 Gb/s |
| OVH VPS-3 | $12 | 6 | 12 GB | 100 GB | 2 Gb/s | same page: 6 vCores, 12 GB, 100 GB NVMe, 2 Gb/s |
| OVH VPS-4 | $23 | 8 | 24 GB | 200 GB | 3 Gb/s | same page: 8 vCores, 24 GB, 200 GB NVMe, 3 Gb/s |
| OVH ADVANCE-2 | $198 | 16 | 64 GB | 960 GB | 3 Gb/s | cost.md price · 2 x 960 GB NVMe in RAID 1 assumed · 25 Gb/s vRack |
| Hetzner CX33 | $10 | 4 | 8 GB | 80 GB | 1 Gb/s | costgoat.com Hetzner listing (2026-09-05): EUR 8.49 / $9.99, EU only, listed as not orderable · port assumed |
| Hetzner CAX21 (ARM) | $12 | 4 | 8 GB | 80 GB | 1 Gb/s | same listing: EUR 10.49 / $12.49, EU only |
| Hetzner CPX22 | $23 | 2 | 4 GB | 80 GB | 1 Gb/s | same listing: EUR 19.49 / $22.99, every region |
| Hetzner AX42 | $109 | 16 | 64 GB | 1,920 GB | 1 Gb/s | cost.md price · hetzner.com AX matrix (2026-10-06): 2 x 1.92 TB datacenter NVMe |
| Hetzner AX42 + 10G | $157 | 16 | 64 GB | 1,920 GB | 10 Gb/s | AX42 + the 10G uplink addon ($48, 20 TB out included, then $1.20/TB), cost.md prices |
| AWS c7gd.large | $66 | 2 | 4 GB | 118 GB | 0.94 Gb/s | $0.0907/h on demand (search listings, 2026-10-06) · 118 GB instance NVMe · 0.94 Gb/s baseline (assumed) |
| AWS c7gd.xlarge | $132 | 4 | 8 GB | 237 GB | 1.88 Gb/s | $0.1814/h on demand · 237 GB instance NVMe · 1.88 Gb/s baseline (assumed) |

Like [cost](cost.md), the model sizes nodes for a minute burst at 2x the peak hour at 70% CPU, and lets
accounts and DID state grow with the event rate. That's an upper bound, and it's what makes disk the
limit at 100x (~850 GB of DID state per node).

## Reproducing it

```
python3 scripts/cost_model.py --quorum          # every cost table on this page
python3 scripts/cost_model.py --quorum --json   # the per-load, per-flush numbers
```
