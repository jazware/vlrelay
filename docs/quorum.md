# vlRelay: quorum replication (design and cost study)

The first pass at vlRelay's economics didn't pan out. Three nodes on OVH with R2 came to ~$3.4k a month, and ~$2.8k of that was bucket requests: segments sealed every 25 ms on every node, plus host bookkeeping in bucket objects every 2-5 s. A non-archival sync 1.1 relay on one node costs about $10-15 a month to run. This page is a design and cost study for a vlRelay that keeps its recent log in replicas instead of the bucket, and writes the bucket rarely and in bulk. The replication core (Phase 1) and the commitlog (Phase 2) are built and measured, see [Implementation notes](#implementation-notes). The flush and the relay's wiring aren't built yet. `scripts/cost_model.py --quorum` generates every table above the notes.

The idea is the one in vlpds's TODO ("Quorum in-memory durability"). A leader appends each event, replicates it to two other nodes and emits it once two of the three hold it. The bucket gets a flush every 10-60 s, and the host cursors ride in the same flush as the log they belong to. A relay suits this better than a PDS does, because the PDSes upstream are the source of truth. If the relay loses an unflushed tail, it asks each PDS to replay from its last flushed cursor. Losing a quorum costs a re-ingest and some time, and never a user's data. Jaz's call: the firehose holds every event back until a quorum has it, so consumers only see events that a takeover keeps.

Three nodes is the shape most likely to run in production, so most of this page is about it. A single node gets its own section near the end. It uses the same flush and re-ingest model, with a local NVMe write-ahead log as its durability point.

## Headline

Today's load (~350 events/s), 10 full-firehose consumers, R2 after its free tier:

| setup | flush | $/mo | where it goes |
|---|---|---|---|
| Old design, 3 nodes, OVH ADVANCE-2 + R2 | 25 ms linger | $3,131 | bucket requests $2,532, hosts $594 |
| Old design, one node, OVH ADVANCE-2 + R2 | 25 ms linger | $658 | bucket requests $455, hosts $198 |
| Quorum HA, 3 x OVH VPS-1, commitlog | 30 s | $22 | hosts $14, bucket requests $3, storage $5 |
| Quorum HA, 3 x OVH VPS-1, commitlog | 60 s | $18 | hosts $14, bucket requests $0, storage $5 |
| Quorum HA, 3 x OVH VPS-1, commitlog, 24 h of log in the bucket | 60 s | $15 | hosts $14, bucket requests $0, storage $2 |
| Quorum HA, 3 x Hetzner CAX21 (ARM), commitlog | 30 s | $46 | hosts $37, bucket $8 |
| Quorum HA, 3 x Hetzner AX42, commitlog | 30 s | $335 | hosts $327, bucket $8 |
| Quorum HA, 3 x OVH ADVANCE-2, commitlog | 30 s | $602 | hosts $594, bucket $8 |
| Quorum HA, 3 x AWS c7gd.large + S3, consumers outside AWS | 30 s | $4,544 | egress and cross-AZ $4,326, hosts $199, bucket $19 |
| Single node, OVH VPS-1, NVMe WAL + R2 (72 h of log) | 30 s | $13 | host $5, bucket requests $3, storage $5 |
| Single node, OVH VPS-1, NVMe WAL + R2 (72 h of log) | 60 s | $9 | host $5, bucket requests $0, storage $5 |
| Single node, OVH VPS-2, NVMe WAL + R2 for state and cursors only | 60 s | $9 | host $8, bucket $0 |
| Single node, OVH VPS-2, NVMe only |  | $8 | no bucket: losing the disk loses the cursors |
| Benchmark: a non-archival sync 1.1 relay on one node |  | $10-15 | Jaz's figure |

What the numbers say:

- The bucket stops mattering. Requests go from ~$2.5k a month on 3 nodes to ~$11 before R2's free tier and ~$0-3 after it, at a 30-60 s flush. What's left in the bucket bill is storage for 72 h of log (~$5 on R2).
- Hosts are the bill now. At today's load the leader needs ~0.1 vCPU at the sized rate, so the smallest NVMe VPS carries a node. Three OVH VPS-1 come to $14 a month, and with the log kept 24 h in the bucket the cluster lands at ~$15.
- So 3-node HA lands right at the top of the $10-15 benchmark only on 4 GB VPSes with a 60 s flush and a 24 h bucket window. On 8 GB VPSes (the size this page recommends for headroom) it's ~$30-34. On dedicated boxes it's whatever three boxes cost ($335 on AX42s, $602 on ADVANCE-2s).
- A single node gets under the benchmark: ~$9 on an OVH VPS-1 with R2 holding 72 h of log, ~$8 on a VPS-2 with no bucket at all.
- AWS is still decided by egress. Ten consumers outside AWS cost ~$4.1k a month in egress, and replication across three AZs adds ~$260.
- At 10x the cheapest HA is ~$124 (three VPS-4s, where disk and the port run out first). At 100x it's ~$3.4k, and $2.4k of that is edge boxes for the 10 consumers' 14.8 Gb/s. The bucket is ~$500 a month of storage there, still not requests.

## The shape

```
PDSes ──ws──> host owner (any node: verify) ──forward──> leader
                                                          │ check chain, apply, assign seq, append
                                                          ├──replicate──> follower A ─┐
                                                          └──replicate──> follower B ─┤ hold (memory, or memory + commitlog)
                                                          <───────── ack ─────────────┘
                                         commit at 2 of 3 ─> emit (leader), commit index ─> followers emit
                                         every N s: flush segments + state + cursors, manifest last ─> bucket
```

1. Each node keeps the websockets of its share of the PDSes and verifies their frames, as host shards do today (`assign-hosts/` fair shares). Verify is most of the CPU (~55 µs an event of the ~90), so it stays spread out.
2. The host owner forwards a verified event to the leader. That's today's forward path with one destination.
3. The leader runs `check_chain` against its DID state, applies the event, gives it the next seq and appends it to its log.
4. The leader sends the entry to both followers. A follower holds it (in memory, or also in a local commitlog, see [memory or commitlog](#memory-only-or-a-local-commitlog)) and acks.
5. Once two of the three nodes hold an entry (the leader counts itself), it's committed. The leader emits it to its subscribers and tells the followers the commit index on the next append. Followers emit up to it.
6. Every N seconds the leader flushes everything up to the commit index to the bucket: log segments, the DID state and the host cursors, with one manifest written last as the commit point.

One stream log with one leader is a choice. With 3 nodes and RF 3, every node is in every replica set anyway, so per-shard leaders would only spread the leader's work, which is ~2 cores at 100x's sized rate. One log means the seq is assigned in one place. That drops the k-way merge, the watermarks, the "slowest of three logs sets the pace" latency from perf iteration 6, and the `seqck/` renumbering checkpoints. Past ~100x the stream would split into P logs with a leader each and a merge as today, and nothing on this page needs that.

## 1. Seqs and the emit point

The leader assigns seqs. A seq is a plain counter, given at append time in log order, so it's dense within a leader's term and the same on every node without any renumbering. Each entry carries `(epoch, seq)`. A follower only accepts an entry whose predecessor it already holds with the same epoch, which is Raft's log matching rule.

The emit point is the commit. Nothing reaches a consumer, from the leader or a follower, until two nodes hold it. A follower that hasn't received an entry yet emits up to the lower of the commit index and its own head, so it trails the leader by half a round trip at most.

### The reservation

Every flush manifest carries a seq ceiling R, and the leader never emits a seq above the last committed manifest's R. Each flush sets R to the flushed seq F plus a headroom H. H is three flush intervals at the sized rate (~86k seqs today at a 30 s flush, ~8.6M at 100x). Normally that never binds. If the bucket stops taking flushes, emission stops at R, which is also the backpressure that keeps the unflushed tail bounded. A bigger H rides out a longer bucket outage and makes the jump after a lost quorum bigger, and seqs are cheap (2^53 is ~3,000 years at 100k events/s).

### Leader change with a quorum alive

The new leader takes the longest tail from a quorum (below) and carries on from its last seq + 1. Entries in that tail that weren't committed yet are committed under the new epoch with the seqs they already had. They were never emitted, so nobody has seen those seqs. Entries that lived only on the dead leader were never committed and never emitted either. Their host owners never got an `Appended`, so the forwarder resends them to the new leader (its existing 20 s retry), and they get new seqs. So no seq is emitted twice and no emitted seq is lost, and consumers see no gap.

### Leader change after a lost quorum

When fewer than two nodes hold an intact log, nobody can prove which seqs were emitted after the last flush. The new leader resumes at R + 1 of the last manifest, which is above anything any node could have emitted. Consumers see a jump forward and never a rewind:

| Consumer's cursor | What it gets |
|---|---|
| at or below F (the last flush) | backfill from the bucket up to F, then the stream from R + 1 |
| in (F, R] | the stream from R + 1 |
| above R | `FutureCursor` after the usual 2 s wait, as today |

Events in (F, last emitted] are gone from the relay. The re-ingest (section 2) brings them back from the PDSes under new seqs above R, so a consumer that saw them sees them again. A repeated commit has the same rev, and a sync 1.1 consumer already drops a commit whose rev isn't newer than the one it holds. `#identity` and `#account` events restate current state, so a repeat is harmless. That's the price of a lost quorum, and with the commitlog (below) it's only paid when two disks are gone.

## 2. The flush

### The manifest is the commit point

The bucket holds one object that recovery trusts, `qlog/manifest`, and it's written last with a CAS (`If-Match` on the ETag the leader wrote before). A flush at commit index F goes like this:

1. The leader picks F as the current commit index. Only committed entries are ever flushed, since an uncommitted one could be dropped by a takeover.
2. It seals the DID state at exactly F. The apply loop pauses at F while SlateDB's memtable is frozen (an in-memory swap), then carries on. The frozen memtable uploads as an L0 SST in the background.
3. It uploads the log up to F. Segments are cut at 64 MiB of zstd as the log fills, each a create-only PUT named by its epoch and seq range (`qlog/seg/{first_seq}-{epoch}`), and the flush PUTs the partial one.
4. It writes the manifest: epoch, F, R, the SlateDB checkpoint that holds the state at F, the host cursors as of F, the host registry rows that changed and the host counters. With ~6k hosts that's a few hundred KB.
5. The manifest's CAS is the commit. A crash before it leaves the previous manifest in charge, and the segments past it are ignored and deleted later.

A single object can't do it, because SlateDB writes its own SSTs and manifest. So the state is referenced by a SlateDB checkpoint, and our manifest names it. Keeping segments as their own objects also spreads the upload over the interval and lets backfill GET a segment by name.

### Cursors can't get ahead of the log

The danger is a cursor that says "PDS X is done through 1,000" in a flush whose log only holds X's events through 990. After a crash, the relay would ask X for 1,001 onwards and 991-1,000 would be gone. Cursors get into the log the same way events do:

- Each host owner sends its hosts' acked cursors to the leader about once a second, as a cursor entry. An acked cursor C for host X means every event of X up to C is committed, a duplicate or refused.
- The leader appends cursor entries to the log like events (replicated, never emitted). An event counted in C was committed before the host owner sent C, so its seq is below the cursor entry's.
- The manifest's cursors are the latest cursor entry for each host at or below F.

So every event a manifest's cursors count is at or below F, and in the flushed log. The same entries let a new leader resume a dead node's hosts from a cursor ~1 s old, from its own log, without the bucket.

The state follows the same rule. If the DID state got ahead of the log, a re-ingested commit would hit a rev the state already holds and be dropped as a duplicate, which loses it. That's why step 2 seals the state at exactly F. It needs SlateDB never to flush a memtable between our barriers (WAL off, an L0 size above what an interval holds). That's a thing to check in the prototype.

### What a flush costs

| load | flush | segment PUTs/s | Class A/s | Class B/s | R2 req $/mo, no free tier | R2 req $/mo | S3 req $/mo | bucket GB | R2 storage | S3 storage |
|---|---|---|---|---|---|---|---|---|---|---|
| today | 10 s | 0.12 | 1.99 | 5.23 | $28 | $20 | $32 | 323 | $5 | $7 |
| today | 30 s | 0.05 | 0.67 | 2.81 | $11 | $3 | $12 | 323 | $5 | $7 |
| today | 60 s | 0.03 | 0.35 | 2.20 | $6 | $0 | $7 | 323 | $5 | $7 |
| 10x | 10 s | 0.28 | 2.15 | 5.23 | $30 | $22 | $34 | 3,228 | $48 | $74 |
| 10x | 30 s | 0.21 | 0.83 | 2.81 | $13 | $5 | $14 | 3,228 | $48 | $74 |
| 10x | 60 s | 0.19 | 0.51 | 2.20 | $8 | $1 | $9 | 3,228 | $48 | $74 |
| 100x | 10 s | 1.87 | 3.74 | 5.23 | $49 | $41 | $55 | 32,281 | $484 | $742 |
| 100x | 30 s | 1.81 | 2.43 | 2.81 | $31 | $24 | $35 | 32,281 | $484 | $742 |
| 100x | 60 s | 1.79 | 2.10 | 2.20 | $27 | $20 | $30 | 32,281 | $484 | $742 |

A flush is a manifest PUT, the partial segment and one L0 flush per DID shard (with the compaction it causes, ~4.4 Class A and ~9 Class B, vlpds's measured numbers), plus SlateDB's own polls. Full 64 MiB segments add ~0.02 PUTs a second today and ~1.8 at 100x. Against the old design:

| load | old, 3 nodes | quorum HA, 30 s | old, one node | quorum single, 30 s |
|---|---|---|---|---|
| today | $2,500 | $11 | $442 | $11 |
| 10x | $2,765 | $13 | $477 | $13 |
| 100x | $2,799 | $31 | $481 | $31 |

Nothing in this bill follows host shards any more. The ~$1.3k of host bookkeeping (`hostck/` every 2 s, counters and the registry every 5 s, each a GET and a CAS per host shard, plus every node re-reading them) folds into one manifest per flush. Registry changes that must be seen at once (an admission, an operator ban) go into the log as entries and land in the next manifest.

### Recovering from the bucket

When the log can't be recovered from the replicas (section 3), the new leader:

1. Reads `qlog/manifest` and CASes it with epoch + 1, which fences any older leader's later flush.
2. Opens the DID state at the manifest's SlateDB checkpoint.
3. Salvages what it can. The longest committed prefix any reachable node holds past F is flushed first, with its seqs, since those are the seqs consumers saw.
4. Sets the next seq to R + 1.
5. Reconnects every host from the manifest's cursor (or the salvaged cursor entries), and the PDS replays the rest. Re-ingested events go through verify and `check_chain` against the state at F like any others.

The storm is the events between the cursors and the crash. Worst case, the crash lands just before a flush, so the window is the interval plus an upload (~2 s) plus detection and takeover (~5 s, both assumed):

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

Per PDS it's small. A bsky.network PDS (89 of them hold ~97.5% of accounts) replays ~140 events today at a 30 s flush, and ~14k at 100x. The reference PDS serves cursors from its sequencer table for much longer than a minute, so a 67 s window is well inside what it keeps. Catch-up is the backlog over the CPU headroom the nodes are sized with (2x the peak hour at 70%), so 6-23 s at any load. A small PDS that can't replay that far sends `OutdatedCursor`, and that host has a gap, as it would after any relay crash today.

## 3. Leadership, epochs and fencing

### What the existing leases give, and what they don't

| vlpds / vlRelay piece | Use here | Why |
|---|---|---|
| CAS'd assignment with an epoch (`assign/{shard}`: owner, epoch) | yes, as `qlog/leader` {epoch, leader, members} | one leader per epoch, decided by the bucket's linearizable CAS, no clocks |
| create-only segment PUTs and the fence object | yes, as create-only segments and the manifest CAS | a zombie leader can't overwrite a segment or commit a flush |
| self fail-stop | yes | a leader that can't reach a quorum can't commit anyway, and it steps down after a heartbeat timeout |
| refused-probe fast path | yes | a refused peer port means the process is gone, so takeover starts in ~0.3 s |
| planned handoff with a barrier | yes, for moving leadership | stop appending, wait for a follower to hold the whole log, CAS the epoch to it |
| node leases (`nodes/`) for liveness | no (optional in a prototype) | they cost a CAS and two LISTs per node every 2 s (~$57 a month on R2 at TTL 10 s, tuning table below) and take TTL + skew (12 s) to notice a hung box. Peer heartbeats on the private network notice it in ~1 s for free |
| `seq_floor` commit-wait, `set_revalidate`, lapse grace | no | seqs are counters, and safety comes from the quorum promise |
| host shard assignment judged by node leases (`assign-hosts/`) | moves into the log | the leader assigns host shards among the members it hears from, as a log entry, persisted in the manifest |

The leases fence the bucket. They don't fence memory. A zombie leader whose bucket writes all fail could still get a follower to ack an entry, and then emit it. So each follower keeps a promised epoch, the highest it has seen, and refuses entries from a lower one. That's the piece vlpds doesn't have.

### Takeover with a quorum alive

1. A follower misses heartbeats for ~1 s (100 ms apart), or finds the leader's peer port refused.
2. It CASes `qlog/leader` from epoch e to e + 1, naming itself. The lowest-named live follower tries first and the other waits 500 ms. If both try, one CAS wins and the other follows it.
3. The new leader asks every member to promise e + 1. Each one that does stops accepting epoch e and answers with its log tail: the last `(epoch, seq)` it holds and the entries past the last flush.
4. With promises from two of three (itself and one more), it adopts the tail with the highest last epoch and then the highest seq. Every committed entry is on two nodes, and any two nodes share at least one, so the adopted tail holds every committed entry.
5. It sends that tail to the followers, which truncate anything that differs, commits it under e + 1, and resumes. Host shards of a dead node move to the survivors, which resume from the cursor entries in the log.

That's Raft's election with the bucket in place of the vote count. The term is the epoch and the promise is the vote. What we skip from Raft is the randomized election timers and joint consensus, since the CAS picks the leader and membership only changes at a flush (below). What we still have to build is log matching and truncation, which is a few hundred lines and the place bugs would live.

Takeover after a crash is detection (~0.3-1 s), a CAS (~50-300 ms, R2's PUT latency), a promise round trip and handing the DID state over. In the commitlog shape every follower applies committed entries to its own local copy of the DID state, so the new leader has it at its head and starts at once. In the memory-only shape it opens the state from the last flush and replays the tail, which is ~0.2 s today and ~5-20 s at 100x with a 60 s flush (estimated, not measured).

### Losing the quorum

With fewer than two members answering, nobody can commit, so emission stops. Nothing acked is lost while it's stopped. Two rules decide what happens next:

- A minority never recovers on its own. A node that can't reach the others can't tell "they're dead" from "I'm cut off", and if the other two are alive and carrying on, a bucket recovery from the minority would throw away their tail. So it waits.
- Recovery is automatic once a quorum is reachable again, whatever its members still hold. A restarted node answers the promise with the tail it has (empty in memory-only, its commitlog otherwise). If two promise members have intact tails, it's the normal takeover. If not, it's the bucket recovery above, with the jump to R + 1.

An operator can force bucket recovery from a single survivor (`admin quorum recover --force`, a sketch). Even then nothing is reissued, since the seqs start above R.

### Failover, case by case

| Event | What happens | Emission pause | Lost |
|---|---|---|---|
| A follower dies | the other follower carries the quorum | none | nothing |
| The leader's process dies | refused probe, CAS, promise, adopt | ~0.5-1.5 s | nothing |
| The leader's box dies or hangs | heartbeat timeout, then as above | ~1.5-2.5 s | nothing |
| A partition cuts one node off | the side with two carries on (with a new leader if needed) and the lone node stops emitting | none or as a takeover | nothing |
| Two nodes' processes die (a bad deploy) | memory-only: bucket recovery and re-ingest. Commitlog: the page cache kept their writes, so a normal takeover | seconds | memory-only: the tail since F, re-ingested. Commitlog: nothing |
| Power loss on all three | memory-only: bucket recovery. Commitlog with fsync: normal takeover from any two disks | until two boxes are back | memory-only: the tail, re-ingested. Commitlog: nothing |
| Two disks or boxes gone for good | bucket recovery, jump to R + 1, re-ingest | until a quorum is reachable or forced | the tail since F, re-ingested |
| The bucket is down | emission carries on until R, then stops | after H (3 intervals) | nothing |
| One slow disk | the quorum waits for the faster two | none | nothing |

### Rejoin

A node that restarts reads its commitlog (if it has one) and greets the leader with its last `(epoch, seq)`. The leader answers with the first seq where their logs differ, the node truncates there and fetches the rest. Recent entries come from the leader's local segments and older ones from the bucket. It counts toward the quorum once it's within one batch of the head. In memory-only it starts empty and catches up from the last flush plus the leader's tail.

### Membership changes

The member set lives in `qlog/leader` and only changes at a flush barrier, so no unflushed entry ever depends on two configurations at once and joint consensus isn't needed:

1. The new box joins as a learner. It loads the state and log from the bucket, then follows the leader's stream without counting toward the quorum.
2. When it's caught up, the leader pauses commits, flushes to the commit index and CASes `qlog/leader` with epoch + 1 and the new member set.
3. Commits resume under the new set, and the removed node is told to stop.

The pause is one flush plus one CAS, under a second. Replacing a dead box is the same steps.

### Placement

Replicas go on distinct physical boxes, per Jaz. How far apart is a latency trade:

| Placement | Follower RTT | Quorum ack | What one event takes out |
|---|---|---|---|
| one DC, three dedicated boxes | ~0.2 ms | ~2-4 ms | a DC power or network event takes all three |
| one metro, three DCs (Hetzner FSN1, NBG1, HEL1 on a private network. OVH RBX, GRA, SBG on the vRack) | ~3 ms to the nearest | ~5-7 ms | one DC |
| two regions (OVH Vint Hill and Us-west) | ~65 ms | ~67 ms | one region |

A quorum only waits for the nearest follower, so a far third replica costs nothing on the ack path. Put the leader and one follower close and the third anywhere. With the commitlog fsynced, a single DC is also safe against a DC-wide power cut, which is the strongest reason to take it (below). On VPSes "distinct boxes" needs care. Hetzner Cloud's spread placement groups guarantee different hosts. OVH VPSes have no anti-affinity, so put them in three locations.

The full latency picture (all assumed device and network numbers):

| device | fsync | a WAL emit adds (group commit) |
|---|---|---|
| datacenter NVMe with power-loss protection (AX42, ADVANCE-2, EC2 instance store) | 0.03-0.1 ms | up to 2 ms + 0.03-0.1 ms |
| VPS virtual NVMe (OVH VPS, Hetzner Cloud) | 0.5-2 ms | up to 2 ms + 0.5-2 ms |
| consumer NVMe without power-loss protection | 2.7 ms p50, 5.9 p99 (measured: benchbox's 970 EVO Plus, one writer; 5.5 ms with three) | up to 2 ms + ~3 ms |

| follower placement | RTT | quorum ack, memory only | quorum ack, commitlog fsynced |
|---|---|---|---|
| one DC | 0.2 ms | 2.2 ms | 2.3 ms (dc) / 4.2 ms (vps) |
| one metro (FSN-NBG, RBX-GRA) | 3 ms | 5 ms | 5.1 ms (dc) / 7 ms (vps) |
| cross-region (Vint Hill-Us-west) | 65 ms | 67 ms | 67.1 ms (dc) / 69 ms (vps) |
| old design: 25 ms linger + an R2 PUT |  | ~225 ms p50 |  |

In one DC or one metro that's 30-100x faster than the old design's 25 ms linger plus an R2 PUT.

## Memory only, or a local commitlog

Scylla's commitlog is the model for the second shape: every replica appends each entry to a local file too. The question is what that buys over memory alone.

| | Quorum in memory only | Quorum + local commitlog, fsynced before the ack |
|---|---|---|
| Host cost | same hosts (every host on this page has local NVMe) | same, disk was already there |
| RAM | ring plus the unflushed tail: up to ~16 GB at 100x with a 60 s flush | ring only (~3 GB at 100x) |
| Ack latency | one RTT + group commit | the same plus an fsync on two of three nodes in parallel: +0.1 ms on datacenter NVMe, +0.5-2 ms on a VPS (assumed). Measured on benchbox: RTT + 1-1.7x the device's fsync + ~0.3 ms ([Phase 2](#tests-chaos-and-numbers-phase-2)) |
| One node lost | nothing lost | nothing lost |
| A bad deploy or a shared bug kills all three processes | bucket recovery, jump, re-ingest | nothing lost: the page cache survives a process crash, even before the fsync |
| Power loss on all three | bucket recovery, jump, re-ingest | nothing lost: any two disks hold every committed entry |
| Two disks lost | bucket recovery, jump, re-ingest | the same |
| Promotion | replay the tail into state from the last flush | instant: every node applies committed entries to its local state |
| Code | replication, promise, log matching | the same, plus a commitlog that's the single node's WAL anyway |
| Disk wear | none | ~0.16 TB a day today, 1.6 at 10x, 16 at 100x (below) |

| load | written a day (raw frames) | consumer 1 TB drive, 600 TBW | datacenter 1.92 TB, 1 DWPD for 5 years |
|---|---|---|---|
| today | 0.16 TB | 10.3 years | 59.9 years |
| 10x | 1.60 TB | 1.0 years | 6.0 years |
| 100x | 16.03 TB | 0.1 years | 0.6 years |

The recommendation is the commitlog, fsynced with group commit before the ack. The correlated failures a 3-box cluster actually sees are a bad deploy, a shared kernel or OOM bug, and a DC power event. Memory alone turns each of those into a re-ingest and a seq jump that consumers notice. The commitlog turns them into a normal takeover, and the only thing that still costs a re-ingest is losing two disks. It costs no extra money on these hosts, a millisecond or two on a VPS, and code the single node needs anyway. Since a quorum is two of three, one slow fsync is masked by the other two nodes.

A periodic fsync (Scylla's default, every 10 s) would keep the ack off the disk, but a power loss then loses up to the period on every node at once, which is the case the commitlog is there for. Batch it instead.

Two things to watch. First, wear: past ~10x a consumer drive wears out in about a year, so a cluster running at that rate wants datacenter drives (the AX42's are). Compressing the commitlog at zstd -1 would cut that by a third. Second, a VPS's fsync may be acknowledged by the hypervisor's cache without reaching a disk with power-loss protection. Measure it (`fio --fsync=1`) and don't count on VPS commitlogs surviving a DC power cut until it's checked.

## 4. Catch-up and backfill

Reads come from three tiers:

1. The ring in memory (512 MB, ~290 s today, ~3 s at 100x). In memory-only the ring must also hold the unflushed tail, so it's sized at the larger of 512 MB and the tail.
2. Local disk. Every node keeps the log on its NVMe for at least an hour past the flush. That's ~4 h on a VPS-1, ~11 h on a VPS-2 and the whole 72 h on an AX42 or ADVANCE-2 today. A consumer replaying an hour a day never touches the bucket.
3. The bucket, for anything older. Segments are 64 MiB of zstd now, and backfill reads them with 8 MiB ranged GETs. A 24 h replay today is ~12k GETs (~$0.004 on R2), where the old design's 4-event segments took ~7.2M GETs ($3).

The flush interval and the ring are only coupled in memory-only, where a tail that outgrows the ring has to be held anyway. With a commitlog, the tail is on disk and the ring is a cache.

## Cost tuning

The knobs, in the order they matter:

| knob | today, 10 s | today, 60 s | 100x, 10 s | 100x, 60 s |
|---|---|---|---|---|
| default: 64 MiB segments, 4 DID shards, peers | $28 | $6 | $49 | $27 |
| 8 MiB segments | $30 | $8 | $196 | $174 |
| 24 DID shards (the old cluster's count) | $158 | $34 | $179 | $55 |
| vlpds leases kept, TTL 30 s | $47 | $25 | $68 | $46 |
| vlpds leases kept, TTL 10 s | $85 | $62 | $105 | $83 |

- Flush interval. Each flush costs ~20 Class A and ~36 Class B with 4 DID shards. At 60 s that's inside R2's free tier (1M Class A a month), at 30 s it's ~$3 after it and at 10 s ~$20. A longer flush only costs the re-ingest window, and with the commitlog that window only matters when two disks are lost. So 30-60 s.
- Segment size. 8 MiB segments are fine today but cost ~$150 a month more at 100x. Use 64 MiB.
- DID shards. Each one costs an L0 flush per interval. The old cluster's 24 shards would cost 5x the requests of 4. The leader owns all of them, so 4 is plenty until the state outgrows one SlateDB's compactor.
- Liveness. Keeping vlpds's node leases at TTL 10 s adds ~$57 a month on R2 and ~$70 on S3, which is more than everything else on the bucket. Use peer heartbeats.
- Bucket retention. 72 h of log is ~$5 a month on R2 today, and 24 h is ~$1.6. That's the last lever between ~$18 and ~$15 on VPS-1s.
- Host size. Hosts are now the bill. Size them by RAM, disk and port, since CPU barely registers below 10x (section 6).

## 5. Single node

A single node uses the same flush and re-ingest model, with a local NVMe write-ahead log as its emit point.

- Every event is appended to the WAL, which is the local log segments themselves. Group commit fsyncs every ~2 ms (or per batch) and emits what it covered. (Built in Phase 2 as `qlog::commitlog`: there's no fixed interval, and each fsync covers whatever arrived during the one before.) On a VPS that's ~2-4 ms from apply to emit (assumed fsync of 0.5-2 ms), on datacenter NVMe ~2 ms. indigo's relay, for comparison, emits after a buffered write every 100 ms with no fsync (reference notes).
- The DID state and host cursors live on local disk (SlateDB over the local filesystem), with cursor entries in the WAL as in the cluster.
- The bucket becomes a lagging copy. Every N seconds the node uploads 64 MiB segments and a manifest with the cursors, the state checkpoint and R, exactly as the leader does.

What happens when:

| Event | Recovery | Consumers see |
|---|---|---|
| process crash, OOM, deploy restart | replay the WAL into state past the last checkpoint, reconnect hosts from the cursors in the WAL | a pause of a second or two, no gap, no jump |
| kernel crash or power loss | the same: everything emitted was fsynced | the same |
| the disk or the box is gone | bucket recovery: manifest, state at F, seqs from R + 1, re-ingest from the cursors (same storm table) | a jump to R + 1, repeated commits since F |
| no bucket and the disk is gone | start empty at the PDSes' live heads | a gap for everything they sent while it was down |

Is the bucket needed at all? Not to run. An NVMe-only node keeps its whole log locally (4-35 h on OVH VPSes today, 72 h on a dedicated box) and survives every crash short of losing the disk. The bucket is for that last case and for retention past the disk. Even a state-and-cursors-only bucket (no log) turns "losing the disk" from a gap into a re-ingest, and at a 60 s flush it fits inside R2's free tier:

| load | flush | 72 h log: A/s / B/s | R2 / S3 $/mo | state and cursors only: A/s / B/s | R2 / S3 $/mo |
|---|---|---|---|---|---|
| today | 10 s | 1.99 / 5.23 | $25 / $39 | 1.87 / 5.23 | $19 / $30 |
| today | 30 s | 0.67 / 2.81 | $8 / $19 | 0.62 / 2.81 | $3 / $11 |
| today | 60 s | 0.35 / 2.20 | $5 / $14 | 0.31 / 2.20 | $0 / $7 |
| 10x | 10 s | 2.15 / 5.23 | $71 / $108 | 1.87 / 5.23 | $21 / $33 |
| 10x | 30 s | 0.83 / 2.81 | $54 / $88 | 0.62 / 2.81 | $5 / $14 |
| 10x | 60 s | 0.51 / 2.20 | $50 / $83 | 0.31 / 2.20 | $2 / $10 |
| 100x | 10 s | 3.74 / 5.23 | $525 / $797 | 1.87 / 5.23 | $41 / $64 |
| 100x | 30 s | 2.43 / 2.81 | $508 / $777 | 0.62 / 2.81 | $25 / $45 |
| 100x | 60 s | 2.10 / 2.20 | $504 / $772 | 0.31 / 2.20 | $22 / $40 |

Against the benchmark: one OVH VPS-1 with R2 holding 72 h is ~$9 a month at a 60 s flush and ~$13 at 30 s. A VPS-2 (8 GB, more headroom) is ~$9 with a state-and-cursors bucket and ~$8 with none. All of those are inside or under $10-15. What dominates is the host, then 72 h of log storage. The fit across hosts:

| host | $/mo | today | 10x | 100x |
|---|---|---|---|---|
| OVH VPS-1 | $5 | $13 (4 h on disk) | no: disk 138/40 GB, port 1.6 Gb/s/0.5 Gb/s | no: CPU 12.3/2, disk 1,291/40 GB, port 16.3 Gb/s/0.5 Gb/s |
| OVH VPS-2 | $8 | $17 (11 h on disk) | no: disk 138/75 GB, port 1.6 Gb/s/1 Gb/s | no: CPU 12.3/4, disk 1,291/75 GB, port 16.3 Gb/s/1 Gb/s |
| OVH VPS-3 | $12 | $20 (16 h on disk) | no: disk 138/100 GB, port 1.6 Gb/s/2 Gb/s | no: CPU 12.3/6, disk 1,291/100 GB, port 16.3 Gb/s/2 Gb/s |
| OVH VPS-4 | $23 | $32 (35 h on disk) | $77 (2 h on disk) | no: CPU 12.3/8, disk 1,291/200 GB, port 16.3 Gb/s/3 Gb/s |
| Hetzner CX33 | $10 | $53 (12 h on disk) | no: disk 138/80 GB, port 1.6 Gb/s/1 Gb/s | no: CPU 12.3/4, disk 1,291/80 GB, port 16.3 Gb/s/1 Gb/s |
| Hetzner CAX21 (ARM) | $12 | $55 (12 h on disk) | no: disk 138/80 GB, port 1.6 Gb/s/1 Gb/s | no: CPU 12.3/4, disk 1,291/80 GB, port 16.3 Gb/s/1 Gb/s |
| Hetzner CPX22 | $23 | $66 (12 h on disk) | no: disk 138/80 GB, port 1.6 Gb/s/1 Gb/s | no: CPU 12.3/2, disk 1,291/80 GB, port 16.3 Gb/s/1 Gb/s |
| Hetzner AX42 | $109 | $117 (72 h on disk) | $490, 3 edges (36 h on disk) | no: port 16.3 Gb/s/1 Gb/s |
| Hetzner AX42 + 10G | $157 | $200 (72 h on disk) | $772 (36 h on disk) | $3,063, 22 edges (2 h on disk) |
| OVH ADVANCE-2 | $198 | $206 (72 h on disk) | $252 (17 h on disk) | no: disk 1,291/960 GB, port 16.3 Gb/s/3 Gb/s |

The 4 GB VPS-1 runs today's load at ~2.5 GB (assumed: the shadow run's 1.1 GB at 60 events/s, with the identity cache growing with the rate), which is tight. Hetzner's CX33 would be ~$10 for the host, but its 20 TB of included traffic runs out at four full-firehose consumers, and the listing shows the line as not orderable.

## 6. Hosts and placement

CPU is tiny at today's load. One node measured 65-70 µs an event on 8 physical cores and 89-94 µs on SMT threads (perf iterations 5 and 6), and a 3-node cluster 142-156 µs per event across all its threads. At today's ~350 events/s that's ~0.03 cores for one node. The leader carries more than a third of the cluster's work (every apply, both copies out), assumed here at half of it. Per node:

| load | flush | HA vCPUs | single vCPUs | HA RAM, memory only | HA RAM, commitlog | disk | HA public port | leader replication out | single public port |
|---|---|---|---|---|---|---|---|---|---|
| today | 10 s | 0.10 | 0.12 | 2.5 GB (0.0 tail) | 2.5 GB | 23 GB | 54 Mb/s | 30 Mb/s | 163 Mb/s |
| today | 60 s | 0.10 | 0.12 | 2.5 GB (0.2 tail) | 2.5 GB | 23 GB | 54 Mb/s | 30 Mb/s | 163 Mb/s |
| 10x | 10 s | 1.03 | 1.23 | 2.5 GB (0.3 tail) | 2.5 GB | 138 GB | 544 Mb/s | 297 Mb/s | 1.6 Gb/s |
| 10x | 60 s | 1.03 | 1.23 | 3.6 GB (1.6 tail) | 2.6 GB | 138 GB | 544 Mb/s | 297 Mb/s | 1.6 Gb/s |
| 100x | 10 s | 10.29 | 12.34 | 5.2 GB (3.1 tail) | 2.7 GB | 1,291 GB | 5.4 Gb/s | 3.0 Gb/s | 16.3 Gb/s |
| 100x | 60 s | 10.29 | 12.34 | 18.5 GB (15.8 tail) | 3.3 GB | 1,291 GB | 5.4 Gb/s | 3.0 Gb/s | 16.3 Gb/s |

So at today's load the limits are RAM (2.5 GB), disk (~23 GB: OS, 8.5 GB of DID state, an hour of log) and nothing else. At 10x, disk (138 GB, mostly DID state) and the port run out on small VPSes. At 100x the leader needs ~10 vCPUs at the sized rate and replicates 3 Gb/s, and the consumers need edge boxes.

| host | $/mo each | today | 10x | 100x |
|---|---|---|---|---|
| OVH VPS-1 | $5 | $22 | no: disk 138/40 GB, port 841 Mb/s/0.5 Gb/s | no: CPU 10.3/2, disk 1,291/40 GB, port 8.4 Gb/s/0.5 Gb/s |
| OVH VPS-2 | $8 | $34 | no: disk 138/75 GB, port 841 Mb/s/1 Gb/s | no: CPU 10.3/4, disk 1,291/75 GB, port 8.4 Gb/s/1 Gb/s |
| OVH VPS-3 | $12 | $45 | no: disk 138/100 GB | no: CPU 10.3/6, disk 1,291/100 GB, port 8.4 Gb/s/2 Gb/s |
| OVH VPS-4 | $23 | $78 | $124 | no: CPU 10.3/8, disk 1,291/200 GB, port 8.4 Gb/s/3 Gb/s |
| OVH ADVANCE-2 | $198 | $602 | $648 | no: disk 1,291/960 GB, port 5.4 Gb/s/3 Gb/s |
| Hetzner CX33 | $10 | $38 | no: disk 138/80 GB, port 841 Mb/s/1 Gb/s | no: CPU 10.3/4, disk 1,291/80 GB, port 8.4 Gb/s/1 Gb/s |
| Hetzner CAX21 (ARM) | $12 | $46 | no: disk 138/80 GB, port 841 Mb/s/1 Gb/s | no: CPU 10.3/4, disk 1,291/80 GB, port 8.4 Gb/s/1 Gb/s |
| Hetzner AX42 | $109 | $335 | $708, 3 edges | no: port 8.4 Gb/s/1 Gb/s |
| Hetzner AX42 + 10G | $157 | $479 | $1,038 | $3,377, 22 edges |
| AWS c7gd.large | $66 | $4,544 | no: disk 138/118 GB, port 841 Mb/s/0.94 Gb/s | no: CPU 10.3/2, disk 1,291/118 GB, port 8.4 Gb/s/0.94 Gb/s |
| AWS c7gd.xlarge | $132 | $4,743 | $30k | no: CPU 10.3/4, disk 1,291/237 GB, port 8.4 Gb/s/1.88 Gb/s |

| load | flush | HA + R2 | HA + S3 | single + R2 |
|---|---|---|---|---|
| today | 10 s | $39 (OVH VPS-1) | $53 (OVH VPS-1) | $30 (OVH VPS-1) |
| today | 30 s | $22 (OVH VPS-1) | $33 (OVH VPS-1) | $13 (OVH VPS-1) |
| today | 60 s | $18 (OVH VPS-1) | $28 (OVH VPS-1) | $9 (OVH VPS-1) |
| 10x | 10 s | $141 (OVH VPS-4) | $178 (OVH VPS-4) | $94 (OVH VPS-4) |
| 10x | 30 s | $124 (OVH VPS-4) | $158 (OVH VPS-4) | $77 (OVH VPS-4) |
| 10x | 60 s | $120 (OVH VPS-4) | $153 (OVH VPS-4) | $73 (OVH VPS-4) |
| 100x | 10 s | $3,394, 22 edges (Hetzner AX42 + 10G) | $3,666, 22 edges (Hetzner AX42 + 10G) | $3,080, 22 edges (Hetzner AX42 + 10G) |
| 100x | 30 s | $3,377, 22 edges (Hetzner AX42 + 10G) | $3,646, 22 edges (Hetzner AX42 + 10G) | $3,063, 22 edges (Hetzner AX42 + 10G) |
| 100x | 60 s | $3,373, 22 edges (Hetzner AX42 + 10G) | $3,641, 22 edges (Hetzner AX42 + 10G) | $3,059, 22 edges (Hetzner AX42 + 10G) |

Can three small VPSes run HA? At today's load, yes: three OVH VPS-1s carry it at ~$18-22 a month, and three VPS-2s at ~$30-34 with room for 2x growth and a misbehaving identity cache. Replication is ~30 Mb/s out of the leader, under a tenth of a VPS-1's 500 Mb/s port. The risk with shared vCPUs is a noisy neighbour. The quorum masks a stalled follower, but a stalled leader holds up every ack. Past 10x, small VPSes run out of disk and port before CPU.

Replication bandwidth is free where the boxes are:

| load | peer bytes/event | cluster total | TB/mo | OVH vRack | Hetzner private network | AWS cross-AZ |
|---|---|---|---|---|---|---|
| today | 14.2 KB | 40 Mb/s | 13 | $0 | $0 | $261 |
| 10x | 14.2 KB | 397 Mb/s | 131 | $0 | $0 | $2,613 |
| 100x | 14.2 KB | 4.0 Gb/s | 1,306 | $0 | $0 | $26k |

The prices behind all of this:

| host | $/mo | vCPU | RAM | NVMe | port | source |
|---|---|---|---|---|---|---|
| OVH VPS-1 | $5 | 2 | 4 GB | 40 GB | 0.5 Gb/s | us.ovhcloud.com/vps (2026-10-06): 2 vCores, 4 GB, 40 GB NVMe, 500 Mb/s, unlimited traffic |
| OVH VPS-2 | $8 | 4 | 8 GB | 75 GB | 1 Gb/s | same page: 4 vCores, 8 GB, 75 GB NVMe, 1 Gb/s |
| OVH VPS-3 | $12 | 6 | 12 GB | 100 GB | 2 Gb/s | same page: 6 vCores, 12 GB, 100 GB NVMe, 2 Gb/s |
| OVH VPS-4 | $23 | 8 | 24 GB | 200 GB | 3 Gb/s | same page: 8 vCores, 24 GB, 200 GB NVMe, 3 Gb/s |
| OVH ADVANCE-2 | $198 | 16 | 64 GB | 960 GB | 3 Gb/s | cost.md price; 2 x 960 GB NVMe in RAID 1 assumed; 25 Gb/s vRack |
| Hetzner CX33 | $10 | 4 | 8 GB | 80 GB | 1 Gb/s | costgoat.com Hetzner listing (2026-09-05): EUR 8.49 / $9.99, EU only, listed as not orderable; port assumed |
| Hetzner CAX21 (ARM) | $12 | 4 | 8 GB | 80 GB | 1 Gb/s | same listing: EUR 10.49 / $12.49, EU only |
| Hetzner CPX22 | $23 | 2 | 4 GB | 80 GB | 1 Gb/s | same listing: EUR 19.49 / $22.99, every region |
| Hetzner AX42 | $109 | 16 | 64 GB | 1,920 GB | 1 Gb/s | cost.md price; hetzner.com AX matrix (2026-10-06): 2 x 1.92 TB datacenter NVMe |
| Hetzner AX42 + 10G | $157 | 16 | 64 GB | 1,920 GB | 10 Gb/s | AX42 + the 10G uplink addon ($48, 20 TB out included, then $1.20/TB), cost.md prices |
| AWS c7gd.large | $66 | 2 | 4 GB | 118 GB | 0.94 Gb/s | $0.0907/h on demand (search listings, 2026-10-06); 118 GB instance NVMe; 0.94 Gb/s baseline (assumed) |
| AWS c7gd.xlarge | $132 | 4 | 8 GB | 237 GB | 1.88 Gb/s | $0.1814/h on demand; 237 GB instance NVMe; 1.88 Gb/s baseline (assumed) |

## 7. Risks and what to prototype first

Risks, biggest first:

- Log matching and truncation are where the correctness bugs would be. vlpds has none of this, since the bucket was its only log. It needs a model checker or a fault-injecting test harness before it carries traffic.
- Sealing the DID state at exactly F depends on SlateDB not flushing a memtable on its own between barriers. If it can, the state gets ahead of the log and a re-ingest drops commits.
- VPS fsyncs may lie. The commitlog's claim to survive a DC power cut only holds on disks with power-loss protection, which a VPS doesn't promise.
- Shared vCPUs. The leader's ack path is one node's, so a noisy neighbour shows up as firehose latency.
- The re-ingest after a lost quorum sends repeated commits. Consumers that don't check revs (the reference says they should) would double-count. It only happens when two disks are lost, with the commitlog.
- The per-PDS replay window is assumed. A PDS that keeps less than a minute or so of history turns a lost quorum into a gap for that host.
- Hosts on this page are priced from list pages and one third-party listing (Hetzner Cloud), and the RAM baseline is assumed. None of the quorum numbers are measured.

What to prototype, in order:

1. Replication with quorum ack and the promise round, in `bench/ha` style: leader, two followers, kill -9 and partition chaos, with a checker that every emitted seq survives every takeover. This is the part that has to be right.
2. The commitlog with group commit and fsync, then `fio --fsync=1` and the ack latency on the actual target hosts (an OVH VPS-2 and an AX42).
3. The flush: the state sealed at F, segments, cursor entries and the manifest CAS, with a crash injected between every step and recovery checked against the PDS fleet (fakepds) for no missing and no ahead-of-log cursors.
4. Bucket recovery with the jump to R + 1, the re-ingest storm at 10x against fakepds, and consumers checked for the jump and for repeated revs only.
5. Membership change at a flush barrier, and a box replacement under load.
6. Wire vlpds's `Store::counted` in and run an hour at today's rate to check the request table above against what the code really sends.

## Implementation notes

The build is under way on the `vlrelay-quorum` branch, in `src/qlog/` (the library), `src/bin/qlog.rs` (the node, load and checker tools) and `tests/qlog/` (the chaos harness). These notes record the decisions made while building it and what was measured.

### Serving: vlpds's firehose, one counted log

Consumers are served by vlpds's `Firehose` as it is. The quorum log feeds it as one followed log (`qlog`). Its watermark is the commit index, and it's handed only committed entries as `LogBatch`es, in seq order (`src/qlog/emit.rs`).

- The k-way merge doesn't fight the single-leader model. With one log it degenerates to "emit up to the watermark", which is exactly the hold-until-quorum rule. Every node runs the same merger over its own commit index, so a follower emits up to the commit index it has learned, and a promoted follower keeps the same firehose with no re-sourcing.
- The watermark source is `Source::Remote` (a plain `AtomicI64`), not `Source::Local`. vlpds's `Watermark` is time-based: when it's idle, `get()` advances to the clock, which is the right thing for `unix_micros << 8` keys and wrong for a counter. The name is historical, and nothing in the merger cares where the atomic is stored.
- Seqs are already dense, so no `Renumber`, `seqck/` checkpoints or `seq::dense` are involved. The frame carries the leader's seq from the moment it's appended (`wire::splice_seq`), so every node emits identical bytes for a seq.
- The one vlpds hook is `firehose::Options::start_floor`. `None` keeps vlpds's behaviour (the stream starts at the clock). `Some(f)` starts a counted stream above `f`. In counted mode, a cursor past the head gets the renumbered stream's 2 s grace instead of a clock comparison, and the clock-based emit-delay metric is skipped. It's tested in vlpds (`counted_stream_starts_at_its_floor`), and `Firehose` is otherwise untouched.
- Why the rule is airtight: the emitter reads only `(emitted, commit]`. It sends the batch before it stores the watermark (the merger reads the watermark, then drains, so it never holds a watermark past events it hasn't been given). The commit index only moves when a quorum's acks cover a current-epoch entry. `Log::truncate_after` and `Log::reset` panic rather than touch anything at or below the commit index.
- What carries over unchanged: the subscriber registry, kick, labelled per-consumer series, `ConsumerTooSlow`, per-IP caps, the takedown filter hook and the ring. Backfill also carries over, provided Phase 3 writes its 64 MiB segments in vlpds's segment format under `log/qlog/` with the seq as the key. The existing `backfill::Reader`, `retention::retained_floor` and `OutdatedCursor` then work as they are. Until then, a cursor below a node's ring floor gets `OutdatedCursor`, because the firehose has no store.
- Each node creates its firehose at its first emission, with the floor at where that emission starts. That's 0 at genesis, or the leader's base for a memory-only node that restarted and was reset to it.

The alternative, a thinner serving path of our own, would have needed the subscriber machinery rebuilt for no gain.

### The replication core (Phase 1)

What's built (`src/qlog/`):

- `log.rs`: one node's log in memory. Entries are `(epoch, seq, frame)`, and seqs are dense above a base. It implements Raft's log matching. An entry that differs from the one held at its seq truncates the log from there, while a matching entry is kept, so a stale, shorter append never drops later entries. A prev at or below the commit index always matches. It also does trimming and resets.
- `node.rs`: the leader assigns seqs at append and replicates to each follower over one connection per follower. There's one request in flight, batched up to 4 MiB, and a heartbeat every 100 ms when idle. The leader commits at a quorum (itself counted only once its own `Durability::persist` returns) and answers the submitter at commit. Followers learn the commit index on the next append, and the leader sends one at once when the commit index moves.
- Takeover: CAS `qlog/leader` to epoch + 1, then the promise round, then adopting the longest tail. The details are the next list.
- `Durability` is the Phase 2 seam. A follower acks after it, and the leader counts itself after it. `MemoryOnly` returns at once.

Choices the design left open, and what was picked:

- **Re-tagging the adopted tail.** The new leader re-tags every entry above its commit index with its own epoch before it leads. That's Raft's rule that only a current-term entry commits by count, done without a no-op entry (a no-op would burn a seq that consumers would see as a hole). Seqs and bytes are unchanged, and followers holding the old tag replace it through the matching rule.
- **The longest tail is fetched from its holder.** Promise replies carry only `(last_epoch, last_seq)`, and the new leader pulls what it lacks with `Fetch` (4 MiB a request). The holder has promised the epoch, so its log can't change under the fetch.
- **Restarted memory-only nodes are "not intact".** A node that starts while `qlog/leader` exists may have acked entries it no longer holds. It still promises, but it doesn't count toward a takeover's quorum until its log has matched the leader's last seq as of the first append it hears after starting. Without this rule, a quorum of one real log and one emptied one could elect a leader that lacks an emitted entry. With it, losing the leader while the only other intact node is down stops emission. That's the design's "lost quorum", which is bucket recovery in Phase 4, and the commitlog makes it rare in Phase 2.
- **A minority never takes over.** Before its CAS, a candidate needs pongs from enough peers to make a quorum. Without that, a cut-off node would CAS a higher epoch and unseat the majority's leader when the partition healed. The pings, like the promise round, wait only until they have a quorum, not for a member that's down or blackholed.
- **Detection.** The leader dials its followers, so a follower sees its inbound replication connection close the moment the leader's process dies, and probes the leader's port at once. A refused or reset connection starts the takeover. Otherwise a silence of 300 ms triggers one probe, and 1 s of silence triggers the takeover. The lowest-ranked follower tries first, and the others wait 500 ms per rank.
- **Liveness.** It comes from peer heartbeats only: no node leases and no bucket polling. The bucket sees one GET and one CAS per takeover.
- **Memory bounds.** Each node keeps 512 MiB of committed, emitted log (`--retain-mb`) on top of the firehose's 512 MiB ring. A follower that falls further behind than that is reset to the leader's base, and its stream jumps (counted as `emit_gaps`; a node that restarted empty just starts its stream at the base).

### Tests and chaos (Phase 1)

In-tree (`cargo test --lib qlog`): log matching, truncation and trimming, a randomized follower-versus-leader model (200 seeds of appends, stale and reordered deliveries, takeovers with truncation), the wire format, and the checker itself. Six integration tests run three nodes on real TCP, each node on its own runtime, so a crash takes its memory and sockets at once:

- commit and emit on every node;
- an isolated leader acks and emits nothing, and its lone entry is never committed;
- five leader kills under load, each victim restarted empty;
- a restarted node can't make a quorum before it catches up;
- six partitions;
- seeded random kills, restarts, partitions and heals (`QLOG_SEED`). Seeds 1-20 all pass with 0 violations: 2.34M seqs across 54 kills and 55 isolations, with every acked seq emitted.

Two mutations confirm the tests bite. With the tail adoption skipped, the random chaos test reports 72,648 violations ("seq 60081 emitted with two contents"). With commit at one ack instead of two, three tests fail.

The harness (`tests/qlog/chaos.sh`, `just qlog-chaos`) runs three `qlog node` processes on a local MinIO (`qlog/leader` only), each restarted by a supervisor 1 s after it exits. Load comes from `qlog load`, frames are ~5.3 KB (the network's mean), and `qlog check` consumes every node's `subscribeRepos` from cursor 0. Partitions blackhole every peer route through `tests/chaos/proxy.py`, one route per direction. The checker asserts:

- no seq is emitted with two contents across all nodes and incarnations (emitted, lost and reissued);
- every consumer's stream is strictly increasing and dense (no repeat, no step back, no hole);
- every acked seq is emitted with the content it was acked with;
- every consumer ends at the same commit index.

Results on benchbox (Ryzen 395, loopback), with the final build:

| run | faults | distinct seqs | violations |
|---|---|---|---|
| baseline, 350/s, 60 s | none | 21,001 | 0 |
| baseline, 3,500/s, 60 s | none | 210,017 | 0 |
| baseline, 35,000/s, 30 s | none | 1,050,175 | 0 |
| kill-leader, 350/s | 7 x kill -9 | 35,001 | 0 |
| kill-leader, 3,500/s | 7 x kill -9 | 350,017 | 0 |
| kill-follower, 3,500/s | 7 x kill -9 | 350,017 | 0 |
| partition-leader, 350/s | 5 x isolated 5 s | 35,001 | 0 |
| partition-follower, 3,500/s | 5 x isolated 5 s | 350,017 | 0 |
| pause-leader, 350/s | 4 x SIGSTOP 3 s | 28,001 | 0 |
| mixed, 3,500/s, 180 s | 4 kill -9, 6 partitions, 3 SIGSTOP | 630,017 | 0 |

That's 3,059,264 seqs across 25 kill -9s, 16 partitions and 7 SIGSTOPs, seen 9.18M times by the three consumers. Every acked seq was emitted, and nothing was lost, duplicated or reissued.

What was measured:

| | 350/s (today) | 3,500/s (10x) | 35,000/s (100x) |
|---|---|---|---|
| leader append to quorum commit | p50 0.04 ms, p99 0.10 | p50 0.07, p99 0.17 | p50 0.21, p99 0.56 |
| submit to ack, at the host owner | p50 0.10 ms, p99 0.22 | p50 0.17, p99 0.38 | p50 0.56, p99 1.31 |
| submit to the first consumer's receipt | p50 1.23 ms, p99 2.29 | p50 1.33, p99 2.45 | p50 1.98, p99 3.48 |
| CPU, leader / follower (cores) | 0.034 / 0.019 | 0.054 / 0.030 | 0.20 / 0.095 |
| RSS a node | 254 MB | 1.2 GB | 1.3 GB |

| fault | emission pause (fault to the next new seq at any consumer) |
|---|---|
| kill -9 the leader | 15-40 ms at 350/s (n=7), 43-81 ms at 3,500/s (n=7) |
| kill -9 a follower | none over 15 ms |
| partition the leader | 984-1,015 ms (n=5) |
| partition a follower | none over 15 ms |
| SIGSTOP the leader | 990-1,016 ms (n=4) |

What the numbers say against the study's assumptions:

- **The emit path is dominated by the firehose's 2 ms merger tick**, not by replication. The quorum ack on loopback is 0.1-0.2 ms. Consumers see an event 1.2-2.5 ms after it's submitted, which is ~1 ms on average and 2 ms at most from the tick. Real placements add the RTT (0.2 ms in one DC, 3 ms in a metro), as §3's table assumed. A waker on commit would cut the tick if it ever matters.
- **Takeover is faster than assumed when the process dies.** The study assumed 0.5-1.5 s for a dead leader process. Measured, it's 15-80 ms: the follower's replication connection closes, the refused probe confirms it, and the pre-vote ping, the CAS on a local MinIO, the promise round and the first commit follow. On R2 the CAS alone is ~200 ms, so expect ~0.25-0.3 s there. A hung or partitioned leader takes the 1 s heartbeat timeout, as assumed. That's within the study's 1.5-2.5 s, and the host owner's resend needed its timeout cut to 1 s to meet it.
- **The replication CPU is tiny.** At 100x the leader spends 0.2 cores and each follower 0.1 on replication, emission and one consumer each, ~6 µs an event on the leader. That's a small part of the ~10 vCPUs the study sizes the leader at for 100x, which are verify and apply. The sizing stands, and replication isn't what decides the host.
- **RAM in memory-only mode is ~2x the log window.** The node keeps its retained log (512 MiB here) and the firehose ring (512 MiB) as separate copies of the same frames, so a node sits at ~1.2-1.6 GB at 10x and above (2.1 GB at its peak during the mixed run). The study's 2 GB baseline plus a ring should count the ring twice in memory-only mode, or the log should share the ring's bytes. With the commitlog (Phase 2), the retained log can shrink to what replication needs.
- **Memory-only can't survive two losses, by design.** A takeover needs two intact logs. A restarted node that hasn't caught up doesn't count, so losing the leader while the other node is still catching up stops emission rather than risk losing an emitted seq. That's the design's lost-quorum case. The commitlog (Phase 2) turns a restarted node into an intact one at once.

Known gaps, for later phases:

- The relay itself doesn't use the quorum log yet. The forward path, verify and state still run on the old node log. Wiring `cluster/forward.rs` to `Client::submit` (with its resend) and the DID state apply to committed entries is the integration step.
- A follower more than `--retain-mb` behind is reset to the leader's base, and its stream jumps. Phase 3's bucket segments let it catch up instead. (Phase 2: with a commitlog it's served from the leader's disk first, and only a follower behind the leader's disk retention is reset.)
- The pre-vote and the probe treat any I/O error from the leader as "dead". A flaky link can cost an unneeded takeover (availability, not safety).
- Nothing is flushed, and a lost quorum is unrecoverable until Phase 4. Until then it stops emission rather than reissuing seqs.
- Membership is fixed (`--peer`).

### The commitlog (Phase 2)

`src/qlog/commitlog.rs` is the local log behind `node::Durability`, and the single node's WAL. It knows nothing about replication: a caller stages ops and waits for them to be durable.

- **What's written.** The in-memory `Log` journals every change it makes: appends, truncations, resets, and restamps (written as a truncation plus the entries again, so the last record of a seq is always its current state). The node stages the journal under its lock, so the disk sees changes in exactly the order memory made them. Promises are journaled the same way, with whom they went to. The commit index rides along after each batch as a lower bound. Safety never needs it, but a restarted node can emit at once.
- **Record format.** `len u32 | crc32 u32 | type u8 | payload`, little endian, the CRC over type and payload. Types: append `(epoch, seq, frame)`, truncate-after, reset, promise `(epoch, leader)`, commit, and a segment header `(magic, base epoch, base seq, promise in force)`. That's 25 bytes over each ~5.3 KB frame (0.5%).
- **Group commit.** One writer thread drains everything staged, writes it with one `write`, `fdatasync`s, then publishes the batch's ticket. There's no linger: a batch is whatever arrived during the previous fsync. A follower acks, the leader counts itself, a candidate counts its own vote and anyone answers a promise only once their ticket is durable. Raising the promise on an append from a newer leader is journaled too, and that append's ack waits for it. A failed write or fsync poisons the log: nothing more is acked, and the binary aborts (after a failed fsync the page cache can't be trusted).
- **Truncation and torn writes.** Truncations are records, not rewrites, so a crash can't half-apply one. Recovery replays the segments in order and stops the last segment at its first bad record (short, too long, a CRC mismatch or zeros). It truncates the file there and fsyncs it. Everything past the last fsync was never acked, so losing it is fine. A bad record in any segment but the last is corruption, and the node refuses to start.
- **Segments and trimming.** A segment rolls at 64 MiB (`--segment-mb`). The new one starts at the commit index and repeats the uncommitted tail, so any later truncation lands inside it (nothing at or below the commit index is ever truncated) and older segments can be deleted on their own. A roll fsyncs the old segment, creates the new one and fsyncs the directory. Old segments go once they're wholly below the trim floor and the log is over `--disk-retain-mb` (4 GiB). The floor is `min(emitted, commit)` today, and Phase 3 lowers it to the flush point R.
- **Recovery.** The node opens the commitlog before it starts and gets back its log, the promise and whom it went to, and the commit index. It loads the newest `--retain-mb` (plus anything uncommitted) into memory and indexes the rest on disk at 32 bytes an entry. A node that recovered a commitlog is intact at once, with no catch-up gate. Only a node whose commitlog is new or wiped, while `qlog/leader` exists, still starts "not intact". Its greeting is the first append's reply: its last seq and commit index, and the leader backs up to the commit index on a mismatch.
- **Reads from disk.** The leader serves a follower that's behind its in-memory window from its commitlog, and a candidate's fetch the same way. These are committed entries, which never change, so the read takes the lock only to find them. Only a follower behind the leader's disk retention is still reset.
- **Each node emits only what its own disk holds.** Emission stops at `min(commit, local durable)`. A follower can learn a commit index ahead of its own fsync, and without this rule a power cut could bring it back behind what its consumers had seen. With it, a consumer reconnects to a restarted node and carries on from its cursor with no step back. In every chaos run below, consumers resumed across restarts with no skip notice.
- **Backpressure.** The leader takes no new submits while 256 MiB is uncommitted (`max_pending_bytes`). It waits up to 500 ms (under the submitter's 1 s timeout) and then answers "busy". Without this, a quorum slower than its submitters grows the leader's memory without bound. The first ceiling run on benchbox's shared disk reached 52 GB RSS before the kernel killed two nodes.
- **RAM.** `--retain-mb` defaults to 64 with a commitlog (512 without). The firehose ring copies frames into its own websocket-framed batches (`MergedBatch`), so sharing bytes between the log and the ring would mean changing vlpds. With a 64 MiB log that copy is small, and it wasn't worth doing.
- **The single node's WAL.** `CommitLog::open`, `stage`, `wait`, `note_commit`, `set_floor` and `read` are the whole API. A single node journals its log the same way, waits before it emits, and recovers the same way. Host cursors would be one more record type.

### Tests, chaos and numbers (Phase 2)

In-tree (`cargo test --lib qlog`, 20 tests): commitlog replay across truncations, restamps, promises and rollovers. A torn tail at three cut points, with garbage that looks like a record, is cut and appended past. Old segments go below the floor and the head stops reading. Integration tests run three nodes with commitlogs:

- restarted nodes are intact at once and keep their logs;
- kill -9 two nodes at once, then all three, six rounds;
- power cuts on all three nodes at once under load, with a 3 ms fsync. Each node loses a random part of what it wrote since its last fsync and gets a torn record;
- seeded random kills, power cuts (any number of nodes), restarts, partitions and heals;
- a follower down for longer than the leader's 32 KiB in-memory window catches up from the leader's disk with no reset.

Seeds 1-10 of the power-cut test, the durable random chaos and Phase 1's random chaos all pass with 0 violations. **The mutation:** with a follower acking before its fsync, the power-cut test fails on 4 of 5 seeds, with 84-1,440 violations ("seq N emitted with two contents": two followers ack, the commit index moves past the leader's own fsync, and all three disks lose it).

The process harness (`tests/qlog/chaos.sh`) runs with commitlogs by default (`COMMITLOG=0` for memory only). It adds `kill-two`, `kill-all`, `power-cut-leader` and `power-cut-all` (SIGUSR1: the commitlog loses a random part of its unsynced tail plus a torn record, then SIGKILL), and `mixed-durable`. Results on benchbox, all three nodes' commitlogs on one 970 EVO Plus:

| run | faults | distinct seqs | violations |
|---|---|---|---|
| kill-leader, 3,500/s | 7 x kill -9 | 350,402 | 0 |
| kill-follower, 3,500/s | 7 x kill -9 | 350,017 | 0 |
| kill-two, 3,500/s | 7 x kill -9 of the leader and a follower at once | 350,156 | 0 |
| kill-all, 3,500/s | 7 x kill -9 of all three at once | 350,158 | 0 |
| power-cut-leader, 3,500/s | 7 | 350,349 | 0 |
| power-cut-all, 350/s | 7 x all three at once | 35,006 | 0 |
| power-cut-all, 3,500/s | 7 x all three at once | 350,034 | 0 |
| partition-leader, 350/s | 5 x isolated 5 s | 35,010 | 0 |
| partition-follower, 3,500/s | 5 x isolated 5 s | 350,017 | 0 |
| pause-leader, 350/s | 4 x SIGSTOP 3 s | 28,011 | 0 |
| mixed, 3,500/s, 180 s | 6 kill -9, 4 partitions, 3 SIGSTOP | 630,627 | 0 |
| mixed-durable, 3,500/s, 240 s | 4 kill-all, 4 kill-two, 3 kill -9, 4 power cuts (1 of all three), 3 partitions, 2 SIGSTOP | 840,644 | 0 |

That's 4.0M seqs across 78 node kill -9s and 55 node power cuts, every acked seq emitted, nothing lost, duplicated or reissued. Recovery cut torn tails of 4-36 bytes. Every consumer resumed from its cursor with no skip notice and no hole. The measurement runs below add 10 more whole-cluster kills and power cuts and 21 leader kills and power cuts, also clean.

Measured on benchbox (Ryzen 395, loopback). The disk is one consumer NVMe without power-loss protection, which three nodes share. `fsync_probe.sh` gives it 2.7 ms p50 for one writer and 5.5 ms for three. tmpfs stands in for a disk whose fsync costs nothing, an upper bound for datacenter NVMe with power-loss protection. "tmpfs + N ms" sleeps N ms before each fsync to emulate a device. Loads are as in Phase 1 (~5.3 KB frames, 5 ms submit ticks):

| submit to quorum ack, p50 / p99 | 350/s | 3,500/s | 35,000/s |
|---|---|---|---|
| memory only (same build) | 0.11 / 0.23 ms | 0.20 / 0.38 | 0.55 / 1.40 |
| commitlog on tmpfs | 0.14 / 0.69 | 0.23 / 0.78 | 0.88 / 1.79 |
| commitlog, three nodes on one consumer NVMe (fsync 5.2-5.6 ms) | 8.5 / 21.7 | 9.3 / 17.8 | saturated: ~25,000/s committed (20,000/s: 27 / 65 ms, fsync 10 ms) |

| emulated device fsync, 3,500/s | 0.1 ms | 0.5 ms | 1 ms | 2 ms | 2.7 ms (this NVMe, one node a disk) |
|---|---|---|---|---|---|
| submit to quorum ack, p50 / p99 | 0.37 / 0.91 | 0.76 / 1.23 | 1.26 / 1.92 | 2.27 / 3.12 | 2.96 / 3.17 |
| submit to the first consumer, p50 / p99 | 1.52 / 2.68 | 1.96 / 3.13 | 2.42 / 3.54 | 3.44 / 5.10 | 4.14 / 5.28 |

At 35,000/s with a 2 ms fsync the ack is 2.87 / 4.39 ms, with 175 events a group commit and one fsync per 5 ms tick.

| | 350/s | 3,500/s | 35,000/s |
|---|---|---|---|
| CPU leader / follower, memory only (cores) | 0.035 / 0.019 | 0.059 / 0.030 | 0.20 / 0.10 |
| CPU leader / follower, commitlog | 0.033-0.044 / 0.022-0.028 | 0.059-0.072 / 0.036-0.045 | 0.29 / 0.17 |
| RSS a node, memory only (512 MiB log + 512 MiB ring) | 255 MB | 1.0 GB | 1.2-1.7 GB |
| RSS a node, commitlog (64 MiB log + 512 MiB ring) | 220 MB | 0.57-0.60 GB | 0.73-0.80 GB |
| events a group commit, p50 | 2 | 17-18 | 175 |

Throughput ceilings (200,000/s offered for 30 s, 5.3 KB frames, three nodes):

| | committed | notes |
|---|---|---|
| memory only | 200,000/s (1 GB/s a node) | ack p50 3.0 ms; leader 0.99 cores, followers 0.46 |
| commitlog on tmpfs | 200,000/s | ack p50 5.8 ms, p99 62; leader 1.78 cores, followers 0.91 |
| commitlog, three nodes on one consumer NVMe | ~25,000/s (~130 MB/s a node fsynced, ~400 MB/s on the drive) | fsync p50 13-15 ms, p99 70-240; backpressure held the leader at 3.6 GB RSS |

The tmpfs ceiling run kept only 256 MiB of commitlog on each node, which is 0.25 s at 1 GB/s. One follower fell further behind than that three times and was reset, a counted gap in its stream (`emit_gaps`, which the checker reports as skipped seqs). That's the reset Phase 1 documents, now reached only past the leader's disk retention. Every acked seq was emitted with its content. Phase 3's catch-up from the bucket removes it, and keeping segments a live follower still needs would too.

| fault, 3,500/s, commitlog | emission pause (fault to the next new seq at any consumer) |
|---|---|
| kill -9 the leader | 55 ms median, 61 max on tmpfs; 72 / 88 on the NVMe (Phase 1, memory only: 43-81) |
| kill -9 a follower | 17 ms median, 38 max |
| power cut of the leader | 79 ms median, 579 max |
| kill -9 the leader and a follower | 1.5-2.3 s |
| kill -9 all three | 2.36 s median, 2.70 max |
| power cut of all three | 2.47 s median, 2.72 max |

The two-node and three-node pauses are the supervisor's 1 s restart, recovery, and then the 1 s election timeout, since nobody is leading. A real deploy restarts faster or slower than that, and the rest is ~0.4 s. Recovery reads and checksums every retained segment: 0.65 s for 1.8 GB (24 segments), ~0.35 s a GB.

What the numbers say against the study:

- **The commitlog ack is about RTT + fsync + 0.3 ms** when group commits line up with arrivals, as they do under the 5 ms ticks here. When fsyncs run back to back, an event also waits out part of the one in flight. On the shared NVMe (5.5 ms fsync) the ack was 9.3 ms, ~1.7 fsyncs. The study's "+0.5-2 ms on a VPS" holds if VPS fsyncs are 0.5-2 ms, which is the thing to measure. Budget ack ≈ RTT + 1-1.7x fsync.
- **Don't put two members on one disk.** Three nodes on one consumer NVMe fsync at 5.5 ms instead of 2.7, and they also share its write bandwidth. That's the shared-disk case here, not a deployment, but the same holds for VPSes that might land on one host's disk (another reason for spread placement).
- **Disk bandwidth with fsync, not CPU, sets the ceiling.** On tmpfs, three nodes sustain 200,000/s (1 GB/s a node) at 1.75 cores on the leader, so the commitlog code isn't the limit. On a disk, one node a disk needs ~185 MB/s fsynced at 100x. This consumer drive manages 278 MB/s with one writer at 1 MiB fdatasyncs, so 100x is borderline on consumer NVMe even before the wear the study already flags. 100x wants datacenter drives, and 10x is comfortable anywhere.
- **RAM drops to the ring.** A node with a commitlog sits at ~0.6 GB at 10x against ~1.0 GB memory-only (Phase 1's "2x the log window" note). That's the study's "ring only" row.
- **Two- and three-process failures are now a normal takeover, as the study assumed.** Kill or power-cut two or all three and the cluster resumes in about two seconds with nothing lost and no jump. The only lost-quorum case left is losing two disks.
- **Recovery time grows with what's kept on disk** (~0.35 s a GB). Phase 3's trimming at R keeps that to one flush interval plus a segment: at 100x with a 60 s flush that's ~11 GB, ~4 s. If that matters, a sealed segment could carry an index footer so recovery reads only the tail segment.
- **One append in flight per follower caps a follower at `max_batch_bytes` (4 MiB) per RTT + fsync.** At a 2 ms fsync that's ~2 GB/s, ample. It only bit on the saturated shared disk (20 ms fsyncs, ~200 MB/s). Pipelining appends, or a larger batch, is the fix if a host's fsync is that slow.

### Measuring a real host (OVH, Hetzner)

Nothing here touches a cloud host or real R2. To run on candidate hosts, with Jaz's OK:

1. **fsync**, on each box, on the disk the commitlog would use (no root, needs `fio`):
   ```
   tests/qlog/fsync_probe.sh /var/lib/vlrelay/probe
   ```
   It prints fdatasync p50, p99 and p99.9 for 4 KiB, 64 KiB and 1 MiB appends with one writer, 64 KiB with three, and the unsynced write ceiling. Run it three times at different hours: VPS disks are shared. A VPS whose fdatasync is well under 0.1 ms is acknowledging from a cache (hypervisor or controller), so don't count on it surviving a host power cut.
2. **The cluster**, three boxes (an OVH VPS-2 in each of three locations, or three Hetzner CX33/CAX21 in a spread placement group):
   - Build `qlog` for the target (`cargo build --profile dev-release --bin qlog`, or `docker buildx` for linux/amd64 on the Mac) and copy it over.
   - On box 1, MinIO for `qlog/leader` only (one GET and one CAS a takeover): `docker compose -f tests/qlog/compose.yml up -d --wait minio && docker compose -f tests/qlog/compose.yml run --rm minio-init`.
   - On each box `i`, with the peer port (3151) and the http port (3161) open to the others only:
     ```
     qlog node --id n$i --listen 0.0.0.0:3151 --http 0.0.0.0:3161 \
       --peer nJ=<box J>:3151 --peer nK=<box K>:3151 \
       --s3-endpoint http://<box 1>:3190 --prefix bench-$(date +%s) \
       --commitlog /var/lib/vlrelay/qlog
     ```
   - From box 1 (or a fourth box in the leader's DC), the checker, then the load:
     ```
     qlog check --node n1=<box1>:3161 --node n2=<box2>:3161 --node n3=<box3>:3161 \
       --stop-file stop --acked acked.txt --out . &
     qlog load --node n1=<box1>:3151 --node n2=<box2>:3151 --node n3=<box3>:3151 \
       --rate 3500 --duration 60 --acked acked.txt --out load.json
     for i in 1 2 3; do curl -s <box$i>:3161/qlog/status > status-n$i.json; done; touch stop; wait
     python3 tests/qlog/report.py .
     ```
   - Repeat at `--rate 350` and `35000`. Then kill -9 the leader's `qlog node` (and two, and all three) under `--rate 3500` and restart it, for the takeover pause and the restart recovery.
   - `report.py` gives the ack and end-to-end latency, each node's fsync p50 and p99 with the group-commit sizes, CPU and RSS. Paste them under the benchbox numbers above, against the "tmpfs + N ms" row that matches the host's fsync.

## Inputs

| Input | Value | Label |
|---|---|---|
| Events/s, frame size, zstd ratio, peak hour | ~350 (480 peak hour), 5.3 KB, 1.56x | measured ([cost](cost.md#inputs)) |
| CPU per event | 65-70 µs one node on cores, 89-94 on threads, 142-156 a 3-node cluster | measured ([perf](perf.md)) |
| Leader's share of the cluster's CPU | half | assumed |
| L0 flush with its compaction | ~4.4 Class A, ~9 Class B | measured in vlpds |
| SlateDB polls | ~0.4 GET/s per shard | vlpds defaults, measured there |
| vlpds lease loop | 1.5 Class A + 1 Class B per node per second at TTL 10 s | measured in vlpds |
| Per-DID state | 121.8 B a DID (243.6 B right after an update) | measured (`tests/state_bulk.rs`) |
| Hosts on the network | 6,260 listed, 1,956 active, 89 bsky.network PDSes with 23.4M of 24.0M accounts | measured (listHosts, reference notes) |
| RAM baseline | 2 GB a node plus the ring | assumed (shadow run: 1.1 GB at 60 events/s) |
| Flush upload, detection and takeover | ~2 s, ~5 s | assumed |
| fsync | 0.03-0.1 ms datacenter NVMe, 0.5-2 ms VPS | assumed |
| fsync, consumer NVMe | 2.7 ms p50, 5.9 ms p99 (fdatasync, one appending writer; 5.5 ms with three on one disk) | measured (benchbox, `tests/qlog/fsync_probe.sh`) |
| RTT | 0.2 ms one DC, 3 ms one metro, 65 ms cross-region | assumed |
| R2 PUT | ~200 ms p50 from benchbox | measured (vlpds `bench/results/spaces-r2-2026-10-05.md`) |
| Segment size, read size, DID shards, group commit | 64 MiB, 8 MiB, 4, 2 ms | design |
| R2 free tier | 1M Class A, 10M Class B, 10 GB-month a month | developers.cloudflare.com/r2/pricing (2026-10-06) |
| OVH VPS-1 to VPS-4 | $4.54, $8.50, $12.32, $23.37 | us.ovhcloud.com/vps (2026-10-06) |
| Hetzner Cloud CX33, CAX21, CPX22 | $9.99, $12.49, $22.99 (EUR 8.49, 10.49, 19.49) | costgoat.com listing (2026-09-05), not Hetzner's own page, which didn't render |
| AWS c7gd.large, .xlarge | $0.0907/h, $0.1814/h on demand | third-party listings (2026-10-06) |
| Everything else (S3, R2 rates, OVH ADVANCE-2, Hetzner AX42, AWS egress and cross-AZ) | as in [cost](cost.md#prices) | |

Like cost.md, the model sizes nodes for a minute burst at 2x the peak hour at 70% CPU, and lets accounts and DID state grow with the event rate. That's an upper bound, and it's what makes disk the limit at 100x (~850 GB of DID state per node).

## Reproducing it

```
python3 scripts/cost_model.py --quorum          # every table on this page
python3 scripts/cost_model.py --quorum --json   # the per-load, per-flush numbers
```
