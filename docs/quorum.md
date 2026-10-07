# vlRelay: quorum replication (design and cost study)

The first pass at vlRelay's economics didn't pan out. Three nodes on OVH with R2 came to ~$3.4k a month, and ~$2.8k of that was bucket requests: segments sealed every 25 ms on every node, plus host bookkeeping in bucket objects every 2-5 s. A non-archival sync 1.1 relay on one node costs about $10-15 a month to run. This page is a design and cost study for a vlRelay that keeps its recent log in replicas instead of the bucket, and writes the bucket rarely and in bulk. The replication core (Phase 1), the commitlog (Phase 2), the flush (Phase 3), bucket recovery (Phase 4), membership changes (Phase 5) and request counting with retention deletes (Phase 6) are built and measured, see [Implementation notes](#implementation-notes). The bucket figures here are the Phase 6 hour run's. The relay runs on it (Phase 7; [what's left](#whats-left)). `scripts/cost_model.py --quorum` generates every table above the notes.

The idea is the one in vlpds's TODO ("Quorum in-memory durability"). A leader appends each event, replicates it to two other nodes and emits it once two of the three hold it. The bucket gets a flush every 10-60 s, and the host cursors ride in the same flush as the log they belong to. A relay suits this better than a PDS does, because the PDSes upstream are the source of truth. If the relay loses an unflushed tail, it asks each PDS to replay from its last flushed cursor. Losing a quorum costs a re-ingest and some time, and never a user's data. Jaz's call: the firehose holds every event back until a quorum has it, so consumers only see events that a takeover keeps.

Three nodes is the shape most likely to run in production, so most of this page is about it. A single node gets its own section near the end. It uses the same flush and re-ingest model, with a local NVMe write-ahead log as its durability point.

## Headline

Today's load (~350 events/s), 10 full-firehose consumers, R2 after its free tier:

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
| Single node, OVH VPS-2, NVMe WAL + R2 for state and cursors only | 60 s | $9 | host $8, bucket $0 |
| Single node, OVH VPS-2, NVMe only |  | $8 | no bucket: losing the disk loses the cursors |
| Benchmark: a non-archival sync 1.1 relay on one node |  | $10-15 | Jaz's figure |

What the numbers say:

- The bucket stops mattering. Requests go from ~$2.5k a month on 3 nodes to ~$7 before R2's free tier and ~$0-1 after it, at a 30-60 s flush (measured over an hour at today's rate in Phase 6, see [Counting](#counting-phase-6)). What's left in the bucket bill is storage for 72 h of log (~$5 on R2).
- Hosts are the bill now. At today's load the leader needs ~0.1 vCPU at the sized rate, so the smallest NVMe VPS carries a node. Three OVH VPS-1 come to $14 a month, and with the log kept 24 h in the bucket the cluster lands at ~$15.
- So 3-node HA lands right at the top of the $10-15 benchmark only on 4 GB VPSes with a 60 s flush and a 24 h bucket window. On 8 GB VPSes (the size this page recommends for headroom) it's ~$30-31. On dedicated boxes it's whatever three boxes cost ($333 on AX42s, $600 on ADVANCE-2s).
- A single node gets under the benchmark: ~$9 on an OVH VPS-1 with R2 holding 72 h of log, ~$8 on a VPS-2 with no bucket at all.
- AWS is still decided by egress. Ten consumers outside AWS cost ~$4.1k a month in egress, and replication across three AZs adds ~$260.
- At 10x the cheapest HA is ~$123 (three VPS-4s, where disk and the port run out first). At 100x it's ~$3.4k, and $2.4k of that is edge boxes for the 10 consumers' 14.8 Gb/s. The bucket is ~$500 a month of storage there, still not requests.

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

1. The leader takes F as the seq its state has applied, which is committed. Only committed entries are ever flushed, since an uncommitted one could be dropped by a takeover.
2. It seals the DID state at exactly F. The apply loop stops at F until a SlateDB checkpoint exists (the memtable's L0 upload, ~20-350 ms measured), then carries on. Nothing waits on it but the state: acks and emission don't apply state in this design.
3. It uploads the log in (previous F, F] as vlpds segments, `log/qlog/{ordinal:012}.seg`, create-only, cut at 64 MiB raw. vlpds keys segments by a dense ordinal and keeps the seqs in the header, so vlpds's backfill reader serves them as they are.
4. It writes the manifest: epoch, F, R, the segments this flush wrote and the next ordinal, the SlateDB checkpoint that holds the state at F, and the host cursors as of F. Registry rows and host counters would ride the same way once the relay is wired in.
5. The manifest's CAS is the commit. A crash before it leaves the previous manifest in charge. A segment past it holds committed entries all the same, and the next flush adopts it if it starts where that flush's would.

A single object can't do it, because SlateDB writes its own SSTs and manifest. So the state is referenced by a SlateDB checkpoint, and our manifest names it. Keeping segments as their own objects also spreads the upload over the interval and lets backfill GET a segment by name.

### Cursors can't get ahead of the log

The danger is a cursor that says "PDS X is done through 1,000" in a flush whose log only holds X's events through 990. After a crash, the relay would ask X for 1,001 onwards and 991-1,000 would be gone. Cursors get into the log the same way events do:

- Each host owner sends its hosts' acked cursors to the leader about once a second, on its next submit. An acked cursor C for host X means every event of X up to C is committed, a duplicate or refused.
- The leader puts them on the first entry it appends after receiving them (replicated and journaled with it, never emitted). An event counted in C was committed before the host owner sent C, so its seq is below that entry's. They ride on an event rather than taking a seq of their own, which would put holes in the dense stream consumers see.
- The manifest's cursors are the highest each host's entries carry at or below F.

So every event a manifest's cursors count is at or below F, and in the flushed log. The same entries let a new leader resume a dead node's hosts from a cursor ~1 s old, from its own log, without the bucket.

The state follows the same rule. If the DID state got ahead of the log, a re-ingested commit would hit a rev the state already holds and be dropped as a duplicate, which loses it. That's why step 2 seals the state at exactly F. SlateDB does flush memtables on its own (at its L0 size), and a flush or checkpoint only promises *at least* the writes before it, so the seal doesn't rely on barriers: the applier writes nothing past F until the checkpoint exists ([Phase 3](#the-flush-phase-3)).

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

A flush is a manifest CAS, its segments (cut at 64 MiB raw, the last one partial) and the state's share: ~8 Class A and ~12 Class B a flush for its L0, the checkpoint it seals and the one it retires and the compaction, plus ~1.26 Class B a second of SlateDB's polls. Those are the Phase 6 hour run's numbers for the one SlateDB the leader keeps ([Counting](#counting-phase-6)), less SlateDB's GC deletes, which R2 doesn't bill as Class A ([Real R2 hour](#real-r2-hour)); the study had assumed four DID shards at vlpds's ~4.4 Class A and ~9 Class B an L0 flush and ~0.4 GETs a second of polls each. Segments are one PUT a flush today and ~2.8 a second at 100x. Against the old design:

| load | old, 3 nodes | quorum HA, 30 s | old, one node | quorum single, 30 s |
|---|---|---|---|---|
| today | $2,500 | $6 | $442 | $6 |
| 10x | $2,765 | $9 | $477 | $9 |
| 100x | $2,799 | $38 | $481 | $38 |

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

An operator can force bucket recovery from a single survivor (`admin quorum recover --force`, a sketch). Even then nothing is reissued, since the seqs start above R. (As built, [Phase 4](#bucket-recovery-phase-4): the candidate recovers on its own once its promise round proves no quorum of intact logs can exist, counting silent members as intact; `--no-auto-recover` makes that wait for an operator.)

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

The pause is one flush plus one CAS, under a second. Replacing a dead box is the same steps. (As built, [Phase 5](#membership-phase-5): a flush just before the pause leaves the barrier's own flush small, so commits pause for 6-13 ms on a local MinIO at 10x; a removed leader hands the new epoch to a member instead of stopping.)

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
| default: 64 MiB segments, one state, peers | $14 | $4 | $46 | $36 |
| 8 MiB segments | $17 | $6 | $276 | $265 |
| 4 DID shards (the study's design) | $50 | $12 | $82 | $45 |
| 24 DID shards (the old cluster's count) | $285 | $72 | $317 | $104 |
| vlpds leases kept, TTL 30 s | $33 | $22 | $65 | $55 |
| vlpds leases kept, TTL 10 s | $70 | $60 | $102 | $92 |

- Flush interval. Each flush costs ~14 Class A and ~12 Class B with the one state (measured), on top of ~1.3 Class B a second of polls. At 60 s that's inside R2's free tier (1M Class A a month), at 30 s it's ~$1 after it and at 10 s ~$13. A longer flush only costs the re-ingest window, and with the commitlog that window only matters when two disks are lost. So 30-60 s.
- Segment size. 8 MiB segments are fine today but cost ~$230 a month more at 100x. Use 64 MiB.
- DID shards. Each one costs its flush share and its polls. As built the leader keeps one SlateDB, and four (the study's design) would cost ~3.5x its requests at a 10 s flush, the old cluster's 24 ~20x. One is enough until the state outgrows one SlateDB's compactor.
- Liveness. Keeping vlpds's node leases at TTL 10 s adds ~$56 a month on R2 and ~$62 on S3, which is more than everything else on the bucket. Use peer heartbeats.
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
| today | 10 s | 1.00 / 2.46 | $12 / $23 | 0.90 / 2.46 | $6 / $15 |
| today | 30 s | 0.33 / 1.66 | $5 / $14 | 0.30 / 1.66 | $0 / $6 |
| today | 60 s | 0.18 / 1.46 | $5 / $11 | 0.15 / 1.46 | $0 / $4 |
| 10x | 10 s | 1.20 / 2.46 | $58 / $93 | 0.90 / 2.46 | $8 / $18 |
| 10x | 30 s | 0.60 / 1.66 | $51 / $84 | 0.30 / 1.66 | $2 / $9 |
| 10x | 60 s | 0.43 / 1.46 | $49 / $81 | 0.15 / 1.46 | $2 / $7 |
| 100x | 10 s | 3.70 / 2.46 | $523 / $794 | 0.90 / 2.46 | $28 / $48 |
| 100x | 30 s | 3.07 / 1.66 | $516 / $785 | 0.30 / 1.66 | $22 / $39 |
| 100x | 60 s | 2.92 / 1.46 | $514 / $782 | 0.15 / 1.46 | $22 / $37 |

Against the benchmark: one OVH VPS-1 with R2 holding 72 h is ~$9 a month at a 60 s flush and ~$10 at 30 s. A VPS-2 (8 GB, more headroom) is ~$9 with a state-and-cursors bucket and ~$8 with none. All of those are inside or under $10-15. What dominates is the host, then 72 h of log storage. The fit across hosts:

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

Can three small VPSes run HA? At today's load, yes: three OVH VPS-1s carry it at ~$18-19 a month, and three VPS-2s at ~$30-31 with room for 2x growth and a misbehaving identity cache. Replication is ~30 Mb/s out of the leader, under a tenth of a VPS-1's 500 Mb/s port. The risk with shared vCPUs is a noisy neighbour. The quorum masks a stalled follower, but a stalled leader holds up every ack. Past 10x, small VPSes run out of disk and port before CPU.

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
- Sealing the DID state at exactly F. SlateDB can put writes past F into F's checkpoint, so the applier stops at F until the checkpoint exists, and the seal checks the checkpoint's `last_l0_seq` is F ([Phase 3](#the-flush-phase-3)). Settled.
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

The harness (`tests/qlog/chaos.sh`, `just qlog-chaos`) runs three `qlog node` processes on a local MinIO (`qlog/leader` only), each restarted by a supervisor 1 s after it exits. Load comes from `qlog load`, frames are ~5.3 KB (the network's mean), and `qlog check` consumes every node's `subscribeRepos` from cursor 0. Partitions blackhole every peer route through `tests/qlog/proxy.py`, one route per direction. The checker asserts:

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
- A follower more than `--retain-mb` behind is reset to the leader's base, and its stream jumps. (Phase 2: with a commitlog it's served from the leader's disk first, and only a follower behind the leader's disk retention is reset. Phase 3: with the flush on, it's served from the bucket after that, and only a follower behind the bucket's retention is reset.)
- The pre-vote and the probe treat any I/O error from the leader as "dead". A flaky link can cost an unneeded takeover (availability, not safety).
- A lost quorum is unrecoverable until Phase 4. Until then it stops emission rather than reissuing seqs. (Phase 3 writes everything Phase 4 recovers from.)
- Membership is fixed (`--peer`). (Phase 5: the set lives in `qlog/leader` and changes at a flush barrier.)

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

### The flush (Phase 3)

`src/qlog/flush.rs` (the leader's flush loop, the manifest, the consistency check), `src/qlog/state.rs` (the state and its seal), plus cursor entries in `log.rs`, `wire.rs` and `commitlog.rs`, and one vlpds hook (`firehose::LocalTail`). The flush runs on the leader every `--flush-ms` (30 s by default). Each step below is in the order it runs.

- **Fence.** A new leader first CASes `qlog/manifest` to its own epoch, with the content unchanged. That's one GET and one PUT per takeover, off the ack path. An older leader's flush then fails its CAS because the ETag moved, and one that reads the manifest afterwards sees a newer epoch and steps down. A flush that lands before the fence is harmless: everything it names is committed, and F and R only go up. The mutation proves the fence matters. Without it, an old leader stalled before its CAS commits its manifest over the new leader's (`an_old_leaders_flush_loses_to_a_takeover`).
- **The state.** One SlateDB at `qlog/state` with the WAL off, written only by the leader. Opening it fences the previous writer. It stands in for the relay's DID state: per DID the seq and content of its last event, per host the highest cursor carried in the log, and `_applied`. The leader applies committed entries as they commit, so state never waits on acks or emission. Each batch is written with SlateDB's user seqnum set to the batch's last log seq. That makes a manifest's `last_l0_seq` the log seq its state reaches.
  - **Followers don't keep a state.** The study had every follower apply committed entries, for an instant promotion. As built, a new leader opens `qlog/state` at whatever the old leader last flushed (at or past the manifest's F: it sealed before writing its manifest) and replays from its own log. That's 20-30 ms to open on a local MinIO, plus the replay of up to one interval.
  - Every node already keeps its log above F (next point), so the replay always has what it needs. It's also Phase 4's path: open at a checkpoint, replay or re-ingest. The state stays one writer's in the bucket, and that's the cost model's one L0 flush per interval.
  - In the real relay the leader's `check_chain` needs state at the head, so promotion would wait for that replay (a second or two of entries at 100x and a 60 s flush, estimated). If that's too slow, followers can apply too. That would cost their L0 flushes in the bucket, or a local SlateDB per node.
- **State at exactly F, and what SlateDB does.** Reading the fork at `c7b29a06`:
  - With the WAL off, a memtable freezes only on the write path, at `l0_sst_size_bytes`, on an explicit flush or checkpoint, or at close. There's no timer (`flush_interval` is WAL-only). The freeze itself happens between write batches.
  - The flush tracker, though, targets the newest frozen memtable when it handles a request, and the manifest writer folds every contiguous uploaded memtable into one manifest update along with any pending checkpoint. So a `flush()` or a `create_checkpoint(All)` can land in a manifest that also holds writes made after it was asked for. It promises "at least F", never "exactly F".
  - The design therefore doesn't rely on SlateDB staying quiet between barriers. The applier is the state's only writer and it writes nothing past F until `create_checkpoint(All)` returns. Then the seal reads the checkpoint's manifest back and refuses it unless `last_l0_seq` is F. Automatic flushes at other times are fine: they only ever hold seqs at or below what's applied.
  - `a_seal_holds_exactly_its_point_across_automatic_flushes` seals four times with 16 KiB L0s, so memtables freeze and upload on their own before and between seals. Each checkpoint holds exactly its seal point, by manifest and by contents.
  - No fork change was needed.
  - SlateDB's default `l0_max_ssts` (8) made seals wait 1-5 s for the compactor at 2 s flushes. It's 32 here, as in vlpds.
- **Cursor entries.** Host cursors ride on log entries (`Entry::cursors`, an `n | (host, cursor)*` blob) instead of taking seqs of their own, so the stream consumers see stays dense.
  - A submit carries the submitter's cursors. The leader puts them on the first entry it appends next, holding them if the submit had no events.
  - They're replicated with the entry and journaled as a new commitlog record (`T_APPEND_C`: epoch, seq, cursor length, cursors, data). The state applies them, and the manifest's cursors are the state's at F.
  - An acked event's seq is below the entry its cursor rides on, so cursors at F never count an event above F. The check confirms it for every manifest, and a mutation catches it: cursors sent one batch ahead are reported as "ahead of the log at F".
- **Segments.** vlpds's own segments, `log/qlog/{ordinal:012}.seg`. vlpds keys segments by a dense ordinal and keeps the first and last seq in the header, so it isn't `qlog/seg/{first_seq}-{epoch}` as §2 sketched. They're create-only, zstd at vlpds's level 1, built in a blocking task.
  - Each flush writes exactly (previous F, F], cut at 64 MiB raw. A partial segment is never rewritten, so at today's rate a 30 s flush makes one ~40 MiB segment.
  - A flush that finds its ordinal taken reads that segment's header. If it starts where this flush's would, and ends at or below F, the flush adopts it. That's a flush that died after a segment PUT, or a deposed leader's: committed entries are the same on every node. If it ends past F, the flush retries next tick. Anything else stops the flush.
- **Manifest.** `qlog/manifest` is JSON: `{epoch, leader, flushed: F, reserve: R, next_ordinal, segments: [{ordinal, first, last, bytes}], state: {checkpoint, manifest_id, seq}, cursors: {host: cursor}, flushes, at_ms}`. It's a few KB with 64 hosts. It lists only this flush's segments: ordinals are dense from 0 and headers carry seqs, so the full list would only grow with retention. After the CAS the leader deletes the previous manifest's checkpoint. A new leader deletes any `qlog-*` checkpoint the manifest doesn't name, which covers flushes that died between seal and CAS.
- **The reservation.** R = max(previous R, F + H), `--headroom`, 8.64M by default (three 30 s intervals at 100x). The leader caps the commit index itself at the last committed manifest's R, which caps emission on every node. Followers learn F and R from the append header, so a new leader starts with them, and its own fence read raises them. `commit_stops_at_the_reservation` holds commit at exactly R = H when no flush follows the first.
- **Trim.** A node's commitlog floor is min(emitted, commit, F as it knows it). On the leader it's also every live follower's match: a follower heard from within `laggard_grace` (10 s) keeps the leader's disk back to it. The brief said min(emitted, commit, R), but R is never below the commit index, so the bound that matters is F: only what's in the bucket leaves local disk.
  - A follower behind the leader's disk is now served from the bucket segments (one segment cached per replicator), not reset. The Phase 2 ceiling run's three resets, and a reset the first mid-trim run here hit, were exactly that case. With the bucket path off, the checker fails on the hole (`a_follower_behind_the_leaders_disk_catches_up_from_the_bucket`).
  - When a follower is also behind the leader's disk with nothing flushed for it, the reset now lands at the oldest seq the disk holds, not at the in-memory base, which could have been past F.
- **Backfill.** The quorum log's firehose gets the bucket (`Firehose::store`), so a cursor older than the ring backfills from the segments instead of getting `OutdatedCursor`. That alone isn't enough.
  - vlpds's backfill assumes every emitted event is in the bucket ("everything ≤ the ring floor that exists was sent"). The quorum log emits up to a flush interval before it flushes, so a cursor between F and the ring floor would have skipped silently.
  - The one vlpds change is `firehose::LocalTail`, opt-in and counted streams only. The backfill reads the bucket up to the tail's floor, then the node's own log (memory or commitlog) up to the ring. Every node keeps its log above F, so the two always meet.
  - The merger's spill is off for this log (`set_max_queue_bytes(usize::MAX)`). One followed log never queues behind another, and the spill's read-back would look up bucket ordinals by the batches' ordinals, which don't match.
  - `old_cursors_backfill_from_the_bucket_then_the_local_log` reads densely from cursors 0, F - 1, F and F + 1 with a 32 KiB ring. Every chaos run ends with a consumer from cursor 0 through all three tiers.
- **Bucket retention isn't built.** Segments, and the state's SSTs past compaction, stay until something deletes them. Phase 6 adds vlpds's retention: the `retain/` report first, then deletes. The bucket path for lagging followers then resets only past that.

### Tests, chaos and numbers (Phase 3)

In-tree (`cargo test --lib qlog`, 29 tests, all passing):

- the seal across automatic flushes;
- a crash between two commitlog segment deletions (what's left is a readable suffix);
- flushes under load across three leader kills, with the consistency check mid-run and at the end;
- commit held at R;
- a crash at each flush step, twice round (fenced, sealed, after a segment PUT, before and after the manifest CAS), each one killing the leader right there;
- an old leader's stalled flush losing to a takeover;
- backfill from every tier;
- a follower catching up from the bucket;
- seeded random kills, power cuts, partitions and flush crashes at 4% a step (`flushing_random_chaos_keeps_every_manifest_consistent`, seeds 1-10 clean).

The check (`flush::verify`, also `qlog verify`) holds for any manifest:

- the segments hold the log densely from 1 to F, and the last named one ends at F;
- the state checkpoint's manifest has `last_l0_seq` = F;
- the checkpoint's contents equal replaying those entries to F: every DID's seq and content, the count, `_applied`;
- its cursors are the manifest's;
- no host's cursor counts an event that isn't in the log at or below F;
- segments past the manifest still continue the log.

Restarting from a manifest is opening that checkpoint (a `DbReader` at it), so "the state a restart gets equals replaying the log to F" is the same check.

The process harness (`tests/qlog/chaos.sh`) flushes every 2 s by default (`FLUSH_MS`). It runs `qlog verify` against the bucket every 20 s and at the end, and the checker finishes with a consumer from cursor 0. New scenarios:

- `flush-crash`: SIGKILL at a random flush step, 5% a step;
- `mid-trim`: SIGKILL between commitlog segment deletions, with a 32 MiB disk budget;
- `mixed-flush`: `mixed-durable` with flush crashes on top.

`RING_MB=16` keeps the ring small, so reconnecting consumers backfill through the bucket and the local tail. Results on benchbox, commitlogs on the shared NVMe:

| run | faults | distinct seqs | violations | manifest checks |
|---|---|---|---|---|
| kill-leader, 3,500/s, 120 s | 7 x kill -9 (pause median 58 ms, max 73) | 420,455 | 0 | 5 + final, consistent |
| kill-two, 3,500/s, 120 s | 7 (pause median 1.79 s) | 420,191 | 0 | 5 + final |
| power-cut-all, 3,500/s, 120 s | 7 (pause median 2.57 s) | 420,052 | 0 | 5 + final |
| partition-leader, 350/s, 120 s | 5 x isolated 5 s (pause ~1.01 s) | 42,011 | 0 | 6 + final |
| flush-crash, 3,500/s, 180 s | 18 flush crashes (6 after the CAS, 5 before, 4 after the seal, 1 after a segment, 2 at the fence) | 631,119 | 0 | 5 + final |
| flush-crash again | 11 flush crashes | 630,787 | 0 | 8 + final |
| mid-trim, 3,500/s, 90 s | 3 crashes mid-trim | 315,087 | 0 | 4 + final |
| mixed-flush, 3,500/s, 240 s | 23 flush crashes, 3 kill-all, 2 power-cut-all, 1 kill-two, 5 kill -9 and power cuts, 2 SIGSTOP | 841,381 | 0 | 10 + final |
| mixed-durable (flushing), 3,500/s, 240 s | 2 power-cut-all, 2 power cuts, 3 kill -9, 2 partitions, 4 SIGSTOP | 840,541 | 0 | 10 + final |

That's 4.6M seqs, every acked seq emitted with its content, and every consumer stream dense. Every manifest checked was consistent, and every run ended with 0 segments past the manifest (adopted or superseded). Every consumer from cursor 0 read the whole log densely through the bucket, the local tail and the ring, ~0.6M events a second.

Three things the runs found, now fixed:

- **Resets past what a follower had emitted.** The first `mid-trim` run crashed all three nodes every few seconds (30% a deletion). A follower fell behind the leader's disk and was reset, a hole in its stream (the Phase 1/2 reset). Lagging followers are now served from the bucket.
- **Seals waiting 1-5 s** on SlateDB's L0 cap (8), at 2 s flushes. Raised to 32.
- **The chaos harness** aborted a run when a fault picked a node the supervisor was restarting, which is common under flush crashes. The verifier also raced the next flush deleting the checkpoint it was about to read. It now opens the checkpoint first; SlateDB's GC keeps the files for minutes.

Measured on benchbox, three nodes on loopback, commitlogs on tmpfs with a 1 ms emulated fsync (the "tmpfs + 1 ms" rows of Phase 2: the shared NVMe was noisier today, ~21-25 ms acks at 3,500/s with and without the flush). MinIO is local, on tmpfs. Frames are the load generator's ~5.3 KB, padded with one repeated byte, so they compress ~700x. Stored bytes below are meaningless; raw bytes aren't.

| 95 s runs | 350/s, 30 s flush | 3,500/s, 30 s flush | 35,000/s, 10 s flush |
|---|---|---|---|
| flushes | 4 | 4 | 10 |
| per flush: entries / raw / segments | 8.3k / 42 MiB / 1 | 83k / 420 MiB / 7 | 333k / 1.68 GiB / 26.7 |
| flush duration (seal to CAS), p50 / max | 74 / 80 ms | 745 / 803 ms | 2.32 / 2.39 s |
| applier paused for the seal, p50 / max | 21 / 24 ms | 126 / 132 ms | 346 / 373 ms |
| leader CPU, flush off → on (cores) | 0.031 → 0.050 | 0.054 → 0.121 | 0.24 → 0.72 |
| leader RSS, off → on | 295 → 318 MB | 698 → 795 MB | 770 → 1,815 MB |
| ack p50 / p99, off | 1.19 / 1.80 ms | 1.32 / 1.82 | 2.65 / 3.46 |
| ack p50 / p99, on | 1.23 / 1.90 ms | 1.33 / 1.96 | 2.45 / 3.64 |
| worst per-second ack p99, seconds with a flush / without | 2.36 / 2.55 ms | 3.11 / 2.54 | 9.71 / 6.26 |
| submit to first consumer p50, off / on | 3.32 / 3.37 ms | 2.53 / 2.52 | 4.05 / 3.89 |

Requests per flush, counted by vlpds's object-store counters over each flush's window, which includes the state's own background work in it:

| per flush | 350/s, 30 s | 3,500/s, 30 s | 35,000/s, 10 s |
|---|---|---|---|
| PUT create (segments + SlateDB SSTs and manifests) | 3.2 | 8.8 | 28.6 (+ 0.9 multipart uploads, 1.8 parts) |
| PUT plain | 1.0 | 1.0 | 0.1 |
| PUT CAS (our manifest) | 1.0 | 1.0 | 1.0 |
| LIST | 0.8 | 0.8 | 0.9 |
| GET | 9.8 | 10.8 | 15.3 |
| Class A, less the log segments | ~5.0 | ~4.6 | ~5.0 |

Over a whole steady run (350/s, 30 s flushes, 300 s, the leader process; followers send ~nothing), with the state's manifest polled every 10 s (SlateDB's default 1 s doubled the GETs, to ~4 a second):

| per second | GET | GET range | HEAD | LIST | PUT | PUT CAS | PUT create | Class A | Class B |
|---|---|---|---|---|---|---|---|---|---|
| measured, one DID shard | 2.07 | 0.24 | 0.03 | 0.06 | 0.06 | 0.04 | 0.18 | 0.34 | 2.34 |
| study, today, 30 s, four shards | | | | | | | | 0.67 | 2.81 |

The rest of the GETs are the state's compactor and GC polls, which tick whether or not anything was flushed.

What the numbers say:

- **The flush doesn't touch the ack path.** Acks and emission are the same with the flush on and off at all three rates, within run-to-run noise. The flush's pause is the state applier's alone (20-350 ms), and nothing waits on the state. At 100x the worst per-second p99 rises from ~6 to ~10 ms in the seconds a flush runs: the leader's zstd and segment reads compete for CPU on a shared box.
- **A flush costs the leader CPU in proportion to bytes.** ~0.5 cores at 100x for 1.7 GB raw every 10 s: reading the entries back, zstd and the PUTs. That's ~5% of the study's 10-vCPU leader at 100x. Followers don't flush or keep state, so their CPU is unchanged.
- **Requests match the cost model.** Less the log segments, a flush with one DID shard is ~5 Class A and ~10-15 Class B. The study assumed ~4.4 A + ~9 B per shard plus the manifest (~5.4 A). Segments are one PUT per 64 MiB raw, as assumed. Over a whole run the leader sends 0.34 Class A and 2.3 Class B a second at today's rate with one shard, against the model's 0.67 and 2.81 with four. The model's "~0.4 GET/s of SlateDB polls per shard" holds only with the 10 s manifest poll vlpds also uses: at SlateDB's default 1 s it's ~2 GETs a second more per shard (~5M Class B a month, inside R2's free tier for one shard, not for 24).
- **Flush duration is ~1.4 s a GB at 100x on a local MinIO.** That's two to three intervals of headroom for H at 10 s, and more at 30-60 s. On R2 add ~200 ms a PUT, run several in parallel (the PUTs here go one at a time). The study's "upload ~2 s" assumption holds at 10x, and at 100x it wants parallel segment PUTs.
- **Leader RSS grows with the flush at 100x** (~1 GB more). That's the entries of one segment held while it's built and compressed, plus SlateDB's memtable for 333k DIDs. A 30 s flush at 100x would be ~3x the memtable. Budget ~1 GB for the state's memtable per 1M DIDs changed in an interval, or seal more often.
- **The emit path and replication are unchanged** by cursors on entries. At one host-cursor update per second per host owner it's a few hundred bytes a second.

### Bucket recovery (Phase 4)

`flush::recovery_point` and `flush::recover` (the bucket side), `Node::bucket_recover` (the node side), `State::recover` and `State::jump`. Phase 3 listed what this needed (the state from the manifest's checkpoint, salvage, seqs from R + 1, re-ingest, the single node, the retention report); as built:

- **The trigger is the promise round, and it's automatic.** A candidate that has won `qlog/leader` collects promises as before. If fewer than a quorum of the members that promised are intact, it counts every member that didn't promise as possibly intact. Only when even that falls short of a quorum is the quorum lost, and then the candidate runs the bucket recovery itself.
  - A member that promised this epoch and isn't intact can't become intact behind the candidate's back: it refuses older leaders now. So "intact that promised + silent < quorum" proves that no quorum of intact logs exists anywhere. That's the "never while an intact quorum could exist" rule.
  - The candidate already has promises from a quorum (it needs pongs from one before its CAS), so a minority can't recover. A cut-off node never gets that far, as before.
  - Wiping one disk while the leader is dead leaves one intact log answering and one member silent. That's 1 + 1 ≥ 2, so the candidate waits, and the dead leader's return makes a normal takeover (`no_recovery_while_a_silent_member_could_be_intact`).
  - Mutation: with silent members left out of the count, that test fails, because the cluster recovers and jumps while the old leader's intact log is only down.
  - "Not intact" is a node with a new or wiped commitlog while `qlog/leader` exists, or a memory-only node that restarted and hasn't caught up. So with commitlogs a lost quorum is two lost disks. Memory-only now recovers on its own after two process deaths (it used to stop).
  - `--no-auto-recover` (`Config::auto_recover`) turns the trigger into a log line and a wait, for an operator who wants to decide. The default is automatic, since the proof above is what an operator would check.
- **What a recovery keeps.** In order:
  1. The manifest's F.
  2. Orphan segments past it that continue the log densely. A flush that died before its CAS wrote them, and they're committed entries. F moves to their end, F'. Segments don't carry the entries' host cursors, so cursors stay at the manifest's, which only costs more re-ingest.
  3. The longest committed prefix past F' that a member which promised holds. The members are tried by their commit index, and the entries are fetched with the existing `Fetch` (from the candidate's own log if it's the best).
  - The result is S. Seqs `(S, R]` are skipped for good, and the log resumes at R + 1 under the recovery's epoch.
  - A segment past the manifest that starts at or below F is a deposed leader's flush still running. It's deleted, so it can't take an ordinal the recovery or the next flush writes. The flush does the same when it finds one in its way.
- **The state is a clone of the manifest's checkpoint.** `State::recover` clones it to `qlog/state-e{epoch}` (`create_clone_builder_from_source(CloneSourceSpec::with_checkpoint)`), applies the orphans and the salvage to the clone, and moves `_applied` to R with a write at SlateDB seqnum R (`State::jump`). Then it seals at R.
  - Why a clone and not a rewrite: the clone is a new SlateDB manifest over the checkpoint's SSTs, so it's O(1) in the state's size (25-46 ms on a local MinIO). Rewriting every key changed past F in place would mean diffing the whole state, ~850 GB a node at 100x.
  - The clone pins its source with a checkpoint of its own, and SlateDB lists the source as an external database until compaction has rewritten its SSTs. The chain is transitive: after several recoveries the current state listed two older paths. A clone cut short is resumed by SlateDB when the same epoch retries; another epoch leaves it for the retention report.
  - `a_recovered_state_is_its_checkpoint_not_the_latest` keeps writing to the source after the checkpoint (with memtables uploading on their own) and checks that the clone holds exactly the checkpoint plus its own writes.
  - `the_recovered_state_equals_replaying_the_bucket` stops the new leader's flush at its fence, so the recovery manifest stays current. `flush::verify` then holds for it: the segments replayed across the gap equal the state at R, `last_l0_seq` = R and `_applied` = R. It also crashes one recovery attempt at each step.
- **The recovery manifest.** It has F = R, R' = R + H, the salvaged segments, the state at R, the cursors at S, and two new fields:
  - `gaps`: every `(S, R]` so far, kept for good, so `verify` and readers walk the segments across them.
  - `recovery`: `{generation, epoch, after: S, base: R, cursors, at_ms}`.

  It's CASed against the ETag read at the start. Losing the CAS, or a crash anywhere before it, leaves the old manifest in charge. With F = R, the next flush writes `(R, F2]` with no special case, and the stale-segment rule covers a zombie's.
- **Two recoveries in a row jump twice.** An attempt that dies after its manifest CAS may have led and committed above its R, for all the next candidate can tell, so the next one recovers from that manifest and skips another H. The checker and `verify` handle gaps that follow each other.
- **The node side.** Under the emitter's ordering lock, the candidate does four things, then leads:
  - If this process has live consumers and emitted less than S, it first emits up to S from the bucket, so its own consumers see no hole below the gap.
  - It resets its log to `(epoch, R)`. The base takes the recovery's epoch, so a promise round prefers this log over an older one still on a member's disk.
  - It sets `emitted` to R.
  - It learns F, R' and the generation.

  Followers are reset to R by the existing reset path. A follower with an empty log (`AppendResp.last_seq == 0`, a wiped or new disk) now starts at the leader's oldest local entry instead of replaying the bucket from seq 1. That's also the normal rejoin of a wiped box, with no recovery: such a follower has emitted nothing, so it has no hole to avoid. Otherwise every recovery would have waited for a follower to replay the whole bucket before anything committed.
- **Emitters cross a gap before reading the bucket there.** A follower reset past what it emitted, and a candidate topping its stream up to S, hand over what the bucket holds up to the reset, across every recorded gap. They check the manifest's gaps first and read the bucket only outside them. Until the new leader's first flush replaces it, a deposed leader's segment can sit at the recovery's next ordinal holding seqs from S + 1, and a read at S + 1 would emit seqs inside `(S, R]` that come back above R with other contents. The emitter reads the manifest again whenever its reset target grows: a later recovery's gap is in the manifest before any log resumes above it. A failed bucket read during a recovery's top-up costs that stream a gap, not the recovery, since its manifest is already written. `a_follower_reset_across_a_gap_never_emits_a_deposed_leaders_segment` and `a_follower_reset_across_two_gaps_emits_the_log_between_them` play the recoveries by hand.
- **What consumers see.** The firehose needed nothing new: backfill already reads "what exists" up to the local tail's floor, and the counted stream doesn't require dense seqs.

  | consumer | sees |
  |---|---|
  | live on a node that kept running | everything up to S (topped up from the bucket if needed), then R + 1 on: one jump over `(S, R]` |
  | reconnecting at or below S | the bucket to S, then R + 1 on |
  | reconnecting in `(S, R]` (it saw events the recovery lost) | R + 1 on |
  | above R | `FutureCursor` after the 2 s grace, as before |

  No seq is ever emitted with two contents. The events the jump skips come back from the hosts above R, so a consumer that saw them sees them again (a repeated rev for a sync 1.1 consumer, as §1 says).
- **Re-ingest.** Each `Submitted` carries the leader's recovery generation, which also rides on appends and promises and is read from the manifest at start and at a fence. A host owner that sees a higher generation asks the leader for that recovery's cursors (`Cursors`), re-reads each host from there, and marks everything at or below them done.
  - A submit carries the generation its cursors were computed under. The leader drops cursors from a submitter that hasn't rewound yet, since they may count events the recovery lost and would put the next manifest's cursors past the log.
  - The load generator's hosts stand in for fakepds. Acks from before the rewind don't count, and events acked under the new generation aren't sent again.
  - Duplicates have two sources. An event can be both at or below S (or emitted in the gap before the crash) and above R: these are bounded by what a host sent after its cursor, which is up to one cursor interval (1 s) plus everything emitted in `(S, crash]`. Separately, every resend of a batch whose ack was lost, with or without a recovery, makes one. The real relay drops both against the state with `check_chain`, and consumers drop repeated revs. The checker counts them and doesn't fail on them.

### The single node (Phase 4)

A config with no peers is a quorum of one: the commitlog is the WAL, an entry commits at its own fsync, and emission follows the local durable point as for any member. Two changes:

- **It leads at once.** It has nobody to hear from, so it doesn't wait out the election timeout (the restart pause went from 2.2 s to 1.2-1.5 s, which is mostly the harness's 1 s restart delay).
- **A wiped disk is a lost quorum.** Its commitlog is fresh, it isn't intact, and nobody is silent, so it recovers from the bucket at R + 1. kill -9 and power cuts replay the WAL as in Phase 2, with no jump.

`a_single_node_recovers_from_its_wal_and_from_the_bucket` runs six kills and power cuts under load (no gap) and then a wipe (one gap). Everything the hosts sent is in the log.

### Bucket retention (Phase 4: the report)

`qlog retain` (`retain.rs`) writes `retain/qlog`. That's vlpds's per-log retention report: its `pruned_seq` is left as it is, because nothing is deleted, and the plan sits alongside. The plan lists:

- **Segments that could go.** vlpds deletes oldest first, so it's the longest prefix of ordinals older than the horizon. They're all named by a manifest and at or below F, and the state checkpoint covers them. Also the `pruned_seq` that deleting them would raise.
- **Stale segments** past the manifest.
- **Every state path,** marked as current, referenced (an external database of the current state) or deletable, with stale `qlog-*` checkpoints and SlateDB's clone checkpoints.

Nothing beyond Phase 3's checkpoint deletes was added. Every chaos run ends with the report: after mixed-wipe's eight recoveries, six old state paths were deletable and two were still referenced.

### Tests, chaos and numbers (Phase 4)

In-tree (`cargo test --lib qlog`, 37 tests, looped 8 times clean):

- **The recovery tests:**
  - wiping every disk twice under load;
  - wiping two (the leader back in time keeps its quorum, and otherwise there's salvage from the survivor);
  - no recovery while a silent member could be intact;
  - orphan adoption;
  - the recovered state equal to replaying the bucket, with a crash at each recovery step;
  - the single node;
  - the clone test in `state.rs`;
  - the checker's own gap test.
- **Mutations:** resuming at S + 1 instead of R + 1 gives 1,040 "seq N emitted with two contents" on the single node and fails the wipe-all test; leaving silent members out of the trigger fails the no-recovery test.
- **Test-harness fixes the new tests exposed:** `converge` now waits for the checker's tap to reach the top, and test ports are unique per process. An OS-picked port came back to a cluster running alongside, whose leader then appended into another test's nodes.

The checker (`check.rs`) now takes the manifest's gaps and the load's summary:

- A stream may jump only across gaps.
- An acked seq in a gap must have its event emitted again above it.
- Every event the hosts sent must be emitted at a seq outside every gap.

The chaos harness adds:

- `wipe-all`;
- `wipe-two` (survivor leader or follower at random);
- `mixed-wipe` (kills, kill-two, kill-all, power-cut-all, wipes, partitions);
- `single-kill`, `single-power-cut`, `single-wipe` and `single-mixed` (`NODES=1`).

A wipe is kill -9, then the commitlog deleted before the supervisor restarts the node. Runs on benchbox at 3,500/s (10x), 5.3 KB frames, 64 hosts, commitlogs on tmpfs with a 1 ms fsync, local MinIO:

| run | faults | recoveries | distinct seqs | violations | manifest checks |
|---|---|---|---|---|---|
| wipe-all, 2 s flush, 90 s | 5 wipe-all | 5 | 365,961 | 0 | 4 + final |
| wipe-two, 2 s flush, 120 s | 8 wipe-two (4 survivor leaders, 4 followers) | 8 (5 with salvage, 2.4k-6.5k entries) | 463,204 | 0 | 5 + final |
| mixed-wipe, 240 s | 2 wipe-all, 6 wipe-two, 3 kill-all, 1 kill-two, 2 power-cut-all, 3 partitions | 8 | 895,476 | 0 | 10 + final |
| mixed-wipe with flush and recovery crashes (5% a step), 180 s | 19 injected crashes, 4 power-cut-all, 2 kill-two, 4 kill -9, 1 wipe-two, 2 partitions | 1 + crashed attempts | 635,842 | 0 | 8 + final |
| single-mixed, 150 s | 4 kill -9, 12 power cuts, 1 wipe | 1 | 532,453 | 0 | 6 + final |
| kill-two, memory only, 90 s | 5 kill-two | 5 (salvage 1.1k-5.8k each) | 341,081 | 0 | 4 + final |
| wipe-all, 30 s flush, H = 8.64M, 170 s | 3 wipe-all | 3 | 818,301 | 0 | 6 + final |

That's 4.05M distinct seqs, 31 recoveries and 0 violations. Every stream jumped only across gaps, and every acked seq lost in a gap came back above it: 200,812 of them in the 30 s run. Every event the hosts sent is in the final log outside the gaps. Every consumer from cursor 0 read the whole log through the bucket, across the gaps, and every manifest verified.

| | measured (local MinIO) |
|---|---|
| recovery, bucket side: manifest and orphans read / state clone / apply, jump and seal / salvage segments / manifest CAS | 0-2 ms / 21-46 ms / 3-58 ms / 0-49 ms / 0-1 ms; 33-146 ms in all (the long ones salvaged 2-6k entries) |
| emission pause, wipe-all (fault to the next new seq at any consumer) | 2.20-2.30 s: the harness's 1 s restart, the 1 s election timeout, ~40 ms of recovery |
| emission pause, wipe-two | 1.3-3.0 s (median 3.0 s): the restart, the election timeout, and the 0.5 s rank stagger when a lower-ranked candidate goes first |
| emission pause, single node: wipe / kill -9 / power cut | 1.21 s / 1.49 s median / 1.39 s median (1 s restart plus WAL replay or recovery) |
| load: first new-generation ack to the recovery's cursors | 0-18 ms |
| re-ingest at 10x, 2 s flush | 4.5k-12k events sent again, all acked again in 57-209 ms |
| re-ingest at 10x, 30 s flush | 71k-81k events sent again (66k-77k of them acked before: the consumers' repeats), all acked again in 0.71-0.88 s |

Consumers saw the cursor-to-crash window again: with a 30 s flush, ~70k events a recovery at 10x (~23 s of load, i.e. the interval since F plus the cursor lag). Retried batches gave another 5k-36k duplicates a run with or without recoveries, which is the forwarder's resend from Phase 1, never deduplicated in this harness.

### What changes the study

- **Recovery after a lost quorum is cheap on the bucket side, and the pause is detection.** The clone is O(1), so the state's size doesn't matter, and the whole bucket side was 33-146 ms. On R2, budget ~6 round trips: a manifest GET, a few SlateDB manifest reads and writes for the clone and seal, one PUT per salvaged segment, and the CAS. That's ~1-1.5 s at ~200 ms each. The study's ~5 s for detection and takeover is then mostly restart and timeout, as before.
- **Re-ingest catch-up is CPU-bound in the real relay, not here.** The harness has no verify, so 81k events came back in under 1 s. The study's 12.7 s at a 30 s flush (2x headroom on verify and apply) stands for the relay. The storm size matched §2's table: ~71-81k events at 10x and 30 s against the 129.5k worst case (a crash right before a flush).
- **The repeat window is the flush interval.** A consumer sees again everything emitted since F, as the study said, and a shorter flush shrinks it in proportion. Salvage from a surviving member shrinks it further: in wipe-two, salvage kept 2-6k entries a recovery, so the gap's acked losses were 0.
- **A surviving leader back within its timeout keeps its quorum.** Two wiped followers restarted within the election timeout rejoin by the fresh-follower reset, with no recovery at all. The study's "two disks gone" row needs the two to be gone for longer than the timeout, or the survivor to be a follower.
- **Each recovery costs H seqs, and a recovery that dies after its CAS costs another H.** At the default H (8.64M) that's still ~3,000 years of seqs at 100k/s for a recovery a day.
- **Old state paths are retention's job.** Each recovery leaves the previous path, which stays referenced until compaction rewrites its SSTs (transitively: one state listed two older paths). Phase 6's deletes must check `external_dbs` before removing a path, as the report does.
- **Memory-only now survives two process deaths**, by recovering with salvage from the survivor, at the cost of a jump. The commitlog still turns those into a normal takeover.
- **The single node's restart pause** is the process restart plus the WAL replay (~0.35 s a GB). Disk loss costs a jump and the re-ingest of everything since F.

### What Phase 5 needs

- **Membership from the bucket.** `Config::members` comes from `--peer`, and the trigger counts silent members against it. With membership changes, the trigger, the quorum size and the promise round must use the member set in `qlog/leader` at the epoch. Otherwise a removed node, or a learner, could be counted as possibly intact or as a voter.
- **Learners.** A new box joins as a fresh follower. It now starts at the leader's oldest local entry (no bucket replay) and is intact once it has matched the leader's last seq. A learner needs the same, without being counted in `advance_commit` or in promise rounds until the switch.
- **The switch at a flush barrier.** Pause commits, flush to the commit index, CAS `qlog/leader` with epoch + 1 and the new set, then resume. The fence and manifest code already handle an epoch change with the content unchanged. The `Recovery` generation must carry across.
- **Box replacement under load.** Wipe a box and bring it back as the same id (that works today, as the fresh-follower reset), and replace it with a new id (needs the above). Chaos: `replace-follower` and `replace-leader` under 10x, checked as now.
- **Salvage holds everything past F' in memory.** That's fine at 10x, but ~1.7 GB at 100x with a 10 s flush. Stream it into segments while applying.
- **Known flake, not from this lane:** `fakepds selftest_small` binds fixed ports at 39000+, inside the ephemeral range, and fails on benchbox while other processes hold connections there.

### Membership (Phase 5)

`Node::change_members` (`node.rs`), `POST /qlog/members` on a node's http port, and `qlog member add|remove|replace|set`, which finds the leader through `/qlog/status` and retries until `qlog/leader` holds the new set. As built:

- **The member set is `qlog/leader`'s.** The record is `{epoch, leader, members, learners, addrs, since}`. `members` are the voters at that epoch, `since` is the epoch they took effect at, `learners` are replicated to without counting, and `addrs` fills in peer addresses that `--peer` doesn't name. Commit counting, the leader's quorum-heard check, the pre-vote, the promise round, the candidate's rank and the lost-quorum trigger's silent-member count all use the record's members. `--peer` is only an address book now, and `--members` (default: the node and its peers) is the bootstrap set the first CAS writes.
  - A takeover reads the record first (it already did, for the CAS) and copies the set into the next epoch. A node the record doesn't name doesn't campaign. A learner waits for the change that makes it a member. Anything else was removed (or never added) and retires: it makes no more takeover attempts, so a retired box costs the bucket nothing, until a leader appends to it again.
  - Followers don't learn the set from appends. They don't need it: only a candidate counts, and it reads the record.
- **Learners.** `change_members` records the new ids as learners in `qlog/leader` at the same epoch (a CAS, so a takeover that read the record before it retries) and starts replicating to them. A takeover keeps replicating to the record's learners. A learner is a fresh follower (Phase 4's rule: an empty log starts at the leader's oldest local entry, which the bucket has everything below), it emits what's committed like any follower, and its acks never count: `advance_commit` counts only members, and the promise round asks only members.
- **The switch at a flush barrier.** Once every learner is intact and holds the commit index:
  1. A flush, so the barrier's own flush is small (`flush::Shared::request`: the flush loop applies up to the commit index and flushes at once instead of at its interval).
  2. Appends pause (submits wait up to their 500 ms, then get "busy" and resend).
  3. Drain: wait until everything appended has committed under the old set, and every learner and at least |new| - quorum(new) + 1 of the new set durably hold it. That last seq is the barrier B.
  4. Flush to B.
  5. CAS `qlog/leader` from (epoch e, old set) to (e + 1, new set, `since` e + 1). Then the leader leads e + 1 with the new set (`become_leader`), or, if it isn't in it, hands off (below).
  6. Resume. Commits are paused from step 2 to here.

  A failed CAS, or one whose answer was lost and whose record doesn't read back as ours, ends in a step-down, never a resume at e. Every abort before the CAS resumes at e with the old set and leaves the learners as learners. The change runs on its own task, so a caller that goes away (an HTTP client hanging up) can't cut it short between the CAS and the move to e + 1.
- **Why one step from the old set to the new is safe here, without joint consensus.** Joint consensus exists because, in Raft, two configurations can each make decisions during a change. Here each epoch has exactly one set, the CAS decides which, and every decision is made by a leader of one epoch with that epoch's set:
  - No entry is committed under the new set before the CAS: nobody leads e + 1 yet.
  - No entry is committed under the old set after the CAS: the leader of e appends nothing after B, and it moves to e + 1 (or steps down) in the same locked step that ends the pause. Any later leader read the record, so it uses the new set or a later one.
  - Every entry committed under the old set (at or below B) is in the bucket (step 4), so it survives whatever a later takeover asks. Beyond that, enough of the new set durably holds B that every quorum of the new set includes one (step 3), so a takeover's longest tail holds it too, and recovery from the bucket isn't needed for it.
  - So every committed entry is held by every later quorum (or the bucket), which is the takeover rule's invariant, and no seq can be committed twice. The flush alone would be enough for safety. The holders condition makes the new set able to take over from its own logs at once, and the learners' catch-up makes it able to commit at once.
- **Removed members are fenced by the epoch.** Nobody sends a removed member an append or a promise of e + 1 or later, so it never holds an entry of e + 1, never promises e + 1, and so is never counted after the switch. A takeover by it would need the record to name it, which the record no longer does, and the CAS is against the ETag of what was read, so a stale read can't win either. The members that remain promise e + 1 at its first append and refuse e from then on.
- **Replacing the leader: a handoff.** A leader that isn't in the new set CASes e + 1 naming a member of the new set that holds B (a continuing member before a learner: it has the longer local log), steps down, retires, and sends it `Lead { epoch }` (a new wire message). That member checks the record names it at that epoch, then runs the takeover's promise round directly (`Node::won`, shared with the CAS path). If the message is lost, or the old leader dies after the CAS, the members time out and take over from the record as usual (e + 2), and the named member doesn't wait out the grace other candidates give a record that names someone else.
- **No flush configured** (memory-only tests, `--flush-ms 0`): the same steps without the flushes. Safety then rests on the holders condition alone, which is the standard overlap argument.
- **What stays manual.** A change that aborts leaves its learners in the record and replicated to; the operator retries it or sets the members back. Nothing removes a dead learner on its own.
- **Salvage streaming.** Phase 4's recovery held all of (F', S] in memory before applying it. Now a task fetches committed chunks (4 MiB) from the best member and hands them over a two-slot channel; `flush::recover` applies each to the cloned state and adds it to a segment builder, PUTs a segment each time one reaches 64 MiB raw, and seals the state once the stream ends. So the recovering node holds one segment and two chunks, whatever the interval. A member that fails partway still leaves a dense committed prefix, which is kept (before, a failed fetch dropped that member's whole salvage).

- **A long recovery keeps its members quiet.** Found by the 100x run below: salvaging a 10 s interval at 100x (1.2-1.7 GB) takes 3-5 s, longer than the 1 s election timeout. Another member then took over mid-recovery, and since the first attempt had already CASed its manifest, the next one jumped another H (Phase 4's "two recoveries in a row"). A recovering candidate now re-sends its promise every heartbeat, and a member that promised that candidate's epoch treats it as hearing from its leader. With it, each kill-two is one recovery and one jump.

### Tests, chaos and numbers (Phase 5)

In-tree (`cargo test --lib qlog`, 44 tests, looped 6 times clean; the whole crate's suite passes, `fakepds selftest_small` included now that it binds free ports below the ephemeral range):

- replacing a follower under load, then killing the other old member (the leader commits with the new one alone);
- replacing the leader under load: the handoff leads epoch + 1 itself;
- 3 → 5 → 3 (two of five down and it carries on), ending on a different three;
- a learner never counts (leader and learner alone commit nothing; one member and the learner elect nobody), and a removed member, still running, never leads (it and a new member alone elect nobody);
- a crash at each of the four switch steps, replacing a follower and then the leader in turn, retried until the change lands;
- the leader cut off before the barrier and after the barrier's flush;
- a single node growing to three and back.

Every removed member is checked to hold no entry and no promise from the epoch its removal took effect at (`since`). **Mutations:** counting learners' acks against the members' quorum fails the learner test (the leader and the learner commit alone); dropping the takeover's membership check fails it too (the learner campaigns). Dropping the barrier's flush or its holders condition is masked by the other (and by the not-intact rule for a learner that isn't caught up), which is the defense in depth the argument above intends, so no test fails on either alone.

The harness (`tests/qlog/chaos.sh`) gets node slots: `SLOTS` ids (9, or more for long runs) of which `NODES` are the bootstrap members, every one with a proxy route to every other, and each id used once (a replacement is a new id on an empty disk). New scenarios: `replace-follower`, `replace-leader`, `grow-shrink` (3 → 4 → 3 → 5 → 3), `switch-crash` (nodes die at a random switch step, 35% a step) and `switch-fault` (a kill -9 of the leader, a follower or the learner, or a 3 s partition of the leader, at a random point of the change). `qlog member` drives each change and retries it until it lands. After each change the removed node stays up for 2 s and is checked against the leader's `members_since`, then retired (killed, disk gone). The checker holds removed nodes to consistency but not to the top.

Runs on benchbox at 3,500/s (10x), 5.3 KB frames, 64 hosts, commitlogs on tmpfs with a 1 ms fsync, 2 s flushes, local MinIO:

| run | changes | faults | distinct seqs | violations | removed members checked / counted after removal | manifest checks |
|---|---|---|---|---|---|---|
| replace-follower, 300 s | 5 | none | 1,050,017 | 0 | 5 / 0 | 12 + final |
| replace-leader, 180 s | 6 (all by handoff) | none | 630,017 | 0 | 6 / 0 | 7 + final |
| grow-shrink, 150 s | 7 (3 → 4 → 3 → 5 → 3 → 4 → 3 → 5) | none | 525,017 | 0 | 4 / 0 | 6 + final |
| switch-crash, 240 s | 10 | 31 crashes: 16 in catch-up, 8 before the pause, 5 after the barrier's flush, 2 after the CAS | 840,086 | 0 | 10 / 0 | 10 + final |
| switch-fault, 240 s | 10 | 5 leader partitions, 1 leader kill, 4 follower kills | 840,034 | 0 | 10 / 0 | 10 + final |
| replace-follower (smoke), 60 s | 3 | none | 210,017 | 0 | 3 / 0 | 2 + final |
| mixed-wipe (Phase 4 regression, streamed salvage), 180 s | 0 | wipes, kills, power cuts | 646,292 | 0 | | 8 + final |
| kill-two, memory only, 35,000/s, 10 s flush, 90 s | 0 | 3 kill-two | 3,346,350 | 0 | | 3 + final |

That's 8.1M distinct seqs, 41 membership changes and 38 removed members checked, with no violation. Every change landed (none needed the operator), every consumer stream was dense, and every manifest verified.

| measured | |
|---|---|
| commits paused (pause to leading the new epoch) | 6-13 ms (median 7-12 by run), 50 ms max in switch-crash: the drain ~0 ms, the barrier's flush 5-12 ms, the CAS 0-2 ms |
| emission pause, replacing a follower | none over the checker's 15 ms gap threshold in 6 of 8 changes without faults, 18-20 ms in the other two |
| emission pause, replacing the leader (handoff) | 15-49 ms (n=6): the pause, the CAS, `Lead`, the promise round and the first commit |
| emission pause, a change with a crash or a partition of the leader | ~1.0-2.1 s: the election timeout, as for any leader failure |
| command to done | 0.6 s for a removal; 1-6.6 s for an addition, mostly the learner's catch-up |
| learner catch-up at 10x | 0.4-6.1 s, growing with the leader's local log: 1.3 s for ~0.86 GB, 6.1 s for 4.2 GB (the default 4 GiB `--disk-retain-mb`), ~0.7 GB/s on loopback, while the load runs |
| ack latency in the seconds a change runs, against the others (300 s run) | median per-second p50 1.30 / 1.32 ms, median p99 1.86 / 1.76 ms, worst p99 9.5 / 2.5 ms: the catch-up's disk reads on the leader, not the pause |
| salvage at 100x, 10 s flush (memory only) | 220k-316k entries (1.2-1.7 GB) streamed into 4-5 segments; 2.9-4.6 s a recovery, of which applying them to the state is 2.3-3.7 s and the segment PUTs 0.5-0.7 s |

### What changes the study (Phase 5)

- **A membership change costs about one RTT and one small flush of commits, not "one flush plus one CAS, under a second".** The flush just before the pause makes the barrier's own flush a few ms of entries. On R2 the barrier is one manifest round (a segment PUT, a SlateDB checkpoint, the manifest CAS) plus the `qlog/leader` CAS, so budget ~0.6-1 s of paused commits there (~200 ms a round trip, Phase 4's estimate), and the pre-flush can move the segment out of the pause entirely if that matters. The CAS here is 0-2 ms on a local MinIO.
- **Replacing a box is mostly copying the leader's local log.** A learner starts at the leader's oldest local entry, so catch-up is the leader's disk window over the replication rate: 4 GiB in ~6 s on loopback, but ~35 s on a 1 Gb/s port and ~70 s on a VPS-1's 0.5 Gb/s, while it shares the leader's port with consumers. Two cheaper options if that bites: start a learner at F (the bucket holds everything below; its consumers then backfill from the bucket rather than its own disk), or rate-limit the catch-up. Neither is needed for correctness.
- **Replacing the leader doesn't cost a takeover.** The handoff makes it a 15-50 ms pause instead of the ~1 s election timeout, so rolling every box (three replacements) costs ~0.1 s of emission at 10x.
- **The bucket sees one more CAS per change** (two, with learners recorded first). Nothing on the request table moves.
- **Membership is the operator's.** A dead box is replaced by `qlog member replace OLD NEW`; nothing removes members on its own, and a change that can't catch its learners up aborts with the old set intact.
- **Recovery salvage is bounded in memory now**, one segment and two chunks however long the interval. At 100x with a 10 s flush the salvage itself takes 3-5 s, mostly the state apply, which is the same CPU the relay's re-ingest would spend. It holds the members off with promises while it runs.

### What Phase 6 needs

- **Wire `Store::counted` into the relay's quorum path and run an hour at today's rate (350/s)** to check the request-rate table (§2, "What a flush costs") against what the code sends: the flush, the state's compactor and GC polls, the manifest, and now `qlog/leader` (one GET and one CAS a takeover, two a membership change). `qlog node` already counts through `Store::counted("qlog")`, and `/qlog/status` has `requests_total`, so the hour run is `chaos.sh baseline --rate 350 --duration 3600` with `FLUSH_MS=30000` plus reading the totals.
- **Bucket retention's deletes** (the `retain/qlog` report exists): segments older than the horizon, stale checkpoints, and old state paths once nothing lists them in `external_dbs`.
- **The relay itself on the quorum log:** `cluster/forward.rs` submitting through `Client::submit_acked`, the DID state applied from committed entries, host shard assignment as log entries among the members the leader hears from (the study's "moves into the log" row), and membership reading the same `qlog/leader`.
- Optional, from the numbers above: a learner that starts at F, or a rate limit on catch-up, for hosts on a 0.5-1 Gb/s port; parallel segment PUTs for 100x flushes on R2.

### Counting (Phase 6)

`src/qlog/bucket.rs`. Every request the quorum log sends goes through vlpds's `Store::counted`, which counts it at the bottom of the stack (retries and SlateDB's own traffic included) in `vlpds_object_store_requests_total` by op, key component and client. A node now holds one counted client per purpose, all over one uncounted S3 store, so each request is billed to what sent it:

| client | what it carries |
|---|---|
| `qlog_flush` | the leader's fence, segment PUTs, the manifest CAS, checkpoint deletes |
| `qlog_state` | the leader's SlateDB: memtable uploads, the seal's checkpoint, its manifest polls, compactor, compaction worker and GC |
| `qlog_leader` | `qlog/leader` reads and CASes (takeovers, membership) |
| `qlog_recovery` | a bucket recovery: manifest, orphans, the state's clone and seal, salvaged segments |
| `qlog_backfill` | segments read for a follower behind the leader's disk, a recovery's catch-up emit, consumers' old cursors |
| `qlog_retain` | `qlog retain` and its deletes |
| `qlog_tool` | `qlog verify`, `qlog check` |

vlpds's `component()` now knows the quorum log's paths (`qlog_manifest`, `qlog_leader`, `state_*` under `qlog/state*`; segments were already `log_segment`), so a request is also labelled by what it touched. `/qlog/status` has `requests`: totals by R2 class, and by purpose, by `purpose/component` and by `purpose/op`. The node's http port serves `/metrics` (Prometheus text, the same counter with `client="qlog_*"`). Classes follow R2 and the vlpds cost model: Class A is every PUT, LIST, copy, multipart step and `DeleteObjects` POST, Class B every GET and HEAD, a single DELETE and a multipart abort are free.

One counting detail matters for the bill: object_store 0.14 sends even a single `delete` as a `DeleteObjects` POST for one key, so every SlateDB GC delete is counted as a `delete_batch` (Class A) as well as a free `delete`. vlpds counts it the same way. R2's pricing page lists DeleteObject as free and doesn't name DeleteObjects, so this is the conservative reading; it's ~0.15 A/s here (below).

`tests/qlog/chaos.sh` samples every node's status every `STATUS_EVERY` seconds into `status.jsonl`, and `report.py` turns the samples into rates per class, purpose and component, per month and in R2 dollars before and after the free tier.

### Bucket retention deletes (Phase 6)

`qlog retain --apply` deletes what the report marks deletable, after checking each item again against the manifest as it is at the time (`retain::apply`):

- **Segments past the horizon.** `pruned_seq` in `retain/qlog` is raised to the last one's end and published first, as vlpds's retention does, so a reader that checked the floor never misses data silently. Then they're deleted oldest first, so a pass cut short leaves a dense suffix. Each is below the manifest's next ordinal and at or below F, so neither a restart nor a recovery reads it again: the state checkpoint covers it.
- **Old state paths.** A path goes whole only if it isn't the manifest's, nothing the current state reads lists it in `external_dbs` (followed transitively: a clone's source can itself be a clone), and it's an older epoch's than the current path. A newer path may be a recovery's clone in progress, and an older one can't become current again: a recovery's manifest CAS is against the manifest it read, which the current one replaced. The report now applies the same rule, so it only marks what `--apply` would delete.
- **Not deleted:** stale segments past the manifest (the flush or a recovery deletes one when it reaches its ordinal; a delete here could race that and take the fresh segment written at the same key) and stale `qlog-*` checkpoints (a flush between its seal and its CAS holds one the manifest doesn't name yet; the leader clears them at its fence).

`flush::verify` reads `pruned_seq` and walks a pruned bucket from the oldest segment left, which must continue the log from the floor; a DID the state holds but the remaining log doesn't must be at or below the floor, and a host's cursor is checked against its events' run from the first one left.

`retention_deletes_only_what_it_reports_and_recovery_still_works` runs two wipe-all recoveries under load (so the state is a clone of a clone), plants a lost recovery's clone at an older epoch and one in progress at a newer epoch, applies a 1 s horizon, and checks: exactly the planned segments are gone and the next one is there, `pruned_seq` is published, the lost clone is deleted and the in-progress, current and referenced paths are kept, the manifest verifies over the pruned bucket, and a third wipe-all recovers from it with the checker clean (no holes, nothing lost) and the final manifest verified.

### Membership changes need a token (Phase 6)

`POST /qlog/members` takes `Authorization: Bearer <token>` matching the node's `--admin-token` (or `QLOG_ADMIN_TOKEN`). With no token set it's open only when `--http` is bound to loopback (the chaos harness, tests) and refused otherwise, with a warning at start. `qlog member` sends the token from the same flag or variable and stops at a 401 instead of retrying. `GET /qlog/status` and `/metrics` stay open.

### Two tests made timing-proof (Phase 6)

Both failed on the Mac on `e06da6d8`, run alone, because they asserted a condition at a fixed moment:

- `old_cursors_backfill_from_the_bucket_then_the_local_log` wanted F well behind the head after a fixed 1.6 s of load with a 700 ms flush; on a slower box the flush caught up. The flush interval is now an hour and the test flushes on request (`flush::Shared::request`) once 1,000 entries are committed, then loads until the head is 1,000 past F.
- `a_follower_behind_the_leaders_disk_catches_up_from_the_bucket` slept 4 s with the follower down and hoped the leader had trimmed past it. It now waits until the leader's memory and commitlog no longer reach the follower's last seq (`readable_floor`) and F is past it.

Both looped 5 times, and 3 more pinned to two CPUs, clean.

### The hour run (Phase 6)

Three nodes on benchbox, commitlogs on tmpfs with a 1 ms emulated fsync, local MinIO, `qlog load` at 350/s over 64 hosts (5.3 KB frames), 30 s flushes, one hour, no faults, every node's status sampled each minute (`STATUS_EVERY=60`). Rates are over the last 58 minutes (from a minute in, past the start's reads and the first fence), all nodes summed. Run 1 had the state's SlateDB at its default compactor and worker polls (5 s), run 2 at 30 s, as vlpds runs them (now the default, `--state-compactor-poll-ms`). A 15-minute run at 10x (3,500/s) used run 2's settings.

All three: 0 violations, 0 holes, nothing acked lost (1,260,001 and 3,150,017 distinct seqs), every mid-run and final manifest consistent, ack p50 1.2-1.3 ms and p99 under 2 ms. Run 1's final consumer from cursor 0 didn't run, because the harness script was edited while it ran and bash stopped at the edit after the load had finished. Its sampled counters cover the hour all the same.

| cluster-wide, per second | Class A | Class B | free | per month A / B | R2 $/mo, no free tier | R2 $/mo |
|---|---|---|---|---|---|---|
| study, today, 30 s (four DID shards) | 0.67 | 2.81 | | 1.77M / 7.39M | $11 | $3 |
| run 1, 350/s, SlateDB's 5 s polls | 0.481 | 2.398 | 0.146 | 1.27M / 6.31M | $7.97 | $1.20 |
| run 2, 350/s, 30 s polls | 0.478 | 1.660 | 0.146 | 1.26M / 4.36M | $7.23 | $1.16 |
| study, 10x, 30 s | 0.83 | 2.81 | | 2.18M / 7.39M | $13 | $5 |
| 10x, 15 min, 30 s polls | 0.634 | 1.479 | 0.045 | 1.67M / 3.89M | $8.91 | $3.01 |

By purpose and what it touched, run 2 (per second; the purposes left out sent nothing):

| purpose / component | Class A | Class B | free | study's term |
|---|---|---|---|---|
| flush / `log_segment` | 0.033 | | | segments: 0.051 |
| flush / `qlog_manifest` (the CAS) | 0.033 | | | manifest: 0.033 |
| flush / `state_manifest`, `state_gc_boundary` (retiring the last checkpoint) | 0.067 | 0.100 | | in the L0 flush |
| state / `state_sst` (L0s and compaction output; compaction's ranged reads) | 0.117 | 0.461 | 0.038 | L0 flush and compaction, four shards: 0.589 A, 1.209 B |
| state / `state_manifest` (writes, the seal's checkpoint, the 10 s poll) | 0.127 | 0.295 | 0.067 | |
| state / `state_compactions` (compactor and worker) | 0.095 | 0.257 | 0.041 | |
| state / `state_gc_boundary` (read with every latest-manifest and compactions read) | 0.003 | 0.547 | | polls, four shards: 1.6 B |
| state / `state_wal` (opening it) | 0.003 | | | |
| leader / `qlog_leader` | 0 | 0 | | 0 (peers for liveness) |
| followers, the whole hour | 0 | 3 requests each, at start | | 0 |

Of the state's 0.345 Class A, 0.146 is SlateDB's GC deleting replaced SSTs, manifests and compaction records. object_store sends each delete as a one-key `DeleteObjects` POST, counted Class A (see [Counting](#counting-phase-6)); if R2 bills that as the free DeleteObject, the hour's Class A drops to 0.33 a second. In the first six minutes, before GC's first pass (every 10 min, for files older than 5), the state sent 0.158 Class A a second; the 10x run's 13-minute window caught fewer passes (0.045 a second of deletes).

Every difference from the study, explained:

- **One state, not four shards.** The study gave the leader four DID shards, each with its own L0 flush, compaction and polls. As built it keeps one SlateDB, so the state's whole share is one shard's. That's most of why both classes come in under the study.
- **Each state costs more than vlpds's shard did.** Per flush, the state's share is ~12.4 Class A (6.0 for the L0, the seal's checkpoint and compaction output, 4.4 for GC deletes, 2 for retiring the previous checkpoint) against vlpds's 4.42 an L0 flush. The checkpoint pair is the price of sealing at exactly F (Phase 3), and the GC deletes weren't in the vlpds figure the study used.
- **Polls are ~1.26 GETs a second for one SlateDB, not 0.4.** SlateDB reads its GC boundary file with every latest-manifest and compactions read (0.55 a second on its own), and the compactor and its worker each poll the manifest and the compactions record. At SlateDB's default 5 s compactor and worker polls (run 1) the state sent 2.3 Class B a second, 0.74 more. Nothing waits on those polls here (one writer, one L0 a flush, `l0_max_ssts` 32), so the state now polls every 30 s, as vlpds does.
- **Segments are one PUT a flush at today's rate, not 0.051 a second.** A 30 s flush is ~56 MB raw, under one 64 MiB segment, and each flush cuts its own. The study added full segments by compressed bytes on top of the partial one, which double counts below 64 MiB a flush. The other way round, segments are cut at 64 MiB *raw* (Phase 3), not 64 MiB of zstd, so above that they're 1.56x what the study assumed: 8.71 a flush at 10x (0.29 a second against 0.21), ~2.8 a second at 100x against 1.8 (~$12 a month more on R2 there, before the free tier). Cutting by compressed size would close that if it matters.
- **The leader record, recovery, backfill and retention cost nothing in steady state,** as designed: `qlog/leader` is a GET and a CAS a takeover and two a membership change (Phases 1 and 5), and followers sent 3 requests each in the hour, all at start.
- **The bill:** at today's rate and a 30 s flush, R2 requests come to ~$7 a month before the free tier and ~$1 after it, against the study's $11 and $3. The bucket is still storage (72 h of log) and hosts, as the study concluded. Storage wasn't measured here: the harness's padded frames compress ~600x.

`scripts/cost_model.py` now uses these numbers (Q_DID_SHARDS 1, Q_STATE_FLUSH_A 12.4, Q_STATE_FLUSH_B 12, Q_STATE_POLL_B 1.26, segments cut per flush by raw bytes), and every table above the notes is regenerated from it. The model reproduces run 2 exactly (0.48 A, 1.66 B) and is within 20% of the 10x run's short window. What moved: 3-node HA on VPS-1s at 30 s is $19 (was $22), the single VPS-1 at 30 s $10 (was $13), §2's request table, the old-against-quorum table, and the tuning table, where 8 MiB segments at 100x now cost ~$230 more (raw-cut segments) and four shards ~3.5x one. The 60 s figures barely move (they were already inside the free tier), and the 100x totals rise ~$10 (segments).

### What changed from the study, across the phases

- **Takeover is faster when a process dies** (15-80 ms on loopback, ~0.25-0.3 s with R2's CAS) and the same ~1 s when a leader hangs or is cut off. The emit path is the firehose's 2 ms merger tick plus the RTT, not replication (Phase 1).
- **Replication CPU is small** (~6 µs an event on the leader at 100x); verify and apply still size the leader (Phase 1).
- **The commitlog ack is RTT + 1-1.7x fsync.** Fsynced disk bandwidth, not CPU, is the ceiling: 100x is borderline on consumer NVMe, and two members must never share a disk. With the commitlog, two or three process deaths are a normal takeover and RAM drops to the ring (Phases 1-2).
- **Followers keep no state.** A new leader opens the state at the last flush and replays its own log (20-30 ms to open, plus up to an interval of replay), so the bucket sees one L0 flush an interval, not three (Phase 3).
- **Exactly-F state needs the applier to pause** until SlateDB's checkpoint exists, since SlateDB only promises "at least F"; 20-350 ms a flush, off the ack path (Phase 3).
- **Flushes take ~1.4 s a GB on a local MinIO;** at 100x on R2 they want parallel segment PUTs (Phase 3).
- **Lost-quorum recovery is cheap on the bucket side** (an O(1) clone, 33-146 ms here, ~1-1.5 s on R2); the pause is detection. A survivor back within its timeout keeps its quorum. Each recovery skips H seqs, and the repeat window consumers see is the flush interval (Phase 4).
- **Membership changes pause commits ~10 ms here** (~0.6-1 s of round trips on R2). Replacing a box is mostly copying the leader's local log to it, and replacing the leader is a 15-50 ms handoff, not a takeover (Phase 5).
- **The bucket bill is lower than modeled** (~$7 a month of requests before R2's free tier at 30 s, ~$1 after, against $11 and $3), because the state is one SlateDB. Per SlateDB it's ~3x vlpds's per-shard figures: the checkpoint pair, GC deletes and SlateDB's own polls. Segments cut by raw bytes cost 1.5x the model's PUTs at 10x and up (Phase 6).
- **What the study concluded still holds:** bucket requests stop mattering, hosts are the bill, three small VPSes carry today's load at ~$18-19 a month, and a single node lands at ~$9-10.

### The relay on the log (Phase 7)

The relay runs on the quorum log, and only there (`src/node/quorum.rs`; the library side is `qlog::node::Hooks`, what the relay plugs into the log). Every node keeps its upstream sockets, verify and lanes; the lease cluster, the old single node, archival and the old PLC export reader are deleted (no migration: nothing was shipped). A node with no `--qlog-peer` is the one-member quorum, its commitlog the WAL.

- **Forwarding (slice 1).** The lane's DID owner is `QuorumOwner`. Each checked event becomes a `wire::Item`: the frame split around its seq (the leader splices its own), and meta naming the DID, host, upstream seq, the node that read it and the host owner's checks. Sixteen slots, one batch in flight each, a DID always on one slot, so a DID's events reach the leader in order. `Client::submit_events` is `submit_acked` for these batches: the same resend to the next leader, cursors under a recovery generation, and per event an outcome (appended at a seq once committed, duplicate, rejected, or "retry"). The lane acks the upstream seq on an outcome. Nothing is emitted before its entry commits.
- **The leader decides before it appends (slice 2).** `Hooks::admit` runs the relay's state apply (`StateStore::apply_held`: host authority, account status and gate, rate, `check_chain`) against the DID's record as of everything the term has appended. An accepted event is appended with its new record in the entry's meta (`log::Meta::writes`, keyed as the relay's state keys it); a duplicate or a rejection is answered without an entry. A duplicate of an entry still uncommitted is answered once that entry commits.
- **One writer, still exact F.** The quorum state's SlateDB is written only by the log's applier, which writes each committed entry's meta writes with seqnum = its seq, as in Phase 3. The leader's view is a `ShardState` over the same database whose staged records are released (to its cache, never written) once the applier has their entry. So the seal at exactly F, `verify` (state at F = the segments replayed to F: every state row) and bucket recovery all carry over. Entries flush with their cursors and meta (one segment mutation), so orphans and salvage replay into the state too.
- **Takeover.** A new leader opens the state (at or past F), stages every entry above what's applied from its own log (the adopted tail included), and only then admits; until then it answers "retry" (`not_ready`). That's the study's "promotion waits for the replay": 4-25k entries here, read from memory or the commitlog in the same step as the open (8-90 ms).
- **What's not in the log.** Unlogged record changes (failed-check counts, a desync mark from a failed check) stay in the leader's memory: a takeover loses them, and the next bad commit marks the account again. `relay_throttled` and the signing key ride in the record, so they carry over. Host counters are per node and in memory.
- **Host shards as log entries (slice 3).** The leader keeps the host table: every host, its tier, and the member that owns it. A host's owner is the live member (heard from within `--qlog-host-failover-ms`, 2 s) with the highest rendezvous hash, and only a dead owner's hosts move. A changed row rides the next appended entry as a state write (`h/{host}`), so it's in the state at F, replayed by `verify`, and a new leader starts from the old table (no reshuffle at a takeover). Members read the table from the leader every `--qlog-host-poll-ms` (500 ms; `Ask leader:hosts`), which also carries each host's newest committed cursor: a node that gains a host resumes it from there. A node that admits a host (`--host`, requestCrawl, the policy's tier changes) sends the row with its next submit until the table has it. The row also carries the record's policy fields (operator throttle, account cap, the tier a ban restores, the action trail), so every member enforces the same ones; a member re-applies a host whose tier or policy a new table moved at once, and an operator's action sends its row at once and waits to read it back (admin-api.md, "Host actions").
- **Membership from `qlog/leader` (slice 4).** The relay's members are the record's: host assignment counts them, and the dashboard's cluster view is each member's status (role, hosts owned, its own rates, CPU and memory, asked over the peer protocol). `NodeAdmin::quorum` asks every member's `status`, and `change_quorum_members` sends the change to the leader with the qlog admin token. The rest of the admin API answers for the node it's asked: consumers, host actions and the pipeline are that node's, accounts are the leader's (a follower names it), and operator takedowns are entries: the leader sets the record's flag and appends the `#account` it makes.
- **Retention as a loop (slice 5).** Each leader term runs `retain::plan`, `apply` and `write` every `--qlog-retain-every-secs` (600) with `--qlog-retain-hours` (72; 0 turns it off), the same plan and re-checks as `qlog retain --apply`, billed to `qlog_retain`. `the_leader_runs_bucket_retention_on_a_loop` prunes under load and verifies what's left.
- **The firehose starts at the first emission.** A node makes its firehose (and its backfill floor) at the first entry it emits, as Phases 1-6 did, so a node wiped past a recovery gap serves old cursors from the bucket across every gap rather than skipping the seqs between the bucket's F and its first emission.

- **The PLC export and host discovery as leader jobs.** The export reader (`plc_seed`) and host discovery (`discovery`: other relays' `listHosts` and the export's PDS hosts, through this relay's own admission) run on the leader, one term at a time, with their progress in the bucket (`plc/export-checkpoint.json`, `discovery/state.json`) so a new leader resumes them. Both are caches of what the network says, not the log's state: the export's seeds live in their own SlateDB (`plc/seeds`, every member reads it), and discovery only feeds admission.

Four bugs the chaos found, each now covered:

- **Two sockets on one host** (a node that had just started subscribed to every host before its filter landed, and a moved host's old owner keeps reading until it polls) interleaved a DID's events at the leader: prevData mismatches, then the account desynchronized. The leader admits a host's events only from the member its host table names (`not_owner`, which the old owner's lane takes as a fenced socket), and a node reads nothing until the leader gives it hosts.
- **A record released to the cache after it left pending**: a load between the two found neither and read the previous record from the cache. `ShardState::release` caches before it lets go, under pending's lock.
- **Decide-then-append across two batches.** A batch slow on identity lookups timed out at the host owner and was resent; the copy answered its DID's event as a duplicate of the still-unappended original, the next batch's event appended first, and the original landed after it: a rev out of order in the stream. The leader locks every DID of a batch (in stripe order) from deciding until appended (`Admission::hold`), and decides DIDs in parallel within a batch. `the_leader_admits_each_dids_events_once_and_in_order` races copies of batches against slow decisions; with per-event locks it fails every time, with the hold it passes.
- **A wiped node skipped seqs on backfill.** With the floor taken from the bucket's manifest at start, a node wiped while the cluster was past a recovery gap served live consumers from its first emission and skipped the seqs between. The lazy firehose above fixes it; `wipe-two` covers it.

### Tests, chaos and numbers (Phase 7)

`tests/qlog/relay-chaos.sh` (`just relay-chaos`) runs three `vlrelay` nodes on a local MinIO, each restarted by a supervisor, with commitlogs on tmpfs and a 1 ms emulated fsync, fed by `fakepds` (16 hosts, 200 accounts each, commits, `#identity`, `#account` and `#sync`, the fake PLC on the side), 2 s flushes. Every node's `subscribeRepos` is consumed from cursor 0 by `qlog check --relay-frames` (no seq emitted twice or out of order on any node, nothing acked lost, every node's stream the same), `qlog verify` runs every 20 s and at the end (the state at F is the segments replayed to F), and `e2e_check` compares every upstream event with what the relay emitted (each DID's commits once and in rev order, `#identity` and `#account` too). A run fails on any of them.

At 3,500 events/s (10x), 90 s each, on the builds after the old mode was removed: 0 violations, 0 holes, nothing acked lost and nothing missing upstream to relay, in every scenario (about 30M events observed across the runs):

| scenario | faults | emission pause or switch |
|---|---|---|
| kill-leader | kill -9 every 15 s | 73-75 ms median, 137 ms max (n=14) |
| power-cut-leader | SIGUSR1 power cut | 94-95 ms median, 155 ms max |
| pause-leader | SIGSTOP 3 s | 1.02 s median |
| partition-leader | isolated 5 s | 1.01 s median |
| kill-follower, down-follower | kill -9, down 6 s | 0 |
| kill-two, power-cut-all | quorum lost, then back | 1.8 s, 2.5 s median |
| wipe-two, wipe-all | commitlogs gone: bucket recovery | 2.4 s, 2.3 s median |
| single-kill, single-wipe | the one-member log | 1.4 s, 1.3 s median |
| replace-follower, replace-leader | 4 replacements each | 1.1-2.8 s command to done, longest pause 65 ms |
| grow-shrink | 3 -> 4 -> 3 -> 4 -> 3 | 0.7-2.2 s, longest pause 77 ms |
| flush-crash | kill -9 at random flush steps | |
| mixed-wipe, mixed-durable, mixed-flush, baseline | a random fault every 15 s | |

The first passes found one relay bug and three harness ones. `wipe-two` failed before the firehose went back to starting at the first emission (live consumers of a node wiped past a recovery gap skipped the seqs between the bucket's F and its first emission); it passes since. A mid-run `verify` that read the manifest just before a recovery's landed flagged the recovery's segments (above R, so from a later generation; `verify` now stops there). And the harness gave the proxy's routes ports that overlapped fakepds's with nine slots (so `replace-*` and `grow-shrink` never elected), and let `flush-crash` crash a node in the run's last second, leaving its hosts' tail unread when the fleet stopped (142 commits "missing"); crashes now stop with the other faults, 12 s before the end.

The PLC export ran through the same scenarios (`PLC_EXPORT=1`, 150 s, kill-leader, power-cut-leader, mixed-durable) against fakepds's `/export` with 200,000 ops and a 429 on every sixth request, at 4 requests a second: in kill-leader the export changed hands eight times mid-history, each new leader resuming from the checkpoint (21,979, 42,959 ... 187,820 ops read before it), and the ninth caught up; every run ended caught up, with the relay's checks unchanged. Host discovery ran the same way (`DISCOVERY=1`, kill-leader and power-cut-all): fakepds's `listHosts` served the 16 fleet hosts and 40 that don't answer, one a page with a 429 every sixth request. The list was read to the end both times (56 seen, 16 known, 40 refused, 10-11 429s waited out), with the run taken over mid-list by a new leader 2 and 4 times, each resuming from the saved cursor. `the_leaders_job_survives_a_takeover_mid_export` does the same on a three-node log in-process (and a member's cache then fills from the seeds without a PLC lookup), and `a_new_leader_resumes_the_export_from_the_checkpoint` checks that the new writer fences the old one.

**The hour run.** Phase 6's profile with the relay on top: three nodes on benchbox, commitlogs on tmpfs with a 1 ms emulated fsync (`fsync` mode, the build before durability modes), local MinIO, fakepds at 350/s over 64 hosts (commits, `#identity`, `#account`, `#sync`; ~4.9 KB frames), 30 s flushes, one hour, no faults, `STATUS_EVERY=60`. It passed:
- 0 violations, 0 holes and 1,318,748 seqs, every one emitted on every node.
- 146 mid-run verifies, all consistent.
- e2e missing 0: every upstream event in the relay once, in rev order.
- A final consumer from cursor 0 read the whole log from the bucket in 7.1 s.
- Leader append to quorum commit: p50 1.34 ms, p99 2.61.
- Upstream receive to emit: p50 3.6 ms, p99 6.2.
- CPU: the leader 0.115 cores, followers 0.06.

| cluster-wide, per second | Class A as counted | of it, one-key `DeleteObjects` (GC) | Class A billed | Class B | free |
|---|---|---|---|---|---|
| Phase 6 run 2, `qlog load`, 30 s polls | 0.478 | 0.146 | 0.332 | 1.660 | 0.146 |
| Phase 7, the relay | 0.521 | 0.183 | 0.338 | 1.739 | 0.183 |

"Billed" leaves out the GC's one-key `DeleteObjects` POSTs, which the counter files as Class A and which, per Cloudflare's own figures for the R2 hour, R2 doesn't bill as Class A. Prices are left to `scripts/cost_model.py`.

| purpose / component, Phase 7 | Class A | Class B | free |
|---|---|---|---|
| flush / `log_segment` | 0.035 | | |
| flush / `qlog_manifest` | 0.034 | | |
| flush / `state_manifest`, `state_gc_boundary` | 0.067 | 0.101 | |
| state / `state_sst` | 0.130 | 0.480 | 0.052 |
| state / `state_manifest` | 0.143 | 0.310 | 0.081 |
| state / `state_compactions` | 0.106 | 0.276 | 0.050 |
| state / `state_gc_boundary` | 0.003 | 0.573 | |
| leader, followers | 0 | 0 | |

The flush's share is Phase 6's exactly (one segment and one manifest CAS a flush, the checkpoint pair). The state's is ~10% more on each class. Its SSTs now hold the relay's real records (12,864 accounts with their chains and keys, the host table, the throttle index) rather than the stand-in's one row per DID. So each flush's L0 and the compactions behind it are bigger, and GC deletes more files (0.183 a second against 0.146). Followers sent nothing in steady state, as before.


### Durability modes (Phase 7)

`--durability` says when an entry counts on a node: when a follower acks it and when the leader counts itself toward the quorum.

| mode | an entry counts | a process crash | a power cut | default for |
|---|---|---|---|---|
| `fsync` | after its commitlog fdatasync | loses nothing | loses nothing acked | one node (the only mode it runs) |
| `page-cache` | once written to the commitlog (the page cache); fdatasync'd in the background every `--durability-sync-ms` (100) | loses nothing: the kernel still holds the pages | can lose the last ~100 ms of acked writes on that box | three members or more |
| `memory` | in memory, no commitlog | the node comes back empty | the same | |

Promises are fdatasync'd before they're answered in every mode.

**The safety argument.** Jaz's two rules stay: a seq is never reused, and nothing is emitted before a quorum holds it (in the mode's sense: a majority's disks, page caches or memory). What changes is which failures a node survives with its log whole, so the rule that decides it is this: *a node counts as intact (vouches for its log) only if its log still holds everything it ever acked*.

1. **Epochs.** A promise is on disk before it's answered, in every mode, so a node never promises an older epoch after a newer one, power cut or not. A takeover still needs a quorum of promises and the `qlog/leader` CAS (Phases 1 and 5), so two leaders never hold one epoch.
2. **Who is intact.** In `fsync` mode every acked entry was synced first, so a restarted node is intact. In `page-cache` mode a process crash leaves the page cache, and with it every write, so the node is intact too. A power cut can drop the unsynced tail, which may hold entries the node acked. The node can't tell that from the file (a short log looks like any log). So the commitlog directory records the boot id and the mode of the run that wrote it, fsynced at open. When the next open finds the previous run wrote in `page-cache` mode and the boot id has changed (or the emulated power cut's marker is there), the node keeps its log to catch up from, but starts *not intact*. A wiped disk has always started that way. It becomes intact again only once it holds the leader's commit index from when it rejoined, which by then includes every committed entry. In `memory` mode a restarted node is empty, and not intact for the same reason.
3. **Commits and takeovers count only intact logs.** The leader counts an entry committed once a majority of intact members hold it (Phase 1's `peer_intact`). A takeover needs promises from a quorum of intact members, and adopts the longest log among them. A committed entry sits in the logs of a majority that were intact when they acked it. Any later takeover quorum of intact logs meets that majority in a node that is still intact: a node that lost power since was excluded in step 2. So the new leader holds every committed entry, and nothing emitted is reissued.
4. **When no quorum of intact logs can exist** (power cut on a majority within the sync window in `page-cache` mode; a majority of `memory` nodes restarting), the candidate runs the bucket recovery (Phase 4), exactly as for lost disks:
   - It starts from the state at the manifest's F, keeps any committed entries an intact survivor holds past F, and jumps to R + 1.
   - No seq above R was ever emitted while that manifest was current, so no seq is reused.
   - The gap up to R is recorded. Events emitted inside it are re-ingested from the recovery's cursors and reach consumers again under new seqs. Consumers already dedupe those by rev.
5. **The mutation.** With the check in step 2 off (`--qlog-unsafe-trust-log`), nodes trust logs that lost acked entries after a power cut on all three. They elect a leader from those logs, and it hands out seqs that were already emitted with other contents. `trusting_a_short_log_after_a_power_loss_is_caught` shows the checker reports exactly that, and so does the relay chaos: `power-cut-all` with `TRUST_LOG=1` failed with 257 seqs "emitted with two contents (emitted, lost and reissued)". (`power-cut-majority` under the mutation passed: the third node's whole log usually wins the election, so it doesn't reliably expose the bug.)

Residual risks:

- A power cut on a majority within ~100 ms of each other in `page-cache` mode is a bucket recovery with a gap rather than a normal takeover. It never loses anything silently.
- A disk that loses data after an fdatasync (no power-loss protection, lying firmware) breaks `fsync` mode the same way. It's the reason the cluster wants three boxes on three disks.
- A machine that stops without rebooting, without power loss and without killing the process (a frozen VM resumed later) keeps its page cache. That's a pause, not a loss.

**Chaos, by mode.** The full relay suite ran three times, once per mode (`DURABILITY=fsync|page-cache|memory`, 24 scenarios each, 3,500/s, 90 s):

- **All 72 runs passed.** 0 violations, nothing acked lost, and e2e missing 0 throughout. That includes `power-cut-majority`, new here: the leader and one follower lose power at once.
- **What differs is which faults end in a bucket recovery, as the argument says.**
  - `kill-all` (every process killed at once) is a plain takeover in `fsync` and `page-cache` (1.9-2.3 s, the supervisor's restart plus an election) and a recovery in `memory`.
  - `power-cut-all` and `power-cut-majority` are recoveries in `page-cache` (2.7 s and 2.1 s median) and `memory` (2.3 and 1.6 s), with their gaps recorded and every event re-ingested. In `fsync` they're takeovers.
- **The kill -9 pause differs too, and not because of the takeover.** From the port refusing to the new leader leading takes 2-4 ms in every mode. What changes is how long the killed process takes to release its port, the follower's cue to take over: 40-56 ms in `fsync`, 72-85 in `page-cache`, 113-139 in `memory`. That grows with what the process holds in memory (a `memory` node keeps 512 MiB of log), so the pause medians are 73, ~140 and ~200 ms. A hung leader is caught by the 1 s election timeout in every mode.

**Numbers, by mode.** `tests/qlog/chaos.sh baseline` on benchbox, three `qlog node`s, ~5.3 KB frames:

- **Ack.** Commitlogs on tmpfs with a 2 ms emulated fsync (`FSYNC_DELAY_US=2000`), 60 s at 3,500/s and at 35,000/s. "Ack" is submit to quorum ack, client view; "consumer" is submit to the first consumer on any node. Values are p50 / p99 in ms.

| mode | ack, 3,500/s | consumer, 3,500/s | ack, 35,000/s | consumer, 35,000/s |
|---|---|---|---|---|
| `fsync` | 2.28 / 3.06 | 3.43 / 4.82 | 2.89 / 4.47 | 4.26 / 6.15 |
| `page-cache` | 0.24 / 0.82 | 1.41 / 2.70 | 0.80 / 1.66 | 2.13 / 3.63 |
| `memory` | 0.20 / 0.76 | 1.45 / 2.67 | 0.61 / 36.5 | 1.98 / 40.6 |

- **Ceiling.** The three commitlogs share benchbox's one consumer NVMe, with 30 s flushes to the local MinIO and 200,000/s offered until the load's backpressure held it. Committed is distinct seqs over the load's seconds; the load's own "acked" undercounts past its submit timeout.

| mode | committed | notes |
|---|---|---|
| `fsync` | ~18,000/s (2.06M seqs in 114 s) | group fsyncs p50 23-28 ms, ~800 events each; the disk is the limit, as in Phase 2 (~25,000/s there with no flush competing for it) |
| `page-cache` | ~35,500/s (1.71M seqs in 48 s) | background fsyncs fall to 110-350 ms p50 behind ~180 MB/s a node; the ack path never waits on them |
| `memory` | | no number: at this offered rate the nodes' and the load's backlogs reached the run's 24 GiB memory cap, which stopped the run |

What it says:

- **`page-cache` takes the fsync off the ack**: ~10x faster acks at today's rate (0.24 ms against 2.3 at a 2 ms fsync) and about 2x the ceiling on one shared disk. That's within ~0.05-0.2 ms of `memory` until 35,000/s.
- **`memory`'s p99 at 35,000/s** (36 ms) is the in-memory log's trimming and the larger heap, which `page-cache` doesn't carry (its log keeps 64 MiB in memory against 512).
- **What `page-cache` gives up** is one case: a power cut on a majority within ~100 ms becomes a bucket recovery with a gap, rather than a takeover. On separate boxes on separate power, that's a correlated failure of a different order than a process crash, which `page-cache` survives like `fsync`. So it's the default for three or more, and `fsync` stays for one node, where the commitlog is the only copy.


### Real R2 hour

Phase 6's hour again (`chaos.sh baseline`: three nodes on benchbox, commitlogs on tmpfs with a 1 ms emulated fsync, 350/s over 64 hosts, 30 s flushes, status each minute), against a dedicated R2 bucket (location hint `wnam`) instead of MinIO, on 2026-10-06 from 21:07:57 to 22:08 UTC. Both request guards were armed (`tests/qlog/R2_HOUR.md`): 3,500 A / 12,000 B a run, 2.4 A / 8.3 B a second over 120 s, 250 A / 300 B in any 10 s, and a watchdog polling every node each 10 s. Neither tripped. The checker passed: 1,260,001 distinct seqs, 0 violations, 0 holes, nothing acked lost, and the consumer from cursor 0 read all of it back, dense, in 6.0 s. Acks were the same as on MinIO: p50 1.20 ms, p99 1.68 ms, max 32 ms.

**Requests: the same as on MinIO.** Counted over the last 58 minutes, all nodes:

| cluster-wide, per second | Class A | Class B | free (deletes) |
|---|---|---|---|
| Phase 6 run 2, MinIO | 0.478 | 1.660 | 0.146 |
| R2, counted | 0.478 | 1.725 | 0.148 |
| R2, counted, less one-key `DeleteObjects` (what R2 bills) | 0.330 | 1.725 | |
| `cost_model.py`, today at 30 s (GC deletes now left out) | 0.33 | 1.66 | |

By purpose the split matches run 2's to within ~0.04 a second everywhere: B is 4% higher, from SlateDB's GC-boundary and compactions reads (0.579 and 0.293 B/s, against 0.547 and 0.257). The followers sent 3 requests each, at start. Whole run (the in-node totals at exit): 1,983 A, 6,802 B, 754 free deletes.

**Cloudflare's count.** infra read the bucket's metrics in the dashboard. For a window where only the three nodes ran (21:30:00-22:00:00, counters interpolated between 10 s polls, under 2 requests of error at each end):

| 21:30-22:00 | Class A | Class B |
|---|---|---|
| counted | 1,006: PUT 140, PUT create 334, PUT CAS 66, LIST 75, one-key `DeleteObjects` 391 | 3,277: GET 2,365, ranged GET 908, HEAD 5 |
| Cloudflare (legend total; the tile showed 650 A, 3.42k B with an edge minute) | 626 | 3,280 |

- **R2 doesn't bill a one-key `DeleteObjects` as Class A.** 1,006 - 391 = 615, Cloudflare 626. So SlateDB's GC deletes are free on R2, like `DeleteObject`, and the bill is ~0.33 A a second, not 0.48. `scripts/cost_model.py` now leaves them out (`Q_STATE_FLUSH_A` 8.0, was 12.4), and every table above is regenerated: 3-node HA on VPS-1s at 30 s is $18 (was $19), and at 10 s R2 requests are ~$7 a month after the free tier (was ~$13). S3 and GCS don't bill deletes either. The guards still count them as Class A: it's conservative.
- **The counters miss no Class B.** 3,277 against 3,280. Every request a node sends goes through `Store::counted` above object_store's own retries, and none were logged (object_store logs a retry at info, which the nodes log).
- **The day's totals** (22:14, last 24 h): 1.32k A, 9.46k B. A fits: the hour's 1,860 counted by then, the wiring run's 63 and a few `mc` LISTs, less ~650 one-key deletes. B was ~2,500 over. That was the hour's final `qlog verify`, which runs against the bucket in a process of its own, outside both guards. From 22:08:40 it read the state at ~8-9 GETs a second. It was killed by hand at 22:20:58, after ~12 minutes and ~6,000 GETs. Since then, `qlog check`, `verify` and `retain` take the guards' flags, each with a cap of its own and a state file the watchdog adds to the cluster total, a tool that trips stops the run, and `r2_hour.sh` skips the final verify on R2 (the consumer from cursor 0 checks the log). See tests/qlog/R2_HOUR.md and the `tool-verify` case of `r2_guard_test.sh`.

**The guards' margins.** The largest 10 s poll interval was 89 A (a GC pass: R2 spreads each over ~20 s, 59-89 A in each of two polls, every 10 minutes) and 131 B, against 250 and 300. The largest 120 s mean was 1.54 A and 3.93 B a second, against 2.4 and 8.3. The total was 1,983 A and 6,802 B, against 3,500 and 12,000. With R2 billing ~0.33 A a second, the budget has more headroom on Cloudflare's count than on ours.

**R2 round trips**, from benchbox, from the nodes' `vlpds_object_store_request_seconds` (to the response head for GETs, the first page for LISTs). The histogram's buckets double, so these are ranges:

| op | requests | p50 | p90 | p99 | max |
|---|---|---|---|---|---|
| GET | 5,074 | 51-102 ms | 102-205 ms | 410-819 ms | 3.3-6.6 s |
| ranged GET | 1,711 | 102-205 ms | 205-410 ms | 410-819 ms | 0.8-1.6 s |
| HEAD | 16 | 51-102 ms | 102-205 ms | | 102-205 ms |
| LIST | 165 | 102-205 ms | 205-410 ms | | 205-410 ms |
| PUT | 272 | 205-410 ms | 410-819 ms | 3.3-6.6 s | 6.6-13.1 s |
| PUT create | 660 | 205-410 ms | 205-410 ms | 0.8-1.6 s | 3.3-6.6 s |
| PUT CAS | 132 | 205-410 ms | 410-819 ms | 0.8-1.6 s | 1.6-3.3 s |

So reads are at or under the ~200 ms round trip the estimates assumed, and writes are 1-2x it (a PUT's p50 is 205-410 ms), with a long tail.

**Flushes take seconds on R2, not tens of milliseconds.** 120 flushes, seal to CAS: p50 2.35 s, p90 3.34 s, p99 10.2 s, max 11.4 s (MinIO at 350/s and 30 s: 74 ms p50, 80 ms max). The applier's pause for the seal: p50 0.92 s, p99 7.9 s, max 8.5 s (MinIO: 21 ms). A flush sends ~20 requests (12.5 GETs, ~6 PUTs, a LIST), many of them one after another: the seal's L0 and checkpoint writes, then the segment PUT, then the manifest CAS. The tail is R2's PUT tail. Acks don't wait for either: the seconds with a flush had a worst per-second ack p99 of 23 ms and a max of 32 ms (2.99 ms and 6.9 ms in the seconds without). Consumers do wait for the applier: submit to a node's consumer was p50 8.6 ms and p99 38 ms. At 30 s that's comfortable: even the 11 s outlier left 19 s. At a 10 s flush the p99 would eat the interval, so on R2 30-60 s stays the choice.

What that does to the estimates that assumed ~200 ms round trips:

- **Recovery after a lost quorum** (~6 round trips, mostly writes): ~1.5-2.5 s on the bucket side, not ~1-1.5 s. Detection still dominates the study's ~5 s.
- **A membership change** (a manifest round plus the `qlog/leader` CAS) pauses commits ~1-1.6 s on R2, not ~0.6-1 s, and up to several seconds on a PUT in the tail. Moving the barrier's segment out of the pause (the pre-flush) is worth more than it looked.
- **Takeover's CAS** (~50-300 ms assumed) is a PUT CAS at 205-410 ms p50 and up to 1.6 s at p99.

**Storage:** Cloudflare averaged 20.45 MB over the day. The run's padded frames compress ~600x, so as in Phase 6 this says nothing about the log's real size.

### What's left

- **Learners at F:** a new box could start at F and read below it from the bucket, rather than copying the leader's local log, or the catch-up could be rate-limited, for 0.5-1 Gb/s ports.
- **Parallel segment PUTs** for 100x flushes on R2 (and, if the PUT count matters there, cutting segments by compressed size).
- **The sync API's repo endpoints on followers** (the records are the leader's).
- **Real hosts,** with Jaz's OK: fsync on OVH and Hetzner VPSes and the 3-box run in [Measuring a real host](#measuring-a-real-host-ovh-hetzner). (The R2 hour is done: [Real R2 hour](#real-r2-hour).)

### Measuring a real host (OVH, Hetzner)

Measured so far: vps1 (an ex-vlpds OVH VPS) as a single node, [below](#vps1-one-ovh-vps-single-node), and one fsync run of a real 3-box cluster (vps1, benchbox and devhost over Tailscale), [below that](#real-3-box-run), which hit Tailscale's userspace ceiling on vps1 (R2: [Real R2 hour](#real-r2-hour)). To run on candidate hosts, with Jaz's OK:

1. **fsync**, on each box, on the disk the commitlog would use (no root, needs `fio`):
   ```
   tests/qlog/fsync_probe.sh /var/lib/vlrelay/probe
   ```
   It prints fdatasync p50, p99 and p99.9 for 4 KiB, 64 KiB and 1 MiB appends with one writer, 64 KiB with three, and the unsynced write ceiling. Run it three times at different hours: VPS disks are shared. A VPS whose fdatasync is well under 0.1 ms is acknowledging from a cache (hypervisor or controller), so don't count on it surviving a host power cut.
2. **The cluster**, three boxes (an OVH VPS-2 in each of three locations, or three Hetzner CX33/CAX21 in a spread placement group):
   - Build `qlog` for the target's CPU and copy it over. `.cargo/config.toml` builds for `target-cpu=native`, and a benchbox (Zen 5) binary dies with SIGILL on a VPS's Haswell vCPU: `RUSTFLAGS="-C target-cpu=haswell" CARGO_TARGET_DIR=<a separate dir> cargo build --profile dev-release --bin qlog`, then `objcopy --strip-debug` (330 MB to 39 MB). Or `docker buildx` for linux/amd64 on the Mac.
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
3. **A single node** needs no docker or root: `tests/qlog/vps1/run_single.sh` runs one `chaos.sh` scenario on the host. It uses a userland MinIO (the binary from `vlpds-minio:local`, since dl.min.io no longer serves it) and puts the commitlog on the host's disk. It refuses to start with under 12 GB free, and deletes the run's data afterwards. `chaos.sh` takes `S3_ENDPOINT` for a MinIO it didn't start, and its timestamps work with Ubuntu 26.04's uutils `date`, which ignores `%3N`.

#### vps1: one OVH VPS, single node

The host: an OVH VPS in Us-west (the decommissioned vlpds-node1). It has 2 vCPU (KVM, "Intel Core Processor (Haswell, no TSX)"), 3.8 GB RAM and no swap. The disk is a 40 GB virtio `QEMU HARDDISK` (ext4, write cache "write back", `discard`), running Ubuntu 26.04 and kernel 7.0. Measured 2026-10-06 from 17:15 to 19:20 UTC. RTT from benchbox is 4.5 ms (same metro).

`fsync_probe.sh`, four runs over two hours (p50 / p99 in ms):

| | 17:15 | 17:48 | 18:04 | 19:18 |
|---|---|---|---|
| fdatasync 4 KiB, 1 writer | 0.61 / 0.94 | 0.57 / 0.95 | 0.59 / 0.86 | 0.62 / 0.90 |
| fdatasync 64 KiB, 1 writer | 0.68 / 0.95 | 0.63 / 1.07 | 0.65 / 1.07 | 0.66 / 0.91 |
| fdatasync 1 MiB, 1 writer (MiB/s) | 1.12 / 2.15 (548) | 1.09 / 2.25 (548) | 1.12 / 1.53 (519) | 1.16 / 1.50 (568) |
| fdatasync 64 KiB, 3 writers (MiB/s) | 0.86 / 1.42 (175) | 0.86 / 1.35 (179) | 0.87 / 1.38 (173) | 0.86 / 1.38 (178) |
| unsynced write ceiling | 1,565 MiB/s | 1,499 | 1,428 | 1,491 |

That's steady across two hours and well above 0.1 ms, so the flush really reaches the hypervisor's storage, not just a cache. It's 0.6-0.7 ms at commitlog-sized appends, the "0.5-2 ms VPS" the study assumed and 4x better than benchbox's consumer NVMe (2.7 ms). Three writers cost ~0.2 ms more, against 2x on benchbox. The 1 MiB rate (~550 MiB/s fsynced) is past what one member needs at 100x (~185 MB/s).

Single node (`run_single.sh`): the commitlog on the disk, MinIO on the same disk, 30 s flushes, a 256 MiB ring. The load generator and the checker ran on the box too, so all four share its 2 vCPU. Frames ~5.3 KB, ack = submit to the node's fsynced ack, loopback:

| | 350/s | 3,500/s | 30,000/s | 45,000/s | 60,000/s |
|---|---|---|---|---|---|
| ack p50 / p99 | 1.16 / 2.43 ms | 1.58 / 4.23 | 4.05 / 11.5 | 15.4 / 456 | saturated (commit p50 310 ms, p99 8.2 s) |
| submit to the consumer, p50 / p99 | 8.6 / 38 ms | 3.2 / 6.6 | 6.6 / 16.8 | 23 / 899 | |
| commitlog fsync p50 / p99 | 0.83 / 1.58 ms | 0.93 / 2.43 | 1.34 / 3.66 | 2.65 / 12.8 | 8.6 / 73 |
| events a group commit | 2 | 18 | 150 | 225 (p99 4,275) | 2,101 |
| node CPU (cores), max RSS | 0.11, 360 MB | 0.27, 527 MB | 0.86, 655 MB | 1.27, 959 MB | 1.14, 1.8 GB |
| checker | PASS | PASS | PASS | PASS | none: the kernel OOM-killed a 1.7 GB qlog process, the checker (the node kept running) |

- **The ceiling is ~30,000-40,000/s with everything on 2 vCPU,** about 85x today's rate. Above that the box runs out of CPU, not disk: 45,000/s is 240 MB/s fsynced, under half the fio rate. A dedicated node with its load arriving over the network would go higher. So 10x is easy on this class of VPS, and 100x is about the limit of a 2 vCPU box (a VPS-2 or bigger has more cores).
- **Against the emulated rows.** At 3,500/s the commitlog fsync is 0.93 ms. The nearest benchbox row, "tmpfs + 1 ms", has a 3-node ack of 1.26 / 1.92 ms. vps1's single node acks at 1.58 / 4.23, with no replication RTT but with the load generator, checker and MinIO competing for 2 vCPU, so its p99 is the CPU's, not the disk's. The "ack ≈ RTT + 1-1.7x fsync" rule holds: 1.58 ms is 1.7x 0.93.
- **At 350/s the consumer sees an event 8.6 ms after submit** (p99 38), against 3.2 ms at 3,500/s. The ack is 1.2 ms at both rates, so this is the emit side waking idle vCPUs, not the disk. That's worth a look on a real node: an idle merger tick pays a VPS's wake-up latency.
- **Restarts.** kill -9 under 3,500/s: the emission pause was 1.6-3.2 s (median 2.4 s, n=5). A power cut (a random part of the unsynced tail lost, plus a torn record) paused 1.6-2.8 s (median 2.1 s, n=5). Both include the harness's 1 s restart delay, and recovery replayed 1.8 GB of commitlog. On benchbox the same single-node restart is 1.2-1.5 s. Both runs: PASS, nothing acked lost, the consumer from cursor 0 dense and matching.
- **Over the internet.** The node on vps1, driven from benchbox through an ssh tunnel (4.5 ms RTT): ack p50 / p99 5.97 / 8.66 ms at 350/s and 6.79 / 11.6 ms at 3,500/s, so RTT + ~1.5-2.3 ms. That's what a host owner in the same metro sees.

What it changes: nothing in the study's conclusions. The VPS fsync it assumed is right (0.6-0.9 ms, steady). A 2 vCPU VPS carries 10x on one node with room to spare. The 3-box run still has to measure replication across real hosts.

#### Real 3-box run

Run on 2026-10-07 from 02:20 to 02:28 UTC, and cut short: Jaz spun multi-host testing down after the first run. The cluster was n1 on vps1 (the OVH VPS above), n2 on benchbox and n3 on devhost (a lab VM: 16 vCPU, a `QEMU HARDDISK` on LVM). The members talked over Tailscale. MinIO for `qlog/leader` and the bucket, the load generator and the checker all ran on benchbox. All three boxes ran one Haswell build. Each commitlog was on its own box's disk. Flushes ran every 10 s. `tests/qlog/3box/run.sh` puts n1 first in `--members`, so with no leader yet vps1 campaigns first. It then runs 180 s of steady load, and kills the leader 15 s into a 60 s load, restarting it 1 s later. `3box/report.py` splits `report.py`'s output into those two parts. devhost has no fio and no root, so `tests/qlog/fsync_probe.py` runs the same tests in Python. On vps1 it read within ~5% of fio.

| fdatasync p50 / p99 | 4 KiB | 64 KiB | 1 MiB (MiB/s) | 64 KiB, 3 writers |
|---|---|---|---|---|
| vps1 (fio) | 0.59 / 0.91 ms | 0.70 / 0.97 | 1.17 / 1.73 (515) | 0.90 / 1.52 |
| devhost (python) | 1.04 / 7.17 | 1.15 / 1.84 | 2.06 / 3.02 (289) | 2.37 / 4.51 |

| tailnet RTT, 200 pings | p50 | p99 |
|---|---|---|
| vps1 - devhost | 5.19 ms | 6.45 |
| benchbox - vps1 | 5.67 | 7.64 |
| benchbox - devhost (one LAN) | 0.93 | 2.71 |

Every path was direct, not DERP.

**fsync durability, 3,500/s, vps1 leading: a Tailscale ceiling, not a relay one.** At 3,500/s a leader sends ~37 MB/s of frames (5.3 KB each, to two followers) and takes in ~19 MB/s of submits. On vps1 all of that goes through tailscaled, and wireguard-go encrypts in userspace. tailscaled sat at 145% of vps1's 2 vCPU while qlog used 27%. vps1's tailnet egress topped out at ~21 MB/s. Followers missed heartbeats and called elections. vps1 won back epochs 3 and 5, then benchbox took epoch 6, 75 s into the run. Until then the client ack was 0.9-4 s p50, the load retried 114,266 submits, and benchbox's follower fell ~18k seqs behind. Local runs sustain 3,500/s and far more ([Phase 2](#tests-chaos-and-numbers-phase-2), [vps1 single node](#vps1-one-ovh-vps-single-node)). So the limit is userspace WireGuard on a 2 vCPU box: a leader there wants a cheaper transport (kernel WireGuard, or TLS between members) or more cores. That wasn't tried here.

**The same run once benchbox led** (followers devhost, 0.9 ms away, and vps1, 5.7 ms away), for the last 105 s at 3,500/s:

| | measured | expected |
|---|---|---|
| client ack (benchbox to the leader, local) | 3.6 ms p50; per-second p99 16 ms median, 202 worst | commit ≈ max(leader fsync, RTT + follower fsync) = max(2.8, 0.9 + 1.4) ms, times 1-1.7: 2.8-4.8 ms. The emulated "2.7 ms" row gives 2.96 / 3.17 |
| commitlog fsync p50 / p99 | benchbox 2.8 / 9.7 ms, devhost 1.4 / 8.5, vps1 1.0 / 4.3 | fio: benchbox 2.7, devhost 1.15, vps1 0.70 |
| events per group commit | p50 18, p99 795 | 17-18 at 3,500/s |
| node CPU, max RSS | vps1 0.23 cores, 781 MB; benchbox 0.12, 1.5 GB; devhost 0.22, 1.1 GB | 0.06-0.07 leader, 0.04 follower (Phase 2, benchbox) |
| busy cores, whole box | vps1 0.92, benchbox 0.85, devhost 2.05 (its own services included) | |

With a leader on the LAN the ack is the leader's own fsync (benchbox's consumer NVMe), as the model says. The extra CPU and RSS against Phase 2 come from catching up after the first 75 s (`disk_reads` 183-278, 4 GB of commitlog on each node), so they aren't steady-state numbers.

**The kill.** `run.sh` (since fixed) killed vps1, which by then was a follower, so this was a follower kill, not the planned leader failover. The emission pause was 212 ms. vps1's restart replayed 4.0 GB of commitlog (61 segments) in 5.4 s, ~1.35 s a GB against benchbox's ~0.35, on 2 vCPU. It answered at +5.6 s, followed at +5.6 s, and caught up to the leader's commit at +9.8 s. In the 60 s around it the ack was 11.3 ms p50 and 205 ms p99, with 5 retries.

**Checker:** PASS. 891,399 seqs, 0 violations, 0 holes, 0 acked-but-not-emitted, 0 re-ingested. A consumer from cursor 0 through the bucket read all of them dense and matching in 82.5 s. Three leader changes under overload and a kill all came out clean. 31,321 events were emitted at two seqs. Those are the load generator's retries of submits that timed out but committed. That's at-least-once from the submitter's side, and none crossed a gap.

Not run, because the session was stopped: page-cache and memory durability, the 350/s runs that would show a vps1-led commit away from the WireGuard ceiling (expected ≈ 5.2 ms RTT to devhost + ~1 ms fsync, so ~6-7 ms commit and ~12-13 ms at a submitter in benchbox's metro), and a real leader kill.

## Inputs

| Input | Value | Label |
|---|---|---|
| Events/s, frame size, zstd ratio, peak hour | ~350 (480 peak hour), 5.3 KB, 1.56x | measured ([cost](cost.md#inputs)) |
| CPU per event | 65-70 µs one node on cores, 89-94 on threads, 142-156 a 3-node cluster | measured ([perf](perf.md)) |
| Leader's share of the cluster's CPU | half | assumed |
| The state's share of a flush (its L0, the checkpoint sealed and the one retired, compaction) | ~8 Class A, ~12 Class B, one SlateDB | measured (Phase 6 hour run, less GC's one-key `DeleteObjects`, which R2 doesn't bill as Class A: [Real R2 hour](#real-r2-hour); the B split is Phase 3's per-flush count). The study used vlpds's ~4.4 A and ~9 B an L0 flush, four shards |
| SlateDB polls | ~1.26 GET/s, one SlateDB (manifest 10 s, compactor and worker 30 s) | measured (Phase 6). The study used vlpds's ~0.4 a shard |
| vlpds lease loop | 1.5 Class A + 1 Class B per node per second at TTL 10 s | measured in vlpds |
| Per-DID state | 121.8 B a DID (243.6 B right after an update) | measured (`tests/state_bulk.rs`) |
| Hosts on the network | 6,260 listed, 1,956 active, 89 bsky.network PDSes with 23.4M of 24.0M accounts | measured (listHosts, reference notes) |
| RAM baseline | 2 GB a node plus the ring | assumed (shadow run: 1.1 GB at 60 events/s) |
| Flush upload, detection and takeover | ~2 s, ~5 s | assumed |
| fsync | 0.03-0.1 ms datacenter NVMe, 0.5-2 ms VPS | assumed; one OVH VPS measured 0.57-0.68 ms p50 (fio, 4-64 KiB) and 0.83-0.93 ms in the commitlog ([vps1](#vps1-one-ovh-vps-single-node)) |
| fsync, consumer NVMe | 2.7 ms p50, 5.9 ms p99 (fdatasync, one appending writer; 5.5 ms with three on one disk) | measured (benchbox, `tests/qlog/fsync_probe.sh`) |
| RTT | 0.2 ms one DC, 3 ms one metro, 65 ms cross-region | assumed |
| R2 PUT | ~200 ms p50 from benchbox | measured (vlpds `bench/results/spaces-r2-2026-10-05.md`) |
| Segment size, read size, DID shards, group commit | 64 MiB raw (cut per flush), 8 MiB, 1, 2 ms | code (the study: 64 MiB of zstd, 4 shards) |
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
