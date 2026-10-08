---
title: Quorum log measurements
section: Reference
order: 307
summary: "Every test, chaos run and benchmark behind the quorum log: replication, the commitlog, the flush, bucket recovery, membership, durability modes, an hour on real R2, one VPS and three hosts over Tailscale."
---

```hero
diagram:
  caption: The chaos harness. A load generator (or a fakepds fleet, for the relay) submits to three nodes on a local MinIO or R2, a supervisor restarts whatever the fault schedule kills, and a checker consumes every node's stream from cursor 0. The manifest is verified against the bucket every 20 s.
  nodes:
    - { id: load, label: load, sub: "qlog load · fakepds", at: [0, 5], size: [9, 3], tone: muted }
    - { id: n1, label: node n1, sub: commitlog, at: [13, 0], size: [9, 3], tone: accent }
    - { id: n2, label: leader n2, sub: "seq · replicate", at: [13, 5], size: [9, 3], tone: violet }
    - { id: n3, label: node n3, sub: commitlog, at: [13, 10], size: [9, 3], tone: accent }
    - { id: s3, label: MinIO or R2, sub: "manifest · segments · state", at: [29, 5], size: [11, 3], shape: store, tone: amber }
    - { id: chk, label: checker, sub: every node from 0, at: [29, 10], size: [11, 3], tone: blue }
  edges:
    - "load -> n2: submit"
    - "n2 -> n1: replicate"
    - "n2 -> n3: replicate"
    - { from: n2.r, to: s3.l, label: flush, tone: amber }
    - { from: n3.r, to: chk.l, label: subscribeRepos, tone: blue }
facts:
  - { value: "0", label: violations, note: "every chaos run, from kill -9 to wiped disks and membership changes under load", tone: accent }
  - { value: "1.2 ms", label: ack p50 for an hour on R2, note: "350 events/s, 30 s flushes · p99 1.68 ms", tone: violet }
  - { value: "0.33", unit: A/s, label: billed R2 writes, note: "Cloudflare's count for the same hour · 1.725 reads/s", tone: amber }
  - { value: "~30-40k/s", label: one 2-vCPU VPS, note: "a single node with the load and checker on the box · CPU-bound", tone: blue }
```

These are the measurements behind [The quorum log](quorum.md), in the order the log was built:
replication first, then the commitlog, the flush, bucket recovery, membership changes, request
counting, the relay on top and the durability modes. Then come the runs off the bench box: an hour
against real R2, one VPS as a single node, and three hosts over Tailscale.

Unless a section says otherwise, the runs were on a 32-thread bench box (Zen 5) on loopback, with
~5.3 KB frames (the network's mean) and a local MinIO. Loads are 350/s (today's rate), 3,500/s (10x)
and 35,000/s (100x).

## How it's checked

The harness (`tests/qlog/chaos.sh`, `just qlog-chaos`) runs three `qlog node` processes, each
restarted by a supervisor 1 s after it exits. Load comes from `qlog load`. `qlog check` consumes every
node's `subscribeRepos` from cursor 0. Partitions blackhole every peer route through
`tests/qlog/proxy.py`, one route per direction. The checker asserts:

- No seq is emitted with two contents across all nodes and incarnations.
- Every consumer's stream is strictly increasing and dense (no repeat, no step back, no hole), and
  may jump only across a recovery's recorded gap.
- Every acked seq is emitted with the content it was acked with, and an acked seq inside a gap has
  its event emitted again above it.
- Every event the hosts sent is emitted at a seq outside every gap.
- Every consumer ends at the same commit index.

`qlog verify` checks a manifest against the bucket: the segments hold the log densely up to F, the
state checkpoint holds exactly F (`last_l0_seq` = F, and its contents equal replaying the segments),
its cursors are the manifest's, no host's cursor counts an event that isn't in the log at or below F,
and segments past the manifest still continue the log. The harness runs it every 20 s and at the end.

`tests/qlog/relay-chaos.sh` (`just relay-chaos`) does the same with three `vlrelay` nodes fed by
`fakepds`. There, `e2e_check` also compares every upstream event with what the relay emitted (each
DID's commits once and in rev order).

In-tree tests (`cargo test --lib qlog`) cover log matching, truncation and trimming, a randomized
follower-versus-leader model (200 seeds of appends, stale and reordered deliveries, takeovers with
truncation), and three-node integration tests on real TCP with each node on its own runtime, so a
crash takes its memory and sockets at once. Seeded random chaos (`QLOG_SEED`) runs kills, power
cuts, partitions and flush crashes in process.

Mutations confirm the tests bite:

| mutation | result |
|---|---|
| skip the tail adoption at a takeover | 72,648 violations ("seq 60081 emitted with two contents") |
| commit at one ack instead of two | three tests fail |
| a follower acks before its fsync | the power-cut test fails on 4 of 5 seeds, 84-1,440 violations |
| no fence before an old leader's flush | the old leader commits its manifest over the new one's |
| cursors sent one batch ahead | "ahead of the log at F" |
| the bucket path off for a lagging follower | a hole in its stream |
| resume at S + 1 instead of R + 1 | 1,040 "emitted with two contents" on the single node, and wipe-all fails |
| silent members left out of the recovery trigger | the cluster recovers and jumps while the old leader's intact log is only down |
| count learners' acks toward the quorum | the leader and the learner commit alone |
| drop the takeover's membership check | the learner campaigns |
| trust a short log after a power cut (`--qlog-unsafe-trust-log`) | relay `power-cut-all` fails with 257 seqs "emitted with two contents" |
| per-event locks instead of holding a batch's DIDs | a rev out of order, every time |

Dropping the membership barrier's flush or its holders condition alone fails no test. Each is masked
by the other (and by the not-intact rule for a learner that isn't caught up), which is the defense in
depth the safety argument intends.

## Replication

Memory-only nodes, the first build. In-tree, seeds 1-20 of the random chaos test all pass with 0
violations: 2.34M seqs across 54 kills and 55 isolations, with every acked seq emitted.

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

That's 3,059,264 seqs across 25 kill -9s, 16 partitions and 7 SIGSTOPs, seen 9.18M times by the three
consumers.

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

What it showed against the study's assumptions:

- The emit path is dominated by the firehose's 2 ms merger tick. The quorum ack on loopback is
  0.1-0.2 ms. Consumers see an event 1.2-2.5 ms after it's submitted, of which ~1 ms on average and
  2 ms at most is the tick. Real placements add the RTT.
- Takeover is faster than assumed when the process dies. The study assumed 0.5-1.5 s. Measured, it's
  15-80 ms: the follower's replication connection closes, the refused probe confirms it, and the
  pre-vote, the CAS on a local MinIO, the promise round and the first commit follow. On R2 the CAS
  alone is a PUT. A hung or partitioned leader takes the 1 s heartbeat timeout, as assumed. The host
  owner's resend needed its timeout cut to 1 s to meet that.
- The replication CPU is tiny. At 100x the leader spends 0.2 cores and each follower 0.1 on
  replication, emission and one consumer each, ~6 µs an event on the leader. The ~10 vCPUs the study
  sizes the leader at for 100x are verify and apply.
- RAM in memory-only mode is ~2x the log window. The node keeps its retained log (512 MiB) and the
  firehose ring (512 MiB) as separate copies of the same frames, so a node sits at ~1.2-1.6 GB at 10x
  and above (2.1 GB at its peak during the mixed run).

## The commitlog

Commitlogs on, all three on one consumer NVMe (a 970 EVO Plus). `tests/qlog/fsync_probe.sh` gives it
2.7 ms p50 for one writer and 5.5 ms for three. tmpfs stands in for a disk whose fsync costs nothing,
an upper bound for datacenter NVMe with power-loss protection, and "tmpfs + N ms" sleeps N ms before
each fsync to emulate a device. The power-cut fault (SIGUSR1) makes the commitlog lose a random part
of its unsynced tail plus a torn record, then SIGKILLs the node.

In-tree tests cover commitlog replay across truncations, restamps, promises and rollovers, and a
torn tail at three cut points. Integration tests kill -9 two nodes at once, then all three, six
rounds, cut power on all three under load with a 3 ms fsync, and keep a follower down longer than the
leader's 32 KiB in-memory window, which then catches up from the leader's disk with no reset. Seeds 1-10 of the power-cut test and the durable random chaos
pass with 0 violations.

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

That's 4.0M seqs across 78 node kill -9s and 55 node power cuts, with every acked seq emitted and
nothing lost, duplicated or reissued. Recovery cut torn tails of 4-36 bytes. Every consumer resumed
from its cursor with no skip notice and no hole. The measurement runs below add 10 more whole-cluster
kills and power cuts and 21 leader kills and power cuts, also clean.

| submit to quorum ack, p50 / p99 | 350/s | 3,500/s | 35,000/s |
|---|---|---|---|
| memory only (same build) | 0.11 / 0.23 ms | 0.20 / 0.38 | 0.55 / 1.40 |
| commitlog on tmpfs | 0.14 / 0.69 | 0.23 / 0.78 | 0.88 / 1.79 |
| commitlog, three nodes on one consumer NVMe (fsync 5.2-5.6 ms) | 8.5 / 21.7 | 9.3 / 17.8 | saturated: ~25,000/s committed (20,000/s: 27 / 65 ms, fsync 10 ms) |

| emulated device fsync, 3,500/s | 0.1 ms | 0.5 ms | 1 ms | 2 ms | 2.7 ms (this NVMe, one node a disk) |
|---|---|---|---|---|---|
| submit to quorum ack, p50 / p99 | 0.37 / 0.91 | 0.76 / 1.23 | 1.26 / 1.92 | 2.27 / 3.12 | 2.96 / 3.17 |
| submit to the first consumer, p50 / p99 | 1.52 / 2.68 | 1.96 / 3.13 | 2.42 / 3.54 | 3.44 / 5.10 | 4.14 / 5.28 |

At 35,000/s with a 2 ms fsync the ack is 2.87 / 4.39 ms, with 175 events a group commit and one fsync
per 5 ms submit tick.

| | 350/s | 3,500/s | 35,000/s |
|---|---|---|---|
| CPU leader / follower, memory only (cores) | 0.035 / 0.019 | 0.059 / 0.030 | 0.20 / 0.10 |
| CPU leader / follower, commitlog | 0.033-0.044 / 0.022-0.028 | 0.059-0.072 / 0.036-0.045 | 0.29 / 0.17 |
| RSS a node, memory only (512 MiB log + 512 MiB ring) | 255 MB | 1.0 GB | 1.2-1.7 GB |
| RSS a node, commitlog (64 MiB log + 512 MiB ring) | 220 MB | 0.57-0.60 GB | 0.73-0.80 GB |
| events a group commit, p50 | 2 | 17-18 | 175 |

Throughput ceilings (200,000/s offered for 30 s, three nodes):

| | committed | notes |
|---|---|---|
| memory only | 200,000/s (1 GB/s a node) | ack p50 3.0 ms · leader 0.99 cores, followers 0.46 |
| commitlog on tmpfs | 200,000/s | ack p50 5.8 ms, p99 62 · leader 1.78 cores, followers 0.91 |
| commitlog, three nodes on one consumer NVMe | ~25,000/s (~130 MB/s a node fsynced, ~400 MB/s on the drive) | fsync p50 13-15 ms, p99 70-240 · backpressure held the leader at 3.6 GB RSS |

The tmpfs ceiling run kept only 256 MiB of commitlog on each node, which is 0.25 s at 1 GB/s. One
follower fell further behind than that three times and was reset, a counted gap in its stream that
the checker reports as skipped seqs. Every acked seq was emitted with its content. The flush's
catch-up from the bucket (next section) removed that case.

| fault, 3,500/s, commitlog | emission pause (fault to the next new seq at any consumer) |
|---|---|
| kill -9 the leader | 55 ms median, 61 max on tmpfs · 72 / 88 on the NVMe (memory only: 43-81) |
| kill -9 a follower | 17 ms median, 38 max |
| power cut of the leader | 79 ms median, 579 max |
| kill -9 the leader and a follower | 1.5-2.3 s |
| kill -9 all three | 2.36 s median, 2.70 max |
| power cut of all three | 2.47 s median, 2.72 max |

The two- and three-node pauses are the supervisor's 1 s restart, recovery, then the 1 s election
timeout, since nobody is leading. The rest is ~0.4 s. Recovery reads and checksums every retained
segment: 0.65 s for 1.8 GB (24 segments), ~0.35 s a GB.

What it showed:

- The `fsync` ack is about RTT + fsync + 0.3 ms when group commits line up with arrivals, as they do
  under 5 ms ticks. When fsyncs run back to back, an event also waits out part of the one in flight.
  On the shared NVMe (5.5 ms fsync) the ack was 9.3 ms, ~1.7 fsyncs. Budget ack ≈ RTT + 1-1.7x fsync.
- Don't put two members on one disk. Three nodes on one consumer NVMe fsync at 5.5 ms instead of 2.7,
  and they also share its write bandwidth.
- Fsynced disk bandwidth sets the ceiling. On tmpfs, three nodes sustain 200,000/s (1 GB/s a node)
  at 1.75 cores on the leader, so the commitlog code isn't the limit. On a disk, one node a disk needs
  ~185 MB/s fsynced at 100x. This consumer drive manages 278 MB/s with one writer at 1 MiB fdatasyncs,
  so 100x is borderline on consumer NVMe even before wear. 10x is comfortable anywhere.
- With a commitlog, two- and three-process failures are a normal takeover: the cluster resumes in
  about two seconds with nothing lost and no jump.
- One append in flight per follower caps a follower at 4 MiB per RTT + fsync. At a 2 ms fsync that's
  ~2 GB/s. It only bit on the saturated shared disk (20 ms fsyncs, ~200 MB/s). Pipelining appends is
  the fix if a host's fsync is that slow.

## The flush

Flushes every 2 s by default in the harness (`FLUSH_MS`), with `qlog verify` against the bucket every
20 s and at the end, and a final consumer from cursor 0. `RING_MB=16` keeps the ring small, so
reconnecting consumers backfill through the bucket and the local tail. New faults: `flush-crash`
(SIGKILL at a random flush step, 5% a step), `mid-trim` (SIGKILL between commitlog segment deletions,
with a 32 MiB disk budget) and `mixed-flush`.

In-tree (29 tests): the seal across automatic SlateDB flushes (four seals with 16 KiB L0s, each
checkpoint holding exactly its seal point), a crash between two commitlog segment deletions, a crash at
each flush step twice round, an old leader's stalled flush losing to a takeover, backfill from every
tier, a follower catching up from the bucket, and seeded random chaos with flush crashes at 4% a step
(seeds 1-10 clean).

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

That's 4.6M seqs, every acked seq emitted with its content, and every consumer stream dense. Every
manifest checked was consistent, and every run ended with 0 segments past the manifest. Every
consumer from cursor 0 read the whole log densely through the bucket, the local tail and the ring,
~0.6M events a second.

The runs found three things, all fixed. The first `mid-trim` run (30% a deletion) had a follower fall
behind the leader's disk and get reset, a hole in its stream, so lagging followers are now served from
the bucket. Seals waited 1-5 s on SlateDB's L0 cap of 8 at 2 s flushes, so it's 32. And the harness
aborted a run when a fault picked a node the supervisor was restarting, and the verifier raced the next
flush deleting the checkpoint it was about to read (it now opens the checkpoint first).

Cost of the flush, commitlogs on tmpfs with a 1 ms emulated fsync and MinIO on tmpfs. The load's
frames are padded with one repeated byte, so they compress ~700x. Stored bytes are meaningless here,
but raw bytes aren't.

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

Requests per flush, counted by the object-store counters vlRelay shares with vlpds over each flush's window (which includes
the state's own background work):

| per flush | 350/s, 30 s | 3,500/s, 30 s | 35,000/s, 10 s |
|---|---|---|---|
| PUT create (segments + SlateDB SSTs and manifests) | 3.2 | 8.8 | 28.6 (+ 0.9 multipart uploads, 1.8 parts) |
| PUT plain | 1.0 | 1.0 | 0.1 |
| PUT CAS (the quorum manifest) | 1.0 | 1.0 | 1.0 |
| LIST | 0.8 | 0.8 | 0.9 |
| GET | 9.8 | 10.8 | 15.3 |
| Class A, less the log segments | ~5.0 | ~4.6 | ~5.0 |

Over a whole steady run (350/s, 30 s flushes, 300 s, the leader process, with the state's manifest
polled every 10 s). Followers send almost nothing.

| per second | GET | GET range | HEAD | LIST | PUT | PUT CAS | PUT create | Class A | Class B |
|---|---|---|---|---|---|---|---|---|---|
| measured, one DID shard | 2.07 | 0.24 | 0.03 | 0.06 | 0.06 | 0.04 | 0.18 | 0.34 | 2.34 |
| study, today, 30 s, four shards | | | | | | | | 0.67 | 2.81 |

What it showed:

- The flush doesn't touch the ack path. Acks and emission are the same with the flush on and off at
  all three rates, within run-to-run noise. Its pause is the state applier's alone (20-350 ms). At
  100x the worst per-second p99 rises from ~6 to ~10 ms in the seconds a flush runs, as the leader's
  zstd and segment reads compete for CPU.
- A flush costs the leader CPU in proportion to bytes: ~0.5 cores at 100x for 1.7 GB raw every 10 s
  (reading the entries back, zstd and the PUTs). That's ~5% of the study's 10-vCPU leader at 100x.
  Followers don't flush or keep state, so their CPU is unchanged.
- Flush duration is ~1.4 s a GB at 100x on a local MinIO, with the PUTs one at a time. On R2 that
  wants parallel segment PUTs at 100x.
- Leader RSS grows ~1 GB with the flush at 100x: one segment's entries held while it's built and
  compressed, plus SlateDB's memtable for 333k DIDs. Budget ~1 GB of memtable per 1M DIDs changed in
  an interval, or seal more often.
- SlateDB's manifest poll matters. At SlateDB's default 1 s it's ~2 GETs a second more per database.
  The state polls every 10 s, as vlpds does.

## Bucket recovery

Wipe faults: a wipe is kill -9, then the commitlog deleted before the supervisor restarts the node.
New scenarios: `wipe-all`, `wipe-two` (the survivor a leader or a follower at random), `mixed-wipe`,
and `single-kill`, `single-power-cut`, `single-wipe` and `single-mixed` (`NODES=1`). Runs at 3,500/s,
64 hosts, commitlogs on tmpfs with a 1 ms fsync.

In-tree (37 tests, looped 8 times clean): wiping every disk twice under load, wiping two (the leader
back in time keeps its quorum, otherwise there's salvage from the survivor), no recovery while a silent
member could be intact, orphan adoption, the recovered state equal to replaying the bucket with a crash
at each recovery step, the single node, a clone that holds exactly its checkpoint while the source
keeps changing, and followers reset across one and two gaps never emitting a deposed leader's segment.

| run | faults | recoveries | distinct seqs | violations | manifest checks |
|---|---|---|---|---|---|
| wipe-all, 2 s flush, 90 s | 5 wipe-all | 5 | 365,961 | 0 | 4 + final |
| wipe-two, 2 s flush, 120 s | 8 wipe-two (4 survivor leaders, 4 followers) | 8 (5 with salvage, 2.4k-6.5k entries) | 463,204 | 0 | 5 + final |
| mixed-wipe, 240 s | 2 wipe-all, 6 wipe-two, 3 kill-all, 1 kill-two, 2 power-cut-all, 3 partitions | 8 | 895,476 | 0 | 10 + final |
| mixed-wipe with flush and recovery crashes (5% a step), 180 s | 19 injected crashes, 4 power-cut-all, 2 kill-two, 4 kill -9, 1 wipe-two, 2 partitions | 1 + crashed attempts | 635,842 | 0 | 8 + final |
| single-mixed, 150 s | 4 kill -9, 12 power cuts, 1 wipe | 1 | 532,453 | 0 | 6 + final |
| kill-two, memory only, 90 s | 5 kill-two | 5 (salvage 1.1k-5.8k each) | 341,081 | 0 | 4 + final |
| wipe-all, 30 s flush, H = 8.64M, 170 s | 3 wipe-all | 3 | 818,301 | 0 | 6 + final |

That's 4.05M distinct seqs, 31 recoveries and 0 violations. Every stream jumped only across gaps, and
every acked seq lost in a gap came back above it (200,812 of them in the 30 s run). Every event the
hosts sent is in the final log outside the gaps, and every consumer from cursor 0 read the whole log
through the bucket, across the gaps. After mixed-wipe's eight recoveries, the retention report marked
six old state paths deletable and two still referenced.

| | measured (local MinIO) |
|---|---|
| recovery, bucket side: manifest and orphans read / state clone / apply, jump and seal / salvage segments / manifest CAS | 0-2 ms / 21-46 ms / 3-58 ms / 0-49 ms / 0-1 ms · 33-146 ms in all (the long ones salvaged 2-6k entries) |
| emission pause, wipe-all | 2.20-2.30 s: the harness's 1 s restart, the 1 s election timeout, ~40 ms of recovery |
| emission pause, wipe-two | 1.3-3.0 s (median 3.0 s): the restart, the election timeout, and the 0.5 s rank stagger when a lower-ranked candidate goes first |
| emission pause, single node: wipe / kill -9 / power cut | 1.21 s / 1.49 s median / 1.39 s median (1 s restart plus WAL replay or recovery) |
| load: first new-generation ack to the recovery's cursors | 0-18 ms |
| re-ingest at 10x, 2 s flush | 4.5k-12k events sent again, all acked again in 57-209 ms |
| re-ingest at 10x, 30 s flush | 71k-81k events sent again (66k-77k of them acked before: the consumers' repeats), all acked again in 0.71-0.88 s |

With a 30 s flush, consumers saw ~70k events again a recovery at 10x (~23 s of load: the interval
since F plus the cursor lag). Retried batches gave another 5k-36k duplicates a run with or without
recoveries. That's the load generator resending batches whose ack was lost, which this harness never
deduplicates (the relay drops them with `check_chain`).

What it showed:

- Recovery after a lost quorum is cheap on the bucket side. The clone is O(1), so the state's size
  doesn't matter, and the whole bucket side was 33-146 ms. The pause is detection and restart.
- Re-ingest catch-up here is fast because the harness has no verify (81k events came back in under
  1 s). In the relay it's CPU-bound, and the model's 12.7 s at a 30 s flush stands. The storm size
  matched the model: ~71-81k events at 10x and 30 s against the 129.5k worst case.
- Salvage from a surviving member shrinks the repeat window. In wipe-two it kept 2-6k entries a
  recovery, so the gap's acked losses were 0.
- A surviving leader back within its timeout keeps its quorum. Two wiped followers restarted within
  the election timeout rejoin as fresh followers, with no recovery at all.
- The single node's restart pause is the process restart plus the WAL replay (~0.35 s a GB). Disk
  loss costs a jump and the re-ingest of everything since F.

## Membership changes

The harness gets node slots (9 or more ids, of which `NODES` are the bootstrap members, each id used
once). New scenarios: `replace-follower`, `replace-leader`, `grow-shrink` (3 → 4 → 3 → 5 → 3),
`switch-crash` (nodes die at a random switch step, 35% a step) and `switch-fault` (a kill -9 of the
leader, a follower or the learner, or a 3 s partition of the leader, at a random point of the change).
`qlog member` drives each change and retries it until it lands. After each change the removed node
stays up for 2 s, is checked to hold no entry and no promise from the epoch its removal took effect
at, and is then retired. Runs at 3,500/s, 64 hosts, commitlogs on tmpfs with a 1 ms fsync, 2 s
flushes.

In-tree (44 tests, looped 6 times clean): replacing a follower under load and then killing the other
old member, replacing the leader under load, 3 → 5 → 3 with two of five down, a learner never counting,
a removed member never leading, a crash at each of the four switch steps, the leader cut off before
and after the barrier's flush, and a single node growing to three and back.

| run | changes | faults | distinct seqs | violations | removed members checked / counted after removal | manifest checks |
|---|---|---|---|---|---|---|
| replace-follower, 300 s | 5 | none | 1,050,017 | 0 | 5 / 0 | 12 + final |
| replace-leader, 180 s | 6 (all by handoff) | none | 630,017 | 0 | 6 / 0 | 7 + final |
| grow-shrink, 150 s | 7 (3 → 4 → 3 → 5 → 3 → 4 → 3 → 5) | none | 525,017 | 0 | 4 / 0 | 6 + final |
| switch-crash, 240 s | 10 | 31 crashes: 16 in catch-up, 8 before the pause, 5 after the barrier's flush, 2 after the CAS | 840,086 | 0 | 10 / 0 | 10 + final |
| switch-fault, 240 s | 10 | 5 leader partitions, 1 leader kill, 4 follower kills | 840,034 | 0 | 10 / 0 | 10 + final |
| replace-follower (smoke), 60 s | 3 | none | 210,017 | 0 | 3 / 0 | 2 + final |
| mixed-wipe (regression, streamed salvage), 180 s | 0 | wipes, kills, power cuts | 646,292 | 0 | | 8 + final |
| kill-two, memory only, 35,000/s, 10 s flush, 90 s | 0 | 3 kill-two | 3,346,350 | 0 | | 3 + final |

That's 8.1M distinct seqs, 41 membership changes and 38 removed members checked, with no violation.
Every change landed without an operator, every consumer stream was dense, and every manifest verified.

| measured | |
|---|---|
| commits paused (pause to leading the new epoch) | 6-13 ms (median 7-12 by run), 50 ms max in switch-crash: the drain ~0 ms, the barrier's flush 5-12 ms, the CAS 0-2 ms |
| emission pause, replacing a follower | none over the checker's 15 ms gap threshold in 6 of 8 changes without faults, 18-20 ms in the other two |
| emission pause, replacing the leader (handoff) | 15-49 ms (n=6): the pause, the CAS, `Lead`, the promise round and the first commit |
| emission pause, a change with a crash or a partition of the leader | ~1.0-2.1 s: the election timeout, as for any leader failure |
| command to done | 0.6 s for a removal · 1-6.6 s for an addition, mostly the learner's catch-up |
| learner catch-up at 10x | 0.4-6.1 s, growing with the leader's local log: 1.3 s for ~0.86 GB, 6.1 s for 4.2 GB (the default 4 GiB disk retention), ~0.7 GB/s on loopback, while the load runs |
| ack latency in the seconds a change runs, against the others (300 s run) | median per-second p50 1.30 / 1.32 ms, median p99 1.86 / 1.76 ms, worst p99 9.5 / 2.5 ms: the catch-up's disk reads on the leader, not the pause |
| salvage at 100x, 10 s flush (memory only) | 220k-316k entries (1.2-1.7 GB) streamed into 4-5 segments · 2.9-4.6 s a recovery, of which applying them to the state is 2.3-3.7 s and the segment PUTs 0.5-0.7 s |

The 100x run found one bug. Salvaging a 10 s interval at 100x takes 3-5 s, longer than the 1 s election
timeout, so another member took over mid-recovery and jumped a second H. A recovering candidate now
re-sends its promise every heartbeat, and with that each kill-two is one recovery and one jump.

What it showed:

- A membership change costs about one RTT and one small flush of commits, well under the study's "one
  flush plus one CAS, under a second". The pre-flush makes the barrier's own flush a few ms of
  entries.
- Replacing a box is mostly copying the leader's local log: 4 GiB in ~6 s on loopback, but ~35 s on a
  1 Gb/s port and ~70 s on a VPS-1's 0.5 Gb/s, while it shares the leader's port with consumers.
- Replacing the leader doesn't cost a takeover. The handoff is a 15-50 ms pause, so rolling every box
  (three replacements) costs ~0.1 s of emission at 10x.
- The bucket sees one more CAS per change (two, with learners recorded first).

## Counting requests

Every request the quorum log sends goes through `Store::counted` (vlsync's `vlsync-store`, shared with vlpds), which counts it at the bottom
of the stack (retries and SlateDB's own traffic included) in `vlpds_object_store_requests_total` by op,
key component and client. A node holds one counted client per purpose, so each request is billed to
what sent it:

| client | what it carries |
|---|---|
| `qlog_flush` | the leader's fence, segment PUTs, the manifest CAS, checkpoint deletes |
| `qlog_state` | the leader's SlateDB: memtable uploads, the seal's checkpoint, its manifest polls, compactor, compaction worker and GC |
| `qlog_leader` | `qlog/leader` reads and CASes (takeovers, membership) |
| `qlog_recovery` | a bucket recovery: manifest, orphans, the state's clone and seal, salvaged segments |
| `qlog_backfill` | segments read for a follower behind the leader's disk, a recovery's catch-up emit, consumers' old cursors |
| `qlog_retain` | retention passes and their deletes |
| `qlog_tool` | `qlog verify`, `qlog check` |

`/qlog/status` has the totals by R2 class, by purpose, by `purpose/component` and by `purpose/op`, and
the node's http port serves the same counter at `/metrics`. Class A is every PUT, LIST, copy, multipart
step and `DeleteObjects` POST, Class B every GET and HEAD, and a single DELETE and a multipart abort are
free. object_store sends even a single delete as a one-key `DeleteObjects` POST, so every SlateDB GC
delete is counted as Class A. The hour on R2 showed R2 doesn't bill it that way.

The hour: three nodes, commitlogs on tmpfs with a 1 ms emulated fsync, `qlog load` at 350/s over 64
hosts, 30 s flushes, one hour, no faults, every node's status sampled each minute. Rates are over the
last 58 minutes, all nodes summed. Run 1 had the state's SlateDB at its default compactor and worker
polls (5 s), run 2 at 30 s (now the default). A 15-minute run at 10x used run 2's settings. All three
had 0 violations, 0 holes and nothing acked lost (1,260,001 and 3,150,017 distinct seqs), every manifest
consistent, and acks of 1.2-1.3 ms p50 and under 2 ms p99. Run 1's final consumer from cursor 0 didn't
run because the harness was interrupted after the load finished, but its sampled counters cover the
hour.

| cluster-wide, per second | Class A | Class B | free | per month A / B | R2 $/mo, no free tier | R2 $/mo |
|---|---|---|---|---|---|---|
| study, today, 30 s (four DID shards) | 0.67 | 2.81 | | 1.77M / 7.39M | $11 | $3 |
| run 1, 350/s, SlateDB's 5 s polls | 0.481 | 2.398 | 0.146 | 1.27M / 6.31M | $7.97 | $1.20 |
| run 2, 350/s, 30 s polls | 0.478 | 1.660 | 0.146 | 1.26M / 4.36M | $7.23 | $1.16 |
| study, 10x, 30 s | 0.83 | 2.81 | | 2.18M / 7.39M | $13 | $5 |
| 10x, 15 min, 30 s polls | 0.634 | 1.479 | 0.045 | 1.67M / 3.89M | $8.91 | $3.01 |

These dollar figures count GC deletes as Class A. By purpose and what it touched, run 2 (per second,
the purposes left out sent nothing):

| purpose / component | Class A | Class B | free | study's term |
|---|---|---|---|---|
| flush / `log_segment` | 0.033 | | | segments: 0.051 |
| flush / `qlog_manifest` (the CAS) | 0.033 | | | manifest: 0.033 |
| flush / `state_manifest`, `state_gc_boundary` (retiring the last checkpoint) | 0.067 | 0.100 | | in the L0 flush |
| state / `state_sst` (L0s and compaction output, compaction's ranged reads) | 0.117 | 0.461 | 0.038 | L0 flush and compaction, four shards: 0.589 A, 1.209 B |
| state / `state_manifest` (writes, the seal's checkpoint, the 10 s poll) | 0.127 | 0.295 | 0.067 | |
| state / `state_compactions` (compactor and worker) | 0.095 | 0.257 | 0.041 | |
| state / `state_gc_boundary` (read with every latest-manifest and compactions read) | 0.003 | 0.547 | | polls, four shards: 1.6 B |
| state / `state_wal` (opening it) | 0.003 | | | |
| leader / `qlog_leader` | 0 | 0 | | 0 (peers for liveness) |
| followers, the whole hour | 0 | 3 requests each, at start | | 0 |

Of the state's 0.345 Class A, 0.146 is SlateDB's GC deleting replaced SSTs, manifests and compaction
records. In the first six minutes, before GC's first pass (every 10 minutes, for files older than 5),
the state sent 0.158 Class A a second. The 10x run's 13-minute window caught fewer passes (0.045 a second
of deletes).

Against the study:

- One state, not four shards. The study gave the leader four DID shards, each with its own L0 flush,
  compaction and polls. The leader keeps one SlateDB, which is most of why both classes come in under
  the study.
- Each state costs more than vlpds's shard did. Per flush, the state's share is ~12.4 Class A as
  counted (6.0 for the L0, the seal's checkpoint and compaction output, 4.4 for GC deletes, 2 for
  retiring the previous checkpoint) against vlpds's 4.42 an L0 flush. The checkpoint pair is the price
  of sealing at exactly F.
- Polls are ~1.26 GETs a second for one SlateDB, not 0.4. SlateDB reads its GC boundary file with every
  latest-manifest and compactions read (0.55 a second on its own), and the compactor and its worker
  each poll. At the default 5 s polls the state sent 0.74 Class B a second more.
- Segments are one PUT a flush at today's rate. A 30 s flush is ~56 MB raw, under one 64 MiB segment.
  The study added full segments by compressed bytes on top of the partial one, which double counts
  below 64 MiB a flush. Above that, segments cut at 64 MiB raw are 1.56x what the study assumed: 8.71 a
  flush at 10x (0.29 a second against 0.21), ~2.8 a second at 100x against 1.8.
- The leader record, recovery, backfill and retention cost nothing in steady state.

`scripts/cost_model.py` uses these numbers (one DID shard, the state's flush and poll shares, segments
cut per flush by raw bytes) and reproduces run 2 exactly (0.48 A, 1.66 B as counted). It's within 20%
of the 10x run's short window. After the hour on R2 it also leaves GC deletes out of Class A.

## The relay on the log

`tests/qlog/relay-chaos.sh` runs three `vlrelay` nodes on a local MinIO, commitlogs on tmpfs with a 1 ms
emulated fsync, fed by `fakepds` (16 hosts, 200 accounts each, commits, `#identity`, `#account` and
`#sync`, with the fake PLC on the side) and 2 s flushes. At 3,500 events/s, 90 s each, every scenario
had 0 violations, 0 holes, nothing acked lost and nothing missing between upstream and the relay (about
30M events observed across the runs):

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

The chaos found four relay bugs, each now covered by a test or a scenario:

- Two sockets on one host. A node that had just started subscribed to every host before its filter
  landed, and a moved host's old owner kept reading until it polled. Their events interleaved a DID's
  events at the leader (prevData mismatches, then a desynchronized account). The leader now admits a
  host's events only from the member its host table names, and a node reads nothing until the leader
  gives it hosts.
- A record released to the cache after it left pending, so a load between the two read the previous
  record. The release now caches before it lets go, under pending's lock.
- Decide-then-append across two batches. A batch slow on identity lookups timed out and was resent, the
  copy answered its DID's event as a duplicate of the still-unappended original, and the original landed
  after the next batch's event: a rev out of order. The leader now holds every DID of a batch from
  deciding until appended.
- A wiped node skipped seqs on backfill. With the firehose's floor taken from the bucket's manifest at
  start, a node wiped while the cluster was past a recovery gap skipped the seqs between the bucket's F
  and its first emission. The firehose now starts at the first emission (`wipe-two` covers it).

The first passes also found harness bugs: a mid-run `verify` that flagged a recovery's segments (above
R, so from a later generation, and `verify` now stops there), proxy routes whose ports overlapped fakepds's
with nine slots, and `flush-crash` crashing a node in the run's last second (crashes now stop 12 s
before the end with the other faults).

The PLC export ran through kill-leader, power-cut-leader and mixed-durable (150 s) against fakepds's
`/export` with 200,000 ops and a 429 on every sixth request, at 4 requests a second. In kill-leader the
export changed hands eight times mid-history, each new leader resuming from the checkpoint (21,979,
42,959 ... 187,820 ops read before it), and the ninth caught up. Every run ended caught up. Host
discovery ran the same way (kill-leader and power-cut-all): fakepds's `listHosts` served the 16 fleet
hosts and 40 that don't answer, one a page with a 429 every sixth request. The list was read to the end
both times (56 seen, 16 known, 40 refused, 10-11 429s waited out), taken over mid-list 2 and 4 times.

### The relay's hour

The counting hour's profile with the relay on top: commitlogs on tmpfs with a 1 ms emulated fsync
(`fsync` mode), local MinIO, fakepds at 350/s over 64 hosts (commits, `#identity`, `#account`, `#sync`,
~4.9 KB frames), 30 s flushes, one hour, no faults.

- 0 violations, 0 holes and 1,318,748 seqs, every one emitted on every node.
- 146 mid-run verifies, all consistent.
- Every upstream event in the relay once, in rev order.
- A final consumer from cursor 0 read the whole log from the bucket in 7.1 s.
- Leader append to quorum commit: p50 1.34 ms, p99 2.61.
- Upstream receive to emit: p50 3.6 ms, p99 6.2.
- CPU: the leader 0.115 cores, followers 0.06.

| cluster-wide, per second | Class A as counted | of it, one-key `DeleteObjects` (GC) | Class A billed | Class B | free |
|---|---|---|---|---|---|
| `qlog load`, 30 s polls (run 2) | 0.478 | 0.146 | 0.332 | 1.660 | 0.146 |
| the relay | 0.521 | 0.183 | 0.338 | 1.739 | 0.183 |

| purpose / component, the relay | Class A | Class B | free |
|---|---|---|---|
| flush / `log_segment` | 0.035 | | |
| flush / `qlog_manifest` | 0.034 | | |
| flush / `state_manifest`, `state_gc_boundary` | 0.067 | 0.101 | |
| state / `state_sst` | 0.130 | 0.480 | 0.052 |
| state / `state_manifest` | 0.143 | 0.310 | 0.081 |
| state / `state_compactions` | 0.106 | 0.276 | 0.050 |
| state / `state_gc_boundary` | 0.003 | 0.573 | |
| leader, followers | 0 | 0 | |

The flush's share is the same as with `qlog load` (one segment and one manifest CAS a flush, plus the
checkpoint pair). The state's is ~10% more on each class, because its SSTs hold the relay's real
records (12,864 accounts with their chains and keys, the host table, the throttle index). So each L0
and the compactions behind it are bigger, and GC deletes more files.

## Durability modes

The full relay suite ran once per mode (`DURABILITY=fsync|page-cache|memory`, 24 scenarios each,
3,500/s, 90 s). All 72 runs passed, with 0 violations, nothing acked lost and nothing missing
upstream, including `power-cut-majority` (the leader and one follower lose power at once). What
differs is which faults end in a bucket recovery, as the safety argument says:

- `kill-all` is a plain takeover in `fsync` and `page-cache` (1.9-2.3 s, the supervisor's restart plus
  an election) and a recovery in `memory`.
- `power-cut-all` and `power-cut-majority` are recoveries in `page-cache` (2.7 s and 2.1 s median) and
  `memory` (2.3 and 1.6 s), with their gaps recorded and every event re-ingested. In `fsync` they're
  takeovers.
- The kill -9 pause differs too. From the port refusing to the new leader leading takes 2-4 ms in
  every mode. What changes is how long the killed process takes to release its port: 40-56 ms in
  `fsync`, 72-85 in `page-cache`, 113-139 in `memory`. That grows with what the process holds in
  memory (a `memory` node keeps 512 MiB of log), so the pause medians are 73, ~140 and ~200 ms. A hung
  leader is caught by the 1 s election timeout in every mode.
- `power-cut-all` with the intact check turned off (`TRUST_LOG=1`) failed with 257 seqs "emitted with
  two contents". `power-cut-majority` under the same mutation passed, since the third node's whole log
  usually wins the election.

Ack and latency, three `qlog node`s, commitlogs on tmpfs with a 2 ms emulated fsync, 60 s at each rate.
"Ack" is submit to quorum ack, "consumer" is submit to the first consumer on any node, p50 / p99 in ms:

| mode | ack, 3,500/s | consumer, 3,500/s | ack, 35,000/s | consumer, 35,000/s |
|---|---|---|---|---|
| `fsync` | 2.28 / 3.06 | 3.43 / 4.82 | 2.89 / 4.47 | 4.26 / 6.15 |
| `page-cache` | 0.24 / 0.82 | 1.41 / 2.70 | 0.80 / 1.66 | 2.13 / 3.63 |
| `memory` | 0.20 / 0.76 | 1.45 / 2.67 | 0.61 / 36.5 | 1.98 / 40.6 |

Ceilings, the three commitlogs on the bench box's one consumer NVMe, 30 s flushes to the local MinIO,
200,000/s offered until the load's backpressure held it:

| mode | committed | notes |
|---|---|---|
| `fsync` | ~18,000/s (2.06M seqs in 114 s) | group fsyncs p50 23-28 ms, ~800 events each · the disk is the limit (~25,000/s with no flush competing for it) |
| `page-cache` | ~35,500/s (1.71M seqs in 48 s) | background fsyncs fall to 110-350 ms p50 behind ~180 MB/s a node · the ack path never waits on them |
| `memory` | | no number: at this offered rate the nodes' and the load's backlogs reached the run's 24 GiB memory cap, which stopped the run |

`page-cache` acks are ~10x faster than `fsync` at today's rate (0.24 ms against 2.3 at a 2 ms fsync),
within ~0.05-0.2 ms of `memory` until 35,000/s. `memory`'s p99 at 35,000/s (36 ms) is the in-memory
log's trimming and the larger heap, which `page-cache` doesn't carry (its log keeps 64 MiB in memory
against 512).

## An hour on R2

The counting hour again (three `qlog node`s on the bench box, commitlogs on tmpfs with a 1 ms emulated
fsync, 350/s over 64 hosts, 30 s flushes, status each minute), against a dedicated R2 bucket (location
hint `wnam`) instead of MinIO, on 2026-10-06 from 21:07:57 to 22:08 UTC.

Two independent request guards were armed, so a runaway would be caught quickly. A watchdog process
read every node's counters every 10 s and killed the run past 3,500 Class A or 12,000 Class B in all,
2.4 A or 8.3 B a second over 120 s, or 250 A or 300 B in any 10 s. Each node also enforced the same
budget itself (`src/qlog/budget.rs`) and exited at once on a breach. Neither tripped. The largest 10 s
poll interval was 89 A (a GC pass, which R2 spreads over ~20 s) and 131 B. The largest 120 s mean was
1.54 A and 3.93 B a second. The run's total was 1,983 A, 6,802 B and 754 free deletes.

The checker passed: 1,260,001 distinct seqs, 0 violations, 0 holes, nothing acked lost, and the consumer
from cursor 0 read all of it back, dense, in 6.0 s. Acks were the same as on MinIO: p50 1.20 ms, p99
1.68 ms, max 32 ms.

Requests were the same as on MinIO too. Counted over the last 58 minutes, all nodes:

| cluster-wide, per second | Class A | Class B | free (deletes) |
|---|---|---|---|
| MinIO (run 2) | 0.478 | 1.660 | 0.146 |
| R2, counted | 0.478 | 1.725 | 0.148 |
| R2, counted, less one-key `DeleteObjects` (what R2 bills) | 0.330 | 1.725 | |
| `cost_model.py`, today at 30 s (GC deletes left out) | 0.33 | 1.66 | |

By purpose the split matches MinIO's to within ~0.04 a second everywhere. B is 4% higher, from
SlateDB's GC-boundary and compactions reads (0.579 and 0.293 B/s, against 0.547 and 0.257). The
followers sent 3 requests each, at start.

Cloudflare's own count, from the bucket's metrics in the dashboard, for a window where only the three
nodes ran (21:30:00-22:00:00, counters interpolated between 10 s polls, under 2 requests of error at
each end):

| 21:30-22:00 | Class A | Class B |
|---|---|---|
| counted | 1,006: PUT 140, PUT create 334, PUT CAS 66, LIST 75, one-key `DeleteObjects` 391 | 3,277: GET 2,365, ranged GET 908, HEAD 5 |
| Cloudflare (legend total; the tile showed 650 A, 3.42k B with an edge minute) | 626 | 3,280 |

- R2 doesn't bill a one-key `DeleteObjects` as Class A. 1,006 - 391 = 615, and Cloudflare counted 626.
  So SlateDB's GC deletes are free on R2, and the bill is ~0.33 A a second. The cost model leaves them
  out (S3 and GCS don't bill deletes either). The guards still count them as Class A, which is
  conservative.
- The counters miss no Class B: 3,277 against 3,280. Every request goes through `Store::counted`
  above object_store's own retries, and none were logged.
- The day's totals (1.32k A, 9.46k B) fit for A. B was ~2,500 over, from the hour's final
  `qlog verify`, which ran in a process of its own outside both guards. It read the state at ~8-9 GETs
  a second for ~12 minutes (~6,000 GETs) until it was killed. Since then `qlog check`, `verify` and
  `retain` take the same guard flags, each with a cap of its own counted into the cluster total, and the
  R2 run skips the final verify (the consumer from cursor 0 checks the log).

R2 round trips from the bench box, from the nodes' `vlpds_object_store_request_seconds` (to the
response head for GETs, the first page for LISTs). The histogram's buckets double, so these are ranges:

| op | requests | p50 | p90 | p99 | max |
|---|---|---|---|---|---|
| GET | 5,074 | 51-102 ms | 102-205 ms | 410-819 ms | 3.3-6.6 s |
| ranged GET | 1,711 | 102-205 ms | 205-410 ms | 410-819 ms | 0.8-1.6 s |
| HEAD | 16 | 51-102 ms | 102-205 ms | | 102-205 ms |
| LIST | 165 | 102-205 ms | 205-410 ms | | 205-410 ms |
| PUT | 272 | 205-410 ms | 410-819 ms | 3.3-6.6 s | 6.6-13.1 s |
| PUT create | 660 | 205-410 ms | 205-410 ms | 0.8-1.6 s | 3.3-6.6 s |
| PUT CAS | 132 | 205-410 ms | 410-819 ms | 0.8-1.6 s | 1.6-3.3 s |

Reads are at or under the ~200 ms round trip the estimates assumed, and writes are 1-2x it, with a long
tail.

Flushes take seconds on R2. Over 120 flushes, seal to CAS: p50 2.35 s, p90 3.34 s, p99 10.2 s, max
11.4 s (MinIO at 350/s and 30 s: 74 ms p50, 80 ms max). The applier's pause for the seal: p50 0.92 s,
p99 7.9 s, max 8.5 s (MinIO: 21 ms). A flush sends ~20 requests (12.5 GETs, ~6 PUTs, a LIST), many of
them one after another: the seal's L0 and checkpoint writes, then the segment PUT, then the manifest
CAS. Acks don't wait for either. The seconds with a flush had a worst per-second ack p99 of 23 ms and a
max of 32 ms (2.99 ms and 6.9 ms in the seconds without). Consumers do wait for the applier: submit to a
node's consumer was p50 8.6 ms and p99 38 ms. At 30 s even the 11 s outlier left 19 s. At a 10 s flush
the p99 would eat the interval, so on R2 30-60 s stays the choice.

What that does to the estimates that assumed ~200 ms round trips:

- Recovery after a lost quorum (~6 round trips, mostly writes) is ~1.5-2.5 s on the bucket side, not
  ~1-1.5 s. Detection still dominates the model's ~5 s.
- A membership change (a manifest round plus the `qlog/leader` CAS) pauses commits ~1-1.6 s on R2, not
  ~0.6-1 s, and up to several seconds on a PUT in the tail. Moving the barrier's segment out of the
  pause is worth more than it looked.
- A takeover's CAS (~50-300 ms assumed) is a PUT CAS at 205-410 ms p50 and up to 1.6 s at p99.

Storage: Cloudflare averaged 20.45 MB over the day. The run's padded frames compress ~600x, so this
says nothing about the log's real size.

## One VPS, one node

The host: an OVH VPS in a US West datacenter with 2 vCPU (KVM, "Intel Core Processor (Haswell, no
TSX)"), 3.8 GB RAM and no swap. The disk is a 40 GB virtio `QEMU HARDDISK` (ext4, write cache "write
back", `discard`), on Ubuntu 26.04 and kernel 7.0. Measured 2026-10-06 from 17:15 to 19:20 UTC. The
RTT from the bench box is 4.5 ms (same metro).

`tests/qlog/fsync_probe.sh`, four runs over two hours (p50 / p99 in ms):

| | 17:15 | 17:48 | 18:04 | 19:18 |
|---|---|---|---|---|
| fdatasync 4 KiB, 1 writer | 0.61 / 0.94 | 0.57 / 0.95 | 0.59 / 0.86 | 0.62 / 0.90 |
| fdatasync 64 KiB, 1 writer | 0.68 / 0.95 | 0.63 / 1.07 | 0.65 / 1.07 | 0.66 / 0.91 |
| fdatasync 1 MiB, 1 writer (MiB/s) | 1.12 / 2.15 (548) | 1.09 / 2.25 (548) | 1.12 / 1.53 (519) | 1.16 / 1.50 (568) |
| fdatasync 64 KiB, 3 writers (MiB/s) | 0.86 / 1.42 (175) | 0.86 / 1.35 (179) | 0.87 / 1.38 (173) | 0.86 / 1.38 (178) |
| unsynced write ceiling | 1,565 MiB/s | 1,499 | 1,428 | 1,491 |

That's steady across two hours and well above 0.1 ms, so the flush really reaches the hypervisor's
storage and doesn't stop at a cache. It's 0.6-0.7 ms at commitlog-sized appends, the "0.5-2 ms VPS" the
model assumed and 4x better than the bench box's consumer NVMe (2.7 ms). Three writers cost ~0.2 ms
more, against 2x on the bench box. The 1 MiB rate (~550 MiB/s fsynced) is past what one member needs at
100x (~185 MB/s).

A single node, with one `chaos.sh` scenario run on the host: the commitlog on the disk, a userland MinIO
on the same disk, 30 s flushes, a 256 MiB ring. The load generator and the checker ran on the box too,
so all four share its 2 vCPU. Ack is submit to the node's fsynced ack, on loopback:

| | 350/s | 3,500/s | 30,000/s | 45,000/s | 60,000/s |
|---|---|---|---|---|---|
| ack p50 / p99 | 1.16 / 2.43 ms | 1.58 / 4.23 | 4.05 / 11.5 | 15.4 / 456 | saturated (commit p50 310 ms, p99 8.2 s) |
| submit to the consumer, p50 / p99 | 8.6 / 38 ms | 3.2 / 6.6 | 6.6 / 16.8 | 23 / 899 | |
| commitlog fsync p50 / p99 | 0.83 / 1.58 ms | 0.93 / 2.43 | 1.34 / 3.66 | 2.65 / 12.8 | 8.6 / 73 |
| events a group commit | 2 | 18 | 150 | 225 (p99 4,275) | 2,101 |
| node CPU (cores), max RSS | 0.11, 360 MB | 0.27, 527 MB | 0.86, 655 MB | 1.27, 959 MB | 1.14, 1.8 GB |
| checker | PASS | PASS | PASS | PASS | none: the kernel OOM-killed the checker, a 1.7 GB process (the node kept running) |

- The ceiling is ~30,000-40,000/s with everything on 2 vCPU, about 85x today's rate. Above that the box
  runs out of CPU before disk: 45,000/s is 240 MB/s fsynced, under half the fio rate. A dedicated node
  with its load arriving over the network would go higher. So 10x is easy on this class of VPS, and
  100x is about the limit of a 2-vCPU box.
- Against the emulated rows. At 3,500/s the commitlog fsync is 0.93 ms. The nearest bench-box row,
  "tmpfs + 1 ms", has a 3-node ack of 1.26 / 1.92 ms. The VPS's single node acks at 1.58 / 4.23, with no
  replication RTT but with the load generator, checker and MinIO competing for 2 vCPU, so its p99 is
  the CPU's. The "ack ≈ RTT + 1-1.7x fsync" rule holds: 1.58 ms is 1.7x 0.93.
- At 350/s the consumer sees an event 8.6 ms after submit (p99 38), against 3.2 ms at 3,500/s. The ack
  is 1.2 ms at both rates, so this is the emit side waking idle vCPUs. An idle merger tick pays a VPS's
  wake-up latency, which is worth a look on a real node.
- Restarts. kill -9 under 3,500/s paused emission 1.6-3.2 s (median 2.4 s, n=5). A power cut paused
  1.6-2.8 s (median 2.1 s, n=5). Both include the harness's 1 s restart delay, and recovery replayed
  1.8 GB of commitlog. On the bench box the same single-node restart is 1.2-1.5 s. Both runs passed the
  checker, with nothing acked lost and the consumer from cursor 0 dense and matching.
- Over the internet. The node on the VPS, driven from the bench box through an ssh tunnel (4.5 ms RTT),
  acked at 5.97 / 8.66 ms p50 / p99 at 350/s and 6.79 / 11.6 ms at 3,500/s, so RTT + ~1.5-2.3 ms.
  That's what a host owner in the same metro sees.

## Three hosts over Tailscale

One fsync-mode run of a real three-host cluster, on 2026-10-07 from 02:20 to 02:28 UTC. The run was
stopped after the first pass. The members were n1 on the VPS above, n2 on the bench box and n3 on a VM
on a home server (16 vCPU, a `QEMU HARDDISK` on LVM), talking over Tailscale. MinIO (for `qlog/leader`
and the bucket), the load generator and the checker all ran on the bench box. All three hosts ran one
Haswell build, each commitlog was on its own host's disk, and flushes ran every 10 s. The VPS was listed
first in the member set, so with no leader yet it campaigned first. The plan was 180 s of steady load,
then a leader kill 15 s into a 60 s load with a restart 1 s later. The home server had no fio and no
root, so `tests/qlog/fsync_probe.py` ran the same tests in Python (on the VPS it read within ~5% of
fio).

| fdatasync p50 / p99 | 4 KiB | 64 KiB | 1 MiB (MiB/s) | 64 KiB, 3 writers |
|---|---|---|---|---|
| VPS (fio) | 0.59 / 0.91 ms | 0.70 / 0.97 | 1.17 / 1.73 (515) | 0.90 / 1.52 |
| home server VM (python) | 1.04 / 7.17 | 1.15 / 1.84 | 2.06 / 3.02 (289) | 2.37 / 4.51 |

| tailnet RTT, 200 pings | p50 | p99 |
|---|---|---|
| VPS - home server | 5.19 ms | 6.45 |
| bench box - VPS | 5.67 | 7.64 |
| bench box - home server (one LAN) | 0.93 | 2.71 |

Every path was direct (no DERP relay).

The VPS leading at 3,500/s hit a Tailscale ceiling. At 3,500/s a leader sends ~37 MB/s of frames (5.3 KB
each, to two followers) and takes in ~19 MB/s of submits. On the VPS all of that goes through
tailscaled, and wireguard-go encrypts in userspace. tailscaled sat at 145% of the VPS's 2 vCPU while
the node used 27%. The VPS's tailnet egress topped out at ~21 MB/s. Followers missed heartbeats and
called elections. The VPS won back epochs 3 and 5, then the bench box took epoch 6, 75 s into the run.
Until then the client ack was 0.9-4 s p50, the load retried 114,266 submits, and the bench box's
follower fell ~18k seqs behind. Local runs sustain 3,500/s and far more, so the limit was userspace
WireGuard on a 2-vCPU box. A leader there wants a cheaper transport (kernel WireGuard, or TLS between
members) or more cores. That wasn't tried.

Once the bench box led (followers the home server VM, 0.9 ms away, and the VPS, 5.7 ms away), for the
last 105 s at 3,500/s:

| | measured | expected |
|---|---|---|
| client ack (bench box to the leader, local) | 3.6 ms p50 · per-second p99 16 ms median, 202 worst | commit ≈ max(leader fsync, RTT + follower fsync) = max(2.8, 0.9 + 1.4) ms, times 1-1.7: 2.8-4.8 ms. The emulated "2.7 ms" row gives 2.96 / 3.17 |
| commitlog fsync p50 / p99 | bench box 2.8 / 9.7 ms, home server 1.4 / 8.5, VPS 1.0 / 4.3 | fio: bench box 2.7, home server 1.15, VPS 0.70 |
| events per group commit | p50 18, p99 795 | 17-18 at 3,500/s |
| node CPU, max RSS | VPS 0.23 cores, 781 MB · bench box 0.12, 1.5 GB · home server 0.22, 1.1 GB | 0.06-0.07 leader, 0.04 follower (bench box, loopback) |
| busy cores, whole box | VPS 0.92, bench box 0.85, home server 2.05 (its own services included) | |

With a leader on the LAN the ack is the leader's own fsync (the bench box's consumer NVMe), as the model
says. The extra CPU and RSS come from catching up after the first 75 s (183-278 disk reads, 4 GB of
commitlog on each node), so they aren't steady-state numbers.

The kill landed on the VPS, which by then was a follower, so it was a follower kill and not the planned
leader failover. The emission pause was 212 ms. The VPS's restart replayed 4.0 GB of commitlog (61
segments) in 5.4 s, ~1.35 s a GB against the bench box's ~0.35, on 2 vCPU. It answered at +5.6 s,
followed at +5.6 s, and caught up to the leader's commit at +9.8 s. In the 60 s around it the ack was
11.3 ms p50 and 205 ms p99, with 5 retries.

The checker passed: 891,399 seqs, 0 violations, 0 holes, 0 acked-but-not-emitted, 0 re-ingested. A
consumer from cursor 0 through the bucket read all of them dense and matching in 82.5 s. Three leader
changes under overload and a kill all came out clean. 31,321 events were emitted at two seqs. Those are
the load generator's retries of submits that timed out but committed, which is at-least-once from the
submitter's side, and none crossed a gap.

Not run: `page-cache` and `memory` durability, the 350/s runs that would show a VPS-led commit away from
the WireGuard ceiling (expected ≈ 5.2 ms RTT to the home server + ~1 ms fsync, so ~6-7 ms commit and
~12-13 ms at a submitter in the same metro), and a real leader kill.

## Measuring your own hosts

1. fsync, on each box, on the disk the commitlog would use (no root, needs `fio`, or
   `tests/qlog/fsync_probe.py` without it):

   ```
   tests/qlog/fsync_probe.sh /var/lib/vlrelay/probe
   ```

   It prints fdatasync p50, p99 and p99.9 for 4 KiB, 64 KiB and 1 MiB appends with one writer, 64 KiB
   with three, and the unsynced write ceiling. Run it three times at different hours, since VPS disks
   are shared. A VPS whose fdatasync is well under 0.1 ms is acknowledging from a cache (hypervisor or
   controller), so don't count on it surviving a host power cut.
2. The cluster, three boxes (an OVH VPS-2 in each of three locations, or three Hetzner CX33/CAX21 in a
   spread placement group):
   - Build `qlog` for the target's CPU. `.cargo/config.toml` builds for `target-cpu=native`, and a
     binary built on a Zen 5 box dies with SIGILL on a VPS's Haswell vCPU:
     `RUSTFLAGS="-C target-cpu=haswell" CARGO_TARGET_DIR=<a separate dir> cargo build --profile dev-release --bin qlog`,
     then `objcopy --strip-debug` (330 MB to 39 MB).
   - On box 1, MinIO for `qlog/leader` (one GET and one CAS a takeover):
     `docker compose -f tests/qlog/compose.yml up -d --wait minio && docker compose -f tests/qlog/compose.yml run --rm minio-init`.
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

   - Repeat at `--rate 350` and `35000`. Then kill -9 the leader's `qlog node` (and two, and all three)
     under `--rate 3500` and restart it, for the takeover pause and the restart recovery.
   - `report.py` gives the ack and end-to-end latency, each node's fsync p50 and p99 with the
     group-commit sizes, CPU and RSS. Compare them with the "tmpfs + N ms" row above that matches the
     host's fsync.
3. A single node needs no docker or root. Run one `chaos.sh` scenario on the host with `S3_ENDPOINT`
   pointing at a MinIO you started yourself and the commitlog on the host's disk. `chaos.sh`'s
   timestamps work with Ubuntu 26.04's uutils `date`, which ignores `%3N`.
