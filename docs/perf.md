---
title: Performance
section: Reference
order: 302
summary: "How many events a second a node and a cluster take with every event checked, where the CPU goes, what time to firehose looks like, and how many consumers a node can feed."
---

```hero
diagram:
  caption: The bench. A synthetic PDS fleet emits signed, production-sized sync 1.1 events from 40 hosts and 100k accounts. The relay runs pinned to its own cores, and a checker compares its stream with every upstream's for missing, extra, reordered and duplicate events.
  nodes:
    - { id: fleet, label: fakepds fleet, sub: "40 hosts · 100k DIDs", at: [0, 3], size: [9, 3], tone: muted, stack: true }
    - { id: relay, label: vlRelay, sub: 8 pinned cores · 12 GB cap, at: [14, 3], size: [10, 3], tone: accent }
    - { id: minio, label: MinIO, sub: on local disk, at: [29, 0], size: [9, 2.6], shape: store, tone: amber }
    - { id: check, label: e2e_check, sub: "+ N-1 more consumers", at: [29, 6], size: [9, 3], tone: blue }
  edges:
    - "fleet -> relay: subscribeRepos"
    - "relay.r -> minio.l: segment PUTs"
    - "relay.r -> check.l: firehose"
    - { from: fleet.b, to: check.b, label: the upstreams' own streams, dash: true, via: [[4.5, 11], [33.5, 11]] }
facts:
  - { value: "~90–95k", unit: events/s, label: one 8-core node, note: "every event checked, 0 rejects, clean e2e", tone: amber }
  - { value: "~65–70", unit: µs, label: of CPU per event, note: "a third of it is the secp256k1 signature", tone: accent }
  - { value: "~98k", unit: events/s, label: on 3 nodes of 3 cores + SMT, note: "100k on 3 × 8 cores fits with 1.6–2× headroom", tone: violet }
  - { value: "~1.6", unit: Gb/s, label: per full-firehose consumer, note: "at 33k events/s; ~0.025 cores each", tone: blue }
```

The design target is 100k events/s on 3 nodes of 8 cores and 32 GB, so about 33k/s per node
([Design](design.md#scale)). Bluesky's whole network averages ~350 events/s today. One node meets
the per-node target on ~2.4 cores, and a scaled-down three-node cluster meets the whole target
with room to spare. CPU sets the ceiling, and the network decides how many consumers a node can
feed.

## The bench

The upstream is `fakepds`, a synthetic PDS fleet. A real vlpds tops out at a few thousand writes a
second on a laptop, so one fakepds process plays many hosts, and four of them emit ~170k signed
sync 1.1 events/s with about 7 cores. Frames are production-sized (p50 5.3 KB, p99 10 KB).

Each load step starts a fresh fleet (4 processes × 10 hosts × 2,500 accounts) at the offered rate
and a fresh relay (`dev-release` build) on a new bucket prefix. The relay is pinned to 8 physical
cores and capped at 12 GB. After 30 s of warmup, `e2e_check` subscribes to all 40 upstreams and
the relay, as the correctness check and the first full-firehose consumer, and the step measures
for a fixed time. The summary has the relay's rates, time to firehose (upstream emit to relay
emit, both timestamped on receipt by the checker), time to durable, segment PUT latency, CPU per
thread pool and stage, peak RSS, PUTs/s and the checker's counts.

Caveats:

- MinIO on the bench box's disk stands in for S3. Its PUT latency grows with bytes in flight, so
  above ~50k/s it sets the time to firehose. S3 would look different there, and that needs
  measuring.
- The box (16 cores) was shared with other work. Steps at the same rate moved by up to 2× in p99
  between runs.
- Loopback stands in for the NIC.

The full log, iteration by iteration with every table and profile, is in the repository next to
these pages, with the bench scripts (`scripts/perf.sh`, `scripts/cluster-perf.sh`).

## One node

One node with 8 pinned cores sustains ~90–95k events/s, every event checked, with 0 rejects and a
clean e2e. That's 2.8× the 33k/s target. It takes ~106k/s at its CPU ceiling (7.2 of 8 cores),
where latency grows with any backlog. So the ceiling is the CPU.

| Part | µs per event |
|---|---|
| secp256k1, the signature itself (the floor) | ~24 |
| The rest of verify: block hashes, MST inversion, commit decode | ~5 |
| Dispatch, parse, apply, the lane, spam signals, the clock | ~13 |
| Main runtime: upstream sockets, sequencer, committers and SlateDB, scheduling | ~15–16 |
| zstd -1 on the segments (~5 on production frames) | ~9 |

That's ~65–70 µs per event, linear in the rate, so 8 cores give ~115k/s with no headroom. The
33k/s target takes ~2.4 of the 8 cores, with p99 ~100–140 ms and 0 rejects.

Time to firehose is linger plus one segment PUT. At 33k/s it's ~50–65 ms p50 and ~100–140 ms p99
on disk MinIO, and 32 / 50 ms with the bucket in RAM. Above ~50k/s a segment seals on size
(8 MiB, `--max-segment-mb`) before its 25 ms linger is up, and an 8 MiB PUT took 100–400 ms on this
MinIO, so the p50 climbs to ~150–250 ms at 75–90k.

What got it there, from ~50k/s as first measured:

| Change | Why |
|---|---|
| 32 segment PUTs in flight, up from 4 (`--log-inflight`) | Past ~40k/s segments seal on size, and 4 PUTs of ~100 ms each capped the log at ~40 segments a second |
| jemalloc, without its oversize arena for 8 MiB segment buffers | libc malloc was 13% of samples, and page faults on fresh 8 MiB buffers another ~8% |
| A committer per DID shard | One committer gave out at ~90–95k/s, and `vlrelay_ack_pending` showed it |
| zstd -1 for segments (`--log-compression`) | Half of level 1's CPU for 0.6% more bytes |
| SigV4 `UNSIGNED-PAYLOAD` on https endpoints | Each PUT hashed its whole 8 MiB body on a runtime worker |
| Metric labels resolved once, foldhash for hot maps | SipHash ran about ten times per event |

## Compression

Segments are zstd'd before they're PUT. Firehose frames are mostly CIDs and signatures, so they
don't compress much. Over 8 MiB of production frames read off `bsky.network`, on one core:

| Level | Ratio | MB/s | µs per event (5.4 KB) |
|---|---|---|---|
| 1 (vlpds's default) | 1.569 | 575–645 | ~9 |
| -1 (the relay's default) | 1.559 | 1,070–1,150 | ~5 |
| -3 | 1.542 | 1,450–1,540 | ~3.6 |
| -8 | 1.421 | 1,820 | ~3 |
| 0 (none) | 1.0 | | 0 |

-3 would save another ~1.4 µs per event for 1.8% more bytes, and storing segments raw would save
all of it (~13% of the node) for 57% more bucket bytes and PUT bandwidth. That's a cost decision
for the operator, so the default stops at -1.

## A three-node cluster

The bench box can't host three 8-core nodes and a fleet fast enough to load them, so the cluster
was scaled down: 3 cores of 3 physical cores plus SMT each (6 hardware threads, 7 GB cap), on one
MinIO, with peer mTLS on loopback. The same 6 threads also ran one node alone, so the cluster's
cost per event compares with a single node's on identical hardware.

| Offered | Accepted/s | e2e TTF p50 / p99 | CPU (of 18 threads) | µs per event | Peer MB/s | RSS max | e2e |
|---|---|---|---|---|---|---|---|
| 33k | 34.2k | 181 / 484 ms | 5.57 | 163 | 521 | 1.7 GB | clean, all 3 streams identical |
| 60k | 63.7k | 362 / 699 ms | 9.93 | 156 | 932 | 2.2 GB | clean |
| 90k | 96.9k | 0.78 / 1.9 s | 14.47 | 149 | 1,304 | 4.3 GB | clean |
| 120k | 120.8k | | 17.21 | 142 | 1,533 | 5.9 GB | out 104–115k/s, backlog growing |

- Every step that kept up was clean: 0 missing, reordered or duplicated events across 2.4–6.5M
  events per step. At 33k, all three nodes' streams over the same window (847,161 events) had the
  same events at the same seqs.
- The three nodes take ~98k/s cleanly. At 120k they reach the CPU ceiling, and the merged stream
  trails intake.
- At 33k the cluster's time to firehose is p50 181 ms, against 55 ms for one node on the same
  cores. A cluster node emits an event once every log's watermark has passed it, so the slowest
  of three logs sets the pace. Each log also seals on linger at a third of the rate, which triples
  the PUTs. From 90k up, MinIO's 410–819 ms PUTs dominate.

On the same threads, the cluster costs 142–156 µs per event and one node costs 89–94, about 1.65×.
The extra ~55–65 µs is copies and TLS in proportion to the bytes on the peer links:

| Cluster cost | Per event |
|---|---|
| Receiving and serving the log streams (every core streams its log to both peers) | ~20 µs |
| The forward hop to the DID owner (two thirds of events): serialization ~8, HTTP/2 and TLS ~7.5 | ~15 µs per forwarded event |
| Dense-seq renumbering | under 1 µs |
| Peer bytes | 12.7–15.2 KB |

Scaled to 3 × 8 cores, 100k events/s takes 50–60% of the cluster's CPU: 1.6× headroom if "8
cores" means 8 vCPUs, ~2× if it means 8 physical cores. Memory is 4–6 GB per node at 100–120k.
Three things qualify it. The time to firehose at 100k was MinIO-bound here (p50 0.8–1.4 s) and
needs measuring on S3. The peer links carry ~3.5 Gb/s each way per node at 100k, before
consumers. And host placement is by hash while ingest load follows hosts, so one core verified
about half what another did.

## Fan-out

N full-firehose consumers at 33k events/s, on one node with disk MinIO:

| Consumers | Firehose out | Relay CPU | Firehose threads | TTF p50 / p99 | Each consumer got | Disconnects |
|---|---|---|---|---|---|---|
| 1 | 203 MB/s | 2.52 | 0.03 | 48 / 89 ms | 34.8k/s | 0 |
| 4 | 786 MB/s | 2.63 | 0.15 | 53 / 108 ms | 34.8k/s | 0 |
| 8 | 1.59 GB/s | 2.79 | 0.25 | 56 / 121 ms | 35.0k/s | 0 |
| 16 | 3.16 GB/s | 3.03 | 0.44 | 58 / 146 ms | 34.8k/s | 0 |
| 32 | 6.29 GB/s | 3.45 | 0.81 | 57 / 115 ms | 34.6k/s | 0 |

Each consumer costs ~0.025 cores and ~195 MB/s (1.6 Gb/s). There are 4 serve threads, so firehose
CPU tops out at 4 cores, or ~150 consumers. The NIC runs out first:

| NIC | Consumers at 33k events/s |
|---|---|
| 10 GbE | ~6 |
| 25 GbE | ~15 |
| 100 GbE | ~60 |

So a node serving many consumers wants edges or replicas, not more cores. At today's ~350 events/s
the same NIC feeds a hundred times more.

## What's next

- Compressed log streams between cores. Streaming sealed (zstd'd) segments to peers instead of raw
  frames would cut the peer bytes by about a third, and the copy and TLS cost with them.
- A DID owner's slow DID-document lookup holds its whole forward batch. When the fleet's PLC fell
  behind at 150k offered, every node stalled. The host owner could pass the key it just resolved.
- Cold DID lookups cost ~1.7× on a cluster, since the host owner and the DID owner each resolve.
  `--plc-export` seeding covers this in production.
- Host shards placed by load instead of by hash.
- Time to firehose on S3 and R2, not MinIO.
