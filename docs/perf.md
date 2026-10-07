---
title: Performance
section: Reference
order: 302
summary: "How many events a second a node takes with every event checked, where the CPU goes, what the quorum log adds, and how many consumers a node can feed."
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
    - "relay.r -> minio.l: bucket writes"
    - "relay.r -> check.l: firehose"
    - { from: fleet.b, to: check.b, label: the upstreams' own streams, dash: true, via: [[4.5, 11], [33.5, 11]] }
facts:
  - { value: "~90–95k", unit: events/s, label: one 8-core node, note: "every event checked, 0 rejects, clean e2e", tone: amber }
  - { value: "~65–70", unit: µs, label: of CPU per event, note: "a third of it is the secp256k1 signature", tone: accent }
  - { value: "~1.3", unit: ms, label: submit to consumer on the quorum log, note: "3 nodes on loopback at 350/s · 2 ms at 100x", tone: violet }
  - { value: "~1.6", unit: Gb/s, label: per full-firehose consumer, note: "at 33k events/s; ~0.025 cores each", tone: blue }
```

The design target is 100k events/s on 3 nodes of 8 cores and 32 GB, so about 33k/s per node
([Design](design.md)). Bluesky's whole network averages ~350 events/s today. One node checks
33k/s on ~2.4 cores. The quorum log itself costs little: it carried 200k events/s on three nodes
in its own bench, at under 2 cores on the leader. CPU for verify sets the ceiling, and the network
decides how many consumers a node can feed.

## The bench

The upstream is `fakepds`, a synthetic PDS fleet ([Load fleet](loadfleet.md)). A real vlpds tops
out at a few thousand writes a second on a laptop, so one fakepds process plays many hosts, and
four of them emit ~170k signed sync 1.1 events/s with about 7 cores. Frames are production-sized
(p50 5.3 KB, p99 10 KB).

Every load step ran on one 16-core bench box. A step started a fresh fleet (4 processes × 10
hosts × 2,500 accounts, so 100k DIDs) at the offered rate and a fresh relay (`dev-release` build)
on a new bucket prefix. The relay ran pinned to 8 physical cores, with their SMT siblings left
idle, and everything else (the fleet, MinIO and the checkers) shared the other cores. Each process
ran under its own memory cap:

| Process | Memory cap |
|---|---|
| Relay | 12 GB (peak RSS was 4.6 GB, at 110k/s) |
| Fleet | 4 × 5 GB |
| MinIO, data on the box's disk | 4 GB |
| `e2e_check` | 4 GB |
| Each extra consumer (`fakepds consume`) | 400 MB |

After 30 s of warmup, `e2e_check` subscribed to all 40 upstreams and to the relay. It's the
correctness check and the first full-firehose consumer. The step then measured for a fixed time.
The summary has the relay's rates, time to firehose (upstream emit to relay emit, both timestamped
on receipt by the checker), time to durable, segment PUT latency, CPU per thread pool and stage,
peak RSS, PUTs/s and the checker's missing, extra, reordered and duplicate counts. A profile is
`perf record` with DWARF call graphs on the relay for 10 s mid-step.

To run a step yourself, start with a bucket. We used MinIO in Docker with its data on local disk,
and on tmpfs for a bucket in RAM. Then build the binaries (`cargo build --profile dev-release
--bin vlrelay --bin fakepds --bin e2e_check`, into `target/dev-release/`) and start the fleet.
Each host listens on port 30000 plus its global index:

```bash
# one of four fleet processes; the other three take --host-base 10, 20 and 30 and no --plc-port
fakepds run --host-base 0 --hosts 10 --dids 2500 --rate 25000 --gen-threads 6 \
  --lag-secs 60 --replay-mb 1024 --plc-port 29999
```

Pin the relay and cap its memory (`systemd-run --user -p CPUAffinity=8-15 -p MemoryMax=12G`, or
`taskset`), and point it at all 40 hosts and the fleet's PLC. A node with no `--qlog-peer` is a
one-member quorum log, and `--qlog-dir` gives it a commitlog:

```bash
systemd-run --user -p CPUAffinity=8-15 -p MemoryMax=12G -- vlrelay \
  --s3-endpoint http://127.0.0.1:9000 --s3-bucket vlrelay --s3-region us-east-1 \
  --s3-access-key minioadmin --s3-secret-key minioadmin --s3-unsigned-payload true \
  --prefix bench-1 --qlog-dir /mnt/nvme/vlrelay --admin-token bench \
  --plc-url http://127.0.0.1:29999 --did-lookups-per-sec 200000 \
  --host http://127.0.0.1:30000 --host http://127.0.0.1:30001 ... --host http://127.0.0.1:30039
```

100k cold DIDs resolve at once on a fresh relay, so before the warmup the bench also lifted the
policy's PLC lookup budget (`cluster.plcLookupsPerSec`) through `PUT /admin/api/policy/full`
([Admin API](admin-api.md)). Then check the stream and add consumers:

```bash
e2e_check --upstream http://127.0.0.1:30000 ... --upstream http://127.0.0.1:30039 \
  --relay http://127.0.0.1:2980 --duration 60 --settle 10 --report-only --json-out step.json
fakepds consume --host http://127.0.0.1:2980 --duration 65    # each extra consumer
```

The fleet runs with `--lag-secs 60 --replay-mb 1024` for a reason. With the defaults (1 s and
256 MB), a relay that fell more than 1 s behind while 100k cold DIDs resolved got dropped, and the
replay ring couldn't cover the reconnect. The gaps then desynchronized 1.2M of the accounts' chains.
Production PDSes keep far more replay than that.

Caveats:

- MinIO on the bench box's disk stands in for S3. Its PUT latency grows with bytes in flight, so
  above ~50k/s it sets the time to firehose. S3 would look different there, and that needs
  measuring.
- The box was shared with other work. Steps at the same rate moved by up to 2× in p99 between
  runs, so comparisons between builds were interleaved, minutes apart, at the same rate.
- Loopback stands in for the NIC.

The one-node numbers below were measured on the relay's earlier architecture, a single node that
PUT a log segment to the bucket every 25 ms. That node and its log are gone. Verify, parse and
apply are the same code on the quorum log, so their costs carry over. The log's share and the time
to firehose don't, and the quorum log's own numbers are in [The quorum log](#the-quorum-log).
Along the way the bench also found limits in that log (segment PUTs in flight, one committer task)
that the quorum log doesn't have, so they aren't repeated here.

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
33k/s target takes ~2.4 of the 8 cores, with 0 rejects. The last two rows were the earlier log's.
On the quorum log the leader compresses segments when it flushes them, off the path to consumers.

By thread pool, the 75k/s profile split ingest 58%, main runtime 25%, the commit pool 16% and the
firehose 1%. Past the signature (~33% of samples) and zstd (15%), nothing is over ~2% of samples,
so what's left above the floor is spread thin.

An SMT thread is worth less than a core. On 6 hardware threads (3 cores and their SMT siblings),
one node cost 89–94 µs per event at 25–50k/s, against ~70 for the same build on 8 whole cores.

Rejects were 0 at every step that kept up, and `e2e_check` found no missing, reordered or
duplicated events. Steps that fell behind showed missing and extra events only because the relay's
stream trailed the checker's window.

Changes from that work that still hold:

| Change | Why |
|---|---|
| jemalloc, without its oversize arena for big segment buffers | libc malloc was 13% of samples, and page faults on fresh 8 MiB buffers another ~8% |
| zstd -1 for segments (`--log-compression`) | Half of level 1's CPU for 0.6% more bytes |
| SigV4 `UNSIGNED-PAYLOAD` on https endpoints | Each PUT hashed its whole body on a runtime worker |
| Metric labels resolved once, foldhash for hot maps | SipHash ran about ten times per event |
| Spam signals evict from a batch of candidates | The per-account table scanned 512 entries on nearly every add (2.4% of samples) |

Tried and not kept: `--lanes 16` against the default 64, and `--ingest-threads 6` against 8. Both
were within run-to-run noise (65.8 against 66.7 µs per event, 65.2 against 65.4), so the defaults
stay.

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

The fleet's frames are a poor stand-in here. Their padding is random English-ish words, which
compress 2.3x at level 1 (2.1x at -1) and slowly, so a fleet bench overstates both the ratio and
zstd's CPU.

## The quorum log

The quorum log was benched on its own (`qlog load` and `qlog check`, three `qlog node` processes on
one 16-core box over loopback), with ~5.3 KB frames, the network's mean. These are the log's
costs: replicate, commit, emit. Verify and apply come on top, as in the table above.

| | 350/s (today) | 3,500/s (10x) | 35,000/s (100x) |
|---|---|---|---|
| Leader append to quorum commit, p50 / p99 | 0.04 / 0.10 ms | 0.07 / 0.17 ms | 0.21 / 0.56 ms |
| Submit to the first consumer, p50 / p99 | 1.23 / 2.29 ms | 1.33 / 2.45 ms | 1.98 / 3.48 ms |
| CPU, leader / follower (cores) | 0.034 / 0.019 | 0.054 / 0.030 | 0.20 / 0.095 |

Those are memory-only numbers. Most of the path to a consumer is the firehose's 2 ms batching
tick, and a real placement adds the round trip between nodes (0.2 ms in one data center, ~3 ms
across a metro). In `fsync` durability (the default below three members), the ack costs about one
round trip plus 1–1.7 fsyncs. At 3,500/s with a 1 ms fsync, submit to quorum ack was 1.26 / 1.92 ms
(p50 / p99), and submit to the first consumer 2.42 / 3.54 ms. `page-cache` durability (the default
for three members or more, `--durability`) takes the fsync off the ack. At today's rate that acked
in 0.24 ms against 2.3 ms at a 2 ms fsync.

Offered 200,000 events/s for 30 s, three nodes committed all of it (1 GB/s a node) with the
commitlog on tmpfs, at 1.78 cores on the leader. With all three commitlogs on one consumer NVMe,
fsync bandwidth capped it at ~25,000/s. So disk bandwidth with fsync, not CPU, sets the log's
ceiling. 100x needs ~185 MB/s fsynced per node, which wants datacenter drives, and two members
should never share a disk.

| Fault, 3,500/s, commitlog | Emission pause (fault to the next new seq at any consumer) |
|---|---|
| kill -9 the leader | 55 ms median, 61 max (72 / 88 on the NVMe) |
| kill -9 a follower | 17 ms median, 38 max |
| Partition or SIGSTOP the leader | ~1 s (`--qlog-election-ms`) |
| kill -9 all three | ~2.4 s (a 1 s supervisor restart, recovery and the election) |

RAM with a commitlog is ~0.6 GB a node at 10x, mostly the firehose ring.

The relay end to end on the quorum log (fakepds through verify, the leader's checks and the log)
has run for an hour at 350/s on three nodes, with commitlogs on tmpfs behind a 1 ms emulated fsync.
Upstream receive to emit was p50 3.6 ms and p99 6.2 ms, at 0.115 cores on the leader and 0.06 on
each follower. It has also run through the chaos scenarios at 3,500/s with nothing missing. It
hasn't been benched for throughput at 10x and 100x yet. The rest of the quorum log's measurements,
real hosts and a real R2 bucket included, are on [The quorum log](quorum.md).

## Fan-out

N full-firehose consumers at 33k events/s, on one node with disk MinIO. This ran on the earlier
node too, but the serving side is the same (vlpds's firehose on 4 serve threads):

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

So a node serving many consumers wants a faster NIC, or more members to spread consumers over
(any member serves the stream), not more cores. At today's ~350 events/s the same NIC feeds a
hundred times more.

## What's next

- The relay end to end on the quorum log at 10x and 100x, on real hosts and R2 instead of loopback
  and MinIO.
- Parallel segment PUTs in the flush, for 100x on R2.
- A cold start with `--plc-export`. The leader seeds DID documents from the PLC directory's export
  into a database every member reads, so a cold relay doesn't resolve each account. Its effect at
  bench rates hasn't been measured.
- Host placement by load. The leader's host table spreads hosts by hash, and a few big PDSes carry
  most events.
- The upstream read buffer. Each socket reads 16 KB at a time, about three frames per `recv`, and
  `recv` is ~2% of samples. 64 KB would cut the syscalls but costs 48 KB more per host connection,
  ~150 MB across 3,000 hosts.
- The clock. About 10 clock reads per event remain (~2% of samples). Sharing them across stages
  would save ~0.5 µs.
- MST hashing. Each key of each loaded node is hashed for its height, ~0.3% of samples on the
  fleet's 50–100 record repos. Production repos are deeper, so a per-thread key-height cache may be
  worth measuring on production frames (`verify_bench`).
