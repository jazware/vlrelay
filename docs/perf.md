# vlRelay: node performance

How many events a second one relay node takes, and how fast they reach the firehose. The target in `docs/design.html` is 100k events/s on 3 nodes of 8 cores and 32 GB, so about 33k/s per node.

## The bench

`scripts/perf.sh` runs on benchbox, from `~/vlrelay-perf/vlrelay` (synced and built with `REMOTE_DIR=vlrelay-perf scripts/benchbox.sh build`).

```
scripts/perf.sh up                          # MinIO in docker, data on disk
scripts/perf.sh step NAME RATE [SECS]       # one load step; summary in perf-out/NAME.json
scripts/perf.sh down                        # stop everything, delete MinIO's data
python3 scripts/perf_show.py perf-out/*.json
```

A step does this:

1. It starts a fresh fleet (`scripts/fleet.sh`, 4 processes x 10 hosts x 2,500 accounts, so 100k DIDs) at RATE.
2. It starts a fresh relay (dev-release) on a new bucket prefix. The relay is pinned to cores 8-15 (`CPUAffinity`, 8 physical cores, their SMT siblings left idle) and capped at 12 GB.
3. After 30 s of warmup, it starts `e2e_check` against all 40 upstreams and the relay. That is the correctness check and the first full-firehose consumer. `CONSUMERS=N` adds N-1 more `fakepds consume` subscribers.
4. It measures for SECS. Everything except the relay (fleet, MinIO, checkers) runs on the other cores.

The summary has:

- the relay's in/accepted/out rates;
- time to firehose from `e2e_check`, which timestamps both sides on receipt, so this is upstream emit to relay emit;
- time to durable and the mean append-to-durable lag;
- segment PUT latency;
- CPU per thread pool, from `/proc`, and busy time per stage from `vlrelay_stage_busy_us_total`;
- peak RSS;
- PUT/s from MinIO's own metrics;
- rejects;
- `e2e_check`'s missing, extra, out-of-order and duplicate counts.

Frames are production-sized: p50 5.3 KB, p99 10 KB (`docs/loadfleet.md`).

Memory caps add up to 40 GB:

| Process | Cap |
|---|---|
| Relay | 12 GB (RSS never went past 3.3 GB) |
| Fleet | 4 x 5 GB |
| MinIO | 4 GB |
| `e2e_check` | 4 GB |

`PERF_RECORD=1` records `perf` (dwarf call graphs) on the relay for 10 s mid-step. benchbox's `/usr/bin/perf` doesn't match its kernel, so the script uses `/usr/lib/linux-tools-6.8.0-142/perf`.

Caveats:

- MinIO on benchbox's disk stands in for S3. Its PUT latency grows with bytes in flight, so above ~50k/s it sets the time to firehose. On S3 that part would look different.
- benchbox is shared. Steps at the same rate moved by up to 2x in p99 between runs, depending on what else was using the disk and the cores. The tables mark each case.
- Loopback stands in for the NIC.

## Iteration 0: as found (797d4602)

| Offered | Accepted/s | TTF p50 / p99 | Durable p50, lag | Relay CPU | Verify (busy) | RSS | PUT/s | Rejects | e2e |
|---|---|---|---|---|---|---|---|---|---|
| 10k | 10.8k | 29 / 43 ms | 34 ms, 26 ms | 1.13 | 0.45 | 1.2 GB | 48 | 0 | clean |
| 25k | 25.7k | 39 / 68 ms | 55 ms, 40 ms | 1.94 | 0.82 | 1.5 GB | 49 | 0 | clean |
| 33k | 34.6k | 53 / 97 ms | 55 ms, 49 ms | 2.65 | 1.14 | 1.8 GB | 49 | 0 | clean |
| 50k | 53.0k | 79 / 300 ms | 88 ms, 60 ms | 3.98 | 1.75 | 2.4 GB | 47 | 0 | clean |
| 50k (2nd run) | 53.0k | 220 / 1,843 ms | 225 ms, 425 ms | 4.06 | 1.79 | 2.1 GB | 47 | 0 | clean |
| 75k | 50.5k | 13 s / 26 s | 922 ms, 463 ms | 4.10 | 1.93 | 2.7 GB | 43 | 0 | fell behind |

The target holds as found: 33k/s on 2.65 cores with p99 under 100 ms. The node tops out at ~50k/s, though, and not because of CPU. At 75k offered it took 50k on 4.1 of its 8 cores.

The cause was the log. A segment seals on linger (25 ms) or at 8 MiB. Past ~40k/s it seals on size, and an 8 MiB PUT took ~100 ms on MinIO. With 4 PUTs in flight (`DEFAULT_INFLIGHT`), that caps the log at ~40 segments/s. The PUT rate was a flat ~48/s at every step, the signature of this cap. Once the log's queue fills, the lanes block in `log.submit` and the upstream reads stop.

The first 75k and 100k runs also tripped on the harness. During warmup, while 100k cold DIDs resolved, the relay fell more than 1 s behind. fakepds then dropped it (`--lag-secs 1`) and its 256 MB replay ring couldn't cover the reconnect. The resulting gaps desynchronized 1.2M of the accounts' chains (`desynchronized` rejects). Production PDSes keep far more replay than that. The bench now runs the fleet with `--lag-secs 60 --replay-mb 1024`.

## Iteration 1: PUT parallelism, jemalloc, lane acks (403d95a9)

Changes:

- `DEFAULT_INFLIGHT` 4 -> 16, plus `--log-inflight` and `--max-segment-mb` flags. At 75k/s, 16 in flight takes 80k/s where 4 took 50k/s.
- jemalloc as the relay binary's allocator. libc, mostly malloc, free and memcpy, was 13% of the samples in the 50k profile.
- A lane keeps its appended events' durable receivers in a `FuturesUnordered` and polls it between jobs, instead of spawning a task per event.

A segment PUT now records `vlpds_segment_put_seconds{partition="relay"}`.

K=4 against K=16 at 75k offered, same build otherwise:

| Inflight | Accepted/s | TTF p50 / p99 | PUT p50 / p99 | PUT/s | CPU |
|---|---|---|---|---|---|
| 4 | 50.5k | 13 s / 26 s (behind) | 102 / 205 ms | 43 | 4.10 |
| 16 | 79.8k | 211 / 730 ms | 205 / 410 ms | 62 | 6.21 |

| Offered | Accepted/s | TTF p50 / p99 | Durable p50, lag | PUT p50 | Relay CPU | RSS | e2e |
|---|---|---|---|---|---|---|---|
| 33k | 35.1k | 56 / 547 ms | 55 ms, 71 ms | 26 ms | 2.70 | 1.6 GB | clean |
| 50k | 55.0k | 368 / 5,771 ms | 360 ms, 297 ms | 205 ms | 4.78 | 2.4 GB | clean |
| 75k | 80.4k | 222 / 501 ms | 360 ms, 197 ms | 205 ms | 5.80 | 2.3 GB | clean |
| 100k | 90.5k | 10 s / 21 s | 576 ms, 321 ms | 410 ms | 6.65 | 2.8 GB | behind |

This ladder ran while another bench kept benchbox's disk busy (load average 24, disk at 99% util), which is where the 33k and 50k tails come from. The 75k step is the clean comparison: 5.80 cores for 80.4k, against 6.21 for 79.8k before. That's ~72 µs of CPU per event, down from ~78.

At 100k, MinIO is the wall. benchbox's disk (`dm-0`) was 99% busy writing ~260 MB/s, and MinIO writes with O_DIRECT.

An attempt to move MinIO to tmpfs, with a pruner that deletes log segments older than 10 s (`MINIO_TMPFS=1`), didn't work inside the 40 GB budget. At 33k it gave the cleanest numbers of the session: TTF p50 32 ms and p99 50 ms, durable lag 26 ms, PUT p50 6 ms. At 75k it filled 8 GB during warmup, and then MinIO stalled under its cgroup's memory pressure. The knob stays in `perf.sh` for a box with more RAM.

## Iteration 2: metrics and hashing (5330fa86)

The 75k profile after iteration 1:

| Where | Share of samples |
|---|---|
| secp256k1 (`fe_mul_inner`, `ecmult_strauss_wnaf`, `fe_sqr_inner`) | 31% |
| zstd on the commit pool (level 1) | 15% |
| sha256 (block hashes in verify) | 6% |
| vdso `clock_gettime` | 3% |
| SipHash | 2% |
| aws-lc sha256 (SigV4 payload hash on each PUT) | 1.5% |

By pool, the samples split ingest 57%, main 27% and the commit pool 15%.

Changes:

- The per-event metrics resolve their labels once: `metrics::{PARSE, IDENTITY, VERIFY, APPLY}` and the per-kind in/accepted counters. `with_label_values` hashes its labels with SipHash on every call, and each event made about ten such calls.
- `types::FastMap` (foldhash, seeded per process) for the state shard's pending maps, the TTF tracker and the ack tracker.

| Offered | Accepted/s | TTF p50 / p99 | Durable p50, lag | PUT p50 | Relay CPU | µs/event | RSS | e2e |
|---|---|---|---|---|---|---|---|---|
| 33k | 35.0k | 51 / 116 ms | 55 ms, 47 ms | 26 ms | 2.46 | 70 | 1.7 GB | clean |
| 50k | 51.0k | 69 / 165 ms | 88 ms, 52 ms | 51 ms | 3.54 | 69 | 2.0 GB | clean |
| 75k | 82.5k | 188 / 652 ms | 225 ms, 133 ms | 205 ms | 5.97 | 72 | 2.2 GB | clean |
| 100k | 84.7k | 13 s / 27 s | 576 ms, 418 ms | 410 ms | 6.50 | 77 | 2.8 GB | behind (MinIO) |

Past ~85k the wall is MinIO again: its PUT p50 doubles to ~410 ms. With MinIO's O_DIRECT off (`MINIO_ENV="-e MINIO_API_ODIRECT=off"`), the page cache absorbs its writes and the relay's own limits show:

| Offered | Inflight | Accepted/s | TTF p50 / p99 | Relay CPU | e2e |
|---|---|---|---|---|---|
| 75k | 16 | 80.0k | 209 / 498 ms | 5.77 | clean |
| 90k | 16 | 85.2k | 6.9 s / 14 s | 6.45 | behind (PUTs) |
| 90k | 32 | 96.6k | 289 ms / 3.4 s | 7.11 | clean |
| 100k | 16 | 98.9k | 1.4 s / 10 s | 7.29 | behind |
| 100k | 32 | 103.7k | 0.6 s / 13 s | 7.41 | behind (committer) |
| 125k | 16 | 92.7k | 20 s / 31 s | 6.99 | behind |

## Iteration 3: 32 PUTs in flight, TTF tracker per batch

Changes:

- `DEFAULT_INFLIGHT` 16 -> 32, since 16 PUTs in flight capped the node at ~85k.
- The TTF tracker takes a batch's seqs under one lock, on both the durable side and the emit side.

At 100k offered, a new limit shows: the committer. That is the one task that waits for durable tickets in order, commits their state changes to the shards' SlateDBs and only then answers each lane. Its own rate is the `vlrelay_time_to_durable_seconds` count:

| Run | Accepted/s | Committed/s |
|---|---|---|
| 75k, K=16 | 82.0k | 82.5k |
| 90k, K=32 | 96.1k | 96.8k |
| 100k, K=32 | 103.3k | 88.4k |

Past ~90k/s the committer falls behind. Events still reach the firehose, because emission only waits for the log, but host cursors stop moving and the ack backlog grows (`vlrelay_ack_pending` reached 1M).

Two attempts to widen the committer failed, and both were reverted:

- A leaner loop: bare tickets with their side data in a parallel queue, intake via `recv_many`, and a biased select. It committed 55k/s where the original managed 97k/s, and in one run nothing at all. The cause wasn't found in the time left.
- Each shard's commit as its own task (`tokio::spawn` per shard per batch). The committer stopped completing batches within ~25 s at 90k.

Final ladder (this commit, MinIO with O_DIRECT off):

| Offered | Accepted/s | TTF p50 / p99 | Durable p50, lag | PUT p50 / p99 | Relay CPU | µs/event | RSS | PUT/s | Rejects | e2e |
|---|---|---|---|---|---|---|---|---|---|---|
| 33k | 35.1k | 50 / 96 ms | 55 ms, 52 ms | 26 / 102 ms | 2.46 | 70 | 1.7 GB | 49 | 0 | clean |
| 75k | 78.4k | 145 / 263 ms | 225 ms, 150 ms | 205 / 205 ms | 5.59 | 71 | 2.3 GB | 61 | 0 | clean |
| 90k | 94.2k | 191 / 687 ms | 225 ms, 192 ms | 205 / 410 ms | 6.82 | 72 | 2.7 GB | 70 | 0 | clean |
| 100k | 103.6k | 2.9 s / 11.8 s | 9.7 s (committer) | 205 / 819 ms | 7.43 | 72 | 3.4 GB | 72 | 0 | behind |

At 90k the committer ran 94.2k/s, at 100k it ran 84.8k/s.

The library tests pass, `log_serve` passes, and the e2e passes both with the bucket (60 s at 400 events/s) and with the kill -9 restart.

Rejects were 0 at every step that kept up, and `e2e_check` found no missing, reordered or duplicated events. The "extra" count of 0-5 is events in flight at the window's edges. Steps that fell behind show missing and extra events, because the relay's stream is 10-30 s behind the checker's window. They also show some "out of order" events (11-993, only in those steps). That is most likely the checker pairing `#identity` and `#account` events by occurrence count from different start points. Those events carry no rev, so the count is all it has to pair them by. It's worth confirming with a longer settle.

## The per-node ceiling

One node with 8 pinned cores sustains ~94k events/s, every event checked, with the time to firehose at p50 ~190 ms and p99 under 0.7 s, 0 rejects and a clean e2e. That's 2.8x the 33k/s target. Its peak intake is ~104k/s, but there the committer falls behind and the CPU is at 7.4 of 8 cores.

- **CPU** costs ~70-72 µs per event, linear in the rate:

  | Part | µs per event |
  |---|---|
  | Signature verification (secp256k1 and sha256), being worked on by the verify-perf workstream | ~33 |
  | Upstream sockets, sequencer, committer and SlateDB, on the main runtime | ~19 |
  | zstd on the segments | ~12 |
  | Dispatch, parse, apply and the lane | ~7 |

  8 cores give ~110k/s with no headroom.
- **The committer** is the first serial stage to give out, at ~90-95k/s. Spreading it over shards, without the regressions above, is the next step for the ceiling.
- **Time to firehose** is linger plus PUT. At 33k it's ~50 ms p50 and ~100 ms p99 on disk MinIO. With the bucket in RAM it was 32 / 50 ms. Above ~50k/s a segment seals on size, not linger, and its PUT takes 100-400 ms on this MinIO, so the p50 climbs to ~150-200 ms at 75-90k. S3's PUT latency for a few MB is lower and flatter. That needs measuring on S3.
- **The target**, 33k/s per node, takes ~2.5 of the 8 cores, with p99 ~100 ms and 0 rejects.

## Fan-out at 33k

N full-firehose consumers at the target rate, on the iteration 1 build with disk MinIO. `e2e_check` is one of them, and the rest are `fakepds consume` (each capped at 400 MB, or 300 MB for N=32). All of it runs over loopback, which stands in for a NIC here.

| Consumers | Firehose out | Relay CPU | Firehose threads | TTF p50 / p99 | Each consumer got | Disconnects |
|---|---|---|---|---|---|---|
| 1 | 203 MB/s | 2.52 | 0.03 | 48 / 89 ms | 34.8k/s | 0 |
| 4 | 786 MB/s | 2.63 | 0.15 | 53 / 108 ms | 34.8k/s | 0 |
| 8 | 1.59 GB/s | 2.79 | 0.25 | 56 / 121 ms | 35.0k/s | 0 |
| 16 | 3.16 GB/s | 3.03 | 0.44 | 58 / 146 ms | 34.8k/s | 0 |
| 32 | 6.29 GB/s | 3.45 | 0.81 | 57 / 115 ms | 34.6k/s | 0 |

Each consumer costs ~0.025 cores and ~195 MB/s (1.6 Gb/s). The CPU runs out late. There are 4 serve threads (`serve_threads`), so firehose CPU tops out at 4 cores, or ~150 consumers. That's also what's left of the 8 cores after ingest at 33k. The NIC runs out first:

| NIC | Consumers |
|---|---|
| 10 GbE | ~6 |
| 25 GbE | ~15 |
| 100 GbE | ~60 |

So a node serving many consumers wants a fan-out tier (replicas or edges), not more cores.

## Not done, next

- **The committer.** See iteration 3. Per-shard committers, each with its own ordered queue, would split the work. Each shard's tickets are already independent, and the lane's ack is per event.
- **Per-lane batching into the log.** Each event is still its own `log.submit` with its own oneshot, and its own boxed future in the committer. In the profile, these channel and future costs are under 1%, so they were left alone. Batching per lane would mean a `DidOwner::submit_batch` with a looping default, so the cluster's seam keeps its per-event contract.
- **Seeding the identity cache from state.** A restarted node resolves every DID again at `--did-lookups-per-sec` (50 by default). The state record has the key and the PDS's `HostKey`, and could seed `IdentityCache` on a miss. This bench starts cold every time, so it doesn't show the cost.
- **The clock.** `clock_gettime` is 3% of samples, mostly per-event `Instant::now` for stage timers and acks. One timestamp per event, passed along, would cover most of them.
- **SigV4 payload hashing.** Each PUT hashes its body (1.5%). object_store's unsigned payload would skip that. It's a vlpds `Store` setting.
- **Compression.** zstd level 1 is ~15% of the node's CPU. Lower levels trade bucket bytes for CPU. That's a cost decision, so it wasn't changed here.
