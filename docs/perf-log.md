# vlRelay: node performance, the bench log

Internal: the public summary is [Performance](perf.md) (`/docs/perf`). This is the full log, iteration by iteration.

Every iteration here ran on the old single node and the lease cluster (node logs, a 25 ms segment linger, DID shards, the full-mesh merge). Both are deleted, along with `scripts/cluster-perf.sh` and `just e2e-cluster`. The quorum log's own numbers are in `docs/quorum.md`'s implementation notes.

How many events a second one relay node takes, and how fast they reach the firehose. The target in `docs/design.html` is 100k events/s on 3 nodes of 8 cores and 32 GB, so about 33k/s per node. Iteration 6 measures a 3-node cluster against the whole target.

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

## Iteration 1: PUT parallelism, jemalloc, lane acks (79f01b0f)

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

## Iteration 2: metrics and hashing (6c74eb3e)

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

## Iteration 3: 32 PUTs in flight, TTF tracker per batch (9d82e46b)

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

## Iteration 4: a committer per DID shard (8de2eb86)

This iteration is rebased on the policy and cluster wiring (236cac3f).

Changes:

- `LocalOwner` runs one committer per DID shard, picked by shard id modulo the count (`--did-shards`, 4 here). A DID's events all land in one shard, so they reach one committer in the order they were applied. Each committer still waits for its tickets in log order and commits before it answers the lane. The ack tracker is unchanged, so a host cursor still only passes contiguous events that are durable and committed. A checkpoint now reads the oldest in-progress batch across all the committers (`committing_since_us()`).
- Fewer clock reads per event:
  - the ack tracker takes the dispatch timestamp instead of reading the clock again, and no longer clones the host name per event;
  - the identity stage's end time doubles as the verify stage's start;
  - the committer and the sequencer read the clock once per batch.
- On a cache miss, the host stage seeds the DID document cache from the DID's state record. That needs this node to hold the record, and the key there to have been resolved within the cache's TTL. A key that then fails a signature is refreshed as before. The seeded entry has no handle, so the admin views show none for it until the next `#identity` or refresh. In the restart e2e, after the kill -9, all but ~5 of ~2,370 identity stages finished under 0.5 ms, so the restarted node wasn't going back to PLC for every DID.

The bench needed a change too. The policy wiring's account gate allows 6,000 new accounts a minute across the cluster, and every one of the fleet's 100k DIDs is a new account to a fresh relay. So the first post-rebase runs deferred almost everything. `perf.sh` now starts the relay with `--admin-token perf` and lifts the new-account budgets, caps and the PLC lookup budget through `PUT /admin/api/policy/full` before the warmup. Since then the gate only rate-limits newly created repos (docs/policy.md), and the fleet's accounts are established, so `perf.sh` lifts only the PLC budget.

100k offered, MinIO with O_DIRECT off, against iteration 3:

| Build | Accepted/s | Committed/s | Durable p50 | Ack backlog | TTF p50 / p99 | CPU (of 8) |
|---|---|---|---|---|---|---|
| Iteration 3, one committer | 103.6k | 84.8k | 9.7 s | 1.0M | 2.9 s / 11.8 s | 7.43 |
| Iteration 4, 4 committers | 101.5k | 101.7k | 225 ms | 19k | 3.3 s / 11.3 s | 7.31 |

The committer is no longer a limit. Commits keep pace with intake, the time to durable is back to the PUT plus linger, and host cursors keep moving. The time to firehose at 100k is still seconds, though. At 7.3 of 8 cores the node runs at its CPU ceiling, and it carries the backlog from warmup, while the fleet's bursts run ~5% over the offered rate. Only one 100k step fit before the 06:10 PT batch window, so the 90k and 110k steps weren't rerun on this build.

Tests on this build:

| Test | Result |
|---|---|
| Lib tests | 107 passed |
| `just e2e`, 60 s at 400/s with the bucket | PASS |
| `just e2e`, with the restart | PASS |
| `just e2e-policy` | PASS |
| `just e2e-cluster --duration 60` | PASS |

## Iteration 5: CPU per event (a6dfcda5)

The 75k profile of the iteration 4 build, by thread pool: ingest 58%, main 25%, commit pool 16%, firehose 1%. Past the signature (~33% of all samples) and zstd (15%), these stood out:

| Where | Share of samples | Why |
|---|---|---|
| Page faults, `madvise` and `rallocx` on 8 MiB buffers | ~8% (4.5 + 1.3 + 2.2) | Segments are ~8 MiB, jemalloc's oversize threshold. Allocations that big get an arena that hands pages back to the OS on free, so each segment buffer and its compressed copy were faulted in afresh. |
| `policy::signals::TopK::add` | 2.4% | The per-account spam table holds 8,192 DIDs and the fleet has 100k, so nearly every add evicted. Each eviction scanned 512 entries for the minimum. |
| aws-lc sha256 (SigV4 payload hash) | 1.6-2.2% | Each PUT hashed its whole body on a main-runtime worker, holding it for milliseconds per 8 MiB segment. |

Changes, one commit each:

- **`--log-compression`** (f759b7c4). A relay-only zstd level for segments, default -1. vlpds's own default stays 1, and its readers take either, since the level isn't in the format. The fleet's frames are a poor stand-in here. Their padding is random English-ish words, which compress 2.3x and slowly. Production frames are mostly CIDs and signatures, and compress 1.6x at level 1. So both were measured with the zstd CLI on one benchbox core, over 8 MiB of frames each (production: 21,943 frames read off `bsky.network` with `verify_bench capture`):

  | Level | Production ratio | Production MB/s | µs/event (5.4 KB) | Fleet ratio | Fleet MB/s |
  |---|---|---|---|---|---|
  | 1 (vlpds) | 1.569 | 575-645 | ~9 | 2.256 | 465-488 |
  | -1 (relay) | 1.559 | 1,070-1,150 | ~5 | 2.141 | 532-558 |
  | -3 | 1.542 | 1,450-1,540 | ~3.6 | 2.014 | 585-613 |
  | -8 | 1.421 | 1,820 | ~3 | 1.718 | 689 |
  | 0 (none) | 1.0 | | 0 | 1.0 | |

  On production frames -1 halves zstd's CPU for 0.6% more log bytes. -3 saves another ~1.4 µs for 1.8%. Storing segments raw would save all ~9 µs (~13% of the node) for 57% more bucket bytes and PUT bandwidth. On the fleet, -1 saves ~1 µs and stores 3.5% more bytes per event in all (2,620 to 2,710 B, log and state).
- **jemalloc without the oversize arena** (2d714082). `oversize_threshold:0` in the binary's `malloc_conf`, so 8 MiB buffers come from the normal arenas and reuse dirty pages within the decay time. Page faults, `madvise` and `rallocx` went from ~8% of samples to ~1.4%. Peak RSS went up ~0.5 GB at 75k (2.6 to 3.1-3.3 GB), still far under the 12 GB cap.
- **Spam signals evict from a batch of candidates** (0569eb6c). One scan finds the lightest 1/16 of a table shard, and each eviction takes the next of them whose estimate hasn't grown since. The newcomer still inherits the victim's count as error, so a lower bound never overstates and a trip is never a false positive. Only which light key goes first changes.
- **SigV4 `UNSIGNED-PAYLOAD`** (a6dfcda5). `--s3-unsigned-payload`, on by default for an `https://` endpoint, where TLS already protects the body. vlpds gains `Store::s3_with`, and `Store::s3` is unchanged. The bench's MinIO is `http://`, so the steps below pass `--s3-unsigned-payload true` to measure it.

`scripts/perf.sh` takes `PERF_HOME` (86d0bc23) for the MinIO data and the fleet dir, so a second bench on the box doesn't share them.

### A/B on one box

benchbox was busier than in iteration 4. Other relays were running unpinned, and the relay cores' SMT siblings (24-31) were 12-16% busy. The same iteration 4 build that measured 71.8 µs/event at 75k early in the session measured 77 µs here. So the comparison is interleaved: base (06a5737d), this build, base, this build, at the same rate, minutes apart.

| Step | Build | Accepted/s | µs/event | Ingest | Main | Commit pool | TTF p50 / p99 | e2e |
|---|---|---|---|---|---|---|---|---|
| 75k | base | 78.1k | 77.2 | 44.9 | 19.8 | 11.5 | 200 / 436 ms | clean |
| 75k | this | 79.7k | 70.7 | 43.0 | 17.1 | 9.8 | 214 / 524 ms | clean |
| 75k | base | 77.6k | 76.9 | 44.7 | 19.8 | 11.5 | 212 / 447 ms | clean |
| 75k | this | 78.5k | 70.3 | 42.7 | 17.1 | 9.7 | 194 / 425 ms | clean |
| 75k | this, unsigned | 82.6k | 69.3 | 42.7 | 16.0 | 9.8 | 205 / 637 ms | clean |
| 90k | base | 94.9k | 76.2 | 44.9 | 19.5 | 11.0 | 398 ms / 6.2 s | clean, behind at times |
| 90k | this | 95.6k | 70.3 | 43.3 | 17.0 | 9.2 | 247 / 792 ms | clean |
| 100k | base | 96.5k | 75.8 | 44.9 | 19.3 | 10.9 | 6.9 s / 23 s | behind |
| 100k | this | 101.1k | 69.9 | 42.7 | 17.2 | 9.2 | 5.3 s / 21 s | behind |
| 100k | this, unsigned | 106.1k | 67.7 | 42.4 | 15.5 | 9.1 | 2.0 s / 10.8 s | 0 missing, behind |

The per-pool columns are µs of that pool's CPU per accepted event. Per change, from these and from single steps earlier in the session:

| Change | µs/event saved | Where |
|---|---|---|
| zstd -1, fleet data | ~0.7-1 | commit pool |
| zstd -1, production frames (CLI) | ~4.4 | commit pool |
| jemalloc oversize arena off | ~4 | main 2.2-2.7, commit pool ~1 |
| Spam table eviction | ~1.1-1.9 | ingest |
| SigV4 unsigned payload | ~1.2 | main |
| All four, fleet data | ~7.5 (77.0 to 69.5, -10%) | |

Latency didn't move: at 75k both builds held ~200 ms p50 and ~430-520 ms p99, which is linger plus PUT. MinIO's PUT p50 was 205 ms at 75k and 409 ms from 90k up in this session, on a disk shared with the other benches.

Tried and not kept: `--lanes 16` against 64, and `--ingest-threads 6` against 8. Both were within run-to-run noise (65.8 against 66.7 µs/event, 65.2 against 65.4), so the defaults stay.

The ladder for this build with `--s3-unsigned-payload true`, as on an https bucket:

| Offered | Accepted/s | TTF p50 / p99 | Durable p50 | PUT p50 | Relay CPU | µs/event | RSS | Rejects | e2e |
|---|---|---|---|---|---|---|---|---|---|
| 33k | 34.2k | 63 / 141 ms | 88 ms | 51 ms | 2.45 | 71.6 | 1.8 GB | 0 | clean |
| 75k | 82.6k | 205 / 637 ms | 225 ms | 205 ms | 5.73 | 69.3 | 3.5 GB | 0 | clean |
| 90k | 95.2k | 488 ms / 1.6 s | 576 ms | 409 ms | 6.23 | 65.4 | 4.1 GB | 0 | clean |
| 100k | 106.1k | 2.0 s / 10.8 s | 360 ms | 409 ms | 7.18 | 67.7 | 4.0 GB | 0 | 0 missing, behind |
| 110k | 107.1k | 11.5 s / 26 s | 576 ms | 409 ms | 7.12 | 66.5 | 4.6 GB | 0 | behind |

At 90k the node now keeps up, where the base build fell seconds behind at times. It takes ~106-107k/s at ~7.1-7.2 of its 8 cores, against ~101k for iteration 4. The time to firehose at 90k is mostly MinIO's 409 ms PUTs in this session.

Some steps on both builds saw the checker's relay socket closed with `ConsumerTooSlow` (1-4 times a step). The checker reconnects with its cursor and saw nothing missing or duplicated. It runs on the cores the fleet and MinIO share, so it's most likely the checker falling behind under load.

Tests on this build:

| Test | Result |
|---|---|
| Lib tests (the verify mutation tests included) | 108 passed |
| `just e2e --duration 60 --rate 400 --accounts 60 --bucket` | PASS, p50 / p99 18 / 31 ms |
| `just e2e --duration 60 --rate 50 --restart-at 20` | PASS |
| `just e2e-cluster --duration 60` | PASS |

The e2e runs use an `http://` MinIO, so they cover the signed path. The unsigned path ran in the 75k-110k steps above against MinIO, with every event accounted for.

## Iteration 6: 3-node cluster (7d7f33c1)

The target is 100k events/s on 3 nodes of 8 cores and 32 GB. benchbox can't host three 8-core nodes and a fleet fast enough to load them, so the cluster is scaled down: 3 core nodes, each with 6 hardware threads (3 physical cores and their SMT siblings) and a 7 GB cap. The same cores also ran one node without `--cluster`, so the cluster's cost per event compares with a single node's on identical hardware.

### The bench

`scripts/cluster-perf.sh` (87a37c8e) does what `perf.sh` does, for three nodes:

```
scripts/cluster-perf.sh up                       # MinIO (O_DIRECT off), peer certs from `vlpds admin tls`
scripts/cluster-perf.sh step NAME RATE [SECS]    # 3 fresh cores on a new prefix, then the fleet; $PERF_HOME/out/NAME.json
SINGLE=1 scripts/cluster-perf.sh step ...        # node 1 alone, without --cluster, on the same cores
scripts/cluster-perf.sh down
```

| | CPUs | Memory cap |
|---|---|---|
| n1, n2, n3 | 5-7,21-23 / 8-10,24-26 / 11-13,27-29 | 7 GB each (7.5 GB at 120k+) |
| Fleet (4 fakepds x 10 hosts x 2,500 accounts) | 0-4,14-20,30-31, shared | 3 GB each (3.5 GB at 120k+) |
| MinIO (one, on benchbox's disk) | shared | 3 GB |
| `e2e_check` on n1 | shared | 3 GB (not run at 120k+) |

- The cores run with `--cluster --did-shards 12 --host-shards 64`, peer mTLS on loopback with certificates from `vlpds admin tls`, `--s3-unsigned-payload true`, and the PLC budget lifted as in `perf.sh`.
- The fleet starts only once the three cores have joined and spread the host shards (`SETTLE`, 15 s). The fleet's 40 hosts landed on the three cores by hash: 15-17, 15 and 8-9.
- Every core gets the same 40 `--host` flags. Each host connects on the core that owns its host shard.
- Per node, the summary has the same numbers as `perf.sh`: accepted/s, time to firehose, CPU per pool and stage, RSS and rejects. Cluster-wide, it adds:
  - forwards: events to the local stage and to peers, bytes, and the batch round trip (new metrics, fd7c8546);
  - each log's lag: our oldest append not yet durable, and each followed peer log's watermark behind our clock, sampled every 100 ms;
  - bytes on the peer sockets, from `ss`;
  - MinIO's PUT/s;
  - `e2e_check` against n1;
  - with `SAME=1`, every node's stream over the same window, compared seq by seq.

Caveats:

- It's one box. The three nodes share the memory bus, the L3 and one MinIO on one disk. Loopback stands in for the NICs, so the forward hop and the log streams cost CPU (TLS, copies) but no network latency.
- The nodes are 3 cores + SMT, not 8 cores. Below, "µs/event" is the CPU time of all three nodes per accepted event, in hardware-thread seconds. On the same 6 threads, a single node costs ~90 µs/event, where the same build on 8 whole cores (iteration 5) costs ~70.
- MinIO's PUT latency sets the time to firehose from ~60k/s up, as in the single-node ladder. PUT p50 was 205 ms at 60k and 410-819 ms at 90k and above. The benchbox disk was shared with other benches throughout.
- The fleet runs on 14 threads shared with MinIO and the checker. At 150k offered it couldn't keep up (below).

### Bugs the bench found

| Commit | What |
|---|---|
| d00c96b3 | **Forward batches were too big for the peer listener.** A forward batch takes up to 512 events, and 512 production-sized frames are ~3 MB. axum's default body limit is 2 MB. So under load, owners answered 413, the forward retried for its 20 s budget and gave up, and the host replayed into the same loop. At 33k/s, most forwards failed and accounts desynchronized. Batches are now capped at 4 MiB, and the forward route takes 10 MiB (a full batch plus the largest upstream frame). |
| 7d7f33c1 | **The merge queue spilled at 90k/s.** vlpds's merger holds up to 256 MiB while it waits for the slowest log's watermark, and past that it reads a log back from the bucket. At ~100k/s, 256 MiB is half a second, less than a linger plus one slow PUT. Each node spilled every few seconds (968 segments in a minute), and the read-back couldn't keep up: the stream fell 25 s behind, 6.8k/s out against 98k/s in. vlRelay now sets 1 GiB (`serve::MERGE_QUEUE_BYTES`). The same step then kept up, with the time to firehose at p50 373 ms and p99 1.0 s. |

The harness needed fixes too:

- The fd limits. A systemd user unit's soft limit is 1024. A core passed it at startup, and so did fakepds process 0, whose PLC took all three cores' lookups. Lookups then timed out, after 5 s each, and those held whole forward batches.
- The startup handoff. Started together with the fleet, the first core to join took every host shard and handed most of them over seconds later. The next owner resumes from the acked cursor, and above ~60k/s that cursor was older than the fleet's replay ring, which holds seconds (a real PDS keeps days). The gap desynchronized those accounts for the whole step. That's why the fleet now starts after `SETTLE`.
- The fleet's memory. At 120k/s, 3 GB per fakepds process wasn't enough and one process was OOM-killed mid-step.

### The ladder

The build is 7d7f33c1. `e2e TTF` is upstream emit to emit on n1's stream, measured by `e2e_check`. `Relay TTF` is the nodes' own histogram, at its bucket bounds. CPU is out of 18 threads. The per-pool columns are µs of that pool's CPU per accepted event.

| Offered | Accepted/s | e2e TTF p50 / p99 | Relay TTF p50 / p99 | PUT p50 / p99 | CPU | µs/event | Main | Ingest | Commit pool | PUT/s | Peer MB/s | RSS max | Rejects | e2e |
|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|
| 33k | 34.2k | 181 / 484 ms | 141 / 360 ms | 102 / 205 ms | 5.57 | 163 | 90.6 | 57.6 | 12.0 | 141 | 521 | 1.7 GB | 0 | clean, all 3 streams identical |
| 60k | 63.7k | 362 / 699 ms | 360 / 576 ms | 205 / 410 ms | 9.93 | 156 | 80.8 | 60.1 | 13.8 | 113 | 932 | 2.2 GB | 0 | clean |
| 90k | 96.9k | 783 ms / 1.9 s | 576 ms / 1.5 s | 410 / 819 ms | 14.47 | 149 | 72.9 | 61.9 | 13.4 | 124 | 1,304 | 4.3 GB | 0 | clean |
| 90k (2nd run) | 98.0k | 1.4 / 3.0 s | 0.9-1.5 / 1.5 s | 819 / 819 ms | 14.96 | 153 | 76.4 | 61.6 | 13.4 | 117 | 1,310 | 4.5 GB | 0 | clean |
| 120k | 120.8k | not run | 0.9-1.5 / 1.5-2.4 s | 819 / 819 ms | 17.21 | 142 | 68.4 | 61.7 | 12.2 | 129 | 1,533 | 5.9 GB | 0 | out 104-115k/s, backlog growing |
| 150k | 32.6k | not run | | | 5.32 | | | | | | | | 900 `identity_unavailable` | collapsed in warmup |

The same 6 threads as n1, one node without `--cluster`:

| Offered | Accepted/s | e2e TTF p50 / p99 | PUT p50 | CPU (of 6) | µs/event | Main | Ingest | Commit pool | PUT/s | e2e |
|---|---|---|---|---|---|---|---|---|---|---|
| 25k | 26.6k | 69 / 135 ms | 51 ms | 2.49 | 93.5 | 24.4 | 56.0 | 12.0 | 48 | clean |
| 33k | 34.0k | 55 / 164 ms | 26 ms | 3.08 | 90.6 | 22.6 | 55.0 | 12.1 | 48 | clean |
| 50k | 54.0k | 104 / 408 ms | 102 ms | 4.82 | 89.2 | 20.7 | 54.8 | 12.8 | 49 | clean |

What the ladder shows:

- **Correctness.** Every step that kept up was clean. `e2e_check` found 0 missing, reordered or duplicated events across 2.4-6.5M events per step. At 33k, all three nodes' streams over the same window (847,161 events) had the same events at the same seqs, in the same order. No node logged a seq checkpoint disagreement.
- **Capacity.** The three nodes take ~98k/s cleanly. At 120k they take 120.8k/s on 17.2 of their 18 threads, which is the CPU ceiling. There, the merged stream trails intake (104-115k/s out) and the ack backlog grows.
- **150k didn't get past warmup, and that's a harness limit.** The fleet's PLC, on the shared fleet cores, stopped answering in time, so cold DID-document lookups timed out. Each timed-out lookup on a DID owner holds its forward batch for up to 5 s, so all three nodes stalled: the apply stage waited 40 s per second, and the nodes used 1.8 cores each. It does show a real coupling, though (next steps).
- **Latency.** At 33k the cluster's time to firehose is p50 181 ms and p99 484 ms. The single node on the same cores and the same MinIO session does p50 55 ms and p99 164 ms. A cluster node emits an event only once every log's watermark has passed it, so the slowest of three logs sets the pace. Each log also seals on linger at a third of the rate, which tripled the PUTs: 141/s against 48/s. From 90k up, MinIO's 410-819 ms PUTs dominate, as they did for a single node.
- **Balance.** DID shards split 4/4/4, so every node's log and committers carry a third of the events. Host shards split by hash, and the 40 hosts landed 15-17/15/8-9, so n3 verified about half what n1 did (ingest 1.3 against 2.2 cores at 90k).

### Forwarding and the merged stream

Two thirds of the events are forwarded (`remote_share` 0.66-0.68 at every step), as random DIDs over 3 owners predict.

| Per event | Forwarded | Local |
|---|---|---|
| Forward hop bytes | 5.0-5.8 KB: the frame plus ~90 B (DID, host, seq, meta) | 0 |
| Forward CPU (send, receive, HTTP/2 and TLS) | ~15 µs | ~0: the stage is called directly |
| Batch round trip p50 | the time to durable: 141 ms at 33k, 225 ms at 60k, 576-922 ms at 90k | the same |

The round trip is the owner's time to durable, so it's the same for local and remote batches. On loopback, the network hop doesn't show at these histogram bounds.

The merged stream is a cost on every event, forwarded or not. Each node streams its own log to both peers and merges all three logs:

| | Per event |
|---|---|
| Peer bytes | 12.7-15.2 KB: the log streams carry raw frames to 2 peers (~11 KB), plus the forwards (~3.8 KB) |
| CPU | ~20 µs: receiving peer logs, serving ours, the merge (TLS and copies included) |
| Peer traffic per node at 100k/s | ~440 MB/s (3.5 Gb/s) each way, before any consumer |

Per node, a log's lag behind the merge was:

| Rate | Own log's oldest pending append, p50 / p99 | Peer watermarks, p50 / p99 |
|---|---|---|
| 33k | 88 / 360 ms | 141 / 577 ms |
| 60k | 141-225 / 360 ms | 360 / 577 ms |
| 90k | 360-576 / 922 ms | 360-576 ms / 0.9-1.5 s |

That's PUT plus linger, plus ~100-300 ms for the stream hop and the peer's own lag.

### Cost per event: the cluster against one node

On the same 6 threads, the cluster costs 142-156 µs/event from 60k up (163 at 33k, where fixed costs weigh more). One node costs 89-94. That's ~1.65x, or +55-65 µs per event:

| Pool | One node | Cluster (60-120k) | Difference |
|---|---|---|---|
| Main runtime | 21-24 | 68-81 | +47-57 |
| Ingest (verify, dispatch) | 55-56 | 60-62 | +5 |
| Commit pool (zstd) | 12-13 | 12-14 | +1 |

**Profile at the knee** (`PERF_RECORD=1`, n1, 10 s at 97k/s cluster-wide, 8.5k samples). By pool, main is 49%, ingest 40%, the commit pool 9% and firehose 2%. A single node at 75k was main 25%, ingest 58%, commit pool 16%. Within main:

| Where | Share of main | µs per cluster event |
|---|---|---|
| Receiving peers' log streams (`vlpds::remote::follow_log`, websocket over TLS) | 14.4% | ~10.5 |
| Serving our log to peers (`follow::serve_stream`) | 8.3% | ~6 |
| HTTP/2 and TLS of the forward RPC | 6.9% | ~5 |
| Forward send (lanes, `encode_batch`) and receive (`decode_batch`) | 7.0% | ~5 |
| SlateDB (memtable, flush) | 7.1% | ~5 |
| Merge and dense renumbering | 4.7% | ~3.5 |
| DID-owner stage (`Stage::apply_did`) | 4.1% | ~3 |
| Upstream sockets, committer, sequencer | 11.3% | single-node work |
| SipHash and `RandomState` (std `HashMap`s on the path) | ~5.8% | ~4 |
| vdso `clock_gettime` | 3.5% | ~2.5 |

Across those rows, `memcpy` is 23% of main: 9.2 points in receiving log streams, 5.0 in serving them, and 2.1 in the sequencer. TLS (AES-GCM) is 13.6%: 4.7 points receiving, 4.0 serving and 4.0 forwarding. Of the cluster-specific costs that were asked about:

- **Forward serialization.** Sending and receiving (the lanes, `encode_batch`, `decode_batch`: copies and UTF-8 checks) is ~8 µs per forwarded event. The RPC transport around them (HTTP/2, TLS) is another ~7.5.
- **Peer TLS.** ~10 µs/event, about 7% of a node.
- **The follow and merge path.** ~20 µs/event, the largest cluster cost. It's mostly copies and TLS of raw frames, twice per event (one log to two peers).
- **Dense-seq renumbering.** Under 1 µs. `seq::skip` and `event::skip` together are under 1% of main.

Past the two bugs above, nothing cheap showed up. The cluster's costs are copies and crypto in proportion to the bytes on the peer links, and the way to cut them is to send fewer bytes ("Not done, next").

### Extrapolation to 3 x 8 cores

Measured: 142-156 µs per event (hardware-thread seconds), at the ceiling ~120k/s on 18 threads.

| Reading of "8 cores" | CPU per event | 100k/s needs | Of 3 x 8 | CPU ceiling |
|---|---|---|---|---|
| 8 vCPUs (4 cores + SMT, as on cloud VMs; what was measured, scaled) | ~150 µs per thread | 15 threads | 62% | ~160k/s |
| 8 physical cores (as in iterations 0-5; single node 70 µs there vs 90 here) | ~117 µs per core | 11.7 cores | 49% | ~205k/s |

Next to the single-node ladder:

| | One node, 8 cores (iteration 5) | 3-node cluster, 3 x 6 threads (here) |
|---|---|---|
| Clean, every event checked | 90-95k/s | ~98k/s |
| At the CPU ceiling | ~106k/s on 7.2 of 8 cores | ~121k/s on 17.2 of 18 threads |
| CPU per event | 65-70 µs | 142-156 µs (90 µs for one node on these threads) |
| TTF at 33k, p50 / p99 | 63 / 141 ms | 181 / 484 ms |
| Peak RSS | 4.6 GB at 110k | 5.9 GB per node at 120k |

**Verdict: 100k events/s on 3 nodes of 8 cores and 32 GB holds, with headroom.** On CPU it takes 50-60% of the cluster: 1.6x headroom if "8 cores" means 8 vCPUs, ~2x if it means 8 physical cores. Memory is a non-issue: 4-6 GB per node at 100-120k against 32. Three things qualify that:

- **The time to firehose at 100k is MinIO-bound here.** It was p50 0.8-1.4 s and p99 1.9-3.0 s, behind 410-819 ms PUTs. The merge waits for the slowest of three logs, so a cluster's tail tracks the worst PUT among them. S3 needs measuring.
- **The peer links carry ~3.5 Gb/s each way per node at 100k,** before consumers. Each full-firehose consumer is another ~4.3 Gb/s at 100k/s. The cores want 25 GbE, or consumers should be on edges and replicas.
- **Host placement is by hash, and ingest load follows hosts.** Here one node verified half what another did. With a few big PDSes carrying most of the network, one core could carry far more than a third of the verify load.

## The per-node ceiling

One node with 8 pinned cores sustains ~90-95k events/s, every event checked, with 0 rejects and a clean e2e. That's 2.8x the 33k/s target. It takes ~106k/s at its CPU ceiling (7.2 of 8 cores), where latency grows with any backlog. So the ceiling is the CPU.

- **CPU** costs ~65-70 µs per event with the iteration 5 build, against ~72 before (both on a quiet box), linear in the rate. From the 75k profile:

  | Part | µs per event |
  |---|---|
  | secp256k1, the signature itself (the floor) | ~24 |
  | The rest of verify: block hashes, MST inversion, commit decode | ~5 |
  | Dispatch, parse, apply, the lane, spam signals, the clock | ~13 |
  | Main runtime: upstream sockets (~2), sequencer (~2.5), committers and SlateDB (~5), scheduling and syscalls | ~15-16 |
  | zstd -1 on the segments (fleet data; ~5 on production frames) | ~9 |

  8 cores give ~115k/s with no headroom.
- **The committer** gave out at ~90-95k/s while it was one task. A committer per shard (iteration 4) took it off the list.
- **Time to firehose** is linger plus PUT. At 33k it's ~50-65 ms p50 and ~100-140 ms p99 on disk MinIO. With the bucket in RAM it was 32 / 50 ms. Above ~50k/s a segment seals on size, not linger, and its PUT takes 100-400 ms on this MinIO, so the p50 climbs to ~150-250 ms at 75-90k. S3's PUT latency for a few MB is lower and flatter. That needs measuring on S3.
- **The target**, 33k/s per node, takes ~2.4 of the 8 cores, with p99 ~100-140 ms and 0 rejects.

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

- **Compressed log streams between cores.** A core streams raw frames to each peer, ~11 KB per event in all, and receiving and serving them is ~20 µs/event of copies and TLS (iteration 6). Its sealed segments are already zstd'd, 1.56x on production frames. Streaming those would cut the peer bytes, and the copy and TLS cost with them, by about a third, for a decompress on each receiver. Worth measuring.
- **A DID owner's lookup holds a forward batch.** The owner's apply can resolve a DID document, and a lookup that times out (5 s) holds the whole batch it's in, along with one of its lane's 4 batch slots. When the fleet's PLC fell behind at 150k, every node stalled. The host owner has just resolved the key, so it could pass it along in `meta`. Or the owner could answer a slow DID's events apart from the rest of the batch.
- **Cold DID lookups cost ~1.7x on a cluster.** 100k cold DIDs took 177k PLC fetches across three cores, against 104k on one node, because the host owner and the DID owner each resolve. `--plc-export` seeding covers this in production.
- **Host shards placed by load.** Placement is by hash, and verify cost follows hosts. With 40 equal hosts the cores got 15-17, 15 and 8-9.
- **std `HashMap`s on the cluster path** (the forward lanes' per-DID sets, `Stage::apply`'s grouping). SipHash and `RandomState` were ~6% of the main runtime at 97k/s, ~4 µs/event. `types::FastMap` is there for it.
- **CPU.** Still the ceiling. The signature is about a third of it and is the floor. What's left above it is spread thin: outside secp256k1 and zstd, nothing is over ~2% of samples.
- **Compression.** zstd -1 is still ~14% of the node's CPU on the fleet, ~7% on production frames. -3 would save ~1.4 µs more on production for 1.8% more bytes, and raw segments all of it for 57% more. That's a cost decision for the operator, so the default stops at -1.
- **The upstream read buffer.** tungstenite reads 16 KB at a time (`read_buffer_bytes`), about three frames per `recv`, and `recv` is ~2% of samples. 64 KB would cut the syscalls but costs 48 KB more per host connection, ~150 MB across 3,000 hosts.
- **The clock.** About 10 clock reads per event remain (vdso `clock_gettime`, ~2% of samples): the stage timers, the identity and verify stage boundaries, `now_secs` for the state step and the future-rev check, and the policy's `now_ms`. Sharing them across stages would save ~0.5 µs.
- **Per-lane batching into the log.** Each event is still its own `log.submit` with its own oneshot, and its own boxed future in the committer. In the profile, these channel and future costs are under 1%, so they were left alone. Batching per lane would mean a `DidOwner::submit_batch` with a looping default, so the cluster's seam keeps its per-event contract.
- **Parse once.** The dispatcher's `event::route` and the lane's strict parse both walk the frame, but `route` is ~0.3% of samples, so handing the parse downstream isn't worth the churn.
- **MST hashing.** `height_for_key` hashes each key of each loaded node, ~0.3% of samples on the fleet's 50-100 record repos. Production repos are deeper, so a per-thread key-height cache may be worth measuring on production frames (`verify_bench`). Caching a DID's previous proof nodes would help little: every node on a commit's path is new, and only unchanged neighbours can repeat.
