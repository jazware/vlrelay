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

- **CPU.** Still the ceiling. The signature is about a third of it and is the floor. What's left above it is spread thin: outside secp256k1 and zstd, nothing is over ~2% of samples.
- **Compression.** zstd -1 is still ~14% of the node's CPU on the fleet, ~7% on production frames. -3 would save ~1.4 µs more on production for 1.8% more bytes, and raw segments all of it for 57% more. That's a cost decision for the operator, so the default stops at -1.
- **The upstream read buffer.** tungstenite reads 16 KB at a time (`read_buffer_bytes`), about three frames per `recv`, and `recv` is ~2% of samples. 64 KB would cut the syscalls but costs 48 KB more per host connection, ~150 MB across 3,000 hosts.
- **The clock.** About 10 clock reads per event remain (vdso `clock_gettime`, ~2% of samples): the stage timers, the identity and verify stage boundaries, `now_secs` for the state step and the future-rev check, and the policy's `now_ms`. Sharing them across stages would save ~0.5 µs.
- **Per-lane batching into the log.** Each event is still its own `log.submit` with its own oneshot, and its own boxed future in the committer. In the profile, these channel and future costs are under 1%, so they were left alone. Batching per lane would mean a `DidOwner::submit_batch` with a looping default, so the cluster's seam keeps its per-event contract.
- **Parse once.** The dispatcher's `event::route` and the lane's strict parse both walk the frame, but `route` is ~0.3% of samples, so handing the parse downstream isn't worth the churn.
- **MST hashing.** `height_for_key` hashes each key of each loaded node, ~0.3% of samples on the fleet's 50-100 record repos. Production repos are deeper, so a per-thread key-height cache may be worth measuring on production frames (`verify_bench`). Caching a DID's previous proof nodes would help little: every node on a commit's path is new, and only unchanged neighbours can repeat.
