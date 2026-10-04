# vlRelay chaos

We break the cluster on purpose and check what survives. `tests/chaos/chaos.sh` runs the cluster e2e's three cores, edge and replica on MinIO under steady load, injects one fault schedule, then checks the same invariants after every scenario.

```
just chaos list
just chaos kill9                      # or: tests/chaos/chaos.sh kill9 [--duration 100] [--rate 50] [--fake-rate 200]
VLRELAY_BIN=/path/to/old/vlrelay just chaos zombie   # the same scenario on another build
CHAOS_BASE=4150 just chaos minio-errors              # another port block, beside a running one
CHAOS_PROFILE=dev-release just chaos soak --duration 3000   # optimized relay, as on benchbox
```

## The setup

- **Upstreams.** devnet (`DEV_PDS=3` vlpds plus the reference PDS, 50 writes/s with handle changes and deactivations) and a fakepds fleet of 3 hosts (200 events/s, 100 accounts each, real signed sync 1.1 commits). fakepds serves the one PLC the relays use and hands every DID it doesn't own to devnet's PLC (`--plc-fallback`). Together that's about 370 events/s.
- **Relays.** 3 cores, an edge and a replica on one MinIO prefix, TTL 3 s, 8 DID shards, 15 host shards. A supervisor loop restarts any node 1 s after it exits, as systemd's `Restart=always` would. A scenario can hold a node down.
- **Fault proxy** (`tests/chaos/proxy.py`, stdlib asyncio, toxiproxy-style). One route per node to MinIO (`--s3-endpoint` points at it), and one in front of each core's peer port (`--advertise-url` points at it). Each route takes `latency [lo,hi]` ms, `reset p`, `timeout p`, `refuse` and `blackhole`, set over HTTP during the run. MinIO itself is paused with `docker compose pause`.
- **Checkers.** One `e2e_check` per stream: each core with failover to the other two, the edge and the replica.
- **Ports.** Each run takes a block from `CHAOS_BASE` (3700 by default; 33xx, 34xx and 35xx belong to other workstreams' networks). Its docker project is `vlrelay-chaos-$CHAOS_BASE` and its state lives in `dev/state-chaos-$CHAOS_BASE`, so two runs can go side by side. `dev/up.sh` and `down.sh` gained `DEV_STATE` and `DEV_COMPOSE_EXTRA` for this.

### What's checked after every scenario (`tests/chaos/report.py`)

| Invariant | How |
|---|---|
| Every stream identical | Same events at the same relay seqs on all five, over the shared seq range. The checkers' #identity/#account occurrence counts depend on where each socket started, so they're dropped first. |
| 0 missing, 0 duplicates, per-DID order kept | Each checker. An event counts as missing if an upstream emitted it in the window and it never reached that stream. The upstreams here only emit valid events, so everything they send must arrive. |
| No seq regressions | Each checker, relay side. Upstream replays (the `replay` fault) are skipped as replays, not counted as new events. |
| No acked-but-lost events | At the end the harness reads every host's checkpointed cursor (`hostck/`) from the bucket. A missing event whose upstream seq is at or below its host's checkpoint was acked and lost. The replica reads only the bucket, so missing on the replica means missing from the bucket. |
| Recovery | For each fault: the worst upstream→relay latency of events reaching each stream until the next mark, and "recovered", the time from the mark to the last event slower than 1 s. Node exits are listed with what preceded them, as is what each SIGSTOPped node did once it woke. |
| Leaks (soak) | RSS and open fds per node, bucket object counts per prefix, and MinIO's memory, every 60 s. |

`e2e_check` gained three things for this: it skips upstream frames whose seq doesn't move forward (a replay), it follows an upstream's FutureCursor from cursor 0, and its JSON lists missing events with their upstream seqs.

## Scenarios

| Scenario | Faults (seconds into the load) |
|---|---|
| `baseline` | none |
| `kill9` | kill -9 the busiest core and hold it down 10 s, at 15; kill -9 a random core at 45 and the busiest at 70 (supervisor restarts) |
| `term` | the same with SIGTERM (planned handoffs) |
| `double-crash` | kill -9 two cores 2.5 s apart, then two more 1 s apart |
| `crash-loop` | kill -9 a random core every 6 s, 20 times (run with `--duration 150`): every takeover adds a span to the shards' histories unless the new owner trims them |
| `term-bucket` | SIGTERM the busiest core while its bucket path refuses (held down 10 s); later the same with 1-3 s of latency on its bucket path |
| `zombie` | SIGSTOP the busiest core for 4×TTL, then SIGCONT; again for 3×TTL |
| `gc-pause` | SIGSTOP for 0.5, 0.9 and 1.5×TTL |
| `minio-latency` | 100-500 ms on every node's bucket path for 30 s; 300-1500 ms on one core's for 20 s |
| `minio-errors` | 5% connection resets on every bucket path for 25 s; then 20% hung connections plus 2% resets for 25 s |
| `minio-pause` | MinIO paused 2 s, then 10 s |
| `node-bucket-hang` | one core's bucket path blackholed 15 s (a hung disk); later refused 10 s |
| `partition-peer` | one core's peer port blackholed 20 s (peers can't reach it, it reaches them and the bucket), then another refused 15 s |
| `isolate` | one core cut off from its peers and the bucket for 20 s |
| `upstream-faults` | fakepds `disconnect` (every 25 s, 4 s down), `stall` (6 s every 30 s), `replay` (60 frames every 20 s) |
| `upstream-restart` | fakepds `restart`: a host's sequence starts over at 1 every 35 s (FutureCursor). New fakepds fault. |
| `consumers` | 20 slow consumers (1 KB/s) and 100 workers reconnecting in a loop (half live, half from cursor 0), 40 s, across two cores, the edge and the replica |
| `clock-skew` | skipped, see below |
| `soak` | a random fault every `SOAK_EVERY` s (kill -9, SIGTERM, a double crash, a zombie, a 1 s pause, bucket latency, bucket resets, a peer partition), plus slow fakepds disconnects, stalls, replays and restarts, for `--duration` |

**Clock skew** is skipped. The clocks that matter are `vlpds::tid::now_micros` (relay seq allocation and the watermarks) and the lease wall clock. vlpds's `ClusterConfig::clock_offset_ms` shims only the lease's `expires_ms`, and peers judge leases on their own monotonic clocks, so it changes nothing observable. A real test needs a process-wide offset in `now_micros` (an env var read once), which is vlpds core code, so it's left for the lead. It would test the 250 ms edge/replica `guard` assumption.

## Results

Mac (M-series, 14 cores, load average 60-120 from other agents' builds the whole time, so latencies are pessimistic), dev build, 100 s of load per scenario, ~370 events/s, ~25-32k events per stream. "Before" is the tip this work started from (236cac3f); "after" is with the fixes below.

"Worst" is the worst upstream→relay latency on the core streams after each fault mark, in seconds, in schedule order. Missing is per stream (the streams were always identical, so every stream missed the same events). Acked-but-lost is summed over the five streams.

| Scenario | Before | After |
|---|---|---|
| baseline | pass, p50 32 ms | pass |
| kill9 | pass. Worst 5.2, 3.2, 3.2 s | pass. 5.0, 2.9, 4.7 s |
| term | pass. 0.8, 1.5, 1.2 s, each process gone 0.3-1.1 s after SIGTERM | pass. 1.2, 1.0, 0.7 s |
| double-crash | pass. 1.9, 5.8 s; 6.1 s | pass. 2.8, 6.9 s; 24.8 s once (both crashed cores' shards reopened while the third reshuffled) |
| zombie | invariants pass. **11.8 s stall for a 12 s stop**; 5.4 s for 9 s. Each zombie fail-stopped 0.3-0.7 s after SIGCONT, and wrote and acked nothing after waking | pass. 5.8-6.9 s on the streams not attached to the zombie for the 12 s stop. Same fail-stop on waking |
| gc-pause | pass. 0.5×TTL: 1.6 s, nothing moved. 0.9×TTL and 1.5×TTL: the node fail-stopped on waking, 6.6 and 9.8 s | pass. 1.8 s; 7.8 and 14 s |
| minio-latency | pass. 3.6 s on the cores under 100-500 ms (replica 24 s); 16.4 s when one core got 300-1,500 ms, until it lost its lease | pass. 3.7 s; 10.4 s |
| minio-errors | **fail: 289 missing, 1,380 acked-but-lost**, 27 reordered, 6 accounts desynchronized, 6 node exits (every core fail-stopped on a lost lease answer) | three runs: 29 missing and 0 acked-but-lost; 0 and 0; 166 and 170. Out-of-order 0-31. Open issue 2 is what's left |
| minio-pause | **fail: 7,672 missing, 38,360 acked-but-lost**. The 2 s pause stalled every stream ~25 s; the 10 s pause killed all three cores | pass, twice. 2 s: 2.3 s, 2.2 s after. 10 s: all three cores fail-stop (leases lapse, as they must) and the cluster is back in 6.8-32 s |
| node-bucket-hang | pass. 4.7 s (the hung core lost its lease in 3.9 s); refused: 6.2 s | pass. 4.9 s; 6.5 s |
| partition-peer | pass. Events for the partitioned core's DIDs waited out the partition: 20.7 s for 20 s, 15.9 s for 15 s (open issue 4) | pass. 21.9 s; 17.9 s |
| isolate | pass. 4.8 s (lease lapsed in 4.3 s, then fail-stop) | pass. 7.4 s, and 20 s after heal |
| upstream-faults | pass. 360 upstream replays skipped | pass |
| upstream-restart | **fail: 4,604 missing** (host 0's accounts desynchronized after each restart; open issue 1) | fail, same cause |
| consumers | **fail: 4,444 missing.** The storm (74 connects/s, half replaying from 0) starved lease renewal on a loaded machine: 3 fail-stops (`lapsed past takeover`), then losses through the duplicate bug. Slow consumers: none of 20 dropped in 40 s | fail: 59 missing, 10 node exits from the same starvation. Same slow consumers |
| soak | | see below |

The "acked-but-lost" counts after the fixes are events rejected as `desynchronized` (a rejected event is acked by design) after a `prev_data_mismatch` caused by reordering (open issue 2). In the before runs most came from the duplicate bug. A reader should treat open issue 2 as a loss bug.

Variance is high. The machine was shared with other agents' builds, and the soak, a batch and a side run went at once, so take the latencies as upper bounds. `minio-errors` got three after-fix runs for that reason: one clean, one with 29 missing, one with 166. One more run on the newest build (with d6c95714) wedged with all streams at 0/s from 40 s to the end of the window. The nodes recovered on their own about 90 s later, after 30 s shard opens and 30 s drains of dead logs under the error rate. Whether the faster fail-stop makes `minio-errors` worse needs more runs.

## Bugs found and fixed

Each is its own commit with a regression test.

| Bug | Found by | Fix | Commit |
|---|---|---|---|
| **Duplicate answered before the first copy was durable.** A DID owner answered a second copy of an event `Duplicate` as soon as the first copy was applied, before its segment landed. The host owner acks past a duplicate, so if the first copy's append then failed (bucket errors, a lapsed lease, the owner dying) the event was acked and lost, and the account's next commit failed `prev_data_mismatch` and desynchronized it: every later commit was rejected. This is the documented zombie-plus-crash gap, but it doesn't need a zombie: any forward retry or host replay during a slow append hits it. | `minio-errors`: 289 missing, 1,380 acked-but-lost across the streams, 6 accounts desynchronized | The stage registers each append per DID. A duplicate (from the dedupe set or the state's rev check) waits for the DID's newest in-flight append: durable and still leased gives `Duplicate`, anything else gives `Unavailable`, and the forwarder retries. Claim, apply, append and registration run under a per-DID stripe lock. `node/cluster.rs` | 3394641a |
| **Two crashes in one checkpoint interval lose dedupe entries.** The documented gap. | (unit test; the double-crash scenario didn't hit it in 100 s) | Each DID shard's inherited dedupe entries (those its own log marker doesn't cover) go to `dedupe/{shard}/{log_id}`: written at open before any event is routed, and on a checkpoint tick when they change. An open reads every log of the shard's history and deletes the earlier owners' objects. Steady state writes nothing. | 4a1e1f2b |
| **FutureCursor seq collisions.** The documented gap: a restarted host's new `#identity`/`#account`/`#sync` at a seq the dedupe set still held were dropped as duplicates for up to 15 minutes. | (unit test) | Dedupe entries carry their DID's key, and only the same DID at the same (host, seq) is a replay. | a60207b6 |
| **A forward to a hung owner held its lane for the whole RPC timeout.** A SIGSTOPped DID owner keeps its socket open, so each request out to it sat out the 10 s timeout, with its forwarder lane (and every DID hashed to it) blocked behind it, long after the shards moved. | `zombie`: 11.8 s stall for a 12 s stop with a 3 s TTL | The request races a 100 ms poll of the DIDs' owners and is retried against the new owner once one moved. `cluster/forward.rs` | 5f396689 |
| **A renewal whose answer was lost read as a lost lease.** A lease PUT the store applied had its response reset; object_store retried the same If-Match, got 412, and the node fail-stopped. With 5% resets every core died within a second. | `minio-errors`, soak | `recreate_vanished_lease` adopts the object when it's ours (our log id, a higher renewal count). In vlpds: `src/cluster.rs` plus a `landed` fault in its `Stalls` test store. | c906521b |
| **A core whose log failed lived on.** The cluster passed no `on_fatal` to its NodeLog. A 2 s bucket pause lapsed a lease long enough to kill the log for good, but a renewal sent before the pause landed after it, so the lease stayed valid and the node kept its shards with no way to append. Every event for them failed for 20 s, the firehose stalled ~25 s, and the host replays that followed desynchronized accounts. | `minio-pause`: 7,672 missing, 38k acked-but-lost entries across the streams | The log's `on_fatal` calls `lost_now`, the same path as a lost lease. `cluster.rs` | d6c95714 |

Also: `new_account_deferred` kept its own reason label across a forward instead of turning into `owner_rejected` (f3f1ce85). The harness itself: fakepds `restart` fault and PLC fallback, `e2e_check` upstream replays/restarts and missing-event list, `DEV_STATE`/`DEV_COMPOSE_EXTRA` (53faa5ca, 9a9e7a15, e9fafa56, 67e5e7d2).

## The robust-cluster pass

Open issues 3, 4, 5, 7, 8 (second half) and 10 below, fixed in `cluster.rs`, `cluster/forward.rs`, `cluster/follow.rs`, `node/cluster.rs`, `node/adapters.rs` and `seq.rs`, plus three small opt-in or default-preserving changes to vlpds's `cluster.rs` (docs/cluster.md, "Failure handling", has the design and why each is safe). Same Mac and harness, again under load from other agents' builds. "Before" is 68f6fcb4 run beside it (`VLRELAY_BIN`) where a scenario was rerun, else the table above.

| Issue | Fix | Before | After |
|---|---|---|---|
| 8: takeovers slow as histories grow | An open reads each earlier span only up to its end, without a LIST, once per log; then flushes the shard and reports it `checkpointed`, and the cluster trims histories to one span (vlpds `set_trim_spans(1)`) | `crash-loop`, two runs: histories grew to 11 spans; shard replay p50 187-246 ms, p90 0.4-2.3 s, max 0.9-15.1 s | two runs: every open replayed at most 1 span (115 of 131 opens; the rest none); replay p50 50 ms, p90 0.15-0.6 s, max 1.1-2.2 s. The per-crash stall is the takeover (TTL + skew, ~3.2 s median) either way |
| 10: a planned leave can fail under load | vlpds fences our own log at `durable_end` without the scan; a leave that can't finish (own fence failed, or our lease lapsed under it) asks a peer to fence our log | soak: exit 1 without fencing. `term-bucket`: 5.5 s, 7.5 s | `term`: 0.2-0.95 s (1.2, 1.0, 0.7 s before), all three exits 0, "fenced our log at its end" with no LIST. `term-bucket`: 4.6 s and 9.9 s, then 0.6 s and 10.3 s after each heal (23.5 and 23.1 s before). Both leaves ended with a peer fencing the log at the leaver's request |
| 5: a pause between ~1.8 s and the TTL kills an unreplaced node | vlpds `set_revalidate` (opt-in): a lapsed lease is CAS-renewed and kept if our log is unfenced (a few GETs at its end); our log holds meanwhile (`lapse_grace`) | `gc-pause`: 1.6 s; 0.9×TTL fail-stopped, 6.6 s; 1.5×TTL 9.8 s | 1.6 s; 0.9×TTL revalidated, no exit, 2.8 s (the pause itself); 1.5×TTL fenced by then, so fail-stop, 6.7 s |
| 4: a core its peers can't reach keeps its shards | Each core probes its own advertised peer address; failing for a TTL it hands its shards over and fail-stops, and its restart doesn't join (vlpds `ShardHost::may_join`) until the address answers | `partition-peer`: 20.7 s for a 20 s blackhole, 15.9 s for 15 s of refusals | 6.5 s and 4.0 s: the core steps down 4.3-5.6 s in, once per partition (its restart waits to join). A few events still wait out the partition on the cores and the edge, arriving within 0.1 s of the heal (worst 9-20 s there), while the replica had them within 1 s: they were in the bucket, so they sat on an mTLS follow path (likely the held restart's new log, which peers follow over the dark port). Not root-caused |
| 3: one slow bucket path stalls the whole firehose | Forwarder lanes keep one batch per owner in flight, so a slow owner delays only its own DIDs. A core whose log alone is slow (oldest pending append past max(TTL/3, 1 s) and 4x the median peer's watermark age, for a TTL) hands its shards over | `minio-latency` with 300-1,500 ms on one core: 16.4 s (7.2 s in the side run, where that core's lease lapsed sooner) | 5.7 s: the slow core stepped down 5.7 s in, and the other cores' streams never stalled past that. Forwarding alone (before the slow-log step-down) gave 7.9-13.7 s |
| 7: the replica lags under bucket latency | `follow_bucket`: a window of concurrent GETs (2 caught up, up to 32), 20 ms polls, a LIST only after 5 s without a segment | `minio-latency`, 100-500 ms on every path: replica 24 s, 27.6 s in the side run (28.5 s after heal) | cores 2.1 s, replica 7.0 s (5.5 s after heal), and no core stepped down |

Also from these runs: `node-bucket-hang` 5.0-6.3 s and 4.5-6.1 s (4.7, 6.2 s before); `double-crash` 2.2, 5.5 s; 4.2 s (1.9, 5.8 s; 6.1 s); `isolate` 5.0-6.8 s and 0.65 s after the heal (7.4 s and 20 s after); `just e2e-cluster --duration 60` passes, the SIGTERM'd process gone 128 ms after the signal. Every run passed its invariants: identical streams, 0 missing, 0 duplicates, 0 acked-but-lost.

What the runs found along the way, each fixed before the numbers above: the first step-down flapped (the restart joined, took shards and stepped down again every ~6 s), hence `may_join`; the first revalidation granted validity before its fence check, whose LIST of the whole log then timed out every time under bucket latency, so the node held its log for 13 s; the first slow-log check used a fixed bound and a core stepped down under latency every node had (24 s); and a leave on a broken bucket path spent its time retrying until the watchdog killed it, never reaching the peer fence.

Still open from these runs:

- One `crash-loop` run and one `gc-pause` run had a forward give up after 20 s with "the duplicated event isn't durable" (30 events of 3 DIDs, and 1). Found in round 2 (below): a stage cancelled mid-batch left the DID's in-flight entry unresolved. Fixed by `forward::Detached`.
- The worst per-crash stall in `crash-loop` has a 15-32 s tail in both builds: crashes every 6 s land while the previous victim is rejoining and shards are moving back.
- Cores and edges still catch a log up from the bucket with vlpds's one-GET-per-segment `catch_up` on every stream reconnect; the replica's window would fit there too (vlpds code).
- `partition-peer`: the few events held on the cores until the heal (above). A follower that can't reach a live log's stream never falls back to the bucket while the lease is live (vlpds `remote::follow_log`); that fallback would fix this and the symmetric-partition stall.
- A step-down is a fail-stop and a restart. Under an asymmetric link (we reach our own address, peers don't) the lease still rules.

## Round 2: robust-pipeline

The soak's death spiral and open issues 1, 2, 8 (backpressure) and 9, plus what the reruns turned up. Each is its own commit.

| Fix | What | Commit |
|---|---|---|
| **Per-socket epochs and fences** (issue 2) | Each host socket is an epoch. When a forward gives up, the forwarder trips the socket's `forward::Fence` before it answers: the DID's later events in the batch give up with it, and nothing else from that socket is sent (`ForwardError::Fenced`). The lanes drop fenced jobs before verifying them. The host is kicked once per socket (`Manager::kick_epoch`) and the new socket replays everything past the cursor, in order. The forwarder also looks an owner up once per DID per pass, so an owner appearing mid-pass can't send a DID's later event ahead of an unrouted earlier one. The ack tracker is per socket: a new socket settles what the old one left at or below its resume cursor and ignores the old socket's late acks. (Before this, a failed entry at or below a cursor handed over by another node pinned the cursor for good.) A host handed to another node is fenced here once its in-flight wait ends. With robust-cluster's per-owner batches, a lane also looks each DID's owner up once when it splits a batch (b2086055). | 340fa945, b2086055 |
| **In-flight caps** (issue 8, first half) | Every frame read takes an `upstream::flow` permit that rides with it until it's durable and committed, a duplicate, rejected or dropped. A host at 8,192 frames or 64 MiB in flight, or any host while the node is at 32,768 or 384 MiB, stops reading its socket. Replays come through the same socket path. Metrics: `vlrelay_upstream_inflight_{events,bytes}`, `_paused_hosts`, `_pauses_total{cap}`, `_host_inflight_events_max`. The admin's host views carry each host's `inflight_events`, `inflight_bytes` and `paused`. Flags: `--host-inflight-events/-mb`, `--inflight-events/-mb`. | 44147703 |
| **FutureCursor replays from 0** (issue 1) | See the trade-off below. | 9689be4c |
| **Lease plane** (issue 9) | vlpds `ClusterConfig::lease_plane`: the renew loop and the watchdog run on a one-thread runtime of their own, and the lease PUT goes through a second object-store client, whose connections are driven on that runtime. Renewals asked for elsewhere (a step's keepalive, the join, the drain) are handed to the plane's loop, because a renewal on a starved runtime held the renew lock across its late answer and starved the plane too (b0316c6a). The public listener runs on the subscriber runtime, so a storm's accepts stay off the pipeline and peer RPC. | c83afb42, b0316c6a |
| **The DID-owner stage runs detached** | A peer's forward dropped mid-batch (its forwarder abandoned a hung owner, or the peer died) cancelled `apply_did` between an append and its durability. The DID's in-flight entry never resolved, so every later copy (a duplicate) answered "the duplicated event isn't durable" until its forward gave up, again and again: 2,071 give-ups and a 78 s stall in one `minio-errors` run, 25 s in `kill9`, and the 30-39 give-ups robust-cluster saw in `crash-loop`. Every stage runs under `forward::Detached`, which spawns each batch as its own task. | 8bb815fc, 195fc6cb |
| **PLC trouble is waited out** | `identity_unavailable` after ~2 s of failed lookups was a rejection, so it was acked and lost, and the account's next commit failed prevData. 58 of them made up one `minio-errors` run's acked-but-lost. Lookups now retry for 30 s. The lane holds meanwhile, and the in-flight caps turn that into backpressure. | 74341df7 |
| **A seq whose copies all failed is a first sighting** | A `#sync` that restates the head is appended only on a first sighting. The replay of a fenced `#sync` counted as a replay of a copy that never landed, and was dropped (one `minio-errors` run, one event). | 382c21e5 |
| **A node lost while starting exits** | A core restarting into hung bucket requests lost its lease inside `start_cluster`, before `on_lost` was set, and lived on inert. | be8b126d |

Harness: a raised fd limit (the fault proxy ran out of fds within minutes under Linux's default 1024, which voided the first benchbox soak: a3b3e70f), `CHAOS_PROFILE` (`dev-release` for soaks), `CHAOS_NO_BUILD`, `RELAY_EXTRA`, a 16 MiB lag bound in `consumers`, `vlpds_firehose_disconnects` in the metric dumps, `e2e_check` telling a restarted sequence from a replay (7cb08a55), and the e2e scripts honouring `DEV_STATE` (7769d8cc).

### FutureCursor: replay from 0

A host answers our cursor with `FutureCursor` when its sequence restarted below it: a PDS whose sequencer was wiped, or one restored from a backup. indigo marks the host idle and stops (docs/reference-notes.md). Resuming live, as we did, skips whatever the host emitted between its restart and our reconnect, and every account touched then desynchronizes on its next commit.

The relay now resets the host's cursor to 0 and reconnects from there, so the host replays its new sequence from its first event:

- Wiped sequencer, same repos: the replay is exactly the window we missed. Commits apply in order.
- Restored from backup: the replay goes back over history we already have. Commits are dropped by rev (`stale`, answered as duplicates). `#identity`, `#account` and `#sync` of the new sequence are caught by the restart dedupe while their (host, seq, DID) is held (until the host's checkpoint passes it). The old sequence's entries are dropped once the reset reaches `hostck/` (a new sequence generation), so its events replayed from the backup are emitted again. They restate state, so a consumer sees a repeat, not a wrong state.
- The cost is one full replay of the host's window, which the in-flight caps bound in memory.

The alternative, marking the affected accounts for resync, needs to know which accounts the gap touched, which we can't know without the missing events. A non-archival relay would wait for each account's next `#sync`, which may never come. An archival one would re-fetch every account of the host. Replaying from 0 loses nothing the host still has, and the resync path stays as the fallback for what it doesn't have (`OutdatedCursor`).

When the reconnect comes long after the restart, the host's window may no longer reach back to it: the soaks saw `FutureCursor` answered by an `OutdatedCursor` at cursor 0 after a core spent a minute unable to start. What's gone is gone either way; those accounts desynchronize (open issue 14).

The cursor stays 0 through further reconnects and takeovers until acks move it. The old sequence's late acks no longer count, so they can't push the cursor back up. A `FutureCursor` in answer to cursor 0 is a broken host: it backs off.

### Results

Mac, dev build, same setup as above, one scenario at a time (load 40-100 from other agents). "Round 1" is the "after" column above. "Round 2" ran on this work alone; "rebased" on it rebased onto the robust-cluster pass (0886ce5b).

| Scenario | Round 1 | Round 2 | Rebased |
|---|---|---|---|
| minio-errors | 29 missing / 0 lost; 0 / 0; 166 / 170 | five runs: 0 / 0 four times, 1 / 1 once (the `#sync` fixed by 382c21e5); 0 out of order in all five | two runs: 0 / 0 both. Worst latency after the second heal 41-43 s (open issue 13) |
| upstream-restart | fail: 4,604 missing on every stream | pass: 0 missing, 0 rejections, 3 restarts followed | pass, same |
| consumers | fail: 59 missing, 10 node exits | pass, twice: 0 missing, 0 exits, worst 1.2-1.4 s at 107 connects/s; 22 `too_slow` drops at a 16 MiB bound | pass: 0 exits, worst 0.1 s at 216 connects/s; 28 `too_slow` drops |
| kill9 | 5.0, 2.9, 4.7 s | 4.9, 3.7, 3.7 s (once 4.9, 5.1, 14 s on a busier machine) | 4.7, 2.8, 2.7 s |
| zombie | 5.8-6.9 s; the zombie fail-stops on waking | 5.2 s for the 12 s stop, 4.5 s for the 9 s one; only the zombie exits | 4.8 s, 6.8 s; only the zombie exits |
| gc-pause | 1.8 s; 7.8 and 14 s | | 1.6 s; 2.8 s with no exit (revalidated); 6.9 s for the 1.5×TTL pause |
| crash-loop (robust-cluster's) | | | two runs, 20 kills each: 0 missing, 0 lost, identical streams, and 0 forward give-ups (robust-cluster saw 30-39 "isn't durable" give-ups in 2 of 4 runs before `Detached`). Per-crash stall ~3 s, one 15 s |

Before the stage ran detached, `kill9` showed a 25 s stall and a bystander exit in `zombie`; both are gone in the reruns. Two cores of two different clusters lapsed in the same instant once, from a stall of the whole Mac. With a 3 s TTL that's expected. `crash-loop`'s 20 kills run past the default 100 s of load, so chaos.sh ends before its report; report.py run by hand gives the verdict above (run it with `--duration 150`).

`just e2e` and `just e2e-cluster --duration 60` pass, before and after the rebase (on their own port block): single node 3,164 and 2,894 events matched, p50 27 ms; cluster 3,171 and 3,045 events identical on all five streams, worst pause 2.15 and 1.95 s after the kill -9. `cargo test --lib`: 147 pass after the rebase.

### The soak, again

benchbox (Ryzen AI Max+ 395), dev-release build, `CHAOS_PROFILE=dev-release CHAOS_BASE=4600 RETENTION_H=1 SOAK_EVERY=240 RELAY_MEM_MB=2400 tests/chaos/chaos.sh soak --rate 50 --fake-rate 600 --accounts 60`, the whole harness under a systemd scope capped at 9-10 GB (the box is shared). ~700 events/s, a fault every 4 minutes. Three runs:

- **Run 1** was void: the fault proxy ran out of fds (Linux's 1024) in its first minutes and every bucket and peer route hung. Fixed in the harness (a3b3e70f).
- **Run 2**, this work without the robust-cluster pass, 41.5 of 50 minutes before the scope's OOM killer ended it.
- **Run 3**, rebased onto the robust-cluster pass (without ef3f8c71), 29 of 47 minutes, then the same.

| | Old soak (Mac, dev build) | Run 2 | Run 3 |
|---|---|---|---|
| Healthy stretch | 24 min | 28 min: checker backlog 12-50 events, p50 44 ms | 24 min: backlog 34-380, p50 63-90 ms |
| Core RSS | 0.73-1.3 GB healthy, then 3.1 GB and 23 OOM kills at the 3 GB cap | 0.75-1.68 GB, never at the 2.4 GB cap | 0.45-1.13 GB until the stall; then one core at 7.3 GB (below) |
| `ack_pending` | 27k on one core | ≤ 600 healthy; 16-24k per core after the double crash, held there by the caps (8,192 per host, 32,768 per node; 1,912 host-cap pauses) | ≤ 590 healthy; 12.2k after the stall |
| Missing / acked-but-lost | 1.03M missing, 28,419 acked-but-lost | 0 through the healthy stretch; then 22 `prev_data_mismatch` and 38k `desynchronized` rejections on one core, all after 119 `OutdatedCursor`s | 0 through the healthy stretch; 45 `OutdatedCursor`s after the stall |
| How it ended | death spiral, 31 min behind | cores couldn't start during 5% bucket resets (a LIST body error fails startup, exit 1, restart loop), then a double crash: 1,500 forwards gave up with "no live owner" (shard opens of 15-28 s without the trim), the hosts' 64 MB replay windows ran out (`OutdatedCursor`), and the backlog reached 460k before the scope OOM'd | after a kill -9 every core went silent for ~7 minutes (no log lines at all), one vlrelay process grew to 7.3 GB anon RSS and the scope's OOM killer took it, then fakepds (3.1 GB) |

What held: memory stayed bounded through catch-up in both runs until the end, no reordering showed up (every `prev_data_mismatch` followed an `OutdatedCursor`, an upstream that no longer had the events), and the streams stayed identical. What didn't: neither run reached 45 minutes. The losses that remain come from hosts whose window ran out while the cluster couldn't keep up, which with fakepds's 64 MB window (~70 s per host at this rate) takes about a minute of trouble. A production PDS keeps days.

Run 3's 7.3 GB process isn't explained. It's the one relay process that ever passed 2 GB in these runs, and `capped.sh` (which polls RSS) didn't catch it. A caller that gives up and retries while its detached batch runs on would pile up batches against a stuck stage; ef3f8c71 bounds that, but whether it was the cause is unproven. The next soak should run with the harness's checkers outside the relays' memory scope, so the scope's OOM can only mean the relays.


## Found, not fixed (for the lead)

These cross into code other workstreams own (node.rs pipeline internals, upstream, the merge, vlpds membership), or they're design decisions.

1. ~~**A FutureCursor reconnect loses the restart window, then desynchronizes the host's accounts.**~~ Fixed in round 2 (below): the host replays its new sequence from cursor 0, and old-sequence acks no longer count. `upstream-restart` passes. Resyncing desynchronized accounts from `getRepo` on a non-archival relay is still open (it waits for the next `#sync`).
2. ~~**Kick-and-replay reorders a DID's events.**~~ Fixed in round 2: a give-up fences its host socket, and the replay brings everything again in order. `minio-errors`: 0 out of order and 0 acked-but-lost in 4 of 5 runs (the fifth lost one `#sync`, fixed since).
3. **Fixed** (robust-cluster pass, above). **One slow bucket path stalls the whole firehose** (the merge). 300-1,500 ms of latency on one core's bucket path stalled every stream for 16.4 s, until that core lost its lease and died. The merge waits on every live log's watermark, so the slowest core sets everyone's latency. A core whose segment PUTs stay past some bound should drain itself or give up its lease. A bucket blackhole (`node-bucket-hang`) is cleaner: the lease lapses and it's over in ~4.7 s.
4. **Fixed** (robust-cluster pass). **A core its peers can't reach keeps its shards** (vlpds membership). Membership is the bucket lease, so a core whose peer port is blackholed or refusing, but which still reaches the bucket, stays live. Events for its DIDs waited out the whole partition: 20.2 s for a 20 s blackhole and 15.9 s for 15 s of refusals. Nothing was lost. Peer reachability could feed liveness (the `refused` probe already does on a dead port), or a node could drop its lease when it sees its forwards failing.
5. **Fixed** (robust-cluster pass). **A pause between ~1.8 s and the TTL kills a node that nobody replaced.** With TTL 3 s, renewals every 0.6 s and 0.6 s of skew, a SIGSTOP of 2.8 s (0.9×TTL) lapsed the lease locally, the node fail-stopped on waking (`lapsed before renewal`), and the restart cost a 6.6 s stall, where riding out the pause would have cost 2.8 s. At the default 10 s TTL the window is ~6-10 s, which is GC-pause and VM-migration territory. vlpds could CAS-renew a lapsed lease if nobody fenced the log yet, instead of fail-stopping. That's safe only if peers fence before taking shards, which they do.
6. **Any core pause stalls every stream.** Each core merges every log, so a paused core's log holds the merge until its lease lapses and it's fenced. A 1.6 s SIGSTOP showed up as a 1.6 s stall on all five streams. This is by design, but it's the cost of the total order: TTL + skew bounds the stall for any hung node.
7. **Fixed** (robust-cluster pass; push to replicas is still M5). **The replica falls far behind under bucket latency.** With 100-500 ms per request it lagged 24 s (cores 3.5 s) and took the whole fault window to come back: it polls LIST + GET per segment. Push to replicas (M5) would fix it.
8. ~~**Catch-up has no backpressure, and takeovers slow down as shard histories grow.**~~ Fixed: in-flight caps (round 2) and one-span shard histories (the robust-cluster pass). The round 2 soak ran on a build without the trim (below).
9. ~~**Consumer load starves lease renewal.**~~ Fixed in round 2: renewals run on their own runtime and bucket client, and the public listener on the subscriber runtime. `consumers`: 0 node exits at 107 connects/s (10 before). Slow consumers are dropped at the lag bound (128 MiB by default; `--max-lag-mb` sets it on a dev network); the scenario sets 16 MiB and the nodes logged 22 `too_slow` drops in its 40 s. The script's slow readers don't see the close: the relay closes after the error frame, but a reader taking 1 KB/s has megabytes of kernel buffer to drain first.
10. **Fixed** (robust-cluster pass). **A planned leave can fail under load.** The shutdown's fence-scan has a 3 s control-plane timeout. In the soak it timed out and the core exited 1 without fencing, which turned a SIGTERM into a crash.
11. **Redundant retention on every core, edge/replica clock assumption, idle heartbeats.** Untouched (docs/cluster.md). The clock-skew scenario needs a shim first (above).
12. **PLC trouble past 30 s still loses events.** The host stage now retries a failing DID lookup for 30 s, then rejects `identity_unavailable`, which is acked, and the account's next commit fails prevData. (On the DID owner's side, the state apply's own lookups, `identity_unavailable` is no longer acked since pipeline-fixes: the forwarder retries it, and a single node fences the socket and replays.) Holding the event (fail it, fence, replay later) would lose nothing, but one DID whose document never resolves would then hold its host's cursor for good. That needs a policy: a per-(host, seq) retry budget, or a parked-events queue that doesn't hold the cursor.
13. **Recovery after hung bucket connections is slow.** In every `minio-errors` run the worst latency after the second heal (20% hung connections) was 33-40 s, with one core fail-stopping ~20 s into the fault. A hung segment PUT waits out object_store's 30 s request timeout while the merge waits on that log's watermark (issue 3). A shorter timeout on segment PUTs, with the hedge, would bound it.
14. **`OutdatedCursor` isn't acted on.** When a host's window no longer reaches our cursor we log the `#info` and take what comes; the accounts with gaps desynchronize on their next commit and wait for a `#sync`. An archival relay could queue the host's accounts for a re-fetch right away.
15. **A relay process reached 7.3 GB in the round 2 soak** (run 3, above). Unexplained; ef3f8c71 bounds the one unbounded path this work added. The pipeline-fixes review found one more unbounded structure, the ack tracker behind a failed entry no replay ever brought back (fixed: a newer socket's higher seq settles it as skipped), but it can't explain 7.3 GB: an entry is well under 100 bytes, the run saw about 1.2M events in its 29 minutes, and its in-flight permits are released when each event is done, not when the cursor moves. Tens of MB at most.
16. **A core can't start during bucket resets.** A LIST whose response body fails isn't retried at startup, so the core exits 1 and restarts every second until the fault ends (run 2 lost a minute of two cores this way).

## The soak

`CHAOS_BASE=3900 RETENTION_H=1 SOAK_EVERY=240 just chaos soak --duration 3900 --rate 50 --fake-rate 600 --accounts 60` on the Mac, with the build after the first four fixes (without c906521b and d6c95714). That's ~700 events/s for 65 minutes, 2.66M upstream events, a fault every 4 minutes. It ran beside the scenario batches on a machine at load 60-120.

**It failed, and the failure is the most important result here.** The first 24 minutes went well: every fault recovered (worst latencies 1.5-43 s, the long ones after bucket latency and a peer partition), the streams stayed identical, and RSS was flat. Then a SIGTERM'd core's planned leave failed (`fencing our log on shutdown: control-plane fence-scan timed out after 3s`, under a backlog from the faults before) and it exited 1 without fencing, so peers had to wait out its lease. The cluster entered a death spiral it never left:

1. A takeover after a crash replays the dead owner's log span. Each crash adds a span to a shard's history, so by then a shard open replayed 10 spans and took ~14 s.
2. While a DID has no owner, its forwards fail after 20 s, the host is kicked and replays from its cursor, and the host owner keeps the replayed events in flight: `vlrelay_ack_pending` reached 27k on one core.
3. The cores' RSS climbed past their 3 GB cap and `capped.sh` killed them, 23 times in the last 40 minutes. Each kill added more spans, more replays and more churn.
4. The kick-and-replay reorders (open issue 2) desynchronized most accounts: 439k `desynchronized` rejections.

At the end, stream latency was 31 minutes, and 1.03M of the 2.66M events were missing from every stream. Most were still in backlog when the window closed; 28,419 were acked-but-lost entries, nearly all desync rejections. Still, all five streams were identical, with 0 duplicates and 0 seq regressions throughout, and 1,300 upstream replays were skipped correctly.

Resources (60 s samples):

| | First 24 min | After |
|---|---|---|
| Core RSS | 0.73-1.3 GB, no trend (n1 864 MB at 6 min, 1.06 GB at 22 min). One core spiked to 1.9 GB during the bucket-latency fault and came back | 0.1-3.1 GB, OOM-killed at the 3 GB cap |
| Edge / replica RSS | 0.62-0.91 GB, flat | ~50-120 MB: nothing reached them |
| Open fds | 45-65 per core, 25-27 edge/replica, flat | up to 157 during catch-up |
| Log objects | +4.5k a minute (28k at 6 min, 105k at 22 min) | 165k at the end across 28 logs: every restart leaves a fenced log that only retention removes. The 1 h retention had just started (`retain/` appeared at 63 min) |
| `state/` objects | 3.3-4.7k, compaction keeps up | 3.1-3.4k |
| `dedupe/` objects | 0 | 3-8, bounded |
| MinIO | 390-550 MB | |

No leak shows in the first 24 minutes. Memory is bounded by load, not by time. What the soak exposed is the missing bound on in-flight work during catch-up. That, and the takeovers slowing as shard histories grow, turn a few faults into a cluster that can't catch up. The fix is backpressure: stop reading an upstream once its host has too much in flight, and compact a shard's history so an open replays at most one span (or checkpoint the earlier owners' markers when it opens). Both live in node.rs and cluster. Note the soak ran the dev build (the relay crate at opt-level 0) on a loaded laptop, which lowered the catch-up throughput. A dev-release soak on benchbox is the next run to make, after the backpressure fix.

## Harness notes

- The proxy accepts a connection before it dials the target, so a dead node's peer port looks like a reset instead of a refusal. The relay's fast takeover (`ShardHost::refused`, ~1.6 s in docs/cluster.md) doesn't fire through it, and kill -9 costs TTL + skew instead: 3.2-5.2 s here. A load balancer or service mesh in front of the peer port would do the same.
- A consumer socket to a SIGSTOPped node just hangs (no read timeout in `e2e_check`, nor in most real consumers), so the core checker attached to it reports nothing until the node fail-stops on waking.
- MinIO keeps its data in tmpfs, which counts against its 1 GB container cap. At ~1k events/s it was OOM-killed after ~13 minutes. The soak runs it on a docker volume (`tests/chaos/minio-disk.yml` through `DEV_COMPOSE_EXTRA`).
- The scenarios run serially. Each takes 2.5-3 minutes end to end with a warm build.
