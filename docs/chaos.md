# vlRelay chaos

We break the cluster on purpose and check what survives. `tests/chaos/chaos.sh` runs the cluster e2e's three cores, edge and replica on MinIO under steady load, injects one fault schedule, then checks the same invariants after every scenario.

```
just chaos list
just chaos kill9                      # or: tests/chaos/chaos.sh kill9 [--duration 100] [--rate 50] [--fake-rate 200]
VLRELAY_BIN=/path/to/old/vlrelay just chaos zombie   # the same scenario on another build
CHAOS_BASE=4150 just chaos minio-errors              # another port block, beside a running one
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

- One `crash-loop` run and one `gc-pause` run had a forward give up after 20 s with "the duplicated event isn't durable" (30 events of 3 DIDs, and 1): the DID owner's in-flight append that a duplicate waits on didn't resolve within its 10 s, twice. The rerun of `crash-loop` and the last `gc-pause` had none, and neither did either base run, so whether one of these changes makes it likelier is open. It's the robust-pipeline workstream's ground (`node/cluster.rs` Stage, kick-and-replay).
- The worst per-crash stall in `crash-loop` has a 15-32 s tail in both builds: crashes every 6 s land while the previous victim is rejoining and shards are moving back.
- Cores and edges still catch a log up from the bucket with vlpds's one-GET-per-segment `catch_up` on every stream reconnect; the replica's window would fit there too (vlpds code).
- `partition-peer`: the few events held on the cores until the heal (above). A follower that can't reach a live log's stream never falls back to the bucket while the lease is live (vlpds `remote::follow_log`); that fallback would fix this and the symmetric-partition stall.
- A step-down is a fail-stop and a restart. Under an asymmetric link (we reach our own address, peers don't) the lease still rules.

## Found, not fixed (for the lead)

These cross into code other workstreams own (node.rs pipeline internals, upstream, the merge, vlpds membership), or they're design decisions.

1. **A FutureCursor reconnect loses the restart window, then desynchronizes the host's accounts** (upstream). On FutureCursor the client resumes live (`skip_cursor`). Everything the host emitted between its restart and the reconnect is skipped, so each affected account's next commit fails `prev_data_mismatch`, the account is marked desynchronized, and every later commit is rejected until a `#sync`. `upstream-restart`: 4,604 of host 0's events missing on every stream, 6,116 `desynchronized` rejections. fakepds has no `getRepo`, so nothing resyncs. The options: reconnect from cursor 0 after FutureCursor (right for a wiped PDS whose window starts at the restart; a PDS restored from backup would replay history, where commits are caught by rev but `#identity`/`#account` would repeat), or resync desynchronized accounts from `getRepo`. The second is needed anyway. Separately, an old-sequence ack landing after the reset pushes `acked_seq` back up (it's `fetch_max`), so the next reconnect gets FutureCursor again. The report's acked-but-lost count is inflated for this scenario because the checkpoint it compares with is from the old sequence.
2. **Kick-and-replay reorders a DID's events** (node.rs pipeline: the lanes and acks). When a forward gives up (20 s, no owner), the host owner fails the ack and kicks the socket, and the host replays from its cursor. Later events of the same DID that were already past the socket's queue still go ahead. The DID owner then sees N+1 before N: `prev_data_mismatch`, desynchronized, and every later commit is rejected (and acked, so the checker shows those as acked-but-lost). After the fixes, `minio-errors` still lost 29 events across 5 accounts this way, with 26 out of order. A fix holds every later in-flight event of the host (or at least of that DID) when one fails, or treats a mismatch from a host that was just kicked as retryable instead of desynchronizing.
3. **Fixed** (robust-cluster pass, above). **One slow bucket path stalls the whole firehose** (the merge). 300-1,500 ms of latency on one core's bucket path stalled every stream for 16.4 s, until that core lost its lease and died. The merge waits on every live log's watermark, so the slowest core sets everyone's latency. A core whose segment PUTs stay past some bound should drain itself or give up its lease. A bucket blackhole (`node-bucket-hang`) is cleaner: the lease lapses and it's over in ~4.7 s.
4. **Fixed** (robust-cluster pass). **A core its peers can't reach keeps its shards** (vlpds membership). Membership is the bucket lease, so a core whose peer port is blackholed or refusing, but which still reaches the bucket, stays live. Events for its DIDs waited out the whole partition: 20.2 s for a 20 s blackhole and 15.9 s for 15 s of refusals. Nothing was lost. Peer reachability could feed liveness (the `refused` probe already does on a dead port), or a node could drop its lease when it sees its forwards failing.
5. **Fixed** (robust-cluster pass). **A pause between ~1.8 s and the TTL kills a node that nobody replaced.** With TTL 3 s, renewals every 0.6 s and 0.6 s of skew, a SIGSTOP of 2.8 s (0.9×TTL) lapsed the lease locally, the node fail-stopped on waking (`lapsed before renewal`), and the restart cost a 6.6 s stall, where riding out the pause would have cost 2.8 s. At the default 10 s TTL the window is ~6-10 s, which is GC-pause and VM-migration territory. vlpds could CAS-renew a lapsed lease if nobody fenced the log yet, instead of fail-stopping. That's safe only if peers fence before taking shards, which they do.
6. **Any core pause stalls every stream.** Each core merges every log, so a paused core's log holds the merge until its lease lapses and it's fenced. A 1.6 s SIGSTOP showed up as a 1.6 s stall on all five streams. This is by design, but it's the cost of the total order: TTL + skew bounds the stall for any hung node.
7. **Fixed** (robust-cluster pass; push to replicas is still M5). **The replica falls far behind under bucket latency.** With 100-500 ms per request it lagged 24 s (cores 3.5 s) and took the whole fault window to come back: it polls LIST + GET per segment. Push to replicas (M5) would fix it.
8. **Second half fixed** (robust-cluster pass: histories trim to one span). **Catch-up has no backpressure, and takeovers slow down as shard histories grow** (node.rs, cluster). This is the soak's death spiral, described below. It's the top item.
9. **Consumer load starves lease renewal.** A reconnect storm (57-74 connects/s, half replaying from cursor 0) on a loaded machine delayed renewals past the 3 s TTL, and cores fail-stopped (`lapsed past takeover`): 3 exits before the fixes, 10 after. Renewal should run where serving can't starve it, such as a dedicated thread or runtime. Slow consumers (1 KB/s) weren't dropped within 40 s at ~1 MB/s of stream, so each holds up to its lag bound in memory.
10. **Fixed** (robust-cluster pass). **A planned leave can fail under load.** The shutdown's fence-scan has a 3 s control-plane timeout. In the soak it timed out and the core exited 1 without fencing, which turned a SIGTERM into a crash.
11. **Redundant retention on every core, edge/replica clock assumption, idle heartbeats.** Untouched (docs/cluster.md). The clock-skew scenario needs a shim first (above).

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
