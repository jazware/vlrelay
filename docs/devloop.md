# vlRelay dev loop

Everything here runs from `vlrelay` with `just`. Run `just` on its own to list the recipes.

## Edit, check, test

```
just check          # cargo check --all-targets
just build          # the relay binary (dev profile)
just test           # cargo nextest run if installed, else cargo test
just watch [job]    # re-run check (or test, clippy, build) on every save
just buildtime      # time no-op and one-module-edit builds (scripts/buildtime.sh)
```

`just watch` uses bacon if it's installed (`cargo install --locked bacon`), then cargo-watch, and otherwise falls back to a polling loop. Neither tool is on this Mac yet, so you get the polling loop. It's fine, just a bit noisier.

## The local network

```
just dev-up                 # MinIO, PLC, reference PDS, 2 vlpds upstreams (~5 s warm)
just dev-seed 30            # 30 accounts round-robin across all upstreams (~1 s)
just dev-load 50            # 50 writes/s until ^C (or: just dev-load 50 60 for 60 s)
just relay                  # the relay against every upstream (once it has a CLI)
just e2e-check              # compare the relay's firehose with the upstreams'
just dev-down               # stop everything and delete dev/state
```

What `dev-up` brings up (ports in `dev/ports.sh`, all on 127.0.0.1):

| Port | What | Notes |
|---|---|---|
| 2980 | the relay | `just relay` and the e2e start it here |
| 2982 | PLC directory | did-method-plc with Postgres in tmpfs. Every DID in the network lives here. |
| 2983 | reference PDS | `ghcr.io/bluesky-social/pds:0.4`, dev mode, URL `http://localhost:2983`, handles `*.test` |
| 2984, 2985 | vlpds upstreams | native, `--memory`, handles `*.pds1.test`, `*.pds2.test`. `DEV_PDS=3` adds 2986. |
| 2990, 2991 | MinIO | S3 and console, `minioadmin`/`minioadmin`, bucket `vlrelay` |

The docker half (`dev/docker-compose.yml`, project `vlrelay-dev`) reuses the images from vlpds's migrate e2e and `build/`, so a machine that has run those builds nothing. The vlpds upstreams are this tree's vlpds, built from this crate's lockfile and target dir (`cargo build -p vlpds --bin vlpds`). That costs ~80 s once and shares every dependency with the relay build.

Nothing leaves the machine. The vlpds upstreams run with `--crawlers ''` (vlpds tells `bsky.network` about itself by default) and the reference PDS with `PDS_CRAWLERS=""`.

Every piece has a memory cap. The containers have `mem_limit` (1 GB for MinIO and the reference PDS, 512 MB for PLC and Postgres). The vlpds upstreams and the relay run under `dev/capped.sh`, which uses a systemd scope with `MemoryMax` on Linux. macOS has no per-process cap, so there it polls the process tree's RSS every second and kills it past the cap. Each vlpds gets 2 GB (`VLPDS_MEM_MB`) with small rings and caches, and sits around 160 MB at rest.

`dev/state/` holds the pids, the logs (`pdsN.log`), the upstream list (`hosts`) and `accounts.json`. The accounts only exist while the network is up, so `dev-down` deletes it.

### Load

`devnet load` is open loop. Writes start on a fixed schedule, and a write that can't start because 512 are already in flight counts as dropped. Each write is a post (35%), like (25%), follow (10%), delete or unfollow of a record this run made (15%), profile edit (7%) or repost (8%). On top of that it changes a handle every `--identity-every` seconds (20) and deactivates an account for 5 s every `--deactivate-every` seconds (45). At startup it logs every account in and reactivates it. So every event type shows up: `#commit`, `#identity`, `#account`, and `#sync`, which both PDSes emit on reactivation.

Measured on the Mac (M-series, 14 cores, other builds running): 50 writes/s and ~470 writes/s across 60 accounts, both with 0 errors and 0 drops.

## The e2e contract

The e2e starts the relay like this, and `just relay` does the same:

```
vlrelay --listen 127.0.0.1:2980 \
        --memory \
        --plc-url http://127.0.0.1:2982 \
        --linger-ms 25 \
        --host http://127.0.0.1:2984 --host http://127.0.0.1:2985 --host http://localhost:2983
```

- `--listen ADDR` serves `com.atproto.sync.subscribeRepos` and answers `GET /xrpc/_health` with 200 once it's ready. The e2e waits up to 20 s for that.
- `--memory` keeps everything in memory. The bucket form takes vlpds's flag names: `--s3-endpoint http://127.0.0.1:2990 --s3-bucket vlrelay --s3-access-key minioadmin --s3-secret-key minioadmin --prefix <run>`.
- `--host URL` (repeatable) is an upstream to subscribe to. An `http://` origin means plain `ws://`, which the dev network needs. A bare hostname means `wss://`. `--crawl` (accept `requestCrawl`) can replace or add to it.
- `--plc-url` resolves `did:plc` against the local directory, since none of these DIDs exist anywhere else.
- `--linger-ms` is the segment linger (PLAN.md decision 1).

The e2e checks the relay's `--help` for `--listen` and `--host`. Until both show up it prints `SKIP relay` and checks the upstreams against themselves instead, which still exercises the network, the load and the checker.

The relay implements the contract (`src/main.rs`, pipeline in `src/node.rs`). Its other flags:

- `--admin-token T` turns on `/admin` (the dashboard, from `--ui-dir` or this tree's `ui/dist`) and its API. Without it there's no `/admin`.
- `--dev-mode` allows `ws://`, IPs, localhost and ports. An `http://` `--host` or a loopback `--plc-url` turns it on by itself.
- `--did-shards N` (4 alone, 24 in a cluster) is the number of DID state shards, each a SlateDB. `--retention H` (72) is the log's replay window in hours.
- `--lanes N` (64) and `--ingest-threads N` (cores, at most 16) size the pipeline. `--did-lookups-per-sec` (50) is the DID document budget.

One listener serves `GET /xrpc/_health` (`{"version"}`), `subscribeRepos`, the sync API (`listRepos`, `getRepoStatus`, `getLatestCommit`, `listHosts`, `getHostStatus`), `requestCrawl` (with `--crawl`), `/admin` and Prometheus `/metrics`. Every response carries `Server: vlrelay/… (atproto-relay)`, so other relays won't crawl it. The relay's own series are `vlrelay_*`: events in by kind, accepted by kind, out, rejected by reason, duplicates by where they were caught, time to firehose and time to durable (histograms), time per pipeline stage, durable lag, hosts by status, consumers. vlpds's firehose and process series come with them.

`just e2e-archival` is the archival mode's e2e on its own ports (docs/archival.md).

`just e2e-reshard` splits a DID shard of a three-core cluster and merges the halves back, under load with archival on and the PLC export seeding, on its own ports (base 3680, nodes on 3700+; docs/cluster.md, "Resharding").

## e2e_check

`e2e_check` subscribes to each upstream's own `subscribeRepos` and to the relay's, and matches events across them:

- `#commit` and `#sync` match on (DID, rev, commit CID). A second copy on the relay counts as a duplicate.
- `#identity` matches on (DID, handle) and `#account` on (DID, active, status). Neither has a rev, so each also carries its occurrence count. A relay that makes up its own identity events will show them as extras of those kinds, which doesn't fail the run.

Both streams are timestamped when they arrive at the checker, so the latency (upstream emit to relay emit) needs no clock agreement with either server. Upstream events seen in `[--warmup, --duration]` are expected on the relay, and the relay gets `--settle` more seconds to deliver them. It reports missing, extra, out of order per DID (relative to the upstream's order), rev regressions, duplicates, relay seq regressions and latency p50/p90/p99/max, per kind and overall. `--json-out` writes the same as JSON. It exits 1 on anything missing, extra commits, disorder, regressions or duplicates, unless `--report-only` is set.

`--relay` is repeatable and the relay side is the union of its streams. So `just e2e-self` (each upstream compared with itself) is the checker's own test, and it should report zero of everything:

```
  kind        upstream     relay   matched  missing    extra
  #commit         2945      2945      2945        0        0
  #sync             41        41        41        0        0
  #identity         43        43        43        0        0
  #account          42        42        42        0        0
  upstream rate 102 ev/s, DIDs 60
```

Pointing `--upstream pds1 --relay pds2` at each other reports everything as missing and extra, as it should.

For the production relay against a real PDS, use `--scope seen` (or `hosted`, which lists the PDS's repos first) so the rest of the network is out of scope:

```
target/debug/e2e_check --upstream https://morel.us-east.host.bsky.network --relay wss://bsky.network \
    --scope seen --duration 25 --settle 15 --report-only
```

A 25 s run of that matched 195 of 196 commits, with p50 ~140 ms and p99 ~1.3 s from that PDS to `bsky.network`.

## Full e2e

```
just e2e [--duration 60] [--rate 50] [--accounts 30]     # KEEP=1 leaves the network up
```

`tests/e2e/run.sh` builds the relay, `e2e_check` and `devnet`, runs `dev-up`, seeds, starts the relay (or skips it, as above), starts the checker, then the load, and tears it all down. Logs, `report.json` and the relay's `vlrelay_*` metrics (`metrics.txt`) go to `dev/state/e2e/`, copied to `$TMPDIR/vlrelay-e2e-last` before teardown. A 30 s run at 100 writes/s takes ~52 s end to end with a warm build.

Two more flags:

- `--bucket` runs the relay on the dev MinIO (`:2990`, a new prefix per run) instead of `--memory`.
- `--restart-at S` kill -9s the relay S seconds into the load and starts it again on the same prefix. It implies `--bucket`. The checker's relay socket reconnects with its last cursor, so a gap or a duplicate across the restart fails the run.

### Results

Measured on the Mac (M-series, 14 cores), dev build (the relay crate at opt-level 0, dependencies at 2), three upstreams (two vlpds and the reference PDS).

| Run | Events | Missing | Extra | Reordered | Dups | Upstream → relay p50 / p90 / p99 / max |
|---|---|---|---|---|---|---|
| `--duration 30 --rate 50`, memory | 1,658 | 0 | 0 | 0 | 0 | 27 / 29 / 31 / 39 ms |
| `--duration 60 --rate 400 --accounts 60 --bucket` | 24,223 | 0 | 0 | 0 | 0 | 17 / 28 / 30 / 36 ms |
| `--duration 60 --rate 50 --restart-at 20` | 3,168 | 0 | 0 | 0 | 0 | 29 / 32 / 265 / 869 ms |

The floor is the 25 ms linger plus a MinIO PUT. Each run covers every event type: `#commit`, `#sync` (on reactivation), `#identity` (handle changes) and `#account` (deactivations).

In the restart run, the relay was back serving 1 s after the kill. It replayed 113 state deltas from the dead log in 0.6 s, and each upstream resumed from its last durable cursor. The upstreams then re-sent 112 events that were already in the log past those cursors. The relay dropped all of them (`vlrelay_events_duplicate_total{at="restart_log"}`), so the checker, resuming from its own cursor, saw no gap and no duplicate. The p99 is the events caught in the kill.

Time per event in each stage, from `vlrelay_stage_busy_us_total` over the 400/s run (dev build, so upper bounds): strict parse 10 µs, verify (hashes, signature, MST inversion) 46 µs, apply (the DID owner's state step) 20 µs.

## Cluster e2e

```
just e2e-cluster [--duration 90] [--rate 50] [--accounts 30] [--no-ha]
                 [--kill-at 20] [--restart-at 35] [--term-at 50] [--return-at 60]
```

`tests/e2e/cluster.sh` brings the network up with 4 upstreams (`DEV_PDS=3`), starts three core relays on one fresh MinIO prefix with peer mTLS (`--dev-mode` issues the certificates into `dev/state/peer-tls`), then an edge and a replica. It runs five checkers, one per relay stream, each with the same upstreams:

- one per core, with `--relay a,b,c` listing all three cores in a different order. A socket that closes or can't connect moves to the next server with its cursor, so the consumers of a dead node reconnect elsewhere, as real ones would.
- one on the edge and one on the replica.

Each checker must report 0 missing, extra, reordered and duplicated events on its own. They also write every relay event (`--seq-out`) and every latency (`--lat-out`), and `tests/e2e/cluster_report.py` checks that all five streams carry the same events at the same relay seqs, then reports steady-state latency, the pause after each HA action, and when the survivors' logs show the shards moving.

The HA schedule (seconds into the load): kill -9 the core with the most upstream sockets at `--kill-at`, start it again at `--restart-at`, SIGTERM the busiest of the other two at `--term-at`, start it again at `--return-at`. `--no-ha` runs steady state only.

Ports: node i (cores 1-3, the edge 4, the replica 5) serves on `CLUSTER_PORT_BASE`+i (base 2960, so :2961-:2965) and listens for peers on +10+i. Every port in `dev/ports.sh` can be moved by env, and `COMPOSE_PROJECT_NAME` separates the docker half, so a cluster run can sit beside another worktree's e2e:

```
COMPOSE_PROJECT_NAME=vlrelay-cluster PLC_PORT=3182 REF_PDS_PORT=3105 PDS_BASE_PORT=3106 \
  MINIO_PORT=3190 MINIO_CONSOLE_PORT=3191 CLUSTER_PORT_BASE=3160 HOST_SHARDS=15 just e2e-cluster
```

Which node owns which upstream follows from the hostnames' hashes, so the ports pick the split. With the ports above and 15 host shards the 4 upstreams land 2/1/1. With the defaults and 16 host shards they all land on one node. Env: `TTL_MS` (3000, the lease TTL), `HOST_SHARDS` (16), `KEEP=1`, `OUT` (`dev/state/e2e-cluster`, copied to `$TMPDIR/vlrelay-e2e-cluster-last`).

Results (Mac, dev build, the ports above, `--duration 80`, about 110 s end to end): 4,173 events on every stream, 0 missing, extra, reordered or duplicated on all five, the same seqs on all five. Steady-state p50 33 ms on the cores (29.6 ms for one node on the same network and load) and ~308 ms on the edge and the replica. kill -9 paused events up to 1.6-1.8 s, a planned handoff up to 0.66-0.89 s, and a rejoin's rebalance up to 0.2-1.2 s. docs/cluster.md has the breakdown.

### Against real PDSes

`scripts/prodcmp.sh [SECONDS]` runs vlRelay locally (`--memory`, 127.0.0.1) against the same three PDSes the reference work used (amanita, eurosky.social, blacksky.app), with plain `--host` and no requestCrawl. It runs one checker with `--separate`, which compares vlRelay and `wss://bsky.network` with the PDSes each on its own, over the same PDS sockets, so both latencies are measured against the same arrivals and each PDS gets one socket from the checker.

Use `--scope all` for vlRelay (`--relay-scope`, in `--relay` order), since it carries only those hosts, and `--scope seen` for production. With `seen`, a relay event for a DID the checker's PDS sockets haven't named yet is held for up to 60 s and matched once they do. Before that hold, such an event counted out of scope and its PDS copy then counted missing: vlRelay beats the checker's PDS sockets often enough (~1,250 relay-first events in 10 minutes) that the first run reported 221 commits missing that vlRelay had in fact emitted, and the production column below still carries ~60 of these per 10 minutes.

10 minutes, 2026-10-04 ~10:33Z, ~14 events/s, 1,938 DIDs:

| | Commits matched | Missing | Extra | Reordered | p50 | p90 | p99 | max |
|---|---|---|---|---|---|---|---|---|
| vlRelay (local, dev build) | 8,283 of 8,285 | 1 | 0 | 0 | 32 ms | 109 ms | 290 ms | 884 ms |
| bsky.network | 8,198 | 62 (scope artifact) | 0 | 0 | 97 ms | 183 ms | 361 ms | 3.0 s |

The run before it (`--scope seen` for both) gave vlRelay p50 32 ms, p90 175 ms, p99 559 ms against production's 104, 275 and 755 ms. Production's baseline in `reference-notes.md` is ~90 ms p50 and ~590 ms p99. The tails are mostly PDS burstiness that both relays see, and vlRelay sits on the same machine as the checker, so this measures pipeline delay, not network distance.

Rejects on real traffic over the two runs (~16k events): one `bad_op` from eurosky.social in the second, which is the one missing commit. The first had 2 `bad_op`, 2 `commit_rev_mismatch`, 2 `prev_data_mismatch` (each followed by `desynchronized` drops for that account until a `#sync`) and 1 `wrong_host`. There was no `missing_record_block`. `bad_op` is the strict parse rejecting an op (most likely a record path that `valid_record_path` refuses), where indigo is lenient. The verify workstream should look at the frame.

## benchbox

```
just benchbox-sync            # tracked + uncommitted files (not ignored) -> benchbox:~/vlrelay-dev (~1-4 s)
just benchbox-build           # sync, then cargo build --profile dev-release --bins there
just benchbox-run vlrelay --help                  # run a bin under MemoryMax (MEM, default 8G)
scripts/benchbox.sh ssh       # a shell in the remote crate dir
scripts/benchbox.sh clean     # delete ~/vlrelay-dev
```

The sync ships `vlrelay` and `vlpds` (a path dependency, without its bench results) with rsync, and deletes remote files that are gone here. The target dir (`~/vlrelay-dev/target`) stays between runs, so builds are incremental. Builds run in a 16 GB scope (`BUILD_MEM`) at `nice`. Both `build` and `run` refuse while the batch pipeline is active or activating, or due within 15 minutes, or while a vlpds bench runs (the same guard as `vlpds/build/benchbox-image.sh`). `FORCE=1` skips the guard. `REMOTE_DIR` picks another directory.

Measured: a cold dev-release build takes 2m12s, and an edit to one vlrelay module takes ~7 s including the sync.

## Build speed

Measured with `scripts/buildtime.sh`, median of 3 (2 for the big-crate rows), on the Mac with other agents' builds running (load average 25-85, so treat these as upper bounds).

| Step | Before | After |
|---|---|---|
| Cold dev build (all of vlpds and its deps) | 9m24s | same |
| vlrelay edit, `cargo check` | 0.28 s | 0.28 s |
| vlrelay edit, build the relay | 0.40 s | 0.43 s |
| vlrelay edit, build the lib tests | 0.31 s | 0.30 s |
| vlpds-sized crate edit (80k lines), rebuild and link a 65 MB binary | 45 s (opt-level 2), 42 s (opt-level 1) | 4.2 s (opt-level 0, line tables) |
| Relink only, 65 MB binary, Apple ld (ld-prime) | 0.9-1.4 s | |

The relay crate is a stub today, so its own numbers are all link and cargo overhead. The row that matters is the vlpds one, since the relay will grow toward that size. An edit at opt-level 1 or 2 spends ~40 s optimizing the crate again. At opt-level 0 with line tables only it takes ~4 s (8 s with full debug info). So `Cargo.toml` now builds the vlrelay crate (and its bins) at opt-level 0 with line tables in dev and test, and dependencies stay at opt-level 2. That changed only this crate's settings, so it rebuilt nothing else. For real performance runs, use `--profile dev-release` or `--release`.

Things that didn't help:

- The linker. Apple's ld (ld-prime, Xcode 16) links the 65 MB vlpds binary in ~1 s, so there's little left for lld or mold to win. The toolchain's `rust-lld` through its `gcc-ld/ld64.lld` wrapper aborts when clang calls it with `-fuse-ld` (tried through a linker wrapper script, since removed). Switching the linker also invalidates every build script and proc macro, which cost a 3.5 min rebuild each way.
- cargo-nextest. The crate has no tests yet, so there's nothing to measure. Once there are a few hundred, it's worth trying, since nextest runs each test in its own process in parallel.
