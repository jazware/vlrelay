---
title: Development
section: Testing
order: 200
summary: Build and test vlRelay, run a local network of real PDSes on your machine, and check the relay's firehose against theirs event for event.
---

```hero
diagram:
  caption: "`just dev-up` starts the PLC directory, the reference PDS and two vlpds upstreams. `devnet load` writes to them, the relay subscribes to all three, and `e2e_check` reads both sides and matches every event. MinIO only comes in when the relay runs on a bucket."
  nodes:
    - { id: load, label: "`devnet load`", sub: writes · handles · deactivations, at: [0, 5], size: [9, 3] }
    - { id: pds, label: PDSes, sub: "reference PDS · 2 vlpds", at: [13, 5], size: [9, 3], tone: muted, stack: true }
    - { id: plc, label: PLC, sub: ":2982", at: [27, 0], size: [9, 2.6], tone: muted }
    - { id: relay, label: vlRelay, sub: ":2980 · one node", at: [27, 5], size: [9, 3], tone: accent }
    - { id: check, label: "`e2e_check`", sub: missing · extra · order, at: [27, 11], size: [9, 3], tone: solid }
    - { id: minio, label: MinIO, sub: ":2990 · bucket vlrelay", at: [41, 5.2], size: [10, 2.6], shape: store, tone: amber }
  edges:
    - "load -> pds: writes"
    - "pds -> relay: subscribeRepos"
    - { from: pds.t, to: plc.l, label: register, via: [[17.5, 1.3]] }
    - "relay -> plc: resolve"
    - "relay -> check: firehose"
    - { from: pds.b, to: check.l, label: their own streams, via: [[17.5, 12.5]] }
    - { from: relay.r, to: minio.l, label: "--bucket", dash: true }
facts:
  - { value: "~5 s", label: dev-up when warm, note: "all of it on 127.0.0.1, every piece memory-capped", tone: amber }
  - { value: "4", unit: event kinds, label: in every e2e run, note: "#commit · #sync · #identity · #account", tone: blue }
  - { value: "0", label: "missing, extra or reordered", note: "anything else fails the run", tone: accent }
  - { value: "~4 s", label: rebuild after an edit, note: "a vlpds-sized crate at opt-level 0 (measured)", tone: violet }
```

Everything here runs from the repository root with [just](https://github.com/casey/just). Run
`just` on its own to list the recipes. The local network is real software: the PLC directory,
Bluesky's reference PDS and two [vlpds](https://github.com/jazware/vlpds) servers, all on
127.0.0.1. So the e2e tests the relay against the frames real PDS software sends.

## What you need

- Rust. `rust-toolchain.toml` pins the version, and rustup installs it on the first build.
- Docker with compose, for MinIO, the PLC directory and the reference PDS.
- `just`, `curl`, `lsof` and `python3` (the chaos harnesses' fault proxy and reports).
- Go and Node, only for `just compat`.

vlpds comes in as a Cargo dependency, so you don't need a checkout of it. `dev-up` builds vlpds's
own binary from this crate's lockfile and target dir (`cargo build -p vlpds --bin vlpds`), which
costs ~80 s once and shares every dependency with the relay build. The first `dev-up` also builds
two images: MinIO from source (`vlpds-minio:local`, since MinIO no longer publishes container
images) and the PLC directory from did-method-plc's repo at a pinned commit. The reference PDS is
pulled from `ghcr.io/bluesky-social/pds`.

## Edit, check, test

```
just check          # cargo check --all-targets
just build          # the relay binary (dev profile)
just test           # cargo nextest run if installed, else cargo test
just clippy         # clippy with -D warnings
just watch [job]    # re-run check (or test, clippy, build) on every save
just buildtime      # time no-op and one-module-edit builds (scripts/buildtime.sh)
```

`just watch` uses bacon if it's installed (`cargo install --locked bacon`), then cargo-watch, and
otherwise falls back to a polling loop on `src/` and `Cargo.toml`. The loop works, it's just a
bit noisier.

The production image builds with the repository root as its context:
`docker build -t vlrelay:local .`. `--build-arg VLRELAY_TOOLS=1` adds `fakepds` and `e2e_check` to
it.

## The local network

```
just dev-up                 # MinIO, PLC, reference PDS, 2 vlpds upstreams (~5 s warm)
just dev-seed 30            # 30 accounts round-robin across all upstreams (~1 s)
just dev-load 50            # 50 writes/s until ^C (or: just dev-load 50 60 for 60 s)
just relay                  # a single-node relay against every upstream
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

The docker half is `dev/docker-compose.yml` (project `vlrelay-dev`). Nothing leaves the machine.
The vlpds upstreams run with `--crawlers ''` (vlpds tells `bsky.network` about itself by default)
and the reference PDS with `PDS_CRAWLERS=""`.

Every piece has a memory cap. The containers have `mem_limit` (1 GB for MinIO and the reference
PDS, 512 MB for PLC and Postgres). The vlpds upstreams and the relay run under `dev/capped.sh`,
which uses a systemd scope with `MemoryMax` on Linux. macOS has no per-process cap, so there it
polls the process tree's RSS every second and kills it past the cap. Each vlpds gets 2 GB
(`VLPDS_MEM_MB`) with small rings and caches, and sits around 160 MB at rest.

`dev/state/` holds the pids, the logs (`pdsN.log`), the upstream list (`hosts`) and
`accounts.json`. The accounts only exist while the network is up, so `dev-down` deletes it.

### Load

`devnet load` is open loop. Writes start on a fixed schedule, and a write that can't start because
512 are already in flight counts as dropped. Each write is a post (35%), like (25%), follow (10%),
delete or unfollow of a record this run made (15%), profile edit (7%) or repost (8%). On top of
that it changes a handle every `--identity-every` seconds (20) and deactivates an account for 5 s
every `--deactivate-every` seconds (45). At startup it logs every account in and reactivates it. So
every event type shows up: `#commit`, `#identity`, `#account`, and `#sync`, which both PDS
implementations emit on reactivation.

Measured on a 14-core laptop with other builds running: 50 writes/s and ~470 writes/s across 60
accounts, both with 0 errors and 0 drops.

## The e2e contract

The e2e starts the relay like this, and `just relay` does the same:

```
vlrelay --listen 127.0.0.1:2980 \
        --memory \
        --plc-url http://127.0.0.1:2982 \
        --qlog-listen 127.0.0.1:0 \
        --host http://127.0.0.1:2984 --host http://127.0.0.1:2985 --host http://localhost:2983
```

- `--listen ADDR` serves `com.atproto.sync.subscribeRepos` and answers `GET /xrpc/_health` with
  200 once it's ready. The e2e waits up to 20 s for that.
- `--memory` keeps everything in memory. The bucket form takes vlpds's flag names:
  `--s3-endpoint http://127.0.0.1:2990 --s3-bucket vlrelay --s3-access-key minioadmin
  --s3-secret-key minioadmin --prefix <run>`.
- `--host URL` (repeatable) is an upstream to subscribe to. An `http://` origin means plain
  `ws://`, which the dev network needs. A bare hostname means `wss://`. `--crawl` (accept
  `requestCrawl`) can replace or add to it.
- `--plc-url` resolves `did:plc` against the local directory, since none of these DIDs exist
  anywhere else.
- `--qlog-listen 127.0.0.1:0` puts the quorum log's peer port on a free loopback port. With no
  `--qlog-peer` the relay is a one-member quorum log ([Cluster](cluster.md)), and without
  `--qlog-dir` its log is in memory only.

If the relay's `--help` doesn't name `--listen` and `--host`, the e2e prints `SKIP relay` and
checks the upstreams against themselves instead.

The relay's entry point is `src/main.rs`, the pipeline is `src/node.rs`, and the quorum log is
`src/node/quorum.rs` and `src/qlog/`. A few more flags matter on a dev network:

- `--admin-token T` turns on `/admin` (the dashboard, from `--ui-dir` or this tree's `ui/dist`)
  and its API. Without it there's no `/admin`.
- `--dev-mode` allows `ws://`, IPs, localhost and ports. An `http://` `--host` or a loopback
  `--plc-url` turns it on by itself.
- `--qlog-flush-ms` (30000) is how often the leader flushes to the bucket, and
  `--qlog-retain-hours` (72) how long the bucket keeps log segments.
- `--lanes N` (64) and `--ingest-threads N` (cores, at most 16) size the pipeline.
  `--did-lookups-per-sec` (50) is the DID document budget, and `--did-lookup-prefetch` (256) how
  many lookups the dispatcher starts ahead of the lanes.

[Configuration](operations/configuration.md) lists every flag.

One listener serves `GET /xrpc/_health` (`{"version"}`), `subscribeRepos`, the sync API
(`listRepos`, `getRepoStatus`, `getLatestCommit`, `listHosts`, `getHostStatus`), `requestCrawl`
(with `--crawl`), `/admin` and Prometheus `/metrics`. Every response carries
`Server: vlrelay/… (atproto-relay)`, so other relays won't crawl it. The relay's own series are
`vlrelay_*` ([Monitoring](operations/monitoring.md)), and vlpds's firehose and process series come
with them.

## e2e_check

`e2e_check` subscribes to each upstream's own `subscribeRepos` and to the relay's, and matches
events across them:

- `#commit` and `#sync` match on (DID, rev, commit CID). A second copy on the relay counts as a
  duplicate.
- `#identity` matches on (DID, handle) and `#account` on (DID, active, status). Neither has a rev,
  so each also carries its occurrence count. A relay that makes up its own identity events will
  show them as extras of those kinds, which doesn't fail the run.

Both streams are timestamped when they arrive at the checker, so the latency (upstream emit to
relay emit) needs no clock agreement with either server. Upstream events seen in
`[--warmup, --duration]` are expected on the relay, and the relay gets `--settle` more seconds to
deliver them. It reports missing, extra, out of order per DID (relative to the upstream's order),
rev regressions, duplicates, relay seq regressions and latency p50/p90/p99/max, per kind and
overall. `--json-out` writes the same as JSON. It exits 1 on anything missing, extra commits,
disorder, regressions or duplicates, unless `--report-only` is set.

`--relay` is repeatable and the relay side is the union of its streams. So `just e2e-self` (each
upstream compared with itself) is the checker's own test, and it should report zero of everything:

```
  kind        upstream     relay   matched  missing    extra
  #commit         2945      2945      2945        0        0
  #sync             41        41        41        0        0
  #identity         43        43        43        0        0
  #account          42        42        42        0        0
  upstream rate 102 ev/s, DIDs 60
```

Pointing `--upstream pds1 --relay pds2` at each other reports everything as missing and extra, as
it should.

To check a relay that carries the whole network against one real PDS, use `--scope seen` (or
`hosted`, which lists the PDS's repos first) so the rest of the network is out of scope:

```
target/debug/e2e_check --upstream https://morel.us-east.host.bsky.network --relay wss://bsky.network \
    --scope seen --duration 25 --settle 15 --report-only
```

A 25 s run of that matched 195 of 196 commits, with p50 ~140 ms and p99 ~1.3 s from that PDS to
`bsky.network`.

## Full e2e

```
just e2e [--duration 60] [--rate 50] [--accounts 30]     # KEEP=1 leaves the network up
```

`tests/e2e/run.sh` builds the relay, `e2e_check` and `devnet`, runs `dev-up`, seeds, starts the
relay, starts the checker, then the load, and tears it all down. Logs, `report.json` and the
relay's `vlrelay_*` metrics (`metrics.txt`) go to `dev/state/e2e/`, copied to
`$TMPDIR/vlrelay-e2e-last` before teardown. A 30 s run at 100 writes/s takes ~52 s end to end with
a warm build.

Two more flags:

- `--bucket` runs the relay on the dev MinIO (`:2990`, a new prefix per run) instead of
  `--memory`.
- `--restart-at S` kill -9s the relay S seconds into the load and starts it again on the same
  prefix. It implies `--bucket`. The relay runs without `--qlog-dir`, so the restart recovers from
  the bucket's last flush and the upstreams resend the rest. The checker's relay socket reconnects
  with its last cursor, so a missing event across the restart fails the run.

### Results

These runs are from before the quorum log was vlRelay's only architecture. They ran on the
previous single-node design (a per-node log with a 25 ms segment linger), which has since been
deleted, so the latencies and the restart below are that design's. They haven't been rerun since.

Measured on a 14-core laptop, dev build (the relay crate at opt-level 0, dependencies at 2), three
upstreams (two vlpds and the reference PDS):

| Run | Events | Missing | Extra | Reordered | Dups | Upstream → relay p50 / p90 / p99 / max |
|---|---|---|---|---|---|---|
| `--duration 30 --rate 50`, memory | 1,658 | 0 | 0 | 0 | 0 | 27 / 29 / 31 / 39 ms |
| `--duration 60 --rate 400 --accounts 60 --bucket` | 24,223 | 0 | 0 | 0 | 0 | 17 / 28 / 30 / 36 ms |
| `--duration 60 --rate 50 --restart-at 20` | 3,168 | 0 | 0 | 0 | 0 | 29 / 32 / 265 / 869 ms |

The floor there was the 25 ms linger plus a MinIO PUT. Each run covers every event type:
`#commit`, `#sync` (on reactivation), `#identity` (handle changes) and `#account` (deactivations).

In the restart run, the relay was back serving 1 s after the kill. It replayed 113 state deltas
from the dead log in 0.6 s, and each upstream resumed from its last durable cursor. The upstreams
then re-sent 112 events that were already in the log past those cursors, and the relay dropped all
of them as duplicates. So the checker, resuming from its own cursor, saw no gap and no duplicate.
The p99 is the events caught in the kill.

Time per event in each stage, from `vlrelay_stage_busy_us_total` over the 400/s run (dev build, so
upper bounds): strict parse 10 µs, verify (hashes, signature, MST inversion) 46 µs, apply (the
state step, now the leader's) 20 µs.

## More test suites

```
just e2e-policy             # one relay against a fakepds fleet whose hosts misbehave
just compat                 # vlRelay beside indigo's relay, with ecosystem consumers on both
just relay-chaos list       # the relay on the quorum log under kill -9, power cuts, partitions
just qlog-chaos list        # the bare quorum log, no relay
```

- `just e2e-policy` (`tests/e2e/policy.sh`) runs one relay against five `fakepds` hosts at tier
  `default`. One sends bad signatures, one spams new accounts and one replays old frames. The run
  passes when the bad-signature host is auto-throttled, cases open for it and the spammer, a
  domain rule bans the replayer mid-run, and the clean hosts come through with nothing missing or
  extra. It also checks that a takedown hides an account's commits from a replay from cursor 0
  until it's lifted. It needs no docker, since `fakepds` serves its own PLC.
  [Load fleet](loadfleet.md) has the fleet.
- `just compat` (`tests/compat/run.sh`) runs vlRelay and indigo's relay side by side on one dev
  network, with `goat`, indigo's consumer, `@atproto/sync` and Jetstream on both.
  [Compatibility](compat.md) has the results.
- `just relay-chaos` and `just qlog-chaos` are the multi-node tests. There's no separate cluster
  e2e on the dev network. [Chaos](chaos.md) has the scenarios, what's checked and the results.

### Against real PDSes

`scripts/prodcmp.sh [SECONDS]` runs vlRelay locally (`--memory`, 127.0.0.1) against three real
PDSes (amanita, eurosky.social and blacksky.app by default, `PRODCMP_HOSTS` to change them), with
plain `--host` and no requestCrawl. It runs one checker with `--separate`, which compares vlRelay
and `wss://bsky.network` with the PDSes each on its own, over the same PDS sockets. So both
latencies are measured against the same arrivals, and each PDS gets one socket from the checker.

Use `--scope all` for vlRelay (`--relay-scope`, in `--relay` order), since it carries only those
hosts, and `--scope seen` for production. With `seen`, a relay event for a DID the checker's PDS
sockets haven't named yet is held for up to 60 s and matched once they do. Before that hold, such
an event counted out of scope and its PDS copy then counted missing. vlRelay beats the checker's
PDS sockets often enough (~1,250 relay-first events in 10 minutes) that the first run reported 221
commits missing that vlRelay had in fact emitted. The production column below still carries ~60 of
these per 10 minutes.

10 minutes, 2026-10-04 ~10:33Z, ~14 events/s, 1,938 DIDs, on the previous single-node design:

| | Commits matched | Missing | Extra | Reordered | p50 | p90 | p99 | max |
|---|---|---|---|---|---|---|---|---|
| vlRelay (local, dev build) | 8,283 of 8,285 | 1 | 0 | 0 | 32 ms | 109 ms | 290 ms | 884 ms |
| bsky.network | 8,198 | 62 (scope artifact) | 0 | 0 | 97 ms | 183 ms | 361 ms | 3.0 s |

The run before it (`--scope seen` for both) gave vlRelay p50 32 ms, p90 175 ms, p99 559 ms against
production's 104, 275 and 755 ms. Production's baseline in [Reference notes](reference-notes.md)
is ~90 ms p50 and ~590 ms p99. The tails are mostly PDS burstiness that both relays see. vlRelay
sits on the same machine as the checker, so this measures pipeline delay and leaves network
distance out.

Rejects on real traffic over the two runs (~16k events): one `bad_op` from eurosky.social in the
second, which is the one missing commit. The first had 2 `bad_op`, 2 `commit_rev_mismatch`, 2
`prev_data_mismatch` (each followed by `desynchronized` drops for that account until a `#sync`) and
1 `wrong_host`. There was no `missing_record_block`. `bad_op` is the strict parse rejecting an op
(most likely a record path that `valid_record_path` refuses), where indigo is lenient. That frame
still needs a look. The [shadow run](shadow.md) did the same comparison for two hours against ten
PDSes.

## Build speed

Measured with `scripts/buildtime.sh`, median of 3 (2 for the big-crate rows), on a laptop with
other builds running (load average 25-85, so treat these as upper bounds). The relay crate was
still a stub then, so its own rows are all link and cargo overhead. The row that matters is the
vlpds-sized one, which is about the size the relay has grown to since.

| Step | Before | After |
|---|---|---|
| Cold dev build (all of vlpds and its deps) | 9m24s | same |
| vlrelay edit, `cargo check` | 0.28 s | 0.28 s |
| vlrelay edit, build the relay | 0.40 s | 0.43 s |
| vlrelay edit, build the lib tests | 0.31 s | 0.30 s |
| vlpds-sized crate edit (80k lines), rebuild and link a 65 MB binary | 45 s (opt-level 2), 42 s (opt-level 1) | 4.2 s (opt-level 0, line tables) |
| Relink only, 65 MB binary, Apple ld (ld-prime) | 0.9-1.4 s | |

An edit at opt-level 1 or 2 spends ~40 s optimizing the crate again. At opt-level 0 with line
tables only it takes ~4 s (8 s with full debug info). So `Cargo.toml` builds the vlrelay crate at
opt-level 0 in dev and test, with line tables in dev, and dependencies stay at opt-level 2. That
changes only this crate's settings, so switching rebuilds nothing else. For real performance runs,
use `--profile dev-release` or `--release`.

The linker didn't help. Apple's ld (ld-prime, Xcode 16) links the 65 MB vlpds binary in ~1 s, so
there's little left for lld or mold to win. The toolchain's `rust-lld` through its
`gcc-ld/ld64.lld` wrapper aborts when clang calls it with `-fuse-ld` (tried through a linker
wrapper script, since removed). Switching the linker also invalidates every build script and proc
macro, which cost a 3.5 min rebuild each way.

The bench numbers elsewhere in these docs come from a 32-thread Linux box, built with
`--profile dev-release` in a target dir kept between runs, under a memory cap. A cold dev-release
build there takes 2m12s, and an edit to one vlrelay module ~7 s.
