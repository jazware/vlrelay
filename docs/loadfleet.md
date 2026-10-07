# vlRelay: the load fleet

A real vlpds tops out at a few thousand writes a second on a laptop, and the relay's target is 100k events/s (`docs/design.html`). So the bench upstream is `fakepds`, a synthetic PDS fleet. One process plays many hosts, and four processes on benchbox emit ~170k signed sync 1.1 events/s with about 7 cores.

```
scripts/fleet.sh build        # cargo build --profile dev-release --bin fakepds
scripts/fleet.sh start        # 4 processes x 10 hosts, 40k events/s each, prints the relay's flags
scripts/fleet.sh consume --verify --duration 30
scripts/fleet.sh stop         # stop them all and delete the fleet dir
```

`WHERE=benchbox` runs any of these on benchbox in `~/vlrelay-loadfleet` (the build syncs the tree there first, the same way `scripts/benchbox.sh` does). `WHERE=benchbox scripts/fleet.sh clean` stops the fleet and deletes that directory.

## What a host looks like

Each host listens on its own port (`PORT_BASE + g`, where `g` is the host's global index), so the relay sees every one as a distinct host, `127.0.0.1:30007` say. A host serves:

| Endpoint | What it does |
|---|---|
| `com.atproto.sync.subscribeRepos` | The stream, with cursors. A cursor in the replay ring replays from there, an older one gets `#info OutdatedCursor` first, and a future one gets a `FutureCursor` error and a close. A subscriber more than `--lag-secs` (1) behind gets `ConsumerTooSlow` and a close. |
| `com.atproto.server.describeServer` | `did:web:h<g>.fakepds.test` and the host's handle domain |
| `com.atproto.sync.listRepos` | every account that has a commit, with its head and rev |
| `com.atproto.sync.getRepoStatus`, `getLatestCommit` | the account's head (always `active`) |
| `com.atproto.sync.getRepo` | the whole repo as a CAR (`application/vnd.ipld.car`): the commit as the root, then every MST node and record in vlpds's streamable order. `since` is ignored, so it's always the full repo. |
| `/xrpc/_health` | 200 |

Anything else gets 501 `MethodNotImplemented`.

The head these serve is the repo's, not the stream's. It moves when a generator applies a commit, and that's up to `--pregen-secs` ahead of emission. A commit swallowed by a `gap` fault moves it too. So a relay that bootstraps from getRepo can get a rev it hasn't seen on the stream yet, the same as with a real PDS. It should drop stream commits at or below that rev, and the next one chains from it. getRepo and getLatestCommit read the same per-account snapshot, so their commit and rev agree at any moment.

getRepo rebuilds the record bytes on each request instead of storing them (see Records). In the selftest an 80-record repo (50 small initial records, the rest ~5 KB commit records) is about 165 KB and takes 0.2-0.4 ms of one core to build on the Mac. It's built on the host's tokio threads, so a bootstrap storm costs the fleet process CPU.

Every account has a real secp256k1 key and a real MST built with vlpds's `mst::Tree`. A commit is built the way vlpds's own repo worker builds one. The ops go into the tree, `write_diff_blocks` gives the new nodes and the proof nodes, the commit is signed with `crypto::Keypair`, and the CAR carries the signed commit, those nodes and the new records. The frame comes from `events::commit_frame` with `since` and `prevData` set. So a commit passes the full sync 1.1 checks: CAR root, block hashes, signature, rev order, `since`/`prevData` chaining and MST inversion.

## DIDs and the fake PLC

The DIDs are `did:plc` and the fleet ships its own PLC directory. Any process started with `--plc-port` serves `GET /<did>` with the account's document, and `fleet.sh start` gives process 0 `PORT_BASE - 1`. The relay then just takes `--plc-url http://127.0.0.1:29999`.

One PLC is enough for a fleet of independent processes because the DID carries its own address. Its 24 base32 characters are 15 bytes: a tag from the seed, the global host index, the account index and a 3-byte MAC. The signing key is `sha256(seed, host, account)`. So any process can compute any account's document (its key and its `atproto_pds` endpoint, `ADVERTISE:PORT_BASE+g`) without knowing what the others generated, and nothing has to be registered. That's why this beat the devloop's real PLC, where registering 100k DIDs one operation at a time would take a while, and did:web, which needs a resolvable hostname per account. Every process in a fleet has to agree on `--seed`, `--advertise` and `--port-base`, and `fleet.sh` passes the same ones to all of them.

`selftest` resolves fleet DIDs through vlpds's own `DidResolver` against the fake PLC and checks the endpoint and the key, so a relay using that resolver with a `--plc-url` override should take them as is.

## The event mix

The defaults match the production measurements in `docs/reference-notes.md` (#commit p50 5.2 KB, p99 9.9 KB, mean ~5.3 KB, ops per commit p50 1):

| What | Default | Flag |
|---|---|---|
| #commit frame size | log-normal, p50 5,200 B, p99 9,600 B, capped at 16 KB | `--size-p50`, `--size-p99`, `--size-max` |
| Big multi-op commits (10-50 ops, 16-200 KB) | 0.05% | `--big-share`, `--big-max` |
| Ops per commit | 92% one, 5% two, 2% 3-5, 1% 6-10 | |
| Op kinds | 85% create, 5% update, 10% delete, until an account has `--target-records` (100), then 45/5/50 | |
| `#identity`, `#account`, `#sync` | 0.2%, 0.2%, 0.1% | `--identity-share`, `--account-share`, `--sync-share` |

A record's text is padded so the frame lands on the sampled size, after subtracting the commit, CAR overhead and a running average of the account's MST bytes. The text is random English-ish words, so it compresses about as well as real posts do. Measured on a 40 s benchbox run, every frame on the wire was p50 5,311 B, p90 7,743 B, p99 10,303 B, mean 5,245 B. That's close enough to production that frame size shouldn't skew a relay bench.

Each account starts with `--initial-records` (50) records, so commits carry real proof paths. They're small, 150-600 bytes. Production repos are much bigger, and their proofs are deeper. Raise it if the relay's MST inversion cost matters to the bench (or the size of a getRepo). Memory goes up by about 260 bytes per record: 263 measured from 50 to 250 initial records over 20,000 accounts, down from 308 before records were rebuilt from state. Building them costs startup time: 20,000 accounts with 250 records each took 4.9 s on 4 threads of a busy Mac, against 3.4 s before.

### Records

Nobody keeps a record's bytes. They're a pure function of 64 bytes of state: a seed, the collection, the rkey, the target size and the `createdAt` second. The seed hashes the account's DID (so the fleet seed, host and account) with the account's write counter. The text comes from the seed, so the same state always gives the same bytes and the same CID. A commit builds its records once for the CAR, and getRepo builds them again from the state. Each account keeps that state for its live records next to its MST.

### Rate and burstiness

`--rate` is events/s for the process, split over its hosts evenly or by a Zipf law with `--skew` (1.0 gives host 0 about a third of a 10-host process). Each host's rate is multiplied by a mean-reverting log-normal factor (`--noise` 0.3, half-life 2 s), and each emitter thread has bursts of 3x for 250 ms about 6 times a minute (`--burst-x`, `--burst-ms`, `--bursts-per-min`). The bursts add ~5% on top of the mean. Emission happens in 5 ms ticks (`--tick-ms`), and a tick's frames go out as one websocket flush.

### Pre-generation

Signing and the MST work cost ~35 µs an event per generator thread (measured on benchbox, ~28k events/s per thread). So generation runs on its own threads (`--gen-threads`), ahead of the stream, into a per-host pool (`--pool-mb`, by default room for `--pregen-secs` (2) of the target rate). The stream starts once the pool holds `--pregen-secs` of events. The emitters then pop from it, stamp `seq` and the current `time` into the pre-built frame and publish. If the generators fall behind, the emitter sends what's there and counts the rest as `starved` in the stats, so a starved fleet shows up as a lower rate instead of a backlog. Commit `time` is set at emission, so latency measured from `time` is honest. The revs are set at generation, up to a couple of seconds earlier.

Each account belongs to one generator thread (account index mod threads), so per-account order needs no lock and holds through the shared per-host queues.

## Faults

`--fault kind:hosts[:k=v,...]`, repeatable, with hosts as global indexes (`all`, `3`, `0,2-4`). `FAULTS="..."` passes them through `fleet.sh`.

| Kind | Params (defaults) | What the host does | What a checker should see |
|---|---|---|---|
| `badsig` | `rate` (0.01) | a fraction of commits get one flipped signature bit. They're built on a copy of the tree, so the account's chain doesn't move. | signature failure, and the next good commit chains from the last good one |
| `gap` | `rate` (0.01), `heal` (3) | a commit is built and applied but never emitted, so the next commit's `since` and `prevData` skip it. After `heal` more commits the account emits `#sync` (0: never). | a broken chain, then `#sync` |
| `foreign` | `rate` (0.01) | commits for accounts of host `g+1`, signed with their real keys | the DID document names another host |
| `spam` | `rate` (100/s), `secs` (10), `every` (60), `delay` (5) | bursts of new accounts, each `#identity`, `#account` and a first commit (no `since`, `prevData` the empty tree) | new-account limits |
| `stall` | `secs` (10), `every` (60) | stops writing to every socket (no frames, no pings) and then delivers the backlog | a quiet host that comes back |
| `disconnect` | `every` (30), `down` (5) | closes every socket and answers new connections with 503 for `down` seconds | reconnects with backoff and cursor resume |
| `replay` | `every` (20), `count` (100) | re-sends its last `count` frames with their old seqs | seq regressions and rev-order rejects |
| `lag` | `secs` (900) | stamps every event's `time` `secs` in the past, a PDS whose stream runs late however fast it's read | the host's read lag at `secs`, and a `read-lag` case once it holds past the relay's threshold |

A local run with one of each on 8 hosts, checked by `consume --verify`, counted 143 signature, 89 chain, 133 foreign and 100 rev-order failures and 100 seq regressions (two replays of 50), and nothing else.

## Checking the frames

`fakepds selftest` generates 15,000 events over 5 hosts with every fault on, checks them in process, then serves them on real ports and checks them again over a websocket from cursor 0, byte for byte. It also resolves DIDs through the fake PLC and checks `listRepos`. Then it fetches getRepo for 23 accounts, one of them right after a `gap` commit that moved the head without a frame, and checks each CAR as a bootstrapping relay would: every block hashes to its CID, the commit is signed by the account's key, the tree rebuilt from the records alone has the commit's `data` as its root, and the commit and rev match getLatestCommit. Another host's DID gets `RepoNotFound`. The checker (`src/fakepds/check.rs`) is built from vlpds's `cbor`, `car`, `mst` and `crypto` modules. It verifies the CAR root and every block hash, the commit's DID, rev, version and signature, the rev order, `since`/`prevData` against the last commit it saw for the account, and the inversion of every op on the CAR's partial tree back to `prevData`. Every clean event passes and every faulty one fails the check it should. `cargo test --bin fakepds` runs a smaller version.

`fakepds consume` subscribes to hosts (`--host URL` repeatable, or `--count N` for the fleet's first N), prints events/s every second and a JSON summary with the frame size percentiles and seq regressions. `--verify` runs the same checker on every frame on `--verify-threads` threads, sharded by DID. Verifying 63k events/s took ~2 cores on the Mac, so checking a whole 160k/s fleet needs ~5 cores more. `e2e_check` is the tool for relay-vs-upstream comparisons.

## Throughput

| Where | Setup | Events/s | CPU | RSS |
|---|---|---|---|---|
| benchbox | 4 processes x 10 hosts x 2,500 accounts, `RATE=40000 GEN=6`, one subscriber per host | ~170k sustained (40 s), 870-960 MB/s | 6.8 cores for the four | ~1.8 GB each |
| benchbox | 1 process, 10 hosts x 5,000 accounts, `--rate 80000 --gen-threads 10` | ~80k | 4.5 cores | ~3 GB |
| Mac (M-series, 14 cores, other builds running) | 1 process, 20 hosts, `--rate 60000 --gen-threads 6`, `consume --verify` on 6 threads | ~63k, 1.26M events verified, 0 failures | ~2 cores | ~3 GB |
| Mac | 1 process, 40 hosts, `--rate 160000 --gen-threads 10` | ~150k for 15 s, then ~100k once the generators slowed under load | | ~4-5 GB |

So a fakepds process costs about 4 cores per 100k events/s with subscribers attached, nearly all of it generation. Four processes on benchbox put out well over the relay's 100k target, and the consumer saw 0 seq regressions. Run the fleet on the same box as the relay only if it has the cores to spare. Otherwise point `ADVERTISE` and `BIND` at an address the relay can reach.

Memory per process is the pool, the replay ring (`--replay-mb`, 256), up to `--lag-secs` of frames still owed to subscribers, and the accounts' trees, which grow to about accounts x `--target-records` x 260 bytes. `fleet.sh` runs each process in a systemd scope with `MemoryMax=$MEM` (6G) on Linux and under `dev/capped.sh` on macOS. Size `MEM` from that sum. A 2G cap with the old 1 GB default pool got a process killed within 20 s.

## fleet.sh

```
WHERE=benchbox PROCS=4 HOSTS=10 DIDS=2500 RATE=40000 GEN=6 MEM=6G DURATION=600 scripts/fleet.sh start
```

`start` refuses if a fleet is already running in `FLEET_DIR`. On benchbox it also refuses while the batch pipeline runs or when its next run is due within `DURATION` + 5 minutes (`FORCE=1` skips that). Every process exits by itself after `DURATION` seconds (600), so a forgotten fleet doesn't sit on benchbox. When all of them are `READY` it prints the relay's flags on one line:

```
--host http://127.0.0.1:30000 ... --host http://127.0.0.1:30039 --plc-url http://127.0.0.1:29999
```

`status` prints each process's last stats line (events/s, generation rate and cost, starved, pool, subscribers, RSS). `stop` stops the processes and deletes `FLEET_DIR` (`$TMPDIR/vlrelay-fleet` here, `~/vlrelay-loadfleet/fleet` on benchbox).

## Gaps

- getRepo ignores `since` and always sends the full repo. Accounts that haven't committed yet (only their `--initial-records`) get `RepoNotFound`.
- `#account` is always `active: true`. There's no deactivate/reactivate cycle, so the relay's inactive-account path isn't exercised.
- secp256k1 keys only. vlpds signs with libsecp256k1 and has no p256 signer.
- Handles (`u<i>.h<g>.fakepds.test`) don't resolve, so a relay that verifies handles will mark them invalid.
- `consume` doesn't reconnect, so it loses a host for good on a `disconnect` fault.
