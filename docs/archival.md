# vlRelay: archival mode

An archiving relay keeps a full copy of every repo it mirrors and serves `getRepo`, `getRecord` and `getBlocks` from it, the way the original relays did. It's the same binary. Archiving is a policy setting, per account, and it's off by default (PLAN.md decision 6).

The code is `src/archive.rs` and `src/archive/`. It hooks into the state step in three places: `StateStore::apply_with_frame` (the DID owner's apply), `ShardState::commit` (rows written when the log entry is durable) and `StateStore::recover` (replay).

## Turning it on

All of it lives in the policy object (docs/policy.md), so a PUT of `policy/full` switches it cluster-wide within 10 s.

| Field | Default | What |
|---|---|---|
| `archive.mode` | `off` | `off`, `all`, `tiers` (accounts on hosts of `archive.tiers`) or `hosts` (accounts on `archive.hosts`) |
| `archive.takedownRetentionHours` | 72 | how long a taken-down account's mirror is kept before it's deleted |
| `tiers.<tier>.archivalFetchesPerHost` | trusted 5, default and new 1, throttled 0.1 | bootstrap fetches per second from one host |
| `cluster.archivalFetchConcurrency` | 32 | fetches in flight across the cluster, split over live nodes (at least 1 each) |
| `cluster.archivalFetchBytesPerSec` | 50 MiB/s | fetched bytes per second across the cluster, split the same way |

An account is mirrored when the policy wants its host. The host is the one its events come from, which the DID owner has already checked against the DID document.

## What's stored

The mirror sits in the DID shard's SlateDB, next to the sync state, in vlpds's layout as it is (`vlpds::state`):

| Key | What |
|---|---|
| `R/{did}\0{gen}{path}` | the record: CID, the rev that wrote it, its bytes. The source of truth. |
| `c/{did}\0{gen}{cid8}{path}` | the record CID index, for `getBlocks` |
| `M/{did}\0{gen}{cid}` | interior MST nodes (height 1 and up). Leaves are rebuilt from records. |
| `h/{did}` | the head: commit CID, data CID, rev, the signed commit block |
| `V/{did}` | the relay's own: the live generation, one being staged, ones left to delete, and when a takedown was first seen |

Blob refs (`b/`) and backlinks (`bl/`) aren't kept. The relay serves neither.

Every row but the head and `V/` is under a generation. A bootstrap writes a whole repo under a new one, where no reader looks, then switches `V/` and `h/` to it in one batch. The old generation's rows are deleted in the background.

## Keeping the mirror current

The DID owner applies a #commit to the mirror after `check_chain` passes, in the same apply-before-ack, commit-after-durable flow as the sync state:

1. `mirror::apply_live` opens the repo's stored tree at its head (vlpds's `LazyTree`, loading only the paths the ops touch). It takes the rows vlpds's own replay derives from a #commit frame (`segment::derive_commit_muts`: records, CID index, head), applies the ops to the tree, and adds the `M/` puts and deletes the tree reports. So this is vlpds's apply code, not a second one.
2. The new root must equal the commit's `data`. That checks the commit against the relay's own copy of the whole tree, not just the partial tree in the CAR.
3. The rows ride the event's state ticket. `ShardState::commit` writes them in the same batch as the sync record, once the log segment holding the event is durable. A memtable flush can't persist mirror rows for an event the log might still lose.
4. Until then the tree stays in memory (`ShardMirror`), so the account's next commit builds on it instead of on the DB, which doesn't have the rows yet. It's dropped once nothing is outstanding.

A #sync for a mirrored account that restates the same tree writes a new head with its ticket. Anything else queues a fresh copy.

When the stored tree disagrees with a commit that passed the sync 1.1 checks, one side is wrong and the relay can't tell which without the PDS. It keeps emitting the event (the proof passed) and queues a re-fetch, and the fetched repo replaces the mirror. `vlrelay` counts these as `mismatches` on `GET /admin/api/archive`.

### Replay

Log entries already carry the full upstream frame, so nothing new goes into them. A shard's new owner replays the deltas into the sync state as before, then the same entries' #commit and #sync frames into the mirrors (`ReplaySource::frames`). A frame at or behind the mirror's rev is skipped. One whose prevData isn't the mirror's head queues a re-fetch.

## Bootstrap

A repo needs a full copy when:

- its account is new to an archiving relay (the first commit finds no mirror),
- archiving is switched on for it (the sweeper's rescan after a policy change queues every active account it should mirror),
- its chain breaks: a `prev_data_mismatch`, a commit from a desynchronized account, a #sync the mirror can't link, or a stored-tree mismatch.

The DID owner queues it (`archive::fetch::Queue`). A fetch resolves the account's PDS endpoint and key from the DID document cache, keeps only the endpoint's `scheme://host[:port]` (a path or query in the document doesn't reach the request), reads `getLatestCommit`, then `getRepo`, and checks the CAR with vlpds's import parse (every block hashed, the complete canonical tree rebuilt and matched to the commit's `data`) and then the commit's DID, version and signature against the key. Its rev must be at or past `getLatestCommit`'s. The records and nodes become rows with vlpds's `import_rows`, the same function `importRepo` stages with.

Live commits for the account keep being checked, sequenced and emitted while it's queued. Their frames wait in the queue entry (up to 1,024 frames or 16 MiB). The import applies the ones past the fetched rev under the DID's lock, after the account's earlier commits are written, and then goes live. A frame that doesn't chain from the fetched head, or a buffer that overflowed, queues another fetch.

A desynchronized account also takes the fetched head as its chain and loses its desync mark. The head is signed by the account's key and is at least what the PDS's `getLatestCommit` says, which is the same evidence a #sync carries. So an archiving relay heals a broken chain without waiting for the PDS to emit #sync.

The DID document is anyone's to write, so fetches go through vlpds's guarded client: https only, no redirects followed, and only hostnames that resolve to public addresses. `--dev-mode` allows plain http to local PDSes.

Politeness: each host has a token bucket at its tier's `archivalFetchesPerHost` and at most 2 fetches in flight, and hosts take turns. `getLatestCommit` is read up to 64 KiB. A read idle for 30 s fails the fetch, and so does a `getRepo` under 32 KiB/s after its first 30 s, so a PDS that trickles can't hold the node's fetch slots. The node's share of the cluster's concurrency and bytes budgets caps the rest. Failures retry 5 times with backoff (4 s, 8 s, ... 64 s). The last 32 failures show on `GET /admin/api/archive`.

## Deletes and takedowns

The sweeper (`archive::sweep`, every 10 s per open shard) walks the `V/` rows:

- a mirror the policy no longer wants (archiving off for its host, or the account deleted upstream) moves to the garbage, then its rows are deleted in batches;
- a taken-down account (an operator takedown, or the PDS's `#account` status) stops serving at once, because the read endpoints check the sync record first. The sweeper notes when it first saw the takedown, and deletes the mirror `takedownRetentionHours` later. An untakedown before then keeps it;
- a staged generation whose fetch is gone (a crash mid-import) goes to the garbage.

After a policy change (and at startup), it also walks the shard's sync records and queues every active account that should be mirrored and isn't.

## Endpoints

| Endpoint | Served |
|---|---|
| `com.atproto.sync.getRepo` | streamed from one SlateDB snapshot by vlpds's export (`xrpc::stream_export`), in the streamable CAR order. `since` sends only records written after that rev, with the whole current tree, as vlpds does. 32 at once. |
| `com.atproto.sync.getRecord` | the commit, the proof path and the record |
| `com.atproto.sync.getBlocks` | commit, `M/` nodes and records by CID; leaves found with one walk of the tree |
| `com.atproto.sync.listBlobs` | 501 `MethodNotImplemented`: blobs stay on the PDS |

Errors follow the PDS's `assertRepoAvailability`: `RepoTakendown`, `RepoSuspended`, `RepoDeactivated`, and `RepoNotFound` for a deleted account, an unknown one, or one that isn't mirrored. Desynchronized and throttled accounts are served.

Only the DID owner answers (PLAN.md decision 7). A core node that doesn't hold the shard forwards the request over the peer mTLS listener (`/internal/relay/v1/archive/...`) to the owner and streams its answer back. Edges and replicas don't serve archival reads (decision 9).

Operator routes, behind the admin token: `GET /admin/api/archive` (counters, queue, per-shard trees and SST bytes, recent errors), `POST /admin/api/archive/fetch?did=` (queue a fetch) and `POST /admin/api/archive/desync?did=` (break an account's stored chain on purpose, for the e2e).

## Numbers

Measured on the Mac (M-series, 14 cores, other agents' builds running, so treat these as upper bounds).

### Bench

`cargo test --profile dev-release --lib bench_archival -- --ignored --nocapture` (in `src/archive/tests.rs`): 200 synthetic repos of 300 records each, bootstrapped from their CARs, then 4,000 commits of one op each on top, all on an in-memory bucket.

| What | Result |
|---|---|
| Bootstrap (check the CAR, stage, switch), 8 at a time | 1,293 repos/s, 97 MB/s of CAR, 388k records/s, 1.1 ms CPU per repo |
| Bucket bytes per record, after a flush | 87 B per record, for record blocks of 125 B |
| Apply, archival off | 4.0 µs CPU per commit |
| Apply, archival on, tree reopened from the DB every commit | 133 µs CPU, 577 µs wall |
| Apply, archival on, tree kept from the account's last commit | 69 µs CPU, 276 µs wall |
| getRepo, one at a time, streamed from storage | 1,096 repos/s, 88 MB/s, 0.48 ms CPU per repo |

The bytes per record come out below the record itself because the synthetic records are near-identical text and SSTs are compressed. Real posts compress far less, so read it as "records, plus about 1 key's worth of index and node overhead". vlpds's own measurement of this layout is the better guide for real data.

Archival adds ~65-130 µs of CPU per commit. At Bluesky's ~330 commits/s that's 2-4% of one core per node. Most of it is the tree walk on the blocking pool and vlpds's frame derive, which decodes the frame a second time. The wall time is mostly the hop to the blocking pool and SlateDB reads on cold paths. Two fixes during the bench: the first version reopened every tree after its commit was written (now an LRU of 4,096 idle trees per shard, top 3 levels kept), and flushed the memtable after every import, which left one L0 SST per repo for every later read to check (27x slower bootstraps, 2x slower cold applies).

### Over HTTP, from fakepds

A dev-release relay with `archive: all` against a 4-host fakepds fleet (1,000 accounts per host, 100 initial records each, 1,000 events/s), fetch limits raised to 200/s per host and 32 in flight. Every account is bootstrapped when its first commit arrives.

| What | Result |
|---|---|
| Bootstraps | 4,003 repos, 401k records, 227 MB, 0 failures, 0 retries |
| First 5 s | 2,832 repos, ~570 repos/s, ~32 MB/s (after that the queue waits on accounts' first commits) |
| Live commits applied meanwhile | 62,915, 0 mismatches, 144 buffered during their repo's fetch, 3,290 skipped (fakepds's heads run ahead of its stream, so the fetched repo already had them) |
| Apply stage, wall per event | 2.3 ms in the lane, which includes the identity step and the queueing behind other lanes |

### e2e

`just e2e-archival --duration 90 --rate 100 --accounts 60` on the dev network (two vlpds and the reference PDS, each with 20 accounts):

| Check | Result |
|---|---|
| Archive switched off -> all at 30 s | 60 bootstraps within 4 s of the switch |
| Forced desync of one account | re-fetched and healed; its one commit with the broken prevData is the only event missing from the firehose |
| getRepo vs the vlpds upstreams | 40 of 40 byte-identical |
| getRepo vs the reference PDS | 20 of 20 with the same root and the same block set |
| Firehose (e2e_check) | 9,155 of 9,156 commits, every #sync, #identity and #account, p50 28 ms |
| Mirror | 6,906 commits applied, 0 mismatches, 0 stale; getRepo served in 1.1 ms each |

The reference PDS writes its CAR's blocks in its own order (by the rev that wrote each block, then by CID), which a mirror that rebuilds leaves can't reproduce, so byte identity with it isn't possible. Its root and blocks match. vlpds's export order is a pure function of the tree, so the mirror matches vlpds byte for byte.

## Gaps

- No bootstrap is durable until the shard's next checkpoint (5 s): the import doesn't flush on its own. A crash before then loses it, and the account's next commit queues it again.
- A repo over 256 MiB isn't mirrored: the import holds the CAR in memory (vlpds's streamed import is tied to its own worker). The fetch fails and gives up after 5 tries.
- `getRepo` with `since` sends the whole current tree with only the newer records, like vlpds. The reference sends only the blocks written since.
- The fetch queue is in memory. A restart or a shard move drops it. The sweeper's rescan after startup queues every account that should be mirrored and isn't, so nothing is lost, but the order starts over.
- Healing a desynchronized account from a fetched repo doesn't emit a #sync. Consumers see the account's next commit chain from a head they never saw.
- The stored-tree check catches a commit whose ops don't produce its `data` from the mirror's tree. It can't catch lost record rows whose leaf nodes are still in the node cache (nodes are content-addressed). Those show up when the leaf is rebuilt: on export, or after the cache drops it.
- The sweeper walks every `V/` row of a shard every 10 s, and every sync record after a policy change. Fine at dev scale; at 56M accounts it wants a cursor and pacing.
- Takedown retention counts from when the sweeper first saw the takedown, not from the takedown itself (the sync record doesn't keep a time).
- Archival reads are forwarded to the owner on core nodes only. An edge or a replica answers `ShardUnavailable` (PLAN.md decision 9).
- Apply cost is per commit on the blocking pool. At the 100k events/s target with everything archived it'd be ~7-13 cores of CPU per cluster for the mirror alone. Batching a DID's commits and keeping hot trees warmer would cut it; nobody needs archival at that rate yet.
- Prometheus doesn't have the archive's counters yet: they're on `GET /admin/api/archive`.
