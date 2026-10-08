---
title: Policy internals
section: Reference
order: 305
summary: "How the policy engine is wired into a node: who owns a host's tier, what each part of the pipeline feeds the engine and enforces, and how the PLC export seeds DID documents."
---

```hero
diagram:
  caption: One node's wiring. PolicyHooks hands the engine's decisions to the upstream manager (limits, connect) and the leader's account gate, and passes their signals back. The engine reloads the policy from the bucket, and every serving node polls the takedown list for its replay filter.
  nodes:
    - { id: engine, label: Policy engine, sub: "`policy::Engine`", at: [0, 4.5], size: [10, 3], tone: accent }
    - { id: bucket, label: "`policy/`", sub: "doc · rules · takedowns", at: [0, 10.2], size: [10, 2.6], shape: store, tone: amber }
    - { id: hooks, label: PolicyHooks, sub: "`src/node/policy.rs`", at: [15, 4.5], size: [10, 3], tone: accent }
    - { id: up, label: Upstream manager, sub: "limits · connect", at: [31, 0], size: [10, 3], tone: accent }
    - { id: gate, label: Account gate, sub: on the leader, at: [31, 4.5], size: [10, 3], tone: violet }
    - { id: serve, label: Serve, sub: "`TakedownSet`", at: [31, 10], size: [10, 3], tone: blue }
  edges:
    - { from: bucket.t, to: engine.b, label: reload, dash: true }
    - "engine.r <-> hooks.l: decisions · signals"
    - "hooks.r20 <-> up.l: for_host"
    - "hooks.r <-> gate.l"
    - { from: bucket.r, to: serve.l, label: poll 10 s, dash: true }
facts:
  - { value: "1", unit: s, label: policy version check, note: "the hooks' cache refreshes on a new version, and every 30 s for the rest", tone: accent }
  - { value: "10", unit: s, label: takedown poll, note: "`policy/takedowns/current/`, a LIST and a GET per changed object", tone: blue }
  - { value: "256", label: lookups started ahead, note: "`--did-lookup-prefetch`, besides the 64 lanes' own", tone: violet }
  - { value: "~88", unit: ns, label: an event to filter takedowns, note: "with a non-empty set; an empty one parses nothing", tone: amber }
```

[Policy](policy.md) says what the engine decides. Inside a node, `PolicyHooks`
(`src/node/policy.rs`) carries those decisions to the parts that enforce them and feeds the engine
what those parts see. This page names the types and the paths in the code, for anyone changing
that wiring.

## Wiring

`main` builds the engine (with `FixedNodes(1)`, so every node holds the cluster-wide budgets in
full) and hands it to the node in `NodeConfig.policy`. `Node::start` installs the hooks before
the upstream manager starts, so no host connects without the policy. Then it starts the engine's
refresher, the driver and the sync loop.

### Who owns a host's tier

The leader's host table owns it (`HostTable` in `src/node/quorum.rs`, in the log and flushed to
`qlog/state`). Each node's `QuorumHosts` is its `HostStore`. A host's tier comes from the table,
and its counters and policy fields are the node's own copy. The policy engine writes the tier
(operator actions through `PolicyAdmin`, the driver's throttles and recoveries), and a changed
tier rides the node's next submit to the leader as a proposed row. Everything else follows it:

- `HostStore::update_host(hostname, f)` is a read-modify-write of the node's record. The driver,
  `PolicyAdmin::host_action`, the counter flush and the upstream registry's flush each change
  only their own fields.
- The upstream registry's flush (`QuorumHosts`'s `upstream::HostStore`) only proposes the tier
  when the table has no row for the host (a new host's admission tier). After that it never
  writes it.
- The hooks keep a per-host cache of `engine.for_host(&record)` (the tier after domain rules,
  `connect`, the limits, the rule and the operator throttle). It's the upstream manager's
  `PolicySource`. It's refreshed when a record is written through the hooks' `HostStore` (which
  names every host it writes to the sync loop), when the policy or the domain rules change
  version (checked every second), and every 30 s for records written elsewhere.
- `Manager::apply_policy(host)` copies the cache onto the host's registry entry. That's its tier,
  its limits (the socket's buckets are retuned in place, keeping what they hold or owe), and a
  disconnect or a connect when `connect` flips. So a domain-rule ban added later disconnects a
  host that's already registered, within about a second.

### Upstream

- `requestCrawl` goes through `engine.admit_host` (`upstream::Admission`): the crawl switch, the
  hostname rules, domain bans, a known host's own tier, allow-list mode, the starting tier and the
  cluster's daily new-host budget. A known host is checked too, so a ban added since holds. A new
  host is checked without spending (`AdmitRequest::dry_run`), then probed (`describeServer` and a
  `subscribeRepos` socket), and only then admitted for real. That spends the daily budget and
  checks the bans again, so names that don't answer can't use the budget up. A hostname whose
  probe failed is refused for 10 minutes without another probe, and each client IP gets 10 calls
  a minute (429 past it). On a dev network the engine's indigo hostname rules refuse IPs and
  ports, so those hosts are admitted the way an operator would add them.
- `for_host` limits are events/s, bytes/s, and events per hour and per day (each a token bucket
  as deep as its window), plus reconnects per hour (a dial bucket). `--host` upstreams start at
  `--host-tier` (default `trusted`) the first time they're seen.
- The node's reject hook turns every verification failure into a `FailedValidation` signal (with
  the reason as detail) and oversized frames into `OversizedCommit`. Follow-ons and the relay's
  own trouble (`stale`, `desynchronized`, `inactive`, `rate_limited`, `identity_unavailable`,
  `store`, `not_owner`) aren't signals. Rejects from the host stage (signatures, malformed frames,
  oversized commits) also count toward the host's `failed_checks`, so the error budget sees them.
  The state step counts its own.
- Accepted `#identity` events are `IdentityChange`. Each host's `#identity` events spend its
  tier's `identityEventsPerHour` (0 is unlimited) at the host stage. Past it they're dropped as
  `identity_rate`, which is an `IdentityChange` signal too, so `hostIdentityChurn` sees the flood.

### State and the node

- Accounts go through `state::AccountGate` on the leader, when it checks the event against the
  account's record (`StateStore::apply_held` in `src/node/quorum.rs`). The gate tells a
  **newly created** account from one that's only first seen. A relay that starts cold sees all
  ~56M established accounts for the first time, and none of them is new.
  - Newly created means the event is the repo's first commit. That's a `#commit` with no `since`,
    and no `prevData` or the empty tree's (`verify::Verified::created`, carried to the leader in
    the submit's meta flags). A PDS usually announces a new account with `#identity` and
    `#account` first, so the gate is asked again at a known account's first commit
    (`Arrival::FirstCommit`), as well as when an unknown DID shows up (`FirstSeen` or `Created`).
    Only the account's own PDS creates its record. Another host's `#identity` for an unknown DID
    is emitted without one, so it can't credit an account to a PDS's cap or let a later commit
    skip the cap as `FirstCommit`.
  - Every account counts toward its host's cap (`maxAccounts`, indigo's per-host account limit).
    Past it the account is created throttled (`relay_throttled`, status `throttled`). Its commits
    are dropped, an upstream `#account` doesn't lift it, and an operator's untakedown does. The
    cap counts the record's `account_count` plus the accounts admitted since the last sync, so it
    can lag by one counter flush (5 s) but never overcounts. Bluesky's PDSes
    (`*.host.bsky.network`) are trusted, at 10M each, well above the ~0.5-1M accounts one of them
    holds. An untrusted PDS is held to 1,000 by default (indigo uses 100). Raise well-known PDSes
    per host when deploying.
  - Only newly created accounts spend the host's `newAccountsPerHour` and the cluster's
    `NewAccountsPerMin` budget, and only they are `NewAccount` signals. The signal is recorded
    once per creation, deferred or not, so a farm trips `hostNewAccounts` at the rate it creates
    accounts. Past a rate the event is dropped (`new_account_deferred`) and the DID is remembered
    for an hour (100k at most). So its later events, which no longer look like a creation, wait
    for the budget too.
  - First-seen accounts aren't deferred. Each costs a DID lookup, which the PLC budget paces
    (below).
  - Creation can be faked. A farm can send a first commit with a made-up `since` and `prevData`,
    and the relay holds no earlier state to check them against. The host's account cap still
    applies (1,000 for an untrusted host by default). Checking the DID's PLC creation op would
    catch it, but that's an audit-log fetch per unknown DID (56M on a cold start), so it isn't
    done.
- Accepted commits are `Record` signals (count 1).
- DID document fetches spend the cluster's `PlcLookupsPerSec` share
  (`IdentityCache::set_budget_gate`), waiting up to the cache's `max_budget_wait` for it. The host
  stage and the leader look up through `IdentityCache::lookup_paced`, which waits again whenever
  the budget is spent. The event holds its lane, so the backpressure reaches the host's socket,
  and nothing is dropped for want of budget. (Three spent-budget waits used to drop the event as
  `identity_unavailable` and ack it upstream.) A failed fetch still gives up after three tries.
- A lane works one event at a time, so the lanes alone wait on at most `--lanes` (64) lookups at
  once, and each lookup is a seed read, a state read and the fetch. On a cold cache in production
  that came to ~0.5 s a miss (the reads are bucket GETs at ~100 ms). So lookups topped out near
  64 / 0.5 s whatever `--did-lookups-per-sec` allowed (72/s at a budget of 100, with ~5k events
  queued). Two changes take the latency out of that bound. The dispatcher starts the lookup of a
  `#commit` or `#sync` DID that isn't cached when it queues the event (`IdentityCache::prefetch`,
  at most `--did-lookup-prefetch` (256) at once, skipped when they're all taken), and the lane's
  own lookup later joins it or finds it cached. The seeder also reads the seed and the account's
  record side by side. The budget still paces every fetch, and a prefetch that finds it spent
  leaves the lookup to the lane. `vlrelay_identity_lookups{outcome="prefetched"|"prefetch_full"}`
  counts them.
- Fresh fetches an event asks for spend its host's own budget first (`AccountGate::forced_lookup`,
  `PolicyHooks::take_forced_lookup`). That's the tier's `identityEventsPerHour`, at most a
  minute's worth (and 10) at once. These are an `#identity`'s refresh, the re-resolve on a host
  mismatch and the refresh after a signature fails against the cached key. With that budget
  spent, the event is checked against the cached document, and
  `vlrelay_forced_lookups_refused_total` counts it. Any host can send `#identity` for any DID, so
  without this one host's stream would drain the shared PLC budget and stall every other host's
  lookups behind it. Besides that:
  - an `#identity` from a host that isn't the account's PDS doesn't refresh a document fetched in
    the last 30 s (`reresolve_after_secs`, the same window a host mismatch gets)
  - the identity cache answers a forced refresh with the DID's last one if that's under 30 s old
    (`identity::Options::min_refresh`)
  - the host stage doesn't drop the DID from its cache on `#identity` (that made the next lookup a
    fetch), and the leader's refresh updates its cache.
- A takedown or its reversal is written to `policy/takedowns/audit/` (one object per action,
  If-None-Match) and `policy/takedowns/current/{sha256(did)}.json` (who, when, why, for the
  account page) before the account changes, and logged on `vlrelay::audit`.
- A takedown also filters the replay window. A consumer whose cursor reaches back past it doesn't
  get the account's earlier `#commit` and `#sync` frames, whether they come from the ring or from
  segment backfill, on any node. Its `#account` and `#identity` frames pass, so consumers see the
  status change. Frames are skipped, not renumbered, so every node keeps the same seqs and
  consumers see a gap, which the spec allows. Lifting the takedown lets the old frames replay
  again. indigo doesn't filter replay at all ([Reference notes](reference-notes.md#account-status)),
  so after a reversal both relays serve the same thing.
  - Every serving node keeps a `TakedownSet` (`policy::takedowns`), the DIDs whose current object
    says `takedown: true`. `Serve` polls `policy/takedowns/current/` every `REFRESH_EVERY` (10 s)
    with a LIST, then a GET for each object whose ETag changed, 16 at a time. It also reads the
    list once before the node serves (`Serve::load_takedowns`), so every node agrees within one
    poll.
  - The node whose admin API takes an account down applies the change to its own set
    (`TakedownSet::apply_local`) after the current object is written. Then it asks the leader for
    the takedown entry (`Glue::takedown`, which sets the record's flag and carries the `#account`
    in one entry). So its consumers never see the `#account` and then the account's old commits.
    A poll that listed the objects before that local apply can't undo it, and the object is
    fetched again on the next poll.
  - The set is a `FrameFilter` on the firehose (`Firehose::set_filter`, vlsync's
    `vlsync-firehose`, shared with vlpds). The firehose reads each frame's type and DID straight
    from the CBOR (`frame_meta`: `repo` for a `#commit`, `did` for
    the rest, the same keys `event::route` uses). A ring batch computes its verdicts once per set
    generation, and every subscriber shares them. Backfill checks each frame it reads. An empty
    set does no parsing at all. With a non-empty set, a 2,000-event batch costs 88 ns an event
    once (about 1% of a core at 100k/s), and a batch with nothing to skip is written as one slice,
    the same as with no filter.
  - Until a node's first poll lands (the bucket is down at startup, say), it serves unfiltered and
    logs a warning. Other nodes lag that node by up to one poll.

### Operator API

`node::admin::NodeAdmin` forwards policy, domain rules, cases and every host action but
`reconnect` to `PolicyAdmin`, then applies the host's new policy to its socket right away.
`GET/PUT policy/full`, `GET domain-rules/audit` and `GET cases/{id}/evidence` carry the whole
document, the rules' audit log and a case's evidence ([Admin API](admin-api.md)).

## PLC export seeding

With `--plc-export`, the quorum log's leader reads the PLC directory's `/export` as a background
job (`plc_seed::job::PlcJob` in `src/plc_seed`), started when the node leads and stopped when its
term ends. The history is split into `--plc-export-streams` (4) windows read side by side, paced
together at `--plc-export-rate` (2 requests a second). A 429 or a 5xx waits out its `Retry-After`
(with backoff) and doesn't count against the pace. The last window becomes the tail once it
catches up (`ingest::Ingester`).

Each DID's latest op (key, PDS host, http or https, tombstone, `createdAt`) goes into its own
SlateDB at `{prefix}/plc/seeds`, and the windows' cursors go into `plc/export-checkpoint.json`,
written only after the rows before them are flushed. Newest-wins is the database's merge operator
(`plc_seed::NewestWins`: the row with the later `createdAt` wins, and the encodings break a tie),
so a page's rows are written as merges with no read first. Each read used to be a bucket GET
whenever its block wasn't cached (~100 ms on R2), one after another on the ingest's coordinator.
With each page's thousand rows behind it, the readers waited on their page channel and a request
took ~5 s, ~180 ops/s at any `--plc-export-rate`. The writer, the members' readers and the
compactor all run the operator.

A new leader opens the database as its writer, which fences the old one (its next flush fails, so
it never writes a checkpoint past the new one's), and resumes from the checkpoint. The seeds
aren't in `qlog/state`. They're a cache and not the log's state, so flushes, checkpoints, `verify`
and bucket recovery never carry them, and losing the last seconds of them at a takeover costs only
lookups.

The database runs inside `--plc-seeds-slatedb`'s bounds (one compaction at a time, no
subcompactions, 2 x 1 MiB of read-ahead per input SST, 64 MiB output SSTs, 128 MiB of memtables).
With SlateDB's defaults its compactions alone took a 4 GB node past its memory limit. The bounds
cost a fill nothing at the export's pace, and writing as fast as the database takes rows they cost
~14% of the rate (`src/bin/seeds_bench.rs`).

Every member reads the database (`plc_seed::SeedReader`, a SlateDB reader following the latest
manifest every 30 s, or the leader's own writer) as its identity cache's seeder. A miss takes the
seed (weighed against the account's record by `plc_seed::choose` where the node holds it) without
spending the PLC budget, and a forced refresh still asks PLC. When the export writes an op created
after a DID's cached document was fetched and the two differ, the leader drops its cached copy
(`plc_seed::invalidate_if_stale`). The other members' copies age out with the cache's TTL, and an
`#identity` refreshes them sooner. The ingest logs its time per phase every minute (`phases_ms`:
pace, fetch, parse, handoff, apply, checkpoint). Requests are counted as `qlog_plc`
(`GET /admin/api/store`).

### Local seed tables

An LSM is the wrong shape for the read side. Every seed row is a merge operand, so a get probes
every sorted run's filter, and the filters and indexes (~2 B a row, ~190 MiB at 99M ops) have to
sit in memory or each lookup fetches them. At 99M ops and 14 sorted runs a relay with a 64 MiB
metadata cache did ~35 lookups/s at 32 at once, p50 0.9 s, ~5 bucket GETs each
(`seeds_bench`, R2's latency injected). That holds at 5x only with ~1 GB of filters in memory.

With `--plc-seeds-dir`, each member keeps its own copy as a table on local disk
(`plc_seed::table`): a fixed 49-byte record per DID in 4 KiB pages of an on-disk hash, 1,024
shard files. A did:plc id is 120 bits of a sha256, so the id is the hash. Its first 10 bits pick
the shard, the next 56 are the record's tag, and the tag's top 32 bits pick the page, so
placement follows the database's key order. A lookup is one 8 KiB `pread` (the home page and the
one it spills into), with nothing per DID in memory: a page cache helps and nothing needs it. A
record keeps the op's `createdAt` (ms), the flags (tombstone, http, fetched rather than
exported), the key's curve and parity and its 32 bytes, and a 3-byte number for the PDS host
(~21k hosts in all of PLC's history). A key that isn't a compressed k256 or P-256 multikey is
left out, and its DID resolves from PLC as it would anyway. did:web rows (lookups only) are
placed by a sha256 of the DID.

Two DIDs that share 66 bits share a record, and the second's lookup finds the first's key, fails
the signature and refreshes from PLC, the path a stale seed takes. At 457M DIDs a collision
anywhere is about a 1% event. A page that fills spills into the next, at most 8 pages on, and a
row with no slot there is dropped (`dropped`): DIDs ground to one page (a few million sha256s
each, and PLC rate-limits creates) cost only the pages they land on. A shard grows by 1.15x when
it's 95% full and rewrites only itself, so a table's extra disk is 1/1,024 of it at a time. A
page carries a CRC, and one that fails reads as empty.

The bucket's database stays the shared, durable copy. Each row the leader writes is also kept for
3 days under `c` + the time it was written. A member builds its table from one scan of the
database (its own reader, read-ahead 2 MiB x 2 per sorted run), then reads the changelog every
10 s from 10 minutes before its cursor, which covers the leader's flush and the readers' manifest
poll (rows read twice are harmless), and syncs the table before it moves the cursor
(`meta.json`). A member whose cursor is older than the changelog, or whose table came from
another database, rebuilds. The leader also writes what it applies straight into its own table.
Until a member's table is built, its lookups read the database as before. The rows a member
reads off the changelog also drop its identity cache's stale documents, which only the leader's
did before.

At 99M rows a build took 229 s on 2 cores against R2's latency (3,365 GETs, peak RSS 187 MB) and
wrote 5.5 GB (58 B a DID). Lookups through the seeder then ran at ~174k/s, p50 175 µs and p99
0.55 ms, with no bucket requests.

### Fetched documents

The seeds also keep what the cache fetches, so they hold the export plus every document the relay
has looked up, and a restart loses only the in-memory cache. On the leader, every fetch that finds
a document, or finds that the DID doesn't exist, did:plc or did:web, becomes a row
(`Seeder::learned`, flagged `lookup`). Its `createdAt` is when the fetch started, less a minute
(`LOOKUP_SKEW`). PLC stamps each op's `createdAt` itself, so a document fetched at T already has
every op from before T. An export op created after the stamp replaces the row and an older one
doesn't, through the same merge. The comparison is on time because a fetched document doesn't
carry the CID of the op that made it. The minute covers the relay's clock running ahead of PLC's.
An op inside it replaces the fetched row, which changes nothing when the document already had it.
An `#identity` refreshes the document on the leader, and that fetch writes a newer row. A fetched
did:web document has no export to replace it, so the seeder uses it for
`--did-web-seed-ttl-secs` (a day) and then fetches it again.

The hot path never writes. A lookup appends to a buffer, and the leader's writer task
(`SeedReader::write_learned`) writes it as one batch per 1,024 documents or per second, into the
memtable. The export's checkpoints flush it every 10 s with the export's own rows, so persisting
lookups adds no PUTs to the bucket. At 100 lookups/s that's a batch a second and ~60 KB more per
checkpoint's L0. A buffer that can't be written (a fenced writer) is dropped, since the documents
are only a cache. Only the leader writes. Other members keep their lookups in memory, because the
leader resolves every account it applies, so their DIDs reach the seeds through it anyway.
Forwarding would add a hop to a lookup's path for documents the leader is about to fetch. Fetched
documents are kept only with `--plc-export`, since the seeds database exists only then.

Measured against plc.directory from a home server (2026-10, 12 requests over one HTTP/2
connection, at least 2 s apart), a full 1,000-op page takes 0.19-0.56 s (0.07-0.25 s to the
first byte) and weighs 0.5-1.6 MB, 0.5-1.6 KB an op. A DID document takes 64 ms. The directory
sends no rate-limit headers. Its API spec only says "reasonable rate-limits are applied", and the
reader honors 429 and `Retry-After`. Ops a day ran from ~20k (2023-09) through 60-120k
(2024-2025) to ~180k (2026-10), roughly 100M in all, so a first fill reads ~100 GB. That's ~14 h
at 2 requests/s and ~28 h at 1. Earlier measurements put a seeded DID at ~100 bytes raw for real
hosts (~3 GB in SSTs for 56M). `tests/qlog/relay-chaos.sh` with `PLC_EXPORT=1` runs the export
against fakepds's `/export` (with 429s) through the scenario's faults.
