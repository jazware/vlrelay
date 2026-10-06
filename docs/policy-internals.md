# vlRelay policy: wiring

Internal: the public page is [Policy](policy.md) (`/docs/policy`). This is how the engine is wired into one node: who owns a host's tier, and what each part of the pipeline feeds it and enforces.

## Wiring

`src/node/policy.rs` (`PolicyHooks`) carries the engine's decisions to the parts that enforce them and feeds it what they see. `main` builds the engine (`FixedNodes(1)` on one node, and a cluster passes its own `LiveNodes`) and hands it to the node in `NodeConfig.policy`. `Node::start` installs the hooks before the upstream manager starts, so no host connects against the policy, then starts the engine's refresher, the driver and the sync loop.

### Who owns a host's tier

The state host record. The policy engine writes it (operator actions through `PolicyAdmin`, the driver's throttles and recoveries), and everything else follows it:

- `HostStore::update_host(hostname, f)` is an atomic read-modify-write under the host's lock. The driver, `PolicyAdmin::host_action`, the counter flush and the upstream registry's flush each change only their own fields, so a counter flush can't overwrite a tier change or the other way round.
- The upstream registry's flush (`node::adapters::StateHosts`) only seeds the tier when it creates a record (a new host's admission tier). After that it never writes it.
- The hooks keep a per-host cache of `engine.for_host(&record)` (tier after domain rules, `connect`, limits, rule, operator throttle). It's the upstream manager's `PolicySource`. It's refreshed when a record is written through the hooks' `HostStore` (which names every host it writes to the sync loop), when the policy or the domain rules change version (checked every second), and every 30 s for records written elsewhere.
- `Manager::apply_policy(host)` copies the cache onto the host's registry entry: its tier, its limits (the socket's buckets are retuned in place, keeping what they hold or owe), and a disconnect or a connect when `connect` flips. So a domain-rule ban added later disconnects a host that's already registered, within about a second.

### Upstream

- requestCrawl goes through `engine.admit_host` (`upstream::Admission`): crawl switch, hostname rules, domain bans, a known host's own tier, allow-list mode, the starting tier and the cluster's daily new-host budget. A known host is checked too, so a ban added since holds. A new host is checked without spending (`AdmitRequest::dry_run`), then probed (`describeServer` and a `subscribeRepos` socket), and only then admitted for real: the daily budget is spent and the bans are checked again. So names that don't answer can't use the budget up. A hostname whose probe failed is refused for 10 minutes without another probe, and each client IP gets 10 calls a minute (429 past it). On a dev network the engine's indigo hostname rules refuse IPs and ports, so those hosts are admitted as an operator would add them.
- `for_host` limits: events/s, bytes/s, events per hour and per day (each a token bucket as deep as its window), reconnects per hour (a dial bucket). `--host` upstreams start at `--host-tier` (default `trusted`) the first time they're seen.
- Signals: the node's reject hook turns every verification failure into `FailedValidation` (with the reason as detail) and oversized frames into `OversizedCommit`. Follow-ons and the relay's own trouble (`stale`, `desynchronized`, `inactive`, `rate_limited`, `identity_unavailable`, `store`, `not_owner`) aren't signals. Rejects from the host stage (signatures, malformed frames, oversized commits) also count toward the host's `failed_checks`, so the error budget sees them. The state step counts its own.
- Accepted `#identity` events are `IdentityChange`. Each host's `#identity` events spend its tier's `identityEventsPerHour` (0 is unlimited) at the host stage. Past it they're dropped as `identity_rate`, which is an `IdentityChange` signal too, so `hostIdentityChurn` sees the flood.

### State and the node

- Accounts go through `state::AccountGate` after their host checks out. The gate tells a **newly created** account from one that's only **first seen**. A relay that starts cold sees all ~56M established accounts for the first time, and none of them is new.
  - Newly created means the event is the repo's first commit: a `#commit` with no `since`, and no `prevData` or the empty tree's (`verify::Verified::created`, carried to a remote DID owner in the forward's meta flags). A PDS usually announces a new account with `#identity` and `#account` first. So the gate is asked again at a known account's first commit (`Arrival::FirstCommit`), as well as when an unknown DID shows up (`FirstSeen` or `Created`). Only the account's own PDS creates its record: another host's `#identity` for an unknown DID is emitted without one, so it can't credit an account to a PDS's cap or let a later commit skip the cap as `FirstCommit`.
  - Every account counts toward its host's cap (`maxAccounts`, indigo's per-host account limit). Past it the account is created throttled (`relay_throttled`, status `throttled`): its commits are dropped, an upstream `#account` doesn't lift it, and an operator's untakedown does. The cap counts the record's `account_count` plus the accounts admitted since the last sync, so it can lag by one counter flush (5 s), never overcount. Bluesky's PDSes (`*.host.bsky.network`) are trusted, at 10M each, well above the ~0.5-1M accounts a mushroom holds. An untrusted PDS is held to 1,000 by default (indigo uses 100). Raise well-known PDSes per host when deploying.
  - Only newly created accounts spend the host's `newAccountsPerHour` and the cluster's `NewAccountsPerMin` budget, and only they are `NewAccount` signals. The signal is recorded once per creation, deferred or not, so a farm trips `hostNewAccounts` at the rate it creates accounts. Past a rate the event is dropped (`new_account_deferred`) and the DID is remembered for an hour (100k at most), so its later events, which no longer look like a creation, wait for the budget too.
  - First-seen accounts aren't deferred. Each costs a DID lookup, which the PLC budget paces (below).
  - Creation can be faked: a farm can send a first commit with a made-up `since` and `prevData`, and the relay holds no earlier state to check them against. The host's account cap still applies, and that's 100 for an untrusted host. Checking the DID's PLC creation op would catch it, but that's an audit-log fetch per unknown DID (56M on a cold start), so it isn't done.
- Accepted commits are `Record` signals (count 1).
- DID document fetches spend the cluster's `PlcLookupsPerSec` share (`IdentityCache::set_budget_gate`), waiting up to the cache's `max_budget_wait` for it. The host stage and the DID owner look up through `IdentityCache::lookup_paced`, which waits again whenever the budget is spent. The event holds its lane, so the backpressure reaches the host's socket, and nothing is dropped for want of budget. Before this, three spent-budget waits dropped the event as `identity_unavailable` and acked it upstream. A failed fetch still gives up after three tries.
- Fresh fetches an event asks for spend its host's own budget first (`AccountGate::forced_lookup`, `PolicyHooks::take_forced_lookup`): the tier's `identityEventsPerHour`, at most a minute's worth (and 10) at once. These are an `#identity`'s refresh, the re-resolve on a host mismatch and the refresh after a signature fails against the cached key. With that budget spent, the event is checked against the cached document. `vlrelay_forced_lookups_refused_total` counts these. Any host can send `#identity` for any DID, so without this one host's stream would drain the shared PLC budget and stall every other host's lookups behind it. Besides that:
  - an `#identity` from a host that isn't the account's PDS doesn't refresh a document fetched in the last 30 s (`reresolve_after_secs`, the same window a host mismatch gets);
  - the identity cache answers a forced refresh with the DID's last one if that's under 30 s old (`identity::Options::min_refresh`);
  - the host stage no longer drops the DID from its cache on `#identity`, which made the next lookup a fetch. The DID owner's refresh updates the cache, and a changed key reaches host owners as before.
- A takedown or its reversal is written to `policy/takedowns/audit/` (one object per action, If-None-Match) and `policy/takedowns/current/{sha256(did)}.json` (who, when, why, for the account page) before the account changes, and logged on `vlrelay::audit`.
- A takedown also filters the replay window. A consumer whose cursor reaches back past it doesn't get the account's earlier `#commit` and `#sync` frames, whether they come from the ring or from segment backfill, on any core, edge or replica. Its `#account` and `#identity` frames pass, so consumers see the status change. Frames are skipped, not renumbered, so every node keeps the same seqs and consumers see a gap, which the spec allows. Lifting the takedown lets the old frames replay again. indigo doesn't filter replay at all (reference-notes.md, "Account status"), so after a reversal both relays serve the same thing.
  - Every serving node keeps a `TakedownSet` (`policy::takedowns`), the DIDs whose current object says `takedown: true`. `Serve` polls `policy/takedowns/current/` every `REFRESH_EVERY` (10 s): a LIST, then a GET for each object whose ETag changed, 16 at a time. It also reads the list once before the node serves (`Serve::load_takedowns`). Edges and replicas read the bucket the same way, so every node agrees within one poll.
  - The core that takes an account down applies the change to its own set (`TakedownSet::apply_local`) after the current object is written and before it appends the `#account`, so its consumers never see the `#account` and then the account's old commits. A poll that listed the objects before that local apply can't undo it, and the object is fetched again on the next poll.
  - The set is vlpds's `FrameFilter` on the firehose (`Firehose::set_filter`). vlpds reads each frame's type and DID straight from the CBOR (`frame_meta`: `repo` for a `#commit`, `did` for the rest, the same keys `event::route` uses). A ring batch computes its verdicts once per set generation, and every subscriber shares them. Backfill checks each frame it reads. An empty set does no parsing at all. With a non-empty set, a 2,000-event batch costs 88 ns an event once (about 1% of a core at 100k/s), and a batch with nothing to skip is written as one slice, the same as with no filter.
  - Until a node's first poll lands (the bucket is down at startup, say), it serves unfiltered and logs a warning. Other cores lag the owner by up to one poll.

### Operator API

`node::admin::NodeAdmin` forwards policy, domain rules, cases and every host action but `reconnect` to `PolicyAdmin`, then applies the host's new policy to its socket right away. `GET/PUT policy/full`, `GET domain-rules/audit` and `GET cases/{id}/evidence` carry the whole document, the rules' audit log and a case's evidence.

## PLC export seeding: the full notes

`src/plc_seed.rs` and `src/plc_seed/`. With `--plc-export`, the relay reads the PLC directory's `/export` (JSON lines, 1,000 ops a request, `after=<createdAt>` cursors) and keeps, per did:plc, the latest op's `#atproto` key, its `atproto_pds` host, whether it's a tombstone, and the op's `createdAt`. A DID document cache miss then costs no PLC lookup.

### Reading

- History is split into `--plc-export-streams` time windows (4), each with its own cursor, read side by side. One cursor is bound by a page's round trip (0.64 s for 1,000 ops from plc.directory, measured), so windows let `--plc-export-rate` (2 requests/s, all windows together) set the pace. The last window has no end: once every other window is done and it reads a short page, it's the live tail, polled every 2 s.
- `after` is exclusive and ops can share a millisecond across a page boundary, so a window asks from a millisecond before the newest op it read and skips the DIDs it already read at that millisecond.
- 429 and 5xx back off (1 s doubling to 2 min, or the `Retry-After`). Errors and the pace are counted in `vlrelay_plc_export{what}`, and `vlrelay_identity_lookups{outcome="seeded"}` counts the misses it saved.
- The cursors and per-window counts checkpoint to `plc/export-checkpoint.json` every 10 s, after the sink has flushed every entry before them to the shards (they run without a WAL). A restart re-reads at most that interval. Any sink error ends the run and the supervisor resumes from the checkpoint.

### Validation

Kept thin on purpose. A line must be a well-formed op of a known type (vlpds's `plc::op_type`, strict about fields) for a valid did:plc, with a parseable `createdAt`. Nullified ops are skipped (the later recovery op replaces them). A key the relay can't parse leaves the entry without one, which means "resolve". The op chain isn't checked: checking each op's signature against the previous op's rotation keys needs those keys per DID (~70 bytes more each) and the 72 h recovery-fork rules, and buys little here, since every commit is still verified against the seeded key and a failure re-resolves from PLC.

### Where it's kept

`0x03 ‖ slot ‖ DID` rows in the DID shard's SlateDB, beside the state records' `0x01` (so `listRepos` scans never walk them), written by the shard's owner: version, flags, `createdAt` (varint ms), the key's multicodec bytes and the PDS host. A flag keeps an `http://` endpoint's scheme (a local PDS's), since the host alone reads back as https. A write keeps the newer `createdAt`, so an op the export repeats or delivers late doesn't replace a newer one.

In a cluster, the lowest-named live core reads the export and forwards each batch to the DID owners (`plc_seed::peer`, `POST /internal/relay/v1/plc/apply`, then `.../flush` before a checkpoint). The alternative, every owner filtering the whole stream for its slots, costs plc.directory and every node N times the requests and bytes. A leadership change can briefly leave two readers; both write the same ops, and the checkpoint at worst sends the next reader back a few seconds. A split or merge doesn't carry these rows to the children yet, so the children's DIDs resolve from PLC on a miss ([Cluster](cluster.md#resharding)).

### Using it

The DID document cache asks its seeder on a miss, before it spends any budget (not on a forced refresh). The seeder weighs the DID's state record against its export entry (`plc_seed::choose`):

| State record | Export entry | Used |
|---|---|---|
| resolved within the cache TTL (1 h) | none, or an older op | the record |
| resolved within the TTL | a newer op (a tie goes to the op) | the entry |
| none, or older than the TTL and agreeing | any age | the entry |
| an `#identity` not resolved since (`fetched_at` 0), op older than the TTL | | resolve |
| resolved after the op with another key or PDS, op older than the TTL | | resolve |
| older than the TTL | none or a tombstone | resolve |

A host stage that doesn't hold the DID asks the owner (`GET /internal/relay/v1/plc/pick`, 1 s timeout; a failure means resolve). An `#identity` still forces a fresh resolve, which stamps the record, so an `#identity` newer than the export wins. A signature that fails against a seeded key refreshes from PLC as before. When a write changes a DID's key or PDS (or the op is recent enough to postdate a cached copy), its owner drops the DID from its cache and tells its peers to, so a rotation the tail picks up reaches the next lookup.

### Numbers

`plc_seed::tests` against fakepds's fake export (`src/fakepds/export.rs`: the fleet's DIDs, one genesis op each, then appended ops; pagination and a tail like plc.directory's), one process, debug build of the relay crate:

| | |
|---|---|
| Ingest, 100,000 DIDs in 150 pages | 2.5 s: 39,900 ops/s, 20 MB/s (the fake serves its own pages from the same process) |
| Stored per DID | 79 bytes raw (19-byte key, value), 52 bytes in the shards' SSTs after compression |
| Their first commits after seeding | 100,000 accepted, 0 PLC document fetches |
| Restart after a quarter (4,996 of 20,000 ops) | resumed from both windows' cursors and read the other 15,005 (one op twice, at a millisecond boundary) |

Against the dev network's real did-method-plc (`VLRELAY_PLC_EXPORT=1 just e2e`): 36 ops read, none invalid, 30 cache misses seeded and 28 PLC fetches, one per `#identity` (forced refreshes), with the e2e passing. `VLRELAY_PLC_EXPORT=1 just e2e-cluster --duration 60` passes too: the reader moved to another core after the kill -9 and the SIGTERM and resumed from the checkpoint each time, and host stages filled 38 misses from their DIDs' owners.

The relay isn't the limit, plc.directory is. Its pages measured 515 KB (2022 ops, legacy `create`), 711 KB (2024) and 934 KB (2026), about 0.7 KB an op, so the ~80M ops behind 56M DIDs (an estimate: ~1.4 ops a DID) are ~56 GB. Cold start at 1,000 ops a request:

| `--plc-export-rate` | Cold start | Download |
|---|---|---|
| 1/s | 22 h | 0.7 MB/s |
| 2/s (default) | 11 h | 1.4 MB/s |
| 5/s | 4.4 h | 3.5 MB/s |

Each request needs a window to carry it, about rate × 0.64 s of them, so 4 windows cover up to ~6/s. The windows are even in time, not in ops, so the heavy 2023-2024 windows finish last. Real accounts' PDS hosts are ~35 characters against the fake's ~15, so expect ~100 bytes raw per DID: ~5.6 GB raw, ~3 GB in SSTs for 56M. The rates plc.directory allows aren't documented; start at the default and watch `vlrelay_plc_export{what="throttled"}`.

