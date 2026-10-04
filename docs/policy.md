# vlRelay policy engine

`src/policy.rs` and `src/policy/` hold every limit the relay enforces, the domain rules, requestCrawl admission, the cluster budgets, the spam counters and the cases. The engine decides and counts. The upstream module (host limits) and the state module (per-DID limits) enforce.

## What's in the bucket

| Object | What | Written by |
|---|---|---|
| `policy/current.json` | The policy: tier limits, transitions, spam thresholds, cluster budgets, consumer limits, crawl rules | operators, through `PolicyAdmin` |
| `policy/audit/{version:020}.json` | One audit entry per policy version (who, when, note, every changed leaf) | the save that made the version |
| `policy/domain-rules.json` | Domain rules with their own version | operators |
| `policy/domain-rules-audit/{version:020}.json` | The rules' audit log | the save |
| `policy/counters/new-hosts.json` | `{day, used}`, today's new-host admissions across the cluster | `admit_host`, by CAS |
| `cases/c/{id:016}.json` | One case with its evidence (newest 20 trips) | the trip driver and operators, by CAS |
| `cases/open/{hash}.json` | The open case for a dedupe key (rule, host, DID) | created with If-None-Match |
| `cases/next-id.json` | The case id counter | CAS |

A save reads the object, checks the operator's base version against it and PUTs version + 1 with If-Match on the ETag. So two operators editing at once get one success and one 409 (tested with eight concurrent writers). The audit entry is a separate object created with If-None-Match, so the log only grows. The policy object repeats its own audit entry, and the next save writes it if a crash lost it.

Every node polls both objects every 10 s with a conditional GET (a 304 costs nothing) and swaps in a new snapshot when either changes. A node that fetches an invalid object keeps its last good one and reports why in `Engine::last_error`. `Engine::wake()` makes the poll happen now, so the lead can nudge peers after a save the way vlpds does.

## Defaults

They match indigo's relay where the reference notes give a number.

| Setting | Default | indigo |
|---|---|---|
| Accounts per host (`tiers.default.maxAccounts`), every account the host serves | 100 | `--default-account-limit` 100 |
| Accounts per trusted host | 10,000,000 | `TrustedRepoLimit` |
| Newly created accounts per host (`tiers.*.newAccountsPerHour`) | 100 (`default`), 25 (`new`), 10 (`throttled`), none (`trusted`) | none |
| Newly created accounts per cluster (`cluster.newAccountsPerMin`) | 6,000 | none |
| DID lookups (`cluster.plcLookupsPerSec`) | 500/s across the cluster | none (per-process `--did-lookups-per-sec`) |
| Untrusted events | 50 + n/1000 per s, 2,500 + n per h, 20,000 + 10n per day | same formula |
| Trusted events | 5,000/s, 50M/h, 500M/day | same |
| Trusted domains (`crawl.trustedDomains`) | `*.host.bsky.network` | same |
| New hosts per day (`cluster.newHostsPerDay`) | 50, shared across the cluster | 50, per process |
| New host tier | `new` for 7 clean days, then `default` | no tiers |

The relay-only defaults are guesses to tune against real traffic. Auto-throttle trips at 50% failed frames over a sweep interval with at least 200 frames. A throttled host recovers after an hour without a trip. The spam thresholds are 300 newly created accounts an hour per host, 600 records a minute per account and 600 failed frames a minute per host. The trusted tier is never auto-throttled, since throttling the big PDSes for a buggy minute would stall most of the network.

There are two deliberate departures from indigo. Trusted-domain and allow-listed hosts don't spend the daily new-host budget (indigo counts everything but admin requests). And a failed counter read refuses the crawl, where indigo answers 200 when its ban lookup fails.

## Tiers

`step()` in `policy/tiers.rs` is the whole state machine. It's a pure function of the host's tier, its policy state, what happened since the last step, the policy and the time.

| From | To | When |
|---|---|---|
| (new host) | `new`, or `trusted` on a trusted domain, or a rule's tier | admission |
| `new` | `default` | `promoteAfterDays` old and no trip in that long |
| `new`, `default` (`trusted` if its tier allows) | `throttled` | error budget or a spam threshold whose action throttles |
| `throttled` (auto) | the tier it came from | `recoverAfterSecs` without a trip |
| any | `suspended`, `banned` and back | operators only |

The policy state (where to recover to, last trip, trip count, operator throttle, the last 20 operator actions) lives in the host record under `extra.policy`. The state module's format doesn't change, and records without it read as "never tripped".

`policy::driver::Driver` applies it. `process_trips()` runs every second, throttles the hosts whose trips say so and opens or updates cases. `sweep()` runs every 30 s and steps every host its `HostStore` lists, using the counter deltas since the previous sweep for the error budget. After a restart the first sweep only takes a baseline. Records are only written when something changed.

## Wiring

`src/node/policy.rs` (`PolicyHooks`) carries the engine's decisions to the parts that enforce them and feeds it what they see. `main` builds the engine (`FixedNodes(1)` on one node, and a cluster passes its own `LiveNodes`) and hands it to the node in `NodeConfig.policy`. `Node::start` installs the hooks before the upstream manager starts, so no host connects against the policy, then starts the engine's refresher, the driver and the sync loop.

### Who owns a host's tier

The state host record. The policy engine writes it (operator actions through `PolicyAdmin`, the driver's throttles and recoveries), and everything else follows it:

- `HostStore::update_host(hostname, f)` is an atomic read-modify-write under the host's lock. The driver, `PolicyAdmin::host_action`, the counter flush and the upstream registry's flush each change only their own fields, so a counter flush can't overwrite a tier change or the other way round.
- The upstream registry's flush (`node::adapters::StateHosts`) only seeds the tier when it creates a record (a new host's admission tier). After that it never writes it.
- The hooks keep a per-host cache of `engine.for_host(&record)` (tier after domain rules, `connect`, limits, rule, operator throttle). It's the upstream manager's `PolicySource`. It's refreshed when a record is written through the hooks' `HostStore` (which names every host it writes to the sync loop), when the policy or the domain rules change version (checked every second), and every 30 s for records written elsewhere.
- `Manager::apply_policy(host)` copies the cache onto the host's registry entry: its tier, its limits (the socket's buckets are retuned in place, keeping what they hold or owe), and a disconnect or a connect when `connect` flips. So a domain-rule ban added later disconnects a host that's already registered, within about a second.

### Upstream

- requestCrawl goes through `engine.admit_host` (`upstream::Admission`): crawl switch, hostname rules, domain bans, a known host's own tier, allow-list mode, the starting tier and the cluster's daily new-host budget. A known host is checked too, so a ban added since holds. On a dev network the engine's indigo hostname rules refuse IPs and ports, so those hosts are admitted as an operator would add them.
- `for_host` limits: events/s, bytes/s, events per hour and per day (each a token bucket as deep as its window), reconnects per hour (a dial bucket). `--host` upstreams start at `--host-tier` (default `trusted`) the first time they're seen.
- Signals: the node's reject hook turns every verification failure into `FailedValidation` (with the reason as detail) and oversized frames into `OversizedCommit`. Follow-ons and the relay's own trouble (`stale`, `desynchronized`, `inactive`, `rate_limited`, `identity_unavailable`, `store`, `not_owner`) aren't signals. Rejects from the host stage (signatures, malformed frames, oversized commits) also count toward the host's `failed_checks`, so the error budget sees them. The state step counts its own.
- Accepted `#identity` events are `IdentityChange`.

### State and the node

- Accounts go through `state::AccountGate` after their host checks out. The gate tells a **newly created** account from one that's only **first seen**. A relay that starts cold sees all ~56M established accounts for the first time, and none of them is new.
  - Newly created means the event is the repo's first commit: a `#commit` with no `since`, and no `prevData` or the empty tree's (`verify::Verified::created`, carried to a remote DID owner in the forward's meta flags). A PDS usually announces a new account with `#identity` and `#account` first. So the gate is asked again at a known account's first commit (`Arrival::FirstCommit`), as well as when an unknown DID shows up (`FirstSeen` or `Created`).
  - Every account counts toward its host's cap (`maxAccounts`, indigo's per-host account limit). Past it the account is created throttled (`relay_throttled`, status `throttled`): its commits are dropped, an upstream `#account` doesn't lift it, and an operator's untakedown does. The cap counts the record's `account_count` plus the accounts admitted since the last sync, so it can lag by one counter flush (5 s), never overcount. Bluesky's PDSes (`*.host.bsky.network`) are trusted, at 10M each, well above the ~0.5-1M accounts a mushroom holds. An untrusted PDS is held to 100, as in indigo.
  - Only newly created accounts spend the host's `newAccountsPerHour` and the cluster's `NewAccountsPerMin` budget, and only they are `NewAccount` signals. The signal is recorded once per creation, deferred or not, so a farm trips `hostNewAccounts` at the rate it creates accounts. Past a rate the event is dropped (`new_account_deferred`) and the DID is remembered for an hour (100k at most), so its later events, which no longer look like a creation, wait for the budget too.
  - First-seen accounts aren't deferred. Each costs a DID lookup, which the PLC budget paces (below).
  - Creation can be faked: a farm can send a first commit with a made-up `since` and `prevData`, and the relay holds no earlier state to check them against. The host's account cap still applies, and that's 100 for an untrusted host. Checking the DID's PLC creation op would catch it, but that's an audit-log fetch per unknown DID (56M on a cold start), so it isn't done.
- Accepted commits are `Record` signals (count 1).
- DID document fetches spend the cluster's `PlcLookupsPerSec` share (`IdentityCache::set_budget_gate`), waiting up to the cache's `max_budget_wait` for it. The host stage and the DID owner look up through `IdentityCache::lookup_paced`, which waits again whenever the budget is spent. The event holds its lane, so the backpressure reaches the host's socket, and nothing is dropped for want of budget. Before this, three spent-budget waits dropped the event as `identity_unavailable` and acked it upstream. A failed fetch still gives up after three tries.
- A takedown or its reversal is written to `policy/takedowns/audit/` (one object per action, If-None-Match) and `policy/takedowns/current/{sha256(did)}.json` (who, when, why, for the account page) before the account changes, and logged on `vlrelay::audit`.

### Operator API

`node::admin::NodeAdmin` forwards policy, domain rules, cases and every host action but `reconnect` to `PolicyAdmin`, then applies the host's new policy to its socket right away. `GET/PUT policy/full`, `GET domain-rules/audit` and `GET cases/{id}/evidence` carry the whole document, the rules' audit log and a case's evidence.

## PLC export seeding

`src/plc_seed.rs` and `src/plc_seed/`. With `--plc-export`, the relay reads the PLC directory's `/export` (JSON lines, 1,000 ops a request, `after=<createdAt>` cursors) and keeps, per did:plc, the latest op's `#atproto` key, its `atproto_pds` host, whether it's a tombstone, and the op's `createdAt`. A DID document cache miss then costs no PLC lookup.

### Reading

- History is split into `--plc-export-streams` time windows (4), each with its own cursor, read side by side. One cursor is bound by a page's round trip (0.64 s for 1,000 ops from plc.directory, measured), so windows let `--plc-export-rate` (2 requests/s, all windows together) set the pace. The last window has no end: once every other window is done and it reads a short page, it's the live tail, polled every 2 s.
- `after` is exclusive and ops can share a millisecond across a page boundary, so a window asks from a millisecond before the newest op it read and skips the DIDs it already read at that millisecond.
- 429 and 5xx back off (1 s doubling to 2 min, or the `Retry-After`). Errors and the pace are counted in `vlrelay_plc_export{what}`, and `vlrelay_identity_lookups{outcome="seeded"}` counts the misses it saved.
- The cursors and per-window counts checkpoint to `plc/export-checkpoint.json` every 10 s, after the sink has flushed every entry before them to the shards (they run without a WAL). A restart re-reads at most that interval. Any sink error ends the run and the supervisor resumes from the checkpoint.

### Validation

Kept thin on purpose. A line must be a well-formed op of a known type (vlpds's `plc::op_type`, strict about fields) for a valid did:plc, with a parseable `createdAt`. Nullified ops are skipped (the later recovery op replaces them). A key the relay can't parse leaves the entry without one, which means "resolve". The op chain isn't checked: checking each op's signature against the previous op's rotation keys needs those keys per DID (~70 bytes more each) and the 72 h recovery-fork rules, and buys little here, since every commit is still verified against the seeded key and a failure re-resolves from PLC.

### Where it's kept

`0x03 ‖ slot ‖ DID` rows in the DID shard's SlateDB, beside the state records' `0x01` (so `listRepos` scans never walk them), written by the shard's owner: version, flags, `createdAt` (varint ms), the key's multicodec bytes and the PDS host. A write keeps the newer `createdAt`, so an op the export repeats or delivers late doesn't replace a newer one.

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

## Spam counting

Each threshold has a fixed-size Space-Saving table: `spam.trackHosts` keys per host rule (1,024) and `spam.trackAccounts` per account rule (8,192), over a sliding window. A new key replaces the lightest one and inherits its count as error, so any key above total/capacity stays in the table. Thresholds are checked against `count - error`, which never overstates a key, so churn through thousands of quiet hosts can't trip a false positive. A key trips at most once per window.

Measured in the tests: 10,000 hosts plus five account farms, and the table held 1,024 hosts at most. All seven tables together stay under 4 MiB, the five farms tripped once each, and no other host tripped. vlpds's busiest-keys tracker uses the same replace-the-minimum idea, but its insert is private to vlpds and isn't capped per table, so this is a separate implementation.

Actions are `alert` (log only), `case`, `throttle` and `throttle-and-case`. A per-account threshold that throttles throttles the DID's host.

## Gaps

- `engine.consumer_limits()` isn't enforced by `serve.rs` yet (connections per IP, consumers per node, the slow-consumer cutoff and the backfill limit come from vlpds's firehose options).
- `LiveNodes` is `FixedNodes(1)`. The cluster module should pass one over its node leases.
- A relay-throttled account stays throttled until an operator lifts it, even after its host drops below its cap.
- Without `--plc-export`, a cold start resolves every account once at the PLC budget, so 56M accounts at the default 500/s take about 31 hours. With it, the export takes about 11 hours at the default 2 requests/s (below), off the lookup budget, and only did:web accounts and DIDs the export hasn't reached yet spend lookups. The export read itself can't go faster than plc.directory lets it.
- The account cap and the per-host new-account rate are per node, so a host shard that moves starts them over from the record's count.
- Peer nudges after a save aren't sent. Peers pick changes up within 10 s.
- The new-hosts counter is per UTC day. indigo uses a sliding 24 h window.
- Signals don't feed Prometheus yet. `Signals::top()` returns the busiest keys per rule for a bounded-cardinality gauge.
