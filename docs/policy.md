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
| Accounts per host (`tiers.default.maxAccounts`) | 100 | `--default-account-limit` 100 |
| Accounts per trusted host | 10,000,000 | `TrustedRepoLimit` |
| Untrusted events | 50 + n/1000 per s, 2,500 + n per h, 20,000 + 10n per day | same formula |
| Trusted events | 5,000/s, 50M/h, 500M/day | same |
| Trusted domains (`crawl.trustedDomains`) | `*.host.bsky.network` | same |
| New hosts per day (`cluster.newHostsPerDay`) | 50, shared across the cluster | 50, per process |
| New host tier | `new` for 7 clean days, then `default` | no tiers |

The relay-only defaults are guesses to tune against real traffic. Auto-throttle trips at 50% failed frames over a sweep interval with at least 200 frames. A throttled host recovers after an hour without a trip. The spam thresholds are 300 new accounts an hour per host, 600 records a minute per account and 600 failed frames a minute per host. The trusted tier is never auto-throttled, since throttling the big PDSes for a buggy minute would stall most of the network.

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

## Integration points

The lead wires these up. Nothing outside `src/policy*` calls the engine yet.

Construction and loops (in `main` or the node module):

```rust
let engine = policy::Engine::new(store.clone(), &node_id, live_nodes);  // live_nodes: Arc<dyn LiveNodes>
engine.spawn_refresher();
let driver = Arc::new(policy::driver::Driver::new(engine.clone(), host_store.clone()));
driver.spawn();
```

`LiveNodes` is a one-method trait (`live_nodes() -> usize`). The cluster module should implement it over the node leases. `FixedNodes` covers tests and single-node runs.

### Upstream (`src/upstream*`)

- `engine.admit_host(&AdmitRequest { hostname, by_admin, existing })` before subscribing. It parses and normalizes the hostname (indigo's rules), checks the crawl switch, domain bans, the host's own tier and the allow-list mode, picks the starting tier and spends the daily budget. On `Admit::Admit { host, tier, .. }` the upstream calls `describeServer` and creates the record with `tier`. `Reject` carries a message fit for the XRPC error.
- `engine.for_host(&record)` gives the `HostLimits` to enforce: `connect` (false for suspended and banned), and the tier's events/s, per hour and per day, bytes/s, account cap, new accounts per hour, identity events per hour and reconnects per hour. Rule and operator throttles are already folded in. Re-read it when `HostLimits.policy_version` or the record's tier changes (cheap, two hash probes per label).
- `engine.record_signal(Signal { kind, host, did, count, detail })` for `FailedValidation`, `OversizedCommit` and `IdentityChange` as frames are checked. It returns any trips right away, and also queues them for the driver.
- The host's `events` and `failed_checks` counters (already flushed by `add_counts`) feed the error budget. The upstream doesn't need to do anything else for it.

### State (`src/state*`)

- `engine.record_signal(...)` with `NewAccount` the first time a DID appears on a host, and `Record` per commit (or `count` = ops), on the DID owner.
- `engine.try_take(BudgetKind::NewAccountsPerMin, 1.0)` before creating an account. When it's false, create it `host-throttled`. Use `try_take(BudgetKind::PlcLookupsPerSec, 1.0)` before a PLC resolve.
- `for_host(...).limits.max_accounts` is the per-host account cap.
- `HostStore` would do well with an atomic `update_host(hostname, FnOnce(&mut HostRecord))`. Today the driver reads, steps and puts, so a counter flush landing in between can be overwritten. The window is small and only opens when a tier changes. The fix belongs in the state module.

### Serving (`src/serve.rs`)

- `engine.consumer_limits()` gives connections per IP, consumers per node, the slow-consumer lag cutoff and the backfill limit.

### Operator API (`src/admin.rs`)

`policy::admin::PolicyAdmin { engine, hosts }` implements the policy half of `AdminSource`. The relay's real source forwards these calls to it.

| `AdminSource` method | `PolicyAdmin` |
|---|---|
| `policy`, `update_policy`, `policy_audit` | same names. The wire `Policy` is a subset, and a PUT keeps every field it doesn't carry. |
| `domain_rules`, `create_domain_rule`, `update_domain_rule`, `delete_domain_rule` | same names. `matches` comes from one scan of the host records. |
| `cases`, `case`, `update_case` | same names. Resolving or dismissing frees the dedupe key. |
| `host_action` | returns the updated `HostRecord`, which the caller turns into a `HostRow`. `Reconnect` is the upstream's and is refused here. |
| `host` (the detail page) | `host_limits(&rec)` for `limits`, `host_actions(&rec)` for `actions`. |

Extras with no endpoint yet are `full_policy` and `update_full_policy` (the whole document, for an editor that shows cluster budgets, transitions and consumer limits), `domain_rules_audit` and `case_detail` (a case with its evidence).

Changes made to `src/admin.rs` and the demo:

- `RuleEffect` gained `Allow` (`{"kind": "allow"}`). The demo treats it as a no-op. `ui/src/lib/api.ts` and the rules page need the new variant.

## Spam counting

Each threshold has a fixed-size Space-Saving table: `spam.trackHosts` keys per host rule (1,024) and `spam.trackAccounts` per account rule (8,192), over a sliding window. A new key replaces the lightest one and inherits its count as error, so any key above total/capacity stays in the table. Thresholds are checked against `count - error`, which never overstates a key, so churn through thousands of quiet hosts can't trip a false positive. A key trips at most once per window.

Measured in the tests: 10,000 hosts plus five account farms, and the table held 1,024 hosts at most. All seven tables together stay under 4 MiB, the five farms tripped once each, and no other host tripped. vlpds's busiest-keys tracker uses the same replace-the-minimum idea, but its insert is private to vlpds and isn't capped per table, so this is a separate implementation.

Actions are `alert` (log only), `case`, `throttle` and `throttle-and-case`. A per-account threshold that throttles throttles the DID's host.

## Gaps

- No `policy/*` HTTP endpoints for the full document, rules audit or case evidence yet (the methods exist).
- Peer nudges after a save aren't sent. Peers pick changes up within 10 s.
- The new-hosts counter is per UTC day. indigo uses a sliding 24 h window.
- Signals don't feed Prometheus yet. `Signals::top()` returns the busiest keys per rule for a bounded-cardinality gauge.
