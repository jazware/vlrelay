---
title: Policy
section: vlRelay
order: 6
summary: "Every limit the relay enforces, the domain rules, requestCrawl admission, host tiers, spam counters, cases and takedowns: one versioned document in the bucket that every node reloads."
---

```hero
diagram:
  caption: Operators edit the policy through the dashboard with compare-and-swap writes, and every change gets an audit entry. Every node polls it every 10 s. The node that reads a host enforces its limits, the leader enforces account limits, and every node filters taken-down accounts out of replays.
  nodes:
    - { id: op, label: Operator, sub: "`/admin` dashboard", at: [0, 4], size: [8, 3] }
    - { id: pol, label: "`policy/current.json`", sub: "versioned · If-Match", at: [12, 1], size: [11, 2.6], shape: store, tone: amber }
    - { id: audit, label: "`policy/audit/`", sub: one entry per version, at: [12, 7], size: [11, 2.6], shape: store, tone: amber }
    - { id: host, label: Reading node, sub: "tiers · admission · rates", at: [28, 0], size: [10, 3], tone: accent }
    - { id: did, label: Leader, sub: "account cap · new accounts", at: [28, 4.5], size: [10, 3], tone: accent }
    - { id: serve, label: Every node, sub: takedown filter, at: [28, 9], size: [10, 3], tone: blue }
    - { id: cases, label: Cases, sub: spam trips · read lag, at: [42, 4.5], size: [8, 3], tone: danger }
  edges:
    - "op.r -> pol.l: PUT, CAS"
    - "op.r -> audit.l"
    - { from: pol.r, to: host.l, label: poll 10 s, dash: true }
    - { from: pol.r, to: did.l, dash: true }
    - { from: pol.r, to: serve.l, dash: true }
    - "host.r -> cases.l30: signals"
    - "did.r -> cases.l"
facts:
  - { value: "10 s", label: for a change to reach every node, note: "a conditional GET; a 304 costs nothing", tone: amber }
  - { value: "1,000", unit: accounts, label: per default-tier host, note: "indigo uses 100; trusted hosts get 10M" }
  - { value: "50", unit: new hosts/day, label: across the cluster, note: "trusted domains and allow-listed hosts don't spend it", tone: violet }
  - { value: "7", unit: clean days, label: from new to default, note: "auto-throttled hosts recover after an hour without a trip", tone: blue }
```

The policy engine holds every limit the relay enforces, the domain rules, `requestCrawl`
admission, the cluster-wide budgets, the spam counters and the cases. The engine decides and
counts. The node that reads a host enforces its host limits, and the leader enforces per-account
ones when it checks each event against the account's record. The
defaults follow indigo's relay wherever it has a number, and the operator dashboard edits all of
it ([Admin API](admin-api.md)).

## What's in the bucket

| Object | What | Written by |
|---|---|---|
| `policy/current.json` | The policy: tier limits, transitions, spam thresholds, cluster budgets, consumer limits, crawl rules | operators |
| `policy/audit/{version:020}.json` | One audit entry per policy version (who, when, note, every changed leaf) | the save that made the version |
| `policy/domain-rules.json` | Domain rules, with their own version | operators |
| `policy/domain-rules-audit/{version:020}.json` | The rules' audit log | the save |
| `policy/counters/new-hosts.json` | `{day, used}`, today's new-host admissions across the cluster | admission, by CAS |
| `policy/takedowns/` | One audit object per takedown or reversal, and the current state per account | the takedown |
| `cases/c/{id:016}.json` | One case with its evidence (the newest 20 trips) | the trip driver and operators, by CAS |
| `cases/open/{hash}.json` | The open case for a dedupe key (rule, host, DID) | created with If-None-Match |
| `cases/next-id.json` | The case id counter | CAS |

A save reads the object, checks the operator's base version against it and PUTs version + 1 with
If-Match on the ETag. So two operators editing at once get one success and one 409 (tested with
eight concurrent writers). The audit entry is a separate object created with If-None-Match, so
the log only grows. The policy object repeats its own audit entry, and the next save writes it if
a crash lost it.

Every node polls the policy and the rules every 10 s with a conditional GET and swaps in the new
snapshot when either changes. A node that fetches an invalid object keeps its last good one and
reports why.

## Defaults

| Setting | Default | indigo |
|---|---|---|
| Accounts per host (`tiers.default.maxAccounts`) | 1,000, so mid-sized PDSes aren't throttled on day one | `--default-account-limit` 100 |
| Accounts per trusted host | 10,000,000 | `TrustedRepoLimit` |
| Newly created accounts per host an hour (`tiers.*.newAccountsPerHour`) | 100 (`default`), 25 (`new`), 10 (`throttled`), none (`trusted`) | none |
| Newly created accounts per cluster (`cluster.newAccountsPerMin`) | 6,000 a minute | none |
| DID lookups (`cluster.plcLookupsPerSec`) | 500/s across the cluster | per process (`--did-lookups-per-sec`) |
| Untrusted events | 50 + n/1000 per s, 2,500 + n per h, 20,000 + 10n per day | same formula |
| Trusted events | 5,000/s, 50M/h, 500M/day | same |
| Trusted domains (`crawl.trustedDomains`) | `*.host.bsky.network` | same |
| New hosts per day (`cluster.newHostsPerDay`) | 50, shared across the cluster | 50, per process |
| New host tier | `new` for 7 clean days, then `default` | no tiers |

The relay-only defaults are guesses to tune against real traffic. Auto-throttle trips at 50%
failed frames over a sweep interval with at least 200 frames, and a throttled host recovers after
an hour without a trip. The spam thresholds are 300 newly created accounts an hour per host, 600
records a minute per account and 600 failed frames a minute per host. The trusted tier isn't
auto-throttled by default (`tiers.trusted.autoThrottle`), since throttling the big PDSes for a buggy
minute would stall most of the network.

There are two deliberate departures from indigo. Trusted-domain and allow-listed hosts don't spend
the daily new-host budget (indigo counts everything but admin requests). And a failed counter
read refuses the crawl, where indigo answers 200 when its ban lookup fails.

## Tiers

| From | To | When |
|---|---|---|
| (new host) | `new`, or `trusted` on a trusted domain, or a domain rule's tier | admission |
| `new` | `default` | `promoteAfterDays` old and no trip in that long |
| `new`, `default` (and `trusted` if its tier allows) | `throttled` | the error budget, or a spam threshold whose action throttles |
| `throttled` (auto) | the tier it came from | `recoverAfterSecs` without a trip |
| any | `suspended`, `banned` and back | operators only |

A throttled host's reads are paused down to a low rate, so the PDS buffers instead of the relay
dropping. Its status says `throttled` whenever its own limits (its tier's, a domain rule's or an
operator's throttle) pause the reader, in any tier. A host the relay pauses because the relay is
behind shows `backpressure` instead, with what's full: its in-flight cap (`inflight_full`), the
node's (`node_inflight_full`), its lane queue (`queue_full`, usually an identity-lookup
backlog) or the node's memory budget (`memory_full`). That's never the host's doing, and no tier or throttle change releases it: it resumes
as the relay catches up. A suspended host is disconnected. A banned one is never connected, and its
`requestCrawl` is refused. `--host` upstreams start at `--host-tier` (`trusted` by default) the
first time they're seen. After that the host record's tier holds.

The driver processes trips every second and sweeps every host every 30 s, using the counter
deltas since the previous sweep for the error budget. After a restart the first sweep only takes a
baseline. A host's policy state (where to recover to, last trip, trip count, operator throttle,
the last 20 tier actions) lives in its host record, and a domain-rule ban added later
disconnects a host that's already connected within about a second.

Every tier change the relay makes itself (an auto-throttle, a recovery, a promotion) goes on the
host's action trail next to the operators', as a `set-tier` by `relay (service)` with its `reason`
and, for a throttle, the `case` it opened or updated. That case's `autoAction` says `throttled from`
the old tier. A spam threshold's throttle uses that threshold's case. An error-budget throttle opens
an `error-budget` case (the failed share of the sweep's frames against the budget). An operator's
`set-tier` doesn't exempt a host from the next trip: if its tier allows auto-throttling, a host
still failing its checks goes back to `throttled`, and the trail shows why.

## Domain rules

A domain rule is `example.com` (that host) or `*.example.com` (the domain and every subdomain),
with an effect of `ban`, `allow`, `tier` or `throttle`. An exact rule may also name one host by
IPv4 address or `localhost`, with a port (`127.0.0.1:30003`), which is how dev-network hosts are
known. A spammer spinning up hosts on one domain gets caught as a group.

When several rules match a host, the most specific one decides: an exact name before any wildcard,
then the longest `*.` suffix. So `demo.example.social` can be trusted under a `*.example.social`
that starts its hosts at `new`, and a rule's host count (`matches`) leaves out the hosts a more
specific rule takes.

## Admission

`requestCrawl` goes through these checks in order: the crawl switch, the hostname rules, domain
bans, a known host's own tier, allow-list mode, the starting tier and the cluster's daily
new-host budget. A known host is checked too, so a ban added since holds.

A new host is checked without spending the budget, then probed (`describeServer` and a
`subscribeRepos` socket), and only then admitted for real: the budget is spent and the bans are
checked again. So names that don't answer can't use the budget up. A hostname whose probe failed
is refused for 10 minutes without another probe, and each client IP gets 10 calls a minute (429
past it). An upstream whose handshake says it's a relay (`Server: … atproto-relay`) is refused
at connect and banned through the audited ban action, and an operator unban is how to retry it.

## Discovering hosts

A cold relay knows only the hosts it's given and the PDSes that ask it to crawl. The `discovery`
section of the policy document adds two sources, both run by the leader in the background:

- **Seed relays** (`discovery.seedRelays`: `{url, enabled, refreshIntervalSecs}`): their
  `com.atproto.sync.listHosts`, read a page at a time at `discovery.requestsPerSec` (2), waiting
  out a 429 or a 5xx for its `Retry-After`, the whole list again every `refreshIntervalSecs` (6 h).
  This relay only reads: it never asks them to crawl anything. `--bootstrap-relay` fills the list
  on a first start, while the document has none. After that the dashboard edits it like every
  other policy field (a versioned save with its base version).
- **The PLC export** (`discovery.plc`, with `--plc-export`): the distinct PDS hosts the documents
  the export reader reads name.

Every host found goes through the admission above: the crawl switch aside, the same hostname
rules, domain bans, allow-list mode, starting tier and `describeServer` probe. Nothing comes from
the other relay but the name: not its status, bans or tiers. A new host starts live, with no
backfill. Discovery's admissions don't spend `cluster.newHostsPerDay`, which stays
requestCrawl's: they're paced by their own `discovery.connectsPerMin` (120), so a cold start
isn't held to the daily budget.

Each source's progress is saved in the bucket (`discovery/state.json`) after every page: a new
leader resumes a list where the old one stopped. Each host's source (`requestCrawl`,
`bootstrap:<relay>`, `plc`, `cli`) is kept in the leader's host table and shown on the host and in
the admission log. `GET /admin/api/discovery` shows each source's last and next run and its counts,
and `POST /admin/api/discovery/run` starts one now ([Admin API](admin-api.md)).

## Limits and signals

Each host gets token buckets for events per second, bytes per second, and events per hour and per
day (each as deep as its window), plus a reconnect budget. A host's limits block its reader
instead of dropping frames, as indigo does.

The engine counts signals. Every verification failure is a `FailedValidation` (with the reason)
and oversized frames are `OversizedCommit`. Both count toward the host's error budget. The
relay's own trouble (`stale`, `desynchronized`, `rate_limited`, `identity_unavailable`, `store`,
`not_owner` and so on) doesn't. Accepted `#identity` events spend the host's
`identityEventsPerHour` (0 is unlimited), and past it they're dropped as `identity_rate`.

### Counted on the host's own timeline

A relay that restarts, a host that reconnects after an outage, or a cursor resumed from minutes
back all read a backlog: an hour of a host's normal traffic arriving in a few minutes. Counted when
the relay reads them, those events look like a flood, and the host is throttled and tripped right
when it's only catching up. So per-host limits and spam signals count each event at the `time` the
host stamped on it (`src/upstream/clock.rs`), and a replay costs what the original traffic did.

The host's clock is the newest event time seen, clamped:

- never past now, so a future-dated event buys no room
- never back, so a host can't spend the same stretch twice (the clock outlives its sockets)
- never further back than `--event-horizon-secs` (a day), so a stale or bogus `time` counts at
  the horizon
- forward by any limiter pause it sat out, so debt is paid in real time

Over any stretch the clock moves at most as far as the wall clock plus how far behind it started:
a horizon's worth the first time a process sees the host, and the real time it was away after
that. A genuine burst, whose events are stamped as they're sent, meets the limits exactly as
before.

| Limit or signal | Counted by | Why |
|---|---|---|
| events/s, bytes/s, events/h, events/day | the host's clock | the token buckets run on it |
| `identityEventsPerHour` | the host's clock | the event's own clock as its reader saw it |
| Spam signals except new accounts, and so the auto-throttle they trigger | the host's pace | each event weighs 1/pace, where pace is host seconds per wall second (1 live, 60 for an hour read in a minute), so a window holds about what the host sent in that much of its own time |
| New accounts: `newAccountsPerHour`, `newAccountsPerMin`, the new-accounts signal, the account cap | the wall clock | a farm replayed is still a farm |
| Fresh DID fetches per host, the PLC budget, in-flight caps, other cluster budgets | the wall clock | they guard what the relay spends now |
| Reconnects per hour | the wall clock | dials happen now |
| The error budget | neither | it's a ratio of failed to accepted frames |

The host's pace shows on host detail ("catching up at ×N") and as `catchUpPace` on its row.

## New accounts and the account cap

The account gate tells a newly created account from one that's only first seen. A relay that
starts cold sees all ~56M established accounts for the first time, and none of them is new.

- Newly created means the event is the repo's first commit (a `#commit` with no `since`, and no
  `prevData` or the empty tree's). Only the account's own PDS creates its record, so another
  host's `#identity` for an unknown DID is emitted without one.
- Every account counts toward its host's cap (`maxAccounts`). Past it the account is created
  throttled (status `throttled`): its commits are dropped, an upstream `#account` doesn't lift it,
  and an operator's untakedown does. The cap can lag by one counter flush (5 s), but it never
  overcounts.
- Only newly created accounts spend the host's `newAccountsPerHour` and the cluster's
  `newAccountsPerMin`. Past a rate the event is dropped (`new_account_deferred`) and the DID is
  remembered for an hour (100k at most), so its later events wait for the budget too.
- First-seen accounts aren't deferred. Each costs a DID lookup, which the PLC budget paces. The
  lookup waits for budget instead of dropping the event, and the backpressure reaches the host's
  socket.

Creation can be faked: a farm can send a first commit with a made-up `since` and `prevData`, and
the relay has no earlier state to check them against. The host's account cap still applies.
Checking the DID's PLC creation op would catch it, but that's an audit-log fetch per unknown DID
(56M on a cold start), so it isn't done.

### Big independent PDSes

On a relay that has just started, or has just added a host, every active account is one the relay
hasn't seen. So a real PDS reaches the default cap within minutes. In a two-hour run against ten
real PDSes, eurosky.social (36k accounts), blacksky.app (42k) and atproto.brid.gy (65k) were each
past 100 accounts in the first ten minutes, and 3,508 of their accounts were created throttled
over 90 minutes. Bluesky's own PDSes don't hit it, since `*.host.bsky.network` is trusted.

indigo has the same cap and behaves the same way. Its operators raise the limit per host, and
raising it releases up to that many throttled accounts. In vlRelay:

- Set a host's own cap with the `set-account-limit` host action (`maxAccounts`, or null for the
  tier's cap again). It replaces the tier's cap for that host only. Host detail warns about a host
  at its cap, with a one-click "Raise cap to 1,000,000" (above every independent PDS today, below
  trusted's 10M).
- Accounts created throttled before the raise stay throttled until an operator lifts them
  (untakedown). indigo releases them.
- For a host you already know is real, raise its cap (or add a `tier` domain rule) before or right
  after adding it, so the first wave of accounts isn't throttled.

## When a throttled host falls behind

Since a host's limits block its reader, a host that sends more than its tier allows doesn't lose
events right away. It falls behind its own stream, and the backlog sits in the PDS's outbox. A PDS
keeps a bounded outbox per subscriber, and once the relay is past it the PDS sends
`ConsumerTooSlow` and closes the socket. The relay reconnects from its cursor, and whatever the
PDS no longer holds is gone. In the same run, eurosky.social was auto-throttled to 5 events/s and
2,500 an hour while it sent ~6/s. It was ~50 minutes behind after an hour and was cut off.

Each host's lag is the newest frame's age when it was read, plus the time since while the reader
is held back. A reader that has waited 10 s on an empty socket has everything the host sent, so a
quiet host reads 0, not the age of its last frame. It's on the Hosts page, in
`vlrelay_host_read_lag_max_seconds` and `vlrelay_hosts_lagging`. PDS clocks are in the number, so
it's good to seconds.

### Read-lag cases

A `read-lag` case says the host fell behind on its own, so the lag it looks at leaves out what the
relay did (`src/node/lag.rs`): time the reader sat in `backpressure` doesn't count, and neither
does a frame read soon after, since it's old because the relay held it. A case opens when a
host's lag is over `--lag-case-minutes` (10) and:

- the host hasn't been in `backpressure` for `--lag-case-grace-secs` (180)
- this node's in-flight caps and busiest lane have stayed under `--lag-case-pressure-pct` (50%)
  for the same grace
- the lag has stayed over the line for `--lag-case-sustain-secs` (120) without the reader
  catching up on it faster than a tenth of real time

Each trip notes the node's in-flight and lane fill in its evidence (`inflightFill`, `laneFill`).

The node that reads a host resolves its open (or acknowledged) read-lag cases once the host's lag
has been under the line for `--lag-case-resolve-secs` (600): status `resolved`, the note "resolved:
lag recovered" by `relay (service)`, and a `case` change event. It looks every minute, cases from
before it started included. Only that node knows the host's lag. Cases are shared objects written
by CAS, so a host changing owners at worst has two nodes try and the second find it closed. A
case an operator resolved or dismissed is never touched.

| Option | Keeps | Costs |
|---|---|---|
| Keep blocking (today, plus the case) | Every event the PDS still holds, and a spammer's flood stays slow | A busy host keeps falling behind until an operator raises its limits or the PDS cuts the relay off |
| Raise limits automatically past a lag | Freshness for real hosts | Defeats the throttle for exactly the hosts it's for, since a spam flood lags too |
| Skip to the live head | Freshness and the socket | Loses the backlog on purpose, for every account on the host |
| Drop frames over the limit | Freshness | Loses events while the host is still reachable, and consumers see gaps with no `#sync` |

Losing events is worse than being late, and only an operator knows whether a host is busy or
abusive. So the relay keeps blocking and opens a case that names the host and how far behind it
is. The fix is a tier or limit change on that host.

## Spam counting

Each threshold has a fixed-size Space-Saving table, `spam.trackHosts` keys per host rule (1,024)
and `spam.trackAccounts` per account rule (8,192), over a sliding window. A new key replaces the
lightest one and inherits its count as error, so any key above total ÷ capacity stays in the
table. Thresholds are checked against `count - error`, which never overstates a key, so churn
through thousands of quiet hosts can't trip a false positive. A key trips at most once per
window.

In the tests, 10,000 hosts plus five account farms kept the table at 1,024 hosts at most. All
seven tables together stay under 4 MiB, the five farms tripped once each, and no other host
tripped.

Actions are `alert` (log only), `case`, `throttle` and `throttle-and-case`. A per-account
threshold that throttles throttles the DID's host. Cases show up on the dashboard with their
evidence, and an operator clears them, suspends the host, bans the domain or takes down accounts.

## Takedowns

A takedown or its reversal is written to `policy/takedowns/` (an audit object per action and the
account's current state: who, when, why) before the account changes, and the relay emits an
`#account` frame with the new status.

It also filters the replay window. A consumer whose cursor reaches back past a takedown doesn't
get the account's earlier `#commit` and `#sync` frames, from the ring or from the bucket, on any
node. Its `#account` and `#identity` frames pass, so consumers see the status change. Frames are
skipped, not renumbered, so every node keeps the same seqs and consumers see a gap. Lifting the
takedown lets the old frames replay again. indigo doesn't filter replay at all, so after a reversal
both relays serve the same thing.

The leader makes the takedown an entry in the log, which sets the account record's flag and
carries the `#account`, so it commits like any event. Every node polls `policy/takedowns/` every
10 s for its replay filter. An empty filter does no parsing at all, and a non-empty one costs
~88 ns an event (about 1% of a core at 100k/s).

## Gaps

- The consumer limits in the policy (consumers per node, the slow-consumer cutoff, the backfill
  limit) aren't enforced from it yet. They come from the firehose's own defaults
  ([Subscribe to the firehose](subscribing.md#falling-behind)). Connections per IP isn't a policy
  knob: the firehose allows 256 from one address (IPv6: one /64), enough for a relay reading
  every `?shard=k/n` slice.
- A relay-throttled account stays throttled until an operator lifts it, even after its host drops
  below its cap or its cap is raised.
- Without `--plc-export`, a cold start resolves every account once at the PLC budget, about 31
  hours for 56M accounts at 500/s. With it, the leader reads the directory's export at
  `--plc-export-rate` (2 requests a second, ~14 hours for the history in 2026) and a seed fills
  a cache miss without a lookup. A forced refresh (an `#identity`, a signature that fails) still
  asks PLC. Every document the leader fetches is kept in the seeds too, so a restart doesn't
  resolve those accounts again.
- The account cap and the per-host new-account rate are counted on the leader and aren't in the
  bucket's host table, so they start over at a takeover.
- Peers aren't nudged after a save. They pick changes up within 10 s, and a takedown reaches other
  nodes' replay filters within one poll.
- The new-hosts counter is per UTC day. indigo uses a sliding 24 h window.
- Spam signals don't feed Prometheus yet.
