---
title: Shadow run
section: Testing
order: 203
summary: "Two hours and fifteen minutes against ten real PDSes, read-only, with a checker comparing vlRelay's firehose to the PDSes' own and to bsky.network the whole time."
---

```hero
diagram:
  caption: "One checker held one socket to each of the ten PDSes and compared both relays with them over the same arrivals. vlRelay ran on a bench box and subscribed to the same ten hosts. The production relay carries the whole network, so only events from these hosts counted on its side."
  nodes:
    - { id: pds, label: ten real PDSes, sub: "6 Bluesky · 4 independent", at: [0, 5], size: [10, 3], tone: muted, stack: true }
    - { id: vl, label: vlRelay, sub: "read-only · not announced", at: [16, 0], size: [10, 3], tone: accent }
    - { id: prod, label: bsky.network, sub: the production relay, at: [16, 10], size: [10, 3], tone: muted }
    - { id: check, label: "`e2e_check --separate`", sub: one socket per PDS, at: [32, 5], size: [11, 3], tone: solid }
  edges:
    - { from: pds.t, to: vl.l, label: subscribeRepos, via: [[5, 1.5]] }
    - { from: pds.b, to: prod.l, via: [[5, 11.5]] }
    - "pds -> check: PDS streams"
    - { from: vl.r, to: check.t, label: firehose, via: [[37.5, 1.5]], tone: blue }
    - { from: prod.r, to: check.b, label: firehose, via: [[37.5, 11.5]], tone: blue }
facts:
  - { value: "0", label: commits missing on the Bluesky PDSes, note: "in every window that counted per host", tone: accent }
  - { value: "40-50", unit: ms, label: time to firehose p50, note: "production ~100 ms over the same arrivals", tone: blue }
  - { value: "~60", unit: events/s, label: in, note: "about a fifth of the network", tone: muted }
  - { value: "2 h 15", unit: min, label: of real traffic, note: "90 min one node, 45 min three", tone: violet }
```

vlRelay ran for 2 h 15 min on 2026-10-04 (14:09-16:25 UTC, a Sunday morning in the US) against
ten real PDSes, read-only. One checker compared its firehose with the PDSes' own and with
`wss://bsky.network` the whole time. The first 90 minutes were one node on MinIO with the default
policy. The last 45 were three nodes on one box, with every host trusted.

> [!NOTE]
> This run predates the quorum log. It ran on vlRelay's two earlier designs: a single node that
> wrote its own log to the bucket in 25 ms segments (`--linger-ms`), and the lease cluster, where
> each node owned DID shards through bucket leases. Both are deleted. The findings about real PDSes
> and the production relay still hold, and so do the policy bugs. The latencies, PUT rates and
> per-node numbers are those designs'.

What it found:

- On the six Bluesky PDSes, vlRelay matched production event for event. There were 0 commits
  missing in each of the seven windows where the checker counted misses per host, and in all nine
  windows 0 reordered, 0 duplicates and 0 rev or seq regressions. Its time to firehose was p50
  40-50 ms against production's ~100 ms, except while other benches on the same box slowed MinIO
  (p50 up to ~120 ms).
- The default policy throttled three of the four independent PDSes within ten minutes (tngl.sh is
  too quiet to trip it), and vlRelay dropped most of their traffic for the rest of the single-node
  run. That relay counted every account it hadn't seen as a new account. It was the biggest
  problem the run found, and it was in the policy, not the pipeline. Each piece is fixed or has an
  operator path now ([Bugs](#bugs)).
- With those hosts trusted (the cluster phase), vlRelay matched production on all ten hosts. It
  carried every `#identity` handle production strips, and it dropped the same eurosky `#sync`
  events production drops, for a reason the reference notes had wrong.
- Nothing leaked that we could see. RSS rose to ~1.1 GB, most of it the 512 MB firehose ring
  filling. The bucket levelled off at ~85k objects once the 1 h retention started deleting. File
  descriptors stayed at 37-45.

## Setup

| | |
|---|---|
| Build | `dev-release` on a 32-thread bench box, with the `e2e_check` changes below |
| Bucket | MinIO in Docker on the same box, `--memory 4g`, data on local disk, 127.0.0.1 only |
| Node | `--retention 1` (hours), `--linger-ms 25`, default lanes, shards and identity budget (50 lookups/s), in a systemd user unit with `MemoryMax=8G`. These are the deleted design's flags. |
| Cluster | three nodes on the same box and prefix, `--cluster`, peer mTLS from `vlpds admin tls` (127.0.0.1), serve :3261-3263, peer :3271-3273, 64 host shards, 4 DID shards |
| Admin, metrics | 127.0.0.1 only, reached through an ssh tunnel |
| Checker | one `e2e_check --separate` per 15-minute window (870 s plus 30 s settle, 30 s warmup), vlRelay with `--relay-scope all`, `wss://bsky.network` with `--relay-scope seen` |
| Sampler | every 60 s: each node's `/metrics`, RSS, fds and CPU ticks from `/proc`, the admin API's host count. Every 5 min: `mc ls -r` and `mc du` of the prefix |

Hosts (one socket each from vlRelay and one from the checker):

| Host | Kind | Accounts (listHosts) | Tier, single node | Tier, cluster |
|---|---|---|---|---|
| amanita.us-east.host.bsky.network | mushroom | 213k | trusted | trusted |
| morel.us-east.host.bsky.network | mushroom | 211k | trusted | trusted |
| shiitake.us-east.host.bsky.network | mushroom | 214k | trusted | trusted |
| jellybaby.us-east.host.bsky.network | mushroom | 1.14M | trusted | trusted |
| stropharia.us-west.host.bsky.network | mushroom | 1.14M | trusted | trusted |
| cordyceps.us-west.host.bsky.network | mushroom | 189k | trusted | trusted |
| eurosky.social | reference PDS, Europe | 36k | default | trusted |
| blacksky.app | its own PDS implementation | 42k | default | trusted |
| atproto.brid.gy | Bridgy Fed (Arroba) | 65k | default | trusted |
| tngl.sh | Tangled | 12k | default | trusted |

Traffic was ~60 events/s in, about a fifth of the network. Nothing called `requestCrawl` and the
relay wasn't announced anywhere. The tiers came from `--host-tier`: a first start with the six
mushrooms and `--host-tier trusted`, then a restart with all ten and `--host-tier default` (a
host keeps the tier it was first seen with).

The cluster ran with every host trusted because by the end of the first window the default policy
had throttled three of the four independent hosts (below). So the single-node run said nothing
about how vlRelay handles their frames, and trusting them for the cluster phase got that answer.

### Checker changes

`e2e_check` needed a few things for this:

- `--separate` compares each `--relay` with the upstreams on its own over one set of PDS sockets.
  Without it, the shadow run would have needed one checker per relay, so three sockets to each PDS
  from the bench box instead of two. `scripts/prodcmp.sh` uses it now.
- With `--scope seen`, a relay event for a DID no PDS socket has named yet is held for up to 60 s
  and matched when the PDS copy arrives. Before, it counted out of scope and its PDS copy then
  counted missing. That was the ~60 "missing" production commits per 10 minutes in
  [Development](devloop.md#against-real-pdses), and here production shows 0.
- The JSON report has per-upstream latency and missing counts, and lists every missing or extra
  `#sync`, `#identity` and `#account`.

## Results

### Single node, default policy

Commits are upstream / matched / missing / extra. Latency is from the checker's PDS socket to the
relay's socket, both on the bench box, over every matched event.

| Window (UTC) | vlRelay commits | Production commits | vlRelay p50 / p90 / p99 | Production p50 / p90 / p99 | Rejects |
|---|---|---|---|---|---|
| 14:09 | 56,177 / 50,747 / 5,165 / 0 | 56,177 / 56,173 / 0 / 0 | 38 / 113 / 9,650 ms | 97 / 168 / 319 ms | 3,416 `inactive` |
| 14:24 | 56,712 / 48,653 / 7,565 / 121 | 56,712 / 56,711 / 0 / 0 | 57 / 155 / 342 ms | 98 / 172 / 344 ms | 3,274 `inactive` |
| 14:39 | 56,170 / 48,194 / 7,426 / 155 | 56,170 / 56,168 / 0 / 0 | 51 / 161 / 400 ms | 100 / 174 / 1,023 ms | 1,848 `inactive` |
| 14:54 | 57,249 / 49,399 / 7,374 / 440 | 57,249 / 57,244 / 0 / 0 | 98 / 198 / 364 ms | 98 / 169 / 315 ms | 2,430 `inactive`, 1 `commit_rev_mismatch` |
| 15:09 | 54,800 / 47,385 / 6,904 / 785 | 54,800 / 54,791 / 0 / 0 | 120 / 232 / 361 ms | 99 / 168 / 325 ms | 5,540 `inactive`, 3 `commit_rev_mismatch` |
| 15:24 | 57,829 / 50,131 / 7,139 / 133 | 57,829 / 57,827 / 0 / 0 | 41 / 171 / 340 ms | 99 / 168 / 311 ms | 1,784 `inactive` |

No window had a reordered event, a duplicate, a rev regression or a relay seq regression, on
either relay.

Every vlRelay miss and every extra is from the three hosts the policy throttled. Per host, from
the 14:39 window on (when the checker started keeping per-upstream numbers):

| Host | vlRelay missing, per window | vlRelay p50 / p99 | Production p50 / p99 |
|---|---|---|---|
| the six mushrooms | 0, 0, 0, 0 | 39-122 / 296-452 ms | 95-102 / 196-1,162 ms |
| eurosky.social | 4,724, 4,756, 4,504, 4,705 (all of them) | | 80-102 / 405-666 ms |
| blacksky.app | 1,952, 1,729, 1,578, 1,717 | minutes late | 91-95 / 187-1,188 ms |
| atproto.brid.gy | 753, 903, 837, 728 | 42-158 / 179-29,082 ms | 107-117 / 199-642 ms |
| tngl.sh | 0 | 79-124 ms (a handful of events) | 87-171 ms |

- The missing commits are accounts the policy created throttled (`relay_throttled`, the host past
  `maxAccounts` 100) and the backlog of the throttled hosts. A throttled host is held to 5 events/s
  and 2,500 an hour by blocking its reader, and eurosky alone sends ~6/s, so its reader fell
  further behind every minute. By the end vlRelay was ~50 minutes behind eurosky, and at 15:10
  eurosky closed the socket with `ConsumerTooSlow`.
- The extra commits are the same backlog arriving in a later window. Their revs decode to times in
  the previous window, so the checker saw their PDS copies before the window opened.
- The p99 of 9.65 s and the maxima of 6-12 minutes are those delayed events too.
- The `inactive` rejects are the throttled accounts' commits. The dashboard showed them as "taken
  down" ([Bugs](#bugs), fixed).
- The `commit_rev_mismatch` rejects are eurosky's post-migration `#sync` (below).

### Three nodes, every host trusted

| Window (UTC) | vlRelay commits | Production commits | vlRelay p50 / p90 / p99 | Production p50 / p90 / p99 | Rejects |
|---|---|---|---|---|---|
| 15:40 | 62,692 / 62,688 / 0 / 1 | 62,692 / 62,685 / 0 / 0 | 44 / 148 / 256 ms | 99 / 169 / 310 ms | 1 `commit_rev_mismatch` |
| 15:55 | 60,286 / 60,279 / 0 / 0 | 60,286 / 60,274 / 0 / 0 | 48 / 161 / 296 ms | 99 / 170 / 326 ms | 1 `commit_rev_mismatch` |
| 16:10 | 60,101 / 60,078 / 0 / 0 | 60,101 / 60,070 / 0 / 0 | 68 / 237 / 933 ms | 102 / 180 / 1,995 ms | none |

All ten hosts had 0 commits missing in any window, with no reordering, duplicates or regressions.
The gap between the upstream and matched counts, on both relays, is the window's edges: PDS events
whose relay copy came after the checker stopped, which it doesn't count as missing. The last
window's tails (vlRelay p99 933 ms, production 2.0 s) came from the PDSes. shiitake's p99 was
1.45 s on vlRelay and 2.96 s on production in the same window.

The checker read n1, which subscribed to six of the hosts. n2 had cordyceps and eurosky, and n3 had
amanita and morel. n3 owned no DID shards (4 DID shards over 3 nodes went 2/2/0), so every event n3
read went to n1 or n2 over the peer port before it reached the stream. Per host in the first
cluster window, vlRelay's p50 was 40-58 ms on all nine hosts with traffic (production 94-109 ms),
and its p99 161-373 ms (production 187-426 ms).

The one extra commit is from tngl.sh, which resets idle sockets every few minutes. The checker's
socket reconnected without a cursor (it hadn't seen a tngl.sh event yet) and missed one commit
that vlRelay, resuming with its cursor, didn't.

A separate 7-minute check with n1's stream as the "upstream" and n2's and n3's as the relays found
the three streams identical: 29,040 commits, 2 `#sync`, 2 `#identity` and 3 `#account` on each,
with 0 missing, extra, reordered or duplicated, p50 under 1.3 ms and max 28 ms between nodes. The
nodes forwarded every event they read for a DID they didn't own, and there were no forwarding
errors until we stopped all three at once at the end. Each then gave up on its in-flight forwards
after 20 s, which is why stopping the cluster took two minutes.

### Sync, identity and account events, against production

Every difference from the PDSes' own streams, over all nine windows (103 `#identity`, 71 `#sync`,
141 `#account` at the PDSes):

| | vlRelay | Production |
|---|---|---|
| `#identity` with a handle | carried as sent, every one it received | stripped on all 87 that had one |
| `#identity`, `#account`, `#sync` from the throttled hosts | missing or late (single node only: 66 diffs, every DID on eurosky, blacksky or brid.gy by PLC) | |
| eurosky `#sync` after an account migrates in | dropped (`commit_rev_mismatch`, the 6 that reached verification) | dropped (11 of 11) |
| `#account` | otherwise identical | identical |

- Handles. Production strips the handle from `#identity` unless it resolves (the
  `SkipHandleVerification` quirk in [Reference notes](reference-notes.md#identity-handles)).
  vlRelay forwards it when it matches the DID document's `alsoKnownAs`. So every `#identity` with
  a handle shows as a "missing" `i:<handle>` plus an "extra" `i:-` on production's side.
- eurosky's migration `#sync`. Every account that moved to eurosky during the run produced
  `#identity`, `#account` and then a `#sync`, and both relays dropped all of those `#sync` events.
  We captured one (`verify_bench hunt --cursor`, eurosky seq 68027536). The event's `rev` is
  `3mx2ltq3irc2r`, which is what eurosky's `getLatestCommit` reports, but the signed commit in its
  blocks says `3mwig7nq4qn2i`, the rev the account had on its old PDS. The root CID is the same
  commit. So the reference PDS gives the imported repo a new rev in its database without signing a
  new commit, and puts the database rev on the `#sync`. vlRelay's `evt.rev == commit.rev` check
  drops it, and so does indigo's (`VerifyRepoSync` checks DID and rev). [Reference
  notes](reference-notes.md#missing-extra-and-out-of-order-events) put production's missing
  eurosky `#sync` events down to a PLC race. This is the likelier cause, and it isn't vlRelay's to
  fix, since the account's next commit is signed with the right rev and goes through.

### Load, latency and the bucket

| Window | RSS | fds | CPU | Durable lag (mean, max of 1-min samples) | PUT/s | Identity cache | Throttled accounts |
|---|---|---|---|---|---|---|---|
| 14:09 | 521 MB | 37 | 6.8% of a core | 34, 125 ms | 20.2 | 11,705 | 1,076 |
| 14:24 | 876 MB | 42 | 7.3% | 56, 172 ms | 19.7 | 19,051 | 1,631 |
| 14:39 | 953 MB | 42 | 6.4% | 75, 220 ms | 19.3 | 25,446 | 2,027 |
| 14:54 | 1,004 MB | 42 | 8.1% | 89, 165 ms | 19.8 | 31,305 | 2,438 |
| 15:09 | 1,063 MB | 42 | 8.3% | 114, 210 ms | 19.6 | 37,124 | 3,192 |
| 15:24 | 1,108 MB | 37 | 6.6% | 53, 163 ms | 19.7 | 41,821 | 3,508 |
| Cluster 15:40 (n1 / n2 / n3) | 539 / 532 / 475 MB | 50 / 52 / 48 | 6.3 / 5.5 / 3.8% | 31 / 33 / 0 ms | 34.8 (all) | 9,184 / 7,219 / 5,277 | |
| Cluster 15:55 | 802 / 789 / 707 MB | 47 / 39 / 40 | 7.7 / 6.7 / 4.9% | 44 / 41 / 0 ms | 34.0 | 15,443 / 12,216 / 8,956 | |
| Cluster 16:10 | 840 / 824 / 717 MB | 47 / 42 / 39 | 8.4 / 7.3 / 5.3% | 68 / 73 / 0 ms | 33.6 | 20,328 / 15,984 / 11,795 | |

n3 has no durable lag because it owned no DID shards and so wrote nothing to a log of its own.
Each node kept its own identity cache, and a DID's document ended up in the cache of every node
that read one of its events or owned its shard. After 45 minutes the three nodes held ~48k
documents between them, where the single node had held ~31k at the same point.

CPU per event over the single-node run (`vlrelay_stage_busy_us_total` over 313k frames,
dev-release on a busy box): parse 18 µs, verify 79 µs, apply 138 µs. The identity stage averaged
9 ms of wall time per event, nearly all of it the first lookup of each DID.

The bench box wasn't quiet. Other benchmarks ran their own MinIOs and fakepds fleets on it during
the run. When we looked at 15:10 and 15:25 the load average was 32-40 on 32 threads and
`/proc/pressure/io` showed "full" at 10-21%. Segment PUTs to our MinIO went from ~8 ms to
50-135 ms in episodes of 5-10 minutes, and the durable lag and vlRelay's p50 followed them (the
14:54 and 15:09 windows). In the quiet stretches vlRelay's p50 was 40-50 ms, which is the 25 ms
linger plus a PUT plus the hop to the checker.

PUTs ran at ~20/s on one node for ~60 events/s, and ~35/s across three nodes. At this rate a
segment held 3 events. That was the linger doing its job, and it set the old design's cost floor
for a quiet relay: ~1.7M PUTs a day per node, ~$8.50 a day at S3's request price, $0 on R2. The
quorum log replaced it with a flush every 30 s ([Cost](cost.md)).

## Leaks and growth

| What | Start | After 90 min | Verdict |
|---|---|---|---|
| RSS | 99 MB | 1,108 MB | The 512 MB firehose ring filled in the first ~25 min. After that RSS grew ~3.5 MB/min, from 897 MB to 1,108 MB in the last hour |
| fds | 47 | 37-45 | flat |
| Bucket objects | 590 | 82-86k from 15:10 on | flat once retention ran: the first pass at 15:09 deleted 158 segments, and the prefix stayed at 860-910 MiB |
| Identity cache | 643 | 41,821 | grows with every new DID, ~23k an hour here. Bounded at 2^20 entries |
| Host registry | 10 | 10 | flat |
| MinIO | 181 MB | 726 MB of 4 GB | levelled off |

The ring wasn't configurable then. It's `--ring-mb` now, still 512 by default.

The RSS growth after the ring filled is the one thing to watch. The identity cache accounts for
some of it. Its entries expired after an hour but were only dropped when the cache was full
(`IdentityCache::store`), so on the whole network it would have held ~1M documents before it
cleaned anything. It sweeps expired entries every minute now, so it holds the DIDs seen within the
hour. At ~390 new documents a minute here that's well under 1 MB of the ~3.5 MB a minute, and we
couldn't attribute the rest without a heap profile. It looked linear for the hour we saw (and ~2.5
MB a minute per node in the cluster's last window), so a longer run with jemalloc's stats or
`heaptrack` is the next step.

`vlrelay_identity_cache_entries` was added for this run.

## Bugs

Each bug the run found, and where it stands now.

1. The dashboard called every inactive account's reject "taken down". `reject_class` mapped the
   state step's `inactive` (deactivated, suspended, throttled, deleted or taken down) to
   `Takendown`, so the overview showed 3-5 takedowns a second on a relay that had none. Fixed: it's
   its own class, "inactive account", with a regression test.

2. A fresh relay throttles every big host it doesn't trust. The account gate treated the first
   event from a DID as a new account. On a relay that has just started, or has just added a host,
   that's every active account. On default-tier hosts:
   - `maxAccounts` (100) throttled 3,508 real, long-lived accounts within 90 minutes. A
     relay-throttled account stays throttled until an operator lifts it ([Policy](policy.md#gaps)).
   - The `new-accounts` spam threshold (300 an hour) tripped on eurosky.social at 14:12, three
     minutes in, and on atproto.brid.gy and blacksky.app soon after. The driver auto-throttled all
     three to 5 events/s and 2,500 an hour, which they can't live inside, and they never recovered,
     since every minute brought more "new" accounts.
   - On trusted hosts it can't throttle, so it opened a high-severity case instead: 8 open cases on
     the single node and 7 on the cluster (all six mushrooms and eurosky), every one a false
     positive.

   The spam rule and the new-account rates are fixed. They count only newly created repos (no
   `since`, no `prevData`). The cap is unchanged on purpose. It's indigo's, and indigo has the same
   first-wave problem, which bsky.network handles by raising each big host's limit. vlRelay now has
   the same per-host override (`set-account-limit`), a warning on host detail for a host at its cap
   with a one-click raise to 1,000,000, and the accounts column marked on the Hosts list.
   [Policy](policy.md#big-independent-pdses) has the operator notes. Accounts throttled before a
   raise still stay throttled until an untakedown, where indigo releases them.

3. A throttled host falls behind without limit, and nothing showed it. The tier's hourly bucket
   blocks the reader, which is right for a spammer but turns into an ever-growing backlog for a
   host that's legitimately above the rate. eurosky was ~50 minutes behind when it cut vlRelay off
   with `ConsumerTooSlow`, and after that the events between the reconnect cursor and the PDS's own
   window are gone. The dashboard's lag column said 0. Each host's lag is now measured from its
   events' `time` (Hosts, host detail, `vlrelay_host_read_lag_max_seconds`,
   `vlrelay_hosts_lagging`), and a reader more than 10 minutes behind opens a `read-lag` case. What
   else the relay could do, and why it doesn't, is in
   [Policy](policy.md#when-a-throttled-host-falls-behind).

4. Cluster views on the dashboard were per node. On n1, the hosts other nodes read showed as `idle`
   with node `n1`, the Consumers page listed only n1's sockets, and the Cluster page showed 0
   events/s, 0 B and 0 cores for n2 and n3.
   This was fixed in the lease cluster by aggregating across nodes. On the quorum log the
   dashboard's cluster view asks every member for its status.

5. 4 DID shards over 3 nodes went 2/2/0, so n3 owned none and forwarded everything it read. The
   lease cluster's fix was 24 DID shards by default. The quorum log has no DID shards: the leader
   checks every event, so this no longer applies.

6. The merger dropped a late event at join. n2 and n3 each logged `firehose merger: dropped late
   events below the emitted watermark late=1` a second after they started. Nothing was subscribed
   to them yet, and the node comparison later found the three streams identical. That merge went
   with the lease cluster. On the quorum log every node emits the leader's seqs.

Not a vlRelay bug: eurosky's migration `#sync`
([above](#sync-identity-and-account-events-against-production)).

## Dashboard screenshots

The run's screenshots were of the earlier designs' dashboard, and they've left the repository
with it. The README's screenshots are today's console on a local three-node cluster reading a
[`fakepds`](loadfleet.md) fleet.

## Running one yourself

The run's scripts were throwaway, but the checker is the part that matters, and it works the same
on today's relay. Start a relay on a bucket with the hosts you want to compare, at the tier you
want to test:

```sh
vlrelay --listen 127.0.0.1:3280 --s3-endpoint ... --s3-bucket ... --prefix shadow \
    --host-tier default --host amanita.us-east.host.bsky.network --host eurosky.social ...
```

Then compare it and production with the same PDSes, one window at a time:

```sh
e2e_check --upstream amanita.us-east.host.bsky.network --upstream eurosky.social ... --separate \
    --relay http://127.0.0.1:3280 --relay-scope all --relay wss://bsky.network --relay-scope seen \
    --warmup 30 --duration 870 --settle 30 --report-only --json-out cmp.json
```

`scripts/prodcmp.sh` does a 10-minute version against three PDSes with an in-memory relay
([Development](devloop.md#against-real-pdses)).
