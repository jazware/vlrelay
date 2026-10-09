---
title: Compatibility
section: Reference
order: 301
summary: "indigo's Go consumers, goat, @atproto/sync, Jetstream and indigo's relay itself, run against vlRelay beside indigo's relay on the same upstreams, with every difference classified."
---

```hero
diagram:
  caption: The compat run. Both relays subscribe to the same three upstreams under the same load, every consumer reads both, and a checker compares the two streams event by event.
  nodes:
    - { id: up, label: Upstreams, sub: 2 vlpds + the reference PDS, at: [0, 4], size: [10, 3], tone: muted, stack: true }
    - { id: vl, label: vlRelay, sub: ":3480", at: [14, 1], size: [8, 3], tone: accent }
    - { id: ind, label: indigo relay, sub: ":3470 · sqlite", at: [14, 7], size: [8, 3], tone: muted }
    - { id: go, label: indigo consumer, sub: "+ sync 1.1 verifier", at: [27, 0], size: [10, 2.6], tone: blue }
    - { id: ts, label: "`@atproto/sync`", sub: Firehose 0.4.13, at: [27, 3.4], size: [10, 2.6], tone: blue }
    - { id: goat, label: goat firehose, sub: "--verify-sig --verify-mst", at: [27, 6.8], size: [10, 2.6], tone: blue }
    - { id: js, label: Jetstream, sub: restarted halfway, at: [27, 10.2], size: [10, 2.6], tone: blue }
  edges:
    - "up.r -> vl.l: same load"
    - "up.r -> ind.l"
    - vl.r -> go.l
    - vl.r -> ts.l
    - vl.r -> goat.l
    - vl.r -> js.l
    - { from: ind.r, to: ts.l, dash: true }
facts:
  - { value: "1,861", unit: events, label: per relay, note: "60 s at 30 writes/s; every frame type", tone: blue }
  - { value: "0", label: verifier failures, note: "on 1,815 commits and syncs, and goat had no warnings", tone: accent }
  - { value: "=", label: seqs on both relays, note: "the same run's takedown frames are 1859–1861 on both", tone: violet }
  - { value: "~29", unit: ms, label: p50 time to firehose, note: "indigo's relay: 46–60 ms on the same run", tone: amber }
```

Does software written for Bluesky's relay work against vlRelay unchanged? To find out, we ran indigo's Go consumers, `goat`, `@atproto/sync`, Jetstream and indigo's relay itself against vlRelay on the local dev network, with indigo's relay beside it on the same upstreams and load, and compared the two.

Everything works, `@atproto/sync` included. vlRelay's stream matches indigo's event for event, and on the same run it matches seq for seq. The first run found one thing that broke: TypeScript. vlRelay's seqs were bigger than 2^53, and `@atproto/sync` rejected every frame. vlRelay serves dense seqs, which the leader assigns at commit ([Cluster](cluster.md#the-path-of-an-event)).

## How to run it

```
just compat                      # tests/compat/run.sh --secs 60 --rate 30
tests/compat/run.sh --keep       # leave the network and both relays up afterwards
```

`run.sh` clones and builds indigo, goat, jetstream and jetstream-legacy into `tests/compat/scratch/` (ignored) the first time. It needs Go, Node 22+ (it uses Homebrew's `node` if the default one is older) and docker. Then it:

1. Brings up the dev network on the 34xx ports (`tests/compat/env.sh`, compose project `vlrelay-compat`), so it can sit beside a default dev network. It seeds 30 accounts.
2. Starts vlRelay on :3480 (`--memory`), a second vlRelay on :3478 with dev-only 10 s retention and `--max-lag-mb 1` (so `OutdatedCursor` and `ConsumerTooSlow` can be reached), and indigo's relay on :3470 (sqlite, `--lenient-sync-validation` as in production, account limit 10,000) on the same three upstreams. It also starts jetstream-legacy on :3460 with vlRelay as its upstream.
3. Starts consumers on both relays, runs `devnet load` for 60 s at 30 writes/s (handle changes and deactivations included), restarts Jetstream halfway, and waits.
4. Tests cursors, account states, the sync API and relay chaining, then writes `scratch/run/summary.txt`.

The pieces run on their own too. `net.sh` starts the network, either relay, a chained relay or the load. `ts.sh` runs the TypeScript consumer, `scratch/bin/gocheck` (built from `gocheck/`) runs indigo's consumer, and `syncdiff.py` and `states.py` cover the sync API.

Two dev-network details matter here:

- indigo's relay only accepts a port on the name `localhost`, and it compares the DID document's PDS hostname exactly. So `dev/up.sh` now takes `DEV_PDS_HOST`, and the compat network sets it to `localhost` so the vlpds upstreams advertise `http://localhost:PORT`.
- indigo's `requestCrawl` checks `describeServer` through a client that only reaches public IPs, so it can't add a loopback host, admin or not. `net.sh indigo` writes the host rows into indigo's database instead and restarts it. The relay resubscribes to every `active` host at startup, and it dials a no-SSL `localhost` host with a plain dialer.

## Results

Run on 2026-10-04 on a laptop (dev build): 60 s at 30 writes/s, 30 accounts on two vlpds upstreams and the reference PDS, 1,861 events per relay (1,794 `#commit`, 21 `#sync`, 24 `#identity`, 22 `#account`).

| Software | Feature | vlRelay | indigo relay |
|---|---|---|---|
| indigo `events.HandleRepoStream` (`gocheck`) | decode every frame type, seq order | pass: 1,861 frames, 0 seq regressions | pass |
| | cursor resume from the middle of the run | pass: exactly the events after the cursor | pass |
| | cursor past the head | `FutureCursor` error frame, then close 1000 | nothing, the socket stays open on live (indigo#1328) |
| | `cursor=1` | replays the window, no `#info` (nothing pruned yet) | replays the window |
| | `#info OutdatedCursor` (`cursor=1` on the 10 s window relay) | pass: the info frame, then the stream from the oldest checkpoint left (mid-load: seq 425 onward; after the load, nothing older than 10 s is left) | |
| | `ConsumerTooSlow` (`slow.py` stops reading on the 1 MiB lag relay) | pass: the error frame after the backlog, then close | |
| sync 1.1 verifier on top of indigo's `atproto/repo` (`gocheck --verify`) | CAR, structure, signature, MST inversion, `prevData` chain, `since`, rev order | pass: 0 failures on 1,815 commits and syncs | pass: 0 failures |
| `goat firehose --verify-basic --verify-sig --verify-mst` | everything goat checks | pass: no warnings | pass: no warnings |
| `@atproto/sync` `Firehose` (0.4.13) | lexicon validation, signatures, MST proofs, records | pass with dense seqs: 1,875 events, 0 errors, every seq a JS `number` (first run: **fail on every frame**, `Expected integer value type (got 458526062626109184n) at $.seq`) | pass: 1,875 events, 0 errors |
| | seqs | identical to indigo's on the same run (the takedown test's `#account` frames are seq 1859, 1860 and 1861 on both) | |
| Jetstream (jetstream-legacy), restarted halfway | ingest, its JSON output, resume from its saved relay cursor | pass: 1,794 unique commits out, the same as the relay emitted. Nothing dropped across the restart | not run |
| Jetstream (current `bluesky-social/jetstream`) | bootstrap and live | blocked: its backfill reaches PDSes only through an SSRF-hardened client, so it never leaves bootstrap on a loopback network. Its live socket connected (cursor 0) | not run |
| `e2e_check` against the upstreams | set, per-DID order, dups, latency | pass: 0 missing, extra, reordered or duplicated. p50 29 ms, p99 65 ms | handles differ (below). p50 46-60 ms, p99 105 ms |
| `e2e_check` with indigo's stream as the reference | the two relays' outputs | identical `#commit`, `#sync` and `#account` sets and per-DID order. vlRelay was first on 1,286 of 1,806 | |
| sync API (`syncdiff.py`, every account and host) | `listRepos`, `getRepoStatus`, `getLatestCommit`, `listHosts`, `getHostStatus` | field for field identical for active accounts and every host. Differences below | |
| `#account` on deactivation and takedown (`states.py`) | frame fields | identical (`active: false`, `status: deactivated` / `takendown`, then `active: true`) | |
| relay chaining: indigo's relay with vlRelay as a host | | refused: indigo bans the host on our `Server: … (atproto-relay)` | |
| relay chaining: vlRelay with indigo's relay as `--host` | | was: subscribed, rejected all 97 events as `wrong_host`. Now refused at connect and banned, as indigo does (`listHosts` says `banned`, and the ban is on the audit log `by: vlrelay`), and never dialed again | |

### Every difference, classified

### Stream

| Difference | Class | Notes |
|---|---|---|
| seqs above 2^53 (`unix_micros << 8 \| writer`, ~4.6e17) | was ours wrong, fixed | It broke every JavaScript consumer that validates (all of `@atproto/sync`). vlRelay serves dense seqs, the same on every node. vlpds as a PDS still emits the time-based seqs, since it's in production with stored cursors. |
| `#identity` keeps the handle, indigo strips it | deliberate | `SkipHandleVerification` makes indigo drop nearly every handle. 24 of 24 handles kept on vlRelay, 0 of 24 on indigo. `e2e_check` keys identity on the handle, so these show as 7-9 "missing" and "extra" on the indigo side. |
| `FutureCursor` error frame on a future cursor | deliberate | Per the event-stream spec. indigo ignores the cursor and serves live (indigo#1328). indigo's consumer surfaces the frame through its `Error` callback, and the socket closes normally. |
| vlRelay emits sooner | deliberate | p50 29 vs 46-60 ms. indigo writes to disk every 100 ms before it broadcasts. |

### Sync API

| Difference | Class | Notes |
|---|---|---|
| `RepoNotFound` and `HostNotFound` are HTTP 400, indigo 404 | deliberate | 400 is what the PDSes return for `RepoNotFound`. Jetstream's client goes by the error name (`isRepoNotFoundError`), not the status. |
| `getLatestCommit` errors are 400, indigo 403 | deliberate | same reason |
| Bad params give `InvalidRequest`, indigo `BadRequest` | deliberate | `InvalidRequest` is the XRPC spec's generic name |
| `listRepos` lists deactivated and taken-down accounts with `active: false` and `status`. indigo leaves them out | deliberate | The lexicon has `active` and `status` for this. indigo can't list them. |
| `getRepoStatus` leaves out `rev` while inactive. indigo sends it | deliberate | The lexicon: rev "if active=true" |
| `listRepos?limit=1` sets a cursor. indigo never pages a 1-item page | theirs wrong | |
| `getLatestCommit` on a deleted or other inactive account gives `RepoNotFound`. indigo sends the non-lexicon `RepoDeleted` / `RepoInactive` | deliberate | not hit in this run |
| `getHostStatus.seq` off by one for a moment | timing | Both relays flush cursors on a timer, so a snapshot mid-traffic differs. Identical once quiet. |

### Relay chaining

- indigo's relay ← vlRelay. Refused by design, twice over. The first dial sees `Server: vlrelay/0.1.0 (atproto-relay)` and the host goes to `banned`. Even without the ban, `CreateAccountHost` would refuse every account, since each DID document names a PDS and not us. indigo has no relay-upstream mode.
- vlRelay ← indigo's relay. Before this work, vlRelay subscribed (it never looked at the upstream's `Server` header, although the must-match list says to), took 97 events in and rejected all 97 as `wrong_host`. `#identity` was rejected too: vlRelay checks host authority on `#identity`, and indigo doesn't. Fixed in `upstream/client.rs`: an upstream whose handshake `Server` contains `atproto-relay` is refused at connect, which covers `--host` and `requestCrawl` (crawl runs the same connect). Regression test: `upstream::client::tests::refuses_an_upstream_that_says_its_a_relay`.

Should vlRelay support relay-as-upstream? Not as an upstream mode. Host authority ([Cluster](cluster.md#the-path-of-an-event)) is what keeps a host from speaking for accounts it doesn't hold, and a relay upstream is exactly that, for every account. To support it, we'd need to trust the relay for authority (signatures prove who wrote a commit, not that it's the newest), to re-check every `#account` and `#identity` at the PDS (they aren't signed), and to dedupe against the same accounts arriving directly. The original design's optional "other relays" input is better served by the two cheaper things it's really for:

- Bootstrap a host list by reading another relay's `listHosts` and subscribing to those PDSes directly. This needs no trust in the relay, and it's built: `--bootstrap-relay` and the policy's `discovery.seedRelays` read the list and never ask the other relay to crawl anything ([Policy](policy.md#discovering-hosts)).
- Mirror a vlRelay, which every member of a cluster already does.

### Host authority

indigo checks the account's host on `#commit`, `#sync` and `#account`, and passes `#identity` from any host. On a mismatch it purges the DID document and resolves it again, on every mismatched event. `#identity` purges it too. vlRelay checks the same events, and re-resolves a mismatch at most once per DID per 30 s, so a host sending events for accounts it doesn't hold can't spend PLC lookups per event.

- Migration inside that window: was ours wrong, fixed. The new PDS sends `#identity` when it creates the account (the document still names the old PDS, and vlRelay looks it up), then again after the PLC op, then `#account` active and its first commit. A quick migration does all that within 30 s, so vlRelay trusted the first lookup: it dropped the new PDS's `#account` and commits as `wrong_host`, and took the old PDS's `#account` deactivated as the account's, which indigo rejects. Now an `#identity` from another host inside the window marks the document stale, paid from the sender's lookup budget, and the next event resolves it again. Regression test: `state::tests::migration_inside_the_reresolve_window_follows_the_new_identity`.
- One PDS under several hostnames: ours reads it once. A PDS whose name, wildcard certificate and DNS answer for other names shows up in `listHosts` under each of them, and a relay subscribes to each. Every name streams the same events, and the DID documents name one of them. The copies from the other names are `wrong_host` when they win the race to the relay, and duplicates when they lose it, and each costs a DID document fetch per account per 30 s. indigo reads every name. On a production relay, these copies were most of its `wrong_host` rejects and, by estimate, close to half of its DID document fetches. vlRelay's leader now watches a pair once one's event names the other, and when they send the same events at the same seqs and answer `describeServer` with the same service DID, it marks the sender an alias: its socket closes, and either name speaks for the other's accounts ([Policy](policy.md#host-aliases)). `listHosts` leaves aliases out and `getHostStatus` calls them `offline`, so a relay seeding from vlRelay only finds the name it reads. A cleared alias starts at its PDS's head. Regression tests: `node::aliases::tests::one_pds_under_two_hostnames_is_read_once` and `state::tests::an_alias_and_the_host_it_names_speak_for_each_other`.

## Follow-ups from the first run

1. Seqs above 2^53: fixed. vlRelay serves a dense counter that the leader assigns in commit order ([Cluster](cluster.md#the-path-of-an-event)). It's the same on every node, and kept across restarts and takeovers by the quorum log and its flushes to the bucket. `@atproto/sync` passes with 0 errors, and the seqs equal indigo's on the same run. vlpds's own PDS firehose is unchanged.
2. `#identity` from any host: fixed. vlRelay refreshes the DID document and emits the event, as indigo does. Host authority still applies to `#commit`, `#sync` and `#account` (`state::tests::identity_event_refreshes_the_key`). Like indigo, another host's `#identity` doesn't create an account the relay hasn't seen: it's emitted with no state change, and the account is created (and meets its PDS's account cap) at that PDS's first event (`state::tests::foreign_identity_creates_no_account`).
3. A refused relay upstream: banned, fixed. A permanent refusal at connect (`upstream::client::Refused`, today the `atproto-relay` Server header) bans the host through the policy engine's audited ban action, and its task stops. An operator unban is how to retry it (`node::policy::tests::a_relay_upstream_is_banned_not_retried`).
4. 80 more events on `cursor=1`: not vlRelay replaying a backlog.
   - vlRelay's first subscription to a host has no cursor: `RegistryCursor` has no acked seq for a new host, and `ws_url` leaves the parameter out. A test now pins that (`upstream::client::tests::a_new_host_is_subscribed_without_a_cursor`).
   - In the first run, all 80 events arrived 0.45 s after vlRelay started, before it was even listening. They came from the reference PDS's 10 accounts, all 8 events each. indigo subscribed about 3 s later.
   - A quiet seeded network gives vlRelay nothing on a no-cursor subscription, whether it starts idle or right after a fresh seed. The full rerun had identical `cursor=1` replays on both relays (1,857 events).
   - So the reference PDS sent those events live just after it came up (`dev/up.sh` recreates its container when `DEV_PDS_HOST` changes), and only vlRelay was connected yet. Any relay subscribed then would have relayed them.
5. `OutdatedCursor` and `ConsumerTooSlow` from outside: done. `--qlog-retain-secs` (retention in seconds) and the dev-only `--max-lag-mb` exist now. The :3478 relay also keeps only 1 MiB of committed log in memory (`--qlog-memory-mb 1`), because memory serves whatever it holds and old cursors have to reach the bucket. The compat run checks both frames on that relay.
6. The current Jetstream needs public-looking PDS hostnames: still open.

