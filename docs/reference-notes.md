# vlRelay: reference notes

What the production relay (indigo `cmd/relay`) and shrike actually do, what the production firehose looks like on the wire, and what vlRelay has to match. Read this before changing anything in `verify`, `upstream`, `serve` or `sync_api`.

Sources, pinned:

- indigo at `b2619d864df0` (bluesky-social/indigo `main`, 2026-10-04). Paths below are relative to `indigo/`. Most relay code is in `cmd/relay/relay/` and `cmd/relay/stream/`. Commit checks call `atproto/repo/`.
- shrike at `80218c6176e5` (jcalabro/shrike `main`). Paths are relative to `shrike/`.
- The atproto sync, event-stream and account specs (atproto.com/specs/sync, /event-stream, /account), the `com.atproto.sync.*` lexicons, and proposal 0006 (sync iteration).
- `tools/refdiff` runs against `relay1.us-east.bsky.network` and three PDSes (see [Measured](#measured-the-production-firehose)).

To read along, clone both into an ignored scratch dir (`git clone --depth 50 https://github.com/bluesky-social/indigo`).

## The shape of indigo's relay

One goroutine per upstream host reads frames (`cmd/relay/stream/consumer.go:L112-L327`). It decodes the header and body, drops seqs that went backwards on this connection, and hands each event to a per-host parallel scheduler keyed by DID (`cmd/relay/stream/schedulers/parallel/parallel.go`). Workers apply the host's rate limits, then run `Relay.processRepoEvent` (`cmd/relay/relay/ingest.go:L22-L49`). An event that passes goes to `DiskPersistence.Persist`, which stamps the relay seq, writes it to a local file and only then broadcasts it (`cmd/relay/stream/persist/diskpersist/diskpersist.go:L498-L612`).

Every failure in a handler is logged and dropped. The slurper callbacks log `"failed handling event"`, still advance the host cursor and return nil (`cmd/relay/relay/slurper.go:L398-L437`). The scheduler only logs worker errors too (`parallel.go:L140-L142`). So indigo never retries an event, never disconnects a host for a bad event and never marks an account desynchronized. Wherever the tables below say "dropped", it means logged and skipped.

Bluesky runs it with `--lenient-sync-validation` (`RELAY_LENIENT_SYNC_VALIDATION`, `cmd/relay/main.go:L139-L143`), per Bluesky's relay rollout post (atproto.com/blog/relay-rollout). So the "strict" checks below are logged and the event goes out anyway. `relay1.us-east.bsky.network` and `bsky.network` both answer with `Server: openresty` and the rainbow banner, so consumers see the relay through a rainbow splitter.

## What it checks, per event type

### Every frame

| Order | Check | On failure | Where |
|---|---|---|---|
| 1 | Websocket message ≤ 5,000,000 bytes (`MaxMessageBytes`) | connection closes | `consumer.go:L18`, `L150` |
| 2 | Binary message | disconnect | `consumer.go:L183-L188` |
| 3 | Header decodes, `op` is 1 or -1 | disconnect | `consumer.go:L196-L199`, `L322-L323` |
| 4 | Body decodes (cbor-gen, unknown fields dropped) | disconnect | `consumer.go:L208-L210` and the matching lines per type |
| 5 | `seq > lastSeq` for this connection | logged at error and skipped, connection stays up | `consumer.go:L212-L215` (and per type) |
| 6 | Host rate limits, per second then hour then day | the worker blocks, nothing is dropped | `consumer.go:L66-L94` |

`lastSeq` starts at -1 on every connection (`consumer.go:L170`), so a host that rewinds after a reconnect isn't caught here. The per-account rev check is what keeps those replays out. `#labels` is decoded and then silently dropped since the slurper has no handler for it. Unknown `t` values are logged at info and skipped (`consumer.go:L306-L307`).

Rate limits never drop events. When every worker is waiting on a limiter, `AddWork` blocks on an unbuffered channel and the websocket read stops, so the PDS buffers instead (`parallel.go:L50`, `L122-L127`).

### Account lookup (all repo events)

`preProcessEvent` (`ingest.go:L54-L86`) runs before `#commit`, `#sync` and `#account`, and `#identity` runs it but ignores the error.

1. The DID must parse. `did:plc:` and `did:web:` get lower-cased (`account.go:L338-L344`).
2. The account comes from a 2M-entry LRU or the database. If it's new, `CreateAccountHost` resolves the DID, and the PDS in the DID document must be the host this frame came from (`account.go:L59-L122`). A brand new account on a host that's at its account limit is created with status `host-throttled` (`account.go:L100-L102`). Only hosts the relay already subscribes to can create accounts.
3. `EnsureAccountHost` handles migrations (`account.go:L127-L194`). If the account is stored under another host, the relay re-resolves the DID. On a mismatch it purges the identity cache and resolves again. If the DID document now names this host, the account moves (counts adjusted, new host's limit not checked). Otherwise the event is dropped.
4. A last `LookupDID` for the signing key. If it fails, `ident` is nil and only a warning is logged (`ingest.go:L80-L83`).

### `#commit`

`processCommitEvent` (`ingest.go:L88-L147`) and `VerifyRepoCommit` (`verify.go:L38-L82`):

| Order | Check | On failure |
|---|---|---|
| 1 | Account lookup above | dropped |
| 2 | `EnsureAccountActive` (`account.go:L197-L232`). If the upstream status is inactive, it calls `com.atproto.sync.getRepoStatus` on the PDS right then, on every such commit (`host_checker.go:L67-L91`) | dropped, `events_warn_counter{warn="inactive-account"}` |
| 3 | `evt.Rev <= stored rev` (string compare) | dropped with a warning, even in lenient mode (`ingest.go:L114-L120`) |
| 4 | `len(blocks) > 2,000,000` or `len(ops) > 200` | dropped (`verify.go:L23-L24`, `L41-L47`) |
| 5 | CAR v1 with a root, the root block present, the commit decodes, `version == 3`, a non-empty sig, DID and rev syntax (`atproto/repo/car.go:L82-L120`, `commit.go:L25-L41`) | dropped |
| 6 | Commit rev not more than 5 min in the future (`verify.go:L22`, `L94-L100`) | dropped |
| 7 | Signature against the `#atproto` key, if `ident` resolved (`verify.go:L102-L115`) | dropped. If `ident` is nil the check is skipped with a warning |
| 8 | `evt.repo == commit.did` and `evt.rev == commit.rev` (`verify.go:L60-L65`) | dropped |
| 9 | Strict checks, `VerifyCommitMessageStrict` (`verify.go:L119-L166`), listed below | dropped unless lenient. Bluesky runs lenient, so logged only |
| 10 | Upsert `account_repo(uid, rev, commit_cid, data_cid)` and emit the frame body verbatim with a new seq (`ingest.go:L129-L144`) | |

The strict checks, in order:

1. Empty blocks is an error. It can't happen since step 5 already failed.
2. `tooBig` set is an error.
3. No stored state and no `prevData` passes with no MST checks at all, on the theory that it's the account's first commit (`verify.go:L132-L135`).
4. Missing `prevData` is an error.
5. With stored state, a `prevData` that doesn't match the stored data CID is only a warning. So is a `since` that doesn't match the stored rev. `rev <= stored rev` is `ErrRevSequence`.
6. `repo.VerifyCommitMessage` (MST inversion) is only a warning, even in strict mode (`verify.go:L153-L155`).
7. `rebase` set is an error, and `time` must parse as an atproto datetime.

Inside `repo.VerifyCommitMessage` (`atproto/repo/sync.go:L19-L123`), created and updated records must be in the CAR and match the new tree. A delete or update without `prev` makes it return success early with no inversion (`sync.go:L76-L89`). `NormalizeOps` rejects duplicate paths, then the ops are inverted on a copy of the tree and the root is compared with `prevData`. None of that reaches the event's fate today.

indigo never compares the CAR root with `evt.commit` (`car.go:L93`, `sync.go:L54` TODO) and doesn't re-hash blocks.

### `#sync`

`processSyncEvent` (`ingest.go:L149-L192`) and `VerifyRepoSync` (`verify.go:L177-L208`):

1. Account lookup and `EnsureAccountActive`, as for commits.
2. There's no rev check, so a `#sync` can roll an account back (the TODO is at `ingest.go:L169`).
3. Blocks must be ≤ 2,000,000 bytes. The lexicon's `maxLength` for `#sync.blocks` is 10,000.
4. CAR, structure, future rev and signature as for commits, then DID and rev must match.
5. Upsert `account_repo` and emit verbatim.

It never fetches the repo. A `#sync` just resets the stored rev and data CID.

### `#identity`

`processIdentityEvent` (`ingest.go:L194-L241`):

1. Parse and normalize the DID, then purge it from the identity cache and the account cache.
2. Run the account lookup. Any error just nulls the handle. The lookup can create the account.
3. If the handle in the event doesn't match the resolved identity's handle, null it.
4. Emit `{did, time (upstream's), handle}` from any host. There's no host or status check on purpose: "Any PDS host can emit an identity event for any account" (`ingest.go:L224`).

There's a quirk in step 3. Production builds the directory with `SkipHandleVerification: true` (`main.go:L328-L329`), and with that set `LookupDID` always returns `handle.invalid` (`atproto/identity/base_directory.go:L76-L79`). So whenever resolution works, the handle never matches and the relay strips it. refdiff measures this below.

If the account row doesn't exist (say the DID lives on another host), `Persist` can't find a uid and the event is silently dropped (`diskpersist.go:L590-L593`).

### `#account`

`processAccountEvent` (`ingest.go:L243-L294`):

1. The account lookup must pass (the account is on this host or can be created). An inactive account is still processed.
2. The new upstream status is `active` if `active` is true, else the event's `status` string as is (not validated), else `inactive`.
3. A changed status updates `upstream_status`.
4. Emit `#account` with `active = IsActive()` and `status = StatusField()`, which combine the local and upstream status (below), and the upstream `time`.

`deleted` doesn't decrement the host's `account_count`, although `models.go:L56` says it should.

### `#info` and error frames from a host

`#info` is logged at debug and not forwarded. An upstream `FutureCursor` error sets the host `idle` and stops the subscription for good. Other error frames are logged and the connection stays up (`slurper.go:L438-L456`).

## Account status

Each account has a local `status`, an `upstream_status` and a `host_id` (`models/models.go:L91-L99`). The values are `active`, `deactivated`, `deleted`, `desynchronized`, `suspended`, `takendown`, `throttled`, `host-throttled` and a catch-all `inactive` (`models.go:L75-L89`).

- `AccountStatus()` is the local status if it isn't `active` (`host-throttled` is reported as `throttled`), otherwise the upstream status (`models/methods.go:L25-L33`).
- `IsActive()` is `(local == active || local == throttled) && upstream == active` (`methods.go:L46-L48`). Nothing ever sets local `throttled`.
- `StatusField()` is nil when active, else `AccountStatus()`.

So a host-throttled account goes out as `active: false, status: "throttled"`. The account spec says `throttled` and `desynchronized` are states where `active` may be true. indigo also maps an upstream `desynchronized` to inactive (`host_checker.go:L83`). And a local `active` account with an upstream `inactive` goes out with the non-spec status `"inactive"`.

Admin takedowns set the local status and emit `#account` with `time` set to now (`account.go:L252-L285`). They don't remove the account's old events from the replay window. `TakeDownRepo` exists in the persister but nothing calls it.

## Hosts

### Status transitions

The host statuses are `active`, `idle`, `offline`, `throttled` and `banned` (`models.go:L15-L23`).

| From | To | When | Where |
|---|---|---|---|
| (new) | `active` | requestCrawl passes | `crawl.go:L11-L56` |
| `active` | `offline` | more than 15 failed dials in a row (about 3-4 min) | `slurper.go:L327-L341` |
| `active` | `idle` | the host sent a `FutureCursor` error frame | `slurper.go:L438-L456` |
| `active` | `banned` | the host's `Server` header contains `atproto-relay`, or an admin block | `slurper.go:L343-L351`, `handlers_admin.go:L338-L370` |
| `banned` | `active` | admin unblock (it doesn't resubscribe) | `handlers_admin.go:L372-L401` |
| any | `active` | every 4 s cursor flush writes `status = active` for every subscribed host with a cursor | `host.go:L127-L138` |

`throttled` is never set, and nothing marks a host `idle` for being quiet (`cmd/relay/README.md` says so). An `offline` or `idle` host stays down until someone sends requestCrawl, since startup only resubscribes `active` hosts (`crawl.go:L59-L79`). The last row is probably a bug. A ban or idle written while the subscription is still in the map can get flipped back to `active`.

Reconnects have two bugs of their own. `sleepForBackoff` returns `time.Duration(b) * 2`, which is nanoseconds, plus up to a second of jitter, so the first nine retries come within a second (`slurper.go:L380-L390`). And a dial that works and then drops reconnects with no sleep at all (`slurper.go:L355-L361`).

The production `listHosts` matches this. On 2026-10-04 it listed 6,260 hosts: 1,956 active, 3,937 offline, 298 idle and 69 banned. There were 89 `*.host.bsky.network` hosts with 23.4M of the 24.0M accounts.

### requestCrawl

`handleComAtprotoSyncRequestCrawl` (`handlers.go:L19-L69`) checks, in order:

1. Crawling disabled and not an admin gives 403.
2. `ParseHostname` (`host.go:L143-L190`). It takes https or wss (or http and ws, as no-SSL), and it ignores any path or query. A port is only allowed on `localhost`. The name must pass handle syntax, so IPs and single labels fail. It's lower-cased.
3. No-SSL without `--allow-insecure-hosts` (and not an admin) gives 400.
4. `localhost` from a non-admin gives 400.
5. Domain bans match the host and every parent with at least two labels (`domain_ban.go:L19-L44`). A DB error here returns an empty 200 without subscribing (`handlers.go:L43-L46`), which is a bug.
6. `describeServer` on the host over an SSRF-safe client with a 5 s timeout. Failure gives 400 `HostNotFound`.
7. Forward to sibling relays, before subscribing.
8. `SubscribeToHost`. A new host counts against the new-hosts-per-day limit unless an admin sent it. Trust is decided here, once, and stored on the host row.

A new host starts at the live head, since the cursor is only sent when it's above 0 (`slurper.go:L320-L323`). That's why quiet hosts show `seq: -1` for a long time (indigo#1064).

## Limits and policy

| Limit | Default | Flag / env | Notes |
|---|---|---|---|
| Accounts per host | 100 | `--default-account-limit`, `RELAY_DEFAULT_ACCOUNT_LIMIT` | new accounts over it become `host-throttled` |
| Accounts per trusted host | 10,000,000 | hard-coded `TrustedRepoLimit` (`relay.go:L58`) | |
| Trusted domains | `*.host.bsky.network` | `--trusted-domains`, `RELAY_TRUSTED_DOMAINS` | exact or `*.` suffix match, decided at host creation only |
| New hosts per day | 50 | `--new-hosts-per-day-limit`, `RELAY_NEW_HOSTS_PER_DAY_LIMIT` | in-memory sliding window, admin requests skip it |
| Untrusted host events | 50 + limit/1000 per s, 2,500 + limit per h, 20,000 + 10 × limit per day | none (`slurper.go:L178-L191`) | built when the host subscribes |
| Trusted host events | 5,000/s, 50M/h, 500M/day | none | |
| Workers per host | 40 | `--host-concurrency` | |
| Identity cache | 5M entries, 24 h TTL, 2 min for errors | `--ident-cache-size` | |

Raising a host's account limit (`/admin/pds/changeLimits`) promotes up to that many `host-throttled` accounts whose upstream status is active, oldest uid first, and emits `#account` for each (`host.go:L78-L122`). Lowering it doesn't demote anyone. The per-day limit and the crawl switch are changed through admin endpoints, live, and aren't persisted.

The limits are tuned for spam. At the default 100 accounts a host gets 50 events/s and 2,600 an hour, so a small community PDS that grows past 100 users goes quiet for its new users until an operator notices. That's the subject of atproto discussion 5271.

## Admin API

Everything is under `/admin` with HTTP basic auth (user `admin`, password from `--admin-password` / `RELAY_ADMIN_PASSWORD` or `RELAY_ADMIN_KEY`, random and logged if unset). Routes are at `service.go:L158-L190`. "Fwd" means it's forwarded to `--sibling-relays`.

| Method | Path | Params | What it does | Fwd |
|---|---|---|---|---|
| GET | `/subs/getUpstreamConns` | | list of connected hostnames | |
| POST | `/subs/killUpstream` | `host`, `block=true` | drop the connection, ban if `block` | yes |
| GET, POST | `/subs/getEnabled`, `/subs/setEnabled` | `enabled` | public requestCrawl on or off (not persisted) | no |
| GET, POST | `/subs/perDayLimit`, `/subs/setPerDayLimit` | `limit` | new hosts per day (not persisted) | no |
| GET | `/subs/listDomainBans` | | `{banned_domains}` | |
| POST | `/subs/banDomain`, `/subs/unbanDomain` | JSON `{Domain}` | edit domain bans. Existing hosts aren't touched | yes |
| GET | `/repo/takedowns` | `cursor` | page of 500 taken-down DIDs | |
| POST | `/repo/takeDown`, `/repo/reverseTakedown` | JSON `{did}` | set local status, emit `#account` | yes |
| GET | `/pds/list` | | up to 10,000 hosts with cursor, counts, limits, live rates, alert state | |
| POST | `/pds/requestCrawl` | JSON `{hostname}` | requestCrawl as admin (skips the switch, SSL rule and per-day limit) | yes |
| POST | `/pds/changeLimits` | JSON `{host, repo_limit, account_limit_alerts_silenced}` | change the account limit or silence alerts | yes |
| POST | `/pds/block`, `/pds/unblock` | `host` | ban and disconnect, or unban | yes |
| POST | `/alerts/accountLimitSent` | JSON | record that an account-limit alert went out | |
| GET | `/consumers/list` | | firehose subscribers with address, UA, events sent | |

Forwarding skips requests whose `Via` or `User-Agent` contains `atproto-relay` (`forward.go:L21-L90`). The metrics listener (`:2471`) uses the default mux, which also serves `net/http/pprof`.

## Sync endpoints

| Endpoint | Behavior |
|---|---|
| `listRepos` | `limit` 1-1000 (default 500), cursor is an internal uid. Only accounts with `status = active` and `upstream_status = active` and a stored commit are listed, so `active` is always true and `status` always absent. `head` is the commit CID, `rev` the stored rev. The next cursor is only set if the page is full and longer than 1 item, so `limit=1` never pages (`handlers.go:L130-L168`, `account.go:L295-L309`). |
| `getRepoStatus` | DID parsed but not normalized. Unknown gives 404 `RepoNotFound`. Returns `active = IsActive()`, `status = StatusField()`, and `rev` only if a commit is stored (`handlers.go:L170-L199`). |
| `getLatestCommit` | takendown or suspended gives `RepoTakendown`, deactivated `RepoDeactivated`, deleted `RepoDeleted`, any other non-active `RepoInactive` (all 403). No stored commit is 404 `RepoNotSynchronized` (`handlers.go:L201-L238`). |
| `listHosts` | `limit` 1-1000 (default 200), cursor is a host id. Lists every host with `last_seq > 0`, including banned and offline. Fields are `hostname`, `seq` (the persisted cursor), `status` and `accountCount` (`host.go:L57-L72`). |
| `getHostStatus` | `hostname` used raw. 404 `HostNotFound`. Same fields, and `seq` can be -1. `accountCount` includes throttled and deleted accounts (`handlers.go:L108-L128`). |
| `getRepo` | 302 to the account's PDS, with no status check, so taken-down accounts still redirect (`stubs.go:L125-L158`). `getRecord` and `getBlob` aren't routed. |
| `subscribeRepos` | Bad or negative cursor gives 400. Pings every 30 s when idle. No per-IP limits (`broadcast.go:L63-L196`). |

## Seqs, cursors and the replay window

The relay seq is an in-memory counter, bumped under a lock when an event is persisted (`diskpersist.go:L498-L501`). So the order across hosts is just the order events finished processing. On restart it scans the newest log file for the last seq and resumes from there, never below `--initial-seq-number` (`diskpersist.go:L181-L251`). That floor exists because two production relays once restarted from an empty log file and went from ~20B back to 1 (indigo#1133).

Events are buffered and written every 100 ms or 400 events (no fsync), and live subscribers only get an event after that write (`diskpersist.go:L334-L394`). Files hold 10,000 events each. An hourly job deletes files older than `--replay-window` (`RELAY_REPLAY_WINDOW`, default 72 h).

Cursors:

- A cursor replays events with `seq > cursor`. The spec text says "greater-or-equal" and the lexicon says "last known seq", so this is ambiguous (indigo#837). Everything in production uses `>`.
- `cursor=0` replays the whole window.
- A cursor older than the window replays from the oldest file. There's no `#info OutdatedCursor` (indigo#1328).
- A future cursor logs an error and the client just gets the live stream. There's no `FutureCursor` error.
- Playback goes into a 512-event channel, then the subscriber is added and the gap between playback and the first live event is filled in (`event_manager.go:L117-L218`).
- Each subscriber has a 16,384-event buffer. When it fills, the relay sends `ConsumerTooSlow` (op -1) and closes. That can happen during catch-up, before the consumer ever reaches live (indigo#975).

On the upstream side, the host cursor only advances to a seq once every lower seq in flight has finished (`parallel.go:L159-L174`), and it's flushed to the database every 4 s (`slurper.go:L72`). So a restart replays up to a few seconds per host, and the rev checks absorb the duplicates for commits. `#identity` and `#account` replays go out again.

## Bugs, deviations and open questions

From the code:

1. Sync 1.1 isn't enforced. MST inversion, `prevData` and `since` mismatches are warnings even in strict mode, and production runs lenient on top of that.
2. No account is ever marked `desynchronized`, and the relay never emits `#sync`.
3. `#identity` handles are stripped whenever resolution succeeds (the `SkipHandleVerification` quirk above).
4. `#sync` has no rev check, so it can roll an account back.
5. `sleepForBackoff` mixes nanoseconds and milliseconds, and a dropped connection reconnects without any delay.
6. The cursor flush forces hosts back to `active`.
7. requestCrawl returns 200 when the domain-ban lookup fails, and forwards to siblings before it has subscribed.
8. `#account` status strings pass through unvalidated, and the catch-all `inactive` isn't a spec value.
9. Deleted accounts still count against the host's limit.
10. Takedowns don't purge the replay window, and `getRepo` redirects for taken-down accounts.
11. Upstream error frames other than `FutureCursor` don't close the connection.
12. Signature checks are skipped when DID resolution fails.

From the issue tracker (all open unless noted):

- indigo#1328: no `OutdatedCursor` or `FutureCursor` downstream.
- indigo#837: cursor `>` vs `>=`, and indigo's own persisters disagree (pebble uses `>=`).
- indigo#1478: duplicate and out-of-order seqs seen on `bsky.network` around the backfill-to-live switch (Sep 2026).
- indigo#975: slow consumers cut off during catch-up.
- indigo#1161 and PR #1215 (merged): accounts wrongly considered inactive. A scan found 7% of self-hosters blocked. The fix added the live `getRepoStatus` re-check.
- indigo#1143: accounts with no commit yet have no `rev`, but `listRepos` requires one.
- indigo#1064 and atproto discussion 4595: a new quiet host shows `seq: -1` until it emits something.
- indigo#1063: host status lifecycle unfinished (no idle, no recovery from offline).
- indigo#1231 and atproto-website#485: a PDS that resets its seq gets marked `idle` and stays that way.
- indigo#1423: a PDS whose frames fail to decode just reconnect-loops with no feedback. Proposes a `lastError` on `getHostStatus`.
- indigo#1357: `since: ""` passes through. indigo#1347: `time` isn't validated on `#account`.
- atproto discussion 5271: `throttled` sent with `active: false` although the spec says it may be active.
- proposals#78: no exact definition of the blocks a commit must carry. Producers include enough for inversion in any op order.
- proposals#77: `prevData` is optional in the lexicon, but every current PDS sends it and relays expect it. An inversion failure means a bug (drop), a `prevData` mismatch means lost events (resync).
- atproto#2400: PDSes skip seqs, so gaps don't mean anything.
- atproto#4212: PDSes accept CBOR (floats, unsafe ints) that strict consumers reject.

Spec ambiguities that matter to us:

- The sync spec says 5 MB per frame. The event-stream spec says there's no specific limit.
- `#sync.blocks` is 10,000 bytes in the lexicon, but indigo checks 2,000,000.
- Whether `throttled` and `desynchronized` accounts are `active: true`.
- Whether the relay should rewrite `time` on events it forwards.
- What a relay should do when a PDS resets its seq.

## Where shrike and indigo disagree

shrike has no relay. It's a Rust atproto library whose closest piece is a verifying firehose consumer: a strict sync 1.1 frame parser (`src/sync/raw.rs`), a per-DID verifier with chain state, inversion and `getRepo` resync (`src/sync/verifier.rs`, `invert.rs`, `resync.rs`), and a per-DID parallel consumer modeled on indigo's scheduler (`src/streaming/parallel.rs`). Its comments say it follows atmos (jcalabro's Go library) more than indigo.

| Topic | indigo relay | shrike |
|---|---|---|
| MST inversion failure | warning only (`verify.go:L153-L155`) | enforced. Chain break, resync or error (`verifier.rs:L511-L529`) |
| `prevData` ≠ stored data | warning (`verify.go:L140-L143`) | chain break (`verifier.rs:L1071-L1085`) |
| No `prevData` with stored state | error unless lenient | accepted as a legacy commit if no update or delete has `prev` (`verifier.rs:L1015-L1025`) |
| `tooBig`, `rebase` | errors in strict mode | ignored, but the parser requires both fields (`raw.rs:L177-L178`) |
| `time` | must parse (strict) | never parsed |
| `since` | mismatch is a warning | ignored |
| Identity won't resolve | signature check skipped | hard error |
| Bad signature | dropped | purge the identity and retry once (`verifier.rs:L1172-L1199`) |
| `#identity` | purges the identity cache | doesn't purge |
| Handle on `#identity` | nulled unless it matches (always, in production) | forwarded after a syntax check. Bad syntax rejects the frame |
| CAR root vs `evt.commit` | not compared | must match, hard error (`invert.rs:L112-L120`) |
| Block hashes | not re-hashed | every block re-hashed (`invert.rs:L123-L134`) |
| Check order | sizes, structure, future rev, signature, fields, then inversion | future rev, sizes, replay, CAR, inversion, fields, signature, op CIDs. The MST work happens before the signature check |
| `#sync` | no rev check, just reset state | drop replays, no-op fast path if the data CID matches, else full `getRepo` |
| `#sync` blocks > 2 MB | error | resync instead |
| Inactive accounts | always dropped | only under `HostingPolicy::Gate` (default is track) |
| Seq ordering | per connection | not checked |
| CBOR | cbor-gen, lenient on key order (unverified) | strict DRISL: minimal ints, canonical key order, tag 42 only, CIDv1 sha-256 only |
| Max frame | 5,000,000 bytes | `max_message_size` is ignored on native, so tungstenite's defaults |

They agree on the core numbers: 5 min future-rev tolerance, 2,000,000 bytes of blocks and 200 ops (`verify.go:L22-L24`, `verifier.rs:L28-L30`). Both drop a commit whose rev isn't newer than the stored one.

shrike also has a few gaps of its own. Error frames without a `t` fail to parse although the spec's error header is just `{op: -1}`. In verified mode an unknown `t` comes back as an error instead of being skipped. Resync only happens if `async_resync_workers > 0`, which defaults to 0.

## Measured: the production firehose

`tools/refdiff` subscribes to the relay and to a few PDSes at once and matches events by (DID, kind, rev). It reports what one side saw and the other didn't, per-DID order, the PDS→relay delay as a consumer sees it, and the relay's frame mix. It only reads public streams.

```bash
cd tools/refdiff && cargo build --release
./target/release/refdiff --relay relay1.us-east.bsky.network \
  --pds amanita.us-east.host.bsky.network --pds eurosky.social --pds blacksky.app \
  --secs 780 --out run.json
```

We ran it twice from benchbox on 2026-10-04, 09:10 and 09:28 UTC (a Sunday, ~2 am Pacific, so near the daily low), for 16 and 13 minutes. amanita is a Bluesky mushroom with ~213k accounts. eurosky.social (~35k accounts, in Europe) and blacksky.app (~42k accounts, its own PDS implementation) are the biggest independent hosts by account count in `listHosts`. Raw output is in `tools/refdiff/results/`.

### Frame sizes and rates

| | Run 1 (930 s) | Run 2 (750 s) |
|---|---|---|
| Relay events | 216,147 | 161,164 |
| Events/s (p50 · p99 · max per second) | 214 · 311 · 564 | 189 · 271 · 333 |
| MB/s | 1.14 | 1.01 |
| Mean frame | 5,323 B | 5,283 B |
| `#commit` share of events · of bytes | 99.56% · ~100% | 99.55% · ~100% |
| `#commit` size p50 · p90 · p99 · p99.9 · max | 5,223 · 7,251 · 9,863 · 16,239 · 799,743 B | 5,159 · 7,279 · 9,959 · 16,479 · 763,391 B |
| `#commit` blocks p50 · p99 | 4,851 · 9,455 B | 4,771 · 9,543 B |
| `#commit` ops p50 · p99 · max | 1 · 2 · 100 | 1 · 3 · 100 |
| `#sync` · `#identity` · `#account` per s | 0.3 · 0.3 · 0.4 | 0.2 · 0.3 · 0.3 |
| `#sync` · `#identity` · `#account` size | 409 · 100 · ~111 B | 409 · 100 · ~111 B |
| `tooBig` commits | 0 | 0 |
| Relay seq gaps | 0 | 0 |

Almost every frame is a one-op commit, and about 90% of its bytes are the CAR blocks. A record is a few hundred bytes, so most of the ~4.8 KB is the MST proof path that sync 1.1 requires. That's why frames got bigger than the ~4.5 KB the design assumed.

Our indexer's ClickHouse archive puts the rate in context (`default.repo_records`, record ops per hour over the last 7 days). The average is ~350 ops/s, the busiest hour ~480/s and the quietest ~180/s. So we sampled close to the bottom of the day, and the peak is about 2.3× what we saw.

For the capacity math, that means:

- Plan on ~5.3 KB per event. 100k events/s is ~530 MB/s (~4.2 Gbit/s) coming in, and the same again for every full-firehose subscriber going out. That's still inside 10 GbE per node for the ingest plus a subscriber or so, but not much more.
- Today's ~350 events/s is ~1.9 MB/s (~15 Mbit/s), and a 72 h window is ~480 GB raw. The design used 12 Mbit/s and ~390 GB.
- At 100k/s, a 72 h window is ~137 TB raw before compression.
- Frames up to ~800 KB are real (a 100-op commit). Size buffers for the 5 MB limit, not the average.

### Time to firehose

The delay is the time between our socket getting an event from the PDS and our socket getting the same event from the relay. That's the extra wait a consumer has by reading the relay instead of the PDS. Both sockets ran on the same machine, so there's no clock skew in it.

| PDS (run 2) | Matched | p10 | p50 | p90 | p99 | p99.9 | max |
|---|---|---|---|---|---|---|---|
| amanita (mushroom) | 1,799 | 1 ms | 89 ms | 169 ms | 899 ms | 1,050 ms | 1,084 ms |
| eurosky.social | 4,508 | -55 ms | 92 ms | 258 ms | 563 ms | 836 ms | 1,048 ms |
| blacksky.app | 443 | 52 ms | 94 ms | 133 ms | 339 ms | 374 ms | 374 ms |
| all three | 6,763 | | 92 ms | 223 ms | 589 ms | 1,014 ms | 1,084 ms |

Run 1 agrees (n = 10,228, p50 102 ms, p90 192 ms, p99 734 ms, p99.9 2.5 s). Its percentiles leave out the ~8% of events that reached us from the relay first, so they read a bit high.

Some of the deltas are negative, down to -880 ms. That's when the PDS's stream to us was slower than the PDS→relay→us path. TCP connects from benchbox take ~30 ms to the relay's edge, ~60 ms to amanita and ~160 ms to eurosky, so the paths aren't equal. The PDS streams also arrive in bursts (amanita's own commit-to-socket time has a p99 of 2.6 s). So p50 is the trustworthy number and the tails carry PDS noise. Part of indigo's p50 is built in: it writes events to disk every 100 ms (or 400 events) before broadcasting, and rainbow adds a hop.

The baseline vlRelay has to beat is ~90 ms p50 and ~600 ms p99 from PDS to consumer, measured this way.

### Missing, extra and out-of-order events

Across both runs, 17,000 commits matched with no misses, no CID mismatches, no duplicates and no per-DID reordering. The relay stream had no seq gaps in 377k events.

Three `#sync` events from eurosky never showed up at the relay. All three DIDs had just migrated to eurosky from Bluesky mushrooms (morel, scalycap and shiitake), and each `#sync` reached us a second or two after the PLC operation that moved the account (`did:plc:2x3ipl2td62fs4ubqwqoe273` at 09:14:09.66Z, for example). The likely cause is indigo resolving the DID before PLC (or its cache) had the new PDS and dropping the event as coming from the wrong host. The accounts' later commits went through, and `getRepoStatus` at the relay showed the right rev afterwards.

This matters for PLAN decision 3. vlRelay will drop events from a host that the DID document doesn't name yet, so it'll hit the same race. A DID owner should hold events from a non-matching host for a few seconds and re-resolve once more before dropping them, and the `#sync` that follows a migration is the event most likely to be in that window.

The two "relay-only" commits in run 2 are an artifact of how refdiff picks DIDs. They came from the account's old PDS (shiitake) before it moved to eurosky later in the run.

### `#identity` handles

None of the 230 `#identity` events from the relay in run 2 carried a handle. Five of the seven eurosky `#identity` events that matched had one at the PDS, and the relay stripped all five. That's the `SkipHandleVerification` quirk from [`#identity`](#identity) showing up in production.

## What vlRelay must match

Wire format and stream:

- [ ] Re-emit `#commit` and `#sync` with every lexicon field as received except `seq`, including the deprecated ones (`tooBig: false`, `rebase: false`, `blobs: []`). indigo decodes into cbor-gen structs and re-encodes, so unknown fields are dropped on the way through.
- [ ] Emit only `#commit`, `#sync`, `#identity` and `#account` downstream. Drop upstream `#info` and `#labels`.
- [ ] Seqs strictly increasing, 1 to 2^53, never reset across restarts or failovers.
- [ ] `cursor` replays `seq > cursor`. `cursor=0` replays the whole window. No cursor means live.
- [ ] Ping idle subscribers every 30 s. Answer slow consumers with `ConsumerTooSlow` (op -1) before closing.
- [ ] Frames up to 5 MB from upstream.

Checks, in this order of cost:

- [ ] Drop frames over 5 MB, `#commit` with more than 2,000,000 bytes of blocks or more than 200 ops, and `#sync` over the same blocks limit.
- [ ] Commit CAR v1, root block present, `version == 3`, DID and rev syntax, rev at most 5 minutes in the future.
- [ ] `evt.repo == commit.did` and `evt.rev == commit.rev`.
- [ ] Signature against the `#atproto` key.
- [ ] Drop a `#commit` whose rev isn't newer than the stored rev, even when we're lenient about everything else.
- [ ] Only accept repo events from the host the DID document names. On a mismatch, purge and re-resolve once before dropping.
- [ ] Drop `#commit` and `#sync` for inactive accounts, but re-check `getRepoStatus` at the PDS first when only the upstream status says inactive (indigo#1161).
- [ ] Pass `#identity` from any host, purging our identity cache for the DID.

State and endpoints:

- [ ] Account status combines a local and an upstream status, with `takendown` (and other local states) winning.
- [ ] `listRepos`, `getRepoStatus`, `getLatestCommit`, `listHosts`, `getHostStatus` with indigo's fields, errors, limits and defaults (tables above).
- [ ] requestCrawl hostname rules: https/wss only, no port except `localhost`, handle syntax, domain bans on every parent, `describeServer` must answer.
- [ ] New host starts at the live head. Persist each host's cursor every few seconds and resume from it.
- [ ] Per-host account limit (default 100) with a trusted-domain list (default `*.host.bsky.network`). Over-limit accounts are throttled, and raising the limit releases them oldest first with an `#account` each.
- [ ] Per-host event limits that block the reader instead of dropping.
- [ ] Refuse to subscribe to another relay (`Server` header contains `atproto-relay`), and send `Server: … (atproto-relay)` ourselves.
- [ ] Admin: takedown and reverse with `#account`, host block and unblock, domain bans, account limit changes, crawl switch and per-day limit.

## Where vlRelay differs on purpose

Decided in the design doc:

- Sync 1.1 is enforced. A failed `prevData` or inversion check drops the event and marks the account `desynchronized` until a `#sync` (or a fresh `getRepo` on archival shards) resets it. indigo only logs these.
- Takedowns filter the replay window as well as the live stream, through the takedown list in the bucket.
- A slow consumer falls back to reading segments, so catch-up doesn't trip `ConsumerTooSlow` before it reaches live.
- The relay seq is the merge key of the node logs, so seqs are time-ordered ids with gaps instead of a dense counter. The spec allows gaps.
- Account migrations: the DID owner only accepts events from the host its fresh DID document names, and re-resolves when that changes (PLAN decision 3). That's what indigo does too, but per DID owner instead of per host.

Proposed here, for the lead to confirm:

- Events from a host the DID document doesn't name yet wait a few seconds for one more re-resolve before they're dropped, so the `#sync` right after a migration isn't lost (see [Missing, extra and out-of-order events](#missing-extra-and-out-of-order-events)).
- `#sync` rev ordering is checked like `#commit`, so a `#sync` can't roll an account back.
- `#info OutdatedCursor` for cursors older than the window, and `FutureCursor` (then close) for cursors past the head, per the event-stream spec.
- The CAR root must equal `evt.commit`, and blocks are re-hashed as they're used (vlpds's CAR reader already does).
- If DID resolution fails, the event waits on the host's identity budget instead of skipping the signature check.
- `#identity` keeps the handle if it matches the DID document's `alsoKnownAs`. indigo strips almost all of them by accident, and consumers have learned not to trust the field, so this is cheap to get right.
- Hosts get `idle` after a quiet period, automatic retry for `offline` with real backoff, and `throttled` as a live state set by policy. The cursor flush never overwrites a status.
- `getHostStatus` gets the last upstream error as an extension field (indigo#1423).
- `throttled` and `desynchronized` accounts are reported `active: true` with their status, per the account spec. Check what the AppView does with them before shipping this.
- `getRepo` respects takedowns.
