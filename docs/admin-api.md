---
title: Admin API
section: Reference
order: 300
summary: "The JSON API behind the /admin dashboard (overview, hosts, consumers, cluster, quorum, settings, policy, domain rules, accounts, takedowns and cases) and the public stats behind the page at /."
---

```hero
diagram:
  caption: The dashboard is a static page that calls /admin/api with the admin token. Each node answers for itself (its hosts, consumers and numbers), asks the other members for their quorum status over the peer port, and sends account reads and takedowns through the leader. Policy and cases are shared in the bucket.
  nodes:
    - { id: ui, label: Dashboard, sub: "`/admin`", at: [0, 3], size: [8, 3], tone: accent }
    - { id: curl, label: curl, sub: "`-u admin:$TOKEN`", at: [0, 8], size: [8, 3] }
    - { id: n1, label: node answering, sub: "`/admin/api/*`", at: [13, 5.5], size: [10, 3], tone: accent }
    - { id: n2, label: other members, sub: status · peer port, at: [28, 2], size: [10, 3], tone: accent }
    - { id: lead, label: leader, sub: records · takedowns, at: [28, 9], size: [10, 3], tone: violet }
    - { id: pol, label: "`policy/` `cases/`", sub: shared · CAS, at: [42, 5.5], size: [9, 2.6], shape: store, tone: amber }
  edges:
    - "ui.r -> n1.l30: Basic auth"
    - "curl.r -> n1.l70"
    - "n1.r30 -> n2.l: quorum status"
    - "n1.r70 -> lead.l: takedown entry"
    - { from: n1.r, to: pol.l, label: edits, dash: true }
facts:
  - { value: "Basic", label: "`admin:<token>`", note: "`--admin-token`; a bad or missing token is a 401", tone: accent }
  - { value: "camelCase", label: JSON, note: "times in unix ms (`…Ms`) unless a field says seconds" }
  - { value: "409", label: on a stale edit, note: "policy and rules carry the version they were edited from", tone: rust }
  - { value: "this node", label: answers, note: "accounts are the leader's; the cluster view asks every member", tone: blue }
```

The dashboard (`ui/`) talks to JSON endpoints under `/admin/api/`. The wire types live in
`src/admin.rs`, and `ui/src/lib/api.ts` mirrors them. Field names are camelCase and times are
unix milliseconds (`...Ms`) unless a field says seconds.

Every endpoint needs `Authorization: Basic admin:<token>`, the same scheme as the vlpds console,
so `curl -u admin:$TOKEN` works, or a proxy that names the operator on the admin listener (the
next section). A bad or missing token is a 401. Errors come back as
`{"error": "...", "message": "..."}` with 400 (`InvalidRequest`), 403 (`OperatorRefused`, a
proxy's sign-in that was refused), 404 (`NotFound`) or 409 (`VersionConflict`, or `TierSetByRule`:
a `set-tier` a domain rule overrides). A save the bucket didn't take (a timeout, a failed
request) is a 503 (`Unavailable`) with `Retry-After: 5`: the same save can be sent again. Without
`--admin-token` the console and its API are off (404), and the public page at `/`, its stats
(`/api/public/stats`, below) and `/docs` are still served.

## Sign-in through a proxy

A proxy that already knows who you are can sign you in to the console. The relay reads the
operator's login from one header the proxy sets, so the console opens without the token form and
the audit trail names the person (`alice@example.com (proxy)`) where it would say
`admin (token)`. The token keeps working alongside it, for curl, scripts and anyone the proxy
can't name.

Four flags turn it on, and they come together. Each has a `VLRELAY_` env twin
([Configuration](operations/configuration.md)), and all of them are off by default.

| Flag | Example | What it does |
|---|---|---|
| `--admin-listen` | `0.0.0.0:2985` | A second listener, for operators. It serves what `--listen` serves, and it's the only listener that reads the header. Never route public traffic to it. |
| `--admin-proxy-header` | `Tailscale-User-Login` | The header that names the operator. It needs `--admin-token` too, since `/admin` is off without it. |
| `--admin-proxy-from` | `100.64.0.10/32` | The proxy's own address, as `--admin-listen` sees the connection (the TCP peer, never `X-Forwarded-For`). The header from any other address is ignored. |
| `--admin-operators` | `alice@example.com,bob@example.com` | The logins that may sign in, compared exactly. Anyone else gets a 403 and the token form. |

Every address in `--admin-proxy-from` can claim any login, so name the proxy's own address (a
`/32`) and nothing around it. A subnet takes in its neighbors, and on a Docker network it takes in
the gateway, which is every process on the host.

What the relay does with the header:

- It reads the header only on `--admin-listen`, and only from `--admin-proxy-from`. Sent to
  `--listen`, or from anywhere else, it means nothing. So the proxy must remove any copy the
  client sent and set its own (the examples below do).
- One header with one login. Two copies of the header, an empty one or a login off the list is
  a 403.
- A request with an `Authorization` header is token auth, whatever else it carries. A wrong token
  is a 401 even when the proxy named an operator.
- The login is ambient, like a cookie. So a call signed in this way must come from the console's
  own page. A write needs `Sec-Fetch-Site: same-origin` or an `Origin` with this scheme and host,
  which every browser sends from the console. A read is refused when the browser marks it
  cross-site or same-site, or `Sec-Fetch-Site: none` (a link opened from mail or chat). That
  means another site, or a link, can't act through your browser, and `curl` through the proxy can
  still make reads. Scripts that write use the token.
- The scheme comes from the proxy's `X-Forwarded-Proto` (only a `--admin-proxy-from` peer gets
  that far), and it's plain `http` without one.
- Every audit entry says how its actor got in: `alice@example.com (proxy)` for a login the proxy
  named, `admin (token)` for the token. The relay writes both halves, so a token caller can't pass
  as an operator. The `vlrelay::audit` log lines carry `by` and `auth` the same way.
- A kick of a consumer on another member carries the label there in the peer ask, which members
  take only with `--qlog-admin-token`. No client listener reads it.

On load the console calls `GET /admin/api/session` without a token. If the answer is
`{"auth": "proxy", "operator": "alice@example.com"}` it opens straight away and shows who you are
in the top bar. A 403 (a login that isn't an operator, or a cross-site request) shows its reason
above the token form.

### Tailscale

Tailscale is the worked example because it knows every device's user already. One Caddy on the
tailnet can front several admin UIs. Put the console at `https://relay-admin.example.com`, with an
A record pointing at that Caddy host's tailnet address (DNS-only, not proxied by a CDN). Caddy with
the [caddy-tailscale](https://github.com/tailscale/caddy-tailscale) module asks the local
tailscaled who each connection is (`tailscale_auth`), and passes the login on to the node's admin
listener over the tailnet.

```caddyfile
relay-admin.example.com {
	tls {
		dns cloudflare {env.CF_API_TOKEN}
	}
	@tailnet remote_ip 100.64.0.0/10 fd7a:115c:a1e0::/48
	handle @tailnet {
		route {
			# tagged devices and failed lookups get a 401 here
			tailscale_auth
			reverse_proxy relay-node.tailnet-name.ts.net:2985 {
				# replaces any copy the client sent
				header_up Tailscale-User-Login {http.auth.user.id}
				header_up -Tailscale-User-Name
				header_up -Tailscale-User-Profile-Pic
				# the console's live tail is a websocket
				flush_interval -1
			}
		}
	}
	handle {
		respond 403
	}
}
```

Caddy needs the module (`xcaddy build --with github.com/tailscale/caddy-tailscale`) and
tailscaled's socket (`/var/run/tailscale/tailscaled.sock`) mounted. The node then runs with:

```bash
VLRELAY_ADMIN_LISTEN=0.0.0.0:2985
VLRELAY_ADMIN_PROXY_HEADER=Tailscale-User-Login
VLRELAY_ADMIN_PROXY_FROM=100.64.0.10/32   # the Caddy host's tailnet address
VLRELAY_ADMIN_OPERATORS=alice@example.com
```

Two things trip this up on a Docker host. The relay must see the proxy's real address, so check
what a connection to the published admin port looks like from inside the container. If
tailscaled masquerades what it forwards into containers (`tailscale set
--snat-subnet-routes=false` turns that off), every tailnet client arrives from the bridge
gateway, and the header is ignored. And don't publish `--admin-listen` anywhere the tailnet ACL
doesn't limit to the proxy.

`tailscale serve` works too, with no Caddy at all. It sets `Tailscale-User-Login` for devices that
belong to a user, drops the copy a client sent, and sets `X-Forwarded-Proto: https`. Point it at
the admin listener on loopback and trust only loopback:

```bash
tailscale serve --bg --https=8443 http://127.0.0.1:2985
# VLRELAY_ADMIN_LISTEN=127.0.0.1:2985  VLRELAY_ADMIN_PROXY_FROM=127.0.0.1/32
```

Trusting loopback trusts every process on the host, so use it only where nothing else runs that
you wouldn't hand the admin token.

### Other proxies

Anything that authenticates the user and sets a header works the same way. Cloudflare Access
(`Cf-Access-Authenticated-User-Email`, behind a tunnel so that only `cloudflared` reaches the
listener), oauth2-proxy (`X-Auth-Request-Email`) and Pomerium are common choices. Set
`--admin-proxy-header` to that header and `--admin-proxy-from` to the proxy's address, and make
sure the proxy overwrites the header on every request.

## Endpoints

| Method | Path | Body / query | Returns |
|---|---|---|---|
| GET | `session` | | How the caller got in: `{"auth": "token"}`, or `{"auth": "proxy", "operator": <login>}` ([Sign-in through a proxy](#sign-in-through-a-proxy)) |
| GET | `overview` | | `Overview` for this node: events/s in and out, bytes/s, consumers, hosts connected/total and by status, rejects/s by reason, time to firehose p50/p99, commit lag, open cases, the busiest hosts (each with `history`, its last 60 s of events/s) and 5 min of 1 s history for the charts. `byNode` has this node's row, and `streamEventsPerSec` is the stream's own rate |
| GET | `hosts` | `q`, `tier`, `status`, `source` (exact, a prefix ending in `:` or `*`, or `none`: not recorded), `throttled` (`true`: hosts with throttled accounts), `flag` (`atCap` · `lagging`: connected, throttled or in backpressure and over a minute behind · `erroring`: over 10% of frames rejected · `throttledOrAtCap`), `rule` (a domain rule's id: the hosts it decides, whose `rule` is it), `sort` (`host`, `tier`, `status`, `events`, `errors`, `accounts`, `seq`, `since`, `lag`, `throttled`, `source`), `desc`, `limit` (default 10,000), `offset` | `{total, hosts: HostRow[]}`: every host in the leader's table, each with its `status` (`connected` · `idle` · `backoff` · `throttled`: held at its own limits, its tier's, a domain rule's or an operator's throttle · `backpressure`: paused because the relay is behind, whatever the host does · `suspended` · `banned`), `backpressureReason` while it's in `backpressure` (`inflight_full`: its own in-flight cap, `--host-inflight-events`/`--host-inflight-mb` · `node_inflight_full`: the node's over every host, `--inflight-events`/`--inflight-mb` · `queue_full`: its lane queue, usually the lanes waiting on identity lookups · `memory_full`: the node is over `--ingest-mem-mb` and its pipeline holds a tenth of its in-flight caps · null otherwise), the `node` reading it, `source` (how the relay found it: `requestCrawl`, `bootstrap:<relay>`, `plc` or `cli`), `topReason` (the reject reason with the most of its rejects in the last five minutes on the node reading it, or null), `catchUpPace` (set while the host's events are read faster than it sent them, as with a backlog after a restart, reconnect or cursor resume: host seconds per wall second, and its limits and spam signals count on its own timeline meanwhile, see [Policy](policy.md)) and `throttledAccounts` (accounts it created that the relay throttled past its cap and nobody released, the leader's count). Each row is the reading member's, asked over the peer port (`node:hosts`, 800 ms). A member that doesn't answer leaves its hosts as this node has them, idle and without numbers. `accounts` is the larger of this node's count and the reader's, since only the leader counts accounts, and only from its own term on |
| GET | `hosts/admissions` | | `AdmissionLog`: `{newHostsToday, newHostsPerDay, entries}`, the cluster's new-host budget and the last 500 `requestCrawl`s this node answered, newest first: `{atMs, host, outcome, tier?, reason, source}`, with `outcome` one of `admitted` (a new host, or a known one woken), `refused`, `banned` or `rate-limited` |
| GET | `hosts/{host}` | | `HostDetail`: the row, the limits in force, rejects by reason, a sample of recent rejects, 2 min of per-second events and rejects, the action trail (`{atMs, by, action}`, where the relay's own tier changes are `by: "relay (service)"` with a `reason` and, for a throttle, its `case`), open cases. The rates and rejects are this node's |
| POST | `hosts/{host}/action` | `{"action": "set-tier", "tier"}`, `{"action": "throttle", "eventsPerSec": n or null}`, `{"action": "suspend", "reason"}`, `{"action": "ban", "reason"}`, `{"action": "unban"}`, `{"action": "reconnect"}`, `{"action": "set-account-limit", "maxAccounts"}` (null: back to the tier's cap), `{"action": "alias", "of"}` (another name for `of`'s PDS: no socket, [Host aliases](policy.md#host-aliases)), `{"action": "unalias", "pin"}` (`pin`: the relay won't mark it again. The name reconnects at its PDS's head, and the trail entry says `cursor reset to head`) | the updated `HostRow`, as this node applies it, or one marked `pending` ([Host actions](#host-actions)). `reconnect` works only on the node reading the host, and elsewhere it's a 400 naming that node. A `set-tier` the host's domain rule would override (any tier under a `ban` rule, any but the rule's own tier and `throttled` under a `tier` rule) is a 409 `TierSetByRule` naming the rule, and nothing is recorded: change the rule instead |
| GET | `discovery` | | `DiscoveryView`: host discovery as the leader runs it (any node answers, asking it): `{leader, leading, connectsPerMin, requestsPerSec, sources}`. Each source (`bootstrap:<relay host>` for a seed relay's `listHosts`, `plc` for the PDS hosts the PLC export names) has `url`, `enabled`, `refreshIntervalSecs`, `nextRunMs` (now while a run is in progress), `pending` (`plc`), and its run's state: `runs`, `lastStartedMs`, `lastFinishedMs`, `cursor`, `inProgress`, `runRequested`, `hostsSeen`, `known` (the relay had them), `new`, `admitted`, `refused`, `errors`, `throttled` (429s and 5xxs waited out), `pages`, `resumed` (a new leader took the run over from its cursor), `lastError` |
| POST | `discovery/run` | `{source?}` (a source's key, or none for every enabled one) | `DiscoveryView` with the run requested |
| POST | `hosts/{host}/release-throttled` | | `{released}`: lifts the relay throttle of every account the host created past its cap. The leader lists them and each lift is an entry on the log with the `#account` announcing the account's status, so consumers see it. It answers once they've committed, and the host's row (`throttledAccounts`) and its one `host` event already show them |
| GET, POST | `domain-rules` | POST `{pattern, effect, note}` | `DomainRule[]`, or the new rule. `matches` counts the known hosts the rule decides: those its pattern matches less those a more specific rule takes |
| PUT, DELETE | `domain-rules/{id}` | PUT as POST | the rule, or 204 |
| GET, PUT | `policy` | PUT `{baseVersion, policy, note}` | `PolicyDoc` (version, policy, updated at/by) |
| GET | `policy/audit` | | `PolicyAudit[]`, newest first |
| GET, PUT | `policy/full` | PUT `{baseVersion, policy, note}` | `FullPolicyDoc`: the engine's whole document (tier limits, transitions, spam thresholds and actions, cluster budgets, consumer limits, crawl settings) |
| GET | `policy/defaults` | | the full policy document as a fresh relay has it (`PolicyBody::default()`), for the Tuning page's defaults |
| GET | `domain-rules/audit` | | `PolicyAudit[]` of the domain rules, newest first |
| GET | `consumers` | | `Consumer[]` of every member, each with its `node`, asked over the peer protocol (a member that doesn't answer is left out). `readTier` is where its next events come from: `ring` (the firehose's memory, every live consumer), `disk` (the node's own log) or `bucket` |
| POST | `consumers/{id}/kick` | `node` (ids are per node, default this node) | 204. A consumer on another node is kicked there over the peer protocol, with `--qlog-admin-token`, and its audit line names the same operator |
| GET | `cluster` | | `ClusterView`: `{nodes, leader, epoch, hosts, unownedHosts, lastSeq}`. Each node comes from its quorum status: `role` (`leader`, `follower`, `candidate` or `unreachable`), `healthy` (answering, a member, its log intact), `learner`, `ownedHosts` (what the leader's table gives it), `hosts` (sockets open), consumers, rates, `commitLagMs`, CPU, memory, stream seq and `stale`. `memBytes` is null where the platform doesn't report it. `hosts` and `unownedHosts` count the leader's table and the hosts no healthy member owns, and `lastSeq` is the commit index |
| GET | `cluster/quorum` | | `QuorumView`: `{nodes: [{node, addr, stale, error, reportedMs, status}]}`, one per member or learner. `status` is the node's `/qlog/status` as it serialized it (snake_case: role, epoch, leader, last, commit, emitted, flushed F, reserve R, flush (with `last_at_ms` and `recent`, the leader's last 32 flushes: F, entries, segments, bytes, took), members, learners, switches, recovered, `history` (its leadership changes), `requests` (bucket requests by class and purpose), counters, `commit_us`, disk), passed through so new fields show up |
| POST | `cluster/quorum/members` | `{members: [...], addrs?: {node: "host:port"}}`: the whole member set wanted, and addresses for nodes the leader can't dial yet | the leader's status after the change. Sent to the leader with `--qlog-admin-token`, and a 400 when the node has none. A new node joins as a learner, and a removed one retires |
| GET | `cluster/quorum/history` | | `QuorumHistory`: `{events, stale}`, every member's leadership changes newest first: `{node, atMs, kind, epoch, from?, why}`, `kind` `lead` (`why`: `election`, `recovery` or `membership change`) or `step_down` (`why` the reason). Each member keeps its last 64, and `stale` names members that didn't answer |
| POST | `cluster/quorum/flush` | | the leader's status once F reached the commit index it had (a flush now, with `--qlog-admin-token`) |
| GET | `policy/usage` | | `PolicyUsage` on this node: PLC lookups a second against `cluster.plcLookupsPerSec` and this node's share, misses the PLC export's seeds filled, new accounts a minute against `cluster.newAccountsPerMin` (the leader's count, asked of it: the account gate runs there), and new hosts today against `cluster.newHostsPerDay`, over `windowSecs` |
| GET | `policy/signals` | | `SignalsView`: per spam rule its threshold and the ten heaviest keys on this node (`{key, host, estimate, lower}`, Space-Saving estimates with their lower bounds) |
| GET | `takedowns` | | `TakedownEntry[]`: every account under a relay takedown, newest first (`{did, takedown, atMs, by, reason}`) |
| GET | `settings` | `node` (default this node) | `SettingsView`: `{binary, version, entries: [{flag, env, value, source, default, secret, set, help}]}` for every process flag. `source` is `flag`, `env`, `default` or `unset`. A secret (`hide_env_values`, or a name with token, secret, access_key or password) has `value` and `default` null and only says whether it's `set` |
| GET | `ops/pipeline` | | `PipelineView`: this node's events in flight (read, not yet answered by the leader), the oldest one's age, the lane queue, paused readers and every pipeline gauge, and its hosts with the most in flight or a paused reader |
| GET | `ops/tail` | `host`, `rejects=1`, `sinceMs`, `limit` (default 200) | `TailFrame[]`, newest first: `{atMs, host, did, kind, reason?, detail?, upstreamSeq?, seq?, event?}`. `rejects=1` gives the frames this node read that never reached the stream, `kind` `reject` or `held` (an account the relay throttled, or a new one deferred). `host` narrows them to one host and adds its `passed` frames with the seq they went out at, from the last 8,192 this node read. One of the two is required |
| GET | `ops/rejects/top` | `reason` (a reject reason such as `bad-signature` or `prev-data-mismatch`, or none for all), `limit` (10) | `RejectTop[]`, every member's merged by host, most rejects a second first: `{host, rejectsPerSec, total, lastAtMs, sample?}`. The rate is over each member's last sample window (~10 s), the total since its start, `sample` the newest of them (`{atMs, did, reason, upstreamSeq, detail}`) |
| GET | `store` | | `StoreView`: the object store as this node uses it. `total` and `purposes` (flush, state, leader, recovery, backfill, retain) give requests by class (`a`, `b`, `free`) since start, `perSec` over `windowSecs` (since this node's previous sample, at most every 10 s, and 0 on the first call), and payload bytes up and down. `latency` is per op (count, mean, and p50 and p99 interpolated linearly within the histogram's buckets). `retention` is `retain/qlog` as the leader's last pass wrote it: the segments and state paths with their sizes, what was deletable and what it deleted, and `pruned_seq`. `dbs` is every SlateDB database the node has open (`qlog_state`, `plc_seeds` on the leader, `plc_seeds_reader` on the other members, `qlog_state_checkpoint` while a recovery reads one). From its manifest in memory, `l0Ssts` and `l0Bytes`, `sortedRuns` (newest first, each with `ssts` and `bytes`), `sstCount`, `totalBytes` (estimates), `checkpoints`, `externalDbs`. From its metrics, `memtableBytes` and `walBufferBytes` (null on a reader), `cache` hits and misses by entry kind, `compaction` (`running`, `bytesInFlight`, `bytesCompacted`, `lastAtSecs`) and `stalls` (`l0Stalls`, `backpressure`), all since the handle opened. The same names are the `db` label of `/metrics`' `slatedb_*` series |
| GET | `ops/plc` | | `PlcView`: the PLC export as the leader reads it (`enabled` with `--plc-export`): `leader`, `caughtUp`, ops read, seeds written, requests, `throttled` (429s), errors, restarts, the newest `createdAt` read, and the checkpoint's `windows` (each window's span, how far it's read, ops, done). The counters are the current leader's term, and the checkpoint is shared. Any node answers, asking the leader |
| GET | `accounts` | `q`: a DID, a handle or a prefix | up to 100 `Account`s |
| GET | `accounts/{did}` | | `Account`: handle, host, the relay's status and the host's, takedown, rev |
| POST | `accounts/{did}/takedown` | `{reason}` (required) | `Account` |
| POST | `accounts/{did}/untakedown` | | `Account` |
| GET | `cases` | `status` (open, acknowledged, resolved, dismissed), `kind`, `host`, `limit` (default all), `offset` | `{cases, total, counts: {byStatus, byKind}}`: the page of matching cases, worst severity first, how many match, and counts for the filter tabs: `byStatus` under every filter but `status`, `byKind` under every filter but `kind` |
| GET, POST | `cases/{id}` | POST `{status?, note}` | `Case` |
| POST | `cases/bulk` | `{ids?: [id], filter?: {kind?, status?, host?}, status?, note}`: the cases in `ids`, or matching `filter` (both: those in `ids` that match). One of the two is required, and so is a status or a note. Without a note each case gets `bulk: <status>`, so every case's notes name who changed it. At most 5,000 at once | `{updated, ids}`: the cases changed. Each is updated as `cases/{id}` would and gets a `case` change event, which coalesce to one `*` past the feed's bound |
| GET | `cases/{id}/evidence` | | `CaseDetail`: the case, its trip count and the newest trips (what was measured, every signal's count at the time) |
| GET | `changes` | `since` (an event id; the `Last-Event-ID` header wins) | `text/event-stream`: what changed, as it changes ([Change feed](#change-feed)) |

## Host actions

A host action's answer reads its own write. Tier, throttle, suspend, ban, unban and the account
cap change the host's record, whose tier and policy fields (throttle, cap, the tier a ban
restores, the action trail) live in the quorum log's host table, held by the leader. The node that
takes the action sends the record to the leader at once, reads the table back until its copy holds
the write, applies it to the host's limits and socket, and only then builds the `HostRow` it
answers with and makes the action's one `host` event (same `version`). It's the same on the
leader, a single-node relay included, and on any other member.

That wait is bounded (`node::admin::HOST_ACTION_SETTLE`, 3 s). Past it, with no leader answering
or another write to the host winning, the answer is the row as this node reads it so far, with
`"pending": true`: the action was recorded on this node, not confirmed by the cluster. It may
still land, and a later `host` event says when; reload the host before acting on it. `pending`
appears only on an action's answer, never in a listing.

Every other member applies the change when it next reads the table (every `--qlog-host-poll-ms`,
500 ms), re-applying the host's limits and socket then, so its row and its own `host` event follow
within about a second. The table reaches the bucket with the next entry the leader appends, so a
leader lost in between takes an unlogged change with it; the acting node's answer doesn't wait
for that.

`reconnect` changes nothing in the table: it drops the socket on the node reading the host, and
the reconnect's own status changes are later events. `release-throttled` goes through the log
(above).

## Change feed

`GET /admin/api/changes` is one Server-Sent Events stream that says what changed and when, so the
console refetches (or patches) the rows that moved instead of polling every page. An event names
an entity and its new version and carries little else: the endpoints above stay the source of
the data.

```bash
curl -N -u admin:$TOKEN https://relay.example.com/admin/api/changes
```

It takes the same auth as every other endpoint. `EventSource` can't send an `Authorization`
header, so with the token the console reads the stream with `fetch` and a `ReadableStream`, which
also lets it send `Last-Event-ID`. Signed in through a proxy, a plain `EventSource` works. A relay
without the feed answers 404, and the console goes on polling. A node serves at most 64 feeds at
once. A client that disconnects frees its slot as the node sees the connection close, but a
proxy can hold the node's side open until a write fails (the next `ping`), so a full node closes
its oldest feed for a new one when that feed is at least 15 s old: the closed client reconnects
and resumes from its last id. With every feed younger, the new one is a 503 (`TooManyFeeds`).

### Messages

| Event | `id:` | Data | When |
|---|---|---|---|
| `hello` | none | `{node, boot, atMs, resumed}` | First on every connection. `resumed` is true when the feed picked up after the `Last-Event-ID` it was sent, with nothing missed |
| `change` | `<boot>.<n>` | `Change`: `{kind, id, version, node, atMs, hint?}` | Something changed (the kinds below) |
| `resync` | `<boot>.<n>` | `{reason, atMs}` | The feed can't say what the client missed: refetch everything the page shows. `reason` is `unknown-cursor` (a `Last-Event-ID` from another node or before a restart), `expired` (older than the node keeps) or `lagged` (this client fell more than 1,024 events behind) |
| comment | | `: ping` | Every 15 s, so proxies keep the stream open and the client can tell a dead one |

`boot` is fixed for the life of the serving process and `n` counts up from 1. A client sends the
last `id:` it saw as `Last-Event-ID` (or `?since=`) when it reconnects. The serving node keeps its
last 4,096 events, and a cursor among them is replayed from the next event on, so a reconnect
inside that window misses nothing. Anything else gets `hello` with `resumed: false` and then a
`resync`. With no cursor at all the feed starts live, after `hello`, and the client does its usual
first fetch.

After a `resync` the stream goes on from live, and the `resync`'s own id is the cursor to resume
from.

### Kinds

| `kind` | `id` | `version` | What makes one | `hint` | Refetch |
|---|---|---|---|---|---|
| `host` | hostname | node-scoped | The host's row as `node` reads it: status (backpressure included, with its reason), tier, throttle, domain rule, account cap, source, owner, throttled accounts. An operator's host action makes one at once on the node that ran it | `{status, backpressureReason?, tier}` | `hosts`, `hosts/{host}` |
| `policy` | `policy` | the policy document's version | A saved policy (`PUT policy` or `policy/full`) | | `policy`, `policy/full`, `policy/audit`, `policy/usage` |
| `rules` | `rules` | the domain rules' version | A rule added, changed or deleted | | `domain-rules`, `domain-rules/audit`, `hosts` (the `rule` column) |
| `takedown` | DID | log seq | A takedown or its reversal, committed | `{takedown}` | `takedowns`, `accounts/{did}` |
| `account` | DID | log seq | A relay throttle lifted (`release-throttled`), committed | `{host}` | `accounts/{did}`, the host's row |
| `cluster` | `quorum` | log seq: the commit index when `node` saw it | The leader, the epoch, the members or learners changed, or a bucket recovery | `{epoch, leader}` | `cluster`, `cluster/quorum`, `cluster/quorum/history` |
| `consumer` | `<node>/<consumer id>` | node-scoped | A `subscribeRepos` consumer connected or left (a kick included) | `{event}`: `connect` or `disconnect` | `consumers` |
| `discovery` | the source's key (`plc`, `bootstrap:<relay host>`) | node-scoped (the leader's) | A run started, finished or moved on | `{inProgress}` | `discovery` |
| `plc` | `export` | node-scoped (the leader's) | The PLC export read on, or caught up | `{caughtUp}` | `ops/plc` |
| `case` | the case id | node-scoped | A case changed through the API (at once, and a bulk update's coalesce), resolved by the relay once a host's read lag recovered (on the host's node, within a minute), or opened or tripped again (the serving node compares the cases every 10 s) | `{status}` | `cases`, `cases/{id}` |

An `id` of `*` means more than 256 of that kind changed within one window: refetch the kind's list
rather than each row. A `*` with a `node` is about that node's view only.

`node` is the node whose observation it is: the one that read the host, served the consumer, ran
discovery or saw the commit. The `hint` is a convenience for patching a row before the refetch
lands. It may be absent, and the refetched row wins.

### Versions

Every `version` is a string, in one of two forms:

- **A number** (`"48213"`): a log seq (the commit index for `cluster`), or a document's version
  for `policy` and `rules`. These are cluster-wide, so they compare across nodes: a bigger one is
  newer for the same kind and id. `PolicyDoc.version`, `FullPolicyDoc.version`,
  `DomainRule.version` and `ClusterView.lastSeq` are the matching row versions.
- **Node-scoped** (`"n2:1759816523004117"`): `<node>:<counter>`. It compares only with another
  version from the same node, and the counter keeps counting up across that node's restarts
  (it starts from the clock in microseconds). `HostRow.version` is one.

Two versions that don't compare (different forms, or two nodes) say nothing about which is newer:
treat the event as new and refetch. `compareVersions` in `ui/src/lib/api.ts` does this.

Rows that carry a version, for the console's stale-response checks:

| Row | Field | Means |
|---|---|---|
| `HostRow` | `version`, `updatedAtMs` | The node-scoped version and time of the last change to the row as the answering node reads it (null until it sees one). A row's data is never older than its version. A host action's answer carries its own event's version, or `pending` ([Host actions](#host-actions)). `ownerVersion` is the newest `host` version the host's owner (the row's `node`) made, as far as the answering node has heard (null until it has): to tell whether the owner's status in a row or an event is newer, compare `ownerVersion` with the `version` of a `host` event whose `node` is the owner (both are that node's), never `atMs` with `updatedAtMs`, which are different nodes' clocks |
| `PolicyDoc`, `FullPolicyDoc` | `version`, `updatedAtMs` | The document's version |
| `DomainRule` | `version` | The rule set's version when it was read |
| `ClusterView` | `lastSeq` | The commit index it was read at |
| `Case` | `updatedAtMs` | When it last changed |

Consumers, discovery, the PLC view, takedowns and the quorum statuses carry live numbers and no
version. Their events only say when to refetch.

### Order and delivery

- One serving node's feed is in the order it emits. Log-backed events come in commit order, and
  one node's node-scoped events come in that node's order. There is no order between sources.
- Delivery is at least once. The same change can come twice (a policy version seen by two nodes,
  a replay), so applying an event must be idempotent.
- Hot entities are coalesced: a host, consumer, discovery source or the PLC export makes at most
  one event per second, carrying its latest state. Log-backed events go out as their entries
  commit.

### Across members

Any member serves the feed, and it covers the whole cluster:

- **Log-backed** kinds (`takedown`, `account`, `cluster`) come from the serving node's own copy of
  the log, so they reach every feed as each node commits.
- **Node-scoped** kinds (`host`, `consumer`, `discovery`, `plc`, `case`) and the document kinds
  (`policy`, `rules`) are made by the node that saw them. While it has a feed open, the serving
  node asks every other member for its new events once a second over the peer port (as
  `consumers` does), so they arrive within about two seconds. A member forwards a `host` event
  for a host it reads, since its status is live only there, and for an operator's action it ran.
- A member that restarted, or that the serving node lost track of, gets a `*` event for each of
  those kinds with its `node`, so the console refetches what it shows of that member. A member
  that doesn't answer sends nothing, and the `cluster` event says why.
- A host action rides the host table, which members read from the leader. The node that ran it
  makes its `host` event as it answers, and each other node makes its own within about a second,
  once its row shows the change ([Host actions](#host-actions)).

### admin_demo

`admin_demo` serves the feed over its simulation: host statuses flapping, consumers coming and
going, a discovery run and the PLC export moving, every few seconds, and an event for each action
(host actions, throttled-account release, takedowns, policy and rule edits, membership changes,
kicks), with versions from its simulated log and nodes. Its rows carry `version` and
`updatedAtMs` the same way.

## Policy

The tier limits and spam thresholds are one object with a version. That way a change touching
several limits lands on every node at once, and two operators can't silently overwrite each other.
A PUT carries the version it was edited from, and a stale one gets a 409 telling the operator to
reload. Each accepted change appends an audit entry listing every changed leaf
(`tiers.default.eventsPerSec: 51 → 80`), computed by `admin::diff_json`.

`validate_policy` runs on every PUT before the source sees it. It checks that the default tier
exists, rates are positive, the hourly and daily caps aren't below the per-second rate and the
reject ratio is between 0 and 1. The UI runs the same checks before it offers Save.

A domain rule is `example.com` (that host) or `*.example.com` (the domain and every subdomain), with
an effect of `ban`, `allow`, `tier` or `throttle`. An exact rule may also name one host by IPv4
address or `localhost`, with a port (`127.0.0.1:30003`), which is how dev-network hosts are known.
When several rules match a host, the most specific one decides it and is its `rule`: an exact name
before any wildcard, then the longest `*.` suffix.

On the relay (`node::admin::NodeAdmin`), consumers are the live `subscribeRepos` connections with
their events/s, bytes/s and cursor lag, and a kick drops the socket at once. Account search takes a
DID, a handle or a handle prefix ending in `*`, matched against the DID documents in the identity
cache (every account with recent traffic on this node's hosts).

## Public stats

`GET /api/public/stats` needs no token. It feeds the public page at `/` and sends only aggregates,
built by copying named fields out of `Overview` (`admin::public::project`), so a field added to
the operator API stays private until it's added there on purpose. A test fails if the key set
changes. The answer is cached for 1 s however many clients ask, carries
`Access-Control-Allow-Origin: *` and `Cache-Control: public, max-age=1`, and a failed refresh is a
503.

| Field | What |
|---|---|
| `timeMs`, `version`, `uptimeSecs` | when the numbers were taken, the build, seconds since this process started serving |
| `eventsInPerSec`, `eventsOutPerSec`, `streamEventsPerSec` | frames read from hosts, events sent, and the stream's own rate, on the node answering |
| `timeToFirehoseP50Ms`, `timeToFirehoseP99Ms` | upstream receive to sent on subscribeRepos |
| `hostsConnected`, `hostsBackpressure`, `consumers` | counts only: hosts connected, hosts the relay pauses because it's behind (status `backpressure`), subscribeRepos consumers |
| `lastSeq` | the newest firehose seq (any consumer sees it) |
| `nodes`, `nodesHealthy` | node counts from the overview's `byNode`, or the quorum log's current members when it's empty |
| `health` | `ok`, `degraded` (serving with a node or member down) or `down` |
| `quorum` | the quorum log's `ok`, `degraded` or `down` (a leader and a majority of members answering is serving) |
| `history` | `{sampleSecs, t, eventsIn, eventsOut, ttfP50Ms, ttfP99Ms}`, the last 5 min at 1 s |

No host names, IPs, DIDs, consumer addresses, reject samples, policy, case or node names are in it.

## On a cluster

Each node's dashboard is that node's view. There's no fan-out to the other members, except for
their quorum status:

| What | Answered by |
|---|---|
| Overview, host numbers, host detail, pipeline | this node: the hosts it reads and its own rates |
| Consumers and kicks | this node. Each member's dashboard lists its own consumers |
| Hosts list | every host in the leader's table, with the node reading each one |
| Reconnect | the node reading the host. Elsewhere it's a 400 naming that node |
| Tier, throttle, suspend, ban, account cap | any node, through the leader's host table, and the answer reads its own write ([Host actions](#host-actions)) |
| Domain rules, policy, cases | any node, through the shared policy store in the bucket |
| Cluster and Quorum pages | every member's `status`, asked over the peer port with an 800 ms timeout. A member that doesn't answer is `stale`, with the error, and the page never waits on it |
| Accounts | the leader, which holds every account's record. On a follower an account read is a 400 naming the leader |
| Takedown, untakedown, release-throttled | any node. Each goes to the leader as an entry on the log (the record's flag and the `#account` frame together). On a follower the `Account` in a takedown's answer has only what that node knows (status, takedown, the leader as `node`) |
| Admissions, tail, store | this node: the `requestCrawl`s it answered, the frames it read, the requests it sent. Retention is the leader's last pass, read from the bucket |
| Membership changes | any node, which sends them to the leader with `--qlog-admin-token` |
| Change feed | any node: its own events and the log's, and every other member's, asked for once a second while a feed is open ([Change feed](#across-members)) |

So to look at a member's consumers or a host's live numbers, open that member's dashboard. To
read accounts, open the leader's.

## Demo backend

```bash
cd ui && npm ci && npm run build && cd ..
cargo run --bin admin_demo            # http://127.0.0.1:2790/admin, token "demo"
cd ui && npm run dev                  # :5790 with hot reload, proxies /admin/api to :2790
```

`admin_demo` serves the same API over a simulated relay, for working on the dashboard without a
network. It holds 5,000 hosts: 24 big hosts carry most of the traffic (~2k events/s each), 60
community PDSes are mid-sized and the rest is a long tail of self-hosted PDSes, mostly idle. Four
buggy implementations reject 25–45% of their frames, and seven spam hosts trip the policy's
thresholds (account farms, forged signatures, single-account floods). A 1 s tick moves every rate
with a compressed 20-minute "day" and the odd surge, so the totals land between ~45k and ~75k
events/s. There are ~25 consumers (a couple replaying from old cursors), 3 nodes, and cases open
when a host crosses a threshold. The quorum log is a 3-member log at epoch 14 with two past
membership changes and one bucket recovery. Adding a member makes it a learner for ~20 s before it
joins. Its domain rules include `*.example.social` and an exact `demo.example.social` that
overrides it on one host. Settings is a plausible production config, and the full policy document starts from the
defaults with a few edits. Actions, rules, policy edits, membership changes and takedowns change
the simulation, but nothing is persisted.
