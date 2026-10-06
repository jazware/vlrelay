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
so `curl -u admin:$TOKEN` works. A bad or missing token is a 401. Errors come back as
`{"error": "...", "message": "..."}` with 400 (`InvalidRequest`), 404 (`NotFound`) or 409
(`VersionConflict`). Without `--admin-token` the console and its API are off (404), and the
public page at `/`, its stats (`/api/public/stats`, below) and `/docs` are still served.

## Endpoints

| Method | Path | Body / query | Returns |
|---|---|---|---|
| GET | `overview` | | `Overview` for this node: events/s in and out, bytes/s, consumers, hosts connected/total and by status, rejects/s by reason, time to firehose p50/p99, commit lag, open cases, the busiest hosts (each with `history`, its last 60 s of events/s) and 5 min of 1 s history for the charts. `byNode` has this node's row, and `streamEventsPerSec` is the stream's own rate |
| GET | `hosts` | `q`, `tier`, `status`, `sort`, `desc`, `limit` (default 10,000), `offset` | `{total, hosts: HostRow[]}`: every host in the leader's table, each with the `node` reading it. Only this node's hosts carry live numbers |
| GET | `hosts/admissions` | | `AdmissionLog`: `{newHostsToday, newHostsPerDay, entries}`, the cluster's new-host budget and the last 500 `requestCrawl`s this node answered, newest first: `{atMs, host, outcome, tier?, reason}`, with `outcome` one of `admitted` (a new host, or a known one woken), `refused`, `banned` or `rate-limited` |
| GET | `hosts/{host}` | | `HostDetail`: the row, the limits in force, rejects by reason, a sample of recent rejects, 2 min of per-second events and rejects, operator actions, open cases. The rates and rejects are this node's |
| POST | `hosts/{host}/action` | `{"action": "set-tier", "tier"}`, `{"action": "throttle", "eventsPerSec": n or null}`, `{"action": "suspend", "reason"}`, `{"action": "ban", "reason"}`, `{"action": "unban"}`, `{"action": "reconnect"}`, `{"action": "set-account-limit", "maxAccounts"}` (null: back to the tier's cap) | the updated `HostRow`. `reconnect` works only on the node reading the host, and elsewhere it's a 400 naming that node |
| POST | `hosts/{host}/release-throttled` | | `{released}`: lifts the relay throttle of every account the host created past its cap. The leader lists them and each lift is an entry on the log with the `#account` announcing the account's status, so consumers see it |
| GET, POST | `domain-rules` | POST `{pattern, effect, note}` | `DomainRule[]`, or the new rule |
| PUT, DELETE | `domain-rules/{id}` | PUT as POST | the rule, or 204 |
| GET, PUT | `policy` | PUT `{baseVersion, policy, note}` | `PolicyDoc` (version, policy, updated at/by) |
| GET | `policy/audit` | | `PolicyAudit[]`, newest first |
| GET, PUT | `policy/full` | PUT `{baseVersion, policy, note}` | `FullPolicyDoc`: the engine's whole document (tier limits, transitions, spam thresholds and actions, cluster budgets, consumer limits, crawl settings) |
| GET | `policy/defaults` | | the full policy document as a fresh relay has it (`PolicyBody::default()`), for the Tuning page's defaults |
| GET | `domain-rules/audit` | | `PolicyAudit[]` of the domain rules, newest first |
| GET | `consumers` | | `Consumer[]`, this node's |
| POST | `consumers/{id}/kick` | `node` (ids are per node; default this node) | 204. A consumer on another node is a 400 naming it |
| GET | `cluster` | | `ClusterView`: `{nodes, leader, epoch, hosts, unownedHosts, lastSeq}`. Each node comes from its quorum status: `role` (`leader`, `follower`, `candidate` or `unreachable`), `healthy` (answering, a member, its log intact), `learner`, `ownedHosts` (what the leader's table gives it), `hosts` (sockets open), consumers, rates, `commitLagMs`, CPU, memory, stream seq and `stale`. `hosts` and `unownedHosts` count the leader's table and the hosts no healthy member owns, and `lastSeq` is the commit index |
| GET | `cluster/quorum` | | `QuorumView`: `{nodes: [{node, addr, stale, error, reportedMs, status}]}`, one per member or learner. `status` is the node's `/qlog/status` as it serialized it (snake_case: role, epoch, leader, last, commit, emitted, flushed F, reserve R, flush, members, learners, switches, recovered, counters, `commit_us`, disk), passed through so new fields show up |
| POST | `cluster/quorum/members` | `{members: [...], addrs?: {node: "host:port"}}`: the whole member set wanted, and addresses for nodes the leader can't dial yet | the leader's status after the change. Sent to the leader with `--qlog-admin-token`, and a 400 when the node has none. A new node joins as a learner, and a removed one retires |
| GET | `settings` | | `SettingsView`: `{binary, version, entries: [{flag, env, value, source, default, secret, set, help}]}` for every process flag. `source` is `flag`, `env`, `default` or `unset`. A secret (`hide_env_values`, or a name with token, secret, access_key or password) has `value` and `default` null and only says whether it's `set` |
| GET | `ops/pipeline` | | `PipelineView`: this node's events in flight (read, not yet answered by the leader), the oldest one's age, the lane queue, paused readers and every pipeline gauge, and its hosts with the most in flight or a paused reader |
| GET | `ops/tail` | `host`, `rejects=1`, `sinceMs`, `limit` (default 200) | `TailFrame[]`, newest first: `{atMs, host, did, kind, reason?, detail?, upstreamSeq?, seq?, event?}`. `rejects=1` gives the frames this node read that never reached the stream, `kind` `reject` or `held` (an account the relay throttled, or a new one deferred). `host` narrows them to one host and adds its `passed` frames with the seq they went out at, from the last 8,192 this node read. One of the two is required |
| GET | `store` | | `StoreView`: the object store as this node uses it. `total` and `purposes` (flush, state, leader, recovery, backfill, retain) give requests by class (`a`, `b`, `free`) since start, `perSec` over `windowSecs` (since this node's previous sample, at most every 10 s; 0 on the first call), and payload bytes up and down. `latency` is per op (count, mean, p50, p99 from the histogram's buckets). `retention` is `retain/qlog` as the leader's last pass wrote it: the segments and state paths with their sizes, what was deletable and what it deleted, and `pruned_seq` |
| GET | `ops/plc` | | `PlcView` with `enabled: false`. PLC export seeding isn't built yet on the quorum log |
| GET | `accounts` | `q`: a DID, a handle or a prefix | up to 100 `Account`s |
| GET | `accounts/{did}` | | `Account`: handle, host, the relay's status and the host's, takedown, rev |
| POST | `accounts/{did}/takedown` | `{reason}` (required) | `Account` |
| POST | `accounts/{did}/untakedown` | | `Account` |
| GET | `cases` | `status`: open, acknowledged, resolved, dismissed | `Case[]`, worst severity first |
| GET, POST | `cases/{id}` | POST `{status?, note}` | `Case` |
| GET | `cases/{id}/evidence` | | `CaseDetail`: the case, its trip count and the newest trips (what was measured, every signal's count at the time) |

## Policy

The tier limits and spam thresholds are one object with a version. That way a change touching
several limits lands on every node at once, and two operators can't silently overwrite each other.
A PUT carries the version it was edited from, and a stale one gets a 409 telling the operator to
reload. Each accepted change appends an audit entry listing every changed leaf
(`tiers.standard.eventsPerSec: 50 → 80`), computed by `admin::diff_json`.

`validate_policy` runs on every PUT before the source sees it. It checks that the default tier
exists, rates are positive, the hourly and daily caps aren't below the per-second rate and the
reject ratio is between 0 and 1. The UI runs the same checks before it offers Save.

A domain rule is `example.com` (that host) or `*.example.com` (the domain and every subdomain), with
an effect of `ban`, `allow`, `tier` or `throttle`. An exact rule may also name one host by IPv4
address or `localhost`, with a port (`127.0.0.1:30003`), which is how dev-network hosts are known.

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
| `hostsConnected`, `consumers` | counts only |
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
| Tier, throttle, suspend, ban, domain rules, policy, cases | any node, through the shared policy store in the bucket |
| Cluster and Quorum pages | every member's `status`, asked over the peer port with an 800 ms timeout. A member that doesn't answer is `stale`, with the error, and the page never waits on it |
| Accounts | the leader, which holds every account's record. On a follower an account read is a 400 naming the leader |
| Takedown, untakedown, release-throttled | any node. Each goes to the leader as an entry on the log (the record's flag and the `#account` frame together). On a follower the `Account` in a takedown's answer has only what that node knows (status, takedown, the leader as `node`) |
| Admissions, tail, store | this node: the `requestCrawl`s it answered, the frames it read, the requests it sent. Retention is the leader's last pass, read from the bucket |
| Membership changes | any node, which sends them to the leader with `--qlog-admin-token` |

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
joins. Settings is a plausible production config, and the full policy document starts from the
defaults with a few edits. Actions, rules, policy edits, membership changes and takedowns change
the simulation, but nothing is persisted.
