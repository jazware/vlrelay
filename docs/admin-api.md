# vlRelay operator API

The dashboard (`ui/`) talks to JSON endpoints under `/admin/api/`. The wire types live in
`src/admin.rs` and `ui/src/lib/api.ts` mirrors them. Field names are camelCase and times are unix
milliseconds (`...Ms`) unless a field says seconds.

Every endpoint needs `Authorization: Basic admin:<token>`, the same scheme as the vlpds console, so
`curl -u admin:$TOKEN` works. A bad or missing token is a 401. Errors come back as
`{"error": "...", "message": "..."}` with 400 (`InvalidRequest`), 404 (`NotFound`) or 409
(`VersionConflict`).

The router is generic over the `AdminSource` trait, so the relay implements that trait over its
real state and gets the endpoints and the UI for free. Until then `admin::demo::Demo` simulates a
busy relay (below).

## Endpoints

| Method | Path | Body / query | Returns |
|---|---|---|---|
| GET | `overview` | | `Overview`: events/s in and out, bytes/s, consumers, hosts connected/total and by status, rejects/s by reason, time to firehose p50/p99, log durability lag, open cases, the busiest hosts and 5 min of 1 s history for the charts |
| GET | `hosts` | `q`, `tier`, `status`, `sort`, `desc`, `limit` (default 10,000), `offset` | `{total, hosts: HostRow[]}` |
| GET | `hosts/{host}` | | `HostDetail`: the row, the limits in force, rejects by reason, a sample of recent rejects, 2 min of per-second events and rejects, operator actions, open cases |
| POST | `hosts/{host}/action` | `{"action": "set-tier", "tier"}`, `{"action": "throttle", "eventsPerSec": n or null}`, `{"action": "suspend", "reason"}`, `{"action": "ban", "reason"}`, `{"action": "unban"}`, `{"action": "reconnect"}` | the updated `HostRow` |
| GET, POST | `domain-rules` | POST `{pattern, effect, note}` | `DomainRule[]`, or the new rule |
| PUT, DELETE | `domain-rules/{id}` | PUT as POST | the rule, or 204 |
| GET, PUT | `policy` | PUT `{baseVersion, policy, note}` | `PolicyDoc` (version, policy, updated at/by) |
| GET | `policy/audit` | | `PolicyAudit[]`, newest first |
| GET | `consumers` | | `Consumer[]` |
| POST | `consumers/{id}/kick` | | 204 |
| GET | `cluster` | | nodes (lease, shards, rates, build) and the owner of every host shard and DID shard |
| GET | `accounts` | `q`: a DID, a handle or a prefix | up to 100 `Account`s |
| GET | `accounts/{did}` | | `Account` |
| POST | `accounts/{did}/takedown` | `{reason}` (required) | `Account` |
| POST | `accounts/{did}/untakedown` | | `Account` |
| GET | `cases` | `status`: open, acknowledged, resolved, dismissed | `Case[]`, worst severity first |
| GET, POST | `cases/{id}` | POST `{status?, note}` | `Case` |

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
an effect of `ban`, `tier` or `throttle`.

## Demo backend

```bash
cd ui && npm ci && npm run build && cd ..
cargo run --bin admin_demo            # http://127.0.0.1:2790/admin, token "demo"
cd ui && npm run dev                  # :5790 with hot reload, proxies /admin/api to :2790
```

The demo holds 5,000 hosts. 24 big hosts carry most of the traffic (~2k events/s each), 60
community PDSes are mid-sized and the rest is a long tail of self-hosted PDSes, mostly idle. Four
buggy implementations reject 25-45% of their frames, and seven spam hosts trip the policy's
thresholds (account farms, forged signatures, single-account floods). A 1 s tick moves every rate
with a compressed 20 min "day" and the odd surge, so the totals land between ~45k and ~75k
events/s. There are ~25 consumers (a couple replaying from old cursors), a 3-node cluster with 64
host shards and 256 DID shards, and cases open when a host crosses a threshold. Actions, rules,
policy edits and takedowns change the simulation, but nothing is persisted.

Screenshots: `docs/assets/dashboard-overview.jpg`, `docs/assets/dashboard-hosts.jpg`.
