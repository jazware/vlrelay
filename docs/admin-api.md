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
| GET | `overview` | | `Overview`: events/s in and out, bytes/s, consumers, hosts connected/total and by status, rejects/s by reason, time to firehose p50/p99, log durability lag, open cases, the busiest hosts and 5 min of 1 s history for the charts. On a cluster also `byNode` (each node's share, stale ones flagged) and `streamEventsPerSec` |
| GET | `hosts` | `q`, `tier`, `status`, `sort`, `desc`, `limit` (default 10,000), `offset` | `{total, hosts: HostRow[]}` |
| GET | `hosts/{host}` | | `HostDetail`: the row, the limits in force, rejects by reason, a sample of recent rejects, 2 min of per-second events and rejects, operator actions, open cases |
| POST | `hosts/{host}/action` | `{"action": "set-tier", "tier"}`, `{"action": "throttle", "eventsPerSec": n or null}`, `{"action": "suspend", "reason"}`, `{"action": "ban", "reason"}`, `{"action": "unban"}`, `{"action": "reconnect"}` | the updated `HostRow` |
| GET, POST | `domain-rules` | POST `{pattern, effect, note}` | `DomainRule[]`, or the new rule |
| PUT, DELETE | `domain-rules/{id}` | PUT as POST | the rule, or 204 |
| GET, PUT | `policy` | PUT `{baseVersion, policy, note}` | `PolicyDoc` (version, policy, updated at/by) |
| GET | `policy/audit` | | `PolicyAudit[]`, newest first |
| GET, PUT | `policy/full` | PUT `{baseVersion, policy, note}` | `FullPolicyDoc`: the engine's whole document (tier limits, transitions, spam thresholds and actions, cluster budgets, consumer limits, crawl settings) |
| GET | `domain-rules/audit` | | `PolicyAudit[]` of the domain rules, newest first |
| GET | `consumers` | | `Consumer[]` |
| POST | `consumers/{id}/kick` | `node` (ids are per node; default the node answering) | 204 |
| GET | `cluster` | | nodes (role, lease, shards, rates, consumers, stream seq, CPU, memory, build, `stale`) and the owner of every host shard and DID shard |
| GET | `ops/pipeline` | | `PipelineView`: per core the events in flight (read, not yet durable), the oldest one's age, the lane queue, restart-dedupe entries, paused readers and every pipeline gauge; the hosts with the most in flight or a paused reader |
| GET | `ops/seq` | | `SeqView`: each node's stream head and newest seq checkpoint, and the recent 10 s boundaries with the seq each node counted, flagged where they disagree |
| GET | `ops/archive` | | `ArchiveView`: archival mode and policy version, mirrored repos, fetch queue (queued, running), failed, retried, fetched bytes and records, mismatches, per core, and the newest fetch failures |
| GET | `ops/plc` | | `PlcView`: the core reading the PLC export, ops read and their rate, written, throttled (429s), errors, caught up or not, per-window progress from the stored checkpoint, per core |
| GET | `cluster/layout` | | the DID shard layout: `{version, shards: [{id, lo, hi, owner}], nextId, op}` (cluster core nodes only) |
| POST | `cluster/reshard` | `{"op": "split", "shard", "at"?, "wait"?}`, `{"op": "merge", "left", "right", "wait"?}`, `{"op": "abort"}` | `{op, done?, layout}` (`done` with `wait`: the op flipped). See [Cluster](cluster.md#resharding) |
| GET | `accounts` | `q`: a DID, a handle or a prefix | up to 100 `Account`s |
| GET | `accounts/{did}` | | `Account`, with `archive` (wanted, mirrored, mirror rev, fetching, staging, last fetch error, takedown time) on an archiving relay |
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
cache (every account with recent traffic).

`GET /admin/api/archive` (archival's raw counters, this node's shards only), `.../archive/fetch` and
`.../archive/desync` are docs/archival.md's. `ops/archive` is the cluster's summary.

## On a cluster

Any core node's dashboard shows the whole cluster. The node answering asks every other member for
its own numbers over the peer admin RPC (`node::peer_admin`) and adds them up (`admin::fleet`):

| Member | Found from | Asked at | Auth |
|---|---|---|---|
| core | the leases | its peer listener, `/internal/relay/v1/admin/{report,hosts,host,reconnect,consumers,kick,account,accounts,takedown}` | peer mTLS and the internal token |
| edge, replica | `--admin-follower URL` on each core (repeatable or comma-separated; `VLRELAY_ADMIN_FOLLOWERS`) | its public listener, `/admin/api/node/{report,consumers,kick}` | the admin token (`--admin-token` on the follower, the same as the cores') |

Followers answer on their public listener because a replica has no peer listener and a core's peer
client only trusts origins that hold a lease. A core with no `--admin-token` still serves the peer
routes, so a dashboard on another core can include it.

- **Totals are sums over the nodes that answered.** Overview rates, bytes, consumers, hosts and
  rejects add up; time to firehose and durability lag take the worst node; history adds the
  per-second samples by time. `byNode` lists each node's share, so the totals equal the sum of the
  rows. `eventsOutPerSec` sums every node's emits; `streamEventsPerSec` is the stream's own rate.
- **Stale members.** A member that errors or takes over 1.5 s is `stale` for that round, with the
  error. Its rows show zeros (the UI shows dashes) and it's left out of the sums, so the page never
  waits on a dead node. A killed core stays listed until its lease lapses, then drops off; a
  follower stays listed (as stale) until it answers again. One round of reports is shared by the
  requests of one refresh (0.8 s), so the pages don't multiply peer calls.
- **Hosts.** Every host is listed once, from the core reading it (its host shard owner) with its
  live rate. A host whose owner didn't answer shows this node's registry row with no rate, under the
  owner's name. A host's detail page and a reconnect go to the owner; tier, throttle, suspend and
  ban go through the shared policy store as before.
- **Consumers** are every node's, edges and replicas included, each with its `node`. Kicks go to
  that node.
- **Accounts.** An account's page and a takedown go to its DID shard's owner, which holds its state.
  A handle search asks every core (handles are cached by the core reading the account's host) and
  merges the answers.
- **Pipeline** gauges are every `vlrelay_*` gauge whose name says in-flight, pending, queued,
  backlog, paused, cap, dedupe or lag, read by name, so the in-flight caps and pause metrics the
  pipeline adds show up without a dashboard change. `inflightCap` per host is null until the relay
  has one.

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

Screenshots: `docs/assets/dashboard-overview.jpg`, `docs/assets/dashboard-hosts.jpg` (the demo);
`docs/assets/dashboard-overview-cluster.jpg`, `docs/assets/dashboard-cluster.jpg` (a dev cluster of
three cores, an edge and a replica under load, one core killed).
