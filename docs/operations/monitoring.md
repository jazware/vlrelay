---
title: Monitoring
section: Operations
order: 103
summary: "The Prometheus series every node serves at /metrics, the quorum log's /qlog/status, what to watch first, and how to read them."
---

```hero
diagram:
  caption: Where the main series are measured along an event's path on one node. Commit lag is the time from submitting an event to the leader until a quorum holds it, and time to firehose runs from the frame arriving to it going out on subscribeRepos.
  nodes:
    - { id: pds, label: PDS socket, sub: "`vlrelay_events_in_total`", at: [0, 0], size: [10, 3], tone: muted }
    - { id: pipe, label: Pipeline, sub: "`vlrelay_stage_seconds`", at: [13, 0], size: [10, 3], tone: accent }
    - { id: log, label: Leader commit, sub: "`vlrelay_durable_lag_ms`", at: [26, 0], size: [10, 3], tone: violet }
    - { id: out, label: subscribeRepos, sub: "`vlrelay_events_out_total`", at: [39, 0], size: [10, 3], tone: blue }
    - { id: rej, label: Rejects, sub: "`…_rejected_total{reason}`", at: [13, 6], size: [10, 3], tone: danger }
    - { id: q, label: Quorum, sub: "`/qlog/status`", at: [26, 6], size: [10, 3], tone: solid }
  edges:
    - pds -> pipe
    - "pipe -> log: submit"
    - "log -> out: commit"
    - { from: pipe.b, to: rej.t, label: dropped, dash: true }
    - { from: q.t, to: log.b, label: role · commit · flush, dash: true }
facts:
  - { value: "/metrics", label: on every node, note: "on the --listen port, with no auth: keep it private", tone: blue }
  - { value: "/qlog/status", label: the quorum log, note: "JSON on the same port, also no auth", tone: violet }
  - { value: "flat", label: "`vlrelay_lane_queued`", note: "climbing and still climbing means the node is behind", tone: danger }
  - { value: "0", unit: alert rules, label: shipped so far, note: "the dashboard at /admin is the live view", tone: muted }
```

Every node serves Prometheus metrics at `GET /metrics` on its `--listen` port, with no auth (keep
it off the public side of your proxy, see [Deploy](deploy.md#in-front-of-it)). The relay's own
series start with `vlrelay_`. vlpds's firehose, object store and process series come with them,
since vlRelay runs vlpds's code for those parts.

The quorum log has no Prometheus series of its own. Each node reports it as JSON at
`GET /qlog/status` on the same port, and the dashboard's Quorum page shows every member's.

There are no alert rules or Grafana dashboards for vlRelay yet. The operator dashboard at `/admin`
([Admin API](../admin-api.md)) covers the live view.

## What to watch first

| Question | Where |
|---|---|
| Is the quorum up? | `/qlog/status` on each node: one `leader`, every member answering, `commit` moving. The public `/api/public/stats` sums it up as `quorum` (`ok`, `degraded` or `down`) |
| Is it keeping up? | `vlrelay_events_in_total` against `vlrelay_events_accepted_total` and `vlrelay_events_out_total`, plus `vlrelay_lane_queued` and `vlrelay_upstream_inflight_events` (both should stay flat) |
| How fresh is the stream? | `vlrelay_time_to_firehose_seconds` (upstream frame received to emitted), p50 and p99 |
| Is the leader slow to commit? | `vlrelay_durable_lag_ms`, and `commit_us` and `disk.fsync_us` in `/qlog/status` |
| Is the bucket flush keeping up? | the leader's `flush` in `/qlog/status`: `last_at_ms` within `--qlog-flush-ms` (30 s), `failed` not rising |
| Are upstreams sending bad data? | `vlrelay_events_rejected_total` by `reason` |
| Are hosts connected? | `vlrelay_hosts` by `status` |
| Is a host reader falling behind? | `vlrelay_host_read_lag_max_seconds`, `vlrelay_hosts_lagging` (more than a minute behind) |
| Who's reading? | `vlrelay_consumers`, `vlpds_firehose_disconnects_total` by `reason` |

## The vlrelay series

Defined in `src/node/metrics.rs` and `src/upstream/flow.rs`. Each node counts its own: the PDSes
it reads, the events it submits and its own consumers.

| Series | Type | Labels | What |
|---|---|---|---|
| `vlrelay_events_in_total` | counter | `kind` | Frames read off upstream sockets |
| `vlrelay_events_accepted_total` | counter | `kind` | Events this node submitted that the leader appended |
| `vlrelay_events_out_total` | counter | | Events emitted on `subscribeRepos` |
| `vlrelay_events_rejected_total` | counter | `reason` | Upstream events dropped |
| `vlrelay_events_duplicate_total` | counter | `at` | Upstream events the leader had already appended (replays after a reconnect or a host move) |
| `vlrelay_events_fenced_total` | counter | | Events dropped unsent because an earlier event of their socket gave up or the host moved. The host sends them again |
| `vlrelay_events_skipped_total` | counter | `kind` | Upstream frames not relayed on purpose (`#info`, unknown types) |
| `vlrelay_time_to_firehose_seconds` | histogram | | Upstream frame received to emitted on `subscribeRepos` |
| `vlrelay_stage_seconds` | histogram | `stage` | Wall time per event in a pipeline stage |
| `vlrelay_stage_busy_us_total` | counter | `stage` | Microseconds spent in a stage, summed over events |
| `vlrelay_durable_lag_ms` | gauge | | Mean time from submitting an event to the leader to its commit, over the last second |
| `vlrelay_hosts` | gauge | `status` | Upstream hosts by status: `connected`, `idle`, `backoff`, `throttled` (held at its own limits), `backpressure` (paused because the relay is behind), `suspended`, `banned` |
| `vlrelay_host_read_lag_max_seconds` | gauge | | The furthest any host reader on this node is behind its host's stream: the newest frame's age when it was read (read time minus the event's `time`), plus the time since while the reader is held back by its limits |
| `vlrelay_hosts_lagging` | gauge | | Hosts whose reader is more than a minute behind. Hosts and host detail on the dashboard show each host's lag |
| `vlrelay_consumers` | gauge | | Connected `subscribeRepos` consumers |
| `vlrelay_identity_cache_entries` | gauge | | DID documents in the identity cache |
| `vlrelay_identity_lookups` | gauge | `outcome` | DID document lookups since start: `hit`, `seeded`, `fetched` |
| `vlrelay_forced_lookups_refused_total` | counter | | Fresh DID document fetches an event asked for that its host's budget refused (the cached document was used) |
| `vlrelay_lane_queued` | gauge | | Events queued in front of the pipeline lanes |
| `vlrelay_upstream_inflight_events` | gauge | | Upstream frames read and not yet done, all hosts |
| `vlrelay_upstream_inflight_bytes` | gauge | | The same in bytes |
| `vlrelay_upstream_host_inflight_events_max` | gauge | | The most frames any one host has in flight |
| `vlrelay_upstream_paused_hosts` | gauge | | Hosts whose socket isn't read because of an in-flight cap (`--host-inflight-events`, `--inflight-events`) |
| `vlrelay_upstream_pauses_total` | counter | `cap` | Socket reads paused at an in-flight cap |
| `vlrelay_accounts_throttled_total` | counter | `why` | New accounts created throttled by policy (`host_cap`) |
| `vlrelay_accounts_deferred_total` | counter | `why` | Events of new accounts dropped while a new-account budget was spent (`host_rate`, `cluster_budget`) |

Label values:

- `kind` is `commit`, `sync`, `identity` or `account` (and `malformed` on `events_in`).
- `stage` is `parse`, `identity` or `verify`. Parse and verify are CPU only, and identity
  includes DID document lookups.
- `at` is `owner`, the leader's check.
- `reason` comes from two places. The leader's check against the account's record gives `stale`,
  `wrong_host`, `inactive`, `desynchronized`, `rev_not_newer`, `prev_data_mismatch`, `chain`,
  `rate_limited`, `new_account_deferred`, `no_identity`, `identity_unavailable` and others.
  Verification failures carry the verifier's own names (`bad_op`, `commit_rev_mismatch` and
  others, `src/verify.rs`). [Policy](../policy.md) says which of them count against a host.

Of vlpds's series, the firehose ones apply as they are: `vlpds_firehose_subscribers`,
`vlpds_firehose_disconnects_total{reason}`, `vlpds_firehose_backfills{state}` and
`vlpds_firehose_backfill_events_total` for consumers reading old cursors from the bucket, and
`vlpds_object_store_requests_total` for every bucket request by `op` and `result`.

## The quorum log

`GET /qlog/status` is one JSON object per node. The fields worth watching:

| Field | What |
|---|---|
| `role`, `leader`, `epoch` | `leader`, `follower` or `candidate`, who leads, and the epoch, which rises at every takeover |
| `members`, `learners` | the current member set, and nodes copying the log before a membership change |
| `last`, `commit`, `emitted` | the last seq this node holds, the last one a quorum holds, and the last one it sent to consumers. On a healthy follower all three track the leader's `commit` |
| `flushed`, `reserve` | F, the last seq in the bucket, and R, the highest seq the log may commit before the next flush |
| `commit_us` | append to quorum commit on the leader, as `p50`, `p99` and `max` in microseconds |
| `disk` | the commitlog's `fsync_us`, group-commit sizes and bytes on disk (null without `--qlog-dir`) |
| `flush` | on the leader: flushes, `failed`, `duration_us` (seal to manifest) and `last_at_ms` |
| `takeovers`, `lost_quorums`, `recoveries` | counters since start. A rising `recoveries` means the log resumed from the bucket |

`?reset=true` clears `commit_us` after reading it, for a per-interval percentile. The dashboard
never resets it.

## Reading them

`vlrelay_lane_queued` and `vlrelay_upstream_inflight_events` are the backlog between reading an
event and the leader's answer. When they climb and keep climbing, the node is behind, and the
in-flight caps start pausing host sockets (`vlrelay_upstream_paused_hosts`). Those hosts, and
any whose lane queue is full, count as `vlrelay_hosts{status="backpressure"}`, not `throttled`:
the relay is behind, not the hosts.

Time to firehose is the pipeline, the submit to the leader, the quorum commit and the emit. The
commit waits until two of the three nodes hold the event on disk, so fsync time sets much of it.
`disk.fsync_us` in `/qlog/status` shows each node's.

`vlrelay_stage_busy_us_total` divided by `vlrelay_events_in_total` is CPU per event in each
stage. About a third of a node's CPU per event is signature verification
([Performance](../perf.md)).
