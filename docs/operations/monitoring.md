# Monitoring vlRelay

Every node serves Prometheus metrics at `GET /metrics` on its `--listen` port, with no auth (keep
it off the public side of your proxy, see [Deploy](deploy.md#in-front-of-it)). The relay's own
series start with `vlrelay_`. vlpds's firehose, log and process series come with them, since
vlRelay runs vlpds's code for those parts.

There are no alert rules or Grafana dashboards for vlRelay yet. The operator dashboard at `/admin`
([Admin API](../admin-api.md)) covers the live view.

## What to watch first

| Question | Series |
|---|---|
| Is it keeping up? | `vlrelay_events_in_total` against `vlrelay_events_accepted_total` and `vlrelay_events_out_total`, plus `vlrelay_ack_pending` and `vlrelay_lane_queued` (both should stay flat) |
| How fresh is the stream? | `vlrelay_time_to_firehose_seconds` (upstream frame received to emitted), p50 and p99 |
| Is the bucket slow? | `vlrelay_durable_lag_ms`, `vlrelay_time_to_durable_seconds`, `vlpds_segment_put_seconds{partition="relay"}` |
| Are upstreams sending bad data? | `vlrelay_events_rejected_total` by `reason` |
| Are hosts connected? | `vlrelay_hosts` by `status` |
| Who's reading? | `vlrelay_consumers` |

## The vlrelay series

Defined in `src/node/metrics.rs` and `src/node/cluster.rs`.

| Series | Type | Labels | What |
|---|---|---|---|
| `vlrelay_events_in_total` | counter | `kind` | Frames read off upstream sockets |
| `vlrelay_events_accepted_total` | counter | `kind` | Events appended to the node log |
| `vlrelay_events_out_total` | counter | | Events emitted on `subscribeRepos` by the merger |
| `vlrelay_events_rejected_total` | counter | `reason` | Upstream events dropped |
| `vlrelay_events_duplicate_total` | counter | `at` | Upstream events already applied (replays after a reconnect or restart), by where they were caught |
| `vlrelay_events_skipped_total` | counter | `kind` | Upstream frames not relayed on purpose (`#info`, unknown types) |
| `vlrelay_time_to_firehose_seconds` | histogram | | Upstream frame received to emitted on `subscribeRepos` |
| `vlrelay_time_to_durable_seconds` | histogram | | Upstream frame received to its log segment durable and its state committed |
| `vlrelay_stage_seconds` | histogram | `stage` | Wall time per event in a pipeline stage |
| `vlrelay_stage_busy_us_total` | counter | `stage` | Microseconds spent in a stage, summed over events |
| `vlrelay_durable_lag_ms` | gauge | | Mean append-to-durable time over the last second |
| `vlrelay_hosts` | gauge | `status` | Upstream hosts by status |
| `vlrelay_consumers` | gauge | | Connected `subscribeRepos` consumers |
| `vlrelay_ack_pending` | gauge | | Upstream events read but not yet durable, rejected or skipped |
| `vlrelay_lane_queued` | gauge | | Events queued in front of the pipeline lanes |
| `vlrelay_accounts_throttled_total` | counter | `why` | New accounts created throttled by policy (`host_cap`) |
| `vlrelay_accounts_deferred_total` | counter | `why` | Events of new accounts dropped while a new-account budget was spent (`host_rate`, `cluster_budget`) |
| `vlrelay_cluster_dedupe_entries` | gauge | | (host, upstream seq) pairs the DID owner holds until the host's checkpoint passes them |
| `vlrelay_cluster_shard_open_seconds` | histogram | | Opening a batch of DID shards: state open plus replay of earlier owners' log spans |

Label values:

- `kind` is `commit`, `sync`, `identity` or `account`.
- `stage` is `parse`, `identity`, `verify` or `apply`. Parse and verify are CPU only. Identity
  includes DID document lookups, and apply includes the DID owner's lookups.
- `at` is `restart_log`, `state`, `owner` or `cluster_recent`.
- `reason` comes from two places. The state step gives `stale`, `wrong_host`, `inactive`,
  `desynchronized`, `rev_not_newer`, `prev_data_mismatch`, `chain`, `rate_limited`,
  `new_account_deferred`, `no_identity`, `bad_cid`, `not_owner`, `identity_unavailable` and
  `store`. Verification failures carry the verifier's own names (`bad_op`, `commit_rev_mismatch`
  and others, `src/verify.rs`). [Policy](../policy.md) says which of them count against a host.

## Reading them

`vlrelay_ack_pending` is the backlog between reading an event and making it durable. When it
climbs and keeps climbing, the node is behind. [Performance](../perf.md) found the committer to be
the first stage to give out on one node (~90-95k events/s), and this gauge is how that showed up.

Time to firehose is linger plus a segment PUT (`--linger-ms`, 25 by default). Above ~50k events/s
a segment seals on size (`--max-segment-mb`) before its linger is up, so the PUT latency of your
bucket sets the p50.

`vlrelay_stage_busy_us_total` divided by `vlrelay_events_in_total` is CPU per event in each
stage. On the perf bench the whole node used ~70 µs of CPU per event, and ~33 µs of it was
signature verification.
