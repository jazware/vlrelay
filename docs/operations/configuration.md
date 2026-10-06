---
title: Configuration
section: Operations
order: 102
summary: "Every flag of vlrelay and its env var, generated from vlrelay --help by just config-doc."
---

```hero
diagram:
  caption: One binary, configured by flags or env vars. The bucket and the prefix are the relay's identity. Everything else is how this node serves, which upstreams it starts with, how its log seals segments, and its place in a cluster.
  nodes:
    - { id: bin, label: vlrelay, sub: flags · VLRELAY_* env, at: [14, 4.5], size: [9, 3], tone: accent }
    - { id: serve, label: Serving, sub: "--listen · --admin-token", at: [0, 0], size: [10, 3], tone: blue }
    - { id: up, label: Upstreams, sub: "--host · --crawl · --plc-export", at: [0, 4.5], size: [10, 3], tone: muted }
    - { id: clu, label: Cluster, sub: "--role · --peer-tls-dir", at: [0, 9], size: [10, 3], tone: accent }
    - { id: bucket, label: Bucket, sub: "--s3-* · --prefix", at: [27, 0], size: [10, 3], shape: store, tone: amber }
    - { id: log, label: Log, sub: "--linger-ms · --retention", at: [27, 4.5], size: [10, 3], tone: amber }
    - { id: pipe, label: Pipeline, sub: "--did-shards · --lanes", at: [27, 9], size: [10, 3], tone: accent }
  edges:
    - serve.r -> bin.l30
    - up.r -> bin.l
    - clu.r -> bin.l70
    - bin.r30 -> bucket.l
    - bin.r -> log.l
    - bin.r70 -> pipe.l
facts:
  - { value: "2980", label: the public port, note: "`--listen`; the image binds 0.0.0.0", tone: accent }
  - { value: "25", unit: ms, label: segment linger, note: "`--linger-ms`; time to firehose is about linger plus one PUT", tone: amber }
  - { value: "72", unit: h, label: of log for cursor replay, note: "`--retention`", tone: blue }
  - { value: "flag", label: wins over its env var, note: "pass secrets as env vars; --help never prints them", tone: violet }
```

Every flag of `vlrelay`, generated from `vlrelay --help` by `just config-doc` (`build/config_doc.py`).
Flags with an env var can be set either way, and the flag wins. Secrets (`--s3-secret-key`,
`--admin-token`, `--internal-token`) are best passed as env vars, and `--help` never prints their
values.

The image sets `VLRELAY_LISTEN=0.0.0.0:2980` and passes `--ui-dir /usr/share/vlrelay/ui` in its
entrypoint ([Deploy](deploy.md#the-image)).

## Serving

| Flag | Env | Default | What |
|---|---|---|---|
| `--listen <LISTEN>` | `VLRELAY_LISTEN` | `127.0.0.1:2980` | Serves subscribeRepos, the sync API, requestCrawl, /admin and /metrics |
| `--trusted-proxy <TRUSTED_PROXIES>` | `VLRELAY_TRUSTED_PROXIES` |  | Proxies whose `X-Forwarded-For` names the client (CIDRs, repeatable or comma-separated): per-IP limits key on its rightmost address that isn't one of these. Other peers' headers are ignored |
| `--admin-token <ADMIN_TOKEN>` | `VLRELAY_ADMIN_TOKEN` |  | Turns on /admin (dashboard and API) with this token |
| `--ui-dir <UI_DIR>` |  |  | A built dashboard (`ui/dist`); default: this tree's, if built |

## Bucket

Without `--memory`, the four `--s3-*` values are required. Every node of a cluster uses the same bucket and `--prefix`.

| Flag | Env | Default | What |
|---|---|---|---|
| `--memory` |  |  | Everything in memory: nothing survives a restart |
| `--s3-endpoint <S3_ENDPOINT>` | `VLRELAY_S3_ENDPOINT` |  | The S3 API endpoint, e.g. `https://s3.us-east-1.amazonaws.com` or `http://minio:9000` |
| `--s3-bucket <S3_BUCKET>` | `VLRELAY_S3_BUCKET` |  | The bucket |
| `--s3-access-key <S3_ACCESS_KEY>` | `VLRELAY_S3_ACCESS_KEY` |  | Access key id |
| `--s3-secret-key <S3_SECRET_KEY>` | `VLRELAY_S3_SECRET_KEY` |  | Secret access key |
| `--s3-region <S3_REGION>` | `VLRELAY_S3_REGION` | `auto` | The bucket's region |
| `--s3-unsigned-payload <S3_UNSIGNED_PAYLOAD>` | `VLRELAY_S3_UNSIGNED_PAYLOAD` |  | Send PUT bodies as SigV4 UNSIGNED-PAYLOAD instead of hashing each one (default: on for an https endpoint, where TLS covers the body) [possible values: true, false] |
| `--prefix <PREFIX>` | `VLRELAY_PREFIX` | `vlrelay` | Key prefix in the bucket: one relay per prefix |

## Upstreams and identity

`--host` and `--crawl` work on any core node of a cluster, since the host registry is in the bucket.

| Flag | Env | Default | What |
|---|---|---|---|
| `--host <HOSTS>` |  |  | An upstream to subscribe to (repeatable). `http://` means plain `ws://` (dev mode); a bare hostname means `wss://` |
| `--crawl` |  |  | Accept com.atproto.sync.requestCrawl |
| `--host-tier <HOST_TIER>` |  | `trusted` | The tier a --host upstream starts at the first time it's seen. After that its record's tier holds (operators, auto-throttle) |
| `--plc-url <PLC_URL>` | `VLRELAY_PLC_URL` | `https://plc.directory` | The PLC directory `did:plc` documents are resolved against |
| `--dev-mode` |  |  | Allows plain ws://, IPs, localhost and ports for upstreams and DID documents. Implied by an http:// --host or a loopback --plc-url |
| `--did-lookups-per-sec <DID_LOOKUPS_PER_SEC>` |  | `50` | DID document fetches per second, all DIDs together |

## PLC export seeding

A cold relay would resolve each of ~56M accounts once at the PLC lookup budget (about 31 h at 500/s). With `--plc-export` it reads the directory's `/export` instead and keeps each did:plc's key and PDS in its DID shard, so a cache miss costs no lookup. The cursors checkpoint to `plc/export-checkpoint.json` in the bucket, so a restart resumes; once caught up it follows the export's tail. A signature that fails against a seeded key, and every `#identity`, still resolve from PLC ([Policy](../policy.md#plc-export-seeding)).

| Flag | Env | Default | What |
|---|---|---|---|
| `--plc-export` | `VLRELAY_PLC_EXPORT` |  | Seed DID documents from the PLC directory's /export (resumable, then follows its tail), so a cold relay doesn't resolve each account. On a cluster the lowest-named live core reads it |
| `--plc-export-url <PLC_EXPORT_URL>` | `VLRELAY_PLC_EXPORT_URL` |  | The directory --plc-export reads (default: --plc-url) |
| `--plc-export-rate <PLC_EXPORT_RATE>` |  | `2` | /export requests per second, all streams together |
| `--plc-export-streams <PLC_EXPORT_STREAMS>` |  | `4` | Time windows of the export read side by side on a fresh start |

## Log

Time to firehose is about linger plus one segment PUT. Above ~50k events/s segments seal on size before the linger is up ([Performance](../perf.md)).

| Flag | Env | Default | What |
|---|---|---|---|
| `--linger-ms <LINGER_MS>` |  | `25` | Segment linger: a segment seals this long after its first event (docs/design.md, "Decisions") |
| `--log-inflight <LOG_INFLIGHT>` |  | `32` | Segment PUTs in flight at once |
| `--max-segment-mb <MAX_SEGMENT_MB>` |  | `8` | A segment seals at this size even before its linger is up |
| `--log-compression <LOG_COMPRESSION>` |  | `-1` | zstd level for log segments: 0 stores them uncompressed, negative levels are zstd's fast ones. Firehose frames are mostly hashes: on production frames -1 compresses 1.8x faster than 1 for 0.6% more bytes (docs/perf.md, "Compression") |
| `--retention <RETENTION>` |  | `72` | How long the log keeps events for cursor replay, in hours |

## Pipeline and state

| Flag | Env | Default | What |
|---|---|---|---|
| `--did-shards <DID_SHARDS>` |  |  | DID state shards (SlateDB instances). Default: 4 on one node, 24 in a cluster. Only read when the bucket has no DID layout yet |
| `--lanes <LANES>` |  | `64` | Pipeline lanes; a DID always maps to the same one |
| `--ingest-threads <INGEST_THREADS>` |  |  | Threads verifying events (default: the core count, at most 16) |

## Quorum cluster

With `--quorum` the node is a member of a quorum cluster (one member is a single node with its commitlog as the WAL). Every member uses the same bucket and `--prefix` ([Quorum cluster](../quorum-cluster.md)).

| Flag | Env | Default | What |
|---|---|---|---|
| `--quorum` | `VLRELAY_QUORUM` |  | Run on the quorum log |
| `--qlog-listen <QLOG_LISTEN>` | `VLRELAY_QLOG_LISTEN` | `127.0.0.1:2978` | The peer protocol: replication, submits, members' questions |
| `--qlog-peer <QLOG_PEERS>` | `VLRELAY_QLOG_PEERS` |  | Another node: `id=host:port` of its --qlog-listen (repeatable, or comma-separated) |
| `--qlog-members <QLOG_MEMBERS>` | `VLRELAY_QLOG_MEMBERS` |  | The bootstrap member set (default: this node and its peers); after the first start, `qlog/leader` holds it |
| `--qlog-dir <QLOG_DIR>` | `VLRELAY_QLOG_DIR` |  | The commitlog's directory (NVMe). Without one the log is memory only |
| `--qlog-flush-ms <QLOG_FLUSH_MS>` |  | `30000` | The bucket flush interval |
| `--qlog-headroom <QLOG_HEADROOM>` |  | `8640000` | Seqs reserved past each flush (R = F + H) |
| `--qlog-admin-token <QLOG_ADMIN_TOKEN>` | `QLOG_ADMIN_TOKEN` |  | Bearer token membership changes need (`qlog member`, the dashboard) |
| `--qlog-retain-hours <QLOG_RETAIN_HOURS>` |  | `72` | Bucket retention, run by the leader: segments older than this go (0: never) |
| `--qlog-retain-secs <QLOG_RETAIN_SECS>` |  |  | Dev: retention in seconds instead |
| `--qlog-retain-every-secs <QLOG_RETAIN_EVERY_SECS>` |  | `600` |  |
| `--qlog-host-failover-ms <QLOG_HOST_FAILOVER_MS>` |  | `2000` | A member silent this long loses its hosts to the others |
| `--qlog-host-poll-ms <QLOG_HOST_POLL_MS>` |  | `500` |  |
| `--qlog-election-ms <QLOG_ELECTION_MS>` |  | `1000` |  |
| `--qlog-heartbeat-ms <QLOG_HEARTBEAT_MS>` |  | `100` |  |
| `--qlog-state-compactor-poll-ms <QLOG_STATE_COMPACTOR_POLL_MS>` |  | `30000` | The state's SlateDB compactor and worker poll |
| `--qlog-no-auto-recover` |  |  | A lost quorum waits for an operator instead of recovering from the bucket |
| `--qlog-segment-mb <QLOG_SEGMENT_MB>` |  | `64` |  |
| `--qlog-disk-retain-mb <QLOG_DISK_RETAIN_MB>` |  | `4096` |  |
| `--qlog-memory-mb <QLOG_MEMORY_MB>` |  |  | Committed log kept in memory (default 64 with --qlog-dir, else 512) |
| `--qlog-crash-at <QLOG_CRASH_AT>` |  |  | Chaos: kill -9 at this flush step (or `any`), with --qlog-crash-prob |
| `--qlog-crash-prob <QLOG_CRASH_PROB>` |  | `0.05` |  |
| `--qlog-crash-stop-file <QLOG_CRASH_STOP_FILE>` |  |  | Chaos: no crash injected once this file exists |
| `--qlog-power-cut-on-usr1` |  |  | Chaos: SIGUSR1 is a power cut |
| `--qlog-fsync-delay-us <QLOG_FSYNC_DELAY_US>` |  |  | Chaos: sleep this long before each commitlog fsync (emulates a disk) |

## Lease cluster

The first cluster, kept for comparison: it runs only with `--legacy-cluster`. Without `--cluster` or `--role` the node runs alone. Core and edge nodes need `--peer-tls-dir` and `--internal-token`, and a replica needs neither ([Deploy](deploy.md#a-cluster), [Cluster](../cluster.md)).

| Flag | Env | Default | What |
|---|---|---|---|
| `--node-id <NODE_ID>` | `VLRELAY_NODE_ID` | `relay` | Node id: the node log's id prefix, and the cluster member name |
| `--legacy-cluster` | `VLRELAY_LEGACY_CLUSTER` |  | The lease cluster (`--role`, `--cluster`), which the quorum log (`--quorum`) supersedes: kept for comparison, off unless asked for |
| `--cluster` |  |  | Run as a core cluster node (the same as --role core) |
| `--role <ROLE>` |  |  | Cluster role: core (lease, shards, a log), edge (follows every log over peer mTLS) or replica (follows every log from the bucket) `core`: Lease, DID and host shards, a log; serves · `edge`: Follows every log over peer mTLS and serves. No lease, no shards · `replica`: Follows every log from the bucket (read-only) and serves |
| `--peer-listen <PEER_LISTEN>` | `VLRELAY_PEER_LISTEN` | `127.0.0.1:2979` | The peer listener (node-to-node mTLS): forwarding, log streams |
| `--advertise-url <ADVERTISE_URL>` | `VLRELAY_ADVERTISE_URL` |  | `https://host:port` peers reach --peer-listen at |
| `--peer-tls-dir <PEER_TLS_DIR>` | `VLRELAY_PEER_TLS_DIR` |  | Peer TLS: `ca.crt`, `{node-id}.crt`, `{node-id}.key` (vlpds admin tls ca / issue). With --dev-mode they're created as needed |
| `--internal-token <INTERNAL_TOKEN>` | `VLRELAY_INTERNAL_TOKEN` |  | Shared secret on every peer request |
| `--lease-ttl-ms <LEASE_TTL_MS>` |  | `10000` | Node lease TTL: a crashed core node's shards move after about this plus a fifth of it |
| `--host-shards <HOST_SHARDS>` |  | `64` | Host shards (used only when the bucket has no host layout yet) |

## Other

| Flag | Env | Default | What |
|---|---|---|---|
| `--admin-follower <ADMIN_FOLLOWERS>` | `VLRELAY_ADMIN_FOLLOWERS` |  | An edge's or a replica's public URL (repeatable, or comma-separated), for a core's dashboard to include its numbers and consumers. They answer with the same --admin-token |
| `--retention-secs <RETENTION_SECS>` |  |  | Dev mode only: retention in seconds instead (overrides --retention), so a test can see `OutdatedCursor` |
| `--max-lag-mb <MAX_LAG_MB>` |  |  | Dev mode only: how far a live consumer may fall behind before `ConsumerTooSlow`, in MiB (default 128) |
| `--host-inflight-events <HOST_INFLIGHT_EVENTS>` |  | `8192` | Upstream frames one host may have read and not yet durable; past it (or its MB cap) the host's socket isn't read |
| `--host-inflight-mb <HOST_INFLIGHT_MB>` |  | `64` |  |
| `--inflight-events <INFLIGHT_EVENTS>` |  | `32768` | The same over every host together |
| `--inflight-mb <INFLIGHT_MB>` |  | `384` |  |
