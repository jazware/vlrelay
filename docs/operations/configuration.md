---
title: Configuration
section: Operations
order: 102
summary: "Every flag of vlrelay and its env var, generated from vlrelay --help by just config-doc."
---

```hero
diagram:
  caption: One binary, configured by flags or env vars. The bucket and the prefix are the relay's identity. Everything else is how this node serves, which upstreams it starts with, and its place in the quorum log.
  nodes:
    - { id: bin, label: vlrelay, sub: flags · VLRELAY_* env, at: [14, 4.5], size: [9, 3], tone: accent }
    - { id: serve, label: Serving, sub: "--listen · --admin-token", at: [0, 0], size: [10, 3], tone: blue }
    - { id: up, label: Upstreams, sub: "--host · --crawl", at: [0, 4.5], size: [10, 3], tone: muted }
    - { id: clu, label: Quorum log, sub: "--qlog-peer · --qlog-dir", at: [0, 9], size: [10, 3], tone: accent }
    - { id: bucket, label: Bucket, sub: "--s3-* · --prefix", at: [27, 0], size: [10, 3], shape: store, tone: amber }
    - { id: log, label: Flush, sub: "--qlog-flush-ms · --qlog-retain-hours", at: [27, 4.5], size: [10, 3], tone: amber }
    - { id: pipe, label: Pipeline, sub: "--lanes · --ingest-threads", at: [27, 9], size: [10, 3], tone: accent }
  edges:
    - serve.r -> bin.l30
    - up.r -> bin.l
    - clu.r -> bin.l70
    - bin.r30 -> bucket.l
    - bin.r -> log.l
    - bin.r70 -> pipe.l
facts:
  - { value: "2980", label: the public port, note: "`--listen`; the image binds 0.0.0.0", tone: accent }
  - { value: "30", unit: s, label: bucket flush, note: "`--qlog-flush-ms`; the log, the records and the hosts at one seq", tone: amber }
  - { value: "72", unit: h, label: of log for cursor replay, note: "`--qlog-retain-hours`", tone: blue }
  - { value: "flag", label: wins over its env var, note: "pass secrets as env vars; --help never prints them", tone: violet }
```

Every flag of `vlrelay`, generated from `vlrelay --help` by `just config-doc` (`build/config_doc.py`).
Flags with an env var can be set either way, and the flag wins. Secrets (`--s3-secret-key`,
`--admin-token`, `--qlog-admin-token`) are best passed as env vars, and `--help` never prints their
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
| `--s3-region <S3_REGION>` | `VLRELAY_S3_REGION` |  | The bucket's region |
| `--s3-unsigned-payload <S3_UNSIGNED_PAYLOAD>` | `VLRELAY_S3_UNSIGNED_PAYLOAD` |  | Send PUT bodies as SigV4 UNSIGNED-PAYLOAD instead of hashing each one (default: on for an https endpoint, where TLS covers the body) [possible values: true, false] |
| `--prefix <PREFIX>` | `VLRELAY_PREFIX` | `vlrelay` | Key prefix in the bucket: one relay per prefix |

## Upstreams and identity

`--host` and `--crawl` work on any member: a host admitted anywhere goes into the leader's host table, and the leader gives it to a member.

| Flag | Env | Default | What |
|---|---|---|---|
| `--host <HOSTS>` |  |  | An upstream to subscribe to (repeatable). `http://` means plain `ws://` (dev mode); a bare hostname means `wss://` |
| `--crawl` |  |  | Accept com.atproto.sync.requestCrawl |
| `--host-tier <HOST_TIER>` |  | `trusted` | The tier a --host upstream starts at the first time it's seen. After that its record's tier holds (operators, auto-throttle) |
| `--plc-url <PLC_URL>` | `VLRELAY_PLC_URL` |  | The PLC directory `did:plc` documents are resolved against |
| `--plc-export` | `VLRELAY_PLC_EXPORT` |  | Seed DID documents in bulk from the PLC directory's /export: the quorum log's leader reads the history, then follows the tail, into a database in the bucket every member reads, so a cold relay doesn't resolve each account |
| `--plc-export-url <PLC_EXPORT_URL>` | `VLRELAY_PLC_EXPORT_URL` |  | The directory --plc-export reads (default: --plc-url) |
| `--plc-export-rate <PLC_EXPORT_RATE>` |  | `2` | /export requests per second, all streams together (a 429 waits out its Retry-After on top) |
| `--plc-export-streams <PLC_EXPORT_STREAMS>` |  | `4` | Time windows of the export read side by side on a fresh start |
| `--dev-mode` |  |  | Allows plain ws://, IPs, localhost and ports for upstreams and DID documents. Implied by an http:// --host or a loopback --plc-url |
| `--did-lookups-per-sec <DID_LOOKUPS_PER_SEC>` |  | `50` | DID document fetches per second, all DIDs together |

## Pipeline and serving

| Flag | Env | Default | What |
|---|---|---|---|
| `--lanes <LANES>` |  | `64` | Pipeline lanes; a DID always maps to the same one |
| `--ingest-threads <INGEST_THREADS>` |  |  | Threads verifying events (default: the core count, at most 16) |
| `--host-inflight-events <HOST_INFLIGHT_EVENTS>` |  | `8192` | Upstream frames one host may have read and not yet durable; past it (or its MB cap) the host's socket isn't read |
| `--host-inflight-mb <HOST_INFLIGHT_MB>` |  | `64` | The same cap in bytes |
| `--inflight-events <INFLIGHT_EVENTS>` |  | `32768` | The same over every host together |
| `--inflight-mb <INFLIGHT_MB>` |  | `384` | The same cap over every host, in bytes |
| `--ring-mb <RING_MB>` |  |  | The firehose's in-memory ring of recent events, in MiB (default 512); older cursors read the node's log, then the bucket |
| `--max-lag-mb <MAX_LAG_MB>` |  |  | Dev mode only: how far a live consumer may fall behind before `ConsumerTooSlow`, in MiB (default 128) |
| `--log-compression <LOG_COMPRESSION>` |  | `-1` | zstd level for log segments: 0 stores them uncompressed, negative levels are zstd's fast ones. Firehose frames are mostly hashes: on production frames -1 compresses 1.8x faster than 1 for 0.6% more bytes (docs/perf.md, "Compression") |

## Quorum log

Every member uses the same bucket and `--prefix`. A node with no `--qlog-peer` is a single node with its commitlog as the WAL ([Cluster](../cluster.md)).

| Flag | Env | Default | What |
|---|---|---|---|
| `--node-id <NODE_ID>` | `VLRELAY_NODE_ID` | `relay` | The member's name in the quorum log |
| `--qlog-listen <QLOG_LISTEN>` | `VLRELAY_QLOG_LISTEN` | `127.0.0.1:2978` | The peer protocol: replication, submits, members' questions |
| `--qlog-peer <QLOG_PEERS>` | `VLRELAY_QLOG_PEERS` |  | Another node: `id=host:port` of its --qlog-listen (repeatable, or comma-separated) |
| `--qlog-members <QLOG_MEMBERS>` | `VLRELAY_QLOG_MEMBERS` |  | The bootstrap member set (default: this node and its peers); after the first start, `qlog/leader` holds it |
| `--qlog-dir <QLOG_DIR>` | `VLRELAY_QLOG_DIR` |  | The commitlog's directory (NVMe). Without one the log is memory only |
| `--qlog-flush-ms <QLOG_FLUSH_MS>` |  | `30000` | The bucket flush interval |
| `--qlog-headroom <QLOG_HEADROOM>` |  | `8640000` | Seqs reserved past each flush (R = F + H) |
| `--qlog-admin-token <QLOG_ADMIN_TOKEN>` | `QLOG_ADMIN_TOKEN` |  | Bearer token membership changes need (`qlog member`, the dashboard) |
| `--qlog-retain-hours <QLOG_RETAIN_HOURS>` |  | `72` | Bucket retention, run by the leader: segments older than this go (0: never) |
| `--qlog-retain-secs <QLOG_RETAIN_SECS>` |  |  | Dev: retention in seconds instead |
| `--qlog-retain-every-secs <QLOG_RETAIN_EVERY_SECS>` |  | `600` | How often the leader runs a retention pass |
| `--qlog-host-failover-ms <QLOG_HOST_FAILOVER_MS>` |  | `2000` | A member silent this long loses its hosts to the others |
| `--qlog-host-poll-ms <QLOG_HOST_POLL_MS>` |  | `500` | How often a member reads the host table and the hosts' cursors from the leader |
| `--qlog-election-ms <QLOG_ELECTION_MS>` |  | `1000` | Silence from the leader that starts an election |
| `--qlog-heartbeat-ms <QLOG_HEARTBEAT_MS>` |  | `100` | How often the leader heartbeats its followers |
| `--qlog-state-compactor-poll-ms <QLOG_STATE_COMPACTOR_POLL_MS>` |  | `30000` | The state's SlateDB compactor and worker poll |
| `--qlog-no-auto-recover` |  |  | A lost quorum waits for an operator instead of recovering from the bucket |
| `--qlog-segment-mb <QLOG_SEGMENT_MB>` |  | `64` | Commitlog file size on the local disk |
| `--qlog-disk-retain-mb <QLOG_DISK_RETAIN_MB>` |  | `4096` | Flushed commitlog kept on the local disk, for followers catching up |
| `--qlog-memory-mb <QLOG_MEMORY_MB>` |  |  | Committed log kept in memory (default 64 with --qlog-dir, else 512) |

## Chaos

For the chaos harness (`tests/qlog/relay-chaos.sh`); never on a production node.

| Flag | Env | Default | What |
|---|---|---|---|
| `--qlog-crash-at <QLOG_CRASH_AT>` |  |  | Chaos: kill -9 at this flush step (or `any`), with --qlog-crash-prob |
| `--qlog-crash-prob <QLOG_CRASH_PROB>` |  | `0.05` | Chaos: the chance of the crash at each step |
| `--qlog-crash-stop-file <QLOG_CRASH_STOP_FILE>` |  |  | Chaos: no crash injected once this file exists |
| `--qlog-power-cut-on-usr1` |  |  | Chaos: SIGUSR1 is a power cut |
| `--qlog-fsync-delay-us <QLOG_FSYNC_DELAY_US>` |  |  | Chaos: sleep this long before each commitlog fsync (emulates a disk) |
