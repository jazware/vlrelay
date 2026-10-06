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
| `--listen <LISTEN>` |  |  | Serves subscribeRepos, the sync API, requestCrawl, /admin and /metrics [env: VLRELAY_LISTEN=] [default: 127.0.0.1:2980] |
| `--trusted-proxy <TRUSTED_PROXIES>` |  |  | Proxies whose `X-Forwarded-For` names the client (CIDRs, repeatable or comma-separated): per-IP limits key on its rightmost address that isn't one of these. Other peers' headers are ignored [env: VLRELAY_TRUSTED_PROXIES=] |
| `--admin-token <ADMIN_TOKEN>` |  |  | Turns on /admin (dashboard and API) with this token [env: VLRELAY_ADMIN_TOKEN] |
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
| `--s3-unsigned-payload <S3_UNSIGNED_PAYLOAD>` |  |  | Send PUT bodies as SigV4 UNSIGNED-PAYLOAD instead of hashing each one (default: on for an https endpoint, where TLS covers the body) [env: VLRELAY_S3_UNSIGNED_PAYLOAD=] [possible values: true, false] |
| `--prefix <PREFIX>` |  |  | Key prefix in the bucket: one relay per prefix [env: VLRELAY_PREFIX=] [default: vlrelay] |

## Upstreams and identity

`--host` and `--crawl` work on any member: a host admitted anywhere goes into the leader's host table, and the leader gives it to a member.

| Flag | Env | Default | What |
|---|---|---|---|
| `--host <HOSTS>` |  |  | An upstream to subscribe to (repeatable). `http://` means plain `ws://` (dev mode); a bare hostname means `wss://` |
| `--crawl` |  |  | Accept com.atproto.sync.requestCrawl |
| `--host-tier <HOST_TIER>` |  |  | The tier a --host upstream starts at the first time it's seen. After that its record's tier holds (operators, auto-throttle) [default: trusted] |
| `--plc-url <PLC_URL>` | `VLRELAY_PLC_URL` |  | The PLC directory `did:plc` documents are resolved against |
| `--dev-mode` |  |  | Allows plain ws://, IPs, localhost and ports for upstreams and DID documents. Implied by an http:// --host or a loopback --plc-url |
| `--did-lookups-per-sec <DID_LOOKUPS_PER_SEC>` |  |  | DID document fetches per second, all DIDs together [default: 50] |

## Pipeline and serving

| Flag | Env | Default | What |
|---|---|---|---|
| `--lanes <LANES>` |  |  | Pipeline lanes; a DID always maps to the same one [default: 64] |
| `--ingest-threads <INGEST_THREADS>` |  |  | Threads verifying events (default: the core count, at most 16) |
| `--host-inflight-events <HOST_INFLIGHT_EVENTS>` |  |  | Upstream frames one host may have read and not yet durable; past it (or its MB cap) the host's socket isn't read [default: 8192] |
| `--host-inflight-mb <HOST_INFLIGHT_MB>` |  |  | The same cap in bytes [default: 64] |
| `--inflight-events <INFLIGHT_EVENTS>` |  |  | The same over every host together [default: 32768] |
| `--inflight-mb <INFLIGHT_MB>` |  |  | The same cap over every host, in bytes [default: 384] |
| `--ring-mb <RING_MB>` |  |  | The firehose's in-memory ring of recent events, in MiB (default 512); older cursors read the node's log, then the bucket |
| `--max-lag-mb <MAX_LAG_MB>` |  |  | Dev mode only: how far a live consumer may fall behind before `ConsumerTooSlow`, in MiB (default 128) |
| `--log-compression <LOG_COMPRESSION>` |  |  | zstd level for log segments: 0 stores them uncompressed, negative levels are zstd's fast ones. Firehose frames are mostly hashes: on production frames -1 compresses 1.8x faster than 1 for 0.6% more bytes (docs/perf.md, "Compression") [default: -1] |

## Quorum log

Every member uses the same bucket and `--prefix`. A node with no `--qlog-peer` is a single node with its commitlog as the WAL ([Cluster](../cluster.md)).

| Flag | Env | Default | What |
|---|---|---|---|
| `--node-id <NODE_ID>` |  |  | The member's name in the quorum log [env: VLRELAY_NODE_ID=] [default: relay] |
| `--qlog-listen <QLOG_LISTEN>` |  |  | The peer protocol: replication, submits, members' questions [env: VLRELAY_QLOG_LISTEN=] [default: 127.0.0.1:2978] |
| `--qlog-peer <QLOG_PEERS>` |  |  | Another node: `id=host:port` of its --qlog-listen (repeatable, or comma-separated) [env: VLRELAY_QLOG_PEERS=] |
| `--qlog-members <QLOG_MEMBERS>` |  |  | The bootstrap member set (default: this node and its peers); after the first start, `qlog/leader` holds it [env: VLRELAY_QLOG_MEMBERS=] |
| `--qlog-dir <QLOG_DIR>` |  |  | The commitlog's directory (NVMe). Without one the log is memory only [env: VLRELAY_QLOG_DIR=] |
| `--qlog-flush-ms <QLOG_FLUSH_MS>` |  |  | The bucket flush interval [default: 30000] |
| `--qlog-headroom <QLOG_HEADROOM>` |  |  | Seqs reserved past each flush (R = F + H) [default: 8640000] |
| `--qlog-admin-token <QLOG_ADMIN_TOKEN>` |  |  | Bearer token membership changes need (`qlog member`, the dashboard) [env: QLOG_ADMIN_TOKEN] |
| `--qlog-retain-hours <QLOG_RETAIN_HOURS>` |  |  | Bucket retention, run by the leader: segments older than this go (0: never) [default: 72] |
| `--qlog-retain-secs <QLOG_RETAIN_SECS>` |  |  | Dev: retention in seconds instead |
| `--qlog-retain-every-secs <QLOG_RETAIN_EVERY_SECS>` |  |  | How often the leader runs a retention pass [default: 600] |
| `--qlog-host-failover-ms <QLOG_HOST_FAILOVER_MS>` |  |  | A member silent this long loses its hosts to the others [default: 2000] |
| `--qlog-host-poll-ms <QLOG_HOST_POLL_MS>` |  |  | How often a member reads the host table and the hosts' cursors from the leader [default: 500] |
| `--qlog-election-ms <QLOG_ELECTION_MS>` |  |  | Silence from the leader that starts an election [default: 1000] |
| `--qlog-heartbeat-ms <QLOG_HEARTBEAT_MS>` |  |  | How often the leader heartbeats its followers [default: 100] |
| `--qlog-state-compactor-poll-ms <QLOG_STATE_COMPACTOR_POLL_MS>` |  |  | The state's SlateDB compactor and worker poll [default: 30000] |
| `--qlog-no-auto-recover` |  |  | A lost quorum waits for an operator instead of recovering from the bucket |
| `--qlog-segment-mb <QLOG_SEGMENT_MB>` |  |  | Commitlog file size on the local disk [default: 64] |
| `--qlog-disk-retain-mb <QLOG_DISK_RETAIN_MB>` |  |  | Flushed commitlog kept on the local disk, for followers catching up [default: 4096] |
| `--qlog-memory-mb <QLOG_MEMORY_MB>` |  |  | Committed log kept in memory (default 64 with --qlog-dir, else 512) |

## Chaos

For the chaos harness (`tests/qlog/relay-chaos.sh`); never on a production node.

| Flag | Env | Default | What |
|---|---|---|---|
| `--qlog-crash-at <QLOG_CRASH_AT>` |  |  | Chaos: kill -9 at this flush step (or `any`), with --qlog-crash-prob |
| `--qlog-crash-prob <QLOG_CRASH_PROB>` |  |  | Chaos: the chance of the crash at each step [default: 0.05] |
| `--qlog-crash-stop-file <QLOG_CRASH_STOP_FILE>` |  |  | Chaos: no crash injected once this file exists |
| `--qlog-power-cut-on-usr1` |  |  | Chaos: SIGUSR1 is a power cut |
| `--qlog-fsync-delay-us <QLOG_FSYNC_DELAY_US>` |  |  | Chaos: sleep this long before each commitlog fsync (emulates a disk) |
