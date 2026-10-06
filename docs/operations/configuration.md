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

## Cluster

Without `--cluster` or `--role` the node runs alone. Core and edge nodes need `--peer-tls-dir` and `--internal-token`, and a replica needs neither ([Deploy](deploy.md#a-cluster), [Cluster](../cluster.md)).

| Flag | Env | Default | What |
|---|---|---|---|
| `--node-id <NODE_ID>` | `VLRELAY_NODE_ID` | `relay` | Node id: the node log's id prefix, and the cluster member name |
| `--cluster` |  |  | Run as a core cluster node (the same as --role core) |
| `--role <ROLE>` |  |  | Cluster role: core (lease, shards, a log), edge (follows every log over peer mTLS) or replica (follows every log from the bucket) `core`: Lease, DID and host shards, a log; serves · `edge`: Follows every log over peer mTLS and serves. No lease, no shards · `replica`: Follows every log from the bucket (read-only) and serves |
| `--peer-listen <PEER_LISTEN>` | `VLRELAY_PEER_LISTEN` | `127.0.0.1:2979` | The peer listener (node-to-node mTLS): forwarding, log streams |
| `--advertise-url <ADVERTISE_URL>` | `VLRELAY_ADVERTISE_URL` |  | `https://host:port` peers reach --peer-listen at |
| `--peer-tls-dir <PEER_TLS_DIR>` | `VLRELAY_PEER_TLS_DIR` |  | Peer TLS: `ca.crt`, `{node-id}.crt`, `{node-id}.key` (vlpds admin tls ca / issue). With --dev-mode they're created as needed |
| `--internal-token <INTERNAL_TOKEN>` | `VLRELAY_INTERNAL_TOKEN` |  | Shared secret on every peer request |
| `--lease-ttl-ms <LEASE_TTL_MS>` |  | `10000` | Node lease TTL: a crashed core node's shards move after about this plus a fifth of it |
| `--host-shards <HOST_SHARDS>` |  | `64` | Host shards (used only when the bucket has no host layout yet) |
