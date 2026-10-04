# Configuration

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
| `--linger-ms <LINGER_MS>` |  | `25` | Segment linger (PLAN.md decision 1) |
| `--log-inflight <LOG_INFLIGHT>` |  | `32` | Segment PUTs in flight at once |
| `--max-segment-mb <MAX_SEGMENT_MB>` |  | `8` | A segment seals at this size even before its linger is up |
| `--log-compression <LOG_COMPRESSION>` |  | `-1` | zstd level for log segments: 0 stores them uncompressed, negative levels are zstd's fast ones. Firehose frames are mostly hashes: on production frames -1 compresses 1.8x faster than 1 for 0.6% more bytes (docs/perf.md, iteration 5) |
| `--retention <RETENTION>` |  | `72` | How long the log keeps events for cursor replay, in hours |

## Pipeline and state

| Flag | Env | Default | What |
|---|---|---|---|
| `--did-shards <DID_SHARDS>` |  | `4` | DID state shards (SlateDB instances) |
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
