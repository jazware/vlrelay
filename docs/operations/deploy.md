---
title: Deploy
section: Operations
order: 101
summary: "The image, the bucket, one node on Docker Compose, a three-node quorum cluster, the proxy and tokens in front, and rolling upgrades."
---

```hero
diagram:
  caption: A production cluster. Three nodes on separate machines share one bucket and prefix and replicate the log to each other over the peer port on the private network. A TLS proxy in front gives consumers wss:// and PDSes https://, and any node serves any consumer.
  nodes:
    - { id: pds, label: PDSes, sub: requestCrawl · streams, at: [0, 0], size: [8, 3], tone: muted, stack: true }
    - { id: cons, label: Consumers, sub: subscribeRepos, at: [0, 8], size: [8, 3], tone: blue, stack: true }
    - { id: proxy, label: TLS proxy, sub: "Caddy · nginx · an LB", at: [11, 4], size: [8, 3], tone: muted }
    - { id: c1, label: node n1, sub: ":2980 · peer :2978", at: [23, 0], size: [9, 2.6], tone: accent }
    - { id: c2, label: node n2, sub: ":2980 · peer :2978", at: [23, 4.2], size: [9, 2.6], tone: accent }
    - { id: c3, label: node n3, sub: ":2980 · peer :2978", at: [23, 8.4], size: [9, 2.6], tone: accent }
    - { id: bucket, label: Bucket, sub: "one `--prefix`", at: [37, 4.2], size: [8, 2.6], shape: store, tone: amber }
  groups:
    - { label: private network · quorum log, around: [c1, c2, c3], tone: accent }
  edges:
    - "pds.r -> proxy.l30"
    - "cons.r -> proxy.l70"
    - proxy.r -> c1.l
    - "proxy.r -> c2.l: any node"
    - proxy.r -> c3.l
    - c1.r -> bucket.l
    - c2.r -> bucket.l
    - c3.r -> bucket.l
facts:
  - { value: "1", unit: NVMe disk, label: per node, note: "`--qlog-dir`, the commitlog; the bucket holds the rest", tone: amber }
  - { value: "2980", label: public port, note: "firehose, sync API, /admin, /docs, /metrics; peers on 2978", tone: accent }
  - { value: "2 of 3", label: commit, note: "so one node at a time can be down or restarting", tone: violet }
  - { value: "~390", unit: GB, label: raw log at 72 h, note: "at ~330 events/s, before zstd", tone: amber }
```

A vlRelay node is one process, a local disk for its commitlog and one bucket prefix. Every node is
a member of a quorum log. One node alone is a one-member log, and three nodes commit an event once
two of them hold it. Every flag is in [Configuration](configuration.md), and
[Cluster](../cluster.md) has the mechanism.

The usual order is a bucket and its credentials, then one node on Docker Compose to look around,
then a cluster on three machines with a proxy in front.

## The image

Images are published to `ghcr.io/jazware/vlrelay`. `main` follows the main branch, `sha-<7>`
pins one commit, and releases get semver tags. To build one yourself, run this from the root of a
clone of [the repository](https://github.com/jazware/vlrelay):

```bash
docker build -t vlrelay:local .
docker build -t vlrelay:dev --build-arg VLRELAY_TOOLS=1 .   # with fakepds and e2e_check too
```

The crate pulls vlpds as a git dependency, so the repository root is the whole build context. The
image has three stages. A node stage builds the dashboard and the docs into
`/usr/share/vlrelay/ui`. A Rust stage builds the release binary (fat LTO and no
`target-cpu=native`). The runtime is `debian:bookworm-slim` running as uid 10001 under `tini`. The
UI is the last layer, so a UI-only or docs-only change rebuilds in seconds.

The entrypoint is `vlrelay --ui-dir /usr/share/vlrelay/ui`, so the container takes the relay's
flags as its command. `VLRELAY_LISTEN` defaults to `0.0.0.0:2980` in the image, and the health
check is `GET /xrpc/_health`.

```bash
docker run --rm ghcr.io/jazware/vlrelay:main --help
docker run --rm -p 2980:2980 ghcr.io/jazware/vlrelay:main --memory --dev-mode     # nothing kept, for a look around
docker run --rm -p 2980:2980 ghcr.io/jazware/vlrelay:main \
  --memory --host morel.us-east.host.bsky.network --admin-token dev  # one of Bluesky's PDSes
```

## The bucket

vlRelay uses vlpds's store, so a bucket that works for vlpds works here. It needs strongly
consistent conditional writes (`If-None-Match: *` and `If-Match`), which the flush manifest, the
leader record and the policy use. S3, R2, GCS, Tigris and MinIO all qualify. vlpds's
[object store page](https://github.com/jazware/vlpds/blob/main/docs/operations/object-store.md)
covers choosing one, and its `vlpds-bucket-probe` checks a bucket before you trust it.

To set one up:

1. Create a bucket. It needs no lifecycle rules, since the leader deletes old segments itself
   (below).
2. Create an access key that can read, write, list and delete objects in that bucket only.
3. Pick a `--prefix` (default `vlrelay`). One relay lives under one prefix. Every node of a
   cluster uses the same bucket and prefix, and two relays can share a bucket with different
   prefixes.
4. Give the node the endpoint, the bucket and the key:

| Provider | `--s3-endpoint` | `--s3-region` |
|---|---|---|
| AWS S3 | `https://s3.<region>.amazonaws.com` | the bucket's region |
| Cloudflare R2 | `https://<account-id>.r2.cloudflarestorage.com` | `auto` (the default) |
| MinIO | `http://<host>:9000` | `auto` |

The same values go in `VLRELAY_S3_ENDPOINT`, `VLRELAY_S3_BUCKET`, `VLRELAY_S3_ACCESS_KEY` and
`VLRELAY_S3_SECRET_KEY`. The keys can come from files instead (`--s3-access-key-file`,
`--s3-secret-key-file`), which keeps them out of the container's env.

The log's part of the prefix is written by the leader:

| Path | What | Written |
|---|---|---|
| `qlog/leader` | the epoch, the leader and the members | at a takeover or a membership change |
| `qlog/manifest` | the last seq flushed, the segments, the records' checkpoint, the cursors | every flush (`--qlog-flush-ms`, 30 s), last |
| `qlog/state*` | one SlateDB: each account's record, the host table and the PDS cursors | every flush |
| `log/qlog/` | 64 MiB segments of the log, for old cursors | every flush |
| `retain/qlog` | what retention deleted | every retention pass |

The policy, domain rules, takedowns (`policy/`) and cases (`cases/`) sit next to them, written by
whichever node an operator or the policy engine changes them on. Host discovery keeps its progress
in `discovery/state.json`, and `--plc-export` adds `plc/seeds` and `plc/export-checkpoint.json`
([Policy](../policy.md#discovering-hosts)).

The leader deletes segments older than `--qlog-retain-hours` (72) every
`--qlog-retain-every-secs` (600). At Bluesky's average of ~330 events/s, 72 hours is ~390 GB of
raw events, and less after zstd (`--log-compression`). At today's rate the bucket sees about 0.5
writes and 1.7 reads a second. [Cost](../cost.md) has the bill per provider.

## One node

A node with no `--qlog-peer` is a one-member quorum log. Its commitlog on `--qlog-dir` is the
write-ahead log, and it flushes to the bucket on the same schedule as a cluster.

```bash
vlrelay --node-id n1 --qlog-dir /var/lib/vlrelay/qlog \
        --s3-endpoint ... --prefix relay1 --host pds.example.com --crawl
```

Without `--qlog-dir` the log is in memory only. A restart then resumes from the bucket's last
flush, with a jump in the seqs, and the PDSes send the rest again.

`deploy/single/docker-compose.yml` runs a node on a local MinIO. It builds MinIO from source (MinIO
no longer publishes images) and the relay from the repository root:

```bash
cd deploy/single
UPSTREAM=morel.us-east.host.bsky.network VLRELAY_ADMIN_TOKEN=$(openssl rand -hex 16) \
  docker compose up -d --build
```

To run the published image instead, set `VLRELAY_IMAGE=ghcr.io/jazware/vlrelay:main`, run
`docker compose pull vlrelay`, and leave out `--build`.

- `UPSTREAM` is a PDS to subscribe to (`--host`). A bare hostname means `wss://`. Add more with
  more `--host` flags, or let PDSes ask with `requestCrawl` (`--crawl`, which the example turns
  on). `--host` upstreams start in the `trusted` tier (`--host-tier`), and crawled ones go through
  the policy's admission rules ([Policy](../policy.md#admission)).
- The firehose is `ws://127.0.0.1:2980/xrpc/com.atproto.sync.subscribeRepos`, the dashboard is
  `http://127.0.0.1:2980/admin` (user `admin`, password the token) and these docs are at
  `http://127.0.0.1:2980/docs`. The MinIO console is on `http://127.0.0.1:9001`.
- The example passes no `--qlog-dir`, so its log is in memory. `docker compose down` and `up`
  again resumes from the last flush in MinIO, with a seq jump. `down -v` deletes the MinIO volume
  and the relay with it.

For a real deployment, swap MinIO for your bucket (`VLRELAY_S3_*`), drop the `minio` services,
and mount a local disk for `--qlog-dir` (`VLRELAY_QLOG_DIR`), writable by uid 10001.

A 2 vCPU / 4 GB box carries a single node at a few thousand hosts, but not the PLC export's
first fill at its default rate on top. [Configuration](configuration.md#a-small-box) has the
settings for one.

## A cluster

Three nodes on separate machines, each with a local NVMe disk, the same bucket and the same
`--prefix`. Give each node its own id, its peer port on the private network, and the other two as
peers:

```bash
vlrelay --node-id n1 --listen 0.0.0.0:2980 \
        --qlog-listen 10.0.0.1:2978 --qlog-peer n2=10.0.0.2:2978 --qlog-peer n3=10.0.0.3:2978 \
        --qlog-dir /var/lib/vlrelay/qlog --qlog-admin-token "$QLOG_TOKEN" \
        --s3-endpoint ... --prefix relay1 --host pds.example.com --crawl
```

| What | Flag / env | Notes |
|---|---|---|
| Node id | `--node-id` / `VLRELAY_NODE_ID` | Unique per node. It's the member's name in `qlog/leader` |
| Peer port | `--qlog-listen` / `VLRELAY_QLOG_LISTEN` | Replication and submits. On the private network only |
| Peers | `--qlog-peer id=host:port` / `VLRELAY_QLOG_PEERS` | Each other node's `--qlog-listen`, repeatable or comma-separated |
| Commitlog | `--qlog-dir` / `VLRELAY_QLOG_DIR` | A local NVMe disk. With three members an entry counts once it's written there and fdatasync runs every 100 ms (`--durability page-cache`). A single node waits for the fsync |
| Membership token | `--qlog-admin-token` / `QLOG_ADMIN_TOKEN` | The same on every node. Without it membership changes are refused |
| Admin token | `--admin-token` / `VLRELAY_ADMIN_TOKEN` | The dashboard's password, the same on every node |

The peer port has no TLS or auth of its own, so keep it on a private network or a tunnel
(WireGuard, Tailscale). The first start writes the member set to `qlog/leader`
(`--qlog-members`, default this node and its peers). After that the bucket's record wins, and the
set changes only through a membership change.

In Docker, mount the commitlog and publish the peer port on the private address only:

```bash
docker run -d --name vlrelay --restart unless-stopped --stop-timeout 30 \
  -p 2980:2980 -p 10.0.0.1:2978:2978 \
  -v /var/lib/vlrelay/qlog:/var/lib/vlrelay/qlog \
  -e VLRELAY_S3_ENDPOINT -e VLRELAY_S3_BUCKET -e VLRELAY_S3_ACCESS_KEY -e VLRELAY_S3_SECRET_KEY \
  -e VLRELAY_ADMIN_TOKEN -e QLOG_ADMIN_TOKEN \
  ghcr.io/jazware/vlrelay:main --node-id n1 --qlog-listen 0.0.0.0:2978 \
  --qlog-peer n2=10.0.0.2:2978 --qlog-peer n3=10.0.0.3:2978 \
  --qlog-dir /var/lib/vlrelay/qlog --prefix relay1 --host pds.example.com --crawl
```

The host directory must be writable by uid 10001 (`chown 10001:10001 /var/lib/vlrelay/qlog`).
Pin a `sha-<7>` or release tag in production so every node runs the same build.

Every node reads the PDSes the leader's host table gives it, and every node serves the whole
stream with the same seqs. So a consumer can connect to any node (or a load balancer over all
three) and resume on another with its cursor. `--host` and `--crawl` work on any node, since a PDS
admitted anywhere goes into the leader's host table.

### Changing the members

Replacing a machine, or growing from one node to three, is a membership change. Start the new
node with the others as `--qlog-peer`s. It joins as a learner, copies the leader's log, and the
switch happens at a flush. Send the change from the dashboard's Quorum page, or from the admin API
on any node:

```bash
curl -u admin:$VLRELAY_ADMIN_TOKEN -H 'content-type: application/json' \
  -d '{"members": ["n1", "n2", "n4"], "addrs": {"n4": "10.0.0.4:2978"}}' \
  https://relay.example.com/admin/api/cluster/quorum/members
```

`members` is the whole set wanted, and `addrs` names a new node that the others' `--qlog-peer`
flags don't. The `qlog` tool (`cargo run --release --bin qlog -- member`) does the same with
`add ID`, `remove ID`, `replace OLD NEW` or `set ID,ID,...`. It finds the leader through each
node's `/qlog/status` and retries across a leader change. Details:
[Cluster](../cluster.md#changing-the-members).

## In front of it

vlRelay serves plain HTTP. Put a TLS proxy (Caddy, nginx, a cloud load balancer) in front of
`--listen` so consumers get `wss://` and PDSes can reach `requestCrawl` over `https://`. A
minimal Caddyfile for a node on the same machine:

```caddyfile
relay.example.com {
	@private path /metrics /qlog/*
	respond @private 404
	reverse_proxy 127.0.0.1:2980 {
		flush_interval -1
	}
}
```

Run that node with `--listen 127.0.0.1:2980 --trusted-proxy 127.0.0.1/32`. The rules for any
proxy:

- `/metrics` and `/qlog/status` have no auth. Keep them off the public side of the proxy and
  scrape them on the private address.
- `POST /qlog/members` is on `--listen` too, behind the bearer `--qlog-admin-token`. Don't proxy
  `/qlog/` at all.
- `/admin` is behind the admin token (HTTP basic, user `admin`). It's fine to expose, but there's
  no reason to. `/docs` is public and static, and it's served even without `--admin-token`.
  Operators can sign in through a proxy instead, on a separate `--admin-listen` that's never
  public ([Admin API](../admin-api.md#sign-in-through-a-proxy)).
- Proxy the websocket without buffering, and with an idle timeout of a minute or more.
- The peer port (`--qlog-listen`, 2978) belongs on the private network only.
- Name the proxy with `--trusted-proxy <CIDR,...>` (or `VLRELAY_TRUSTED_PROXIES`), and have it
  append the client's address to `X-Forwarded-For`. Per-IP limits (consumers per IP,
  `requestCrawl` calls per minute) key on that header's rightmost address that isn't a trusted
  proxy, and only for requests whose peer is one. Without the flag every consumer behind the
  proxy counts as the proxy's address, and they share one per-IP cap. Don't list addresses that
  clients can connect from directly, since their `X-Forwarded-For` would be believed.

## Tokens

A relay has two secrets besides the bucket keys. Generate each with `openssl rand -hex 32` and
use the same value on every node.

| Token | Flag / env | Gates |
|---|---|---|
| Admin token | `--admin-token` / `VLRELAY_ADMIN_TOKEN` | `/admin` and its API, as HTTP basic with user `admin`. Without it `/admin` is off (404), and `/`, its stats and `/docs` are still served |
| Membership token | `--qlog-admin-token` / `QLOG_ADMIN_TOKEN` | `POST /qlog/members`, membership changes from the dashboard, and kicking a consumer on another member |

Each has a `-file` twin (`--admin-token-file`, `--qlog-admin-token-file`) that reads it from a
file once at start, less one trailing newline. A file keeps it out of `docker inspect`.

## Shutting down and upgrading

Send SIGTERM (`docker stop` does, and `tini` forwards it). The node closes its PDS sockets and
exits. Its consumers' sockets close with it, and they resume on another node with their cursor.
Its PDSes move to the other nodes after `--qlog-host-failover-ms` (2 s) and resume from their
cursors. If it was the leader, another node takes over, in 50-120 ms when the process is gone and
after `--qlog-election-ms` (1 s) of silence when it hangs.

A rolling upgrade is one node at a time, since two of three must stay up to commit:

1. Stop one node and start the new image on the same `--qlog-dir`.
2. Wait for `/xrpc/_health`, then for the node to show as a member on the dashboard's Quorum page
   (or in its `/qlog/status`) with its `commit` caught up to the leader's.
3. Move on to the next. Doing the leader last costs one takeover instead of two.

A one-node relay is down for the length of its restart. vlRelay doesn't have vlpds's feature
levels yet, so there's no guard against a new version writing something an old one can't read.

Prometheus series are on `/metrics` of every node, and [Monitoring](monitoring.md) lists them.
