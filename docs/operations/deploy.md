---
title: Deploy
section: Operations
order: 101
summary: "Building the image, one node on Docker Compose, a three-core cluster with peer mTLS, edges and replicas, and the proxy in front."
---

```hero
diagram:
  caption: A production cluster. Three cores on separate machines share one bucket and prefix and talk over mTLS on the private network. A TLS proxy in front gives consumers wss:// and PDSes https://. Edges and replicas take consumers off the cores.
  nodes:
    - { id: pds, label: PDSes, sub: requestCrawl · streams, at: [0, 0], size: [8, 3], tone: muted, stack: true }
    - { id: cons, label: Consumers, sub: subscribeRepos, at: [0, 8], size: [8, 3], tone: blue, stack: true }
    - { id: proxy, label: TLS proxy, sub: "Caddy · nginx · an LB", at: [11, 4], size: [8, 3], tone: muted }
    - { id: c1, label: core n1, sub: ":2980 · peer :2979", at: [23, 0], size: [9, 2.6], tone: accent }
    - { id: c2, label: core n2, sub: ":2980 · peer :2979", at: [23, 4.2], size: [9, 2.6], tone: accent }
    - { id: c3, label: core n3, sub: ":2980 · peer :2979", at: [23, 8.4], size: [9, 2.6], tone: accent }
    - { id: edge, label: Edges · replicas, sub: more egress, at: [11, 12.5], size: [8, 3], tone: blue }
    - { id: bucket, label: Bucket, sub: "one `--prefix`", at: [37, 4.2], size: [8, 2.6], shape: store, tone: amber }
  groups:
    - { label: private network · peer mTLS, around: [c1, c2, c3], tone: accent }
  edges:
    - "pds.r -> proxy.l30"
    - "cons.r -> proxy.l70"
    - proxy.r -> c1.l
    - "proxy.r -> c2.l: any core"
    - proxy.r -> c3.l
    - c1.r -> bucket.l
    - c2.r -> bucket.l
    - c3.r -> bucket.l
    - { from: edge.t, to: proxy.b, label: behind the same name, dash: true }
facts:
  - { value: "0", unit: local disk, label: needed by any node, note: "a node is the binary, bucket credentials and a port" }
  - { value: "2980", label: public port, note: "firehose, sync API, /admin, /docs and /metrics; peers on 2979", tone: accent }
  - { value: "24", unit: DID shards, label: in a new cluster, note: "splits evenly over 2, 3, 4, 6 and 8 cores; read once", tone: violet }
  - { value: "~390", unit: GB, label: raw log at 72 h, note: "at ~330 events/s, before zstd's ~1.56×", tone: amber }
```

A vlRelay node is one process and one bucket prefix. It keeps nothing on local disk, so a node is
just the `vlrelay` binary (or the image), bucket credentials and a public port. Every flag is in
[Configuration](configuration.md).

## The image

The repository doesn't publish an image yet, so you build it. The crate depends on vlpds by path,
so the build context is the directory holding both crates, and `Dockerfile.dockerignore` keeps the
context to the two of them.

```bash
just docker-build                    # this machine's platform, tagged vlrelay:local
just docker-build vlrelay:dev 1      # with fakepds and e2e_check in the image too
```

The image has three stages. A node stage builds the dashboard and the docs into
`/usr/share/vlrelay/ui`. A Rust stage builds the release binary (fat LTO, no debug info, and no
`target-cpu=native`). The runtime is `debian:bookworm-slim` running as uid 10001 under `tini`. The
UI is the last layer, so a UI-only or docs-only change rebuilds in seconds.

The entrypoint is `vlrelay --ui-dir /usr/share/vlrelay/ui`, so the container takes the relay's
flags as its command. `VLRELAY_LISTEN` defaults to `0.0.0.0:2980` in the image, and the health
check is `GET /xrpc/_health`.

```bash
docker run --rm vlrelay:local --help
docker run --rm -p 2980:2980 vlrelay:local --memory --dev-mode     # nothing kept, for a look around
docker run --rm -p 2980:2980 vlrelay:local \
  --memory --host morel.us-east.host.bsky.network --admin-token dev  # one of Bluesky's PDSes
```

## The bucket

vlRelay uses vlpds's store, so a bucket that works for vlpds works here. It needs strongly
consistent conditional writes (`If-None-Match: *` for log segments and fences, `If-Match` for
leases, assignments, host records and the policy). S3, R2, GCS, Tigris and MinIO all qualify.
vlpds's [object store page](https://github.com/jazware/vlpds/blob/main/docs/operations/object-store.md)
covers choosing one, and its `vlpds-bucket-probe` checks a bucket before you trust it.

One relay lives under one `--prefix` (default `vlrelay`). Every node of a cluster uses the same
bucket and prefix, and two relays can share a bucket with different prefixes. The log keeps
`--retention` hours of events for cursor replay (72 by default). At Bluesky's average of ~330
events/s that's ~390 GB of raw events, or about 250 GB after zstd. [Cost](../cost.md) has the
request and storage bill per provider.

## One node

`deploy/single/docker-compose.yml` runs a node on a local MinIO:

```bash
cd deploy/single
UPSTREAM=morel.us-east.host.bsky.network VLRELAY_ADMIN_TOKEN=$(openssl rand -hex 16) \
  docker compose up -d --build
```

- `UPSTREAM` is a PDS to subscribe to (`--host`). A bare hostname means `wss://`. Add more with
  more `--host` flags, or let PDSes ask with `requestCrawl` (`--crawl`, which the example turns
  on). `--host` upstreams start in the `trusted` tier (`--host-tier`), and crawled ones go through
  the policy's admission rules ([Policy](../policy.md#admission)).
- The firehose is `ws://127.0.0.1:2980/xrpc/com.atproto.sync.subscribeRepos`, the dashboard is
  `http://127.0.0.1:2980/admin` (user `admin`, password the token) and these docs are at
  `http://127.0.0.1:2980/docs`.
- `docker compose down` and `up` again resumes every host from its checkpointed cursor, since all
  state is in the bucket. `down -v` deletes the MinIO volume and the relay with it.

For a real deployment, swap MinIO for your bucket (`VLRELAY_S3_*`) and drop the `minio` services.

## A cluster

A cluster is several core nodes on the same bucket and prefix. Each core holds a lease, owns some
host shards and some DID shards, and merges every core's log into the same stream. A crashed
core's shards move to the others after its lease lapses (`--lease-ttl-ms`, 10 s, plus a fifth of
it), or within about a second if its peer port refuses connections. A SIGTERM'd core hands its
shards over first. [Cluster](../cluster.md) has the mechanism and the HA measurements.

Cores and edges talk to each other over mTLS on `--peer-listen` (2979). Each needs a certificate
from a cluster CA, the shared `--internal-token` and an `--advertise-url` its peers can reach.
vlRelay uses vlpds's peer TLS as is, so `vlpds admin tls` makes the files.

`deploy/cluster/` runs three cores on one machine:

```bash
cd deploy/cluster
./certs.sh          # pki/ca.{crt,key} and pki/<node>/{ca.crt,<node>.crt,<node>.key} for n1 n2 n3 edge
export VLRELAY_INTERNAL_TOKEN=$(openssl rand -hex 32) VLRELAY_ADMIN_TOKEN=$(openssl rand -hex 16)
UPSTREAM=morel.us-east.host.bsky.network docker compose up -d --build
docker compose --profile followers up -d      # an edge (:2984) and a replica (:2985)
```

The cores serve on 127.0.0.1:2981-2983. They emit the same events with the same seqs, so a
consumer can connect to any of them (or a load balancer over all three) and resume on another
with its cursor.

On real hosts, run one node per machine and give each:

| What | Flag / env | Notes |
|---|---|---|
| Role | `--role core` (or `--cluster`) | |
| Node id | `--node-id` / `VLRELAY_NODE_ID` | Unique, and the name on its certificate |
| Peer listener | `--peer-listen` / `VLRELAY_PEER_LISTEN` | On the private network only |
| Advertise URL | `--advertise-url` / `VLRELAY_ADVERTISE_URL` | `https://<private address>:2979`, the address on its certificate |
| Peer TLS | `--peer-tls-dir` / `VLRELAY_PEER_TLS_DIR` | `ca.crt`, `<node-id>.crt`, `<node-id>.key` |
| Token | `--internal-token` / `VLRELAY_INTERNAL_TOKEN` | The same on every core and edge |

Make the certificates with a vlpds binary, and keep `ca.key` off the nodes:

```bash
vlpds admin tls ca --out ./pki
vlpds admin tls issue --ca ./pki/ca.crt --ca-key ./pki/ca.key --out ./pki \
  --node-id n1 --host 10.0.0.1          # per node, 365 days (--days)
```

Nodes reload changed certificate files without a restart. vlpds's
[scaling and clustering page](https://github.com/jazware/vlpds/blob/main/docs/operations/scaling-and-clustering.md)
covers renewal and CA rotation, and it's the same here.

`--did-shards` and `--host-shards` only matter the first time a cluster starts on an empty prefix.
After that the layout in the bucket wins. Each core takes at most its fair share,
`ceil(shards / cores)`, so a shard count that doesn't spread can leave a core with no DID state,
and then everything it reads is forwarded (4 shards over 3 cores goes 2/2/0). The cluster default
of 24 splits evenly over 2, 3, 4, 6 and 8 cores and leaves none empty at 5. For 7 cores or more
than 8, pick a multiple of the core count you expect. A shard can also be split or merged later
([Resharding](../cluster.md#resharding)).

### Edges and replicas

Both serve the merged firehose and `/xrpc/_health`, and neither takes upstreams or holds shards.
They sit ~275 ms behind the cores (the merge guard plus polling).

- An edge (`--role edge`) follows every core's log over peer mTLS. It needs a certificate, the
  token and bucket read access.
- A replica (`--role replica`) follows the logs from the bucket alone. It needs read-only bucket
  credentials and nothing else, so it can run anywhere that can read the bucket.

Add them when consumers need more egress than the cores have. A full-firehose consumer costs ~1.6
Gb/s at 33k events/s, so a NIC runs out long before the CPU ([Performance](../perf.md#fan-out)).
To include them in a core's dashboard, pass their public URLs with `--admin-follower` on each core
and give them the same `--admin-token` ([Admin API](../admin-api.md#on-a-cluster)).

## In front of it

vlRelay serves plain HTTP. Put a TLS proxy (Caddy, nginx, a cloud load balancer) in front of
`--listen` so consumers get `wss://` and PDSes can reach `requestCrawl` over `https://`.

- `/metrics` has no auth. Keep it off the public side of the proxy and scrape it on the private
  address.
- `/admin` is behind the admin token (HTTP basic, user `admin`). It's fine to expose, but there's
  no reason to. `/docs` is public and static, and it's served even without `--admin-token`.
- Proxy the websocket without buffering, and with an idle timeout of a minute or more.
- The peer port (2979) belongs on the private network only.
- Name the proxy with `--trusted-proxy <CIDR,...>` (or `VLRELAY_TRUSTED_PROXIES`), and have it
  append the client's address to `X-Forwarded-For`. Per-IP limits (consumers per IP,
  `requestCrawl` calls per minute) key on that header's rightmost address that isn't a trusted
  proxy, and only for requests whose peer is one. Without the flag every consumer behind the
  proxy counts as the proxy's address, and they share one per-IP cap. Don't list addresses that
  clients can connect from directly, since their `X-Forwarded-For` would be believed.

## Shutting down and upgrading

Send SIGTERM (`docker stop` does, and `tini` forwards it). A core marks its lease draining, hands
its host shards over (cursors checkpointed), then its DID shards, fences its log and exits. Its
consumers keep their sockets until the process exits and then resume elsewhere with their cursor.
The compose files give it 30 s (`stop_grace_period`).

So a rolling upgrade is one core at a time: stop it, start the new image, and wait for
`/xrpc/_health` before the next. vlRelay doesn't have vlpds's feature levels yet, so there's no
guard against a new version writing something an old one can't read.

Prometheus series are on `/metrics` of every node, and [Monitoring](monitoring.md) lists them.
