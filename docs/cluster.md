---
title: Cluster
section: vlRelay
order: 5
summary: "Every node reads its share of the PDSes and forwards each checked event to the leader, which checks its chain, gives it the next seq and emits it once two of the three nodes hold it. The bucket gets a flush every 30 s."
---

```hero
diagram:
  caption: Each node reads the PDSes the leader's host table gives it and forwards every checked event to the leader. The leader checks the event against the account's record, appends it and replicates it; once two of the three nodes hold it, it's committed and every node emits it. Every 30 s the leader flushes the log, the records and the host table to the bucket in one manifest.
  nodes:
    - { id: pds, label: PDSes, sub: subscribeRepos, at: [0, 5], size: [8, 3], tone: muted, stack: true }
    - { id: n1, label: node n1, sub: "hosts · verify", at: [12, 0], size: [9, 3], tone: accent }
    - { id: n2, label: leader n2, sub: "check chain · seq · append", at: [12, 5], size: [9, 3], tone: violet }
    - { id: n3, label: node n3, sub: "hosts · verify", at: [12, 10], size: [9, 3], tone: accent }
    - { id: man, label: "`qlog/manifest`", sub: "F · R · segments", at: [27, 0], size: [10, 2.6], shape: store, tone: amber }
    - { id: state, label: "`qlog/state`", sub: "records · hosts · cursors at F", at: [27, 3.4], size: [10, 2.6], shape: store, tone: amber }
    - { id: log, label: "`log/qlog/`", sub: "64 MiB segments", at: [27, 6.8], size: [10, 2.6], shape: store, tone: amber }
    - { id: lead, label: "`qlog/leader`", sub: "epoch · members", at: [27, 10.2], size: [10, 2.6], shape: store, tone: amber }
    - { id: cons, label: Consumers, sub: any node, at: [0, 14.5], size: [8, 3], tone: blue }
  groups:
    - { label: one quorum log · commit at 2 of 3, around: [n1, n2, n3], tone: accent }
    - { label: the bucket · a flush every 30 s, around: [man, state, log, lead], tone: amber }
  edges:
    - "pds -> n1: ws"
    - "pds -> n3: ws"
    - "n1 -> n2: submit"
    - "n3 -> n2: submit"
    - { from: n2.r, to: state.l, label: flush }
    - { from: n2.r70, to: log.l, label: segments }
    - { from: n2.b, to: cons.r, label: emit at commit, tone: blue, via: [[16.5, 16]] }
facts:
  - { value: "2 of 3", label: commit, note: "nothing reaches a consumer before a quorum holds it", tone: accent }
  - { value: "50-120 ms", label: emission pause when the leader dies, note: "kill -9 at 10x; ~1 s when it hangs or is cut off", tone: violet }
  - { value: "30 s", label: bucket flush, note: "`--qlog-flush-ms`; the log, the records and the host table at one seq", tone: amber }
  - { value: "2 s", label: a dead node's hosts move, note: "`--qlog-host-failover-ms`, then they resume from their cursors", tone: blue }
```

A quorum cluster is three nodes (or one, or five) that keep the recent log in each other instead
of the bucket. One node leads. Every node reads its share of the PDSes and verifies their events,
then hands each one to the leader. The leader checks it against the account's record, gives it the
next seq and replicates it, and the event reaches consumers on every node once two of the three
hold it. The bucket gets everything up to one seq every 30 seconds: the log as segments, the
accounts' records, the host table and the PDS cursors, with one manifest written last. A consumer
can connect to any node and resume on another with its cursor.

One node is the same thing with one member: its commitlog is the write-ahead log, it flushes to
the bucket on the same schedule, and adding members later is a membership change.

## Running one

You need three hosts, each with a local NVMe disk, a private network between them, and one bucket.

### The bucket

Any S3-compatible store with strongly consistent conditional writes (`If-None-Match: *` and
`If-Match`) works: S3, R2, GCS, Tigris and MinIO all do. Create a bucket and an access key that
can read, write, list and delete in it. Every node uses the same bucket and the same `--prefix`,
and two relays can share a bucket under different prefixes.

| Flag | Env | What |
|---|---|---|
| `--s3-endpoint URL` | `VLRELAY_S3_ENDPOINT` | `https://<account-id>.r2.cloudflarestorage.com`, `https://s3.<region>.amazonaws.com`, `http://minio:9000` |
| `--s3-bucket NAME` | `VLRELAY_S3_BUCKET` | The bucket |
| `--s3-access-key`, `--s3-secret-key` | `VLRELAY_S3_ACCESS_KEY`, `VLRELAY_S3_SECRET_KEY` | The key. `--s3-access-key-file` and `--s3-secret-key-file` read them from files |
| `--s3-region` | `VLRELAY_S3_REGION` | `auto` (the default) for R2, the bucket's region for S3 |
| `--prefix` | `VLRELAY_PREFIX` | The relay's key prefix, `vlrelay` by default |

Put the shared settings in one env file and copy it to every host (mode 600):

```bash
# /etc/vlrelay/env
VLRELAY_S3_ENDPOINT=https://<account-id>.r2.cloudflarestorage.com
VLRELAY_S3_BUCKET=<your-bucket>
VLRELAY_S3_ACCESS_KEY=<access key id>
VLRELAY_S3_SECRET_KEY=<secret access key>
VLRELAY_PREFIX=relay1
# the dashboard's password
VLRELAY_ADMIN_TOKEN=<openssl rand -hex 16>
# what a membership change must carry
QLOG_ADMIN_TOKEN=<openssl rand -hex 16>
```

### The peer network

The nodes replicate to each other on the peer port (`--qlog-listen`, 2978). It has no TLS or
auth of its own, so it belongs on a private network: a LAN, a VPC, or a WireGuard mesh such as
Tailscale between hosts at different providers. The examples use `10.0.0.1`, `10.0.0.2` and
`10.0.0.3` for the three hosts' private addresses. Every node needs to reach the other two on
2978, and nothing else should.

### Starting the nodes

On the first host (`10.0.0.1`), give the commitlog a directory the image's user (uid 10001) can
write, then start the node with the other two as peers:

```bash
sudo install -d -o 10001 -g 10001 /var/lib/vlrelay/qlog
docker run -d --name vlrelay --restart unless-stopped --stop-timeout 30 \
  --env-file /etc/vlrelay/env \
  -p 10.0.0.1:2980:2980 -p 10.0.0.1:2978:2978 \
  -v /var/lib/vlrelay/qlog:/var/lib/vlrelay/qlog \
  ghcr.io/jazware/vlrelay \
  --node-id n1 --qlog-listen 0.0.0.0:2978 \
  --qlog-peer n2=10.0.0.2:2978 --qlog-peer n3=10.0.0.3:2978 \
  --qlog-dir /var/lib/vlrelay/qlog --host pds.example.com --crawl
```

Do the same on the other two with `--node-id n2` and `n3`, their own addresses in `-p`, and the
other two nodes as `--qlog-peer`s. The nodes elect a leader once two of them are up, and the first
start writes the member set to `qlog/leader` in the bucket. After that the bucket's record wins
over the flags.

`--host` and `--crawl` work on any node: a PDS admitted anywhere goes into the leader's host
table, and the leader gives it to a node. Each node's `/qlog/status` says who leads, and the
dashboard's Quorum page (`/admin/quorum`) shows all three members. Port 2980 serves the firehose,
the sync API, `/admin` and `/docs`, but also `/metrics` and `/qlog/status` with no auth, so put a
TLS proxy in front of it that passes everything except `/qlog/` and `/metrics`.
[Deploy](operations/deploy.md) covers the proxy and rolling upgrades.

| Flag | Default | What |
|---|---|---|
| `--node-id ID` | `relay` | The member's name. Unique per node. |
| `--qlog-listen ADDR` | `127.0.0.1:2978` | The peer port: replication, submits, members' questions. Keep it to the other nodes. |
| `--qlog-peer ID=ADDR` | | Where to reach each other node (repeatable). |
| `--qlog-members a,b,c` | this node and its peers | The first member set. After the first start, `qlog/leader` in the bucket holds it. |
| `--qlog-dir DIR` | | The commitlog, on a local NVMe disk. Without it the log is in memory only. |
| `--qlog-flush-ms MS` | 30000 | How often the leader flushes to the bucket. |
| `--qlog-admin-token T` | | What a membership change must carry (`QLOG_ADMIN_TOKEN`). |
| `--qlog-host-failover-ms MS` | 2000 | A member the leader hasn't heard from in this long loses its PDSes to the others. |
| `--qlog-retain-hours H` | 72 | The leader deletes log segments older than this (0: never). |
| `--durability MODE` | `page-cache` (three or more), `fsync` (one) | When an entry counts on a node: once in the commitlog's page cache (fdatasync'd every `--durability-sync-ms`, 100), after its fdatasync, or in `memory` only. A single node runs `fsync`. |
| `--bootstrap-relay URL` | | A relay whose `listHosts` seeds host discovery on a first start ([Policy](policy.md#discovering-hosts)). |

Every flag is in [Configuration](operations/configuration.md).

## The path of an event

1. A node reads a PDS the leader gave it, parses the event and checks its signature and its MST
   proof. Verifying is most of the CPU, so it stays spread out.
2. It sends the event to the leader in a batch, with the account's events always in one batch
   stream, so they arrive in order.
3. The leader checks the event against the account's record: the PDS is the one the DID document
   names, the account is active, the commit's `prevData` matches the record and its rev is newer.
   It appends the event with the account's new record, under the next seq, and replicates it.
4. Once two of the three nodes hold it on disk, the leader answers and every node emits it. The
   reading node then counts the event as done for its PDS's cursor.

A duplicate (the PDS sent it again after a reconnect) is answered without an entry, and so is an
event that fails a check. The leader takes a PDS's events only from the node its host table names,
so two sockets on one PDS can't mix up an account's order.

## When a node dies

| What happens | What it costs |
|---|---|
| The leader's process dies | another node takes over in 50-120 ms (kill -9 at 10x), and nothing committed is lost |
| The leader hangs or is cut off | the others take over after 1 s of silence |
| A follower dies | nothing for consumers. Its PDSes move to the others after 2 s and resume from their cursors |
| Every process dies at once | a takeover once they're back. Nothing is lost in any mode but `memory` (the page cache outlives a process) |
| Two nodes lose power within ~100 ms (`page-cache`), or two disks are lost | the log resumes from the bucket's last flush, with a jump in the seqs, and the PDSes send the rest again |

The new leader opens the account records at the last flush and replays its own log from there
before it takes events, which takes tens of milliseconds. A node that comes back catches up from
the leader and serves its consumers from its own log again.

## Changing the members

Replacing a machine, or growing from one node to three, is a membership change. Start the new node
first, with the others as `--qlog-peer`s, then send the change to the leader from the dashboard's
Quorum page. `POST /admin/api/cluster/quorum/members` ([Admin API](admin-api.md#endpoints)) does
the same, and so does the `qlog` tool in this repository, which finds the leader and retries
across a leader change:

```bash
QLOG_ADMIN_TOKEN=... cargo run --release --bin qlog -- member \
  --node n1=10.0.0.1:2980 --node n2=10.0.0.2:2980 replace n3 n4
```

The new node joins as a learner, copies the leader's log, and the switch happens at a flush:
commits pause for about 10 ms. A removed node stops getting events and its PDSes move.

## What's in the bucket

| Path | What | Written |
|---|---|---|
| `qlog/leader` | the epoch, the leader and the members | at a takeover or a membership change |
| `qlog/manifest` | F (the last seq flushed), the segments, the records' checkpoint, the cursors | every flush, last |
| `qlog/state*` | one SlateDB: each account's record, the host table, the cursors, at exactly F | every flush |
| `log/qlog/` | 64 MiB segments of the log, for old cursors | every flush |
| `retain/qlog` | what retention deleted | every retention pass |

At today's network rate that's about 0.5 write and 1.7 read requests a second, a few dollars a
month on R2 before its free tier.

## What a node answers for

The accounts' records are the leader's: the sync API's repo endpoints and the admin API's account
pages answer on the leader, and a follower names it. Consumers, a host's socket and the pipeline
numbers are each node's own. Any member serves the stream, so there are no separate edges or
replicas.
