---
title: Cost
section: Reference
order: 303
summary: "What vlRelay costs to run for the whole network today and at 10x and 100x: hosts, the bucket and bandwidth, on OVH, Hetzner and AWS with six object stores."
---

```hero
diagram:
  caption: Where a relay's money goes. The bucket gets one flush every 30 s, so its requests barely register. The hosts are the bill, and consumer egress decides it wherever egress is metered.
  nodes:
    - { id: pds, label: PDSes, sub: "~15 Mb/s in today", at: [0, 3], size: [8, 3], tone: muted, stack: true }
    - { id: nodes, label: 3 nodes, sub: "~$14/mo on OVH VPS-1", at: [12, 3], size: [9, 3], tone: accent }
    - { id: bucket, label: Bucket requests, sub: "~$1/mo on R2", at: [25, 0], size: [10, 2.6], shape: store, tone: amber }
    - { id: store, label: Bucket storage, sub: "~$5/mo · 72 h of log", at: [25, 6], size: [10, 2.6], shape: store, tone: amber }
    - { id: cons, label: 10 consumers, sub: "~49 TB/mo out", at: [39, 3], size: [9, 3], tone: blue, stack: true }
  edges:
    - pds -> nodes
    - "nodes.r -> bucket.l: a flush every 30 s"
    - "nodes.r -> store.l"
    - "nodes.r -> cons.l: $0 on OVH · ~$4.1k on AWS"
facts:
  - { value: "~$18", unit: /mo, label: 3 nodes at today's load, note: "OVH VPS-1 + R2, 30 s flush, 10 consumers", tone: accent }
  - { value: "~$10", unit: /mo, label: one node, note: "OVH VPS-1 + R2, no HA", tone: violet }
  - { value: "~$1", unit: /mo, label: of bucket requests, note: "R2 after its free tier · ~$7 before it (measured over an hour)", tone: amber }
  - { value: "~$4.1k", unit: /mo, label: of egress on AWS, note: "10 consumers × the whole stream, today", tone: rust }
```

What vlRelay would cost to run for the whole network today, and at 10x and 100x that load: hosts,
bucket and bandwidth, priced on OVH, Hetzner and AWS with S3, R2, Tigris, GCS, B2 and Wasabi.
Every input is measured, counted from the code or assumed, and each table says which.
`python3 scripts/cost_model.py --quorum` regenerates the tables here from the inputs and prices at
its top.

## Headline

Monthly list prices at today's load (~350 events/s), 72 h of log in the bucket, 10 full-firehose
consumers, R2 after its free tier:

| Setup | Flush | Total | Where it goes |
|---|---|---|---|
| 3 x OVH VPS-1, commitlog | 30 s | ~$18 | hosts $14, bucket requests $0, storage $5 |
| 3 x OVH VPS-1, commitlog | 60 s | ~$18 | hosts $14, bucket requests $0, storage $5 |
| 3 x OVH VPS-1, commitlog, 24 h of log in the bucket | 60 s | ~$15 | hosts $14, storage $2 |
| 3 x Hetzner CAX21 (ARM), commitlog | 30 s | ~$43 | hosts $37, bucket $6 |
| 3 x Hetzner AX42, commitlog | 30 s | ~$333 | hosts $327, bucket $6 |
| 3 x OVH ADVANCE-2, commitlog | 30 s | ~$600 | hosts $594, bucket $6 |
| 3 x AWS c7gd.large + S3, consumers outside AWS | 30 s | ~$4.5k | egress and cross-AZ $4.3k, hosts $199, bucket $15 |
| One node, OVH VPS-1, NVMe commitlog + R2 | 30 s | ~$10 | host $5, bucket requests $1, storage $5 |
| One node, OVH VPS-1, NVMe commitlog + R2 | 60 s | ~$9 | host $5, storage $5 |

What the numbers say, in order of size:

- Consumer egress decides the bill wherever egress is metered. 10 consumers today pull ~49 TB a
  month. That's $0 on OVH's unmetered ports and ~$4.1k a month on AWS. Three AZs on AWS add
  ~$260 of replication traffic on top.
- The bucket barely registers. The leader flushes everything up to one seq every 30 s
  (`--qlog-flush-ms`), so requests come to ~$7 a month on R2 before its free tier and ~$1 after it
  (measured over an hour at today's rate). What's left is storage for 72 h of log (~$5 on R2).
- So hosts are the bill. At today's load the leader needs ~0.1 vCPU at the sized rate, and the
  smallest NVMe VPS carries a node. Three OVH VPS-1 come to $14 a month. On 8 GB VPSes, for more
  headroom, the cluster is ~$31.
- At 10x the cheapest three nodes are ~$123 (three VPS-4s, where disk and the port run out first).
  At 100x it's ~$3.4k, and ~$2.4k of that is edge boxes for 10 consumers' 14.8 Gb/s. The bucket is
  ~$500 a month of storage there, still not requests.

> [!NOTE]
> For comparison, a design that PUT a segment every 25 ms on every node and kept host bookkeeping
> in bucket objects came to ~$3.1k a month on three ADVANCE-2s, ~$2.5k of it bucket requests.

## Inputs

| Input | Value | Source |
|---|---|---|
| Events/s | ~350 average, ~480 busiest hour, ~180 quietest | the network's records, 7 days |
| Mean frame | ~5.3 KB (5,283-5,323 B measured) | frames read off `relay1.us-east.bsky.network` |
| Log compression | 1.56x at zstd -1 on production frames | [Performance](perf.md#compression) |
| CPU per event | 65-70 µs one node on cores, 89-94 µs on SMT threads | [Performance](perf.md) |
| Leader's share of a cluster's CPU | half (it checks and applies every event, and the others verify their hosts' share) | assumed |
| A flush's bucket work | a manifest CAS, its segments (64 MiB raw, cut per flush), ~8 Class A and ~12 Class B for the state's share | measured over an hour at 350/s, on MinIO and on R2 |
| SlateDB polls | ~1.26 GET/s for the leader's one SlateDB | measured, same run |
| Per-DID state | 121.8 B a DID in SSTs (243.6 B right after an update, before compaction) | measured on 1M DIDs in an in-memory bucket (2026-10-04) |
| Network today | 56M repos, ~24B records, 89.9M PLC DIDs | [vlpds](https://github.com/jazware/vlpds)'s cost model's queries |
| Hosts on the network | 6,260 listed, 1,956 active, 89 bsky.network PDSes with 23.4M of 24.0M accounts | `listHosts` |
| RAM baseline | 2 GB a node plus the firehose ring | assumed (1.1 GB measured at 60 events/s) |
| fsync | 0.03-0.1 ms datacenter NVMe, 0.5-2 ms VPS, 2.7 ms p50 consumer NVMe | assumed, the last measured |
| Fan-out | ~0.025 cores and ~1.6 Gb/s per consumer at 33k events/s | [Performance](perf.md#fan-out) |
| R2 free tier | 1M Class A, 10M Class B, 10 GB-month a month | developers.cloudflare.com/r2/pricing (2026-10-06) |

Assumptions the model adds:

- Size for a minute burst at 2x the peak hour (as vlpds's model does), at no more than 70% CPU. So
  350 events/s average is sized as 960.
- A NIC or a guaranteed port is usable to 70%.
- Accounts, records and PLC DIDs grow with the event rate. That's an upper bound, and it's what
  makes disk the limit at 100x (~850 GB of DID state per node).
- A month is 30.44 days (2.63M s), as in vlpds's model.

## Load levels

| Load | Events/s avg | Peak hour | Sized for | Ingest | Log after zstd -1 | The 512 MB ring holds |
|---|---|---|---|---|---|---|
| today | 350 | 480 | 960 | 15 Mb/s | 10 Mb/s | ~290 s |
| 10x | 3,500 | 4,800 | 9,600 | 148 Mb/s | 95 Mb/s | ~29 s |
| 100x | 35,000 | 48,000 | 96,000 | 1.5 Gb/s | 950 Mb/s | ~3 s |
| 1000x | 350,000 | 480,000 | 960,000 | 14.8 Gb/s | 9.5 Gb/s | under 1 s |

The design's 100k events/s target sits between 100x and 1000x (~285x). Ingest is the same number
of bits as one full-firehose consumer, since the relay re-emits every frame as it came. Past the
ring, a consumer that reconnects reads from its node's commitlog or the bucket.

## What a node needs

The leader in a three-node cluster, with 10 consumers:

| Load | Flush | vCPUs | RAM, commitlog | Disk | Public port | Replication out |
|---|---|---|---|---|---|---|
| today | 10 s | 0.10 | 2.5 GB | 23 GB | 54 Mb/s | 30 Mb/s |
| today | 60 s | 0.10 | 2.5 GB | 23 GB | 54 Mb/s | 30 Mb/s |
| 10x | 10 s | 1.03 | 2.5 GB | 138 GB | 544 Mb/s | 297 Mb/s |
| 10x | 60 s | 1.03 | 2.6 GB | 138 GB | 544 Mb/s | 297 Mb/s |
| 100x | 10 s | 10.29 | 2.7 GB | 1,291 GB | 5.4 Gb/s | 3.0 Gb/s |
| 100x | 60 s | 10.29 | 3.3 GB | 1,291 GB | 5.4 Gb/s | 3.0 Gb/s |

A single node needs ~20% more CPU (it verifies every host) and the whole public port (163 Mb/s
today with 10 consumers). Disk is the commitlog plus the DID state. CPU barely registers below
10x, so size hosts by RAM, disk and port.

Which hosts fit, three nodes with commitlogs, a 30 s flush and 10 consumers ($/mo for all three
with R2, or S3 on AWS):

| Host | $/mo each | today | 10x | 100x |
|---|---|---|---|---|
| OVH VPS-1 | $5 | $18 | no: disk 138/40 GB, port 841 Mb/s/0.5 Gb/s | no: CPU, disk, port |
| OVH VPS-2 | $8 | $31 | no: disk 138/75 GB, port 841 Mb/s/1 Gb/s | no: CPU, disk, port |
| OVH VPS-4 | $23 | $76 | $123 | no: CPU 10.3/8, disk 1,291/200 GB, port 8.4/3 Gb/s |
| OVH ADVANCE-2 | $198 | $600 | $647 | no: disk 1,291/960 GB, port 5.4/3 Gb/s |
| Hetzner CAX21 (ARM) | $12 | $43 | no: disk 138/80 GB, port 841 Mb/s/1 Gb/s | no: CPU, disk, port |
| Hetzner AX42 | $109 | $333 | $707, 3 edges | no: port 8.4/1 Gb/s |
| Hetzner AX42 + 10G | $157 | $476 | $1,035 | $3,385, 22 edges |
| AWS c7gd.large | $66 | $4,540 | no: disk 138/118 GB, port 841 Mb/s/0.94 Gb/s | no: CPU, disk, port |
| AWS c7gd.xlarge | $132 | $4,739 | ~$30k | no: CPU, disk, port |

One node, NVMe commitlog, a 30 s flush, 10 consumers, R2 holding 72 h:

| Host | $/mo | today | 10x | 100x |
|---|---|---|---|---|
| OVH VPS-1 | $5 | $10 (4 h on disk) | no: disk 138/40 GB, port 1.6/0.5 Gb/s | no |
| OVH VPS-4 | $23 | $29 (35 h on disk) | $76 (2 h on disk) | no |
| Hetzner AX42 | $109 | $115 (72 h on disk) | $489, 3 edges (36 h on disk) | no: port 16.3/1 Gb/s |
| Hetzner AX42 + 10G | $157 | $196 (72 h on disk) | $769 (36 h on disk) | $3,071, 22 edges (2 h on disk) |
| OVH ADVANCE-2 | $198 | $204 (72 h on disk) | $251 (17 h on disk) | no: disk, port |

## The bucket

### Requests

A flush is a manifest CAS, its segments and the state's share: ~8 Class A and ~12 Class B for
the state's L0, the checkpoint it seals and the one it retires and compaction, plus ~1.26 Class B a second of SlateDB's polls. Segments are cut at 64 MiB raw, so at
today's rate a 30 s flush (~56 MB) is one PUT. The leader is the only node that writes, and
followers send nothing in steady state.

| Load | Flush | Segment PUTs/s | Class A/s | Class B/s | R2 $/mo, no free tier | R2 $/mo | S3 $/mo |
|---|---|---|---|---|---|---|---|
| today | 10 s | 0.10 | 1.00 | 2.46 | $14 | $7 | $16 |
| today | 30 s | 0.03 | 0.33 | 1.66 | $6 | $0 | $6 |
| today | 60 s | 0.03 | 0.18 | 1.46 | $4 | $0 | $4 |
| 10x | 10 s | 0.30 | 1.20 | 2.46 | $17 | $10 | $18 |
| 10x | 30 s | 0.30 | 0.60 | 1.66 | $9 | $3 | $10 |
| 10x | 60 s | 0.28 | 0.43 | 1.46 | $7 | $1 | $7 |
| 100x | 10 s | 2.80 | 3.70 | 2.46 | $46 | $39 | $51 |
| 100x | 30 s | 2.77 | 3.07 | 1.66 | $38 | $32 | $42 |
| 100x | 60 s | 2.77 | 2.92 | 1.46 | $36 | $30 | $40 |

The today-at-30 s row is measured: three nodes, an hour at 350 events/s on a local MinIO, every
request counted by purpose. It came to 0.48 Class A and 1.66 Class B a second, which the model
reproduces. An hour on a real R2 bucket (2026-10-06, the same three nodes and load) counted the
same: 0.478 Class A and 1.725 Class B a second. Cloudflare's own count for a half hour of it
matched on Class B exactly (3,280 against 3,277 counted) and came to 626 Class A against 1,006
counted: R2 bills none of SlateDB's GC deletes (one-key `DeleteObjects`, ~0.15 a second) as Class
A. So the model leaves them out, and what R2 bills today at 30 s is ~0.33 Class A and ~1.7 Class B
a second. Reads from a bench box took 51-102 ms at the median and writes 205-410 ms, the round trips
the recovery and membership estimates assume. The rest is modeled from it. Takeovers, membership changes, recovery and retention are
a few requests each, and backfill reads one GET per segment.

The knobs, in R2 $/mo before the free tier, to show the slope:

| Knob | today, 10 s | today, 60 s | 100x, 10 s | 100x, 60 s |
|---|---|---|---|---|
| default: 64 MiB segments, one state | $14 | $4 | $46 | $36 |
| 8 MiB segments | $17 | $6 | $276 | $265 |

- Flush interval. At 60 s requests sit inside R2's free tier, at 30 s they are too, and at
  10 s they're ~$7 after it. A longer flush only costs the re-ingest window ([below](#re-ingest-after-a-lost-tail)),
  and with a commitlog that only matters when two disks are lost. So 30-60 s.
- Segment size. 8 MiB segments are fine today but cost ~$230 a month more at 100x.
- Bucket retention. 72 h of log (`--qlog-retain-hours`) is ~$5 a month on R2 today, and 24 h is
  ~$1.6.

### Storage

| Load | Log, 72 h | Per-DID state | S3 $/mo | R2 $/mo |
|---|---|---|---|---|
| today | 308 GB | 8.5 GB | $7 | $5 |
| 10x | 3.1 TB | 85 GB | $74 | $48 |
| 100x | 31 TB | 853 GB | $742 | $484 |

The log is the rate times 5.3 KB over 1.56, for 72 h. Per-DID state carries the 25% budget vlpds
uses for replaced SSTs awaiting GC. At 100x, storage is the bucket's whole bill, and a shorter
window is the lever (24 h saves two thirds of the log line).

### Per provider

> [!WARNING]
> B2 and Wasabi can't hold a vlRelay bucket. `qlog/leader` and `qlog/manifest` are written with
> compare-and-swap, and segments and every SlateDB manifest depend on conditional PUTs
> (`If-None-Match: *`, `If-Match`). Neither documents them, and B2 is reported to accept and
> ignore the headers, which would let a deposed leader overwrite its successor's manifest.

- R2 is ~10% cheaper than S3 per request and 35% cheaper per GB, and its egress is free.
- Tigris prices Class A like S3 and Class B 25% above it. Its egress is free too, and it documents
  conditional writes.
- GCS matches S3 per request. Its regional storage is a bit cheaper. Turn off its 7-day soft
  delete, or every deleted segment is billed for a week.
- Wasabi's requests are free, but every object is billed for at least 90 days. A 72 h log would be
  billed as 30 times its size.

## Replication traffic

| Load | Bytes per event | Cluster total | TB/mo | OVH vRack | Hetzner private network | AWS cross-AZ |
|---|---|---|---|---|---|---|
| today | 14.2 KB | 40 Mb/s | 13 | $0 | $0 | $261 |
| 10x | 14.2 KB | 397 Mb/s | 131 | $0 | $0 | $2,613 |
| 100x | 14.2 KB | 4.0 Gb/s | 1,306 | $0 | $0 | $26k |

Each node submits its hosts' events to the leader, and the leader replicates every entry to both
followers. Traffic between nodes is free on OVH's vRack and Hetzner's private network. On AWS,
nodes in three AZs pay $0.01/GB each way, so a 3-AZ cluster at 100x pays ~$26k a month to talk to
itself.

## Re-ingest after a lost tail

If two nodes' disks are lost, the log resumes from the bucket's last flush and the PDSes send the
rest again from their flushed cursors ([Cluster](cluster.md#when-a-node-dies)). The worst case is a
crash just before a flush:

| Load | Flush | Window | Events re-requested | Bytes | Catch-up |
|---|---|---|---|---|---|
| today | 30 s | 37 s | 12,950 | 0.07 GB | 12.7 s |
| today | 60 s | 67 s | 23,450 | 0.12 GB | 23.0 s |
| 10x | 30 s | 37 s | 129,500 | 0.69 GB | 12.7 s |
| 100x | 30 s | 37 s | 1,295,000 | 6.86 GB | 12.7 s |
| 100x | 60 s | 67 s | 2,345,000 | 12.43 GB | 23.0 s |

A process crash, or all three at once, loses nothing, since the commitlogs hold every committed
entry.

## Consumer egress

Each full-firehose consumer gets every frame, so it costs exactly one ingest's worth:

| Load | Consumers | Egress | TB/mo | Nodes at 10 GbE | 25 GbE | 100 GbE |
|---|---|---|---|---|---|---|
| today | 10 | 148 Mb/s | 49 | 1 | 1 | 1 |
| today | 100 | 1.5 Gb/s | 488 | 1 | 1 | 1 |
| today | 1,000 | 14.8 Gb/s | 4,878 | 3 | 1 | 1 |
| 10x | 10 | 1.5 Gb/s | 488 | 1 | 1 | 1 |
| 10x | 100 | 14.8 Gb/s | 4,878 | 3 | 1 | 1 |
| 10x | 1,000 | 148 Gb/s | 48,783 | 22 | 9 | 3 |
| 100x | 10 | 14.8 Gb/s | 4,878 | 3 | 1 | 1 |
| 100x | 100 | 148 Gb/s | 48,783 | 22 | 9 | 3 |
| 100x | 1,000 | 1,484 Gb/s | 487,828 | 212 | 85 | 22 |

Node counts are at 70% of the NIC. CPU isn't the limit: serving costs ~1 core per 64 Gb/s
([Performance](perf.md#fan-out)), so a 100 GbE edge needs ~1.1 cores for its consumers. What it
costs per provider with dedicated nodes (ADVANCE-2 on OVH, AX42 on Hetzner), picking each
provider's cheapest SKU per usable Gb/s, and serving from the nodes while half their spare port
covers it:

| Load | Consumers | OVH | Hetzner | AWS |
|---|---|---|---|---|
| today | 10 | $0 (on the nodes) | $0 (on the nodes) | ~$4.1k egress |
| today | 100 | $0 (on the nodes) | ~$510 egress past 20 TB a server | ~$27k egress |
| today | 1,000 | ~$1.6k (8 x ADVANCE-2) | ~$2.4k (22 x AX42) | ~$247k |
| 10x | 100 | ~$1.6k (8) | ~$2.4k (22) | ~$247k |
| 10x | 1,000 | ~$14k (71) | ~$23k (213) | ~$2.4M |
| 100x | 10 | ~$1.6k (8) | ~$2.4k (22) | ~$247k |
| 100x | 100 | ~$14k (71) | ~$23k (213) | ~$2.4M |
| 100x | 1,000 | ~$140k (707) | ~$231k (2,120) | ~$24M |

Per usable Gb/s a month, the options are:

| Provider and SKU | Port | $/mo | $ per usable Gb/s |
|---|---|---|---|
| OVH ADVANCE-2 | 3 Gb/s guaranteed, unmetered | $198 | ~$94 |
| OVH SCALE-a1 + 25 Gb/s | 25 Gb/s guaranteed, unmetered | $2,165 | ~$124 |
| OVH ADVANCE-2 + 5 Gb/s | 5 Gb/s guaranteed, unmetered | $478 | ~$137 |
| Hetzner AX42 | 1 Gb/s, unlimited traffic | ~$109 | ~$156 |
| Hetzner AX42 + 10G uplink | 10 Gb/s, 20 TB out, then $1.20/TB | ~$157 | ~$410 at full use |
| AWS internet egress | metered | $0.05-0.09/GB | ~$16k-30k |

So OVH's small dedicated boxes are the cheapest egress there is, and the big ones are a fair trade
for fewer nodes (9 SCALE boxes at 25 GbE instead of 71 ADVANCE). OVH has no public port above
25 Gb/s, so 100 GbE means other providers or colocation. OVH's VPSes list unlimited traffic, but
on shared best-effort ports, so they aren't in this table. On AWS, egress is 90% or more of every
row with consumers outside AWS. The only AWS deployment that makes sense is one where the
consumers are in the same region.

### Public segments on R2 (design option, not built)

The design leaves publishing flushed segments in a public R2 bucket for later
([Design](design.md#decisions)). It would make backfill nearly free for the relay. A consumer replaying 24 h at 100x pulls ~10 TB of
compressed segments. From the relay that's ~11 hours of an OVH 3 Gb/s port or ~$920 of AWS egress.
From public R2 it's $0 of egress and well under a dollar of GETs, since a segment holds up to
64 MiB of frames. The cost is that the segment format and naming become a public API.

## Compared with vlpds

| | vlpds (PDS) | vlRelay |
|---|---|---|
| Load modeled | Bluesky's writes, ~334 commits/s | Bluesky's firehose, ~350 events/s |
| Bucket requests at 3 nodes, S3 | ~$1.7k/mo (64 shards) | ~$8/mo (30 s flush) |
| CPU per commit or event | ~100 µs end to end | ~70 µs on one node |
| Storage | ~4.9 TB (all repos) | ~320 GB (72 h of log and per-DID state) |

vlpds's bill is a request bill, since every commit must be durable in the bucket before it's
acked. A relay's upstream PDSes are the source of truth, so vlRelay keeps its recent log in its
members' commitlogs and writes the bucket in bulk. If it loses an unflushed tail, it asks the PDSes
again. The relay's real difference is fan-out. A PDS's outbound is getBlob and proxying, and a
relay's is the whole stream once per consumer, which is why bandwidth decides where to run it.

## Caveats

- The bucket's request rate is measured on a local MinIO, over an hour at today's rate and for 15
  minutes at 10x, and on a real R2 bucket for an hour at today's rate. 10x and 100x haven't been
  measured on R2, so those rows are modeled.
- The VPS prices assume the smallest NVMe VPS keeps up. Their fsync latency (assumed 0.5-2 ms) and
  how often two members land on one physical disk haven't been measured.
- The per-event CPU numbers come from one 16-core box on loopback. AWS cores are Graviton, and
  nothing measured vlRelay on ARM.
- AWS on-demand list prices, no reserved, savings plan or private egress pricing. OVH prices are
  the US catalog's no-commitment monthly prices. Hetzner repriced twice in 2026, and its figures
  are from third-party listings.
- Consumers here take the whole stream. Jetstream-style filtered or compressed outputs would divide
  every egress number, and they don't exist yet.

## What 1000x would need

At 1000x (350k events/s) no host in the model fits, and none of this is built:

- More than one log. One leader checks and applies every event, which is ~10 vCPUs at 100x
  sized. Past that the stream would split into several logs with a leader each and a merge into
  one stream.
- Parallel segment PUTs in the flush, and datacenter NVMe for the commitlogs. At 100x a node
  already writes ~16 TB a day and needs ~185 MB/s fsynced.
- Fan-out tiers that aren't full-firehose websockets: a compressed stream extension (~3x less per
  consumer, [Design](design.md#scale)), filtered outputs, and public segments behind a CDN for anyone
  who can read segments directly. At 14.8 Gb/s per raw consumer, 1,000 consumers is 14.8 Tb/s, and
  no host bill fixes that.
- Host placement by load, since verify cost follows hosts and a few big PDSes carry most events.
- A shorter retention window, since 72 h of log is 308 TB.

## Prices

Fetched 2026-10-01 to 2026-10-06:

| Item | Price | Source |
|---|---|---|
| S3 Standard us-east-1 | $0.023/GB-mo (first 50 TB) · PUT/LIST $5/M · GET $0.40/M · DELETE free | aws.amazon.com/s3/pricing (2026-10-01) |
| AWS internet egress, us-east-1 | $0.09/GB first 10 TB · $0.085 next 40 · $0.07 next 100 · $0.05 above · 100 GB free | AWS data transfer pricing via egresscost.com (2026-10-04) |
| AWS cross-AZ | $0.01/GB each way | AWS data transfer pricing |
| GCS Standard regional | $0.020/GiB-mo · Class A $5/M · Class B $0.40/M | cloud.google.com/storage/pricing (2026-10-01) |
| Cloudflare R2 Standard | $0.015/GB-mo · Class A $4.50/M · Class B $0.36/M · egress free · free tier 1M A, 10M B, 10 GB-month | developers.cloudflare.com/r2/pricing (2026-10-06) |
| Tigris Standard | $0.02/GB-mo · Class A $5/M ($0.005/1k) · Class B $0.50/M · egress free · conditional writes documented | tigrisdata.com/pricing, tigrisdata.com/docs/objects/conditionals (2026-10-04) |
| Backblaze B2 | $6.95/TB-mo · Class A, B and C calls free · egress free up to 3x storage, then $0.01/GB · no documented conditional writes | backblaze.com/cloud-storage/pricing, /transaction-pricing (2026-10-04) |
| Wasabi pay-go | $7.99/TB-mo · no request or egress fees while egress ≤ storage · 90-day minimum per object · no documented conditional writes | wasabi.com/pricing, docs.wasabi.com 90-day policy (2026-10-04) |
| OVH VPS-1 to VPS-4 | $4.54, $8.50, $12.32, $23.37/mo · 2-8 vCores, 4-24 GB, 40-200 GB NVMe · 0.5-3 Gb/s, unlimited traffic | us.ovhcloud.com/vps (2026-10-06) |
| OVH ADVANCE-2 (EPYC 4344P 8c/16t, 64 GB), US | $198/mo · 3 Gb/s guaranteed unmetered public · 25 Gb/s vRack · 5 Gb/s +$280 | api.us.ovhcloud.com public bare-metal catalog (2026-10-04) |
| OVH SCALE-a1 2026 (EPYC 9135 16c/32t), US | $765/mo · 5 Gb/s included · 10 Gb/s +$621 · 25 Gb/s +$1,400 · 50 Gb/s private | same catalog |
| Hetzner Cloud CX33, CAX21, CPX22 | $9.99, $12.49, $22.99/mo (EUR 8.49, 10.49, 19.49) | costgoat.com listing (2026-09-05) |
| Hetzner AX42 (Ryzen 7 PRO 8700GE 8c/16t, 64 GB, 2 x 1.92 TB NVMe) | ~€97.30/mo (~$109) · 1 Gb/s, unlimited traffic | third-party listing of the June 2026 repricing (bex.co, 2026-09) |
| Hetzner 10G uplink | €43 ($48)/mo · 20 TB out included, then €1 ($1.20)/TB · inbound and internal free | docs.hetzner.com price-server-addons, 10g-uplink (2026-10-04) |
| AWS c7gd.large / c7gd.xlarge | $0.0907/h / $0.1814/h on demand · 118 / 237 GB instance NVMe | third-party listings (2026-10-06) |
| AWS c7gn.2xlarge | $0.499/h on demand · up to 50 Gb/s | AWS on-demand prices via Vantage and Holori (2026-10-04) |

## Reproducing it

```
python3 scripts/cost_model.py --quorum          # every table on this page but consumer egress
python3 scripts/cost_model.py --quorum --json   # the per-load, per-flush numbers
python3 scripts/cost_model.py                   # the consumer egress tables
```

The hour on R2 confirmed the request table at today's rate. The next measurement worth making is
the request rate on R2 at 10x and 100x.
