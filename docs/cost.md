---
title: Cost
section: Reference
order: 303
summary: "What vlRelay costs to run for the whole network today and at 10x, 100x and 1000x: hosts, bucket and bandwidth on OVH, Hetzner and AWS with six object stores."
---

```hero
diagram:
  caption: Where a relay's money goes. Requests to the bucket follow the number of logs and host shards, not the traffic. Consumer egress follows the number of consumers times the whole stream, and decides the bill wherever egress is metered.
  nodes:
    - { id: pds, label: PDSes, sub: "~15 Mb/s in today", at: [0, 3], size: [8, 3], tone: muted, stack: true }
    - { id: cores, label: 3 core nodes, sub: "~$600/mo on OVH", at: [12, 3], size: [9, 3], tone: accent }
    - { id: bucket, label: Bucket requests, sub: "~$2.8k/mo on R2", at: [25, 0], size: [10, 2.6], shape: store, tone: amber }
    - { id: store, label: Bucket storage, sub: "~$5/mo · 72 h of log", at: [25, 6], size: [10, 2.6], shape: store, tone: amber }
    - { id: cons, label: 100 consumers, sub: "~490 TB/mo out", at: [39, 3], size: [9, 3], tone: blue, stack: true }
  edges:
    - pds -> cores
    - "cores.r -> bucket.l: PUTs · GETs"
    - "cores.r -> store.l"
    - "cores.r -> cons.l: $0 on OVH · ~$27k on AWS"
facts:
  - { value: "~$3.4k", unit: /mo, label: 3 cores at today's load, note: "OVH + R2, 100 consumers; requests are $2.8k of it", tone: amber }
  - { value: "~$1.0k", unit: /mo, label: one node with an edge, note: "no HA; OVH + R2", tone: accent }
  - { value: "~$27k", unit: /mo, label: of egress on AWS, note: "100 consumers × the whole stream, today", tone: rust }
  - { value: "~1M", unit: events/s, label: where the full mesh stops scaling, note: "every core streams to every other; a merge tier fixes it (not built)", tone: violet }
```

What vlRelay would cost to run for the whole network today, and at 10x, 100x and 1000x that load: hosts, bucket and bandwidth, priced on OVH, Hetzner and AWS with S3, R2, Tigris, GCS, B2 and Wasabi. It follows vlpds's cost model. Every input is measured, counted from the code or assumed, and each table says which. `scripts/cost_model.py` regenerates every table here from the inputs and prices at its top.

## Headline

Monthly list prices, 3 core nodes at today's load (~350 events/s), 72 h of log, 100 full-firehose consumers:

| Setup | Total | Where it goes |
|---|---|---|
| OVH (3 x ADVANCE-2) + R2 | ~$3.4k | bucket requests $2.8k, hosts $594, storage $5, bandwidth $0 |
| OVH + Tigris | ~$3.9k | requests $3.3k |
| OVH + S3 | ~$4.9k | requests $3.1k, S3 egress for backfill reads $1.2k |
| Hetzner (3 x AX42 + 10G) + R2 | ~$3.8k | requests $2.8k, egress past 20 TB a server $0.5k |
| AWS (3 x c7g.4xlarge) + S3, in region | ~$32k | internet egress to the consumers $27k |
| One node, no HA, OVH + R2 | ~$1.0k | requests $572, two ADVANCE-2 (node + edge) $396 |

What the numbers say, in order of size:

- Consumer egress decides the bill wherever egress is metered. 100 consumers today pull ~490 TB a month. That's $0 on OVH's unmetered ports and ~$27k a month on AWS. At 100x it's ~$2.5M a month on AWS and ~$14k of OVH edge boxes.
- On unmetered hosts the bucket's requests are the biggest line until about 10x, and storage barely shows. Requests follow the number of logs and host shards, so they hardly follow traffic: ~$2.5k at 350 events/s and ~$2.8k at 35,000 (R2, requests only).
- Over half of a cluster's request bill isn't the log. It's the cluster's host bookkeeping in the bucket: host cursor checkpoints every 2 s, host counters and the registry every 5 s, each a GET plus a conditional PUT per host shard. That's ~$1.3k a month on S3 (~$1.15k on R2), counted from the code and not measured. A single node keeps the same rows in its SlateDB and pays ~$0.5k in all.
- CPU is cheap until 100x. Three 8-core nodes run today's load at under 1% and 100x at ~30% of their threads, sized for a 2x burst over the peak hour.
- 1000x (350k events/s) doesn't work as built. Every core streams its log to every other core, so the per-event cost grows with the node count and stops converging around 60 nodes. A merge tier fixes the CPU, but the consumer egress at that rate (14.8 Gb/s per full-firehose consumer) needs compressed or filtered streams more than it needs any host.

## Inputs

| Input | Value | Source |
|---|---|---|
| Events/s | ~350 average, ~480 busiest hour, ~180 quietest | the network's records, 7 days |
| Mean frame | ~5.3 KB (5,283-5,323 B measured) | frames read off `relay1.us-east.bsky.network` |
| Log compression | 1.56x at zstd -1 on production frames | [Performance](perf.md#compression) |
| CPU per event | 65-70 µs (one node, 8 cores) · 89-94 µs (one node on SMT threads) · 142-156 µs (3-node cluster, threads) | [Performance](perf.md) |
| Cluster overhead split | forward ~15 µs per forwarded event · log streams ~8 µs per event per peer copy · merge ~1 µs per event per node · the rest ~30 µs | the cluster profile ([Performance](perf.md#a-three-node-cluster)), used to scale with the node count |
| Peer bytes | 12.7-15.2 KB per event on 3 nodes | [Performance](perf.md#a-three-node-cluster) |
| Segment sealing | 25 ms linger from a segment's first event (`--linger-ms`), 32 PUTs in flight, 8 MiB raw cap | `src/seq.rs` |
| Seal overhead | ~2.8 ms per seal | vlpds cost model |
| Segment PUTs at low rates | ~20/s for 60 events/s on one node, ~35/s for 3 cores (2 with a log) | a two-hour run against ten real PDSes |
| DID shard checkpoints | every 5 s per shard, an L0 flush each: ~4.4 Class A + ~9 Class B with the compaction it causes | `node.rs` + vlpds cost model |
| SlateDB polls | ~0.4 GET/s per shard (manifest 10 s, compactor 30 s) | vlpds defaults, measured there |
| DID shards | 4 on one node, 24 in a cluster | `--did-shards` default |
| Host bookkeeping | 64 host shards · hostck/ every 2 s · tick every 5 s | `src/cluster/hosts.rs`, `src/node/cluster.rs` (code, not measured) |
| Per-DID state | 121.8 B a DID in SSTs (243.6 B right after an update, before compaction) | `tests/state_bulk.rs`, 1M DIDs, run for this page (2026-10-04) |
| PLC seeds | ~54 B a DID in SSTs (~3 GB for 56M) | [policy](policy.md#plc-export-seeding) |
| Archival mirror | 154.2 B a record + 323 B a repo | vlpds `bench/results/storage-2026-10-02` (same layout) |
| Network today | 56M repos, ~24B records, 89.9M PLC DIDs | vlpds cost model's ClickHouse queries |
| Fan-out | ~0.025 cores and ~1.6 Gb/s per consumer at 33k events/s | [Performance](perf.md#fan-out) |

Assumptions the model adds:

- Size for a minute burst at 2x the peak hour (as vlpds's model does), at no more than 70% CPU. So 350 events/s average is sized as 960.
- A NIC or a guaranteed port is usable to 70%.
- Each consumer replays 1 h of history a day from the bucket (reconnects past the ring, deploys).
- Accounts, records and PLC DIDs grow with the event rate. That's an upper bound for storage, and storage is small anyway.
- A month is 30.44 days (2.63M s), as in vlpds's model.

## Load levels

| Load | Events/s avg | Peak hour | Sized for | Ingest | Log after zstd -1 | The 512 MB ring holds |
|---|---|---|---|---|---|---|
| today | 350 | 480 | 960 | 15 Mb/s | 10 Mb/s | ~290 s |
| 10x | 3,500 | 4,800 | 9,600 | 148 Mb/s | 95 Mb/s | ~29 s |
| 100x | 35,000 | 48,000 | 96,000 | 1.5 Gb/s | 950 Mb/s | ~3 s |
| 1000x | 350,000 | 480,000 | 960,000 | 14.8 Gb/s | 9.5 Gb/s | under 1 s |

The design's 100k events/s target sits between 100x and 1000x (~285x). Ingest is the same number of bits as one full-firehose consumer, since the relay re-emits every frame as it came. The ring column matters for backfill: past 100x, any consumer that reconnects reads from the bucket.

## CPU and nodes

Sized with 8-core, 16-thread boxes (OVH ADVANCE-2, Hetzner AX42), at the cluster's measured cost per thread:

| Load | Core nodes | µs per event | Busy threads, average | At the sized rate | Of the cluster |
|---|---|---|---|---|---|
| today | 3 | 150 | 0.1 | 0.1 | ~0% |
| 10x | 3 | 150 | 0.5 | 1.4 | 3% |
| 100x | 3 | 150 | 5.3 | 14.4 | 30% |
| 1000x, as built | 58 | 675 | 236 | 648 | 70% |

Three nodes is the HA floor, and CPU doesn't move it until well past 100x. A single node runs today's load on ~0.03 cores (70 µs an event) and could take 100x on average (2.5 cores), but not the sized burst with headroom.

The 1000x row is where the full mesh gives out. Each core streams its whole log to every other core, so every event is copied, encrypted and decrypted once per peer. The cost per event grows with the node count:

| Nodes | µs/event, full mesh | µs/event, 3 merge nodes | Events/s at 70%, mesh | Events/s at 70%, merge tier |
|---|---|---|---|---|
| 3 | 150 | 155 | 224k | 217k |
| 6 | 181 | 157 | 371k | 427k |
| 12 | 239 | 158 | 563k | 848k |
| 24 | 353 | 159 | 762k | 1.7M |
| 48 | 580 | 159 | 927k | 3.4M |

So the mesh tops out around 1M events/s however many nodes it has, and 960k sized needs ~58 of them at 675 µs an event. With cores streaming only to 3 merge nodes (a design sketch, not built), 14 cores cover it at ~159 µs. The scaling terms are the cluster bench's profile split linearly, which nothing measured past 3 nodes.

Archival apply adds ~130 µs an event (69 when the tree is still in memory, [archival](archival.md#numbers)). That's nothing today and 12.5 threads at 100x sized, which takes the 3 nodes from 30% to ~56%.

## Peer traffic

| Load | Nodes | Bytes per event | Cluster total | Per node, each way | AWS cross-AZ |
|---|---|---|---|---|---|
| today | 3 | 14.2 KB | 40 Mb/s | 13 Mb/s | ~$170/mo |
| 10x | 3 | 14.2 KB | 400 Mb/s | 130 Mb/s | ~$1.7k/mo |
| 100x | 3 | 14.2 KB | 4.0 Gb/s | 1.3 Gb/s | ~$17k/mo |
| 1000x, as built | 58 | 307 KB | 861 Gb/s | 14.8 Gb/s | ~$3.8M/mo |

Per event, a 3-node cluster forwards two thirds of the frames to their DID owner (~3.6 KB) and streams the raw frame to both peers (~10.6 KB). The model gives 14.2 KB against 12.7-15.2 measured. Per node each way, that's about one full firehose at any node count, since a node receives every other node's log. What grows with the count is the cluster total.

Peer traffic is free on OVH (the vRack, 25 Gb/s on ADVANCE) and Hetzner (internal traffic is unmetered). On AWS, nodes in three AZs pay $0.01/GB each way, so a 3-AZ cluster at 100x pays ~$17k a month to talk to itself. Compressed log streams ([Performance](perf.md#what-s-next)) would cut the stream part by a third.

## The bucket

### Requests

Segment PUTs follow nodes and round trips. A segment opens at its first event and seals a linger (25 ms) later. So one log PUTs at `1 / (linger + 2.8 ms + 1/rate)` until 8 MiB fills inside a linger, which takes ~57k events/s per log. The model gives 16.4 PUTs/s for a 30 events/s log against 17.2 measured in the shadow run, and 22.5 against ~20 at 60 events/s.

| Load | Nodes | Segment PUTs/s | Events per segment | Other Class A/s | Class B/s | S3 $/mo | R2 $/mo |
|---|---|---|---|---|---|---|---|
| today | 1 | 33 | 11 (36 KB) | 4 | 9 | $491 | $442 |
| today | 3 | 83 | 4 (14 KB) | 111 | 216 | $2,777 | $2,500 |
| 10x | 1 | 36 | 97 (331 KB) | 4 | 9 | $530 | $477 |
| 10x | 3 | 106 | 33 (113 KB) | 111 | 216 | $3,072 | $2,765 |
| 100x | 3 | 109 | 322 (1.1 MB) | 111 | 216 | $3,111 | $2,799 |
| 1000x, as built | 58 | 2,094 | 167 (570 KB) | 925 | 2,383 | $42k | $38k |

Requests barely move from today to 100x: a log seals ~28-36 times a second either way, and the segments just get bigger. Over the day today's rate moves the 3-node PUT rate between ~68/s (quietest hour) and ~88/s (busiest). That's the vlpds lesson again, and it holds at 1000x as built only because 58 nodes means 58 logs.

The "other" column on a cluster is mostly host bookkeeping. From the code, per second, on 3 nodes:

| Source | Class A/s | Class B/s | S3 $/mo |
|---|---|---|---|
| DID shard checkpoints (24 shards, an L0 flush each every 5 s, plus compaction) | 21.2 | 43.5 | $325 |
| SlateDB polls | 0 | 9.6 | $10 |
| Host cursor checkpoints (`hostck/`): a GET and a conditional PUT per host shard every 2 s | 32.0 | 32.0 | $454 |
| Host counters (`hosts/`): every node, for every host shard, every 5 s | 38.4 | 38.4 | $545 |
| Host registry flush (`hosts/`), per host shard every 5 s | 12.8 | 12.8 | $182 |
| Re-reading `hostck/` and `hosts/` on every node every 5 s (a LIST, then a GET of each changed object) | 1.2 | 76.8 | $97 |
| Leases, assignments, seq checkpoints, retention | 5.2 | 3.0 | $72 |

At 64 host shards and a network's worth of hosts, every shard has a moving cursor and fresh counters every tick, so these run at their maximum from today's load up. Two of them don't touch latency or failover: the host counters are statistics, and the registry rows are reloaded only to find hosts another node admitted. Flushing both every 60 s instead of every 5 s would save ~$670 a month on S3 (~24% of the cluster's requests). The cursor checkpoints are different, since their interval is how much a PDS replays after a crash. None of this is measured. vlpds's wrapper that counts requests by op and key (`Store::counted`) isn't wired into vlRelay, and wiring it is how to check this table.

GETs, beyond the polls and bookkeeping:

- Catch-up and backfill read one GET per segment. Today's 3-node segments hold ~4 events (~14 KB), so a consumer replaying an hour costs ~300k GETs (~$0.12 on S3). 100 consumers replaying an hour a day add ~350 GETs/s, about $360 a month on S3. A node's local segment cache would absorb most of it.
- A replica tails each log with 2 GETs every 20 ms plus the GET time (`src/cluster/follow.rs`). That's ~133 GETs/s for a 3-log cluster at any load, ~$126 a month on R2 (~$420 on S3 once the replica sits outside AWS and pays egress, ~$18k at 100x).
- Core restarts and catch-ups read a log's tail once.

### Storage

| Load | Log, 72 h | Per-DID state | PLC seeds | S3 $/mo | R2 $/mo |
|---|---|---|---|---|---|
| today | 308 GB | 8.5 GB | 6.1 GB | $7 | $5 |
| 10x | 3.1 TB | 85 GB | 61 GB | $74 | $48 |
| 100x | 31 TB | 853 GB | 607 GB | $742 | $484 |
| 1000x | 308 TB | 8.5 TB | 6.1 TB | $7.2k | $4.8k |

The log is the rate times 5.3 KB over 1.56, for 72 h. The design guessed ~130 GB today at 3x compression, but production frames are mostly hashes and compress 1.56x, so it's ~310 GB. Per-DID state and seeds carry the 25% budget vlpds uses for replaced SSTs awaiting GC. Storage stays under a quarter of the bucket bill until 1000x, where a shorter window would be the lever (24 h saves two thirds of the log line).

### Per provider

Requests plus storage, no consumers:

| Load | S3 | GCS | R2 | Tigris | B2 | Wasabi |
|---|---|---|---|---|---|---|
| today | $2,785 | $2,783 | $2,504 | $2,841 | ($2) | ($74) |
| 10x | $3,146 | $3,132 | $2,813 | $3,193 | ($22) | ($740) |
| 100x | $3,853 | $3,712 | $3,284 | $3,813 | ($224) | ($7,400) |
| 1000x, as built | $49k | $48k | $43k | $49k | ($2.2k) | ($74k) |

> [!WARNING]
> B2 and Wasabi can't hold a vlRelay bucket. Leases, fences, segment creates and every SlateDB manifest depend on conditional PUTs (`If-None-Match: *`, `If-Match`). Neither documents them, and B2 is reported to accept and ignore the headers, which would let a zombie node overwrite its successor. Their numbers are in parentheses for comparison only.

- R2 is ~10% cheaper than S3 per request and 35% cheaper per GB, and its egress is free.
- Tigris prices Class A like S3 and Class B 25% above it. Its egress is free too, and it documents conditional writes.
- GCS matches S3 per request. Its regional storage is a bit cheaper. Turn off its 7-day soft delete, or every deleted segment is billed for a week.
- Wasabi's requests are free, but every object is billed for at least 90 days. A 72 h log would be billed as 30 times its size.

### Archival mirror (optional)

| Load | Records | Mirror | R2 $/mo | S3 $/mo | Apply CPU (sized) |
|---|---|---|---|---|---|
| today | 24B | ~4.6 TB | ~$69 | ~$107 | 0.1 threads |
| 10x | 240B | ~46 TB | ~$690 | ~$1.0k | 1.2 |
| 100x | 2.4T | ~460 TB | ~$6.9k | ~$10.2k | 12.5 |
| 1000x | 24T | ~4.6 PB | ~$69k | ~$97k | 125 |

Records scale with load here, which overstates the mirror for a year at 10x. The bytes use vlpds's measured layout (154.2 B a record plus 323 B a repo) and the same 25% transient budget. The design's "$50-100 a month on R2" holds today. Requests barely change: the rows ride the DID shards' existing checkpoint flushes, and compaction at today's rate is a few GETs a second. The first backfill is ~56M `getRepo` fetches of a few TB, inbound and free on every provider here.

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
| 1000x | 10 | 148 Gb/s | 48,783 | 22 | 9 | 3 |
| 1000x | 100 | 1,484 Gb/s | 487,828 | 212 | 85 | 22 |
| 1000x | 1,000 | 14,840 Gb/s | 4.9M | 2,120 | 848 | 212 |

Node counts are at 70% of the NIC. CPU isn't the limit: serving costs ~1 core per 64 Gb/s ([Performance](perf.md#fan-out)), so a 100 GbE edge needs ~1.1 cores for its consumers. What it costs per provider, picking each provider's cheapest SKU per usable Gb/s, and serving from the core nodes while half their spare port covers it:

| Load | Consumers | OVH | Hetzner | AWS |
|---|---|---|---|---|
| today | 10 | $0 (on the cores) | $0 (on the cores) | ~$4.1k egress |
| today | 100 | $0 (on the cores) | ~$510 egress past 20 TB a server | ~$27k egress |
| today | 1,000 | ~$1.6k (8 x ADVANCE-2) | ~$2.4k (22 x AX42) | ~$247k |
| 10x | 100 | ~$1.6k (8) | ~$2.4k (22) | ~$247k |
| 10x | 1,000 | ~$14k (71) | ~$23k (213) | ~$2.4M |
| 100x | 10 | ~$1.6k (8) | ~$2.4k (22) | ~$247k |
| 100x | 100 | ~$14k (71) | ~$23k (213) | ~$2.4M |
| 100x | 1,000 | ~$140k (707) | ~$231k (2,120) | ~$24M |
| 1000x | 100 | ~$140k (707) | ~$231k (2,120) | ~$24M |
| 1000x | 1,000 | ~$1.4M (7,067) | ~$2.3M (21,200) | ~$244M |

Per usable Gb/s a month, the options are:

| Provider and SKU | Port | $/mo | $ per usable Gb/s |
|---|---|---|---|
| OVH ADVANCE-2 | 3 Gb/s guaranteed, unmetered | $198 | ~$94 |
| OVH SCALE-a1 + 25 Gb/s | 25 Gb/s guaranteed, unmetered | $2,165 | ~$124 |
| OVH ADVANCE-2 + 5 Gb/s | 5 Gb/s guaranteed, unmetered | $478 | ~$137 |
| Hetzner AX42 | 1 Gb/s, unlimited traffic | ~$109 | ~$156 |
| Hetzner AX42 + 10G uplink | 10 Gb/s, 20 TB out, then $1.20/TB | ~$157 | ~$410 at full use |
| AWS internet egress | metered | $0.05-0.09/GB | ~$16k-30k |

So OVH's small boxes are the cheapest egress there is, and the big ones are a fair trade for fewer nodes (9 SCALE boxes at 25 GbE instead of 71 ADVANCE). OVH has no public port above 25 Gb/s, so 100 GbE means other providers or colocation. OVH's VPS-4 lists 3 Gb/s with unlimited traffic for $23, but that's a shared best-effort port and isn't in the tables. On AWS, egress is 90% or more of every row with consumers outside AWS. The only AWS deployment that makes sense is one where the consumers are in the same region.

### Public segments on R2 (design option, not built)

The design leaves open publishing sealed segments in a public R2 bucket (design "Backfill"). It changes two things.

Backfill gets nearly free. A consumer replaying 24 h at 100x pulls ~10 TB of compressed segments. From the relay that's ~11 hours of an OVH 3 Gb/s port or ~$920 of AWS egress. From public R2 it's ~9M GETs, ~$3, and egress is $0:

| Load | One 24 h replay (zstd) | GETs | From public R2 | From AWS | On an OVH 3 Gb/s port |
|---|---|---|---|---|---|
| today | 0.10 TB | 7.2M | ~$3 | ~$9 | ~6 min |
| 10x | 1.0 TB | 9.1M | ~$3 | ~$92 | ~1 h |
| 100x | 10 TB | 9.4M | ~$3 | ~$920 | ~11 h |
| 1000x | 103 TB | 181M | ~$65 | ~$7.8k | ~4.5 days |

Live fan-out could move too, as replicas tailing the public bucket. Then each tailing consumer costs ~133 GETs/s on R2 (~$126 a month) whatever the rate, against ~$94 per usable Gb/s on OVH: ~$1.40 a consumer today, ~$14 at 10x, ~$139 at 100x and ~$1,400 at 1000x. So direct tailing breaks even near 100x and wins by ~10x at 1000x. A CDN in front of the bucket would turn most of those GETs into cache hits, since every consumer asks for the same segment names. The cost is that the segment format and naming become a public API.

## Monthly totals

Hosts, bucket and metered bandwidth together. Core nodes are ADVANCE-2 on OVH, AX42 with the 10G uplink on Hetzner and c7g.4xlarge on AWS. Edges are as in the egress section. OVH and Hetzner with S3 pay S3's egress on backfill reads.

| Load | Consumers | OVH + R2 | OVH + Tigris | OVH + S3 | Hetzner + R2 | AWS + S3 |
|---|---|---|---|---|---|---|
| today | 10 | $3.1k | $3.5k | $3.5k | $3.0k | $8.3k |
| today | 100 | $3.4k | $3.9k | $4.9k | $3.8k | $32k |
| today | 1,000 | $8.0k | $9.6k | $18k | $8.7k | $254k |
| 10x | 10 | $3.4k | $3.8k | $4.9k | $3.8k | $33k |
| 10x | 100 | $5.4k | $6.0k | $15k | $6.1k | $253k |
| 10x | 1,000 | $22k | $24k | $90k | $31k | $2.5M |
| 100x | 10 | $5.5k | $6.1k | $15k | $6.2k | $270k |
| 100x | 100 | $18k | $19k | $87k | $27k | $2.5M |
| 100x | 1,000 | $148k | $150k | $803k | $239k | $24.5M |
| 1000x, as built | 10 | $69k | $76k | $144k | $76k | $6.3M |
| 1000x, as built | 100 | $203k | $212k | $864k | $291k | $28.3M |
| 1000x, as built | 1,000 | $1.5M | $1.6M | $8.1M | $2.4M | $248M |
| 1000x, merge tier (sketch) | 10 | $38k | $41k | $109k | n/a | $2.7M |
| 1000x, merge tier (sketch) | 100 | $166k | $170k | $824k | n/a | $24.7M |

The merge-tier rows add 3 SCALE-a1 boxes with 25 Gb/s ports to take the whole raw stream. Hetzner's 10 Gb/s uplink can't carry 14.8 Gb/s, so it's n/a there without compressed peer streams.

Where the money goes, at 100 consumers:

OVH + R2:

| Load | Core nodes | Edges | Bucket requests | Bucket storage | Metered bandwidth | Total |
|---|---|---|---|---|---|---|
| today | $594 | $0 (on the cores) | $2,828 | $5 | $0 | $3.4k |
| 10x | $594 | $1,584 (8) | $3,182 | $48 | $0 | $5.4k |
| 100x | $594 | $14k (71) | $3,228 | $484 | $0 | $18k |
| 1000x, as built | $11k (58) | $140k (707) | $46k | $4.8k | $0 | $203k |
| 1000x, merge tier | $9.3k (14 + 3) | $140k (707) | $12k | $4.8k | $0 | $166k |

AWS + S3:

| Load | Core nodes | Edges | Bucket requests | Bucket storage | Metered bandwidth | Total |
|---|---|---|---|---|---|---|
| today | $1,270 | $0 | $3,142 | $7 | $27k | $32k |
| 10x | $1,270 | $0 | $3,535 | $74 | $248k | $253k |
| 100x | $1,270 | $3,278 (9) | $3,587 | $742 | $2.5M | $2.5M |
| 1000x, as built | $25k | $31k (85) | $51k | $7.2k | $28M | $28M |

On OVH the split moves from the bucket (82% today) to edge boxes (78% at 100x). On AWS it's egress at every size.

## The linger knob

The relay's linger is 25 ms, chosen for time to firehose ([Design](design.md#decisions)). Segment PUTs and their S3 cost per month, at each load's node count, with nothing else changed:

| Load | Nodes | 10 ms | 25 ms (default) | 50 ms | 100 ms | 250 ms |
|---|---|---|---|---|---|---|
| today | 3 | 142/s · $1,863 | 83/s · $1,095 | 49/s · $649 | 27/s · $358 | 12/s · $152 |
| 10x | 3 | 222/s · $2,916 | 106/s · $1,390 | 56/s · $742 | 29/s · $384 | 12/s · $157 |
| 100x | 3 | 235/s · $3,090 | 109/s · $1,428 | 57/s · $753 | 29/s · $387 | 22/s · $294 |
| 1000x, as built | 58 | 4,516/s · $59k | 2,094/s · $28k | 1,105/s · $15k | 569/s · $7.5k | 231/s · $3.0k |

R2 is 10% less. Going from 25 to 50 ms would save ~$450 a month today and ~$680 at 100x. It would add up to 25 ms to time to firehose, since a segment's first event waits the whole extra linger and its last waits none. At 250 ms and 100x, segments start sealing on size, so the line flattens. So the 25 ms default costs a few hundred dollars a month over the top of the design's 25–50 ms range, and that's the price of the lower latency.

## Compared with vlpds

| | vlpds (PDS) | vlRelay |
|---|---|---|
| Load modeled | Bluesky's writes, ~334 commits/s | Bluesky's firehose, ~350 events/s |
| Bucket at 3 nodes, S3 | ~$1.7k/mo (64 shards) | ~$2.8k/mo |
| Per million commits or events | ~$1.95 | ~$3.03 (~$0.54 on one node) |
| Biggest bucket line | segment PUTs, then checkpoint flushes | host bookkeeping, then segment PUTs |
| CPU per commit or event | ~100 µs end to end | ~70 µs on one node, ~150 µs per thread in a cluster |
| Storage | ~4.9 TB (all repos) | ~320 GB (72 h of log and per-DID state), ~4.6 TB more with the archival mirror |

Both bills are request bills that don't follow traffic. Per node, the relay's segment PUTs come out about where vlpds's do at today's rate (~27/s), since a 25 ms linger plus the gap to the next event is about one S3 round trip. It has fewer shards (24 against 64), so fewer checkpoints and polls. What it adds is the host bookkeeping in bucket objects, which a vlpds node keeps nowhere. The relay's real difference is fan-out. A PDS's outbound is getBlob and proxying, and a relay's is the whole stream once per consumer, which is why bandwidth and not the bucket decides where to run it.

## Caveats

- The 100k events/s figure is extrapolated from one box on loopback. The cluster bench ran 3 nodes of 3 cores plus SMT on one 16-core box, sharing one memory bus, one L3 and one MinIO, with loopback for the NICs. The per-event CPU numbers come from there, and so does everything at 100x and above.
- MinIO's latency isn't S3's or R2's. The model doesn't depend on PUT latency below ~1 s (32 in flight), but time to firehose does, and replica polling assumes ~25 ms GETs. R2 is often slower than S3 in-region, which would lower the replica GET rate and raise the cluster's time to firehose.
- The host bookkeeping is counted from the code and hasn't been measured. Each term scales with the host shards that see traffic in a tick (at most 64), so a quieter network than assumed would pay less, never more.
- 1000x goes far past anything measured. The node counts there extrapolate one profile's split of the cluster overhead linearly to 58 nodes. A 58-node full mesh would need 14.8 Gb/s each way per node just between cores and 861 Gb/s across the cluster, which no part of the system has been tried at.
- AWS on-demand list prices, no reserved, savings plan or private egress pricing. AWS cores are Graviton, and nothing measured vlRelay on ARM. OVH prices are the US catalog's no-commitment monthly prices. Hetzner repriced twice in 2026, and the AX42 figure is from a third-party listing.
- Consumers here take the whole stream. Jetstream-style filtered or compressed outputs would divide every egress number, and they don't exist yet.

## What 1000x would need

From the numbers above, none of them built:

- Cores that stream their logs to a merge tier instead of to each other. That keeps the per-event cost flat (~159 µs) as cores are added, and only the merge and edge nodes see the whole stream.
- Compressed peer streams. Sealed segments are already zstd'd. Shipping those instead of raw frames saves a third of the peer bytes and the copy and TLS work that follows them, and gets the merged stream (9.5 Gb/s compressed) under a 10 Gb/s port.
- Fan-out tiers that aren't full-firehose websockets: a compressed stream extension (~3x less per consumer, design "Scale target"), filtered outputs, and public segments behind a CDN for anyone who can read segments directly. At 14.8 Gb/s per raw consumer, 1,000 consumers is 14.8 Tb/s, and no host bill fixes that.
- Host shards placed by load, and more of them, since verify cost follows hosts and a few big PDSes carry most events.
- A shorter retention window, since 72 h of log is 308 TB.

## Prices

Fetched 2026-10-01 (vlpds cost model, still current) and 2026-10-04:

| Item | Price | Source |
|---|---|---|
| S3 Standard us-east-1 | $0.023/GB-mo (first 50 TB) · PUT/LIST $5/M · GET $0.40/M · DELETE free | aws.amazon.com/s3/pricing (2026-10-01) |
| AWS internet egress, us-east-1 | $0.09/GB first 10 TB · $0.085 next 40 · $0.07 next 100 · $0.05 above · 100 GB free | AWS data transfer pricing via egresscost.com (2026-10-04) |
| AWS cross-AZ | $0.01/GB each way | AWS data transfer pricing |
| GCS Standard regional | $0.020/GiB-mo · Class A $5/M · Class B $0.40/M | cloud.google.com/storage/pricing (2026-10-01) |
| Cloudflare R2 Standard | $0.015/GB-mo · Class A $4.50/M · Class B $0.36/M · egress free | developers.cloudflare.com/r2/pricing (2026-10-01) |
| Tigris Standard | $0.02/GB-mo · Class A $5/M ($0.005/1k) · Class B $0.50/M · egress free · conditional writes documented | tigrisdata.com/pricing, tigrisdata.com/docs/objects/conditionals (2026-10-04) |
| Backblaze B2 | $6.95/TB-mo · Class A, B and C calls free · egress free up to 3x storage, then $0.01/GB · no documented conditional writes | backblaze.com/cloud-storage/pricing, /transaction-pricing (2026-10-04) |
| Wasabi pay-go | $7.99/TB-mo · no request or egress fees while egress ≤ storage · 90-day minimum per object · no documented conditional writes | wasabi.com/pricing, docs.wasabi.com 90-day policy (2026-10-04) |
| OVH ADVANCE-2 (EPYC 4344P 8c/16t, 64 GB), US | $198/mo · 3 Gb/s guaranteed unmetered public · 25 Gb/s vRack · 5 Gb/s +$280 | api.us.ovhcloud.com public bare-metal catalog (2026-10-04) |
| OVH SCALE-a1 2026 (EPYC 9135 16c/32t), US | $765/mo · 5 Gb/s included · 10 Gb/s +$621 · 25 Gb/s +$1,400 · 50 Gb/s private | same catalog |
| OVH VPS-4 | $23.37/mo · 8 vCores, 24 GB · 3 Gb/s, unlimited traffic (outside APAC) | us.ovhcloud.com/vps (2026-10-04) |
| Hetzner AX42 (Ryzen 7 PRO 8700GE 8c/16t, 64 GB) | ~€97.30/mo (~$109) · 1 Gb/s, unlimited traffic | third-party listing of the June 2026 repricing (bex.co, 2026-09) |
| Hetzner 10G uplink | €43 ($48)/mo · 20 TB out included, then €1 ($1.20)/TB · inbound and internal free | docs.hetzner.com price-server-addons, 10g-uplink (2026-10-04) |
| AWS c7g.4xlarge / c7gn.2xlarge | $0.580/h / $0.499/h on demand (730 h) · up to 15 / 50 Gb/s | AWS on-demand prices via Vantage and Holori (2026-10-04) |

## Reproducing it

```
python3 scripts/cost_model.py          # every table on this page, as markdown
python3 scripts/cost_model.py --json   # the per-load scenario numbers
```

The per-DID state number comes from

```
BULK_DIDS=1000000 BULK_SHARDS=8 cargo test --release --test state_bulk -- --ignored --nocapture
```

on an in-memory bucket (121.8 B a DID after create, 243.6 B after an update until compaction). The next measurement worth making is the cluster's request mix: wire `Store::counted` into vlRelay and run `cluster-perf.sh` at today's rate for an hour, which checks the host-bookkeeping table above.
