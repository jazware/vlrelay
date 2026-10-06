---
title: Stream seqs
section: vlRelay
order: 4
summary: "Consumers see seqs 1, 2, 3, … like indigo's relay. Each seq is an event's position in the merged stream, a pure function of the bucket, so every node agrees on it without talking to the others."
---

```hero
diagram:
  caption: Every node merges the same durable log entries in the same key order, so the n-th event is the same everywhere. Checkpoints in the bucket let a node that starts late, or a cursor from yesterday, find its place without counting from the first event.
  nodes:
    - { id: l1, label: core 1 log, sub: merge keys, at: [0, 0], size: [8, 2.6], tone: accent }
    - { id: l2, label: core 2 log, sub: merge keys, at: [0, 3.5], size: [8, 2.6], tone: accent }
    - { id: l3, label: core 3 log, sub: merge keys, at: [0, 7], size: [8, 2.6], tone: accent }
    - { id: merge, label: Merger, sub: sort by key · 2 ms batches, at: [12, 3.5], size: [9, 2.6], tone: blue }
    - { id: num, label: Renumber, sub: "next seq, spliced in", at: [25, 3.5], size: [9, 2.6], tone: blue }
    - { id: subs, label: Consumers, sub: "seq 1, 2, 3, …", at: [38, 3.5], size: [8, 2.6] }
    - { id: ck, label: "`seqck/{key}-{seq}`", sub: "empty object · every 10 s", at: [24, 9], size: [11, 2.6], shape: store, tone: amber }
  edges:
    - l1.r -> merge.l30
    - "l2.r -> merge.l: durable entries"
    - l3.r -> merge.l70
    - "merge -> num"
    - "num -> subs"
    - { from: num.b, to: ck.t, label: write or check, dash: true }
facts:
  - { value: "< 2^53", label: dense seqs, note: "a JavaScript number holds them; 100k events/s for 2,800 years", tone: blue }
  - { value: "0", label: coordination per event, note: "one empty PUT per checkpoint interval per core" }
  - { value: "10 s", label: between checkpoints, note: "8,640 a day; ~26k live at 72 h retention", tone: amber }
  - { value: "< 1 µs", label: of CPU per event, note: "measured on a 3-node cluster at 97k/s", tone: violet }
```

Consumers see seqs 1, 2, 3, … like indigo's relay. Internally, everything still runs on vlpds's
merge key, `unix_micros << 8 | writer`. That key is about 4.6e17, well above 2^53. `@atproto/sync`
decodes it as a BigInt and fails lexicon validation on every frame (`Expected integer value
type`), and any JSON or JavaScript consumer that doesn't validate loses precision and resumes from
the wrong cursor. The atproto spec and indigo both expect a dense integer that a double can hold,
so vlRelay renumbers the stream on the way out.

vlpds's own PDS firehose keeps its time-based seqs. The renumbering is opt-in in vlpds's firehose,
and only vlRelay turns it on.

## What a seq is

The seq of an event is its position in the merged stream: the number of events whose merge key is
at or below its own, counted from the first event the bucket ever held.

That's a pure function of what's in the bucket. Every node merges the same durable log entries in
the same key order (the cluster e2e checks this byte for byte), so every core, edge and replica
gives the same event the same seq with no coordination at all. Seqs only go up and are never
reused across takeovers and restarts, because the merged order never changes after the fact. An
event at or below the merger's emitted bound is never added later.

| Requirement | How |
|---|---|
| Below 2^53, dense | A counter: 100k events/s for 2,800 years |
| Identical everywhere, late starters included | A function of the bucket's contents, not of any node's history |
| Monotonic, no reuse | Merged order is append-only |
| Cheap | No per-event coordination, one empty PUT per checkpoint interval per core |

## Checkpoints

A node can't count from the first event every time it starts, since retention deletes old
segments. So nodes write checkpoints:

- `seqck/{key:020}-{seq:020}` is an empty object. Its name says that exactly `seq` events have a
  merge key at or below `key`. Since the pair is in the name, a LIST returns everything and no
  GETs are needed.
- The keys are fixed boundaries, `(k * every) << 8 | 0xff` with `every` = 10 s. Every node
  computes the same pair for the same boundary as its merger passes it. A core lists the boundary
  first and PUTs it if it's missing. If it's there, the core checks it's the same pair, and a
  different one is logged as an error (`seq checkpoints disagree`), since it means two nodes
  numbered the stream differently.
- A core whose own log has been fenced is a zombie (a successor took over, and its merge no longer
  sees every log), so it writes nothing. Edges and replicas never write, since they have read-only
  credentials, but they keep their own pairs in memory.

Retention cuts at a checkpoint: the newest one at or below the window's edge. It deletes only
segments wholly below that checkpoint, then the checkpoints below the retained floor. So there's
always a checkpoint at or above the oldest event still in the bucket.

## Where the seq is spliced in

Frames are logged with the merge key in `seq`, as in vlpds. The merger sorts each 2 ms batch by
key, then assigns the next seqs and splices each one into its frame while it builds the batch's
shared wire buffer. So each event is spliced once per node, and every subscriber writes the same
pre-framed bytes. The ring, cursors and per-connection stats are all in seqs. Each batch also keeps
its events' keys for time to firehose, lag and backfill.

A node's merged stream starts at a floor F (its clock at startup). Before it emits anything, the
merger waits until every followed log is durable up to F. Then it anchors: it takes the newest
checkpoint (K, N) with K ≤ F, and counts the events in (K, F] from the bucket with the ordinary
backfill reader, which is at most one checkpoint interval of events. Subscribers wait until the
stream is anchored, a few hundred milliseconds at startup.

The count is only right if the merge can't settle past F before the node follows every peer's
log, so two holds close that gap. A core follows the peers its join found before its merger
starts, and keeps a placeholder source just below F until its first membership sync (a lease TTL
at most). Edges and replicas start their membership source just below F.

## Cursors

- No cursor: live from the head.
- A cursor in the ring: served from memory.
- An older cursor: the node finds the newest checkpoint (K, N) with N ≤ cursor and K at or above
  the retained floor, reads the bucket from K, numbers events N+1, N+2, … and skips the ones at or
  below the cursor. When the backfill reaches the ring's floor, its count has to land exactly on
  the ring floor's seq, and a mismatch is logged as a numbering error.
- A cursor older than retention: if the newest usable checkpoint's seq is above the cursor, the
  events in between were pruned. The consumer gets `#info OutdatedCursor` and continues from that
  checkpoint. The few events kept below it (less than one interval) aren't served.
- A cursor past the head: a count can't be compared with the clock the way a time-based seq can.
  A node that trails another (an edge ~300 ms behind the cores, or a node that just restarted)
  would answer `FutureCursor` to a consumer that failed over with a good cursor. So the stream
  waits up to 2 s for its head to reach the cursor, and only then sends `FutureCursor`.

## Alternatives considered

- Compact time keys, such as `(µs since 2026) << 4 | writer`. Simpler, and identical everywhere
  for free. But they aren't dense, they fit 16 writer ids at most (vlpds claims a writer byte per
  node incarnation), and they pass 2^53 in about 18 years. Every time-based assumption in the
  vlpds firehose would also need a format parameter.
- Millisecond keys (`unix_ms << 8 | writer`) allow one event per millisecond per writer. A node
  doing 30k events/s would push its clock ahead about 30 times faster than real time.
- A central counter (a lease holder assigning seqs) means coordination on every event, and a gap
  at failover.
- Mapping cursors through a table on the way in and out is this design without noticing that the
  mapping is a pure function of the bucket.

## Costs and limits

- Anchoring and old-cursor lookups read up to one checkpoint interval (10 s) of events more than
  they need. At 100k events/s that's up to 1M events per node start. Counting by segment headers
  would fix it, but needs a per-segment event count in the header.
- Checkpoint objects: 8,640 a day (one LIST per core per checkpoint, one PUT by whichever core
  gets there first), and ~26k live at 72 h retention. Each node lists them once at start.
- A bucket pruned before it ever had checkpoints (only possible for data from before seqs were
  dense) is numbered from its retained floor, with a warning.
