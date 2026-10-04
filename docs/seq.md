# vlRelay: stream seqs

Consumers see seqs 1, 2, 3, … like indigo's relay. Internally, everything still runs on vlpds's merge key.

## Why

The merge key is `unix_micros << 8 | writer`, about 4.6e17, which is above 2^53. `@atproto/sync` decodes it as a BigInt and fails lexicon validation on every frame (`Expected integer value type`). Any JSON or JavaScript consumer that doesn't validate loses precision and resumes from the wrong cursor. The atproto spec and indigo both expect a dense integer that a double can hold.

vlpds's own PDS firehose is unchanged. It's in production and bsky.network stores cursors against it, so changing its seqs is a separate decision. The renumbering is opt-in (`Firehose::set_renumber`), and only vlRelay turns it on.

## What a seq is

The seq of an event is its position in the merged stream: the number of events whose merge key is at or below its own, counted from the first event the bucket ever held.

This is a pure function of what's in the bucket. Every node merges the same durable log entries in the same key order (the merger's existing invariant, which the cluster e2e checks byte for byte). So every core node, edge and replica assigns the same seq to the same event with no coordination at all. The seq only goes up and is never reused across takeovers and restarts, because the merged order never changes after the fact: an event at or below the merger's emitted bound is never added later.

| Requirement | How |
|---|---|
| below 2^53, dense | a counter: 1e5 events/s for 2,800 years |
| identical everywhere, late starters included | a function of the bucket's contents, not of any node's history |
| monotonic, no reuse | merged order is append-only |
| cheap | no per-event coordination; one empty PUT per checkpoint interval per core node |

## Recovering the count: checkpoints

A node can't count from the first event every time it starts, because retention deletes old segments. So nodes write checkpoints:

- `seqck/{key:020}-{seq:020}` is an empty object. Its name says that exactly `seq` events have a merge key at or below `key`.
- The keys are fixed boundaries: `(k * every) << 8 | 0xff`, with `every` = 10 s by default. Every node computes the same pair for the same boundary as its merger passes it (`Renumber::emitted`). A core node lists the boundary first. If it's missing, the node PUTs it. If it's there, the node checks it's the same pair: a different one would mean two nodes numbered the stream differently, and that's logged as an error. A node whose own log has been fenced is a zombie, since a successor took over and its merge no longer sees every log, so it writes nothing. Edges and replicas never write (they have read-only credentials). They still keep their own pairs in memory.
- Encoding the pair in the name means a LIST returns everything, with no GETs. Each node keeps the pairs it has listed or computed in memory. It lists only past the newest key it knows, and only when it needs to.

Retention (`seq::prune`) cuts at a checkpoint: the newest one at or below the window's edge. It deletes only segments wholly below that checkpoint, then the checkpoints below the retained floor. So there's always a checkpoint at or above the oldest event still in the bucket. A cursor older than that checkpoint gets `OutdatedCursor` and resumes from it. The few events kept below it (less than one checkpoint interval) aren't served.

## Where the seq is spliced in

Frames are still logged with the merge key in `seq`, as before. The merger sorts each 2 ms batch by key as before. Then, if renumbering is on, it assigns the next seqs and splices each one into its frame while it builds the batch's shared wire buffer (`MergedBatch::renumbered`). So each event is spliced once per node, and every subscriber writes the same pre-framed bytes. The ring, `last_emitted`, `ConnStats::last_seq` and cursors are all in seqs. Each batch also keeps its events' keys (`MergedBatch::key`) for time-to-firehose, lag and the backfill floor.

**Anchoring.** A node's merged stream starts at a floor F (the clock at startup, as before). Before it emits anything, the merger waits until every followed log is durable up to F, which is the same `settled` condition backfill already uses. Then it asks `anchor(F)`: the newest checkpoint (K, N) with K ≤ F, plus a count of the events in (K, F] read from the bucket with the ordinary backfill reader. That's at most one checkpoint interval of events. Subscribers wait until the stream is anchored, which takes a few hundred milliseconds at startup.

**Old cursors.** A cursor below the ring's floor is located with `locate(cursor)`: the newest pair (K, N) with N ≤ cursor and K at or above the retained floor. Backfill reads the bucket from K, numbers events N+1, N+2, … and skips the ones at or below the cursor. When the backfill reaches the ring floor, its count has to land exactly on the ring floor's seq. If it doesn't, that's logged as a numbering error.

## Cursor semantics

- **No cursor:** live from the head, as before.
- **Cursor in the ring:** served from memory.
- **Older cursor:** backfilled from the bucket, as above.
- **Cursor older than retention:** if the newest usable pair has a seq above the cursor, the events in between were pruned. The consumer gets `#info OutdatedCursor` and continues from that checkpoint.
- **Cursor past the head:** a time-based seq could be compared with the clock, but a count can't. A node that trails another (an edge about 300 ms behind the cores, or a node that just restarted) would otherwise answer `FutureCursor` to a consumer that failed over with a perfectly good cursor. So the stream waits up to 2 s for its head to reach the cursor, and only then sends `FutureCursor`.

## Alternatives considered

- **Compact time keys**, e.g. `(µs since 2026) << 4 | writer`. This is simpler, and it's identical everywhere for free. But it isn't dense, it fits 16 writer ids at most (vlpds claims a writer byte per node incarnation), and it overflows 2^53 in about 18 years (35 years with 8 writers). Every time-based assumption in the vlpds firehose would also have to take a format parameter.
- **Millisecond keys** (`unix_ms << 8 | writer`): one event per millisecond per writer. A node doing 30k events/s would push its clock ahead about 30 times faster than real time.
- **A central counter** (a lease holder assigning seqs): coordination on every event, and a failover gap.
- **Mapping cursors on the way in and out through a table:** that's this design without the observation that the mapping is a pure function of the bucket.

## Costs and limits

- Anchoring and old-cursor lookups read up to one checkpoint interval (10 s) of events more than strictly needed. At 100k events/s that's up to 1M events per node start. The fix, if it matters, is to count by segment headers, which would need a per-segment event count in the header.
- Checkpoint objects: 8,640 a day (one LIST per core per checkpoint, one PUT by whichever core gets there first). 26k live objects at 72 h retention, and each node lists them once at start.
- A bucket pruned before it ever had checkpoints (only possible for data from before this change) is numbered from its retained floor, with a warning.
