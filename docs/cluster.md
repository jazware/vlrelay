---
title: Quorum cluster
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
  - { value: "~60 ms", label: emission pause when the leader dies, note: "kill -9 at 10x; ~1 s when it hangs or is cut off", tone: violet }
  - { value: "30 s", label: bucket flush, note: "`--qlog-flush-ms`; the log, the records and the host table at one seq", tone: amber }
  - { value: "2 s", label: a dead node's hosts move, note: "`--qlog-host-failover-ms`, then they resume from their cursors", tone: blue }
```

A quorum cluster is three nodes (or one, or five) that keep the recent log in each other instead
of the bucket. One node leads. Every node reads its share of the PDSes and verifies their events,
as a lone relay does, then hands each one to the leader. The leader checks it against the
account's record, gives it the next seq and replicates it, and the event reaches consumers on
every node once two of the three hold it. The bucket gets everything up to one seq every 30
seconds: the log as segments, the accounts' records, the host table and the PDS cursors, with one
manifest written last. A consumer can connect to any node and resume on another with its cursor.

It replaces the [lease cluster](cluster.md), which kept host bookkeeping and a log per node in the
bucket and wrote it every few milliseconds. That one now needs `--legacy-cluster`.

## Running one

```bash
vlrelay --quorum --node-id n1 --listen :2980 \
        --qlog-listen 10.0.0.1:2978 --qlog-peer n2=10.0.0.2:2978 --qlog-peer n3=10.0.0.3:2978 \
        --qlog-dir /var/lib/vlrelay/qlog --qlog-admin-token "$QLOG_TOKEN" \
        --s3-endpoint ... --prefix relay1 --host pds.example.com --crawl
```

| Flag | Default | What |
|---|---|---|
| `--quorum` | | Run on the quorum log. |
| `--node-id ID` | `relay` | The member's name. Unique per node. |
| `--qlog-listen ADDR` | `127.0.0.1:2978` | The peer port: replication, submits, members' questions. Keep it to the other nodes. |
| `--qlog-peer ID=ADDR` | | Where to reach each other node (repeatable). |
| `--qlog-members a,b,c` | this node and its peers | The first member set. After the first start, `qlog/leader` in the bucket holds it. |
| `--qlog-dir DIR` | | The commitlog, on a local NVMe disk. Without it the log is in memory only. |
| `--qlog-flush-ms MS` | 30000 | How often the leader flushes to the bucket. |
| `--qlog-admin-token T` | | What a membership change must carry (`QLOG_ADMIN_TOKEN`). |
| `--qlog-host-failover-ms MS` | 2000 | A member the leader hasn't heard from in this long loses its PDSes to the others. |
| `--qlog-retain-hours H` | 72 | The leader deletes log segments older than this (0: never). |

Every node points at the same bucket and `--prefix`. `--host` and `--crawl` work on any node: a
PDS admitted anywhere goes into the leader's host table, and the leader gives it to a node.

## The path of an event

1. A node reads a PDS the leader gave it, parses the event and checks its signature and its MST
   proof, as a lone relay does. Verifying is most of the CPU, so it stays spread out.
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
| The leader's process dies | another node takes over in 50-120 ms (kill -9 at 10x); nothing committed is lost |
| The leader hangs or is cut off | the others take over after 1 s of silence |
| A follower dies | nothing for consumers; its PDSes move to the others after 2 s and resume from their cursors |
| Two nodes' disks are lost | the log resumes from the bucket's last flush, with a jump in the seqs; the PDSes send the rest again |

The new leader opens the account records at the last flush and replays its own log from there
before it takes events, which takes tens of milliseconds. A node that comes back catches up from
the leader and serves its consumers from its own log again.

## Changing the members

`qlog member replace n3 n4`, or the dashboard's Quorum page, sends a change to the leader with the
admin token. The new node joins as a learner, copies the leader's log, and the switch happens at a
flush: commits pause for about 10 ms. A removed node stops getting events and its PDSes move.

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

## What isn't built

Archival mode, PLC export seeding, resharding (there's one set of records, on the leader), the
sync API's repo endpoints on followers, and edges and replicas (any member serves the stream).
