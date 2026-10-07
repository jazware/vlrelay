---
title: Chaos
section: Testing
order: 202
summary: "Three relays on the quorum log under kill -9, power cuts, partitions, wiped disks and membership changes, with every node's stream, the bucket and every upstream event checked after each run."
---

```hero
diagram:
  caption: "`just relay-chaos`. A fakepds fleet feeds three relays, each restarted by a supervisor when it dies. Their peer links go through a fault proxy, so a partition cuts a node off from its peers and leaves its bucket and upstreams alone. After the run, checkers read every node's stream, the bucket and every upstream."
  nodes:
    - { id: fleet, label: fakepds fleet, sub: "16 hosts · fake PLC", at: [0, 5], size: [9, 3], tone: muted, stack: true }
    - { id: n1, label: leader n1, sub: seq · append · flush, at: [14, 0], size: [9, 3], tone: violet }
    - { id: n2, label: node n2, sub: commitlog, at: [14, 5], size: [9, 3], tone: accent }
    - { id: n3, label: node n3, sub: commitlog, at: [14, 10], size: [9, 3], tone: accent }
    - { id: proxy, label: fault proxy, sub: one route per direction, at: [28, 5], size: [10, 3], tone: rust }
    - { id: minio, label: MinIO, sub: "manifest · state · segments", at: [42, 0.2], size: [10, 2.6], shape: store, tone: amber }
  groups:
    - { label: three vlrelay nodes, around: [n1, n2, n3], tone: accent }
  edges:
    - "fleet -> n1"
    - "fleet -> n2: ws"
    - "fleet -> n3"
    - { from: n1.r30, to: minio.l, label: flush, tone: amber }
    - { from: n1.r70, to: proxy.t, label: peers, arrow: both, via: [[33, 2.1]] }
    - "n2 <-> proxy"
    - { from: n3.r, to: proxy.b, arrow: both, via: [[33, 11.5]] }
facts:
  - { value: "24", unit: scenarios, label: on three relays, note: "kills, power cuts, partitions, wipes, membership", tone: accent }
  - { value: "0", label: violations or acked-but-lost, note: "every scenario, ~30M events at 3,500/s", tone: blue }
  - { value: "~75", unit: ms, label: pause when the leader is killed, note: "73-75 ms median, 137 ms max", tone: violet }
  - { value: "72", unit: runs, label: across three durability modes, note: "fsync · page-cache · memory, all passed", tone: amber }
```

We break the relay on purpose and check what survives. `tests/qlog/relay-chaos.sh`
(`just relay-chaos`) runs the relay on the [quorum log](quorum.md): a fakepds fleet feeding three
`vlrelay` nodes on a local MinIO, one fault schedule under steady load, then the same checks after
every scenario. `tests/qlog/chaos.sh` (`just qlog-chaos`) does the same for the bare quorum log
(`qlog node` and `qlog load`, no relay).

```
just relay-chaos list
just relay-chaos kill-leader                       # or: tests/qlog/relay-chaos.sh kill-leader [--rate 350] [--duration 90] [--every 15] [--hosts 16] [--dids 200]
RQ_BASE=3650 just relay-chaos mixed-durable        # another port block, beside a running one
CL_DIR=/dev/shm/rq FSYNC_DELAY_US=1000 just relay-chaos power-cut-all   # commitlogs on tmpfs with an emulated 1 ms fsync
RETAIN_SECS=120 just relay-chaos mixed-flush       # with the leader's bucket retention on
DURABILITY=memory just relay-chaos kill-all        # a durability mode other than the default
```

The harnesses run on Linux, since they read `/proc` and GNU `date`. They need Docker for MinIO
and `python3` for the fault proxy and the reports, and they build in the `dev-release` profile
(`QLOG_PROFILE`). The MinIO image is the dev network's (`vlpds-minio:local`), so run `just dev-up`
once first ([Development](devloop.md)).

## The setup

- Upstreams. A fakepds fleet (`--hosts` 16, `--dids` 200, `--rate` 350 events/s by default) with
  its own PLC: real signed sync 1.1 commits, `#identity`, `#account` and `#sync`, and a replay
  buffer so hosts can resend after a recovery. [Load fleet](loadfleet.md) has the details.
- Relays. Three `vlrelay` nodes (one for the `single-*` scenarios) with commitlogs (`--qlog-dir`,
  power cut on SIGUSR1), a 2 s flush (`FLUSH_MS`), the qlog admin token, and every fakepds host as
  `--host`. A supervisor loop restarts a node 1 s after it exits (`RESTART_SEC`), the way
  systemd's `Restart=always` would, and a scenario can hold a node down or retire it. Bucket
  retention is off unless `RETAIN_SECS` is set.
- Fault proxy (`tests/qlog/proxy.py`, one route per direction between every pair of nodes), so a
  partition can cut a node's peer links without touching its bucket or its upstreams. `PROXY=0`
  (and the single-node scenarios) skips it.
- Membership scenarios start spare slots (`SLOTS`, 9) so a replacement is a new id on an empty
  disk, and change members with `qlog member` and the admin token.
- Ports. Each run takes a block from `RQ_BASE` (3550 by default), its docker project is
  `vlrq-relay-$RQ_BASE` and its output lands in `dev/state-relayq-$RQ_BASE/<scenario>`, so two
  runs can go side by side.

The header of `tests/qlog/relay-chaos.sh` lists every env knob. Three of them turn on a leader
job under the same faults: `PLC_EXPORT=1` (the PLC export against fakepds's `/export`),
`DISCOVERY=1` (host discovery from fakepds's `listHosts`) and `FAKE_FAULT` (a fakepds `--fault`,
for the rejects views).

## What's checked

| Check | How |
|---|---|
| No seq emitted with two contents, dense streams | `qlog check --relay-frames` on every node's `subscribeRepos`, from cursor 0, across nodes and restarts. Every stream is dense except across a bucket recovery's gap, nothing acked is lost, and every member's consumer ends at the same commit. |
| Backfill | The same checker runs a consumer from cursor 0 through the bucket. |
| The bucket matches the log | `qlog verify` every `VERIFY_EVERY` (20) s and at the end: every manifest's state (the relay's DID records, the host table, cursors) equals the log replayed to F. |
| No upstream event missing | `e2e_check` on the fakepds hosts against the relay, failing over between nodes with its cursor: each DID's commits once and in rev order, `#identity` and `#account` too. Repeats after a recovery (the gap's events, sent again by their hosts) are counted, not failed. |
| Membership changes land | Any `switch-FAILED` in the events log fails the run. |

A run fails on any of them. `tests/qlog/relay_report.py` summarizes a run: the checker's verdict,
emission pauses after each fault, CPU and RSS, recoveries, bucket requests, `e2e_check`'s
upstream-against-relay counts and latency, and each node's admissions and host table.

The checks catch what they're meant to. With the power-loss check turned off
(`TRUST_LOG=1`, which runs the nodes with `--qlog-unsafe-trust-log`), nodes trust logs that lost
acked entries after a power cut and elect a leader from them. `power-cut-all` then failed with 257
seqs "emitted with two contents (emitted, lost and reissued)". (`power-cut-majority` under the same
mutation passed, because the third node's whole log usually wins the election, so it doesn't
reliably expose the bug.)

## Scenarios

| Scenario | Faults (every `--every` s, 15 by default) |
|---|---|
| `baseline` | none |
| `kill-leader`, `kill-follower` | kill -9 the leader, or a follower (the supervisor restarts it) |
| `down-follower`, `down-leader` | kill -9 and hold the node down `DOWN_SEC` (6) s, past `--qlog-host-failover-ms`, so its hosts move to the others with their cursors |
| `kill-two`, `kill-all` | kill -9 the leader and a follower at once, or all three |
| `power-cut-leader`, `power-cut-all`, `power-cut-majority` | SIGUSR1: the commitlog loses a random part of its unsynced tail plus a torn record, then the process dies. `power-cut-majority` cuts the leader and one follower at once. |
| `partition-leader`, `partition-follower` | cut the node's peer links for 5 s through the proxy, then heal |
| `pause-leader` | SIGSTOP the leader for 3 s |
| `mixed-durable` | a random pick of the kills, power cuts, partitions, the pause and `down-follower` |
| `flush-crash` | no scheduled faults. Nodes crash at random points inside a flush (`--qlog-crash-at any`, `CRASH_PROB` 0.05) |
| `mixed-flush` | `mixed-durable` plus the flush crashes |
| `wipe-all`, `wipe-two` | kill and delete the commitlogs of all three, or of two (the survivor is the leader or a follower at random): a lost quorum, resumed from the bucket's last flush with a seq jump |
| `mixed-wipe` | a random pick of kills, power cuts, wipes and a leader partition |
| `replace-follower`, `replace-leader` | `qlog member replace` onto a new node with an empty disk, then retire the old one |
| `grow-shrink` | alternately add a member, then remove members back down to three |
| `single-kill`, `single-wipe` | one node (no peers): kill -9 it, or kill it and delete its commitlog |

## Results

At 3,500 events/s (10x today's network), 90 s each, commitlogs on tmpfs with a 1 ms emulated
fsync, on the builds after the quorum log became the only mode. Every scenario had 0 violations, 0
holes, nothing acked lost and nothing missing from upstream to relay, over about 30M events
observed across the runs:

| Scenario | Faults | Emission pause or switch |
|---|---|---|
| kill-leader | kill -9 every 15 s | 73-75 ms median, 137 ms max (n=14) |
| power-cut-leader | SIGUSR1 power cut | 94-95 ms median, 155 ms max |
| pause-leader | SIGSTOP 3 s | 1.02 s median |
| partition-leader | isolated 5 s | 1.01 s median |
| kill-follower, down-follower | kill -9, down 6 s | 0 |
| kill-two, power-cut-all | quorum lost, then back | 1.8 s, 2.5 s median |
| wipe-two, wipe-all | commitlogs gone: bucket recovery | 2.4 s, 2.3 s median |
| single-kill, single-wipe | the one-member log | 1.4 s, 1.3 s median |
| replace-follower, replace-leader | 4 replacements each | 1.1-2.8 s command to done, longest pause 65 ms |
| grow-shrink | 3 -> 4 -> 3 -> 4 -> 3 | 0.7-2.2 s, longest pause 77 ms |
| flush-crash | kill -9 at random flush steps | |
| mixed-wipe, mixed-durable, mixed-flush, baseline | a random fault every 15 s | |

A killed leader costs ~75 ms because the followers notice its port refusing. A hung or cut-off
leader is caught by the 1 s election timeout, which is the ~1 s for `pause-leader` and
`partition-leader`.

The PLC export ran through the same scenarios (`PLC_EXPORT=1`, 150 s, kill-leader,
power-cut-leader, mixed-durable) against fakepds's `/export` with 200,000 ops and a 429 on every
sixth request, at 4 requests a second. In kill-leader the export changed hands eight times
mid-history, each new leader resuming from the checkpoint (21,979, 42,959 ... 187,820 ops read
before it), and the ninth caught up. Every run ended caught up, with the relay's checks unchanged.
Host discovery ran the same way (`DISCOVERY=1`, kill-leader and power-cut-all). fakepds's
`listHosts` served the 16 fleet hosts and 40 that don't answer, one a page, with a 429 every sixth
request. The list was read to the end both times (56 seen, 16 known, 40 refused, 10-11 429s
waited out), with the run taken over mid-list by a new leader 2 and 4 times, each resuming from
the saved cursor.

### By durability mode

`--durability` says when an entry counts on a node: after its fdatasync (`fsync`), once it's in
the page cache (`page-cache`, the default for three members), or in memory only (`memory`). The
full suite ran once per mode (`DURABILITY=fsync|page-cache|memory`, 24 scenarios each, 3,500/s,
90 s), and all 72 runs passed: 0 violations, nothing acked lost and nothing missing from upstream
to relay. That includes `power-cut-majority`, which was new for these runs.

What differs is which faults end in a bucket recovery:

- `kill-all` (every process killed at once) is a plain takeover in `fsync` and `page-cache`
  (1.9-2.3 s, the supervisor's restart plus an election) and a recovery in `memory`.
- `power-cut-all` and `power-cut-majority` are recoveries in `page-cache` (2.7 s and 2.1 s median)
  and `memory` (2.3 and 1.6 s), with their gaps recorded and every event re-ingested. In `fsync`
  they're takeovers.

The kill -9 pause differs by mode too, and the takeover isn't the reason. From the port refusing
to the new leader leading takes 2-4 ms in every mode. What changes is how long the killed process
takes to release its port, which is the follower's cue to take over: 40-56 ms in `fsync`, 72-85 in
`page-cache` and 113-139 in `memory`. That grows with what the process holds in memory (a `memory`
node keeps 512 MiB of log), so the pause medians are 73, ~140 and ~200 ms.

### What the runs found

The first passes found four relay bugs, each covered by a test now:

- Two sockets on one host interleaved a DID's events at the leader, which caused `prevData`
  mismatches and then desynchronized the account. It happened when a node that had just started
  subscribed to every host before its filter landed, and when a moved host's old owner kept reading
  until it polled. The leader now admits a host's events only from the member its host table names,
  and a node reads nothing until the leader gives it hosts.
- A record was released to the cache after it left the pending set, so a load between the two
  found neither and read the previous record. The release now caches before it lets go.
- A batch slow on identity lookups timed out and was resent. The copy's event was answered as a
  duplicate of the still-unappended original, the next batch's event appended first, and the
  original landed after it: a rev out of order in the stream. The leader now locks every DID of a
  batch from deciding until it's appended.
- A node wiped while the cluster was past a recovery gap served live consumers from its first
  emission and skipped the seqs between the bucket's F and that emission. A node's firehose now
  starts at the first entry it emits, and `wipe-two` covers it.

They also found three harness bugs. A mid-run `verify` that read the manifest just before a
recovery's landed flagged the recovery's segments, and `verify` now stops there. The proxy's routes
overlapped fakepds's ports with nine slots, so `replace-*` and `grow-shrink` never elected. And
`flush-crash` could crash a node in the run's last second, which left its hosts' tail unread when
the fleet stopped (142 commits "missing"). Crashes now stop with the other faults, 12 s before the
end.

## Earlier: the lease cluster

Before the quorum log, vlRelay ran as a lease cluster: each core owned DID shards through leases in
the bucket and wrote its own log, and every core merged all the logs into one stream. That design
and its chaos harness are deleted. Its runs found a few things that still shape the relay:

- A duplicate must wait for the first copy to be durable. Answering it as soon as the first copy
  was applied lost events whenever that append then failed: they were acked upstream, and the
  account's next commit failed `prevData` and desynchronized it. On the quorum log, the leader
  answers a duplicate of an entry that's still uncommitted only once that entry commits.
- Catch-up needs backpressure. A 65-minute soak at ~700 events/s with a fault every 4 minutes held
  up for 24 minutes, then fell into a death spiral: replays piled up in flight, cores hit their
  memory cap, and every restart made it worse. That's where the in-flight caps came from: a host
  with 8,192 frames or 64 MiB in flight, or any host while the node holds 32,768 or 384 MiB, stops
  being read (`--host-inflight-events`, `--host-inflight-mb`, `--inflight-events`,
  `--inflight-mb`).
- A host's replay window bounds what a long outage can recover. The losses that remained in the
  later soaks came from hosts whose window ran out while the relay couldn't keep up. With
  fakepds's 64 MB window that's about a minute of trouble at that rate, where a production PDS
  keeps days. The relay counts and logs an upstream `OutdatedCursor` and takes what comes, so the
  accounts with gaps desynchronize on their next commit and wait for a `#sync`.
- A host whose sequence restarted needs a replay from 0 ([below](#futurecursor-replay-from-0)).

And a few lessons about the harness itself:

- The fault proxy accepts a connection before it dials the target, so a dead node's peer port looks
  like a reset instead of a refusal. A load balancer or service mesh in front of the peer port
  would do the same.
- A consumer socket to a SIGSTOPped node just hangs (no read timeout in `e2e_check`, nor in most
  real consumers).
- MinIO keeps its data in tmpfs, which counts against its container's memory cap. At ~1k events/s
  a 1 GB cap was OOM-killed after ~13 minutes, so long soaks belong on a docker volume.
- The fault proxy ran out of file descriptors within minutes under Linux's default limit of 1024,
  which voided one soak. Raise the limit for long runs.

### FutureCursor: replay from 0

A host answers our cursor with `FutureCursor` when its sequence restarted below it: a PDS whose
sequencer was wiped, or one restored from a backup. indigo marks the host idle and stops
([Reference notes](reference-notes.md)). Resuming live skips whatever the host emitted between its
restart and our reconnect, and every account touched then desynchronizes on its next commit. The
lease cluster's `upstream-restart` scenario lost 4,604 events to exactly that.

So the relay resets the host's cursor to 0 and reconnects from there, and the host replays its new
sequence from its first event (`src/upstream/client.rs`):

- Wiped sequencer, same repos: the replay is exactly the window we missed. Commits apply in order.
- Restored from backup: the replay goes back over history we already have. Commits are dropped by
  rev. `#identity`, `#account` and `#sync` have no rev, so the leader catches a second copy by
  (host, upstream seq, DID) until the host's cursor passes it. Copies past that are emitted again.
  They restate state, so a consumer sees a repeat and never a wrong state.
- The cost is one full replay of the host's window, which the in-flight caps bound in memory.

The alternative, marking the affected accounts for resync, needs to know which accounts the gap
touched, which we can't know without the missing events. The relay would then wait for each
account's next `#sync`, which may never come. Replaying from 0 loses nothing the host still has.
When the reconnect comes long after the restart, the host's window may no longer reach back to it
and answers `OutdatedCursor` at cursor 0, and those accounts desynchronize. A `FutureCursor` in
answer to cursor 0 is a broken host, and the relay backs off.
