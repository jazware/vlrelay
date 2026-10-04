#!/usr/bin/env python3
"""The chaos harness's verdict (tests/chaos/chaos.sh OUT SCENARIO): the
invariants after a scenario, the pause and recovery after every fault, node
exits, zombie behaviour, and resource growth. Exits 1 on a broken invariant.

Invariants: every stream 0 missing / extra commits / duplicates / reordered /
seq regressions (the checkers), every stream the same events at the same
seqs (cluster_report.py), and no acked-but-lost event: nothing missing whose
upstream seq is at or below the host's checkpointed cursor in the bucket.
"""
import datetime
import json
import os
import re
import sys
from collections import defaultdict

out, scenario = sys.argv[1], sys.argv[2]
names = ["core1", "core2", "core3", "edge", "replica"]
fail = []


def load(p):
    try:
        return json.load(open(p))
    except (OSError, ValueError):
        return None


print(f"\n== chaos {scenario}: invariants")
print("stream     missing  extra-commits  dups  reordered  seq-regr  matched  up-replays  up-restarts")
reports = {}
for n in names:
    r = load(f"{out}/report-{n}.json")
    if r is None:
        print(f"{n:<10} no report")
        fail.append(f"{n}: no report")
        continue
    reports[n] = r
    ce = r["kinds"]["#commit"]["extra"] + r["kinds"]["#sync"]["extra"]
    matched = sum(k["matched"] for k in r["kinds"].values())
    print(f"{n:<10} {r['missing']:>7} {ce:>14} {r['duplicates']:>5} {r['out_of_order']:>10} {r['seq_regressions']:>9} {matched:>8} "
          f"{r.get('upstream_replays', 0):>11} {r.get('upstream_restarts', 0):>12}")
    for what, v in (("missing", r["missing"]), ("extra commits", ce), ("duplicates", r["duplicates"]),
                    ("reordered", r["out_of_order"]), ("seq regressions", r["seq_regressions"]), ("rev regressions", r["rev_regressions"])):
        if v:
            fail.append(f"{n}: {v} {what}")

# every stream the same events at the same seqs. cluster_report.py compares
# the checkers' keys, whose #identity/#account occurrence counts depend on
# where each socket started; here those counts are dropped.
seqs = {}
for n in names:
    try:
        rows = (l.split(" ", 2) for l in open(f"{out}/seqs-{n}.txt"))
        seqs[n] = {int(s): (d, re.sub(r"#\d+$", "", k.strip())) for s, d, k in rows if int(s) > 0}
    except OSError:
        pass
ref = "core1" if "core1" in seqs else next(iter(seqs), None)
for n in names:
    if n not in seqs or n == ref:
        continue
    a, b = seqs[ref], seqs[n]
    if not a or not b:
        fail.append(f"identical {ref} vs {n}: empty stream")
        continue
    lo, hi = max(min(a), min(b)), min(max(a), max(b))
    ka = {s: v for s, v in a.items() if lo <= s <= hi}
    kb = {s: v for s, v in b.items() if lo <= s <= hi}
    only_a, only_b = len(ka.keys() - kb.keys()), len(kb.keys() - ka.keys())
    differ = sum(1 for s in ka.keys() & kb.keys() if ka[s] != kb[s])
    ok = only_a == 0 and only_b == 0 and differ == 0
    line = (f"identical {ref} vs {n}: {len(ka)} events in the shared seq range, only in {ref} {only_a}, "
            f"only in {n} {only_b}, different {differ}: {'OK' if ok else 'MISMATCH'}")
    print(line)
    if not ok:
        fail.append(line)

# acked-but-lost: a missing event the host's checkpoint had already passed
ck = {}
for line in open(f"{out}/hostck.jsonl") if os.path.exists(f"{out}/hostck.jsonl") else []:
    line = line.strip()
    if not line.startswith("{"):
        continue
    try:
        for h, s in json.loads(line).get("cursors", {}).items():
            ck[h] = max(ck.get(h, -1), s)
    except ValueError:
        pass


def hostname(url):
    return re.sub(r"^[a-z]+://", "", url).rstrip("/")


lost = 0
for n, r in reports.items():
    for m in r.get("missing_events", []):
        c = ck.get(hostname(m["upstream"]))
        if c is not None and m["seq"] <= c:
            lost += 1
            if lost <= 5:
                print(f"  acked but lost on {n}: {m['upstream']} seq {m['seq']} (checkpoint {c}) {m['did']} {m['key']}")
print(f"acked-but-lost: {lost} (host checkpoints read: {len(ck)})")
if lost:
    fail.append(f"{lost} acked-but-lost")

# ---- faults: pause and recovery per stream
t_load = int(open(f"{out}/t_load_ms").read())
marks = []
for line in open(f"{out}/events.txt") if os.path.exists(f"{out}/events.txt") else []:
    p = line.split()
    if len(p) == 3:
        marks.append((p[0], p[1], int(p[2])))
lat = {}
for n in names:
    try:
        lat[n] = sorted(tuple(map(float, l.split())) for l in open(f"{out}/lat-{n}.txt"))
    except OSError:
        lat[n] = []

if marks:
    print("\n== faults: worst upstream->relay latency after each mark (until the next, max 30 s), and recovered = last event over 1 s, ms after the mark")
    for k, (what, node, t) in enumerate(marks):
        end = min(t + 30000, marks[k + 1][2] if k + 1 < len(marks) else t + 30000)
        row = []
        for n in names:
            v = [(tt, l) for tt, l in lat[n] if t <= tt < end]
            worst = max((l for _, l in v), default=float("nan"))
            slow = [tt for tt, l in v if l > 1000]
            rec = (max(slow) - t) if slow else 0
            row.append(f"{n} {worst:6.0f}/{rec:5.0f}")
        print(f"  {(t - t_load) / 1000:6.1f}s {what:<15} {node:<11} " + "  ".join(row))

# ---- node exits the supervisor saw, and what preceded them
exits = []
for line in open(f"{out}/exits.txt") if os.path.exists(f"{out}/exits.txt") else []:
    p = line.split()
    if len(p) == 4:
        exits.append((p[1], int(p[2]), int(p[3])))
if exits:
    print("\n== node exits (the supervisor restarted each)")
    for node, t, rc in exits:
        prior = [m for m in marks if m[2] <= t and (m[1] == node or m[1] == "all" or m[0].startswith("minio"))]
        why = f"{prior[-1][0]} {prior[-1][1]} {t - prior[-1][2]} ms before" if prior else "no fault before it"
        print(f"  {(t - t_load) / 1000:6.1f}s {node} rc {rc}: after {why}")

# ---- zombies: what a SIGSTOPped node did once it woke
for what, node, t in marks:
    if what != "sigcont":
        continue
    stop = max((m[2] for m in marks if m[0] == "sigstop" and m[1] == node and m[2] <= t), default=t)
    ex = [e for e in exits if e[0] == node and e[1] >= t]
    gone = f"exited {ex[0][1] - t} ms after SIGCONT (rc {ex[0][2]})" if ex else "kept running"
    print(f"  zombie {node}: stopped {(t - stop) / 1000:.1f} s, {gone}")

# ---- resources
res = defaultdict(list)
for line in open(f"{out}/resources.txt") if os.path.exists(f"{out}/resources.txt") else []:
    p = line.split()
    if len(p) == 5:
        res[p[1]].append((int(p[0]), int(p[2]), int(p[3]), int(p[4])))
if res:
    print("\n== resources per node (RSS MB first/max/last of the last process, fds first/last)")
    for node in sorted(res):
        v = res[node]
        last_pid = v[-1][1]
        v = [x for x in v if x[1] == last_pid]
        print(f"  {node}: samples {len(v)}, RSS {v[0][2] / 1024:.0f}/{max(x[2] for x in v) / 1024:.0f}/{v[-1][2] / 1024:.0f} MB, fds {v[0][3]}/{v[-1][3]}")
objs = defaultdict(dict)
for line in open(f"{out}/objects.txt") if os.path.exists(f"{out}/objects.txt") else []:
    p = line.split()
    if len(p) == 3:
        objs[int(p[0])][p[1]] = int(p[2])
if objs:
    ts = sorted(objs)
    first, last = objs[ts[0]], objs[ts[-1]]
    keys = sorted(set(first) | set(last), key=lambda k: (k.startswith("log/"), k))
    agg = lambda d: sum(v for k, v in d.items() if k.startswith("log/"))
    print(f"\n== bucket objects, first sample -> last ({(ts[-1] - ts[0]) / 60000:.0f} min apart)")
    print("  " + ", ".join(f"{k} {first.get(k, 0)}->{last.get(k, 0)}" for k in keys if not k.startswith("log/")))
    print(f"  log/ (all logs) {agg(first)}->{agg(last)} across {sum(1 for k in last if k.startswith('log/'))} logs")

print(f"\nverdict {scenario}:", "FAIL" if fail else "PASS")
for f in fail[:20]:
    print("  -", f)
sys.exit(1 if fail else 0)
