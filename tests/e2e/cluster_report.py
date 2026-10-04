#!/usr/bin/env python3
"""The cluster e2e's verdict (tests/e2e/cluster.sh): per relay stream the
checker's counts, the streams compared with each other seq by seq, steady
state latency, the pause each HA event caused, and how long the shard moves
took according to the nodes' logs. Exits 1 on any discrepancy."""
import datetime
import json
import os
import re
import sys

out = sys.argv[1]
names = os.environ.get("STREAMS", "core1 core2 core3 edge replica").split()
fail = False

events = []
if os.path.exists(f"{out}/events.txt"):
    for line in open(f"{out}/events.txt"):
        p = line.split()
        if len(p) == 3:
            events.append((p[0], p[1], int(p[2])))
t_load = int(open(f"{out}/t_load_ms").read()) if os.path.exists(f"{out}/t_load_ms") else 0
first_event = min((e[2] for e in events), default=None)

print("\nstream     missing  extra  reordered  dups  seq-regr  matched   p50 ms  p99 ms  max ms")
for n in names:
    try:
        r = json.load(open(f"{out}/report-{n}.json"))
    except (OSError, ValueError):
        print(f"{n:<10} no report")
        fail = True
        continue
    commit_extra = r["kinds"]["#commit"]["extra"] + r["kinds"]["#sync"]["extra"]
    matched = sum(k["matched"] for k in r["kinds"].values())
    bad = r["missing"] or commit_extra or r["out_of_order"] or r["duplicates"] or r["rev_regressions"] or r["seq_regressions"]
    fail |= bool(bad)
    l = r["latency_ms"]
    print(f"{n:<10} {r['missing']:>7} {r['extra']:>6} {r['out_of_order']:>10} {r['duplicates']:>5} {r['seq_regressions']:>9} "
          f"{matched:>8} {l['p50']:>8.1f} {l['p99']:>7.1f} {l['max']:>7.1f}")

# every stream carries the same events at the same seqs
seqs = {}
for n in names:
    try:
        rows = [l.split(" ", 2) for l in open(f"{out}/seqs-{n}.txt")]
    except OSError:
        continue
    seqs[n] = {int(s): (d, k.strip()) for s, d, k in rows if int(s) > 0}
print()
ref = "core1" if "core1" in seqs else next(iter(seqs), None)
for n in names:
    if n not in seqs or n == ref:
        continue
    a, b = seqs[ref], seqs[n]
    if not a or not b:
        print(f"identical {ref} vs {n}: empty stream")
        fail = True
        continue
    lo, hi = max(min(a), min(b)), min(max(a), max(b))
    ka = {s: v for s, v in a.items() if lo <= s <= hi}
    kb = {s: v for s, v in b.items() if lo <= s <= hi}
    only_a = len(ka.keys() - kb.keys())
    only_b = len(kb.keys() - ka.keys())
    differ = sum(1 for s in ka.keys() & kb.keys() if ka[s] != kb[s])
    ok = only_a == 0 and only_b == 0 and differ == 0
    fail |= not ok
    print(f"identical {ref} vs {n}: {len(ka)} events in the shared seq range, only in {ref} {only_a}, only in {n} {only_b}, different {differ}: {'OK' if ok else 'MISMATCH'}")


def lat_rows(n):
    try:
        return [tuple(map(float, l.split())) for l in open(f"{out}/lat-{n}.txt")]
    except OSError:
        return []


def pct(v, q):
    v = sorted(v)
    return v[min(len(v) - 1, int(q * len(v)))] if v else float("nan")


print("\nsteady state (load start + 5 s to the first HA event): upstream -> relay latency, ms")
steady_end = first_event or 10**15
for n in names:
    v = [l for t, l in lat_rows(n) if t_load + 5000 <= t < steady_end]
    print(f"  {n:<8} n={len(v):<6} p50 {pct(v, .5):7.1f}  p90 {pct(v, .9):7.1f}  p99 {pct(v, .99):7.1f}  max {max(v, default=float('nan')):7.1f}")

if events:
    print("\npauses: the worst upstream -> relay latency of events that reached each stream after the event (up to 15 s or the next event), ms")
    marks = [e for e in events if e[0] != "exited"]
    for k, (what, node, t) in enumerate(marks):
        end = min(t + 15000, marks[k + 1][2] if k + 1 < len(marks) else t + 15000)
        row = []
        for n in names:
            v = [l for tt, l in lat_rows(n) if t <= tt < end]
            row.append(f"{n} {max(v, default=float('nan')):7.1f}")
        print(f"  {what:<8} {node} @{(t - t_load) / 1000:5.1f}s   " + "  ".join(row))


def log_times(path, pat):
    ts = []
    try:
        for line in open(path, errors="replace"):
            line = re.sub(r"\x1b\[[0-9;]*m", "", line)
            if re.search(pat, line):
                m = re.match(r"(\d{4}-\d\d-\d\dT\d\d:\d\d:\d\d\.\d+)Z", line)
                if m:
                    d = datetime.datetime.fromisoformat(m.group(1)[:26]).replace(tzinfo=datetime.timezone.utc)
                    ts.append(int(d.timestamp() * 1000))
    except OSError:
        pass
    return ts


if events:
    print("\nshard moves, from the nodes' logs (ms after the event)")
    for what, node, t in events:
        if what not in ("kill9", "term", "restart"):
            continue
        for kind, pat in (("DID shards opened", "opened DID shard"), ("host shards taken", "(acquired|adopted) host shards")):
            hits = []
            for i in range(1, 4):
                hits += [x - t for x in log_times(f"{out}/n{i}.log", pat) if t <= x < t + 30000]
            if hits:
                print(f"  {what:<8} {node}: {kind}: first {min(hits)} ms, last {max(hits)} ms ({len(hits)} log lines)")
        if what == "term":
            ex = [e[2] for e in events if e[0] == "exited" and e[1] == node]
            if ex:
                print(f"  term     {node}: process exited {ex[0] - t} ms after SIGTERM")

print("\nverdict:", "FAIL" if fail else "PASS")
sys.exit(1 if fail else 0)
