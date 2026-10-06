#!/usr/bin/env python3
"""Summarizes a tests/qlog/chaos.sh run: the checker's verdict, latencies,
the emission pause after each fault, and CPU and RSS per node.

    report.py OUT_DIR
"""
import json
import os
import sys

out = sys.argv[1]


def load(name, default=None):
    try:
        with open(os.path.join(out, name)) as f:
            return json.load(f)
    except (OSError, ValueError):
        return default


def q(d):
    if not d:
        return "-"
    return f"p50 {d['p50'] / 1000:.2f} ms, p90 {d['p90'] / 1000:.2f}, p99 {d['p99'] / 1000:.2f}, max {d['max'] / 1000:.1f} (n={d['count']})"


check = load("check.json", {})
r = check.get("report", {})
print(f"verdict: {check.get('verdict', 'NO REPORT')}")
print(
    f"  observed {r.get('observed')} events, {r.get('distinct_seqs')} distinct seqs up to {r.get('max_seq')}; "
    f"acked {r.get('acked')}, acked but not emitted {r.get('acked_missing')}; violations {r.get('violations')}, "
    f"holes {r.get('holes')}, skipped with notice {r.get('skipped_with_notice')}"
)
for m in r.get("messages", [])[:10]:
    print(f"  ! {m}")

ld = load("load.json", {})
if ld:
    print(f"load: {ld['rate']:.0f}/s asked, {ld['acked_per_sec']:.0f}/s acked over {ld['seconds']:.0f} s, frames ~{ld['pad'] + 90} B, retries {ld['retries']}")
    print(f"  submit -> quorum ack (client view): {q(ld['ack_us'])}")
print(f"  submit -> first consumer (any node): {q(check.get('e2e_first_us'))}")
for n, h in sorted(check.get("e2e_by_node_us", {}).items()):
    print(f"  submit -> consumer of {n}: {q(h)}")
for i in (1, 2, 3):
    s = load(f"status-n{i}.json", {})
    if s.get("role") == "leader":
        print(f"  leader n{i} append -> quorum commit (since it took over): {q(s.get('commit_us'))}")
    if s:
        print(
            f"  n{i}: {s.get('role')} epoch {s.get('epoch')} commit {s.get('commit')} takeovers {s.get('takeovers')} "
            f"step_downs {s.get('step_downs')} resets {s.get('resets')} emit_gaps {s.get('emit_gaps')} promise_rounds {s.get('promise_rounds')}"
        )

pauses = []
try:
    with open(os.path.join(out, "pauses.jsonl")) as f:
        pauses = [json.loads(l) for l in f if l.strip()]
except OSError:
    pass
faults = []
try:
    with open(os.path.join(out, "events.log")) as f:
        for l in f:
            p = l.split()
            if len(p) >= 3 and p[1] in ("kill9", "isolate", "stop"):
                faults.append((int(p[0]), p[1], p[2]))
except OSError:
    pass
if faults:
    print("faults (emission pause = fault -> the next new seq at any consumer):")
    got = []
    for t, kind, who in faults:
        # the longest quiet stretch covering the fault or starting within
        # 1.5 s of it (detection takes up to ~1 s, and a 15 ms gap threshold
        # also catches ordinary lulls in the load)
        hit = sorted((p for p in pauses if t - 50 <= p["from_ms"] <= t + 1500 or p["from_ms"] <= t <= p["to_ms"]), key=lambda p: -p["ms"])
        if hit:
            d = hit[0]["ms"]
            got.append((kind, d))
            print(f"  {kind} {who}: {d} ms (stream quiet {hit[0]['ms']} ms from {hit[0]['from_ms'] - t:+d} ms)")
        else:
            got.append((kind, 0))
            print(f"  {kind} {who}: no pause over the gap threshold (--gap-ms)")
    for kind in sorted({k for k, _ in got}):
        ds = sorted(d for k, d in got if k == kind)
        print(f"  {kind}: n={len(ds)} median {ds[len(ds) // 2]} ms, max {ds[-1]} ms")

try:
    rows = {}
    with open(os.path.join(out, "resources.log")) as f:
        for l in f:
            p = l.split()
            if len(p) == 4:
                rows.setdefault(p[1], []).append((int(p[0]), int(p[2]), int(p[3])))
    hz = os.sysconf("SC_CLK_TCK")
    for n, rs in sorted(rows.items()):
        best = None
        # the longest stretch of one process (ticks only grow within one)
        seg = [rs[0]]
        for a, b in zip(rs, rs[1:]):
            if b[1] < a[1]:
                seg = [b]
            else:
                seg.append(b)
            if len(seg) > 1 and (best is None or seg[-1][0] - seg[0][0] > best[0]):
                best = (seg[-1][0] - seg[0][0], (seg[-1][1] - seg[0][1]) / hz)
        rss = max(x[2] for x in rs) / 1024
        cores = best[1] / (best[0] / 1000) if best and best[0] > 0 else 0
        print(f"  {n}: {cores:.3f} cores, max RSS {rss:.0f} MB")
except OSError:
    pass
