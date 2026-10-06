#!/usr/bin/env python3
"""Summarizes a tests/qlog/chaos.sh run: the checker's verdict, latencies,
the emission pause after each fault, and CPU and RSS per node.

    report.py OUT_DIR
"""
import json
import os
import sys

out = sys.argv[1]
# every node id the run started (membership runs add new ones)
slots = sorted({int(f[1:].split(".")[0]) for f in os.listdir(out) if f.startswith("n") and f.endswith(".log") and f[1:-4].isdigit()})


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
if r.get("jumped") or r.get("duplicates") or r.get("reingested"):
    print(
        f"  recovery gaps: streams jumped {r.get('jumped')} seqs across them; acked seqs lost and re-ingested {r.get('reingested')}; "
        f"events at two seqs {r.get('duplicates')} ({r.get('duplicates_across_gaps')} across a gap); events never emitted outside a gap {r.get('events_lost')}"
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
for i in slots:
    s = load(f"status-n{i}.json", {})
    if s.get("role") == "leader":
        print(f"  leader n{i} append -> quorum commit (since it took over): {q(s.get('commit_us'))}")
    if s:
        print(
            f"  n{i}: {s.get('role')} epoch {s.get('epoch')} commit {s.get('commit')} takeovers {s.get('takeovers')} "
            f"step_downs {s.get('step_downs')} resets {s.get('resets')} emit_gaps {s.get('emit_gaps')} promise_rounds {s.get('promise_rounds')} disk_reads {s.get('disk_reads')}"
        )
        d = s.get("disk")
        if d:
            print(
                f"  n{i} commitlog: {d['fsyncs']} fsyncs, fsync {q(d['fsync_us'])}; ops per group commit p50 {d['batch_ops']['p50']} p99 {d['batch_ops']['p99']}; "
                f"{d['bytes_written'] / 2**20:.0f} MiB written, {d['disk_bytes'] / 2**20:.0f} MiB on disk, {d['rollovers']} rollovers, {d['deleted']} deleted"
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
            if len(p) >= 3 and p[1] in ("kill9", "isolate", "stop", "powercut"):
                multi = ("kill-two", "kill-all", "power-cut-all", "wipe-all", "wipe-two", "single-wipe", "single-kill", "single-power-cut", "kill-learner", "isolate-leader", "kill-follower", "kill-leader")
                kind = p[3] if len(p) > 3 and p[3] in multi else p[1]
                # one fault on several nodes at once is one fault
                if faults and faults[-1][1] == kind and int(p[0]) - faults[-1][0] < 200:
                    faults[-1] = (faults[-1][0], kind, faults[-1][2] + "+" + p[2])
                else:
                    faults.append((int(p[0]), kind, p[2]))
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

# the flush: per leader, what a flush took and sent; the final manifest
for i in slots:
    s = load(f"status-n{i}.json", {})
    fl = s.get("flush") if s else None
    if not fl or not fl.get("flushes"):
        continue
    n = fl["flushes"]
    reqs = ", ".join(f"{op} {c / n:.1f}" for op, c in sorted(fl.get("requests", {}).items()))
    print(
        f"  n{i} flush: {n} flushes ({fl['aborted']} retried, {fl['failed']} failed, {fl['adopted']} segments adopted, {fl['fences']} fences); "
        f"took {q(fl['duration_us'])}; applier paused {q(fl['seal_us'])}"
    )
    print(
        f"  n{i} per flush: {fl['entries'] / n:.0f} entries, {fl['segments'] / n:.2f} segments, {fl['raw_bytes'] / n / 2**20:.2f} MiB raw, "
        f"{fl['segment_bytes'] / n / 2**20:.3f} MiB stored; requests per flush: {reqs}"
    )
for i in slots:
    s = load(f"status-n{i}.json", {})
    fl = (s or {}).get("flush") or {}
    tot = fl.get("requests_total")
    if tot and fl.get("flushes"):
        secs = (ld or {}).get("seconds") or 1
        print(f"  n{i} all object-store requests, per second of load: " + ", ".join(f"{op} {c / secs:.2f}" for op, c in sorted(tot.items())))
v = load("verify.json")
if v is not None:
    print(
        f"manifest: {'consistent' if v.get('ok') else 'INCONSISTENT'}: epoch {v['epoch']} F {v['flushed']} R {v['reserve']}, "
        f"{v['segments']} segments ({v['orphans']} past it), {v['entries']} entries, {v['dids']} DIDs, {v['hosts']} host cursors"
    )
    for m in v.get("messages", [])[:10]:
        print(f"  ! {m}")
try:
    with open(os.path.join(out, "verify.jsonl")) as f:
        vs = [json.loads(l) for l in f if l.startswith("{")]
    print(f"  mid-run verifies: {len(vs)}, {sum(1 for x in vs if not x.get('ok'))} inconsistent")
except OSError:
    pass
if check.get("backfilled"):
    n, secs = check["backfilled"]
    print(f"consumer from cursor 0 at the end: {n} events in {secs:.1f} s, dense and matching")

# ack latency in the seconds a flush overlapped, against the others
try:
    import re

    flushes = []
    for i in slots:
        try:
            with open(os.path.join(out, f"n{i}.log")) as f:
                for l in f:
                    if "qlog flush: committed" in l:
                        e = int(re.search(r"end_ms=(\d+)", l).group(1))
                        ms = int(re.search(r" ms=(\d+)", l).group(1))
                        flushes.append((e - ms, e))
        except OSError:
            pass
    with open(os.path.join(out, "load.json.timeline.jsonl")) as f:
        tl = [json.loads(l) for l in f if l.strip()]
    if flushes and tl:
        def overl(t):
            return any(a - 1000 < t and t - 1000 < b for a, b in flushes)
        inn = [x for x in tl if x["n"] and overl(x["t_ms"])]
        outn = [x for x in tl if x["n"] and not overl(x["t_ms"])]
        def agg(xs):
            if not xs:
                return "-"
            p50 = sorted(x["p50"] for x in xs)[len(xs) // 2] / 1000
            p99 = max(x["p99"] for x in xs) / 1000
            mx = max(x["max"] for x in xs) / 1000
            return f"median per-second p50 {p50:.2f} ms, worst per-second p99 {p99:.2f} ms, max {mx:.1f} ms over {len(xs)} s"
        print(f"ack latency, seconds with a flush: {agg(inn)}")
        print(f"ack latency, seconds without: {agg(outn)}")
except (OSError, AttributeError, ValueError):
    pass

# bucket recoveries: what each node logged, and the load's re-ingest
try:
    import re

    recs = []
    for i in slots:
        try:
            with open(os.path.join(out, f"n{i}.log")) as f:
                for l in f:
                    if "qlog recovery: the bucket's log adopted" in l:
                        recs.append(dict(re.findall(r"(\w+)=(\d+)", l)))
        except OSError:
            pass
    for x in sorted(recs, key=lambda x: int(x.get("generation", 0))):
        print(
            f"recovery {x.get('generation')} (epoch {x.get('epoch')}): F {x.get('f')} -> kept to S {x.get('after')} "
            f"({x.get('orphans')} orphan segments, {x.get('salvaged')} salvaged), resumed above R {x.get('base')}; "
            f"read {x.get('read_ms')} ms, clone {x.get('clone_ms')} ms, apply+seal {x.get('apply_seal_ms')} ms, "
            f"segments {x.get('segments_ms')} ms, manifest {x.get('manifest_ms')} ms, total {x.get('ms')} ms"
        )
    for rw in (ld or {}).get("rewinds", []):
        print(
            f"  load rewind for recovery {rw['generation']}: cursors after {rw['cursors_ms']} ms, {rw['resent']} events sent again "
            f"({rw['acked_before']} of them acked before), all acked again in {rw['catch_up_ms']} ms"
        )
except (OSError, ValueError):
    pass
rt = load("retain.json")
if rt:
    pl = rt.get("plan", {})
    print(
        f"retain/qlog: {pl.get('segments')} segments ({pl.get('segment_bytes', 0) / 2**20:.1f} MiB); {len(pl.get('deletable', []))} deletable past "
        f"{pl.get('horizon_secs')} s ({pl.get('deletable_bytes', 0) / 2**20:.1f} MiB, pruned_seq -> {pl.get('pruned_seq_after')}); "
        f"{len(pl.get('stale_segments', []))} stale; state paths: "
        + ", ".join(f"{x['path']}{' (current)' if x['current'] else ''}{' (referenced)' if x['referenced'] else ''}{' deletable' if x['deletable'] else ''} {x['bytes'] / 2**20:.1f} MiB, {len(x['stale_checkpoints'])} stale checkpoints" for x in pl.get("states", []))
    )

# membership changes: each one's steps (the leader's view), the emission
# pause around it, ack latency in its seconds, and the removed-member checks
try:
    sw = []
    with open(os.path.join(out, "switches.jsonl")) as f:
        for l in f:
            if l.startswith("{"):
                x = json.loads(l)
                if "switch" in x:
                    sw.append(x["switch"])
    ev = []
    with open(os.path.join(out, "events.log")) as f:
        for l in f:
            p = l.split()
            if len(p) >= 2 and (p[1].startswith("switch-") or p[1] in ("removed-ok", "VIOLATION")):
                ev.append((int(p[0]), p[1], " ".join(p[2:])))
    if sw or ev:
        print(f"membership changes: {sum(1 for e in ev if e[1] == 'switch-done')} done, {sum(1 for e in ev if e[1] == 'switch-FAILED')} failed; "
              f"removed members checked {sum(1 for e in ev if e[1] == 'removed-ok')}, counted after removal {sum(1 for e in ev if e[1] == 'VIOLATION')}")
    windows = []
    start = None
    for t, k, rest in ev:
        if k == "switch-start":
            start = (t, rest)
        elif k in ("switch-done", "switch-FAILED") and start:
            windows.append((start[0], t, start[1], k))
            start = None
    for a, b, what, k in windows:
        hit = [p for p in pauses if p["from_ms"] <= b and p["to_ms"] >= a]
        worst = max((p["ms"] for p in hit), default=0)
        print(f"  {what}: {b - a} ms command to done ({k}); longest emission pause in it {worst} ms")
    for x in sw:
        if x.get("epoch") == x.get("from_epoch"):
            continue
        print(
            f"  switch {x['from']} -> {x['to']} at epoch {x['epoch']} (leader {x['leader']}): learners caught up in {x['catch_up_ms']} ms, "
            f"pre-flush {x['pre_flush_ms']} ms; paused {x['paused_ms']} ms = drain {x['drain_ms']} + flush {x['flush_ms']} + CAS {x['cas_ms']} (+ moving over); barrier at {x['flushed']}"
        )
    real = [x for x in sw if x.get("epoch") != x.get("from_epoch")]
    if real:
        def med(k):
            v = sorted(x[k] for x in real)
            return f"median {v[len(v) // 2]}, max {v[-1]}"
        print(f"  over {len(real)} switches: catch-up ms {med('catch_up_ms')}; paused ms {med('paused_ms')}; drain {med('drain_ms')}; flush {med('flush_ms')}; CAS {med('cas_ms')}")
    tl = []
    try:
        with open(os.path.join(out, "load.json.timeline.jsonl")) as f:
            tl = [json.loads(l) for l in f if l.strip()]
    except OSError:
        pass
    if windows and tl:
        def inw(t):
            return any(a - 1000 < t and t - 1000 < b for a, b, _, _ in windows)
        def agg(xs):
            if not xs:
                return "-"
            p50 = sorted(x["p50"] for x in xs)[len(xs) // 2] / 1000
            p99 = sorted(x["p99"] for x in xs)[len(xs) // 2] / 1000
            w99 = max(x["p99"] for x in xs) / 1000
            return f"median per-second p50 {p50:.2f} ms, median p99 {p99:.2f} ms, worst p99 {w99:.2f} ms over {len(xs)} s"
        print(f"  ack latency, seconds with a change running: {agg([x for x in tl if x['n'] and inw(x['t_ms'])])}")
        print(f"  ack latency, other seconds: {agg([x for x in tl if x['n'] and not inw(x['t_ms'])])}")
except (OSError, ValueError, KeyError):
    pass


# Bucket requests by R2 class, purpose and key component (qlog::bucket),
# as rates over the sampled window (STATUS_EVERY), the whole cluster, and
# per month at R2's prices (docs/quorum.md §2 "What a flush costs").
R2_A, R2_B, R2_FREE_A, R2_FREE_B = 4.50, 0.36, 1e6, 10e6
MONTH_S = 30.4375 * 86400
samples = []
try:
    with open(os.path.join(out, "status.jsonl")) as f:
        samples = [json.loads(line) for line in f if line.strip()]
except OSError:
    pass
by_node = {}
for s in samples:
    if "requests" in s["status"]:
        by_node.setdefault(s["node"], []).append(s)
if by_node:
    # from the first sample a minute in (past the start's reads and the
    # first fence) to the last
    t0 = min(x["at_ms"] for xs in by_node.values() for x in xs)
    rates, components, ops, window = {}, {}, {}, None
    for n, xs in sorted(by_node.items()):
        xs = [x for x in xs if x["at_ms"] >= t0 + 60_000] or xs
        a, b = xs[0], xs[-1]
        secs = (b["at_ms"] - a["at_ms"]) / 1000
        if secs <= 0:
            continue
        window = secs if window is None else min(window, secs)
        ra, rb = a["status"]["requests"], b["status"]["requests"]
        for key, dst in (("by_purpose", rates), ("by_component", components)):
            for k, c in rb[key].items():
                p = ra[key].get(k, {"a": 0, "b": 0, "free": 0})
                d = dst.setdefault(k, [0.0, 0.0, 0.0])
                d[0] += (c["a"] - p["a"]) / secs
                d[1] += (c["b"] - p["b"]) / secs
                d[2] += (c["free"] - p["free"]) / secs
        for k, c in rb["by_op"].items():
            ops[k] = ops.get(k, 0.0) + (c - ra["by_op"].get(k, 0)) / secs
    if window:
        print(f"bucket requests, all nodes, over {window / 60:.0f} min (per s; per month; R2 $/mo before / after the free tier):")

        def row(name, a, b, free):
            mo_a, mo_b = a * MONTH_S, b * MONTH_S
            usd = mo_a / 1e6 * R2_A + mo_b / 1e6 * R2_B
            print(f"  {name:<34} A {a:7.3f}/s  B {b:7.3f}/s  free {free:6.3f}/s   A {mo_a / 1e6:6.2f}M  B {mo_b / 1e6:6.2f}M   ${usd:6.2f}")

        tot = [sum(v[i] for v in rates.values()) for i in range(3)]
        for k, v in sorted(rates.items()):
            row(k, *v)
        row("total", *tot)
        paid = max(0, tot[0] * MONTH_S - R2_FREE_A) / 1e6 * R2_A + max(0, tot[1] * MONTH_S - R2_FREE_B) / 1e6 * R2_B
        print(f"  total after R2's free tier: ${paid:.2f}/mo")
        print("  by purpose/component:")
        for k, v in sorted(components.items(), key=lambda kv: -(kv[1][0] + kv[1][1])):
            if v[0] + v[1] + v[2] > 0:
                row("  " + k, *v)
        print("  by purpose/op (per s): " + ", ".join(f"{k} {v:.3f}" for k, v in sorted(ops.items()) if v > 0))
