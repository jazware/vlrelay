#!/usr/bin/env python3
"""One cluster load step's numbers from the files scripts/cluster-perf.sh
leaves: each node through perf_summary.py, plus forwarding, log lag, peer
bytes off the sockets, PUT/s, e2e_check and the cross-node seq comparison."""
import json
import os
import re
import subprocess
import sys
from collections import defaultdict

out, name, offered, nodes = sys.argv[1], sys.argv[2], float(sys.argv[3]), int(sys.argv[4])
here = os.path.dirname(os.path.abspath(__file__))
LINE = re.compile(r'^([a-zA-Z_:][a-zA-Z0-9_:]*)(\{[^}]*\})?\s+(\S+)')


def prom(path):
    m = {}
    try:
        for l in open(path):
            if not l.startswith("#"):
                g = LINE.match(l)
                if g:
                    m[(g.group(1), g.group(2) or "")] = float(g.group(3))
    except OSError:
        pass
    return m


def total(m, metric, match=""):
    return sum(v for (k, lab), v in m.items() if k == metric and match in lab)


def hist_q(m0, m1, metric, q, match=""):
    b = defaultdict(float)
    for (k, lab), v in m1.items():
        if k == metric + "_bucket" and match in lab:
            le = re.search(r'le="([^"]*)"', lab).group(1)
            b[float(le)] += v - m0.get((k, lab), 0)
    if not b:
        return None
    les = sorted(b)
    n = b[les[-1]]
    if n <= 0:
        return None
    for le in les:
        if b[le] >= q * n:
            return round(le * 1000, 1)
    return None


def peer_bytes(path):
    r = {}
    try:
        for l in open(path):
            a = l.split()
            if len(a) == 4:
                r[(a[0], a[1])] = (int(a[2]), int(a[3]))
    except OSError:
        pass
    return r


per, secs = [], []
for i in range(1, nodes + 1):
    n = f"{name}.n{i}"
    s = json.loads(subprocess.run([sys.executable, f"{here}/perf_summary.py", out, n, str(offered)],
                                  capture_output=True, text=True, check=True).stdout)
    m0, m1 = prom(f"{out}/{n}.m0"), prom(f"{out}/{n}.m1")
    dt = s["secs"]
    secs.append(dt)

    def rate(metric, match=""):
        return round((total(m1, metric, match) - total(m0, metric, match)) / dt, 1)

    fwd_local = rate("vlrelay_cluster_forward_events_total", 'to="local"')
    fwd_remote = rate("vlrelay_cluster_forward_events_total", 'to="remote"')
    node = {
        "node": f"n{i}",
        "in_per_s": s["in_per_s"],
        "accepted_per_s": s["accepted_per_s"],
        "out_per_s": s["out_per_s"],
        "ttf_relay_ms": s["ttf_relay_ms"],
        "durable_ms": s["durable_ms"],
        "put_ms": s["put_ms"],
        "cpu_total": s["cpu_total"],
        "cpu_threads": s["cpu_threads"],
        "stage_busy_cores": s["stage_busy_cores"],
        "rss_gb_max": s["rss_gb_max"],
        "rejects": s["rejects"],
        "ack_pending": s["ack_pending"],
        "lane_queued": s["lane_queued"],
        "fwd_local_per_s": fwd_local,
        "fwd_remote_per_s": fwd_remote,
        "fwd_mb_per_s": round(rate("vlrelay_cluster_forward_bytes_total") / 1e6, 2),
        "fwd_bytes_per_event": round(rate("vlrelay_cluster_forward_bytes_total") / fwd_remote) if fwd_remote else None,
        "fwd_batch_ms": {
            to: {q: hist_q(m0, m1, "vlrelay_cluster_forward_batch_seconds", v, f'to="{to}"') for q, v in (("p50", .5), ("p99", .99))}
            for to in ("local", "remote")
        },
        "log_lag_ms": {
            lg: {q: hist_q(m0, m1, "vlrelay_cluster_log_lag_seconds", v, f'log="{lg}"') for q, v in (("p50", .5), ("p99", .99))}
            for lg in ("own", "peer")
        },
        "hosts_connected": total(m1, "vlrelay_hosts", 'status="connected"') or total(m1, "vlrelay_hosts", 'active'),
        "firehose_disconnects": s["firehose_disconnects"],
    }
    per.append(node)

dt = secs[0] if secs else 1
mi0, mi1 = prom(f"{out}/{name}.miniom0"), prom(f"{out}/{name}.miniom1")
puts = (total(mi1, "minio_s3_requests_total", 'api="putobject"') - total(mi0, "minio_s3_requests_total", 'api="putobject"')) / dt
gets = (total(mi1, "minio_s3_requests_total", 'api="getobject"') - total(mi0, "minio_s3_requests_total", 'api="getobject"')) / dt
p0, p1 = peer_bytes(f"{out}/{name}.peerm0"), peer_bytes(f"{out}/{name}.peerm1")
sent = sum(v[0] - p0.get(k, (0, 0))[0] for k, v in p1.items())
recv = sum(v[1] - p0.get(k, (0, 0))[1] for k, v in p1.items())
e2e = {}
try:
    e2e = json.load(open(f"{out}/{name}.e2e.json"))
except (OSError, ValueError):
    pass
lat = e2e.get("latency_ms", {})
accepted = sum(n["accepted_per_s"] for n in per)
cpu = sum(n["cpu_total"] for n in per)
remote = sum(n["fwd_remote_per_s"] for n in per)


def same_seqs():
    """Every node's stream over the same window: the seqs all of them saw must
    carry the same events, in the same order."""
    streams = []
    for i in range(1, nodes + 1):
        try:
            streams.append([l.split() for l in open(f"{out}/{name}.seqs-n{i}.txt") if l.strip()])
        except OSError:
            return None
    if not all(streams):
        return {"compared": 0}
    maps = [{int(r[0]): tuple(r[1:]) for r in s} for s in streams]
    common = set(maps[0])
    for m in maps[1:]:
        common &= set(m)
    differ = sum(1 for q in common if any(m[q] != maps[0][q] for m in maps[1:]))
    orders = [[int(r[0]) for r in s if int(r[0]) in common] for s in streams]
    return {
        "compared": len(common),
        "per_node": [len(m) for m in maps],
        "differ": differ,
        "same_order": all(o == orders[0] for o in orders[1:]),
    }


r = {
    "step": name,
    "offered": offered,
    "nodes": nodes,
    "secs": dt,
    "accepted_per_s": round(accepted, 1),
    "in_per_s": round(sum(n["in_per_s"] for n in per), 1),
    "cpu_total": round(cpu, 2),
    "us_per_event": round(cpu * 1e6 / accepted, 1) if accepted else None,
    "remote_share": round(remote / accepted, 3) if accepted else None,
    "peer_mb_per_s": {"listener_sent": round(sent / dt / 1e6, 1), "listener_recv": round(recv / dt / 1e6, 1)},
    "peer_bytes_per_event": round((sent + recv) / dt / accepted) if accepted else None,
    "puts_per_s": round(puts, 1),
    "gets_per_s": round(gets, 1),
    "ttf_e2e_ms": {k: lat.get(k) for k in ("p50", "p90", "p99", "max")},
    "e2e": {k: e2e.get(k) for k in ("missing", "extra", "out_of_order", "rev_regressions", "duplicates", "seq_regressions", "bad_frames") if k in e2e},
    "same_seqs": same_seqs(),
    "seq_disagree": os.path.exists(f"{out}/{name}.disagree") and os.path.getsize(f"{out}/{name}.disagree") > 0,
    "per_node": per,
}
r["e2e"]["n"] = lat.get("n")
print(json.dumps(r, indent=1))
