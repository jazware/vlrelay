#!/usr/bin/env python3
"""One load step's numbers from the files scripts/perf.sh leaves: relay
/metrics before and after, MinIO metrics, per-thread CPU samples, e2e_check."""
import json
import re
import sys
from collections import defaultdict

out, name, offered = sys.argv[1], sys.argv[2], float(sys.argv[3])
LINE = re.compile(r'^([a-zA-Z_:][a-zA-Z0-9_:]*)(\{[^}]*\})?\s+(\S+)')


def prom(path):
    m = {}
    try:
        for l in open(path):
            if l.startswith("#"):
                continue
            g = LINE.match(l)
            if g:
                m[(g.group(1), g.group(2) or "")] = float(g.group(3))
    except OSError:
        pass
    return m


def total(m, metric, match=""):
    return sum(v for (k, lab), v in m.items() if k == metric and match in lab)


def by_label(m, metric, label):
    r = defaultdict(float)
    for (k, lab), v in m.items():
        if k == metric:
            g = re.search(label + r'="([^"]*)"', lab)
            r[g.group(1) if g else ""] += v
    return r


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
            return le * 1000
    return None


m0, m1 = prom(f"{out}/{name}.m0"), prom(f"{out}/{name}.m1")
samples = [json.loads(l) for l in open(f"{out}/{name}.samples") if l.strip()]
dt = samples[-1]["t"] - samples[0]["t"] if len(samples) > 1 else 1
cpu = {}
for k, v in samples[-1]["cpu"].items():
    cpu[k] = round((v - samples[0]["cpu"].get(k, 0)) / 100 / dt, 2)
cpu = dict(sorted(((k, v) for k, v in cpu.items() if v >= 0.01), key=lambda x: -x[1]))


def rate(metric, match=""):
    return round((total(m1, metric, match) - total(m0, metric, match)) / dt, 1)


stage = {}
b0, b1 = by_label(m0, "vlrelay_stage_busy_us_total", "stage"), by_label(m1, "vlrelay_stage_busy_us_total", "stage")
for k in b1:
    stage[k] = round((b1[k] - b0.get(k, 0)) / 1e6 / dt, 2)
rej0, rej1 = by_label(m0, "vlrelay_events_rejected_total", "reason"), by_label(m1, "vlrelay_events_rejected_total", "reason")
rejects = {k: int(v - rej0.get(k, 0)) for k, v in rej1.items() if v - rej0.get(k, 0) > 0}
mi0, mi1 = prom(f"{out}/{name}.minio0"), prom(f"{out}/{name}.minio1")
puts = (total(mi1, "minio_s3_requests_total", 'api="putobject"') - total(mi0, "minio_s3_requests_total", 'api="putobject"')) / dt
gets = (total(mi1, "minio_s3_requests_total", 'api="getobject"') - total(mi0, "minio_s3_requests_total", 'api="getobject"')) / dt
e2e = {}
try:
    e2e = json.load(open(f"{out}/{name}.e2e.json"))
except (OSError, ValueError):
    pass
lat = e2e.get("latency_ms", {})
r = {
    "step": name,
    "offered": offered,
    "secs": round(dt, 1),
    "in_per_s": rate("vlrelay_events_in_total"),
    "accepted_per_s": rate("vlrelay_events_accepted_total"),
    "out_per_s": rate("vlrelay_events_out_total"),
    "dup_per_s": rate("vlrelay_events_duplicate_total"),
    "rejects": rejects,
    "ttf_relay_ms": {
        "p50": hist_q(m0, m1, "vlrelay_time_to_firehose_seconds", 0.5),
        "p99": hist_q(m0, m1, "vlrelay_time_to_firehose_seconds", 0.99),
    },
    "ttf_e2e_ms": {k: lat.get(k) for k in ("p50", "p90", "p99", "max")},
    "durable_ms": {
        "p50": hist_q(m0, m1, "vlrelay_time_to_durable_seconds", 0.5),
        "p99": hist_q(m0, m1, "vlrelay_time_to_durable_seconds", 0.99),
        "lag_gauge": total(m1, "vlrelay_durable_lag_ms"),
    },
    "put_ms": {
        "p50": hist_q(m0, m1, "vlpds_segment_put_seconds", 0.5),
        "p99": hist_q(m0, m1, "vlpds_segment_put_seconds", 0.99),
    },
    "lane_queued": total(m1, "vlrelay_lane_queued"),
    "ack_pending": total(m1, "vlrelay_ack_pending"),
    "cpu_total": round(sum(cpu.values()), 2),
    "cpu_threads": cpu,
    "stage_busy_cores": stage,
    "rss_gb_max": round(max(s["rss"] for s in samples) / 2**30, 2),
    "puts_per_s": round(puts, 1),
    "gets_per_s": round(gets, 1),
    "e2e": {
        k: e2e.get(k)
        for k in ("missing", "extra", "out_of_order", "rev_regressions", "duplicates", "seq_regressions", "bad_frames")
        if k in e2e
    },
}
r["e2e"]["n"] = lat.get("n")
print(json.dumps(r, indent=1))
