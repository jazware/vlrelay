#!/usr/bin/env python3
"""The headline numbers of perf.sh step summaries, one line each."""
import json
import sys

KEYS = ["in_per_s", "out_per_s", "ttf_e2e_ms", "durable_ms", "put_ms", "cpu_total", "cpu_threads",
        "stage_busy_cores", "rss_gb_max", "puts_per_s", "rejects", "e2e", "lane_queued", "ack_pending"]
for f in sys.argv[1:]:
    r = json.load(open(f))
    print(r["step"], json.dumps({k: r.get(k) for k in KEYS}))
