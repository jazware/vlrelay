#!/usr/bin/env python3
"""Summarizes a tests/qlog/relay-chaos.sh run: report.py's log view (the
checker's verdict, pauses after faults, CPU and RSS, recoveries, bucket
requests), then the relay's own: e2e_check's upstream-vs-relay counts and
latency, and each node's admissions and host table.

    relay_report.py OUT_DIR
"""
import json
import os
import subprocess
import sys

out = sys.argv[1]
here = os.path.dirname(os.path.abspath(__file__))
subprocess.run([sys.executable, os.path.join(here, "report.py"), out], check=False)


def load(name, default=None):
    try:
        with open(os.path.join(out, name)) as f:
            return json.load(f)
    except (OSError, ValueError):
        return default


e = load("e2e.json", {})
if e:
    lat = e.get("latency_ms", {})
    print(
        f"e2e (fakepds -> relay): missing {e.get('missing')}, extra {e.get('extra')} (repeats after a recovery), "
        f"out of order {e.get('out_of_order')}, rev regressions {e.get('rev_regressions')}"
    )
    for k, c in sorted(e.get("kinds", {}).items()):
        print(f"  {k}: upstream {c.get('upstream')} relay {c.get('relay')} matched {c.get('matched')} missing {c.get('missing')} extra {c.get('extra')}")
    if lat:
        print(f"  upstream -> relay: p50 {lat.get('p50')} ms, p90 {lat.get('p90')}, p99 {lat.get('p99')}, max {lat.get('max')} (n={lat.get('n')})")
v = load("verify.json", {})
if v:
    print(f"final manifest: ok={v.get('ok')} F={v.get('flushed')} segments={v.get('segments')} entries={v.get('entries')} relay entries={v.get('relay_entries')} rows={v.get('dids')} hosts={v.get('hosts')} gaps={v.get('gaps')}")
    for m in v.get("messages", [])[:5]:
        print(f"  ! {m}")
for f in sorted(os.listdir(out)):
    if f.startswith("status-n") and f.endswith(".json"):
        s = load(f, {})
        r = s.get("relay")
        if r:
            print(
                f"{f[7:-5]}: admitted {r.get('admitted')} duplicates {r.get('duplicates')} rejected {r.get('rejected')} retried {r.get('retried')} "
                f"not_ready {r.get('not_ready')} terms {r.get('terms')} host_moves {r.get('host_moves')} retain {r.get('retain_runs')}/{r.get('retain_deleted')} owners {r.get('owners')}"
            )
