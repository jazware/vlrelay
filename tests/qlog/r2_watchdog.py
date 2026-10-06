#!/usr/bin/env python3
"""The bucket-request watchdog for runs against a billed bucket (R2): a
process of its own, apart from the nodes and the harness, that polls every
node's /qlog/status, sums `requests.total` across the cluster, and kills the
run on a breach.

    tests/qlog/r2_watchdog.py --node n1=127.0.0.1:3161 ... \
        --pgid-file OUT/pgid [--kill-pattern REGEX] --log OUT/watchdog.jsonl

Rules (cluster totals, R2 classes; defaults are the 1-hour 350/s budget in
tests/qlog/R2_HOUR.md):
  - cumulative Class A >= --budget-a or Class B >= --budget-b;
  - the mean rate over the last --window-s (120) > --rate-a / --rate-b;
  - any one poll interval (--poll-s, 10) adding > --burst-a / --burst-b;
  - no status from some node for > --stale-s (30): it may be sending
    requests nobody can see, so that's a breach too.

On a breach: SIGKILL the run's process group (--pgid-file, the harness
started under setsid), then every process matching --kill-pattern, write
--tripped-file, exit 3. A node's count starts from zero when it restarts, so
each process (status `requests.pid`) is summed on its own: the cluster
total is every incarnation's last count. --done-file existing ends the watch
cleanly (exit 0).
"""
import argparse
import json
import os
import signal
import subprocess
import sys
import time
import urllib.request


def parse():
    p = argparse.ArgumentParser()
    p.add_argument("--node", action="append", required=True, help="id=host:port (the http port)")
    p.add_argument("--budget-a", type=int, default=3500)
    p.add_argument("--budget-b", type=int, default=12000)
    p.add_argument("--rate-a", type=float, default=2.4)
    p.add_argument("--rate-b", type=float, default=8.3)
    p.add_argument("--window-s", type=float, default=120)
    p.add_argument("--burst-a", type=int, default=150)
    p.add_argument("--burst-b", type=int, default=300)
    p.add_argument("--poll-s", type=float, default=10)
    p.add_argument("--stale-s", type=float, default=30)
    p.add_argument("--pgid-file", help="holds the run's process group id")
    p.add_argument("--kill-pattern", help="pkill -9 -f this too")
    p.add_argument("--log", required=True)
    p.add_argument("--tripped-file")
    p.add_argument("--done-file")
    return p.parse_args()


def status(addr, timeout):
    with urllib.request.urlopen(f"http://{addr}/qlog/status", timeout=timeout) as r:
        return json.load(r)


def kill_everything(a, log):
    killed = []
    if a.pgid_file and os.path.exists(a.pgid_file):
        try:
            pgid = int(open(a.pgid_file).read().split()[0])
            if pgid > 1 and pgid != os.getpgrp():
                os.killpg(pgid, signal.SIGKILL)
                killed.append(f"pgid {pgid}")
        except (ValueError, ProcessLookupError, PermissionError) as e:
            killed.append(f"pgid: {e!r}")
    if a.kill_pattern:
        r = subprocess.run(["pkill", "-9", "-f", a.kill_pattern])
        killed.append(f"pkill rc {r.returncode}")
    log({"event": "killed", "how": killed})


def main():
    a = parse()
    nodes = dict(n.split("=", 1) for n in a.node)
    out = open(a.log, "a", buffering=1)

    def log(rec):
        rec = {"at_ms": int(time.time() * 1000), **rec}
        out.write(json.dumps(rec) + "\n")
        if rec.get("event") != "poll":
            print(json.dumps(rec), file=sys.stderr, flush=True)

    # (node, pid) -> last counts of that process
    procs = {}
    last_ok = {n: time.monotonic() for n in nodes}
    history = []  # (monotonic, cluster a, cluster b)
    log({"event": "start", "nodes": nodes, "rules": {k: v for k, v in vars(a).items() if k not in ("node",)}})

    def trip(why):
        log({"event": "TRIPPED", "why": why})
        kill_everything(a, log)
        if a.tripped_file:
            with open(a.tripped_file, "w") as f:
                f.write(why + "\n")
        sys.exit(3)

    while True:
        if a.done_file and os.path.exists(a.done_file):
            log({"event": "done"})
            return 0
        t = time.monotonic()
        seen = {}
        for n, addr in nodes.items():
            try:
                s = status(addr, timeout=min(5.0, a.poll_s))
                r = s.get("requests", {})
                tot = r.get("total", {})
                procs[(n, r.get("pid", 0))] = (tot.get("a", 0), tot.get("b", 0))
                last_ok[n] = t
                seen[n] = {"pid": r.get("pid"), "a": tot.get("a", 0), "b": tot.get("b", 0), "role": s.get("role")}
            except Exception as e:  # noqa: BLE001 - any failure is "no status"
                seen[n] = {"error": str(e)[:120]}
        ca = sum(v[0] for v in procs.values())
        cb = sum(v[1] for v in procs.values())
        history.append((t, ca, cb))
        rec = {"event": "poll", "a": ca, "b": cb, "nodes": seen}
        if len(history) >= 2:
            t1, a1, b1 = history[-2]
            rec["interval"] = {"s": round(t - t1, 2), "a": ca - a1, "b": cb - b1}
        log(rec)

        if ca >= a.budget_a:
            trip(f"Class A budget: {ca} >= {a.budget_a}")
        if cb >= a.budget_b:
            trip(f"Class B budget: {cb} >= {a.budget_b}")
        # the first poll's interval runs from zero: a node's counts start
        # with its process
        _, a1, b1 = history[-2] if len(history) >= 2 else (0, 0, 0)
        if ca - a1 > a.burst_a:
            trip(f"Class A burst: {ca - a1} in one poll > {a.burst_a}")
        if cb - b1 > a.burst_b:
            trip(f"Class B burst: {cb - b1} in one poll > {a.burst_b}")
        old = [h for h in history if t - h[0] >= a.window_s - 0.5]
        if old:
            t0, a0, b0 = old[-1]
            secs = t - t0
            if (ca - a0) / secs > a.rate_a:
                trip(f"Class A rate: {(ca - a0) / secs:.2f}/s over {secs:.0f} s > {a.rate_a}")
            if (cb - b0) / secs > a.rate_b:
                trip(f"Class B rate: {(cb - b0) / secs:.2f}/s over {secs:.0f} s > {a.rate_b}")
        stale = [n for n in nodes if t - last_ok[n] > a.stale_s]
        if stale:
            trip(f"no status from {', '.join(stale)} for > {a.stale_s:.0f} s")
        history = [h for h in history if t - h[0] <= a.window_s + 2 * a.poll_s]
        time.sleep(max(0.0, a.poll_s - (time.monotonic() - t)))


if __name__ == "__main__":
    sys.exit(main())
