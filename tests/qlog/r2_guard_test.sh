#!/usr/bin/env bash
# Tests of the R2 request guards against the local MinIO (tests/qlog/R2_HOUR.md):
# each case runs chaos.sh under setsid with a deliberate runaway (`--runaway`
# GETs) or a fault, and checks that the right guard stopped the run, how
# long it took, and that no process of the run is left.
#
#   tests/qlog/r2_guard_test.sh [CASE...]    # default: all but calibrate
#
# Cases: node-burst node-burst-a node-rate node-budget (the in-node guard
# alone, no watchdog); wd-burst wd-burst-a wd-rate wd-stale (the watchdog
# alone, nodes unarmed); the -a cases PUT, so Class A trips;
# wd-restarts (kill -9s under the watchdog: counts summed per process, no
# trip); tool-verify (the final verify under a 5 GET cap: it trips, retain
# never runs, the watchdog's last count includes the tool); calibrate (CAL_SEC, 2100, at 350/s with 30 s flushes and both
# guards at the hour's numbers: neither may trip).
set -uo pipefail
here="$(cd "$(dirname "$0")" && pwd)"
crate="$(cd "$here/../.." && pwd)"
cd "$crate"
export QLOG_BASE=${QLOG_BASE:-4650} QLOG_NO_BUILD=1
B=$QLOG_BASE
bin=${CARGO_TARGET_DIR:-target}/dev-release/qlog
root=${OUT:-$crate/dev/r2-guard-test}
real="--budget-a 3500 --budget-b 12000 --budget-rate-a 2.4 --budget-rate-b 8.3 --budget-window-s 120 --budget-burst-a 250 --budget-burst-b 300"
cases=${*:-node-burst node-burst-a node-rate node-budget wd-burst wd-burst-a wd-rate wd-stale wd-restarts tool-verify}
fail=0

# case name, seconds of load, scenario, watchdog (1/0), node flags, [extra env...]
run() {
  local name=$1 secs=$2 scenario=$3 wd=$4 budget=$5
  shift 5
  local out=$root/$name run=$root/$name/run
  rm -rf "$out"
  mkdir -p "$out"
  local t0
  t0=$(($(date +%s%N) / 1000000))
  env "$@" OUT="$run" BUDGET="$budget" CL_DIR=/dev/shm/vlrq-guard-$B FLUSH_MS="${FLUSH_MS:-30000}" \
    setsid bash "$here/chaos.sh" "$scenario" --duration "$secs" --every 15 >"$out/chaos.log" 2>&1 &
  local harness=$!
  for _ in $(seq 1 100); do [ -s "$run/pgid" ] && [ -s "$run/prefix" ] && break; sleep 0.1; done
  local prefix
  prefix=$(cat "$run/prefix")
  local watchdog=""
  if [ "$wd" = 1 ]; then
    setsid python3 "$here/r2_watchdog.py" --node "n1=127.0.0.1:$((B + 11))" --node "n2=127.0.0.1:$((B + 12))" --node "n3=127.0.0.1:$((B + 13))" \
      --pgid-file "$run/pgid" --kill-pattern "^$bin .*--prefix $prefix" --log "$out/watchdog.jsonl" --count-glob "$run/budget-tool-*.json" \
      --tripped-file "$out/watchdog-tripped" --done-file "$out/done" ${WD_FLAGS:-} 2>"$out/watchdog.log" &
    watchdog=$!
  fi
  [ -n "${DURING:-}" ] && (eval "$DURING") &
  wait "$harness"
  local rc=$?
  local t1
  t1=$(($(date +%s%N) / 1000000))
  touch "$out/done"
  [ -n "$watchdog" ] && wait "$watchdog"
  local wrc=$?
  [ -n "$watchdog" ] || wrc=-
  sleep 1
  local left
  left=$(pgrep -fc -- "--prefix $prefix" || true)
  [ "$left" = 0 ] || pgrep -af -- "--prefix $prefix" | cut -c1-200 | sed 's/^/  left: /'
  pkill -9 -f -- "--prefix $prefix" 2>/dev/null || true
  # kill -9 of the group skips chaos.sh's own cleanup
  pkill -9 -f "proxy.py --control $((B + 29)) " 2>/dev/null || true
  COMPOSE_PROJECT_NAME=vlrq-chaos-$B MINIO_PORT=$((B + 40)) docker compose -f "$here/compose.yml" down -v >/dev/null 2>&1 || true
  rm -rf "/dev/shm/vlrq-guard-$B"
  echo "$name: harness rc $rc, watchdog rc $wrc, $(((t1 - t0) / 1000)) s from start, processes left $left"
  grep -h BUDGET "$run/events.log" 2>/dev/null | head -3
  grep -h "TRIPPED" "$run"/n*.log 2>/dev/null | head -3
  [ -e "$out/watchdog-tripped" ] && echo "  watchdog: $(cat "$out/watchdog-tripped")"
  python3 - "$run" "$out" <<'PY'
import glob, json, os, re, sys, datetime
run, out = sys.argv[1:]
def ts(line):
    m = re.match(r"(\d{4}-\d\d-\d\dT[\d:.]+)Z", line)
    return datetime.datetime.fromisoformat(m.group(1)).replace(tzinfo=datetime.timezone.utc).timestamp() if m else None
start = None
for f in glob.glob(f"{run}/n*.log"):
    for l in open(f, errors="replace"):
        if "--runaway" in l and (t := ts(l)):
            start = t if start is None else min(start, t)
ev = {}
if os.path.exists(f"{run}/events.log"):
    for l in open(f"{run}/events.log", errors="replace"):
        if "BUDGET" in l:
            ev.setdefault("budget", int(l.split()[0]) / 1000)
wd = [json.loads(l) for l in open(f"{out}/watchdog.jsonl")] if os.path.exists(f"{out}/watchdog.jsonl") else []
trip = next((r for r in wd if r["event"] == "TRIPPED"), None)
killed = next((r for r in wd if r["event"] == "killed"), None)
last = [r for r in wd if r["event"] == "poll"][-1:] or [{}]
if start:
    if "budget" in ev:
        print(f"  in-node guard stopped the run {ev['budget'] - start:.1f} s after the runaway started")
    if trip:
        print(f"  watchdog stopped the run {killed['at_ms'] / 1000 - start:.1f} s after the runaway started")
if wd:
    pids = {(n, v.get('pid')) for r in wd if r["event"] == "poll" for n, v in r["nodes"].items() if v.get("pid")}
    print(f"  watchdog: {len([r for r in wd if r['event'] == 'poll'])} polls, last cluster A {last[0].get('a')} B {last[0].get('b')}, {len(pids)} node processes seen")
PY
  case $name in
    node-*) { [ "$rc" = 86 ] && [ "$left" = 0 ]; } || { echo "  FAIL"; fail=1; } ;;
    tool-*) { [ "$rc" = 86 ] && [ -e "$run/budget-tool-verify.tripped" ] && [ ! -s "$run/retain.json" ] && [ "$left" = 0 ] &&
      grep -q '"event": "done".*budget-tool-verify' "$out/watchdog.jsonl"; } || { echo "  FAIL"; fail=1; }
      cat "$run"/budget-tool-*.tripped 2>/dev/null | sed 's/^/  tool: /'
      grep '"event": "done"' "$out/watchdog.jsonl" | cut -c1-300 | sed 's/^/  watchdog: /' ;;
    wd-restarts | calibrate) { [ ! -e "$out/watchdog-tripped" ] && [ ! -e "$run/budget-tripped" ]; } || { echo "  FAIL"; fail=1; } ;;
    wd-*) { [ "$wrc" = 3 ] && [ "$left" = 0 ]; } || { echo "  FAIL"; fail=1; } ;;
  esac
}

for c in $cases; do
  case $c in
    node-burst) run node-burst 60 baseline 0 "$real --runaway get:200" ;;
    node-burst-a) run node-burst-a 60 baseline 0 "$real --runaway put:30" ;;
    node-rate) run node-rate 180 baseline 0 "$real --runaway get:15" ;;
    node-budget) run node-budget 120 baseline 0 "--budget-a 30 --budget-b 12000" ;;
    wd-burst) run wd-burst 60 baseline 1 "--runaway get:100" ;;
    wd-burst-a) run wd-burst-a 60 baseline 1 "--runaway put:10" ;;
    wd-rate) run wd-rate 200 baseline 1 "--runaway get:4" ;;
    wd-stale) DURING="sleep 20; pkill -STOP -f -- '^$bin node --id n2 '" run wd-stale 120 baseline 1 "" ;;
    tool-verify) run tool-verify 45 baseline 1 "" TOOL_BUDGET="--budget-b 5" FINAL_VERIFY=1 VERIFY_EVERY=0 ;;
    wd-restarts) run wd-restarts 75 kill-leader 1 "$real" ;;
    calibrate) run calibrate "${CAL_SEC:-2100}" baseline 1 "$real" STATUS_EVERY=60 ;;
    *) echo "unknown case $c" >&2; fail=1 ;;
  esac
done
exit $fail
