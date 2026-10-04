#!/usr/bin/env bash
# The relay e2e (docs/devloop.md): local network up, accounts seeded, load
# running, the relay crawling every upstream, and e2e_check comparing the
# relay's firehose with each upstream's own.
#
#   tests/e2e/run.sh [--duration 60] [--rate 50] [--accounts 30]
#
# Until the relay implements the CLI contract (its --help names --listen and
# --host), the relay part is skipped and the checker runs against the
# upstreams themselves instead, which still proves the network, the load and
# the checker. Env: KEEP=1 leaves the network up (dev-down tears it down),
# OUT (dev/state/e2e: logs and the JSON report).
set -euo pipefail
here="$(cd "$(dirname "$0")" && pwd)"
crate="$(cd "$here/../.." && pwd)"
cd "$crate"
. dev/ports.sh

duration=60 rate=50 accounts=30
while [ $# -gt 0 ]; do
  case $1 in
    --duration) duration=$2; shift 2 ;;
    --rate) rate=$2; shift 2 ;;
    --accounts) accounts=$2; shift 2 ;;
    *) echo "e2e: unknown flag $1" >&2; exit 1 ;;
  esac
done
out=${OUT:-dev/state/e2e}
target=${CARGO_TARGET_DIR:-target}/debug

t0=$(date +%s)
cargo build --quiet --bin vlrelay --bin e2e_check --bin devnet
echo "e2e: built in $(($(date +%s) - t0))s"

pids=()
cleanup() {
  for p in "${pids[@]}"; do kill "$p" 2>/dev/null || true; done
  wait 2>/dev/null || true
  if [ "${KEEP:-}" = 1 ]; then
    echo "e2e: KEEP=1: network left up (just dev-down)"
  else
    dev/down.sh
  fi
}
trap cleanup EXIT

dev/up.sh
mkdir -p "$out"
hosts=$(cat dev/state/hosts)
up_flags=$(sed 's/^/--upstream /' dev/state/hosts | tr '\n' ' ')
"$target/devnet" seed --accounts "$accounts" $(sed 's/^/--host /' dev/state/hosts | tr '\n' ' ')

relay_flags=""
if "$target/vlrelay" --help 2>/dev/null | grep -q -- '--listen' && "$target/vlrelay" --help 2>/dev/null | grep -q -- '--host'; then
  dev/capped.sh "${RELAY_MEM_MB:-4096}" "$target/vlrelay" \
    --listen "127.0.0.1:$RELAY_PORT" --memory --plc-url "http://127.0.0.1:$PLC_PORT" --linger-ms 25 \
    $(sed 's/^/--host /' dev/state/hosts | tr '\n' ' ') >"$out/relay.log" 2>&1 &
  pids+=($!)
  echo $! >dev/state/relay.pid
  for _ in $(seq 1 100); do
    curl -sf "http://127.0.0.1:$RELAY_PORT/xrpc/_health" >/dev/null && break
    sleep 0.2
  done
  curl -sf "http://127.0.0.1:$RELAY_PORT/xrpc/_health" >/dev/null || { echo "e2e: the relay didn't come up" >&2; tail -30 "$out/relay.log" >&2; exit 1; }
  relay_flags="--relay http://127.0.0.1:$RELAY_PORT"
  echo "e2e: relay up on :$RELAY_PORT"
else
  echo "e2e: SKIP relay: vlrelay doesn't implement the CLI contract yet (--listen, --host); checking the upstreams against themselves"
  relay_flags=$(sed 's/^/--relay /' dev/state/hosts | tr '\n' ' ')
fi

# the checker subscribes first so the load's first events are in its window
"$target/e2e_check" $up_flags $relay_flags --duration "$duration" --warmup 3 --settle 10 --json-out "$out/report.json" >"$out/check.txt" 2>"$out/check.log" &
check=$!
sleep 2
"$target/devnet" load --rate "$rate" --duration "$((duration + 2))" --identity-every 10 --deactivate-every 20 2>"$out/load.log" &
load=$!
pids+=($load)
rc=0
wait "$check" || rc=$?
wait "$load" 2>/dev/null || true
cat "$out/check.txt"
tail -3 "$out/load.log"
echo "e2e: $( [ $rc = 0 ] && echo PASS || echo FAIL ) in $(($(date +%s) - t0))s (report $out/report.json)"
[ "${KEEP:-}" = 1 ] || cp -r "$out" "${TMPDIR:-/tmp}/vlrelay-e2e-last" 2>/dev/null || true
exit $rc
