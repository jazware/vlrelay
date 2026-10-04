#!/usr/bin/env bash
# The cluster e2e (docs/cluster.md, docs/devloop.md): three core relays on one
# MinIO prefix with peer mTLS, the upstreams split across them by host shard,
# plus an edge and a replica. One checker per relay stream: each must carry
# every upstream event, once, in order, and every stream must carry the same
# events at the same relay seqs.
#
#   tests/e2e/cluster.sh [--duration 90] [--rate 50] [--accounts 30] [--no-ha]
#                        [--kill-at 20] [--restart-at 35] [--term-at 50] [--return-at 60]
#
# HA, by default: kill -9 core 1 at --kill-at, start it again at
# --restart-at, SIGTERM core 2 at --term-at (a planned handoff) and start it
# again at --return-at. Times are seconds into the load. The checkers of
# cores 1-3 each list all three cores (`--relay a,b,c`): a socket to a dead
# node moves to the next one with its cursor. --no-ha just runs steady state.
#
# Ports: core i serves on CLUSTER_PORT_BASE+i (2960) and peers on
# CLUSTER_PORT_BASE+10+i; the edge and the replica take +3 and +4. Env: KEEP=1,
# OUT (dev/state/e2e-cluster), TTL_MS (3000: node lease TTL).
set -euo pipefail
here="$(cd "$(dirname "$0")" && pwd)"
crate="$(cd "$here/../.." && pwd)"
cd "$crate"
. dev/ports.sh

duration=90 rate=50 accounts=30 ha=1 kill_at=20 restart_at=35 term_at=50 return_at=60
while [ $# -gt 0 ]; do
  case $1 in
    --duration) duration=$2; shift 2 ;;
    --rate) rate=$2; shift 2 ;;
    --accounts) accounts=$2; shift 2 ;;
    --no-ha) ha=""; shift ;;
    --kill-at) kill_at=$2; shift 2 ;;
    --restart-at) restart_at=$2; shift 2 ;;
    --term-at) term_at=$2; shift 2 ;;
    --return-at) return_at=$2; shift 2 ;;
    *) echo "e2e-cluster: unknown flag $1" >&2; exit 1 ;;
  esac
done
out=${OUT:-dev/state/e2e-cluster}
target=${CARGO_TARGET_DIR:-target}/debug
base=${CLUSTER_PORT_BASE:-2960}
ttl=${TTL_MS:-3000}

t0=$(date +%s)
cargo build --quiet --bin vlrelay --bin e2e_check --bin devnet
echo "e2e-cluster: built in $(($(date +%s) - t0))s"

pids=()
cleanup() {
  for p in "${pids[@]}"; do
    pkill -P "$p" 2>/dev/null || true
    kill "$p" 2>/dev/null || true
  done
  wait 2>/dev/null || true
  if [ "${KEEP:-}" = 1 ]; then
    echo "e2e-cluster: KEEP=1: network left up (just dev-down)"
  else
    dev/down.sh
  fi
}
trap cleanup EXIT

DEV_PDS=${DEV_PDS:-3} dev/up.sh
rm -rf "$out" && mkdir -p "$out"
up_flags=$(sed 's/^/--upstream /' dev/state/hosts | tr '\n' ' ')
"$target/devnet" seed --accounts "$accounts" $(sed 's/^/--host /' dev/state/hosts | tr '\n' ' ')

prefix="e2e-cluster-$(date +%s)-$$"
store="--s3-endpoint http://127.0.0.1:$MINIO_PORT --s3-bucket vlrelay --s3-access-key minioadmin --s3-secret-key minioadmin --prefix $prefix"
common="$store --plc-url http://127.0.0.1:$PLC_PORT --linger-ms 25 --dev-mode --internal-token e2e-cluster-token --peer-tls-dir dev/state/peer-tls --lease-ttl-ms $ttl --did-shards 8 --host-shards ${HOST_SHARDS:-16}"
hosts=$(sed 's/^/--host /' dev/state/hosts | tr '\n' ' ')

declare -a node_pid
pub() { echo $((base + $1)); }
start_node() { # i role
  local i=$1 role=$2
  dev/capped.sh "${RELAY_MEM_MB:-3072}" "$target/vlrelay" --role "$role" --node-id "n$i" \
    --listen "127.0.0.1:$(pub "$i")" --peer-listen "127.0.0.1:$((base + 10 + i))" \
    --advertise-url "https://127.0.0.1:$((base + 10 + i))" $common $hosts >>"$out/n$i.log" 2>&1 &
  node_pid[$i]=$!
  pids+=($!)
  for _ in $(seq 1 150); do
    curl -sf "http://127.0.0.1:$(pub "$i")/xrpc/_health" >/dev/null && return 0
    sleep 0.2
  done
  echo "e2e-cluster: n$i ($role) didn't come up" >&2
  tail -30 "$out/n$i.log" >&2
  exit 1
}
victim() { pgrep -P "${node_pid[$1]}" vlrelay || echo "${node_pid[$1]}"; }
ms() { python3 -c 'import time; print(int(time.time()*1000))'; }

# the first core alone creates the layouts; the others join it
start_node 1 core
start_node 2 core
start_node 3 core
start_node 4 edge
start_node 5 replica
echo "e2e-cluster: 3 cores, an edge and a replica up on :$(pub 1)-:$(pub 5)"
sleep 3

u() { echo "http://127.0.0.1:$(pub "$1")"; }
streams=("$(u 1),$(u 2),$(u 3)" "$(u 2),$(u 3),$(u 1)" "$(u 3),$(u 1),$(u 2)" "$(u 4)" "$(u 5)")
names=(core1 core2 core3 edge replica)
checks=()
for k in "${!streams[@]}"; do
  n=${names[$k]}
  "$target/e2e_check" $up_flags --relay "${streams[$k]}" --duration "$duration" --warmup 3 --settle 15 \
    --json-out "$out/report-$n.json" --seq-out "$out/seqs-$n.txt" --lat-out "$out/lat-$n.txt" \
    >"$out/check-$n.txt" 2>"$out/check-$n.log" &
  checks+=($!)
done
sleep 2
t_load=$(ms)
"$target/devnet" load --rate "$rate" --duration "$((duration + 2))" --identity-every 10 --deactivate-every 20 2>"$out/load.log" &
load=$!
pids+=($load)

events=()
at() { # seconds-into-load
  local now=$(( ($(ms) - t_load) / 1000 ))
  [ "$1" -gt "$now" ] && sleep $(( $1 - now ))
  return 0
}
# the busiest core (most upstream sockets) is the one to lose
connected() {
  local c
  c=$(curl -sf "http://127.0.0.1:$(pub "$1")/metrics" | awk '/^vlrelay_hosts\{status="connected"\}/ {print $2}') || true
  echo "${c:-0}"
}
busiest() { # candidates...
  local best="" n=-1 c
  for i in "$@"; do c=$(connected "$i"); [ "$c" -gt "$n" ] && { n=$c; best=$i; }; done
  echo "$best"
}
sockets() { echo "e2e-cluster: upstream sockets per core: n1 $(connected 1), n2 $(connected 2), n3 $(connected 3)"; }
if [ -n "$ha" ]; then
  at "$kill_at"
  sockets
  k=$(busiest 1 2 3)
  v=$(victim "$k"); kill -9 "$v"; kill "${node_pid[$k]}" 2>/dev/null || true
  events+=("kill9 n$k $(ms)")
  echo "e2e-cluster: kill -9 n$k (pid $v) at ${kill_at}s"
  at "$restart_at"
  start_node "$k" core
  events+=("restart n$k $(ms)")
  echo "e2e-cluster: n$k back at $(( ($(ms) - t_load) / 1000 ))s"
  at "$term_at"
  sockets
  others=()
  for i in 1 2 3; do [ "$i" != "$k" ] && others+=("$i"); done
  t=$(busiest "${others[@]}")
  v=$(victim "$t"); kill -TERM "$v"
  events+=("term n$t $(ms)")
  echo "e2e-cluster: SIGTERM n$t (pid $v) at ${term_at}s"
  for _ in $(seq 1 300); do kill -0 "$v" 2>/dev/null || break; sleep 0.1; done
  events+=("exited n$t $(ms)")
  kill "${node_pid[$t]}" 2>/dev/null || true
  at "$return_at"
  start_node "$t" core
  events+=("restart n$t $(ms)")
  echo "e2e-cluster: n$t back at $(( ($(ms) - t_load) / 1000 ))s"
  sleep 3
  sockets
fi
printf '%s\n' "${events[@]}" >"$out/events.txt"
echo "$t_load" >"$out/t_load_ms"

rc=0
for k in "${!checks[@]}"; do
  wait "${checks[$k]}" || { echo "e2e-cluster: checker ${names[$k]} FAILED"; rc=1; }
done
for i in 1 2 3 4 5; do
  curl -sf "http://127.0.0.1:$(pub "$i")/metrics" | grep -E '^vlrelay_|^vlpds_cluster' >"$out/metrics-n$i.txt" || true
done
wait "$load" 2>/dev/null || true

for n in "${names[@]}"; do
  echo "== $n"
  grep -E '^\s+#|latency|out of order' "$out/check-$n.txt" || true
done
python3 tests/e2e/cluster_report.py "$out" || rc=1
tail -3 "$out/load.log"
echo "e2e-cluster: $( [ $rc = 0 ] && echo PASS || echo FAIL ) in $(($(date +%s) - t0))s ($out)"
[ "${KEEP:-}" = 1 ] || { rm -rf "${TMPDIR:-/tmp}/vlrelay-e2e-cluster-last" && cp -r "$out" "${TMPDIR:-/tmp}/vlrelay-e2e-cluster-last"; } 2>/dev/null || true
exit $rc
