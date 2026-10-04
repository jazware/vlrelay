#!/usr/bin/env bash
# The chaos harness (docs/chaos.md): the cluster e2e's three cores, edge and
# replica on MinIO, under steady load from devnet (vlpds + reference PDS) and
# a fakepds fleet, with a fault proxy in front of MinIO and every peer port,
# a supervisor that restarts any node that exits (systemd Restart=always),
# one checker per stream, and a fault schedule per scenario.
#
#   tests/chaos/chaos.sh SCENARIO [--duration 100] [--rate 50] [--fake-rate 200] [--accounts 30]
#   tests/chaos/chaos.sh list
#
# Env: CHAOS_BASE (3700: port block, see ports below), KEEP=1, OUT
# (dev/state-chaos-$CHAOS_BASE/run), TTL_MS (3000), HOST_SHARDS (15),
# RELAY_MEM_MB (3072 per node), RESTART_SEC (1: supervisor delay),
# FAKE_HOSTS (3), RETENTION_H (72), SOAK_EVERY (180: soak's fault interval),
# VLRELAY_BIN (another relay build, e.g. one from before a fix).
set -euo pipefail
here="$(cd "$(dirname "$0")" && pwd)"
crate="$(cd "$here/../.." && pwd)"
cd "$crate"

scenarios="baseline kill9 term double-crash crash-loop zombie gc-pause minio-latency minio-errors minio-pause node-bucket-hang partition-peer isolate upstream-faults upstream-restart consumers clock-skew soak"
scenario=${1:-}
[ -n "$scenario" ] && shift || true
if [ -z "$scenario" ] || [ "$scenario" = list ]; then
  echo "scenarios: $scenarios"
  exit 0
fi
case " $scenarios " in *" $scenario "*) ;; *) echo "chaos: unknown scenario $scenario ($scenarios)" >&2; exit 1 ;; esac

duration=100 rate=50 fake_rate=200 accounts=30
while [ $# -gt 0 ]; do
  case $1 in
    --duration) duration=$2; shift 2 ;;
    --rate) rate=$2; shift 2 ;;
    --fake-rate) fake_rate=$2; shift 2 ;;
    --accounts) accounts=$2; shift 2 ;;
    *) echo "chaos: unknown flag $1" >&2; exit 1 ;;
  esac
done

# ---- ports: one block per CHAOS_BASE, clear of dev (29xx), the cluster e2e (31xx)
# and the other workstreams' runs (33xx, 34xx)
B=${CHAOS_BASE:-3700}
export COMPOSE_PROJECT_NAME=vlrelay-chaos-$B
export PLC_PORT=$((B + 82)) REF_PDS_PORT=$((B + 83)) PDS_BASE_PORT=$((B + 84))
export MINIO_PORT=$((B + 90)) MINIO_CONSOLE_PORT=$((B + 91)) RELAY_PORT=$((B + 80)) RELAY_METRICS_PORT=$((B + 81))
export DEV_STATE="$crate/dev/state-chaos-$B"
pub() { echo $((B + 60 + $1)); }       # node i's public port (cores 1-3, edge 4, replica 5)
peer() { echo $((B + 70 + $1)); }      # node i's peer listener
peerx() { echo $((B + 40 + $1)); }     # the proxy in front of it (what peers dial)
s3x() { echo $((B + 50 + $1)); }       # node i's own proxy to MinIO
ctl=$((B + 59))
fake_base=$((B + 100)) fake_plc=$((B + 99))
fake_hosts=${FAKE_HOSTS:-3}

out=${OUT:-$DEV_STATE/run}
target=${CARGO_TARGET_DIR:-target}/debug
relay_bin=${VLRELAY_BIN:-$target/vlrelay}
ttl=${TTL_MS:-3000}
t0=$(date +%s)
cargo build --quiet --bin vlrelay --bin e2e_check --bin devnet --bin fakepds
echo "chaos[$scenario]: built in $(($(date +%s) - t0))s"

ms() { python3 -c 'import time; print(int(time.time()*1000))'; }
prefix=""
vpid() { [ -n "$prefix" ] && pgrep -f "^$relay_bin --role [a-z]+ --node-id n$1 .*--prefix $prefix" | head -1; }
pids=()
cleanup() {
  touch "$out/stop" 2>/dev/null || true
  for i in 1 2 3 4 5; do p=$(vpid "$i" || true); [ -n "$p" ] && kill -CONT "$p" 2>/dev/null; done
  for p in "${pids[@]}"; do
    pkill -P "$p" 2>/dev/null || true
    kill "$p" 2>/dev/null || true
  done
  for i in 1 2 3 4 5; do p=$(vpid "$i" || true); [ -n "$p" ] && kill -9 "$p" 2>/dev/null; done
  docker compose -f dev/docker-compose.yml unpause minio >/dev/null 2>&1 || true
  wait 2>/dev/null || true
  if [ "${KEEP:-}" = 1 ]; then
    echo "chaos: KEEP=1: network left up (DEV_STATE=$DEV_STATE dev/down.sh)"
  else
    dev/down.sh >/dev/null
  fi
}
trap cleanup EXIT

# a soak outgrows MinIO's tmpfs (it counts against the 1 GB cap)
[ "$scenario" = soak ] && export DEV_COMPOSE_EXTRA=${DEV_COMPOSE_EXTRA:-$here/minio-disk.yml}
DEV_PDS=${DEV_PDS:-3} dev/up.sh | sed 's/^/  /'
rm -rf "$out" && mkdir -p "$out"
accts="$DEV_STATE/accounts.json"
"$target/devnet" --accounts-file "$accts" seed --accounts "$accounts" $(sed 's/^/--host /' "$DEV_STATE/hosts" | tr '\n' ' ') >/dev/null

# ---- the fault proxy: a route per node to MinIO, one per core's peer port
routes=""
for i in 1 2 3 4 5; do routes="$routes --route s3n$i:$(s3x "$i"):$MINIO_PORT"; done
for i in 1 2 3; do routes="$routes --route peer$i:$(peerx "$i"):$(peer "$i")"; done
python3 "$here/proxy.py" --control "$ctl" $routes >"$out/proxy.log" 2>&1 &
pids+=($!)
for _ in $(seq 1 50); do curl -sf "http://127.0.0.1:$ctl/" >/dev/null && break; sleep 0.1; done
fault() { curl -sf -X POST "http://127.0.0.1:$ctl/$1" -d "${2:-{\}}" >/dev/null; }

# ---- fakepds: its hosts, and the one PLC the relays use (it hands every
# DID that isn't the fleet's to the docker PLC)
fake_faults=()
case $scenario in
  upstream-faults) fake_faults=(--fault "disconnect:0:every=25,down=4" --fault "stall:1:secs=6,every=30" --fault "replay:2:every=20,count=60") ;;
  upstream-restart) fake_faults=(--fault "restart:0:every=35") ;;
  soak) fake_faults=(--fault "disconnect:0:every=600,down=5" --fault "stall:1:secs=6,every=420" --fault "replay:2:every=300,count=100" --fault "restart:0:every=1500") ;;
esac
"$target/fakepds" run --seed "chaos$B" --port-base "$fake_base" --hosts "$fake_hosts" --dids 100 --rate "$fake_rate" \
  --gen-threads 2 --plc-port "$fake_plc" --plc-fallback "http://127.0.0.1:$PLC_PORT" --replay-mb 64 --lag-secs 5 \
  --initial-records 20 --target-records 60 --stats-secs 30 "${fake_faults[@]}" >"$out/fakepds.log" 2>&1 &
pids+=($!)
for _ in $(seq 1 300); do grep -q READY "$out/fakepds.log" && break; sleep 0.2; done
grep -q READY "$out/fakepds.log" || { echo "chaos: fakepds didn't come up" >&2; tail "$out/fakepds.log" >&2; exit 1; }
cp "$DEV_STATE/hosts" "$out/upstreams"
for g in $(seq 0 $((fake_hosts - 1))); do echo "http://127.0.0.1:$((fake_base + g))" >>"$out/upstreams"; done

prefix="chaos-$scenario-$(date +%s)-$$"
echo "$prefix" >"$out/prefix"
creds="--s3-bucket vlrelay --s3-access-key minioadmin --s3-secret-key minioadmin --prefix $prefix"
common="--plc-url http://127.0.0.1:$fake_plc --linger-ms 25 --dev-mode --internal-token chaos-token --peer-tls-dir $DEV_STATE/peer-tls --lease-ttl-ms $ttl --did-shards 8 --host-shards ${HOST_SHARDS:-15} --retention ${RETENTION_H:-72}"
hosts=$(sed 's/^/--host /' "$out/upstreams" | tr '\n' ' ')
roles=(x core core core edge replica)

# A node runs under a supervisor loop: it's restarted RESTART_SEC after any
# exit, unless $out/hold-n$i exists (a scenario keeping it down).
supervise() { # i
  local i=$1 role=${roles[$1]} gen=0
  local skew=""
  [ -f "$out/skew-n$i" ] && skew=$(cat "$out/skew-n$i")
  while [ ! -f "$out/stop" ]; do
    while [ -f "$out/hold-n$i" ] && [ ! -f "$out/stop" ]; do sleep 0.2; done
    [ -f "$out/stop" ] && break
    gen=$((gen + 1))
    echo "== start n$i gen $gen $(ms)" >>"$out/n$i.log"
    set +e
    dev/capped.sh "${RELAY_MEM_MB:-3072}" "$relay_bin" --role "$role" --node-id "n$i" \
      --listen "127.0.0.1:$(pub "$i")" --peer-listen "127.0.0.1:$(peer "$i")" \
      --advertise-url "https://127.0.0.1:$(peerx "$i")" --s3-endpoint "http://127.0.0.1:$(s3x "$i")" \
      $creds $common $hosts >>"$out/n$i.log" 2>&1
    rc=$?
    set -e
    echo "exit n$i $(ms) $rc" >>"$out/exits.txt"
    echo "== exit n$i rc $rc $(ms)" >>"$out/n$i.log"
    sleep "${RESTART_SEC:-1}"
  done
}
healthy() { curl -sf -m 1 "http://127.0.0.1:$(pub "$1")/xrpc/_health" >/dev/null; }
wait_up() { # i [secs]
  for _ in $(seq 1 $((${2:-30} * 5))); do healthy "$1" && return 0; sleep 0.2; done
  return 1
}
for i in 1 2 3 4 5; do
  supervise "$i" &
  pids+=($!)
  wait_up "$i" 40 || { echo "chaos: n$i (${roles[$i]}) didn't come up" >&2; tail -30 "$out/n$i.log" >&2; exit 1; }
done
echo "chaos[$scenario]: 3 cores, an edge and a replica up on :$(pub 1)-:$(pub 5), prefix $prefix"
sleep 3

# ---- checkers, one per stream
up_flags=$(sed 's/^/--upstream /' "$out/upstreams" | tr '\n' ' ')
u() { echo "http://127.0.0.1:$(pub "$1")"; }
streams=("$(u 1),$(u 2),$(u 3)" "$(u 2),$(u 3),$(u 1)" "$(u 3),$(u 1),$(u 2)" "$(u 4)" "$(u 5)")
names=(core1 core2 core3 edge replica)
checks=()
settle=${SETTLE:-25}
for k in "${!streams[@]}"; do
  n=${names[$k]}
  "$target/e2e_check" $up_flags --relay "${streams[$k]}" --duration "$duration" --warmup 3 --settle "$settle" --report-secs 30 \
    --json-out "$out/report-$n.json" --seq-out "$out/seqs-$n.txt" --lat-out "$out/lat-$n.txt" \
    >"$out/check-$n.txt" 2>"$out/check-$n.log" &
  checks+=($!)
done
sleep 2
t_load=$(ms)
echo "$t_load" >"$out/t_load_ms"
"$target/devnet" --accounts-file "$accts" load --rate "$rate" --duration "$((duration + 2))" --identity-every 10 --deactivate-every 20 2>"$out/load.log" &
load=$!
pids+=($load)

# ---- the fault vocabulary
mark() { echo "$1 $2 $(ms)" >>"$out/events.txt"; echo "chaos[$scenario]: $(( ($(ms) - t_load) / 1000 ))s $1 $2"; }
at() { local now=$(( ($(ms) - t_load) / 1000 )); [ "$1" -gt "$now" ] && sleep $(( $1 - now )); return 0; }
connected() {
  local c
  c=$(curl -sf -m 2 "http://127.0.0.1:$(pub "$1")/metrics" | awk '/^vlrelay_hosts\{status="connected"\}/ {print $2}') || true
  echo "${c:-0}"
}
busiest() { local best=1 n=-1 c; for i in "$@"; do c=$(connected "$i"); [ "$c" -gt "$n" ] && { n=$c; best=$i; }; done; echo "$best"; }
rand_core() { echo $(( (RANDOM % 3) + 1 )); }
kill9() { local p; p=$(vpid "$1"); [ -n "$p" ] && kill -9 "$p"; mark kill9 "n$1"; }
term() { local p; p=$(vpid "$1"); [ -n "$p" ] && kill -TERM "$p"; mark term "n$1"; }
hold() { touch "$out/hold-n$1"; }
release() { rm -f "$out/hold-n$1"; }
down_for() { # i secs kind: take a node down and keep it down for secs
  hold "$1"
  if [ "$3" = term ]; then term "$1"; else kill9 "$1"; fi
  ( sleep "$2"; release "$1" ) &
}
stopn() { local p; p=$(vpid "$1"); echo "$p" >"$out/stopped-n$1"; kill -STOP "$p"; mark sigstop "n$1"; }
contn() { kill -CONT "$(cat "$out/stopped-n$1")" 2>/dev/null || true; mark sigcont "n$1"; }
bucket_all() { for i in 1 2 3 4 5; do fault "s3n$i" "$1"; done; }
heal_all() { for i in 1 2 3 4 5; do fault "s3n$i"; done; for i in 1 2 3; do fault "peer$i"; done; }

# resource samples (RSS, fds, bucket objects) every 60 s until the checkers finish
sample() {
  local t
  t=$(ms)
  for i in 1 2 3 4 5; do
    local p; p=$(vpid "$i")
    [ -n "$p" ] || continue
    local rss fds
    rss=$(ps -o rss= -p "$p" 2>/dev/null | tr -d ' ')
    fds=$(lsof -p "$p" 2>/dev/null | wc -l | tr -d ' ')
    echo "$t n$i $p ${rss:-0} ${fds:-0}" >>"$out/resources.txt"
  done
  docker stats --no-stream --format '{{.MemUsage}}' "$COMPOSE_PROJECT_NAME-minio-1" 2>/dev/null | sed "s/^/$t minio /" >>"$out/minio-mem.txt" || true
  docker compose -f dev/docker-compose.yml exec -T minio sh -c \
    "mc alias set l http://localhost:9000 minioadmin minioadmin >/dev/null 2>&1; mc ls --recursive l/vlrelay/$prefix/ 2>/dev/null" |
    awk -v t="$t" '{p=$NF; split(p, a, "/"); k=a[1]; if (k=="log") k="log/" a[2]; n[k]++; s[k]+=0} END {for (k in n) print t, k, n[k]}' >>"$out/objects.txt" || true
}
( while [ ! -f "$out/stop" ] && [ ! -f "$out/sampled" ]; do sample; sleep 60; done ) &
sampler=$!
pids+=($sampler)

# ---- the scenarios (seconds into the load)
f0=15
case $scenario in
  baseline) ;;
  kill9)
    at $f0; k=$(busiest 1 2 3); down_for "$k" 10 kill; at $((f0 + 30))
    k=$(rand_core); kill9 "$k"; at $((f0 + 55)); k=$(busiest 1 2 3); kill9 "$k" ;;
  term)
    at $f0; k=$(busiest 1 2 3); down_for "$k" 10 term; at $((f0 + 30)); term "$(rand_core)"; at $((f0 + 55)); term "$(busiest 1 2 3)" ;;
  double-crash)
    # the second crash lands within a checkpoint interval of the first's takeover
    at $f0; a=$(busiest 1 2 3); kill9 "$a"; sleep 2.5
    b=$(( a % 3 + 1 )); kill9 "$b"; at $((f0 + 35))
    a=$(rand_core); b=$(( a % 3 + 1 )); kill9 "$a"; sleep 1; kill9 "$b" ;;
  crash-loop)
    # 20 kill -9s in a row, a random core every 6 s: every takeover adds a
    # span to the shards' histories unless the new owner trims them
    for n in $(seq 1 20); do at $((f0 + (n - 1) * 6)); kill9 "$(rand_core)"; done ;;
  zombie)
    # SIGSTOP well past TTL + skew: peers take its shards; on SIGCONT it must
    # neither emit nor ack, and should fail-stop (the supervisor restarts it)
    at $f0; k=$(busiest 1 2 3); stopn "$k"; sleep $(( ttl * 4 / 1000 )); contn "$k"
    at $((f0 + 35)); k=$(rand_core); stopn "$k"; sleep $(( ttl * 3 / 1000 )); contn "$k" ;;
  gc-pause)
    # shorter than the TTL: nothing should move
    at $f0; k=$(busiest 1 2 3); stopn "$k"; sleep "$(python3 -c "print($ttl*0.5/1000)")"; contn "$k"
    at $((f0 + 20)); k=$(rand_core); stopn "$k"; sleep "$(python3 -c "print($ttl*0.9/1000)")"; contn "$k"
    at $((f0 + 40)); k=$(rand_core); stopn "$k"; sleep "$(python3 -c "print($ttl*1.5/1000)")"; contn "$k" ;;
  minio-latency)
    at $f0; mark bucket-latency all; bucket_all '{"latency":[100,500]}'
    at $((f0 + 30)); mark heal all; heal_all
    at $((f0 + 40)); k=$(busiest 1 2 3); mark bucket-latency "n$k"; fault "s3n$k" '{"latency":[300,1500]}'
    at $((f0 + 60)); mark heal all; heal_all ;;
  minio-errors)
    at $f0; mark bucket-resets all; bucket_all '{"reset":0.05}'
    at $((f0 + 25)); mark heal all; heal_all
    at $((f0 + 35)); mark bucket-timeouts all; bucket_all '{"timeout":0.2,"reset":0.02}'
    at $((f0 + 60)); mark heal all; heal_all ;;
  minio-pause)
    # docker pause: every node's bucket hangs at once. Short, then past the TTL.
    at $f0; mark minio-pause 2s; docker compose -f dev/docker-compose.yml pause minio >/dev/null; sleep 2
    docker compose -f dev/docker-compose.yml unpause minio >/dev/null; mark heal all
    at $((f0 + 25)); mark minio-pause 10s; docker compose -f dev/docker-compose.yml pause minio >/dev/null; sleep 10
    docker compose -f dev/docker-compose.yml unpause minio >/dev/null; mark heal all ;;
  node-bucket-hang)
    # one core's bucket path blackholed (a hung disk): its lease lapses
    at $f0; k=$(busiest 1 2 3); mark bucket-hang "n$k"; fault "s3n$k" '{"blackhole":true}'
    at $((f0 + 15)); mark heal all; heal_all
    at $((f0 + 35)); k=$(rand_core); mark bucket-refuse "n$k"; fault "s3n$k" '{"refuse":true}'
    at $((f0 + 45)); mark heal all; heal_all ;;
  partition-peer)
    # nobody can reach one core's peer port (it can still reach the others and the bucket)
    at $f0; k=$(busiest 1 2 3); mark peer-blackhole "n$k"; fault "peer$k" '{"blackhole":true}'
    at $((f0 + 20)); mark heal all; heal_all
    at $((f0 + 35)); k=$(rand_core); mark peer-refuse "n$k"; fault "peer$k" '{"refuse":true}'
    at $((f0 + 50)); mark heal all; heal_all ;;
  isolate)
    # one core cut off from its peers and the bucket, then healed
    at $f0; k=$(busiest 1 2 3); mark isolate "n$k"; fault "peer$k" '{"blackhole":true}'; fault "s3n$k" '{"blackhole":true}'
    at $((f0 + 20)); mark heal all; heal_all ;;
  upstream-faults|upstream-restart) ;;
  consumers)
    at $f0; mark consumers slow+storm
    python3 "$here/consumers.py" --url "$(u 1)" --url "$(u 2)" --url "$(u 4)" --url "$(u 5)" --slow 20 --storm 100 --secs 40 >"$out/consumers.txt" 2>&1 || true
    mark consumers-done all ;;
  clock-skew)
    echo "chaos: clock-skew: no clock shim on this host (see docs/chaos.md); running baseline" ;;
  soak)
    # a random fault every SOAK_EVERY seconds until the end
    every=${SOAK_EVERY:-180}
    kinds=(kill9 term double zombie gcpause bucketlat bucketerr partition)
    n=0
    while :; do
      n=$((n + 1))
      next=$(( f0 + n * every ))
      [ "$next" -gt $((duration - 60)) ] && break
      at "$next"
      kind=${kinds[$((RANDOM % ${#kinds[@]}))]}
      k=$(rand_core)
      case $kind in
        kill9) kill9 "$k" ;;
        term) term "$k" ;;
        double) kill9 "$k"; sleep 2; kill9 $(( k % 3 + 1 )) ;;
        zombie) stopn "$k"; sleep $(( ttl * 4 / 1000 )); contn "$k" ;;
        gcpause) stopn "$k"; sleep 1; contn "$k" ;;
        bucketlat) mark bucket-latency all; bucket_all '{"latency":[100,500]}'; sleep 30; mark heal all; heal_all ;;
        bucketerr) mark bucket-resets all; bucket_all '{"reset":0.05}'; sleep 30; mark heal all; heal_all ;;
        partition) mark peer-blackhole "n$k"; fault "peer$k" '{"blackhole":true}'; sleep 15; mark heal all; heal_all ;;
      esac
    done ;;
esac

rc=0
for k in "${!checks[@]}"; do
  wait "${checks[$k]}" || echo "chaos[$scenario]: checker ${names[$k]} reported a discrepancy (report.py decides)"
done
touch "$out/sampled"
sample
for i in 1 2 3 4 5; do
  curl -sf -m 2 "http://127.0.0.1:$(pub "$i")/metrics" | grep -E '^vlrelay_|^vlpds_cluster' >"$out/metrics-n$i.txt" || true
done
# the hosts' checkpointed cursors, for acked-but-lost
docker compose -f dev/docker-compose.yml exec -T minio sh -c \
  "mc alias set l http://localhost:9000 minioadmin minioadmin >/dev/null 2>&1; for o in \$(mc ls --recursive l/vlrelay/$prefix/hostck/ | awk '{print \$NF}'); do mc cat l/vlrelay/$prefix/hostck/\$o; echo; done" >"$out/hostck.jsonl" 2>/dev/null || true
docker compose -f dev/docker-compose.yml exec -T minio sh -c \
  "mc alias set l http://localhost:9000 minioadmin minioadmin >/dev/null 2>&1; mc ls --recursive l/vlrelay/$prefix/" >"$out/bucket-ls.txt" 2>/dev/null || true
wait "$load" 2>/dev/null || true

# report.py is the verdict: the checkers and cluster_report.py also fail on
# #identity/#account occurrence counts that depend on where a socket started
python3 tests/e2e/cluster_report.py "$out" >"$out/cluster-report.txt" || true
python3 "$here/report.py" "$out" "$scenario" || rc=1
echo "chaos[$scenario]: $( [ $rc = 0 ] && echo PASS || echo FAIL ) in $(($(date +%s) - t0))s ($out)"
[ "${KEEP:-}" = 1 ] || { rm -rf "${TMPDIR:-/tmp}/vlrelay-chaos-$scenario-last" && cp -r "$out" "${TMPDIR:-/tmp}/vlrelay-chaos-$scenario-last"; } 2>/dev/null || true
exit $rc
