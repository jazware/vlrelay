#!/usr/bin/env bash
# The quorum log's chaos harness (docs/quorum.md, "Implementation notes"):
# three `qlog node` processes on a local MinIO, each restarted by a
# supervisor when it exits, peers dialing each other through a fault proxy
# (tests/chaos/proxy.py, one route per direction so a partition is exact),
# a load generator at --rate, and `qlog check` consuming every node's
# subscribeRepos with the emission checker.
#
#   tests/qlog/chaos.sh SCENARIO [--rate 350] [--duration 90] [--pad 5200] [--every 15]
#   tests/qlog/chaos.sh list
#
# Env: QLOG_BASE (3150: ports B+1..3 peer, B+11..13 http, B+29 proxy
# control, B+30..38 proxy routes, B+40 MinIO), PROXY (1; 0 dials peers
# directly), COMMITLOG (1: each node keeps a commitlog under OUT, and
# SIGUSR1 is a power cut; 0: memory only), CL_DIR (where the commitlogs
# go, OUT by default: /dev/shm for a device with no fsync cost),
# DISK_RETAIN_MB, FSYNC_DELAY_US (emulates a slower device), RETAIN_MB,
# RING_MB (the firehose ring: small, and old cursors come from the bucket),
# QLOG_PROFILE (dev-release), QLOG_NO_BUILD=1, RESTART_SEC (1),
# OUT (dev/state-qlog-$B/<scenario>), KEEP=1 (leave MinIO up),
# FLUSH_MS (2000; 0: no flush), HEADROOM (H, 100M), FLUSH_SEGMENT_MB (64),
# CRASH_AT/CRASH_PROB (die at a flush step: fenced, sealed, segment,
# before-manifest, after-manifest, any; or mid-trim), VERIFY_EVERY (20 s).
#
# With the flush on, `qlog verify` checks the manifest (segments, state
# and cursors at one F) every VERIFY_EVERY seconds and at the end, and the
# checker ends with a consumer from cursor 0 through the bucket backfill.
# flush-crash dies at a random flush step (CRASH_AT=any, CRASH_PROB 0.05
# a step);
# mid-trim dies between commitlog segment deletions (small disk budget);
# mixed-flush is mixed-durable with flush crashes on top.
#
# kill-two kills the leader and a follower at once, kill-all every node;
# power-cut-* also makes each victim's commitlog lose a random part of what
# it wrote since its last fsync, ending in a torn record (COMMITLOG=1 only).
#
# Bucket recovery (Phase 4): wipe-* kill -9 nodes and delete their
# commitlogs before the supervisor restarts them (held WIPE_DOWN_SEC, 0 by
# default, past its RESTART_SEC). wipe-all wipes every node; wipe-two wipes
# two, keeping the leader or a follower at random (a leader back within
# its election timeout keeps its quorum: no recovery). mixed-wipe is
# mixed-durable with wipes. single-* run one node (NODES=1, no proxy):
# single-kill, single-power-cut, single-wipe, single-mixed. The checker
# reads the manifest's gaps (the R + 1 jump) and the load's summary (every
# event sent must be emitted outside a gap); the load re-reads its hosts
# from a recovery's cursors when an ack says one happened.
set -euo pipefail
here="$(cd "$(dirname "$0")" && pwd)"
crate="$(cd "$here/../.." && pwd)"
cd "$crate"

scenarios="baseline kill-leader kill-follower partition-leader partition-follower pause-leader mixed kill-two kill-all power-cut-leader power-cut-all mixed-durable flush-crash mid-trim mixed-flush wipe-all wipe-two mixed-wipe single-kill single-power-cut single-wipe single-mixed"
scenario=${1:-}
[ -n "$scenario" ] && shift || true
if [ -z "$scenario" ] || [ "$scenario" = list ]; then
  echo "scenarios: $scenarios"
  exit 0
fi
case " $scenarios " in *" $scenario "*) ;; *) echo "qlog chaos: unknown scenario $scenario ($scenarios)" >&2; exit 1 ;; esac

rate=350 duration=90 pad=5200 every=15
while [ $# -gt 0 ]; do
  case $1 in
    --rate) rate=$2; shift 2 ;;
    --duration) duration=$2; shift 2 ;;
    --pad) pad=$2; shift 2 ;;
    --every) every=$2; shift 2 ;;
    *) echo "qlog chaos: unknown flag $1" >&2; exit 1 ;;
  esac
done

B=${QLOG_BASE:-3150}
case $scenario in single-*) NODES=1 PROXY=0 ;; esac
N=${NODES:-3}
ids=$(seq 1 "$N")
peer() { echo $((B + $1)); }
http() { echo $((B + 10 + $1)); }
ctl=$((B + 29))
route() { echo $((B + 30 + 3 * ($1 - 1) + $2 - 1)); } # i dials j here
minio=$((B + 40))
proxy=${PROXY:-1}
export COMPOSE_PROJECT_NAME=vlrq-chaos-$B MINIO_PORT=$minio

profile=${QLOG_PROFILE:-dev-release}
target=${CARGO_TARGET_DIR:-target}/$([ "$profile" = dev ] && echo debug || echo "$profile")
bin=$target/qlog
[ "${QLOG_NO_BUILD:-}" = 1 ] || cargo build --quiet --profile "$profile" --bin qlog

out=${OUT:-$crate/dev/state-qlog-$B/$scenario}
rm -rf "$out"
mkdir -p "$out"
prefix="qlog-$scenario-$(date +%s)"
ms() { date +%s%3N; }
log() { echo "$(ms) $*" | tee -a "$out/events.log"; }

pids=()
cleanup() {
  touch "$out/stop" 2>/dev/null || true
  for i in $ids; do pkill -CONT -f "^$bin node --id n$i " 2>/dev/null || true; done
  for p in "${pids[@]}"; do kill "$p" 2>/dev/null || true; done
  for i in $ids; do pkill -9 -f "^$bin node --id n$i " 2>/dev/null || true; done
  wait 2>/dev/null || true
  [ "$cl_dir" = "$out" ] || rm -rf "$cl_dir"
  [ "${KEEP:-}" = 1 ] || docker compose -f "$here/compose.yml" down -v >/dev/null 2>&1 || true
}
trap cleanup EXIT

docker compose -f "$here/compose.yml" up -d --wait minio >/dev/null
docker compose -f "$here/compose.yml" run --rm minio-init >/dev/null

if [ "$proxy" = 1 ]; then
  routes=()
  for i in $ids; do for j in $ids; do
    [ "$i" = "$j" ] || routes+=(--route "r$i$j:$(route "$i" "$j"):$(peer "$j")")
  done; done
  ulimit -n 65536 2>/dev/null || true
  python3 "$crate/tests/chaos/proxy.py" --control "$ctl" "${routes[@]}" >"$out/proxy.log" 2>&1 &
  pids+=($!)
fi

commitlog=${COMMITLOG:-1}
flush_ms=${FLUSH_MS:-2000}
crash_at=${CRASH_AT:-} crash_prob=${CRASH_PROB:-0.05}
case $scenario in
  flush-crash | mixed-flush) crash_at=${crash_at:-any} ;;
  mid-trim)
    crash_at=mid-trim crash_prob=${CRASH_PROB:-0.02}
    DISK_RETAIN_MB=${DISK_RETAIN_MB:-32}
    seg_mb=8
    ;;
esac
cl_dir=${CL_DIR:-$out}
[ "$cl_dir" = "$out" ] || { rm -rf "$cl_dir"; mkdir -p "$cl_dir"; }
supervise() {
  local i=$1 peers=() disk=()
  [ "$commitlog" = 1 ] && disk=(--commitlog "$cl_dir/cl-n$i" --power-cut-on-usr1)
  [ -n "${RETAIN_MB:-}" ] && disk+=(--retain-mb "$RETAIN_MB")
  [ -n "${RING_MB:-}" ] && disk+=(--ring-mb "$RING_MB")
  [ -n "${DISK_RETAIN_MB:-}" ] && disk+=(--disk-retain-mb "$DISK_RETAIN_MB")
  [ -n "${FSYNC_DELAY_US:-}" ] && disk+=(--fsync-delay-us "$FSYNC_DELAY_US")
  [ -n "${seg_mb:-}" ] && disk+=(--segment-mb "$seg_mb")
  disk+=(--flush-ms "$flush_ms" --headroom "${HEADROOM:-100000000}" --flush-segment-mb "${FLUSH_SEGMENT_MB:-64}")
  [ -n "$crash_at" ] && disk+=(--crash-at "$crash_at" --crash-prob "$crash_prob" --crash-stop-file "$out/no-more-crashes")
  for j in $ids; do
    [ "$i" = "$j" ] && continue
    if [ "$proxy" = 1 ]; then peers+=(--peer "n$j=127.0.0.1:$(route "$i" "$j")"); else peers+=(--peer "n$j=127.0.0.1:$(peer "$j")"); fi
  done
  while [ ! -e "$out/stop" ]; do
    set +e
    "$bin" node --id "n$i" --listen "127.0.0.1:$(peer "$i")" --http "127.0.0.1:$(http "$i")" "${peers[@]}" \
      --s3-endpoint "http://127.0.0.1:$minio" --prefix "$prefix" "${disk[@]}" >>"$out/n$i.log" 2>&1
    local rc=$?
    set -e
    echo "$(ms) exit n$i $rc" >>"$out/events.log"
    [ -e "$out/stop" ] && break
    sleep "${RESTART_SEC:-1}"
    # a wipe holds the restart until the disk is gone
    while [ -e "$out/hold-n$i" ] && [ ! -e "$out/stop" ]; do sleep 0.05; done
  done
}
for i in $ids; do supervise "$i" & pids+=($!); done

status() { curl -sf --max-time 1 "http://127.0.0.1:$(http "$1")/qlog/status" || echo '{}'; }
leader() {
  for i in $ids; do
    if status "$i" | python3 -c 'import json,sys; sys.exit(0 if json.load(sys.stdin).get("role")=="leader" else 1)' 2>/dev/null; then
      echo "$i"; return
    fi
  done
  echo ""
}
follower() {
  local l
  l=$(leader)
  for i in $ids; do [ "$i" != "$l" ] && { echo "$i"; return; }; done
}
nodepid() { pgrep -f "^$bin node --id n$1 " | head -1; }

t=0
until [ -n "$(leader)" ]; do
  sleep 0.2; t=$((t + 1))
  [ $t -lt 100 ] || { echo "qlog chaos: no leader in 20 s" >&2; exit 1; }
done
log "leader n$(leader)"

nodes=() https=()
for i in $ids; do nodes+=(--node "n$i=127.0.0.1:$(peer "$i")"); https+=(--node "n$i=127.0.0.1:$(http "$i")"); done
# with no flush there's no bucket: a cursor older than the ring is outdated
s3=(--s3-endpoint "http://127.0.0.1:$minio" --prefix "$prefix")
"$bin" check "${https[@]}" --backfill "$([ "$flush_ms" != 0 ] && echo true || echo false)" --gap-ms "${GAP_MS:-15}" --stop-file "$out/stop" --acked "$out/acked.txt" --out "$out" \
  "${s3[@]}" --load-summary "$out/load.json" >"$out/check.log" 2>&1 &
checker=$!
sleep 1
"$bin" load "${nodes[@]}" --rate "$rate" --pad "$pad" --duration "$duration" --acked "$out/acked.txt" --out "$out/load.json" --run "$scenario" >"$out/load.log" 2>&1 &
loader=$!
pids+=($loader)

if [ "$flush_ms" != 0 ]; then
  (
    while kill -0 "$loader" 2>/dev/null; do
      sleep "${VERIFY_EVERY:-20}"
      "$bin" verify "${s3[@]}" >>"$out/verify.jsonl" 2>>"$out/verify.log" || echo "$(ms) verify FAILED" >>"$out/events.log"
    done
  ) &
  pids+=($!)
fi

# CPU and RSS per node every 5 s
(
  while kill -0 "$loader" 2>/dev/null; do
    for i in $ids; do
      p=$(nodepid "$i" || true)
      [ -n "$p" ] && echo "$(ms) n$i $(awk '{print $14+$15}' "/proc/$p/stat" 2>/dev/null) $(awk '/VmRSS/{print $2}' "/proc/$p/status" 2>/dev/null)" >>"$out/resources.log"
    done
    sleep 5
  done
) &
pids+=($!)

isolate() {
  local k=$1
  for j in $ids; do
    [ "$j" = "$k" ] && continue
    curl -sf -X POST "127.0.0.1:$ctl/r$k$j" -d '{"blackhole": true}' >/dev/null
    curl -sf -X POST "127.0.0.1:$ctl/r$j$k" -d '{"blackhole": true}' >/dev/null
  done
}
heal() {
  for i in $ids; do for j in $ids; do
    [ "$i" = "$j" ] || curl -sf -X POST "127.0.0.1:$ctl/r$i$j" -d '{}' >/dev/null
  done; done
}

# kill -9 and the disk gone: the supervisor restarts each on an empty one
wipe() {
  local kind=$1 v p ps=()
  shift
  for v in "$@"; do touch "$out/hold-n$v"; done
  for v in "$@"; do p=$(nodepid "$v" || true); [ -n "$p" ] && ps+=("$p"); log "kill9 n$v $kind"; done
  [ ${#ps[@]} -gt 0 ] && kill -9 "${ps[@]}" 2>/dev/null || true
  for v in "$@"; do
    while [ -n "$(nodepid "$v" || true)" ]; do sleep 0.02; done
    rm -rf "$cl_dir/cl-n$v"
  done
  log "wiped $* $kind"
  sleep "${WIPE_DOWN_SEC:-0}"
  for v in "$@"; do rm -f "$out/hold-n$v"; done
}

fault() {
  local kind=$1 who
  case $kind in
    kill-leader | partition-leader | pause-leader | power-cut-leader | kill-two | wipe-two) who=$(leader) ;;
    single-*) who=1 ;;
    *) who=$(follower) ;;
  esac
  [ -n "$who" ] || { log "skip $kind: no leader"; return; }
  case $kind in
    wipe-all) wipe "$kind" $ids ;;
    wipe-two)
      # keep the leader or a follower, at random
      local keep=$who v vs=()
      [ $((RANDOM % 2)) = 0 ] && keep=$(follower)
      for v in $ids; do [ "$v" != "$keep" ] && vs+=("$v"); done
      log "survivor n$keep"
      wipe "$kind" "${vs[@]}"
      ;;
    single-wipe) wipe "$kind" 1 ;;
    single-kill | single-power-cut)
      local p
      p=$(nodepid 1 || true)
      [ -n "$p" ] || { log "skip $kind: n1 is restarting"; return; }
      if [ "$kind" = single-kill ]; then log "kill9 n1 $kind"; kill -9 "$p"; else log "powercut n1 $kind"; kill -USR1 "$p"; fi
      ;;
    kill-two | kill-all)
      local victims=$who
      if [ "$kind" = kill-all ]; then victims="1 2 3"; else for j in 1 2 3; do [ "$j" != "$who" ] && { victims="$who $j"; break; }; done; fi
      local ps=()
      # a node the supervisor is restarting (a flush crash) has no pid
      for v in $victims; do p=$(nodepid "$v" || true); [ -n "$p" ] && { ps+=("$p"); log "kill9 n$v $kind"; }; done
      [ ${#ps[@]} -gt 0 ] && kill -9 "${ps[@]}" || true
      ;;
    power-cut-leader | power-cut-all)
      local victims=$who ps=()
      [ "$kind" = power-cut-all ] && victims="1 2 3"
      for v in $victims; do p=$(nodepid "$v" || true); [ -n "$p" ] && { ps+=("$p"); log "powercut n$v $kind"; }; done
      [ ${#ps[@]} -gt 0 ] && kill -USR1 "${ps[@]}" || true
      ;;
    kill-*)
      local p
      p=$(nodepid "$who" || true)
      [ -n "$p" ] || { log "skip $kind: n$who is restarting"; return; }
      log "kill9 n$who $kind"
      kill -9 "$p" 2>/dev/null || true
      ;;
    partition-*)
      log "isolate n$who $kind"
      isolate "$who"
      sleep 5
      heal
      log "heal n$who"
      ;;
    pause-leader)
      local p
      p=$(nodepid "$who" || true)
      [ -n "$p" ] || { log "skip $kind: n$who is restarting"; return; }
      log "stop n$who $kind"
      kill -STOP "$p" 2>/dev/null || true
      sleep 3
      kill -CONT "$p" 2>/dev/null || true
      log "cont n$who"
      ;;
  esac
}

kinds="kill-leader kill-follower partition-leader partition-follower pause-leader"
wipe_kinds="kill-leader kill-two kill-all power-cut-all wipe-all wipe-two wipe-two partition-leader"
single_kinds="single-kill single-power-cut single-power-cut single-wipe"
durable_kinds="kill-leader kill-follower kill-two kill-all power-cut-leader power-cut-all partition-leader pause-leader"
start=$(date +%s)
sleep "$every"
while [ $(($(date +%s) - start + 12)) -lt "$duration" ] && [ "$scenario" != baseline ]; do
  case $scenario in
    mixed) set -- $kinds; shift $((RANDOM % 5)); k=$1 ;;
    mixed-durable | mixed-flush) set -- $durable_kinds; shift $((RANDOM % 8)); k=$1 ;;
    mixed-wipe) set -- $wipe_kinds; shift $((RANDOM % 8)); k=$1 ;;
    single-mixed) set -- $single_kinds; shift $((RANDOM % 4)); k=$1 ;;
    flush-crash | mid-trim) k=none ;;
    *) k=$scenario ;;
  esac
  if [ "$k" = none ]; then :; elif [ "$proxy" != 1 ] && [[ $k == partition-* ]]; then log "skip $k: PROXY=0"; else fault "$k"; fi
  # let a killed node come back and catch up before the next fault
  sleep "$every"
done

wait "$loader" || true
touch "$out/no-more-crashes"
if [ "$flush_ms" != 0 ]; then
  # the last flush covers everything committed (no more injected crashes:
  # a node restarted by the supervisor gets the same flags, so give it time)
  sleep $(( (flush_ms / 1000) + 3 ))
fi
for i in $ids; do status "$i" >"$out/status-n$i.json"; done
touch "$out/stop"
set +e
wait "$checker"
rc=$?
if [ "$flush_ms" != 0 ]; then
  "$bin" verify "${s3[@]}" >"$out/verify.json" 2>>"$out/verify.log"
  vrc=$?
  [ $vrc = 0 ] || { echo "qlog chaos: the final manifest is inconsistent" >&2; rc=1; }
  grep -q FAILED "$out/events.log" && { echo "qlog chaos: a mid-run verify failed" >&2; rc=1; }
  "$bin" retain "${s3[@]}" --horizon-secs "${RETAIN_HORIZON_SEC:-30}" >"$out/retain.json" 2>>"$out/verify.log" || true
fi
set -e
python3 "$here/report.py" "$out" | tee "$out/report.txt"
exit $rc
