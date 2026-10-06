#!/usr/bin/env bash
# The relay on the quorum log under chaos (docs/quorum.md, Phase 7): a
# fakepds fleet (signed sync 1.1 commits, its own PLC) -> three
# `vlrelay` nodes on a local MinIO, each restarted by a supervisor
# when it exits, peers dialing each other through the fault proxy
# (tests/qlog/proxy.py, one route per direction) -> consumers.
#
#   tests/qlog/relay-chaos.sh SCENARIO [--rate 350] [--duration 90] [--every 15] [--hosts 16] [--dids 200]
#   tests/qlog/relay-chaos.sh list
#
# Checked at the end:
# - `qlog check --relay-frames` on every node's subscribeRepos: no seq
#   emitted with two contents across nodes and restarts, every stream
#   dense except across a bucket recovery's gap, every member's consumer at
#   the same commit, and a consumer from cursor 0 through the bucket;
# - `qlog verify`: every manifest's state (the relay's DID records, the
#   host table, cursors) equals the log replayed to F, mid-run and final;
# - `e2e_check` on the fakepds hosts against the relay (failing over between
#   nodes with its cursor): no upstream event missing. Repeats after a
#   recovery (the gap's events, sent again by their hosts) are counted, not
#   failed.
#
# down-follower and down-leader keep the node down DOWN_SEC (6), past the
# host failover (2 s): its hosts move to the others, with their cursors.
#
# Env: RQ_BASE (3550: B+1..9 peer, B+11..19 http, B+29 proxy control, B+40
# MinIO, B+45 the fake PLC, B+50.. proxy routes (up to B+130 with 9 slots), B+200.. fakepds hosts),
# FLUSH_MS (2000), HEADROOM (100M), CL_DIR (OUT: the commitlogs; /dev/shm
# for no fsync cost), FSYNC_DELAY_US, RESTART_SEC (1), QLOG_PROFILE
# (dev-release), RQ_NO_BUILD=1, KEEP=1, OUT (dev/state-relayq-$B/<scenario>),
# CRASH_AT/CRASH_PROB, VERIFY_EVERY (20), STATUS_EVERY (0), SLOTS,
# RETAIN_SECS (bucket retention's horizon in the leader's loop; off unset),
# RQ_CRATE (the crate, when this script runs from a copy), RETAIN_EVERY (60), STATE_POLL_MS (the state's compactor poll: 5000 under
# 10 s flushes, else 30000), RELAY_LOG (RUST_LOG for the relays).
#
# PLC_EXPORT=1 runs the PLC export on the leader against the fake PLC's
# /export: PLC_HOSTS (1000) hosts of --dids accounts in its history,
# PLC_RATE (4) requests a second, and every PLC_THROTTLE_EVERY'th (6)
# answered 429. The run fails unless the leader at the end has read the
# whole export (caught up), whatever the faults did to the leaders that
# read it before.
set -euo pipefail
here="$(cd "$(dirname "$0")" && pwd)"
# RQ_CRATE: a copy of this script elsewhere (so editing it can't cut a
# running one short) still runs this crate's binaries
crate="${RQ_CRATE:-$(cd "$here/../.." && pwd)}"
cd "$crate"

scenarios="baseline kill-leader kill-follower down-follower down-leader kill-two kill-all power-cut-leader power-cut-all partition-leader partition-follower pause-leader mixed-durable flush-crash mixed-flush wipe-all wipe-two mixed-wipe replace-follower replace-leader grow-shrink single-kill single-wipe"
scenario=${1:-}
[ -n "$scenario" ] && shift || true
if [ -z "$scenario" ] || [ "$scenario" = list ]; then
  echo "scenarios: $scenarios"
  exit 0
fi
case " $scenarios " in *" $scenario "*) ;; *) echo "relay chaos: unknown scenario $scenario ($scenarios)" >&2; exit 1 ;; esac

rate=350 duration=90 every=15 fake_hosts=16 dids=200
while [ $# -gt 0 ]; do
  case $1 in
    --rate) rate=$2; shift 2 ;;
    --duration) duration=$2; shift 2 ;;
    --every) every=$2; shift 2 ;;
    --hosts) fake_hosts=$2; shift 2 ;;
    --dids) dids=$2; shift 2 ;;
    *) echo "relay chaos: unknown flag $1" >&2; exit 1 ;;
  esac
done

B=${RQ_BASE:-3550}
case $scenario in single-*) NODES=1 PROXY=0 ;; esac
N=${NODES:-3}
case $scenario in replace-* | grow-shrink) SLOTS=${SLOTS:-9} ;; esac
S=${SLOTS:-$N}
ids=$(seq 1 "$N")
slots=$(seq 1 "$S")
members_flag=$(seq -s, -f 'n%g' 1 "$N")
peer() { echo $((B + $1)); }
http() { echo $((B + 10 + $1)); }
ctl=$((B + 29))
route() { echo $((B + 50 + S * ($1 - 1) + $2 - 1)); } # i dials j here
minio=$((B + 40))
fake_plc=$((B + 45))
fake_base=$((B + 200))
proxy=${PROXY:-1}
export COMPOSE_PROJECT_NAME=vlrq-relay-$B MINIO_PORT=$minio

profile=${QLOG_PROFILE:-dev-release}
target=${CARGO_TARGET_DIR:-target}/$([ "$profile" = dev ] && echo debug || echo "$profile")
bin=$target/vlrelay
qlog=$target/qlog
[ "${RQ_NO_BUILD:-}" = 1 ] || cargo build --quiet --profile "$profile" --bin vlrelay --bin qlog --bin fakepds --bin e2e_check

out=${OUT:-$crate/dev/state-relayq-$B/$scenario}
rm -rf "$out"
mkdir -p "$out"
prefix="relayq-$scenario-$(date +%s)"
ms() { date +%s%3N; }
log() { echo "$(ms) $*" | tee -a "$out/events.log"; }
nodepid() { pgrep -f "^$bin --node-id n$1 " | head -1; }

pids=()
cl_dir=${CL_DIR:-$out}
cleanup() {
  touch "$out/stop" 2>/dev/null || true
  for i in $slots; do p=$(nodepid "$i" || true); [ -n "$p" ] && kill -CONT "$p" 2>/dev/null; done
  for p in "${pids[@]}"; do kill "$p" 2>/dev/null || true; done
  for i in $slots; do pkill -9 -f "^$bin --node-id n$i " 2>/dev/null || true; done
  pkill -f "^$target/fakepds run --seed relayq$B " 2>/dev/null || true
  wait 2>/dev/null || true
  [ "$cl_dir" = "$out" ] || rm -rf "$cl_dir"
  # the commitlogs are big and the run's verdict is in the logs
  rm -rf "$out"/cl-n*
  [ "${KEEP:-}" = 1 ] || docker compose -f "$here/compose.yml" down -v >/dev/null 2>&1 || true
}
trap cleanup EXIT

docker compose -f "$here/compose.yml" up -d --wait minio >/dev/null
docker compose -f "$here/compose.yml" run --rm minio-init >/dev/null

if [ "$proxy" = 1 ]; then
  routes=()
  for i in $slots; do for j in $slots; do
    [ "$i" = "$j" ] || routes+=(--route "r$i-$j:$(route "$i" "$j"):$(peer "$j")")
  done; done
  ulimit -n 65536 2>/dev/null || true
  python3 "$crate/tests/qlog/proxy.py" --control "$ctl" "${routes[@]}" >"$out/proxy.log" 2>&1 &
  pids+=($!)
fi

# ---- the upstream fleet and its PLC
gen_threads=$(( rate > 2000 ? 8 : 2 ))
"$target/fakepds" run --seed "relayq$B" --port-base "$fake_base" --hosts "$fake_hosts" --dids "$dids" --rate "$rate" \
  --gen-threads "$gen_threads" --plc-port "$fake_plc" --replay-mb 256 --lag-secs 30 \
  --initial-records 20 --target-records 60 --stats-secs 30 \
  $([ "${PLC_EXPORT:-}" = 1 ] && echo "--plc-hosts ${PLC_HOSTS:-1000} --plc-throttle-every ${PLC_THROTTLE_EVERY:-6}") \
  >"$out/fakepds.log" 2>&1 &
pids+=($!)
for _ in $(seq 1 600); do grep -q READY "$out/fakepds.log" && break; sleep 0.2; done
grep -q READY "$out/fakepds.log" || { echo "relay chaos: fakepds didn't come up" >&2; tail "$out/fakepds.log" >&2; exit 1; }
upstreams=()
hosts=()
for g in $(seq 0 $((fake_hosts - 1))); do
  upstreams+=(--upstream "http://127.0.0.1:$((fake_base + g))")
  hosts+=(--host "http://127.0.0.1:$((fake_base + g))")
done

flush_ms=${FLUSH_MS:-2000}
crash_at=${CRASH_AT:-} crash_prob=${CRASH_PROB:-0.05}
case $scenario in flush-crash | mixed-flush) crash_at=${crash_at:-any} ;; esac
cl_dir=${CL_DIR:-$out}
[ "$cl_dir" = "$out" ] || { rm -rf "$cl_dir"; mkdir -p "$cl_dir"; }
s3=(--s3-endpoint "http://127.0.0.1:$minio" --prefix "$prefix")
supervise() {
  local i=$1 peers=() extra=()
  [ -n "${FSYNC_DELAY_US:-}" ] && extra+=(--qlog-fsync-delay-us "$FSYNC_DELAY_US")
  # 2 s flushes add an L0 each; at the 30 s default poll they outrun the
  # compactor and seals wait for L0 room
  extra+=(--qlog-state-compactor-poll-ms "${STATE_POLL_MS:-$(( flush_ms < 10000 ? 5000 : 30000 ))}")
  [ "${PLC_EXPORT:-}" = 1 ] && extra+=(--plc-export --plc-export-rate "${PLC_RATE:-4}" --admin-token relayq)
  [ -n "$crash_at" ] && extra+=(--qlog-crash-at "$crash_at" --qlog-crash-prob "$crash_prob" --qlog-crash-stop-file "$out/no-more-crashes")
  if [ -n "${RETAIN_SECS:-}" ]; then
    extra+=(--qlog-retain-secs "$RETAIN_SECS" --qlog-retain-every-secs "${RETAIN_EVERY:-60}")
  else
    extra+=(--qlog-retain-hours 0)
  fi
  for j in $slots; do
    [ "$i" = "$j" ] && continue
    if [ "$proxy" = 1 ]; then peers+=(--qlog-peer "n$j=127.0.0.1:$(route "$i" "$j")"); else peers+=(--qlog-peer "n$j=127.0.0.1:$(peer "$j")"); fi
  done
  while [ ! -e "$out/stop" ] && [ ! -e "$out/retired-n$i" ]; do
    set +e
    RUST_LOG=${RELAY_LOG:-info,slatedb=warn} "$bin" --node-id "n$i" --listen "127.0.0.1:$(http "$i")" --qlog-listen "127.0.0.1:$(peer "$i")" "${peers[@]}" \
      --qlog-members "$members_flag" --qlog-dir "$cl_dir/cl-n$i" --qlog-power-cut-on-usr1 \
      --qlog-flush-ms "$flush_ms" --qlog-headroom "${HEADROOM:-100000000}" --qlog-admin-token relayq \
      --s3-endpoint "http://127.0.0.1:$minio" --s3-bucket vlrelay --s3-access-key minioadmin --s3-secret-key minioadmin \
      --prefix "$prefix" --plc-url "http://127.0.0.1:$fake_plc" --dev-mode "${hosts[@]}" "${extra[@]}" \
      >>"$out/n$i.log" 2>&1
    local rc=$?
    set -e
    echo "$(ms) exit n$i $rc" >>"$out/events.log"
    [ -e "$out/stop" ] || [ -e "$out/retired-n$i" ] && break
    sleep "${RESTART_SEC:-1}"
    while [ -e "$out/hold-n$i" ] && [ ! -e "$out/stop" ]; do sleep 0.05; done
  done
}
for i in $ids; do supervise "$i" & pids+=($!); done
started="$ids"
start_slot() {
  supervise "$1" &
  pids+=($!)
  started="$started $1"
}

status() { curl -sf --max-time 1 "http://127.0.0.1:$(http "$1")/qlog/status" || echo '{}'; }
leader() {
  for i in $slots; do
    if status "$i" | python3 -c 'import json,sys; sys.exit(0 if json.load(sys.stdin).get("role")=="leader" else 1)' 2>/dev/null; then
      echo "$i"; return
    fi
  done
  echo ""
}
members() {
  local l
  l=$(leader)
  [ -n "$l" ] || return 0
  status "$l" | python3 -c 'import json,sys; print(" ".join(m[1:] for m in json.load(sys.stdin).get("members", [])))'
}
follower() {
  local l ms i
  l=$(leader)
  ms=$(members)
  for i in ${ms:-$ids}; do [ "$i" != "$l" ] && { echo "$i"; return; }; done
}

t=0
until [ -n "$(leader)" ]; do
  sleep 0.2; t=$((t + 1))
  [ $t -lt 150 ] || { echo "relay chaos: no leader in 30 s" >&2; exit 1; }
done
log "leader n$(leader)"

https=()
relays=""
for i in $slots; do
  https+=(--node "n$i=127.0.0.1:$(http "$i")")
  relays="$relays${relays:+,}http://127.0.0.1:$(http "$i")"
done
"$qlog" check --relay-frames "${https[@]}" --backfill true --gap-ms "${GAP_MS:-50}" --stop-file "$out/stop" --out "$out" \
  "${s3[@]}" >"$out/check.log" 2>&1 &
checker=$!
"$target/e2e_check" "${upstreams[@]}" --relay "$relays" --duration "$duration" --warmup 10 --settle 45 \
  --report-only --json-out "$out/e2e.json" >"$out/e2e.log" 2>&1 &
e2e=$!
pids+=($e2e)
load_start=$(date +%s)

(
  while [ ! -e "$out/stop" ]; do
    sleep "${VERIFY_EVERY:-20}"
    [ -e "$out/stop" ] && break
    "$qlog" verify "${s3[@]}" >>"$out/verify.jsonl" 2>>"$out/verify.log" || echo "$(ms) verify FAILED" >>"$out/events.log"
  done
) &
pids+=($!)

if [ "${STATUS_EVERY:-0}" != 0 ]; then
  (
    while [ ! -e "$out/stop" ]; do
      for i in $slots; do
        s=$(curl -sf --max-time 2 "http://127.0.0.1:$(http "$i")/qlog/status") && echo "{\"at_ms\": $(ms), \"node\": \"n$i\", \"status\": $s}" >>"$out/status.jsonl"
      done
      sleep "$STATUS_EVERY"
    done
  ) &
  pids+=($!)
fi

(
  while [ ! -e "$out/stop" ]; do
    for i in $slots; do
      p=$(nodepid "$i" || true)
      [ -n "$p" ] && echo "$(ms) n$i $(awk '{print $14+$15}' "/proc/$p/stat" 2>/dev/null) $(awk '/VmRSS/{print $2}' "/proc/$p/status" 2>/dev/null)" >>"$out/resources.log"
    done
    sleep 5
  done
) &
pids+=($!)

isolate() {
  local k=$1
  for j in $slots; do
    [ "$j" = "$k" ] && continue
    curl -sf -X POST "127.0.0.1:$ctl/r$k-$j" -d '{"blackhole": true}' >/dev/null
    curl -sf -X POST "127.0.0.1:$ctl/r$j-$k" -d '{"blackhole": true}' >/dev/null
  done
}
heal() {
  for i in $slots; do for j in $slots; do
    [ "$i" = "$j" ] || curl -sf -X POST "127.0.0.1:$ctl/r$i-$j" -d '{}' >/dev/null
  done; done
}
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
  for v in "$@"; do rm -f "$out/hold-n$v"; done
}
fault() {
  local kind=$1 who p
  case $kind in
    kill-leader | down-leader | partition-leader | pause-leader | power-cut-leader | kill-two | wipe-two) who=$(leader) ;;
    single-*) who=1 ;;
    *) who=$(follower) ;;
  esac
  [ -n "$who" ] || { log "skip $kind: no leader"; return; }
  case $kind in
    wipe-all) wipe "$kind" $ids ;;
    single-wipe) wipe "$kind" 1 ;;
    wipe-two)
      local keep=$who v vs=()
      [ $((RANDOM % 2)) = 0 ] && keep=$(follower)
      for v in $ids; do [ "$v" != "$keep" ] && vs+=("$v"); done
      log "survivor n$keep"
      wipe "$kind" "${vs[@]}"
      ;;
    kill-two | kill-all)
      local victims=$who ps=()
      if [ "$kind" = kill-all ]; then victims="$ids"; else for j in $ids; do [ "$j" != "$who" ] && { victims="$who $j"; break; }; done; fi
      for v in $victims; do p=$(nodepid "$v" || true); [ -n "$p" ] && { ps+=("$p"); log "kill9 n$v $kind"; }; done
      [ ${#ps[@]} -gt 0 ] && kill -9 "${ps[@]}" || true
      ;;
    power-cut-leader | power-cut-all)
      local victims=$who ps=()
      [ "$kind" = power-cut-all ] && victims="$ids"
      for v in $victims; do p=$(nodepid "$v" || true); [ -n "$p" ] && { ps+=("$p"); log "powercut n$v $kind"; }; done
      [ ${#ps[@]} -gt 0 ] && kill -USR1 "${ps[@]}" || true
      ;;
    down-follower | down-leader)
      # down past --qlog-host-failover-ms: its hosts move to the others
      touch "$out/hold-n$who"
      p=$(nodepid "$who" || true)
      [ -n "$p" ] && { log "kill9 n$who $kind"; kill -9 "$p" 2>/dev/null || true; }
      sleep "${DOWN_SEC:-6}"
      rm -f "$out/hold-n$who"
      log "back n$who"
      ;;
    kill-* | single-kill)
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

# ---- membership: every id used once; a replacement is a new id on an empty disk
next_slot=$((N + 1))
new_box() {
  [ "$next_slot" -le "$S" ] || return 1
  local t=0
  NEW=$next_slot
  next_slot=$((next_slot + 1))
  start_slot "$NEW"
  until curl -sf --max-time 1 "http://127.0.0.1:$(http "$NEW")/qlog/status" >/dev/null; do
    sleep 0.1; t=$((t + 1))
    [ $t -lt 300 ] || break
  done
}
member() {
  QLOG_ADMIN_TOKEN=relayq "$qlog" member "${https[@]}" --retry-secs "${MEMBER_RETRY:-90}" "$@" 2>>"$out/member.log" \
    | tee -a "$out/switches.jsonl" >/dev/null
}
removed() {
  local r=$1 p
  sleep 2
  touch "$out/retired-n$r"
  p=$(nodepid "$r" || true)
  [ -n "$p" ] && kill -9 "$p" 2>/dev/null || true
  while [ -n "$(nodepid "$r" || true)" ]; do sleep 0.02; done
  rm -rf "$cl_dir/cl-n$r"
  log "retired n$r"
}
replace() {
  local kind=$1 old new
  if [ "$kind" = replace-leader ]; then old=$(leader); else old=$(follower); fi
  [ -n "$old" ] || { log "skip $kind: no leader"; return; }
  new_box || { log "skip $kind: no ids left"; return; }
  new=$NEW
  log "switch-start $kind n$old n$new"
  if member replace "n$old" "n$new"; then log "switch-done $kind n$old n$new"; else log "switch-FAILED $kind n$old n$new"; fi
  removed "$old"
}
grow_step=0
grow_shrink() {
  local ms new v victims
  ms=$(members)
  [ -n "$ms" ] || { log "skip grow-shrink: no leader"; return; }
  if [ $((grow_step % 2)) = 0 ]; then
    new_box || { log "skip grow: no ids left"; return; }
    new=$NEW
    log "switch-start add n$new"
    member add "n$new" && log "switch-done add n$new" || log "switch-FAILED add n$new"
  else
    set -- $ms; victims=$(printf '%s\n' "$@" | shuf -n $(( $# - 3 )) | tr '\n' ' ')
    local set=()
    for v in $ms; do case " $victims " in *" $v "*) ;; *) set+=("n$v") ;; esac; done
    log "switch-start remove $(echo $victims)"
    member set "$(IFS=,; echo "${set[*]}")" && log "switch-done remove $(echo $victims)" || log "switch-FAILED remove $(echo $victims)"
    for v in $victims; do removed "$v"; done
  fi
  grow_step=$((grow_step + 1))
}

durable_kinds="kill-leader kill-follower kill-two kill-all power-cut-leader power-cut-all partition-leader pause-leader down-follower partition-follower"
wipe_kinds="kill-leader kill-two kill-all power-cut-all wipe-all wipe-two wipe-two partition-leader"
sleep "$every"
# injected crashes stop when the faults do: the hosts of a node that dies
# at the very end need the failover and a reconnect before the fleet stops
( sleep $(( duration > 12 ? duration - 12 - ($(date +%s) - load_start) : 0 )) 2>/dev/null; touch "$out/no-more-crashes" ) &
pids+=($!)
while [ $(($(date +%s) - load_start + 12)) -lt "$duration" ] && [ "$scenario" != baseline ]; do
  case $scenario in
    mixed-durable | mixed-flush) set -- $durable_kinds; shift $((RANDOM % 10)); k=$1 ;;
    mixed-wipe) set -- $wipe_kinds; shift $((RANDOM % 8)); k=$1 ;;
    flush-crash) k=none ;;
    *) k=$scenario ;;
  esac
  if [ "$k" = none ]; then :;
  elif [[ $k == replace-* ]]; then replace "$k";
  elif [ "$k" = grow-shrink ]; then grow_shrink;
  elif [ "$proxy" != 1 ] && [[ $k == partition-* ]]; then log "skip $k: PROXY=0"; else fault "$k"; fi
  sleep "$every"
done

while [ $(($(date +%s) - load_start)) -lt "$duration" ]; do sleep 1; done
touch "$out/no-more-crashes"
# the fleet stops; the relay settles, flushes everything committed, and
# every consumer reaches the same head
pkill -f "^$target/fakepds run --seed relayq$B " 2>/dev/null || true
sleep $(( (flush_ms / 1000) + 5 ))
for i in $started; do
  status "$i" >"$out/status-n$i.json"
  curl -sf --max-time 2 "http://127.0.0.1:$(http "$i")/metrics" >"$out/metrics-n$i.txt" || true
done
set +e
wait "$e2e"
touch "$out/stop"
wait "$checker"
rc=$?
"$qlog" verify "${s3[@]}" >"$out/verify.json" 2>>"$out/verify.log"
vrc=$?
[ $vrc = 0 ] || { echo "relay chaos: the final manifest is inconsistent" >&2; rc=1; }
grep -q "verify FAILED" "$out/events.log" && { echo "relay chaos: a mid-run verify failed" >&2; rc=1; }
grep -q "switch-FAILED" "$out/events.log" && { echo "relay chaos: a membership change never landed" >&2; rc=1; }
if [ "${PLC_EXPORT:-}" = 1 ]; then
  for i in $started; do
    curl -sf --max-time 5 -u admin:relayq "http://127.0.0.1:$(http "$i")/admin/api/ops/plc" >"$out/plc-n$i.json" || echo '{}' >"$out/plc-n$i.json"
  done
  python3 - "$out" $started <<'PY' || rc=1
import json, sys
out, ids = sys.argv[1], sys.argv[2:]
views = {i: json.load(open(f"{out}/plc-n{i}.json")) for i in ids}
lead = [v for v in views.values() if v.get("leader")]
v = lead[0] if lead else {}
print(f"plc export: leader {v.get('leader')} caught up {v.get('caughtUp')} ops {v.get('ops')} written {v.get('written')} "
      f"requests {v.get('requests')} throttled {v.get('throttled')} restarts {v.get('restarts')} "
      f"windows {[(w['ops'], w['done']) for w in v.get('windows', [])]}")
if not v.get("caughtUp"):
    print("relay chaos: the PLC export never caught up", file=sys.stderr)
    sys.exit(1)
PY
fi
missing=$(python3 -c 'import json,sys; print(json.load(open(sys.argv[1])).get("missing", -1))' "$out/e2e.json" 2>/dev/null || echo -1)
[ "$missing" = 0 ] || { echo "relay chaos: e2e_check: $missing upstream events missing from the relay" >&2; rc=1; }
set -e
python3 "$here/relay_report.py" "$out" | tee "$out/report.txt" || true
exit $rc
