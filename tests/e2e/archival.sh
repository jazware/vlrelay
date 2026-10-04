#!/usr/bin/env bash
# The archival e2e (docs/archival.md): the dev network under load, the relay
# switched from `archive: off` to `all` mid-run, one account's chain broken on
# purpose, and at the end every account's getRepo from the relay compared
# with its PDS's.
#
#   tests/e2e/archival.sh [--duration 60] [--rate 50] [--accounts 30]
#
# It passes when every account's mirror has the PDS's root and blocks (and
# the vlpds upstreams' bytes exactly), the switch bootstrapped every existing
# account, the broken chain was re-fetched and healed, and e2e_check finds
# nothing missing in the firehose. Runs on its own ports and compose project
# (base 3580), so it doesn't collide with `just e2e` or other dev networks.
# Env: KEEP=1 leaves the network up, OUT (dev/state/e2e-archival).
set -euo pipefail
here="$(cd "$(dirname "$0")" && pwd)"
crate="$(cd "$here/../.." && pwd)"
cd "$crate"

export COMPOSE_PROJECT_NAME=${COMPOSE_PROJECT_NAME:-vlrelay-archival}
base=${PORT_BASE:-3580}
export RELAY_PORT=$base RELAY_METRICS_PORT=$((base + 1)) PLC_PORT=$((base + 2)) REF_PDS_PORT=$((base + 3))
export PDS_BASE_PORT=$((base + 4)) MINIO_PORT=$((base + 10)) MINIO_CONSOLE_PORT=$((base + 11))
. dev/ports.sh

duration=60 rate=50 accounts=30
while [ $# -gt 0 ]; do
  case $1 in
    --duration) duration=$2; shift 2 ;;
    --rate) rate=$2; shift 2 ;;
    --accounts) accounts=$2; shift 2 ;;
    *) echo "archival e2e: unknown flag $1" >&2; exit 1 ;;
  esac
done
out=${OUT:-dev/state/e2e-archival}
target=${CARGO_TARGET_DIR:-target}/debug
token=e2e
relay="http://127.0.0.1:$RELAY_PORT"
api="$relay/admin/api"

t0=$(date +%s)
cargo build --quiet --bin vlrelay --bin e2e_check --bin devnet
echo "archival e2e: built in $(($(date +%s) - t0))s"

pids=()
cleanup() {
  for p in "${pids[@]}"; do kill "$p" 2>/dev/null || true; done
  wait 2>/dev/null || true
  if [ "${KEEP:-}" = 1 ]; then
    echo "archival e2e: KEEP=1: network left up (COMPOSE_PROJECT_NAME=$COMPOSE_PROJECT_NAME dev/down.sh)"
  else
    dev/down.sh
  fi
}
trap cleanup EXIT
fail() {
  echo "archival e2e: FAIL: $*" >&2
  tail -20 "$out/relay.log" >&2 || true
  exit 1
}

dev/up.sh
mkdir -p "$out"
: >"$out/relay.log"
hosts=$(sed 's/^/--host /' dev/state/hosts | tr '\n' ' ')
"$target/devnet" seed --accounts "$accounts" $hosts

dev/capped.sh "${RELAY_MEM_MB:-4096}" "$target/vlrelay" --listen "127.0.0.1:$RELAY_PORT" --memory \
  --plc-url "http://127.0.0.1:$PLC_PORT" --linger-ms 25 --admin-token "$token" $hosts >>"$out/relay.log" 2>&1 &
pids+=($!)
for _ in $(seq 1 100); do
  curl -sf "$relay/xrpc/_health" >/dev/null && break
  sleep 0.2
done
curl -sf "$relay/xrpc/_health" >/dev/null || fail "the relay didn't come up"
echo "archival e2e: relay up on :$RELAY_PORT"

get() { curl -sf -u "admin:$token" "$api/$1"; }
send() { curl -sf -u "admin:$token" -X "$1" -H 'content-type: application/json' --data "$3" "$api/$2"; }
set_archive() {
  get policy/full | python3 -c "
import json, sys
d = json.load(sys.stdin)
p = d['policy']
p['archive'] = {'mode': '$1'}
for t in ('trusted', 'default', 'new'):
    p['tiers'][t]['archivalFetchesPerHost'] = 50
    p['tiers'][t]['eventsPerHour'] = 1000000
    p['tiers'][t]['eventsPerDay'] = 10000000
p['cluster']['archivalFetchConcurrency'] = 16
print(json.dumps({'baseVersion': d['version'], 'policy': p, 'note': 'archival e2e: $1'}))
" >"$out/policy-$1.json"
  send PUT policy/full "$(cat "$out/policy-$1.json")" >/dev/null || fail "PUT policy/full ($1)"
}
status() { get archive; }
field() { status | F="$1" python3 -c 'import json, os, sys; d = json.load(sys.stdin); print(eval(os.environ["F"]))'; }

"$target/e2e_check" $(sed 's/^/--upstream /' dev/state/hosts | tr '\n' ' ') --relay "$relay" --duration "$duration" \
  --warmup 3 --settle 15 --report-only --json-out "$out/report.json" >"$out/check.txt" 2>"$out/check.log" &
check=$!
sleep 2
"$target/devnet" load --rate "$rate" --duration "$((duration + 2))" --identity-every 10 --deactivate-every 20 \
  2>"$out/load.log" &
load=$!
pids+=($load)

# a third of the way in, with every account already live and unmirrored
sleep $((duration / 3))
[ "$(field "d['apply']['applied']")" = 0 ] || fail "archive off, but commits were mirrored"
set_archive all
t_switch=$(date +%s)
echo "archival e2e: archive: off -> all at $((duration / 3))s"
# the sweeper's rescan queues every account within a sweep interval (10 s)
for _ in $(seq 1 60); do
  done_n=$(field "d['fetch']['done']")
  [ "$done_n" -ge "$accounts" ] && break
  sleep 1
done
echo "archival e2e: ok: $done_n bootstraps $(($(date +%s) - t_switch))s after the switch"
[ "$done_n" -ge "$accounts" ] || fail "only $done_n of $accounts accounts bootstrapped"

# break one mirrored account's chain: its next commit fails prevData
victim=$(python3 -c "import json; print(json.load(open('dev/state/accounts.json'))[0]['did'])")
send POST "archive/desync?did=$victim" '' >/dev/null || fail "desync $victim"
echo "archival e2e: forced a desync of $victim"
for _ in $(seq 1 40); do
  [ "$(field "d['fetch']['healed']")" -ge 1 ] && break
  sleep 1
done
[ "$(field "d['fetch']['healed']")" -ge 1 ] || fail "the desynchronized account wasn't re-fetched and healed"
echo "archival e2e: ok: $victim re-fetched and healed"

rc=0
wait "$check" || rc=$?
wait "$load" 2>/dev/null || true
# commits in flight, the last deactivations (5 s) and their #sync
sleep 8
for _ in $(seq 1 30); do
  q=$(field "d['queue']['queued'] + d['queue']['running']")
  [ "$q" = 0 ] && break
  sleep 1
done
cat "$out/check.txt"
python3 tests/e2e/archival_compare.py "$relay" dev/state/accounts.json --json "$out/compare.json" || rc=1
status >"$out/archive.json"
curl -sf "$relay/metrics" | grep '^vlrelay_' >"$out/metrics.txt" || true
python3 - "$out/archive.json" <<'EOF'
import json, sys
d = json.load(open(sys.argv[1]))
f, a, r = d["fetch"], d["apply"], d["reads"]
print(f"  bootstraps {f['done']} ({f['byWhy']}), failed {f['failed']}, retried {f['retried']}, healed {f['healed']}")
print(f"  fetched {f['bytes'] / 1e6:.2f} MB, {f['records']} records, {f['fetchUs'] / max(f['done'], 1) / 1000:.1f} ms/fetch, import {f['importUs'] / max(f['done'], 1) / 1000:.1f} ms/repo")
print(f"  applied {a['applied']} commits ({a['appliedUs'] / max(a['applied'], 1):.0f} us each), skipped {a['skipped']}, buffered {a['buffered']}, mismatches {a['mismatches']}, stale {a['stale']}")
print(f"  getRepo served {r['exports']} ({r['exportUs'] / max(r['exports'], 1) / 1000:.1f} ms each)")
EOF
# the forced desync drops exactly one commit of the victim (its prevData
# fails), and nothing else may go missing
missing=$(python3 -c "import json; print(json.load(open('$out/report.json'))['missing'])")
others=$(sed -n '/^  missing:/,/^  [a-z]/p' "$out/check.txt" | grep -c 'did:' | tr -d ' ')
victims=$(sed -n '/^  missing:/,/^  [a-z]/p' "$out/check.txt" | grep -c "$victim" | tr -d ' ')
if [ "$missing" -gt 1 ] || [ "$others" != "$victims" ]; then
  echo "archival e2e: the firehose lost $missing events (victim's: $victims)" >&2
  rc=1
fi
echo "archival e2e: $([ $rc = 0 ] && echo PASS || echo FAIL) in $(($(date +%s) - t0))s ($out)"
exit $rc
