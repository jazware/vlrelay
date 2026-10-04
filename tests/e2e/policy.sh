#!/usr/bin/env bash
# The policy e2e (docs/policy.md): one relay against a fakepds fleet whose
# hosts misbehave, with the policy engine enforcing for real.
#
#   tests/e2e/policy.sh [--duration 60]
#
# Five fakepds hosts, all admitted at tier `default` (--host-tier):
#
#   0, 4  clean
#   1     badsig: 60% of commits carry a flipped signature bit
#   2     spam: bursts of brand-new accounts
#   3     replay: re-sends old frames (dropped as seq regressions), then gets
#         banned by a domain rule mid-run
#
# It passes when host 1 is auto-throttled, cases open for hosts 1 and 2, the
# ban rule disconnects host 3, and e2e_check finds nothing missing or extra
# for the clean hosts. Needs no docker: fakepds serves its own PLC. Env:
# OUT (dev/state/e2e-policy), RELAY_PORT (2978), FLEET_PORT (30100).
set -euo pipefail
here="$(cd "$(dirname "$0")" && pwd)"
crate="$(cd "$here/../.." && pwd)"
cd "$crate"

duration=60
while [ $# -gt 0 ]; do
  case $1 in
    --duration) duration=$2; shift 2 ;;
    *) echo "policy e2e: unknown flag $1" >&2; exit 1 ;;
  esac
done
out=${OUT:-dev/state/e2e-policy}
target=${CARGO_TARGET_DIR:-target}/debug
relay_port=${RELAY_PORT:-2978}
base=${FLEET_PORT:-30100}
token=e2e
api="http://127.0.0.1:$relay_port/admin/api"
hosts=5

t0=$(date +%s)
cargo build --quiet --bin vlrelay --bin e2e_check --bin fakepds
echo "policy e2e: built in $(($(date +%s) - t0))s"
mkdir -p "$out"
: >"$out/relay.log"

pids=()
cleanup() {
  for p in "${pids[@]}"; do kill "$p" 2>/dev/null || true; done
  wait 2>/dev/null || true
}
trap cleanup EXIT

fail() {
  echo "policy e2e: FAIL: $*" >&2
  tail -20 "$out/relay.log" >&2 || true
  exit 1
}

"$target/fakepds" run --seed policy-e2e --advertise http://127.0.0.1 --port-base "$base" --plc-port $((base - 1)) \
  --hosts "$hosts" --dids 50 --rate 100 --gen-threads 2 --initial-records 5 --duration $((duration + 60)) \
  --fault badsig:1:rate=0.6 --fault spam:2:rate=20,secs=10,every=40,delay=5 --fault replay:3:every=15,count=20 \
  >"$out/fakepds.log" 2>&1 &
pids+=($!)
for _ in $(seq 1 100); do
  curl -sf "http://127.0.0.1:$base/xrpc/_health" >/dev/null && break
  sleep 0.2
done

host_flags=""
for ((g = 0; g < hosts; g++)); do host_flags="$host_flags --host http://127.0.0.1:$((base + g))"; done
dev/capped.sh "${RELAY_MEM_MB:-4096}" "$target/vlrelay" --listen "127.0.0.1:$relay_port" --memory \
  --plc-url "http://127.0.0.1:$((base - 1))" --linger-ms 25 --host-tier default --admin-token "$token" \
  $host_flags >>"$out/relay.log" 2>&1 &
pids+=($!)
for _ in $(seq 1 100); do
  curl -sf "http://127.0.0.1:$relay_port/xrpc/_health" >/dev/null && break
  sleep 0.2
done
curl -sf "http://127.0.0.1:$relay_port/xrpc/_health" >/dev/null || fail "the relay didn't come up"
echo "policy e2e: relay up on :$relay_port"

get() { curl -sf -u "admin:$token" "$api/$1"; }
send() { curl -sf -u "admin:$token" -X "$1" -H 'content-type: application/json' --data "$3" "$api/$2"; }

# Thresholds a one-minute run can trip, and hourly caps it can't: the
# defaults (indigo's 2,600 events/h for an untrusted host) would throttle
# the clean hosts too. On a fresh relay every account is new, so the
# new-accounts threshold sits above a clean host's 50 and below the spam
# host's first burst.
get policy/full | python3 -c '
import json, sys
d = json.load(sys.stdin)
p = d["policy"]
for t in ("default", "new"):
    p["tiers"][t]["eventsPerHour"] = 1000000
    p["tiers"][t]["eventsPerDay"] = 10000000
p["spam"]["hostFailedValidation"] = {"limit": 60, "windowSecs": 60, "action": "throttle-and-case"}
p["spam"]["hostNewAccounts"] = {"limit": 120, "windowSecs": 3600, "action": "case"}
print(json.dumps({"baseVersion": d["version"], "policy": p, "note": "policy e2e"}))
' >"$out/policy.json"
send PUT policy/full "$(cat "$out/policy.json")" >/dev/null || fail "PUT policy/full"
echo "policy e2e: policy saved"

e2e_flags="--upstream http://127.0.0.1:$base --upstream http://127.0.0.1:$((base + 4)) --relay http://127.0.0.1:$relay_port"
"$target/e2e_check" $e2e_flags --duration "$duration" --warmup 5 --settle 10 --scope seen --report-only \
  --json-out "$out/report.json" >"$out/check.txt" 2>"$out/check.log" &
check=$!

tier_of() { get "hosts/127.0.0.1:$1" | python3 -c 'import json,sys; r=json.load(sys.stdin)["row"]; print(r["tier"], r["status"])'; }
wait_for() {
  local what=$1 secs=$2
  shift 2
  for _ in $(seq 1 "$secs"); do
    if "$@"; then
      echo "policy e2e: ok: $what"
      return 0
    fi
    sleep 1
  done
  fail "$what (after ${secs}s)"
}
throttled() { [ "$(tier_of $((base + 1)) | cut -d' ' -f1)" = throttled ]; }
case_for() {
  get cases | python3 -c "
import json, sys
cs = json.load(sys.stdin)
sys.exit(0 if any(c['host'] == '127.0.0.1:$1' and c['kind'] == '$2' for c in cs) else 1)"
}
banned() { [ "$(tier_of $((base + 3)) | cut -d' ' -f2)" = banned ]; }

wait_for "host 1 (badsig) auto-throttled" 45 throttled
wait_for "a failed-validation case for host 1" 15 case_for $((base + 1)) failed-validation
wait_for "a new-accounts case for host 2 (spam)" 45 case_for $((base + 2)) new-accounts

send POST domain-rules "{\"pattern\": \"127.0.0.1:$((base + 3))\", \"effect\": {\"kind\": \"ban\"}, \"note\": \"policy e2e\"}" >/dev/null \
  || fail "POST domain-rules"
wait_for "the ban rule disconnected host 3" 15 banned
if get "hosts?status=connected" | grep -q "127.0.0.1:$((base + 3))"; then fail "host 3 still connected"; fi

get "cases" >"$out/cases.json"
for g in 0 4; do
  tier=$(tier_of $((base + g)))
  [ "${tier% *}" = default ] || fail "clean host $g moved to $tier"
  if python3 -c "import json,sys; sys.exit(0 if any(c['host']=='127.0.0.1:$((base + g))' for c in json.load(open('$out/cases.json'))) else 1)"; then
    fail "a case opened for clean host $g"
  fi
done
echo "policy e2e: ok: clean hosts stay at default with no cases"

rc=0
wait "$check" || rc=$?
cat "$out/check.txt"
curl -sf "http://127.0.0.1:$relay_port/metrics" | grep '^vlrelay_' >"$out/metrics.txt" || true
python3 - "$out/report.json" <<'EOF' || fail "e2e_check found discrepancies for the clean hosts"
import json, sys
r = json.load(open(sys.argv[1]))
print(f"policy e2e: clean hosts: missing {r['missing']}, extra {r['extra']}")
sys.exit(0 if r["missing"] == 0 and r["extra"] == 0 else 1)
EOF
[ $rc = 0 ] || fail "e2e_check exited $rc"
echo "policy e2e: PASS in $(($(date +%s) - t0))s (logs in $out)"
