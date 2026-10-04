#!/usr/bin/env bash
# The reshard e2e (docs/cluster.md, "Resharding"): three core relays on one
# MinIO prefix with archival on, under load, split a DID shard and then merge
# its two halves back through the admin API. (No --plc-export: in dev mode a
# seeded PDS endpoint loses its http scheme, so the archive fetches the ref
# PDS's repos over https and fails. state::tests covers the seed rows.)
#
#   tests/e2e/reshard.sh [--duration 60] [--rate 50] [--accounts 30]
#                        [--split-at 20] [--merge-at 35]
#
# It passes when:
# - every core's stream carries every upstream event once, in order, at the
#   same seqs (e2e_check, cluster_report.py);
# - the split and the merge each flip;
# - every host keeps its row, its cursor never goes back, and no account is
#   created twice (the hosts' account counts don't move: a child that lost a
#   DID's sync record would count its next commit as a new account);
# - no stored tree disagrees with a commit after the reshard (the archive's
#   mismatches), and at the end every account's getLatestCommit and getRepo
#   through the relay match its PDS's. (Fetches by why are only printed: a
#   deactivated account's reactivation fetches again as "new".)
#
# Own ports and compose project (base 3680, nodes on 3700+), so it runs
# beside `just e2e-cluster`. Env: KEEP=1, OUT (dev/state/e2e-reshard).
set -euo pipefail
here="$(cd "$(dirname "$0")" && pwd)"
crate="$(cd "$here/../.." && pwd)"
cd "$crate"

export COMPOSE_PROJECT_NAME=${COMPOSE_PROJECT_NAME:-vlrelay-reshard}
pbase=${PORT_BASE:-3680}
export RELAY_PORT=$pbase RELAY_METRICS_PORT=$((pbase + 1)) PLC_PORT=$((pbase + 2)) REF_PDS_PORT=$((pbase + 3))
export PDS_BASE_PORT=$((pbase + 4)) MINIO_PORT=$((pbase + 10)) MINIO_CONSOLE_PORT=$((pbase + 11))
. dev/ports.sh

duration=60 rate=50 accounts=30 split_at=20 merge_at=35
while [ $# -gt 0 ]; do
  case $1 in
    --duration) duration=$2; shift 2 ;;
    --rate) rate=$2; shift 2 ;;
    --accounts) accounts=$2; shift 2 ;;
    --split-at) split_at=$2; shift 2 ;;
    --merge-at) merge_at=$2; shift 2 ;;
    *) echo "e2e-reshard: unknown flag $1" >&2; exit 1 ;;
  esac
done
out=${OUT:-dev/state/e2e-reshard}
target=${CARGO_TARGET_DIR:-target}/debug
base=${CLUSTER_PORT_BASE:-3700}
token=e2e

t0=$(date +%s)
cargo build --quiet --bin vlrelay --bin e2e_check --bin devnet
echo "e2e-reshard: built in $(($(date +%s) - t0))s"

pids=()
cleanup() {
  for p in "${pids[@]}"; do
    pkill -P "$p" 2>/dev/null || true
    kill "$p" 2>/dev/null || true
  done
  wait 2>/dev/null || true
  if [ "${KEEP:-}" = 1 ]; then
    echo "e2e-reshard: KEEP=1: network left up (COMPOSE_PROJECT_NAME=$COMPOSE_PROJECT_NAME dev/down.sh)"
  else
    dev/down.sh
  fi
}
trap cleanup EXIT
fail() {
  echo "e2e-reshard: FAIL: $*" >&2
  exit 1
}

DEV_PDS=${DEV_PDS:-3} dev/up.sh
rm -rf "$out" && mkdir -p "$out"
up_flags=$(sed 's/^/--upstream /' dev/state/hosts | tr '\n' ' ')
"$target/devnet" seed --accounts "$accounts" $(sed 's/^/--host /' dev/state/hosts | tr '\n' ' ')

prefix="e2e-reshard-$(date +%s)-$$"
store="--s3-endpoint http://127.0.0.1:$MINIO_PORT --s3-bucket vlrelay --s3-access-key minioadmin --s3-secret-key minioadmin --prefix $prefix"
common="$store --plc-url http://127.0.0.1:$PLC_PORT --linger-ms 25 --dev-mode --internal-token e2e-reshard-token --peer-tls-dir dev/state/peer-tls --lease-ttl-ms 3000 --did-shards 4 --host-shards 16 --admin-token $token"
hosts=$(sed 's/^/--host /' dev/state/hosts | tr '\n' ' ')

pub() { echo $((base + $1)); }
u() { echo "http://127.0.0.1:$(pub "$1")"; }
start_node() { # i
  local i=$1
  dev/capped.sh "${RELAY_MEM_MB:-3072}" "$target/vlrelay" --role core --node-id "n$i" \
    --listen "127.0.0.1:$(pub "$i")" --peer-listen "127.0.0.1:$((base + 10 + i))" \
    --advertise-url "https://127.0.0.1:$((base + 10 + i))" $common $hosts >>"$out/n$i.log" 2>&1 &
  pids+=($!)
  for _ in $(seq 1 150); do
    curl -sf "$(u "$i")/xrpc/_health" >/dev/null && return 0
    sleep 0.2
  done
  tail -30 "$out/n$i.log" >&2
  fail "n$i didn't come up"
}
ms() { python3 -c 'import time; print(int(time.time()*1000))'; }
api() { # node method path [body]
  curl -sf -u "admin:$token" -X "$2" -H 'content-type: application/json' ${4:+--data "$4"} "$(u "$1")/admin/api/$3"
}

start_node 1
start_node 2
start_node 3
echo "e2e-reshard: 3 cores up on :$(pub 1)-:$(pub 3)"
sleep 3

# archival on, cluster-wide (the policy object is in the bucket)
api 1 GET policy/full | python3 -c "
import json, sys
d = json.load(sys.stdin)
p = d['policy']
p['archive'] = {'mode': 'all'}
for t in ('trusted', 'default', 'new'):
    p['tiers'][t]['archivalFetchesPerHost'] = 50
    p['tiers'][t]['eventsPerHour'] = 1000000
    p['tiers'][t]['eventsPerDay'] = 10000000
p['cluster']['archivalFetchConcurrency'] = 16
print(json.dumps({'baseVersion': d['version'], 'policy': p, 'note': 'reshard e2e'}))
" >"$out/policy.json"
api 1 PUT policy/full "$(cat "$out/policy.json")" >/dev/null || fail "PUT policy/full"

streams=("$(u 1),$(u 2),$(u 3)" "$(u 2),$(u 3),$(u 1)" "$(u 3),$(u 1),$(u 2)")
names=(core1 core2 core3)
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
echo "$t_load" >"$out/t_load_ms"
"$target/devnet" load --rate "$rate" --duration "$((duration + 2))" --identity-every 10 --deactivate-every 20 2>"$out/load.log" &
load=$!
pids+=($load)
at() { # seconds-into-load
  local now=$(( ($(ms) - t_load) / 1000 ))
  [ "$1" -gt "$now" ] && sleep $(( $1 - now ))
  return 0
}

# per-why fetch counts and account/apply numbers, summed over the cores
archive_sum() {
  for i in 1 2 3; do curl -sf -u "admin:$token" "$(u "$i")/admin/api/archive" || echo '{}'; echo; done | python3 -c '
import json, sys
tot = {"done": 0, "mismatches": 0, "byWhy": {}}
for line in sys.stdin:
    line = line.strip()
    if not line:
        continue
    d = json.loads(line)
    f, a = d.get("fetch", {}), d.get("apply", {})
    tot["done"] += f.get("done", 0)
    tot["mismatches"] += a.get("mismatches", 0)
    for k, v in (f.get("byWhy") or {}).items():
        tot["byWhy"][k] = tot["byWhy"].get(k, 0) + v
print(json.dumps(tot))'
}
hosts_snap() { # file
  curl -sf "$(u 1)/xrpc/com.atproto.sync.listHosts?limit=1000" >"$1" || fail "listHosts"
}
hosts_cmp() { # before after
  python3 - "$1" "$2" <<'EOF' || fail "host rows changed across the reshard"
import json, sys
a = {h["hostname"]: h for h in json.load(open(sys.argv[1]))["hosts"]}
b = {h["hostname"]: h for h in json.load(open(sys.argv[2]))["hosts"]}
bad = []
if set(a) != set(b):
    bad.append(f"hosts {sorted(set(a) ^ set(b))}")
for n, h in a.items():
    if n not in b:
        continue
    if b[n].get("seq", 0) < h.get("seq", 0):
        bad.append(f"{n}: cursor {h.get('seq')} -> {b[n].get('seq')}")
    if b[n].get("accountCount") != h.get("accountCount"):
        bad.append(f"{n}: accountCount {h.get('accountCount')} -> {b[n].get('accountCount')}")
for x in bad:
    print("  ", x)
print(f"  {len(b)} hosts, cursors forward, account counts {sum(h.get('accountCount', 0) for h in b.values())}")
sys.exit(1 if bad else 0)
EOF
}

for _ in $(seq 1 60); do
  done_n=$(archive_sum | python3 -c 'import json, sys; print(json.load(sys.stdin)["done"])')
  [ "$done_n" -ge "$accounts" ] && break
  sleep 1
done
if [ "$done_n" -lt "$accounts" ]; then
  for i in 1 2 3; do curl -sf -u "admin:$token" "$(u "$i")/admin/api/archive" >"$out/archive-n$i.json" || true; done
  fail "only $done_n of $accounts accounts bootstrapped ($out/archive-n*.json)"
fi
echo "e2e-reshard: ok: $done_n mirrors bootstrapped at $(( ($(ms) - t_load) / 1000 ))s"

at "$split_at"
api 1 GET cluster/layout >"$out/layout-0.json"
archive_sum >"$out/archive-before.json"
hosts_snap "$out/hosts-0.json"
shard=$(python3 -c "import json; print(json.load(open('$out/layout-0.json'))['shards'][0]['id'])")
t=$(ms)
api 1 POST cluster/reshard "{\"op\":\"split\",\"shard\":$shard,\"wait\":true}" >"$out/split.json" || fail "split $shard"
python3 -c "import json, sys; sys.exit(0 if json.load(open('$out/split.json')).get('done') else 1)" || fail "the split didn't flip: $(cat "$out/split.json")"
read -r left right < <(python3 -c "import json; c = json.load(open('$out/split.json'))['op']['children']; print(c[0]['id'], c[1]['id'])")
echo "e2e-reshard: split shard $shard into $left and $right in $(( $(ms) - t )) ms"
sleep 3
hosts_snap "$out/hosts-1.json"
hosts_cmp "$out/hosts-0.json" "$out/hosts-1.json"

at "$merge_at"
t=$(ms)
api 1 POST cluster/reshard "{\"op\":\"merge\",\"left\":$left,\"right\":$right,\"wait\":true}" >"$out/merge.json" || fail "merge $left $right"
python3 -c "import json, sys; sys.exit(0 if json.load(open('$out/merge.json')).get('done') else 1)" || fail "the merge didn't flip: $(cat "$out/merge.json")"
echo "e2e-reshard: merged $left and $right in $(( $(ms) - t )) ms"
api 1 GET cluster/layout >"$out/layout-2.json"
sleep 3
hosts_snap "$out/hosts-2.json"
hosts_cmp "$out/hosts-0.json" "$out/hosts-2.json"

rc=0
for k in "${!checks[@]}"; do
  wait "${checks[$k]}" || { echo "e2e-reshard: checker ${names[$k]} FAILED"; rc=1; }
done
wait "$load" 2>/dev/null || true
for n in "${names[@]}"; do
  echo "== $n"
  grep -E '^\s+#|out of order' "$out/check-$n.txt" || true
done
STREAMS="${names[*]}" python3 tests/e2e/cluster_report.py "$out" || rc=1

# the last deactivations (5 s), then the end state against the PDSes
sleep 8
archive_sum >"$out/archive-after.json"
python3 - "$out/archive-before.json" "$out/archive-after.json" <<'EOF' || rc=1
import json, sys
a, b = json.load(open(sys.argv[1])), json.load(open(sys.argv[2]))
print(f"  mirror fetches by why: before the split {a['byWhy']}, at the end {b['byWhy']}")
bad = [w for w in ("mismatch",) if b["byWhy"].get(w, 0) > a["byWhy"].get(w, 0)]
if b["mismatches"] > a["mismatches"]:
    bad.append(f"tree mismatches {a['mismatches']} -> {b['mismatches']}")
for w in bad:
    print(f"  mirrors fetched again across the reshard: {w}")
sys.exit(1 if bad else 0)
EOF
python3 - "$(u 1)" "$(u 2)" "$(u 3)" <<'EOF' || rc=1
import json, sys, urllib.error, urllib.request
cores = sys.argv[1:]
def get(url):
    try:
        with urllib.request.urlopen(url, timeout=30) as r:
            return r.status, json.loads(r.read())
    except urllib.error.HTTPError as e:
        return e.code, json.loads(e.read() or b"{}")
bad = 0
accts = json.load(open("dev/state/accounts.json"))
for a in accts:
    q = f"/xrpc/com.atproto.sync.getLatestCommit?did={a['did']}"
    up = get(a["host"].rstrip("/") + q)
    mine = [r for r in (get(c + q) for c in cores) if r[0] == 200 or r[1].get("error") != "ShardUnavailable"]
    rel = mine[0] if mine else (0, {})
    if up[0] == 200 and rel != up or up[0] != 200 and rel[0] == 200:
        bad += 1
        print(f"  sync state of {a['did']}: relay {rel} vs PDS {up}")
print(f"  getLatestCommit: {len(accts) - bad} of {len(accts)} accounts match their PDS")
sys.exit(1 if bad else 0)
EOF
python3 tests/e2e/archival_compare.py "$(u 1)" dev/state/accounts.json --json "$out/compare.json" || rc=1
if grep -h "seq checkpoints disagree" "$out"/n*.log >/dev/null 2>&1; then
  echo "e2e-reshard: nodes numbered the stream differently"
  rc=1
fi
grep -h "cloned reshard children\|reshard child cloned\|reshard flipped" "$out"/n*.log | sed 's/\x1b\[[0-9;]*m//g' | cut -c1-220 || true
tail -3 "$out/load.log"
echo "e2e-reshard: $( [ $rc = 0 ] && echo PASS || echo FAIL ) in $(($(date +%s) - t0))s ($out)"
exit $rc
