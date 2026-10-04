#!/usr/bin/env bash
# The compat run (docs/compat.md): vlRelay and indigo's relay side by side on
# one dev network, the same upstreams and load, with ecosystem consumers on
# both. Results go to scratch/run/ (COMPAT_OUT), summary.txt at the end.
#
#   tests/compat/run.sh [--secs 60] [--rate 30] [--keep]
#
# --keep leaves the network and both relays up. Every piece can be run on its
# own with net.sh, ts.sh, gocheck and syncdiff.py.
set -euo pipefail
. "$(dirname "$0")/env.sh"
secs=60 rate=30 keep=0
while [ $# -gt 0 ]; do
  case $1 in
  --secs) secs=$2; shift ;;
  --rate) rate=$2; shift ;;
  --keep) keep=1 ;;
  *) sed -n '2,10p' "$0"; exit 1 ;;
  esac
  shift
done
cd "$crate"
net="$compat/net.sh"
node=${NODE:-$(command -v /opt/homebrew/bin/node || command -v node)}
VL="ws://127.0.0.1:$VLRELAY_PORT" IN="ws://127.0.0.1:$INDIGO_PORT"
plc="http://127.0.0.1:$PLC_PORT"
settle=10
consume=$((secs + settle + 5))

[ -x "$gobin/goat" ] && [ -x "$gobin/indigo-relay" ] && [ -x "$gobin/jetstream-legacy" ] || "$net" tools
(cd "$compat/gocheck" && go build -o "$gobin/gocheck" .)
rm -rf "$run" && mkdir -p "$run"
trap '[ $keep = 1 ] || "$net" down >/dev/null 2>&1' EXIT

"$net" up
"$net" vlrelay
"$net" indigo
nohup "$gobin/jetstream-legacy" --ws-url "$VL/xrpc/com.atproto.sync.subscribeRepos" --data-dir "$run/jetstream" \
  --listen-addr "127.0.0.1:$JETSTREAM_PORT" --metrics-listen-addr "127.0.0.1:$((JETSTREAM_PORT + 1))" \
  >"$run/jetstream.log" 2>&1 &
echo $! >"$run/jetstream.pid"
sleep 2

bg() { "$@" & echo $! >>"$run/consumers.pids"; }
upstreams=$(sed 's/^/--upstream /' dev/state/hosts | tr '\n' ' ')
e2e() { # name relay-args...
  local name=$1; shift
  "$tdir/e2e_check" "$@" --duration "$secs" --settle "$settle" --report-only --json-out "$run/e2e-$name.json" >"$run/e2e-$name.txt" 2>&1
}
bg e2e vl $upstreams --relay "$VL"
bg e2e in $upstreams --relay "$IN"
# indigo's stream as the reference, vlRelay's as the one under test
bg e2e in-vs-vl --upstream "$IN" --relay "$VL"
for side in vl in; do
  url=$([ $side = vl ] && echo "$VL" || echo "$IN")
  bg sh -c "'$gobin/gocheck' --url '$url' --secs $consume --verify --plc '$plc' --seqs-out '$run/seqs-$side.txt' >'$run/gocheck-$side.json' 2>'$run/gocheck-$side.err'"
  # goat runs until killed
  bg sh -c "'$gobin/goat' --plc-host '$plc' firehose --relay-host '$url' --verify-basic --verify-sig --verify-mst --quiet >/dev/null 2>'$run/goat-$side.err' & sleep $consume; kill \$!"
  bg "$compat/ts.sh" "$side" "$url" "$consume"
done
bg "$compat/ts.sh" vl-coerced "$VL" "$consume" --coerce-seq
bg sh -c "cd '$compat/ts' && '$node' jsconsume.mjs --url ws://127.0.0.1:$JETSTREAM_PORT/subscribe --secs $consume --out '$run/jetstream-revs.txt' >'$run/jetstream-sub.json'"
sleep 1

"$net" load "$rate" "$secs" >"$run/load.txt" 2>&1 &
loadpid=$!
# jetstream restarts halfway: it resumes from the relay cursor it saved
sleep $((secs / 2))
kill "$(cat "$run/jetstream.pid")"; sleep 1
nohup "$gobin/jetstream-legacy" --ws-url "$VL/xrpc/com.atproto.sync.subscribeRepos" --data-dir "$run/jetstream" \
  --listen-addr "127.0.0.1:$JETSTREAM_PORT" --metrics-listen-addr "127.0.0.1:$((JETSTREAM_PORT + 1))" \
  >>"$run/jetstream.log" 2>&1 &
echo $! >"$run/jetstream.pid"
wait $loadpid
while read -r p; do wait "$p" 2>/dev/null || true; done <"$run/consumers.pids"

# cursor resume: from the middle of each stream to its end, now that the load
# has stopped; then cursors past the head and before the start
for side in vl in; do
  url=$([ $side = vl ] && echo "$VL" || echo "$IN")
  n=$(wc -l <"$run/seqs-$side.txt")
  mid=$(sed -n "$((n / 2))p" "$run/seqs-$side.txt")
  "$gobin/gocheck" --url "$url" --cursor "$mid" --secs 5 --seqs-out "$run/resume-$side.txt" >"$run/resume-$side.json"
  tail -n +"$((n / 2 + 1))" "$run/seqs-$side.txt" >"$run/resume-$side.want"
  last=$(tail -1 "$run/seqs-$side.txt")
  "$gobin/gocheck" --url "$url" --cursor "$((last * 2))" --secs 4 >"$run/future-$side.json"
  "$gobin/gocheck" --url "$url" --cursor 1 --secs 4 >"$run/old-$side.json"
done

python3 "$compat/states.py" --vl "http://127.0.0.1:$VLRELAY_PORT" --vl-token "$VLRELAY_ADMIN_TOKEN" \
  --indigo "http://127.0.0.1:$INDIGO_PORT" --indigo-password "$INDIGO_ADMIN_PW" \
  --accounts dev/state/accounts.json --out "$run" >"$run/states.json"

python3 "$compat/syncdiff.py" --a "http://127.0.0.1:$VLRELAY_PORT" --b "http://127.0.0.1:$INDIGO_PORT" \
  --accounts dev/state/accounts.json --json-out "$run/syncdiff.json" >"$run/syncdiff.txt"
# relay chaining, both ways: a second vlRelay with indigo's relay as its only
# upstream, then vlRelay added to indigo's relay as a host
"$net" vlrelay-chain
"$net" load 10 5 >/dev/null 2>&1
sleep 2
curl -s "http://127.0.0.1:$((VLRELAY_PORT - 1))/metrics" | grep -E '^vlrelay_events_(in|rejected|out)_total' >"$run/chain-vl.txt" || true
"$net" indigo-add-host "localhost:$VLRELAY_PORT"
sleep 3
grep "localhost:$VLRELAY_PORT" "$run/indigo.log" | grep -v '"method"' >"$run/chain-indigo.txt" || true
sqlite3 "$run/indigo/relay.sqlite" "select hostname, status from host" >>"$run/chain-indigo.txt"

python3 "$compat/summary.py" "$run" | tee "$run/summary.txt"
