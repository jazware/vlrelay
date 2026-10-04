#!/usr/bin/env bash
# Read-only comparison with production (docs/devloop.md "Against real PDSes"):
# vlRelay (--memory, local) subscribes to a few real PDSes, and two checkers
# run side by side over the same window, one against vlRelay and one against
# the production relay, so the two latencies share every condition.
#
#   scripts/prodcmp.sh [DURATION_S] [OUT_DIR]
#
# Never requestCrawls anyone and binds to 127.0.0.1 only.
set -euo pipefail
cd "$(dirname "$0")/.."
duration=${1:-600}
out=${2:-${TMPDIR:-/tmp}/vlrelay-prodcmp}
hosts=${PRODCMP_HOSTS:-"amanita.us-east.host.bsky.network eurosky.social blacksky.app"}
port=${PRODCMP_PORT:-2988}
target=${CARGO_TARGET_DIR:-target}/debug
mkdir -p "$out"
cargo build --quiet --bin vlrelay --bin e2e_check

host_flags=""
up_flags=""
for h in $hosts; do
  host_flags="$host_flags --host $h"
  up_flags="$up_flags --upstream $h"
done
RUST_LOG=${RUST_LOG:-info,slatedb=warn,vlrelay::node=debug} dev/capped.sh "${RELAY_MEM_MB:-4096}" "$target/vlrelay" --listen "127.0.0.1:$port" --memory \
  --did-lookups-per-sec "${LOOKUPS:-100}" $host_flags >"$out/relay.log" 2>&1 &
relay=$!
trap 'kill $relay 2>/dev/null; wait 2>/dev/null' EXIT
for _ in $(seq 1 100); do
  curl -sf "http://127.0.0.1:$port/xrpc/_health" >/dev/null && break
  sleep 0.2
done
# the first minute fills the DID cache: every account is new to vlRelay
# vlRelay carries only these hosts, so every event of its stream is in scope;
# --scope seen would drop a DID's events that reach vlRelay's stream before
# the checker's own PDS socket names the DID, and count them missing
common="--warmup ${WARMUP:-60} --duration $duration --settle 20 --report-only"
"$target/e2e_check" $up_flags --relay "http://127.0.0.1:$port" --scope all $common --json-out "$out/vlrelay.json" >"$out/vlrelay.txt" 2>"$out/vlrelay.log" &
a=$!
"$target/e2e_check" $up_flags --relay wss://bsky.network --scope seen $common --json-out "$out/prod.json" >"$out/prod.txt" 2>"$out/prod.log" &
b=$!
wait $a $b || true
curl -sf "http://127.0.0.1:$port/metrics" | grep '^vlrelay_' >"$out/metrics.txt" || true
echo "== vlRelay"; cat "$out/vlrelay.txt"
echo "== production (bsky.network)"; cat "$out/prod.txt"
