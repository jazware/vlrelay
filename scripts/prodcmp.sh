#!/usr/bin/env bash
# Read-only comparison with production (docs/devloop.md "Against real PDSes"):
# vlRelay (--memory, local) subscribes to a few real PDSes, and one checker
# compares both vlRelay and the production relay with the same PDS sockets
# (e2e_check --separate), so the two latencies share every condition.
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
# One checker, so each PDS gets one socket from it and both relays are
# measured against the same upstream arrivals. vlRelay carries only these
# hosts, so every event of its stream is in scope; production needs --scope
# seen (relay-first events are held until the PDS names the DID).
"$target/e2e_check" $up_flags --separate \
  --relay "http://127.0.0.1:$port" --relay-scope all \
  --relay wss://bsky.network --relay-scope seen \
  --warmup "${WARMUP:-60}" --duration "$duration" --settle 20 --report-only --show 20 \
  --json-out "$out/cmp.json" >"$out/cmp.txt" 2>"$out/cmp.log" || true
curl -sf "http://127.0.0.1:$port/metrics" | grep '^vlrelay_' >"$out/metrics.txt" || true
cat "$out/cmp.txt"
