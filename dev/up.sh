#!/usr/bin/env bash
# Bring up the local network (docs/devloop.md): MinIO, PLC and the reference
# PDS in docker, plus $DEV_PDS native vlpds upstreams, each memory-capped.
# Idempotent: a running piece is left alone.
#
# Env: DEV_PDS (2: vlpds upstreams, at most 3), VLPDS_BIN (skip building vlpds),
# VLPDS_MEM_MB (2048: each vlpds's cap), DEV_NO_DOCKER=1 (vlpds only, no PLC:
# accounts then need a PLC elsewhere, so mostly for debugging).
set -euo pipefail
here="$(cd "$(dirname "$0")" && pwd)"
crate="$(cd "$here/.." && pwd)"
. "$here/ports.sh"
state="$here/state"
mkdir -p "$state"

n=${DEV_PDS:-2}
[ "$n" -ge 1 ] && [ "$n" -le 3 ] || { echo "dev-up: DEV_PDS must be 1-3" >&2; exit 1; }
mem=${VLPDS_MEM_MB:-2048}
# small rings and caches: a dev upstream carries a few hundred events/s, and
# vlpds refuses to start when its fixed costs exceed the budget
small="--firehose-ring-mb 64 --live-ring-mb 32 --firehose-merge-queue-mb 32 --firehose-max-lag-mb 32 --backfill-cache-mb 32 --backfill-readahead-mb 16 --firehose-max-backfills 4 --max-exports 4 --lazy-mst-node-cache-mb 32"

t0=$(date +%s)
if [ "${DEV_NO_DOCKER:-}" != 1 ]; then
  docker compose --progress quiet -f "$here/docker-compose.yml" up -d --wait minio plc-db plc ref-pds >/dev/null
  docker compose --progress quiet -f "$here/docker-compose.yml" run --rm minio-init
fi
echo "dev-up: docker services up in $(($(date +%s) - t0))s"

bin=${VLPDS_BIN:-}
if [ -z "$bin" ]; then
  # vlpds's binary built from this crate's lockfile and target dir: it shares
  # the dependency builds with vlrelay instead of a second full build
  (cd "$crate" && cargo build --quiet -p vlpds --bin vlpds)
  bin="${CARGO_TARGET_DIR:-$crate/target}/debug/vlpds"
fi

# fixed dev PLC rotation keys, one per upstream (local PLC only)
keys=(9f2c1d4e5b6a79880716253443526170f1e2d3c4b5a69788a9b8c7d6e5f40311
      9f2c1d4e5b6a79880716253443526170f1e2d3c4b5a69788a9b8c7d6e5f40312
      9f2c1d4e5b6a79880716253443526170f1e2d3c4b5a69788a9b8c7d6e5f40313)
for i in $(seq 1 "$n"); do
  port=$((PDS_BASE_PORT + i - 1))
  pidf="$state/pds$i.pid"
  if [ -f "$pidf" ] && kill -0 "$(cat "$pidf")" 2>/dev/null; then
    continue
  fi
  if lsof -nP -iTCP:"$port" -sTCP:LISTEN >/dev/null 2>&1; then
    echo "dev-up: port $port is taken by something else" >&2
    exit 1
  fi
  # --crawlers '': vlpds defaults to telling bsky.network about itself
  nohup "$here/capped.sh" "$mem" "$bin" --memory --dev-mode --no-rate-limits \
    --listen "127.0.0.1:$port" --public-url "http://127.0.0.1:$port" \
    --handle-domain "pds$i.test" --service-did "did:web:pds$i.test" \
    --plc-url "http://127.0.0.1:$PLC_PORT" --plc-mode directory --plc-rotation-key "${keys[$((i - 1))]}" \
    --crawlers '' --memory-budget-mb "$mem" $small \
    >"$state/pds$i.log" 2>&1 &
  echo $! >"$pidf"
done
for i in $(seq 1 "$n"); do
  port=$((PDS_BASE_PORT + i - 1))
  for _ in $(seq 1 150); do
    curl -sf "http://127.0.0.1:$port/xrpc/_health" >/dev/null && break
    sleep 0.2
  done
  curl -sf "http://127.0.0.1:$port/xrpc/_health" >/dev/null || { echo "dev-up: pds$i didn't come up:" >&2; tail -20 "$state/pds$i.log" >&2; exit 1; }
done

hosts=()
for i in $(seq 1 "$n"); do hosts+=("http://127.0.0.1:$((PDS_BASE_PORT + i - 1))"); done
[ "${DEV_NO_DOCKER:-}" = 1 ] || hosts+=("http://localhost:$REF_PDS_PORT")
printf '%s\n' "${hosts[@]}" >"$state/hosts"
echo "dev-up: ready in $(($(date +%s) - t0))s"
echo "  PLC        http://127.0.0.1:$PLC_PORT"
echo "  MinIO      http://127.0.0.1:$MINIO_PORT (console :$MINIO_CONSOLE_PORT, minioadmin/minioadmin, bucket vlrelay)"
for h in "${hosts[@]}"; do echo "  upstream   $h"; done
echo "  relay      http://127.0.0.1:$RELAY_PORT (yours to start: just relay)"
