#!/usr/bin/env bash
# Tear down the local network: the native vlpds upstreams (and a relay
# started by `just relay`), the docker services and their tmpfs data, and
# dev/state (its accounts only exist in the stopped servers).
set -uo pipefail
here="$(cd "$(dirname "$0")" && pwd)"
state="${DEV_STATE:-$here/state}"
for pidf in "$state"/*.pid; do
  [ -f "$pidf" ] || continue
  pid=$(cat "$pidf")
  # the children first: once capped.sh is gone they'd be orphans
  kids=$(pgrep -P "$pid" | tr "\n" " ")
  kill $kids "$pid" 2>/dev/null
  for p in $kids "$pid"; do
    for _ in $(seq 1 50); do kill -0 "$p" 2>/dev/null || break; sleep 0.1; done
  done
  kill -9 $kids "$pid" 2>/dev/null
done
docker compose -f "$here/docker-compose.yml" ${DEV_COMPOSE_EXTRA:+-f "$DEV_COMPOSE_EXTRA"} down -v --remove-orphans >/dev/null 2>&1
rm -rf "$state"
echo "dev-down: done"
