#!/usr/bin/env bash
# @atproto/sync against a relay: ts.sh NAME WS_URL SECS [extra consume.mjs args]
# Writes $run/ts-NAME.json. Needs Node >= 20 (undici 7); NODE overrides.
set -euo pipefail
. "$(dirname "$0")/env.sh"
node=${NODE:-$(command -v /opt/homebrew/bin/node || command -v node)}
name=$1 url=$2 secs=$3; shift 3
cd "$compat/ts"
[ -d node_modules ] || npm install --silent
"$node" consume.mjs --service "$url" --plc "http://localhost:$PLC_PORT" --secs "$secs" "$@" >"$run/ts-$name.json" 2>"$run/ts-$name.err"
