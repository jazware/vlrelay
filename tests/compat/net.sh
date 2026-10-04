#!/usr/bin/env bash
# The compat network, piece by piece (docs/compat.md). Each subcommand is
# idempotent enough to rerun; run.sh drives them in order.
#
#   net.sh tools      clone indigo, goat, jetstream(-legacy) into scratch/ and build them
#   net.sh up         dev network on the 34xx ports with localhost hostnames, 30 accounts
#   net.sh vlrelay    vlRelay (--memory) against every upstream on :3480
#   net.sh indigo     indigo's relay (sqlite) on :3470, every upstream added through its admin API
#   net.sh load R D   devnet load at R writes/s for D seconds
#   net.sh down       stop everything
set -euo pipefail
. "$(dirname "$0")/env.sh"
cd "$crate"

pidfile() { echo "$run/$1.pid"; }
running() { [ -f "$(pidfile "$1")" ] && kill -0 "$(cat "$(pidfile "$1")")" 2>/dev/null; }
start() { # name cmd...
  local name=$1; shift
  running "$name" && { echo "$name already running"; return; }
  nohup "$@" >"$run/$name.log" 2>&1 &
  echo $! >"$(pidfile "$name")"
}
wait_http() { # url secs
  for _ in $(seq 1 $(($2 * 5))); do curl -sf "$1" >/dev/null && return 0; sleep 0.2; done
  echo "timed out waiting for $1" >&2; return 1
}
hosts() { cat dev/state/hosts; }

case "${1:-}" in
tools)
  mkdir -p "$scratch" "$gobin"
  for r in indigo goat jetstream jetstream-legacy; do
    [ -d "$scratch/$r" ] || git clone -q --depth 1 "https://github.com/bluesky-social/$r" "$scratch/$r"
  done
  (cd "$scratch/goat" && go build -o "$gobin/goat" .)
  (cd "$scratch/indigo" && go build -o "$gobin/indigo-relay" ./cmd/relay)
  (cd "$scratch/jetstream" && go build -o "$gobin/jetstream" ./cmd/jetstream)
  (cd "$scratch/jetstream-legacy" && go build -o "$gobin/jetstream-legacy" ./cmd/jetstream)
  (cd "$compat/ts" && npm install --silent)
  cargo build --quiet --bin vlrelay --bin e2e_check --bin devnet
  cargo build --quiet -p vlpds --bin vlpds
  ;;
up)
  dev/up.sh
  [ -s dev/state/accounts.json ] || "$tdir/devnet" seed --accounts "${ACCOUNTS:-30}" $(hosts | sed 's/^/--host /')
  ;;
vlrelay)
  start vlrelay "$tdir/vlrelay" --listen "127.0.0.1:$VLRELAY_PORT" --memory \
    --plc-url "http://127.0.0.1:$PLC_PORT" --linger-ms 25 --admin-token "$VLRELAY_ADMIN_TOKEN" $(hosts | sed 's/^/--host /') ${VLRELAY_ARGS:-}
  wait_http "http://127.0.0.1:$VLRELAY_PORT/xrpc/_health" 30
  ;;
vlrelay-chain)
  # a second vlRelay whose only upstream is indigo's relay
  start vlrelay-chain "$tdir/vlrelay" --listen "127.0.0.1:$((VLRELAY_PORT - 1))" --memory \
    --plc-url "http://127.0.0.1:$PLC_PORT" --linger-ms 25 --host "http://localhost:$INDIGO_PORT"
  wait_http "http://127.0.0.1:$((VLRELAY_PORT - 1))/xrpc/_health" 30
  ;;
indigo)
  mkdir -p "$run/indigo"
  db="$run/indigo/relay.sqlite"
  # lenient like production; the account limit above the dev network's size
  # so no account starts host-throttled
  indigo_cmd=(env RELAY_ADMIN_PASSWORD="$INDIGO_ADMIN_PW" "$gobin/indigo-relay" --plc-host "http://127.0.0.1:$PLC_PORT" serve
    --db-url "sqlite://$db" --persist-dir "$run/indigo/persist"
    --bind "127.0.0.1:$INDIGO_PORT" --metrics-listen "127.0.0.1:$INDIGO_METRICS_PORT"
    --allow-insecure-hosts --lenient-sync-validation --default-account-limit 10000 ${INDIGO_ARGS:-})
  # requestCrawl (admin or not) checks describeServer through indigo's
  # public-IP-only client, which refuses loopback. A host row in the
  # database is what the relay resubscribes to at startup, and it dials a
  # no-SSL (localhost) host with a plain dialer, so: start once for the
  # schema, write the rows, start again.
  if [ ! -f "$db" ]; then
    start indigo "${indigo_cmd[@]}"
    wait_http "http://127.0.0.1:$INDIGO_PORT/xrpc/_health" 30
    kill "$(cat "$(pidfile indigo)")"; rm -f "$(pidfile indigo)"; sleep 1
    for h in $(hosts); do
      sqlite3 "$db" "INSERT OR IGNORE INTO host (created_at, updated_at, hostname, no_ssl, account_limit, trusted, status, last_seq, account_count)
        VALUES (datetime('now'), datetime('now'), '${h#http://}', 1, 10000, 0, 'active', -1, 0);"
    done
  fi
  start indigo "${indigo_cmd[@]}"
  wait_http "http://127.0.0.1:$INDIGO_PORT/xrpc/_health" 30
  ;;
indigo-add-host)
  # net.sh indigo-add-host localhost:PORT: a host row as for the PDSes, then
  # a restart so the relay subscribes to it
  [ -n "${2:-}" ] || { echo "usage: net.sh indigo-add-host localhost:PORT" >&2; exit 1; }
  running indigo && { kill "$(cat "$(pidfile indigo)")"; rm -f "$(pidfile indigo)"; sleep 1; }
  sqlite3 "$run/indigo/relay.sqlite" "INSERT OR IGNORE INTO host (created_at, updated_at, hostname, no_ssl, account_limit, trusted, status, last_seq, account_count)
    VALUES (datetime('now'), datetime('now'), '$2', 1, 10000, 0, 'active', -1, 0);"
  "$0" indigo
  ;;
load)
  "$tdir/devnet" load --rate "${2:-20}" --duration "${3:-60}" ${LOAD_ARGS:-}
  ;;
down)
  for f in "$run"/*.pid; do
    [ -f "$f" ] || continue
    kill "$(cat "$f")" 2>/dev/null || true
    rm -f "$f"
  done
  dev/down.sh
  ;;
*)
  sed -n '2,12p' "$0"; exit 1
  ;;
esac
