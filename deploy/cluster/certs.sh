#!/usr/bin/env bash
# The peer mTLS files for deploy/cluster: a CA and one certificate per node,
# made with vlpds's `admin tls` (vlRelay uses vlpds's peer TLS as is).
#
#   ./certs.sh [node ...]          (default: n1 n2 n3 edge)
#
# Writes pki/ca.crt and pki/ca.key (keep the key off the nodes), and
# pki/<node>/{ca.crt,<node>.crt,<node>.key}, the directory each node mounts as
# --peer-tls-dir. A node's certificate names it (vlpds://node/<node>) and its
# advertise host, which is the compose service name here.
#
# Env: VLPDS, the vlpds binary (default: vlpds on PATH, else a debug build of
# ../../../vlpds, which takes a few minutes the first time).
set -euo pipefail
cd "$(dirname "$0")"
nodes=("$@")
[ ${#nodes[@]} -gt 0 ] || nodes=(n1 n2 n3 edge)

vlpds=${VLPDS:-}
if [ -z "$vlpds" ]; then
  if command -v vlpds >/dev/null; then
    vlpds=vlpds
  else
    echo "certs: no vlpds on PATH, building ../../../vlpds (debug)" >&2
    cargo build --quiet --manifest-path ../../../vlpds/Cargo.toml --bin vlpds
    vlpds=$(cd ../../../vlpds && pwd)/target/debug/vlpds
  fi
fi

mkdir -p pki
[ -f pki/ca.key ] || "$vlpds" admin tls ca --out pki
for n in "${nodes[@]}"; do
  mkdir -p "pki/$n"
  "$vlpds" admin tls issue --ca pki/ca.crt --ca-key pki/ca.key --out "pki/$n" --node-id "$n" --host "$n" --force
  cp pki/ca.crt "pki/$n/ca.crt"
done
# The image runs as uid 10001. On Linux, bind-mounted keys keep this user's
# owner and mode, so hand them over (Docker Desktop and OrbStack don't need it).
if [ "$(uname)" = Linux ]; then
  echo "certs: on Linux, run: sudo chown -R 10001:10001 $(pwd)/pki/{$(IFS=,; echo "${nodes[*]}")}" >&2
fi
ls -l pki "${nodes[@]/#/pki/}"
