#!/usr/bin/env bash
# Builds vlpds, the dev network's upstream PDS, and prints its binary's path.
# In the monorepo it's built through interop/ (the tests against an
# in-process vlpds, which depend on it) in this crate's target dir, sharing
# the dependency builds with vlRelay. Without interop/ it's installed from
# github.com/jazware/vlpds (VLPDS_REV: a commit; default its main).
set -euo pipefail
crate="$(cd "$(dirname "$0")/.." && pwd)"
target="${CARGO_TARGET_DIR:-$crate/target}"
if [ -d "$crate/interop" ]; then
  (cd "$crate/interop" && CARGO_TARGET_DIR="$target" cargo build --quiet -p vlpds --bin vlpds)
  echo "$target/debug/vlpds"
else
  root="$target/vlpds-install"
  cargo install --quiet --locked --root "$root" --git https://github.com/jazware/vlpds \
    ${VLPDS_REV:+--rev "$VLPDS_REV"} --bin vlpds vlpds >&2
  echo "$root/bin/vlpds"
fi
