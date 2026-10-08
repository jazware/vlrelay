#!/usr/bin/env bash
# Edit -> build timings for the vlrelay crate (docs/devloop.md "Build speed").
#
#   scripts/buildtime.sh [label]
#
# For each step: a no-op run, then REPS runs each after a one-line edit to
# MODULE (a new pub fn with a fresh constant, so codegen and the link really
# happen). Prints the median seconds. The module is restored afterwards.
#
# Env: MODULE (src/seq.rs), REPS (3), STEPS ("check build test-build", plus link-big
# in the monorepo: vlpds through interop/),
# BUILD_ARGS (extra cargo build flags, e.g. --config ...), and anything cargo reads (RUSTFLAGS, CARGO_TARGET_DIR, CARGO_PROFILE_*).
set -euo pipefail
cd "$(dirname "$0")/.."
label=${1:-default}
module=${MODULE:-src/seq.rs}
reps=${REPS:-3}
steps=${STEPS:-check build test-build$([ -d interop ] && echo " link-big")}
backup=$(mktemp)
cp "$module" "$backup"
trap 'cp "$backup" "$module"; rm -f "$backup"' EXIT

now() { perl -MTime::HiRes=time -e 'printf "%.3f", time'; }
run_step() {
  case $1 in
    check) cargo check --quiet --bin vlrelay ;;
    build) cargo build --quiet ${BUILD_ARGS:-} --bin vlrelay ;;
    test-build) cargo test --quiet --no-run --lib 2>/dev/null ;;
    # a full-size binary: vlpds's main.rs recompiled and everything linked
    # (through interop/, which depends on vlpds), to compare with the relay's
    link-big) touch ../vlpds/src/main.rs && (cd interop && CARGO_TARGET_DIR="${CARGO_TARGET_DIR:-$PWD/../target}" cargo build --quiet ${BUILD_ARGS:-} -p vlpds --bin vlpds) ;;
  esac
}
median() { sort -n | awk '{a[NR]=$1} END {print a[int((NR+1)/2)]}'; }

for s in $steps; do
  run_step "$s" # warm: whatever the last edit left stale
  t0=$(now); run_step "$s"; t1=$(now)
  noop=$(perl -e "printf '%.2f', $t1 - $t0")
  times=()
  for i in $(seq 1 "$reps"); do
    printf '\npub fn __buildtime_probe() -> u64 { %d }\n' "$RANDOM$i" >>"$module"
    t0=$(now); run_step "$s"; t1=$(now)
    times+=("$(perl -e "printf '%.2f', $t1 - $t0")")
    cp "$backup" "$module"
  done
  med=$(printf '%s\n' "${times[@]}" | median)
  printf 'buildtime %-12s %-10s no-op %6ss  edit %6ss  (runs: %s)\n' "$label" "$s" "$noop" "$med" "${times[*]}"
done
