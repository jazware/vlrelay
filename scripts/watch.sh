#!/usr/bin/env bash
# Re-run a job on every save: scripts/watch.sh [check|test|clippy|build]
# bacon if installed (best: incremental output, keeps the last good result),
# else cargo-watch, else a polling loop on src/ and Cargo.toml mtimes.
set -euo pipefail
cd "$(dirname "$0")/.."
job=${1:-check}
case $job in
  check) cmd=(cargo check --all-targets) ;;
  test) cmd=(cargo test) ;;
  clippy) cmd=(cargo clippy --all-targets) ;;
  build) cmd=(cargo build --bin vlrelay) ;;
  *) echo "watch: unknown job $job" >&2; exit 1 ;;
esac
if command -v bacon >/dev/null; then
  exec bacon "$job"
fi
if command -v cargo-watch >/dev/null; then
  exec cargo watch -q -c -w src -w Cargo.toml -x "${cmd[*]#cargo }"
fi
echo "watch: neither bacon nor cargo-watch is installed (cargo install --locked bacon); polling" >&2
stamp() { find src Cargo.toml tests -type f -newer "$1" 2>/dev/null | head -1; }
mark=$(mktemp)
trap 'rm -f "$mark"' EXIT
while true; do
  touch "$mark"
  clear
  "${cmd[@]}" 2>&1 | tail -60 || true
  echo "--- $(date +%T) watching src/ (${cmd[*]})"
  while [ -z "$(stamp "$mark")" ]; do sleep 0.3; done
done
