#!/usr/bin/env bash
# Run a command under a hard memory cap: dev/capped.sh <MiB> <cmd...>
#
# Linux with a user systemd: a transient scope with MemoryMax (the kernel
# OOM-kills inside it). Elsewhere (macOS has no per-process cap): a watchdog
# polls the RSS of the command's process tree every second and kills it past
# the cap, so a runaway shows up as a dead process with a note on stderr
# instead of a swapping laptop.
set -euo pipefail
mib=$1
shift
if [ "$(uname)" = Linux ] && command -v systemd-run >/dev/null && systemctl --user is-system-running >/dev/null 2>&1; then
  exec systemd-run --user --scope --quiet -p MemoryMax="${mib}M" -p MemorySwapMax=0 -- "$@"
fi

"$@" &
child=$!
trap 'kill $child 2>/dev/null; wait $child 2>/dev/null' INT TERM
tree_rss_kib() {
  local pids=$1 all=$1 kids
  while [ -n "$pids" ]; do
    kids=$(for p in $pids; do pgrep -P "$p" 2>/dev/null || true; done | tr '\n' ' ')
    all="$all $kids"
    pids=${kids% }
  done
  ps -o rss= -p "$(echo $all | tr ' ' ',')" 2>/dev/null | awk '{s+=$1} END {print s+0}'
}
while kill -0 "$child" 2>/dev/null; do
  rss=$(tree_rss_kib "$child")
  if [ "$rss" -gt $((mib * 1024)) ]; then
    echo "capped.sh: $1 (pid $child) at $((rss / 1024)) MiB > cap ${mib} MiB: killed" >&2
    kill -9 "$child" 2>/dev/null || true
    wait "$child" 2>/dev/null || true
    exit 137
  fi
  sleep 1
done
wait "$child"
