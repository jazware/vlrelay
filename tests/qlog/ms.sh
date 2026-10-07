# Sourced by the harness scripts: ms prints the wall clock in milliseconds.
# Not `date`: uutils date ignores %3N and BSD date has no %N. bash 5's
# EPOCHREALTIME needs no fork; macOS's bash 3.2 lacks it.
ms() {
  if [ -n "${EPOCHREALTIME:-}" ]; then
    local t=${EPOCHREALTIME/[.,]/}
    echo $((10#$t / 1000))
  else
    python3 -c 'import time; print(time.time_ns() // 1000000)'
  fi
}
