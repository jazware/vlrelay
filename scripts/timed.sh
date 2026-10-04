#!/usr/bin/env bash
# Run a command and print its wall time to stderr: scripts/timed.sh <label> <cmd...>
set -uo pipefail
label=$1
shift
t0=$(perl -MTime::HiRes=time -e 'printf "%.3f", time')
"$@"
rc=$?
t1=$(perl -MTime::HiRes=time -e 'printf "%.3f", time')
perl -e "printf STDERR \"timed: %s: %.1fs (exit %d)\n\", '$label', $t1 - $t0, $rc"
exit $rc
