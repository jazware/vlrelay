#!/usr/bin/env bash
# What a host's disk costs the qlog commitlog (docs/quorum.md, "The
# commitlog (Phase 2)"): fdatasync latency for an appending writer at the
# commitlog's typical group-commit sizes, one writer and three (three nodes
# on one disk), plus the sequential write ceiling. Needs only fio, no root.
#
#   tests/qlog/fsync_probe.sh [DIR]     # DIR on the disk the commitlog would use (default: ./fsync-probe)
#
# Prints one line per test; paste them into the notes. Deletes its files.
set -euo pipefail
dir=${1:-./fsync-probe}
mkdir -p "$dir"
trap 'rm -rf "$dir"/qprobe.*' EXIT
command -v fio >/dev/null || { echo "fsync_probe: needs fio (apt install fio)" >&2; exit 1; }

echo "host $(hostname) kernel $(uname -r) $(nproc) cpus; $(df -hT "$dir" | awk 'NR==2{print $2" on "$1}')"
for dev in /sys/block/*/queue/write_cache; do
  [ -e "$dev" ] && echo "  $(basename "$(dirname "$(dirname "$dev")")") write_cache: $(cat "$dev")"
done

run() {
  local label=$1; shift
  fio --name=qprobe --directory="$dir" --ioengine=sync --rw=write --file_append=1 --fallocate=none \
    --group_reporting --output-format=json "$@" >"$dir/qprobe.json" 2>/dev/null
  python3 - "$label" "$dir/qprobe.json" <<'PY'
import json, sys
d = json.load(open(sys.argv[2]))["jobs"][0]
w, s = d["write"], d["sync"]["lat_ns"]
p = s.get("percentile", {})
f = lambda k: p.get(k, 0) / 1e6
print(f"{sys.argv[1]:<34} {w['iops']:>9.0f} writes/s {w['bw'] / 1024:>8.1f} MiB/s   fdatasync p50 {f('50.000000'):.3f} ms  p99 {f('99.000000'):.3f}  p99.9 {f('99.900000'):.3f}")
PY
  rm -f "$dir"/qprobe.*
}

# ~5 KB frames: 4 KiB is one at a time (350/s), 64 KiB a group of ~12
# (3,500/s), 1 MiB ~200 (35,000/s)
run "fdatasync 4k, 1 writer" --bs=4k --size=32M --fdatasync=1
run "fdatasync 64k, 1 writer" --bs=64k --size=256M --fdatasync=1
run "fdatasync 1m, 1 writer" --bs=1m --size=1G --fdatasync=1
run "fdatasync 64k, 3 writers" --bs=64k --size=128M --fdatasync=1 --numjobs=3
run "no sync, 1m (write ceiling)" --bs=1m --size=4G --end_fsync=1
