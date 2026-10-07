#!/usr/bin/env python3
"""fsync_probe.sh's tests without fio, for a host where fio isn't installed
and can't be (no root): appending writes with an fdatasync after each, timed
per fdatasync, one writer and three, plus the unsynced write ceiling.

    fsync_probe.py [DIR]     # DIR on the disk the commitlog would use

Same sizes and output as fsync_probe.sh. Deletes its files.
"""
import os
import shutil
import socket
import sys
import threading
import time

d = sys.argv[1] if len(sys.argv) > 1 else "./fsync-probe"
os.makedirs(d, exist_ok=True)
KB, MB = 1024, 1024 * 1024


def pct(xs, p):
    xs = sorted(xs)
    return xs[min(len(xs) - 1, int(len(xs) * p / 100))] if xs else 0


def writer(path, bs, size, sync, lat):
    buf = os.urandom(bs)
    fd = os.open(path, os.O_WRONLY | os.O_CREAT | os.O_APPEND, 0o644)
    try:
        for _ in range(size // bs):
            os.write(fd, buf)
            if sync:
                t = time.perf_counter_ns()
                os.fdatasync(fd)
                lat.append(time.perf_counter_ns() - t)
        if not sync:
            os.fsync(fd)
    finally:
        os.close(fd)
        os.unlink(path)


def run(label, bs, size, jobs=1, sync=True):
    lats = [[] for _ in range(jobs)]
    t0 = time.perf_counter()
    ts = [threading.Thread(target=writer, args=(os.path.join(d, f"qprobe.{i}"), bs, size, sync, lats[i])) for i in range(jobs)]
    for t in ts:
        t.start()
    for t in ts:
        t.join()
    secs = time.perf_counter() - t0
    n = jobs * (size // bs)
    lat = [x for l in lats for x in l]
    f = lambda p: pct(lat, p) / 1e6
    print(f"{label:<34} {n / secs:>9.0f} writes/s {n * bs / MB / secs:>8.1f} MiB/s   fdatasync p50 {f(50):.3f} ms  p99 {f(99):.3f}  p99.9 {f(99.9):.3f}", flush=True)


st = shutil.disk_usage(d)
print(f"host {socket.gethostname()} kernel {os.uname().release} {os.cpu_count()} cpus; {st.free / 2**30:.0f} GiB free under {d} (python, no fio)")
try:
    run("fdatasync 4k, 1 writer", 4 * KB, 32 * MB)
    run("fdatasync 64k, 1 writer", 64 * KB, 256 * MB)
    run("fdatasync 1m, 1 writer", MB, 1024 * MB)
    run("fdatasync 64k, 3 writers", 64 * KB, 128 * MB, jobs=3)
    run("no sync, 1m (write ceiling)", MB, 4096 * MB, sync=False)
finally:
    for x in os.listdir(d):
        if x.startswith("qprobe."):
            os.unlink(os.path.join(d, x))
