#!/usr/bin/env python3
"""Consumer faults for the chaos harness: slow consumers and a reconnect storm
against subscribeRepos, over raw sockets (stdlib only).

    consumers.py --url http://127.0.0.1:3361 [--url ...] --slow 20 --storm 100 --secs 40

slow: each socket reads 1 KB a second, so the relay's buffer for it fills
and it should be dropped (ConsumerTooSlow) without hurting anyone else.
storm: workers connect, read up to 256 KB (half live, half from cursor 0,
a full replay), drop the socket and do it again, for --secs.
"""
import argparse
import base64
import os
import random
import socket
import threading
import time
from urllib.parse import urlparse

stats = {"storm_connects": 0, "storm_errors": 0, "storm_bytes": 0, "slow_closed": 0, "slow_open_at_end": 0}
closed_after = []
lock = threading.Lock()


def connect(url, cursor):
    u = urlparse(url)
    s = socket.create_connection((u.hostname, u.port), timeout=5)
    path = "/xrpc/com.atproto.sync.subscribeRepos" + (f"?cursor={cursor}" if cursor is not None else "")
    key = base64.b64encode(os.urandom(16)).decode()
    s.sendall(
        f"GET {path} HTTP/1.1\r\nHost: {u.hostname}:{u.port}\r\nUpgrade: websocket\r\nConnection: Upgrade\r\n"
        f"Sec-WebSocket-Key: {key}\r\nSec-WebSocket-Version: 13\r\nUser-Agent: vlrelay-chaos\r\n\r\n".encode()
    )
    head = b""
    while b"\r\n\r\n" not in head:
        b = s.recv(4096)
        if not b:
            raise ConnectionError("closed during handshake")
        head += b
    if b" 101 " not in head.split(b"\r\n", 1)[0]:
        raise ConnectionError(head.split(b"\r\n", 1)[0].decode(errors="replace"))
    return s


def slow(url, end):
    t0 = time.time()
    try:
        s = connect(url, None)
        s.setsockopt(socket.SOL_SOCKET, socket.SO_RCVBUF, 4096)
        s.settimeout(2)
        while time.time() < end:
            try:
                b = s.recv(1024)
            except socket.timeout:
                continue
            if not b:
                with lock:
                    stats["slow_closed"] += 1
                    closed_after.append(time.time() - t0)
                return
            time.sleep(1)
        with lock:
            stats["slow_open_at_end"] += 1
    except Exception:
        with lock:
            stats["slow_closed"] += 1
            closed_after.append(time.time() - t0)


def storm(urls, end):
    while time.time() < end:
        url = random.choice(urls)
        try:
            s = connect(url, 0 if random.random() < 0.5 else None)
            s.settimeout(2)
            got = 0
            stop = time.time() + random.uniform(0.2, 2.0)
            while got < 256 * 1024 and time.time() < stop:
                try:
                    b = s.recv(65536)
                except socket.timeout:
                    break
                if not b:
                    break
                got += len(b)
            s.close()
            with lock:
                stats["storm_connects"] += 1
                stats["storm_bytes"] += got
        except Exception:
            with lock:
                stats["storm_errors"] += 1
            time.sleep(0.1)


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--url", action="append", required=True)
    ap.add_argument("--slow", type=int, default=20)
    ap.add_argument("--storm", type=int, default=100)
    ap.add_argument("--secs", type=float, default=40)
    a = ap.parse_args()
    end = time.time() + a.secs
    ts = [threading.Thread(target=slow, args=(a.url[i % len(a.url)], end)) for i in range(a.slow)]
    ts += [threading.Thread(target=storm, args=(a.url, end)) for _ in range(a.storm)]
    for t in ts:
        t.start()
    for t in ts:
        t.join()
    c = sorted(closed_after)
    med = c[len(c) // 2] if c else float("nan")
    print(
        f"storm: {stats['storm_connects']} connects ({stats['storm_connects'] / a.secs:.0f}/s), {stats['storm_errors']} errors, "
        f"{stats['storm_bytes'] / 1e6:.1f} MB read; slow: {stats['slow_closed']} of {a.slow} dropped by the relay "
        f"(median after {med:.1f} s), {stats['slow_open_at_end']} still open"
    )


if __name__ == "__main__":
    main()
