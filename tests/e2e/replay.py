#!/usr/bin/env python3
"""Replays subscribeRepos from a cursor and prints each event as
[seq, type, did] JSON lines (stdlib only).

    replay.py --url http://127.0.0.1:2978 [--cursor 0] (--frames N | --until-account DID --after SEQ)

--frames: stop after N events. --until-account: stop at DID's first #account
with a seq above --after (whatever it was waiting behind has been sent).
"""
import argparse
import base64
import json
import os
import socket
import sys
from urllib.parse import urlparse


def connect(url, cursor):
    u = urlparse(url)
    s = socket.create_connection((u.hostname, u.port), timeout=30)
    key = base64.b64encode(os.urandom(16)).decode()
    s.sendall(
        f"GET /xrpc/com.atproto.sync.subscribeRepos?cursor={cursor} HTTP/1.1\r\nHost: {u.hostname}:{u.port}\r\n"
        f"Upgrade: websocket\r\nConnection: Upgrade\r\nSec-WebSocket-Key: {key}\r\nSec-WebSocket-Version: 13\r\n\r\n".encode()
    )
    buf = b""
    while b"\r\n\r\n" not in buf:
        b = s.recv(65536)
        if not b:
            raise ConnectionError("closed during handshake")
        buf += b
    head, rest = buf.split(b"\r\n\r\n", 1)
    if b" 101 " not in head.split(b"\r\n", 1)[0]:
        raise ConnectionError(head.decode(errors="replace"))
    return s, rest


def split(buf):
    """(opcode, payload, rest) of the first whole message in buf, or None."""
    if len(buf) < 2:
        return None
    n, at = buf[1] & 0x7F, 2
    if n == 126:
        n, at = int.from_bytes(buf[2:4], "big"), 4
    elif n == 127:
        n, at = int.from_bytes(buf[2:10], "big"), 10
    if len(buf) < at or len(buf) < at + n:
        return None
    return buf[0] & 0x0F, buf[at : at + n], buf[at + n :]


def messages(s, buf):
    while True:
        m = split(buf)
        if m is None:
            b = s.recv(1 << 20)
            if not b:
                return
            buf += b
            continue
        op, payload, buf = m
        if op == 0x8:
            return
        if op == 0x2:
            yield payload


def head(f, i):
    b = f[i]
    major, low, i = b >> 5, b & 0x1F, i + 1
    if low < 24:
        return major, low, i
    n = {24: 1, 25: 2, 26: 4, 27: 8}[low]
    return major, int.from_bytes(f[i : i + n], "big"), i + n


def item(f, i):
    """(value, next index); byte strings, arrays, maps and tags are skipped (None)."""
    major, n, i = head(f, i)
    if major == 0:
        return n, i
    if major == 3:
        return f[i : i + n].decode(), i + n
    if major == 2:
        return None, i + n
    if major == 4:
        for _ in range(n):
            _, i = item(f, i)
        return None, i
    if major == 5:
        for _ in range(2 * n):
            _, i = item(f, i)
        return None, i
    if major == 6:
        return item(f, i)
    return None, i


def cbor_map(f, i):
    major, n, i = head(f, i)
    out = {}
    for _ in range(n):
        k, i = item(f, i)
        out[k], i = item(f, i)
    return out, i


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--url", required=True)
    ap.add_argument("--cursor", type=int, default=0)
    ap.add_argument("--frames", type=int)
    ap.add_argument("--until-account")
    ap.add_argument("--after", type=int, default=0)
    a = ap.parse_args()
    s, buf = connect(a.url, a.cursor)
    n = 0
    for m in messages(s, buf):
        h, i = cbor_map(m, 0)
        body, _ = cbor_map(m, i)
        t = h.get("t")
        did = body.get("repo") if t == "#commit" else body.get("did")
        seq = body.get("seq")
        print(json.dumps([seq, t, did]))
        n += 1
        if a.frames is not None and n >= a.frames:
            return
        if a.until_account and t == "#account" and did == a.until_account and seq > a.after:
            return
    sys.exit(f"replay: the stream ended after {n} events")


if __name__ == "__main__":
    main()
