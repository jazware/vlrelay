#!/usr/bin/env python3
"""A subscribeRepos consumer that stops reading until it's far behind, to
make the relay cut it off with ConsumerTooSlow. Prints one JSON report.

  slow.py --port 3478 --lag-mb 1 --secs 60

A second, fast connection measures the stream: every subscriber is sent the
same bytes, so once the fast one has read well past --lag-mb, the stalled
one is past its allowance (its receive buffer is tiny, so the kernel holds
little of the backlog). Then the stalled one reads again and should find the
error frame at the end. Frames are matched by name in the bytes rather than
decoded: only whether one arrived matters.
"""
import argparse, base64, json, os, socket, threading, time

ap = argparse.ArgumentParser()
ap.add_argument("--port", type=int, required=True)
ap.add_argument("--lag-mb", type=float, default=1)
ap.add_argument("--secs", type=float, default=60)
a = ap.parse_args()


def subscribe(rcvbuf=None):
    s = socket.socket()
    if rcvbuf:
        s.setsockopt(socket.SOL_SOCKET, socket.SO_RCVBUF, rcvbuf)
    s.connect(("127.0.0.1", a.port))
    key = base64.b64encode(os.urandom(16)).decode()
    s.sendall(f"GET /xrpc/com.atproto.sync.subscribeRepos HTTP/1.1\r\nHost: 127.0.0.1:{a.port}\r\n"
              f"Upgrade: websocket\r\nConnection: Upgrade\r\nSec-WebSocket-Key: {key}\r\n"
              f"Sec-WebSocket-Version: 13\r\n\r\n".encode())
    return s


end = time.time() + a.secs
fast_bytes = 0


def fast():
    global fast_bytes
    s = subscribe()
    s.settimeout(1)
    while time.time() < end:
        try:
            b = s.recv(1 << 16)
        except socket.timeout:
            continue
        if not b:
            return
        fast_bytes += len(b)


slow = subscribe(4096)
threading.Thread(target=fast, daemon=True).start()
# Not much further: the relay waits only 10 s for the stalled socket to take
# the message in flight before it gives up without the error frame.
while fast_bytes < (a.lag_mb + 0.5) * (1 << 20) and time.time() < end:
    time.sleep(0.2)
behind = fast_bytes
slow.settimeout(1)
data, closed = b"", False
while time.time() < end:
    try:
        b = slow.recv(1 << 16)
    except socket.timeout:
        continue
    except OSError:
        closed = True
        break
    if not b:
        closed = True
        break
    data += b
print(json.dumps({
    "streamBytesWhenResumed": behind,
    "bytes": len(data),
    "consumerTooSlow": b"ConsumerTooSlow" in data,
    "closed": closed,
}))
