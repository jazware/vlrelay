#!/usr/bin/env python3
"""A toxiproxy-style TCP fault proxy for the chaos harness (docs/chaos.md).

    proxy.py --control PORT --route NAME:LISTEN:TARGET [--route ...]

Each route forwards 127.0.0.1:LISTEN to 127.0.0.1:TARGET. Its faults are set
over HTTP on the control port, one JSON object per route, replacing the old:

    curl -X POST localhost:PORT/NAME -d '{"latency": [100, 500]}'
    curl -X POST localhost:PORT/NAME -d '{}'                         # healthy

    latency  [lo, hi] ms   each chunk is held for U(lo, hi), order kept per direction
    reset    p             each chunk resets both sockets with probability p
    refuse   true          new connections are reset at once, live ones too
    blackhole true         nothing moves either way and nothing is closed (a
                           partition or a hung disk), new connections hang too
    timeout  p             a new connection hangs (blackholed) with probability p

GET /NAME returns the route's faults and its counters. Stdlib only.
"""
import argparse
import asyncio
import json
import random
import socket
import struct
import sys
import time


class Route:
    def __init__(self, name, listen, target):
        self.name, self.listen, self.target = name, listen, target
        self.faults = {}
        self.conns = set()
        self.stats = {"conns": 0, "resets": 0, "bytes": 0, "hung": 0}
        self.changed = asyncio.Event()

    def set(self, faults):
        self.faults = faults
        self.changed.set()
        self.changed = asyncio.Event()
        if faults.get("refuse"):
            for c in list(self.conns):
                c.reset()


def rst(w):
    # SO_LINGER 0: close() sends a RST, like a crashed peer or a dropped NAT entry
    try:
        s = w.get_extra_info("socket")
        if s is not None:
            s.setsockopt(socket.SOL_SOCKET, socket.SO_LINGER, struct.pack("ii", 1, 0))
        w.transport.abort()
    except Exception:
        pass


class Conn:
    def __init__(self, route, cr, cw):
        self.route, self.cr, self.cw = route, cr, cw
        self.ur = self.uw = None
        self.dead = False

    def reset(self):
        if self.dead:
            return
        self.dead = True
        self.route.stats["resets"] += 1
        rst(self.cw)
        if self.uw:
            rst(self.uw)

    async def held(self):
        while self.route.faults.get("blackhole") and not self.dead:
            try:
                await asyncio.wait_for(self.route.changed.wait(), 1.0)
            except asyncio.TimeoutError:
                pass

    async def pump(self, r, w):
        # deliver times only move forward, so latency never reorders a stream
        last = 0.0
        try:
            while not self.dead:
                data = await r.read(65536)
                if not data:
                    break
                await self.held()
                f = self.route.faults
                if f.get("reset") and random.random() < f["reset"]:
                    self.reset()
                    break
                lat = f.get("latency")
                if lat:
                    due = max(last, time.monotonic() + random.uniform(lat[0], lat[1]) / 1000)
                    last = due
                    await asyncio.sleep(max(0.0, due - time.monotonic()))
                await self.held()
                if self.dead:
                    break
                w.write(data)
                self.route.stats["bytes"] += len(data)
                await w.drain()
        except (ConnectionError, OSError, asyncio.IncompleteReadError):
            pass
        finally:
            if not self.dead:
                try:
                    w.write_eof()
                except Exception:
                    try:
                        w.close()
                    except Exception:
                        pass

    async def run(self):
        rt = self.route
        f = rt.faults
        rt.stats["conns"] += 1
        if f.get("refuse"):
            self.reset()
            return
        if f.get("timeout") and random.random() < f["timeout"]:
            rt.stats["hung"] += 1
            # hold the socket open with nothing behind it until the client gives up
            try:
                while await self.cr.read(65536):
                    pass
            except Exception:
                pass
            rst(self.cw)
            return
        await self.held()
        try:
            self.ur, self.uw = await asyncio.open_connection("127.0.0.1", rt.target)
        except OSError:
            self.reset()
            return
        rt.conns.add(self)
        try:
            await asyncio.gather(self.pump(self.cr, self.uw), self.pump(self.ur, self.cw))
        finally:
            rt.conns.discard(self)
            for w in (self.cw, self.uw):
                try:
                    w.close()
                except Exception:
                    pass


async def control(routes, reader, writer):
    try:
        head = await reader.readuntil(b"\r\n\r\n")
        lines = head.decode().split("\r\n")
        method, path, _ = lines[0].split(" ", 2)
        n = 0
        for l in lines[1:]:
            if l.lower().startswith("content-length:"):
                n = int(l.split(":", 1)[1])
        body = await reader.readexactly(n) if n else b""
        name = path.strip("/")
        if name == "" and method == "GET":
            out = {k: {"faults": r.faults, **r.stats} for k, r in routes.items()}
            code = 200
        elif name not in routes:
            out, code = {"error": f"no route {name}"}, 404
        elif method == "POST":
            routes[name].set(json.loads(body or b"{}"))
            out, code = {"faults": routes[name].faults}, 200
        else:
            out, code = {"faults": routes[name].faults, **routes[name].stats}, 200
        b = json.dumps(out).encode()
        writer.write(f"HTTP/1.1 {code} X\r\nContent-Type: application/json\r\nContent-Length: {len(b)}\r\nConnection: close\r\n\r\n".encode() + b)
        await writer.drain()
    except Exception as e:
        print(f"proxy control: {e}", file=sys.stderr)
    finally:
        writer.close()


async def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--control", type=int, required=True)
    ap.add_argument("--route", action="append", default=[])
    a = ap.parse_args()
    routes = {}
    servers = []
    for spec in a.route:
        name, listen, target = spec.split(":")
        r = Route(name, int(listen), int(target))
        routes[name] = r
        servers.append(await asyncio.start_server(lambda cr, cw, r=r: Conn(r, cr, cw).run(), "127.0.0.1", r.listen, backlog=1024))
    servers.append(await asyncio.start_server(lambda cr, cw: control(routes, cr, cw), "127.0.0.1", a.control))
    print(f"proxy: {len(routes)} routes, control :{a.control}", flush=True)
    await asyncio.gather(*(s.serve_forever() for s in servers))


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        pass
