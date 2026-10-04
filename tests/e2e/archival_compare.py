#!/usr/bin/env python3
"""Compares the relay's archival getRepo with each upstream's, for every
account the dev network seeded (tests/e2e/archival.sh).

Per DID: the same root commit, the same set of blocks, and whether the bytes
are identical. Block order in a CAR is the server's choice, so a mirror can
only be byte-identical with a server that orders blocks the way vlpds does.

  archival_compare.py RELAY_URL ACCOUNTS_JSON [--json OUT] [--retries N]
"""
import json
import sys
import time
import urllib.error
import urllib.request


def varint(b, i):
    n = shift = 0
    while True:
        c = b[i]
        i += 1
        n |= (c & 0x7F) << shift
        if c < 0x80:
            return n, i
        shift += 7


def read_cid(b, i):
    start = i
    version, i = varint(b, i)
    if version != 1:
        raise ValueError("CIDv0 in a CAR")
    _, i = varint(b, i)  # codec
    _, i = varint(b, i)  # multihash code
    n, i = varint(b, i)
    i += n
    return bytes(b[start:i]), i


def read_car(b):
    hlen, i = varint(b, 0)
    i += hlen
    blocks = {}
    order = []
    while i < len(b):
        n, i = varint(b, i)
        end = i + n
        cid, j = read_cid(b, i)
        blocks[cid] = bytes(b[j:end])
        order.append(cid)
        i = end
    return blocks, order


def root_of(b):
    """The header's one root: {roots: [tag 42 bytes(0x00 ++ cid)], version: 1}."""
    hlen, i = varint(b, 0)
    h = bytes(b[i:i + hlen])
    at = h.index(b"\xd8\x2a") + 2
    n = h[at] - 0x40 if h[at] < 0x58 else h[at + 1]
    at += 1 if h[at] < 0x58 else 2
    return h[at + 1:at + n]


def get(url, timeout=60):
    try:
        with urllib.request.urlopen(url, timeout=timeout) as r:
            return r.status, r.read()
    except urllib.error.HTTPError as e:
        return e.code, e.read()


def compare(relay, acct):
    did = acct["did"]
    up = acct["host"].rstrip("/")
    s1, a = get(f"{up}/xrpc/com.atproto.sync.getRepo?did={did}")
    s2, b = get(f"{relay}/xrpc/com.atproto.sync.getRepo?did={did}")
    if s1 != 200 and s1 == s2 and json.loads(a).get("error") == json.loads(b).get("error"):
        # an account deactivated when the load stopped: both say so
        return {"did": did, "host": up, "result": "same_error", "error": json.loads(a).get("error")}
    if s1 != 200 or s2 != 200:
        err = lambda s, body: f"{s} {body[:120]!r}" if s != 200 else "200"
        return {"did": did, "host": up, "result": "error", "upstream": err(s1, a), "relay": err(s2, b)}
    if a == b:
        return {"did": did, "host": up, "result": "identical", "bytes": len(a)}
    ba, _ = read_car(a)
    bb, _ = read_car(b)
    same_root = root_of(a) == root_of(b)
    if same_root and ba == bb:
        return {"did": did, "host": up, "result": "same_blocks", "bytes": len(a), "relay_bytes": len(b)}
    return {
        "did": did,
        "host": up,
        "result": "differ",
        "same_root": same_root,
        "only_upstream": len(set(ba) - set(bb)),
        "only_relay": len(set(bb) - set(ba)),
    }


def main():
    args = sys.argv[1:]
    relay, accounts = args[0].rstrip("/"), args[1]
    out = args[args.index("--json") + 1] if "--json" in args else None
    retries = int(args[args.index("--retries") + 1]) if "--retries" in args else 3
    accts = json.load(open(accounts))
    results = []
    for acct in accts:
        for attempt in range(retries + 1):
            r = compare(relay, acct)
            # a deactivation or a commit landing between the two reads
            # resolves itself; anything else is a real difference
            if r["result"] in ("identical", "same_blocks", "same_error") or attempt == retries:
                break
            time.sleep(2)
        r["attempts"] = attempt + 1
        results.append(r)
    by_host = {}
    for r in results:
        h = by_host.setdefault(r["host"], {"identical": 0, "same_blocks": 0, "same_error": 0, "differ": 0, "error": 0, "bytes": 0})
        h[r["result"]] += 1
        h["bytes"] += r.get("bytes", 0)
    print(f"  {'host':<28} {'identical':>9} {'same blocks':>11} {'both inactive':>13} {'differ':>7} {'error':>6} {'MB':>7}")
    for h, c in sorted(by_host.items()):
        print(f"  {h:<28} {c['identical']:>9} {c['same_blocks']:>11} {c['same_error']:>13} {c['differ']:>7} {c['error']:>6} {c['bytes'] / 1e6:>7.2f}")
    bad = [r for r in results if r["result"] not in ("identical", "same_blocks", "same_error")]
    for r in bad[:10]:
        print("  MISMATCH", json.dumps(r))
    if out:
        json.dump({"by_host": by_host, "results": results}, open(out, "w"), indent=1)
    sys.exit(1 if bad else 0)


if __name__ == "__main__":
    main()
