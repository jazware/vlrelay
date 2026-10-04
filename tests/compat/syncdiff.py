#!/usr/bin/env python3
"""Diff the sync API of two relays field for field over the same accounts and hosts.

syncdiff.py --a http://127.0.0.1:3480 --b http://127.0.0.1:3470 --accounts dev/state/accounts.json [--json-out f]

For each DID: getRepoStatus and getLatestCommit on both. Then listRepos
(paged to the end) and listHosts, getHostStatus for every host either lists.
A field that differs is reported with both values; status codes and error
names count as fields. Prints a summary per endpoint and field.
"""
import argparse
import json
import sys
import urllib.error
import urllib.parse
import urllib.request
from collections import Counter, defaultdict


def get(base, method, **params):
    q = urllib.parse.urlencode({k: v for k, v in params.items() if v is not None})
    url = f"{base}/xrpc/{method}" + (f"?{q}" if q else "")
    try:
        with urllib.request.urlopen(url, timeout=10) as r:
            return r.status, json.loads(r.read() or b"null")
    except urllib.error.HTTPError as e:
        body = e.read()
        try:
            return e.code, json.loads(body)
        except Exception:
            return e.code, {"raw": body.decode(errors="replace")[:200]}


def page_all(base, method, key, limit):
    out, cursor, pages = [], None, 0
    while True:
        s, v = get(base, method, limit=limit, cursor=cursor)
        if s != 200:
            return out, s, v
        out += v.get(key, [])
        pages += 1
        cursor = v.get("cursor")
        if not cursor or pages > 1000:
            return out, 200, pages


def shape(status, body):
    if status != 200:
        return {"_http": status, "error": (body or {}).get("error")}
    return dict(body, _http=status)


def diff(a, b):
    return {k: (a.get(k), b.get(k)) for k in sorted(set(a) | set(b)) if a.get(k) != b.get(k)}


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--a", required=True, help="first relay (vlRelay)")
    ap.add_argument("--b", required=True, help="second relay (reference)")
    ap.add_argument("--accounts", required=True)
    ap.add_argument("--json-out")
    ap.add_argument("--samples", type=int, default=3)
    args = ap.parse_args()

    dids = sorted({a["did"] for a in json.load(open(args.accounts))})
    counts = Counter()
    fields = defaultdict(Counter)
    samples = defaultdict(list)

    def record(ep, key, da, db):
        counts[(ep, "total")] += 1
        d = diff(da, db)
        if not d:
            counts[(ep, "same")] += 1
            return
        counts[(ep, "differ")] += 1
        for f, vals in d.items():
            fields[ep][f] += 1
            if len(samples[(ep, f)]) < args.samples:
                samples[(ep, f)].append({"key": key, "a": vals[0], "b": vals[1]})

    for did in dids:
        for ep in ("com.atproto.sync.getRepoStatus", "com.atproto.sync.getLatestCommit"):
            record(ep, did, shape(*get(args.a, ep, did=did)), shape(*get(args.b, ep, did=did)))

    ra, sa, _ = page_all(args.a, "com.atproto.sync.listRepos", "repos", 1000)
    rb, sb, _ = page_all(args.b, "com.atproto.sync.listRepos", "repos", 1000)
    ma, mb = {r["did"]: r for r in ra}, {r["did"]: r for r in rb}
    for did in sorted(set(ma) | set(mb)):
        record("com.atproto.sync.listRepos", did, ma.get(did, {"_absent": True}), mb.get(did, {"_absent": True}))

    ha, _, _ = page_all(args.a, "com.atproto.sync.listHosts", "hosts", 1000)
    hb, _, _ = page_all(args.b, "com.atproto.sync.listHosts", "hosts", 1000)
    mha, mhb = {h["hostname"]: h for h in ha}, {h["hostname"]: h for h in hb}
    for h in sorted(set(mha) | set(mhb)):
        record("com.atproto.sync.listHosts", h, mha.get(h, {"_absent": True}), mhb.get(h, {"_absent": True}))
        ep = "com.atproto.sync.getHostStatus"
        record(ep, h, shape(*get(args.a, ep, hostname=h)), shape(*get(args.b, ep, hostname=h)))

    # error shapes for inputs neither knows
    for ep, p in [
        ("com.atproto.sync.getRepoStatus", {"did": "did:plc:aaaaaaaaaaaaaaaaaaaaaaaa"}),
        ("com.atproto.sync.getLatestCommit", {"did": "did:plc:aaaaaaaaaaaaaaaaaaaaaaaa"}),
        ("com.atproto.sync.getRepoStatus", {"did": "not-a-did"}),
        ("com.atproto.sync.getHostStatus", {"hostname": "nope.example.com"}),
        ("com.atproto.sync.listRepos", {"limit": "0"}),
        ("com.atproto.sync.listRepos", {"limit": "1"}),
        ("com.atproto.sync.listHosts", {"limit": "2000"}),
    ]:
        sa_, va = get(args.a, ep, **p)
        sb_, vb = get(args.b, ep, **p)
        ka = {"_http": sa_, "error": (va or {}).get("error"), "cursor?": "cursor" in (va or {})}
        kb = {"_http": sb_, "error": (vb or {}).get("error"), "cursor?": "cursor" in (vb or {})}
        record(f"{ep} {p}", "edge", ka, kb)

    report = {
        "a": args.a,
        "b": args.b,
        "repos_listed": {"a": len(ra), "b": len(rb)},
        "hosts_listed": {"a": sorted(mha), "b": sorted(mhb)},
        "endpoints": {},
    }
    eps = sorted({ep for ep, _ in counts})
    for ep in eps:
        report["endpoints"][ep] = {
            "total": counts[(ep, "total")],
            "same": counts[(ep, "same")],
            "differ": counts[(ep, "differ")],
            "fields": {f: {"n": n, "samples": samples[(ep, f)]} for f, n in fields[ep].items()},
        }
    print(f"repos listed: a={len(ra)} b={len(rb)}; hosts: a={sorted(mha)} b={sorted(mhb)}")
    for ep in eps:
        e = report["endpoints"][ep]
        print(f"{ep}: {e['same']}/{e['total']} identical")
        for f, d in e["fields"].items():
            print(f"    {f}: {d['n']} differ, e.g. {json.dumps(d['samples'][0])[:300]}")
    if args.json_out:
        json.dump(report, open(args.json_out, "w"), indent=2)


if __name__ == "__main__":
    sys.exit(main())
