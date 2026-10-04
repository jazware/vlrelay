#!/usr/bin/env python3
"""Account states on both relays: deactivate one account at its PDS, take
another down on each relay (each relay's own admin API), then diff the sync
API and wait for the #account frames. Then undo both and diff again.

states.py --vl http://127.0.0.1:3480 --vl-token compat --indigo http://127.0.0.1:3470 \
          --indigo-password compat --accounts dev/state/accounts.json --out DIR
"""
import argparse
import base64
import json
import subprocess
import sys
import time
import urllib.request

ap = argparse.ArgumentParser()
ap.add_argument("--vl", required=True)
ap.add_argument("--vl-token", required=True)
ap.add_argument("--indigo", required=True)
ap.add_argument("--indigo-password", required=True)
ap.add_argument("--accounts", required=True)
ap.add_argument("--out", required=True)
args = ap.parse_args()
here = __file__.rsplit("/", 1)[0]


def call(url, body=None, headers=None, method=None):
    data = json.dumps(body).encode() if body is not None else None
    req = urllib.request.Request(url, data=data, method=method or ("POST" if data is not None else "GET"))
    req.add_header("content-type", "application/json")
    for k, v in (headers or {}).items():
        req.add_header(k, v)
    try:
        with urllib.request.urlopen(req, timeout=10) as r:
            b = r.read()
            return r.status, (json.loads(b) if b else None)
    except urllib.error.HTTPError as e:
        return e.code, e.read().decode(errors="replace")[:300]


def basic(user, pw):
    return {"authorization": "Basic " + base64.b64encode(f"{user}:{pw}".encode()).decode()}


accts = json.load(open(args.accounts))
# one account per host for each action, from different hosts
by_host = {}
for a in accts:
    by_host.setdefault(a["host"], []).append(a)
hosts = sorted(by_host)
deact, tdown = by_host[hosts[0]][0], by_host[hosts[1]][0]


def session(a):
    s, v = call(f"{a['host']}/xrpc/com.atproto.server.createSession", {"identifier": a["did"], "password": a["password"]})
    assert s == 200, (s, v)
    return {"authorization": f"Bearer {v['accessJwt']}"}


def takedown(did, on):
    r1 = call(f"{args.vl}/admin/api/accounts/{did}/{'takedown' if on else 'untakedown'}",
              {"reason": "compat"} if on else {}, basic("admin", args.vl_token))
    r2 = call(f"{args.indigo}/admin/repo/{'takeDown' if on else 'reverseTakedown'}", {"did": did},
              basic("admin", args.indigo_password))
    return {"vl": r1[0], "indigo": r2[0]}


def watch(name, secs):
    procs = []
    for side, url in (("vl", args.vl), ("in", args.indigo)):
        ws = url.replace("http://", "ws://")
        f = open(f"{args.out}/states-{name}-{side}.json", "w")
        procs.append(subprocess.Popen([f"{here}/scratch/bin/gocheck", "--url", ws, "--secs", str(secs), "--accounts-out",
                                       f"{args.out}/states-{name}-{side}-frames.jsonl"], stdout=f, stderr=subprocess.DEVNULL))
    return procs


def syncdiff(name):
    with open(f"{args.out}/states-{name}-syncdiff.txt", "w") as f:
        subprocess.run([sys.executable, f"{here}/syncdiff.py", "--a", args.vl, "--b", args.indigo, "--accounts", args.accounts,
                        "--json-out", f"{args.out}/states-{name}-syncdiff.json"], stdout=f, check=True)


log = {"deactivated": deact["did"], "takendown": tdown["did"]}
procs = watch("on", 6)
time.sleep(1)
h = session(deact)
log["deactivate"] = call(f"{deact['host']}/xrpc/com.atproto.server.deactivateAccount", {}, h)[0]
log["takedown"] = takedown(tdown["did"], True)
for p in procs:
    p.wait()
syncdiff("on")

procs = watch("off", 6)
time.sleep(1)
# a deactivated account's old access token can be refused: log in again
log["activate"] = call(f"{deact['host']}/xrpc/com.atproto.server.activateAccount", {}, session(deact))
log["untakedown"] = takedown(tdown["did"], False)
for p in procs:
    p.wait()
syncdiff("off")
print(json.dumps(log))
