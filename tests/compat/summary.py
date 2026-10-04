#!/usr/bin/env python3
"""Summarize a run.sh output dir: one block per consumer, both relays side by side."""
import json
import os
import re
import sys
from collections import Counter

run = sys.argv[1]


def p(name):
    return os.path.join(run, name)


def load(name):
    try:
        return json.load(open(p(name)))
    except Exception as e:
        return {"_error": str(e)}


def lines(name):
    try:
        return [l.strip() for l in open(p(name)) if l.strip()]
    except Exception:
        return []


print("== e2e_check (upstreams -> relay; in-vs-vl: indigo's stream as the reference)")
for n in ("vl", "in", "in-vs-vl"):
    txt = lines(f"e2e-{n}.txt")
    start = max((i for i, l in enumerate(txt) if l.startswith("kind")), default=None)
    print(f"-- {n}")
    for l in txt[start:] if start is not None else txt[-12:]:
        print("   " + l)

print("\n== gocheck (indigo events.HandleRepoStream + sync 1.1 verifier)")
for side in ("vl", "in"):
    d = load(f"gocheck-{side}.json")
    print(f"-- {side}: frames={d.get('frames')} seqBackward={d.get('seqBackward')} verifyFailures={d.get('verify')} identity={d.get('identity')} ended={d.get('ended')}")
    for k, v in (d.get("verifySamples") or {}).items():
        print(f"     {k}: {v[:200]}")

print("\n== goat firehose --verify-basic --verify-sig --verify-mst (warnings by message)")
for side in ("vl", "in"):
    c = Counter()
    for l in lines(f"goat-{side}.err"):
        try:
            j = json.loads(l)
        except Exception:
            c[l[:100]] += 1
            continue
        if j.get("level") in ("WARN", "ERROR"):
            c[j.get("msg")] += 1
    print(f"-- {side}: {dict(c) or 'no warnings'}")

print("\n== @atproto/sync Firehose")
for n in ("vl", "in", "vl-coerced"):
    d = load(f"ts-{n}.json")
    s = (d.get("errorSamples") or [{}])[0]
    print(f"-- {n}: events={d.get('events')} errors={sum((d.get('errors') or {}).values())} seqTypes={(d.get('seqs') or {}).get('types')} {('e.g. ' + (s.get('cause') or s.get('message') or '')[:160]) if s else ''}")

print("\n== Jetstream (legacy) on vlRelay, restarted halfway")
d = load("jetstream-sub.json")
print(f"-- subscriber: kinds={d.get('kinds')} ops={d.get('ops')} uniqueCommits={d.get('commits')} timeBackward={d.get('timeBackward')} closes={d.get('closes')}")
e = load("e2e-vl.json")
log = "\n".join(lines("jetstream.log"))
errs = Counter(re.findall(r'"level":"(?:ERROR|WARN)","msg":"([^"]+)"', log))
print(f"-- jetstream log warnings/errors: {dict(errs) or 'none'}")
revs = set(lines("jetstream-revs.txt"))
relay = (load("gocheck-vl.json").get("frames") or {}).get("#commit")
print(f"-- unique (did, rev) through jetstream: {len(revs)}, vlRelay #commit frames in the same window: {relay}")

print("\n== cursors")
for side in ("vl", "in"):
    got, want = lines(f"resume-{side}.txt"), lines(f"resume-{side}.want")
    r = load(f"resume-{side}.json")
    verdict = "exact" if got == want else f"got {len(got)} want {len(want)}; first got {got[:1]} want {want[:1]}"
    print(f"-- {side} resume from the middle: {verdict} (ended: {r.get('ended')})")
    for n in ("future", "old"):
        d = load(f"{n}-{side}.json")
        print(f"   {n} cursor {d.get('cursor')}: frames={d.get('frames')} infos={d.get('infos')} errorFrames={d.get('errorFrames')} ended={d.get('ended')}")

d = load("old-short.json")
print(f"-- vlRelay --retention-secs 10, cursor 1: infos={d.get('infos')} firstSeq={d.get('firstSeq')} frames={d.get('frames')} ended={d.get('ended')}")
print(f"-- vlRelay --max-lag-mb 1, a consumer that stops reading: {json.dumps(load('slow.json'))}")

print("\n== account states (deactivate at the PDS, takedown on each relay, then undo)")
print("   " + json.dumps(load("states.json")))
for n in ("on", "off"):
    for side in ("vl", "in"):
        for l in lines(f"states-{n}-{side}-frames.jsonl"):
            print(f"   {n} {side}: {l}")
    for l in lines(f"states-{n}-syncdiff.txt"):
        if not l.startswith(("com.atproto.sync.getHostStatus {", "com.atproto.sync.getRepoStatus {", "com.atproto.sync.getLatestCommit {", "com.atproto.sync.listRepos {", "com.atproto.sync.listHosts {")):
            print(f"   {n} syncdiff: {l}")

print("\n== sync API after the run (a = vlRelay, b = indigo)")
for l in lines("syncdiff.txt"):
    print("   " + l)

print("\n== relay chaining")
print("-- vlRelay with indigo's relay as its upstream:")
for l in lines("chain-vl.txt"):
    print("   " + l)
print("-- indigo's relay with vlRelay as a host:")
for l in lines("chain-indigo.txt"):
    print("   " + l[:300])
