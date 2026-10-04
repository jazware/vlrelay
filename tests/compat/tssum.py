#!/usr/bin/env python3
"""One line per ts.sh result file: events by kind, errors, a sample cause, seq types."""
import json
import sys

for f in sys.argv[1:]:
    try:
        d = json.load(open(f))
    except Exception as e:
        print(f"{f}: unreadable ({e})")
        continue
    s = d["errorSamples"][0] if d["errorSamples"] else None
    cause = f" e.g. {s['cause'] or s['message']}" if s else ""
    print(f"{f.rsplit('/', 1)[-1]} [{d['mode']}] events={d['events']} errors={sum(d['errors'].values())}{cause} seqs={d['seqs']['types']} {d['seqs']['first']}..{d['seqs']['last']}")
