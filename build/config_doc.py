#!/usr/bin/env python3
"""docs/operations/configuration.md from `vlrelay --help` on stdin (just config-doc).

Flags not named in SECTIONS land under "Other", so a new flag shows up
before anyone files it.
"""
import re
import sys

SECTIONS = [
    ("Serving", "", ["listen", "trusted-proxy", "admin-token", "ui-dir"]),
    (
        "Bucket",
        "Without `--memory`, the four `--s3-*` values are required. Every node of a cluster uses the same "
        "bucket and `--prefix`.",
        ["memory", "s3-endpoint", "s3-bucket", "s3-access-key", "s3-secret-key", "s3-region", "s3-unsigned-payload", "prefix"],
    ),
    (
        "Upstreams and identity",
        "`--host` and `--crawl` work on any core node of a cluster, since the host registry is in the bucket.",
        ["host", "crawl", "host-tier", "plc-url", "dev-mode", "did-lookups-per-sec"],
    ),
    (
        "PLC export seeding",
        "A cold relay would resolve each of ~56M accounts once at the PLC lookup budget (about 31 h at 500/s). "
        "With `--plc-export` it reads the directory's `/export` instead and keeps each did:plc's key and PDS "
        "in its DID shard, so a cache miss costs no lookup. The cursors checkpoint to "
        "`plc/export-checkpoint.json` in the bucket, so a restart resumes; once caught up it follows the "
        "export's tail. A signature that fails against a seeded key, and every `#identity`, still resolve "
        "from PLC ([Policy](../policy.md#plc-export-seeding)).",
        ["plc-export", "plc-export-url", "plc-export-rate", "plc-export-streams"],
    ),
    (
        "Log",
        "Time to firehose is about linger plus one segment PUT. Above ~50k events/s segments seal on size "
        "before the linger is up ([Performance](../perf.md)).",
        ["linger-ms", "log-inflight", "max-segment-mb", "log-compression", "retention"],
    ),
    ("Pipeline and state", "", ["did-shards", "lanes", "ingest-threads"]),
    (
        "Cluster",
        "Without `--cluster` or `--role` the node runs alone. Core and edge nodes need `--peer-tls-dir` and "
        "`--internal-token`, and a replica needs neither ([Deploy](deploy.md#a-cluster), [Cluster](../cluster.md)).",
        ["node-id", "cluster", "role", "peer-listen", "advertise-url", "peer-tls-dir", "internal-token",
         "lease-ttl-ms", "host-shards"],
    ),
]

HEADER = """---
title: Configuration
section: Operations
order: 102
summary: "Every flag of vlrelay and its env var, generated from vlrelay --help by just config-doc."
---

```hero
diagram:
  caption: One binary, configured by flags or env vars. The bucket and the prefix are the relay's identity. Everything else is how this node serves, which upstreams it starts with, how its log seals segments, and its place in a cluster.
  nodes:
    - { id: bin, label: vlrelay, sub: flags · VLRELAY_* env, at: [14, 4.5], size: [9, 3], tone: accent }
    - { id: serve, label: Serving, sub: "--listen · --admin-token", at: [0, 0], size: [10, 3], tone: blue }
    - { id: up, label: Upstreams, sub: "--host · --crawl · --plc-export", at: [0, 4.5], size: [10, 3], tone: muted }
    - { id: clu, label: Cluster, sub: "--role · --peer-tls-dir", at: [0, 9], size: [10, 3], tone: accent }
    - { id: bucket, label: Bucket, sub: "--s3-* · --prefix", at: [27, 0], size: [10, 3], shape: store, tone: amber }
    - { id: log, label: Log, sub: "--linger-ms · --retention", at: [27, 4.5], size: [10, 3], tone: amber }
    - { id: pipe, label: Pipeline, sub: "--did-shards · --lanes", at: [27, 9], size: [10, 3], tone: accent }
  edges:
    - serve.r -> bin.l30
    - up.r -> bin.l
    - clu.r -> bin.l70
    - bin.r30 -> bucket.l
    - bin.r -> log.l
    - bin.r70 -> pipe.l
facts:
  - { value: "2980", label: the public port, note: "`--listen`; the image binds 0.0.0.0", tone: accent }
  - { value: "25", unit: ms, label: segment linger, note: "`--linger-ms`; time to firehose is about linger plus one PUT", tone: amber }
  - { value: "72", unit: h, label: of log for cursor replay, note: "`--retention`", tone: blue }
  - { value: "flag", label: wins over its env var, note: "pass secrets as env vars; --help never prints them", tone: violet }
```

Every flag of `vlrelay`, generated from `vlrelay --help` by `just config-doc` (`build/config_doc.py`).
Flags with an env var can be set either way, and the flag wins. Secrets (`--s3-secret-key`,
`--admin-token`, `--internal-token`) are best passed as env vars, and `--help` never prints their
values.

The image sets `VLRELAY_LISTEN=0.0.0.0:2980` and passes `--ui-dir /usr/share/vlrelay/ui` in its
entrypoint ([Deploy](deploy.md#the-image)).
"""


def parse(text):
    flags = {}
    cur = None
    for line in text.splitlines():
        m = re.match(r"^\s{2,6}(?:-\w, )?--([a-z0-9-]+)(?: <([A-Z0-9_]+)>)?\s*$", line)
        if m:
            cur = {"name": m.group(1), "value": m.group(2), "help": [], "env": None, "default": None,
                   "values": []}
            flags[cur["name"]] = cur
            continue
        if cur is None:
            continue
        s = line.strip()
        if not s:
            continue
        if m := re.match(r"^\[env: ([A-Z0-9_]+)", s):
            cur["env"] = m.group(1)
        elif m := re.match(r"^\[default: (.*)\]$", s):
            cur["default"] = m.group(1)
        elif m := re.match(r"^- ([a-z0-9-]+):\s+(.*)$", s):
            cur["values"].append((m.group(1), m.group(2)))
        elif s == "Possible values:":
            pass
        else:
            cur["help"].append(s)
    for f in ("help", "version"):
        flags.pop(f, None)
    return flags


def cell(s):
    return s.replace("|", "\\|")


# For flags whose doc comment in src/main.rs is empty (--help prints nothing).
FALLBACK = {
    "s3-endpoint": "The S3 API endpoint, e.g. `https://s3.us-east-1.amazonaws.com` or `http://minio:9000`",
    "s3-bucket": "The bucket",
    "s3-access-key": "Access key id",
    "s3-secret-key": "Secret access key",
    "s3-region": "The bucket's region",
    "plc-url": "The PLC directory `did:plc` documents are resolved against",
}


def row(f):
    flag = f"`--{f['name']}" + (f" <{f['value']}>`" if f["value"] else "`")
    env = f"`{f['env']}`" if f["env"] else ""
    default = f"`{f['default']}`" if f["default"] else ""
    what = " ".join(f["help"]) or FALLBACK.get(f["name"], "")
    if f["values"]:
        what += " " + " · ".join(f"`{v}`: {d}" for v, d in f["values"])
    return f"| {flag} | {env} | {default} | {cell(what)} |"


def main():
    flags = parse(sys.stdin.read())
    out = [HEADER]
    sections = list(SECTIONS)
    other = [n for n in flags if n not in {x for _, _, names in SECTIONS for x in names}]
    if other:
        sections.append(("Other", "", other))
    for title, intro, names in sections:
        names = [n for n in names if n in flags]
        if not names:
            continue
        out.append(f"## {title}\n")
        if intro:
            out.append(intro + "\n")
        out.append("| Flag | Env | Default | What |\n|---|---|---|---|")
        out.extend(row(flags[n]) for n in names)
        out.append("")
    sys.stdout.write("\n".join(out))


if __name__ == "__main__":
    main()
