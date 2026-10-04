#!/usr/bin/env python3
"""docs/operations/configuration.md from `vlrelay --help` on stdin (just config-doc).

Flags not named in SECTIONS land under "Other", so a new flag shows up
before anyone files it.
"""
import re
import sys

SECTIONS = [
    ("Serving", "", ["listen", "admin-token", "ui-dir"]),
    (
        "Bucket",
        "Without `--memory`, the four `--s3-*` values are required. Every node of a cluster uses the same "
        "bucket and `--prefix`.",
        ["memory", "s3-endpoint", "s3-bucket", "s3-access-key", "s3-secret-key", "s3-region", "prefix"],
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
        ["linger-ms", "log-inflight", "max-segment-mb", "retention"],
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

HEADER = """# Configuration

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
