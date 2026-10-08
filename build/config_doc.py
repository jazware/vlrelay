#!/usr/bin/env python3
"""docs/operations/configuration.md from `vlrelay --help` on stdin (just config-doc).

Flags not named in SECTIONS land under "Other", so a new flag shows up
before anyone files it.
"""
import re
import sys

SECTIONS = [
    ("Serving", "", ["listen", "trusted-proxy", "admin-token", "admin-token-file", "admin-listen",
                     "admin-proxy-header", "admin-proxy-from", "admin-operators", "ui-dir"]),
    (
        "Bucket",
        "Without `--memory`, the four `--s3-*` values are required (the keys directly or from their files). "
        "Every node of a cluster uses the same bucket and `--prefix`. [Deploy](deploy.md#the-bucket) has the setup "
        "for S3, R2 and MinIO.",
        ["memory", "s3-endpoint", "s3-bucket", "s3-access-key", "s3-access-key-file", "s3-secret-key", "s3-secret-key-file", "s3-region", "s3-unsigned-payload", "prefix"],
    ),
    (
        "Upstreams and identity",
        "`--host` and `--crawl` work on any member: a host admitted anywhere goes into the leader's host table, "
        "and the leader gives it to a member.",
        ["host", "crawl", "host-tier", "plc-url", "plc-export", "plc-export-url", "plc-export-rate", "plc-export-streams", "plc-seeds-slatedb", "plc-seeds-dir", "plc-seed-reads", "plc-export-mem-mb", "bootstrap-relay", "dev-mode", "did-lookups-per-sec", "did-lookup-prefetch", "did-web-seed-ttl-secs"],
    ),
    ("Pipeline and serving", "", ["lanes", "ingest-threads", "host-inflight-events", "host-inflight-mb", "inflight-events", "inflight-mb", "ring-mb", "max-lag-mb", "log-compression",
                              "event-horizon-secs", "lag-case-minutes", "lag-case-sustain-secs", "lag-case-grace-secs",
                              "lag-case-pressure-pct", "lag-case-resolve-secs"]),
    (
        "Quorum log",
        "Every member uses the same bucket and `--prefix`. A node with no `--qlog-peer` is a single node with "
        "its commitlog as the WAL ([Cluster](../cluster.md)).",
        ["node-id", "qlog-listen", "qlog-peer", "qlog-members", "qlog-dir", "qlog-flush-ms", "qlog-headroom",
         "qlog-admin-token", "qlog-admin-token-file", "qlog-retain-hours", "qlog-retain-secs", "qlog-retain-every-secs",
         "qlog-host-failover-ms", "qlog-host-poll-ms", "qlog-election-ms", "qlog-heartbeat-ms",
         "qlog-state-compactor-poll-ms", "qlog-state-slatedb", "qlog-no-auto-recover", "qlog-segment-mb", "qlog-disk-retain-mb", "durability", "durability-sync-ms", "slatedb-cache-mb",
         "slatedb-meta-mb", "slatedb-disk-cache-dir", "slatedb-disk-cache-mb",
         "qlog-memory-mb"],
    ),
    (
        "Chaos",
        "For the chaos harness (`tests/qlog/relay-chaos.sh`); never on a production node.",
        ["qlog-crash-at", "qlog-crash-prob", "qlog-crash-stop-file", "qlog-power-cut-on-usr1", "qlog-fsync-delay-us", "qlog-unsafe-trust-log"],
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
  caption: One binary, configured by flags or env vars. The bucket and the prefix are the relay's identity. Everything else is how this node serves, which upstreams it starts with, and its place in the quorum log.
  nodes:
    - { id: bin, label: vlrelay, sub: flags · VLRELAY_* env, at: [14, 4.5], size: [9, 3], tone: accent }
    - { id: serve, label: Serving, sub: "--listen · --admin-token", at: [0, 0], size: [10, 3], tone: blue }
    - { id: up, label: Upstreams, sub: "--host · --crawl", at: [0, 4.5], size: [10, 3], tone: muted }
    - { id: clu, label: Quorum log, sub: "--qlog-peer · --qlog-dir", at: [0, 9], size: [10, 3], tone: accent }
    - { id: bucket, label: Bucket, sub: "--s3-* · --prefix", at: [27, 0], size: [10, 3], shape: store, tone: amber }
    - { id: log, label: Flush, sub: "--qlog-flush-ms · --qlog-retain-hours", at: [27, 4.5], size: [10, 3], tone: amber }
    - { id: pipe, label: Pipeline, sub: "--lanes · --ingest-threads", at: [27, 9], size: [10, 3], tone: accent }
  edges:
    - serve.r -> bin.l30
    - up.r -> bin.l
    - clu.r -> bin.l70
    - bin.r30 -> bucket.l
    - bin.r -> log.l
    - bin.r70 -> pipe.l
facts:
  - { value: "2980", label: the public port, note: "`--listen`; the image binds 0.0.0.0", tone: accent }
  - { value: "30", unit: s, label: bucket flush, note: "`--qlog-flush-ms`; the log, the records and the hosts at one seq", tone: amber }
  - { value: "72", unit: h, label: of log for cursor replay, note: "`--qlog-retain-hours`", tone: blue }
  - { value: "flag", label: wins over its env var, note: "pass secrets as files; --help never prints them", tone: violet }
```

Every flag of `vlrelay`, generated from `vlrelay --help` by `just config-doc` (`build/config_doc.py`).
Flags with an env var can be set either way, and the flag wins. Each secret (`--s3-access-key`,
`--s3-secret-key`, `--admin-token`, `--qlog-admin-token`) has a `-file` twin (`VLRELAY_ADMIN_TOKEN_FILE`
and so on) that reads it from a file once at start, less one trailing newline. A file keeps the secret
out of the container's env, which `docker inspect` and a rendered compose file show. Setting a secret
and its file is an error, so is an empty file, and no error prints a secret. `--help` never prints
their values either.

The image sets `VLRELAY_LISTEN=0.0.0.0:2980` and passes `--ui-dir /usr/share/vlrelay/ui` in its
entrypoint ([Deploy](deploy.md#the-image)).
"""


FOOTER = """## A small box

A single node at ~3,400 hosts runs on ~1.3 GB of heap and half a core without `--plc-export`. On
a 2 vCPU / 4 GB box:

- With `--plc-export`, give the node `--plc-seeds-dir` on local disk. Every member then keeps
  the seeds as a table there, ~58 B a DID with nothing per DID in memory: 5.0 GB at 91M DIDs,
  15 GB at 274M, 25 GB at 457M. A lookup is one 8 KiB read, p50 ~0.1-0.2 ms and p99 0.5-6 ms
  cold on NVMe under a 512 MiB cgroup, at any of those sizes. A member builds it from one scan of
  the bucket's seed database (229 s at 99M rows on 2 cores against R2's latency, 3,365 GETs,
  peak 187 MB), then follows its changelog every 10 s (~0.2 GETs/s). The table survives
  restarts, and a member whose cursor is older than the 3-day changelog rebuilds.
- Without it, a seed lookup reads the seed database. Every row is a merge, so a lookup probes
  each sorted run's filter, and the filters and indexes are ~2 B a row (~190 MiB at 99M ops). At
  99M ops and 14 sorted runs, with the default 64 MiB metadata share, lookups ran ~35/s at 32 at
  once, p50 0.9 s, ~5 bucket GETs each. `--slatedb-disk-cache-dir` with
  `--slatedb-meta-mb 224` gets that to ~4,900/s, p50 5.7 ms and no GETs once the SSTs (8-15 GB)
  are on local disk, but the metadata stops fitting past ~110M rows.
- `--slatedb-disk-cache-dir` keeps the state's SSTs on local disk too (all of
  `--slatedb-disk-cache-mb` with `--plc-seeds-dir`). The seeder reads the account's record
  beside the seed, so both should be local.
- Give it `--plc-export-mem-mb` (1800 under a 2300 MiB container limit). The budget pauses the
  export and the seed reads while the process is over it and resumes below 85%.
  `--plc-export-rate` is requests a second across every window, ~1,000 ops each.
- `--qlog-disk-retain-mb 1024`: a single node has no followers to catch up, so the disk only
  serves cursors older than `--ring-mb`, and older ones read the bucket. A start reads all of
  it, about 7 s for the default 4 GiB.
- Keep `--slatedb-cache-mb` at 320. The state's and the seeds' databases share it.
- To see where the heap goes, build with `--features heap-profiling` (the Dockerfile's
  `VLRELAY_FEATURES` build argument) and start the node with
  `_RJEM_MALLOC_CONF=prof:true,lg_prof_sample:19,prof_gdump:true,prof_prefix:<dir>/heap`: jemalloc
  writes a profile at each new peak, which `jeprof --text <binary> <file>` reads.
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
            # a one-line help ends with its tags
            if m := re.search(r"\s*\[env: ([A-Z0-9_]+)=?[^\]]*\]", s):
                cur["env"] = m.group(1)
                s = s[: m.start()] + s[m.end():]
            if m := re.search(r"\s*\[default: ([^\]]*)\]", s):
                cur["default"] = m.group(1)
                s = s[: m.start()] + s[m.end():]
            cur["help"].append(s.strip())
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
    out.append(FOOTER)
    sys.stdout.write("\n".join(out))


if __name__ == "__main__":
    main()
