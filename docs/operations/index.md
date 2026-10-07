---
title: Operations
section: Operations
order: 100
summary: "Running vlRelay: building the image, deploying one node or a three-node quorum cluster, configuring it, and watching it."
---

```hero
diagram:
  caption: An operator's loop. Deploy points a node at a bucket, a prefix and a local disk for its commitlog, the proxy puts TLS in front, Prometheus scrapes /metrics, and the dashboard at /admin shows the live view, the quorum's members and the policy. Upgrades restart one node at a time.
  nodes:
    - { id: bucket, label: Object store, sub: bucket + prefix, at: [0, 0.2], size: [8, 2.6], shape: store, tone: amber }
    - { id: keys, label: Secrets, sub: "S3 keys · tokens", at: [0, 5.2], size: [8, 2.6], tone: muted }
    - { id: deploy, label: Deploy, sub: image · compose, at: [12, 2.6], size: [8, 3], tone: accent }
    - { id: proxy, label: TLS proxy, sub: "wss:// · https://", at: [24, 0], size: [8, 3], tone: muted }
    - { id: watch, label: Monitoring, sub: "`/metrics`", at: [24, 5.2], size: [8, 3], tone: blue }
    - { id: admin, label: Dashboard, sub: "`/admin` · quorum · policy", at: [36, 2.6], size: [9, 3], tone: accent }
    - { id: upgrade, label: Upgrades, sub: one node at a time, at: [12, 9.5], size: [8, 3], tone: accent }
  edges:
    - "bucket.r -> deploy.l30"
    - "keys.r -> deploy.l70"
    - "deploy.r -> proxy.l: --listen"
    - "deploy.r -> watch.l: scrape"
    - { from: proxy.r, to: admin.l30 }
    - { from: watch.r, to: admin.l70, dash: true }
    - { from: deploy.b, to: upgrade.t, label: new image }
facts:
  - { value: "1", unit: binary, label: and one bucket prefix per relay, note: "the dashboard and the docs are in the image" }
  - { value: "1", unit: NVMe disk, label: per node, note: "`--qlog-dir`; everything older than a flush is in the bucket", tone: amber }
  - { value: "2 of 3", label: nodes up to commit, note: "so upgrades restart one node at a time", tone: violet }
  - { value: "~1.6", unit: Gb/s, label: per full-firehose consumer, note: "at 33k events/s; size the NIC, not the CPU", tone: blue }
```

These pages are for whoever runs a vlRelay, from one node in memory to a three-node quorum
cluster. Read [Deploy](deploy.md) first. It covers the image (`ghcr.io/jazware/vlrelay`, or
[built from the repository](deploy.md#the-image)), the bucket and the proxy.
[Configuration](configuration.md) lists every flag, and [Monitoring](monitoring.md) says what to
watch. The [Overview](../overview.md) explains the design in one screen, and
[Cluster](../cluster.md) explains the quorum log.

```pages
{}
```

## What there isn't yet

- Alert rules, a runbook and Grafana dashboards. The dashboard at `/admin` covers the live view,
  and [Monitoring](monitoring.md) lists the series to alert on.
- An Ansible kit. [vlpds's kit](https://github.com/jazware/vlpds/tree/main/deploy/ansible) is
  the model for one, and most of it (base hardening, Caddy, Alloy, secrets as files) would carry
  over unchanged.
- Feature levels for upgrades. Nothing stops a new version from writing something an old one can't
  read, so roll one node at a time and don't roll back across a format change.
