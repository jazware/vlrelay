---
title: Operations
section: Operations
order: 100
summary: "Running vlRelay: building the image, deploying one node or a cluster, configuring it, and watching it."
---

```hero
diagram:
  caption: An operator's loop. Deploy points a node at a bucket and a prefix, the proxy puts TLS in front, Prometheus scrapes /metrics, and the dashboard at /admin shows the live view and edits the policy. Upgrades are SIGTERM and a restart of the same binary on the same bucket.
  nodes:
    - { id: bucket, label: Object store, sub: bucket + prefix, at: [0, 0.2], size: [8, 2.6], shape: store, tone: amber }
    - { id: keys, label: Secrets, sub: "S3 keys · tokens · peer TLS", at: [0, 5.2], size: [8, 2.6], tone: muted }
    - { id: deploy, label: Deploy, sub: image · compose, at: [12, 2.6], size: [8, 3], tone: accent }
    - { id: proxy, label: TLS proxy, sub: "wss:// · https://", at: [24, 0], size: [8, 3], tone: muted }
    - { id: watch, label: Monitoring, sub: "`/metrics`", at: [24, 5.2], size: [8, 3], tone: blue }
    - { id: admin, label: Dashboard, sub: "`/admin` · policy · cases", at: [36, 2.6], size: [9, 3], tone: accent }
    - { id: upgrade, label: Upgrades, sub: SIGTERM · one core at a time, at: [12, 9.5], size: [8, 3], tone: accent }
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
  - { value: "0", unit: local state, label: on any node, note: "restart anywhere with the bucket's credentials", tone: amber }
  - { value: "30 s", label: stop grace for SIGTERM, note: "the compose files' `stop_grace_period`; a core hands its shards over first", tone: violet }
  - { value: "~1.6", unit: Gb/s, label: per full-firehose consumer, note: "at 33k events/s; size the NIC, not the CPU", tone: blue }
```

These pages are for whoever runs a vlRelay, from one node in memory to a cluster of cores with
edges and replicas. Read [Deploy](deploy.md) first. [Configuration](configuration.md) lists every
flag, and [Monitoring](monitoring.md) says what to watch. The [Overview](../overview.md) explains
the design in one screen.

```pages
{}
```

## What there isn't yet

- A published image. You build it from the repository ([Deploy](deploy.md#the-image)).
- Alert rules, a runbook and Grafana dashboards. The dashboard at `/admin` covers the live view,
  and [Monitoring](monitoring.md) lists the series to alert on.
- An Ansible kit. vlpds's kit is the model for one, and most of it (base hardening, Caddy, Alloy,
  secrets as files) would carry over unchanged.
- Feature levels for upgrades. Nothing stops a new version from writing something an old one can't
  read, so roll one core at a time and don't roll back across a format change.
