# vlRelay docs

Read them roughly in this order. The design doc says what vlRelay is for and why it's shaped the
way it is, the operations pages say how to run it, and the internals pages say how each part works
and how it was measured.

## Overview

| Page | What's in it |
|---|---|
| [Design](design.html) | The design-session doc: the job, what carries over from vlpds, host and DID shards, sequencing, validation, policy, archival mode, replicas, cost and failure modes. Open it in a browser. |
| [Cost](cost.md) | What it costs today and at 10x, 100x and 1000x: CPU, peer traffic, bucket requests and storage, consumer egress, priced on OVH, Hetzner and AWS with six object stores (`scripts/cost_model.py`) |
| [Build plan](PLAN.md) | The design session's decisions (linger, where signatures are checked, archival defaults) and who owns which module |

## Operations

| Page | What's in it |
|---|---|
| [Deploy](operations/deploy.md) | The image, one node on Compose, a three-node cluster with peer mTLS, edges and replicas, the proxy in front |
| [Configuration](operations/configuration.md) | Every flag and env var, from `vlrelay --help` |
| [Monitoring](operations/monitoring.md) | The `/metrics` series and what to watch |
| [Dashboard and admin API](admin-api.md) | The `/admin` dashboard's JSON API, the policy document and the demo backend |

## Internals

| Page | What's in it |
|---|---|
| [Cluster](cluster.md) | Roles, leases, host and DID shards, handoff, restart dedupe, what's in the bucket, HA results |
| [Policy](policy.md) | Tiers, limits, domain rules, requestCrawl admission, spam counting and cases |
| [Performance](perf.md) | The node bench, each optimization pass, the per-node ceiling and fan-out |
| [Compatibility](compat.md) | indigo's consumers, goat, `@atproto/sync`, Jetstream and indigo's relay against vlRelay, every difference classified |
| [Shadow run](shadow.md) | Two hours against ten real PDSes beside `bsky.network`: matches, latency, policy on real traffic, leaks, bugs found |
| [Reference notes](reference-notes.md) | How indigo's relay and the production relay behave, the baseline vlRelay is checked against |

Archival mode (a full mirror of every repo, `docs/design.html` "Archival mode") is in progress and
has no page yet. The sequencer (`src/seq.rs`) is described in the design doc's "Sequencing and
cursors" and in [Cluster](cluster.md).

## Development

| Page | What's in it |
|---|---|
| [Dev loop](devloop.md) | Building and testing, the local network, the e2e contract, `e2e_check`, the cluster e2e, benchbox and build speed |
| [Load fleet](loadfleet.md) | `fakepds`, the synthetic upstream fleet behind the perf numbers |
