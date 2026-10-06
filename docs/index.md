# vlRelay docs

Every page in this directory, roughly in reading order. The public ones are the docs site, served
by every node at `/docs` (front matter, heroes and the voice in `_style.md`). The internal ones are
listed in `_internal.txt` and stay in the repository: they name private hosts and paths, or only
make sense next to the code.

## The site (public)

| Page | What's in it |
|---|---|
| [Overview](overview.md) | The shape, the path of an event, one stream on every node, the numbers |
| [Subscribe to the firehose](subscribing.md) | For consumers: frames, seqs, cursors on any node, falling behind, takedowns, the sync endpoints |
| [Design](design.md) | The job, where it's hard, what carries over from vlpds, where each check runs, decisions, failure modes |
| [Cluster](cluster.md) | The relay on the quorum log: running it, the path of an event, failures, members, the bucket |
| [Policy](policy.md) | Defaults, tiers, domain rules, admission, account caps, spam counting, takedowns |
| [Operations](operations/index.md) | [Deploy](operations/deploy.md), [Configuration](operations/configuration.md) (generated), [Monitoring](operations/monitoring.md) |
| [Admin API](admin-api.md) | The `/admin` dashboard's JSON API, the cluster view and the demo backend |
| [Compatibility](compat.md) | indigo's consumers, goat, `@atproto/sync`, Jetstream and indigo's relay against vlRelay |
| [Performance](perf.md) | One node, compression, a three-node cluster, fan-out |
| [Cost](cost.md) | Today and at 10x, 100x and 1000x on OVH, Hetzner and AWS with six object stores (`scripts/cost_model.py`) |

## Internal

| Page | What's in it |
|---|---|
| [Design session](design.html) | The original design-session doc. Open it in a browser. |
| [Build plan](PLAN.md) | The design session's decisions and who owned which module |
| [Quorum study](quorum.md) | Quorum replication: design, cost study and implementation notes ([page](quorum-study.html)) |
| [Perf log](perf-log.md) | The node and cluster benches, iteration by iteration, with profiles |
| [Policy internals](policy-internals.md) | How the policy engine is wired into the node |
| [Shadow run](shadow.md) | Two hours against ten real PDSes beside `bsky.network` |
| [Chaos](chaos.md) | Fault schedules against the relay on the quorum log, and the lease cluster's history |
| [Reference notes](reference-notes.md) | How indigo's relay and the production relay behave |
| [Dev loop](devloop.md) | Building and testing, the local network, the e2e contract, the bench box |
| [Load fleet](loadfleet.md) | `fakepds`, the synthetic upstream fleet behind the perf numbers |
