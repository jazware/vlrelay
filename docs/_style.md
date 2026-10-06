# vlRelay docs: style guide

Not published (files starting with `_` are skipped). Read this before writing or editing a page.
It's vlpds's guide (`vlpds/docs/_style.md`), with vlRelay's sections, tones and
public/internal split. The renderer is a port of vlpds's, so a page that builds there builds here.

The docs site is built from the Markdown files in this directory and served by the relay at
`/docs`, with no auth (a node without `--admin-token` still serves it). `docs/foo.md` is
`/docs/foo`, `docs/operations/bar.md` is `/docs/operations/bar`, `docs/operations/index.md` is
`/docs/operations`, and `/docs` itself is `overview.md`.

## Public and internal

The site is public. Some of this directory isn't: dev notes, bench logs and design studies that
name private hosts, paths and people, or that only make sense with the repo open.

- `docs/_internal.txt` lists the files the site leaves out. They keep plain Markdown with no
  front matter, and nothing changes about how they're read in the repo.
- A published page can't link to an internal one (the check fails with "it is internal"). Say
  what's needed on the public page, or name the file in code (`docs/chaos.md`) only when an
  operator would actually go read it. The deny list below catches most of those names anyway.
- `ui/scripts/docs-deny.txt` holds patterns no published page may contain: private hosts,
  networks, accounts, paths and the internal notes' names. Use placeholders
  (`relay.example.com`, `<your-bucket>`, "a 16-core bench box").
- `docs/design.html` and `docs/quorum-study.html` are standalone pages for the repo and aren't
  served.

When a page mixes the two (a public mechanism plus the Rust API that wires it), the public part
stays here and the rest moves to an internal file such as `cluster-internals.md`.

## How it is built

- `ui/docs-build/` is a Vite plugin. At build time it reads every published page, renders the
  Markdown (markdown-it), highlights code (highlight.js) and draws the diagrams as inline SVG. The
  browser gets finished HTML, one lazily loaded chunk per page: no Markdown parser, no diagram
  library and no external host (the page CSP is `'self'` only).
- The sidebar is generated from front matter. Nobody maintains a nav list.
- The build **fails** on a problem: missing front matter, a page that doesn't start with a hero, a
  bad diagram spec, a link to a page or heading that doesn't exist, an internal page in a link.
  Check without a full build:

  ```bash
  just docs-check                 # validates, checks the deny list, prints the nav
  just dev-ui                     # live preview on :5790, reloads when a page changes
  cd ui && npm run build          # what the server serves (ui/dist)
  ```

## Adding a page

1. Create `docs/<slug>.md` (or `docs/operations/<slug>.md`) with front matter:

   ```yaml
   ---
   title: Cluster                  # sidebar and page title
   section: vlRelay                # "vlRelay" or "Reference" for docs/*.md, "Operations" for docs/operations/*.md
   order: 5                        # position in its section (vlRelay 1-99, Operations 100+, Reference 300+); unique
   status: ready                   # stub (an outline; grey dot in the nav) | draft | ready (default)
   summary: "One sentence under the title. Quote it if it contains ': '."
   ---
   ```

2. Start the body with a ```` ```hero ```` block (below), then `##` sections. No `#` heading: the
   title comes from front matter.
3. Link to other pages by file: `[resharding](cluster.md#resharding)`,
   `[deploy](operations/deploy.md)`, `[overview](../overview.md)` from inside `operations/`.
   Anchors are the heading text, lowercased, with runs of other characters turned into `-`.
   vlpds pages are links to its source (`https://github.com/jazware/vlpds/blob/main/docs/…`).
4. Run `just docs-check`.

## Page template

Every page has the same shape, so a reader can tell what's up from the top of the page:

1. **Hero**: one diagram of the whole topic and 3-5 stat tiles (round numbers, the key defaults, the
   one thing to remember). Someone who reads only the hero should come away with the right picture.
2. **One or two short paragraphs** saying what the thing is and why an operator cares. Not a table
   of contents for the page: the sidebar and the headings already do that (see [Voice](#voice)).
3. **Sections** (`##`), each opening with its own diagram, steps or table when the idea has a shape,
   then the explanation. A section explains its diagram: name the boxes and arrows the reader just
   saw, in the same words.
4. **Links out** at the end of a section ("Details: …") rather than repeating another page.

```markdown
---
title: …
section: vlRelay
order: 6
summary: "…"
---

```hero
diagram: { … }
facts: [ … ]
```

What the thing is and why an operator cares, in two or three sentences.

## First idea

```diagram
…
```

Explanation of the diagram.
```

`overview.md` is the worked example of everything here.

## Visual blocks

All visual blocks are fenced code blocks whose body is YAML. In YAML flow maps (`{ a: 1, b: 2 }`),
**quote any string that contains a comma** (`label: "65,536 slots"`) or `: `; an unquoted comma
starts a new key, and the build reports it as an unknown key.

### hero

```yaml
diagram: { …a diagram spec, below… }
facts:
  - { value: "~60k", unit: commits/s, label: per 16-core node, note: "measured; ~330/s is Bluesky's average", tone: amber }
```

### facts (stat tiles)

A list of `{ value, unit?, label, note?, tone? }`. `value` is short and big (`~60k`, `$0`, `64`,
`10 s`); `label` says what it is; `note` gives the basis or the caveat. Tones: `accent` (default),
`amber`, `blue`, `violet`, `rust`, `muted`. Use 3-5 tiles; four fit one row on a laptop.

### steps (numbered sequence)

A list of `{ title, body? }`; `body` is Markdown. For ordered processes: a takeover, a deploy,
the life of a write.

### pages (section index)

```` ```pages ```` with an empty body (`{}`) lists the cards of every page under the current
directory; `{ dir: operations }` lists another one. Used by `operations/index.md`.

### diagram

Boxes on a grid, with groups behind them and arrows between them. One grid unit is 20 px;
a default box is 8 × 3 units (160 × 60 px). The drawing is sized to its content and scales down
with the column (on a phone, diagrams wider than 560 px scroll sideways instead).

```yaml
caption: One or two sentences under the figure. `code` works here.
nodes:
  - { id: n1, label: core 1, sub: "host + DID shards · log", at: [21, 1], size: [9, 3], tone: accent }
  - { id: log, label: "`log/`", sub: segments, at: [35, 1], size: [10, 2.6], shape: store, tone: amber }
groups:
  - { label: vlRelay cluster, around: [n1], tone: accent }       # or at: [x, y], size: [w, h]
edges:
  - "n1 -> log: append, then ack"                              # string form
  - "n1 <-> n2"                                                # both ends
  - "n1 ~> log"                                                # dashed
  - "n1 -- log"                                                # no arrow
  - { from: n1.b25, to: appview.t, label: app.bsky.*, dash: true, tone: blue }
notes:
  - { at: [21, 8.6], text: split / merge online, align: start }
```

- **Nodes**: `id`, `label` (`\n` for a second line; backticks for code), `sub` (smaller, muted),
  `at: [x, y]` in grid units (fractions are fine), `size: [w, h]` (default `[8, 3]`),
  `tone`, `shape` (`box` default, `store` = cylinder for anything in the bucket, `pill`, `note`
  = dashed), `stack: true` (drawn as several), `badge: ×3`.
- **Edges** connect node ids. Without sides, aligned boxes get a straight line through their
  overlap; otherwise a diagonal between their edges. Pin the ends to sides to get clean right-angle
  routes: `id.t`, `id.b`, `id.l`, `id.r`, optionally a percentage along the side (`n3.b25` is a
  quarter of the way along the bottom). `via: [[x, y], …]` forces waypoints. `arrow`: `end`
  (default), `start`, `both`, `none`. Labels sit on the longest segment of the route; `labelAt: [x, y]`
  moves one.
- **Groups** are drawn behind the nodes with a small caps label: a cluster, a process, the bucket.
  `around: [ids]` wraps those nodes (`pad`, default 1 unit).
- **Tones** (theme-aware, the same in light and dark):

  | tone | use it for |
  |---|---|
  | `ink` (default) | clients, generic components |
  | `accent` | vlRelay itself: core nodes, the pipeline, the log |
  | `amber` | the object store and anything durable in it |
  | `blue` | the firehose, edges, replicas and consumers |
  | `violet`, `cyan`, `rust` | a third or fourth kind of thing, when needed |
  | `muted` | PDSes, PLC and other outside services, optional parts |
  | `solid` | the one outcome a diagram is about (an ack, "serving") |
  | `danger`, `ok` | failure and recovery states |

  Keep to these meanings across pages so color means the same thing everywhere.

Diagram rules:

- **Accurate to the code.** Every box is a real component, key prefix or process, named as the code or
  the bucket names it. If you simplify (three nodes stand for N), say so in the caption.
- **One idea per diagram.** If a diagram needs a legend, split it.
- **Arrows are data or control flow, in the direction it moves**, labeled with the verb or the
  payload (`append, then ack`, `lease CAS`). Dashed = background or optional.
- **Don't let lines cross boxes.** Pin sides (`n3.b15 -> appview.t`) and leave a grid unit or two
  between rows for routes. Check the result in both themes (the theme toggle is in the top bar).
- **Short labels.** A box label is a noun of one to three words; details go in `sub` or the text.
  A box is 160 px wide by default: a `sub` longer than ~24 characters needs a wider box.

### timeline (swimlanes)

Lanes are rows and time runs left to right, for anything where order and waiting matter: the
life of a write, pipelining, a takeover. Time is in abstract units (`scale` px each, default 56),
so a 44 µs step and a 300 ms PUT fit one figure. Put real durations in `dur` and `ticks`, and say
in the caption when the spacing isn't to scale. A hero can take `timeline:` in place of `diagram:`.
Timelines wider than 620 px scroll sideways on a phone instead of shrinking their text.

```yaml
caption: Required. What the reader should see in it.
scale: 44
lanes:
  - { id: log, label: Node log, sub: sequencer · finalizer, tone: accent }
  - { id: store, label: Object store, sub: "`log/` segments", tone: amber }
spans:                                      # bars; one lane does one thing at a time
  - { lane: log, from: 4, to: 6, label: batch, dur: "0–1 PUT wait" }
  - { lane: store, from: 6, to: 10, label: segment PUT, dur: "~30 ms", tone: amber, dash: true }
events:                                     # a diamond, label underneath
  - { lane: log, at: 11, label: lease check, tone: muted }
arrows:                                     # between lanes, at a time or [sent, arrived]
  - { from: log, to: store, at: 6.1, label: If-None-Match, side: left }
marks:                                      # a labelled rule across every lane
  - { at: 10, label: durable, tone: amber }  # tone defaults to solid
ticks:                                      # the axis under the lanes
  - { at: 10, label: "~30–50 ms" }
```

The build places every label by fixed rules and fails, naming both parties, when something
doesn't fit: an unknown lane or key, a missing caption, `to` not after `from`, two spans
overlapping in one lane, a span label wider than its bar, two labels overlapping, an arrow running
through a span in a lane it passes, or a mark's rule cutting through text. Fix it by moving a
time, shortening a label, flipping an arrow label to `side: left`, or raising `scale`. Spans may
touch end to end. Tones mean the same as in diagrams.

## Tone and content

- **Operator- and consumer-focused.** Write for someone running a relay or reading its firehose:
  what it does, what it costs, what to watch, what to do. Bench logs, design history and rejected
  alternatives stay in the internal notes (`perf-log.md`, `docs/design.html`).
- **Concise.** Short sentences, plain words, active voice. Lead with the point. [Voice](#voice)
  below spells out what that means sentence by sentence.
- **Round numbers** with their basis: "~60k commits/s per 16-core node (measured)", "~$1.7k/mo on S3".
  Say whether a number is measured, modeled or a design target. Link the benchmark directory
  (`bench/results/…`) in a note or sentence rather than copying tables.
- **Defaults with their flag**: "a 10 s lease (`--lease-ttl-ms`)". Name metrics exactly
  (`vlrelay_time_to_firehose_seconds`).
- **Code paths sparingly**, as inline code (`src/nodelog.rs`), when an operator would actually go
  read it. Not as links.
- **Say what isn't built.** If something is design only (backups, planet scale), say so plainly.
- Callouts: `> [!NOTE]`, `> [!TIP]`, `> [!WARNING]`, `> [!DANGER]` as the first line of a
  blockquote. Use them rarely. One warning on a page reads as a warning, but five read as noise.

## Voice

The docs should read like Jaz explaining the system to another engineer: the voice of the
[example.com](https://example.com) posts and the [Practical Observability](https://book.someone.me)
book, tightened for reference docs. A reader said the earlier drafts were "a little flowery" and
took a while to get into. Most of that came from a handful of habits that a language model reaches
for and Jaz doesn't. This section lists them so they can be checked for.

### What the voice is

- **Plain declarative sentences, one idea each.** Most sentences run 12-25 words. The blog posts
  average about 25 words a sentence and use no semicolons and no em dashes at all in ~8,000
  words. These docs had over 400 semicolons before the voice pass.
- **Conversational connectives.** Sentences link up with "So", "Since", "Now", "That means",
  "Additionally", "In this case", "That being said". They don't link up with punctuation.
- **Contractions everywhere.** it's, don't, can't, isn't, there's, that's, we'd, won't.
- **Person.** The operator is "you" ("If you run one node…"). The system is the subject of
  mechanism sentences ("vlRelay checks…", "A node renews…", "The DID owner replays…"). "We" is fine
  when walking the reader through a mechanism or a calculation ("If we PUT one object per commit,
  we'd pay…"). No "I" in the docs.
- **Concrete before abstract.** Name the problem or the scenario, with numbers, then the
  mechanism. "A node PUTs one segment about 27 times a second, whether it holds 300 commits or
  20,000" lands faster than "request cost is decoupled from write rate". When a term needs a
  definition, give it in one plain sentence ("A shard is a range of slots with one owner") and
  then an example.
- **Numbers do the arguing.** Round numbers with `~` and units, before → after, and the arithmetic
  when it explains a cost ("~$43k a day at 100k commits/s"). Say where a number came from in a few
  words: measured, modeled, design target.
- **Trade-offs said plainly.** State the cost, give the number, say why it's acceptable: "This
  doubles the storage at most, which is fine for a 72 h window." No hand-wringing and no stacked
  hedges. One "about" or "~" per number is enough.
- **Paragraphs of 1-4 sentences.** Short is good. What's banned is a short paragraph whose only
  job is to land a line for effect (see the list below).
- **Parentheses for asides**, not dashes or semicolons: "(the default)", "(`--lease-ttl-ms`)",
  "(i.e. the node's own log)". Keep them short and useful: a flag, a default, a basis, an example.
- **A little humour is fine, and rare.** The blog has the odd "Not quite…" or "problem solved,
  right?". In the docs, at most one light aside on a page, and never in the operations pages or
  the runbook. Don't invent jokes to hit a quota.
- **Words Jaz uses:** keep up with, get by on, a bit, pretty, just (sparingly), cheap, plenty,
  hold up, spread out, so, since, that's, around, something like.

### Banned patterns

Search for each of these on every page you touch.

| Pattern | Looks like | Do instead |
|---|---|---|
| Semicolon splice | "Slots are permanent; shards are not." | Two sentences, or join with "and", "but" or "so". |
| Em dash or `--` aside | "the log — and only the log — is durable" | Parentheses, a comma, or a second sentence. |
| "X, not Y" / "not X, but Y" flourish | "Logins and proxying use the CPU, not commits." | Say X. Mention Y only if readers would assume it, in its own sentence: "Most CPU goes to logins and proxying. A commit is cheap (~100 µs)." |
| Colon chains | "The rule is simple: one owner per shard: always." / "A: B, C: D" | One colon per sentence at most, and only to introduce a list, a code span or an example. |
| Rule-of-three lists | "fast, cheap and simple"; "no ZooKeeper, no Raft, no quorum" | Keep the items that carry information. Two is fine. One is often better. |
| Abstract nouns for actions | "the persistence of", "guarantees around", "the handling of", "ensures the durability of" | Use the verb: "stores", "makes sure … is durable", "handles". |
| Filler and AI-isms | load-bearing, crucially, critically, importantly, in practice, it's worth noting, notably, essentially, effectively (as filler), robust, seamless(ly), under the hood, the heart of, first-class, by design, key (as in "the key insight"), "this is where X comes in", "the trick is", "the catch is" | Delete the word. If the sentence falls apart, rewrite it. |
| Bold lead-in on every bullet | "- **Fail-stop over guessing.** When a node…" | Plain bullets. Bold only a term at its definition, or a real warning. Reference lists of named things (components, flags, alerts) can start with the name in `code` or plain text and a period. |
| Bold for emphasis in running text | "**the object store is the database**" | No bold. If a phrase matters, put it first in the sentence. |
| One-line paragraph for drama | "Unavailability is recoverable; a forked repo is not." on its own | Fold it into the paragraph it concludes, as a normal sentence. |
| Aphorism endings | "Less to write and less that can disagree." | State the consequence plainly or cut it. |
| Over-precise parentheticals | "(at the default `--lease-ttl-ms 10000`, 12 s at the tiny profile's 60 s, see below)" | One fact per parenthesis. Move the rest to the section or table that owns it. |
| Narrating the page | "This page covers how X is built, what holds it back, and how Y is served." / "The short version:" / "In this section we'll look at" | Delete. Start with the content. A one-line pointer to another page for a related topic is fine. |
| Stacked qualifiers | "typically only briefly and usually safely", "about ~", "roughly up to" | One qualifier, or none if the number already has `~`. |
| Sentence fragments as style | "Nothing else." / "No second copy." / "By design." | Full sentences in the pages. (The runbook can use fragments; see below.) |
| Italic stress | "lease timing only decides *when*" | Rewrite so the stressed word is at the end of the sentence. |
| Restating the obvious | "This matters because…" before something that obviously matters | Cut the lead-in, keep the reason. |
| Exclamation marks | "and it's free!" | None in the docs. |

### Mechanical pass

Apply these in order to every paragraph, list item, table cell, `steps` body and `facts` note you
edit. Don't touch `hero` or `diagram` blocks (see below).

1. Replace every semicolon in prose with a period or with "and", "but" or "so". Table cells and
   facts notes may use `·` to separate short items instead.
2. Replace every em dash or double hyphen used as punctuation.
3. Count colons. More than one in a sentence: split it. A colon that isn't introducing a list,
   code or an example: make it a period or "because"/"so".
4. Find ", not " and "rather than". Keep one only when it corrects a real misconception; otherwise
   drop the contrast half.
5. Delete every word in the filler list. Re-read the sentence.
6. Remove bold except (a) a term at its definition, once per page, and (b) callouts. Remove bold
   lead-ins from bullets that are prose. Remove italics used for stress.
7. Use contractions: it is → it's, does not → doesn't, cannot → can't, there is → there's.
8. Split any sentence over ~35 words, or with more than two clauses joined by commas.
9. Make the actor the subject: "is acknowledged by the node" → "the node acknowledges".
10. Delete sentences that describe the page or section instead of the system.
11. Turn lists of three into lists of what's needed.
12. Read the paragraph aloud. If Jaz wouldn't say it to a coworker, rewrite it.

Length usually comes out about the same. Splitting crammed sentences and adding connectives costs a
few words, and cutting flourish wins them back. The goal is faster reading, not a lower word count.
If a page grows by more than ~10%, something probably got explained twice.

### Before and after

From the vlpds docs, rewritten in the voice:

> Before: The log is the write-ahead log *and* the firehose: there is no second copy.
>
> After: The log is both the write-ahead log and the firehose, so every write is stored once.

> Before: Fail-stop over guessing. When a node can't be sure it's still allowed to write, it exits
> and lets its supervisor restart it. Unavailability is recoverable; a forked repo is not.
>
> After: If a node isn't sure it's still allowed to write, it exits and its supervisor restarts it.
> Being unavailable for a while is recoverable, but a forked repo isn't.

> Before: Logins and proxying use the CPU, not commits. A commit costs ~100 µs of CPU end to end,
> while an Argon2 login costs ~20 ms.
>
> After: Most of the CPU goes to logins and proxying. A commit costs ~100 µs of CPU end to end,
> and an Argon2 login costs ~20 ms.

> Before: There is no single sequencer: each node numbers the events in its own log, and every node
> merges all the logs into one stream with a fixed order. … This page covers how that order is
> built, what holds it back, how subscribers are served and limited, and how old cursors are served
> from the bucket.
>
> After: There's no single sequencer. Each node numbers the events in its own log, and every node
> merges all of the logs into one stream in the same order.

> Before: The short version: records are the truth. The tree's interior nodes are stored so that a
> cold repo opens in one or two round trips. Leaves are rebuilt from records when needed.
>
> After: Records are the source of truth. vlpds also stores the tree's interior nodes, so a cold
> repo opens in one or two round trips, and rebuilds the leaves from records when it needs them.

> Before: vlpds therefore never stores a secret it can recover in the clear: keys it must use are
> wrapped under a **key-encryption key (KEK)** that is not in the bucket, and secrets it only checks
> are stored as hashes.
>
> After: So vlpds never stores a usable secret in the clear. Keys it needs to use are wrapped under
> a key-encryption key (KEK) that isn't in the bucket, and secrets it only checks are stored as
> hashes.

> Before (runbook): If `VlpdsShardsUnowned` isn't firing, data-wise nothing is urgent; the cluster
> is running with less capacity.
>
> After: If `VlpdsShardsUnowned` isn't firing, nothing is urgent. The cluster is just running with
> less capacity.

### The Operations pages

The Operations pages are read by someone running a relay, sometimes in a hurry. Terser than the
other pages, and imperative where they say what to do.

- Every command in backticks, runnable as written. Every threshold with its number and flag.
- Explain a mechanism only as far as it changes what to do, and link the page that owns it.
- No humour.

vlRelay has no alert rules or runbook yet. When it does, they follow vlpds's shape: one runbook
section per alert with **Means / Causes / Confirm / Do**.

### What must not change

A voice pass changes wording only. Check each of these before committing:

- **Facts.** Every number, unit, default, flag, config key, metric name, alert name, exit code,
  error name, key prefix and file path stays exactly as it is. If a sentence goes, the fact in it
  moves to another sentence or was already stated on the page.
- **Nothing new.** Don't add a claim, number, feature or promise that isn't already in the page or
  the code. The docs only describe what exists. If something reads wrong, flag it instead of fixing
  it in prose.
- **Headings.** `check-docs` checks every link and anchor. Leave headings as they are. If one must
  change, update every link to it in `docs/`, `README.md` and `ops/` in the same commit.
- **Links.** Every link stays and points to the same target.
- **Front matter.** `title`, `section`, `order`, `status` stay. `summary` may be reworded for voice
  (keep it quoted if it contains `: `).
- **Visual blocks.** Don't edit `hero` or `diagram` blocks (captions included). `steps` bodies and
  `facts` notes are prose and may be reworded, keeping the YAML valid. Every page still starts
  with its hero.
- **Code blocks** and inline code are untouched.
- Callouts stay callouts, with the same level.

Run `just docs-check` after every page.

## Avoiding duplication

- The internal notes (`perf-log.md`, `cluster-internals.md`, `policy-internals.md`, `chaos.md`,
  `shadow.md`, `reference-notes.md`, `PLAN.md`) remain the deep log: every iteration, measurement
  and rejected alternative. The docs are the curated, current view: what's true now and what an
  operator or consumer needs.
- `operations/configuration.md` is generated by `just config-doc` (`build/config_doc.py`). Edit
  the script's header and section notes, not the page.
- Each fact has one home page. Other pages state it in a clause and link there.
- When the code changes a default or a mechanism, update the page that owns it in the same change.
