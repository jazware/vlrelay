# Operator console

The console at `/admin` and the public page at `/` share one look, "exchange": a switchboard at night. Bone and ink-indigo panels, cobalt for action, a magenta carrier lamp for the live stream (seq, commit, emit), Archivo widened for labels and IBM Plex Mono for numbers. The shell is the vlpds console's (`vlpds/ui/CONSOLE.md`): a top bar with the carrier rule, a patch-panel rail of sections, a dense main column and one slide-over for any row. The kit was copied from there and re-skinned, so the two consoles drive the same way.

## Where things live

| Path | What |
| --- | --- |
| `src/console.css` | Tokens and every console class. All classes start with `cx-` (or sit under one) because `styles.css` is global and owns `.btn`, `.tile`, `.seg`, `.empty`. The tokens live on `.cx`, so the public page uses them too (`.cx.cx-pubroot`). |
| `src/components/console/` | The kit: `kit.tsx` (small parts, plus `HostName`, `TierTag`, `HostStatusChip`, `Bars`, `Jack`), `DataTable.tsx` (client or server sort), `Drawer.tsx`, `dialogs.tsx`, `toast.tsx`, `LiveTail.tsx`, `Exchange.tsx` (the overview canvas), `LogRail.tsx` (the quorum log rail), `Palette.tsx`, `Shell.tsx`, `sections.tsx` (the IA), `nav.ts`, `hostActions.tsx` (every host action behind a confirm). |
| `src/lib/console/` | Data: `live.ts` (pause, stale, `createPoller`, `useLivePoll`), `polls.ts` (the shared polls, and client-side series for values the API has no history for), `relay.ts` (nodes, colours, the quorum's health), `firehose.ts` (the subscribeRepos tail), `adminAdapter.ts` (every endpoint), `fmt.ts`. |
| `src/pages/admin/` | `AdminApp.tsx` routes. `Overview.tsx`, `Hosts.tsx`, `Consumers.tsx`, `Quorum.tsx` and `Store.tsx` are built on the kit. `hostDetail.tsx` registers the `host` detail kind, `quorumDetail.tsx` `node` and `epoch`, `Consumers.tsx` `consumer`. `relayUi.tsx` holds the banners they share, `quorumUi.tsx` the member rows, epoch changes and the membership dialog, `logPages.css` the classes the log pages add. The other sections render the classic pages from `src/pages/` inside `<Legacy>`. |
| `src/pages/Public.tsx` | The public page. It reads only `/api/public/stats`. |

## Building a section

1. Replace the section's `case` in `route()` (`AdminApp.tsx`) with your page. Keep the old paths in `aliases` (`sections.tsx`) so links keep landing.
2. Start with `<PageHead>`, then `<Banners>` if anything needs attention, then `<HealthLine>` or `<Tiles>`, then panels in `cx-grid2` / `cx-stack`.
3. A row with more to show opens in the slide-over: register a detail kind with `registerDetail` and give the table `open={(row) => ({ type, id })}`. `o` and "Full page ↗" go to `/admin/<section>/<type>/<id>`.
4. Anything that changes the relay goes through `confirmAction` with the exact request in `call` (a function of the fields when they change it). No mock actions: call the endpoint through the adapter and let the dialog show the error.
5. Add entities and verbs to ⌘K with `registerPalette`.
6. Check it at 390 px and in both themes, against `admin_demo` and a real relay, with the browser console clean.

The kit's parts are documented in the vlpds CONSOLE.md. What differs here:

- `DataTable serverSort={{ id, asc, sortable, onSort }}` for tables the server sorts and pages (Hosts). Duplicate row keys get a suffix instead of breaking React.
- `useLivePoll(fetch, key, ms, { keep })` is a poll owned by one component (a host's detail, one page of hosts). It pauses with space. `keep` keeps the last rows while a new filter loads.
- `seriesOf(key)` (`polls.ts`) is a series the console builds from its own polls: the stream's rate, the commit latency, each busy host's rate, each consumer's rate, bucket requests per second. The busiest hosts' rates come from `overview.topHosts[].history`. It fills in while the page is open.
- `storePoll` keeps `GET store`'s rates as `store-a` and `store-b`. The quorum poll keeps what the statuses only have as counters: `requestRates()` (bucket requests per second over the last minute, by purpose, component and op, summed over the members that answer), `seenFlushes()` (F moving between two polls of the leader), `seenEpochs()` (the epoch moving) and `flushSeenAt()`. They start empty on each page load.
- `NeedsVersion what endpoint` stands in for a panel whose endpoint isn't there yet (below).

## The adapter and what the backend lane owes

`lib/console/adminAdapter.ts` is the one place the console calls the admin API. The endpoints the design assumes but the relay doesn't serve answer `{ supported: false, endpoint }` without a request, so the browser console stays clean and the page shows a "needs a newer vlRelay" placeholder. To light one up, implement it and replace its `missing(...)` with the call. `MISSING` in the same file is this list.

| Endpoint | What it feeds |
| --- | --- |
| `qlog status.flush.recent` | Quorum › Flush: the leader's recent flushes (F, entries, bytes, how long). Until then the table lists the flushes the page saw |
| `GET cluster/quorum/history` | Quorum › Leadership: takeovers and handoffs with their times and pauses. The statuses list membership changes (`switches`) and recoveries (`recovered`, no time) only, so a takeover shows as "new epoch" if the page was open for it |
| `consumers[].readTier` | Consumers: where a replaying consumer reads from (ring, disk, bucket). Until then it says live or replaying |
| `GET consumers` on every member | Consumers › Connections: every member's sockets. The list is the answering node's until it's asked over the qlog peer protocol, so the page labels it "this node" |

Found while building this, for the backend lane:

- On the quorum log `GET consumers` lists the sockets of the node the console talks to. The Consumers page says so ("this node"); its Serving nodes table counts every member's from `cluster`.
- On a local quorum cluster (macOS) every consumer reports `eventsPerSec` and `bytesPerSec` 0, and `cluster` reports `cpu` and `memBytes` 0 (they read `/proc`). The node drawer shows a dash for 0 memory.
- `admin_demo`'s consumer kick answers "no node relay-b" for a consumer on another demo node (it has no fleet).
- `GET store`'s rates are since the node's previous sample, so the first answer after a start has a window of 0 and the page waits for the next.
- `admin_demo` lists a few hostnames twice (`pds.kettle.social`, `bsky.pixel.net`, `pds.moss.social`).
- `admin_demo` serves no `subscribeRepos`, so the tail says the firehose isn't reachable there. `npm run dev` proxies `/xrpc` (websockets too), so the tail works against a real relay.
- The console's own tail is a consumer: it shows in the consumer list with the browser's user agent.

## Sections

| Section | Path | State |
| --- | --- | --- |
| Overview | `/admin` | Built: banners (quorum held or degraded, a node not answering, throttled hosts falling behind, hosts at their account cap, a slow consumer), the health line, eight tiles, the exchange, rejects by reason, busiest hosts, the sampled tail (with the frames that never reached the stream behind "rejects", and a hostname followed at full rate from `ops/tail`), and a rail with the stream, members, consumers and open cases |
| Hosts | `/admin/hosts` | Built: status tiles, the paged table (server-side filter, sort and page; the at-cap, lagging and erroring flags filter here over every match), the `host` drawer and full page with every host action, crawl admission (placeholder) and tiers. `/admin/hosts/<host>` still lands on the host |
| Consumers | `/admin/consumers` | Built: slow-consumer and "this node" banners, tiles, serving nodes (consumers, sent/s, entries behind the commit), the connection table (rate against the stream, mode, `?node=` and `?q=` filters), the `consumer` drawer with the kick behind a typed confirm (`#id`), consumer limits, read tiers (placeholder). ⌘K finds consumers by id, client or IP |
| Quorum & cluster | `/admin/quorum` | Built: banners, eight tiles, the log rail, leadership (ribbon and table from `switches`, `recovered` and the epochs the page saw; `epoch` drawer), members (`node` drawer), flush (F's age from `flush.last_at_ms`), host owners (one cell per host from `hosts?sort=host`, each opening the host), counters, the ack backlog with the hosts that have work in flight, and the membership dialog (typed `change members`; off without `--qlog-admin-token`). `/admin/cluster` and `/admin/ops` land here (the classic Operations page went with seq checkpoints and archival) |
| Object store | `/admin/store` | Built from `GET store` (the answering node): requests by purpose in R2's classes with rates and bytes, latency by op, the leader's last retention pass (segments, what's past the horizon, the pruned seq, state paths). The members' statuses add requests by key component and per member. Counts and rates only, no prices |
| Policy | `/admin/policy` | Classic pages: Limits, Tuning (`/admin/tuning`), Domain rules (`/admin/rules`) |
| Moderation | `/admin/moderation` | Classic pages: Cases (`/admin/cases`, `/admin/cases/<id>`), Accounts (`/admin/accounts`, `/admin/accounts/<did>`) |
| Settings | `/admin/settings` | Classic page |

Phase 2 rebuilds the rest on the kit: Policy (one draft with a diff and a versioned save), Moderation (cases, accounts and takedowns as detail kinds) and Settings. Edges and replicas are gone for now, so their panels stay out until they come back.

## Keyboard

`⌘K` palette: sections, hosts by name, a DID or handle, nodes, and verbs on a host (`ban …`, `suspend …`, `unban …`, `throttle …`, `reconnect …`, `raise cap …`). `g` then a letter jumps to a section (`g u` is the public page). `j` / `k` move through rows, `Enter` opens, `o` goes full page, `Esc` closes. `/` focuses the page's search, else the palette. `space` pauses live updates, `t` toggles the theme, `?` lists all of it.

## Trying it

```bash
cargo run --bin admin_demo                    # :2790, token "demo"
cd ui && npm run dev                          # :5790, proxies /admin/api, /api/public and /xrpc
```

A real relay with a tail and real actions: `fakepds run --hosts 5 --dids 200 --rate 150 --plc-port 29999 --lag-secs 60 --fault badsig:4:rate=0.4`, then `vlrelay --listen 127.0.0.1:2980 --memory --plc-url http://127.0.0.1:29999 --admin-token <token>` with a `--host http://127.0.0.1:3000N` per fakepds host, and `VLRELAY_URL=http://127.0.0.1:2980 npm run dev`.

## Look

- Status is a colour and a glyph, always both: `ok ●`, `warn ▲`, `err ■`, `info ◆`, `idle ○`.
- Cobalt (`--accent`) is for action, magenta (`--signal`) only for the live stream: the seq, the commit, the firehose line, the lit jack.
- The fonts are self-hosted in `public/fonts` (the page CSP allows no other origin).
- Use example.com-style names in fixtures and placeholders, and show what the server says (its hostname is the page's own). The docs and UI are public.
