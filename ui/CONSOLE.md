# Operator console

The console at `/admin` and the public page at `/` share one look, "exchange": a switchboard at night. Bone and ink-indigo panels, cobalt for action, a magenta carrier lamp for the live stream (seq, commit, emit), Archivo widened for labels and IBM Plex Mono for numbers. The shell is the vlpds console's (`vlpds/ui/CONSOLE.md`): a top bar with the carrier rule, a patch-panel rail of sections, a dense main column and one slide-over for any row. The kit was copied from there and re-skinned, so the two consoles drive the same way.

## Where things live

| Path | What |
| --- | --- |
| `src/console.css` | Tokens and every console class. All classes start with `cx-` (or sit under one) because `styles.css` is global and owns `.btn`, `.tile`, `.seg`, `.empty`. The tokens live on `.cx`, so the public page uses them too (`.cx.cx-pubroot`). |
| `src/components/console/` | The kit: `kit.tsx` (small parts, plus `HostName`, `TierTag`, `HostStatusChip`, `Bars`, `Jack`), `DataTable.tsx` (client or server sort), `Drawer.tsx`, `dialogs.tsx`, `toast.tsx`, `LiveTail.tsx`, `Exchange.tsx` (the overview canvas), `LogRail.tsx` (the quorum log rail), `Palette.tsx`, `Shell.tsx`, `sections.tsx` (the IA), `nav.ts`, `hostActions.tsx` (every host action behind a confirm). |
| `src/console-rules.css` | Policy, Moderation and Settings: the draft (dirty fields, knobs, the draft bar, diffs), case evidence and notes. |
| `src/lib/console/` | Data: `policyDraft.ts` (the one policy draft: diff, rebase onto a newer version, undo from an audit entry), `live.ts` (pause, stale, `createPoller`, `useLivePoll`), `polls.ts` (the shared polls, and client-side series for values the API has no history for), `relay.ts` (nodes, colours, the quorum's health), `firehose.ts` (the subscribeRepos tail), `adminAdapter.ts` (every endpoint), `fmt.ts`. |
| `src/pages/admin/` | `AdminApp.tsx` routes; every section is on the kit. `Overview.tsx`, `Hosts.tsx` (with `Admissions.tsx`, its admission panel, and `hostSource.tsx`, a host's source and its filter) and `hostDetail.tsx` (the `host` kind); `Discovery.tsx` (the `dsource` kind; its policy editor is `DiscoveryPolicy` in `Policy.tsx`); `Consumers.tsx` (the `consumer` kind); `Quorum.tsx` with `quorumDetail.tsx` (the `node` and `epoch` kinds) and `quorumUi.tsx` (member rows, epoch changes, the membership dialog); `Store.tsx`; `Policy.tsx` (the `ver` kind); `Moderation.tsx` with `moderationDetail.tsx` (the `case`, `acct` and `rule` kinds and every moderation confirm); `Settings.tsx` (the `flag` kind). `relayUi.tsx` holds the banners the traffic pages share, `logPages.css` the classes the log pages add. |
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
- `storePoll` keeps `GET store`'s rates as `store-a` and `store-b`. The quorum poll keeps what the statuses only have as counters: `requestRates()` (bucket requests per second over the last minute, by purpose, component and op, summed over the members that answer), `seenEpochs()` (the epoch moving, for a change no member's history lists) and `flushSeenAt()`. They start empty on each page load.
- `historyPoll` is `GET cluster/quorum/history`; `epochEvents` (`quorumUi.tsx`) joins it with the statuses' `switches` and `recovered`: each lead becomes a takeover, handoff, membership change or recovery, and a step-down belongs to the next epoch's lead (or stands alone while nobody leads). `discoveryPoll` is `GET discovery`.
- The Discovery page edits the policy's `discovery` section through the one policy draft (`DiscoveryPolicy` and `DraftBar` come from `Policy.tsx`), so a seed relay added there is reviewed and saved as any policy change. The review lists the seed relays one line per relay.
- `NeedsVersion what endpoint` stands in for a panel whose endpoint isn't there yet (below).
- `Over r detail` (`kit.tsx`) is a value against its limit: the ratio as the headline ("6.7×"), the value and limit muted beside it, and a bar on one log scale from 0.1× to 10× with a tick at 1×, so a bigger overage always draws longer. Cases, the case drawer's trips and the spam signals use it.
- `seqSeenAt()` (`polls.ts`) is when the console last saw the newest seq move, to within a poll: Consumers' stream freshness.
- `recent.ts` keeps the last six details opened in the tab (`sessionStorage`, beside the token), written by the slide-over and the full page; `attention()` (`relayUi.tsx`) turns the banners' notices into palette items. Together they are ⌘K's empty-query inbox.
- `currentLead`, `recentLeaderChange` and `leaderChangeText` (`quorumUi.tsx`) say who leads since when and word a change for the Overview banner (30 minutes), the Quorum leader tile and ⌘K.

## The adapter and what the backend lane owes

`lib/console/adminAdapter.ts` is the one place the console calls the admin API. An endpoint the design assumes but the relay doesn't serve answers `{ supported: false, endpoint }` through `missing(...)` without a request, so the browser console stays clean and the page shows a "needs a newer vlRelay" placeholder; `MISSING` in the same file lists them. `ops/rejects/top` is served now; a relay older than it answers the first request with a 404 (one line in the network log per page load) and the console doesn't ask again until a reload.

| Endpoint | Feeds | State |
| --- | --- | --- |
| `GET ops/rejects/top?reason=<reason>&limit=10` → `[{host, rejectsPerSec, total, lastAtMs, sample?}]`, cluster-wide | Hosts with `?reason=` (the Overview's reject bars): the hosts sending that reject | Served. `rejectsTop` in the adapter; "needs a newer vlRelay" on an older relay |

Found while building this, for the backend lane:

- The Hosts flags (at cap, lagging, erroring, throttled accounts or at cap), its source filter and Moderation's accounts-created-throttled panel ask for every host (`limit` 10,000) and filter in the page. `GET hosts` takes `source` and `throttled` now, so the source filter could move to the server; the flags have no server filter.
- The Overview's busiest-by-rejects view reads each listed host's detail (`hosts/{host}`, eight calls every 10 s while it's on) for its top reason; a `topReason` on `HostRow` would save them.
- `policy/usage` is the answering node's: `newAccountsPerMin` counts on the leader only, so from a follower the Policy page says it's counted on the leader instead of showing 0.
- `admin_demo`: its statuses carry no `flush.recent` or `history` (its `cluster/quorum/history` does), `takedowns` doesn't list a takedown made through the demo, and its discovery sources don't follow the policy's `discovery` section.
- `admin_demo` serves no `subscribeRepos`, so the tail says the firehose isn't reachable there. `npm run dev` proxies `/xrpc` (websockets too), so the tail works against a real relay.
- The console's own tail is a consumer: it shows in the consumer list with the browser's user agent.

## Sections

| Section | Path | State |
| --- | --- | --- |
| Overview | `/admin` | Built: banners (quorum held or degraded, a leader change in the last 30 minutes with who took over from whom, why and the pause, and Open epoch, a node not answering, throttled hosts falling behind, hosts at their account cap, a slow consumer), the health line (each cell drills somewhere else: Firehose to Consumers, Time to firehose to Quorum, Rejects to hosts by rejects; the Quorum cell says since when the leader leads), eight tiles, the exchange, rejects by reason (each bar opens the hosts sending it), busiest hosts by events or by rejects (`hosts?sort=errors`, with each one's top reason), the sampled tail (with the frames that never reached the stream behind "rejects", and a hostname followed at full rate from `ops/tail`), and a rail with the stream, members, consumers and open cases |
| Hosts | `/admin/hosts` | Built: status tiles, the paged table (server-side filter, sort and page; the at-cap, lagging, erroring and throttled-accounts flags and the source filter here over every match), throttled accounts and source columns, `?reason=` (the hosts sending one reject, from `ops/rejects/top`), a search that matches nothing showing that name's admissions and Request crawl, and the `host` drawer and full page: "why it's held" first for a throttled, backing-off, suspended or banned host (the binding limit, the domain rule or case auto-throttle or operator behind it, and the way out), then every host action and where the host came from. Admissions live on Discovery and the tier matrix on Policy. `/admin/hosts/<host>` still lands on the host |
| Discovery | `/admin/discovery` | Built: tiles (sources, hosts seen, admitted, the connects budget against the running sources' pace), the sources table from `discovery` (status, last and next run, seen, new, admitted, refused, errors and 429s, pages) with Run now per source and for all behind a confirm with the call, the `dsource` drawer (this run's pace against both budgets, cursor, last error), admissions with their source and a source filter (`hosts/admissions`, with today's new-host budget), and the seed relays, PLC switch and budgets through the policy draft. ⌘K: `run discovery`, `add seed relay <url>` |
| Consumers | `/admin/consumers` | Built: every member's sockets (a banner names a member that didn't answer), slow-consumer banner, tiles led by stream freshness (newest seq age, time to firehose p99, the serving node furthest behind the commit), serving nodes (consumers, sent/s, entries behind the commit), the connection table (rate against the stream, read tier, `?node=`, `?tier=` and `?q=` filters), the `consumer` drawer (a one-line verdict to paste back: replaying from disk or the bucket, its node behind the commit, falling behind, or live under the cutoff) with the kick behind a typed confirm (`#id`; one on another member is sent on with `--qlog-admin-token`, and the dialog says so), consumer limits, read tiers by count. ⌘K finds consumers by id, client or IP |
| Quorum & cluster | `/admin/quorum` | Built: banners, eight tiles (the leader with since when and the change that put it there), the log rail, leadership (ribbon and table from `cluster/quorum/history` with `switches` and `recovered`: takeovers, handoffs, membership changes, recoveries and step-downs with their times and pauses; `epoch` drawer), members with each one's durability (`node` drawer, with its own leadership history and CPU and memory), flush (F's age, the leader's last 32 flushes from `flush.recent`, Flush now behind a typed confirm), host owners (one stacked bar from `cluster`'s owned counts, a find-a-host's-owner box, and the one-cell-per-host map behind Show the map), counters, the ack backlog with the hosts that have work in flight, and the membership dialog (typed `change members`; off without `--qlog-admin-token`). `/admin/cluster` and `/admin/ops` land here (the classic Operations page went with seq checkpoints and archival) |
| Object store | `/admin/store` | Built from `GET store` (the answering node): requests by purpose in R2's classes with rates and bytes, latency by op, the leader's last retention pass (segments, what's past the horizon, the pruned seq, state paths). Each class and purpose also says what its rate comes to in 30 days. The members' statuses add requests by key component and per member. Counts and rates only, no prices |
| Policy | `/admin/policy` | Built: the tier matrix and every knob of the full document (`policy/full`, or the older tier form on `policy`), each with its default and its use now where the API has it (budgets from `policy/usage`, each spam signal's heaviest key from `policy/signals`), host discovery (as on Discovery); one draft with a sticky bar, Review with the diff and the exact PUT, a 409 handled in the dialog by moving the draft onto the newer version; View JSON edits the whole document into the draft; version history with the `ver` drawer and undo (a new version putting the old values back). `/admin/tuning` lands here |
| Moderation | `/admin/moderation` | Built: cases with a status filter and how far over its threshold each was (`case` drawer: evidence, notes, status, the host, a domain ban, a takedown), each spam signal's heaviest key against its threshold (`policy/signals`), account lookup (`acct` drawer: takedown and reverse), every takedown (`takedowns`), domain rules with add, edit, delete and history (`rule` drawer), and the ten hosts with the most accounts created throttled (`throttledAccounts`) or at their cap, with raise cap and lift, and the rest in Hosts (`?flag=thr&sort=throttled`). `/admin/cases`, `/admin/accounts?q=`, `/admin/rules` land here; `/admin/cases/<id>` and `/admin/accounts/<did>` are the full pages |
| Settings | `/admin/settings` | Built: every flag with its source and default on every node (`settings?node=`: a node switch, "differs" on a flag not the same everywhere, and the `flag` drawer's per-node values), secrets as set or not, a "changed from default or differing" filter, the build, the console, and PLC export seeding (`ops/plc`: progress, seeds written, requests, 429s, restarts, the checkpoint and each window) |

Every section is on the kit. Edges and replicas are gone for now, so their panels stay out until they come back.

## Keyboard

`⌘K` palette: on an empty query, what needs attention (each banner's notice, opening its row) and the last six details opened in this tab, then a pointer to type for the rest; typing finds sections, hosts by name, a DID or handle, nodes, verbs on a host (`ban …`, `suspend …`, `unban …`, `throttle …`, `reconnect …`, `raise cap …`), `run discovery` (all, or one source) and `add seed relay <url>` (into the policy draft). `g` then a letter jumps to a section (`g u` is the public page). `j` / `k` move through rows, `Enter` opens, `o` goes full page, `Esc` closes. `/` focuses the page's search, else the palette. `space` pauses live updates, `t` toggles the theme, `?` lists all of it.

## Trying it

```bash
cargo run --bin admin_demo                    # :2790, token "demo"
cd ui && npm run dev                          # :5790, proxies /admin/api, /api/public and /xrpc
```

A real relay with a tail and real actions: `fakepds run --hosts 5 --dids 200 --rate 150 --plc-port 29999 --lag-secs 60 --fault badsig:4:rate=0.4`, then `vlrelay --listen 127.0.0.1:2980 --memory --plc-url http://127.0.0.1:29999 --admin-token <token>` with a `--host http://127.0.0.1:3000N` per fakepds host, and `VLRELAY_URL=http://127.0.0.1:2980 npm run dev`.

## Look

- Status is a colour and a glyph, always both: `ok ●`, `warn ▲`, `err ■`, `info ◆`, `idle ○`.
- Cobalt (`--accent`) is for action, magenta (`--signal`) only for the live stream (the seq, the commit, the firehose line, the lit jack) and for the unsaved policy draft, which isn't live yet.
- The fonts are self-hosted in `public/fonts` (the page CSP allows no other origin).
- Use example.com-style names in fixtures and placeholders, and show what the server says (its hostname is the page's own). The docs and UI are public.
