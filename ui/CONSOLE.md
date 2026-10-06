# Operator console

The console at `/admin` and the public page at `/` share one look, "exchange": a switchboard at night. Bone and ink-indigo panels, cobalt for action, a magenta carrier lamp for the live stream (seq, commit, emit), Archivo widened for labels and IBM Plex Mono for numbers. The shell is the vlpds console's (`vlpds/ui/CONSOLE.md`): a top bar with the carrier rule, a patch-panel rail of sections, a dense main column and one slide-over for any row. The kit was copied from there and re-skinned, so the two consoles drive the same way.

## Where things live

| Path | What |
| --- | --- |
| `src/console.css` | Tokens and every console class. All classes start with `cx-` (or sit under one) because `styles.css` is global and owns `.btn`, `.tile`, `.seg`, `.empty`. The tokens live on `.cx`, so the public page uses them too (`.cx.cx-pubroot`). |
| `src/components/console/` | The kit: `kit.tsx` (small parts, plus `HostName`, `TierTag`, `HostStatusChip`, `Bars`, `Jack`), `DataTable.tsx` (client or server sort), `Drawer.tsx`, `dialogs.tsx`, `toast.tsx`, `LiveTail.tsx`, `Exchange.tsx` (the overview canvas), `Palette.tsx`, `Shell.tsx`, `sections.tsx` (the IA), `nav.ts`, `hostActions.tsx` (every host action behind a confirm). |
| `src/console-rules.css` | Policy, Moderation and Settings: the draft (dirty fields, knobs, the draft bar, diffs), case evidence and notes. |
| `src/lib/console/` | Data: `policyDraft.ts` (the one policy draft: diff, rebase onto a newer version, undo from an audit entry), `live.ts` (pause, stale, `createPoller`, `useLivePoll`), `polls.ts` (the shared polls, and client-side series for values the API has no history for), `relay.ts` (nodes, colours, the quorum's health), `firehose.ts` (the subscribeRepos tail), `adminAdapter.ts` (every endpoint), `fmt.ts`. |
| `src/pages/admin/` | `AdminApp.tsx` routes. `Overview.tsx` and `Hosts.tsx` are built on the kit, `hostDetail.tsx` registers the `host` detail kind, `relayUi.tsx` holds the banners they share. `Policy.tsx` (and the `ver` kind), `Moderation.tsx` with `moderationDetail.tsx` (the `case`, `acct` and `rule` kinds and every moderation confirm), `Settings.tsx` (the `flag` kind) and `Admissions.tsx` (the Hosts page's crawl admission panel). The other sections render the classic pages from `src/pages/` inside `<Legacy>`. |
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
- `seriesOf(key)` (`polls.ts`) is a series the console builds from its own polls: the stream's rate, the commit latency, each busy host's rate. It fills in while the page is open.
- `NeedsVersion what endpoint` stands in for a panel whose endpoint isn't there yet (below).

## The adapter and what the backend lane owes

`lib/console/adminAdapter.ts` is the one place the console calls the admin API. The endpoints the design assumes but the relay doesn't serve answer `{ supported: false, endpoint }` without a request, so the browser console stays clean and the page shows a "needs a newer vlRelay" placeholder. To light one up, implement it and replace its `missing(...)` with the call. `MISSING` in the same file is this list.

| Endpoint | What it feeds |
| --- | --- |
| `GET ops/tail?host=&rejects=1` | The Overview tail's rejected and held frames (the firehose carries only what passed), and following one host at full rate. Today the tail can follow a DID, a handle or a collection at full rate, but not a host: frames don't name their PDS |
| `overview.topHosts[].history` | A rate series per busy host, for the exchange's trunks and the Busiest hosts sparklines. Until then the console keeps its own from the 2 s polls |
| `qlog status.flush.last_at_ms` | "Flushed N s ago" in the health line. The status has F but not when it moved |
| `GET store/cost` | The monthly bill: node prices from config, bucket requests by class and storage, priced by provider. Object store & cost has only a placeholder until then |
| `HostRow.throttledAccounts` | Moderation › Accounts created throttled: how many each host has. Until then the panel lists the busy hosts at their cap, which are the ones creating them |
| `GET policy/usage` | Policy › Cluster budgets: PLC lookups/s and new accounts/min against their budgets (new hosts today already comes from `hosts/admissions`) |
| `GET policy/signals` | Policy › Spam signals and Moderation › Spam signals, live: the heaviest key per signal against its threshold. Open cases stand in until then |
| `GET takedowns` | Moderation: every account taken down here. Today the API answers per DID, so a takedown is found by looking the account up |
| `GET settings?node=` | Settings: each node's flags side by side and the "differs" flag. `settings` answers for the node you reach |

Found while building this, for the backend lane:

- On a relay without the quorum log or a cluster, `GET cluster` answers `Demo::cluster()`'s simulated three-node layout (`node/admin.rs`). The console ignores it when `/api/public/stats` counts one node and draws the single node the hosts name as their reader.
- `GET ops/pipeline` is a 404 on `admin_demo`, so the Overview's "ack backlog" tile became "durability lag" from the overview's own history. Phase 2's Quorum page can bring the backlog back with an adapter fallback.
- `admin_demo` lists a few hostnames twice (`pds.kettle.social`, `bsky.pixel.net`, `pds.moss.social`).
- `admin_demo` serves no `subscribeRepos`, so the tail says the firehose isn't reachable there. `npm run dev` proxies `/xrpc` (websockets too), so the tail works against a real relay.
- The console's own tail is a consumer: it shows in the consumer list with the browser's user agent.

## Sections

| Section | Path | State |
| --- | --- | --- |
| Overview | `/admin` | Built: banners (quorum held or degraded, a node not answering, throttled hosts falling behind, hosts at their account cap, a slow consumer), the health line, eight tiles, the exchange, rejects by reason, busiest hosts, the sampled tail, and a rail with the stream, members, consumers, open cases and the bill |
| Hosts | `/admin/hosts` | Built: status tiles, the paged table (server-side filter, sort and page; the at-cap, lagging and erroring flags filter here over every match), the `host` drawer and full page with every host action, crawl admission (placeholder) and tiers. `/admin/hosts/<host>` still lands on the host |
| Consumers | `/admin/consumers` | Classic page |
| Quorum & cluster | `/admin/quorum` | Classic pages: Quorum log, Cluster (`/admin/cluster`), Operations (`/admin/ops`) |
| Object store & cost | `/admin/store` | Placeholder |
| Policy | `/admin/policy` | Built: the tier matrix and every knob of the full document (`policy/full`, or the older tier form on `policy`), each with its default and its use now where the API has it; one draft with a sticky bar, Review with the diff and the exact PUT, a 409 handled in the dialog by moving the draft onto the newer version; View JSON edits the whole document into the draft; version history with the `ver` drawer and undo (a new version putting the old values back). `/admin/tuning` lands here |
| Moderation | `/admin/moderation` | Built: cases with a status filter (`case` drawer: evidence, notes, status, the host, a domain ban, a takedown), spam signals against thresholds from open cases, account lookup (`acct` drawer: takedown and reverse), domain rules with add, edit, delete and history (`rule` drawer), and hosts at their account cap with raise cap and release-throttled. `/admin/cases`, `/admin/accounts?q=`, `/admin/rules` land here; `/admin/cases/<id>` and `/admin/accounts/<did>` are the full pages |
| Settings | `/admin/settings` | Built: every flag with its source and default, secrets as set or not, a "changed from default" filter, the `flag` drawer, the build, the console, and PLC export seeding (`ops/plc`, asked only when a `--plc-export` flag is on). Ready for `settings?node=`: a node switch and "differs" once more than one node answers |

Phase 2 rebuilds the rest on the kit: Consumers (per node, read tier, kick behind a confirm; the lists answer for the node you reach until they're rebuilt over the qlog peer protocol, so label them "this node"), Quorum & cluster (the log rail, leadership history, members, membership changes, flush and counters), Object store & cost, Policy (one draft with a diff and a versioned save), Moderation (cases, accounts and takedowns as detail kinds) and Settings. Edges and replicas are gone for now, so their panels stay out until they come back.

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
- Cobalt (`--accent`) is for action, magenta (`--signal`) only for the live stream (the seq, the commit, the firehose line, the lit jack) and for the unsaved policy draft, which isn't live yet.
- The fonts are self-hosted in `public/fonts` (the page CSP allows no other origin).
- Use example.com-style names in fixtures and placeholders, and show what the server says (its hostname is the page's own). The docs and UI are public.
