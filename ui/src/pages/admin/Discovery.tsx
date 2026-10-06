import type { ReactNode } from 'react'
import { DataTable, type Col } from '../../components/console/DataTable'
import { confirmAction } from '../../components/console/dialogs'
import { registerDetail } from '../../components/console/Drawer'
import { Chip, Empty, KV, Loaded, Meter, PageHead, Panel, Sec, Src, Strip, Tiles, type TileSpec } from '../../components/console/kit'
import { openPanel } from '../../components/console/nav'
import { registerPalette, type PalItem } from '../../components/console/Palette'
import { toast } from '../../components/console/toast'
import type { DiscoverySource, DiscoveryView } from '../../lib/api'
import * as A from '../../lib/console/adminAdapter'
import { ago, dt, dur, fmtNum } from '../../lib/console/fmt'
import { getDraft } from '../../lib/console/policyDraft'
import { discoveryPoll } from '../../lib/console/polls'
import { useRelay } from '../../lib/console/relay'
import { Link, navigate } from '../../lib/router'
import '../../console-rules.css'
import { addSeedRelay, DiscoveryPolicy, DraftBar, normSeedUrl, policySourcePoll, seedUrlError } from './Policy'
import { NodeTag } from './relayUi'

// Host discovery for a cold start, run by the leader: each seed relay's listHosts and the PDS
// hosts the PLC export names, with each run's counts and a run-now behind a confirm. The seed
// relays and the PLC switch are policy fields, edited here through the policy's one draft.

/** `bootstrap:relay.example.com` as "relay.example.com", `plc` as "PLC export". */
export const sourceLabel = (key: string) => (key === 'plc' ? 'PLC export' : key.startsWith('bootstrap:') ? key.slice('bootstrap:'.length) : key)
const kindOf = (key: string) => (key === 'plc' ? 'PDS hosts it names' : key.startsWith('bootstrap:') ? 'seed relay' : key)

function StatusChip({ s }: { s: DiscoverySource }) {
  if (!s.enabled) return <Chip k="idle">off</Chip>
  if (s.inProgress) return <Chip k="info">running</Chip>
  if (s.runRequested) return <Chip k="info">requested</Chip>
  if (s.lastError) return <Chip k="err">error</Chip>
  if (!s.runs && !s.lastFinishedMs) return <Chip k="idle">not run yet</Chip>
  return <Chip k="ok">idle</Chip>
}

/** This run's (or the last one's) pace: admissions a minute and pages a second since it started. */
function pace(s: DiscoverySource, now = Date.now()) {
  if (!s.lastStartedMs) return undefined
  const end = s.inProgress ? now : (s.lastFinishedMs ?? now)
  const secs = Math.max(1, (end - s.lastStartedMs) / 1000)
  return { secs, admitsPerMin: (s.admitted / secs) * 60, pagesPerSec: s.pages / secs }
}

export function runDialog(v: DiscoveryView | undefined, s?: DiscoverySource) {
  const all = !s
  const on = (v?.sources ?? []).filter((x) => x.enabled)
  const what = s ? (s.key === 'plc' ? 'the PLC export’s hosts' : `${sourceLabel(s.key)}’s listHosts`) : `every enabled source (${on.map((x) => sourceLabel(x.key)).join(', ') || 'none'})`
  return confirmAction({
    tone: 'warn',
    primary: true,
    title: all ? 'Run discovery now?' : `Run ${sourceLabel(s.key)} now?`,
    items: [
      <>
        {v?.leader ?? 'The leader'} reads {what} now instead of at the next interval
        {s?.key !== 'plc' && v ? `, a page at a time at up to ${fmtNum(v.requestsPerSec, 1)}/s per relay` : ''}.
      </>,
      `Every new host goes through this relay's own admission (hostname rules, bans, allow-list, starting tier, the describeServer probe), at up to ${v ? fmtNum(v.connectsPerMin) : '?'} connects a minute. It doesn't spend requestCrawl's daily budget.`,
      ...(s?.inProgress ? ['A run of it is in progress already.'] : []),
      ...(s && !s.enabled ? [<span className="s-err">It's off in the policy: the leader refuses to run it.</span>] : []),
    ],
    action: 'Run now',
    call: A.runDiscoveryCall(s?.key),
    run: () => A.runDiscovery(s?.key),
    done: () => {
      discoveryPoll.refresh()
      return all ? 'Discovery requested' : `${sourceLabel(s.key)} requested`
    },
  })
}

export function Discovery() {
  const l = discoveryPoll.use()
  policySourcePoll.use()
  const { view } = useRelay()
  const v = l.data
  const src = v?.sources ?? []
  const running = src.filter((s) => s.inProgress)
  const admitPace = running.reduce((a, s) => a + (pace(s)?.admitsPerMin ?? 0), 0)
  const sum = (k: 'hostsSeen' | 'new' | 'admitted' | 'refused' | 'errors') => src.reduce((a, s) => a + s[k], 0)
  const tiles: TileSpec[] = [
    { label: 'Sources', value: fmtNum(src.filter((s) => s.enabled).length), unit: `of ${src.length} on`, sec: running.length ? `${running.length} running` : 'none running' },
    { label: 'Hosts seen', right: 'last runs', value: fmtNum(sum('hostsSeen')), sec: `${fmtNum(sum('new'))} new to this relay` },
    { label: 'Admitted', right: 'last runs', value: fmtNum(sum('admitted')), sec: `${fmtNum(sum('refused'))} refused · ${fmtNum(sum('errors'))} errors` },
    {
      label: 'Connects',
      right: 'budget',
      value: v ? (
        <>
          {running.length ? <Meter v={admitPace} max={v.connectsPerMin} k={admitPace > v.connectsPerMin * 0.9 ? 'warn' : 'ok'} /> : null} {running.length ? fmtNum(admitPace, 1) : '—'}
        </>
      ) : (
        '—'
      ),
      unit: v ? `of ${fmtNum(v.connectsPerMin)}/min` : undefined,
      sec: running.length ? 'admitted a minute' : 'nothing running',
      title: 'Admissions a minute by the running sources, since each started, against discovery.connectsPerMin',
    },
    { label: 'listHosts', right: 'budget', value: v ? fmtNum(v.requestsPerSec, 1) : '—', unit: '/s per relay', sec: 'pages to any one relay' },
  ]
  const cols: Col<DiscoverySource>[] = [
    {
      id: 'src',
      label: 'Source',
      sort: (a, b) => a.key.localeCompare(b.key),
      render: (s) => (
        <span className="cx-cellid">
          <span className="mono">{sourceLabel(s.key)}</span>
          <span className="muted sm">{kindOf(s.key)}</span>
        </span>
      ),
    },
    { id: 'st', label: 'Status', render: (s) => <StatusChip s={s} /> },
    {
      id: 'last',
      label: 'Last run',
      r: true,
      sort: (a, b) => (a.lastStartedMs ?? 0) - (b.lastStartedMs ?? 0),
      render: (s) =>
        s.inProgress && s.lastStartedMs ? (
          <span className="sm" title={dt(s.lastStartedMs)}>
            started {ago(s.lastStartedMs)}
          </span>
        ) : s.lastFinishedMs ? (
          <span className="sm muted" title={`${s.lastStartedMs ? `${dt(s.lastStartedMs)} → ` : ''}${dt(s.lastFinishedMs)}`}>
            {ago(s.lastFinishedMs)}
            {s.lastStartedMs ? ` · took ${dur(s.lastFinishedMs - s.lastStartedMs)}` : ''}
          </span>
        ) : (
          <span className="muted">never</span>
        ),
    },
    {
      id: 'next',
      label: 'Next',
      r: true,
      render: (s) => (
        <span className="sm muted" title={s.nextRunMs ? dt(s.nextRunMs) : undefined}>
          {!s.enabled ? '—' : s.inProgress ? 'now' : s.nextRunMs ? (s.nextRunMs <= Date.now() ? 'due' : `in ${dur(s.nextRunMs - Date.now())}`) : s.key === 'plc' ? 'as the export reads' : '—'}
        </span>
      ),
    },
    { id: 'seen', label: 'Seen', r: true, title: 'Hosts the source listed this run (or the last)', render: (s) => <span className="mono sm">{fmtNum(s.hostsSeen)}</span> },
    { id: 'new', label: 'New', r: true, title: 'Hosts this relay didn’t have', render: (s) => <span className="mono sm">{fmtNum(s.new)}</span> },
    { id: 'adm', label: 'Admitted', r: true, render: (s) => <span className="mono sm s-ok">{fmtNum(s.admitted)}</span> },
    { id: 'ref', label: 'Refused', r: true, render: (s) => <span className={`mono sm${s.refused ? ' s-warn' : ' muted'}`}>{fmtNum(s.refused)}</span> },
    {
      id: 'err',
      label: 'Errors · 429',
      r: true,
      title: 'Errors, and 429s and 5xxs waited out',
      render: (s) => (
        <span className="mono sm" title={s.lastError ?? undefined}>
          <span className={s.errors ? 's-err' : 'muted'}>{fmtNum(s.errors)}</span> · <span className={s.throttled ? 's-warn' : 'muted'}>{fmtNum(s.throttled)}</span>
        </span>
      ),
    },
    { id: 'pages', label: 'Pages', r: true, render: (s) => <span className="mono sm muted">{s.key === 'plc' ? (s.pending ? `${fmtNum(s.pending)} pending` : '—') : fmtNum(s.pages)}</span> },
    {
      id: 'run',
      label: '',
      r: true,
      render: (s) => (
        <button
          type="button"
          className="cx-btn sm"
          disabled={!s.enabled}
          title={s.enabled ? undefined : 'Off in the policy'}
          onClick={(e) => {
            e.stopPropagation()
            runDialog(v, s)
          }}
        >
          Run now…
        </button>
      ),
    },
  ]
  return (
    <>
      <PageHead
        title="Discovery"
        sub={
          v ? (
            <>
              <span style={{ display: 'inline-flex', gap: 6, alignItems: 'center' }}>run by {v.leader ? <NodeTag view={view} id={v.leader} /> : 'no leader'}</span>
              <span>
                {fmtNum(v.connectsPerMin)} connects/min · {fmtNum(v.requestsPerSec, 1)} listHosts/s per relay
              </span>
            </>
          ) : (
            <span>…</span>
          )
        }
        actions={
          <button type="button" className="cx-btn" disabled={!src.some((s) => s.enabled)} onClick={() => runDialog(v)}>
            Run all now…
          </button>
        }
      />
      <div className="cx-stack">
        <div className="cx-tilesbox">
          <Tiles tiles={tiles} />
        </div>
        <Panel
          title="Sources"
          src={<Src>discovery</Src>}
          right={<span className="muted sm">counts are each source's current or last run</span>}
          foot={
            <span>
              Each source's progress is saved in the bucket after every page, so a new leader resumes a list where the old one stopped. Nothing comes from a seed relay but the hostnames:
              not its statuses, bans or tiers. <Link to="/admin/hosts?source=bootstrap">Hosts found by discovery →</Link>
            </span>
          }
        >
          <Loaded load={l}>
            {() => (
              <DataTable
                rows={src}
                cols={cols}
                rowKey={(s) => s.key}
                open={(s) => ({ type: 'dsource', id: s.key })}
                dim={(s) => !s.enabled}
                compact
                label="Discovery sources"
                empty={<Empty title="No sources">Add a seed relay below, or turn on the PLC source (with --plc-export).</Empty>}
              />
            )}
          </Loaded>
        </Panel>
        <Panel title="Seed relays and budgets" to="/admin/policy" src={<Src>policy/full · discovery</Src>} right={<span className="muted sm">edits stay a draft until you save</span>} foot={<span>Saved as a new policy version; every node reloads it within 10 s and the leader picks up new sources on its next pass.</span>}>
          <DiscoveryPolicy />
        </Panel>
      </div>
      <DraftBar />
    </>
  )
}

// ---------------------------------------------------------------- one source

registerDetail('dsource', {
  kind: 'Discovery source',
  section: 'discovery',
  use: (id) => {
    const l = discoveryPoll.use()
    const v = l.data
    const s = v?.sources.find((x) => x.key === id)
    if (!s) return { title: sourceLabel(id), body: null, loading: l.loading, missing: l.loading ? undefined : 'No such source: it left the policy.' }
    const p = pace(s)
    const body: ReactNode = (
      <>
        <Strip
          items={[
            ['seen', fmtNum(s.hostsSeen)],
            ['new', fmtNum(s.new)],
            ['admitted', fmtNum(s.admitted)],
            ['refused', fmtNum(s.refused)],
          ]}
        />
        {p && v && (
          <Sec title={s.inProgress ? 'This run' : 'The last run'} digest={dur(p.secs * 1000)} open>
            <KV
              rows={[
                [
                  'Admissions',
                  <span className="mono">
                    <Meter v={p.admitsPerMin} max={v.connectsPerMin} k="ok" /> {fmtNum(p.admitsPerMin, 1)}/min of {fmtNum(v.connectsPerMin)}
                  </span>,
                ],
                ...(s.key === 'plc'
                  ? []
                  : ([
                      [
                        'listHosts pages',
                        <span className="mono">
                          <Meter v={p.pagesPerSec} max={v.requestsPerSec} k="ok" /> {fmtNum(p.pagesPerSec, 2)}/s of {fmtNum(v.requestsPerSec, 1)} ({fmtNum(s.pages)} pages)
                        </span>,
                      ],
                    ] as [string, ReactNode][])),
                ['Errors', <span className={s.errors ? 's-err' : undefined}>{fmtNum(s.errors)}</span>],
                ['429s and 5xxs waited out', fmtNum(s.throttled)],
                ['Resumed by a new leader', fmtNum(s.resumed)],
              ]}
            />
          </Sec>
        )}
        <Sec title="Source" open>
          <KV
            rows={[
              ['Status', <StatusChip s={s} />],
              ['URL', <span className="mono">{s.url ?? (s.key === 'plc' ? 'the --plc-export reader' : '—')}</span>],
              ['Refresh', s.refreshIntervalSecs ? `every ${dur(s.refreshIntervalSecs * 1000)}` : '—'],
              ['Runs', fmtNum(s.runs)],
              ['Started', s.lastStartedMs ? dt(s.lastStartedMs) : '—'],
              ['Finished', s.lastFinishedMs ? dt(s.lastFinishedMs) : '—'],
              ['Next', s.nextRunMs ? dt(s.nextRunMs) : '—'],
              ...(s.cursor ? ([['Cursor', <span className="mono">{s.cursor}</span>]] as [string, ReactNode][]) : []),
              ...(s.key === 'plc' ? ([['Pending admission', fmtNum(s.pending)]] as [string, ReactNode][]) : []),
            ]}
          />
          {s.lastError && (
            <div className="dlg-err" role="alert">
              <span className="cx-g">■</span>
              <span className="mono sm">{s.lastError}</span>
            </div>
          )}
        </Sec>
        <div className="cx-form-row">
          <button type="button" className="cx-btn" disabled={!s.enabled} onClick={() => runDialog(v, s)}>
            Run now…
          </button>
          <Link className="cx-btn quiet" to={`/admin/hosts?source=${encodeURIComponent(s.key)}`}>
            Hosts it found →
          </Link>
        </div>
      </>
    )
    return { title: sourceLabel(s.key), chip: <StatusChip s={s} />, foot: <>GET /admin/api/discovery · run by {v?.leader ?? 'the leader'}</>, body }
  },
})

// ---------------------------------------------------------------- ⌘K

/** Waits for the policy draft (the Discovery page loads it), then adds the relay to it. */
async function paletteAddSeed(url: string) {
  navigate('/admin/discovery')
  policySourcePoll.refresh()
  for (let i = 0; i < 50 && !getDraft().body; i++) await new Promise((r) => setTimeout(r, 100))
  if (!getDraft().body) return toast("The policy didn't load", { err: true })
  const u = normSeedUrl(url)
  const bad = seedUrlError(u)
  if (bad) return toast(bad, { err: true })
  toast(addSeedRelay(u) ? `Added ${u} to the policy draft: review and save it` : `${u} is already a seed relay`)
}

/** The run dialog from ⌘K: on the Discovery page, once the sources have loaded. */
async function paletteRun(key?: string) {
  navigate('/admin/discovery')
  for (let i = 0; i < 50 && !discoveryPoll.get().data; i++) await new Promise((r) => setTimeout(r, 100))
  const v = discoveryPoll.get().data
  void runDialog(v, key ? v?.sources.find((s) => s.key === key) : undefined)
}

registerPalette({
  items: (q) => {
    const out: PalItem[] = []
    const m = q.match(/^add seed relay\s+(\S+)$/i)
    if (m) out.push({ group: 'Actions', glyph: '+', title: `Add seed relay ${m[1]}…`, desc: 'into the policy draft (discovery.seedRelays)', hay: q, run: () => void paletteAddSeed(m[1]) })
    else if (/^add seed/i.test(q)) out.push({ group: 'Actions', glyph: '+', title: 'Add a seed relay…', desc: 'type its URL after "add seed relay"', hay: q, run: () => navigate('/admin/discovery') })
    if (/^run\b|discover/i.test(q)) {
      const v = discoveryPoll.get().data
      out.push({ group: 'Actions', glyph: '↻', title: 'Run discovery now…', desc: 'every enabled source · discovery/run', hay: 'run discovery', run: () => void paletteRun() })
      for (const s of v?.sources ?? [])
        if (s.enabled) out.push({ group: 'Actions', glyph: '↻', title: `Run ${sourceLabel(s.key)} now…`, desc: `discovery/run ${s.key}`, hay: `run discovery ${s.key}`, run: () => void paletteRun(s.key) })
    }
    if (/^disc|seed/i.test(q)) for (const s of discoveryPoll.get().data?.sources ?? []) out.push({ group: 'Discovery', glyph: '◇', title: sourceLabel(s.key), desc: kindOf(s.key), run: () => (navigate('/admin/discovery'), openPanel('dsource', s.key)) })
    return out
  },
})
