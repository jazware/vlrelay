import { useMemo, useState } from 'react'
import { DataTable, type Col } from '../../components/console/DataTable'
import { registerDetail } from '../../components/console/Drawer'
import { openPanel } from '../../components/console/nav'
import { registerPalette } from '../../components/console/Palette'
import { Chip, Empty, KV, Loaded, Meter, PageHead, Panel, SearchInput, Sec, Seg, Src, Strip, Tiles, Toggle } from '../../components/console/kit'
import { errText, setAdminToken, type ConfigEntry, type SettingsView } from '../../lib/api'
import * as A from '../../lib/console/adminAdapter'
import { ago, dt, dur, fmtNum, fmtSi, plural } from '../../lib/console/fmt'
import { useLiveState, toggleSources, useLivePoll } from '../../lib/console/live'
import { publicPoll, settingsPoll } from '../../lib/console/polls'
import { useRelay } from '../../lib/console/relay'
import { useAdminOperator } from '../../lib/hooks'
import { navigate, useSearch } from '../../lib/router'
import { NodeTag } from './relayUi'
import '../../console-rules.css'

// The process config each node was started with (GET settings, and settings?node= for the other
// members): every flag with its source, secrets only as set or not, and the flags that differ
// across nodes. Flags change with a restart; what changes live is on Policy.

type Src = ConfigEntry['source']
const changed = (e: ConfigEntry) => !e.secret && (e.source === 'flag' || e.source === 'env') && e.value !== e.default

/** Each node's flags: the answering node's from the shared poll, every other member's asked through it. */
function useNodes() {
  const { view } = useRelay()
  const self = settingsPoll.use()
  const ids = (view?.nodes ?? []).map((n) => n.id).filter((id) => id !== view?.self)
  const others = useLivePoll(
    () =>
      Promise.all(
        ids.map((id) =>
          A.settingsOf(id).then(
            (v) => [id, v] as const,
            (e: unknown) => [id, errText(e)] as const,
          ),
        ),
      ),
    ids.join(','),
    60_000,
    { keep: true },
  )
  const per = new Map<string, SettingsView>()
  if (self.data) per.set(view?.self ?? 'this node', self.data)
  const silent = new Map<string, string>()
  for (const [id, r] of others.data ?? []) {
    if (typeof r === 'string') silent.set(id, r)
    else per.set(id, r)
  }
  return { self, view, per, silent }
}

/** The flags whose value isn't the same on every node that answered. */
function differs(per: Map<string, SettingsView>): Set<string> {
  const out = new Set<string>()
  if (per.size < 2) return out
  const vals = new Map<string, Set<string>>()
  for (const s of per.values())
    for (const e of s.entries) {
      const v = e.secret ? (e.set ? 'set' : 'unset') : (e.value ?? '∅')
      if (!vals.has(e.flag)) vals.set(e.flag, new Set())
      vals.get(e.flag)!.add(v)
    }
  for (const [f, v] of vals) if (v.size > 1) out.add(f)
  return out
}

function ValueCell({ e }: { e: ConfigEntry }) {
  if (e.secret) return e.set ? <Chip k="ok">set</Chip> : <Chip k="idle">not set</Chip>
  if (e.value === null) return <span className="muted">—</span>
  return <span className={`mono sm${changed(e) ? ' s-acc' : ''}`}>{e.value}</span>
}

const SourceChip = ({ s }: { s: Src }) => (s === 'default' ? <Chip k="idle">default</Chip> : s === 'unset' ? <Chip k="plain" glyph={false}>unset</Chip> : <Chip k={s === 'flag' ? 'acc' : 'plain'} glyph={false}>{s}</Chip>)

function Flags() {
  const { self, view, per, silent } = useNodes()
  const search = useSearch()
  const [q, setQ] = useState(search.get('q') ?? '')
  const [onlyChanged, setOnlyChanged] = useState(search.get('changed') === '1')
  const nodeIds = [...per.keys()]
  const [node, setNode] = useState<string>()
  const shown = (node && per.get(node)) || self.data
  const diff = useMemo(() => differs(per), [per])
  const needle = q.trim().toLowerCase()
  const rows = (shown?.entries ?? []).filter(
    (e) =>
      (!onlyChanged || changed(e) || diff.has(e.flag)) &&
      (!needle || `${e.flag} ${e.env ?? ''} ${e.help} ${e.secret ? '' : (e.value ?? '')}`.toLowerCase().includes(needle)),
  )
  const cols: Col<ConfigEntry>[] = [
    {
      id: 'flag',
      label: 'Flag',
      sort: (a, b) => a.flag.localeCompare(b.flag),
      render: (e) => (
        <span className="mono">
          {e.flag}
          {diff.has(e.flag) && (
            <>
              {' '}
              <Chip k="warn" title="Not the same on every node">
                differs
              </Chip>
            </>
          )}
        </span>
      ),
    },
    { id: 'value', label: 'Value', render: (e) => <span className="trunc" style={{ display: 'inline-block', maxWidth: 360, verticalAlign: 'middle' }}><ValueCell e={e} /></span> },
    { id: 'source', label: 'Source', sort: (a, b) => a.source.localeCompare(b.source), render: (e) => <SourceChip s={e.source} /> },
    { id: 'default', label: 'Default', render: (e) => <span className="mono sm muted">{e.secret ? '—' : (e.default ?? '—')}</span> },
    { id: 'env', label: 'Env', render: (e) => <span className="mono sm t2">{e.env ?? '—'}</span> },
  ]
  return (
    <Panel
      title="Process flags"
      src={<Src>settings?node=</Src>}
      right={
        nodeIds.length > 1 ? (
          <Seg label="Node" value={node ?? nodeIds[0]} options={nodeIds.map((n) => ({ v: n, label: n }))} onChange={setNode} />
        ) : view && !view.single && view.self ? (
          <span className="sm t2 nowrap" style={{ display: 'inline-flex', gap: 6, alignItems: 'center' }}>
            as <NodeTag view={view} id={view.self} /> started
          </span>
        ) : undefined
      }
      foot={
        <span>
          {silent.size > 0 && <span className="s-warn">{[...silent.keys()].join(', ')} didn't answer, so the comparison leaves {silent.size === 1 ? 'it' : 'them'} out. </span>}
          Flags change with a restart. Roll a change out one node at a time; the leader hands off before its own restart.
        </span>
      }
    >
      <div className="cx-toolbar">
        <SearchInput mono value={q} placeholder="Filter flags, env, values" onChange={setQ} />
        <label className="cx-form-row" style={{ gap: 6, cursor: 'pointer' }}>
          <Toggle on={onlyChanged} onChange={setOnlyChanged} label="Changed from default only" />
          <span className="sm t2" onClick={() => setOnlyChanged(!onlyChanged)}>
            changed from default{diff.size ? ' or differing' : ''}
          </span>
        </label>
        <span className="muted sm">{shown ? `${rows.length} of ${shown.entries.length}` : ''}</span>
      </div>
      <Loaded load={self}>
        {() => (
          <DataTable
            rows={rows}
            cols={cols}
            rowKey={(e) => e.flag}
            open={(e) => ({ type: 'flag', id: e.flag })}
            compact
            label="Process flags"
            empty={<Empty title="No flag matches">{onlyChanged ? 'Every flag is at its default.' : 'Clear the filter to see every flag.'}</Empty>}
          />
        )}
      </Loaded>
    </Panel>
  )
}

const day = (ms: number) => new Date(ms).toLocaleDateString('en-US', { year: 'numeric', month: 'short', day: 'numeric' })

/** PLC export seeding: the leader reads the export into seeds every node resolves from. Any node answers ops/plc. */
function Plc() {
  const l = useLivePoll(A.plc, 'plc', 5000)
  const { view } = useRelay()
  const v = l.data
  const done = v ? v.windows.filter((w) => w.done).length : 0
  const progress = v?.windows.length ? v.windows.reduce((a, w) => a + (w.done ? 1 : w.progress), 0) / v.windows.length : 0
  return (
    <Panel
      title="PLC seeding"
      src={<Src>ops/plc</Src>}
      right={v ? v.enabled ? <Chip k={v.caughtUp ? 'ok' : 'info'}>{v.caughtUp ? 'caught up' : 'backfilling'}</Chip> : <Chip k="idle">off</Chip> : undefined}
      foot={
        v?.enabled ? (
          <span style={{ display: 'inline-flex', flexWrap: 'wrap', gap: '2px 6px', alignItems: 'center' }}>
            Read by {v.leader ? <NodeTag view={view} id={v.leader} /> : 'no node'} · checkpoint {v.checkpointMs ? <span title={dt(v.checkpointMs)}>{ago(v.checkpointMs)}</span> : 'never'} · newest op{' '}
            {v.newestMs ? <span title={dt(v.newestMs)}>{ago(v.newestMs)}</span> : '—'}. The counters are the current leader's term; the checkpoint is shared.
          </span>
        ) : undefined
      }
    >
      <Loaded load={l}>
        {(p) =>
          !p.enabled ? (
            <Empty>This relay doesn't seed DID keys from the PLC export (it runs without --plc-export): it resolves each DID on first sight.</Empty>
          ) : (
          <>
            <Strip
              items={[
                ['export read', p.windows.length ? `${Math.round(progress * 100)}%` : '—'],
                ['ops read', fmtSi(p.ops)],
                ['ops/s', fmtNum(p.opsPerSec, 1)],
                ['seeds written', fmtSi(p.written)],
                ['lookups kept', fmtSi(p.learned)],
                ['requests', fmtSi(p.requests)],
                ['throttled (429)', fmtNum(p.throttled)],
                ['errors', fmtNum(p.errors)],
                ['restarts', fmtNum(p.restarts)],
              ]}
            />
            {p.windows.length > 0 && (
              <Sec title="Windows" digest={`${done} of ${p.windows.length} done`} open flush>
                <div className="cx-tw">
                  <table className="cx-t compact">
                    <thead>
                      <tr>
                        <th>From</th>
                        <th>Until</th>
                        <th>Read to</th>
                        <th className="r">Ops</th>
                        <th>Progress</th>
                      </tr>
                    </thead>
                    <tbody>
                      {p.windows.map((w, i) => (
                        <tr key={i}>
                          <td className="sm nowrap">{day(w.fromMs)}</td>
                          <td className="sm nowrap">{w.untilMs ? day(w.untilMs) : 'live'}</td>
                          <td className="sm muted">{w.done ? '' : w.afterMs ? `at ${dt(w.afterMs)}` : 'not started'}</td>
                          <td className="r mono sm">{fmtSi(w.ops)}</td>
                          <td className="sm">
                            <Meter v={w.progress} max={1} k={w.done ? 'ok' : 'info'} /> <span className="mono">{w.done ? 'done' : `${Math.round(w.progress * 100)}%`}</span>
                          </td>
                        </tr>
                      ))}
                    </tbody>
                  </table>
                </div>
              </Sec>
            )}
            {p.nodes.length > 1 && (
              <Sec title="Per node" flush>
                <div className="cx-tw">
                  <table className="cx-t compact">
                    <tbody>
                      {p.nodes.map((n) => (
                        <tr key={n.node} className={n.stale ? 'dim' : undefined}>
                          <td>
                            <NodeTag view={view} id={n.node} /> {n.leader && <Chip k="acc" glyph={false}>reader</Chip>}
                          </td>
                          <td className="r mono sm">{fmtSi(n.ops)} ops</td>
                          <td className="r mono sm">{fmtNum(n.opsPerSec, 1)}/s</td>
                          <td className="r mono sm">{fmtNum(n.throttled)} 429s</td>
                          <td className="r mono sm">{fmtNum(n.errors)} errors</td>
                        </tr>
                      ))}
                    </tbody>
                  </table>
                </div>
              </Sec>
            )}
          </>
          )
        }
      </Loaded>
    </Panel>
  )
}

export function Settings() {
  const s = settingsPoll.use()
  const pub = publicPoll.use().data
  const live = useLiveState()
  const operator = useAdminOperator()
  const { view } = useRelay()
  const e = s.data?.entries ?? []
  const count = (src: Src) => e.filter((x) => x.source === src).length
  const secrets = e.filter((x) => x.secret)
  return (
    <>
      <PageHead
        title="Settings"
        sub={
          s.data ? (
            <>
              <span>
                <span className="mono">{s.data.binary}</span> {s.data.version}
              </span>
              <span>the flags each node was started with</span>
            </>
          ) : (
            <span>…</span>
          )
        }
      />
      <div className="cx-stack">
        <div className="cx-tilesbox">
          <Tiles
            tiles={[
              { label: 'From flags', value: fmtNum(count('flag')) },
              { label: 'From environment', value: fmtNum(count('env')) },
              { label: 'Changed from default', value: fmtNum(e.filter(changed).length) },
              { label: 'Defaults', value: fmtNum(count('default')) },
              { label: 'Unset', value: fmtNum(count('unset')) },
              { label: 'Secrets set', value: fmtNum(secrets.filter((x) => x.set).length), unit: `of ${secrets.length}`, title: 'Their values never leave the node' },
            ]}
          />
        </div>
        <Flags />
        <div className="cx-grid2">
          <Panel title="This build" src={<><Src>settings · binary, version</Src> <Src>/api/public/stats</Src></>}>
            <KV
              style={{ padding: '10px 12px' }}
              rows={[
                ['Version', <span key="v" className="mono">{s.data ? `${s.data.binary} ${s.data.version}` : '…'}</span>],
                ['Up', pub ? `${dur(pub.uptimeSecs * 1000)} (since ${dt(pub.timeMs - pub.uptimeSecs * 1000)})` : '…'],
                ['Nodes', view ? (view.single ? 'one node' : `${plural(view.nodes.length, 'node')}: ${view.nodes.map((n) => n.id).join(', ')}`) : '…'],
                ['Same build on', view && !view.single ? (new Set(view.nodes.map((n) => n.version).filter(Boolean)).size > 1 ? <Chip key="d" k="warn">mixed versions</Chip> : 'every node') : '—'],
              ]}
            />
          </Panel>
          <Panel
            title="Console"
            foot={
              operator ? undefined : (
                <button
                  type="button"
                  className="cx-btn sm"
                  onClick={() => {
                    setAdminToken(null)
                    navigate('/admin')
                  }}
                >
                  Lock console
                </button>
              )
            }
          >
            <KV
              style={{ padding: '10px 12px' }}
              rows={[
                ['Answered by', view?.self ? <NodeTag key="n" view={view} id={view.self} /> : location.host],
                ['Other nodes', view && !view.single ? 'asked over the peer admin RPC; one slower than 1.5 s shows dashes' : 'none'],
                operator
                  ? ['Signed in', <span key="o">as <span className="mono">{operator}</span>, by the proxy in front of the admin listener</span>]
                  : ['Admin token', 'kept in this tab only'],
                [
                  'Data sources',
                  <span key="s" className="cx-form-row" style={{ gap: 6 }}>
                    <Toggle on={live.showSources} onChange={toggleSources} label="Show data sources" /> <span className="sm t2">show which endpoint feeds each panel</span>
                  </span>,
                ],
              ]}
            />
          </Panel>
        </div>
        <Plc />
      </div>
    </>
  )
}

registerDetail('flag', {
  kind: 'Flag',
  section: 'settings',
  use: (id) => {
    const { self, view, per, silent } = useNodes()
    const e = self.data?.entries.find((x) => x.flag === id)
    if (!e) return { title: id, body: null, loading: self.loading, missing: self.data ? `This relay has no ${id}.` : undefined }
    const vals = [...per.entries()].map(([n, s]) => [n, s.entries.find((x) => x.flag === id)] as const)
    return {
      title: e.flag,
      chip: e.secret ? <Chip k={e.set ? 'ok' : 'idle'}>{e.set ? 'secret · set' : 'secret · not set'}</Chip> : <SourceChip s={e.source} />,
      foot: <>GET /admin/api/settings?node=</>,
      body: (
        <>
          {e.help && <p className="cx-lede">{e.help}</p>}
          <KV
            rows={[
              ['Value', <ValueCell key="v" e={e} />],
              ['Source', <SourceChip key="s" s={e.source} />],
              ['Default', <span key="d" className="mono">{e.secret ? '—' : (e.default ?? '—')}</span>],
              ['Env', <span key="e" className="mono">{e.env ?? '—'}</span>],
            ]}
          />
          <Sec title="Per node" digest={vals.length > 1 ? (differs(per).has(id) ? 'differs by node' : 'same everywhere') : 'this node'} open flush>
            <div className="cx-tw">
              <table className="cx-t compact">
                <tbody>
                  {vals.map(([n, x]) => (
                    <tr key={n}>
                      <td>{view?.byId.has(n) ? <NodeTag view={view} id={n} /> : <span className="sm">{n}</span>}</td>
                      <td className="mono sm">{x ? <ValueCell e={x} /> : <span className="muted">not reported</span>}</td>
                    </tr>
                  ))}
                  {[...silent].map(([n, why]) => (
                    <tr key={n} className="dim">
                      <td>{view?.byId.has(n) ? <NodeTag view={view} id={n} /> : <span className="sm">{n}</span>}</td>
                      <td className="sm muted" title={why}>
                        didn't answer
                      </td>
                    </tr>
                  ))}
                </tbody>
              </table>
            </div>
          </Sec>
          <p className="muted sm" style={{ margin: 0 }}>
            Flags change with a restart. {e.source === 'env' ? `Set in the environment as ${e.env}.` : e.source === 'flag' ? 'Set on the command line.' : ''}
          </p>
        </>
      ),
    }
  },
})

registerPalette({
  items: () =>
    (settingsPoll.get().data?.entries ?? []).map((e) => ({
      group: 'Flags',
      glyph: '⚑',
      title: e.flag,
      desc: e.secret ? (e.set ? 'secret · set' : 'secret · not set') : `${e.value ?? '—'} · ${e.source}`,
      hay: `${e.env ?? ''} ${e.help}`,
      run: () => {
        navigate('/admin/settings')
        openPanel('flag', e.flag)
      },
    })),
})
