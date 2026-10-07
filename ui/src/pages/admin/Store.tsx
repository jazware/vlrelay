import type { ReactNode } from 'react'
import { Chip, Empty, KV, Loaded, Meter, PageHead, Panel, Spark, Src, Tiles, Updated, type TileSpec } from '../../components/console/kit'
import type { QCounts, QuorumView, SettingsView } from '../../lib/api'
import * as A from '../../lib/console/adminAdapter'
import { ago, dur, fmtBytes, fmtNum, fmtSi, fmtUs, seqS } from '../../lib/console/fmt'
import { requestRates, seriesOf, useQuorum, useSettings, useStore } from '../../lib/console/queries'
import { useRelay } from '../../lib/console/relay'
import './logPages.css'
import { memberRows } from './quorumUi'
import { NodeTag } from './relayUi'

// The bucket the quorum log writes. GET store is the answering node's: its requests by purpose
// and R2 class with their rates and bytes, request latency by op, and the leader's last retention
// pass. The members' statuses add every member's requests by key component. Counts and rates
// only: no prices.

const PURPOSE: Record<string, string> = {
  flush: "the leader's fence, segments, manifest CAS and checkpoint deletes",
  state: "the leader's SlateDB: memtable uploads, checkpoints, manifest polls, compactor and GC",
  leader: 'qlog/leader reads and CASes: takeovers and membership changes',
  recovery: "a bucket recovery: the manifest, orphans, the state's clone, salvaged segments",
  backfill: "segments for a follower behind the leader's disk, a recovery's catch-up, consumers' old cursors",
  retain: 'the retention report and its deletes',
  tool: 'qlog verify and qlog check',
}

const zero = (): QCounts => ({ a: 0, b: 0, free: 0 })
const add = (x: QCounts, y?: QCounts) => {
  if (!y) return x
  x.a += y.a
  x.b += y.b
  x.free += y.free
  return x
}

/** Every member's requests by component and in total, from the statuses (since each process started). */
function memberTotals(q: QuorumView) {
  const byComponent: Record<string, QCounts> = {}
  const perMember: { id: string; c: QCounts }[] = []
  for (const n of q.nodes) {
    const r = n.stale ? undefined : n.status?.requests
    if (!r?.total) continue
    for (const [k, v] of Object.entries(r.by_component ?? {})) add((byComponent[k] ??= zero()), v)
    perMember.push({ id: n.node, c: { ...r.total } })
  }
  return { byComponent, perMember }
}

const setting = (s: SettingsView | undefined, flag: string) => s?.entries.find((e) => e.flag === flag)

const MONTH_SECS = 30 * 86_400
const cnt = (n: number) => (n < 10_000 ? fmtNum(n) : fmtSi(n))
const rate = (v: number | undefined) => (v === undefined ? '—' : v === 0 ? '0' : v < 0.01 ? '<0.01' : v < 10 ? v.toFixed(2) : fmtSi(v))

export function Store() {
  const st = useStore()
  const qp = useQuorum()
  const sp = useSettings()
  const { view } = useRelay()
  const leader = view?.quorum?.leader
  const s = sp.data
  const v = st.data
  const bucket = setting(s, '--s3-bucket')?.value
  const endpoint = setting(s, '--s3-endpoint')?.value
  const prefix = setting(s, '--prefix')?.value
  const sub = (
    <>
      {endpoint && <span className="mono">{endpoint.replace(/^https?:\/\//, '')}</span>}
      {bucket && (
        <span className="mono">
          {bucket}
          {prefix ? `/${prefix}` : ''}
        </span>
      )}
      {v && <span>answered by {v.node}</span>}
      {v && leader && leader !== v.node && <span>{leader} leads and sends most of the requests: its console answers for it</span>}
      <Updated l={st} />
    </>
  )
  if (!v)
    return (
      <>
        <PageHead title="Object store" sub={sub} />
        <Loaded load={st}>{() => null}</Loaded>
      </>
    )
  const qv = qp.data?.supported ? qp.data.data : undefined
  const rows = qv ? memberRows(qv, view) : []
  const mt = qv ? memberTotals(qv) : { byComponent: {} as Record<string, QCounts>, perMember: [] as { id: string; c: QCounts }[] }
  const rr = requestRates()
  const ret = A.retentionOf(v)
  const win = v.windowSecs > 0 ? `last ${dur(v.windowSecs * 1000)}` : 'filling in'
  const tot = v.total

  // what the current rate comes to over a month: a count, not a price
  const month = (perSec: number) => <div className="cx-tsub">{v.windowSecs > 0 ? `≈ ${fmtSi(perSec * MONTH_SECS)} per 30 days at this rate` : win}</div>
  const monthTitle = `${v.node}'s rate over the ${win}, kept up for 30 days`
  const tiles: TileSpec[] = [
    { label: 'Class A', right: 'writes, lists', value: v.windowSecs > 0 ? rate(tot.perSec.a) : '—', unit: '/s', sec: win, title: monthTitle, spark: (
        <>
          <Spark data={seriesOf('store-a')} color="c3" />
          {month(tot.perSec.a)}
        </>
      ) },
    { label: 'Class B', right: 'reads', value: v.windowSecs > 0 ? rate(tot.perSec.b) : '—', unit: '/s', sec: win, title: monthTitle, spark: (
        <>
          <Spark data={seriesOf('store-b')} color="c5" />
          {month(tot.perSec.b)}
        </>
      ) },
    { label: 'Since start', right: v.node, value: cnt(tot.requests.a), unit: 'A', sec: `${cnt(tot.requests.b)} B · ${cnt(tot.requests.free)} free` },
    { label: 'Payload', right: v.node, value: fmtBytes(tot.bytesUp), unit: 'up', sec: `${fmtBytes(tot.bytesDown)} down` },
    { label: 'Log segments', right: ret ? `as of ${ago(ret.plan.at_ms)}` : undefined, value: ret ? fmtBytes(ret.plan.segment_bytes) : '—', sec: ret ? `${fmtNum(ret.plan.segments)} segments` : 'no retention pass yet' },
    { label: 'Pruned to', right: 'older cursors: OutdatedCursor', value: ret ? seqS(ret.pruned_seq) : '—', sec: ret?.applied ? `last pass deleted ${fmtNum(ret.applied.segments)}` : undefined },
  ]

  const purposes = [...v.purposes].sort((a, b) => b.requests.a + b.requests.b - (a.requests.a + a.requests.b))
  const comps = Object.entries(mt.byComponent).sort((a, b) => b[1].a + b[1].b - (a[1].a + a[1].b))
  const maxComp = comps.length ? comps[0][1].a + comps[0][1].b : 1
  const latency = [...v.latency].sort((a, b) => b.count - a.count)

  return (
    <>
      <PageHead title="Object store" sub={sub} />
      <div className="cx-tilesbox">
        <Tiles tiles={tiles} />
      </div>
      <Panel
        title="Requests by purpose"
        src={<Src>store · purposes</Src>}
        right={
          <span className="muted sm">
            {v.node} · {win}
          </span>
        }
        foot={
          <span>
            Classes are R2's: A is every write and LIST, B every GET and HEAD, and a single DELETE is free. Totals are since {v.node} started, failed requests included; the rates are over the window
            since its previous sample.
          </span>
        }
      >
        <div className="cx-tw">
          <table className="cx-t compact">
            <thead>
              <tr>
                <th>Purpose</th>
                <th className="r">A/s</th>
                <th className="r">B/s</th>
                <th className="r" title="A and B requests in 30 days at this rate: a count, not a price">
                  30 d at this rate
                </th>
                <th className="r">A total</th>
                <th className="r">B total</th>
                <th className="r">Up</th>
                <th className="r">Down</th>
                <th>What sends them</th>
              </tr>
            </thead>
            <tbody>
              {purposes.map((p) => (
                <tr key={p.purpose}>
                  <td className="mono">{p.purpose}</td>
                  <td className="r mono sm">{rate(p.perSec.a)}</td>
                  <td className="r mono sm">{rate(p.perSec.b)}</td>
                  <td className="r mono sm t2">{v.windowSecs > 0 ? fmtSi((p.perSec.a + p.perSec.b) * MONTH_SECS) : '—'}</td>
                  <td className="r mono sm">{fmtNum(p.requests.a)}</td>
                  <td className="r mono sm">{fmtNum(p.requests.b)}</td>
                  <td className="r mono sm">{fmtBytes(p.bytesUp)}</td>
                  <td className="r mono sm">{fmtBytes(p.bytesDown)}</td>
                  <td className="sm t2" style={{ whiteSpace: 'normal', minWidth: 200 }}>
                    {PURPOSE[p.purpose] ?? ''}
                  </td>
                </tr>
              ))}
              {!purposes.length && (
                <tr>
                  <td colSpan={9}>
                    <Empty>{v.node} hasn't sent a bucket request since it started.</Empty>
                  </td>
                </tr>
              )}
            </tbody>
          </table>
        </div>
      </Panel>
      <div className="cx-grid2 cx-mt">
        <Panel title="Latency by operation" src={<Src>store · latency</Src>} right={<span className="muted sm">{v.node}, since it started</span>}>
          <div className="cx-tw">
            <table className="cx-t compact">
              <thead>
                <tr>
                  <th>Op</th>
                  <th className="r">Count</th>
                  <th className="r">Mean</th>
                  <th className="r">p50</th>
                  <th className="r">p99</th>
                </tr>
              </thead>
              <tbody>
                {latency.map((l) => (
                  <tr key={l.op}>
                    <td className="mono">{l.op}</td>
                    <td className="r mono sm">{fmtNum(l.count)}</td>
                    <td className="r mono sm">{fmtUs(l.meanMs * 1000)}</td>
                    <td className="r mono sm">{fmtUs(l.p50Ms * 1000)}</td>
                    <td className={`r mono sm${l.p99Ms > 1000 ? ' s-warn' : ''}`}>{fmtUs(l.p99Ms * 1000)}</td>
                  </tr>
                ))}
                {!latency.length && (
                  <tr>
                    <td colSpan={5}>
                      <Empty>No requests timed yet.</Empty>
                    </td>
                  </tr>
                )}
              </tbody>
            </table>
          </div>
        </Panel>
        <Panel
          title="By component"
          src={<Src>cluster/quorum · status.requests.by_component</Src>}
          right={<span className="muted sm">every member · {rr?.windowSecs ? `last ${dur(rr.windowSecs * 1000)}` : 'rates filling in'}</span>}
        >
          <div className="cx-tw">
            <table className="cx-t compact">
              <thead>
                <tr>
                  <th>Purpose / key</th>
                  <th className="r">A/s</th>
                  <th className="r">B/s</th>
                  <th className="r">Total</th>
                  <th>Share</th>
                </tr>
              </thead>
              <tbody>
                {comps.slice(0, 14).map(([k, c]) => (
                  <tr key={k}>
                    <td className="mono sm">{k}</td>
                    <td className="r mono sm">{rate(rr?.byComponent[k]?.a)}</td>
                    <td className="r mono sm">{rate(rr?.byComponent[k]?.b)}</td>
                    <td className="r mono sm" title={`${fmtNum(c.a)} A · ${fmtNum(c.b)} B · ${fmtNum(c.free)} free`}>
                      {fmtNum(c.a + c.b)}
                    </td>
                    <td>
                      <Meter v={c.a + c.b} max={maxComp} />
                    </td>
                  </tr>
                ))}
                {!comps.length && (
                  <tr>
                    <td colSpan={5}>
                      <Empty>{qv ? 'No member reports its requests by component.' : 'No member answered.'}</Empty>
                    </td>
                  </tr>
                )}
              </tbody>
            </table>
          </div>
        </Panel>
      </div>
      <div className="cx-grid2 cx-mt">
        <Retention ret={ret} />
        <Panel title="Per member" src={<Src>cluster/quorum · status.requests.total</Src>} right={<span className="muted sm">since each process started</span>}>
          <div className="cx-tw">
            <table className="cx-t compact">
              <thead>
                <tr>
                  <th>Member</th>
                  <th className="r">A</th>
                  <th className="r">B</th>
                  <th className="r">Free</th>
                  <th className="r">Log on disk</th>
                </tr>
              </thead>
              <tbody>
                {rows.map((m) => {
                  const c = mt.perMember.find((x) => x.id === m.id)?.c
                  return (
                    <tr key={m.id} className={m.stale ? 'dim' : undefined}>
                      <td>
                        <NodeTag view={view} id={m.id} />
                      </td>
                      <td className="r mono sm">{c ? fmtNum(c.a) : '—'}</td>
                      <td className="r mono sm">{c ? fmtNum(c.b) : '—'}</td>
                      <td className="r mono sm">{c ? fmtNum(c.free) : '—'}</td>
                      <td className="r mono sm">{m.s ? fmtBytes(m.s.disk?.disk_bytes ?? m.s.log_bytes) : '—'}</td>
                    </tr>
                  )
                })}
                {!rows.length && (
                  <tr>
                    <td colSpan={5}>
                      <Empty>No member answered.</Empty>
                    </td>
                  </tr>
                )}
              </tbody>
            </table>
          </div>
        </Panel>
      </div>
    </>
  )
}

function Retention({ ret }: { ret?: A.RetainReport }) {
  if (!ret)
    return (
      <Panel title="Retention" src={<Src>store · retention</Src>}>
        <Empty title="No retention pass yet">The leader writes retain/qlog after each pass. With --qlog-retain-hours 0 retention is off and segments are kept.</Empty>
      </Panel>
    )
  const p = ret.plan
  const a = ret.applied
  const rows: [ReactNode, ReactNode][] = [
    ['Horizon', dur(p.horizon_secs * 1000)],
    ['Segments', <span className="mono">{fmtNum(p.segments)} · {fmtBytes(p.segment_bytes)}</span>],
    ['Past the horizon', <span className="mono">{fmtNum(p.deletable.length)} · {fmtBytes(p.deletable_bytes)}</span>],
    ['Pruned seq', <span className="mono">{seqS(ret.pruned_seq)}{p.pruned_seq_after > ret.pruned_seq ? ` → ${seqS(p.pruned_seq_after)}` : ''}</span>],
    ['Last deleted', a ? <span className="mono">{fmtNum(a.segments)} segments · {fmtBytes(a.segment_bytes)}{a.state_paths.length ? ` · ${a.state_paths.length} state paths (${fmtBytes(a.state_bytes)})` : ''}</span> : 'nothing yet'],
    ['Stale segments', p.stale_segments.length ? <span className="s-warn">{fmtNum(p.stale_segments.length)} a deposed leader wrote below F</span> : '0'],
  ]
  if (a?.kept.length) rows.push(['Kept on a second look', <span className="sm">{a.kept.join(', ')}</span>])
  return (
    <Panel title="Retention" src={<Src>store · retention (retain/qlog)</Src>} right={<span className="muted sm">the leader's pass {ago(p.at_ms)}</span>}>
      <KV style={{ padding: '10px 12px', margin: 0 }} rows={rows} />
      {p.states.length > 0 && (
        <div className="cx-tw">
          <table className="cx-t compact">
            <thead>
              <tr>
                <th>State path</th>
                <th />
                <th className="r">Objects</th>
                <th className="r">Bytes</th>
              </tr>
            </thead>
            <tbody>
              {p.states.map((x) => (
                <tr key={x.path}>
                  <td className="mono sm">{x.path}</td>
                  <td>
                    {x.current ? <Chip k="ok">current</Chip> : x.referenced ? <Chip k="info">referenced</Chip> : x.deletable ? <Chip k="idle">deletable</Chip> : <Chip k="plain" glyph={false}>kept</Chip>}
                    {x.stale_checkpoints.length > 0 && <span className="muted sm"> · {x.stale_checkpoints.length} stale checkpoints</span>}
                  </td>
                  <td className="r mono sm">{fmtNum(x.objects)}</td>
                  <td className="r mono sm">{fmtBytes(x.bytes)}</td>
                </tr>
              ))}
            </tbody>
          </table>
        </div>
      )}
    </Panel>
  )
}
