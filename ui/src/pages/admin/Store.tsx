import type { ReactNode } from 'react'
import { Chip, Empty, KV, Loaded, PageHead, Panel, Spark, Src, Tiles, Updated, type TileSpec } from '../../components/console/kit'
import type { QCounts, QuorumView, SettingsView, StoreLatency } from '../../lib/api'
import * as A from '../../lib/console/adminAdapter'
import { ago, dur, fmtBytes, fmtMs, fmtNum, fmtSi, plural, seqS } from '../../lib/console/fmt'
import { requestRates, seriesOf, useQuorum, useSettings, useStore, type ReqRates } from '../../lib/console/queries'
import { useRelay } from '../../lib/console/relay'
import './logPages.css'
import { memberRows } from './quorumUi'
import { NodeTag } from './relayUi'

// The bucket the quorum log writes. GET store is the answering node's: its requests by purpose
// and R2 class with their rates and bytes, request latency by op, and the leader's last retention
// pass. The members' statuses add every member's requests, by member and by key component.
// Counts and rates only: no prices.

const PURPOSE: Record<string, { label: string; what: string }> = {
  flush: { label: 'Flush', what: "the leader's fence, segments, manifest CAS and checkpoint deletes" },
  state: { label: 'State DB', what: "the leader's SlateDB: memtable uploads, checkpoints, manifest polls, compactor and GC" },
  leader: { label: 'Leadership', what: 'qlog/leader reads and CASes: takeovers and membership changes' },
  recovery: { label: 'Recovery', what: "a bucket recovery: the manifest, orphans, the state's clone, salvaged segments" },
  backfill: { label: 'Backfill', what: "segments for a follower behind the leader's disk, a recovery's catch-up, consumers' old cursors" },
  retain: { label: 'Retention', what: 'the retention report and its deletes' },
  plc: { label: 'PLC export', what: "the export reader's DID document seeds and its checkpoint" },
  discovery: { label: 'Discovery', what: "host discovery's state: each source's cursor and runs" },
  tool: { label: 'Tools', what: 'qlog verify and qlog check' },
}
const purposeLabel = (p: string) => PURPOSE[p]?.label ?? p

// vlpds's objstats::component names, as the keys they count
const COMPONENT: Record<string, string> = {
  log_segment: 'log segments',
  qlog_manifest: 'log manifest',
  qlog_leader: 'leader record',
  retention_report: 'retention report',
  state_manifest: 'state DB manifest',
  state_sst: 'state DB SSTs',
  state_wal: 'state DB WAL',
  state_compactions: 'state DB compactions',
  state_gc_boundary: 'state DB GC boundary',
  state_other: 'state DB, other files',
}
const COMPONENT_WHAT: Record<string, string> = {
  log_segment: "the log's segment objects",
  qlog_manifest: 'qlog/manifest: what is flushed, CASed on every flush',
  qlog_leader: 'qlog/leader: who leads, CASed on a takeover',
  retention_report: "retain/qlog: the leader's last retention pass",
  state_manifest: "SlateDB's manifest, polled and CASed",
  state_sst: "SlateDB's sorted tables, from memtable flushes and compactions",
  state_wal: "SlateDB's write-ahead log",
  state_compactions: "SlateDB's compaction state",
  state_gc_boundary: "SlateDB's GC boundary files, read with every manifest or compaction read",
  state_other: "SlateDB's other files",
}
// keys the component list has no name for, by the purpose that sends them
const OTHER: Record<string, string> = { plc: 'DID document seeds, export checkpoint', discovery: 'discovery state' }
const OTHER_WHAT: Record<string, string> = { plc: 'plc/seeds and plc/export-checkpoint.json', discovery: 'discovery/state.json' }
const componentWhat = (purpose: string, comp: string) => (comp === 'other' ? (OTHER_WHAT[purpose] ?? 'keys outside the named components') : (COMPONENT_WHAT[comp] ?? ''))
const componentLabel = (purpose: string, comp: string) => (comp === 'other' ? (OTHER[purpose] ?? 'other keys') : (COMPONENT[comp] ?? comp.replace(/_/g, ' ')))

const CLASS_A = 'Class A: every write and LIST (R2 bills them as the dearer class)'
const CLASS_B = 'Class B: every GET and HEAD'
const FREE = 'Free: single DELETEs and aborted multipart uploads, which R2 doesn’t bill'

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

/** A purpose or component: its readable name over the raw key in mono. */
function Named({ label, raw, title, indent }: { label: ReactNode; raw: string; title?: string; indent?: boolean }) {
  return (
    <span className={`cx-named${indent ? ' in' : ''}`} title={title}>
      <span>{label}</span>
      <span className="raw">{raw}</span>
    </span>
  )
}

/** Column headers for R2's request classes, each saying what it counts. */
const ClassTh = ({ k, children }: { k: 'a' | 'b' | 'free'; children: ReactNode }) => (
  <th className="r" title={k === 'a' ? CLASS_A : k === 'b' ? CLASS_B : FREE}>
    {children}
  </th>
)

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
  const mt = qv ? memberTotals(qv) : { byComponent: {} as Record<string, QCounts>, perMember: [] as { id: string; c: QCounts }[] }
  const rr = requestRates()
  const ret = A.retentionOf(v)
  const win = v.windowSecs > 0 ? `last ${dur(v.windowSecs * 1000)}` : 'filling in'
  const tot = v.total

  // what the current rate comes to over a month: a count, not a price
  const month = (perSec: number) => <div className="cx-tsub">{v.windowSecs > 0 ? `≈ ${fmtSi(perSec * MONTH_SECS)} per 30 days at this rate` : win}</div>
  const monthTitle = `${v.node}'s rate over the ${win}, kept up for 30 days`
  const tiles: TileSpec[] = [
    { label: 'Class A', right: 'writes, lists', value: v.windowSecs > 0 ? rate(tot.perSec.a) : '—', unit: '/s', sec: win, title: `${CLASS_A}. ${monthTitle}`, spark: (
        <>
          <Spark data={seriesOf('store-a')} color="c3" />
          {month(tot.perSec.a)}
        </>
      ) },
    { label: 'Class B', right: 'reads', value: v.windowSecs > 0 ? rate(tot.perSec.b) : '—', unit: '/s', sec: win, title: `${CLASS_B}. ${monthTitle}`, spark: (
        <>
          <Spark data={seriesOf('store-b')} color="c5" />
          {month(tot.perSec.b)}
        </>
      ) },
    { label: 'Since start', right: v.node, value: cnt(tot.requests.a), unit: 'class A', spark: <div className="cx-tsub">{`${cnt(tot.requests.b)} class B · ${cnt(tot.requests.free)} free`}</div>, title: `Requests since ${v.node} started. ${CLASS_A}. ${CLASS_B}. ${FREE}.` },
    { label: 'Payload', right: v.node, value: fmtBytes(tot.bytesUp), unit: 'up', spark: <div className="cx-tsub">{fmtBytes(tot.bytesDown)} down</div>, title: `Request and response bodies since ${v.node} started` },
    { label: 'Log segments', right: ret ? `as of ${ago(ret.plan.at_ms)}` : undefined, value: ret ? fmtBytes(ret.plan.segment_bytes) : '—', spark: <div className="cx-tsub">{ret ? plural(ret.plan.segments, 'segment') : 'no retention pass yet'}</div> },
    { label: 'Pruned to', right: 'seq', value: ret ? seqS(ret.pruned_seq) : '—', title: 'Cursors older than this get OutdatedCursor', spark: <div className="cx-tsub">{ret?.applied ? `last pass deleted ${plural(ret.applied.segments, 'segment')}` : 'older cursors get OutdatedCursor'}</div> },
  ]

  const purposes = [...v.purposes].sort((a, b) => b.requests.a + b.requests.b - (a.requests.a + a.requests.b))
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
            {v.node} · rates over the {win}
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
                <ClassTh k="a">Class A/s</ClassTh>
                <ClassTh k="b">Class B/s</ClassTh>
                <th className="r" title="Class A and B requests in 30 days at this rate: a count, not a price">
                  30 d at this rate
                </th>
                <ClassTh k="a">Class A since start</ClassTh>
                <ClassTh k="b">Class B since start</ClassTh>
                <th className="r">Up</th>
                <th className="r">Down</th>
                <th>What sends them</th>
              </tr>
            </thead>
            <tbody>
              {purposes.map((p) => (
                <tr key={p.purpose}>
                  <td>
                    <Named label={purposeLabel(p.purpose)} raw={p.purpose} />
                  </td>
                  <td className="r mono sm">{rate(p.perSec.a)}</td>
                  <td className="r mono sm">{rate(p.perSec.b)}</td>
                  <td className="r mono sm t2">{v.windowSecs > 0 ? fmtSi((p.perSec.a + p.perSec.b) * MONTH_SECS) : '—'}</td>
                  <td className="r mono sm">{fmtNum(p.requests.a)}</td>
                  <td className="r mono sm">{fmtNum(p.requests.b)}</td>
                  <td className="r mono sm">{fmtBytes(p.bytesUp)}</td>
                  <td className="r mono sm">{fmtBytes(p.bytesDown)}</td>
                  <td className="sm t2" style={{ whiteSpace: 'normal', minWidth: 200 }}>
                    {PURPOSE[p.purpose]?.what ?? ''}
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
        <Latency node={v.node} rows={latency} />
        <Retention ret={ret} />
      </div>
      <Members qv={qv} mt={mt} rr={rr} />
    </>
  )
}

// ---------------------------------------------------------------- latency

// vlpds's request histogram: buckets at 0.1 ms × 2^k, k = 0..19
const isBound = (ms: number) => {
  const k = Math.log2(ms / 0.1)
  return k >= 0 && k <= 19 && Math.abs(k - Math.round(k)) < 1e-6
}

/** A quantile the relay reads off the histogram: the upper bound of its bucket, so "≤ 410 ms". */
function Quantile({ ms, q, warn }: { ms: number | null; q: string; warn?: boolean }) {
  if (ms === null || !isFinite(ms)) return <td className="r mono sm s-warn" title={`${q} is past the histogram's last bucket (52 s)`}>&gt; 52 s</td>
  const title = isBound(ms) ? `${q} falls in the histogram bucket ${fmtMs(ms / 2)} – ${fmtMs(ms)}: the relay reports the bucket's upper bound` : `${q}: the upper bound of the histogram bucket it falls in`
  return (
    <td className={`r mono sm${warn ? ' s-warn' : ''}`} title={title}>
      <span className="muted">≤</span> {fmtMs(ms)}
    </td>
  )
}

function Latency({ node, rows }: { node: string; rows: StoreLatency[] }) {
  return (
    <Panel
      title="Latency by operation"
      src={<Src>store · latency</Src>}
      right={<span className="muted sm">{node} · since it started</span>}
      foot={<span>Mean is exact. p50 and p99 come from a histogram whose buckets double, so each is the bucket's upper bound: the request took at most that, and more than half of it.</span>}
    >
      <div className="cx-tw">
        <table className="cx-t compact">
          <thead>
            <tr>
              <th>Op</th>
              <th className="r" title={`Requests timed since ${node} started (answered ones; deletes aren't timed)`}>
                Timed
              </th>
              <th className="r">Mean</th>
              <th className="r" title="The histogram bucket's upper bound">
                p50
              </th>
              <th className="r" title="The histogram bucket's upper bound">
                p99
              </th>
            </tr>
          </thead>
          <tbody>
            {rows.map((l) => (
              <tr key={l.op}>
                <td className="mono sm">{l.op}</td>
                <td className="r mono sm">{fmtNum(l.count)}</td>
                <td className="r mono sm">{fmtMs(l.meanMs)}</td>
                <Quantile ms={l.p50Ms} q="p50" />
                <Quantile ms={l.p99Ms} q="p99" warn={l.p99Ms > 1000} />
              </tr>
            ))}
            {!rows.length && (
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
  )
}

// ---------------------------------------------------------------- every member

/** A row's share of every member's requests: one bar split into class A and B, or just the figure when it's small. */
function Share({ a, b, all }: { a: number; b: number; all: number }) {
  if (!(all > 0)) return <span className="muted">—</span>
  const s = (a + b) / all
  const pct = s >= 0.995 ? '100%' : s >= 0.095 ? `${Math.round(s * 100)}%` : s >= 0.001 ? `${(s * 100).toFixed(1)}%` : s > 0 ? '<0.1%' : '0'
  return (
    <span className="cx-share" title={`${(s * 100).toFixed(2)}% of every member's class A and B requests: ${((a / all) * 100).toFixed(2)}% class A, ${((b / all) * 100).toFixed(2)}% class B`}>
      {s >= 0.02 ? (
        <span className="bar" aria-hidden="true">
          <i className="a" style={{ width: `${((a / all) * 100).toFixed(1)}%` }} />
          <i className="b" style={{ width: `${((b / all) * 100).toFixed(1)}%` }} />
        </span>
      ) : (
        <span className="bar none" aria-hidden="true" />
      )}
      <span className={s < 0.02 ? 'muted' : undefined}>{pct}</span>
    </span>
  )
}

type CompRow = { key: string; comp: string; total: QCounts; rate?: QCounts }

function Members({ qv, mt, rr }: { qv?: QuorumView; mt: ReturnType<typeof memberTotals>; rr?: ReqRates }) {
  const { view } = useRelay()
  const rows = qv ? memberRows(qv, view) : []
  const hasRates = !!rr?.windowSecs
  const win = hasRates ? `last ${dur(rr!.windowSecs * 1000)}` : 'rates filling in'
  // weigh by the window's rates when there are any, else by the totals
  const w = (r: CompRow | { total: QCounts; rate?: QCounts }) => (hasRates ? (r.rate ? r.rate.a + r.rate.b : 0) : r.total.a + r.total.b)

  const groups = new Map<string, { purpose: string; total: QCounts; rate: QCounts; rows: CompRow[] }>()
  for (const [key, total] of Object.entries(mt.byComponent)) {
    const i = key.indexOf('/')
    const purpose = i < 0 ? key : key.slice(0, i)
    const comp = i < 0 ? '' : key.slice(i + 1)
    const g = groups.get(purpose) ?? { purpose, total: zero(), rate: zero(), rows: [] }
    groups.set(purpose, g)
    const r = rr?.byComponent[key]
    g.rows.push({ key, comp, total, rate: r })
    add(g.total, total)
    add(g.rate, r)
  }
  const gs = [...groups.values()].sort((a, b) => w(b) - w(a) || b.total.a + b.total.b - (a.total.a + a.total.b))
  for (const g of gs) g.rows.sort((a, b) => w(b) - w(a) || b.total.a + b.total.b - (a.total.a + a.total.b))
  const allRate = rr ? rr.total.a + rr.total.b : 0
  const allTotal = gs.reduce((s, g) => s + g.total.a + g.total.b, 0)
  const allMembers = mt.perMember.reduce((s, m) => s + m.c.a + m.c.b, 0)
  const shareOf = (r: { total: QCounts; rate?: QCounts }) =>
    hasRates ? <Share a={r.rate?.a ?? 0} b={r.rate?.b ?? 0} all={allRate} /> : <Share a={r.total.a} b={r.total.b} all={allTotal} />

  return (
    <Panel
      className="cx-mt"
      title="Every member's requests"
      src={
        <>
          <Src>cluster/quorum · status.requests</Src>
        </>
      }
      right={<span className="muted sm">rates over the {win} · totals since each process started</span>}
      foot={
        <span>
          Summed over the members that answer. The rates are the console's own, from the statuses it polls while this page is open; the totals restart with each process. Share is of the{' '}
          {hasRates ? 'rates' : 'totals'}, split into class A (<span className="cx-sw a" /> writes, lists) and class B (<span className="cx-sw b" /> reads).
        </span>
      }
    >
      <div className="cx-tw">
        <table className="cx-t compact cx-members">
          <thead>
            <tr>
              <th>Member</th>
              <ClassTh k="a">Class A/s</ClassTh>
              <ClassTh k="b">Class B/s</ClassTh>
              <ClassTh k="a">Class A since start</ClassTh>
              <ClassTh k="b">Class B since start</ClassTh>
              <ClassTh k="free">Free (deletes)</ClassTh>
              <th className="r" title="The member's log on its own disk">
                Log on disk
              </th>
              <th className="fill" title={hasRates ? 'Share of every member’s class A and B requests per second' : 'Share of every member’s class A and B requests since start'}>
                Share
              </th>
            </tr>
          </thead>
          <tbody>
            {rows.map((m) => {
              const c = mt.perMember.find((x) => x.id === m.id)?.c
              const r = rr?.byNode[m.id]
              return (
                <tr key={m.id} className={m.stale ? 'dim' : undefined}>
                  <td>
                    <NodeTag view={view} id={m.id} />
                  </td>
                  <td className="r mono sm">{hasRates ? rate(r?.a) : '—'}</td>
                  <td className="r mono sm">{hasRates ? rate(r?.b) : '—'}</td>
                  <td className="r mono sm">{c ? fmtNum(c.a) : '—'}</td>
                  <td className="r mono sm">{c ? fmtNum(c.b) : '—'}</td>
                  <td className="r mono sm">{c ? fmtNum(c.free) : '—'}</td>
                  <td className="r mono sm">{m.s ? fmtBytes(m.s.disk?.disk_bytes ?? m.s.log_bytes) : '—'}</td>
                  <td className="fill">{hasRates ? <Share a={r?.a ?? 0} b={r?.b ?? 0} all={allRate} /> : c ? <Share a={c.a} b={c.b} all={allMembers} /> : null}</td>
                </tr>
              )
            })}
            {!rows.length && (
              <tr>
                <td colSpan={8}>
                  <Empty>No member answered.</Empty>
                </td>
              </tr>
            )}
          </tbody>
        </table>
      </div>
      <div className="cx-tw cx-sep">
        <table className="cx-t compact cx-comps">
          <thead>
            <tr>
              <th>Purpose and key</th>
              <ClassTh k="a">Class A/s</ClassTh>
              <ClassTh k="b">Class B/s</ClassTh>
              <th className="r" title="Class A and B requests since each member's process started">
                A + B since start
              </th>
              <th title={hasRates ? 'Share of every member’s class A and B requests per second' : 'Share of every member’s class A and B requests since start'}>Share</th>
              <th className="fill">What they are</th>
            </tr>
          </thead>
          {gs.map((g) => (
            <tbody key={g.purpose}>
              <tr className="grp">
                <td>
                  <Named label={purposeLabel(g.purpose)} raw={g.purpose} />
                </td>
                <td className="r mono sm">{hasRates ? rate(g.rate.a) : '—'}</td>
                <td className="r mono sm">{hasRates ? rate(g.rate.b) : '—'}</td>
                <td className="r mono sm" title={`${fmtNum(g.total.a)} class A · ${fmtNum(g.total.b)} class B · ${fmtNum(g.total.free)} free`}>
                  {fmtNum(g.total.a + g.total.b)}
                </td>
                <td>{shareOf(g)}</td>
                <td className="fill wrap sm t2">{PURPOSE[g.purpose]?.what ?? ''}</td>
              </tr>
              {g.rows.map((r) => (
                <tr key={r.key}>
                  <td>
                    <Named indent label={componentLabel(g.purpose, r.comp)} raw={r.key} />
                  </td>
                  <td className="r mono sm t2">{hasRates ? rate(r.rate?.a) : '—'}</td>
                  <td className="r mono sm t2">{hasRates ? rate(r.rate?.b) : '—'}</td>
                  <td className="r mono sm t2" title={`${fmtNum(r.total.a)} class A · ${fmtNum(r.total.b)} class B · ${fmtNum(r.total.free)} free`}>
                    {fmtNum(r.total.a + r.total.b)}
                  </td>
                  <td>{shareOf(r)}</td>
                  <td className="fill wrap sm muted">{componentWhat(g.purpose, r.comp)}</td>
                </tr>
              ))}
            </tbody>
          ))}
          {!gs.length && (
            <tbody>
              <tr>
                <td colSpan={6}>
                  <Empty>{qv ? 'No member reports its requests by key.' : 'No member answered.'}</Empty>
                </td>
              </tr>
            </tbody>
          )}
        </table>
      </div>
    </Panel>
  )
}

// ---------------------------------------------------------------- retention

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
    ['Last deleted', a ? <span className="mono">{plural(a.segments, 'segment')} · {fmtBytes(a.segment_bytes)}{a.state_paths.length ? ` · ${plural(a.state_paths.length, 'state path')} (${fmtBytes(a.state_bytes)})` : ''}</span> : 'nothing yet'],
    ['Stale segments', p.stale_segments.length ? <span className="s-warn">{fmtNum(p.stale_segments.length)} a deposed leader wrote below F</span> : '0'],
  ]
  if (a?.kept.length) rows.push(['Kept on a second look', <span className="sm">{a.kept.join(', ')}</span>])
  const stale = p.states.some((x) => x.stale_checkpoints.length > 0)
  return (
    <Panel title="Retention" src={<Src>store · retention (retain/qlog)</Src>} right={<span className="muted sm">the leader's pass {ago(p.at_ms)}</span>}>
      <KV style={{ padding: '10px 12px', margin: 0 }} rows={rows} />
      {p.states.length > 0 && (
        <div className="cx-tw">
          <table className="cx-t compact">
            <thead>
              <tr>
                <th>State path</th>
                <th>Status</th>
                {stale && (
                  <th className="r" title="Checkpoints no reader needs any more, which the next pass deletes">
                    Stale checkpoints
                  </th>
                )}
                <th className="r">Objects</th>
                <th className="r">Bytes</th>
              </tr>
            </thead>
            <tbody>
              {p.states.map((x) => (
                <tr key={x.path}>
                  <td className="mono sm">{x.path}</td>
                  <td>{x.current ? <Chip k="ok">current</Chip> : x.referenced ? <Chip k="info">referenced</Chip> : x.deletable ? <Chip k="idle">deletable</Chip> : <Chip k="plain" glyph={false}>kept</Chip>}</td>
                  {stale && <td className="r mono sm">{x.stale_checkpoints.length ? fmtNum(x.stale_checkpoints.length) : <span className="muted">0</span>}</td>}
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
