import type { ReactNode } from 'react'
import { Empty, KV, Loaded, Meter, NeedsVersion, PageHead, Panel, Spark, Src, Tiles, type TileSpec } from '../../components/console/kit'
import type { QCounts, QStatus, QuorumView, SettingsView } from '../../lib/api'
import { dur, fmtBytes, fmtNum, fmtSi, fmtUs } from '../../lib/console/fmt'
import { quorumPoll, requestRates, seriesOf, settingsPoll } from '../../lib/console/polls'
import { useRelay } from '../../lib/console/relay'
import './logPages.css'
import { memberRows } from './quorumUi'
import { NodeTag } from './relayUi'

// The bucket the quorum log writes: requests by purpose and R2 class (what sent each one, from
// every member's status), their rates over the last minute, the flush's timings, retention and
// what's stored. Counts and rates only: no prices.

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

/** An older status has the flush's request counts by operation only: classed here the way bucket.rs classes them. */
function classOf(op: string): keyof QCounts {
  const o = op.toLowerCase()
  if (o.startsWith('get') || o.startsWith('head')) return 'b'
  if (o.startsWith('delete') && !o.includes('objects')) return 'free'
  return 'a'
}

type Totals = { total: QCounts; byPurpose: Record<string, QCounts>; byComponent: Record<string, QCounts>; perMember: { id: string; c: QCounts }[]; exact: boolean }

function totals(q: QuorumView): Totals {
  const t: Totals = { total: zero(), byPurpose: {}, byComponent: {}, perMember: [], exact: false }
  for (const n of q.nodes) {
    const s = n.stale ? null : n.status
    if (!s) continue
    if (s.requests?.total) {
      t.exact = true
      add(t.total, s.requests.total)
      for (const [k, v] of Object.entries(s.requests.by_purpose ?? {})) add((t.byPurpose[k] ??= zero()), v)
      for (const [k, v] of Object.entries(s.requests.by_component ?? {})) add((t.byComponent[k] ??= zero()), v)
      t.perMember.push({ id: n.node, c: { ...s.requests.total } })
    } else if (s.flush?.requests_total) {
      const c = zero()
      for (const [op, v] of Object.entries(s.flush.requests_total)) c[classOf(op)] += v
      add(t.total, c)
      for (const [op, v] of Object.entries(s.flush.requests_total)) {
        const k = `flush/${op}`
        const x = (t.byComponent[k] ??= zero())
        x[classOf(op)] += v
      }
      add((t.byPurpose.flush ??= zero()), c)
      t.perMember.push({ id: n.node, c })
    }
  }
  return t
}

const setting = (s: SettingsView | undefined, flag: string) => s?.entries.find((e) => e.flag === flag)

const rate = (v: number | undefined) => (v === undefined ? '—' : v === 0 ? '0' : v < 0.01 ? '<0.01' : v < 10 ? v.toFixed(2) : fmtSi(v))

export function Store() {
  const qp = quorumPoll.use()
  const sp = settingsPoll.use()
  const { view } = useRelay()
  const s = sp.data
  const bucket = setting(s, '--s3-bucket')?.value
  const endpoint = setting(s, '--s3-endpoint')?.value
  const prefix = setting(s, '--prefix')?.value
  const sub = (
    <>
      {endpoint ? <span className="mono">{endpoint.replace(/^https?:\/\//, '')}</span> : <span>the relay's bucket</span>}
      {bucket && (
        <span className="mono">
          {bucket}
          {prefix ? `/${prefix}` : ''}
        </span>
      )}
    </>
  )
  const q = qp.data
  if (!q) return <><PageHead title="Object store" sub={sub} /><Loaded load={qp}>{() => null}</Loaded></>
  if (!q.supported)
    return (
      <>
        <PageHead title="Object store" sub={sub} />
        <Panel title="Bucket requests">
          <Empty title="This relay runs without the quorum log">
            The request counts by purpose come from the quorum log's statuses. A single relay's segments and state go through vlpds's store; their counts are in the metrics as{' '}
            <span className="mono">vlpds_object_store_requests_total</span>.
          </Empty>
        </Panel>
      </>
    )

  const qv = q.data
  const rows = memberRows(qv, view)
  const lead = rows.find((r) => r.kind === 'leader')?.s ?? undefined
  const t = totals(qv)
  const r = requestRates()
  const f = lead?.flush
  const relay = lead?.relay
  const retainSecs = setting(s, '--qlog-retain-secs')
  const retainH = retainSecs?.set && retainSecs.value ? `${dur(Number(retainSecs.value) * 1000)}` : setting(s, '--qlog-retain-hours')?.value === '0' ? '0' : setting(s, '--qlog-retain-hours')?.value ? `${setting(s, '--qlog-retain-hours')!.value} h` : undefined
  const flushMs = Number(setting(s, '--qlog-flush-ms')?.value ?? '') || undefined
  const win = r?.windowSecs ? `last ${dur(r.windowSecs * 1000)}` : 'filling in'

  const tiles: TileSpec[] = [
    { label: 'Class A', right: 'writes, lists', value: rate(r?.total.a), unit: '/s', sec: win, spark: <Spark data={seriesOf('req-a')} color="c3" /> },
    { label: 'Class B', right: 'reads', value: rate(r?.total.b), unit: '/s', sec: win, spark: <Spark data={seriesOf('req-b')} color="c5" /> },
    { label: 'Since start', right: 'every member', value: fmtSi(t.total.a), unit: 'A', sec: `${fmtSi(t.total.b)} B · ${fmtNum(t.total.free)} free` },
    {
      label: 'Segments stored',
      value: f ? fmtBytes(f.segment_bytes) : '—',
      sec: f ? `${fmtNum(f.segments)} segs` : 'only the leader flushes',
      right: f && f.raw_bytes ? `${(f.raw_bytes / Math.max(1, f.segment_bytes)).toFixed(1)}× compressed` : undefined,
      title: 'What this leader has flushed since it started',
    },
    { label: 'Flushes', value: f ? fmtNum(f.flushes) : '—', right: flushMs ? `every ${dur(flushMs)}` : undefined, sec: f ? `${fmtNum(f.failed)} failed · ${fmtNum(f.aborted)} aborted` : undefined, to: '/admin/quorum' },
    { label: 'Retention', value: relay?.retain_runs !== undefined ? fmtNum(relay.retain_runs) : '—', unit: 'runs', sec: relay?.retain_deleted !== undefined ? `${fmtNum(relay.retain_deleted)} objects deleted` : retainH === '0' ? 'off' : undefined },
  ]

  const purposes = [...new Set([...Object.keys(t.byPurpose), ...Object.keys(r?.byPurpose ?? {})])].sort((a, b) => {
    const x = t.byPurpose[a]
    const y = t.byPurpose[b]
    return (y ? y.a + y.b : 0) - (x ? x.a + x.b : 0)
  })
  const comps = Object.entries(t.byComponent).sort((a, b) => b[1].a + b[1].b - (a[1].a + a[1].b))
  const maxComp = comps.length ? comps[0][1].a + comps[0][1].b : 1

  return (
    <>
      <PageHead title="Object store" sub={<>{sub}{f && <span>{fmtBytes(f.segment_bytes)} of segments flushed by {lead?.id}</span>}</>} />
      <div className="cx-tilesbox">
        <Tiles tiles={tiles} />
      </div>
      <div className="cx-grid2">
        <Panel
          title="Requests by purpose"
          src={<Src>cluster/quorum · status.requests.by_purpose</Src>}
          right={<span className="muted sm">{win}</span>}
          foot={
            <span>
              Classes are R2's: A is every write and LIST, B every GET and HEAD, and a single DELETE is free. Rates sum the members that answer; totals are since each process started, failed requests
              included.
            </span>
          }
        >
          {!t.exact && <NeedsVersion what="Requests by purpose" endpoint="qlog status.requests">This build reports the flush's requests by operation only, below.</NeedsVersion>}
          <div className="cx-tw">
            <table className="cx-t compact">
              <thead>
                <tr>
                  <th>Purpose</th>
                  <th className="r">A/s</th>
                  <th className="r">B/s</th>
                  <th className="r">A total</th>
                  <th className="r">B total</th>
                  <th>What sends them</th>
                </tr>
              </thead>
              <tbody>
                {purposes.map((p) => (
                  <tr key={p}>
                    <td className="mono">{p}</td>
                    <td className="r mono sm">{rate(r?.byPurpose[p]?.a)}</td>
                    <td className="r mono sm">{rate(r?.byPurpose[p]?.b)}</td>
                    <td className="r mono sm">{fmtNum(t.byPurpose[p]?.a ?? 0)}</td>
                    <td className="r mono sm">{fmtNum(t.byPurpose[p]?.b ?? 0)}</td>
                    <td className="sm t2" style={{ whiteSpace: 'normal', minWidth: 180 }}>
                      {PURPOSE[p] ?? ''}
                    </td>
                  </tr>
                ))}
                {!purposes.length && (
                  <tr>
                    <td colSpan={6}>
                      <Empty>No bucket requests yet.</Empty>
                    </td>
                  </tr>
                )}
              </tbody>
            </table>
          </div>
        </Panel>
        <Panel title="By component" src={<Src>status.requests.by_component</Src>} right={<span className="muted sm">purpose / key prefix</span>}>
          <div className="cx-tw">
            <table className="cx-t compact">
              <thead>
                <tr>
                  <th>Component</th>
                  <th className="r">A/s</th>
                  <th className="r">B/s</th>
                  <th className="r">Total</th>
                  <th>Share</th>
                </tr>
              </thead>
              <tbody>
                {comps.slice(0, 16).map(([k, v]) => (
                  <tr key={k}>
                    <td className="mono sm">{k}</td>
                    <td className="r mono sm">{rate(r?.byComponent[k]?.a)}</td>
                    <td className="r mono sm">{rate(r?.byComponent[k]?.b)}</td>
                    <td className="r mono sm" title={`${fmtNum(v.a)} A · ${fmtNum(v.b)} B · ${fmtNum(v.free)} free`}>
                      {fmtNum(v.a + v.b)}
                    </td>
                    <td>
                      <Meter v={v.a + v.b} max={maxComp} />
                    </td>
                  </tr>
                ))}
                {!comps.length && (
                  <tr>
                    <td colSpan={5}>
                      <Empty>No bucket requests yet.</Empty>
                    </td>
                  </tr>
                )}
              </tbody>
            </table>
          </div>
        </Panel>
      </div>
      <div className="cx-grid2 cx-mt">
        <Panel title="Latency" src={<><Src>status.flush.duration_us, seal_us</Src> <Src isNew>status.requests.latency_us</Src></>}>
          {f ? (
            <KV
              style={{ padding: '10px 12px', margin: 0 }}
              rows={[
                ['Flush, seal to CAS', <span className="mono">p50 {fmtUs(f.duration_us.p50)} · p99 {fmtUs(f.duration_us.p99)} · max {fmtUs(f.duration_us.max)}</span>],
                ['Seal pause', <span className="mono">p50 {fmtUs(f.seal_us.p50)} · p99 {fmtUs(f.seal_us.p99)}</span>],
              ]}
            />
          ) : (
            <Empty>Only the leader flushes.</Empty>
          )}
          <NeedsVersion what="PUT and GET latency by purpose" endpoint="qlog status.requests.latency_us" />
        </Panel>
        <Panel title="Retention" src={<><Src>status.relay.retain_*</Src> <Src isNew>store/retention</Src></>}>
          <KV
            style={{ padding: '10px 12px', margin: 0 }}
            rows={[
              ['Horizon', retainH === undefined ? '—' : retainH === '0' ? 'off: segments are kept' : retainH],
              ['Runs', relay?.retain_runs !== undefined ? `${fmtNum(relay.retain_runs)} by ${lead?.id}` : '—'],
              ['Deleted', relay?.retain_deleted !== undefined ? `${fmtNum(relay.retain_deleted)} objects` : '—'],
            ]}
          />
          <NeedsVersion what="The last retention report (pruned seq, what's past the horizon)" endpoint="GET store/retention" />
        </Panel>
      </div>
      <div className="cx-grid2 cx-mt">
        <Panel title="Per member" src={<Src>status.requests.total · log_bytes · disk</Src>} right={<span className="muted sm">since each process started</span>}>
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
                  const c = t.perMember.find((x) => x.id === m.id)?.c
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
              </tbody>
            </table>
          </div>
        </Panel>
        <Panel title="What's stored" src={<><Src>status.flush</Src> <Src isNew>store/prefixes</Src></>}>
          <Stored f={lead} prefix={prefix} />
        </Panel>
      </div>
    </>
  )
}

function Stored({ f, prefix }: { f?: QStatus; prefix?: string | null }) {
  const fl = f?.flush
  const rows: [ReactNode, ReactNode][] = [
    ['Log segments', fl ? `${fmtNum(fl.segments)} · ${fmtBytes(fl.segment_bytes)}, flushed by this leader${prefix ? ` under ${prefix}/` : ''}` : '—'],
    ['Entries flushed', fl ? fmtNum(fl.entries) : '—'],
    ['State applied', fl ? fmtNum(fl.applied) : '—'],
  ]
  return (
    <>
      <KV style={{ padding: '10px 12px', margin: 0 }} rows={rows} />
      <NeedsVersion what="Objects and bytes under each prefix" endpoint="GET store/prefixes" />
    </>
  )
}
