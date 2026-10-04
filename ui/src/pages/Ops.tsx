import type { ReactNode } from 'react'
import { Live, Tile } from '../components/relay'
import { Empty, ErrorNotice, Loading, Notice, Panel, Status } from '../components/ui'
import { ApiError, enc, type ArchiveView, type PipelineView, type PlcView, type SeqView } from '../lib/api'
import { fmtBytes, fmtNum, fmtSi, fmtTime, relTime, short } from '../lib/format'
import { Link } from '../lib/router'
import { useApi } from '../lib/useApi'

const POLL = 3000
const dash = <span className="muted">—</span>
const fmtMs = (v: number) => (v >= 60_000 ? `${(v / 60_000).toFixed(1)} min` : v >= 1000 ? `${(v / 1000).toFixed(1)} s` : `${v.toFixed(0)} ms`)
const day = (ms: number) => (ms ? new Date(ms).toISOString().slice(0, 10) : '—')

/** Background machinery: archival, PLC export seeding, stream seq checkpoints, and the ack backlog. */
export function Ops() {
  const archive = useApi<ArchiveView>('ops/archive', undefined, POLL)
  const plc = useApi<PlcView>('ops/plc', undefined, POLL)
  const seq = useApi<SeqView>('ops/seq', undefined, POLL)
  const pipe = useApi<PipelineView>('ops/pipeline', undefined, POLL)
  return (
    <>
      <div className="console-head">
        <h1>Operations</h1>
        <Live at={pipe.at} error={pipe.error} every={POLL} />
      </div>
      <Section load={pipe} what="Pipeline numbers">
        {(v) => <Pipeline v={v} />}
      </Section>
      <Section load={seq} what="Seq checkpoints">
        {(v) => <Seqs v={v} />}
      </Section>
      <Section load={archive} what="Archival">
        {(v) => <Archive v={v} />}
      </Section>
      <Section load={plc} what="PLC export seeding">
        {(v) => <Plc v={v} />}
      </Section>
    </>
  )
}

function Section<T>({ load, what, children }: { load: { data?: T; error?: unknown }; what: string; children: (v: T) => ReactNode }) {
  if (load.data) return <>{children(load.data)}</>
  if (load.error instanceof ApiError && load.error.status === 404) {
    return (
      <Panel title={what}>
        <p className="muted">{load.error.message}</p>
      </Panel>
    )
  }
  return load.error ? <ErrorNotice error={load.error} /> : <Loading />
}

function StaleCell({ stale, children }: { stale: boolean; children: ReactNode }) {
  return <>{stale ? dash : children}</>
}

function Pipeline({ v }: { v: PipelineView }) {
  const pending = v.nodes.reduce((a, n) => a + n.ackPending, 0)
  const oldest = v.nodes.reduce((a, n) => Math.max(a, n.oldestPendingMs), 0)
  const paused = v.nodes.reduce((a, n) => a + n.pausedHosts, 0)
  const dedupe = v.nodes.reduce((a, n) => a + n.dedupeEntries, 0)
  const gaugeNames = [...new Set(v.nodes.flatMap((n) => Object.keys(n.gauges)))].sort()
  const caps = v.hosts.some((h) => h.inflightCap !== null)
  return (
    <>
      <h2 className="ops-sub">Ack backlog and dedupe</h2>
      <div className="tiles">
        <Tile k="Events in flight" v={fmtNum(pending)} sub="read upstream, not yet durable, rejected or skipped" tone={oldest > 5000 ? 'warn' : undefined} />
        <Tile k="Oldest in flight" v={fmtMs(oldest)} tone={oldest > 30_000 ? 'bad' : oldest > 5000 ? 'warn' : undefined} sub="a host's cursor waits behind it" />
        <Tile k="Paused readers" v={fmtNum(paused)} tone={paused ? 'warn' : undefined} sub="over their rate, or the pipeline is full" />
        <Tile k="Dedupe entries" v={fmtNum(dedupe)} sub="held until host checkpoints pass them" />
      </div>
      <div className="grid2">
        <Panel flush title="Per core" desc="Each core's own pipeline. Gauges named for in-flight work, queues and caps show up here as the relay gains them.">
          <table className="data compact">
            <thead>
              <tr>
                <th>Node</th>
                <th className="num">In flight</th>
                <th className="num">Oldest</th>
                <th className="num">Lane queue</th>
                <th className="num">Dedupe</th>
                <th className="num">Paused hosts</th>
              </tr>
            </thead>
            <tbody>
              {v.nodes.map((n) => (
                <tr key={n.node} className={n.stale ? 'stale' : ''}>
                  <td>
                    <b>{n.node}</b> {n.stale && <Status kind="bad">stale</Status>}
                  </td>
                  <td className="num">
                    <StaleCell stale={n.stale}>{fmtNum(n.ackPending)}</StaleCell>
                  </td>
                  <td className="num">
                    <StaleCell stale={n.stale}>{fmtMs(n.oldestPendingMs)}</StaleCell>
                  </td>
                  <td className="num">
                    <StaleCell stale={n.stale}>{fmtNum(n.laneQueued)}</StaleCell>
                  </td>
                  <td className="num">
                    <StaleCell stale={n.stale}>{fmtNum(n.dedupeEntries)}</StaleCell>
                  </td>
                  <td className="num">
                    <StaleCell stale={n.stale}>{fmtNum(n.pausedHosts)}</StaleCell>
                  </td>
                </tr>
              ))}
            </tbody>
          </table>
          {gaugeNames.length > 0 && (
            <details style={{ padding: '8px 14px' }}>
              <summary className="small">All {gaugeNames.length} pipeline gauges</summary>
              <div className="gauges" style={{ marginTop: 8 }}>
                {gaugeNames.map((g) => (
                  <GaugeRow key={g} name={g} values={v.nodes.filter((n) => !n.stale).map((n) => [n.node, n.gauges[g]] as const)} />
                ))}
              </div>
            </details>
          )}
        </Panel>
        <Panel flush title="Hosts with work in flight" desc="Most in flight first, and every host whose reader is paused.">
          {v.hosts.length === 0 ? (
            <Empty title="Nothing in flight">Every host's events are durable, and no reader is paused.</Empty>
          ) : (
            <table className="data compact">
              <thead>
                <tr>
                  <th>Host</th>
                  <th>Node</th>
                  <th className="num">In flight</th>
                  {caps && <th className="num">Cap</th>}
                  <th className="num">Events/s</th>
                  <th>Reader</th>
                </tr>
              </thead>
              <tbody>
                {v.hosts.map((h) => (
                  <tr key={h.host}>
                    <td className="mono">
                      <Link to={`/admin/hosts/${enc(h.host)}`} className="plain">
                        {h.host}
                      </Link>
                    </td>
                    <td>{h.node}</td>
                    <td className="num">{fmtNum(h.inflight)}</td>
                    {caps && <td className="num">{h.inflightCap === null ? dash : fmtNum(h.inflightCap)}</td>}
                    <td className="num">{fmtSi(h.eventsPerSec)}</td>
                    <td>{h.paused ? <span className="pill amber">paused</span> : <span className="muted">reading</span>}</td>
                  </tr>
                ))}
              </tbody>
            </table>
          )}
        </Panel>
      </div>
    </>
  )
}

function GaugeRow({ name, values }: { name: string; values: (readonly [string, number | undefined])[] }) {
  const total = values.reduce((a, [, x]) => a + (x ?? 0), 0)
  return (
    <>
      <span title={values.map(([n, x]) => `${n}: ${x ?? '—'}`).join('\n')}>{name}</span>
      <span className="num">{fmtNum(total)}</span>
    </>
  )
}

function Seqs({ v }: { v: SeqView }) {
  const nodes = v.nodes.map((n) => n.node)
  const latest = v.boundaries[0]
  return (
    <>
      <h2 className="ops-sub">Stream seq checkpoints</h2>
      {!v.agree && (
        <Notice kind="err">
          Nodes counted the stream differently at a checkpoint (see the red rows). Consumers failing over between them would resume at the wrong event. docs/seq.md explains the numbering.
        </Notice>
      )}
      <div className="tiles">
        <Tile k="Agreement" v={v.agree ? 'all agree' : 'disagree'} tone={v.agree ? 'ok' : 'bad'} sub={`${v.boundaries.length} recent boundaries checked`} />
        <Tile k="Latest checkpoint" v={latest ? <span className="mono">{fmtNum(Object.values(latest.seqs)[0] ?? 0)}</span> : '—'} sub={latest ? `at ${fmtTime(latest.timeMs)} (${relTime(latest.timeMs)})` : 'none yet'} />
        <Tile k="Stream heads" v={<span className="small mono">{v.nodes.filter((n) => !n.stale).map((n) => `${n.node} ${fmtNum(n.head)}`).join(' · ') || '—'}</span>} />
      </div>
      <Panel flush title="Recent boundaries" desc="Each node numbers the merged stream on its own; at every 10 s boundary they must all count the same seq. A blank cell is a boundary that node hasn't reached or listed.">
        <div className="table-wrap">
          <table className="data compact">
            <thead>
              <tr>
                <th>Boundary</th>
                {nodes.map((n) => (
                  <th key={n} className="num">
                    {n}
                  </th>
                ))}
                <th />
              </tr>
            </thead>
            <tbody>
              {v.boundaries.map((b) => (
                <tr key={b.key} className={b.agree ? '' : 'stale'}>
                  <td className="mono" title={String(b.key)}>
                    {fmtTime(b.timeMs)}
                  </td>
                  {nodes.map((n) => (
                    <td key={n} className="num mono">
                      {b.seqs[n] !== undefined ? fmtNum(b.seqs[n]) : dash}
                    </td>
                  ))}
                  <td>{b.agree ? <Status kind="ok">agree</Status> : <Status kind="bad">disagree</Status>}</td>
                </tr>
              ))}
              {v.nodes.some((n) => n.stale) && (
                <tr>
                  <td className="muted small" colSpan={nodes.length + 2}>
                    Stale: {v.nodes.filter((n) => n.stale).map((n) => n.node).join(', ')}
                  </td>
                </tr>
              )}
            </tbody>
          </table>
        </div>
      </Panel>
    </>
  )
}

function Archive({ v }: { v: ArchiveView }) {
  const t = v.totals
  const off = v.mode === 'off'
  return (
    <>
      <h2 className="ops-sub">Archival</h2>
      <div className="tiles">
        <Tile k="Mode" v={v.mode} sub={`policy v${v.policyVersion}`} tone={off ? undefined : 'ok'} />
        <Tile k="Mirrored repos" v={fmtNum(t.mirrored)} sub={t.sweptAtMs ? `as of the sweep ${relTime(t.sweptAtMs)}` : 'not swept yet'} />
        <Tile k="Fetch queue" v={<>{fmtNum(t.queued + t.running)}<small>{fmtNum(t.running)} running</small></>} tone={t.queued > 1000 ? 'warn' : undefined} sub="repos waiting for a full copy" />
        <Tile k="Failed" v={fmtNum(t.failed)} tone={t.failed ? 'warn' : undefined} sub={`gave up after retries; ${fmtNum(t.retried)} retried`} />
        <Tile k="Fetched" v={fmtBytes(t.bytes)} sub={`${fmtNum(t.fetched)} repos, ${fmtNum(t.records)} records since start`} />
        <Tile k="Mismatches" v={fmtNum(t.mismatches)} tone={t.mismatches ? 'warn' : undefined} sub={`${fmtNum(t.healed)} healed by a fetch`} />
      </div>
      <div className="grid2">
        <Panel flush title="Per core" desc="Each core mirrors the accounts of its own DID shards.">
          <table className="data compact">
            <thead>
              <tr>
                <th>Node</th>
                <th className="num">Mirrored</th>
                <th className="num">Queued</th>
                <th className="num">Running</th>
                <th className="num">Failed</th>
                <th className="num">Applied</th>
                <th className="num">SST bytes</th>
              </tr>
            </thead>
            <tbody>
              {v.nodes.map((n) => (
                <tr key={n.node} className={n.stale ? 'stale' : ''}>
                  <td>
                    <b>{n.node}</b> {n.stale && <Status kind="bad">stale</Status>}
                  </td>
                  <td className="num">
                    <StaleCell stale={n.stale}>{fmtNum(n.counts.mirrored)}</StaleCell>
                  </td>
                  <td className="num">
                    <StaleCell stale={n.stale}>{fmtNum(n.counts.queued)}</StaleCell>
                  </td>
                  <td className="num">
                    <StaleCell stale={n.stale}>{fmtNum(n.counts.running)}</StaleCell>
                  </td>
                  <td className="num">
                    <StaleCell stale={n.stale}>{fmtNum(n.counts.failed)}</StaleCell>
                  </td>
                  <td className="num">
                    <StaleCell stale={n.stale}>{fmtNum(n.counts.applied)}</StaleCell>
                  </td>
                  <td className="num">
                    <StaleCell stale={n.stale}>{fmtBytes(n.counts.sstBytes)}</StaleCell>
                  </td>
                </tr>
              ))}
            </tbody>
          </table>
        </Panel>
        <Panel flush title="Recent fetch failures" desc="The newest failures on each core. An account's page shows its own.">
          {v.errors.length === 0 ? (
            <Empty title="No failures">{off ? 'Archival is off.' : 'Every fetch so far worked.'}</Empty>
          ) : (
            <table className="data compact">
              <thead>
                <tr>
                  <th>Account</th>
                  <th>Node</th>
                  <th>Error</th>
                </tr>
              </thead>
              <tbody>
                {v.errors
                  .slice()
                  .reverse()
                  .slice(0, 20)
                  .map((e, i) => (
                    <tr key={i}>
                      <td className="mono">
                        <Link to={`/admin/accounts/${enc(e.did)}`} className="plain">
                          {short(e.did, 14)}
                        </Link>
                      </td>
                      <td>{e.node}</td>
                      <td className="small err-mid" title={e.error} style={{ overflowWrap: 'anywhere' }}>
                        {e.error.length > 160 ? `${e.error.slice(0, 160)}…` : e.error}
                      </td>
                    </tr>
                  ))}
              </tbody>
            </table>
          )}
        </Panel>
      </div>
    </>
  )
}

function Plc({ v }: { v: PlcView }) {
  if (!v.enabled)
    return (
      <>
        <h2 className="ops-sub">PLC export seeding</h2>
        <Panel>
          <p className="muted">No node runs with --plc-export, so DID documents are resolved one account at a time.</p>
        </Panel>
      </>
    )
  const done = v.windows.filter((w) => w.done).length
  return (
    <>
      <h2 className="ops-sub">PLC export seeding</h2>
      <div className="tiles">
        <Tile k="Reader" v={v.leader ?? 'none'} tone={v.leader ? undefined : 'bad'} sub="the lowest-named live core reads the export" />
        <Tile k="Ops read" v={fmtNum(v.ops)} sub={`${fmtSi(v.opsPerSec)}/s, ${fmtNum(v.written)} written`} />
        <Tile k="State" v={v.caughtUp ? 'caught up' : 'backfilling'} tone={v.caughtUp ? 'ok' : undefined} sub={v.newestMs ? `newest op ${fmtTime(v.newestMs)}` : undefined} />
        <Tile k="Throttled" v={fmtNum(v.throttled)} tone={v.throttled ? 'warn' : undefined} sub={`${fmtNum(v.requests)} requests, ${fmtNum(v.errors)} errors, ${fmtNum(v.restarts)} restarts`} />
      </div>
      <div className="grid2">
        <Panel flush title="Windows" desc={`History split into windows read side by side. ${done} of ${v.windows.length} done. From the stored checkpoint${v.checkpointMs ? `, ${relTime(v.checkpointMs)}` : ''}.`}>
          {v.windows.length === 0 ? (
            <Empty title="No checkpoint yet">The reader writes one every 10 s.</Empty>
          ) : (
            <table className="data compact">
              <thead>
                <tr>
                  <th>From</th>
                  <th>Until</th>
                  <th>Read to</th>
                  <th className="num">Ops</th>
                  <th style={{ width: 160 }}>Progress</th>
                </tr>
              </thead>
              <tbody>
                {v.windows.map((w, i) => (
                  <tr key={i}>
                    <td className="mono">{day(w.fromMs)}</td>
                    <td className="mono">{w.untilMs ? day(w.untilMs) : 'the tail'}</td>
                    <td className="mono">{fmtTime(w.afterMs)}</td>
                    <td className="num">{fmtNum(w.ops)}</td>
                    <td>
                      <div className={`progress${w.done ? ' done' : ''}`} title={`${(w.progress * 100).toFixed(1)}%`}>
                        <i style={{ width: `${(w.progress * 100).toFixed(1)}%` }} />
                      </div>
                    </td>
                  </tr>
                ))}
              </tbody>
            </table>
          )}
        </Panel>
        <Panel flush title="Per core" desc="Counters start over with each reader; the totals above are the current reader's.">
          <table className="data compact">
            <thead>
              <tr>
                <th>Node</th>
                <th>Role</th>
                <th className="num">Ops</th>
                <th className="num">Ops/s</th>
                <th className="num">Throttled</th>
                <th className="num">Errors</th>
              </tr>
            </thead>
            <tbody>
              {v.nodes.map((n) => (
                <tr key={n.node} className={n.stale ? 'stale' : ''}>
                  <td>
                    <b>{n.node}</b> {n.stale && <Status kind="bad">stale</Status>}
                  </td>
                  <td>{n.leader ? <span className="pill accent">reader</span> : <span className="muted">standby</span>}</td>
                  <td className="num">
                    <StaleCell stale={n.stale}>{fmtNum(n.ops)}</StaleCell>
                  </td>
                  <td className="num">
                    <StaleCell stale={n.stale}>{fmtSi(n.opsPerSec)}</StaleCell>
                  </td>
                  <td className="num">
                    <StaleCell stale={n.stale}>{fmtNum(n.throttled)}</StaleCell>
                  </td>
                  <td className="num">
                    <StaleCell stale={n.stale}>{fmtNum(n.errors)}</StaleCell>
                  </td>
                </tr>
              ))}
            </tbody>
          </table>
        </Panel>
      </div>
    </>
  )
}
