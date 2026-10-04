import { useMemo } from 'react'
import { Chart, type Series } from '../components/Chart'
import { Bar, HOST_STATUSES, Live, REASON_COLOR, REASON_LABEL, StatusPill, Tile, TierPill } from '../components/relay'
import { ErrorNotice, Loading, Panel } from '../components/ui'
import type { HostStatus, Overview as O, RejectReason } from '../lib/api'
import { enc } from '../lib/api'
import { fmtBytes, fmtNum, fmtSi } from '../lib/format'
import { Link, navigate } from '../lib/router'
import { useApi } from '../lib/useApi'

const POLL = 2000

export const STATUS_COLOR: Record<HostStatus, string> = {
  connected: 'ok',
  idle: 'ink3',
  throttled: 'amber',
  backoff: 'c3',
  offline: 'rule',
  suspended: 'c5',
  banned: 'danger',
}

const fmtMs = (v: number) => (v >= 1000 ? `${(v / 1000).toFixed(2)} s` : `${v.toFixed(v < 10 ? 1 : 0)} ms`)
const fmtRate = (v: number) => `${fmtBytes(v)}/s`

export function Overview() {
  const l = useApi<O>('overview', undefined, POLL)
  const o = l.data
  const h = o?.history
  const charts = useMemo(() => {
    if (!h) return null
    const reasons = (Object.keys(h.rejects) as RejectReason[])
      .map((r) => [r, (h.rejects[r] ?? []).slice(-60).reduce((a, b) => a + b, 0)] as const)
      .filter(([, v]) => v > 0)
      .sort((a, b) => b[1] - a[1])
      .slice(0, 5)
      .map(([r]) => r)
    const rejSeries: Series[] = reasons.map((r) => ({ label: REASON_LABEL[r], color: REASON_COLOR[r] }))
    return {
      events: [h.t, h.eventsIn, h.eventsOut],
      latency: [h.t, h.ttfP50Ms, h.ttfP99Ms, h.durabilityLagMs],
      bytes: [h.t, h.bytesIn, h.bytesOut],
      rejects: [h.t, ...reasons.map((r) => h.rejects[r] ?? [])],
      rejSeries,
    }
  }, [h])

  if (!o || !charts) return l.error ? <ErrorNotice error={l.error} /> : <Loading />
  const reasons = (Object.entries(o.rejectsByReason) as [RejectReason, number][]).filter(([, v]) => v > 0).sort((a, b) => b[1] - a[1])
  const maxReason = reasons[0]?.[1] ?? 1
  const maxHost = o.topHosts[0]?.eventsPerSec ?? 1
  const p99Tone = o.timeToFirehoseP99Ms > 250 ? 'bad' : o.timeToFirehoseP99Ms > 150 ? 'warn' : undefined
  const durTone = o.logDurabilityLagMs > 200 ? 'bad' : o.logDurabilityLagMs > 80 ? 'warn' : undefined
  const unhealthy = (o.hostsByStatus.backoff ?? 0) + (o.hostsByStatus.offline ?? 0)

  return (
    <>
      <div className="console-head">
        <h1>Overview</h1>
        <span className="row">
          <span className="muted small mono">seq {fmtNum(o.lastSeq)}</span>
          <Live at={l.at} error={l.error} every={POLL} />
        </span>
      </div>
      <ErrorNotice error={l.error} />
      <div className="tiles hero">
        <Tile big k="Events in per second" v={fmtSi(o.eventsInPerSec)} sub={`${fmtRate(o.bytesInPerSec)} from ${fmtNum(o.hostsConnected)} hosts`} />
        <Tile big k="Events out per second" v={fmtSi(o.eventsOutPerSec)} sub={`${fmtSi(o.rejectsPerSec)}/s rejected`} />
        <Tile big k="Consumers" v={fmtNum(o.consumers)} sub={`${fmtRate(o.bytesOutPerSec)} out`} />
        <Tile big k="Time to firehose p50 / p99" v={<>{fmtMs(o.timeToFirehoseP50Ms)}<small>/ {fmtMs(o.timeToFirehoseP99Ms)}</small></>} tone={p99Tone} sub="upstream receive → subscribeRepos" />
      </div>
      <div className="tiles">
        <Tile k="Hosts connected" v={<>{fmtNum(o.hostsConnected)}<small>of {fmtNum(o.hostsTotal)}</small></>} sub={unhealthy ? `${fmtNum(unhealthy)} in backoff or offline` : 'all reachable'} />
        <Tile k="Log durability lag" v={fmtMs(o.logDurabilityLagMs)} tone={durTone} sub="oldest event not yet durable" />
        <Tile k="Rejects per second" v={fmtSi(o.rejectsPerSec)} tone={o.rejectsPerSec / Math.max(1, o.eventsInPerSec) > 0.02 ? 'warn' : undefined} sub={`${((o.rejectsPerSec / Math.max(1, o.eventsInPerSec)) * 100).toFixed(2)}% of frames`} />
        <Tile k="Bytes in / out" v={<>{fmtBytes(o.bytesInPerSec)}<small>/ {fmtBytes(o.bytesOutPerSec)} per s</small></>} />
        <Tile
          k="Open cases"
          v={
            <Link to="/admin/cases" className="plain">
              {fmtNum(o.openCases)}
            </Link>
          }
          tone={o.openCases > 0 ? 'warn' : 'ok'}
          sub="spam and abuse thresholds"
        />
      </div>

      <HostStatusBar counts={o.hostsByStatus} total={o.hostsTotal} />

      <div className="ov-grid">
        <div>
          <div className="grid2">
            <Panel flush>
              <Chart
                title="Events"
                sub="Per second: frames in from hosts, events out on the firehose."
                series={[
                  { label: 'in', color: 'c1' },
                  { label: 'out', color: 'c2', dash: true },
                ]}
                data={charts.events}
                fmt={fmtSi}
              />
            </Panel>
            <Panel flush>
              <Chart
                title="Latency"
                sub="Time to firehose (p50, p99) and log durability lag."
                series={[
                  { label: 'p50', color: 'c1' },
                  { label: 'p99', color: 'c3' },
                  { label: 'durability lag', color: 'c4', dash: true },
                ]}
                data={charts.latency}
                fmt={fmtMs}
              />
            </Panel>
            <Panel flush>
              <Chart
                title="Bandwidth"
                sub="Bytes per second from hosts and to consumers."
                series={[
                  { label: 'in', color: 'c1' },
                  { label: 'out', color: 'c6' },
                ]}
                data={charts.bytes}
                fmt={fmtBytes}
              />
            </Panel>
            <Panel flush>
              <Chart title="Rejects by reason" sub="Per second, top five reasons over the last minute." series={charts.rejSeries} data={charts.rejects} fmt={fmtSi} />
            </Panel>
          </div>
        </div>
        <div>
          <Panel title="Rejects, last minute" desc="Frames dropped before sequencing, per second.">
            {reasons.length === 0 ? (
              <p className="muted">No rejects.</p>
            ) : (
              <div className="reasons">
                {reasons.map(([r, v]) => (
                  <Reason key={r} r={r} v={v} max={maxReason} />
                ))}
              </div>
            )}
          </Panel>
          <Panel title="Busiest hosts" desc="By events per second right now." flush actions={<Link to="/admin/hosts">All hosts</Link>}>
            <table className="data compact">
              <thead>
                <tr>
                  <th>Host</th>
                  <th>Tier</th>
                  <th className="num">Events/s</th>
                  <th style={{ width: 90 }} />
                </tr>
              </thead>
              <tbody>
                {o.topHosts.map((r) => (
                  <tr key={r.host} className="link" onClick={() => navigate(`/admin/hosts/${enc(r.host)}`)}>
                    <td className="mono">
                      <Link to={`/admin/hosts/${enc(r.host)}`} className="plain" onClick={(e) => e.stopPropagation()}>
                        {r.host.replace('.host.bsky.network', '…')}
                      </Link>
                      {r.status !== 'connected' && (
                        <>
                          {' '}
                          <StatusPill status={r.status} />
                        </>
                      )}
                    </td>
                    <td>
                      <TierPill tier={r.tier} />
                    </td>
                    <td className="num">{fmtSi(r.eventsPerSec)}</td>
                    <td>
                      <Bar frac={r.eventsPerSec / maxHost} color="c1" />
                    </td>
                  </tr>
                ))}
              </tbody>
            </table>
          </Panel>
        </div>
      </div>
    </>
  )
}

function Reason({ r, v, max }: { r: RejectReason; v: number; max: number }) {
  return (
    <>
      <span className="sw" style={{ background: `var(--${REASON_COLOR[r]})` }} />
      <span>{REASON_LABEL[r]}</span>
      <Bar frac={v / max} color={REASON_COLOR[r]} />
      <span className="num">{fmtSi(v)}/s</span>
    </>
  )
}

export function HostStatusBar({ counts, total }: { counts: Partial<Record<HostStatus, number>>; total: number }) {
  return (
    <Panel>
      <div className="row between">
        <h3>Hosts by status</h3>
        <span className="muted small">{fmtNum(total)} known hosts</span>
      </div>
      <div className="statusbar" role="img" aria-label="Hosts by status">
        {HOST_STATUSES.map((s) => {
          const n = counts[s] ?? 0
          return n ? <i key={s} title={`${s}: ${n}`} style={{ width: `${(n / total) * 100}%`, background: `var(--${STATUS_COLOR[s]})` }} /> : null
        })}
      </div>
      <div className="chips">
        {HOST_STATUSES.map((s) => (
          <Link key={s} to={`/admin/hosts?status=${s}`} className="chip" aria-pressed="false">
            <StatusPill status={s} />
            <span className="n">{fmtNum(counts[s] ?? 0)}</span>
          </Link>
        ))}
      </div>
    </Panel>
  )
}
