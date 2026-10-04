import { useState } from 'react'
import { Chart } from '../components/Chart'
import { Bar, InlineConfirm, Live, REASON_COLOR, REASON_LABEL, Sparkline, StatusPill, Tile, TierPill } from '../components/relay'
import { CopyText, Empty, ErrorNotice, Loading, Notice, Panel } from '../components/ui'
import type { HostAction, HostDetail as HD, PolicyDoc, RejectReason } from '../lib/api'
import { api, enc, errText } from '../lib/api'
import { fmtLag, fmtNum, fmtSi, fmtTime, relTime, short } from '../lib/format'
import { Link } from '../lib/router'
import { useApi } from '../lib/useApi'

const POLL = 2000
/** The one-click account cap for a real PDS: above every independent PDS today (~65k), below trusted's 10M. */
const BIG_HOST_CAP = 1_000_000

type Pending = { action: HostAction; label: string; msg: string; danger?: boolean; reason?: string }

function describe(a: HostAction): string {
  switch (a.action) {
    case 'set-tier':
      return `tier → ${a.tier}`
    case 'throttle':
      return a.eventsPerSec == null ? 'throttle lifted' : `throttled to ${a.eventsPerSec} events/s`
    case 'suspend':
      return `suspended: ${a.reason}`
    case 'ban':
      return `banned: ${a.reason}`
    case 'unban':
      return 'unbanned'
    case 'reconnect':
      return 'reconnect'
    case 'set-account-limit':
      return a.maxAccounts == null ? "account cap back to the tier's" : `account cap → ${fmtNum(a.maxAccounts)}`
  }
}

export function HostDetail({ host }: { host: string }) {
  const l = useApi<HD>(`hosts/${enc(host)}`, undefined, POLL)
  const pol = useApi<PolicyDoc>('policy')
  const [pending, setPending] = useState<Pending | null>(null)
  const [busy, setBusy] = useState(false)
  const [err, setErr] = useState<unknown>()
  const [tier, setTier] = useState('')
  const [throttle, setThrottle] = useState('')
  const d = l.data

  if (!d) return l.error ? <ErrorNotice error={l.error} /> : <Loading />
  const r = d.row
  const tiers = Object.keys(pol.data?.policy.tiers ?? { [r.tier]: null })
  const blocked = r.status === 'banned' || r.status === 'suspended'
  const tierCap = pol.data?.policy.tiers[r.tier]?.maxAccounts
  const ownCap = tierCap != null && d.limits.maxAccounts !== tierCap
  const atCap = d.limits.maxAccounts > 0 && r.accounts >= d.limits.maxAccounts

  const ask = (p: Pending) => {
    setErr(undefined)
    setPending(p)
  }
  const run = async (reason: string) => {
    if (!pending) return
    const a = pending.action
    const body = a.action === 'ban' || a.action === 'suspend' ? { ...a, reason } : a
    setBusy(true)
    try {
      await api(`hosts/${enc(host)}/action`, { body })
      setPending(null)
      l.reload()
    } catch (e) {
      setErr(e)
    } finally {
      setBusy(false)
    }
  }
  const reasons = (Object.entries(d.rejectsByReason) as [RejectReason, number][]).filter(([, n]) => n > 0).sort((a, b) => b[1] - a[1])
  const maxR = reasons[0]?.[1] ?? 1
  const limitLine = d.series.t.map(() => d.limits.eventsPerSec)

  return (
    <>
      <nav className="crumbs" aria-label="Breadcrumb">
        <Link to="/admin/hosts">Hosts</Link> <span aria-hidden="true">/</span>
      </nav>
      <div className="hd-head">
        <h1 className="break">{r.host}</h1>
        <StatusPill status={r.status} />
        <TierPill tier={r.tier} />
        {r.throttle != null && <span className="pill amber">throttle {fmtSi(r.throttle)}/s</span>}
        {r.rule != null && (
          <Link to="/admin/rules" className="pill">
            domain rule #{r.rule}
          </Link>
        )}
        {d.openCases.map((id) => (
          <Link key={id} to={`/admin/cases/${id}`} className="pill danger">
            case #{id}
          </Link>
        ))}
        <CopyText text={r.host} display="" />
        <span style={{ flex: 1 }} />
        <span className="row small muted" style={{ gap: 8 }} title="Events per second, last 2 minutes">
          <Sparkline values={d.series.events} width={140} height={24} fill />
          <Sparkline values={d.series.rejects} width={80} height={24} color="c5" />
        </span>
        <Live at={l.at} error={l.error} every={POLL} />
      </div>
      <ErrorNotice error={l.error} />

      {atCap && d.limits.maxAccounts < BIG_HOST_CAP && (
        <Notice kind="warn">
          <strong>{host} is at its account cap</strong> ({fmtNum(r.accounts)} of {fmtNum(d.limits.maxAccounts)}). Every account the relay sees for the first time on this host is created throttled
          and its commits are dropped. On a new relay that's every active account, so a real PDS with more than {fmtNum(d.limits.maxAccounts)} users hits this within minutes.
          Raising the cap admits new accounts from now on; accounts already throttled stay throttled until an operator lifts them (untakedown).{' '}
          <button
            type="button"
            className="btn sm primary"
            onClick={() =>
              ask({
                action: { action: 'set-account-limit', maxAccounts: BIG_HOST_CAP },
                label: 'Raise account cap',
                msg: `Raise ${host}'s account cap to ${fmtNum(BIG_HOST_CAP)}? Its tier and event limits stay as they are.`,
              })
            }
          >
            Raise cap to {fmtNum(BIG_HOST_CAP)}
          </button>
        </Notice>
      )}
      <Panel>
        <div className="actions-bar" role="toolbar" aria-label="Host actions">
          <label className="row" style={{ gap: 6 }}>
            <span className="small muted">Tier</span>
            <select value={tier || r.tier} onChange={(e) => setTier(e.target.value)} aria-label="Tier">
              {tiers.map((t) => (
                <option key={t}>{t}</option>
              ))}
            </select>
          </label>
          <button
            type="button"
            className="btn sm"
            disabled={!tier || tier === r.tier}
            onClick={() => ask({ action: { action: 'set-tier', tier }, label: 'Set tier', msg: `Move ${host} from ${r.tier} to ${tier}? Its limits change on every node within a few seconds.` })}
          >
            Set tier
          </button>
          <span className="sep" />
          <label className="row" style={{ gap: 6 }}>
            <span className="small muted">Throttle</span>
            <input type="number" min={0} step="any" placeholder="events/s" value={throttle} onChange={(e) => setThrottle(e.target.value)} style={{ width: 100 }} aria-label="Throttle, events per second" />
          </label>
          <button
            type="button"
            className="btn sm"
            disabled={throttle === '' || !(Number(throttle) >= 0)}
            onClick={() => ask({ action: { action: 'throttle', eventsPerSec: Number(throttle) }, label: 'Throttle', msg: `Cap ${host} at ${throttle} events/s (on top of its tier's ${fmtSi(d.limits.eventsPerSec)}/s)?` })}
          >
            Throttle
          </button>
          {r.throttle != null && (
            <button type="button" className="btn sm" onClick={() => ask({ action: { action: 'throttle', eventsPerSec: null }, label: 'Lift throttle', msg: `Lift the ${r.throttle}/s throttle on ${host}?` })}>
              Lift throttle
            </button>
          )}
          {ownCap && (
            <button
              type="button"
              className="btn sm"
              onClick={() => ask({ action: { action: 'set-account-limit', maxAccounts: null }, label: 'Tier account cap', msg: `Put ${host} back on its tier's account cap (${fmtNum(tierCap ?? 0)})?` })}
            >
              Tier account cap
            </button>
          )}
          <span className="sep" />
          <button type="button" className="btn sm" disabled={blocked} onClick={() => ask({ action: { action: 'reconnect' }, label: 'Reconnect', msg: `Drop and redial the socket to ${host}? It resumes from its cursor (seq ${r.lastUpstreamSeq}).` })}>
            Reconnect
          </button>
          <span style={{ flex: 1 }} />
          {blocked ? (
            <button type="button" className="btn sm primary" onClick={() => ask({ action: { action: 'unban' }, label: r.status === 'banned' ? 'Unban' : 'Resume', msg: `Let ${host} connect again? The relay redials it and resumes from seq ${r.lastUpstreamSeq}.` })}>
              {r.status === 'banned' ? 'Unban' : 'Resume'}
            </button>
          ) : (
            <>
              <button
                type="button"
                className="btn sm danger"
                onClick={() => ask({ action: { action: 'suspend', reason: '' }, label: 'Suspend', danger: true, reason: 'Reason (kept in the audit log)', msg: `Suspend ${host}? The socket closes and its cursor is kept, so resuming loses nothing.` })}
              >
                Suspend
              </button>
              <button
                type="button"
                className="btn sm danger solid"
                onClick={() => ask({ action: { action: 'ban', reason: '' }, label: 'Ban host', danger: true, reason: 'Reason (kept in the audit log)', msg: `Ban ${host}? The relay drops its events and refuses its requestCrawl until unbanned.` })}
              >
                Ban
              </button>
            </>
          )}
        </div>
        <InlineConfirm open={!!pending} action={pending?.label ?? ''} danger={pending?.danger} reason={pending?.reason} busy={busy} error={err ? errText(err) : undefined} onConfirm={run} onCancel={() => setPending(null)}>
          {pending?.msg}
        </InlineConfirm>
      </Panel>

      <div className="tiles">
        <Tile k="Events per second" v={fmtSi(r.eventsPerSec)} sub={`limit ${fmtSi(d.limits.eventsPerSec)}/s`} tone={r.status === 'throttled' ? 'warn' : undefined} />
        <Tile k="Error rate" v={`${(r.errorRate * 100).toFixed(r.errorRate < 0.1 ? 2 : 0)}%`} tone={r.errorRate >= 0.1 ? 'bad' : r.errorRate >= 0.02 ? 'warn' : undefined} sub="rejected frames, last minute" />
        <Tile k="Accounts" v={fmtNum(r.accounts)} sub={`max ${fmtNum(d.limits.maxAccounts)}`} tone={r.accounts > d.limits.maxAccounts ? 'bad' : undefined} />
        <Tile k="New accounts per hour" v={fmtNum(d.newAccountsPerHour)} sub={`limit ${fmtNum(d.limits.newAccountsPerHour)}`} tone={d.newAccountsPerHour > d.limits.newAccountsPerHour ? 'bad' : undefined} />
        <Tile k="Lag" v={r.lagMs ? fmtLag(r.lagMs) : '—'} sub="behind the host's stream (read time minus event time)" tone={r.lagMs > 600_000 ? 'bad' : r.lagMs > 60_000 ? 'warn' : undefined} />
        <Tile k="Connected" v={r.connectedSinceMs ? relTime(r.connectedSinceMs).replace(' ago', '') : '—'} sub={r.connectedSinceMs ? fmtTime(r.connectedSinceMs) : 'not connected'} />
        <Tile k="Upstream seq" v={<span className="mono">{r.lastUpstreamSeq}</span>} sub={`host shard on ${r.node}`} />
      </div>

      <div className="grid2">
        <Panel flush>
          <Chart
            title="Events"
            sub="Per second, with the enforced limit."
            series={[
              { label: 'events', color: 'c1' },
              { label: 'limit', color: 'ink3', dash: true },
            ]}
            data={[d.series.t, d.series.events, limitLine]}
            fmt={fmtSi}
          />
        </Panel>
        <Panel flush>
          <Chart title="Rejects" sub="Frames dropped per second." series={[{ label: 'rejects', color: 'c5' }]} data={[d.series.t, d.series.rejects]} fmt={fmtSi} />
        </Panel>
      </div>

      <div className="grid2">
        <Panel title="Recent rejects" desc="A sample of dropped frames, newest first." flush>
          {d.recentRejects.length === 0 ? (
            <Empty title="No rejects">Every frame from this host passed the checks.</Empty>
          ) : (
            <div className="table-wrap" style={{ maxHeight: 420, overflowY: 'auto' }}>
              <table className="data compact">
                <thead>
                  <tr>
                    <th>When</th>
                    <th>Reason</th>
                    <th>DID</th>
                    <th className="num">Seq</th>
                  </tr>
                </thead>
                <tbody>
                  {d.recentRejects.map((x, i) => (
                    <tr key={i} title={x.detail}>
                      <td className="muted" title={fmtTime(x.atMs)}>
                        {relTime(x.atMs)}
                      </td>
                      <td>
                        <span className="row" style={{ gap: 6, flexWrap: 'nowrap' }}>
                          <span className="swatch" style={{ background: `var(--${REASON_COLOR[x.reason]})` }} />
                          {REASON_LABEL[x.reason]}
                        </span>
                      </td>
                      <td className="mono">
                        <Link to={`/admin/accounts/${enc(x.did)}`}>{short(x.did, 10)}</Link>
                      </td>
                      <td className="num mono muted">{x.upstreamSeq}</td>
                    </tr>
                  ))}
                </tbody>
              </table>
            </div>
          )}
        </Panel>
        <div>
          <Panel title="Rejects by reason" desc="Since the relay started.">
            {reasons.length === 0 ? (
              <p className="muted">None.</p>
            ) : (
              <div className="reasons">
                {reasons.map(([k, n]) => (
                  <span key={k} style={{ display: 'contents' }}>
                    <span className="sw" style={{ background: `var(--${REASON_COLOR[k]})` }} />
                    <span>{REASON_LABEL[k]}</span>
                    <Bar frac={n / maxR} color={REASON_COLOR[k]} />
                    <span className="num">{fmtNum(n)}</span>
                  </span>
                ))}
              </div>
            )}
          </Panel>
          <Panel title="Limits in force" desc={`The ${r.tier} tier${r.throttle != null ? ', with the operator throttle' : ''}.`}>
            <dl className="dl compact">
              <dt>Events per second</dt>
              <dd>{fmtNum(d.limits.eventsPerSec, 1)}</dd>
              <dt>Events per hour</dt>
              <dd>{fmtNum(d.limits.eventsPerHour)}</dd>
              <dt>Events per day</dt>
              <dd>{fmtNum(d.limits.eventsPerDay)}</dd>
              <dt>Max accounts</dt>
              <dd>{fmtNum(d.limits.maxAccounts)}</dd>
              <dt>New accounts per hour</dt>
              <dd>{fmtNum(d.limits.newAccountsPerHour)}</dd>
            </dl>
          </Panel>
          <Panel title="Operator actions" flush>
            {d.actions.length === 0 ? (
              <p className="muted" style={{ padding: '0 18px' }}>
                None yet.
              </p>
            ) : (
              <table className="data compact">
                <tbody>
                  {d.actions.map((a, i) => (
                    <tr key={i}>
                      <td className="muted" title={fmtTime(a.atMs)}>
                        {relTime(a.atMs)}
                      </td>
                      <td>{describe(a.action)}</td>
                      <td className="muted">{a.by}</td>
                    </tr>
                  ))}
                </tbody>
              </table>
            )}
          </Panel>
        </div>
      </div>
    </>
  )
}
