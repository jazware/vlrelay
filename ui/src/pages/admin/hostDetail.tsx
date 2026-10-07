import { useEffect, type ReactNode } from 'react'
import { registerDetail } from '../../components/console/Drawer'
import { openPanel } from '../../components/console/nav'
import { BIG_HOST_CAP, hostActionDialog, useHostsVersion } from '../../components/console/hostActions'
import { Bars, Copy, Empty, Glyph, HostStatusChip, KV, Meter, Mini, Minis, Sec, Seg, Spark, Strip, TierTag } from '../../components/console/kit'
import type { Case, DomainRule, HostAction, HostDetail, Policy, RejectReason } from '../../lib/api'
import { host as fetchHost } from '../../lib/console/adminAdapter'
import { ago, dt, fmtMs, fmtNum, fmtRatio, fmtSi, plural, shortDid } from '../../lib/console/fmt'
import { useLivePoll } from '../../lib/console/live'
import { policyPoll } from '../../lib/console/polls'
import { useRelay } from '../../lib/console/relay'
import { SourceTag } from './hostSource'
import { casesPoll, releaseDialog, rulesPoll } from './moderationDetail'
import { NodeTag, REASON_WHAT, reasonLabel } from './relayUi'

// A PDS host in the slide-over or on its own page: its rates and limits, why its frames are
// rejected, its upstream socket, and every host action behind a confirm.

function describe(a: HostAction): string {
  switch (a.action) {
    case 'set-tier':
      return `set-tier ${a.tier}`
    case 'throttle':
      return a.eventsPerSec == null ? 'throttle lifted' : `throttle ${a.eventsPerSec}/s`
    case 'suspend':
      return `suspend: ${a.reason}`
    case 'ban':
      return `ban: ${a.reason}`
    case 'unban':
      return 'unban'
    case 'reconnect':
      return 'reconnect'
    case 'set-account-limit':
      return a.maxAccounts == null ? "account cap back to the tier's" : `set-account-limit ${fmtNum(a.maxAccounts)}`
  }
}

const cols = (page: boolean, a: ReactNode, b: ReactNode) =>
  page ? (
    <div className="cols">
      <div>{a}</div>
      <div>{b}</div>
    </div>
  ) : (
    <>
      {a}
      {b}
    </>
  )

type Acted = HostDetail['actions'][number]
const HELD = new Set(['throttled', 'backoff', 'suspended', 'banned'])

const by = (a: Acted) => `${a.by}, ${ago(a.atMs)}`
const ruleName = (r: DomainRule) => (
  <button type="button" className="cx-linklike" onClick={() => openPanel('rule', String(r.id))}>
    rule {r.id} <span className="mono">{r.pattern}</span>
  </button>
)

/**
 * Why a throttled, backing-off, suspended or banned host is held, in one place: the limit that
 * binds, where its tier came from, who set an operator throttle, and the way out.
 */
function WhyHeld({ d, policy, rules, cases }: { d: HostDetail; policy?: Policy; rules?: DomainRule[]; cases?: Case[] }) {
  const r = d.row
  if (!HELD.has(r.status)) return null
  const lim = d.limits
  const acts = [...d.actions].sort((a, b) => b.atMs - a.atMs)
  const last = (k: HostAction['action']) => acts.find((a) => a.action.action === k)
  const rule = r.rule != null ? rules?.find((x) => x.id === r.rule) : undefined
  const thrSet = r.throttle != null ? acts.find((a) => a.action.action === 'throttle' && a.action.eventsPerSec != null) : undefined
  const hourUse = d.series.events.length ? (d.series.events.reduce((a, b) => a + b, 0) / d.series.events.length) * 3600 : r.eventsPerSec * 3600
  const binding: [string, number, number][] = (
    [
      ['events/s', r.eventsPerSec, lim.eventsPerSec],
      ['events/h', hourUse, lim.eventsPerHour],
    ] as [string, number, number][]
  ).filter(([, u, v]) => v > 0 && u >= v * 0.8)
  const tiers = Object.entries(policy?.tiers ?? {}).sort((a, b) => a[1].eventsPerSec - b[1].eventsPerSec)
  const here = policy?.tiers[r.tier]?.eventsPerSec ?? lim.eventsPerSec
  const roomier = tiers.find(([t, l]) => t !== r.tier && l.eventsPerSec > here)

  let tone: 'warn' | 'err' = 'warn'
  let title: ReactNode
  const lines: ReactNode[] = []
  const outs: ReactNode[] = []
  const ruleOut = rule && (
    <button key="rule" type="button" className="cx-btn sm" onClick={() => openPanel('rule', String(rule.id))}>
      Edit rule {rule.id} ›
    </button>
  )
  if (r.status === 'banned' || r.status === 'suspended') {
    tone = 'err'
    const act = last(r.status === 'banned' ? 'ban' : 'suspend')
    const byRule = r.status === 'banned' && rule?.effect.kind === 'ban'
    title = byRule ? <>Banned by domain rule {rule.id}</> : r.status === 'banned' ? 'Banned' : 'Suspended'
    if (byRule) lines.push(<>Matched {ruleName(rule)}{rule.note ? <> “{rule.note}”</> : null} · {rule.createdBy}, {ago(rule.createdAtMs)}</>)
    if (act && (act.action.action === 'ban' || act.action.action === 'suspend')) lines.push(<>By {by(act)}: {act.action.reason}</>)
    else if (!byRule) lines.push(<span className="muted">No operator action on record says who.</span>)
    lines.push(r.status === 'banned' ? 'Its socket stays closed and its requestCrawl is refused.' : 'Its socket is closed and its cursor kept: resuming loses nothing.')
    if (!byRule)
      outs.push(
        <button key="unban" type="button" className="cx-btn sm primary" onClick={() => hostActionDialog('unban', r)}>
          {r.status === 'banned' ? 'Unban…' : 'Resume…'}
        </button>,
      )
    if (ruleOut) outs.push(ruleOut)
  } else if (r.status === 'backoff') {
    title = 'Backing off: its last connect failed'
    lines.push(`The reader retries on its own, waiting longer each time${r.connectedSinceMs ? `; it was last connected ${ago(r.connectedSinceMs)}` : ''}.`)
    outs.push(
      <button key="re" type="button" className="cx-btn sm" onClick={() => hostActionDialog('reconnect', r)}>
        Reconnect now…
      </button>,
    )
  } else {
    // a domain rule's throttle shows on the row as `throttle` too; it's the operator's when an action set it or the rates differ
    const ruleThr = rule?.effect.kind === 'throttle' ? rule.effect.eventsPerSec : undefined
    const opThr = r.throttle != null && (!!thrSet || ruleThr === undefined || r.throttle !== ruleThr)
    const byRule = r.throttle != null && !opThr
    // the spam policy's auto-throttle sets the same host throttle, and its case says so
    const auto = opThr && !thrSet ? cases?.find((c) => c.host === r.host && /throttl/i.test(c.autoAction ?? '')) : undefined
    title = auto ? (
      <>
        Held at {fmtNum(r.throttle!)} events/s by case {auto.id}'s auto-throttle
      </>
    ) : opThr ? (
      <>Held at {fmtNum(r.throttle!)} events/s by an operator throttle</>
    ) : byRule || ruleThr !== undefined ? (
      <>
        Held at {fmtNum(ruleThr ?? r.throttle!)} events/s by domain rule {rule!.id}
      </>
    ) : binding.length ? (
      <>
        Held at {binding[0][0] === 'events/s' ? `${fmtNum(lim.eventsPerSec)} events/s` : `${fmtNum(lim.eventsPerHour)} events/h`} by the {r.tier} tier
      </>
    ) : (
      <>Throttled in the {r.tier} tier</>
    )
    if (binding.length) lines.push(<>Binding: {binding.map(([l, u, v]) => `${l} ${fmtSi(u)} of ${fmtNum(v)}`).join(' · ')}</>)
    if (rule && (rule.effect.kind === 'tier' || rule.effect.kind === 'throttle'))
      lines.push(
        <>
          {rule.effect.kind === 'tier' ? 'Tier from' : 'Throttle from'} {ruleName(rule)}
          {rule.note ? <> “{rule.note}”</> : null} · {rule.createdBy}, {ago(rule.createdAtMs)}
        </>,
      )
    if (!rule || rule.effect.kind !== 'tier') {
      const set = last('set-tier')
      lines.push(
        set && set.action.action === 'set-tier' && set.action.tier === r.tier ? (
          <>Tier {r.tier} set by {by(set)}</>
        ) : r.tier === policy?.defaultTier ? (
          <>The {r.tier} tier is the default for a new host: no rule or operator moved it.</>
        ) : (
          <>No rule or operator action on record set the {r.tier} tier.</>
        ),
      )
    }
    lines.push(
      auto ? (
        <>
          <button type="button" className="cx-linklike" onClick={() => openPanel('case', String(auto.id))}>
            Case {auto.id}
          </button>{' '}
          ({auto.kind.replace(/-/g, ' ')}, opened {ago(auto.openedAtMs)}) {auto.autoAction}. No operator has changed it since.
        </>
      ) : !opThr ? 'No operator throttle.' : thrSet && thrSet.action.action === 'throttle' ? <>Operator throttle {fmtNum(thrSet.action.eventsPerSec ?? 0)}/s set by {by(thrSet)}</> : <>Operator throttle {fmtNum(r.throttle!)}/s; no action on record says who set it.</>,
    )
    if (opThr)
      outs.push(
        <button key="lift" type="button" className="cx-btn sm primary" onClick={() => hostActionDialog('unthrottle', r)}>
          Lift throttle…
        </button>,
      )
    // a tier change only helps when the tier is what binds
    if (roomier && r.throttle == null && ruleThr === undefined)
      outs.push(
        <button key="tier" type="button" className="cx-btn sm primary" onClick={() => hostActionDialog('settier', r, roomier[0])}>
          Set tier {roomier[0]} ({fmtNum(roomier[1].eventsPerSec)}/s)…
        </button>,
      )
    if (ruleOut) outs.push(ruleOut)
  }
  return (
    <div className={`cx-banner ${tone} cx-why`} role="note" aria-label="Why it's held">
      <div className="bh">
        <Glyph k={tone} />
        <span className="bt">{title}</span>
      </div>
      <div className="bb">
        {lines.map((l, i) => (
          <div key={i}>{l}</div>
        ))}
        {outs.length > 0 && <div className="cx-form-row">{outs}</div>}
      </div>
    </div>
  )
}

/** An action row: what it does on the left, the control on the right. */
const Act = ({ title, desc, children }: { title: string; desc: ReactNode; children: ReactNode }) => (
  <div className="cx-act">
    <div className="ad">
      <b>{title}</b>
      {desc}
    </div>
    {children}
  </div>
)

function Body({ d, page }: { d: HostDetail; page: boolean }) {
  const { view } = useRelay()
  const pol = policyPoll.use()
  const rules = rulesPoll.use()
  const cases = casesPoll.use()
  const r = d.row
  const live = r.status === 'connected' || r.status === 'throttled'
  const blocked = r.status === 'banned' || r.status === 'suspended'
  const tiers = Object.keys(pol.data?.policy.tiers ?? { [r.tier]: null })
  const tierCap = pol.data?.policy.tiers[r.tier]?.maxAccounts
  const ownCap = tierCap != null && d.limits.maxAccounts !== tierCap
  const cap = d.limits.maxAccounts
  const atCap = cap > 0 && r.accounts >= cap
  const reasons = (Object.entries(d.rejectsByReason) as [RejectReason, number][]).filter(([, n]) => n > 0).sort((a, b) => b[1] - a[1])
  const lim = d.limits
  const hourShare = d.series.events.length ? d.series.events.reduce((a, b) => a + b, 0) / d.series.events.length : r.eventsPerSec
  const limits: [string, number, number][] = [
    ['Events/s', lim.eventsPerSec, r.eventsPerSec],
    ['Events/h', lim.eventsPerHour, hourShare * 3600],
    ['Accounts', cap, r.accounts],
    ['New accounts/h', lim.newAccountsPerHour, d.newAccountsPerHour],
  ]
  const main = (
    <>
      <Strip
        items={[
          ['events/s', live ? fmtSi(r.eventsPerSec) : '—'],
          ['rejected', fmtRatio(r.errorRate)],
          ['accounts / cap', `${fmtSi(r.accounts)} / ${cap ? fmtSi(cap) : '—'}`],
          ['read lag', live && r.lagMs ? fmtMs(r.lagMs) : '—'],
          ['upstream seq', fmtNum(r.lastUpstreamSeq)],
        ]}
      />
      <WhyHeld d={d} policy={pol.data?.policy} rules={rules.data} cases={cases.data} />
      {atCap && cap < BIG_HOST_CAP && (
        <div className="cx-banner warn">
          <div className="bh">
            <Glyph k="warn" />
            <span>
              <b>At its account cap</b> ({fmtNum(r.accounts)} of {fmtNum(cap)}). New accounts on it are created throttled.
            </span>
            <button type="button" className="cx-btn sm primary" onClick={() => hostActionDialog('raisecap', r)}>
              Raise cap to {fmtNum(BIG_HOST_CAP)}…
            </button>
          </div>
        </div>
      )}
      {live && r.lagMs > 60_000 && (
        <div className="cx-banner warn">
          <div className="bh">
            <Glyph k="warn" />
            <span>
              <b>{fmtMs(r.lagMs)} behind its own stream.</b> Past what its PDS keeps in its outbox, the PDS cuts the relay off and events are lost.
            </span>
          </div>
        </div>
      )}
      <Minis style={{ padding: 0 }}>
        <Mini label={`Events/s, ${d.series.events.length} s`} value={live ? fmtSi(r.eventsPerSec) : '—'}>
          <Spark data={d.series.events} color={r.status === 'throttled' ? 'warn' : 'accent'} th={lim.eventsPerSec < Math.max(...d.series.events, 0) * 3 ? lim.eventsPerSec : undefined} />
        </Mini>
        <Mini label="Rejects/s" value={fmtSi(d.series.rejects[d.series.rejects.length - 1] ?? 0)}>
          <Spark data={d.series.rejects} color="err" />
        </Mini>
      </Minis>
      <Sec title="Limits in force" digest={`${r.tier} tier${r.throttle != null ? ` · operator throttle ${fmtNum(r.throttle)}/s` : ''}${ownCap ? ' · own account cap' : ''}`} open flush>
        <div className="cx-tw">
          <table className="cx-t compact">
            <thead>
              <tr>
                <th>Limit</th>
                <th className="r">Value</th>
                <th>Use now</th>
              </tr>
            </thead>
            <tbody>
              {limits.map(([l, v, u]) => (
                <tr key={l}>
                  <td>{l}</td>
                  <td className="r mono sm">{v > 0 ? fmtNum(v) : 'unlimited'}</td>
                  <td>
                    {v > 0 && <Meter v={u} max={v} k={u >= v ? 'err' : u > v * 0.8 ? 'warn' : 'ok'} />} <span className="mono sm">{fmtSi(u)}</span>
                  </td>
                </tr>
              ))}
            </tbody>
          </table>
        </div>
      </Sec>
      <Sec title="Rejects by reason" digest={reasons.length ? plural(reasons.length, 'reason') : 'none'} open={reasons.length > 0} flush>
        {reasons.length ? (
          <Bars color="err" rows={reasons.map(([k, n]) => ({ key: k, label: reasonLabel(k), v: n, fmt: fmtNum(n), title: REASON_WHAT[k] }))} />
        ) : (
          <Empty>Every frame from this host passed the checks.</Empty>
        )}
      </Sec>
      <Sec title="Recent rejects" digest={d.recentRejects.length ? `${d.recentRejects.length} newest` : 'none'} open={page && d.recentRejects.length > 0} flush>
        {d.recentRejects.length ? (
          <div className="cx-tw">
            <table className="cx-t compact">
              <tbody>
                {d.recentRejects.slice(0, 12).map((x, i) => (
                  <tr key={i} title={x.detail}>
                    <td className="sm muted">{ago(x.atMs)}</td>
                    <td className="cx-did">
                      <button type="button" className="cx-linklike" onClick={() => openPanel('acct', x.did)}>{shortDid(x.did)}</button>
                    </td>
                    <td className="mono sm s-err">{reasonLabel(x.reason)}</td>
                    <td className="r mono sm t2">#{fmtNum(x.upstreamSeq)}</td>
                  </tr>
                ))}
              </tbody>
            </table>
          </div>
        ) : (
          <Empty>Nothing rejected recently.</Empty>
        )}
      </Sec>
    </>
  )
  const side = (
    <>
      <Sec title="Upstream" open>
        <KV
          rows={[
            ['Socket', <Copy key="s" text={`wss://${r.host}/xrpc/com.atproto.sync.subscribeRepos?cursor=${r.lastUpstreamSeq}`} />],
            ['Status', <span key="st"><HostStatusChip s={r.status} /> {r.connectedSinceMs ? `since ${ago(r.connectedSinceMs)}` : ''}</span>],
            ['Reader', <NodeTag key="n" view={view} id={r.node} />],
            ['Tier', <TierTag key="t" t={r.tier} />],
            [
              'Domain rule',
              r.rule != null ? (
                <button key="r" type="button" className="cx-linklike" onClick={() => openPanel('rule', String(r.rule))}>
                  rule {r.rule}
                </button>
              ) : (
                <span key="r" className="muted">none</span>
              ),
            ],
            ['Connected', r.connectedSinceMs ? dt(r.connectedSinceMs) : <span key="c" className="muted">not connected</span>],
            ['Found by', <SourceTag key="src" s={r.source} />],
          ]}
        />
      </Sec>
      <Sec title="Actions" digest="each one is audited on the host record" open flush>
        <div className="cx-acts">
          <Act title="Tier" desc="Overrides the tier until it's changed again.">
            <Seg label="Tier" value={r.tier} options={tiers.map((t) => ({ v: t, label: t }))} onChange={(t) => t !== r.tier && hostActionDialog('settier', r, t)} />
          </Act>
          <Act title="Throttle" desc={r.throttle != null ? `Operator throttle at ${fmtNum(r.throttle)}/s.` : 'Hold its reader at a rate. The PDS buffers; nothing is dropped.'}>
            <span className="cx-form-row">
              <button type="button" className="cx-btn sm" onClick={() => hostActionDialog('throttle', r)}>
                Throttle…
              </button>
              {r.throttle != null && (
                <button type="button" className="cx-btn sm" onClick={() => hostActionDialog('unthrottle', r)}>
                  Lift
                </button>
              )}
            </span>
          </Act>
          <Act title="Account cap" desc={`${ownCap ? 'Its own cap' : 'The tier cap'}: ${cap ? fmtNum(cap) : 'none'}.`}>
            <span className="cx-form-row">
              {cap < BIG_HOST_CAP && (
                <button type="button" className="cx-btn sm" onClick={() => hostActionDialog('raisecap', r)}>
                  Raise to {fmtNum(BIG_HOST_CAP)}…
                </button>
              )}
              {ownCap && (
                <button type="button" className="cx-btn sm" onClick={() => hostActionDialog('tiercap', r)}>
                  Use the tier's…
                </button>
              )}
            </span>
          </Act>
          <Act title="Reconnect" desc="Closes the socket and resumes from the last acked cursor.">
            <button type="button" className="cx-btn sm" disabled={blocked} onClick={() => hostActionDialog('reconnect', r)}>
              Reconnect…
            </button>
          </Act>
        </div>
      </Sec>
      <Sec title="Suspend or ban" danger open={page || blocked} flush>
        <div className="cx-acts">
          {blocked ? (
            <Act title={r.status === 'banned' ? 'Unban' : 'Resume'} desc="It can connect again and its requestCrawl works.">
              <button type="button" className="cx-btn sm" onClick={() => hostActionDialog('unban', r)}>
                {r.status === 'banned' ? 'Unban…' : 'Resume…'}
              </button>
            </Act>
          ) : (
            <>
              <Act title="Suspend" desc="Disconnects it and keeps its cursor; resuming later loses nothing.">
                <button type="button" className="cx-btn sm danger" onClick={() => hostActionDialog('suspend', r)}>
                  Suspend…
                </button>
              </Act>
              <Act title="Ban" desc="Never connected again; its requestCrawl is refused.">
                <button type="button" className="cx-btn sm danger" onClick={() => hostActionDialog('ban', r)}>
                  Ban…
                </button>
              </Act>
            </>
          )}
        </div>
      </Sec>
      <Sec title="Operator actions" digest={d.actions.length ? `${d.actions.length}` : 'none'} open={d.actions.length > 0} flush>
        {d.actions.length ? (
          <div className="cx-tw">
            <table className="cx-t compact">
              <tbody>
                {d.actions.map((a, i) => (
                  <tr key={i}>
                    <td className="sm muted">{ago(a.atMs)}</td>
                    <td className="sm">{a.by}</td>
                    <td className="mono sm">{describe(a.action)}</td>
                  </tr>
                ))}
              </tbody>
            </table>
          </div>
        ) : (
          <Empty>None yet.</Empty>
        )}
      </Sec>
      {d.openCases.length > 0 && (
        <Sec title="Cases" digest={`${d.openCases.length} open`} open flush>
          {d.openCases.map((id) => (
            <button key={id} type="button" className="cx-rrow" onClick={() => openPanel('case', String(id))}>
              <Glyph k="warn" />
              <span className="mono sm">case {id}</span>
              <span className="x">open</span>
            </button>
          ))}
        </Sec>
      )}
      {(atCap || r.throttledAccounts > 0) && (
        <Sec title="Accounts created throttled" digest={r.throttledAccounts ? plural(r.throttledAccounts, 'account') : 'past its cap'} open flush>
          <div className="cx-acts">
            <Act
              title={r.throttledAccounts ? `Lift ${plural(r.throttledAccounts, 'account')}` : 'Lift them'}
              desc={atCap ? 'Each gets #account active. Raise the cap first, or new ones keep arriving throttled.' : 'Each gets #account active.'}
            >
              <button type="button" className="cx-btn sm" onClick={() => releaseDialog(r.host, atCap)}>
                Lift…
              </button>
            </Act>
          </div>
        </Sec>
      )}
    </>
  )
  return cols(page, main, side)
}

registerDetail('host', {
  kind: 'PDS host',
  section: 'hosts',
  use: (id, mode) => {
    const v = useHostsVersion()
    const l = useLivePoll(() => fetchHost(id), id, 2000)
    const reload = l.reload
    useEffect(() => {
      if (v) reload()
    }, [v, reload])
    const d = l.data
    if (!d)
      return {
        title: id,
        body: null,
        loading: !l.error,
        missing: l.error ? `Couldn't load ${id}: ${l.error instanceof Error ? l.error.message : String(l.error)}` : undefined,
      }
    return {
      title: d.row.host,
      chip: (
        <>
          <HostStatusChip s={d.row.status} /> <TierTag t={d.row.tier} />
        </>
      ),
      foot: (
        <>
          read by <span className="mono">{d.row.node || '—'}</span> · GET /admin/api/hosts/{'{host}'}
        </>
      ),
      body: <Body d={d} page={mode === 'page'} />,
    }
  },
})
