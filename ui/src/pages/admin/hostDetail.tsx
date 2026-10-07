import type { ReactNode } from 'react'
import { registerDetail } from '../../components/console/Drawer'
import { openPanel } from '../../components/console/nav'
import { BIG_HOST_CAP, hostActionDialog } from '../../components/console/hostActions'
import { Bars, Copy, Empty, Glyph, HostStatusChip, KV, Meter, Mini, Minis, Sec, Seg, Spark, Strip, TierTag } from '../../components/console/kit'
import type { BackpressureReason, Case, DomainRule, HostAction, HostDetail, Policy, RejectReason } from '../../lib/api'
import { ago, dt, fmtMs, fmtNum, fmtRatio, fmtSi, plural, shortDid } from '../../lib/console/fmt'
import { useCases, useHostDetail, useHostRow, usePolicy, useRules } from '../../lib/console/queries'
import { useRelay } from '../../lib/console/relay'
import { overriddenOn } from '../../lib/console/ruleScope'
import { Link } from '../../lib/router'
import { SourceTag } from './hostSource'
import { deleteRuleDialog, releaseDialog } from './moderationDetail'
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
const HELD = new Set(['throttled', 'backpressure', 'backoff', 'suspended', 'banned'])

/** What's full while the relay pauses a host, in a few words, for the Upstream panel. */
const BP_SHORT: Record<BackpressureReason, string> = {
  inflight_full: 'its in-flight cap',
  node_inflight_full: 'the node’s in-flight cap',
  queue_full: 'lane queue full',
}

/** Why-it's-held for a host the relay pauses: the title and what's going on, by what's full. */
const BP_WHY: Record<BackpressureReason | 'unknown', [string, string]> = {
  queue_full: [
    'Paused: the relay is behind on identity lookups, not this host’s limits',
    'Its lane queue is full: the relay’s lanes aren’t taking its frames as fast as it sends them, most often while they wait on DID document lookups. Its reader resumes on its own as they drain, and its PDS buffers meanwhile.',
  ],
  inflight_full: [
    'Paused: its frames are waiting on the relay, not on this host’s limits',
    'It has as many frames read and not yet durable as one host may (--host-inflight-events, --host-inflight-mb). Its reader resumes as they commit, and its PDS buffers meanwhile.',
  ],
  node_inflight_full: [
    'Paused: the relay is at its in-flight cap across every host',
    'This node has as many frames read and not yet durable as it allows over all its hosts (--inflight-events, --inflight-mb), so every host it reads waits for commits. The node is behind, not this host.',
  ],
  unknown: ['Paused by the relay, which is behind', 'Its reader resumes on its own once the relay catches up, and its PDS buffers meanwhile.'],
}

const by = (a: Acted) => `${a.by}, ${ago(a.atMs)}`
const ruleName = (r: DomainRule) => (
  <button type="button" className="cx-linklike" onClick={() => openPanel('rule', String(r.id))}>
    rule {r.id} (<span className="mono">{r.pattern}</span>)
  </button>
)
const ruleButtons = (r: DomainRule) => [
  <button key="edit" type="button" className="cx-btn sm" onClick={() => openPanel('rule', String(r.id))}>
    Edit rule ›
  </button>,
  <button key="rm" type="button" className="cx-btn sm danger" onClick={() => deleteRuleDialog(r, { stay: true })}>
    Remove rule…
  </button>,
]
const ruleSets = (r: DomainRule) => (r.effect.kind === 'ban' ? 'banned' : r.effect.kind === 'tier' ? r.effect.tier : r.effect.kind === 'throttle' ? `${fmtNum(r.effect.eventsPerSec)} events/s` : 'allowed')

/** After the rule that won for a host, the broader rule it took the host from. */
const Overrides = ({ over }: { over?: DomainRule }) =>
  over ? (
    <span className="muted">
      {' '}
      (overrides{' '}
      <button type="button" className="cx-linklike" onClick={() => openPanel('rule', String(over.id))}>
        rule {over.id}
      </button>
      )
    </span>
  ) : null

/** A host setting a domain rule decides: the rule wins, so the row says which rule and what it sets. */
function ByRule({ rule, over, children }: { rule: DomainRule; over?: DomainRule; children?: ReactNode }) {
  return (
    <span className="cx-byrule">
      <span>
        Set by {ruleName(rule)}{' '}
        <span className="nowrap">
          → <span className="to">{ruleSets(rule)}</span>
        </span>
        <Overrides over={over} />
      </span>
      {children}
    </span>
  )
}

/** Who holds the row's throttle: the operator, or a domain rule (some relays show a rule's rate there too). */
function throttleOf(d: HostDetail, rule?: DomainRule) {
  const r = d.row
  const thrSet = r.throttle != null ? [...d.actions].sort((a, b) => b.atMs - a.atMs).find((a) => a.action.action === 'throttle' && a.action.eventsPerSec != null) : undefined
  const ruleThr = rule?.effect.kind === 'throttle' ? rule.effect.eventsPerSec : undefined
  const opThr = r.throttle != null && (!!thrSet || ruleThr === undefined || r.throttle !== ruleThr)
  return { thrSet, ruleThr, opThr }
}

/** The rule that decides the host's tier: a tier rule, or a ban rule. `set-tier` against it is refused (409 TierSetByRule). */
const tierRuleOf = (rule?: DomainRule) => (rule && (rule.effect.kind === 'tier' || rule.effect.kind === 'ban') ? rule : undefined)

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
  const over = rule && rules ? overriddenOn(r.host, rule.id, rules) : undefined
  const { thrSet, ruleThr, opThr } = throttleOf(d, rule)
  const ruleTier = rule?.effect.kind === 'tier' ? rule.effect.tier : undefined
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

  let tone: 'info' | 'warn' | 'err' = 'warn'
  let title: ReactNode
  const lines: ReactNode[] = []
  const outs: ReactNode[] = []
  // a rule that sets something here is changed on the rule, never on the host
  const ruleOut = rule && rule.effect.kind !== 'allow' ? ruleButtons(rule) : []
  const ruleLine = (r: DomainRule, lead = 'Set') => (
    <>
      {lead} by {ruleName(r)} → {ruleSets(r)}
      <Overrides over={over} />
      {r.note ? <> “{r.note}”</> : null} · {r.createdBy}, {ago(r.createdAtMs)}
    </>
  )
  if (r.status === 'banned' || r.status === 'suspended') {
    tone = 'err'
    const act = last(r.status === 'banned' ? 'ban' : 'suspend')
    const byRule = r.status === 'banned' && rule?.effect.kind === 'ban'
    title = byRule ? <>Banned by domain rule {rule.id}</> : r.status === 'banned' ? 'Banned' : 'Suspended'
    if (byRule) lines.push(ruleLine(rule))
    if (act && (act.action.action === 'ban' || act.action.action === 'suspend')) lines.push(<>By {by(act)}: {act.action.reason}</>)
    else if (!byRule) lines.push(<span className="muted">No operator action on record says who.</span>)
    lines.push(r.status === 'banned' ? 'Its socket stays closed and its requestCrawl is refused.' : 'Its socket is closed and its cursor kept: resuming loses nothing.')
    if (!byRule)
      outs.push(
        <button key="unban" type="button" className="cx-btn sm primary" onClick={() => hostActionDialog('unban', r)}>
          {r.status === 'banned' ? 'Unban…' : 'Resume…'}
        </button>,
      )
    if (byRule) outs.push(...ruleOut)
  } else if (r.status === 'backpressure') {
    tone = 'info'
    const [t, what] = BP_WHY[r.backpressureReason ?? 'unknown']
    title = t
    lines.push(what)
    lines.push(r.throttle != null ? <>Its tier ({r.tier}) and throttle aren't what holds it: changing them won't release it.</> : <>Its tier ({r.tier}) isn't what holds it: changing it won't release it.</>)
    outs.push(
      <Link key="q" className="cx-btn sm" to="/admin/quorum">
        The relay's backlog ›
      </Link>,
    )
  } else if (r.status === 'backoff') {
    title = 'Backing off: its last connect failed'
    lines.push(`The reader retries on its own, waiting longer each time${r.connectedSinceMs ? `; it was last connected ${ago(r.connectedSinceMs)}` : ''}.`)
    outs.push(
      <button key="re" type="button" className="cx-btn sm" onClick={() => hostActionDialog('reconnect', r)}>
        Reconnect now…
      </button>,
    )
  } else {
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
      lines.push(ruleLine(rule, rule.effect.kind === 'tier' ? 'Tier set' : 'Throttle set'))
    if (ruleTier && r.tier !== ruleTier) lines.push(<>It's {r.tier} now, which the rule doesn't override; it goes back to {ruleTier} when released.</>)
    if (!ruleTier) {
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
    // a tier change only helps when the tier is what binds, and under a tier rule only the rule's tier lands
    if (ruleTier && r.tier !== ruleTier && r.throttle == null && ruleThr === undefined)
      outs.push(
        <button key="tier" type="button" className="cx-btn sm primary" onClick={() => hostActionDialog('settier', r, ruleTier)}>
          Release to {ruleTier}, the rule's tier…
        </button>,
      )
    else if (!ruleTier && roomier && r.throttle == null && ruleThr === undefined)
      outs.push(
        <button key="tier" type="button" className="cx-btn sm primary" onClick={() => hostActionDialog('settier', r, roomier[0])}>
          Set tier {roomier[0]} ({fmtNum(roomier[1].eventsPerSec)}/s)…
        </button>,
      )
    outs.push(...ruleOut)
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

/** An action row: what it does on the left, the control on the right (`wide`: the control below, for a long description). */
const Act = ({ title, desc, wide, children }: { title: string; desc: ReactNode; wide?: boolean; children: ReactNode }) => (
  <div className={`cx-act${wide ? ' wide' : ''}`}>
    <div className="ad">
      <b>{title}</b>
      {desc}
    </div>
    {children}
  </div>
)

function Body({ d, page }: { d: HostDetail; page: boolean }) {
  const { view } = useRelay()
  const pol = usePolicy()
  const rules = useRules()
  const cases = useCases()
  const r = d.row
  const live = r.status === 'connected' || r.status === 'throttled' || r.status === 'backpressure'
  const blocked = r.status === 'banned' || r.status === 'suspended'
  const tiers = Object.keys(pol.data?.policy.tiers ?? { [r.tier]: null })
  const rule = r.rule != null ? rules.data?.find((x) => x.id === r.rule) : undefined
  const over = rule && rules.data ? overriddenOn(r.host, rule.id, rules.data) : undefined
  const tierRule = tierRuleOf(rule)
  const banRule = rule?.effect.kind === 'ban' ? rule : undefined
  const thr = throttleOf(d, rule)
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
      <Sec
        title="Limits in force"
        digest={`${r.tier} tier${thr.opThr ? ` · operator throttle ${fmtNum(r.throttle!)}/s` : ''}${thr.ruleThr !== undefined ? ` · rule throttle ${fmtNum(thr.ruleThr)}/s` : ''}${ownCap ? ' · own account cap' : ''}`}
        open
        flush
      >
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
            [
              'Status',
              <span key="st">
                <HostStatusChip s={r.status} /> {r.status === 'backpressure' && r.backpressureReason ? <span className="muted sm">{BP_SHORT[r.backpressureReason]} </span> : null}
                {r.connectedSinceMs ? `since ${ago(r.connectedSinceMs)}` : ''}
              </span>,
            ],
            ['Reader', <NodeTag key="n" view={view} id={r.node} />],
            ['Tier', <TierTag key="t" t={r.tier} />],
            [
              'Domain rule',
              r.rule != null ? (
                <span key="r">
                  <button type="button" className="cx-linklike" onClick={() => openPanel('rule', String(r.rule))}>
                    rule {r.rule}
                  </button>
                  <Overrides over={over} />
                </span>
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
          <Act
            title="Tier"
            wide={!!tierRule}
            desc={
              tierRule ? (
                <ByRule rule={tierRule} over={over}>
                  {tierRule.effect.kind === 'tier' && r.tier !== tierRule.effect.tier && <span>It's {r.tier} now; the rule's tier applies when that's lifted.</span>}
                </ByRule>
              ) : (
                "Overrides the tier until it's changed again."
              )
            }
          >
            <span className="cx-form-row">
              <Seg
                label="Tier"
                value={tierRule?.effect.kind === 'tier' ? tierRule.effect.tier : banRule ? '' : r.tier}
                options={tiers.map((t) => ({ v: t, label: t }))}
                disabled={!!tierRule}
                title={tierRule ? `Set by rule ${tierRule.id} (${tierRule.pattern}): change it on the rule` : undefined}
                onChange={(t) => !tierRule && t !== r.tier && hostActionDialog('settier', r, t)}
              />
              {/* a ban rule's buttons are on the Ban row */}
              {tierRule && !banRule && ruleButtons(tierRule)}
            </span>
          </Act>
          <Act
            title="Throttle"
            wide={thr.ruleThr !== undefined}
            desc={
              thr.ruleThr !== undefined ? (
                <ByRule rule={rule!} over={over}>
                  <span>{thr.opThr ? `An operator throttle at ${fmtNum(r.throttle!)}/s holds too; the lower one wins.` : 'An operator throttle only holds below it.'}</span>
                </ByRule>
              ) : thr.opThr ? (
                `Operator throttle at ${fmtNum(r.throttle!)}/s.`
              ) : (
                'Hold its reader at a rate. The PDS buffers; nothing is dropped.'
              )
            }
          >
            <span className="cx-form-row">
              <button type="button" className="cx-btn sm" onClick={() => hostActionDialog('throttle', r)}>
                Throttle…
              </button>
              {thr.opThr && (
                <button type="button" className="cx-btn sm" onClick={() => hostActionDialog('unthrottle', r)}>
                  Lift
                </button>
              )}
              {thr.ruleThr !== undefined && ruleButtons(rule!)}
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
          {banRule && r.status !== 'suspended' ? (
            <Act title="Ban" wide desc={<ByRule rule={banRule} over={over} />}>
              <span className="cx-form-row">{ruleButtons(banRule)}</span>
            </Act>
          ) : blocked ? (
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
    const l = useHostDetail(id)
    // the newest row any answer carried (the list, an action, a change's hint): the drawer and the tables show the same one
    const row = useHostRow(id) ?? l.data?.row
    const d = l.data && row ? { ...l.data, row } : l.data
    const chip = row && (
      <>
        <HostStatusChip s={row.status} /> <TierTag t={row.tier} labeled />
        {row.pending && (
          <span className="cx-pending" title="The cluster hadn't confirmed the last action when it answered. A change says when it lands: reload the host before acting on it again.">
            applying…
          </span>
        )}
      </>
    )
    if (!d)
      return {
        title: row?.host ?? id,
        chip,
        body: null,
        loading: !l.error,
        missing: l.error ? `Couldn't load ${id}: ${l.error instanceof Error ? l.error.message : String(l.error)}` : undefined,
      }
    return {
      title: d.row.host,
      chip,
      fresh: l,
      foot: (
        <>
          read by <span className="mono">{d.row.node || '—'}</span> · GET /admin/api/hosts/{'{host}'}
        </>
      ),
      body: <Body d={d} page={mode === 'page'} />,
    }
  },
})
