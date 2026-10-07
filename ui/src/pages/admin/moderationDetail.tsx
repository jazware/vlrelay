import { useState, useSyncExternalStore, type ReactNode } from 'react'
import { confirmAction, FormDialog, openDialog } from '../../components/console/dialogs'
import { registerDetail } from '../../components/console/Drawer'
import { hostActionDialog, useHostsVersion, type HostVerb } from '../../components/console/hostActions'
import { closePanel, openPanel } from '../../components/console/nav'
import { toast } from '../../components/console/toast'
import { Chip, Copy, Empty, HostStatusChip, KV, Over, Sec, Strip, TierTag, type ChipKind } from '../../components/console/kit'
import { errText, type Account, type Case, type CaseStatus, type DomainRule, type DomainRuleInput, type RuleEffect, type Severity, type SignalKey } from '../../lib/api'
import * as A from '../../lib/console/adminAdapter'
import { ago, dt, fmtNum, plural, shortDid } from '../../lib/console/fmt'
import { createPoller, useLivePoll } from '../../lib/console/live'
import { openCasesPoll, policyPoll } from '../../lib/console/polls'
import '../../console-rules.css'

// The moderation detail kinds (case, account, domain rule) and every moderation write behind a
// confirm that shows the exact call: case status and notes, takedowns, domain rules.

export const rulesPoll = createPoller(A.domainRules, 10_000)
export const rulesAuditPoll = createPoller(A.domainRulesAudit, 30_000)
/** Every case, for the status counts; open ones also come from openCasesPoll. */
export const casesPoll = createPoller(() => A.cases(), 10_000)
/** Opens what a spam signal's key names: the host, or the account (a per-account signal's key is the DID). */
export const signalKeyRef = (per: string, k: SignalKey) => (per === 'account' && k.key.startsWith('did:') ? { type: 'acct', id: k.key } : { type: 'host', id: k.host || k.key })
export const openSignalKey = (per: string, k: SignalKey) => {
  const r = signalKeyRef(per, k)
  openPanel(r.type, r.id)
}

/** Every account under a takedown, newest first. */
export const takedownsPoll = createPoller(A.takedowns, 30_000)

// accounts re-read after a takedown lands
let acctVersion = 0
const acctSubs = new Set<() => void>()
const acctChanged = () => {
  acctVersion++
  takedownsPoll.refresh()
  acctSubs.forEach((l) => l())
}
export const useAcctVersion = () =>
  useSyncExternalStore(
    (l) => {
      acctSubs.add(l)
      return () => {
        acctSubs.delete(l)
      }
    },
    () => acctVersion,
  )

// ---------------------------------------------------------------- shared bits

export const cols = (page: boolean, a: ReactNode, b: ReactNode) =>
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

/** An action row: what it does on the left, the control on the right. */
export const Act = ({ title, desc, children }: { title: string; desc?: ReactNode; children: ReactNode }) => (
  <div className="cx-act">
    <div className="ad">
      <b>{title}</b>
      {desc}
    </div>
    {children}
  </div>
)

export const SEV_TONE: Record<Severity, ChipKind> = { critical: 'err', high: 'err', warn: 'warn', info: 'info' }
export const SEV_RANK: Record<Severity, number> = { critical: 3, high: 2, warn: 1, info: 0 }

export function CaseStatusChip({ c }: { c: Pick<Case, 'status' | 'severity'> }) {
  if (c.status === 'open') return <Chip k={SEV_TONE[c.severity] === 'err' ? 'err' : 'warn'}>open</Chip>
  if (c.status === 'acknowledged') return <Chip k="info">ack</Chip>
  return <Chip k="idle">{c.status}</Chip>
}

/** Observed against threshold, in the threshold's own unit. */
export function caseObs(c: Pick<Case, 'kind' | 'observed' | 'threshold'>): string {
  if (c.kind === 'reject-ratio') return `${Math.round(c.observed * 100)}% / ${Math.round(c.threshold * 100)}%`
  const d = c.threshold % 1 || c.threshold < 10 ? 1 : 0
  return `${fmtNum(c.observed, d)} / ${fmtNum(c.threshold, d)}`
}

const kindLabel = (k: string) => k.replace(/-/g, ' ')

export function AccountChip({ s }: { s: string }) {
  if (s === 'active') return <Chip k="ok">active</Chip>
  if (s === 'takendown') return <Chip k="err">taken down</Chip>
  if (s === 'throttled') return <Chip k="warn">throttled</Chip>
  return <Chip k="idle">{s}</Chip>
}

/** The same check the server makes: a hostname, or `*.` plus a domain (an IPv4 or localhost with a port for dev hosts). */
export function patternError(p: string): string | undefined {
  const s = p.trim().toLowerCase()
  if (!s) return 'Enter a hostname or *.domain'
  if (/^(\d{1,3}(\.\d{1,3}){3}|localhost)(:\d{1,5})?$/.test(s)) return undefined
  const base = s.startsWith('*.') ? s.slice(2) : s
  const ok = base.includes('.') && base.split('.').every((l) => l.length > 0 && l.length <= 63 && /^[a-z0-9-]+$/.test(l))
  return ok ? undefined : 'Use a hostname (pds.example.com) or *.example.com'
}
export const baseDomain = (p: string) => (p.startsWith('*.') ? p.slice(2) : p)
/** The registrable-looking tail of a host: pds.a.example.com → example.com. */
export const domainOf = (host: string) => (/^[\d.:]+$|^localhost/.test(host) ? host : host.split('.').slice(-2).join('.'))

export function EffectChip({ e }: { e: RuleEffect }) {
  if (e.kind === 'ban') return <Chip k="err">ban</Chip>
  if (e.kind === 'allow') return <Chip k="ok" title="Admitted when requestCrawl is allow-list only, without spending the new-hosts budget">allow</Chip>
  if (e.kind === 'tier')
    return (
      <span className="cx-chip plain">
        tier <TierTag t={e.tier} />
      </span>
    )
  return <Chip k="warn">throttle {fmtNum(e.eventsPerSec, e.eventsPerSec % 1 ? 1 : 0)}/s</Chip>
}

// ---------------------------------------------------------------- writes

const changedCases = () => {
  casesPoll.refresh()
  openCasesPoll.refresh()
}

const VERB: Record<CaseStatus, string> = { acknowledged: 'Acknowledge', resolved: 'Resolve', dismissed: 'Dismiss', open: 'Reopen' }

export function caseStatusDialog(c: Case, to: CaseStatus) {
  const needNote = to === 'resolved' || to === 'dismissed'
  return confirmAction({
    tone: 'warn',
    primary: true,
    title: `${VERB[to]} case ${c.id}?`,
    items: [
      to === 'acknowledged'
        ? 'It stays listed as being looked at; the open count stops counting it.'
        : to === 'resolved'
          ? 'Closes it. A new trip on the same host and kind opens a new case.'
          : to === 'dismissed'
            ? 'Closes it as a false positive. A new trip opens a new case.'
            : 'It goes back on the open list.',
      'Nothing changes on the host: bans, throttles and takedowns are their own actions.',
    ],
    fields: [{ id: 'note', label: needNote ? 'Note (required, kept on the case)' : 'Note (optional)', type: 'textarea', required: needNote, placeholder: to === 'dismissed' ? 'Real users: a migration wave' : 'Banned the domain' }],
    action: VERB[to],
    call: (v) => A.updateCaseCall(c.id, { status: to, note: String(v.note ?? '').trim() }),
    run: (v) => A.updateCase(c.id, { status: to, note: String(v.note ?? '').trim() }).then((r) => (changedCases(), r)),
    done: `Case ${c.id} ${to}`,
  })
}

function noteDialog(c: Case) {
  return confirmAction({
    tone: 'warn',
    primary: true,
    title: `Add a note to case ${c.id}`,
    items: ['Kept on the case with the time and who you signed in as. The status stays ' + c.status + '.'],
    fields: [{ id: 'note', label: 'Note', type: 'textarea', required: true }],
    action: 'Add note',
    call: (v) => A.updateCaseCall(c.id, { status: null, note: String(v.note ?? '').trim() }),
    run: (v) => A.updateCase(c.id, { status: null, note: String(v.note).trim() }).then((r) => (changedCases(), r)),
    done: 'Note added',
  })
}

export function takedownDialog(a: Pick<Account, 'did' | 'handle' | 'host'>) {
  const name = a.handle ?? shortDid(a.did)
  return confirmAction({
    tone: 'err',
    title: `Take down ${name}?`,
    items: [
      'Emits #account with status takendown, and its events stop going out on this relay at once.',
      'Replays skip its #commit and #sync frames, from the ring or the bucket; seqs aren’t renumbered.',
      `Its PDS (${a.host || 'its host'}) keeps the repo.`,
    ],
    fields: [{ id: 'reason', label: 'Reason (required, audited)', type: 'textarea', required: true, placeholder: 'case 1031: spam farm' }],
    word: a.handle ?? a.did,
    action: 'Take down',
    call: () => `POST /admin/api/accounts/${a.did}/takedown {"reason":…}`,
    run: (v) => A.takedown(a.did, String(v.reason).trim()).then((r) => (acctChanged(), r)),
    done: `Took down ${name}`,
  })
}

export function untakedownDialog(a: Pick<Account, 'did' | 'handle' | 'status'>) {
  const name = a.handle ?? shortDid(a.did)
  const lift = a.status === 'throttled'
  return confirmAction({
    tone: 'warn',
    primary: true,
    title: lift ? `Lift the throttle on ${name}?` : `Reverse the takedown of ${name}?`,
    items: ['Emits #account active; its next commits are accepted.', lift ? 'If its host is still at its cap, its next new accounts arrive throttled too.' : 'Events dropped while it was down aren’t replayed.'],
    action: lift ? 'Lift' : 'Reverse',
    call: `POST /admin/api/accounts/${a.did}/untakedown`,
    run: () => A.untakedown(a.did).then((r) => (acctChanged(), r)),
    done: lift ? `Lifted ${name}` : `Restored ${name}`,
  })
}

/** Lift every account a host created throttled past its cap. */
export function releaseDialog(host: string, atCap: boolean) {
  return confirmAction({
    tone: 'warn',
    primary: true,
    title: `Lift the throttled accounts on ${host}?`,
    items: [
      'Each gets an untakedown: #account active goes out and their next commits are accepted.',
      atCap ? 'The host is still at its cap, so new accounts keep arriving throttled. Raise the cap first.' : 'The host is under its cap now.',
    ],
    action: 'Lift accounts',
    call: `POST /admin/api/hosts/${host}/release-throttled`,
    run: () => A.releaseThrottled(host),
    done: (r) => `Lifted ${plural((r as { released: number }).released, 'account')}`,
  })
}

type Draft = { pattern: string; kind: RuleEffect['kind']; tier: string; eps: string; note: string }
const draftOf = (r?: DomainRule, pattern = '', tier = 'new'): Draft => ({
  pattern: r?.pattern ?? pattern,
  kind: r?.effect.kind ?? 'ban',
  tier: r?.effect.kind === 'tier' ? r.effect.tier : tier,
  eps: r?.effect.kind === 'throttle' ? String(r.effect.eventsPerSec) : '5',
  note: r?.note ?? '',
})
function inputOf(d: Draft): DomainRuleInput {
  const effect: RuleEffect = d.kind === 'ban' || d.kind === 'allow' ? { kind: d.kind } : d.kind === 'tier' ? { kind: 'tier', tier: d.tier } : { kind: 'throttle', eventsPerSec: Number(d.eps) }
  return { pattern: d.pattern.trim().toLowerCase(), effect, note: d.note.trim() }
}
function draftError(d: Draft): string | undefined {
  const pe = patternError(d.pattern)
  if (pe) return pe
  if (d.kind === 'throttle' && !(d.eps.trim() !== '' && Number.isFinite(Number(d.eps)) && Number(d.eps) >= 0)) return 'A throttle is a number of events per second, 0 or more'
  return undefined
}

function RuleDialog({ rule, pattern, close }: { rule?: DomainRule; pattern?: string; close: () => void }) {
  const pol = policyPoll.use().data
  const tiers = Object.keys(pol?.policy.tiers ?? {})
  const [d, setD] = useState<Draft>(() => draftOf(rule, pattern, tiers.includes('new') ? 'new' : (tiers[tiers.length - 1] ?? 'new')))
  const [tried, setTried] = useState(false)
  const [busy, setBusy] = useState(false)
  const [error, setError] = useState<unknown>()
  const err = draftError(d)
  const body = inputOf(d)
  const call = rule ? `PUT /admin/api/domain-rules/${rule.id} ${JSON.stringify(body)}` : `POST /admin/api/domain-rules ${JSON.stringify(body)}`
  return (
    <FormDialog
      title={rule ? `Edit rule ${rule.id}` : 'Add a domain rule'}
      action={rule ? 'Save rule' : 'Add rule'}
      busy={busy}
      error={error ?? (tried && err ? new Error(err) : undefined)}
      call={call}
      onCancel={close}
      onSubmit={async () => {
        setTried(true)
        if (err) return
        setBusy(true)
        setError(undefined)
        try {
          const r = rule ? await A.updateRule(rule.id, body) : await A.createRule(body)
          rulesPoll.refresh()
          rulesAuditPoll.refresh()
          close()
          openPanel('rule', String(r.id))
        } catch (e) {
          setError(e)
        } finally {
          setBusy(false)
        }
      }}
    >
      <div>
        <label className="cx-lbl" htmlFor="rule-p">
          Pattern: a hostname, or *. and a domain for it and every subdomain
        </label>
        <input id="rule-p" className="cx-inp mono" autoFocus autoComplete="off" spellCheck={false} placeholder="*.example.com" value={d.pattern} onChange={(e) => setD({ ...d, pattern: e.target.value })} />
      </div>
      <div className="cx-form-row">
        <select className="cx-inp" aria-label="Effect" value={d.kind} onChange={(e) => setD({ ...d, kind: e.target.value as Draft['kind'] })}>
          <option value="ban">Ban</option>
          <option value="allow">Allow</option>
          <option value="tier">Set tier</option>
          <option value="throttle">Throttle</option>
        </select>
        {d.kind === 'tier' && (
          <select className="cx-inp" aria-label="Tier" value={d.tier} onChange={(e) => setD({ ...d, tier: e.target.value })}>
            {(tiers.length ? tiers : [d.tier]).map((t) => (
              <option key={t}>{t}</option>
            ))}
          </select>
        )}
        {d.kind === 'throttle' && (
          <>
            <input className="cx-inp num" inputMode="decimal" aria-label="Events per second" value={d.eps} onChange={(e) => setD({ ...d, eps: e.target.value })} />
            <span className="muted sm">events/s</span>
          </>
        )}
      </div>
      <ul>
        {d.kind === 'ban' && <li>Matching hosts are disconnected within about a second and their requestCrawl is refused.</li>}
        {d.kind === 'allow' && <li>Matching hosts get in when requestCrawl is allow-list only, without spending the new-hosts budget.</li>}
        {d.kind === 'tier' && <li>Matching hosts take the {d.tier} tier's limits, now and when they're crawled.</li>}
        {d.kind === 'throttle' && <li>Matching hosts' readers are held at that rate; their PDSes buffer instead of the relay dropping.</li>}
        <li>Every node reloads the rules within 10 s.</li>
      </ul>
      <div>
        <label className="cx-lbl" htmlFor="rule-n">
          Note (why)
        </label>
        <input id="rule-n" className="cx-inp" autoComplete="off" value={d.note} onChange={(e) => setD({ ...d, note: e.target.value })} />
      </div>
    </FormDialog>
  )
}

export const ruleDialog = (rule?: DomainRule, pattern?: string) => openDialog((close) => <RuleDialog rule={rule} pattern={pattern} close={close} />)

export function deleteRuleDialog(r: DomainRule) {
  return confirmAction({
    tone: 'err',
    title: `Delete rule ${r.id} (${r.pattern})?`,
    items: [
      r.effect.kind === 'ban' ? `${plural(r.matches, 'banned host')} may connect again on ${r.matches === 1 ? 'its' : 'their'} next requestCrawl.` : `${plural(r.matches, 'host')} ${r.matches === 1 ? 'goes' : 'go'} back to ${r.matches === 1 ? 'its' : 'their'} own tier and limits.`,
      'Hosts it already changed keep their state until they’re retiered or unbanned on the host.',
      'Every node reloads the rules within 10 s.',
    ],
    word: r.pattern,
    action: 'Delete rule',
    call: `DELETE /admin/api/domain-rules/${r.id}`,
    run: async () => {
      await A.deleteRule(r.id)
      rulesPoll.refresh()
      rulesAuditPoll.refresh()
      closePanel()
    },
    done: `Deleted rule ${r.id}`,
  })
}

/** A host verb from a case or rule: the host's row first, so the confirm can say what changes. */
async function onHost(host: string, verb: HostVerb, arg?: string) {
  try {
    const d = await A.host(host)
    await hostActionDialog(verb, d.row, arg)
  } catch (e) {
    toast(`Couldn't load ${host}: ${errText(e)}`, { err: true })
  }
}

// ---------------------------------------------------------------- case

function CaseBody({ c, page }: { c: Case; page: boolean }) {
  const ev = useLivePoll(() => A.caseEvidence(c.id), String(c.id), 10_000)
  const d = ev.data?.supported ? ev.data.data : undefined
  const trips = d ? [...d.evidence].reverse() : []
  const dom = domainOf(c.host)
  const main = (
    <>
      <Strip
        items={[
          ['observed / threshold', caseObs(c)],
          ['over by', c.threshold > 0 ? `${fmtNum(c.observed / c.threshold, 1)}×` : '—'],
          ['auto action', c.autoAction ?? 'none'],
          ['opened', ago(c.openedAtMs)],
          ['trips', d ? fmtNum(d.trips) : '—'],
        ]}
      />
      <p className="cx-lede">
        {c.summary} on{' '}
        <button type="button" className="cx-linklike mono" onClick={() => openPanel('host', c.host)}>
          {c.host}
        </button>
        {c.did && (
          <>
            {' '}
            · account{' '}
            <button type="button" className="cx-linklike mono" onClick={() => openPanel('acct', c.did!)}>
              {shortDid(c.did)}
            </button>
          </>
        )}
        .
      </p>
      <Sec title="Evidence" digest={d ? (d.evidence.length && d.trips > d.evidence.length ? `newest ${d.evidence.length} of ${fmtNum(d.trips)} trips` : plural(d.trips, 'trip')) : 'every signal at each trip'} open flush>
        {ev.data && !ev.data.supported ? (
          <Empty>This relay keeps no evidence per case.</Empty>
        ) : !trips.length ? (
          <Empty>{d ? 'No measurements recorded for this case.' : 'Loading…'}</Empty>
        ) : (
          <div className="cx-tw">
            <table className="cx-t compact">
              <thead>
                <tr>
                  <th>Trip</th>
                  <th className="r">Observed</th>
                  <th className="r">Window</th>
                  <th>Node</th>
                  <th>Signals then</th>
                </tr>
              </thead>
              <tbody>
                {trips.map((t, i) => (
                  <tr key={`${t.atMs}-${i}`}>
                    <td className="sm muted" title={dt(t.atMs)}>
                      {ago(t.atMs)}
                    </td>
                    <td className="r">
                      <Over r={t.threshold > 0 ? t.observed / t.threshold : NaN} detail={caseObs({ kind: c.kind, observed: t.observed, threshold: t.threshold })} />
                    </td>
                    <td className="r mono sm">{t.windowSecs} s</td>
                    <td className="mono sm">{t.node}</td>
                    <td style={{ whiteSpace: 'normal', minWidth: 200 }}>
                      {t.detail && <div className="sm t2">{t.detail}</div>}
                      <span className="cx-signals">
                        {Object.entries(t.signals).map(([k, v]) => (
                          <span key={k}>
                            {k} {Number.isInteger(v) ? fmtNum(v) : v.toFixed(3)}
                          </span>
                        ))}
                      </span>
                    </td>
                  </tr>
                ))}
              </tbody>
            </table>
          </div>
        )}
      </Sec>
      <Sec title="Notes" digest={plural(c.notes.length, 'note')} open flush right={<button type="button" className="cx-btn sm" onClick={(e) => (e.preventDefault(), noteDialog(c))}>Add…</button>}>
        {c.notes.length ? (
          c.notes.map((n, i) => (
            <div key={i} className="cx-note">
              <span className="by">{n.by}</span>
              <span>{n.text}</span>
              <span className="x" title={dt(n.atMs)}>
                {ago(n.atMs)}
              </span>
            </div>
          ))
        ) : (
          <Empty>No notes yet.</Empty>
        )}
      </Sec>
    </>
  )
  const next: CaseStatus[] = c.status === 'open' ? ['acknowledged', 'resolved', 'dismissed'] : c.status === 'acknowledged' ? ['resolved', 'dismissed', 'open'] : ['open']
  const acts = (
    <Sec title="Actions" digest="each one confirms first" open flush>
      <div className="cx-acts">
        <Act title="Status" desc={`Now ${c.status}.`}>
          <span className="cx-form-row">
            {next.map((s) => (
              <button key={s} type="button" className="cx-btn sm" onClick={() => caseStatusDialog(c, s)}>
                {VERB[s]}…
              </button>
            ))}
          </span>
        </Act>
        <Act title="The host" desc={<span className="mono sm">{c.host}</span>}>
          <span className="cx-form-row">
            <button type="button" className="cx-btn sm" onClick={() => onHost(c.host, 'throttle')}>
              Throttle…
            </button>
            <button type="button" className="cx-btn sm danger" onClick={() => onHost(c.host, 'suspend')}>
              Suspend…
            </button>
          </span>
        </Act>
        <Act title="The domain" desc="Catch every host on it as a group.">
          <button type="button" className="cx-btn sm danger" onClick={() => ruleDialog(undefined, /^[\d.:]+$|^localhost/.test(dom) ? dom : `*.${dom}`)}>
            Ban {/^[\d.:]+$|^localhost/.test(dom) ? dom : `*.${dom}`}…
          </button>
        </Act>
        {c.did && (
          <Act title="The account" desc={<span className="mono sm">{shortDid(c.did)}</span>}>
            <button type="button" className="cx-btn sm danger" onClick={() => takedownDialog({ did: c.did!, handle: null, host: c.host })}>
              Take down…
            </button>
          </Act>
        )}
      </div>
    </Sec>
  )
  return cols(page, main, acts)
}

registerDetail('case', {
  kind: 'Case',
  section: 'moderation',
  use: (id, mode) => {
    const all = casesPoll.use()
    const l = useLivePoll(() => A.caseOf(Number(id)), id, 5000)
    const fromAll = all.data?.find((x) => String(x.id) === id)
    // the list refreshes right after an action; the case's own poll may be a tick behind
    const c = l.data && fromAll ? (fromAll.updatedAtMs > l.data.updatedAtMs ? fromAll : l.data) : (l.data ?? fromAll)
    if (!c) return { title: `Case ${id}`, body: null, loading: !l.error, missing: l.error ? `Couldn't load case ${id}: ${l.error instanceof Error ? l.error.message : String(l.error)}` : undefined }
    return {
      title: `Case ${c.id} · ${kindLabel(c.kind)}`,
      chip: (
        <>
          <Chip k={SEV_TONE[c.severity]}>{c.severity}</Chip> <CaseStatusChip c={c} />
        </>
      ),
      foot: <>GET /admin/api/cases/{'{id}'} · cases/{'{id}'}/evidence</>,
      body: <CaseBody c={c} page={mode === 'page'} />,
    }
  },
})

// ---------------------------------------------------------------- account

function AcctBody({ a, page }: { a: Account; page: boolean }) {
  const down = a.status === 'takendown' || a.status === 'throttled'
  const main = (
    <>
      <Strip
        items={[
          ['events/h', fmtNum(a.eventsLastHour)],
          ['rejects/h', fmtNum(a.rejectsLastHour)],
          ['last seq', fmtNum(a.lastSeq)],
          ['last event', a.lastEventMs ? ago(a.lastEventMs) : '—'],
        ]}
      />
      <Sec title="Account" open>
        <KV
          rows={[
            ['DID', <Copy key="d" text={a.did} />],
            ['Handle', a.handle ?? <span className="muted">none cached</span>],
            [
              'Host',
              <button key="h" type="button" className="cx-linklike mono" onClick={() => openPanel('host', a.host)}>
                {a.host}
              </button>,
            ],
            ['Status here', <AccountChip key="s" s={a.status} />],
            ['Upstream status', a.upstreamStatus],
            ['Rev', <span key="r" className="mono">{a.rev || '—'}</span>],
            ['DID shard', <span key="sh" className="mono sm">{a.didShard} on {a.node || '—'}</span>],
          ]}
        />
      </Sec>
    </>
  )
  const side = (
    <>
      {a.takedown && (
        <Sec title="Takedown" open danger>
          <KV
            rows={[
              ['By', a.takedown.by],
              ['When', dt(a.takedown.atMs)],
              ['Reason', a.takedown.reason],
              ['Replays', "its #commit and #sync frames are skipped on every node; #account and #identity pass"],
            ]}
          />
        </Sec>
      )}
      <Sec title="Actions" open flush danger={!down}>
        <div className="cx-acts">
          {down ? (
            <Act title={a.status === 'throttled' ? 'Lift the throttle' : 'Reverse the takedown'} desc="Emits #account active; its next commits are accepted.">
              <button type="button" className="cx-btn sm" onClick={() => untakedownDialog(a)}>
                {a.status === 'throttled' ? 'Lift…' : 'Reverse…'}
              </button>
            </Act>
          ) : (
            <Act title="Take down" desc="Emits #account takendown and filters its commits from replays.">
              <button type="button" className="cx-btn sm danger" onClick={() => takedownDialog(a)}>
                Take down…
              </button>
            </Act>
          )}
        </div>
      </Sec>
    </>
  )
  return cols(page, main, side)
}

registerDetail('acct', {
  kind: 'Account',
  section: 'moderation',
  use: (did, mode) => {
    const v = useAcctVersion()
    const l = useLivePoll(() => A.account(did), `${did}#${v}`, 10_000, { keep: true })
    const a = l.data
    if (!a) return { title: shortDid(did), body: null, loading: !l.error, missing: l.error ? `Couldn't load ${did}: ${l.error instanceof Error ? l.error.message : String(l.error)}` : undefined }
    return {
      title: a.handle ?? shortDid(a.did),
      chip: <AccountChip s={a.status} />,
      foot: <>state on the DID shard's owner ({a.node || 'this node'}) · GET /admin/api/accounts/{'{did}'}</>,
      body: <AcctBody a={a} page={mode === 'page'} />,
    }
  },
})

// ---------------------------------------------------------------- domain rule

function RuleBody({ r }: { r: DomainRule }) {
  const hv = useHostsVersion()
  const hosts = useLivePoll(() => A.hosts({ q: baseDomain(r.pattern), sort: 'events', desc: true, limit: 200 }), `${r.id}#${r.pattern}#${hv}`, 15_000)
  const m = (hosts.data?.hosts ?? []).filter((h) => h.rule === r.id)
  return (
    <>
      <KV
        rows={[
          ['Effect', <EffectChip key="e" e={r.effect} />],
          ['Note', r.note || <span className="muted">none</span>],
          ['Added', `${dt(r.createdAtMs)} by ${r.createdBy}`],
          ['Matches', fmtNum(r.matches)],
        ]}
      />
      <Sec title="Matching hosts" digest={hosts.data ? plural(m.length, 'host') : '…'} open flush>
        {m.length ? (
          m.slice(0, 20).map((h) => (
            <button key={h.host} type="button" className="cx-rrow" onClick={() => openPanel('host', h.host)}>
              <span className="mono sm">{h.host}</span>
              <span className="x">
                <HostStatusChip s={h.status} />
              </span>
            </button>
          ))
        ) : (
          <Empty>{hosts.data ? 'No known host matches yet.' : 'Loading…'}</Empty>
        )}
      </Sec>
      <div className="cx-form-row">
        <button type="button" className="cx-btn" onClick={() => ruleDialog(r)}>
          Edit…
        </button>
        <button type="button" className="cx-btn danger" onClick={() => deleteRuleDialog(r)}>
          Delete…
        </button>
      </div>
    </>
  )
}

registerDetail('rule', {
  kind: 'Domain rule',
  section: 'moderation',
  use: (id) => {
    const l = rulesPoll.use()
    const r = l.data?.find((x) => String(x.id) === id)
    if (!r) return { title: `Rule ${id}`, body: null, loading: l.loading, missing: l.data ? `There's no rule ${id} (deleted?).` : l.error ? String(l.error) : undefined }
    return { title: r.pattern, chip: <EffectChip e={r.effect} />, foot: <>GET /admin/api/domain-rules</>, body: <RuleBody r={r} /> }
  },
})
