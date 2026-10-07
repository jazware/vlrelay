import { useEffect, useMemo, useState } from 'react'
import { DataTable, type Col } from '../../components/console/DataTable'
import { hostActionDialog, BIG_HOST_CAP } from '../../components/console/hostActions'
import { openPanel } from '../../components/console/nav'
import { registerPalette, type PalItem } from '../../components/console/Palette'
import { Chip, Empty, Glyph, HostName, Kbd, Loaded, Meter, Over, PageHead, Panel, Sec, Seg, Src, TierTag, Updated } from '../../components/console/kit'
import type { Account, Case, CaseStatus, DomainRule, HostRow, PolicyAudit, SignalKey, SignalTop, TakedownEntry } from '../../lib/api'
import { cached, keys } from '../../lib/console/cache'
import { ago, dt, fmtNum, fmtSi, plural, shortDid } from '../../lib/console/fmt'
import { useAccountSearch, useCases, useHostList, useOverview, useRules, useRulesAudit, useSignals, useTakedowns } from '../../lib/console/queries'
import { overriddenBy, overriddenRules } from '../../lib/console/ruleScope'
import { sevTone } from '../../lib/console/tone'
import { Link, useSearch } from '../../lib/router'
import { signalOfKind } from './Policy'
import {
  AccountChip,
  CaseStatusChip,
  EffectChip,
  SEV_RANK,
  caseObs,
  signalKeyRef,
  releaseDialog,
  ruleDialog,
  takedownDialog,
} from './moderationDetail'

// Cases, the spam signals' heaviest keys, accounts and every takedown, domain rules and the
// accounts hosts created throttled. Every row opens in the slide-over (case, acct, rule, host);
// every write is a confirm with its call.

type Filter = CaseStatus | 'all'
const FILTERS: Filter[] = ['open', 'acknowledged', 'resolved', 'dismissed', 'all']
const isDid = (s: string) => /^did:(plc|web):\S+$/.test(s.trim())

function setParam(k: string, v: string) {
  const s = new URLSearchParams(location.search)
  if (v) s.set(k, v)
  else s.delete(k)
  const q = s.toString()
  history.replaceState(null, '', `${location.pathname}${q ? `?${q}` : ''}${location.hash}`)
  dispatchEvent(new PopStateEvent('popstate'))
}

function Cases() {
  const all = useCases()
  const search = useSearch()
  const f = (search.get('cases') as Filter | null) ?? 'open'
  const counts = useMemo(() => {
    const m: Record<string, number> = { all: 0 }
    for (const c of all.data ?? []) {
      m[c.status] = (m[c.status] ?? 0) + 1
      m.all++
    }
    return m
  }, [all.data])
  const rows = useMemo(
    () => (all.data ?? []).filter((c) => f === 'all' || c.status === f).sort((a, b) => SEV_RANK[b.severity] - SEV_RANK[a.severity] || b.openedAtMs - a.openedAtMs),
    [all.data, f],
  )
  const cols: Col<Case>[] = [
    {
      id: 'id',
      label: 'Case',
      sort: (a, b) => a.id - b.id,
      render: (c) => (
        <span className="mono">
          <Glyph k={sevTone(c.severity)} title={c.severity} /> {c.id}
        </span>
      ),
    },
    { id: 'kind', label: 'Kind', render: (c) => c.kind.replace(/-/g, ' ') },
    {
      id: 'subject',
      label: 'Subject',
      render: (c) => (
        <span className="cx-cellid">
          <HostName host={c.host} />
          {c.did && <span className="cx-did">{shortDid(c.did)}</span>}
        </span>
      ),
    },
    {
      id: 'obs',
      label: 'Over threshold',
      r: true,
      title: 'How far past its threshold it was when it tripped: observed / threshold. The bar is logarithmic, 0.1× to 10×, with a tick at the threshold',
      sort: (a, b) => a.observed / (a.threshold || 1) - b.observed / (b.threshold || 1),
      render: (c) => <Over r={c.threshold > 0 ? c.observed / c.threshold : NaN} detail={caseObs(c)} />,
    },
    { id: 'auto', label: 'Auto action', render: (c) => (c.autoAction ? <span className="mono sm">{c.autoAction}</span> : <span className="muted sm">none</span>) },
    { id: 'status', label: 'Status', render: (c) => <CaseStatusChip c={c} /> },
    { id: 'opened', label: 'Opened', r: true, sort: (a, b) => a.openedAtMs - b.openedAtMs, render: (c) => <span className="sm muted" title={dt(c.openedAtMs)}>{ago(c.openedAtMs)}</span> },
  ]
  return (
    <Panel
      title="Cases"
      src={<><Src>cases</Src> <Src>cases/{'{id}'}/evidence</Src></>}
      right={<Seg<Filter> label="Case status" value={f} options={FILTERS.map((s) => ({ v: s, label: s === 'acknowledged' ? 'ack' : s, n: counts[s] ?? 0 }))} onChange={(v) => setParam('cases', v === 'open' ? '' : v)} />}
      foot={<span>Opened when a host or account crosses a spam threshold in the policy. A host gets at most one open case per kind.</span>}
    >
      <Loaded load={all}>
        {() => (
          <DataTable
            rows={rows}
            cols={cols}
            rowKey={(c) => String(c.id)}
            open={(c) => ({ type: 'case', id: String(c.id) })}
            dim={(c) => c.status === 'resolved' || c.status === 'dismissed'}
            compact
            label="Cases"
            empty={<Empty title={f === 'open' ? 'No open cases' : `No ${f === 'all' ? '' : `${f} `}cases`}>Nothing has crossed a threshold.</Empty>}
          />
        )}
      </Loaded>
    </Panel>
  )
}

type SignalRow = { s: SignalTop; k?: SignalKey; r: number }

function Signals() {
  const sig = useSignals()
  const v = sig.data
  const rows: SignalRow[] = (v?.signals ?? [])
    .filter((s) => s.enabled && s.limit > 0)
    .map((s) => ({ s, k: s.top[0], r: s.top[0] ? s.top[0].estimate / s.limit : 0 }))
    .sort((a, b) => b.r - a.r)
  const off = (v?.signals ?? []).filter((s) => !s.enabled || s.limit <= 0).length
  const cols: Col<SignalRow>[] = [
    {
      id: 'sig',
      label: 'Signal',
      render: ({ s }) => (
        <span className="sm">
          {signalOfKind(s.rule)?.label ?? s.rule.replace(/-/g, ' ')} <span className="muted">{s.per}</span>
        </span>
      ),
    },
    {
      id: 'key',
      label: 'Heaviest',
      render: ({ s, k }) =>
        !k ? <span className="muted sm">nothing counted</span> : s.per === 'account' && k.key.startsWith('did:') ? <span className="cx-did" title={k.key}>{shortDid(k.key)}</span> : <HostName host={k.host || k.key} />,
    },
    {
      id: 'vs',
      label: 'vs threshold',
      r: true,
      sort: (a, b) => a.r - b.r,
      title: 'Its estimate against the threshold over the window (Space-Saving: it may overcount down to its lower bound). 1× trips it; the bar is logarithmic, 0.1× to 10×',
      render: ({ s, k, r }) =>
        k ? (
          <Over r={r} title={`~${fmtNum(k.estimate)} (at least ${fmtNum(k.lower)}) of ${fmtNum(s.limit)} in ${s.windowSecs} s`} />
        ) : (
          <span className="muted">—</span>
        ),
    },
  ]
  return (
    <Panel
      title="Spam signals, live"
      to="/admin/policy"
      src={<Src>policy/signals</Src>}
      right={v ? <span className="muted sm">on {v.node}</span> : undefined}
      foot={<span>Each signal's heaviest key against its threshold; 1× trips it. Open a row for the host or account.{off ? ` ${plural(off, 'signal')} off (threshold 0).` : ''}</span>}
    >
      <Loaded load={sig}>
        {() => (
          <DataTable
            rows={rows}
            cols={cols}
            rowKey={(x) => x.s.rule}
            open={({ s, k }) => (k ? signalKeyRef(s.per, k) : undefined)}
            compact
            label="Spam signals"
            empty={<Empty>Every signal is off.</Empty>}
          />
        )}
      </Loaded>
    </Panel>
  )
}

function Takedowns() {
  const l = useTakedowns()
  const cols: Col<TakedownEntry>[] = [
    { id: 'did', label: 'Account', render: (t) => <span className="cx-did" title={t.did}>{shortDid(t.did)}</span> },
    { id: 'reason', label: 'Reason', className: 'wrap sm t2', render: (t) => t.reason || <span className="muted">—</span> },
    { id: 'by', label: 'By', render: (t) => <span className="sm">{t.by}</span> },
    { id: 'at', label: 'When', r: true, sort: (a, b) => a.atMs - b.atMs, render: (t) => <span className="sm muted" title={dt(t.atMs)}>{ago(t.atMs)}</span> },
  ]
  return (
    <Panel
      title="Takedowns"
      src={<Src>takedowns</Src>}
      right={l.data ? <span className="muted sm">{plural(l.data.length, 'account')}</span> : undefined}
      foot={<span>Open one to reverse it. A takedown stops the account's events here; its PDS keeps the repo.</span>}
    >
      <Loaded load={l}>
        {(rows) => (
          <DataTable
            rows={rows}
            cols={cols}
            rowKey={(t) => t.did}
            open={(t) => ({ type: 'acct', id: t.did })}
            dim={(t) => !t.takedown}
            compact
            label="Takedowns"
            sort={{ id: 'at', asc: false }}
            empty={<Empty>No account is taken down on this relay.</Empty>}
          />
        )}
      </Loaded>
    </Panel>
  )
}

function Lookup() {
  const search = useSearch()
  const q = (search.get('q') ?? '').trim()
  const [text, setText] = useState(q)
  useEffect(() => setText(q), [q])
  const l = useAccountSearch(q)
  const cols: Col<Account>[] = [
    { id: 'who', label: 'Account', render: (a) => (a.handle ? <span className="mono sm">{a.handle}</span> : <span className="cx-did">{shortDid(a.did)}</span>) },
    { id: 'host', label: 'Host', render: (a) => <span className="mono sm t2 trunc" style={{ display: 'inline-block', maxWidth: 160 }}>{a.host}</span> },
    { id: 'status', label: 'Status', render: (a) => <AccountChip s={a.status} /> },
    { id: 'evh', label: 'Events/h', r: true, render: (a) => <span className="mono sm">{fmtNum(a.eventsLastHour)}</span> },
  ]
  return (
    <Panel
      title="Look up an account"
      src={<><Src>accounts?q=</Src> <Src>accounts/{'{did}'}</Src></>}
    >
      <form
        className="cx-pn-b cx-form-row"
        onSubmit={(e) => {
          e.preventDefault()
          const v = text.trim()
          setParam('q', v)
          if (isDid(v)) openPanel('acct', v)
        }}
      >
        <input className="cx-inp mono" data-search placeholder="did:plc:…, handle, or prefix*" spellCheck={false} autoComplete="off" aria-label="DID or handle" value={text} onChange={(e) => setText(e.target.value)} />
        <button className="cx-btn primary">Look up</button>
      </form>
      {q ? (
        <Loaded load={l}>
          {(rows) => (
            <DataTable
              rows={rows}
              cols={cols}
              rowKey={(a) => a.did}
              open={(a) => ({ type: 'acct', id: a.did })}
              compact
              label="Accounts"
              empty={<Empty title="No account matches">A full DID or handle finds any account the relay has seen; end a handle with * for a prefix.</Empty>}
            />
          )}
        </Loaded>
      ) : (
        <Empty>A DID opens the account; a handle or a prefix ending in * lists matches. Takedowns are on the account.</Empty>
      )}
    </Panel>
  )
}

/** A rule's place among the others: the hosts more specific rules take from it, and the broader rule it wins over. */
function Precedence({ r, rules }: { r: DomainRule; rules: DomainRule[] }) {
  const by = overriddenBy(r, rules)
  const lost = by.reduce((n, s) => n + s.matches, 0)
  const over = overriddenRules(r, rules)[0]
  const parts = [
    ...(by.length ? [`${plural(lost, 'host')} to ${by.length === 1 ? `rule ${by[0].id}` : plural(by.length, 'rule')}`] : []),
    ...(over ? [`overrides rule ${over.id}`] : []),
  ]
  if (!parts.length) return <span className="muted">—</span>
  const title = [...by.map((s) => `rule ${s.id} (${s.pattern}) takes ${plural(s.matches, 'host')}`), ...(over ? [`wins over rule ${over.id} (${over.pattern}) on its hosts`] : [])].join('\n')
  return <span title={title}>{parts.join(' · ')}</span>
}

function Rules() {
  const l = useRules()
  const audit = useRulesAudit()
  const [f, setF] = useState('')
  const rq = f.trim().toLowerCase()
  const rows = (l.data ?? []).filter((r) => !rq || r.pattern.includes(rq) || r.note.toLowerCase().includes(rq))
  const cols: Col<DomainRule>[] = [
    { id: 'id', label: '#', sort: (a, b) => a.id - b.id, render: (r) => <span className="mono muted">{r.id}</span> },
    { id: 'pattern', label: 'Pattern', sort: (a, b) => a.pattern.localeCompare(b.pattern), render: (r) => <span className="mono">{r.pattern}</span> },
    { id: 'effect', label: 'Effect', render: (r) => <EffectChip e={r.effect} /> },
    { id: 'note', label: 'Note', className: 'wrap sm t2', render: (r) => r.note || <span className="muted">—</span> },
    { id: 'matches', label: 'Covers', r: true, sort: (a, b) => a.matches - b.matches, render: (r) => <span className="mono">{fmtNum(r.matches)}</span> },
    { id: 'prec', label: 'Precedence', className: 'sm t2', render: (r) => <Precedence r={r} rules={l.data ?? []} /> },
    { id: 'by', label: 'By', render: (r) => <span className="sm">{r.createdBy}</span> },
    { id: 'at', label: 'Added', r: true, sort: (a, b) => a.createdAtMs - b.createdAtMs, render: (r) => <span className="sm muted" title={dt(r.createdAtMs)}>{ago(r.createdAtMs)}</span> },
  ]
  const latest: PolicyAudit | undefined = audit.data?.[0]
  return (
    <Panel
      id="rules"
      title="Domain rules"
      src={<><Src>domain-rules</Src> <Src>domain-rules/audit</Src></>}
      right={
        <>
          <input className="cx-inp mono" style={{ height: 26, width: 160 }} placeholder="filter" aria-label="Filter rules" value={f} onChange={(e) => setF(e.target.value)} />
          <button type="button" className="cx-btn sm" onClick={() => ruleDialog()}>
            Add…
          </button>
        </>
      }
      foot={<span>{latest ? `Rules version ${latest.version} · ` : ''}a ban disconnects a connected host within about a second. Allow admits a host when requestCrawl is allow-list only.</span>}
    >
      <Loaded load={l}>
        {() => (
          <DataTable
            rows={rows}
            cols={cols}
            rowKey={(r) => String(r.id)}
            open={(r) => ({ type: 'rule', id: String(r.id) })}
            compact
            label="Domain rules"
            sort={{ id: 'id', asc: true }}
            empty={<Empty title={rq ? 'No rule matches' : 'No domain rules'}>{rq ? 'Clear the filter.' : 'Every host gets the policy’s initial tier.'}</Empty>}
          />
        )}
      </Loaded>
      <Sec title="History" digest={audit.data ? plural(audit.data.length, 'change') : '…'} flush>
        {audit.data?.length ? (
          <div className="cx-tw">
            <table className="cx-t compact">
              <tbody>
                {audit.data.slice(0, 50).map((a) => (
                  <tr key={a.version}>
                    <td className="mono sm">v{a.version}</td>
                    <td className="sm">{a.by}</td>
                    <td className="mono sm t2" style={{ whiteSpace: 'normal' }}>
                      {a.changes.join(' · ') || a.note}
                    </td>
                    <td className="r sm muted" title={dt(a.atMs)}>
                      {ago(a.atMs)}
                    </td>
                  </tr>
                ))}
              </tbody>
            </table>
          </div>
        ) : (
          <Empty>No rule changes recorded.</Empty>
        )}
      </Sec>
    </Panel>
  )
}

const SHOWN = 10

/** The hosts with accounts created throttled, or at their cap (the next ones will be). */
function Throttled() {
  const list = useHostList({ flag: 'throttledOrAtCap', sort: 'accounts', desc: true }, { poll: 30_000, keep: true })
  const atCap = (h: HostRow) => h.maxAccounts > 0 && h.accounts >= h.maxAccounts
  const rows = [...(list.data?.hosts ?? [])].sort((a, b) => b.throttledAccounts - a.throttledAccounts || b.accounts - a.accounts)
  const total = rows.reduce((a, h) => a + h.throttledAccounts, 0)
  const capped = rows.filter(atCap).length
  const cols: Col<HostRow>[] = [
    {
      id: 'host',
      label: 'Host',
      render: (h) => (
        <span className="cx-cellid">
          <HostName host={h.host} />
          <TierTag t={h.tier} />
        </span>
      ),
    },
    { id: 'thr', label: 'Throttled accounts', r: true, sort: (a, b) => a.throttledAccounts - b.throttledAccounts, render: (h) => <span className={`mono${h.throttledAccounts ? ' s-warn' : ' muted'}`}>{fmtNum(h.throttledAccounts)}</span> },
    {
      id: 'acc',
      label: 'Accounts / cap',
      r: true,
      render: (h) => (
        <span className="mono sm">
          {fmtSi(h.accounts)} <Meter v={h.accounts} max={h.maxAccounts} k="err" /> {fmtSi(h.maxAccounts)}
        </span>
      ),
    },
    {
      id: 'act',
      label: '',
      r: true,
      render: (h) => (
        <span className="cx-form-row" style={{ justifyContent: 'flex-end' }}>
          {h.maxAccounts < BIG_HOST_CAP && (
            <button type="button" className="cx-btn sm" onClick={() => hostActionDialog('raisecap', h)}>
              Raise cap…
            </button>
          )}
          {h.throttledAccounts > 0 && (
            <button type="button" className="cx-btn sm" onClick={() => releaseDialog(h.host, atCap(h))}>
              Lift…
            </button>
          )}
        </span>
      ),
    },
  ]
  return (
    <Panel
      title="Accounts created throttled"
      src={<><Src>hosts · throttledAccounts</Src> <Src>hosts/{'{host}'}/release-throttled</Src></>}
      right={
        rows.length ? (
          <span className="cx-form-row" style={{ gap: 6 }}>
            {total > 0 && <Chip k="warn">{plural(total, 'account')}</Chip>}
            {capped > 0 && <Chip k="err">{plural(capped, 'host')} at cap</Chip>}
          </span>
        ) : undefined
      }
      foot={<span>Past a host’s cap, new accounts are created throttled and stay so after a raise until lifted. The counts are the leader’s.</span>}
    >
      <Loaded load={list}>
        {() => (
          <DataTable rows={rows.slice(0, SHOWN)} cols={cols} rowKey={(h) => h.host} open={(h) => ({ type: 'host', id: h.host })} compact label="Hosts with throttled accounts" empty={<Empty>No host has accounts created throttled, and none is at its account cap.</Empty>} />
        )}
      </Loaded>
      {rows.length > SHOWN && (
        <div className="cx-pn-b">
          <Link className="cx-btn sm quiet" to="/admin/hosts?flag=thr&sort=throttled">
            All {fmtNum(rows.length)} in Hosts ›
          </Link>{' '}
          <span className="muted sm">the {SHOWN} with the most throttled accounts are here</span>
        </div>
      )}
    </Panel>
  )
}

export function Moderation() {
  const cases = useCases()
  const all = cases.data
  const rules = useRules().data
  const ov = useOverview().data
  const open = (all ?? []).filter((c) => c.status === 'open').length
  const by = ov?.hostsByStatus ?? {}
  useEffect(() => {
    if (location.hash === '#rules' || location.pathname === '/admin/rules') setTimeout(() => document.getElementById('rules')?.scrollIntoView({ block: 'start' }), 60)
  }, [])
  return (
    <>
      <PageHead
        title="Moderation"
        sub={
          <>
            <span>{all ? plural(open, 'open case') : '…'}</span>
            <span>{rules ? plural(rules.length, 'domain rule') : '…'}</span>
            {ov && (
              <span>
                {fmtNum(by.throttled ?? 0)} throttled · {fmtNum(by.suspended ?? 0)} suspended · {fmtNum(by.banned ?? 0)} banned hosts
              </span>
            )}
            <Updated l={cases} />
          </>
        }
        actions={
          <>
            <button type="button" className="cx-btn" onClick={() => ruleDialog()}>
              Add domain rule…
            </button>
            <span className="muted sm">
              <Kbd k={['j', 'k', '↵']} />
            </span>
          </>
        }
      />
      <div className="cx-stack">
        <Cases />
        <div className="cx-grid2">
          <Signals />
          <Lookup />
        </div>
        <Takedowns />
        <Rules />
        <Throttled />
      </div>
    </>
  )
}

registerPalette({
  items: (q) => {
    const out: PalItem[] = []
    const cs = cached<Case[]>(keys.cases('all')) ?? []
    for (const c of cs.filter((x) => x.status === 'open' || x.status === 'acknowledged').slice(0, 30))
      out.push({ group: 'Cases', glyph: '◇', title: `Case ${c.id} ${c.kind.replace(/-/g, ' ')}`, desc: `${c.host} · ${c.status}`, hay: c.did ?? '', run: () => openPanel('case', String(c.id)) })
    for (const r of (cached<DomainRule[]>(keys.rules()) ?? []).slice(0, 50)) out.push({ group: 'Rules', glyph: '§', title: r.pattern, desc: `${r.effect.kind} · rule ${r.id}`, run: () => openPanel('rule', String(r.id)) })
    const m = q.match(/^take ?down\s+(did:\S+)$/i)
    if (m) out.push({ group: 'Actions', glyph: <span className="cx-g s-err">■</span>, title: `Take down ${shortDid(m[1])}…`, desc: 'accounts/{did}/takedown', hay: q, run: () => (openPanel('acct', m[1]), takedownDialog({ did: m[1], handle: null, host: '' })) })
    else if (/^add rule|^domain rule/i.test(q) || q === 'rule') out.push({ group: 'Actions', glyph: '+', title: 'Add a domain rule…', run: () => ruleDialog() })
    return out
  },
})

