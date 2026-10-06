import { useEffect, useMemo, useState } from 'react'
import { DataTable, type Col } from '../../components/console/DataTable'
import { hostActionDialog, BIG_HOST_CAP } from '../../components/console/hostActions'
import { openPanel } from '../../components/console/nav'
import { registerPalette, type PalItem } from '../../components/console/Palette'
import { Bars, Chip, Empty, Glyph, HostName, Kbd, Loaded, Meter, NeedsVersion, PageHead, Panel, Sec, Seg, Src, TierTag } from '../../components/console/kit'
import type { Account, Case, CaseStatus, DomainRule, HostRow, PolicyAudit } from '../../lib/api'
import * as A from '../../lib/console/adminAdapter'
import { ago, dt, fmtNum, fmtSi, plural, shortDid } from '../../lib/console/fmt'
import { useLivePoll } from '../../lib/console/live'
import { capPoll, overviewPoll } from '../../lib/console/polls'
import { useSearch } from '../../lib/router'
import { signalOfKind } from './Policy'
import {
  AccountChip,
  CaseStatusChip,
  EffectChip,
  SEV_RANK,
  SEV_TONE,
  caseObs,
  casesPoll,
  releaseDialog,
  ruleDialog,
  rulesAuditPoll,
  rulesPoll,
  takedownDialog,
} from './moderationDetail'

// Cases, accounts and takedowns, domain rules and the accounts hosts created throttled. Every
// row opens in the slide-over (case, acct, rule, host); every write is a confirm with its call.

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
  const all = casesPoll.use()
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
          <Glyph k={SEV_TONE[c.severity] === 'err' ? 'err' : SEV_TONE[c.severity] === 'warn' ? 'warn' : 'info'} title={c.severity} /> {c.id}
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
      label: 'Observed',
      sort: (a, b) => a.observed / (a.threshold || 1) - b.observed / (b.threshold || 1),
      render: (c) => (
        <span className="sm">
          <Meter v={c.observed} max={c.threshold * 2} k={c.observed >= c.threshold ? 'err' : 'warn'} title="the threshold is half way" /> <span className="mono">{caseObs(c)}</span>
        </span>
      ),
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

function Signals() {
  const all = casesPoll.use().data ?? []
  const sig = useLivePoll(A.spamSignals, 'signals', 10_000)
  const open = all.filter((c) => (c.status === 'open' || c.status === 'acknowledged') && c.threshold > 0)
  const byKind = new Map<string, Case>()
  for (const c of open) {
    const w = byKind.get(c.kind)
    if (!w || c.observed / c.threshold > w.observed / w.threshold) byKind.set(c.kind, c)
  }
  const rows = [...byKind.values()]
    .sort((a, b) => b.observed / b.threshold - a.observed / a.threshold)
    .map((c) => {
      const s = signalOfKind(c.kind)
      const r = c.observed / c.threshold
      return {
        key: c.kind,
        label: (
          <>
            {s?.label ?? c.kind[0].toUpperCase() + c.kind.slice(1).replace(/-/g, ' ')} <span className="muted">{s?.per ?? 'host'}</span>
          </>
        ),
        v: r,
        fmt: `${Math.round(r * 100)}%`,
        color: r >= 1 ? 'err' : r > 0.7 ? 'warn' : 'ok',
        title: `${c.host}: ${caseObs(c)} (case ${c.id})`,
        onClick: () => openPanel('case', String(c.id)),
      }
    })
  return (
    <Panel
      title="Spam signals, live"
      to="/admin/policy"
      src={<><Src>cases</Src> <Src isNew>policy/signals</Src></>}
      right={<span className="muted sm">the worst open case per signal against its threshold</span>}
      foot={sig.data && !sig.data.supported ? <NeedsVersion what="Every signal's heaviest key" endpoint={sig.data.endpoint}>Until then only signals with an open case show.</NeedsVersion> : <span>100% is the threshold. Hover for the key.</span>}
    >
      {rows.length ? <Bars rows={rows} /> : <Empty>No signal has an open case.</Empty>}
    </Panel>
  )
}

function Lookup() {
  const search = useSearch()
  const q = (search.get('q') ?? '').trim()
  const [text, setText] = useState(q)
  useEffect(() => setText(q), [q])
  const l = useLivePoll(() => (q ? A.accounts(q) : Promise.resolve([] as Account[])), q, q ? 15_000 : 0)
  const tds = useLivePoll(A.takedowns, 'takedowns', 0)
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
      foot={tds.data && !tds.data.supported ? <NeedsVersion what="A list of every takedown" endpoint={tds.data.endpoint}>Look an account up to see or reverse its takedown.</NeedsVersion> : undefined}
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

function Rules() {
  const l = rulesPoll.use()
  const audit = rulesAuditPoll.use()
  const [f, setF] = useState('')
  const rq = f.trim().toLowerCase()
  const rows = (l.data ?? []).filter((r) => !rq || r.pattern.includes(rq) || r.note.toLowerCase().includes(rq))
  const cols: Col<DomainRule>[] = [
    { id: 'id', label: '#', sort: (a, b) => a.id - b.id, render: (r) => <span className="mono muted">{r.id}</span> },
    { id: 'pattern', label: 'Pattern', sort: (a, b) => a.pattern.localeCompare(b.pattern), render: (r) => <span className="mono">{r.pattern}</span> },
    { id: 'effect', label: 'Effect', render: (r) => <EffectChip e={r.effect} /> },
    { id: 'note', label: 'Note', className: 'wrap sm t2', render: (r) => r.note || <span className="muted">—</span> },
    { id: 'matches', label: 'Matches', r: true, sort: (a, b) => a.matches - b.matches, render: (r) => <span className="mono">{fmtNum(r.matches)}</span> },
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

function Throttled() {
  const cap = capPoll.use()
  const thr = useLivePoll(A.throttledAccounts, 'thracc', 0)
  const counts = thr.data?.supported ? thr.data.data : undefined
  const rows = (cap.data?.hosts ?? []).filter((h) => h.maxAccounts > 0 && h.accounts >= h.maxAccounts)
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
    { id: 'thr', label: 'Throttled accounts', r: true, render: (h) => <span className="mono">{counts ? fmtNum(counts[h.host] ?? 0) : '—'}</span> },
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
          <button type="button" className="cx-btn sm" onClick={() => releaseDialog(h.host, h.accounts >= h.maxAccounts)}>
            Lift…
          </button>
        </span>
      ),
    },
  ]
  return (
    <Panel
      title="Accounts created throttled"
      src={<><Src>hosts?sort=accounts</Src> <Src>hosts/{'{host}'}/release-throttled</Src></>}
      right={rows.length ? <Chip k="warn">{plural(rows.length, 'host')} at cap</Chip> : undefined}
      foot={
        counts ? (
          <span>Past a host’s cap, new accounts are created throttled and stay so after a raise until lifted.</span>
        ) : (
          <NeedsVersion what="How many accounts each host created throttled" endpoint="HostRow.throttledAccounts">
            The hosts at their cap are the ones creating them. Past a cap, new accounts stay throttled after a raise until lifted.
          </NeedsVersion>
        )
      }
    >
      <Loaded load={cap}>
        {() => (
          <DataTable rows={rows} cols={cols} rowKey={(h) => h.host} open={(h) => ({ type: 'host', id: h.host })} compact label="Hosts at their account cap" empty={<Empty>No busy host is at its account cap.</Empty>} />
        )}
      </Loaded>
    </Panel>
  )
}

export function Moderation() {
  const all = casesPoll.use().data
  const rules = rulesPoll.use().data
  const ov = overviewPoll.use().data
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
        <Rules />
        <Throttled />
      </div>
    </>
  )
}

registerPalette({
  items: (q) => {
    const out: PalItem[] = []
    const cs = casesPoll.get().data ?? []
    for (const c of cs.filter((x) => x.status === 'open' || x.status === 'acknowledged').slice(0, 30))
      out.push({ group: 'Cases', glyph: '◇', title: `Case ${c.id} ${c.kind.replace(/-/g, ' ')}`, desc: `${c.host} · ${c.status}`, hay: c.did ?? '', run: () => openPanel('case', String(c.id)) })
    for (const r of (rulesPoll.get().data ?? []).slice(0, 50)) out.push({ group: 'Rules', glyph: '§', title: r.pattern, desc: `${r.effect.kind} · rule ${r.id}`, run: () => openPanel('rule', String(r.id)) })
    const m = q.match(/^take ?down\s+(did:\S+)$/i)
    if (m) out.push({ group: 'Actions', glyph: <span className="cx-g s-err">■</span>, title: `Take down ${shortDid(m[1])}…`, desc: 'accounts/{did}/takedown', hay: q, run: () => (openPanel('acct', m[1]), takedownDialog({ did: m[1], handle: null, host: '' })) })
    else if (/^add rule|^domain rule/i.test(q) || q === 'rule') out.push({ group: 'Actions', glyph: '+', title: 'Add a domain rule…', run: () => ruleDialog() })
    return out
  },
})

