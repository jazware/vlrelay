import { useEffect, useMemo, useState } from 'react'
import { confirmAction } from '../../components/console/dialogs'
import { openPanel, panelParam, usePanel } from '../../components/console/nav'
import { Empty, Glyph, HostName, Loaded, Over, Panel, Seg, Src, fmtX } from '../../components/console/kit'
import { errText, type Case, type CaseStatus } from '../../lib/api'
import * as A from '../../lib/console/adminAdapter'
import { ago, dt, fmtNum, plural, shortDid } from '../../lib/console/fmt'
import { useCases } from '../../lib/console/queries'
import { sevTone } from '../../lib/console/tone'
import * as W from '../../lib/console/writes'
import { useSearch } from '../../lib/router'
import { CaseStatusChip, SEV_RANK, caseObs } from './moderationDetail'

// Cases grouped by kind: a summary row per kind (how many, the worst and where) that opens to its
// cases worst first, a page at a time, so a flood of one kind doesn't push the rest of the page
// away. Rows tick into a selection, or a kind's whole group does, for a bulk status change.

type Filter = CaseStatus | 'all'
const FILTERS: Filter[] = ['open', 'acknowledged', 'resolved', 'dismissed', 'all']
const PAGE = 12
/** Up to this many cases in view, every kind starts open. */
const OPEN_UNDER = 15

const kindLabel = (k: string) => k.replace(/-/g, ' ')
const ratio = (c: Case) => (c.threshold > 0 ? c.observed / c.threshold : NaN)
const worse = (a: Case, b: Case) => (ratio(b) || 0) - (ratio(a) || 0) || SEV_RANK[b.severity] - SEV_RANK[a.severity] || b.openedAtMs - a.openedAtMs

function setParam(k: string, v: string) {
  const s = new URLSearchParams(location.search)
  if (v) s.set(k, v)
  else s.delete(k)
  const q = s.toString()
  history.replaceState(null, '', `${location.pathname}${q ? `?${q}` : ''}${location.hash}`)
  dispatchEvent(new PopStateEvent('popstate'))
}

type Group = { kind: string; cases: Case[]; worst: Case }

const VERB: Record<'acknowledged' | 'resolved' | 'dismissed', string> = { acknowledged: 'Acknowledge', resolved: 'Resolve', dismissed: 'Dismiss' }

/** One status for many cases: a confirm with the call, then a POST per case with progress. */
function bulkDialog(cases: Case[], to: keyof typeof VERB, done: () => void) {
  const todo = cases.filter((c) => c.status !== to)
  const skip = cases.length - todo.length
  const needNote = to !== 'acknowledged'
  let left = todo.map((c) => c.id)
  return confirmAction({
    tone: 'warn',
    primary: true,
    title: `${VERB[to]} ${plural(todo.length, 'case')}?`,
    items: [
      to === 'acknowledged'
        ? 'They stay listed as being looked at; the open count stops counting them.'
        : to === 'resolved'
          ? 'Closes them. A new trip on the same host and kind opens a new case.'
          : 'Closes them as false positives. A new trip opens a new case.',
      `The same note goes on each.${skip ? ` ${plural(skip, 'case')} already ${to} ${skip === 1 ? 'is' : 'are'} left alone.` : ''}`,
      'Nothing changes on the hosts: bans, throttles and takedowns are their own actions.',
    ],
    fields: [{ id: 'note', label: needNote ? 'Note (required, kept on each case)' : 'Note (optional)', type: 'textarea', required: needNote, placeholder: to === 'dismissed' ? 'Read lag from a slow network, not abuse' : 'Looked at the lot' }],
    action: `${VERB[to]} ${fmtNum(todo.length)}`,
    call: (v) => `${fmtNum(todo.length)} × ${A.updateCaseCall(todo[0]?.id ?? 0, { status: to, note: String(v.note ?? '').trim() }).replace(/cases\/\d+/, 'cases/{id}')}`,
    run: async (v, progress) => {
      const n = left.length
      const failed = await W.updateCases(left, { status: to, note: String(v.note ?? '').trim() }, (ok, bad) => progress(`${fmtNum(ok + bad)} of ${fmtNum(n)}${bad ? ` · ${fmtNum(bad)} failed` : ''}`))
      left = failed.map((f) => f.id)
      if (failed.length) throw new Error(`${plural(failed.length, 'case')} failed (${failed.slice(0, 5).map((f) => f.id).join(', ')}${failed.length > 5 ? ', …' : ''}): ${errText(failed[0].error)}. Submit again to retry just those.`)
      done()
    },
    done: `${plural(todo.length, 'case')} ${to}`,
  })
}

export function Cases() {
  const all = useCases()
  const search = useSearch()
  const panel = usePanel()
  const f = (search.get('cases') as Filter | null) ?? 'open'
  const kind = search.get('kind') ?? ''
  const [sel, setSel] = useState<Set<number>>(new Set())
  const [open, setOpen] = useState<Map<string, boolean>>(new Map())
  const [shown, setShown] = useState<Map<string, number>>(new Map())
  useEffect(() => setSel(new Set()), [f, kind])

  const counts = useMemo(() => {
    const m: Record<string, number> = { all: 0 }
    for (const c of all.data ?? []) {
      m[c.status] = (m[c.status] ?? 0) + 1
      m.all++
    }
    return m
  }, [all.data])
  const inStatus = useMemo(() => (all.data ?? []).filter((c) => f === 'all' || c.status === f), [all.data, f])
  const kinds = useMemo(() => {
    const m = new Map<string, number>()
    for (const c of inStatus) m.set(c.kind, (m.get(c.kind) ?? 0) + 1)
    return [...m].sort((a, b) => b[1] - a[1])
  }, [inStatus])
  const groups: Group[] = useMemo(() => {
    const m = new Map<string, Case[]>()
    for (const c of inStatus) if (!kind || c.kind === kind) (m.get(c.kind) ?? m.set(c.kind, []).get(c.kind)!).push(c)
    return [...m]
      .map(([k, cs]) => {
        cs.sort(worse)
        return { kind: k, cases: cs, worst: cs[0] }
      })
      .sort((a, b) => SEV_RANK[b.worst.severity] - SEV_RANK[a.worst.severity] || b.cases.length - a.cases.length)
  }, [inStatus, kind])
  const total = groups.reduce((n, g) => n + g.cases.length, 0)
  const isOpen = (k: string) => open.get(k) ?? (groups.length === 1 || total <= OPEN_UNDER)
  const byId = new Map(inStatus.map((c) => [c.id, c]))
  const picked = [...sel].map((id) => byId.get(id)).filter((c): c is Case => !!c)
  const toggle = (ids: number[], on: boolean) =>
    setSel((s) => {
      const n = new Set(s)
      for (const id of ids) on ? n.add(id) : n.delete(id)
      return n
    })
  const closable = f !== 'resolved' && f !== 'dismissed'

  return (
    <Panel
      title="Cases"
      src={
        <>
          <Src>cases</Src> <Src>cases/{'{id}'}/evidence</Src>
        </>
      }
      right={
        <span className="cx-form-row" style={{ gap: 8, justifyContent: 'flex-end' }}>
          <select className="cx-inp" style={{ height: 26, width: 'auto', flex: 'none' }} aria-label="Case kind" value={kind} onChange={(e) => setParam('kind', e.target.value)}>
            <option value="">every kind</option>
            {kinds.map(([k, n]) => (
              <option key={k} value={k}>
                {kindLabel(k)} · {fmtNum(n)}
              </option>
            ))}
            {kind && !kinds.some(([k]) => k === kind) && <option value={kind}>{kindLabel(kind)} · 0</option>}
          </select>
          <Seg<Filter> label="Case status" value={f} options={FILTERS.map((s) => ({ v: s, label: s === 'acknowledged' ? 'ack' : s, n: counts[s] ?? 0 }))} onChange={(v) => setParam('cases', v === 'open' ? '' : v)} />
        </span>
      }
      foot={<span>Opened when a host or account crosses a spam threshold in the policy. A host gets at most one open case per kind. Tick cases, or a kind's whole group, to change their status together.</span>}
    >
      {picked.length > 0 && (
        <div className="cx-bulk" role="region" aria-label="Selected cases">
          <b>{plural(picked.length, 'case')} selected</b>
          {closable && (
            <button type="button" className="cx-btn sm" onClick={() => bulkDialog(picked, 'acknowledged', () => setSel(new Set()))}>
              Acknowledge…
            </button>
          )}
          <button type="button" className="cx-btn sm" onClick={() => bulkDialog(picked, 'resolved', () => setSel(new Set()))}>
            Resolve…
          </button>
          <button type="button" className="cx-btn sm" onClick={() => bulkDialog(picked, 'dismissed', () => setSel(new Set()))}>
            Dismiss…
          </button>
          <button type="button" className="cx-btn sm quiet" onClick={() => setSel(new Set())}>
            Clear
          </button>
        </div>
      )}
      <Loaded load={all}>
        {() =>
          total === 0 ? (
            <Empty title={f === 'open' ? 'No open cases' : `No ${f === 'all' ? '' : `${f} `}cases`}>{kind ? `None of kind ${kindLabel(kind)}. ` : ''}Nothing has crossed a threshold.</Empty>
          ) : (
            <div className="cx-tw">
              <table className="cx-t compact cx-cases" aria-label="Cases">
                <thead>
                  <tr>
                    <th className="ck" />
                    <th>Case</th>
                    <th>Subject</th>
                    <th className="r" title="How far past its threshold it was when it tripped: observed / threshold. The bar is logarithmic, 0.1× to 10×, with a tick at the threshold">
                      Over threshold
                    </th>
                    <th>Auto action</th>
                    <th>Status</th>
                    <th className="r">Opened</th>
                  </tr>
                </thead>
                {groups.map((g) => {
                  const on = isOpen(g.kind)
                  const n = shown.get(g.kind) ?? PAGE
                  const ids = g.cases.map((c) => c.id)
                  const nSel = ids.filter((id) => sel.has(id)).length
                  return (
                    <tbody key={g.kind}>
                      <tr className="grp" onClick={(e) => !(e.target as HTMLElement).closest('input,button') && setOpen((m) => new Map(m).set(g.kind, !on))}>
                        <td className="ck">
                          <input
                            type="checkbox"
                            aria-label={`Select every ${kindLabel(g.kind)} case`}
                            checked={nSel === ids.length}
                            ref={(el) => {
                              if (el) el.indeterminate = nSel > 0 && nSel < ids.length
                            }}
                            onChange={(e) => toggle(ids, e.target.checked)}
                          />
                        </td>
                        <td colSpan={6}>
                          <button type="button" className="cx-grpbtn" aria-expanded={on} onClick={() => setOpen((m) => new Map(m).set(g.kind, !on))}>
                            <span className="cx-chev">›</span>
                            <b>{kindLabel(g.kind)}</b>
                            <span className="t2">
                              {fmtNum(g.cases.length)} {f === 'all' ? '' : f === 'acknowledged' ? 'acknowledged' : f}
                            </span>
                            {isFinite(ratio(g.worst)) && (
                              <span className="t2">
                                · worst <b className={ratio(g.worst) >= 1 ? 's-err' : undefined}>{fmtX(ratio(g.worst))}</b> ({g.worst.host || shortDid(g.worst.did ?? '')})
                              </span>
                            )}
                            {nSel > 0 && <span className="muted">· {fmtNum(nSel)} selected</span>}
                          </button>
                        </td>
                      </tr>
                      {on &&
                        g.cases.slice(0, n).map((c) => {
                          const ref = panelParam('case', String(c.id))
                          const cur = panel?.type === 'case' && panel.id === String(c.id)
                          return (
                            <tr
                              key={c.id}
                              data-open={ref}
                              className={`${cur ? 'sel' : ''}${c.status === 'resolved' || c.status === 'dismissed' ? ' dim' : ''}`}
                              onClick={(e) => {
                                if ((e.target as HTMLElement).closest('button,a,input,select,textarea,label')) return
                                openPanel('case', String(c.id))
                              }}
                            >
                              <td className="ck">
                                <input type="checkbox" aria-label={`Select case ${c.id}`} checked={sel.has(c.id)} onChange={(e) => toggle([c.id], e.target.checked)} />
                              </td>
                              <td className="mono nowrap">
                                <Glyph k={sevTone(c.severity)} title={c.severity} /> {c.id}
                              </td>
                              <td className="subj">
                                <span className="cx-cellid">
                                  <HostName host={c.host} />
                                  {c.did && <span className="cx-did">{shortDid(c.did)}</span>}
                                </span>
                              </td>
                              <td className="r">
                                <Over r={ratio(c)} detail={caseObs(c)} />
                              </td>
                              <td>{c.autoAction ? <span className="mono sm">{c.autoAction}</span> : <span className="muted sm">none</span>}</td>
                              <td>
                                <CaseStatusChip c={c} />
                              </td>
                              <td className="r">
                                <span className="sm muted" title={dt(c.openedAtMs)}>
                                  {ago(c.openedAtMs)}
                                </span>
                              </td>
                            </tr>
                          )
                        })}
                      {on && g.cases.length > PAGE && (
                        <tr className="more">
                          <td />
                          <td colSpan={6}>
                            {g.cases.length > n && (
                              <button type="button" className="cx-btn sm quiet" onClick={() => setShown((m) => new Map(m).set(g.kind, n + PAGE * 4))}>
                                Show {fmtNum(Math.min(PAGE * 4, g.cases.length - n))} more
                              </button>
                            )}
                            {n > PAGE && (
                              <button type="button" className="cx-btn sm quiet" onClick={() => setShown((m) => new Map(m).set(g.kind, PAGE))}>
                                Show fewer
                              </button>
                            )}
                            <span className="muted sm">
                              {fmtNum(Math.min(n, g.cases.length))} of {fmtNum(g.cases.length)}, worst first
                            </span>
                          </td>
                        </tr>
                      )}
                    </tbody>
                  )
                })}
              </table>
            </div>
          )
        }
      </Loaded>
    </Panel>
  )
}
