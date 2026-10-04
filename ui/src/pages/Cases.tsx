import { useEffect, useMemo, useRef, useState } from 'react'
import { Empty, ErrorNotice, Loading, PageHead, Panel, Spinner } from '../components/ui'
import { InlineConfirm, Live, SeverityPill } from '../components/relay'
import { api, ApiError, enc, errText, type Case, type CaseDetail as CaseDetailT, type CaseEvidence, type CaseStatus, type Severity } from '../lib/api'
import { fmtNum, fmtTime, relTime, short } from '../lib/format'
import { useAction } from '../lib/hooks'
import { Link, navigate, useSearch } from '../lib/router'
import { useApi, useKey } from '../lib/useApi'
import './pages2.css'

const SEV_RANK: Record<Severity, number> = { critical: 3, high: 2, warn: 1, info: 0 }
const FILTERS: (CaseStatus | 'all')[] = ['open', 'acknowledged', 'resolved', 'dismissed', 'all']

const KIND_LABEL: Record<string, string> = {
  'new-accounts': 'new accounts',
  'reject-ratio': 'reject ratio',
  'bad-signatures': 'bad signatures',
  'account-rate': 'account rate',
}

export function CaseStatusPill({ status }: { status: CaseStatus }) {
  const cls = status === 'open' ? 'sp-throttled' : status === 'acknowledged' ? 'sp-idle' : status === 'resolved' ? 'sp-connected' : 'sp-offline'
  return <span className={`sp ${cls}`}>{status}</span>
}

/** Observed against threshold, in the threshold's own unit. */
export function fmtObserved(c: Pick<Case, 'kind' | 'observed' | 'threshold'>): string {
  if (c.kind === 'reject-ratio') return `${(c.observed * 100).toFixed(0)}% / ${(c.threshold * 100).toFixed(0)}%`
  const d = c.kind === 'account-rate' ? 1 : 0
  return `${fmtNum(c.observed, d)} / ${fmtNum(c.threshold, d)}`
}

const fmtWindow = (s: number) => (s % 3600 === 0 ? `${s / 3600} h` : s % 60 === 0 ? `${s / 60} min` : `${s} s`)
const fmtSignal = (v: number) => (Number.isInteger(v) ? fmtNum(v) : Math.abs(v) < 1 ? v.toFixed(3) : fmtNum(v, 2))

/** The trips folded into a case, each with every signal's value for the host at that moment. */
function Evidence({ id, kind }: { id: number; kind: string }) {
  const [missing, setMissing] = useState(false)
  const l = useApi<CaseDetailT>(`cases/${id}/evidence`, undefined, missing ? undefined : 10000)
  useEffect(() => {
    if (l.error instanceof ApiError && l.error.status === 404) setMissing(true)
  }, [l.error])
  if (missing) return null
  const d = l.data
  const rows: CaseEvidence[] = d ? [...d.evidence].reverse() : []
  const title = d ? `Evidence: ${fmtNum(d.trips)} trip${d.trips === 1 ? '' : 's'}` : 'Evidence'
  const shown = d && d.trips > d.evidence.length ? `The newest ${fmtNum(d.evidence.length)} of ${fmtNum(d.trips)} trips, newest first.` : 'Each time the threshold tripped, newest first.'
  return (
    <Panel title={title} desc={d && rows.length > 0 ? shown : undefined} flush>
      <ErrorNotice error={l.error} />
      {!d ? (
        l.error ? null : <Loading />
      ) : rows.length === 0 ? (
        <p className="muted small audit-empty">No measurements recorded for this case.</p>
      ) : (
        <div className="table-wrap">
          <table className="data compact evidence">
            <thead>
              <tr>
                <th>When</th>
                <th className="num">Observed / threshold</th>
                <th className="num">Window</th>
                <th>Node</th>
                <th>Signals</th>
              </tr>
            </thead>
            <tbody>
              {rows.map((e, i) => (
                <tr key={`${e.atMs}-${i}`}>
                  <td className="nowrap" title={fmtTime(e.atMs)}>
                    {relTime(e.atMs)}
                  </td>
                  <td className="num mono">{fmtObserved({ kind, observed: e.observed, threshold: e.threshold })}</td>
                  <td className="num">{fmtWindow(e.windowSecs)}</td>
                  <td>{e.node}</td>
                  <td className="wrap-cell">
                    {e.detail && <div className="small evidence-detail">{e.detail}</div>}
                    <div className="signals">
                      {Object.entries(e.signals).map(([k, v]) => (
                        <span key={k} className="signal mono">
                          {k}={fmtSignal(v)}
                        </span>
                      ))}
                    </div>
                  </td>
                </tr>
              ))}
            </tbody>
          </table>
        </div>
      )}
    </Panel>
  )
}

export function Cases() {
  const search = useSearch()
  const filter = (search.get('status') as CaseStatus | 'all' | null) ?? 'open'
  // one fetch for every status: the chips need counts for all of them
  const l = useApi<Case[]>('cases', undefined, 5000)
  const [sel, setSel] = useState(0)
  const body = useRef<HTMLTableSectionElement>(null)
  const counts = useMemo(() => {
    const m: Record<string, number> = { all: 0 }
    for (const c of l.data ?? []) {
      m[c.status] = (m[c.status] ?? 0) + 1
      m.all++
    }
    return m
  }, [l.data])
  const rows = useMemo(
    () =>
      (l.data ?? [])
        .filter((c) => filter === 'all' || c.status === filter)
        .sort((a, b) => SEV_RANK[b.severity] - SEV_RANK[a.severity] || b.openedAtMs - a.openedAtMs),
    [l.data, filter],
  )
  useEffect(() => setSel(0), [filter])
  useKey(
    (e) => {
      if (!rows.length) return
      if (e.key === 'j' || e.key === 'ArrowDown') {
        e.preventDefault()
        setSel((s) => Math.min(rows.length - 1, s + 1))
      } else if (e.key === 'k' || e.key === 'ArrowUp') {
        e.preventDefault()
        setSel((s) => Math.max(0, s - 1))
      } else if (e.key === 'Enter') {
        const c = rows[Math.min(sel, rows.length - 1)]
        if (c) navigate(`/admin/cases/${c.id}`)
      }
    },
    [rows, sel],
  )
  useEffect(() => {
    body.current?.querySelector('tr.sel')?.scrollIntoView({ block: 'nearest' })
  }, [sel])

  return (
    <>
      <div className="console-head">
        <h1>Cases</h1>
        <Live at={l.at} error={l.error} every={5000} />
      </div>
      <p className="muted small">Opened when a host crosses a spam threshold in the policy. A host gets at most one open case per kind.</p>
      <div className="toolbar">
        <div className="chips" role="group" aria-label="Status">
          {FILTERS.map((f) => (
            <button
              key={f}
              type="button"
              className="chip"
              aria-pressed={filter === f}
              onClick={() => navigate(f === 'open' ? '/admin/cases' : `/admin/cases?status=${f}`, { replace: true })}
            >
              {f} <span className="n">{counts[f] ?? 0}</span>
            </button>
          ))}
        </div>
        <span className="muted small">
          <kbd>j</kbd>
          <kbd>k</kbd> move, <kbd>↵</kbd> open
        </span>
      </div>
      <ErrorNotice error={l.error} />
      {!l.data ? (
        l.error ? null : <Loading />
      ) : rows.length === 0 ? (
        <Empty title={filter === 'open' ? 'No open cases' : `No ${filter === 'all' ? '' : `${filter} `}cases`}>Nothing has crossed a threshold.</Empty>
      ) : (
        <Panel flush>
          <div className="table-wrap">
            <table className="data compact">
              <thead>
                <tr>
                  <th>Severity</th>
                  <th>Kind</th>
                  <th>Host</th>
                  <th>Summary</th>
                  <th className="num">Observed / threshold</th>
                  <th>Opened</th>
                  <th>Status</th>
                </tr>
              </thead>
              <tbody ref={body}>
                {rows.map((c, i) => (
                  <tr key={c.id} className={`link${i === sel ? ' sel' : ''}`} onClick={() => navigate(`/admin/cases/${c.id}`)}>
                    <td>
                      <SeverityPill severity={c.severity} />
                    </td>
                    <td>{KIND_LABEL[c.kind] ?? c.kind}</td>
                    <td className="mono" onClick={(e) => e.stopPropagation()}>
                      <Link to={`/admin/hosts/${enc(c.host)}`}>{c.host}</Link>
                    </td>
                    <td className="case-summary">{c.summary}</td>
                    <td className="num mono">{fmtObserved(c)}</td>
                    <td title={fmtTime(c.openedAtMs)}>{relTime(c.openedAtMs)}</td>
                    <td>
                      <CaseStatusPill status={c.status} />
                    </td>
                  </tr>
                ))}
              </tbody>
            </table>
          </div>
        </Panel>
      )}
    </>
  )
}

const TRANSITIONS: Record<CaseStatus, { to: CaseStatus; label: string }[]> = {
  open: [
    { to: 'acknowledged', label: 'Acknowledge' },
    { to: 'resolved', label: 'Resolve' },
    { to: 'dismissed', label: 'Dismiss' },
  ],
  acknowledged: [
    { to: 'resolved', label: 'Resolve' },
    { to: 'dismissed', label: 'Dismiss' },
    { to: 'open', label: 'Reopen' },
  ],
  resolved: [{ to: 'open', label: 'Reopen' }],
  dismissed: [{ to: 'open', label: 'Reopen' }],
}

export function CaseDetail({ id }: { id: number }) {
  const l = useApi<Case>(`cases/${id}`, undefined, 5000)
  const [pending, setPending] = useState<{ to: CaseStatus; label: string } | null>(null)
  const [note, setNote] = useState('')
  const act = useAction(async (status: CaseStatus | null, text: string) => {
    await api(`cases/${id}`, { body: { status, note: text } })
    setPending(null)
    setNote('')
    l.reload()
  })
  const c = l.data
  const crumbs = [{ to: '/admin/cases', label: 'Cases' }]
  if (!c)
    return (
      <>
        <PageHead title={`Case ${id}`} crumbs={crumbs} />
        <ErrorNotice error={l.error} />
        {!l.error && <Loading />}
      </>
    )
  const hostUrl = `/admin/hosts/${enc(c.host)}`
  return (
    <>
      <PageHead title={`Case ${c.id}: ${KIND_LABEL[c.kind] ?? c.kind}`} crumbs={crumbs} />
      <div className="console-head">
        <div className="row">
          <SeverityPill severity={c.severity} />
          <CaseStatusPill status={c.status} />
          <span className="muted small" title={fmtTime(c.openedAtMs)}>
            opened {relTime(c.openedAtMs)}
            {c.updatedAtMs !== c.openedAtMs && <>, updated {relTime(c.updatedAtMs)}</>}
          </span>
        </div>
        <Live at={l.at} error={l.error} every={5000} />
      </div>
      <ErrorNotice error={l.error} />
      <div className="grid2">
        <div>
          <Panel title={c.summary}>
            <dl className="dl">
              <dt>Host</dt>
              <dd>
                <Link to={hostUrl} className="mono">
                  {c.host}
                </Link>
              </dd>
              {c.did && (
                <>
                  <dt>Account</dt>
                  <dd>
                    <Link to={`/admin/accounts/${enc(c.did)}`} className="mono">
                      {short(c.did, 14)}
                    </Link>
                  </dd>
                </>
              )}
              <dt>Observed / threshold</dt>
              <dd className="mono">{fmtObserved(c)}</dd>
              <dt>Over by</dt>
              <dd>{c.threshold > 0 ? `${(c.observed / c.threshold).toFixed(1)}×` : '—'}</dd>
              <dt>Automatic action</dt>
              <dd>{c.autoAction ?? <span className="muted">none (auto-throttle is off, or below high)</span>}</dd>
            </dl>
          </Panel>
          <Panel title="Act on the host" desc="Bans, throttles and tier changes live on the host page, with their own confirmation.">
            <div className="row">
              <Link to={hostUrl} className="btn sm danger">
                Ban or suspend…
              </Link>
              <Link to={hostUrl} className="btn sm">
                Throttle or retier…
              </Link>
              {c.did && (
                <Link to={`/admin/accounts/${enc(c.did)}`} className="btn sm">
                  Account takedown…
                </Link>
              )}
            </div>
          </Panel>
        </div>
        <div>
          <Panel title="Status">
            <div className="row">
              {TRANSITIONS[c.status].map((t) => (
                <button
                  key={t.to}
                  type="button"
                  className={`btn sm${t.to === 'resolved' ? ' primary' : ''}`}
                  disabled={pending !== null || act.busy}
                  onClick={() => setPending(t)}
                >
                  {t.label}
                </button>
              ))}
            </div>
            <InlineConfirm
              open={pending !== null}
              action={pending?.label ?? ''}
              busy={act.busy}
              error={act.error ? errText(act.error) : undefined}
              onConfirm={() => pending && act.run(pending.to, note)}
              onCancel={() => setPending(null)}
            >
              <span>
                Mark this case <b>{pending?.to}</b>.
              </span>
              <input type="text" className="confirm-note" value={note} onChange={(e) => setNote(e.target.value)} placeholder="Note (optional)" aria-label="Note" />
            </InlineConfirm>
          </Panel>
          <Panel title="Notes">
            {c.notes.length === 0 ? (
              <p className="muted small">No notes yet.</p>
            ) : (
              <ol className="timeline">
                {c.notes.map((n, i) => (
                  <li key={i}>
                    <div className="small muted" title={fmtTime(n.atMs)}>
                      {n.by}, {relTime(n.atMs)}
                    </div>
                    <div>{n.text}</div>
                  </li>
                ))}
              </ol>
            )}
            {pending === null && (
              <form
                className="note-form"
                onSubmit={(e) => {
                  e.preventDefault()
                  if (note.trim()) act.run(null, note.trim())
                }}
              >
                <input type="text" value={note} onChange={(e) => setNote(e.target.value)} placeholder="Add a note" aria-label="Add a note" />
                <button className="btn sm" disabled={!note.trim() || act.busy}>
                  {act.busy && <Spinner />}
                  Add note
                </button>
              </form>
            )}
          </Panel>
        </div>
      </div>
      <Evidence id={c.id} kind={c.kind} />
    </>
  )
}
