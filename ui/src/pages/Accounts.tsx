import { useEffect, useRef, useState } from 'react'
import { CopyText, Empty, ErrorNotice, Loading, PageHead, Panel } from '../components/ui'
import { InlineConfirm, Live } from '../components/relay'
import { api, enc, errText, type Account, type AccountArchive } from '../lib/api'
import { fmtNum, fmtTime, relTime, short } from '../lib/format'
import { useAction } from '../lib/hooks'
import { Link, navigate, useSearch } from '../lib/router'
import { useApi, useKey } from '../lib/useApi'
import './pages2.css'

function ArchiveState({ a }: { a: AccountArchive }) {
  if (a.fetching) return <span className="pill amber">{a.mirrored ? 'mirrored, re-fetching' : 'fetching'}</span>
  if (a.mirrored) return <span className="pill accent">mirrored</span>
  if (a.lastError) return <span className="pill danger">fetch failed</span>
  if (a.wanted) return <span className="pill amber">wanted, not mirrored yet</span>
  return <span className="muted">not archived (policy doesn't cover its host)</span>
}

export function AccountStatus({ status }: { status: string }) {
  const cls =
    status === 'takendown' ? 'sp-banned' : status === 'throttled' ? 'sp-throttled' : status === 'active' ? 'sp-connected' : status === 'suspended' ? 'sp-suspended' : 'sp-offline'
  return <span className={`sp ${cls}`}>{status === 'takendown' ? 'taken down' : status}</span>
}

const isDid = (s: string) => /^did:(plc:[a-z2-7]{24}|web:.+)$/.test(s.trim())

export function Accounts() {
  const search = useSearch()
  const [q, setQ] = useState(search.get('q') ?? '')
  const [debounced, setDebounced] = useState(q)
  const [sel, setSel] = useState(0)
  const body = useRef<HTMLTableSectionElement>(null)
  useEffect(() => {
    const id = setTimeout(() => setDebounced(q.trim()), 200)
    return () => clearTimeout(id)
  }, [q])
  useEffect(() => {
    navigate(debounced ? `/admin/accounts?q=${enc(debounced)}` : '/admin/accounts', { replace: true })
    setSel(0)
  }, [debounced])
  const l = useApi<Account[]>('accounts', { q: debounced || undefined }, 10000)
  const rows = l.data ?? []

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
        const r = rows[Math.min(sel, rows.length - 1)]
        if (r) navigate(`/admin/accounts/${enc(r.did)}`)
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
        <h1>Accounts</h1>
        <Live at={l.at} error={l.error} every={10000} />
      </div>
      <form
        className="toolbar"
        onSubmit={(e) => {
          e.preventDefault()
          if (isDid(q)) navigate(`/admin/accounts/${enc(q.trim())}`)
          else if (rows[sel]) navigate(`/admin/accounts/${enc(rows[sel].did)}`)
        }}
      >
        <input
          type="search"
          data-search
          value={q}
          onChange={(e) => setQ(e.target.value)}
          placeholder="DID or handle (prefix)"
          aria-label="Search accounts"
          spellCheck={false}
          autoCapitalize="off"
          className="acct-search"
          onKeyDown={(e) => {
            if (e.key === 'ArrowDown') {
              e.preventDefault()
              ;(e.target as HTMLInputElement).blur()
              setSel(0)
            }
          }}
        />
        <span className="muted small">
          {debounced ? `${rows.length}${rows.length >= 100 ? '+' : ''} match${rows.length === 1 ? '' : 'es'}` : 'Recently active'} · <kbd>/</kbd> search, <kbd>j</kbd>
          <kbd>k</kbd> move, <kbd>↵</kbd> open
        </span>
      </form>
      <ErrorNotice error={l.error} />
      {!l.data ? (
        l.error ? null : <Loading />
      ) : rows.length === 0 ? (
        <Empty title="No accounts match">Search by a full DID or a handle to look up any account the relay has seen.</Empty>
      ) : (
        <Panel flush>
          <div className="table-wrap">
            <table className="data compact">
              <thead>
                <tr>
                  <th>Handle</th>
                  <th>DID</th>
                  <th>Host</th>
                  <th>Status</th>
                  <th>Last event</th>
                  <th className="num">Events/h</th>
                </tr>
              </thead>
              <tbody ref={body}>
                {rows.map((a, i) => (
                  <tr key={a.did} className={`link${i === sel ? ' sel' : ''}`} onClick={() => navigate(`/admin/accounts/${enc(a.did)}`)}>
                    <td>{a.handle ?? <span className="muted">—</span>}</td>
                    <td className="mono">{short(a.did, 14)}</td>
                    <td className="mono" onClick={(e) => e.stopPropagation()}>
                      <Link to={`/admin/hosts/${enc(a.host)}`}>{a.host}</Link>
                    </td>
                    <td>
                      <AccountStatus status={a.status} />
                    </td>
                    <td title={fmtTime(a.lastEventMs)}>{relTime(a.lastEventMs)}</td>
                    <td className="num">{fmtNum(a.eventsLastHour)}</td>
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

export function AccountDetail({ did }: { did: string }) {
  const l = useApi<Account>(`accounts/${enc(did)}`, undefined, 10000)
  const [open, setOpen] = useState<'takedown' | 'restore' | null>(null)
  const act = useAction(async (kind: 'takedown' | 'restore', reason: string) => {
    if (kind === 'takedown') await api(`accounts/${enc(did)}/takedown`, { body: { reason } })
    else await api(`accounts/${enc(did)}/untakedown`, { method: 'POST' })
    setOpen(null)
    l.reload()
  })
  const a = l.data
  const crumbs = [{ to: '/admin/accounts', label: 'Accounts' }]
  if (!a)
    return (
      <>
        <PageHead title={<span className="mono">{did}</span>} crumbs={crumbs} />
        <ErrorNotice error={l.error} />
        {!l.error && <Loading />}
      </>
    )
  return (
    <>
      <PageHead title={a.handle ?? <span className="mono">{short(a.did, 14)}</span>} crumbs={crumbs} />
      <div className="console-head acct-head">
        <div className="row">
          <AccountStatus status={a.status} />
          {a.status !== a.upstreamStatus && <span className="muted small">host says {a.upstreamStatus}</span>}
        </div>
        <Live at={l.at} error={l.error} every={10000} />
      </div>
      <ErrorNotice error={l.error} />
      <div className="grid2">
        <Panel title="Account">
          <dl className="dl">
            <dt>Handle</dt>
            <dd>{a.handle ?? <span className="muted">not known to the relay</span>}</dd>
            <dt>DID</dt>
            <dd>
              <CopyText text={a.did} />
            </dd>
            <dt>Host</dt>
            <dd>
              <Link to={`/admin/hosts/${enc(a.host)}`} className="mono">
                {a.host}
              </Link>
            </dd>
            <dt>Relay status</dt>
            <dd>
              <AccountStatus status={a.status} />
            </dd>
            <dt>Upstream status</dt>
            <dd>{a.upstreamStatus}</dd>
            <dt>Rev</dt>
            <dd className="mono">{a.rev}</dd>
            <dt>Last seq</dt>
            <dd className="mono">{a.lastSeq}</dd>
            <dt>Last event</dt>
            <dd title={fmtTime(a.lastEventMs)}>{relTime(a.lastEventMs)}</dd>
            <dt>Last hour</dt>
            <dd>
              {fmtNum(a.eventsLastHour)} events, <span className={a.rejectsLastHour > 0 ? 'err-mid' : ''}>{fmtNum(a.rejectsLastHour)} rejected</span>
            </dd>
            <dt>DID shard</dt>
            <dd>
              <span className="mono">{a.didShard}</span> <span className="muted">on {a.node}</span>
            </dd>
            {a.archive && (
              <>
                <dt>Archive</dt>
                <dd>
                  <ArchiveState a={a.archive} />
                </dd>
                {a.archive.rev && (
                  <>
                    <dt>Mirror rev</dt>
                    <dd className="mono">
                      {a.archive.rev}
                      {a.archive.rev !== a.rev && <span className="muted small"> (sync state at {a.rev})</span>}
                    </dd>
                  </>
                )}
                {a.archive.takedownAtMs && (
                  <>
                    <dt>Mirror deletion</dt>
                    <dd title={fmtTime(a.archive.takedownAtMs)}>taken down {relTime(a.archive.takedownAtMs)}; deleted after the takedown retention</dd>
                  </>
                )}
                {a.archive.lastError && (
                  <>
                    <dt>Last fetch error</dt>
                    <dd className="err-mid small">{a.archive.lastError}</dd>
                  </>
                )}
              </>
            )}
          </dl>
        </Panel>
        <Panel title="Takedown" desc="A takedown drops the account's events on this relay and serves it as taken down. The host keeps the repo." danger={!!a.takedown}>
          {a.takedown ? (
            <>
              <dl className="dl">
                <dt>Reason</dt>
                <dd>{a.takedown.reason}</dd>
                <dt>By</dt>
                <dd>{a.takedown.by}</dd>
                <dt>When</dt>
                <dd title={fmtTime(a.takedown.atMs)}>
                  {relTime(a.takedown.atMs)} <span className="muted">({fmtTime(a.takedown.atMs)})</span>
                </dd>
              </dl>
              <div className="row end" style={{ marginTop: 12 }}>
                <button type="button" className="btn sm" onClick={() => setOpen('restore')} disabled={open !== null}>
                  Reverse takedown
                </button>
              </div>
            </>
          ) : (
            <div className="row between">
              <span className="muted small">Not taken down.</span>
              <button type="button" className="btn sm danger" onClick={() => setOpen('takedown')} disabled={open !== null}>
                Take down
              </button>
            </div>
          )}
          <InlineConfirm
            open={open === 'takedown'}
            danger
            action="Take down"
            reason="Reason (recorded in the audit trail)"
            busy={act.busy}
            error={act.error ? errText(act.error) : undefined}
            onConfirm={(r) => act.run('takedown', r)}
            onCancel={() => setOpen(null)}
          >
            Take down <b>{a.handle ?? short(a.did, 14)}</b>? Its events stop going out on this relay right away.
          </InlineConfirm>
          <InlineConfirm
            open={open === 'restore'}
            action="Reverse takedown"
            busy={act.busy}
            error={act.error ? errText(act.error) : undefined}
            onConfirm={() => act.run('restore', '')}
            onCancel={() => setOpen(null)}
          >
            Restore <b>{a.handle ?? short(a.did, 14)}</b>? New events go out again. Events dropped while it was down aren't replayed.
          </InlineConfirm>
        </Panel>
      </div>
    </>
  )
}
