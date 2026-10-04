import { useEffect, useLayoutEffect, useMemo, useRef, useState } from 'react'
import { HOST_STATUSES, Live, StatusPill, TierPill } from '../components/relay'
import { ErrorNotice, Loading } from '../components/ui'
import type { HostList, HostRow, HostStatus } from '../lib/api'
import { enc } from '../lib/api'
import { fmtLag, fmtNum, fmtSi, lagClass, relTime } from '../lib/format'
import { navigate, useSearch } from '../lib/router'
import { useApi, useKey } from '../lib/useApi'

const POLL = 5000
const RH = 30
const OVERSCAN = 12

type SortKey = 'host' | 'status' | 'tier' | 'events' | 'errors' | 'accounts' | 'seq' | 'since' | 'lag' | 'node'

const STATUS_ORDER: Record<HostStatus, number> = { banned: 0, suspended: 1, throttled: 2, backoff: 3, offline: 4, connected: 5, idle: 6 }

const COLS: { key: SortKey; label: string; num?: boolean; width: string; title?: string }[] = [
  { key: 'host', label: 'Host', width: 'minmax(220px, 2.6fr)' },
  { key: 'status', label: 'Status', width: '104px' },
  { key: 'tier', label: 'Tier', width: '86px' },
  { key: 'events', label: 'Events/s', num: true, width: '84px' },
  { key: 'errors', label: 'Errors', num: true, width: '70px', title: 'Rejected frames / all frames, last minute' },
  { key: 'accounts', label: 'Accounts', num: true, width: '84px' },
  { key: 'seq', label: 'Upstream seq', num: true, width: '120px' },
  { key: 'since', label: 'Connected', num: true, width: '92px' },
  { key: 'lag', label: 'Lag', num: true, width: '74px', title: 'Receive time minus the host’s event time, p50' },
  { key: 'node', label: 'Node', width: '70px' },
]
const GRID = COLS.map((c) => c.width).join(' ')

function cmp(a: HostRow, b: HostRow, k: SortKey): number {
  switch (k) {
    case 'host':
      return a.host.localeCompare(b.host)
    case 'status':
      return STATUS_ORDER[a.status] - STATUS_ORDER[b.status]
    case 'tier':
      return a.tier.localeCompare(b.tier)
    case 'events':
      return a.eventsPerSec - b.eventsPerSec
    case 'errors':
      return a.errorRate - b.errorRate
    case 'accounts':
      return a.accounts - b.accounts
    case 'seq':
      return a.lastUpstreamSeq - b.lastUpstreamSeq
    case 'since':
      return (a.connectedSinceMs ?? Infinity) - (b.connectedSinceMs ?? Infinity)
    case 'lag':
      return a.lagMs - b.lagMs
    case 'node':
      return a.node.localeCompare(b.node)
  }
}

const errClass = (e: number) => (e >= 0.1 ? 'err-hi' : e >= 0.02 ? 'err-mid' : e === 0 ? 'dim' : '')
const fmtPct = (e: number) => (e === 0 ? '0' : e < 0.001 ? '<0.1%' : `${(e * 100).toFixed(e < 0.1 ? 1 : 0)}%`)

export function Hosts() {
  const search = useSearch()
  const l = useApi<HostList>('hosts', undefined, POLL)
  const [q, setQ] = useState(search.get('q') ?? '')
  const [status, setStatus] = useState<HostStatus | ''>((search.get('status') as HostStatus) ?? '')
  const [tier, setTier] = useState(search.get('tier') ?? '')
  const [sort, setSort] = useState<{ k: SortKey; desc: boolean }>({ k: (search.get('sort') as SortKey) || 'events', desc: search.get('asc') === null })
  const [sel, setSel] = useState<string | null>(null)
  const body = useRef<HTMLDivElement>(null)
  const [scroll, setScroll] = useState(0)
  const [viewH, setViewH] = useState(600)

  // keep the filter in the URL so a link (from Overview, a case, a rule) lands filtered
  useEffect(() => {
    const p = new URLSearchParams()
    if (q) p.set('q', q)
    if (status) p.set('status', status)
    if (tier) p.set('tier', tier)
    if (sort.k !== 'events') p.set('sort', sort.k)
    if (!sort.desc) p.set('asc', '1')
    const s = p.toString()
    // replaceState directly: navigate() would scroll to the top on every keystroke
    history.replaceState(null, '', `/admin/hosts${s ? `?${s}` : ''}`)
  }, [q, status, tier, sort])

  const all = l.data?.hosts
  const { rows, statusCounts, tiers } = useMemo(() => {
    const statusCounts: Partial<Record<HostStatus, number>> = {}
    const tiers = new Map<string, number>()
    const needle = q.trim().toLowerCase()
    const out: HostRow[] = []
    for (const h of all ?? []) {
      if (needle && !h.host.includes(needle)) continue
      if (!tier || h.tier === tier) statusCounts[h.status] = (statusCounts[h.status] ?? 0) + 1
      if (!status || h.status === status) tiers.set(h.tier, (tiers.get(h.tier) ?? 0) + 1)
      if (status && h.status !== status) continue
      if (tier && h.tier !== tier) continue
      out.push(h)
    }
    out.sort((a, b) => {
      const c = cmp(a, b, sort.k) || a.host.localeCompare(b.host)
      return sort.desc ? -c : c
    })
    return { rows: out, statusCounts, tiers: [...tiers.entries()].sort() }
  }, [all, q, status, tier, sort])

  useLayoutEffect(() => {
    const el = body.current
    if (!el) return
    const ro = new ResizeObserver(() => setViewH(el.clientHeight))
    ro.observe(el)
    return () => ro.disconnect()
  }, [all === undefined])

  const selIdx = sel ? rows.findIndex((r) => r.host === sel) : -1

  const move = (d: number) => {
    if (!rows.length) return
    const i = Math.max(0, Math.min(rows.length - 1, (selIdx < 0 ? (d > 0 ? -1 : rows.length) : selIdx) + d))
    setSel(rows[i].host)
    const el = body.current
    if (el) {
      const top = i * RH
      if (top < el.scrollTop) el.scrollTop = top
      else if (top + RH > el.scrollTop + el.clientHeight) el.scrollTop = top + RH - el.clientHeight
    }
  }
  const open = (h?: string) => {
    const host = h ?? sel
    if (host) navigate(`/admin/hosts/${enc(host)}`)
  }
  const page = Math.max(1, Math.floor(viewH / RH) - 1)

  useKey(
    (e) => {
      if (e.key === 'j' || e.key === 'ArrowDown') move(1)
      else if (e.key === 'k' || e.key === 'ArrowUp') move(-1)
      else if (e.key === 'PageDown') move(page)
      else if (e.key === 'PageUp') move(-page)
      else if (e.key === 'Home') move(-rows.length)
      else if (e.key === 'End') move(rows.length)
      else if (e.key === 'Enter') open()
      else return
      e.preventDefault()
    },
    [rows, selIdx, page],
  )

  if (!all) return l.error ? <ErrorNotice error={l.error} /> : <Loading />

  const first = Math.max(0, Math.floor(scroll / RH) - OVERSCAN)
  const last = Math.min(rows.length, Math.ceil((scroll + viewH) / RH) + OVERSCAN)
  const now = Date.now()
  const totalEvents = rows.reduce((a, r) => a + r.eventsPerSec, 0)

  return (
    <>
      <div className="console-head">
        <h1>Hosts</h1>
        <Live at={l.at} error={l.error} every={POLL} />
      </div>
      <ErrorNotice error={l.error} />
      <div className="toolbar">
        <input
          type="search"
          data-search
          placeholder="Filter by hostname   /"
          value={q}
          onChange={(e) => setQ(e.target.value)}
          onKeyDown={(e) => {
            if (e.key === 'Escape') (e.target as HTMLInputElement).blur()
            if (e.key === 'ArrowDown' || e.key === 'Enter') {
              e.preventDefault()
              ;(e.target as HTMLInputElement).blur()
              if (e.key === 'Enter' && rows.length === 1) open(rows[0].host)
              else move(1)
            }
          }}
          aria-label="Filter hosts"
        />
        <div className="chips" role="group" aria-label="Status">
          <button type="button" className="chip" aria-pressed={status === ''} onClick={() => setStatus('')}>
            any status
          </button>
          {HOST_STATUSES.map((s) => (
            <button key={s} type="button" className="chip" aria-pressed={status === s} onClick={() => setStatus(status === s ? '' : s)}>
              <StatusPill status={s} />
              <span className="n">{fmtNum(statusCounts[s] ?? 0)}</span>
            </button>
          ))}
        </div>
        <div className="chips" role="group" aria-label="Tier">
          {tiers.map(([t, n]) => (
            <button key={t} type="button" className="chip" aria-pressed={tier === t} onClick={() => setTier(tier === t ? '' : t)}>
              <TierPill tier={t} />
              <span className="n">{fmtNum(n)}</span>
            </button>
          ))}
        </div>
      </div>
      <div className="vt" style={{ ['--cols' as string]: GRID, ['--rh' as string]: `${RH}px` }}>
        <div className="vt-head" role="row">
          {COLS.map((c) => (
            <span key={c.key} className={`${c.num ? 'num' : ''}${sort.k === c.key ? ' on' : ''}`} role="columnheader" aria-sort={sort.k === c.key ? (sort.desc ? 'descending' : 'ascending') : 'none'}>
              <button
                type="button"
                title={c.title}
                onClick={() => setSort((s) => (s.k === c.key ? { k: c.key, desc: !s.desc } : { k: c.key, desc: c.num || c.key === 'since' ? c.key !== 'since' : false }))}
              >
                {c.label}
                {sort.k === c.key && <span aria-hidden="true">{sort.desc ? '↓' : '↑'}</span>}
              </button>
            </span>
          ))}
        </div>
        <div
          className="vt-body"
          ref={body}
          tabIndex={0}
          role="grid"
          aria-rowcount={rows.length}
          aria-label="Hosts"
          style={{ height: 'max(320px, calc(100vh - 300px))' }}
          onScroll={(e) => setScroll((e.target as HTMLDivElement).scrollTop)}
        >
          <div style={{ height: rows.length * RH }} />
          {rows.slice(first, last).map((r, i) => {
            const idx = first + i
            return (
              <div
                key={r.host}
                className={`vt-row st-${r.status}${r.host === sel ? ' sel' : ''}`}
                style={{ top: idx * RH }}
                role="row"
                aria-rowindex={idx + 1}
                aria-selected={r.host === sel}
                onClick={(e) => {
                  setSel(r.host)
                  if (e.detail >= 2 || e.metaKey || e.ctrlKey) open(r.host)
                }}
              >
                <a className="host plain" href={`/admin/hosts/${enc(r.host)}`} onClick={(e) => { if (!e.metaKey && !e.ctrlKey) { e.preventDefault(); open(r.host) } }} title={r.host}>
                  {r.host}
                </a>
                <span>
                  <StatusPill status={r.status} />
                </span>
                <span>
                  <TierPill tier={r.tier} />
                </span>
                <span className={`num${r.eventsPerSec === 0 ? ' dim' : ''}`}>{r.eventsPerSec === 0 ? '0' : fmtSi(r.eventsPerSec)}{r.throttle != null && <span className="dim" title={`throttled to ${r.throttle}/s`}> ⌁</span>}</span>
                <span className={`num ${errClass(r.errorRate)}`}>{fmtPct(r.errorRate)}</span>
                <span className="num">{fmtNum(r.accounts)}</span>
                <span className="num mono dim">{r.lastUpstreamSeq}</span>
                <span className="num dim">{r.connectedSinceMs ? relTime(r.connectedSinceMs).replace(' ago', '') : '—'}</span>
                <span className={`num ${lagClass(r.lagMs) || 'dim'}`} title="how far the reader is behind the host's stream">{r.lagMs ? fmtLag(r.lagMs) : '—'}</span>
                <span className="dim">{r.node}</span>
              </div>
            )
          })}
          {rows.length === 0 && <div className="empty">No hosts match.</div>}
        </div>
        <div className="vt-foot">
          <span>
            {fmtNum(rows.length)} of {fmtNum(all.length)} hosts · {fmtSi(totalEvents)} events/s
          </span>
          <span>
            <kbd>j</kbd> <kbd>k</kbd> move · <kbd>↵</kbd> open · <kbd>/</kbd> filter · as of {relTime(l.at ?? now)}
          </span>
        </div>
      </div>
    </>
  )
}
