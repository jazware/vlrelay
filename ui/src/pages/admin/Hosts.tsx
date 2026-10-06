import { useEffect, useMemo } from 'react'
import { DataTable, type Col } from '../../components/console/DataTable'
import { useHostsVersion } from '../../components/console/hostActions'
import { Banners, Chip, Empty, Glyph, HostName, Kbd, Loaded, LiveVal, Meter, PageHead, Panel, SearchInput, Seg, Src, Tiles, TierTag, hostTone, type TileSpec } from '../../components/console/kit'
import { HOST_STATUSES } from '../../components/relay'
import type { HostRow, HostStatus } from '../../lib/api'
import * as A from '../../lib/console/adminAdapter'
import { fmtMs, fmtNum, fmtRatio, fmtSi } from '../../lib/console/fmt'
import { useLivePoll } from '../../lib/console/live'
import { capPoll, overviewPoll, policyFullPoll, policyPoll, slowLagMs, throttledPoll } from '../../lib/console/polls'
import { useRelay } from '../../lib/console/relay'
import { Link, useSearch } from '../../lib/router'
import { Admissions } from './Admissions'
import { crawlDialog } from './Overview'
import { NodeTag, relayBanners } from './relayUi'

// Every PDS the relay knows, thousands of them: the server filters, sorts and pages
// (GET hosts?q&tier&status&sort&desc&limit&offset). The flags (at cap, lagging, erroring) have no
// server filter, so with one on the page asks for every match and filters here.

const PAGE = 100
type Flag = '' | 'cap' | 'lag' | 'err'
const SORTABLE: A.HostSort[] = ['host', 'status', 'events', 'errors', 'accounts', 'lag', 'seq']
const live = (h: HostRow) => h.status === 'connected' || h.status === 'throttled'
const flagOk = (f: Flag, h: HostRow) => (f === 'cap' ? h.maxAccounts > 0 && h.accounts >= h.maxAccounts : f === 'lag' ? live(h) && h.lagMs > 60_000 : f === 'err' ? h.errorRate > 0.1 : true)

function useUrlState() {
  const s = useSearch()
  return {
    q: s.get('q') ?? '',
    tier: s.get('tier') ?? '',
    status: (s.get('status') ?? '') as HostStatus | '',
    flag: (s.get('flag') ?? '') as Flag,
    sort: (s.get('sort') as A.HostSort) || 'events',
    asc: s.get('asc') === '1',
    page: Math.max(0, Number(s.get('page') ?? 0) || 0),
  }
}
type UrlState = ReturnType<typeof useUrlState>

/** Filters live in the URL so a link (from a banner, a case, the palette) lands filtered; replaceState keeps typing smooth. */
function setUrl(p: Partial<UrlState>, cur: UrlState) {
  const n = { ...cur, ...p }
  if (!('page' in p)) n.page = 0
  const s = new URLSearchParams(location.search)
  const put = (k: string, v: string | number | boolean) => (v && v !== 'events' ? s.set(k, String(v === true ? 1 : v)) : s.delete(k))
  put('q', n.q)
  put('tier', n.tier)
  put('status', n.status)
  put('flag', n.flag)
  put('sort', n.sort)
  put('asc', n.asc)
  put('page', n.page)
  const qs = s.toString()
  history.replaceState(null, '', `${location.pathname}${qs ? `?${qs}` : ''}`)
  dispatchEvent(new PopStateEvent('popstate'))
}

export function Hosts() {
  const u = useUrlState()
  const ov = overviewPoll.use()
  const pol = policyPoll.use()
  const polFull = policyFullPoll.use()
  const thr = throttledPoll.use()
  const cap = capPoll.use()
  const { view } = useRelay()
  const v = useHostsVersion()
  const byStatus = ov.data?.hostsByStatus ?? {}
  const total = ov.data?.hostsTotal

  const key = JSON.stringify({ ...u, v })
  const list = useLivePoll(
    () =>
      A.hosts({
        q: u.q.trim().toLowerCase() || undefined,
        tier: u.tier,
        status: u.status,
        sort: u.sort,
        desc: !u.asc,
        ...(u.flag ? { limit: 10_000 } : { limit: PAGE, offset: u.page * PAGE }),
      }),
    key,
    5000,
    { keep: true },
  )
  const { rows, matched } = useMemo(() => {
    const d = list.data
    if (!d) return { rows: [] as HostRow[], matched: 0 }
    if (!u.flag) return { rows: d.hosts, matched: d.total }
    const f = d.hosts.filter((h) => flagOk(u.flag, h))
    return { rows: f.slice(u.page * PAGE, u.page * PAGE + PAGE), matched: f.length }
  }, [list.data, u.flag, u.page])
  const pages = Math.max(1, Math.ceil(matched / PAGE))

  // a tier's host count is one cheap call each (limit 0 still returns the total)
  const tiers = Object.keys(pol.data?.policy.tiers ?? {})
  const tierCounts = useLivePoll(() => Promise.all(tiers.map((t) => A.hosts({ tier: t, sort: 'host', desc: false, limit: 0 }).then((r) => [t, r.total] as const))), `${tiers.join(',')}#${v}`, 30_000, { keep: true })
  const counts = useMemo(() => new Map(tierCounts.data ?? []), [tierCounts.data])

  useEffect(() => {
    if (u.page >= pages && pages > 0 && list.data) setUrl({ page: pages - 1 }, u)
  }, [pages, u, list.data])

  const banners = relayBanners({ view, throttled: thr.data?.hosts, capped: cap.data?.hosts, slowCutMs: slowLagMs(polFull.data), scope: 'hosts' })
  const statusTiles: TileSpec[] = HOST_STATUSES.map((s) => ({
    label: (
      <button type="button" className="cx-tilebtn" onClick={() => setUrl({ status: u.status === s ? '' : s }, u)} aria-pressed={u.status === s}>
        <Glyph k={hostTone(s)} /> {s}
      </button>
    ),
    right: u.status === s ? 'filtered' : undefined,
    value: fmtNum(byStatus[s] ?? 0),
  }))

  const cols: Col<HostRow>[] = [
    {
      id: 'host',
      label: 'Host',
      render: (h) => (
        <span className="cx-cellid">
          <HostName host={h.host} />
          {h.tier && <TierTag t={h.tier} />}
          {h.rule != null && (
            <span className="muted sm" title={`domain rule ${h.rule}`}>
              r{h.rule}
            </span>
          )}
        </span>
      ),
    },
    {
      id: 'status',
      label: 'Status',
      render: (h) => (
        <>
          <Chip k={hostTone(h.status)}>{h.status}</Chip>
          {h.throttle != null && <span className="mono sm muted"> ≤{fmtNum(h.throttle)}/s</span>}
        </>
      ),
    },
    { id: 'events', label: 'Events/s', r: true, render: (h) => <LiveVal className="mono sm">{live(h) ? fmtSi(h.eventsPerSec) : '—'}</LiveVal> },
    {
      id: 'errors',
      label: 'Errors',
      r: true,
      title: 'Rejected frames over all frames, last minute',
      render: (h) => (
        <>
          {h.errorRate > 0.02 && <Meter v={h.errorRate} max={1} k={h.errorRate > 0.25 ? 'err' : h.errorRate > 0.05 ? 'warn' : 'ok'} />} <span className="mono sm">{fmtRatio(h.errorRate)}</span>
        </>
      ),
    },
    {
      id: 'accounts',
      label: 'Accounts / cap',
      r: true,
      render: (h) => (
        <>
          <span className="mono sm">{fmtSi(h.accounts)}</span> {h.maxAccounts > 0 && <Meter v={h.accounts} max={h.maxAccounts} k={h.accounts >= h.maxAccounts ? 'err' : h.accounts > h.maxAccounts * 0.8 ? 'warn' : undefined} />}{' '}
          <span className="muted sm mono">{h.maxAccounts > 0 ? fmtSi(h.maxAccounts) : '—'}</span>
        </>
      ),
    },
    { id: 'lag', label: 'Read lag', r: true, title: 'How far the reader is behind the host’s stream', render: (h) => <span className={`mono sm${live(h) && h.lagMs > 60_000 ? ' s-warn' : ''}`}>{live(h) && h.lagMs ? fmtMs(h.lagMs) : '—'}</span> },
    { id: 'node', label: 'Reader', render: (h) => (live(h) ? <NodeTag view={view} id={h.node} /> : <span className="muted">—</span>) },
    { id: 'seq', label: 'Upstream seq', r: true, render: (h) => <span className="mono sm t2">{fmtNum(h.lastUpstreamSeq)}</span> },
  ]

  return (
    <>
      <PageHead
        title="Hosts"
        sub={
          <>
            <span>{total !== undefined ? `${fmtNum(total)} known PDSes` : '…'}</span>
            {ov.data && <span>{fmtNum(ov.data.hostsConnected)} connected</span>}
            {thr.data && thr.data.total > 0 && <span>{fmtNum(thr.data.total)} throttled</span>}
          </>
        }
        actions={
          <>
            <button type="button" className="cx-btn" onClick={crawlDialog}>
              Request crawl…
            </button>
            <Link className="cx-btn" to="/admin/moderation#rules">
              Domain rules
            </Link>
          </>
        }
      />
      <Banners items={banners} />
      <div className="cx-tilesbox">
        <Tiles tiles={statusTiles} />
      </div>
      <Panel
        title="Every host"
        src={<Src>hosts?q&amp;tier&amp;status&amp;sort&amp;limit&amp;offset</Src>}
        right={
          <span className="muted sm">
            <Kbd k={['j', 'k', '↵']} />
          </span>
        }
        foot={<span>Each host is listed once, by the node reading it. A host whose reader didn't answer shows with no rate.</span>}
      >
        <div className="cx-toolbar">
          <SearchInput mono value={u.q} placeholder="Filter by hostname" onChange={(q) => setUrl({ q }, u)} />
          <Seg label="Tier" value={u.tier} options={[{ v: '', label: 'all tiers' }, ...tiers.map((t) => ({ v: t, label: t, n: counts.has(t) ? fmtNum(counts.get(t)!) : undefined }))]} onChange={(tier) => setUrl({ tier }, u)} />
          <select className="cx-inp" style={{ height: 28 }} value={u.status} onChange={(e) => setUrl({ status: e.target.value as HostStatus | '' }, u)} aria-label="Status">
            <option value="">any status</option>
            {HOST_STATUSES.map((s) => (
              <option key={s}>{s}</option>
            ))}
          </select>
          <Seg<Flag>
            label="Flags"
            value={u.flag}
            options={[
              { v: '', label: 'all' },
              { v: 'cap', label: 'at cap' },
              { v: 'lag', label: 'lagging' },
              { v: 'err', label: 'erroring' },
            ]}
            onChange={(flag) => setUrl({ flag }, u)}
          />
        </div>
        <Loaded load={list}>
          {() => (
            <>
              <DataTable
                rows={rows}
                cols={cols}
                rowKey={(h) => h.host}
                open={(h) => ({ type: 'host', id: h.host })}
                dim={(h) => h.status === 'banned' || h.status === 'suspended'}
                compact
                label="Hosts"
                serverSort={{ id: u.sort, asc: u.asc, sortable: SORTABLE, onSort: (s) => setUrl({ sort: s.id as A.HostSort, asc: s.asc }, u) }}
                empty={<Empty title="No host matches">Try a shorter name or clear the filters.</Empty>}
              />
              <div className="cx-pager">
                <span className="l">
                  {fmtNum(matched)}
                  {total !== undefined && ` of ${fmtNum(total)}`} hosts · showing {fmtNum(rows.length ? u.page * PAGE + 1 : 0)}–{fmtNum(u.page * PAGE + rows.length)}
                  {list.loading && ' · …'}
                </span>
                <button type="button" className="cx-btn sm" disabled={u.page === 0} onClick={() => setUrl({ page: u.page - 1 }, u)}>
                  ← Prev
                </button>
                <span className="mono">
                  {u.page + 1} / {pages}
                </span>
                <button type="button" className="cx-btn sm" disabled={u.page >= pages - 1} onClick={() => setUrl({ page: u.page + 1 }, u)}>
                  Next →
                </button>
              </div>
            </>
          )}
        </Loaded>
      </Panel>
      <div className="cx-grid2 cx-mt">
        <Admissions />
        <Panel title="Tiers" to="/admin/policy" src={<Src>policy · hosts?tier</Src>}>
          {pol.data ? (
            <div className="cx-tw">
              <table className="cx-t compact">
                <thead>
                  <tr>
                    <th>Tier</th>
                    <th className="r">Hosts</th>
                    <th className="r">Events/s</th>
                    <th className="r">Account cap</th>
                    <th className="r">New accounts/h</th>
                  </tr>
                </thead>
                <tbody>
                  {Object.entries(pol.data.policy.tiers).map(([t, l]) => (
                    <tr key={t} data-open={`row:${t}`} onClick={() => setUrl({ tier: u.tier === t ? '' : t }, u)} className={u.tier === t ? 'sel' : undefined}>
                      <td>
                        <TierTag t={t} /> {t === pol.data!.policy.defaultTier && <span className="muted sm">default</span>}
                      </td>
                      <td className="r mono">{counts.has(t) ? fmtNum(counts.get(t)!) : '—'}</td>
                      <td className="r mono">{fmtNum(l.eventsPerSec)}</td>
                      <td className="r mono">{fmtSi(l.maxAccounts)}</td>
                      <td className="r mono">{fmtNum(l.newAccountsPerHour)}</td>
                    </tr>
                  ))}
                </tbody>
              </table>
            </div>
          ) : (
            <Loaded load={pol}>{() => null}</Loaded>
          )}
        </Panel>
      </div>
    </>
  )
}
