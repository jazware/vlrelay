import { Fragment, useMemo, useState } from 'react'
import { InlineConfirm, Live, Tile } from '../components/relay'
import { ErrorNotice, Loading, Notice, Panel } from '../components/ui'
import type { Consumer } from '../lib/api'
import { api, errText } from '../lib/api'
import { fmtBytes, fmtLag, fmtNum, fmtSi, fmtTime, lagClass, relTime } from '../lib/format'
import { useApi, useKey } from '../lib/useApi'

const POLL = 2000

/** Consumer ids are per node. */
const keyOf = (c: Consumer) => `${c.node}/${c.id}`

export function Consumers() {
  const l = useApi<Consumer[]>('consumers', undefined, POLL)
  const [q, setQ] = useState('')
  const [sel, setSel] = useState<string | null>(null)
  const [kick, setKick] = useState<string | null>(null)
  const [busy, setBusy] = useState(false)
  const [err, setErr] = useState<unknown>()
  const [kicked, setKicked] = useState<string>()

  const rows = useMemo(() => {
    const n = q.trim().toLowerCase()
    return (l.data ?? [])
      .filter((c) => !n || c.ip.includes(n) || c.userAgent.toLowerCase().includes(n) || c.node.includes(n))
      .sort((a, b) => Number(b.backfilling) - Number(a.backfilling) || b.lagMs - a.lagMs || a.node.localeCompare(b.node) || a.id - b.id)
  }, [l.data, q])
  const idx = rows.findIndex((c) => keyOf(c) === sel)

  useKey(
    (e) => {
      if (kick !== null) return
      const at = (i: number) => (rows[i] ? keyOf(rows[i]) : null)
      if (e.key === 'j' || e.key === 'ArrowDown') setSel(at(Math.min(rows.length - 1, idx + 1)))
      else if (e.key === 'k' || e.key === 'ArrowUp') setSel(at(Math.max(0, idx - 1)))
      else if (e.key === 'x' && sel !== null) setKick(sel)
      else return
      e.preventDefault()
    },
    [rows, idx, sel, kick],
  )

  if (!l.data) return l.error ? <ErrorNotice error={l.error} /> : <Loading />
  const all = l.data
  const backfilling = all.filter((c) => c.backfilling).length
  const bytes = all.reduce((a, c) => a + c.bytesPerSec, 0)
  const live = all.filter((c) => !c.backfilling)
  const worstLive = live.reduce((a, c) => Math.max(a, c.lagMs), 0)

  const doKick = async () => {
    if (kick === null) return
    setBusy(true)
    setErr(undefined)
    try {
      const c = all.find((x) => keyOf(x) === kick)
      if (!c) throw new Error(`consumer ${kick} is gone`)
      await api(`consumers/${c.id}/kick`, { method: 'POST', params: { node: c.node } })
      setKicked(`${c.ip} (${c.userAgent || 'no user agent'}) on ${c.node}`)
      if (sel === kick) setSel(null)
      setKick(null)
      l.reload()
    } catch (e) {
      setErr(e)
    } finally {
      setBusy(false)
    }
  }

  return (
    <>
      <div className="console-head">
        <h1>Consumers</h1>
        <Live at={l.at} error={l.error} every={POLL} />
      </div>
      <ErrorNotice error={l.error} />
      {kicked && (
        <Notice kind="ok">
          Kicked <span className="mono">{kicked}</span>.{' '}
          <button type="button" className="btn sm quiet" onClick={() => setKicked(undefined)}>
            Dismiss
          </button>
        </Notice>
      )}
      <div className="tiles">
        <Tile k="Connected" v={fmtNum(all.length)} sub={`${fmtNum(live.length)} live, ${fmtNum(backfilling)} replaying`} />
        <Tile k="Bytes out per second" v={fmtBytes(bytes)} />
        <Tile k="Worst live lag" v={fmtLag(worstLive)} tone={worstLive > 500 ? 'warn' : undefined} sub="newest seq minus what was sent" />
        <Tile k="Per node" v={<span className="small">{[...new Set(all.map((c) => c.node))].sort().map((n) => `${n} ${all.filter((c) => c.node === n).length}`).join(' · ')}</span>} />
      </div>
      <Panel
        flush
        title="subscribeRepos connections"
        desc="Replaying consumers (a cursor behind the live head) first, then by lag."
        actions={<input type="search" data-search placeholder="IP, user agent or node   /" value={q} onChange={(e) => setQ(e.target.value)} aria-label="Filter consumers" style={{ width: 280 }} />}
      >
        <div className="table-wrap">
          <table className="data compact">
            <thead>
              <tr>
                <th>IP</th>
                <th>User agent</th>
                <th>Node</th>
                <th>Mode</th>
                <th className="num">Lag</th>
                <th className="num">Cursor</th>
                <th className="num">Events/s</th>
                <th className="num">Bytes/s</th>
                <th className="num">Connected</th>
                <th />
              </tr>
            </thead>
            <tbody>
              {rows.map((c) => (
                <Fragment key={keyOf(c)}>
                  <tr className={`link${keyOf(c) === sel ? ' sel' : ''}`} onClick={() => setSel(keyOf(c))}>
                    <td className="mono">{c.ip}</td>
                    <td className="muted">{c.userAgent || '—'}</td>
                    <td>{c.node}</td>
                    <td>{c.backfilling ? <span className="pill amber">replaying</span> : <span className="pill accent">live</span>}</td>
                    <td className={`num ${lagClass(c.lagMs)}`}>{fmtLag(c.lagMs)}</td>
                    <td className="num mono muted">{c.cursor > 0 ? c.cursor : '—'}</td>
                    <td className="num">{fmtSi(c.eventsPerSec)}</td>
                    <td className="num">{fmtBytes(c.bytesPerSec)}</td>
                    <td className="num muted" title={fmtTime(c.connectedSinceMs)}>
                      {relTime(c.connectedSinceMs).replace(' ago', '')}
                    </td>
                    <td className="num">
                      <button
                        type="button"
                        className="btn sm danger"
                        onClick={(e) => {
                          e.stopPropagation()
                          setErr(undefined)
                          setKick(keyOf(c))
                        }}
                      >
                        Kick
                      </button>
                    </td>
                  </tr>
                  {kick === keyOf(c) && (
                    <tr>
                      <td colSpan={10} style={{ paddingTop: 0 }}>
                        <InlineConfirm open danger action="Kick" busy={busy} error={err ? errText(err) : undefined} onConfirm={doKick} onCancel={() => setKick(null)}>
                          Close the connection from <span className="mono">{c.ip}</span> ({c.userAgent})? A well-behaved client reconnects with its cursor and resumes.
                        </InlineConfirm>
                      </td>
                    </tr>
                  )}
                </Fragment>
              ))}
            </tbody>
          </table>
        </div>
      </Panel>
      <p className="muted small">
        <kbd>j</kbd> <kbd>k</kbd> select · <kbd>x</kbd> kick the selected consumer
      </p>
    </>
  )
}
