import { useEffect, useLayoutEffect, useRef, useState, type ReactNode } from 'react'
import { Mark, ThemeIcon, useThemeToggle } from '../components/console/Shell'
import { Copy, Glyph } from '../components/console/kit'
import { Toasts } from '../components/console/toast'
import type { Health, PublicStats } from '../lib/api'
import { dur, fmtMs, fmtNum, fmtSi, seqS } from '../lib/console/fmt'
import { publicPoll } from '../lib/console/polls'
import { Link } from '../lib/router'

// The page anyone sees at /: what this relay is, how to subscribe, and its live numbers. Only
// /api/public/stats feeds it (aggregates: no hosts, IPs, DIDs or node names), so it needs no
// token. The hostname is the one this page was served on.

const STATUS: Record<Health, { tone: 'ok' | 'warn' | 'err'; title: string; note?: string }> = {
  ok: { tone: 'ok', title: 'Operating normally' },
  degraded: { tone: 'warn', title: 'Degraded', note: 'serving with a node down; nothing is lost' },
  down: { tone: 'err', title: 'Paused', note: 'the firehose is held while the relay recovers; reconnect with your cursor' },
}

function Status({ s }: { s?: PublicStats; error?: unknown }) {
  if (!s) return <span className="muted">Checking…</span>
  const h = STATUS[s.health]
  return (
    <>
      <Glyph k={h.tone} />
      <b className={`s-${h.tone}`}>{h.title}</b>
      {h.note && <span className="t2">{h.note}</span>}
    </>
  )
}

type Line = { data: number[]; color: string; label: string; dashed?: boolean; value: string }

/** A small time chart: filled first series, dashed others, the last 5 minutes. */
function PubChart({ title, sub, t, lines, fmt }: { title: string; sub: string; t: number[]; lines: Line[]; fmt: (v: number) => string }) {
  const box = useRef<HTMLDivElement>(null)
  const [W, setW] = useState(600)
  useLayoutEffect(() => {
    const el = box.current
    if (!el) return
    const ro = new ResizeObserver(() => setW(el.clientWidth || 600))
    ro.observe(el)
    return () => ro.disconnect()
  }, [])
  const H = 150
  const pl = 46
  const pb = 18
  const pt = 6
  const n = t.length
  const max = Math.max(1e-9, ...lines.flatMap((l) => l.data)) * 1.15
  const X = (i: number) => pl + (n > 1 ? i / (n - 1) : 0) * (W - pl - 4)
  const Y = (v: number) => pt + (1 - v / max) * (H - pt - pb)
  const span = n > 1 ? t[n - 1] - t[0] : 0
  const ticks = span > 0 ? [0, 1 / 3, 2 / 3, 1].map((f) => ({ f, l: f === 1 ? 'now' : `−${Math.round(span * (1 - f))} s` })) : []
  return (
    <div className="cx-chart">
      <h3>{title}</h3>
      <p>{sub}</p>
      <div ref={box}>
        <svg viewBox={`0 0 ${W} ${H}`} width={W} height={H} role="img" aria-label={title}>
          {[0, 0.5, 1].map((f) => (
            <g key={f}>
              <line className="gr" x1={pl} x2={W - 4} y1={Y(max * f)} y2={Y(max * f)} />
              <text className="ax" x={pl - 6} y={Y(max * f) + 3} textAnchor="end">
                {fmt(max * f)}
              </text>
            </g>
          ))}
          {ticks.map(({ f, l }) => (
            <text key={f} className="ax" x={pl + f * (W - pl - 4)} y={H - 4} textAnchor={f === 0 ? 'start' : f === 1 ? 'end' : 'middle'}>
              {l}
            </text>
          ))}
          {lines.map((l) => {
            if (l.data.length < 2) return null
            const d = l.data.map((v, i) => `${i ? 'L' : 'M'}${X(i).toFixed(1)} ${Y(v).toFixed(1)}`).join('')
            return (
              <g key={l.label}>
                {!l.dashed && <path d={`${d}L${X(l.data.length - 1)} ${H - pb}L${pl} ${H - pb}Z`} fill={`color-mix(in oklab, var(--${l.color}) 13%, transparent)`} />}
                <path d={d} fill="none" stroke={`var(--${l.color})`} strokeWidth="1.6" strokeDasharray={l.dashed ? '4 3' : undefined} />
                <circle cx={X(l.data.length - 1)} cy={Y(l.data[l.data.length - 1])} r="3" fill={`var(--${l.color})`} />
              </g>
            )
          })}
        </svg>
      </div>
      <div className="cx-legend">
        {lines.map((l) => (
          <span key={l.label}>
            <i style={l.dashed ? { borderTop: `1.5px dashed var(--${l.color})`, background: 'none' } : { background: `var(--${l.color})` }} />
            {l.label} <b className="mono">{l.value}</b>
          </span>
        ))}
      </div>
    </div>
  )
}

const Fig = ({ v, unit, label, em }: { v: ReactNode; unit?: string; label: string; em?: ReactNode }) => (
  <div>
    <b>
      {v}
      {unit && <small>{unit}</small>}
    </b>
    <span>{label}</span>
    {em && <em>{em}</em>}
  </div>
)

/** Whether /admin answers from here: a proxy in front of a public relay usually hides it. */
function useConsoleReachable() {
  const [ok, setOk] = useState(false)
  useEffect(() => {
    let live = true
    fetch('/admin', { method: 'HEAD' })
      .then((r) => live && setOk(r.ok))
      .catch(() => {})
    return () => {
      live = false
    }
  }, [])
  return ok
}

export function Public() {
  const l = publicPoll.use()
  const s = l.data
  const { theme, toggle } = useThemeToggle()
  const host = location.host
  const dot = /^\d+(\.\d+){3}(:\d+)?$/.test(host) ? -1 : host.indexOf('.')
  const ws = `${location.protocol === 'https:' ? 'wss' : 'ws'}://${host}/xrpc/com.atproto.sync.subscribeRepos`
  const consoleHere = useConsoleReachable()
  useEffect(() => {
    document.title = `${location.hostname} · vlRelay`
  }, [])
  const h = s?.history
  const quorum = s?.quorum ?? null
  return (
    <div className="cx cx-pubroot" data-theme-resolved={theme}>
      <header className="cx-pub-top">
        <Link to="/" className="cx-wordmark" aria-label="vlRelay">
          <Mark />
          vlRelay
        </Link>
        <Link to="/docs" className="l hide-sm">
          Docs
        </Link>
        {consoleHere && (
          <Link to="/admin" className="l">
            Console
          </Link>
        )}
        <span className="cx-spacer" />
        <button type="button" className="cx-iconbtn" onClick={toggle} title="Toggle theme" aria-label={`Switch to ${theme === 'dark' ? 'light' : 'dark'} theme`}>
          <ThemeIcon />
        </button>
      </header>
      <main className="cx-pub-in">
        <section className="cx-hero">
          <div>
            <h1 style={{ '--n': host.length } as React.CSSProperties}>
              {dot > 0 ? (
                <>
                  {host.slice(0, dot)}
                  <wbr />
                  <span className="d">{host.slice(dot)}</span>
                </>
              ) : (
                host
              )}
            </h1>
            <p>
              An AT Protocol relay. It follows every PDS it knows of, checks each commit's signature and history, and serves the merged stream as one firehose that apps, feeds and labelers
              subscribe to.
            </p>
            <div className="cx-form-row" style={{ gap: 10 }}>
              <Copy text={ws} mono={false}>
                <span className="cx-btn primary">Copy the firehose URL</span>
              </Copy>
              <Link to="/docs/subscribing" className="cx-btn">
                How to subscribe
              </Link>
              {consoleHere && (
                <Link to="/admin" className="cx-btn quiet">
                  Operator console
                </Link>
              )}
            </div>
            <div className="cx-pubstat" aria-live="polite">
              {l.error && !s ? (
                <>
                  <Glyph k="err" />
                  <b className="s-err">Stats unavailable</b>
                </>
              ) : (
                <Status s={s} />
              )}
              {s && (
                <span className="muted">
                  vlRelay {s.version} · up {dur(s.uptimeSecs * 1000)}
                </span>
              )}
            </div>
          </div>
          <aside className="cx-subcard" id="subscribe" aria-labelledby="sub-h">
            <h2 id="sub-h">Subscribe</h2>
            <span className="t2 sm">
              One websocket carries every commit, identity and account event the relay has checked, in order.{quorum !== null && ' Every node sends the same seqs, so you can resume on any of them.'}
            </span>
            <dl className="ep">
              <dt>Firehose</dt>
              <dd>
                <Copy text={ws} />
              </dd>
              <dt>Resume</dt>
              <dd>?cursor=&lt;seq&gt; replays from a seq you saw</dd>
              <dt>Sync API</dt>
              <dd>/xrpc/com.atproto.sync.*</dd>
              <dt>Newest seq</dt>
              <dd className="s-sig">{s ? seqS(s.lastSeq) : '…'}</dd>
            </dl>
            <pre>websocat '{ws}'</pre>
          </aside>
        </section>

        <section className="cx-bigfig" aria-label="Live numbers">
          <Fig v={s ? fmtSi(s.eventsInPerSec) : '…'} unit="/s" label="Events in" em="frames read from PDSes" />
          <Fig v={s ? fmtSi(s.streamEventsPerSec || s.eventsOutPerSec) : '…'} unit="/s" label="Firehose events" em={s && `${fmtSi(s.eventsOutPerSec)}/s sent to all consumers`} />
          <Fig v={s ? fmtMs(s.timeToFirehoseP50Ms) : '…'} label="Time to firehose, p50" em={s && `p99 ${fmtMs(s.timeToFirehoseP99Ms)}`} />
          <Fig v={s ? fmtNum(s.hostsConnected) : '…'} label="Connected PDS hosts" em="sockets open now" />
          <Fig v={s ? fmtNum(s.consumers) : '…'} label="Consumers" em="firehose subscribers" />
        </section>

        {h && h.t.length > 1 && (
          <section className="cx-grid2">
            <PubChart
              title={`Events, last ${Math.round((h.t[h.t.length - 1] - h.t[0]) / 60) || 1} minutes`}
              sub="Per second: frames in from PDSes, and events sent to all consumers."
              t={h.t}
              fmt={fmtSi}
              lines={[
                { data: h.eventsIn, color: 'accent', label: 'in', value: fmtSi(s!.eventsInPerSec) },
                { data: h.eventsOut, color: 'signal', label: 'sent', dashed: true, value: fmtSi(s!.eventsOutPerSec) },
              ]}
            />
            <PubChart
              title="Time to firehose"
              sub="From a PDS frame arriving to it going out, p50 and p99."
              t={h.t}
              fmt={fmtMs}
              lines={[
                { data: h.ttfP50Ms, color: 'c2', label: 'p50', value: fmtMs(s!.timeToFirehoseP50Ms) },
                { data: h.ttfP99Ms, color: 'warn', label: 'p99', dashed: true, value: fmtMs(s!.timeToFirehoseP99Ms) },
              ]}
            />
          </section>
        )}

        <section className="cx-grid2">
          <div>
            <h2 className="cx-pubh">This relay</h2>
            <dl className="cx-kv">
              <dt>Nodes</dt>
              <dd>{s ? `${s.nodesHealthy} of ${s.nodes} healthy` : '…'}</dd>
              {quorum && (
                <>
                  <dt>Quorum log</dt>
                  <dd>
                    <Glyph k={STATUS[quorum].tone} /> {quorum === 'ok' ? 'Every member answering' : quorum === 'degraded' ? 'A member is down; still committing' : 'Waiting for a majority'}
                  </dd>
                </>
              )}
              <dt>Newest seq</dt>
              <dd className="mono s-sig">{s ? seqS(s.lastSeq) : '…'}</dd>
              <dt>Version</dt>
              <dd className="mono">{s?.version ?? '…'}</dd>
            </dl>
          </div>
          <div>
            <h2 className="cx-pubh">How an event gets to you</h2>
            <div className="cx-howto">
              <div>
                <b>Read from its PDS</b>The relay keeps a socket open to each PDS it knows of, within that host's rate limits.
              </div>
              <div>
                <b>Checked</b>Signature, repo history and account status are verified before anything is passed on.
              </div>
              {quorum !== null ? (
                <div>
                  <b>Held by a majority</b>The leader numbers it and a majority of nodes hold it before it goes out, so a node failing loses nothing you saw.
                </div>
              ) : (
                <div>
                  <b>Numbered</b>Each event gets the next seq and is kept for replay, so a cursor can pick up where it left off.
                </div>
              )}
              <div>
                <b>Sent on the firehose</b>Every subscriber gets the same order, live or replaying from a cursor.
              </div>
            </div>
          </div>
        </section>
        <footer className="cx-pubfoot">
          <span>
            vlRelay <span className="mono">{s?.version ?? ''}</span>
          </span>
          <Link to="/docs">Docs</Link>
          <a href="/api/public/stats">Stats JSON</a>
          <a href="/xrpc/_health">Health</a>
          {consoleHere && <Link to="/admin">Operator console</Link>}
        </footer>
      </main>
      <Toasts />
    </div>
  )
}
