import { useEffect, useMemo, type ReactNode } from 'react'
import { Chart } from '../components/Chart'
import { Sparkline } from '../components/relay'
import { CopyText, Status, Topbar } from '../components/ui'
import { publicStats, type Health, type PublicStats } from '../lib/api'
import { fmtNum, fmtSi } from '../lib/format'
import { useLoad } from '../lib/hooks'
import { Link } from '../lib/router'
import './public.css'

const POLL = 2000

const fmtMs = (v: number) => (v >= 1000 ? `${(v / 1000).toFixed(2)} s` : `${v.toFixed(v < 10 ? 1 : 0)} ms`)

function fmtUptime(s: number): string {
  const d = Math.floor(s / 86400)
  const h = Math.floor((s % 86400) / 3600)
  const m = Math.floor((s % 3600) / 60)
  if (d > 0) return `${d}d ${h}h`
  if (h > 0) return `${h}h ${m}m`
  return `${m}m`
}

const HEALTH: Record<Health, { kind: 'ok' | 'warn' | 'bad'; label: string }> = {
  ok: { kind: 'ok', label: 'Operating normally' },
  degraded: { kind: 'warn', label: 'Degraded: serving, with a node down' },
  down: { kind: 'bad', label: 'Down' },
}

export function Public() {
  const l = useLoad<PublicStats>(publicStats, [], POLL)
  const s = l.data
  const [host, port] = location.host.split(':')
  const ws = location.protocol === 'https:' ? 'wss' : 'ws'
  const firehose = `${ws}://${location.host}/xrpc/com.atproto.sync.subscribeRepos`
  useEffect(() => {
    document.title = `${host} · vlRelay`
  }, [host])
  const h = s?.history
  const charts = useMemo(
    () =>
      h && {
        events: [h.t, h.eventsIn, h.eventsOut],
        latency: [h.t, h.ttfP50Ms, h.ttfP99Ms],
      },
    [h],
  )
  const health = s ? HEALTH[s.health] : undefined

  return (
    <>
      <Topbar />
      <main className="pub">
        <section className="pub-hero">
          <div>
            <h1 className="host">
              {host}
              {port && <span className="port">:{port}</span>}
            </h1>
            <p className="lede">
              An AT Protocol relay. It follows every PDS it knows of, checks each commit's signature and history, and serves the merged
              stream as one firehose that apps, feeds and labelers subscribe to.
            </p>
            <div className="actions">
              <a href="#subscribe" className="btn primary">
                Subscribe to the firehose
              </a>
              <Link to="/docs" className="btn">
                Read the docs
              </Link>
              <Link to="/admin" className="btn quiet">
                Operator console
              </Link>
            </div>
            <div className="pub-status" aria-live="polite">
              {health ? <Status kind={health.kind}>{health.label}</Status> : l.error ? <Status kind="bad">Stats unavailable</Status> : <Status kind="idle">Checking…</Status>}
              {s && (
                <span className="muted small">
                  vlRelay <span className="mono">{s.version}</span>, up {fmtUptime(s.uptimeSecs)}
                </span>
              )}
            </div>
          </div>
          <aside className="pub-sub" id="subscribe" aria-labelledby="sub-h">
            <h2 id="sub-h">Subscribe</h2>
            <p className="muted">One websocket carries every commit, identity and account event the relay has checked, in order.</p>
            <dl className="dl compact">
              <dt>Firehose</dt>
              <dd>
                <CopyText text={firehose} />
              </dd>
              <dt>Resume</dt>
              <dd>
                <span className="mono">?cursor=&lt;seq&gt;</span>
                <span className="muted small"> replays from a seq you saw</span>
              </dd>
              <dt>Sync API</dt>
              <dd>
                <span className="mono">/xrpc/com.atproto.sync.*</span>
              </dd>
            </dl>
            <pre className="pub-cmd" aria-label="Example">
              <code>websocat '{firehose}'</code>
            </pre>
            <Link to="/docs/subscribing" className="pub-more">
              How to consume it, limits and cursors <span aria-hidden="true">&rarr;</span>
            </Link>
          </aside>
        </section>

        <section aria-label="Live numbers">
          <div className="tiles hero pub-tiles">
            <PubTile k="Events in per second" v={s && fmtSi(s.eventsInPerSec)} sub="frames read from PDSes" spark={h?.eventsIn} color="c1" />
            <PubTile
              k="Firehose events per second"
              v={s && fmtSi(s.streamEventsPerSec || s.eventsOutPerSec)}
              sub={s && `${fmtSi(s.eventsOutPerSec)}/s sent to all consumers`}
              spark={h?.eventsOut}
              color="c2"
            />
            <PubTile
              k="Time to firehose p50 / p99"
              v={
                s && (
                  <>
                    {fmtMs(s.timeToFirehoseP50Ms)}
                    <small>/ {fmtMs(s.timeToFirehoseP99Ms)}</small>
                  </>
                )
              }
              sub="PDS frame received → sent to consumers"
              spark={h?.ttfP99Ms}
              color="c3"
            />
            <PubTile k="Connected PDS hosts" v={s && fmtNum(s.hostsConnected)} sub="sockets open right now" />
            <PubTile k="Consumers" v={s && fmtNum(s.consumers)} sub="firehose subscribers" />
          </div>
        </section>

        {charts && (
          <div className="grid2">
            <section className="panel">
              <div className="body flush">
                <Chart
                  title="Events, last 5 minutes"
                  sub="Per second: frames in from PDSes, and the firehose out."
                  series={[
                    { label: 'in', color: 'c1' },
                    { label: 'out (all consumers)', color: 'c2', dash: true },
                  ]}
                  data={charts.events}
                  fmt={fmtSi}
                />
              </div>
            </section>
            <section className="panel">
              <div className="body flush">
                <Chart
                  title="Time to firehose"
                  sub="From a PDS frame arriving to it going out, p50 and p99."
                  series={[
                    { label: 'p50', color: 'c1' },
                    { label: 'p99', color: 'c3' },
                  ]}
                  data={charts.latency}
                  fmt={fmtMs}
                />
              </div>
            </section>
          </div>
        )}

        <section className="pub-facts" aria-labelledby="facts-h">
          <div>
            <h2 id="facts-h">This relay</h2>
            <dl className="dl">
              <dt>Nodes</dt>
              <dd>{s ? `${s.nodesHealthy} of ${s.nodes} healthy` : '…'}</dd>
              {s?.quorum && (
                <>
                  <dt>Quorum log</dt>
                  <dd>
                    <Status kind={HEALTH[s.quorum].kind}>{s.quorum === 'ok' ? 'Every member answering' : s.quorum === 'degraded' ? 'A member is down; still committing' : 'No quorum'}</Status>
                  </dd>
                </>
              )}
              <dt>Newest seq</dt>
              <dd className="mono">{s ? fmtNum(s.lastSeq) : '…'}</dd>
              <dt>Version</dt>
              <dd className="mono">{s?.version ?? '…'}</dd>
            </dl>
          </div>
          <div className="how">
            <div className="segments" aria-hidden="true">
              <div style={{ width: '100%' }} />
              <div style={{ width: '78%', opacity: 0.7 }} />
              <div style={{ width: '54%', opacity: 0.45 }} />
              <div style={{ width: '30%', opacity: 0.25, background: 'var(--amber)' }} />
            </div>
            <h2>How an event gets to you</h2>
            <ol>
              <li>
                <strong>Read from its PDS</strong>
                The relay keeps a socket open to each PDS it knows of, within that host's rate limits.
              </li>
              <li>
                <strong>Checked</strong>
                Signature, repo history and account status are verified before anything is passed on.
              </li>
              <li>
                <strong>Made durable</strong>
                Events are numbered and written to object storage, so a cursor can replay them after a restart.
              </li>
              <li>
                <strong>Sent on the firehose</strong>
                Every subscriber gets the same order, live or replaying from a cursor.
              </li>
            </ol>
          </div>
        </section>
      </main>
      <footer className="footer">
        <span>vlRelay{s && <span className="mono muted"> {s.version}</span>}</span>
        <Link to="/docs">Docs</Link>
        <a href="/xrpc/_health">Health</a>
        <a href="/api/public/stats">Stats JSON</a>
        <Link to="/admin">Operator console</Link>
      </footer>
    </>
  )
}

function PubTile({ k, v, sub, spark, color }: { k: string; v?: ReactNode; sub?: ReactNode; spark?: number[]; color?: string }) {
  return (
    <div className="tile big">
      <div className="v">{v ?? '…'}</div>
      <div className="k">{k}</div>
      {sub && <div className="tsub">{sub}</div>}
      {spark && color && (
        <div className="pub-spark">
          <Sparkline values={spark.slice(-120)} color={color} width={180} height={28} />
        </div>
      )}
    </div>
  )
}
