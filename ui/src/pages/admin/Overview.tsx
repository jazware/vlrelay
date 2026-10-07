import { useEffect, useState } from 'react'
import { openDialog, FormDialog } from '../../components/console/dialogs'
import { Exchange } from '../../components/console/Exchange'
import { Banners, Bars, Empty, Glyph, HealthLine, HostName, Kbd, Loaded, LiveVal, PageHead, Panel, RRow, Seg, Spark, Src, Swatch, Tiles, type BannerSpec, type HealthCell, type TileSpec, type Tone } from '../../components/console/kit'
import { LiveTail } from '../../components/console/LiveTail'
import { openPanel } from '../../components/console/nav'
import { toast } from '../../components/console/toast'
import type { HostRow, Overview as O, RejectReason } from '../../lib/api'
import * as A from '../../lib/console/adminAdapter'
import { ago, dur, fmtBytes, fmtMs, fmtNum, fmtRatio, fmtSi, plural, seqS, since } from '../../lib/console/fmt'
import { togglePaused, useLiveState, useLivePoll } from '../../lib/console/live'
import { capPoll, consumersPoll, historyPoll, isSlow, openCasesPoll, overviewPoll, policyFullPoll, publicPoll, quorumPoll, seenEpochs, seriesOf, slowLagMs, throttledPoll } from '../../lib/console/polls'
import { useRelay, type RelayView } from '../../lib/console/relay'
import { navigate } from '../../lib/router'
import { currentLead, epochEvents, leaderChangeText, recentLeaderChange, type EpochEvent } from './quorumUi'
import { Lg, NodeTag, REASON_WHAT, reasonLabel, relayBanners } from './relayUi'

// The relay at a glance: what needs attention, one line of health, the figures, the exchange
// (PDS trunks → cores → leader → firehose → serving nodes), why frames are rejected, the
// busiest hosts, a sample of the firehose, and a rail with the log, members, consumers and cases.

const sumAt = (h: O['history']) => h.t.map((_, i) => Object.values(h.rejects).reduce((a, s) => a + (s?.[i] ?? 0), 0))

export function crawlDialog(initial = '') {
  openDialog((close) => <CrawlForm close={close} initial={initial} />)
}

function CrawlForm({ close, initial }: { close: () => void; initial: string }) {
  const [host, setHost] = useState(initial)
  const [busy, setBusy] = useState(false)
  const [error, setError] = useState<unknown>()
  const v = host.trim().toLowerCase().replace(/^https?:\/\//, '').replace(/\/+$/, '')
  const ok = /^[a-z0-9-]+(\.[a-z0-9-]+)+(:\d+)?$/.test(v)
  return (
    <FormDialog
      title="Request a crawl"
      action="Request crawl"
      call={`POST /xrpc/com.atproto.sync.requestCrawl {"hostname":"${v || '…'}"}`}
      busy={busy}
      disabled={!ok}
      error={error}
      onCancel={close}
      onSubmit={async () => {
        setBusy(true)
        setError(undefined)
        try {
          await A.requestCrawl(v)
          toast(`Requested a crawl of ${v}`)
          close()
        } catch (e) {
          setError(e)
        } finally {
          setBusy(false)
        }
      }}
    >
      <div>
        <label className="cx-lbl" htmlFor="crh">
          Hostname
        </label>
        <input id="crh" className="cx-inp mono" autoFocus placeholder="pds.example.com" value={host} onChange={(e) => setHost(e.target.value)} autoComplete="off" spellCheck={false} />
      </div>
      <p className="muted sm" style={{ margin: 0 }}>
        The same path a PDS takes: the relay checks the crawl switch, domain rules, bans and today's new-host budget, then probes the host before it connects.
      </p>
    </FormDialog>
  )
}

/** The newest seq, moving with the stream's rate between polls so the counter reads as live. */
function useSeqNow(seq: number | undefined, rate: number, at: number | undefined) {
  const [, setT] = useState(0)
  const live = useLiveState()
  useEffect(() => {
    if (live.paused || live.stale) return
    const id = setInterval(() => setT((t) => t + 1), 200)
    return () => clearInterval(id)
  }, [live.paused, live.stale])
  if (seq === undefined) return undefined
  if (!at || live.paused || live.stale) return seq
  return seq + Math.min(2.5, (Date.now() - at) / 1000) * rate
}

/** The banner for a leader change in the last 30 minutes, with a way into its epoch. */
export function leaderBanner(e: EpochEvent): BannerSpec {
  const t = leaderChangeText(e)
  return {
    id: 'leader',
    tone: 'info',
    title: t.title,
    desc: t.desc,
    right: (
      <button type="button" className="cx-btn sm" onClick={() => openPanel('epoch', e.id)}>
        Open epoch ›
      </button>
    ),
  }
}

const REJECTS_TO = '/admin/hosts?sort=errors'
type Busy = 'events' | 'rejects'

function healthCells(o: O, view: RelayView | undefined, cases: number, crit: number, throttled: number, consumers: { n: number; slow: number; backfill: number } | undefined, leadSince?: number): HealthCell[] {
  const q = view?.quorum
  const stream = o.streamEventsPerSec ?? o.eventsOutPerSec
  const rejPct = (o.rejectsPerSec / Math.max(1, o.eventsInPerSec)) * 100
  const held = q?.health === 'down'
  const cells: HealthCell[] = [
    { label: 'Firehose', tone: held ? 'err' : 'ok', value: held ? 'held' : fmtSi(stream), unit: held ? undefined : 'ev/s', sub: held ? `nothing past ${seqS(q?.commit)}` : `seq ${seqS(o.lastSeq)}`, to: '/admin/consumers', title: 'Who reads the stream, and how far behind' },
    {
      label: 'Time to firehose',
      tone: o.timeToFirehoseP99Ms > 250 ? 'warn' : 'ok',
      value: fmtMs(o.timeToFirehoseP99Ms),
      unit: 'p99',
      sub: `p50 ${fmtMs(o.timeToFirehoseP50Ms)}`,
      to: q ? '/admin/quorum' : '/admin/consumers',
      title: q ? 'Most of it is the commit: the quorum page has its latency' : undefined,
    },
  ]
  if (q) {
    const tone: Tone = q.health === 'down' ? 'err' : q.health === 'degraded' ? 'warn' : 'ok'
    cells.push({ label: 'Quorum', tone, value: `${q.answering.length}`, unit: `of ${q.members.length}`, sub: held ? 'no leader with a majority' : leadSince ? `${q.leader ?? '—'} leads · since ${since(leadSince)}` : `epoch ${q.epoch} · ${q.leader ?? '—'} leads`, to: '/admin/quorum', title: `epoch ${q.epoch}` })
    cells.push({
      label: 'Flush to bucket',
      tone: held ? 'err' : 'ok',
      value: fmtNum(Math.max(0, q.commit - q.flushed)),
      unit: 'unflushed',
      sub: q.lead?.flush?.last_at_ms ? `flushed ${ago(q.lead.flush.last_at_ms)} · R +${fmtSi(Math.max(0, q.reserve - q.commit))}` : `F ${seqS(q.flushed)} · R +${fmtSi(Math.max(0, q.reserve - q.commit))}`,
      to: '/admin/quorum',
      title: 'Committed entries the leader hasn’t flushed to the bucket yet (above F), and the headroom to R',
    })
  } else if (view && !view.single) {
    const down = view.nodes.filter((n) => n.stale).length
    cells.push({ label: 'Nodes', tone: down ? 'warn' : 'ok', value: `${view.nodes.length - down}`, unit: `of ${view.nodes.length}`, sub: down ? `${down} not answering` : 'all answering', to: '/admin/quorum' })
  }
  cells.push(
    { label: 'PDS hosts', tone: throttled > 3 ? 'warn' : 'ok', value: fmtNum(o.hostsConnected), sub: `connected of ${fmtNum(o.hostsTotal)}${throttled ? ` · ${throttled} thr` : ''}`, to: '/admin/hosts' },
    { label: 'Rejects', tone: rejPct > 2 ? 'warn' : 'ok', value: fmtSi(o.rejectsPerSec), unit: '/s', sub: 'hosts by rejects ›', to: REJECTS_TO, title: `${rejPct.toFixed(2)}% of frames` },
    { label: 'Consumers', tone: consumers?.slow ? 'warn' : 'ok', value: fmtNum(o.consumers), sub: consumers ? `${consumers.slow} slow · ${consumers.backfill} backfilling` : 'subscribeRepos', to: '/admin/consumers' },
    { label: 'Cases', tone: crit ? 'err' : cases ? 'warn' : 'ok', value: fmtNum(cases), unit: 'open', sub: crit ? `${crit} critical` : 'none critical', to: '/admin/moderation' },
  )
  return cells
}

export function Overview() {
  const ov = overviewPoll.use()
  const { view } = useRelay()
  const pub = publicPoll.use()
  const cases = openCasesPoll.use()
  const subs = consumersPoll.use()
  const thr = throttledPoll.use()
  const cap = capPoll.use()
  const pol = policyFullPoll.use()
  const live = useLiveState()
  const o = ov.data
  const stream = o ? (o.streamEventsPerSec ?? o.eventsOutPerSec) : 0
  const seqNow = useSeqNow(view?.quorum?.commit ?? o?.lastSeq, stream, ov.at)
  const cut = slowLagMs(pol.data)
  const q = view?.quorum
  const cores = view?.nodes.filter((n) => n.core) ?? []
  const qp = quorumPoll.use()
  // the shell keeps the history poll running on a quorum relay
  const hist = historyPoll.get()
  const events = q ? epochEvents(qp.data?.supported ? qp.data.data : undefined, hist.data?.events ?? [], seenEpochs()) : []
  const lead = currentLead(events)
  const [busy, setBusy] = useState<Busy>('events')
  const rej = useLivePoll(() => (busy === 'rejects' ? A.hosts({ sort: 'errors', desc: true, limit: 8 }).then((r) => r.hosts) : Promise.resolve([] as HostRow[])), `busy:${busy}`, 5000)
  const rejNames = (rej.data ?? []).filter((h) => h.errorRate > 0).map((h) => h.host)

  const sub = (
    <>
      <span className="mono">{location.host}</span>
      {view && <span>{q ? `${q.members.length} quorum members${q.learners.length ? ` + ${q.learners.length} learning` : ''}` : view.single ? 'a single node' : `${view.nodes.length} nodes`}</span>}
      {pub.data && <span>up {dur(pub.data.uptimeSecs * 1000)}</span>}
    </>
  )
  const actions = (
    <>
      <button type="button" className="cx-btn" onClick={togglePaused}>
        {live.paused ? '▶ Resume' : '❚❚ Pause'} <Kbd k="space" />
      </button>
      <button type="button" className="cx-btn" onClick={() => crawlDialog()}>
        Request crawl…
      </button>
    </>
  )
  if (!o)
    return (
      <>
        <PageHead title="Overview" sub={sub} actions={actions} />
        <Loaded load={ov}>{() => null}</Loaded>
      </>
    )

  const h = o.history
  const rejSeries = sumAt(h)
  const crit = cases.data?.filter((c) => c.severity === 'critical').length ?? 0
  const slow = subs.data?.filter((c) => isSlow(c, cut)).length ?? 0
  const consumers = subs.data ? { n: subs.data.length, slow, backfill: subs.data.filter((c) => c.backfilling).length } : undefined
  const banners = relayBanners({ view, throttled: thr.data?.hosts, backpressure: ov.data?.hostsByStatus.backpressure, capped: cap.data, consumers: subs.data, slowCutMs: cut, scope: 'overview' })
  const changed = q && q.health !== 'down' ? recentLeaderChange(events) : undefined
  // after the quorum's own banner (held or degraded), before the rest
  if (changed) banners.splice(banners[0]?.id === 'degraded' ? 1 : 0, 0, leaderBanner(changed))
  const commitP99 = seriesOf('commit-p99')
  const tiles: TileSpec[] = [
    { label: 'Frames in', right: 'from PDSes', value: fmtSi(o.eventsInPerSec), unit: '/s', spark: <Spark data={h.eventsIn} color="accent" />, to: '/admin/hosts' },
    { label: 'Firehose', right: 'merged stream', value: fmtSi(stream), unit: '/s', spark: <Spark data={seriesOf('stream')} color="signal" /> },
    { label: 'Sent', right: 'all consumers', value: fmtSi(o.eventsOutPerSec), unit: '/s', sec: `${fmtBytes(o.bytesOutPerSec)}/s`, spark: <Spark data={h.eventsOut} color="c5" />, to: '/admin/consumers' },
    { label: 'Time to firehose', right: 'p99 · p50 dashed', value: fmtMs(o.timeToFirehoseP99Ms), sec: fmtMs(o.timeToFirehoseP50Ms), spark: <Spark data={h.ttfP99Ms} l2={h.ttfP50Ms} color="warn" /> },
    { label: 'Rejects', right: `${((o.rejectsPerSec / Math.max(1, o.eventsInPerSec)) * 100).toFixed(2)}%`, value: fmtSi(o.rejectsPerSec), unit: '/s', spark: <Spark data={rejSeries} color="err" /> },
    { label: 'Durability lag', right: 'oldest not yet durable', value: fmtMs(o.commitLagMs), spark: <Spark data={h.durabilityLagMs} color="violet" /> },
    q
      ? { label: 'Commit', right: 'p99 · p50 dashed', value: commitP99.length ? fmtMs(commitP99[commitP99.length - 1]) : '—', sec: fmtMs(seriesOf('commit-p50').slice(-1)[0]), spark: <Spark data={commitP99} l2={seriesOf('commit-p50')} color="signal" />, to: '/admin/quorum' }
      : { label: 'Consumers', value: fmtNum(o.consumers), sec: `${fmtBytes(o.bytesOutPerSec)}/s`, to: '/admin/consumers' },
    { label: 'Read from PDSes', right: 'bytes in', value: `${fmtBytes(o.bytesInPerSec)}/s`, spark: <Spark data={h.bytesIn} color="accent" />, to: '/admin/hosts' },
  ]
  const reasons = (Object.entries(o.rejectsByReason) as [RejectReason, number][]).filter(([, v]) => v > 0).sort((a, b) => b[1] - a[1]).slice(0, 8)

  return (
    <>
      <PageHead title="Overview" sub={sub} actions={actions} />
      <Banners items={banners} />
      <HealthLine cells={healthCells(o, view, cases.data?.length ?? o.openCases, crit, thr.data?.total ?? 0, consumers, lead && lead.leader === q?.leader ? lead.atMs : undefined)} />
      <div className="cx-ov">
        <div className="cx-stack">
          <div className="cx-tilesbox t4" style={{ margin: 0 }}>
            <Tiles tiles={tiles} />
          </div>
          <Panel
            title="The exchange"
            src={
              <>
                <Src>overview · cluster · cluster/quorum</Src> <Src>overview.topHosts[].history</Src>
              </>
            }
            right={
              <span className="cx-legend">
                <Lg color="ink3">PDS trunks</Lg>
                <Lg color="signal">committed, emitted</Lg>
                <Lg color="err">rejected</Lg>
              </span>
            }
          >
            <div className="cx-cvwrap">
              <Exchange o={o} view={view} />
              <div className="cx-cvnote">
                <span>
                  {q
                    ? `Each PDS stream is read by the member that owns its host shard, verified there and forwarded to the leader, which numbers it and emits it once ${Math.floor(q.members.length / 2) + 1} of ${q.members.length} members hold it. Every serving node sends the same stream.`
                    : 'Each PDS stream is read, verified and numbered here, then sent to every consumer in the same order.'}{' '}
                  The trunks are the {o.topHosts.length} busiest hosts and the rest of the traffic.
                </span>
              </div>
            </div>
          </Panel>
          <div className="cx-grid2 ov2">
            <Panel title="Rejects by reason" src={<Src>overview.rejectsByReason</Src>} right={<span className="muted sm">per second</span>}>
              {reasons.length ? (
                <Bars
                  color="err"
                  rows={reasons.map(([k, v]) => ({
                    key: k,
                    label: reasonLabel(k),
                    v,
                    fmt: (
                      <>
                        {fmtSi(v)}/s <span className="muted">›</span>
                      </>
                    ),
                    title: `${REASON_WHAT[k]}. Opens the hosts sending it.`,
                    onClick: () => navigate(`${REJECTS_TO}&reason=${k}`),
                  }))}
                />
              ) : (
                <Empty>No rejects right now.</Empty>
              )}
            </Panel>
            <Panel
              title="Busiest hosts"
              to={busy === 'rejects' ? REJECTS_TO : '/admin/hosts'}
              src={<Src>{busy === 'rejects' ? 'hosts?sort=errors&limit=8 · topReason' : 'overview.topHosts'}</Src>}
              right={<Seg<Busy> label="Busiest by" value={busy} options={[{ v: 'events', label: 'events' }, { v: 'rejects', label: 'rejects' }]} onChange={setBusy} />}
            >
              {busy === 'rejects' ? (
                !rej.data ? (
                  <Loaded load={rej}>{() => null}</Loaded>
                ) : rejNames.length ? (
                  <div className="cx-tw">
                    <table className="cx-t compact">
                      <thead>
                        <tr>
                          <th>Host</th>
                          <th className="r" title="Rejected frames over all frames, last minute">
                            Rejected
                          </th>
                          <th>Top reason</th>
                        </tr>
                      </thead>
                      <tbody>
                        {rej.data
                          .filter((h) => h.errorRate > 0)
                          .map((r, i) => {
                            const why = r.topReason
                            return (
                              <tr key={`${r.host}#${i}`} data-open={`host:${r.host}`} onClick={() => openPanel('host', r.host)}>
                                <td className="trunc" style={{ maxWidth: 190 }}>
                                  <HostName host={r.host} short />
                                </td>
                                <td className={`r mono sm${r.errorRate > 0.25 ? ' s-err' : r.errorRate > 0.05 ? ' s-warn' : ''}`}>
                                  <LiveVal>{fmtRatio(r.errorRate)}</LiveVal>
                                </td>
                                <td className="sm t2" title={why ? REASON_WHAT[why] : undefined}>
                                  {why ? reasonLabel(why) : <span className="muted">—</span>}
                                </td>
                              </tr>
                            )
                          })}
                      </tbody>
                    </table>
                  </div>
                ) : (
                  <Empty>No host is sending rejects right now.</Empty>
                )
              ) : (
              <div className="cx-tw">
                <table className="cx-t compact">
                  <thead>
                    <tr>
                      <th>Host</th>
                      <th className="r">Events/s</th>
                      <th>Reader</th>
                    </tr>
                  </thead>
                  <tbody>
                    {o.topHosts.slice(0, 8).map((r, i) => (
                      <tr key={`${r.host}#${i}`} data-open={`host:${r.host}`} onClick={() => openPanel('host', r.host)}>
                        <td className="trunc" style={{ maxWidth: 190 }}>
                          <HostName host={r.host} short />
                        </td>
                        <td className="r">
                          <Spark data={r.history ?? []} size="inline" color={r.status === 'throttled' ? 'warn' : 'accent'} /> <LiveVal className="mono sm">{fmtSi(r.eventsPerSec)}</LiveVal>
                        </td>
                        <td>
                          <NodeTag view={view} id={r.node} />
                        </td>
                      </tr>
                    ))}
                  </tbody>
                </table>
              </div>
              )}
            </Panel>
          </div>
          <Panel title="Firehose"right={<span className="muted sm">every node sends the same seqs</span>}>
            <LiveTail height={300} />
          </Panel>
        </div>
        <div className="cx-stack cx-rail">
          <Panel title="Stream" src={<Src>{q ? 'cluster/quorum' : 'overview'}</Src>}>
            <div className="cx-seqbox">
              <div className="cx-eyebrow">newest seq</div>
              <div className="cx-seqv">
                <LiveVal>{seqS(seqNow)}</LiveVal>
              </div>
              <div className="cx-seqd">{q ? `epoch ${q.epoch} · ${q.health === 'down' ? 'no leader' : `${q.leader} leads`}` : `${fmtSi(stream)} events/s`}</div>
            </div>
            {q && (
              <div className="cx-minis">
                <div className="cx-mini">
                  <div className="ml">
                    <span>F (flushed)</span>
                    <b>
                      <LiveVal>{fmtNum(Math.max(0, q.commit - q.flushed))}</LiveVal>
                    </b>
                  </div>
                  <div className="cx-seqd">{seqS(q.flushed)}</div>
                </div>
                <div className="cx-mini">
                  <div className="ml">
                    <span>R headroom</span>
                    <b>
                      <LiveVal>{fmtNum(Math.max(0, q.reserve - q.commit))}</LiveVal>
                    </b>
                  </div>
                  <div className="cx-seqd">{seqS(q.reserve)}</div>
                </div>
              </div>
            )}
          </Panel>
          <Panel title={q ? 'Members' : 'Nodes'} to="/admin/quorum" src={<Src>{q ? 'cluster/quorum' : 'cluster'}</Src>}>
            {(q ? cores : (view?.nodes ?? [])).map((n) => {
              const lead = q?.leader === n.id
              const lag = q && n.status && q.lead ? Math.max(0, q.lead.last - n.status.last) : undefined
              return (
                <RRow key={n.id} to="/admin/quorum" x={n.stale ? 'no answer' : lag !== undefined ? `lag ${fmtNum(lag)}` : `${fmtSi(view?.single ? o.eventsInPerSec : n.eventsInPerSec)}/s in`}>
                  {n.stale ? <Glyph k="err" /> : lead ? <span className="cx-g s-sig">★</span> : <Glyph k="ok" />}
                  <Swatch color={n.color} />
                  <span className="nm mono">{n.id}</span>
                  <span className="muted sm">{n.stale ? '' : n.role}</span>
                </RRow>
              )
            })}
            {!view && <Empty>Loading…</Empty>}
          </Panel>
          <Panel title="Consumers" to="/admin/consumers" src={<Src>consumers</Src>}>
            {(subs.data ?? [])
              .slice()
              .sort((a, b) => b.eventsPerSec * (b.backfilling ? 3 : 1) - a.eventsPerSec * (a.backfilling ? 3 : 1))
              .slice(0, 5)
              .map((c) => (
                <RRow key={`${c.node}/${c.id}`} to="/admin/consumers" x={c.backfilling ? 'backfill' : fmtMs(c.lagMs)}>
                  <Glyph k={isSlow(c, cut) ? 'warn' : c.backfilling ? 'info' : 'ok'} />
                  <span className="nm">
                    <span className="mono">#{c.id}</span> {(c.userAgent || 'no user agent').split('/')[0]}
                  </span>
                </RRow>
              ))}
            {subs.data && !subs.data.length && <Empty>No consumers.</Empty>}
          </Panel>
          <Panel title="Open cases" to="/admin/moderation" src={<Src>cases?status=open</Src>}>
            {(cases.data ?? []).slice(0, 5).map((c) => (
              <RRow key={c.id} onClick={() => openPanel('case', String(c.id))} x={ago(c.openedAtMs)}>
                <Glyph k={c.severity === 'critical' ? 'err' : c.severity === 'high' ? 'warn' : c.severity === 'warn' ? 'warn' : 'info'} />
                <span className="mono sm">{c.id}</span>
                <span className="nm">
                  {c.kind} · {c.host}
                </span>
              </RRow>
            ))}
            {cases.data && !cases.data.length && <Empty>No open cases.</Empty>}
          </Panel>
          <div className="muted sm" style={{ padding: '0 2px' }}>
            {plural(o.hostsConnected, 'host')} connected · {plural(o.consumers, 'consumer')} · as of {ov.at ? ago(ov.at) : '—'}
          </div>
        </div>
      </div>
    </>
  )
}
