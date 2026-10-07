import { useMemo, type ReactNode } from 'react'
import { DataTable, type Col } from '../../components/console/DataTable'
import { confirmAction } from '../../components/console/dialogs'
import { registerDetail } from '../../components/console/Drawer'
import { Banners, Bars, Chip, Copy, Empty, Glyph, KV, LiveVal, Loaded, Meter, Mini, PageHead, Panel, SearchInput, Sec, Seg, Spark, Src, Strip, Tiles, type BannerSpec, type TileSpec } from '../../components/console/kit'
import { openPanel } from '../../components/console/nav'
import { registerPalette } from '../../components/console/Palette'
import type { Consumer, Overview } from '../../lib/api'
import * as A from '../../lib/console/adminAdapter'
import { dt, dur, fmtBytes, fmtMs, fmtNum, fmtSi, plural, seqS } from '../../lib/console/fmt'
import { consumersPoll, isSlow, overviewPoll, policyFullPoll, quorumPoll, seqSeenAt, seriesOf, settingsPoll, slowLagMs } from '../../lib/console/polls'
import { useRelay } from '../../lib/console/relay'
import { Link, navigate, useSearch } from '../../lib/router'
import './logPages.css'
import { RoleChip, memberRows, membershipOn, type MemberRow } from './quorumUi'
import { NodeTag } from './relayUi'

// Every subscribeRepos socket on every member (the answering node asks the others over the peer
// protocol): how each keeps up with the stream, which tier it reads from, and a kick behind a
// typed confirm, sent on to the member serving it.

const keyOf = (c: Consumer) => `${c.node}/${c.id}`
type Tier = Consumer['readTier']
const TIERS: Tier[] = ['ring', 'disk', 'bucket']
const TIER_LABEL: Record<Tier, string> = { ring: 'ring', disk: 'local disk', bucket: 'bucket' }

export function kickDialog(c: Consumer) {
  const flags = settingsPoll.get().data
  const self = flags?.entries.find((e) => e.flag === '--node-id')?.value
  const relayed = !!self && c.node !== self
  const on = membershipOn(flags)
  return confirmAction({
    tone: 'warn',
    title: `Kick consumer #${c.id}?`,
    items: [
      <>
        Drops the socket from <span className="mono">{c.ip}</span> ({c.userAgent || 'no user agent'}) on {c.node} at once.
      </>,
      ...(!relayed
        ? []
        : on
          ? [
              <>
                {self} sends it on to {c.node} over the peer protocol, with the nodes' <span className="mono">--qlog-admin-token</span>.
              </>,
            ]
          : [
              <span className="s-err">
                {c.node} isn't the node answering this console, and the nodes run without <span className="mono">--qlog-admin-token</span>, so {self} can't send the kick on.
              </span>,
            ]),
      'A well-behaved client reconnects with its cursor, on any node, and resumes where it was.',
      'It is not banned: nothing stops it reconnecting.',
    ],
    word: `#${c.id}`,
    action: 'Kick',
    call: A.kickCall(c.id, c.node),
    run: () => A.kickConsumer(c.id, c.node),
    done: `Kicked #${c.id} on ${c.node}`,
  }).then((ok) => {
    if (ok) consumersPoll.refresh()
    return ok
  })
}

/** Entries the leader has committed that a member hasn't emitted yet. */
const behindOf = (rows: MemberRow[], id: string) => {
  const commit = rows.find((r) => r.kind === 'leader')?.s?.commit
  const m = rows.find((r) => r.id === id)
  return commit !== undefined && m?.s ? Math.max(0, commit - m.s.emitted) : undefined
}
const BEHIND_WARN = 1000

/**
 * Is the stream itself fresh: how long since the newest seq moved (to within a poll), the time
 * to firehose, and the serving node furthest behind the commit. The tile that says whether
 * "we're behind" is the relay or the consumer.
 */
function freshnessTile(o: Overview | undefined, rows: MemberRow[], held: boolean): TileSpec {
  const seen = seqSeenAt()
  const age = seen !== undefined ? Date.now() - seen : undefined
  const worst = rows
    .filter((r) => r.s && !r.stale && r.kind !== 'retired')
    .map((r) => ({ id: r.id, n: behindOf(rows, r.id) ?? 0 }))
    .sort((a, b) => b.n - a.n)[0]
  const moving = age !== undefined && age < 5000
  const tone = held || (age !== undefined && age > 30_000) ? 'err' : !moving && age !== undefined ? 'warn' : (o && o.timeToFirehoseP99Ms > 250) || (worst && worst.n > BEHIND_WARN) ? 'warn' : 'ok'
  return {
    label: (
      <span className="t2">
        <Glyph k={tone} /> Stream freshness
      </span>
    ),
    right: 'newest seq age',
    value: held ? 'held' : age === undefined ? '—' : moving ? '< 2 s' : dur(age),
    spark: (
      <div className="cx-tsub">
        TTF p99 {o ? fmtMs(o.timeToFirehoseP99Ms) : '—'}
        {worst ? (worst.n > 0 ? ` · ${worst.id} ${fmtNum(worst.n)} behind` : ' · every node at the commit') : ''}
      </div>
    ),
    title: 'How long since the console saw the newest seq move (it polls every 2 s), the time from a PDS frame to the firehose, and the serving node furthest behind the commit',
    to: '/admin/quorum',
  }
}

/** One line on how this consumer is doing, to paste back to whoever asked. */
function verdict(c: Consumer, cut: number, behind: number | undefined): string {
  const lag = fmtMs(c.lagMs)
  if (c.readTier === 'disk' || c.readTier === 'bucket') return `Replaying from ${c.readTier === 'disk' ? `${c.node}'s local disk` : "the bucket's segments"}, ${lag} behind live${c.cursor > 0 ? ` (cursor ${seqS(c.cursor)})` : ''}.`
  if (c.backfilling) return `Replaying from the firehose's memory, ${lag} behind live.`
  if (behind !== undefined && behind > BEHIND_WARN) return `Its node ${c.node} is ${fmtNum(behind)} entries behind the commit: the lag is the relay's, not the consumer's.`
  if (isSlow(c, cut)) return `Falling behind: ${lag} behind live, cut off at ${fmtMs(cut)}. It reads slower than the stream.`
  return `Live, ${lag} behind, under the ${fmtMs(cut)} cutoff.`
}

/** Where its next events come from: the firehose's memory, the node's own log, or the bucket's segments. */
function TierChip({ c }: { c: Consumer }) {
  if (c.readTier === 'disk') return <Chip k="info">local disk</Chip>
  if (c.readTier === 'bucket') return <Chip k="violet">bucket</Chip>
  return (
    <Chip k="plain" glyph={false}>
      ring{c.backfilling ? ' · replaying' : ''}
    </Chip>
  )
}

export function Consumers() {
  const subs = consumersPoll.use()
  const ov = overviewPoll.use()
  const pol = policyFullPoll.use()
  const qp = quorumPoll.use()
  const { view } = useRelay()
  const s = useSearch()
  const node = s.get('node') ?? ''
  const tier = (s.get('tier') ?? '') as Tier | ''
  const q = (s.get('q') ?? '').toLowerCase()
  const cut = slowLagMs(pol.data)
  const all = subs.data
  const silent = (view?.nodes ?? []).filter((n) => n.stale).map((n) => n.id)
  const o = ov.data
  const stream = o ? (o.streamEventsPerSec ?? o.eventsOutPerSec) : 0
  const setUrl = (k: string, v: string) => {
    const p = new URLSearchParams(location.search)
    if (v) p.set(k, v)
    else p.delete(k)
    const qs = p.toString()
    history.replaceState(null, '', `${location.pathname}${qs ? `?${qs}` : ''}`)
    dispatchEvent(new PopStateEvent('popstate'))
  }
  const rows = useMemo(
    () =>
      (all ?? [])
        .filter((c) => (!node || c.node === node) && (!tier || c.readTier === tier) && (!q || c.ip.includes(q) || c.userAgent.toLowerCase().includes(q) || String(c.id) === q))
        .sort((a, b) => Number(isSlow(b, cut)) - Number(isSlow(a, cut)) || Number(b.backfilling) - Number(a.backfilling) || b.lagMs - a.lagMs || a.node.localeCompare(b.node) || a.id - b.id),
    [all, node, tier, q, cut],
  )
  const nodes = [...new Set((all ?? []).map((c) => c.node))].sort()
  const qv = qp.data?.supported ? qp.data.data : undefined
  const mrows = qv ? memberRows(qv, view) : []
  const leadCommit = mrows.find((r) => r.kind === 'leader')?.s?.commit

  const slow = (all ?? []).filter((c) => isSlow(c, cut))
  const backfill = (all ?? []).filter((c) => c.backfilling).length
  const byTier: Record<Tier, number> = { ring: 0, disk: 0, bucket: 0 }
  for (const c of all ?? []) byTier[c.readTier] = (byTier[c.readTier] ?? 0) + 1
  const liveOnes = (all ?? []).filter((c) => !c.backfilling)
  const slowest = liveOnes.reduce<Consumer | undefined>((a, c) => (!a || c.lagMs > a.lagMs ? c : a), undefined)
  const frameBytes = o && o.eventsOutPerSec > 0 ? o.bytesOutPerSec / o.eventsOutPerSec : 0
  const banners: BannerSpec[] = []
  if (slow.length) {
    const w = [...slow].sort((a, b) => b.lagMs - a.lagMs)[0]
    banners.push({
      id: 'slow',
      tone: 'warn',
      title: slow.length === 1 ? 'A consumer is falling behind' : `${slow.length} consumers are falling behind`,
      desc: (
        <>
          <span className="mono">#{w.id}</span> on {w.node} · {w.userAgent || 'no user agent'}
        </>
      ),
      right: `${fmtMs(w.lagMs)} behind`,
      body: (
        <>
          <p>It reads slower than the stream. At the slow-consumer cutoff ({fmtMs(cut)}) it's disconnected and can resume with its cursor.</p>
          <div className="cx-form-row">
            <button type="button" className="cx-btn sm" onClick={() => openPanel('consumer', keyOf(w))}>
              Open #{w.id}
            </button>
          </div>
        </>
      ),
    })
  }
  if (silent.length)
    banners.push({
      id: 'silent',
      tone: 'warn',
      title: `${silent.join(', ')} didn't answer`,
      desc: `${silent.length === 1 ? 'its' : 'their'} consumers aren't listed until ${silent.length === 1 ? 'it answers' : 'they answer'}`,
    })

  const tiles: TileSpec[] = [
    freshnessTile(o, mrows, view?.quorum?.health === 'down'),
    { label: 'Consumers', right: 'every node', value: all ? fmtNum(all.length) : '—', sec: `${fmtNum(backfill)} replaying` },
    { label: 'Events sent', right: 'every node', value: o ? fmtSi(o.eventsOutPerSec) : '—', unit: '/s', spark: <Spark data={o?.history.eventsOut ?? []} color="c5" /> },
    {
      label: 'Egress',
      right: frameBytes ? `≈ ${fmtBytes(stream * frameBytes)}/s per consumer` : 'every node',
      value: o ? `${fmtBytes(o.bytesOutPerSec)}/s` : '—',
      spark: <Spark data={o?.history.bytesOut ?? []} color="c5" />,
      title: 'Per full-stream consumer: the merged stream’s rate times the mean frame size sent',
    },
    { label: 'Slowest live', value: slowest ? fmtMs(slowest.lagMs) : '—', sec: `cut off at ${fmtMs(cut)}`, title: 'The live consumer furthest behind the newest seq' },
  ]

  const cols: Col<Consumer>[] = [
    {
      id: 'id',
      label: '#',
      sort: (a, b) => a.id - b.id,
      render: (c) => (
        <span className="mono">
          <Glyph k={isSlow(c, cut) ? 'warn' : c.backfilling ? 'info' : 'ok'} /> {c.id}
        </span>
      ),
    },
    { id: 'node', label: 'Node', sort: (a, b) => a.node.localeCompare(b.node), render: (c) => <NodeTag view={view} id={c.node} /> },
    { id: 'ua', label: 'Client', render: (c) => <span className="mono sm trunc" style={{ maxWidth: 220, display: 'inline-block', verticalAlign: 'bottom' }} title={c.userAgent}>{c.userAgent || '—'}</span> },
    { id: 'ip', label: 'IP', render: (c) => <span className="mono sm t2">{c.ip}</span> },
    { id: 'since', label: 'Connected', r: true, sort: (a, b) => b.connectedSinceMs - a.connectedSinceMs, render: (c) => <span className="sm muted" title={dt(c.connectedSinceMs)}>{dur(Date.now() - c.connectedSinceMs)}</span> },
    { id: 'lag', label: 'Lag', r: true, sort: (a, b) => a.lagMs - b.lagMs, render: (c) => <LiveVal className={`mono sm${isSlow(c, cut) ? ' s-warn' : ''}`}>{fmtMs(c.lagMs)}</LiveVal> },
    { id: 'tier', label: 'Reads from', sort: (a, b) => TIERS.indexOf(a.readTier) - TIERS.indexOf(b.readTier), render: (c) => <TierChip c={c} /> },
    {
      id: 'rate',
      label: 'Events/s vs stream',
      r: true,
      sort: (a, b) => a.eventsPerSec - b.eventsPerSec,
      title: 'Its rate (solid) against the merged stream (dashed)',
      render: (c) => (
        <span className="cx-gauge">
          <Spark data={seriesOf(`cons:${keyOf(c)}`)} l2={seriesOf('stream').slice(-seriesOf(`cons:${keyOf(c)}`).length)} size="inline" color={isSlow(c, cut) ? 'warn' : 'c5'} />
          <LiveVal className="mono sm">{fmtSi(c.eventsPerSec)}</LiveVal>
        </span>
      ),
    },
    { id: 'bytes', label: 'Bytes/s', r: true, sort: (a, b) => a.bytesPerSec - b.bytesPerSec, render: (c) => <span className="mono sm">{fmtBytes(c.bytesPerSec)}/s</span> },
  ]

  const serving = view?.nodes ?? []
  const P = (pol.data?.policy as { consumers?: Record<string, number> } | undefined)?.consumers

  return (
    <>
      <PageHead
        title="Consumers"
        sub={
          <>
            <span>{all ? `${plural(all.length, 'subscribeRepos socket')}${nodes.length > 1 ? ` on ${nodes.length} nodes` : ''}` : '…'}</span>
            {o && <span>{fmtBytes(o.bytesOutPerSec)}/s out</span>}
          </>
        }
        actions={
          nodes.length > 1 ? (
            <Seg label="Node" value={node} options={[{ v: '', label: 'all nodes', n: all ? fmtNum(all.length) : undefined }, ...nodes.map((n) => ({ v: n, label: n, n: fmtNum((all ?? []).filter((c) => c.node === n).length) }))]} onChange={(v) => setUrl('node', v)} />
          ) : undefined
        }
      />
      <Banners items={banners} />
      <div className="cx-tilesbox">
        <Tiles tiles={tiles} />
      </div>
      <Panel title="Serving nodes" src={<Src>cluster · cluster/quorum</Src>} right={<span className="muted sm">every node sends the same stream</span>}>
        <div className="cx-tw">
          <table className="cx-t compact">
            <thead>
              <tr>
                <th>Node</th>
                <th>Role</th>
                <th className="r">Consumers</th>
                <th className="r">Sent/s</th>
                <th className="r">Behind the commit</th>
              </tr>
            </thead>
            <tbody>
              {serving.map((n) => {
                const m = mrows.find((r) => r.id === n.id)
                const behind = leadCommit !== undefined && m?.s ? Math.max(0, leadCommit - m.s.emitted) : undefined
                return (
                  <tr key={n.id} data-open={`node:${n.id}`} onClick={() => openPanel('node', n.id)} className={n.stale ? 'dim' : undefined}>
                    <td>
                      <NodeTag view={view} id={n.id} />
                    </td>
                    <td>{m ? <RoleChip kind={m.kind} /> : <span className="sm">{n.role}</span>}</td>
                    <td className="r mono sm">{n.stale ? '—' : fmtNum(all ? all.filter((c) => c.node === n.id).length : n.consumers)}</td>
                    <td className="r mono sm">{n.stale ? '—' : fmtSi(view?.single ? (o?.eventsOutPerSec ?? n.eventsOutPerSec) : n.eventsOutPerSec)}</td>
                    <td className="r mono sm" title="Entries the leader has committed that this node hasn't emitted yet">
                      {behind === undefined ? '—' : `${fmtNum(behind)} entries`}
                    </td>
                  </tr>
                )
              })}
              {!serving.length && (
                <tr>
                  <td colSpan={5}>
                    <Empty>Loading…</Empty>
                  </td>
                </tr>
              )}
            </tbody>
          </table>
        </div>
      </Panel>
      <Panel
        title="Connections"
        className="cx-mt"
        src={<Src>consumers</Src>}
        right={
          <>
            <Seg<Tier | ''> label="Reads from" value={tier} options={[{ v: '', label: 'any tier' }, ...TIERS.map((t) => ({ v: t, label: TIER_LABEL[t], n: fmtNum(byTier[t]) }))]} onChange={(v) => setUrl('tier', v)} />
            <SearchInput mono value={q} placeholder="IP, client or #id" onChange={(v) => setUrl('q', v.trim().toLowerCase())} style={{ width: 200 }} />
          </>
        }
      >
        <Loaded load={subs}>
          {() => (
            <DataTable
              rows={rows}
              cols={cols}
              rowKey={keyOf}
              open={(c) => ({ type: 'consumer', id: keyOf(c) })}
              compact
              label="Consumers"
              empty={<Empty title={all?.length ? 'No consumer matches' : 'No consumers'}>{all?.length ? 'Clear the filters to see them all.' : 'Nobody is subscribed to any member’s firehose right now.'}</Empty>}
            />
          )}
        </Loaded>
      </Panel>
      <div className="cx-grid2 cx-mt">
        <Panel title="Consumer limits" to="/admin/policy" src={<Src>policy/full · consumers</Src>}>
          {P ? (
            <KV
              style={{ padding: '10px 12px', margin: 0 }}
              rows={[
                ['Connections per IP', <span className="mono">{fmtNum(P.connectionsPerIp)}</span>],
                ['Consumers per node', <span className="mono">{fmtNum(P.consumersPerNode)}</span>],
                ['Slow consumer cutoff', <span className="mono">{fmtMs(cut)}</span>],
                ['Max backfill', <span className="mono">{P.maxBackfillSecs ? dur(P.maxBackfillSecs * 1000) : '—'}</span>],
              ]}
            />
          ) : (
            <Loaded load={pol}>{() => <Empty>The policy has no consumer limits.</Empty>}</Loaded>
          )}
        </Panel>
        <Panel
          title="Where consumers read from"
          src={<Src>consumers · readTier</Src>}
          foot={
            <span>
              Every live consumer reads the firehose's memory (the ring). A replay older than it reads the node's own log on disk, and older still the bucket's segments (the{' '}
              <span className="mono">backfill</span> purpose on <Link to="/admin/store">Object store</Link>).
            </span>
          }
        >
          <Loaded load={subs}>
            {(cs) =>
              cs.length ? (
                <div className="cx-pn-b">
                  <Bars
                    rows={TIERS.map((t) => ({
                      key: t,
                      label: TIER_LABEL[t],
                      v: byTier[t],
                      fmt: fmtNum(byTier[t]),
                      color: t === 'ring' ? 'c5' : t === 'disk' ? 'info' : 'violet',
                      title: `Show only the consumers reading from ${TIER_LABEL[t]}`,
                      onClick: () => setUrl('tier', tier === t ? '' : t),
                    }))}
                  />
                </div>
              ) : (
                <Empty>No consumers.</Empty>
              )
            }
          </Loaded>
        </Panel>
      </div>
    </>
  )
}

// ---------------------------------------------------------------- one consumer

registerDetail('consumer', {
  kind: 'Consumer',
  section: 'consumers',
  use: (id, mode) => {
    const subs = consumersPoll.use()
    const ov = overviewPoll.use()
    const pol = policyFullPoll.use()
    const qp = quorumPoll.use()
    const { view } = useRelay()
    const c = subs.data?.find((x) => keyOf(x) === id)
    if (!c) return { title: `#${id.split('/').pop()}`, body: null, loading: subs.loading, missing: subs.loading ? undefined : 'It disconnected: consumer ids are per connection.' }
    const cut = slowLagMs(pol.data)
    const o = ov.data
    const stream = o ? (o.streamEventsPerSec ?? o.eventsOutPerSec) : 0
    const share = stream > 0 ? c.eventsPerSec / stream : 0
    const mine = seriesOf(`cons:${id}`)
    const behind = qp.data?.supported ? behindOf(memberRows(qp.data.data, view), c.node) : undefined
    const line = verdict(c, cut, behind)
    const main = (
      <>
        <div className={`cx-verdict ${isSlow(c, cut) || (behind ?? 0) > BEHIND_WARN ? 'warn' : c.backfilling ? 'info' : 'ok'}`}>
          <Copy text={line} mono={false} />
        </div>
        <Strip
          items={[
            ['events/s', fmtSi(c.eventsPerSec)],
            ['bytes/s', fmtBytes(c.bytesPerSec)],
            ['behind live', fmtMs(c.lagMs)],
            ['of the stream', stream ? `${Math.round(share * 100)}%` : '—'],
          ]}
        />
        <Mini label="Events/s against the stream (dashed)" value={fmtSi(c.eventsPerSec)}>
          <Spark data={mine} l2={seriesOf('stream').slice(-mine.length)} size="big" color={isSlow(c, cut) ? 'warn' : 'c5'} />
        </Mini>
        {stream > 0 && (
          <div className="cx-form-row sm muted">
            <Meter v={c.eventsPerSec} max={stream} k={isSlow(c, cut) ? 'warn' : 'ok'} wide /> {fmtSi(c.eventsPerSec)} of {fmtSi(stream)}/s
          </div>
        )}
      </>
    )
    const side = (
      <>
        <Sec title="Connection" open>
          <KV
            rows={[
              ['Node', <NodeTag view={view} id={c.node} />],
              ['IP', <Copy text={c.ip} />],
              ['Client', <span className="mono">{c.userAgent || '—'}</span>],
              ['Connected', `${dt(c.connectedSinceMs)} (${dur(Date.now() - c.connectedSinceMs)})`],
              ['Cursor', <span className="mono">{c.cursor > 0 ? seqS(c.cursor) : '—'}</span>],
              ['Reads from', <TierChip c={c} />],
            ]}
          />
        </Sec>
        <Sec title="Kick" open flush danger>
          <div className="cx-acts">
            <div className="cx-act">
              <div className="ad">
                <b>Disconnect</b>
                Drops the socket at once. It can resume with its cursor on any node.
              </div>
              <button
                type="button"
                className="cx-btn sm danger"
                onClick={() => kickDialog(c)}
              >
                Kick…
              </button>
            </div>
          </div>
        </Sec>
      </>
    )
    const body: ReactNode =
      mode === 'page' ? (
        <div className="cols">
          <div>{main}</div>
          <div>{side}</div>
        </div>
      ) : (
        <>
          {main}
          {side}
        </>
      )
    return {
      title: `#${c.id} · ${c.userAgent || 'no user agent'}`,
      chip: isSlow(c, cut) ? <Chip k="warn">slow</Chip> : c.backfilling ? <Chip k="info">replaying</Chip> : <Chip k="ok">live</Chip>,
      foot: <>ids are per node · GET /admin/api/consumers</>,
      body,
    }
  },
})

registerPalette({
  items: (q) => {
    if (!q) return []
    return (consumersPoll.get().data ?? []).slice(0, 400).map((c) => ({
      group: 'Consumers',
      glyph: '◆',
      title: `#${c.id} ${c.userAgent || 'no user agent'}`,
      desc: `${c.node} · ${c.ip} · ${fmtMs(c.lagMs)} behind`,
      hay: `${c.ip} ${c.node}`,
      run: () => {
        if (location.pathname !== '/admin/consumers') navigate('/admin/consumers')
        openPanel('consumer', keyOf(c))
      },
    }))
  },
})

