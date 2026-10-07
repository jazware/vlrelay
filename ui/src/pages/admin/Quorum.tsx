import { useState, type ReactNode } from 'react'
import { DataTable, type Col } from '../../components/console/DataTable'
import { Banners, Chip, Empty, HostName, KV, LiveVal, Loaded, NeedsVersion, PageHead, Panel, RRow, Spark, Src, Swatch, Tiles, type BannerSpec, type TileSpec } from '../../components/console/kit'
import { LogRail, type RailData } from '../../components/console/LogRail'
import { openPanel } from '../../components/console/nav'
import { confirmAction } from '../../components/console/dialogs'
import { errText, type ClusterView, type HostRow, type QStatus, type QuorumView } from '../../lib/api'
import * as A from '../../lib/console/adminAdapter'
import { ago, clock, dt, dur, fmtBytes, fmtMs, fmtNum, fmtSi, fmtUs, seqS, since } from '../../lib/console/fmt'
import { useLivePoll } from '../../lib/console/live'
import { clusterPoll, flushSeenAt, historyPoll, overviewPoll, quorumPoll, seenEpochs, seriesOf, settingsPoll } from '../../lib/console/polls'
import { useRelay, type RelayView } from '../../lib/console/relay'
import './logPages.css'
import './quorumDetail'
import { currentLead, Durability, EPOCH_GLYPH, EpochChip, epochDetail, epochEvents, memberRows, membersDialog, membershipOn, refStatus, RoleChip, type EpochEvent, type MemberRow } from './quorumUi'
import { Lg, NodeTag, relayBanners } from './relayUi'

// The quorum log and the cluster around it: the log rail (each member's track around F and the
// commit), the leadership history, the members, the flushes, which member reads each host, and
// the counters. On a relay without the quorum log it falls back to the cluster's nodes.

const setting = (s: ReturnType<typeof settingsPoll.use>['data'], flag: string) => s?.entries.find((e) => e.flag === flag)

export function Quorum() {
  const qp = quorumPoll.use()
  const cp = clusterPoll.use()
  const { view } = useRelay()
  const q = qp.data
  if (q && !q.supported) return <NoQuorum c={cp.data} view={view} />
  if (!q) return <Loaded load={qp}>{() => null}</Loaded>
  return <QuorumLog qv={q.data} at={qp.at} c={cp.data} view={view} />
}

// Its own component so its hooks (the history poll among them) run only once the log answered:
// called after Quorum's early returns, they'd throw on a cold load.
function QuorumLog({ qv, at, c, view }: { qv: QuorumView; at?: number; c?: ClusterView; view?: RelayView }) {
  const sp = settingsPoll.use()
  const ov = overviewPoll.use()
  const hist = historyPoll.use()
  const rows = memberRows(qv, view)
  const ref = refStatus(qv)
  const lead = rows.find((r) => r.kind === 'leader')?.s ?? undefined
  const info = view?.quorum
  const members = ref?.members ?? []
  const majority = Math.floor(members.length / 2) + 1
  const answering = rows.filter((r) => !r.stale && members.includes(r.id)).length
  const on = membershipOn(sp.data)
  const flushMs = Number(setting(sp.data, '--qlog-flush-ms')?.value ?? '') || undefined
  const stream = ov.data ? (ov.data.streamEventsPerSec ?? ov.data.eventsOutPerSec) : 0
  const events = epochEvents(qv, hist.data?.events ?? [], seenEpochs())
  const known = [...new Set([...rows.map((r) => r.id), ...(view?.nodes.map((n) => n.id) ?? [])])]
  const change = () => membersDialog({ current: members, leader: lead?.id ?? null, known, on })

  const banners: BannerSpec[] = relayBanners({ view, slowCutMs: 0, scope: 'hosts' })
  const epochsSeen = new Set(rows.filter((r) => r.s).map((r) => r.s!.epoch))
  if (epochsSeen.size > 1) banners.push({ id: 'epochs', tone: 'warn', title: `Members report different epochs (${[...epochsSeen].sort().join(', ')})`, desc: 'a takeover or membership change is in flight' })
  if (lead?.paused) banners.push({ id: 'paused', tone: 'warn', title: `${lead.id} has paused appends for a membership change`, desc: 'they resume once the CAS lands' })
  const learners = rows.filter((r) => r.kind === 'learner')
  if (learners.length)
    banners.push({ id: 'learners', tone: 'info', title: `${learners.map((r) => r.id).join(', ')} catching up as ${learners.length === 1 ? 'a learner' : 'learners'}`, desc: lead ? `${fmtNum(Math.max(0, lead.commit - Math.min(...learners.map((r) => r.s?.last ?? 0))))} entries to go` : undefined })
  const gaps = rows.filter((r) => (r.s?.emit_gaps ?? 0) > 0 || (r.s?.resets ?? 0) > 0)
  if (gaps.length)
    banners.push({ id: 'gaps', tone: 'err', title: `Emit gaps or resets on ${gaps.map((r) => r.id).join(', ')}`, desc: 'these stay at 0 in a healthy log; see Counters' })

  const commitP99 = seriesOf('commit-p99')
  const fSeen = A.flushedAt(lead, flushSeenAt())
  const tiles: TileSpec[] = [
    { label: 'Epoch', value: ref ? String(ref.epoch) : '—', sec: ref ? `promised ${ref.promised}` : undefined },
    leaderTile(lead, currentLead(events)),
    { label: 'Members answering', value: `${answering}`, unit: `of ${members.length}`, sec: `majority is ${majority}` },
    { label: 'Commit index', value: seqS(lead?.commit ?? ref?.commit), spark: <Spark data={seriesOf('stream')} color="signal" /> },
    {
      label: 'F · flushed',
      right: fSeen ? `moved ${ago(fSeen)}` : undefined,
      value: seqS(ref?.flushed),
      sec: lead ? `${fmtNum(Math.max(0, lead.commit - lead.flushed))} above` : undefined,
      title: 'The log may leave local disk up to F: everything at or below it is in the bucket',
    },
    { label: 'R · reserve', value: seqS(ref?.reserve), sec: lead ? `${fmtNum(Math.max(0, lead.reserve - lead.commit))} headroom` : undefined, title: 'The commit index may rise to R before the leader must flush' },
    {
      label: 'Commit latency',
      right: 'append → majority ack',
      value: lead ? fmtUs(lead.commit_us.p50) : '—',
      sec: lead ? `${fmtUs(lead.commit_us.p99)} p99` : undefined,
      spark: <Spark data={commitP99} l2={seriesOf('commit-p50')} color="signal" />,
    },
    { label: 'Recoveries', value: fmtNum(ref?.recoveries ?? 0), sec: `generation ${ref?.generation ?? 0} · lost quorums ${fmtNum(Math.max(0, ...rows.map((r) => r.s?.lost_quorums ?? 0)))}` },
  ]

  const rail: RailData = {
    rows: rows
      .filter((r) => r.kind !== 'retired' && r.kind !== 'unknown')
      .map((r) => ({ id: r.id, color: r.color, dead: r.stale, learner: r.kind === 'learner', leader: r.kind === 'leader', last: r.s?.last ?? 0, commit: r.s?.commit ?? 0, emitted: r.s?.emitted ?? 0 })),
    flushed: ref?.flushed ?? 0,
    reserve: ref?.reserve ?? 0,
    commit: lead?.commit ?? ref?.commit ?? 0,
    held: info?.health === 'down',
    rate: stream,
    at: at ?? Date.now(),
  }

  return (
    <>
      <PageHead
        title="Quorum & cluster"
        sub={
          ref ? (
            <>
              <span>epoch {ref.epoch}</span>
              <span>{lead ? `${lead.id} leads` : `no leader: ${answering} of ${members.length} answering`}</span>
              <span>members since epoch {ref.members_since}</span>
              <span>generation {ref.generation}</span>
            </>
          ) : (
            <span>no member answered</span>
          )
        }
        actions={
          <>
            <button type="button" className="cx-btn" onClick={change} title={on ? undefined : 'The nodes run without --qlog-admin-token'}>
              Change membership…
            </button>
          </>
        }
      />
      <Banners items={banners} />
      <div className="cx-tilesbox t4">
        <Tiles tiles={tiles} />
      </div>
      <Panel
        title="The log"
        src={<Src>cluster/quorum · status.{'{last,commit,emitted,flushed,reserve}'}</Src>}
        right={
          <span className="cx-legend">
            <Lg color="accent">flushed to the bucket (≤ F)</Lg>
            <Lg color="signal">committed on {majority} of {members.length || '?'}, not yet flushed</Lg>
            <span>
              <i style={{ border: '1px solid var(--ink2)', height: 8, background: 'none' }} />
              appended, waiting for acks
            </span>
            <span>▲ emitted</span>
          </span>
        }
      >
        <div className="cx-cvwrap">
          <LogRail data={rail} />
          <div className="cx-cvnote">
            <span>
              Nothing reaches a consumer before {majority} of {members.length || '?'} members hold it.
              {flushMs ? ` Every ${dur(flushMs)}` : ' On each flush'} the leader writes segments, state and host cursors to the bucket and CASes the manifest: F moves up, and R is how far the commit index
              may run past it.
            </span>
          </div>
        </div>
      </Panel>
      <Leadership events={events} stale={hist.data?.stale ?? []} error={hist.error && !hist.data ? hist.error : undefined} view={view} className="cx-mt" />
      <Panel title="Members" className="cx-mt" src={<Src>cluster/quorum</Src>} right={<span className="muted sm">lag is entries behind the leader's last append</span>}>
        <Members rows={rows} lead={lead} />
      </Panel>
      <div className="cx-grid2 cx-mt">
        <div className="cx-stack">
          <FlushPanel lead={lead} flushMs={flushMs} on={on} />
          <ShardPanel c={c} view={view} />
        </div>
        <div className="cx-stack">
          <Counters rows={rows} />
          <Pipeline view={view} />
        </div>
      </div>
    </>
  )
}

/** Who leads, since when, and the change that put it there (its epoch's drawer). */
function leaderTile(lead: QStatus | undefined, cur: EpochEvent | undefined): TileSpec {
  if (!lead) return { label: 'Leader', value: '—', sec: 'none: nothing commits' }
  const mine = cur && cur.leader === lead.id ? cur : undefined
  if (!mine) return { label: 'Leader', value: lead.id, sec: `epoch ${lead.epoch}`, title: "No member's history dates this leader's term" }
  const from = mine.lead?.from ?? mine.down?.node
  const how =
    mine.kind === 'takeover' || mine.kind === 'handoff'
      ? `${mine.kind}${from && from !== lead.id ? ` from ${from}` : ''}`
      : mine.kind === 'switch'
        ? 'membership change'
        : mine.kind === 'recovery'
          ? 'bucket recovery'
          : 'new epoch'
  return {
    label: 'Leader',
    value: lead.id,
    unit: `since ${since(mine.atMs!)}`,
    spark: (
      <div className="cx-tsub">
        <button type="button" className="cx-linklike" onClick={() => openPanel('epoch', mine.id)} title={`epoch ${mine.epoch}, ${dt(mine.atMs!)}`}>
          {how} ›
        </button>
      </div>
    ),
  }
}

// ---------------------------------------------------------------- members

function Members({ rows, lead }: { rows: MemberRow[]; lead?: QStatus }) {
  const head = lead?.last ?? Math.max(0, ...rows.map((r) => r.s?.last ?? 0))
  const cols: Col<MemberRow>[] = [
    {
      id: 'node',
      label: 'Node',
      sort: (a, b) => a.id.localeCompare(b.id),
      render: (r) => (
        <span className="cx-cellid">
          <Swatch color={r.color} />
          <span className="mono">{r.id}</span>
        </span>
      ),
    },
    { id: 'role', label: 'Role', render: (r) => <RoleChip kind={r.kind} /> },
    { id: 'addr', label: 'Address', render: (r) => <span className="mono sm t2">{r.addr || '—'}</span> },
    { id: 'contact', label: 'Contact', render: (r) => <span className="sm muted" title={r.error ?? undefined}>{r.stale ? (r.error ?? 'no answer') : ago(r.reportedMs)}</span> },
    { id: 'last', label: 'Last (acked)', r: true, render: (r) => <LiveVal className="mono sm">{r.s ? seqS(r.s.last) : '—'}</LiveVal> },
    { id: 'commit', label: 'Commit', r: true, render: (r) => <LiveVal className="mono sm">{r.s ? seqS(r.s.commit) : '—'}</LiveVal> },
    { id: 'emitted', label: 'Emitted', r: true, render: (r) => <LiveVal className="mono sm">{r.s ? seqS(r.s.emitted) : '—'}</LiveVal> },
    {
      id: 'lag',
      label: 'Lag',
      r: true,
      sort: (a, b) => (a.s ? head - a.s.last : -1) - (b.s ? head - b.s.last : -1),
      render: (r) => {
        if (!r.s || r.kind === 'retired') return <span className="muted">—</span>
        const lag = Math.max(0, head - r.s.last)
        return <span className={`mono sm${lag > 10_000 ? ' s-err' : lag > 200 ? ' s-warn' : ''}`}>{fmtNum(lag)}</span>
      },
    },
    {
      id: 'log',
      label: 'Log',
      render: (r) =>
        r.s ? (
          <>
            {r.s.intact ? <span className="cx-chip ok"><span className="cx-g">●</span>intact</span> : <span className="cx-chip warn"><span className="cx-g">▲</span>not intact</span>}{' '}
            <span className="muted sm">{fmtBytes(r.s.log_bytes)}</span>
          </>
        ) : (
          <span className="muted">—</span>
        ),
    },
    { id: 'fsync', label: 'fsync p99', r: true, render: (r) => <span className="mono sm">{r.s?.disk ? fmtUs(r.s.disk.fsync_us.p99) : '—'}</span> },
    { id: 'durability', label: 'Durability', title: 'When an entry counts on this member (--durability)', render: (r) => <Durability d={r.s?.durability} /> },
  ]
  return <DataTable rows={rows} cols={cols} rowKey={(r) => r.id} open={(r) => ({ type: 'node', id: r.id })} dim={(r) => r.stale || r.kind === 'retired'} compact label="Members" empty={<Empty>No member answered.</Empty>} />
}

// ---------------------------------------------------------------- leadership

export function Leadership({ events, stale, error, view, className, limit }: { events: EpochEvent[]; stale: string[]; error?: unknown; view?: RelayView; className?: string; limit?: number }) {
  const timed = events.filter((e) => e.atMs !== undefined).sort((a, b) => a.atMs! - b.atMs!)
  const now = Date.now()
  const first = timed[0]?.atMs
  const t0 = first !== undefined ? first - Math.max(60_000, (now - first) * 0.06) : now - 3_600_000
  const X = (t: number) => ((Math.max(t0, t) - t0) / (now - t0)) * 100
  const cols: Col<EpochEvent>[] = [
    { id: 'epoch', label: 'Epoch', render: (e) => <span className="mono">{e.fromEpoch !== undefined ? `${e.fromEpoch} → ${e.epoch}` : `→ ${e.epoch}`}</span> },
    { id: 'what', label: 'What', render: (e) => <EpochChip kind={e.kind} /> },
    { id: 'leader', label: 'Leader after', render: (e) => (e.leader ? <NodeTag view={view} id={e.leader} /> : <span className="muted">—</span>) },
    {
      id: 'pause',
      label: 'Pause',
      r: true,
      title: 'Appends paused (a membership change), the recovery took, or no member led (from the old leader stepping down to the new one leading)',
      render: (e) => <span className="mono sm">{e.pausedMs !== undefined ? fmtMs(e.pausedMs) : '—'}</span>,
    },
    { id: 'why', label: 'Detail', render: (e) => <span className="sm t2">{epochDetail(e)}</span> },
    { id: 'when', label: 'When', r: true, render: (e) => <span className="sm muted" title={e.atMs ? dt(e.atMs) : undefined}>{e.atMs ? ago(e.atMs) : '—'}</span> },
  ]
  return (
    <Panel
      title="Leadership"
      className={className}
      src={
        <>
          <Src>cluster/quorum/history</Src> <Src>cluster/quorum · status.switches, status.recovered</Src>
        </>
      }
      right={
        <span className="cx-legend">
          <span>{EPOCH_GLYPH.takeover} takeover</span>
          <span>{EPOCH_GLYPH.handoff} handoff</span>
          <span>{EPOCH_GLYPH.switch} membership</span>
          <span>{EPOCH_GLYPH.recovery} recovery</span>
          <span>{EPOCH_GLYPH.stepdown} step-down</span>
        </span>
      }
      foot={
        <span>
          {error ? <span className="s-err">The history didn't load ({errText(error)}); only membership changes and recoveries show. </span> : null}
          {stale.length ? <span className="s-warn">{stale.join(', ')} didn't answer, so {stale.length === 1 ? 'its' : 'their'} changes are missing. </span> : null}
          Each member keeps its last 64 leadership changes; membership changes and recoveries add their timings from the status of the leader that ran them.
        </span>
      }
    >
      {timed.length > 0 && (
        <div className="cx-ribbon" aria-hidden="true">
          {[{ atMs: t0, leader: null as string | null, id: 'start' }, ...timed].map((e, i, all) => {
            const end = i + 1 < all.length ? all[i + 1].atMs! : now
            const c = e.leader ? view?.byId.get(e.leader)?.color : undefined
            return <div key={e.id} className="band" style={{ left: `${X(e.atMs!)}%`, width: `${Math.max(0.3, X(end) - X(e.atMs!))}%`, background: c ?? 'var(--idle)', opacity: c ? 0.85 : 0.3 }} title={e.leader ?? 'before'} />
          })}
          {timed.map((e) => (
            <button key={e.id} type="button" className="mk" style={{ left: `${X(e.atMs!)}%` }} title={`epoch ${e.epoch} · ${e.kind}`} tabIndex={-1} onClick={() => openPanel('epoch', e.id)}>
              {EPOCH_GLYPH[e.kind]}
              <span className="st" />
            </button>
          ))}
          <div className="ax">
            <span>{dur(now - t0)} ago</span>
            <span>now</span>
          </div>
        </div>
      )}
      <DataTable
        rows={limit ? events.slice(0, limit) : events}
        cols={cols}
        rowKey={(e) => e.id}
        open={(e) => ({ type: 'epoch', id: e.id })}
        compact
        label="Epoch changes"
        empty={<Empty title="No leadership changes recorded">No member lists a takeover, handoff, membership change or recovery.</Empty>}
      />
    </Panel>
  )
}

// ---------------------------------------------------------------- flush

function flushNowDialog(lead: QStatus, on: boolean, flushMs?: number) {
  const f = lead.flush
  return confirmAction({
    tone: 'warn',
    primary: true,
    title: 'Flush the log now?',
    items: [
      <>
        {lead.id} seals what it has committed and writes the segments, state and host cursors to the bucket, then CASes the manifest: F moves up to the commit index it has when the request lands
        (now {seqS(lead.commit)}, {fmtNum(Math.max(0, lead.commit - lead.flushed))} above F).
      </>,
      `It's the flush the leader makes${flushMs ? ` every ${dur(flushMs)}` : ' on its interval'}, early: one more set of bucket writes${f?.flushes ? ` (about ${fmtNum(Object.values(f.requests ?? {}).reduce((a, b) => a + b, 0) / f.flushes, 0)} requests)` : ''}, and the applier pauses for the state checkpoint${f ? ` (p99 ${fmtUs(f.seal_us.p99)})` : ''}.`,
      'Nothing is lost if it fails: the next flush covers the same entries.',
      ...(on ? [] : [<span className="s-err">The nodes run without --qlog-admin-token, so the leader refuses flushes on demand.</span>]),
    ],
    word: 'flush',
    action: 'Flush now',
    call: A.flushNowCall,
    run: () => A.flushNow(),
    done: (r) => {
      quorumPoll.refresh()
      const s = r as QStatus | undefined
      return s && typeof s.flushed === 'number' ? `Flushed: F is ${seqS(s.flushed)}` : 'Flushed'
    },
  })
}

function FlushPanel({ lead, flushMs, on }: { lead?: QStatus; flushMs?: number; on: boolean }) {
  const f = lead?.flush
  const recent = [...(f?.recent ?? [])].reverse()
  const [all, setAll] = useState(false)
  return (
    <Panel
      title="Flush"
      src={<Src>cluster/quorum · status.flush, flush.recent</Src>}
      right={
        <>
          {flushMs ? <span className="muted sm">every {dur(flushMs)}</span> : null}
          {lead && f && (
            <button type="button" className="cx-btn sm" onClick={() => flushNowDialog(lead, on, flushMs)} title={on ? undefined : 'The nodes run without --qlog-admin-token'}>
              Flush now…
            </button>
          )}
        </>
      }
    >
      {f ? (
        <>
          <KV
            style={{ padding: '10px 12px', margin: 0 }}
            rows={[
              [
                'Flushes',
                <span className="mono" key="f">
                  {fmtNum(f.flushes)}{' '}
                  <span className="muted">
                    ({fmtNum(f.aborted)} aborted, {fmtNum(f.failed)} failed, {fmtNum(f.fences)} fences)
                  </span>
                </span>,
              ],
              ['Duration p50 / p99', <span className="mono" key="d">{fmtUs(f.duration_us.p50)} / {fmtUs(f.duration_us.p99)}</span>],
              [
                'Seal pause p99',
                <span className="mono" key="s">
                  {fmtUs(f.seal_us.p99)} <span className="muted">(the applier waits for the state checkpoint)</span>
                </span>,
              ],
              [
                'Segments',
                <span className="mono" key="g">
                  {fmtNum(f.segments)} · {fmtBytes(f.segment_bytes)} <span className="muted">of {fmtBytes(f.raw_bytes)} raw</span>
                </span>,
              ],
              ['Per flush', <span className="mono" key="p">{f.flushes ? `~${fmtNum(Object.values(f.requests ?? {}).reduce((a, b) => a + b, 0) / f.flushes, 1)} requests` : '—'}</span>],
            ]}
          />
          <div className="cx-tw">
            <table className="cx-t compact">
              <thead>
                <tr>
                  <th>When</th>
                  <th className="r">F</th>
                  <th className="r">Entries</th>
                  <th className="r">Segs</th>
                  <th className="r">Stored</th>
                  <th className="r">Raw</th>
                  <th className="r" title="Seal to manifest CAS; the seal pause in brackets">Took</th>
                </tr>
              </thead>
              <tbody>
                {(all ? recent : recent.slice(0, 8)).map((x) => (
                  <tr key={`${x.epoch}-${x.flushed}-${x.at_ms}`} className={x.epoch !== lead?.epoch ? 'dim' : undefined}>
                    <td className="sm muted nowrap" title={`${dt(x.at_ms)} · epoch ${x.epoch}`}>
                      {clock(x.at_ms)}
                    </td>
                    <td className="r mono sm">{seqS(x.flushed)}</td>
                    <td className="r mono sm">{fmtNum(x.entries)}</td>
                    <td className="r mono sm">{fmtNum(x.segments)}</td>
                    <td className="r mono sm">{fmtBytes(x.bytes)}</td>
                    <td className="r mono sm t2">{fmtBytes(x.raw_bytes)}</td>
                    <td className="r mono sm nowrap" title={`seal pause ${fmtUs(x.seal_us)}`}>
                      {fmtUs(x.took_us)}
                    </td>
                  </tr>
                ))}
                {!recent.length && (
                  <tr>
                    <td colSpan={7}>
                      <Empty>{f.recent ? `${lead?.id ?? 'The leader'} hasn't flushed since it started.` : "The leader's status lists no recent flushes (flush.recent)."}</Empty>
                    </td>
                  </tr>
                )}
              </tbody>
            </table>
          </div>
          {recent.length > 8 && (
            <div className="cx-pn-b">
              <button type="button" className="cx-btn sm quiet" onClick={() => setAll(!all)}>
                {all ? 'Show the last 8' : `Show all ${recent.length}`}
              </button>
              <span className="muted sm"> the leader keeps its last 32</span>
            </div>
          )}
        </>
      ) : (
        <Empty title="No flush status">Only the leader flushes{lead ? ', and this one has no bucket configured' : ', and no member leads right now'}.</Empty>
      )}
    </Panel>
  )
}

// ---------------------------------------------------------------- host shards

function ShardPanel({ c, view }: { c?: ClusterView; view?: RelayView }) {
  const [focus, setFocus] = useState<string | null>(null)
  const [map, setMap] = useState(false)
  const [q, setQ] = useState('')
  const needle = q.trim().toLowerCase()
  const found = useLivePoll(() => (needle.length >= 2 ? A.hosts({ q: needle, sort: 'host', desc: false, limit: 6 }) : Promise.resolve(undefined)), `owner:${needle}`, 10_000, { keep: true })
  const owners = (c?.nodes ?? []).filter((n) => n.ownedHosts > 0 || !n.stale).sort((a, b) => a.id.localeCompare(b.id))
  const total = owners.reduce((a, n) => a + n.ownedHosts, 0) + (c?.unownedHosts ?? 0)
  const pct = (n: number) => (total ? (n / total) * 100 : 0)
  const parts = [...owners.map((n) => ({ id: n.id, n: n.ownedHosts, color: view?.byId.get(n.id)?.color })), ...(c?.unownedHosts ? [{ id: '', n: c.unownedHosts, color: undefined }] : [])]
  return (
    <Panel title="Host owners" src={<Src>cluster · hosts?q</Src>} right={<span className="muted sm">{c ? `${fmtNum(c.hosts)} hosts in the leader's table` : null}</span>}>
      <div className="cx-pn-b">
        {!c ? (
          <Empty>Loading…</Empty>
        ) : !total ? (
          <Empty>No hosts yet: the leader gives each host it learns of to a member as a log entry.</Empty>
        ) : (
          <>
            <div className={`cx-ownbar${focus ? ' focus' : ''}`} role="img" aria-label={parts.map((p) => `${p.id || 'unowned'} ${fmtNum(p.n)}`).join(', ')}>
              {parts.map((p) => (
                <i key={p.id || 'unowned'} className={`${p.id ? '' : 'unowned'}${focus === p.id ? ' hl' : ''}`} style={{ width: `${pct(p.n)}%`, background: p.color }} title={`${p.id || 'unowned'}: ${fmtNum(p.n)} hosts`} />
              ))}
            </div>
            <div className="cx-shardlegend cx-ownlegend">
              {owners.map((n) => (
                <button key={n.id} type="button" onMouseEnter={() => setFocus(n.id)} onMouseLeave={() => setFocus(null)} onFocus={() => setFocus(n.id)} onBlur={() => setFocus(null)} onClick={() => openPanel('node', n.id)}>
                  <Swatch color={view?.byId.get(n.id)?.color} />
                  <b className="mono">{Math.round(pct(n.ownedHosts))}%</b> {n.id} <span className="mono muted">{fmtNum(n.ownedHosts)}</span>
                </button>
              ))}
              {!!c.unownedHosts && (
                <span className="s-err">
                  <Swatch /> <b className="mono">{Math.round(pct(c.unownedHosts))}%</b> unowned <span className="mono">{fmtNum(c.unownedHosts)}</span>
                </span>
              )}
            </div>
          </>
        )}
        <div className="cx-form-row cx-ownfind">
          <input className="cx-inp mono" placeholder="find a host's owner" aria-label="Find a host's owner" value={q} onChange={(e) => setQ(e.target.value)} spellCheck={false} autoComplete="off" />
          <button type="button" className="cx-btn sm quiet" aria-expanded={map} onClick={() => setMap(!map)}>
            {map ? 'Hide the map' : 'Show the map'}
          </button>
        </div>
        {needle.length >= 2 &&
          (found.data?.hosts.length ? (
            <div className="cx-ownhits">
              {found.data.hosts.map((h, i) => (
                <RRow key={`${h.host}#${i}`} onClick={() => openPanel('host', h.host)} x={h.status}>
                  <Swatch color={h.node ? view?.byId.get(h.node)?.color : undefined} />
                  <span className="nm">
                    <HostName host={h.host} />
                  </span>
                  <span className="mono sm t2">{h.node || 'unowned'}</span>
                </RRow>
              ))}
              {found.data.total > found.data.hosts.length && <div className="muted sm">and {fmtNum(found.data.total - found.data.hosts.length)} more; type more of the name</div>}
            </div>
          ) : found.data ? (
            <div className="muted sm">No host matches “{needle}”.</div>
          ) : null)}
      </div>
      {map && <OwnerMap view={view} focus={focus} />}
    </Panel>
  )
}

/** One cell per host in the leader's table, coloured by its owner: behind "Show the map", since it asks for thousands of rows. */
function OwnerMap({ view, focus }: { view?: RelayView; focus: string | null }) {
  const [hover, setHover] = useState<HostRow | null>(null)
  const list = useLivePoll(() => A.hosts({ sort: 'host', desc: false, limit: OWNER_CAP }), 'owners', 10_000, { keep: true })
  const hosts = list.data?.hosts ?? []
  const dead = (h: HostRow) => !h.node || !!view?.byId.get(h.node)?.stale
  if (!list.data) return <Loaded load={list}>{() => null}</Loaded>
  return (
    <div className="cx-pn-b">
      <div className={`cx-shardmap${hosts.length > 64 ? ' dense' : ''}${focus ? ' focus' : ''}`} onMouseLeave={() => setHover(null)}>
        {hosts.map((h, i) => (
          <button
            key={`${h.host}#${i}`}
            type="button"
            className={`cx-shard${dead(h) ? ' unowned' : ''}${focus && h.node === focus ? ' hl' : ''}`}
            style={dead(h) ? undefined : { background: view?.byId.get(h.node)?.color ?? 'var(--idle)' }}
            onMouseEnter={() => setHover(h)}
            onFocus={() => setHover(h)}
            onClick={() => openPanel('host', h.host)}
            aria-label={`${h.host}: ${h.node || 'unowned'}`}
          />
        ))}
      </div>
      <div className="cx-shardinfo">
        {hover ? (
          <>
            <span className="mono">{hover.host}</span> · {hover.node ? `read by ${hover.node}` : 'nobody reads it'} · {hover.status}
          </>
        ) : (
          `One cell per host in the leader's table${list.data.total > hosts.length ? ` (the first ${fmtNum(hosts.length)} of ${fmtNum(list.data.total)})` : ''}. The leader gives each to a member as a log entry and moves a dead member's hosts after the failover timeout.`
        )}
      </div>
    </div>
  )
}

const OWNER_CAP = 4000

// ---------------------------------------------------------------- counters, pipeline

const COUNTERS: [keyof QStatus, string, boolean?][] = [
  ['takeovers', 'Takeovers'],
  ['step_downs', 'Step-downs'],
  ['promise_rounds', 'Promise rounds'],
  ['resets', 'Resets', true],
  ['emit_gaps', 'Emit gaps', true],
  ['disk_reads', 'Disk reads'],
  ['bucket_reads', 'Bucket reads'],
  ['lost_quorums', 'Lost quorums'],
  ['recoveries', 'Recoveries'],
]

function Counters({ rows }: { rows: MemberRow[] }) {
  const live = rows.filter((r) => r.s)
  return (
    <Panel title="Counters" src={<Src>cluster/quorum · status</Src>} right={<span className="muted sm">since each process started; emit gaps and resets stay at 0</span>}>
      <div className="cx-tw">
        <table className="cx-t compact">
          <thead>
            <tr>
              <th>Counter</th>
              {live.map((r) => (
                <th key={r.id} className="r">
                  {r.id}
                </th>
              ))}
            </tr>
          </thead>
          <tbody>
            {COUNTERS.map(([k, label, bad]) => (
              <tr key={k}>
                <td>{label}</td>
                {live.map((r) => {
                  const v = Number(r.s![k] ?? 0)
                  return (
                    <td key={r.id} className={`r mono sm${bad && v > 0 ? ' s-err' : ''}`}>
                      {fmtNum(v)}
                    </td>
                  )
                })}
              </tr>
            ))}
          </tbody>
        </table>
      </div>
    </Panel>
  )
}

function Pipeline({ view }: { view?: RelayView }) {
  const p = useLivePoll(A.pipelineOpt, 'pipeline', 5000)
  const d = p.data
  let body: ReactNode
  if (!d) body = <Loaded load={p}>{() => null}</Loaded>
  else if (!d.supported) body = <NeedsVersion what="The ack backlog" endpoint="GET ops/pipeline" />
  else if (!d.data.nodes.length) body = <Empty>No node reported its backlog.</Empty>
  else
    body = (
      <div className="cx-tw">
        <table className="cx-t compact">
          <thead>
            <tr>
              <th>Node</th>
              <th className="r">In flight</th>
              <th className="r">Oldest</th>
              <th className="r">Lane queue</th>
              <th className="r">Dedupe</th>
              <th className="r">Paused readers</th>
            </tr>
          </thead>
          <tbody>
            {d.data.nodes.map((n) => (
              <tr key={n.node} className={n.stale ? 'dim' : undefined}>
                <td>
                  <NodeTag view={view} id={n.node} />
                </td>
                <td className="r mono sm">{n.stale ? '—' : fmtNum(n.ackPending)}</td>
                <td className={`r mono sm${n.oldestPendingMs > 5000 ? ' s-warn' : ''}`}>{n.stale ? '—' : fmtMs(n.oldestPendingMs)}</td>
                <td className="r mono sm">{n.stale ? '—' : fmtNum(n.laneQueued)}</td>
                <td className="r mono sm">{n.stale ? '—' : fmtNum(n.dedupeEntries)}</td>
                <td className="r mono sm">{n.stale ? '—' : fmtNum(n.pausedHosts)}</td>
              </tr>
            ))}
          </tbody>
        </table>
        {d.data.hosts.length > 0 && (
          <table className="cx-t compact">
            <thead>
              <tr>
                <th>Host with work in flight</th>
                <th className="r">In flight</th>
                <th className="r">Events/s</th>
                <th>Reader</th>
              </tr>
            </thead>
            <tbody>
              {d.data.hosts.slice(0, 8).map((h) => (
                <tr key={`${h.node}/${h.host}`} data-open={`host:${h.host}`} onClick={() => openPanel('host', h.host)}>
                  <td className="trunc" style={{ maxWidth: 220 }}>
                    <HostName host={h.host} />
                  </td>
                  <td className="r mono sm">
                    {fmtNum(h.inflight)}
                    {h.inflightCap != null && <span className="muted"> / {fmtNum(h.inflightCap)}</span>}
                  </td>
                  <td className="r mono sm">{fmtSi(h.eventsPerSec)}</td>
                  <td>{h.paused ? <Chip k="warn">paused</Chip> : <span className="muted sm">reading</span>}</td>
                </tr>
              ))}
            </tbody>
          </table>
        )}
      </div>
    )
  return (
    <Panel title="Ack backlog" src={<Src>ops/pipeline</Src>} right={<span className="muted sm">read upstream, not yet durable</span>}>
      {body}
    </Panel>
  )
}

// ---------------------------------------------------------------- no quorum log

function NoQuorum({ c, view }: { c?: ClusterView; view?: RelayView }) {
  const nodes = view?.nodes ?? []
  return (
    <>
      <PageHead title="Quorum & cluster" sub={<span>{view?.single ? 'a single node' : `${nodes.length} nodes`}, without the quorum log</span>} />
      <Panel title="Nodes" src={<Src>cluster</Src>}>
        <div className="cx-tw">
          <table className="cx-t compact">
            <thead>
              <tr>
                <th>Node</th>
                <th>Role</th>
                <th className="r">Consumers</th>
                <th className="r">In/s</th>
                <th className="r">Out/s</th>
                <th>Build</th>
              </tr>
            </thead>
            <tbody>
              {nodes.map((n) => (
                <tr key={n.id} data-open={`node:${n.id}`} onClick={() => openPanel('node', n.id)} className={n.stale ? 'dim' : undefined}>
                  <td>
                    <NodeTag view={view} id={n.id} />
                  </td>
                  <td className="sm">{n.role}</td>
                  <td className="r mono sm">{fmtNum(n.consumers)}</td>
                  <td className="r mono sm">{fmtSi(n.eventsInPerSec)}</td>
                  <td className="r mono sm">{fmtSi(n.eventsOutPerSec)}</td>
                  <td className="mono sm t2">{n.version || '—'}</td>
                </tr>
              ))}
            </tbody>
          </table>
        </div>
      </Panel>
      <Panel title="The quorum log" className="cx-mt">
        <Empty title="This relay runs without the quorum log">
          Its log stays on this node. Start the nodes with <span className="mono">--quorum</span> to replicate it to a majority and flush it to the bucket (the docs' Quorum cluster page).
          {c && c.lastSeq ? <> The stream is at seq {seqS(c.lastSeq)}.</> : null}
        </Empty>
      </Panel>
    </>
  )
}

