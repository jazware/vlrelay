import type { ReactNode } from 'react'
import { registerDetail } from '../../components/console/Drawer'
import { Bars, Chip, Empty, HostName, KV, NeedsVersion, RRow, Sec, Strip } from '../../components/console/kit'
import { openPanel } from '../../components/console/nav'
import { ago, dt, fmtBytes, fmtMs, fmtNum, fmtSi, fmtUs, seqS } from '../../lib/console/fmt'
import { clusterPoll, overviewPoll, quorumPoll, seenEpochs, settingsPoll } from '../../lib/console/polls'
import { useRelay } from '../../lib/console/relay'
import { EpochChip, epochEvents, memberRows, membersDialog, membershipOn, refStatus, RoleChip, SetDiff } from './quorumUi'
import { NodeTag } from './relayUi'

// A member (or a node of an older cluster) and an epoch change, in the slide-over or on a page.

const cols = (page: boolean, a: ReactNode, b: ReactNode) =>
  page ? (
    <div className="cols">
      <div>{a}</div>
      <div>{b}</div>
    </div>
  ) : (
    <>
      {a}
      {b}
    </>
  )

const Act = ({ title, desc, children }: { title: string; desc: ReactNode; children: ReactNode }) => (
  <div className="cx-act">
    <div className="ad">
      <b>{title}</b>
      {desc}
    </div>
    {children}
  </div>
)

registerDetail('node', {
  kind: 'Node',
  section: 'quorum',
  use: (id, mode) => {
    const { view } = useRelay()
    const qp = quorumPoll.use()
    const cp = clusterPoll.use()
    const ov = overviewPoll.use()
    const sp = settingsPoll.use()
    const qv = qp.data?.supported ? qp.data.data : undefined
    const rows = qv ? memberRows(qv, view) : []
    const row = rows.find((r) => r.id === id)
    const ref = qv ? refStatus(qv) : undefined
    const lead = rows.find((r) => r.kind === 'leader')?.s ?? undefined
    const n = view?.byId.get(id)
    const cn = cp.data?.nodes.find((x) => x.id === id)
    if (!row && !n && !cn) {
      const loading = qp.loading || cp.loading
      return { title: id, body: null, loading, missing: loading ? undefined : `${id} isn't a node of this relay any more.` }
    }
    const s = row?.s ?? null
    const dead = !!(row?.stale || n?.stale)
    const owned = (cp.data?.hostShards ?? []).filter((o) => o === id).length
    const busy = (ov.data?.topHosts ?? []).filter((h) => h.node === id).slice(0, 8)
    const members = ref?.members ?? []
    const isMember = members.includes(id)
    const head = lead?.last ?? s?.last ?? 0
    const main = (
      <>
        <Strip
          items={[
            ['CPU', dead || !cn?.memBytes ? '—' : `${cn.cpu.toFixed(2)} cores`],
            ['memory', dead || !cn?.memBytes ? '—' : fmtBytes(cn.memBytes)],
            ['consumers', dead ? '—' : fmtNum(n?.consumers ?? cn?.consumers ?? 0)],
            ['in/s', dead ? '—' : fmtSi(n?.eventsInPerSec ?? 0)],
            ['out/s', dead ? '—' : fmtSi(n?.eventsOutPerSec ?? 0)],
          ]}
        />
        {qv && (
          <Sec title="Quorum log" digest={dead ? (row?.error ?? 'no answer') : s ? `lag ${fmtNum(Math.max(0, head - s.last))} entries` : ''} open>
            {s ? (
              <KV
                rows={[
                  ['Last (acked)', <span className="mono">{seqS(s.last)}</span>],
                  ['Commit', <span className="mono">{seqS(s.commit)}</span>],
                  ['Emitted', <span className="mono">{seqS(s.emitted)}</span>],
                  ['F · R', <span className="mono">{seqS(s.flushed)} · {seqS(s.reserve)}</span>],
                  ['Log', <>{s.intact ? 'intact' : 'not intact'} · {fmtBytes(s.log_bytes)} from seq {seqS(s.base)}</>],
                  ['Disk', s.disk ? <>{fmtBytes(s.disk.disk_bytes)} · fsync p50 {fmtUs(s.disk.fsync_us.p50)}, p99 {fmtUs(s.disk.fsync_us.p99)}</> : 'in memory only'],
                  ['Epoch', <span className="mono">{s.epoch} (promised {s.promised})</span>],
                  ['Last contact', row ? ago(row.reportedMs) : '—'],
                ]}
              />
            ) : (
              <Empty>{row?.error ?? 'It didn’t answer this round.'}</Empty>
            )}
          </Sec>
        )}
        <Sec title="Hosts it reads" digest={`${fmtNum(owned || n?.hostShards || 0)} ${qv ? 'hosts' : 'host shards'}`} open flush>
          {busy.length ? (
            busy.map((h) => (
              <RRow key={h.host} onClick={() => openPanel('host', h.host)} x={`${fmtSi(h.eventsPerSec)}/s`}>
                <HostName host={h.host} />
              </RRow>
            ))
          ) : (
            <Empty>None of the busiest hosts. Hosts lists every host with the member reading it.</Empty>
          )}
        </Sec>
      </>
    )
    const side = (
      <>
        <Sec title="Box" open>
          <KV
            rows={[
              ['Address', <span className="mono">{row?.addr || n?.addr || cn?.addr || '—'}</span>],
              ['Build', <span className="mono">{cn?.version || n?.version || '—'}{cn?.rev ? ` · ${cn.rev.slice(0, 8)}` : ''}</span>],
              ['Role', row ? <RoleChip kind={row.kind} /> : (n?.role ?? '—')],
              ['Stream seq', <span className="mono">{cn?.streamSeq ? seqS(cn.streamSeq) : '—'}</span>],
            ]}
          />
        </Sec>
        {s && (
          <Sec title="Counters" digest="since it started">
            <KV
              rows={[
                ['Takeovers', fmtNum(s.takeovers)],
                ['Step-downs', fmtNum(s.step_downs)],
                ['Resets', <span className={s.resets ? 's-err' : undefined}>{fmtNum(s.resets)}</span>],
                ['Emit gaps', <span className={s.emit_gaps ? 's-err' : undefined}>{fmtNum(s.emit_gaps)}</span>],
                ['Disk / bucket reads', `${fmtNum(s.disk_reads)} / ${fmtNum(s.bucket_reads)}`],
                ['Lost quorums', fmtNum(s.lost_quorums)],
              ]}
            />
          </Sec>
        )}
        {qv && (
          <Sec title="Membership" open flush danger={isMember}>
            <div className="cx-acts">
              {isMember ? (
                <Act title="Remove from the quorum" desc="The leader drains, flushes and moves to the next epoch without it. Its hosts move to the others.">
                  <button type="button" className="cx-btn sm danger" onClick={() => membersDialog({ current: members, leader: lead?.id ?? null, known: rows.map((r) => r.id), remove: id, on: membershipOn(sp.data) })}>
                    Change membership…
                  </button>
                </Act>
              ) : (
                <Act title="Add to the quorum" desc="It joins as a learner, copies the log and becomes a member once it holds the commit index.">
                  <button type="button" className="cx-btn sm" onClick={() => membersDialog({ current: members, leader: lead?.id ?? null, known: rows.map((r) => r.id), on: membershipOn(sp.data) })}>
                    Change membership…
                  </button>
                </Act>
              )}
            </div>
          </Sec>
        )}
      </>
    )
    return {
      title: id,
      chip: row ? <RoleChip kind={row.kind} /> : n?.stale ? <Chip k="err">no answer</Chip> : <Chip k="plain">{n?.role ?? cn?.role ?? 'node'}</Chip>,
      foot: <>GET /admin/api/cluster · cluster/quorum</>,
      body: cols(mode === 'page', main, side),
    }
  },
})

registerDetail('epoch', {
  kind: 'Epoch change',
  section: 'quorum',
  use: (id) => {
    const { view } = useRelay()
    const qp = quorumPoll.use()
    const qv = qp.data?.supported ? qp.data.data : undefined
    const e = epochEvents(qv, seenEpochs()).find((x) => x.id === id)
    if (!e) return { title: id, body: null, loading: qp.loading, missing: qp.loading ? undefined : 'No member lists this epoch change any more (the statuses keep what the running leaders saw).' }
    const head = (
      <Strip
        items={[
          ['leads after', e.leader ? <NodeTag view={view} id={e.leader} /> : '—'],
          ['when', e.atMs ? dt(e.atMs) : '—'],
          ['epoch', `${e.fromEpoch ?? '?'} → ${e.epoch}`],
        ]}
      />
    )
    let body: ReactNode
    if (e.kind === 'switch' && e.sw) {
      const w = e.sw
      body = (
        <>
          {head}
          <SetDiff from={w.from} to={w.to} leader={w.leader} />
          <Sec title="Timings" digest={`appends paused ${fmtMs(w.paused_ms)}`} open flush>
            <Bars
              rows={(
                [
                  ['record learners', w.record_ms, 'idle'],
                  ['catch-up', w.catch_up_ms, 'idle'],
                  ['pre-flush', w.pre_flush_ms, 'idle'],
                  ['drain (paused)', w.drain_ms, 'signal'],
                  ['flush (paused)', w.flush_ms, 'signal'],
                  ['CAS (paused)', w.cas_ms, 'signal'],
                ] as [string, number, string][]
              ).map(([label, v, color]) => ({ key: label, label, v, fmt: fmtMs(v), color }))}
            />
          </Sec>
          <KV rows={[['Flushed to', <span className="mono">{seqS(w.flushed)}</span>]]} />
        </>
      )
    } else if (e.kind === 'recovery' && e.rec) {
      const r = e.rec
      body = (
        <>
          {head}
          <Strip
            items={[
              ['old F', seqS(r.manifest_flushed)],
              ['salvaged to', seqS(r.after)],
              ['resumed at', seqS(r.base)],
              ['seqs skipped', `+${fmtNum(Math.max(0, r.base - r.after - 1))}`],
            ]}
          />
          <p className="cx-lede">
            No quorum of intact logs was left, so the leader cloned the state from the last manifest, applied what the members still held and resumed past R. Consumers saw a jump from {seqS(r.after)}{' '}
            to {seqS(r.base)}, never a rewind.
          </p>
          <Sec title="What it read" open>
            <KV
              rows={[
                ['Generation', fmtNum(r.generation)],
                ['Orphan segments', fmtNum(r.orphan_segments)],
                ['Salvaged entries', fmtNum(r.salvaged)],
                ['Orphans to', <span className="mono">{seqS(r.orphans_to)}</span>],
              ]}
            />
          </Sec>
          <Sec title="Timings" digest={`total ${fmtMs(r.total_ms)}`} open flush>
            <Bars
              color="err"
              rows={(
                [
                  ['read manifest + orphans', r.read_ms],
                  ['clone state', r.clone_ms],
                  ['apply + seal', r.apply_seal_ms],
                  ['segments', r.segments_ms],
                  ['manifest CAS', r.manifest_ms],
                ] as [string, number][]
              ).map(([label, v]) => ({ key: label, label, v, fmt: fmtMs(v) }))}
            />
          </Sec>
        </>
      )
    } else {
      body = (
        <>
          {head}
          <p className="cx-lede">
            The console saw the epoch move from {e.fromEpoch} to {e.epoch} between two polls{e.leader ? `, with ${e.leader} leading after` : ''}. No status lists it as a membership change or a recovery, so it was a
            takeover or a planned handoff.
          </p>
          <NeedsVersion what="Which kind, and how long the firehose paused" endpoint="GET cluster/quorum/history" />
        </>
      )
    }
    return { title: `Epoch ${e.fromEpoch ?? '?'} → ${e.epoch}`, chip: <EpochChip kind={e.kind} />, foot: <>cluster/quorum · status.{e.kind === 'recovery' ? 'recovered' : 'switches'}</>, body }
  },
})
