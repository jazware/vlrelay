import { useState } from 'react'
import { Bar, Live, Tile } from '../components/relay'
import { Confirm, Empty, ErrorNotice, Field, Loading, Notice, Panel, Status } from '../components/ui'
import { api, ApiError, type QStatus, type QuorumNode, type QuorumView } from '../lib/api'
import { fmtBytes, fmtNum, fmtTime, relTime } from '../lib/format'
import { useAction } from '../lib/hooks'
import { useApi } from '../lib/useApi'
import './ops2.css'

const POLL = 2000

const us = (v?: number) => (v === undefined ? '—' : v >= 1000 ? `${(v / 1000).toFixed(v >= 10_000 ? 0 : 1)} ms` : `${v} µs`)
const ms = (v?: number) => (v === undefined ? '—' : v >= 1000 ? `${(v / 1000).toFixed(1)} s` : `${v} ms`)
const seq = (v?: number) => (v === undefined ? '—' : fmtNum(v))

type Row = QuorumNode & { s: QStatus | null; kind: 'leader' | 'member' | 'learner' | 'retired' | 'unknown' }

export function Quorum() {
  const l = useApi<QuorumView>('cluster/quorum', undefined, POLL)
  if (l.error instanceof ApiError && l.error.status === 404)
    return (
      <>
        <div className="console-head">
          <h1>Quorum log</h1>
        </div>
        <Notice kind="info">{l.error.message}. Nodes sequence through the per-node logs; this page fills in once the cluster runs the quorum log (docs/quorum.md).</Notice>
      </>
    )
  const q = l.data
  if (!q) return l.error ? <ErrorNotice error={l.error} /> : <Loading />

  const live = q.nodes.filter((n) => !n.stale && n.status)
  const leader = live.find((n) => n.status!.role === 'leader')?.status ?? null
  const ref = leader ?? live[0]?.status ?? null
  const members = ref?.members ?? []
  const learners = ref?.learners ?? []
  const rows: Row[] = q.nodes.map((n) => {
    const s = n.status
    const kind = s?.role === 'leader' ? 'leader' : s?.retired ? 'retired' : members.includes(n.node) ? 'member' : learners.includes(n.node) ? 'learner' : 'unknown'
    return { ...n, s, kind }
  })
  const order = { leader: 0, member: 1, learner: 2, unknown: 3, retired: 4 }
  rows.sort((a, b) => order[a.kind] - order[b.kind] || a.node.localeCompare(b.node))
  const answering = members.filter((m) => live.some((n) => n.node === m)).length
  const majority = Math.floor(members.length / 2) + 1
  const stale = q.nodes.filter((n) => n.stale)
  const epochs = new Set(live.map((n) => n.status!.epoch))
  const head = leader?.last ?? Math.max(0, ...live.map((n) => n.status!.last))
  const maxLag = Math.max(1, ...rows.filter((r) => r.s && r.kind !== 'retired').map((r) => head - r.s!.last))

  return (
    <>
      <div className="console-head">
        <h1>Quorum log</h1>
        <Live at={l.at} error={l.error} every={POLL} />
      </div>
      <ErrorNotice error={l.error} />
      {!leader && <Notice kind="err">No node reports itself leader. Nothing commits until a takeover finishes; consumers keep their sockets and wait.</Notice>}
      {leader && answering < majority && <Notice kind="err">Only {answering} of {members.length} members answer: below a majority, so nothing new commits.</Notice>}
      {stale.length > 0 && <Notice kind="warn">Not answering: {stale.map((n) => `${n.node} (${n.error ?? 'no answer'})`).join(', ')}.</Notice>}
      {epochs.size > 1 && <Notice kind="warn">Nodes report different epochs ({[...epochs].join(', ')}): a takeover or membership change is in flight.</Notice>}
      {learners.length > 0 && <Notice kind="info">Catching up as learners: {learners.join(', ')}. They join the member set once they hold the commit index.</Notice>}
      {leader?.paused && <Notice kind="warn">The leader is paused for a membership switch: appends wait until it finishes.</Notice>}

      <div className="tiles qtiles">
        <Tile k="Epoch" v={<span className="mono">{ref ? ref.epoch : '—'}</span>} sub={ref ? `promised ${ref.promised}, last entry's ${ref.last_epoch}` : undefined} />
        <Tile k="Leader" v={leader ? leader.id : '—'} tone={leader ? undefined : 'bad'} sub={leader ? `${fmtNum(leader.takeovers)} takeovers here` : 'none'} />
        <Tile k="Members answering" v={<>{answering}<small>of {members.length}</small></>} tone={answering < majority ? 'bad' : answering < members.length ? 'warn' : undefined} sub={`majority is ${majority}`} />
        <Tile k="Commit index" v={<span className="mono">{seq(leader?.commit)}</span>} sub={leader ? `${fmtNum(leader.last - leader.commit)} appended, not yet committed` : undefined} />
        <Tile k="Flushed (F)" v={<span className="mono">{seq(ref?.flushed)}</span>} sub={leader ? `${fmtNum(leader.commit - leader.flushed)} committed above F` : undefined} />
        <Tile k="Reserve (R)" v={<span className="mono">{seq(ref?.reserve)}</span>} sub={leader ? `commit may run ${fmtNum(leader.reserve - leader.commit)} further` : undefined} tone={leader && leader.reserve - leader.commit < 20_000 ? 'warn' : undefined} />
        <Tile k="Commit latency p50 / p99" v={<>{us(leader?.commit_us.p50)}<small>/ {us(leader?.commit_us.p99)}</small></>} sub="append to a majority's ack" />
        <Tile k="Recoveries" v={fmtNum(ref?.recoveries ?? 0)} sub={`generation ${ref?.generation ?? 0}, lost quorums: ${fmtNum(Math.max(0, ...live.map((n) => n.status!.lost_quorums)))}`} />
      </div>

      <Panel flush title="Members" desc="Each node's own view of its log. Lag is entries behind the leader's last append. Last contact is when its status was last read.">
        <div className="table-wrap">
          <table className="data compact">
            <thead>
              <tr>
                <th>Node</th>
                <th>Role</th>
                <th>Address</th>
                <th>Last contact</th>
                <th className="num">Last (acked)</th>
                <th className="num">Commit</th>
                <th className="num">Emitted</th>
                <th>Lag</th>
                <th className="num">F</th>
                <th>Log</th>
                <th className="num">Disk</th>
                <th className="num">fsync p99</th>
              </tr>
            </thead>
            <tbody>
              {rows.map((r) => {
                const s = r.s
                const lag = s ? head - s.last : 0
                const dash = <span className="muted">—</span>
                return (
                  <tr key={r.node} className={r.stale ? 'stale' : undefined}>
                    <td>
                      <b>{r.node}</b>
                    </td>
                    <td>
                      {r.stale ? (
                        <Status kind="bad">not answering</Status>
                      ) : r.kind === 'leader' ? (
                        <Status kind="ok">leader</Status>
                      ) : r.kind === 'learner' ? (
                        <Status kind="warn">learner</Status>
                      ) : r.kind === 'retired' ? (
                        <Status kind="idle">retired</Status>
                      ) : s?.role === 'candidate' ? (
                        <Status kind="warn">candidate</Status>
                      ) : (
                        <Status kind="ok">follower</Status>
                      )}
                    </td>
                    <td className="mono muted">{r.addr || dash}</td>
                    <td className="nowrap" title={fmtTime(r.reportedMs)}>
                      {relTime(r.reportedMs)}
                    </td>
                    <td className="num mono">{s ? seq(s.last) : dash}</td>
                    <td className="num mono">{s ? seq(s.commit) : dash}</td>
                    <td className="num mono">{s ? seq(s.emitted) : dash}</td>
                    <td className="lagcell">
                      {s && r.kind !== 'retired' ? (
                        <>
                          <span className={`mono${lag > 10_000 ? ' err-hi' : lag > 200 ? ' err-mid' : ''}`}>{fmtNum(lag)}</span>
                          <Bar frac={lag / maxLag} color={lag > 200 ? 'amber' : 'accent'} />
                        </>
                      ) : (
                        dash
                      )}
                    </td>
                    <td className="num mono">{s ? seq(s.flushed) : dash}</td>
                    <td>{s ? s.intact ? <span className="muted">intact, {fmtBytes(s.log_bytes)}</span> : <Status kind="warn">not intact</Status> : dash}</td>
                    <td className="num">{s?.disk ? fmtBytes(s.disk.disk_bytes) : dash}</td>
                    <td className="num">{s?.disk ? us(s.disk.fsync_us.p99) : dash}</td>
                  </tr>
                )
              })}
            </tbody>
          </table>
        </div>
      </Panel>

      <div className="grid2">
        {leader?.flush ? <FlushPanel s={leader} /> : <Panel title="Flush">{<Empty title="No flush status">Only the leader flushes, and only with a bucket configured.</Empty>}</Panel>}
        <CountersPanel rows={rows} />
      </div>

      <ChangesPanel leader={leader} />
      <RecoveriesPanel leader={leader} />
      <RequestsPanel rows={rows} />
      {leader && <MembershipPanel leader={leader} known={rows.map((r) => r.node)} onDone={l.reload} />}
    </>
  )
}

function FlushPanel({ s }: { s: QStatus }) {
  const f = s.flush!
  const reqs = Object.entries(f.requests_total ?? {}).sort((a, b) => b[1] - a[1])
  return (
    <Panel title="Flush" desc="The leader seals the log into segments in the bucket and CASes the manifest: F moves up, and R sets how far commit may run ahead of the last flush.">
      <dl className="dl compact kv2">
        <dt>F (flushed)</dt>
        <dd className="mono">{seq(f.last_flushed)}</dd>
        <dt>R (reserve)</dt>
        <dd className="mono">{seq(f.last_reserve)}</dd>
        <dt>Flushes</dt>
        <dd>
          {fmtNum(f.flushes)} <span className="muted small">({fmtNum(f.aborted)} aborted, {fmtNum(f.failed)} failed, {fmtNum(f.fences)} fences)</span>
        </dd>
        <dt>Flush p50 / p99</dt>
        <dd>
          {us(f.duration_us.p50)} / {us(f.duration_us.p99)}
        </dd>
        <dt>Seal pause p99</dt>
        <dd>{us(f.seal_us.p99)}</dd>
        <dt>Segments</dt>
        <dd>
          {fmtNum(f.segments)} <span className="muted small">{fmtBytes(f.segment_bytes)} stored of {fmtBytes(f.raw_bytes)} raw</span>
        </dd>
        <dt>Bucket requests</dt>
        <dd className="small">
          {reqs.length ? reqs.map(([k, v]) => (
            <span key={k} className="nowrap" style={{ marginRight: 12 }}>
              <span className="mono">{k}</span> {fmtNum(v)}
            </span>
          )) : '—'}
        </dd>
      </dl>
    </Panel>
  )
}

const COUNTERS: [keyof QStatus, string][] = [
  ['takeovers', 'Takeovers'],
  ['step_downs', 'Step-downs'],
  ['promise_rounds', 'Promise rounds'],
  ['resets', 'Resets'],
  ['emit_gaps', 'Emit gaps'],
  ['disk_reads', 'Disk reads'],
  ['bucket_reads', 'Bucket reads'],
  ['lost_quorums', 'Lost quorums'],
]

function CountersPanel({ rows }: { rows: Row[] }) {
  const live = rows.filter((r) => r.s)
  return (
    <Panel flush title="Counters" desc="Since each node started. Emit gaps and resets should stay at 0.">
      <div className="table-wrap">
        <table className="data compact">
          <thead>
            <tr>
              <th>Counter</th>
              {live.map((r) => (
                <th key={r.node} className="num">
                  {r.node}
                </th>
              ))}
            </tr>
          </thead>
          <tbody>
            {COUNTERS.map(([k, label]) => (
              <tr key={k}>
                <td>{label}</td>
                {live.map((r) => {
                  const v = Number(r.s![k] ?? 0)
                  const bad = (k === 'emit_gaps' || k === 'resets') && v > 0
                  return (
                    <td key={r.node} className={`num${bad ? ' err-hi' : ''}`}>
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

function SetDiff({ from, to }: { from: string[]; to: string[] }) {
  const all = [...new Set([...from, ...to])].sort()
  return (
    <span className="setdiff">
      {all.map((m) => (
        <span key={m} className={!from.includes(m) ? 'add' : !to.includes(m) ? 'del' : ''}>
          {!from.includes(m) ? '+' : !to.includes(m) ? '−' : ''}
          {m}
        </span>
      ))}
    </span>
  )
}

function ChangesPanel({ leader }: { leader: QStatus | null }) {
  const sw = leader?.switches ?? []
  return (
    <Panel flush title="Membership changes" desc="Run by the current leader, newest first. Catch-up is the learners reaching the commit index; the pause is the only time appends wait.">
      {sw.length === 0 ? (
        <Empty title="None recorded">This leader hasn't run a membership change since it started.</Empty>
      ) : (
        <div className="table-wrap">
          <table className="data compact">
            <thead>
              <tr>
                <th>When</th>
                <th>Epoch</th>
                <th>Members</th>
                <th>Leader after</th>
                <th className="num">Catch-up</th>
                <th className="num">Drain</th>
                <th className="num">Flush</th>
                <th className="num">CAS</th>
                <th className="num">Paused</th>
                <th className="num">At F</th>
              </tr>
            </thead>
            <tbody>
              {sw.map((w) => (
                <tr key={`${w.epoch}-${w.at_ms}`}>
                  <td className="nowrap" title={fmtTime(w.at_ms)}>
                    {relTime(w.at_ms)}
                  </td>
                  <td className="mono">
                    {w.from_epoch} → {w.epoch}
                  </td>
                  <td>
                    <SetDiff from={w.from} to={w.to} />
                  </td>
                  <td>{w.leader}</td>
                  <td className="num">{ms(w.catch_up_ms)}</td>
                  <td className="num">{ms(w.drain_ms)}</td>
                  <td className="num">{ms(w.flush_ms)}</td>
                  <td className="num">{ms(w.cas_ms)}</td>
                  <td className={`num${w.paused_ms > 2000 ? ' err-mid' : ''}`}>{ms(w.paused_ms)}</td>
                  <td className="num mono">{seq(w.flushed)}</td>
                </tr>
              ))}
            </tbody>
          </table>
        </div>
      )}
    </Panel>
  )
}

function RecoveriesPanel({ leader }: { leader: QStatus | null }) {
  const rs = leader?.recovered ?? []
  if (!rs.length) return null
  return (
    <Panel flush title="Bucket recoveries" desc="Run when a leader found no quorum of intact logs: the state cloned from the last manifest, orphans and salvage applied, seqs resuming at R + 1.">
      <div className="table-wrap">
        <table className="data compact">
          <thead>
            <tr>
              <th>Generation</th>
              <th>Epoch</th>
              <th className="num">Old F</th>
              <th className="num">Resumed after</th>
              <th className="num">Orphan segments</th>
              <th className="num">Salvaged</th>
              <th className="num">Read</th>
              <th className="num">Clone</th>
              <th className="num">Apply + seal</th>
              <th className="num">Total</th>
            </tr>
          </thead>
          <tbody>
            {rs.map((r) => (
              <tr key={r.generation}>
                <td className="mono">{r.generation}</td>
                <td className="mono">{r.epoch}</td>
                <td className="num mono">{seq(r.manifest_flushed)}</td>
                <td className="num mono">{seq(r.after)}</td>
                <td className="num">{fmtNum(r.orphan_segments)}</td>
                <td className="num">{fmtNum(r.salvaged)}</td>
                <td className="num">{ms(r.read_ms)}</td>
                <td className="num">{ms(r.clone_ms)}</td>
                <td className="num">{ms(r.apply_seal_ms)}</td>
                <td className="num">{ms(r.total_ms)}</td>
              </tr>
            ))}
          </tbody>
        </table>
      </div>
    </Panel>
  )
}

/** Newer builds report bucket requests by class and purpose; shown as reported. */
function RequestsPanel({ rows }: { rows: Row[] }) {
  const withReq = rows.filter((r) => r.s?.requests && typeof r.s.requests === 'object')
  if (!withReq.length) return null
  const flat = (o: Record<string, unknown>, p = ''): [string, number][] =>
    Object.entries(o).flatMap(([k, v]) => (typeof v === 'number' ? [[p + k, v] as [string, number]] : v && typeof v === 'object' ? flat(v as Record<string, unknown>, `${p}${k}.`) : []))
  const per = withReq.map((r) => [r.node, new Map(flat(r.s!.requests!))] as const)
  const keys = [...new Set(per.flatMap(([, m]) => [...m.keys()]))].sort()
  return (
    <Panel flush title="Bucket requests" desc="By request class and purpose, since each node started.">
      <div className="table-wrap">
        <table className="data compact">
          <thead>
            <tr>
              <th>Request</th>
              {per.map(([n]) => (
                <th key={n} className="num">
                  {n}
                </th>
              ))}
            </tr>
          </thead>
          <tbody>
            {keys.map((k) => (
              <tr key={k}>
                <td className="mono">{k}</td>
                {per.map(([n, m]) => (
                  <td key={n} className="num">
                    {fmtNum(m.get(k))}
                  </td>
                ))}
              </tr>
            ))}
          </tbody>
        </table>
      </div>
    </Panel>
  )
}

const CONFIRM = 'change members'

function MembershipPanel({ leader, known, onDone }: { leader: QStatus; known: string[]; onDone: () => void }) {
  const current = leader.members
  const [want, setWant] = useState<string[] | null>(null)
  const [addId, setAddId] = useState('')
  const [addAddr, setAddAddr] = useState('')
  const [addrs, setAddrs] = useState<Record<string, string>>({})
  const [open, setOpen] = useState(false)
  const [done, setDone] = useState<string>()
  const set = want ?? current
  const changed = JSON.stringify([...set].sort()) !== JSON.stringify([...current].sort())
  const tooFew = set.length < 3
  const act = useAction(async () => {
    await api('cluster/quorum/members', { body: { members: set, addrs } })
    setOpen(false)
    setWant(null)
    setAddrs({})
    setDone(`Sent to ${leader.id}: ${set.join(', ')}.`)
    onDone()
  })
  const add = () => {
    const id = addId.trim()
    if (!id || set.includes(id)) return
    setWant([...set, id])
    if (addAddr.trim()) setAddrs({ ...addrs, [id]: addAddr.trim() })
    setAddId('')
    setAddAddr('')
  }
  return (
    <Panel
      title="Change membership"
      desc="Add, remove or replace members through the leader's POST /qlog/members. A new node joins as a learner and becomes a member once it holds the commit index; a removed one retires. Keep at least 3."
    >
      {done && <Notice kind="ok">{done}</Notice>}
      <div className="chips member-chips" aria-label="Member set">
        {set.map((m) => (
          <span key={m} className={`chip${current.includes(m) ? '' : ' new'}`}>
            {m}
            {m === leader.id && <span className="muted small"> leader</span>}
            <button type="button" aria-label={`Remove ${m}`} title={`Remove ${m}`} onClick={() => setWant(set.filter((x) => x !== m))}>
              ×
            </button>
          </span>
        ))}
      </div>
      <form
        className="row wrap member-add"
        onSubmit={(e) => {
          e.preventDefault()
          add()
        }}
      >
        <Field label="Node id">
          <input type="text" value={addId} onChange={(e) => setAddId(e.target.value)} list="known-nodes" placeholder="relay-d" spellCheck={false} />
        </Field>
        <datalist id="known-nodes">
          {known.filter((k) => !set.includes(k)).map((k) => (
            <option key={k} value={k} />
          ))}
        </datalist>
        <Field label="Address (if the leader can't dial it yet)">
          <input type="text" value={addAddr} onChange={(e) => setAddAddr(e.target.value)} placeholder="10.0.7.14:2981" spellCheck={false} />
        </Field>
        <button type="submit" className="btn" disabled={!addId.trim()}>
          Add
        </button>
      </form>
      {changed && (
        <div className="member-preview">
          <span className="muted">From → to:</span> <SetDiff from={current} to={set} />
        </div>
      )}
      {tooFew && changed && <Notice kind="warn">Fewer than 3 members can't survive losing one.</Notice>}
      <div className="row end">
        {changed && (
          <button type="button" className="btn quiet" onClick={() => (setWant(null), setAddrs({}))}>
            Reset
          </button>
        )}
        <button type="button" className="btn danger" disabled={!changed || tooFew} onClick={() => setOpen(true)}>
          Apply membership change
        </button>
      </div>
      <Confirm
        open={open}
        title="Change the quorum's members"
        action="Change members"
        danger
        confirmText={CONFIRM}
        busy={act.busy}
        onConfirm={() => act.run()}
        onClose={() => setOpen(false)}
      >
        <p>
          The leader pauses appends for the switch (usually under a second) and moves to a new epoch.
        </p>
        <p>
          <SetDiff from={current} to={set} />
        </p>
        <ErrorNotice error={act.error} />
      </Confirm>
    </Panel>
  )
}
