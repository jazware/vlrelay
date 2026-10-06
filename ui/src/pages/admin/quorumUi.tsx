import { useState, type ReactNode } from 'react'
import { closeDialog, openDialog } from '../../components/console/dialogs'
import { Chip, Spinner } from '../../components/console/kit'
import { toast } from '../../components/console/toast'
import { errText, type QRecovery, type QStatus, type QSwitch, type QuorumView, type SettingsView } from '../../lib/api'
import * as A from '../../lib/console/adminAdapter'
import { quorumPoll, type SeenEpoch } from '../../lib/console/polls'
import type { RelayView } from '../../lib/console/relay'

// What the Quorum page, the node and epoch details and the membership dialog share: each
// member's row, the epoch changes the statuses and the console's own polls know about, and the
// membership editor.

export type MemberKind = 'leader' | 'follower' | 'candidate' | 'learner' | 'retired' | 'unknown' | 'down'
export type MemberRow = { id: string; addr: string; stale: boolean; error: string | null; reportedMs: number; s: QStatus | null; kind: MemberKind; color?: string }

const ORDER: Record<MemberKind, number> = { leader: 0, candidate: 1, follower: 2, learner: 3, down: 4, unknown: 5, retired: 6 }

/** The status the others are read against: the leader's, else any member's that answered. */
export function refStatus(q: QuorumView): QStatus | undefined {
  const live = q.nodes.filter((n) => !n.stale && n.status)
  return (live.find((n) => n.status!.role === 'leader') ?? live[0])?.status ?? undefined
}

export function memberRows(q: QuorumView, view?: RelayView): MemberRow[] {
  const ref = refStatus(q)
  const members = ref?.members ?? []
  const learners = ref?.learners ?? []
  const rows = q.nodes.map((n): MemberRow => {
    const s = n.stale ? null : n.status
    const kind: MemberKind = n.stale
      ? 'down'
      : s?.role === 'leader'
        ? 'leader'
        : s?.retired
          ? 'retired'
          : learners.includes(n.node)
            ? 'learner'
            : s?.role === 'candidate'
              ? 'candidate'
              : members.includes(n.node)
                ? 'follower'
                : 'unknown'
    return { id: n.node, addr: n.addr, stale: n.stale, error: n.error, reportedMs: n.reportedMs, s, kind, color: view?.byId.get(n.node)?.color }
  })
  return rows.sort((a, b) => ORDER[a.kind] - ORDER[b.kind] || a.id.localeCompare(b.id))
}

export function RoleChip({ kind }: { kind: MemberKind }) {
  switch (kind) {
    case 'leader':
      return (
        <span className="cx-chip sig">
          <span className="cx-g">★</span>leader
        </span>
      )
    case 'down':
      return <Chip k="err">no answer</Chip>
    case 'learner':
      return <Chip k="info">learner</Chip>
    case 'candidate':
      return <Chip k="warn">candidate</Chip>
    case 'retired':
      return <Chip k="idle">retired</Chip>
    case 'unknown':
      return <Chip k="idle">not a member</Chip>
    default:
      return (
        <Chip k="plain" glyph={false}>
          follower
        </Chip>
      )
  }
}

// ---------------------------------------------------------------- epoch changes

export type EpochKind = 'switch' | 'recovery' | 'seen'
export type EpochEvent = { id: string; epoch: number; fromEpoch?: number; atMs?: number; kind: EpochKind; leader: string | null; pausedMs?: number; sw?: QSwitch; rec?: QRecovery; seen?: SeenEpoch }

/**
 * Membership changes (`switches`, from whichever leader ran them) and bucket recoveries
 * (`recovered`) as the statuses list them, plus the epoch changes the console saw while open
 * that neither explains (a takeover or a handoff: the status doesn't say which). Newest first.
 */
export function epochEvents(q: QuorumView | undefined, seen: SeenEpoch[]): EpochEvent[] {
  const out = new Map<string, EpochEvent>()
  for (const n of q?.nodes ?? []) {
    const s = n.status
    if (!s || n.stale) continue
    for (const w of s.switches ?? []) {
      const id = `e${w.epoch}`
      if (!out.has(id)) out.set(id, { id, epoch: w.epoch, fromEpoch: w.from_epoch, atMs: w.at_ms, kind: 'switch', leader: w.leader, pausedMs: w.paused_ms, sw: w })
    }
    for (const r of s.recovered ?? []) {
      const id = `g${r.generation}`
      if (!out.has(id)) out.set(id, { id, epoch: r.epoch, kind: 'recovery', leader: s.id, pausedMs: r.total_ms, rec: r })
    }
  }
  const known = new Set([...out.values()].map((e) => e.epoch))
  for (const e of seen) {
    if (known.has(e.epoch)) {
      // a recovery's time is when the console saw its epoch
      const r = [...out.values()].find((x) => x.kind === 'recovery' && x.epoch === e.epoch && x.atMs === undefined)
      if (r) r.atMs = e.atMs
      continue
    }
    out.set(`e${e.epoch}`, { id: `e${e.epoch}`, epoch: e.epoch, fromEpoch: e.from, atMs: e.atMs, kind: 'seen', leader: e.leader, seen: e })
  }
  return [...out.values()].sort((a, b) => b.epoch - a.epoch || (b.atMs ?? 0) - (a.atMs ?? 0))
}

export const EPOCH_GLYPH: Record<EpochKind, ReactNode> = {
  switch: <span className="s-acc">◆</span>,
  recovery: <span className="s-err">■</span>,
  seen: <span className="s-warn">▲</span>,
}

export function EpochChip({ kind }: { kind: EpochKind }) {
  if (kind === 'switch') return <Chip k="acc">membership</Chip>
  if (kind === 'recovery') return <Chip k="err">bucket recovery</Chip>
  return <Chip k="warn">new epoch</Chip>
}

/** Members before and after: kept plain, added dashed green, removed struck red. */
export function SetDiff({ from, to, leader }: { from: string[]; to: string[]; leader?: string | null }) {
  const all = [...new Set([...from, ...to])].sort()
  return (
    <span className="cx-memchips">
      {all.map((m) => {
        const add = !from.includes(m)
        const rm = !to.includes(m)
        return (
          <span key={m} className={`cx-memchip${add ? ' add' : rm ? ' rm' : ''}`}>
            {add ? '+' : rm ? '−' : ''}
            {m}
            {m === leader && <span className="muted"> leader</span>}
          </span>
        )
      })}
    </span>
  )
}

// ---------------------------------------------------------------- membership

const WORD = 'change members'

/** Membership changes need the nodes started with --qlog-admin-token; false only when settings say it's unset. */
export function membershipOn(s?: SettingsView): boolean {
  const e = s?.entries.find((x) => x.flag === '--qlog-admin-token')
  return !e || e.set
}

export function membersDialog(o: { current: string[]; leader: string | null; known: string[]; remove?: string; on: boolean }) {
  openDialog(() => <MembersForm {...o} />)
}

function MembersForm({ current, leader, known, remove, on }: { current: string[]; leader: string | null; known: string[]; remove?: string; on: boolean }) {
  const [set, setSet] = useState<string[]>(() => current.filter((m) => m !== remove))
  const [addrs, setAddrs] = useState<Record<string, string>>({})
  const [id, setId] = useState('')
  const [addr, setAddr] = useState('')
  const [word, setWord] = useState('')
  const [busy, setBusy] = useState(false)
  const [error, setError] = useState<unknown>()
  const add = set.filter((m) => !current.includes(m))
  const rm = current.filter((m) => !set.includes(m))
  const changed = add.length + rm.length > 0
  const req: A.MembersChange = { members: [...set].sort(), addrs: Object.fromEntries(Object.entries(addrs).filter(([k]) => set.includes(k))) }
  const warn: string[] = []
  if (set.length < 3) warn.push('Keep at least 3 members: with 2, one failure holds the firehose.')
  else if (set.length % 2 === 0) warn.push('An even member count adds no failure tolerance over one fewer.')
  if (leader && rm.includes(leader)) warn.push(`${leader} leads: it hands the new epoch to a remaining member instead of stopping.`)
  if (add.length) warn.push(`${add.join(', ')} ${add.length === 1 ? 'joins' : 'join'} as a learner, copies the log and becomes a member once it holds the commit index.`)
  const can = on && changed && set.length > 0 && word.trim() === WORD && !busy
  const toggle = (m: string) => setSet((s) => (s.includes(m) ? s.filter((x) => x !== m) : [...s, m]))
  const addOne = () => {
    const v = id.trim()
    if (!v || set.includes(v)) return
    setSet((s) => [...s, v])
    if (addr.trim()) setAddrs((a) => ({ ...a, [v]: addr.trim() }))
    setId('')
    setAddr('')
  }
  return (
    <form
      className="cx-dlg"
      role="alertdialog"
      aria-modal="true"
      aria-labelledby="cx-dlg-t"
      onSubmit={async (e) => {
        e.preventDefault()
        if (!can) return
        setBusy(true)
        setError(undefined)
        try {
          await A.changeMembers(req)
          toast(`Members now ${req.members.join(', ')}`)
          quorumPoll.refresh()
          closeDialog()
        } catch (err) {
          setError(err)
        } finally {
          setBusy(false)
        }
      }}
    >
      <div className="dh">
        <div className="ico warn" aria-hidden="true">
          ▲
        </div>
        <h2 id="cx-dlg-t">Change the quorum's members</h2>
      </div>
      <div className="db">
        <div className="cx-memchips" aria-label="Member set">
          {[...new Set([...current, ...set])].map((m) => {
            const isAdd = add.includes(m)
            const isRm = rm.includes(m)
            return (
              <span key={m} className={`cx-memchip${isAdd ? ' add' : isRm ? ' rm' : ''}`}>
                {isAdd ? '+' : isRm ? '−' : ''}
                {m}
                {m === leader && <span className="muted"> leader</span>}
                <button type="button" aria-label={`${isRm ? 'Keep' : 'Remove'} ${m}`} title={isRm ? 'keep' : 'remove'} onClick={() => toggle(m)}>
                  {isRm ? '↺' : '×'}
                </button>
              </span>
            )
          })}
        </div>
        <div className="cx-form-row">
          <input className="cx-inp mono" list="cx-known-nodes" placeholder="node id" aria-label="Node id to add" value={id} onChange={(e) => setId(e.target.value)} spellCheck={false} autoComplete="off"
            onKeyDown={(e) => {
              if (e.key === 'Enter') {
                e.preventDefault()
                addOne()
              }
            }}
          />
          <input className="cx-inp mono" placeholder="host:port, if the leader can't dial it yet" aria-label="Its peer address" value={addr} onChange={(e) => setAddr(e.target.value)} spellCheck={false} autoComplete="off" />
          <button type="button" className="cx-btn" disabled={!id.trim() || set.includes(id.trim())} onClick={addOne}>
            Add
          </button>
          <datalist id="cx-known-nodes">
            {known.filter((k) => !set.includes(k)).map((k) => (
              <option key={k} value={k} />
            ))}
          </datalist>
        </div>
        {changed && warn.length > 0 && (
          <ul className="cx-warnlist">
            {warn.map((w) => (
              <li key={w}>{w}</li>
            ))}
          </ul>
        )}
        <p className="muted sm" style={{ margin: 0 }}>
          The leader drains, flushes and CASes <span className="mono">qlog/leader</span> to the next epoch with the new set. Appends pause for the drain, flush and CAS.
        </p>
        {!on && (
          <div className="dlg-err" role="alert">
            <span className="cx-g">■</span>
            <span>Membership changes are off: the nodes run without --qlog-admin-token.</span>
          </div>
        )}
        <div>
          <label className="cx-lbl" htmlFor="cf_word">
            Type{' '}
            <b className="mono" style={{ color: 'var(--ink)' }}>
              {WORD}
            </b>{' '}
            to confirm
          </label>
          <input id="cf_word" className="cx-inp mono" autoComplete="off" spellCheck={false} value={word} onChange={(e) => setWord(e.target.value)} disabled={!on} />
        </div>
        {!!error && (
          <div className="dlg-err" role="alert">
            <span className="cx-g">■</span>
            <span>{errText(error)}</span>
          </div>
        )}
      </div>
      <div className="df">
        <span className="call" title={A.changeMembersCall(req)}>
          {A.changeMembersCall(req)}
        </span>
        <button type="button" className="cx-btn" onClick={closeDialog}>
          Cancel
        </button>
        <button type="submit" className="cx-btn solid-danger" disabled={!can}>
          {busy && <Spinner />}
          Change members
        </button>
      </div>
    </form>
  )
}
