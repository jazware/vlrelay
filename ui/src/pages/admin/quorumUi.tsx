import { useState, type ReactNode } from 'react'
import { closeDialog, openDialog } from '../../components/console/dialogs'
import { Chip, Spinner, type ChipKind } from '../../components/console/kit'
import { toast } from '../../components/console/toast'
import { errText, type QDurability, type QRecovery, type QStatus, type QSwitch, type QuorumEvent, type QuorumView, type SettingsView } from '../../lib/api'
import * as A from '../../lib/console/adminAdapter'
import { ago, fmtBytes, fmtMs, fmtNum, seqS } from '../../lib/console/fmt'
import type { SeenEpoch } from '../../lib/console/queries'
import * as W from '../../lib/console/writes'
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

/** When an entry counts on a member: after its fdatasync, once in the page cache (synced in the background), or in memory only. */
export function Durability({ d, long }: { d?: QDurability; long?: boolean }) {
  if (!d) return <span className="muted">—</span>
  if (d.mode === 'fsync') return <Chip k="ok" title="An entry counts once it's fdatasync'd">fsync</Chip>
  if (d.mode === 'memory') return <Chip k="warn" title="No commitlog: a restart loses what only this node held">memory</Chip>
  return (
    <span className="nowrap" title={`Acked once written; fdatasync'd every ${d.sync_ms ?? '?'} ms in the background (${fmtNum(d.background_syncs)} so far). Unsynced bytes are what a power cut on a majority could lose.`}>
      <Chip k="info">page cache</Chip>{' '}
      <span className="mono sm t2">
        {fmtBytes(d.unsynced_bytes)} unsynced{d.since_sync_ms != null ? ` · ${fmtMs(d.since_sync_ms)}${long ? ' since the last sync' : ''}` : ''}
      </span>
    </span>
  )
}

// ---------------------------------------------------------------- epoch changes

export type EpochKind = 'takeover' | 'handoff' | 'switch' | 'recovery' | 'stepdown' | 'seen'
export type EpochEvent = {
  id: string
  epoch: number
  fromEpoch?: number
  atMs?: number
  kind: EpochKind
  /** Who leads after it (null: nobody, after a step-down nothing followed). */
  leader: string | null
  /** Appends paused (a membership change), the recovery took, or no member led (a step-down to the next lead). */
  pausedMs?: number
  sw?: QSwitch
  rec?: QRecovery
  /** The new leader's own record of taking over (`status.history`). */
  lead?: QuorumEvent
  /** The old leader stepping down before it, when it lived to record it. */
  down?: QuorumEvent
  seen?: SeenEpoch
}

/**
 * The quorum's leadership changes, newest first: each member's own history (takeovers by
 * election, handoffs, recoveries, membership changes and step-downs, with their times), joined
 * with the membership changes (`switches`) and bucket recoveries (`recovered`) the statuses
 * detail, plus any epoch change the console saw that no member's history explains (its leader
 * is gone, or the change is older than the 64 a member keeps).
 */
export function epochEvents(q: QuorumView | undefined, history: QuorumEvent[], seen: SeenEpoch[]): EpochEvent[] {
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
  const leads: EpochEvent[] = []
  for (const h of history) {
    if (h.kind !== 'lead') continue
    const why = h.why.toLowerCase()
    let e: EpochEvent | undefined
    if (why.startsWith('recovery')) e = [...out.values()].find((x) => x.kind === 'recovery' && x.epoch === h.epoch)
    else e = out.get(`e${h.epoch}`)
    if (e) {
      e.lead = h
      e.atMs ??= h.atMs
    } else {
      const kind: EpochKind = why.startsWith('membership') ? 'switch' : why.startsWith('recovery') ? 'recovery' : why.startsWith('handoff') ? 'handoff' : 'takeover'
      e = { id: `e${h.epoch}`, epoch: h.epoch, atMs: h.atMs, kind, leader: h.node, lead: h }
      out.set(e.id, e)
    }
    leads.push(e)
  }
  leads.sort((a, b) => a.epoch - b.epoch || a.atMs! - b.atMs!)
  // a step-down belongs to the next epoch's lead (a partitioned leader may only notice after it);
  // one no later epoch followed leaves the quorum leaderless
  for (const h of history) {
    if (h.kind !== 'step_down') continue
    const next = leads.find((e) => e.epoch > h.epoch)
    if (next) {
      if (!next.down || next.down.atMs < h.atMs) next.down = h
      if ((next.kind === 'takeover' || next.kind === 'handoff') && next.atMs! >= h.atMs) next.pausedMs = next.atMs! - h.atMs
    } else {
      const id = `d${h.node}-${h.epoch}-${h.atMs}`
      out.set(id, { id, epoch: h.epoch, atMs: h.atMs, kind: 'stepdown', leader: null, down: h })
    }
  }
  const known = new Set([...out.values()].map((e) => e.epoch))
  for (const e of seen) {
    if (known.has(e.epoch)) {
      // a recovery no history dates is dated when the console saw its epoch
      const r = [...out.values()].find((x) => x.kind === 'recovery' && x.epoch === e.epoch && x.atMs === undefined)
      if (r) r.atMs = e.atMs
      continue
    }
    out.set(`e${e.epoch}`, { id: `e${e.epoch}`, epoch: e.epoch, fromEpoch: e.from, atMs: e.atMs, kind: 'seen', leader: e.leader, seen: e })
  }
  const all = [...out.values()]
  const epochs = [...new Set(all.map((e) => e.epoch))].sort((a, b) => a - b)
  for (const e of all) if (e.fromEpoch === undefined && e.kind !== 'stepdown') e.fromEpoch = epochs[epochs.indexOf(e.epoch) - 1]
  return all.sort((a, b) => b.epoch - a.epoch || (b.atMs ?? 0) - (a.atMs ?? 0))
}

export const EPOCH_GLYPH: Record<EpochKind, ReactNode> = {
  takeover: <span className="s-warn">▲</span>,
  handoff: <span className="s-ok">●</span>,
  switch: <span className="s-acc">◆</span>,
  recovery: <span className="s-err">■</span>,
  stepdown: <span className="s-idle">○</span>,
  seen: <span className="s-warn">△</span>,
}

const EPOCH_CHIP: Record<EpochKind, [ChipKind, string]> = {
  takeover: ['warn', 'takeover'],
  handoff: ['ok', 'handoff'],
  switch: ['acc', 'membership'],
  recovery: ['err', 'bucket recovery'],
  stepdown: ['idle', 'step-down'],
  seen: ['warn', 'new epoch'],
}

export function EpochChip({ kind }: { kind: EpochKind }) {
  const [k, label] = EPOCH_CHIP[kind]
  return <Chip k={k}>{label}</Chip>
}

/** One line on what happened. */
export function epochDetail(e: EpochEvent): string {
  const down = e.down ? `${e.down.node} stepped down (${e.down.why})` : ''
  switch (e.kind) {
    case 'switch':
      return e.sw ? `${e.sw.from.join(', ')} → ${e.sw.to.join(', ')}` : (e.lead?.why ?? 'membership change')
    case 'recovery':
      return e.rec ? `generation ${e.rec.generation}: resumed after ${seqS(e.rec.after)}, ${fmtNum(e.rec.salvaged)} salvaged` : `${e.leader} recovered from the bucket`
    case 'handoff':
      return `${e.lead?.from ?? e.down?.node ?? 'the leader'} handed off to ${e.leader}`
    case 'takeover':
      return [
        `${e.leader} elected${e.lead?.from ? ` after ${e.lead.from}` : ''}`,
        e.down && e.lead && e.down.atMs > e.lead.atMs ? `${e.down.node} stepped down ${fmtMs(e.down.atMs - e.lead.atMs)} later (${e.down.why})` : down || (e.lead?.from ? `${e.lead.from} stopped answering` : ''),
      ]
        .filter(Boolean)
        .join(' · ')
    case 'stepdown':
      return `${down}: no member has led since`
    default:
      return "seen by this console: no member's history lists it"
  }
}

/** How long a leader change stays news: the Overview banner and ⌘K's "Needs attention". */
export const LEADER_NEWS_MS = 30 * 60_000

/** The epoch the current leader took over in (the newest change that isn't a lone step-down), when it's dated. */
export function currentLead(events: EpochEvent[]): EpochEvent | undefined {
  const e = events.find((x) => x.kind !== 'stepdown')
  return e?.atMs !== undefined ? e : undefined
}

/** The newest leader change, when it happened within LEADER_NEWS_MS. */
export function recentLeaderChange(events: EpochEvent[], now = Date.now()): EpochEvent | undefined {
  const e = currentLead(events)
  return e && now - e.atMs! < LEADER_NEWS_MS ? e : undefined
}

/** A leader change in one line and its detail: "relay-a took over from relay-b 4m ago". */
export function leaderChangeText(e: EpochEvent): { title: string; desc: string } {
  const from = e.lead?.from ?? e.down?.node
  const when = e.atMs ? ago(e.atMs) : ''
  const who = e.leader ?? 'no member'
  const title =
    e.kind === 'takeover'
      ? from && from !== e.leader
        ? `Leader changed ${when}: ${who} took over from ${from}`
        : `${who} took the lead ${when}`
      : e.kind === 'handoff'
        ? `Leader changed ${when}: ${from ?? 'the leader'} handed off to ${who}`
        : e.kind === 'switch'
          ? `Members changed ${when}: ${who} leads the new set`
          : e.kind === 'recovery'
            ? `${who} recovered the log from the bucket ${when}`
            : `Epoch changed ${when}: ${who} leads`
  const parts = [
    e.kind === 'switch' || e.kind === 'recovery' ? epochDetail(e) : e.down ? `${e.down.node} stepped down (${e.down.why})` : '',
    e.pausedMs !== undefined ? `${e.kind === 'recovery' ? 'took' : 'appends paused'} ${fmtMs(e.pausedMs)}` : '',
    e.fromEpoch !== undefined ? `epoch ${e.fromEpoch} → ${e.epoch}` : `epoch ${e.epoch}`,
  ]
  return { title, desc: parts.filter(Boolean).join(' · ') }
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
          await W.changeMembers(req)
          toast(`Members now ${req.members.join(', ')}`)
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
