import { useEffect, useRef, useState, type ReactNode } from 'react'
import type { HostStatus, PolicyAudit, RejectReason, Severity } from '../lib/api'
import { fmtTime, relTime } from '../lib/format'
import { Spinner } from './ui'

// ---------------------------------------------------------------- pills

const STATUS_LABEL: Record<HostStatus, string> = {
  connected: 'connected',
  idle: 'idle',
  backoff: 'backoff',
  offline: 'offline',
  throttled: 'throttled',
  suspended: 'suspended',
  banned: 'banned',
}

export const HOST_STATUSES: HostStatus[] = ['connected', 'idle', 'throttled', 'backoff', 'offline', 'suspended', 'banned']

export function StatusPill({ status }: { status: HostStatus }) {
  return <span className={`sp sp-${status}`}>{STATUS_LABEL[status]}</span>
}

export function TierPill({ tier }: { tier: string }) {
  const cls = tier === 'trusted' ? 'tp-trusted' : tier === 'probation' ? 'tp-probation' : tier === 'standard' ? 'tp-standard' : 'tp-other'
  return <span className={`tp ${cls}`}>{tier}</span>
}

export function SeverityPill({ severity }: { severity: Severity }) {
  return <span className={`sev sev-${severity}`}>{severity}</span>
}

export const REASON_LABEL: Record<RejectReason, string> = {
  'bad-signature': 'bad signature',
  'invalid-commit': 'invalid commit',
  'rev-out-of-order': 'rev out of order',
  'prev-data-mismatch': 'prevData mismatch',
  'wrong-host': 'wrong host',
  'unknown-did': 'unknown DID',
  'too-large': 'too large',
  'rate-limited': 'rate-limited',
  takendown: 'taken down',
  malformed: 'malformed frame',
}

/** Reason → color slot, fixed so a reason keeps its color on every page. */
export const REASON_COLOR: Record<RejectReason, string> = {
  'bad-signature': 'c5',
  'invalid-commit': 'c4',
  'rev-out-of-order': 'c3',
  'prev-data-mismatch': 'c6',
  'wrong-host': 'c2',
  'unknown-did': 'c1',
  'too-large': 'ink3',
  'rate-limited': 'amber',
  takendown: 'danger',
  malformed: 'ink2',
}

// ---------------------------------------------------------------- numbers

export function Tile({ k, v, sub, tone, big }: { k: string; v: ReactNode; sub?: ReactNode; tone?: 'warn' | 'bad' | 'ok'; big?: boolean }) {
  return (
    <div className={`tile${tone ? ` tone-${tone}` : ''}${big ? ' big' : ''}`}>
      <div className="v">{v}</div>
      <div className="k">{k}</div>
      {sub && <div className="tsub">{sub}</div>}
    </div>
  )
}

/** A tiny line, no axes. `max` shares a scale across rows. */
export function Sparkline({ values, color = 'accent', width = 96, height = 22, max, fill }: { values: number[]; color?: string; width?: number; height?: number; max?: number; fill?: boolean }) {
  if (values.length < 2) return <svg className="spark" width={width} height={height} aria-hidden="true" />
  const m = max ?? Math.max(...values, 1e-9)
  const step = width / (values.length - 1)
  const y = (v: number) => height - 1 - (Math.min(v, m) / m) * (height - 2)
  const pts = values.map((v, i) => `${(i * step).toFixed(1)},${y(v).toFixed(1)}`).join(' ')
  return (
    <svg className="spark" width={width} height={height} viewBox={`0 0 ${width} ${height}`} aria-hidden="true">
      {fill && <polygon points={`0,${height} ${pts} ${width},${height}`} style={{ fill: `var(--${color})`, opacity: 0.14 }} />}
      <polyline points={pts} style={{ stroke: `var(--${color})` }} />
    </svg>
  )
}

/** A horizontal bar for "share of the max" cells. */
export function Bar({ frac, color = 'accent' }: { frac: number; color?: string }) {
  return (
    <span className="bar" aria-hidden="true">
      <i style={{ width: `${Math.max(0, Math.min(1, frac)) * 100}%`, background: `var(--${color})` }} />
    </span>
  )
}

// ---------------------------------------------------------------- in-page confirm

/**
 * Confirmation that opens in place (no modal, no browser dialog): a strip
 * under the button that triggered it. Enter confirms, Escape cancels.
 * `reason`: ask for a reason, required before confirming.
 */
export function InlineConfirm({
  open,
  children,
  action,
  danger,
  reason,
  busy,
  error,
  onConfirm,
  onCancel,
}: {
  open: boolean
  children: ReactNode
  action: string
  danger?: boolean
  reason?: string
  busy?: boolean
  error?: ReactNode
  onConfirm: (reason: string) => void
  onCancel: () => void
}) {
  const [text, setText] = useState('')
  const ref = useRef<HTMLInputElement>(null)
  const btn = useRef<HTMLButtonElement>(null)
  useEffect(() => {
    if (!open) return
    setText('')
    // focus lands in the strip so Enter/Escape work without reaching for the mouse
    setTimeout(() => (ref.current ?? btn.current)?.focus(), 0)
  }, [open])
  if (!open) return null
  const ok = !reason || text.trim().length > 0
  return (
    <form
      className={`confirm${danger ? ' danger' : ''}`}
      role="alertdialog"
      aria-live="polite"
      onSubmit={(e) => {
        e.preventDefault()
        if (ok && !busy) onConfirm(text.trim())
      }}
      onKeyDown={(e) => {
        if (e.key === 'Escape') {
          e.stopPropagation()
          onCancel()
        }
      }}
    >
      <div className="confirm-msg">{children}</div>
      {reason && <input ref={ref} type="text" placeholder={reason} value={text} onChange={(e) => setText(e.target.value)} aria-label={reason} />}
      <div className="row">
        <button type="button" className="btn sm" onClick={onCancel}>
          Cancel <kbd>Esc</kbd>
        </button>
        <button ref={btn} type="submit" className={`btn sm ${danger ? 'danger solid' : 'primary'}`} disabled={!ok || busy}>
          {busy && <Spinner />}
          {action} <kbd>↵</kbd>
        </button>
      </div>
      {error && <div className="confirm-err">{error}</div>}
    </form>
  )
}

/** "Updated 2 s ago" next to a page title. */
export function Live({ at, error, every }: { at?: number; error?: unknown; every: number }) {
  const [, force] = useState(0)
  useEffect(() => {
    const id = setInterval(() => force((x) => x + 1), 1000)
    return () => clearInterval(id)
  }, [])
  const age = at ? Date.now() - at : Infinity
  const stale = !!error || age > every * 3
  return (
    <span className={`live${stale ? ' stale' : ''}`}>
      <i aria-hidden="true" />
      {stale ? 'Not updating' : `Live, every ${every / 1000} s`}
    </span>
  )
}

/** A versioned object's audit log (policy, domain rules), newest first. */
export function AuditTable({ rows }: { rows: PolicyAudit[] }) {
  return (
    <div className="table-wrap">
      <table className="data audit">
        <thead>
          <tr>
            <th>Version</th>
            <th>When</th>
            <th>By</th>
            <th>Note</th>
            <th>Changes</th>
          </tr>
        </thead>
        <tbody>
          {rows.map((a) => (
            <tr key={a.version}>
              <td className="mono">v{a.version}</td>
              <td className="nowrap" title={fmtTime(a.atMs)}>
                {relTime(a.atMs)}
              </td>
              <td>{a.by}</td>
              <td className="wrap-cell">{a.note || <span className="muted">—</span>}</td>
              <td>
                <ul className="changes">
                  {a.changes.map((c, i) => (
                    <li key={i} className="mono">
                      {c}
                    </li>
                  ))}
                </ul>
              </td>
            </tr>
          ))}
        </tbody>
      </table>
    </div>
  )
}

/** Kbd hint. */
export const K = ({ children }: { children: ReactNode }) => <kbd>{children}</kbd>
