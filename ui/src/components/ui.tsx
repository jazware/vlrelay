import { useEffect, useRef, useState, type JSX, type ReactNode } from 'react'
import { errText } from '../lib/api'
import { setTheme, useTheme, type Theme } from '../lib/hooks'
import { Link } from '../lib/router'
import * as I from './icons'

export function Notice({ kind = 'info', children }: { kind?: 'info' | 'ok' | 'warn' | 'err'; children: ReactNode }) {
  const Icon = kind === 'ok' ? I.Check : kind === 'info' ? I.Info : I.Alert
  return (
    <div className={`notice ${kind === 'info' ? '' : kind}`} role={kind === 'err' ? 'alert' : 'status'}>
      <Icon />
      <div>{children}</div>
    </div>
  )
}

export function ErrorNotice({ error }: { error: unknown }) {
  if (!error) return null
  return <Notice kind="err">{errText(error)}</Notice>
}

/** Cluster-wide admin listings (searchAccounts, getInviteCodes) gather from
 * every node; a peer that didn't answer, or a shard nobody owned mid-move,
 * leaves the list incomplete. */
export type PartialResult = { unreachableNodes?: string[]; missingShards?: number[] }

export function partialOf(r: PartialResult): PartialResult | undefined {
  return r.unreachableNodes?.length || r.missingShards?.length ? { unreachableNodes: r.unreachableNodes, missingShards: r.missingShards } : undefined
}

export function PartialNotice({ partial }: { partial?: PartialResult }) {
  if (!partial) return null
  const nodes = partial.unreachableNodes ?? []
  const shards = partial.missingShards ?? []
  return (
    <Notice kind="warn">
      <p>
        <b>Incomplete results.</b>
        {nodes.length > 0 && <> No answer from {nodes.length === 1 ? 'node' : 'nodes'} <span className="mono">{nodes.join(', ')}</span>.</>}
        {shards.length > 0 && <> {shards.length === 1 ? 'Shard' : `${shards.length} shards`} unowned or unreachable{shards.length <= 8 && <> (<span className="mono">{shards.join(', ')}</span>)</>}.</>}
        {' '}Retry once the cluster settles.
      </p>
    </Notice>
  )
}

export function Spinner({ label = 'Loading' }: { label?: string }) {
  return <span className="spinner" role="status" aria-label={label} />
}

export function Loading() {
  return (
    <div className="empty">
      <Spinner />
    </div>
  )
}

export function Empty({ title, children }: { title: string; children?: ReactNode }) {
  return (
    <div className="empty">
      <strong>{title}</strong>
      {children}
    </div>
  )
}

export function CopyText({ text, display, mono = true }: { text: string; display?: ReactNode; mono?: boolean }) {
  const [done, setDone] = useState(false)
  const copy = async () => {
    try {
      await navigator.clipboard.writeText(text)
      setDone(true)
      setTimeout(() => setDone(false), 1400)
    } catch {
      /* clipboard blocked */
    }
  }
  return (
    <span className="copy">
      <span className={mono ? 'mono' : ''}>{display ?? text}</span>
      <button type="button" onClick={copy} aria-label={done ? 'Copied' : `Copy ${text}`} title={done ? 'Copied' : 'Copy'}>
        {done ? <I.Check /> : <I.Copy />}
      </button>
    </span>
  )
}

export function Panel({
  title,
  desc,
  actions,
  children,
  flush,
  danger,
  id,
}: {
  title?: ReactNode
  desc?: ReactNode
  actions?: ReactNode
  children: ReactNode
  flush?: boolean
  danger?: boolean
  id?: string
}) {
  return (
    <section className={`panel${danger ? ' danger' : ''}`} id={id} aria-labelledby={id ? `${id}-h` : undefined}>
      {(title || actions) && (
        <header>
          <div>
            {title && <h2 id={id ? `${id}-h` : undefined}>{title}</h2>}
            {desc && <p>{desc}</p>}
          </div>
          {actions && <div className="row">{actions}</div>}
        </header>
      )}
      <div className={`body${flush ? ' flush' : ''}`}>{children}</div>
    </section>
  )
}

/** A labelled control. `action` (usually a submit button) sits beside the input, with the hint underneath both. */
export function Field({ label, hint, action, children }: { label: string; hint?: ReactNode; action?: ReactNode; children: ReactNode }) {
  return (
    <div className="field">
      <label>
        <span className="label">{label}</span>
        {action ? (
          <span className="control">
            {children}
            {action}
          </span>
        ) : (
          children
        )}
      </label>
      {hint && <span className="hint">{hint}</span>}
    </div>
  )
}

export function Status({ kind, children }: { kind: 'ok' | 'warn' | 'bad' | 'idle'; children: ReactNode }) {
  return <span className={`status ${kind}`}>{children}</span>
}

export function PageHead({ title, desc, crumbs }: { title: ReactNode; desc?: ReactNode; crumbs?: { to: string; label: string }[] }) {
  return (
    <div className="pagehead">
      {crumbs && (
        <nav className="crumbs" aria-label="Breadcrumb">
          {crumbs.map((c, i) => (
            <span key={c.to}>
              <Link to={c.to}>{c.label}</Link>
              {i < crumbs.length - 1 && <span aria-hidden="true"> /</span>}
            </span>
          ))}
        </nav>
      )}
      <h1 className="break">{title}</h1>
      {desc && <p>{desc}</p>}
    </div>
  )
}

/** A modal confirmation. `confirmText` (if set) must be typed to enable the action. */
export function Confirm({
  open,
  title,
  children,
  action,
  danger,
  confirmText,
  busy,
  onConfirm,
  onClose,
}: {
  open: boolean
  title: string
  children?: ReactNode
  action: string
  danger?: boolean
  confirmText?: string
  busy?: boolean
  onConfirm: () => void
  onClose: () => void
}) {
  const ref = useRef<HTMLDialogElement>(null)
  const [typed, setTyped] = useState('')
  useEffect(() => {
    const d = ref.current
    if (!d) return
    if (open && !d.open) {
      setTyped('')
      d.showModal()
    } else if (!open && d.open) d.close()
  }, [open])
  const ok = !confirmText || typed.trim() === confirmText
  return (
    <dialog ref={ref} className="modal" onClose={onClose} aria-labelledby="confirm-title">
      <form
        className="inner"
        method="dialog"
        onSubmit={(e) => {
          e.preventDefault()
          if (ok) onConfirm()
        }}
      >
        <h2 id="confirm-title">{title}</h2>
        <div className="muted" style={{ marginBottom: 14 }}>
          {children}
        </div>
        {confirmText && (
          <Field label={`Type ${confirmText} to confirm`}>
            <input type="text" value={typed} onChange={(e) => setTyped(e.target.value)} autoComplete="off" spellCheck={false} />
          </Field>
        )}
        <div className="row end">
          <button type="button" className="btn" onClick={onClose}>
            Cancel
          </button>
          <button type="submit" className={`btn ${danger ? 'danger solid' : 'primary'}`} disabled={!ok || busy}>
            {busy && <Spinner />}
            {action}
          </button>
        </div>
      </form>
    </dialog>
  )
}

export function ThemeToggle() {
  const t = useTheme()
  const opts: { v: Theme; label: string; Icon: (p: any) => JSX.Element }[] = [
    { v: 'light', label: 'Light', Icon: I.Sun },
    { v: 'dark', label: 'Dark', Icon: I.Moon },
    { v: 'system', label: 'Match system', Icon: I.Auto },
  ]
  return (
    <div className="seg" role="group" aria-label="Theme">
      {opts.map(({ v, label, Icon }) => (
        <button key={v} type="button" aria-pressed={t === v} onClick={() => setTheme(v)} title={label} aria-label={label}>
          <Icon />
        </button>
      ))}
    </div>
  )
}

export function Topbar({ where, home = '/admin', children }: { where?: string; home?: string; children?: ReactNode }) {
  return (
    <>
      <header className="topbar">
        <Link to={home} className="wordmark" aria-label={home === '/' ? 'vlRelay home' : 'vlRelay console'}>
          <I.Mark />
          vlRelay
          {where && <span className="where">{where}</span>}
        </Link>
        <div className="spacer" />
        {children}
        <ThemeToggle />
      </header>
      <div className="strata" aria-hidden="true" />
    </>
  )
}

// ---------------------------------------------------------------- JSON

type LinkFn = (s: string) => string | undefined

export function JsonView({ value, linkFor }: { value: unknown; linkFor?: LinkFn }) {
  return <pre className="json">{render(value, 0, linkFor)}</pre>
}

function render(v: unknown, depth: number, linkFor?: LinkFn): ReactNode {
  const pad = '  '.repeat(depth + 1)
  const end = '  '.repeat(depth)
  if (v === null) return <span className="b">null</span>
  if (typeof v === 'boolean') return <span className="b">{String(v)}</span>
  if (typeof v === 'number') return <span className="n">{v}</span>
  if (typeof v === 'string') {
    const href = linkFor?.(v)
    const s = JSON.stringify(v)
    return href ? (
      <Link to={href} className="s l">
        {s}
      </Link>
    ) : (
      <span className="s">{s}</span>
    )
  }
  if (Array.isArray(v)) {
    if (!v.length) return '[]'
    return (
      <>
        {'[\n'}
        {v.map((x, i) => (
          <span key={i}>
            {pad}
            {render(x, depth + 1, linkFor)}
            {i < v.length - 1 ? ',\n' : '\n'}
          </span>
        ))}
        {end}]
      </>
    )
  }
  if (typeof v === 'object') {
    const entries = Object.entries(v as Record<string, unknown>)
    if (!entries.length) return '{}'
    return (
      <>
        {'{\n'}
        {entries.map(([k, x], i) => (
          <span key={k}>
            {pad}
            <span className="k">{JSON.stringify(k)}</span>: {render(x, depth + 1, linkFor)}
            {i < entries.length - 1 ? ',\n' : '\n'}
          </span>
        ))}
        {end}
        {'}'}
      </>
    )
  }
  return String(v)
}

/** Triggers a browser download of `blob` as `name`. */
export function saveBlob(blob: Blob, name: string) {
  const url = URL.createObjectURL(blob)
  const a = document.createElement('a')
  a.href = url
  a.download = name
  document.body.appendChild(a)
  a.click()
  a.remove()
  setTimeout(() => URL.revokeObjectURL(url), 10_000)
}
