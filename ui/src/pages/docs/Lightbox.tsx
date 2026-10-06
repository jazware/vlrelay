import { useEffect, useLayoutEffect, useRef, useState, type MouseEvent, type PointerEvent as RPointerEvent } from 'react'
import { createPortal } from 'react-dom'

export type Diagram = { svg: string; caption: string; aspect: number }

type View = { s: number; x: number; y: number }
const FIT: View = { s: 1, x: 0, y: 0 }
const MAX = 8

/**
 * A docs diagram full screen: fitted to the viewport, then wheel / pinch to
 * zoom and drag to pan. Esc, the close button or a click outside the
 * diagram closes it; focus stays inside while open and goes back after.
 */
export function Lightbox({ diagram, onClose }: { diagram: Diagram; onClose: () => void }) {
  const root = useRef<HTMLDivElement>(null)
  const stage = useRef<HTMLDivElement>(null)
  const closeBtn = useRef<HTMLButtonElement>(null)
  // the box the diagram fills on screen (after any rotation)
  const [box, setBox] = useState<{ w: number; h: number } | null>(null)
  const [turned, setTurned] = useState(false)
  const boxRef = useRef(box)
  boxRef.current = box
  const [view, setView] = useState<View>(FIT)
  const viewRef = useRef(view)
  viewRef.current = view
  const pointers = useRef(new Map<number, { x: number; y: number }>())
  const gesture = useRef({ moved: false, inside: false })

  useEffect(() => {
    const opener = document.activeElement as HTMLElement | null
    const overflow = document.body.style.overflow
    document.body.style.overflow = 'hidden'
    closeBtn.current?.focus()
    return () => {
      document.body.style.overflow = overflow
      opener?.focus()
    }
  }, [])

  // The diagram's box: as large as the stage allows at its own aspect ratio.
  useLayoutEffect(() => {
    const el = stage.current
    if (!el) return
    const aspect = turned ? 1 / diagram.aspect : diagram.aspect
    const fit = () => {
      const r = el.getBoundingClientRect()
      const w = Math.min(r.width, r.height * aspect)
      setBox({ w, h: w / aspect })
      setView(FIT)
    }
    fit()
    const ro = new ResizeObserver(fit)
    ro.observe(el)
    return () => ro.disconnect()
  }, [diagram.aspect, turned])

  /** Scale by `k` keeping the stage point (px, py) still. */
  const zoomAt = (k: number, px?: number, py?: number) => {
    const el = stage.current
    if (!el) return
    const r = el.getBoundingClientRect()
    const cx = r.left + r.width / 2
    const cy = r.top + r.height / 2
    const v = viewRef.current
    const s = Math.min(MAX, Math.max(1, v.s * k))
    if (s === 1) return setView(FIT)
    const dx = (px ?? cx) - cx
    const dy = (py ?? cy) - cy
    setView(clamp({ s, x: dx - (s / v.s) * (dx - v.x), y: dy - (s / v.s) * (dy - v.y) }))
  }

  // Panned no further than keeps the zoomed diagram covering its fitted box.
  const clamp = (v: View): View => {
    const box = boxRef.current
    if (!box) return v
    const mx = ((v.s - 1) * box.w) / 2
    const my = ((v.s - 1) * box.h) / 2
    return { s: v.s, x: Math.max(-mx, Math.min(mx, v.x)), y: Math.max(-my, Math.min(my, v.y)) }
  }

  // Not React's onWheel: that listener is passive, and a trackpad pinch
  // (ctrl+wheel) would zoom the whole page.
  useEffect(() => {
    const el = stage.current
    if (!el) return
    const onWheel = (e: WheelEvent) => {
      e.preventDefault()
      zoomAt(Math.exp(-e.deltaY * (e.ctrlKey ? 0.01 : 0.002)), e.clientX, e.clientY)
    }
    el.addEventListener('wheel', onWheel, { passive: false })
    return () => el.removeEventListener('wheel', onWheel)
  })

  const onPointerDown = (e: RPointerEvent) => {
    if ((e.target as HTMLElement).closest('button')) return
    pointers.current.set(e.pointerId, { x: e.clientX, y: e.clientY })
    // with the pointer captured, the click lands on the stage: remember where it began
    if (pointers.current.size === 1) gesture.current = { moved: false, inside: !!(e.target as HTMLElement).closest('.dgl-canvas') }
    ;(e.currentTarget as HTMLElement).setPointerCapture(e.pointerId)
  }
  const onPointerMove = (e: RPointerEvent) => {
    const p = pointers.current.get(e.pointerId)
    if (!p) return
    const pts = [...pointers.current.values()]
    if (pts.length === 2) {
      const other = pts.find((q) => q !== p)!
      const d0 = Math.hypot(p.x - other.x, p.y - other.y)
      const d1 = Math.hypot(e.clientX - other.x, e.clientY - other.y)
      if (d0 > 0) zoomAt(d1 / d0, (e.clientX + other.x) / 2, (e.clientY + other.y) / 2)
      gesture.current.moved = true
    } else {
      const dx = e.clientX - p.x
      const dy = e.clientY - p.y
      if (Math.abs(dx) + Math.abs(dy) > 0) {
        if (viewRef.current.s > 1) setView((v) => clamp({ ...v, x: v.x + dx, y: v.y + dy }))
        if (Math.abs(dx) + Math.abs(dy) > 3) gesture.current.moved = true
      }
    }
    pointers.current.set(e.pointerId, { x: e.clientX, y: e.clientY })
  }
  const onPointerUp = (e: RPointerEvent) => {
    pointers.current.delete(e.pointerId)
  }

  const onStageClick = (e: MouseEvent) => {
    if (e.detail > 1 || gesture.current.moved || gesture.current.inside) return
    onClose()
  }

  // On the document: a click on the diagram leaves focus on <body>.
  useEffect(() => {
    document.addEventListener('keydown', onKeyDown)
    return () => document.removeEventListener('keydown', onKeyDown)
  })
  const onKeyDown = (e: KeyboardEvent) => {
    if (e.key === 'Escape') {
      onClose()
    } else if (e.key === 'Tab') {
      const f = [...(root.current?.querySelectorAll<HTMLElement>('button:not(:disabled)') ?? [])]
      if (!f.length) return
      const i = f.indexOf(document.activeElement as HTMLElement)
      const next = i < 0 ? 0 : e.shiftKey ? (i === 0 ? f.length - 1 : i - 1) : i === f.length - 1 ? 0 : i + 1
      e.preventDefault()
      f[next].focus()
    } else if (e.key === '+' || e.key === '=') zoomAt(1.4)
    else if (e.key === '-') zoomAt(1 / 1.4)
    else if (e.key === '0') setView(FIT)
    else if (e.key.startsWith('Arrow') && viewRef.current.s > 1) {
      const step = 60
      const [dx, dy] = { ArrowLeft: [step, 0], ArrowRight: [-step, 0], ArrowUp: [0, step], ArrowDown: [0, -step] }[e.key] ?? [0, 0]
      e.preventDefault()
      setView((v) => clamp({ ...v, x: v.x + dx, y: v.y + dy }))
    }
  }

  // A wide diagram on a phone held upright: offer to lay it along the long side.
  const canTurn = window.innerHeight > window.innerWidth && window.innerWidth < 700 && diagram.aspect > 1.6

  return createPortal(
    <div ref={root} className="dgl" role="dialog" aria-modal="true" aria-label={diagram.caption || 'Diagram'}>
      <div className="dgl-bar">
        <p className="dgl-cap">{diagram.caption}</p>
        <div className="dgl-tools">
          <button type="button" className="dgl-btn" onClick={() => zoomAt(1 / 1.4)} disabled={view.s <= 1} aria-label="Zoom out">
            −
          </button>
          <button type="button" className="dgl-btn dgl-pct" onClick={() => setView(FIT)} disabled={view.s <= 1} aria-label="Fit to screen">
            {Math.round(view.s * 100)}%
          </button>
          <button type="button" className="dgl-btn" onClick={() => zoomAt(1.4)} disabled={view.s >= MAX} aria-label="Zoom in">
            +
          </button>
          {canTurn && (
            <button type="button" className="dgl-btn" onClick={() => setTurned((t) => !t)} aria-pressed={turned} aria-label="Rotate diagram">
              <svg viewBox="0 0 20 20" width="18" height="18" aria-hidden="true">
                <path d="M15.5 9.5a5.5 5.5 0 1 1-1.8-4.1M15.5 3.5v3.6h-3.6" />
              </svg>
            </button>
          )}
          <button ref={closeBtn} type="button" className="dgl-btn dgl-close" onClick={onClose} aria-label="Close">
            <svg viewBox="0 0 20 20" width="18" height="18" aria-hidden="true">
              <path d="M5 5l10 10M15 5 5 15" />
            </svg>
          </button>
        </div>
      </div>
      <div
        ref={stage}
        className={`dgl-stage${view.s > 1 ? ' zoomed' : ''}`}
        onPointerDown={onPointerDown}
        onPointerMove={onPointerMove}
        onPointerUp={onPointerUp}
        onPointerCancel={onPointerUp}
        onClick={onStageClick}
        onDoubleClick={(e) => (viewRef.current.s > 1 ? setView(FIT) : zoomAt(2.5, e.clientX, e.clientY))}
      >
        {box && (
          <div
            className="dgl-canvas"
            style={{
              width: turned ? box.h : box.w,
              height: turned ? box.w : box.h,
              transform: `translate(${view.x}px, ${view.y}px) scale(${view.s})${turned ? ' rotate(90deg)' : ''}`,
            }}
            dangerouslySetInnerHTML={{ __html: diagram.svg }}
          />
        )}
      </div>
      <p className="dgl-hint" aria-hidden="true">
        {canTurn && !turned
          ? 'Turn your phone sideways, or rotate it here, for a bigger view'
          : matchMedia('(hover: none)').matches
            ? 'Pinch to zoom · drag to pan'
            : 'Scroll to zoom · drag to pan · Esc to close'}
      </p>
    </div>,
    document.body,
  )
}

/** The figure's diagram, from a click inside its `<figure>`. */
export function diagramOf(fig: HTMLElement): Diagram | null {
  const svg = fig.querySelector<SVGSVGElement>('svg.dg')
  if (!svg) return null
  const vb = svg.viewBox.baseVal
  const aspect = vb && vb.width > 0 && vb.height > 0 ? vb.width / vb.height : 1.6
  const caption = fig.querySelector('figcaption')?.textContent?.trim() ?? svg.getAttribute('aria-label') ?? ''
  return { svg: svg.outerHTML, caption, aspect }
}
