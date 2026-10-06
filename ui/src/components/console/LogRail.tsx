import { useEffect, useRef } from 'react'
import { fmtSi } from '../../lib/console/fmt'
import { getLive } from '../../lib/console/live'

// The log rail: one track per member around F and the commit index. Left of F (dotted cobalt)
// is in the bucket; F to the commit is committed on a majority but only on the members' disks;
// the right-hand zoom is the last few dozen entries, where appended entries wait for acks (the
// outlined box) and ▲ is what the member has emitted. R's headroom is how far commit may run
// before the leader must flush. The commit moves with the stream between polls so the rail reads
// as live; every number it lands on is a status's.

export type RailRow = { id: string; color?: string; dead: boolean; learner: boolean; leader: boolean; last: number; commit: number; emitted: number }
export type RailData = { rows: RailRow[]; flushed: number; reserve: number; commit: number; held: boolean; rate: number; at: number }

const tokenOf = (color?: string) => color?.match(/--([a-z0-9-]+)/)?.[1] ?? 'c1'

function hatch(g: CanvasRenderingContext2D, x: number, y: number, w: number, h: number, color: string) {
  g.save()
  g.beginPath()
  g.rect(x, y, w, h)
  g.clip()
  g.strokeStyle = color
  g.globalAlpha = 0.5
  g.lineWidth = 1
  for (let i = -h; i < w; i += 5) {
    g.beginPath()
    g.moveTo(x + i, y + h)
    g.lineTo(x + i + h, y)
    g.stroke()
  }
  g.restore()
}

const seqS = (s: number) => Math.floor(s).toLocaleString('en-US')

export function LogRail({ data }: { data: RailData }) {
  const ref = useRef<HTMLCanvasElement>(null)
  const d = useRef(data)
  d.current = data
  useEffect(() => {
    const c = ref.current
    if (!c) return
    const col: Record<string, string> = {}
    let colAt = 0
    let raf = 0
    let last = 0
    // the window eases between sizes instead of jumping with each flush
    let span = 0
    let shownF = data.flushed
    let fFrom = data.flushed
    let fTo = data.flushed
    let fAt = 0
    const reduced = matchMedia('(prefers-reduced-motion: reduce)')
    const draw = (ts: number) => {
      raf = requestAnimationFrame(draw)
      if (last && ts - last < 50) return
      last = ts
      if (ts - colAt > 1000 || !colAt) {
        const cs = getComputedStyle(c)
        for (const k of ['ink', 'ink2', 'ink3', 'rule', 'rule2', 'signal', 'accent', 'err', 'idle', 'c1', 'c2', 'c3', 'c4', 'c5', 'c6']) col[k] = cs.getPropertyValue(`--${k}`).trim()
        colAt = ts
      }
      const D = d.current
      const RH = 34
      const top = 30
      const H = top + Math.max(1, D.rows.length) * RH + 6
      if (c.style.height !== `${H}px`) c.style.height = `${H}px`
      const W = c.clientWidth
      if (!W) return
      const dpr = window.devicePixelRatio || 1
      if (c.width !== Math.round(W * dpr) || c.height !== Math.round(H * dpr)) {
        c.width = Math.round(W * dpr)
        c.height = Math.round(H * dpr)
      }
      const g = c.getContext('2d')!
      g.setTransform(dpr, 0, 0, dpr, 0, 0)
      g.clearRect(0, 0, W, H)
      const narrow = W < 620
      const live = getLive()
      const moving = !D.held && !live.paused && !live.stale && !reduced.matches
      const ahead = moving ? Math.min(2.5, Math.max(0, (Date.now() - D.at) / 1000)) * D.rate : 0
      const commitNow = Math.min(D.reserve || Infinity, D.commit + ahead)

      if (D.flushed !== fTo) {
        fFrom = shownF
        fTo = D.flushed
        fAt = ts
      }
      const fe = fAt ? Math.min(1, (ts - fAt) / 700) : 1
      shownF = fFrom + (fTo - fFrom) * (1 - (1 - fe) ** 3)

      const want = Math.max(40, (commitNow - shownF) * 1.25, D.rate * 4)
      span = span ? span + (want - span) * 0.08 : want
      const lo = shownF - span * 0.22
      const hi = shownF + span
      const lw = narrow ? 58 : 128
      const zw = narrow ? 64 : 150
      const rt = narrow ? 0 : 96
      const x0 = lw
      const x1 = W - zw - rt - 14
      const z0 = x1 + 10
      const z1 = W - rt - 4
      const X = (s: number) => x0 + ((Math.min(hi, Math.max(lo, s)) - lo) / (hi - lo || 1)) * (x1 - x0)
      const zlo = Math.floor(commitNow) - 40
      const zhi = Math.floor(commitNow) + 40
      const Z = (s: number) => z0 + ((Math.min(zhi, Math.max(zlo, s)) - zlo) / (zhi - zlo)) * (z1 - z0)

      g.font = '500 10px "IBM Plex Mono", monospace'
      g.textBaseline = 'middle'
      const fx = X(shownF)
      g.strokeStyle = col.accent
      g.lineWidth = 1
      g.setLineDash([2, 2])
      g.beginPath()
      g.moveTo(fx + 0.5, top - 6)
      g.lineTo(fx + 0.5, H - 4)
      g.stroke()
      g.setLineDash([])
      g.fillStyle = col.accent
      g.textAlign = 'center'
      g.fillText(narrow ? 'F' : `F ${seqS(D.flushed)}`, Math.max(x0 + (narrow ? 6 : 50), Math.min(x1 - 50, fx)), 9)
      const cx = X(commitNow)
      g.strokeStyle = D.held ? col.err : col.signal
      g.beginPath()
      g.moveTo(cx + 0.5, top - 6)
      g.lineTo(cx + 0.5, H - 4)
      g.stroke()
      g.fillStyle = D.held ? col.err : col.signal
      g.textAlign = cx > x1 - 70 ? 'right' : 'left'
      g.fillText(D.held ? ' commit held ' : ' commit ', cx, 21)
      g.textAlign = 'center'
      g.fillStyle = col.ink3
      g.fillText(narrow ? '±40' : '±40 entries', (z0 + z1) / 2, 9)
      if (!narrow) {
        g.textAlign = 'left'
        g.fillStyle = col.ink2
        g.fillText(D.reserve ? `R +${fmtSi(Math.max(0, D.reserve - commitNow))}` : 'R —', z1 + 10, 9)
        g.fillStyle = col.ink3
        g.fillText('headroom', z1 + 10, 21)
      }

      D.rows.forEach((r, i) => {
        const y = top + i * RH
        const color = col[tokenOf(r.color)] || col.c1
        g.textAlign = 'left'
        g.fillStyle = color
        g.fillRect(0, y + 4, 8, 8)
        g.fillStyle = r.dead ? col.err : col.ink
        g.font = '600 11px "IBM Plex Mono", monospace'
        const name = narrow && r.id.length > 6 ? `${r.id.slice(0, 5)}…` : r.id.length > 14 ? `${r.id.slice(0, 13)}…` : r.id
        g.fillText(name, 13, y + 9)
        g.font = '400 9.5px "IBM Plex Mono", monospace'
        g.fillStyle = r.dead ? col.err : r.leader ? col.signal : col.ink3
        g.fillText(r.dead ? 'no answer' : r.learner ? 'learner' : r.leader ? '★ leader' : 'follower', 13, y + 22)

        const ty = y + 3
        const th = 18
        // the bucket and the disks
        g.fillStyle = col.rule2
        g.fillRect(x0, ty, x1 - x0, th)
        // a member behind the leader trails by its own gap
        const behind = Math.max(0, D.commit - r.commit)
        const mc = r.leader ? commitNow : Math.max(0, commitNow - behind)
        const fX = X(Math.min(mc, shownF))
        g.fillStyle = col.accent
        g.globalAlpha = 0.32
        g.fillRect(x0, ty, Math.max(0, fX - x0), th)
        g.globalAlpha = 1
        const cX = X(mc)
        if (cX > fX) {
          g.fillStyle = r.leader ? col.signal : color
          g.globalAlpha = r.learner ? 0.35 : 0.75
          g.fillRect(fX, ty + 3, cX - fX, th - 6)
          g.globalAlpha = 1
        }
        if (fAt && ts - fAt < 900) {
          g.fillStyle = col.accent
          g.globalAlpha = 0.4 * (1 - (ts - fAt) / 900)
          g.fillRect(X(fFrom), ty, fX - X(fFrom), th)
          g.globalAlpha = 1
        }
        if (D.held && r.last > r.commit) {
          const lX = X(r.last)
          g.strokeStyle = col.idle
          g.setLineDash([3, 2])
          g.strokeRect(cX + 0.5, ty + 3.5, Math.max(0, lX - cX), th - 7)
          g.setLineDash([])
        }
        if (r.dead) hatch(g, x0, ty, x1 - x0, th, col.err)

        // the zoom: the last few dozen entries
        g.fillStyle = col.rule2
        g.fillRect(z0, ty, z1 - z0, th)
        if (!r.dead) {
          const lead = r.last - r.commit
          const zc = Z(mc)
          g.fillStyle = r.leader ? col.signal : color
          g.globalAlpha = 0.75
          g.fillRect(z0, ty + 3, zc - z0, th - 6)
          g.globalAlpha = 1
          const zl = Z(mc + Math.max(0, lead))
          g.strokeStyle = col.ink2
          g.lineWidth = 1
          g.strokeRect(zc + 0.5, ty + 3.5, Math.max(1, zl - zc), th - 7)
          const ze = Z(mc - Math.max(0, r.commit - r.emitted))
          g.fillStyle = col.ink
          g.beginPath()
          g.moveTo(ze, ty + th + 1)
          g.lineTo(ze - 3, ty + th + 5)
          g.lineTo(ze + 3, ty + th + 5)
          g.fill()
        } else hatch(g, z0, ty, z1 - z0, th, col.err)
        if (!narrow) {
          g.textAlign = 'left'
          g.fillStyle = col.ink3
          g.font = '400 10px "IBM Plex Mono", monospace'
          g.fillText(r.dead ? 'frozen' : `+${Math.max(0, r.last - r.commit).toLocaleString('en-US')} acked`, z1 + 10, y + 12)
        }
      })
    }
    raf = requestAnimationFrame(draw)
    return () => cancelAnimationFrame(raf)
  }, [])
  return <canvas ref={ref} className="cx-cv" style={{ height: 30 + Math.max(1, data.rows.length) * 34 + 6 }} role="img" aria-label="Each member's log: flushed to the bucket, committed, and appended entries waiting for acks" />
}
