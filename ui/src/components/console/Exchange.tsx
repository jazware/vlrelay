import { useEffect, useRef } from 'react'
import type { Overview } from '../../lib/api'
import { fmtSi } from '../../lib/console/fmt'
import { getLive } from '../../lib/console/live'
import type { RelayView } from '../../lib/console/relay'

// The exchange: PDS trunks on the left are patched into the core that reads their host shard,
// verified there and forwarded to the leader, which numbers each event and emits it once a
// majority holds it; the firehose fans out to every serving node. Rejects drop out at the cores.
// Trunks are the busiest hosts (grouped by their domain when several share one) plus the rest
// of the traffic as one bundle. Particles are a picture of the rates, not individual events.

export type Trunk = { label: string; eps: number; by: Record<string, number>; tone: 'accent' | 'warn' | 'err'; rej: number }

/** "porcini.us-west.host.bsky.network" groups as "host.bsky.network"; short names stay whole. */
const groupKey = (h: string) => {
  if (isAddr(h)) return h
  const p = h.split('.')
  return p.length >= 4 ? p.slice(-3).join('.') : h
}

/** An IP or a name with a port (dev-network hosts): not a domain to split. */
export const isAddr = (h: string) => h.includes(':') || /^\d+(\.\d+){3}$/.test(h)

export function trunksOf(o: Overview, cores: string[], max = 5): Trunk[] {
  const groups = new Map<string, Overview['topHosts']>()
  for (const h of o.topHosts) {
    const k = groupKey(h.host)
    groups.set(k, [...(groups.get(k) ?? []), h])
  }
  const fallback = cores[0] ?? ''
  const T: Trunk[] = []
  for (const [k, hs] of groups) {
    const by: Record<string, number> = {}
    let eps = 0
    let rej = 0
    for (const h of hs) {
      eps += h.eventsPerSec
      rej += h.eventsPerSec * h.errorRate
      const n = cores.includes(h.node) ? h.node : fallback
      by[n] = (by[n] ?? 0) + h.eventsPerSec
    }
    const throttled = hs.some((h) => h.status === 'throttled')
    T.push({ label: hs.length > 1 ? `${hs.length} *.${k}` : `${k}${throttled ? ' · throttled' : ''}`, eps, by, tone: throttled ? 'warn' : rej / Math.max(eps, 1e-9) > 0.2 ? 'err' : 'accent', rej: rej / Math.max(eps, 1e-9) })
  }
  T.sort((a, b) => b.eps - a.eps)
  const named = T.slice(0, max)
  const namedEps = named.reduce((a, t) => a + t.eps, 0)
  const rest = Math.max(0, o.eventsInPerSec - namedEps)
  const restBy: Record<string, number> = {}
  const nodeIn = (o.byNode ?? []).filter((n) => !n.stale && cores.includes(n.node))
  if (nodeIn.length) {
    for (const n of nodeIn) {
      const namedHere = named.reduce((a, t) => a + (t.by[n.node] ?? 0), 0)
      restBy[n.node] = Math.max(0, n.eventsInPerSec - namedHere)
    }
  } else if (cores.length) for (const c of cores) restBy[c] = rest / cores.length
  const others = Math.max(0, o.hostsConnected - o.topHosts.length)
  if (rest > 0 && others > 0) named.push({ label: `${others.toLocaleString('en-US')} other PDSes`, eps: rest, by: restBy, tone: 'accent', rej: o.rejectsPerSec / Math.max(o.eventsInPerSec, 1) })
  return named
}

type P = { t: number; c: string; u: number; rej: boolean; f: number; sp: number; fall?: number; at?: [number, number] }

const tokenOf = (color: string) => color.match(/--([a-z0-9-]+)/)?.[1] ?? 'c1'

export function Exchange({ o, view, height = 236 }: { o: Overview; view?: RelayView; height?: number }) {
  const ref = useRef<HTMLCanvasElement>(null)
  const data = useRef({ o, view })
  data.current = { o, view }

  useEffect(() => {
    const c = ref.current
    if (!c) return
    const parts: P[] = []
    const spawn: Record<string, number> = {}
    let last = 0
    let raf = 0
    const col: Record<string, string> = {}
    let colAt = 0
    const reduced = matchMedia('(prefers-reduced-motion: reduce)')
    const readColors = () => {
      const cs = getComputedStyle(c)
      for (const k of ['ink', 'ink2', 'ink3', 'rule', 'rule2', 'signal', 'accent', 'err', 'warn', 'ok', 'idle', 'sheet', 'sunk', 'paper', 'raised', 'c1', 'c2', 'c3', 'c4', 'c5', 'c6']) col[k] = cs.getPropertyValue(`--${k}`).trim()
    }
    const bez = (p0: number[], p1: number[], u: number): [number, number] => {
      const mx = (p0[0] + p1[0]) / 2
      const v = 1 - u
      const x = v * v * v * p0[0] + 3 * v * v * u * mx + 3 * v * u * u * mx + u * u * u * p1[0]
      const y = v * v * v * p0[1] + 3 * v * v * u * p0[1] + 3 * v * u * u * p1[1] + u * u * u * p1[1]
      return [x, y]
    }
    const draw = (ts: number) => {
      raf = requestAnimationFrame(draw)
      if (ts - last < 40 && last) return
      const dt = last ? Math.min(0.1, (ts - last) / 1000) : 0
      last = ts
      // colours change with the theme; re-read now and then
      if (ts - colAt > 1000) {
        readColors()
        colAt = ts
      }
      const W = c.clientWidth
      const H = height
      if (!W) return
      const dpr = window.devicePixelRatio || 1
      if (c.width !== Math.round(W * dpr) || c.height !== Math.round(H * dpr)) {
        c.width = Math.round(W * dpr)
        c.height = Math.round(H * dpr)
      }
      const g = c.getContext('2d')!
      g.setTransform(dpr, 0, 0, dpr, 0, 0)
      g.clearRect(0, 0, W, H)

      const { o, view } = data.current
      const coreNodes = view?.nodes.filter((n) => n.core) ?? []
      const cores = coreNodes.length ? coreNodes.map((n) => n.id) : [view?.self ?? 'relay']
      const colorOf = (id: string) => col[tokenOf(view?.byId.get(id)?.color ?? 'var(--c1)')]
      const dead = (id: string) => !!view?.byId.get(id)?.stale
      const held = view?.quorum?.health === 'down'
      const leader = view?.quorum?.leader ?? null
      const serve = (view?.nodes.length && !view.single ? view.nodes : [{ id: cores[0], consumers: o.consumers }]).map((n) => ({ id: n.id, consumers: n.consumers }))
      const T = trunksOf(o, cores)
      const narrow = W < 620
      const xT1 = W * 0.27
      const xC = W * 0.43
      const bw = narrow ? 52 : 82
      const bh = 30
      const xS = W * 0.63
      const sw = narrow ? 58 : 98
      const sh = 56
      const xF = W * 0.83
      const ty = T.map((_, i) => 22 + (i * (H - 36)) / Math.max(1, T.length - 1))
      const cy: Record<string, number> = Object.fromEntries(cores.map((id, i) => [id, cores.length === 1 ? H / 2 : H * (0.2 + (0.6 * i) / (cores.length - 1))]))
      const fy = serve.map((_, i) => (serve.length === 1 ? H / 2 : 26 + (i * (H - 46)) / (serve.length - 1)))
      const ys = H / 2
      const maxE = Math.max(...T.map((t) => t.eps), 1)
      const mono = (w: number, s: number) => `${w} ${s}px "IBM Plex Mono", ui-monospace, monospace`
      const curve = (p0: number[], p1: number[]) => {
        const mx = (p0[0] + p1[0]) / 2
        g.beginPath()
        g.moveTo(p0[0], p0[1])
        g.bezierCurveTo(mx, p0[1], mx, p1[1], p1[0], p1[1])
        g.stroke()
      }
      const hatch = (x: number, y: number, w: number, h: number, color: string) => {
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
      g.lineCap = 'round'
      g.textBaseline = 'alphabetic'

      // trunks and their patch cords to the cores reading them
      T.forEach((t, i) => {
        const y = ty[i]
        const w = 1 + 4 * Math.sqrt(t.eps / maxE)
        const tone = t.tone === 'err' ? col.err : t.tone === 'warn' ? col.warn : col.ink3
        g.strokeStyle = tone
        g.globalAlpha = 0.55
        g.lineWidth = w
        g.beginPath()
        g.moveTo(0, y)
        g.lineTo(xT1, y)
        g.stroke()
        for (const [core, e] of Object.entries(t.by)) {
          if (cy[core] === undefined) continue
          g.lineWidth = 0.8 + 3 * Math.sqrt(e / maxE)
          g.setLineDash(dead(core) ? [3, 3] : [])
          g.strokeStyle = dead(core) ? col.err : tone
          curve([xT1, y], [xC - bw / 2, cy[core]])
          g.setLineDash([])
        }
        g.globalAlpha = 1
        g.fillStyle = t.tone === 'err' ? col.err : t.tone === 'warn' ? col.warn : col.ink2
        g.textAlign = 'left'
        g.font = mono(500, narrow ? 9.5 : 10.5)
        const lab = narrow ? t.label.replace(/\*\.host\.bsky\.network/, 'bsky PDSes').replace(' · throttled', ' ▲') : t.label
        const maxLab = xT1 - 6
        let shown = lab
        while (shown.length > 4 && g.measureText(shown).width > maxLab) shown = `${shown.slice(0, -2)}…`
        g.fillText(shown, 0, y - w / 2 - 4)
        const rt = `${fmtSi(t.eps)}/s`
        if (!narrow && g.measureText(shown).width + g.measureText(rt).width + 14 < xT1) {
          g.fillStyle = col.ink3
          g.textAlign = 'right'
          g.fillText(rt, xT1 - 4, y - w / 2 - 4)
        }
      })

      // cores → the leader
      const coreIn: Record<string, number> = {}
      for (const t of T) for (const [k, e] of Object.entries(t.by)) coreIn[k] = (coreIn[k] ?? 0) + e
      for (const id of cores) {
        const y = cy[id]
        const d = dead(id)
        const cc = colorOf(id)
        g.strokeStyle = d ? col.err : cc
        g.globalAlpha = d ? 0.5 : 0.7
        g.lineWidth = 2
        g.setLineDash(d ? [3, 3] : [])
        curve([xC + bw / 2, y], [xS - sw / 2, ys])
        g.setLineDash([])
        g.globalAlpha = 1
        const x = xC - bw / 2
        g.fillStyle = col.raised
        g.fillRect(x, y - bh / 2, bw, bh)
        if (d) hatch(x, y - bh / 2, bw, bh, col.err)
        g.strokeStyle = d ? col.err : cc
        g.lineWidth = 1.5
        g.strokeRect(x + 0.5, y - bh / 2 + 0.5, bw - 1, bh - 1)
        g.fillStyle = d ? col.err : col.ink
        g.textAlign = 'center'
        g.font = mono(600, narrow ? 9.5 : 11)
        let name = id
        while (name.length > 3 && g.measureText(name).width > bw - 8) name = `${name.slice(0, -2)}…`
        g.fillText(name, xC, y - 1)
        g.font = mono(400, narrow ? 8.5 : 9.5)
        g.fillStyle = col.ink3
        g.fillText(d ? 'no answer' : narrow ? fmtSi(coreIn[id] ?? 0) : `verify · ${fmtSi(coreIn[id] ?? 0)}/s`, xC, y + 10)
      }

      // the leader: numbers each event, commits on a majority
      const sx = xS - sw / 2
      const sy = ys - sh / 2
      g.fillStyle = col.raised
      g.fillRect(sx, sy, sw, sh)
      g.strokeStyle = held ? col.err : col.signal
      g.lineWidth = 2
      g.strokeRect(sx + 1, sy + 1, sw - 2, sh - 2)
      g.textAlign = 'center'
      g.fillStyle = col.ink
      g.font = mono(700, narrow ? 9.5 : 11)
      g.fillText(held ? 'no leader' : narrow ? 'leader' : leader ? `${leader.length > 12 ? 'leader' : leader} leads` : 'sequencer', xS, sy + 15)
      g.font = mono(400, narrow ? 8.5 : 9.5)
      g.fillStyle = col.ink3
      const ep = view?.quorum?.epoch
      g.fillText(held ? 'no quorum' : ep ? (narrow ? `e${ep}` : `epoch ${ep} · seq`) : 'seq', xS, sy + 28)
      const lamps = view?.quorum?.members ?? cores
      lamps.forEach((m, i) => {
        const lx = xS + (i - (lamps.length - 1) / 2) * 12
        const ly = sy + 42
        const d = dead(m) || !view?.quorum?.answering.includes(m)
        g.beginPath()
        g.arc(lx, ly, 4, 0, 7)
        g.fillStyle = d && view?.quorum ? col.sunk : colorOf(m)
        g.fill()
        g.strokeStyle = d && view?.quorum ? col.err : colorOf(m)
        g.lineWidth = 1
        g.stroke()
      })

      // the firehose and the nodes serving it
      g.strokeStyle = held ? col.err : col.signal
      g.lineWidth = 4
      g.setLineDash(held ? [6, 5] : [])
      g.beginPath()
      g.moveTo(sx + sw, ys)
      g.lineTo(xF, ys)
      g.stroke()
      g.setLineDash([])
      g.textAlign = 'left'
      g.fillStyle = held ? col.err : col.signal
      g.font = mono(600, narrow ? 9 : 10.5)
      if (!narrow) g.fillText(held ? 'held' : `firehose ${fmtSi(o.streamEventsPerSec ?? o.eventsOutPerSec)}/s`, sx + sw + 6, ys - 8)
      serve.forEach((n, i) => {
        const d = dead(n.id) || held
        g.strokeStyle = d ? col.err : colorOf(n.id)
        g.globalAlpha = 0.6
        g.lineWidth = 1.5
        g.setLineDash(d ? [3, 3] : [])
        curve([xF, ys], [W - 4, fy[i]])
        g.setLineDash([])
        g.globalAlpha = 1
        g.textAlign = 'right'
        g.fillStyle = col.ink2
        g.font = mono(500, narrow ? 8.5 : 10)
        g.fillText(`${narrow ? n.id.replace(/^[a-z]+-/, '') : n.id} · ${n.consumers}`, W - 4, fy[i] - 5)
      })

      // particles
      const live = getLive()
      const pos = (p: P): [number, number] => {
        const u = p.u
        if (u < 1) {
          const y = ty[p.t] ?? ys
          if (u < 0.35) return [(u / 0.35) * xT1, y]
          return bez([xT1, y], [xC - bw / 2, cy[p.c] ?? ys], (u - 0.35) / 0.65)
        }
        if (u < 2) return bez([xC + bw / 2, cy[p.c] ?? ys], [xS - sw / 2, ys], u - 1)
        if (u < 3) return [xS + sw / 2 + (u - 2) * (xF - xS - sw / 2), ys]
        return bez([xF, ys], [W - 4, fy[p.f] ?? ys], u - 3)
      }
      if (!reduced.matches && !live.paused && !live.stale && dt) {
        T.forEach((t, ti) => {
          for (const [core, e] of Object.entries(t.by)) {
            if (cy[core] === undefined) continue
            const k = `${ti}:${core}`
            spawn[k] = (spawn[k] ?? 0) + dt * 1.9 * Math.log10(1 + e * 3)
            while (spawn[k] >= 1 && parts.length < 220) {
              spawn[k] -= 1
              parts.push({ t: ti, c: core, u: 0, rej: Math.random() < Math.min(0.9, t.rej * 1.5 + 0.004), f: Math.floor(Math.random() * serve.length), sp: 0.85 + Math.random() * 0.3 })
            }
          }
        })
        let pile = 0
        for (let i = parts.length - 1; i >= 0; i--) {
          const p = parts[i]
          if (p.fall !== undefined) {
            p.fall += dt
            if (p.fall > 0.8) parts.splice(i, 1)
            continue
          }
          const prevU = p.u
          p.u += dt * p.sp
          if (prevU < 1 && p.u >= 1) {
            if (dead(p.c)) {
              parts.splice(i, 1)
              continue
            }
            if (p.rej) {
              p.fall = 0
              p.at = pos({ ...p, u: 0.999 })
              continue
            }
          }
          if (p.u >= 2 && held) {
            p.u = 2
            if (++pile > 40) parts.splice(i, 1)
            continue
          }
          if (p.u >= 4) parts.splice(i, 1)
        }
      }
      let pi = 0
      for (const p of parts) {
        if (p.fall !== undefined && p.at) {
          const [x, y] = p.at
          g.fillStyle = col.err
          g.globalAlpha = 1 - p.fall / 0.8
          g.fillRect(x - 2, y + p.fall * 46, 4, 4)
          g.globalAlpha = 1
          continue
        }
        if (p.u === 2 && held) {
          const x = sx - 6 - (pi % 8) * 5
          const y = ys + 34 + Math.floor(pi / 8) * 5
          pi++
          g.fillStyle = col.idle
          g.fillRect(x, y, 3, 3)
          continue
        }
        const [x, y] = pos(p)
        g.beginPath()
        g.arc(x, y, p.u >= 2 ? 2.4 : 1.8, 0, 7)
        g.fillStyle = p.u >= 2 ? col.signal : p.u >= 1 ? colorOf(p.c) : col.ink2
        g.fill()
      }
    }
    raf = requestAnimationFrame(draw)
    return () => cancelAnimationFrame(raf)
  }, [height])

  return <canvas ref={ref} className="cx-cv" style={{ height }} aria-label="PDS streams merging through the cores and the leader into one firehose" />
}
