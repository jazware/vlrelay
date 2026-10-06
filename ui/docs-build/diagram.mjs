// Build-time diagram renderer: a small YAML spec (```diagram fences and the
// `diagram:` of a ```hero) → inline SVG. Colors come only from classes styled
// in src/docs.css with the theme tokens, so a diagram follows light/dark and
// needs no inline style (the page CSP has no 'unsafe-inline'). The spec
// format is documented in docs/_style.md.

export const U = 20 // px per grid unit
const PAD = 14
const ARROW = 7
export const TONES = new Set(['ink', 'accent', 'amber', 'blue', 'violet', 'rust', 'cyan', 'muted', 'solid', 'danger', 'ok'])
const SHAPES = new Set(['box', 'store', 'pill', 'note'])

export function esc(s) {
  return String(s).replace(/&/g, '&amp;').replace(/</g, '&lt;').replace(/>/g, '&gt;').replace(/"/g, '&quot;')
}

// Rough advance widths, only used to size label backgrounds and the viewBox.
export function textWidth(s, size, mono) {
  let w = 0
  for (const ch of s) w += mono ? 0.6 : /[mwMW@]/.test(ch) ? 0.82 : /[ilj.,:;'|!]/.test(ch) ? 0.3 : /[A-Z]/.test(ch) ? 0.66 : 0.54
  return w * size
}

/** `code` spans become mono tspans. */
export function tspans(s) {
  return String(s)
    .split(/(`[^`]*`)/)
    .filter(Boolean)
    .map((p) => (p.startsWith('`') && p.endsWith('`') ? `<tspan class="dg-code">${esc(p.slice(1, -1))}</tspan>` : esc(p)))
    .join('')
}
export function plainWidth(s, size) {
  return String(s)
    .split(/(`[^`]*`)/)
    .filter(Boolean)
    .reduce((w, p) => w + (p.startsWith('`') ? textWidth(p.slice(1, -1), size * 0.92, true) : textWidth(p, size, false)), 0)
}

const KEYS = {
  node: ['id', 'label', 'sub', 'at', 'size', 'tone', 'shape', 'stack', 'badge'],
  group: ['label', 'around', 'pad', 'tone', 'at', 'size'],
  edge: ['from', 'to', 'label', 'dash', 'arrow', 'tone', 'via', 'labelAt'],
  note: ['at', 'text', 'align', 'tone'],
  diagram: ['caption', 'title', 'nodes', 'groups', 'edges', 'notes'],
  hero: ['diagram', 'timeline', 'facts'],
}

/**
 * Unknown keys are almost always a YAML flow map split at a comma
 * ({ sub: a, b } is sub: "a" plus a key "b"): quote such strings.
 */
export function checkKeys(obj, kind, what) {
  if (!obj || typeof obj !== 'object' || Array.isArray(obj)) return
  const bad = Object.keys(obj).filter((k) => !KEYS[kind].includes(k))
  if (bad.length) throw new Error(`${what}: unknown key(s) ${bad.map((k) => JSON.stringify(k)).join(', ')} (a comma in an unquoted string? quote it)`)
}

// group labels: 10.5px uppercase with 0.08em tracking
function labelCapsWidth(s) {
  const t = String(s).replace(/`/g, '')
  return textWidth(t.toUpperCase(), 10.5, false) + t.length * 0.84
}

export function num(v, what) {
  if (typeof v !== 'number' || !Number.isFinite(v)) throw new Error(`${what}: expected a number, got ${JSON.stringify(v)}`)
  return v
}
function pair(v, what) {
  if (!Array.isArray(v) || v.length !== 2) throw new Error(`${what}: expected [x, y], got ${JSON.stringify(v)}`)
  return [num(v[0], what), num(v[1], what)]
}

function parseEdge(e, i) {
  if (typeof e === 'string') {
    const m = e.match(/^\s*(\S+)\s+(<?)(->|--|~>|~~|-|~)(>?)\s*(\S+)\s*(?::\s*(.*))?$/)
    // "a -> b", "a <-> b", "a -- b", "a ~> b" (dashed), optional ": label"
    const m2 = m ?? e.match(/^\s*(\S+)\s+(<?)(-+|~+)(>?)\s+(\S+)\s*(?::\s*(.*))?$/)
    if (!m2) throw new Error(`edge ${i + 1}: can't parse ${JSON.stringify(e)} (use "a -> b: label", "a <-> b", "a ~> b" dashed, "a -- b" no arrow)`)
    const [, from, back, op, fwd, to, label] = m2
    const dash = op.startsWith('~')
    const end = op.endsWith('>') || fwd === '>'
    return { from, to, label: label?.trim() || undefined, dash, start: back === '<', end }
  }
  if (!e || typeof e !== 'object') throw new Error(`edge ${i + 1}: expected a string or a map`)
  checkKeys(e, 'edge', `edge ${i + 1}`)
  const arrow = e.arrow ?? 'end'
  return {
    ...e,
    start: arrow === 'start' || arrow === 'both',
    end: arrow === 'end' || arrow === 'both',
  }
}

function sidePoint(n, side, frac) {
  const f = frac ?? 0.5
  switch (side) {
    case 't':
      return [n.x + n.w * f, n.y]
    case 'b':
      return [n.x + n.w * f, n.y + n.h]
    case 'l':
      return [n.x, n.y + n.h * f]
    case 'r':
      return [n.x + n.w, n.y + n.h * f]
  }
}

function endpoint(ref, nodes, i) {
  const m = String(ref).match(/^([A-Za-z0-9_-]+)(?:\.([tblr])(\d{1,3})?)?$/)
  if (!m) throw new Error(`edge ${i + 1}: bad endpoint ${JSON.stringify(ref)} (id, or id.t / id.b / id.l / id.r, optionally with a percentage: id.r25)`)
  const n = nodes.get(m[1])
  if (!n) throw new Error(`edge ${i + 1}: no node ${JSON.stringify(m[1])}`)
  return { n, side: m[2], frac: m[3] !== undefined ? Number(m[3]) / 100 : undefined }
}

/** Where the segment from the centre of `n` towards `p` leaves its box. */
function clip(n, p) {
  const cx = n.x + n.w / 2
  const cy = n.y + n.h / 2
  const dx = p[0] - cx
  const dy = p[1] - cy
  if (dx === 0 && dy === 0) return [cx, cy]
  const tx = dx === 0 ? Infinity : n.w / 2 / Math.abs(dx)
  const ty = dy === 0 ? Infinity : n.h / 2 / Math.abs(dy)
  const t = Math.min(tx, ty)
  return [cx + dx * t, cy + dy * t]
}

function overlap(a0, a1, b0, b1) {
  const lo = Math.max(a0, b0)
  const hi = Math.min(a1, b1)
  return hi - lo >= 8 ? (lo + hi) / 2 : null
}

function route(e, nodes, i) {
  const a = endpoint(e.from, nodes, i)
  const b = endpoint(e.to, nodes, i)
  if (e.via) {
    const via = e.via.map((v, k) => pair(v, `edge ${i + 1} via[${k}]`).map((c) => c * U))
    const p0 = a.side ? sidePoint(a.n, a.side, a.frac) : clip(a.n, via[0])
    const p1 = b.side ? sidePoint(b.n, b.side, b.frac) : clip(b.n, via[via.length - 1])
    return [p0, ...via, p1]
  }
  if (!a.side && !b.side) {
    // Aligned boxes get a straight orthogonal line through their overlap.
    const ox = overlap(a.n.x, a.n.x + a.n.w, b.n.x, b.n.x + b.n.w)
    if (ox !== null) {
      const down = b.n.y > a.n.y
      return [
        [ox, down ? a.n.y + a.n.h : a.n.y],
        [ox, down ? b.n.y : b.n.y + b.n.h],
      ]
    }
    const oy = overlap(a.n.y, a.n.y + a.n.h, b.n.y, b.n.y + b.n.h)
    if (oy !== null) {
      const right = b.n.x > a.n.x
      return [
        [right ? a.n.x + a.n.w : a.n.x, oy],
        [right ? b.n.x : b.n.x + b.n.w, oy],
      ]
    }
    const ca = [a.n.x + a.n.w / 2, a.n.y + a.n.h / 2]
    const cb = [b.n.x + b.n.w / 2, b.n.y + b.n.h / 2]
    return [clip(a.n, cb), clip(b.n, ca)]
  }
  const sa = a.side ?? autoSide(a.n, b.n)
  const sb = b.side ?? autoSide(b.n, a.n)
  const p = sidePoint(a.n, sa, a.frac)
  const q = sidePoint(b.n, sb, b.frac)
  const ha = sa === 'l' || sa === 'r'
  const hb = sb === 'l' || sb === 'r'
  if (p[0] === q[0] || p[1] === q[1]) return [p, q]
  if (ha && hb) {
    const mx = (p[0] + q[0]) / 2
    return [p, [mx, p[1]], [mx, q[1]], q]
  }
  if (!ha && !hb) {
    const my = (p[1] + q[1]) / 2
    return [p, [p[0], my], [q[0], my], q]
  }
  return ha ? [p, [q[0], p[1]], q] : [p, [p[0], q[1]], q]
}

function autoSide(n, other) {
  const dx = other.x + other.w / 2 - (n.x + n.w / 2)
  const dy = other.y + other.h / 2 - (n.y + n.h / 2)
  return Math.abs(dx) > Math.abs(dy) ? (dx > 0 ? 'r' : 'l') : dy > 0 ? 'b' : 't'
}

/** Shortens the path at an arrowed end so the line stops at the arrow's base. */
function trim(pts, atEnd, by) {
  const out = pts.map((p) => [...p])
  const [i, j] = atEnd ? [out.length - 1, out.length - 2] : [0, 1]
  const dx = out[i][0] - out[j][0]
  const dy = out[i][1] - out[j][1]
  const len = Math.hypot(dx, dy) || 1
  const k = Math.min(by, len - 1) / len
  out[i] = [out[i][0] - dx * k, out[i][1] - dy * k]
  return out
}

export function arrowHead(tip, from, cls) {
  const dx = tip[0] - from[0]
  const dy = tip[1] - from[1]
  const len = Math.hypot(dx, dy) || 1
  const ux = dx / len
  const uy = dy / len
  const bx = tip[0] - ux * ARROW
  const by = tip[1] - uy * ARROW
  const w = ARROW * 0.55
  const pts = [tip, [bx - uy * w, by + ux * w], [bx + uy * w, by - ux * w]]
  return `<polygon class="${cls}" points="${pts.map((p) => p.map(r).join(',')).join(' ')}"/>`
}

export const r = (v) => Math.round(v * 10) / 10

/** A path through `pts` with rounded corners. */
function pathD(pts) {
  let d = `M${r(pts[0][0])},${r(pts[0][1])}`
  for (let k = 1; k < pts.length; k++) {
    const p = pts[k]
    if (k < pts.length - 1) {
      const prev = pts[k - 1]
      const next = pts[k + 1]
      const l1 = Math.hypot(p[0] - prev[0], p[1] - prev[1])
      const l2 = Math.hypot(next[0] - p[0], next[1] - p[1])
      const rad = Math.min(8, l1 / 2, l2 / 2)
      const a = [p[0] - ((p[0] - prev[0]) / (l1 || 1)) * rad, p[1] - ((p[1] - prev[1]) / (l1 || 1)) * rad]
      const b = [p[0] + ((next[0] - p[0]) / (l2 || 1)) * rad, p[1] + ((next[1] - p[1]) / (l2 || 1)) * rad]
      d += ` L${r(a[0])},${r(a[1])} Q${r(p[0])},${r(p[1])} ${r(b[0])},${r(b[1])}`
    } else d += ` L${r(p[0])},${r(p[1])}`
  }
  return d
}

function labelPos(pts) {
  let best = 0
  let bestLen = -1
  for (let k = 0; k < pts.length - 1; k++) {
    const l = Math.hypot(pts[k + 1][0] - pts[k][0], pts[k + 1][1] - pts[k][1])
    if (l > bestLen) {
      bestLen = l
      best = k
    }
  }
  return [(pts[best][0] + pts[best + 1][0]) / 2, (pts[best][1] + pts[best + 1][1]) / 2]
}

export function tone(t, what) {
  const v = t ?? 'ink'
  if (!TONES.has(v)) throw new Error(`${what}: unknown tone ${JSON.stringify(v)} (${[...TONES].join(', ')})`)
  return v
}

/**
 * Renders a diagram spec to an SVG string. Throws with a readable message on
 * a bad spec (the build reports it with the page and line).
 */
export function renderDiagram(spec) {
  if (!spec || typeof spec !== 'object') throw new Error('diagram: expected a map with nodes/edges')
  checkKeys(spec, 'diagram', 'diagram')
  const nodes = new Map()
  const parts = { groups: [], edges: [], nodes: [], labels: [], notes: [] }
  const box = { x0: Infinity, y0: Infinity, x1: -Infinity, y1: -Infinity }
  const grow = (x0, y0, x1, y1) => {
    box.x0 = Math.min(box.x0, x0)
    box.y0 = Math.min(box.y0, y0)
    box.x1 = Math.max(box.x1, x1)
    box.y1 = Math.max(box.y1, y1)
  }

  for (const [i, n] of (spec.nodes ?? []).entries()) {
    const what = `node ${n?.id ?? i + 1}`
    if (!n?.id) throw new Error(`node ${i + 1}: missing id`)
    if (nodes.has(n.id)) throw new Error(`${what}: duplicate id`)
    checkKeys(n, 'node', what)
    const [x, y] = pair(n.at, `${what} at`)
    const [w, h] = n.size ? pair(n.size, `${what} size`) : [8, 3]
    const shape = n.shape ?? 'box'
    if (!SHAPES.has(shape)) throw new Error(`${what}: unknown shape ${JSON.stringify(shape)} (${[...SHAPES].join(', ')})`)
    const node = { ...n, x: x * U, y: y * U, w: w * U, h: h * U, shape, tone: tone(n.tone, what) }
    nodes.set(n.id, node)
    grow(node.x, node.y - (n.stack ? 8 : 0), node.x + node.w + (n.stack ? 8 : 0), node.y + node.h)
  }

  for (const [i, g] of (spec.groups ?? []).entries()) {
    const what = `group ${g?.label ?? i + 1}`
    checkKeys(g, 'group', what)
    let x0, y0, x1, y1
    if (g.around) {
      const pad = (g.pad ?? 1) * U
      const ns = g.around.map((id) => {
        const n = nodes.get(id)
        if (!n) throw new Error(`${what}: no node ${JSON.stringify(id)}`)
        return n
      })
      x0 = Math.min(...ns.map((n) => n.x)) - pad
      y0 = Math.min(...ns.map((n) => n.y - (n.stack ? 8 : 0))) - pad - (g.label ? 14 : 0)
      x1 = Math.max(...ns.map((n) => n.x + n.w + (n.stack ? 8 : 0))) + pad
      y1 = Math.max(...ns.map((n) => n.y + n.h)) + pad
    } else {
      const [x, y] = pair(g.at, `${what} at`)
      const [w, h] = pair(g.size, `${what} size`)
      ;[x0, y0, x1, y1] = [x * U, y * U, (x + w) * U, (y + h) * U]
    }
    const t = tone(g.tone ?? 'muted', what)
    if (g.label) x1 = Math.max(x1, x0 + 24 + labelCapsWidth(g.label))
    grow(x0, y0, x1, y1)
    parts.groups.push(
      `<g class="dg-g dg-t-${t}"><rect x="${r(x0)}" y="${r(y0)}" width="${r(x1 - x0)}" height="${r(y1 - y0)}" rx="10"/>` +
        (g.label ? `<text class="dg-gl" x="${r(x0 + 12)}" y="${r(y0 + 18)}">${tspans(g.label)}</text>` : '') +
        `</g>`,
    )
  }

  for (const [i, raw] of (spec.edges ?? []).entries()) {
    const e = parseEdge(raw, i)
    let pts = route(e, nodes, i)
    for (const p of pts) grow(p[0], p[1], p[0], p[1])
    const t = tone(e.tone, `edge ${i + 1}`)
    const heads = []
    if (e.end) {
      heads.push(arrowHead(pts[pts.length - 1], pts[pts.length - 2], 'dg-ah'))
      pts = trim(pts, true, ARROW - 1)
    }
    if (e.start) {
      heads.push(arrowHead(pts[0], pts[1], 'dg-ah'))
      pts = trim(pts, false, ARROW - 1)
    }
    parts.edges.push(`<g class="dg-e dg-t-${t}${e.dash ? ' dg-dash' : ''}"><path d="${pathD(pts)}"/>${heads.join('')}</g>`)
    if (e.label) {
      const [lx, ly] = e.labelAt ? pair(e.labelAt, `edge ${i + 1} labelAt`).map((c) => c * U) : labelPos(pts)
      const lines = String(e.label).split('\n')
      const w = Math.max(...lines.map((l) => plainWidth(l, 11.5))) + 10
      const h = lines.length * 14 + 4
      grow(lx - w / 2, ly - h / 2, lx + w / 2, ly + h / 2)
      parts.labels.push(
        `<g class="dg-el dg-t-${t}"><rect x="${r(lx - w / 2)}" y="${r(ly - h / 2)}" width="${r(w)}" height="${r(h)}" rx="4"/>` +
          lines.map((l, k) => `<text x="${r(lx)}" y="${r(ly - h / 2 + 13 + k * 14)}">${tspans(l)}</text>`).join('') +
          `</g>`,
      )
    }
  }

  for (const n of nodes.values()) parts.nodes.push(renderNode(n))

  for (const [i, n] of (spec.notes ?? []).entries()) {
    checkKeys(n, 'note', `note ${i + 1}`)
    const [x, y] = pair(n.at, `note ${i + 1} at`).map((c) => c * U)
    const anchor = n.align === 'end' ? 'end' : n.align === 'middle' ? 'middle' : 'start'
    const lines = String(n.text ?? '').split('\n')
    const w = Math.max(...lines.map((l) => plainWidth(l, 11.5)))
    const x0 = anchor === 'start' ? x : anchor === 'end' ? x - w : x - w / 2
    grow(x0, y - 11, x0 + w, y + (lines.length - 1) * 14 + 4)
    parts.notes.push(
      `<text class="dg-note dg-t-${tone(n.tone ?? 'muted', `note ${i + 1}`)}" text-anchor="${anchor}">` +
        lines.map((l, k) => `<tspan x="${r(x)}" y="${r(y + k * 14)}">${tspans(l)}</tspan>`).join('') +
        `</text>`,
    )
  }

  if (!Number.isFinite(box.x0)) throw new Error('diagram: nothing to draw')
  const vx = box.x0 - PAD
  const vy = box.y0 - PAD
  const vw = box.x1 - box.x0 + PAD * 2
  const vh = box.y1 - box.y0 + PAD * 2
  const title = spec.title ?? spec.caption ?? ''
  return {
    width: vw,
    svg:
      `<svg class="dg" viewBox="${r(vx)} ${r(vy)} ${r(vw)} ${r(vh)}" width="${Math.round(vw)}" height="${Math.round(vh)}" role="img"${title ? ` aria-label="${esc(title.replace(/`/g, ''))}"` : ''}>` +
      parts.groups.join('') +
      parts.edges.join('') +
      parts.nodes.join('') +
      parts.labels.join('') +
      parts.notes.join('') +
      `</svg>`,
  }
}

function renderNode(n) {
  const { x, y, w, h } = n
  let shape
  if (n.shape === 'store') {
    const e = Math.min(8, h / 5)
    const body = `M${x},${y + e} A${w / 2},${e} 0 0 1 ${x + w},${y + e} V${y + h - e} A${w / 2},${e} 0 0 1 ${x},${y + h - e} Z`
    shape = `<path class="dg-s" d="${body}"/><path class="dg-lid" d="M${x},${y + e} A${w / 2},${e} 0 0 0 ${x + w},${y + e}"/>`
  } else {
    const rx = n.shape === 'pill' ? h / 2 : n.shape === 'note' ? 2 : 6
    shape = `<rect class="dg-s" x="${x}" y="${y}" width="${w}" height="${h}" rx="${r(rx)}"/>`
  }
  const stack = n.stack
    ? `<rect class="dg-s dg-back" x="${x + 8}" y="${y - 8}" width="${w}" height="${h}" rx="6"/><rect class="dg-s dg-back" x="${x + 4}" y="${y - 4}" width="${w}" height="${h}" rx="6"/>`
    : ''
  const lines = String(n.label ?? n.id).split('\n')
  const subs = n.sub ? String(n.sub).split('\n') : []
  const lh = 16
  const sh = 14
  const total = lines.length * lh + subs.length * sh
  const cx = x + w / 2
  let ty = y + h / 2 - total / 2 + (n.shape === 'store' ? 4 : 0)
  const texts = []
  for (const l of lines) {
    texts.push(`<text class="dg-l" x="${r(cx)}" y="${r(ty + 12)}">${tspans(l)}</text>`)
    ty += lh
  }
  for (const s of subs) {
    texts.push(`<text class="dg-sub" x="${r(cx)}" y="${r(ty + 11)}">${tspans(s)}</text>`)
    ty += sh
  }
  let badge = ''
  if (n.badge) {
    const bw = plainWidth(String(n.badge), 11) + 12
    badge = `<g class="dg-badge"><rect x="${r(x + w - bw + 6)}" y="${y - 9}" width="${r(bw)}" height="18" rx="9"/><text x="${r(x + w - bw / 2 + 6)}" y="${y + 4}">${tspans(n.badge)}</text></g>`
  }
  return `<g class="dg-n dg-t-${n.tone} dg-sh-${n.shape}">${stack}${shape}${texts.join('')}${badge}</g>`
}
