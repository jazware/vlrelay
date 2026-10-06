// Build-time swimlane timeline renderer: a ```timeline spec (or a hero's
// `timeline:`) → inline SVG. Lanes are rows, time runs left to right in
// abstract units (`scale` px each), so a µs step and a 300 ms PUT can share a
// figure: real durations go in the labels and the axis ticks. Colors come
// only from the tone classes in src/docs.css, as for diagrams. The spec
// format is documented in docs/_style.md ("timeline").
//
// Every text box is placed by fixed rules and checked against every other
// one; a collision, an unknown lane, overlapping spans in one lane or an
// arrow through a span fails the build instead of drawing a mess.

import { esc, tspans, plainWidth, tone, num, arrowHead, r } from './diagram.mjs'

const PAD = 14
const LANE_H = 58
const BAR_Y = 9 // bar top within a lane row
const BAR_H = 22
const MID = BAR_Y + BAR_H / 2
const SUB_Y = 46 // baseline of a span's `dur` or an event's label
const ROW_H = 22 // one row of mark labels
const AXIS_H = 30
const ARROW = 7

const KEYS = {
  timeline: ['caption', 'title', 'scale', 'lanes', 'spans', 'events', 'arrows', 'marks', 'ticks'],
  lane: ['id', 'label', 'sub', 'tone'],
  span: ['lane', 'from', 'to', 'label', 'dur', 'tone', 'dash'],
  event: ['lane', 'at', 'label', 'tone'],
  arrow: ['from', 'to', 'at', 'label', 'tone', 'dash', 'side'],
  mark: ['at', 'label', 'tone'],
  tick: ['at', 'label'],
}

function keys(obj, kind, what) {
  if (!obj || typeof obj !== 'object' || Array.isArray(obj)) throw new Error(`${what}: expected a map`)
  const bad = Object.keys(obj).filter((k) => !KEYS[kind].includes(k))
  if (bad.length) throw new Error(`${what}: unknown key(s) ${bad.map((k) => JSON.stringify(k)).join(', ')} (a comma in an unquoted string? quote it)`)
}

function list(v, what) {
  if (v === undefined) return []
  if (!Array.isArray(v)) throw new Error(`${what}: expected a list`)
  return v
}

const bold = (s, size) => plainWidth(s, size) * 1.06

/**
 * Renders a timeline spec to { width, svg }. Throws a readable message on a
 * bad spec (the build reports it with the page and line).
 */
export function renderTimeline(spec) {
  keys(spec, 'timeline', 'timeline')
  if (typeof spec.caption !== 'string' || !spec.caption.trim()) throw new Error('timeline: needs a caption (what the reader should see in it)')
  const px = spec.scale === undefined ? 56 : num(spec.scale, 'timeline scale')
  if (px < 8) throw new Error('timeline: scale is px per time unit, at least 8')

  const lanes = new Map()
  for (const [i, l] of list(spec.lanes, 'timeline lanes').entries()) {
    const what = `lane ${l?.id ?? i + 1}`
    keys(l, 'lane', what)
    if (!l.id) throw new Error(`lane ${i + 1}: missing id`)
    if (lanes.has(l.id)) throw new Error(`${what}: duplicate id`)
    lanes.set(l.id, { ...l, i, tone: tone(l.tone, what) })
  }
  if (!lanes.size) throw new Error('timeline: needs lanes')
  const lane = (id, what) => {
    const l = lanes.get(id)
    if (!l) throw new Error(`${what}: no lane ${JSON.stringify(id)} (lanes: ${[...lanes.keys()].join(', ')})`)
    return l
  }

  const spans = list(spec.spans, 'timeline spans').map((s, i) => {
    const what = `span ${i + 1}${s?.label ? ` (${s.label})` : ''}`
    keys(s, 'span', what)
    const from = num(s.from, `${what} from`)
    const to = num(s.to, `${what} to`)
    if (!(to > from)) throw new Error(`${what}: to (${to}) must be after from (${from})`)
    if (s.dur !== undefined && typeof s.dur !== 'string') throw new Error(`${what}: dur is a label such as "~30 ms" (quote it)`)
    return { ...s, what, from, to, l: lane(s.lane, what), tone: tone(s.tone ?? 'accent', what) }
  })
  for (let a = 0; a < spans.length; a++)
    for (let b = a + 1; b < spans.length; b++) {
      const [p, q] = [spans[a], spans[b]]
      if (p.l === q.l && p.from < q.to && q.from < p.to) throw new Error(`${p.what} and ${q.what} overlap in lane ${p.l.id}: one lane does one thing at a time (add a lane)`)
    }
  const events = list(spec.events, 'timeline events').map((e, i) => {
    const what = `event ${i + 1}${e?.label ? ` (${e.label})` : ''}`
    keys(e, 'event', what)
    return { ...e, what, at: num(e.at, `${what} at`), l: lane(e.lane, what), tone: tone(e.tone ?? 'accent', what) }
  })
  const arrows = list(spec.arrows, 'timeline arrows').map((a, i) => {
    const what = `arrow ${i + 1}${a?.label ? ` (${a.label})` : ''}`
    keys(a, 'arrow', what)
    const [t0, t1] = Array.isArray(a.at) ? [num(a.at[0], `${what} at`), num(a.at[1], `${what} at`)] : [num(a.at, `${what} at`), num(a.at, `${what} at`)]
    if (Array.isArray(a.at) && (a.at.length !== 2 || t1 < t0)) throw new Error(`${what}: at is a time or [sent, arrived] with arrived ≥ sent`)
    const from = lane(a.from, what)
    const to = lane(a.to, what)
    if (from === to) throw new Error(`${what}: from and to are the same lane`)
    if (a.side !== undefined && a.side !== 'left' && a.side !== 'right') throw new Error(`${what}: side is left or right`)
    return { ...a, what, t0, t1, from, to, tone: tone(a.tone, what) }
  })
  const marks = list(spec.marks, 'timeline marks').map((m, i) => {
    const what = `mark ${i + 1}`
    keys(m, 'mark', what)
    if (!m.label) throw new Error(`${what}: needs a label`)
    return { ...m, what: `${what} (${m.label})`, at: num(m.at, `${what} at`), tone: tone(m.tone ?? 'solid', what) }
  })
  const ticks = list(spec.ticks, 'timeline ticks').map((t, i) => {
    keys(t, 'tick', `tick ${i + 1}`)
    return { ...t, at: num(t.at, `tick ${i + 1} at`) }
  })

  // x: lane labels on the left, then time
  const times = [...spans.flatMap((s) => [s.from, s.to]), ...events.map((e) => e.at), ...arrows.flatMap((a) => [a.t0, a.t1]), ...marks.map((m) => m.at), ...ticks.map((t) => t.at)]
  if (!times.length) throw new Error('timeline: nothing on it (spans, events, arrows or marks)')
  const t0 = Math.min(...times)
  const labelW = Math.max(...[...lanes.values()].map((l) => Math.max(bold(String(l.label ?? l.id), 12.5), l.sub ? plainWidth(String(l.sub), 11) : 0))) + 18
  const X = (t) => labelW + 10 + (t - t0) * px

  // mark labels: greedy rows above the lanes, so close marks stack
  const rows = []
  for (const m of [...marks].sort((a, b) => a.at - b.at)) {
    const w = bold(String(m.label), 11.5) + 14
    const x0 = Math.max(X(m.at) - w / 2, labelW + 4)
    let row = rows.findIndex((end) => end + 6 <= x0)
    if (row < 0) row = rows.push(-Infinity) - 1
    rows[row] = x0 + w
    Object.assign(m, { row, x0, w })
  }
  const top = rows.length * ROW_H + (rows.length ? 8 : 0)
  const laneY = (l) => top + l.i * LANE_H
  const bottom = top + lanes.size * LANE_H

  const boxes = []
  // text keeps a 2 px gap from everything; bars may touch each other
  const place = (b, what, gap = 2, text = true) => {
    for (const o of boxes) {
      const g = Math.max(gap, o.gap)
      if (b.x0 < o.x1 + g && o.x0 < b.x1 + g && b.y0 < o.y1 + 1 && o.y0 < b.y1 + 1)
        throw new Error(`timeline: ${what} overlaps ${o.what} (move one, shorten a label, or raise scale)`)
    }
    boxes.push({ ...b, what, gap, text })
  }
  const ext = { x0: 0, y0: 0, x1: 0, y1: bottom }
  const grow = (x0, y0, x1, y1) => {
    ext.x0 = Math.min(ext.x0, x0)
    ext.y0 = Math.min(ext.y0, y0)
    ext.x1 = Math.max(ext.x1, x1)
    ext.y1 = Math.max(ext.y1, y1)
  }

  const out = { bands: [], grid: [], marks: [], arrows: [], spans: [], labels: [], heads: [] }

  for (const l of lanes.values()) {
    const y = laneY(l)
    out.bands.push(
      `<g class="tl-lane dg-t-${l.tone}${l.i % 2 ? '' : ' tl-odd'}"><rect x="0" y="${y}" width="@W" height="${LANE_H}"/>` +
        `<text class="tl-ll" x="8" y="${y + (l.sub ? 24 : 32)}">${tspans(l.label ?? l.id)}</text>` +
        (l.sub ? `<text class="tl-ls" x="8" y="${y + 40}">${tspans(l.sub)}</text>` : '') +
        `</g>`,
    )
  }

  for (const s of spans) {
    const y = laneY(s.l)
    const [x0, x1] = [X(s.from), X(s.to)]
    place({ x0, y0: y + BAR_Y, x1, y1: y + BAR_Y + BAR_H }, s.what, 0, false)
    grow(x0, y, x1, y + LANE_H)
    let text = ''
    if (s.label) {
      const w = bold(String(s.label), 11.5) + 10
      if (w > x1 - x0) throw new Error(`${s.what}: label needs ~${Math.ceil(w)} px but the span is ${Math.round(x1 - x0)} px (widen it, raise scale, or move the words to dur)`)
      text = `<text class="tl-sl" x="${r((x0 + x1) / 2)}" y="${y + BAR_Y + 15}">${tspans(s.label)}</text>`
    }
    out.spans.push(
      `<g class="dg-n tl-span dg-t-${s.tone}${s.dash ? ' dg-sh-note' : ''}"><rect class="dg-s" x="${r(x0)}" y="${y + BAR_Y}" width="${r(x1 - x0)}" height="${BAR_H}" rx="4"/>${text}</g>`,
    )
    if (s.dur) {
      const w = plainWidth(String(s.dur), 11)
      const cx = (x0 + x1) / 2
      place({ x0: cx - w / 2, y0: y + SUB_Y - 10, x1: cx + w / 2, y1: y + SUB_Y + 3 }, `${s.what} dur`)
      grow(cx - w / 2, y, cx + w / 2, y + LANE_H)
      out.labels.push(`<text class="tl-dur" x="${r(cx)}" y="${y + SUB_Y}">${tspans(s.dur)}</text>`)
    }
  }

  for (const e of events) {
    const y = laneY(e.l)
    const x = X(e.at)
    place({ x0: x - 7, y0: y + MID - 7, x1: x + 7, y1: y + MID + 7 }, e.what, 2, false)
    out.spans.push(`<g class="dg-n dg-t-${e.tone}"><path class="dg-s" d="M${r(x)},${y + MID - 7} L${r(x + 7)},${y + MID} L${r(x)},${y + MID + 7} L${r(x - 7)},${y + MID} Z"/></g>`)
    if (e.label) {
      const w = plainWidth(String(e.label), 11)
      place({ x0: x - w / 2, y0: y + SUB_Y - 10, x1: x + w / 2, y1: y + SUB_Y + 3 }, `${e.what} label`)
      grow(x - w / 2, y, x + w / 2, y + LANE_H)
      out.labels.push(`<text class="tl-dur tl-ev dg-t-${e.tone}" x="${r(x)}" y="${y + SUB_Y}">${tspans(e.label)}</text>`)
    }
  }

  for (const a of arrows) {
    const down = a.to.i > a.from.i
    const ya = laneY(a.from) + MID
    const yb = laneY(a.to) + MID + (down ? -(BAR_H / 2 + 1) : BAR_H / 2 + 1)
    const [xa, xb] = [X(a.t0), X(a.t1)]
    // an arrow passing a lane must not run through that lane's spans
    for (const l of lanes.values()) {
      if (l.i <= Math.min(a.from.i, a.to.i) || l.i >= Math.max(a.from.i, a.to.i)) continue
      const yl = laneY(l) + MID
      const xl = xa + ((xb - xa) * (yl - ya)) / (yb - ya)
      const hit = spans.find((s) => s.l === l && X(s.from) - 2 < xl && xl < X(s.to) + 2)
      if (hit) throw new Error(`${a.what} runs through ${hit.what} in lane ${l.id} (move it in time or reorder the lanes)`)
    }
    const tip = [xb, yb]
    const base = [xa, ya]
    const len = Math.hypot(xb - xa, yb - ya) || 1
    const end = [xb - ((xb - xa) / len) * (ARROW - 1), yb - ((yb - ya) / len) * (ARROW - 1)]
    out.arrows.push(
      `<g class="dg-e dg-t-${a.tone}${a.dash ? ' dg-dash' : ''}"><path d="M${r(xa)},${r(ya)} L${r(end[0])},${r(end[1])}"/>${arrowHead(tip, base, 'dg-ah')}</g>`,
    )
    if (a.label) {
      const w = plainWidth(String(a.label), 11) + 8
      const [mx, my] = [(xa + xb) / 2, (ya + yb) / 2]
      const left = a.side === 'left'
      const lx0 = left ? mx - 4 - w : mx + 4
      place({ x0: lx0, y0: my - 8, x1: lx0 + w, y1: my + 8 }, `${a.what} label`)
      grow(lx0, my - 8, lx0 + w, my + 8)
      out.heads.push(
        `<g class="dg-el dg-t-${a.tone}"><rect x="${r(lx0)}" y="${r(my - 8)}" width="${r(w)}" height="16" rx="3"/>` +
          `<text class="tl-al" x="${r(lx0 + w / 2)}" y="${r(my + 4)}">${tspans(a.label)}</text></g>`,
      )
    }
  }

  for (const m of marks) {
    const x = X(m.at)
    // a mark's rule runs behind bars but would cut through text
    const cut = boxes.find((b) => b.text && b.x0 - 1 < x && x < b.x1 + 1)
    if (cut) throw new Error(`timeline: ${m.what}'s rule runs through ${cut.what} (move one, or put the words inside a span)`)
    const ly = m.row * ROW_H
    out.marks.push(`<g class="tl-mark dg-t-${m.tone}"><path d="M${r(x)},${ly + 18} V${bottom + 4}"/></g>`)
    out.heads.push(
      `<g class="tl-mk dg-t-${m.tone}"><rect x="${r(m.x0)}" y="${ly}" width="${r(m.w)}" height="18" rx="9"/>` +
        `<text x="${r(m.x0 + m.w / 2)}" y="${ly + 13}">${tspans(m.label)}</text></g>`,
    )
    grow(m.x0, ly, m.x0 + m.w, bottom)
  }

  let axis = bottom
  if (ticks.length) {
    axis = bottom + AXIS_H
    out.grid.push(`<path class="tl-axis" d="M${r(X(t0))},${bottom + 0.5} H${r(Math.max(...ticks.map((t) => X(t.at))))}"/>`)
    for (const t of ticks) {
      const x = X(t.at)
      out.grid.push(`<path class="tl-axis" d="M${r(x)},${bottom} v5"/>`)
      if (t.label) {
        const w = plainWidth(String(t.label), 11)
        place({ x0: x - w / 2, y0: bottom + 8, x1: x + w / 2, y1: bottom + 22 }, `tick ${JSON.stringify(t.label)}`)
        grow(x - w / 2, bottom, x + w / 2, axis)
        out.labels.push(`<text class="tl-tick" x="${r(x)}" y="${bottom + 19}">${tspans(t.label)}</text>`)
      }
    }
  }
  grow(0, 0, 0, axis)

  const vx = ext.x0 - PAD
  const vy = ext.y0 - PAD
  const vw = ext.x1 - ext.x0 + PAD * 2
  const vh = ext.y1 - ext.y0 + PAD
  const title = String(spec.title ?? spec.caption).replace(/`/g, '')
  const bandW = r(ext.x1 + PAD / 2)
  return {
    width: vw,
    svg:
      `<svg class="dg tl" viewBox="${r(vx)} ${r(vy)} ${r(vw)} ${r(vh)}" width="${Math.round(vw)}" height="${Math.round(vh)}" role="img" aria-label="${esc(title)}">` +
      out.bands.join('').replaceAll('@W', bandW) +
      out.grid.join('') +
      out.marks.join('') +
      out.arrows.join('') +
      out.spans.join('') +
      out.labels.join('') +
      out.heads.join('') +
      `</svg>`,
  }
}
