import { useSyncExternalStore } from 'react'

// The policy page's one draft. The relay keeps the policy as one versioned JSON document; every
// edit on the page (a tier cell, a knob, a spam threshold) lands here, and a save sends the whole
// document with the version it was edited from. It lives outside the page so a draft survives
// opening a drawer or leaving the section and coming back.

export type Json = Record<string, unknown>
/** `full`: the engine's whole document (PUT policy/full). `wire`: the older tier form (PUT policy). */
export type PolicyMode = 'full' | 'wire'
export type PolicyBase = { mode: PolicyMode; version: number; updatedAtMs: number; updatedBy: string; note?: string; body: Json }
export type Change = { path: string; from: unknown; to: unknown }

const isObj = (x: unknown): x is Json => typeof x === 'object' && x !== null && !Array.isArray(x)
const same = (a: unknown, b: unknown) => JSON.stringify(a) === JSON.stringify(b)

/** Every changed leaf, keys sorted, the way the relay's audit log lists them (`admin::diff_json`). */
export function diff(a: unknown, b: unknown, path = '', out: Change[] = []): Change[] {
  if (isObj(a) && isObj(b)) {
    for (const k of [...new Set([...Object.keys(a), ...Object.keys(b)])].sort()) diff(a[k], b[k], path ? `${path}.${k}` : k, out)
  } else if (!same(a, b)) out.push({ path, from: a, to: b })
  return out
}

export const getIn = (o: unknown, path: string): unknown => path.split('.').reduce<unknown>((a, k) => (isObj(a) ? a[k] : undefined), o)

export function setIn(o: Json, path: string, v: unknown): Json {
  const [k, ...rest] = path.split('.')
  return { ...o, [k]: rest.length ? setIn(isObj(o[k]) ? (o[k] as Json) : {}, rest.join('.'), v) : v }
}

function unset(o: Json, path: string): Json {
  const [k, ...rest] = path.split('.')
  if (!rest.length) {
    const { [k]: _, ...keep } = o
    return keep
  }
  return isObj(o[k]) ? { ...o, [k]: unset(o[k] as Json, rest.join('.')) } : o
}

/** A value as the audit log writes it: JSON, cut at 80 characters. */
export function showVal(v: unknown): string {
  if (v === undefined) return '—'
  const s = JSON.stringify(v)
  return s.length > 80 ? `${s.slice(0, 80)}…` : s
}

// ---------------------------------------------------------------- the store

type State = { base?: PolicyBase; body?: Json; newer?: PolicyBase }
let st: State = {}
const subs = new Set<() => void>()
const emit = () => subs.forEach((l) => l())
const subscribe = (l: () => void) => {
  subs.add(l)
  return () => {
    subs.delete(l)
  }
}
export const useDraft = () => useSyncExternalStore(subscribe, () => st)
export const getDraft = () => st

export const changesOf = (s: State = st): Change[] => (s.base && s.body ? diff(s.base.body, s.body) : [])

/**
 * A poll's answer. An untouched draft follows the server; an edited one keeps its edits and
 * remembers that a newer version exists, so the page can say so before a save gets a 409.
 */
export function serverSaw(doc: PolicyBase) {
  const b = st.base
  if (!b || b.mode !== doc.mode || (doc.version > b.version && !changesOf().length)) st = { base: doc, body: doc.body }
  else if (doc.version > b.version) st = { ...st, newer: doc }
  else if (doc.version === b.version && st.newer) st = { ...st, newer: undefined }
  else return
  emit()
}

export function setField(path: string, v: unknown) {
  if (!st.body || !st.base) return
  st = { ...st, body: setIn(st.body, path, v) }
  emit()
}

/** Replaces the whole draft body (the JSON editor). */
export function setBody(body: Json) {
  if (!st.base) return
  st = { ...st, body }
  emit()
}

export function discard() {
  if (!st.base) return
  st = { base: st.newer ?? st.base, body: (st.newer ?? st.base).body }
  emit()
}

/** After a save: the saved version is the new base. */
export function saved(doc: PolicyBase) {
  st = { base: doc, body: doc.body }
  emit()
}

/**
 * Moves the draft onto `latest` (after a 409): my changed leaves on top of the newer version.
 * Returns the leaves someone else changed too, where mine now wins.
 */
export function rebase(latest: PolicyBase): string[] {
  const mine = changesOf()
  let body = latest.body
  const clash: string[] = []
  for (const c of mine) {
    if (!same(getIn(latest.body, c.path), c.from)) clash.push(c.path)
    body = c.to === undefined ? unset(body, c.path) : setIn(body, c.path, c.to)
  }
  st = { base: latest, body }
  emit()
  return clash
}

// ---------------------------------------------------------------- undo

/** `path: old → new` from an audit entry, with the old value parsed back (undefined: it was absent). */
export type Undo = { path: string; to: unknown; from: unknown; ok: boolean; why?: string }

function parseVal(s: string): { ok: boolean; v?: unknown } {
  if (s === '—') return { ok: true, v: undefined }
  try {
    return { ok: true, v: JSON.parse(s) }
  } catch {
    return { ok: false }
  }
}

/**
 * The leaves an audit entry changed, each with the value to put back. A value the audit cut at
 * 80 characters can't be put back from the log, so that leaf isn't undoable.
 */
export function undoOf(changes: string[], body: Json): Undo[] {
  return changes.map((c) => {
    const at = c.indexOf(': ')
    const arrow = c.lastIndexOf(' → ')
    if (at < 0 || arrow < at) return { path: c, from: undefined, to: undefined, ok: false, why: 'not a leaf change' }
    const path = c.slice(0, at)
    const old = parseVal(c.slice(at + 2, arrow))
    const now = parseVal(c.slice(arrow + 3))
    if (!old.ok) return { path, from: undefined, to: undefined, ok: false, why: 'the log cut the old value short' }
    const parent = path.includes('.') ? getIn(body, path.slice(0, path.lastIndexOf('.'))) : body
    if (!isObj(parent)) return { path, from: undefined, to: old.v, ok: false, why: 'the document has no such field now' }
    return { path, from: getIn(body, path), to: old.v, ok: true, why: now.ok && !same(getIn(body, path), now.v) ? 'changed again since' : undefined }
  })
}

/** The document with each undoable leaf put back. */
export function applyUndo(body: Json, undo: Undo[]): Json {
  let b = body
  for (const u of undo) if (u.ok) b = u.to === undefined ? unset(b, u.path) : setIn(b, u.path, u.to)
  return b
}
