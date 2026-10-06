import { decode, decodeFirst, type TagDecoder } from 'cborg'
import { useSyncExternalStore } from 'react'
import { getLive } from './live'

// The tail: this relay's own com.atproto.sync.subscribeRepos over a websocket, decoded in the
// browser. Every node sends the same seqs, so the node serving the console is enough. A relay
// sends far more than anyone can read, so the tail shows a sample (SAMPLE_PER_SEC rows a
// second) unless a filter is set: then every decoded frame that matches is shown, which is how
// to follow one DID or one collection at full rate. Decoding itself is capped at DECODE_PER_SEC;
// past that, frames are counted and skipped. While paused, rows wait in a buffer. The console
// shows up in the consumer list as one more connection, without a cursor.

export type FhKind = 'commit' | 'identity' | 'account' | 'sync'
export type FhOp = { action: 'create' | 'update' | 'delete'; path: string; cid?: string }
export type FhEvent = {
  id: number
  seq: number
  kind: FhKind
  did: string
  /** When the frame arrived here. */
  at: number
  /** The event's own `time`. */
  time?: string
  ops: FhOp[]
  handle?: string
  active?: boolean
  status?: string
  rev?: string
  blocksBytes?: number
  frameBytes: number
  body: Record<string, unknown>
}

export type FhState = {
  status: 'idle' | 'connecting' | 'open' | 'closed'
  events: FhEvent[]
  /** Rows that arrived while paused, shown on resume. */
  held: number
  /** Frames a second arriving and decoded, over the last few seconds. */
  rate: number
  decodedRate: number
  error?: string
  version: number
}

const MAX = 600
const SAMPLE_PER_SEC = 6
const DECODE_PER_SEC = 3000
const B32 = 'abcdefghijklmnopqrstuvwxyz234567'
function base32(bytes: Uint8Array): string {
  let out = ''
  let bits = 0
  let v = 0
  for (const b of bytes) {
    v = (v << 8) | b
    bits += 8
    while (bits >= 5) {
      out += B32[(v >>> (bits - 5)) & 31]
      bits -= 5
    }
  }
  if (bits > 0) out += B32[(v << (5 - bits)) & 31]
  return out
}
// DAG-CBOR links: tag 42 over the CID bytes behind a 0x00 multibase prefix
const tags: Record<number, TagDecoder> = { 42: (inner) => `b${base32((inner() as Uint8Array).subarray(1))}` }

let st: FhState = { status: 'idle', events: [], held: 0, rate: 0, decodedRate: 0, version: 0 }
let heldQ: FhEvent[] = []
let seqId = 0
let ws: WebSocket | undefined
let retry = 1000
let retryTimer: ReturnType<typeof setTimeout> | undefined
const subs = new Set<() => void>()
const emit = (p: Partial<FhState>) => {
  st = { ...st, ...p, version: st.version + 1 }
  subs.forEach((l) => l())
}

/** What the tail shows: everything when a matcher is set, else a sample. */
let matcher: ((e: FhEvent) => boolean) | undefined
export function setTailFilter(m: ((e: FhEvent) => boolean) | undefined) {
  matcher = m
}

const handles = new Map<string, string>()
export const handleOf = (did: string) => handles.get(did)

function parseFrame(bytes: Uint8Array): FhEvent | undefined {
  const [header, rest] = decodeFirst(bytes, { tags }) as [{ op: number; t?: string }, Uint8Array]
  if (header.op !== 1 || !header.t) return undefined
  const body = decode(rest, { tags, allowUndefined: true }) as Record<string, any>
  const kind = header.t.replace(/^#/, '') as FhKind
  if (!['commit', 'identity', 'account', 'sync'].includes(kind)) return undefined
  const did: string = body.repo ?? body.did ?? ''
  const shown: Record<string, unknown> = {}
  for (const [k, v] of Object.entries(body)) shown[k] = v instanceof Uint8Array ? `<${v.length} bytes>` : typeof v === 'bigint' ? v.toString() : v
  if (kind === 'identity' && body.handle) handles.set(did, body.handle)
  return {
    id: ++seqId,
    seq: Number(body.seq),
    kind,
    did,
    at: Date.now(),
    time: body.time,
    ops: Array.isArray(body.ops) ? body.ops.map((o: any) => ({ action: o.action, path: o.path, cid: o.cid ?? undefined })) : [],
    handle: body.handle,
    active: body.active,
    status: body.status,
    rev: body.rev,
    blocksBytes: body.blocks instanceof Uint8Array ? body.blocks.length : undefined,
    frameBytes: bytes.length,
    body: shown,
  }
}

// per-second budgets and rates
let secStart = 0
let arrived = 0
let decoded = 0
let sampled = 0
let rates: { a: number; d: number }[] = []

function rollSecond(now: number) {
  if (now - secStart < 1000) return
  rates = [...rates.slice(-4), { a: arrived, d: decoded }]
  secStart = now
  arrived = decoded = sampled = 0
  const n = rates.length || 1
  st = { ...st, rate: rates.reduce((x, r) => x + r.a, 0) / n, decodedRate: rates.reduce((x, r) => x + r.d, 0) / n }
}

let pending: FhEvent[] = []
let flushTimer: ReturnType<typeof setTimeout> | undefined
function flush() {
  flushTimer = undefined
  if (!pending.length) {
    emit({})
    return
  }
  const evs = pending
  pending = []
  if (getLive().paused) {
    heldQ.push(...evs)
    if (heldQ.length > MAX) heldQ = heldQ.slice(-MAX)
    emit({ held: heldQ.length })
    return
  }
  emit({ events: [...st.events, ...evs].slice(-MAX) })
}

function onFrame(buf: ArrayBuffer) {
  const now = Date.now()
  rollSecond(now)
  arrived++
  if (decoded >= DECODE_PER_SEC) return
  decoded++
  let e: FhEvent | undefined
  try {
    e = parseFrame(new Uint8Array(buf))
  } catch (err) {
    st = { ...st, error: `Undecodable frame: ${err instanceof Error ? err.message : err}` }
    return
  }
  if (!e) return
  const keep = matcher ? matcher(e) : sampled < SAMPLE_PER_SEC && Math.random() < SAMPLE_PER_SEC / Math.max(SAMPLE_PER_SEC, arrivalEstimate())
  if (!keep) return
  sampled++
  pending.push(e)
  // batch renders: a busy relay sends thousands of frames a second
  flushTimer ??= setTimeout(flush, 250)
}

const arrivalEstimate = () => (st.rate > 0 ? st.rate : 50)

function connect() {
  clearTimeout(retryTimer)
  const proto = location.protocol === 'https:' ? 'wss:' : 'ws:'
  const sock = new WebSocket(`${proto}//${location.host}/xrpc/com.atproto.sync.subscribeRepos`)
  sock.binaryType = 'arraybuffer'
  ws = sock
  emit({ status: 'connecting', error: undefined })
  sock.onopen = () => {
    retry = 1000
    emit({ status: 'open' })
  }
  sock.onmessage = (m) => onFrame(m.data as ArrayBuffer)
  sock.onerror = () => {
    /* onclose follows with the code */
  }
  sock.onclose = (ev) => {
    if (ws !== sock) return
    ws = undefined
    emit({ status: 'closed', error: ev.reason || (ev.code !== 1000 ? `closed (${ev.code})` : undefined) })
    if (subs.size) {
      retryTimer = setTimeout(connect, retry)
      retry = Math.min(retry * 2, 30_000)
    }
  }
}

/** Shows what arrived while paused. Called by the shell when live updates resume. */
export function releaseHeld() {
  if (!heldQ.length) return
  const evs = heldQ
  heldQ = []
  emit({ events: [...st.events, ...evs].slice(-MAX), held: 0 })
}

// the rates tick even when no row is kept
let rateTimer: ReturnType<typeof setInterval> | undefined

function subscribe(l: () => void) {
  subs.add(l)
  if (subs.size === 1 && !ws) {
    connect()
    rateTimer = setInterval(() => {
      rollSecond(Date.now())
      if (!flushTimer) emit({})
    }, 1000)
  }
  return () => {
    subs.delete(l)
    if (!subs.size) {
      clearTimeout(retryTimer)
      clearInterval(rateTimer)
      const s = ws
      ws = undefined
      s?.close(1000)
      st = { ...st, status: 'idle' }
    }
  }
}

export const useFirehose = () => useSyncExternalStore(subscribe, () => st)
export const findEvent = (id: number) => st.events.find((e) => e.id === id)
