import { useEffect, useSyncExternalStore } from 'react'
import { basic, getAdminToken, setAdminToken, type Change, type ChangeHello, type ChangeResync } from '../api'

// The admin change feed (docs/admin-api.md, "Change feed"): one Server-Sent Events stream saying
// what changed. Read with fetch, so the token and Last-Event-ID go along. Each change goes to
// `onChange` (cache.ts invalidates its keys); a resync, or a reconnect that couldn't resume, to
// `onLost`. A relay without the feed answers 404 and the console polls until a reload.

/** live: events arrive. reconnecting: the stream dropped (or hasn't opened yet), polls stand in. polling: this relay has no feed. */
export type FeedStatus = 'live' | 'reconnecting' | 'polling'
export type FeedState = {
  status: FeedStatus
  /** The node serving the feed. */
  node?: string
  /** The last message or ping. */
  lastAt?: number
  /** Why it isn't live. */
  why?: string
  /** Changes received since the page loaded. */
  changes: number
}

let state: FeedState = { status: 'reconnecting', changes: 0 }
const listeners = new Set<() => void>()
function set(p: Partial<FeedState>) {
  state = { ...state, ...p }
  listeners.forEach((l) => l())
}
const subscribe = (l: () => void) => {
  listeners.add(l)
  return () => {
    listeners.delete(l)
  }
}
export const getFeed = () => state
export const useFeed = () => useSyncExternalStore(subscribe, getFeed)
/** Only re-renders when live flips. */
export const useFeedLive = () => useSyncExternalStore(subscribe, () => state.status === 'live')

export type FeedHandlers = {
  onChange: (c: Change) => void
  /** Changes may have been missed: a resync's reason, or `reconnected` (back after a drop with no cursor to resume from). */
  onLost: (why: string) => void
}

const PING_DEAD_MS = 40_000
// a hidden tab gets its timers and stream reads late, in bursts: only a much longer silence there means a dead stream
const PING_DEAD_HIDDEN_MS = 180_000
const MAX_BACKOFF_MS = 15_000

/** One SSE message as the stream frames it. */
type Msg = { event: string; id?: string; data: string }

/** Splits a text stream into SSE messages (the WHATWG rules: `field: value` lines, a blank line ends one, `:` lines are comments). */
export function sseParser(onMsg: (m: Msg) => void, onComment: () => void) {
  let buf = ''
  let event = ''
  let id: string | undefined
  let data: string[] = []
  return (chunk: string) => {
    buf += chunk
    let i: number
    while ((i = buf.search(/\r\n|\r|\n/)) >= 0) {
      // a lone \r at the end may be half of a \r\n
      if (buf[i] === '\r' && i === buf.length - 1) break
      const line = buf.slice(0, i)
      buf = buf.slice(i + (buf[i] === '\r' && buf[i + 1] === '\n' ? 2 : 1))
      if (line === '') {
        if (data.length || event) onMsg({ event: event || 'message', id, data: data.join('\n') })
        event = ''
        id = undefined
        data = []
        continue
      }
      if (line.startsWith(':')) {
        onComment()
        continue
      }
      const c = line.indexOf(':')
      const field = c < 0 ? line : line.slice(0, c)
      const value = c < 0 ? '' : line.slice(c + 1).replace(/^ /, '')
      if (field === 'event') event = value
      else if (field === 'data') data.push(value)
      else if (field === 'id') id = value
    }
  }
}

/** Keeps the feed open while the console is unlocked (`unlock` changing reconnects). */
export function useChangeFeed(unlock: string | null, h: FeedHandlers) {
  useEffect(() => {
    if (!unlock) return
    return connect(h)
    // the handlers are module functions
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [unlock])
}

function connect(h: FeedHandlers): () => void {
  let stopped = false
  let ctl: AbortController | undefined
  let retry: ReturnType<typeof setTimeout> | undefined
  let cursor: string | undefined
  let attempt = 0
  let wasLive = false

  const later = (why: string) => {
    if (stopped) return
    const ms = Math.min(MAX_BACKOFF_MS, 1000 * 2 ** attempt) * (0.75 + Math.random() * 0.5)
    attempt++
    set({ status: 'reconnecting', why })
    retry = setTimeout(open, ms)
  }

  async function open() {
    if (stopped) return
    const c = (ctl = new AbortController())
    const token = getAdminToken()
    const headers: Record<string, string> = { Accept: 'text/event-stream' }
    if (token) headers.Authorization = basic(token)
    const sentCursor = cursor
    if (cursor) headers['Last-Event-ID'] = cursor
    let r: Response
    try {
      r = await fetch('/admin/api/changes', { headers, signal: c.signal, cache: 'no-store' })
    } catch (e) {
      return later(e instanceof Error ? e.message : String(e))
    }
    if (r.status === 404 || r.status === 501) return set({ status: 'polling', why: 'this relay has no change feed' })
    if (r.status === 401) {
      if (token) setAdminToken(null)
      return later('signed out')
    }
    if (!r.ok) return later(r.status === 503 ? 'the node serves as many feeds as it allows' : `HTTP ${r.status}`)
    if (!r.body || !/text\/event-stream/.test(r.headers.get('content-type') ?? '')) return set({ status: 'polling', why: 'the answer to /admin/api/changes is not an event stream' })
    let last = Date.now()
    let why = 'the stream ended'
    const parse = sseParser(
      (m) => {
        last = Date.now()
        if (m.id) cursor = m.id
        let data: unknown
        try {
          data = JSON.parse(m.data)
        } catch {
          return
        }
        if (m.event === 'hello') {
          const hello = data as ChangeHello
          attempt = 0
          // with a cursor the feed itself says what's missed (a resync); a reconnect without one can't know
          if (wasLive && !sentCursor && !hello.resumed) h.onLost('reconnected')
          wasLive = true
          set({ status: 'live', node: hello.node, lastAt: last, why: undefined })
        } else if (m.event === 'change') {
          h.onChange(data as Change)
          set({ lastAt: last, changes: state.changes + 1 })
        } else if (m.event === 'resync') {
          h.onLost((data as ChangeResync).reason)
          set({ lastAt: last })
        }
      },
      () => {
        last = Date.now()
        set({ lastAt: last })
      },
    )
    const watchdog = setInterval(() => {
      if (Date.now() - last < (document.hidden ? PING_DEAD_HIDDEN_MS : PING_DEAD_MS)) return
      why = 'no ping for 40 s'
      c.abort()
    }, 5000)
    try {
      const reader = r.body.pipeThrough(new TextDecoderStream()).getReader()
      for (;;) {
        const { value, done } = await reader.read()
        if (done) break
        parse(value)
      }
    } catch {
      /* aborted, or the connection dropped */
    } finally {
      clearInterval(watchdog)
    }
    later(why)
  }

  void open()
  return () => {
    stopped = true
    clearTimeout(retry)
    ctl?.abort()
    set({ status: 'reconnecting', why: undefined })
  }
}
