// Console formatting on top of lib/format.ts. Everything returns "—" for unknown values.

import { relTime } from '../format'

export { fmtBytes, fmtCount, fmtNum, fmtSi, relTime as ago, short } from '../format'

/** How long since: "4m", "1h" (relTime without its "ago"). */
export const since = (ms: number) => relTime(ms).replace(/ ago$/, '')

/** A duration: "42s", "3m 10s", "2h 5m", "4d 1h". */
export function dur(ms: number | undefined | null): string {
  if (ms === undefined || ms === null || !isFinite(ms)) return '—'
  const s = Math.max(0, Math.round(ms / 1000))
  if (s < 60) return `${s}s`
  if (s < 3600) return `${Math.floor(s / 60)}m ${s % 60}s`
  if (s < 86400) return `${Math.floor(s / 3600)}h ${Math.floor((s % 3600) / 60)}m`
  return `${Math.floor(s / 86400)}d ${Math.floor((s % 86400) / 3600)}h`
}

/** Milliseconds as a latency: "840 µs", "4.2 ms", "152 ms", "1.20 s", then a duration. */
export function fmtMs(ms: number | undefined | null): string {
  if (ms === undefined || ms === null || !isFinite(ms)) return '—'
  if (ms === 0) return '0 ms'
  if (ms < 1) return `${(ms * 1000).toFixed(0)} µs`
  if (ms < 10) return `${ms.toFixed(1)} ms`
  if (ms < 1000) return `${Math.round(ms)} ms`
  if (ms < 60_000) return `${(ms / 1000).toFixed(ms < 10_000 ? 2 : 1)} s`
  return dur(ms)
}

/** Microseconds (the quorum log's quantiles) as a latency. */
export const fmtUs = (us: number | undefined | null) => (us === undefined || us === null ? '—' : fmtMs(us / 1000))

export const fmtPct = (v: number | undefined, digits = 0) => (v === undefined || !isFinite(v) ? '—' : `${v.toFixed(digits)}%`)

/** A ratio (0..1) as a percent, finer near zero. */
export const fmtRatio = (r: number) => (!isFinite(r) ? '—' : r === 0 ? '0%' : r < 0.001 ? '<0.1%' : `${(r * 100).toFixed(r < 0.1 ? 2 : 0)}%`)

export const clock = (ms: number) => new Date(ms).toLocaleTimeString('en-GB', { hour12: false })

export const dt = (ms: number) => new Date(ms).toLocaleString('en-US', { month: 'short', day: 'numeric', hour: '2-digit', minute: '2-digit', hour12: false })

/** "did:plc:abcdefgh…wxyz". */
export const shortDid = (d: string) => (d.length > 22 ? `${d.slice(0, 14)}…${d.slice(-4)}` : d)

export const plural = (n: number, w: string, p?: string) => `${n.toLocaleString('en-US')} ${n === 1 ? w : (p ?? `${w}s`)}`

/** A firehose seq, grouped. */
export const seqS = (s: number | undefined | null) => (s === undefined || s === null || !isFinite(s) ? '—' : Math.floor(s).toLocaleString('en-US'))
