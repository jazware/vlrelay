export function fmtTime(s?: string | number | null): string {
  if (s === undefined || s === null || s === '') return '—'
  const d = new Date(s)
  if (isNaN(d.getTime())) return String(s)
  return d.toLocaleString(undefined, { dateStyle: 'medium', timeStyle: 'short' })
}

export function relTime(ms: number): string {
  // 0 is the API's "never"
  if (!ms) return '—'
  const d = Math.round((Date.now() - ms) / 1000)
  const abs = Math.abs(d)
  const unit = abs < 60 ? [abs, 's'] : abs < 3600 ? [Math.round(abs / 60), 'm'] : abs < 86400 ? [Math.round(abs / 3600), 'h'] : [Math.round(abs / 86400), 'd']
  return d >= 0 ? `${unit[0]}${unit[1]} ago` : `in ${unit[0]}${unit[1]}`
}

export function fmtBytes(n: number): string {
  if (!isFinite(n)) return '—'
  const u = ['B', 'KiB', 'MiB', 'GiB', 'TiB']
  let i = 0
  while (Math.abs(n) >= 1024 && i < u.length - 1) {
    n /= 1024
    i++
  }
  return `${n >= 100 || i === 0 ? n.toFixed(0) : n.toFixed(1)} ${u[i]}`
}

export function fmtNum(n: number | undefined | null, digits = 0): string {
  if (n === undefined || n === null || !isFinite(n)) return '—'
  return n.toLocaleString(undefined, { maximumFractionDigits: digits, minimumFractionDigits: 0 })
}

/** Compact rate/latency formatting for chart axes and tiles. */
export function fmtSi(n: number): string {
  if (!isFinite(n)) return '—'
  const a = Math.abs(n)
  if (a >= 1e9) return `${(n / 1e9).toFixed(1)}G`
  if (a >= 1e6) return `${(n / 1e6).toFixed(1)}M`
  if (a >= 1e4) return `${(n / 1e3).toFixed(0)}k`
  if (a >= 1e3) return `${(n / 1e3).toFixed(1)}k`
  if (a >= 100) return n.toFixed(0)
  if (a >= 10) return n.toFixed(1)
  return n.toFixed(2)
}

export function fmtSecs(s: number): string {
  if (!isFinite(s)) return '—'
  if (s === 0) return '0'
  if (s < 1e-3) return `${(s * 1e6).toFixed(0)} µs`
  if (s < 1) return `${(s * 1e3).toFixed(s < 0.01 ? 1 : 0)} ms`
  return `${s.toFixed(2)} s`
}

/** "abcd…wxyz" for long identifiers. */
export function short(s: string, keep = 8): string {
  return s.length <= keep * 2 + 1 ? s : `${s.slice(0, keep)}…${s.slice(-keep)}`
}

/** Firehose seqs are unix_micros × 256 + writer (sent as strings: > 2^53). */
export function seqMillis(seq?: string | null): number | undefined {
  if (!seq || seq === '0') return undefined
  try {
    return Number(BigInt(seq) / 256000n)
  } catch {
    return undefined
  }
}

export function seqWriter(seq?: string | null): number | undefined {
  if (!seq) return undefined
  try {
    return Number(BigInt(seq) % 256n)
  } catch {
    return undefined
  }
}

/** A lag in ms as ms, s, min or h. */
export const fmtLag = (ms: number) =>
  ms < 1000 ? `${ms.toFixed(0)} ms` : ms < 120_000 ? `${(ms / 1000).toFixed(1)} s` : ms < 7_200_000 ? `${(ms / 60_000).toFixed(0)} min` : `${(ms / 3_600_000).toFixed(1)} h`

/** Over a minute is bad, over half a second worth a look. */
export const lagClass = (ms: number) => (ms > 60_000 ? 'err-hi' : ms > 500 ? 'err-mid' : '')
