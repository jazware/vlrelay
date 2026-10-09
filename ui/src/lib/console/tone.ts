import type { Case, Consumer, HostStatus, Severity } from '../api'

// One status vocabulary for the whole console: the tone each status word gets, and so its glyph
// (● ok, ▲ warn, ■ err, ◆ info, ○ idle). Chips, glyphs, banners and badges read these maps, so
// a host, a case or a consumer looks the same on every page and in every drawer.

export type Tone = 'ok' | 'warn' | 'err' | 'info' | 'idle'
export const GLYPH: Record<Tone, string> = { ok: '●', warn: '▲', err: '■', info: '◆', idle: '○' }

// backpressure is the relay's own state, not the host's: info, so it never reads as a throttle
const HOST: Record<HostStatus, Tone> = { connected: 'ok', idle: 'idle', backoff: 'warn', offline: 'err', throttled: 'warn', backpressure: 'info', suspended: 'err', banned: 'err', alias: 'idle' }
export const HOST_TITLE: Partial<Record<HostStatus, string>> = {
  throttled: 'Held at its own limits: its tier, a domain rule or an operator throttle',
  backpressure: 'Paused by the relay, which is behind: not this host’s limits',
  alias: 'Another name for a PDS the relay reads under its own name: no socket of its own',
}
export const hostTone = (s: string): Tone => HOST[s as HostStatus] ?? 'idle'

const SEV: Record<Severity, Tone> = { critical: 'err', high: 'err', warn: 'warn', info: 'info' }
export const sevTone = (s: Severity): Tone => SEV[s] ?? 'info'

/** Open cases take their severity's tone (at least warn); acknowledged ones are being looked at; closed ones are idle. */
export const caseTone = (c: Pick<Case, 'status' | 'severity'>): Tone => (c.status === 'open' ? (sevTone(c.severity) === 'err' ? 'err' : 'warn') : c.status === 'acknowledged' ? 'info' : 'idle')

const ACCOUNT: Record<string, Tone> = { active: 'ok', takendown: 'err', throttled: 'warn' }
export const accountTone = (s: string): Tone => ACCOUNT[s] ?? 'idle'

/** A consumer: falling behind, replaying, or live. */
export const consumerState = (c: Consumer, slow: boolean): { tone: Tone; label: string } =>
  slow ? { tone: 'warn', label: 'slow' } : c.backfilling ? { tone: 'info', label: 'replaying' } : { tone: 'ok', label: 'live' }
