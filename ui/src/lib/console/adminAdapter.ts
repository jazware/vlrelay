import {
  api,
  ApiError,
  enc,
  publicStats,
  type Case,
  type ClusterView,
  type Consumer,
  type FullPolicyDoc,
  type HostAction,
  type HostDetail,
  type HostList,
  type HostRow,
  type HostStatus,
  type Overview,
  type PipelineView,
  type PolicyDoc,
  type QuorumView,
  type SettingsView,
} from '../api'
import { isUnsupported } from './live'

// The console's one way into the admin API (docs/admin-api.md). Pages call these instead of
// api() so the endpoint list lives in one file, and so the panels the console design assumes but
// the relay doesn't serve yet degrade the same way everywhere: those answer
// { supported: false, endpoint } without a request (no 404s in the browser console), and a page
// shows a "needs a newer vlRelay" placeholder. CONSOLE.md lists them for the backend lane; when
// one lands, replace its `missing(...)` with the call.

export type Optional<T> = { supported: true; data: T } | { supported: false; endpoint: string; why: string }

const missing = <T>(endpoint: string, why: string): Promise<Optional<T>> => Promise.resolve({ supported: false, endpoint, why })

async function optional<T>(endpoint: string, run: () => Promise<T>): Promise<Optional<T>> {
  try {
    return { supported: true, data: await run() }
  } catch (e) {
    if (isUnsupported(e)) return { supported: false, endpoint, why: '' }
    throw e
  }
}

// ---------------------------------------------------------------- what the relay serves

export const overview = () => api<Overview>('overview')

export type HostSort = 'host' | 'events' | 'errors' | 'accounts' | 'seq' | 'tier' | 'status' | 'since' | 'lag'
export type HostQuery = { q?: string; tier?: string; status?: HostStatus | ''; sort: HostSort; desc: boolean; limit?: number; offset?: number }
/** Server-side filter, sort and page (`limit` default 10,000 on the server). */
export const hosts = (q: HostQuery) => api<HostList>('hosts', { params: { q: q.q || undefined, tier: q.tier || undefined, status: q.status || undefined, sort: q.sort, desc: q.desc, limit: q.limit, offset: q.offset } })
export const host = (h: string) => api<HostDetail>(`hosts/${enc(h)}`)
export const hostAction = (h: string, a: HostAction) => api<HostRow>(`hosts/${enc(h)}/action`, { body: a })
/** What hostAction sends, for the confirm dialog's footer. */
export const hostActionCall = (h: string, a: HostAction) => `POST /admin/api/hosts/${h}/action ${JSON.stringify(a)}`

export const cluster = () => api<ClusterView>('cluster')
export const consumers = () => api<Consumer[]>('consumers')
export const openCases = () => api<Case[]>('cases', { params: { status: 'open' } })
export const pipeline = () => api<PipelineView>('ops/pipeline')
export const policy = () => api<PolicyDoc>('policy')
export const policyFull = () => api<FullPolicyDoc>('policy/full')
export const settings = () => api<SettingsView>('settings')

let hasQuorum: boolean | undefined
/**
 * Every member's /qlog/status, or unsupported on a relay without the quorum log. The public
 * stats say which (`quorum: null`), so a single relay never polls an endpoint that 404s.
 */
export async function quorum(): Promise<Optional<QuorumView>> {
  if (hasQuorum === undefined) {
    try {
      hasQuorum = (await publicStats()).quorum !== null
    } catch {
      hasQuorum = true
    }
  }
  if (!hasQuorum) return { supported: false, endpoint: 'cluster/quorum', why: 'this relay runs without the quorum log' }
  return optional('cluster/quorum', () => api<QuorumView>('cluster/quorum'))
}

/** com.atproto.sync.requestCrawl on this relay: the same admission path a PDS takes. */
export async function requestCrawl(hostname: string): Promise<void> {
  const r = await fetch('/xrpc/com.atproto.sync.requestCrawl', { method: 'POST', headers: { 'Content-Type': 'application/json' }, body: JSON.stringify({ hostname }) })
  if (!r.ok) {
    let body: { error?: string; message?: string } = {}
    try {
      body = await r.json()
    } catch {
      /* not JSON */
    }
    throw new ApiError(r.status, body.error ?? `HTTP ${r.status}`, body.message ?? (r.status === 404 ? 'This server has no requestCrawl (the admin demo has none).' : ''))
  }
}

// ---------------------------------------------------------------- what the console wants next

export type Admission = { atMs: number; host: string; outcome: 'admitted' | 'refused' | 'banned' | 'rate-limited'; tier?: string; reason: string }
export type AdmissionLog = { newHostsToday: number; newHostsPerDay: number; entries: Admission[] }
/** Recent requestCrawl outcomes and today's new-host budget. */
export const admissions = () => missing<AdmissionLog>('GET hosts/admissions', 'the crawl admission log')

export type TailFrame = { atMs: number; host: string; did: string; kind: 'reject' | 'held'; reason?: string; upstreamSeq?: number }
/** A sample of frames that never reached the firehose (rejected or read but held), and one host's frames at full rate. */
export const tailFrames = (_host?: string) => missing<TailFrame[]>('GET ops/tail?host=&rejects=1', 'rejected and held frames in the tail, and following one host')

/** A history for each of the busiest hosts, so the exchange and the Busiest table can draw real trunks. */
export const topHostHistory = () => missing<Record<string, number[]>>('overview.topHosts[].history', 'per-host rate history on the overview')

/** When the leader last flushed (F's age); the status only has F itself. */
export const lastFlush = () => missing<{ atMs: number }>('qlog status.flush.last_at_ms', 'the time of the last flush')

export type CostModel = { hostsPerMonth: number; bucketPerMonth: number; classAPerSec: number; classBPerSec: number; storedBytes: number }
/** The monthly bill: node prices from config, bucket requests and storage priced by provider. */
export const costModel = () => missing<CostModel>('GET store/cost', 'the cost model')

/** Lift every account a host created throttled past its cap (indigo does this on a raise). */
export const releaseThrottled = (h: string) => missing<{ released: number }>(`POST hosts/${h}/release-throttled`, 'releasing throttled accounts')

/** Everything above, for CONSOLE.md and the "needs a newer vlRelay" placeholders. */
export const MISSING = [
  ['GET hosts/admissions', 'Hosts › Crawl admission: requestCrawl outcomes (admitted, refused, banned, 429) with the reason, and newHostsToday vs cluster.newHostsPerDay'],
  ['GET ops/tail?host=&rejects=1', 'Overview tail: rejected and held frames (the firehose only carries what passed), and following one host at full rate'],
  ['overview.topHosts[].history', 'Overview: per-host rate series for the exchange trunks and the Busiest hosts sparklines (the console keeps its own from polls until then)'],
  ['qlog status.flush.last_at_ms', 'Overview health line: "flushed N s ago" (F alone has no time)'],
  ['GET store/cost', 'Overview rail and health line: the monthly bill (node prices, bucket requests by class, storage)'],
  ['POST hosts/{host}/release-throttled', 'Host page: lift the accounts created throttled past the cap'],
] as const
