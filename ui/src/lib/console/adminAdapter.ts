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
  type QStatus,
  type QuorumView,
  type Quantiles,
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

/**
 * The cluster in the console's terms. Its wire names are moving to quorum terms (hostShards to
 * hostOwners); this is the one place that reads them, so the pages don't follow the rename.
 */
export async function cluster(): Promise<ClusterView> {
  const r = await api<ClusterView & { hostOwners?: (string | null)[] }>('cluster')
  return { ...r, hostShards: r.hostShards ?? r.hostOwners ?? [] }
}
export const consumers = () => api<Consumer[]>('consumers')
/** Consumer ids are per node: `node` names the one serving it. */
export const kickConsumer = (id: number, node: string) => api(`consumers/${id}/kick`, { method: 'POST', params: { node: node || undefined } })
export const kickCall = (id: number, node: string) => `POST /admin/api/consumers/${id}/kick${node ? `?node=${node}` : ''}`
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

export type MembersChange = { members: string[]; addrs: Record<string, string> }
/** A membership change, sent on to the leader with the nodes' --qlog-admin-token. */
export const changeMembers = (c: MembersChange) => api<unknown>('cluster/quorum/members', { body: c })
export const changeMembersCall = (c: MembersChange) => `POST /admin/api/cluster/quorum/members ${JSON.stringify(Object.keys(c.addrs).length ? c : { members: c.members })}`

/** This node's ack backlog (the pipeline), or unsupported where the route isn't served. */
export async function pipelineOpt(): Promise<Optional<PipelineView>> {
  try {
    return { supported: true, data: await pipeline() }
  } catch (e) {
    if (e instanceof ApiError && e.status === 404) return { supported: false, endpoint: 'GET ops/pipeline', why: e.message }
    throw e
  }
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

/** When F last moved: the leader's own time where the status has it, else when this console saw F move. */
export const flushedAt = (lead: QStatus | undefined, seenAt: number | undefined) => lead?.flush?.last_at_ms || seenAt

export type FlushRecord = { atMs: number; flushed: number; reserve: number; entries: number; segmentBytes: number; tookMs: number }
/** The leader's recent flushes. Until then the console records the ones it sees F move for. */
export const flushHistory = () => missing<FlushRecord[]>('qlog status.flush.recent', 'the recent flushes')

export type EpochChange = { epoch: number; atMs: number; kind: 'takeover' | 'handoff' | 'switch' | 'recovery'; leader: string; pausedMs: number; why: string }
/** Every epoch change (takeovers and handoffs too), not only the membership changes and recoveries this leader ran. */
export const quorumHistory = () => missing<EpochChange[]>('GET cluster/quorum/history', 'takeovers and handoffs in the leadership history')

export type ConsumerTier = 'ring' | 'disk' | 'bucket'
/** Where each consumer's frames come from: the in-memory ring, the commitlog on disk, or the bucket. */
export const consumerTiers = () => missing<Record<string, ConsumerTier>>('consumers[].readTier', 'where a replaying consumer reads from')

export type RetentionReport = { atMs: number; by: string; horizonSecs: number; prunedSeq: number; deletedSegments: number; deletedStatePaths: number; pastHorizon: { segments: number; bytes: number } }
/** The leader's last retention report (`retain/qlog` in the bucket). The status counts runs and deletes only. */
export const retentionReport = () => missing<RetentionReport>('GET store/retention', 'the last retention report')

export type PrefixStats = { prefix: string; objects: number; bytes: number }[]
/** Objects and bytes under each of the relay's bucket prefixes. */
export const prefixStats = () => missing<PrefixStats>('GET store/prefixes', 'objects and bytes per prefix')

/** Object-store request latency (PUT and GET quantiles) per purpose. */
export const requestLatency = () => missing<Record<string, Quantiles>>('qlog status.requests.latency_us', 'bucket request latency')

/** Lift every account a host created throttled past its cap (indigo does this on a raise). */
export const releaseThrottled = (h: string) => missing<{ released: number }>(`POST hosts/${h}/release-throttled`, 'releasing throttled accounts')

/** Everything above, for CONSOLE.md and the "needs a newer vlRelay" placeholders. */
export const MISSING = [
  ['GET hosts/admissions', 'Hosts › Crawl admission: requestCrawl outcomes (admitted, refused, banned, 429) with the reason, and newHostsToday vs cluster.newHostsPerDay'],
  ['GET ops/tail?host=&rejects=1', 'Overview tail: rejected and held frames (the firehose only carries what passed), and following one host at full rate'],
  ['overview.topHosts[].history', 'Overview: per-host rate series for the exchange trunks and the Busiest hosts sparklines (the console keeps its own from polls until then)'],
  ['qlog status.flush.last_at_ms', 'Overview health line: "flushed N s ago" (F alone has no time)'],
  ['POST hosts/{host}/release-throttled', 'Host page: lift the accounts created throttled past the cap'],
  ['qlog status.flush.recent', 'Quorum › Flush: the recent flushes (F, entries, bytes, how long); the console records only the ones it sees while open'],
  ['GET cluster/quorum/history', 'Quorum › Leadership: takeovers and handoffs with their times (the status has membership changes and recoveries only)'],
  ['consumers[].readTier', 'Consumers: where a replaying consumer reads from (ring, disk, bucket)'],
  ['GET store/retention', 'Object store › Retention: the last report (horizon, pruned seq, what it deleted, what is past the horizon now)'],
  ['GET store/prefixes', 'Object store › Prefixes: objects and bytes under each prefix'],
  ['qlog status.requests.latency_us', 'Object store: PUT and GET latency per purpose'],
] as const
