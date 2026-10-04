// The operator API (src/admin.rs). Auth is `Basic admin:<token>`, as in the
// vlpds console; the token lives in sessionStorage (survives a reload, not a
// closed tab).

export class ApiError extends Error {
  status: number
  error: string
  constructor(status: number, error: string, message: string) {
    super(message || error)
    this.status = status
    this.error = error
  }
}

// ---------------------------------------------------------------- wire types

export type HostStatus = 'connected' | 'idle' | 'backoff' | 'offline' | 'throttled' | 'suspended' | 'banned'
export type Severity = 'info' | 'warn' | 'high' | 'critical'
export type RejectReason =
  | 'bad-signature'
  | 'invalid-commit'
  | 'rev-out-of-order'
  | 'prev-data-mismatch'
  | 'wrong-host'
  | 'unknown-did'
  | 'too-large'
  | 'rate-limited'
  | 'takendown'
  | 'malformed'

export type HostRow = {
  host: string
  tier: string
  status: HostStatus
  eventsPerSec: number
  errorRate: number
  accounts: number
  lastUpstreamSeq: number
  connectedSinceMs: number | null
  lagMs: number
  throttle: number | null
  rule: number | null
  node: string
}

export type History = {
  sampleSecs: number
  t: number[]
  eventsIn: number[]
  eventsOut: number[]
  bytesIn: number[]
  bytesOut: number[]
  ttfP50Ms: number[]
  ttfP99Ms: number[]
  durabilityLagMs: number[]
  rejects: Partial<Record<RejectReason, number[]>>
}

export type Overview = {
  timeMs: number
  eventsInPerSec: number
  eventsOutPerSec: number
  bytesInPerSec: number
  bytesOutPerSec: number
  consumers: number
  hostsConnected: number
  hostsTotal: number
  hostsByStatus: Partial<Record<HostStatus, number>>
  rejectsPerSec: number
  rejectsByReason: Partial<Record<RejectReason, number>>
  timeToFirehoseP50Ms: number
  timeToFirehoseP99Ms: number
  logDurabilityLagMs: number
  lastSeq: number
  openCases: number
  topHosts: HostRow[]
  history: History
  /** The merged stream's own rate; eventsOutPerSec sums every node's emits. */
  streamEventsPerSec?: number
  /** Each node's share. The totals above sum the nodes that aren't stale. */
  byNode?: NodeTotals[]
}

export type NodeTotals = {
  node: string
  role: string
  stale: boolean
  error: string | null
  eventsInPerSec: number
  eventsOutPerSec: number
  bytesInPerSec: number
  bytesOutPerSec: number
  consumers: number
  hostsConnected: number
  hostsTotal: number
  rejectsPerSec: number
}

export type HostList = { total: number; hosts: HostRow[] }

export type TierLimits = { eventsPerSec: number; eventsPerHour: number; eventsPerDay: number; maxAccounts: number; newAccountsPerHour: number }

export type RejectSample = { atMs: number; did: string; reason: RejectReason; upstreamSeq: number; detail: string }

export type HostAction =
  | { action: 'set-tier'; tier: string }
  | { action: 'throttle'; eventsPerSec: number | null }
  | { action: 'suspend'; reason: string }
  | { action: 'ban'; reason: string }
  | { action: 'unban' }
  | { action: 'reconnect' }

export type HostDetail = {
  row: HostRow
  limits: TierLimits
  newAccountsPerHour: number
  rejectsByReason: Partial<Record<RejectReason, number>>
  recentRejects: RejectSample[]
  series: { sampleSecs: number; t: number[]; events: number[]; rejects: number[] }
  actions: { atMs: number; by: string; action: HostAction }[]
  openCases: number[]
}

export type RuleEffect = { kind: 'ban' } | { kind: 'allow' } | { kind: 'tier'; tier: string } | { kind: 'throttle'; eventsPerSec: number }
export type DomainRule = { id: number; pattern: string; effect: RuleEffect; note: string; createdAtMs: number; createdBy: string; matches: number }
export type DomainRuleInput = { pattern: string; effect: RuleEffect; note: string }

export type SpamThresholds = {
  newAccountsPerHour: number
  rejectRatio: number
  badSignaturesPerMin: number
  accountEventsPerSec: number
  autoThrottle: boolean
}
export type Policy = { tiers: Record<string, TierLimits>; defaultTier: string; spam: SpamThresholds }
export type PolicyDoc = { version: number; policy: Policy; updatedAtMs: number; updatedBy: string }
export type PolicyAudit = { version: number; atMs: number; by: string; note: string; changes: string[] }
/** The engine's whole policy document; `policy` is its PolicyBody, which the API doesn't type. */
export type FullPolicyDoc = { version: number; updatedAtMs: number; updatedBy: string; note: string; policy: Record<string, unknown> }
export type FullPolicyUpdate = { baseVersion: number; policy: unknown; note: string }

export type Consumer = {
  id: number
  ip: string
  userAgent: string
  node: string
  connectedSinceMs: number
  cursor: number
  lagMs: number
  eventsPerSec: number
  bytesPerSec: number
  backfilling: boolean
}

export type NodeView = {
  id: string
  addr: string
  version: string
  rev: string
  reachable: boolean
  leaseValid: boolean
  leaseExpiresMs: number
  hostShards: number
  didShards: number
  hosts: number
  consumers: number
  eventsInPerSec: number
  eventsOutPerSec: number
  logDurabilityLagMs: number
  cpu: number
  memBytes: number
  role?: string
  /** It didn't answer this round: its numbers are 0, not its last ones. */
  stale?: boolean
  error?: string | null
  reportedMs?: number
  bytesOutPerSec?: number
  streamSeq?: number
}
export type ClusterView = { nodes: NodeView[]; hostShards: (string | null)[]; didShards: (string | null)[]; lastSeq: number }

export type Takedown = { atMs: number; by: string; reason: string }
export type Account = {
  did: string
  handle: string | null
  host: string
  status: string
  upstreamStatus: string
  takedown: Takedown | null
  rev: string
  lastSeq: number
  lastEventMs: number
  eventsLastHour: number
  rejectsLastHour: number
  didShard: number
  node: string
  archive?: AccountArchive
}

export type AccountArchive = {
  wanted: boolean
  mirrored: boolean
  rev: string | null
  fetching: boolean
  staging: boolean
  lastError: string | null
  takedownAtMs: number | null
}

// ---------------------------------------------------------------- operations

export type ArchiveCounts = {
  mirrored: number
  queued: number
  running: number
  failed: number
  fetched: number
  retried: number
  bytes: number
  records: number
  sstBytes: number
  applied: number
  mismatches: number
  healed: number
  sweptAtMs: number
}
export type ArchiveView = {
  mode: string
  policyVersion: number
  totals: ArchiveCounts
  nodes: { node: string; stale: boolean; counts: ArchiveCounts }[]
  errors: { node: string; did: string; error: string }[]
}

export type PlcWindow = { fromMs: number; afterMs: number; untilMs: number | null; ops: number; done: boolean; progress: number }
export type PlcView = {
  enabled: boolean
  leader: string | null
  caughtUp: boolean
  ops: number
  opsPerSec: number
  written: number
  requests: number
  throttled: number
  errors: number
  restarts: number
  newestMs: number
  windows: PlcWindow[]
  checkpointMs: number
  nodes: { node: string; stale: boolean; leader: boolean; ops: number; opsPerSec: number; throttled: number; errors: number }[]
}

export type SeqPair = { key: number; timeMs: number; seq: number }
export type SeqView = {
  nodes: { node: string; role: string; stale: boolean; head: number; latest: SeqPair | null }[]
  boundaries: { key: number; timeMs: number; seqs: Record<string, number>; agree: boolean }[]
  agree: boolean
}

export type PipelineNode = {
  node: string
  stale: boolean
  ackPending: number
  oldestPendingMs: number
  laneQueued: number
  dedupeEntries: number
  pausedHosts: number
  gauges: Record<string, number>
}
export type PipelineHost = {
  host: string
  node: string
  inflight: number
  inflightCap: number | null
  paused: boolean
  status: HostStatus | null
  eventsPerSec: number
}
export type PipelineView = { nodes: PipelineNode[]; hosts: PipelineHost[] }

export type CaseStatus = 'open' | 'acknowledged' | 'resolved' | 'dismissed'
export type Case = {
  id: number
  host: string
  did: string | null
  kind: string
  severity: Severity
  status: CaseStatus
  openedAtMs: number
  updatedAtMs: number
  summary: string
  observed: number
  threshold: number
  autoAction: string | null
  notes: { atMs: number; by: string; text: string }[]
}
export type CaseEvidence = {
  atMs: number
  observed: number
  threshold: number
  windowSecs: number
  node: string
  detail?: string
  signals: Record<string, number>
}
export type CaseDetail = Case & { trips: number; evidence: CaseEvidence[] }

// ---------------------------------------------------------------- token

const AKEY = 'vlrelay.admin'
let adminToken: string | null = (() => {
  try {
    return sessionStorage.getItem(AKEY)
  } catch {
    return null
  }
})()
const adminListeners = new Set<() => void>()

export const getAdminToken = () => adminToken
export function setAdminToken(t: string | null) {
  adminToken = t
  try {
    if (t) sessionStorage.setItem(AKEY, t)
    else sessionStorage.removeItem(AKEY)
  } catch {
    /* memory only */
  }
  adminListeners.forEach((l) => l())
}
export function subscribeAdmin(l: () => void) {
  adminListeners.add(l)
  return () => {
    adminListeners.delete(l)
  }
}

export const basic = (token: string) => `Basic ${btoa(`admin:${token}`)}`

// ---------------------------------------------------------------- calls

type Params = Record<string, string | number | boolean | undefined | null>

function qs(params?: Params): string {
  if (!params) return ''
  const u = new URLSearchParams()
  for (const [k, v] of Object.entries(params)) if (v !== undefined && v !== null && v !== '') u.set(k, String(v))
  const s = u.toString()
  return s ? `?${s}` : ''
}

export async function api<T = unknown>(
  path: string,
  o: { params?: Params; body?: unknown; method?: 'GET' | 'POST' | 'PUT' | 'DELETE'; token?: string } = {},
): Promise<T> {
  const token = o.token ?? adminToken
  if (!token) throw new ApiError(401, 'AuthenticationRequired', 'Enter the admin token')
  const headers: Record<string, string> = { Authorization: basic(token) }
  if (o.body !== undefined) headers['Content-Type'] = 'application/json'
  const r = await fetch(`/admin/api/${path}${qs(o.params)}`, {
    method: o.method ?? (o.body !== undefined ? 'POST' : 'GET'),
    headers,
    body: o.body !== undefined ? JSON.stringify(o.body) : undefined,
  })
  const text = await r.text()
  let body: any
  try {
    body = text ? JSON.parse(text) : undefined
  } catch {
    body = text
  }
  if (!r.ok) {
    const err = typeof body === 'object' && body ? body : {}
    if (r.status === 401 && !o.token) setAdminToken(null)
    throw new ApiError(r.status, err.error ?? `HTTP ${r.status}`, err.message ?? (typeof body === 'string' ? body : ''))
  }
  return body as T
}

export const enc = encodeURIComponent

export function errText(e: unknown): string {
  if (e instanceof ApiError) return e.message || e.error
  if (e instanceof Error) return e.message
  return String(e)
}
