import type { ClusterView, Health, QStatus, QuorumView, SettingsView } from '../api'
import type { Optional } from './adminAdapter'
import { useMemo } from 'react'
import { clusterPoll, overviewPoll, publicPoll, quorumPoll, settingsPoll } from './polls'

// The relay as the console draws it: its nodes (cluster), the quorum log (cluster/quorum) and
// which node is answering (settings' --node-id). Colours go to nodes in name order so a node
// keeps its colour across pages.

export type NodeInfo = {
  id: string
  color: string
  role: string
  /** A member of the quorum log (or a core on an older cluster). */
  core: boolean
  stale: boolean
  error: string | null
  consumers: number
  eventsInPerSec: number
  eventsOutPerSec: number
  hostShards: number
  addr: string
  version: string
  rev: string
  status?: QStatus | null
}

export type QuorumInfo = {
  epoch: number
  leader: string | null
  members: string[]
  learners: string[]
  /** Members whose status came back this round. */
  answering: string[]
  health: Health
  commit: number
  flushed: number
  reserve: number
  generation: number
  /** The leader's status (else any member's). */
  lead?: QStatus
}

export type RelayView = {
  nodes: NodeInfo[]
  byId: Map<string, NodeInfo>
  /** The node serving the console, when settings name it. */
  self?: string
  single: boolean
  quorum?: QuorumInfo
  /** cluster/quorum isn't there: no quorum log on this relay. */
  noQuorum: boolean
  version?: string
}

const COLORS = ['var(--c1)', 'var(--c2)', 'var(--c3)', 'var(--c4)', 'var(--c5)', 'var(--c6)']

function quorumInfo(q: QuorumView): QuorumInfo | undefined {
  const live = q.nodes.filter((n) => !n.stale && n.status)
  const lead = live.find((n) => n.status!.role === 'leader')?.status ?? undefined
  const any = lead ?? live[0]?.status ?? undefined
  if (!any) return { epoch: 0, leader: null, members: q.nodes.map((n) => n.node), learners: [], answering: [], health: 'down', commit: 0, flushed: 0, reserve: 0, generation: 0 }
  const members = any.members ?? []
  const answering = live.map((n) => n.node).filter((id) => members.includes(id))
  const health: Health = !lead || answering.length * 2 <= members.length ? 'down' : answering.length < members.length ? 'degraded' : 'ok'
  return {
    epoch: any.epoch,
    leader: lead ? lead.id : null,
    members,
    learners: any.learners ?? [],
    answering,
    health,
    commit: any.commit,
    flushed: any.flushed,
    reserve: any.reserve,
    generation: any.generation,
    lead: any,
  }
}

/**
 * `pubNodes` and `reader` say what a single relay is: the public stats count one node, and the
 * hosts name the node reading them. A relay without the quorum log or a cluster answers
 * `cluster` with a placeholder layout, so on a single node only the node itself is drawn.
 */
export function buildView(c?: ClusterView, q?: Optional<QuorumView>, s?: SettingsView, pubNodes?: number, reader?: string): RelayView | undefined {
  if (!c && !q) return undefined
  const qv = q?.supported ? q.data : undefined
  const selfId = s?.entries.find((e) => e.flag === '--node-id')?.value ?? undefined
  if (!qv && pubNodes === 1) {
    const id = reader || selfId || 'relay'
    const n = c?.nodes.find((x) => x.id === id)
    const node: NodeInfo = {
      id,
      color: COLORS[0],
      role: 'single node',
      core: true,
      stale: false,
      error: null,
      consumers: n?.consumers ?? 0,
      eventsInPerSec: n?.eventsInPerSec ?? 0,
      eventsOutPerSec: n?.eventsOutPerSec ?? 0,
      hostShards: n?.hostShards ?? 0,
      addr: n?.addr ?? '',
      version: n?.version ?? s?.version ?? '',
      rev: n?.rev ?? '',
    }
    return { nodes: [node], byId: new Map([[id, node]]), self: id, single: true, noQuorum: !!q && !q.supported, version: s?.version }
  }
  const ids = new Set<string>([...(c?.nodes.map((n) => n.id) ?? []), ...(qv?.nodes.map((n) => n.node) ?? [])])
  const sorted = [...ids].sort()
  const quorum = qv ? quorumInfo(qv) : undefined
  const nodes: NodeInfo[] = sorted.map((id, i) => {
    const n = c?.nodes.find((x) => x.id === id)
    const qn = qv?.nodes.find((x) => x.node === id)
    const role = qn?.status?.role ?? n?.role ?? 'core'
    return {
      id,
      color: COLORS[i % COLORS.length],
      role: qn?.stale ? 'no answer' : role,
      core: !!qn || role === 'core' || role === 'leader' || role === 'follower' || role === 'learner' || role === '',
      stale: !!(n?.stale || qn?.stale),
      error: n?.error ?? qn?.error ?? null,
      consumers: n?.consumers ?? 0,
      eventsInPerSec: n?.eventsInPerSec ?? 0,
      eventsOutPerSec: n?.eventsOutPerSec ?? 0,
      hostShards: n?.hostShards ?? 0,
      addr: qn?.addr || n?.addr || '',
      version: n?.version ?? '',
      rev: n?.rev ?? '',
      status: qn?.status,
    }
  })
  const self = selfId
  return {
    nodes,
    byId: new Map(nodes.map((n) => [n.id, n])),
    self: self && ids.has(self) ? self : undefined,
    single: nodes.length <= 1,
    quorum,
    noQuorum: !!q && !q.supported,
    version: s?.version,
  }
}

/** The relay's nodes and quorum, from the shared polls. */
export function useRelay(): { view?: RelayView; loading: boolean } {
  const c = clusterPoll.use()
  const q = quorumPoll.use()
  const s = settingsPoll.use()
  const p = publicPoll.use()
  const o = overviewPoll.use()
  const view = useMemo(() => buildView(c.data, q.data, s.data, p.data?.nodes, o.data?.topHosts[0]?.node), [c.data, q.data, s.data, p.data?.nodes, o.data?.topHosts])
  return { view, loading: c.loading && q.loading }
}

export const nodeColor = (v: RelayView | undefined, id: string | null | undefined) => (id && v?.byId.get(id)?.color) || undefined
