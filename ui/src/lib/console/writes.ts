import type { Account, Consumer, DiscoveryView, DomainRule, DomainRuleInput, HostAction, Policy, QStatus } from '../api'
import * as A from './adminAdapter'
import { invalidate, keys, keysFor, markUnsettled, queryClient as qc, writeCase, writeHostRow } from './cache'
import type { Json, PolicyBase } from './policyDraft'

// Every write the console makes. Each puts its answer into the cache at once (the row an action
// returns, the rule, the case, the account), then invalidates what the change touches, the same
// keys a change event for it would (cache.ts `keysFor`), so every panel agrees without waiting
// for a poll.

/** A host action; its answer is the host's row after the write (docs/admin-api.md, "Host actions"), or one marked `pending`. */
export async function hostAction(name: string, a: HostAction) {
  const row = await A.hostAction(name, a)
  const won = writeHostRow(row)
  // pending: the cluster hadn't confirmed it in time, and a later change says when it lands
  if (row.pending) markUnsettled(won)
  invalidate(keysFor('host', name), true)
  return row
}

/** Lifts every account a host created throttled past its cap. */
export async function releaseThrottled(host: string) {
  const r = await A.releaseThrottled(host)
  invalidate(keysFor('account', '*', { host }), true)
  return r
}

/** Adds a rule (no `id`) or replaces one. */
export async function saveRule(id: number | undefined, body: DomainRuleInput): Promise<DomainRule> {
  const r = id === undefined ? await A.createRule(body) : await A.updateRule(id, body)
  qc.setQueryData<DomainRule[]>(keys.rules(), (l) => {
    if (!l) return l
    const next = l.some((x) => x.id === r.id) ? l.map((x) => (x.id === r.id ? r : x)) : [...l, r]
    // the whole set is at the new rule's version now
    return r.version === undefined ? next : next.map((x) => ({ ...x, version: r.version }))
  })
  invalidate(keysFor('rules'), true)
  return r
}

export async function deleteRule(id: number) {
  await A.deleteRule(id)
  qc.setQueryData<DomainRule[]>(keys.rules(), (l) => l?.filter((x) => x.id !== id))
  invalidate(keysFor('rules'), true)
}

/** A case's status or a note. */
export async function updateCase(id: number, u: Parameters<typeof A.updateCase>[1]) {
  const c = await A.updateCase(id, u)
  writeCase(c)
  invalidate(keysFor('case', String(id)), true)
  return c
}

/**
 * Several cases' status at once, through `cases/bulk` a thousand at a time; the lists refetch
 * once at the end. Resolves to the ids that failed (with why): a chunk the relay refused, or a
 * case it no longer had. The rest were written.
 */
export async function updateCases(ids: number[], u: Parameters<typeof A.updateCase>[1], onProgress?: (done: number, failed: number) => void) {
  const failed: { id: number; error: unknown }[] = []
  let done = 0
  for (let i = 0; i < ids.length; i += 1000) {
    const chunk = ids.slice(i, i + 1000)
    try {
      const r = await A.bulkUpdateCases({ ids: chunk, status: u.status ?? undefined, note: u.note })
      const ok = new Set(r.ids)
      done += r.updated
      for (const id of chunk) if (!ok.has(id)) failed.push({ id, error: new Error(`no case ${id}`) })
    } catch (error) {
      for (const id of chunk) failed.push({ id, error })
    }
    onProgress?.(done, failed.length)
  }
  invalidate(keysFor('case'), true)
  return failed
}

function wroteAccount(a: Account) {
  qc.setQueryData(keys.account(a.did), a)
  invalidate(keysFor('takedown', a.did), true)
  return a
}
export const takedown = (did: string, reason: string) => A.takedown(did, reason).then(wroteAccount)
export const untakedown = (did: string) => A.untakedown(did).then(wroteAccount)

export async function kickConsumer(c: Pick<Consumer, 'id' | 'node'>) {
  await A.kickConsumer(c.id, c.node)
  qc.setQueryData<Consumer[]>(keys.consumers(), (l) => l?.filter((x) => !(x.id === c.id && x.node === c.node)))
  invalidate(keysFor('consumer'), true)
}

/** Runs one source now (its key), or every enabled one; the answer is discovery with the run requested. */
export async function runDiscovery(source?: string): Promise<DiscoveryView> {
  const v = await A.runDiscovery(source)
  qc.setQueryData(keys.discovery(), v)
  invalidate(keysFor('discovery'), true)
  return v
}

/** Saves the policy against the version it was edited from (a 409 when someone saved first). */
export async function savePolicy(base: PolicyBase, body: Json, note: string): Promise<PolicyBase> {
  let doc: PolicyBase
  if (base.mode === 'full') {
    const d = await A.savePolicyFull({ baseVersion: base.version, policy: body, note })
    qc.setQueryData(keys.policyFull(), d)
    doc = { mode: 'full', version: d.version, updatedAtMs: d.updatedAtMs, updatedBy: d.updatedBy, note: d.note, body: d.policy }
  } else {
    const d = await A.savePolicy({ baseVersion: base.version, policy: body as unknown as Policy, note })
    qc.setQueryData(keys.policy(), d)
    doc = { mode: 'wire', version: d.version, updatedAtMs: d.updatedAtMs, updatedBy: d.updatedBy, note, body: d.policy as unknown as Json }
  }
  qc.setQueryData(keys.policySource(), doc)
  invalidate(keysFor('policy'), true)
  return doc
}

export async function changeMembers(c: A.MembersChange) {
  const r = await A.changeMembers(c)
  invalidate(keysFor('cluster'), true)
  return r
}

export async function flushNow(): Promise<QStatus> {
  const r = await A.flushNow()
  invalidate(keysFor('cluster'), true)
  return r
}

/** requestCrawl on this relay: a new host, a woken one, or a refusal in the admissions. */
export async function requestCrawl(hostname: string) {
  await A.requestCrawl(hostname)
  invalidate([['hosts'], keys.admissions(), keys.overview()], true)
}
