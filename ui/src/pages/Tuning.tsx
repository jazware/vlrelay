import { useEffect, useMemo, useState, type ReactNode } from 'react'
import { InlineConfirm, Live, TierPill } from '../components/relay'
import { ErrorNotice, Loading, Notice, Panel } from '../components/ui'
import { api, ApiError, errText, type FullPolicyDoc } from '../lib/api'
import { fmtBytes, fmtTime, relTime } from '../lib/format'
import { useAction } from '../lib/hooks'
import { Link } from '../lib/router'
import { useApi } from '../lib/useApi'
import { diffJson } from './Policy'
import './ops2.css'

type Json = Record<string, unknown>
type Kind = { t: 'num'; int?: boolean; unit?: string; bytes?: boolean } | { t: 'ratio' } | { t: 'bool' } | { t: 'enum'; options: string[] } | { t: 'list' }
type Knob = { path: string[]; label: string; why: string; kind: Kind }

const TIERS = ['trusted', 'default', 'new', 'throttled']
const ACTIONS = ['alert', 'case', 'throttle', 'throttle-and-case']

const SECTIONS: { id: string; title: string; desc: string; knobs: Knob[] }[] = [
  {
    id: 'transitions',
    title: 'Tier transitions',
    desc: 'How hosts move between tiers on their own.',
    knobs: [
      { path: ['transitions', 'promoteAfterDays'], label: 'Promote new hosts after', why: 'A new host moves to default once this old with no trip for as long. Shorter trusts strangers sooner.', kind: { t: 'num', int: true, unit: 'days' } },
      { path: ['transitions', 'recoverAfterSecs'], label: 'Auto-throttle recovers after', why: 'An auto-throttled host goes back to its tier after this long without a trip.', kind: { t: 'num', int: true, unit: 's' } },
      { path: ['transitions', 'errorRatio'], label: 'Error budget', why: 'Rejected over all frames in one driver interval before a host is throttled (where its tier allows).', kind: { t: 'ratio' } },
      { path: ['transitions', 'errorMinEvents'], label: 'Error budget floor', why: 'Fewer frames than this never trip the budget, so one bad commit from a tiny PDS doesn’t throttle it.', kind: { t: 'num', int: true, unit: 'frames' } },
    ],
  },
  {
    id: 'cluster',
    title: 'Cluster budgets',
    desc: 'Shared by every node. The per-second ones are split evenly over the live cores.',
    knobs: [
      { path: ['cluster', 'plcLookupsPerSec'], label: 'PLC lookups', why: 'DID document fetches per second across the cluster. The directory rate-limits; stay well under its limit.', kind: { t: 'num', unit: '/s' } },
      { path: ['cluster', 'newAccountsPerMin'], label: 'New accounts', why: 'Accounts first seen per minute across every host. A spam wave hits this before it reaches consumers.', kind: { t: 'num', unit: '/min' } },
      { path: ['cluster', 'newHostsPerDay'], label: 'New hosts', why: 'requestCrawl admissions per day (allow rules don’t count against it).', kind: { t: 'num', int: true, unit: '/day' } },
      { path: ['cluster', 'archivalFetchConcurrency'], label: 'Archival fetches in flight', why: 'getRepo bootstrap fetches running at once, cluster-wide.', kind: { t: 'num', int: true } },
      { path: ['cluster', 'archivalFetchBytesPerSec'], label: 'Archival fetch bandwidth', why: 'Bytes per second all bootstrap fetches may pull together.', kind: { t: 'num', int: true, bytes: true, unit: 'B/s' } },
    ],
  },
  {
    id: 'consumers',
    title: 'Consumer limits',
    desc: 'Enforced by the node serving each subscriber.',
    knobs: [
      { path: ['consumers', 'connectionsPerIp'], label: 'Connections per IP', why: 'Raise for consumers behind shared NAT; lower if one address is hogging sockets.', kind: { t: 'num', int: true } },
      { path: ['consumers', 'consumersPerNode'], label: 'Consumers per node', why: 'Sockets one node serves before refusing new ones. Bandwidth is the real ceiling.', kind: { t: 'num', int: true } },
      { path: ['consumers', 'slowConsumerLagSecs'], label: 'Slow consumer cutoff', why: 'A consumer this far behind live is disconnected; it can resume from its cursor.', kind: { t: 'num', int: true, unit: 's' } },
      { path: ['consumers', 'maxBackfillSecs'], label: 'Max backfill', why: 'Cursors further back get OutdatedCursor. Can’t exceed what the log retains.', kind: { t: 'num', int: true, unit: 's' } },
    ],
  },
  {
    id: 'crawl',
    title: 'Crawl admission',
    desc: 'Who requestCrawl lets in, and where they start. Operators can always add hosts.',
    knobs: [
      { path: ['crawl', 'enabled'], label: 'Public requestCrawl', why: 'Off: only operators add hosts.', kind: { t: 'bool' } },
      { path: ['crawl', 'allowlistOnly'], label: 'Allow-list only', why: 'Only hosts an allow rule or trusted domain covers get in. For incidents or closed networks.', kind: { t: 'bool' } },
      { path: ['crawl', 'allowInsecure'], label: 'Allow insecure hosts', why: 'Plain ws:// and IP hosts. Leave off outside dev networks.', kind: { t: 'bool' } },
      { path: ['crawl', 'initialTier'], label: 'Initial tier', why: 'The tier every other admitted host starts in.', kind: { t: 'enum', options: TIERS } },
      { path: ['crawl', 'trustedDomains'], label: 'Trusted domains', why: 'Hosts matching these start trusted (one pattern per line, *.example.com style).', kind: { t: 'list' } },
    ],
  },
  {
    id: 'archive',
    title: 'Archival',
    desc: 'Which accounts the relay mirrors (docs/archival.md).',
    knobs: [
      { path: ['archive', 'mode'], label: 'Mode', why: 'off, all, tiers (hosts in the listed tiers) or hosts (listed hosts).', kind: { t: 'enum', options: ['off', 'all', 'tiers', 'hosts'] } },
      { path: ['archive', 'takedownRetentionHours'], label: 'Takedown retention', why: 'A taken-down account’s mirror stops serving at once and is deleted this long after.', kind: { t: 'num', int: true, unit: 'h' } },
    ],
  },
]

const TIER_EXTRAS: { k: string; label: string; kind: Kind }[] = [
  { k: 'bytesPerSec', label: 'Read rate', kind: { t: 'num', int: true, bytes: true } },
  { k: 'identityEventsPerHour', label: 'Identity events/h', kind: { t: 'num', int: true } },
  { k: 'reconnectsPerHour', label: 'Reconnects/h', kind: { t: 'num', int: true } },
  { k: 'archivalFetchesPerHost', label: 'Archival fetches/s', kind: { t: 'num' } },
  { k: 'autoThrottle', label: 'Auto-throttle', kind: { t: 'bool' } },
]

const SIGNALS: { k: string; label: string; per: 'host' | 'account' }[] = [
  { k: 'hostNewAccounts', label: 'New accounts', per: 'host' },
  { k: 'hostFailedValidation', label: 'Failed validation', per: 'host' },
  { k: 'hostIdentityChurn', label: 'Identity churn', per: 'host' },
  { k: 'hostOversizedCommits', label: 'Oversized commits', per: 'host' },
  { k: 'accountRecords', label: 'Records written', per: 'account' },
  { k: 'accountFailedValidation', label: 'Failed validation', per: 'account' },
  { k: 'accountIdentityChurn', label: 'Identity churn', per: 'account' },
]

const get = (o: unknown, path: string[]): unknown => path.reduce<unknown>((a, k) => (a && typeof a === 'object' ? (a as Json)[k] : undefined), o)

function setIn(o: Json, path: string[], v: unknown): Json {
  const [k, ...rest] = path
  return { ...o, [k]: rest.length ? setIn((o[k] as Json) ?? {}, rest, v) : v }
}

const same = (a: unknown, b: unknown) => JSON.stringify(a) === JSON.stringify(b)

function fmtVal(v: unknown, kind: Kind): string {
  if (v === undefined) return '—'
  if (kind.t === 'bool') return v ? 'on' : 'off'
  if (kind.t === 'ratio') return `${(Number(v) * 100).toFixed(1)}%`
  if (kind.t === 'list') return (v as string[]).join(', ') || '(none)'
  if (kind.t === 'num' && kind.bytes) return `${fmtBytes(Number(v))}${kind.unit === 'B/s' ? '/s' : ''}`
  return `${String(v)}${kind.t === 'num' && kind.unit && !kind.bytes ? ` ${kind.unit}` : ''}`
}

function invalid(v: unknown, kind: Kind): string | undefined {
  if (kind.t === 'num' || kind.t === 'ratio') {
    const n = Number(v)
    if (typeof v !== 'number' || !Number.isFinite(n) || n < 0) return 'a number ≥ 0'
    if (kind.t === 'num' && kind.int && !Number.isInteger(n)) return 'a whole number'
    if (kind.t === 'ratio' && n > 1) return 'between 0 and 100%'
  }
  return undefined
}

function Input({ id, value, kind, onChange, bad }: { id: string; value: unknown; kind: Kind; onChange: (v: unknown) => void; bad?: boolean }) {
  const cls = bad ? 'bad' : undefined
  switch (kind.t) {
    case 'bool':
      return <input id={id} type="checkbox" checked={!!value} onChange={(e) => onChange(e.target.checked)} />
    case 'enum':
      return (
        <select id={id} value={String(value)} onChange={(e) => onChange(e.target.value)}>
          {kind.options.map((o) => (
            <option key={o}>{o}</option>
          ))}
        </select>
      )
    case 'list':
      return <textarea id={id} rows={3} value={((value as string[]) ?? []).join('\n')} onChange={(e) => onChange(e.target.value.split('\n').map((s) => s.trim()).filter(Boolean))} spellCheck={false} />
    case 'ratio':
      return (
        <span className="unit-input">
          <input id={id} className={cls} type="number" step="0.1" min={0} max={100} value={Number.isNaN(value) ? '' : Math.round(Number(value) * 1000) / 10} onChange={(e) => onChange(e.target.value === '' ? NaN : Number(e.target.value) / 100)} />
          <span>%</span>
        </span>
      )
    default:
      return (
        <span className="unit-input">
          <input id={id} className={[cls, kind.bytes ? 'wide' : ''].filter(Boolean).join(' ') || undefined} type="number" step={kind.int ? 1 : 'any'} min={0} value={Number.isNaN(value) ? '' : (value as number)} onChange={(e) => onChange(e.target.value === '' ? NaN : Number(e.target.value))} />
          {kind.unit && <span>{kind.unit}</span>}
        </span>
      )
  }
}

export function Tuning() {
  const l = useApi<FullPolicyDoc>('policy/full', undefined, 10000)
  const defs = useApi<Json>('policy/defaults')
  const [base, setBase] = useState<FullPolicyDoc>()
  const [draft, setDraft] = useState<Json>()
  const [note, setNote] = useState('')
  const [review, setReview] = useState(false)
  const [conflict, setConflict] = useState<string>()
  const [saved, setSaved] = useState<number>()

  useEffect(() => {
    if (l.data && !base) {
      setBase(l.data)
      setDraft(l.data.policy)
    }
  }, [l.data, base])

  const changes = useMemo(() => (base && draft ? diffJson(base.policy, draft) : []), [base, draft])
  const problems = useMemo(() => {
    const out: string[] = []
    if (!draft) return out
    for (const s of SECTIONS) for (const k of s.knobs) if (invalid(get(draft, k.path), k.kind)) out.push(`${k.path.join('.')} must be ${invalid(get(draft, k.path), k.kind)}`)
    for (const t of TIERS) for (const f of TIER_EXTRAS) if (invalid(get(draft, ['tiers', t, f.k]), f.kind)) out.push(`tiers.${t}.${f.k} must be ${invalid(get(draft, ['tiers', t, f.k]), f.kind)}`)
    for (const s of SIGNALS) for (const f of ['limit', 'windowSecs']) if (invalid(get(draft, ['spam', s.k, f]), { t: 'num', int: f === 'windowSecs' })) out.push(`spam.${s.k}.${f} must be a number ≥ 0`)
    return out
  }, [draft])

  const save = useAction(async () => {
    if (!base || !draft) return
    try {
      const doc = await api<FullPolicyDoc>('policy/full', { method: 'PUT', body: { baseVersion: base.version, policy: draft, note: note.trim() } })
      setBase(doc)
      setDraft(doc.policy)
      setNote('')
      setReview(false)
      setSaved(doc.version)
      l.reload()
    } catch (e) {
      if (e instanceof ApiError && e.status === 409) {
        setConflict(e.message)
        setReview(false)
        return
      }
      throw e
    }
  })

  if (l.error instanceof ApiError && l.error.status === 404) return <Notice kind="info">{l.error.message}.</Notice>
  if (!base || !draft) return l.error ? <ErrorNotice error={l.error} /> : <Loading />
  const d = defs.data
  const newer = l.data && l.data.version > base.version
  const set = (path: string[]) => (v: unknown) => {
    setDraft((x) => setIn(x!, path, v))
    setSaved(undefined)
  }

  const row = (k: Knob) => {
    const id = `k-${k.path.join('-')}`
    const v = get(draft, k.path)
    const was = get(base.policy, k.path)
    const def = d ? get(d, k.path) : undefined
    const bad = invalid(v, k.kind)
    return (
      <div key={id} className={`knob${same(v, was) ? '' : ' edited'}`}>
        <label htmlFor={id} className="knob-label">
          <b>{k.label}</b>
          <span className="muted small">{k.why}</span>
        </label>
        <div className="knob-input">
          <Input id={id} value={v} kind={k.kind} onChange={set(k.path)} bad={!!bad} />
          {bad && <span className="err-hi small">{bad}</span>}
        </div>
        <div className="knob-def small">
          {def !== undefined && (
            <>
              <span className="muted">default</span> <span className="mono">{fmtVal(def, k.kind)}</span>
              {!same(v, def) && (
                <button type="button" className="btn sm quiet" onClick={() => set(k.path)(def)} title="Set to the default">
                  reset
                </button>
              )}
            </>
          )}
        </div>
      </div>
    )
  }

  return (
    <>
      <div className="console-head">
        <h1>Tuning</h1>
        <Live at={l.at} error={l.error} every={10000} />
      </div>
      <p className="muted">
        The rest of the policy document: knobs that apply live on every node within seconds of a save, no restart. Tier rates, account caps and case thresholds
        are on <Link to="/admin/policy">Limits</Link>; the whole document as JSON is under Advanced there. Version <span className="mono">v{base.version}</span>, updated{' '}
        <span title={fmtTime(base.updatedAtMs)}>{relTime(base.updatedAtMs)}</span> by {base.updatedBy}
        {base.note && <> ({base.note})</>}.
      </p>
      {newer && !conflict && <Notice kind="warn">Version v{l.data!.version} was saved since you loaded this page. Saving will be refused until you reload.</Notice>}
      {conflict && (
        <Notice kind="err">
          <p>{conflict}</p>
          <button
            type="button"
            className="btn sm"
            onClick={() => {
              setBase(undefined)
              setConflict(undefined)
              l.reload()
            }}
          >
            Reload (drops your edits)
          </button>
        </Notice>
      )}
      {saved && <Notice kind="ok">Saved as v{saved}. Every node picks it up on its next policy poll.</Notice>}

      <div className="tuning">
        {SECTIONS.map((s) => (
          <Panel key={s.id} id={`tune-${s.id}`} title={s.title} desc={s.desc}>
            <div className="knobs">{s.knobs.map(row)}</div>
          </Panel>
        ))}

        <Panel flush title="Spam signals" desc="Per host (counted by the host's owner) and per account (by the DID's owner). A limit of 0 turns a signal off. A per-account signal that throttles throttles the account's host.">
          <div className="table-wrap">
            <table className="data compact tune-table">
              <thead>
                <tr>
                  <th>Signal</th>
                  <th>Per</th>
                  <th>Limit</th>
                  <th>Window</th>
                  <th>Action</th>
                  <th>Default</th>
                </tr>
              </thead>
              <tbody>
                {SIGNALS.map((g) => {
                  const p = ['spam', g.k]
                  const v = get(draft, p) as Json | undefined
                  const def = d ? (get(d, p) as Json | undefined) : undefined
                  const edited = !same(v, get(base.policy, p))
                  return (
                    <tr key={g.k} className={edited ? 'edited' : undefined}>
                      <td>{g.label}</td>
                      <td className="muted">{g.per}</td>
                      <td>
                        <Input id={`s-${g.k}-l`} value={v?.limit} kind={{ t: 'num' }} onChange={set([...p, 'limit'])} />
                      </td>
                      <td>
                        <Input id={`s-${g.k}-w`} value={v?.windowSecs} kind={{ t: 'num', int: true, unit: 's' }} onChange={set([...p, 'windowSecs'])} />
                      </td>
                      <td>
                        <Input id={`s-${g.k}-a`} value={v?.action} kind={{ t: 'enum', options: ACTIONS }} onChange={set([...p, 'action'])} />
                      </td>
                      <td className="mono muted small">{def ? `${def.limit} / ${def.windowSecs} s, ${def.action}` : '—'}</td>
                    </tr>
                  )
                })}
              </tbody>
            </table>
          </div>
          <div className="knobs pad">
            {row({ path: ['spam', 'trackHosts'], label: 'Hosts tracked per signal', why: 'Only the heaviest keys are tracked, so memory stays fixed however many hosts are noisy.', kind: { t: 'num', int: true } })}
            {row({ path: ['spam', 'trackAccounts'], label: 'Accounts tracked per signal', why: 'The same for DIDs.', kind: { t: 'num', int: true } })}
          </div>
        </Panel>

        <Panel flush title="Per-tier extras" desc="Tier limits Limits doesn't show. Read rate pauses the host's socket past it, so the PDS buffers instead of the relay dropping. Auto-throttle off keeps a tier out of the error and spam budgets.">
          <div className="table-wrap">
            <table className="data compact tune-table">
              <thead>
                <tr>
                  <th>Tier</th>
                  {TIER_EXTRAS.map((f) => (
                    <th key={f.k}>{f.label}</th>
                  ))}
                </tr>
              </thead>
              <tbody>
                {TIERS.map((t) => (
                  <tr key={t}>
                    <td>
                      <TierPill tier={t} />
                    </td>
                    {TIER_EXTRAS.map((f) => {
                      const p = ['tiers', t, f.k]
                      const v = get(draft, p)
                      const def = d ? get(d, p) : undefined
                      return (
                        <td key={f.k} className={same(v, get(base.policy, p)) ? undefined : 'edited'} title={def !== undefined ? `default ${fmtVal(def, f.kind)}` : undefined}>
                          <Input id={`t-${t}-${f.k}`} value={v} kind={f.kind} onChange={set(p)} />
                          {f.kind.t === 'num' && f.kind.bytes && <span className="muted small"> {fmtBytes(Number(v))}/s</span>}
                        </td>
                      )
                    })}
                  </tr>
                ))}
              </tbody>
            </table>
          </div>
        </Panel>
      </div>

      <div className={`savebar${changes.length ? ' dirty' : ''}`}>
        <span>
          {changes.length ? (
            <>
              <b>{changes.length}</b> change{changes.length === 1 ? '' : 's'} against v{base.version}
            </>
          ) : (
            <span className="muted">No changes</span>
          )}
        </span>
        {problems.length > 0 && <span className="err-hi small">{problems[0]}{problems.length > 1 && ` (+${problems.length - 1} more)`}</span>}
        <input type="text" placeholder="Note for the audit log (why)" value={note} onChange={(e) => setNote(e.target.value)} aria-label="Change note" />
        <button type="button" className="btn quiet" disabled={!changes.length} onClick={() => (setDraft(base.policy), setReview(false))}>
          Discard
        </button>
        <button type="button" className="btn primary" disabled={!changes.length || problems.length > 0 || !!conflict} onClick={() => setReview(true)}>
          Review and save
        </button>
      </div>
      <InlineConfirm
        open={review}
        action={`Save as v${base.version + 1}`}
        busy={save.busy}
        error={save.error ? errText(save.error) : undefined}
        onConfirm={() => save.run()}
        onCancel={() => setReview(false)}
      >
        <Changes list={changes} />
      </InlineConfirm>
    </>
  )
}

function Changes({ list }: { list: string[] }): ReactNode {
  return (
    <>
      <p>Every node applies these within seconds:</p>
      <ul className="changes">
        {list.map((c) => (
          <li key={c} className="mono">
            {c}
          </li>
        ))}
      </ul>
    </>
  )
}
