import { useEffect, useMemo, useState } from 'react'
import { ErrorNotice, Loading, Notice, Panel } from '../components/ui'
import { AuditTable, InlineConfirm, Live, TierPill } from '../components/relay'
import { api, ApiError, errText, type FullPolicyDoc, type Policy as PolicyT, type PolicyAudit, type PolicyDoc, type SpamThresholds, type TierLimits } from '../lib/api'
import { fmtTime, relTime } from '../lib/format'
import { useAction } from '../lib/hooks'
import { useApi } from '../lib/useApi'
import './pages2.css'

// Tiers as a list so a row can be renamed without losing its place.
type Draft = { tiers: { name: string; limits: TierLimits }[]; defaultTier: string; spam: SpamThresholds }

const draftOf = (p: PolicyT): Draft => ({
  tiers: Object.entries(p.tiers).map(([name, limits]) => ({ name, limits: { ...limits } })),
  defaultTier: p.defaultTier,
  spam: { ...p.spam },
})

const policyOf = (d: Draft): PolicyT => ({
  tiers: Object.fromEntries(d.tiers.map((t) => [t.name, t.limits])),
  defaultTier: d.defaultTier,
  spam: d.spam,
})

const TIER_FIELDS: { k: keyof TierLimits; label: string; int: boolean }[] = [
  { k: 'eventsPerSec', label: 'Events/s', int: false },
  { k: 'eventsPerHour', label: 'Events/h', int: true },
  { k: 'eventsPerDay', label: 'Events/day', int: true },
  { k: 'maxAccounts', label: 'Max accounts', int: true },
  { k: 'newAccountsPerHour', label: 'New accounts/h', int: true },
]

/** Mirrors `validate_policy` in src/admin.rs, plus the integer checks serde would reject. Keys are input ids. */
function validate(d: Draft): Map<string, string> {
  const e = new Map<string, string>()
  if (!d.tiers.length) e.set('tiers', 'At least one tier is required')
  const seen = new Set<string>()
  d.tiers.forEach((t, i) => {
    if (!t.name || !/^[a-z0-9-]+$/.test(t.name)) e.set(`t${i}.name`, `Tier name ${JSON.stringify(t.name)}: use a-z, 0-9 and -`)
    else if (seen.has(t.name)) e.set(`t${i}.name`, `Tier ${t.name} is defined twice`)
    seen.add(t.name)
    for (const f of TIER_FIELDS) {
      const v = t.limits[f.k]
      if (!Number.isFinite(v) || v < 0 || (f.int && !Number.isInteger(v))) e.set(`t${i}.${f.k}`, `tiers.${t.name}.${f.k} must be a ${f.int ? 'whole number' : 'number'} ≥ 0`)
    }
    const l = t.limits
    if (!e.has(`t${i}.eventsPerSec`) && !(l.eventsPerSec > 0)) e.set(`t${i}.eventsPerSec`, `tiers.${t.name}.eventsPerSec must be > 0`)
    if (!e.has(`t${i}.eventsPerHour`) && l.eventsPerHour < l.eventsPerSec) e.set(`t${i}.eventsPerHour`, `tiers.${t.name}.eventsPerHour is below one second's worth`)
    if (!e.has(`t${i}.eventsPerDay`) && l.eventsPerDay < l.eventsPerHour) e.set(`t${i}.eventsPerDay`, `tiers.${t.name}.eventsPerDay is below eventsPerHour`)
  })
  if (!d.tiers.some((t) => t.name === d.defaultTier)) e.set('defaultTier', `Default tier ${JSON.stringify(d.defaultTier)} is not defined`)
  const s = d.spam
  if (!(s.rejectRatio >= 0 && s.rejectRatio <= 1)) e.set('rejectRatio', 'Reject ratio must be between 0 and 100%')
  if (!(Number.isFinite(s.accountEventsPerSec) && s.accountEventsPerSec > 0)) e.set('accountEventsPerSec', 'Per-account events/s must be > 0')
  for (const k of ['newAccountsPerHour', 'badSignaturesPerMin'] as const) {
    if (!Number.isInteger(s[k]) || s[k] < 0) e.set(k, `spam.${k} must be a whole number ≥ 0`)
  }
  return e
}

/** `path: old → new` per changed leaf, in the server's format (sorted keys, `—` for absent). */
export function diffJson(a: unknown, b: unknown, path = '', out: string[] = []): string[] {
  const isObj = (x: unknown): x is Record<string, unknown> => typeof x === 'object' && x !== null && !Array.isArray(x)
  if (isObj(a) && isObj(b)) {
    const keys = [...new Set([...Object.keys(a), ...Object.keys(b)])].sort()
    for (const k of keys) {
      const p = path ? `${path}.${k}` : k
      if (k in a && k in b) diffJson(a[k], b[k], p, out)
      else if (k in a) out.push(`${p}: ${JSON.stringify(a[k])} → —`)
      else out.push(`${p}: — → ${JSON.stringify(b[k])}`)
    }
  } else if (JSON.stringify(a) !== JSON.stringify(b)) out.push(`${path}: ${JSON.stringify(a)} → ${JSON.stringify(b)}`)
  return out
}

const numVal = (v: number) => (Number.isNaN(v) ? '' : v)
const parse = (s: string) => (s.trim() === '' ? NaN : Number(s))

export function Policy() {
  const l = useApi<PolicyDoc>('policy', undefined, 10000)
  const audit = useApi<PolicyAudit[]>('policy/audit', undefined, 10000)
  const [base, setBase] = useState<PolicyDoc>()
  const [draft, setDraft] = useState<Draft>()
  const [review, setReview] = useState(false)
  const [confirm, setConfirm] = useState(false)
  const [note, setNote] = useState('')
  const [conflict, setConflict] = useState<string>()
  const [saved, setSaved] = useState<number>()

  // the editor starts from the first load; later polls only report that a newer version exists
  useEffect(() => {
    if (l.data && !base) {
      setBase(l.data)
      setDraft(draftOf(l.data.policy))
    }
  }, [l.data, base])

  const errors = useMemo(() => (draft ? validate(draft) : new Map<string, string>()), [draft])
  const changes = useMemo(() => (draft && base ? diffJson(base.policy, policyOf(draft)) : []), [draft, base])
  const save = useAction(async () => {
    if (!draft || !base) return
    try {
      const doc = await api<PolicyDoc>('policy', { method: 'PUT', body: { baseVersion: base.version, policy: policyOf(draft), note: note.trim() } })
      setBase(doc)
      setDraft(draftOf(doc.policy))
      setReview(false)
      setConfirm(false)
      setNote('')
      setSaved(doc.version)
      l.reload()
      audit.reload()
    } catch (e) {
      if (e instanceof ApiError && e.status === 409) {
        setConflict(e.message)
        setConfirm(false)
        return
      }
      throw e
    }
  })

  const reloadLatest = () => {
    setBase(undefined)
    setDraft(undefined)
    setReview(false)
    setConfirm(false)
    setConflict(undefined)
    setNote('')
    l.reload()
  }

  if (!draft || !base)
    return (
      <>
        <div className="console-head">
          <h1>Policy</h1>
        </div>
        <ErrorNotice error={l.error} />
        <Loading />
      </>
    )

  const orig = draftOf(base.policy)
  const origTier = (name: string) => orig.tiers.find((t) => t.name === name)
  const cls = (id: string, changed: boolean) => [errors.has(id) ? 'invalid' : '', changed ? 'changed' : ''].filter(Boolean).join(' ')
  const setTier = (i: number, t: Draft['tiers'][number]) => setDraft({ ...draft, tiers: draft.tiers.map((x, j) => (j === i ? t : x)) })
  const setSpam = (s: Partial<SpamThresholds>) => setDraft({ ...draft, spam: { ...draft.spam, ...s } })
  const newer = l.data && l.data.version > base.version
  const dirty = changes.length > 0

  return (
    <>
      <div className="console-head">
        <h1>Policy</h1>
        <Live at={l.at} error={l.error} every={10000} />
      </div>
      <p className="muted small">
        Tier limits and spam thresholds are one versioned object: a save lands on every node at once, and a save against a stale version is refused.{' '}
        {base.version === 0 ? (
          <>The relay runs on the defaults: nothing has been saved yet.</>
        ) : (
          <>
            <span className="mono">v{base.version}</span>, updated <span title={fmtTime(base.updatedAtMs)}>{relTime(base.updatedAtMs)}</span> by {base.updatedBy}.
          </>
        )}
      </p>
      {saved !== undefined && !dirty && <Notice kind="ok">Saved as version {saved}.</Notice>}
      {conflict && (
        <Notice kind="err">
          <p>
            <b>Someone else saved first.</b> {conflict}
          </p>
          <p>Your edits are still in the form. Reload the latest version and reapply them (the diff below lists what you changed).</p>
          <button type="button" className="btn sm" onClick={reloadLatest}>
            Reload latest (discards your edits)
          </button>
        </Notice>
      )}
      {newer && !conflict && (
        <Notice kind="warn">
          <p>
            Version {l.data!.version} was saved by {l.data!.updatedBy} {relTime(l.data!.updatedAtMs)}. You're editing version {base.version}, so saving will be refused.
          </p>
          <button type="button" className="btn sm" onClick={reloadLatest}>
            Reload latest{dirty ? ' (discards your edits)' : ''}
          </button>
        </Notice>
      )}

      <Panel
        title="Tiers"
        desc="Limits enforced per host. A host's tier comes from a domain rule, an operator, or the default for newly crawled hosts."
        actions={
          <button
            type="button"
            className="btn sm"
            onClick={() => {
              const t = draft.tiers.find((x) => x.name === draft.defaultTier) ?? draft.tiers[0]
              setDraft({ ...draft, tiers: [...draft.tiers, { name: '', limits: t ? { ...t.limits } : { eventsPerSec: 10, eventsPerHour: 20000, eventsPerDay: 200000, maxAccounts: 1000, newAccountsPerHour: 50 } }] })
            }}
          >
            Add tier
          </button>
        }
        flush
      >
        <div className="table-wrap">
          <table className="data compact policy-table">
            <thead>
              <tr>
                <th>Tier</th>
                {TIER_FIELDS.map((f) => (
                  <th key={f.k} className="num">
                    {f.label}
                  </th>
                ))}
                <th />
              </tr>
            </thead>
            <tbody>
              {draft.tiers.map((t, i) => {
                const o = origTier(t.name)
                return (
                  <tr key={i}>
                    <td>
                      <input
                        type="text"
                        value={t.name}
                        onChange={(e) => setTier(i, { ...t, name: e.target.value })}
                        className={`mono-in tier-name ${cls(`t${i}.name`, !o)}`}
                        aria-label="Tier name"
                        spellCheck={false}
                        placeholder="name"
                      />
                    </td>
                    {TIER_FIELDS.map((f) => (
                      <td key={f.k} className="num">
                        <input
                          type="number"
                          min={0}
                          step={f.int ? 1 : 'any'}
                          value={numVal(t.limits[f.k])}
                          onChange={(e) => setTier(i, { ...t, limits: { ...t.limits, [f.k]: parse(e.target.value) } })}
                          className={`num-in ${cls(`t${i}.${f.k}`, !!o && o.limits[f.k] !== t.limits[f.k])}`}
                          aria-label={`${t.name || 'new tier'} ${f.label}`}
                          title={errors.get(`t${i}.${f.k}`)}
                        />
                      </td>
                    ))}
                    <td className="num">
                      <button
                        type="button"
                        className="btn sm quiet danger"
                        disabled={draft.tiers.length <= 1}
                        onClick={() => setDraft({ ...draft, tiers: draft.tiers.filter((_, j) => j !== i) })}
                        aria-label={`Remove tier ${t.name}`}
                      >
                        Remove
                      </button>
                    </td>
                  </tr>
                )
              })}
            </tbody>
          </table>
        </div>
        <div className="policy-default">
          <label>
            <span className="label">Default tier for new hosts</span>
            <select value={draft.defaultTier} onChange={(e) => setDraft({ ...draft, defaultTier: e.target.value })} className={cls('defaultTier', draft.defaultTier !== orig.defaultTier)}>
              {!draft.tiers.some((t) => t.name === draft.defaultTier) && <option value={draft.defaultTier}>{draft.defaultTier} (removed)</option>}
              {draft.tiers
                .filter((t) => t.name)
                .map((t) => (
                  <option key={t.name} value={t.name}>
                    {t.name}
                  </option>
                ))}
            </select>
          </label>
          <span className="muted small">
            Now: <TierPill tier={orig.defaultTier} />
          </span>
        </div>
      </Panel>

      <Panel title="Spam thresholds" desc="Crossing one on a host opens a case. With auto-throttle on, high and critical cases also throttle the host to the probation tier's rate.">
        <div className="spam-grid">
          <NumField
            label="New accounts per hour, per host"
            value={draft.spam.newAccountsPerHour}
            onChange={(v) => setSpam({ newAccountsPerHour: v })}
            className={cls('newAccountsPerHour', draft.spam.newAccountsPerHour !== orig.spam.newAccountsPerHour)}
            int
          />
          <NumField
            label="Reject ratio, 5 min (%)"
            value={Number.isNaN(draft.spam.rejectRatio) ? NaN : Math.round(draft.spam.rejectRatio * 10000) / 100}
            onChange={(v) => setSpam({ rejectRatio: Number.isNaN(v) ? NaN : v / 100 })}
            className={cls('rejectRatio', draft.spam.rejectRatio !== orig.spam.rejectRatio)}
          />
          <NumField
            label="Bad signatures per minute, per host"
            value={draft.spam.badSignaturesPerMin}
            onChange={(v) => setSpam({ badSignaturesPerMin: v })}
            className={cls('badSignaturesPerMin', draft.spam.badSignaturesPerMin !== orig.spam.badSignaturesPerMin)}
            int
          />
          <NumField
            label="Events/s from one account"
            value={draft.spam.accountEventsPerSec}
            onChange={(v) => setSpam({ accountEventsPerSec: v })}
            className={cls('accountEventsPerSec', draft.spam.accountEventsPerSec !== orig.spam.accountEventsPerSec)}
          />
          <label className={`check auto-throttle${draft.spam.autoThrottle !== orig.spam.autoThrottle ? ' changed-check' : ''}`}>
            <input type="checkbox" checked={draft.spam.autoThrottle} onChange={(e) => setSpam({ autoThrottle: e.target.checked })} />
            <span>Throttle hosts automatically when a high or critical case opens</span>
          </label>
        </div>
      </Panel>

      {errors.size > 0 && (
        <Notice kind="err">
          <ul className="err-list">
            {[...new Set(errors.values())].map((m) => (
              <li key={m}>{m}</li>
            ))}
          </ul>
        </Notice>
      )}

      <div className="policy-save">
        <span className="muted small">{dirty ? `${changes.length} change${changes.length === 1 ? '' : 's'} not saved` : 'No changes'}</span>
        <div className="row">
          <button type="button" className="btn sm" disabled={!dirty} onClick={() => setDraft(draftOf(base.policy))}>
            Discard
          </button>
          <button type="button" className="btn sm primary" disabled={!dirty || errors.size > 0} onClick={() => setReview(true)}>
            Review changes
          </button>
        </div>
      </div>

      {review && dirty && (
        <Panel title={`Changes against version ${base.version}`}>
          <div className="diff" role="list">
            {changes.map((c) => {
              const at = c.indexOf(': ')
              const path = c.slice(0, at)
              const [from, to] = c.slice(at + 2).split(' → ')
              return (
                <div key={c} role="listitem">
                  <div className="del">
                    − {path}: {from}
                  </div>
                  <div className="add">
                    + {path}: {to}
                  </div>
                </div>
              )
            })}
          </div>
          <label className="field">
            <span className="label">Note for the audit log</span>
            <input type="text" value={note} onChange={(e) => setNote(e.target.value)} placeholder="Why this change" />
          </label>
          <div className="row end">
            <button type="button" className="btn sm" onClick={() => setReview(false)}>
              Back to editing
            </button>
            <button type="button" className="btn sm primary" disabled={errors.size > 0 || confirm} onClick={() => setConfirm(true)}>
              Save as version {base.version + 1}
            </button>
          </div>
          <InlineConfirm
            open={confirm}
            action="Save policy"
            busy={save.busy}
            error={save.error ? errText(save.error) : undefined}
            onConfirm={() => save.run()}
            onCancel={() => setConfirm(false)}
          >
            Apply {changes.length} change{changes.length === 1 ? '' : 's'} on every node. Hosts pick up new limits within a few seconds.
          </InlineConfirm>
        </Panel>
      )}

      <AdvancedPolicy
        onSaved={() => {
          // the form shares the version counter: an untouched form follows the save, an edited one gets the newer-version warning
          if (!dirty)
            api<PolicyDoc>('policy')
              .then((doc) => {
                setBase(doc)
                setDraft(draftOf(doc.policy))
              })
              .catch(() => undefined)
          l.reload()
          audit.reload()
        }}
      />

      <Panel title="Audit log" flush>
        <ErrorNotice error={audit.error} />
        {!audit.data ? <Loading /> : <AuditTable rows={audit.data} />}
      </Panel>
    </>
  )
}

const pretty = (v: unknown) => JSON.stringify(v, null, 2)

/** The engine's whole policy document as JSON, for settings the form above doesn't cover. Hidden on relays without one (404). */
function AdvancedPolicy({ onSaved }: { onSaved: () => void }) {
  const [missing, setMissing] = useState(false)
  const l = useApi<FullPolicyDoc>('policy/full', undefined, missing ? undefined : 10000)
  const [open, setOpen] = useState(false)
  const [base, setBase] = useState<FullPolicyDoc>()
  const [text, setText] = useState('')
  const [note, setNote] = useState('')
  const [confirm, setConfirm] = useState(false)
  const [conflict, setConflict] = useState<string>()
  const [saved, setSaved] = useState<number>()

  useEffect(() => {
    if (l.error instanceof ApiError && l.error.status === 404) setMissing(true)
  }, [l.error])
  useEffect(() => {
    if (l.data && !base) {
      setBase(l.data)
      setText(pretty(l.data.policy))
    }
  }, [l.data, base])

  const parsed = useMemo((): { ok: true; v: unknown } | { ok: false; err: string } => {
    try {
      return { ok: true, v: JSON.parse(text) }
    } catch (e) {
      return { ok: false, err: errText(e) }
    }
  }, [text])
  const changes = useMemo(() => (base && parsed.ok ? diffJson(base.policy, parsed.v) : []), [base, parsed])
  const notObject = parsed.ok && (typeof parsed.v !== 'object' || parsed.v === null || Array.isArray(parsed.v))

  const save = useAction(async () => {
    if (!base || !parsed.ok) return
    try {
      const doc = await api<FullPolicyDoc>('policy/full', { method: 'PUT', body: { baseVersion: base.version, policy: parsed.v, note: note.trim() } })
      setBase(doc)
      setText(pretty(doc.policy))
      setNote('')
      setConfirm(false)
      setSaved(doc.version)
      l.reload()
      onSaved()
    } catch (e) {
      if (e instanceof ApiError && e.status === 409) {
        setConflict(e.message)
        setConfirm(false)
        return
      }
      throw e
    }
  })

  const reloadLatest = () => {
    setBase(undefined)
    setConflict(undefined)
    setConfirm(false)
    setSaved(undefined)
    l.reload()
  }

  if (missing || (!l.data && !l.error)) return null
  const newer = base && l.data && l.data.version > base.version
  const dirty = changes.length > 0

  return (
    <Panel
      title="Advanced"
      desc="The whole policy document: per-tier limits beyond the ones above, tier transitions, spam actions, cluster budgets, consumer limits and crawl settings. Saved and versioned like the form above."
      actions={
        <button type="button" className="btn sm" aria-expanded={open} onClick={() => setOpen(!open)}>
          {open ? 'Hide' : 'Edit JSON'}
        </button>
      }
    >
      {!open ? (
        base ? (
          <p className="muted small">
            {base.version === 0 ? (
              <>Defaults, never saved</>
            ) : (
              <>
                <span className="mono">v{base.version}</span>, updated <span title={fmtTime(base.updatedAtMs)}>{relTime(base.updatedAtMs)}</span> by {base.updatedBy}
              </>
            )}
            {base.note && <> ({base.note})</>}.{dirty && ` ${changes.length} unsaved change${changes.length === 1 ? '' : 's'}.`}
          </p>
        ) : (
          <ErrorNotice error={l.error} />
        )
      ) : !base ? (
        <ErrorNotice error={l.error} />
      ) : (
        <>
          {saved !== undefined && !dirty && <Notice kind="ok">Saved as version {saved}.</Notice>}
          {conflict && (
            <Notice kind="err">
              <p>
                <b>Someone else saved first.</b> {conflict}
              </p>
              <button type="button" className="btn sm" onClick={reloadLatest}>
                Reload latest (discards your edits)
              </button>
            </Notice>
          )}
          {newer && !conflict && (
            <Notice kind="warn">
              <p>
                Version {l.data!.version} was saved by {l.data!.updatedBy} {relTime(l.data!.updatedAtMs)}. You're editing version {base.version}, so saving will be refused.
              </p>
              <button type="button" className="btn sm" onClick={reloadLatest}>
                Reload latest{dirty ? ' (discards your edits)' : ''}
              </button>
            </Notice>
          )}
          <textarea
            className={`policy-json${parsed.ok && !notObject ? '' : ' invalid'}`}
            value={text}
            onChange={(e) => {
              setText(e.target.value)
              setConfirm(false)
            }}
            spellCheck={false}
            aria-label="Policy document (JSON)"
            rows={24}
          />
          {!parsed.ok && <div className="field-err">Not valid JSON: {parsed.err}</div>}
          {notObject && <div className="field-err">The policy document must be a JSON object</div>}
          {dirty && (
            <div className="diff policy-json-diff" role="list">
              {changes.map((c) => (
                <div key={c} role="listitem" className="mono small">
                  {c}
                </div>
              ))}
            </div>
          )}
          <div className="policy-json-save">
            <input type="text" value={note} onChange={(e) => setNote(e.target.value)} placeholder="Note for the audit log (why)" aria-label="Note" />
            <button type="button" className="btn sm" disabled={!dirty && parsed.ok} onClick={() => setText(pretty(base.policy))}>
              Discard
            </button>
            <button type="button" className="btn sm primary" disabled={!dirty || !parsed.ok || notObject || confirm} onClick={() => setConfirm(true)}>
              Save as version {base.version + 1}
            </button>
          </div>
          <InlineConfirm
            open={confirm}
            action="Save policy"
            busy={save.busy}
            error={save.error ? errText(save.error) : undefined}
            onConfirm={() => save.run()}
            onCancel={() => setConfirm(false)}
          >
            Apply {changes.length} change{changes.length === 1 ? '' : 's'} on every node. The relay validates the whole document and refuses it with a reason if anything is off.
          </InlineConfirm>
        </>
      )}
    </Panel>
  )
}

function NumField({ label, value, onChange, className, int }: { label: string; value: number; onChange: (v: number) => void; className: string; int?: boolean }) {
  return (
    <label className="field">
      <span className="label">{label}</span>
      <input type="number" min={0} step={int ? 1 : 'any'} value={numVal(value)} onChange={(e) => onChange(parse(e.target.value))} className={className} />
    </label>
  )
}
