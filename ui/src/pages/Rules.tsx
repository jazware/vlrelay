import { useState } from 'react'
import { ErrorNotice, Empty, Loading, Panel, Spinner } from '../components/ui'
import { InlineConfirm, Live, TierPill } from '../components/relay'
import { api, errText, type DomainRule, type DomainRuleInput, type PolicyDoc, type RuleEffect } from '../lib/api'
import { fmtNum, fmtTime, relTime } from '../lib/format'
import { useAction } from '../lib/hooks'
import { Link } from '../lib/router'
import { useApi } from '../lib/useApi'
import './pages2.css'

type Kind = RuleEffect['kind']

/** The same check the server makes: a hostname, or `*.` plus a domain. */
export function patternError(p: string): string | undefined {
  const s = p.trim().toLowerCase()
  if (!s) return 'Enter a hostname or *.domain'
  const base = s.startsWith('*.') ? s.slice(2) : s
  const ok = base.includes('.') && base.split('.').every((l) => l.length > 0 && l.length <= 63 && /^[a-z0-9-]+$/.test(l))
  return ok ? undefined : 'Use a hostname (pds.example.com) or *.example.com'
}

const baseDomain = (p: string) => (p.startsWith('*.') ? p.slice(2) : p)

function EffectPill({ e }: { e: RuleEffect }) {
  if (e.kind === 'ban') return <span className="pill danger">ban</span>
  if (e.kind === 'tier')
    return (
      <span className="effect">
        tier <TierPill tier={e.tier} />
      </span>
    )
  return <span className="pill amber">throttle {fmtNum(e.eventsPerSec, 1)} ev/s</span>
}

type Draft = { pattern: string; kind: Kind; tier: string; eps: string; note: string }

const draftOf = (r?: DomainRule, tier = 'probation'): Draft => ({
  pattern: r?.pattern ?? '',
  kind: r?.effect.kind ?? 'ban',
  tier: r?.effect.kind === 'tier' ? r.effect.tier : tier,
  eps: r?.effect.kind === 'throttle' ? String(r.effect.eventsPerSec) : '5',
  note: r?.note ?? '',
})

function inputOf(d: Draft): DomainRuleInput {
  const effect: RuleEffect = d.kind === 'ban' ? { kind: 'ban' } : d.kind === 'tier' ? { kind: 'tier', tier: d.tier } : { kind: 'throttle', eventsPerSec: Number(d.eps) }
  return { pattern: d.pattern.trim().toLowerCase(), effect, note: d.note.trim() }
}

function draftError(d: Draft): string | undefined {
  const pe = patternError(d.pattern)
  if (pe) return pe
  if (d.kind === 'throttle' && !(d.eps.trim() !== '' && Number.isFinite(Number(d.eps)) && Number(d.eps) >= 0)) return 'Throttle must be a number ≥ 0'
  return undefined
}

function EffectInputs({ d, set, tiers }: { d: Draft; set: (d: Draft) => void; tiers: string[] }) {
  return (
    <>
      <select value={d.kind} onChange={(e) => set({ ...d, kind: e.target.value as Kind })} aria-label="Effect">
        <option value="ban">Ban</option>
        <option value="tier">Set tier</option>
        <option value="throttle">Throttle</option>
      </select>
      {d.kind === 'tier' && (
        <select value={d.tier} onChange={(e) => set({ ...d, tier: e.target.value })} aria-label="Tier">
          {tiers.map((t) => (
            <option key={t}>{t}</option>
          ))}
        </select>
      )}
      {d.kind === 'throttle' && (
        <input type="number" min={0} step="any" value={d.eps} onChange={(e) => set({ ...d, eps: e.target.value })} aria-label="Events per second" className="num-in" />
      )}
    </>
  )
}

export function Rules() {
  const l = useApi<DomainRule[]>('domain-rules', undefined, 5000)
  const pol = useApi<PolicyDoc>('policy')
  const tiers = Object.keys(pol.data?.policy.tiers ?? { trusted: 0, standard: 0, probation: 0 })
  const [add, setAdd] = useState<Draft>(draftOf())
  const [tried, setTried] = useState(false)
  const create = useAction(async (d: Draft) => {
    await api('domain-rules', { body: inputOf(d) })
    setAdd(draftOf())
    setTried(false)
    l.reload()
  })
  const addErr = draftError(add)
  const rules = l.data

  return (
    <>
      <div className="console-head">
        <h1>Domain rules</h1>
        <Live at={l.at} error={l.error} every={5000} />
      </div>
      <p className="muted small">
        A rule applies to a hostname, or with <code>*.</code> to a domain and every subdomain. It takes effect on matching hosts when saved, and on new hosts as they're crawled.
      </p>
      <Panel title="Add a rule">
        <form
          className="rule-form"
          onSubmit={(e) => {
            e.preventDefault()
            setTried(true)
            if (!addErr) create.run(add)
          }}
        >
          <input
            type="text"
            placeholder="*.example.com"
            value={add.pattern}
            onChange={(e) => setAdd({ ...add, pattern: e.target.value })}
            aria-label="Pattern"
            className={`mono-in${tried && patternError(add.pattern) ? ' invalid' : ''}`}
            spellCheck={false}
            autoCapitalize="off"
          />
          <EffectInputs d={add} set={setAdd} tiers={tiers} />
          <input type="text" placeholder="Note (why)" value={add.note} onChange={(e) => setAdd({ ...add, note: e.target.value })} aria-label="Note" className="note-in" />
          <button className="btn sm primary" disabled={create.busy}>
            {create.busy && <Spinner />}
            Add rule
          </button>
        </form>
        {tried && addErr && <div className="field-err">{addErr}</div>}
        <ErrorNotice error={create.error} />
      </Panel>
      <ErrorNotice error={l.error} />
      {!rules ? (
        <Loading />
      ) : rules.length === 0 ? (
        <Empty title="No domain rules">Every host gets the policy's default tier.</Empty>
      ) : (
        <Panel flush>
          <div className="table-wrap">
            <table className="data compact">
              <thead>
                <tr>
                  <th>Pattern</th>
                  <th>Effect</th>
                  <th className="num">Matches</th>
                  <th>Note</th>
                  <th>Created</th>
                  <th />
                </tr>
              </thead>
              <tbody>
                {rules.map((r) => (
                  <RuleRow key={r.id} r={r} tiers={tiers} reload={l.reload} />
                ))}
              </tbody>
            </table>
          </div>
        </Panel>
      )}
    </>
  )
}

function RuleRow({ r, tiers, reload }: { r: DomainRule; tiers: string[]; reload: () => void }) {
  const [edit, setEdit] = useState<Draft | null>(null)
  const [del, setDel] = useState(false)
  const save = useAction(async (d: Draft) => {
    await api(`domain-rules/${r.id}`, { method: 'PUT', body: inputOf(d) })
    setEdit(null)
    reload()
  })
  const remove = useAction(async () => {
    await api(`domain-rules/${r.id}`, { method: 'DELETE' })
    setDel(false)
    reload()
  })
  if (edit) {
    const err = draftError(edit)
    return (
      <tr className="sel">
        <td colSpan={6}>
          <form
            className="rule-form"
            onSubmit={(e) => {
              e.preventDefault()
              if (!err) save.run(edit)
            }}
            onKeyDown={(e) => e.key === 'Escape' && setEdit(null)}
          >
            <input
              type="text"
              value={edit.pattern}
              onChange={(e) => setEdit({ ...edit, pattern: e.target.value })}
              aria-label="Pattern"
              className={`mono-in${patternError(edit.pattern) ? ' invalid' : ''}`}
              autoFocus
              spellCheck={false}
            />
            <EffectInputs d={edit} set={setEdit} tiers={tiers} />
            <input type="text" value={edit.note} onChange={(e) => setEdit({ ...edit, note: e.target.value })} aria-label="Note" className="note-in" />
            <button type="button" className="btn sm" onClick={() => setEdit(null)}>
              Cancel <kbd>Esc</kbd>
            </button>
            <button className="btn sm primary" disabled={!!err || save.busy}>
              {save.busy && <Spinner />}
              Save
            </button>
          </form>
          {err && <div className="field-err">{err}</div>}
          {save.error ? <div className="field-err">{errText(save.error)}</div> : null}
        </td>
      </tr>
    )
  }
  return (
    <>
      <tr>
        <td className="mono">{r.pattern}</td>
        <td>
          <EffectPill e={r.effect} />
        </td>
        <td className="num">
          <Link to={`/admin/hosts?q=${encodeURIComponent(baseDomain(r.pattern))}`}>{fmtNum(r.matches)}</Link>
        </td>
        <td className="wrap-cell">{r.note || <span className="muted">—</span>}</td>
        <td title={fmtTime(r.createdAtMs)}>
          {relTime(r.createdAtMs)} <span className="muted">by {r.createdBy}</span>
        </td>
        <td className="num">
          <div className="row nowrap-row">
            <button type="button" className="btn sm quiet" onClick={() => setEdit(draftOf(r))}>
              Edit
            </button>
            <button type="button" className="btn sm quiet danger" onClick={() => setDel(true)}>
              Delete
            </button>
          </div>
        </td>
      </tr>
      {del && (
        <tr>
          <td colSpan={6}>
            <InlineConfirm
              open
              danger
              action="Delete rule"
              busy={remove.busy}
              error={remove.error ? errText(remove.error) : undefined}
              onConfirm={() => remove.run()}
              onCancel={() => setDel(false)}
            >
              Delete the rule for <b className="mono">{r.pattern}</b>? Hosts it already changed keep their current state (unban or retier them on the host page).
            </InlineConfirm>
          </td>
        </tr>
      )}
    </>
  )
}
