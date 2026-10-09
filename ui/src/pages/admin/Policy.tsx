import { useEffect, useMemo, useState, type ReactNode } from 'react'
import { DataTable, type Col } from '../../components/console/DataTable'
import { confirmAction, FormDialog, openDialog } from '../../components/console/dialogs'
import { registerDetail } from '../../components/console/Drawer'
import { openPanel } from '../../components/console/nav'
import { registerPalette } from '../../components/console/Palette'
import { toast } from '../../components/console/toast'
import { Banners, Chip, Empty, ErrorState, KV, Loaded, Meter, PageHead, Panel, Src, TierTag, Updated, type BannerSpec } from '../../components/console/kit'
import { ApiError, errText, type Case, type PolicyAudit, type PolicyUsage } from '../../lib/api'
import * as A from '../../lib/console/adminAdapter'
import { ago, dt, dur, fmtNum, plural } from '../../lib/console/fmt'
import { cached, keys } from '../../lib/console/cache'
import { readPolicySource, useCapHosts, useConsumers, useHostList, useOpenCases, useOverview, usePolicyAudit, usePolicyDefaults, usePolicySource, usePolicyUsage, useSignals, useTierCounts } from '../../lib/console/queries'
import * as W from '../../lib/console/writes'
import {
  applyUndo,
  changesOf,
  discard,
  getDraft,
  getIn,
  rebase,
  saved,
  setBody,
  setField,
  showVal,
  undoOf,
  useDraft,
  type Change,
  type Json,
  type PolicyBase,
} from '../../lib/console/policyDraft'
import { Link } from '../../lib/router'
import { useRelay } from '../../lib/console/relay'
import { openSignalKey, patternError } from './moderationDetail'
import '../../console-rules.css'

// The policy is one versioned document. Every edit on this page (a tier cell, a knob, a spam
// threshold) goes into one draft; Review shows the diff and the exact PUT with the version it was
// edited from, and a 409 is handled in the dialog by moving the draft onto the newer version.
// History lists every saved version; undo saves a new version with the old values put back.

// ---------------------------------------------------------------- what the page knows about the document

const TIER_ORDER = ['trusted', 'default', 'new', 'throttled']
const tiersOf = (body?: Json) => {
  const t = Object.keys((body?.tiers as Json | undefined) ?? {})
  return [...TIER_ORDER.filter((x) => t.includes(x)), ...t.filter((x) => !TIER_ORDER.includes(x))]
}

type Row = { k: string; label: string; unit?: string; why: string; int?: boolean; bytes?: boolean; zero?: string }
const TIER_ROWS: Row[] = [
  { k: 'eventsPerSec', label: 'Events', unit: '/s', why: 'A host past it is read more slowly; its reader blocks and nothing is dropped.' },
  { k: 'eventsPerHour', label: 'Events', unit: '/h', int: true, zero: 'no limit', why: 'An hourly bucket on top of the rate.' },
  { k: 'eventsPerDay', label: 'Events', unit: '/day', int: true, zero: 'no limit', why: 'A daily bucket on top of that.' },
  { k: 'maxAccounts', label: 'Account cap', int: true, why: 'Accounts on one host. Past it new accounts are created throttled.' },
  { k: 'newAccountsPerHour', label: 'New accounts', unit: '/h', int: true, zero: 'no limit', why: 'Newly created accounts only; past it their events wait.' },
  { k: 'bytesPerSec', label: 'Read rate', unit: '/s', int: true, bytes: true, why: "Bytes a second off the host's socket." },
  { k: 'identityEventsPerHour', label: 'Identity events', unit: '/h', int: true, zero: 'no limit', why: 'Accepted #identity events; past it they’re dropped.' },
  { k: 'reconnectsPerHour', label: 'Reconnects', unit: '/h', int: true, zero: 'no limit', why: 'How often the relay may reconnect to the host.' },
]

export type Signal = { k: string; label: string; per: 'host' | 'account'; kinds: string[] }
/** The spam signals, and the case kinds each opens (the demo's older names too). */
export const SIGNALS: Signal[] = [
  { k: 'hostNewAccounts', label: 'New accounts', per: 'host', kinds: ['new-accounts'] },
  { k: 'hostFailedValidation', label: 'Failed validation', per: 'host', kinds: ['failed-validation', 'bad-signatures'] },
  { k: 'hostIdentityChurn', label: 'Identity churn', per: 'host', kinds: ['identity-churn'] },
  { k: 'hostOversizedCommits', label: 'Oversized commits', per: 'host', kinds: ['oversized-commits'] },
  { k: 'accountRecords', label: 'Records written', per: 'account', kinds: ['account-records', 'account-rate'] },
  { k: 'accountFailedValidation', label: 'Failed validation', per: 'account', kinds: ['account-failed-validation'] },
  { k: 'accountIdentityChurn', label: 'Identity churn', per: 'account', kinds: ['account-identity-churn'] },
]
export const signalOfKind = (kind: string) => SIGNALS.find((s) => s.kinds.includes(kind))
const ACTIONS = ['alert', 'case', 'throttle', 'throttle-and-case']

const num = (v: unknown) => (typeof v === 'number' ? v : NaN)
const isInt = (v: unknown) => typeof v === 'number' && Number.isInteger(v) && v >= 0

/** The relay's own checks (`policy::doc::validate`, `admin::validate_policy`), by field path. */
function validate(base: PolicyBase, b: Json): Map<string, string> {
  const e = new Map<string, string>()
  const put = (p: string, m: string) => !e.has(p) && e.set(p, m)
  for (const t of tiersOf(b)) {
    const p = `tiers.${t}`
    const eps = num(getIn(b, `${p}.eventsPerSec`))
    if (!(Number.isFinite(eps) && eps > 0)) put(`${p}.eventsPerSec`, `${p}.eventsPerSec must be > 0`)
    for (const r of TIER_ROWS.slice(1)) {
      const v = getIn(b, `${p}.${r.k}`)
      if (v !== undefined && !isInt(v)) put(`${p}.${r.k}`, `${p}.${r.k} must be a whole number ≥ 0`)
    }
    const eph = num(getIn(b, `${p}.eventsPerHour`))
    const epd = num(getIn(b, `${p}.eventsPerDay`))
    if (eph !== 0 && eph < eps) put(`${p}.eventsPerHour`, `${p}.eventsPerHour is below one second's worth`)
    if (epd !== 0 && epd < eph) put(`${p}.eventsPerDay`, `${p}.eventsPerDay is below eventsPerHour`)
  }
  if (base.mode === 'wire') {
    const s = (b.spam ?? {}) as Json
    if (!tiersOf(b).includes(String(b.defaultTier))) put('defaultTier', `The default tier ${JSON.stringify(b.defaultTier)} is not a tier`)
    const rr = num(s.rejectRatio)
    if (!(rr >= 0 && rr <= 1)) put('spam.rejectRatio', 'The reject ratio must be between 0 and 1')
    if (!(num(s.accountEventsPerSec) > 0)) put('spam.accountEventsPerSec', 'Per-account events/s must be > 0')
    for (const k of ['newAccountsPerHour', 'badSignaturesPerMin']) if (!isInt(s[k])) put(`spam.${k}`, `spam.${k} must be a whole number ≥ 0`)
    return e
  }
  const er = num(getIn(b, 'transitions.errorRatio'))
  if (!(er > 0 && er <= 1)) put('transitions.errorRatio', 'transitions.errorRatio must be in (0, 1]')
  for (const k of ['promoteAfterDays', 'recoverAfterSecs', 'errorMinEvents']) if (!isInt(getIn(b, `transitions.${k}`))) put(`transitions.${k}`, `transitions.${k} must be a whole number ≥ 0`)
  for (const s of SIGNALS) {
    const p = `spam.${s.k}`
    if (getIn(b, p) === undefined) continue
    const lim = num(getIn(b, `${p}.limit`))
    const w = getIn(b, `${p}.windowSecs`)
    if (!(Number.isFinite(lim) && lim >= 0)) put(`${p}.limit`, `${p}.limit must be ≥ 0`)
    if (!isInt(w)) put(`${p}.windowSecs`, `${p}.windowSecs must be a whole number of seconds`)
    else if (lim > 0 && w === 0) put(`${p}.windowSecs`, `${p}.windowSecs must be > 0`)
    else if ((w as number) > 86_400) put(`${p}.windowSecs`, `${p}.windowSecs is over a day`)
  }
  for (const k of ['trackHosts', 'trackAccounts']) {
    const v = getIn(b, `spam.${k}`)
    if (v !== undefined && !(isInt(v) && (v as number) >= 16 && (v as number) <= 1 << 20)) put(`spam.${k}`, `spam.${k} must be 16..1048576`)
  }
  for (const k of ['plcLookupsPerSec', 'newAccountsPerMin']) if (!(num(getIn(b, `cluster.${k}`)) > 0)) put(`cluster.${k}`, `cluster.${k} must be > 0`)
  if (!isInt(getIn(b, 'cluster.newHostsPerDay'))) put('cluster.newHostsPerDay', 'cluster.newHostsPerDay must be a whole number ≥ 0')
  for (const k of ['consumersPerNode', 'slowConsumerLagSecs', 'maxBackfillSecs']) {
    const v = getIn(b, `consumers.${k}`)
    if (v !== undefined && !isInt(v)) put(`consumers.${k}`, `consumers.${k} must be a whole number ≥ 0`)
  }
  if (getIn(b, 'discovery') !== undefined) {
    for (const k of ['connectsPerMin', 'requestsPerSec']) if (!(num(getIn(b, `discovery.${k}`)) > 0)) put(`discovery.${k}`, `discovery.${k} must be > 0`)
    const seeds = (getIn(b, 'discovery.seedRelays') as SeedRelay[] | undefined) ?? []
    const seen = new Set<string>()
    for (const r of seeds) {
      const err = seedUrlError(r.url)
      if (err) put('discovery.seedRelays', `discovery.seedRelays: ${err}`)
      else if (seen.has(r.url)) put('discovery.seedRelays', `discovery.seedRelays: ${r.url} is listed twice`)
      else if (!(isInt(r.refreshIntervalSecs) && r.refreshIntervalSecs >= 60)) put('discovery.seedRelays', `discovery.seedRelays: ${r.url} refreshes at least every 60 s`)
      seen.add(r.url)
    }
  }
  const it = getIn(b, 'crawl.initialTier')
  if (it !== undefined && !['trusted', 'default', 'new'].includes(String(it))) put('crawl.initialTier', 'crawl.initialTier must be trusted, default or new')
  const td = getIn(b, 'crawl.trustedDomains')
  if (Array.isArray(td)) for (const d of td) if (patternError(String(d))) put('crawl.trustedDomains', `crawl.trustedDomains: ${JSON.stringify(d)} isn't a hostname or *.domain`)
  return e
}

let memo: { body?: Json; base?: PolicyBase; errs: Map<string, string> } = { errs: new Map() }
function useErrors(): Map<string, string> {
  const d = useDraft()
  if (d.body !== memo.body || d.base !== memo.base) memo = { body: d.body, base: d.base, errs: d.base && d.body ? validate(d.base, d.body) : new Map() }
  return memo.errs
}

// ---------------------------------------------------------------- inputs

const same = (a: unknown, b: unknown) => JSON.stringify(a) === JSON.stringify(b)

const UNIT: Record<string, number> = { k: 1e3, m: 1e6, g: 1e9, t: 1e12, ki: 1024, mi: 1024 ** 2, gi: 1024 ** 3, ti: 1024 ** 4 }

/** What an operator types into a number: "50000000", "50,000,000", "50M", "200 MiB", "no limit" (0). NaN when it isn't one. */
export function parseNum(text: string): number {
  const t = text.trim().toLowerCase().replace(/[,_\s]/g, '').replace(/%$/, '')
  if (t === '') return NaN
  if (t === 'nolimit' || t === 'none') return 0
  const m = t.match(/^(-?\d*\.?\d+(?:e[+-]?\d+)?)(ki|mi|gi|ti|k|m|g|t)?b?(?:\/s)?$/)
  if (!m) return NaN
  return Number(m[1]) * (m[2] ? UNIT[m[2]] : 1)
}

/** Bytes in the largest binary unit that says them exactly ("200 MiB"), else grouped ("2,100,000 B"). */
export function exactBytes(n: number): string {
  if (n === 0) return '0 B'
  for (const [u, f] of [['TiB', 1024 ** 4], ['GiB', 1024 ** 3], ['MiB', 1024 ** 2], ['KiB', 1024]] as const) if (n >= f && n % f === 0) return `${fmtNum(n / f)} ${u}`
  return `${fmtNum(n)} B`
}

/**
 * A number in the draft. At rest it reads grouped ("50,000,000"), as exact bytes ("200 MiB") or as
 * "no limit" for a 0 that means none; focused, it shows the raw value. It takes "50M" or "200 MiB" too.
 */
function NumIn({ path, w = 96, label, ratio, bytes, zero }: { path: string; w?: number; label: string; ratio?: boolean; bytes?: boolean; zero?: string }) {
  const d = useDraft()
  const errs = useErrors()
  const v = getIn(d.body, path)
  const raw = (x: unknown) => (typeof x === 'number' && Number.isFinite(x) ? String(ratio ? Math.round(x * 10000) / 100 : x) : '')
  const [text, setText] = useState(raw(v))
  const [focus, setFocus] = useState(false)
  const n = num(v)
  const none = !focus && !!zero && n === 0
  const shown = focus || !Number.isFinite(n) ? text : ratio ? raw(v) : none ? zero : bytes ? exactBytes(n) : fmtNum(n, n % 1 ? 4 : 0)
  const dirty = !same(v, getIn(d.base?.body, path))
  const err = errs.get(path)
  return (
    <input
      className={`cx-inp num${dirty ? ' dirty' : ''}${err ? ' bad' : ''}${none ? ' none' : ''}`}
      style={{ width: w }}
      inputMode={bytes ? 'text' : 'decimal'}
      aria-label={label}
      aria-invalid={!!err}
      title={err ?? (bytes ? 'Bytes: 209715200, 200 MiB or 200M' : zero ? `0 is ${zero}` : 'Takes 50000, 50,000 or 50k')}
      value={shown}
      onFocus={() => {
        setFocus(true)
        // the draft may have moved under the input (discard, a rebase, the JSON editor)
        if (Number.isFinite(n)) setText(raw(v))
      }}
      onBlur={() => setFocus(false)}
      onChange={(e) => {
        setText(e.target.value)
        const x = parseNum(e.target.value)
        setField(path, Number.isFinite(x) ? (ratio ? x / 100 : x) : NaN)
      }}
    />
  )
}

function ToggleIn({ path, label }: { path: string; label: string }) {
  const d = useDraft()
  const v = !!getIn(d.body, path)
  const dirty = v !== !!getIn(d.base?.body, path)
  return <button type="button" className={`cx-toggle${v ? ' on' : ''}${dirty ? ' dirty' : ''}`} aria-pressed={v} aria-label={label} onClick={() => setField(path, !v)} />
}

function SelectIn({ path, label, options }: { path: string; label: string; options: string[] }) {
  const d = useDraft()
  const v = String(getIn(d.body, path) ?? '')
  const dirty = v !== String(getIn(d.base?.body, path) ?? '')
  return (
    <select className={`cx-inp${dirty ? ' dirty' : ''}`} style={{ height: 26 }} aria-label={label} value={v} onChange={(e) => setField(path, e.target.value)}>
      {!options.includes(v) && <option value={v}>{v}</option>}
      {options.map((o) => (
        <option key={o}>{o}</option>
      ))}
    </select>
  )
}

function ListIn({ path, label }: { path: string; label: string }) {
  const d = useDraft()
  const errs = useErrors()
  const v = (getIn(d.body, path) as string[] | undefined) ?? []
  const [text, setText] = useState(v.join('\n'))
  useEffect(() => {
    if (!same(text.split('\n').map((s) => s.trim()).filter(Boolean), v)) setText(v.join('\n'))
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [JSON.stringify(v)])
  const dirty = !same(v, getIn(d.base?.body, path))
  return (
    <textarea
      className={`cx-inp${dirty ? ' dirty' : ''}${errs.has(path) ? ' bad' : ''}`}
      rows={2}
      aria-label={label}
      spellCheck={false}
      placeholder="*.example.com"
      value={text}
      onChange={(e) => {
        setText(e.target.value)
        setField(
          path,
          e.target.value
            .split('\n')
            .map((s) => s.trim().toLowerCase())
            .filter(Boolean),
        )
      }}
    />
  )
}

const fmtDef = (v: unknown, unit?: string) => (v === undefined ? '—' : typeof v === 'boolean' ? (v ? 'on' : 'off') : Array.isArray(v) ? (v.length ? v.join(', ') : 'none') : typeof v === 'number' ? `${fmtNum(v, v % 1 ? 2 : 0)}${unit ? ` ${unit}` : ''}` : String(v))

/** The default a fresh relay has, magenta when the draft is off it. */
function Def({ path, unit, ratio, quiet, bytes }: { path: string; unit?: string; ratio?: boolean; quiet?: boolean; bytes?: boolean }) {
  const d = useDraft()
  const defs = usePolicyDefaults().data
  if (!defs) return null
  const def = getIn(defs, path)
  if (def === undefined) return null
  const v = getIn(d.body, path)
  if (quiet && same(v, def)) return null
  return (
    <span className={`cx-kd${same(v, def) ? '' : ' chg'}`} title="What a fresh relay has">
      default {ratio && typeof def === 'number' ? `${fmtNum(def * 100, 1)}%` : bytes && typeof def === 'number' ? exactBytes(def) : fmtDef(def, unit)}
    </span>
  )
}

/** One setting: what it is and why, the control, its default and how much of it is in use now. */
function Knob({ path, label, why, unit, opts, ratio, use, chip }: { path: string; label: string; why: ReactNode; unit?: string; opts?: string[]; ratio?: boolean; use?: ReactNode; chip?: ReactNode }) {
  const d = useDraft()
  const v = getIn(d.body, path)
  if (v === undefined) return null
  const ctl =
    typeof v === 'boolean' ? (
      <ToggleIn path={path} label={label} />
    ) : opts ? (
      <SelectIn path={path} label={label} options={opts} />
    ) : Array.isArray(v) ? (
      <ListIn path={path} label={label} />
    ) : (
      <>
        <NumIn path={path} label={label} ratio={ratio} />
        {(unit || ratio) && <span className="muted sm">{ratio ? '%' : unit}</span>}
      </>
    )
  return (
    <div className="cx-knob">
      <div className="kl">
        {label}
        {chip}
        {use && <span className="use">{use}</span>}
      </div>
      <div className="kw">{why}</div>
      <div className="kc">
        {ctl}
        <Def path={path} unit={unit} ratio={ratio} />
      </div>
    </div>
  )
}

/** Use against a limit: a meter and the figure. */
function Use({ v, max, label }: { v: number; max: number; label: ReactNode }) {
  const k = max > 0 && v >= max ? 'err' : max > 0 && v > max * 0.8 ? 'warn' : 'ok'
  return (
    <>
      {max > 0 && <Meter v={v} max={max} k={k} />} <span className="mono">{label}</span>
    </>
  )
}

// ---------------------------------------------------------------- panels

function TierMatrix() {
  const d = useDraft()
  const errs = useErrors()
  const ov = useOverview().data
  const cap = useCapHosts().data
  const tiers = tiersOf(d.body)
  const counts = useTierCounts(tiers)
  const n = new Map(counts.data ?? [])
  const wire = d.base?.mode === 'wire'
  const rows = TIER_ROWS.filter((r) => tiers.some((t) => getIn(d.body, `tiers.${t}.${r.k}`) !== undefined))
  const atCap = (t: string) => (cap?.hosts ?? []).filter((h) => h.tier === t && h.maxAccounts > 0 && h.accounts >= h.maxAccounts).length
  const nearRate = (t: string) => {
    const lim = num(getIn(d.body, `tiers.${t}.eventsPerSec`))
    return (ov?.topHosts ?? []).filter((h) => h.tier === t && lim > 0 && h.eventsPerSec >= lim * 0.9).length
  }
  return (
    <div className="cx-tw">
      <table className="cx-t compact cx-tiers">
        <thead>
          <tr>
            <th>Limit</th>
            {tiers.map((t) => (
              <th key={t} className="tier">
                <span className="th">
                  <TierTag t={t} />
                  <span className="n" title="hosts in this tier now">
                    {n.has(t) ? plural(n.get(t)!, 'host') : ''}
                  </span>
                </span>
              </th>
            ))}
            <th className="fill">What it limits</th>
          </tr>
        </thead>
        <tbody>
          {rows.map((r) => (
            <tr key={r.k}>
              <td className="lim">
                {r.label}
                {r.unit && <span className="muted"> {r.unit}</span>}
              </td>
              {tiers.map((t) => {
                const path = `tiers.${t}.${r.k}`
                const use = r.k === 'maxAccounts' ? atCap(t) : r.k === 'eventsPerSec' ? nearRate(t) : 0
                return (
                  <td key={t} className="tier">
                    <span className="cell">
                      <NumIn path={path} w={118} label={`${t} ${r.label}${r.unit ?? ''}`} bytes={r.bytes} zero={r.zero} />
                      {use > 0 && (
                        <span className="cx-kd" style={{ color: 'var(--warn)' }} title={r.k === 'maxAccounts' ? 'busy hosts at their account cap' : 'busiest hosts within 10% of the rate'}>
                          {r.k === 'maxAccounts' ? `${use} at cap` : `${use} near it`}
                        </span>
                      )}
                      {!wire && <Def path={path} bytes={r.bytes} quiet />}
                      {errs.has(path) && <span className="cx-kd s-err">{errs.get(path)!.replace(/^tiers\.\S+ /, '')}</span>}
                    </span>
                  </td>
                )
              })}
              <td className="fill why">
                {r.why}
                {r.zero ? ' 0 is no limit.' : ''}
              </td>
            </tr>
          ))}
          {tiers.some((t) => typeof getIn(d.body, `tiers.${t}.autoThrottle`) === 'boolean') && (
            <tr>
              <td className="lim">Auto-throttle</td>
              {tiers.map((t) => (
                <td key={t} className="tier">
                  <span className="cell">
                    <ToggleIn path={`tiers.${t}.autoThrottle`} label={`auto-throttle ${t}`} />
                  </span>
                </td>
              ))}
              <td className="fill why">Off keeps a tier out of the error and spam budgets.</td>
            </tr>
          )}
        </tbody>
      </table>
    </div>
  )
}

function SpamTable() {
  const d = useDraft()
  const defs = usePolicyDefaults().data
  const cases = useOpenCases().data ?? []
  const sig = useSignals().data
  const sigs = SIGNALS.filter((s) => getIn(d.body, `spam.${s.k}`) !== undefined)
  const worst = (s: Signal): Case | undefined =>
    cases.filter((c) => s.kinds.includes(c.kind) && c.threshold > 0).sort((a, b) => b.observed / b.threshold - a.observed / a.threshold)[0]
  return (
    <div className="cx-tw">
      <table className="cx-t compact">
        <thead>
          <tr>
            <th>Signal</th>
            <th>Per</th>
            <th className="r">Threshold</th>
            <th className="r">Window s</th>
            <th>Action</th>
            <th>Default</th>
            <th title="The heaviest key this signal tracks now, against the threshold in force">Heaviest now</th>
            <th title="Open cases of this kind, and the worst one against its threshold">Open cases</th>
          </tr>
        </thead>
        <tbody>
          {sigs.map((s) => {
            const p = `spam.${s.k}`
            const def = defs ? (getIn(defs, p) as Json | undefined) : undefined
            const w = worst(s)
            const open = cases.filter((c) => s.kinds.includes(c.kind)).length
            const off = num(getIn(d.body, `${p}.limit`)) === 0
            const live = sig?.signals.find((x) => s.kinds.includes(x.rule))
            const top = live?.top[0]
            return (
              <tr key={s.k} className={off ? 'dim' : undefined}>
                <td>
                  {s.label}
                  {off && <span className="muted sm"> off</span>}
                </td>
                <td className="sm t2">{s.per}</td>
                <td className="r">
                  <NumIn path={`${p}.limit`} w={80} label={`${s.label} per ${s.per} threshold`} />
                </td>
                <td className="r">
                  <NumIn path={`${p}.windowSecs`} w={74} label={`${s.label} per ${s.per} window`} />
                </td>
                <td>
                  <SelectIn path={`${p}.action`} label={`${s.label} per ${s.per} action`} options={ACTIONS} />
                </td>
                <td className="cx-kd">{def ? `${fmtNum(num(def.limit), num(def.limit) % 1 ? 2 : 0)} / ${def.windowSecs} s · ${def.action}` : '—'}</td>
                <td className="sm">
                  {top && live ? (
                    <button type="button" className="cx-linklike" onClick={() => openSignalKey(live.per, top)} title={`${top.key}: ~${fmtNum(top.estimate)} (at least ${fmtNum(top.lower)}) in ${live.windowSecs} s, on ${sig?.node}`}>
                      <Use v={top.estimate} max={live.limit} label={live.limit > 0 ? `${fmtNum(top.estimate / live.limit, 2)}×` : fmtNum(top.estimate)} />
                    </button>
                  ) : (
                    <span className="muted">{sig ? 'nothing counted' : '—'}</span>
                  )}
                </td>
                <td className="sm">
                  {w ? (
                    <button type="button" className="cx-linklike" onClick={() => openPanel('case', String(w.id))} title={`case ${w.id} on ${w.host}`}>
                      <Use v={w.observed} max={w.threshold} label={`${open} open · ${fmtNum(w.observed / w.threshold, 1)}×`} />
                    </button>
                  ) : (
                    <span className="muted">none</span>
                  )}
                </td>
              </tr>
            )
          })}
        </tbody>
      </table>
    </div>
  )
}

function ConsumerKnobs() {
  const cs = useConsumers().data
  const max = (m: Map<string, number>) => Math.max(0, ...m.values())
  const count = (key: (c: NonNullable<typeof cs>[number]) => string) => {
    const m = new Map<string, number>()
    for (const c of cs ?? []) m.set(key(c), (m.get(key(c)) ?? 0) + 1)
    return m
  }
  const d = useDraft()
  const lim = (k: string) => num(getIn(d.body, `consumers.${k}`))
  const perNode = max(count((c) => c.node))
  const live = (cs ?? []).filter((c) => !c.backfilling)
  const slow = Math.max(0, ...live.map((c) => c.lagMs / 1000))
  const back = Math.max(0, ...(cs ?? []).filter((c) => c.backfilling).map((c) => c.lagMs / 1000))
  const u = (v: number, k: string, label: string) => (cs ? <Use v={v} max={lim(k)} label={label} /> : undefined)
  return (
    <>
      <Knob path="consumers.consumersPerNode" label="Consumers per node" why="Sockets one node serves before refusing new ones." use={u(perNode, 'consumersPerNode', `${perNode} on the busiest`)} />
      <Knob path="consumers.slowConsumerLagSecs" label="Slow consumer cutoff" why="A consumer this far behind live is disconnected and can resume." unit="s" use={u(slow, 'slowConsumerLagSecs', `${fmtNum(slow, 1)} s worst now`)} />
      <Knob path="consumers.maxBackfillSecs" label="Max backfill" why="Older cursors get OutdatedCursor. Can’t exceed what the log keeps." unit="s" use={u(back, 'maxBackfillSecs', back ? `${fmtNum(back)} s oldest replay` : 'no replays now')} />
    </>
  )
}

function ErrorBudget() {
  const d = useDraft()
  const ratio = num(getIn(d.body, 'transitions.errorRatio'))
  const l = useHostList({ sort: 'errors', desc: true, limit: 200 }, { poll: 15_000 })
  const over = (l.data?.hosts ?? []).filter((h) => (h.status === 'connected' || h.status === 'throttled' || h.status === 'backpressure') && h.errorRate > ratio).length
  return (
    <Knob
      path="transitions.errorRatio"
      label="Error budget"
      why="Rejected over all frames in one sweep before a host is throttled (where its tier allows)."
      ratio
      use={l.data && Number.isFinite(ratio) ? <span className={`mono${over ? ' s-warn' : ''}`}>{plural(over, 'host')} over it now</span> : undefined}
    />
  )
}

/** The budgets against what the answering node sees them spend (policy/usage). */
function BudgetKnobs({ u }: { u?: PolicyUsage }) {
  const d = useDraft()
  const { view } = useRelay()
  const lim = (k: string) => num(getIn(d.body, `cluster.${k}`))
  const leader = view?.quorum?.leader
  const counted = !u || !leader || u.node === leader
  return (
    <>
      <Knob
        path="cluster.plcLookupsPerSec"
        label="PLC lookups"
        why="DID document fetches per second across the cluster. The directory rate-limits; stay well under it."
        unit="/s"
        use={u && <Use v={u.plcLookupsPerSec} max={u.plcLookupsShare || lim('plcLookupsPerSec')} label={`${fmtNum(u.plcLookupsPerSec, 1)}/s on ${u.node}${u.plcLookupsShare ? ` of its ${fmtNum(u.plcLookupsShare, 1)}/s share` : ''}${u.seededPerSec ? ` · ${fmtNum(u.seededPerSec, 1)}/s seeded` : ''}`} />}
      />
      <Knob
        path="cluster.newAccountsPerMin"
        label="New accounts"
        why="Accounts first seen per minute across every host. A spam wave hits this before consumers see it."
        unit="/min"
        use={u && (counted ? <Use v={u.newAccountsPerMin} max={lim('newAccountsPerMin')} label={`${fmtNum(u.newAccountsPerMin, 1)}/min now`} /> : <span className="muted">counted on the leader ({leader}), not {u.node}</span>)}
      />
      <Knob path="cluster.newHostsPerDay" label="New hosts" why="requestCrawl admissions per UTC day. Allow rules and trusted domains don’t spend it." unit="/day" use={u && <Use v={u.newHostsToday} max={lim('newHostsPerDay')} label={`${fmtNum(u.newHostsToday)} today`} />} />
    </>
  )
}

// ---------------------------------------------------------------- discovery (Discovery shows it too)

export type SeedRelay = { url: string; enabled: boolean; refreshIntervalSecs: number }

/** Why a seed relay's URL won't do, or undefined. */
export function seedUrlError(url: string): string | undefined {
  try {
    const u = new URL(url)
    if (u.protocol !== 'https:' && u.protocol !== 'http:') return `${url} isn't http(s)`
    if (u.pathname !== '/' || u.search || u.hash) return `${url}: give the relay's origin, no path`
    return undefined
  } catch {
    return `${JSON.stringify(url)} isn't a URL`
  }
}

/** A seed relay URL as the relay keys it: the origin, no trailing slash. */
export const normSeedUrl = (url: string) => {
  const t = url.trim()
  const v = /^[a-z]+:\/\//i.test(t) ? t : `https://${t}`
  try {
    return new URL(v).origin
  } catch {
    return v
  }
}

const seedsOf = (body?: Json) => (getIn(body, 'discovery.seedRelays') as SeedRelay[] | undefined) ?? []

/** Adds a seed relay to the draft (enabled, read every 6 h). False if it's already there. */
export function addSeedRelay(url: string): boolean {
  const d = getDraft()
  const u = normSeedUrl(url)
  const seeds = seedsOf(d.body)
  if (!d.body || seeds.some((r) => r.url === u)) return false
  setField('discovery.seedRelays', [...seeds, { url: u, enabled: true, refreshIntervalSecs: 6 * 3600 }])
  return true
}

function setSeed(i: number, r: SeedRelay | null) {
  const seeds = [...seedsOf(getDraft().body)]
  if (r) seeds[i] = r
  else seeds.splice(i, 1)
  setField('discovery.seedRelays', seeds)
}

/** The seed relays, the PLC source and discovery's own budgets, all in the policy draft. */
export function DiscoveryPolicy() {
  const d = useDraft()
  const errs = useErrors()
  const [url, setUrl] = useState('')
  if (getIn(d.body, 'discovery') === undefined) return <Empty>This relay's policy has no discovery section.</Empty>
  const seeds = seedsOf(d.body)
  const base = seedsOf(d.base?.body)
  const nu = normSeedUrl(url)
  const bad = url.trim() ? (seedUrlError(nu) ?? (seeds.some((r) => r.url === nu) ? `${nu} is already listed` : undefined)) : undefined
  const err = errs.get('discovery.seedRelays')
  return (
    <>
      <div className="cx-tw">
        <table className="cx-t compact cx-seeds">
          <thead>
            <tr>
              <th>Seed relay</th>
              <th>On</th>
              <th>Read every</th>
              <th />
              <th className="fill" />
            </tr>
          </thead>
          <tbody>
            {seeds.map((r, i) => {
              const was = base.find((b) => b.url === r.url)
              return (
                <tr key={r.url} className={r.enabled ? undefined : 'dim'}>
                  <td className="mono sm">
                    {r.url}
                    {!was && <span className="s-sig sm"> new</span>}
                  </td>
                  <td>
                    <button
                      type="button"
                      className={`cx-toggle${r.enabled ? ' on' : ''}${was && was.enabled !== r.enabled ? ' dirty' : ''}`}
                      aria-pressed={r.enabled}
                      aria-label={`Read ${r.url}`}
                      onClick={() => setSeed(i, { ...r, enabled: !r.enabled })}
                    />
                  </td>
                  <td className="nowrap">
                    <input
                      className={`cx-inp num${was && was.refreshIntervalSecs !== r.refreshIntervalSecs ? ' dirty' : ''}`}
                      style={{ width: 64 }}
                      inputMode="decimal"
                      aria-label={`Hours between reads of ${r.url}`}
                      value={String(Math.round((r.refreshIntervalSecs / 3600) * 100) / 100)}
                      onChange={(e) => {
                        const h = Number(e.target.value)
                        if (Number.isFinite(h) && h > 0) setSeed(i, { ...r, refreshIntervalSecs: Math.round(h * 3600) })
                      }}
                    />{' '}
                    <span className="muted sm">h</span>
                  </td>
                  <td>
                    <button type="button" className="cx-btn sm quiet" onClick={() => setSeed(i, null)} aria-label={`Remove ${r.url}`}>
                      Remove
                    </button>
                  </td>
                  <td className="fill" />
                </tr>
              )
            })}
            {base
              .filter((b) => !seeds.some((r) => r.url === b.url))
              .map((b) => (
                <tr key={`rm-${b.url}`} className="dim">
                  <td className="mono sm">
                    <s>{b.url}</s> <span className="s-err sm">removed</span>
                  </td>
                  <td colSpan={2} />
                  <td>
                    <button type="button" className="cx-btn sm quiet" onClick={() => setField('discovery.seedRelays', [...seeds, b])}>
                      Keep
                    </button>
                  </td>
                  <td className="fill" />
                </tr>
              ))}
            {!seeds.length && !base.length && (
              <tr>
                <td colSpan={5}>
                  <Empty>No seed relays: only requestCrawl and the hosts given at start find new PDSes.</Empty>
                </td>
              </tr>
            )}
          </tbody>
        </table>
      </div>
      <form
        className="cx-pn-b cx-form-row cx-addseed"
        onSubmit={(e) => {
          e.preventDefault()
          if (url.trim() && !bad && addSeedRelay(url)) setUrl('')
        }}
      >
        <input className={`cx-inp mono${bad ? ' bad' : ''}`} placeholder="https://relay.example.com" aria-label="Seed relay URL" spellCheck={false} autoComplete="off" value={url} onChange={(e) => setUrl(e.target.value)} title={bad} />
        <button className="cx-btn" disabled={!url.trim() || !!bad}>
          Add seed relay
        </button>
        {(bad || err) && <span className="s-err sm">{bad ?? err}</span>}
      </form>
      <div className="cx-knobs">
        <Knob path="discovery.plc" label="PDS hosts from the PLC export" why="Admit the PDS endpoints the documents the export reader reads name (needs --plc-export)." />
        <Knob path="discovery.aliases" label="Find host aliases" why="Read a PDS listed under several hostnames under one of them: a name that streams another's events at the same seqs, with the same describeServer DID, gets no socket of its own." />
        <Knob path="discovery.connectsPerMin" label="Discovery connects" why="New hosts discovery may connect a minute, cluster-wide. Its own budget: requestCrawl keeps the daily one." unit="/min" />
        <Knob path="discovery.requestsPerSec" label="listHosts requests" why="Pages a second to any one seed relay; a 429 or 5xx waits out its Retry-After on top." unit="/s" />
        <Knob path="discovery.seedAccounts.enabled" label="Seed account counts" why="A new or default host's limits start from the accountCount a seed relay lists it with (active or idle only), not from the accounts this relay has seen." />
        <Knob path="discovery.seedAccounts.headroom" label="Seed headroom" why="Limits are indigo's formula for this many times the listed count, so a busy day fits the per-day window." unit="×" />
        <Knob path="discovery.seedAccounts.max" label="Seed cap" why="The most accounts a seed adds to a host's limits, after the headroom." />
        <Knob path="discovery.seedAccounts.ttlSecs" label="Seed lifetime" why="A count no seed relay has listed again in this long stops counting." unit="s" />
      </div>
    </>
  )
}

/** The older relay's form: tier limits, the default tier and five spam thresholds (PUT policy). */
function WireForm() {
  const d = useDraft()
  return (
    <Panel title="Spam thresholds" src={<Src>policy · spam</Src>}>
      <Knob path="defaultTier" label="Default tier" why="The tier newly crawled hosts start in." opts={tiersOf(d.body)} />
      <Knob path="spam.newAccountsPerHour" label="New accounts per hour" why="Per host. Crossing it opens a case." />
      <Knob path="spam.rejectRatio" label="Reject ratio" why="Rejected over all frames, 5 min, per host." ratio />
      <Knob path="spam.badSignaturesPerMin" label="Bad signatures per minute" why="Per host." />
      <Knob path="spam.accountEventsPerSec" label="Events/s from one account" why="Per account." />
      <Knob path="spam.autoThrottle" label="Auto-throttle" why="High and critical cases also throttle the host." />
    </Panel>
  )
}

function History() {
  const a = usePolicyAudit()
  const d = useDraft()
  const cols: Col<PolicyAudit>[] = [
    {
      id: 'v',
      label: 'Version',
      render: (x) => (
        <span className="mono">
          v{x.version} {x.version === d.base?.version && <Chip k="acc" glyph={false}>current</Chip>}
        </span>
      ),
    },
    { id: 'by', label: 'By', render: (x) => <span className="sm">{x.by}</span> },
    { id: 'note', label: 'Note', fill: true, className: 'wrap sm', render: (x) => x.note || <span className="muted">—</span> },
    { id: 'ch', label: 'Changed', render: (x) => <span className="mono sm t2 trunc" style={{ display: 'inline-block', maxWidth: 380 }}>{x.changes.join(' · ') || '—'}</span> },
    { id: 'at', label: 'When', r: true, render: (x) => <span className="sm muted" title={dt(x.atMs)}>{ago(x.atMs)}</span> },
  ]
  return (
    <Loaded load={a}>
      {(rows) => <DataTable rows={rows} cols={cols} rowKey={(x) => String(x.version)} open={(x) => ({ type: 'ver', id: String(x.version) })} compact label="Policy versions" empty={<Empty title="Never saved">The relay runs on the defaults.</Empty>} />}
    </Loaded>
  )
}

// ---------------------------------------------------------------- review, conflicts, JSON

/** The seed relay list as one line per relay added, removed or changed (the document holds it as one array). */
function seedDiff(from: unknown, to: unknown): ReactNode[] {
  const a = Array.isArray(from) ? (from as SeedRelay[]) : []
  const b = Array.isArray(to) ? (to as SeedRelay[]) : []
  const say = (r: SeedRelay) => `${r.enabled ? 'on' : 'off'}, every ${dur(r.refreshIntervalSecs * 1000)}`
  const out: ReactNode[] = []
  for (const r of b) {
    const was = a.find((x) => x.url === r.url)
    if (!was) out.push(<span className="n">+ {r.url} ({say(r)})</span>)
    else if (!same(was, r))
      out.push(
        <>
          {r.url}: <span className="o">{say(was)}</span> → <span className="n">{say(r)}</span>
        </>,
      )
  }
  for (const r of a) if (!b.some((x) => x.url === r.url)) out.push(<span className="o">− {r.url}</span>)
  return out
}

function DiffList({ list, clash }: { list: Change[]; clash?: string[] }) {
  return (
    <div className="cx-diff" role="list">
      {list.map((c) => (
        <div key={c.path} role="listitem">
          {c.path === 'discovery.seedRelays' ? (
            <>
              {c.path}:
              {seedDiff(c.from, c.to).map((x, i) => (
                <div key={i} style={{ paddingLeft: 12 }}>
                  {x}
                </div>
              ))}
            </>
          ) : (
            <>
              {c.path}: <span className="o">{showVal(c.from)}</span> → <span className="n">{showVal(c.to)}</span>
            </>
          )}
          {clash?.includes(c.path) && <span className="w"> (also changed by the newer version; yours wins)</span>}
        </div>
      ))}
    </div>
  )
}

function ReviewDialog({ close }: { close: () => void }) {
  const d = useDraft()
  const [note, setNote] = useState('')
  const [busy, setBusy] = useState(false)
  const [error, setError] = useState<unknown>()
  const [conflict, setConflict] = useState<{ latest: PolicyBase; message: string }>()
  const [clash, setClash] = useState<string[]>()
  const list = changesOf(d)
  const base = d.base!
  const go = async () => {
    setBusy(true)
    setError(undefined)
    try {
      const doc = await W.savePolicy(base, d.body!, note.trim())
      saved(doc)
      close()
      toast(`Saved version ${doc.version}. Every node picks it up within 10 s.`)
    } catch (e) {
      if (e instanceof ApiError && e.status === 409) {
        try {
          setConflict({ latest: await readPolicySource(), message: e.message })
        } catch (e2) {
          setError(e2)
        }
      } else setError(e)
    } finally {
      setBusy(false)
    }
  }
  const latestAudit = conflict && cached<PolicyAudit[]>(keys.policyAudit())?.find((a) => a.version === conflict.latest.version)
  return (
    <FormDialog
      title={`Save policy version ${base.version + 1}?`}
      icon="v"
      action={conflict ? 'Save' : `Save version ${base.version + 1}`}
      busy={busy}
      disabled={!list.length || !!conflict}
      error={error}
      call={A.savePolicyCall(base.mode === 'full', base.version, note.trim())}
      onSubmit={go}
      onCancel={close}
    >
      {conflict ? (
        <div className="cx-conflict" role="alert">
          <span>
            <b>409 VersionConflict.</b> {conflict.message}. Nothing was changed.
          </span>
          <span className="sm t2">
            Version {conflict.latest.version} by {conflict.latest.updatedBy} {ago(conflict.latest.updatedAtMs)}
            {latestAudit?.note ? ` (“${latestAudit.note}”)` : ''}
            {latestAudit?.changes.length ? `: ${latestAudit.changes.slice(0, 3).join(' · ')}${latestAudit.changes.length > 3 ? ' …' : ''}` : ''}
          </span>
          <button
            type="button"
            className="cx-btn sm primary"
            onClick={() => {
              setClash(rebase(conflict.latest))
              setConflict(undefined)
            }}
          >
            Move my changes onto version {conflict.latest.version}
          </button>
        </div>
      ) : null}
      <DiffList list={list} clash={clash} />
      <ul>
        <li>Every node reloads it within 10 s; hosts pick up new limits on their next check.</li>
        <li>It's saved against version {base.version}. If someone saved first you get a 409 here and nothing changes.</li>
        {base.mode === 'wire' && <li>This relay only has the tier form (PUT policy), so other settings aren't here.</li>}
      </ul>
      <div>
        <label className="cx-lbl" htmlFor="pol-note">
          Note (kept in the audit log)
        </label>
        <input id="pol-note" className="cx-inp" autoComplete="off" placeholder="Why this change" value={note} onChange={(e) => setNote(e.target.value)} autoFocus />
      </div>
    </FormDialog>
  )
}

export const reviewDraft = () => {
  const s = getDraft()
  if (!s.base || !changesOf(s).length) return
  openDialog((close) => <ReviewDialog close={close} />)
}

function JsonDialog({ close }: { close: () => void }) {
  const d = useDraft()
  const [text, setText] = useState(() => JSON.stringify(d.body, null, 2))
  const parsed = useMemo((): { ok: true; v: Json } | { ok: false; err: string } => {
    try {
      const v = JSON.parse(text)
      return typeof v === 'object' && v && !Array.isArray(v) ? { ok: true, v } : { ok: false, err: 'The policy document is a JSON object' }
    } catch (e) {
      return { ok: false, err: errText(e) }
    }
  }, [text])
  const n = changesOf(d).length
  return (
    <FormDialog
      title={`Policy version ${d.base?.version}${n ? ' with your draft' : ''}`}
      icon="{}"
      action="Apply to the draft"
      disabled={!parsed.ok}
      error={parsed.ok ? undefined : new Error(`Not valid: ${parsed.err}`)}
      call={`GET /admin/api/${d.base?.mode === 'wire' ? 'policy' : 'policy/full'} · nothing is sent until you review and save`}
      onSubmit={() => {
        if (!parsed.ok) return
        setBody(parsed.v)
        close()
        toast('Applied to the draft')
      }}
      onCancel={close}
    >
      <p className="cx-lede">The engine's whole document, including what the form doesn't show. Edits here join the draft; the relay validates the whole document on save.</p>
      <textarea className="cx-inp cx-jsonedit" spellCheck={false} aria-label="Policy document (JSON)" value={text} onChange={(e) => setText(e.target.value)} />
    </FormDialog>
  )
}

// ---------------------------------------------------------------- the page

export function DraftBar() {
  const d = useDraft()
  const errs = useErrors()
  const list = changesOf(d)
  if (!list.length || !d.base) return null
  return (
    <div className="cx-draftbar" role="region" aria-label="Unsaved policy draft">
      <span>
        <b>{plural(list.length, 'change')}</b> from version {d.base.version}
      </span>
      <span className="muted sm mono trunc paths">
        {list
          .slice(0, 3)
          .map((x) => x.path)
          .join(' · ')}
        {list.length > 3 ? ' …' : ''}
      </span>
      {errs.size > 0 && (
        <span className="s-err sm" title={[...new Set(errs.values())].join('\n')}>
          ■ {errs.size} invalid: {[...errs.values()][0]}
        </span>
      )}
      <span className="r">
        <button type="button" className="cx-btn sm" onClick={discard}>
          Discard
        </button>
        <button type="button" className="cx-btn sm primary" disabled={errs.size > 0} onClick={reviewDraft}>
          Review &amp; save…
        </button>
      </span>
    </div>
  )
}

export function Policy() {
  const src = usePolicySource()
  const d = useDraft()
  const full = d.base?.mode === 'full'
  const banners: BannerSpec[] = []
  if (d.newer)
    banners.push({
      id: 'newer',
      tone: 'warn',
      title: `Version ${d.newer.version} was saved ${ago(d.newer.updatedAtMs)} by ${d.newer.updatedBy}`,
      desc: `Your draft is against version ${d.base?.version}, so saving it now gets a 409.`,
      right: (
        <button
          type="button"
          className="cx-btn sm"
          onClick={() => {
            const c = rebase(d.newer!)
            toast(c.length ? `Moved onto version ${d.newer!.version}; you both changed ${c.join(', ')}` : `Moved your draft onto version ${d.newer!.version}`)
          }}
        >
          Move my draft onto v{d.newer.version}
        </button>
      ),
    })
  if (src.error && d.base) banners.push({ id: 'err', tone: 'err', title: "Couldn't refresh the policy", desc: errText(src.error) })
  return (
    <>
      <PageHead
        title="Policy"
        sub={
          d.base ? (
            <>
              <span>
                version <b className="mono">{d.base.version}</b>
              </span>
              {d.base.version > 0 ? (
                <span title={dt(d.base.updatedAtMs)}>
                  saved by {d.base.updatedBy} {ago(d.base.updatedAtMs)}
                  {d.base.note ? ` (“${d.base.note}”)` : ''}
                </span>
              ) : (
                <span>the defaults: never saved</span>
              )}
              <span>every node reloads within 10 s</span>
              <Updated l={src} />
            </>
          ) : (
            <span>…</span>
          )
        }
        actions={
          <>
            <Link className="cx-btn" to="/admin/moderation#rules">
              Domain rules
            </Link>
            <button type="button" className="cx-btn" disabled={!d.base} onClick={() => openDialog((close) => <JsonDialog close={close} />)}>
              View JSON…
            </button>
          </>
        }
      />
      <Banners items={banners} />
      <Loaded load={{ data: d.base, error: src.error, loading: src.loading, reload: src.reload }}>
        {() => (
          <div className="cx-stack">
            <Panel title="Host tiers" src={<Src>{full ? 'policy/full · tiers' : 'policy · tiers'}</Src>} right={<span className="muted sm">edits stay a draft until you save</span>}>
              <TierMatrix />
            </Panel>
            {full ? (
              <>
                <div className="cx-grid2">
                  <Panel title="Tier transitions" src={<Src>policy/full · transitions</Src>}>
                    <Knob path="transitions.promoteAfterDays" label="Promote new hosts after" why="A new host moves to default once this old with no trip for as long." unit="days" />
                    <Knob path="transitions.recoverAfterSecs" label="Auto-throttle recovers after" why="An auto-throttled host goes back to its tier after this long without a trip." unit="s" />
                    <ErrorBudget />
                    <Knob path="transitions.errorMinEvents" label="Error budget floor" why="Fewer frames than this never trip the budget, so one bad commit from a tiny PDS doesn’t throttle it." unit="frames" />
                  </Panel>
                  <Budgets />
                </div>
                {getIn(d.body, 'discovery') !== undefined && (
                  <Panel title="Host discovery" to="/admin/discovery" src={<Src>policy/full · discovery</Src>} right={<Link className="sm" to="/admin/discovery">sources and runs →</Link>}>
                    <DiscoveryPolicy />
                  </Panel>
                )}
                <div className="cx-grid2">
                  <Panel title="Crawl admission" src={<Src>policy/full · crawl</Src>}>
                    <Knob path="crawl.enabled" label="Public requestCrawl" why="Off: only operators add hosts." />
                    <Knob path="crawl.allowlistOnly" label="Allow-list only" why="Only hosts an allow rule or trusted domain covers get in. For incidents." />
                    <Knob path="crawl.allowInsecure" label="Allow insecure hosts" why="Plain ws:// and IP hosts. Leave off outside dev networks." />
                    <Knob path="crawl.initialTier" label="Initial tier" why="The tier every other admitted host starts in." opts={['trusted', 'default', 'new']} />
                    <Knob path="crawl.trustedDomains" label="Trusted domains" why="Hosts matching these start trusted. One pattern per line." />
                  </Panel>
                  <Panel title="Consumer limits" src={<><Src>policy/full · consumers</Src> <Src>consumers</Src></>} right={<Chip k="warn" title="docs/policy.md › Gaps: the firehose uses its own defaults for these">not enforced yet</Chip>}>
                    <ConsumerKnobs />
                  </Panel>
                </div>
                <Panel
                  title="Spam signals"
                  src={<><Src>policy/full · spam</Src> <Src>cases?status=open</Src> <Src>policy/signals</Src></>}
                  right={<span className="muted sm">checked against count − error, so churn can’t trip a false positive</span>}
                  foot={<span>A threshold of 0 turns a signal off. A per-account signal that throttles throttles the account's host.</span>}
                >
                  <SpamTable />
                  <div className="cx-knobs">
                    <Knob path="spam.trackHosts" label="Hosts tracked per signal" why="Only the heaviest keys are tracked, so memory stays fixed however many hosts are noisy." />
                    <Knob path="spam.trackAccounts" label="Accounts tracked per signal" why="The same for DIDs." />
                  </div>
                </Panel>
              </>
            ) : (
              <WireForm />
            )}
            <Panel title="Version history" src={<Src>policy/audit</Src>} right={<span className="muted sm">open a version to see its diff or undo it</span>}>
              <History />
            </Panel>
          </div>
        )}
      </Loaded>
      <DraftBar />
    </>
  )
}

function Budgets() {
  const use = usePolicyUsage()
  const u = use.data
  return (
    <Panel
      title="Cluster budgets"
      src={<><Src>policy/full · cluster</Src> <Src>policy/usage</Src></>}
      right={u ? <span className="muted sm">use over the last {fmtNum(u.windowSecs, 0)} s</span> : undefined}
      foot={<span>Each budget is shared by every node; per-second ones split over the live cores. The rates are the answering node's.</span>}
    >
      {use.error && !u ? <ErrorState error={use.error} retry={use.reload} /> : <BudgetKnobs u={u} />}
    </Panel>
  )
}

// ---------------------------------------------------------------- a version: its diff and undo

function undoDialog(a: PolicyAudit) {
  const cur = cached<PolicyBase>(keys.policySource())
  if (!cur) return
  const undo = undoOf(a.changes, cur.body)
  const ok = undo.filter((u) => u.ok)
  const note = `undo v${a.version}`
  confirmAction({
    tone: 'warn',
    primary: true,
    title: `Undo version ${a.version}?`,
    items: [
      `Saves version ${cur.version + 1} with ${ok.map((u) => `${u.path} back to ${showVal(u.to)}`).join(', ')}.`,
      ...undo.filter((u) => u.ok && u.why).map((u) => `${u.path} was ${u.why}: it's ${showVal(u.from)} now and goes back to ${showVal(u.to)}.`),
      ...undo.filter((u) => !u.ok).map((u) => `${u.path} stays: ${u.why}.`),
      'Later changes to other fields stay.',
    ],
    word: `v${a.version}`,
    action: 'Save undo',
    call: A.savePolicyCall(cur.mode === 'full', cur.version, note),
    run: async () => {
      const latest = await readPolicySource()
      const doc = await W.savePolicy(latest, applyUndo(latest.body, undoOf(a.changes, latest.body)), note)
      const s = getDraft()
      if (!changesOf(s).length) saved(doc)
      return doc
    },
    done: (r) => `Saved version ${(r as PolicyBase).version}`,
  })
}

registerDetail('ver', {
  kind: 'Policy version',
  section: 'policy',
  use: (id) => {
    const a = usePolicyAudit()
    const d = useDraft()
    const v = Number(id)
    const x = a.data?.find((r) => r.version === v)
    if (!x) return { title: `Version ${id}`, body: null, loading: a.loading, missing: a.data ? `Version ${id} isn't in the audit log.` : undefined }
    const undo = d.base ? undoOf(x.changes, d.base.body) : []
    const changes = x.changes.map((c) => {
      const at = c.indexOf(': ')
      const arrow = c.lastIndexOf(' → ')
      return at > 0 && arrow > at ? { path: c.slice(0, at), from: c.slice(at + 2, arrow), to: c.slice(arrow + 3) } : { path: c, from: '', to: '' }
    })
    return {
      title: `Version ${x.version}`,
      chip: x.version === d.base?.version ? <Chip k="acc" glyph={false}>current</Chip> : <Chip k="plain">{ago(x.atMs)}</Chip>,
      foot: <>GET /admin/api/policy/audit</>,
      body: (
        <>
          <KV
            rows={[
              ['By', x.by],
              ['When', dt(x.atMs)],
              ['Note', x.note || <span className="muted">none</span>],
            ]}
          />
          {changes.length ? (
            <div className="cx-diff" role="list">
              {changes.map((c) => (
                <div key={c.path} role="listitem">
                  {c.path}: <span className="o">{c.from}</span> → <span className="n">{c.to}</span>
                </div>
              ))}
            </div>
          ) : (
            <Empty>No field changed.</Empty>
          )}
          <div className="cx-form-row">
            <button type="button" className="cx-btn" disabled={!undo.some((u) => u.ok)} title={undo.some((u) => u.ok) ? undefined : 'Nothing here can be put back from the log'} onClick={() => undoDialog(x)}>
              Undo this change…
            </button>
            <span className="muted sm">Saves a new version with these fields put back.</span>
          </div>
        </>
      ),
    }
  },
})

registerPalette({
  items: () => {
    const s = getDraft()
    const n = changesOf(s).length
    const out = n
      ? [
          { group: 'Actions', glyph: '◆', title: `Review the policy draft (${plural(n, 'change')})`, run: reviewDraft },
          { group: 'Actions', glyph: '○', title: 'Discard the policy draft', run: discard },
        ]
      : []
    const vers = (cached<PolicyAudit[]>(keys.policyAudit()) ?? []).slice(0, 12).map((a) => ({ group: 'Policy', glyph: 'v', title: `Policy v${a.version}`, desc: a.note || a.changes.slice(0, 2).join(' · '), run: () => openPanel('ver', String(a.version)) }))
    return [...out, ...vers]
  },
})
