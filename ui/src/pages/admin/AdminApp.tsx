import { useEffect, useState, type JSX, type ReactNode } from 'react'
import { DetailPage, detailKind } from '../../components/console/Drawer'
import { Empty, Glyph } from '../../components/console/kit'
import { Mark, Shell } from '../../components/console/Shell'
import { SECTION, sectionOf, type Section } from '../../components/console/sections'
import { ErrorNotice, Field, Spinner } from '../../components/ui'
import { api, setAdminToken } from '../../lib/api'
import { useAdminToken } from '../../lib/hooks'
import { Link, match } from '../../lib/router'
import { AccountDetail, Accounts } from '../Accounts'
import { CaseDetail, Cases } from '../Cases'
import { Ops } from '../Ops'
import { Policy } from '../Policy'
import { Rules } from '../Rules'
import { Settings } from '../Settings'
import { Tuning } from '../Tuning'
import { Consumers } from './Consumers'
import './hostDetail'
import { Hosts } from './Hosts'
import { Overview } from './Overview'
import { Quorum } from './Quorum'
import { Store } from './Store'

// The operator console: the token gate, then the shell around one page per route. Sections
// still on their pre-console pages render them inside <Legacy> until they're rebuilt
// (CONSOLE.md lists which); their old paths keep working.

type Route = { section: Section; page: JSX.Element; crumbs?: ReactNode; title?: string }

const crumb = (section: Section, last: ReactNode) => (
  <>
    <Link to={section.path}>{section.label}</Link>
    <span className="sep">/</span>
    <b>{last}</b>
  </>
)

type Tab = { to: string; label: string }
const TABS: Partial<Record<Section['id'], Tab[]>> = {
  quorum: [
    { to: '/admin/quorum', label: 'Quorum & cluster' },
    { to: '/admin/ops', label: 'Operations' },
  ],
  policy: [
    { to: '/admin/policy', label: 'Limits' },
    { to: '/admin/tuning', label: 'Tuning' },
    { to: '/admin/rules', label: 'Domain rules' },
  ],
  moderation: [
    { to: '/admin/moderation', label: 'Cases' },
    { to: '/admin/accounts', label: 'Accounts' },
  ],
}

/** An older console page shown inside the new shell, in its own type, until its section is rebuilt. */
function Legacy({ section, path, children }: { section: Section; path: string; children: ReactNode }) {
  const tabs = TABS[section.id]
  return (
    <div className="cx-legacy">
      <div className="cx-legacy-note">
        <Glyph k="idle" />
        <span>The {section.label.toLowerCase()} pages are the classic console's until this section is rebuilt.</span>
      </div>
      {tabs && (
        <nav className="cx-subtabs" aria-label={section.label}>
          {tabs.map((t) => (
            <Link key={t.to} to={t.to} aria-current={path === t.to || (t.to === '/admin/moderation' && path.startsWith('/admin/cases')) || path.startsWith(`${t.to}/`) ? 'page' : undefined}>
              {t.label}
            </Link>
          ))}
        </nav>
      )}
      {children}
    </div>
  )
}

function route(p: string): Route {
  let m: Record<string, string> | null
  const S = SECTION
  const legacy = (section: Section, page: JSX.Element, last?: ReactNode): Route => ({ section, page: <Legacy section={section} path={p}>{page}</Legacy>, crumbs: last ? crumb(section, last) : undefined })
  switch (p) {
    case '/admin':
      return { section: S.overview, page: <Overview /> }
    case '/admin/hosts':
      return { section: S.hosts, page: <Hosts /> }
    case '/admin/consumers':
      return { section: S.consumers, page: <Consumers /> }
    case '/admin/quorum':
    case '/admin/cluster':
      return { section: S.quorum, page: <Quorum /> }
    case '/admin/ops':
      return legacy(S.quorum, <Ops />, 'Operations')
    case '/admin/store':
      return { section: S.store, page: <Store /> }
    case '/admin/policy':
      return legacy(S.policy, <Policy />)
    case '/admin/tuning':
      return legacy(S.policy, <Tuning />, 'Tuning')
    case '/admin/rules':
      return legacy(S.policy, <Rules />, 'Domain rules')
    case '/admin/moderation':
    case '/admin/cases':
      return legacy(S.moderation, <Cases />)
    case '/admin/accounts':
      return legacy(S.moderation, <Accounts />, 'Accounts')
    case '/admin/settings':
      return legacy(S.settings, <Settings />)
  }
  if ((m = match('/admin/cases/:id', p))) return legacy(S.moderation, <CaseDetail id={Number(m.id)} />, `case ${m.id}`)
  if ((m = match('/admin/accounts/:did', p))) return legacy(S.moderation, <AccountDetail did={m.did} />, m.did)
  // the classic console's host page
  if ((m = match('/admin/hosts/:host', p))) return { section: S.hosts, page: <DetailPage key="host" type="host" id={m.host} />, crumbs: crumb(S.hosts, m.host) }
  // a detail kind's full page: /admin/<section>/<type>/<id>
  if ((m = match('/admin/:section/:type/:id', p)) && detailKind(m.type)) {
    const section = sectionOf(p)
    return { section, page: <DetailPage key={m.type} type={m.type} id={m.id} />, crumbs: crumb(section, m.id) }
  }
  return { section: sectionOf(p), page: <Empty title="No such page">There is no console page at {p}.</Empty> }
}

export function AdminApp({ path }: { path: string }) {
  const token = useAdminToken()
  const p = path.replace(/\/+$/, '') || '/admin'
  const r = token ? route(p) : undefined
  useEffect(() => {
    document.title = r ? `${r.section.label} · vlRelay` : 'Console · vlRelay'
  }, [r?.section.label])
  if (!token || !r) return <Login />
  return (
    <Shell section={r.section} crumbs={r.crumbs}>
      {r.page}
    </Shell>
  )
}

function Login() {
  const [token, setToken] = useState('')
  const [busy, setBusy] = useState(false)
  const [error, setError] = useState<unknown>()
  return (
    <div className="cx cx-pubroot">
      <header className="cx-pub-top">
        <Link to="/" className="cx-wordmark" aria-label="vlRelay">
          <Mark />
          vlRelay<span className="where">operator · {location.host}</span>
        </Link>
      </header>
      <main className="cx-signin">
        <form
          className="cx-pn"
          onSubmit={async (e) => {
            e.preventDefault()
            setBusy(true)
            setError(undefined)
            try {
              await api('cluster', { token: token.trim() })
              setAdminToken(token.trim())
            } catch (err) {
              setError(err)
            } finally {
              setBusy(false)
            }
          }}
        >
          <h1>Relay console</h1>
          <p className="t2">Hosts, consumers, the quorum log, policy and moderation for this relay. The token stays in this tab only.</p>
          <ErrorNotice error={error} />
          <Field label="Admin token" hint="The relay's --admin-token (VLRELAY_ADMIN_TOKEN).">
            <input className="cx-inp" type="password" value={token} onChange={(e) => setToken(e.target.value)} autoComplete="off" required autoFocus />
          </Field>
          <div className="cx-form-row" style={{ justifyContent: 'flex-end' }}>
            <button className="cx-btn primary" disabled={busy}>
              {busy && <Spinner />}
              Unlock console
            </button>
          </div>
        </form>
      </main>
    </div>
  )
}
