import { useEffect, useState, type JSX, type ReactNode } from 'react'
import { DetailPage, detailKind } from '../../components/console/Drawer'
import { Empty } from '../../components/console/kit'
import { Mark, Shell } from '../../components/console/Shell'
import { SECTION, sectionOf, type Section } from '../../components/console/sections'
import { ErrorNotice, Field, Spinner } from '../../components/ui'
import { api, setAdminToken } from '../../lib/api'
import { useAdminToken } from '../../lib/hooks'
import { Link, match } from '../../lib/router'
import { Consumers } from './Consumers'
import './hostDetail'
import { Hosts } from './Hosts'
import { Moderation } from './Moderation'
import './moderationDetail'
import { Overview } from './Overview'
import { Policy } from './Policy'
import { Quorum } from './Quorum'
import { Settings } from './Settings'
import { Store } from './Store'

// The operator console: the token gate, then the shell around one page per route. The classic
// console's paths (/admin/cluster, /admin/ops, /admin/tuning, /admin/rules, /admin/cases/<id>,
// /admin/accounts/<did>) still land on the section that took them over.

type Route = { section: Section; page: JSX.Element; crumbs?: ReactNode; title?: string }

const crumb = (section: Section, last: ReactNode) => (
  <>
    <Link to={section.path}>{section.label}</Link>
    <span className="sep">/</span>
    <b>{last}</b>
  </>
)

function route(p: string): Route {
  let m: Record<string, string> | null
  const S = SECTION
  switch (p) {
    case '/admin':
      return { section: S.overview, page: <Overview /> }
    case '/admin/hosts':
      return { section: S.hosts, page: <Hosts /> }
    case '/admin/consumers':
      return { section: S.consumers, page: <Consumers /> }
    case '/admin/quorum':
    case '/admin/cluster':
    case '/admin/ops':
      return { section: S.quorum, page: <Quorum /> }
    case '/admin/store':
      return { section: S.store, page: <Store /> }
    case '/admin/policy':
    case '/admin/tuning':
      return { section: S.policy, page: <Policy /> }
    case '/admin/moderation':
    case '/admin/cases':
    case '/admin/accounts':
    case '/admin/rules':
      return { section: S.moderation, page: <Moderation /> }
    case '/admin/settings':
      return { section: S.settings, page: <Settings /> }
  }
  // the classic console's case and account pages
  if ((m = match('/admin/cases/:id', p))) return { section: S.moderation, page: <DetailPage key="case" type="case" id={m.id} />, crumbs: crumb(S.moderation, `case ${m.id}`) }
  if ((m = match('/admin/accounts/:did', p))) return { section: S.moderation, page: <DetailPage key="acct" type="acct" id={m.did} />, crumbs: crumb(S.moderation, m.did) }
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
