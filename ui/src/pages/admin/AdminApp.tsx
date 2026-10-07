import { useEffect, useState, type JSX, type ReactNode } from 'react'
import { DetailPage, detailKind } from '../../components/console/Drawer'
import { Empty } from '../../components/console/kit'
import { Mark, Shell } from '../../components/console/Shell'
import { SECTION, sectionOf, type Section } from '../../components/console/sections'
import { ErrorNotice, Field, Spinner } from '../../components/ui'
import { api, ApiError, proxySession, setAdminOperator, setAdminToken } from '../../lib/api'
import { useAdminUnlock } from '../../lib/hooks'
import { Link, match } from '../../lib/router'
import { Consumers } from './Consumers'
import { Discovery } from './Discovery'
import './hostDetail'
import { Hosts } from './Hosts'
import { Moderation } from './Moderation'
import './moderationDetail'
import { Overview } from './Overview'
import { Policy } from './Policy'
import { Quorum } from './Quorum'
import { Settings } from './Settings'
import { Store } from './Store'

// The operator console: the gate (a proxy's sign-in, else the token), then the shell around one page per route. The classic
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
    case '/admin/discovery':
      return { section: S.discovery, page: <Discovery /> }
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
  const unlocked = useAdminUnlock()
  const p = path.replace(/\/+$/, '') || '/admin'
  const r = unlocked ? route(p) : undefined
  useEffect(() => {
    document.title = r ? `${r.section.label} · vlRelay` : 'Console · vlRelay'
  }, [r?.section.label])
  if (!unlocked || !r) return <Login />
  return (
    <Shell section={r.section} crumbs={r.crumbs}>
      {r.page}
    </Shell>
  )
}

// Asked without a token: a proxy in front of the admin listener may already have named the
// operator (docs/admin-api.md "Sign-in through a proxy"). A 403 is a proxy sign-in that was
// refused (not an operator, or a cross-site request): shown above the token form.
function useProxySignIn() {
  const [state, setState] = useState<{ checking: boolean; refused?: unknown }>({ checking: true })
  useEffect(() => {
    let live = true
    proxySession()
      .then((s) => {
        if (s.auth === 'proxy' && s.operator) setAdminOperator(s.operator)
        else if (live) setState({ checking: false })
      })
      .catch((e) => live && setState({ checking: false, refused: e instanceof ApiError && e.status === 403 ? e : undefined }))
    return () => {
      live = false
    }
  }, [])
  return state
}

function Login() {
  const [token, setToken] = useState('')
  const [busy, setBusy] = useState(false)
  const [error, setError] = useState<unknown>()
  const proxy = useProxySignIn()
  return (
    <div className="cx cx-pubroot">
      <header className="cx-pub-top">
        <Link to="/" className="cx-wordmark" aria-label="vlRelay">
          <Mark />
          vlRelay<span className="where">operator · {location.host}</span>
        </Link>
      </header>
      <main className="cx-signin">
        {proxy.checking ? (
          <Spinner />
        ) : (
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
          <ErrorNotice error={error ?? proxy.refused} />
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
        )}
      </main>
    </div>
  )
}
