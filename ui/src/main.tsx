import { StrictMode, Suspense, lazy, useEffect } from 'react'
import { createRoot } from 'react-dom/client'
import './styles.css'
import './relay.css'
import './console.css'
import { navigate, usePath } from './lib/router'
import { AdminApp } from './pages/admin/AdminApp'
import { Public } from './pages/Public'

// its own chunk: the docs' nav and page index aren't needed by the console
const DocsApp = lazy(() => import('./pages/docs/DocsApp').then((m) => ({ default: m.DocsApp })))

const isDocs = (path: string) => path === '/docs' || path.startsWith('/docs/')

function App() {
  const path = usePath()
  const known = path === '/' || path.startsWith('/admin') || isDocs(path)
  useEffect(() => {
    if (!known) navigate('/', { replace: true })
  }, [known])
  if (isDocs(path))
    return (
      <Suspense fallback={null}>
        <DocsApp path={path} />
      </Suspense>
    )
  if (path.startsWith('/admin')) return <AdminApp path={path} />
  return <Public />
}

createRoot(document.getElementById('root')!).render(
  <StrictMode>
    <App />
  </StrictMode>,
)
