import { StrictMode, Suspense, lazy, useEffect } from 'react'
import { createRoot } from 'react-dom/client'
import './styles.css'
import './relay.css'
import { navigate, usePath } from './lib/router'
import { AdminApp } from './pages/AdminApp'

// its own chunk: the docs' nav and page index aren't needed by the console
const DocsApp = lazy(() => import('./pages/docs/DocsApp').then((m) => ({ default: m.DocsApp })))

const isDocs = (path: string) => path === '/docs' || path.startsWith('/docs/')

function App() {
  const path = usePath()
  useEffect(() => {
    if (!path.startsWith('/admin') && !isDocs(path)) navigate('/admin', { replace: true })
  }, [path])
  if (isDocs(path))
    return (
      <Suspense fallback={null}>
        <DocsApp path={path} />
      </Suspense>
    )
  return <AdminApp path={path} />
}

createRoot(document.getElementById('root')!).render(
  <StrictMode>
    <App />
  </StrictMode>,
)
