import { StrictMode, useEffect } from 'react'
import { createRoot } from 'react-dom/client'
import './styles.css'
import './relay.css'
import { navigate, usePath } from './lib/router'
import { AdminApp } from './pages/AdminApp'
import { Public } from './pages/Public'

function App() {
  const path = usePath()
  const known = path === '/' || path.startsWith('/admin')
  useEffect(() => {
    if (!known) navigate('/', { replace: true })
  }, [known])
  if (path.startsWith('/admin')) return <AdminApp path={path} />
  return <Public />
}

createRoot(document.getElementById('root')!).render(
  <StrictMode>
    <App />
  </StrictMode>,
)
