import { StrictMode, useEffect } from 'react'
import { createRoot } from 'react-dom/client'
import './styles.css'
import './relay.css'
import { navigate, usePath } from './lib/router'
import { AdminApp } from './pages/AdminApp'

function App() {
  const path = usePath()
  useEffect(() => {
    if (!path.startsWith('/admin')) navigate('/admin', { replace: true })
  }, [path])
  return <AdminApp path={path} />
}

createRoot(document.getElementById('root')!).render(
  <StrictMode>
    <App />
  </StrictMode>,
)
