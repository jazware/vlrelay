import { defineConfig } from 'vite'
import react from '@vitejs/plugin-react'
import { vlrelayDocs } from './docs-build/plugin.mjs'

// Node's environment, without pulling in @types/node for one variable.
declare const process: { env: Record<string, string | undefined> }

// `npm run dev` proxies the API to a running relay or `admin_demo` (default :2790).
const target = process.env.VLRELAY_URL ?? 'http://127.0.0.1:2790'

export default defineConfig({
  plugins: [react(), vlrelayDocs()],
  base: '/',
  build: {
    outDir: 'dist',
    emptyOutDir: true,
    // no inline polyfill script: the page CSP is script-src 'self'
    modulePreload: { polyfill: false },
    assetsInlineLimit: 0,
  },
  server: {
    port: 5790,
    proxy: {
      '/admin/api': { target, changeOrigin: false },
      '/api/public': { target, changeOrigin: false },
      // the console's tail (subscribeRepos) and requestCrawl; admin_demo serves neither
      '/xrpc': { target, changeOrigin: false, ws: true },
    },
  },
})
