// Vite plugin (ported from vlpds): `virtual:vlrelay-docs` is the nav and page
// metadata; each page's HTML is its own chunk (`virtual:vlrelay-docs/page/<slug>`),
// loaded when the page is opened. A docs error fails `vite build`; the dev
// server reloads on any change under docs/ and shows errors in the overlay.

import { loadDocs, DOCS_DIR } from './docs.mjs'

const ID = 'virtual:vlrelay-docs'
const PAGE = `${ID}/page/`

export function vlrelayDocs() {
  let docs = null
  const get = () => (docs ??= loadDocs())
  const fail = (errors) => new Error(`docs: ${errors.length} problem(s):\n  ${errors.join('\n  ')}`)
  return {
    name: 'vlrelay-docs',
    buildStart() {
      docs = null
    },
    resolveId(id) {
      if (id === ID || id.startsWith(PAGE)) return '\0' + id
    },
    load(id) {
      if (!id.startsWith('\0' + ID)) return
      const d = get()
      if (d.errors.length) throw fail(d.errors)
      if (id === '\0' + ID) {
        const meta = d.pages.map(({ html: _html, file: _file, ...m }) => m)
        const loaders = d.pages.map((p) => `${JSON.stringify(p.slug)}: () => import(${JSON.stringify(PAGE + p.slug)})`)
        return `export const nav = ${JSON.stringify(d.nav)};\nexport const pages = ${JSON.stringify(meta)};\nexport const loaders = {${loaders.join(',\n')}};\n`
      }
      const slug = id.slice(('\0' + PAGE).length)
      const p = d.pages.find((x) => x.slug === slug)
      if (!p) throw new Error(`docs: no page ${slug}`)
      return `export default ${JSON.stringify(p.html)};\n`
    },
    configureServer(server) {
      server.watcher.add(DOCS_DIR)
      const reload = (file) => {
        if (!file.startsWith(DOCS_DIR)) return
        docs = null
        for (const m of server.moduleGraph.idToModuleMap.values()) if (m.id?.startsWith('\0' + ID)) server.moduleGraph.invalidateModule(m)
        server.ws.send({ type: 'full-reload' })
      }
      server.watcher.on('change', reload)
      server.watcher.on('add', reload)
      server.watcher.on('unlink', reload)
    },
  }
}
