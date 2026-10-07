// Loads docs/**/*.md at build time: front matter, markdown →
// HTML (markdown-it + highlight.js, nothing shipped to the browser), the
// fenced visuals (hero, diagram, timeline, facts, steps, pages), and
// validation: front matter, a hero first on every page, internal links and
// their anchors.
// Not published: files whose name starts with "_" (the style guide), and the
// internal dev notes listed in docs/_internal.txt (the site is public).

import fs from 'node:fs'
import path from 'node:path'
import { fileURLToPath } from 'node:url'
import MarkdownIt from 'markdown-it'
import hljs from 'highlight.js/lib/core'
import bash from 'highlight.js/lib/languages/bash'
import json from 'highlight.js/lib/languages/json'
import yaml from 'highlight.js/lib/languages/yaml'
import rust from 'highlight.js/lib/languages/rust'
import ini from 'highlight.js/lib/languages/ini'
import plaintext from 'highlight.js/lib/languages/plaintext'
import { load as loadYaml } from 'js-yaml'
import { renderDiagram, esc, checkKeys } from './diagram.mjs'
import { renderTimeline } from './timeline.mjs'

hljs.registerLanguage('bash', bash)
hljs.registerLanguage('json', json)
hljs.registerLanguage('yaml', yaml)
hljs.registerLanguage('rust', rust)
hljs.registerLanguage('toml', ini)
hljs.registerLanguage('ini', ini)
hljs.registerLanguage('text', plaintext)
const LANG_ALIASES = { sh: 'bash', shell: 'bash', console: 'bash', yml: 'yaml', rs: 'rust', txt: 'text', promql: 'text' }

export const DOCS_DIR = path.resolve(path.dirname(fileURLToPath(import.meta.url)), '../../docs')
const STATUSES = new Set(['stub', 'draft', 'ready'])
const VISUAL_FENCES = new Set(['hero', 'diagram', 'timeline', 'facts', 'steps', 'pages'])

export function slugify(s) {
  return (
    s
      .toLowerCase()
      .replace(/<[^>]+>/g, '')
      .replace(/&[a-z]+;/g, '')
      .replace(/[`*_~]/g, '')
      .replace(/[^a-z0-9]+/g, '-')
      .replace(/^-+|-+$/g, '') || 'section'
  )
}

/** docs/_internal.txt: one path per line, relative to docs/; `#` starts a comment. */
export function internalDocs() {
  const f = path.join(DOCS_DIR, '_internal.txt')
  if (!fs.existsSync(f)) return []
  return fs
    .readFileSync(f, 'utf8')
    .split('\n')
    .map((l) => l.replace(/#.*/, '').trim())
    .filter(Boolean)
}

/** Every published page: docs/**\/*.md minus "_" files and the internal list. */
export function publishedFiles() {
  const internal = new Set(internalDocs())
  const walk = (dir) => {
    const out = []
    for (const e of fs.readdirSync(dir, { withFileTypes: true })) {
      const p = path.join(dir, e.name)
      if (e.isDirectory()) out.push(...walk(p))
      else if (e.name.endsWith('.md') && !e.name.startsWith('_') && !internal.has(path.relative(DOCS_DIR, p).replace(/\\/g, '/'))) out.push(p)
    }
    return out
  }
  return walk(DOCS_DIR).sort()
}

/** docs/operations/index.md → "operations", docs/blobs.md → "blobs". */
function slugOf(file) {
  const rel = path.relative(DOCS_DIR, file).replace(/\\/g, '/').replace(/\.md$/, '')
  return rel.endsWith('/index') ? rel.slice(0, -'/index'.length) : rel
}

function splitFrontMatter(src, file) {
  const m = src.match(/^---\n([\s\S]*?)\n---\n?/)
  if (!m) throw new Error(`${file}: missing front matter (--- title/section/order/summary ---)`)
  const fm = loadYaml(m[1]) ?? {}
  const lines = m[0].split('\n').length - 1
  return { fm, body: src.slice(m[0].length), bodyLine: lines }
}

/** Fenced-visual helpers, rendered with the page's markdown-it for inline text. */
function renderFacts(facts, md, where) {
  if (!Array.isArray(facts) || !facts.length) throw new Error(`${where}: facts must be a non-empty list`)
  return (
    `<div class="facts">` +
    facts
      .map((f, i) => {
        if (f?.value === undefined || !f.label) throw new Error(`${where}: fact ${i + 1} needs value and label`)
        const extra = Object.keys(f).filter((k) => !['value', 'unit', 'label', 'note', 'tone'].includes(k))
        if (extra.length) throw new Error(`${where}: fact ${i + 1}: unknown key(s) ${extra.join(', ')} (a comma in an unquoted string? quote it)`)
        const tone = f.tone ? ` fact-${esc(f.tone)}` : ''
        return (
          `<div class="fact${tone}"><div class="fact-v">${esc(f.value)}${f.unit ? `<span class="fact-u">${esc(f.unit)}</span>` : ''}</div>` +
          `<div class="fact-l">${md.renderInline(String(f.label))}</div>` +
          (f.note ? `<div class="fact-n">${md.renderInline(String(f.note))}</div>` : '') +
          `</div>`
        )
      })
      .join('') +
    `</div>`
  )
}

function renderFigure(spec, where, render = renderDiagram) {
  let d
  try {
    d = render(spec)
  } catch (e) {
    throw new Error(`${where}: ${e.message}`)
  }
  // timelines keep their text readable on a phone and scroll instead
  const wide = render === renderTimeline ? (d.width > 620 ? ' tl-wide' : '') : d.width > 560 ? ' dg-wide' : ''
  // DocsApp opens the diagram full screen from this button (or a click on the diagram)
  const expand =
    `<button type="button" class="dg-expand" aria-label="Expand diagram" title="Expand">` +
    `<svg viewBox="0 0 20 20" width="16" height="16" aria-hidden="true"><path d="M12 3.5h4.5V8M8 16.5H3.5V12M16.5 3.5 11.5 8.5M3.5 16.5l5-5"/></svg></button>`
  return (
    `<figure class="figure${wide}">${expand}<div class="dg-scroll">${d.svg}</div>` +
    (spec.caption ? `<figcaption>${esc(spec.caption).replace(/`([^`]+)`/g, '<code>$1</code>')}</figcaption>` : '') +
    `</figure>`
  )
}

function renderSteps(steps, md, where) {
  if (!Array.isArray(steps) || !steps.length) throw new Error(`${where}: steps must be a non-empty list`)
  return (
    `<ol class="steps">` +
    steps
      .map((s, i) => {
        if (!s?.title) throw new Error(`${where}: step ${i + 1} needs a title`)
        return `<li><div class="step-t">${md.renderInline(String(s.title))}</div>${s.body ? `<div class="step-b">${md.render(String(s.body))}</div>` : ''}</li>`
      })
      .join('') +
    `</ol>`
  )
}

function makeMd(ctx) {
  const md = new MarkdownIt({ html: true, linkify: false, typographer: false })

  md.renderer.rules.fence = (tokens, idx) => {
    const t = tokens[idx]
    const info = t.info.trim().split(/\s+/)[0]
    const where = `${ctx.file}:${(t.map?.[0] ?? 0) + 1 + ctx.bodyLine} (${info})`
    if (VISUAL_FENCES.has(info)) {
      let spec
      try {
        spec = loadYaml(t.content)
      } catch (e) {
        throw new Error(`${where}: bad YAML: ${e.message}`)
      }
      switch (info) {
        case 'diagram':
          return renderFigure(spec, where)
        case 'timeline':
          return renderFigure(spec, where, renderTimeline)
        case 'facts':
          return renderFacts(spec, md, where)
        case 'steps':
          return renderSteps(spec, md, where)
        case 'pages':
          return ctx.renderPages(spec, where)
        case 'hero': {
          if (!(spec?.diagram || spec?.timeline) || !spec?.facts) throw new Error(`${where}: hero needs a diagram (or a timeline) and facts`)
          if (spec.diagram && spec.timeline) throw new Error(`${where}: hero takes a diagram or a timeline, not both`)
          try {
            checkKeys(spec, 'hero', 'hero')
          } catch (e) {
            throw new Error(`${where}: ${e.message}`)
          }
          const fig = spec.timeline ? renderFigure(spec.timeline, where, renderTimeline) : renderFigure(spec.diagram, where)
          return `<section class="hero">${fig}${renderFacts(spec.facts, md, where)}</section>`
        }
      }
    }
    const lang = LANG_ALIASES[info] ?? info
    const code = lang && hljs.getLanguage(lang) ? hljs.highlight(t.content, { language: lang, ignoreIllegals: true }).value : esc(t.content)
    // DocsApp copies the block from this button
    return (
      `<div class="code-block"><pre class="code"><code class="hljs${lang ? ` language-${esc(lang)}` : ''}">${code}</code></pre>` +
      `<button type="button" class="code-copy" aria-label="Copy code">Copy</button></div>`
    )
  }

  // Heading ids (and the page's table of contents).
  md.core.ruler.push('heading_ids', (state) => {
    const seen = new Set()
    const toks = state.tokens
    for (let i = 0; i < toks.length; i++) {
      if (toks[i].type !== 'heading_open') continue
      const inline = toks[i + 1]
      const text = inline.children.map((c) => c.content).join('')
      let id = slugify(text)
      for (let n = 2; seen.has(id); n++) id = `${slugify(text)}-${n}`
      seen.add(id)
      toks[i].attrSet('id', id)
      const level = Number(toks[i].tag.slice(1))
      if (level === 1) ctx.errors.push(`${ctx.file}:${(toks[i].map?.[0] ?? 0) + 1 + ctx.bodyLine}: use ## and below; the title comes from front matter`)
      if (level === 2 || level === 3) ctx.headings.push({ id, text, level })
    }
  })
  md.renderer.rules.heading_close = (tokens, idx) => {
    const open = tokens[idx - 2]
    const id = open?.attrGet('id')
    return id ? `<a class="h-anchor" href="#${id}" aria-label="Link to this section">#</a></${tokens[idx].tag}>\n` : `</${tokens[idx].tag}>\n`
  }

  // Internal links: "architecture.md#x" or "../blobs.md" → /docs/<slug>#x.
  const defaultLink = md.renderer.rules.link_open ?? ((t, i, o, e, s) => s.renderToken(t, i, o))
  md.renderer.rules.link_open = (tokens, idx, opts, env, self) => {
    const t = tokens[idx]
    const href = t.attrGet('href') ?? ''
    if (/^[a-z]+:/i.test(href)) {
      t.attrSet('rel', 'noreferrer')
    } else if (href.startsWith('#')) {
      ctx.links.push({ slug: ctx.slug, anchor: href.slice(1), where: `${ctx.file}` })
    } else {
      const [p, anchor] = href.split('#')
      let slug
      if (p.startsWith('/docs')) slug = p.replace(/^\/docs\/?/, '').replace(/\/$/, '') || 'overview'
      else if (p.endsWith('.md')) slug = slugOf(path.resolve(path.dirname(path.join(DOCS_DIR, ctx.file)), p))
      else ctx.errors.push(`${ctx.file}: link ${JSON.stringify(href)}: link other pages by their .md file (e.g. cluster.md#resharding)`)
      if (slug !== undefined) {
        ctx.links.push({ slug, anchor, where: `${ctx.file} → ${href}` })
        t.attrSet('href', `/docs/${slug}${anchor ? `#${anchor}` : ''}`)
      }
    }
    return defaultLink(tokens, idx, opts, env, self)
  }

  // Tables scroll on their own instead of widening the page.
  md.renderer.rules.table_open = () => '<div class="table-wrap"><table>\n'
  md.renderer.rules.table_close = () => '</table></div>\n'
  return md
}

/** GitHub-style callouts (a blockquote starting with [!NOTE] / [!TIP] / [!WARNING] / [!DANGER]); author comments dropped. */
function callouts(html) {
  return html.replace(/<!--[\s\S]*?-->\n?/g, '').replace(/<blockquote>\n<p>\[!(NOTE|TIP|WARNING|DANGER)\]\s*/g, (_, k) => {
    const kind = k.toLowerCase()
    const label = { note: 'Note', tip: 'Tip', warning: 'Warning', danger: 'Danger' }[kind]
    return `<blockquote class="callout callout-${kind}"><p><strong class="callout-k">${label}</strong> `
  })
}

/**
 * Every page, rendered, in nav order, plus validation errors. Never throws
 * for content problems: the caller decides (the build fails, the dev server
 * shows them).
 */
export function loadDocs() {
  const errors = []
  const internal = internalDocs()
  for (const f of internal) if (!fs.existsSync(path.join(DOCS_DIR, f))) errors.push(`docs/_internal.txt: ${f} doesn't exist`)
  const files = publishedFiles()
  const metas = []
  for (const file of files) {
    const rel = path.relative(DOCS_DIR, file).replace(/\\/g, '/')
    let parsed
    try {
      parsed = splitFrontMatter(fs.readFileSync(file, 'utf8'), rel)
    } catch (e) {
      errors.push(e.message)
      continue
    }
    const { fm } = parsed
    for (const k of ['title', 'section', 'summary']) if (typeof fm[k] !== 'string' || !fm[k].trim()) errors.push(`${rel}: front matter needs ${k}`)
    if (typeof fm.order !== 'number') errors.push(`${rel}: front matter needs a numeric order`)
    const status = fm.status ?? 'ready'
    if (!STATUSES.has(status)) errors.push(`${rel}: status must be one of ${[...STATUSES].join(', ')}`)
    const slug = slugOf(file)
    const dir = slug.includes('/') ? slug.split('/')[0] : rel.endsWith('/index.md') ? slug : ''
    metas.push({ file: rel, slug, dir, title: fm.title, section: fm.section, order: fm.order, summary: fm.summary, status, ...parsed })
  }

  // Nav: sections ordered by their lowest page order; a directory's pages
  // share one section.
  const sections = new Map()
  for (const m of metas) {
    if (!sections.has(m.section)) sections.set(m.section, [])
    sections.get(m.section).push(m)
  }
  for (const [name, ps] of sections) {
    ps.sort((a, b) => a.order - b.order)
    const dirs = new Set(ps.map((p) => p.dir))
    if (dirs.size > 1) errors.push(`section ${JSON.stringify(name)} mixes pages from ${[...dirs].map((d) => d || 'docs/').join(' and ')}`)
    const orders = ps.map((p) => p.order)
    if (new Set(orders).size !== orders.length) errors.push(`section ${JSON.stringify(name)}: duplicate order values`)
  }
  const nav = [...sections.entries()].sort((a, b) => a[1][0].order - b[1][0].order).map(([name, ps]) => ({ name, pages: ps.map((p) => p.slug) }))
  const ordered = nav.flatMap((s) => s.pages.map((slug) => metas.find((m) => m.slug === slug)))
  if (!metas.some((m) => m.slug === 'overview')) errors.push('docs/overview.md is missing (it is /docs)')

  const links = []
  const pages = []
  for (const m of ordered) {
    const ctx = {
      file: m.file,
      slug: m.slug,
      bodyLine: m.bodyLine,
      errors,
      headings: [],
      links,
      renderPages: (spec, where) => {
        const dir = spec?.dir ?? m.slug
        const kids = ordered.filter((p) => p.dir === dir && p.slug !== dir)
        if (!kids.length) throw new Error(`${where}: no pages under ${dir}/`)
        return (
          `<div class="page-cards">` +
          kids
            .map((p) => `<a class="page-card" href="/docs/${p.slug}"><strong>${esc(p.title)}</strong><span>${esc(p.summary ?? '')}</span>${p.status !== 'ready' ? `<em class="badge">${p.status === 'stub' ? 'outline' : p.status}</em>` : ''}</a>`)
            .join('') +
          `</div>`
        )
      },
    }
    const firstBlock = m.body.replace(/^(\s*<!--[\s\S]*?-->\s*)*/, '').trimStart()
    if (!firstBlock.startsWith('```hero')) errors.push(`${m.file}: every page starts with a \`\`\`hero block (style guide: "Page template")`)
    let html = ''
    try {
      html = callouts(makeMd(ctx).render(m.body))
    } catch (e) {
      errors.push(e.message)
    }
    pages.push({ slug: m.slug, title: m.title, section: m.section, order: m.order, summary: m.summary, status: m.status, file: m.file, headings: ctx.headings, html })
  }

  const bySlug = new Map(pages.map((p) => [p.slug, p]))
  for (const l of links) {
    const target = bySlug.get(l.slug)
    if (!target)
      errors.push(`${l.where}: no page ${JSON.stringify(l.slug)}${internal.some((f) => f.replace(/\.md$/, '') === l.slug) ? ' (it is internal: docs/_internal.txt)' : ''}`)
    else if (l.anchor && !new RegExp(`id="${l.anchor.replace(/[.*+?^${}()|[\]\\]/g, '\\$&')}"`).test(target.html))
      errors.push(`${l.where}: no heading #${l.anchor} on ${l.slug}`)
  }
  return { pages, nav, errors, internal }
}
