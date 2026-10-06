// `npm run check-docs`: validates docs/ without a full build (front matter,
// a hero first on every page, diagrams, links and anchors) and prints the nav.
import { loadDocs } from './docs.mjs'

const { pages, nav, errors, internal } = loadDocs()
for (const s of nav) {
  console.log(s.name)
  for (const slug of s.pages) {
    const p = pages.find((x) => x.slug === slug)
    console.log(`  /docs/${slug}${p.status !== 'ready' ? ` (${p.status})` : ''}`)
  }
}
if (internal.length) console.log(`\nNot published (docs/_internal.txt): ${internal.join(', ')}`)
if (errors.length) {
  console.error(`\n${errors.length} problem(s):\n  ${errors.join('\n  ')}`)
  process.exit(1)
}
console.log(`\n${pages.length} pages OK`)
