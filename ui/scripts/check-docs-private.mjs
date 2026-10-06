// The docs site is public: fail the docs check if a published page matches a
// pattern in docs-deny.txt (this deployment's hosts, networks, accounts and
// paths, and the names of internal notes). Internal notes (docs/_internal.txt)
// and "_" files aren't published, so they're skipped.
import { existsSync, readFileSync } from 'node:fs'
import { join, relative, dirname } from 'node:path'
import { fileURLToPath } from 'node:url'
import { DOCS_DIR, publishedFiles } from '../docs-build/docs.mjs'

const denyFile = join(dirname(fileURLToPath(import.meta.url)), 'docs-deny.txt')
if (!existsSync(denyFile)) {
  console.log('check-docs-private: no docs-deny.txt, skipped')
  process.exit(0)
}
const patterns = readFileSync(denyFile, 'utf8')
  .split('\n')
  .map((l) => l.trim())
  .filter((l) => l && !l.startsWith('#'))
  .map((l) => new RegExp(l, 'i'))

const hits = []
for (const file of publishedFiles()) {
  readFileSync(file, 'utf8')
    .split('\n')
    .forEach((line, i) => {
      for (const re of patterns) {
        const m = line.match(re)
        if (m) hits.push(`  ${relative(DOCS_DIR, file)}:${i + 1}: "${m[0]}"  ${line.trim().slice(0, 120)}`)
      }
    })
}

if (hits.length) {
  console.error(
    `check-docs-private: ${hits.length} reference(s) to private infrastructure or internal notes in published docs.\n` +
      `The docs are public: use placeholders (relay.example.com, <your-bucket>, "a 16-core bench box"), and leave\n` +
      `dev-only material on a page listed in docs/_internal.txt.\n` +
      hits.join('\n'),
  )
  process.exit(1)
}
console.log('check-docs-private: ok')
