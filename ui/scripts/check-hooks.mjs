// Rules of hooks for src/**: no hook after an early return, inside a condition or loop, or in a
// callback. eslint-plugin-react-hooks can't see the console's shared polls (`quorumPoll.use()`):
// it only treats `use` on a PascalCase namespace as a hook, and lets `use` run conditionally. A
// hook after an early return only throws when the first render takes that return, so a page
// works when navigated to with a warm poll and goes blank on a cold load.
import { readFileSync, readdirSync } from 'node:fs'
import { join, relative } from 'node:path'
import { parseAst } from 'vite'

const root = new URL('../src/', import.meta.url).pathname
const files = (function walk(d) {
  return readdirSync(d, { withFileTypes: true }).flatMap((e) => (e.isDirectory() ? walk(join(d, e.name)) : /\.tsx?$/.test(e.name) && !e.name.endsWith('.d.ts') ? [join(d, e.name)] : []))
})(root)

const isHookName = (n) => n === 'use' || /^use[A-Z0-9]/.test(n)
const hookName = (call) => {
  const c = call.callee
  if (c.type === 'Identifier' && isHookName(c.name)) return c.name
  if (c.type === 'MemberExpression' && !c.computed && c.property.type === 'Identifier' && isHookName(c.property.name)) {
    const o = c.object
    return `${o.type === 'Identifier' ? o.name : '…'}.${c.property.name}`
  }
}
const isFn = (n) => n.type === 'FunctionDeclaration' || n.type === 'FunctionExpression' || n.type === 'ArrowFunctionExpression'
// components (PascalCase) and hooks (useX) may call hooks; anything else is a callback
const mayHook = (name) => !!name && (/^[A-Z]/.test(name) || isHookName(name))
const wrappers = new Set(['memo', 'forwardRef'])

const lineOf = (src, pos) => src.slice(0, pos).split('\n').length
let bad = 0

for (const file of files) {
  const src = readFileSync(file, 'utf8')
  const ast = parseAst(src, { lang: file.endsWith('.tsx') ? 'tsx' : 'ts' })
  const report = (node, msg) => {
    bad++
    console.error(`${relative(process.cwd(), file)}:${lineOf(src, node.start)}  ${msg}`)
  }

  // fn: { name, returns: [end offsets] }; cond: the innermost conditional construct since fn
  function visit(node, parent, fn, cond) {
    if (!node || typeof node.type !== 'string') return
    if (isFn(node)) {
      let name = node.id?.name
      if (!name && parent?.type === 'VariableDeclarator' && parent.id.type === 'Identifier') name = parent.id.name
      // a detail kind's `use: (id) => …` and a poll's `use: () => …` are hooks
      if (!name && (parent?.type === 'Property' || parent?.type === 'MethodDefinition') && !parent.computed && parent.key.type === 'Identifier') name = parent.key.name
      if (!name && parent?.type === 'CallExpression' && parent.callee.type === 'Identifier' && wrappers.has(parent.callee.name)) name = 'Wrapped'
      const inner = { name, returns: [], node }
      // returns first, so a hook can be checked against every return that precedes it
      collectReturns(node.body, inner.returns)
      children(node, (c) => visit(c, node, inner, null))
      return
    }
    if (node.type === 'CallExpression') {
      const h = hookName(node)
      if (h && fn) {
        if (!mayHook(fn.name)) report(node, `${h}() in ${fn.name ? `${fn.name}(), which isn't a component or hook` : 'a callback'}`)
        else if (cond) report(node, `${h}() inside ${cond} in ${fn.name}()`)
        else if (fn.returns.some((r) => r <= node.start)) report(node, `${h}() after an early return in ${fn.name}()`)
      }
    }
    children(node, (c, key) => visit(c, node, fn, condOf(node, key) ?? cond))
  }

  function condOf(node, key) {
    switch (node.type) {
      case 'IfStatement':
        return key === 'test' ? null : 'an if'
      case 'ConditionalExpression':
        return key === 'test' ? null : 'a ternary'
      case 'LogicalExpression':
        return key === 'right' ? `a ${node.operator}` : null
      case 'ForStatement':
      case 'ForInStatement':
      case 'ForOfStatement':
      case 'WhileStatement':
      case 'DoWhileStatement':
        return 'a loop'
      case 'SwitchCase':
        return 'a switch'
      case 'TryStatement':
        return key === 'block' ? null : 'a catch'
    }
    return null
  }

  function collectReturns(node, out) {
    if (!node || typeof node.type !== 'string' || isFn(node)) return
    if (node.type === 'ReturnStatement') out.push(node.end)
    children(node, (c) => collectReturns(c, out))
  }

  visit(ast, null, null, null)
}

function children(node, f) {
  for (const key of Object.keys(node)) {
    if (key === 'parent') continue
    const v = node[key]
    if (Array.isArray(v)) v.forEach((c) => c && typeof c === 'object' && f(c, key))
    else if (v && typeof v === 'object' && typeof v.type === 'string') f(v, key)
  }
}

if (bad) {
  console.error(`\n${bad} hook call${bad === 1 ? '' : 's'} break the rules of hooks`)
  process.exit(1)
}
console.log(`hooks ok (${files.length} files)`)
