// Line icons drawn for this UI (1.6px strokes on a 20px grid).
import type { SVGProps } from 'react'

const base = {
  viewBox: '0 0 20 20',
  fill: 'none',
  stroke: 'currentColor',
  strokeWidth: 1.6,
  strokeLinecap: 'round' as const,
  strokeLinejoin: 'round' as const,
  'aria-hidden': true,
}

type P = SVGProps<SVGSVGElement>

export const Mark = (p: P) => (
  <svg width="22" height="18" viewBox="0 0 22 18" aria-hidden="true" {...p}>
    <rect x="0" y="0" width="22" height="4" rx="1" fill="currentColor" />
    <rect x="3" y="7" width="16" height="4" rx="1" fill="currentColor" opacity=".6" />
    <rect x="6" y="14" width="10" height="4" rx="1" fill="currentColor" opacity=".35" />
  </svg>
)
export const Sun = (p: P) => (
  <svg {...base} {...p}>
    <circle cx="10" cy="10" r="3.4" />
    <path d="M10 2.5v1.8M10 15.7v1.8M2.5 10h1.8M15.7 10h1.8M4.7 4.7l1.3 1.3M14 14l1.3 1.3M4.7 15.3 6 14M14 6l1.3-1.3" />
  </svg>
)
export const Moon = (p: P) => (
  <svg {...base} {...p}>
    <path d="M15.8 12.4A6.5 6.5 0 0 1 7.6 4.2a6.5 6.5 0 1 0 8.2 8.2Z" />
  </svg>
)
export const Auto = (p: P) => (
  <svg {...base} {...p}>
    <circle cx="10" cy="10" r="6.5" />
    <path d="M10 3.5v13" />
    <path d="M10 3.5a6.5 6.5 0 0 1 0 13Z" fill="currentColor" stroke="none" />
  </svg>
)
export const Copy = (p: P) => (
  <svg {...base} {...p}>
    <rect x="7" y="7" width="9.5" height="9.5" rx="1.5" />
    <path d="M13 7V5a1.5 1.5 0 0 0-1.5-1.5h-6A1.5 1.5 0 0 0 4 5v6a1.5 1.5 0 0 0 1.5 1.5H7" />
  </svg>
)
export const Check = (p: P) => (
  <svg {...base} {...p}>
    <path d="m4.5 10.5 3.5 3.5 7.5-8" />
  </svg>
)
export const Alert = (p: P) => (
  <svg {...base} {...p}>
    <path d="M10 3 2.8 16h14.4Z" />
    <path d="M10 8.2v3.6M10 14.1v.1" />
  </svg>
)
export const Info = (p: P) => (
  <svg {...base} {...p}>
    <circle cx="10" cy="10" r="7" />
    <path d="M10 9v4.5M10 6.4v.1" />
  </svg>
)
export const Download = (p: P) => (
  <svg {...base} {...p}>
    <path d="M10 3.5v9M6 9l4 4 4-4M4 16.5h12" />
  </svg>
)
export const External = (p: P) => (
  <svg {...base} {...p}>
    <path d="M11.5 4H16v4.5M16 4l-7 7M14 11.5V15a1 1 0 0 1-1 1H5a1 1 0 0 1-1-1V7a1 1 0 0 1 1-1h3.5" />
  </svg>
)
export const Refresh = (p: P) => (
  <svg {...base} {...p}>
    <path d="M15.5 8A6 6 0 0 0 4.6 6.5M4.5 12a6 6 0 0 0 10.9 1.5" />
    <path d="M4.3 3.5v3.2h3.2M15.7 16.5v-3.2h-3.2" />
  </svg>
)
