/**
 * Display formatting.
 *
 * formatRange structurally refuses to collapse a range to its midpoint. That
 * rule is enforced here rather than left to developer discipline, because a
 * single component rendering `.mid` would quietly undo the engine's entire
 * treatment of uncertainty.
 */

import type { Range } from '../../engine/range';
import { isPoint } from '../../engine/range';

export function formatBytes(n: number): string {
  const abs = Math.abs(n);
  if (abs >= 1024 ** 4) return `${trim(n / 1024 ** 4)} TiB`;
  if (abs >= 1024 ** 3) return `${trim(n / 1024 ** 3)} GiB`;
  if (abs >= 1024 ** 2) return `${trim(n / 1024 ** 2)} MiB`;
  if (abs >= 1024) return `${trim(n / 1024)} KiB`;
  return `${Math.round(n)} B`;
}

function trim(n: number): string {
  if (Number.isInteger(n)) return `${n}`;
  if (Math.abs(n) >= 100) return n.toFixed(0);
  if (Math.abs(n) >= 10) return n.toFixed(1);
  return n.toFixed(2);
}

export function formatNumber(n: number): string {
  return n.toLocaleString(undefined, { maximumFractionDigits: 1 });
}

/** Never returns a bare midpoint for a non-point range. */
export function formatRange(r: Range, unit: 'bytes' | 'plain' | 'x' = 'plain'): string {
  const f = unit === 'bytes' ? formatBytes : trim;
  const suffix = unit === 'x' ? 'x' : '';
  if (isPoint(r)) return `${f(r.mid)}${suffix}`;
  return `${f(r.mid)}${suffix} (${f(r.low)}-${f(r.high)}${suffix})`;
}

export function formatSeconds(s: number): string {
  if (s < 60) return `${trim(s)}s`;
  if (s < 3600) return `${trim(s / 60)} min`;
  return `${trim(s / 3600)} hr`;
}

export function formatPercent(f: number): string {
  return `${Math.round(f * 100)}%`;
}

/** Parses "2 GB", "500gb", "1.5 TiB" into bytes. Binary units throughout. */
export function parseSize(input: string): number | null {
  const m = /^\s*([\d.]+)\s*([a-zA-Z]*)\s*$/.exec(input);
  if (!m) return null;
  const n = Number(m[1]);
  if (!Number.isFinite(n)) return null;
  const unit = (m[2] ?? '').toLowerCase().replace(/i?b$/, '');
  const mult: Record<string, number> = {
    '': 1024 ** 3, k: 1024, m: 1024 ** 2, g: 1024 ** 3, t: 1024 ** 4, p: 1024 ** 5,
  };
  const factor = mult[unit];
  return factor === undefined ? null : n * factor;
}
