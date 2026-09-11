/**
 * Step construction helpers.
 *
 * Formula strings are user-facing output, not debug logging: they are what the
 * derivation ladder renders. A formula that says something different from what
 * the code did is the worst possible bug in an explanation tool, so these
 * helpers build the string from the same values the caller returns.
 */

import type { Step, StepId, ScalarOrRange, Citation, Alternative } from '../types/steps';
import type { Confidence } from '../constants/heuristics';
import type { Range } from '../range';
import { isPoint } from '../range';

export interface StepSpec {
  id: StepId;
  title: string;
  formula: string;
  inputs: Record<string, ScalarOrRange>;
  outputs: Record<string, ScalarOrRange>;
  rationale: string;
  confidence: Confidence;
  citations?: Citation[];
  alternatives?: Alternative[];
  pass?: number;
}

export const makeStep = (spec: StepSpec): Step => ({ ...spec });

/** Bytes -> human units, for use inside formula strings. */
export function fmtBytes(n: number): string {
  const abs = Math.abs(n);
  if (abs >= 1024 ** 4) return `${round(n / 1024 ** 4)} TiB`;
  if (abs >= 1024 ** 3) return `${round(n / 1024 ** 3)} GiB`;
  if (abs >= 1024 ** 2) return `${round(n / 1024 ** 2)} MiB`;
  if (abs >= 1024) return `${round(n / 1024)} KiB`;
  return `${Math.round(n)} B`;
}

function round(n: number): string {
  if (Number.isInteger(n)) return `${n}`;
  if (Math.abs(n) >= 100) return n.toFixed(0);
  if (Math.abs(n) >= 10) return n.toFixed(1);
  return n.toFixed(2);
}

/** Ranges never collapse to a midpoint, here or anywhere else. */
export function fmtRange(r: Range, unit: 'bytes' | 'plain' = 'plain'): string {
  const f = unit === 'bytes' ? fmtBytes : round;
  if (isPoint(r)) return f(r.mid);
  return `${f(r.mid)} (${f(r.low)}-${f(r.high)})`;
}

export const fmtX = (r: Range): string =>
  isPoint(r) ? `${round(r.mid)}x` : `${round(r.mid)}x (${round(r.low)}-${round(r.high)}x)`;

export function fmtSeconds(s: number): string {
  if (s < 60) return `${round(s)}s`;
  if (s < 3600) return `${round(s / 60)} min`;
  return `${round(s / 3600)} hr`;
}
