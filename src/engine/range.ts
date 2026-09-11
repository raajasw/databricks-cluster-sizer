/**
 * Interval arithmetic for uncertain quantities.
 *
 * Most numbers in cluster sizing are not knowable to a single value. The
 * in-memory inflation factor of a Parquet file legitimately spans 3-4x
 * depending on dictionary encoding and column types; throughput per core spans
 * 20-100 MiB/s. Reporting a midpoint as if it were a measurement is the core
 * dishonesty of every Spark sizing calculator that exists.
 *
 * DESIGN CHOICE: we use interval arithmetic, not Monte Carlo. Multiplying
 * intervals assumes the extremes are perfectly correlated (all worst cases
 * co-occur), so the resulting interval is WIDER than a probabilistic treatment
 * would give. That is wrong in the statistical sense and right in the
 * engineering sense: a capacity planner should be conservative, and the result
 * stays deterministic, explainable, and unit-testable. The cost is that chained
 * multiplications inflate the interval quickly -- keep chains short and prefer
 * widening a single factor over multiplying many near-point factors.
 */

export interface Range {
  readonly low: number;
  readonly mid: number;
  readonly high: number;
}

/** A known-exact value. low === mid === high. */
export const point = (v: number): Range => ({ low: v, mid: v, high: v });

export function range(low: number, mid: number, high: number): Range {
  if (!(low <= mid && mid <= high)) {
    throw new Error(`Range invariant violated: ${low} <= ${mid} <= ${high}`);
  }
  return { low, mid, high };
}

/** Build from bounds, taking the geometric mean as the midpoint. */
export function fromBounds(low: number, high: number): Range {
  if (low > high) throw new Error(`fromBounds: low ${low} > high ${high}`);
  const mid = low > 0 && high > 0 ? Math.sqrt(low * high) : (low + high) / 2;
  return { low, mid, high };
}

export const isPoint = (r: Range): boolean => r.low === r.high;

// --- arithmetic -------------------------------------------------------------

export const addRange = (a: Range, b: Range): Range => ({
  low: a.low + b.low,
  mid: a.mid + b.mid,
  high: a.high + b.high,
});

export const subRange = (a: Range, b: Range): Range => ({
  low: a.low - b.high,
  mid: a.mid - b.mid,
  high: a.high - b.low,
});

/** Assumes both ranges are non-negative, which holds for every quantity here. */
export const mulRange = (a: Range, b: Range): Range => ({
  low: a.low * b.low,
  mid: a.mid * b.mid,
  high: a.high * b.high,
});

export const divRange = (a: Range, b: Range): Range => {
  if (b.low <= 0 || b.high <= 0) {
    throw new Error(`divRange: divisor range spans or touches zero: [${b.low}, ${b.high}]`);
  }
  return { low: a.low / b.high, mid: a.mid / b.mid, high: a.high / b.low };
};

export const scaleRange = (a: Range, k: number): Range =>
  k >= 0
    ? { low: a.low * k, mid: a.mid * k, high: a.high * k }
    : { low: a.high * k, mid: a.mid * k, high: a.low * k };

export const mulAll = (...rs: Range[]): Range => rs.reduce(mulRange, point(1));

export const mapRange = (a: Range, f: (n: number) => number): Range => {
  const [lo, mi, hi] = [f(a.low), f(a.mid), f(a.high)];
  // f may be order-reversing (e.g. x => 1/x), so re-sort rather than assume.
  const sorted = [lo, hi].sort((x, y) => x - y) as [number, number];
  return { low: sorted[0], mid: mi, high: sorted[1] };
};

export const clampRange = (a: Range, lo: number, hi: number): Range => ({
  low: Math.min(Math.max(a.low, lo), hi),
  mid: Math.min(Math.max(a.mid, lo), hi),
  high: Math.min(Math.max(a.high, lo), hi),
});

export const maxRange = (a: Range, b: Range): Range => ({
  low: Math.max(a.low, b.low),
  mid: Math.max(a.mid, b.mid),
  high: Math.max(a.high, b.high),
});

export const minRange = (a: Range, b: Range): Range => ({
  low: Math.min(a.low, b.low),
  mid: Math.min(a.mid, b.mid),
  high: Math.min(a.high, b.high),
});

// --- policy -----------------------------------------------------------------

/**
 * The value to size MEMORY from.
 *
 * Loss is asymmetric: undersizing memory means an OOM three hours into a
 * production run; oversizing costs maybe 20% on the bill. So we plan against
 * the pessimistic end, not the midpoint. The UI must say it is doing this.
 */
export const sizingValue = (r: Range): number => r.mid + (r.high - r.mid) * 0.6;

/**
 * The value to size THROUGHPUT from -- the conservative (slow) end, for the
 * same asymmetry reason: assuming your cores are faster than they are produces
 * a cluster that misses its SLA.
 */
export const throughputValue = (r: Range): number => r.mid - (r.mid - r.low) * 0.6;

/** Relative width, for deciding whether the UI should flag an estimate as soft. */
export const spread = (r: Range): number => (r.mid === 0 ? 0 : (r.high - r.low) / r.mid);

export const isFinitePositive = (r: Range): boolean =>
  [r.low, r.mid, r.high].every((n) => Number.isFinite(n) && n >= 0);
