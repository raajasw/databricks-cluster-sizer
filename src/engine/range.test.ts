import { describe, it, expect } from 'vitest';
import fc from 'fast-check';
import {
  point, range, fromBounds, isPoint, addRange, subRange, mulRange, divRange,
  scaleRange, mulAll, mapRange, clampRange, maxRange, minRange,
  sizingValue, throughputValue, spread, isFinitePositive, type Range,
} from './range';

const arbRange = (): fc.Arbitrary<Range> =>
  fc.tuple(
    fc.double({ min: 0.01, max: 1e6, noNaN: true }),
    fc.double({ min: 0.01, max: 1e6, noNaN: true }),
    fc.double({ min: 0.01, max: 1e6, noNaN: true }),
  ).map(([a, b, c]) => {
    const [low, mid, high] = [a, b, c].sort((x, y) => x - y) as [number, number, number];
    return { low, mid, high };
  });

describe('construction', () => {
  it('point collapses all three bounds', () => {
    const p = point(5);
    expect(p).toEqual({ low: 5, mid: 5, high: 5 });
    expect(isPoint(p)).toBe(true);
  });

  it('range enforces the ordering invariant', () => {
    expect(() => range(3, 2, 1)).toThrow(/invariant/);
    expect(() => range(1, 5, 3)).toThrow(/invariant/);
    expect(range(1, 2, 3).mid).toBe(2);
  });

  it('fromBounds uses the geometric mean', () => {
    // Geometric, not arithmetic: these factors are multiplicative, so the
    // midpoint of 2x..8x should be 4x, not 5x.
    const r = fromBounds(2, 8);
    expect(r.mid).toBeCloseTo(4, 10);
  });

  it('fromBounds rejects inverted bounds', () => {
    expect(() => fromBounds(9, 1)).toThrow();
  });
});

describe('arithmetic', () => {
  it('adds and subtracts with correct bound pairing', () => {
    const a = range(1, 2, 3);
    const b = range(10, 20, 30);
    expect(addRange(a, b)).toEqual({ low: 11, mid: 22, high: 33 });
    // Subtraction pairs low-with-high: the smallest result comes from the
    // smallest a against the largest b.
    expect(subRange(b, a)).toEqual({ low: 7, mid: 18, high: 29 });
  });

  it('multiplies bound-wise', () => {
    expect(mulRange(range(2, 3, 4), range(10, 20, 30)))
      .toEqual({ low: 20, mid: 60, high: 120 });
  });

  it('divides with inverted bound pairing', () => {
    expect(divRange(range(10, 20, 30), range(2, 4, 5)))
      .toEqual({ low: 2, mid: 5, high: 15 });
  });

  it('refuses division by a range touching zero', () => {
    expect(() => divRange(point(1), range(0, 1, 2))).toThrow(/zero/);
  });

  it('scaleRange flips bounds for negative multipliers', () => {
    const r = scaleRange(range(1, 2, 3), -1);
    expect(r.low).toBe(-3);
    expect(r.high).toBe(-1);
  });

  it('mulAll chains from an identity of 1', () => {
    expect(mulAll()).toEqual(point(1));
    expect(mulAll(point(2), point(3), point(4))).toEqual(point(24));
  });

  it('mapRange re-sorts under order-reversing functions', () => {
    const r = mapRange(range(1, 2, 4), (x) => 1 / x);
    expect(r.low).toBeCloseTo(0.25, 10);
    expect(r.high).toBeCloseTo(1, 10);
  });

  it('clamps, maxes and mins bound-wise', () => {
    expect(clampRange(range(1, 5, 100), 2, 50)).toEqual({ low: 2, mid: 5, high: 50 });
    expect(maxRange(range(1, 2, 3), range(2, 2, 2))).toEqual({ low: 2, mid: 2, high: 3 });
    expect(minRange(range(1, 2, 3), range(2, 2, 2))).toEqual({ low: 1, mid: 2, high: 2 });
  });
});

describe('sizing policy', () => {
  it('sizes memory above the midpoint and throughput below it', () => {
    const r = range(10, 20, 40);
    expect(sizingValue(r)).toBeGreaterThan(r.mid);
    expect(sizingValue(r)).toBeLessThanOrEqual(r.high);
    expect(throughputValue(r)).toBeLessThan(r.mid);
    expect(throughputValue(r)).toBeGreaterThanOrEqual(r.low);
  });

  it('collapses to the value itself for a point', () => {
    expect(sizingValue(point(7))).toBe(7);
    expect(throughputValue(point(7))).toBe(7);
  });

  it('reports relative spread', () => {
    expect(spread(point(5))).toBe(0);
    expect(spread(range(5, 10, 15))).toBeCloseTo(1, 10);
  });
});

describe('properties', () => {
  it('every operation preserves low <= mid <= high', () => {
    fc.assert(
      fc.property(arbRange(), arbRange(), (a, b) => {
        const results = [
          addRange(a, b), subRange(a, b), mulRange(a, b), divRange(a, b),
          scaleRange(a, 2.5), maxRange(a, b), minRange(a, b),
          clampRange(a, 1, 1000), mapRange(a, (x) => x * 2),
        ];
        return results.every((r) => r.low <= r.mid && r.mid <= r.high);
      }),
      { numRuns: 500 },
    );
  });

  it('never produces NaN or Infinity from finite positive inputs', () => {
    fc.assert(
      fc.property(arbRange(), arbRange(), (a, b) => {
        const results = [addRange(a, b), mulRange(a, b), divRange(a, b), subRange(a, b)];
        return results.every(isFinitePositive) || results.every((r) =>
          [r.low, r.mid, r.high].every(Number.isFinite));
      }),
      { numRuns: 500 },
    );
  });

  it('sizingValue and throughputValue stay within bounds', () => {
    fc.assert(
      fc.property(arbRange(), (r) => {
        const s = sizingValue(r);
        const t = throughputValue(r);
        return s >= r.low && s <= r.high && t >= r.low && t <= r.high;
      }),
      { numRuns: 500 },
    );
  });
});
