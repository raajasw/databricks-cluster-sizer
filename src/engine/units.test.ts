import { describe, it, expect } from 'vitest';
import fc from 'fast-check';
import {
  mib, gib, decimalGB, millicores, parseSparkBytes, formatSparkBytes,
  formatK8sCpu, roundDownTo, roundUpTo, bytes, cores,
} from './units';

describe('constructors', () => {
  it('uses binary multipliers', () => {
    expect(mib(1)).toBe(1048576);
    expect(gib(1)).toBe(1073741824);
    expect(gib(64)).toBe(68719476736);
  });

  it('keeps decimal GB distinct from binary GiB', () => {
    // The ~7.4% gap that silently undersizes clusters when catalogs are
    // ingested as if vendor "GB" meant GiB.
    expect(decimalGB(64)).toBe(64e9);
    expect(decimalGB(64)).toBeLessThan(gib(64));
    expect(gib(64) / decimalGB(64)).toBeCloseTo(1.0737, 3);
  });

  it('converts millicores', () => {
    expect(millicores(3500)).toBe(3.5);
    expect(millicores(1000)).toBe(1);
  });
});

describe('parseSparkBytes', () => {
  it('treats a bare number as bytes, matching JavaUtils', () => {
    expect(parseSparkBytes('1024')).toBe(1024);
  });

  it('parses 1024-based suffixes', () => {
    expect(parseSparkBytes('512m')).toBe(512 * 1024 ** 2);
    expect(parseSparkBytes('4g')).toBe(4 * 1024 ** 3);
    expect(parseSparkBytes('384M')).toBe(384 * 1024 ** 2);
    expect(parseSparkBytes('2gb')).toBe(2 * 1024 ** 3);
    expect(parseSparkBytes('1t')).toBe(1024 ** 4);
  });

  it('tolerates surrounding whitespace', () => {
    expect(parseSparkBytes('  8g ')).toBe(8 * 1024 ** 3);
  });

  it('rejects malformed input', () => {
    expect(() => parseSparkBytes('abc')).toThrow(/Not a Spark byte string/);
    expect(() => parseSparkBytes('12x')).toThrow(/Unknown Spark byte suffix/);
    expect(() => parseSparkBytes('')).toThrow();
  });
});

describe('formatSparkBytes', () => {
  it('picks the largest evenly-dividing suffix', () => {
    expect(formatSparkBytes(gib(18))).toBe('18g');
    expect(formatSparkBytes(mib(384))).toBe('384m');
    expect(formatSparkBytes(mib(1536))).toBe('1536m'); // 1.5g is not an integer
  });

  it('rounds up to MiB when nothing divides evenly', () => {
    // Rounding a memory grant DOWN could drop a container below the sizing
    // assumption, so the fallback must round up.
    expect(formatSparkBytes(bytes(1048577))).toBe('2m');
  });

  it('round-trips through parseSparkBytes for clean values', () => {
    for (const v of [gib(1), gib(31), mib(384), mib(512)]) {
      expect(parseSparkBytes(formatSparkBytes(v))).toBe(v);
    }
  });
});

describe('formatK8sCpu', () => {
  it('emits integers bare and fractions as millicores', () => {
    expect(formatK8sCpu(cores(4))).toBe('4');
    expect(formatK8sCpu(cores(3.5))).toBe('3500m');
    expect(formatK8sCpu(cores(0.8))).toBe('800m');
  });
});

describe('rounding', () => {
  it('rounds toward the named direction', () => {
    expect(roundDownTo(bytes(1000), bytes(256))).toBe(768);
    expect(roundUpTo(bytes(1000), bytes(256))).toBe(1024);
    expect(roundDownTo(bytes(1024), bytes(256))).toBe(1024);
    expect(roundUpTo(bytes(1024), bytes(256))).toBe(1024);
  });
});

describe('properties', () => {
  it('formatSparkBytes output always re-parses to >= the input', () => {
    fc.assert(
      fc.property(fc.integer({ min: 1, max: 2 ** 45 }), (n) => {
        const parsed = parseSparkBytes(formatSparkBytes(bytes(n)));
        return parsed >= n;
      }),
      { numRuns: 500 },
    );
  });

  it('roundDownTo <= input <= roundUpTo', () => {
    fc.assert(
      fc.property(
        fc.integer({ min: 0, max: 2 ** 40 }),
        fc.integer({ min: 1, max: 2 ** 20 }),
        (n, unit) => {
          const down = roundDownTo(bytes(n), bytes(unit));
          const up = roundUpTo(bytes(n), bytes(unit));
          return down <= n && n <= up && down % unit === 0 && up % unit === 0;
        },
      ),
      { numRuns: 500 },
    );
  });
});
