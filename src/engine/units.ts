/**
 * Branded numeric types and unit helpers.
 *
 * Every quantity that flows through the engine carries its unit in the type.
 * This exists to kill an entire bug class: cloud vendor catalogs quote memory in
 * decimal GB, Spark parses "512m" as 1024-based MiB, and mixing the two silently
 * undersizes clusters by ~7%.
 *
 * CONVENTION: all internal math is in BYTES, binary (MiB/GiB, 1024-based),
 * matching Spark's `JavaUtils.byteStringAsBytes`. Conversion happens only at the
 * formatting layer and at catalog-ingest boundaries.
 */

export type Bytes = number & { readonly __brand: 'Bytes' };
export type Cores = number & { readonly __brand: 'Cores' };
export type Seconds = number & { readonly __brand: 'Seconds' };

// --- constructors -----------------------------------------------------------

export const bytes = (n: number): Bytes => n as Bytes;
export const kib = (n: number): Bytes => (n * 1024) as Bytes;
export const mib = (n: number): Bytes => (n * 1024 * 1024) as Bytes;
export const gib = (n: number): Bytes => (n * 1024 * 1024 * 1024) as Bytes;
export const tib = (n: number): Bytes => (n * 1024 * 1024 * 1024 * 1024) as Bytes;

/** Decimal GB, as quoted by cloud vendor instance catalogs. */
export const decimalGB = (n: number): Bytes => (n * 1e9) as Bytes;

export const cores = (n: number): Cores => n as Cores;
/** Kubernetes millicores: 500m -> 0.5 cores. */
export const millicores = (n: number): Cores => (n / 1000) as Cores;

export const seconds = (n: number): Seconds => n as Seconds;
export const minutes = (n: number): Seconds => (n * 60) as Seconds;
export const hours = (n: number): Seconds => (n * 3600) as Seconds;

// --- arithmetic -------------------------------------------------------------
// Branded types block raw +/- at the type level, so arithmetic goes through these.

export const addBytes = (...xs: Bytes[]): Bytes =>
  xs.reduce((a, b) => (a + b) as Bytes, 0 as Bytes);
export const subBytes = (a: Bytes, b: Bytes): Bytes => (a - b) as Bytes;
export const scaleBytes = (a: Bytes, k: number): Bytes => (a * k) as Bytes;
export const divBytes = (a: Bytes, b: Bytes): number => a / b;
export const minBytes = (...xs: Bytes[]): Bytes => Math.min(...xs) as Bytes;
export const maxBytes = (...xs: Bytes[]): Bytes => Math.max(...xs) as Bytes;

export const addCores = (...xs: Cores[]): Cores =>
  xs.reduce((a, b) => (a + b) as Cores, 0 as Cores);
export const subCores = (a: Cores, b: Cores): Cores => (a - b) as Cores;
export const scaleCores = (a: Cores, k: number): Cores => (a * k) as Cores;

/** Round DOWN to a multiple of `unit`. Used to emit clean Spark memory values. */
export const roundDownTo = (b: Bytes, unit: Bytes): Bytes =>
  (Math.floor(b / unit) * unit) as Bytes;

/** Round UP to a multiple of `unit`. Used for YARN container rounding. */
export const roundUpTo = (b: Bytes, unit: Bytes): Bytes =>
  (Math.ceil(b / unit) * unit) as Bytes;

export const clampBytes = (b: Bytes, lo: Bytes, hi: Bytes): Bytes =>
  Math.min(Math.max(b, lo), hi) as Bytes;

// --- Spark byte strings -----------------------------------------------------

const SPARK_SUFFIX: Record<string, number> = {
  b: 1,
  k: 1024,
  kb: 1024,
  m: 1024 ** 2,
  mb: 1024 ** 2,
  g: 1024 ** 3,
  gb: 1024 ** 3,
  t: 1024 ** 4,
  tb: 1024 ** 4,
  p: 1024 ** 5,
  pb: 1024 ** 5,
};

/**
 * Parse a Spark byte string ("512m", "4g", "1024") the way Spark does.
 * Mirrors org.apache.spark.network.util.JavaUtils#byteStringAsBytes:
 * suffixes are 1024-based, and a bare number is BYTES.
 */
export function parseSparkBytes(s: string): Bytes {
  const m = /^\s*(-?\d+(?:\.\d+)?)\s*([a-zA-Z]*)\s*$/.exec(s);
  if (!m) throw new Error(`Not a Spark byte string: ${JSON.stringify(s)}`);
  const [, numRaw, suffixRaw] = m;
  const n = Number(numRaw);
  const suffix = (suffixRaw ?? '').toLowerCase();
  if (suffix === '') return bytes(n);
  const mult = SPARK_SUFFIX[suffix];
  if (mult === undefined) throw new Error(`Unknown Spark byte suffix: ${suffixRaw}`);
  return bytes(n * mult);
}

/**
 * Render bytes as a Spark config value ("18g", "384m").
 *
 * Spark accepts only integers here, so we pick the largest suffix that divides
 * evenly and fall back to MiB (rounding up) when nothing does. Rounding UP is
 * deliberate: these strings are memory grants, and rounding a grant down can
 * push a container below what the sizing math assumed.
 */
export function formatSparkBytes(b: Bytes): string {
  const units: Array<[string, number]> = [
    ['t', 1024 ** 4],
    ['g', 1024 ** 3],
    ['m', 1024 ** 2],
    ['k', 1024],
  ];
  for (const [suffix, mult] of units) {
    if (b >= mult && b % mult === 0) return `${b / mult}${suffix}`;
  }
  return `${Math.ceil(b / 1024 ** 2)}m`;
}

/** Render Cores as a Kubernetes CPU quantity ("3500m", "4"). */
export function formatK8sCpu(c: Cores): string {
  return Number.isInteger(c) ? `${c}` : `${Math.round(c * 1000)}m`;
}
