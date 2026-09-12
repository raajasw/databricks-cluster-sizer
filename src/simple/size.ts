/**
 * Databricks worker sizing, from six questions.
 *
 * The whole calculation is here and is meant to be read top to bottom. It is
 * deliberately simpler than a full model: it answers "how many workers, of what
 * type, and roughly how long", and says where it is guessing.
 *
 * The three numbers that matter, and where they come from:
 *
 *   1. How big the data gets in memory. Parquet on disk is compressed and
 *      dictionary-encoded; reading it expands it several times over.
 *   2. How fast one core chews through it. Varies enormously - a Python UDF
 *      is 4-20x slower than the same logic in SQL or Scala.
 *   3. How much memory each core needs. A plain scan streams through; a join
 *      holds far more at once.
 *
 * Cluster size then falls out: work / (rate x time you will accept).
 */

import { type Cloud, type NodeType, type NodeShape, nodesFor } from './nodes';

// --- the questions ---------------------------------------------------------

export type Format = 'parquet' | 'delta' | 'csv' | 'json' | 'avro';
export type Language = 'sql' | 'scala' | 'python-sql' | 'python-udf';
export type Operation = 'scan' | 'aggregate' | 'join' | 'sort';

export interface Answers {
  cloud: Cloud;
  /** Bytes read per run, as stored on disk. */
  dataBytes: number;
  format: Format;
  language: Language;
  operation: Operation;
  /** Acceptable wall-clock, in minutes. */
  targetMinutes: number;
  /** Optional override; otherwise the tool picks. */
  nodeTypeId?: string;
}

// --- the constants, and why they are what they are -------------------------

/**
 * How much bigger data gets once read into memory.
 *
 * Columnar formats are compressed AND encoded on disk - a column of repeated
 * strings is stored once in a dictionary with small integer codes, then becomes
 * one string object per row in memory. Text formats are not compressed as
 * tightly but must be parsed into typed objects, which costs more than it looks.
 */
const EXPANSION: Record<Format, number> = {
  parquet: 6,
  delta: 6,
  avro: 4,
  csv: 3,
  json: 4,
};

/**
 * Megabytes of SOURCE data one core gets through per second.
 *
 * Measured against bytes as stored, not after expansion. That matters: if the
 * rate were applied to the expanded size, formats that expand more would look
 * artificially slower purely because of the expansion factor, which is
 * double-counting. Reading 1 GB of Parquet and 1 GB of CSV are different jobs,
 * but the difference belongs in the format multiplier below, not in both.
 *
 * Anchored on a common reference point: a roughly 100-core cluster aggregating
 * 1 TB of Parquet in 10-20 minutes works out to 11-17 MB of source bytes per
 * core-second. With the aggregate multiplier below, the 40 here lands in that
 * range. A pure scan is faster, which the operation multiplier handles.
 *
 * This is the softest number in the tool by a wide margin - real throughput
 * depends on the query, the data, the file layout and the runtime. Treat the
 * runtime estimate as a rough order of magnitude, not a promise.
 *
 * The Python UDF figure is low for a real reason: every row is serialised out
 * of the JVM, sent to a Python process and sent back. That round trip dominates
 * everything else the job does.
 */
const MB_PER_CORE_SECOND: Record<Language, number> = {
  sql: 40,
  scala: 40,
  'python-sql': 35,
  'python-udf': 5,
};

/**
 * How much slower a format is to read, per source byte.
 *
 * Parquet is the baseline: columnar, typed, and it can skip whole column
 * chunks. Text formats have to be parsed field by field and typed at runtime,
 * which is several times more work for the same bytes off storage.
 */
const FORMAT_READ_COST: Record<Format, number> = {
  parquet: 1,
  delta: 1,
  avro: 1.4,
  csv: 3.5,
  json: 4.5,
};

/**
 * How much slower an operation is than a straight scan.
 *
 * Anything that shuffles has to write its output to local disk, send it across
 * the network and read it back on the other side. That round trip, not the
 * computation, is what costs the time.
 *
 * Calibrated so that an aggregate lands at 11-16 MB of source Parquet per
 * core-second, matching the reference point the base rate is anchored on. The
 * ballpark test in size.test.ts pins this; if these change, that test should
 * fail and be re-argued rather than adjusted to fit.
 */
const OP_SLOWDOWN: Record<Operation, number> = {
  scan: 1,
  aggregate: 2.8,
  join: 4,
  sort: 4.5,
};

/**
 * Gigabytes of memory each core wants.
 *
 * A scan streams a partition through and forgets it. A join holds both sides
 * plus the hash table; a sort holds a whole partition and its buffers. Get this
 * wrong and the job spills to disk, which is not a crash but is several times
 * slower.
 */
const GB_PER_CORE: Record<Operation, number> = {
  scan: 2,
  aggregate: 3,
  join: 6,
  sort: 6,
};

/** Databricks keeps some of each worker for its own agent and the OS. */
const USABLE_FRACTION = 0.75;

const GB = 1_000_000_000;

// --- the answer ------------------------------------------------------------

export interface Recommendation {
  node: NodeType;
  workers: number;
  totalCores: number;
  /** Why this node type, in one line. */
  nodeReason: string;
  estimatedMinutes: number;
  /** Runtime is a guess; show the span, not a false point. */
  estimatedRangeMinutes: [number, number];
  /** Short reasoning, one line each. */
  why: string[];
  /** Things worth knowing about this job. */
  notes: string[];
  /** Settings worth changing from their defaults. */
  config: Array<{ key: string; value: string; note: string }>;
}

function pickShape(a: Answers, available: NodeShape[]): { shape: NodeShape; reason: string } {
  const has = (s: NodeShape) => available.includes(s);

  if (GB_PER_CORE[a.operation] >= 6 && has('memory')) {
    return {
      shape: 'memory',
      reason: `a ${a.operation} holds a lot in memory at once, so it wants more RAM per core`,
    };
  }
  if (a.operation === 'aggregate' && has('storage')) {
    return {
      shape: 'storage',
      reason: 'aggregations shuffle, and shuffle writes land on local disk',
    };
  }
  if (a.operation === 'aggregate' && has('balanced')) {
    return {
      shape: 'balanced',
      reason: 'an aggregation shuffles but does not need extra memory per core',
    };
  }
  if (has('balanced')) {
    return { shape: 'balanced', reason: 'a straight scan does not need extra memory per core' };
  }
  return { shape: available[0]!, reason: 'the only shape available on this cloud' };
}

export function size(a: Answers): Recommendation {
  const catalog = nodesFor(a.cloud);

  // 1. How much data ends up in memory.
  const inMemoryBytes = a.dataBytes * EXPANSION[a.format];

  // 2. How much core-time that takes. Rate applies to SOURCE bytes; the format
  // cost and the operation cost are what make one byte harder than another.
  const mbPerCoreSec =
    MB_PER_CORE_SECOND[a.language] /
    OP_SLOWDOWN[a.operation] /
    FORMAT_READ_COST[a.format];
  const sourceMB = a.dataBytes / 1_000_000;
  const coreSeconds = sourceMB / mbPerCoreSec;

  // 3. How many cores to finish inside the target.
  const targetSeconds = a.targetMinutes * 60;
  const coresNeeded = Math.max(1, Math.ceil(coreSeconds / targetSeconds));

  // 4. Which node, and how many.
  const available = [...new Set(catalog.map((x) => x.shape))];
  const picked = pickShape(a, available);
  const override = a.nodeTypeId ? catalog.find((x) => x.id === a.nodeTypeId) : undefined;

  const pool = catalog.filter((x) => x.shape === picked.shape);

  // Of the preferred shape, take the mid-size box. Very small nodes multiply
  // per-worker overhead; very large ones make the count too coarse to tune.
  const node = override ?? pool[Math.floor(pool.length / 2)] ?? catalog[0]!;

  const usableCores = Math.max(1, Math.floor(node.cores * USABLE_FRACTION));
  const usableMemoryGB = (node.memoryBytes / GB) * USABLE_FRACTION;

  // Memory can bind before cores do: if each core wants 6 GB and the node only
  // has 4 GB per core, some cores must sit idle or the job will spill.
  const coresByMemory = Math.max(1, Math.floor(usableMemoryGB / GB_PER_CORE[a.operation]));
  const effectiveCores = Math.min(usableCores, coresByMemory);
  const memoryBound = coresByMemory < usableCores;

  const workers = Math.max(1, Math.ceil(coresNeeded / effectiveCores));
  const totalCores = workers * effectiveCores;

  // 5. What that actually buys you.
  const estimatedSeconds = coreSeconds / totalCores;
  const estimatedMinutes = estimatedSeconds / 60;

  const why = [
    `${fmtBytes(a.dataBytes)} of ${a.format} becomes roughly ` +
      `${fmtBytes(inMemoryBytes)} once read into memory (about ${EXPANSION[a.format]}x).`,
    `At roughly ${mbPerCoreSec.toFixed(0)} MB of source data per core-second for ` +
      `${languageLabel(a.language)} doing a ${a.operation}, that is about ` +
      `${fmtCoreHours(coreSeconds)} of work.`,
    `To finish in ${a.targetMinutes} minutes you need about ${coresNeeded} cores, ` +
      `which is ${workers} × ${node.id} at ${effectiveCores} usable cores each.`,
  ];

  const notes: string[] = [];

  if (memoryBound) {
    notes.push(
      `A ${a.operation} wants about ${GB_PER_CORE[a.operation]} GB per core, but ` +
      `${node.id} only has ${(usableMemoryGB / usableCores).toFixed(1)} GB per usable core. ` +
      `Each worker is counted at ${effectiveCores} cores rather than ${usableCores} to leave ` +
      'room, otherwise the job would spill to disk and run several times slower.',
    );
  }

  if (a.language === 'python-udf') {
    notes.push(
      'Python UDFs are the reason this cluster is large. Every row leaves the JVM, crosses ' +
      'into a Python process and comes back. Rewriting the UDF with built-in Spark functions ' +
      'is usually worth far more than any amount of extra hardware - often 5-10x.',
    );
  }

  if (a.format === 'csv' || a.format === 'json') {
    notes.push(
      `${a.format.toUpperCase()} has to be parsed on every run, and cannot skip columns or ` +
      'rows the way Parquet can. If this job runs regularly, converting the source to Parquet ' +
      'or Delta once will pay for itself quickly.',
    );
  }

  if (!node.localSsd && (a.operation === 'join' || a.operation === 'sort')) {
    notes.push(
      `${a.operation}s write a lot of shuffle data to local disk, and ${node.id} has no local ` +
      'SSD. Expect shuffle to be the bottleneck.',
    );
  }

  if (workers === 1) {
    notes.push(
      'One worker is a very small cluster. If this is all the data you have, a single machine ' +
      'running DuckDB or pandas will likely be faster than Spark and much simpler.',
    );
  }

  const shufflePartitions = Math.max(totalCores, Math.round((inMemoryBytes / GB / 0.128) / totalCores) * totalCores);

  const config = [
    {
      key: 'spark.sql.shuffle.partitions',
      value: String(shufflePartitions),
      note: `The default is 200, which is unrelated to your data size or cluster. ` +
            `This gives each of your ${totalCores} cores a few partitions to work through.`,
    },
    {
      key: 'spark.sql.adaptive.enabled',
      value: 'true',
      note: 'On by default in current runtimes. It lets Spark fix partition sizes at runtime, ' +
            'which covers a lot of estimation error.',
    },
  ];

  if (a.language === 'python-udf') {
    config.push({
      key: 'spark.executor.memoryOverheadFactor',
      value: '0.4',
      note: 'Python processes live outside the JVM heap. The JVM default of 0.1 undersizes the ' +
            'container and gets it killed by the kernel.',
    });
  }

  return {
    node,
    workers,
    totalCores,
    nodeReason: override
      ? 'You chose this node type.'
      : `Picked because ${picked.reason}.`,
    estimatedMinutes,
    // Throughput genuinely varies by several times, so the span is wide and honest.
    estimatedRangeMinutes: [estimatedMinutes * 0.6, estimatedMinutes * 2.5],
    why,
    notes,
    config,
  };
}

// --- formatting ------------------------------------------------------------

export function fmtBytes(b: number): string {
  if (b >= 1e12) return `${trim(b / 1e12)} TB`;
  if (b >= 1e9) return `${trim(b / 1e9)} GB`;
  if (b >= 1e6) return `${trim(b / 1e6)} MB`;
  return `${Math.round(b)} B`;
}

export function fmtMinutes(m: number): string {
  if (m < 1) return 'under a minute';
  if (m < 90) return `${Math.round(m)} min`;
  return `${trim(m / 60)} hr`;
}

function fmtCoreHours(coreSeconds: number): string {
  const h = coreSeconds / 3600;
  if (h < 1) return `${Math.round(coreSeconds / 60)} core-minutes`;
  return `${trim(h)} core-hours`;
}

function trim(x: number): string {
  if (Number.isInteger(x)) return String(x);
  return x >= 10 ? x.toFixed(0) : x.toFixed(1);
}

function languageLabel(l: Language): string {
  return {
    sql: 'SQL', scala: 'Scala',
    'python-sql': 'PySpark', 'python-udf': 'PySpark with Python UDFs',
  }[l];
}

export { EXPANSION, MB_PER_CORE_SECOND, GB_PER_CORE, FORMAT_READ_COST };
