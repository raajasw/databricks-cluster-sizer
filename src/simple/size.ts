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
  /**
   * Whether Photon is enabled on the cluster. Default true: it is on by
   * default for SQL warehouses and is the common choice for new job clusters.
   * A Python UDF forces the JVM path regardless of this setting.
   */
  photon?: boolean;
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
 * CALIBRATION - see README for the full working.
 *
 * The TPC-DS 100 TB Full Disclosure Report (Databricks SQL 8.3, audited by the
 * TPC council, Nov 2021) loaded 100 TB on 256 x i3.2xlarge (2,048 vCPU) in
 * 7,929 seconds. That is 6.2 MB per core-second.
 *
 * Two things about that anchor have to be stated plainly, because both were
 * previously got wrong here:
 *
 *   1. It ALREADY INCLUDES PHOTON. The FDR names the system under test as
 *      "Databricks Photon Engine 8.3". So 6.2 is a Photon number, and the
 *      Photon adjustment below makes the non-Photon case SLOWER rather than
 *      making the Photon case faster. An earlier version of this comment read
 *      as though Photon were a speedup still to be applied on top; applying it
 *      that way would double-count.
 *
 *   2. It EXPIRED on 2024-11-01 and is now listed by the TPC as a historical
 *      result. Databricks has not resubmitted, and no newer audited Databricks
 *      figure exists at any scale. It is kept as the anchor anyway, because an
 *      expired audit is still independently verified, which no vendor blog
 *      post or customer anecdote is. But it is four runtime generations old
 *      (DBR 8.3 against today's 17.x), so it is an old number, honestly
 *      labelled, not a current one.
 *
 * The hardware half of that staleness is handled separately and better: the
 * FDR names the i3's processor as a Xeon E5-2686 v4, a 2016 Broadwell part, so
 * this rate means "per 2016-Broadwell core" and each node's cpuFactor scales it
 * to the silicon actually chosen. See CPU_FACTOR in nodes.ts.
 *
 * These base rates sit near the anchor, deliberately toward the pessimistic
 * end. Under-provisioning surfaces as an out-of-memory failure three hours into
 * a production run; over-provisioning costs some money. Those are not
 * symmetric, so the bias is intentional. A bulk load is also lighter work than
 * a shuffling query, which is the other reason not to round the anchor up.
 *
 * An earlier version of this file used 40 here, calibrated against a
 * half-remembered figure rather than a published one. That was roughly 6-25x
 * too fast and produced clusters that would have missed their targets badly.
 * The lesson held to since: every constant names its source, or it does not
 * belong in this file.
 *
 * The Python UDF figure is low for a real reason: every row is serialised out
 * of the JVM, sent to a Python process and sent back. That round trip dominates
 * everything else the job does. Published benchmarks put row-at-a-time UDFs at
 * 10-20x slower than native functions; the ratio here is at the conservative
 * end of that range.
 */
const MB_PER_CORE_SECOND: Record<Language, number> = {
  sql: 6,
  scala: 6,
  'python-sql': 5.5,
  'python-udf': 0.7,
};

/**
 * How much slower the same work runs without Photon.
 *
 * The anchor above is a Photon measurement, so this is a penalty applied when
 * Photon is off - not a bonus when it is on.
 *
 * The number is 2, not the 4 this tool used to imply. The Photon paper's "4x"
 * is an AVERAGE ACROSS QUERIES with a stated maximum of 23x, which is a
 * different claim from "your job will be 4x slower without it". Measurements
 * that compare like with like on the same instances:
 *
 *   Databricks' own TPC-DS 1 TB figures      ~2x
 *   Independent testing, identical instances ~2x
 *   Intel co-marketing, 1 TB / 10 TB         3.4x / 6.7x (tuned, vendor)
 *   Customer-reported real workloads         3-8x (self-selected, unaudited)
 *
 * The independent ~2x is the one to build on; the larger figures come from
 * tuned or self-selected configurations. Choosing 2 also fails safe: if Photon
 * does better than that on your job, the cluster is merely larger than it
 * needed to be.
 *
 * Photon does not cover every operator. Unsupported ones fall back to the JVM
 * engine mid-query, so a Photon cluster does not get the full benefit on every
 * stage - another reason the conservative end is the right end.
 */
const NON_PHOTON_SLOWDOWN = 2;

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

/**
 * Which node shape suits the work.
 *
 * Follows Databricks' own guidance: for anything that shuffles - joins, sorts,
 * aggregations - they recommend storage-optimised nodes with local disk and
 * disk caching enabled. Shuffle is disk- and network-bound more than it is
 * memory-bound, because every shuffled row is written to local disk, sent
 * across the network and read back on the other side.
 *
 * Memory-optimised is the fallback for shuffle-heavy work on clouds with no
 * storage tier in the catalogue, since a join still needs room for its build
 * side.
 */
export function pickShape(a: Answers, available: NodeShape[]): { shape: NodeShape; reason: string } {
  const has = (s: NodeShape) => available.includes(s);
  const shuffles = a.operation !== 'scan';

  if (shuffles && has('storage')) {
    return {
      shape: 'storage',
      reason: `a ${a.operation} shuffles, and shuffle traffic goes to local disk before it ` +
        'goes anywhere else',
    };
  }
  if (shuffles && has('memory')) {
    return {
      shape: 'memory',
      reason: `a ${a.operation} shuffles and holds a lot at once; this cloud has no ` +
        'storage-optimised tier here, so more RAM per core is the next best thing',
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
  //
  // Photon: the anchor rate is a Photon measurement, so turning Photon off
  // costs you. A Python UDF is already priced at the JVM-plus-serialisation
  // rate and cannot use Photon for the UDF itself, so applying the penalty
  // again there would charge twice for the same thing.
  const photonOn = a.photon ?? true;
  const paysPhotonPenalty = !photonOn && a.language !== 'python-udf';

  const mbPerCoreSec =
    MB_PER_CORE_SECOND[a.language] /
    OP_SLOWDOWN[a.operation] /
    FORMAT_READ_COST[a.format] /
    (paysPhotonPenalty ? NON_PHOTON_SLOWDOWN : 1);
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

  // The core count above is in anchor-cores: the 2016 Broadwell core the
  // throughput figure was measured on. A modern core does more per second, so
  // one of these nodes' cores is worth cpuFactor of them.
  const effectiveCoreWork = effectiveCores * node.cpuFactor;

  const workers = Math.max(1, Math.ceil(coresNeeded / effectiveCoreWork));
  const totalCores = workers * effectiveCores;

  // 5. What that actually buys you. Measured in anchor-core work, so the
  // faster silicon shows up in the runtime rather than being quietly dropped.
  const totalCoreWork = workers * effectiveCoreWork;
  const estimatedSeconds = coreSeconds / totalCoreWork;
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

  if (node.cpuFactor !== 1) {
    why.push(
      `${node.id} runs on ${node.cpu}, which the vendors' own figures put at about ` +
      `${node.cpuFactor}× the per-core throughput of the 2016 Broadwell part the ` +
      'calibration was measured on, so fewer of its cores are needed.',
    );
  }

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
    if (photonOn) {
      notes.push(
        'Photon cannot run a Python UDF, so the rows in that UDF fall back to the JVM engine ' +
        'whatever this cluster is set to. You are paying the Photon premium for the stages ' +
        'around it. Removing the UDF is what makes Photon worth buying.',
      );
    }
  } else if (!photonOn) {
    notes.push(
      `Photon is off, so this cluster is sized about ${NON_PHOTON_SLOWDOWN}× larger than it ` +
      'would otherwise need to be. Photon costs more per hour but usually finishes enough ' +
      'sooner to be cheaper overall - worth measuring on one run before ruling it out.',
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

export { EXPANSION, MB_PER_CORE_SECOND, GB_PER_CORE, FORMAT_READ_COST, NON_PHOTON_SLOWDOWN };
