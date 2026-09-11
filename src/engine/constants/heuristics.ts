/**
 * Every magic number that is NOT a Spark default lives here, with its
 * provenance attached. Nothing in the pipeline may invent a constant inline.
 *
 * Confidence tiers, which the UI renders differently:
 *   documented -- from Spark source or docs; deterministic
 *   measured   -- from a vendor-published catalog or formula
 *   estimated  -- a defensible model with real variance
 *   guess      -- genuinely unknowable without running the job
 */

import { mib, gib, type Bytes } from '../units';
import { fromBounds, point, type Range } from '../range';
import type { QueryShape, RuntimeLanguage } from '../types/input';

export type Confidence = 'documented' | 'measured' | 'estimated' | 'guess';

// --- cores per executor -----------------------------------------------------

/**
 * The "4-5 cores per executor" rule.
 *
 * Provenance matters here because the usual justification is obsolete. The rule
 * comes from a 2015 Cloudera post observing that HDFS client write throughput
 * degrades past ~5 concurrent threads per process. On object storage (S3, ADLS,
 * GCS) that mechanism does not exist at all.
 *
 * What still justifies it in 2026:
 *   - GC pause scaling: one heap shared by N tasks, and pauses stop the world
 *     for all of them.
 *   - Blast radius: a single skewed task OOMing the executor kills the N-1
 *     innocent tasks sharing it.
 *
 * Databricks contradicts the rule outright -- one executor per worker using all
 * of the node's cores -- and works fine, which is why this is a per-platform
 * POLICY rather than a universal constant.
 */
export const CORES_PER_EXECUTOR_MIN = 4;
export const CORES_PER_EXECUTOR_MAX = 5;
/** Widened bounds when neither 4 nor 5 divides the node evenly. */
export const CORES_PER_EXECUTOR_WIDE_MIN = 3;
export const CORES_PER_EXECUTOR_WIDE_MAX = 6;
/** Past this, GC pauses and blast radius get flagged regardless of platform. */
export const CORES_PER_EXECUTOR_WARN_ABOVE = 8;

// --- partitioning -----------------------------------------------------------

/**
 * Target in-memory bytes per partition. The 128-200 MiB band is folklore with
 * good empirical support: small enough that a task's working set fits in its
 * share of execution memory, large enough that per-task scheduling overhead
 * (~1-10ms) stays negligible.
 */
export const TARGET_PARTITION_BYTES: Bytes = mib(128);
export const TARGET_PARTITION_BYTES_MIN: Bytes = mib(64);
export const TARGET_PARTITION_BYTES_MAX: Bytes = mib(200);

/**
 * Waves = partitions / task slots. Two to four gives the scheduler room to hide
 * stragglers without paying excessive task-launch overhead. Exactly one wave
 * means a single slow task sets the stage's duration.
 */
export const TARGET_WAVES_MIN = 2;
export const TARGET_WAVES_MAX = 4;
export const WAVES_WARN_ABOVE = 8;

/** Task count past which driver-side bookkeeping becomes a real memory cost. */
export const DRIVER_TASK_COUNT_PRESSURE_THRESHOLD = 500_000;

// --- throughput -------------------------------------------------------------

/**
 * Bytes processed per core-second. THE least reliable number in the tool.
 *
 * Real throughput depends on the query, the data, the storage backend, the
 * network and the JVM. The span below is wide on purpose; any runtime estimate
 * derived from it must be shown as a range with an explicit disclaimer, never
 * as "your job will take 23 minutes".
 */
export const BYTES_PER_CORE_SECOND_BY_SHAPE: Record<QueryShape, Range> = {
  'scan-filter-write': fromBounds(mib(40), mib(120)),
  'narrow-transform': fromBounds(mib(30), mib(100)),
  aggregation: fromBounds(mib(20), mib(70)),
  sort: fromBounds(mib(15), mib(50)),
  'shuffle-join': fromBounds(mib(12), mib(45)),
  'broadcast-join': fromBounds(mib(25), mib(80)),
  window: fromBounds(mib(15), mib(50)),
  'iterative-ml': fromBounds(mib(8), mib(35)),
  'multi-stage-dag': fromBounds(mib(10), mib(40)),
};

/** Language multiplier on throughput. Row-at-a-time Python UDFs are brutal. */
export const THROUGHPUT_LANGUAGE_FACTOR: Record<RuntimeLanguage, Range> = {
  'scala-java': point(1.0),
  'pyspark-sql-only': fromBounds(0.9, 1.0),
  'pyspark-udf': fromBounds(0.05, 0.25),
  'pyspark-pandas-udf': fromBounds(0.4, 0.8),
  sparkr: fromBounds(0.05, 0.25),
};

export const THROUGHPUT_CONFIDENCE: Confidence = 'guess';

// --- "do you even need Spark" ----------------------------------------------

/**
 * Below this working set, a single process running DuckDB or Polars generally
 * beats a Spark cluster on both latency and cost: no JVM warmup, no shuffle, no
 * scheduling. Saying so plainly is more useful than emitting a tidy 3-node
 * config.
 */
export const SINGLE_MACHINE_VIABLE_BYTES: Bytes = gib(50);
/** Below this it is not close. */
export const SPARK_CLEARLY_OVERKILL_BYTES: Bytes = gib(10);

/** Shuffle-heavy shapes tolerate more data single-node than their size suggests. */
export const SINGLE_NODE_TOLERANT_SHAPES: QueryShape[] = [
  'scan-filter-write',
  'narrow-transform',
];

// --- small files ------------------------------------------------------------

/** Below this average size, listing and open cost dominate actual reading. */
export const SMALL_FILE_THRESHOLD: Bytes = mib(16);
export const SMALL_FILE_COUNT_THRESHOLD = 10_000;

// --- memory -----------------------------------------------------------------

/** Executor heap ceiling before off-heap execution memory is worth recommending. */
export const OFF_HEAP_RECOMMENDED_ABOVE_HEAP: Bytes = gib(24);

/** Streaming wants small heaps: a 60s full GC on a 60s trigger is a missed batch. */
export const STREAMING_MAX_RECOMMENDED_HEAP: Bytes = gib(16);

/** Leave this share of a laptop for the OS, browser and IDE. */
export const LOCAL_MACHINE_RESERVE_FRACTION = 0.35;

/** Driver floor for anything non-trivial. */
export const DRIVER_MEMORY_FLOOR: Bytes = gib(4);
export const DRIVER_CORES_FLOOR = 2;

/** Broadcasting past this risks driver OOM and bloats every executor. */
export const BROADCAST_WARN_BYTES: Bytes = gib(1);

/** Safety multiplier on computed local disk need for shuffle and spill. */
export const SHUFFLE_DISK_SAFETY_FACTOR = 1.5;

// --- profile tunables -------------------------------------------------------

/**
 * ML lowers spark.memory.fraction. The reason is precise and worth stating:
 * Arrow buffers and user data structures live in USER memory, which is
 * (heap - 300 MiB) * (1 - memory.fraction). Lowering the fraction ENLARGES the
 * unmanaged region these need. This is not the same as spark.memory.offHeap.
 */
export const ML_MEMORY_FRACTION = 0.35;

/** Interactive clusters exist to serve a hot cached dataset. */
export const INTERACTIVE_STORAGE_FRACTION = 0.65;
/** Iterative training re-scans the same data; eviction means full recompute. */
export const ML_STORAGE_FRACTION_WHEN_CACHING = 0.6;

/**
 * Queueing headroom for interactive clusters. Waiting time scales as
 * 1/(1-rho), so a p95 latency target needs utilization near 0.7, not 0.95.
 */
export const INTERACTIVE_TARGET_UTILIZATION = 0.7;

/** Streaming stability: processing must finish well inside the trigger. */
export const STREAMING_TARGET_BATCH_TIME_FRACTION = 0.5;

/** RocksDB block cache and memtables are native memory outside the heap. */
export const ROCKSDB_OVERHEAD_PER_EXECUTOR: Bytes = mib(512);

/** HDFS-backed state past this per executor is a GC problem; move to RocksDB. */
export const HDFS_STATE_STORE_WARN_BYTES: Bytes = gib(1);

/** Peak Python memory per Arrow batch: Arrow buffer + pandas copy + result. */
export const ARROW_BATCH_MEMORY_MULTIPLIER = 3;

/** Default per-Python-worker RSS when the user has not measured it. */
export const DEFAULT_PYTHON_WORKER_MEMORY: Bytes = mib(512);

// --- bin packing ------------------------------------------------------------

/** Stranded capacity past this share is worth a warning with a concrete fix. */
export const BIN_PACK_WASTE_WARN_FRACTION = 0.2;

/** Node provisioning plus image pull, which dominates dynamic-allocation latency. */
export const K8S_POD_STARTUP_SECONDS = fromBounds(30, 120);

/**
 * Assumed batch window when the user gives no SLA.
 *
 * Cluster size is fundamentally a time decision, so something has to stand in
 * for "how long is acceptable". An hour is a common batch cadence and keeps the
 * recommendation in a sane range; the UI says plainly that this is the
 * assumption and that changing the target resizes the cluster.
 */
export const DEFAULT_BATCH_WINDOW_SECONDS = 3600;
