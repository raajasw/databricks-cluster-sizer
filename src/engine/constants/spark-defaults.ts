/**
 * Spark's own defaults, keyed by the config name users will recognize.
 *
 * Everything here is `documented` confidence: it comes from Spark source or the
 * official configuration docs, not from heuristics. When the engine departs
 * from one of these, the derivation step must say so and why -- "we set X
 * instead of the default Y because Z" is the product.
 *
 * Sources: spark/core/src/main/scala/org/apache/spark/internal/config/package.scala,
 * spark/sql/catalyst/.../SQLConf.scala, and spark.apache.org/docs/latest/configuration.html
 */

import { mib, gib, bytes, type Bytes } from '../units';

/** Spark versions the engine knows how to target. */
export type SparkVersion = '3.3' | '3.4' | '3.5' | '4.0';

export const SUPPORTED_SPARK_VERSIONS: SparkVersion[] = ['3.3', '3.4', '3.5', '4.0'];

export const DEFAULT_SPARK_VERSION: SparkVersion = '3.5';

// --- memory manager ---------------------------------------------------------

/**
 * UnifiedMemoryManager.RESERVED_SYSTEM_MEMORY_BYTES. Hardcoded 300 MiB, carved
 * off the heap before spark.memory.fraction applies. Overridable only by the
 * test-only spark.testing.reservedMemory.
 */
export const RESERVED_SYSTEM_MEMORY: Bytes = mib(300);

/**
 * Spark refuses to start below 1.5x the reserved memory, failing with
 * "System memory N must be at least 471859200".
 */
export const MIN_EXECUTOR_HEAP: Bytes = bytes(Math.floor(RESERVED_SYSTEM_MEMORY * 1.5));

/**
 * spark.memory.fraction -- share of (heap - 300 MiB) available to the unified
 * execution+storage pool. The remaining 40% is "user memory": UDF objects,
 * user data structures, and Spark internal metadata.
 */
export const MEMORY_FRACTION = 0.6;

/**
 * spark.memory.storageFraction -- the eviction-immunity FLOOR for cached
 * blocks, not a partition. Execution may borrow all of storage and evict cached
 * blocks down to this floor; storage can never evict execution. Describing this
 * as "half the pool is for cache" is the single most common misreading.
 */
export const STORAGE_FRACTION = 0.5;

/** spark.memory.offHeap.enabled / .size */
export const OFF_HEAP_ENABLED_DEFAULT = false;
export const OFF_HEAP_SIZE_DEFAULT: Bytes = bytes(0);

// --- executor / container ---------------------------------------------------

/** spark.executor.memory */
export const EXECUTOR_MEMORY_DEFAULT: Bytes = gib(1);

/** spark.executor.cores -- 1 in standalone/K8s; all cores on the worker in YARN. */
export const EXECUTOR_CORES_DEFAULT = 1;

/** spark.executor.memoryOverhead floor: max(384 MiB, factor * heap). */
export const MIN_MEMORY_OVERHEAD: Bytes = mib(384);

/**
 * spark.executor.memoryOverheadFactor, added in Spark 3.3.
 *
 * 0.10 for JVM workloads, but 0.40 when the job is non-JVM (PySpark/SparkR),
 * because Python worker processes live entirely outside the JVM heap. Defaulting
 * PySpark to 0.10 systematically undersizes containers and produces K8s
 * OOMKills (exit 137) that look like random executor loss.
 */
export const MEMORY_OVERHEAD_FACTOR_JVM = 0.1;
export const MEMORY_OVERHEAD_FACTOR_NON_JVM = 0.4;

// --- driver -----------------------------------------------------------------

export const DRIVER_MEMORY_DEFAULT: Bytes = gib(1);
export const DRIVER_CORES_DEFAULT = 1;

/**
 * spark.driver.maxResultSize. Setting this to 0 means UNLIMITED, which removes
 * the only guardrail preventing a collect() from OOMing the driver. Never
 * recommend 0.
 */
export const DRIVER_MAX_RESULT_SIZE_DEFAULT: Bytes = gib(1);

// --- SQL / partitioning -----------------------------------------------------

/**
 * spark.sql.shuffle.partitions. The notorious 200. Correct only by coincidence:
 * it predates AQE and has no relationship to your data size or cluster.
 */
export const SHUFFLE_PARTITIONS_DEFAULT = 200;

/**
 * spark.sql.files.maxPartitionBytes -- measured against ON-DISK file bytes, not
 * in-memory size.
 */
export const MAX_PARTITION_BYTES_DEFAULT: Bytes = mib(128);

/**
 * spark.sql.files.openCostInBytes -- the cost of opening a file, in "bytes" for
 * split-packing purposes. Each file is treated as being at least this large,
 * which is why 100k tiny files produce far more partitions than
 * (totalBytes / maxPartitionBytes) predicts.
 */
export const OPEN_COST_IN_BYTES_DEFAULT: Bytes = mib(4);

/** spark.sql.autoBroadcastJoinThreshold. -1 disables broadcast joins. */
export const AUTO_BROADCAST_JOIN_THRESHOLD_DEFAULT: Bytes = mib(10);

// --- adaptive query execution ----------------------------------------------

/** spark.sql.adaptive.enabled -- ON by default since Spark 3.2. */
export const AQE_ENABLED_DEFAULT = true;

/**
 * spark.sql.adaptive.advisoryPartitionSizeInBytes. With AQE on, THIS is the
 * real partition-sizing knob; spark.sql.shuffle.partitions becomes an upper
 * bound that AQE coalesces down from.
 */
export const AQE_ADVISORY_PARTITION_SIZE_DEFAULT: Bytes = mib(64);

/** spark.sql.adaptive.coalescePartitions.minPartitionSize */
export const AQE_MIN_PARTITION_SIZE_DEFAULT: Bytes = mib(1);

/** spark.sql.adaptive.skewJoin.skewedPartitionFactor */
export const AQE_SKEW_PARTITION_FACTOR_DEFAULT = 5.0;

/** spark.sql.adaptive.skewJoin.skewedPartitionThresholdInBytes */
export const AQE_SKEW_THRESHOLD_DEFAULT: Bytes = mib(256);

// --- shuffle ----------------------------------------------------------------

export const SHUFFLE_COMPRESS_DEFAULT = true;
/** spark.shuffle.file.buffer */
export const SHUFFLE_FILE_BUFFER_DEFAULT: Bytes = bytes(32 * 1024);

// --- dynamic allocation -----------------------------------------------------

export const DYNAMIC_ALLOCATION_ENABLED_DEFAULT = false;
export const DA_EXECUTOR_IDLE_TIMEOUT_SECONDS = 60;
export const DA_SCHEDULER_BACKLOG_TIMEOUT_SECONDS = 1;

// --- structured streaming ---------------------------------------------------

/**
 * spark.sql.streaming.minBatchesToRetain. With the default HDFS-backed state
 * store this many state VERSIONS are kept in memory, multiplying heap-resident
 * state well beyond its logical size.
 */
export const MIN_BATCHES_TO_RETAIN_DEFAULT = 100;

// --- python / arrow ---------------------------------------------------------

/**
 * spark.sql.execution.arrow.maxRecordsPerBatch. Peak Python-side memory is
 * roughly maxRecordsPerBatch * bytesPerRow * ~3 (Arrow buffer + pandas copy +
 * result), so wide feature vectors at the default batch size are a leading
 * cause of PySpark worker OOM.
 */
export const ARROW_MAX_RECORDS_PER_BATCH_DEFAULT = 10000;

// --- JVM --------------------------------------------------------------------

/**
 * Compressed ordinary object pointers stop working past roughly 32 GiB of heap
 * (exactly, when the heap can no longer be addressed by a 32-bit offset at the
 * default 8-byte object alignment). Beyond it every reference widens from 4 to
 * 8 bytes, so a 40 GiB heap can hold LESS live data than a 31 GiB one.
 */
export const COMPRESSED_OOPS_LIMIT: Bytes = gib(32);

/** Below this the compressed-oops loss is definitely not yet in play. */
export const COMPRESSED_OOPS_SAFE_HEAP: Bytes = gib(31);

/**
 * Above roughly this, the larger heap has recouped the ~20% loss from widened
 * references. Between SAFE and here is the dead zone worth warning about.
 */
export const COMPRESSED_OOPS_RECOVERY_HEAP: Bytes = gib(48);

/** Per-thread JVM stack, one per task slot -- part of what memoryOverhead covers. */
export const JVM_THREAD_STACK_SIZE: Bytes = mib(1);
