/**
 * What the engine produces: a sized cluster, the reasoning behind it, and the
 * arguments against it.
 */

import type { Bytes, Cores, Seconds } from '../units';
import type { Range } from '../range';
import type { Confidence } from '../constants/heuristics';
import type { Step, InputError } from './steps';
import type { PlatformId, WorkloadInput } from './input';
import type { NodeType, PlatformConfigOutput } from './platform';
import type { Finding } from './rules';

/**
 * The node -> executor -> heap -> unified-memory breakdown. This is the
 * headline visual: it makes concrete that an "18g executor" does not have 18 GB
 * available for your data.
 */
export interface UnifiedMemoryBreakdown {
  heap: Bytes;
  /** Hardcoded 300 MiB, off the top, before any fraction applies. */
  reserved: Bytes;
  usableForUnified: Bytes;
  /** On-heap unified pool: (heap - 300 MiB) * memory.fraction. */
  unifiedOnHeap: Bytes;
  /** Unified pool including off-heap, when enabled. */
  unifiedTotal: Bytes;
  /**
   * Eviction-immunity floor for cached blocks -- NOT a cap on cache and NOT
   * half the pool reserved for storage. Execution may borrow everything above
   * this line; storage may never evict execution.
   */
  storageFloor: Bytes;
  executionFloor: Bytes;
  /** (heap - 300 MiB) * (1 - memory.fraction): UDF objects, Spark metadata. */
  userMemory: Bytes;
  memoryFraction: number;
  storageFraction: number;
  /**
   * unifiedTotal / coresPerExecutor -- the optimistic per-task ceiling, and the
   * number that predicts spill.
   */
  perTaskExecutionAtFullParallelism: Bytes;
}

export interface ExecutorSpec {
  count: number;
  /** Present when dynamic allocation or autoscaling is recommended. */
  countRange?: { min: number; max: number };
  coresPerExecutor: Cores;
  /** spark.executor.memory */
  heapBytes: Bytes;
  /** spark.executor.memoryOverhead */
  overheadBytes: Bytes;
  overheadFactor: number;
  /** spark.memory.offHeap.size; NOT part of overhead. */
  offHeapBytes: Bytes;
  /** spark.executor.pyspark.memory */
  pysparkMemoryBytes: Bytes;
  /** What the scheduler must actually grant: the sum of all of the above. */
  containerTotalBytes: Bytes;
  memoryBreakdown: UnifiedMemoryBreakdown;
}

export interface DriverSpec {
  cores: Cores;
  heapBytes: Bytes;
  overheadBytes: Bytes;
  maxResultSizeBytes: Bytes;
  containerTotalBytes: Bytes;
  onSeparateNode: boolean;
  nodeTypeId?: string;
}

export interface ParallelismSpec {
  totalCores: Cores;
  totalTaskSlots: number;
  inputPartitions: number;
  /** spark.sql.files.maxPartitionBytes; measured against on-disk bytes. */
  targetInputPartitionBytes: Bytes;
  /** spark.sql.shuffle.partitions -- an upper bound when AQE is on. */
  shufflePartitions: number;
  /** spark.sql.adaptive.advisoryPartitionSizeInBytes -- the real knob with AQE. */
  aqeAdvisoryPartitionBytes: Bytes;
  expectedPostAqePartitions?: Range;
  wavesPerStage: Range;
  defaultParallelism: number;
}

export interface StorageSpec {
  shuffleWriteBytesPerStage: Range;
  peakShuffleOnDiskBytes: Range;
  spillEstimateBytes: Range;
  spillPredicted: boolean;
  /** Ratio of per-task demand to per-task execution memory; >1 means spill. */
  spillPressureRatio: Range;
  requiredLocalDiskPerExecutorBytes: Bytes;
  availableLocalDiskPerExecutorBytes?: Bytes;
  hasEnoughLocalDisk: boolean;
  localDirStrategy: string;
}

export interface StreamingSpec {
  bytesPerMicroBatch: Range;
  peakBytesPerMicroBatch: Range;
  estimatedBatchProcessingSeconds: Range;
  /** triggerInterval / processingTime. Below 1 means an unbounded queue. */
  headroomRatio: Range;
  stable: boolean;
  maxOffsetsPerTrigger?: number;
  stateStoreBytesTotal: Range;
  stateStoreBytesPerExecutor: Range;
  recommendedStateProvider: 'hdfs' | 'rocksdb';
  /** Hard ceiling on read parallelism, from source partition count. */
  readParallelismCeiling: number;
  checkpointAdvice: string;
}

export interface UtilizationSpec {
  cpuPackingEfficiency: number;
  memPackingEfficiency: number;
  strandedCpuPerNode: Cores;
  strandedMemoryPerNode: Bytes;
  /** Which dimension limited executors per node. Drives instance-family advice. */
  bindingDimension: 'cpu' | 'memory';
}

export interface RuntimeEstimate {
  /** Always a range. The least reliable number the engine produces. */
  seconds: Range;
  confidence: Confidence;
  disclaimer: string;
}

export interface SizingResult {
  input: WorkloadInput;
  platform: PlatformId;
  nodeType?: NodeType;
  nodeCount: number;
  nodeCountRange?: { min: number; max: number };
  executor: ExecutorSpec;
  driver: DriverSpec;
  parallelism: ParallelismSpec;
  memory: {
    inflation: InflationBreakdown;
    inflatedScannedBytes: Range;
    workingSetBytes: Range;
    cacheDemandBytes: Range;
    totalClusterMemory: Bytes;
  };
  storage: StorageSpec;
  streaming?: StreamingSpec;
  utilization: UtilizationSpec;
  runtime?: RuntimeEstimate;
  trace: Step[];
  config: PlatformConfigOutput;
}

/** The four-factor decomposition; never collapsed to a single number. */
export interface InflationBreakdown {
  codecFactor: Range;
  encodingFactor: Range;
  representationFactor: Range;
  queryAmplification: Range;
  total: Range;
  confidence: Confidence;
  /** What the user could tell us to narrow this. */
  tighteningHint?: string;
}

export type VerdictLevel = 'dont-use-spark' | 'ok' | 'risky' | 'will-fail';

export interface Verdict {
  level: VerdictLevel;
  headline: string;
  reasoning: string[];
}

export interface AlternativeSizing {
  label: string;
  rationale: string;
  result: SizingResult;
}

export interface Recommendation {
  primary: SizingResult;
  alternatives: AlternativeSizing[];
  /** Ranked, with suppressed findings already removed. */
  findings: Finding[];
  verdict: Verdict;
  errors: InputError[];
  engineVersion: string;
  /** Injected, never read from a clock -- the engine must stay deterministic. */
  computedAtMs: number;
}

/** What to check in the Spark UI after run one, and which input to correct. */
export interface MeasurementLoopItem {
  observe: string;
  where: string;
  ifWrong: string;
  correctsInput: string;
}

export type { Seconds };
