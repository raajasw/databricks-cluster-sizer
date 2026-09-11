/**
 * Everything the questionnaire collects.
 *
 * Design rule: every field here must change a number in the output. If adding a
 * question does not move the recommendation, it does not belong in the form.
 */

import type { Bytes, Cores, Seconds } from '../units';
import type { SparkVersion } from '../constants/spark-defaults';

export type WorkloadProfile = 'batch-etl' | 'streaming' | 'interactive-sql' | 'ml-feature';

export type PlatformId = 'databricks' | 'kubernetes' | 'yarn' | 'local' | 'emr';

export type Environment = 'dev' | 'staging' | 'prod';

export type StorageFormat =
  | 'parquet' | 'delta' | 'iceberg' | 'orc'
  | 'avro' | 'json' | 'csv' | 'text';

export type Codec = 'snappy' | 'zstd' | 'gzip' | 'lz4' | 'none' | 'unknown';

/**
 * Drives the memoryOverheadFactor branch (0.10 JVM vs 0.40 non-JVM) and the
 * representation inflation factor. One of the highest-leverage inputs.
 */
export type RuntimeLanguage =
  | 'scala-java'
  | 'pyspark-sql-only'
  | 'pyspark-udf'
  | 'pyspark-pandas-udf'
  | 'sparkr';

export type QueryShape =
  | 'scan-filter-write'
  | 'narrow-transform'
  | 'aggregation'
  | 'sort'
  | 'shuffle-join'
  | 'broadcast-join'
  | 'window'
  | 'iterative-ml'
  | 'multi-stage-dag';

/** Relative weights; normalized by the engine, so they need not sum to 1. */
export interface ColumnMix {
  numeric: number;
  lowCardString: number;
  highCardString: number;
  nested: number;
}

export interface DataShape {
  /**
   * On-disk size of the primary input for ONE RUN -- compressed and encoded as
   * stored. Per-run, not per-day: an hourly job described with a daily figure
   * is sized 24x wrong, and that confusion is the most common sizing error
   * there is.
   */
  logicalBytesOnDisk: Bytes;
  format: StorageFormat;
  codec: Codec;

  approxRowCount?: number;
  approxColumnCount?: number;

  /** Share of columns actually read. Only helps on columnar formats. */
  columnsReadFraction?: number;
  /** Share of rows surviving partition pruning and row-group skipping. */
  rowsSurvivingFilterFraction?: number;

  /** Optional, but narrows the inflation estimate by roughly 3x. */
  columnMix?: ColumnMix;

  /** Physical file count; drives small-file and openCost effects. */
  fileCount?: number;
}

export interface PipelineShape {
  dominantQueryShape: QueryShape;
  /** Roughly the number of shuffle boundaries. */
  shuffleStages: number;
  largestJoinBuildSideBytes?: Bytes;
  knownSkew: 'none' | 'suspected' | 'severe';
  /** Share of the working set deliberately cached for reuse. */
  cacheWorkingSetFraction: number;
  /** Whether caching goes through the DataFrame API (compressed columnar). */
  cachesViaDataFrameApi: boolean;
  /** Output/input size ratio: joins fan out, aggregations collapse. */
  outputToInputRatio?: number;
  /** Result size returned to the driver via collect()/toPandas(). */
  driverCollectBytes?: Bytes;
}

export interface SlaTarget {
  kind: 'best-effort' | 'deadline' | 'latency';
  /** Wall-clock budget for a batch run. */
  targetRuntime?: Seconds;
}

export type StatefulOp =
  | 'none' | 'dedup' | 'window-agg' | 'stream-stream-join' | 'flatMapGroupsWithState';

export interface StreamingParams {
  sourceType: 'kafka' | 'kinesis' | 'eventhubs' | 'autoloader-files' | 'delta-cdf' | 'socket';
  eventsPerSecond: number;
  avgEventBytes: Bytes;
  /** Burst multiplier over steady state. Stability must hold at PEAK. */
  peakToAverageRatio: number;
  triggerMode: 'processing-time' | 'available-now' | 'continuous' | 'default';
  triggerIntervalSeconds?: Seconds;
  /**
   * Source partition count. For Kafka/Kinesis this is a HARD CEILING on read
   * parallelism -- cores beyond it do nothing on the read stage.
   */
  sourcePartitions: number;
  statefulOps: StatefulOp[];
  watermarkSeconds?: Seconds;
  stateKeyCount?: number;
  avgStateValueBytes?: Bytes;
  /** HDFS-backed state is heap-resident; RocksDB is native and off-heap. */
  stateStoreProvider: 'hdfs' | 'rocksdb';
  outputMode: 'append' | 'update' | 'complete';
  /** Whether a durable checkpoint location is configured. */
  checkpointDurable: boolean;
  /** maxOffsetsPerTrigger / maxFilesPerTrigger set? Guards restart stampedes. */
  hasSourceRateLimit: boolean;
}

export interface InteractiveParams {
  concurrentUsers: number;
  queriesPerUserPerHour: number;
  avgQueryDurationSeconds: Seconds;
  p95TargetSeconds: Seconds;
  scheduling: 'fifo' | 'fair';
  /** Hot dataset repeatedly scanned; drives cache sizing. */
  hotDatasetBytes?: Bytes;
}

export interface MlParams {
  library:
    | 'spark-mllib' | 'pandas-udf-arrow' | 'xgboost-spark'
    | 'torch-distributor' | 'sklearn-broadcast';
  arrowBatchRows?: number;
  /** Per-Python-worker RSS estimate; there are up to executorCores of them. */
  pythonWorkerMemoryBytes?: Bytes;
  cachesTrainingSet: boolean;
  featureCount?: number;
  broadcastModelBytes?: Bytes;
  usesGpu: boolean;
}

export interface ClusterPreferences {
  maxTotalCores?: Cores;
  maxTotalMemory?: Bytes;
  maxNodes?: number;
  pinnedNodeTypeId?: string;
  /** User override; the engine honours it but may warn. */
  pinnedExecutorCores?: Cores;
  pinnedShufflePartitions?: number;
  preferFewerLargerNodes: boolean;
  allowSpot: boolean;
}

// --- platform-specific inputs ----------------------------------------------

export interface DatabricksInput {
  runtimeVersion: string;
  photonEnabled: boolean;
  clusterKind: 'job' | 'all-purpose' | 'sql-warehouse';
  autoscale: boolean;
  minWorkers?: number;
  maxWorkers?: number;
  singleNode: boolean;
}

export interface KubernetesInput {
  /** Advertised node capacity; allocatable is derived from it. */
  nodeCapacityCpu?: Cores;
  nodeCapacityMemory?: Bytes;
  nodeEphemeralStorage?: Bytes;
  /** Preset formulas for kubelet/system reservations. */
  reservePreset: 'gke' | 'eks' | 'aks' | 'manual' | 'none';
  manualKubeReservedCpu?: Cores;
  manualKubeReservedMemory?: Bytes;
  /** CNI, log shippers, node exporters, service-mesh sidecars. */
  daemonsetCpu?: Cores;
  daemonsetMemory?: Bytes;
  dynamicAllocation: boolean;
  shuffleTrackingEnabled: boolean;
  /** tmpfs puts shuffle in RAM, counting against the pod memory limit. */
  shuffleStorage: 'emptydir' | 'pvc' | 'tmpfs';
  /** A CPU limit below executor cores causes CFS throttling. */
  setCpuLimit: boolean;
}

export interface LocalInput {
  machineCores: Cores;
  machineMemory: Bytes;
  freeDiskBytes?: Bytes;
}

export interface YarnInput {
  nodeManagerMemory?: Bytes;
  nodeManagerCores?: Cores;
  minAllocationMemory?: Bytes;
  incrementAllocationMemory?: Bytes;
}

export interface PlatformInput {
  platform: PlatformId;
  databricks?: DatabricksInput;
  kubernetes?: KubernetesInput;
  local?: LocalInput;
  yarn?: YarnInput;
}

/**
 * Profile params are optional SIBLINGS rather than a discriminated union on
 * `profile`. A tagged union types more precisely but discards the user's
 * answers whenever they toggle profile in the form; validateInput checks the
 * required bag is present instead.
 */
export interface WorkloadInput {
  profile: WorkloadProfile;
  environment: Environment;
  runtimeLanguage: RuntimeLanguage;
  sparkVersion: SparkVersion;
  aqeEnabled: boolean;

  data: DataShape;
  pipeline: PipelineShape;
  sla: SlaTarget;

  streaming?: StreamingParams;
  interactive?: InteractiveParams;
  ml?: MlParams;

  cluster: ClusterPreferences;
  platformInput: PlatformInput;
}
