/**
 * The platform abstraction.
 *
 * Key decision: an adapter does NOT run the pipeline. It supplies policy and
 * capacity facts, and hooks the shared pipeline at three points. That keeps the
 * derivation trace identical in shape across platforms -- so a user can compare
 * a Databricks and a Kubernetes recommendation step by step -- while letting
 * the platforms disagree completely about what a valid config looks like.
 */

import type { Bytes, Cores } from '../units';
import type { Confidence } from '../constants/heuristics';
import type { PlatformId, WorkloadInput } from './input';
import type { Step, StepId } from './steps';
import type { Rule } from './rules';

export type NodeFamily =
  | 'general' | 'compute-optimized' | 'memory-optimized'
  | 'storage-optimized' | 'gpu';

export interface NodeType {
  id: string;
  displayName: string;
  family: NodeFamily;
  vcpus: Cores;
  /** Advertised capacity. Allocatable is always strictly less. */
  memoryBytes: Bytes;
  localSsdBytes?: Bytes;
  /** Multiple devices allow parallel spill via a multi-dir spark.local.dir. */
  localSsdCount?: number;
  networkGbps?: number;
  gpuCount?: number;
  /** Non-Spark reservation: kubelet, DaemonSets, Databricks agent, YARN NM. */
  reserve: { cpu: Cores; memory: Bytes; confidence: Confidence };
  notes?: string[];
}

/**
 * How the platform decides cores per executor. A policy rather than a constant,
 * because Databricks (one whole-node executor) and Kubernetes (4-5 cores) are
 * both correct on their own platforms.
 */
export type CoresPerExecutorPolicy =
  | { kind: 'four-to-five'; min: Cores; max: Cores; reason: string }
  | { kind: 'whole-node'; reason: string }
  | { kind: 'single-jvm'; reason: string }
  | { kind: 'user-pinned'; cores: Cores; reason: string };

export interface PlatformCapabilities {
  supportsMultipleExecutorsPerNode: boolean;
  supportsExternalShuffleService: boolean;
  supportsDynamicAllocation: boolean;
  /** True on K8s: no external shuffle service exists upstream. */
  dynamicAllocationRequiresShuffleTracking: boolean;
  supportsOffHeap: boolean;
  hasSeparateDriverNode: boolean;
  /** K8s splits spark.executor.cores from pod CPU request and limit. */
  hasCpuRequestLimitSplit: boolean;
  userChooses: 'workers' | 'executors' | 'nothing';
}

export interface AllocatableCapacity {
  cpu: Cores;
  memory: Bytes;
  ephemeralStorage?: Bytes;
  /** Shows the subtraction, e.g. "64 GiB - 4.6 GiB kubelet - 2 GiB DaemonSets". */
  derivation: Step;
}

/** Everything the pipeline accumulates before emitting a SizingResult. */
export interface DraftShape {
  nodeType?: NodeType;
  allocatable?: AllocatableCapacity;
  coresPerExecutor?: Cores;
  executorsPerNode?: number;
  executorCount?: number;
  nodeCount?: number;
  heapBytes?: Bytes;
  overheadBytes?: Bytes;
  offHeapBytes?: Bytes;
  pysparkMemoryBytes?: Bytes;
  containerTotalBytes?: Bytes;
  /** Free-form slot for profile stages to pass values down the pipeline. */
  scratch: Record<string, unknown>;
}

export interface ContainerFootprint {
  total: Bytes;
  components: Array<{ name: string; bytes: Bytes; configKey?: string }>;
}

export interface ConfigEntry {
  key: string;
  value: string;
  /** Links the value back to the derivation step that produced it. */
  rationaleStepId?: StepId;
  confidence: Confidence;
  sparkDefault?: string;
  /** Set when the platform ignores this key, e.g. executor.memory in local mode. */
  ignored?: boolean;
  note?: string;
}

export type PlatformConfigOutput =
  | {
      kind: 'databricks';
      clusterJson: Record<string, unknown>;
      sparkConf: ConfigEntry[];
      nodeTypeId: string;
      driverNodeTypeId: string;
      workers: number | { min: number; max: number };
    }
  | {
      kind: 'kubernetes';
      sparkSubmitArgs: string[];
      sparkApplicationYaml: string;
      sparkConf: ConfigEntry[];
    }
  | {
      kind: 'local';
      sparkSubmitArgs: string[];
      sparkConf: ConfigEntry[];
      envVars: Record<string, string>;
    }
  | {
      kind: 'yarn';
      sparkSubmitArgs: string[];
      sparkConf: ConfigEntry[];
    };

/** Forward-declared to avoid a cycle; the real shape lives in pipeline/index.ts. */
export interface PipelineContextLike {
  input: WorkloadInput;
  draft: DraftShape;
}

export interface PlatformAdapter {
  readonly id: PlatformId;
  readonly displayName: string;
  readonly status: 'supported' | 'stub';
  readonly capabilities: PlatformCapabilities;

  listNodeTypes(input: WorkloadInput): NodeType[];

  /** The critical divergence: what Spark may actually use on this node. */
  allocatable(node: NodeType, input: WorkloadInput): AllocatableCapacity;

  coresPerExecutorPolicy(input: WorkloadInput, node: NodeType): CoresPerExecutorPolicy;

  /** Hook 1: constrain the shared shape before executor packing. */
  constrainShape?(draft: DraftShape, ctx: PipelineContextLike): DraftShape;

  /** Hook 2: what the scheduler must grant beyond heap + overhead. */
  containerFootprint(
    heap: Bytes,
    overhead: Bytes,
    ctx: PipelineContextLike,
  ): ContainerFootprint;

  /** Hook 3: emit native config artifacts. */
  emitConfig(result: EmitInput, ctx: PipelineContextLike): PlatformConfigOutput;

  rules(): Rule[];
}

/** The subset of SizingResult an adapter needs, avoiding an import cycle. */
export interface EmitInput {
  nodeType?: NodeType;
  nodeCount: number;
  nodeCountRange?: { min: number; max: number };
  executorCount: number;
  executorCountRange?: { min: number; max: number };
  coresPerExecutor: Cores;
  heapBytes: Bytes;
  overheadBytes: Bytes;
  offHeapBytes: Bytes;
  pysparkMemoryBytes: Bytes;
  containerTotalBytes: Bytes;
  driverCores: Cores;
  driverHeapBytes: Bytes;
  driverOverheadBytes: Bytes;
  driverMaxResultSize: Bytes;
  shufflePartitions: number;
  aqeAdvisoryPartitionBytes: Bytes;
  maxPartitionBytes: Bytes;
  memoryFraction: number;
  storageFraction: number;
}
