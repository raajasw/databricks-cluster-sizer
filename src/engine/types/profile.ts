/**
 * Workload profile strategies.
 *
 * Layered deliberately: a profile that only needs different constants (ML
 * lowering memory.fraction) overrides `tunables` and nothing else, while one
 * that needs entirely different demand math (streaming sizing from rate rather
 * than volume) replaces `demandModel` -- without either forking the pipeline.
 */

import type { Bytes } from '../units';
import type { Range } from '../range';
import type { WorkloadProfile, WorkloadInput } from './input';
import type { Rule } from './rules';

export interface Tunables {
  memoryFraction: number;
  storageFraction: number;
  targetInputPartitionBytes: Bytes;
  targetShufflePartitionBytes: Bytes;
  advisoryPartitionBytes: Bytes;
  targetWavesMin: number;
  targetWavesMax: number;
  coresPerExecutorMin: number;
  coresPerExecutorMax: number;
  /** Multiplier on computed capacity, for burst headroom. */
  headroomFactor: number;
  driverMemoryFloor: Bytes;
  maxRecommendedHeap?: Bytes;
  overheadFactorOverride?: number;
}

/** How many cores the workload needs, and why. */
export interface DemandEstimate {
  totalCores: number;
  /** Set when demand is rate-driven rather than volume-driven. */
  basis: 'sla-deadline' | 'wave-target' | 'streaming-rate' | 'concurrency' | 'fixed-budget';
  explanation: string;
  runtimeSeconds?: Range;
}

export interface ProfileStrategy {
  readonly id: WorkloadProfile;
  readonly displayName: string;
  tunables(input: WorkloadInput): Partial<Tunables>;
  rules(): Rule[];
  /** Replaces volume-based core sizing entirely when present. */
  demandModel?: string;
}
