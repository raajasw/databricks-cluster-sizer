/**
 * The pipeline: an ordered array of pure stages. The array IS the algorithm
 * documentation.
 */

import { bytes } from '../units';
import type { WorkloadInput } from '../types/input';
import type { PlatformAdapter, DraftShape } from '../types/platform';
import type { ProfileStrategy, Tunables } from '../types/profile';
import { BASELINE_TUNABLES } from '../profiles/batch';
import { runStages, type PipelineContext, type NamedStage } from './context';
import { effectiveDataSize, inflation } from './stages/data-size';
import { workingSet } from './stages/working-set';
import { selectNodeAndCapacity, coresPerExecutor, executorsPerNode, clusterSize } from './stages/cluster-shape';
import { executorMemory, unifiedMemorySplit } from './stages/memory';
import { partitioning, shuffleStorage, driverSizing } from './stages/partitions';
import { validateInput } from './stages/validate';

/**
 * Stage order.
 *
 * There is a genuine cycle in the underlying math: heap depends on working set
 * per task, which depends on partition count, which depends on total cores,
 * which depends on executor count, which depends on heap. It is broken by
 * seeding the partition target from a constant, computing forward, and then
 * correcting once in the convergence stage -- a labelled second pass is
 * explainable to a user in a way a fixed-point solver is not.
 */
export const STAGES: NamedStage[] = [
  validateInput,          // s00
  effectiveDataSize,      // s01
  inflation,              // s02
  workingSet,             // s03
  selectNodeAndCapacity,  // s04 + s05
  coresPerExecutor,       // s06
  executorsPerNode,       // s07
  clusterSize,            // s12 (before memory: executor count sets the budget)
  executorMemory,         // s08 + s09
  unifiedMemorySplit,     // s10
  partitioning,           // s13 + s14
  shuffleStorage,         // s15 (includes the s11 spill check)
  driverSizing,           // s16
];

export function buildContext(
  input: WorkloadInput,
  adapter: PlatformAdapter,
  profile: ProfileStrategy,
): PipelineContext {
  const tunables: Tunables = { ...BASELINE_TUNABLES, ...profile.tunables(input) };
  const draft: DraftShape = { scratch: {} };
  return {
    input, adapter, profile, tunables, draft,
    trace: [], errors: [], halted: false,
  };
}

export function runPipeline(ctx: PipelineContext): PipelineContext {
  return runStages(ctx, STAGES);
}

export { bytes };
export type { PipelineContext };
