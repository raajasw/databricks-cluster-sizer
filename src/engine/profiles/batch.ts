/**
 * Batch ETL: the baseline profile. Every other profile is described as a
 * deviation from these tunables.
 */

import { mib, gib } from '../units';
import {
  TARGET_PARTITION_BYTES, TARGET_WAVES_MIN, TARGET_WAVES_MAX,
  CORES_PER_EXECUTOR_MIN, CORES_PER_EXECUTOR_MAX, DRIVER_MEMORY_FLOOR,
} from '../constants/heuristics';
import {
  MEMORY_FRACTION, STORAGE_FRACTION, MAX_PARTITION_BYTES_DEFAULT,
  AQE_ADVISORY_PARTITION_SIZE_DEFAULT,
} from '../constants/spark-defaults';
import type { ProfileStrategy, Tunables } from '../types/profile';
import type { Rule } from '../types/rules';

export const BASELINE_TUNABLES: Tunables = {
  memoryFraction: MEMORY_FRACTION,
  storageFraction: STORAGE_FRACTION,
  targetInputPartitionBytes: MAX_PARTITION_BYTES_DEFAULT,
  targetShufflePartitionBytes: TARGET_PARTITION_BYTES,
  advisoryPartitionBytes: AQE_ADVISORY_PARTITION_SIZE_DEFAULT,
  targetWavesMin: TARGET_WAVES_MIN,
  targetWavesMax: TARGET_WAVES_MAX,
  coresPerExecutorMin: CORES_PER_EXECUTOR_MIN,
  coresPerExecutorMax: CORES_PER_EXECUTOR_MAX,
  headroomFactor: 1.0,
  driverMemoryFloor: DRIVER_MEMORY_FLOOR,
};

const spotDriver: Rule = {
  id: 'spot-driver',
  category: 'platform',
  defaultSeverity: 'warning',
  evaluate(ctx) {
    if (!ctx.input.cluster.allowSpot) return null;
    if (!ctx.adapter.capabilities.hasSeparateDriverNode) return null;
    return {
      severity: 'warning',
      title: 'Keep the driver on on-demand capacity',
      message:
        'Spot and preemptible instances are a good fit for executors: losing one costs a ' +
        'recompute of its tasks. Losing the driver kills the entire job, including all ' +
        'completed work. Run executors on spot and the driver on on-demand.',
      evidence: [{ label: 'Spot allowed', value: 'yes' }],
      confidence: 'documented',
      impact: 60,
    };
  },
};

export const batchProfile: ProfileStrategy = {
  id: 'batch-etl',
  displayName: 'Batch ETL',
  tunables: () => ({}),
  rules: () => [spotDriver],
};

export { gib, mib };
