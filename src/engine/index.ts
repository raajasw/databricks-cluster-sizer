/**
 * Public engine API.
 *
 * computeRecommendation is a PURE function: no I/O, no clock, no randomness.
 * That is what makes the math exhaustively testable, lets the UI recompute on
 * every keystroke, and makes sensitivity analysis a matter of calling it twenty
 * times with perturbed inputs.
 */

import { bytes, cores, type Bytes } from './units';
import { sizingValue, point, type Range } from './range';
import type { WorkloadInput } from './types/input';
import type {
  Recommendation, SizingResult, Verdict, ParallelismSpec, StorageSpec,
  DriverSpec, UtilizationSpec, InflationBreakdown, UnifiedMemoryBreakdown,
} from './types/output';
import type { EmitInput } from './types/platform';
import { getAdapter } from './platforms/registry';
import { getProfile } from './profiles/registry';
import { buildContext, runPipeline } from './pipeline';
import { allRules } from './rules/registry';
import { evaluateRules } from './rules/engine';
import { INFLATION, INFLATED_BYTES } from './pipeline/stages/data-size';
import { WORKING_SET, CACHE_DEMAND } from './pipeline/stages/working-set';
import { MEMORY_BREAKDOWN } from './pipeline/stages/memory';
import { PARALLELISM, STORAGE, DRIVER } from './pipeline/stages/partitions';
import { UTILIZATION } from './pipeline/stages/cluster-shape';
import { fmtBytes } from './pipeline/trace';

export const ENGINE_VERSION = '0.1.0';

export interface ComputeOptions {
  /** Injected rather than read from a clock, to keep the engine deterministic. */
  nowMs?: number;
}

export function computeRecommendation(
  input: WorkloadInput,
  options: ComputeOptions = {},
): Recommendation {
  const adapter = getAdapter(input.platformInput.platform);
  const profile = getProfile(input.profile);

  const ctx = runPipeline(buildContext(input, adapter, profile));

  const inflationBreakdown = ctx.draft.scratch[INFLATION] as InflationBreakdown | undefined;
  const memoryBreakdown = ctx.draft.scratch[MEMORY_BREAKDOWN] as UnifiedMemoryBreakdown | undefined;
  const parallelism = ctx.draft.scratch[PARALLELISM] as ParallelismSpec | undefined;
  const storage = ctx.draft.scratch[STORAGE] as StorageSpec | undefined;
  const driver = ctx.draft.scratch[DRIVER] as DriverSpec | undefined;
  const utilization = ctx.draft.scratch[UTILIZATION] as UtilizationSpec | undefined;

  // A fatal error halts the pipeline; return what was derived plus the errors
  // rather than throwing, so the UI can still show partial reasoning.
  if (ctx.halted || !memoryBreakdown || !parallelism || !storage || !driver) {
    return {
      primary: partialResult(ctx, input),
      alternatives: [],
      findings: [],
      verdict: {
        level: 'will-fail',
        headline: 'Could not size this configuration',
        reasoning: ctx.errors.map((e) => e.message),
      },
      errors: [...ctx.errors],
      engineVersion: ENGINE_VERSION,
      computedAtMs: options.nowMs ?? 0,
    };
  }

  const executorCount = ctx.draft.executorCount ?? 1;
  const emitInput: EmitInput = {
    nodeType: ctx.draft.nodeType,
    nodeCount: ctx.draft.nodeCount ?? 1,
    executorCount,
    coresPerExecutor: ctx.draft.coresPerExecutor ?? cores(1),
    heapBytes: ctx.draft.heapBytes ?? bytes(0),
    overheadBytes: ctx.draft.overheadBytes ?? bytes(0),
    offHeapBytes: ctx.draft.offHeapBytes ?? bytes(0),
    pysparkMemoryBytes: ctx.draft.pysparkMemoryBytes ?? bytes(0),
    containerTotalBytes: ctx.draft.containerTotalBytes ?? bytes(0),
    driverCores: driver.cores,
    driverHeapBytes: driver.heapBytes,
    driverOverheadBytes: driver.overheadBytes,
    driverMaxResultSize: driver.maxResultSizeBytes,
    shufflePartitions: parallelism.shufflePartitions,
    aqeAdvisoryPartitionBytes: parallelism.aqeAdvisoryPartitionBytes,
    maxPartitionBytes: parallelism.targetInputPartitionBytes,
    memoryFraction: memoryBreakdown.memoryFraction,
    storageFraction: memoryBreakdown.storageFraction,
  };

  const config = adapter.emitConfig(emitInput, { input, draft: ctx.draft });

  const inflated = (ctx.draft.scratch[INFLATED_BYTES] as Range) ?? point(0);
  const working = (ctx.draft.scratch[WORKING_SET] as Range) ?? point(0);
  const cacheDemand = (ctx.draft.scratch[CACHE_DEMAND] as Range) ?? point(0);

  const result: SizingResult = {
    input,
    platform: adapter.id,
    nodeType: ctx.draft.nodeType,
    nodeCount: ctx.draft.nodeCount ?? 1,
    executor: {
      count: executorCount,
      coresPerExecutor: ctx.draft.coresPerExecutor ?? cores(1),
      heapBytes: ctx.draft.heapBytes ?? bytes(0),
      overheadBytes: ctx.draft.overheadBytes ?? bytes(0),
      overheadFactor: overheadFactorOf(ctx.draft.heapBytes, ctx.draft.overheadBytes),
      offHeapBytes: ctx.draft.offHeapBytes ?? bytes(0),
      pysparkMemoryBytes: ctx.draft.pysparkMemoryBytes ?? bytes(0),
      containerTotalBytes: ctx.draft.containerTotalBytes ?? bytes(0),
      memoryBreakdown,
    },
    driver,
    parallelism,
    memory: {
      inflation: inflationBreakdown ?? emptyInflation(),
      inflatedScannedBytes: inflated,
      workingSetBytes: working,
      cacheDemandBytes: cacheDemand,
      totalClusterMemory: bytes(executorCount * (ctx.draft.containerTotalBytes ?? 0)),
    },
    storage,
    utilization: utilization ?? {
      cpuPackingEfficiency: 1, memPackingEfficiency: 1,
      strandedCpuPerNode: cores(0), strandedMemoryPerNode: bytes(0),
      bindingDimension: 'cpu',
    },
    trace: [...ctx.trace],
    config,
  };

  const findings = evaluateRules(allRules(adapter, profile), { input, result, adapter });

  return {
    primary: result,
    alternatives: [],
    findings,
    verdict: buildVerdict(result, findings),
    errors: [...ctx.errors],
    engineVersion: ENGINE_VERSION,
    computedAtMs: options.nowMs ?? 0,
  };
}

function overheadFactorOf(heap: Bytes | undefined, overhead: Bytes | undefined): number {
  if (!heap || !overhead) return 0.1;
  return Math.round((overhead / heap) * 100) / 100;
}

function emptyInflation(): InflationBreakdown {
  return {
    codecFactor: point(1), encodingFactor: point(1),
    representationFactor: point(1), queryAmplification: point(1),
    total: point(1), confidence: 'guess',
  };
}

function buildVerdict(result: SizingResult, findings: ReturnType<typeof evaluateRules>): Verdict {
  const dontUseSpark = findings.find((f) => f.ruleId === 'data-too-small-for-spark');
  if (dontUseSpark) {
    // Severity distinguishes "clearly fits one machine" from "borderline".
    // Both belong at the top of the page: a config the user should not be
    // building at all outranks any tuning detail within it.
    const clear = dontUseSpark.severity === 'critical';
    return {
      level: clear ? 'dont-use-spark' : 'reconsider-spark',
      headline: clear
        ? 'A single machine would likely serve you better here'
        : 'Worth checking whether you need a cluster at all',
      reasoning: [dontUseSpark.message],
    };
  }

  const blockers = findings.filter((f) => f.severity === 'blocker');
  if (blockers.length > 0) {
    return {
      level: 'will-fail',
      headline: 'This configuration will not run as specified',
      reasoning: blockers.map((f) => f.message),
    };
  }

  const criticals = findings.filter((f) => f.severity === 'critical');
  if (criticals.length > 0) {
    return {
      level: 'risky',
      headline: `${criticals.length} serious issue${criticals.length === 1 ? '' : 's'} to address first`,
      reasoning: criticals.map((f) => f.message),
    };
  }

  return {
    level: 'ok',
    headline: `${result.nodeCount} node${result.nodeCount === 1 ? '' : 's'}, ` +
      `${result.parallelism.totalCores} cores, ` +
      `${fmtBytes(result.executor.heapBytes)} per executor`,
    reasoning: [
      `Sized for ${result.parallelism.shufflePartitions} shuffle partitions across ` +
      `${result.parallelism.totalTaskSlots} task slots.`,
    ],
  };
}

function partialResult(
  ctx: ReturnType<typeof runPipeline>,
  input: WorkloadInput,
): SizingResult {
  const zero = bytes(0);
  return {
    input,
    platform: input.platformInput.platform,
    nodeCount: 0,
    executor: {
      count: 0, coresPerExecutor: cores(0), heapBytes: zero, overheadBytes: zero,
      overheadFactor: 0, offHeapBytes: zero, pysparkMemoryBytes: zero,
      containerTotalBytes: zero,
      memoryBreakdown: {
        heap: zero, reserved: zero, usableForUnified: zero, unifiedOnHeap: zero,
        unifiedTotal: zero, storageFloor: zero, executionFloor: zero, userMemory: zero,
        memoryFraction: 0, storageFraction: 0, perTaskExecutionAtFullParallelism: zero,
      },
    },
    driver: {
      cores: cores(0), heapBytes: zero, overheadBytes: zero, maxResultSizeBytes: zero,
      containerTotalBytes: zero, onSeparateNode: false,
    },
    parallelism: {
      totalCores: cores(0), totalTaskSlots: 0, inputPartitions: 0,
      targetInputPartitionBytes: zero, shufflePartitions: 0,
      aqeAdvisoryPartitionBytes: zero, wavesPerStage: point(0), defaultParallelism: 0,
    },
    memory: {
      inflation: emptyInflation(), inflatedScannedBytes: point(0),
      workingSetBytes: point(0), cacheDemandBytes: point(0), totalClusterMemory: zero,
    },
    storage: {
      shuffleWriteBytesPerStage: point(0), peakShuffleOnDiskBytes: point(0),
      spillEstimateBytes: point(0), spillPredicted: false, spillPressureRatio: point(0),
      requiredLocalDiskPerExecutorBytes: zero, hasEnoughLocalDisk: true,
      localDirStrategy: '',
    },
    utilization: {
      cpuPackingEfficiency: 0, memPackingEfficiency: 0,
      strandedCpuPerNode: cores(0), strandedMemoryPerNode: zero,
      bindingDimension: 'cpu',
    },
    trace: [...ctx.trace],
    config: { kind: 'local', sparkSubmitArgs: [], sparkConf: [], envVars: {} },
  };
}

export { sizingValue };
export type { WorkloadInput, Recommendation, SizingResult };
