/**
 * s08-s11: executor heap, overhead, the unified memory split, and the per-task
 * spill check.
 */

import {
  bytes, mib, roundDownTo, maxBytes, minBytes, type Bytes, type Cores,
} from '../../units';
import { point, divRange, type Range } from '../../range';
import { sizingValue } from '../../range';
import {
  RESERVED_SYSTEM_MEMORY, MIN_EXECUTOR_HEAP, MIN_MEMORY_OVERHEAD,
  MEMORY_OVERHEAD_FACTOR_JVM, MEMORY_OVERHEAD_FACTOR_NON_JVM,
} from '../../constants/spark-defaults';
import {
  OFF_HEAP_RECOMMENDED_ABOVE_HEAP, DEFAULT_PYTHON_WORKER_MEMORY,
} from '../../constants/heuristics';
import type { UnifiedMemoryBreakdown } from '../../types/output';
import { makeStep, fmtBytes } from '../trace';
import { withStep, withScratch, withDraft, type NamedStage, type PipelineContext } from '../context';

export const HEAP = 'heapBytes';
export const OVERHEAD = 'overheadBytes';
export const MEMORY_BREAKDOWN = 'memoryBreakdown';
export const PER_TASK_EXECUTION = 'perTaskExecution';

const isNonJvm = (lang: string): boolean =>
  lang.startsWith('pyspark') || lang === 'sparkr';

/** Python workers are per task slot, and live entirely outside the JVM heap. */
function pysparkMemoryFor(ctx: PipelineContext, coresPerExecutor: Cores): Bytes {
  if (!isNonJvm(ctx.input.runtimeLanguage)) return bytes(0);
  const perWorker = ctx.input.ml?.pythonWorkerMemoryBytes ?? DEFAULT_PYTHON_WORKER_MEMORY;
  return bytes(perWorker * coresPerExecutor);
}

/**
 * s08 + s09: solve heap and overhead together.
 *
 * Works BACKWARDS from the container budget, which is the reverse of how most
 * tutorials teach it but the only correct order on K8s and YARN: the scheduler
 * enforces the container total, so heap is what remains after everything else
 * is subtracted, not the starting point.
 *
 *   heap + overhead(heap) + offHeap + pyspark <= budget
 *   where overhead(heap) = max(384 MiB, heap * factor)
 *
 * Solving the proportional branch: heap = (budget - offHeap - pyspark)/(1+factor)
 */
export const executorMemory: NamedStage = {
  name: 'executorMemory',
  run(ctx: PipelineContext): PipelineContext {
    const allocatable = ctx.draft.allocatable;
    const executorsPerNode = ctx.draft.executorsPerNode;
    const coresPerExecutor = ctx.draft.coresPerExecutor;
    if (!allocatable || !executorsPerNode || !coresPerExecutor) {
      throw new Error('executorMemory requires allocatable, executorsPerNode, coresPerExecutor');
    }

    // Local mode runs one JVM that is both driver and executor, so the whole
    // allocatable budget belongs to it. Splitting it would report two heaps
    // for a process that only has one.
    const budget = bytes(Math.floor(allocatable.memory / executorsPerNode));

    const nonJvm = isNonJvm(ctx.input.runtimeLanguage);
    const overheadFactor =
      ctx.tunables.overheadFactorOverride ??
      (nonJvm ? MEMORY_OVERHEAD_FACTOR_NON_JVM : MEMORY_OVERHEAD_FACTOR_JVM);

    const pysparkMemory = pysparkMemoryFor(ctx, coresPerExecutor);

    // Off-heap is recommended for large heaps to sidestep GC on Tungsten
    // operators. It is NOT part of memoryOverhead: Spark adds it separately to
    // the container request, and forgetting that is a top K8s failure.
    const provisionalHeap = bytes((budget - pysparkMemory) / (1 + overheadFactor));
    const useOffHeap =
      ctx.adapter.capabilities.supportsOffHeap &&
      provisionalHeap > OFF_HEAP_RECOMMENDED_ABOVE_HEAP;
    const offHeap = useOffHeap ? roundDownTo(bytes(provisionalHeap * 0.25), mib(256)) : bytes(0);

    const available = bytes(budget - offHeap - pysparkMemory);
    let heap = bytes(available / (1 + overheadFactor));
    // Where the proportional overhead falls under the 384 MiB floor, the floor
    // binds instead and heap is simply the remainder.
    if (heap * overheadFactor < MIN_MEMORY_OVERHEAD) {
      heap = bytes(available - MIN_MEMORY_OVERHEAD);
    }
    heap = roundDownTo(heap, mib(256));

    if (ctx.tunables.maxRecommendedHeap) {
      heap = minBytes(heap, ctx.tunables.maxRecommendedHeap);
    }

    const overhead = maxBytes(MIN_MEMORY_OVERHEAD, bytes(Math.ceil(heap * overheadFactor)));
    const containerTotal = bytes(heap + overhead + offHeap + pysparkMemory);

    const belowMinimum = heap < MIN_EXECUTOR_HEAP;

    const step = makeStep({
      id: 'executor-heap',
      title: 'Executor heap and container footprint',
      formula:
        `budget ${fmtBytes(budget)} = allocatable ${fmtBytes(allocatable.memory)} / ` +
        `${executorsPerNode} executors per node\n` +
        `heap = (${fmtBytes(budget)} - ${fmtBytes(offHeap)} off-heap - ` +
        `${fmtBytes(pysparkMemory)} pyspark) / (1 + ${overheadFactor}) = ${fmtBytes(heap)}\n` +
        `overhead = max(384 MiB, ${fmtBytes(heap)} x ${overheadFactor}) = ${fmtBytes(overhead)}\n` +
        `container = ${fmtBytes(heap)} + ${fmtBytes(overhead)} + ${fmtBytes(offHeap)} + ` +
        `${fmtBytes(pysparkMemory)} = ${fmtBytes(containerTotal)}`,
      inputs: {
        allocatableMemory: allocatable.memory,
        executorsPerNode,
        coresPerExecutor,
        overheadFactor,
        runtimeLanguage: ctx.input.runtimeLanguage,
      },
      outputs: {
        heapBytes: heap, overheadBytes: overhead, offHeapBytes: offHeap,
        pysparkMemoryBytes: pysparkMemory, containerTotalBytes: containerTotal,
      },
      rationale:
        'Heap is solved backwards from the container budget, because the scheduler ' +
        'enforces the container total, not the heap. ' +
        (nonJvm
          ? `This is a ${ctx.input.runtimeLanguage} workload, so memoryOverheadFactor is ` +
            `${overheadFactor}, not the 0.1 JVM default: Python worker processes live ` +
            'entirely outside the heap. Using 0.1 here is what produces exit-137 OOMKills ' +
            'that look like random executor loss.'
          : 'JVM workload, so the 0.1 overhead factor applies.') +
        (useOffHeap
          ? ` Off-heap execution memory is enabled because the heap exceeds ${fmtBytes(OFF_HEAP_RECOMMENDED_ABOVE_HEAP)}; ` +
            'Tungsten operators then bypass GC entirely. Note this is added to the container ' +
            'request separately from overhead.'
          : ''),
      confidence: 'documented',
      citations: [
        { label: 'spark.executor.memoryOverheadFactor', kind: 'spark-config' },
        { label: 'spark.memory.offHeap.size', kind: 'spark-config' },
      ],
    });

    let next = withDraft(withStep(ctx, step), {
      heapBytes: heap,
      overheadBytes: overhead,
      offHeapBytes: offHeap,
      pysparkMemoryBytes: pysparkMemory,
      containerTotalBytes: containerTotal,
    });

    if (belowMinimum) {
      next = {
        ...next,
        errors: [...next.errors, {
          path: 'cluster.node',
          message:
            `Computed executor heap ${fmtBytes(heap)} is below Spark's ${fmtBytes(MIN_EXECUTOR_HEAP)} ` +
            'minimum. Spark will refuse to start with "System memory must be at least 471859200". ' +
            'Use a larger node or fewer executors per node.',
          severity: 'fatal',
        }],
        halted: true,
      };
    }

    return withScratch(next, { [HEAP]: heap, [OVERHEAD]: overhead });
  },
};

/**
 * s10: the unified memory split.
 *
 * The subtlety the UI must convey: storageFraction is an eviction-immunity
 * FLOOR, not a partition. Execution can borrow the whole unified pool and evict
 * cached blocks down to that floor; storage can never evict execution. Reading
 * it as "half the pool is reserved for cache" is the most common conceptual
 * error users have about Spark memory.
 */
export const unifiedMemorySplit: NamedStage = {
  name: 'unifiedMemorySplit',
  run(ctx: PipelineContext): PipelineContext {
    const heap = ctx.draft.heapBytes;
    const offHeap = ctx.draft.offHeapBytes ?? bytes(0);
    const coresPerExecutor = ctx.draft.coresPerExecutor;
    if (!heap || !coresPerExecutor) throw new Error('unifiedMemorySplit requires heap and cores');

    const { memoryFraction, storageFraction } = ctx.tunables;

    const usable = bytes(heap - RESERVED_SYSTEM_MEMORY);
    const unifiedOnHeap = bytes(usable * memoryFraction);
    const unifiedTotal = bytes(unifiedOnHeap + offHeap);
    const storageFloor = bytes(unifiedTotal * storageFraction);
    const executionFloor = bytes(unifiedTotal - storageFloor);
    const userMemory = bytes(usable * (1 - memoryFraction));
    const perTask = bytes(unifiedTotal / coresPerExecutor);

    const breakdown: UnifiedMemoryBreakdown = {
      heap, reserved: RESERVED_SYSTEM_MEMORY, usableForUnified: usable,
      unifiedOnHeap, unifiedTotal, storageFloor, executionFloor, userMemory,
      memoryFraction, storageFraction,
      perTaskExecutionAtFullParallelism: perTask,
    };

    const step = makeStep({
      id: 'unified-memory-split',
      title: 'Inside the executor heap',
      formula:
        `usable = ${fmtBytes(heap)} - ${fmtBytes(RESERVED_SYSTEM_MEMORY)} reserved = ${fmtBytes(usable)}\n` +
        `unified pool = ${fmtBytes(usable)} x ${memoryFraction}` +
        (offHeap > 0 ? ` + ${fmtBytes(offHeap)} off-heap` : '') +
        ` = ${fmtBytes(unifiedTotal)}\n` +
        `storage floor = ${fmtBytes(unifiedTotal)} x ${storageFraction} = ${fmtBytes(storageFloor)}\n` +
        `user memory = ${fmtBytes(usable)} x ${(1 - memoryFraction).toFixed(2)} = ${fmtBytes(userMemory)}\n` +
        `per task slot = ${fmtBytes(unifiedTotal)} / ${coresPerExecutor} cores = ${fmtBytes(perTask)}`,
      inputs: { heap, offHeap, memoryFraction, storageFraction, coresPerExecutor },
      outputs: {
        usableForUnified: usable, unifiedTotal, storageFloor,
        executionFloor, userMemory, perTaskExecution: perTask,
      },
      rationale:
        `A ${fmtBytes(heap)} executor does not have ${fmtBytes(heap)} for your data. ` +
        `Spark reserves ${fmtBytes(RESERVED_SYSTEM_MEMORY)} off the top, then splits what ` +
        `remains: ${(memoryFraction * 100).toFixed(0)}% into the unified execution+storage ` +
        `pool and ${((1 - memoryFraction) * 100).toFixed(0)}% into user memory for UDF ` +
        'objects and Spark internal metadata. The storage floor is an eviction-immunity ' +
        'line, not a reservation: execution may borrow the entire pool and evict cached ' +
        'blocks down to that floor, while storage can never evict execution. ' +
        `The number that actually governs spill is the per-task-slot share: ${fmtBytes(perTask)}. ` +
        'That figure assumes partitions of roughly equal size; a skewed partition consumes ' +
        'more and may spill while average ones do not.',
      confidence: 'documented',
      citations: [
        { label: 'spark.memory.fraction', kind: 'spark-config' },
        { label: 'spark.memory.storageFraction', kind: 'spark-config' },
        { label: 'UnifiedMemoryManager.RESERVED_SYSTEM_MEMORY_BYTES', kind: 'spark-source' },
      ],
    });

    return withScratch(withStep(ctx, step), {
      [MEMORY_BREAKDOWN]: breakdown,
      [PER_TASK_EXECUTION]: perTask,
    });
  },
};

/**
 * s11: will this spill?
 *
 * The quantitative basis for the flagship warning. Spill is not a failure --
 * it is Spark working as designed -- but it costs 2-10x, and users are
 * routinely unaware they are paying it.
 */
export function computeSpillPressure(
  perPartitionBytes: Range,
  perTaskExecution: Bytes,
): { ratio: Range; predicted: boolean } {
  const ratio = divRange(perPartitionBytes, point(perTaskExecution));
  return { ratio, predicted: sizingValue(ratio) > 1 };
}

export { RESERVED_SYSTEM_MEMORY };
