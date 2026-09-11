/**
 * Memory rules, including the spill prediction that is the engine's most
 * decision-relevant output.
 */

import { sizingValue } from '../../range';
import {
  COMPRESSED_OOPS_SAFE_HEAP, COMPRESSED_OOPS_RECOVERY_HEAP,
  MEMORY_OVERHEAD_FACTOR_NON_JVM, SHUFFLE_PARTITIONS_DEFAULT,
} from '../../constants/spark-defaults';
import { CORES_PER_EXECUTOR_WARN_ABOVE } from '../../constants/heuristics';
import { fmtBytes } from '../../pipeline/trace';
import type { Rule } from '../../types/rules';

/** The flagship quantitative warning. */
const spillPredicted: Rule = {
  id: 'spill-predicted',
  category: 'memory',
  defaultSeverity: 'warning',
  evaluate(ctx) {
    const { storage, parallelism, executor } = ctx.result;
    if (!storage.spillPredicted) return null;
    const ratio = sizingValue(storage.spillPressureRatio);
    const perTask = executor.memoryBreakdown.perTaskExecutionAtFullParallelism;
    const perPartition = sizingValue(ctx.result.memory.inflatedScannedBytes) /
      parallelism.shufflePartitions;

    return {
      severity: ratio > 3 ? 'critical' : 'warning',
      title: 'Tasks will spill to disk',
      message:
        `${parallelism.shufflePartitions} shuffle partitions against ` +
        `${fmtBytes(sizingValue(ctx.result.memory.inflatedScannedBytes))} of in-memory data ` +
        `puts about ${fmtBytes(perPartition)} in each partition, but each task slot has only ` +
        `${fmtBytes(perTask)} of execution memory -- roughly ${ratio.toFixed(1)}x more demand ` +
        'than supply. The sort and aggregate operators will spill to disk. Spill is Spark ' +
        'working as designed rather than a crash, but it typically costs 2-10x in wall time. ' +
        'Raise the partition count, lower the advisory partition size, or give each task more ' +
        'memory by reducing cores per executor.',
      evidence: [
        { label: 'Per partition', value: fmtBytes(perPartition), stepId: 'shuffle-storage' },
        { label: 'Per task slot', value: fmtBytes(perTask), stepId: 'unified-memory-split' },
        { label: 'Pressure ratio', value: `${ratio.toFixed(1)}x` },
        { label: 'Shuffle partitions', value: `${parallelism.shufflePartitions}` },
      ],
      confidence: 'estimated',
      impact: 90,
    };
  },
};

/**
 * A widely-unknown JVM cliff: past ~32 GiB, compressed oops stop working and
 * every reference widens from 4 to 8 bytes.
 */
const compressedOopsBoundary: Rule = {
  id: 'compressed-oops-boundary',
  category: 'memory',
  defaultSeverity: 'warning',
  evaluate(ctx) {
    const heap = ctx.result.executor.heapBytes;
    if (heap <= COMPRESSED_OOPS_SAFE_HEAP || heap >= COMPRESSED_OOPS_RECOVERY_HEAP) return null;
    return {
      severity: 'warning',
      title: 'This heap size lands in the compressed-oops dead zone',
      message:
        `A ${fmtBytes(heap)} heap sits in the worst region for JVM memory efficiency. Below ` +
        `about ${fmtBytes(COMPRESSED_OOPS_SAFE_HEAP)} the JVM uses compressed ordinary object ` +
        'pointers -- 4-byte references. Above it, every reference widens to 8 bytes, costing ' +
        `roughly 20% of effective heap, so a ${fmtBytes(heap)} heap can hold LESS live data ` +
        `than a ${fmtBytes(COMPRESSED_OOPS_SAFE_HEAP)} one. Either drop to ` +
        `${fmtBytes(COMPRESSED_OOPS_SAFE_HEAP)} and run more executors, or go well past ` +
        `${fmtBytes(COMPRESSED_OOPS_RECOVERY_HEAP)} so the larger heap outweighs the loss.`,
      evidence: [
        { label: 'Executor heap', value: fmtBytes(heap), stepId: 'executor-heap' },
        { label: 'Safe below', value: fmtBytes(COMPRESSED_OOPS_SAFE_HEAP) },
        { label: 'Recovers above', value: fmtBytes(COMPRESSED_OOPS_RECOVERY_HEAP) },
      ],
      confidence: 'documented',
      impact: 75,
    };
  },
};

const pysparkOverheadTooLow: Rule = {
  id: 'overhead-too-low-for-pyspark',
  category: 'python',
  defaultSeverity: 'critical',
  evaluate(ctx) {
    const lang = ctx.input.runtimeLanguage;
    const nonJvm = lang.startsWith('pyspark') || lang === 'sparkr';
    if (!nonJvm) return null;
    if (ctx.result.executor.overheadFactor >= MEMORY_OVERHEAD_FACTOR_NON_JVM) return null;
    return {
      severity: 'critical',
      title: 'Memory overhead is too low for a non-JVM workload',
      message:
        `This is a ${lang} workload, where Python worker processes live entirely outside the ` +
        `JVM heap. Spark defaults memoryOverheadFactor to ${MEMORY_OVERHEAD_FACTOR_NON_JVM} ` +
        'for non-JVM jobs precisely for this reason. At the JVM default of 0.1 the container ' +
        'is undersized, and the failure mode is the kernel killing the container -- exit 137 ' +
        'on Kubernetes, "killed by external signal" on YARN -- which looks like random ' +
        'executor loss rather than a memory misconfiguration.',
      evidence: [
        { label: 'Runtime', value: lang },
        { label: 'Overhead factor', value: `${ctx.result.executor.overheadFactor}` },
        { label: 'Recommended', value: `${MEMORY_OVERHEAD_FACTOR_NON_JVM}` },
      ],
      confidence: 'documented',
      impact: 95,
    };
  },
};

const tooManyCoresPerExecutor: Rule = {
  id: 'too-many-cores-per-executor',
  category: 'memory',
  defaultSeverity: 'info',
  evaluate(ctx) {
    const cpe = ctx.result.executor.coresPerExecutor;
    if (cpe <= CORES_PER_EXECUTOR_WARN_ABOVE) return null;
    if (ctx.adapter.id === 'local') return null;
    if (!ctx.adapter.capabilities.supportsMultipleExecutorsPerNode) return null;
    return {
      severity: 'info',
      title: `${cpe} cores share one heap`,
      message:
        `Each executor runs ${cpe} tasks against a single ` +
        `${fmtBytes(ctx.result.executor.heapBytes)} heap. Two consequences worth weighing: a ` +
        `garbage collection pause stops all ${cpe} tasks at once, and one skewed task that ` +
        `exhausts the heap takes the other ${cpe - 1} down with it. Fewer cores per executor ` +
        'trades some memory efficiency for a smaller blast radius.',
      evidence: [
        { label: 'Cores per executor', value: `${cpe}`, stepId: 'cores-per-executor' },
        { label: 'Shared heap', value: fmtBytes(ctx.result.executor.heapBytes) },
      ],
      confidence: 'estimated',
      impact: 40,
    };
  },
};

const shufflePartitionsDefault: Rule = {
  id: 'shuffle-partitions-default-200',
  category: 'parallelism',
  defaultSeverity: 'warning',
  evaluate(ctx) {
    const recommended = ctx.result.parallelism.shufflePartitions;
    if (recommended === SHUFFLE_PARTITIONS_DEFAULT) return null;
    const ratio = recommended / SHUFFLE_PARTITIONS_DEFAULT;
    if (ratio > 0.5 && ratio < 2) return null;

    const inflated = sizingValue(ctx.result.memory.inflatedScannedBytes);
    const atDefault = inflated / SHUFFLE_PARTITIONS_DEFAULT;
    const perTask = ctx.result.executor.memoryBreakdown.perTaskExecutionAtFullParallelism;

    return {
      severity: 'warning',
      title: `The default 200 shuffle partitions is ${ratio > 1 ? 'far too few' : 'far too many'} here`,
      message: ratio > 1
        ? `Leaving spark.sql.shuffle.partitions at 200 would put ${fmtBytes(atDefault)} in ` +
          `each partition, against ${fmtBytes(perTask)} of execution memory per task slot. ` +
          `This configuration uses ${recommended} instead.`
        : `200 partitions across only ${ctx.result.parallelism.totalTaskSlots} task slots ` +
          `means many waves of very small tasks, where scheduling overhead outweighs the work. ` +
          `This configuration uses ${recommended} instead.`,
      evidence: [
        { label: 'Recommended', value: `${recommended}`, stepId: 'shuffle-partitions' },
        { label: 'Spark default', value: '200' },
        { label: 'At the default', value: `${fmtBytes(atDefault)} per partition` },
      ],
      confidence: 'estimated',
      impact: 65,
    };
  },
};

export const memoryRules = (): Rule[] => [
  spillPredicted,
  compressedOopsBoundary,
  pysparkOverheadTooLow,
  tooManyCoresPerExecutor,
  shufflePartitionsDefault,
];
