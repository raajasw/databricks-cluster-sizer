/**
 * Local-mode rules. These encode the traps that catch essentially every local
 * PySpark user at least once.
 */

import { gib } from '../../units';
import { fmtBytes } from '../../pipeline/trace';
import type { Rule } from '../../types/rules';

const executorMemoryIgnored: Rule = {
  id: 'local-executor-memory-ignored',
  category: 'config',
  defaultSeverity: 'info',
  appliesTo: { platforms: ['local'] },
  evaluate(ctx) {
    return {
      severity: 'info',
      title: 'spark.executor.memory does nothing in local mode',
      message:
        'Local mode runs the driver and executor in a single JVM, so there is no executor ' +
        'process to size. Only spark.driver.memory has any effect. This is shown rather than ' +
        'hidden because setting executor memory and seeing no change is a common source of ' +
        'confusion.',
      evidence: [
        { label: 'Master', value: `local[${Math.floor(ctx.result.parallelism.totalCores)}]` },
        { label: 'Effective heap', value: fmtBytes(ctx.result.driver.heapBytes), stepId: 'driver-sizing' },
      ],
      confidence: 'documented',
      impact: 30,
    };
  },
};

const driverMemorySetTooLate: Rule = {
  id: 'local-driver-memory-set-too-late',
  category: 'config',
  defaultSeverity: 'warning',
  appliesTo: { platforms: ['local'] },
  evaluate(ctx) {
    if (!ctx.input.runtimeLanguage.startsWith('pyspark')) return null;
    return {
      severity: 'warning',
      title: 'Set driver memory before the JVM starts, not in builder.config()',
      message:
        'In PySpark, SparkSession.builder.config("spark.driver.memory", ...) is a silent no-op ' +
        'when the JVM is already running: heap size is fixed at JVM launch. Use --driver-memory ' +
        'on spark-submit, set SPARK_DRIVER_MEMORY, or put it in spark-defaults.conf. The symptom ' +
        'is an OOM at exactly the default 1g no matter what you configure.',
      evidence: [
        { label: 'Required heap', value: fmtBytes(ctx.result.driver.heapBytes), stepId: 'driver-sizing' },
        { label: 'Default if ignored', value: '1g' },
      ],
      fix: {
        description: 'Launch with the memory flag, or export it before starting Python.',
      },
      confidence: 'documented',
      impact: 80,
    };
  },
};

const oversubscribed: Rule = {
  id: 'local-oversubscribed',
  category: 'memory',
  defaultSeverity: 'critical',
  appliesTo: { platforms: ['local'] },
  evaluate(ctx) {
    const machine = ctx.input.platformInput.local?.machineMemory;
    if (!machine) return null;
    const share = ctx.result.driver.heapBytes / machine;
    if (share <= 0.65) return null;
    return {
      severity: 'critical',
      title: 'Driver heap claims too much of the machine',
      message:
        `A ${fmtBytes(ctx.result.driver.heapBytes)} heap on a ${fmtBytes(machine)} machine is ` +
        `${(share * 100).toFixed(0)}% of total RAM. Once the OS starts swapping, Spark runs ` +
        'orders of magnitude slower than it would with a smaller heap, and the machine becomes ' +
        'unusable. Leave at least a third of RAM to the OS.',
      evidence: [
        { label: 'Machine RAM', value: fmtBytes(machine) },
        { label: 'Requested heap', value: fmtBytes(ctx.result.driver.heapBytes) },
        { label: 'Share', value: `${(share * 100).toFixed(0)}%` },
      ],
      confidence: 'estimated',
      impact: 90,
    };
  },
};

const localDiskForShuffle: Rule = {
  id: 'local-tmp-disk-for-shuffle',
  category: 'storage',
  defaultSeverity: 'warning',
  appliesTo: { platforms: ['local'] },
  evaluate(ctx) {
    const free = ctx.input.platformInput.local?.freeDiskBytes;
    const need = ctx.result.storage.requiredLocalDiskPerExecutorBytes;
    if (!free || need < gib(1)) return null;
    if (free > need * 1.5) return null;
    return {
      severity: 'warning',
      title: 'Not much room for shuffle and spill',
      message:
        `This job is estimated to need about ${fmtBytes(need)} of scratch space for shuffle ` +
        `and spill, against ${fmtBytes(free)} free. Running out mid-job fails with ` +
        '"No space left on device", typically well into the run. Point spark.local.dir at a ' +
        'roomier disk, or reduce the data scanned per run.',
      evidence: [
        { label: 'Estimated need', value: fmtBytes(need), stepId: 'shuffle-storage' },
        { label: 'Free disk', value: fmtBytes(free) },
      ],
      confidence: 'estimated',
      impact: 70,
    };
  },
};

export const localRules = (): Rule[] => [
  executorMemoryIgnored,
  driverMemorySetTooLate,
  oversubscribed,
  localDiskForShuffle,
];
