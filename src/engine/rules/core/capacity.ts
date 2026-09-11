/**
 * Capacity rules: is this cluster physically capable of the job at all?
 *
 * These sit above tuning advice. A config that cannot run is not improved by
 * better partition sizing, and saying "looks fine" about an impossible job is
 * the worst failure mode this tool has.
 */

import { fmtBytes, fmtSeconds } from '../../pipeline/trace';

import type { Rule } from '../../types/rules';

/**
 * Local mode cannot scale out, so the machine is a hard ceiling. Data that
 * needs many multiples of the machine's memory will thrash spill for hours or
 * simply fail.
 */
const exceedsSingleMachine: Rule = {
  id: 'exceeds-single-machine-capacity',
  category: 'sanity',
  defaultSeverity: 'blocker',
  appliesTo: { platforms: ['local'] },
  // When the job cannot run at all, tuning advice is noise.
  supersedes: [
    'shuffle-partitions-default-200',
    'spill-predicted',
    'local-tmp-disk-for-shuffle',
    'data-too-small-for-spark',
  ],
  evaluate(ctx) {
    const machine = ctx.input.platformInput.local?.machineMemory;
    if (!machine) return null;

    // Spark streams partitions, so a dataset larger than RAM is ordinary. What
    // makes a fixed single machine unusable is the combination the sizer is
    // forced into once it cannot add capacity: partitions grown far past the
    // target to keep the wave count sane, which then cannot fit in a task slot.
    // Severe spill pressure on a cluster that cannot grow IS the failure.
    const total = ctx.result.memory.inflatedScannedBytes.mid;
    const ratio = total / machine;
    const spill = ctx.result.storage.spillPressureRatio.mid;
    if (spill < 4) return null;

    return {
      severity: spill > 20 ? 'blocker' : 'critical',
      title: 'One machine is too small for this much data',
      message:
        `${fmtBytes(total)} has to stream through ${ctx.result.parallelism.totalTaskSlots} ` +
        `task slots on a machine with ${fmtBytes(machine)} of RAM (${ratio.toFixed(0)}x the ` +
        'data). Spark streams partitions rather than holding everything, so size alone is not ' +
        'the problem. The problem is that a single machine cannot add capacity, so each task ' +
        `ends up needing roughly ${spill.toFixed(0)}x more memory than its slot provides -- ` +
        'every stage will spill heavily to disk. ' +
        (spill > 20
          ? 'At this ratio the run would take many hours at best. This needs a real cluster, ' +
            'or far less data per run.'
          : 'Expect a slow run. Consider a cluster, or processing one period at a time.'),
      evidence: [
        { label: 'Total volume', value: fmtBytes(total), stepId: 'working-set' },
        { label: 'Machine RAM', value: fmtBytes(machine) },
        { label: 'Memory pressure per task', value: `${spill.toFixed(0)}x`, stepId: 'shuffle-storage' },
      ],
      fix: {
        description:
          'Partition the input by date and process one period per run, or move to a ' +
          'multi-node cluster.',
      },
      confidence: 'estimated',
      impact: 100,
    };
  },
};

/**
 * Too many waves means the cluster is far too small for the work, regardless of
 * whether any single task fits in memory.
 */
/**
 * A high wave count is normal on large data -- partitions are sized to fit a
 * task slot, so a big dataset has many of them. What matters is whether the
 * cluster is too small to finish in a sensible time, and whether there is
 * enough parallelism to absorb a straggler.
 */
const tooFewWavesToHideStragglers: Rule = {
  id: 'too-few-waves',
  category: 'parallelism',
  defaultSeverity: 'warning',
  evaluate(ctx) {
    const waves = ctx.result.parallelism.wavesPerStage.mid;
    if (waves >= 2 || ctx.result.parallelism.shufflePartitions <= 1) return null;
    return {
      severity: 'warning',
      title: 'Not enough parallelism to absorb a slow task',
      message:
        `${ctx.result.parallelism.shufflePartitions} partitions across ` +
        `${ctx.result.parallelism.totalTaskSlots} task slots is under one full wave, so every ` +
        'task runs concurrently and the stage takes as long as its slowest task. With at ' +
        'least two waves the scheduler can start a new task as soon as a slot frees, which ' +
        'hides variation between tasks. Either use fewer, larger executors, or split the ' +
        'input into more partitions.',
      evidence: [
        { label: 'Partitions', value: `${ctx.result.parallelism.shufflePartitions}` },
        { label: 'Task slots', value: `${ctx.result.parallelism.totalTaskSlots}` },
        { label: 'Waves', value: waves.toFixed(1), stepId: 'shuffle-partitions' },
      ],
      confidence: 'estimated',
      impact: 45,
    };
  },
};

/**
 * The honest version of "your cluster is too small": not a wave count, but a
 * runtime that misses the target the user actually asked for.
 */
const missesRuntimeTarget: Rule = {
  id: 'misses-runtime-target',
  category: 'sanity',
  defaultSeverity: 'warning',
  evaluate(ctx) {
    const target = ctx.input.sla.targetRuntime;
    if (!target) return null;
    const estimated = ctx.result.runtime?.seconds.mid;
    if (!estimated) return null;
    const overshoot = estimated / target;
    if (overshoot <= 1.5) return null;

    return {
      severity: overshoot > 4 ? 'critical' : 'warning',
      title: 'This cluster will likely miss your runtime target',
      message:
        `You asked for about ${fmtSeconds(target)}, but this configuration is estimated at ` +
        `${fmtSeconds(estimated)} -- roughly ${overshoot.toFixed(1)}x over. ` +
        (ctx.result.parallelism.totalCores >= ctx.result.parallelism.shufflePartitions
          ? 'The cluster already has a core per partition, so adding nodes will not help: ' +
            'the work needs to be split into more partitions first, or the target relaxed.'
          : 'Adding cores would shorten it roughly proportionally, if the budget allows.') +
        ' Throughput per core is the least reliable figure in this model, so treat the ' +
        'estimate as an order of magnitude rather than a promise.',
      evidence: [
        { label: 'Target', value: fmtSeconds(target) },
        { label: 'Estimated', value: fmtSeconds(estimated), stepId: 'node-count' },
        { label: 'Cores', value: `${ctx.result.parallelism.totalCores}` },
      ],
      confidence: 'guess',
      impact: 70,
    };
  },
};

export const capacityRules = (): Rule[] => [
  exceedsSingleMachine,
  tooFewWavesToHideStragglers,
  missesRuntimeTarget,
];
