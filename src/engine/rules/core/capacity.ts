/**
 * Capacity rules: is this cluster physically capable of the job at all?
 *
 * These sit above tuning advice. A config that cannot run is not improved by
 * better partition sizing, and saying "looks fine" about an impossible job is
 * the worst failure mode this tool has.
 */

import { fmtBytes } from '../../pipeline/trace';
import { WAVES_WARN_ABOVE } from '../../constants/heuristics';
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
    'too-many-waves',
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
    const waves = ctx.result.parallelism.wavesPerStage.mid;
    if (spill < 4 && waves < 200) return null;

    return {
      severity: spill > 20 || waves > 800 ? 'blocker' : 'critical',
      title: 'One machine is too small for this much data',
      message:
        `${fmtBytes(total)} has to stream through ${ctx.result.parallelism.totalTaskSlots} ` +
        `task slots on a machine with ${fmtBytes(machine)} of RAM (${ratio.toFixed(0)}x the ` +
        'data). Spark streams partitions rather than holding everything, so size alone is not ' +
        'the problem. The problem is that a single machine cannot add capacity, so each task ' +
        `ends up needing roughly ${spill.toFixed(0)}x more memory than its slot provides -- ` +
        'every stage will spill heavily to disk. ' +
        (spill > 20 || waves > 800
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
const farTooManyWaves: Rule = {
  id: 'too-many-waves',
  category: 'parallelism',
  defaultSeverity: 'warning',
  evaluate(ctx) {
    const waves = ctx.result.parallelism.wavesPerStage.mid;
    if (waves <= WAVES_WARN_ABOVE) return null;

    const severe = waves > 100;
    return {
      severity: severe ? 'critical' : 'warning',
      title: `Every stage runs in about ${Math.round(waves).toLocaleString()} waves`,
      message:
        `${ctx.result.parallelism.shufflePartitions.toLocaleString()} partitions across only ` +
        `${ctx.result.parallelism.totalTaskSlots} task slots means each stage runs in roughly ` +
        `${Math.round(waves).toLocaleString()} sequential waves. ` +
        (severe
          ? 'At that ratio the cluster is drastically undersized for the data: wall-clock time ' +
            'scales with the wave count, so this will be slow in a way no tuning fixes. Add ' +
            'capacity or reduce the data per run.'
          : `Two to four waves is the useful range -- enough to hide stragglers, not so many ` +
            'that task-launch overhead accumulates.'),
      evidence: [
        { label: 'Partitions', value: ctx.result.parallelism.shufflePartitions.toLocaleString() },
        { label: 'Task slots', value: `${ctx.result.parallelism.totalTaskSlots}` },
        { label: 'Waves', value: Math.round(waves).toLocaleString(), stepId: 'shuffle-partitions' },
      ],
      confidence: 'estimated',
      impact: severe ? 95 : 55,
    };
  },
};

export const capacityRules = (): Rule[] => [exceedsSingleMachine, farTooManyWaves];
