/**
 * Sanity rules: the ones that question the premise rather than tune the config.
 */

import { gib } from '../../units';
import {
  SINGLE_MACHINE_VIABLE_BYTES, SPARK_CLEARLY_OVERKILL_BYTES,
  SINGLE_NODE_TOLERANT_SHAPES,
} from '../../constants/heuristics';
import { CODEC_IS_SPLITTABLE } from '../../constants/inflation';
import { SMALL_FILE_THRESHOLD, SMALL_FILE_COUNT_THRESHOLD } from '../../constants/heuristics';
import { fmtBytes } from '../../pipeline/trace';
import type { Rule } from '../../types/rules';

/**
 * The flagship pushback. An advisor that produces a tidy 3-node config for a
 * 2 GB job has failed at its actual purpose.
 */
const dataTooSmallForSpark: Rule = {
  id: 'data-too-small-for-spark',
  category: 'antipattern',
  defaultSeverity: 'critical',
  // When this fires, partition tuning is beside the point.
  supersedes: [
    'shuffle-partitions-default-200',
    'spill-predicted',
    'too-many-cores-per-executor',
  ],
  evaluate(ctx) {
    // Uses the MIDPOINT, not the pessimistic sizing value. Those serve
    // different questions: "how much memory should I provision" wants the
    // pessimistic end, while "would this fit on one machine" wants the typical
    // case -- judging feasibility by the worst case would never recommend the
    // simpler tool.
    const working = ctx.result.memory.inflatedScannedBytes.mid;
    const tolerant = SINGLE_NODE_TOLERANT_SHAPES.includes(
      ctx.input.pipeline.dominantQueryShape,
    );

    // The real question is not an absolute size but whether this fits ONE
    // machine. Where the user told us their machine, compare against it; a
    // 21 GiB working set is trivial on a 64 GiB box and impossible on an 8 GiB
    // one. Streaming and interactive workloads are excluded: they exist for
    // reasons other than volume.
    if (ctx.input.profile === 'streaming' || ctx.input.profile === 'interactive-sql') {
      return null;
    }

    const machineMemory = ctx.input.platformInput.local?.machineMemory;
    const ceiling = Math.max(
      tolerant ? SINGLE_MACHINE_VIABLE_BYTES : SPARK_CLEARLY_OVERKILL_BYTES,
      // A single-process engine can reasonably work against ~70% of a machine,
      // and streams rather than materializing everything, so it tolerates
      // working sets somewhat above raw RAM.
      machineMemory ? machineMemory * 0.7 : 0,
    );
    if (working >= ceiling) return null;

    const clearly = machineMemory
      ? working < machineMemory * 0.5
      : working < SPARK_CLEARLY_OVERKILL_BYTES;
    return {
      severity: clearly ? 'critical' : 'warning',
      title: clearly ? 'You probably do not need Spark for this' : 'A single machine may beat a cluster here',
      message:
        `The peak working set is about ${fmtBytes(working)}` +
        (machineMemory ? `, against ${fmtBytes(machineMemory)} on the machine you described` : '') +
        '. That fits in one machine. DuckDB or Polars will typically finish this faster than Spark ' +
        'can start: no JVM warmup, no shuffle across a network, no scheduling overhead. ' +
        'Spark earns its complexity when data exceeds one machine, when you need fault ' +
        'tolerance across a long run, or when you are already on a cluster for other reasons. ' +
        'If one of those applies, carry on -- the configuration below is sound. If none does, ' +
        'the fastest fix is to not use Spark.',
      evidence: [
        { label: 'Peak working set', value: fmtBytes(working), stepId: 'working-set' },
        { label: 'Single-machine ceiling', value: fmtBytes(ceiling) },
        ...(machineMemory
          ? [{ label: 'Your machine', value: fmtBytes(machineMemory) }]
          : []),
        { label: 'Query shape', value: ctx.input.pipeline.dominantQueryShape },
      ],
      confidence: 'estimated',
      impact: 100,
    };
  },
};

const nonSplittableGzip: Rule = {
  id: 'non-splittable-gzip',
  category: 'parallelism',
  defaultSeverity: 'critical',
  evaluate(ctx) {
    const { codec, fileCount, logicalBytesOnDisk } = ctx.input.data;
    if (CODEC_IS_SPLITTABLE[codec]) return null;
    const files = fileCount ?? 1;
    const slots = ctx.result.parallelism.totalTaskSlots;
    if (files >= slots) return null;

    return {
      severity: 'critical',
      title: `${codec} files cannot be split across tasks`,
      message:
        `${codec} is not a splittable codec, so each file is read start to finish by exactly ` +
        `one task. With ${files} file${files === 1 ? '' : 's'} and ${slots} task slots, at most ` +
        `${files} core${files === 1 ? '' : 's'} will do any work during the read stage -- the ` +
        `other ${slots - files} sit idle no matter how large the cluster is. Re-encode to ` +
        'snappy or zstd Parquet, or split the input into at least as many files as you have ' +
        'task slots.',
      evidence: [
        { label: 'Codec', value: codec },
        { label: 'Files', value: `${files}` },
        { label: 'Task slots', value: `${slots}`, stepId: 'shuffle-partitions' },
        { label: 'Input size', value: fmtBytes(logicalBytesOnDisk) },
      ],
      fix: { description: 'Re-encode the input as snappy or zstd Parquet.' },
      confidence: 'documented',
      impact: 95,
    };
  },
};

const tooManySmallFiles: Rule = {
  id: 'too-many-small-files',
  category: 'parallelism',
  defaultSeverity: 'warning',
  evaluate(ctx) {
    const { fileCount, logicalBytesOnDisk } = ctx.input.data;
    if (!fileCount || fileCount < SMALL_FILE_COUNT_THRESHOLD) return null;
    const avg = logicalBytesOnDisk / fileCount;
    if (avg >= SMALL_FILE_THRESHOLD) return null;

    return {
      severity: 'warning',
      title: 'Small files will dominate the read',
      message:
        `${fileCount.toLocaleString()} files averaging ${fmtBytes(avg)} each. Spark charges ` +
        'roughly 4 MiB of "open cost" per file when packing splits, so tiny files inflate the ' +
        'partition count far beyond what the data volume implies, and object-store listing ' +
        'becomes a bottleneck before any data is read. Compact these into files of a few ' +
        'hundred MiB -- OPTIMIZE on Delta, or a repartition-and-rewrite elsewhere.',
      evidence: [
        { label: 'File count', value: fileCount.toLocaleString() },
        { label: 'Average size', value: fmtBytes(avg) },
        { label: 'Input partitions', value: `${ctx.result.parallelism.inputPartitions}`, stepId: 'shuffle-partitions' },
      ],
      confidence: 'estimated',
      impact: 70,
    };
  },
};

const devEnvironmentOversized: Rule = {
  id: 'dev-environment-oversized',
  category: 'antipattern',
  defaultSeverity: 'warning',
  evaluate(ctx) {
    if (ctx.input.environment !== 'dev') return null;
    if (ctx.result.parallelism.totalCores <= 16) return null;
    return {
      severity: 'warning',
      title: 'That is a production-sized cluster for development',
      message:
        `This is marked as a development configuration but sizes to ` +
        `${ctx.result.parallelism.totalCores} cores across ${ctx.result.nodeCount} node(s). ` +
        'Development iterations are usually better served by a sample of the data on a small ' +
        'cluster, or local mode: you find the same bugs, in seconds rather than minutes, ' +
        'without the cost.',
      evidence: [
        { label: 'Environment', value: 'dev' },
        { label: 'Total cores', value: `${ctx.result.parallelism.totalCores}` },
      ],
      confidence: 'estimated',
      impact: 50,
    };
  },
};

export const sanityRules = (): Rule[] => [
  dataTooSmallForSpark,
  nonSplittableGzip,
  tooManySmallFiles,
  devEnvironmentOversized,
];

export { gib };
