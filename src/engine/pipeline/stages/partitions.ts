/**
 * s13-s16: input partitions, shuffle partitions with AQE, shuffle/spill
 * storage, and driver sizing.
 */

import { bytes, cores, mib, maxBytes, type Bytes } from '../../units';
import { point, divRange, mulRange, scaleRange, sizingValue, type Range } from '../../range';
import {
  MAX_PARTITION_BYTES_DEFAULT, OPEN_COST_IN_BYTES_DEFAULT,
  SHUFFLE_PARTITIONS_DEFAULT, DRIVER_MAX_RESULT_SIZE_DEFAULT,
} from '../../constants/spark-defaults';
import {
  SHUFFLE_DISK_SAFETY_FACTOR, DRIVER_CORES_FLOOR,
  DRIVER_TASK_COUNT_PRESSURE_THRESHOLD,
} from '../../constants/heuristics';
import {
  SHUFFLE_COMPRESSION_FACTOR, SPILL_WRITE_AMPLIFICATION, CODEC_IS_SPLITTABLE,
} from '../../constants/inflation';
import type { ParallelismSpec, StorageSpec, DriverSpec } from '../../types/output';
import { makeStep, fmtBytes, fmtRange } from '../trace';
import { withStep, withScratch, type NamedStage, type PipelineContext } from '../context';
import { SCANNED_BYTES, INFLATED_BYTES } from './data-size';
import { PER_TASK_EXECUTION } from './memory';
import { computeSpillPressure } from './memory';

export const PARALLELISM = 'parallelism';
export const STORAGE = 'storage';
export const DRIVER = 'driver';

/** s13 + s14: partition counts, and the AQE branch. */
export const partitioning: NamedStage = {
  name: 'partitioning',
  run(ctx: PipelineContext): PipelineContext {
    const { coresPerExecutor: cpe, executorCount } = ctx.draft;
    if (!cpe || !executorCount) throw new Error('partitioning missing prerequisites');

    const scanned = ctx.draft.scratch[SCANNED_BYTES] as Bytes;
    const inflated = ctx.draft.scratch[INFLATED_BYTES] as Range;
    const perTask = ctx.draft.scratch[PER_TASK_EXECUTION] as Bytes;

    const totalCores = cores(executorCount * cpe);
    const totalTaskSlots = executorCount * cpe;

    // Input partitions are sized against ON-DISK bytes: maxPartitionBytes is
    // measured against file bytes, and each file costs at least openCostInBytes
    // in the split packing -- which is why many tiny files produce far more
    // partitions than totalBytes/maxPartitionBytes suggests.
    const maxPartitionBytes = ctx.tunables.targetInputPartitionBytes;
    const fileCount = ctx.input.data.fileCount ?? 0;
    const paddedBytes = fileCount > 0
      ? scanned + fileCount * OPEN_COST_IN_BYTES_DEFAULT
      : scanned;
    const splittable = CODEC_IS_SPLITTABLE[ctx.input.data.codec];

    let inputPartitions = Math.max(1, Math.ceil(paddedBytes / maxPartitionBytes));
    let splitNote = '';
    if (!splittable && fileCount > 0) {
      // A non-splittable codec pins each file to exactly one task.
      inputPartitions = fileCount;
      splitNote =
        `${ctx.input.data.codec} is not splittable, so each of the ${fileCount} files is read ` +
        'by exactly one task regardless of size or cluster capacity.';
    } else if (!splittable) {
      inputPartitions = 1;
      splitNote =
        `${ctx.input.data.codec} is not splittable, so the entire input is read by ONE task. ` +
        'Cluster size is irrelevant until this is re-encoded.';
    }

    // Shuffle partitions: with AQE on, a static count is the wrong knob.
    /*
     * Partition count is governed by what fits in a TASK SLOT, not by a wave
     * target.
     *
     * A partition has to be processed by one task, in that task's share of
     * execution memory. Capping the partition count to hold waves down just
     * makes each partition bigger than a slot can hold, which trades a high
     * wave count for guaranteed spill -- strictly worse, since spill costs
     * 2-10x while extra waves cost only scheduling overhead.
     *
     * So size partitions to fit, and let the wave count be whatever it is. A
     * high wave count is then a true signal that the cluster is small for the
     * work, which the rules report honestly rather than hiding.
     */
    const targetShuffleBytes = ctx.tunables.targetShufflePartitionBytes;
    const amplificationForFit =
      (ctx.draft.scratch['queryAmplification'] as Range | undefined)?.mid ?? 1;

    // Leave headroom: a task needs room for its partition AND the operator
    // structures built on top of it.
    const fitsInSlot = Math.max(
      mib(16),
      (perTask * 0.7) / amplificationForFit,
    );
    const effectiveTarget = Math.min(targetShuffleBytes, fitsInSlot);

    const computed = Math.max(1, Math.ceil(sizingValue(inflated) / effectiveTarget));
    const partitionsCapped = effectiveTarget < targetShuffleBytes;
    const effectivePartitionBytes = sizingValue(inflated) / computed;

    let shufflePartitions: number;
    let aqeNote: string;
    if (ctx.input.aqeEnabled) {
      // Set high and let AQE coalesce down; round to a multiple of slots.
      shufflePartitions = Math.max(
        totalTaskSlots,
        Math.ceil(computed / totalTaskSlots) * totalTaskSlots,
      );
      aqeNote =
        'AQE is on, so this acts as an upper bound rather than a fixed count: Spark coalesces ' +
        'post-shuffle partitions at runtime using the advisory size below. Pinning a low ' +
        'static value here fights AQE rather than helping it.';
    } else {
      shufflePartitions = Math.max(
        totalTaskSlots,
        Math.ceil(computed / totalTaskSlots) * totalTaskSlots,
      );
      aqeNote =
        'AQE is off, so this static count is what you get. It is rounded up to a multiple of ' +
        `${totalTaskSlots} task slots so the final wave does not leave most cores idle.`;
    }

    const pinned = ctx.input.cluster.pinnedShufflePartitions;
    if (pinned) shufflePartitions = pinned;

    const cappedNote = partitionsCapped
      ? ` Partitions are sized at ${fmtBytes(effectiveTarget)} rather than the usual ` +
        `${fmtBytes(targetShuffleBytes)}, because that is what fits in one task slot's ` +
        `${fmtBytes(perTask)} of execution memory once query amplification is allowed for. ` +
        'Bigger partitions would spill, and spill costs far more than the extra waves.'
      : '';

    const advisoryBytes = ctx.tunables.advisoryPartitionBytes;
    const expectedPostAqe = ctx.input.aqeEnabled
      ? divRange(inflated, point(advisoryBytes))
      : undefined;

    const waves = divRange(point(shufflePartitions), point(totalTaskSlots));

    const parallelism: ParallelismSpec = {
      totalCores,
      totalTaskSlots,
      inputPartitions,
      targetInputPartitionBytes: maxPartitionBytes,
      shufflePartitions,
      aqeAdvisoryPartitionBytes: advisoryBytes,
      expectedPostAqePartitions: expectedPostAqe,
      wavesPerStage: waves,
      defaultParallelism: totalTaskSlots,
    };

    const step = makeStep({
      id: 'shuffle-partitions',
      title: 'Partitions and parallelism',
      formula:
        `task slots = ${executorCount} executors x ${cpe} cores = ${totalTaskSlots}\n` +
        `input partitions = ceil((${fmtBytes(scanned)}` +
        (fileCount > 0 ? ` + ${fileCount} files x ${fmtBytes(OPEN_COST_IN_BYTES_DEFAULT)} open cost` : '') +
        `) / ${fmtBytes(maxPartitionBytes)}) = ${inputPartitions}\n` +
        `shuffle partitions = ${shufflePartitions} (default is ${SHUFFLE_PARTITIONS_DEFAULT})\n` +
        `waves = ${shufflePartitions} / ${totalTaskSlots} = ${fmtRange(waves)}`,
      inputs: {
        scannedBytes: scanned,
        inflatedBytes: inflated,
        fileCount,
        totalTaskSlots,
        aqeEnabled: ctx.input.aqeEnabled,
      },
      outputs: {
        inputPartitions, shufflePartitions, totalTaskSlots,
        waves, advisoryPartitionBytes: advisoryBytes,
        effectivePartitionBytes, partitionsCapped,
      },
      rationale:
        (`${aqeNote}${cappedNote} ${splitNote}`).trim() +
        ' All of this assumes data spread evenly across partitions. Real keys rarely are, ' +
        'and a skewed partition will be larger and slower than the average shown here -- but ' +
        'by how much depends on your key distribution, which cannot be inferred from volume ' +
        'or format. Size for the even case, then look at the max-versus-median task duration ' +
        'in the Spark UI after a run and tune from what you actually see.',
      confidence: 'estimated',
      citations: [
        { label: 'spark.sql.shuffle.partitions', kind: 'spark-config' },
        { label: 'spark.sql.files.maxPartitionBytes', kind: 'spark-config' },
        { label: 'spark.sql.adaptive.advisoryPartitionSizeInBytes', kind: 'spark-config' },
      ],
      alternatives: [{
        value: SHUFFLE_PARTITIONS_DEFAULT,
        whyRejected:
          `The ${SHUFFLE_PARTITIONS_DEFAULT} default would put ` +
          `${fmtBytes(sizingValue(inflated) / SHUFFLE_PARTITIONS_DEFAULT)} in each partition ` +
          `against ${fmtBytes(perTask)} of execution memory per task slot.`,
      }],
    });

    return withScratch(withStep(ctx, step), { [PARALLELISM]: parallelism });
  },
};

/** s15: shuffle write volume, spill prediction, and local disk need. */
export const shuffleStorage: NamedStage = {
  name: 'shuffleStorage',
  run(ctx: PipelineContext): PipelineContext {
    const inflated = ctx.draft.scratch[INFLATED_BYTES] as Range;
    const perTask = ctx.draft.scratch[PER_TASK_EXECUTION] as Bytes;
    const parallelism = ctx.draft.scratch[PARALLELISM] as ParallelismSpec;
    const executorCount = ctx.draft.executorCount ?? 1;

    const shuffleStages = Math.max(0, ctx.input.pipeline.shuffleStages);
    const shuffleWrite = shuffleStages > 0
      ? mulRange(inflated, SHUFFLE_COMPRESSION_FACTOR)
      : point(0);

    // Concurrent stages retaining shuffle output: usually 1, 2 for a wide DAG.
    const peakOnDisk = scaleRange(shuffleWrite, shuffleStages > 2 ? 2 : 1);

    // Per-partition demand against per-task execution memory decides spill.
    // Query amplification belongs HERE: a sort-merge join holds sort buffers
    // for its partition on top of the partition itself.
    const amplification = (ctx.draft.scratch['queryAmplification'] as Range | undefined)
      ?? point(1);
    const perPartition = mulRange(
      divRange(inflated, point(parallelism.shufflePartitions)),
      amplification,
    );
    const { ratio, predicted } = computeSpillPressure(perPartition, perTask);

    const spillBytes = predicted
      ? mulRange(
          scaleRange(perPartition, parallelism.totalTaskSlots),
          SPILL_WRITE_AMPLIFICATION,
        )
      : point(0);

    const perExecutorDisk = bytes(
      ((sizingValue(peakOnDisk) + sizingValue(spillBytes)) / executorCount) *
        SHUFFLE_DISK_SAFETY_FACTOR,
    );

    const available = ctx.input.platformInput.local?.freeDiskBytes
      ?? ctx.draft.nodeType?.localSsdBytes;

    const storage: StorageSpec = {
      shuffleWriteBytesPerStage: shuffleWrite,
      peakShuffleOnDiskBytes: peakOnDisk,
      spillEstimateBytes: spillBytes,
      spillPredicted: predicted,
      spillPressureRatio: ratio,
      requiredLocalDiskPerExecutorBytes: perExecutorDisk,
      availableLocalDiskPerExecutorBytes: available,
      hasEnoughLocalDisk: available === undefined || available >= perExecutorDisk,
      localDirStrategy:
        'Point spark.local.dir at fast local storage with real free space. On nodes with ' +
        'several NVMe devices, list them comma-separated so Spark round-robins across them.',
    };

    const step = makeStep({
      id: 'shuffle-storage',
      title: 'Shuffle, spill and local disk',
      formula:
        `shuffle write = ${fmtRange(inflated, 'bytes')} x compression ` +
        `= ${fmtRange(shuffleWrite, 'bytes')}\n` +
        `per partition = ${fmtRange(inflated, 'bytes')} / ${parallelism.shufflePartitions} ` +
        `x ${amplification.mid.toFixed(1)}x amplification = ${fmtRange(perPartition, 'bytes')}\n` +
        `spill pressure = ${fmtRange(perPartition, 'bytes')} / ${fmtBytes(perTask)} per task slot ` +
        `= ${fmtRange(ratio)}x\n` +
        `disk per executor = ${fmtBytes(perExecutorDisk)}`,
      inputs: {
        inflatedBytes: inflated,
        shufflePartitions: parallelism.shufflePartitions,
        perTaskExecution: perTask,
        shuffleStages,
      },
      outputs: {
        shuffleWriteBytes: shuffleWrite,
        spillPressureRatio: ratio,
        spillPredicted: predicted,
        requiredDiskPerExecutor: perExecutorDisk,
      },
      rationale:
        predicted
          ? 'Each task needs more memory for its partition than its share of the execution ' +
            'pool provides, so the sort and aggregate operators will spill to disk. Spill is ' +
            'Spark working as designed rather than a failure, but it typically costs 2-10x in ' +
            'wall time, and most people paying it do not know they are.'
          : 'Each partition fits within a task slot\'s share of execution memory, so operators ' +
            'should complete in memory without spilling.',
      confidence: 'estimated',
      citations: [{ label: 'spark.shuffle.compress', kind: 'spark-config' }],
    });

    return withScratch(withStep(ctx, step), { [STORAGE]: storage });
  },
};

/** s16: driver sizing. Drivers are not small executors. */
export const driverSizing: NamedStage = {
  name: 'driverSizing',
  run(ctx: PipelineContext): PipelineContext {
    const parallelism = ctx.draft.scratch[PARALLELISM] as ParallelismSpec;
    const isLocal = ctx.adapter.id === 'local';

    const collectBytes = ctx.input.pipeline.driverCollectBytes ?? bytes(0);
    const broadcastBytes = ctx.input.pipeline.largestJoinBuildSideBytes ?? bytes(0);
    const totalTasks = parallelism.shufflePartitions * Math.max(1, ctx.input.pipeline.shuffleStages);
    const taskBookkeeping = totalTasks > DRIVER_TASK_COUNT_PRESSURE_THRESHOLD
      ? mib(Math.ceil(totalTasks / 10000))
      : mib(256);

    let heap = maxBytes(
      ctx.tunables.driverMemoryFloor,
      bytes(collectBytes * 1.5 + broadcastBytes * 2 + taskBookkeeping),
    );

    if (isLocal) {
      // The driver JVM IS the executor here, so report the one heap that
      // actually exists rather than inventing a second figure.
      heap = ctx.draft.heapBytes ?? heap;
    }

    const maxResultSize = maxBytes(
      DRIVER_MAX_RESULT_SIZE_DEFAULT,
      bytes(collectBytes * 1.2),
    );
    const overhead = maxBytes(mib(384), bytes(heap * 0.1));

    const driver: DriverSpec = {
      cores: cores(isLocal ? (ctx.draft.coresPerExecutor ?? 2) : DRIVER_CORES_FLOOR),
      heapBytes: heap,
      overheadBytes: overhead,
      maxResultSizeBytes: maxResultSize,
      containerTotalBytes: bytes(heap + overhead),
      onSeparateNode: ctx.adapter.capabilities.hasSeparateDriverNode,
      nodeTypeId: ctx.draft.nodeType?.id,
    };

    const step = makeStep({
      id: 'driver-sizing',
      title: 'Driver',
      formula: isLocal
        ? `local mode: one JVM, so driver heap = executor heap = ${fmtBytes(heap)}`
        : `heap = max(${fmtBytes(ctx.tunables.driverMemoryFloor)} floor, ` +
          `${fmtBytes(collectBytes)} collect x 1.5 + ${fmtBytes(broadcastBytes)} broadcast x 2 ` +
          `+ ${fmtBytes(taskBookkeeping)} task bookkeeping) = ${fmtBytes(heap)}`,
      inputs: { collectBytes, broadcastBytes, totalTasks, isLocal },
      outputs: {
        driverHeap: heap, driverCores: driver.cores, maxResultSize,
      },
      rationale: isLocal
        ? 'In local mode the driver JVM also runs every task, so there is exactly one heap. ' +
          'spark.driver.memory is the only memory setting that has any effect.'
        : 'A driver is not a small executor. It is sized by what actually lands there: ' +
          'collected results, the build side of broadcast joins materialized before ' +
          'broadcasting, and DAG bookkeeping that scales with total task count. ' +
          `maxResultSize is set to ${fmtBytes(maxResultSize)} rather than left unlimited -- ` +
          'setting it to 0 removes the one guardrail against a collect() killing the driver.',
      confidence: 'estimated',
      citations: [
        { label: 'spark.driver.maxResultSize', kind: 'spark-config' },
        { label: 'spark.sql.autoBroadcastJoinThreshold', kind: 'spark-config' },
      ],
    });

    return withScratch(withStep(ctx, step), { [DRIVER]: driver });
  },
};

export { MAX_PARTITION_BYTES_DEFAULT };
