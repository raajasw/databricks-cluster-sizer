/**
 * s03: total data volume, peak resident working set, and cache demand.
 *
 * The critical distinction, and the one that ruins naive sizing models: Spark
 * does NOT hold the whole dataset in memory. It streams partition by partition,
 * so the memory that must actually exist at any instant is bounded by the
 * number of tasks running concurrently times the size of a partition -- not by
 * the total volume.
 *
 * Conflating the two is what produces absurd conclusions like "12 GB of Parquet
 * needs 190 GB of RAM". Total volume drives RUNTIME and shuffle/disk sizing;
 * concurrent residency drives MEMORY sizing. Both are reported, separately.
 */

import { bytes, type Bytes } from '../../units';
import { mulRange, scaleRange, sizingValue, point, type Range } from '../../range';
import { DATAFRAME_CACHE_FACTOR, RDD_CACHE_FACTOR } from '../../constants/inflation';
import { makeStep, fmtRange, fmtBytes } from '../trace';
import { withStep, withScratch, type NamedStage, type PipelineContext } from '../context';
import { INFLATED_BYTES } from './data-size';

export const WORKING_SET = 'workingSetBytes';
export const TOTAL_VOLUME = 'totalVolumeBytes';
export const CACHE_DEMAND = 'cacheDemandBytes';

export const workingSet: NamedStage = {
  name: 'workingSet',
  run(ctx: PipelineContext): PipelineContext {
    const inflated = ctx.draft.scratch[INFLATED_BYTES] as Range;
    const { cacheWorkingSetFraction, cachesViaDataFrameApi } = ctx.input.pipeline;

    // Peak RESIDENT memory: concurrent tasks x per-partition size, not the
    // whole dataset. Uses the seeded partition target since executor count is
    // not known yet; the convergence pass revisits this.
    const targetPartition = ctx.tunables.targetShufflePartitionBytes;
    const assumedConcurrentTasks = ctx.draft.scratch['assumedConcurrentTasks'] as number | undefined;
    const concurrency = assumedConcurrentTasks ?? 16;
    const amplification = (ctx.draft.scratch['queryAmplification'] as Range | undefined)
      ?? point(1);
    const resident = mulRange(
      scaleRange(point(targetPartition), concurrency),
      amplification,
    );

    // Caching a DataFrame does NOT store the inflated form: Spark SQL caches
    // into a compressed columnar InMemoryRelation, frequently SMALLER than the
    // deserialized size. Getting this backwards oversizes clusters badly.
    const cacheFactor = cachesViaDataFrameApi ? DATAFRAME_CACHE_FACTOR : RDD_CACHE_FACTOR;
    const cacheDemand = cacheWorkingSetFraction > 0
      ? mulRange(scaleRange(inflated, cacheWorkingSetFraction), cacheFactor)
      : point(0);

    const step = makeStep({
      id: 'working-set',
      title: 'Peak working set and cache demand',
      formula:
        `total volume through memory = ${fmtRange(inflated, 'bytes')}\n` +
        `peak resident = ${concurrency} concurrent tasks x ${fmtBytes(targetPartition)} ` +
        `per partition x ${amplification.mid.toFixed(1)}x query amplification ` +
        `= ${fmtRange(resident, 'bytes')}\n` +
        (cacheWorkingSetFraction > 0
          ? `cache demand = ${fmtRange(inflated, 'bytes')} x ${cacheWorkingSetFraction} cached ` +
            `x ${cachesViaDataFrameApi ? 'compressed columnar' : 'raw object'} factor ` +
            `= ${fmtRange(cacheDemand, 'bytes')}`
          : 'cache demand = 0 (nothing persisted)'),
      inputs: {
        inflatedBytes: inflated,
        cacheWorkingSetFraction,
        cachesViaDataFrameApi,
      },
      outputs: {
        totalVolumeBytes: inflated,
        residentWorkingSetBytes: resident,
        cacheDemandBytes: cacheDemand,
      },
      rationale:
        'Spark streams partition by partition rather than materializing the dataset, so the ' +
        'memory that must exist at any instant is set by how many tasks run at once times ' +
        'the size of one partition. Total volume determines how long the job takes and how ' +
        'much shuffle hits disk; it is not a memory requirement. ' +
        (cacheWorkingSetFraction > 0
        ? (cachesViaDataFrameApi
            ? 'Caching through the DataFrame API stores a compressed columnar ' +
              'InMemoryRelation, which is often smaller than the deserialized data rather ' +
              'than larger. Assuming otherwise is a common way to oversize a cluster.'
            : 'RDD-level caching holds live JVM objects, so it offers none of the compression ' +
              'a DataFrame cache gets.')
        : 'Nothing is persisted, so storage memory stays free for execution to borrow.'),
      confidence: 'estimated',
      citations: [
        { label: 'spark.sql.inMemoryColumnarStorage.compressed', kind: 'spark-config' },
      ],
    });

    return withScratch(withStep(ctx, step), {
      [WORKING_SET]: resident,
      [TOTAL_VOLUME]: inflated,
      [CACHE_DEMAND]: cacheDemand,
    });
  },
};

export { bytes, sizingValue, fmtBytes };
export type { Bytes };
