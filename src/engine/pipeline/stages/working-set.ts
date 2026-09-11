/**
 * s03: peak working set and cache demand.
 *
 * These are distinct and routinely conflated. The working set is what is live
 * in execution memory at peak; the cache demand is what a persist() asks
 * storage to hold.
 */

import { bytes, type Bytes } from '../../units';
import { mulRange, scaleRange, sizingValue, point, type Range } from '../../range';
import { DATAFRAME_CACHE_FACTOR, RDD_CACHE_FACTOR } from '../../constants/inflation';
import { makeStep, fmtRange, fmtBytes } from '../trace';
import { withStep, withScratch, type NamedStage, type PipelineContext } from '../context';
import { INFLATED_BYTES } from './data-size';

export const WORKING_SET = 'workingSetBytes';
export const CACHE_DEMAND = 'cacheDemandBytes';

export const workingSet: NamedStage = {
  name: 'workingSet',
  run(ctx: PipelineContext): PipelineContext {
    const inflated = ctx.draft.scratch[INFLATED_BYTES] as Range;
    const { cacheWorkingSetFraction, cachesViaDataFrameApi } = ctx.input.pipeline;

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
        `working set = ${fmtRange(inflated, 'bytes')}\n` +
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
      outputs: { workingSetBytes: inflated, cacheDemandBytes: cacheDemand },
      rationale: cacheWorkingSetFraction > 0
        ? (cachesViaDataFrameApi
            ? 'Caching through the DataFrame API stores a compressed columnar ' +
              'InMemoryRelation, which is often smaller than the deserialized data rather ' +
              'than larger. Assuming otherwise is a common way to oversize a cluster.'
            : 'RDD-level caching holds live JVM objects, so it offers none of the compression ' +
              'a DataFrame cache gets.')
        : 'Nothing is persisted, so storage memory stays free for execution to borrow.',
      confidence: 'estimated',
      citations: [
        { label: 'spark.sql.inMemoryColumnarStorage.compressed', kind: 'spark-config' },
      ],
    });

    return withScratch(withStep(ctx, step), {
      [WORKING_SET]: inflated,
      [CACHE_DEMAND]: cacheDemand,
    });
  },
};

export { bytes, sizingValue, fmtBytes };
export type { Bytes };
