/**
 * Named scenarios used by both golden tests and the UI presets.
 */

import { gib, mib, cores, seconds } from '../../src/engine/units';
import type { WorkloadInput } from '../../src/engine/types/input';

export const laptopSmallData: WorkloadInput = {
  profile: 'batch-etl',
  environment: 'dev',
  runtimeLanguage: 'pyspark-sql-only',
  sparkVersion: '3.5',
  aqeEnabled: true,
  data: {
    logicalBytesOnDisk: gib(2),
    format: 'parquet',
    codec: 'snappy',
    fileCount: 20,
    columnMix: { numeric: 6, lowCardString: 2, highCardString: 1, nested: 1 },
  },
  pipeline: {
    dominantQueryShape: 'aggregation',
    shuffleStages: 1,
    cacheWorkingSetFraction: 0,
    cachesViaDataFrameApi: true,
  },
  sla: { kind: 'best-effort' },
  cluster: { preferFewerLargerNodes: false, allowSpot: false },
  platformInput: {
    platform: 'local',
    local: { machineCores: cores(10), machineMemory: gib(32), freeDiskBytes: gib(100) },
  },
};

export const laptopBigJoin: WorkloadInput = {
  ...laptopSmallData,
  environment: 'prod',
  runtimeLanguage: 'pyspark-udf',
  data: {
    logicalBytesOnDisk: gib(80),
    format: 'parquet',
    codec: 'snappy',
    fileCount: 400,
    columnMix: { numeric: 3, lowCardString: 5, highCardString: 1, nested: 1 },
  },
  pipeline: {
    dominantQueryShape: 'shuffle-join',
    shuffleStages: 3,
    cacheWorkingSetFraction: 0.3,
    cachesViaDataFrameApi: true,
  },
  sla: { kind: 'deadline', targetRuntime: seconds(1800) },
};

export const gzipTrap: WorkloadInput = {
  ...laptopSmallData,
  environment: 'prod',
  data: {
    logicalBytesOnDisk: gib(40),
    format: 'csv',
    codec: 'gzip',
    fileCount: 2,
  },
  pipeline: {
    dominantQueryShape: 'scan-filter-write',
    shuffleStages: 0,
    cacheWorkingSetFraction: 0,
    cachesViaDataFrameApi: true,
  },
};

export const smallFiles: WorkloadInput = {
  ...laptopSmallData,
  environment: 'prod',
  data: {
    logicalBytesOnDisk: gib(60),
    format: 'parquet',
    codec: 'snappy',
    fileCount: 250_000,
    columnMix: { numeric: 8, lowCardString: 1, highCardString: 1, nested: 0 },
  },
};

export const SCENARIOS = {
  'laptop-small-data': laptopSmallData,
  'laptop-big-join': laptopBigJoin,
  'gzip-trap': gzipTrap,
  'small-files': smallFiles,
} as const;

export { mib };
