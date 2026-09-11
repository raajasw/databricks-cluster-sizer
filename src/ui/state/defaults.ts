/**
 * Default form state and presets.
 */

import { gib, cores } from '../../engine/units';
import type { WorkloadInput } from '../../engine/types/input';

export const DEFAULT_INPUT: WorkloadInput = {
  profile: 'batch-etl',
  environment: 'prod',
  runtimeLanguage: 'pyspark-sql-only',
  sparkVersion: '3.5',
  aqeEnabled: true,
  data: {
    logicalBytesOnDisk: gib(12),
    format: 'parquet',
    codec: 'snappy',
    fileCount: 240,
    columnMix: { numeric: 6, lowCardString: 2, highCardString: 1, nested: 1 },
  },
  pipeline: {
    dominantQueryShape: 'shuffle-join',
    shuffleStages: 2,
    cacheWorkingSetFraction: 0,
    cachesViaDataFrameApi: true,
  },
  sla: { kind: 'best-effort' },
  cluster: { preferFewerLargerNodes: false, allowSpot: false },
  platformInput: {
    platform: 'local',
    local: { machineCores: cores(10), machineMemory: gib(32), freeDiskBytes: gib(200) },
  },
};

export interface Preset {
  name: string;
  description: string;
  input: WorkloadInput;
}

export const PRESETS: Preset[] = [
  {
    name: 'Small job on a laptop',
    description: '2 GB Parquet aggregation — should advise against Spark',
    input: {
      ...DEFAULT_INPUT,
      environment: 'dev',
      data: { ...DEFAULT_INPUT.data, logicalBytesOnDisk: gib(2), fileCount: 20 },
      pipeline: { ...DEFAULT_INPUT.pipeline, dominantQueryShape: 'aggregation', shuffleStages: 1 },
    },
  },
  {
    name: 'The gzip trap',
    description: '40 GB of gzipped CSV in 2 files — single-threaded read',
    input: {
      ...DEFAULT_INPUT,
      data: {
        logicalBytesOnDisk: gib(40), format: 'csv', codec: 'gzip', fileCount: 2,
      },
      pipeline: { ...DEFAULT_INPUT.pipeline, dominantQueryShape: 'scan-filter-write', shuffleStages: 0 },
    },
  },
  {
    name: 'Death by small files',
    description: '250,000 tiny Parquet files',
    input: {
      ...DEFAULT_INPUT,
      data: { ...DEFAULT_INPUT.data, logicalBytesOnDisk: gib(60), fileCount: 250_000 },
    },
  },
  {
    name: 'PySpark UDF join',
    description: '60 GB join with Python UDFs on a big workstation',
    input: {
      ...DEFAULT_INPUT,
      runtimeLanguage: 'pyspark-udf',
      data: {
        ...DEFAULT_INPUT.data,
        logicalBytesOnDisk: gib(60),
        fileCount: 1200,
        columnMix: { numeric: 3, lowCardString: 5, highCardString: 1, nested: 1 },
      },
      pipeline: {
        ...DEFAULT_INPUT.pipeline, dominantQueryShape: 'shuffle-join',
        shuffleStages: 3,
      },
      platformInput: {
        platform: 'local',
        local: { machineCores: cores(32), machineMemory: gib(256), freeDiskBytes: gib(2000) },
      },
    },
  },
  {
    name: 'Too big for one machine',
    description: '500 GB on a 32 GB laptop — should refuse',
    input: {
      ...DEFAULT_INPUT,
      data: { ...DEFAULT_INPUT.data, logicalBytesOnDisk: gib(500), fileCount: 5000 },
    },
  },
];
