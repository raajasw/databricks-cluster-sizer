/**
 * Default form state and presets.
 */

import { gib, cores, seconds } from '../../engine/units';
import type { WorkloadInput } from '../../engine/types/input';

export const DEFAULT_INPUT: WorkloadInput = {
  profile: 'batch-etl',
  environment: 'prod',
  runtimeLanguage: 'pyspark-sql-only',
  sparkVersion: '3.5',
  aqeEnabled: true,
  data: {
    logicalBytesOnDisk: gib(500),
    format: 'parquet',
    codec: 'snappy',
    fileCount: 5000,
    columnMix: { numeric: 6, lowCardString: 2, highCardString: 1, nested: 1 },
  },
  pipeline: {
    dominantQueryShape: 'shuffle-join',
    shuffleStages: 2,
    cacheWorkingSetFraction: 0,
    cachesViaDataFrameApi: true,
  },
  sla: { kind: 'deadline', targetRuntime: seconds(3600) },
  cluster: { preferFewerLargerNodes: false, allowSpot: false },
  platformInput: {
    platform: 'kubernetes',
    kubernetes: {
      reservePreset: 'gke',
      dynamicAllocation: false,
      shuffleTrackingEnabled: false,
      shuffleStorage: 'emptydir',
      setCpuLimit: false,
    },
  },
};

const LOCAL_PLATFORM: WorkloadInput['platformInput'] = {
  platform: 'local',
  local: { machineCores: cores(10), machineMemory: gib(32), freeDiskBytes: gib(200) },
};

export interface Preset {
  name: string;
  description: string;
  input: WorkloadInput;
}

export const PRESETS: Preset[] = [
  {
    name: 'Daily 500 GB join',
    description: 'Sizes a cluster to finish inside an hour',
    input: {
      ...DEFAULT_INPUT,
      data: { ...DEFAULT_INPUT.data, logicalBytesOnDisk: gib(500), fileCount: 5000 },
    },
  },
  {
    name: '10 TB overnight',
    description: 'Same job, 6-hour window — far fewer nodes',
    input: {
      ...DEFAULT_INPUT,
      data: { ...DEFAULT_INPUT.data, logicalBytesOnDisk: gib(10000), fileCount: 40000 },
      sla: { kind: 'deadline', targetRuntime: seconds(21600) },
    },
  },
  {
    name: 'Small job on a laptop',
    description: '2 GB Parquet aggregation — should advise against Spark',
    input: {
      ...DEFAULT_INPUT,
      environment: 'dev',
      data: { ...DEFAULT_INPUT.data, logicalBytesOnDisk: gib(2), fileCount: 20 },
      pipeline: { ...DEFAULT_INPUT.pipeline, dominantQueryShape: 'aggregation', shuffleStages: 1 },
      platformInput: LOCAL_PLATFORM,
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
      platformInput: LOCAL_PLATFORM,
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
    description: '800 GB join with Python UDFs — overhead 0.4, not 0.1',
    input: {
      ...DEFAULT_INPUT,
      runtimeLanguage: 'pyspark-udf',
      data: {
        ...DEFAULT_INPUT.data,
        logicalBytesOnDisk: gib(800),
        fileCount: 8000,
        columnMix: { numeric: 3, lowCardString: 5, highCardString: 1, nested: 1 },
      },
      pipeline: { ...DEFAULT_INPUT.pipeline, shuffleStages: 3 },
    },
  },
  {
    name: 'Too big for one machine',
    description: '500 GB on a 32 GB laptop — should refuse',
    input: {
      ...DEFAULT_INPUT,
      data: { ...DEFAULT_INPUT.data, logicalBytesOnDisk: gib(500), fileCount: 5000 },
      platformInput: LOCAL_PLATFORM,
    },
  },
];
