import { describe, it, expect } from 'vitest';
import { computeRecommendation } from '../../src/engine';
import { gib, seconds, type Bytes } from '../../src/engine/units';
import type { WorkloadInput } from '../../src/engine/types/input';

const k8sJob = (overrides: Partial<WorkloadInput> = {}): WorkloadInput => ({
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
    shuffleStages: 3,
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
  ...overrides,
});

const withData = (bytes: Bytes, files: number) =>
  k8sJob({ data: { ...k8sJob().data, logicalBytesOnDisk: bytes, fileCount: files } });

describe('cluster sizing from data volume', () => {
  it('recommends a real node type and count', () => {
    const r = computeRecommendation(withData(gib(500), 5000)).primary;
    expect(r.nodeType).toBeDefined();
    expect(r.nodeCount).toBeGreaterThan(0);
    expect(r.executor.count).toBeGreaterThan(0);
    // A 500 GiB hourly job is a small cluster, not a data centre.
    expect(r.nodeCount).toBeLessThan(100);
  });

  it('scales sub-linearly with data volume', () => {
    const small = computeRecommendation(withData(gib(100), 1000)).primary;
    const large = computeRecommendation(withData(gib(2000), 20000)).primary;

    expect(large.nodeCount).toBeGreaterThan(small.nodeCount);
    // 20x the data must not mean 20x the nodes forever, but it should grow.
    const dataRatio = 20;
    const nodeRatio = large.nodeCount / small.nodeCount;
    expect(nodeRatio).toBeLessThanOrEqual(dataRatio * 1.2);
  });

  it('trades cluster size against the runtime target', () => {
    const fast = computeRecommendation(
      k8sJob({ sla: { kind: 'deadline', targetRuntime: seconds(600) } }),
    ).primary;
    const slow = computeRecommendation(
      k8sJob({ sla: { kind: 'deadline', targetRuntime: seconds(21600) } }),
    ).primary;

    // A 36x longer window must buy a materially smaller cluster.
    expect(fast.parallelism.totalCores).toBeGreaterThan(slow.parallelism.totalCores * 3);
    expect(slow.nodeCount).toBeLessThan(fast.nodeCount);
  });

  it('never recommends more cores than there are partitions', () => {
    // Beyond one partition per core, the extra cores have nothing to run.
    for (const size of [gib(10), gib(100), gib(1000)]) {
      const r = computeRecommendation(withData(size, 1000)).primary;
      expect(r.parallelism.totalCores).toBeLessThanOrEqual(
        r.parallelism.shufflePartitions,
      );
    }
  });

  it('keeps the container within node allocatable capacity', () => {
    // The property that matters most: an executor that does not fit is a pod
    // stuck Pending forever.
    for (const size of [gib(50), gib(500), gib(5000)]) {
      const r = computeRecommendation(withData(size, 5000)).primary;
      const perNode = r.executor.count / r.nodeCount;
      const node = r.nodeType!;
      expect(r.executor.containerTotalBytes * perNode).toBeLessThanOrEqual(node.memoryBytes);
      expect(r.executor.coresPerExecutor * perNode).toBeLessThanOrEqual(node.vcpus);
    }
  });

  it('accounts every byte of the container footprint', () => {
    const r = computeRecommendation(k8sJob()).primary;
    const e = r.executor;
    expect(e.containerTotalBytes).toBe(
      e.heapBytes + e.overheadBytes + e.offHeapBytes + e.pysparkMemoryBytes,
    );
  });

  it('uses the 0.4 overhead factor for PySpark UDF workloads', () => {
    const r = computeRecommendation(k8sJob({ runtimeLanguage: 'pyspark-udf' })).primary;
    expect(r.executor.overheadFactor).toBeGreaterThanOrEqual(0.4);
  });

  it('emits a Kubernetes config that names the recommended node', () => {
    const rec = computeRecommendation(k8sJob());
    const config = rec.primary.config;
    expect(config.kind).toBe('kubernetes');
    if (config.kind !== 'kubernetes') throw new Error('wrong config kind');
    expect(config.sparkApplicationYaml).toContain(rec.primary.nodeType!.id);
    expect(config.sparkConf.some((c) => c.key === 'spark.executor.instances')).toBe(true);
  });

  it('requires shuffle tracking when dynamic allocation is on', () => {
    const rec = computeRecommendation(k8sJob({
      platformInput: {
        platform: 'kubernetes',
        kubernetes: {
          reservePreset: 'gke', dynamicAllocation: true, shuffleTrackingEnabled: false,
          shuffleStorage: 'emptydir', setCpuLimit: false,
        },
      },
    }));
    expect(rec.findings.some((f) => f.ruleId === 'k8s-dynamic-allocation-no-shuffle-tracking'))
      .toBe(true);
  });

  it('produces no NaN or Infinity across a wide range of sizes', () => {
    for (const size of [gib(1), gib(50), gib(500), gib(5000), gib(50000)]) {
      const json = JSON.stringify(computeRecommendation(withData(size, 5000)));
      expect(json, `${size} bytes produced NaN`).not.toMatch(/NaN/);
      expect(json, `${size} bytes produced Infinity`).not.toMatch(/Infinity/);
    }
  });
});
