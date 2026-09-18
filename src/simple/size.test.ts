import { describe, it, expect } from 'vitest';
import { size, pickShape, type Answers } from './size';
import { nodesFor } from './nodes';

const GB = 1e9;
const TB = 1e12;

const base: Answers = {
  cloud: 'aws',
  dataBytes: 500 * GB,
  format: 'parquet',
  language: 'python-sql',
  operation: 'join',
  targetMinutes: 60,
};

const ask = (o: Partial<Answers> = {}): Answers => ({ ...base, ...o });

describe('the levers actually move the answer', () => {
  it('a shorter deadline buys more workers', () => {
    const hour = size(ask({ targetMinutes: 60 }));
    const quarter = size(ask({ targetMinutes: 15 }));
    expect(quarter.workers).toBeGreaterThan(hour.workers * 2);
  });

  it('more data needs more workers', () => {
    expect(size(ask({ dataBytes: 2 * TB })).workers)
      .toBeGreaterThan(size(ask({ dataBytes: 500 * GB })).workers);
  });

  it('Python UDFs cost several times more hardware than SQL', () => {
    const sql = size(ask({ language: 'sql' }));
    const udf = size(ask({ language: 'python-udf' }));
    // The JVM-to-Python round trip per row is the dominant cost in such a job.
    expect(udf.workers).toBeGreaterThan(sql.workers * 4);
  });

  it('text formats cost more than columnar for the same bytes', () => {
    expect(size(ask({ format: 'csv' })).workers)
      .toBeGreaterThan(size(ask({ format: 'parquet' })).workers);
  });

  it('joins need more than scans', () => {
    expect(size(ask({ operation: 'join' })).workers)
      .toBeGreaterThan(size(ask({ operation: 'scan' })).workers);
  });
});

describe('node selection', () => {
  it('picks storage-optimized for shuffling work, per Databricks guidance', () => {
    // Shuffle is disk- and network-bound before it is memory-bound.
    for (const operation of ['join', 'sort', 'aggregate'] as const) {
      expect(size(ask({ cloud: 'aws', operation })).node.shape).toBe('storage');
    }
  });

  it('falls back to memory-optimized where a cloud has no storage tier', () => {
    /*
     * Exercises pickShape's fallback directly rather than through a cloud that
     * happens to lack the tier today. GCP used to be that cloud; adding c3
     * local-SSD nodes to the catalogue gave it a storage tier and quietly
     * stopped this test checking the fallback at all. Constructing the case
     * keeps it testing the branch it is named after.
     */
    const noStorage = nodesFor('gcp').filter((x) => x.shape !== 'storage');
    expect(noStorage.some((x) => x.shape === 'memory')).toBe(true);
    expect(pickShape({ ...base, operation: 'join' }, ['balanced', 'memory']).shape)
      .toBe('memory');
  });

  it('picks balanced for a plain scan', () => {
    expect(size(ask({ operation: 'scan' })).node.shape).toBe('balanced');
  });

  it('only ever recommends a node from the chosen cloud', () => {
    for (const cloud of ['aws', 'azure', 'gcp'] as const) {
      for (const operation of ['scan', 'aggregate', 'join', 'sort'] as const) {
        const r = size(ask({ cloud, operation }));
        expect(r.node.cloud, `${cloud}/${operation} picked a ${r.node.cloud} node`).toBe(cloud);
      }
    }
  });

  it('honours an override and says so', () => {
    const r = size(ask({ nodeTypeId: 'i3.2xlarge' }));
    expect(r.node.id).toBe('i3.2xlarge');
    expect(r.nodeReason).toMatch(/you chose/i);
  });

  it('ignores an override from the wrong cloud rather than using it', () => {
    // Standard_* is Azure; on AWS it must fall back to a real AWS node.
    const r = size(ask({ cloud: 'aws', nodeTypeId: 'Standard_D8ds_v5' }));
    expect(r.node.cloud).toBe('aws');
  });
});

describe('the answer is always usable', () => {
  it('never returns zero or fractional workers', () => {
    const sizes = [1 * GB, 100 * GB, 10 * TB];
    const targets = [5, 60, 480];
    for (const dataBytes of sizes) {
      for (const targetMinutes of targets) {
        const r = size(ask({ dataBytes, targetMinutes }));
        expect(Number.isInteger(r.workers)).toBe(true);
        expect(r.workers).toBeGreaterThanOrEqual(1);
        expect(r.totalCores).toBeGreaterThanOrEqual(1);
      }
    }
  });

  it('produces no NaN or Infinity for any combination', () => {
    for (const cloud of ['aws', 'azure', 'gcp'] as const) {
      for (const format of ['parquet', 'delta', 'csv', 'json', 'avro'] as const) {
        for (const language of ['sql', 'scala', 'python-sql', 'python-udf'] as const) {
          for (const operation of ['scan', 'aggregate', 'join', 'sort'] as const) {
            const json = JSON.stringify(size(ask({ cloud, format, language, operation })));
            expect(json).not.toMatch(/NaN|Infinity|null/);
          }
        }
      }
    }
  });

  it('estimates a runtime near the target it was asked for', () => {
    // Sized for 60 minutes, it should land within a factor of two, or the
    // arithmetic is not self-consistent.
    const r = size(ask({ targetMinutes: 60 }));
    expect(r.estimatedMinutes).toBeGreaterThan(25);
    expect(r.estimatedMinutes).toBeLessThan(90);
  });

  it('quotes runtime as a span, never a single figure', () => {
    const r = size(ask());
    const [lo, hi] = r.estimatedRangeMinutes;
    expect(lo).toBeLessThan(r.estimatedMinutes);
    expect(hi).toBeGreaterThan(r.estimatedMinutes);
  });

  it('matches the audited TPC-DS benchmark on the hardware it was measured on', () => {
    /*
     * The anchor for every throughput constant in this tool.
     *
     * The TPC-DS 100 TB Full Disclosure Report (Databricks SQL 8.3, audited by
     * the TPC council, Nov 2021) loaded 100 TB on 256 x i3.2xlarge - 2,048
     * vCPU - in 7,929 seconds, which is 6.2 MB per core-second. The FDR names
     * the engine as "Databricks Photon Engine 8.3" and the processor as a Xeon
     * E5-2686 v4, so that figure is WITH Photon, on 2016 Broadwell cores.
     *
     * The comparison is therefore pinned to those same conditions: Photon on,
     * i3.2xlarge forced. Anything else is comparing against different hardware
     * and a different engine, which is what made an earlier version of this
     * test assert the wrong direction - it expected MORE than 2,048 cores on
     * the theory that the tool modelled the JVM engine, when the anchor it
     * compares against had Photon in it all along.
     *
     * The tool should still ask for somewhat more than the audit used: a bulk
     * load is lighter work than the shuffling queries this tool sizes for, and
     * the base rates are deliberately set toward the pessimistic end.
     *
     * If a change to the constants breaks this, re-derive it from the source
     * rather than widening the bounds to fit.
     */
    const auditedCores = 2048;
    const auditedMinutes = 7929 / 60;

    const r = size(ask({
      dataBytes: 100 * TB, format: 'parquet', language: 'sql',
      operation: 'scan', targetMinutes: auditedMinutes,
      photon: true, nodeTypeId: 'i3.2xlarge',
    }));

    expect(r.node.cpuFactor).toBe(1);
    expect(r.totalCores).toBeGreaterThan(auditedCores);
    expect(r.totalCores).toBeLessThan(auditedCores * 10);
  });

  it('needs fewer cores on modern silicon than on the anchor hardware', () => {
    // The whole point of the per-node CPU factor: an Ice Lake core does more
    // than the 2016 Broadwell core the rate was calibrated on, so the same job
    // needs fewer of them. Treating all cores as equal over-provisions.
    const common = {
      dataBytes: 10 * TB, format: 'parquet', language: 'sql',
      operation: 'scan', targetMinutes: 60, photon: true,
    } as const;

    const old = size(ask({ ...common, nodeTypeId: 'i3.2xlarge' }));
    const modern = size(ask({ ...common, nodeTypeId: 'i4i.2xlarge' }));

    expect(modern.node.cpuFactor).toBeGreaterThan(old.node.cpuFactor);
    expect(modern.totalCores).toBeLessThan(old.totalCores);
  });

  it('sizes a non-Photon cluster larger, and says why', () => {
    // The anchor includes Photon, so turning it off is a penalty. Sized at
    // NON_PHOTON_SLOWDOWN, this should be a clear step up, not a rounding.
    const on = size(ask({ language: 'sql', photon: true }));
    const off = size(ask({ language: 'sql', photon: false }));

    expect(off.totalCores).toBeGreaterThan(on.totalCores);
    expect(off.notes.join(' ')).toMatch(/Photon is off/i);
  });

  it('does not charge the Photon penalty twice for a Python UDF', () => {
    // A UDF already runs outside the JVM at its own much lower rate, and
    // cannot use Photon either way. Toggling Photon must not change its size,
    // or the same slowdown is being counted twice.
    const on = size(ask({ language: 'python-udf', photon: true }));
    const off = size(ask({ language: 'python-udf', photon: false }));

    expect(off.totalCores).toBe(on.totalCores);
    expect(on.notes.join(' ')).toMatch(/Photon cannot run a Python UDF/i);
  });
});

describe('it warns about what matters', () => {
  it('calls out Python UDFs as the reason for a big cluster', () => {
    const r = size(ask({ language: 'python-udf' }));
    expect(r.notes.join(' ')).toMatch(/Python UDF/i);
  });

  it('suggests converting CSV when it is the source', () => {
    expect(size(ask({ format: 'csv' })).notes.join(' ')).toMatch(/Parquet|Delta/i);
  });

  it('says so when the job is too small to need Spark', () => {
    const r = size(ask({ dataBytes: 2 * GB, operation: 'scan', targetMinutes: 60 }));
    expect(r.workers).toBe(1);
    expect(r.notes.join(' ')).toMatch(/DuckDB|pandas|single machine/i);
  });

  it('raises the overhead factor for Python UDF jobs', () => {
    const keys = size(ask({ language: 'python-udf' })).config.map((c) => c.key);
    expect(keys).toContain('spark.executor.memoryOverheadFactor');
  });

  it('always recommends a shuffle partition count instead of the 200 default', () => {
    const c = size(ask()).config.find((x) => x.key === 'spark.sql.shuffle.partitions');
    expect(c).toBeDefined();
    expect(Number(c!.value)).toBeGreaterThan(0);
  });
});
