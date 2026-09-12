import { describe, it, expect } from 'vitest';
import { size, type Answers } from './size';

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
  it('picks memory-optimized for joins and sorts', () => {
    expect(size(ask({ operation: 'join' })).node.shape).toBe('memory');
    expect(size(ask({ operation: 'sort' })).node.shape).toBe('memory');
  });

  it('does not pick memory-optimized for a plain scan', () => {
    expect(size(ask({ operation: 'scan' })).node.shape).not.toBe('memory');
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

  it('lands in the right ballpark against a known reference point', () => {
    // ~100 cores aggregating 1 TB of Parquet in 10-20 minutes is a commonly
    // cited shape. If this drifts far from that, the constants are wrong.
    const r = size(ask({
      dataBytes: 1 * TB, format: 'parquet', language: 'sql',
      operation: 'aggregate', targetMinutes: 15,
    }));
    expect(r.totalCores).toBeGreaterThan(50);
    expect(r.totalCores).toBeLessThan(250);
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
