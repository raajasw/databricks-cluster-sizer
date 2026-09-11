import { describe, it, expect } from 'vitest';
import { computeRecommendation } from '../../src/engine';
import { SCENARIOS } from '../fixtures/scenarios';
import { validateRuleRegistry } from '../../src/engine/rules/engine';
import { allRules } from '../../src/engine/rules/registry';
import { getAdapter } from '../../src/engine/platforms/registry';
import { getProfile } from '../../src/engine/profiles/registry';

describe('rule registry', () => {
  it('has unique ids and no dangling supersedes', () => {
    const rules = allRules(getAdapter('local'), getProfile('batch-etl'));
    expect(validateRuleRegistry(rules)).toEqual([]);
  });
});

describe('engine end to end', () => {
  it('advises against Spark for a small local job', () => {
    const rec = computeRecommendation(SCENARIOS['laptop-small-data']);
    expect(['dont-use-spark', 'reconsider-spark']).toContain(rec.verdict.level);
    expect(rec.findings.some((f) => f.ruleId === 'data-too-small-for-spark')).toBe(true);
  });

  it('suppresses partition noise when the data is too small for Spark', () => {
    const rec = computeRecommendation(SCENARIOS['laptop-small-data']);
    // The headline finding supersedes tuning advice that would bury it.
    expect(rec.findings.some((f) => f.ruleId === 'spill-predicted')).toBe(false);
    expect(rec.findings.some((f) => f.ruleId === 'shuffle-partitions-default-200')).toBe(false);
  });

  it('catches the non-splittable gzip trap', () => {
    const rec = computeRecommendation(SCENARIOS['gzip-trap']);
    const finding = rec.findings.find((f) => f.ruleId === 'non-splittable-gzip');
    expect(finding).toBeDefined();
    expect(finding!.message).toMatch(/not a splittable codec/);
  });

  it('catches the small-file problem', () => {
    const rec = computeRecommendation(SCENARIOS['small-files']);
    expect(rec.findings.some((f) => f.ruleId === 'too-many-small-files')).toBe(true);
  });

  it('produces a complete derivation trace', () => {
    const rec = computeRecommendation(SCENARIOS['laptop-big-join']);
    const ids = rec.primary.trace.map((s) => s.id);
    expect(ids).toContain('inflation');
    expect(ids).toContain('unified-memory-split');
    expect(ids).toContain('shuffle-partitions');
    // Every step must carry a formula and a rationale: they are user-facing.
    for (const step of rec.primary.trace) {
      expect(step.formula.length).toBeGreaterThan(0);
      expect(step.rationale.length).toBeGreaterThan(0);
    }
  });

  it('never leaves the unified memory split unaccounted', () => {
    const rec = computeRecommendation(SCENARIOS['laptop-big-join']);
    const m = rec.primary.executor.memoryBreakdown;
    // reserved + unified-on-heap + user memory must reconstruct the heap.
    const sum = m.reserved + m.unifiedOnHeap + m.userMemory;
    expect(Math.abs(sum - m.heap)).toBeLessThan(1024);
  });

  it('is deterministic', () => {
    const a = computeRecommendation(SCENARIOS['laptop-big-join']);
    const b = computeRecommendation(SCENARIOS['laptop-big-join']);
    expect(JSON.stringify(a)).toBe(JSON.stringify(b));
  });

  it('produces no NaN or Infinity anywhere', () => {
    for (const name of Object.keys(SCENARIOS) as Array<keyof typeof SCENARIOS>) {
      const rec = computeRecommendation(SCENARIOS[name]);
      const json = JSON.stringify(rec);
      expect(json, `${name} contains NaN`).not.toMatch(/NaN/);
      expect(json, `${name} contains Infinity`).not.toMatch(/Infinity/);
    }
  });
});

describe('scope boundary: even distribution', () => {
  it('models no skew factor anywhere in the sizing math', () => {
    // Deliberate design boundary. Skew depends on key distribution, which
    // cannot be inferred from volume, format or query shape -- so the engine
    // sizes for the even case and says so, rather than inventing a factor.
    // If a future change adds a skew multiplier to the memory math, this test
    // should fail and the change should be argued for explicitly.
    const rec = computeRecommendation(SCENARIOS['laptop-big-join']);
    const r = rec.primary;

    // Per-task memory is a clean division of the pool by task slots.
    const m = r.executor.memoryBreakdown;
    expect(m.perTaskExecutionAtFullParallelism).toBeCloseTo(
      m.unifiedTotal / r.executor.coresPerExecutor, 0,
    );

    // Waves are a clean division of partitions by slots.
    expect(r.parallelism.wavesPerStage.mid).toBeCloseTo(
      r.parallelism.shufflePartitions / r.parallelism.totalTaskSlots, 4,
    );
  });

  it('tells the user it assumed even distribution', () => {
    const rec = computeRecommendation(SCENARIOS['laptop-big-join']);
    const trace = rec.primary.trace.map((s) => s.rationale).join(' ');
    expect(trace).toMatch(/evenly|equal size/i);
  });
});
