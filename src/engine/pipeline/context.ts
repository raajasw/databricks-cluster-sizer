/**
 * The pipeline context and its reducer.
 *
 * Every stage is (ctx) => ctx: pure, returning a NEW context with steps
 * appended. The ordered stage array IS the algorithm documentation, and any
 * stage can be unit-tested by building a context and asserting on both the
 * draft values and the emitted formula strings.
 */

import type { WorkloadInput } from '../types/input';
import type { Step, InputError } from '../types/steps';
import type { DraftShape, PlatformAdapter } from '../types/platform';
import type { ProfileStrategy, Tunables } from '../types/profile';

export interface PipelineContext {
  readonly input: WorkloadInput;
  readonly adapter: PlatformAdapter;
  readonly profile: ProfileStrategy;
  readonly tunables: Tunables;
  readonly draft: DraftShape;
  readonly trace: readonly Step[];
  readonly errors: readonly InputError[];
  /** Set once a fatal error is recorded; remaining stages no-op. */
  readonly halted: boolean;
}

export type StageFn = (ctx: PipelineContext) => PipelineContext;

export interface NamedStage {
  name: string;
  run: StageFn;
}

export const withStep = (ctx: PipelineContext, step: Step): PipelineContext => ({
  ...ctx,
  trace: [...ctx.trace, step],
});

export const withDraft = (
  ctx: PipelineContext,
  patch: Partial<DraftShape>,
): PipelineContext => ({ ...ctx, draft: { ...ctx.draft, ...patch } });

export const withScratch = (
  ctx: PipelineContext,
  patch: Record<string, unknown>,
): PipelineContext => ({
  ...ctx,
  draft: { ...ctx.draft, scratch: { ...ctx.draft.scratch, ...patch } },
});

export const withError = (ctx: PipelineContext, error: InputError): PipelineContext => ({
  ...ctx,
  errors: [...ctx.errors, error],
  halted: ctx.halted || error.severity === 'fatal',
});

/** Reads a scratch value written by an earlier stage. */
export function scratch<T>(ctx: PipelineContext, key: string): T | undefined {
  return ctx.draft.scratch[key] as T | undefined;
}

export function requireScratch<T>(ctx: PipelineContext, key: string): T {
  const v = ctx.draft.scratch[key];
  if (v === undefined) {
    throw new Error(`Pipeline stage ordering bug: scratch key "${key}" not set yet`);
  }
  return v as T;
}

/**
 * Runs stages in order. A fatal error halts further computation but preserves
 * everything already derived, so the UI can show a partial result alongside the
 * error rather than a blank screen.
 */
export function runStages(ctx: PipelineContext, stages: NamedStage[]): PipelineContext {
  return stages.reduce((acc, stage) => {
    if (acc.halted) return acc;
    return stage.run(acc);
  }, ctx);
}
