/**
 * s00: structural validation.
 *
 * Produces InputError[] rather than throwing: a partial result plus a clear
 * error is more useful to the user than a blank screen.
 */

import { MIN_EXECUTOR_HEAP } from '../../constants/spark-defaults';
import { makeStep, fmtBytes } from '../trace';
import { withStep, withError, type NamedStage, type PipelineContext } from '../context';

export const validateInput: NamedStage = {
  name: 'validateInput',
  run(ctx: PipelineContext): PipelineContext {
    let next = ctx;
    const { data, pipeline, profile, platformInput } = ctx.input;

    if (!(data.logicalBytesOnDisk > 0)) {
      next = withError(next, {
        path: 'data.logicalBytesOnDisk',
        message: 'Input data size must be greater than zero.',
        severity: 'fatal',
      });
    }

    for (const [path, value] of [
      ['data.columnsReadFraction', data.columnsReadFraction],
      ['data.rowsSurvivingFilterFraction', data.rowsSurvivingFilterFraction],
      ['pipeline.cacheWorkingSetFraction', pipeline.cacheWorkingSetFraction],
    ] as const) {
      if (value !== undefined && (value < 0 || value > 1)) {
        next = withError(next, {
          path,
          message: `${path} must be between 0 and 1 (got ${value}).`,
          severity: 'fatal',
        });
      }
    }

    if (profile === 'streaming' && !ctx.input.streaming) {
      next = withError(next, {
        path: 'streaming',
        message: 'Streaming workloads need streaming parameters (rate, trigger, source).',
        severity: 'fatal',
      });
    }
    if (profile === 'interactive-sql' && !ctx.input.interactive) {
      next = withError(next, {
        path: 'interactive',
        message: 'Interactive workloads need concurrency parameters.',
        severity: 'fatal',
      });
    }

    if (platformInput.platform === 'local') {
      const local = platformInput.local;
      if (local && (local.machineCores <= 0 || local.machineMemory <= 0)) {
        next = withError(next, {
          path: 'platformInput.local',
          message: 'Machine cores and memory must both be greater than zero.',
          severity: 'fatal',
        });
      }
    }

    const step = makeStep({
      id: 'validate-input',
      title: 'Input check',
      formula: next.errors.length === 0
        ? 'inputs consistent'
        : `${next.errors.length} problem(s) found`,
      inputs: { profile, platform: platformInput.platform },
      outputs: { errorCount: next.errors.length },
      rationale: next.errors.length === 0
        ? 'Inputs are structurally consistent, so sizing can proceed.'
        : 'Sizing stops here; fix the inputs above. Anything already derived is still shown.',
      confidence: 'documented',
      citations: [{
        label: `Spark minimum heap: ${fmtBytes(MIN_EXECUTOR_HEAP)}`,
        kind: 'spark-source',
      }],
    });

    return withStep(next, step);
  },
};
