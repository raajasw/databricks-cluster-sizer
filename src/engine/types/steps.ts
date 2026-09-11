/**
 * The derivation trace.
 *
 * Every pipeline stage emits at least one Step recording what it computed, from
 * what, using which formula, and how confident it is. This is the spine of the
 * product: a sizing tool that emits "executor memory: 18g" is a black box, and
 * a black box cannot be defended in a design review or adapted when the data
 * grows. The trace is what turns a number into an argument.
 */

import type { Range } from '../range';
import type { Confidence } from '../constants/heuristics';

export type StepId =
  | 'validate-input'
  | 'effective-data-size'
  | 'inflation'
  | 'working-set'
  | 'select-node-type'
  | 'allocatable-capacity'
  | 'cores-per-executor'
  | 'executors-per-node'
  | 'executor-heap'
  | 'memory-overhead'
  | 'unified-memory-split'
  | 'per-task-memory'
  | 'node-count'
  | 'input-partitions'
  | 'shuffle-partitions'
  | 'shuffle-storage'
  | 'driver-sizing'
  | 'elasticity'
  | 'platform-emit'
  | 'convergence'
  // profile-spliced stages
  | 'streaming-micro-batch'
  | 'streaming-read-parallelism'
  | 'streaming-state-store'
  | 'streaming-memory-profile'
  | 'interactive-concurrency'
  | 'ml-python-memory';

export type ScalarOrRange = number | string | boolean | Range;

export interface Citation {
  label: string;
  kind: 'spark-config' | 'spark-source' | 'vendor-doc' | 'heuristic' | 'folklore';
  url?: string;
}

/** An option the engine considered and did not take. Shown on expand. */
export interface Alternative {
  value: ScalarOrRange;
  whyRejected: string;
}

export interface Step {
  id: StepId;
  title: string;
  /** Formula with real values substituted, for display. */
  formula: string;
  inputs: Record<string, ScalarOrRange>;
  outputs: Record<string, ScalarOrRange>;
  /** Why this rule or constant applies here. */
  rationale: string;
  confidence: Confidence;
  citations?: Citation[];
  alternatives?: Alternative[];
  /** Set when a stage re-runs in the corrective second pass. */
  pass?: number;
}

export type InputErrorSeverity = 'fatal' | 'warning';

export interface InputError {
  /** Dotted path into WorkloadInput, for focusing the offending form field. */
  path: string;
  message: string;
  severity: InputErrorSeverity;
}
