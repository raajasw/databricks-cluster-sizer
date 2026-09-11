/**
 * The rule engine: where the tool stops calculating and starts arguing.
 *
 * Hard requirement on every message: interpolate the actual numbers. "Your
 * shuffle partitions are too low" is worthless. "200 shuffle partitions x
 * 2.1 TiB shuffle = 10.7 GiB per partition, but each task slot has 1.8 GiB of
 * execution memory -- expect roughly 6x spill" is the entire product.
 */

import type { Confidence } from '../constants/heuristics';
import type { PlatformId, WorkloadProfile, WorkloadInput } from './input';
import type { StepId } from './steps';
import type { ConfigEntry, PlatformAdapter } from './platform';
import type { SizingResult } from './output';

export type Severity = 'blocker' | 'critical' | 'warning' | 'info' | 'praise';

export type RuleCategory =
  | 'sanity' | 'memory' | 'parallelism' | 'shuffle' | 'skew'
  | 'platform' | 'streaming' | 'python' | 'config' | 'antipattern' | 'storage';

export const SEVERITY_WEIGHT: Record<Severity, number> = {
  blocker: 5,
  critical: 4,
  warning: 3,
  info: 2,
  praise: 1,
};

export interface Evidence {
  label: string;
  value: string;
  /** Deep-links the finding to the derivation step that produced the number. */
  stepId?: StepId;
}

export interface Fix {
  description: string;
  configChanges?: ConfigEntry[];
  /** Enables a one-click "apply and recompute" in the UI. */
  inputChanges?: Partial<WorkloadInput>;
}

export interface Finding {
  ruleId: string;
  severity: Severity;
  category: RuleCategory;
  /** Short and imperative. */
  title: string;
  /** Full prose WITH the real numbers substituted in. */
  message: string;
  evidence: Evidence[];
  fix?: Fix;
  confidence: Confidence;
  /** severityWeight * 100 + impact, used for ranking. */
  score: number;
}

export interface RuleContext {
  input: WorkloadInput;
  result: SizingResult;
  adapter: PlatformAdapter;
}

/** What a rule returns; the engine fills in id, category and score. */
export type RuleVerdict = Omit<Finding, 'ruleId' | 'category' | 'score'> & {
  /** Optional 0-100 nudge to ranking within a severity band. */
  impact?: number;
};

export interface Rule {
  id: string;
  category: RuleCategory;
  defaultSeverity: Severity;
  appliesTo?: {
    profiles?: WorkloadProfile[];
    platforms?: PlatformId[];
  };
  /** Pure predicate; null when the rule does not fire. */
  evaluate(ctx: RuleContext): RuleVerdict | null;
  /**
   * Rules made redundant when this one fires. Without suppression, telling a
   * user their 2 GB job does not need Spark alongside fifteen partition-tuning
   * warnings buries the one thing that matters.
   */
  supersedes?: string[];
}
