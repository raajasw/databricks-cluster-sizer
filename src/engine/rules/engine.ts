/**
 * Rule evaluation, ranking and suppression.
 */

import { SEVERITY_WEIGHT, type Finding, type Rule, type RuleContext } from '../types/rules';

export function evaluateRules(rules: Rule[], ctx: RuleContext): Finding[] {
  const fired: Array<{ rule: Rule; finding: Finding }> = [];

  for (const rule of rules) {
    const applies = rule.appliesTo;
    if (applies?.profiles && !applies.profiles.includes(ctx.input.profile)) continue;
    if (applies?.platforms && !applies.platforms.includes(ctx.input.platformInput.platform)) continue;

    let verdict;
    try {
      verdict = rule.evaluate(ctx);
    } catch {
      // A rule that throws must not take down the whole recommendation.
      continue;
    }
    if (!verdict) continue;

    const { impact = 50, ...rest } = verdict;
    fired.push({
      rule,
      finding: {
        ...rest,
        ruleId: rule.id,
        category: rule.category,
        score: SEVERITY_WEIGHT[rest.severity] * 100 + impact,
      },
    });
  }

  // Suppression: when a rule that supersedes others fires, those others are
  // noise. Telling someone their 2 GB job does not need Spark, then burying it
  // under fifteen partition-tuning notes, defeats the point.
  const suppressed = new Set<string>();
  for (const { rule } of fired) {
    for (const id of rule.supersedes ?? []) suppressed.add(id);
  }

  return fired
    .filter(({ rule }) => !suppressed.has(rule.id))
    .map(({ finding }) => finding)
    .sort((a, b) => b.score - a.score);
}

/** Unique-id and dangling-supersedes check, asserted in tests. */
export function validateRuleRegistry(rules: Rule[]): string[] {
  const problems: string[] = [];
  const ids = new Set<string>();
  for (const r of rules) {
    if (ids.has(r.id)) problems.push(`Duplicate rule id: ${r.id}`);
    ids.add(r.id);
  }
  for (const r of rules) {
    for (const s of r.supersedes ?? []) {
      if (!ids.has(s)) problems.push(`Rule ${r.id} supersedes unknown rule ${s}`);
    }
  }
  return problems;
}
