/**
 * The derivation trace, rendered as expandable steps.
 *
 * This is what separates the tool from a black box: every number can be traced
 * to the formula and reasoning that produced it.
 */

import { useState } from 'react';
import type { Step } from '../../engine/types/steps';
import { formatRange } from '../format';
import type { Range } from '../../engine/range';

const isRange = (v: unknown): v is Range =>
  typeof v === 'object' && v !== null && 'low' in v && 'mid' in v && 'high' in v;

export function DerivationLadder({ trace }: { trace: Step[] }) {
  const [open, setOpen] = useState<Set<number>>(new Set());

  const toggle = (i: number) => {
    const next = new Set(open);
    if (next.has(i)) next.delete(i); else next.add(i);
    setOpen(next);
  };

  return (
    <div>
      {trace.map((step, i) => (
        <div className="step" key={`${step.id}-${i}`}>
          <div className="step-head" onClick={() => toggle(i)}>
            <span className="step-num">{open.has(i) ? '−' : '+'}</span>
            <span className="step-title">{step.title}</span>
            <span className={`step-conf ${step.confidence}`}>{step.confidence}</span>
          </div>
          {open.has(i) && (
            <div className="step-body">
              <div className="step-formula">{step.formula}</div>
              <p className="step-rationale">{step.rationale}</p>
              {step.alternatives && step.alternatives.length > 0 && (
                <div className="step-alts">
                  {step.alternatives.map((alt, j) => (
                    <div key={j}>
                      <b>Considered {isRange(alt.value) ? formatRange(alt.value) : String(alt.value)}:</b>{' '}
                      {alt.whyRejected}
                    </div>
                  ))}
                </div>
              )}
              {step.citations && step.citations.length > 0 && (
                <div className="step-alts" style={{ marginTop: 6 }}>
                  <b>Refers to:</b> {step.citations.map((c) => c.label).join(', ')}
                </div>
              )}
            </div>
          )}
        </div>
      ))}
    </div>
  );
}
