import type { Finding } from '../../engine/types/rules';

export function FindingsPanel({ findings }: { findings: Finding[] }) {
  if (findings.length === 0) {
    return (
      <p style={{ color: 'var(--text-dim)', fontSize: 13, margin: 0 }}>
        Nothing to flag. The configuration below has no known anti-patterns for this workload.
      </p>
    );
  }
  return (
    <div>
      {findings.map((f) => (
        <div className={`finding ${f.severity}`} key={f.ruleId}>
          <div className="finding-head">
            <span className="finding-sev">{f.severity}</span>
            <span className="finding-title">{f.title}</span>
          </div>
          <p className="finding-msg">{f.message}</p>
          {f.evidence.length > 0 && (
            <div className="finding-evidence">
              {f.evidence.map((e, i) => (
                <span className="ev" key={i}>{e.label}: <b>{e.value}</b></span>
              ))}
            </div>
          )}
        </div>
      ))}
    </div>
  );
}
