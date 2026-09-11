import { useState, useMemo } from 'react';
import { computeRecommendation } from './engine';
import { DEFAULT_INPUT } from './ui/state/defaults';
import { Questionnaire } from './ui/components/Questionnaire';
import { MemoryBar } from './ui/components/MemoryBar';
import { DerivationLadder } from './ui/components/DerivationLadder';
import { FindingsPanel } from './ui/components/FindingsPanel';
import { ConfigOutput } from './ui/components/ConfigOutput';
import { formatBytes, formatRange, formatNumber } from './ui/format';
import type { WorkloadInput } from './engine/types/input';
import './ui/styles.css';

export default function App() {
  const [input, setInput] = useState<WorkloadInput>(DEFAULT_INPUT);

  // The engine is pure and sub-millisecond, so recomputing on every edit is free.
  const rec = useMemo(() => computeRecommendation(input), [input]);
  const r = rec.primary;
  const failed = rec.errors.some((e) => e.severity === 'fatal');

  return (
    <div className="app">
      <aside className="sidebar">
        <Questionnaire input={input} onChange={setInput} />
      </aside>

      <main className="main">
        <div className={`verdict ${rec.verdict.level}`}>
          <div className="level">{rec.verdict.level.replace(/-/g, ' ')}</div>
          <div className="headline">{rec.verdict.headline}</div>
          {rec.verdict.reasoning[0] && <p>{rec.verdict.reasoning[0]}</p>}
        </div>

        {failed && (
          <div className="panel errors">
            <h2>Cannot size this</h2>
            <ul>
              {rec.errors.map((e, i) => <li key={i}>{e.message}</li>)}
            </ul>
          </div>
        )}

        {!failed && (
          <>
            <div className="panel">
              <h2>The shape of it</h2>
              <div className="stat-grid">
                <div className="stat">
                  <div className="label">Executors</div>
                  <div className="value">{r.executor.count}</div>
                  <div className="sub">{r.executor.coresPerExecutor} cores each</div>
                </div>
                <div className="stat">
                  <div className="label">Executor memory</div>
                  <div className="value">{formatBytes(r.executor.heapBytes)}</div>
                  <div className="sub">
                    + {formatBytes(r.executor.overheadBytes)} overhead
                  </div>
                </div>
                <div className="stat">
                  <div className="label">Task slots</div>
                  <div className="value">{r.parallelism.totalTaskSlots}</div>
                  <div className="sub">
                    {formatRange(r.parallelism.wavesPerStage)} waves per stage
                  </div>
                </div>
                <div className="stat">
                  <div className="label">Shuffle partitions</div>
                  <div className="value">{formatNumber(r.parallelism.shufflePartitions)}</div>
                  <div className="sub">default is 200</div>
                </div>
                <div className="stat">
                  <div className="label">Data in memory</div>
                  <div className="value">
                    {formatBytes(r.memory.inflatedScannedBytes.mid)}
                  </div>
                  <div className="sub">
                    from {formatBytes(input.data.logicalBytesOnDisk)} on disk
                  </div>
                </div>
                <div className="stat">
                  <div className="label">Inflation</div>
                  <div className="value">
                    {r.memory.inflation.total.mid.toFixed(1)}x
                  </div>
                  <div className="sub">
                    range {r.memory.inflation.total.low.toFixed(1)}–
                    {r.memory.inflation.total.high.toFixed(1)}x
                  </div>
                </div>
              </div>

              <div className="uncertainty-note">
                <strong>This assumes data spread evenly across partitions.</strong> Real keys
                rarely are, and a skewed partition will be bigger and slower than the average
                here — but by how much depends on your key distribution, which nothing in this
                form can reveal. Size for the even case, then tune from what the Spark UI shows
                after a real run.
                <br /><br />
                Memory figures are sized from the pessimistic end of each range, not the
                midpoint: running out of memory three hours into a run costs far more than
                modest overprovisioning. Ranges are shown rather than collapsed because the
                underlying quantities genuinely vary — the width is the honest answer.
              </div>
            </div>

            <div className="panel">
              <h2>Inside one executor</h2>
              <MemoryBar
                m={r.executor.memoryBreakdown}
                coresPerExecutor={r.executor.coresPerExecutor}
              />
            </div>

            <div className="panel">
              <h2>
                What to watch out for
                {rec.findings.length > 0 && (
                  <span style={{ color: 'var(--text-faint)', fontWeight: 400 }}>
                    {' '}({rec.findings.length})
                  </span>
                )}
              </h2>
              <FindingsPanel findings={rec.findings} />
            </div>

            <div className="panel">
              <h2>How every number was derived</h2>
              <DerivationLadder trace={r.trace} />
            </div>

            <div className="panel">
              <h2>Configuration</h2>
              <ConfigOutput config={r.config} />
            </div>

            <div className="panel">
              <h2>After your first run</h2>
              <p style={{ color: 'var(--text-dim)', fontSize: 13, margin: '0 0 10px' }}>
                This is a starting point, not an oracle. It assumes even data distribution,
                and the least reliable inputs are the inflation factor and throughput per core.
                One real run tells you more than any estimate. Open the Spark UI and check:
              </p>
              <table className="conf-table">
                <tbody>
                  <tr>
                    <td><b>Spill (memory / disk)</b></td>
                    <td className="conf-note">
                      Stages tab. Non-zero means raise shuffle partitions or lower the
                      advisory partition size.
                    </td>
                  </tr>
                  <tr>
                    <td><b>GC time</b></td>
                    <td className="conf-note">
                      Executors tab. Above ~10% of task time means the heap is too large
                      per core, or too small overall.
                    </td>
                  </tr>
                  <tr>
                    <td><b>Task duration spread</b></td>
                    <td className="conf-note">
                      Stages tab, max vs median. This is where skew shows up, and it is the
                      one thing above that the sizing above cannot predict for you. A max far
                      above the median means a few keys dominate: turn on
                      <code> spark.sql.adaptive.skewJoin.enabled</code>, or salt the join key
                      if AQE is not enough. Expect to iterate here.
                    </td>
                  </tr>
                  <tr>
                    <td><b>Shuffle read / write</b></td>
                    <td className="conf-note">
                      Compare against the estimate above. A large miss means the inflation
                      factor needs correcting.
                    </td>
                  </tr>
                </tbody>
              </table>
            </div>
          </>
        )}
      </main>
    </div>
  );
}
