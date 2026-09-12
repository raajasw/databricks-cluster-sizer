/**
 * Six questions, one answer.
 */

import { useState, useMemo } from 'react';
import { size, fmtBytes, fmtMinutes, type Answers, type Format, type Language, type Operation } from './size';
import { nodesFor, CLOUD_LABEL, type Cloud } from './nodes';
import './simple.css';

const GB = 1_000_000_000;

const DEFAULT: Answers = {
  cloud: 'aws',
  dataBytes: 500 * GB,
  format: 'parquet',
  language: 'python-sql',
  operation: 'join',
  targetMinutes: 60,
};

/** Accepts "500", "500gb", "2 TB", "1.5tb". Plain numbers are read as GB. */
function parseSize(text: string): number | null {
  const m = /^\s*([\d.]+)\s*([a-zA-Z]*)\s*$/.exec(text);
  if (!m) return null;
  const value = Number(m[1]);
  if (!Number.isFinite(value) || value <= 0) return null;
  const unit = (m[2] ?? '').toLowerCase().replace(/b$/, '');
  const mult: Record<string, number> = { '': 1e9, k: 1e3, m: 1e6, g: 1e9, t: 1e12 };
  const f = mult[unit];
  return f === undefined ? null : value * f;
}

export default function Simple() {
  const [a, setA] = useState<Answers>(DEFAULT);
  const [sizeText, setSizeText] = useState('500 GB');
  const [showWhy, setShowWhy] = useState(false);

  const r = useMemo(() => size(a), [a]);
  const set = (patch: Partial<Answers>) => setA((prev) => ({ ...prev, ...patch }));

  const catalog = nodesFor(a.cloud);

  return (
    <div className="wrap">
      <header>
        <h1>How many workers?</h1>
        <p>Databricks cluster sizing, in six questions.</p>
      </header>

      <section className="form">
        <div className="q">
          <label>Cloud</label>
          <div className="seg">
            {(['aws', 'azure', 'gcp'] as Cloud[]).map((c) => (
              <button
                key={c}
                className={a.cloud === c ? 'on' : ''}
                onClick={() => set({ cloud: c, nodeTypeId: undefined })}
              >
                {CLOUD_LABEL[c]}
              </button>
            ))}
          </div>
        </div>

        <div className="q">
          <label>How much data does one run read?</label>
          <input
            value={sizeText}
            onChange={(e) => {
              setSizeText(e.target.value);
              const parsed = parseSize(e.target.value);
              if (parsed) set({ dataBytes: parsed });
            }}
          />
          <small>Per run, not per day. A plain number is read as GB.</small>
        </div>

        <div className="q">
          <label>What format?</label>
          <select value={a.format} onChange={(e) => set({ format: e.target.value as Format })}>
            <option value="parquet">Parquet</option>
            <option value="delta">Delta</option>
            <option value="avro">Avro</option>
            <option value="csv">CSV</option>
            <option value="json">JSON</option>
          </select>
        </div>

        <div className="q">
          <label>Written in what?</label>
          <select value={a.language} onChange={(e) => set({ language: e.target.value as Language })}>
            <option value="sql">SQL</option>
            <option value="scala">Scala</option>
            <option value="python-sql">PySpark, DataFrame API only</option>
            <option value="python-udf">PySpark with Python UDFs</option>
          </select>
          <small>
            Python UDFs are the single biggest factor here. If you have them, say so.
          </small>
        </div>

        <div className="q">
          <label>What is the heaviest thing it does?</label>
          <select value={a.operation} onChange={(e) => set({ operation: e.target.value as Operation })}>
            <option value="scan">Read and filter</option>
            <option value="aggregate">Group by / aggregate</option>
            <option value="join">Join two large tables</option>
            <option value="sort">Sort everything</option>
          </select>
        </div>

        <div className="q">
          <label>How long may it take?</label>
          <div className="seg">
            {[15, 30, 60, 120, 240].map((m) => (
              <button
                key={m}
                className={a.targetMinutes === m ? 'on' : ''}
                onClick={() => set({ targetMinutes: m })}
              >
                {m < 60 ? `${m}m` : `${m / 60}h`}
              </button>
            ))}
          </div>
          <small>The main lever. A tighter deadline buys more workers.</small>
        </div>
      </section>

      <section className="answer">
        <div className="big">
          <span className="n">{r.workers}</span>
          <span className="x">×</span>
          <span className="node">{r.node.id}</span>
        </div>
        <div className="spec">
          {r.node.cores} cores · {fmtBytes(r.node.memoryBytes)} RAM
          {r.node.localSsd ? ' · local SSD' : ''} — {r.nodeReason}
        </div>
        <div className="runtime">
          Should finish in about <b>{fmtMinutes(r.estimatedMinutes)}</b>, though
          realistically anywhere from {fmtMinutes(r.estimatedRangeMinutes[0])} to{' '}
          {fmtMinutes(r.estimatedRangeMinutes[1])}.
        </div>

        <div className="override">
          <label>Use a different node type</label>
          <select
            value={a.nodeTypeId ?? ''}
            onChange={(e) => set({ nodeTypeId: e.target.value || undefined })}
          >
            <option value="">Let the tool choose</option>
            {catalog.map((nd) => (
              <option key={nd.id} value={nd.id}>
                {nd.id} — {nd.cores} cores, {fmtBytes(nd.memoryBytes)}
              </option>
            ))}
          </select>
        </div>
      </section>

      {r.notes.length > 0 && (
        <section className="notes">
          {r.notes.map((note, i) => (
            <p key={i}>{note}</p>
          ))}
        </section>
      )}

      <section className="why">
        <button className="link" onClick={() => setShowWhy(!showWhy)}>
          {showWhy ? 'Hide the reasoning' : 'Show me how that was worked out'}
        </button>
        {showWhy && (
          <>
            <ol>
              {r.why.map((line, i) => (
                <li key={i}>{line}</li>
              ))}
            </ol>
            <table>
              <tbody>
                {r.config.map((c) => (
                  <tr key={c.key}>
                    <td className="k">{c.key}</td>
                    <td className="v">{c.value}</td>
                    <td className="note">{c.note}</td>
                  </tr>
                ))}
              </tbody>
            </table>
            <p className="caveat">
              The runtime estimate is the least reliable number here — throughput varies with
              the query, the data and the file layout. Treat it as a starting point, run the
              job once, then check the Spark UI for spill and task skew and adjust.
            </p>
          </>
        )}
      </section>
    </div>
  );
}
