/**
 * The form.
 *
 * Design rule carried over from the engine: every control here must change a
 * number in the output. Anything that does not is a question not worth asking.
 */

import { bytes, cores, type Bytes } from '../../engine/units';
import { formatBytes, parseSize } from '../format';
import { PRESETS } from '../state/defaults';
import type {
  WorkloadInput, StorageFormat, Codec, RuntimeLanguage, QueryShape,
  WorkloadProfile, Environment,
} from '../../engine/types/input';

interface Props {
  input: WorkloadInput;
  onChange: (next: WorkloadInput) => void;
}

export function Questionnaire({ input, onChange }: Props) {
  const set = (patch: Partial<WorkloadInput>) => onChange({ ...input, ...patch });
  const setData = (patch: Partial<WorkloadInput['data']>) =>
    onChange({ ...input, data: { ...input.data, ...patch } });
  const setPipeline = (patch: Partial<WorkloadInput['pipeline']>) =>
    onChange({ ...input, pipeline: { ...input.pipeline, ...patch } });
  const setLocal = (patch: Partial<NonNullable<WorkloadInput['platformInput']['local']>>) =>
    onChange({
      ...input,
      platformInput: {
        ...input.platformInput,
        local: { ...input.platformInput.local!, ...patch },
      },
    });

  return (
    <div>
      <h1>Spark Sizing Advisor</h1>
      <p className="tagline">
        Opinionated cluster sizing, with the reasoning shown.
      </p>

      <h3>Start from</h3>
      <div className="presets">
        {PRESETS.map((p) => (
          <button className="preset" key={p.name} onClick={() => onChange(p.input)}>
            <strong>{p.name}</strong>
            <span>{p.description}</span>
          </button>
        ))}
      </div>

      <h3>Workload</h3>
      <div className="field">
        <label>Profile</label>
        <select value={input.profile}
                onChange={(e) => set({ profile: e.target.value as WorkloadProfile })}>
          <option value="batch-etl">Batch ETL</option>
          <option value="streaming">Structured Streaming (not yet modelled)</option>
          <option value="interactive-sql">Interactive SQL (not yet modelled)</option>
          <option value="ml-feature">ML / feature engineering (not yet modelled)</option>
        </select>
      </div>

      <div className="row">
        <div className="field">
          <label>Environment</label>
          <select value={input.environment}
                  onChange={(e) => set({ environment: e.target.value as Environment })}>
            <option value="dev">Development</option>
            <option value="staging">Staging</option>
            <option value="prod">Production</option>
          </select>
        </div>
        <div className="field">
          <label>Language</label>
          <select value={input.runtimeLanguage}
                  onChange={(e) => set({ runtimeLanguage: e.target.value as RuntimeLanguage })}>
            <option value="scala-java">Scala / Java</option>
            <option value="pyspark-sql-only">PySpark (SQL only)</option>
            <option value="pyspark-udf">PySpark (Python UDFs)</option>
            <option value="pyspark-pandas-udf">PySpark (Pandas UDFs)</option>
            <option value="sparkr">SparkR</option>
          </select>
        </div>
      </div>

      <h3>Data, per run</h3>
      <div className="field">
        <label>Volume scanned per run</label>
        <input
          defaultValue={formatBytes(input.data.logicalBytesOnDisk)}
          key={input.data.logicalBytesOnDisk}
          onBlur={(e) => {
            const parsed = parseSize(e.target.value);
            if (parsed) setData({ logicalBytesOnDisk: bytes(parsed) as Bytes });
          }}
        />
        <div className="hint">
          Per run, not per day. An hourly job described with a daily figure sizes 24x wrong.
        </div>
      </div>

      <div className="row">
        <div className="field">
          <label>Format</label>
          <select value={input.data.format}
                  onChange={(e) => setData({ format: e.target.value as StorageFormat })}>
            <option value="parquet">Parquet</option>
            <option value="delta">Delta</option>
            <option value="iceberg">Iceberg</option>
            <option value="orc">ORC</option>
            <option value="avro">Avro</option>
            <option value="json">JSON</option>
            <option value="csv">CSV</option>
          </select>
        </div>
        <div className="field">
          <label>Compression</label>
          <select value={input.data.codec}
                  onChange={(e) => setData({ codec: e.target.value as Codec })}>
            <option value="snappy">snappy</option>
            <option value="zstd">zstd</option>
            <option value="gzip">gzip</option>
            <option value="lz4">lz4</option>
            <option value="none">none</option>
          </select>
        </div>
      </div>

      <div className="field">
        <label>Number of files</label>
        <input type="number" value={input.data.fileCount ?? 0}
               onChange={(e) => setData({ fileCount: Number(e.target.value) || 0 })} />
        <div className="hint">
          Drives the small-file check and split packing.
        </div>
      </div>

      <h3>Shape of the work</h3>
      <div className="field">
        <label>Dominant operation</label>
        <select value={input.pipeline.dominantQueryShape}
                onChange={(e) => setPipeline({ dominantQueryShape: e.target.value as QueryShape })}>
          <option value="scan-filter-write">Scan / filter / write (no shuffle)</option>
          <option value="narrow-transform">Narrow transforms</option>
          <option value="aggregation">Aggregation / groupBy</option>
          <option value="shuffle-join">Large-to-large join</option>
          <option value="broadcast-join">Broadcast join</option>
          <option value="sort">Sort</option>
          <option value="window">Window functions</option>
          <option value="multi-stage-dag">Multi-stage pipeline</option>
        </select>
      </div>

      <div className="field">
        <label>Shuffle stages</label>
        <input type="number" min={0} value={input.pipeline.shuffleStages}
               onChange={(e) => setPipeline({ shuffleStages: Number(e.target.value) || 0 })} />
      </div>

      <div className="field">
        <label>Cached fraction of the working set</label>
        <input type="number" min={0} max={1} step={0.1}
               value={input.pipeline.cacheWorkingSetFraction}
               onChange={(e) => setPipeline({
                 cacheWorkingSetFraction: Math.min(1, Math.max(0, Number(e.target.value) || 0)),
               })} />
      </div>

      <h3>Machine</h3>
      <div className="row">
        <div className="field">
          <label>Cores</label>
          <input type="number" min={1}
                 value={input.platformInput.local?.machineCores ?? 8}
                 onChange={(e) => setLocal({ machineCores: cores(Number(e.target.value) || 1) })} />
        </div>
        <div className="field">
          <label>RAM</label>
          <input
            key={input.platformInput.local?.machineMemory}
            defaultValue={formatBytes(input.platformInput.local?.machineMemory ?? 0)}
            onBlur={(e) => {
              const parsed = parseSize(e.target.value);
              if (parsed) setLocal({ machineMemory: bytes(parsed) as Bytes });
            }}
          />
        </div>
      </div>
      <div className="hint" style={{ marginTop: -8, marginBottom: 14 }}>
        Only local mode is implemented so far. Databricks and Kubernetes are next.
      </div>

      <div className="field">
        <label>
          <input type="checkbox" checked={input.aqeEnabled} style={{ width: 'auto', marginRight: 6 }}
                 onChange={(e) => set({ aqeEnabled: e.target.checked })} />
          Adaptive Query Execution enabled
        </label>
        <div className="hint">On by default since Spark 3.2.</div>
      </div>
    </div>
  );
}
