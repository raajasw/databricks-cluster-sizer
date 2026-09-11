import type { PlatformConfigOutput } from '../../engine/types/platform';

export function ConfigOutput({ config }: { config: PlatformConfigOutput }) {
  const entries = config.sparkConf;

  return (
    <div>
      {'sparkSubmitArgs' in config && config.sparkSubmitArgs.length > 0 && (
        <>
          <h3>Launch</h3>
          <pre className="cmd">{`spark-submit \\\n  ${config.sparkSubmitArgs.join(' \\\n  ')} \\\n  your_job.py`}</pre>
        </>
      )}

      {config.kind === 'local' && Object.keys(config.envVars).length > 0 && (
        <>
          <h3 style={{ marginTop: 16 }}>Or, for PySpark, before the JVM starts</h3>
          <pre className="cmd">
            {Object.entries(config.envVars)
              .map(([k, v]) => `export ${k}="${v}"`)
              .join('\n')}
          </pre>
        </>
      )}

      <h3 style={{ marginTop: 16 }}>Configuration</h3>
      <table className="conf-table">
        <tbody>
          {entries.map((e) => (
            <tr key={e.key} className={e.ignored ? 'conf-ignored' : ''}>
              <td className="conf-key">{e.key}</td>
              <td className="conf-val">{e.value}</td>
              <td className="conf-note">
                {e.note}
                {e.sparkDefault && e.value !== e.sparkDefault && (
                  <span> (default: {e.sparkDefault})</span>
                )}
              </td>
            </tr>
          ))}
        </tbody>
      </table>
    </div>
  );
}
