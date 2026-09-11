/**
 * The headline visual: what is actually inside an executor's heap.
 *
 * The point it exists to make is that an "18 GiB executor" does not have 18 GiB
 * for your data -- 300 MiB is reserved outright, 40% of the rest is user memory,
 * and what remains is shared between execution and cache. Seeing the segments
 * to scale makes that immediate in a way a table of numbers does not.
 */

import type { UnifiedMemoryBreakdown } from '../../engine/types/output';
import { formatBytes } from '../format';

interface Props {
  m: UnifiedMemoryBreakdown;
  coresPerExecutor: number;
}

export function MemoryBar({ m, coresPerExecutor }: Props) {
  const pct = (b: number) => (b / m.heap) * 100;

  // Execution and storage share the unified pool; the storage figure is a
  // floor, so it is drawn as a division of that pool rather than a fixed block.
  const execShare = m.unifiedOnHeap - m.storageFloor;

  return (
    <div className="membar">
      <div className="membar-row">
        <div className="membar-label">
          <span>Executor heap (spark.executor.memory)</span>
          <strong>{formatBytes(m.heap)}</strong>
        </div>
        <div className="membar-track">
          <div className="membar-seg reserved" style={{ width: `${pct(m.reserved)}%` }}
               title={`Reserved: ${formatBytes(m.reserved)}`}>
            {pct(m.reserved) > 7 ? 'reserved' : ''}
          </div>
          <div className="membar-seg exec" style={{ width: `${pct(execShare)}%` }}
               title={`Execution: ${formatBytes(execShare)}`}>
            {pct(execShare) > 12 ? `execution ${formatBytes(execShare)}` : 'exec'}
          </div>
          <div className="membar-seg storage" style={{ width: `${pct(m.storageFloor)}%` }}
               title={`Storage floor: ${formatBytes(m.storageFloor)}`}>
            {pct(m.storageFloor) > 12 ? `storage ${formatBytes(m.storageFloor)}` : 'cache'}
          </div>
          <div className="membar-seg user" style={{ width: `${pct(m.userMemory)}%` }}
               title={`User memory: ${formatBytes(m.userMemory)}`}>
            {pct(m.userMemory) > 12 ? `user ${formatBytes(m.userMemory)}` : 'user'}
          </div>
        </div>
        <div className="membar-legend">
          <span><i className="swatch" style={{ background: 'var(--reserved)' }} />
            Reserved {formatBytes(m.reserved)}</span>
          <span><i className="swatch" style={{ background: 'var(--exec)' }} />
            Execution {formatBytes(execShare)}</span>
          <span><i className="swatch" style={{ background: 'var(--storage)' }} />
            Storage floor {formatBytes(m.storageFloor)}</span>
          <span><i className="swatch" style={{ background: 'var(--user)' }} />
            User {formatBytes(m.userMemory)}</span>
        </div>
      </div>

      <div className="callout">
        <strong>{formatBytes(m.perTaskExecutionAtFullParallelism)} per task slot.</strong>{' '}
        The unified pool ({formatBytes(m.unifiedTotal)}) is shared by all{' '}
        {coresPerExecutor} cores in this executor, so that is what one task gets when
        every slot is busy. It is the number that decides whether you spill.
      </div>

      <div className="callout" style={{ borderLeftColor: 'var(--storage)' }}>
        The storage figure is an <strong>eviction floor, not a reservation</strong>.
        Execution can borrow the whole pool and evict cached blocks down to that line;
        storage can never evict execution. Reading it as "half the pool is for cache"
        is the most common misunderstanding of Spark memory.
      </div>
    </div>
  );
}
