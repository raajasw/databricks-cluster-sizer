/**
 * s04-s07 and s12: node selection, allocatable capacity, cores per executor,
 * executors per node, and cluster size.
 */

import { bytes, cores, type Cores } from '../../units';
import { sizingValue, type Range } from '../../range';
import {
  CORES_PER_EXECUTOR_WIDE_MIN, CORES_PER_EXECUTOR_WIDE_MAX,
  CORES_PER_EXECUTOR_WARN_ABOVE,
} from '../../constants/heuristics';
import type { UtilizationSpec } from '../../types/output';
import { makeStep, fmtBytes } from '../trace';
import {
  withStep, withDraft, withScratch, type NamedStage, type PipelineContext,
} from '../context';
import { INFLATED_BYTES } from './data-size';

export const UTILIZATION = 'utilization';

/** s04 + s05: pick a node and establish what Spark may actually use on it. */
export const selectNodeAndCapacity: NamedStage = {
  name: 'selectNodeAndCapacity',
  run(ctx: PipelineContext): PipelineContext {
    const candidates = ctx.adapter.listNodeTypes(ctx.input);
    const pinned = ctx.input.cluster.pinnedNodeTypeId;
    const node = (pinned && candidates.find((n) => n.id === pinned)) || candidates[0];
    if (!node) throw new Error(`No node types available for platform ${ctx.adapter.id}`);

    const allocatable = ctx.adapter.allocatable(node, ctx.input);

    const selectStep = makeStep({
      id: 'select-node-type',
      title: 'Node type',
      formula: `${node.displayName}: ${node.vcpus} vCPU, ${fmtBytes(node.memoryBytes)} RAM`,
      inputs: { platform: ctx.adapter.id, pinned: pinned ?? 'none' },
      outputs: { nodeTypeId: node.id, vcpus: node.vcpus, memoryBytes: node.memoryBytes },
      rationale: pinned
        ? 'Using the node type you pinned.'
        : `Selected from the ${ctx.adapter.displayName} catalog.`,
      confidence: 'measured',
    });

    return withDraft(withStep(withStep(ctx, selectStep), allocatable.derivation), {
      nodeType: node,
      allocatable,
    });
  },
};

/** s06: resolve the platform's cores-per-executor policy to a number. */
export const coresPerExecutor: NamedStage = {
  name: 'coresPerExecutor',
  run(ctx: PipelineContext): PipelineContext {
    const { nodeType, allocatable } = ctx.draft;
    if (!nodeType || !allocatable) throw new Error('coresPerExecutor requires node and allocatable');

    const pinnedCores = ctx.input.cluster.pinnedExecutorCores;
    const policy = pinnedCores
      ? ({ kind: 'user-pinned', cores: pinnedCores, reason: 'You pinned this value.' } as const)
      : ctx.adapter.coresPerExecutorPolicy(ctx.input, nodeType);

    let resolved: Cores;
    let detail: string;
    const alternatives: Array<{ value: number; whyRejected: string }> = [];

    switch (policy.kind) {
      case 'whole-node':
        resolved = cores(Math.floor(allocatable.cpu));
        detail = `All ${resolved} allocatable cores go to a single executor.`;
        break;
      case 'single-jvm':
        resolved = cores(Math.max(1, Math.floor(allocatable.cpu)));
        detail = `One JVM with ${resolved} task slots.`;
        break;
      case 'user-pinned':
        resolved = policy.cores;
        detail = `Pinned to ${resolved}.`;
        break;
      case 'four-to-five': {
        // Prefer the value in range that strands the fewest cores.
        const avail = Math.floor(allocatable.cpu);
        let best: number = policy.min;
        let bestWaste = Number.POSITIVE_INFINITY;
        for (let c = policy.min; c <= policy.max; c++) {
          const waste = avail % c;
          if (waste < bestWaste) { bestWaste = waste; best = c; }
        }
        if (bestWaste > 0) {
          // Nothing in 4-5 divides evenly; widen before accepting stranded cores.
          for (let c = CORES_PER_EXECUTOR_WIDE_MIN; c <= CORES_PER_EXECUTOR_WIDE_MAX; c++) {
            const waste = avail % c;
            if (waste < bestWaste) { bestWaste = waste; best = c; }
          }
        }
        resolved = cores(best);
        detail = `${best} cores per executor strands ${bestWaste} of ${avail} allocatable cores.`;
        for (let c = policy.min; c <= policy.max; c++) {
          if (c !== best) {
            alternatives.push({
              value: c,
              whyRejected: `${c} cores would strand ${avail % c} cores per node.`,
            });
          }
        }
        break;
      }
    }

    const step = makeStep({
      id: 'cores-per-executor',
      title: 'Cores per executor',
      formula: `${resolved} cores per executor (policy: ${policy.kind})`,
      inputs: {
        policy: policy.kind,
        allocatableCpu: allocatable.cpu,
        pinned: pinnedCores ?? 'none',
      },
      outputs: { coresPerExecutor: resolved },
      rationale: `${policy.reason} ${detail}` +
        (policy.kind === 'four-to-five'
          ? ' Worth knowing where this rule comes from: the familiar "4-5 cores" guidance ' +
            'traces to a 2015 observation that HDFS client throughput degrades past about ' +
            'five concurrent threads per process. On object storage that mechanism does not ' +
            'exist. What still justifies the range is GC behaviour and blast radius -- tasks ' +
            'in one executor share a heap, so pauses stop all of them and a single skewed ' +
            'task can OOM its innocent siblings.'
          : ''),
      confidence: 'estimated',
      citations: [{ label: 'spark.executor.cores', kind: 'spark-config' }],
      alternatives: alternatives.length ? alternatives : undefined,
    });

    return withDraft(withStep(ctx, step), { coresPerExecutor: resolved });
  },
};

/** s07: how many executors fit on a node, and what is stranded. */
export const executorsPerNode: NamedStage = {
  name: 'executorsPerNode',
  run(ctx: PipelineContext): PipelineContext {
    const { allocatable, coresPerExecutor: cpe, nodeType } = ctx.draft;
    if (!allocatable || !cpe || !nodeType) throw new Error('executorsPerNode missing prerequisites');

    if (!ctx.adapter.capabilities.supportsMultipleExecutorsPerNode) {
      const util: UtilizationSpec = {
        cpuPackingEfficiency: 1,
        memPackingEfficiency: 1,
        strandedCpuPerNode: cores(0),
        strandedMemoryPerNode: bytes(0),
        bindingDimension: 'cpu',
      };
      const step = makeStep({
        id: 'executors-per-node',
        title: 'Executors per node',
        formula: '1 executor per node (platform constraint)',
        inputs: { platform: ctx.adapter.id },
        outputs: { executorsPerNode: 1 },
        rationale: `${ctx.adapter.displayName} runs exactly one executor per node.`,
        confidence: 'documented',
      });
      return withScratch(withDraft(withStep(ctx, step), { executorsPerNode: 1 }), {
        [UTILIZATION]: util,
      });
    }

    const byCpu = Math.floor(allocatable.cpu / cpe);
    const count = Math.max(1, byCpu);
    const strandedCpu = cores(allocatable.cpu - count * cpe);

    const util: UtilizationSpec = {
      cpuPackingEfficiency: (count * cpe) / allocatable.cpu,
      memPackingEfficiency: 1,
      strandedCpuPerNode: strandedCpu,
      strandedMemoryPerNode: bytes(0),
      bindingDimension: 'cpu',
    };

    const step = makeStep({
      id: 'executors-per-node',
      title: 'Executors per node',
      formula:
        `floor(${allocatable.cpu} allocatable cores / ${cpe} cores per executor) = ${count}\n` +
        `stranded: ${strandedCpu} cores per node`,
      inputs: { allocatableCpu: allocatable.cpu, coresPerExecutor: cpe },
      outputs: { executorsPerNode: count, strandedCpu },
      rationale:
        'Packed against allocatable capacity, not advertised capacity. ' +
        (strandedCpu > 0
          ? `${strandedCpu} cores per node cannot form another executor and will sit idle.`
          : 'Cores divide evenly, so nothing is stranded.'),
      confidence: 'documented',
    });

    return withScratch(withDraft(withStep(ctx, step), { executorsPerNode: count }), {
      [UTILIZATION]: util,
    });
  },
};

/** s12: how many executors in total, and therefore how many nodes. */
export const clusterSize: NamedStage = {
  name: 'clusterSize',
  run(ctx: PipelineContext): PipelineContext {
    const { coresPerExecutor: cpe, executorsPerNode: epn } = ctx.draft;
    if (!cpe || !epn) throw new Error('clusterSize missing prerequisites');

    // Local mode is a fixed budget: one JVM on one machine.
    if (!ctx.adapter.capabilities.supportsMultipleExecutorsPerNode &&
        ctx.adapter.id === 'local') {
      const step = makeStep({
        id: 'node-count',
        title: 'Cluster size',
        formula: '1 machine, 1 JVM',
        inputs: { platform: 'local' },
        outputs: { nodeCount: 1, executorCount: 1, totalCores: cpe },
        rationale: 'Local mode has no cluster to size; the machine is the budget.',
        confidence: 'documented',
      });
      return withDraft(withStep(ctx, step), {
        executorCount: 1, nodeCount: 1,
      });
    }

    const inflated = ctx.draft.scratch[INFLATED_BYTES] as Range;
    const targetBytes = ctx.tunables.targetShufflePartitionBytes;
    const partitions = Math.max(1, Math.ceil(sizingValue(inflated) / targetBytes));

    // Aim for the target wave count: enough tasks to hide stragglers, not so
    // many that per-task overhead dominates.
    const targetWaves = (ctx.tunables.targetWavesMin + ctx.tunables.targetWavesMax) / 2;
    const desiredSlots = Math.max(1, Math.ceil(partitions / targetWaves));
    let executorCount = Math.max(1, Math.ceil(desiredSlots / cpe));

    const maxCores = ctx.input.cluster.maxTotalCores;
    if (maxCores) executorCount = Math.min(executorCount, Math.floor(maxCores / cpe) || 1);
    const maxNodes = ctx.input.cluster.maxNodes;
    if (maxNodes) executorCount = Math.min(executorCount, maxNodes * epn);

    const nodeCount = Math.ceil(executorCount / epn);
    const totalCores = cores(executorCount * cpe);

    const step = makeStep({
      id: 'node-count',
      title: 'Cluster size',
      formula:
        `partitions = ${fmtBytes(sizingValue(inflated))} / ${fmtBytes(targetBytes)} = ${partitions}\n` +
        `task slots = ceil(${partitions} / ${targetWaves} waves) = ${desiredSlots}\n` +
        `executors = ceil(${desiredSlots} / ${cpe}) = ${executorCount}\n` +
        `nodes = ceil(${executorCount} / ${epn}) = ${nodeCount}`,
      inputs: {
        inflatedBytes: inflated,
        targetPartitionBytes: targetBytes,
        targetWaves,
        coresPerExecutor: cpe,
        executorsPerNode: epn,
      },
      outputs: { executorCount, nodeCount, totalCores, partitions },
      rationale:
        `Sized for ${ctx.tunables.targetWavesMin}-${ctx.tunables.targetWavesMax} waves per ` +
        'stage. One wave leaves no room to hide a straggler -- a single slow task sets the ' +
        'stage duration -- while very many waves pay task-launch overhead repeatedly. ' +
        'Memory figures use the pessimistic end of the inflation range, because running out ' +
        'of memory three hours into a run costs more than modest overprovisioning.',
      confidence: 'estimated',
    });

    return withDraft(withStep(ctx, step), { executorCount, nodeCount });
  },
};

export { CORES_PER_EXECUTOR_WARN_ABOVE };
