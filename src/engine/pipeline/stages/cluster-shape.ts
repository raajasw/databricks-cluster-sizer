/**
 * s04-s07 and s12: node selection, allocatable capacity, cores per executor,
 * executors per node, and cluster size.
 */

import { bytes, cores, gib, type Cores } from '../../units';
import { sizingValue, throughputValue, mulRange, type Range } from '../../range';
import {
  CORES_PER_EXECUTOR_WIDE_MIN, CORES_PER_EXECUTOR_WIDE_MAX,
  CORES_PER_EXECUTOR_WARN_ABOVE, CORES_PER_EXECUTOR_MIN, CORES_PER_EXECUTOR_MAX,
} from '../../constants/heuristics';
import { MEMORY_FRACTION } from '../../constants/spark-defaults';
import {
  BYTES_PER_CORE_SECOND_BY_SHAPE, THROUGHPUT_LANGUAGE_FACTOR,
  DEFAULT_BATCH_WINDOW_SECONDS,
} from '../../constants/heuristics';
import type { NodeType } from '../../types/platform';
import type { UtilizationSpec } from '../../types/output';
import { makeStep, fmtBytes, fmtSeconds } from '../trace';
import {
  withStep, withDraft, withScratch, type NamedStage, type PipelineContext,
} from '../context';
import { INFLATED_BYTES } from './data-size';

export const UTILIZATION = 'utilization';

/**
 * How much memory one task slot wants, in bytes.
 *
 * This ratio decides the instance FAMILY: compare it against a node's
 * memory-per-core and you know whether to reach for compute-, general- or
 * memory-optimized.
 *
 * Using the nominal partition target alone would be circular -- it is a
 * constant, so every workload would want the same ratio and the selector would
 * always pick the cheapest cores. What actually varies is how much data each
 * core has to move: a job with more bytes per core needs bigger partitions to
 * avoid an absurd wave count, and bigger partitions need more memory per slot.
 */
function memoryPerSlotNeeded(ctx: PipelineContext, assumedCores?: number): number {
  const amplification =
    (ctx.draft.scratch['queryAmplification'] as { mid: number } | undefined)?.mid ?? 1;
  const nominal = ctx.tunables.targetShufflePartitionBytes;

  const inflated = ctx.draft.scratch[INFLATED_BYTES] as Range | undefined;
  let partition = nominal;

  if (inflated && assumedCores && assumedCores > 0) {
    const volume = sizingValue(inflated);
    // Aim for a partition size that keeps waves in the target band for a
    // cluster of roughly this size, but never below the nominal target.
    /*
     * Bounded by what a task can actually hold, not by an arbitrary constant.
     *
     * Clamping too low caps the memory-per-core figure and the selector picks
     * cheap compute nodes that force hundreds of waves. Leaving it unbounded
     * asks for more memory per core than any instance provides, and the
     * selector overcorrects into one huge executor per node.
     *
     * The honest ceiling is the largest partition a single task can process
     * without spilling on a realistic node. Past ~1 GiB per partition the
     * sort and hash structures built on top stop fitting in any sane heap
     * slice, so that is the bound.
     */
    const targetWaves = ctx.tunables.targetWavesMax * 4;
    const wanted = volume / (assumedCores * targetWaves);
    partition = Math.max(nominal, Math.min(wanted, gib(1))) as typeof nominal;
  }

  // A task holds its partition plus the operator structures on top of it, and
  // the unified pool is only ~60% of the heap, so gross it up.
  return (partition * amplification) / MEMORY_FRACTION;
}

interface ScoredNode {
  node: NodeType;
  score: number;
  reasons: string[];
}

/**
 * Score a node for this workload. Lower is better.
 *
 * Three things matter, in order: does the memory-per-core ratio match what the
 * work needs, do executors tile the node without stranding capacity, and does
 * the node have local NVMe when the job shuffles heavily.
 */
function scoreNode(
  node: NodeType,
  ctx: PipelineContext,
  neededPerSlot: number,
): ScoredNode {
  const reasons: string[] = [];
  const allocatable = ctx.adapter.allocatable(node, ctx.input);

  // 1. Ratio fit, scored in log space so 2x too much and 2x too little cost
  // the same. This is the dominant term: it is what picks the family.
  const nodeRatio = allocatable.memory / allocatable.cpu;
  const ratioFit = Math.abs(Math.log2(nodeRatio / neededPerSlot));
  let score = ratioFit * 10;
  if (ratioFit < 0.5) reasons.push('memory-per-core matches the workload closely');
  else if (nodeRatio > neededPerSlot) reasons.push('more memory per core than this job needs');
  else reasons.push('less memory per core than this job wants');

  // 2. Packing waste: cores that cannot form another executor are paid for and idle.
  const cpe = Math.min(
    CORES_PER_EXECUTOR_MAX,
    Math.max(CORES_PER_EXECUTOR_MIN, Math.floor(allocatable.cpu / 2)),
  );
  const perNode = Math.max(1, Math.floor(allocatable.cpu / cpe));
  const strandedFraction = (allocatable.cpu - perNode * cpe) / allocatable.cpu;
  score += strandedFraction * 12;
  if (strandedFraction > 0.15) reasons.push(`strands ${Math.round(strandedFraction * 100)}% of its cores`);

  // 3. Local NVMe, when the job actually shuffles.
  const shuffleHeavy = ctx.input.pipeline.shuffleStages >= 2;
  if (shuffleHeavy && node.localSsdBytes) {
    score -= 3;
    reasons.push('local NVMe for shuffle');
  } else if (shuffleHeavy && !node.localSsdBytes) {
    score += 2;
  }

  // 4. Mild preference against very small nodes, which multiply per-executor
  // overhead across more JVMs than necessary.
  if (allocatable.cpu < 6) score += 2;

  /*
   * 5. Wave pressure.
   *
   * A node with little memory per core forces small partitions, which forces a
   * high wave count, which is wall-clock time. The ratio term above measures
   * the mismatch symmetrically, but the two directions are NOT symmetric in
   * consequence: too much memory wastes money, while too little forces the job
   * into hundreds of sequential waves. Penalise the starved side harder.
   */
  if (nodeRatio < neededPerSlot) {
    const starvation = neededPerSlot / nodeRatio;
    score += Math.min(starvation, 8);
    if (starvation > 2) {
      reasons.push('would force small partitions and a high wave count');
    }
  }

  return { node, score, reasons };
}

/** s04 + s05: pick a node and establish what Spark may actually use on it. */
export const selectNodeAndCapacity: NamedStage = {
  name: 'selectNodeAndCapacity',
  run(ctx: PipelineContext): PipelineContext {
    const candidates = ctx.adapter.listNodeTypes(ctx.input);
    if (candidates.length === 0) {
      throw new Error(`No node types available for platform ${ctx.adapter.id}`);
    }
    const pinned = ctx.input.cluster.pinnedNodeTypeId;
    const pinnedNode = pinned ? candidates.find((n) => n.id === pinned) : undefined;

    // Rough core count from the runtime target, so the ratio reflects how much
    // data each core will actually handle. Refined later once the node is known.
    const inflated = ctx.draft.scratch[INFLATED_BYTES] as Range | undefined;
    const shape = ctx.input.pipeline.dominantQueryShape;
    const perCore = throughputValue(mulRange(
      BYTES_PER_CORE_SECOND_BY_SHAPE[shape],
      THROUGHPUT_LANGUAGE_FACTOR[ctx.input.runtimeLanguage],
    ));
    const seconds = ctx.input.sla.targetRuntime ?? DEFAULT_BATCH_WINDOW_SECONDS;
    const roughCores = inflated
      ? Math.max(1, Math.ceil(sizingValue(inflated) / (perCore * seconds)))
      : 1;

    const neededPerSlot = memoryPerSlotNeeded(ctx, roughCores);
    const ranked = candidates
      .map((n) => scoreNode(n, ctx, neededPerSlot))
      .sort((a, b) => a.score - b.score);

    const best = ranked[0]!;
    const node = pinnedNode ?? best.node;
    const allocatable = ctx.adapter.allocatable(node, ctx.input);

    const runnerUp = ranked.find((r) => r.node.id !== node.id);

    const selectStep = makeStep({
      id: 'select-node-type',
      title: 'Node type',
      formula:
        `${node.displayName}: ${node.vcpus} vCPU, ${fmtBytes(node.memoryBytes)} RAM ` +
        `(${fmtBytes(node.memoryBytes / node.vcpus)} per core)
` +
        `this workload wants about ${fmtBytes(neededPerSlot)} per task slot`,
      inputs: {
        platform: ctx.adapter.id,
        pinned: pinned ?? 'none',
        memoryNeededPerSlot: neededPerSlot,
        candidatesConsidered: candidates.length,
      },
      outputs: { nodeTypeId: node.id, vcpus: node.vcpus, memoryBytes: node.memoryBytes },
      rationale: pinnedNode
        ? `Using the node type you pinned. ${best.node.id !== node.id ? `Left to itself the ` +
            `sizer would have picked ${best.node.displayName}, because it ${best.reasons[0]}.` : ''}`
        : `Chosen from ${candidates.length} candidates by matching memory-per-core to what ` +
          `this workload needs per task slot, then penalising nodes that strand capacity. ` +
          `${node.displayName} ${best.reasons.join(', ')}.`,
      confidence: 'measured',
      alternatives: runnerUp
        ? [{
            value: runnerUp.node.displayName,
            whyRejected:
              `Scored ${(runnerUp.score - best.score).toFixed(1)} worse: ` +
              `${runnerUp.reasons.join(', ')}.`,
          }]
        : undefined,
    });

    return withScratch(
      withDraft(withStep(withStep(ctx, selectStep), allocatable.derivation), {
        nodeType: node,
        allocatable,
      }),
      { roughCores },
    );
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

    // Pack against BOTH dimensions. A CPU-only calculation overcommits memory
    // on memory-hungry workloads, and which dimension binds is itself useful
    // information -- it tells the user whether to reach for more cores or a
    // different instance family.
    const byCpu = Math.floor(allocatable.cpu / cpe);

    const roughCoresForFit = ctx.draft.scratch['roughCores'] as number | undefined;
    const perSlotNeeded = memoryPerSlotNeeded(ctx, roughCoresForFit);
    const memPerExecutorNeeded = perSlotNeeded * cpe;
    const byMem = Math.floor(allocatable.memory / memPerExecutorNeeded);

    const count = Math.max(1, Math.min(byCpu, byMem));
    const bindingDimension: 'cpu' | 'memory' = byMem < byCpu ? 'memory' : 'cpu';

    const strandedCpu = cores(Math.max(0, allocatable.cpu - count * cpe));
    const strandedMem = bytes(Math.max(0, allocatable.memory - count * memPerExecutorNeeded));

    const util: UtilizationSpec = {
      cpuPackingEfficiency: (count * cpe) / allocatable.cpu,
      memPackingEfficiency: Math.min(1, (count * memPerExecutorNeeded) / allocatable.memory),
      strandedCpuPerNode: strandedCpu,
      strandedMemoryPerNode: strandedMem,
      bindingDimension,
    };

    const step = makeStep({
      id: 'executors-per-node',
      title: 'Executors per node',
      formula:
        `by CPU:    floor(${allocatable.cpu.toFixed(1)} cores / ${cpe}) = ${byCpu}\n` +
        `by memory: floor(${fmtBytes(allocatable.memory)} / ${fmtBytes(memPerExecutorNeeded)}) = ${byMem}\n` +
        `executors per node = min(${byCpu}, ${byMem}) = ${count}  (${bindingDimension}-bound)\n` +
        `stranded: ${strandedCpu.toFixed(1)} cores, ${fmtBytes(strandedMem)}`,
      inputs: {
        allocatableCpu: allocatable.cpu,
        allocatableMemory: allocatable.memory,
        coresPerExecutor: cpe,
        memoryNeededPerExecutor: memPerExecutorNeeded,
      },
      outputs: {
        executorsPerNode: count, byCpu, byMem, strandedCpu,
        strandedMemory: strandedMem, bindingDimension,
      },
      rationale:
        'Packed against allocatable capacity in both dimensions, not advertised capacity. ' +
        (bindingDimension === 'memory'
          ? 'Memory is the binding constraint here: there is CPU headroom on the node that ' +
            'cannot be used because each executor needs more memory than the cores would ' +
            'suggest. A node with more memory per core would fit more work.'
          : 'CPU is the binding constraint, which is the usual and healthier case.') +
        (strandedCpu > 0
          ? ` ${strandedCpu.toFixed(1)} cores per node cannot form another executor and will sit idle.`
          : ' Cores divide evenly, so nothing is stranded.'),
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

    /*
     * Cluster size is a TIME decision, not a capacity one.
     *
     * Spark streams partitions, so almost any cluster can eventually process
     * almost any volume -- the question is how long you are willing to wait.
     * Sizing purely to hit a wave count ignores that and grows the cluster
     * without bound: 10 TiB would ask for thousands of nodes to keep 3 waves.
     *
     * So: derive the core count from a runtime target, and let the wave count
     * fall out of it. Where the user gave an SLA, use it. Otherwise assume a
     * reasonable batch window.
     */
    const roughCoresHint = ctx.draft.scratch['roughCores'] as number | undefined;
    const shape = ctx.input.pipeline.dominantQueryShape;
    const perCoreThroughput = throughputValue(
      mulRange(
        BYTES_PER_CORE_SECOND_BY_SHAPE[shape],
        THROUGHPUT_LANGUAGE_FACTOR[ctx.input.runtimeLanguage],
      ),
    );

    const targetSeconds = ctx.input.sla.targetRuntime ?? DEFAULT_BATCH_WINDOW_SECONDS;
    const volume = sizingValue(inflated);

    // cores = work / (rate x time)
    const coresForSla = Math.ceil(volume / (perCoreThroughput * targetSeconds));

    /*
     * Runtime is the sizing criterion. Waves are an OUTCOME, not a constraint.
     *
     * It is tempting to force the wave count into the 2-4 band, but that band
     * is a quality target for hiding stragglers, not a bound on correctness.
     * Partitions are sized to fit a task slot, so a large dataset legitimately
     * produces tens of thousands of them, and demanding 4 waves of those would
     * buy thousands of cores to finish an hour-long job in 33 seconds.
     *
     * Many waves is normal and fine. What matters is finishing in the target
     * time; the floor below only prevents a cluster so small it cannot hide a
     * single straggler.
     */
    const targetBytes = ctx.tunables.targetShufflePartitionBytes;
    const perSlotMemory = memoryPerSlotNeeded(ctx, roughCoresHint);
    const usablePartitionBytes = Math.min(targetBytes, perSlotMemory * MEMORY_FRACTION * 0.7);
    const partitions = Math.max(1, Math.ceil(volume / usablePartitionBytes));

    // Floor: at least two task slots' worth of parallelism, so one slow task
    // does not define the stage. Ceiling: never more cores than partitions.
    const minUsefulCores = cpe * 2;
    const maxUsefulCores = partitions;

    let totalCoresWanted = Math.max(
      minUsefulCores,
      Math.min(coresForSla, maxUsefulCores),
    );
    const slaBound = coresForSla > maxUsefulCores;

    const maxCores = ctx.input.cluster.maxTotalCores;
    if (maxCores) totalCoresWanted = Math.min(totalCoresWanted, maxCores);

    let executorCount = Math.max(1, Math.ceil(totalCoresWanted / cpe));
    const maxNodes = ctx.input.cluster.maxNodes;
    if (maxNodes) executorCount = Math.min(executorCount, maxNodes * epn);

    const nodeCount = Math.ceil(executorCount / epn);
    const totalCores = cores(executorCount * cpe);
    const estimatedSeconds = volume / (perCoreThroughput * totalCores);
    const finalWaves = partitions / (executorCount * cpe);

    const step = makeStep({
      id: 'node-count',
      title: 'Cluster size',
      formula:
        `throughput = ${fmtBytes(perCoreThroughput)} per core-second (${shape}, ` +
        `${ctx.input.runtimeLanguage})\n` +
        `cores for a ${fmtSeconds(targetSeconds)} target = ${fmtBytes(volume)} / ` +
        `(${fmtBytes(perCoreThroughput)} x ${fmtSeconds(targetSeconds)}) = ${coresForSla}\n` +
        `bounded to [${minUsefulCores}, ${maxUsefulCores.toLocaleString()}] cores ` +
        `-> ${totalCoresWanted} cores\n` +
        `${partitions.toLocaleString()} partitions over ${totalCoresWanted} cores = ` +
        `${(partitions / totalCoresWanted).toFixed(0)} waves\n` +
        `executors = ceil(${totalCoresWanted} / ${cpe}) = ${executorCount}\n` +
        `nodes = ceil(${executorCount} / ${epn}) = ${nodeCount}\n` +
        `estimated runtime at this size: ${fmtSeconds(estimatedSeconds)}`,
      inputs: {
        inflatedBytes: inflated,
        perCoreThroughput,
        targetSeconds,
        coresPerExecutor: cpe,
        executorsPerNode: epn,
      },
      outputs: {
        executorCount, nodeCount, totalCores, partitions,
        wavesPerStage: finalWaves,
        estimatedRuntimeSeconds: estimatedSeconds,
      },
      rationale:
        'Cluster size is a decision about time, not capacity. Spark streams partitions, so a ' +
        'small cluster can process a very large dataset -- it simply takes longer. Sizing to ' +
        'hold everything at once would grow the cluster without bound. ' +
        (slaBound
          ? 'The runtime target would justify more cores than there are partitions to run, ' +
            'so the cluster is capped at one core per partition -- beyond that, cores idle.'
          : ctx.input.sla.targetRuntime
            ? `Sized to finish in about ${fmtSeconds(targetSeconds)}, as you asked. A shorter ` +
              'target buys more nodes, a longer one fewer.'
            : `No runtime target was given, so this assumes a ${fmtSeconds(targetSeconds)} ` +
              'batch window. Change the target and the cluster resizes with it.') +
        ` Each stage runs in roughly ${(partitions / totalCoresWanted).toFixed(0)} waves, which ` +
        'is expected at this data size: partitions are sized to fit a task slot, so a large ' +
        'dataset simply has many of them.' +
        ' Throughput per core is the least reliable input in the whole model: it varies with ' +
        'the query, the data and the storage backend, so treat the runtime as an order of ' +
        'magnitude rather than a promise.',
      confidence: 'guess',
    });

    // Publish the runtime estimate so rules and the UI can use it. It is the
    // softest number the engine produces, so it travels as a wide range with
    // its confidence attached rather than as a figure.
    const runtimeRange = {
      low: estimatedSeconds * 0.5,
      mid: estimatedSeconds,
      high: estimatedSeconds * 3,
    };

    return withScratch(withDraft(withStep(ctx, step), { executorCount, nodeCount }), {
      estimatedRuntime: runtimeRange,
    });
  },
};

export { CORES_PER_EXECUTOR_WARN_ABOVE };
