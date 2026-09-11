/**
 * Kubernetes-specific rules: the failures that only happen because executors
 * are pods.
 */

import { BIN_PACK_WASTE_WARN_FRACTION } from '../../constants/heuristics';
import { fmtBytes } from '../../pipeline/trace';
import type { Rule } from '../../types/rules';

const cpuLimitThrottling: Rule = {
  id: 'k8s-cpu-limit-throttling',
  category: 'platform',
  defaultSeverity: 'warning',
  appliesTo: { platforms: ['kubernetes'] },
  evaluate(ctx) {
    if (!ctx.input.platformInput.kubernetes?.setCpuLimit) return null;
    return {
      severity: 'warning',
      title: 'A CPU limit will throttle your executors',
      message:
        `Each executor runs ${ctx.result.executor.coresPerExecutor} tasks in parallel, so it ` +
        'genuinely wants that many cores. A CPU limit imposes a CFS quota: once the pod ' +
        'exhausts its slice within a 100ms period it is frozen until the next one. The ' +
        'symptom is tasks that are randomly slow with no GC activity and no I/O wait, which ' +
        'is very hard to diagnose. Unless you have a hard multi-tenancy requirement, set a ' +
        'CPU request and no limit, leaving the pod burstable.',
      evidence: [
        { label: 'Cores per executor', value: `${ctx.result.executor.coresPerExecutor}`,
          stepId: 'cores-per-executor' },
        { label: 'CPU limit', value: 'set' },
      ],
      confidence: 'documented',
      impact: 75,
    };
  },
};

const dynamicAllocationNoShuffleTracking: Rule = {
  id: 'k8s-dynamic-allocation-no-shuffle-tracking',
  category: 'platform',
  defaultSeverity: 'critical',
  appliesTo: { platforms: ['kubernetes'] },
  evaluate(ctx) {
    const k8s = ctx.input.platformInput.kubernetes;
    if (!k8s?.dynamicAllocation || k8s.shuffleTrackingEnabled) return null;
    return {
      severity: 'critical',
      title: 'Dynamic allocation on Kubernetes needs shuffle tracking',
      message:
        'Upstream Spark has no external shuffle service on Kubernetes, so when an executor is ' +
        'released its shuffle files go with it. Without ' +
        'spark.dynamicAllocation.shuffleTracking.enabled=true, Spark either refuses to scale ' +
        'down or drops shuffle data that later stages still need, forcing recomputation. ' +
        'Turn shuffle tracking on, or use decommissioning (spark.decommission.enabled) to ' +
        'migrate blocks before an executor goes away.',
      evidence: [
        { label: 'Dynamic allocation', value: 'enabled' },
        { label: 'Shuffle tracking', value: 'disabled' },
      ],
      confidence: 'documented',
      impact: 90,
    };
  },
};

const binPackingWaste: Rule = {
  id: 'k8s-bin-packing-waste',
  category: 'platform',
  defaultSeverity: 'warning',
  appliesTo: { platforms: ['kubernetes'] },
  evaluate(ctx) {
    const u = ctx.result.utilization;
    const node = ctx.result.nodeType;
    if (!node) return null;
    const worst = Math.min(u.cpuPackingEfficiency, u.memPackingEfficiency);
    if (worst >= 1 - BIN_PACK_WASTE_WARN_FRACTION) return null;

    return {
      severity: 'warning',
      title: `Each node wastes about ${Math.round((1 - worst) * 100)}% of its capacity`,
      message:
        `${ctx.result.executor.count} executors of ${ctx.result.executor.coresPerExecutor} ` +
        `cores do not tile ${node.displayName} evenly: ` +
        `${u.strandedCpuPerNode.toFixed(1)} cores and ${fmtBytes(u.strandedMemoryPerNode)} per ` +
        'node cannot form another executor and will sit idle. You are paying for capacity ' +
        'that cannot be used. Either pick a node whose core count divides evenly by the ' +
        'executor size, or adjust cores per executor to fit the node you have.',
      evidence: [
        { label: 'Node', value: node.displayName },
        { label: 'Stranded CPU', value: `${u.strandedCpuPerNode.toFixed(1)} cores` },
        { label: 'Stranded memory', value: fmtBytes(u.strandedMemoryPerNode) },
        { label: 'Binding dimension', value: u.bindingDimension },
      ],
      confidence: 'estimated',
      impact: 60,
    };
  },
};

const tmpfsShuffleUnaccounted: Rule = {
  id: 'k8s-tmpfs-shuffle-unaccounted',
  category: 'storage',
  defaultSeverity: 'critical',
  appliesTo: { platforms: ['kubernetes'] },
  evaluate(ctx) {
    if (ctx.input.platformInput.kubernetes?.shuffleStorage !== 'tmpfs') return null;
    const need = ctx.result.storage.requiredLocalDiskPerExecutorBytes;
    return {
      severity: 'critical',
      title: 'tmpfs shuffle storage comes out of pod memory',
      message:
        `With spark.kubernetes.local.dirs.tmpfs=true the shuffle directory is a RAM disk, so ` +
        `the roughly ${fmtBytes(need)} of shuffle and spill per executor counts against the ` +
        'pod memory limit rather than against disk. Unless that is added to memoryOverhead, ' +
        'the kernel will OOMKill the pod partway through the job (exit 137). tmpfs is fast, ' +
        'but it must be budgeted as memory.',
      evidence: [
        { label: 'Shuffle storage', value: 'tmpfs (RAM)' },
        { label: 'Estimated need per executor', value: fmtBytes(need), stepId: 'shuffle-storage' },
        { label: 'Current overhead', value: fmtBytes(ctx.result.executor.overheadBytes) },
      ],
      confidence: 'documented',
      impact: 85,
    };
  },
};

const noLocalSsdShuffleHeavy: Rule = {
  id: 'k8s-no-local-ssd-shuffle-heavy',
  category: 'storage',
  defaultSeverity: 'warning',
  appliesTo: { platforms: ['kubernetes'] },
  evaluate(ctx) {
    const node = ctx.result.nodeType;
    if (!node || node.localSsdBytes) return null;
    if (ctx.input.pipeline.shuffleStages < 2) return null;
    const shuffle = ctx.result.storage.peakShuffleOnDiskBytes.mid;

    return {
      severity: 'warning',
      title: 'Shuffle-heavy job on nodes without local NVMe',
      message:
        `This pipeline writes roughly ${fmtBytes(shuffle)} of shuffle across its stages, but ` +
        `${node.displayName} has no local NVMe, so that traffic goes to network-attached ` +
        'storage. Shuffle write and read then become the bottleneck regardless of how many ' +
        'cores you add. A d-suffix or i-family node with local NVMe is usually a large win ' +
        'for this shape of work.',
      evidence: [
        { label: 'Node', value: node.displayName },
        { label: 'Shuffle volume', value: fmtBytes(shuffle), stepId: 'shuffle-storage' },
        { label: 'Shuffle stages', value: `${ctx.input.pipeline.shuffleStages}` },
      ],
      confidence: 'estimated',
      impact: 65,
    };
  },
};

const wrongInstanceFamily: Rule = {
  id: 'k8s-memory-per-core-imbalanced',
  category: 'platform',
  defaultSeverity: 'warning',
  appliesTo: { platforms: ['kubernetes'] },
  evaluate(ctx) {
    const node = ctx.result.nodeType;
    if (!node) return null;
    const nodeRatio = node.memoryBytes / node.vcpus / 1024 ** 3;
    const needed = ctx.result.memory.workingSetBytes.mid /
      Math.max(1, ctx.result.parallelism.totalTaskSlots) / 1024 ** 3;
    if (needed <= 0) return null;

    const ratio = nodeRatio / needed;
    if (ratio > 0.5 && ratio < 3) return null;

    const tooMuch = ratio >= 3;
    return {
      severity: 'warning',
      title: tooMuch
        ? 'You are paying for memory this workload will not use'
        : 'This node may be memory-starved for the work',
      message: tooMuch
        ? `${node.displayName} provides ${nodeRatio.toFixed(1)} GiB per core, but the workload ` +
          `needs roughly ${needed.toFixed(1)} GiB per task slot. Memory-optimized instances ` +
          'cost substantially more per core, and here that premium buys nothing: this job is ' +
          'CPU-bound. A compute-optimized node gives you more cores for the same spend.'
        : `${node.displayName} provides only ${nodeRatio.toFixed(1)} GiB per core against a ` +
          `working set of about ${needed.toFixed(1)} GiB per task slot. Expect heavy spill. ` +
          'A memory-optimized node, or fewer cores per executor, would give each task more room.',
      evidence: [
        { label: 'Node', value: `${node.displayName} (${node.family})` },
        { label: 'Memory per core', value: `${nodeRatio.toFixed(1)} GiB` },
        { label: 'Needed per task', value: `${needed.toFixed(1)} GiB` },
      ],
      confidence: 'estimated',
      impact: 70,
    };
  },
};

export const k8sRules = (): Rule[] => [
  cpuLimitThrottling,
  dynamicAllocationNoShuffleTracking,
  binPackingWaste,
  tmpfsShuffleUnaccounted,
  noLocalSsdShuffleHeavy,
  wrongInstanceFamily,
];
