/**
 * Kubernetes: executors are pods, and the scheduler enforces the container
 * total rather than the heap.
 *
 * Three things this adapter exists to get right, each a common production
 * failure:
 *   1. Pods are packed against node ALLOCATABLE, not advertised capacity.
 *      Sizing against capacity produces pods that stay Pending forever.
 *   2. Pod memory = heap + overhead + offHeap + pyspark. Off-heap is NOT
 *      inside memoryOverhead; Spark adds it to the request separately.
 *   3. spark.executor.cores, the CPU request, and the CPU limit are three
 *      different numbers. A limit below the core count causes CFS throttling
 *      that looks like randomly slow tasks.
 */

import {
  bytes, cores, gib, formatSparkBytes, formatK8sCpu, type Bytes, type Cores,
} from '../../units';
import {
  CORES_PER_EXECUTOR_MIN, CORES_PER_EXECUTOR_MAX,
} from '../../constants/heuristics';
import type {
  PlatformAdapter, NodeType, AllocatableCapacity, CoresPerExecutorPolicy,
  PlatformCapabilities, ContainerFootprint, PlatformConfigOutput,
  EmitInput, PipelineContextLike, ConfigEntry,
} from '../../types/platform';
import type { WorkloadInput } from '../../types/input';
import { makeStep, fmtBytes } from '../../pipeline/trace';
import { K8S_NODE_TYPES } from './node-catalog';
import { k8sRules } from './rules';

const capabilities: PlatformCapabilities = {
  supportsMultipleExecutorsPerNode: true,
  // No external shuffle service exists for K8s upstream.
  supportsExternalShuffleService: false,
  supportsDynamicAllocation: true,
  dynamicAllocationRequiresShuffleTracking: true,
  supportsOffHeap: true,
  hasSeparateDriverNode: true,
  hasCpuRequestLimitSplit: true,
  userChooses: 'executors',
};

export const kubernetesAdapter: PlatformAdapter = {
  id: 'kubernetes',
  displayName: 'Kubernetes',
  status: 'supported',
  capabilities,

  listNodeTypes(input: WorkloadInput): NodeType[] {
    const k8s = input.platformInput.kubernetes;
    // A user-described node overrides the catalog entirely.
    if (k8s?.nodeCapacityCpu && k8s.nodeCapacityMemory) {
      return [{
        id: 'custom',
        displayName: 'Your node type',
        family: 'general',
        vcpus: k8s.nodeCapacityCpu,
        memoryBytes: k8s.nodeCapacityMemory,
        localSsdBytes: k8s.nodeEphemeralStorage,
        reserve: { cpu: cores(0.5), memory: gib(1.5), confidence: 'estimated' },
      }];
    }
    return K8S_NODE_TYPES;
  },

  allocatable(node: NodeType, input: WorkloadInput): AllocatableCapacity {
    const k8s = input.platformInput.kubernetes;

    const kubeCpu = k8s?.manualKubeReservedCpu ?? node.reserve.cpu;
    const kubeMem = k8s?.manualKubeReservedMemory ?? node.reserve.memory;
    // DaemonSets are cluster-specific: CNI, log shippers, node exporters,
    // service-mesh sidecars. Easy to forget and routinely 1-4 GiB.
    const dsCpu = k8s?.daemonsetCpu ?? cores(0.5);
    const dsMem = k8s?.daemonsetMemory ?? gib(1);

    const cpu = cores(Math.max(0.1, node.vcpus - kubeCpu - dsCpu));
    const memory = bytes(Math.max(0, node.memoryBytes - kubeMem - dsMem));

    return {
      cpu,
      memory,
      ephemeralStorage: node.localSsdBytes,
      derivation: makeStep({
        id: 'allocatable-capacity',
        title: 'What Spark may actually use on a node',
        formula:
          `CPU: ${node.vcpus} - ${kubeCpu.toFixed(2)} kubelet/system - ${dsCpu} DaemonSets ` +
          `= ${cpu.toFixed(2)} cores\n` +
          `Memory: ${fmtBytes(node.memoryBytes)} - ${fmtBytes(kubeMem)} kubelet/system ` +
          `- ${fmtBytes(dsMem)} DaemonSets = ${fmtBytes(memory)}`,
        inputs: {
          capacityCpu: node.vcpus,
          capacityMemory: node.memoryBytes,
          kubeReservedCpu: kubeCpu,
          kubeReservedMemory: kubeMem,
          daemonsetCpu: dsCpu,
          daemonsetMemory: dsMem,
        },
        outputs: { allocatableCpu: cpu, allocatableMemory: memory },
        rationale:
          'Pods are scheduled against allocatable capacity, never the advertised figure. ' +
          'The kubelet and OS take a tiered reservation, the hard eviction threshold takes ' +
          'more, and DaemonSets take whatever your cluster runs on every node. Sizing an ' +
          `executor against the full ${fmtBytes(node.memoryBytes)} is the most common reason ` +
          'pods sit in Pending forever with "Insufficient memory" -- the request simply ' +
          'cannot be satisfied by any node.',
        confidence: 'estimated',
        citations: [
          { label: 'GKE allocatable resources', kind: 'vendor-doc' },
          { label: 'kube-reserved / system-reserved', kind: 'vendor-doc' },
        ],
      }),
    };
  },

  coresPerExecutorPolicy(): CoresPerExecutorPolicy {
    return {
      kind: 'four-to-five',
      min: cores(CORES_PER_EXECUTOR_MIN),
      max: cores(CORES_PER_EXECUTOR_MAX),
      reason:
        'Multiple executor pods per node, each with a modest core count, to keep GC pauses ' +
        'short and limit the blast radius when one task exhausts a heap.',
    };
  },

  containerFootprint(heap: Bytes, overhead: Bytes, ctx: PipelineContextLike): ContainerFootprint {
    const offHeap = ctx.draft.offHeapBytes ?? bytes(0);
    const pyspark = ctx.draft.pysparkMemoryBytes ?? bytes(0);
    const components = [
      { name: 'JVM heap', bytes: heap, configKey: 'spark.executor.memory' },
      { name: 'Overhead (metaspace, stacks, netty buffers)', bytes: overhead,
        configKey: 'spark.executor.memoryOverhead' },
    ];
    if (offHeap > 0) {
      components.push({ name: 'Off-heap execution memory', bytes: offHeap,
        configKey: 'spark.memory.offHeap.size' });
    }
    if (pyspark > 0) {
      components.push({ name: 'Python workers', bytes: pyspark,
        configKey: 'spark.executor.pyspark.memory' });
    }
    return {
      total: bytes(heap + overhead + offHeap + pyspark),
      components,
    };
  },

  emitConfig(r: EmitInput, ctx: PipelineContextLike): PlatformConfigOutput {
    const k8s = ctx.input.platformInput.kubernetes;
    const requestCores = cores(Math.round(r.coresPerExecutor * 0.8 * 1000) / 1000);

    const sparkConf: ConfigEntry[] = [
      { key: 'spark.executor.instances', value: `${r.executorCount}`,
        rationaleStepId: 'node-count', confidence: 'estimated' },
      { key: 'spark.executor.cores', value: `${r.coresPerExecutor}`,
        rationaleStepId: 'cores-per-executor', confidence: 'estimated', sparkDefault: '1',
        note: 'Task slots per executor -- a Spark scheduling number, not a Kubernetes one.' },
      { key: 'spark.kubernetes.executor.request.cores', value: formatK8sCpu(requestCores),
        confidence: 'estimated',
        note: `Requesting ~80% of ${r.coresPerExecutor} cores improves bin packing; the pod ` +
              'can still burst above it.' },
      { key: 'spark.executor.memory', value: formatSparkBytes(r.heapBytes),
        rationaleStepId: 'executor-heap', confidence: 'documented', sparkDefault: '1g' },
      { key: 'spark.executor.memoryOverhead', value: formatSparkBytes(r.overheadBytes),
        rationaleStepId: 'memory-overhead', confidence: 'documented',
        note: 'Added to the pod request on top of the heap, not carved out of it.' },
    ];

    if (r.offHeapBytes > 0) {
      sparkConf.push(
        { key: 'spark.memory.offHeap.enabled', value: 'true', confidence: 'documented' },
        { key: 'spark.memory.offHeap.size', value: formatSparkBytes(r.offHeapBytes),
          confidence: 'estimated',
          note: 'Counts toward the pod memory request separately from overhead.' },
      );
    }
    if (r.pysparkMemoryBytes > 0) {
      sparkConf.push({
        key: 'spark.executor.pyspark.memory',
        value: formatSparkBytes(r.pysparkMemoryBytes),
        confidence: 'estimated',
        note: 'Bounds Python worker RSS so they fail loudly instead of being OOMKilled.',
      });
    }

    sparkConf.push(
      { key: 'spark.driver.memory', value: formatSparkBytes(r.driverHeapBytes),
        rationaleStepId: 'driver-sizing', confidence: 'estimated' },
      { key: 'spark.driver.cores', value: `${r.driverCores}`, confidence: 'estimated' },
      { key: 'spark.sql.shuffle.partitions', value: `${r.shufflePartitions}`,
        rationaleStepId: 'shuffle-partitions', confidence: 'estimated', sparkDefault: '200' },
      { key: 'spark.sql.adaptive.enabled', value: 'true',
        confidence: 'documented', sparkDefault: 'true' },
      { key: 'spark.sql.adaptive.advisoryPartitionSizeInBytes',
        value: formatSparkBytes(r.aqeAdvisoryPartitionBytes),
        confidence: 'estimated', sparkDefault: '64m',
        note: 'With AQE on this is the real partition-sizing knob.' },
    );

    if (k8s?.dynamicAllocation) {
      sparkConf.push(
        { key: 'spark.dynamicAllocation.enabled', value: 'true', confidence: 'documented' },
        { key: 'spark.dynamicAllocation.shuffleTracking.enabled', value: 'true',
          confidence: 'documented',
          note: 'Required on Kubernetes: there is no external shuffle service, so without ' +
                'this executors cannot be released safely.' },
        { key: 'spark.dynamicAllocation.minExecutors',
          value: `${Math.max(1, Math.floor(r.executorCount / 4))}`, confidence: 'estimated' },
        { key: 'spark.dynamicAllocation.maxExecutors', value: `${r.executorCount}`,
          confidence: 'estimated' },
      );
    }

    const nodeId = r.nodeType?.id ?? 'your-node-type';
    const yaml = [
      'apiVersion: sparkoperator.k8s.io/v1beta2',
      'kind: SparkApplication',
      'metadata:',
      '  name: your-job',
      'spec:',
      '  type: Python',
      '  mode: cluster',
      '  image: your-registry/spark:3.5.0',
      '  mainApplicationFile: local:///opt/spark/work-dir/your_job.py',
      '  sparkVersion: "3.5.0"',
      '  driver:',
      `    cores: ${r.driverCores}`,
      `    memory: "${formatSparkBytes(r.driverHeapBytes)}"`,
      '  executor:',
      `    instances: ${r.executorCount}`,
      `    cores: ${r.coresPerExecutor}`,
      `    coreRequest: "${formatK8sCpu(requestCores)}"`,
      `    memory: "${formatSparkBytes(r.heapBytes)}"`,
      `    memoryOverhead: "${formatSparkBytes(r.overheadBytes)}"`,
      '    nodeSelector:',
      `      node.kubernetes.io/instance-type: ${nodeId}`,
    ].join('\n');

    return {
      kind: 'kubernetes',
      sparkSubmitArgs: [
        '--master k8s://https://your-cluster:6443',
        '--deploy-mode cluster',
        `--conf spark.executor.instances=${r.executorCount}`,
        `--conf spark.executor.cores=${r.coresPerExecutor}`,
        `--conf spark.executor.memory=${formatSparkBytes(r.heapBytes)}`,
        `--conf spark.executor.memoryOverhead=${formatSparkBytes(r.overheadBytes)}`,
        `--conf spark.kubernetes.executor.request.cores=${formatK8sCpu(requestCores)}`,
        `--conf spark.sql.shuffle.partitions=${r.shufflePartitions}`,
      ],
      sparkApplicationYaml: yaml,
      sparkConf,
    };
  },

  rules: k8sRules,
};

export { cores as k8sCores };
export type { Cores };
