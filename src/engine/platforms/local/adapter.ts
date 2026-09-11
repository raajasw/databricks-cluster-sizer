/**
 * Local mode: one JVM, driver and executor in the same process.
 *
 * The simplest adapter, and the one that most often produces the most useful
 * answer -- which is frequently "you do not need Spark for this".
 *
 * Local mode has three traps worth encoding, because essentially every local
 * PySpark user hits at least one:
 *   1. spark.executor.memory is IGNORED. There is no executor process.
 *   2. spark.driver.memory must be set before the JVM starts. Setting it via
 *      SparkSession.builder.config() is a silent no-op.
 *   3. The default 200 shuffle partitions on a 10-core laptop is absurd.
 */

import { bytes, cores, gib, mib, type Bytes } from '../../units';
import {
  LOCAL_MACHINE_RESERVE_FRACTION, DRIVER_MEMORY_FLOOR,
} from '../../constants/heuristics';
import { formatSparkBytes } from '../../units';
import type {
  PlatformAdapter, NodeType, AllocatableCapacity, CoresPerExecutorPolicy,
  PlatformCapabilities, ContainerFootprint, PlatformConfigOutput,
  EmitInput, PipelineContextLike, ConfigEntry,
} from '../../types/platform';
import type { WorkloadInput } from '../../types/input';
import { makeStep, fmtBytes } from '../../pipeline/trace';
import { localRules } from './rules';

const capabilities: PlatformCapabilities = {
  supportsMultipleExecutorsPerNode: false,
  supportsExternalShuffleService: false,
  supportsDynamicAllocation: false,
  dynamicAllocationRequiresShuffleTracking: false,
  supportsOffHeap: false,
  hasSeparateDriverNode: false,
  hasCpuRequestLimitSplit: false,
  userChooses: 'nothing',
};

export const localAdapter: PlatformAdapter = {
  id: 'local',
  displayName: 'Local (single machine)',
  status: 'supported',
  capabilities,

  listNodeTypes(input: WorkloadInput): NodeType[] {
    const local = input.platformInput.local;
    const vcpus = local?.machineCores ?? cores(8);
    const memory = local?.machineMemory ?? gib(16);
    return [{
      id: 'local-machine',
      displayName: 'This machine',
      family: 'general',
      vcpus,
      memoryBytes: memory,
      reserve: {
        // A laptop is not a server: the OS, browser and IDE need real headroom,
        // and swapping is far worse than a smaller heap.
        cpu: cores(1),
        memory: bytes(memory * LOCAL_MACHINE_RESERVE_FRACTION),
        confidence: 'estimated',
      },
      notes: ['Driver and executor share one JVM in local mode.'],
    }];
  },

  allocatable(node: NodeType, _input: WorkloadInput): AllocatableCapacity {
    const cpu = cores(Math.max(1, node.vcpus - node.reserve.cpu));
    const memory = bytes(node.memoryBytes - node.reserve.memory);
    return {
      cpu,
      memory,
      derivation: makeStep({
        id: 'allocatable-capacity',
        title: 'What Spark may use on this machine',
        formula:
          `CPU: ${node.vcpus} - ${node.reserve.cpu} reserved = ${cpu} cores\n` +
          `Memory: ${fmtBytes(node.memoryBytes)} - ${fmtBytes(node.reserve.memory)} ` +
          `(${(LOCAL_MACHINE_RESERVE_FRACTION * 100).toFixed(0)}% for OS/browser/IDE) ` +
          `= ${fmtBytes(memory)}`,
        inputs: { machineCores: node.vcpus, machineMemory: node.memoryBytes },
        outputs: { allocatableCpu: cpu, allocatableMemory: memory },
        rationale:
          `Roughly ${(LOCAL_MACHINE_RESERVE_FRACTION * 100).toFixed(0)}% of the machine is ` +
          'held back deliberately. Handing Spark nearly all of a laptop\'s RAM causes swap ' +
          'thrashing that is far slower than simply running with a smaller heap, and it ' +
          'makes the machine unusable while the job runs.',
        confidence: 'estimated',
      }),
    };
  },

  coresPerExecutorPolicy(_input: WorkloadInput, node: NodeType): CoresPerExecutorPolicy {
    return {
      kind: 'single-jvm',
      reason:
        `Local mode runs one JVM, so all ${node.vcpus} usable cores belong to the single ` +
        'process. Leave at least one core for the OS: local[*] takes every core and makes ' +
        'the machine unresponsive.',
    };
  },

  containerFootprint(heap: Bytes, overhead: Bytes, _ctx: PipelineContextLike): ContainerFootprint {
    // No container here -- this is just the JVM's own footprint.
    return {
      total: bytes(heap + overhead),
      components: [
        { name: 'Driver heap (executor shares it)', bytes: heap, configKey: 'spark.driver.memory' },
        { name: 'JVM overhead (metaspace, code cache, stacks)', bytes: overhead },
      ],
    };
  },

  emitConfig(r: EmitInput, ctx: PipelineContextLike): PlatformConfigOutput {
    const k = Math.max(1, Math.floor(r.coresPerExecutor));
    const driverMem = bytes(Math.max(r.driverHeapBytes, DRIVER_MEMORY_FLOOR));

    const sparkConf: ConfigEntry[] = [
      {
        key: 'spark.master',
        value: `local[${k}]`,
        confidence: 'documented',
        sparkDefault: 'local[*]',
        note: `local[*] would grab all ${ctx.input.platformInput.local?.machineCores ?? '?'} cores and starve the OS.`,
      },
      {
        key: 'spark.driver.memory',
        value: formatSparkBytes(driverMem),
        rationaleStepId: 'driver-sizing',
        confidence: 'documented',
        sparkDefault: '1g',
        note: 'Must be set BEFORE the JVM starts -- see the launch commands below.',
      },
      {
        key: 'spark.executor.memory',
        value: formatSparkBytes(r.heapBytes),
        confidence: 'documented',
        ignored: true,
        note: 'Ignored in local mode: there is no separate executor process. Shown so you know why setting it changes nothing.',
      },
      {
        key: 'spark.sql.shuffle.partitions',
        value: `${r.shufflePartitions}`,
        rationaleStepId: 'shuffle-partitions',
        confidence: 'estimated',
        sparkDefault: '200',
        note: `The 200 default would create ${Math.round(200 / k)} waves of tiny tasks on ${k} cores.`,
      },
      {
        key: 'spark.sql.adaptive.enabled',
        value: 'true',
        confidence: 'documented',
        sparkDefault: 'true',
      },
      {
        key: 'spark.local.dir',
        value: '/tmp/spark-local',
        confidence: 'estimated',
        sparkDefault: '/tmp',
        note: 'Point this at a disk with real free space; shuffle and spill land here.',
      },
    ];

    return {
      kind: 'local',
      sparkSubmitArgs: [
        `--master local[${k}]`,
        `--driver-memory ${formatSparkBytes(driverMem)}`,
        `--conf spark.sql.shuffle.partitions=${r.shufflePartitions}`,
      ],
      sparkConf,
      envVars: {
        // The reliable way to set driver memory for PySpark, because by the time
        // Python code runs the JVM already exists.
        PYSPARK_SUBMIT_ARGS: `--driver-memory ${formatSparkBytes(driverMem)} pyspark-shell`,
        SPARK_DRIVER_MEMORY: formatSparkBytes(driverMem),
      },
    };
  },

  rules: localRules,
};

export { mib };
