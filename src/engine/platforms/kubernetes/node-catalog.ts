/**
 * Node catalog for Kubernetes clusters.
 *
 * Instance specs are from the cloud vendors' published tables, which quote
 * memory in DECIMAL GB. They are converted at ingest so the engine only ever
 * handles binary bytes -- the ~7% gap between the two is exactly the kind of
 * silent undersizing the branded units exist to prevent.
 *
 * The `reserve` field is the kubelet + system reservation, NOT DaemonSets:
 * those vary per cluster and are collected separately as user input.
 */

import { cores, decimalGB, gib, type Bytes, type Cores } from '../../units';
import type { NodeType } from '../../types/platform';

/**
 * GKE's published kube-reserved memory formula, tiered by node memory:
 *   255 MiB for the first GB, then 25% of the next 4GB, 20% of the next 4GB,
 *   10% of the next 8GB, 6% of the next 112GB, 2% above 128GB.
 * EKS and AKS differ in detail; this is close enough for sizing and is flagged
 * as estimated rather than measured.
 */
export function kubeReservedMemory(capacity: Bytes): Bytes {
  const gb = capacity / 1024 ** 3;
  let reserved = 0;
  let remaining = gb;

  const take = (amount: number, rate: number) => {
    const slice = Math.min(remaining, amount);
    if (slice > 0) { reserved += slice * rate; remaining -= slice; }
  };

  reserved += 0.255; // first GB
  remaining -= 1;
  take(4, 0.25);
  take(4, 0.20);
  take(8, 0.10);
  take(112, 0.06);
  take(remaining, 0.02);

  // Plus the default hard eviction threshold.
  return gib(reserved + 0.1) as Bytes;
}

/** kube-reserved CPU: 6% of the first core, 1% of the next, 0.5% of the next 2, 0.25% beyond. */
export function kubeReservedCpu(vcpus: Cores): Cores {
  let reserved = 0;
  let remaining: number = vcpus;
  const take = (amount: number, rate: number) => {
    const slice = Math.min(remaining, amount);
    if (slice > 0) { reserved += slice * rate; remaining -= slice; }
  };
  take(1, 0.06);
  take(1, 0.01);
  take(2, 0.005);
  take(remaining, 0.0025);
  return cores(reserved);
}

function node(
  id: string,
  displayName: string,
  family: NodeType['family'],
  vcpus: number,
  memoryGB: number,
  opts: { localSsdGB?: number; localSsdCount?: number; networkGbps?: number; notes?: string[] } = {},
): NodeType {
  const memoryBytes = decimalGB(memoryGB);
  return {
    id,
    displayName,
    family,
    vcpus: cores(vcpus),
    memoryBytes,
    localSsdBytes: opts.localSsdGB ? decimalGB(opts.localSsdGB) : undefined,
    localSsdCount: opts.localSsdCount,
    networkGbps: opts.networkGbps,
    reserve: {
      cpu: kubeReservedCpu(cores(vcpus)),
      memory: kubeReservedMemory(memoryBytes),
      confidence: 'estimated',
    },
    notes: opts.notes,
  };
}

/**
 * A deliberately small, representative catalog spanning the three shapes that
 * matter for Spark: balanced, CPU-dense, and memory-dense -- plus
 * storage-optimized variants with local NVMe, which matter enormously for
 * shuffle-heavy work.
 */
export const K8S_NODE_TYPES: NodeType[] = [
  // General purpose: ~4 GB per core.
  node('m5.2xlarge', 'm5.2xlarge', 'general', 8, 32),
  node('m5.4xlarge', 'm5.4xlarge', 'general', 16, 64),
  node('m5.8xlarge', 'm5.8xlarge', 'general', 32, 128),
  node('m5d.4xlarge', 'm5d.4xlarge (local NVMe)', 'general', 16, 64,
    { localSsdGB: 600, localSsdCount: 2, notes: ['Local NVMe suits shuffle-heavy jobs.'] }),

  // Compute optimized: ~2 GB per core. Right when you are CPU-bound.
  node('c5.4xlarge', 'c5.4xlarge', 'compute-optimized', 16, 32),
  node('c5.9xlarge', 'c5.9xlarge', 'compute-optimized', 36, 72),
  node('c5d.9xlarge', 'c5d.9xlarge (local NVMe)', 'compute-optimized', 36, 72,
    { localSsdGB: 900, localSsdCount: 1 }),

  // Memory optimized: ~8 GB per core. For cache-heavy or wide-shuffle work.
  node('r5.2xlarge', 'r5.2xlarge', 'memory-optimized', 8, 64),
  node('r5.4xlarge', 'r5.4xlarge', 'memory-optimized', 16, 128),
  node('r5.8xlarge', 'r5.8xlarge', 'memory-optimized', 32, 256),
  node('r5d.4xlarge', 'r5d.4xlarge (local NVMe)', 'memory-optimized', 16, 128,
    { localSsdGB: 600, localSsdCount: 2 }),

  // Storage optimized: local NVMe is the point.
  node('i3.4xlarge', 'i3.4xlarge (NVMe)', 'storage-optimized', 16, 122,
    { localSsdGB: 3800, localSsdCount: 2,
      notes: ['Large local NVMe; strong choice for very shuffle-heavy pipelines.'] }),
];

export const memoryPerCore = (n: NodeType): number => n.memoryBytes / n.vcpus;
