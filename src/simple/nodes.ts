/**
 * Databricks worker node types, per cloud.
 *
 * A deliberately short list. Databricks offers hundreds of instance types; for
 * sizing a batch job only three shapes matter:
 *
 *   balanced   - ~4 GB per core. The sensible default.
 *   memory     - ~8 GB per core. For big joins, sorts and heavy caching.
 *   storage    - balanced ratio but with fast local SSD, which matters a lot
 *                when a job shuffles, because shuffle writes hit local disk.
 *
 * Memory is quoted the way the cloud vendors quote it (decimal GB) and
 * converted once here, so the rest of the code only ever sees bytes.
 */

export type Cloud = 'aws' | 'azure' | 'gcp';
export type NodeShape = 'balanced' | 'memory' | 'storage';

export interface NodeType {
  id: string;
  cloud: Cloud;
  shape: NodeShape;
  cores: number;
  memoryBytes: number;
  /** Local SSD makes shuffle-heavy jobs markedly faster. */
  localSsd: boolean;
  /**
   * How much work one of this node's cores does, relative to the core the
   * throughput anchor was measured on. See CPU_FACTOR below.
   */
  cpuFactor: number;
  /** The processor generation, for explaining the factor above in one line. */
  cpu: string;
}

const GB = 1_000_000_000;

/**
 * Per-core speed, relative to the core the throughput anchor was measured on.
 *
 * The anchor (see MB_PER_CORE_SECOND in size.ts) ran on i3.2xlarge, whose
 * processor the TPC-DS Full Disclosure Report names as the Intel Xeon E5-2686
 * v4 - Broadwell, 2016. So 1.0 here means "as fast as a 2016 Broadwell core",
 * and every modern core is worth more than one of them.
 *
 * Treating all cores as equal was a real source of error: it silently assumed
 * a 2016 core and a 2023 core get through the same bytes per second, which
 * over-provisions any cluster built from current instance types.
 *
 * The figures are deliberately conservative, taken from the low end of what
 * the vendors publish:
 *
 *   Ice Lake (i4i, 3.5 GHz vs i3's 3.0 GHz) is documented at ~1.5x per core on
 *   database work and 2.2-2.7x per vCPU on IO-heavy work. Sizing is closer to
 *   the former, so 1.5 is used rather than the headline number.
 *
 *   Graviton3 is credited by AWS with up to 25% more compute than Graviton2
 *   and competitive with contemporary x86; it sits alongside Ice Lake here.
 *
 * These scale throughput only. They say nothing about price, and a faster core
 * usually costs more per hour - this tool does not price anything.
 */
const CPU_FACTOR = {
  /** Broadwell / early Skylake: the generation the anchor was measured on. */
  legacy: 1.0,
  /** Cascade Lake and similar: a modest step up. */
  mid: 1.25,
  /** Ice Lake, Sapphire Rapids, Graviton3 and later. */
  modern: 1.5,
} as const;

const n = (
  id: string, cloud: Cloud, shape: NodeShape,
  cores: number, memoryGB: number, localSsd = false,
  cpuFactor: number = CPU_FACTOR.legacy, cpu = 'Broadwell/Skylake',
): NodeType => ({
  id, cloud, shape, cores, memoryBytes: memoryGB * GB, localSsd, cpuFactor, cpu,
});

const ICE = CPU_FACTOR.modern;
const OLD = CPU_FACTOR.legacy;
const MID = CPU_FACTOR.mid;

export const NODES: NodeType[] = [
  // --- AWS ---
  // Current generation first: these are what a new workspace should pick.
  n('m6id.xlarge',  'aws', 'balanced', 4,  16, true,  ICE, 'Ice Lake'),
  n('m6id.2xlarge', 'aws', 'balanced', 8,  32, true,  ICE, 'Ice Lake'),
  n('m6id.4xlarge', 'aws', 'balanced', 16, 64, true,  ICE, 'Ice Lake'),
  n('r6id.xlarge',  'aws', 'memory',   4,  32, true,  ICE, 'Ice Lake'),
  n('r6id.2xlarge', 'aws', 'memory',   8,  64, true,  ICE, 'Ice Lake'),
  n('r6id.4xlarge', 'aws', 'memory',   16, 128, true, ICE, 'Ice Lake'),
  n('i4i.xlarge',   'aws', 'storage',  4,  32, true,  ICE, 'Ice Lake'),
  n('i4i.2xlarge',  'aws', 'storage',  8,  64, true,  ICE, 'Ice Lake'),
  n('i4i.4xlarge',  'aws', 'storage',  16, 128, true, ICE, 'Ice Lake'),
  // Previous generation, kept because plenty of workspaces still run them -
  // and because i3.2xlarge is the node the throughput anchor was measured on.
  n('m5d.2xlarge',  'aws', 'balanced', 8,  32, true,  OLD, 'Skylake'),
  n('r5d.2xlarge',  'aws', 'memory',   8,  64, true,  OLD, 'Skylake'),
  n('i3.2xlarge',   'aws', 'storage',  8,  61, true,  OLD, 'Broadwell'),
  n('i3.4xlarge',   'aws', 'storage',  16, 122, true, OLD, 'Broadwell'),

  // --- Azure ---
  // The v5 family is Ice Lake; the older v4/v3 families are Cascade Lake.
  n('Standard_D4ds_v5',  'azure', 'balanced', 4,  16, true,  ICE, 'Ice Lake'),
  n('Standard_D8ds_v5',  'azure', 'balanced', 8,  32, true,  ICE, 'Ice Lake'),
  n('Standard_D16ds_v5', 'azure', 'balanced', 16, 64, true,  ICE, 'Ice Lake'),
  n('Standard_E4ds_v5',  'azure', 'memory',   4,  32, true,  ICE, 'Ice Lake'),
  n('Standard_E8ds_v5',  'azure', 'memory',   8,  64, true,  ICE, 'Ice Lake'),
  n('Standard_E16ds_v5', 'azure', 'memory',   16, 128, true, ICE, 'Ice Lake'),
  n('Standard_L8s_v3',   'azure', 'storage',  8,  64, true,  MID, 'Cascade Lake'),
  n('Standard_L16s_v3',  'azure', 'storage',  16, 128, true, MID, 'Cascade Lake'),

  // --- GCP ---
  // n2 is Cascade Lake; c3 is Sapphire Rapids and has local SSD options.
  n('c3-standard-4-lssd',  'gcp', 'balanced', 4,  16, true, ICE, 'Sapphire Rapids'),
  n('c3-standard-8-lssd',  'gcp', 'balanced', 8,  32, true, ICE, 'Sapphire Rapids'),
  n('c3-standard-16-lssd', 'gcp', 'storage',  16, 64, true, ICE, 'Sapphire Rapids'),
  n('n2-standard-8',  'gcp', 'balanced', 8,  32, false, MID, 'Cascade Lake'),
  n('n2-standard-16', 'gcp', 'balanced', 16, 64, false, MID, 'Cascade Lake'),
  n('n2-highmem-8',   'gcp', 'memory',   8,  64, false, MID, 'Cascade Lake'),
  n('n2-highmem-16',  'gcp', 'memory',   16, 128, false, MID, 'Cascade Lake'),
];

export const nodesFor = (cloud: Cloud): NodeType[] =>
  NODES.filter((x) => x.cloud === cloud);

export const findNode = (id: string): NodeType | undefined =>
  NODES.find((x) => x.id === id);

export const CLOUD_LABEL: Record<Cloud, string> = {
  aws: 'AWS',
  azure: 'Azure',
  gcp: 'GCP',
};

export { CPU_FACTOR };
