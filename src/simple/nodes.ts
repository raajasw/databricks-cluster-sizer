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
}

const GB = 1_000_000_000;

const n = (
  id: string, cloud: Cloud, shape: NodeShape,
  cores: number, memoryGB: number, localSsd = false,
): NodeType => ({ id, cloud, shape, cores, memoryBytes: memoryGB * GB, localSsd });

export const NODES: NodeType[] = [
  // --- AWS ---
  n('m5d.xlarge',   'aws', 'balanced', 4,  16, true),
  n('m5d.2xlarge',  'aws', 'balanced', 8,  32, true),
  n('m5d.4xlarge',  'aws', 'balanced', 16, 64, true),
  n('r5d.xlarge',   'aws', 'memory',   4,  32, true),
  n('r5d.2xlarge',  'aws', 'memory',   8,  64, true),
  n('r5d.4xlarge',  'aws', 'memory',   16, 128, true),
  n('i3.xlarge',    'aws', 'storage',  4,  30.5, true),
  n('i3.2xlarge',   'aws', 'storage',  8,  61, true),
  n('i3.4xlarge',   'aws', 'storage',  16, 122, true),

  // --- Azure ---
  n('Standard_D4ds_v5',  'azure', 'balanced', 4,  16, true),
  n('Standard_D8ds_v5',  'azure', 'balanced', 8,  32, true),
  n('Standard_D16ds_v5', 'azure', 'balanced', 16, 64, true),
  n('Standard_E4ds_v5',  'azure', 'memory',   4,  32, true),
  n('Standard_E8ds_v5',  'azure', 'memory',   8,  64, true),
  n('Standard_E16ds_v5', 'azure', 'memory',   16, 128, true),
  n('Standard_L8s_v3',   'azure', 'storage',  8,  64, true),
  n('Standard_L16s_v3',  'azure', 'storage',  16, 128, true),

  // --- GCP ---
  n('n2-standard-4',  'gcp', 'balanced', 4,  16),
  n('n2-standard-8',  'gcp', 'balanced', 8,  32),
  n('n2-standard-16', 'gcp', 'balanced', 16, 64),
  n('n2-highmem-4',   'gcp', 'memory',   4,  32),
  n('n2-highmem-8',   'gcp', 'memory',   8,  64),
  n('n2-highmem-16',  'gcp', 'memory',   16, 128),
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
