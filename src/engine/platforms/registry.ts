import type { PlatformId } from '../types/input';
import type { PlatformAdapter } from '../types/platform';
import { localAdapter } from './local/adapter';

const ADAPTERS: Partial<Record<PlatformId, PlatformAdapter>> = {
  local: localAdapter,
};

export function getAdapter(id: PlatformId): PlatformAdapter {
  const a = ADAPTERS[id];
  if (!a) throw new Error(`Platform "${id}" is not implemented yet`);
  return a;
}

export const listAdapters = (): PlatformAdapter[] => Object.values(ADAPTERS) as PlatformAdapter[];
export const isPlatformImplemented = (id: PlatformId): boolean => id in ADAPTERS;
