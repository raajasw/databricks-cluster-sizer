import type { WorkloadProfile } from '../types/input';
import type { ProfileStrategy } from '../types/profile';
import { batchProfile } from './batch';

/**
 * Profiles other than batch are registered as they are implemented; until then
 * they fall back to baseline batch behaviour rather than failing, so the engine
 * always returns something.
 */
const PROFILES: Partial<Record<WorkloadProfile, ProfileStrategy>> = {
  'batch-etl': batchProfile,
};

export function getProfile(id: WorkloadProfile): ProfileStrategy {
  return PROFILES[id] ?? batchProfile;
}

export const isProfileImplemented = (id: WorkloadProfile): boolean => id in PROFILES;
