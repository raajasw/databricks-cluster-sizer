import type { Rule } from '../types/rules';
import type { PlatformAdapter } from '../types/platform';
import type { ProfileStrategy } from '../types/profile';
import { sanityRules } from './core/sanity';
import { memoryRules } from './core/memory';

export const coreRules = (): Rule[] => [...sanityRules(), ...memoryRules()];

export const allRules = (adapter: PlatformAdapter, profile: ProfileStrategy): Rule[] => [
  ...coreRules(),
  ...profile.rules(),
  ...adapter.rules(),
];
