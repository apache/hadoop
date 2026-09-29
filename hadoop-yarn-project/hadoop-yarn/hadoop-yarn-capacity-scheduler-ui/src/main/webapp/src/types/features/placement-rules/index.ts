/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */


/**
 * Types for YARN placement rules feature
 */

// Import PlacementRule type for local use, then re-export
import type { PlacementRule } from './validators';
export type { PlacementRule };
export {
  PlacementRuleSchema,
  isPlacementRule,
  validatePlacementRule,
  isValidPlacementRule,
} from './validators';

export type RuleType = 'user' | 'group' | 'application';

export type PlacementPolicy =
  | 'specified'
  | 'primaryGroup'
  | 'primaryGroupUser'
  | 'secondaryGroup'
  | 'secondaryGroupUser'
  | 'reject'
  | 'defaultQueue'
  | 'user'
  | 'applicationName'
  | 'custom'
  | 'setDefaultQueue';

export type FallbackResult = 'skip' | 'placeDefault' | 'reject';

export interface PlacementRulesConfig {
  rules: PlacementRule[];
}

export interface LegacyRuleFormat {
  raw: string;
  type: 'user' | 'group';
  source: string;
  target: string;
}

export interface PlacementRulesData {
  format: 'json' | 'legacy' | 'none';
  rules?: PlacementRule[];
  legacyRules?: string;
  requiresMigration?: boolean;
  inconsistentFormat?: boolean; // True when JSON rules exist but format property is not 'json'
}

export interface MigrationResult {
  success: boolean;
  rules: PlacementRule[];
  errors: string[];
}
