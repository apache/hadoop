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
 * Validators for placement rules types
 */

import { z } from 'zod';

export const PlacementRuleSchema = z.object({
  type: z.enum(['user', 'group', 'application']),
  matches: z.string().min(1, 'Matches pattern is required'),
  policy: z.enum([
    'specified',
    'primaryGroup',
    'primaryGroupUser',
    'secondaryGroup',
    'secondaryGroupUser',
    'reject',
    'defaultQueue',
    'user',
    'applicationName',
    'custom',
    'setDefaultQueue',
  ]),
  parentQueue: z.string().optional(),
  value: z.string().optional(),
  customPlacement: z.string().optional(),
  create: z.boolean().optional(),
  fallbackResult: z.enum(['skip', 'placeDefault', 'reject']).optional(),
});

export type PlacementRule = z.infer<typeof PlacementRuleSchema>;

/**
 * Type guard to check if a value is a valid PlacementRule
 */
export function isPlacementRule(value: unknown): value is PlacementRule {
  return PlacementRuleSchema.safeParse(value).success;
}

/**
 * Validate and parse a placement rule, throwing if invalid
 */
export function validatePlacementRule(value: unknown): PlacementRule {
  return PlacementRuleSchema.parse(value);
}

/**
 * Check if a placement rule is valid without throwing
 */
export function isValidPlacementRule(
  value: unknown,
): { valid: true; data: PlacementRule } | { valid: false; error: z.ZodError } {
  const result = PlacementRuleSchema.safeParse(value);
  if (result.success) {
    return { valid: true, data: result.data };
  }
  return { valid: false, error: result.error };
}
