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
 * Migration utilities for converting legacy placement rules to JSON format
 */

import type { PlacementRule, MigrationResult } from '~/types/features/placement-rules';

/**
 * Convert legacy placement rules to JSON format
 * Legacy format: "u:alice:root.users.alice,g:developers:root.teams.dev"
 */
export const migrateLegacyRules = (legacyRules: string): MigrationResult => {
  const rules: PlacementRule[] = [];
  const errors: string[] = [];

  if (!legacyRules || !legacyRules.trim()) {
    return { success: true, rules: [], errors: [] };
  }

  const ruleStrings = legacyRules
    .split(',')
    .map((r) => r.trim())
    .filter(Boolean);

  for (const ruleString of ruleStrings) {
    try {
      const rule = convertLegacyRule(ruleString);
      rules.push(rule);
    } catch (error) {
      errors.push(
        `Failed to convert rule "${ruleString}": ${error instanceof Error ? error.message : 'Unknown error'}`,
      );
    }
  }

  return { success: errors.length === 0, rules, errors };
};

/**
 * Convert a single legacy rule to JSON format
 */
function convertLegacyRule(legacyRule: string): PlacementRule {
  // Parse the legacy rule
  const parts = legacyRule.split(':');

  if (parts.length < 2) {
    throw new Error('Invalid rule format');
  }

  let type: 'user' | 'group' | 'application';
  let matcher: string;
  let targetQueue: string;

  // Determine rule type and parse components
  if (parts[0] === 'u') {
    type = 'user';
    matcher = parts[1];
    targetQueue = parts.slice(2).join(':'); // Handle colons in queue names
  } else if (parts[0] === 'g') {
    type = 'group';
    matcher = parts[1];
    targetQueue = parts.slice(2).join(':');
  } else {
    // No prefix means application rule
    type = 'application';
    matcher = parts[0];
    targetQueue = parts.slice(1).join(':');
  }

  // Validate that we have all required parts
  if (!matcher || !targetQueue) {
    throw new Error('Missing matcher or target queue');
  }

  // Convert %user to * for user rules
  if (type === 'user' && matcher === '%user') {
    matcher = '*';
  }

  // Analyze the target queue to determine policy
  const { parentQueue, leafName } = parseQueuePath(targetQueue);
  const policy = determinePolicy(type, leafName, parentQueue);

  // Build the JSON rule
  const rule: PlacementRule = {
    type,
    matches: matcher,
    policy,
    fallbackResult: 'placeDefault',
    create: true,
  };

  // Add additional fields based on policy
  if (policy === 'custom') {
    rule.customPlacement = targetQueue;
  } else if (policy === 'primaryGroupUser' || policy === 'secondaryGroupUser') {
    // For primaryGroupUser/secondaryGroupUser, remove the group variable from parent
    const cleanedParent = parentQueue
      .replace(/\.%primary_group$/, '')
      .replace(/\.%secondary_group$/, '');
    if (cleanedParent) {
      rule.parentQueue = cleanedParent;
    }
  } else if (
    parentQueue &&
    policy !== 'defaultQueue' &&
    policy !== 'reject' &&
    policy !== 'setDefaultQueue'
  ) {
    // For other policies that use variables, set the parent queue
    rule.parentQueue = parentQueue;
  }

  return rule;
}

/**
 * Parse queue path into parent and leaf components
 */
function parseQueuePath(queuePath: string): { parentQueue: string; leafName: string } {
  const lastDotIndex = queuePath.lastIndexOf('.');

  if (lastDotIndex === -1) {
    return { parentQueue: '', leafName: queuePath };
  }

  return {
    parentQueue: queuePath.substring(0, lastDotIndex),
    leafName: queuePath.substring(lastDotIndex + 1),
  };
}

/**
 * Determine the policy based on rule type and target queue structure
 */
function determinePolicy(
  type: 'user' | 'group' | 'application',
  leafName: string,
  parentQueue: string,
): PlacementRule['policy'] {
  // Special cases for user rules
  if (type === 'user') {
    // Check for primaryGroupUser pattern
    if (parentQueue.endsWith('%primary_group') && leafName === '%user') {
      return 'primaryGroupUser';
    }
    // Check for secondaryGroupUser pattern
    if (parentQueue.endsWith('%secondary_group') && leafName === '%user') {
      return 'secondaryGroupUser';
    }
  }

  // Standard variable-based policies
  switch (leafName) {
    case '%user':
      return 'user';
    case '%primary_group':
      return 'primaryGroup';
    case '%secondary_group':
      return 'secondaryGroup';
    case '%application':
      // Only use applicationName policy for application type rules
      // For user/group rules with %application, use custom
      return type === 'application' ? 'defaultQueue' : 'custom';
    default:
      return 'custom';
  }
}
