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


import type { SchedulerInfo, StagedChange, ValidationIssue } from '~/types';
import { validateQueue } from './service';
import { isBlockingError, isCrossQueueRule } from './ruleCategories';
import { mergeStagedConfig } from '~/utils/configUtils';
import { getAffectedQueuesForValidation } from './utils/affectedQueues';
import { dedupeIssues } from './utils/dedupeIssues';

interface ValidatePropertyChangeOptions {
  propertyName: string;
  propertyValue: string;
  queuePath: string;
  schedulerData: SchedulerInfo | null;
  configData: Map<string, string>;
  stagedChanges: StagedChange[];
  includeBlockingErrors?: boolean;
}

export function validatePropertyChange({
  propertyName,
  propertyValue,
  queuePath,
  schedulerData,
  configData,
  stagedChanges,
  includeBlockingErrors = false,
}: ValidatePropertyChangeOptions): ValidationIssue[] {
  if (!schedulerData) {
    return [];
  }

  const affectedQueues = getAffectedQueuesForValidation(
    propertyName,
    queuePath,
    schedulerData,
    stagedChanges,
  );

  const tempChange: StagedChange = {
    id: `temp-${Date.now()}`,
    type: 'update',
    queuePath,
    property: propertyName,
    oldValue: '',
    newValue: propertyValue,
    timestamp: Date.now(),
  };

  const stagedChangesWithTemp = [...stagedChanges, tempChange];
  const mergedConfig = mergeStagedConfig(configData, stagedChangesWithTemp);

  const issues: ValidationIssue[] = [];

  affectedQueues.forEach((affectedQueuePath) => {
    const queueProperties: Record<string, string> = {};

    mergedConfig.forEach((value, key) => {
      if (key.startsWith(`yarn.scheduler.capacity.${affectedQueuePath}.`)) {
        const property = key.replace(`yarn.scheduler.capacity.${affectedQueuePath}.`, '');
        queueProperties[property] = value;
      }
    });

    const result = validateQueue({
      queuePath: affectedQueuePath,
      properties: queueProperties,
      configData: mergedConfig,
      stagedChanges: stagedChangesWithTemp,
      schedulerData,
    });

    const filtered = includeBlockingErrors
      ? result.issues
      : result.issues.filter((issue) => !isBlockingError(issue.rule, issue.severity));

    const relevantIssues = filtered.filter((issue) => {
      if (issue.queuePath === queuePath && issue.field === propertyName) {
        return true;
      }

      // For parent-child-capacity-mode: only include errors for child queues
      // when their parent is the queue being changed (so parent mode changes
      // show errors for all affected children)
      if (issue.rule === 'parent-child-capacity-mode') {
        // Check if queuePath is the parent of issue.queuePath
        const isParentOfIssueQueue = issue.queuePath.startsWith(`${queuePath}.`);
        return isParentOfIssueQueue;
      }

      // Include other cross-queue issues (like child-capacity-sum on parent,
      // or parent-child-capacity-constraint warnings on children)
      // But only if they're for related properties (capacity-related rules)
      if (isCrossQueueRule(issue.rule)) {
        return true;
      }

      return false;
    });

    issues.push(...relevantIssues);
  });

  return dedupeIssues(issues);
}

export interface ValidateStagedChangesOptions {
  stagedChanges: StagedChange[];
  schedulerData: SchedulerInfo | null;
  configData: Map<string, string>;
  /** Optional: only validate changes affecting these queue paths */
  affectedQueuePaths?: Set<string>;
  /** Optional: only validate changes affecting these properties */
  affectedProperties?: Set<string>;
}

/**
 * Validate staged changes and return a map of change IDs to their validation issues.
 * When affectedQueuePaths and/or affectedProperties are provided, only changes
 * affecting those will be re-validated; others retain their existing validation state.
 */
export function validateStagedChanges({
  stagedChanges,
  schedulerData,
  configData,
  affectedQueuePaths,
  affectedProperties,
}: ValidateStagedChangesOptions): Map<string, ValidationIssue[] | undefined> {
  const validationResults = new Map<string, ValidationIssue[] | undefined>();

  if (!schedulerData || stagedChanges.length === 0) {
    return validationResults;
  }

  const hasFilter = affectedQueuePaths || affectedProperties;

  stagedChanges.forEach((change) => {
    // Check if this change should be validated based on filters
    if (hasFilter) {
      const isAffected =
        (affectedQueuePaths && affectedQueuePaths.has(change.queuePath)) ||
        (affectedProperties && change.property && affectedProperties.has(change.property));

      if (!isAffected) {
        // Keep existing validation state for unaffected changes
        validationResults.set(change.id, change.validationErrors);
        return;
      }
    }

    // Handle 'add' type with capacity property
    if (change.type === 'add' && change.property === 'capacity') {
      const issues = validatePropertyChange({
        propertyName: 'capacity',
        propertyValue: change.newValue || '',
        queuePath: change.queuePath,
        schedulerData,
        configData,
        stagedChanges: stagedChanges.filter((c) => c.id !== change.id),
        includeBlockingErrors: false,
      });

      validationResults.set(change.id, issues.length > 0 ? issues : undefined);
      return;
    }

    // Skip non-update changes or changes without property
    if (change.type !== 'update' || !change.property) {
      validationResults.set(change.id, change.validationErrors);
      return;
    }

    // Validate update changes
    const issues = validatePropertyChange({
      propertyName: change.property,
      propertyValue: change.newValue || '',
      queuePath: change.queuePath,
      schedulerData,
      configData,
      stagedChanges: stagedChanges.filter((c) => c.id !== change.id),
      includeBlockingErrors: false,
    });

    validationResults.set(change.id, issues.length > 0 ? issues : undefined);
  });

  return validationResults;
}
