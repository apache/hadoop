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

    // Only include issues that are relevant to the queue being changed.
    // This prevents duplicate errors when multiple sibling queues have the same issue.
    const relevantIssues = filtered.filter((issue) => {
      // Always include issues for the queue being directly edited
      if (issue.queuePath === queuePath) {
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
      if (isCrossQueueRule(issue.rule)) {
        return true;
      }

      return false;
    });

    issues.push(...relevantIssues);
  });

  return dedupeIssues(issues);
}

interface ValidateAllStagedChangesOptions {
  stagedChanges: StagedChange[];
  schedulerData: SchedulerInfo | null;
  configData: Map<string, string>;
}

export function validateAllStagedChanges({
  stagedChanges,
  schedulerData,
  configData,
}: ValidateAllStagedChangesOptions): Map<string, ValidationIssue[] | undefined> {
  const validationResults = new Map<string, ValidationIssue[] | undefined>();

  if (!schedulerData || stagedChanges.length === 0) {
    return validationResults;
  }

  stagedChanges.forEach((change) => {
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

    if (change.type !== 'update' || !change.property) {
      validationResults.set(change.id, change.validationErrors);
      return;
    }

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

interface SelectiveValidateOptions {
  affectedQueuePaths: Set<string>;
  affectedProperties: Set<string>;
  stagedChanges: StagedChange[];
  schedulerData: SchedulerInfo | null;
  configData: Map<string, string>;
}

export function selectivelyValidateStagedChanges({
  affectedQueuePaths,
  affectedProperties,
  stagedChanges,
  schedulerData,
  configData,
}: SelectiveValidateOptions): Map<string, ValidationIssue[] | undefined> {
  const validationResults = new Map<string, ValidationIssue[] | undefined>();

  if (!schedulerData || stagedChanges.length === 0) {
    return validationResults;
  }

  stagedChanges.forEach((change) => {
    const isAffected =
      affectedQueuePaths.has(change.queuePath) ||
      (change.property && affectedProperties.has(change.property));

    if (!isAffected) {
      validationResults.set(change.id, change.validationErrors);
      return;
    }

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

    if (change.type !== 'update' || !change.property) {
      validationResults.set(change.id, change.validationErrors);
      return;
    }

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

function dedupeIssues(issues: ValidationIssue[]): ValidationIssue[] {
  const seen = new Set<string>();
  const result: ValidationIssue[] = [];

  issues.forEach((issue) => {
    const key = `${issue.queuePath}|${issue.field}|${issue.rule}|${issue.message}|${issue.severity}`;
    if (!seen.has(key)) {
      seen.add(key);
      result.push(issue);
    }
  });

  return result;
}
