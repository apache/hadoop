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
 * Capacity validation utilities
 *
 * These functions extract changes from capacity editor drafts
 * and validate capacity configurations.
 */

import type { CapacityRowDraft } from '~/stores/slices/capacityEditorSlice';
import type { SchedulerStore } from '~/stores/schedulerStore';
import {
  convertVectorDraftToString,
  DEFAULT_PARTITION_VALUE,
  getPropertyNameForLabel,
} from './capacityEditor';
import { buildPropertyKey } from '~/utils/propertyUtils';
import { validateQueue } from '~/features/validation/service';
import type { ValidationIssue, StagedChange, SchedulerInfo } from '~/types';

const ACCESSIBLE_NODE_LABELS_PROPERTY = 'accessible-node-labels';
const LABEL_PARTITION_ACCESS_RULE = 'label-partition-access';

export type QueuePropertyReader = Pick<SchedulerStore, 'hasQueueProperty' | 'getQueuePropertyValue'>;

export interface DraftCacheEntry {
  drafts: Record<string, CapacityRowDraft>;
  draftOrder: string[];
}

/**
 * Returns whether a queue lists the label in its own accessible-node-labels property.
 * Parent inheritance is intentionally not considered.
 */
export function queueListsAccessibleNodeLabel(
  queuePath: string,
  label: string,
  store: QueuePropertyReader,
): boolean {
  if (!store.hasQueueProperty(queuePath, ACCESSIBLE_NODE_LABELS_PROPERTY)) {
    return false;
  }

  const accessibleLabels = store
    .getQueuePropertyValue(queuePath, ACCESSIBLE_NODE_LABELS_PROPERTY)
    .value.trim();
  if (!accessibleLabels) {
    return false;
  }

  const labels = accessibleLabels.split(',').map((entry) => entry.trim());
  return labels.includes('*') || labels.includes(label);
}

const getDraftCapacityValues = (draft: CapacityRowDraft) => ({
  capacityValue:
    draft.mode === 'vector'
      ? convertVectorDraftToString(draft.vectorCapacity).trim()
      : draft.capacityValue.trim(),
  maxCapacityValue:
    draft.mode === 'vector'
      ? convertVectorDraftToString(draft.vectorMaxCapacity).trim()
      : draft.maxCapacityValue.trim(),
});

const createLabelPartitionAccessIssue = (
  queuePath: string,
  field: string,
  label: string,
  propertyLabel: 'capacity' | 'maximum capacity',
): ValidationIssue => ({
  queuePath,
  field,
  severity: 'error',
  rule: LABEL_PARTITION_ACCESS_RULE,
  message: `Add "${label}" to accessible-node-labels before setting label partition ${propertyLabel}.`,
});

/**
 * Validates label-partition capacity drafts for queues that do not list the label
 * in their own accessible-node-labels property.
 */
export function getLabelPartitionAccessIssues(
  rows: CapacityRowDraft[],
  selectedNodeLabel: string | null,
  store: QueuePropertyReader,
): ValidationIssue[] {
  if (!selectedNodeLabel) {
    return [];
  }

  const capacityField = getPropertyNameForLabel(selectedNodeLabel, 'capacity');
  const maxCapacityField = getPropertyNameForLabel(selectedNodeLabel, 'maximum-capacity');
  const issues: ValidationIssue[] = [];

  rows.forEach((row) => {
    if (queueListsAccessibleNodeLabel(row.queuePath, selectedNodeLabel, store)) {
      return;
    }

    const { capacityValue, maxCapacityValue } = getDraftCapacityValues(row);

    if (capacityValue) {
      issues.push(
        createLabelPartitionAccessIssue(
          row.queuePath,
          capacityField,
          selectedNodeLabel,
          'capacity',
        ),
      );
    }

    if (maxCapacityValue) {
      issues.push(
        createLabelPartitionAccessIssue(
          row.queuePath,
          maxCapacityField,
          selectedNodeLabel,
          'maximum capacity',
        ),
      );
    }
  });

  return issues;
};

const mergeCapacityEditorDraftCache = (
  draftCache: Record<string, DraftCacheEntry>,
  currentDrafts: Record<string, CapacityRowDraft>,
  currentDraftOrder: string[],
  selectedNodeLabel: string | null,
): Record<string, DraftCacheEntry> => {
  const currentCacheKey = selectedNodeLabel ?? DEFAULT_PARTITION_VALUE;

  return {
    ...draftCache,
    [currentCacheKey]: {
      drafts: { ...currentDrafts },
      draftOrder: [...currentDraftOrder],
    },
  };
};

/** Validates all cached label-partition drafts in the capacity editor. */
export function getLabelPartitionAccessIssuesForEditor(
  draftCache: Record<string, DraftCacheEntry>,
  currentDrafts: Record<string, CapacityRowDraft>,
  currentDraftOrder: string[],
  selectedNodeLabel: string | null,
  store: QueuePropertyReader,
): ValidationIssue[] {
  const completeDraftCache = mergeCapacityEditorDraftCache(
    draftCache,
    currentDrafts,
    currentDraftOrder,
    selectedNodeLabel,
  );

  const issues: ValidationIssue[] = [];

  Object.entries(completeDraftCache).forEach(([cacheKey, cachedData]) => {
    if (cacheKey === DEFAULT_PARTITION_VALUE) {
      return;
    }

    const rows = cachedData.draftOrder
      .map((queuePath) => cachedData.drafts[queuePath])
      .filter((row): row is CapacityRowDraft => Boolean(row));

    issues.push(...getLabelPartitionAccessIssues(rows, cacheKey, store));
  });

  return issues;
}

export interface ExtractChangesParams {
  draftCache: Record<string, DraftCacheEntry>;
  currentDrafts: Record<string, CapacityRowDraft>;
  currentDraftOrder: string[];
  selectedNodeLabel: string | null;
  getQueuePropertyValue: (queuePath: string, property: string) => { value: string };
}

/**
 * Extract changes from capacity editor drafts across all cached labels.
 * Compares current values to existing store values and returns a map of changes.
 */
export function extractChangesFromDrafts({
  draftCache,
  currentDrafts,
  currentDraftOrder,
  selectedNodeLabel,
  getQueuePropertyValue,
}: ExtractChangesParams): Map<string, Record<string, string>> {
  const normalizeValue = (value: string) => value.trim();
  const changesByQueue = new Map<string, Record<string, string>>();

  const completeDraftCache = mergeCapacityEditorDraftCache(
    draftCache,
    currentDrafts,
    currentDraftOrder,
    selectedNodeLabel,
  );

  Object.entries(completeDraftCache).forEach(([cacheKey, cachedData]) => {
    const label = cacheKey === DEFAULT_PARTITION_VALUE ? null : cacheKey;
    const capacityProperty = getPropertyNameForLabel(label, 'capacity');
    const maxCapacityProperty = getPropertyNameForLabel(label, 'maximum-capacity');

    cachedData.draftOrder.forEach((queuePath) => {
      const draft = cachedData.drafts[queuePath];
      if (!draft) {
        return;
      }

      const capacityString =
        draft.mode === 'vector'
          ? convertVectorDraftToString(draft.vectorCapacity)
          : draft.capacityValue;
      const maxCapacityString =
        draft.mode === 'vector'
          ? convertVectorDraftToString(draft.vectorMaxCapacity)
          : draft.maxCapacityValue;

      const currentCapacity = normalizeValue(capacityString);
      const currentMaxCapacity = normalizeValue(maxCapacityString);

      const existingCapacity = normalizeValue(
        getQueuePropertyValue(queuePath, capacityProperty).value,
      );
      const existingMaxCapacity = normalizeValue(
        getQueuePropertyValue(queuePath, maxCapacityProperty).value,
      );

      const existingChanges = changesByQueue.get(queuePath) ?? {};

      if (currentCapacity !== existingCapacity) {
        existingChanges[capacityProperty] = currentCapacity;
      }

      if (currentMaxCapacity !== existingMaxCapacity) {
        existingChanges[maxCapacityProperty] = currentMaxCapacity;
      }

      if (Object.keys(existingChanges).length > 0) {
        changesByQueue.set(queuePath, existingChanges);
      }
    });
  });

  return changesByQueue;
}

/**
 * Build a preview config map with proposed changes applied.
 */
export function buildPreviewConfig(
  baseConfig: Map<string, string>,
  changesByQueue: Map<string, Record<string, string>>,
): Map<string, string> {
  const previewConfig = new Map(baseConfig);

  changesByQueue.forEach((properties, queuePath) => {
    Object.entries(properties).forEach(([propertyName, value]) => {
      const key = buildPropertyKey(queuePath, propertyName);
      if (value === '') {
        previewConfig.delete(key);
      } else {
        previewConfig.set(key, value);
      }
    });
  });

  return previewConfig;
}

export interface ValidateCapacityChangesParams {
  changesByQueue: Map<string, Record<string, string>>;
  previewConfig: Map<string, string>;
  stagedChanges: StagedChange[];
  schedulerData: SchedulerInfo | null;
  force?: boolean;
}

export interface ValidateCapacityChangesResult {
  issues: ValidationIssue[];
  hasBlockingErrors: boolean;
}

/**
 * Validate capacity changes for all queues.
 * Returns aggregated issues and whether there are blocking errors.
 */
export function validateCapacityChanges({
  changesByQueue,
  previewConfig,
  stagedChanges,
  schedulerData,
  force = false,
}: ValidateCapacityChangesParams): ValidateCapacityChangesResult {
  let aggregatedIssues: ValidationIssue[] = [];
  let hasBlockingErrors = false;

  changesByQueue.forEach((properties, queuePath) => {
    const result = validateQueue({
      queuePath,
      properties,
      configData: previewConfig,
      stagedChanges,
      schedulerData,
    });

    aggregatedIssues = aggregatedIssues.concat(result.issues);

    if (!force && result.issues.some((issue) => issue.severity === 'error')) {
      hasBlockingErrors = true;
    }
  });

  return { issues: aggregatedIssues, hasBlockingErrors };
}
