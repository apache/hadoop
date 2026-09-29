/**
 * Capacity validation utilities
 *
 * These functions extract changes from capacity editor drafts
 * and validate capacity configurations.
 */

import type { CapacityRowDraft } from '~/stores/slices/capacityEditorSlice';
import {
  convertVectorDraftToString,
  DEFAULT_PARTITION_VALUE,
  getPropertyNameForLabel,
} from './capacityEditor';
import { buildPropertyKey } from '~/utils/propertyUtils';
import { validateQueue } from '~/features/validation/service';
import type { ValidationIssue, StagedChange, SchedulerInfo } from '~/types';

export interface DraftCacheEntry {
  drafts: Record<string, CapacityRowDraft>;
  draftOrder: string[];
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

  // Build a complete cache including the current drafts
  const currentCacheKey = selectedNodeLabel ?? DEFAULT_PARTITION_VALUE;
  const completeDraftCache: Record<string, DraftCacheEntry> = {
    ...draftCache,
    [currentCacheKey]: {
      drafts: { ...currentDrafts },
      draftOrder: [...currentDraftOrder],
    },
  };

  // Process all cached labels (including the currently selected one)
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
