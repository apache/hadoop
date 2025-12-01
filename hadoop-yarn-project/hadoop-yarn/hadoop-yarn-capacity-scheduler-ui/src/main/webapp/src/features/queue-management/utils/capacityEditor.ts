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


import { nanoid } from 'nanoid';
import { buildPropertyKey, getParentQueuePath, getQueueNameFromPath } from '~/utils/propertyUtils';
import { isVectorCapacity } from '~/utils/capacityUtils';
import { isTemplateQueuePath } from '~/utils/templateUtils';
import type { SchedulerStore } from '~/stores/schedulerStore';
import type {
  CapacityResourceMode,
  CapacityRowDraft,
  CapacityVectorEntryDraft,
} from '~/stores/slices/capacityEditorSlice';

const DEFAULT_VECTOR_KEYS = ['memory', 'vcores'];

export const DEFAULT_PARTITION_VALUE = '__DEFAULT_PARTITION__';

const sanitize = (value?: string | null) => (value ?? '').trim();

export const parseVectorDraft = (value: string): CapacityVectorEntryDraft[] => {
  // More permissive parsing for drafts - allows empty values for editing
  const trimmed = value.trim();
  if (!isVectorCapacity(trimmed)) {
    return [];
  }

  const withoutBrackets = trimmed.slice(1, -1).trim();
  if (!withoutBrackets) {
    return [];
  }

  return withoutBrackets
    .split(',')
    .map((pair) => {
      const [rawKey, rawValue] = pair.split('=');
      const key = rawKey?.trim() ?? '';
      const entryValue = rawValue?.trim() ?? '';

      if (!key) {
        return null;
      }

      return {
        id: nanoid(),
        key,
        value: entryValue,
      };
    })
    .filter((entry): entry is CapacityVectorEntryDraft => entry !== null);
};

export const ensureCoreEntries = (
  entries: CapacityVectorEntryDraft[],
  includeDefaults = true,
): CapacityVectorEntryDraft[] => {
  if (!includeDefaults) {
    return entries;
  }

  const existingKeys = new Set(entries.map((entry) => entry.key));
  const withDefaults = [...entries];

  DEFAULT_VECTOR_KEYS.forEach((key) => {
    if (!existingKeys.has(key)) {
      withDefaults.push({
        id: nanoid(),
        key,
        value: '',
      });
    }
  });

  return withDefaults;
};

const inferModeFromValues = (capacity: string, maxCapacity: string): CapacityResourceMode => {
  if (isVectorCapacity(capacity) || isVectorCapacity(maxCapacity)) {
    return 'vector';
  }
  return 'simple';
};

export interface CreateRowDraftOptions {
  queuePath: string;
  queueName: string;
  baseCapacity: string | undefined;
  baseMaxCapacity: string | undefined;
  currentCapacity?: string | null;
  currentMaxCapacity?: string | null;
  isOrigin?: boolean;
  isNew?: boolean;
  hasStagedChange?: boolean;
}

export const createRowDraft = ({
  queuePath,
  queueName,
  baseCapacity,
  baseMaxCapacity,
  currentCapacity,
  currentMaxCapacity,
  isOrigin = false,
  isNew = false,
  hasStagedChange = false,
}: CreateRowDraftOptions): CapacityRowDraft => {
  const baseCapacityValue = sanitize(baseCapacity);
  const baseMaxCapacityValue = sanitize(baseMaxCapacity);

  const capacityValue = sanitize(
    currentCapacity !== null && currentCapacity !== undefined ? currentCapacity : baseCapacityValue,
  );
  const maxCapacityValue = sanitize(
    currentMaxCapacity !== null && currentMaxCapacity !== undefined
      ? currentMaxCapacity
      : baseMaxCapacityValue,
  );

  const mode = inferModeFromValues(capacityValue, maxCapacityValue);
  const baseMode = inferModeFromValues(baseCapacityValue, baseMaxCapacityValue);

  const vectorCapacity =
    mode === 'vector' ? ensureCoreEntries(parseVectorDraft(capacityValue)) : [];
  const vectorMaxCapacity =
    mode === 'vector' ? ensureCoreEntries(parseVectorDraft(maxCapacityValue)) : [];

  return {
    queuePath,
    queueName,
    isOrigin,
    isNew,
    hasStagedChange,
    mode,
    baseMode,
    baseCapacityValue,
    baseMaxCapacityValue,
    capacityValue,
    maxCapacityValue,
    vectorCapacity,
    vectorMaxCapacity,
  };
};

export const convertVectorDraftToString = (entries: CapacityVectorEntryDraft[]): string => {
  if (entries.length === 0) {
    return '';
  }

  const parts = entries
    .filter((entry) => entry.key.trim().length > 0)
    .map((entry) => `${entry.key.trim()}=${entry.value.trim()}`);

  if (parts.length === 0) {
    return '';
  }

  return `[${parts.join(',')}]`;
};

export const createEmptyVectorEntry = (key = '', value = ''): CapacityVectorEntryDraft => ({
  id: nanoid(),
  key,
  value,
});

const getBaseValue = (store: SchedulerStore, queuePath: string, property: string): string => {
  const key = buildPropertyKey(queuePath, property);
  return sanitize(store.configData.get(key) ?? '');
};

export const getPropertyNameForLabel = (
  label: string | null,
  property: 'capacity' | 'maximum-capacity',
) => {
  if (!label) {
    return property;
  }
  return `accessible-node-labels.${label}.${property}`;
};

export interface BuildCapacityEditorDraftsParams {
  store: SchedulerStore;
  parentQueuePath: string;
  originQueuePath: string;
  originQueueName: string;
  originInitialCapacity?: string | null;
  originInitialMaxCapacity?: string | null;
  originIsNew?: boolean;
  selectedNodeLabel?: string | null;
}

export const buildCapacityEditorDrafts = ({
  store,
  parentQueuePath,
  originQueuePath,
  originQueueName,
  originInitialCapacity = null,
  originInitialMaxCapacity = null,
  originIsNew = false,
  selectedNodeLabel = null,
}: BuildCapacityEditorDraftsParams): CapacityRowDraft[] => {
  if (!parentQueuePath) {
    return [];
  }

  const capacityProperty = getPropertyNameForLabel(selectedNodeLabel, 'capacity');
  const maxCapacityProperty = getPropertyNameForLabel(selectedNodeLabel, 'maximum-capacity');

  // Special handling for template paths: only create a draft for the template itself
  if (isTemplateQueuePath(originQueuePath)) {
    const baseCapacity = getBaseValue(store, originQueuePath, capacityProperty);
    const baseMaxCapacity = getBaseValue(store, originQueuePath, maxCapacityProperty);

    const capacityResult = store.getQueuePropertyValue(originQueuePath, capacityProperty);
    const maxCapacityResult = store.getQueuePropertyValue(originQueuePath, maxCapacityProperty);

    const currentCapacity =
      originInitialCapacity !== null ? originInitialCapacity : capacityResult.value;
    const currentMaxCapacity =
      originInitialMaxCapacity !== null ? originInitialMaxCapacity : maxCapacityResult.value;

    const hasStagedChange = originIsNew || capacityResult.isStaged || maxCapacityResult.isStaged;

    return [
      createRowDraft({
        queuePath: originQueuePath,
        queueName: originQueueName,
        baseCapacity,
        baseMaxCapacity,
        currentCapacity,
        currentMaxCapacity,
        isOrigin: true,
        isNew: originIsNew,
        hasStagedChange,
      }),
    ];
  }

  const drafts: CapacityRowDraft[] = [];
  const seen = new Set<string>();

  const childQueues = store.getChildQueues(parentQueuePath) ?? [];

  childQueues.forEach((queue) => {
    const queuePath = queue.queuePath;
    seen.add(queuePath);

    const baseCapacity = getBaseValue(store, queuePath, capacityProperty);
    const baseMaxCapacity = getBaseValue(store, queuePath, maxCapacityProperty);

    const capacityResult = store.getQueuePropertyValue(queuePath, capacityProperty);
    const maxCapacityResult = store.getQueuePropertyValue(queuePath, maxCapacityProperty);

    const isOrigin = queuePath === originQueuePath;
    const currentCapacity =
      isOrigin && originInitialCapacity !== null ? originInitialCapacity : capacityResult.value;
    const currentMaxCapacity =
      isOrigin && originInitialMaxCapacity !== null
        ? originInitialMaxCapacity
        : maxCapacityResult.value;

    const hasStagedChange = capacityResult.isStaged || maxCapacityResult.isStaged;

    drafts.push(
      createRowDraft({
        queuePath,
        queueName: queue.queueName,
        baseCapacity,
        baseMaxCapacity,
        currentCapacity,
        currentMaxCapacity,
        isOrigin,
        isNew: false,
        hasStagedChange,
      }),
    );
  });

  const stagedAdditions = new Map<
    string,
    {
      capacity?: string;
      maxCapacity?: string;
    }
  >();

  store.stagedChanges.forEach((change) => {
    if (change.type !== 'add' || !change.queuePath) {
      return;
    }

    const changeParent = getParentQueuePath(change.queuePath);
    if (changeParent !== parentQueuePath) {
      return;
    }

    const entry = stagedAdditions.get(change.queuePath) ?? {};

    if (change.property === capacityProperty && change.newValue !== undefined) {
      entry.capacity = change.newValue;
    } else if (change.property === maxCapacityProperty && change.newValue !== undefined) {
      entry.maxCapacity = change.newValue;
    }

    stagedAdditions.set(change.queuePath, entry);
  });

  stagedAdditions.forEach((values, queuePath) => {
    if (seen.has(queuePath)) {
      return;
    }

    const queueName = getQueueNameFromPath(queuePath) || queuePath;
    const isOrigin = queuePath === originQueuePath;

    const currentCapacity =
      (isOrigin && originInitialCapacity !== null ? originInitialCapacity : values.capacity) ?? '';
    const currentMaxCapacity =
      (isOrigin && originInitialMaxCapacity !== null
        ? originInitialMaxCapacity
        : values.maxCapacity) ?? '';

    drafts.push(
      createRowDraft({
        queuePath,
        queueName,
        baseCapacity: '',
        baseMaxCapacity: '',
        currentCapacity,
        currentMaxCapacity,
        isOrigin,
        isNew: true,
        hasStagedChange: true,
      }),
    );

    seen.add(queuePath);
  });

  if (!seen.has(originQueuePath)) {
    drafts.unshift(
      createRowDraft({
        queuePath: originQueuePath,
        queueName: originQueueName || getQueueNameFromPath(originQueuePath) || originQueuePath,
        baseCapacity: originIsNew ? '' : (originInitialCapacity ?? ''),
        baseMaxCapacity: originIsNew ? '' : (originInitialMaxCapacity ?? ''),
        currentCapacity: originInitialCapacity ?? '',
        currentMaxCapacity: originInitialMaxCapacity ?? '',
        isOrigin: true,
        isNew: originIsNew,
        hasStagedChange: originIsNew,
      }),
    );
  }

  const originRow = drafts.find((draft) => draft.isOrigin);
  const otherRows = drafts.filter((draft) => !draft.isOrigin);

  return originRow ? [originRow, ...otherRows] : otherRows;
};

export interface LabelOption {
  value: string;
  label: string;
}

export interface CapacityEditorLabelOptions {
  options: LabelOption[];
  labelsWithoutAccess: Set<string>;
}

/**
 * Gets all labels that have configured capacities for a specific queue.
 * Scans configData for any accessible-node-labels.<label>.capacity properties.
 */
const getConfiguredLabelsForQueue = (store: SchedulerStore, queuePath: string): Set<string> => {
  const configured = new Set<string>();
  const prefix = buildPropertyKey(queuePath, 'accessible-node-labels.');

  store.configData.forEach((_, key) => {
    if (key.startsWith(prefix)) {
      const remainder = key.substring(prefix.length);
      const match = remainder.match(/^([^.]+)\.(capacity|maximum-capacity)$/);
      if (match && match[1]) {
        configured.add(match[1]);
      }
    }
  });

  return configured;
};

/**
 * Gets the accessible labels for a queue based on its nodeLabels field.
 * Falls back to parent queue if current queue's nodeLabels is undefined/empty.
 */
const getAccessibleLabelsForQueue = (
  store: SchedulerStore,
  queuePath: string,
): { labels: Set<string>; hasWildcard: boolean } => {
  const queue = store.getQueueByPath(queuePath);

  // Check current queue's nodeLabels
  if (queue?.nodeLabels && queue.nodeLabels.length > 0) {
    const hasWildcard = queue.nodeLabels.includes('*');
    const labels = new Set<string>();

    if (hasWildcard) {
      // Add all system-wide labels
      store.nodeLabels.forEach((label) => {
        if (label.name) {
          labels.add(label.name);
        }
      });
    } else {
      // Add specific labels from nodeLabels array
      queue.nodeLabels.forEach((label) => {
        if (label && label !== '*') {
          labels.add(label);
        }
      });
    }

    return { labels, hasWildcard };
  }

  // Fallback to parent if nodeLabels is undefined or empty
  const parentPath = getParentQueuePath(queuePath);
  if (parentPath && parentPath !== queuePath) {
    return getAccessibleLabelsForQueue(store, parentPath);
  }

  // No accessible labels found
  return { labels: new Set<string>(), hasWildcard: false };
};

export const buildCapacityEditorLabelOptions = (
  store: SchedulerStore,
  queuePath: string | null,
  currentlySelectedLabel?: string | null,
): CapacityEditorLabelOptions => {
  const options: LabelOption[] = [
    {
      value: DEFAULT_PARTITION_VALUE,
      label: 'Default partition',
    },
  ];

  const labelsWithoutAccess = new Set<string>();

  if (!queuePath) {
    return { options, labelsWithoutAccess };
  }

  // Get accessible labels based on nodeLabels field
  const { labels: accessibleLabels } = getAccessibleLabelsForQueue(store, queuePath);

  // Get configured labels (those with existing capacity config)
  const configuredLabels = getConfiguredLabelsForQueue(store, queuePath);

  // Combine all labels: accessible + configured
  const allLabels = new Set([...accessibleLabels, ...configuredLabels]);

  // Add currently selected label if not already included
  if (currentlySelectedLabel && currentlySelectedLabel !== DEFAULT_PARTITION_VALUE) {
    allLabels.add(currentlySelectedLabel);
  }

  // Track which labels are configured but not accessible
  configuredLabels.forEach((label) => {
    if (!accessibleLabels.has(label)) {
      labelsWithoutAccess.add(label);
    }
  });

  // Track if currently selected label doesn't have access
  if (
    currentlySelectedLabel &&
    currentlySelectedLabel !== DEFAULT_PARTITION_VALUE &&
    !accessibleLabels.has(currentlySelectedLabel)
  ) {
    labelsWithoutAccess.add(currentlySelectedLabel);
  }

  // Convert to options and sort
  Array.from(allLabels)
    .sort((a, b) => a.localeCompare(b))
    .forEach((label) => {
      options.push({
        value: label,
        label,
      });
    });

  return { options, labelsWithoutAccess };
};
