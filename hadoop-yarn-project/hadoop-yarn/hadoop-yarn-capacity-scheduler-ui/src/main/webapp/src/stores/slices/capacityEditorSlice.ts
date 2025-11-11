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
 * Capacity Editor slice
 *
 * Manages dialog visibility, draft state, and contextual metadata
 * for the Capacity Editor experience.
 */

import type { StateCreator } from 'zustand';
import { produce } from 'immer';
import type { SchedulerStore } from './types';
import {
  buildCapacityEditorDrafts,
  buildCapacityEditorLabelOptions,
  convertVectorDraftToString,
  DEFAULT_PARTITION_VALUE,
  getPropertyNameForLabel,
} from '~/features/queue-management/utils/capacityEditor';
import { buildPropertyKey } from '~/utils/propertyUtils';
import { validateQueue } from '~/features/validation/service';
import type { ValidationIssue } from '~/types';

export type CapacityEditorOrigin = 'property-editor' | 'context-menu' | 'add-queue';

export type CapacityResourceMode = 'simple' | 'vector';

export interface CapacityVectorEntryDraft {
  key: string;
  value: string;
  id: string;
}

export interface CapacityRowDraft {
  queuePath: string;
  queueName: string;
  isOrigin: boolean;
  isNew: boolean;
  hasStagedChange: boolean;
  mode: CapacityResourceMode;
  baseMode: CapacityResourceMode;
  baseCapacityValue: string;
  baseMaxCapacityValue: string;
  capacityValue: string;
  maxCapacityValue: string;
  vectorCapacity: CapacityVectorEntryDraft[];
  vectorMaxCapacity: CapacityVectorEntryDraft[];
}

interface CapacityEditorDialogState {
  isOpen: boolean;
  origin: CapacityEditorOrigin | null;
  parentQueuePath: string | null;
  originQueuePath: string | null;
  originQueueName: string | null;
  originQueueState: string | null;
  originInitialCapacity: string | null;
  originInitialMaxCapacity: string | null;
  originIsNew: boolean;
  selectedNodeLabel: string | null;
  labelOptions: Array<{ value: string; label: string }>;
  labelsWithoutAccess: Set<string>;
  drafts: Record<string, CapacityRowDraft>;
  draftOrder: string[];
  draftCache: Record<string, { drafts: Record<string, CapacityRowDraft>; draftOrder: string[] }>;
  isSaving: boolean;
  saveError: string | null;
  validationIssues: ValidationIssue[];
}

const createEmptyDialogState = (): CapacityEditorDialogState => ({
  isOpen: false,
  origin: null,
  parentQueuePath: null,
  originQueuePath: null,
  originQueueName: null,
  originQueueState: null,
  originInitialCapacity: null,
  originInitialMaxCapacity: null,
  originIsNew: false,
  selectedNodeLabel: null,
  labelOptions: [
    {
      value: DEFAULT_PARTITION_VALUE,
      label: 'Default partition',
    },
  ],
  labelsWithoutAccess: new Set<string>(),
  drafts: {},
  draftOrder: [],
  draftCache: {},
  isSaving: false,
  saveError: null,
  validationIssues: [],
});

const applyDraftsToState = (editorState: CapacityEditorDialogState, drafts: CapacityRowDraft[]) => {
  editorState.drafts = {};
  editorState.draftOrder = [];

  drafts.forEach((draft) => {
    editorState.drafts[draft.queuePath] = draft;
    editorState.draftOrder.push(draft.queuePath);
  });
};

const normalizeLabelOptions = (
  options: Array<{ value: string; label: string }>,
  selectedLabel: string | null,
) => {
  const dedup = new Map<string, string>();
  options.forEach((option) => {
    dedup.set(option.value, option.label);
  });

  if (selectedLabel && !dedup.has(selectedLabel)) {
    dedup.set(selectedLabel, selectedLabel);
  }

  const sorted = Array.from(dedup.entries())
    .filter(([value]) => value !== DEFAULT_PARTITION_VALUE)
    .map(([value, label]) => ({ value, label }))
    .sort((a, b) => a.value.localeCompare(b.value));

  return [
    {
      value: DEFAULT_PARTITION_VALUE,
      label: 'Default partition',
    },
    ...sorted,
  ];
};

export interface CapacityEditorSlice {
  capacityEditor: CapacityEditorDialogState;
  openCapacityEditor: (options: {
    origin: CapacityEditorOrigin;
    parentQueuePath: string;
    originQueuePath: string;
    originQueueName: string;
    originQueueState?: string | null;
    originInitialCapacity?: string | null;
    originInitialMaxCapacity?: string | null;
    originIsNew?: boolean;
    selectedNodeLabel?: string | null;
  }) => void;
  closeCapacityEditor: () => void;
  setCapacityEditorLabel: (label: string | null) => void;
  updateCapacityDraft: (queuePath: string, updater: (draft: CapacityRowDraft) => void) => void;
  resetCapacityDrafts: () => void;
  saveCapacityDrafts: (options?: { force?: boolean }) => Promise<boolean>;
}

export const createCapacityEditorSlice: StateCreator<
  SchedulerStore,
  [['zustand/immer', never]],
  [],
  CapacityEditorSlice
> = (set, get) => ({
  capacityEditor: createEmptyDialogState(),

  openCapacityEditor: ({
    origin,
    parentQueuePath,
    originQueuePath,
    originQueueName,
    originQueueState = null,
    originInitialCapacity = null,
    originInitialMaxCapacity = null,
    originIsNew = false,
    selectedNodeLabel = null,
  }) => {
    const store = get();
    const drafts = buildCapacityEditorDrafts({
      store,
      parentQueuePath,
      originQueuePath,
      originQueueName,
      originInitialCapacity,
      originInitialMaxCapacity,
      originIsNew,
      selectedNodeLabel,
    });

    const { options, labelsWithoutAccess } = buildCapacityEditorLabelOptions(
      store,
      originQueuePath,
      selectedNodeLabel,
    );
    const labelOptions = normalizeLabelOptions(options, selectedNodeLabel);

    set((state) => {
      const editorState = state.capacityEditor;
      editorState.isOpen = true;
      editorState.origin = origin;
      editorState.parentQueuePath = parentQueuePath;
      editorState.originQueuePath = originQueuePath;
      editorState.originQueueName = originQueueName;
      editorState.originQueueState = originQueueState;
      editorState.originInitialCapacity = originInitialCapacity ?? null;
      editorState.originInitialMaxCapacity = originInitialMaxCapacity ?? null;
      editorState.originIsNew = originIsNew;
      editorState.selectedNodeLabel = selectedNodeLabel;
      editorState.labelOptions = labelOptions;
      editorState.labelsWithoutAccess = labelsWithoutAccess;
      applyDraftsToState(editorState, drafts);
    });
  },

  closeCapacityEditor: () =>
    set((state) => {
      state.capacityEditor = createEmptyDialogState();
    }),

  setCapacityEditorLabel: (label) => {
    const store = get();
    const {
      parentQueuePath,
      originQueuePath,
      originQueueName,
      originInitialCapacity,
      originInitialMaxCapacity,
      originIsNew,
      selectedNodeLabel: currentLabel,
      drafts: currentDrafts,
      draftOrder: currentDraftOrder,
      draftCache,
    } = store.capacityEditor;

    if (!parentQueuePath || !originQueuePath || !originQueueName) {
      set((state) => {
        state.capacityEditor.selectedNodeLabel = label;
      });
      return;
    }

    // Save current drafts to cache before switching labels
    const cacheKey = currentLabel ?? DEFAULT_PARTITION_VALUE;
    const updatedCache = {
      ...draftCache,
      [cacheKey]: {
        drafts: { ...currentDrafts },
        draftOrder: [...currentDraftOrder],
      },
    };

    // Check if we have cached drafts for the new label
    const newCacheKey = label ?? DEFAULT_PARTITION_VALUE;
    const cachedData = updatedCache[newCacheKey];

    let drafts: CapacityRowDraft[];
    if (cachedData) {
      // Restore from cache
      drafts = cachedData.draftOrder.map((queuePath) => cachedData.drafts[queuePath]);
    } else {
      // Build new drafts from scratch
      drafts = buildCapacityEditorDrafts({
        store,
        parentQueuePath,
        originQueuePath,
        originQueueName,
        originInitialCapacity,
        originInitialMaxCapacity,
        originIsNew,
        selectedNodeLabel: label,
      });
    }

    const { options, labelsWithoutAccess } = buildCapacityEditorLabelOptions(
      store,
      originQueuePath,
      label,
    );
    const labelOptions = normalizeLabelOptions(options, label);

    set((state) => {
      const editorState = state.capacityEditor;
      editorState.selectedNodeLabel = label;
      editorState.labelOptions = labelOptions;
      editorState.labelsWithoutAccess = labelsWithoutAccess;
      editorState.draftCache = updatedCache;
      applyDraftsToState(editorState, drafts);
    });
  },

  updateCapacityDraft: (queuePath, updater) =>
    set((state) => {
      const existing = state.capacityEditor.drafts[queuePath];
      if (!existing) {
        return;
      }

      state.capacityEditor.drafts[queuePath] = produce(existing, (draft) => {
        updater(draft);
      });
    }),

  resetCapacityDrafts: () => {
    const store = get();
    const {
      parentQueuePath,
      originQueuePath,
      originQueueName,
      originInitialCapacity,
      originInitialMaxCapacity,
      originIsNew,
      selectedNodeLabel,
    } = store.capacityEditor;

    if (!parentQueuePath || !originQueuePath || !originQueueName) {
      return;
    }

    const drafts = buildCapacityEditorDrafts({
      store,
      parentQueuePath,
      originQueuePath,
      originQueueName,
      originInitialCapacity,
      originInitialMaxCapacity,
      originIsNew,
      selectedNodeLabel,
    });

    set((state) => {
      applyDraftsToState(state.capacityEditor, drafts);
    });
  },

  saveCapacityDrafts: async ({ force = false } = {}) => {
    const storeSnapshot = get();
    const {
      parentQueuePath,
      originQueuePath,
      originQueueName,
      originInitialCapacity: _originInitialCapacity,
      originInitialMaxCapacity: _originInitialMaxCapacity,
      originIsNew,
      selectedNodeLabel,
      drafts,
      draftOrder,
      draftCache,
    } = storeSnapshot.capacityEditor;

    if (!parentQueuePath || !originQueuePath || !originQueueName) {
      return false;
    }

    set((state) => {
      state.capacityEditor.isSaving = true;
      state.capacityEditor.saveError = null;
      if (!force) {
        state.capacityEditor.validationIssues = [];
      }
    });

    const normalizeValue = (value: string) => value.trim();

    const changesByQueue = new Map<string, Record<string, string>>();

    // Build a complete cache including the current drafts
    const currentCacheKey = selectedNodeLabel ?? DEFAULT_PARTITION_VALUE;
    const completeDraftCache = {
      ...draftCache,
      [currentCacheKey]: {
        drafts: { ...drafts },
        draftOrder: [...draftOrder],
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
          storeSnapshot.getQueuePropertyValue(queuePath, capacityProperty).value,
        );
        const existingMaxCapacity = normalizeValue(
          storeSnapshot.getQueuePropertyValue(queuePath, maxCapacityProperty).value,
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

    if (changesByQueue.size === 0) {
      set((state) => {
        state.capacityEditor.isSaving = false;
        state.capacityEditor.validationIssues = [];
      });
      return true;
    }

    const previewConfig = new Map(storeSnapshot.configData);
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

    let aggregatedIssues: ValidationIssue[] = [];
    let hasBlockingErrors = false;

    changesByQueue.forEach((properties, queuePath) => {
      const result = validateQueue({
        queuePath,
        properties,
        configData: previewConfig,
        stagedChanges: storeSnapshot.stagedChanges,
        schedulerData: storeSnapshot.schedulerData,
      });

      aggregatedIssues = aggregatedIssues.concat(result.issues);

      if (!force && result.issues.some((issue) => issue.severity === 'error')) {
        hasBlockingErrors = true;
      }
    });

    if (!force && hasBlockingErrors) {
      set((state) => {
        state.capacityEditor.isSaving = false;
        state.capacityEditor.saveError = 'Capacity validation failed.';
        state.capacityEditor.validationIssues = aggregatedIssues;
      });
      return false;
    }

    changesByQueue.forEach((properties, queuePath) => {
      Object.entries(properties).forEach(([propertyName, value]) => {
        const propertyIssues = aggregatedIssues.filter(
          (issue) => issue.queuePath === queuePath && issue.field === propertyName,
        );

        const labelMatch = propertyName.match(/^accessible-node-labels\.([^.]+)\.([^.]+)$/);

        if (labelMatch) {
          const [, labelName, baseProperty] = labelMatch;
          storeSnapshot.stageLabelQueueChange(
            queuePath,
            labelName,
            baseProperty as 'capacity' | 'maximum-capacity',
            value,
            propertyIssues.length > 0 ? propertyIssues : undefined,
          );
          return;
        }

        storeSnapshot.stageQueueChange(
          queuePath,
          propertyName,
          value,
          propertyIssues.length > 0 ? propertyIssues : undefined,
        );
      });
    });

    const refreshedStore = get();
    const refreshedDrafts = buildCapacityEditorDrafts({
      store: refreshedStore,
      parentQueuePath,
      originQueuePath,
      originQueueName,
      originInitialCapacity: null,
      originInitialMaxCapacity: null,
      originIsNew,
      selectedNodeLabel,
    });

    const refreshedOrigin = refreshedDrafts.find((draft) => draft.isOrigin);

    set((state) => {
      const editorState = state.capacityEditor;
      editorState.isSaving = false;
      editorState.saveError = null;
      editorState.validationIssues = aggregatedIssues;
      editorState.draftCache = {}; // Clear cache after successful save
      if (refreshedOrigin) {
        editorState.originInitialCapacity = refreshedOrigin.capacityValue;
        editorState.originInitialMaxCapacity = refreshedOrigin.maxCapacityValue;
      }
      applyDraftsToState(editorState, refreshedDrafts);
    });

    return true;
  },
});
