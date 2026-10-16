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
 * Staged changes slice - handles all change management operations
 */

import type { StateCreator } from 'zustand';
import { nanoid } from 'nanoid';
import { AUTO_CREATION_PROPS, MUTATION_OPERATIONS, SPECIAL_VALUES } from '~/types';
import type { SchedConfUpdateInfo, StagedChange, ValidationIssue } from '~/types';
import {
  buildGlobalPropertyKey,
  buildNodeLabelPropertyKey,
  buildPropertyKey,
} from '~/utils/propertyUtils';
import { buildMutationRequest } from '~/features/staged-changes/utils/mutationBuilder';
import {
  getParentQueuesForAdditions,
  getQueuesForRemoval,
  getQueuesForAutoCreationEnable,
  prepareMutationRequestForSubmission,
  prepareMutationRequestWithVersion,
  applyQueueStates,
  addQueueHierarchyToSet,
  restartQueues,
} from '~/features/staged-changes/utils/queueStateManager';
import { isValidQueueName } from '~/types';
import { createStoreError, ERROR_CODES, extractErrorMessage, isNetworkError } from '~/lib/errors';
import { assertWritable } from '~/lib/errors/readOnlyGuard';
import type { StagedChangesSlice, SchedulerStore } from './types';
import { getAffectedQueuesForValidation } from '~/features/validation/utils/affectedQueues';
import { validateStagedChanges, validatePropertyChange } from '~/features/validation/crossQueue';

type MutationErrorState = Pick<SchedulerStore, 'applyError' | 'error' | 'errorContext'>;
const clearMutationError = (state: MutationErrorState) => {
  if (state.applyError) {
    state.applyError = null;
  }
  if (state.errorContext === 'mutation') {
    state.error = null;
    state.errorContext = null;
  }
};

export const createStagedChangesSlice: StateCreator<
  SchedulerStore,
  [['zustand/immer', never]],
  [],
  StagedChangesSlice
> = (set, get) => ({
  stagedChanges: [],
  applyError: null,

  stageQueueChange: (queuePath, property, value, validationErrors) => {
    if (!queuePath || !queuePath.startsWith(SPECIAL_VALUES.ROOT_QUEUE_NAME)) {
      throw createStoreError(
        ERROR_CODES.INVALID_QUEUE_PATH,
        `Invalid queue path: ${queuePath}. Queue paths must start with '${SPECIAL_VALUES.ROOT_QUEUE_NAME}'`,
      );
    }

    if (!property || property.trim() === '') {
      throw createStoreError(ERROR_CODES.INVALID_PROPERTY_NAME, 'Property name cannot be empty');
    }

    let mutated = false;
    set((state) => {
      const propertyKey = buildPropertyKey(queuePath, property);
      const originalValue = state.configData.get(propertyKey);

      const existingIndex = state.stagedChanges.findIndex(
        (c) => c.queuePath === queuePath && c.property === property,
      );

      // If the new value matches the original value, remove the staged change
      if (value === originalValue && existingIndex >= 0) {
        state.stagedChanges.splice(existingIndex, 1);
        mutated = true;
      } else if (existingIndex >= 0) {
        // Update existing staged change
        state.stagedChanges[existingIndex].newValue = value;
        state.stagedChanges[existingIndex].validationErrors = validationErrors;
        mutated = true;
      } else if (value !== originalValue) {
        // Only create a new staged change if the value differs from the original
        const change: StagedChange = {
          id: nanoid(),
          type: 'update',
          queuePath,
          property,
          oldValue: originalValue,
          newValue: value,
          timestamp: Date.now(),
          validationErrors,
        };
        state.stagedChanges.push(change);
        mutated = true;
      }

      if (mutated) {
        clearMutationError(state);
      }
    });

    // Refresh validation errors for affected changes
    get().refreshAffectedValidationErrors(queuePath, property);
  },

  stageGlobalChange: (property, value, validationErrors) => {
    let mutated = false;
    set((state) => {
      // For JSON properties like placement rules, stringify the value if it's an object
      let stringValue: string;
      if (property === SPECIAL_VALUES.MAPPING_RULE_JSON_PROPERTY && typeof value === 'object') {
        stringValue = JSON.stringify(value);
      } else {
        stringValue = String(value);
      }

      const propertyKey = buildGlobalPropertyKey(property);
      const originalValue = state.configData.get(propertyKey);

      const existingIndex = state.stagedChanges.findIndex(
        (c) => c.queuePath === SPECIAL_VALUES.GLOBAL_QUEUE_PATH && c.property === property,
      );

      // If the new value matches the original value, remove the staged change
      if (stringValue === originalValue && existingIndex >= 0) {
        state.stagedChanges.splice(existingIndex, 1);
        mutated = true;
      } else if (existingIndex >= 0) {
        // Update existing staged change
        state.stagedChanges[existingIndex].newValue = stringValue;
        state.stagedChanges[existingIndex].validationErrors = validationErrors;
        mutated = true;
      } else if (stringValue !== originalValue) {
        // Only create a new staged change if the value differs from the original
        const change: StagedChange = {
          id: nanoid(),
          type: 'update',
          queuePath: SPECIAL_VALUES.GLOBAL_QUEUE_PATH,
          property,
          oldValue: originalValue,
          newValue: stringValue,
          timestamp: Date.now(),
          validationErrors,
        };
        state.stagedChanges.push(change);
        mutated = true;
      }

      if (mutated) {
        clearMutationError(state);
      }
    });

    // Refresh validation errors for affected changes
    get().refreshAffectedValidationErrors(SPECIAL_VALUES.GLOBAL_QUEUE_PATH, property);
  },

  stageQueueAddition: (parentPath, queueName, config, validationErrors) => {
    if (!isValidQueueName(queueName)) {
      throw createStoreError(
        ERROR_CODES.INVALID_QUEUE_NAME,
        `Invalid queue name: "${queueName}". Queue names must contain only letters, numbers, hyphens, and underscores.`,
      );
    }

    const newQueuePath =
      parentPath === SPECIAL_VALUES.ROOT_QUEUE_NAME
        ? `${SPECIAL_VALUES.ROOT_QUEUE_NAME}.${queueName}`
        : `${parentPath}.${queueName}`;

    const { schedulerData, configData, stagedChanges: existingStagedChanges } = get();
    const capacityValue = config.capacity;

    const previewStagedChanges =
      typeof capacityValue === 'string'
        ? [
            ...existingStagedChanges,
            {
              id: 'preview-add-capacity',
              type: 'add' as const,
              queuePath: newQueuePath,
              property: 'capacity',
              oldValue: undefined,
              newValue: capacityValue,
              timestamp: Date.now(),
            },
          ]
        : existingStagedChanges;

    const computedCapacityErrors =
      typeof capacityValue === 'string'
        ? validatePropertyChange({
            propertyName: 'capacity',
            propertyValue: capacityValue,
            queuePath: newQueuePath,
            schedulerData,
            configData,
            stagedChanges: previewStagedChanges,
            includeBlockingErrors: true,
          })
        : [];

    set((state) => {
      let mutated = false;
      // Check if queue already exists
      const queue = get().getQueueByPath(newQueuePath);
      if (queue) {
        throw createStoreError(
          ERROR_CODES.QUEUE_ALREADY_EXISTS,
          `Queue "${newQueuePath}" already exists`,
        );
      }

      // Remove any existing changes for the same queue
      const beforeLength = state.stagedChanges.length;
      state.stagedChanges = state.stagedChanges.filter((c) => c.queuePath !== newQueuePath);
      if (state.stagedChanges.length !== beforeLength) {
        mutated = true;
      }

      // Create one staged change per property
      Object.entries(config).forEach(([property, value]) => {
        const change: StagedChange = {
          id: nanoid(),
          type: 'add',
          queuePath: newQueuePath,
          property,
          oldValue: undefined,
          newValue: value,
          timestamp: Date.now(),
          // Only attach validation errors to the first property (capacity) to avoid duplication
          validationErrors:
            property === 'capacity'
              ? validationErrors ||
                (computedCapacityErrors.length > 0 ? computedCapacityErrors : undefined)
              : undefined,
        };
        state.stagedChanges.push(change);
        mutated = true;
      });

      if (mutated) {
        clearMutationError(state);
      }
    });

    if (typeof capacityValue === 'string') {
      get().refreshAffectedValidationErrors(newQueuePath, 'capacity');
    }
  },

  stageQueueRemoval: (queuePath, validationErrors) => {
    set((state) => {
      let mutated = false;

      const beforeLength = state.stagedChanges.length;
      state.stagedChanges = state.stagedChanges.filter((c) => c.queuePath !== queuePath);
      if (state.stagedChanges.length !== beforeLength) {
        mutated = true;
      }

      const change: StagedChange = {
        id: nanoid(),
        type: 'remove',
        queuePath,
        property: SPECIAL_VALUES.QUEUE_MARKER,
        oldValue: 'exists',
        newValue: undefined,
        timestamp: Date.now(),
        validationErrors,
      };

      state.stagedChanges.push(change);
      mutated = true;

      if (mutated) {
        clearMutationError(state);
      }
    });
  },

  stageLabelQueueChange: (queuePath, label, property, value, validationErrors) => {
    if (!queuePath || !queuePath.startsWith(SPECIAL_VALUES.ROOT_QUEUE_NAME)) {
      throw createStoreError(ERROR_CODES.INVALID_QUEUE_PATH, `Invalid queue path: ${queuePath}`);
    }

    if (!label || label.trim() === '') {
      throw createStoreError(ERROR_CODES.INVALID_PROPERTY_NAME, 'Label name cannot be empty');
    }

    if (!property || property.trim() === '') {
      throw createStoreError(ERROR_CODES.INVALID_PROPERTY_NAME, 'Property name cannot be empty');
    }

    const fullPropertyName = `accessible-node-labels.${label}.${property}`;

    let mutated = false;
    set((state) => {
      const propertyKey = buildNodeLabelPropertyKey(queuePath, label, property);
      const originalValue = state.configData.get(propertyKey);

      const existingIndex = state.stagedChanges.findIndex(
        (c) => c.queuePath === queuePath && c.property === fullPropertyName,
      );

      // If the new value matches the original value, remove the staged change
      if (value === originalValue && existingIndex >= 0) {
        state.stagedChanges.splice(existingIndex, 1);
        mutated = true;
      } else if (existingIndex >= 0) {
        // Update existing staged change
        state.stagedChanges[existingIndex].newValue = value;
        state.stagedChanges[existingIndex].label = label;
        state.stagedChanges[existingIndex].validationErrors = validationErrors;
        mutated = true;
      } else if (value !== originalValue) {
        // Only create a new staged change if the value differs from the original
        const change: StagedChange = {
          id: nanoid(),
          type: 'update',
          queuePath,
          property: fullPropertyName,
          oldValue: originalValue,
          newValue: value,
          timestamp: Date.now(),
          label,
          validationErrors,
        };
        state.stagedChanges.push(change);
        mutated = true;
      }

      if (mutated) {
        clearMutationError(state);
      }
    });

    // Refresh validation errors for affected changes
    get().refreshAffectedValidationErrors(queuePath, fullPropertyName);
  },

  applyChanges: async () => {
    const changes = get().stagedChanges;
    if (changes.length === 0) return;

    // Block applying changes in read-only mode
    assertWritable(get().isReadOnly, 'apply changes');

    set((state) => {
      state.isLoading = true;
      if (state.errorContext === 'mutation') {
        state.error = null;
        state.errorContext = null;
      }
      state.applyError = null;
    });

    const mutationRequest = buildMutationRequest(changes);
    const { request: submissionRequest, childQueuesToStart } =
      prepareMutationRequestForSubmission(mutationRequest);

    const parentQueuesToStop = getParentQueuesForAdditions(
      submissionRequest[MUTATION_OPERATIONS.ADD_QUEUE],
    );
    const queuesToStopForRemoval = getQueuesForRemoval(
      submissionRequest[MUTATION_OPERATIONS.REMOVE_QUEUE],
    );
    const queuesToStopForAutoCreation = getQueuesForAutoCreationEnable(changes);

    const parentQueuesStopped = new Set<string>();
    const removalQueuesStopped = new Set<string>();
    const autoCreationQueuesStopped = new Set<string>();
    const apiClient = get().apiClient;
    let mutationApplied = false;

    // Helper to get child queues for hierarchy collection
    const getChildQueues = (path: string) => get().getChildQueues(path);

    const restartParents = async () => restartQueues(parentQueuesStopped, apiClient);
    const restartRemovalQueues = async () => restartQueues(removalQueuesStopped, apiClient);
    const restartAutoCreationQueues = async () =>
      restartQueues(autoCreationQueuesStopped, apiClient);

    try {
      for (const parentQueue of parentQueuesToStop) {
        addQueueHierarchyToSet(parentQueue, parentQueuesStopped, getChildQueues);
      }

      for (const queueName of queuesToStopForRemoval) {
        addQueueHierarchyToSet(queueName, removalQueuesStopped, getChildQueues);
      }

      for (const queueName of queuesToStopForAutoCreation) {
        addQueueHierarchyToSet(queueName, autoCreationQueuesStopped, getChildQueues);
      }

      const allQueuesToStop = new Set<string>([
        ...parentQueuesStopped,
        ...removalQueuesStopped,
        ...autoCreationQueuesStopped,
      ]);

      if (allQueuesToStop.size > 0) {
        await applyQueueStates(allQueuesToStop, 'STOPPED', apiClient);
      }

      const validationResponse = await apiClient.validateSchedulerConf(submissionRequest);

      if (validationResponse.validation === 'failed') {
        const validationMessage = validationResponse.errors?.join('; ').trim();
        throw new Error(validationMessage || 'Scheduler configuration validation failed');
      }

      const mutationVersion =
        validationResponse.versionId ??
        validationResponse.mutationId ??
        validationResponse.newVersionId;

      const finalMutation = prepareMutationRequestWithVersion(submissionRequest, mutationVersion);

      await apiClient.updateSchedulerConf(finalMutation);
      mutationApplied = true;

      await restartParents();
      await restartAutoCreationQueues();

      for (const queueName of childQueuesToStart) {
        await applyQueueStates([queueName], 'RUNNING', apiClient);
      }

      // Reload configuration after successful update
      const [config, version] = await Promise.all([
        apiClient.getSchedulerConf(),
        apiClient.getSchedulerConfVersion(),
      ]);

      set((state) => {
        // Update config data
        state.configData = new Map(config.property.map((p) => [p.name, p.value]));
        state.configVersion = version.versionId;

        // Clear staged changes
        state.stagedChanges = [];
        state.isLoading = false;
        if (state.errorContext === 'mutation') {
          state.error = null;
          state.errorContext = null;
        }
        state.applyError = null;
      });

      // Refresh scheduler data to get updated queue information
      await get().refreshSchedulerData();
    } catch (error) {
      const errorMessage = extractErrorMessage(error);

      set((state) => {
        state.error = errorMessage;
        state.errorContext = 'mutation';
        state.applyError = errorMessage;
        state.isLoading = false;
      });

      throw createStoreError(
        isNetworkError(error) ? ERROR_CODES.NETWORK_ERROR : ERROR_CODES.APPLY_CHANGES_FAILED,
        errorMessage,
        error,
      );
    } finally {
      await restartParents();
      await restartAutoCreationQueues();
      if (!mutationApplied) {
        await restartRemovalQueues();
      }
    }
  },

  revertChange: (changeId) => {
    set((state) => {
      const beforeLength = state.stagedChanges.length;
      state.stagedChanges = state.stagedChanges.filter((c) => c.id !== changeId);
      if (state.stagedChanges.length !== beforeLength) {
        clearMutationError(state);
      }
    });

    // Refresh validation errors for remaining staged changes
    get().refreshValidationErrors();
  },

  clearAllChanges: () => {
    set((state) => {
      if (state.stagedChanges.length > 0) {
        state.stagedChanges = [];
        clearMutationError(state);
      }
    });
  },

  clearQueueChanges: (queuePath) => {
    set((state) => {
      const beforeLength = state.stagedChanges.length;
      state.stagedChanges = state.stagedChanges.filter((c) => c.queuePath !== queuePath);
      if (state.stagedChanges.length !== beforeLength) {
        clearMutationError(state);
      }
    });

    // Refresh validation errors for remaining staged changes
    get().refreshValidationErrors();
  },

  hasUnsavedChanges: () => {
    return get().stagedChanges.length > 0;
  },

  getChangesForQueue: (queuePath) => {
    return get().stagedChanges.filter((c) => c.queuePath === queuePath);
  },

  hasPendingDeletion: (queuePath) => {
    return get().stagedChanges.some((c) => c.queuePath === queuePath && c.type === 'remove');
  },

  revertQueueDeletion: (queuePath) => {
    const removalChange = get().stagedChanges.find(
      (c) => c.queuePath === queuePath && c.type === 'remove',
    );
    if (removalChange) {
      get().revertChange(removalChange.id);
    }
  },

  getStagedChangeById: (changeId) => {
    return get().stagedChanges.find((c) => c.id === changeId);
  },

  getLabelChangesForQueue: (queuePath, label) => {
    return get().stagedChanges.filter((c) => c.queuePath === queuePath && c.label === label);
  },

  refreshValidationErrors: () => {
    const { stagedChanges, schedulerData, configData } = get();

    if (!schedulerData || stagedChanges.length === 0) {
      return;
    }

    // Validate all staged changes using the unified validation function
    const validationResults = validateStagedChanges({
      stagedChanges,
      schedulerData,
      configData,
    });

    set((state) => {
      // Update each staged change with its validation errors
      state.stagedChanges = state.stagedChanges.map((change) => ({
        ...change,
        validationErrors: validationResults.get(change.id),
      }));
    });
  },

  refreshAffectedValidationErrors: (triggeringQueuePath: string, triggeringProperty: string) => {
    const { stagedChanges, schedulerData, configData } = get();

    if (!schedulerData || stagedChanges.length === 0) {
      return;
    }

    // Determine which queues and properties could be affected
    const affectedQueues = getAffectedQueuesForValidation(
      triggeringProperty,
      triggeringQueuePath,
      schedulerData,
      stagedChanges,
    );

    const affectedQueuePaths = new Set(affectedQueues);
    const affectedProperties = new Set<string>();

    // Some properties affect validation of other properties
    if (triggeringProperty === 'capacity') {
      affectedProperties.add('capacity');
      affectedProperties.add('maximum-capacity');
    } else if (triggeringProperty === SPECIAL_VALUES.LEGACY_MODE_PROPERTY) {
      // Legacy mode affects all capacity validations
      affectedProperties.add('capacity');
      affectedProperties.add('maximum-capacity');
      // Need to re-validate all queues when legacy mode changes
      stagedChanges.forEach((change) => {
        if (change.queuePath) {
          affectedQueuePaths.add(change.queuePath);
        }
      });
    }

    // Selectively validate only affected changes using the unified validation function
    const validationResults = validateStagedChanges({
      stagedChanges,
      schedulerData,
      configData,
      affectedQueuePaths,
      affectedProperties,
    });

    set((state) => {
      // Update each staged change with its validation errors
      state.stagedChanges = state.stagedChanges.map((change) => ({
        ...change,
        validationErrors: validationResults.get(change.id),
      }));
    });
  },
});
