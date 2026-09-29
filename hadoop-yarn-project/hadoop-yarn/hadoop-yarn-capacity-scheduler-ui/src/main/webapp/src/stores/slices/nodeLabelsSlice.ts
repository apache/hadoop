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
 * Node labels slice - handles node label management operations
 */

import type { StateCreator } from 'zustand';
import {
  createDetailedErrorMessage,
  createStoreError,
  ERROR_CODES,
  isNetworkError,
} from '~/lib/errors';
import { createReadOnlyBlockedError } from '~/lib/errors/readOnlyGuard';
import { normalizeNodeLabels, normalizeNodeToLabels } from '~/lib/normalizers/nodeDataNormalizers';
import { validateLabelRemoval } from '~/features/node-labels/utils/labelValidation';
import type { NodeLabelsSlice, SchedulerStore } from './types';

export const createNodeLabelsSlice: StateCreator<
  SchedulerStore,
  [['zustand/immer', never]],
  [],
  NodeLabelsSlice
> = (set, get) => ({
  nodeLabels: [],
  nodes: [],
  nodeToLabels: [],
  selectedNodeLabel: null,

  selectNodeLabel: (label) => {
    set((state) => {
      state.selectedNodeLabel = label;
    });
  },

  addNodeLabel: async (name, exclusivity) => {
    // Block adding node labels in read-only mode
    if (get().isReadOnly) {
      const { errorMessage, error } = createReadOnlyBlockedError('add node labels');

      set((state) => {
        state.error = errorMessage;
        state.errorContext = 'nodeLabels';
        state.isLoading = false;
      });

      throw error;
    }

    set((state) => {
      state.isLoading = true;
      if (state.errorContext === 'nodeLabels') {
        state.error = null;
        state.errorContext = null;
      }
    });

    try {
      await get().apiClient.addNodeLabels([{ name, exclusivity }]);

      // Refresh node labels
      const labels = await get().apiClient.getNodeLabels();

      set((state) => {
        state.nodeLabels = normalizeNodeLabels(labels);
        state.isLoading = false;
        if (state.errorContext === 'nodeLabels') {
          state.error = null;
          state.errorContext = null;
        }
      });
    } catch (error) {
      const errorMessage = createDetailedErrorMessage(`add node label "${name}"`, error);

      set((state) => {
        state.error = errorMessage;
        state.errorContext = 'nodeLabels';
        state.isLoading = false;
      });

      throw createStoreError(
        isNetworkError(error) ? ERROR_CODES.NETWORK_ERROR : ERROR_CODES.ADD_NODE_LABEL_FAILED,
        errorMessage,
        error,
      );
    }
  },

  removeNodeLabel: async (name) => {
    // Block removing node labels in read-only mode
    if (get().isReadOnly) {
      const { errorMessage, error } = createReadOnlyBlockedError('remove node labels');

      set((state) => {
        state.error = errorMessage;
        state.errorContext = 'nodeLabels';
        state.isLoading = false;
      });

      throw error;
    }

    // Validate that the label can be safely removed
    const nodeToLabels = get().nodeToLabels;
    const nodeAssignments = new Map<string, string[]>();
    nodeToLabels.forEach((mapping) => {
      nodeAssignments.set(mapping.nodeId, mapping.nodeLabels);
    });

    const validation = validateLabelRemoval(name, nodeAssignments);
    if (!validation.valid) {
      const errorMessage = validation.error || `Cannot remove label "${name}"`;

      set((state) => {
        state.error = errorMessage;
        state.errorContext = 'nodeLabels';
        state.isLoading = false;
      });

      throw createStoreError(ERROR_CODES.REMOVE_NODE_LABEL_FAILED, errorMessage);
    }

    set((state) => {
      state.isLoading = true;
      if (state.errorContext === 'nodeLabels') {
        state.error = null;
        state.errorContext = null;
      }
    });

    try {
      await get().apiClient.removeNodeLabels([name]);

      // Refresh node labels and node-to-label mappings
      const [labels, nodeToLabels] = await Promise.all([
        get().apiClient.getNodeLabels(),
        get().apiClient.getNodeToLabels(),
      ]);

      set((state) => {
        state.nodeLabels = normalizeNodeLabels(labels);
        state.nodeToLabels = normalizeNodeToLabels(nodeToLabels);
        state.isLoading = false;

        // Clear selection if the removed label was selected
        if (state.selectedNodeLabel === name) {
          state.selectedNodeLabel = null;
        }

        if (state.errorContext === 'nodeLabels') {
          state.error = null;
          state.errorContext = null;
        }
      });
    } catch (error) {
      const errorMessage = createDetailedErrorMessage(`remove node label "${name}"`, error);

      set((state) => {
        state.error = errorMessage;
        state.errorContext = 'nodeLabels';
        state.isLoading = false;
      });

      throw createStoreError(
        isNetworkError(error) ? ERROR_CODES.NETWORK_ERROR : ERROR_CODES.REMOVE_NODE_LABEL_FAILED,
        errorMessage,
        error,
      );
    }
  },

  assignNodeToLabel: async (nodeId, labelName) => {
    // Block assigning nodes to labels in read-only mode
    if (get().isReadOnly) {
      const { errorMessage, error } = createReadOnlyBlockedError('assign nodes to labels');

      set((state) => {
        state.error = errorMessage;
        state.errorContext = 'nodeLabels';
        state.isLoading = false;
      });

      throw error;
    }

    set((state) => {
      state.isLoading = true;
      if (state.errorContext === 'nodeLabels') {
        state.error = null;
        state.errorContext = null;
      }
    });

    try {
      // Replace with new label or empty array if null
      const newLabels = labelName ? [labelName] : [];

      await get().apiClient.replaceNodeToLabels([{ nodeId, labels: newLabels }]);

      // Refresh node-to-label mappings
      const nodeToLabels = await get().apiClient.getNodeToLabels();

      set((state) => {
        state.nodeToLabels = normalizeNodeToLabels(nodeToLabels);
        state.isLoading = false;
        if (state.errorContext === 'nodeLabels') {
          state.error = null;
          state.errorContext = null;
        }
      });
    } catch (error) {
      const errorMessage = createDetailedErrorMessage(
        `assign node "${nodeId}" to label "${labelName || 'DEFAULT'}"`,
        error,
      );

      set((state) => {
        state.error = errorMessage;
        state.errorContext = 'nodeLabels';
        state.isLoading = false;
      });

      throw createStoreError(
        isNetworkError(error) ? ERROR_CODES.NETWORK_ERROR : ERROR_CODES.ASSIGN_NODE_TO_LABEL_FAILED,
        errorMessage,
        error,
      );
    }
  },
});
