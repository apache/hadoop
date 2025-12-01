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
 * Search slice - handles context-aware search functionality
 */

import type { StateCreator } from 'zustand';
import type { SearchSlice, SchedulerStore } from './types';
import { filterSchedulerTree, findMatchingQueues } from '~/utils/treeUtils';
import { globalPropertyDefinitions } from '~/config/properties/global-properties';
import { buildGlobalPropertyKey } from '~/utils/propertyUtils';
import { calculateSearchResults } from '~/utils/searchUtils';

export const createSearchSlice: StateCreator<
  SchedulerStore,
  [['zustand/immer', never]],
  [],
  SearchSlice
> = (set, get) => {
  // Memoization cache for search results - scoped to this store instance
  let cachedSearchQuery: string | null = null;
  let cachedSearchContext: string | null = null;
  let cachedSearchResults: { count: number; hasResults: boolean } | null = null;

  return {
    searchQuery: '',
    searchContext: null,
    isSearchFocused: false,
    selectedNodeLabelFilter: '', // '' represents DEFAULT partition

    setSearchQuery: (query) => {
      set((state) => {
        state.searchQuery = query;
      });
      // Invalidate cache when query changes
      if (query !== cachedSearchQuery) {
        cachedSearchResults = null;
      }
    },

    setSearchContext: (context) => {
      set((state) => {
        state.searchContext = context;
      });
      // Invalidate cache when context changes
      if (context !== cachedSearchContext) {
        cachedSearchResults = null;
      }
    },

    clearSearch: () => {
      set((state) => {
        state.searchQuery = '';
        state.isSearchFocused = false;
      });
    },

    setSearchFocused: (focused) => {
      set((state) => {
        state.isSearchFocused = focused;
      });
    },

    getFilteredQueues: () => {
      const { searchQuery, schedulerData } = get();
      if (!searchQuery || !schedulerData) return schedulerData;

      // Find all matching queue paths
      const matches = findMatchingQueues(schedulerData, searchQuery);

      // Return filtered scheduler tree
      return filterSchedulerTree(schedulerData, matches);
    },

    getFilteredNodes: () => {
      const { searchQuery, nodes } = get();
      if (!searchQuery) return nodes;

      const lowerQuery = searchQuery.toLowerCase();
      return nodes.filter(
        (node) =>
          node.nodeHostName.toLowerCase().includes(lowerQuery) ||
          node.nodeLabels?.some((label) => label.toLowerCase().includes(lowerQuery)) ||
          node.rack?.toLowerCase().includes(lowerQuery),
      );
    },

    getFilteredSettings: () => {
      const { searchQuery } = get();
      if (!searchQuery) return globalPropertyDefinitions;

      const lowerQuery = searchQuery.toLowerCase();

      return globalPropertyDefinitions.filter((prop) => {
        // Build the full property key
        const fullPropertyKey = buildGlobalPropertyKey(prop.name);

        // Search in property metadata - technical name, full key, and display name
        const matchesSearch =
          prop.name.toLowerCase().includes(lowerQuery) ||
          fullPropertyKey.toLowerCase().includes(lowerQuery) ||
          prop.displayName.toLowerCase().includes(lowerQuery) ||
          prop.description?.toLowerCase().includes(lowerQuery) ||
          prop.category.toLowerCase().includes(lowerQuery);

        return matchesSearch;
      });
    },

    getSearchResults: () => {
      const { searchQuery, searchContext } = get();

      // Return empty results if no query
      if (!searchQuery) return { count: 0, hasResults: false };

      // Check cache
      if (
        cachedSearchQuery === searchQuery &&
        cachedSearchContext === searchContext &&
        cachedSearchResults !== null
      ) {
        return cachedSearchResults;
      }

      // Calculate results using shared utility
      const results = calculateSearchResults({
        searchQuery,
        searchContext,
        filteredQueues: get().getFilteredQueues(),
        filteredNodes: get().getFilteredNodes(),
        filteredSettings: get().getFilteredSettings(),
      });

      // Cache the results
      cachedSearchQuery = searchQuery;
      cachedSearchContext = searchContext;
      cachedSearchResults = results;

      return cachedSearchResults;
    },

    selectNodeLabelFilter: (label) => {
      set((state) => {
        state.selectedNodeLabelFilter = label;
      });
    },

    getQueueAccessibility: (queuePath, label) => {
      // DEFAULT label is accessible to all
      if (label === '') return true;

      // Root queue always has access to all labels
      if (queuePath === 'root') return true;

      // Check if queue has accessible-node-labels configuration (including staged changes)
      const accessibleLabelsResult = get().getQueuePropertyValue(
        queuePath,
        'accessible-node-labels',
      );
      const accessibleLabels = accessibleLabelsResult.value;

      // Check if property exists in config (not just empty string default)
      const hasExplicitConfig = get().hasQueueProperty(queuePath, 'accessible-node-labels');

      if (!hasExplicitConfig) {
        // If not configured, inherit from parent
        const parentPath = queuePath.substring(0, queuePath.lastIndexOf('.'));
        if (parentPath && parentPath !== queuePath) {
          return get().getQueueAccessibility(parentPath, label);
        }
        return false;
      }

      // Empty string means only DEFAULT partition
      if (accessibleLabels === '') {
        return false;
      }

      // Check if label is in the accessible list
      const labelList = accessibleLabels.split(',').map((l) => l.trim());

      // '*' means access to all labels (typically only root has this)
      if (labelList.includes('*')) return true;

      return labelList.includes(label);
    },

    getQueueLabelCapacity: (queuePath, label) => {
      if (label === '') {
        // Return default capacities (including staged changes)
        const capacityResult = get().getQueuePropertyValue(queuePath, 'capacity');
        const maxCapacityResult = get().getQueuePropertyValue(queuePath, 'maximum-capacity');
        const absoluteCapacityResult = get().getQueuePropertyValue(queuePath, 'absolute-capacity');

        return {
          capacity: capacityResult.value || '0',
          maxCapacity: maxCapacityResult.value || '100',
          absoluteCapacity: absoluteCapacityResult.value || '0',
          isLabelSpecific: false,
          label: 'DEFAULT',
          hasAccess: true,
          canUseLabel: true,
        };
      }

      // Check if queue has access to this label
      const hasAccess = get().getQueueAccessibility(queuePath, label);

      // Return label-specific capacities (including staged changes)
      const capacityResult = get().getQueuePropertyValue(
        queuePath,
        `accessible-node-labels.${label}.capacity`,
      );
      const maxCapacityResult = get().getQueuePropertyValue(
        queuePath,
        `accessible-node-labels.${label}.maximum-capacity`,
      );
      const absoluteCapacityResult = get().getQueuePropertyValue(
        queuePath,
        `accessible-node-labels.${label}.absolute-capacity`,
      );

      const isRootQueue = queuePath === 'root';
      const capacity = capacityResult.value || (isRootQueue ? '100' : '0');

      return {
        capacity,
        maxCapacity: maxCapacityResult.value || '100',
        absoluteCapacity: absoluteCapacityResult.value || '0',
        isLabelSpecific: true,
        label,
        hasAccess,
        // A queue with 0% capacity cannot use the label even if it has access
        canUseLabel: hasAccess && parseFloat(capacity) > 0,
      };
    },
  };
};
