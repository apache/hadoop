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
 * Scheduler data slice - handles loading and storing scheduler configuration
 */

import type { StateCreator } from 'zustand';
import type {
  ConfigProperty,
  NodeLabel,
  NodeLabelInfoItem,
  NodeLabelsResponse,
  NodeToLabelMapping,
  NodeToLabelsMapEntry,
  NodeToLabelsResponse,
} from '~/types';
import {
  createDetailedErrorMessage,
  createStoreError,
  ERROR_CODES,
  isNetworkError,
} from '~/lib/errors';
import type { SchedulerDataSlice, SchedulerStore } from './types';

export const createSchedulerDataSlice: StateCreator<
  SchedulerStore,
  [['zustand/immer', never]],
  [],
  SchedulerDataSlice
> = (set, get) => ({
  schedulerData: null,
  configData: new Map(),
  configVersion: 0,
  isLoading: false,
  error: null,
  errorContext: null,
  isReadOnly: false,

  loadInitialData: async () => {
    set((state) => {
      state.isLoading = true;
      if (state.errorContext === 'load') {
        state.error = null;
        state.errorContext = null;
      }
    });

    try {
      const [scheduler, config, labels, nodes, nodeToLabels, version] = await Promise.all([
        get().apiClient.getScheduler(),
        get().apiClient.getSchedulerConf(),
        get().apiClient.getNodeLabels(),
        get().apiClient.getNodes(),
        get().apiClient.getNodeToLabels(),
        get().apiClient.getSchedulerConfVersion(),
      ]);

      set((state) => {
        state.schedulerData = scheduler.scheduler.schedulerInfo;
        state.configData = new Map(config.property.map((p: ConfigProperty) => [p.name, p.value]));

        // Update node labels data
        state.nodeLabels = normalizeNodeLabels(labels);
        state.nodes = nodes.nodes?.node || [];
        state.nodeToLabels = normalizeNodeToLabels(nodeToLabels);

        state.configVersion = version.versionId;
        state.isReadOnly = get().apiClient.getIsReadOnly();
        state.isLoading = false;
        if (state.errorContext === 'load') {
          state.error = null;
          state.errorContext = null;
        }
      });
    } catch (error) {
      const errorMessage = createDetailedErrorMessage('load initial data', error);

      set((state) => {
        state.error = errorMessage;
        state.errorContext = 'load';
        state.isLoading = false;
      });

      throw createStoreError(
        isNetworkError(error) ? ERROR_CODES.NETWORK_ERROR : ERROR_CODES.LOAD_INITIAL_DATA_FAILED,
        errorMessage,
        error,
      );
    }
  },

  refreshSchedulerData: async () => {
    set((state) => {
      state.isLoading = true;
      if (state.errorContext === 'load') {
        state.error = null;
        state.errorContext = null;
      }
    });

    try {
      const scheduler = await get().apiClient.getScheduler();

      set((state) => {
        state.schedulerData = scheduler.scheduler.schedulerInfo;
        state.isLoading = false;
        if (state.errorContext === 'load') {
          state.error = null;
          state.errorContext = null;
        }
      });
    } catch (error) {
      const errorMessage = createDetailedErrorMessage('refresh scheduler data', error);

      set((state) => {
        state.error = errorMessage;
        state.errorContext = 'load';
        state.isLoading = false;
      });

      throw createStoreError(
        isNetworkError(error) ? ERROR_CODES.NETWORK_ERROR : ERROR_CODES.REFRESH_SCHEDULER_FAILED,
        errorMessage,
        error,
      );
    }
  },
});

/**
 * Helper function to normalize node labels from API response
 * Ensures exclusivity defaults to true if not specified (YARN default)
 */
function normalizeNodeLabels(response?: NodeLabelsResponse): NodeLabel[] {
  const rawNodeLabelInfo = response?.nodeLabelInfo;
  const nodeLabelInfo = Array.isArray(rawNodeLabelInfo)
    ? rawNodeLabelInfo
    : rawNodeLabelInfo
      ? [rawNodeLabelInfo]
      : [];

  return nodeLabelInfo.map((label) => ({
    ...label,
    exclusivity: parseExclusivity(label.exclusivity),
  }));
}

function parseExclusivity(value?: boolean | 'true' | 'false'): boolean {
  if (typeof value === 'string') {
    return value.toLowerCase() !== 'false';
  }

  return value ?? true;
}

type NodeLabelInfoLike =
  | NodeLabelInfoItem
  | NodeLabelInfoItem[]
  | {
      nodeLabelInfo?: NodeLabelInfoItem | NodeLabelInfoItem[];
    };

function normalizeNodeToLabels(response?: NodeToLabelsResponse): NodeToLabelMapping[] {
  const entries = extractEntries(response?.nodeToLabels);

  return entries.map((entry) => ({
    nodeId: entry.key,
    nodeLabels: extractLabelNames(entry.value?.nodeLabelInfo),
  }));
}

function extractEntries(data?: NodeToLabelsResponse['nodeToLabels']): NodeToLabelsMapEntry[] {
  if (!data || !data.entry) {
    return [];
  }

  const { entry } = data;
  return Array.isArray(entry) ? entry : [entry];
}

function extractLabelNames(info?: NodeLabelInfoLike): string[] {
  if (!info) {
    return [];
  }

  if (Array.isArray(info)) {
    return info.map((item) => item?.name).filter((name): name is string => Boolean(name));
  }

  if ('nodeLabelInfo' in info && info.nodeLabelInfo) {
    // Some responses nest nodeLabelInfo inside another object
    return extractLabelNames(info.nodeLabelInfo as NodeLabelInfoItem | NodeLabelInfoItem[]);
  }

  if (typeof info === 'object' && info !== null && 'name' in info) {
    const name = (info as NodeLabelInfoItem).name;
    return name ? [name] : [];
  }

  return [];
}
