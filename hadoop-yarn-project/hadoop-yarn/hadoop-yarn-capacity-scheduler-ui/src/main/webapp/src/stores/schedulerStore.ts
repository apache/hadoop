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
 * Main scheduler store - combines all slices into a single store
 */

import { create } from 'zustand';
import { immer } from 'zustand/middleware/immer';
import { enableMapSet } from 'immer';
import { YarnApiClient } from '~/lib/api/YarnApiClient';
import { API_CONFIG } from '~/lib/api/config';
import {
  createSchedulerDataSlice,
  createNodeLabelsSlice,
  createStagedChangesSlice,
  createQueueSelectionSlice,
  createQueueDataSlice,
  createPlacementRulesSlice,
  createSearchSlice,
  createCapacityEditorSlice,
  type SchedulerStore,
} from './slices';

// Enable Map and Set support in immer
enableMapSet();

/**
 * Creates the scheduler store with the given API client
 */
const createStoreImplementation = (apiClient: YarnApiClient) =>
  immer<SchedulerStore>((set, get, api) => ({
    apiClient,
    ...createSchedulerDataSlice(set, get, api),
    ...createNodeLabelsSlice(set, get, api),
    ...createStagedChangesSlice(set, get, api),
    ...createQueueSelectionSlice(set, get, api),
    ...createQueueDataSlice(set, get, api),
    ...createPlacementRulesSlice(set, get, api),
    ...createSearchSlice(set, get, api),
    ...createCapacityEditorSlice(set, get, api),
  }));

/**
 * The default scheduler store instance
 */
export const useSchedulerStore = create(
  createStoreImplementation(
    new YarnApiClient(API_CONFIG.baseUrl, { userName: API_CONFIG.userName }),
  ),
);

/**
 * Factory function to create a scheduler store with a custom API client
 * Useful for testing or different environments
 */
export const createSchedulerStore = (apiClient: YarnApiClient) => {
  return create(createStoreImplementation(apiClient));
};

// Re-export types
export type { SchedulerStore } from './slices';
