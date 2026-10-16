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


import { flattenSchedulerTree } from './treeUtils';
import type { SchedulerInfo } from '~/types';
import type { PropertyDescriptor } from '~/types/property-descriptor';

/**
 * Calculate search results based on search query, context, and filtered data
 */
export function calculateSearchResults(params: {
  searchQuery: string;
  searchContext: 'queues' | 'nodes' | 'settings' | null;
  filteredQueues: SchedulerInfo | null;
  filteredNodes: Array<{ nodeHostName: string; nodeLabels?: string[]; rack?: string }>;
  filteredSettings: PropertyDescriptor[];
}): { count: number; hasResults: boolean } {
  const { searchQuery, searchContext, filteredQueues, filteredNodes, filteredSettings } = params;

  // Return empty results if no query
  if (!searchQuery) {
    return { count: 0, hasResults: false };
  }

  let count = 0;

  switch (searchContext) {
    case 'queues': {
      if (filteredQueues) {
        count = flattenSchedulerTree(filteredQueues).length;
      }
      break;
    }
    case 'nodes':
      count = filteredNodes.length;
      break;
    case 'settings':
      count = filteredSettings.length;
      break;
    case null:
    default: {
      // When context is not set, search all contexts and return the total
      count =
        (filteredQueues ? flattenSchedulerTree(filteredQueues).length : 0) +
        filteredNodes.length +
        filteredSettings.length;
      break;
    }
  }

  return {
    count,
    hasResults: count > 0,
  };
}
