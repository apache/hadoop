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
 * Utilities for working with SchedulerInfo structures
 */

import type { SchedulerInfo, QueueInfo } from '~/types';
import { queueTreeUtils } from './queueTreeUtils';

/**
 * Get all queues from a SchedulerInfo structure without conversion
 * @param scheduler The scheduler info to extract queues from
 * @returns Array of all queues in the scheduler
 */
export function flattenSchedulerTree(scheduler: SchedulerInfo): QueueInfo[] {
  const queues: QueueInfo[] = [];

  if (scheduler.queues?.queue) {
    scheduler.queues.queue.forEach((queue) => {
      queues.push(...queueTreeUtils.flattenQueueTree(queue));
    });
  }

  return queues;
}

/**
 * Filter a scheduler tree based on matching queue paths
 * @param scheduler The scheduler to filter
 * @param matches Set of queue paths that should be included
 * @returns The scheduler with only matching queues, or null if no matches
 */
export function filterSchedulerTree(
  scheduler: SchedulerInfo,
  matches: Set<string>,
): SchedulerInfo | null {
  if (!scheduler.queues?.queue || matches.size === 0) {
    return null;
  }

  // Filter root's children
  const filteredQueues = scheduler.queues.queue
    .map((queue) => filterQueueSubtree(queue, matches))
    .filter((queue): queue is QueueInfo => queue !== null);

  if (filteredQueues.length === 0) {
    return null;
  }

  // Return scheduler with filtered queues
  return {
    ...scheduler,
    queues: {
      queue: filteredQueues,
    },
  };
}

/**
 * Filter a queue subtree based on matching paths
 * @param queue The queue to filter
 * @param matches Set of queue paths that should be included
 * @returns The filtered queue or null if not included
 */
function filterQueueSubtree(queue: QueueInfo, matches: Set<string>): QueueInfo | null {
  if (!matches.has(queue.queuePath)) {
    return null;
  }

  // If this queue has children, filter them too
  if (queue.queues?.queue) {
    const filteredChildren = queue.queues.queue
      .map((child) => filterQueueSubtree(child, matches))
      .filter((child): child is QueueInfo => child !== null);

    return {
      ...queue,
      queues: filteredChildren.length > 0 ? { queue: filteredChildren } : undefined,
    };
  }

  return queue;
}

/**
 * Build an index of queue relationships for efficient lookups
 * @param queues Array of all queues
 * @returns Index containing parent-child relationships and path lookups
 */
function buildQueueIndex(queues: QueueInfo[]) {
  const pathToQueue = new Map<string, QueueInfo>();
  const pathToDescendants = new Map<string, Set<string>>();

  // First pass: build path lookup
  queues.forEach((queue) => {
    pathToQueue.set(queue.queuePath, queue);
  });

  // Second pass: build descendant relationships
  queues.forEach((queue) => {
    const pathParts = queue.queuePath.split('.');

    // For each ancestor path, add this queue as a descendant
    for (let i = 1; i < pathParts.length; i++) {
      const ancestorPath = pathParts.slice(0, i).join('.');
      if (!pathToDescendants.has(ancestorPath)) {
        pathToDescendants.set(ancestorPath, new Set());
      }
      pathToDescendants.get(ancestorPath)!.add(queue.queuePath);
    }
  });

  return { pathToQueue, pathToDescendants };
}

/**
 * Find all queues matching a search query
 * @param scheduler The scheduler to search in
 * @param searchQuery The search query
 * @returns Set of queue paths that match, including ancestors and descendants
 */
export function findMatchingQueues(scheduler: SchedulerInfo, searchQuery: string): Set<string> {
  const matches = new Set<string>();
  const lowerQuery = searchQuery.toLowerCase();

  // Get all queues
  const allQueues = flattenSchedulerTree(scheduler);

  // Build index for efficient lookups
  const { pathToDescendants } = buildQueueIndex(allQueues);

  // Find direct matches
  allQueues.forEach((queue) => {
    if (
      queue.queueName.toLowerCase().includes(lowerQuery) ||
      queue.queuePath.toLowerCase().includes(lowerQuery)
    ) {
      // Add the match itself
      matches.add(queue.queuePath);

      // Add all ancestors
      const pathParts = queue.queuePath.split('.');
      for (let i = 1; i <= pathParts.length; i++) {
        matches.add(pathParts.slice(0, i).join('.'));
      }

      // Add all descendants using the index
      const descendants = pathToDescendants.get(queue.queuePath);
      if (descendants) {
        descendants.forEach((descendantPath) => {
          matches.add(descendantPath);
        });
      }
    }
  });

  return matches;
}

// Export as namespace for consistency with queueTreeUtils
export const schedulerTreeUtils = {
  flattenSchedulerTree,
  filterSchedulerTree,
  findMatchingQueues,
};
