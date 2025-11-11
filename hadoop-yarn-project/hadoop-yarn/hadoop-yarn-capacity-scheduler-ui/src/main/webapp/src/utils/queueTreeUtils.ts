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
 * Utilities for working with queue tree structures
 */

import type { QueueInfo, SchedulerInfo } from '~/types';

/**
 * Flatten a queue tree into a flat array of all queues
 * @param root The root queue to start from
 * @returns Array of all queues in the tree
 */
export function flattenQueueTree(root: QueueInfo): QueueInfo[] {
  const result: QueueInfo[] = [root];

  if (root.queues?.queue) {
    root.queues.queue.forEach((child) => {
      result.push(...flattenQueueTree(child));
    });
  }

  return result;
}

/**
 * Traverse a queue tree and call a callback for each queue
 * @param root The root queue to start from
 * @param callback Function to call for each queue
 * @param depth Current depth in the tree (starts at 0)
 * @param parent Parent queue (undefined for root)
 */
export function traverseQueueTree(
  root: QueueInfo,
  callback: (queue: QueueInfo, depth: number, parent?: QueueInfo) => void,
  depth = 0,
  parent?: QueueInfo,
): void {
  callback(root, depth, parent);

  if (root.queues?.queue) {
    root.queues.queue.forEach((child) => {
      traverseQueueTree(child, callback, depth + 1, root);
    });
  }
}

/**
 * Find a queue by its path in the queue tree or scheduler data
 * @param rootOrScheduler The root queue or scheduler data to search from
 * @param queuePath The queue path to find (e.g., "root", "root.production.team1")
 * @returns The queue if found, null otherwise
 */
export function findQueueByPath(
  rootOrScheduler: QueueInfo | SchedulerInfo | undefined | null,
  queuePath: string,
): QueueInfo | null {
  if (!rootOrScheduler || !queuePath) {
    return null;
  }

  // Handle root queue special case for SchedulerInfo
  if (
    'queueName' in rootOrScheduler &&
    rootOrScheduler.queueName === 'root' &&
    queuePath === 'root'
  ) {
    return rootOrScheduler as unknown as QueueInfo;
  }

  // If it's a QueueInfo and matches, return it
  if ('queuePath' in rootOrScheduler && rootOrScheduler.queuePath === queuePath) {
    return rootOrScheduler as QueueInfo;
  }

  // Use iterative approach for performance
  const pathParts = queuePath.split('.');
  let currentQueue: QueueInfo | undefined = rootOrScheduler as QueueInfo;

  // Verify first part matches root queue name
  if (!currentQueue || pathParts[0] !== currentQueue.queueName) {
    return null;
  }

  // Traverse down the path
  for (let i = 1; i < pathParts.length; i += 1) {
    if (!currentQueue?.queues?.queue) {
      return null;
    }

    currentQueue = currentQueue.queues.queue.find((q) => q.queueName === pathParts[i]);

    if (!currentQueue) {
      return null;
    }
  }

  return currentQueue;
}

/**
 * Get all sibling queues for a given queue path
 * @param schedulerData The scheduler data
 * @param queuePath The queue path to find siblings for
 * @returns Array of sibling queues (excluding the queue itself)
 */
export function getSiblingQueues(
  schedulerData: SchedulerInfo | undefined | null,
  queuePath: string,
): QueueInfo[] {
  const lastDotIndex = queuePath.lastIndexOf('.');
  const parentPath = lastDotIndex > 0 ? queuePath.substring(0, lastDotIndex) : null;

  if (!parentPath) {
    return [];
  }

  const parentQueue = findQueueByPath(schedulerData, parentPath);
  return parentQueue?.queues?.queue || [];
}

// Export as namespace for easier use
export const queueTreeUtils = {
  flattenQueueTree,
  traverseQueueTree,
  findQueueByPath,
  getSiblingQueues,
};
