/**
 * Utilities for working with queue tree and scheduler structures
 *
 * This module provides functions for traversing, searching, and filtering
 * the hierarchical queue tree structure used by YARN Capacity Scheduler.
 */

import type { SchedulerInfo, QueueInfo } from '~/types';

// =============================================================================
// Queue Tree Traversal
// =============================================================================

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
 * Map a queue tree to an array using a mapper function
 * @param root The root queue to start from
 * @param mapper Function to transform each queue into the desired type
 * @param filter Optional function to filter which queues to include
 * @returns Array of mapped values
 */
export function mapQueueTree<T>(
  root: QueueInfo,
  mapper: (queue: QueueInfo, depth: number) => T,
  filter?: (queue: QueueInfo) => boolean,
): T[] {
  const result: T[] = [];
  traverseQueueTree(root, (queue, depth) => {
    if (!filter || filter(queue)) {
      result.push(mapper(queue, depth));
    }
  });
  return result;
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

// =============================================================================
// Scheduler Tree Operations
// =============================================================================

/**
 * Get all queues from a SchedulerInfo structure without conversion
 * @param scheduler The scheduler info to extract queues from
 * @returns Array of all queues in the scheduler
 */
export function flattenSchedulerTree(scheduler: SchedulerInfo): QueueInfo[] {
  const queues: QueueInfo[] = [];

  if (scheduler.queues?.queue) {
    scheduler.queues.queue.forEach((queue) => {
      queues.push(...flattenQueueTree(queue));
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
