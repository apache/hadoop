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


import type { QueueInfo, SchedulerInfo } from '~/types';
import { mapQueueTree } from '~/utils/treeUtils';

export interface QueueOption {
  value: string;
  label: string;
}

/**
 * Check if a queue has children (is a parent queue)
 */
function isParentQueue(queue: QueueInfo): boolean {
  return !!(
    queue.queues?.queue &&
    (Array.isArray(queue.queues.queue) ? queue.queues.queue.length > 0 : true)
  );
}

/**
 * Map a QueueInfo to a QueueOption
 */
const toQueueOption = (queue: QueueInfo): QueueOption => ({
  value: queue.queuePath,
  label: queue.queuePath,
});

/**
 * Generic function to get queues from scheduler data
 * @param schedulerData - The scheduler data
 * @param filter - Optional filter function to determine which queues to include
 */
function getQueues(
  schedulerData: SchedulerInfo | null,
  filter?: (queue: QueueInfo) => boolean,
): QueueOption[] {
  if (!schedulerData) {
    return [];
  }

  const result: QueueOption[] = [
    // Add root queue
    { value: 'root', label: 'root' },
  ];

  // Process child queues using shared utility
  if (schedulerData.queues?.queue) {
    const children = Array.isArray(schedulerData.queues.queue)
      ? schedulerData.queues.queue
      : [schedulerData.queues.queue];

    for (const child of children) {
      result.push(...mapQueueTree(child, toQueueOption, filter));
    }
  }

  return result.sort((a, b) => a.value.localeCompare(b.value));
}

/**
 * Get all parent queue paths from the scheduler data
 * Returns an array of queue options suitable for use in a combobox
 */
export function getAllParentQueues(schedulerData: SchedulerInfo | null): QueueOption[] {
  return getQueues(schedulerData, isParentQueue);
}

/**
 * Get all queue paths from the scheduler data
 * Returns an array of queue options suitable for use in a combobox
 * This includes both parent and leaf queues
 */
export function getAllQueues(schedulerData: SchedulerInfo | null): QueueOption[] {
  return getQueues(schedulerData);
}
