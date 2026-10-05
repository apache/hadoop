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
import { AUTO_CREATION_PROPS } from '~/types/constants/auto-creation';
import { mapQueueTree } from '~/utils/treeUtils';

export interface QueueOption {
  value: string;
  label: string;
}

/**
 * Accessor for a queue's effective property value. Matches the store's getQueuePropertyValue.
 */
export type QueuePropertyAccessor = (
  queuePath: string,
  property: string,
) => { value: string; isStaged: boolean };

/**
 * Check whether Dynamic Queue Creation is enabled for a queue. A queue with
 * auto queue creation enabled acts as a parent even though it currently has no
 * static child queues, so it must be selectable as a parent queue in placement rules.
 *
 * Staged, not yet applied changes take precedence, if the operator has staged an
 * override for either auto creation property, that staged view is the source of
 * truth. Only when neither property has a staged override do we fall back to the
 * live scheduler view
 */
function hasAutoQueueCreation(
  queue: QueueInfo,
  getQueuePropertyValue?: QueuePropertyAccessor,
): boolean {
  if (getQueuePropertyValue) {
    const flexible = getQueuePropertyValue(queue.queuePath, AUTO_CREATION_PROPS.FLEXIBLE_ENABLED);
    const legacy = getQueuePropertyValue(queue.queuePath, AUTO_CREATION_PROPS.LEGACY_ENABLED);

    if (flexible.isStaged || legacy.isStaged) {
      return flexible.value === 'true' || legacy.value === 'true';
    }
  }

  const eligibility = queue.autoCreationEligibility;
  return (
    eligibility === AUTO_CREATION_PROPS.ELIGIBILITY_FLEXIBLE ||
    eligibility === AUTO_CREATION_PROPS.ELIGIBILITY_LEGACY
  );
}

/**
 * Check if a queue can act as a parent queue. This is true when the queue already
 * has static child queues, or when Dynamic Queue Creation is enabled for it.
 */
function isParentQueue(queue: QueueInfo, getQueuePropertyValue?: QueuePropertyAccessor): boolean {
  const hasChildren = !!(
    queue.queues?.queue &&
    (Array.isArray(queue.queues.queue) ? queue.queues.queue.length > 0 : true)
  );
  return hasChildren || hasAutoQueueCreation(queue, getQueuePropertyValue);
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
 *
 * Queues with Dynamic Queue Creation enabled are included even when they have
 * no static children, since dynamic child queues will be created under them.
 * Pass getQueuePropertyValue to also recognise queues whose Dynamic Queue
 * Creation toggle is staged but not yet applied.
 */
export function getAllParentQueues(
  schedulerData: SchedulerInfo | null,
  getQueuePropertyValue?: QueuePropertyAccessor,
): QueueOption[] {
  return getQueues(schedulerData, (queue) => isParentQueue(queue, getQueuePropertyValue));
}

/**
 * Get all queue paths from the scheduler data
 * Returns an array of queue options suitable for use in a combobox
 * This includes both parent and leaf queues
 */
export function getAllQueues(schedulerData: SchedulerInfo | null): QueueOption[] {
  return getQueues(schedulerData);
}
