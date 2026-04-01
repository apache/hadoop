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
 * Queue data slice - provides getter functions for queue information
 */

import type { StateCreator } from 'zustand';
import { SPECIAL_VALUES } from '~/types';
import type { QueueInfo, CapacitySchedulerInfo, QueueCapacitiesByPartition } from '~/types';
import { buildGlobalPropertyKey, buildPropertyKey } from '~/utils/propertyUtils';
import { globalPropertyDefinitions } from '~/config/properties/global-properties';
import type { QueueDataSlice, SchedulerStore } from './types';

export const createQueueDataSlice: StateCreator<
  SchedulerStore,
  [['zustand/immer', never]],
  [],
  QueueDataSlice
> = (_set, get) => ({
  getQueuePropertyValue: (queuePath, property) => {
    const propertyKey = buildPropertyKey(queuePath, property);
    const configValue = get().configData.get(propertyKey) || '';

    // Check if there's a staged change for this property
    const stagedChange = get().stagedChanges.find(
      (c) => c.queuePath === queuePath && c.property === property,
    );

    if (stagedChange && stagedChange.newValue !== undefined) {
      return { value: stagedChange.newValue, isStaged: true };
    }

    return { value: configValue, isStaged: false };
  },

  getGlobalPropertyValue: (property) => {
    const propertyKey = buildGlobalPropertyKey(property);
    const configValue = get().configData.get(propertyKey);

    // Check if there's a staged change for this property
    const stagedChange = get().stagedChanges.find(
      (c) => c.queuePath === SPECIAL_VALUES.GLOBAL_QUEUE_PATH && c.property === property,
    );

    if (stagedChange && stagedChange.newValue !== undefined) {
      return { value: stagedChange.newValue, isStaged: true };
    }

    // If config doesn't have the value, check for default value
    if (configValue === undefined || configValue === null) {
      const propertyDef = globalPropertyDefinitions.find((p) => p.name === property);
      const defaultValue = propertyDef?.defaultValue || '';
      return { value: defaultValue, isStaged: false };
    }

    return { value: configValue, isStaged: false };
  },

  hasQueueProperty: (queuePath, property) => {
    const propertyKey = buildPropertyKey(queuePath, property);
    return (
      get().configData.has(propertyKey) ||
      get().stagedChanges.some((c) => c.queuePath === queuePath && c.property === property)
    );
  },

  getQueueByPath: (queuePath) => {
    const schedulerData = get().schedulerData;
    if (!schedulerData) return null;

    // Handle root queue - create a QueueInfo-compatible object from schedulerData
    if (queuePath === SPECIAL_VALUES.ROOT_QUEUE_NAME) {
      // Convert SchedulerInfo to QueueInfo format
      const rootQueueInfo: QueueInfo = {
        queueType: 'parent' as const,
        queueName: schedulerData.queueName,
        queuePath: SPECIAL_VALUES.ROOT_QUEUE_NAME,
        capacity: schedulerData.capacity,
        usedCapacity: schedulerData.usedCapacity,
        maxCapacity: schedulerData.maxCapacity,
        absoluteCapacity: schedulerData.capacity,
        absoluteMaxCapacity: schedulerData.maxCapacity,
        absoluteUsedCapacity: schedulerData.usedCapacity,
        numApplications: 0,
        numActiveApplications: 0,
        numPendingApplications: 0,
        state: 'RUNNING',
        queues: schedulerData.queues,
        resourcesUsed: {
          memory: 0,
          vCores: 0,
        },
        creationMethod: 'static',
      };
      return rootQueueInfo;
    }

    const pathParts = queuePath.split('.');

    // Find in the queue tree
    if (!schedulerData.queues?.queue) return null;

    const children = Array.isArray(schedulerData.queues.queue)
      ? schedulerData.queues.queue
      : [schedulerData.queues.queue];

    // Start traversing from root's children
    let currentQueue: QueueInfo | null = null;

    for (let i = 1; i < pathParts.length; i++) {
      const queueName = pathParts[i];

      if (i === 1) {
        // First level - search in root's children
        currentQueue = children.find((q: QueueInfo) => q.queueName === queueName) || null;
      } else {
        // Deeper levels - search in current queue's children
        if (!currentQueue?.queues?.queue) return null;

        const currentChildren: QueueInfo[] = Array.isArray(currentQueue.queues.queue)
          ? currentQueue.queues.queue
          : [currentQueue.queues.queue];

        currentQueue = currentChildren.find((q: QueueInfo) => q.queueName === queueName) || null;
      }

      if (!currentQueue) return null;
    }

    return currentQueue;
  },

  getChildQueues: (parentPath) => {
    const parentQueue = get().getQueueByPath(parentPath);
    if (!parentQueue || !parentQueue.queues?.queue) return [];

    return Array.isArray(parentQueue.queues.queue)
      ? parentQueue.queues.queue
      : [parentQueue.queues.queue];
  },

  getQueuePartitionCapacities: (queuePath, partitionName) => {
    const schedulerData = get().schedulerData as CapacitySchedulerInfo | null;
    if (!schedulerData) return null;

    // Handle root queue
    if (queuePath === SPECIAL_VALUES.ROOT_QUEUE_NAME) {
      if (!schedulerData.capacities?.queueCapacitiesByPartition) {
        return null;
      }

      const partition = schedulerData.capacities.queueCapacitiesByPartition.find(
        (p: QueueCapacitiesByPartition) => (p.partitionName || '') === partitionName,
      );

      return partition || null;
    }

    // For non-root queues, traverse the tree to find the queue with capacities data
    // Note: The API returns richer data than the QueueInfo type suggests
    const pathParts = queuePath.split('.');
    if (!schedulerData.queues?.queue) return null;

    const children = Array.isArray(schedulerData.queues.queue)
      ? schedulerData.queues.queue
      : [schedulerData.queues.queue];

    // eslint-disable-next-line @typescript-eslint/no-explicit-any
    let currentQueue: any = null;

    for (let i = 1; i < pathParts.length; i++) {
      const queueName = pathParts[i];

      if (i === 1) {
        // First level - search in root's children
        // eslint-disable-next-line @typescript-eslint/no-explicit-any
        currentQueue = children.find((q: any) => q.queueName === queueName) || null;
      } else {
        // Deeper levels - search in current queue's children
        if (!currentQueue?.queues?.queue) return null;

        const currentChildren = Array.isArray(currentQueue.queues.queue)
          ? currentQueue.queues.queue
          : [currentQueue.queues.queue];

        // eslint-disable-next-line @typescript-eslint/no-explicit-any
        currentQueue = currentChildren.find((q: any) => q.queueName === queueName) || null;
      }

      if (!currentQueue) return null;
    }

    if (!currentQueue?.capacities?.queueCapacitiesByPartition) {
      return null;
    }

    const partition = currentQueue.capacities.queueCapacitiesByPartition.find(
      (p: QueueCapacitiesByPartition) => (p.partitionName || '') === partitionName,
    );

    return partition || null;
  },
});
