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


import { describe, it, expect, beforeEach } from 'vitest';
import { createSchedulerStore } from '~/stores/schedulerStore';
import { YarnApiClient } from '~/lib/api/YarnApiClient';
import type { QueueInfo, SchedulerInfo } from '~/types';
import { SPECIAL_VALUES, CONFIG_PREFIXES } from '~/types';

/**
 * Local helper: traverse queue tree and apply a visitor function.
 * Combines queue info with configured properties from configData.
 */
function traverseQueueTree(
  queueInfo: QueueInfo,
  configData: Map<string, string>,
  visitor: (queue: QueueInfo & { configured: Record<string, string> }) => void,
): void {
  const configured: Record<string, string> = {};

  const prefix = `${CONFIG_PREFIXES.BASE}.${queueInfo.queuePath}.`;
  for (const [key, value] of configData.entries()) {
    if (key.startsWith(prefix)) {
      const property = key.substring(prefix.length);
      configured[property] = value;
    }
  }

  const combinedQueue = {
    ...queueInfo,
    configured,
  };

  visitor(combinedQueue);

  if (queueInfo.queues?.queue) {
    const children = Array.isArray(queueInfo.queues.queue)
      ? queueInfo.queues.queue
      : [queueInfo.queues.queue];

    for (const child of children) {
      traverseQueueTree(child, configData, visitor);
    }
  }
}

describe('queueDataSlice', () => {
  const createTestStore = () => {
    const mockApiClient = new YarnApiClient('http://test.com', {});
    return createSchedulerStore(mockApiClient);
  };

  const createMockSchedulerData = (): SchedulerInfo => ({
    type: 'capacityScheduler',
    capacity: 100,
    usedCapacity: 10,
    maxCapacity: 100,
    queueName: 'root',
    queues: {
      queue: [
        {
          queueName: 'default',
          queuePath: 'root.default',
          queueType: 'leaf',
          capacity: 40,
          usedCapacity: 5,
          maxCapacity: 40,
          absoluteCapacity: 40,
          absoluteMaxCapacity: 40,
          absoluteUsedCapacity: 5,
          numApplications: 1,
          numActiveApplications: 1,
          numPendingApplications: 0,
          state: 'RUNNING',
          resourcesUsed: { memory: 1024, vCores: 1 },
          creationMethod: 'static',
        },
        {
          queueName: 'production',
          queuePath: 'root.production',
          queueType: 'parent',
          capacity: 60,
          usedCapacity: 5,
          maxCapacity: 60,
          absoluteCapacity: 60,
          absoluteMaxCapacity: 60,
          absoluteUsedCapacity: 5,
          numApplications: 0,
          numActiveApplications: 0,
          numPendingApplications: 0,
          state: 'RUNNING',
          resourcesUsed: { memory: 0, vCores: 0 },
          creationMethod: 'static',
          queues: {
            queue: [
              {
                queueName: 'critical',
                queuePath: 'root.production.critical',
                queueType: 'leaf',
                capacity: 70,
                usedCapacity: 0,
                maxCapacity: 70,
                absoluteCapacity: 42,
                absoluteMaxCapacity: 42,
                absoluteUsedCapacity: 0,
                numApplications: 0,
                numActiveApplications: 0,
                numPendingApplications: 0,
                state: 'RUNNING',
                resourcesUsed: { memory: 0, vCores: 0 },
                creationMethod: 'static',
              },
              {
                queueName: 'batch',
                queuePath: 'root.production.batch',
                queueType: 'leaf',
                capacity: 30,
                usedCapacity: 0,
                maxCapacity: 30,
                absoluteCapacity: 18,
                absoluteMaxCapacity: 18,
                absoluteUsedCapacity: 0,
                numApplications: 0,
                numActiveApplications: 0,
                numPendingApplications: 0,
                state: 'RUNNING',
                resourcesUsed: { memory: 0, vCores: 0 },
                creationMethod: 'static',
              },
            ],
          },
        },
      ],
    },
  });

  beforeEach(() => {
    // Clear any state between tests
  });

  describe('getQueuePropertyValue', () => {
    it('should return config value when no staged changes exist', () => {
      const store = createTestStore();
      store.setState({
        configData: new Map([['yarn.scheduler.capacity.root.default.capacity', '40']]),
        stagedChanges: [],
      });

      const result = store.getState().getQueuePropertyValue('root.default', 'capacity');

      expect(result.value).toBe('40');
      expect(result.isStaged).toBe(false);
    });

    it('should return empty string when property does not exist', () => {
      const store = createTestStore();
      store.setState({
        configData: new Map(),
        stagedChanges: [],
      });

      const result = store.getState().getQueuePropertyValue('root.default', 'capacity');

      expect(result.value).toBe('');
      expect(result.isStaged).toBe(false);
    });

    it('should return staged value when staged change exists', () => {
      const store = createTestStore();
      store.setState({
        configData: new Map([['yarn.scheduler.capacity.root.default.capacity', '40']]),
        stagedChanges: [
          {
            id: '1',
            type: 'update',
            queuePath: 'root.default',
            property: 'capacity',
            oldValue: '40',
            newValue: '50',
            timestamp: Date.now(),
          },
        ],
      });

      const result = store.getState().getQueuePropertyValue('root.default', 'capacity');

      expect(result.value).toBe('50');
      expect(result.isStaged).toBe(true);
    });

    it('should return staged value over config value', () => {
      const store = createTestStore();
      store.setState({
        configData: new Map([['yarn.scheduler.capacity.root.default.capacity', '40']]),
        stagedChanges: [
          {
            id: '1',
            type: 'update',
            queuePath: 'root.default',
            property: 'capacity',
            oldValue: '40',
            newValue: '60',
            timestamp: Date.now(),
          },
        ],
      });

      const result = store.getState().getQueuePropertyValue('root.default', 'capacity');

      expect(result.value).toBe('60');
      expect(result.isStaged).toBe(true);
    });

    it('should handle multiple staged changes and return the matching one', () => {
      const store = createTestStore();
      store.setState({
        configData: new Map([
          ['yarn.scheduler.capacity.root.default.capacity', '40'],
          ['yarn.scheduler.capacity.root.production.capacity', '60'],
        ]),
        stagedChanges: [
          {
            id: '1',
            type: 'update',
            queuePath: 'root.default',
            property: 'capacity',
            oldValue: '40',
            newValue: '45',
            timestamp: Date.now(),
          },
          {
            id: '2',
            type: 'update',
            queuePath: 'root.production',
            property: 'capacity',
            oldValue: '60',
            newValue: '55',
            timestamp: Date.now(),
          },
        ],
      });

      const result1 = store.getState().getQueuePropertyValue('root.default', 'capacity');
      const result2 = store.getState().getQueuePropertyValue('root.production', 'capacity');

      expect(result1.value).toBe('45');
      expect(result1.isStaged).toBe(true);
      expect(result2.value).toBe('55');
      expect(result2.isStaged).toBe(true);
    });

    it('should handle newValue of empty string', () => {
      const store = createTestStore();
      store.setState({
        configData: new Map([['yarn.scheduler.capacity.root.default.capacity', '40']]),
        stagedChanges: [
          {
            id: '1',
            type: 'update',
            queuePath: 'root.default',
            property: 'capacity',
            oldValue: '40',
            newValue: '',
            timestamp: Date.now(),
          },
        ],
      });

      const result = store.getState().getQueuePropertyValue('root.default', 'capacity');

      expect(result.value).toBe('');
      expect(result.isStaged).toBe(true);
    });
  });

  describe('getGlobalPropertyValue', () => {
    it('should return config value when no staged changes exist', () => {
      const store = createTestStore();
      store.setState({
        configData: new Map([['yarn.scheduler.capacity.legacy-queue-mode.enabled', 'true']]),
        stagedChanges: [],
      });

      const result = store.getState().getGlobalPropertyValue('legacy-queue-mode.enabled');

      expect(result.value).toBe('true');
      expect(result.isStaged).toBe(false);
    });

    it('should return default value when property does not exist in config', () => {
      const store = createTestStore();
      store.setState({
        configData: new Map(),
        stagedChanges: [],
      });

      const result = store.getState().getGlobalPropertyValue('legacy-queue-mode.enabled');

      // Should return default value from property definitions
      expect(result.value).toBeDefined();
      expect(result.isStaged).toBe(false);
    });

    it('should return staged value when staged change exists', () => {
      const store = createTestStore();
      store.setState({
        configData: new Map([['yarn.scheduler.capacity.legacy-queue-mode.enabled', 'false']]),
        stagedChanges: [
          {
            id: '1',
            type: 'update',
            queuePath: SPECIAL_VALUES.GLOBAL_QUEUE_PATH,
            property: 'legacy-queue-mode.enabled',
            oldValue: 'false',
            newValue: 'true',
            timestamp: Date.now(),
          },
        ],
      });

      const result = store.getState().getGlobalPropertyValue('legacy-queue-mode.enabled');

      expect(result.value).toBe('true');
      expect(result.isStaged).toBe(true);
    });

    it('should return staged value over config value', () => {
      const store = createTestStore();
      store.setState({
        configData: new Map([['yarn.scheduler.capacity.legacy-queue-mode.enabled', 'false']]),
        stagedChanges: [
          {
            id: '1',
            type: 'update',
            queuePath: SPECIAL_VALUES.GLOBAL_QUEUE_PATH,
            property: 'legacy-queue-mode.enabled',
            oldValue: 'false',
            newValue: 'true',
            timestamp: Date.now(),
          },
        ],
      });

      const result = store.getState().getGlobalPropertyValue('legacy-queue-mode.enabled');

      expect(result.value).toBe('true');
      expect(result.isStaged).toBe(true);
    });

    it('should handle null config value with default', () => {
      const store = createTestStore();
      store.setState({
        configData: new Map([['yarn.scheduler.capacity.legacy-queue-mode.enabled', null as any]]),
        stagedChanges: [],
      });

      const result = store.getState().getGlobalPropertyValue('legacy-queue-mode.enabled');

      // Should fallback to default value
      expect(result.value).toBeDefined();
      expect(result.isStaged).toBe(false);
    });

    it('should handle undefined config value with default', () => {
      const store = createTestStore();
      store.setState({
        configData: new Map([
          ['yarn.scheduler.capacity.legacy-queue-mode.enabled', undefined as any],
        ]),
        stagedChanges: [],
      });

      const result = store.getState().getGlobalPropertyValue('legacy-queue-mode.enabled');

      // Should fallback to default value
      expect(result.value).toBeDefined();
      expect(result.isStaged).toBe(false);
    });
  });

  describe('hasQueueProperty', () => {
    it('should return true when property exists in config', () => {
      const store = createTestStore();
      store.setState({
        configData: new Map([['yarn.scheduler.capacity.root.default.capacity', '40']]),
        stagedChanges: [],
      });

      const result = store.getState().hasQueueProperty('root.default', 'capacity');

      expect(result).toBe(true);
    });

    it('should return false when property does not exist', () => {
      const store = createTestStore();
      store.setState({
        configData: new Map(),
        stagedChanges: [],
      });

      const result = store.getState().hasQueueProperty('root.default', 'capacity');

      expect(result).toBe(false);
    });

    it('should return true when property exists in staged changes', () => {
      const store = createTestStore();
      store.setState({
        configData: new Map(),
        stagedChanges: [
          {
            id: '1',
            type: 'update',
            queuePath: 'root.default',
            property: 'capacity',
            oldValue: '',
            newValue: '50',
            timestamp: Date.now(),
          },
        ],
      });

      const result = store.getState().hasQueueProperty('root.default', 'capacity');

      expect(result).toBe(true);
    });

    it('should return true when property exists in both config and staged changes', () => {
      const store = createTestStore();
      store.setState({
        configData: new Map([['yarn.scheduler.capacity.root.default.capacity', '40']]),
        stagedChanges: [
          {
            id: '1',
            type: 'update',
            queuePath: 'root.default',
            property: 'capacity',
            oldValue: '40',
            newValue: '50',
            timestamp: Date.now(),
          },
        ],
      });

      const result = store.getState().hasQueueProperty('root.default', 'capacity');

      expect(result).toBe(true);
    });
  });

  describe('getQueueByPath', () => {
    it('should return null when schedulerData is null', () => {
      const store = createTestStore();
      store.setState({
        schedulerData: null,
      });

      const result = store.getState().getQueueByPath('root.default');

      expect(result).toBe(null);
    });

    it('should return root queue when path is "root"', () => {
      const store = createTestStore();
      const schedulerData = createMockSchedulerData();
      store.setState({
        schedulerData,
      });

      const result = store.getState().getQueueByPath('root');

      expect(result).not.toBeNull();
      expect(result?.queueName).toBe('root');
      expect(result?.queuePath).toBe('root');
      expect(result?.queueType).toBe('parent');
      expect(result?.capacity).toBe(100);
    });

    it('should return first-level child queue', () => {
      const store = createTestStore();
      const schedulerData = createMockSchedulerData();
      store.setState({
        schedulerData,
      });

      const result = store.getState().getQueueByPath('root.default');

      expect(result).not.toBeNull();
      expect(result?.queueName).toBe('default');
      expect(result?.queuePath).toBe('root.default');
      expect(result?.capacity).toBe(40);
    });

    it('should return nested child queue', () => {
      const store = createTestStore();
      const schedulerData = createMockSchedulerData();
      store.setState({
        schedulerData,
      });

      const result = store.getState().getQueueByPath('root.production.critical');

      expect(result).not.toBeNull();
      expect(result?.queueName).toBe('critical');
      expect(result?.queuePath).toBe('root.production.critical');
      expect(result?.capacity).toBe(70);
    });

    it('should return null for non-existent queue', () => {
      const store = createTestStore();
      const schedulerData = createMockSchedulerData();
      store.setState({
        schedulerData,
      });

      const result = store.getState().getQueueByPath('root.nonexistent');

      expect(result).toBe(null);
    });

    it('should return null when parent has no children', () => {
      const store = createTestStore();
      const schedulerData = createMockSchedulerData();
      store.setState({
        schedulerData,
      });

      const result = store.getState().getQueueByPath('root.default.child');

      expect(result).toBe(null);
    });

    it('should handle single child object instead of array', () => {
      const store = createTestStore();
      const schedulerData = createMockSchedulerData();
      // Modify to have single child
      schedulerData.queues = {
        queue: [
          {
            queueName: 'default',
            queuePath: 'root.default',
            queueType: 'leaf',
            capacity: 100,
            usedCapacity: 0,
            maxCapacity: 100,
            absoluteCapacity: 100,
            absoluteMaxCapacity: 100,
            absoluteUsedCapacity: 0,
            numApplications: 0,
            numActiveApplications: 0,
            numPendingApplications: 0,
            state: 'RUNNING',
            resourcesUsed: { memory: 0, vCores: 0 },
            creationMethod: 'static',
          },
        ] as QueueInfo[],
      };
      store.setState({
        schedulerData,
      });

      const result = store.getState().getQueueByPath('root.default');

      expect(result).not.toBeNull();
      expect(result?.queueName).toBe('default');
    });

    it('should return null when queues.queue is missing', () => {
      const store = createTestStore();
      const schedulerData = createMockSchedulerData();

      (schedulerData.queues as any) = undefined;
      store.setState({
        schedulerData,
      });

      const result = store.getState().getQueueByPath('root.default');

      expect(result).toBe(null);
    });

    it('should handle deeply nested queues', () => {
      const store = createTestStore();
      const schedulerData = createMockSchedulerData();
      // Add a deeply nested queue
      const productionQueue = (schedulerData.queues!.queue as QueueInfo[])[1];
      const criticalQueue = (productionQueue.queues!.queue as QueueInfo[])[0];
      criticalQueue.queues = {
        queue: [
          {
            queueName: 'urgent',
            queuePath: 'root.production.critical.urgent',
            queueType: 'leaf',
            capacity: 100,
            usedCapacity: 0,
            maxCapacity: 100,
            absoluteCapacity: 42,
            absoluteMaxCapacity: 42,
            absoluteUsedCapacity: 0,
            numApplications: 0,
            numActiveApplications: 0,
            numPendingApplications: 0,
            state: 'RUNNING',
            resourcesUsed: { memory: 0, vCores: 0 },
            creationMethod: 'static',
          },
        ],
      };

      store.setState({
        schedulerData,
      });

      const result = store.getState().getQueueByPath('root.production.critical.urgent');

      expect(result).not.toBeNull();
      expect(result?.queueName).toBe('urgent');
    });
  });

  describe('getChildQueues', () => {
    it('should return children for root queue', () => {
      const store = createTestStore();
      const schedulerData = createMockSchedulerData();
      store.setState({
        schedulerData,
      });

      const result = store.getState().getChildQueues('root');

      expect(result).toHaveLength(2);
      expect(result[0].queueName).toBe('default');
      expect(result[1].queueName).toBe('production');
    });

    it('should return children for parent queue', () => {
      const store = createTestStore();
      const schedulerData = createMockSchedulerData();
      store.setState({
        schedulerData,
      });

      const result = store.getState().getChildQueues('root.production');

      expect(result).toHaveLength(2);
      expect(result[0].queueName).toBe('critical');
      expect(result[1].queueName).toBe('batch');
    });

    it('should return empty array for leaf queue', () => {
      const store = createTestStore();
      const schedulerData = createMockSchedulerData();
      store.setState({
        schedulerData,
      });

      const result = store.getState().getChildQueues('root.default');

      expect(result).toEqual([]);
    });

    it('should return empty array for non-existent queue', () => {
      const store = createTestStore();
      const schedulerData = createMockSchedulerData();
      store.setState({
        schedulerData,
      });

      const result = store.getState().getChildQueues('root.nonexistent');

      expect(result).toEqual([]);
    });

    it('should handle single child object', () => {
      const store = createTestStore();
      const schedulerData = createMockSchedulerData();
      // Modify production to have single child
      const productionQueue = (schedulerData.queues!.queue as QueueInfo[])[1];
      productionQueue.queues = {
        queue: [
          {
            queueName: 'critical',
            queuePath: 'root.production.critical',
            queueType: 'leaf',
            capacity: 100,
            usedCapacity: 0,
            maxCapacity: 100,
            absoluteCapacity: 60,
            absoluteMaxCapacity: 60,
            absoluteUsedCapacity: 0,
            numApplications: 0,
            numActiveApplications: 0,
            numPendingApplications: 0,
            state: 'RUNNING',
            resourcesUsed: { memory: 0, vCores: 0 },
            creationMethod: 'static',
          },
        ] as QueueInfo[],
      };

      store.setState({
        schedulerData,
      });

      const result = store.getState().getChildQueues('root.production');

      expect(result).toHaveLength(1);
      expect(result[0].queueName).toBe('critical');
    });
  });

  describe('getQueuePartitionCapacities', () => {
    it('should return null when schedulerData is null', () => {
      const store = createTestStore();
      store.setState({
        schedulerData: null,
      });

      const result = store.getState().getQueuePartitionCapacities('root', '');

      expect(result).toBe(null);
    });

    it('should return null when root has no capacities', () => {
      const store = createTestStore();
      const schedulerData = createMockSchedulerData();
      store.setState({
        schedulerData,
      });

      const result = store.getState().getQueuePartitionCapacities('root', 'gpu');

      expect(result).toBe(null);
    });

    it('should return partition data for root queue', () => {
      const store = createTestStore();
      const schedulerData = createMockSchedulerData();
      (schedulerData as any).capacities = {
        queueCapacitiesByPartition: [
          {
            partitionName: '',
            capacity: 100,
            usedCapacity: 10,
            maxCapacity: 100,
          },
          {
            partitionName: 'gpu',
            capacity: 100,
            usedCapacity: 20,
            maxCapacity: 100,
          },
        ],
      };

      store.setState({
        schedulerData,
      });

      const result = store.getState().getQueuePartitionCapacities('root', 'gpu');

      expect(result).not.toBe(null);
      expect(result?.partitionName).toBe('gpu');
      expect(result?.capacity).toBe(100);
      expect(result?.usedCapacity).toBe(20);
    });

    it('should return default partition for root queue when partitionName is empty', () => {
      const store = createTestStore();
      const schedulerData = createMockSchedulerData();
      (schedulerData as any).capacities = {
        queueCapacitiesByPartition: [
          {
            partitionName: '',
            capacity: 100,
            usedCapacity: 10,
            maxCapacity: 100,
          },
        ],
      };

      store.setState({
        schedulerData,
      });

      const result = store.getState().getQueuePartitionCapacities('root', '');

      expect(result).not.toBe(null);
      expect(result?.partitionName).toBe('');
      expect(result?.capacity).toBe(100);
    });

    it('should return partition data for child queue', () => {
      const store = createTestStore();
      const schedulerData = createMockSchedulerData();
      const defaultQueue = (schedulerData.queues!.queue as any[])[0];
      defaultQueue.capacities = {
        queueCapacitiesByPartition: [
          {
            partitionName: '',
            capacity: 40,
            usedCapacity: 5,
            maxCapacity: 40,
          },
          {
            partitionName: 'gpu',
            capacity: 50,
            usedCapacity: 10,
            maxCapacity: 50,
          },
        ],
      };

      store.setState({
        schedulerData,
      });

      const result = store.getState().getQueuePartitionCapacities('root.default', 'gpu');

      expect(result).not.toBe(null);
      expect(result?.partitionName).toBe('gpu');
      expect(result?.capacity).toBe(50);
    });

    it('should return null for non-existent partition', () => {
      const store = createTestStore();
      const schedulerData = createMockSchedulerData();
      (schedulerData as any).capacities = {
        queueCapacitiesByPartition: [
          {
            partitionName: '',
            capacity: 100,
            usedCapacity: 10,
            maxCapacity: 100,
          },
        ],
      };

      store.setState({
        schedulerData,
      });

      const result = store.getState().getQueuePartitionCapacities('root', 'nonexistent');

      expect(result).toBe(null);
    });
  });

  describe('traverseQueueTree', () => {
    it('should visit all queues in the tree', () => {
      const queueInfo: QueueInfo = {
        queueName: 'root',
        queuePath: 'root',
        queueType: 'parent',
        capacity: 100,
        usedCapacity: 0,
        maxCapacity: 100,
        absoluteCapacity: 100,
        absoluteMaxCapacity: 100,
        absoluteUsedCapacity: 0,
        numApplications: 0,
        numActiveApplications: 0,
        numPendingApplications: 0,
        state: 'RUNNING',
        resourcesUsed: { memory: 0, vCores: 0 },
        creationMethod: 'static',
        queues: {
          queue: [
            {
              queueName: 'default',
              queuePath: 'root.default',
              queueType: 'leaf',
              capacity: 50,
              usedCapacity: 0,
              maxCapacity: 50,
              absoluteCapacity: 50,
              absoluteMaxCapacity: 50,
              absoluteUsedCapacity: 0,
              numApplications: 0,
              numActiveApplications: 0,
              numPendingApplications: 0,
              state: 'RUNNING',
              resourcesUsed: { memory: 0, vCores: 0 },
              creationMethod: 'static',
            },
            {
              queueName: 'production',
              queuePath: 'root.production',
              queueType: 'leaf',
              capacity: 50,
              usedCapacity: 0,
              maxCapacity: 50,
              absoluteCapacity: 50,
              absoluteMaxCapacity: 50,
              absoluteUsedCapacity: 0,
              numApplications: 0,
              numActiveApplications: 0,
              numPendingApplications: 0,
              state: 'RUNNING',
              resourcesUsed: { memory: 0, vCores: 0 },
              creationMethod: 'static',
            },
          ],
        },
      };

      const configData = new Map<string, string>();
      const visited: string[] = [];
      const visitor = (queue: QueueInfo & { configured: Record<string, string> }) => {
        visited.push(queue.queuePath);
      };

      traverseQueueTree(queueInfo, configData, visitor);

      expect(visited).toHaveLength(3);
      expect(visited).toContain('root');
      expect(visited).toContain('root.default');
      expect(visited).toContain('root.production');
    });

    it('should extract configured properties for each queue', () => {
      const queueInfo: QueueInfo = {
        queueName: 'default',
        queuePath: 'root.default',
        queueType: 'leaf',
        capacity: 50,
        usedCapacity: 0,
        maxCapacity: 50,
        absoluteCapacity: 50,
        absoluteMaxCapacity: 50,
        absoluteUsedCapacity: 0,
        numApplications: 0,
        numActiveApplications: 0,
        numPendingApplications: 0,
        state: 'RUNNING',
        resourcesUsed: { memory: 0, vCores: 0 },
        creationMethod: 'static',
      };

      const configData = new Map([
        ['yarn.scheduler.capacity.root.default.capacity', '50'],
        ['yarn.scheduler.capacity.root.default.maximum-capacity', '100'],
        ['yarn.scheduler.capacity.root.default.state', 'RUNNING'],
        ['yarn.scheduler.capacity.root.production.capacity', '50'],
      ]);

      let capturedConfig: Record<string, string> | null = null;
      const visitor = (queue: QueueInfo & { configured: Record<string, string> }) => {
        capturedConfig = queue.configured;
      };

      traverseQueueTree(queueInfo, configData, visitor);

      expect(capturedConfig).not.toBe(null);
      expect(capturedConfig!['capacity']).toBe('50');
      expect(capturedConfig!['maximum-capacity']).toBe('100');
      expect(capturedConfig!['state']).toBe('RUNNING');
      expect(capturedConfig!['root.production.capacity']).toBeUndefined();
    });

    it('should handle single child object', () => {
      const queueInfo: QueueInfo = {
        queueName: 'root',
        queuePath: 'root',
        queueType: 'parent',
        capacity: 100,
        usedCapacity: 0,
        maxCapacity: 100,
        absoluteCapacity: 100,
        absoluteMaxCapacity: 100,
        absoluteUsedCapacity: 0,
        numApplications: 0,
        numActiveApplications: 0,
        numPendingApplications: 0,
        state: 'RUNNING',
        resourcesUsed: { memory: 0, vCores: 0 },
        creationMethod: 'static',
        queues: {
          queue: [
            {
              queueName: 'default',
              queuePath: 'root.default',
              queueType: 'leaf',
              capacity: 100,
              usedCapacity: 0,
              maxCapacity: 100,
              absoluteCapacity: 100,
              absoluteMaxCapacity: 100,
              absoluteUsedCapacity: 0,
              numApplications: 0,
              numActiveApplications: 0,
              numPendingApplications: 0,
              state: 'RUNNING',
              resourcesUsed: { memory: 0, vCores: 0 },
              creationMethod: 'static',
            },
          ] as QueueInfo[],
        },
      };

      const configData = new Map<string, string>();
      const visited: string[] = [];
      const visitor = (queue: QueueInfo & { configured: Record<string, string> }) => {
        visited.push(queue.queuePath);
      };

      traverseQueueTree(queueInfo, configData, visitor);

      expect(visited).toHaveLength(2);
      expect(visited).toContain('root');
      expect(visited).toContain('root.default');
    });

    it('should handle queue with no children', () => {
      const queueInfo: QueueInfo = {
        queueName: 'default',
        queuePath: 'root.default',
        queueType: 'leaf',
        capacity: 100,
        usedCapacity: 0,
        maxCapacity: 100,
        absoluteCapacity: 100,
        absoluteMaxCapacity: 100,
        absoluteUsedCapacity: 0,
        numApplications: 0,
        numActiveApplications: 0,
        numPendingApplications: 0,
        state: 'RUNNING',
        resourcesUsed: { memory: 0, vCores: 0 },
        creationMethod: 'static',
      };

      const configData = new Map<string, string>();
      const visited: string[] = [];
      const visitor = (queue: QueueInfo & { configured: Record<string, string> }) => {
        visited.push(queue.queuePath);
      };

      traverseQueueTree(queueInfo, configData, visitor);

      expect(visited).toHaveLength(1);
      expect(visited).toContain('root.default');
    });
  });
});
