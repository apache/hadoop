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


import { describe, it, expect } from 'vitest';
import type { SchedulerData, ConfigData, CapacitySchedulerInfo } from '~/types/index';
import type { NodeLabelsResponse } from '~/types/api';
import { isLeafQueue, isParentQueue } from '~/types/guards';
import { QUEUE_STATES, SCHEDULER_TYPES } from '~/types/constants';

describe('Integration Tests - Complete API Responses', () => {
  describe('Scheduler API Response', () => {
    it('should handle complete scheduler response with nested queues', () => {
      const schedulerResponse: SchedulerData = {
        scheduler: {
          schedulerInfo: {
            type: SCHEDULER_TYPES.CAPACITY,
            capacity: 100,
            usedCapacity: 60,
            maxCapacity: 100,
            queueName: 'root',
            queues: {
              queue: [
                {
                  queueType: 'parent',
                  capacity: 70,
                  usedCapacity: 45.5,
                  maxCapacity: 100,
                  absoluteCapacity: 70,
                  absoluteMaxCapacity: 100,
                  absoluteUsedCapacity: 31.85,
                  numApplications: 150,
                  numActiveApplications: 100,
                  numPendingApplications: 50,
                  queueName: 'production',
                  queuePath: 'root.production',
                  state: QUEUE_STATES.RUNNING,
                  queues: {
                    queue: [
                      {
                        queueType: 'leaf',
                        capacity: 60,
                        usedCapacity: 50,
                        maxCapacity: 100,
                        absoluteCapacity: 42,
                        absoluteMaxCapacity: 70,
                        absoluteUsedCapacity: 21,
                        numApplications: 100,
                        numActiveApplications: 70,
                        numPendingApplications: 30,
                        queueName: 'batch',
                        queuePath: 'root.production.batch',
                        state: QUEUE_STATES.RUNNING,
                      },
                      {
                        queueType: 'leaf',
                        capacity: 40,
                        usedCapacity: 37.5,
                        maxCapacity: 100,
                        absoluteCapacity: 28,
                        absoluteMaxCapacity: 70,
                        absoluteUsedCapacity: 10.5,
                        numApplications: 50,
                        numActiveApplications: 30,
                        numPendingApplications: 20,
                        queueName: 'interactive',
                        queuePath: 'root.production.interactive',
                        state: QUEUE_STATES.RUNNING,
                      },
                    ],
                  },
                },
                {
                  queueType: 'parent',
                  capacity: 20,
                  usedCapacity: 75,
                  maxCapacity: 50,
                  absoluteCapacity: 20,
                  absoluteMaxCapacity: 50,
                  absoluteUsedCapacity: 15,
                  numApplications: 80,
                  numActiveApplications: 50,
                  numPendingApplications: 30,
                  queueName: 'development',
                  queuePath: 'root.development',
                  state: QUEUE_STATES.RUNNING,
                  queues: {
                    queue: [
                      {
                        queueType: 'leaf',
                        capacity: 70,
                        usedCapacity: 71.4,
                        maxCapacity: 100,
                        absoluteCapacity: 14,
                        absoluteMaxCapacity: 20,
                        absoluteUsedCapacity: 10,
                        numApplications: 60,
                        numActiveApplications: 40,
                        numPendingApplications: 20,
                        queueName: 'team1',
                        queuePath: 'root.development.team1',
                        state: QUEUE_STATES.RUNNING,
                      },
                      {
                        queueType: 'leaf',
                        capacity: 30,
                        usedCapacity: 83.3,
                        maxCapacity: 100,
                        absoluteCapacity: 6,
                        absoluteMaxCapacity: 20,
                        absoluteUsedCapacity: 5,
                        numApplications: 20,
                        numActiveApplications: 10,
                        numPendingApplications: 10,
                        queueName: 'team2',
                        queuePath: 'root.development.team2',
                        state: QUEUE_STATES.RUNNING,
                      },
                    ],
                  },
                },
                {
                  queueType: 'leaf',
                  capacity: 10,
                  usedCapacity: 0,
                  maxCapacity: 20,
                  absoluteCapacity: 10,
                  absoluteMaxCapacity: 20,
                  absoluteUsedCapacity: 0,
                  numApplications: 0,
                  numActiveApplications: 0,
                  numPendingApplications: 0,
                  queueName: 'marketing',
                  queuePath: 'root.marketing',
                  state: QUEUE_STATES.STOPPED,
                },
              ],
            },
          },
        },
      };

      const info = schedulerResponse.scheduler.schedulerInfo;
      expect(info.type).toBe(SCHEDULER_TYPES.CAPACITY);
      expect(info.queues.queue).toHaveLength(3);

      // Verify queue hierarchy
      const prodQueue = info.queues.queue[0];
      expect(prodQueue.queuePath).toBe('root.production');
      expect(isParentQueue(prodQueue)).toBe(true);
      expect(prodQueue.queues?.queue).toHaveLength(2);

      const devQueue = info.queues.queue[1];
      expect(isParentQueue(devQueue)).toBe(true);
      expect(devQueue.queues?.queue).toHaveLength(2);

      const marketingQueue = info.queues.queue[2];
      expect(isLeafQueue(marketingQueue)).toBe(true);
      expect(marketingQueue.state).toBe(QUEUE_STATES.STOPPED);

      // Verify capacity calculations
      const rootCapacity = info.queues.queue.reduce((sum, q) => sum + q.capacity, 0);
      expect(rootCapacity).toBe(100);
    });

    it('should handle capacity scheduler info with extended properties', () => {
      const extendedSchedulerInfo: CapacitySchedulerInfo = {
        type: SCHEDULER_TYPES.CAPACITY,
        capacity: 100,
        usedCapacity: 60,
        maxCapacity: 100,
        queueName: 'root',
        queues: {
          queue: [],
        },
        autoCreationEligibility: 'off',
        autoQueueLeafTemplateProperties: {
          capacity: '1',
          'maximum-capacity': '100',
        },
        autoQueueParentTemplateProperties: {},
        autoQueueTemplateProperties: {},
        capacities: {
          queueCapacitiesByPartition: [
            {
              absoluteCapacity: 100,
              absoluteMaxCapacity: 100,
              absoluteUsedCapacity: 60,
              capacity: 100,
              maxCapacity: 100,
              usedCapacity: 60,
              weight: 1,
              normalizedWeight: 1,
              partitionName: '',
              totalResource: {
                memory: 65536,
                vCores: 32,
              },
              usedResource: {
                memory: 39321,
                vCores: 19,
              },
            },
            {
              absoluteCapacity: 100,
              absoluteMaxCapacity: 100,
              absoluteUsedCapacity: 80,
              capacity: 100,
              maxCapacity: 100,
              usedCapacity: 80,
              partitionName: 'gpu',
              totalResource: {
                memory: 32768,
                vCores: 16,
                resourceInformations: {
                  gpu: 8,
                },
              },
              usedResource: {
                memory: 26214,
                vCores: 13,
                resourceInformations: {
                  gpu: 6,
                },
              },
            },
          ],
        },
        health: {
          lastRun: 1234567890,
          operationsInfo: {
            entry: [
              {
                key: 'last-allocation',
                value: {
                  nodeId: 'node1.example.com:8041',
                  containerId: 'container_1234567890_0001_01_000001',
                  queue: 'root.production.batch',
                },
              },
            ],
          },
          lastRunDetails: [
            {
              operation: 'allocations',
              count: 15,
              resources: {
                memory: 4096,
                vCores: 4,
              },
            },
          ],
        },
        mode: 'absolute',
        nodeLabels: ['gpu', 'fpga', 'ssd'],
        orderingPolicyInfo: 'fifo',
        queueAcls: {
          queueAcl: [
            {
              accessType: 'SUBMIT_APP',
              accessControlList: '*',
            },
            {
              accessType: 'ADMINISTER_QUEUE',
              accessControlList: 'admin',
            },
          ],
        },
        queuePath: 'root',
        queuePriority: 0,
        queueType: 'parent',
        defaultNodeLabelExpression: '',
        defaultPriority: 0,
        isAutoCreatedLeafQueue: false,
        maxParallelApps: 2147483647,
        maximumAllocation: {
          memory: 32768,
          vCores: 8,
        },
        minimumAllocation: {
          memory: 1024,
          vCores: 1,
        },
        preemptionDisabled: false,
        priority: 0,
        state: 'RUNNING',
        usedResources: {
          memory: 39321,
          vCores: 19,
        },
        allocatedContainers: 19,
        amResourceLimit: {
          memory: 16384,
          vCores: 4,
        },
        amUsedResource: {
          memory: 4096,
          vCores: 1,
        },
        absoluteUsedCapacity: 60,
        userAmResourceLimit: {
          memory: 16384,
          vCores: 4,
        },
      };

      expect(extendedSchedulerInfo.type).toBe(SCHEDULER_TYPES.CAPACITY);
      expect(extendedSchedulerInfo.capacities?.queueCapacitiesByPartition).toHaveLength(2);
      expect(extendedSchedulerInfo.nodeLabels).toContain('gpu');
      expect(extendedSchedulerInfo.health?.lastRunDetails).toHaveLength(1);
      expect(extendedSchedulerInfo.queueAcls?.queueAcl).toHaveLength(2);
    });
  });

  describe('Configuration API Response', () => {
    it('should handle complete configuration response', () => {
      const configResponse: ConfigData = {
        property: [
          {
            name: 'yarn.scheduler.capacity.root.capacity',
            value: '100',
          },
          {
            name: 'yarn.scheduler.capacity.root.queues',
            value: 'production,development,marketing',
          },
          {
            name: 'yarn.scheduler.capacity.root.production.capacity',
            value: '70',
          },
          {
            name: 'yarn.scheduler.capacity.root.production.maximum-capacity',
            value: '100',
          },
          {
            name: 'yarn.scheduler.capacity.root.production.queues',
            value: 'batch,interactive',
          },
          {
            name: 'yarn.scheduler.capacity.root.production.batch.capacity',
            value: '60',
          },
          {
            name: 'yarn.scheduler.capacity.root.production.interactive.capacity',
            value: '40',
          },
          {
            name: 'yarn.scheduler.capacity.root.development.capacity',
            value: '20',
          },
          {
            name: 'yarn.scheduler.capacity.root.development.maximum-capacity',
            value: '50',
          },
          {
            name: 'yarn.scheduler.capacity.root.marketing.capacity',
            value: '10',
          },
          {
            name: 'yarn.scheduler.capacity.root.marketing.state',
            value: 'STOPPED',
          },
          {
            name: 'yarn.scheduler.capacity.maximum-applications',
            value: '10000',
          },
          {
            name: 'yarn.scheduler.capacity.maximum-am-resource-percent',
            value: '0.1',
          },
          {
            name: 'yarn.scheduler.capacity.resource-calculator',
            value: 'org.apache.hadoop.yarn.util.resource.DominantResourceCalculator',
          },
          {
            name: 'yarn.scheduler.capacity.root.production.accessible-node-labels',
            value: 'gpu,fpga',
          },
          {
            name: 'yarn.scheduler.capacity.root.production.accessible-node-labels.gpu.capacity',
            value: '80',
          },
          {
            name: 'yarn.scheduler.capacity.root.production.accessible-node-labels.gpu.maximum-capacity',
            value: '100',
          },
        ],
      };

      expect(configResponse.property).toHaveLength(17);

      // Verify queue properties
      const queueProperties = configResponse.property.filter(
        (p) => p.name.includes('.root.') && !p.name.includes('accessible-node-labels'),
      );
      expect(queueProperties.length).toBeGreaterThan(0);

      // Verify global properties
      const globalProperties = configResponse.property.filter((p) => !p.name.includes('.root.'));
      expect(globalProperties).toHaveLength(3);

      // Verify node label properties
      const labelProperties = configResponse.property.filter((p) =>
        p.name.includes('accessible-node-labels'),
      );
      expect(labelProperties).toHaveLength(3);

      // Verify capacity values sum to 100
      const rootQueues = ['production', 'development', 'marketing'];
      const capacities = rootQueues.map((queue) => {
        const prop = configResponse.property.find(
          (p) => p.name === `yarn.scheduler.capacity.root.${queue}.capacity`,
        );
        return parseInt(prop?.value || '0');
      });
      expect(capacities.reduce((sum, cap) => sum + cap, 0)).toBe(100);
    });
  });

  describe('Node Labels API Response', () => {
    it('should handle node labels response', () => {
      const nodeLabelsResponse: NodeLabelsResponse = {
        nodeLabelInfo: [
          {
            name: 'gpu',
            exclusivity: true,
            activeNMs: 4,
          },
          {
            name: 'fpga',
            exclusivity: false,
            activeNMs: 2,
          },
          {
            name: 'ssd',
            exclusivity: false,
            activeNMs: 10,
          },
        ],
      };

      expect(Array.isArray(nodeLabelsResponse.nodeLabelInfo)).toBe(true);
      if (Array.isArray(nodeLabelsResponse.nodeLabelInfo)) {
        expect(nodeLabelsResponse.nodeLabelInfo).toHaveLength(3);

        const gpuLabel = nodeLabelsResponse.nodeLabelInfo.find((l) => l.name === 'gpu');
        expect(gpuLabel?.exclusivity).toBe(true);
        expect(gpuLabel?.activeNMs).toBe(4);

        const nonExclusiveLabels = nodeLabelsResponse.nodeLabelInfo.filter((l) => !l.exclusivity);
        expect(nonExclusiveLabels).toHaveLength(2);
      }
    });
  });

  describe('Complete Workflow Integration', () => {
    it('should handle scheduler and config data together', () => {
      const schedulerData: SchedulerData = {
        scheduler: {
          schedulerInfo: {
            type: SCHEDULER_TYPES.CAPACITY,
            capacity: 100,
            usedCapacity: 60,
            maxCapacity: 100,
            queueName: 'root',
            queues: {
              queue: [
                {
                  queueType: 'leaf',
                  capacity: 70,
                  usedCapacity: 45.5,
                  maxCapacity: 100,
                  absoluteCapacity: 70,
                  absoluteMaxCapacity: 100,
                  absoluteUsedCapacity: 31.85,
                  numApplications: 150,
                  numActiveApplications: 100,
                  numPendingApplications: 50,
                  queueName: 'production',
                  queuePath: 'root.production',
                  state: QUEUE_STATES.RUNNING,
                },
              ],
            },
          },
        },
      };

      const configData: ConfigData = {
        property: [
          {
            name: 'yarn.scheduler.capacity.root.production.capacity',
            value: '70',
          },
          {
            name: 'yarn.scheduler.capacity.root.production.user-limit-factor',
            value: '2',
          },
        ],
      };

      // Verify scheduler shows live metrics
      const prodQueue = schedulerData.scheduler.schedulerInfo.queues.queue[0];
      expect(prodQueue.usedCapacity).toBe(45.5);

      // Verify config shows configured values
      const capacityConfig = configData.property.find((p) => p.name.endsWith('.capacity'));
      expect(capacityConfig?.value).toBe('70');

      // Both should agree on the queue's configured capacity
      expect(prodQueue.capacity).toBe(70);
      expect(parseInt(capacityConfig?.value || '0')).toBe(70);
    });
  });
});
