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
import type { SchedulerInfo, SchedulerData, CapacitySchedulerInfo } from '~/types';

describe('SchedulerData interface', () => {
  it('should accept valid scheduler data response', () => {
    const schedulerData: SchedulerData = {
      scheduler: {
        schedulerInfo: {
          type: 'capacityScheduler',
          capacity: 100,
          usedCapacity: 60,
          maxCapacity: 100,
          queueName: 'root',
          queues: {
            queue: [],
          },
        },
      },
    };

    expect(schedulerData.scheduler.schedulerInfo.type).toBe('capacityScheduler');
    expect(schedulerData.scheduler.schedulerInfo.capacity).toBe(100);
  });
});

describe('SchedulerInfo interface', () => {
  it('should handle capacity scheduler info', () => {
    const schedulerInfo: SchedulerInfo = {
      type: 'capacityScheduler',
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
            state: 'RUNNING',
          },
        ],
      },
    };

    expect(schedulerInfo.queues.queue).toHaveLength(1);
    expect(schedulerInfo.queues.queue[0].queueName).toBe('production');
  });
});

describe('CapacitySchedulerInfo interface', () => {
  it('should accept complete capacity scheduler info', () => {
    const capacitySchedulerInfo: CapacitySchedulerInfo = {
      type: 'capacityScheduler',
      capacity: 100,
      usedCapacity: 60,
      maxCapacity: 100,
      queueName: 'root',
      queues: {
        queue: [],
      },
      autoCreationEligibility: 'off',
      autoQueueLeafTemplateProperties: {},
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
            configuredMaxResource: {
              memory: 0,
              vCores: 0,
            },
            configuredMinResource: {
              memory: 0,
              vCores: 0,
            },
            effectiveMaxResource: {
              memory: 0,
              vCores: 0,
            },
            effectiveMinResource: {
              memory: 0,
              vCores: 0,
            },
            maximumAllocation: {
              memory: 32768,
              vCores: 8,
            },
            minimumAllocation: {
              memory: 1024,
              vCores: 1,
            },
            netPending: {
              memory: 0,
              vCores: 0,
            },
            partitionName: '',
            pendingResource: {
              memory: 0,
              vCores: 0,
            },
            reservedResource: {
              memory: 0,
              vCores: 0,
            },
            totalResource: {
              memory: 65536,
              vCores: 32,
            },
            usedResource: {
              memory: 39321,
              vCores: 19,
            },
          },
        ],
      },
      health: {
        lastRun: 1234567890,
        operationsInfo: {
          entry: [
            {
              key: 'last-release',
              value: {
                nodeId: 'node1',
                containerId: 'container_1',
                queue: 'root.production',
              },
            },
          ],
        },
        lastRunDetails: [
          {
            operation: 'releases',
            count: 5,
            resources: {
              memory: 2048,
              vCores: 2,
            },
          },
        ],
      },
      mode: 'absolute',
      nodeLabels: ['gpu', 'fpga'],
      orderingPolicyInfo: 'fifo',
      queueAcls: {
        queueAcl: [
          {
            accessType: 'SUBMIT_APP',
            accessControlList: 'user1,user2',
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

    expect(capacitySchedulerInfo.type).toBe('capacityScheduler');
    expect(capacitySchedulerInfo.autoCreationEligibility).toBe('off');
    expect(capacitySchedulerInfo.mode).toBe('absolute');
    expect(capacitySchedulerInfo.nodeLabels).toContain('gpu');
    expect(capacitySchedulerInfo.queueAcls?.queueAcl).toHaveLength(1);
  });

  it('should handle scheduler info with node label partitions', () => {
    const schedulerWithLabels: CapacitySchedulerInfo = {
      type: 'capacityScheduler',
      capacity: 100,
      usedCapacity: 60,
      maxCapacity: 100,
      queueName: 'root',
      queues: {
        queue: [],
      },
      capacities: {
        queueCapacitiesByPartition: [
          {
            absoluteCapacity: 100,
            absoluteMaxCapacity: 100,
            absoluteUsedCapacity: 60,
            capacity: 100,
            maxCapacity: 100,
            usedCapacity: 60,
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
            },
            usedResource: {
              memory: 26214,
              vCores: 13,
            },
          },
        ],
      },
    };

    const partitions = schedulerWithLabels.capacities?.queueCapacitiesByPartition;
    expect(partitions).toHaveLength(2);
    expect(partitions?.[0].partitionName).toBe('');
    expect(partitions?.[1].partitionName).toBe('gpu');
    expect(partitions?.[1].usedCapacity).toBe(80);
  });
});
