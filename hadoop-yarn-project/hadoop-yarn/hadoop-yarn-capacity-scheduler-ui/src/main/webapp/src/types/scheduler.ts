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


import type { QueueInfo } from './queue';
import type { ResourceInfo } from './resource';

export type SchedulerData = {
  scheduler: {
    schedulerInfo: SchedulerInfo;
  };
};

export type SchedulerInfo = {
  type: string;
  capacity: number;
  usedCapacity: number;
  maxCapacity: number;
  queueName: string;
  queues: {
    queue: QueueInfo[];
  };
};

export type QueueCapacitiesByPartition = {
  absoluteCapacity: number;
  absoluteMaxCapacity: number;
  absoluteUsedCapacity: number;
  capacity: number;
  maxCapacity?: number;
  usedCapacity: number;
  weight?: number;
  normalizedWeight?: number;
  configuredMaxResource?: ResourceInfo;
  configuredMinResource?: ResourceInfo;
  effectiveMaxResource?: ResourceInfo;
  effectiveMinResource?: ResourceInfo;
  maximumAllocation?: ResourceInfo;
  minimumAllocation?: ResourceInfo;
  netPending?: ResourceInfo;
  partitionName?: string;
  pendingResource?: ResourceInfo;
  reservedResource?: ResourceInfo;
  totalResource?: ResourceInfo;
  usedResource?: ResourceInfo;
};

export type HealthInfo = {
  lastRun?: number;
  operationsInfo?: {
    entry: Array<{
      key: string;
      value: {
        nodeId?: string;
        containerId?: string;
        queue?: string;
      };
    }>;
  };
  lastRunDetails?: Array<{
    operation: string;
    count: number;
    resources?: ResourceInfo;
  }>;
};

export type QueueAcl = {
  accessType: string;
  accessControlList: string;
};

export type CapacitySchedulerInfo = SchedulerInfo & {
  type: 'capacityScheduler';
  autoCreationEligibility?: string;
  autoQueueLeafTemplateProperties?: Record<string, string>;
  autoQueueParentTemplateProperties?: Record<string, string>;
  autoQueueTemplateProperties?: Record<string, string>;
  capacities?: {
    queueCapacitiesByPartition: QueueCapacitiesByPartition[];
  };
  health?: HealthInfo;
  mode?: string;
  nodeLabels?: string[];
  orderingPolicyInfo?: string;
  queueAcls?: {
    queueAcl: QueueAcl[];
  };
  queuePath?: string;
  queuePriority?: number;
  queueType?: string;
  defaultNodeLabelExpression?: string;
  defaultPriority?: number;
  isAutoCreatedLeafQueue?: boolean;
  maxParallelApps?: number;
  maximumAllocation?: ResourceInfo;
  minimumAllocation?: ResourceInfo;
  preemptionDisabled?: boolean;
  priority?: number;
  state?: string;
  usedResources?: ResourceInfo;
  allocatedContainers?: number;
  amResourceLimit?: ResourceInfo;
  amUsedResource?: ResourceInfo;
  absoluteUsedCapacity?: number;
  userAmResourceLimit?: ResourceInfo;
};
