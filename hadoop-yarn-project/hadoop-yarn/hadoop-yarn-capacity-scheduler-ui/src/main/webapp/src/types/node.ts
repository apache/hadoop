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


import type { ResourceInfo } from './resource';

export type NodeState = 'RUNNING' | 'SHUTDOWN' | 'UNHEALTHY' | 'REBOOTED' | 'LOST' | 'NEW';

export type NodeInfo = {
  id: string;
  rack: string;
  state: NodeState;
  nodeHostName: string;
  nodeHTTPAddress: string;
  lastHealthUpdate: number;
  version: string;
  healthReport: string;
  numContainers: number;
  usedMemoryMB: number;
  availMemoryMB: number;
  usedVirtualCores: number;
  availableVirtualCores: number;
  numRunningOpportContainers: number;
  usedMemoryOpportGB: number;
  usedVirtualCoresOpport: number;
  numQueuedContainers: number;
  nodeLabels: string[];
  allocationTags: Record<string, unknown>;
  resourceUtilization?: {
    nodePhysicalMemoryMB: number;
    nodeVirtualMemoryMB: number;
    nodeCPUUsage: number;
    aggregatedContainersPhysicalMemoryMB: number;
    aggregatedContainersVirtualMemoryMB: number;
    containersCPUUsage: number;
  };
  usedResource: ResourceInfo;
  availableResource: ResourceInfo;
  nodeAttributesInfo: Record<string, unknown>;
};

export type NodesResponse = {
  nodes: {
    node: NodeInfo[];
  };
};

export type NodeToLabelMapping = {
  nodeId: string;
  nodeLabels: string[];
};
