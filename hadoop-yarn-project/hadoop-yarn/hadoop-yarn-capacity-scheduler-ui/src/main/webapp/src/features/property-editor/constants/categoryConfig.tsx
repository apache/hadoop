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


import React from 'react';
import {
  HardDrive,
  Gauge,
  Calendar,
  Shield,
  Cpu,
  Target,
  Package,
  Zap,
  Layers,
  GitBranch,
  Tag,
  AlertTriangle,
} from 'lucide-react';
import type { PropertyCategory } from '~/types';

export const categoryConfig: Record<
  PropertyCategory,
  {
    label: string;
    description: string;
    defaultExpanded: boolean;
    icon: React.ReactElement;
  }
> = {
  resource: {
    label: 'Resource Allocation',
    description: 'Memory, CPU, and other resource allocation settings',
    defaultExpanded: false,
    icon: <HardDrive className="h-4 w-4 text-primary" />,
  },
  scheduling: {
    label: 'Scheduling Policy',
    description: 'Application ordering and priority settings',
    defaultExpanded: false,
    icon: <Calendar className="h-4 w-4 text-primary" />,
  },
  security: {
    label: 'Security & Access Control',
    description: 'User and group access permissions (ACLs)',
    defaultExpanded: false,
    icon: <Shield className="h-4 w-4 text-primary" />,
  },
  core: {
    label: 'Core Settings',
    description: 'Fundamental scheduler configuration and behavior',
    defaultExpanded: true,
    icon: <Cpu className="h-4 w-4 text-primary" />,
  },
  'application-limits': {
    label: 'Application Limits',
    description: 'Global defaults for user limits and application resource constraints',
    defaultExpanded: false,
    icon: <Gauge className="h-4 w-4 text-primary" />,
  },
  placement: {
    label: 'Placement Rules',
    description: 'Queue mapping and application placement configuration',
    defaultExpanded: false,
    icon: <Target className="h-4 w-4 text-primary" />,
  },
  'container-allocation': {
    label: 'Container Allocation',
    description: 'Container assignment, locality, and reservation behavior',
    defaultExpanded: false,
    icon: <Package className="h-4 w-4 text-primary" />,
  },
  'async-scheduling': {
    label: 'Asynchronous Scheduling',
    description: 'Performance optimizations for decoupled scheduling',
    defaultExpanded: false,
    icon: <Zap className="h-4 w-4 text-primary" />,
  },
  capacity: {
    label: 'Capacity Configuration',
    description: 'Queue capacity, maximum capacity, and operational state',
    defaultExpanded: true,
    icon: <Layers className="h-4 w-4 text-primary" />,
  },
  'dynamic-queues': {
    label: 'Dynamic Queue Creation',
    description: 'Auto-creation settings for dynamically created child queues',
    defaultExpanded: false,
    icon: <GitBranch className="h-4 w-4 text-primary" />,
  },
  'node-labels': {
    label: 'Node Labels & Partitions',
    description: 'Node label access control and default partition settings',
    defaultExpanded: false,
    icon: <Tag className="h-4 w-4 text-primary" />,
  },
  preemption: {
    label: 'Preemption Settings',
    description: 'Control preemption behavior for queue containers',
    defaultExpanded: false,
    icon: <AlertTriangle className="h-4 w-4 text-primary" />,
  },
};

export const globalCategoryOrder: PropertyCategory[] = [
  'core',
  'application-limits',
  'placement',
  'container-allocation',
  'async-scheduling',
];

export const queueCategoryOrder: PropertyCategory[] = [
  'capacity',
  'resource',
  'application-limits',
  'dynamic-queues',
  'node-labels',
  'scheduling',
  'security',
  'preemption',
];
