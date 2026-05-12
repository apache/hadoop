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


import type { PropertyDescriptor, PropertyCategory, PropertyType } from '~/types';

// Specify the full config name
export const globalPropertyDefinitions: PropertyDescriptor[] = [
  // Core Settings
  {
    name: 'yarn.scheduler.capacity.legacy-queue-mode.enabled',
    displayName: 'Enable Legacy Queue Mode',
    description:
      'Determines if legacy queue mode is enforced. Default is true. Disabling allows for more flexible capacity configurations with mixing the various capacity modes.',
    type: 'boolean' as PropertyType,
    category: 'core' as PropertyCategory,
    defaultValue: 'true',
    required: false,
  },
  {
    name: 'yarn.scheduler.capacity.resource-calculator',
    displayName: 'Resource Calculator',
    description: 'Class used to calculate resource requirements.',
    type: 'enum' as PropertyType,
    category: 'core' as PropertyCategory,
    defaultValue: 'org.apache.hadoop.yarn.util.resource.DefaultResourceCalculator',
    required: false,
    enumValues: [
      {
        value: 'org.apache.hadoop.yarn.util.resource.DefaultResourceCalculator',
        label: 'Default (Memory Only)',
        description: 'Memory-based calculator suitable for clusters without CPU enforcement.',
      },
      {
        value: 'org.apache.hadoop.yarn.util.resource.DominantResourceCalculator',
        label: 'Dominant Resource',
        description:
          'Considers the dominant resource usage across memory and CPU for fair scheduling.',
      },
    ],
    enumDisplay: 'choiceCard',
  },

  // Application Limits
  {
    name: 'yarn.scheduler.capacity.maximum-applications',
    displayName: 'Maximum Applications (Global)',
    description: 'Maximum number of applications that can be pending and running.',
    type: 'number' as PropertyType,
    category: 'application-limits' as PropertyCategory,
    defaultValue: '10000',
    required: false,
    validationRules: [
      {
        type: 'range',
        message: 'Must be 0 or greater',
        min: 0,
        max: 2147483647,
      },
    ],
  },
  {
    name: 'yarn.scheduler.capacity.global-queue-max-application',
    displayName: 'Max Applications per Queue (Global)',
    description:
      'Global per-queue maximum applications. When set, each queue is capped at this flat value (no capacity scaling). When unset, per-queue limits are calculated from Maximum Applications scaled by queue capacity.',
    type: 'number' as PropertyType,
    category: 'application-limits' as PropertyCategory,
    defaultValue: '',
    required: false,
    validationRules: [
      {
        type: 'custom',
        message: 'Must be a positive integer or empty',
        validator: (value: string) => {
          if (!value.trim()) return true;
          const num = parseFloat(value);
          return !isNaN(num) && Number.isInteger(num) && num > 0;
        },
      },
    ],
  },
  {
    name: 'yarn.scheduler.capacity.application.fail-fast',
    displayName: 'Application Fail Fast',
    description: 'Whether applications should fail fast if submitted to a non-existent queue.',
    type: 'boolean' as PropertyType,
    category: 'application-limits' as PropertyCategory,
    defaultValue: 'false',
    required: false,
  },
  {
    name: 'yarn.scheduler.capacity.max-parallel-apps',
    displayName: 'Max Parallel Apps (Global)',
    description:
      'Global maximum parallel applications across all users. This limits the total number of parallel applications that can run simultaneously in the cluster.',
    type: 'number' as PropertyType,
    category: 'application-limits' as PropertyCategory,
    defaultValue: '2147483647',
    required: false,
    validationRules: [
      {
        type: 'range',
        message: 'Must be between 1 and 2147483647',
        min: 1,
        max: 2147483647,
      },
    ],
  },
  {
    name: 'yarn.scheduler.capacity.user.max-parallel-apps',
    displayName: 'Default Max Parallel Apps per User',
    description: 'Default maximum parallel applications per user.',
    type: 'number' as PropertyType,
    category: 'application-limits' as PropertyCategory,
    defaultValue: '2147483647',
    required: false,
    validationRules: [
      {
        type: 'range',
        message: 'Must be between 1 and 2147483647',
        min: 1,
        max: 2147483647,
      },
    ],
  },
  {
    name: 'yarn.scheduler.capacity.minimum-user-limit-percent',
    displayName: 'Minimum User Limit Percent (Global)',
    description:
      'Global default for minimum percentage of queue resources allocated to a user when there is demand. Default is 100 (no user limits). Can be overridden on a per-queue basis.',
    type: 'number' as PropertyType,
    category: 'application-limits' as PropertyCategory,
    defaultValue: '100',
    required: false,
    validationRules: [
      {
        type: 'range',
        message: 'Must be between 0 and 100',
        min: 0,
        max: 100,
      },
    ],
  },
  {
    name: 'yarn.scheduler.capacity.maximum-am-resource-percent',
    displayName: 'Maximum AM Resource Percent',
    description:
      'Maximum percentage of resources that can be used for Application Masters (expressen in values between 0.0-1.0). Default is 0.1 (10%).',
    type: 'number' as PropertyType,
    category: 'application-limits' as PropertyCategory,
    defaultValue: '0.1',
    required: false,
    validationRules: [
      {
        type: 'range',
        message: 'Must be between 0 and 1',
        min: 0,
        max: 1,
      },
    ],
  },
  {
    name: 'yarn.scheduler.capacity.user-limit-factor',
    displayName: 'User Limit Factor (Global)',
    description:
      "Global default for user limit factor, which controls the max amount of resources a single user can consume as a multiple of the queue's capacity. Default is 1. Set to -1 for unlimited. Can be overridden on a per-queue basis. Note: auto-queue-creation-v2 with weights automatically sets this to -1 on the created queues.",
    type: 'number' as PropertyType,
    category: 'application-limits' as PropertyCategory,
    defaultValue: '1',
    required: false,
    validationRules: [
      {
        type: 'custom',
        message: 'Must be -1 (unlimited) or >= 0',
        validator: (value: string) => {
          const num = parseFloat(value);
          return num === -1 || num >= 0;
        },
      },
    ],
  },

  // Placement Rules
  {
    name: 'yarn.scheduler.capacity.queue-mappings-override.enable',
    displayName: 'Enable Queue Mappings Override',
    description:
      'Sets whether a user-specified target queue is overridden by a matching placement rule or not. Setting it to true will override the target queue with a matching placement rule.',
    type: 'boolean' as PropertyType,
    category: 'placement' as PropertyCategory,
    defaultValue: 'false',
    required: false,
  },
  {
    name: 'yarn.scheduler.capacity.mapping-rule-format',
    displayName: 'Mapping Rule Format',
    description:
      'Format for queue mapping rules. For using the Placement Rule editor on this UI, the JSON format is required.',
    type: 'enum' as PropertyType,
    category: 'placement' as PropertyCategory,
    defaultValue: 'legacy',
    required: false,
    enumValues: [
      {
        value: 'legacy',
        label: 'Legacy',
        description: 'Comma-separated mapping rules compatible with earlier scheduler versions.',
      },
      {
        value: 'json',
        label: 'JSON',
        description: 'Structured JSON mapping rules for complex placement policies.',
      },
    ],
    enumDisplay: 'choiceCard',
  },
  {
    name: 'yarn.scheduler.capacity.queue-mappings',
    displayName: 'Legacy Queue Mappings',
    description:
      'A list of mappings that will be used to assign jobs to queues. The syntax for this list is [u|g]:[name]:[queue_name][,next_mapping]*.',
    type: 'string' as PropertyType,
    category: 'placement' as PropertyCategory,
    defaultValue: '',
    required: false,
    showWhen: [
      ({ getValue }) => getValue('yarn.scheduler.capacity.mapping-rule-format') === 'legacy',
    ],
  },
  {
    name: 'yarn.scheduler.capacity.mapping-rule-json',
    displayName: 'JSON Mapping Rules',
    description: 'Queue mapping rules in JSON format.',
    type: 'string' as PropertyType,
    category: 'placement' as PropertyCategory,
    defaultValue: '',
    required: false,
    showWhen: [
      ({ getValue }) => getValue('yarn.scheduler.capacity.mapping-rule-format') === 'json',
    ],
  },
  {
    name: 'yarn.scheduler.capacity.workflow-priority-mappings-override.enable',
    displayName: 'Enable Workflow Priority Mappings Override',
    description: 'Enable workflow priority mappings override.',
    type: 'boolean' as PropertyType,
    category: 'placement' as PropertyCategory,
    defaultValue: 'false',
    required: false,
  },

  // Container Allocation
  {
    name: 'yarn.scheduler.capacity.node-locality-delay',
    displayName: 'Node Locality Delay',
    description:
      'Number of missed scheduling opportunities after which the scheduler attempts to schedule rack-local containers. Set to -1 to disable node-locality constraint.',
    type: 'number' as PropertyType,
    category: 'container-allocation' as PropertyCategory,
    defaultValue: '40',
    required: false,
    validationRules: [
      {
        type: 'range',
        message: 'Must be between -1 and 1000',
        min: -1,
        max: 1000,
      },
    ],
  },
  {
    name: 'yarn.scheduler.capacity.rack-locality-additional-delay',
    displayName: 'Rack Locality Additional Delay',
    description:
      'Number of additional missed scheduling opportunities over node-locality-delay after which the scheduler attempts to schedule off-switch containers.',
    type: 'number' as PropertyType,
    category: 'container-allocation' as PropertyCategory,
    defaultValue: '-1',
    required: false,
    validationRules: [
      {
        type: 'range',
        message: 'Must be between -1 and 1000',
        min: -1,
        max: 1000,
      },
    ],
  },
  {
    name: 'yarn.scheduler.capacity.per-node-heartbeat.multiple-assignments-enabled',
    displayName: 'Enable Multiple Container Assignments',
    description: 'Allow multiple container assignments per node heartbeat.',
    type: 'boolean' as PropertyType,
    category: 'container-allocation' as PropertyCategory,
    defaultValue: 'true',
    required: false,
  },
  {
    name: 'yarn.scheduler.capacity.per-node-heartbeat.maximum-container-assignments',
    displayName: 'Max Container Assignments per Heartbeat',
    description:
      'If multiple-assignments-enabled is true, this property controls the maximum containers assigned per heartbeat (-1 = unlimited).',
    type: 'number' as PropertyType,
    category: 'container-allocation' as PropertyCategory,
    defaultValue: '100',
    required: false,
    validationRules: [
      {
        type: 'custom',
        message: 'Must be -1 (unlimited) or greater than 0',
        validator: (value: string) => {
          const num = parseFloat(value);
          return num === -1 || num > 0;
        },
      },
    ],
  },
  {
    name: 'yarn.scheduler.capacity.per-node-heartbeat.maximum-offswitch-assignments',
    displayName: 'Maximum Off-switch Assignments Per Heartbeat',
    description:
      'If multiple-assignments-enabled is true, this property controls the number of OFF_SWITCH assignments allowed during a node heartbeat.',
    type: 'number' as PropertyType,
    category: 'container-allocation' as PropertyCategory,
    defaultValue: '1',
    required: false,
    validationRules: [
      {
        type: 'range',
        message: 'Must be 1 or greater',
        min: 1,
        max: 2147483647,
      },
    ],
  },

  {
    name: 'yarn.scheduler.capacity.reservations-continue-look-all-nodes',
    displayName: 'Continue Looking All Nodes for Reservations',
    description:
      'Controls whether the scheduler continues to search for available nodes for a reservation even if the first node it considered is reserved. When set to true, it enables the scheduler to continue searching other nodes, which can improve reservation success rates for large resource requests by preventing a single reserved node from blocking all potential reservations.',
    type: 'boolean' as PropertyType,
    category: 'container-allocation' as PropertyCategory,
    defaultValue: 'true',
    required: false,
  },
  {
    name: 'yarn.scheduler.capacity.skip-allocate-on-nodes-with-reserved-containers',
    displayName: 'Skip Allocate on Nodes with Reserved Containers',
    description: 'Skip trying to allocate on nodes which have reserved containers.',
    type: 'boolean' as PropertyType,
    category: 'container-allocation' as PropertyCategory,
    defaultValue: 'false',
    required: false,
  },

  // Asynchronous Scheduling
  {
    name: 'yarn.scheduler.capacity.schedule-asynchronously.enable',
    displayName: 'Enable Asynchronous Scheduling',
    description:
      'Enabling this decouples container assigments from node heartbeats to improve performance.',
    type: 'boolean' as PropertyType,
    category: 'async-scheduling' as PropertyCategory,
    defaultValue: 'false',
    required: false,
  },
  {
    name: 'yarn.scheduler.capacity.schedule-asynchronously.scheduling-interval-ms',
    displayName: 'Async Scheduling Interval (ms)',
    description: 'Scheduling interval for synchronous Scheduling scheduling.',
    type: 'number' as PropertyType,
    category: 'async-scheduling' as PropertyCategory,
    defaultValue: '5',
    required: false,
    validationRules: [
      {
        type: 'range',
        message: 'Must be between 1 and 1000',
        min: 1,
        max: 1000,
      },
    ],
  },
  {
    name: 'yarn.scheduler.capacity.schedule-asynchronously.maximum-threads',
    displayName: 'Async Scheduling Maximum Threads',
    description: 'Maximum number of threads for asynchronous scheduling.',
    type: 'number' as PropertyType,
    category: 'async-scheduling' as PropertyCategory,
    defaultValue: '1',
    required: false,
    validationRules: [
      {
        type: 'range',
        message: 'Must be 1 or greater',
        min: 1,
        max: 2147483647,
      },
    ],
  },
  {
    name: 'yarn.scheduler.capacity.schedule-asynchronously.maximum-pending-backlogs',
    displayName: 'Async Scheduling Maximum Pending Backlogs',
    description: 'Maximum number of pending backlogs for asynchronous scheduling.',
    type: 'number' as PropertyType,
    category: 'async-scheduling' as PropertyCategory,
    defaultValue: '100',
    required: false,
    validationRules: [
      {
        type: 'range',
        message: 'Must be 1 or greater',
        min: 1,
        max: 2147483647,
      },
    ],
  },
];
