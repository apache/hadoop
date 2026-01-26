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
import { runFieldValidation, type ValidationContext } from '~/config/validation-rules';
import { AUTO_CREATION_PROPS } from '~/types/constants/auto-creation';
import type { SchedulerInfo } from '~/types';

function createMockSchedulerData(): SchedulerInfo {
  return {
    type: 'capacityScheduler',
    queueName: 'root',
    capacity: 100,
    usedCapacity: 0,
    maxCapacity: 100,
    queues: {
      queue: [
        {
          queueName: 'production',
          queuePath: 'root.production',
          capacity: 60,
          usedCapacity: 0,
          maxCapacity: 100,
          absoluteCapacity: 60,
          absoluteMaxCapacity: 100,
          absoluteUsedCapacity: 0,
          numApplications: 0,
          numActiveApplications: 0,
          numPendingApplications: 0,
          queueType: 'parent',
          state: 'RUNNING',
          queues: {
            queue: [
              {
                queueName: 'critical',
                queuePath: 'root.production.critical',
                capacity: 50,
                usedCapacity: 0,
                maxCapacity: 100,
                absoluteCapacity: 30,
                absoluteMaxCapacity: 60,
                absoluteUsedCapacity: 0,
                numApplications: 0,
                numActiveApplications: 0,
                numPendingApplications: 0,
                queueType: 'leaf',
                state: 'RUNNING',
              },
            ],
          },
        },
        {
          queueName: 'development',
          queuePath: 'root.development',
          capacity: 40,
          usedCapacity: 0,
          maxCapacity: 100,
          absoluteCapacity: 40,
          absoluteMaxCapacity: 100,
          absoluteUsedCapacity: 0,
          numApplications: 0,
          numActiveApplications: 0,
          numPendingApplications: 0,
          queueType: 'leaf',
          state: 'RUNNING',
        },
      ],
    },
  };
}

describe('PARENT_CHILD_CAPACITY_MODE validation rule', () => {
  it('should pass when parent uses percentage and child uses percentage', () => {
    const config = new Map([
      ['yarn.scheduler.capacity.root.capacity', '100'],
      ['yarn.scheduler.capacity.root.production.capacity', '60'],
    ]);

    const context: ValidationContext = {
      queuePath: 'root.production',
      fieldName: 'capacity',
      fieldValue: '60',
      config,
      schedulerData: createMockSchedulerData(),
      stagedChanges: [],
      legacyModeEnabled: true,
    };

    const issues = runFieldValidation(context);
    const parentChildModeIssues = issues.filter(
      (issue) => issue.rule === 'parent-child-capacity-mode',
    );

    expect(parentChildModeIssues).toHaveLength(0);
  });

  it('should pass when parent uses absolute and child uses absolute', () => {
    const config = new Map([
      ['yarn.scheduler.capacity.root.capacity', '100'],
      ['yarn.scheduler.capacity.root.production.capacity', '[memory=8192,vcores=8]'],
      ['yarn.scheduler.capacity.root.production.critical.capacity', '[memory=4096,vcores=4]'],
    ]);

    const context: ValidationContext = {
      queuePath: 'root.production.critical',
      fieldName: 'capacity',
      fieldValue: '[memory=4096,vcores=4]',
      config,
      schedulerData: createMockSchedulerData(),
      stagedChanges: [],
      legacyModeEnabled: true,
    };

    const issues = runFieldValidation(context);
    const parentChildModeIssues = issues.filter(
      (issue) => issue.rule === 'parent-child-capacity-mode',
    );

    expect(parentChildModeIssues).toHaveLength(0);
  });

  it('should fail when parent uses absolute and child uses percentage', () => {
    const config = new Map([
      ['yarn.scheduler.capacity.root.capacity', '100'],
      ['yarn.scheduler.capacity.root.production.capacity', '[memory=8192,vcores=8]'],
      ['yarn.scheduler.capacity.root.production.critical.capacity', '50'],
    ]);

    const context: ValidationContext = {
      queuePath: 'root.production.critical',
      fieldName: 'capacity',
      fieldValue: '50',
      config,
      schedulerData: createMockSchedulerData(),
      stagedChanges: [],
      legacyModeEnabled: true,
    };

    const issues = runFieldValidation(context);
    const parentChildModeIssues = issues.filter(
      (issue) => issue.rule === 'parent-child-capacity-mode',
    );

    expect(parentChildModeIssues).toHaveLength(1);
    expect(parentChildModeIssues[0]).toMatchObject({
      queuePath: 'root.production.critical',
      field: 'capacity',
      severity: 'error',
      rule: 'parent-child-capacity-mode',
    });
    expect(parentChildModeIssues[0].message).toContain('Parent queue uses absolute resources');
  });

  it('should fail when parent uses absolute and child uses weight', () => {
    const config = new Map([
      ['yarn.scheduler.capacity.root.capacity', '100'],
      ['yarn.scheduler.capacity.root.production.capacity', '[memory=8192,vcores=8]'],
      ['yarn.scheduler.capacity.root.production.critical.capacity', '2w'],
    ]);

    const context: ValidationContext = {
      queuePath: 'root.production.critical',
      fieldName: 'capacity',
      fieldValue: '2w',
      config,
      schedulerData: createMockSchedulerData(),
      stagedChanges: [],
      legacyModeEnabled: true,
    };

    const issues = runFieldValidation(context);
    const parentChildModeIssues = issues.filter(
      (issue) => issue.rule === 'parent-child-capacity-mode',
    );

    expect(parentChildModeIssues).toHaveLength(1);
    expect(parentChildModeIssues[0]).toMatchObject({
      queuePath: 'root.production.critical',
      field: 'capacity',
      severity: 'error',
      rule: 'parent-child-capacity-mode',
    });
  });

  it('should not run in flexible mode', () => {
    const config = new Map([
      ['yarn.scheduler.capacity.root.capacity', '100'],
      ['yarn.scheduler.capacity.root.production.capacity', '[memory=8192,vcores=8]'],
      ['yarn.scheduler.capacity.root.production.critical.capacity', '50'],
    ]);

    const context: ValidationContext = {
      queuePath: 'root.production.critical',
      fieldName: 'capacity',
      fieldValue: '50',
      config,
      schedulerData: createMockSchedulerData(),
      stagedChanges: [],
      legacyModeEnabled: false,
    };

    const issues = runFieldValidation(context);
    const parentChildModeIssues = issues.filter(
      (issue) => issue.rule === 'parent-child-capacity-mode',
    );

    expect(parentChildModeIssues).toHaveLength(0);
  });

  it('should not apply to root queue', () => {
    const config = new Map([['yarn.scheduler.capacity.root.capacity', '100']]);

    const context: ValidationContext = {
      queuePath: 'root',
      fieldName: 'capacity',
      fieldValue: '100',
      config,
      schedulerData: createMockSchedulerData(),
      stagedChanges: [],
      legacyModeEnabled: true,
    };

    const issues = runFieldValidation(context);
    const parentChildModeIssues = issues.filter(
      (issue) => issue.rule === 'parent-child-capacity-mode',
    );

    expect(parentChildModeIssues).toHaveLength(0);
  });

  it('should allow root children to use absolute resources even when root uses percentage mode', () => {
    const config = new Map([
      ['yarn.scheduler.capacity.root.capacity', '100'],
      ['yarn.scheduler.capacity.root.production.capacity', '[memory=8192,vcores=8]'],
    ]);

    const context: ValidationContext = {
      queuePath: 'root.production',
      fieldName: 'capacity',
      fieldValue: '[memory=8192,vcores=8]',
      config,
      schedulerData: createMockSchedulerData(),
      stagedChanges: [],
      legacyModeEnabled: true,
    };

    const issues = runFieldValidation(context);
    const parentChildModeIssues = issues.filter(
      (issue) => issue.rule === 'parent-child-capacity-mode',
    );

    // Root's children should be allowed to use any capacity mode
    expect(parentChildModeIssues).toHaveLength(0);
  });

  it('should not produce error when validating existing queue with unchanged capacity', () => {
    const config = new Map([
      ['yarn.scheduler.capacity.root.capacity', '100'],
      ['yarn.scheduler.capacity.root.production.capacity', '[memory=8192,vcores=8]'],
      ['yarn.scheduler.capacity.root.production.critical.capacity', '50'],
    ]);

    // Queue already has percentage capacity, not changing it
    const context: ValidationContext = {
      queuePath: 'root.production.critical',
      fieldName: 'capacity',
      fieldValue: '50', // Same as current value
      config,
      schedulerData: createMockSchedulerData(),
      stagedChanges: [],
      legacyModeEnabled: true,
    };

    const issues = runFieldValidation(context);
    const parentChildModeIssues = issues.filter(
      (issue) => issue.rule === 'parent-child-capacity-mode',
    );

    // Should still produce error because it's invalid configuration
    expect(parentChildModeIssues).toHaveLength(1);
  });

  it('should fail when parent uses percentage and child uses absolute (reverse direction)', () => {
    const config = new Map([
      ['yarn.scheduler.capacity.root.capacity', '100'],
      ['yarn.scheduler.capacity.root.production.capacity', '60'],
      ['yarn.scheduler.capacity.root.production.critical.capacity', '[memory=4096,vcores=4]'],
    ]);

    const context: ValidationContext = {
      queuePath: 'root.production.critical',
      fieldName: 'capacity',
      fieldValue: '[memory=4096,vcores=4]',
      config,
      schedulerData: createMockSchedulerData(),
      stagedChanges: [],
      legacyModeEnabled: true,
    };

    const issues = runFieldValidation(context);
    const parentChildModeIssues = issues.filter(
      (issue) => issue.rule === 'parent-child-capacity-mode',
    );

    expect(parentChildModeIssues).toHaveLength(1);
    expect(parentChildModeIssues[0]).toMatchObject({
      queuePath: 'root.production.critical',
      field: 'capacity',
      severity: 'error',
      rule: 'parent-child-capacity-mode',
    });
    expect(parentChildModeIssues[0].message).toContain('Parent queue uses percentage mode');
    expect(parentChildModeIssues[0].message).toContain('cannot use absolute resources');
  });

  it('should fail when parent uses weight and child uses absolute (reverse direction)', () => {
    const config = new Map([
      ['yarn.scheduler.capacity.root.capacity', '100'],
      ['yarn.scheduler.capacity.root.production.capacity', '2w'],
      ['yarn.scheduler.capacity.root.production.critical.capacity', '[memory=4096,vcores=4]'],
    ]);

    const context: ValidationContext = {
      queuePath: 'root.production.critical',
      fieldName: 'capacity',
      fieldValue: '[memory=4096,vcores=4]',
      config,
      schedulerData: createMockSchedulerData(),
      stagedChanges: [],
      legacyModeEnabled: true,
    };

    const issues = runFieldValidation(context);
    const parentChildModeIssues = issues.filter(
      (issue) => issue.rule === 'parent-child-capacity-mode',
    );

    expect(parentChildModeIssues).toHaveLength(1);
    expect(parentChildModeIssues[0]).toMatchObject({
      queuePath: 'root.production.critical',
      field: 'capacity',
      severity: 'error',
      rule: 'parent-child-capacity-mode',
    });
    expect(parentChildModeIssues[0].message).toContain('Parent queue uses weight mode');
    expect(parentChildModeIssues[0].message).toContain('cannot use absolute resources');
  });

  it('should pass when both parent and child use weight mode', () => {
    const config = new Map([
      ['yarn.scheduler.capacity.root.capacity', '100'],
      ['yarn.scheduler.capacity.root.production.capacity', '2w'],
      ['yarn.scheduler.capacity.root.production.critical.capacity', '1w'],
    ]);

    const context: ValidationContext = {
      queuePath: 'root.production.critical',
      fieldName: 'capacity',
      fieldValue: '1w',
      config,
      schedulerData: createMockSchedulerData(),
      stagedChanges: [],
      legacyModeEnabled: true,
    };

    const issues = runFieldValidation(context);
    const parentChildModeIssues = issues.filter(
      (issue) => issue.rule === 'parent-child-capacity-mode',
    );

    expect(parentChildModeIssues).toHaveLength(0);
  });
});

describe('WEIGHT_MODE_TRANSITION_FLEXIBLE_AQC validation rule', () => {
  it('should pass when changing weight to percentage without flexible AQC', () => {
    const config = new Map([
      ['yarn.scheduler.capacity.root.production.capacity', '2w'],
      ['yarn.scheduler.capacity.root.production.auto-queue-creation-v2.enabled', 'false'],
    ]);

    const context: ValidationContext = {
      queuePath: 'root.production',
      fieldName: 'capacity',
      fieldValue: '60',
      config,
      schedulerData: createMockSchedulerData(),
      stagedChanges: [],
      legacyModeEnabled: true,
    };

    const issues = runFieldValidation(context);
    const transitionIssues = issues.filter(
      (issue) => issue.rule === 'weight-mode-transition-flexible-aqc',
    );

    expect(transitionIssues).toHaveLength(0);
  });

  it('should pass when changing weight to absolute without flexible AQC', () => {
    const config = new Map([
      ['yarn.scheduler.capacity.root.production.capacity', '2w'],
      ['yarn.scheduler.capacity.root.production.auto-queue-creation-v2.enabled', 'false'],
    ]);

    const context: ValidationContext = {
      queuePath: 'root.production',
      fieldName: 'capacity',
      fieldValue: '[memory=8192,vcores=8]',
      config,
      schedulerData: createMockSchedulerData(),
      stagedChanges: [],
      legacyModeEnabled: true,
    };

    const issues = runFieldValidation(context);
    const transitionIssues = issues.filter(
      (issue) => issue.rule === 'weight-mode-transition-flexible-aqc',
    );

    expect(transitionIssues).toHaveLength(0);
  });

  it('should fail when changing weight to percentage with flexible AQC enabled', () => {
    const config = new Map([
      ['yarn.scheduler.capacity.root.production.capacity', '2w'],
      ['yarn.scheduler.capacity.root.production.auto-queue-creation-v2.enabled', 'true'],
    ]);

    const context: ValidationContext = {
      queuePath: 'root.production',
      fieldName: 'capacity',
      fieldValue: '60',
      config,
      schedulerData: createMockSchedulerData(),
      stagedChanges: [],
      legacyModeEnabled: true,
    };

    const issues = runFieldValidation(context);
    const transitionIssues = issues.filter(
      (issue) => issue.rule === 'weight-mode-transition-flexible-aqc',
    );

    expect(transitionIssues).toHaveLength(1);
    expect(transitionIssues[0]).toMatchObject({
      queuePath: 'root.production',
      field: 'capacity',
      severity: 'error',
      rule: 'weight-mode-transition-flexible-aqc',
    });
    expect(transitionIssues[0].message).toContain('Cannot change from weight mode to percentage');
    expect(transitionIssues[0].message).toContain(AUTO_CREATION_PROPS.FLEXIBLE_ENABLED);
  });

  it('should fail when changing weight to absolute with flexible AQC enabled', () => {
    const config = new Map([
      ['yarn.scheduler.capacity.root.production.capacity', '2w'],
      ['yarn.scheduler.capacity.root.production.auto-queue-creation-v2.enabled', 'true'],
    ]);

    const context: ValidationContext = {
      queuePath: 'root.production',
      fieldName: 'capacity',
      fieldValue: '[memory=8192,vcores=8]',
      config,
      schedulerData: createMockSchedulerData(),
      stagedChanges: [],
      legacyModeEnabled: true,
    };

    const issues = runFieldValidation(context);
    const transitionIssues = issues.filter(
      (issue) => issue.rule === 'weight-mode-transition-flexible-aqc',
    );

    expect(transitionIssues).toHaveLength(1);
    expect(transitionIssues[0]).toMatchObject({
      queuePath: 'root.production',
      field: 'capacity',
      severity: 'error',
      rule: 'weight-mode-transition-flexible-aqc',
    });
    expect(transitionIssues[0].message).toContain('Cannot change from weight mode to absolute');
  });

  it('should pass when changing percentage to weight (not transitioning FROM weight)', () => {
    const config = new Map([
      ['yarn.scheduler.capacity.root.production.capacity', '60'],
      ['yarn.scheduler.capacity.root.production.auto-queue-creation-v2.enabled', 'true'],
    ]);

    const context: ValidationContext = {
      queuePath: 'root.production',
      fieldName: 'capacity',
      fieldValue: '2w',
      config,
      schedulerData: createMockSchedulerData(),
      stagedChanges: [],
      legacyModeEnabled: true,
    };

    const issues = runFieldValidation(context);
    const transitionIssues = issues.filter(
      (issue) => issue.rule === 'weight-mode-transition-flexible-aqc',
    );

    expect(transitionIssues).toHaveLength(0);
  });

  it('should pass when staying in weight mode', () => {
    const config = new Map([
      ['yarn.scheduler.capacity.root.production.capacity', '2w'],
      ['yarn.scheduler.capacity.root.production.auto-queue-creation-v2.enabled', 'true'],
    ]);

    const context: ValidationContext = {
      queuePath: 'root.production',
      fieldName: 'capacity',
      fieldValue: '3w',
      config,
      schedulerData: createMockSchedulerData(),
      stagedChanges: [],
      legacyModeEnabled: true,
    };

    const issues = runFieldValidation(context);
    const transitionIssues = issues.filter(
      (issue) => issue.rule === 'weight-mode-transition-flexible-aqc',
    );

    expect(transitionIssues).toHaveLength(0);
  });

  it('should not run in flexible mode', () => {
    const config = new Map([
      ['yarn.scheduler.capacity.root.production.capacity', '2w'],
      ['yarn.scheduler.capacity.root.production.auto-queue-creation-v2.enabled', 'true'],
    ]);

    const context: ValidationContext = {
      queuePath: 'root.production',
      fieldName: 'capacity',
      fieldValue: '60',
      config,
      schedulerData: createMockSchedulerData(),
      stagedChanges: [],
      legacyModeEnabled: false,
    };

    const issues = runFieldValidation(context);
    const transitionIssues = issues.filter(
      (issue) => issue.rule === 'weight-mode-transition-flexible-aqc',
    );

    expect(transitionIssues).toHaveLength(0);
  });
});

describe('CAPACITY_SUM validation rule', () => {
  it('should show warning when adding single child to parent with no existing children', () => {
    // Create parent queue with no children
    const schedulerData: SchedulerInfo = {
      type: 'capacityScheduler',
      queueName: 'root',
      capacity: 100,
      usedCapacity: 0,
      maxCapacity: 100,
      queues: {
        queue: [
          {
            queueName: 'production',
            queuePath: 'root.production',
            capacity: 100,
            usedCapacity: 0,
            maxCapacity: 100,
            absoluteCapacity: 100,
            absoluteMaxCapacity: 100,
            absoluteUsedCapacity: 0,
            numApplications: 0,
            numActiveApplications: 0,
            numPendingApplications: 0,
            queueType: 'parent',
            state: 'RUNNING',
            // No children yet!
          },
        ],
      },
    };

    const config = new Map([
      ['yarn.scheduler.capacity.root.capacity', '100'],
      ['yarn.scheduler.capacity.root.production.capacity', '100'],
      ['yarn.scheduler.capacity.root.production.team-a.capacity', '10'],
    ]);

    const context: ValidationContext = {
      queuePath: 'root.production', // Validating parent queue
      fieldName: 'capacity',
      fieldValue: '100',
      config,
      schedulerData,
      stagedChanges: [
        {
          id: '1',
          type: 'add',
          queuePath: 'root.production.team-a',
          property: 'capacity',
          oldValue: undefined,
          newValue: '10',
          timestamp: Date.now(),
        },
      ],
      legacyModeEnabled: true,
    };

    const issues = runFieldValidation(context);
    const capacitySumIssues = issues.filter((issue) => issue.rule === 'child-capacity-sum');

    expect(capacitySumIssues).toHaveLength(1);
    expect(capacitySumIssues[0]).toMatchObject({
      queuePath: 'root.production',
      field: 'capacity',
      severity: 'warning', // Should be warning for new parent
      rule: 'child-capacity-sum',
    });
    expect(capacitySumIssues[0].message).toContain('should sum to 100%');
    expect(capacitySumIssues[0].message).toContain('10.0%');
  });

  it('should show error when modifying children of parent that already had children', () => {
    // Create parent queue WITH existing children
    const schedulerData: SchedulerInfo = {
      type: 'capacityScheduler',
      queueName: 'root',
      capacity: 100,
      usedCapacity: 0,
      maxCapacity: 100,
      queues: {
        queue: [
          {
            queueName: 'production',
            queuePath: 'root.production',
            capacity: 100,
            usedCapacity: 0,
            maxCapacity: 100,
            absoluteCapacity: 100,
            absoluteMaxCapacity: 100,
            absoluteUsedCapacity: 0,
            numApplications: 0,
            numActiveApplications: 0,
            numPendingApplications: 0,
            queueType: 'parent',
            state: 'RUNNING',
            queues: {
              queue: [
                {
                  queueName: 'team-a',
                  queuePath: 'root.production.team-a',
                  capacity: 50,
                  usedCapacity: 0,
                  maxCapacity: 100,
                  absoluteCapacity: 50,
                  absoluteMaxCapacity: 100,
                  absoluteUsedCapacity: 0,
                  numApplications: 0,
                  numActiveApplications: 0,
                  numPendingApplications: 0,
                  queueType: 'leaf',
                  state: 'RUNNING',
                },
                {
                  queueName: 'team-b',
                  queuePath: 'root.production.team-b',
                  capacity: 30, // Was 50, now 30 = only 80% total
                  usedCapacity: 0,
                  maxCapacity: 100,
                  absoluteCapacity: 30,
                  absoluteMaxCapacity: 100,
                  absoluteUsedCapacity: 0,
                  numApplications: 0,
                  numActiveApplications: 0,
                  numPendingApplications: 0,
                  queueType: 'leaf',
                  state: 'RUNNING',
                },
              ],
            },
          },
        ],
      },
    };

    const config = new Map([
      ['yarn.scheduler.capacity.root.capacity', '100'],
      ['yarn.scheduler.capacity.root.production.capacity', '100'],
      ['yarn.scheduler.capacity.root.production.team-a.capacity', '50'],
      ['yarn.scheduler.capacity.root.production.team-b.capacity', '30'], // Modified to 30
    ]);

    const context: ValidationContext = {
      queuePath: 'root.production', // Validating parent queue
      fieldName: 'capacity',
      fieldValue: '100',
      config,
      schedulerData,
      stagedChanges: [
        {
          id: '1',
          type: 'update',
          queuePath: 'root.production.team-b',
          property: 'capacity',
          oldValue: '50',
          newValue: '30',
          timestamp: Date.now(),
        },
      ],
      legacyModeEnabled: true,
    };

    const issues = runFieldValidation(context);
    const capacitySumIssues = issues.filter((issue) => issue.rule === 'child-capacity-sum');

    expect(capacitySumIssues).toHaveLength(1);
    expect(capacitySumIssues[0]).toMatchObject({
      queuePath: 'root.production',
      field: 'capacity',
      severity: 'error', // Should be error for existing parent
      rule: 'child-capacity-sum',
    });
    expect(capacitySumIssues[0].message).toContain('must sum to 100%');
    expect(capacitySumIssues[0].message).toContain('80.0%');
  });

  it('should pass when adding multiple children that sum to 100%', () => {
    const schedulerData: SchedulerInfo = {
      type: 'capacityScheduler',
      queueName: 'root',
      capacity: 100,
      usedCapacity: 0,
      maxCapacity: 100,
      queues: {
        queue: [
          {
            queueName: 'production',
            queuePath: 'root.production',
            capacity: 100,
            usedCapacity: 0,
            maxCapacity: 100,
            absoluteCapacity: 100,
            absoluteMaxCapacity: 100,
            absoluteUsedCapacity: 0,
            numApplications: 0,
            numActiveApplications: 0,
            numPendingApplications: 0,
            queueType: 'parent',
            state: 'RUNNING',
            // No children yet
          },
        ],
      },
    };

    const config = new Map([
      ['yarn.scheduler.capacity.root.capacity', '100'],
      ['yarn.scheduler.capacity.root.production.capacity', '100'],
      ['yarn.scheduler.capacity.root.production.team-a.capacity', '60'],
      ['yarn.scheduler.capacity.root.production.team-b.capacity', '40'],
    ]);

    const context: ValidationContext = {
      queuePath: 'root.production',
      fieldName: 'capacity',
      fieldValue: '100',
      config,
      schedulerData,
      stagedChanges: [
        {
          id: '1',
          type: 'add',
          queuePath: 'root.production.team-a',
          property: 'capacity',
          oldValue: undefined,
          newValue: '60',
          timestamp: Date.now(),
        },
        {
          id: '2',
          type: 'add',
          queuePath: 'root.production.team-b',
          property: 'capacity',
          oldValue: undefined,
          newValue: '40',
          timestamp: Date.now(),
        },
      ],
      legacyModeEnabled: true,
    };

    const issues = runFieldValidation(context);
    const capacitySumIssues = issues.filter((issue) => issue.rule === 'child-capacity-sum');

    expect(capacitySumIssues).toHaveLength(0);
  });

  it('should show warning when adding multiple children that do not sum to 100%', () => {
    const schedulerData: SchedulerInfo = {
      type: 'capacityScheduler',
      queueName: 'root',
      capacity: 100,
      usedCapacity: 0,
      maxCapacity: 100,
      queues: {
        queue: [
          {
            queueName: 'production',
            queuePath: 'root.production',
            capacity: 100,
            usedCapacity: 0,
            maxCapacity: 100,
            absoluteCapacity: 100,
            absoluteMaxCapacity: 100,
            absoluteUsedCapacity: 0,
            numApplications: 0,
            numActiveApplications: 0,
            numPendingApplications: 0,
            queueType: 'parent',
            state: 'RUNNING',
            // No children yet
          },
        ],
      },
    };

    const config = new Map([
      ['yarn.scheduler.capacity.root.capacity', '100'],
      ['yarn.scheduler.capacity.root.production.capacity', '100'],
      ['yarn.scheduler.capacity.root.production.team-a.capacity', '30'],
      ['yarn.scheduler.capacity.root.production.team-b.capacity', '30'],
    ]);

    const context: ValidationContext = {
      queuePath: 'root.production',
      fieldName: 'capacity',
      fieldValue: '100',
      config,
      schedulerData,
      stagedChanges: [
        {
          id: '1',
          type: 'add',
          queuePath: 'root.production.team-a',
          property: 'capacity',
          oldValue: undefined,
          newValue: '30',
          timestamp: Date.now(),
        },
        {
          id: '2',
          type: 'add',
          queuePath: 'root.production.team-b',
          property: 'capacity',
          oldValue: undefined,
          newValue: '30',
          timestamp: Date.now(),
        },
      ],
      legacyModeEnabled: true,
    };

    const issues = runFieldValidation(context);
    const capacitySumIssues = issues.filter((issue) => issue.rule === 'child-capacity-sum');

    expect(capacitySumIssues).toHaveLength(1);
    expect(capacitySumIssues[0]).toMatchObject({
      queuePath: 'root.production',
      field: 'capacity',
      severity: 'warning', // Warning because parent had no children originally
      rule: 'child-capacity-sum',
    });
    expect(capacitySumIssues[0].message).toContain('should sum to 100%');
    expect(capacitySumIssues[0].message).toContain('60.0%');
  });

  it('should pass after removing a child queue if remaining children sum to 100%', () => {
    const schedulerData: SchedulerInfo = {
      type: 'capacityScheduler',
      queueName: 'root',
      capacity: 100,
      usedCapacity: 0,
      maxCapacity: 100,
      queues: {
        queue: [
          {
            queueName: 'production',
            queuePath: 'root.production',
            capacity: 100,
            usedCapacity: 0,
            maxCapacity: 100,
            absoluteCapacity: 100,
            absoluteMaxCapacity: 100,
            absoluteUsedCapacity: 0,
            numApplications: 0,
            numActiveApplications: 0,
            numPendingApplications: 0,
            queueType: 'parent',
            state: 'RUNNING',
            queues: {
              queue: [
                {
                  queueName: 'team-a',
                  queuePath: 'root.production.team-a',
                  capacity: 60,
                  usedCapacity: 0,
                  maxCapacity: 100,
                  absoluteCapacity: 60,
                  absoluteMaxCapacity: 100,
                  absoluteUsedCapacity: 0,
                  numApplications: 0,
                  numActiveApplications: 0,
                  numPendingApplications: 0,
                  queueType: 'leaf',
                  state: 'RUNNING',
                },
                {
                  queueName: 'team-b',
                  queuePath: 'root.production.team-b',
                  capacity: 40,
                  usedCapacity: 0,
                  maxCapacity: 100,
                  absoluteCapacity: 40,
                  absoluteMaxCapacity: 100,
                  absoluteUsedCapacity: 0,
                  numApplications: 0,
                  numActiveApplications: 0,
                  numPendingApplications: 0,
                  queueType: 'leaf',
                  state: 'RUNNING',
                },
              ],
            },
          },
        ],
      },
    };

    const config = new Map([
      ['yarn.scheduler.capacity.root.capacity', '100'],
      ['yarn.scheduler.capacity.root.production.capacity', '100'],
      ['yarn.scheduler.capacity.root.production.team-a.capacity', '100'], // Updated to 100
      // team-b will be removed
    ]);

    const context: ValidationContext = {
      queuePath: 'root.production',
      fieldName: 'capacity',
      fieldValue: '100',
      config,
      schedulerData,
      stagedChanges: [
        {
          id: '1',
          type: 'remove',
          queuePath: 'root.production.team-b',
          property: 'capacity',
          oldValue: '40',
          newValue: undefined,
          timestamp: Date.now(),
        },
        {
          id: '2',
          type: 'update',
          queuePath: 'root.production.team-a',
          property: 'capacity',
          oldValue: '60',
          newValue: '100',
          timestamp: Date.now(),
        },
      ],
      legacyModeEnabled: true,
    };

    const issues = runFieldValidation(context);
    const capacitySumIssues = issues.filter((issue) => issue.rule === 'child-capacity-sum');

    expect(capacitySumIssues).toHaveLength(0);
  });

  it('should skip validation for parent with no children after removals', () => {
    const schedulerData: SchedulerInfo = {
      type: 'capacityScheduler',
      queueName: 'root',
      capacity: 100,
      usedCapacity: 0,
      maxCapacity: 100,
      queues: {
        queue: [
          {
            queueName: 'production',
            queuePath: 'root.production',
            capacity: 100,
            usedCapacity: 0,
            maxCapacity: 100,
            absoluteCapacity: 100,
            absoluteMaxCapacity: 100,
            absoluteUsedCapacity: 0,
            numApplications: 0,
            numActiveApplications: 0,
            numPendingApplications: 0,
            queueType: 'parent',
            state: 'RUNNING',
            queues: {
              queue: [
                {
                  queueName: 'team-a',
                  queuePath: 'root.production.team-a',
                  capacity: 100,
                  usedCapacity: 0,
                  maxCapacity: 100,
                  absoluteCapacity: 100,
                  absoluteMaxCapacity: 100,
                  absoluteUsedCapacity: 0,
                  numApplications: 0,
                  numActiveApplications: 0,
                  numPendingApplications: 0,
                  queueType: 'leaf',
                  state: 'RUNNING',
                },
              ],
            },
          },
        ],
      },
    };

    const config = new Map([
      ['yarn.scheduler.capacity.root.capacity', '100'],
      ['yarn.scheduler.capacity.root.production.capacity', '100'],
    ]);

    const context: ValidationContext = {
      queuePath: 'root.production',
      fieldName: 'capacity',
      fieldValue: '100',
      config,
      schedulerData,
      stagedChanges: [
        {
          id: '1',
          type: 'remove',
          queuePath: 'root.production.team-a',
          property: 'capacity',
          oldValue: '100',
          newValue: undefined,
          timestamp: Date.now(),
        },
      ],
      legacyModeEnabled: true,
    };

    const issues = runFieldValidation(context);
    const capacitySumIssues = issues.filter((issue) => issue.rule === 'child-capacity-sum');

    expect(capacitySumIssues).toHaveLength(0);
  });

  it('should skip validation when not in legacy mode', () => {
    const schedulerData: SchedulerInfo = {
      type: 'capacityScheduler',
      queueName: 'root',
      capacity: 100,
      usedCapacity: 0,
      maxCapacity: 100,
      queues: {
        queue: [
          {
            queueName: 'production',
            queuePath: 'root.production',
            capacity: 100,
            usedCapacity: 0,
            maxCapacity: 100,
            absoluteCapacity: 100,
            absoluteMaxCapacity: 100,
            absoluteUsedCapacity: 0,
            numApplications: 0,
            numActiveApplications: 0,
            numPendingApplications: 0,
            queueType: 'parent',
            state: 'RUNNING',
          },
        ],
      },
    };

    const config = new Map([
      ['yarn.scheduler.capacity.root.capacity', '100'],
      ['yarn.scheduler.capacity.root.production.capacity', '100'],
      ['yarn.scheduler.capacity.root.production.team-a.capacity', '10'],
    ]);

    const context: ValidationContext = {
      queuePath: 'root.production',
      fieldName: 'capacity',
      fieldValue: '100',
      config,
      schedulerData,
      stagedChanges: [
        {
          id: '1',
          type: 'add',
          queuePath: 'root.production.team-a',
          property: 'capacity',
          oldValue: undefined,
          newValue: '10',
          timestamp: Date.now(),
        },
      ],
      legacyModeEnabled: false, // Flexible mode
    };

    const issues = runFieldValidation(context);
    const capacitySumIssues = issues.filter((issue) => issue.rule === 'child-capacity-sum');

    expect(capacitySumIssues).toHaveLength(0);
  });

  it('should skip validation for absolute capacity mode', () => {
    const schedulerData: SchedulerInfo = {
      type: 'capacityScheduler',
      queueName: 'root',
      capacity: 100,
      usedCapacity: 0,
      maxCapacity: 100,
      queues: {
        queue: [
          {
            queueName: 'production',
            queuePath: 'root.production',
            capacity: 100,
            usedCapacity: 0,
            maxCapacity: 100,
            absoluteCapacity: 100,
            absoluteMaxCapacity: 100,
            absoluteUsedCapacity: 0,
            numApplications: 0,
            numActiveApplications: 0,
            numPendingApplications: 0,
            queueType: 'parent',
            state: 'RUNNING',
          },
        ],
      },
    };

    const config = new Map([
      ['yarn.scheduler.capacity.root.capacity', '100'],
      ['yarn.scheduler.capacity.root.production.capacity', '[memory=8192,vcores=8]'],
      ['yarn.scheduler.capacity.root.production.team-a.capacity', '[memory=2048,vcores=2]'],
    ]);

    const context: ValidationContext = {
      queuePath: 'root.production',
      fieldName: 'capacity',
      fieldValue: '[memory=8192,vcores=8]',
      config,
      schedulerData,
      stagedChanges: [
        {
          id: '1',
          type: 'add',
          queuePath: 'root.production.team-a',
          property: 'capacity',
          oldValue: undefined,
          newValue: '[memory=2048,vcores=2]',
          timestamp: Date.now(),
        },
      ],
      legacyModeEnabled: true,
    };

    const issues = runFieldValidation(context);
    const capacitySumIssues = issues.filter((issue) => issue.rule === 'child-capacity-sum');

    // Should skip because absolute capacity doesn't have sum-to-100% requirement
    expect(capacitySumIssues).toHaveLength(0);
  });
});
