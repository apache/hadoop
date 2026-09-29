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


import { describe, it, expect, vi, beforeEach } from 'vitest';
import { validatePropertyChange, validateStagedChanges } from '~/features/validation/crossQueue';
import type { SchedulerInfo, StagedChange, ValidationIssue } from '~/types';

// Mock dependencies
vi.mock('~/features/validation/service', () => ({
  validateQueue: vi.fn(),
}));

vi.mock('~/features/validation/ruleCategories', () => ({
  isBlockingError: vi.fn(),
  isCrossQueueRule: vi.fn(),
}));

vi.mock('~/utils/configUtils', () => ({
  mergeStagedConfig: vi.fn(),
}));

vi.mock('~/features/validation/utils/affectedQueues', () => ({
  getAffectedQueuesForValidation: vi.fn(),
}));

import { validateQueue } from '~/features/validation/service';
import { isBlockingError, isCrossQueueRule } from '~/features/validation/ruleCategories';
import { mergeStagedConfig } from '~/utils/configUtils';
import { getAffectedQueuesForValidation } from '~/features/validation/utils/affectedQueues';

describe('crossQueue validation', () => {
  const createMockSchedulerData = (): SchedulerInfo => ({
    type: 'capacityScheduler',
    capacity: 100,
    usedCapacity: 0,
    maxCapacity: 100,
    queueName: 'root',
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
  });

  beforeEach(() => {
    vi.clearAllMocks();

    // Default mock implementations
    vi.mocked(isBlockingError).mockReturnValue(false);
    vi.mocked(isCrossQueueRule).mockReturnValue(false);
    vi.mocked(mergeStagedConfig).mockImplementation((configData, _stagedChanges) => {
      return new Map(configData);
    });
    vi.mocked(getAffectedQueuesForValidation).mockReturnValue(['root.default']);
    vi.mocked(validateQueue).mockReturnValue({ valid: true, issues: [] });
  });

  describe('validatePropertyChange', () => {
    it('should return empty array when schedulerData is null', () => {
      const result = validatePropertyChange({
        propertyName: 'capacity',
        propertyValue: '60',
        queuePath: 'root.default',
        schedulerData: null,
        configData: new Map(),
        stagedChanges: [],
      });

      expect(result).toEqual([]);
      expect(validateQueue).not.toHaveBeenCalled();
    });

    it('should validate affected queues', () => {
      const schedulerData = createMockSchedulerData();
      const configData = new Map([['yarn.scheduler.capacity.root.default.capacity', '50']]);

      vi.mocked(getAffectedQueuesForValidation).mockReturnValue(['root.default']);
      vi.mocked(validateQueue).mockReturnValue({
        valid: true,
        issues: [],
      });

      validatePropertyChange({
        propertyName: 'capacity',
        propertyValue: '60',
        queuePath: 'root.default',
        schedulerData,
        configData,
        stagedChanges: [],
      });

      expect(getAffectedQueuesForValidation).toHaveBeenCalledWith(
        'capacity',
        'root.default',
        schedulerData,
        expect.any(Array),
      );
      expect(validateQueue).toHaveBeenCalled();
    });

    it('should create temporary staged change for validation', () => {
      const schedulerData = createMockSchedulerData();
      const configData = new Map();

      vi.mocked(getAffectedQueuesForValidation).mockReturnValue(['root.default']);

      validatePropertyChange({
        propertyName: 'capacity',
        propertyValue: '60',
        queuePath: 'root.default',
        schedulerData,
        configData,
        stagedChanges: [],
      });

      const mergeCall = vi.mocked(mergeStagedConfig).mock.calls[0];
      const stagedChangesWithTemp = mergeCall[1] as StagedChange[];

      expect(stagedChangesWithTemp).toHaveLength(1);
      expect(stagedChangesWithTemp[0].queuePath).toBe('root.default');
      expect(stagedChangesWithTemp[0].property).toBe('capacity');
      expect(stagedChangesWithTemp[0].newValue).toBe('60');
    });

    it('should include existing staged changes plus temp change', () => {
      const schedulerData = createMockSchedulerData();
      const configData = new Map();
      const existingChange: StagedChange = {
        id: 'existing',
        type: 'update',
        queuePath: 'root.production',
        property: 'capacity',
        oldValue: '50',
        newValue: '40',
        timestamp: Date.now(),
      };

      vi.mocked(getAffectedQueuesForValidation).mockReturnValue(['root.default']);

      validatePropertyChange({
        propertyName: 'capacity',
        propertyValue: '60',
        queuePath: 'root.default',
        schedulerData,
        configData,
        stagedChanges: [existingChange],
      });

      const mergeCall = vi.mocked(mergeStagedConfig).mock.calls[0];
      const stagedChangesWithTemp = mergeCall[1] as StagedChange[];

      expect(stagedChangesWithTemp).toHaveLength(2);
    });

    it('should filter out blocking errors by default', () => {
      const schedulerData = createMockSchedulerData();
      const configData = new Map();

      vi.mocked(getAffectedQueuesForValidation).mockReturnValue(['root.default']);
      vi.mocked(validateQueue).mockReturnValue({
        valid: false,
        issues: [
          {
            queuePath: 'root.default',
            field: 'capacity',
            message: 'Blocking error',
            severity: 'error',
            rule: 'blocking-rule',
          },
          {
            queuePath: 'root.default',
            field: 'capacity',
            message: 'Non-blocking error',
            severity: 'error',
            rule: 'non-blocking-rule',
          },
        ],
      });

      vi.mocked(isBlockingError).mockImplementation((rule) => rule === 'blocking-rule');

      const result = validatePropertyChange({
        propertyName: 'capacity',
        propertyValue: '60',
        queuePath: 'root.default',
        schedulerData,
        configData,
        stagedChanges: [],
        includeBlockingErrors: false,
      });

      expect(result).toHaveLength(1);
      expect(result[0].rule).toBe('non-blocking-rule');
    });

    it('should include blocking errors when requested', () => {
      const schedulerData = createMockSchedulerData();
      const configData = new Map();

      vi.mocked(getAffectedQueuesForValidation).mockReturnValue(['root.default']);
      vi.mocked(validateQueue).mockReturnValue({
        valid: false,
        issues: [
          {
            queuePath: 'root.default',
            field: 'capacity',
            message: 'Blocking error',
            severity: 'error',
            rule: 'blocking-rule',
          },
          {
            queuePath: 'root.default',
            field: 'capacity',
            message: 'Non-blocking error',
            severity: 'error',
            rule: 'non-blocking-rule',
          },
        ],
      });

      vi.mocked(isBlockingError).mockImplementation((rule) => rule === 'blocking-rule');

      const result = validatePropertyChange({
        propertyName: 'capacity',
        propertyValue: '60',
        queuePath: 'root.default',
        schedulerData,
        configData,
        stagedChanges: [],
        includeBlockingErrors: true,
      });

      expect(result).toHaveLength(2);
    });

    it('should include issues for queue being directly edited', () => {
      const schedulerData = createMockSchedulerData();
      const configData = new Map();

      vi.mocked(getAffectedQueuesForValidation).mockReturnValue(['root.default']);
      vi.mocked(validateQueue).mockReturnValue({
        valid: false,
        issues: [
          {
            queuePath: 'root.default',
            field: 'capacity',
            message: 'Direct queue error',
            severity: 'error',
            rule: 'some-rule',
          },
        ],
      });

      const result = validatePropertyChange({
        propertyName: 'capacity',
        propertyValue: '60',
        queuePath: 'root.default',
        schedulerData,
        configData,
        stagedChanges: [],
      });

      expect(result).toHaveLength(1);
      expect(result[0].queuePath).toBe('root.default');
    });

    it('should filter parent-child-capacity-mode for non-parent queues', () => {
      const schedulerData = createMockSchedulerData();
      const configData = new Map();

      vi.mocked(getAffectedQueuesForValidation).mockReturnValue([
        'root.default',
        'root.production',
      ]);
      vi.mocked(validateQueue)
        .mockReturnValueOnce({
          valid: true,
          issues: [],
        })
        .mockReturnValueOnce({
          valid: false,
          issues: [
            {
              queuePath: 'root.production',
              field: 'capacity',
              message: 'Parent-child mode error',
              severity: 'error',
              rule: 'parent-child-capacity-mode',
            },
          ],
        });

      const result = validatePropertyChange({
        propertyName: 'capacity',
        propertyValue: '60',
        queuePath: 'root.default',
        schedulerData,
        configData,
        stagedChanges: [],
      });

      // Should not include the parent-child-capacity-mode error from sibling
      expect(result).toHaveLength(0);
    });

    it('should include parent-child-capacity-mode for child queues when parent is changed', () => {
      const schedulerData = createMockSchedulerData();
      const configData = new Map();

      vi.mocked(getAffectedQueuesForValidation).mockReturnValue(['root', 'root.default']);
      vi.mocked(validateQueue)
        .mockReturnValueOnce({
          valid: true,
          issues: [],
        })
        .mockReturnValueOnce({
          valid: false,
          issues: [
            {
              queuePath: 'root.default',
              field: 'capacity',
              message: 'Parent-child mode error',
              severity: 'error',
              rule: 'parent-child-capacity-mode',
            },
          ],
        });

      const result = validatePropertyChange({
        propertyName: 'capacity',
        propertyValue: '60',
        queuePath: 'root',
        schedulerData,
        configData,
        stagedChanges: [],
      });

      expect(result).toHaveLength(1);
      expect(result[0].rule).toBe('parent-child-capacity-mode');
    });

    it('should include cross-queue issues', () => {
      const schedulerData = createMockSchedulerData();
      const configData = new Map();

      vi.mocked(getAffectedQueuesForValidation).mockReturnValue([
        'root.default',
        'root.production',
      ]);
      vi.mocked(isCrossQueueRule).mockReturnValue(true);
      vi.mocked(validateQueue)
        .mockReturnValueOnce({
          valid: true,
          issues: [],
        })
        .mockReturnValueOnce({
          valid: false,
          issues: [
            {
              queuePath: 'root.production',
              field: 'capacity',
              message: 'Cross-queue error',
              severity: 'error',
              rule: 'child-capacity-sum',
            },
          ],
        });

      const result = validatePropertyChange({
        propertyName: 'capacity',
        propertyValue: '60',
        queuePath: 'root.default',
        schedulerData,
        configData,
        stagedChanges: [],
      });

      expect(result).toHaveLength(1);
      expect(result[0].rule).toBe('child-capacity-sum');
    });

    it('should deduplicate issues', () => {
      const schedulerData = createMockSchedulerData();
      const configData = new Map();

      vi.mocked(getAffectedQueuesForValidation).mockReturnValue([
        'root.default',
        'root.production',
      ]);
      vi.mocked(validateQueue).mockReturnValue({
        valid: false,
        issues: [
          {
            queuePath: 'root.default',
            field: 'capacity',
            message: 'Duplicate error',
            severity: 'error',
            rule: 'test-rule',
          },
        ],
      });

      const result = validatePropertyChange({
        propertyName: 'capacity',
        propertyValue: '60',
        queuePath: 'root.default',
        schedulerData,
        configData,
        stagedChanges: [],
      });

      // Should only have one instance even though validated for multiple queues
      expect(result).toHaveLength(1);
    });

    it('should extract queue properties correctly', () => {
      const schedulerData = createMockSchedulerData();
      const configData = new Map([
        ['yarn.scheduler.capacity.root.default.capacity', '50'],
        ['yarn.scheduler.capacity.root.default.maximum-capacity', '100'],
        ['yarn.scheduler.capacity.root.production.capacity', '50'],
      ]);

      vi.mocked(getAffectedQueuesForValidation).mockReturnValue(['root.default']);
      vi.mocked(mergeStagedConfig).mockImplementation((config, _changes) => {
        return new Map(config);
      });

      validatePropertyChange({
        propertyName: 'capacity',
        propertyValue: '60',
        queuePath: 'root.default',
        schedulerData,
        configData,
        stagedChanges: [],
      });

      const validateCall = vi.mocked(validateQueue).mock.calls[0][0];
      const properties = validateCall.properties;

      expect(properties['capacity']).toBe('50');
      expect(properties['maximum-capacity']).toBe('100');
      expect(properties['root.production.capacity']).toBeUndefined();
    });

    it('should only attach capacity errors to capacity changes (field matching)', () => {
      const schedulerData = createMockSchedulerData();
      const configData = new Map();

      vi.mocked(getAffectedQueuesForValidation).mockReturnValue(['root.default']);
      vi.mocked(validateQueue).mockReturnValue({
        valid: false,
        issues: [
          {
            queuePath: 'root.default',
            field: 'capacity',
            message: 'Capacity error',
            severity: 'error',
            rule: 'capacity-rule',
          },
          {
            queuePath: 'root.default',
            field: 'state',
            message: 'State error',
            severity: 'error',
            rule: 'state-rule',
          },
        ],
      });

      const result = validatePropertyChange({
        propertyName: 'capacity',
        propertyValue: '60',
        queuePath: 'root.default',
        schedulerData,
        configData,
        stagedChanges: [],
      });

      // Should only include capacity error, not state error
      expect(result).toHaveLength(1);
      expect(result[0].field).toBe('capacity');
      expect(result[0].message).toBe('Capacity error');
    });

    it('should only attach state errors to state changes (field matching)', () => {
      const schedulerData = createMockSchedulerData();
      const configData = new Map();

      vi.mocked(getAffectedQueuesForValidation).mockReturnValue(['root.default']);
      vi.mocked(validateQueue).mockReturnValue({
        valid: false,
        issues: [
          {
            queuePath: 'root.default',
            field: 'capacity',
            message: 'Capacity error',
            severity: 'error',
            rule: 'capacity-rule',
          },
          {
            queuePath: 'root.default',
            field: 'state',
            message: 'State error',
            severity: 'error',
            rule: 'state-rule',
          },
        ],
      });

      const result = validatePropertyChange({
        propertyName: 'state',
        propertyValue: 'STOPPED',
        queuePath: 'root.default',
        schedulerData,
        configData,
        stagedChanges: [],
      });

      // Should only include state error, not capacity error
      expect(result).toHaveLength(1);
      expect(result[0].field).toBe('state');
      expect(result[0].message).toBe('State error');
    });

    it('should not attach capacity errors when changing state property', () => {
      const schedulerData = createMockSchedulerData();
      const configData = new Map();

      vi.mocked(getAffectedQueuesForValidation).mockReturnValue(['root.default']);
      vi.mocked(validateQueue).mockReturnValue({
        valid: false,
        issues: [
          {
            queuePath: 'root.default',
            field: 'capacity',
            message: 'Capacity must be between 0 and 100',
            severity: 'error',
            rule: 'capacity-range',
          },
        ],
      });

      const result = validatePropertyChange({
        propertyName: 'state',
        propertyValue: 'RUNNING',
        queuePath: 'root.default',
        schedulerData,
        configData,
        stagedChanges: [],
      });

      // Should NOT include the capacity error when changing state
      expect(result).toHaveLength(0);
    });

    it('should not attach state errors when changing capacity property', () => {
      const schedulerData = createMockSchedulerData();
      const configData = new Map();

      vi.mocked(getAffectedQueuesForValidation).mockReturnValue(['root.default']);
      vi.mocked(validateQueue).mockReturnValue({
        valid: false,
        issues: [
          {
            queuePath: 'root.default',
            field: 'state',
            message: 'Invalid state transition',
            severity: 'error',
            rule: 'state-validation',
          },
        ],
      });

      const result = validatePropertyChange({
        propertyName: 'capacity',
        propertyValue: '75',
        queuePath: 'root.default',
        schedulerData,
        configData,
        stagedChanges: [],
      });

      // Should NOT include the state error when changing capacity
      expect(result).toHaveLength(0);
    });
  });

  describe('validateStagedChanges', () => {
    it('should return empty map when schedulerData is null', () => {
      const result = validateStagedChanges({
        stagedChanges: [],
        schedulerData: null,
        configData: new Map(),
      });

      expect(result.size).toBe(0);
    });

    it('should return empty map when staged changes is empty', () => {
      const result = validateStagedChanges({
        stagedChanges: [],
        schedulerData: createMockSchedulerData(),
        configData: new Map(),
      });

      expect(result.size).toBe(0);
    });

    it('should validate update changes', () => {
      const schedulerData = createMockSchedulerData();
      const stagedChanges: StagedChange[] = [
        {
          id: '1',
          type: 'update',
          queuePath: 'root.default',
          property: 'capacity',
          oldValue: '50',
          newValue: '60',
          timestamp: Date.now(),
        },
      ];

      vi.mocked(getAffectedQueuesForValidation).mockReturnValue(['root.default']);
      vi.mocked(validateQueue).mockReturnValue({
        valid: false,
        issues: [
          {
            queuePath: 'root.default',
            field: 'capacity',
            message: 'Error',
            severity: 'error',
            rule: 'test-rule',
          },
        ],
      });

      const result = validateStagedChanges({
        stagedChanges,
        schedulerData,
        configData: new Map(),
      });

      expect(result.size).toBe(1);
      expect(result.get('1')).toHaveLength(1);
    });

    it('should validate add changes with capacity property', () => {
      const schedulerData = createMockSchedulerData();
      const stagedChanges: StagedChange[] = [
        {
          id: '1',
          type: 'add',
          queuePath: 'root.newqueue',
          property: 'capacity',
          oldValue: '',
          newValue: '30',
          timestamp: Date.now(),
        },
      ];

      vi.mocked(getAffectedQueuesForValidation).mockReturnValue(['root.newqueue']);
      vi.mocked(validateQueue).mockReturnValue({
        valid: true,
        issues: [],
      });

      const result = validateStagedChanges({
        stagedChanges,
        schedulerData,
        configData: new Map(),
      });

      expect(result.size).toBe(1);
      expect(result.get('1')).toBeUndefined(); // No issues
    });

    it('should skip non-update changes without property', () => {
      const schedulerData = createMockSchedulerData();
      const stagedChanges: StagedChange[] = [
        {
          id: '1',
          type: 'remove',
          queuePath: 'root.default',
          property: 'queues',
          oldValue: '',
          newValue: '',
          timestamp: Date.now(),
        },
      ];

      const result = validateStagedChanges({
        stagedChanges,
        schedulerData,
        configData: new Map(),
      });

      expect(result.size).toBe(1);
      expect(validateQueue).not.toHaveBeenCalled();
    });

    it('should preserve existing validation errors for skipped changes', () => {
      const schedulerData = createMockSchedulerData();
      const existingErrors: ValidationIssue[] = [
        {
          queuePath: 'root.default',
          field: 'state',
          message: 'Existing error',
          severity: 'error',
          rule: 'test-rule',
        },
      ];
      const stagedChanges: StagedChange[] = [
        {
          id: '1',
          type: 'remove',
          queuePath: 'root.default',
          property: 'queues',
          oldValue: '',
          newValue: '',
          timestamp: Date.now(),
          validationErrors: existingErrors,
        },
      ];

      const result = validateStagedChanges({
        stagedChanges,
        schedulerData,
        configData: new Map(),
      });

      expect(result.get('1')).toEqual(existingErrors);
    });

    it('should filter out current change when validating', () => {
      const schedulerData = createMockSchedulerData();
      const stagedChanges: StagedChange[] = [
        {
          id: '1',
          type: 'update',
          queuePath: 'root.default',
          property: 'capacity',
          oldValue: '50',
          newValue: '60',
          timestamp: Date.now(),
        },
        {
          id: '2',
          type: 'update',
          queuePath: 'root.production',
          property: 'capacity',
          oldValue: '50',
          newValue: '40',
          timestamp: Date.now(),
        },
      ];

      vi.mocked(getAffectedQueuesForValidation).mockReturnValue(['root.default']);

      validateStagedChanges({
        stagedChanges,
        schedulerData,
        configData: new Map(),
      });

      // When validating change 1, only change 2 should be in the staged changes
      const mergeCall = vi.mocked(mergeStagedConfig).mock.calls[0];
      const passedChanges = mergeCall[1] as StagedChange[];
      expect(passedChanges).toHaveLength(2); // change 2 + temp change for validation
      expect(passedChanges.some((c) => c.id === '2')).toBe(true);
    });

    it('should set undefined for changes with no issues', () => {
      const schedulerData = createMockSchedulerData();
      const stagedChanges: StagedChange[] = [
        {
          id: '1',
          type: 'update',
          queuePath: 'root.default',
          property: 'capacity',
          oldValue: '50',
          newValue: '60',
          timestamp: Date.now(),
        },
      ];

      vi.mocked(getAffectedQueuesForValidation).mockReturnValue(['root.default']);
      vi.mocked(validateQueue).mockReturnValue({
        valid: true,
        issues: [],
      });

      const result = validateStagedChanges({
        stagedChanges,
        schedulerData,
        configData: new Map(),
      });

      expect(result.get('1')).toBeUndefined();
    });
  });

  describe('validateStagedChanges', () => {
    it('should return empty map when schedulerData is null', () => {
      const result = validateStagedChanges({
        affectedQueuePaths: new Set(),
        affectedProperties: new Set(),
        stagedChanges: [],
        schedulerData: null,
        configData: new Map(),
      });

      expect(result.size).toBe(0);
    });

    it('should return empty map when staged changes is empty', () => {
      const result = validateStagedChanges({
        affectedQueuePaths: new Set(),
        affectedProperties: new Set(),
        stagedChanges: [],
        schedulerData: createMockSchedulerData(),
        configData: new Map(),
      });

      expect(result.size).toBe(0);
    });

    it('should validate changes for affected queue paths', () => {
      const schedulerData = createMockSchedulerData();
      const stagedChanges: StagedChange[] = [
        {
          id: '1',
          type: 'update',
          queuePath: 'root.default',
          property: 'capacity',
          oldValue: '50',
          newValue: '60',
          timestamp: Date.now(),
        },
      ];

      vi.mocked(getAffectedQueuesForValidation).mockReturnValue(['root.default']);
      vi.mocked(validateQueue).mockReturnValue({
        valid: true,
        issues: [],
      });

      const result = validateStagedChanges({
        affectedQueuePaths: new Set(['root.default']),
        affectedProperties: new Set(),
        stagedChanges,
        schedulerData,
        configData: new Map(),
      });

      expect(result.size).toBe(1);
      expect(validateQueue).toHaveBeenCalled();
    });

    it('should validate changes for affected properties', () => {
      const schedulerData = createMockSchedulerData();
      const stagedChanges: StagedChange[] = [
        {
          id: '1',
          type: 'update',
          queuePath: 'root.default',
          property: 'capacity',
          oldValue: '50',
          newValue: '60',
          timestamp: Date.now(),
        },
      ];

      vi.mocked(getAffectedQueuesForValidation).mockReturnValue(['root.default']);
      vi.mocked(validateQueue).mockReturnValue({
        valid: true,
        issues: [],
      });

      const result = validateStagedChanges({
        affectedQueuePaths: new Set(),
        affectedProperties: new Set(['capacity']),
        stagedChanges,
        schedulerData,
        configData: new Map(),
      });

      expect(result.size).toBe(1);
      expect(validateQueue).toHaveBeenCalled();
    });

    it('should preserve validation errors for unaffected changes', () => {
      const schedulerData = createMockSchedulerData();
      const existingErrors: ValidationIssue[] = [
        {
          queuePath: 'root.production',
          field: 'state',
          message: 'Existing error',
          severity: 'error',
          rule: 'test-rule',
        },
      ];
      const stagedChanges: StagedChange[] = [
        {
          id: '1',
          type: 'update',
          queuePath: 'root.production',
          property: 'state',
          oldValue: 'RUNNING',
          newValue: 'STOPPED',
          timestamp: Date.now(),
          validationErrors: existingErrors,
        },
      ];

      const result = validateStagedChanges({
        affectedQueuePaths: new Set(['root.default']),
        affectedProperties: new Set(),
        stagedChanges,
        schedulerData,
        configData: new Map(),
      });

      expect(result.get('1')).toEqual(existingErrors);
      expect(validateQueue).not.toHaveBeenCalled();
    });

    it('should skip unaffected changes', () => {
      const schedulerData = createMockSchedulerData();
      const stagedChanges: StagedChange[] = [
        {
          id: '1',
          type: 'update',
          queuePath: 'root.default',
          property: 'capacity',
          oldValue: '50',
          newValue: '60',
          timestamp: Date.now(),
        },
        {
          id: '2',
          type: 'update',
          queuePath: 'root.production',
          property: 'capacity',
          oldValue: '50',
          newValue: '40',
          timestamp: Date.now(),
        },
      ];

      vi.mocked(getAffectedQueuesForValidation).mockReturnValue(['root.default']);

      validateStagedChanges({
        affectedQueuePaths: new Set(['root.default']),
        affectedProperties: new Set(),
        stagedChanges,
        schedulerData,
        configData: new Map(),
      });

      // Should only validate once for the affected queue
      expect(validateQueue).toHaveBeenCalledTimes(1);
    });

    it('should handle add changes with capacity property', () => {
      const schedulerData = createMockSchedulerData();
      const stagedChanges: StagedChange[] = [
        {
          id: '1',
          type: 'add',
          queuePath: 'root.newqueue',
          property: 'capacity',
          oldValue: '',
          newValue: '30',
          timestamp: Date.now(),
        },
      ];

      vi.mocked(getAffectedQueuesForValidation).mockReturnValue(['root.newqueue']);
      vi.mocked(validateQueue).mockReturnValue({
        valid: true,
        issues: [],
      });

      const result = validateStagedChanges({
        affectedQueuePaths: new Set(['root.newqueue']),
        affectedProperties: new Set(),
        stagedChanges,
        schedulerData,
        configData: new Map(),
      });

      expect(result.size).toBe(1);
      expect(validateQueue).toHaveBeenCalled();
    });
  });
});
