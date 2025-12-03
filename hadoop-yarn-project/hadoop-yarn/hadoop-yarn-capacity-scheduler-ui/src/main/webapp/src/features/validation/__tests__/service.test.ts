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
import {
  validateField,
  validateQueue,
  hasBlockingIssues,
  splitIssues,
} from '~/features/validation/service';
import type { SchedulerInfo, StagedChange, ValidationIssue } from '~/types';
import { SPECIAL_VALUES } from '~/types/constants/special-values';

// Mock dependencies
vi.mock('~/utils/configUtils', () => ({
  mergeStagedConfig: vi.fn(),
  applyFieldPreview: vi.fn(),
  buildEffectivePropertyKey: vi.fn(),
}));

vi.mock('~/config/validation-rules', () => ({
  runFieldValidation: vi.fn(),
}));

import {
  mergeStagedConfig,
  applyFieldPreview,
  buildEffectivePropertyKey,
} from '~/utils/configUtils';
import { runFieldValidation } from '~/config/validation-rules';

describe('validation service', () => {
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
      ],
    },
  });

  beforeEach(() => {
    vi.clearAllMocks();

    // Default mock implementations
    vi.mocked(mergeStagedConfig).mockImplementation((configData, _stagedChanges) => {
      return new Map(configData);
    });

    vi.mocked(applyFieldPreview).mockImplementation((config, _queuePath, _fieldName, _value) => {
      return new Map(config);
    });

    vi.mocked(buildEffectivePropertyKey).mockImplementation((queuePath, field) => {
      return `yarn.scheduler.capacity.${queuePath}.${field}`;
    });

    vi.mocked(runFieldValidation).mockReturnValue([]);
  });

  describe('validateField', () => {
    it('should validate a field successfully with no issues', () => {
      const configData = new Map([['yarn.scheduler.capacity.root.default.capacity', '50']]);

      vi.mocked(runFieldValidation).mockReturnValue([]);

      const result = validateField({
        queuePath: 'root.default',
        fieldName: 'capacity',
        value: '60',
        configData,
        stagedChanges: [],
        schedulerData: createMockSchedulerData(),
      });

      expect(result.valid).toBe(true);
      expect(result.issues).toHaveLength(0);
    });

    it('should return invalid when field has errors', () => {
      const configData = new Map();

      vi.mocked(runFieldValidation).mockReturnValue([
        {
          queuePath: 'root.default',
          field: 'capacity',
          message: 'Capacity must be greater than 0',
          severity: 'error',
          rule: 'capacity-minimum',
        },
      ]);

      const result = validateField({
        queuePath: 'root.default',
        fieldName: 'capacity',
        value: '0',
        configData,
        stagedChanges: [],
      });

      expect(result.valid).toBe(false);
      expect(result.issues).toHaveLength(1);
      expect(result.issues[0].severity).toBe('error');
    });

    it('should return valid when field has only warnings', () => {
      const configData = new Map();

      vi.mocked(runFieldValidation).mockReturnValue([
        {
          queuePath: 'root.default',
          field: 'capacity',
          message: 'Consider increasing capacity',
          severity: 'warning',
          rule: 'capacity-warning',
        },
      ]);

      const result = validateField({
        queuePath: 'root.default',
        fieldName: 'capacity',
        value: '10',
        configData,
        stagedChanges: [],
      });

      expect(result.valid).toBe(true);
      expect(result.issues).toHaveLength(1);
      expect(result.issues[0].severity).toBe('warning');
    });

    it('should merge staged changes into config', () => {
      const configData = new Map([['yarn.scheduler.capacity.root.default.capacity', '50']]);
      const stagedChanges: StagedChange[] = [
        {
          id: '1',
          type: 'update',
          queuePath: 'root.production',
          property: 'capacity',
          oldValue: '50',
          newValue: '60',
          timestamp: Date.now(),
        },
      ];

      validateField({
        queuePath: 'root.default',
        fieldName: 'capacity',
        value: '40',
        configData,
        stagedChanges,
      });

      expect(mergeStagedConfig).toHaveBeenCalledWith(configData, stagedChanges);
    });

    it('should apply field preview to config', () => {
      const configData = new Map();
      const mergedConfig = new Map([['yarn.scheduler.capacity.root.default.capacity', '50']]);

      vi.mocked(mergeStagedConfig).mockReturnValue(mergedConfig);

      validateField({
        queuePath: 'root.default',
        fieldName: 'capacity',
        value: '60',
        configData,
        stagedChanges: [],
      });

      expect(applyFieldPreview).toHaveBeenCalledWith(
        mergedConfig,
        'root.default',
        'capacity',
        '60',
      );
    });

    it('should pass correct context to runFieldValidation', () => {
      const configData = new Map();
      const schedulerData = createMockSchedulerData();
      const stagedChanges: StagedChange[] = [];

      validateField({
        queuePath: 'root.default',
        fieldName: 'capacity',
        value: '60',
        configData,
        stagedChanges,
        schedulerData,
      });

      expect(runFieldValidation).toHaveBeenCalledWith(
        expect.objectContaining({
          queuePath: 'root.default',
          fieldName: 'capacity',
          fieldValue: '60',
          config: expect.any(Map),
          schedulerData,
          stagedChanges,
          legacyModeEnabled: true,
        }),
      );
    });

    it('should detect legacy mode disabled when property is "false"', () => {
      const configData = new Map([[SPECIAL_VALUES.LEGACY_MODE_PROPERTY, 'false']]);

      validateField({
        queuePath: 'root.default',
        fieldName: 'capacity',
        value: '60',
        configData,
        stagedChanges: [],
      });

      const context = vi.mocked(runFieldValidation).mock.calls[0][0];
      expect(context.legacyModeEnabled).toBe(false);
    });

    it('should detect legacy mode enabled when property is absent', () => {
      const configData = new Map();

      validateField({
        queuePath: 'root.default',
        fieldName: 'capacity',
        value: '60',
        configData,
        stagedChanges: [],
      });

      const context = vi.mocked(runFieldValidation).mock.calls[0][0];
      expect(context.legacyModeEnabled).toBe(true);
    });

    it('should detect legacy mode enabled when property is "true"', () => {
      const configData = new Map([[SPECIAL_VALUES.LEGACY_MODE_PROPERTY, 'true']]);

      validateField({
        queuePath: 'root.default',
        fieldName: 'capacity',
        value: '60',
        configData,
        stagedChanges: [],
      });

      const context = vi.mocked(runFieldValidation).mock.calls[0][0];
      expect(context.legacyModeEnabled).toBe(true);
    });
  });

  describe('validateQueue', () => {
    it('should validate a queue successfully with no issues', () => {
      const configData = new Map();
      const properties = {
        capacity: '50',
        'maximum-capacity': '100',
      };

      vi.mocked(runFieldValidation).mockReturnValue([]);

      const result = validateQueue({
        queuePath: 'root.default',
        properties,
        configData,
        stagedChanges: [],
      });

      expect(result.valid).toBe(true);
      expect(result.issues).toHaveLength(0);
    });

    it('should validate all provided properties', () => {
      const configData = new Map();
      const properties = {
        capacity: '50',
        'maximum-capacity': '100',
        state: 'RUNNING',
      };

      vi.mocked(runFieldValidation).mockReturnValue([]);

      validateQueue({
        queuePath: 'root.default',
        properties,
        configData,
        stagedChanges: [],
      });

      // Should be called for each property (3 properties)
      expect(runFieldValidation).toHaveBeenCalledTimes(3);
    });

    it('should always validate capacity even if not in properties', () => {
      const configData = new Map([['yarn.scheduler.capacity.root.default.capacity', '50']]);
      const properties = {
        'maximum-capacity': '100',
      };

      vi.mocked(runFieldValidation).mockReturnValue([]);

      validateQueue({
        queuePath: 'root.default',
        properties,
        configData,
        stagedChanges: [],
      });

      // Should be called for maximum-capacity and capacity
      expect(runFieldValidation).toHaveBeenCalledTimes(2);
      const calls = vi.mocked(runFieldValidation).mock.calls;
      expect(calls.some((call) => call[0].fieldName === 'capacity')).toBe(true);
    });

    it('should delete property from config when value is empty string', () => {
      const configData = new Map([['yarn.scheduler.capacity.root.default.state', 'RUNNING']]);
      const properties = {
        state: '',
      };

      vi.mocked(runFieldValidation).mockReturnValue([]);
      vi.mocked(buildEffectivePropertyKey).mockReturnValue(
        'yarn.scheduler.capacity.root.default.state',
      );

      validateQueue({
        queuePath: 'root.default',
        properties,
        configData,
        stagedChanges: [],
      });

      const context = vi.mocked(runFieldValidation).mock.calls[0][0];
      expect(context.config.has('yarn.scheduler.capacity.root.default.state')).toBe(false);
    });

    it('should delete property from config when value is null', () => {
      const configData = new Map([['yarn.scheduler.capacity.root.default.state', 'RUNNING']]);
      const properties = {
        state: null as any,
      };

      vi.mocked(runFieldValidation).mockReturnValue([]);
      vi.mocked(buildEffectivePropertyKey).mockReturnValue(
        'yarn.scheduler.capacity.root.default.state',
      );

      validateQueue({
        queuePath: 'root.default',
        properties,
        configData,
        stagedChanges: [],
      });

      const context = vi.mocked(runFieldValidation).mock.calls[0][0];
      expect(context.config.has('yarn.scheduler.capacity.root.default.state')).toBe(false);
    });

    it('should delete property from config when value is undefined', () => {
      const configData = new Map([['yarn.scheduler.capacity.root.default.state', 'RUNNING']]);
      const properties = {
        state: undefined as any,
      };

      vi.mocked(runFieldValidation).mockReturnValue([]);
      vi.mocked(buildEffectivePropertyKey).mockReturnValue(
        'yarn.scheduler.capacity.root.default.state',
      );

      validateQueue({
        queuePath: 'root.default',
        properties,
        configData,
        stagedChanges: [],
      });

      const context = vi.mocked(runFieldValidation).mock.calls[0][0];
      expect(context.config.has('yarn.scheduler.capacity.root.default.state')).toBe(false);
    });

    it('should use property value from properties object when provided', () => {
      const configData = new Map([['yarn.scheduler.capacity.root.default.capacity', '50']]);
      const properties = {
        capacity: '60',
      };

      vi.mocked(runFieldValidation).mockReturnValue([]);

      validateQueue({
        queuePath: 'root.default',
        properties,
        configData,
        stagedChanges: [],
      });

      const context = vi.mocked(runFieldValidation).mock.calls[0][0];
      expect(context.fieldValue).toBe('60');
    });

    it('should use config value when property not in properties object', () => {
      const configData = new Map([['yarn.scheduler.capacity.root.default.capacity', '50']]);
      const properties = {
        'maximum-capacity': '100',
      };

      vi.mocked(runFieldValidation).mockReturnValue([]);

      validateQueue({
        queuePath: 'root.default',
        properties,
        configData,
        stagedChanges: [],
      });

      // Find the call for capacity
      const calls = vi.mocked(runFieldValidation).mock.calls;
      const capacityCall = calls.find((call) => call[0].fieldName === 'capacity');
      expect(capacityCall![0].fieldValue).toBe('50');
    });

    it('should use empty string when property not in properties or config', () => {
      const configData = new Map();
      const properties = {
        'maximum-capacity': '100',
      };

      vi.mocked(runFieldValidation).mockReturnValue([]);

      validateQueue({
        queuePath: 'root.default',
        properties,
        configData,
        stagedChanges: [],
      });

      // Find the call for capacity
      const calls = vi.mocked(runFieldValidation).mock.calls;
      const capacityCall = calls.find((call) => call[0].fieldName === 'capacity');
      expect(capacityCall![0].fieldValue).toBe('');
    });

    it('should deduplicate validation issues', () => {
      const configData = new Map();
      const properties = {
        capacity: '50',
        'maximum-capacity': '100',
      };

      vi.mocked(runFieldValidation)
        .mockReturnValueOnce([
          {
            queuePath: 'root.default',
            field: 'capacity',
            message: 'Duplicate error',
            severity: 'error',
            rule: 'test-rule',
          },
        ])
        .mockReturnValueOnce([
          {
            queuePath: 'root.default',
            field: 'capacity',
            message: 'Duplicate error',
            severity: 'error',
            rule: 'test-rule',
          },
        ]);

      const result = validateQueue({
        queuePath: 'root.default',
        properties,
        configData,
        stagedChanges: [],
      });

      expect(result.issues).toHaveLength(1);
    });

    it('should return invalid when any property has errors', () => {
      const configData = new Map();
      const properties = {
        capacity: '50',
        'maximum-capacity': '40',
      };

      vi.mocked(runFieldValidation)
        .mockReturnValueOnce([])
        .mockReturnValueOnce([
          {
            queuePath: 'root.default',
            field: 'maximum-capacity',
            message: 'Maximum capacity must be >= capacity',
            severity: 'error',
            rule: 'max-capacity-minimum',
          },
        ]);

      const result = validateQueue({
        queuePath: 'root.default',
        properties,
        configData,
        stagedChanges: [],
      });

      expect(result.valid).toBe(false);
      expect(result.issues).toHaveLength(1);
    });

    it('should return valid when only warnings exist', () => {
      const configData = new Map();
      const properties = {
        capacity: '50',
      };

      vi.mocked(runFieldValidation).mockReturnValue([
        {
          queuePath: 'root.default',
          field: 'capacity',
          message: 'Warning message',
          severity: 'warning',
          rule: 'test-warning',
        },
      ]);

      const result = validateQueue({
        queuePath: 'root.default',
        properties,
        configData,
        stagedChanges: [],
      });

      expect(result.valid).toBe(true);
      expect(result.issues).toHaveLength(1);
    });

    it('should merge staged changes into config', () => {
      const configData = new Map();
      const properties = {
        capacity: '50',
      };
      const stagedChanges: StagedChange[] = [
        {
          id: '1',
          type: 'update',
          queuePath: 'root.production',
          property: 'capacity',
          oldValue: '50',
          newValue: '60',
          timestamp: Date.now(),
        },
      ];

      vi.mocked(runFieldValidation).mockReturnValue([]);

      validateQueue({
        queuePath: 'root.default',
        properties,
        configData,
        stagedChanges,
      });

      expect(mergeStagedConfig).toHaveBeenCalledWith(configData, stagedChanges);
    });
  });

  describe('hasBlockingIssues', () => {
    it('should return true when issues contain errors', () => {
      const issues: ValidationIssue[] = [
        {
          queuePath: 'root.default',
          field: 'capacity',
          message: 'Error message',
          severity: 'error',
          rule: 'test-rule',
        },
      ];

      const result = hasBlockingIssues(issues);

      expect(result).toBe(true);
    });

    it('should return false when issues contain only warnings', () => {
      const issues: ValidationIssue[] = [
        {
          queuePath: 'root.default',
          field: 'capacity',
          message: 'Warning message',
          severity: 'warning',
          rule: 'test-rule',
        },
      ];

      const result = hasBlockingIssues(issues);

      expect(result).toBe(false);
    });

    it('should return false when issues array is empty', () => {
      const issues: ValidationIssue[] = [];

      const result = hasBlockingIssues(issues);

      expect(result).toBe(false);
    });

    it('should return true when mixed errors and warnings exist', () => {
      const issues: ValidationIssue[] = [
        {
          queuePath: 'root.default',
          field: 'capacity',
          message: 'Warning message',
          severity: 'warning',
          rule: 'test-warning',
        },
        {
          queuePath: 'root.default',
          field: 'capacity',
          message: 'Error message',
          severity: 'error',
          rule: 'test-error',
        },
      ];

      const result = hasBlockingIssues(issues);

      expect(result).toBe(true);
    });
  });

  describe('splitIssues', () => {
    it('should split errors and warnings correctly', () => {
      const issues: ValidationIssue[] = [
        {
          queuePath: 'root.default',
          field: 'capacity',
          message: 'Error message',
          severity: 'error',
          rule: 'test-error',
        },
        {
          queuePath: 'root.default',
          field: 'state',
          message: 'Warning message',
          severity: 'warning',
          rule: 'test-warning',
        },
      ];

      const result = splitIssues(issues);

      expect(result.errors).toHaveLength(1);
      expect(result.warnings).toHaveLength(1);
      expect(result.errors[0].severity).toBe('error');
      expect(result.warnings[0].severity).toBe('warning');
    });

    it('should handle all errors', () => {
      const issues: ValidationIssue[] = [
        {
          queuePath: 'root.default',
          field: 'capacity',
          message: 'Error 1',
          severity: 'error',
          rule: 'test-error-1',
        },
        {
          queuePath: 'root.default',
          field: 'state',
          message: 'Error 2',
          severity: 'error',
          rule: 'test-error-2',
        },
      ];

      const result = splitIssues(issues);

      expect(result.errors).toHaveLength(2);
      expect(result.warnings).toHaveLength(0);
    });

    it('should handle all warnings', () => {
      const issues: ValidationIssue[] = [
        {
          queuePath: 'root.default',
          field: 'capacity',
          message: 'Warning 1',
          severity: 'warning',
          rule: 'test-warning-1',
        },
        {
          queuePath: 'root.default',
          field: 'state',
          message: 'Warning 2',
          severity: 'warning',
          rule: 'test-warning-2',
        },
      ];

      const result = splitIssues(issues);

      expect(result.errors).toHaveLength(0);
      expect(result.warnings).toHaveLength(2);
    });

    it('should handle empty array', () => {
      const issues: ValidationIssue[] = [];

      const result = splitIssues(issues);

      expect(result.errors).toHaveLength(0);
      expect(result.warnings).toHaveLength(0);
    });
  });
});
