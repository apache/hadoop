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
import { mergeStagedConfig, getEffectivePropertyValue } from './configUtils';
import type { StagedChange } from '~/types';
import { SPECIAL_VALUES } from '~/types';

// Test helper
const getMockStagedChange = (overrides?: Partial<StagedChange>): StagedChange => {
  return {
    id: 'change-1',
    queuePath: 'root.default',
    property: 'capacity',
    oldValue: '10',
    newValue: '20',
    type: 'update',
    timestamp: Date.now(),
    ...overrides,
  };
};

describe('configUtils', () => {
  describe('mergeStagedConfig', () => {
    it('returns a copy when there are no staged changes', () => {
      const configData = new Map([
        ['yarn.scheduler.capacity.root.capacity', '100'],
        ['yarn.scheduler.capacity.root.queues', 'prod,dev'],
      ]);

      const result = mergeStagedConfig(configData, []);

      expect(result).toEqual(configData);
      expect(result).not.toBe(configData);
    });

    it('applies staged queue property changes', () => {
      const configData = new Map([
        ['yarn.scheduler.capacity.root.prod.capacity', '60'],
        ['yarn.scheduler.capacity.root.dev.capacity', '40'],
      ]);

      const stagedChanges: StagedChange[] = [
        getMockStagedChange({
          queuePath: 'root.prod',
          property: 'capacity',
          oldValue: '60',
          newValue: '70',
        }),
        getMockStagedChange({
          queuePath: 'root.dev',
          property: 'capacity',
          oldValue: '40',
          newValue: '30',
        }),
      ];

      const result = mergeStagedConfig(configData, stagedChanges);

      expect(result.get('yarn.scheduler.capacity.root.prod.capacity')).toBe('70');
      expect(result.get('yarn.scheduler.capacity.root.dev.capacity')).toBe('30');
    });

    it('applies staged global property changes', () => {
      const configData = new Map([['yarn.scheduler.capacity.legacy-queue-mode.enabled', 'true']]);

      const stagedChanges: StagedChange[] = [
        getMockStagedChange({
          queuePath: SPECIAL_VALUES.GLOBAL_QUEUE_PATH,
          property: SPECIAL_VALUES.LEGACY_MODE_PROPERTY,
          oldValue: 'true',
          newValue: 'false',
        }),
      ];

      const result = mergeStagedConfig(configData, stagedChanges);

      expect(result.get('yarn.scheduler.capacity.legacy-queue-mode.enabled')).toBe('false');
    });

    it('removes properties when staged value is empty', () => {
      const configData = new Map([['yarn.scheduler.capacity.root.prod.maximum-capacity', '100']]);

      const stagedChanges: StagedChange[] = [
        getMockStagedChange({
          queuePath: 'root.prod',
          property: 'maximum-capacity',
          oldValue: '100',
          newValue: '',
        }),
      ];

      const result = mergeStagedConfig(configData, stagedChanges);

      expect(result.has('yarn.scheduler.capacity.root.prod.maximum-capacity')).toBe(false);
    });

    it('handles multiple staged changes for same property (last wins)', () => {
      const configData = new Map([['yarn.scheduler.capacity.root.prod.capacity', '60']]);

      const stagedChanges: StagedChange[] = [
        getMockStagedChange({
          queuePath: 'root.prod',
          property: 'capacity',
          oldValue: '60',
          newValue: '70',
        }),
        getMockStagedChange({
          queuePath: 'root.prod',
          property: 'capacity',
          oldValue: '70',
          newValue: '80',
        }),
      ];

      const result = mergeStagedConfig(configData, stagedChanges);

      expect(result.get('yarn.scheduler.capacity.root.prod.capacity')).toBe('80');
    });

    it('includes properties from staged queue additions', () => {
      const configData = new Map([['yarn.scheduler.capacity.root.existing.capacity', '100']]);

      const stagedChanges: StagedChange[] = [
        getMockStagedChange({
          type: 'add',
          queuePath: 'root.new',
          property: 'capacity',
          oldValue: undefined,
          newValue: '25%',
        }),
      ];

      const result = mergeStagedConfig(configData, stagedChanges);

      expect(result.get('yarn.scheduler.capacity.root.new.capacity')).toBe('25%');
      expect(result.get('yarn.scheduler.capacity.root.existing.capacity')).toBe('100');
    });

    it('removes queue properties when staged removal is present', () => {
      const configData = new Map([
        ['yarn.scheduler.capacity.root.remove.capacity', '40%'],
        ['yarn.scheduler.capacity.root.remove.maximum-capacity', '60%'],
        ['yarn.scheduler.capacity.root.keep.capacity', '60%'],
      ]);

      const stagedChanges: StagedChange[] = [
        getMockStagedChange({
          type: 'remove',
          queuePath: 'root.remove',
          property: SPECIAL_VALUES.QUEUE_MARKER,
          oldValue: 'exists',
        }),
      ];

      const result = mergeStagedConfig(configData, stagedChanges);

      expect(result.has('yarn.scheduler.capacity.root.remove.capacity')).toBe(false);
      expect(result.has('yarn.scheduler.capacity.root.remove.maximum-capacity')).toBe(false);
      expect(result.get('yarn.scheduler.capacity.root.keep.capacity')).toBe('60%');
    });
  });

  describe('getEffectivePropertyValue', () => {
    it('returns staged value when available', () => {
      const configData = new Map([['yarn.scheduler.capacity.root.prod.capacity', '60']]);

      const stagedChanges: StagedChange[] = [
        getMockStagedChange({
          queuePath: 'root.prod',
          property: 'capacity',
          oldValue: '60',
          newValue: '70',
        }),
      ];

      const result = getEffectivePropertyValue(configData, stagedChanges, 'root.prod', 'capacity');

      expect(result).toBe('70');
    });

    it('returns config value when no staged change exists', () => {
      const configData = new Map([['yarn.scheduler.capacity.root.prod.capacity', '60']]);

      const result = getEffectivePropertyValue(configData, [], 'root.prod', 'capacity');

      expect(result).toBe('60');
    });

    it('returns empty string when property not found', () => {
      const configData = new Map();

      const result = getEffectivePropertyValue(configData, [], 'root.prod', 'capacity');

      expect(result).toBe('');
    });

    it('handles global properties', () => {
      const configData = new Map([['yarn.scheduler.capacity.legacy-queue-mode.enabled', 'true']]);

      const stagedChanges: StagedChange[] = [
        getMockStagedChange({
          queuePath: SPECIAL_VALUES.GLOBAL_QUEUE_PATH,
          property: SPECIAL_VALUES.LEGACY_MODE_PROPERTY,
          oldValue: 'true',
          newValue: 'false',
        }),
      ];

      const result = getEffectivePropertyValue(
        configData,
        stagedChanges,
        SPECIAL_VALUES.GLOBAL_QUEUE_PATH,
        SPECIAL_VALUES.LEGACY_MODE_PROPERTY,
      );

      expect(result).toBe('false');
    });
  });
});
