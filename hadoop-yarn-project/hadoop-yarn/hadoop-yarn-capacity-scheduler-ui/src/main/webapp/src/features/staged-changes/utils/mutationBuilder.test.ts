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
import { buildMutationRequest, groupChangesByQueue } from './mutationBuilder';
import type { StagedChange } from '~/types';

const CONFIG_BASE = 'yarn.scheduler.capacity';
const globalProp = (suffix: string) => `${CONFIG_BASE}.${suffix}`;
const MAXIMUM_APPLICATIONS_PROPERTY = globalProp('maximum-applications');

const toParamRecord = (params?: { entry: Array<{ key: string; value: string }> }) =>
  Object.fromEntries((params?.entry ?? []).map(({ key, value }) => [key, value]));

const toGlobalRecord = (
  global?: Array<{
    entry: Array<{ key: string; value: string }>;
  }>,
) =>
  Object.fromEntries(
    (global ?? []).flatMap(({ entry }) => entry).map(({ key, value }) => [key, value]),
  );

describe('mutationBuilder', () => {
  const now = Date.now();

  describe('buildMutationRequest', () => {
    it('should build complete mutation request with all change types', () => {
      const stagedChanges: StagedChange[] = [
        {
          id: '1',
          type: 'update',
          timestamp: now,
          queuePath: 'root.default',
          property: 'capacity',
          oldValue: '50',
          newValue: '60',
        },
        {
          id: '2',
          type: 'update',
          timestamp: now,
          queuePath: 'root.production',
          property: 'maximum-capacity',
          oldValue: '100',
          newValue: '80',
        },
        {
          id: '3',
          type: 'update',
          timestamp: now,
          queuePath: 'global',
          property: MAXIMUM_APPLICATIONS_PROPERTY,
          oldValue: '10000',
          newValue: '15000',
        },
        {
          id: '4',
          type: 'add',
          timestamp: now,
          queuePath: 'root.test',
          property: 'capacity',
          newValue: '20',
        },
        {
          id: '4a',
          type: 'add',
          timestamp: now,
          queuePath: 'root.test',
          property: 'state',
          newValue: 'RUNNING',
        },
        {
          id: '6',
          type: 'remove',
          timestamp: now,
          queuePath: 'root.old',
          property: '__queue__',
          oldValue: 'exists',
          newValue: undefined,
        },
      ];

      const request = buildMutationRequest(stagedChanges);

      const updateQueues = request['update-queue'] ?? [];
      expect(updateQueues).toHaveLength(2);
      expect(updateQueues[0]['queue-name']).toBe('root.default');
      expect(toParamRecord(updateQueues[0].params)).toEqual({ capacity: '60' });
      expect(updateQueues[1]['queue-name']).toBe('root.production');
      expect(toParamRecord(updateQueues[1].params)).toEqual({ 'maximum-capacity': '80' });

      const addQueues = request['add-queue'] ?? [];
      expect(addQueues).toHaveLength(1);
      expect(addQueues[0]['queue-name']).toBe('root.test');
      expect(toParamRecord(addQueues[0].params)).toEqual({ capacity: '20', state: 'RUNNING' });

      expect(request['remove-queue']).toBe('root.old');
      expect(toGlobalRecord(request['global-updates'])).toEqual({
        'yarn.scheduler.capacity.maximum-applications': '15000',
      });
    });

    it('should handle empty staged changes', () => {
      const request = buildMutationRequest([]);

      expect(request).toEqual({});
    });

    it('should handle add queue with multiple property changes', () => {
      const stagedChanges: StagedChange[] = [
        {
          id: '1',
          type: 'add',
          timestamp: now,
          queuePath: 'root.production.team2',
          property: 'capacity',
          newValue: '20',
        },
        {
          id: '2',
          type: 'add',
          timestamp: now,
          queuePath: 'root.production.team2',
          property: 'maximum-capacity',
          newValue: '50',
        },
        {
          id: '3',
          type: 'add',
          timestamp: now,
          queuePath: 'root.production.team2',
          property: 'state',
          newValue: 'RUNNING',
        },
      ];

      const request = buildMutationRequest(stagedChanges);

      expect(request['add-queue']).toHaveLength(1);
      const addQueueParams = request['add-queue']![0];
      expect(addQueueParams['queue-name']).toBe('root.production.team2');
      expect(toParamRecord(addQueueParams.params)).toEqual({
        capacity: '20',
        'maximum-capacity': '50',
        state: 'RUNNING',
      });
    });

    it('should group multiple properties for same queue', () => {
      const stagedChanges: StagedChange[] = [
        {
          id: '1',
          type: 'update',
          timestamp: now,
          queuePath: 'root.default',
          property: 'capacity',
          oldValue: '50',
          newValue: '60',
        },
        {
          id: '2',
          type: 'update',
          timestamp: now,
          queuePath: 'root.default',
          property: 'maximum-capacity',
          oldValue: '100',
          newValue: '90',
        },
        {
          id: '3',
          type: 'update',
          timestamp: now,
          queuePath: 'root.default',
          property: 'state',
          oldValue: 'RUNNING',
          newValue: 'STOPPED',
        },
      ];

      const request = buildMutationRequest(stagedChanges);

      expect(request['update-queue']).toHaveLength(1);
      const updateParams = request['update-queue']![0];
      expect(updateParams['queue-name']).toBe('root.default');
      expect(toParamRecord(updateParams.params)).toEqual({
        capacity: '60',
        'maximum-capacity': '90',
        state: 'STOPPED',
      });
    });

    it('should handle node label properties correctly', () => {
      const stagedChanges: StagedChange[] = [
        {
          id: '1',
          type: 'update',
          timestamp: now,
          queuePath: 'root.default',
          property: 'accessible-node-labels.gpu.capacity',
          oldValue: '30',
          newValue: '40',
        },
        {
          id: '2',
          type: 'update',
          timestamp: now,
          queuePath: 'root.default',
          property: 'accessible-node-labels.gpu.maximum-capacity',
          oldValue: '50',
          newValue: '60',
        },
        {
          id: '3',
          type: 'update',
          timestamp: now,
          queuePath: 'root.default',
          property: 'accessible-node-labels.ssd.capacity',
          oldValue: '20',
          newValue: '25',
        },
      ];

      const request = buildMutationRequest(stagedChanges);

      expect(request['update-queue']).toHaveLength(1);
      const params = request['update-queue']![0];
      expect(params['queue-name']).toBe('root.default');
      expect(toParamRecord(params.params)).toEqual({
        'accessible-node-labels.gpu.capacity': '40',
        'accessible-node-labels.gpu.maximum-capacity': '60',
        'accessible-node-labels.ssd.capacity': '25',
      });
    });

    it('should handle multiple global properties', () => {
      const stagedChanges: StagedChange[] = [
        {
          id: '1',
          type: 'update',
          timestamp: now,
          queuePath: 'global',
          property: MAXIMUM_APPLICATIONS_PROPERTY,
          oldValue: '10000',
          newValue: '15000',
        },
        {
          id: '2',
          type: 'update',
          timestamp: now,
          queuePath: 'global',
          property: globalProp('resource-calculator'),
          oldValue: 'org.apache.hadoop.yarn.util.resource.DefaultResourceCalculator',
          newValue: 'org.apache.hadoop.yarn.util.resource.DominantResourceCalculator',
        },
        {
          id: '3',
          type: 'update',
          timestamp: now,
          queuePath: 'global',
          property: globalProp('user-metrics.enable'),
          oldValue: 'false',
          newValue: 'true',
        },
      ];

      const request = buildMutationRequest(stagedChanges);

      expect(toGlobalRecord(request['global-updates'])).toEqual({
        'yarn.scheduler.capacity.maximum-applications': '15000',
        'yarn.scheduler.capacity.resource-calculator':
          'org.apache.hadoop.yarn.util.resource.DominantResourceCalculator',
        'yarn.scheduler.capacity.user-metrics.enable': 'true',
      });
    });
  });

  describe('groupChangesByQueue', () => {
    it('should group changes by queue path', () => {
      const changes: StagedChange[] = [
        {
          id: '1',
          type: 'update',
          timestamp: now,
          queuePath: 'root.default',
          property: 'capacity',
          oldValue: '50',
          newValue: '60',
        },
        {
          id: '2',
          type: 'update',
          timestamp: now,
          queuePath: 'root.production',
          property: 'capacity',
          oldValue: '50',
          newValue: '40',
        },
        {
          id: '3',
          type: 'update',
          timestamp: now,
          queuePath: 'root.default',
          property: 'maximum-capacity',
          oldValue: '100',
          newValue: '90',
        },
        {
          id: '4',
          type: 'update',
          timestamp: now,
          queuePath: 'global',
          property: MAXIMUM_APPLICATIONS_PROPERTY,
          oldValue: '10000',
          newValue: '15000',
        },
      ];

      const grouped = groupChangesByQueue(changes);

      expect(grouped.size).toBe(3);
      expect(grouped.get('root.default')).toHaveLength(2);
      expect(grouped.get('root.production')).toHaveLength(1);
      expect(grouped.get('global')).toHaveLength(1);
    });

    it('should handle empty changes array', () => {
      const grouped = groupChangesByQueue([]);
      expect(grouped.size).toBe(0);
    });

    it('should handle single change', () => {
      const changes: StagedChange[] = [
        {
          id: '1',
          type: 'update',
          timestamp: now,
          queuePath: 'root.only',
          property: 'capacity',
          oldValue: '50',
          newValue: '60',
        },
      ];

      const grouped = groupChangesByQueue(changes);

      expect(grouped.size).toBe(1);
      expect(grouped.get('root.only')).toHaveLength(1);
    });
  });

  describe('integration scenarios', () => {
    it('should handle complex real-world mutation scenario', () => {
      const stagedChanges: StagedChange[] = [
        // Update existing queue capacities
        {
          id: '1',
          type: 'update',
          timestamp: now,
          queuePath: 'root.default',
          property: 'capacity',
          oldValue: '40',
          newValue: '30',
        },
        {
          id: '2',
          type: 'update',
          timestamp: now,
          queuePath: 'root.production',
          property: 'capacity',
          oldValue: '40',
          newValue: '50',
        },
        {
          id: '3',
          type: 'update',
          timestamp: now,
          queuePath: 'root.production',
          property: 'maximum-capacity',
          oldValue: '60',
          newValue: '80',
        },
        // Add new queue with node label support
        {
          id: '4',
          type: 'add',
          timestamp: now,
          queuePath: 'root.ml',
          property: 'capacity',
          newValue: '20',
        },
        {
          id: '4a',
          type: 'add',
          timestamp: now,
          queuePath: 'root.ml',
          property: 'state',
          newValue: 'RUNNING',
        },
        {
          id: '4b',
          type: 'add',
          timestamp: now,
          queuePath: 'root.ml',
          property: 'accessible-node-labels.gpu.capacity',
          newValue: '100',
        },
        // Remove deprecated queue
        {
          id: '7',
          type: 'remove',
          timestamp: now,
          queuePath: 'root.deprecated',
          property: '__queue__',
          oldValue: undefined,
          newValue: undefined,
        },
        // Update global settings
        {
          id: '8',
          type: 'update',
          timestamp: now,
          queuePath: 'global',
          property: MAXIMUM_APPLICATIONS_PROPERTY,
          oldValue: '10000',
          newValue: '20000',
        },
        {
          id: '9',
          type: 'update',
          timestamp: now,
          queuePath: 'global',
          property: globalProp('resource-calculator'),
          oldValue: 'DefaultResourceCalculator',
          newValue: 'DominantResourceCalculator',
        },
      ];

      const request = buildMutationRequest(stagedChanges);

      const updateQueues = request['update-queue'] ?? [];
      expect(updateQueues).toHaveLength(2);
      expect(updateQueues[0]['queue-name']).toBe('root.default');
      expect(toParamRecord(updateQueues[0].params)).toEqual({ capacity: '30' });
      expect(updateQueues[1]['queue-name']).toBe('root.production');
      expect(toParamRecord(updateQueues[1].params)).toEqual({
        capacity: '50',
        'maximum-capacity': '80',
      });

      const addQueues = request['add-queue'] ?? [];
      expect(addQueues).toHaveLength(1);
      expect(addQueues[0]['queue-name']).toBe('root.ml');
      expect(toParamRecord(addQueues[0].params)).toEqual({
        capacity: '20',
        state: 'RUNNING',
        'accessible-node-labels.gpu.capacity': '100',
      });

      expect(request['remove-queue']).toBe('root.deprecated');
      expect(toGlobalRecord(request['global-updates'])).toEqual({
        'yarn.scheduler.capacity.maximum-applications': '20000',
        'yarn.scheduler.capacity.resource-calculator': 'DominantResourceCalculator',
      });
    });

    it('should handle edge case with only global changes', () => {
      const stagedChanges: StagedChange[] = [
        {
          id: '1',
          type: 'update',
          timestamp: now,
          queuePath: 'global',
          property: MAXIMUM_APPLICATIONS_PROPERTY,
          oldValue: '10000',
          newValue: '15000',
        },
      ];

      const request = buildMutationRequest(stagedChanges);

      expect(request['update-queue']).toBeUndefined();
      expect(request['add-queue']).toBeUndefined();
      expect(request['remove-queue']).toBeUndefined();
      expect(toGlobalRecord(request['global-updates'])).toEqual({
        'yarn.scheduler.capacity.maximum-applications': '15000',
      });
    });

    it('should handle edge case with only queue removals', () => {
      const stagedChanges: StagedChange[] = [
        {
          id: '1',
          type: 'remove',
          timestamp: now,
          queuePath: 'root.old1',
          property: '__queue__',
          oldValue: undefined,
          newValue: undefined,
        },
        {
          id: '2',
          type: 'remove',
          timestamp: now,
          queuePath: 'root.old2',
          property: '__queue__',
          oldValue: undefined,
          newValue: undefined,
        },
      ];

      const request = buildMutationRequest(stagedChanges);

      expect(request['update-queue']).toBeUndefined();
      expect(request['add-queue']).toBeUndefined();
      expect(request['remove-queue']).toEqual(['root.old1', 'root.old2']);
      expect(request['global-updates']).toBeUndefined();
    });
  });
});
