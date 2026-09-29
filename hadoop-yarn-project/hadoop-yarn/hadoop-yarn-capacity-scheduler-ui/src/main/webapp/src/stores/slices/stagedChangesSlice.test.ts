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


import { describe, it, expect, vi } from 'vitest';
import { createStagedChangesSlice } from './stagedChangesSlice';
import type { StagedChange } from '~/types';
import { validateStagedChanges } from '~/features/validation/crossQueue';

// Mock dependencies
vi.mock('~/features/validation/crossQueue');
vi.mock('~/features/validation/utils/configUtils', () => ({
  getMergedConfigData: vi.fn((configData) => configData),
}));

beforeEach(() => {
  vi.resetAllMocks();
});

describe('stagedChangesSlice - validation refresh', () => {
  it('should refresh validation errors for all staged changes', () => {
    const state = {
      stagedChanges: [
        {
          id: '1',
          type: 'update' as const,
          queuePath: 'root.parent.child1',
          property: 'capacity',
          oldValue: '50%',
          newValue: '52%',
          timestamp: Date.now(),
        },
      ] as StagedChange[],
      configData: new Map([
        ['yarn.scheduler.capacity.legacy-queue-mode.enabled', 'true'],
        ['yarn.scheduler.capacity.root.parent.capacity', '100%'],
        ['yarn.scheduler.capacity.root.parent.child1.capacity', '50%'],
        ['yarn.scheduler.capacity.root.parent.child2.capacity', '50%'],
      ]),
      schedulerData: {
        type: 'capacityScheduler',
        capacity: 100,
        usedCapacity: 0,
        maxCapacity: 100,
        queueName: 'root',
        queues: { queue: [] },
      } as any,
    };

    const mockGet = vi.fn(() => state);
    const mockSet = vi.fn((fn) => fn(state as any));

    vi.mocked(validateStagedChanges).mockReturnValue(
      new Map([
        [
          '1',
          [
            {
              queuePath: 'root.parent',
              field: 'capacity',
              message: 'Child queue capacities must sum to 100%',
              severity: 'error',
              rule: 'child-capacity-sum',
            },
          ],
        ],
      ]),
    );

    const slice = createStagedChangesSlice(mockSet as any, mockGet as any, {} as any);

    // Call refreshValidationErrors directly
    slice.refreshValidationErrors();

    // Verify the staged change was updated with validation errors
    expect(mockSet).toHaveBeenCalled();
    const updatedChanges = state.stagedChanges;
    expect(updatedChanges[0].validationErrors).toBeDefined();
    expect(updatedChanges[0].validationErrors).toContainEqual(
      expect.objectContaining({
        field: 'capacity',
        message: 'Child queue capacities must sum to 100%',
        rule: 'child-capacity-sum',
      }),
    );
  });

  it('should clear validation errors when they are resolved', () => {
    const state = {
      stagedChanges: [
        {
          id: '1',
          type: 'update' as const,
          queuePath: 'root.parent.child1',
          property: 'capacity',
          oldValue: '50%',
          newValue: '50%',
          timestamp: Date.now(),
          validationErrors: [
            {
              field: 'capacity',
              message: 'Child queue capacities must sum to 100%',
              severity: 'error',
              rule: 'child-capacity-sum',
            },
          ],
        },
      ] as StagedChange[],
      configData: new Map([['yarn.scheduler.capacity.legacy-queue-mode.enabled', 'true']]),
      schedulerData: {
        type: 'capacityScheduler',
        capacity: 100,
        usedCapacity: 0,
        maxCapacity: 100,
        queueName: 'root',
        queues: { queue: [] },
      } as any,
    };

    const mockGet = vi.fn(() => state);
    const mockSet = vi.fn((fn) => fn(state as any));

    vi.mocked(validateStagedChanges).mockReturnValue(new Map([['1', undefined]]));

    const slice = createStagedChangesSlice(mockSet as any, mockGet as any, {} as any);

    // Call refreshValidationErrors
    slice.refreshValidationErrors();

    // Verify validation errors were cleared
    expect(mockSet).toHaveBeenCalled();
    const updatedChanges = state.stagedChanges;
    expect(updatedChanges[0].validationErrors).toBeUndefined();
  });

  it('should handle absolute resource validation', () => {
    const state = {
      stagedChanges: [
        {
          id: '1',
          type: 'update' as const,
          queuePath: 'root.parent.child1',
          property: 'capacity',
          oldValue: '[memory=1024,vcores=4]',
          newValue: '[memory=3000,vcores=4]',
          timestamp: Date.now(),
        },
      ] as StagedChange[],
      configData: new Map([
        ['yarn.scheduler.capacity.root.parent.capacity', '[memory=2048,vcores=8]'],
        ['yarn.scheduler.capacity.root.parent.child1.capacity', '[memory=1024,vcores=4]'],
      ]),
      schedulerData: {
        type: 'capacityScheduler',
        capacity: 100,
        usedCapacity: 0,
        maxCapacity: 100,
        queueName: 'root',
        queues: { queue: [] },
      } as any,
    };

    const mockGet = vi.fn(() => state);
    const mockSet = vi.fn((fn) => fn(state as any));

    vi.mocked(validateStagedChanges).mockReturnValue(
      new Map([
        [
          '1',
          [
            {
              queuePath: 'root.parent.child1',
              field: 'capacity',
              message:
                'Child queue memory allocation (3000) cannot exceed parent queue memory allocation (2048)',
              severity: 'warning',
              rule: 'parent-child-capacity-constraint',
            },
          ],
        ],
      ]),
    );

    const slice = createStagedChangesSlice(mockSet as any, mockGet as any, {} as any);

    // Call refreshValidationErrors
    slice.refreshValidationErrors();

    // Verify the validation warning was attached
    const updatedChanges = state.stagedChanges;
    expect(updatedChanges[0].validationErrors).toContainEqual(
      expect.objectContaining({
        message: expect.stringContaining('memory allocation (3000)'),
        rule: 'parent-child-capacity-constraint',
        severity: 'warning',
      }),
    );
  });
});

describe('stagedChangesSlice - auto-creation enablement', () => {
  it('stops and restarts queues when enabling legacy auto-creation', async () => {
    const queuePath = 'root.default';
    const propertyKey = 'yarn.scheduler.capacity.root.default.auto-create-child-queue.enabled';

    const updateSchedulerConf = vi.fn().mockResolvedValue(undefined);
    const validateSchedulerConf = vi.fn().mockResolvedValue({
      validation: 'passed',
      versionId: '2',
    });
    const getSchedulerConf = vi.fn().mockResolvedValue({
      property: [{ name: propertyKey, value: 'true' }],
    });
    const getSchedulerConfVersion = vi.fn().mockResolvedValue({ versionId: '2' });

    const apiClient = {
      updateSchedulerConf,
      validateSchedulerConf,
      getSchedulerConf,
      getSchedulerConfVersion,
    } as any;

    const refreshSchedulerData = vi.fn().mockResolvedValue(undefined);

    const state: any = {
      apiClient,
      refreshSchedulerData,
      getChildQueues: () => [],
      schedulerData: {
        type: 'capacityScheduler',
        capacity: 100,
        usedCapacity: 0,
        maxCapacity: 100,
        queueName: 'root',
        queues: { queue: [] },
      },
      configData: new Map(),
      configVersion: 1,
      isLoading: false,
      error: null,
      errorContext: null,
      applyError: null,
    };

    const set = (fn: (draft: any) => void) => {
      fn(state);
    };
    const get = () => state;

    Object.assign(state, createStagedChangesSlice(set as any, get as any, {} as any));

    state.configData = new Map([[propertyKey, 'false']]);
    state.stagedChanges = [
      {
        id: '1',
        type: 'update' as const,
        queuePath,
        property: 'auto-create-child-queue.enabled',
        oldValue: 'false',
        newValue: 'true',
        timestamp: Date.now(),
      },
    ];

    await state.applyChanges();

    expect(validateSchedulerConf).toHaveBeenCalledTimes(1);
    expect(validateSchedulerConf).toHaveBeenCalledWith({
      'update-queue': [
        {
          'queue-name': queuePath,
          params: {
            entry: [{ key: 'auto-create-child-queue.enabled', value: 'true' }],
          },
        },
      ],
    });

    expect(updateSchedulerConf).toHaveBeenCalledTimes(3);

    expect(updateSchedulerConf.mock.calls[0][0]).toEqual({
      'update-queue': [
        {
          'queue-name': queuePath,
          params: {
            entry: [{ key: 'state', value: 'STOPPED' }],
          },
        },
      ],
    });

    const finalMutation = updateSchedulerConf.mock.calls[1][0];
    expect(finalMutation['update-queue']).toEqual([
      {
        'queue-name': queuePath,
        params: {
          entry: expect.arrayContaining([
            { key: 'auto-create-child-queue.enabled', value: 'true' },
          ]),
        },
      },
    ]);
    expect(finalMutation['global-updates']).toEqual([
      {
        entry: expect.arrayContaining([
          { key: 'yarn.webservice.mutation-api.version', value: '2' },
        ]),
      },
    ]);

    expect(updateSchedulerConf.mock.calls[2][0]).toEqual({
      'update-queue': [
        {
          'queue-name': queuePath,
          params: {
            entry: [{ key: 'state', value: 'RUNNING' }],
          },
        },
      ],
    });

    expect(refreshSchedulerData).toHaveBeenCalledTimes(1);
    expect(state.stagedChanges).toEqual([]);
    expect(state.configData.get(propertyKey)).toBe('true');
  });
});
