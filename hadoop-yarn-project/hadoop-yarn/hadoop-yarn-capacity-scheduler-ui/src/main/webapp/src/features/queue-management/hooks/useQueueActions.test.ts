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


import { beforeEach, describe, expect, it, vi } from 'vitest';
import { renderHook, act } from '@testing-library/react';
import type { QueueInfo } from '~/types';

const stageQueueAddition = vi.fn();
const stageQueueRemoval = vi.fn();
const stageQueueChange = vi.fn();

const queueMap: Record<string, QueueInfo | null> = {};

const getQueueByPath = vi.fn((queuePath: string) => queueMap[queuePath] ?? null);

const mockStore = {
  stageQueueAddition,
  stageQueueRemoval,
  stageQueueChange,
  getQueueByPath,
};

vi.mock('~/stores/schedulerStore', () => ({
  useSchedulerStore: (selector: (state: typeof mockStore) => unknown) => selector(mockStore),
}));

import { useQueueActions } from './useQueueActions';

const makeQueue = (overrides: Partial<QueueInfo> = {}): QueueInfo => ({
  queueType: 'leaf',
  queueName: 'analytics',
  queuePath: 'root.analytics',
  capacity: 0,
  usedCapacity: 0,
  maxCapacity: 100,
  absoluteCapacity: 0,
  absoluteUsedCapacity: 0,
  absoluteMaxCapacity: 100,
  numApplications: 0,
  numActiveApplications: 0,
  numPendingApplications: 0,
  state: 'RUNNING',
  creationMethod: 'static',
  ...overrides,
});

describe('useQueueActions', () => {
  beforeEach(() => {
    stageQueueAddition.mockClear();
    stageQueueRemoval.mockClear();
    stageQueueChange.mockClear();
    getQueueByPath.mockClear();
    Object.keys(queueMap).forEach((key) => delete queueMap[key]);
  });

  it('throws when adding child with invalid name', () => {
    const { result } = renderHook(() => useQueueActions());

    expect(() => result.current.addChildQueue('root', 'bad.name', {})).toThrowError(
      'Queue name cannot contain dots',
    );
  });

  it('throws when parent queue is missing', () => {
    const { result } = renderHook(() => useQueueActions());

    expect(() => result.current.addChildQueue('root.missing', 'team', {})).toThrowError(
      'Parent queue not found',
    );
  });

  it('stages queue addition when parent exists', () => {
    queueMap['root'] = makeQueue({
      queueName: 'root',
      queuePath: 'root',
      queueType: 'parent',
      queues: { queue: [] },
    });

    const { result } = renderHook(() => useQueueActions());

    act(() => {
      result.current.addChildQueue('root', 'team', { capacity: '50' });
    });

    expect(stageQueueAddition).toHaveBeenCalledWith('root', 'team', { capacity: '50' });
  });

  it('prevents deleting the root queue', () => {
    const { result } = renderHook(() => useQueueActions());

    expect(() => result.current.deleteQueue('root')).toThrowError('Cannot delete root queue');
  });

  it('prevents deleting queues that still have children', () => {
    queueMap['root.analytics'] = makeQueue({
      queues: { queue: [makeQueue({ queueName: 'child', queuePath: 'root.analytics.child' })] },
    });

    const { result } = renderHook(() => useQueueActions());

    expect(() => result.current.deleteQueue('root.analytics')).toThrowError(
      'Cannot delete queue with children',
    );
  });

  it('stages queue removal for leaf queues', () => {
    queueMap['root.analytics'] = makeQueue();

    const { result } = renderHook(() => useQueueActions());

    act(() => {
      result.current.deleteQueue('root.analytics');
    });

    expect(stageQueueRemoval).toHaveBeenCalledWith('root.analytics');
  });

  it('protects against updates on missing queues', () => {
    queueMap['root.analytics'] = null;

    const { result } = renderHook(() => useQueueActions());

    expect(result.current.canAddChildQueue('root.analytics')).toBe(false);
    expect(result.current.canDeleteQueue('root.analytics')).toBe(false);
  });

  it('reflects queue presence in helper guards', () => {
    queueMap['root.analytics'] = makeQueue();
    queueMap['root.parent'] = makeQueue({
      queueName: 'parent',
      queuePath: 'root.parent',
      queues: { queue: [makeQueue({ queueName: 'child', queuePath: 'root.parent.child' })] },
    });

    const { result } = renderHook(() => useQueueActions());

    expect(result.current.canAddChildQueue('root.analytics')).toBe(true);
    expect(result.current.canDeleteQueue('root.analytics')).toBe(true);
    expect(result.current.canDeleteQueue('root.parent')).toBe(false);
    expect(result.current.canDeleteQueue('root')).toBe(false);
  });
});
