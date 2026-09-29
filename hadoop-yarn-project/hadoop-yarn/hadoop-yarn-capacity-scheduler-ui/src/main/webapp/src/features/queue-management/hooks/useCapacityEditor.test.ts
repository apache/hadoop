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

const openCapacityEditor = vi.fn();
const mockStore = {
  openCapacityEditor,
  selectedNodeLabelFilter: 'legacy',
};

vi.mock('~/stores/schedulerStore', () => ({
  useSchedulerStore: (selector: (state: typeof mockStore) => unknown) => selector(mockStore),
}));

import { useCapacityEditor } from './useCapacityEditor';

describe('useCapacityEditor', () => {
  beforeEach(() => {
    openCapacityEditor.mockClear();
    mockStore.selectedNodeLabelFilter = 'legacy';
  });

  it('resolves origin queue path when one is not supplied', () => {
    const { result } = renderHook(() => useCapacityEditor());

    act(() => {
      result.current.openCapacityEditor({
        origin: 'context-menu',
        parentQueuePath: 'root',
        originQueueName: 'analytics',
      });
    });

    expect(openCapacityEditor).toHaveBeenCalledWith(
      expect.objectContaining({
        originQueuePath: 'root.analytics',
        selectedNodeLabel: 'legacy',
      }),
    );
  });

  it('respects supplied queue path and label override', () => {
    const { result } = renderHook(() => useCapacityEditor());

    act(() => {
      result.current.openCapacityEditor({
        origin: 'add-queue',
        parentQueuePath: 'root.department',
        originQueuePath: 'root.department.new',
        originQueueName: 'new',
        selectedNodeLabel: 'sales',
        capacityValue: '10',
        maxCapacityValue: '50',
        markOriginAsNew: true,
        queueState: 'RUNNING',
      });
    });

    expect(openCapacityEditor).toHaveBeenCalledWith({
      origin: 'add-queue',
      parentQueuePath: 'root.department',
      originQueuePath: 'root.department.new',
      originQueueName: 'new',
      originQueueState: 'RUNNING',
      originInitialCapacity: '10',
      originInitialMaxCapacity: '50',
      originIsNew: true,
      selectedNodeLabel: 'sales',
    });
  });
});
