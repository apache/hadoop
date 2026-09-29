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


import { describe, it, expect, beforeEach } from 'vitest';
import { createSchedulerStore } from '~/stores/schedulerStore';

describe('searchSlice - node label filtering', () => {
  let store: ReturnType<typeof createSchedulerStore>;

  beforeEach(() => {
    store = createSchedulerStore({} as any);
    // Set up test config data
    store.setState({
      stagedChanges: [],
      configData: new Map([
        // Root queue has access to all labels
        ['yarn.scheduler.capacity.root.accessible-node-labels', '*'],

        // Queue A has explicit access to gpu label
        ['yarn.scheduler.capacity.root.a.accessible-node-labels', 'gpu'],
        ['yarn.scheduler.capacity.root.a.capacity', '50'],
        ['yarn.scheduler.capacity.root.a.accessible-node-labels.gpu.capacity', '80'],
        ['yarn.scheduler.capacity.root.a.accessible-node-labels.gpu.maximum-capacity', '100'],

        // Queue B has access to cpu and gpu labels
        ['yarn.scheduler.capacity.root.b.accessible-node-labels', 'cpu,gpu'],
        ['yarn.scheduler.capacity.root.b.capacity', '50'],
        ['yarn.scheduler.capacity.root.b.accessible-node-labels.cpu.capacity', '60'],
        ['yarn.scheduler.capacity.root.b.accessible-node-labels.gpu.capacity', '20'],

        // Queue C has no explicit accessible-node-labels (inherits from parent)
        ['yarn.scheduler.capacity.root.c.capacity', '30'],

        // Queue D has empty accessible-node-labels (only DEFAULT partition)
        ['yarn.scheduler.capacity.root.d.accessible-node-labels', ''],
        ['yarn.scheduler.capacity.root.d.capacity', '20'],

        // Queue E has 0% capacity for gpu label
        ['yarn.scheduler.capacity.root.e.accessible-node-labels', 'gpu'],
        ['yarn.scheduler.capacity.root.e.accessible-node-labels.gpu.capacity', '0'],
      ]),
    });
  });

  describe('selectNodeLabelFilter', () => {
    it('should update selected node label filter', () => {
      store.getState().selectNodeLabelFilter('gpu');
      expect(store.getState().selectedNodeLabelFilter).toBe('gpu');
    });

    it('should handle DEFAULT label (empty string)', () => {
      store.getState().selectNodeLabelFilter('');
      expect(store.getState().selectedNodeLabelFilter).toBe('');
    });
  });

  describe('getQueueAccessibility', () => {
    it('should return true for DEFAULT label for all queues', () => {
      expect(store.getState().getQueueAccessibility('root.a', '')).toBe(true);
      expect(store.getState().getQueueAccessibility('root.b', '')).toBe(true);
      expect(store.getState().getQueueAccessibility('root.d', '')).toBe(true);
    });

    it('should return true for root queue for any label', () => {
      expect(store.getState().getQueueAccessibility('root', 'gpu')).toBe(true);
      expect(store.getState().getQueueAccessibility('root', 'cpu')).toBe(true);
      expect(store.getState().getQueueAccessibility('root', 'any-label')).toBe(true);
    });

    it('should return true for queues with explicit access to label', () => {
      expect(store.getState().getQueueAccessibility('root.a', 'gpu')).toBe(true);
      expect(store.getState().getQueueAccessibility('root.b', 'cpu')).toBe(true);
      expect(store.getState().getQueueAccessibility('root.b', 'gpu')).toBe(true);
    });

    it('should return false for queues without access to label', () => {
      expect(store.getState().getQueueAccessibility('root.a', 'cpu')).toBe(false);
      expect(store.getState().getQueueAccessibility('root.d', 'gpu')).toBe(false);
    });

    it('should inherit access from parent when not configured', () => {
      // Queue C inherits from root, which has '*'
      expect(store.getState().getQueueAccessibility('root.c', 'gpu')).toBe(true);
      expect(store.getState().getQueueAccessibility('root.c', 'cpu')).toBe(true);
    });

    it('should handle wildcard (*) access', () => {
      expect(store.getState().getQueueAccessibility('root', 'any-label')).toBe(true);
    });

    it('should handle empty accessible-node-labels (only DEFAULT)', () => {
      // Queue D has empty accessible-node-labels
      expect(store.getState().getQueueAccessibility('root.d', '')).toBe(true);
      expect(store.getState().getQueueAccessibility('root.d', 'gpu')).toBe(false);
      expect(store.getState().getQueueAccessibility('root.d', 'cpu')).toBe(false);
    });
  });

  describe('getQueueLabelCapacity', () => {
    it('should return default capacity for DEFAULT label', () => {
      const capacity = store.getState().getQueueLabelCapacity('root.a', '');
      expect(capacity).toEqual({
        capacity: '50',
        maxCapacity: '100',
        absoluteCapacity: '0',
        isLabelSpecific: false,
        label: 'DEFAULT',
        hasAccess: true,
        canUseLabel: true,
      });
    });

    it('should return label-specific capacity when configured', () => {
      const capacity = store.getState().getQueueLabelCapacity('root.a', 'gpu');
      expect(capacity).toEqual({
        capacity: '80',
        maxCapacity: '100',
        absoluteCapacity: '0',
        isLabelSpecific: true,
        label: 'gpu',
        hasAccess: true,
        canUseLabel: true,
      });
    });

    it('should return 0% capacity for inaccessible labels', () => {
      const capacity = store.getState().getQueueLabelCapacity('root.a', 'cpu');
      expect(capacity).toEqual({
        capacity: '0',
        maxCapacity: '100',
        absoluteCapacity: '0',
        isLabelSpecific: true,
        label: 'cpu',
        hasAccess: false,
        canUseLabel: false,
      });
    });

    it('should handle queue with 0% capacity for accessible label', () => {
      const capacity = store.getState().getQueueLabelCapacity('root.e', 'gpu');
      expect(capacity).toEqual({
        capacity: '0',
        maxCapacity: '100',
        absoluteCapacity: '0',
        isLabelSpecific: true,
        label: 'gpu',
        hasAccess: true,
        canUseLabel: false, // Has access but can't use due to 0% capacity
      });
    });

    it('should use default max capacity when not specified', () => {
      const capacity = store.getState().getQueueLabelCapacity('root.b', 'cpu');
      expect(capacity?.maxCapacity).toBe('100');
    });

    it('should default root capacity to 100% for any label', () => {
      const capacity = store.getState().getQueueLabelCapacity('root', 'gpu');
      expect(capacity).toEqual({
        capacity: '100',
        maxCapacity: '100',
        absoluteCapacity: '0',
        isLabelSpecific: true,
        label: 'gpu',
        hasAccess: true,
        canUseLabel: true,
      });
    });
  });

  describe('staged changes support', () => {
    it('should reflect staged changes to accessible-node-labels', () => {
      // Initially, root.a only has access to gpu
      expect(store.getState().getQueueAccessibility('root.a', 'cpu')).toBe(false);

      // Stage a change to add cpu access
      store.setState({
        stagedChanges: [
          {
            id: '1',
            type: 'update',
            queuePath: 'root.a',
            property: 'accessible-node-labels',
            oldValue: 'gpu',
            newValue: 'gpu,cpu',
            timestamp: Date.now(),
          },
        ],
      });

      // Now it should have access to cpu
      expect(store.getState().getQueueAccessibility('root.a', 'cpu')).toBe(true);
      expect(store.getState().getQueueAccessibility('root.a', 'gpu')).toBe(true);
    });

    it('should reflect staged changes to label-specific capacity', () => {
      // Stage a change to gpu capacity
      store.setState({
        stagedChanges: [
          {
            id: '1',
            type: 'update',
            queuePath: 'root.a',
            property: 'accessible-node-labels.gpu.capacity',
            oldValue: '80',
            newValue: '90',
            timestamp: Date.now(),
          },
        ],
      });

      const capacity = store.getState().getQueueLabelCapacity('root.a', 'gpu');
      expect(capacity?.capacity).toBe('90'); // Staged value
    });
  });
});
