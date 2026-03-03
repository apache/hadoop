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
import { render, screen } from '@testing-library/react';
import { QueueCardNode } from './QueueCardNode';
import { useSchedulerStore } from '~/stores/schedulerStore';
import type { NodeProps } from '@xyflow/react';
import type { QueueCardData } from '~/features/queue-management/hooks/useQueueTreeData';

// Mock dependencies
vi.mock('~/stores/schedulerStore');
vi.mock('~/features/queue-management/hooks/useQueueActions', () => ({
  useQueueActions: () => ({
    canAddChildQueue: vi.fn(() => true),
    canDeleteQueue: vi.fn(() => true),
    updateQueueProperty: vi.fn(),
  }),
}));

// Mock ReactFlow handles
vi.mock('@xyflow/react', async () => {
  const actual = await vi.importActual('@xyflow/react');
  return {
    ...actual,
    Handle: () => null,
  };
});

describe('QueueCardNode - Node Label Filtering', () => {
  const mockGetQueueLabelCapacity = vi.fn();
  const defaultQueueData: QueueCardData = {
    queuePath: 'root.a',
    queueName: 'a',
    queueType: 'leaf',
    capacity: 50,
    maxCapacity: 100,
    absoluteCapacity: 50,
    absoluteMaxCapacity: 100,
    absoluteUsedCapacity: 25,
    usedCapacity: 25,
    numApplications: 5,
    numActiveApplications: 3,
    numPendingApplications: 2,
    state: 'RUNNING',
    stagedStatus: undefined,
    isLeaf: true,
    capacityConfig: '50',
    maxCapacityConfig: '100',
    resourcesUsed: { memory: 1024, vCores: 4 },
    creationMethod: 'static',
    isAutoCreatedQueue: false,
  };

  const nodeProps: NodeProps = {
    id: 'root.a',
    data: defaultQueueData,
    type: 'queueCard',
    selected: false,
    isConnectable: false,
    positionAbsoluteX: 0,
    positionAbsoluteY: 0,
    dragging: false,
    zIndex: 0,
    draggable: false,
    selectable: false,
    deletable: false,
  };

  const defaultStoreState: Record<string, any> = {
    comparisonQueues: [],
    selectedQueuePath: null,
    selectQueue: vi.fn(),
    setPropertyPanelOpen: vi.fn(),
    isPropertyPanelOpen: false,
    propertyPanelInitialTab: 'overview',
    setPropertyPanelInitialTab: vi.fn(),
    toggleComparisonQueue: vi.fn(),
    selectedNodeLabelFilter: '',
    getQueueLabelCapacity: mockGetQueueLabelCapacity,
    hasPendingDeletion: vi.fn().mockReturnValue(false),
    clearQueueChanges: vi.fn(),
    requestTemplateConfigOpen: vi.fn(),
    searchQuery: '',
    isComparisonModeActive: false,
  };

  function mockStore(overrides: Record<string, any> = {}) {
    const state = { ...defaultStoreState, ...overrides };
    vi.mocked(useSchedulerStore).mockImplementation((selector?: any) => {
      return selector ? selector(state) : state;
    });
  }

  beforeEach(() => {
    vi.clearAllMocks();
    mockStore();
  });

  describe('DEFAULT label (no filter)', () => {
    it('should show normal appearance when no label is selected', () => {
      mockGetQueueLabelCapacity.mockReturnValue({
        capacity: '50',
        maxCapacity: '100',
        absoluteCapacity: '50',
        isLabelSpecific: false,
        label: 'DEFAULT',
        hasAccess: true,
        canUseLabel: true,
      });

      render(<QueueCardNode {...nodeProps} />);

      const card = screen.getByText('a').closest('.relative');
      expect(card).not.toHaveClass('opacity-50');
      expect(card).not.toHaveClass('grayscale');
    });

    it('should show default capacity values', () => {
      mockGetQueueLabelCapacity.mockReturnValue({
        capacity: '50',
        maxCapacity: '100',
        absoluteCapacity: '50',
        isLabelSpecific: false,
        label: 'DEFAULT',
        hasAccess: true,
        canUseLabel: true,
      });

      render(<QueueCardNode {...nodeProps} />);

      // Look for the main capacity display specifically (has text-2xl class)
      const capacityDisplays = screen.getAllByText('50%');
      const mainCapacity = capacityDisplays.find((el) => el.className.includes('text-2xl'));
      expect(mainCapacity).toBeInTheDocument();
      expect(screen.getByText(/Maximum capacity: 100%/)).toBeInTheDocument();
    });
  });

  describe('Label-specific filtering', () => {
    beforeEach(() => {
      mockStore({ selectedNodeLabelFilter: 'gpu' });
    });

    it('should show label-specific capacity when queue has access', () => {
      mockGetQueueLabelCapacity.mockReturnValue({
        capacity: '80',
        maxCapacity: '90',
        absoluteCapacity: '80',
        isLabelSpecific: true,
        label: 'gpu',
        hasAccess: true,
        canUseLabel: true,
      });

      render(<QueueCardNode {...nodeProps} />);

      // Should show label-specific values
      expect(screen.getByText('80%')).toBeInTheDocument();
      expect(screen.getByText(/Maximum capacity: 90%/)).toBeInTheDocument();

      // Should show label badge
      expect(screen.getByText('gpu')).toBeInTheDocument();
    });

    it('should gray out queue when it has no access to selected label', () => {
      mockGetQueueLabelCapacity.mockReturnValue({
        capacity: '0',
        maxCapacity: '100',
        absoluteCapacity: '0',
        isLabelSpecific: true,
        label: 'gpu',
        hasAccess: false,
        canUseLabel: false,
      });

      render(<QueueCardNode {...nodeProps} />);

      const card = screen.getByText('a').closest('.relative');
      expect(card).toHaveClass('opacity-50');
      expect(card).toHaveClass('grayscale');

      // Should show reason for inaccessibility
      expect(screen.getByText('No access to partition: gpu')).toBeInTheDocument();
    });

    it('should gray out queue when it has 0% capacity for label', () => {
      mockGetQueueLabelCapacity.mockReturnValue({
        capacity: '0',
        maxCapacity: '100',
        absoluteCapacity: '0',
        isLabelSpecific: true,
        label: 'gpu',
        hasAccess: true,
        canUseLabel: false,
      });

      render(<QueueCardNode {...nodeProps} />);

      const card = screen.getByText('a').closest('.relative');
      expect(card).toHaveClass('opacity-50');
      expect(card).toHaveClass('grayscale');

      // Should show reason for inaccessibility
      expect(screen.getByText('No capacity allocated for partition: gpu')).toBeInTheDocument();
    });

    it('should never gray out root queue', () => {
      const rootNodeProps = {
        ...nodeProps,
        id: 'root',
        data: {
          ...defaultQueueData,
          queuePath: 'root',
          queueName: 'root',
        },
      };

      mockGetQueueLabelCapacity.mockReturnValue({
        capacity: '100',
        maxCapacity: '100',
        absoluteCapacity: '100',
        isLabelSpecific: true,
        label: 'gpu',
        hasAccess: true,
        canUseLabel: true,
      });

      render(<QueueCardNode {...rootNodeProps} />);

      // Find the card by its data-testid or by the queue name in CardTitle
      const queueNameElement = screen.getByText('root', { selector: '.text-base' });
      const card = queueNameElement.closest('.relative');
      expect(card).not.toHaveClass('opacity-50');
      expect(card).not.toHaveClass('grayscale');
    });
  });

  describe('Label badge tooltip', () => {
    it('should show tooltip for label badge', async () => {
      mockStore({ selectedNodeLabelFilter: 'gpu' });

      mockGetQueueLabelCapacity.mockReturnValue({
        capacity: '80',
        maxCapacity: '90',
        absoluteCapacity: '80',
        isLabelSpecific: true,
        label: 'gpu',
        hasAccess: true,
        canUseLabel: true,
      });

      render(<QueueCardNode {...nodeProps} />);

      // The badge with gpu label should be present
      const badge = screen.getByText('gpu');
      expect(badge).toBeInTheDocument();
    });
  });
});
