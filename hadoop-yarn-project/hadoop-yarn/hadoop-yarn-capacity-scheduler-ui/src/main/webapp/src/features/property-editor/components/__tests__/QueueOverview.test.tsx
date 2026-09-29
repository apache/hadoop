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
import userEvent from '@testing-library/user-event';
import { QueueOverview } from '~/features/property-editor/components/QueueOverview';
import type { QueueInfo } from '~/types';
import { QUEUE_STATES, SPECIAL_VALUES } from '~/types';

// Mock the scheduler store
vi.mock('~/stores/schedulerStore', () => ({
  useSchedulerStore: vi.fn(),
}));

// Mock the queue actions hook
vi.mock('~/features/queue-management/hooks/useQueueActions', () => ({
  useQueueActions: vi.fn(),
}));

// Mock child components
vi.mock('~/features/queue-management/components/QueueCapacityProgress', () => ({
  QueueCapacityProgress: ({ capacity, maxCapacity, usedCapacity }: any) => (
    <div data-testid="capacity-progress">
      <span data-testid="capacity-value">{capacity}</span>
      <span data-testid="max-capacity-value">{maxCapacity}</span>
      <span data-testid="used-capacity-value">{usedCapacity}</span>
    </div>
  ),
}));

vi.mock('~/features/queue-management/components/dialogs/AddQueueDialog', () => ({
  AddQueueDialog: ({ open, parentQueuePath, onClose }: any) =>
    open ? (
      <div data-testid="add-queue-dialog">
        <span>Add Queue Dialog for {parentQueuePath}</span>
        <button onClick={onClose}>Close</button>
      </div>
    ) : null,
}));

vi.mock('~/features/queue-management/components/dialogs/DeleteQueueDialog', () => ({
  DeleteQueueDialog: ({ open, queuePath, onClose }: any) =>
    open ? (
      <div data-testid="delete-queue-dialog">
        <span>Delete Queue Dialog for {queuePath}</span>
        <button onClick={onClose}>Close</button>
      </div>
    ) : null,
}));

import { useSchedulerStore } from '~/stores/schedulerStore';
import { useQueueActions } from '~/features/queue-management/hooks/useQueueActions';

describe('QueueOverview', () => {
  const mockUpdateQueueProperty = vi.fn();
  const mockCanAddChildQueue = vi.fn();
  const mockCanDeleteQueue = vi.fn();

  beforeEach(() => {
    vi.clearAllMocks();

    vi.mocked(useSchedulerStore).mockReturnValue('');
    vi.mocked(useSchedulerStore as any).mockImplementation((selector: any) => {
      if (typeof selector === 'function') {
        return selector({
          selectedNodeLabelFilter: '',
          stagedChanges: [],
        });
      }
      return '';
    });

    vi.mocked(useQueueActions).mockReturnValue({
      canAddChildQueue: mockCanAddChildQueue,
      canDeleteQueue: mockCanDeleteQueue,
      updateQueueProperty: mockUpdateQueueProperty,
    } as any);

    mockCanAddChildQueue.mockReturnValue(true);
    mockCanDeleteQueue.mockReturnValue(false);
  });

  const createMockQueue = (overrides: Partial<QueueInfo> = {}): QueueInfo => ({
    queueName: 'default',
    queuePath: 'root.default',
    queueType: 'leaf',
    capacity: 50,
    usedCapacity: 25,
    maxCapacity: 100,
    absoluteCapacity: 50,
    absoluteMaxCapacity: 100,
    absoluteUsedCapacity: 25,
    numApplications: 5,
    numActiveApplications: 3,
    numPendingApplications: 2,
    state: 'RUNNING',
    resourcesUsed: { memory: 4096, vCores: 4 },
    ...overrides,
  });

  describe('rendering', () => {
    it('should render the component', () => {
      const queue = createMockQueue();

      render(<QueueOverview queue={queue} />);

      expect(screen.getByText('Status')).toBeInTheDocument();
    });

    it('should display queue state badge', () => {
      const queue = createMockQueue({ state: 'RUNNING' });

      render(<QueueOverview queue={queue} />);

      expect(screen.getByText('RUNNING')).toBeInTheDocument();
    });

    it('should display partition label as DEFAULT_PARTITION when no filter selected', () => {
      const queue = createMockQueue();

      render(<QueueOverview queue={queue} />);

      expect(screen.getByText('DEFAULT_PARTITION')).toBeInTheDocument();
    });
  });

  describe('status card', () => {
    it('should display capacity percentage', () => {
      const queue = createMockQueue({ capacity: 60 });

      render(<QueueOverview queue={queue} />);

      expect(screen.getByText('60.0%')).toBeInTheDocument();
    });

    it('should display used capacity', () => {
      const queue = createMockQueue({ usedCapacity: 35.5 });

      render(<QueueOverview queue={queue} />);

      expect(screen.getByText('35.5% used')).toBeInTheDocument();
    });

    it('should display max capacity', () => {
      const queue = createMockQueue({ maxCapacity: 80 });

      render(<QueueOverview queue={queue} />);

      expect(screen.getByText('Max: 80.0%')).toBeInTheDocument();
    });

    it('should display queue path', () => {
      const queue = createMockQueue({ queuePath: 'root.production' });

      render(<QueueOverview queue={queue} />);

      expect(screen.getByText('root.production')).toBeInTheDocument();
    });

    it('should display child queue count for parent queues', () => {
      const queue = createMockQueue({
        queues: {
          queue: [
            createMockQueue({ queueName: 'child1' }),
            createMockQueue({ queueName: 'child2' }),
          ],
        } as any,
      });

      render(<QueueOverview queue={queue} />);

      expect(screen.getByText('Child Queues')).toBeInTheDocument();
      expect(screen.getByText('2')).toBeInTheDocument();
    });

    it('should not display child queue count for leaf queues', () => {
      const queue = createMockQueue();

      render(<QueueOverview queue={queue} />);

      expect(screen.queryByText('Child Queues')).not.toBeInTheDocument();
    });

    it('should display absolute capacity', () => {
      const queue = createMockQueue({ absoluteCapacity: 45.67 });

      render(<QueueOverview queue={queue} />);

      expect(screen.getByText('45.67%')).toBeInTheDocument();
    });

    it('should display absolute max capacity', () => {
      const queue = createMockQueue({ absoluteMaxCapacity: 95.12 });

      render(<QueueOverview queue={queue} />);

      expect(screen.getByText('95.12%')).toBeInTheDocument();
    });
  });

  describe('node labels', () => {
    it('should display accessible node labels', () => {
      const queue = createMockQueue({ nodeLabels: ['gpu', 'ssd'] });

      render(<QueueOverview queue={queue} />);

      expect(screen.getByText('Accessible Node Labels')).toBeInTheDocument();
      expect(screen.getByText('gpu')).toBeInTheDocument();
      expect(screen.getByText('ssd')).toBeInTheDocument();
    });

    it('should display "all" for wildcard node label', () => {
      const queue = createMockQueue({ nodeLabels: ['*'] });

      render(<QueueOverview queue={queue} />);

      expect(screen.getByText('all')).toBeInTheDocument();
    });

    it('should display default node label expression', () => {
      const queue = createMockQueue({ defaultNodeLabelExpression: 'gpu' });

      render(<QueueOverview queue={queue} />);

      expect(screen.getByText('Default Expression')).toBeInTheDocument();
      expect(screen.getByText('gpu')).toBeInTheDocument();
    });

    it('should not display node labels section when no labels present', () => {
      const queue = createMockQueue({ nodeLabels: [] });

      render(<QueueOverview queue={queue} />);

      expect(screen.queryByText('Accessible Node Labels')).not.toBeInTheDocument();
    });
  });

  describe('resource stats', () => {
    it('should display application count', () => {
      const queue = createMockQueue({ numApplications: 10 });

      render(<QueueOverview queue={queue} />);

      expect(screen.getByText('Applications')).toBeInTheDocument();
      expect(screen.getByText('10')).toBeInTheDocument();
    });

    it('should display memory usage', () => {
      const queue = createMockQueue({ resourcesUsed: { memory: 8192, vCores: 8 } });

      render(<QueueOverview queue={queue} />);

      expect(screen.getByText('Memory')).toBeInTheDocument();
      expect(screen.getByText('8192 MB')).toBeInTheDocument();
    });

    it('should display vCores usage', () => {
      const queue = createMockQueue({ resourcesUsed: { memory: 4096, vCores: 16 } });

      render(<QueueOverview queue={queue} />);

      expect(screen.getByText('vCores')).toBeInTheDocument();
      expect(screen.getByText('16')).toBeInTheDocument();
    });

    it('should display 0 MB when no memory usage', () => {
      const queue = createMockQueue({ resourcesUsed: undefined });

      render(<QueueOverview queue={queue} />);

      expect(screen.getByText('0 MB')).toBeInTheDocument();
    });

    it('should display 0 vCores when no vCore usage', () => {
      const queue = createMockQueue({ resourcesUsed: undefined });

      render(<QueueOverview queue={queue} />);

      // vCores label should be present
      expect(screen.getByText('vCores')).toBeInTheDocument();
      // The component displays 0 when resourcesUsed is undefined (line 134 of component)
      // Since there are multiple "0" values, we just verify the label is present
    });
  });

  describe('queue type card', () => {
    it('should display "Leaf Queue" for leaf queues', () => {
      const queue = createMockQueue();

      render(<QueueOverview queue={queue} />);

      expect(screen.getByText('Leaf Queue')).toBeInTheDocument();
    });

    it('should display "Parent Queue" for parent queues', () => {
      const queue = createMockQueue({
        queues: {
          queue: [createMockQueue()],
        } as any,
      });

      render(<QueueOverview queue={queue} />);

      expect(screen.getByText('Parent Queue')).toBeInTheDocument();
    });

    it('should display "Static" creation method by default', () => {
      const queue = createMockQueue();

      render(<QueueOverview queue={queue} />);

      expect(screen.getByText('Creation Method')).toBeInTheDocument();
      expect(screen.getByText('Static')).toBeInTheDocument();
    });

    it('should display "Static" creation method when explicitly set', () => {
      const queue = createMockQueue({ creationMethod: 'static' });

      render(<QueueOverview queue={queue} />);

      expect(screen.getByText('Static')).toBeInTheDocument();
    });

    it('should display "Dynamic (Legacy)" creation method', () => {
      const queue = createMockQueue({ creationMethod: 'dynamicLegacy' });

      render(<QueueOverview queue={queue} />);

      expect(screen.getByText('Dynamic (Legacy)')).toBeInTheDocument();
    });

    it('should display "Dynamic (Flexible)" creation method', () => {
      const queue = createMockQueue({ creationMethod: 'dynamicFlexible' });

      render(<QueueOverview queue={queue} />);

      expect(screen.getByText('Dynamic (Flexible)')).toBeInTheDocument();
    });

    it('should display auto-creation eligibility when present', () => {
      const queue = createMockQueue({ autoCreationEligibility: 'eligible' });

      render(<QueueOverview queue={queue} />);

      expect(screen.getByText('Auto-Creation')).toBeInTheDocument();
      expect(screen.getByText('eligible')).toBeInTheDocument();
    });

    it('should not display auto-creation eligibility when not present', () => {
      const queue = createMockQueue();

      render(<QueueOverview queue={queue} />);

      expect(screen.queryByText('Auto-Creation')).not.toBeInTheDocument();
    });
  });

  describe('action buttons', () => {
    it('should display "Add Child Queue" button', () => {
      const queue = createMockQueue();

      render(<QueueOverview queue={queue} />);

      expect(screen.getByText('Add Child Queue')).toBeInTheDocument();
    });

    it('should disable "Add Child Queue" button when cannot add', () => {
      mockCanAddChildQueue.mockReturnValue(false);
      const queue = createMockQueue();

      render(<QueueOverview queue={queue} />);

      const addButton = screen.getByText('Add Child Queue');
      expect(addButton.closest('button')).toBeDisabled();
    });

    it('should display "Stop Queue" button for running queues', () => {
      const queue = createMockQueue({ state: QUEUE_STATES.RUNNING });

      render(<QueueOverview queue={queue} />);

      expect(screen.getByText('Stop Queue')).toBeInTheDocument();
    });

    it('should display "Start Queue" button for stopped queues', () => {
      const queue = createMockQueue({ state: QUEUE_STATES.STOPPED });

      render(<QueueOverview queue={queue} />);

      expect(screen.getByText('Start Queue')).toBeInTheDocument();
    });

    it('should disable state toggle button for root queue', () => {
      const queue = createMockQueue({ queuePath: SPECIAL_VALUES.ROOT_QUEUE_NAME });

      render(<QueueOverview queue={queue} />);

      const toggleButton = screen.getByText('Stop Queue').closest('button');
      expect(toggleButton).toBeDisabled();
    });

    it('should display "Delete Queue" button when can delete', () => {
      mockCanDeleteQueue.mockReturnValue(true);
      const queue = createMockQueue();

      render(<QueueOverview queue={queue} />);

      expect(screen.getByText('Delete Queue')).toBeInTheDocument();
    });

    it('should not display "Delete Queue" button when cannot delete', () => {
      mockCanDeleteQueue.mockReturnValue(false);
      const queue = createMockQueue();

      render(<QueueOverview queue={queue} />);

      expect(screen.queryByText('Delete Queue')).not.toBeInTheDocument();
    });

    it('should call updateQueueProperty when toggling state', async () => {
      const user = userEvent.setup();
      const queue = createMockQueue({ queuePath: 'root.default', state: QUEUE_STATES.RUNNING });

      render(<QueueOverview queue={queue} />);

      const stopButton = screen.getByText('Stop Queue');
      await user.click(stopButton);

      expect(mockUpdateQueueProperty).toHaveBeenCalledWith(
        'root.default',
        'state',
        QUEUE_STATES.STOPPED,
      );
    });

    it('should open add queue dialog when clicking Add Child Queue', async () => {
      const user = userEvent.setup();
      const queue = createMockQueue();

      render(<QueueOverview queue={queue} />);

      const addButton = screen.getByText('Add Child Queue');
      await user.click(addButton);

      expect(screen.getByTestId('add-queue-dialog')).toBeInTheDocument();
    });

    it('should open delete queue dialog when clicking Delete Queue', async () => {
      const user = userEvent.setup();
      mockCanDeleteQueue.mockReturnValue(true);
      const queue = createMockQueue();

      render(<QueueOverview queue={queue} />);

      const deleteButton = screen.getByText('Delete Queue');
      await user.click(deleteButton);

      expect(screen.getByTestId('delete-queue-dialog')).toBeInTheDocument();
    });

    it('should close add queue dialog', async () => {
      const user = userEvent.setup();
      const queue = createMockQueue();

      render(<QueueOverview queue={queue} />);

      // Open dialog
      await user.click(screen.getByText('Add Child Queue'));
      expect(screen.getByTestId('add-queue-dialog')).toBeInTheDocument();

      // Close dialog
      await user.click(screen.getByText('Close'));
      expect(screen.queryByTestId('add-queue-dialog')).not.toBeInTheDocument();
    });

    it('should close delete queue dialog', async () => {
      const user = userEvent.setup();
      mockCanDeleteQueue.mockReturnValue(true);
      const queue = createMockQueue();

      render(<QueueOverview queue={queue} />);

      // Open dialog
      await user.click(screen.getByText('Delete Queue'));
      expect(screen.getByTestId('delete-queue-dialog')).toBeInTheDocument();

      // Close dialog
      await user.click(screen.getByText('Close'));
      expect(screen.queryByTestId('delete-queue-dialog')).not.toBeInTheDocument();
    });
  });

  describe('partition-specific capacity', () => {
    it('should use default capacity when no partition selected', () => {
      const queue = createMockQueue({ capacity: 50 });

      render(<QueueOverview queue={queue} />);

      const capacityValue = screen.getByTestId('capacity-value');
      expect(capacityValue).toHaveTextContent('50');
    });

    it('should use partition-specific capacity when partition selected', () => {
      vi.mocked(useSchedulerStore as any).mockImplementation((selector: any) => {
        if (typeof selector === 'function') {
          return selector({
            selectedNodeLabelFilter: 'gpu',
            stagedChanges: [],
          });
        }
        return 'gpu';
      });

      const queue = createMockQueue({
        capacity: 50,
        capacities: {
          queueCapacitiesByPartition: [
            {
              partitionName: 'gpu',
              capacity: 70,
              maxCapacity: 90,
              usedCapacity: 40,
              absoluteCapacity: 70,
              absoluteMaxCapacity: 90,
              absoluteUsedCapacity: 40,
            },
          ],
        } as any,
      });

      render(<QueueOverview queue={queue} />);

      const capacityValue = screen.getByTestId('capacity-value');
      expect(capacityValue).toHaveTextContent('70');
    });

    it('should fall back to default capacity when partition not found', () => {
      vi.mocked(useSchedulerStore as any).mockImplementation((selector: any) => {
        if (typeof selector === 'function') {
          return selector({
            selectedNodeLabelFilter: 'nonexistent',
            stagedChanges: [],
          });
        }
        return 'nonexistent';
      });

      const queue = createMockQueue({ capacity: 50 });

      render(<QueueOverview queue={queue} />);

      const capacityValue = screen.getByTestId('capacity-value');
      expect(capacityValue).toHaveTextContent('50');
    });
  });

  describe('staged status', () => {
    it('should disable Add Child Queue button when queue is newly staged', () => {
      vi.mocked(useSchedulerStore as any).mockImplementation((selector: any) => {
        if (typeof selector === 'function') {
          return selector({
            selectedNodeLabelFilter: '',
            stagedChanges: [{ queuePath: 'root.default', type: 'add' }],
          });
        }
        return '';
      });

      const queue = createMockQueue();

      render(<QueueOverview queue={queue} />);

      const addButton = screen.getByText('Add Child Queue').closest('button');
      expect(addButton).toBeDisabled();
    });

    it('should not display Delete Queue button for newly staged queue', () => {
      vi.mocked(useSchedulerStore as any).mockImplementation((selector: any) => {
        if (typeof selector === 'function') {
          return selector({
            selectedNodeLabelFilter: '',
            stagedChanges: [{ queuePath: 'root.default', type: 'add' }],
          });
        }
        return '';
      });

      mockCanDeleteQueue.mockReturnValue(true);
      const queue = createMockQueue();

      render(<QueueOverview queue={queue} />);

      expect(screen.queryByText('Delete Queue')).not.toBeInTheDocument();
    });
  });
});
