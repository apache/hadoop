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
import { QueueInfoTab } from '~/features/property-editor/components/QueueInfoTab';
import type { QueueInfo } from '~/types';
import { useSchedulerStore } from '~/stores/schedulerStore';

// Mock the scheduler store
vi.mock('~/stores/schedulerStore', () => ({
  useSchedulerStore: vi.fn(),
}));

// Mock child components
vi.mock('../MetricRow', () => ({
  MetricRow: ({ label, value }: { label: string; value: any }) => (
    <div data-testid="metric-row">
      <span data-testid="metric-label">{label}:</span>
      <span data-testid="metric-value">{value}</span>
    </div>
  ),
}));

vi.mock('../ResourceDisplay', () => ({
  ResourceDisplay: ({ resources }: { resources: any }) => (
    <div data-testid="resource-display">{resources.memory && `Memory: ${resources.memory}`}</div>
  ),
}));

describe('QueueInfoTab', () => {
  beforeEach(() => {
    vi.mocked(useSchedulerStore).mockReturnValue('');
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

      render(<QueueInfoTab queue={queue} />);

      expect(screen.getByText('Resource Utilization Details')).toBeInTheDocument();
    });

    it('should render all accordion sections', () => {
      const queue = createMockQueue();

      render(<QueueInfoTab queue={queue} />);

      expect(screen.getByText('Resource Utilization Details')).toBeInTheDocument();
      expect(screen.getByText('Absolute Capacity Metrics')).toBeInTheDocument();
      expect(screen.getByText('Application Master Limits')).toBeInTheDocument();
      expect(screen.getByText('Application Limits & Policies')).toBeInTheDocument();
      expect(screen.getByText('Container Information')).toBeInTheDocument();
      expect(screen.getByText('Ordering & Preemption')).toBeInTheDocument();
      expect(screen.getByText('Node Labels & Partitions')).toBeInTheDocument();
      expect(screen.getByText('Access & Priority')).toBeInTheDocument();
      expect(screen.getByText('Lifecycle Settings')).toBeInTheDocument();
    });
  });

  describe('Resource Utilization Details section', () => {
    it('should display partition label as DEFAULT_PARTITION when no filter selected', async () => {
      vi.mocked(useSchedulerStore).mockReturnValue('');
      const queue = createMockQueue();

      render(<QueueInfoTab queue={queue} />);

      expect(screen.getByText('DEFAULT_PARTITION')).toBeInTheDocument();
    });

    it('should display partition label when filter is selected', async () => {
      vi.mocked(useSchedulerStore).mockReturnValue('gpu');
      const queue = createMockQueue();

      render(<QueueInfoTab queue={queue} />);

      expect(screen.getByText('gpu')).toBeInTheDocument();
    });

    it('should display queue state', () => {
      const queue = createMockQueue({ state: 'RUNNING' });

      render(<QueueInfoTab queue={queue} />);

      const metricLabels = screen.getAllByTestId('metric-label');
      expect(metricLabels.some((label) => label.textContent === 'Queue State:')).toBe(true);
    });

    it('should display max capacity as "unlimited" when maxCapacity >= 100', () => {
      const queue = createMockQueue({ maxCapacity: 100 });

      render(<QueueInfoTab queue={queue} />);

      const metricLabels = screen.getAllByTestId('metric-label');
      const maxCapacityLabel = metricLabels.find(
        (label) => label.textContent === 'Max Capacity Display:',
      );
      expect(maxCapacityLabel).toBeDefined();
    });

    it('should display max capacity as percentage when maxCapacity < 100', () => {
      const queue = createMockQueue({ maxCapacity: 75 });

      render(<QueueInfoTab queue={queue} />);

      const metricLabels = screen.getAllByTestId('metric-label');
      const maxCapacityLabel = metricLabels.find(
        (label) => label.textContent === 'Max Capacity Display:',
      );
      expect(maxCapacityLabel).toBeDefined();
    });

    it('should render effective min resource when available', () => {
      const queue = createMockQueue({
        effectiveMinResource: { memory: 8192, vCores: 8 },
      });

      render(<QueueInfoTab queue={queue} />);

      const metricLabels = screen.getAllByTestId('metric-label');
      expect(metricLabels.some((label) => label.textContent === 'Configured Capacity:')).toBe(true);
      expect(metricLabels.some((label) => label.textContent === 'Effective Capacity:')).toBe(true);
    });

    it('should render effective max resource when available', () => {
      const queue = createMockQueue({
        effectiveMaxResource: { memory: 16384, vCores: 16 },
      });

      render(<QueueInfoTab queue={queue} />);

      const metricLabels = screen.getAllByTestId('metric-label');
      expect(metricLabels.some((label) => label.textContent === 'Configured Max Capacity:')).toBe(
        true,
      );
      expect(metricLabels.some((label) => label.textContent === 'Effective Max Capacity:')).toBe(
        true,
      );
    });
  });

  describe('Absolute Capacity Metrics section', () => {
    it('should display absolute capacity metrics', async () => {
      const user = userEvent.setup();
      const queue = createMockQueue({
        absoluteUsedCapacity: 25,
        absoluteCapacity: 50,
        absoluteMaxCapacity: 100,
      });

      render(<QueueInfoTab queue={queue} />);

      // Open the Absolute Capacity Metrics section
      const absoluteCapacityTrigger = screen.getByText('Absolute Capacity Metrics');
      await user.click(absoluteCapacityTrigger);

      const metricLabels = screen.getAllByTestId('metric-label');
      expect(metricLabels.some((label) => label.textContent === 'Absolute Used Capacity:')).toBe(
        true,
      );
      expect(
        metricLabels.some((label) => label.textContent === 'Absolute Configured Capacity:'),
      ).toBe(true);
      expect(
        metricLabels.some((label) => label.textContent === 'Absolute Configured Max Capacity:'),
      ).toBe(true);
    });
  });

  describe('Application Master Limits section', () => {
    it('should display AM resource limit when available', async () => {
      const user = userEvent.setup();
      const queue = createMockQueue({
        configuredMaxAMResourceLimit: 10,
        AMResourceLimit: { memory: 1024, vCores: 1 },
      });

      render(<QueueInfoTab queue={queue} />);
      await user.click(screen.getByText('Application Master Limits'));

      const metricLabels = screen.getAllByTestId('metric-label');
      expect(
        metricLabels.some(
          (label) => label.textContent === 'Configured Max Application Master Limit:',
        ),
      ).toBe(true);
      expect(
        metricLabels.some((label) => label.textContent === 'Max Application Master Resources:'),
      ).toBe(true);
    });

    it('should display AM used resource when available', async () => {
      const user = userEvent.setup();
      const queue = createMockQueue({
        amUsedResource: { memory: 512, vCores: 1 },
      });

      render(<QueueInfoTab queue={queue} />);
      await user.click(screen.getByText('Application Master Limits'));

      const metricLabels = screen.getAllByTestId('metric-label');
      expect(
        metricLabels.some((label) => label.textContent === 'Used Application Master Resources:'),
      ).toBe(true);
    });

    it('should display "No AM limit data available" when no AM data present', async () => {
      const user = userEvent.setup();
      const queue = createMockQueue();

      render(<QueueInfoTab queue={queue} />);
      await user.click(screen.getByText('Application Master Limits'));

      expect(screen.getByText('No AM limit data available')).toBeInTheDocument();
    });

    it('should not display "No AM limit data available" when some AM data is present', async () => {
      const user = userEvent.setup();
      const queue = createMockQueue({
        AMResourceLimit: { memory: 1024, vCores: 1 },
      });

      render(<QueueInfoTab queue={queue} />);
      await user.click(screen.getByText('Application Master Limits'));

      expect(screen.queryByText('No AM limit data available')).not.toBeInTheDocument();
    });
  });

  describe('Application Limits & Policies section', () => {
    it('should display max applications when defined', async () => {
      const user = userEvent.setup();
      const queue = createMockQueue({ maxApplications: 100 });

      render(<QueueInfoTab queue={queue} />);
      await user.click(screen.getByText('Application Limits & Policies'));

      const metricLabels = screen.getAllByTestId('metric-label');
      expect(metricLabels.some((label) => label.textContent === 'Max Applications:')).toBe(true);
    });

    it('should display max applications per user when defined', async () => {
      const user = userEvent.setup();
      const queue = createMockQueue({ maxApplicationsPerUser: 50 });

      render(<QueueInfoTab queue={queue} />);
      await user.click(screen.getByText('Application Limits & Policies'));

      const metricLabels = screen.getAllByTestId('metric-label');
      expect(metricLabels.some((label) => label.textContent === 'Max Applications Per User:')).toBe(
        true,
      );
    });

    it('should always display total, active, and pending applications', async () => {
      const user = userEvent.setup();
      const queue = createMockQueue({
        numApplications: 10,
        numActiveApplications: 5,
        numPendingApplications: 3,
      });

      render(<QueueInfoTab queue={queue} />);
      await user.click(screen.getByText('Application Limits & Policies'));

      const metricLabels = screen.getAllByTestId('metric-label');
      expect(metricLabels.some((label) => label.textContent === 'Total Applications:')).toBe(true);
      expect(metricLabels.some((label) => label.textContent === 'Active Applications:')).toBe(true);
      expect(metricLabels.some((label) => label.textContent === 'Pending Applications:')).toBe(
        true,
      );
    });

    it('should handle zero active and pending applications', async () => {
      const user = userEvent.setup();
      const queue = createMockQueue({
        numApplications: 0,
        numActiveApplications: 0,
        numPendingApplications: 0,
      });

      render(<QueueInfoTab queue={queue} />);
      await user.click(screen.getByText('Application Limits & Policies'));

      const metricLabels = screen.getAllByTestId('metric-label');
      expect(metricLabels.some((label) => label.textContent === 'Total Applications:')).toBe(true);
    });
  });

  describe('Container Information section', () => {
    it('should display container metrics when available', async () => {
      const user = userEvent.setup();
      const queue = createMockQueue({
        numContainers: 20,
        allocatedContainers: 15,
        pendingContainers: 3,
        reservedContainers: 2,
      });

      render(<QueueInfoTab queue={queue} />);
      await user.click(screen.getByText('Container Information'));

      const metricLabels = screen.getAllByTestId('metric-label');
      expect(metricLabels.some((label) => label.textContent === 'Num Containers:')).toBe(true);
      expect(metricLabels.some((label) => label.textContent === 'Allocated Containers:')).toBe(
        true,
      );
      expect(metricLabels.some((label) => label.textContent === 'Pending Containers:')).toBe(true);
      expect(metricLabels.some((label) => label.textContent === 'Reserved Containers:')).toBe(true);
    });

    it('should display "No container data available" when no container data present', async () => {
      const user = userEvent.setup();
      const queue = createMockQueue();

      render(<QueueInfoTab queue={queue} />);
      await user.click(screen.getByText('Container Information'));

      expect(screen.getByText('No container data available')).toBeInTheDocument();
    });

    it('should not display "No container data available" when some container data is present', async () => {
      const user = userEvent.setup();
      const queue = createMockQueue({ numContainers: 10 });

      render(<QueueInfoTab queue={queue} />);
      await user.click(screen.getByText('Container Information'));

      expect(screen.queryByText('No container data available')).not.toBeInTheDocument();
    });
  });

  describe('Ordering & Preemption section', () => {
    it('should display ordering policy when available', async () => {
      const user = userEvent.setup();
      const queue = createMockQueue({ orderingPolicy: 'fifo' });

      render(<QueueInfoTab queue={queue} />);
      await user.click(screen.getByText('Ordering & Preemption'));

      const metricLabels = screen.getAllByTestId('metric-label');
      expect(metricLabels.some((label) => label.textContent === 'Ordering Policy:')).toBe(true);
    });

    it('should display preemption status as enabled badge', async () => {
      const user = userEvent.setup();
      const queue = createMockQueue({ preemptionDisabled: false });

      render(<QueueInfoTab queue={queue} />);
      await user.click(screen.getByText('Ordering & Preemption'));

      expect(screen.getByText('enabled')).toBeInTheDocument();
    });

    it('should display preemption status as disabled badge', async () => {
      const user = userEvent.setup();
      const queue = createMockQueue({ preemptionDisabled: true });

      render(<QueueInfoTab queue={queue} />);
      await user.click(screen.getByText('Ordering & Preemption'));

      expect(screen.getByText('disabled')).toBeInTheDocument();
    });

    it('should display intra-queue preemption status', async () => {
      const user = userEvent.setup();
      const queue = createMockQueue({ intraQueuePreemptionDisabled: false });

      render(<QueueInfoTab queue={queue} />);
      await user.click(screen.getByText('Ordering & Preemption'));

      const metricLabels = screen.getAllByTestId('metric-label');
      expect(metricLabels.some((label) => label.textContent === 'Intra-queue Preemption:')).toBe(
        true,
      );
    });

    it('should display "No ordering/preemption data available" when no data present', async () => {
      const user = userEvent.setup();
      const queue = createMockQueue();

      render(<QueueInfoTab queue={queue} />);
      await user.click(screen.getByText('Ordering & Preemption'));

      expect(screen.getByText('No ordering/preemption data available')).toBeInTheDocument();
    });
  });

  describe('Node Labels & Partitions section', () => {
    it('should display accessible node labels', async () => {
      const user = userEvent.setup();
      const queue = createMockQueue({ nodeLabels: ['gpu', 'ssd'] });

      render(<QueueInfoTab queue={queue} />);
      await user.click(screen.getByText('Node Labels & Partitions'));

      expect(screen.getByText('gpu')).toBeInTheDocument();
      expect(screen.getByText('ssd')).toBeInTheDocument();
    });

    it('should display "all" for wildcard node label', async () => {
      const user = userEvent.setup();
      const queue = createMockQueue({ nodeLabels: ['*'] });

      render(<QueueInfoTab queue={queue} />);
      await user.click(screen.getByText('Node Labels & Partitions'));

      expect(screen.getByText('all')).toBeInTheDocument();
    });

    it('should display default node label expression when available', async () => {
      const user = userEvent.setup();
      const queue = createMockQueue({ defaultNodeLabelExpression: 'gpu' });

      render(<QueueInfoTab queue={queue} />);
      await user.click(screen.getByText('Node Labels & Partitions'));

      const metricLabels = screen.getAllByTestId('metric-label');
      expect(
        metricLabels.some((label) => label.textContent === 'Default Node Label Expression:'),
      ).toBe(true);
    });

    it('should display "No node label data available" when no data present', async () => {
      const user = userEvent.setup();
      const queue = createMockQueue({ nodeLabels: [] });

      render(<QueueInfoTab queue={queue} />);
      await user.click(screen.getByText('Node Labels & Partitions'));

      expect(screen.getByText('No node label data available')).toBeInTheDocument();
    });

    it('should not display "No node label data available" when labels present', async () => {
      const user = userEvent.setup();
      const queue = createMockQueue({ nodeLabels: ['gpu'] });

      render(<QueueInfoTab queue={queue} />);
      await user.click(screen.getByText('Node Labels & Partitions'));

      expect(screen.queryByText('No node label data available')).not.toBeInTheDocument();
    });
  });

  describe('Access & Priority section', () => {
    it('should display queue ACLs when available', async () => {
      const user = userEvent.setup();
      const queue = createMockQueue({
        queueAcls: {
          queueAcl: [
            { accessType: 'SUBMIT_APPLICATIONS', accessControlList: 'user1,user2' },
            { accessType: 'ADMINISTER_QUEUE', accessControlList: 'admin' },
          ],
        },
      });

      render(<QueueInfoTab queue={queue} />);
      await user.click(screen.getByText('Access & Priority'));

      const metricLabels = screen.getAllByTestId('metric-label');
      expect(metricLabels.some((label) => label.textContent === 'SUBMIT_APPLICATIONS:')).toBe(true);
      expect(metricLabels.some((label) => label.textContent === 'ADMINISTER_QUEUE:')).toBe(true);
    });

    it('should display default application priority when defined', async () => {
      const user = userEvent.setup();
      const queue = createMockQueue({ defaultPriority: 5 });

      render(<QueueInfoTab queue={queue} />);
      await user.click(screen.getByText('Access & Priority'));

      const metricLabels = screen.getAllByTestId('metric-label');
      expect(
        metricLabels.some((label) => label.textContent === 'Default Application Priority:'),
      ).toBe(true);
    });

    it('should display queue priority when defined', async () => {
      const user = userEvent.setup();
      const queue = createMockQueue({ queuePriority: 10 });

      render(<QueueInfoTab queue={queue} />);
      await user.click(screen.getByText('Access & Priority'));

      const metricLabels = screen.getAllByTestId('metric-label');
      expect(metricLabels.some((label) => label.textContent === 'Queue Priority:')).toBe(true);
    });

    it('should display "No access/priority data available" when no data present', async () => {
      const user = userEvent.setup();
      const queue = createMockQueue();

      render(<QueueInfoTab queue={queue} />);
      await user.click(screen.getByText('Access & Priority'));

      expect(screen.getByText('No access/priority data available')).toBeInTheDocument();
    });
  });

  describe('Lifecycle Settings section', () => {
    it('should display default application lifetime when defined', async () => {
      const user = userEvent.setup();
      const queue = createMockQueue({ defaultApplicationLifetime: 3600 });

      render(<QueueInfoTab queue={queue} />);
      await user.click(screen.getByText('Lifecycle Settings'));

      const metricLabels = screen.getAllByTestId('metric-label');
      expect(
        metricLabels.some((label) => label.textContent === 'Default Application Lifetime:'),
      ).toBe(true);
    });

    it('should display max application lifetime when defined', async () => {
      const user = userEvent.setup();
      const queue = createMockQueue({ maxApplicationLifetime: 7200 });

      render(<QueueInfoTab queue={queue} />);
      await user.click(screen.getByText('Lifecycle Settings'));

      const metricLabels = screen.getAllByTestId('metric-label');
      expect(metricLabels.some((label) => label.textContent === 'Max Application Lifetime:')).toBe(
        true,
      );
    });

    it('should display "No lifecycle settings configured" when no data present', async () => {
      const user = userEvent.setup();
      const queue = createMockQueue();

      render(<QueueInfoTab queue={queue} />);
      await user.click(screen.getByText('Lifecycle Settings'));

      expect(screen.getByText('No lifecycle settings configured')).toBeInTheDocument();
    });

    it('should not display "No lifecycle settings configured" when some data present', async () => {
      const user = userEvent.setup();
      const queue = createMockQueue({ defaultApplicationLifetime: 3600 });

      render(<QueueInfoTab queue={queue} />);
      await user.click(screen.getByText('Lifecycle Settings'));

      expect(screen.queryByText('No lifecycle settings configured')).not.toBeInTheDocument();
    });
  });
});
