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


import { render, screen, waitFor, within } from '@testing-library/react';
import userEvent from '@testing-library/user-event';
import { vi } from 'vitest';
import { NodesPanel } from './NodesPanel';
import { useSchedulerStore } from '~/stores/schedulerStore';
import { formatMemory } from '~/utils/formatUtils';
import type { NodeInfo, NodeToLabelMapping } from '~/types';
import type { NodeLabel } from '~/types';

// Mock dependencies
vi.mock('~/stores/schedulerStore');
vi.mock('~/utils/formatUtils');

// Mock Select component to simplify testing
vi.mock('~/components/ui/select', () => ({
  Select: ({ children, value, onValueChange, disabled }: any) => (
    <div data-testid="select-wrapper" data-value={value} data-disabled={disabled}>
      <select
        value={value}
        onChange={(e) => onValueChange(e.target.value)}
        disabled={disabled}
        data-testid="label-select"
      >
        {children}
      </select>
    </div>
  ),
  SelectTrigger: ({ children }: any) => <>{children}</>,
  SelectValue: ({ placeholder }: any) => <span>{placeholder}</span>,
  SelectContent: ({ children }: any) => <>{children}</>,
  SelectItem: ({ value, children }: any) => <option value={value}>{children}</option>,
}));

describe('NodesPanel', () => {
  const mockAssignNodeToLabel = vi.fn();
  const mockConsoleError = vi.spyOn(console, 'error').mockImplementation(() => {});

  const createMockNode = (overrides?: Partial<NodeInfo>): NodeInfo => ({
    id: 'node-1',
    rack: '/default-rack',
    state: 'RUNNING',
    nodeHostName: 'node1.example.com',
    nodeHTTPAddress: 'node1.example.com:8042',
    lastHealthUpdate: Date.now(),
    version: '3.3.4',
    healthReport: '',
    numContainers: 5,
    usedMemoryMB: 4096,
    availMemoryMB: 12288,
    usedVirtualCores: 2,
    availableVirtualCores: 6,
    numRunningOpportContainers: 0,
    usedMemoryOpportGB: 0,
    usedVirtualCoresOpport: 0,
    numQueuedContainers: 0,
    nodeLabels: [],
    allocationTags: {},
    usedResource: { memory: 4096, vCores: 2 },
    availableResource: { memory: 12288, vCores: 6 },
    nodeAttributesInfo: {},
    ...overrides,
  });

  const mockNodeLabels: NodeLabel[] = [
    { name: 'gpu', exclusivity: true },
    { name: 'highmem', exclusivity: false },
    { name: 'ssd', exclusivity: false },
  ];

  const defaultStoreState = {
    nodes: [],
    nodeToLabels: [],
    nodeLabels: mockNodeLabels,
    assignNodeToLabel: mockAssignNodeToLabel,
    isLoading: false,
    searchQuery: '',
    getFilteredNodes: vi.fn(() => []),
  };

  function mockStore(overrides: Record<string, any> = {}) {
    const state = { ...defaultStoreState, ...overrides };
    vi.mocked(useSchedulerStore).mockImplementation((selector?: any) => {
      return selector ? selector(state) : state;
    });
  }

  beforeEach(() => {
    vi.clearAllMocks();
    mockConsoleError.mockClear();
    mockStore();
    (formatMemory as any).mockImplementation((mb: number) => `${mb} MB`);
  });

  afterAll(() => {
    mockConsoleError.mockRestore();
  });

  describe('Empty state', () => {
    it('should display empty state when no nodes exist', () => {
      render(<NodesPanel selectedLabel={null} />);

      expect(screen.getByText('No cluster nodes found')).toBeInTheDocument();
      expect(
        screen.getByText('Node information will appear here when available'),
      ).toBeInTheDocument();

      const monitorIcon = document.querySelector('.h-12.w-12');
      expect(monitorIcon).toBeInTheDocument();
    });

    it('should not show table when no nodes exist', () => {
      render(<NodesPanel selectedLabel={null} />);

      expect(screen.queryByRole('table')).not.toBeInTheDocument();
    });
  });

  describe('Node display with no label filter', () => {
    const mockNodes: NodeInfo[] = [
      createMockNode({ id: 'node-1', nodeHostName: 'node1.example.com' }),
      createMockNode({ id: 'node-2', nodeHostName: 'node2.example.com' }),
      createMockNode({ id: 'node-3', nodeHostName: 'node3.example.com' }),
    ];

    it('should display all nodes when no label is selected', () => {
      mockStore({
        nodes: mockNodes,
      });

      render(<NodesPanel selectedLabel={null} />);

      expect(screen.getByText('node1.example.com')).toBeInTheDocument();
      expect(screen.getByText('node2.example.com')).toBeInTheDocument();
      expect(screen.getByText('node3.example.com')).toBeInTheDocument();
    });

    it('should show correct header text for all nodes', () => {
      mockStore({
        nodes: mockNodes,
      });

      render(<NodesPanel selectedLabel={null} />);

      expect(screen.getByText(/All cluster nodes \(3\)/)).toBeInTheDocument();
      expect(screen.getByText(/showing default partition and labeled nodes/)).toBeInTheDocument();
    });
  });

  describe('Node filtering by label', () => {
    const mockNodes: NodeInfo[] = [
      createMockNode({ id: 'node-1', nodeHostName: 'node1.example.com' }),
      createMockNode({ id: 'node-2', nodeHostName: 'node2.example.com' }),
      createMockNode({ id: 'node-3', nodeHostName: 'node3.example.com' }),
    ];

    const mockNodeToLabels: NodeToLabelMapping[] = [
      { nodeId: 'node-1', nodeLabels: ['gpu'] },
      { nodeId: 'node-2', nodeLabels: ['highmem'] },
      { nodeId: 'node-3', nodeLabels: ['ssd'] },
    ];

    it('should filter nodes by selected label', () => {
      mockStore({
        nodes: mockNodes,
        nodeToLabels: mockNodeToLabels,
      });

      render(<NodesPanel selectedLabel="gpu" />);

      expect(screen.getByText('node1.example.com')).toBeInTheDocument();
      expect(screen.queryByText('node2.example.com')).not.toBeInTheDocument();
      expect(screen.queryByText('node3.example.com')).not.toBeInTheDocument();
    });

    it('should show correct header text for filtered nodes', () => {
      mockStore({
        nodes: mockNodes,
        nodeToLabels: mockNodeToLabels,
      });

      render(<NodesPanel selectedLabel="gpu" />);

      const header = screen.getByText(/Nodes with label:/);
      expect(header).toBeInTheDocument();

      const strongTag = header.querySelector('strong');
      expect(strongTag?.textContent).toBe('gpu');

      // Check the count is shown - it's within the same paragraph
      expect(header.textContent).toContain('(1)');
    });

    it('should show empty state for label with no nodes', () => {
      mockStore({
        nodes: mockNodes,
        nodeToLabels: mockNodeToLabels,
      });

      render(<NodesPanel selectedLabel="nonexistent" />);

      expect(screen.getByText('No nodes assigned to label "nonexistent"')).toBeInTheDocument();
      expect(
        screen.getByText(/Nodes without this label operate on the default partition/),
      ).toBeInTheDocument();
    });
  });

  describe('Node information display', () => {
    const mockNode = createMockNode({
      id: 'node-1',
      nodeHostName: 'node1.example.com',
      rack: '/rack-1',
      state: 'RUNNING',
      numContainers: 10,
      usedMemoryMB: 8192,
      availMemoryMB: 8192,
      usedVirtualCores: 4,
      availableVirtualCores: 4,
    });

    it('should display node basic information', () => {
      mockStore({
        nodes: [mockNode],
      });

      render(<NodesPanel selectedLabel={null} />);

      expect(screen.getByText('node1.example.com')).toBeInTheDocument();
      expect(screen.getByText('/rack-1')).toBeInTheDocument();
    });

    it('should display node state with correct variant', () => {
      const nodes = [
        createMockNode({ id: 'n1', state: 'RUNNING' }),
        createMockNode({ id: 'n2', state: 'UNHEALTHY' }),
        createMockNode({ id: 'n3', state: 'SHUTDOWN' }),
      ];

      mockStore({
        nodes,
      });

      render(<NodesPanel selectedLabel={null} />);

      const runningBadge = screen.getByText('RUNNING');
      const unhealthyBadge = screen.getByText('UNHEALTHY');
      const shutdownBadge = screen.getByText('SHUTDOWN');

      expect(runningBadge).toHaveAttribute('data-slot', 'badge');
      // Badges use gradient styling
      expect(runningBadge.className).toContain('from-primary');
      expect(unhealthyBadge.className).toContain('from-destructive');
      expect(shutdownBadge.className).toContain('bg-secondary');
    });

    it('should display container count', () => {
      mockStore({
        nodes: [mockNode],
      });

      render(<NodesPanel selectedLabel={null} />);

      const containerCells = screen.getAllByRole('cell');
      const containerCell = containerCells.find((cell) => cell.textContent === '10');
      expect(containerCell).toBeInTheDocument();
    });
  });

  describe('Resource utilization display', () => {
    const mockNode = createMockNode({
      id: 'node-1',
      usedMemoryMB: 8192,
      availMemoryMB: 8192,
      usedVirtualCores: 4,
      availableVirtualCores: 4,
    });

    it('should display memory utilization', () => {
      mockStore({
        nodes: [mockNode],
      });

      render(<NodesPanel selectedLabel={null} />);

      expect(formatMemory).toHaveBeenCalledWith(8192); // used
      expect(formatMemory).toHaveBeenCalledWith(16384); // total
      expect(screen.getByText('8192 MB / 16384 MB')).toBeInTheDocument();
    });

    it('should display CPU utilization', () => {
      mockStore({
        nodes: [mockNode],
      });

      render(<NodesPanel selectedLabel={null} />);

      expect(screen.getByText('4 / 8')).toBeInTheDocument();
    });

    it('should show progress bars for utilization', () => {
      mockStore({
        nodes: [mockNode],
      });

      render(<NodesPanel selectedLabel={null} />);

      const progressBars = document.querySelectorAll('[data-slot="progress"]');
      expect(progressBars).toHaveLength(2); // Memory and CPU

      // Check that progress bars are rendered
      progressBars.forEach((bar) => {
        expect(bar).toBeInTheDocument();
        expect(bar).toHaveAttribute('role', 'progressbar');
      });
    });

    it('should apply warning color for high utilization', () => {
      const highUtilNode = createMockNode({
        usedMemoryMB: 14000,
        availMemoryMB: 2384,
        usedVirtualCores: 7,
        availableVirtualCores: 1,
      });

      mockStore({
        nodes: [highUtilNode],
      });

      render(<NodesPanel selectedLabel={null} />);

      // Find the progress bar in the memory section
      const memorySection = screen.getByText(/14000 MB/).closest('td');
      const progressBar = memorySection?.querySelector('[data-slot="progress"]');
      expect(progressBar).toHaveClass('text-warning');
    });

    it('should apply destructive color for critical utilization', () => {
      const criticalNode = createMockNode({
        usedMemoryMB: 15000,
        availMemoryMB: 1384,
        usedVirtualCores: 8,
        availableVirtualCores: 0,
      });

      mockStore({
        nodes: [criticalNode],
      });

      render(<NodesPanel selectedLabel={null} />);

      // Find the progress bar in the memory section
      const memorySection = screen.getByText(/15000 MB/).closest('td');
      const progressBar = memorySection?.querySelector('[data-slot="progress"]');
      expect(progressBar).toHaveClass('text-destructive');
    });
  });

  describe('Label assignment display', () => {
    const mockNodeToLabels: NodeToLabelMapping[] = [
      { nodeId: 'node-1', nodeLabels: ['gpu'] },
      { nodeId: 'node-2', nodeLabels: [] },
    ];

    it('should display assigned labels as badges', () => {
      const node = createMockNode({ id: 'node-1' });

      mockStore({
        nodes: [node],
        nodeToLabels: mockNodeToLabels,
      });

      render(<NodesPanel selectedLabel={null} />);

      const nodeRow = screen.getByRole('row', { name: /node1\.example\.com/i });
      const labelBadge = within(nodeRow)
        .getAllByText('gpu')
        .find((element) => element.closest('[data-slot="badge"]'));
      expect(labelBadge).toBeDefined();
    });

    it('should display Default badge for nodes without labels', () => {
      const node = createMockNode({ id: 'node-2' });

      mockStore({
        nodes: [node],
        nodeToLabels: mockNodeToLabels,
      });

      render(<NodesPanel selectedLabel={null} />);

      expect(screen.getByText('Default')).toBeInTheDocument();
    });

    it('should highlight selected label badge', () => {
      const node = createMockNode({ id: 'node-1' });

      mockStore({
        nodes: [node],
        nodeToLabels: mockNodeToLabels,
      });

      render(<NodesPanel selectedLabel="gpu" />);

      const nodeRow = screen.getByRole('row', { name: /node1\.example\.com/i });
      const gpuBadgeText = within(nodeRow)
        .getAllByText('gpu')
        .find((element) => element.closest('[data-slot="badge"]'));
      expect(gpuBadgeText).toBeDefined();
      const gpuBadge = gpuBadgeText!.closest('[data-slot="badge"]');

      // Badge uses gradient styling
      expect(gpuBadge).toHaveClass('from-primary');
    });
  });

  describe('Label assignment actions', () => {
    const mockNode = createMockNode({ id: 'node-1' });
    const mockNodeToLabels: NodeToLabelMapping[] = [{ nodeId: 'node-1', nodeLabels: ['gpu'] }];

    beforeEach(() => {
      mockStore({
        nodes: [mockNode],
        nodeToLabels: mockNodeToLabels,
      });
    });

    it('should display label select dropdown', () => {
      render(<NodesPanel selectedLabel={null} />);

      const select = screen.getByTestId('label-select');
      expect(select).toBeInTheDocument();
      expect(select).toHaveValue('gpu');
    });

    it('should show all available labels in dropdown', () => {
      render(<NodesPanel selectedLabel={null} />);

      const select = screen.getByTestId('label-select');
      const options = within(select).getAllByRole('option');

      expect(options).toHaveLength(4); // default + 3 labels
      expect(options[0]).toHaveTextContent('Default (no label)');
      expect(options[1]).toHaveTextContent('gpu');
      expect(options[2]).toHaveTextContent('highmem');
      expect(options[3]).toHaveTextContent('ssd');
    });

    it('should show exclusivity indicator in dropdown', () => {
      render(<NodesPanel selectedLabel={null} />);

      const select = screen.getByTestId('label-select');
      const gpuOption = within(select).getByRole('option', { name: /gpu/ });

      expect(within(gpuOption).getByText('Exclusive')).toBeInTheDocument();
    });

    it('should call assignNodeToLabel when changing selection', async () => {
      mockAssignNodeToLabel.mockResolvedValue(undefined);

      render(<NodesPanel selectedLabel={null} />);

      const select = screen.getByTestId('label-select');
      await userEvent.selectOptions(select, 'highmem');

      expect(mockAssignNodeToLabel).toHaveBeenCalledWith('node-1', 'highmem');
    });

    it('should call with null when selecting default', async () => {
      mockAssignNodeToLabel.mockResolvedValue(undefined);

      render(<NodesPanel selectedLabel={null} />);

      const select = screen.getByTestId('label-select');
      await userEvent.selectOptions(select, 'default');

      expect(mockAssignNodeToLabel).toHaveBeenCalledWith('node-1', null);
    });

    it('should disable select when loading', () => {
      mockStore({
        nodes: [mockNode],
        nodeToLabels: mockNodeToLabels,
        isLoading: true,
      });

      render(<NodesPanel selectedLabel={null} />);

      const selectWrapper = screen.getByTestId('select-wrapper');
      expect(selectWrapper).toHaveAttribute('data-disabled', 'true');
    });

    it('should handle assignment errors gracefully', async () => {
      const testError = new Error('Failed to assign label');
      mockAssignNodeToLabel.mockRejectedValue(testError);

      render(<NodesPanel selectedLabel={null} />);

      const select = screen.getByTestId('label-select');
      await userEvent.selectOptions(select, 'highmem');

      await waitFor(() => {
        expect(mockAssignNodeToLabel).toHaveBeenCalledWith('node-1', 'highmem');
      });
      // Error is now set in the store and displayed by parent component
    });
  });

  describe('Remove label button', () => {
    const mockNode = createMockNode({ id: 'node-1' });
    const mockNodeToLabels: NodeToLabelMapping[] = [{ nodeId: 'node-1', nodeLabels: ['gpu'] }];

    it('should show remove button for nodes with labels', () => {
      mockStore({
        nodes: [mockNode],
        nodeToLabels: mockNodeToLabels,
      });

      render(<NodesPanel selectedLabel={null} />);

      // Find the remove button by its icon (X)
      const actionCell = screen
        .getByRole('row', { name: /node1.example.com/i })
        .querySelector('td:last-child');
      const removeButton = within(actionCell! as HTMLElement).getByRole('button');
      expect(removeButton).toBeInTheDocument();
      expect(removeButton.querySelector('svg')).toBeInTheDocument();
    });

    it('should not show remove button for nodes without labels', () => {
      const nodeToLabels: NodeToLabelMapping[] = [{ nodeId: 'node-1', nodeLabels: [] }];

      mockStore({
        nodes: [mockNode],
        nodeToLabels,
      });

      render(<NodesPanel selectedLabel={null} />);

      // Check that no remove button exists in the action cell
      const actionCell = screen
        .getByRole('row', { name: /node1.example.com/i })
        .querySelector('td:last-child');
      const buttons = within(actionCell! as HTMLElement).queryAllByRole('button');
      // Should only have the select trigger, no remove button
      expect(buttons.length).toBeLessThan(2);
    });

    it('should call assignNodeToLabel with null when remove is clicked', async () => {
      mockAssignNodeToLabel.mockResolvedValue(undefined);

      mockStore({
        nodes: [mockNode],
        nodeToLabels: mockNodeToLabels,
      });

      render(<NodesPanel selectedLabel={null} />);

      // Find and click the remove button
      const actionCell = screen
        .getByRole('row', { name: /node1.example.com/i })
        .querySelector('td:last-child');
      const removeButton = within(actionCell! as HTMLElement).getByRole('button');
      await userEvent.click(removeButton as HTMLElement);

      expect(mockAssignNodeToLabel).toHaveBeenCalledWith('node-1', null);
    });

    it('should disable remove button when loading', () => {
      mockStore({
        nodes: [mockNode],
        nodeToLabels: mockNodeToLabels,
        isLoading: true,
      });

      render(<NodesPanel selectedLabel={null} />);

      // Find the remove button and check it's disabled
      const actionCell = screen
        .getByRole('row', { name: /node1.example.com/i })
        .querySelector('td:last-child');
      const removeButton = within(actionCell! as HTMLElement).getByRole('button');
      expect(removeButton).toBeDisabled();
    });
  });

  describe('Table structure', () => {
    const mockNodes: NodeInfo[] = [
      createMockNode({ id: 'node-1' }),
      createMockNode({ id: 'node-2' }),
    ];

    it('should render table with correct headers', () => {
      mockStore({
        nodes: mockNodes,
      });

      render(<NodesPanel selectedLabel={null} />);

      expect(screen.getByRole('columnheader', { name: 'Node' })).toBeInTheDocument();
      expect(screen.getByRole('columnheader', { name: 'State' })).toBeInTheDocument();
      expect(screen.getByRole('columnheader', { name: 'Label' })).toBeInTheDocument();
      expect(screen.getByRole('columnheader', { name: 'Memory' })).toBeInTheDocument();
      expect(screen.getByRole('columnheader', { name: 'Cores' })).toBeInTheDocument();
      expect(screen.getByRole('columnheader', { name: 'Containers' })).toBeInTheDocument();
      expect(screen.getByRole('columnheader', { name: 'Action' })).toBeInTheDocument();
    });

    it('should render correct number of rows', () => {
      mockStore({
        nodes: mockNodes,
      });

      render(<NodesPanel selectedLabel={null} />);

      const rows = screen.getAllByRole('row');
      expect(rows).toHaveLength(3); // 1 header + 2 data rows
    });
  });

  describe('Accessibility', () => {
    const mockNode = createMockNode({ id: 'node-1' });

    it('should have accessible table structure', () => {
      mockStore({
        nodes: [mockNode],
      });

      render(<NodesPanel selectedLabel={null} />);

      const table = screen.getByRole('table');
      expect(table).toBeInTheDocument();

      const headers = screen.getAllByRole('columnheader');
      expect(headers).toHaveLength(7);
    });

    it('should have accessible progress bars', () => {
      mockStore({
        nodes: [mockNode],
      });

      render(<NodesPanel selectedLabel={null} />);

      // Progress components should have proper ARIA attributes
      const progressBars = document.querySelectorAll('[data-slot="progress"]');
      expect(progressBars.length).toBeGreaterThan(0);
      progressBars.forEach((bar) => {
        expect(bar).toHaveAttribute('role', 'progressbar');
        // Progress should have proper ARIA attributes
        // Radix UI Progress sets these on render
        const valueMax = bar.getAttribute('aria-valuemax');
        const valueMin = bar.getAttribute('aria-valuemin');
        expect(valueMax).toBeTruthy();
        expect(valueMin).toBeTruthy();
      });
    });

    it('should have accessible tooltips', () => {
      const mockNodeToLabels: NodeToLabelMapping[] = [{ nodeId: 'node-1', nodeLabels: ['gpu'] }];

      mockStore({
        nodes: [mockNode],
        nodeToLabels: mockNodeToLabels,
      });

      render(<NodesPanel selectedLabel={null} />);

      // The tooltip content is rendered when hovering
      // Instead, verify the button with X icon exists
      const actionCell = screen
        .getByRole('row', { name: /node1.example.com/i })
        .querySelector('td:last-child');
      const removeButton = within(actionCell! as HTMLElement).getByRole('button');
      expect(removeButton).toBeInTheDocument();

      // The button should have an X icon
      const xIcon = removeButton.querySelector('svg');
      expect(xIcon).toBeInTheDocument();
    });
  });
});
