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
import { render, screen, within } from '~/testing/setup/setup';
import { QueueCardNode } from './QueueCardNode';
import userEvent from '@testing-library/user-event';
import { useSchedulerStore } from '~/stores/schedulerStore';
import type { QueueCardData } from '~/features/queue-management/hooks/useQueueTreeData';
import { TooltipProvider } from '~/components/ui/tooltip';
import type { NodeProps } from '@xyflow/react';

// Mock the store
vi.mock('~/stores/schedulerStore');

// Mock the dialog components
vi.mock('./dialogs/AddQueueDialog', () => ({
  AddQueueDialog: ({ open }: { open: boolean }) =>
    open ? <div data-testid="add-queue-dialog">Add Queue Dialog</div> : null,
}));

vi.mock('./dialogs/DeleteQueueDialog', () => ({
  DeleteQueueDialog: ({ open }: { open: boolean }) =>
    open ? <div data-testid="delete-queue-dialog">Delete Queue Dialog</div> : null,
}));

// Mock React Flow Handle components
vi.mock('@xyflow/react', () => ({
  Handle: ({ type, position }: any) => <div data-testid={`handle-${type}-${position}`} />,
  Position: {
    Top: 'top',
    Bottom: 'bottom',
    Left: 'left',
    Right: 'right',
  },
}));

const mockSelectQueue = vi.fn();
const mockSetPropertyPanelOpen = vi.fn();
const mockSetPropertyPanelInitialTab = vi.fn();
const mockToggleComparisonQueue = vi.fn();

const renderWithProviders = (ui: React.ReactElement) => {
  return render(<TooltipProvider>{ui}</TooltipProvider>);
};

const createNodeProps = (data: QueueCardData, overrides?: Partial<NodeProps>) => {
  return {
    id: '1',
    data,
    type: 'custom',
    position: { x: 0, y: 0 },
    selected: false,
    draggable: true,
    selectable: true,
    deletable: true,
    dragging: false,
    zIndex: 0,
    isConnectable: true,
    positionAbsoluteX: 0,
    positionAbsoluteY: 0,
    ...overrides,
  };
};

describe('QueueCardNode', () => {
  const defaultNodeData: QueueCardData = {
    queueType: 'leaf',
    queuePath: 'root.default',
    queueName: 'default',
    capacity: 10,
    maxCapacity: 100,
    state: 'RUNNING',
    usedCapacity: 5,
    absoluteCapacity: 10,
    absoluteMaxCapacity: 100,
    absoluteUsedCapacity: 5,
    numApplications: 2,
    numActiveApplications: 1,
    numPendingApplications: 1,
    resourcesUsed: undefined,
    stagedStatus: undefined,
    isLeaf: true,
    capacityConfig: '10',
    maxCapacityConfig: '100',
    stagedState: undefined,
    autoCreationStatus: undefined,
    creationMethod: 'static',
    isAutoCreatedQueue: false,
  };

  beforeEach(() => {
    vi.clearAllMocks();
    (useSchedulerStore as any).mockImplementation((selector: any) => {
      const state = {
        comparisonQueues: [],
        selectedQueuePath: null,
        selectQueue: mockSelectQueue,
        setPropertyPanelOpen: mockSetPropertyPanelOpen,
        isPropertyPanelOpen: false,
        propertyPanelInitialTab: 'overview' as const,
        setPropertyPanelInitialTab: mockSetPropertyPanelInitialTab,
        toggleComparisonQueue: mockToggleComparisonQueue,
        getQueueByPath: vi.fn().mockReturnValue({ queueName: 'default' }),
        getChildQueues: vi.fn().mockReturnValue([]),
        selectedNodeLabelFilter: '',
        getQueueLabelCapacity: vi.fn().mockReturnValue({
          capacity: '10',
          maxCapacity: '100',
          absoluteCapacity: '10',
          isLabelSpecific: false,
          label: 'DEFAULT',
          hasAccess: true,
          canUseLabel: true,
        }),
        hasPendingDeletion: vi.fn().mockReturnValue(false),
        clearQueueChanges: vi.fn(),
        requestTemplateConfigOpen: vi.fn(),
        revertQueueDeletion: vi.fn(),
      };
      return selector ? selector(state) : state;
    });
  });

  it('should display queue name and path', () => {
    renderWithProviders(<QueueCardNode {...createNodeProps(defaultNodeData)} />);

    expect(screen.getByText('default')).toBeInTheDocument();
    expect(screen.getByText('root.default')).toBeInTheDocument();
  });

  it('should display capacity information', () => {
    renderWithProviders(<QueueCardNode {...createNodeProps(defaultNodeData)} />);

    const capacityDisplay = screen
      .getAllByText('10%')
      .find((el) => el.className.includes('text-2xl'));
    expect(capacityDisplay).toBeInTheDocument();
    expect(screen.getByText('capacity')).toBeInTheDocument();
    expect(screen.getByText('Maximum capacity: 100%')).toBeInTheDocument();
  });

  it('should display capacity with weight format', () => {
    const nodeData = {
      ...defaultNodeData,
      capacityConfig: '2w',
      maxCapacityConfig: '5w',
    };

    renderWithProviders(<QueueCardNode {...createNodeProps(nodeData)} />);

    expect(screen.getByText('2w')).toBeInTheDocument();
    expect(screen.getByText('Maximum capacity: 5w')).toBeInTheDocument();
  });

  it('should prioritize inline capacity resources and show overflow details', async () => {
    const user = userEvent.setup();
    const nodeData = {
      ...defaultNodeData,
      capacityConfig: '[memory=12128,vcores=5]',
      maxCapacityConfig: '[memory=16384,vcores=10,yarn.io/gpu=1]',
    };

    renderWithProviders(<QueueCardNode {...createNodeProps(nodeData)} />);

    expect(screen.getByText('memory: 12128')).toBeInTheDocument();
    expect(screen.getByText('vcores: 5')).toBeInTheDocument();
    expect(screen.getByText('memory: 16384')).toBeInTheDocument();
    expect(screen.getByText('vcores: 10')).toBeInTheDocument();
    expect(screen.queryByText('yarn.io/gpu: 1')).not.toBeInTheDocument();

    const summaryTrigger = screen.getByRole('button', { name: /show 1 additional resource/i });
    await user.click(summaryTrigger);
    expect(await screen.findByText('yarn.io/gpu')).toBeInTheDocument();

    expect(screen.getByText('max capacity')).toBeInTheDocument();
    expect(screen.queryByText('Maximum capacity:')).not.toBeInTheDocument();
  });

  it('should reflect multiple overflow resources and keep badge count in sync', async () => {
    const user = userEvent.setup();
    const nodeData = {
      ...defaultNodeData,
      capacityConfig: '[memory=100,vcores=10,yarn.io/gpu=1,yarn.io/fpga=2]',
      maxCapacityConfig: '[memory=150,vcores=15,yarn.io/gpu=2,yarn.io/fpga=3,yarn.io/nic=4]',
    };

    renderWithProviders(<QueueCardNode {...createNodeProps(nodeData)} />);

    const summaryTrigger = screen.getByRole('button', { name: /show 3 additional resources/i });
    expect(summaryTrigger).toBeInTheDocument();

    await user.click(summaryTrigger);
    const popoverHeading = await screen.findByText('Resource capacity details');
    const popover = popoverHeading.closest('[data-slot="popover-content"]');
    if (!popover) {
      throw new Error('Popover should be rendered');
    }

    if (!(popover instanceof HTMLElement)) {
      throw new Error('Popover should be an HTMLElement');
    }

    const popoverScope = within(popover);
    const gpuRow = popoverScope.getByText('yarn.io/gpu');
    const fpgaRow = popoverScope.getByText('yarn.io/fpga');
    const nicRow = popoverScope.getByText('yarn.io/nic');

    const getValues = (resourceNode: Element) => {
      const capacityCell = resourceNode.nextElementSibling as HTMLElement | null;
      const maxCell = capacityCell?.nextElementSibling as HTMLElement | null;
      return [capacityCell?.textContent ?? '', maxCell?.textContent ?? ''];
    };

    expect(getValues(gpuRow)).toEqual(['1', '2']);
    expect(getValues(fpgaRow)).toEqual(['2', '3']);
    expect(getValues(nicRow)).toEqual(['—', '4']);
  });

  it('should render mixed resource vector values without losing suffixes', async () => {
    const user = userEvent.setup();
    const nodeData = {
      ...defaultNodeData,
      capacityConfig: '[vcores=15%,memory=12w,yarn.io/fpga=2]',
      maxCapacityConfig: '[vcores=20%,memory=15w,yarn.io/fpga=4]',
    };

    renderWithProviders(<QueueCardNode {...createNodeProps(nodeData)} />);

    expect(screen.getByText('vcores: 15%')).toBeInTheDocument();
    expect(screen.getByText('memory: 12w')).toBeInTheDocument();
    expect(screen.getByText('vcores: 20%')).toBeInTheDocument();
    expect(screen.getByText('memory: 15w')).toBeInTheDocument();

    const summaryTrigger = screen.getByRole('button', { name: /show 1 additional resource/i });
    await user.click(summaryTrigger);
    const popoverHeading = await screen.findByText('Resource capacity details');
    const popover = popoverHeading.closest('[data-slot="popover-content"]');
    if (!popover) {
      throw new Error('Popover should be rendered');
    }
    if (!(popover instanceof HTMLElement)) {
      throw new Error('Popover should be an HTMLElement');
    }
    const popoverScope = within(popover);
    expect(popoverScope.getByText('yarn.io/fpga')).toBeInTheDocument();
    expect(popoverScope.getByText('2')).toBeInTheDocument();
    expect(popoverScope.getByText('4')).toBeInTheDocument();
  });

  it('should display queue status badges', () => {
    const { container } = renderWithProviders(
      <QueueCardNode {...createNodeProps(defaultNodeData)} />,
    );

    const svgIcons = container.querySelectorAll('svg.lucide');
    expect(svgIcons.length).toBeGreaterThanOrEqual(2); // At least capacity mode and state badges

    // Verify we have the percentage icon (capacity mode)
    const percentIcon = container.querySelector('svg.lucide-percent');
    expect(percentIcon).toBeInTheDocument();

    // Verify we have the play icon (running state)
    const playIcon = container.querySelector('svg.lucide-play');
    expect(playIcon).toBeInTheDocument();
  });

  it('should open property panel on click', async () => {
    const user = userEvent.setup();
    renderWithProviders(<QueueCardNode {...createNodeProps(defaultNodeData)} />);

    const card = screen.getByText('default').closest('.relative');
    if (!card) throw new Error('Card not found');
    await user.click(card);

    expect(mockSelectQueue).toHaveBeenCalledWith('root.default');
    expect(mockSetPropertyPanelOpen).toHaveBeenCalledWith(true);
  });

  it('should not open property panel for newly added queues', async () => {
    const user = userEvent.setup();
    const nodeData = {
      ...defaultNodeData,
      stagedStatus: 'new' as const,
    };

    renderWithProviders(<QueueCardNode {...createNodeProps(nodeData)} />);

    const card = screen.getByText('default').closest('.relative');
    if (!card) throw new Error('Card not found');
    await user.click(card);

    expect(mockSelectQueue).not.toHaveBeenCalled();
    expect(mockSetPropertyPanelOpen).not.toHaveBeenCalled();
  });

  it('should show tooltip for newly added queues', async () => {
    const user = userEvent.setup();
    const nodeData = {
      ...defaultNodeData,
      stagedStatus: 'new' as const,
    };

    renderWithProviders(<QueueCardNode {...createNodeProps(nodeData)} />);

    const card = screen.getByText('default').closest('.relative');
    if (!card) throw new Error('Card not found');
    await user.hover(card);

    const tooltips = await screen.findAllByText(
      'This queue must be applied before it can be edited',
    );
    expect(tooltips.length).toBeGreaterThan(0);
  });

  it('should toggle comparison checkbox', async () => {
    (useSchedulerStore as any).mockImplementation((selector: any) => {
      const state = {
        comparisonQueues: [],
        selectedQueuePath: null,
        selectQueue: mockSelectQueue,
        setPropertyPanelOpen: mockSetPropertyPanelOpen,
        isPropertyPanelOpen: false,
        propertyPanelInitialTab: 'overview' as const,
        setPropertyPanelInitialTab: mockSetPropertyPanelInitialTab,
        toggleComparisonQueue: mockToggleComparisonQueue,
        getQueueByPath: vi.fn().mockReturnValue({ queueName: 'default' }),
        getChildQueues: vi.fn().mockReturnValue([]),
        selectedNodeLabelFilter: '',
        getQueueLabelCapacity: vi.fn().mockReturnValue({
          capacity: '10',
          maxCapacity: '100',
          absoluteCapacity: '10',
          isLabelSpecific: false,
          label: 'DEFAULT',
          hasAccess: true,
          canUseLabel: true,
        }),
        hasPendingDeletion: vi.fn().mockReturnValue(false),
        clearQueueChanges: vi.fn(),
        requestTemplateConfigOpen: vi.fn(),
        isComparisonModeActive: true,
      };
      return selector ? selector(state) : state;
    });

    const user = userEvent.setup();
    renderWithProviders(<QueueCardNode {...createNodeProps(defaultNodeData)} />);

    const checkbox = screen.getByRole('checkbox');
    await user.click(checkbox);

    expect(mockToggleComparisonQueue).toHaveBeenCalledWith('root.default');
  });

  it('should toggle comparison when clicking card in comparison mode', async () => {
    (useSchedulerStore as any).mockImplementation((selector: any) => {
      const state = {
        comparisonQueues: [],
        selectedQueuePath: null,
        selectQueue: mockSelectQueue,
        setPropertyPanelOpen: mockSetPropertyPanelOpen,
        isPropertyPanelOpen: false,
        propertyPanelInitialTab: 'overview' as const,
        setPropertyPanelInitialTab: mockSetPropertyPanelInitialTab,
        toggleComparisonQueue: mockToggleComparisonQueue,
        getQueueByPath: vi.fn().mockReturnValue({ queueName: 'default' }),
        getChildQueues: vi.fn().mockReturnValue([]),
        selectedNodeLabelFilter: '',
        getQueueLabelCapacity: vi.fn().mockReturnValue({
          capacity: '10',
          maxCapacity: '100',
          absoluteCapacity: '10',
          isLabelSpecific: false,
          label: 'DEFAULT',
          hasAccess: true,
          canUseLabel: true,
        }),
        hasPendingDeletion: vi.fn().mockReturnValue(false),
        clearQueueChanges: vi.fn(),
        requestTemplateConfigOpen: vi.fn(),
        isComparisonModeActive: true,
      };
      return selector ? selector(state) : state;
    });

    const user = userEvent.setup();
    renderWithProviders(<QueueCardNode {...createNodeProps(defaultNodeData)} />);

    // Click on the card title (which is on the card)
    const cardTitle = screen.getByText('default');
    await user.click(cardTitle);

    expect(mockToggleComparisonQueue).toHaveBeenCalledWith('root.default');
    // Should NOT open property panel in comparison mode
    expect(mockSetPropertyPanelOpen).not.toHaveBeenCalled();
  });

  it('should open property panel when clicking card outside comparison mode', async () => {
    (useSchedulerStore as any).mockImplementation((selector: any) => {
      const state = {
        comparisonQueues: [],
        selectedQueuePath: null,
        selectQueue: mockSelectQueue,
        setPropertyPanelOpen: mockSetPropertyPanelOpen,
        isPropertyPanelOpen: false,
        propertyPanelInitialTab: 'overview' as const,
        setPropertyPanelInitialTab: mockSetPropertyPanelInitialTab,
        toggleComparisonQueue: mockToggleComparisonQueue,
        getQueueByPath: vi.fn().mockReturnValue({ queueName: 'default' }),
        getChildQueues: vi.fn().mockReturnValue([]),
        selectedNodeLabelFilter: '',
        getQueueLabelCapacity: vi.fn().mockReturnValue({
          capacity: '10',
          maxCapacity: '100',
          absoluteCapacity: '10',
          isLabelSpecific: false,
          label: 'DEFAULT',
          hasAccess: true,
          canUseLabel: true,
        }),
        hasPendingDeletion: vi.fn().mockReturnValue(false),
        clearQueueChanges: vi.fn(),
        requestTemplateConfigOpen: vi.fn(),
        isComparisonModeActive: false,
      };
      return selector ? selector(state) : state;
    });

    const user = userEvent.setup();
    renderWithProviders(<QueueCardNode {...createNodeProps(defaultNodeData)} />);

    // Click on the card title (which is on the card)
    const cardTitle = screen.getByText('default');
    await user.click(cardTitle);

    // Should open property panel when NOT in comparison mode
    expect(mockSetPropertyPanelOpen).toHaveBeenCalledWith(true);
    expect(mockSelectQueue).toHaveBeenCalledWith('root.default');
    // Should NOT toggle comparison
    expect(mockToggleComparisonQueue).not.toHaveBeenCalled();
  });

  it('should show selected state when queue is selected', () => {
    (useSchedulerStore as any).mockImplementation((selector: any) => {
      const state = {
        comparisonQueues: [],
        selectedQueuePath: 'root.default',
        selectQueue: mockSelectQueue,
        setPropertyPanelOpen: mockSetPropertyPanelOpen,
        toggleComparisonQueue: mockToggleComparisonQueue,
        getQueueByPath: vi.fn().mockReturnValue({ queueName: 'default' }),
        getChildQueues: vi.fn().mockReturnValue([]),
        selectedNodeLabelFilter: '',
        getQueueLabelCapacity: vi.fn().mockReturnValue({
          capacity: '10',
          maxCapacity: '100',
          absoluteCapacity: '10',
          isLabelSpecific: false,
          label: 'DEFAULT',
          hasAccess: true,
          canUseLabel: true,
        }),
        hasPendingDeletion: vi.fn().mockReturnValue(false),
        clearQueueChanges: vi.fn(),
        requestTemplateConfigOpen: vi.fn(),
      };
      return selector ? selector(state) : state;
    });

    renderWithProviders(<QueueCardNode {...createNodeProps(defaultNodeData)} />);

    const card = screen.getByText('default').closest('.relative');
    if (!card) throw new Error('Card not found');
    // Selected state uses gradient background and subtle scale
    expect(card).toHaveClass('scale-[1.01]');
    expect(card).toHaveClass('ring-primary');
  });

  it('should show comparison state when queue is in comparison', () => {
    (useSchedulerStore as any).mockImplementation((selector: any) => {
      const state = {
        comparisonQueues: ['root.default'],
        selectedQueuePath: null,
        selectQueue: mockSelectQueue,
        setPropertyPanelOpen: mockSetPropertyPanelOpen,
        toggleComparisonQueue: mockToggleComparisonQueue,
        getQueueByPath: vi.fn().mockReturnValue({ queueName: 'default' }),
        getChildQueues: vi.fn().mockReturnValue([]),
        selectedNodeLabelFilter: '',
        getQueueLabelCapacity: vi.fn().mockReturnValue({
          capacity: '10',
          maxCapacity: '100',
          absoluteCapacity: '10',
          isLabelSpecific: false,
          label: 'DEFAULT',
          hasAccess: true,
          canUseLabel: true,
        }),
        hasPendingDeletion: vi.fn().mockReturnValue(false),
        clearQueueChanges: vi.fn(),
        requestTemplateConfigOpen: vi.fn(),
        isComparisonModeActive: true,
      };
      return selector ? selector(state) : state;
    });

    renderWithProviders(<QueueCardNode {...createNodeProps(defaultNodeData)} />);

    const checkbox = screen.getByRole('checkbox');
    expect(checkbox).toBeChecked();
  });

  it('should show context menu on right click', async () => {
    const user = userEvent.setup();
    renderWithProviders(<QueueCardNode {...createNodeProps(defaultNodeData)} />);

    // Find the card by its queue name
    const card = screen.getByText('default').closest('[data-slot="context-menu-trigger"]');
    if (!card) throw new Error('Card not found');

    await user.pointer({ keys: '[MouseRight]', target: card });

    expect(await screen.findByText('Edit Properties')).toBeInTheDocument();
    expect(screen.getByText('Stop Queue')).toBeInTheDocument();
    expect(screen.getByText('Add Child Queue')).toBeInTheDocument();
  });

  it('should open property panel on settings tab from context menu', async () => {
    const user = userEvent.setup();
    renderWithProviders(<QueueCardNode {...createNodeProps(defaultNodeData)} />);

    mockSelectQueue.mockClear();
    mockSetPropertyPanelOpen.mockClear();
    mockSetPropertyPanelInitialTab.mockClear();

    const trigger = screen.getByText('default').closest('[data-slot="context-menu-trigger"]');
    if (!trigger) throw new Error('Context menu trigger not found');

    await user.pointer({ keys: '[MouseRight]', target: trigger });

    const editItem = await screen.findByText('Edit Properties');
    await user.click(editItem);

    expect(mockSetPropertyPanelInitialTab).toHaveBeenCalledWith('settings');
    expect(mockSelectQueue).toHaveBeenCalledWith('root.default');
    expect(mockSetPropertyPanelOpen).toHaveBeenCalledWith(true);
    expect(mockSelectQueue).not.toHaveBeenCalledWith(null);
  });

  it('should highlight auto-created queues and disable settings editing', async () => {
    const user = userEvent.setup();
    const autoCreatedNode = {
      ...defaultNodeData,
      creationMethod: 'dynamicLegacy' as const,
      isAutoCreatedQueue: true,
    };

    renderWithProviders(<QueueCardNode {...createNodeProps(autoCreatedNode)} />);

    const card = screen.getByText('default').closest('.relative');
    expect(card).toHaveClass('border-dashed');
    expect(card).toHaveClass('border-amber-400');

    const autoBadge = screen.getByText(/Legacy auto-created/i);
    expect(autoBadge).toBeInTheDocument();

    const trigger = screen.getByText('default').closest('[data-slot="context-menu-trigger"]');
    if (!trigger) throw new Error('Context menu trigger not found');

    await user.pointer({ keys: '[MouseRight]', target: trigger });

    const editItem = await screen.findByText('Edit Properties');
    expect(editItem).toHaveAttribute('data-disabled');
  });

  it('should show new queue status with tooltip', () => {
    const nodeData = {
      ...defaultNodeData,
      stagedStatus: 'new' as const,
    };

    const { container } = renderWithProviders(<QueueCardNode {...createNodeProps(nodeData)} />);

    const card = screen.getByText('default').closest('.relative');
    expect(card).toHaveClass('ring-queue-new');
    expect(card).toHaveClass('opacity-75');
    expect(card).toHaveClass('cursor-default');

    const tooltipTrigger = container.querySelector('[data-slot="tooltip-trigger"]');
    expect(tooltipTrigger).toBeInTheDocument();
  });

  it('should show different status badges based on staged changes', () => {
    const nodeData = {
      ...defaultNodeData,
      stagedStatus: 'modified' as const,
    };

    renderWithProviders(<QueueCardNode {...createNodeProps(nodeData)} />);

    const card = screen.getByText('default').closest('.relative');
    if (!card) throw new Error('Card not found');
    expect(card).toHaveClass('ring-queue-modified');
  });

  it('should show resource usage statistics', () => {
    const nodeData = {
      ...defaultNodeData,
      numApplications: 5,
      resourcesUsed: {
        memory: 1024,
        vCores: 4,
      },
    };

    renderWithProviders(<QueueCardNode {...createNodeProps(nodeData)} />);

    // QueueResourceStats shows total apps, memory and vCores
    expect(screen.getByText('Apps: 5')).toBeInTheDocument();
    expect(screen.getByText('Memory: 1 GB')).toBeInTheDocument(); // formatMemory should convert 1024 MB to 1 GB
    expect(screen.getByText('vCores: 4')).toBeInTheDocument();
  });

  it('should open add queue dialog from context menu', async () => {
    const user = userEvent.setup();
    renderWithProviders(<QueueCardNode {...createNodeProps(defaultNodeData)} />);

    // Find the card by queue name
    const card = screen.getByText('default').closest('[data-slot="context-menu-trigger"]');
    if (!card) throw new Error('Card not found');

    await user.pointer({ keys: '[MouseRight]', target: card });

    const addMenuItem = await screen.findByText('Add Child Queue');
    await user.click(addMenuItem);

    expect(screen.getByTestId('add-queue-dialog')).toBeInTheDocument();
  });

  it('should deselect queue when context menu closes', async () => {
    const user = userEvent.setup();
    (useSchedulerStore as any).mockImplementation((selector: any) => {
      const state = {
        comparisonQueues: [],
        selectedQueuePath: 'root.default',
        selectQueue: mockSelectQueue,
        setPropertyPanelOpen: mockSetPropertyPanelOpen,
        toggleComparisonQueue: mockToggleComparisonQueue,
        getQueueByPath: vi.fn().mockReturnValue({ queueName: 'default' }),
        getChildQueues: vi.fn().mockReturnValue([]),
        selectedNodeLabelFilter: '',
        getQueueLabelCapacity: vi.fn().mockReturnValue({
          capacity: '10',
          maxCapacity: '100',
          absoluteCapacity: '10',
          isLabelSpecific: false,
          label: 'DEFAULT',
          hasAccess: true,
          canUseLabel: true,
        }),
        hasPendingDeletion: vi.fn().mockReturnValue(false),
        clearQueueChanges: vi.fn(),
        requestTemplateConfigOpen: vi.fn(),
      };
      return selector ? selector(state) : state;
    });

    renderWithProviders(<QueueCardNode {...createNodeProps(defaultNodeData)} />);

    // Open context menu
    const card = screen.getByText('default').closest('[data-slot="context-menu-trigger"]');
    if (!card) throw new Error('Card not found');

    await user.pointer({ keys: '[MouseRight]', target: card });

    // Wait for context menu to open
    await screen.findByText('Edit Properties');

    // Close by pressing escape
    await user.keyboard('{Escape}');

    expect(mockSelectQueue).toHaveBeenCalledWith(null);
  });

  describe('Root queue state restrictions', () => {
    it('should disable Stop/Start context menu item for root queue', async () => {
      const user = userEvent.setup();
      const rootNodeData: QueueCardData = {
        ...defaultNodeData,
        queuePath: 'root',
        queueName: 'root',
        queueType: 'parent',
        isLeaf: false,
      };

      renderWithProviders(<QueueCardNode {...createNodeProps(rootNodeData)} />);

      // Find by data-slot="card-title" to get the queue name, then get the context menu trigger
      const rootElements = screen.getAllByText('root');
      const trigger = rootElements[0].closest('[data-slot="context-menu-trigger"]');
      if (!trigger) throw new Error('Context menu trigger not found');

      await user.pointer({ keys: '[MouseRight]', target: trigger });

      const stopQueueItem = await screen.findByText('Stop Queue');
      expect(stopQueueItem).toHaveAttribute('data-disabled');
    });

    it('should enable Stop/Start context menu item for non-root queues', async () => {
      const user = userEvent.setup();

      renderWithProviders(<QueueCardNode {...createNodeProps(defaultNodeData)} />);

      const trigger = screen.getByText('default').closest('[data-slot="context-menu-trigger"]');
      if (!trigger) throw new Error('Context menu trigger not found');

      await user.pointer({ keys: '[MouseRight]', target: trigger });

      const stopQueueItem = await screen.findByText('Stop Queue');
      expect(stopQueueItem).not.toHaveAttribute('data-disabled');
    });

    it('should show Start Queue menu item when queue is stopped', async () => {
      const user = userEvent.setup();
      const stoppedQueueData: QueueCardData = {
        ...defaultNodeData,
        state: 'STOPPED',
      };

      renderWithProviders(<QueueCardNode {...createNodeProps(stoppedQueueData)} />);

      const trigger = screen.getByText('default').closest('[data-slot="context-menu-trigger"]');
      if (!trigger) throw new Error('Context menu trigger not found');

      await user.pointer({ keys: '[MouseRight]', target: trigger });

      const startQueueItem = await screen.findByText('Start Queue');
      expect(startQueueItem).toBeInTheDocument();
      expect(startQueueItem).not.toHaveAttribute('data-disabled');
    });
  });

  it('should show "Undo Delete" instead of "Mark for Deletion" when queue has pending deletion', async () => {
    (useSchedulerStore as any).mockImplementation((selector: any) => {
      const state = {
        comparisonQueues: [],
        selectedQueuePath: null,
        selectQueue: mockSelectQueue,
        setPropertyPanelOpen: mockSetPropertyPanelOpen,
        isPropertyPanelOpen: false,
        propertyPanelInitialTab: 'overview' as const,
        setPropertyPanelInitialTab: mockSetPropertyPanelInitialTab,
        toggleComparisonQueue: mockToggleComparisonQueue,
        getQueueByPath: vi.fn().mockReturnValue({ queueName: 'default' }),
        getChildQueues: vi.fn().mockReturnValue([]),
        selectedNodeLabelFilter: '',
        getQueueLabelCapacity: vi.fn().mockReturnValue({
          capacity: '10',
          maxCapacity: '100',
          absoluteCapacity: '10',
          isLabelSpecific: false,
          label: 'DEFAULT',
          hasAccess: true,
          canUseLabel: true,
        }),
        hasPendingDeletion: vi.fn().mockReturnValue(true),
        clearQueueChanges: vi.fn(),
        requestTemplateConfigOpen: vi.fn(),
        revertQueueDeletion: vi.fn(),
      };
      return selector ? selector(state) : state;
    });

    renderWithProviders(<QueueCardNode {...createNodeProps(defaultNodeData)} />);

    const card = screen.getByText('default');
    await userEvent.pointer({ keys: '[MouseRight]', target: card });

    expect(screen.queryByText('Mark for Deletion')).not.toBeInTheDocument();
    expect(screen.getByText('Undo Delete')).toBeInTheDocument();
  });
});
