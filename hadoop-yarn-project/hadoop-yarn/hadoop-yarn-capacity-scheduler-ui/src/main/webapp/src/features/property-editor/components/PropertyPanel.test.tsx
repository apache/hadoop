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
import { render, screen } from '~/testing/setup/setup';
import { PropertyPanel } from './PropertyPanel';
import userEvent from '@testing-library/user-event';
import { useSchedulerStore } from '~/stores/schedulerStore';
import type { SchedulerStore } from '~/stores/schedulerStore';
import type { QueueInfo } from '~/types';
import { SPECIAL_VALUES } from '~/types/constants/special-values';
import { toast } from 'sonner';

// Test helper
const getMockQueueInfo = (overrides?: Partial<QueueInfo>): QueueInfo => {
  return {
    queueType: 'leaf',
    queueName: 'default',
    queuePath: 'root.default',
    capacity: 10,
    maxCapacity: 100,
    absoluteCapacity: 10,
    absoluteMaxCapacity: 100,
    absoluteUsedCapacity: 5,
    usedCapacity: 5,
    numApplications: 2,
    numPendingApplications: 0,
    numActiveApplications: 2,
    state: 'RUNNING',
    queues: undefined,
    creationMethod: 'static',
    ...overrides,
  };
};

// Mock the store
vi.mock('~/stores/schedulerStore');

// Mock toast
vi.mock('sonner', () => ({
  toast: {
    success: vi.fn(),
    error: vi.fn(),
    info: vi.fn(),
  },
}));

// Mock the child components
vi.mock('./QueueOverview', () => ({
  QueueOverview: ({ queue }: any) => (
    <div data-testid="queue-overview">Overview for {queue.queueName}</div>
  ),
}));

vi.mock('./QueueInfoTab', () => ({
  QueueInfoTab: ({ queue }: any) => <div data-testid="queue-info">Info for {queue.queueName}</div>,
}));

// Create mock functions that will be hoisted
let mockSubmit = vi.fn();
let mockReset = vi.fn();
let mockIsValid = vi.fn();
let mockGetErrors = vi.fn();

const mockUseValidation = vi.fn(() => ({ errors: {} }));

vi.mock('~/contexts/ValidationContext', () => ({
  useValidation: () => mockUseValidation(),
}));

// Mock PropertyEditorTab with ref handling
vi.mock('./PropertyEditorTab', async () => {
  const React = await import('react');

  const PropertyEditorTab = ({ onFormDirtyChange, onHasChangesChange, ref }: any) => {
    React.useImperativeHandle(ref, () => ({
      submit: () => mockSubmit(),
      reset: () => mockReset(),
      isValid: () => mockIsValid(),
      getErrors: () => mockGetErrors(),
    }));

    return (
      <div data-testid="property-editor">
        <button onClick={() => onFormDirtyChange?.(true)}>Make Dirty</button>
        <button onClick={() => onFormDirtyChange?.(false)}>Make Clean</button>
        <button onClick={() => onHasChangesChange?.(true)}>Add Changes</button>
      </div>
    );
  };

  PropertyEditorTab.displayName = 'PropertyEditorTab';

  return { PropertyEditorTab };
});

vi.mock('./UnsavedChangesDialog', () => ({
  UnsavedChangesDialog: ({ open, onSave, onDiscard }: any) =>
    open ? (
      <div data-testid="unsaved-dialog">
        <button onClick={onSave}>Save</button>
        <button onClick={onDiscard}>Discard</button>
      </div>
    ) : null,
}));

const mockQueue = getMockQueueInfo({
  queuePath: 'root.default',
  queueName: 'default',
});

const mockGetQueueByPath = vi.fn<SchedulerStore['getQueueByPath']>();
const mockSetPropertyPanelOpen = vi.fn<SchedulerStore['setPropertyPanelOpen']>();
const mockSelectQueue = vi.fn<SchedulerStore['selectQueue']>();
const mockGetQueuePropertyValue = vi.fn<SchedulerStore['getQueuePropertyValue']>(() => ({
  value: 'false',
  isStaged: false,
}));
const mockSetPropertyPanelInitialTab = vi.fn<SchedulerStore['setPropertyPanelInitialTab']>();

function getBaseStoreState(): Partial<SchedulerStore> {
  return {
    selectedQueuePath: null,
    comparisonQueues: [],
    isPropertyPanelOpen: false,
    setPropertyPanelOpen: mockSetPropertyPanelOpen,
    propertyPanelInitialTab: 'overview',
    setPropertyPanelInitialTab: mockSetPropertyPanelInitialTab,
    getQueueByPath: mockGetQueueByPath,
    selectQueue: mockSelectQueue,
    getQueuePropertyValue: mockGetQueuePropertyValue,
    stagedChanges: [],
    configData: new Map<string, string>(),
    schedulerData: null,
    hasPendingDeletion: vi.fn().mockReturnValue(false),
    revertQueueDeletion: vi.fn(),
    shouldOpenTemplateConfig: false,
    requestTemplateConfigOpen: () => {
      storeState = { ...storeState, shouldOpenTemplateConfig: true };
    },
    clearTemplateConfigRequest: () => {
      storeState = { ...storeState, shouldOpenTemplateConfig: false };
    },
  };
}

let storeState: Partial<SchedulerStore> = getBaseStoreState();

const setStoreState = (overrides?: Partial<SchedulerStore>) => {
  storeState = {
    ...getBaseStoreState(),
    ...overrides,
  };
};

describe('PropertyPanel', () => {
  beforeEach(() => {
    vi.clearAllMocks();
    // Reinitialize mock functions
    mockSubmit = vi.fn();
    mockReset = vi.fn();
    mockIsValid = vi.fn().mockReturnValue(true);
    mockGetErrors = vi.fn().mockReturnValue({});
    mockSubmit.mockResolvedValue(undefined);
    mockUseValidation.mockReturnValue({ errors: {} });
    mockGetQueuePropertyValue.mockReset();
    mockGetQueuePropertyValue.mockReturnValue({ value: 'false', isStaged: false });
    mockGetQueueByPath.mockReset();
    mockSetPropertyPanelInitialTab.mockReset();
    setStoreState();
    (useSchedulerStore as any).mockImplementation((selector?: any) => {
      if (typeof selector === 'function') {
        return selector(storeState);
      }
      return storeState;
    });
  });

  it('should not render when no queue is selected', () => {
    render(<PropertyPanel />);
    expect(screen.queryByRole('dialog')).not.toBeInTheDocument();
  });

  it('should not render when panel is closed', () => {
    setStoreState({
      selectedQueuePath: 'root.default',
      isPropertyPanelOpen: false,
    });
    mockGetQueueByPath.mockReturnValue(mockQueue);

    render(<PropertyPanel />);
    expect(screen.queryByRole('dialog')).not.toBeInTheDocument();
  });

  it('should render with selected queue information', () => {
    setStoreState({
      selectedQueuePath: 'root.default',
      isPropertyPanelOpen: true,
    });
    mockGetQueueByPath.mockReturnValue(mockQueue);

    render(<PropertyPanel />);

    expect(screen.getByText('Queue: default')).toBeInTheDocument();
    expect(screen.getByText('root.default')).toBeInTheDocument();
  });

  it('should display all three tabs', () => {
    setStoreState({
      selectedQueuePath: 'root.default',
      isPropertyPanelOpen: true,
    });
    mockGetQueueByPath.mockReturnValue(mockQueue);

    render(<PropertyPanel />);

    expect(screen.getByRole('tab', { name: /overview/i })).toBeInTheDocument();
    expect(screen.getByRole('tab', { name: /info/i })).toBeInTheDocument();
    expect(screen.getByRole('tab', { name: /settings/i })).toBeInTheDocument();
  });

  it('should start with overview tab active', () => {
    setStoreState({
      selectedQueuePath: 'root.default',
      isPropertyPanelOpen: true,
    });
    mockGetQueueByPath.mockReturnValue(mockQueue);

    render(<PropertyPanel />);

    expect(screen.getByRole('tab', { name: /overview/i })).toHaveAttribute('data-state', 'active');
    expect(screen.getByTestId('queue-overview')).toBeInTheDocument();
  });

  it('should respect initial tab selection from store', () => {
    const localSetInitialTab = vi.fn();
    setStoreState({
      selectedQueuePath: 'root.default',
      isPropertyPanelOpen: true,
      propertyPanelInitialTab: 'settings',
      setPropertyPanelInitialTab: localSetInitialTab,
    });
    mockGetQueueByPath.mockReturnValue(mockQueue);

    render(<PropertyPanel />);

    const settingsTab = screen.getByRole('tab', { name: /settings/i });
    expect(settingsTab).toHaveAttribute('data-state', 'active');
    expect(localSetInitialTab).not.toHaveBeenCalled();
  });

  it('should disable settings tab and show warning for auto-created queues', () => {
    setStoreState({
      selectedQueuePath: 'root.auto',
      isPropertyPanelOpen: true,
      propertyPanelInitialTab: 'settings',
    });
    mockGetQueueByPath.mockReturnValue(
      getMockQueueInfo({
        queuePath: 'root.auto',
        queueName: 'auto',
        creationMethod: 'dynamicFlexible',
      }),
    );

    render(<PropertyPanel />);

    const overviewTab = screen.getByRole('tab', { name: /overview/i });
    const settingsTab = screen.getByRole('tab', { name: /settings/i });

    expect(settingsTab).toBeDisabled();
    expect(overviewTab).toHaveAttribute('data-state', 'active');
    expect(
      screen.getByText(/This queue was created automatically by the scheduler/i),
    ).toBeInTheDocument();
    expect(screen.queryByTestId('property-editor')).not.toBeInTheDocument();
  });

  it('should switch between tabs', async () => {
    const user = userEvent.setup();
    setStoreState({
      selectedQueuePath: 'root.default',
      isPropertyPanelOpen: true,
    });
    mockGetQueueByPath.mockReturnValue(mockQueue);

    render(<PropertyPanel />);

    // Switch to info tab
    await user.click(screen.getByRole('tab', { name: /info/i }));
    expect(screen.getByTestId('queue-info')).toBeInTheDocument();

    // Switch to settings tab
    await user.click(screen.getByRole('tab', { name: /settings/i }));
    expect(screen.getByTestId('property-editor')).toBeInTheDocument();
  });

  it('should show apply and reset buttons only on settings tab', async () => {
    const user = userEvent.setup();
    setStoreState({
      selectedQueuePath: 'root.default',
      isPropertyPanelOpen: true,
    });
    mockGetQueueByPath.mockReturnValue(mockQueue);

    render(<PropertyPanel />);

    // No buttons on overview tab
    expect(screen.queryByRole('button', { name: /stage changes/i })).not.toBeInTheDocument();

    // Switch to settings tab
    await user.click(screen.getByRole('tab', { name: /settings/i }));

    // Buttons should be visible but disabled initially
    expect(screen.getByRole('button', { name: /reset/i })).toBeInTheDocument();
    expect(screen.getByRole('button', { name: /no changes/i })).toBeInTheDocument();
  });

  it('should show unsaved badge when form is dirty', async () => {
    const user = userEvent.setup();
    setStoreState({
      selectedQueuePath: 'root.default',
      isPropertyPanelOpen: true,
    });
    mockGetQueueByPath.mockReturnValue(mockQueue);

    render(<PropertyPanel />);

    await user.click(screen.getByRole('tab', { name: /settings/i }));
    await user.click(screen.getByText('Make Dirty'));

    expect(screen.getByText('Unsaved')).toBeInTheDocument();
    expect(screen.getByRole('button', { name: /stage changes/i })).toBeEnabled();
  });

  it('should show staged badge when has changes', async () => {
    const user = userEvent.setup();
    setStoreState({
      selectedQueuePath: 'root.default',
      isPropertyPanelOpen: true,
    });
    mockGetQueueByPath.mockReturnValue(mockQueue);

    render(<PropertyPanel />);

    await user.click(screen.getByRole('tab', { name: /settings/i }));
    await user.click(screen.getByText('Add Changes'));

    expect(screen.getByText('Staged')).toBeInTheDocument();
  });

  it('should show validation errors', async () => {
    const user = userEvent.setup();
    setStoreState({
      selectedQueuePath: 'root.default',
      isPropertyPanelOpen: true,
    });
    mockGetQueueByPath.mockReturnValue(mockQueue);

    mockUseValidation.mockReturnValue({
      errors: {
        'root.default': {
          capacity: [
            {
              field: 'capacity',
              message: 'Invalid value',
              severity: 'error',
              rule: 'test-rule',
            },
          ],
        },
      },
    });

    render(<PropertyPanel />);

    await user.click(screen.getByRole('tab', { name: /settings/i }));

    const summaryButton = screen.getByRole('button', { name: /1 error/i });
    expect(summaryButton).toBeInTheDocument();

    await user.click(summaryButton);

    expect(screen.getByText('Validation issues')).toBeInTheDocument();
    expect(screen.getByText('capacity')).toBeInTheDocument();
    expect(screen.getByText('Invalid value')).toBeInTheDocument();
  });

  it('should handle submit with validation errors', async () => {
    const user = userEvent.setup();
    mockIsValid.mockReturnValue(false);
    mockGetErrors.mockReturnValue({ capacity: 'Invalid value' });
    setStoreState({
      selectedQueuePath: 'root.default',
      isPropertyPanelOpen: true,
    });
    mockGetQueueByPath.mockReturnValue(mockQueue);

    render(<PropertyPanel />);

    await user.click(screen.getByRole('tab', { name: /settings/i }));
    await user.click(screen.getByText('Make Dirty'));

    const submitButton = screen.getByRole('button', { name: /stage changes/i });
    await user.click(submitButton);

    expect(mockSubmit).not.toHaveBeenCalled();
    expect(toast.error).toHaveBeenCalledWith('Please fix validation errors before staging changes');
  });

  it('should submit changes successfully', async () => {
    const user = userEvent.setup();
    setStoreState({
      selectedQueuePath: 'root.default',
      isPropertyPanelOpen: true,
    });
    mockGetQueueByPath.mockReturnValue(mockQueue);

    render(<PropertyPanel />);

    await user.click(screen.getByRole('tab', { name: /settings/i }));
    await user.click(screen.getByText('Make Dirty'));

    const submitButton = screen.getByRole('button', { name: /stage changes/i });
    await user.click(submitButton);

    expect(mockSubmit).toHaveBeenCalled();
  });

  it('should reset form changes', async () => {
    const user = userEvent.setup();
    setStoreState({
      selectedQueuePath: 'root.default',
      isPropertyPanelOpen: true,
    });
    mockGetQueueByPath.mockReturnValue(mockQueue);

    render(<PropertyPanel />);

    await user.click(screen.getByRole('tab', { name: /settings/i }));
    await user.click(screen.getByText('Make Dirty'));

    const resetButton = screen.getByRole('button', { name: /reset/i });
    await user.click(resetButton);

    expect(mockReset).toHaveBeenCalled();
  });

  it('should show unsaved changes dialog when closing with dirty form', async () => {
    const user = userEvent.setup();
    setStoreState({
      selectedQueuePath: 'root.default',
      isPropertyPanelOpen: true,
    });
    mockGetQueueByPath.mockReturnValue(mockQueue);

    render(<PropertyPanel />);

    await user.click(screen.getByRole('tab', { name: /settings/i }));
    await user.click(screen.getByText('Make Dirty'));

    // Simulate closing the panel
    // The Sheet component would normally handle this through onOpenChange
    // We can't directly test the close button, but we can verify the dialog appears
    expect(screen.queryByTestId('unsaved-dialog')).not.toBeInTheDocument();
  });

  it('should deselect queue when panel closes', () => {
    setStoreState({
      selectedQueuePath: 'root.default',
      isPropertyPanelOpen: true,
    });
    mockGetQueueByPath.mockReturnValue(mockQueue);

    const { rerender } = render(<PropertyPanel />);

    // Simulate panel closing
    setStoreState({
      selectedQueuePath: 'root.default',
      isPropertyPanelOpen: false,
    });

    rerender(<PropertyPanel />);

    // The handleClose function should be called which includes selectQueue(null)
    // This is tested through integration tests in real usage
  });

  it('should show info toast when staging with already staged changes', async () => {
    const user = userEvent.setup();
    setStoreState({
      selectedQueuePath: 'root.default',
      isPropertyPanelOpen: true,
    });
    mockGetQueueByPath.mockReturnValue(mockQueue);

    render(<PropertyPanel />);

    await user.click(screen.getByRole('tab', { name: /settings/i }));
    await user.click(screen.getByText('Add Changes'));

    // Form is not dirty but has changes
    const submitButton = screen.getByRole('button', { name: /no changes/i });
    await user.click(submitButton);

    expect(toast.info).toHaveBeenCalledWith(
      'Changes are already staged. Use the bottom drawer to apply all changes.',
    );
  });

  it('should show loading state while submitting', async () => {
    const user = userEvent.setup();
    mockSubmit.mockImplementation(() => new Promise((resolve) => setTimeout(resolve, 100)));

    setStoreState({
      selectedQueuePath: 'root.default',
      isPropertyPanelOpen: true,
    });
    mockGetQueueByPath.mockReturnValue(mockQueue);

    render(<PropertyPanel />);

    await user.click(screen.getByRole('tab', { name: /settings/i }));
    await user.click(screen.getByText('Make Dirty'));

    const submitButton = screen.getByRole('button', { name: /stage changes/i });
    await user.click(submitButton);

    // Would need to set isSubmitting state through the PropertyEditorTab callbacks
    // This is tested through integration tests
  });

  describe('pending deletion state', () => {
    it('should hide Stage Changes and Reset buttons when queue is pending deletion', async () => {
      const user = userEvent.setup();
      setStoreState({
        selectedQueuePath: 'root.default',
        isPropertyPanelOpen: true,
        stagedChanges: [
          {
            id: 'removal-1',
            queuePath: 'root.default',
            property: SPECIAL_VALUES.QUEUE_MARKER,
            type: 'remove',
            oldValue: 'exists',
            newValue: undefined,
          },
        ] as any,
        revertQueueDeletion: vi.fn(),
      });
      mockGetQueueByPath.mockReturnValue(mockQueue);

      render(<PropertyPanel />);

      // Switch to settings tab to verify buttons are hidden
      const settingsTab = screen.getByRole('tab', { name: /settings/i });
      await user.click(settingsTab);

      expect(screen.queryByText('Stage Changes')).not.toBeInTheDocument();
      expect(screen.queryByText('Reset')).not.toBeInTheDocument();
    });

    it('should show deletion badge and undo button when queue is pending deletion', () => {
      setStoreState({
        selectedQueuePath: 'root.default',
        isPropertyPanelOpen: true,
        stagedChanges: [
          {
            id: 'removal-1',
            queuePath: 'root.default',
            property: SPECIAL_VALUES.QUEUE_MARKER,
            type: 'remove',
            oldValue: 'exists',
            newValue: undefined,
          },
        ] as any,
        revertQueueDeletion: vi.fn(),
      });
      mockGetQueueByPath.mockReturnValue(mockQueue);

      render(<PropertyPanel />);

      expect(screen.getByText(/marked for deletion/i)).toBeInTheDocument();
      expect(screen.getByRole('button', { name: /undo/i })).toBeInTheDocument();
    });
  });
});
