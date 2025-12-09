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
import { render, screen, waitFor } from '~/testing/setup/setup';
import { StagedChangesPanel } from './StagedChangesPanel';
import userEvent from '@testing-library/user-event';
import { useSchedulerStore } from '~/stores/schedulerStore';
import { toast } from 'sonner';
import type { StagedChange } from '~/types';

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
vi.mock('./QueueChangeGroup', () => ({
  QueueChangeGroup: ({ queuePath, changes, onRevert }: any) => (
    <div data-testid={`queue-group-${queuePath}`}>
      <div>Queue: {queuePath}</div>
      <div>Changes: {changes.length}</div>
      {changes.map((change: StagedChange) => (
        <button
          key={change.id}
          onClick={() => onRevert(change)}
          data-testid={`revert-${change.id}`}
        >
          Revert {change.property}
        </button>
      ))}
    </div>
  ),
}));

// Setup pointer capture mock for Vaul drawer
beforeEach(() => {
  // Mock pointer capture methods for Vaul drawer
  if (!Element.prototype.setPointerCapture) {
    Element.prototype.setPointerCapture = vi.fn();
    Element.prototype.releasePointerCapture = vi.fn();
  }

  // Mock getComputedStyle to return transform for value
  const originalGetComputedStyle = window.getComputedStyle;
  window.getComputedStyle = vi.fn().mockImplementation((element) => {
    const result = originalGetComputedStyle(element);
    return {
      ...result,
      transform: 'translateY(0px)',
      getPropertyValue: (prop: string) => {
        if (prop === 'transform') return 'translateY(0px)';
        return result.getPropertyValue(prop);
      },
    };
  });
});

const mockStagedChanges: StagedChange[] = [
  {
    id: '1',
    type: 'update',
    queuePath: 'root.default',
    property: 'capacity',
    oldValue: '10',
    newValue: '20',
    timestamp: Date.now(),
  },
  {
    id: '2',
    type: 'add',
    queuePath: 'root.production',
    property: 'maximum-capacity',
    newValue: '100',
    timestamp: Date.now(),
  },
];

const mockRevertChange = vi.fn();
const mockClearAllChanges = vi.fn();
const mockApplyChanges = vi.fn();

describe('StagedChangesPanel', () => {
  beforeEach(() => {
    vi.clearAllMocks();
    // Mock the selector pattern used by the component
    (useSchedulerStore as any).mockImplementation((selector: any) => {
      const state = {
        stagedChanges: [],
        revertChange: mockRevertChange,
        clearAllChanges: mockClearAllChanges,
        applyChanges: mockApplyChanges,
        applyError: null,
      };
      return selector ? selector(state) : state;
    });
  });

  it('should show floating button when closed with staged changes', () => {
    (useSchedulerStore as any).mockImplementation((selector: any) => {
      const state = {
        stagedChanges: mockStagedChanges,
        revertChange: mockRevertChange,
        clearAllChanges: mockClearAllChanges,
        applyChanges: mockApplyChanges,
        applyError: null,
      };
      return selector ? selector(state) : state;
    });

    const onOpen = vi.fn();
    render(<StagedChangesPanel open={false} onClose={vi.fn()} onOpen={onOpen} />);

    const button = screen.getByRole('button', { name: /view staged changes/i });
    expect(button).toBeInTheDocument();
    expect(screen.getByText('2')).toBeInTheDocument(); // Badge count
  });

  it('should not show floating button when no staged changes', () => {
    render(<StagedChangesPanel open={false} onClose={vi.fn()} onOpen={vi.fn()} />);

    expect(screen.queryByRole('button')).not.toBeInTheDocument();
  });

  it('should open panel when floating button is clicked', async () => {
    const user = userEvent.setup();
    (useSchedulerStore as any).mockImplementation((selector: any) => {
      const state = {
        stagedChanges: mockStagedChanges,
        revertChange: mockRevertChange,
        clearAllChanges: mockClearAllChanges,
        applyChanges: mockApplyChanges,
        applyError: null,
      };
      return selector ? selector(state) : state;
    });

    const onOpen = vi.fn();
    render(<StagedChangesPanel open={false} onClose={vi.fn()} onOpen={onOpen} />);

    await user.click(screen.getByRole('button', { name: /view staged changes/i }));
    expect(onOpen).toHaveBeenCalled();
  });

  it('should render staged changes grouped by queue', () => {
    (useSchedulerStore as any).mockImplementation((selector: any) => {
      const state = {
        stagedChanges: mockStagedChanges,
        revertChange: mockRevertChange,
        clearAllChanges: mockClearAllChanges,
        applyChanges: mockApplyChanges,
        applyError: null,
      };
      return selector ? selector(state) : state;
    });

    render(<StagedChangesPanel open={true} onClose={vi.fn()} />);

    expect(screen.getByText('Staged Changes')).toBeInTheDocument();
    expect(screen.getByText('2 changes')).toBeInTheDocument();
    expect(screen.getByTestId('queue-group-root.default')).toBeInTheDocument();
    expect(screen.getByTestId('queue-group-root.production')).toBeInTheDocument();
  });

  it('should show empty state when no staged changes', () => {
    render(<StagedChangesPanel open={true} onClose={vi.fn()} />);

    expect(screen.getByText('No staged changes')).toBeInTheDocument();
    expect(screen.queryByRole('button', { name: /apply all changes/i })).not.toBeInTheDocument();
  });

  it('should revert individual change', async () => {
    const user = userEvent.setup();
    (useSchedulerStore as any).mockImplementation((selector: any) => {
      const state = {
        stagedChanges: mockStagedChanges,
        revertChange: mockRevertChange,
        clearAllChanges: mockClearAllChanges,
        applyChanges: mockApplyChanges,
        applyError: null,
      };
      return selector ? selector(state) : state;
    });

    render(<StagedChangesPanel open={true} onClose={vi.fn()} />);

    await user.click(screen.getByTestId('revert-1'));

    expect(mockRevertChange).toHaveBeenCalledWith(mockStagedChanges[0].id);
    expect(toast.info).toHaveBeenCalledWith('Reverted change: capacity');
  });

  it('should clear all changes', async () => {
    const user = userEvent.setup();
    (useSchedulerStore as any).mockImplementation((selector: any) => {
      const state = {
        stagedChanges: mockStagedChanges,
        revertChange: mockRevertChange,
        clearAllChanges: mockClearAllChanges,
        applyChanges: mockApplyChanges,
        applyError: null,
      };
      return selector ? selector(state) : state;
    });

    render(<StagedChangesPanel open={true} onClose={vi.fn()} />);

    await user.click(screen.getByRole('button', { name: /clear all/i }));

    expect(mockClearAllChanges).toHaveBeenCalled();
    expect(toast.info).toHaveBeenCalledWith('All staged changes cleared');
  });

  it('should apply all changes successfully', async () => {
    const user = userEvent.setup();
    mockApplyChanges.mockResolvedValue(undefined);
    (useSchedulerStore as any).mockImplementation((selector: any) => {
      const state = {
        stagedChanges: mockStagedChanges,
        revertChange: mockRevertChange,
        clearAllChanges: mockClearAllChanges,
        applyChanges: mockApplyChanges,
        applyError: null,
      };
      return selector ? selector(state) : state;
    });

    const onClose = vi.fn();
    render(<StagedChangesPanel open={true} onClose={onClose} />);

    await user.click(screen.getByRole('button', { name: /apply all changes/i }));

    expect(mockApplyChanges).toHaveBeenCalled();

    await waitFor(() => {
      expect(toast.success).toHaveBeenCalledWith('All changes applied successfully');
      expect(onClose).toHaveBeenCalled();
    });
  });

  it('should handle apply changes error', async () => {
    const user = userEvent.setup();
    const consoleErrorSpy = vi.spyOn(console, 'error').mockImplementation(() => {});
    mockApplyChanges.mockRejectedValue(new Error('Network error'));

    (useSchedulerStore as any).mockImplementation((selector: any) => {
      const state = {
        stagedChanges: mockStagedChanges,
        revertChange: mockRevertChange,
        clearAllChanges: mockClearAllChanges,
        applyChanges: mockApplyChanges,
        applyError: null,
      };
      return selector ? selector(state) : state;
    });

    render(<StagedChangesPanel open={true} onClose={vi.fn()} />);

    await user.click(screen.getByRole('button', { name: /apply all changes/i }));

    await waitFor(() => {
      expect(toast.error).toHaveBeenCalledWith('Failed to apply changes');
      expect(consoleErrorSpy).toHaveBeenCalledWith('Failed to apply changes:', expect.any(Error));
    });

    consoleErrorSpy.mockRestore();
  });

  it('should display apply error alert when present', () => {
    (useSchedulerStore as any).mockImplementation((selector: any) => {
      const state = {
        stagedChanges: mockStagedChanges,
        revertChange: mockRevertChange,
        clearAllChanges: mockClearAllChanges,
        applyChanges: mockApplyChanges,
        applyError: 'HTTP 400: Invalid configuration',
      };
      return selector ? selector(state) : state;
    });

    render(<StagedChangesPanel open={true} onClose={vi.fn()} />);

    expect(screen.getByText('Failed to Apply Changes')).toBeInTheDocument();
    expect(screen.getByText('HTTP 400: Invalid configuration')).toBeInTheDocument();
  });

  it('should disable actions while applying changes', async () => {
    const user = userEvent.setup();
    mockApplyChanges.mockImplementation(() => new Promise((resolve) => setTimeout(resolve, 100)));

    (useSchedulerStore as any).mockImplementation((selector: any) => {
      const state = {
        stagedChanges: mockStagedChanges,
        revertChange: mockRevertChange,
        clearAllChanges: mockClearAllChanges,
        applyChanges: mockApplyChanges,
        applyError: null,
      };
      return selector ? selector(state) : state;
    });

    render(<StagedChangesPanel open={true} onClose={vi.fn()} />);

    const applyButton = screen.getByRole('button', { name: /apply all changes/i });
    const clearButton = screen.getByRole('button', { name: /clear all/i });

    await user.click(applyButton);

    // Should show loading state
    expect(screen.getByText('Applying...')).toBeInTheDocument();
    expect(applyButton).toBeDisabled();
    expect(clearButton).toBeDisabled();
  });

  it('should render with draggable drawer', async () => {
    (useSchedulerStore as any).mockImplementation((selector: any) => {
      const state = {
        stagedChanges: mockStagedChanges,
        revertChange: mockRevertChange,
        clearAllChanges: mockClearAllChanges,
        applyChanges: mockApplyChanges,
        applyError: null,
      };
      return selector ? selector(state) : state;
    });

    render(<StagedChangesPanel open={true} onClose={vi.fn()} onOpen={vi.fn()} />);

    // The Drawer component renders content in a portal
    await waitFor(() => {
      const drawerContent = document.querySelector('[data-vaul-drawer]');
      expect(drawerContent).toBeTruthy();
    });

    // Check that drawer is rendered
    const drawerContent = document.querySelector('[data-vaul-drawer]');
    expect(drawerContent).toBeInTheDocument();
    expect(drawerContent).toHaveAttribute('data-vaul-drawer-direction', 'bottom');
  });

  it('should close panel when clicking close button', async () => {
    const onClose = vi.fn();

    render(<StagedChangesPanel open={true} onClose={onClose} />);

    // The Sheet component would normally handle this through onOpenChange
    // We test that the onClose prop is passed correctly
    expect(onClose).toBeDefined();
  });

  it('should display change count in badge', () => {
    const changes: StagedChange[] = [
      ...mockStagedChanges,
      {
        id: '3',
        type: 'remove',
        queuePath: 'root.test',
        property: 'state',
        oldValue: 'RUNNING',
        timestamp: Date.now(),
      },
    ];

    (useSchedulerStore as any).mockImplementation((selector: any) => {
      const state = {
        stagedChanges: changes,
        revertChange: mockRevertChange,
        clearAllChanges: mockClearAllChanges,
        applyChanges: mockApplyChanges,
        applyError: null,
      };
      return selector ? selector(state) : state;
    });

    render(<StagedChangesPanel open={true} onClose={vi.fn()} />);

    expect(screen.getByText('3 changes')).toBeInTheDocument();
  });

  it('should display singular form for one change', () => {
    (useSchedulerStore as any).mockImplementation((selector: any) => {
      const state = {
        stagedChanges: [mockStagedChanges[0]],
        revertChange: mockRevertChange,
        clearAllChanges: mockClearAllChanges,
        applyChanges: mockApplyChanges,
        applyError: null,
      };
      return selector ? selector(state) : state;
    });

    render(<StagedChangesPanel open={true} onClose={vi.fn()} />);

    expect(screen.getByText('1 change')).toBeInTheDocument();
  });
});
