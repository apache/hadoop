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
import { AddQueueDialog } from './AddQueueDialog';
import userEvent from '@testing-library/user-event';

// Mock the useQueueActions hook
const mockAddChildQueue = vi.fn();
const mockUpdateQueueProperty = vi.fn();
const mockCanAddChildQueue = vi.fn();
const mockGetQueueByPath = vi.fn();
const mockGetChildQueues = vi.fn();
const mockGetQueuePropertyValue = vi.fn();
const mockStageQueueAddition = vi.fn();
const mockOpenCapacityEditor = vi.fn();

vi.mock('../../hooks/useQueueActions', () => ({
  useQueueActions: () => ({
    addChildQueue: mockAddChildQueue,
    updateQueueProperty: mockUpdateQueueProperty,
    canAddChildQueue: mockCanAddChildQueue,
  }),
}));

vi.mock('../../hooks/useCapacityEditor', () => ({
  useCapacityEditor: () => ({
    openCapacityEditor: mockOpenCapacityEditor,
  }),
}));

// Mock the store
vi.mock('~/stores/schedulerStore', () => ({
  useSchedulerStore: vi.fn((selector) => {
    const state = {
      getQueueByPath: mockGetQueueByPath,
      getChildQueues: mockGetChildQueues,
      getQueuePropertyValue: mockGetQueuePropertyValue,
      stagedChanges: [],
      stageQueueAddition: mockStageQueueAddition,
    };

    if (typeof selector === 'function') {
      return selector(state);
    }

    return state;
  }),
}));

describe('AddQueueDialog', () => {
  const mockConsoleError = vi.spyOn(console, 'error').mockImplementation(() => {});

  beforeEach(() => {
    vi.clearAllMocks();
    mockConsoleError.mockClear();
    // Default: can add child queue
    mockCanAddChildQueue.mockReturnValue(true);
    // Default: parent queue exists
    mockGetQueueByPath.mockReturnValue({ queuePath: 'root.production', queueName: 'production' });
    mockGetChildQueues.mockReturnValue([]);
    mockGetQueuePropertyValue.mockReturnValue({ value: '', isStaged: false });
    mockUpdateQueueProperty.mockReset();
    mockOpenCapacityEditor.mockReset();
  });

  afterAll(() => {
    mockConsoleError.mockRestore();
  });

  it('should not render when closed', () => {
    render(<AddQueueDialog open={false} parentQueuePath="root" onClose={vi.fn()} />);

    expect(screen.queryByRole('dialog')).not.toBeInTheDocument();
  });

  it('should render dialog with parent queue information', () => {
    render(<AddQueueDialog open={true} parentQueuePath="root.production" onClose={vi.fn()} />);

    expect(screen.getByRole('dialog')).toBeInTheDocument();
    expect(screen.getByRole('heading', { name: /add child queue/i })).toBeInTheDocument();
    expect(screen.getByText(/Creating new queue under:/)).toBeInTheDocument();
    expect(screen.getByText('production')).toBeInTheDocument();
  });

  it('should validate queue name format', async () => {
    const user = userEvent.setup();
    render(<AddQueueDialog open={true} parentQueuePath="root" onClose={vi.fn()} />);

    const nameInput = screen.getByLabelText(/queue name/i);
    const submitButton = screen.getByRole('button', { name: /create queue/i });

    // Initially button should be disabled (no name entered)
    expect(submitButton).toBeDisabled();

    // Test invalid name with dots - button should remain disabled
    await user.type(nameInput, 'queue.with.dots');

    // Wait for validation to update
    await waitFor(() => {
      expect(submitButton).toBeDisabled();
    });

    expect(mockAddChildQueue).not.toHaveBeenCalled();
    expect(mockOpenCapacityEditor).not.toHaveBeenCalled();
  });

  it('should validate queue name with special characters', async () => {
    const user = userEvent.setup();

    render(<AddQueueDialog open={true} parentQueuePath="root" onClose={vi.fn()} />);

    const nameInput = screen.getByLabelText(/queue name/i);
    const submitButton = screen.getByRole('button', { name: /create queue/i });

    await user.type(nameInput, 'queue@#$%');

    // Button should be disabled for invalid characters
    await waitFor(() => {
      expect(submitButton).toBeDisabled();
    });

    expect(mockAddChildQueue).not.toHaveBeenCalled();
  });

  it('should stage new queue on valid submission', async () => {
    const user = userEvent.setup();
    const onClose = vi.fn();

    render(<AddQueueDialog open={true} parentQueuePath="root.production" onClose={onClose} />);

    const nameInput = screen.getByLabelText(/queue name/i);

    // Fill all required fields
    await user.type(nameInput, 'newqueue');

    const submitButton = screen.getByRole('button', { name: /create queue/i });
    expect(submitButton).not.toBeDisabled();

    await user.click(submitButton);

    await waitFor(() => {
      expect(mockAddChildQueue).toHaveBeenCalledWith(
        'root.production',
        'newqueue',
        expect.objectContaining({
          capacity: '10',
          'maximum-capacity': '100', // Default value
          state: 'RUNNING', // Default value
        }),
      );
      expect(onClose).toHaveBeenCalled();
      expect(mockOpenCapacityEditor).toHaveBeenCalledWith(
        expect.objectContaining({
          parentQueuePath: 'root.production',
          originQueuePath: 'root.production.newqueue',
          originQueueName: 'newqueue',
          capacityValue: '10',
          maxCapacityValue: '100',
          markOriginAsNew: true,
          queueState: 'RUNNING',
        }),
      );
    });
  });

  it('should clear form and close on cancel', async () => {
    const user = userEvent.setup();
    const onClose = vi.fn();

    render(<AddQueueDialog open={true} parentQueuePath="root" onClose={onClose} />);

    const nameInput = screen.getByLabelText(/queue name/i);
    const cancelButton = screen.getByRole('button', { name: /cancel/i });

    await user.type(nameInput, 'some-text');
    await user.click(cancelButton);

    expect(onClose).toHaveBeenCalled();
    expect(mockAddChildQueue).not.toHaveBeenCalled();
    expect(mockOpenCapacityEditor).not.toHaveBeenCalled();
  });
});
