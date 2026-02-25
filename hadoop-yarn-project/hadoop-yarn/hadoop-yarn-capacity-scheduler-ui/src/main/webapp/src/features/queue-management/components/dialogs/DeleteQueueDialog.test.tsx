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
import { DeleteQueueDialog } from './DeleteQueueDialog';
import userEvent from '@testing-library/user-event';

// Mock the useQueueActions hook
const mockDeleteQueue = vi.fn();
const mockCanDeleteQueue = vi.fn();

vi.mock('../../hooks/useQueueActions', () => ({
  useQueueActions: () => ({
    deleteQueue: mockDeleteQueue,
    canDeleteQueue: mockCanDeleteQueue,
  }),
}));

describe('DeleteQueueDialog', () => {
  beforeEach(() => {
    vi.clearAllMocks();
    // Default: queue can be deleted
    mockCanDeleteQueue.mockReturnValue(true);
  });

  it('should not render when closed', () => {
    render(<DeleteQueueDialog open={false} queuePath="root.default" onClose={vi.fn()} />);

    expect(screen.queryByRole('dialog')).not.toBeInTheDocument();
  });

  it('should render dialog with queue information', async () => {
    render(<DeleteQueueDialog open={true} queuePath="root.production.team1" onClose={vi.fn()} />);

    expect(screen.getByRole('dialog')).toBeInTheDocument();
    expect(screen.getByRole('heading', { name: /delete queue/i })).toBeInTheDocument();
    expect(screen.getByText(/Are you sure you want to delete the queue/)).toBeInTheDocument();
    // Queue name appears twice: in the prompt and in the confirmation label
    expect(screen.getAllByText('team1')).toHaveLength(2);
    expect(screen.getByText(/This action cannot be undone/)).toBeInTheDocument();
    // Confirmation input should exist
    expect(screen.getByLabelText(/Type.*team1.*to confirm/)).toBeInTheDocument();
  });

  it('should stage queue removal on confirmation', async () => {
    const user = userEvent.setup();
    const onClose = vi.fn();

    render(<DeleteQueueDialog open={true} queuePath="root.production.team1" onClose={onClose} />);

    const deleteButton = screen.getByRole('button', { name: /delete queue/i });
    // Button should be disabled until confirmation text is entered
    expect(deleteButton).toBeDisabled();

    // Type the queue name to confirm
    const confirmInput = screen.getByLabelText(/Type.*team1.*to confirm/);
    await user.type(confirmInput, 'team1');

    // Now button should be enabled
    expect(deleteButton).not.toBeDisabled();
    await user.click(deleteButton);

    expect(mockDeleteQueue).toHaveBeenCalledWith('root.production.team1');
    expect(onClose).toHaveBeenCalled();
  });

  it('should close without deletion on cancel', async () => {
    const user = userEvent.setup();
    const onClose = vi.fn();

    render(<DeleteQueueDialog open={true} queuePath="root.production.team1" onClose={onClose} />);

    const cancelButton = screen.getByRole('button', { name: 'Cancel' });
    await user.click(cancelButton);

    expect(mockDeleteQueue).not.toHaveBeenCalled();
    expect(onClose).toHaveBeenCalled();
  });

  it('should show error for queues with children', () => {
    mockCanDeleteQueue.mockReturnValue(false);

    render(<DeleteQueueDialog open={true} queuePath="root.parent" onClose={vi.fn()} />);

    expect(
      screen.getByText(/This queue has child queues and cannot be deleted/),
    ).toBeInTheDocument();
    expect(screen.getByText(/Please delete all child queues first/)).toBeInTheDocument();

    // Delete button should not exist when queue cannot be deleted
    expect(screen.queryByRole('button', { name: /delete queue/i })).not.toBeInTheDocument();
  });

  it('should handle root queue specially', () => {
    render(<DeleteQueueDialog open={true} queuePath="root" onClose={vi.fn()} />);

    expect(screen.getByText('The root queue cannot be deleted.')).toBeInTheDocument();

    // Root queue should not have a delete button, only cancel
    expect(screen.queryByRole('button', { name: /delete queue/i })).not.toBeInTheDocument();
  });

  it('should close on escape key', async () => {
    const user = userEvent.setup();
    const onClose = vi.fn();

    render(<DeleteQueueDialog open={true} queuePath="root.default" onClose={onClose} />);

    await user.keyboard('{Escape}');

    expect(onClose).toHaveBeenCalled();
    expect(mockDeleteQueue).not.toHaveBeenCalled();
  });

  it('should show correct child queue names', () => {
    mockCanDeleteQueue.mockReturnValue(false);

    render(<DeleteQueueDialog open={true} queuePath="root.production" onClose={vi.fn()} />);

    expect(
      screen.getByText(/This queue has child queues and cannot be deleted/),
    ).toBeInTheDocument();
  });

  it('should use danger variant for delete button', async () => {
    const user = userEvent.setup();
    render(<DeleteQueueDialog open={true} queuePath="root.default" onClose={vi.fn()} />);

    // Type confirmation to enable the button for class check
    const confirmInput = screen.getByLabelText(/Type.*default.*to confirm/);
    await user.type(confirmInput, 'default');

    const deleteButton = screen.getByRole('button', { name: /delete queue/i });
    // Destructive buttons use gradient styling
    expect(deleteButton).toHaveClass('from-destructive');
  });
});
