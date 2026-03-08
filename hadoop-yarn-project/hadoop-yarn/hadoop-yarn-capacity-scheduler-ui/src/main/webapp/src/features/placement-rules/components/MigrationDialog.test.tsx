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
import { render, screen, waitFor } from '@testing-library/react';
import userEvent from '@testing-library/user-event';
import { PlacementRulesMigrationDialog } from './MigrationDialog';
import { useSchedulerStore } from '~/stores/schedulerStore';

// Mock the store
vi.mock('~/stores/schedulerStore');

describe('PlacementRulesMigrationDialog', () => {
  const mockStoreState = {
    legacyRules: 'u:alice:root.users.alice,g:developers:root.teams.dev',
    stageGlobalChange: vi.fn(),
    migrateLegacyRules: vi.fn(),
  };

  function mockStore(overrides: Record<string, any> = {}) {
    const state = { ...mockStoreState, ...overrides };
    vi.mocked(useSchedulerStore).mockImplementation((selector?: any) => {
      return selector ? selector(state) : state;
    });
  }

  const defaultProps = {
    open: true,
    onOpenChange: vi.fn(),
  };

  beforeEach(() => {
    vi.clearAllMocks();
    mockStore();
  });

  it('should render when open is true', () => {
    render(<PlacementRulesMigrationDialog {...defaultProps} />);

    expect(screen.getByText('Migrate Legacy Placement Rules')).toBeInTheDocument();
    expect(screen.getByText(/Legacy placement rules have been detected/)).toBeInTheDocument();
  });

  it('should not render when open is false', () => {
    const { container } = render(<PlacementRulesMigrationDialog {...defaultProps} open={false} />);
    expect(container.firstChild).toBeNull();
  });

  it('should display legacy rules preview', () => {
    render(<PlacementRulesMigrationDialog {...defaultProps} />);

    expect(screen.getByText('Current Legacy Rules:')).toBeInTheDocument();
    expect(screen.getByText(mockStoreState.legacyRules)).toBeInTheDocument();
  });

  it('should handle successful migration', async () => {
    const user = userEvent.setup();

    // Mock successful migration from store
    mockStoreState.migrateLegacyRules.mockResolvedValue(undefined);

    render(<PlacementRulesMigrationDialog {...defaultProps} />);

    const migrateButton = screen.getByRole('button', { name: 'Migrate to JSON' });
    await user.click(migrateButton);

    // Check that store's migrateLegacyRules was called
    expect(mockStoreState.migrateLegacyRules).toHaveBeenCalled();

    // Check success message
    expect(screen.getByText(/Successfully converted/)).toBeInTheDocument();
    expect(screen.getByText(/Changes have been staged for review/)).toBeInTheDocument();

    // Check that dialog closes after timeout
    await waitFor(
      () => {
        expect(defaultProps.onOpenChange).toHaveBeenCalledWith(false);
      },
      { timeout: 2500 },
    );
  });

  it('should handle migration errors', async () => {
    const user = userEvent.setup();

    // Mock failed migration from store
    mockStoreState.migrateLegacyRules.mockRejectedValue(new Error('Migration failed'));

    render(<PlacementRulesMigrationDialog {...defaultProps} />);

    const migrateButton = screen.getByRole('button', { name: 'Migrate to JSON' });
    await user.click(migrateButton);

    // Check error display
    await waitFor(() => {
      expect(screen.getByText('Migration failed:')).toBeInTheDocument();
      expect(screen.getByText('Migration failed')).toBeInTheDocument();
    });

    // Dialog should not close on error
    expect(defaultProps.onOpenChange).not.toHaveBeenCalled();
  });

  it('should handle cancel button', async () => {
    const user = userEvent.setup();
    render(<PlacementRulesMigrationDialog {...defaultProps} />);

    const cancelButton = screen.getByRole('button', { name: 'Keep Legacy Rules' });
    await user.click(cancelButton);

    expect(defaultProps.onOpenChange).toHaveBeenCalledWith(false);
  });

  it('should disable migrate button after successful migration', async () => {
    const user = userEvent.setup();

    // Mock successful migration from store
    mockStoreState.migrateLegacyRules.mockResolvedValue(undefined);

    render(<PlacementRulesMigrationDialog {...defaultProps} />);

    const migrateButton = screen.getByRole('button', { name: 'Migrate to JSON' });
    await user.click(migrateButton);

    // After successful migration, button should be disabled
    await waitFor(() => {
      expect(screen.getByRole('button', { name: 'Migrate to JSON' })).toBeDisabled();
    });
  });

  it('should handle missing legacy rules', async () => {
    mockStore({ legacyRules: null });

    render(<PlacementRulesMigrationDialog {...defaultProps} />);

    const migrateButton = screen.getByRole('button', { name: 'Migrate to JSON' });
    expect(migrateButton).toBeDisabled();
  });

  it('should handle exception during migration', async () => {
    const user = userEvent.setup();

    // Mock migration throwing an error from store
    mockStoreState.migrateLegacyRules.mockRejectedValue(new Error('Unexpected error'));

    render(<PlacementRulesMigrationDialog {...defaultProps} />);

    const migrateButton = screen.getByRole('button', { name: 'Migrate to JSON' });
    await user.click(migrateButton);

    // Check error display
    await waitFor(() => {
      expect(screen.getByText('Migration failed:')).toBeInTheDocument();
      expect(screen.getByText('Unexpected error')).toBeInTheDocument();
    });
  });

  it('should handle closing dialog via X button', async () => {
    const user = userEvent.setup();
    render(<PlacementRulesMigrationDialog {...defaultProps} />);

    // Find the close button (X) - it's usually in the dialog header
    const closeButton = screen.getByRole('button', { name: 'Close' });
    await user.click(closeButton);

    expect(defaultProps.onOpenChange).toHaveBeenCalledWith(false);
  });
});
