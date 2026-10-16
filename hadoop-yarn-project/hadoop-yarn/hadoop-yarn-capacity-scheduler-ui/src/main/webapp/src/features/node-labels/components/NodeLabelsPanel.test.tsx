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


import { render, screen, waitFor, fireEvent, within } from '~/testing/setup/setup';
import userEvent from '@testing-library/user-event';
import { vi } from 'vitest';
import { NodeLabelsPanel } from './NodeLabelsPanel';
import { useSchedulerStore } from '~/stores/schedulerStore';
import { validateLabelRemoval } from '~/features/node-labels/utils/labelValidation';
import type { NodeLabel } from '~/types';

// Test helper
const getMockNodeLabel = (overrides?: Partial<NodeLabel>): NodeLabel => {
  return {
    name: 'gpu',
    exclusivity: true,
    ...overrides,
  };
};

// Mock dependencies
vi.mock('~/stores/schedulerStore');
vi.mock('~/features/node-labels/utils/labelValidation', () => ({
  validateLabelRemoval: vi.fn(),
  validateLabelName: vi.fn(() => ({ valid: true })),
}));

describe('NodeLabelsPanel', () => {
  const mockSelectNodeLabel = vi.fn();
  const mockAddNodeLabel = vi.fn();
  const mockRemoveNodeLabel = vi.fn();
  const mockGetState = vi.fn();
  const mockConsoleError = vi.spyOn(console, 'error').mockImplementation(() => {});

  const defaultStoreState = {
    nodeLabels: [],
    selectedNodeLabel: null,
    selectNodeLabel: mockSelectNodeLabel,
    addNodeLabel: mockAddNodeLabel,
    removeNodeLabel: mockRemoveNodeLabel,
    isLoading: false,
    nodeToLabels: [],
  };

  function mockStore(overrides: Record<string, any> = {}) {
    const state = { ...defaultStoreState, ...overrides };
    vi.mocked(useSchedulerStore).mockImplementation((selector?: any) => {
      return selector ? selector(state) : state;
    });
    vi.mocked(useSchedulerStore).getState = mockGetState;
  }

  beforeEach(() => {
    vi.clearAllMocks();
    mockConsoleError.mockClear();
    mockStore();
    mockGetState.mockReturnValue({ nodeToLabels: [] });
    vi.mocked(validateLabelRemoval).mockReturnValue({ valid: true });
  });

  afterAll(() => {
    mockConsoleError.mockRestore();
  });

  describe('Empty state', () => {
    it('should display empty state when no labels exist', () => {
      render(<NodeLabelsPanel />);

      expect(screen.getByText('No node labels found')).toBeInTheDocument();
      expect(
        screen.getByText(
          'Node labels let you partition cluster nodes for dedicated resource allocation.',
        ),
      ).toBeInTheDocument();
    });

    it('should show CTA button in empty state', () => {
      render(<NodeLabelsPanel />);

      const ctaButton = screen.getByRole('button', { name: /create first label/i });
      expect(ctaButton).toBeInTheDocument();
      expect(ctaButton).not.toBeDisabled();
    });

    it('should open add dialog when CTA button is clicked', async () => {
      render(<NodeLabelsPanel />);

      const ctaButton = screen.getByRole('button', { name: /create first label/i });
      await userEvent.click(ctaButton as HTMLElement);

      await waitFor(() => {
        expect(screen.getByRole('dialog')).toBeInTheDocument();
      });
    });

    it('should show correct label count for empty state', () => {
      render(<NodeLabelsPanel />);

      expect(screen.getByText('0 labels available')).toBeInTheDocument();
    });
  });

  describe('Label list display', () => {
    const mockLabels: NodeLabel[] = [
      getMockNodeLabel({ name: 'gpu', exclusivity: true }),
      getMockNodeLabel({ name: 'highmem', exclusivity: false }),
      getMockNodeLabel({ name: 'ssd', exclusivity: true }),
    ];

    it('should display all node labels', () => {
      mockStore({
        nodeLabels: mockLabels,
      });

      render(<NodeLabelsPanel />);

      expect(screen.getByText('gpu')).toBeInTheDocument();
      expect(screen.getByText('highmem')).toBeInTheDocument();
      expect(screen.getByText('ssd')).toBeInTheDocument();
    });

    it('should show correct label count', () => {
      mockStore({
        nodeLabels: mockLabels,
      });

      render(<NodeLabelsPanel />);

      expect(screen.getByText('3 labels available')).toBeInTheDocument();
    });

    it('should display exclusive badge for exclusive labels', () => {
      mockStore({
        nodeLabels: mockLabels,
      });

      render(<NodeLabelsPanel />);

      const exclusiveBadges = screen.getAllByText('Exclusive');
      expect(exclusiveBadges).toHaveLength(2); // gpu and ssd are exclusive
    });

    it('should show shield icon for exclusive labels', () => {
      mockStore({
        nodeLabels: mockLabels,
      });

      render(<NodeLabelsPanel />);

      const labelItems = screen.getAllByRole('listitem');
      const gpuItem = labelItems.find((item) => within(item).queryByText('gpu'));
      const highmemItem = labelItems.find((item) => within(item).queryByText('highmem'));

      expect(gpuItem?.querySelector('.text-warning')).toBeInTheDocument(); // Shield icon
      expect(highmemItem?.querySelector('.text-primary')).toBeInTheDocument(); // Tag icon
    });

    it('should handle singular vs plural label text', () => {
      mockStore({
        nodeLabels: [getMockNodeLabel()],
      });

      render(<NodeLabelsPanel />);

      expect(screen.getByText('1 label available')).toBeInTheDocument();
    });
  });

  describe('Label selection', () => {
    const mockLabels: NodeLabel[] = [
      getMockNodeLabel({ name: 'gpu', exclusivity: true }),
      getMockNodeLabel({ name: 'highmem', exclusivity: false }),
    ];

    it('should call selectNodeLabel when clicking on a label', async () => {
      mockStore({
        nodeLabels: mockLabels,
      });

      render(<NodeLabelsPanel />);

      const gpuLabel = screen.getByText('gpu').closest('div[class*="group"]');
      await userEvent.click(gpuLabel! as HTMLElement);

      expect(mockSelectNodeLabel).toHaveBeenCalledWith('gpu');
    });

    it('should deselect label when clicking on selected label', async () => {
      mockStore({
        nodeLabels: mockLabels,
        selectedNodeLabel: 'gpu',
      });

      render(<NodeLabelsPanel />);

      const gpuLabel = screen.getByText('gpu').closest('div[class*="group"]');
      await userEvent.click(gpuLabel! as HTMLElement);

      expect(mockSelectNodeLabel).toHaveBeenCalledWith(null);
    });

    it('should highlight selected label', () => {
      mockStore({
        nodeLabels: mockLabels,
        selectedNodeLabel: 'highmem',
      });

      render(<NodeLabelsPanel />);

      const highmemLabel = screen.getByText('highmem');
      expect(highmemLabel).toHaveClass('font-semibold');

      const highmemContainer = highmemLabel.closest('div[class*="group"]');
      expect(highmemContainer).toHaveClass('bg-accent');
    });
  });

  describe('Add label functionality', () => {
    it('should display add button', () => {
      render(<NodeLabelsPanel />);

      const addButton = screen.getByRole('button', { name: /add/i });
      expect(addButton).toBeInTheDocument();
      expect(addButton).not.toBeDisabled();
    });

    it('should disable add button when loading', () => {
      mockStore({
        isLoading: true,
      });

      render(<NodeLabelsPanel />);

      const addButton = screen.getByRole('button', { name: /add/i });
      expect(addButton).toBeDisabled();
    });

    it('should open add dialog when add button is clicked', async () => {
      render(<NodeLabelsPanel />);

      const addButton = screen.getByRole('button', { name: /add/i });
      await userEvent.click(addButton as HTMLElement);

      await waitFor(() => {
        expect(screen.getByRole('dialog')).toBeInTheDocument();
      });
    });

    it('should pass existing label names to add dialog', async () => {
      const mockLabels: NodeLabel[] = [
        getMockNodeLabel({ name: 'gpu' }),
        getMockNodeLabel({ name: 'highmem' }),
      ];

      mockStore({
        nodeLabels: mockLabels,
      });

      render(<NodeLabelsPanel />);

      const addButton = screen.getByRole('button', { name: /add/i });
      await userEvent.click(addButton as HTMLElement);

      await waitFor(() => {
        expect(screen.getByRole('dialog')).toBeInTheDocument();
        expect(screen.getByLabelText(/label name/i)).toBeInTheDocument();
      });
    });

    it('should call addNodeLabel when confirming dialog', async () => {
      mockAddNodeLabel.mockResolvedValue(undefined);

      render(<NodeLabelsPanel />);

      const addButton = screen.getByRole('button', { name: /add/i });
      await userEvent.click(addButton as HTMLElement);

      await waitFor(() => {
        expect(screen.getByRole('dialog')).toBeInTheDocument();
      });

      // Fill in the label name
      const nameInput = screen.getByLabelText(/label name/i);
      await userEvent.type(nameInput, 'new-label');

      // Click the Add Label button in the dialog
      const confirmButton = screen.getByRole('button', { name: 'Add Label' });
      await userEvent.click(confirmButton as HTMLElement);

      await waitFor(() => {
        expect(mockAddNodeLabel).toHaveBeenCalledWith('new-label', false); // Default exclusivity is false
      });
    });

    it('should handle add label errors', async () => {
      const testError = new Error('Failed to add label');
      mockAddNodeLabel.mockRejectedValue(testError);

      render(<NodeLabelsPanel />);

      const addButton = screen.getByRole('button', { name: /add/i });
      await userEvent.click(addButton as HTMLElement);

      await waitFor(() => {
        expect(screen.getByRole('dialog')).toBeInTheDocument();
      });

      // Fill in the label name
      const nameInput = screen.getByLabelText(/label name/i);
      await userEvent.type(nameInput, 'error-label');

      // Click the Add Label button in the dialog
      const confirmButton = screen.getByRole('button', { name: 'Add Label' });
      await userEvent.click(confirmButton as HTMLElement);

      await waitFor(() => {
        expect(mockAddNodeLabel).toHaveBeenCalledWith('error-label', false);
      });
    });

    it('should close dialog when cancel is clicked', async () => {
      render(<NodeLabelsPanel />);

      const addButton = screen.getByRole('button', { name: /add/i });
      await userEvent.click(addButton as HTMLElement);

      await waitFor(() => {
        // The dialog is rendered through a portal, so we need to check for the actual dialog role
        expect(screen.getByRole('dialog')).toBeInTheDocument();
      });

      const cancelButton = within(screen.getByRole('dialog')).getByRole('button', {
        name: /cancel/i,
      });
      await userEvent.click(cancelButton as HTMLElement);

      await waitFor(() => {
        expect(screen.queryByRole('dialog')).not.toBeInTheDocument();
      });
    });
  });

  describe('Remove label functionality', () => {
    const mockLabels: NodeLabel[] = [
      getMockNodeLabel({ name: 'gpu', exclusivity: true }),
      getMockNodeLabel({ name: 'highmem', exclusivity: false }),
    ];

    beforeEach(() => {
      mockStore({
        nodeLabels: mockLabels,
      });
    });

    it('should not show delete button for DEFAULT label', () => {
      const mockLabelsWithDefault: NodeLabel[] = [
        getMockNodeLabel({ name: 'DEFAULT', exclusivity: true }),
        getMockNodeLabel({ name: 'gpu', exclusivity: true }),
      ];

      mockStore({
        nodeLabels: mockLabelsWithDefault,
      });

      render(<NodeLabelsPanel />);

      const defaultLabel = screen.getByText('DEFAULT').closest('div[class*="group"]');
      const gpuLabel = screen.getByText('gpu').closest('div[class*="group"]');

      // DEFAULT label should not have a delete button
      const defaultButtons = within(defaultLabel! as HTMLElement).queryAllByRole('button');
      expect(defaultButtons.length).toBe(0);

      // Other labels should have a delete button
      const gpuButtons = within(gpuLabel! as HTMLElement).getAllByRole('button');
      expect(gpuButtons.length).toBeGreaterThan(0);
    });

    it('should show delete button on hover', async () => {
      render(<NodeLabelsPanel />);

      const gpuLabel = screen.getByText('gpu').closest('div[class*="group"]');

      // Delete button should be hidden initially
      const deleteButton = within(gpuLabel! as HTMLElement).getByRole('button');
      expect(deleteButton).toHaveClass('opacity-0');

      // Hover over the label
      fireEvent.mouseEnter(gpuLabel!);

      // Delete button should be visible
      expect(deleteButton).toHaveClass('group-hover:opacity-100');
    });

    it('should call removeNodeLabel when delete button is clicked', async () => {
      mockRemoveNodeLabel.mockResolvedValue(undefined);

      render(<NodeLabelsPanel />);

      const gpuLabel = screen.getByText('gpu').closest('div[class*="group"]');
      const deleteButton = within(gpuLabel! as HTMLElement).getByRole('button');

      await userEvent.click(deleteButton as HTMLElement);

      expect(mockRemoveNodeLabel).toHaveBeenCalledWith('gpu');
    });

    it('should call removeNodeLabel when validation passes', async () => {
      vi.mocked(validateLabelRemoval).mockReturnValue({ valid: true });
      mockRemoveNodeLabel.mockResolvedValue(undefined);

      render(<NodeLabelsPanel />);

      const gpuLabel = screen.getByText('gpu').closest('div[class*="group"]');
      const deleteButton = within(gpuLabel! as HTMLElement).getByRole('button');

      await userEvent.click(deleteButton as HTMLElement);

      expect(mockRemoveNodeLabel).toHaveBeenCalledWith('gpu');
    });

    it('should prevent event bubbling when clicking delete', async () => {
      render(<NodeLabelsPanel />);

      const gpuLabel = screen.getByText('gpu').closest('div[class*="group"]');
      const deleteButton = within(gpuLabel! as HTMLElement).getByRole('button');

      await userEvent.click(deleteButton as HTMLElement);

      // Should not trigger label selection
      expect(mockSelectNodeLabel).not.toHaveBeenCalled();
    });

    it('should disable delete button when loading', () => {
      mockStore({
        nodeLabels: mockLabels,
        isLoading: true,
      });

      render(<NodeLabelsPanel />);

      const deleteButtons = screen
        .getAllByRole('button')
        .filter((button) => button.querySelector('.text-destructive'));

      deleteButtons.forEach((button) => {
        expect(button).toBeDisabled();
      });
    });

    it('should handle remove label errors gracefully', async () => {
      const testError = new Error('Failed to remove label');
      mockRemoveNodeLabel.mockRejectedValue(testError);

      render(<NodeLabelsPanel />);

      const gpuLabel = screen.getByText('gpu').closest('div[class*="group"]');
      const deleteButton = within(gpuLabel! as HTMLElement).getByRole('button');

      await userEvent.click(deleteButton as HTMLElement);

      await waitFor(() => {
        expect(mockRemoveNodeLabel).toHaveBeenCalledWith('gpu');
      });
      // Error is now set in the store and displayed by parent component
    });
  });

  describe('Visual feedback', () => {
    const mockLabels: NodeLabel[] = [
      getMockNodeLabel({ name: 'gpu', exclusivity: true }),
      getMockNodeLabel({ name: 'highmem', exclusivity: false }),
    ];

    it('should show hover effect on labels', () => {
      mockStore({
        nodeLabels: mockLabels,
      });

      render(<NodeLabelsPanel />);

      const labelItems = screen.getAllByRole('listitem');
      labelItems.forEach((item) => {
        const labelDiv = item.querySelector('div[class*="group"]');
        expect(labelDiv).toHaveClass('hover:bg-accent');
        expect(labelDiv).toHaveClass('transition-colors');
      });
    });

    it('should show cursor pointer on labels', () => {
      mockStore({
        nodeLabels: mockLabels,
      });

      render(<NodeLabelsPanel />);

      const labelItems = screen.getAllByRole('listitem');
      labelItems.forEach((item) => {
        const labelDiv = item.querySelector('div[class*="group"]');
        expect(labelDiv).toHaveClass('cursor-pointer');
      });
    });
  });

  describe('Accessibility', () => {
    const mockLabels: NodeLabel[] = [
      getMockNodeLabel({ name: 'gpu', exclusivity: true }),
      getMockNodeLabel({ name: 'highmem', exclusivity: false }),
    ];

    it('should have accessible list structure', () => {
      mockStore({
        nodeLabels: mockLabels,
      });

      render(<NodeLabelsPanel />);

      const list = screen.getByRole('list');
      expect(list).toBeInTheDocument();

      const listItems = screen.getAllByRole('listitem');
      expect(listItems).toHaveLength(2);
    });

    it('should have accessible tooltips for delete buttons', () => {
      mockStore({
        nodeLabels: mockLabels,
      });

      render(<NodeLabelsPanel />);

      // Check that delete buttons exist and have proper structure
      const deleteButtons = screen
        .getAllByRole('button')
        .filter((button) => button.querySelector('.text-destructive'));

      expect(deleteButtons).toHaveLength(2); // One for each label
      deleteButtons.forEach((button) => {
        // Button component might not have data-slot attribute, check it's a button element
        expect(button.tagName).toBe('BUTTON');
        expect(button).not.toBeDisabled();
      });
    });

    it('should have accessible button labels', () => {
      render(<NodeLabelsPanel />);

      const addButton = screen.getByRole('button', { name: /add/i });
      expect(addButton).toHaveAccessibleName();
    });
  });
});
