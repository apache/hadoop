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


import { render, screen, within } from '@testing-library/react';
import { vi } from 'vitest';
import { NodeLabels } from './NodeLabels';
import { useSchedulerStore } from '~/stores/schedulerStore';
import type { NodeLabel } from '~/types/node-label';

// Test helper
const getMockNodeLabel = (overrides?: Partial<NodeLabel>): NodeLabel => {
  return {
    name: 'gpu',
    exclusivity: true,
    ...overrides,
  };
};

// Mock the store
vi.mock('~/stores/schedulerStore');

// Mock the child components to focus on NodeLabels behavior
vi.mock('./NodeLabelsPanel', () => ({
  NodeLabelsPanel: () => <div data-testid="node-labels-panel">Node Labels Panel</div>,
}));

vi.mock('./NodesPanel', () => ({
  NodesPanel: ({ selectedLabel }: { selectedLabel: string | null }) => (
    <div data-testid="nodes-panel">Nodes Panel - Selected: {selectedLabel || 'none'}</div>
  ),
}));

describe('NodeLabels', () => {
  const mockRefreshSchedulerData = vi.fn();

  const defaultStoreState = {
    isLoading: false,
    error: null,
    errorContext: null,
    applyError: null,
    nodeLabels: [],
    selectedNodeLabel: null,
    refreshSchedulerData: mockRefreshSchedulerData,
  };

  beforeEach(() => {
    vi.clearAllMocks();
    vi.mocked(useSchedulerStore).mockReturnValue(defaultStoreState);
  });

  describe('Loading states', () => {
    it('should display loading skeleton when loading with no existing node labels', () => {
      vi.mocked(useSchedulerStore).mockReturnValue({
        ...defaultStoreState,
        isLoading: true,
        nodeLabels: [],
      });

      render(<NodeLabels />);

      expect(screen.getByText('Loading node labels...')).toBeInTheDocument();
      expect(screen.queryByText('Node Labels Management')).not.toBeInTheDocument();
      expect(screen.queryByTestId('node-labels-panel')).not.toBeInTheDocument();
    });

    it('should display content when loading with existing node labels', () => {
      const existingLabels: NodeLabel[] = [
        getMockNodeLabel({ name: 'gpu', exclusivity: true }),
        getMockNodeLabel({ name: 'highmem', exclusivity: false }),
      ];

      vi.mocked(useSchedulerStore).mockReturnValue({
        ...defaultStoreState,
        isLoading: true,
        nodeLabels: existingLabels,
      });

      render(<NodeLabels />);

      expect(screen.queryByText('Loading node labels...')).not.toBeInTheDocument();
      expect(screen.getByText('Available Labels')).toBeInTheDocument();
      expect(screen.getByTestId('node-labels-panel')).toBeInTheDocument();
    });
  });

  describe('Header', () => {
    it('should not render the duplicated page header', () => {
      render(<NodeLabels />);

      expect(screen.queryByText('Node Labels Management')).not.toBeInTheDocument();
      expect(screen.queryByText(/Manage node labels for the YARN cluster/)).not.toBeInTheDocument();
      expect(
        screen.queryByText(/Each node can be assigned to node labels/),
      ).not.toBeInTheDocument();
    });

    it('should not render a refresh button', () => {
      render(<NodeLabels />);

      expect(screen.queryByRole('button', { name: /refresh/i })).not.toBeInTheDocument();
    });
  });

  describe('Error handling', () => {
    it('should display error alert when error exists', () => {
      const errorMessage = 'Failed to load node labels';
      vi.mocked(useSchedulerStore).mockReturnValue({
        ...defaultStoreState,
        error: errorMessage,
        errorContext: 'nodeLabels',
      });

      render(<NodeLabels />);

      expect(screen.getByText(errorMessage)).toBeInTheDocument();
      expect(screen.getByText('Node Label Operation Failed')).toBeInTheDocument();
      const alerts = screen.getAllByRole('alert');
      expect(alerts[0]).toHaveClass('mb-4');
    });

    it('should not display error alert when no error', () => {
      render(<NodeLabels />);

      expect(screen.queryByRole('alert')).not.toBeInTheDocument();
    });

    it('should display apply error alert when applyError exists', () => {
      const applyErrorMessage = 'HTTP 400: Invalid configuration';
      vi.mocked(useSchedulerStore).mockReturnValue({
        ...defaultStoreState,
        applyError: applyErrorMessage,
      });

      render(<NodeLabels />);

      expect(screen.getByText('Failed to Apply Changes')).toBeInTheDocument();
      expect(screen.getByText(applyErrorMessage)).toBeInTheDocument();
    });
  });

  describe('Panel layout', () => {
    it('should render both NodeLabelsPanel and NodesPanel', () => {
      render(<NodeLabels />);

      expect(screen.getByTestId('node-labels-panel')).toBeInTheDocument();
      expect(screen.getByTestId('nodes-panel')).toBeInTheDocument();
    });

    it('should display correct card titles', () => {
      render(<NodeLabels />);

      expect(screen.getByText('Available Labels')).toBeInTheDocument();
      expect(screen.getByText('Node Label Configuration')).toBeInTheDocument();
    });

    it('should display card descriptions', () => {
      render(<NodeLabels />);

      expect(
        screen.getByText('Select labels to configure queue capacity for each label'),
      ).toBeInTheDocument();
      expect(
        screen.getByText('Assign nodes to labels for resource allocation'),
      ).toBeInTheDocument();
    });
  });

  describe('Selected label display', () => {
    it('should not show selected label badge when no label is selected', () => {
      render(<NodeLabels />);

      const configCard = screen.getByText('Node Label Configuration').closest('[data-slot="card"]');
      const badge = configCard?.querySelector('.bg-primary\\/10');
      expect(badge).not.toBeInTheDocument();
    });

    it('should show selected label badge when a label is selected', () => {
      const selectedLabel = 'gpu';
      vi.mocked(useSchedulerStore).mockReturnValue({
        ...defaultStoreState,
        selectedNodeLabel: selectedLabel,
      });

      render(<NodeLabels />);

      const badge = screen.getByText(selectedLabel);
      expect(badge).toHaveClass('bg-primary/10');
      expect(badge).toHaveClass('text-primary');
    });

    it('should pass selected label to NodesPanel', () => {
      const selectedLabel = 'highmem';
      vi.mocked(useSchedulerStore).mockReturnValue({
        ...defaultStoreState,
        selectedNodeLabel: selectedLabel,
      });

      render(<NodeLabels />);

      const nodesPanel = screen.getByTestId('nodes-panel');
      expect(nodesPanel).toHaveTextContent(`Selected: ${selectedLabel}`);
    });
  });

  describe('Responsive layout', () => {
    it('should apply responsive grid layout', () => {
      render(<NodeLabels />);

      // Find the grid container that contains both cards
      const labelsCard = screen.getByText('Available Labels').closest('[data-slot="card"]');
      const gridContainer = labelsCard?.parentElement;
      expect(gridContainer).toHaveClass('md:grid-cols-[400px_1fr]');
      expect(gridContainer).toHaveClass('gap-6');
    });

    it('should have overflow handling for content areas', () => {
      render(<NodeLabels />);

      const cards = document.querySelectorAll('[data-slot="card"]');
      expect(cards).toHaveLength(2);
      cards.forEach((card) => {
        expect(card).toHaveClass('overflow-hidden');
      });
    });
  });

  describe('Integration with child components', () => {
    it('should render NodeLabelsPanel inside the labels card', () => {
      render(<NodeLabels />);

      const labelsCard = screen.getByText('Available Labels').closest('[data-slot="card"]');
      const labelsPanel = within(labelsCard! as HTMLElement).getByTestId('node-labels-panel');
      expect(labelsPanel).toBeInTheDocument();
    });

    it('should render NodesPanel inside the configuration card', () => {
      render(<NodeLabels />);

      const configCard = screen.getByText('Node Label Configuration').closest('[data-slot="card"]');
      const nodesPanel = within(configCard! as HTMLElement).getByTestId('nodes-panel');
      expect(nodesPanel).toBeInTheDocument();
    });
  });

  describe('Component lifecycle', () => {
    it('should not call refresh on mount', () => {
      render(<NodeLabels />);

      expect(mockRefreshSchedulerData).not.toHaveBeenCalled();
    });

    it('should handle component unmount gracefully', () => {
      const { unmount } = render(<NodeLabels />);

      expect(() => unmount()).not.toThrow();
    });

    it('should update when store state changes', () => {
      const { rerender } = render(<NodeLabels />);

      // Update store to show error
      vi.mocked(useSchedulerStore).mockReturnValue({
        ...defaultStoreState,
        error: 'New error occurred',
        errorContext: 'nodeLabels',
      });

      rerender(<NodeLabels />);

      expect(screen.getByText('New error occurred')).toBeInTheDocument();
    });
  });

  describe('Accessibility', () => {
    it('should expose clear titles for the main sections', () => {
      render(<NodeLabels />);

      const titles = document.querySelectorAll('[data-slot="card-title"]');
      expect(titles).toHaveLength(2);
    });

    it('should have proper card structure', () => {
      render(<NodeLabels />);

      const cards = document.querySelectorAll('[data-slot="card"]');
      expect(cards).toHaveLength(2);
    });
  });
});
