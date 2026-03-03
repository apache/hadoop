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
import { QueueChangeGroup } from '~/features/staged-changes/components/QueueChangeGroup';
import type { StagedChange } from '~/types';
import { SPECIAL_VALUES } from '~/types';

// Mock the DiffView component
vi.mock('~/features/staged-changes/components/DiffView', () => ({
  DiffView: ({ change, onRevert }: { change: StagedChange; onRevert: () => void }) => (
    <div data-testid={`diff-view-${change.id}`}>
      <span>DiffView: {change.property}</span>
      <button onClick={onRevert}>Revert</button>
    </div>
  ),
}));

describe('QueueChangeGroup', () => {
  const mockOnRevert = vi.fn();

  beforeEach(() => {
    vi.clearAllMocks();
  });

  const createMockChange = (overrides: Partial<StagedChange> = {}): StagedChange => ({
    id: '1',
    type: 'update',
    queuePath: 'root.default',
    property: 'capacity',
    oldValue: '50',
    newValue: '60',
    timestamp: Date.now(),
    ...overrides,
  });

  describe('rendering', () => {
    it('should render queue path as header', () => {
      const change = createMockChange();

      render(
        <QueueChangeGroup queuePath="root.default" changes={[change]} onRevert={mockOnRevert} />,
      );

      expect(screen.getByText('root.default')).toBeInTheDocument();
    });

    it('should render "Global Settings" for global queue path', () => {
      const change = createMockChange({ queuePath: SPECIAL_VALUES.GLOBAL_QUEUE_PATH });

      render(
        <QueueChangeGroup
          queuePath={SPECIAL_VALUES.GLOBAL_QUEUE_PATH}
          changes={[change]}
          onRevert={mockOnRevert}
        />,
      );

      expect(screen.getByText('Global Settings')).toBeInTheDocument();
    });

    it('should display summary badges for all change types', () => {
      const changes = [
        createMockChange({ id: '1', type: 'add' }),
        createMockChange({ id: '2', type: 'update' }),
        createMockChange({ id: '3', type: 'remove' }),
      ];

      render(
        <QueueChangeGroup queuePath="root.default" changes={changes} onRevert={mockOnRevert} />,
      );

      // Should show badges for each change type
      expect(screen.getByText('+1')).toBeInTheDocument();
      expect(screen.getByText('~1')).toBeInTheDocument();
      expect(screen.getByText('-1')).toBeInTheDocument();
    });

    it('should display summary badge for single change', () => {
      const change = createMockChange({ type: 'update' });

      render(
        <QueueChangeGroup queuePath="root.default" changes={[change]} onRevert={mockOnRevert} />,
      );

      // Should show badge for the change type
      expect(screen.getByText('~1')).toBeInTheDocument();
    });
  });

  describe('summary badges', () => {
    it('should display add badge with count', () => {
      const changes = [
        createMockChange({ id: '1', type: 'add' }),
        createMockChange({ id: '2', type: 'add' }),
      ];

      render(
        <QueueChangeGroup queuePath="root.default" changes={changes} onRevert={mockOnRevert} />,
      );

      expect(screen.getByText('+2')).toBeInTheDocument();
    });

    it('should display update badge with count', () => {
      const changes = [
        createMockChange({ id: '1', type: 'update' }),
        createMockChange({ id: '2', type: 'update' }),
        createMockChange({ id: '3', type: 'update' }),
      ];

      render(
        <QueueChangeGroup queuePath="root.default" changes={changes} onRevert={mockOnRevert} />,
      );

      expect(screen.getByText('~3')).toBeInTheDocument();
    });

    it('should display remove badge with count', () => {
      const changes = [createMockChange({ id: '1', type: 'remove' })];

      render(
        <QueueChangeGroup queuePath="root.default" changes={changes} onRevert={mockOnRevert} />,
      );

      expect(screen.getByText('-1')).toBeInTheDocument();
    });

    it('should display all badge types when present', () => {
      const changes = [
        createMockChange({ id: '1', type: 'add' }),
        createMockChange({ id: '2', type: 'update' }),
        createMockChange({ id: '3', type: 'remove' }),
      ];

      render(
        <QueueChangeGroup queuePath="root.default" changes={changes} onRevert={mockOnRevert} />,
      );

      expect(screen.getByText('+1')).toBeInTheDocument();
      expect(screen.getByText('~1')).toBeInTheDocument();
      expect(screen.getByText('-1')).toBeInTheDocument();
    });

    it('should not display badge for change type with zero count', () => {
      const changes = [createMockChange({ id: '1', type: 'update' })];

      render(
        <QueueChangeGroup queuePath="root.default" changes={changes} onRevert={mockOnRevert} />,
      );

      expect(screen.queryByText(/^\+/)).not.toBeInTheDocument();
      expect(screen.queryByText(/^-/)).not.toBeInTheDocument();
    });
  });

  describe('collapsible behavior', () => {
    it('should render with queue group expanded by default', () => {
      const change = createMockChange();

      render(
        <QueueChangeGroup queuePath="root.default" changes={[change]} onRevert={mockOnRevert} />,
      );

      // Check if the change type section is visible
      expect(screen.getByText(/Modified \(1\)/)).toBeInTheDocument();
    });

    it('should collapse/expand queue group when header is clicked', async () => {
      const user = userEvent.setup();
      const change = createMockChange();

      render(
        <QueueChangeGroup queuePath="root.default" changes={[change]} onRevert={mockOnRevert} />,
      );

      const header = screen.getByText('root.default').closest('button');
      expect(header).toBeInTheDocument();

      // Initially expanded
      expect(screen.getByText(/Modified \(1\)/)).toBeInTheDocument();

      // Click to collapse
      await user.click(header!);

      // Should be collapsed now (content hidden)
      expect(screen.queryByText(/Modified \(1\)/)).not.toBeInTheDocument();

      // Click to expand again
      await user.click(header!);

      // Should be expanded again
      expect(screen.getByText(/Modified \(1\)/)).toBeInTheDocument();
    });
  });

  describe('change type sections', () => {
    it('should render "New" section for add changes', () => {
      const changes = [createMockChange({ id: '1', type: 'add' })];

      render(
        <QueueChangeGroup queuePath="root.default" changes={changes} onRevert={mockOnRevert} />,
      );

      expect(screen.getByText('New (1)')).toBeInTheDocument();
    });

    it('should render "Modified" section for update changes', () => {
      const changes = [createMockChange({ id: '1', type: 'update' })];

      render(
        <QueueChangeGroup queuePath="root.default" changes={changes} onRevert={mockOnRevert} />,
      );

      expect(screen.getByText('Modified (1)')).toBeInTheDocument();
    });

    it('should render "Removed" section for remove changes', () => {
      const changes = [createMockChange({ id: '1', type: 'remove' })];

      render(
        <QueueChangeGroup queuePath="root.default" changes={changes} onRevert={mockOnRevert} />,
      );

      expect(screen.getByText('Removed (1)')).toBeInTheDocument();
    });

    it('should render all change type sections', () => {
      const changes = [
        createMockChange({ id: '1', type: 'add' }),
        createMockChange({ id: '2', type: 'update' }),
        createMockChange({ id: '3', type: 'remove' }),
      ];

      render(
        <QueueChangeGroup queuePath="root.default" changes={changes} onRevert={mockOnRevert} />,
      );

      expect(screen.getByText('New (1)')).toBeInTheDocument();
      expect(screen.getByText('Modified (1)')).toBeInTheDocument();
      expect(screen.getByText('Removed (1)')).toBeInTheDocument();
    });

    it('should render all changes in each section expanded by default', () => {
      const changes = [
        createMockChange({ id: '1', type: 'add', property: 'capacity' }),
        createMockChange({ id: '2', type: 'update', property: 'maximum-capacity' }),
      ];

      render(
        <QueueChangeGroup queuePath="root.default" changes={changes} onRevert={mockOnRevert} />,
      );

      expect(screen.getByText('DiffView: capacity')).toBeInTheDocument();
      expect(screen.getByText('DiffView: maximum-capacity')).toBeInTheDocument();
    });

    it('should collapse/expand add section when clicked', async () => {
      const user = userEvent.setup();
      const changes = [createMockChange({ id: '1', type: 'add', property: 'capacity' })];

      render(
        <QueueChangeGroup queuePath="root.default" changes={changes} onRevert={mockOnRevert} />,
      );

      // Initially expanded
      expect(screen.getByText('DiffView: capacity')).toBeInTheDocument();

      // Click to collapse
      const addSection = screen.getByText('New (1)').closest('button');
      await user.click(addSection!);

      // Should be collapsed
      expect(screen.queryByText('DiffView: capacity')).not.toBeInTheDocument();

      // Click to expand
      await user.click(addSection!);

      // Should be expanded again
      expect(screen.getByText('DiffView: capacity')).toBeInTheDocument();
    });

    it('should collapse/expand update section when clicked', async () => {
      const user = userEvent.setup();
      const changes = [createMockChange({ id: '1', type: 'update', property: 'capacity' })];

      render(
        <QueueChangeGroup queuePath="root.default" changes={changes} onRevert={mockOnRevert} />,
      );

      const modifySection = screen.getByText('Modified (1)').closest('button');
      await user.click(modifySection!);

      expect(screen.queryByText('DiffView: capacity')).not.toBeInTheDocument();
    });

    it('should collapse/expand remove section when clicked', async () => {
      const user = userEvent.setup();
      const changes = [createMockChange({ id: '1', type: 'remove', property: 'capacity' })];

      render(
        <QueueChangeGroup queuePath="root.default" changes={changes} onRevert={mockOnRevert} />,
      );

      const removeSection = screen.getByText('Removed (1)').closest('button');
      await user.click(removeSection!);

      expect(screen.queryByText('DiffView: capacity')).not.toBeInTheDocument();
    });
  });

  describe('DiffView integration', () => {
    it('should render DiffView for each change', () => {
      const changes = [
        createMockChange({ id: '1', property: 'capacity' }),
        createMockChange({ id: '2', property: 'maximum-capacity' }),
      ];

      render(
        <QueueChangeGroup queuePath="root.default" changes={changes} onRevert={mockOnRevert} />,
      );

      expect(screen.getByText('DiffView: capacity')).toBeInTheDocument();
      expect(screen.getByText('DiffView: maximum-capacity')).toBeInTheDocument();
    });

    it('should call onRevert when DiffView revert is clicked', async () => {
      const user = userEvent.setup();
      const change = createMockChange({ id: '1' });

      render(
        <QueueChangeGroup queuePath="root.default" changes={[change]} onRevert={mockOnRevert} />,
      );

      const revertButton = screen.getByText('Revert');
      await user.click(revertButton);

      expect(mockOnRevert).toHaveBeenCalledWith(change);
    });

    it('should render multiple changes of same type', () => {
      const changes = [
        createMockChange({ id: '1', type: 'update', property: 'capacity' }),
        createMockChange({ id: '2', type: 'update', property: 'maximum-capacity' }),
        createMockChange({ id: '3', type: 'update', property: 'state' }),
      ];

      render(
        <QueueChangeGroup queuePath="root.default" changes={changes} onRevert={mockOnRevert} />,
      );

      expect(screen.getByText('Modified (3)')).toBeInTheDocument();
      expect(screen.getByText('DiffView: capacity')).toBeInTheDocument();
      expect(screen.getByText('DiffView: maximum-capacity')).toBeInTheDocument();
      expect(screen.getByText('DiffView: state')).toBeInTheDocument();
    });
  });

  describe('complex scenarios', () => {
    it('should handle many changes grouped correctly', () => {
      const changes = [
        createMockChange({ id: '1', type: 'add', property: 'capacity' }),
        createMockChange({ id: '2', type: 'add', property: 'maximum-capacity' }),
        createMockChange({ id: '3', type: 'update', property: 'state' }),
        createMockChange({ id: '4', type: 'update', property: 'user-limit-factor' }),
        createMockChange({ id: '5', type: 'update', property: 'minimum-user-limit-percent' }),
        createMockChange({ id: '6', type: 'remove', property: 'ordering-policy' }),
      ];

      render(
        <QueueChangeGroup queuePath="root.default" changes={changes} onRevert={mockOnRevert} />,
      );

      // Should show badges with counts (but not "6 changes" text - that was removed in redesign)
      expect(screen.getByText('+2')).toBeInTheDocument();
      expect(screen.getByText('~3')).toBeInTheDocument();
      expect(screen.getByText('-1')).toBeInTheDocument();
      expect(screen.getByText('New (2)')).toBeInTheDocument();
      expect(screen.getByText('Modified (3)')).toBeInTheDocument();
      expect(screen.getByText('Removed (1)')).toBeInTheDocument();
    });

    it('should handle empty changes array', () => {
      const { container } = render(
        <QueueChangeGroup queuePath="root.default" changes={[]} onRevert={mockOnRevert} />,
      );

      expect(container.querySelector('.border')).toBeInTheDocument();
      // With no changes, there should be no type sections
      expect(screen.queryByText(/Add \(/)).not.toBeInTheDocument();
      expect(screen.queryByText(/Modify \(/)).not.toBeInTheDocument();
      expect(screen.queryByText(/Remove \(/)).not.toBeInTheDocument();
    });

    it('should handle long queue paths', () => {
      const longPath = 'root.production.critical.highpriority.specialqueue';
      const change = createMockChange({ queuePath: longPath });

      render(<QueueChangeGroup queuePath={longPath} changes={[change]} onRevert={mockOnRevert} />);

      expect(screen.getByText(longPath)).toBeInTheDocument();
    });
  });
});
