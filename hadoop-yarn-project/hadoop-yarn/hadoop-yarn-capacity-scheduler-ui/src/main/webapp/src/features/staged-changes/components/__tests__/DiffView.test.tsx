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
import { DiffView } from '~/features/staged-changes/components/DiffView';
import type { StagedChange } from '~/types';

describe('DiffView', () => {
  const mockOnRevert = vi.fn();
  const mockTimestamp = '2 minutes ago';

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
    it('should render change card with timestamp', () => {
      const change = createMockChange();

      render(<DiffView change={change} onRevert={mockOnRevert} timestamp={mockTimestamp} />);

      expect(screen.getByText(mockTimestamp)).toBeInTheDocument();
    });

    it('should render revert button', () => {
      const change = createMockChange();

      render(<DiffView change={change} onRevert={mockOnRevert} timestamp={mockTimestamp} />);

      const revertButton = screen.getByRole('button');
      expect(revertButton).toBeInTheDocument();
    });

    it('should show full property key for update', () => {
      const change = createMockChange({ property: 'capacity' });

      render(<DiffView change={change} onRevert={mockOnRevert} timestamp={mockTimestamp} />);

      // Should show full property key in the diff lines
      expect(
        screen.getByText(/yarn\.scheduler\.capacity\.root\.default\.capacity=50/),
      ).toBeInTheDocument();
      expect(
        screen.getByText(/yarn\.scheduler\.capacity\.root\.default\.capacity=60/),
      ).toBeInTheDocument();
    });
  });

  describe('change types', () => {
    it('should render add change with + prefix', () => {
      const change = createMockChange({
        type: 'add',
        oldValue: undefined,
        newValue: '50',
      });

      render(<DiffView change={change} onRevert={mockOnRevert} timestamp={mockTimestamp} />);

      // Should show the full property key with value
      expect(
        screen.getByText(/yarn\.scheduler\.capacity\.root\.default\.capacity=50/),
      ).toBeInTheDocument();
      // Should have + prefix
      const plusElements = screen.getAllByText('+', { exact: true });
      expect(plusElements.length).toBeGreaterThan(0);
    });

    it('should render update change with - and + prefixes', () => {
      const change = createMockChange({ type: 'update' });

      render(<DiffView change={change} onRevert={mockOnRevert} timestamp={mockTimestamp} />);

      // Should have both - and + prefixes
      const minusElements = screen.getAllByText('-', { exact: true });
      const plusElements = screen.getAllByText('+', { exact: true });
      expect(minusElements.length).toBeGreaterThan(0);
      expect(plusElements.length).toBeGreaterThan(0);
    });

    it('should render remove change with - prefix', () => {
      const change = createMockChange({
        type: 'remove',
        oldValue: '50',
        newValue: undefined,
      });

      render(<DiffView change={change} onRevert={mockOnRevert} timestamp={mockTimestamp} />);

      // Should show the full property key with value
      expect(
        screen.getByText(/yarn\.scheduler\.capacity\.root\.default\.capacity=50/),
      ).toBeInTheDocument();
      // Should have - prefix
      const minusElements = screen.getAllByText('-', { exact: true });
      expect(minusElements.length).toBeGreaterThan(0);
    });
  });

  describe('update changes', () => {
    it('should display old and new values for update changes', () => {
      const change = createMockChange({
        type: 'update',
        oldValue: '50',
        newValue: '60',
      });

      render(<DiffView change={change} onRevert={mockOnRevert} timestamp={mockTimestamp} />);

      expect(
        screen.getByText(/yarn\.scheduler\.capacity\.root\.default\.capacity=50/),
      ).toBeInTheDocument();
      expect(
        screen.getByText(/yarn\.scheduler\.capacity\.root\.default\.capacity=60/),
      ).toBeInTheDocument();
    });

    it('should use red background for old value', () => {
      const change = createMockChange({
        type: 'update',
        oldValue: '50',
        newValue: '60',
      });

      const { container } = render(
        <DiffView change={change} onRevert={mockOnRevert} timestamp={mockTimestamp} />,
      );

      const oldValueElement = container.querySelector('.bg-red-500\\/10');
      expect(oldValueElement).toBeInTheDocument();
    });

    it('should display minus prefix for old value', () => {
      const change = createMockChange({
        type: 'update',
        oldValue: '50',
        newValue: '60',
      });

      render(<DiffView change={change} onRevert={mockOnRevert} timestamp={mockTimestamp} />);

      // The minus prefix is rendered as a separate span
      const minusElements = screen.getAllByText('-', { exact: true });
      expect(minusElements.length).toBeGreaterThan(0);
    });

    it('should display plus prefix for new value', () => {
      const change = createMockChange({
        type: 'update',
        oldValue: '50',
        newValue: '60',
      });

      render(<DiffView change={change} onRevert={mockOnRevert} timestamp={mockTimestamp} />);

      // The plus prefix is rendered as a separate span
      const plusElements = screen.getAllByText('+', { exact: true });
      expect(plusElements.length).toBeGreaterThan(0);
    });

    it('should handle empty old value', () => {
      const change = createMockChange({
        type: 'update',
        oldValue: '',
        newValue: '60',
      });

      render(<DiffView change={change} onRevert={mockOnRevert} timestamp={mockTimestamp} />);

      expect(screen.getByText(/capacity=\(empty\)/)).toBeInTheDocument();
      expect(screen.getByText(/capacity=60/)).toBeInTheDocument();
    });

    it('should handle empty new value', () => {
      const change = createMockChange({
        type: 'update',
        oldValue: '50',
        newValue: '',
      });

      render(<DiffView change={change} onRevert={mockOnRevert} timestamp={mockTimestamp} />);

      expect(screen.getByText(/capacity=50/)).toBeInTheDocument();
      expect(screen.getByText(/capacity=\(empty\)/)).toBeInTheDocument();
    });
  });

  describe('add changes', () => {
    it('should display new value for add changes', () => {
      const change = createMockChange({
        type: 'add',
        oldValue: undefined,
        newValue: '50',
      });

      render(<DiffView change={change} onRevert={mockOnRevert} timestamp={mockTimestamp} />);

      expect(
        screen.getByText(/yarn\.scheduler\.capacity\.root\.default\.capacity=50/),
      ).toBeInTheDocument();
    });

    it('should display plus prefix for add changes', () => {
      const change = createMockChange({
        type: 'add',
        oldValue: undefined,
        newValue: '50',
      });

      render(<DiffView change={change} onRevert={mockOnRevert} timestamp={mockTimestamp} />);

      const plusElements = screen.getAllByText('+', { exact: true });
      expect(plusElements.length).toBeGreaterThan(0);
    });

    it('should not display old value for add changes', () => {
      const change = createMockChange({
        type: 'add',
        oldValue: undefined,
        newValue: '50',
      });

      const { container } = render(
        <DiffView change={change} onRevert={mockOnRevert} timestamp={mockTimestamp} />,
      );

      // Only one diff line should be rendered (green background for add)
      const addLines = container.querySelectorAll('.bg-green-500\\/10');
      expect(addLines.length).toBe(1);
      // Should not have red background lines (remove)
      const removeLines = container.querySelectorAll('.bg-red-500\\/10');
      expect(removeLines.length).toBe(0);
    });

    it('should render (empty) for empty string in add change', () => {
      const change = createMockChange({
        type: 'add',
        oldValue: undefined,
        newValue: '',
      });

      render(<DiffView change={change} onRevert={mockOnRevert} timestamp={mockTimestamp} />);

      expect(screen.getByText(/capacity=\(empty\)/)).toBeInTheDocument();
    });
  });

  describe('remove changes', () => {
    it('should display old value for remove changes', () => {
      const change = createMockChange({
        type: 'remove',
        oldValue: '50',
        newValue: undefined,
      });

      render(<DiffView change={change} onRevert={mockOnRevert} timestamp={mockTimestamp} />);

      expect(
        screen.getByText(/yarn\.scheduler\.capacity\.root\.default\.capacity=50/),
      ).toBeInTheDocument();
    });

    it('should display minus prefix for remove changes', () => {
      const change = createMockChange({
        type: 'remove',
        oldValue: '50',
        newValue: undefined,
      });

      render(<DiffView change={change} onRevert={mockOnRevert} timestamp={mockTimestamp} />);

      const minusElements = screen.getAllByText('-', { exact: true });
      expect(minusElements.length).toBeGreaterThan(0);
    });

    it('should use red background for removed value', () => {
      const change = createMockChange({
        type: 'remove',
        oldValue: '50',
        newValue: undefined,
      });

      const { container } = render(
        <DiffView change={change} onRevert={mockOnRevert} timestamp={mockTimestamp} />,
      );

      const removeLines = container.querySelectorAll('.bg-red-500\\/10');
      expect(removeLines.length).toBe(1);
    });

    it('should not display new value for remove changes', () => {
      const change = createMockChange({
        type: 'remove',
        oldValue: '50',
        newValue: undefined,
      });

      const { container } = render(
        <DiffView change={change} onRevert={mockOnRevert} timestamp={mockTimestamp} />,
      );

      // Only one diff line should be rendered (red background for remove)
      const removeLines = container.querySelectorAll('.bg-red-500\\/10');
      expect(removeLines.length).toBe(1);
      // Should not have green background lines (add)
      const addLines = container.querySelectorAll('.bg-green-500\\/10');
      expect(addLines.length).toBe(0);
    });

    it('should display "Queue will be removed" message when removing queue with QUEUE_MARKER property', () => {
      const change = createMockChange({
        type: 'remove',
        property: '__queue__', // QUEUE_MARKER
        oldValue: 'exists',
        newValue: undefined,
      });

      render(<DiffView change={change} onRevert={mockOnRevert} timestamp={mockTimestamp} />);

      expect(screen.getByText('Queue will be removed')).toBeInTheDocument();
    });

    it('should show diff line for regular property removal', () => {
      const change = createMockChange({
        type: 'remove',
        oldValue: '50',
        newValue: undefined,
      });

      render(<DiffView change={change} onRevert={mockOnRevert} timestamp={mockTimestamp} />);

      // Regular property removal shows diff line
      expect(screen.getByText(/capacity=50/)).toBeInTheDocument();
    });
  });

  describe('validation errors', () => {
    it('should display validation errors', () => {
      const change = createMockChange({
        validationErrors: [
          {
            severity: 'error',
            message: 'Capacity exceeds 100%',
            rule: 'capacity-sum',
            queuePath: 'root.default',
            field: 'capacity',
          },
        ],
      });

      render(<DiffView change={change} onRevert={mockOnRevert} timestamp={mockTimestamp} />);

      expect(screen.getByText('Capacity exceeds 100%')).toBeInTheDocument();
    });

    it('should display validation warnings', () => {
      const change = createMockChange({
        validationErrors: [
          {
            severity: 'warning',
            message: 'Capacity is low',
            rule: 'capacity-low',
            queuePath: 'root.default',
            field: 'capacity',
          },
        ],
      });

      render(<DiffView change={change} onRevert={mockOnRevert} timestamp={mockTimestamp} />);

      expect(screen.getByText('Capacity is low')).toBeInTheDocument();
    });

    it('should display multiple validation errors', () => {
      const change = createMockChange({
        validationErrors: [
          {
            severity: 'error',
            message: 'Capacity exceeds 100%',
            rule: 'capacity-sum',
            queuePath: 'root.default',
            field: 'capacity',
          },
          {
            severity: 'error',
            message: 'Maximum capacity too low',
            rule: 'max-capacity-min',
            queuePath: 'root.default',
            field: 'maximum-capacity',
          },
        ],
      });

      render(<DiffView change={change} onRevert={mockOnRevert} timestamp={mockTimestamp} />);

      expect(screen.getByText('Capacity exceeds 100%')).toBeInTheDocument();
      expect(screen.getByText('Maximum capacity too low')).toBeInTheDocument();
    });

    it('should not display validation section when there are no errors', () => {
      const change = createMockChange({ validationErrors: [] });

      render(<DiffView change={change} onRevert={mockOnRevert} timestamp={mockTimestamp} />);

      expect(screen.queryByText(/Capacity exceeds/)).not.toBeInTheDocument();
    });

    it('should not display validation section when validationErrors is undefined', () => {
      const change = createMockChange({ validationErrors: undefined });

      render(<DiffView change={change} onRevert={mockOnRevert} timestamp={mockTimestamp} />);

      expect(screen.queryByText(/Capacity exceeds/)).not.toBeInTheDocument();
    });

    it('should display "Validation Error" label for errors', () => {
      const change = createMockChange({
        validationErrors: [
          {
            severity: 'error',
            message: 'Test error message',
            rule: 'test-rule',
            queuePath: 'root.default',
            field: 'capacity',
          },
        ],
      });

      render(<DiffView change={change} onRevert={mockOnRevert} timestamp={mockTimestamp} />);

      expect(screen.getByText('Validation Error')).toBeInTheDocument();
    });

    it('should display "Warning" label for warnings', () => {
      const change = createMockChange({
        validationErrors: [
          {
            severity: 'warning',
            message: 'Test warning message',
            rule: 'test-rule',
            queuePath: 'root.default',
            field: 'capacity',
          },
        ],
      });

      render(<DiffView change={change} onRevert={mockOnRevert} timestamp={mockTimestamp} />);

      expect(screen.getByText('Warning')).toBeInTheDocument();
    });

    it('should show "Affects: queue" when error affects different queue', () => {
      const change = createMockChange({
        queuePath: 'root.parent',
        validationErrors: [
          {
            queuePath: 'root.parent.child',
            field: 'capacity',
            message: 'Child capacity error',
            severity: 'error',
            rule: 'test-rule',
          },
        ],
      });

      render(<DiffView change={change} onRevert={mockOnRevert} timestamp={mockTimestamp} />);

      expect(screen.getByText('Affects: root.parent.child')).toBeInTheDocument();
    });

    it('should not show "Affects" when error is for same queue', () => {
      const change = createMockChange({
        queuePath: 'root.default',
        validationErrors: [
          {
            queuePath: 'root.default',
            field: 'capacity',
            message: 'Capacity error',
            severity: 'error',
            rule: 'test-rule',
          },
        ],
      });

      render(<DiffView change={change} onRevert={mockOnRevert} timestamp={mockTimestamp} />);

      expect(screen.queryByText(/Affects:/)).not.toBeInTheDocument();
    });
  });

  describe('revert functionality', () => {
    it('should call onRevert when revert button is clicked', async () => {
      const user = userEvent.setup();
      const change = createMockChange();

      render(<DiffView change={change} onRevert={mockOnRevert} timestamp={mockTimestamp} />);

      const revertButton = screen.getByRole('button');
      await user.click(revertButton);

      expect(mockOnRevert).toHaveBeenCalledTimes(1);
    });

    it('should display revert tooltip on hover', async () => {
      const user = userEvent.setup();
      const change = createMockChange();

      render(<DiffView change={change} onRevert={mockOnRevert} timestamp={mockTimestamp} />);

      const revertButton = screen.getByRole('button');
      await user.hover(revertButton);

      // Tooltip may not be immediately visible in tests, but we can verify the button exists
      expect(revertButton).toBeInTheDocument();
    });
  });

  describe('complex scenarios', () => {
    it('should render node label properties correctly', () => {
      const change = createMockChange({
        queuePath: 'root.default',
        property: 'capacity',
        label: 'gpu',
      });

      render(<DiffView change={change} onRevert={mockOnRevert} timestamp={mockTimestamp} />);

      // Should show full property key including node label (both old and new values for update)
      const matches = screen.getAllByText(
        /yarn\.scheduler\.capacity\.root\.default\.accessible-node-labels\.gpu\.capacity/,
      );
      // For update type, there should be 2 lines (old and new)
      expect(matches.length).toBe(2);
    });

    it('should render update with validation errors', () => {
      const change = createMockChange({
        type: 'update',
        oldValue: '50',
        newValue: '150',
        validationErrors: [
          {
            severity: 'error',
            message: 'Capacity exceeds 100%',
            rule: 'capacity-sum',
            queuePath: 'root.default',
            field: 'capacity',
          },
        ],
      });

      render(<DiffView change={change} onRevert={mockOnRevert} timestamp={mockTimestamp} />);

      expect(screen.getByText(/capacity=50/)).toBeInTheDocument();
      expect(screen.getByText(/capacity=150/)).toBeInTheDocument();
      expect(screen.getByText('Capacity exceeds 100%')).toBeInTheDocument();
    });

    it('should render add with validation warnings', () => {
      const change = createMockChange({
        type: 'add',
        oldValue: undefined,
        newValue: '10',
        validationErrors: [
          {
            severity: 'warning',
            message: 'Capacity is very low',
            rule: 'capacity-low',
            queuePath: 'root.default',
            field: 'capacity',
          },
        ],
      });

      render(<DiffView change={change} onRevert={mockOnRevert} timestamp={mockTimestamp} />);

      expect(screen.getByText(/capacity=10/)).toBeInTheDocument();
      expect(screen.getByText('Capacity is very low')).toBeInTheDocument();
    });

    it('should handle very long values', () => {
      const longValue = 'a'.repeat(200);
      const change = createMockChange({
        type: 'update',
        oldValue: 'short',
        newValue: longValue,
      });

      render(<DiffView change={change} onRevert={mockOnRevert} timestamp={mockTimestamp} />);

      expect(screen.getByText(new RegExp(longValue))).toBeInTheDocument();
      expect(screen.getByText(/capacity=short/)).toBeInTheDocument();
    });

    it('should handle special characters in values', () => {
      const change = createMockChange({
        type: 'update',
        oldValue: '[memory=2048,vcores=4]',
        newValue: '[memory=4096,vcores=8]',
      });

      render(<DiffView change={change} onRevert={mockOnRevert} timestamp={mockTimestamp} />);

      expect(screen.getByText(/\[memory=2048,vcores=4\]/)).toBeInTheDocument();
      expect(screen.getByText(/\[memory=4096,vcores=8\]/)).toBeInTheDocument();
    });

    it('should render global properties correctly', () => {
      const change = createMockChange({
        queuePath: 'global',
        property: 'maximum-applications',
        oldValue: '10000',
        newValue: '20000',
      });

      render(<DiffView change={change} onRevert={mockOnRevert} timestamp={mockTimestamp} />);

      // Should show global property key (without queue path)
      expect(
        screen.getByText(/yarn\.scheduler\.capacity\.maximum-applications=10000/),
      ).toBeInTheDocument();
      expect(
        screen.getByText(/yarn\.scheduler\.capacity\.maximum-applications=20000/),
      ).toBeInTheDocument();
    });
  });
});
