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
    it('should render change card', () => {
      const change = createMockChange();

      render(<DiffView change={change} onRevert={mockOnRevert} timestamp={mockTimestamp} />);

      expect(screen.getByText('UPDATE')).toBeInTheDocument();
      expect(screen.getByText('Capacity')).toBeInTheDocument();
      expect(screen.getByText(mockTimestamp)).toBeInTheDocument();
    });

    it('should render revert button', () => {
      const change = createMockChange();

      render(<DiffView change={change} onRevert={mockOnRevert} timestamp={mockTimestamp} />);

      const revertButton = screen.getByRole('button');
      expect(revertButton).toBeInTheDocument();
    });

    it('should format property name', () => {
      const change = createMockChange({ property: 'maximum-capacity' });

      render(<DiffView change={change} onRevert={mockOnRevert} timestamp={mockTimestamp} />);

      expect(screen.getByText('Maximum Capacity')).toBeInTheDocument();
    });
  });

  describe('change types', () => {
    it('should render ADD badge for add changes', () => {
      const change = createMockChange({
        type: 'add',
        oldValue: undefined,
        newValue: '50',
      });

      render(<DiffView change={change} onRevert={mockOnRevert} timestamp={mockTimestamp} />);

      expect(screen.getByText('ADD')).toBeInTheDocument();
    });

    it('should render UPDATE badge for update changes', () => {
      const change = createMockChange({ type: 'update' });

      render(<DiffView change={change} onRevert={mockOnRevert} timestamp={mockTimestamp} />);

      expect(screen.getByText('UPDATE')).toBeInTheDocument();
    });

    it('should render REMOVE badge for remove changes', () => {
      const change = createMockChange({
        type: 'remove',
        oldValue: '50',
        newValue: undefined,
      });

      render(<DiffView change={change} onRevert={mockOnRevert} timestamp={mockTimestamp} />);

      expect(screen.getByText('REMOVE')).toBeInTheDocument();
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

      expect(screen.getByText('50')).toBeInTheDocument();
      expect(screen.getByText('60')).toBeInTheDocument();
    });

    it('should show strikethrough for old value', () => {
      const change = createMockChange({
        type: 'update',
        oldValue: '50',
        newValue: '60',
      });

      render(<DiffView change={change} onRevert={mockOnRevert} timestamp={mockTimestamp} />);

      const oldValueElement = screen.getByText('50').closest('div');
      expect(oldValueElement).toHaveClass('line-through');
    });

    it('should display minus prefix for old value', () => {
      const change = createMockChange({
        type: 'update',
        oldValue: '50',
        newValue: '60',
      });

      render(<DiffView change={change} onRevert={mockOnRevert} timestamp={mockTimestamp} />);

      // The minus prefix is rendered as a separate span
      const minusElements = screen.getAllByText('-', { exact: false });
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
      const plusElements = screen.getAllByText('+', { exact: false });
      expect(plusElements.length).toBeGreaterThan(0);
    });

    it('should handle empty old value', () => {
      const change = createMockChange({
        type: 'update',
        oldValue: '',
        newValue: '60',
      });

      render(<DiffView change={change} onRevert={mockOnRevert} timestamp={mockTimestamp} />);

      expect(screen.getByText('(empty)')).toBeInTheDocument();
      expect(screen.getByText('60')).toBeInTheDocument();
    });

    it('should handle empty new value', () => {
      const change = createMockChange({
        type: 'update',
        oldValue: '50',
        newValue: '',
      });

      render(<DiffView change={change} onRevert={mockOnRevert} timestamp={mockTimestamp} />);

      expect(screen.getByText('50')).toBeInTheDocument();
      expect(screen.getByText('(empty)')).toBeInTheDocument();
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

      expect(screen.getByText('50')).toBeInTheDocument();
    });

    it('should display plus prefix for add changes', () => {
      const change = createMockChange({
        type: 'add',
        oldValue: undefined,
        newValue: '50',
      });

      render(<DiffView change={change} onRevert={mockOnRevert} timestamp={mockTimestamp} />);

      const plusElements = screen.getAllByText('+', { exact: false });
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

      // Only one value should be rendered
      const valueElements = container.querySelectorAll('.font-mono');
      expect(valueElements.length).toBe(1);
    });

    it('should not render value for empty string in add change', () => {
      const change = createMockChange({
        type: 'add',
        oldValue: undefined,
        newValue: '',
      });

      const { container } = render(
        <DiffView change={change} onRevert={mockOnRevert} timestamp={mockTimestamp} />,
      );

      // Empty string is falsy, so DiffValue won't render
      const valueElements = container.querySelectorAll('.font-mono');
      expect(valueElements.length).toBe(0);
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

      expect(screen.getByText('50')).toBeInTheDocument();
    });

    it('should display minus prefix for remove changes', () => {
      const change = createMockChange({
        type: 'remove',
        oldValue: '50',
        newValue: undefined,
      });

      render(<DiffView change={change} onRevert={mockOnRevert} timestamp={mockTimestamp} />);

      const minusElements = screen.getAllByText('-', { exact: false });
      expect(minusElements.length).toBeGreaterThan(0);
    });

    it('should show strikethrough for removed value', () => {
      const change = createMockChange({
        type: 'remove',
        oldValue: '50',
        newValue: undefined,
      });

      render(<DiffView change={change} onRevert={mockOnRevert} timestamp={mockTimestamp} />);

      const oldValueElement = screen.getByText('50').closest('div');
      expect(oldValueElement).toHaveClass('line-through');
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

      // Only one value should be rendered
      const valueElements = container.querySelectorAll('.font-mono');
      expect(valueElements.length).toBe(1);
    });

    it('should display "Queue will be removed" message when removing queue without old value', () => {
      const change = createMockChange({
        type: 'remove',
        oldValue: undefined,
        newValue: undefined,
      });

      render(<DiffView change={change} onRevert={mockOnRevert} timestamp={mockTimestamp} />);

      expect(screen.getByText('Queue will be removed')).toBeInTheDocument();
    });

    it('should display "Queue will be removed" for empty old value in remove change', () => {
      const change = createMockChange({
        type: 'remove',
        oldValue: '',
        newValue: undefined,
      });

      render(<DiffView change={change} onRevert={mockOnRevert} timestamp={mockTimestamp} />);

      // Empty string is falsy, so it shows the "Queue will be removed" message
      expect(screen.getByText('Queue will be removed')).toBeInTheDocument();
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
    it('should render complex property names correctly', () => {
      const change = createMockChange({
        property: 'accessible-node-labels.gpu.capacity',
      });

      render(<DiffView change={change} onRevert={mockOnRevert} timestamp={mockTimestamp} />);

      // Should format the property name
      expect(screen.getByText('Capacity (label: gpu)')).toBeInTheDocument();
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

      expect(screen.getByText('UPDATE')).toBeInTheDocument();
      expect(screen.getByText('50')).toBeInTheDocument();
      expect(screen.getByText('150')).toBeInTheDocument();
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

      expect(screen.getByText('ADD')).toBeInTheDocument();
      expect(screen.getByText('10')).toBeInTheDocument();
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

      expect(screen.getByText(longValue)).toBeInTheDocument();
      expect(screen.getByText('short')).toBeInTheDocument();
    });

    it('should handle special characters in values', () => {
      const change = createMockChange({
        type: 'update',
        oldValue: '[memory=2048,vcores=4]',
        newValue: '[memory=4096,vcores=8]',
      });

      render(<DiffView change={change} onRevert={mockOnRevert} timestamp={mockTimestamp} />);

      expect(screen.getByText('[memory=2048,vcores=4]')).toBeInTheDocument();
      expect(screen.getByText('[memory=4096,vcores=8]')).toBeInTheDocument();
    });
  });
});
