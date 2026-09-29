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
import { NodeLabelSelector } from './NodeLabelSelector';
import { useSchedulerStore } from '~/stores/schedulerStore';
import type { NodeLabel } from '~/types';

// Mock the scheduler store
vi.mock('~/stores/schedulerStore');

// Mock Radix UI Select to avoid pointer capture issues
vi.mock('~/components/ui/select', () => ({
  Select: ({ children, value, onValueChange }: any) => (
    <div data-testid="select-root">
      <input type="hidden" value={value} onChange={(e) => onValueChange(e.target.value)} />
      {children}
    </div>
  ),
  SelectTrigger: ({ children }: any) => (
    <button role="combobox" data-testid="select-trigger">
      {children}
    </button>
  ),
  SelectValue: () => {
    const store = useSchedulerStore();
    const value = store.selectedNodeLabelFilter || 'DEFAULT';
    if (value === 'DEFAULT') return <span>Default Partition</span>;
    return <span>{value}</span>;
  },
  SelectContent: ({ children }: any) => <div data-testid="select-content">{children}</div>,
  SelectItem: ({ children, value, onClick }: any) => {
    const store = useSchedulerStore();
    return (
      <div
        data-testid={`select-item-${value}`}
        onClick={() => {
          store.selectNodeLabelFilter(value === 'DEFAULT' ? '' : value);
          onClick?.();
        }}
      >
        {children}
      </div>
    );
  },
}));

describe('NodeLabelSelector', () => {
  const mockSelectNodeLabelFilter = vi.fn();
  const mockNodeLabels: NodeLabel[] = [
    { name: 'gpu', exclusivity: true },
    { name: 'cpu', exclusivity: false },
    { name: 'memory', exclusivity: true },
  ];

  beforeEach(() => {
    vi.clearAllMocks();
    vi.mocked(useSchedulerStore).mockReturnValue({
      nodeLabels: mockNodeLabels,
      selectedNodeLabelFilter: '',
      selectNodeLabelFilter: mockSelectNodeLabelFilter,
    } as any);
  });

  it('should render with DEFAULT option selected initially', () => {
    render(<NodeLabelSelector />);

    const trigger = screen.getByRole('combobox');
    expect(trigger).toHaveTextContent('Default Partition');
  });

  it('should display all node labels', () => {
    render(<NodeLabelSelector />);

    // Check that labels are rendered - look within select content specifically
    const selectContent = screen.getByTestId('select-content');
    expect(selectContent).toHaveTextContent('Default Partition');
    expect(selectContent).toHaveTextContent('gpu');
    expect(selectContent).toHaveTextContent('cpu');
    expect(selectContent).toHaveTextContent('memory');
  });

  it('should show exclusivity badges for exclusive labels', () => {
    render(<NodeLabelSelector />);

    // Should have 2 "Exclusive" badges (for gpu and memory)
    const exclusiveBadges = screen.getAllByText('Exclusive');
    expect(exclusiveBadges).toHaveLength(2);
  });

  it('should call selectNodeLabelFilter when label is selected', async () => {
    render(<NodeLabelSelector />);
    const user = userEvent.setup();

    // Click on gpu option
    const gpuOption = screen.getByTestId('select-item-gpu');
    await user.click(gpuOption);

    expect(mockSelectNodeLabelFilter).toHaveBeenCalledWith('gpu');
  });

  it('should call selectNodeLabelFilter with empty string when DEFAULT is selected', async () => {
    render(<NodeLabelSelector />);
    const user = userEvent.setup();

    const defaultOption = screen.getByTestId('select-item-DEFAULT');
    await user.click(defaultOption);

    expect(mockSelectNodeLabelFilter).toHaveBeenCalledWith('');
  });

  it('should show the selected label in the trigger', () => {
    vi.mocked(useSchedulerStore).mockReturnValue({
      nodeLabels: mockNodeLabels,
      selectedNodeLabelFilter: 'cpu',
      selectNodeLabelFilter: mockSelectNodeLabelFilter,
    } as any);

    render(<NodeLabelSelector />);

    const trigger = screen.getByRole('combobox');
    expect(trigger).toHaveTextContent('cpu');
  });

  it('should handle empty node labels list', () => {
    vi.mocked(useSchedulerStore).mockReturnValue({
      nodeLabels: [],
      selectedNodeLabelFilter: '',
      selectNodeLabelFilter: mockSelectNodeLabelFilter,
    } as any);

    render(<NodeLabelSelector />);

    const trigger = screen.getByRole('combobox');
    expect(trigger).toBeInTheDocument();
    expect(trigger).toHaveTextContent('Default Partition');
  });
});
