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


import { describe, it, expect, vi } from 'vitest';
import { render, screen } from '@testing-library/react';
import userEvent from '@testing-library/user-event';
import { PlacementRulesTable } from './PlacementRulesTable';
import type { PlacementRule } from '~/types/features/placement-rules';

const mockRules: PlacementRule[] = [
  {
    type: 'user',
    matches: 'alice',
    policy: 'primaryGroup',
    create: true,
    fallbackResult: 'skip',
  },
  {
    type: 'group',
    matches: 'developers',
    policy: 'specified',
    value: 'dev.queue',
    create: false,
    fallbackResult: 'placeDefault',
  },
  {
    type: 'application',
    matches: 'spark.*',
    policy: 'custom',
    customPlacement: 'root.spark.{app}',
    parentQueue: 'root.spark',
    create: true,
    fallbackResult: 'reject',
  },
];

describe('PlacementRulesTable', () => {
  const defaultProps = {
    rules: mockRules,
    selectedRuleIndex: null,
    onDelete: vi.fn(),
    onSelect: vi.fn(),
    onReorder: vi.fn(),
  };

  it('renders table headers correctly', () => {
    render(<PlacementRulesTable {...defaultProps} />);

    expect(screen.getByText('#')).toBeInTheDocument();
    expect(screen.getByText('Type')).toBeInTheDocument();
    expect(screen.getByText('Matches')).toBeInTheDocument();
    expect(screen.getByText('Policy')).toBeInTheDocument();
    expect(screen.getByText('Target Queue')).toBeInTheDocument();
    expect(screen.getByText('Create')).toBeInTheDocument();
    expect(screen.getByText('Fallback')).toBeInTheDocument();
    // Actions column has no header text
  });

  it('renders all placement rules', () => {
    render(<PlacementRulesTable {...defaultProps} />);

    // Check rule numbers
    expect(screen.getByText('1')).toBeInTheDocument();
    expect(screen.getByText('2')).toBeInTheDocument();
    expect(screen.getByText('3')).toBeInTheDocument();

    // Check types
    expect(screen.getByText('user')).toBeInTheDocument();
    expect(screen.getByText('group')).toBeInTheDocument();
    expect(screen.getByText('application')).toBeInTheDocument();

    // Check match patterns
    expect(screen.getByText('alice')).toBeInTheDocument();
    expect(screen.getByText('developers')).toBeInTheDocument();
    expect(screen.getByText('spark.*')).toBeInTheDocument();

    // Check policies
    expect(screen.getByText('Primary Group')).toBeInTheDocument();
    expect(screen.getByText('Specified Queue')).toBeInTheDocument();
    expect(screen.getByText('Custom Placement')).toBeInTheDocument();
  });

  it('displays target queue correctly for different policies', () => {
    render(<PlacementRulesTable {...defaultProps} />);

    // Check by finding the actual text content directly
    expect(screen.getByText('Dynamic')).toBeInTheDocument(); // primaryGroup policy
    expect(screen.getByText('dev.queue')).toBeInTheDocument(); // specified policy with value
    expect(screen.getByText('root.spark')).toBeInTheDocument(); // custom policy shows parentQueue since it has one
  });

  it('displays create queue status with icons', () => {
    render(<PlacementRulesTable {...defaultProps} />);

    // Should have 2 check icons (rules with create: true)
    const checkIcons = document.querySelectorAll('.text-green-600');
    expect(checkIcons).toHaveLength(2);

    // Should have 1 X icon (rule with create: false)
    const xIcons = document.querySelectorAll('.text-muted-foreground');
    expect(xIcons.length).toBeGreaterThan(0); // At least one for the create: false rule
  });

  it('displays fallback results', () => {
    render(<PlacementRulesTable {...defaultProps} />);

    expect(screen.getByText('skip')).toBeInTheDocument();
    expect(screen.getByText('placeDefault')).toBeInTheDocument();
    expect(screen.getByText('reject')).toBeInTheDocument();
  });

  it('highlights selected row', () => {
    render(<PlacementRulesTable {...defaultProps} selectedRuleIndex={1} />);

    // Find the row containing the second rule's text
    const secondRuleRow = screen.getByText('developers').closest('tr');
    expect(secondRuleRow).toHaveClass('bg-primary/5');
  });

  it('calls onSelect when row is clicked', async () => {
    const onSelect = vi.fn();
    render(<PlacementRulesTable {...defaultProps} onSelect={onSelect} />);

    // Click on the first rule row by finding its content
    const firstRuleRow = screen.getByText('alice').closest('tr');
    await userEvent.click(firstRuleRow!);

    expect(onSelect).toHaveBeenCalledWith(0);
  });

  it('calls onDelete when delete button is clicked', async () => {
    const onDelete = vi.fn();
    render(<PlacementRulesTable {...defaultProps} onDelete={onDelete} />);

    const deleteButtons = screen.getAllByRole('button');
    // Find delete buttons (they have Trash2 icon and h-7 w-7 class)
    const firstDeleteButton = deleteButtons.find(
      (btn) => btn.querySelector('svg') && btn.classList.contains('h-7'),
    );

    if (firstDeleteButton) {
      await userEvent.click(firstDeleteButton);
    }

    // Should open confirmation dialog
    expect(screen.getByText('Delete Placement Rule')).toBeInTheDocument();

    // Confirm deletion
    const confirmButton = screen.getByRole('button', { name: 'Delete' });
    await userEvent.click(confirmButton);

    expect(onDelete).toHaveBeenCalledWith(0);
  });

  it('cancels delete when cancel button is clicked', async () => {
    const onDelete = vi.fn();
    render(<PlacementRulesTable {...defaultProps} onDelete={onDelete} />);

    const deleteButtons = screen.getAllByRole('button');
    const firstDeleteButton = deleteButtons.find(
      (btn) => btn.querySelector('svg') && btn.classList.contains('h-7'),
    );

    if (firstDeleteButton) {
      await userEvent.click(firstDeleteButton);
    }

    // Cancel deletion
    const cancelButton = screen.getByRole('button', { name: 'Cancel' });
    await userEvent.click(cancelButton);

    expect(onDelete).not.toHaveBeenCalled();
  });

  it('shows empty state when no rules', () => {
    render(<PlacementRulesTable {...defaultProps} rules={[]} />);

    expect(screen.getByText('No placement rules configured')).toBeInTheDocument();
  });

  it('renders drag handle for each row', () => {
    render(<PlacementRulesTable {...defaultProps} />);

    const dragHandles = document.querySelectorAll('[aria-label*="Drag handle"]');
    expect(dragHandles).toHaveLength(0); // Drag handles don't have aria-label in the current implementation

    // Check for GripVertical icons instead
    const gripIcons = document.querySelectorAll('.cursor-grab');
    expect(gripIcons).toHaveLength(3); // One for each rule
  });

  it('displays policy names correctly', () => {
    const rulesWithVariousPolicies: PlacementRule[] = [
      { type: 'user', matches: 'test1', policy: 'user' },
      { type: 'user', matches: 'test2', policy: 'primaryGroupUser', parentQueue: 'root.groups' },
      { type: 'user', matches: 'test3', policy: 'secondaryGroup' },
      { type: 'user', matches: 'test4', policy: 'defaultQueue' },
      { type: 'user', matches: 'test5', policy: 'setDefaultQueue' },
      { type: 'user', matches: 'test6', policy: 'reject' },
    ];

    render(<PlacementRulesTable {...defaultProps} rules={rulesWithVariousPolicies} />);

    expect(screen.getByText('User Queue')).toBeInTheDocument();
    expect(screen.getByText('Primary Group → User')).toBeInTheDocument();
    expect(screen.getByText('Secondary Group')).toBeInTheDocument();
    expect(screen.getByText('Default Queue')).toBeInTheDocument();
    expect(screen.getByText('Set as Default Queue')).toBeInTheDocument();
    expect(screen.getByText('Reject Application')).toBeInTheDocument();
  });

  it('displays target queue for policies with parent queue', () => {
    const rulesWithParentQueue: PlacementRule[] = [
      {
        type: 'user',
        matches: 'alice',
        policy: 'primaryGroupUser',
        parentQueue: 'root.groups',
      },
      {
        type: 'group',
        matches: 'devs',
        policy: 'secondaryGroupUser',
        parentQueue: 'root.teams',
      },
    ];

    render(<PlacementRulesTable {...defaultProps} rules={rulesWithParentQueue} />);

    // According to getTargetQueueDisplay logic, it returns just the parentQueue value
    expect(screen.getByText('root.groups')).toBeInTheDocument();
    expect(screen.getByText('root.teams')).toBeInTheDocument();
  });
});
