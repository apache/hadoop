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
import { PlacementRulesList } from './PlacementRulesList';
import { useSchedulerStore } from '~/stores/schedulerStore';
import type { PlacementRule } from '~/types/features/placement-rules';

// Mock the store
vi.mock('~/stores/schedulerStore');

// Mock the components
vi.mock('./MigrationDialog', () => ({
  PlacementRulesMigrationDialog: vi.fn(() => <div data-testid="migration-dialog" />),
}));

vi.mock('./PlacementRulesTable', () => ({
  PlacementRulesTable: vi.fn(({ rules, onDelete, onSelect }) => (
    <div data-testid="placement-rules-table">
      <table>
        <tbody>
          {rules.map((rule: PlacementRule, index: number) => (
            <tr key={`${rule.type}-${rule.matches}-${rule.policy}`} onClick={() => onSelect(index)}>
              <td>{rule.type}</td>
              <td>{rule.matches}</td>
              <td>
                <button onClick={() => onDelete(index)}>Delete</button>
              </td>
            </tr>
          ))}
        </tbody>
      </table>
    </div>
  )),
}));

vi.mock('./PlacementRuleForm', () => ({
  PlacementRuleForm: vi.fn(({ onSubmit, onCancel, rule, ruleIndex }) => (
    <div data-testid="placement-rule-form">
      <h2>{rule ? 'Edit' : 'Add'} Rule Form</h2>
      <button
        onClick={() => onSubmit({ type: 'user', matches: 'test', policy: 'user' }, ruleIndex)}
      >
        Submit
      </button>
      <button onClick={onCancel}>Cancel</button>
    </div>
  )),
}));

// Mock pragmatic drag and drop
vi.mock('@atlaskit/pragmatic-drag-and-drop/element/adapter', () => ({
  monitorForElements: vi.fn(() => vi.fn()),
  dropTargetForElements: vi.fn(() => vi.fn()),
  draggable: vi.fn(() => vi.fn()),
}));

vi.mock('@atlaskit/pragmatic-drag-and-drop-hitbox/closest-edge', () => ({
  attachClosestEdge: vi.fn((data) => data),
  extractClosestEdge: vi.fn(() => null),
}));

vi.mock('@atlaskit/pragmatic-drag-and-drop-hitbox/util/reorder-with-edge', () => ({
  reorderWithEdge: vi.fn(({ list }) => list),
}));

vi.mock('@atlaskit/pragmatic-drag-and-drop/combine', () => ({
  combine: vi.fn(() => () => {}),
}));

describe('PlacementRulesList', () => {
  const mockRules: PlacementRule[] = [
    {
      type: 'user',
      matches: 'alice',
      policy: 'specified',
      value: 'root.users.alice',
    },
    {
      type: 'group',
      matches: 'developers',
      policy: 'primaryGroup',
    },
  ];

  const mockStoreFunctions = {
    rules: [],
    selectedRuleIndex: null,
    addRule: vi.fn(),
    updateRule: vi.fn(),
    deleteRule: vi.fn(),
    reorderRules: vi.fn(),
    selectRule: vi.fn(),
    loadPlacementRules: vi.fn(),
    isLegacyMode: false,
    legacyRules: null,
    configData: new Map(),
    applyError: null,
  };

  function mockStore(overrides: Record<string, any> = {}) {
    const state = { ...mockStoreFunctions, ...overrides };
    vi.mocked(useSchedulerStore).mockImplementation((selector?: any) => {
      return selector ? selector(state) : state;
    });
  }

  beforeEach(() => {
    vi.clearAllMocks();
    mockStore();
  });

  it('should render empty state when no rules exist', () => {
    render(<PlacementRulesList />);

    expect(screen.getByText('No placement rules configured')).toBeInTheDocument();
    expect(
      screen.getByText('Applications will use the default queue assignment behavior'),
    ).toBeInTheDocument();
    expect(screen.getByRole('button', { name: /add first rule/i })).toBeInTheDocument();
  });

  it('should render table view when rules exist', () => {
    mockStore({ rules: mockRules });

    render(<PlacementRulesList />);

    expect(screen.getByTestId('placement-rules-table')).toBeInTheDocument();
  });

  it('should show add form when Add Rule button is clicked', async () => {
    const user = userEvent.setup();
    render(<PlacementRulesList />);

    const addButton = screen.getByRole('button', { name: /add rule/i });
    await user.click(addButton);

    expect(screen.getByTestId('placement-rule-form')).toBeInTheDocument();
    expect(screen.getByText('Add Rule Form')).toBeInTheDocument();
  });

  it('should call addRule when form is submitted for new rule', async () => {
    const user = userEvent.setup();
    render(<PlacementRulesList />);

    // Click add button to show form
    await user.click(screen.getByRole('button', { name: /add rule/i }));

    // Submit form
    await user.click(screen.getByRole('button', { name: /submit/i }));

    expect(mockStoreFunctions.addRule).toHaveBeenCalledWith({
      type: 'user',
      matches: 'test',
      policy: 'user',
    });
  });

  it('should hide form when cancel is clicked', async () => {
    const user = userEvent.setup();
    render(<PlacementRulesList />);

    // Show form
    await user.click(screen.getByRole('button', { name: /add rule/i }));
    expect(screen.getByTestId('placement-rule-form')).toBeInTheDocument();

    // Cancel form
    await user.click(screen.getByRole('button', { name: /cancel/i }));
    expect(screen.queryByTestId('placement-rule-form')).not.toBeInTheDocument();
  });

  it('should call deleteRule when delete is clicked on a rule', async () => {
    const user = userEvent.setup();
    mockStore({ rules: mockRules });

    render(<PlacementRulesList />);

    // Click delete on first rule
    const deleteButtons = screen.getAllByRole('button', { name: /delete/i });
    await user.click(deleteButtons[0]);

    expect(mockStoreFunctions.deleteRule).toHaveBeenCalledWith(0);
  });

  it('should display info alert about rule evaluation order', () => {
    mockStore({ rules: mockRules });

    render(<PlacementRulesList />);

    expect(
      screen.getByText(
        'Placement rules determine how the queue path is constructed for matching applications. Rules are evaluated from top to bottom, and the first matching rule determines the queue assignment. Drag rules to reorder them.',
      ),
    ).toBeInTheDocument();
  });

  it('should display apply error alert when present', () => {
    mockStore({ applyError: 'HTTP 400: Invalid configuration' });

    render(<PlacementRulesList />);

    expect(screen.getByText('Failed to Apply Changes')).toBeInTheDocument();
    expect(screen.getByText('HTTP 400: Invalid configuration')).toBeInTheDocument();
  });

  it('should call loadPlacementRules on mount when configData is available', () => {
    // Set up configData with some content
    const configWithData = new Map([['some.property', 'value']]);
    mockStore({ configData: configWithData });

    render(<PlacementRulesList />);

    expect(mockStoreFunctions.loadPlacementRules).toHaveBeenCalled();
  });

  it('should not call loadPlacementRules on mount when configData is empty', () => {
    // Use default empty Map for configData
    render(<PlacementRulesList />);

    expect(mockStoreFunctions.loadPlacementRules).not.toHaveBeenCalled();
  });

  describe('legacy mode behavior', () => {
    it('should show legacy mode UI when isLegacyMode is true', () => {
      mockStore({
        isLegacyMode: true,
        legacyRules: 'u:user1:root.default,u:user2:root.production',
      });

      render(<PlacementRulesList />);

      expect(screen.getByText('Legacy Placement Rules Detected')).toBeInTheDocument();
      expect(
        screen.getByText(/This scheduler is using legacy placement rules format/),
      ).toBeInTheDocument();
      expect(screen.getByText('u:user1:root.default,u:user2:root.production')).toBeInTheDocument();
    });

    it('should show migrate button in legacy mode', () => {
      mockStore({ isLegacyMode: true });

      render(<PlacementRulesList />);

      const migrateButton = screen.getByRole('button', { name: /migrate to json format/i });
      expect(migrateButton).toBeInTheDocument();
    });

    it('should not show add form in legacy mode', () => {
      mockStore({ isLegacyMode: true });

      render(<PlacementRulesList />);

      // Add Rule button should not be present in legacy mode
      expect(screen.queryByRole('button', { name: /add rule/i })).not.toBeInTheDocument();
    });

    it('should not show rules table in legacy mode', () => {
      mockStore({ isLegacyMode: true, rules: mockRules });

      render(<PlacementRulesList />);

      expect(screen.queryByTestId('placement-rules-table')).not.toBeInTheDocument();
    });
  });
});
