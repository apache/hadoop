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


import { render, screen, fireEvent, within } from '@testing-library/react';
import { describe, it, expect, beforeEach, vi, type Mock } from 'vitest';
import type { ReactNode } from 'react';
import { GlobalSettings } from './GlobalSettings';
import { useSchedulerStore } from '~/stores/schedulerStore';
import { globalPropertyDefinitions } from '~/config/properties/global-properties';
import type { PropertyDescriptor } from '~/types/property-descriptor';
import type { StagedChange } from '~/types/staged-change';
import { ValidationProvider } from '~/contexts/ValidationContext';
import { SPECIAL_VALUES } from '~/types';

// Mock the store
vi.mock('~/stores/schedulerStore');

// Mock the property definitions
vi.mock('~/config/properties/global-properties', () => ({
  globalPropertyDefinitions: [],
}));

// Mock UI components that might have complex implementations
vi.mock('~/components/ui/accordion', () => ({
  Accordion: ({ children, ...props }: any) => (
    <div data-testid="accordion" {...props}>
      {children}
    </div>
  ),
  AccordionContent: ({ children }: any) => <div data-testid="accordion-content">{children}</div>,
  AccordionItem: ({ children, ...props }: any) => (
    <div data-testid="accordion-item" {...props}>
      {children}
    </div>
  ),
  AccordionTrigger: ({ children }: any) => (
    <button data-testid="accordion-trigger">{children}</button>
  ),
}));

vi.mock('./PropertyInput', () => ({
  PropertyInput: vi.fn(({ property, value, isStaged, onChange, disabled }: any) => (
    <div data-testid={`property-input-${property.name}`}>
      <input
        data-testid={`input-${property.name}`}
        value={value || ''}
        onChange={(e) => onChange(e.target.value)}
        data-is-staged={isStaged}
        disabled={disabled}
      />
    </div>
  )),
}));

// Test data factories
const getMockPropertyDescriptor = (overrides?: Partial<PropertyDescriptor>): PropertyDescriptor => {
  return {
    name: 'test-property',
    displayName: 'Test Property',
    description: 'A test property',
    type: 'string',
    category: 'core',
    defaultValue: 'default',
    required: false,
    ...overrides,
  };
};

const getMockStagedChange = (overrides?: Partial<StagedChange>): StagedChange => {
  return {
    id: 'change-123',
    type: 'update',
    queuePath: 'global',
    property: 'test-property',
    oldValue: 'old',
    newValue: 'new',
    timestamp: Date.now(),
    ...overrides,
  };
};

// Mock store implementation
const baseSchedulerData = {
  type: 'capacityScheduler',
  queueName: 'root',
  queuePath: 'root',
  capacity: 100,
  maxCapacity: 100,
  usedCapacity: 0,
  queues: { queue: [] as any[] },
};

const createStoreState = (overrides?: Partial<Record<string, unknown>>) => ({
  schedulerData: baseSchedulerData,
  configData: new Map([[SPECIAL_VALUES.LEGACY_MODE_PROPERTY, 'true']]),
  stagedChanges: [] as StagedChange[],
  searchQuery: undefined as string | undefined,
  getFilteredSettings: vi.fn(() => globalPropertyDefinitions),
  getGlobalPropertyValue: vi.fn().mockReturnValue({ value: 'test-value', isStaged: false }),
  getQueuePropertyValue: vi.fn().mockReturnValue({ value: '', isStaged: false }),
  stageGlobalChange: vi.fn(),
  applyError: null,
  ...overrides,
});

const setupStoreMock = (stateOverrides?: Partial<Record<string, unknown>>) => {
  const storeState = createStoreState(stateOverrides);
  (useSchedulerStore as unknown as Mock).mockImplementation((selector?: (state: any) => any) =>
    selector ? selector(storeState) : storeState,
  );
  return storeState;
};

const renderWithValidation = (ui: ReactNode, storeOverrides?: Partial<Record<string, unknown>>) => {
  const storeState = setupStoreMock(storeOverrides);
  return { renderResult: render(<ValidationProvider>{ui}</ValidationProvider>), storeState };
};

describe('GlobalSettings', () => {
  beforeEach(() => {
    vi.clearAllMocks();
    // Reset property definitions
    (globalPropertyDefinitions as PropertyDescriptor[]).length = 0;
  });

  describe('rendering', () => {
    it('should render a message when no global properties are available', () => {
      const stageGlobalChange = vi.fn();
      renderWithValidation(<GlobalSettings />, { stageGlobalChange });

      expect(screen.getByText('No Global Properties Available')).toBeInTheDocument();
      expect(
        screen.getByText(
          'Global properties configuration is not available. Please check the configuration setup.',
        ),
      ).toBeInTheDocument();
    });

    it('should render property categories as accordion items', () => {
      const properties = [
        getMockPropertyDescriptor({ name: 'prop1', category: 'core' }),
        getMockPropertyDescriptor({ name: 'prop2', category: 'security' }),
        getMockPropertyDescriptor({ name: 'prop3', category: 'core' }),
      ];
      (globalPropertyDefinitions as PropertyDescriptor[]).push(...properties);

      renderWithValidation(<GlobalSettings />);

      expect(screen.getAllByText('Core Settings').length).toBeGreaterThanOrEqual(1);
      expect(screen.getAllByText('Security & Access Control').length).toBeGreaterThanOrEqual(1);
    });

    it('should render all properties within their categories', () => {
      const properties = [
        getMockPropertyDescriptor({ name: 'prop1', category: 'core' }),
        getMockPropertyDescriptor({ name: 'prop2', category: 'core' }),
        getMockPropertyDescriptor({ name: 'prop3', category: 'security' }),
      ];
      (globalPropertyDefinitions as PropertyDescriptor[]).push(...properties);

      renderWithValidation(<GlobalSettings />);

      expect(screen.getByTestId('property-input-prop1')).toBeInTheDocument();
      expect(screen.getByTestId('property-input-prop2')).toBeInTheDocument();
      expect(screen.getByTestId('property-input-prop3')).toBeInTheDocument();
    });

    it('should display current property values', () => {
      const properties = [
        getMockPropertyDescriptor({ name: 'prop1' }),
        getMockPropertyDescriptor({ name: 'prop2' }),
      ];
      (globalPropertyDefinitions as PropertyDescriptor[]).push(...properties);

      const getGlobalPropertyValue = vi.fn((name: string) => {
        if (name === 'prop1') {
          return { value: 'value1', isStaged: false };
        }
        if (name === 'prop2') {
          return { value: 'value2', isStaged: true };
        }
        return { value: '', isStaged: false };
      });

      renderWithValidation(<GlobalSettings />, { getGlobalPropertyValue });

      expect(screen.getByTestId('input-prop1')).toHaveValue('value1');
      expect(screen.getByTestId('input-prop2')).toHaveValue('value2');
      expect(screen.getByTestId('input-prop2')).toHaveAttribute('data-is-staged', 'true');
    });

    it('should display apply error alert when present', () => {
      const properties = [getMockPropertyDescriptor()];
      (globalPropertyDefinitions as PropertyDescriptor[]).push(...properties);

      renderWithValidation(<GlobalSettings />, { applyError: 'HTTP 400: Invalid configuration' });

      expect(screen.getByText('Failed to Apply Changes')).toBeInTheDocument();
      expect(screen.getByText('HTTP 400: Invalid configuration')).toBeInTheDocument();
    });
  });

  describe('unsaved changes alert', () => {
    it('should not display alert when there are no staged changes', () => {
      const properties = [getMockPropertyDescriptor()];
      (globalPropertyDefinitions as PropertyDescriptor[]).push(...properties);

      renderWithValidation(<GlobalSettings />);

      expect(screen.queryByText(/You have \d+ unsaved global setting/)).not.toBeInTheDocument();
    });

    it('should display alert when there are global staged changes', () => {
      const properties = [getMockPropertyDescriptor()];
      (globalPropertyDefinitions as PropertyDescriptor[]).push(...properties);

      const stagedChanges = [
        getMockStagedChange({ property: 'prop1' }),
        getMockStagedChange({ property: 'prop2' }),
      ];

      renderWithValidation(<GlobalSettings />, { stagedChanges });

      expect(
        screen.getByText('You have 2 unsaved global settings. Apply changes to make them active.'),
      ).toBeInTheDocument();
    });

    it('should display correct singular/plural text for staged changes', () => {
      const properties = [getMockPropertyDescriptor()];
      (globalPropertyDefinitions as PropertyDescriptor[]).push(...properties);

      const stagedChanges = [getMockStagedChange()];

      renderWithValidation(<GlobalSettings />, { stagedChanges });

      expect(
        screen.getByText('You have 1 unsaved global setting. Apply changes to make them active.'),
      ).toBeInTheDocument();
    });

    it('should only count global staged changes, not queue-specific ones', () => {
      const properties = [getMockPropertyDescriptor()];
      (globalPropertyDefinitions as PropertyDescriptor[]).push(...properties);

      const stagedChanges = [
        getMockStagedChange({ queuePath: 'global' }),
        getMockStagedChange({ queuePath: 'root.queue1' }),
        getMockStagedChange({ queuePath: 'global' }),
      ];
      renderWithValidation(<GlobalSettings />, { stagedChanges });

      expect(
        screen.getByText('You have 2 unsaved global settings. Apply changes to make them active.'),
      ).toBeInTheDocument();
    });
  });

  describe('category badges', () => {
    it('should show "Has Changes" badge for categories with staged changes', () => {
      const properties = [
        getMockPropertyDescriptor({ name: 'prop1', category: 'core' }),
        getMockPropertyDescriptor({ name: 'prop2', category: 'security' }),
      ];
      (globalPropertyDefinitions as PropertyDescriptor[]).push(...properties);

      const stagedChanges = [getMockStagedChange({ property: 'prop1' })];
      renderWithValidation(<GlobalSettings />, { stagedChanges });

      // Find the general category heading and its badge
      const generalHeading = screen.getByRole('heading', { name: 'Core Settings' }).closest('div');
      expect(generalHeading).toHaveTextContent('Has Changes');

      // Security category should not have the badge
      const securityHeading = screen
        .getByRole('heading', { name: 'Security & Access Control' })
        .closest('div');
      expect(securityHeading).not.toHaveTextContent('Has Changes');
    });

    it('should not show badge when category has no changes', () => {
      const properties = [
        getMockPropertyDescriptor({ name: 'prop1', category: 'core' }),
        getMockPropertyDescriptor({ name: 'prop2', category: 'core' }),
      ];
      (globalPropertyDefinitions as PropertyDescriptor[]).push(...properties);
      renderWithValidation(<GlobalSettings />);

      const generalHeading = screen.getByRole('heading', { name: 'Core Settings' }).closest('div');
      expect(generalHeading).not.toHaveTextContent('Has Changes');
    });
  });

  describe('user interactions', () => {
    it('should call stageGlobalChange when property value is changed', () => {
      const properties = [getMockPropertyDescriptor({ name: 'test-property' })];
      (globalPropertyDefinitions as PropertyDescriptor[]).push(...properties);

      const stageGlobalChange = vi.fn();
      renderWithValidation(<GlobalSettings />, { stageGlobalChange });

      const input = screen.getByTestId('input-test-property');

      // Simulate a real change event with final value
      fireEvent.change(input, { target: { value: 'new-value' } });

      // Verify the function was called with expected value
      expect(stageGlobalChange).toHaveBeenCalledWith('test-property', 'new-value', []);
    });

    it('should handle multiple property changes independently', () => {
      const properties = [
        getMockPropertyDescriptor({ name: 'prop1', category: 'core' }),
        getMockPropertyDescriptor({ name: 'prop2', category: 'core' }),
      ];
      (globalPropertyDefinitions as PropertyDescriptor[]).push(...properties);

      const stageGlobalChange = vi.fn();
      renderWithValidation(<GlobalSettings />, { stageGlobalChange });

      const input1 = screen.getByTestId('input-prop1');
      const input2 = screen.getByTestId('input-prop2');

      // Change first input
      fireEvent.change(input1, { target: { value: 'value1' } });

      // Change second input
      fireEvent.change(input2, { target: { value: 'value2' } });

      // Verify both properties were updated
      expect(stageGlobalChange).toHaveBeenCalledWith('prop1', 'value1', []);
      expect(stageGlobalChange).toHaveBeenCalledWith('prop2', 'value2', []);
    });
  });

  describe('property ordering', () => {
    it('should display categories in globalCategoryOrder', () => {
      const properties = [
        getMockPropertyDescriptor({ category: 'async-scheduling' }),
        getMockPropertyDescriptor({ category: 'core' }),
        getMockPropertyDescriptor({ category: 'application-limits' }),
      ];
      (globalPropertyDefinitions as PropertyDescriptor[]).push(...properties);

      renderWithValidation(<GlobalSettings />);

      const headings = screen.getAllByTestId('accordion-trigger');
      expect(headings[0]).toHaveTextContent('Core Settings');
      expect(headings[1]).toHaveTextContent('Application Limits');
      expect(headings[2]).toHaveTextContent('Asynchronous Scheduling');
    });

    it('should maintain property order within categories as defined', () => {
      const properties = [
        getMockPropertyDescriptor({ name: 'prop3', category: 'core' }),
        getMockPropertyDescriptor({ name: 'prop1', category: 'core' }),
        getMockPropertyDescriptor({ name: 'prop2', category: 'core' }),
      ];
      (globalPropertyDefinitions as PropertyDescriptor[]).push(...properties);

      renderWithValidation(<GlobalSettings />);

      const propertyInputs = screen.getAllByTestId(/^property-input-prop/);
      expect(propertyInputs[0]).toHaveAttribute('data-testid', 'property-input-prop3');
      expect(propertyInputs[1]).toHaveAttribute('data-testid', 'property-input-prop1');
      expect(propertyInputs[2]).toHaveAttribute('data-testid', 'property-input-prop2');
    });
  });

  describe('property value synchronization', () => {
    it('should request correct property values from store', () => {
      const properties = [
        getMockPropertyDescriptor({ name: 'prop1' }),
        getMockPropertyDescriptor({ name: 'prop2' }),
      ];
      (globalPropertyDefinitions as PropertyDescriptor[]).push(...properties);

      const getGlobalPropertyValue = vi.fn().mockReturnValue({ value: 'test', isStaged: false });

      renderWithValidation(<GlobalSettings />, { getGlobalPropertyValue });

      expect(getGlobalPropertyValue).toHaveBeenCalledWith('prop1');
      expect(getGlobalPropertyValue).toHaveBeenCalledWith('prop2');
    });

    it('should pass staged status to property inputs', () => {
      const properties = [
        getMockPropertyDescriptor({ name: 'staged-prop' }),
        getMockPropertyDescriptor({ name: 'unstaged-prop' }),
      ];
      (globalPropertyDefinitions as PropertyDescriptor[]).push(...properties);

      const getGlobalPropertyValue = vi.fn((name: string) => {
        if (name === 'staged-prop') {
          return { value: 'val1', isStaged: true };
        }
        if (name === 'unstaged-prop') {
          return { value: 'val2', isStaged: false };
        }
        return { value: '', isStaged: false };
      });

      renderWithValidation(<GlobalSettings />, { getGlobalPropertyValue });

      expect(screen.getByTestId('input-staged-prop')).toHaveAttribute('data-is-staged', 'true');
      expect(screen.getByTestId('input-unstaged-prop')).toHaveAttribute('data-is-staged', 'false');
    });
  });

  describe('quick-jump navigation', () => {
    it('should render quick-jump links when multiple categories exist', () => {
      const properties = [
        getMockPropertyDescriptor({ name: 'prop1', category: 'core' }),
        getMockPropertyDescriptor({ name: 'prop2', category: 'security' }),
      ];
      (globalPropertyDefinitions as PropertyDescriptor[]).push(...properties);

      renderWithValidation(<GlobalSettings />);

      const nav = screen.getByRole('navigation', { name: /settings sections/i });
      expect(nav).toBeInTheDocument();

      const links = within(nav).getAllByRole('link');
      expect(links).toHaveLength(2);
      expect(links[0]).toHaveTextContent('Core Settings');
      expect(links[1]).toHaveTextContent('Security & Access Control');
    });

    it('should not render quick-jump nav when only one category exists', () => {
      const properties = [
        getMockPropertyDescriptor({ name: 'prop1', category: 'core' }),
        getMockPropertyDescriptor({ name: 'prop2', category: 'core' }),
      ];
      (globalPropertyDefinitions as PropertyDescriptor[]).push(...properties);

      renderWithValidation(<GlobalSettings />);

      expect(
        screen.queryByRole('navigation', { name: /settings sections/i }),
      ).not.toBeInTheDocument();
    });

    it('should add id attributes to accordion sections', () => {
      const properties = [
        getMockPropertyDescriptor({ name: 'prop1', category: 'core' }),
        getMockPropertyDescriptor({ name: 'prop2', category: 'security' }),
      ];
      (globalPropertyDefinitions as PropertyDescriptor[]).push(...properties);

      renderWithValidation(<GlobalSettings />);

      expect(document.getElementById('section-core')).toBeInTheDocument();
      expect(document.getElementById('section-security')).toBeInTheDocument();
    });
  });

  describe('accordion behavior', () => {
    it('should render all category accordions', () => {
      const properties = [
        getMockPropertyDescriptor({ name: 'prop1', category: 'core' }),
        getMockPropertyDescriptor({ name: 'prop2', category: 'security' }),
        getMockPropertyDescriptor({ name: 'prop3', category: 'scheduling' }),
      ];
      (globalPropertyDefinitions as PropertyDescriptor[]).push(...properties);

      renderWithValidation(<GlobalSettings />);

      // Verify all category accordions are rendered (labels also appear in quick-jump nav)
      expect(screen.getAllByText('Core Settings').length).toBeGreaterThanOrEqual(1);
      expect(screen.getAllByText('Security & Access Control').length).toBeGreaterThanOrEqual(1);
      expect(screen.getAllByText('Scheduling Policy').length).toBeGreaterThanOrEqual(1);

      // Verify correct number of accordion items
      const accordionItems = screen.getAllByTestId('accordion-item');
      expect(accordionItems).toHaveLength(3);
    });
  });

  describe('edge cases', () => {
    it('should handle empty property values gracefully', () => {
      const properties = [getMockPropertyDescriptor()];
      (globalPropertyDefinitions as PropertyDescriptor[]).push(...properties);

      const getGlobalPropertyValue = vi.fn().mockReturnValue({ value: '', isStaged: false });

      renderWithValidation(<GlobalSettings />, { getGlobalPropertyValue });

      expect(screen.getByTestId('input-test-property')).toHaveValue('');
    });

    it('should handle properties with special characters in names', () => {
      const properties = [
        getMockPropertyDescriptor({ name: 'property-with-dashes' }),
        getMockPropertyDescriptor({ name: 'property.with.dots' }),
      ];
      (globalPropertyDefinitions as PropertyDescriptor[]).push(...properties);

      const stageGlobalChange = vi.fn();
      renderWithValidation(<GlobalSettings />, { stageGlobalChange });

      const input1 = screen.getByTestId('input-property-with-dashes');
      const input2 = screen.getByTestId('input-property.with.dots');

      // Change inputs
      fireEvent.change(input1, { target: { value: 'test1' } });
      fireEvent.change(input2, { target: { value: 'test2' } });

      // Verify the property names were handled correctly
      expect(stageGlobalChange).toHaveBeenCalledWith('property-with-dashes', 'test1', []);
      expect(stageGlobalChange).toHaveBeenCalledWith('property.with.dots', 'test2', []);
    });

    it('should render correctly when properties have the same category', () => {
      const properties = [
        getMockPropertyDescriptor({ name: 'prop1', category: 'core' }),
        getMockPropertyDescriptor({ name: 'prop2', category: 'core' }),
        getMockPropertyDescriptor({ name: 'prop3', category: 'core' }),
      ];
      (globalPropertyDefinitions as PropertyDescriptor[]).push(...properties);

      renderWithValidation(<GlobalSettings />);

      // Should only have one accordion item for the general category
      expect(screen.getAllByTestId('accordion-item')).toHaveLength(1);
      expect(screen.getByText('Core Settings')).toBeInTheDocument();

      // But all three properties should be rendered
      expect(screen.getByTestId('property-input-prop1')).toBeInTheDocument();
      expect(screen.getByTestId('property-input-prop2')).toBeInTheDocument();
      expect(screen.getByTestId('property-input-prop3')).toBeInTheDocument();
    });
  });
});
