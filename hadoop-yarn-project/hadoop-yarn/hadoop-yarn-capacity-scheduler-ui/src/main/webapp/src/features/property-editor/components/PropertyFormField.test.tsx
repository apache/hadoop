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
import { render, screen } from '~/testing/setup/setup';
import { PropertyFormField } from './PropertyFormField';
import { useForm, FormProvider } from 'react-hook-form';
import userEvent from '@testing-library/user-event';
import { TooltipProvider } from '~/components/ui/tooltip';
import type { PropertyDescriptor } from '~/types/property-descriptor';

// Test helper
const getMockPropertyDescriptor = (overrides?: Partial<PropertyDescriptor>): PropertyDescriptor => {
  return {
    name: 'capacity',
    displayName: 'Capacity',
    description: 'Queue capacity as percentage, weight, or absolute resources',
    type: 'string',
    category: 'core',
    required: true,
    defaultValue: '0',
    ...overrides,
  };
};

// Helper component to wrap PropertyFormField with form context
function FormWrapper({
  children,
  defaultValues = {},
}: {
  children: React.ReactNode;
  defaultValues?: Record<string, string>;
}) {
  const methods = useForm({ defaultValues });
  return (
    <TooltipProvider>
      <FormProvider {...methods}>{children}</FormProvider>
    </TooltipProvider>
  );
}

describe('PropertyFormField', () => {
  it('should render text input for string property type', () => {
    const property = getMockPropertyDescriptor({
      name: 'user-limit-factor',
      displayName: 'User Limit Factor',
      type: 'string',
      required: false,
    });

    render(
      <FormWrapper>
        <PropertyFormField property={property} control={undefined as any} />
      </FormWrapper>,
    );

    const input = screen.getByRole('textbox');
    expect(input).toBeInTheDocument();
    expect(screen.getByText('User Limit Factor')).toBeInTheDocument();
  });

  it('should render number input for number property type', () => {
    const property = getMockPropertyDescriptor({
      name: 'maximum-applications',
      displayName: 'Maximum Applications',
      type: 'number',
      required: false,
    });

    render(
      <FormWrapper>
        <PropertyFormField property={property} control={undefined as any} />
      </FormWrapper>,
    );

    const input = screen.getByRole('spinbutton');
    expect(input).toBeInTheDocument();
    expect(input).toHaveAttribute('type', 'number');
  });

  it('should render switch for boolean property type', () => {
    const property = getMockPropertyDescriptor({
      name: 'enable-preemption',
      displayName: 'Enable Preemption',
      type: 'boolean',
      required: false,
    });

    render(
      <FormWrapper defaultValues={{ 'enable-preemption': 'true' }}>
        <PropertyFormField property={property} control={undefined as any} />
      </FormWrapper>,
    );

    const switchElement = screen.getByRole('switch');
    expect(switchElement).toBeInTheDocument();
    expect(switchElement).toHaveAttribute('aria-checked', 'true');
  });

  it('writes explicit false when boolean switch is toggled off', async () => {
    const property = getMockPropertyDescriptor({
      name: 'auto-queue-creation-v2.enabled',
      displayName: 'Flexible Auto-Creation',
      type: 'boolean',
      required: false,
    });

    const onBlur = vi.fn();
    const user = userEvent.setup();

    render(
      <FormWrapper defaultValues={{ 'auto-queue-creation-v2.enabled': 'true' }}>
        <PropertyFormField property={property} control={undefined as any} onBlur={onBlur} />
      </FormWrapper>,
    );

    const switchElement = screen.getByRole('switch');
    expect(switchElement).toHaveAttribute('aria-checked', 'true');

    await user.click(switchElement);

    expect(switchElement).toHaveAttribute('aria-checked', 'false');
    expect(onBlur).toHaveBeenCalledWith('auto-queue-creation-v2.enabled', 'false');
  });

  it('should render toggle group for enum property type', () => {
    const property = getMockPropertyDescriptor({
      name: 'state',
      displayName: 'State',
      type: 'enum',
      enumValues: [
        { value: 'RUNNING', label: 'Running' },
        { value: 'STOPPED', label: 'Stopped' },
      ],
      required: false,
    });

    render(
      <FormWrapper defaultValues={{ state: 'RUNNING' }}>
        <PropertyFormField property={property} control={undefined as any} />
      </FormWrapper>,
    );

    expect(screen.getByRole('radio', { name: 'Running' })).toBeInTheDocument();
    expect(screen.getByRole('radio', { name: 'Stopped' })).toBeInTheDocument();
    expect(screen.getByRole('radio', { name: 'Running' })).toHaveAttribute('data-state', 'on');
  });

  it('renders choice cards for enum properties requesting card layout', () => {
    const property = getMockPropertyDescriptor({
      name: 'resource-calculator',
      displayName: 'Resource Calculator',
      type: 'enum',
      enumDisplay: 'choiceCard',
      enumValues: [
        {
          value: 'default',
          label: 'Default (Memory Only)',
          description: 'Memory-based calculator for simple clusters.',
        },
        {
          value: 'dominant',
          label: 'Dominant Resource',
          description: 'Considers both memory and CPU for fairness.',
        },
      ],
    });

    render(
      <FormWrapper defaultValues={{ 'resource-calculator': 'default' }}>
        <PropertyFormField property={property} control={undefined as any} />
      </FormWrapper>,
    );

    expect(screen.getByRole('radio', { name: /Default \(Memory Only\)/i })).toBeInTheDocument();
    expect(screen.getByRole('radio', { name: /Dominant Resource/i })).toBeInTheDocument();
    expect(screen.getByText('Memory-based calculator for simple clusters.')).toBeInTheDocument();
  });

  it('should show required indicator for required fields', () => {
    const property = getMockPropertyDescriptor({
      name: 'capacity',
      displayName: 'Capacity',
      type: 'string',
      required: true,
    });

    render(
      <FormWrapper>
        <PropertyFormField property={property} control={undefined as any} />
      </FormWrapper>,
    );

    expect(screen.getByText('Capacity *')).toBeInTheDocument();
  });

  it('should show capacity editor button for capacity field', () => {
    const property = getMockPropertyDescriptor({
      name: 'capacity',
      displayName: 'Capacity',
      description: 'This is the capacity description',
      type: 'string',
    });

    render(
      <FormWrapper>
        <PropertyFormField property={property} control={undefined as any} />
      </FormWrapper>,
    );

    expect(screen.getByText('Edit Capacity')).toBeInTheDocument();
    expect(screen.queryByText('This is the capacity description')).not.toBeInTheDocument();
  });

  it('should display the property description inline for non-capacity fields', () => {
    const property = getMockPropertyDescriptor({
      name: 'yarn.scheduler.capacity.some-property',
      displayName: 'Some Property',
      description: 'Inline description',
      type: 'string',
    });

    render(
      <FormWrapper>
        <PropertyFormField property={property} control={undefined as any} />
      </FormWrapper>,
    );

    expect(screen.getByText('Inline description')).toBeInTheDocument();
  });

  it('should show staged badge when field is modified', () => {
    const property = getMockPropertyDescriptor({
      name: 'capacity',
      displayName: 'Capacity',
      type: 'string',
    });

    render(
      <FormWrapper>
        <PropertyFormField property={property} control={undefined as any} stagedStatus="modified" />
      </FormWrapper>,
    );

    expect(screen.getByText('Staged')).toBeInTheDocument();
  });

  it('should disable field based on enableWhen condition', () => {
    const property = getMockPropertyDescriptor({
      name: 'accessible-node-labels',
      displayName: 'Accessible Node Labels',
      type: 'string',
    });

    render(
      <FormWrapper>
        <PropertyFormField property={property} control={undefined as any} isEnabled={false} />
      </FormWrapper>,
    );

    const input = screen.getByRole('textbox');
    expect(input).toBeDisabled();
  });

  it('should show deprecation warning for deprecated properties', () => {
    const property = getMockPropertyDescriptor({
      name: 'old-property',
      displayName: 'Old Property',
      type: 'string',
      deprecated: true,
      deprecationMessage: 'Use new-property instead',
    });

    render(
      <FormWrapper>
        <PropertyFormField property={property} control={undefined as any} />
      </FormWrapper>,
    );

    expect(screen.getByText('Deprecated')).toBeInTheDocument();
    expect(screen.getByText('Use new-property instead')).toBeInTheDocument();
  });

  it('should render textarea for ACL properties', () => {
    const property = getMockPropertyDescriptor({
      name: 'acl-submit-applications',
      displayName: 'Submit Applications ACL',
      type: 'string',
    });

    render(
      <FormWrapper>
        <PropertyFormField property={property} control={undefined as any} />
      </FormWrapper>,
    );

    const textarea = screen.getByRole('textbox');
    expect(textarea.tagName).toBe('TEXTAREA');
  });

  it('should prevent deselection in enum toggle group', async () => {
    const user = userEvent.setup();
    const property = getMockPropertyDescriptor({
      name: 'state',
      displayName: 'State',
      type: 'enum',
      enumValues: [
        { value: 'RUNNING', label: 'Running' },
        { value: 'STOPPED', label: 'Stopped' },
      ],
    });

    render(
      <FormWrapper defaultValues={{ state: 'RUNNING' }}>
        <PropertyFormField property={property} control={undefined as any} />
      </FormWrapper>,
    );

    const runningToggle = screen.getByRole('radio', { name: 'Running' });
    expect(runningToggle).toHaveAttribute('data-state', 'on');

    // Try to click the already selected toggle
    await user.click(runningToggle);

    // Should still be selected
    expect(runningToggle).toHaveAttribute('data-state', 'on');
  });

  it('should display suffix for number fields with display format', () => {
    const property = getMockPropertyDescriptor({
      name: 'maximum-am-resource-percent',
      displayName: 'Maximum AM Resource Percent',
      type: 'number',
      displayFormat: {
        suffix: ' (0.0-1.0)',
        decimals: 2,
      },
    });

    render(
      <FormWrapper>
        <PropertyFormField property={property} control={undefined as any} />
      </FormWrapper>,
    );

    expect(screen.getByText('(0.0-1.0)')).toBeInTheDocument();
  });

  it('should render select dropdown for enum property with 4 or more options', () => {
    const property = getMockPropertyDescriptor({
      name: 'ordering-policy',
      displayName: 'Ordering Policy',
      description: 'Application ordering policy within the queue',
      type: 'enum',
      enumValues: [
        { value: 'fifo', label: 'FIFO' },
        { value: 'fair', label: 'Fair' },
        {
          value: 'fifo-with-sizebasedweightresourceallocator',
          label: 'FIFO with Resource Allocation Aware',
        },
        { value: 'fifo-for-pending-apps', label: 'FIFO for Pending Apps' },
      ],
      required: false,
    });

    render(
      <FormWrapper defaultValues={{ 'ordering-policy': 'fifo' }}>
        <PropertyFormField property={property} control={undefined as any} />
      </FormWrapper>,
    );

    // Should render a combobox (select) instead of toggle group
    const select = screen.getByRole('combobox');
    expect(select).toBeInTheDocument();

    // Should show the label
    expect(screen.getByText('Ordering Policy')).toBeInTheDocument();

    // Description should be in the document
    expect(screen.getByText('Application ordering policy within the queue')).toBeInTheDocument();
  });

  it('should render toggle group for enum property with 3 or fewer options', () => {
    const property = getMockPropertyDescriptor({
      name: 'state',
      displayName: 'State',
      type: 'enum',
      enumValues: [
        { value: 'RUNNING', label: 'Running' },
        { value: 'STOPPED', label: 'Stopped' },
        { value: 'DRAINING', label: 'Draining' },
      ],
      required: false,
    });

    render(
      <FormWrapper defaultValues={{ state: 'RUNNING' }}>
        <PropertyFormField property={property} control={undefined as any} />
      </FormWrapper>,
    );

    // Should render toggle group (radio buttons) not select
    expect(screen.getByRole('radio', { name: 'Running' })).toBeInTheDocument();
    expect(screen.getByRole('radio', { name: 'Stopped' })).toBeInTheDocument();
    expect(screen.getByRole('radio', { name: 'Draining' })).toBeInTheDocument();
    expect(screen.queryByRole('combobox')).not.toBeInTheDocument();
  });

  it('should display the currently selected value in select dropdown', () => {
    const property = getMockPropertyDescriptor({
      name: 'ordering-policy',
      displayName: 'Ordering Policy',
      type: 'enum',
      enumValues: [
        { value: 'fifo', label: 'FIFO' },
        { value: 'fair', label: 'Fair' },
        {
          value: 'fifo-with-sizebasedweightresourceallocator',
          label: 'FIFO with Resource Allocation Aware',
        },
        { value: 'fifo-for-pending-apps', label: 'FIFO for Pending Apps' },
      ],
    });

    render(
      <FormWrapper defaultValues={{ 'ordering-policy': 'fair' }}>
        <PropertyFormField property={property} control={undefined as any} />
      </FormWrapper>,
    );

    const select = screen.getByRole('combobox');
    expect(select).toBeInTheDocument();
    // The select should show the selected value
    expect(screen.getByText('Fair')).toBeInTheDocument();
  });

  it('should show select dropdown as full width', () => {
    const property = getMockPropertyDescriptor({
      name: 'ordering-policy',
      displayName: 'Ordering Policy',
      type: 'enum',
      enumValues: [
        { value: 'fifo', label: 'FIFO' },
        { value: 'fair', label: 'Fair' },
        {
          value: 'fifo-with-sizebasedweightresourceallocator',
          label: 'FIFO with Resource Allocation Aware',
        },
        { value: 'fifo-for-pending-apps', label: 'FIFO for Pending Apps' },
      ],
    });

    render(
      <FormWrapper defaultValues={{ 'ordering-policy': 'fifo' }}>
        <PropertyFormField property={property} control={undefined as any} />
      </FormWrapper>,
    );

    const select = screen.getByRole('combobox');
    expect(select).toHaveClass('w-full');
  });

  it('should disable select dropdown when isEnabled is false', () => {
    const property = getMockPropertyDescriptor({
      name: 'ordering-policy',
      displayName: 'Ordering Policy',
      type: 'enum',
      enumValues: [
        { value: 'fifo', label: 'FIFO' },
        { value: 'fair', label: 'Fair' },
        {
          value: 'fifo-with-sizebasedweightresourceallocator',
          label: 'FIFO with Resource Allocation Aware',
        },
        { value: 'fifo-for-pending-apps', label: 'FIFO for Pending Apps' },
      ],
    });

    render(
      <FormWrapper defaultValues={{ 'ordering-policy': 'fifo' }}>
        <PropertyFormField property={property} control={undefined as any} isEnabled={false} />
      </FormWrapper>,
    );

    const select = screen.getByRole('combobox');
    expect(select).toBeDisabled();
  });
});
