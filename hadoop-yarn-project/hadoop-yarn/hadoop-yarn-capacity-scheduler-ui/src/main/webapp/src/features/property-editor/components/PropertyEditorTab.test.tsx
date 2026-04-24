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
import userEvent from '@testing-library/user-event';
import { vi } from 'vitest';
import { PropertyEditorTab } from './PropertyEditorTab';
import { usePropertyEditor } from '~/features/property-editor/hooks/usePropertyEditor';
import { useSchedulerStore } from '~/stores/schedulerStore';
import type { QueueInfo, PropertyDescriptor } from '~/types';

// Mock the hooks
vi.mock('~/features/property-editor/hooks/usePropertyEditor');

// Mock the scheduler store
vi.mock('~/stores/schedulerStore', () => ({
  useSchedulerStore: vi.fn((selector: any) => {
    const state = {
      getGlobalPropertyValue: vi.fn().mockReturnValue({ value: '' }),
      getQueuePropertyValue: vi.fn().mockReturnValue({ value: '' }),
      stagedChanges: [],
      configData: new Map(),
      schedulerData: null,
      hasPendingDeletion: vi.fn().mockReturnValue(false),
      revertQueueDeletion: vi.fn(),
    };
    return selector ? selector(state) : state;
  }),
}));

// Mock toast
vi.mock('sonner', () => ({
  toast: {
    success: vi.fn(),
    error: vi.fn(),
  },
}));

// Mock PropertyFormField to avoid react-hook-form issues in tests
vi.mock('./PropertyFormField', () => ({
  PropertyFormField: ({ property }: any) => (
    <div data-testid={`property-field-${property.name}`}>
      <span>{property.displayName}</span>
    </div>
  ),
}));

const createMockPropertyEditor = () => ({
  form: {
    control: {},
    handleSubmit: vi.fn(),
    register: vi.fn(),
    setValue: vi.fn(),
    getValues: vi.fn(),
    watch: vi.fn(),
    reset: vi.fn(),
    formState: { errors: {}, isDirty: false },
  },
  control: {},
  handleSubmit: vi.fn(),
  handleReset: vi.fn(),
  errors: {},
  isValid: true,
  hasChanges: false,
  watchedValues: {},
  propertiesByCategory: {
    capacity: [
      {
        name: 'capacity',
        displayName: 'Capacity',
        type: 'string' as const,
        defaultValue: '50',
        description: 'Queue capacity allocation',
        category: 'capacity' as const,
        formFieldName: 'capacity',
        required: true,
        validationRules: [],
      },
    ],
    resource: [] as PropertyDescriptor[],
    'application-limits': [] as PropertyDescriptor[],
    'dynamic-queues': [] as PropertyDescriptor[],
    'node-labels': [] as PropertyDescriptor[],
    scheduling: [] as PropertyDescriptor[],
    security: [] as PropertyDescriptor[],
    preemption: [] as PropertyDescriptor[],
  },
  getStagedStatus: vi.fn(),
  formState: { isDirty: false },
  handleFieldBlur: vi.fn(),
  getFieldErrors: vi.fn(() => []),
  getFieldWarnings: vi.fn(() => []),
  getInheritanceInfo: vi.fn(() => null),
  properties: [
    {
      name: 'capacity',
      displayName: 'Capacity',
      type: 'string' as const,
      defaultValue: '50',
      description: 'Queue capacity allocation',
      category: 'capacity' as const,
      formFieldName: 'capacity',
      required: true,
      validationRules: [],
    },
  ] as PropertyDescriptor[],
});

describe('PropertyEditorTab', () => {
  const mockQueue: QueueInfo = {
    queueType: 'leaf',
    queueName: 'test-queue',
    queuePath: 'root.test-queue',
    absoluteCapacity: 50,
    absoluteMaxCapacity: 100,
    absoluteUsedCapacity: 25,
    capacity: 50,
    maxCapacity: 100,
    usedCapacity: 25,
    numApplications: 5,
    numActiveApplications: 3,
    numPendingApplications: 2,
    state: 'RUNNING',
    queues: undefined,
    resourcesUsed: {
      memory: 1024,
      vCores: 2,
    },
  };

  let mockPropertyEditor: ReturnType<typeof createMockPropertyEditor>;

  beforeEach(() => {
    vi.clearAllMocks();
    mockPropertyEditor = createMockPropertyEditor();
    vi.mocked(usePropertyEditor).mockReturnValue(mockPropertyEditor as any);
  });

  it('renders configured property categories with capacity expanded by default', () => {
    render(<PropertyEditorTab queue={mockQueue} />);

    expect(screen.getByText('Capacity Configuration')).toBeInTheDocument();
    // Node Labels category should not be rendered when there are no properties in it
    expect(screen.queryByText('Node Labels & Partitions')).not.toBeInTheDocument();

    // Verify capacity category is expanded by default (property field should be visible)
    expect(screen.getByTestId('property-field-capacity')).toBeInTheDocument();
  });

  it('displays error badge when category has errors', () => {
    vi.mocked(usePropertyEditor).mockReturnValue({
      ...mockPropertyEditor,
      errors: {
        capacity: { message: 'Invalid capacity', type: 'validation' },
      },
    } as any);

    render(<PropertyEditorTab queue={mockQueue} />);

    expect(screen.getByText('Capacity Configuration')).toBeInTheDocument();
    const capacityTrigger = screen.getByRole('button', { name: /Capacity Configuration/i });
    expect(within(capacityTrigger).getByText('1')).toBeInTheDocument();
  });

  describe('pending deletion state', () => {
    it('should not show deletion banner in PropertyEditorTab (banner is in PropertyPanel)', () => {
      vi.mocked(useSchedulerStore).mockImplementation((selector: any) => {
        const state = {
          getGlobalPropertyValue: vi.fn().mockReturnValue({ value: '' }),
          getQueuePropertyValue: vi.fn().mockReturnValue({ value: '' }),
          stagedChanges: [],
          configData: new Map(),
          schedulerData: null,
          hasPendingDeletion: vi.fn().mockReturnValue(true),
          revertQueueDeletion: vi.fn(),
        };
        return selector ? selector(state) : state;
      });

      render(<PropertyEditorTab queue={mockQueue} />);

      expect(screen.queryByText(/marked for deletion/i)).not.toBeInTheDocument();
      expect(screen.queryByRole('button', { name: /undo delete/i })).not.toBeInTheDocument();
    });
  });

  it('renders template configuration button when controls allow management', async () => {
    const templateProperty = {
      name: 'auto-queue-creation-v2.enabled',
      displayName: 'Flexible Auto-Creation',
      type: 'string' as const,
      defaultValue: 'false',
      description: 'Flexible mode toggle',
      category: 'dynamic-queues' as const,
      formFieldName: 'auto-queue-creation-v2__DOT__enabled',
      required: false,
      validationRules: [],
    };

    mockPropertyEditor.propertiesByCategory['dynamic-queues'] = [
      ...mockPropertyEditor.propertiesByCategory['dynamic-queues'],
      templateProperty,
    ];
    mockPropertyEditor.properties = [...mockPropertyEditor.properties, templateProperty];

    const onOpenTemplateConfig = vi.fn();
    const user = userEvent.setup();

    render(
      <PropertyEditorTab
        queue={mockQueue}
        templateConfigControls={{
          canManageTemplates: true,
          legacyAvailable: false,
          flexibleAvailable: true,
          onOpenTemplateConfig,
        }}
      />,
    );

    // First expand the Dynamic Queue Creation accordion
    const dynamicQueuesAccordion = screen.getByRole('button', {
      name: /Dynamic Queue Creation/i,
    });
    await user.click(dynamicQueuesAccordion);

    // Now the button should be visible
    const button = screen.getByRole('button', { name: /Manage template properties/i });
    expect(button).toBeInTheDocument();

    await user.click(button);
    expect(onOpenTemplateConfig).toHaveBeenCalled();
  });
});
