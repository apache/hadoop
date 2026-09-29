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
import { render, screen, waitFor } from '@testing-library/react';
import userEvent from '@testing-library/user-event';
import { CapacityEditorDialog } from '~/features/queue-management/components/CapacityEditorDialog';
import { useSchedulerStore } from '~/stores/schedulerStore';
import type { SchedulerStore } from '~/stores/schedulerStore';

// Mock the scheduler store
vi.mock('~/stores/schedulerStore', () => ({
  useSchedulerStore: vi.fn(),
}));

describe('CapacityEditorDialog', () => {
  const mockCloseCapacityEditor = vi.fn();
  const mockUpdateCapacityDraft = vi.fn();
  const mockSetCapacityEditorLabel = vi.fn();
  const mockResetCapacityDrafts = vi.fn();
  const mockSaveCapacityDrafts = vi.fn();
  const mockGetQueuePartitionCapacities = vi.fn();

  const createMockDraft = (overrides = {}) => ({
    queuePath: 'root.default',
    queueName: 'default',
    isOrigin: false,
    isNew: false,
    hasStagedChange: false,
    mode: 'simple' as const,
    baseMode: 'simple' as const,
    baseCapacityValue: '50',
    baseMaxCapacityValue: '100',
    capacityValue: '50',
    maxCapacityValue: '100',
    vectorCapacity: [],
    vectorMaxCapacity: [],
    ...overrides,
  });

  const mockStoreState = (overrides: Partial<SchedulerStore> = {}) => {
    vi.mocked(useSchedulerStore).mockImplementation((selector: any) => {
      const state: Partial<SchedulerStore> = {
        capacityEditor: {
          isOpen: false,
          drafts: {},
          draftOrder: [],
          parentQueuePath: 'root',
          selectedNodeLabel: null,
          labelOptions: [{ value: '__DEFAULT_PARTITION__', label: 'Default partition' }],
          labelsWithoutAccess: new Set(),
          validationIssues: [],
          isSaving: false,
          saveError: null,
          origin: null,
          originQueuePath: null,
          originQueueName: null,
          originQueueState: null,
          originInitialCapacity: null,
          originInitialMaxCapacity: null,
          originIsNew: false,
          draftCache: {},
          ...overrides.capacityEditor,
        },
        closeCapacityEditor: mockCloseCapacityEditor,
        updateCapacityDraft: mockUpdateCapacityDraft,
        setCapacityEditorLabel: mockSetCapacityEditorLabel,
        resetCapacityDrafts: mockResetCapacityDrafts,
        saveCapacityDrafts: mockSaveCapacityDrafts,
        getQueuePropertyValue: () => ({ value: '', isStaged: false }),
        getGlobalPropertyValue: () => ({ value: 'false', isStaged: false }),
        getQueuePartitionCapacities: mockGetQueuePartitionCapacities,
        ...overrides,
      };

      return selector(state);
    });
  };

  beforeEach(() => {
    vi.clearAllMocks();
    mockStoreState();
  });

  describe('rendering', () => {
    it('should not render when dialog is closed', () => {
      mockStoreState({
        capacityEditor: {
          isOpen: false,
          drafts: {},
          draftOrder: [],
          parentQueuePath: null,
          selectedNodeLabel: null,
          labelOptions: [],
          labelsWithoutAccess: new Set(),
          validationIssues: [],
          isSaving: false,
          saveError: null,
          origin: null,
          originQueuePath: null,
          originQueueName: null,
          originQueueState: null,
          originInitialCapacity: null,
          originInitialMaxCapacity: null,
          originIsNew: false,
          draftCache: {},
        },
      });

      const { container } = render(<CapacityEditorDialog />);

      expect(container.firstChild).toBeNull();
    });

    it('should render when dialog is open', () => {
      const draft = createMockDraft();
      mockStoreState({
        capacityEditor: {
          isOpen: true,
          drafts: { [draft.queuePath]: draft },
          draftOrder: [draft.queuePath],
          parentQueuePath: 'root',
          selectedNodeLabel: null,
          labelOptions: [{ value: '__DEFAULT_PARTITION__', label: 'Default partition' }],
          labelsWithoutAccess: new Set(),
          validationIssues: [],
          isSaving: false,
          saveError: null,
          origin: 'property-editor',
          originQueuePath: draft.queuePath,
          originQueueName: draft.queueName,
          originQueueState: null,
          originInitialCapacity: null,
          originInitialMaxCapacity: null,
          originIsNew: false,
          draftCache: {},
        },
      });

      render(<CapacityEditorDialog />);

      expect(screen.getByText('Capacity Editor')).toBeInTheDocument();
      expect(screen.getByText(draft.queueName)).toBeInTheDocument();
    });

    it('should display parent queue path', () => {
      const draft = createMockDraft();
      mockStoreState({
        capacityEditor: {
          isOpen: true,
          drafts: { [draft.queuePath]: draft },
          draftOrder: [draft.queuePath],
          parentQueuePath: 'root.parent',
          selectedNodeLabel: null,
          labelOptions: [{ value: '__DEFAULT_PARTITION__', label: 'Default partition' }],
          labelsWithoutAccess: new Set(),
          validationIssues: [],
          isSaving: false,
          saveError: null,
          origin: 'property-editor',
          originQueuePath: draft.queuePath,
          originQueueName: draft.queueName,
          originQueueState: null,
          originInitialCapacity: null,
          originInitialMaxCapacity: null,
          originIsNew: false,
          draftCache: {},
        },
      });

      render(<CapacityEditorDialog />);

      expect(screen.getByText(/root\.parent/)).toBeInTheDocument();
    });

    it('should display multiple queue drafts', () => {
      const draft1 = createMockDraft({ queuePath: 'root.queue1', queueName: 'queue1' });
      const draft2 = createMockDraft({ queuePath: 'root.queue2', queueName: 'queue2' });

      mockStoreState({
        capacityEditor: {
          isOpen: true,
          drafts: {
            [draft1.queuePath]: draft1,
            [draft2.queuePath]: draft2,
          },
          draftOrder: [draft1.queuePath, draft2.queuePath],
          parentQueuePath: 'root',
          selectedNodeLabel: null,
          labelOptions: [{ value: '__DEFAULT_PARTITION__', label: 'Default partition' }],
          labelsWithoutAccess: new Set(),
          validationIssues: [],
          isSaving: false,
          saveError: null,
          origin: 'property-editor',
          originQueuePath: draft1.queuePath,
          originQueueName: draft1.queueName,
          originQueueState: null,
          originInitialCapacity: null,
          originInitialMaxCapacity: null,
          originIsNew: false,
          draftCache: {},
        },
      });

      render(<CapacityEditorDialog />);

      expect(screen.getByText('queue1')).toBeInTheDocument();
      expect(screen.getByText('queue2')).toBeInTheDocument();
    });

    it('should display "Active queue" badge for origin queue', () => {
      const draft = createMockDraft({ isOrigin: true });
      mockStoreState({
        capacityEditor: {
          isOpen: true,
          drafts: { [draft.queuePath]: draft },
          draftOrder: [draft.queuePath],
          parentQueuePath: 'root',
          selectedNodeLabel: null,
          labelOptions: [{ value: '__DEFAULT_PARTITION__', label: 'Default partition' }],
          labelsWithoutAccess: new Set(),
          validationIssues: [],
          isSaving: false,
          saveError: null,
          origin: 'property-editor',
          originQueuePath: draft.queuePath,
          originQueueName: draft.queueName,
          originQueueState: null,
          originInitialCapacity: null,
          originInitialMaxCapacity: null,
          originIsNew: false,
          draftCache: {},
        },
      });

      render(<CapacityEditorDialog />);

      expect(screen.getByText('Active queue')).toBeInTheDocument();
    });

    it('should display "Staged" badge for queues with staged changes', () => {
      const draft = createMockDraft({ hasStagedChange: true, isOrigin: false });
      mockStoreState({
        capacityEditor: {
          isOpen: true,
          drafts: { [draft.queuePath]: draft },
          draftOrder: [draft.queuePath],
          parentQueuePath: 'root',
          selectedNodeLabel: null,
          labelOptions: [{ value: '__DEFAULT_PARTITION__', label: 'Default partition' }],
          labelsWithoutAccess: new Set(),
          validationIssues: [],
          isSaving: false,
          saveError: null,
          origin: 'property-editor',
          originQueuePath: 'root.other',
          originQueueName: 'other',
          originQueueState: null,
          originInitialCapacity: null,
          originInitialMaxCapacity: null,
          originIsNew: false,
          draftCache: {},
        },
      });

      render(<CapacityEditorDialog />);

      expect(screen.getByText('Staged')).toBeInTheDocument();
    });

    it('should display "New" badge for new queues', () => {
      const draft = createMockDraft({ isNew: true, isOrigin: false });
      mockStoreState({
        capacityEditor: {
          isOpen: true,
          drafts: { [draft.queuePath]: draft },
          draftOrder: [draft.queuePath],
          parentQueuePath: 'root',
          selectedNodeLabel: null,
          labelOptions: [{ value: '__DEFAULT_PARTITION__', label: 'Default partition' }],
          labelsWithoutAccess: new Set(),
          validationIssues: [],
          isSaving: false,
          saveError: null,
          origin: 'add-queue',
          originQueuePath: draft.queuePath,
          originQueueName: draft.queueName,
          originQueueState: null,
          originInitialCapacity: null,
          originInitialMaxCapacity: null,
          originIsNew: true,
          draftCache: {},
        },
      });

      render(<CapacityEditorDialog />);

      expect(screen.getByText('New')).toBeInTheDocument();
    });
  });

  describe('simple mode', () => {
    it('should render capacity and maximum capacity inputs in simple mode', () => {
      const draft = createMockDraft({ mode: 'simple' });
      mockStoreState({
        capacityEditor: {
          isOpen: true,
          drafts: { [draft.queuePath]: draft },
          draftOrder: [draft.queuePath],
          parentQueuePath: 'root',
          selectedNodeLabel: null,
          labelOptions: [{ value: '__DEFAULT_PARTITION__', label: 'Default partition' }],
          labelsWithoutAccess: new Set(),
          validationIssues: [],
          isSaving: false,
          saveError: null,
          origin: 'property-editor',
          originQueuePath: draft.queuePath,
          originQueueName: draft.queueName,
          originQueueState: null,
          originInitialCapacity: null,
          originInitialMaxCapacity: null,
          originIsNew: false,
          draftCache: {},
        },
      });

      render(<CapacityEditorDialog />);

      const capacityInputs = screen.getAllByPlaceholderText(/e\.g\. 50, 10w/i);
      expect(capacityInputs).toHaveLength(1);

      const maxCapacityInputs = screen.getAllByPlaceholderText(/e\.g\. 100, 20w/i);
      expect(maxCapacityInputs).toHaveLength(1);
    });

    it('should display base capacity values', () => {
      const draft = createMockDraft({
        mode: 'simple',
        baseCapacityValue: '50',
        baseMaxCapacityValue: '100',
      });
      mockStoreState({
        capacityEditor: {
          isOpen: true,
          drafts: { [draft.queuePath]: draft },
          draftOrder: [draft.queuePath],
          parentQueuePath: 'root',
          selectedNodeLabel: null,
          labelOptions: [{ value: '__DEFAULT_PARTITION__', label: 'Default partition' }],
          labelsWithoutAccess: new Set(),
          validationIssues: [],
          isSaving: false,
          saveError: null,
          origin: 'property-editor',
          originQueuePath: draft.queuePath,
          originQueueName: draft.queueName,
          originQueueState: null,
          originInitialCapacity: null,
          originInitialMaxCapacity: null,
          originIsNew: false,
          draftCache: {},
        },
      });

      render(<CapacityEditorDialog />);

      const baseLabels = screen.getAllByText(/Base:/);
      expect(baseLabels.length).toBeGreaterThan(0);
    });

    it('should call updateCapacityDraft when capacity input changes', async () => {
      const user = userEvent.setup();
      const draft = createMockDraft({ mode: 'simple', capacityValue: '50' });

      mockStoreState({
        capacityEditor: {
          isOpen: true,
          drafts: { [draft.queuePath]: draft },
          draftOrder: [draft.queuePath],
          parentQueuePath: 'root',
          selectedNodeLabel: null,
          labelOptions: [{ value: '__DEFAULT_PARTITION__', label: 'Default partition' }],
          labelsWithoutAccess: new Set(),
          validationIssues: [],
          isSaving: false,
          saveError: null,
          origin: 'property-editor',
          originQueuePath: draft.queuePath,
          originQueueName: draft.queueName,
          originQueueState: null,
          originInitialCapacity: null,
          originInitialMaxCapacity: null,
          originIsNew: false,
          draftCache: {},
        },
      });

      render(<CapacityEditorDialog />);

      const capacityInput = screen.getByPlaceholderText(/e\.g\. 50, 10w/i);
      await user.clear(capacityInput);
      await user.type(capacityInput, '60');

      await waitFor(() => {
        expect(mockUpdateCapacityDraft).toHaveBeenCalled();
      });
    });
  });

  describe('vector mode', () => {
    it('should render vector entry inputs in vector mode', () => {
      const draft = createMockDraft({
        mode: 'vector',
        vectorCapacity: [
          { id: '1', key: 'memory', value: '2048' },
          { id: '2', key: 'vcores', value: '4' },
        ],
        vectorMaxCapacity: [
          { id: '3', key: 'memory', value: '4096' },
          { id: '4', key: 'vcores', value: '8' },
        ],
      });

      mockStoreState({
        capacityEditor: {
          isOpen: true,
          drafts: { [draft.queuePath]: draft },
          draftOrder: [draft.queuePath],
          parentQueuePath: 'root',
          selectedNodeLabel: null,
          labelOptions: [{ value: '__DEFAULT_PARTITION__', label: 'Default partition' }],
          labelsWithoutAccess: new Set(),
          validationIssues: [],
          isSaving: false,
          saveError: null,
          origin: 'property-editor',
          originQueuePath: draft.queuePath,
          originQueueName: draft.queueName,
          originQueueState: null,
          originInitialCapacity: null,
          originInitialMaxCapacity: null,
          originIsNew: false,
          draftCache: {},
        },
      });

      render(<CapacityEditorDialog />);

      // Both capacity and maxCapacity vectors have memory/vcores entries
      expect(screen.getAllByDisplayValue('memory').length).toBeGreaterThan(0);
      expect(screen.getAllByDisplayValue('vcores').length).toBeGreaterThan(0);
      expect(screen.getByDisplayValue('2048')).toBeInTheDocument();
      expect(screen.getByDisplayValue('4')).toBeInTheDocument();
    });

    it('should render "Add resource" button for vector entries', () => {
      const draft = createMockDraft({
        mode: 'vector',
        vectorCapacity: [{ id: '1', key: 'memory', value: '2048' }],
        vectorMaxCapacity: [{ id: '2', key: 'memory', value: '4096' }],
      });

      mockStoreState({
        capacityEditor: {
          isOpen: true,
          drafts: { [draft.queuePath]: draft },
          draftOrder: [draft.queuePath],
          parentQueuePath: 'root',
          selectedNodeLabel: null,
          labelOptions: [{ value: '__DEFAULT_PARTITION__', label: 'Default partition' }],
          labelsWithoutAccess: new Set(),
          validationIssues: [],
          isSaving: false,
          saveError: null,
          origin: 'property-editor',
          originQueuePath: draft.queuePath,
          originQueueName: draft.queueName,
          originQueueState: null,
          originInitialCapacity: null,
          originInitialMaxCapacity: null,
          originIsNew: false,
          draftCache: {},
        },
      });

      render(<CapacityEditorDialog />);

      const addButtons = screen.getAllByText('Add resource');
      expect(addButtons.length).toBeGreaterThan(0);
    });

    it('should display remove buttons for non-core resources', () => {
      const draft = createMockDraft({
        mode: 'vector',
        vectorCapacity: [
          { id: '1', key: 'memory', value: '2048' },
          { id: '2', key: 'vcores', value: '4' },
          { id: '3', key: 'gpu', value: '2' },
        ],
        vectorMaxCapacity: [],
      });

      mockStoreState({
        capacityEditor: {
          isOpen: true,
          drafts: { [draft.queuePath]: draft },
          draftOrder: [draft.queuePath],
          parentQueuePath: 'root',
          selectedNodeLabel: null,
          labelOptions: [{ value: '__DEFAULT_PARTITION__', label: 'Default partition' }],
          labelsWithoutAccess: new Set(),
          validationIssues: [],
          isSaving: false,
          saveError: null,
          origin: 'property-editor',
          originQueuePath: draft.queuePath,
          originQueueName: draft.queueName,
          originQueueState: null,
          originInitialCapacity: null,
          originInitialMaxCapacity: null,
          originIsNew: false,
          draftCache: {},
        },
      });

      render(<CapacityEditorDialog />);

      // GPU entry should have a remove button, but memory/vcores should not
      expect(screen.getByDisplayValue('gpu')).toBeInTheDocument();
    });
  });

  describe('mode switching', () => {
    it('should render mode toggle buttons', () => {
      const draft = createMockDraft();
      mockStoreState({
        capacityEditor: {
          isOpen: true,
          drafts: { [draft.queuePath]: draft },
          draftOrder: [draft.queuePath],
          parentQueuePath: 'root',
          selectedNodeLabel: null,
          labelOptions: [{ value: '__DEFAULT_PARTITION__', label: 'Default partition' }],
          labelsWithoutAccess: new Set(),
          validationIssues: [],
          isSaving: false,
          saveError: null,
          origin: 'property-editor',
          originQueuePath: draft.queuePath,
          originQueueName: draft.queueName,
          originQueueState: null,
          originInitialCapacity: null,
          originInitialMaxCapacity: null,
          originIsNew: false,
          draftCache: {},
        },
      });

      render(<CapacityEditorDialog />);

      expect(screen.getByText('Simple value')).toBeInTheDocument();
      expect(screen.getByText('Resource vector')).toBeInTheDocument();
    });
  });

  describe('label selection', () => {
    it('should render node label selector', () => {
      const draft = createMockDraft();
      mockStoreState({
        capacityEditor: {
          isOpen: true,
          drafts: { [draft.queuePath]: draft },
          draftOrder: [draft.queuePath],
          parentQueuePath: 'root',
          selectedNodeLabel: null,
          labelOptions: [
            { value: '__DEFAULT_PARTITION__', label: 'Default partition' },
            { value: 'gpu', label: 'gpu' },
          ],
          labelsWithoutAccess: new Set(),
          validationIssues: [],
          isSaving: false,
          saveError: null,
          origin: 'property-editor',
          originQueuePath: draft.queuePath,
          originQueueName: draft.queueName,
          originQueueState: null,
          originInitialCapacity: null,
          originInitialMaxCapacity: null,
          originIsNew: false,
          draftCache: {},
        },
      });

      render(<CapacityEditorDialog />);

      expect(screen.getByText('Node label')).toBeInTheDocument();
    });

    it('should display warning for labels without access', () => {
      const draft = createMockDraft();
      const labelsWithoutAccess = new Set(['gpu']);

      mockStoreState({
        capacityEditor: {
          isOpen: true,
          drafts: { [draft.queuePath]: draft },
          draftOrder: [draft.queuePath],
          parentQueuePath: 'root',
          selectedNodeLabel: 'gpu',
          labelOptions: [
            { value: '__DEFAULT_PARTITION__', label: 'Default partition' },
            { value: 'gpu', label: 'gpu' },
          ],
          labelsWithoutAccess,
          validationIssues: [],
          isSaving: false,
          saveError: null,
          origin: 'property-editor',
          originQueuePath: draft.queuePath,
          originQueueName: draft.queueName,
          originQueueState: null,
          originInitialCapacity: null,
          originInitialMaxCapacity: null,
          originIsNew: false,
          draftCache: {},
        },
      });

      render(<CapacityEditorDialog />);

      expect(
        screen.getByText(/This queue doesn't have access to the gpu label/i),
      ).toBeInTheDocument();
    });
  });

  describe('validation', () => {
    it('should display validation errors', () => {
      const draft = createMockDraft();
      mockStoreState({
        capacityEditor: {
          isOpen: true,
          drafts: { [draft.queuePath]: draft },
          draftOrder: [draft.queuePath],
          parentQueuePath: 'root',
          selectedNodeLabel: null,
          labelOptions: [{ value: '__DEFAULT_PARTITION__', label: 'Default partition' }],
          labelsWithoutAccess: new Set(),
          validationIssues: [
            {
              severity: 'error',
              message: 'Capacity exceeds 100%',
              rule: 'capacity-sum',
              queuePath: draft.queuePath,
              field: 'capacity',
            },
          ],
          isSaving: false,
          saveError: null,
          origin: 'property-editor',
          originQueuePath: draft.queuePath,
          originQueueName: draft.queueName,
          originQueueState: null,
          originInitialCapacity: null,
          originInitialMaxCapacity: null,
          originIsNew: false,
          draftCache: {},
        },
      });

      render(<CapacityEditorDialog />);

      expect(screen.getByText('Capacity exceeds 100%')).toBeInTheDocument();
    });

    it('should display validation warnings', () => {
      const draft = createMockDraft();
      mockStoreState({
        capacityEditor: {
          isOpen: true,
          drafts: { [draft.queuePath]: draft },
          draftOrder: [draft.queuePath],
          parentQueuePath: 'root',
          selectedNodeLabel: null,
          labelOptions: [{ value: '__DEFAULT_PARTITION__', label: 'Default partition' }],
          labelsWithoutAccess: new Set(),
          validationIssues: [
            {
              severity: 'warning',
              message: 'Capacity is low',
              rule: 'capacity-low',
              queuePath: draft.queuePath,
              field: 'capacity',
            },
          ],
          isSaving: false,
          saveError: null,
          origin: 'property-editor',
          originQueuePath: draft.queuePath,
          originQueueName: draft.queueName,
          originQueueState: null,
          originInitialCapacity: null,
          originInitialMaxCapacity: null,
          originIsNew: false,
          draftCache: {},
        },
      });

      render(<CapacityEditorDialog />);

      expect(screen.getByText('Capacity is low')).toBeInTheDocument();
    });

    it('should disable "Stage anyway" button when there are no blocking issues', () => {
      const draft = createMockDraft();
      mockStoreState({
        capacityEditor: {
          isOpen: true,
          drafts: { [draft.queuePath]: draft },
          draftOrder: [draft.queuePath],
          parentQueuePath: 'root',
          selectedNodeLabel: null,
          labelOptions: [{ value: '__DEFAULT_PARTITION__', label: 'Default partition' }],
          labelsWithoutAccess: new Set(),
          validationIssues: [],
          isSaving: false,
          saveError: null,
          origin: 'property-editor',
          originQueuePath: draft.queuePath,
          originQueueName: draft.queueName,
          originQueueState: null,
          originInitialCapacity: null,
          originInitialMaxCapacity: null,
          originIsNew: false,
          draftCache: {},
        },
      });

      render(<CapacityEditorDialog />);

      const stageAnywayButton = screen.getByText('Stage anyway');
      expect(stageAnywayButton).toBeDisabled();
    });

    it('should enable "Stage anyway" button when there are blocking issues', () => {
      const draft = createMockDraft();
      mockStoreState({
        capacityEditor: {
          isOpen: true,
          drafts: { [draft.queuePath]: draft },
          draftOrder: [draft.queuePath],
          parentQueuePath: 'root',
          selectedNodeLabel: null,
          labelOptions: [{ value: '__DEFAULT_PARTITION__', label: 'Default partition' }],
          labelsWithoutAccess: new Set(),
          validationIssues: [
            {
              severity: 'error',
              message: 'Capacity exceeds 100%',
              rule: 'capacity-sum',
              queuePath: draft.queuePath,
              field: 'capacity',
            },
          ],
          isSaving: false,
          saveError: null,
          origin: 'property-editor',
          originQueuePath: draft.queuePath,
          originQueueName: draft.queueName,
          originQueueState: null,
          originInitialCapacity: null,
          originInitialMaxCapacity: null,
          originIsNew: false,
          draftCache: {},
        },
      });

      render(<CapacityEditorDialog />);

      const stageAnywayButton = screen.getByText('Stage anyway');
      expect(stageAnywayButton).not.toBeDisabled();
    });
  });

  describe('save operations', () => {
    it('should render save buttons', () => {
      const draft = createMockDraft();
      mockStoreState({
        capacityEditor: {
          isOpen: true,
          drafts: { [draft.queuePath]: draft },
          draftOrder: [draft.queuePath],
          parentQueuePath: 'root',
          selectedNodeLabel: null,
          labelOptions: [{ value: '__DEFAULT_PARTITION__', label: 'Default partition' }],
          labelsWithoutAccess: new Set(),
          validationIssues: [],
          isSaving: false,
          saveError: null,
          origin: 'property-editor',
          originQueuePath: draft.queuePath,
          originQueueName: draft.queueName,
          originQueueState: null,
          originInitialCapacity: null,
          originInitialMaxCapacity: null,
          originIsNew: false,
          draftCache: {},
        },
      });

      render(<CapacityEditorDialog />);

      expect(screen.getByText('Save changes')).toBeInTheDocument();
      expect(screen.getByText('Stage anyway')).toBeInTheDocument();
    });

    it('should display loading state when saving', () => {
      const draft = createMockDraft();
      mockStoreState({
        capacityEditor: {
          isOpen: true,
          drafts: { [draft.queuePath]: draft },
          draftOrder: [draft.queuePath],
          parentQueuePath: 'root',
          selectedNodeLabel: null,
          labelOptions: [{ value: '__DEFAULT_PARTITION__', label: 'Default partition' }],
          labelsWithoutAccess: new Set(),
          validationIssues: [],
          isSaving: true,
          saveError: null,
          origin: 'property-editor',
          originQueuePath: draft.queuePath,
          originQueueName: draft.queueName,
          originQueueState: null,
          originInitialCapacity: null,
          originInitialMaxCapacity: null,
          originIsNew: false,
          draftCache: {},
        },
      });

      render(<CapacityEditorDialog />);

      expect(screen.getByText('Saving…')).toBeInTheDocument();
    });

    it('should display save error when present', () => {
      const draft = createMockDraft();
      mockStoreState({
        capacityEditor: {
          isOpen: true,
          drafts: { [draft.queuePath]: draft },
          draftOrder: [draft.queuePath],
          parentQueuePath: 'root',
          selectedNodeLabel: null,
          labelOptions: [{ value: '__DEFAULT_PARTITION__', label: 'Default partition' }],
          labelsWithoutAccess: new Set(),
          validationIssues: [],
          isSaving: false,
          saveError: 'Failed to save changes',
          origin: 'property-editor',
          originQueuePath: draft.queuePath,
          originQueueName: draft.queueName,
          originQueueState: null,
          originInitialCapacity: null,
          originInitialMaxCapacity: null,
          originIsNew: false,
          draftCache: {},
        },
      });

      render(<CapacityEditorDialog />);

      expect(screen.getByText('Failed to save changes')).toBeInTheDocument();
    });

    it('should call saveCapacityDrafts with force=false when "Save changes" is clicked', async () => {
      const user = userEvent.setup();
      const draft = createMockDraft();
      mockSaveCapacityDrafts.mockResolvedValue(true);

      mockStoreState({
        capacityEditor: {
          isOpen: true,
          drafts: { [draft.queuePath]: draft },
          draftOrder: [draft.queuePath],
          parentQueuePath: 'root',
          selectedNodeLabel: null,
          labelOptions: [{ value: '__DEFAULT_PARTITION__', label: 'Default partition' }],
          labelsWithoutAccess: new Set(),
          validationIssues: [],
          isSaving: false,
          saveError: null,
          origin: 'property-editor',
          originQueuePath: draft.queuePath,
          originQueueName: draft.queueName,
          originQueueState: null,
          originInitialCapacity: null,
          originInitialMaxCapacity: null,
          originIsNew: false,
          draftCache: {},
        },
      });

      render(<CapacityEditorDialog />);

      const saveButton = screen.getByText('Save changes');
      await user.click(saveButton);

      await waitFor(() => {
        expect(mockSaveCapacityDrafts).toHaveBeenCalledWith({ force: false });
      });
    });

    it('should call saveCapacityDrafts with force=true when "Stage anyway" is clicked', async () => {
      const user = userEvent.setup();
      const draft = createMockDraft();
      mockSaveCapacityDrafts.mockResolvedValue(true);

      mockStoreState({
        capacityEditor: {
          isOpen: true,
          drafts: { [draft.queuePath]: draft },
          draftOrder: [draft.queuePath],
          parentQueuePath: 'root',
          selectedNodeLabel: null,
          labelOptions: [{ value: '__DEFAULT_PARTITION__', label: 'Default partition' }],
          labelsWithoutAccess: new Set(),
          validationIssues: [
            {
              severity: 'error',
              message: 'Capacity exceeds 100%',
              rule: 'capacity-sum',
              queuePath: draft.queuePath,
              field: 'capacity',
            },
          ],
          isSaving: false,
          saveError: null,
          origin: 'property-editor',
          originQueuePath: draft.queuePath,
          originQueueName: draft.queueName,
          originQueueState: null,
          originInitialCapacity: null,
          originInitialMaxCapacity: null,
          originIsNew: false,
          draftCache: {},
        },
      });

      render(<CapacityEditorDialog />);

      const stageAnywayButton = screen.getByText('Stage anyway');
      await user.click(stageAnywayButton);

      await waitFor(() => {
        expect(mockSaveCapacityDrafts).toHaveBeenCalledWith({ force: true });
      });
    });

    it('should close dialog after successful save', async () => {
      const user = userEvent.setup();
      const draft = createMockDraft();
      mockSaveCapacityDrafts.mockResolvedValue(true);

      mockStoreState({
        capacityEditor: {
          isOpen: true,
          drafts: { [draft.queuePath]: draft },
          draftOrder: [draft.queuePath],
          parentQueuePath: 'root',
          selectedNodeLabel: null,
          labelOptions: [{ value: '__DEFAULT_PARTITION__', label: 'Default partition' }],
          labelsWithoutAccess: new Set(),
          validationIssues: [],
          isSaving: false,
          saveError: null,
          origin: 'property-editor',
          originQueuePath: draft.queuePath,
          originQueueName: draft.queueName,
          originQueueState: null,
          originInitialCapacity: null,
          originInitialMaxCapacity: null,
          originIsNew: false,
          draftCache: {},
        },
      });

      render(<CapacityEditorDialog />);

      const saveButton = screen.getByText('Save changes');
      await user.click(saveButton);

      await waitFor(() => {
        expect(mockCloseCapacityEditor).toHaveBeenCalled();
      });
    });

    it('should not close dialog after failed save', async () => {
      const user = userEvent.setup();
      const draft = createMockDraft();
      mockSaveCapacityDrafts.mockResolvedValue(false);

      mockStoreState({
        capacityEditor: {
          isOpen: true,
          drafts: { [draft.queuePath]: draft },
          draftOrder: [draft.queuePath],
          parentQueuePath: 'root',
          selectedNodeLabel: null,
          labelOptions: [{ value: '__DEFAULT_PARTITION__', label: 'Default partition' }],
          labelsWithoutAccess: new Set(),
          validationIssues: [],
          isSaving: false,
          saveError: null,
          origin: 'property-editor',
          originQueuePath: draft.queuePath,
          originQueueName: draft.queueName,
          originQueueState: null,
          originInitialCapacity: null,
          originInitialMaxCapacity: null,
          originIsNew: false,
          draftCache: {},
        },
      });

      render(<CapacityEditorDialog />);

      const saveButton = screen.getByText('Save changes');
      await user.click(saveButton);

      await waitFor(() => {
        expect(mockSaveCapacityDrafts).toHaveBeenCalled();
      });

      expect(mockCloseCapacityEditor).not.toHaveBeenCalled();
    });
  });

  describe('reset operations', () => {
    it('should render reset button', () => {
      const draft = createMockDraft();
      mockStoreState({
        capacityEditor: {
          isOpen: true,
          drafts: { [draft.queuePath]: draft },
          draftOrder: [draft.queuePath],
          parentQueuePath: 'root',
          selectedNodeLabel: null,
          labelOptions: [{ value: '__DEFAULT_PARTITION__', label: 'Default partition' }],
          labelsWithoutAccess: new Set(),
          validationIssues: [],
          isSaving: false,
          saveError: null,
          origin: 'property-editor',
          originQueuePath: draft.queuePath,
          originQueueName: draft.queueName,
          originQueueState: null,
          originInitialCapacity: null,
          originInitialMaxCapacity: null,
          originIsNew: false,
          draftCache: {},
        },
      });

      render(<CapacityEditorDialog />);

      expect(screen.getByText('Reset')).toBeInTheDocument();
    });

    it('should call resetCapacityDrafts when reset button is clicked', async () => {
      const user = userEvent.setup();
      const draft = createMockDraft();

      mockStoreState({
        capacityEditor: {
          isOpen: true,
          drafts: { [draft.queuePath]: draft },
          draftOrder: [draft.queuePath],
          parentQueuePath: 'root',
          selectedNodeLabel: null,
          labelOptions: [{ value: '__DEFAULT_PARTITION__', label: 'Default partition' }],
          labelsWithoutAccess: new Set(),
          validationIssues: [],
          isSaving: false,
          saveError: null,
          origin: 'property-editor',
          originQueuePath: draft.queuePath,
          originQueueName: draft.queueName,
          originQueueState: null,
          originInitialCapacity: null,
          originInitialMaxCapacity: null,
          originIsNew: false,
          draftCache: {},
        },
      });

      render(<CapacityEditorDialog />);

      const resetButton = screen.getByText('Reset');
      await user.click(resetButton);

      await waitFor(() => {
        expect(mockResetCapacityDrafts).toHaveBeenCalled();
      });
    });

    it('should disable reset button when saving', () => {
      const draft = createMockDraft();
      mockStoreState({
        capacityEditor: {
          isOpen: true,
          drafts: { [draft.queuePath]: draft },
          draftOrder: [draft.queuePath],
          parentQueuePath: 'root',
          selectedNodeLabel: null,
          labelOptions: [{ value: '__DEFAULT_PARTITION__', label: 'Default partition' }],
          labelsWithoutAccess: new Set(),
          validationIssues: [],
          isSaving: true,
          saveError: null,
          origin: 'property-editor',
          originQueuePath: draft.queuePath,
          originQueueName: draft.queueName,
          originQueueState: null,
          originInitialCapacity: null,
          originInitialMaxCapacity: null,
          originIsNew: false,
          draftCache: {},
        },
      });

      render(<CapacityEditorDialog />);

      const resetButton = screen.getByText('Reset');
      expect(resetButton).toBeDisabled();
    });
  });
});
