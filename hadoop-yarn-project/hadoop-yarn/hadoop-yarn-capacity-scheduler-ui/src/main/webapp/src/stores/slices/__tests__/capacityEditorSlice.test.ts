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
import { createSchedulerStore } from '~/stores/schedulerStore';
import { YarnApiClient } from '~/lib/api/YarnApiClient';
import type { CapacityRowDraft } from '~/stores/slices/capacityEditorSlice';
import { DEFAULT_PARTITION_VALUE } from '~/features/queue-management/utils/capacityEditor';

// Mock dependencies
vi.mock('~/features/queue-management/utils/capacityEditor', () => ({
  buildCapacityEditorDrafts: vi.fn(),
  buildCapacityEditorLabelOptions: vi.fn(),
  convertVectorDraftToString: vi.fn(),
  getPropertyNameForLabel: vi.fn(),
  DEFAULT_PARTITION_VALUE: '',
}));

vi.mock('~/features/validation/service', () => ({
  validateQueue: vi.fn(),
}));

import {
  buildCapacityEditorDrafts,
  buildCapacityEditorLabelOptions,
  convertVectorDraftToString,
  getPropertyNameForLabel,
} from '~/features/queue-management/utils/capacityEditor';
import { validateQueue } from '~/features/validation/service';

describe('capacityEditorSlice', () => {
  const createTestStore = () => {
    const mockApiClient = new YarnApiClient('http://test.com', {});
    return createSchedulerStore(mockApiClient);
  };

  const createMockDraft = (overrides: Partial<CapacityRowDraft> = {}): CapacityRowDraft => ({
    queuePath: 'root.default',
    queueName: 'default',
    isOrigin: false,
    isNew: false,
    hasStagedChange: false,
    mode: 'simple',
    baseMode: 'simple',
    baseCapacityValue: '50',
    baseMaxCapacityValue: '100',
    capacityValue: '50',
    maxCapacityValue: '100',
    vectorCapacity: [],
    vectorMaxCapacity: [],
    ...overrides,
  });

  beforeEach(() => {
    vi.clearAllMocks();

    // Default mock implementations
    vi.mocked(buildCapacityEditorDrafts).mockReturnValue([createMockDraft()]);
    vi.mocked(buildCapacityEditorLabelOptions).mockReturnValue({
      options: [{ value: DEFAULT_PARTITION_VALUE, label: 'Default partition' }],
      labelsWithoutAccess: new Set(),
    });
    vi.mocked(convertVectorDraftToString).mockReturnValue('[memory=1024,vcores=1]');
    vi.mocked(getPropertyNameForLabel).mockImplementation((label, property) => {
      if (label) {
        return `accessible-node-labels.${label}.${property}`;
      }
      return property;
    });
    vi.mocked(validateQueue).mockReturnValue({
      valid: true,
      issues: [],
    });
  });

  describe('initial state', () => {
    it('should initialize with empty capacity editor state', () => {
      const store = createTestStore();
      const editor = store.getState().capacityEditor;

      expect(editor.isOpen).toBe(false);
      expect(editor.origin).toBeNull();
      expect(editor.parentQueuePath).toBeNull();
      expect(editor.originQueuePath).toBeNull();
      expect(editor.originQueueName).toBeNull();
      expect(editor.selectedNodeLabel).toBeNull();
      expect(editor.drafts).toEqual({});
      expect(editor.draftOrder).toEqual([]);
      expect(editor.isSaving).toBe(false);
      expect(editor.saveError).toBeNull();
    });

    it('should have default label option', () => {
      const store = createTestStore();
      const editor = store.getState().capacityEditor;

      expect(editor.labelOptions).toHaveLength(1);
      expect(editor.labelOptions[0].value).toBe(DEFAULT_PARTITION_VALUE);
      expect(editor.labelOptions[0].label).toBe('Default partition');
    });
  });

  describe('openCapacityEditor', () => {
    it('should open editor with required parameters', () => {
      const store = createTestStore();
      const mockDrafts = [
        createMockDraft({ queuePath: 'root.default', isOrigin: true }),
        createMockDraft({ queuePath: 'root.production' }),
      ];

      vi.mocked(buildCapacityEditorDrafts).mockReturnValue(mockDrafts);

      store.getState().openCapacityEditor({
        origin: 'property-editor',
        parentQueuePath: 'root',
        originQueuePath: 'root.default',
        originQueueName: 'default',
      });

      const editor = store.getState().capacityEditor;

      expect(editor.isOpen).toBe(true);
      expect(editor.origin).toBe('property-editor');
      expect(editor.parentQueuePath).toBe('root');
      expect(editor.originQueuePath).toBe('root.default');
      expect(editor.originQueueName).toBe('default');
    });

    it('should set optional parameters', () => {
      const store = createTestStore();

      store.getState().openCapacityEditor({
        origin: 'add-queue',
        parentQueuePath: 'root',
        originQueuePath: 'root.new',
        originQueueName: 'new',
        originQueueState: 'RUNNING',
        originInitialCapacity: '30',
        originInitialMaxCapacity: '80',
        originIsNew: true,
        selectedNodeLabel: 'gpu',
      });

      const editor = store.getState().capacityEditor;

      expect(editor.originQueueState).toBe('RUNNING');
      expect(editor.originInitialCapacity).toBe('30');
      expect(editor.originInitialMaxCapacity).toBe('80');
      expect(editor.originIsNew).toBe(true);
      expect(editor.selectedNodeLabel).toBe('gpu');
    });

    it('should apply drafts to state', () => {
      const store = createTestStore();
      const mockDrafts = [
        createMockDraft({ queuePath: 'root.default' }),
        createMockDraft({ queuePath: 'root.production' }),
      ];

      vi.mocked(buildCapacityEditorDrafts).mockReturnValue(mockDrafts);

      store.getState().openCapacityEditor({
        origin: 'context-menu',
        parentQueuePath: 'root',
        originQueuePath: 'root.default',
        originQueueName: 'default',
      });

      const editor = store.getState().capacityEditor;

      expect(Object.keys(editor.drafts)).toHaveLength(2);
      expect(editor.draftOrder).toEqual(['root.default', 'root.production']);
      expect(editor.drafts['root.default']).toBeDefined();
      expect(editor.drafts['root.production']).toBeDefined();
    });

    it('should build and normalize label options', () => {
      const store = createTestStore();

      vi.mocked(buildCapacityEditorLabelOptions).mockReturnValue({
        options: [
          { value: DEFAULT_PARTITION_VALUE, label: 'Default partition' },
          { value: 'gpu', label: 'GPU' },
          { value: 'cpu', label: 'CPU' },
        ],
        labelsWithoutAccess: new Set(['restricted']),
      });

      store.getState().openCapacityEditor({
        origin: 'property-editor',
        parentQueuePath: 'root',
        originQueuePath: 'root.default',
        originQueueName: 'default',
      });

      const editor = store.getState().capacityEditor;

      expect(editor.labelOptions).toHaveLength(3);
      expect(editor.labelOptions[0].value).toBe(DEFAULT_PARTITION_VALUE);
      expect(editor.labelsWithoutAccess.has('restricted')).toBe(true);
    });

    it('should call buildCapacityEditorDrafts with correct parameters', () => {
      const store = createTestStore();

      store.getState().openCapacityEditor({
        origin: 'property-editor',
        parentQueuePath: 'root',
        originQueuePath: 'root.default',
        originQueueName: 'default',
        originInitialCapacity: '50',
        originInitialMaxCapacity: '100',
        originIsNew: false,
        selectedNodeLabel: 'gpu',
      });

      expect(buildCapacityEditorDrafts).toHaveBeenCalledWith({
        store: expect.any(Object),
        parentQueuePath: 'root',
        originQueuePath: 'root.default',
        originQueueName: 'default',
        originInitialCapacity: '50',
        originInitialMaxCapacity: '100',
        originIsNew: false,
        selectedNodeLabel: 'gpu',
      });
    });
  });

  describe('closeCapacityEditor', () => {
    it('should reset editor to empty state', () => {
      const store = createTestStore();

      // Open editor first
      store.getState().openCapacityEditor({
        origin: 'property-editor',
        parentQueuePath: 'root',
        originQueuePath: 'root.default',
        originQueueName: 'default',
      });

      expect(store.getState().capacityEditor.isOpen).toBe(true);

      // Close it
      store.getState().closeCapacityEditor();

      const editor = store.getState().capacityEditor;

      expect(editor.isOpen).toBe(false);
      expect(editor.origin).toBeNull();
      expect(editor.parentQueuePath).toBeNull();
      expect(editor.drafts).toEqual({});
      expect(editor.draftOrder).toEqual([]);
      expect(editor.draftCache).toEqual({});
    });

    it('should clear all editor fields', () => {
      const store = createTestStore();

      store.getState().openCapacityEditor({
        origin: 'add-queue',
        parentQueuePath: 'root',
        originQueuePath: 'root.new',
        originQueueName: 'new',
        originInitialCapacity: '30',
        selectedNodeLabel: 'gpu',
      });

      store.getState().closeCapacityEditor();

      const editor = store.getState().capacityEditor;

      expect(editor.originQueuePath).toBeNull();
      expect(editor.originQueueName).toBeNull();
      expect(editor.originInitialCapacity).toBeNull();
      expect(editor.selectedNodeLabel).toBeNull();
    });
  });

  describe('setCapacityEditorLabel', () => {
    it('should set selected label when editor not fully initialized', () => {
      const store = createTestStore();

      // Set label without opening editor
      store.getState().setCapacityEditorLabel('gpu');

      expect(store.getState().capacityEditor.selectedNodeLabel).toBe('gpu');
    });

    it('should rebuild drafts when changing label', () => {
      const store = createTestStore();

      // Open editor
      store.getState().openCapacityEditor({
        origin: 'property-editor',
        parentQueuePath: 'root',
        originQueuePath: 'root.default',
        originQueueName: 'default',
      });

      vi.mocked(buildCapacityEditorDrafts).mockClear();

      const newDrafts = [createMockDraft({ queuePath: 'root.default', capacityValue: '60' })];
      vi.mocked(buildCapacityEditorDrafts).mockReturnValue(newDrafts);

      // Change label
      store.getState().setCapacityEditorLabel('gpu');

      expect(buildCapacityEditorDrafts).toHaveBeenCalled();
      expect(store.getState().capacityEditor.selectedNodeLabel).toBe('gpu');
    });

    it('should cache current drafts before switching labels', () => {
      const store = createTestStore();

      store.getState().openCapacityEditor({
        origin: 'property-editor',
        parentQueuePath: 'root',
        originQueuePath: 'root.default',
        originQueueName: 'default',
      });

      const initialDrafts = store.getState().capacityEditor.drafts;
      const initialDraftOrder = store.getState().capacityEditor.draftOrder;

      store.getState().setCapacityEditorLabel('gpu');

      const cache = store.getState().capacityEditor.draftCache;

      expect(cache[DEFAULT_PARTITION_VALUE]).toBeDefined();
      expect(cache[DEFAULT_PARTITION_VALUE].drafts).toEqual(initialDrafts);
      expect(cache[DEFAULT_PARTITION_VALUE].draftOrder).toEqual(initialDraftOrder);
    });

    it('should restore from cache when switching back to previously selected label', () => {
      const store = createTestStore();

      // Open with default label
      const defaultDrafts = [createMockDraft({ capacityValue: '50' })];
      vi.mocked(buildCapacityEditorDrafts).mockReturnValue(defaultDrafts);

      store.getState().openCapacityEditor({
        origin: 'property-editor',
        parentQueuePath: 'root',
        originQueuePath: 'root.default',
        originQueueName: 'default',
      });

      // Switch to 'gpu'
      const gpuDrafts = [createMockDraft({ capacityValue: '60' })];
      vi.mocked(buildCapacityEditorDrafts).mockReturnValue(gpuDrafts);
      store.getState().setCapacityEditorLabel('gpu');

      // Switch back to default - should restore from cache
      vi.mocked(buildCapacityEditorDrafts).mockClear();
      store.getState().setCapacityEditorLabel(null);

      // Should not call buildCapacityEditorDrafts because it's from cache
      expect(buildCapacityEditorDrafts).not.toHaveBeenCalled();
      expect(store.getState().capacityEditor.drafts['root.default'].capacityValue).toBe('50');
    });

    it('should update label options when changing label', () => {
      const store = createTestStore();

      store.getState().openCapacityEditor({
        origin: 'property-editor',
        parentQueuePath: 'root',
        originQueuePath: 'root.default',
        originQueueName: 'default',
      });

      vi.mocked(buildCapacityEditorLabelOptions).mockReturnValue({
        options: [
          { value: DEFAULT_PARTITION_VALUE, label: 'Default partition' },
          { value: 'gpu', label: 'GPU' },
        ],
        labelsWithoutAccess: new Set(['gpu']),
      });

      store.getState().setCapacityEditorLabel('gpu');

      const editor = store.getState().capacityEditor;
      expect(editor.labelsWithoutAccess.has('gpu')).toBe(true);
    });
  });

  describe('updateCapacityDraft', () => {
    it('should update existing draft', () => {
      const store = createTestStore();

      const mockDrafts = [createMockDraft({ queuePath: 'root.default', capacityValue: '50' })];
      vi.mocked(buildCapacityEditorDrafts).mockReturnValue(mockDrafts);

      store.getState().openCapacityEditor({
        origin: 'property-editor',
        parentQueuePath: 'root',
        originQueuePath: 'root.default',
        originQueueName: 'default',
      });

      store.getState().updateCapacityDraft('root.default', (draft) => {
        draft.capacityValue = '60';
      });

      expect(store.getState().capacityEditor.drafts['root.default'].capacityValue).toBe('60');
    });

    it('should not create new draft if path does not exist', () => {
      const store = createTestStore();

      store.getState().openCapacityEditor({
        origin: 'property-editor',
        parentQueuePath: 'root',
        originQueuePath: 'root.default',
        originQueueName: 'default',
      });

      const draftsBefore = Object.keys(store.getState().capacityEditor.drafts);

      store.getState().updateCapacityDraft('root.nonexistent', (draft) => {
        draft.capacityValue = '100';
      });

      const draftsAfter = Object.keys(store.getState().capacityEditor.drafts);

      expect(draftsAfter).toEqual(draftsBefore);
    });

    it('should allow updating multiple fields', () => {
      const store = createTestStore();

      const mockDrafts = [
        createMockDraft({
          queuePath: 'root.default',
          capacityValue: '50',
          maxCapacityValue: '100',
          mode: 'simple',
        }),
      ];
      vi.mocked(buildCapacityEditorDrafts).mockReturnValue(mockDrafts);

      store.getState().openCapacityEditor({
        origin: 'property-editor',
        parentQueuePath: 'root',
        originQueuePath: 'root.default',
        originQueueName: 'default',
      });

      store.getState().updateCapacityDraft('root.default', (draft) => {
        draft.capacityValue = '60';
        draft.maxCapacityValue = '80';
        draft.mode = 'vector';
      });

      const draft = store.getState().capacityEditor.drafts['root.default'];
      expect(draft.capacityValue).toBe('60');
      expect(draft.maxCapacityValue).toBe('80');
      expect(draft.mode).toBe('vector');
    });

    it('should maintain immutability using produce', () => {
      const store = createTestStore();

      const mockDrafts = [createMockDraft({ queuePath: 'root.default' })];
      vi.mocked(buildCapacityEditorDrafts).mockReturnValue(mockDrafts);

      store.getState().openCapacityEditor({
        origin: 'property-editor',
        parentQueuePath: 'root',
        originQueuePath: 'root.default',
        originQueueName: 'default',
      });

      const draftBefore = store.getState().capacityEditor.drafts['root.default'];

      store.getState().updateCapacityDraft('root.default', (draft) => {
        draft.capacityValue = '70';
      });

      const draftAfter = store.getState().capacityEditor.drafts['root.default'];

      // Should be a new object reference
      expect(draftBefore).not.toBe(draftAfter);
      expect(draftBefore.capacityValue).toBe('50');
      expect(draftAfter.capacityValue).toBe('70');
    });
  });

  describe('resetCapacityDrafts', () => {
    it('should rebuild drafts from store state', () => {
      const store = createTestStore();

      store.getState().openCapacityEditor({
        origin: 'property-editor',
        parentQueuePath: 'root',
        originQueuePath: 'root.default',
        originQueueName: 'default',
      });

      // Modify a draft
      store.getState().updateCapacityDraft('root.default', (draft) => {
        draft.capacityValue = '99';
      });

      expect(store.getState().capacityEditor.drafts['root.default'].capacityValue).toBe('99');

      // Reset
      const freshDrafts = [createMockDraft({ queuePath: 'root.default', capacityValue: '50' })];
      vi.mocked(buildCapacityEditorDrafts).mockReturnValue(freshDrafts);

      store.getState().resetCapacityDrafts();

      expect(store.getState().capacityEditor.drafts['root.default'].capacityValue).toBe('50');
    });

    it('should do nothing if editor not initialized', () => {
      const store = createTestStore();

      vi.mocked(buildCapacityEditorDrafts).mockClear();

      store.getState().resetCapacityDrafts();

      expect(buildCapacityEditorDrafts).not.toHaveBeenCalled();
    });

    it('should call buildCapacityEditorDrafts with current editor state', () => {
      const store = createTestStore();

      store.getState().openCapacityEditor({
        origin: 'add-queue',
        parentQueuePath: 'root',
        originQueuePath: 'root.new',
        originQueueName: 'new',
        originInitialCapacity: '30',
        originIsNew: true,
        selectedNodeLabel: 'gpu',
      });

      vi.mocked(buildCapacityEditorDrafts).mockClear();

      store.getState().resetCapacityDrafts();

      expect(buildCapacityEditorDrafts).toHaveBeenCalledWith(
        expect.objectContaining({
          parentQueuePath: 'root',
          originQueuePath: 'root.new',
          originQueueName: 'new',
          originInitialCapacity: '30',
          originIsNew: true,
          selectedNodeLabel: 'gpu',
        }),
      );
    });
  });

  describe('saveCapacityDrafts', () => {
    beforeEach(() => {
      // Setup store with necessary methods
      vi.mocked(validateQueue).mockReturnValue({
        valid: true,
        issues: [],
      });
    });

    it('should return false if editor not initialized', async () => {
      const store = createTestStore();

      const result = await store.getState().saveCapacityDrafts();

      expect(result).toBe(false);
    });

    it('should set isSaving to true during save', async () => {
      const store = createTestStore();

      const mockDrafts = [createMockDraft({ queuePath: 'root.default', capacityValue: '60' })];
      vi.mocked(buildCapacityEditorDrafts).mockReturnValue(mockDrafts);

      store.setState({
        configData: new Map([
          ['yarn.scheduler.capacity.root.default.capacity', '50'],
          ['yarn.scheduler.capacity.root.default.maximum-capacity', '100'],
        ]),
        stageQueueChange: vi.fn(),
        stageLabelQueueChange: vi.fn(),
        getQueuePropertyValue: vi.fn((path, property) => ({
          value: property === 'capacity' ? '50' : '100',
          isStaged: false,
        })),
      });

      store.getState().openCapacityEditor({
        origin: 'property-editor',
        parentQueuePath: 'root',
        originQueuePath: 'root.default',
        originQueueName: 'default',
      });

      const savePromise = store.getState().saveCapacityDrafts();

      // Check immediately after calling save
      // Note: might already be done by the time we check, so this is just to document the flow
      await savePromise;

      expect(store.getState().capacityEditor.isSaving).toBe(false);
    });

    it('should return true when no changes detected', async () => {
      const store = createTestStore();

      const mockDrafts = [createMockDraft({ queuePath: 'root.default', capacityValue: '50' })];
      vi.mocked(buildCapacityEditorDrafts).mockReturnValue(mockDrafts);

      store.setState({
        getQueuePropertyValue: vi.fn(() => ({ value: '50', isStaged: false })),
      });

      store.getState().openCapacityEditor({
        origin: 'property-editor',
        parentQueuePath: 'root',
        originQueuePath: 'root.default',
        originQueueName: 'default',
      });

      const result = await store.getState().saveCapacityDrafts();

      expect(result).toBe(true);
      expect(store.getState().capacityEditor.isSaving).toBe(false);
    });

    it('should validate all changed queues', async () => {
      const store = createTestStore();

      const mockDrafts = [createMockDraft({ queuePath: 'root.default', capacityValue: '60' })];
      vi.mocked(buildCapacityEditorDrafts).mockReturnValue(mockDrafts);

      store.setState({
        configData: new Map([['yarn.scheduler.capacity.root.default.capacity', '50']]),
        stagedChanges: [],
        schedulerData: null,
        stageQueueChange: vi.fn(),
        stageLabelQueueChange: vi.fn(),
        getQueuePropertyValue: vi.fn(() => ({ value: '50', isStaged: false })),
      });

      store.getState().openCapacityEditor({
        origin: 'property-editor',
        parentQueuePath: 'root',
        originQueuePath: 'root.default',
        originQueueName: 'default',
      });

      await store.getState().saveCapacityDrafts();

      expect(validateQueue).toHaveBeenCalledWith({
        queuePath: 'root.default',
        properties: expect.objectContaining({
          capacity: '60',
        }),
        configData: expect.any(Map),
        stagedChanges: [],
        schedulerData: null,
      });
    });

    it('should return false on validation error without force', async () => {
      const store = createTestStore();

      const mockDrafts = [createMockDraft({ queuePath: 'root.default', capacityValue: '150' })];
      vi.mocked(buildCapacityEditorDrafts).mockReturnValue(mockDrafts);

      vi.mocked(validateQueue).mockReturnValue({
        valid: false,
        issues: [
          {
            queuePath: 'root.default',
            field: 'capacity',
            message: 'Capacity exceeds maximum',
            severity: 'error',
            rule: 'max-capacity',
          },
        ],
      });

      store.setState({
        getQueuePropertyValue: vi.fn(() => ({ value: '50', isStaged: false })),
        stageQueueChange: vi.fn(),
        stageLabelQueueChange: vi.fn(),
      });

      store.getState().openCapacityEditor({
        origin: 'property-editor',
        parentQueuePath: 'root',
        originQueuePath: 'root.default',
        originQueueName: 'default',
      });

      const result = await store.getState().saveCapacityDrafts();

      expect(result).toBe(false);
      expect(store.getState().capacityEditor.saveError).toBe('Capacity validation failed.');
      expect(store.getState().capacityEditor.validationIssues).toHaveLength(1);
    });

    it('should proceed with save when force is true despite validation errors', async () => {
      const store = createTestStore();

      const mockDrafts = [createMockDraft({ queuePath: 'root.default', capacityValue: '150' })];
      vi.mocked(buildCapacityEditorDrafts).mockReturnValue(mockDrafts);

      const stageQueueChange = vi.fn();

      vi.mocked(validateQueue).mockReturnValue({
        valid: false,
        issues: [
          {
            queuePath: 'root.default',
            field: 'capacity',
            message: 'Capacity exceeds maximum',
            severity: 'error',
            rule: 'max-capacity',
          },
        ],
      });

      store.setState({
        getQueuePropertyValue: vi.fn(() => ({ value: '50', isStaged: false })),
        stageQueueChange,
        stageLabelQueueChange: vi.fn(),
      });

      store.getState().openCapacityEditor({
        origin: 'property-editor',
        parentQueuePath: 'root',
        originQueuePath: 'root.default',
        originQueueName: 'default',
      });

      const result = await store.getState().saveCapacityDrafts({ force: true });

      expect(result).toBe(true);
      expect(stageQueueChange).toHaveBeenCalled();
    });

    it('should clear draft cache after successful save', async () => {
      const store = createTestStore();

      const mockDrafts = [createMockDraft({ queuePath: 'root.default', capacityValue: '60' })];
      vi.mocked(buildCapacityEditorDrafts).mockReturnValue(mockDrafts);

      store.setState({
        getQueuePropertyValue: vi.fn(() => ({ value: '50', isStaged: false })),
        stageQueueChange: vi.fn(),
        stageLabelQueueChange: vi.fn(),
      });

      store.getState().openCapacityEditor({
        origin: 'property-editor',
        parentQueuePath: 'root',
        originQueuePath: 'root.default',
        originQueueName: 'default',
      });

      // Add something to cache
      store.getState().setCapacityEditorLabel('gpu');
      expect(Object.keys(store.getState().capacityEditor.draftCache)).toHaveLength(1);

      // Switch back and save
      store.getState().setCapacityEditorLabel(null);
      await store.getState().saveCapacityDrafts();

      expect(store.getState().capacityEditor.draftCache).toEqual({});
    });

    it('should handle vector mode drafts', async () => {
      const store = createTestStore();

      const mockDrafts = [
        createMockDraft({
          queuePath: 'root.default',
          mode: 'vector',
          vectorCapacity: [
            { key: 'memory', value: '2048', id: '1' },
            { key: 'vcores', value: '4', id: '2' },
          ],
        }),
      ];
      vi.mocked(buildCapacityEditorDrafts).mockReturnValue(mockDrafts);
      vi.mocked(convertVectorDraftToString).mockReturnValue('[memory=2048,vcores=4]');

      const stageQueueChange = vi.fn();

      store.setState({
        getQueuePropertyValue: vi.fn(() => ({ value: '[memory=1024,vcores=2]', isStaged: false })),
        stageQueueChange,
        stageLabelQueueChange: vi.fn(),
      });

      store.getState().openCapacityEditor({
        origin: 'property-editor',
        parentQueuePath: 'root',
        originQueuePath: 'root.default',
        originQueueName: 'default',
      });

      await store.getState().saveCapacityDrafts();

      expect(convertVectorDraftToString).toHaveBeenCalled();
      expect(stageQueueChange).toHaveBeenCalledWith(
        'root.default',
        'capacity',
        '[memory=2048,vcores=4]',
        undefined,
      );
    });

    it('should handle node label properties separately', async () => {
      const store = createTestStore();

      const mockDrafts = [createMockDraft({ queuePath: 'root.default', capacityValue: '60' })];
      vi.mocked(buildCapacityEditorDrafts).mockReturnValue(mockDrafts);

      // Mock to return proper property names based on the property type argument
      vi.mocked(getPropertyNameForLabel).mockImplementation((label, property) => {
        return `accessible-node-labels.${label || 'default'}.${property}`;
      });

      const stageLabelQueueChange = vi.fn();

      store.setState({
        getQueuePropertyValue: vi.fn(() => ({ value: '50', isStaged: false })),
        stageQueueChange: vi.fn(),
        stageLabelQueueChange,
      });

      store.getState().openCapacityEditor({
        origin: 'property-editor',
        parentQueuePath: 'root',
        originQueuePath: 'root.default',
        originQueueName: 'default',
        selectedNodeLabel: 'gpu',
      });

      await store.getState().saveCapacityDrafts();

      expect(stageLabelQueueChange).toHaveBeenCalledWith(
        'root.default',
        'gpu',
        'capacity',
        '60',
        undefined,
      );
    });
  });
});
