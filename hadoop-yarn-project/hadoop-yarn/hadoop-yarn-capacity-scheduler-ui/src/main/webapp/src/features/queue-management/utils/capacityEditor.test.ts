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


import { describe, expect, it } from 'vitest';
import type { SchedulerStore } from '~/stores/schedulerStore';
import type { QueueInfo } from '~/types';
import type { StagedChange } from '~/types/staged-change';
import {
  buildCapacityEditorLabelOptions,
  buildCapacityEditorDrafts,
  parseVectorDraft,
  ensureCoreEntries,
  createRowDraft,
  convertVectorDraftToString,
  createEmptyVectorEntry,
  getPropertyNameForLabel,
} from './capacityEditor';

describe('capacity editor utilities', () => {
  it('includes accessible labels and staged labels when building options', () => {
    const configData = new Map<string, string>();
    const store = {
      getQueuePropertyValue: () => ({ value: 'gpu,fpga', isStaged: false }),
      getQueueByPath: (queuePath: string) =>
        ({
          queuePath,
          queueName: queuePath.split('.').pop() || queuePath,
          nodeLabels: ['gpu', 'fpga'],
        }) as unknown as QueueInfo,
      nodeLabels: [
        { name: 'gpu', exclusivity: false },
        { name: 'fpga', exclusivity: false },
      ],
      stagedChanges: [],
      configData,
    } as unknown as SchedulerStore;

    const result = buildCapacityEditorLabelOptions(store, 'root.parent', null);
    expect(result.options).toEqual([
      { value: '__DEFAULT_PARTITION__', label: 'Default partition' },
      { value: 'fpga', label: 'fpga' },
      { value: 'gpu', label: 'gpu' },
    ]);
    expect(result.labelsWithoutAccess.size).toBe(0);
  });

  it('adds configured labels even when queue has no access to them', () => {
    const configData = new Map<string, string>([
      ['yarn.scheduler.capacity.root.parent.accessible-node-labels.ssd.capacity', '20'],
    ]);

    const store = {
      getQueuePropertyValue: () => ({ value: '', isStaged: false }),
      getQueueByPath: (queuePath: string) =>
        ({
          queuePath,
          queueName: queuePath.split('.').pop() || queuePath,
          nodeLabels: [], // Queue has no access to any labels
        }) as unknown as QueueInfo,
      nodeLabels: [],
      stagedChanges: [],
      configData,
    } as unknown as SchedulerStore;

    const result = buildCapacityEditorLabelOptions(store, 'root.parent', null);
    expect(result.options).toEqual([
      { value: '__DEFAULT_PARTITION__', label: 'Default partition' },
      { value: 'ssd', label: 'ssd' },
    ]);
    // ssd should be marked as without access
    expect(result.labelsWithoutAccess.has('ssd')).toBe(true);
  });

  it('shows all system labels when queue has wildcard access', () => {
    const configData = new Map<string, string>();
    const store = {
      getQueuePropertyValue: () => ({ value: '', isStaged: false }),
      getQueueByPath: (queuePath: string) =>
        ({
          queuePath,
          queueName: queuePath.split('.').pop() || queuePath,
          nodeLabels: ['*'], // Wildcard means access to all labels
        }) as unknown as QueueInfo,
      nodeLabels: [
        { name: 'gpu', exclusivity: false },
        { name: 'fpga', exclusivity: false },
        { name: 'ssd', exclusivity: false },
      ],
      stagedChanges: [],
      configData,
    } as unknown as SchedulerStore;

    const result = buildCapacityEditorLabelOptions(store, 'root.parent', null);
    expect(result.options).toEqual([
      { value: '__DEFAULT_PARTITION__', label: 'Default partition' },
      { value: 'fpga', label: 'fpga' },
      { value: 'gpu', label: 'gpu' },
      { value: 'ssd', label: 'ssd' },
    ]);
    expect(result.labelsWithoutAccess.size).toBe(0);
  });

  it('includes currently selected label even if not accessible', () => {
    const configData = new Map<string, string>();
    const store = {
      getQueuePropertyValue: () => ({ value: '', isStaged: false }),
      getQueueByPath: (queuePath: string) =>
        ({
          queuePath,
          queueName: queuePath.split('.').pop() || queuePath,
          nodeLabels: ['gpu'], // Only has access to gpu
        }) as unknown as QueueInfo,
      nodeLabels: [
        { name: 'gpu', exclusivity: false },
        { name: 'fpga', exclusivity: false },
      ],
      stagedChanges: [],
      configData,
    } as unknown as SchedulerStore;

    // User has selected fpga which the queue doesn't have access to
    const result = buildCapacityEditorLabelOptions(store, 'root.parent', 'fpga');
    expect(result.options).toEqual([
      { value: '__DEFAULT_PARTITION__', label: 'Default partition' },
      { value: 'fpga', label: 'fpga' },
      { value: 'gpu', label: 'gpu' },
    ]);
    // fpga should be marked as without access
    expect(result.labelsWithoutAccess.has('fpga')).toBe(true);
    expect(result.labelsWithoutAccess.has('gpu')).toBe(false);
  });

  it('falls back to parent when queue has no nodeLabels', () => {
    const configData = new Map<string, string>();
    const store = {
      getQueuePropertyValue: () => ({ value: '', isStaged: false }),
      getQueueByPath: (queuePath: string) => {
        if (queuePath === 'root.parent.child') {
          return {
            queuePath,
            queueName: 'child',
            nodeLabels: undefined, // No nodeLabels, should fallback to parent
          } as unknown as QueueInfo;
        }
        if (queuePath === 'root.parent') {
          return {
            queuePath,
            queueName: 'parent',
            nodeLabels: ['gpu', 'fpga'], // Parent has these labels
          } as unknown as QueueInfo;
        }
        return null;
      },
      nodeLabels: [
        { name: 'gpu', exclusivity: false },
        { name: 'fpga', exclusivity: false },
      ],
      stagedChanges: [],
      configData,
    } as unknown as SchedulerStore;

    const result = buildCapacityEditorLabelOptions(store, 'root.parent.child', null);
    expect(result.options).toEqual([
      { value: '__DEFAULT_PARTITION__', label: 'Default partition' },
      { value: 'fpga', label: 'fpga' },
      { value: 'gpu', label: 'gpu' },
    ]);
    expect(result.labelsWithoutAccess.size).toBe(0);
  });

  it('builds drafts including staged additions and origin queue first', () => {
    const configData = new Map<string, string>([
      ['yarn.scheduler.capacity.root.parent.child.capacity', '50'],
      ['yarn.scheduler.capacity.root.parent.child.maximum-capacity', '100'],
    ]);

    const childQueue: QueueInfo = {
      queueName: 'child',
      queuePath: 'root.parent.child',
      queueType: 'leaf',
      capacity: 50,
      usedCapacity: 0,
      maxCapacity: 100,
      absoluteCapacity: 0,
      absoluteMaxCapacity: 0,
      absoluteUsedCapacity: 0,
      numApplications: 0,
      numActiveApplications: 0,
      numPendingApplications: 0,
      state: 'RUNNING',
      resourcesUsed: { memory: 0, vCores: 0 },
    };

    const stagedChanges: StagedChange[] = [
      {
        id: 'add-1',
        type: 'add',
        queuePath: 'root.parent.newChild',
        property: 'capacity',
        oldValue: undefined,
        newValue: '30',
        timestamp: Date.now(),
      },
      {
        id: 'add-2',
        type: 'add',
        queuePath: 'root.parent.newChild',
        property: 'maximum-capacity',
        oldValue: undefined,
        newValue: '60',
        timestamp: Date.now(),
      },
    ];

    const store = {
      configData,
      getChildQueues: (parentPath: string) => (parentPath === 'root.parent' ? [childQueue] : []),
      getQueuePropertyValue: (queuePath: string, property: string) => {
        if (queuePath === 'root.parent.child') {
          if (property === 'capacity') {
            return { value: '50', isStaged: false };
          }
          if (property === 'maximum-capacity') {
            return { value: '100', isStaged: false };
          }
        }
        if (queuePath === 'root.parent.newChild') {
          if (property === 'capacity') {
            return { value: '30', isStaged: true };
          }
          if (property === 'maximum-capacity') {
            return { value: '60', isStaged: true };
          }
        }
        return { value: '', isStaged: false };
      },
      stagedChanges,
    } as unknown as SchedulerStore;

    const drafts = buildCapacityEditorDrafts({
      store,
      parentQueuePath: 'root.parent',
      originQueuePath: 'root.parent.child',
      originQueueName: 'child',
      originInitialCapacity: null,
      originInitialMaxCapacity: null,
      originIsNew: false,
      selectedNodeLabel: null,
    });

    expect(drafts).toHaveLength(2);
    expect(drafts[0].queuePath).toBe('root.parent.child');
    expect(drafts[0].isOrigin).toBe(true);
    expect(drafts[1].queuePath).toBe('root.parent.newChild');
    expect(drafts[1].isNew).toBe(true);
    expect(drafts[1].capacityValue).toBe('30');
    expect(drafts[1].maxCapacityValue).toBe('60');
  });

  describe('buildCapacityEditorDrafts with template paths', () => {
    it('builds single draft for legacy template path without sibling queues', () => {
      const configData = new Map<string, string>([
        ['yarn.scheduler.capacity.root.parent.leaf-queue-template.capacity', '10'],
        ['yarn.scheduler.capacity.root.parent.leaf-queue-template.maximum-capacity', '50'],
      ]);

      const store = {
        configData,
        getChildQueues: () => [],
        getQueuePropertyValue: (queuePath: string, property: string) => {
          if (queuePath === 'root.parent.leaf-queue-template') {
            if (property === 'capacity') {
              return { value: '10', isStaged: false };
            }
            if (property === 'maximum-capacity') {
              return { value: '50', isStaged: false };
            }
          }
          return { value: '', isStaged: false };
        },
        stagedChanges: [],
      } as unknown as SchedulerStore;

      const drafts = buildCapacityEditorDrafts({
        store,
        parentQueuePath: 'root.parent',
        originQueuePath: 'root.parent.leaf-queue-template',
        originQueueName: 'leaf-queue-template',
        originInitialCapacity: null,
        originInitialMaxCapacity: null,
        originIsNew: false,
        selectedNodeLabel: null,
      });

      expect(drafts).toHaveLength(1);
      expect(drafts[0].queuePath).toBe('root.parent.leaf-queue-template');
      expect(drafts[0].queueName).toBe('leaf-queue-template');
      expect(drafts[0].isOrigin).toBe(true);
      expect(drafts[0].capacityValue).toBe('10');
      expect(drafts[0].maxCapacityValue).toBe('50');
    });

    it('builds single draft for flexible template path (auto-queue-creation-v2.template)', () => {
      const configData = new Map<string, string>([
        ['yarn.scheduler.capacity.root.parent.auto-queue-creation-v2.template.capacity', '5w'],
        [
          'yarn.scheduler.capacity.root.parent.auto-queue-creation-v2.template.maximum-capacity',
          '100',
        ],
      ]);

      const store = {
        configData,
        getChildQueues: () => [],
        getQueuePropertyValue: (queuePath: string, property: string) => {
          if (queuePath === 'root.parent.auto-queue-creation-v2.template') {
            if (property === 'capacity') {
              return { value: '5w', isStaged: false };
            }
            if (property === 'maximum-capacity') {
              return { value: '100', isStaged: false };
            }
          }
          return { value: '', isStaged: false };
        },
        stagedChanges: [],
      } as unknown as SchedulerStore;

      const drafts = buildCapacityEditorDrafts({
        store,
        parentQueuePath: 'root.parent',
        originQueuePath: 'root.parent.auto-queue-creation-v2.template',
        originQueueName: 'auto-queue-creation-v2.template',
        originInitialCapacity: '5w',
        originInitialMaxCapacity: '100',
        originIsNew: false,
        selectedNodeLabel: null,
      });

      expect(drafts).toHaveLength(1);
      expect(drafts[0].queuePath).toBe('root.parent.auto-queue-creation-v2.template');
      expect(drafts[0].isOrigin).toBe(true);
      expect(drafts[0].capacityValue).toBe('5w');
      expect(drafts[0].maxCapacityValue).toBe('100');
    });

    it('builds single draft for flexible parent template path', () => {
      const store = {
        configData: new Map(),
        getChildQueues: () => [],
        getQueuePropertyValue: (queuePath: string, property: string) => {
          if (
            queuePath === 'root.parent.auto-queue-creation-v2.parent-template' &&
            property === 'capacity'
          ) {
            return { value: '3w', isStaged: false };
          }
          return { value: '', isStaged: false };
        },
        stagedChanges: [],
      } as unknown as SchedulerStore;

      const drafts = buildCapacityEditorDrafts({
        store,
        parentQueuePath: 'root.parent',
        originQueuePath: 'root.parent.auto-queue-creation-v2.parent-template',
        originQueueName: 'auto-queue-creation-v2.parent-template',
        originInitialCapacity: null,
        originInitialMaxCapacity: null,
        originIsNew: false,
        selectedNodeLabel: null,
      });

      expect(drafts).toHaveLength(1);
      expect(drafts[0].queuePath).toBe('root.parent.auto-queue-creation-v2.parent-template');
      expect(drafts[0].isOrigin).toBe(true);
    });

    it('builds single draft for flexible leaf template path', () => {
      const store = {
        configData: new Map(),
        getChildQueues: () => [],
        getQueuePropertyValue: (queuePath: string, property: string) => {
          if (
            queuePath === 'root.parent.auto-queue-creation-v2.leaf-template' &&
            property === 'capacity'
          ) {
            return { value: '2w', isStaged: false };
          }
          return { value: '', isStaged: false };
        },
        stagedChanges: [],
      } as unknown as SchedulerStore;

      const drafts = buildCapacityEditorDrafts({
        store,
        parentQueuePath: 'root.parent',
        originQueuePath: 'root.parent.auto-queue-creation-v2.leaf-template',
        originQueueName: 'auto-queue-creation-v2.leaf-template',
        originInitialCapacity: null,
        originInitialMaxCapacity: null,
        originIsNew: false,
        selectedNodeLabel: null,
      });

      expect(drafts).toHaveLength(1);
      expect(drafts[0].queuePath).toBe('root.parent.auto-queue-creation-v2.leaf-template');
      expect(drafts[0].isOrigin).toBe(true);
    });

    it('respects originInitialCapacity and originInitialMaxCapacity for templates', () => {
      const store = {
        configData: new Map(),
        getChildQueues: () => [],
        getQueuePropertyValue: () => ({ value: '', isStaged: false }),
        stagedChanges: [],
      } as unknown as SchedulerStore;

      const drafts = buildCapacityEditorDrafts({
        store,
        parentQueuePath: 'root.parent',
        originQueuePath: 'root.parent.leaf-queue-template',
        originQueueName: 'leaf-queue-template',
        originInitialCapacity: '15',
        originInitialMaxCapacity: '75',
        originIsNew: false,
        selectedNodeLabel: null,
      });

      expect(drafts).toHaveLength(1);
      expect(drafts[0].capacityValue).toBe('15');
      expect(drafts[0].maxCapacityValue).toBe('75');
    });

    it('marks template as new when originIsNew is true', () => {
      const store = {
        configData: new Map(),
        getChildQueues: () => [],
        getQueuePropertyValue: () => ({ value: '', isStaged: false }),
        stagedChanges: [],
      } as unknown as SchedulerStore;

      const drafts = buildCapacityEditorDrafts({
        store,
        parentQueuePath: 'root.parent',
        originQueuePath: 'root.parent.leaf-queue-template',
        originQueueName: 'leaf-queue-template',
        originInitialCapacity: '20',
        originInitialMaxCapacity: '100',
        originIsNew: true,
        selectedNodeLabel: null,
      });

      expect(drafts).toHaveLength(1);
      expect(drafts[0].isNew).toBe(true);
      expect(drafts[0].hasStagedChange).toBe(true);
    });
  });

  describe('parseVectorDraft', () => {
    it('should parse valid vector string with two resources', () => {
      const result = parseVectorDraft('[memory=2048,vcores=4]');

      expect(result).toHaveLength(2);
      expect(result[0].key).toBe('memory');
      expect(result[0].value).toBe('2048');
      expect(result[0].id).toBeDefined();
      expect(result[1].key).toBe('vcores');
      expect(result[1].value).toBe('4');
      expect(result[1].id).toBeDefined();
    });

    it('should parse vector with multiple resources', () => {
      const result = parseVectorDraft('[memory=2048,vcores=4,gpu=2]');

      expect(result).toHaveLength(3);
      expect(result[2].key).toBe('gpu');
      expect(result[2].value).toBe('2');
    });

    it('should handle vector with whitespace', () => {
      const result = parseVectorDraft('[ memory = 2048 , vcores = 4 ]');

      expect(result).toHaveLength(2);
      expect(result[0].key).toBe('memory');
      expect(result[0].value).toBe('2048');
      expect(result[1].key).toBe('vcores');
      expect(result[1].value).toBe('4');
    });

    it('should return empty array for non-vector string', () => {
      const result = parseVectorDraft('100');

      expect(result).toEqual([]);
    });

    it('should return empty array for empty string', () => {
      const result = parseVectorDraft('');

      expect(result).toEqual([]);
    });

    it('should return empty array for empty brackets', () => {
      const result = parseVectorDraft('[]');

      expect(result).toEqual([]);
    });

    it('should return empty array for brackets with only whitespace', () => {
      const result = parseVectorDraft('[   ]');

      expect(result).toEqual([]);
    });

    it('should skip entries without key', () => {
      const result = parseVectorDraft('[memory=2048,=100,vcores=4]');

      expect(result).toHaveLength(2);
      expect(result[0].key).toBe('memory');
      expect(result[1].key).toBe('vcores');
    });

    it('should handle entries with missing value', () => {
      const result = parseVectorDraft('[memory=2048,vcores=]');

      expect(result).toHaveLength(2);
      expect(result[0].key).toBe('memory');
      expect(result[0].value).toBe('2048');
      expect(result[1].key).toBe('vcores');
      expect(result[1].value).toBe('');
    });

    it('should handle entries with no equals sign', () => {
      const result = parseVectorDraft('[memory=2048,vcores]');

      expect(result).toHaveLength(2);
      expect(result[0].key).toBe('memory');
      expect(result[1].key).toBe('vcores');
      expect(result[1].value).toBe('');
    });

    it('should handle vector with leading/trailing whitespace', () => {
      const result = parseVectorDraft('  [memory=2048,vcores=4]  ');

      expect(result).toHaveLength(2);
      expect(result[0].key).toBe('memory');
      expect(result[0].value).toBe('2048');
    });
  });

  describe('ensureCoreEntries', () => {
    it('should add missing memory entry', () => {
      const entries = [{ id: '1', key: 'vcores', value: '4' }];
      const result = ensureCoreEntries(entries);

      expect(result).toHaveLength(2);
      expect(result.some((e) => e.key === 'memory')).toBe(true);
      expect(result.some((e) => e.key === 'vcores')).toBe(true);
    });

    it('should add missing vcores entry', () => {
      const entries = [{ id: '1', key: 'memory', value: '2048' }];
      const result = ensureCoreEntries(entries);

      expect(result).toHaveLength(2);
      expect(result.some((e) => e.key === 'memory')).toBe(true);
      expect(result.some((e) => e.key === 'vcores')).toBe(true);
    });

    it('should add both memory and vcores when empty', () => {
      const entries: any[] = [];
      const result = ensureCoreEntries(entries);

      expect(result).toHaveLength(2);
      expect(result.some((e) => e.key === 'memory')).toBe(true);
      expect(result.some((e) => e.key === 'vcores')).toBe(true);
    });

    it('should not duplicate existing entries', () => {
      const entries = [
        { id: '1', key: 'memory', value: '2048' },
        { id: '2', key: 'vcores', value: '4' },
      ];
      const result = ensureCoreEntries(entries);

      expect(result).toHaveLength(2);
      expect(result[0].id).toBe('1');
      expect(result[1].id).toBe('2');
    });

    it('should preserve custom entries', () => {
      const entries = [
        { id: '1', key: 'memory', value: '2048' },
        { id: '2', key: 'gpu', value: '2' },
      ];
      const result = ensureCoreEntries(entries);

      expect(result).toHaveLength(3);
      expect(result.some((e) => e.key === 'memory')).toBe(true);
      expect(result.some((e) => e.key === 'gpu')).toBe(true);
      expect(result.some((e) => e.key === 'vcores')).toBe(true);
    });

    it('should not add defaults when includeDefaults is false', () => {
      const entries = [{ id: '1', key: 'gpu', value: '2' }];
      const result = ensureCoreEntries(entries, false);

      expect(result).toHaveLength(1);
      expect(result[0].key).toBe('gpu');
    });

    it('should set empty value for added core entries', () => {
      const entries: any[] = [];
      const result = ensureCoreEntries(entries);

      result.forEach((entry) => {
        expect(entry.value).toBe('');
        expect(entry.id).toBeDefined();
      });
    });
  });

  describe('createRowDraft', () => {
    it('should create draft in simple mode for percentage values', () => {
      const draft = createRowDraft({
        queuePath: 'root.default',
        queueName: 'default',
        baseCapacity: '50',
        baseMaxCapacity: '100',
      });

      expect(draft.queuePath).toBe('root.default');
      expect(draft.queueName).toBe('default');
      expect(draft.mode).toBe('simple');
      expect(draft.baseMode).toBe('simple');
      expect(draft.capacityValue).toBe('50');
      expect(draft.maxCapacityValue).toBe('100');
      expect(draft.vectorCapacity).toEqual([]);
      expect(draft.vectorMaxCapacity).toEqual([]);
      expect(draft.isOrigin).toBe(false);
      expect(draft.isNew).toBe(false);
      expect(draft.hasStagedChange).toBe(false);
    });

    it('should create draft in vector mode for vector values', () => {
      const draft = createRowDraft({
        queuePath: 'root.default',
        queueName: 'default',
        baseCapacity: '[memory=2048,vcores=4]',
        baseMaxCapacity: '[memory=4096,vcores=8]',
      });

      expect(draft.mode).toBe('vector');
      expect(draft.baseMode).toBe('vector');
      expect(draft.vectorCapacity.length).toBeGreaterThan(0);
      expect(draft.vectorMaxCapacity.length).toBeGreaterThan(0);
    });

    it('should use currentCapacity when provided', () => {
      const draft = createRowDraft({
        queuePath: 'root.default',
        queueName: 'default',
        baseCapacity: '50',
        baseMaxCapacity: '100',
        currentCapacity: '60',
        currentMaxCapacity: '80',
      });

      expect(draft.capacityValue).toBe('60');
      expect(draft.maxCapacityValue).toBe('80');
      expect(draft.baseCapacityValue).toBe('50');
      expect(draft.baseMaxCapacityValue).toBe('100');
    });

    it('should set isOrigin flag', () => {
      const draft = createRowDraft({
        queuePath: 'root.default',
        queueName: 'default',
        baseCapacity: '50',
        baseMaxCapacity: '100',
        isOrigin: true,
      });

      expect(draft.isOrigin).toBe(true);
    });

    it('should set isNew flag', () => {
      const draft = createRowDraft({
        queuePath: 'root.default',
        queueName: 'default',
        baseCapacity: '50',
        baseMaxCapacity: '100',
        isNew: true,
      });

      expect(draft.isNew).toBe(true);
    });

    it('should set hasStagedChange flag', () => {
      const draft = createRowDraft({
        queuePath: 'root.default',
        queueName: 'default',
        baseCapacity: '50',
        baseMaxCapacity: '100',
        hasStagedChange: true,
      });

      expect(draft.hasStagedChange).toBe(true);
    });

    it('should handle null current values', () => {
      const draft = createRowDraft({
        queuePath: 'root.default',
        queueName: 'default',
        baseCapacity: '50',
        baseMaxCapacity: '100',
        currentCapacity: null,
        currentMaxCapacity: null,
      });

      expect(draft.capacityValue).toBe('50');
      expect(draft.maxCapacityValue).toBe('100');
    });

    it('should handle undefined base values', () => {
      const draft = createRowDraft({
        queuePath: 'root.default',
        queueName: 'default',
        baseCapacity: undefined,
        baseMaxCapacity: undefined,
      });

      expect(draft.baseCapacityValue).toBe('');
      expect(draft.baseMaxCapacityValue).toBe('');
      expect(draft.capacityValue).toBe('');
      expect(draft.maxCapacityValue).toBe('');
    });

    it('should trim whitespace from values', () => {
      const draft = createRowDraft({
        queuePath: 'root.default',
        queueName: 'default',
        baseCapacity: '  50  ',
        baseMaxCapacity: '  100  ',
      });

      expect(draft.capacityValue).toBe('50');
      expect(draft.maxCapacityValue).toBe('100');
    });

    it('should ensure core entries in vector mode', () => {
      const draft = createRowDraft({
        queuePath: 'root.default',
        queueName: 'default',
        baseCapacity: '[gpu=2]',
        baseMaxCapacity: '[gpu=4]',
      });

      expect(draft.mode).toBe('vector');
      expect(draft.vectorCapacity.some((e) => e.key === 'memory')).toBe(true);
      expect(draft.vectorCapacity.some((e) => e.key === 'vcores')).toBe(true);
      expect(draft.vectorCapacity.some((e) => e.key === 'gpu')).toBe(true);
    });
  });

  describe('convertVectorDraftToString', () => {
    it('should convert entries to vector string', () => {
      const entries = [
        { id: '1', key: 'memory', value: '2048' },
        { id: '2', key: 'vcores', value: '4' },
      ];
      const result = convertVectorDraftToString(entries);

      expect(result).toBe('[memory=2048,vcores=4]');
    });

    it('should handle single entry', () => {
      const entries = [{ id: '1', key: 'memory', value: '2048' }];
      const result = convertVectorDraftToString(entries);

      expect(result).toBe('[memory=2048]');
    });

    it('should return empty string for empty array', () => {
      const result = convertVectorDraftToString([]);

      expect(result).toBe('');
    });

    it('should trim whitespace from keys and values', () => {
      const entries = [
        { id: '1', key: '  memory  ', value: '  2048  ' },
        { id: '2', key: '  vcores  ', value: '  4  ' },
      ];
      const result = convertVectorDraftToString(entries);

      expect(result).toBe('[memory=2048,vcores=4]');
    });

    it('should filter out entries with empty keys', () => {
      const entries = [
        { id: '1', key: 'memory', value: '2048' },
        { id: '2', key: '', value: '100' },
        { id: '3', key: 'vcores', value: '4' },
      ];
      const result = convertVectorDraftToString(entries);

      expect(result).toBe('[memory=2048,vcores=4]');
    });

    it('should filter out entries with whitespace-only keys', () => {
      const entries = [
        { id: '1', key: 'memory', value: '2048' },
        { id: '2', key: '   ', value: '100' },
        { id: '3', key: 'vcores', value: '4' },
      ];
      const result = convertVectorDraftToString(entries);

      expect(result).toBe('[memory=2048,vcores=4]');
    });

    it('should preserve entries with empty values', () => {
      const entries = [
        { id: '1', key: 'memory', value: '2048' },
        { id: '2', key: 'vcores', value: '' },
      ];
      const result = convertVectorDraftToString(entries);

      expect(result).toBe('[memory=2048,vcores=]');
    });

    it('should handle multiple resources', () => {
      const entries = [
        { id: '1', key: 'memory', value: '2048' },
        { id: '2', key: 'vcores', value: '4' },
        { id: '3', key: 'gpu', value: '2' },
      ];
      const result = convertVectorDraftToString(entries);

      expect(result).toBe('[memory=2048,vcores=4,gpu=2]');
    });

    it('should return empty string when all entries have empty keys', () => {
      const entries = [
        { id: '1', key: '', value: '100' },
        { id: '2', key: '   ', value: '200' },
      ];
      const result = convertVectorDraftToString(entries);

      expect(result).toBe('');
    });
  });

  describe('createEmptyVectorEntry', () => {
    it('should create entry with default empty values', () => {
      const entry = createEmptyVectorEntry();

      expect(entry.key).toBe('');
      expect(entry.value).toBe('');
      expect(entry.id).toBeDefined();
      expect(typeof entry.id).toBe('string');
      expect(entry.id.length).toBeGreaterThan(0);
    });

    it('should create entry with provided key', () => {
      const entry = createEmptyVectorEntry('memory');

      expect(entry.key).toBe('memory');
      expect(entry.value).toBe('');
    });

    it('should create entry with provided key and value', () => {
      const entry = createEmptyVectorEntry('memory', '2048');

      expect(entry.key).toBe('memory');
      expect(entry.value).toBe('2048');
    });

    it('should generate unique IDs for multiple entries', () => {
      const entry1 = createEmptyVectorEntry();
      const entry2 = createEmptyVectorEntry();

      expect(entry1.id).not.toBe(entry2.id);
    });

    it('should handle empty string arguments', () => {
      const entry = createEmptyVectorEntry('', '');

      expect(entry.key).toBe('');
      expect(entry.value).toBe('');
      expect(entry.id).toBeDefined();
    });
  });

  describe('getPropertyNameForLabel', () => {
    it('should return base property name when label is null', () => {
      const result = getPropertyNameForLabel(null, 'capacity');

      expect(result).toBe('capacity');
    });

    it('should return base property name for maximum-capacity when label is null', () => {
      const result = getPropertyNameForLabel(null, 'maximum-capacity');

      expect(result).toBe('maximum-capacity');
    });

    it('should return label-prefixed property name for capacity', () => {
      const result = getPropertyNameForLabel('gpu', 'capacity');

      expect(result).toBe('accessible-node-labels.gpu.capacity');
    });

    it('should return label-prefixed property name for maximum-capacity', () => {
      const result = getPropertyNameForLabel('gpu', 'maximum-capacity');

      expect(result).toBe('accessible-node-labels.gpu.maximum-capacity');
    });

    it('should handle different label names', () => {
      const result = getPropertyNameForLabel('ssd', 'capacity');

      expect(result).toBe('accessible-node-labels.ssd.capacity');
    });

    it('should handle label with special characters', () => {
      const result = getPropertyNameForLabel('gpu-high-memory', 'capacity');

      expect(result).toBe('accessible-node-labels.gpu-high-memory.capacity');
    });
  });
});
