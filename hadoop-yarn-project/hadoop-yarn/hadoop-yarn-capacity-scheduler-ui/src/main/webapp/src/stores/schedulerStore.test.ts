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


import { describe, it, expect, beforeEach, vi, afterEach } from 'vitest';
import { createSchedulerStore } from '~/stores/schedulerStore';
import { buildMutationRequest } from '~/features/staged-changes/utils/mutationBuilder';
import type { YarnApiClient } from '~/lib/api/YarnApiClient';
import type {
  QueueInfo,
  StagedChange,
  ConfigData,
  NodeLabelsResponse,
  NodeToLabelsResponse,
  NodesResponse,
  VersionResponse,
  SchedulerResponse,
} from '~/types';
import { CONFIG_PREFIXES, QUEUE_TYPES, SPECIAL_VALUES } from '~/types/constants';
import { AUTO_CREATION_PROPS } from '~/types/constants/auto-creation';

/**
 * Local helper: traverse queue tree and apply a visitor function.
 * Combines queue info with configured properties from configData.
 */
function traverseQueueTree(
  queueInfo: QueueInfo,
  configData: Map<string, string>,
  visitor: (queue: QueueInfo & { configured: Record<string, string> }) => void,
): void {
  const configured: Record<string, string> = {};

  const prefix = `${CONFIG_PREFIXES.BASE}.${queueInfo.queuePath}.`;
  for (const [key, value] of configData.entries()) {
    if (key.startsWith(prefix)) {
      const property = key.substring(prefix.length);
      configured[property] = value;
    }
  }

  const combinedQueue = {
    ...queueInfo,
    configured,
  };

  visitor(combinedQueue);

  if (queueInfo.queues?.queue) {
    const children = Array.isArray(queueInfo.queues.queue)
      ? queueInfo.queues.queue
      : [queueInfo.queues.queue];

    for (const child of children) {
      traverseQueueTree(child, configData, visitor);
    }
  }
}

const toEntryRecord = (entries?: Array<{ key: string; value: string }>) =>
  Object.fromEntries((entries ?? []).map(({ key, value }) => [key, value]));

const MAXIMUM_APPLICATIONS_PROPERTY = 'yarn.scheduler.capacity.maximum-applications';
const LEGACY_MODE_PROPERTY = SPECIAL_VALUES.LEGACY_MODE_PROPERTY;

// Mock data for tests
const mockSchedulerResponse: SchedulerResponse = {
  scheduler: {
    schedulerInfo: {
      type: 'capacityScheduler',
      capacity: 100,
      usedCapacity: 50,
      maxCapacity: 100,
      queueName: 'root',
      queues: {
        queue: [
          {
            queueType: QUEUE_TYPES.LEAF,
            queueName: 'default',
            capacity: 10,
            usedCapacity: 0,
            maxCapacity: 100,
            absoluteCapacity: 10,
            absoluteMaxCapacity: 100,
            absoluteUsedCapacity: 0,
            numApplications: 0,
            numActiveApplications: 0,
            numPendingApplications: 0,
            queuePath: 'root.default',
            queues: { queue: [] },
            resourcesUsed: {
              memory: 0,
              vCores: 0,
            },
            state: 'RUNNING',
          },
          {
            queueType: QUEUE_TYPES.PARENT,
            queueName: 'production',
            capacity: 60,
            usedCapacity: 0,
            maxCapacity: 100,
            absoluteCapacity: 60,
            absoluteMaxCapacity: 100,
            absoluteUsedCapacity: 0,
            numApplications: 0,
            numActiveApplications: 0,
            numPendingApplications: 0,
            queuePath: 'root.production',
            queues: {
              queue: [
                {
                  queueType: QUEUE_TYPES.LEAF,
                  queueName: 'batch',
                  capacity: 50,
                  usedCapacity: 0,
                  maxCapacity: 100,
                  absoluteCapacity: 30,
                  absoluteMaxCapacity: 60,
                  absoluteUsedCapacity: 0,
                  numApplications: 0,
                  numActiveApplications: 0,
                  numPendingApplications: 0,
                  queuePath: 'root.production.batch',
                  queues: { queue: [] },
                  resourcesUsed: {
                    memory: 0,
                    vCores: 0,
                  },
                  state: 'RUNNING',
                },
                {
                  queueType: QUEUE_TYPES.LEAF,
                  queueName: 'interactive',
                  capacity: 50,
                  usedCapacity: 0,
                  maxCapacity: 100,
                  absoluteCapacity: 30,
                  absoluteMaxCapacity: 60,
                  absoluteUsedCapacity: 0,
                  numApplications: 0,
                  numActiveApplications: 0,
                  numPendingApplications: 0,
                  queuePath: 'root.production.interactive',
                  queues: { queue: [] },
                  resourcesUsed: {
                    memory: 0,
                    vCores: 0,
                  },
                  state: 'RUNNING',
                },
              ],
            },
            state: 'RUNNING',
          },
          {
            queueType: QUEUE_TYPES.LEAF,
            queueName: 'development',
            capacity: 30,
            usedCapacity: 0,
            maxCapacity: 100,
            absoluteCapacity: 30,
            absoluteMaxCapacity: 100,
            absoluteUsedCapacity: 0,
            numApplications: 0,
            numActiveApplications: 0,
            numPendingApplications: 0,
            queuePath: 'root.development',
            queues: { queue: [] },
            resourcesUsed: {
              memory: 0,
              vCores: 0,
            },
            state: 'RUNNING',
          },
        ],
      },
    },
  },
};

const mockConfigResponse: ConfigData = {
  property: [
    {
      name: 'yarn.scheduler.capacity.root.queues',
      value: 'default,production,development',
    },
    {
      name: 'yarn.scheduler.capacity.root.capacity',
      value: '100',
    },
    {
      name: 'yarn.scheduler.capacity.root.default.capacity',
      value: '10',
    },
    {
      name: 'yarn.scheduler.capacity.root.default.maximum-capacity',
      value: '100',
    },
    {
      name: 'yarn.scheduler.capacity.root.production.capacity',
      value: '60',
    },
    {
      name: 'yarn.scheduler.capacity.root.production.maximum-capacity',
      value: '100',
    },
    {
      name: 'yarn.scheduler.capacity.root.production.queues',
      value: 'batch,interactive',
    },
    {
      name: 'yarn.scheduler.capacity.root.production.batch.capacity',
      value: '50',
    },
    {
      name: 'yarn.scheduler.capacity.root.production.batch.maximum-capacity',
      value: '100',
    },
    {
      name: 'yarn.scheduler.capacity.root.production.interactive.capacity',
      value: '50',
    },
    {
      name: 'yarn.scheduler.capacity.root.production.interactive.maximum-capacity',
      value: '100',
    },
    {
      name: 'yarn.scheduler.capacity.root.development.capacity',
      value: '30',
    },
    {
      name: 'yarn.scheduler.capacity.root.development.maximum-capacity',
      value: '100',
    },
    {
      name: 'yarn.scheduler.capacity.maximum-applications',
      value: '10000',
    },
    {
      name: 'yarn.scheduler.capacity.maximum-am-resource-percent',
      value: '0.1',
    },
    {
      name: 'yarn.scheduler.capacity.resource-calculator',
      value: 'org.apache.hadoop.yarn.util.resource.DefaultResourceCalculator',
    },
    {
      name: 'yarn.scheduler.capacity.root.default.user-limit-factor',
      value: '1',
    },
    {
      name: 'yarn.scheduler.capacity.root.default.minimum-user-limit-percent',
      value: '100',
    },
    {
      name: 'yarn.scheduler.capacity.root.default.state',
      value: 'RUNNING',
    },
  ],
};

const mockNodeLabelsResponse: NodeLabelsResponse = {
  nodeLabelInfo: [
    { name: 'gpu', exclusivity: 'true' },
    { name: 'ssd', exclusivity: 'false' },
    { name: 'high-memory', exclusivity: true },
  ],
};

const mockNodesResponse: NodesResponse = {
  nodes: {
    node: [
      {
        id: 'node1.example.com:8041',
        rack: '/default-rack',
        nodeHostName: 'node1.example.com',
        state: 'RUNNING',
        nodeHTTPAddress: 'node1.example.com:8042',
        version: '3.4.0',
        lastHealthUpdate: Date.now(),
        healthReport: 'Healthy',
        numContainers: 5,
        usedMemoryMB: 4096,
        availMemoryMB: 12288,
        usedVirtualCores: 4,
        availableVirtualCores: 12,
        numRunningOpportContainers: 0,
        usedMemoryOpportGB: 0,
        usedVirtualCoresOpport: 0,
        numQueuedContainers: 0,
        nodeLabels: ['gpu'],
        allocationTags: {},
        usedResource: {
          memory: 4096,
          vCores: 4,
        },
        availableResource: {
          memory: 12288,
          vCores: 12,
        },
        nodeAttributesInfo: {},
      },
    ],
  },
};

const mockNodeToLabelsResponse: NodeToLabelsResponse = {
  nodeToLabels: {
    entry: [
      {
        key: 'node1.example.com:8041',
        value: {
          nodeLabelInfo: {
            name: 'high-memory',
            exclusivity: 'true',
          },
        },
      },
    ],
  },
};

const mockVersionResponse: VersionResponse = {
  versionId: 1234567890,
};

// Mock the YARN API client
vi.mock('~/lib/api/YarnApiClient');

// Create mock API client
const createMockApiClient = () => ({
  getScheduler: vi.fn(),
  getSchedulerConf: vi.fn(),
  getNodeLabels: vi.fn(),
  getNodes: vi.fn(),
  getNodeToLabels: vi.fn(),
  getSchedulerConfVersion: vi.fn(),
  validateSchedulerConf: vi.fn(),
  updateSchedulerConf: vi.fn(),
  getIsReadOnly: vi.fn(() => false), // Default to writable mode in tests
});

// Helper to create store with mock API client
const createTestStore = () => {
  const mockApiClient = createMockApiClient();
  return createSchedulerStore(mockApiClient as unknown as YarnApiClient);
};

// Helper function to set up store with data
async function setupStoreWithData(store: ReturnType<typeof createSchedulerStore>) {
  const mockApiClient = vi.mocked(store.getState().apiClient);

  mockApiClient.getScheduler.mockResolvedValue(mockSchedulerResponse);
  mockApiClient.getSchedulerConf.mockResolvedValue(mockConfigResponse);
  mockApiClient.getNodeLabels.mockResolvedValue(mockNodeLabelsResponse);
  mockApiClient.getNodes.mockResolvedValue(mockNodesResponse);
  mockApiClient.getNodeToLabels.mockResolvedValue(mockNodeToLabelsResponse);
  mockApiClient.getSchedulerConfVersion.mockResolvedValue(mockVersionResponse);

  await store.getState().loadInitialData();
}

describe('schedulerStore', () => {
  beforeEach(() => {
    vi.clearAllMocks();
  });

  afterEach(() => {
    vi.restoreAllMocks();
  });

  describe('store initialization', () => {
    it('should create store with initial state', () => {
      const mockApiClient = createMockApiClient();
      const store = createSchedulerStore(mockApiClient as unknown as YarnApiClient);
      const state = store.getState();

      expect(state.schedulerData).toBeNull();
      expect(state.configData).toEqual(new Map());
      expect(state.nodeLabels).toEqual([]);
      expect(state.stagedChanges).toEqual([]);
      expect(state.applyError).toBeNull();
      expect(state.selectedNodeLabel).toBeNull();
      expect(state.configVersion).toBe(0);
      expect(state.isLoading).toBe(false);
      expect(state.error).toBeNull();
    });

    it('should have all required actions', () => {
      const store = createTestStore();
      const state = store.getState();

      expect(typeof state.loadInitialData).toBe('function');
      expect(typeof state.refreshSchedulerData).toBe('function');
      expect(typeof state.stageQueueChange).toBe('function');
      expect(typeof state.stageGlobalChange).toBe('function');
      expect(typeof state.stageQueueAddition).toBe('function');
      expect(typeof state.stageQueueRemoval).toBe('function');
      expect(typeof state.stageLabelQueueChange).toBe('function');
      expect(typeof state.applyChanges).toBe('function');
      expect(typeof state.revertChange).toBe('function');
      expect(typeof state.clearAllChanges).toBe('function');
      expect(typeof state.selectNodeLabel).toBe('function');
    });

    it('should have computed value functions', () => {
      const store = createTestStore();
      const state = store.getState();

      expect(typeof state.getQueuePropertyValue).toBe('function');
      expect(typeof state.getGlobalPropertyValue).toBe('function');
      expect(typeof state.getLabelChangesForQueue).toBe('function');
    });
  });

  describe('loadInitialData', () => {
    it('should load all data sources in parallel', async () => {
      const store = createTestStore();
      const mockApiClient = vi.mocked(store.getState().apiClient);

      mockApiClient.getScheduler.mockResolvedValue(mockSchedulerResponse);
      mockApiClient.getSchedulerConf.mockResolvedValue(mockConfigResponse);
      mockApiClient.getNodeLabels.mockResolvedValue(mockNodeLabelsResponse);
      mockApiClient.getNodes.mockResolvedValue(mockNodesResponse);
      mockApiClient.getNodeToLabels.mockResolvedValue(mockNodeToLabelsResponse);
      mockApiClient.getSchedulerConfVersion.mockResolvedValue(mockVersionResponse);

      await store.getState().loadInitialData();

      expect(mockApiClient.getScheduler).toHaveBeenCalledTimes(1);
      expect(mockApiClient.getSchedulerConf).toHaveBeenCalledTimes(1);
      expect(mockApiClient.getNodeLabels).toHaveBeenCalledTimes(1);
      expect(mockApiClient.getSchedulerConfVersion).toHaveBeenCalledTimes(1);

      expect(store.getState().schedulerData).toEqual(mockSchedulerResponse.scheduler.schedulerInfo);
      expect(store.getState().configData.size).toBe(19);
      expect(store.getState().configData.get('yarn.scheduler.capacity.root.default.capacity')).toBe(
        '10',
      );
      expect(store.getState().nodeLabels).toHaveLength(3);
      expect(store.getState().configVersion).toBe(1234567890);
    });

    it('should set loading state during data fetch', async () => {
      const store = createTestStore();
      const mockApiClient = vi.mocked(store.getState().apiClient);

      // Create promises that we can control
      let resolveScheduler: (value: SchedulerResponse) => void;
      const schedulerPromise = new Promise<SchedulerResponse>((resolve) => {
        resolveScheduler = resolve;
      });

      mockApiClient.getScheduler.mockReturnValue(schedulerPromise);
      mockApiClient.getSchedulerConf.mockReturnValue(Promise.resolve(mockConfigResponse));
      mockApiClient.getNodeLabels.mockReturnValue(Promise.resolve(mockNodeLabelsResponse));
      mockApiClient.getSchedulerConfVersion.mockReturnValue(Promise.resolve(mockVersionResponse));

      // Start the load without awaiting
      const loadPromise = store.getState().loadInitialData();

      // Check loading state immediately
      expect(store.getState().isLoading).toBe(true);

      // Resolve the scheduler promise
      resolveScheduler!(mockSchedulerResponse);

      // Wait for the promise to settle
      await loadPromise.catch(() => {}); // Catch any errors to prevent unhandled rejection

      expect(store.getState().isLoading).toBe(false);
    });

    it('should handle API errors gracefully', async () => {
      const store = createTestStore();
      const mockApiClient = vi.mocked(store.getState().apiClient);

      mockApiClient.getScheduler.mockRejectedValue(new Error('Network error'));

      // Call loadInitialData and expect it to throw
      let error: unknown;
      try {
        await store.getState().loadInitialData();
      } catch (e) {
        error = e;
      }

      expect(error).toBeDefined();

      expect(store.getState().error).toBe('Failed to load initial data: Network error');
      expect(store.getState().errorContext).toBe('load');
      expect(store.getState().isLoading).toBe(false);
    });

    it('should handle HTTP error responses', async () => {
      const store = createTestStore();
      const mockApiClient = vi.mocked(store.getState().apiClient);

      mockApiClient.getScheduler.mockRejectedValue(new Error('HTTP 403: Forbidden'));

      // Call loadInitialData and expect it to throw
      let error: unknown;
      try {
        await store.getState().loadInitialData();
      } catch (e) {
        error = e;
      }

      expect(error).toBeDefined();

      expect(store.getState().error).toBe('Failed to load initial data: HTTP 403: Forbidden');
      expect(store.getState().errorContext).toBe('load');
      expect(store.getState().isLoading).toBe(false);
    });
  });

  describe('refreshSchedulerData', () => {
    it('should only refresh scheduler data, not config', async () => {
      const store = createTestStore();
      const mockApiClient = vi.mocked(store.getState().apiClient);

      // First load initial data
      await setupStoreWithData(store);

      vi.clearAllMocks();

      // Now refresh only scheduler data
      const updatedSchedulerResponse = {
        ...mockSchedulerResponse,
        scheduler: {
          ...mockSchedulerResponse.scheduler,
          schedulerInfo: {
            ...mockSchedulerResponse.scheduler.schedulerInfo,
            usedCapacity: 60, // Changed value
          },
        },
      };

      mockApiClient.getScheduler.mockResolvedValue(updatedSchedulerResponse);

      await store.getState().refreshSchedulerData();

      expect(mockApiClient.getScheduler).toHaveBeenCalledTimes(1);

      expect(store.getState().schedulerData?.usedCapacity).toBe(60);
      // Config data should remain unchanged
      expect(store.getState().configData.size).toBe(19);
    });
  });

  describe('staging changes', () => {
    describe('stageQueueChange', () => {
      it('should stage a queue property change', () => {
        const store = createTestStore();

        store.getState().stageQueueChange('root.default', 'capacity', '60');

        expect(store.getState().stagedChanges).toHaveLength(1);
        expect(store.getState().stagedChanges[0]).toMatchObject({
          type: 'update',
          queuePath: 'root.default',
          property: 'capacity',
          oldValue: undefined,
          newValue: '60',
        });
        expect(store.getState().stagedChanges[0].id).toBeDefined();
      });

      it('should update existing staged change for same property', () => {
        const store = createTestStore();

        store.getState().stageQueueChange('root.default', 'capacity', '60');
        store.getState().stageQueueChange('root.default', 'capacity', '70');

        expect(store.getState().stagedChanges).toHaveLength(1);
        expect(store.getState().stagedChanges[0].newValue).toBe('70');
      });
    });

    describe('stageGlobalChange', () => {
      it('should stage a global property change', () => {
        const store = createTestStore();

        store.getState().stageGlobalChange(MAXIMUM_APPLICATIONS_PROPERTY, '15000');

        expect(store.getState().stagedChanges).toHaveLength(1);
        expect(store.getState().stagedChanges[0]).toMatchObject({
          type: 'update',
          queuePath: 'global',
          property: MAXIMUM_APPLICATIONS_PROPERTY,
          oldValue: undefined,
          newValue: '15000',
        });
      });

      it('should stringify object values for placement rules JSON property', () => {
        const store = createTestStore();

        const placementRules = [
          { type: 'user', matches: 'admin*', policy: 'specified', value: 'root.admin' },
          { type: 'group', matches: 'production', policy: 'specified', value: 'root.production' },
        ];

        store
          .getState()
          .stageGlobalChange(SPECIAL_VALUES.MAPPING_RULE_JSON_PROPERTY, placementRules);

        expect(store.getState().stagedChanges).toHaveLength(1);
        expect(store.getState().stagedChanges[0]).toMatchObject({
          type: 'update',
          queuePath: 'global',
          property: SPECIAL_VALUES.MAPPING_RULE_JSON_PROPERTY,
          oldValue: undefined,
          newValue: JSON.stringify(placementRules),
        });
      });

      it('should handle string values for placement rules JSON property', () => {
        const store = createTestStore();

        const placementRulesString = '[{"type":"user","matches":"*","policy":"primaryGroup"}]';

        store
          .getState()
          .stageGlobalChange(SPECIAL_VALUES.MAPPING_RULE_JSON_PROPERTY, placementRulesString);

        expect(store.getState().stagedChanges).toHaveLength(1);
        expect(store.getState().stagedChanges[0]).toMatchObject({
          type: 'update',
          queuePath: 'global',
          property: SPECIAL_VALUES.MAPPING_RULE_JSON_PROPERTY,
          oldValue: undefined,
          newValue: placementRulesString,
        });
      });
    });

    describe('stageQueueAddition', () => {
      it('should stage queue addition with properties', () => {
        const store = createTestStore();

        const queueConfig = {
          capacity: '20',
          'maximum-capacity': '50',
          state: 'RUNNING',
        };

        store.getState().stageQueueAddition('root.production', 'team2', queueConfig);

        expect(store.getState().stagedChanges).toHaveLength(3); // Multiple changes for each property

        const changes = store.getState().stagedChanges;
        expect(changes.every((c) => c.type === 'add')).toBe(true);
        expect(changes.every((c) => c.queuePath === 'root.production.team2')).toBe(true);

        const capacityChange = changes.find((c) => c.property === 'capacity');
        expect(capacityChange?.newValue).toBe('20');

        const maxCapacityChange = changes.find((c) => c.property === 'maximum-capacity');
        expect(maxCapacityChange?.newValue).toBe('50');

        const stateChange = changes.find((c) => c.property === 'state');
        expect(stateChange?.newValue).toBe('RUNNING');
      });

      it('should attach validation errors when addition breaks parent capacity sum', async () => {
        const store = createTestStore();
        await setupStoreWithData(store);

        store.getState().stageQueueAddition('root', 'analytics', {
          capacity: '10',
          'maximum-capacity': '100',
          state: 'RUNNING',
        });

        const capacityChange = store
          .getState()
          .stagedChanges.find(
            (change) => change.queuePath === 'root.analytics' && change.property === 'capacity',
          );

        expect(capacityChange?.validationErrors).toBeDefined();
        expect(
          capacityChange?.validationErrors?.some((error) => error.rule === 'child-capacity-sum'),
        ).toBe(true);
      });

      it('should update staged sibling errors when multiple additions are staged', async () => {
        const store = createTestStore();
        await setupStoreWithData(store);

        store.getState().stageQueueAddition('root', 'analytics', {
          capacity: '10',
          'maximum-capacity': '100',
          state: 'RUNNING',
        });

        const firstAddition = store
          .getState()
          .stagedChanges.find(
            (change) => change.queuePath === 'root.analytics' && change.property === 'capacity',
          );

        expect(firstAddition?.validationErrors?.[0].message).toContain('110.0%');

        store.getState().stageQueueAddition('root', 'ml', {
          capacity: '10',
          'maximum-capacity': '100',
          state: 'RUNNING',
        });

        const updatedFirstAddition = store
          .getState()
          .stagedChanges.find(
            (change) => change.queuePath === 'root.analytics' && change.property === 'capacity',
          );

        expect(updatedFirstAddition?.validationErrors?.[0].message).toContain('120.0%');
      });
    });

    describe('stageQueueRemoval', () => {
      it('should stage queue removal', () => {
        const store = createTestStore();

        store.getState().stageQueueRemoval('root.production.batch');

        expect(store.getState().stagedChanges).toHaveLength(1);
        expect(store.getState().stagedChanges[0]).toMatchObject({
          type: 'remove',
          queuePath: 'root.production.batch',
          property: '__queue__',
        });
      });
    });

    describe('stageLabelQueueChange', () => {
      it('should stage node label property change', () => {
        const store = createTestStore();

        store.getState().stageLabelQueueChange('root.default', 'gpu', 'capacity', '30');

        expect(store.getState().stagedChanges).toHaveLength(1);
        expect(store.getState().stagedChanges[0]).toMatchObject({
          type: 'update',
          queuePath: 'root.default',
          property: 'accessible-node-labels.gpu.capacity',
          oldValue: undefined,
          newValue: '30',
        });
      });
    });
  });

  describe('change management', () => {
    describe('revertChange', () => {
      it('should remove staged change by id', () => {
        const store = createTestStore();

        store.getState().stageQueueChange('root.default', 'capacity', '60');

        const changeId = store.getState().stagedChanges[0].id;

        store.getState().revertChange(changeId);

        expect(store.getState().stagedChanges).toHaveLength(0);
      });

      it('should not affect other staged changes', () => {
        const store = createTestStore();

        store.getState().stageQueueChange('root.default', 'capacity', '60');
        store.getState().stageQueueChange('root.production', 'capacity', '40');

        const firstChangeId = store.getState().stagedChanges[0].id;

        store.getState().revertChange(firstChangeId);

        expect(store.getState().stagedChanges).toHaveLength(1);
        expect(store.getState().stagedChanges[0].queuePath).toBe('root.production');
      });
    });

    describe('revertQueueDeletion', () => {
      it('should revert a pending queue deletion', () => {
        const store = createTestStore();
        store.getState().stageQueueRemoval('root.test');

        expect(store.getState().hasPendingDeletion('root.test')).toBe(true);

        store.getState().revertQueueDeletion('root.test');

        expect(store.getState().hasPendingDeletion('root.test')).toBe(false);
        expect(store.getState().stagedChanges).toHaveLength(0);
      });

      it('should not affect other staged changes', () => {
        const store = createTestStore();
        store.getState().stageQueueChange('root.other', 'capacity', '50');
        store.getState().stageQueueRemoval('root.test');

        store.getState().revertQueueDeletion('root.test');

        expect(store.getState().hasPendingDeletion('root.test')).toBe(false);
        expect(store.getState().stagedChanges).toHaveLength(1);
        expect(store.getState().stagedChanges[0].queuePath).toBe('root.other');
      });

      it('should be a no-op if queue has no pending deletion', () => {
        const store = createTestStore();
        store.getState().stageQueueChange('root.test', 'capacity', '50');

        store.getState().revertQueueDeletion('root.test');

        expect(store.getState().stagedChanges).toHaveLength(1);
      });
    });

    describe('clearAllChanges', () => {
      it('should clear all staged changes', () => {
        const store = createTestStore();

        store.getState().stageQueueChange('root.default', 'capacity', '60');
        store.getState().stageGlobalChange(MAXIMUM_APPLICATIONS_PROPERTY, '15000');

        expect(store.getState().stagedChanges).toHaveLength(2);

        store.getState().clearAllChanges();

        expect(store.getState().stagedChanges).toHaveLength(0);
      });
    });
  });

  describe('applyChanges', () => {
    it('should send mutation request and reload data on success', async () => {
      const store = createTestStore();
      const mockApiClient = vi.mocked(store.getState().apiClient);

      // Stage some changes
      store.getState().stageQueueChange('root.default', 'capacity', '60');
      store.getState().stageGlobalChange(MAXIMUM_APPLICATIONS_PROPERTY, '15000');

      // Mock successful mutation response
      mockApiClient.validateSchedulerConf.mockResolvedValue({
        validation: 'success',
        versionId: 12345000,
      });
      mockApiClient.validateSchedulerConf.mockResolvedValue({
        validation: 'success',
        versionId: 12345000,
      });
      mockApiClient.updateSchedulerConf.mockResolvedValue(undefined);

      // Mock reload calls
      mockApiClient.getScheduler.mockResolvedValue(mockSchedulerResponse);
      mockApiClient.getSchedulerConf.mockResolvedValue(mockConfigResponse);
      mockApiClient.getNodeLabels.mockResolvedValue(mockNodeLabelsResponse);
      mockApiClient.getNodes.mockResolvedValue(mockNodesResponse);
      mockApiClient.getNodeToLabels.mockResolvedValue(mockNodeToLabelsResponse);
      mockApiClient.getSchedulerConfVersion.mockResolvedValue({ versionId: 1234567891 });

      await store.getState().applyChanges();

      // Check mutation request was sent
      expect(mockApiClient.validateSchedulerConf).toHaveBeenCalledTimes(1);
      expect(mockApiClient.updateSchedulerConf).toHaveBeenCalledTimes(1);

      const updatePayload = mockApiClient.updateSchedulerConf.mock.calls[0]?.[0];
      expect(updatePayload).toBeDefined();
      expect(updatePayload?.['global-updates']).toEqual(
        expect.arrayContaining([
          expect.objectContaining({
            entry: expect.arrayContaining([
              expect.objectContaining({
                key: 'yarn.webservice.mutation-api.version',
                value: '12345000',
              }),
            ]),
          }),
        ]),
      );

      // Check staged changes were cleared
      expect(store.getState().stagedChanges).toHaveLength(0);
      // Version should be incremented from initial load
      expect(store.getState().configVersion).toBe(1234567891);
    });

    it('should handle mutation failures without clearing changes', async () => {
      const store = createTestStore();
      const mockApiClient = vi.mocked(store.getState().apiClient);

      store.getState().stageQueueChange('root.default', 'capacity', '60');

      mockApiClient.validateSchedulerConf.mockResolvedValue({ validation: 'success' });
      mockApiClient.updateSchedulerConf.mockRejectedValue(
        new Error('HTTP 400: Invalid configuration'),
      );

      // Call applyChanges and expect it to throw
      let error: unknown;
      try {
        await store.getState().applyChanges();
      } catch (e) {
        error = e;
      }

      expect(error).toBeDefined();

      expect(mockApiClient.validateSchedulerConf).toHaveBeenCalledTimes(1);

      // Changes should not be cleared on failure
      expect(store.getState().stagedChanges).toHaveLength(1);
      expect(store.getState().error).toBe('HTTP 400: Invalid configuration');
      expect(store.getState().errorContext).toBe('mutation');
      expect(store.getState().applyError).toBe('HTTP 400: Invalid configuration');
    });

    it('should clear mutation error when staged changes are modified after a failure', async () => {
      const store = createTestStore();
      const mockApiClient = vi.mocked(store.getState().apiClient);

      store.getState().stageQueueChange('root.default', 'capacity', '60');

      mockApiClient.validateSchedulerConf.mockResolvedValue({ validation: 'success' });
      mockApiClient.updateSchedulerConf.mockRejectedValue(
        new Error('HTTP 400: Invalid configuration'),
      );

      await expect(store.getState().applyChanges()).rejects.toBeDefined();

      expect(store.getState().applyError).toBe('HTTP 400: Invalid configuration');

      store.getState().stageQueueChange('root.default', 'capacity', '61');

      expect(store.getState().applyError).toBeNull();
      expect(store.getState().error).toBeNull();
      expect(store.getState().errorContext).toBeNull();
    });

    it('should stop and restart parent queues when adding a new queue', async () => {
      const store = createTestStore();
      const mockApiClient = vi.mocked(store.getState().apiClient);

      await setupStoreWithData(store);

      store.getState().stageQueueAddition('root.production', 'analytics', {
        capacity: '10',
        state: 'RUNNING',
      });

      mockApiClient.validateSchedulerConf.mockResolvedValue({
        validation: 'success',
        versionId: 98765,
      });
      mockApiClient.updateSchedulerConf.mockResolvedValue(undefined);

      mockApiClient.getScheduler.mockResolvedValue(mockSchedulerResponse);
      mockApiClient.getSchedulerConf.mockResolvedValue(mockConfigResponse);
      mockApiClient.getNodeLabels.mockResolvedValue(mockNodeLabelsResponse);
      mockApiClient.getNodes.mockResolvedValue(mockNodesResponse);
      mockApiClient.getNodeToLabels.mockResolvedValue(mockNodeToLabelsResponse);
      mockApiClient.getSchedulerConfVersion.mockResolvedValue({ versionId: 1234567892 });

      await store.getState().applyChanges();

      expect(mockApiClient.validateSchedulerConf).toHaveBeenCalledTimes(1);
      expect(mockApiClient.updateSchedulerConf).toHaveBeenCalledTimes(4);

      const [stopPayload, finalPayload, restartPayload, childStartPayload] =
        mockApiClient.updateSchedulerConf.mock.calls.map(([payload]) => payload);

      expect(stopPayload?.['update-queue']).toHaveLength(3);
      expect(stopPayload?.['update-queue']).toEqual(
        expect.arrayContaining([
          expect.objectContaining({
            'queue-name': 'root.production',
            params: {
              entry: [{ key: 'state', value: 'STOPPED' }],
            },
          }),
          expect.objectContaining({
            'queue-name': 'root.production.batch',
            params: {
              entry: [{ key: 'state', value: 'STOPPED' }],
            },
          }),
          expect.objectContaining({
            'queue-name': 'root.production.interactive',
            params: {
              entry: [{ key: 'state', value: 'STOPPED' }],
            },
          }),
        ]),
      );

      expect(finalPayload?.['add-queue']).toBeDefined();
      expect(finalPayload?.['global-updates']).toEqual(
        expect.arrayContaining([
          expect.objectContaining({
            entry: expect.arrayContaining([
              expect.objectContaining({
                key: 'yarn.webservice.mutation-api.version',
                value: '98765',
              }),
            ]),
          }),
        ]),
      );

      expect(restartPayload?.['update-queue']).toHaveLength(3);
      expect(restartPayload?.['update-queue']).toEqual(
        expect.arrayContaining([
          expect.objectContaining({
            'queue-name': 'root.production',
            params: {
              entry: [{ key: 'state', value: 'RUNNING' }],
            },
          }),
          expect.objectContaining({
            'queue-name': 'root.production.batch',
            params: {
              entry: [{ key: 'state', value: 'RUNNING' }],
            },
          }),
          expect.objectContaining({
            'queue-name': 'root.production.interactive',
            params: {
              entry: [{ key: 'state', value: 'RUNNING' }],
            },
          }),
        ]),
      );

      expect(childStartPayload).toEqual({
        'update-queue': [
          {
            'queue-name': 'root.production.analytics',
            params: {
              entry: [{ key: 'state', value: 'RUNNING' }],
            },
          },
        ],
      });
    });

    it('should stop queue hierarchies depth-first when enabling auto creation and restart breadth-first', async () => {
      const store = createTestStore();
      const mockApiClient = vi.mocked(store.getState().apiClient);

      await setupStoreWithData(store);

      store
        .getState()
        .stageQueueChange('root.production', AUTO_CREATION_PROPS.LEGACY_ENABLED, 'true');

      mockApiClient.validateSchedulerConf.mockResolvedValue({
        validation: 'success',
        versionId: 24680,
      });
      mockApiClient.updateSchedulerConf.mockResolvedValue(undefined);

      mockApiClient.getScheduler.mockResolvedValue(mockSchedulerResponse);
      mockApiClient.getSchedulerConf.mockResolvedValue(mockConfigResponse);
      mockApiClient.getNodeLabels.mockResolvedValue(mockNodeLabelsResponse);
      mockApiClient.getNodes.mockResolvedValue(mockNodesResponse);
      mockApiClient.getNodeToLabels.mockResolvedValue(mockNodeToLabelsResponse);
      mockApiClient.getSchedulerConfVersion.mockResolvedValue({ versionId: 1234567895 });

      await store.getState().applyChanges();

      expect(mockApiClient.updateSchedulerConf).toHaveBeenCalledTimes(3);

      const [stopPayload, autoCreationUpdatePayload, restartPayload] =
        mockApiClient.updateSchedulerConf.mock.calls.map(([payload]) => payload);

      expect(stopPayload?.['update-queue']).toHaveLength(3);
      expect(stopPayload?.['update-queue']).toEqual(
        expect.arrayContaining([
          expect.objectContaining({
            'queue-name': 'root.production',
            params: {
              entry: [{ key: 'state', value: 'STOPPED' }],
            },
          }),
          expect.objectContaining({
            'queue-name': 'root.production.batch',
            params: {
              entry: [{ key: 'state', value: 'STOPPED' }],
            },
          }),
          expect.objectContaining({
            'queue-name': 'root.production.interactive',
            params: {
              entry: [{ key: 'state', value: 'STOPPED' }],
            },
          }),
        ]),
      );

      expect(autoCreationUpdatePayload?.['update-queue']).toEqual(
        expect.arrayContaining([
          expect.objectContaining({
            'queue-name': 'root.production',
            params: {
              entry: expect.arrayContaining([
                expect.objectContaining({
                  key: AUTO_CREATION_PROPS.LEGACY_ENABLED,
                  value: 'true',
                }),
              ]),
            },
          }),
        ]),
      );
      expect(autoCreationUpdatePayload?.['global-updates']).toEqual(
        expect.arrayContaining([
          expect.objectContaining({
            entry: expect.arrayContaining([
              expect.objectContaining({
                key: 'yarn.webservice.mutation-api.version',
                value: '24680',
              }),
            ]),
          }),
        ]),
      );

      expect(restartPayload?.['update-queue']).toHaveLength(3);
      expect(restartPayload?.['update-queue']).toEqual(
        expect.arrayContaining([
          expect.objectContaining({
            'queue-name': 'root.production',
            params: {
              entry: [{ key: 'state', value: 'RUNNING' }],
            },
          }),
          expect.objectContaining({
            'queue-name': 'root.production.batch',
            params: {
              entry: [{ key: 'state', value: 'RUNNING' }],
            },
          }),
          expect.objectContaining({
            'queue-name': 'root.production.interactive',
            params: {
              entry: [{ key: 'state', value: 'RUNNING' }],
            },
          }),
        ]),
      );
    });
    it('should stop and restart queues when enabling flexible auto creation', async () => {
      const store = createTestStore();
      const mockApiClient = vi.mocked(store.getState().apiClient);

      await setupStoreWithData(store);

      store
        .getState()
        .stageQueueChange('root.production', AUTO_CREATION_PROPS.FLEXIBLE_ENABLED, 'true');

      mockApiClient.validateSchedulerConf.mockResolvedValue({
        validation: 'success',
        versionId: 97531,
      });
      mockApiClient.updateSchedulerConf.mockResolvedValue(undefined);

      mockApiClient.getScheduler.mockResolvedValue(mockSchedulerResponse);
      mockApiClient.getSchedulerConf.mockResolvedValue(mockConfigResponse);
      mockApiClient.getNodeLabels.mockResolvedValue(mockNodeLabelsResponse);
      mockApiClient.getNodes.mockResolvedValue(mockNodesResponse);
      mockApiClient.getNodeToLabels.mockResolvedValue(mockNodeToLabelsResponse);
      mockApiClient.getSchedulerConfVersion.mockResolvedValue({ versionId: 1234567896 });

      await store.getState().applyChanges();

      expect(mockApiClient.updateSchedulerConf).toHaveBeenCalledTimes(3);

      const [stopPayload, autoCreationUpdatePayload, restartPayload] =
        mockApiClient.updateSchedulerConf.mock.calls.map(([payload]) => payload);

      // Verify stop payload
      expect(stopPayload?.['update-queue']).toHaveLength(3);
      expect(stopPayload?.['update-queue']).toEqual(
        expect.arrayContaining([
          expect.objectContaining({
            'queue-name': 'root.production',
            params: {
              entry: [{ key: 'state', value: 'STOPPED' }],
            },
          }),
          expect.objectContaining({
            'queue-name': 'root.production.batch',
            params: {
              entry: [{ key: 'state', value: 'STOPPED' }],
            },
          }),
          expect.objectContaining({
            'queue-name': 'root.production.interactive',
            params: {
              entry: [{ key: 'state', value: 'STOPPED' }],
            },
          }),
        ]),
      );

      // Verify auto-creation update payload
      expect(autoCreationUpdatePayload?.['update-queue']).toEqual(
        expect.arrayContaining([
          expect.objectContaining({
            'queue-name': 'root.production',
            params: {
              entry: expect.arrayContaining([
                expect.objectContaining({
                  key: AUTO_CREATION_PROPS.FLEXIBLE_ENABLED,
                  value: 'true',
                }),
              ]),
            },
          }),
        ]),
      );

      // Verify restart payload
      expect(restartPayload?.['update-queue']).toHaveLength(3);
      expect(restartPayload?.['update-queue']).toEqual(
        expect.arrayContaining([
          expect.objectContaining({
            'queue-name': 'root.production',
            params: {
              entry: [{ key: 'state', value: 'RUNNING' }],
            },
          }),
          expect.objectContaining({
            'queue-name': 'root.production.batch',
            params: {
              entry: [{ key: 'state', value: 'RUNNING' }],
            },
          }),
          expect.objectContaining({
            'queue-name': 'root.production.interactive',
            params: {
              entry: [{ key: 'state', value: 'RUNNING' }],
            },
          }),
        ]),
      );
    });

    it('should stop a queue before removing it', async () => {
      const store = createTestStore();
      const mockApiClient = vi.mocked(store.getState().apiClient);

      store.getState().stageQueueRemoval('root.production.batch');

      mockApiClient.validateSchedulerConf.mockResolvedValue({
        validation: 'success',
        versionId: 13579,
      });
      mockApiClient.updateSchedulerConf.mockResolvedValue(undefined);

      mockApiClient.getScheduler.mockResolvedValue(mockSchedulerResponse);
      mockApiClient.getSchedulerConf.mockResolvedValue(mockConfigResponse);
      mockApiClient.getNodeLabels.mockResolvedValue(mockNodeLabelsResponse);
      mockApiClient.getNodes.mockResolvedValue(mockNodesResponse);
      mockApiClient.getNodeToLabels.mockResolvedValue(mockNodeToLabelsResponse);
      mockApiClient.getSchedulerConfVersion.mockResolvedValue({ versionId: 1234567893 });

      await store.getState().applyChanges();

      expect(mockApiClient.validateSchedulerConf).toHaveBeenCalledTimes(1);
      expect(mockApiClient.updateSchedulerConf).toHaveBeenCalledTimes(2);

      const stopPayload = mockApiClient.updateSchedulerConf.mock.calls[0][0];
      expect(stopPayload).toEqual({
        'update-queue': [
          {
            'queue-name': 'root.production.batch',
            params: {
              entry: [{ key: 'state', value: 'STOPPED' }],
            },
          },
        ],
      });

      const removalPayload = mockApiClient.updateSchedulerConf.mock.calls[1][0];
      expect(removalPayload?.['remove-queue']).toBe('root.production.batch');
      expect(removalPayload?.['global-updates']).toEqual(
        expect.arrayContaining([
          expect.objectContaining({
            entry: expect.arrayContaining([
              expect.objectContaining({
                key: 'yarn.webservice.mutation-api.version',
                value: '13579',
              }),
            ]),
          }),
        ]),
      );
    });
  });

  describe('computed values', () => {
    describe('getQueuePropertyValue', () => {
      it('should return configured value with staged flag false', async () => {
        const store = createTestStore();
        await setupStoreWithData(store);

        const displayValue = store.getState().getQueuePropertyValue('root.default', 'capacity');
        expect(displayValue).toEqual({
          value: '10',
          isStaged: false,
        });
      });

      it('should return staged value with staged flag true', async () => {
        const store = createTestStore();
        await setupStoreWithData(store);

        store.getState().stageQueueChange('root.default', 'capacity', '60');

        const displayValue = store.getState().getQueuePropertyValue('root.default', 'capacity');
        expect(displayValue).toEqual({
          value: '60',
          isStaged: true,
        });
      });
    });

    describe('getGlobalPropertyValue', () => {
      it('should return configured value with staged flag false', async () => {
        const store = createTestStore();
        await setupStoreWithData(store);

        const displayValue = store.getState().getGlobalPropertyValue(MAXIMUM_APPLICATIONS_PROPERTY);
        expect(displayValue).toEqual({
          value: '10000',
          isStaged: false,
        });
      });

      it('should return staged value with staged flag true', async () => {
        const store = createTestStore();
        await setupStoreWithData(store);

        store.getState().stageGlobalChange(MAXIMUM_APPLICATIONS_PROPERTY, '15000');

        const displayValue = store.getState().getGlobalPropertyValue(MAXIMUM_APPLICATIONS_PROPERTY);
        expect(displayValue).toEqual({
          value: '15000',
          isStaged: true,
        });
      });
    });

    describe('getGlobalDisplayValue', () => {
      it('should return empty value with staged flag false for unconfigured global property', async () => {
        const store = createTestStore();
        await setupStoreWithData(store);

        const displayValue = store.getState().getGlobalPropertyValue('non-existent-property');
        expect(displayValue).toEqual({
          value: '',
          isStaged: false,
        });
      });

      it('should return configured global value with staged flag false', async () => {
        const store = createTestStore();
        await setupStoreWithData(store);

        // The mockConfigResponse already includes 'yarn.scheduler.capacity.maximum-applications': '10000'
        const displayValue = store.getState().getGlobalPropertyValue(MAXIMUM_APPLICATIONS_PROPERTY);
        expect(displayValue).toEqual({
          value: '10000',
          isStaged: false,
        });
      });

      it('should return staged global value with staged flag true', async () => {
        const store = createTestStore();
        await setupStoreWithData(store);

        // Stage a global change
        store.getState().stageGlobalChange(MAXIMUM_APPLICATIONS_PROPERTY, '15000');

        const displayValue = store.getState().getGlobalPropertyValue(MAXIMUM_APPLICATIONS_PROPERTY);
        expect(displayValue).toEqual({
          value: '15000',
          isStaged: true,
        });
      });

      it('should prioritize staged value over configured value', async () => {
        const store = createTestStore();
        await setupStoreWithData(store);

        // mockConfigResponse already has 'yarn.scheduler.capacity.maximum-applications': '10000'
        // Stage a different value
        store.getState().stageGlobalChange(MAXIMUM_APPLICATIONS_PROPERTY, '20000');

        const displayValue = store.getState().getGlobalPropertyValue(MAXIMUM_APPLICATIONS_PROPERTY);
        expect(displayValue).toEqual({
          value: '20000',
          isStaged: true,
        });
      });

      it('should handle multiple global properties independently', async () => {
        const store = createTestStore();
        await setupStoreWithData(store);

        // mockConfigResponse has 'yarn.scheduler.capacity.maximum-applications': '10000'
        // Stage a change for a different property
        store.getState().stageGlobalChange(LEGACY_MODE_PROPERTY, 'false');

        const maxAppsValue = store.getState().getGlobalPropertyValue(MAXIMUM_APPLICATIONS_PROPERTY);
        const legacyModeValue = store.getState().getGlobalPropertyValue(LEGACY_MODE_PROPERTY);

        expect(maxAppsValue).toEqual({
          value: '10000',
          isStaged: false,
        });
        expect(legacyModeValue).toEqual({
          value: 'false',
          isStaged: true,
        });
      });
    });

    describe('getLabelChangesForQueue', () => {
      it('should return label-specific changes for a queue', () => {
        const store = createTestStore();

        store.getState().stageLabelQueueChange('root.default', 'gpu', 'capacity', '30');
        store.getState().stageLabelQueueChange('root.default', 'gpu', 'maximum-capacity', '80');
        store.getState().stageLabelQueueChange('root.default', 'ssd', 'capacity', '40');

        const gpuChanges = store.getState().getLabelChangesForQueue('root.default', 'gpu');
        expect(gpuChanges).toHaveLength(2);
        expect(gpuChanges.every((c) => c.property?.includes('gpu'))).toBe(true);

        const ssdChanges = store.getState().getLabelChangesForQueue('root.default', 'ssd');
        expect(ssdChanges).toHaveLength(1);
        expect(ssdChanges[0].property).toContain('ssd');
      });
    });

    describe('getQueueByPath', () => {
      it('should find queue by path in tree', async () => {
        const store = createTestStore();
        await setupStoreWithData(store);

        const rootQueue = store.getState().getQueueByPath('root');
        expect(rootQueue).toBeDefined();
        expect(rootQueue?.queueName).toBe('root');

        const defaultQueue = store.getState().getQueueByPath('root.default');
        expect(defaultQueue).toBeDefined();
        expect(defaultQueue?.queueName).toBe('default');

        const nonExistent = store.getState().getQueueByPath('root.nonexistent');
        expect(nonExistent).toBeNull();
      });

      it('should return null when no scheduler data', () => {
        const store = createTestStore();

        const queue = store.getState().getQueueByPath('root');
        expect(queue).toBeNull();
      });
    });

    describe('getChildQueues', () => {
      it('should return child queues for parent', async () => {
        const store = createTestStore();
        await setupStoreWithData(store);

        const rootChildren = store.getState().getChildQueues('root');
        expect(rootChildren).toHaveLength(3); // default, production, development
        expect(rootChildren.map((q) => q.queueName)).toContain('default');
        expect(rootChildren.map((q) => q.queueName)).toContain('production');
        expect(rootChildren.map((q) => q.queueName)).toContain('development');

        const productionChildren = store.getState().getChildQueues('root.production');
        expect(productionChildren).toHaveLength(2); // batch and interactive
      });

      it('should return empty array for leaf queues', async () => {
        const store = createTestStore();
        await setupStoreWithData(store);

        const leafChildren = store.getState().getChildQueues('root.default');
        expect(leafChildren).toHaveLength(0);
      });

      it('should return empty array for non-existent queue', async () => {
        const store = createTestStore();
        await setupStoreWithData(store);

        const children = store.getState().getChildQueues('root.nonexistent');
        expect(children).toHaveLength(0);
      });
    });

    describe('hasUnsavedChanges', () => {
      it('should return false when no staged changes', () => {
        const store = createTestStore();

        expect(store.getState().hasUnsavedChanges()).toBe(false);
      });

      it('should return true when staged changes exist', () => {
        const store = createTestStore();

        store.getState().stageQueueChange('root.default', 'capacity', '60');

        expect(store.getState().hasUnsavedChanges()).toBe(true);
      });

      it('should return false after clearing changes', () => {
        const store = createTestStore();

        store.getState().stageQueueChange('root.default', 'capacity', '60');
        store.getState().clearAllChanges();

        expect(store.getState().hasUnsavedChanges()).toBe(false);
      });
    });

    describe('getChangesForQueue', () => {
      it('should return all changes for specific queue', () => {
        const store = createTestStore();

        store.getState().stageQueueChange('root.default', 'capacity', '60');
        store.getState().stageQueueChange('root.default', 'maximum-capacity', '90');
        store.getState().stageQueueChange('root.production', 'capacity', '40');

        const defaultChanges = store.getState().getChangesForQueue('root.default');
        expect(defaultChanges).toHaveLength(2);
        expect(defaultChanges.every((c) => c.queuePath === 'root.default')).toBe(true);

        const productionChanges = store.getState().getChangesForQueue('root.production');
        expect(productionChanges).toHaveLength(1);
        expect(productionChanges[0].queuePath).toBe('root.production');
      });

      it('should return empty array for queue with no changes', () => {
        const store = createTestStore();

        const changes = store.getState().getChangesForQueue('root.batch');
        expect(changes).toHaveLength(0);
      });
    });

    describe('getStagedChangeById', () => {
      it('should find staged change by id', () => {
        const store = createTestStore();

        store.getState().stageQueueChange('root.default', 'capacity', '60');
        const changeId = store.getState().stagedChanges[0].id;

        const change = store.getState().getStagedChangeById(changeId);
        expect(change).toBeDefined();
        expect(change?.queuePath).toBe('root.default');
        expect(change?.property).toBe('capacity');
      });

      it('should return undefined for non-existent id', () => {
        const store = createTestStore();

        store.getState().stageQueueChange('root.default', 'capacity', '60');

        const change = store.getState().getStagedChangeById('non-existent-id');
        expect(change).toBeUndefined();
      });
    });
  });

  describe('node label selection', () => {
    it('should set selected node label', () => {
      const store = createTestStore();

      store.getState().selectNodeLabel('gpu');

      expect(store.getState().selectedNodeLabel).toBe('gpu');
    });

    it('should clear selected node label', () => {
      const store = createTestStore();

      store.getState().selectNodeLabel('gpu');
      store.getState().selectNodeLabel(null);

      expect(store.getState().selectedNodeLabel).toBeNull();
    });
  });
});

describe('utility functions', () => {
  describe('traverseQueueTree', () => {
    it('should traverse queue tree and combine with config data', () => {
      const queueInfo: QueueInfo = {
        queueType: 'parent',
        capacity: 100,
        usedCapacity: 45,
        maxCapacity: 100,
        absoluteCapacity: 100,
        absoluteMaxCapacity: 100,
        absoluteUsedCapacity: 45,
        numApplications: 5,
        numActiveApplications: 3,
        numPendingApplications: 2,
        queueName: 'root',
        queuePath: 'root',
        state: 'RUNNING',
        queues: {
          queue: [
            {
              queueType: QUEUE_TYPES.LEAF,
              capacity: 50,
              usedCapacity: 80,
              maxCapacity: 100,
              absoluteCapacity: 50,
              absoluteMaxCapacity: 100,
              absoluteUsedCapacity: 40,
              numApplications: 3,
              numActiveApplications: 2,
              numPendingApplications: 1,
              queueName: 'default',
              queuePath: 'root.default',
              state: 'RUNNING',
            },
          ],
        },
      };

      const configData = new Map([
        ['yarn.scheduler.capacity.root.capacity', '100'],
        ['yarn.scheduler.capacity.root.default.capacity', '10'],
        ['yarn.scheduler.capacity.root.default.maximum-capacity', '100'],
      ]);

      const visitedQueues: any[] = [];
      const visitor = (queue: any) => visitedQueues.push(queue);

      traverseQueueTree(queueInfo, configData, visitor);

      expect(visitedQueues).toHaveLength(2); // root and default
      expect(visitedQueues[0]).toMatchObject({
        queueName: 'root',
        queuePath: 'root',
        configured: {
          capacity: '100',
        },
      });
      expect(visitedQueues[1]).toMatchObject({
        queueName: 'default',
        queuePath: 'root.default',
        configured: {
          capacity: '10',
          'maximum-capacity': '100',
        },
      });
    });
  });

  describe('buildMutationRequest', () => {
    it('should build mutation request from staged changes', () => {
      const stagedChanges: StagedChange[] = [
        {
          id: '1',
          type: 'update',
          queuePath: 'root.default',
          property: 'capacity',
          oldValue: '50',
          newValue: '60',
          timestamp: Date.now(),
        },
        {
          id: '2',
          type: 'update',
          queuePath: 'global',
          property: MAXIMUM_APPLICATIONS_PROPERTY,
          oldValue: '10000',
          newValue: '15000',
          timestamp: Date.now(),
        },
        {
          id: '3',
          type: 'add',
          queuePath: 'root.test',
          property: 'capacity',
          newValue: '20',
          timestamp: Date.now(),
        },
        {
          id: '4',
          type: 'remove',
          queuePath: 'root.old',
          property: '__queue__',
          oldValue: undefined,
          newValue: undefined,
          timestamp: Date.now(),
        },
      ];

      const request = buildMutationRequest(stagedChanges);

      const updateQueues = request['update-queue'] ?? [];
      expect(updateQueues).toHaveLength(1);
      expect(updateQueues[0]['queue-name']).toBe('root.default');
      expect(toEntryRecord(updateQueues[0].params.entry)).toEqual({ capacity: '60' });

      const addQueues = request['add-queue'] ?? [];
      expect(addQueues).toHaveLength(1);
      expect(addQueues[0]['queue-name']).toBe('root.test');
      expect(toEntryRecord(addQueues[0].params.entry)).toEqual({ capacity: '20' });

      expect(request['remove-queue']).toBe('root.old');
      expect(toEntryRecord(request['global-updates']?.[0].entry)).toEqual({
        'yarn.scheduler.capacity.maximum-applications': '15000',
      });
    });

    it('should group multiple changes for same queue', () => {
      const stagedChanges: StagedChange[] = [
        {
          id: '1',
          type: 'update',
          queuePath: 'root.default',
          property: 'capacity',
          oldValue: '50',
          newValue: '60',
          timestamp: Date.now(),
        },
        {
          id: '2',
          type: 'update',
          queuePath: 'root.default',
          property: 'maximum-capacity',
          oldValue: '100',
          newValue: '90',
          timestamp: Date.now(),
        },
      ];

      const request = buildMutationRequest(stagedChanges);

      expect(request['update-queue']).toHaveLength(1);
      const params = request['update-queue']![0];
      expect(params['queue-name']).toBe('root.default');
      expect(toEntryRecord(params.params.entry)).toEqual({
        capacity: '60',
        'maximum-capacity': '90',
      });
    });
  });

  describe('queue selection', () => {
    it('should select a queue by path', async () => {
      const store = createTestStore();
      await setupStoreWithData(store);

      // Initially no queue is selected
      expect(store.getState().selectedQueuePath).toBeNull();

      // Select a queue
      store.getState().selectQueue('root.default');
      expect(store.getState().selectedQueuePath).toBe('root.default');

      // Select a different queue
      store.getState().selectQueue('root.production');
      expect(store.getState().selectedQueuePath).toBe('root.production');
    });

    it('should clear selection when null is passed', async () => {
      const store = createTestStore();
      await setupStoreWithData(store);

      // Select a queue first
      store.getState().selectQueue('root.default');
      expect(store.getState().selectedQueuePath).toBe('root.default');

      // Clear selection
      store.getState().selectQueue(null);
      expect(store.getState().selectedQueuePath).toBeNull();
    });

    it('should validate queue path exists before selecting', async () => {
      const store = createTestStore();
      await setupStoreWithData(store);

      // Try to select a non-existent queue
      store.getState().selectQueue('root.nonexistent');

      // Should not select invalid queue
      expect(store.getState().selectedQueuePath).toBeNull();
    });
  });
});
