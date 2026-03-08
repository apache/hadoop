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
import { createSchedulerDataSlice } from '~/stores/slices/schedulerDataSlice';
import type {
  SchedulerInfo,
  NodeLabelsResponse,
  NodeToLabelsResponse,
  ConfigProperty,
} from '~/types';
import { ERROR_CODES } from '~/lib/errors';

// Mock the error utilities
vi.mock('~/lib/errors', async () => {
  const actual = await vi.importActual('~/lib/errors');
  return {
    ...actual,
    createDetailedErrorMessage: vi.fn((context: string, error: any) => {
      return `Error during ${context}: ${error?.message || String(error)}`;
    }),
    isNetworkError: vi.fn((error: any) => error?.message?.includes('network')),
  };
});

describe('schedulerDataSlice', () => {
  const createMockApiClient = () => ({
    getScheduler: vi.fn(),
    getSchedulerConf: vi.fn(),
    getNodeLabels: vi.fn(),
    getNodes: vi.fn(),
    getNodeToLabels: vi.fn(),
    getSchedulerConfVersion: vi.fn(),
    getIsReadOnly: vi.fn(() => false),
  });

  const createMockSchedulerResponse = () => ({
    scheduler: {
      schedulerInfo: {
        type: 'capacityScheduler',
        capacity: 100,
        usedCapacity: 0,
        maxCapacity: 100,
        queueName: 'root',
        queues: { queue: [] },
      } as SchedulerInfo,
    },
  });

  const createMockConfigResponse = () => ({
    property: [
      { name: 'yarn.scheduler.capacity.root.queues', value: 'default' },
      { name: 'yarn.scheduler.capacity.root.default.capacity', value: '100' },
    ] as ConfigProperty[],
  });

  const createMockNodeLabelsResponse = (): NodeLabelsResponse => ({
    nodeLabelInfo: [
      { name: 'gpu', exclusivity: true },
      { name: 'cpu', exclusivity: false },
    ],
  });

  const createMockNodesResponse = () => ({
    nodes: {
      node: [
        { id: 'node1:8041', nodeHostName: 'node1' },
        { id: 'node2:8041', nodeHostName: 'node2' },
      ],
    },
  });

  const createMockNodeToLabelsResponse = (): NodeToLabelsResponse => ({
    nodeToLabels: {
      entry: [
        {
          key: 'node1:8041',
          value: {
            nodeLabelInfo: [{ name: 'gpu' }],
          },
        },
        {
          key: 'node2:8041',
          value: {
            nodeLabelInfo: { name: 'cpu' },
          },
        },
      ],
    },
  });

  const createMockVersionResponse = () => ({
    versionId: '1',
  });

  beforeEach(() => {
    vi.clearAllMocks();
  });

  describe('loadInitialData', () => {
    it('should successfully load all initial data', async () => {
      const mockApiClient = createMockApiClient();
      mockApiClient.getScheduler.mockResolvedValue(createMockSchedulerResponse());
      mockApiClient.getSchedulerConf.mockResolvedValue(createMockConfigResponse());
      mockApiClient.getNodeLabels.mockResolvedValue(createMockNodeLabelsResponse());
      mockApiClient.getNodes.mockResolvedValue(createMockNodesResponse());
      mockApiClient.getNodeToLabels.mockResolvedValue(createMockNodeToLabelsResponse());
      mockApiClient.getSchedulerConfVersion.mockResolvedValue(createMockVersionResponse());
      mockApiClient.getIsReadOnly.mockReturnValue(false);

      const state: any = {
        schedulerData: null,
        configData: new Map(),
        configVersion: 0,
        isLoading: false,
        error: null,
        errorContext: null,
        isReadOnly: false,
        nodeLabels: [],
        nodes: [],
        nodeToLabels: [],
        apiClient: mockApiClient,
      };

      const set = vi.fn((fn) => fn(state));
      const get = vi.fn(() => state);

      const slice = createSchedulerDataSlice(set as any, get as any, {} as any);
      await slice.loadInitialData();

      // Verify loading states
      expect(set).toHaveBeenCalled();

      // Verify API calls were made
      expect(mockApiClient.getScheduler).toHaveBeenCalledTimes(1);
      expect(mockApiClient.getSchedulerConf).toHaveBeenCalledTimes(1);
      expect(mockApiClient.getNodeLabels).toHaveBeenCalledTimes(1);
      expect(mockApiClient.getNodes).toHaveBeenCalledTimes(1);
      expect(mockApiClient.getNodeToLabels).toHaveBeenCalledTimes(1);
      expect(mockApiClient.getSchedulerConfVersion).toHaveBeenCalledTimes(1);

      // Verify state was updated
      expect(state.schedulerData).toBeDefined();
      expect(state.schedulerData.queueName).toBe('root');
      expect(state.configData.size).toBeGreaterThan(0);
      expect(state.configVersion).toBe('1');
      expect(state.isReadOnly).toBe(false);
      expect(state.isLoading).toBe(false);
      expect(state.error).toBe(null);
    });

    it('should set isLoading to true at the start', async () => {
      const mockApiClient = createMockApiClient();
      mockApiClient.getScheduler.mockResolvedValue(createMockSchedulerResponse());
      mockApiClient.getSchedulerConf.mockResolvedValue(createMockConfigResponse());
      mockApiClient.getNodeLabels.mockResolvedValue({ nodeLabelInfo: [] });
      mockApiClient.getNodes.mockResolvedValue({ nodes: { node: [] } });
      mockApiClient.getNodeToLabels.mockResolvedValue({});
      mockApiClient.getSchedulerConfVersion.mockResolvedValue(createMockVersionResponse());

      const state: any = {
        schedulerData: null,
        isLoading: false,
        error: null,
        errorContext: null,
        apiClient: mockApiClient,
        configData: new Map(),
        nodeLabels: [],
        nodes: [],
        nodeToLabels: [],
      };

      const set = vi.fn((fn) => fn(state));
      const get = vi.fn(() => state);

      const slice = createSchedulerDataSlice(set as any, get as any, {} as any);
      await slice.loadInitialData();

      // Check that the first call to set() updated isLoading
      expect(set).toHaveBeenCalled();
      const firstSetCall = set.mock.calls[0][0];
      const tempState = { ...state, isLoading: false, error: null, errorContext: null };
      firstSetCall(tempState);
      expect(tempState.isLoading).toBe(true);
    });

    it('should clear load errors when starting loadInitialData', async () => {
      const mockApiClient = createMockApiClient();
      mockApiClient.getScheduler.mockResolvedValue(createMockSchedulerResponse());
      mockApiClient.getSchedulerConf.mockResolvedValue(createMockConfigResponse());
      mockApiClient.getNodeLabels.mockResolvedValue({ nodeLabelInfo: [] });
      mockApiClient.getNodes.mockResolvedValue({ nodes: { node: [] } });
      mockApiClient.getNodeToLabels.mockResolvedValue({});
      mockApiClient.getSchedulerConfVersion.mockResolvedValue(createMockVersionResponse());

      const state: any = {
        schedulerData: null,
        isLoading: false,
        error: 'Previous load error',
        errorContext: 'load',
        apiClient: mockApiClient,
        configData: new Map(),
        nodeLabels: [],
        nodes: [],
        nodeToLabels: [],
      };

      const set = vi.fn((fn) => fn(state));
      const get = vi.fn(() => state);

      const slice = createSchedulerDataSlice(set as any, get as any, {} as any);
      await slice.loadInitialData();

      // After loading, error context should be cleared
      expect(state.error).toBe(null);
      expect(state.errorContext).toBe(null);
    });

    it('should handle empty node labels response', async () => {
      const mockApiClient = createMockApiClient();
      mockApiClient.getScheduler.mockResolvedValue(createMockSchedulerResponse());
      mockApiClient.getSchedulerConf.mockResolvedValue(createMockConfigResponse());
      mockApiClient.getNodeLabels.mockResolvedValue({ nodeLabelInfo: [] });
      mockApiClient.getNodes.mockResolvedValue({ nodes: { node: [] } });
      mockApiClient.getNodeToLabels.mockResolvedValue({});
      mockApiClient.getSchedulerConfVersion.mockResolvedValue(createMockVersionResponse());

      const state: any = {
        schedulerData: null,
        isLoading: false,
        error: null,
        errorContext: null,
        apiClient: mockApiClient,
        configData: new Map(),
        nodeLabels: [],
        nodes: [],
        nodeToLabels: [],
      };

      const set = vi.fn((fn) => fn(state));
      const get = vi.fn(() => state);

      const slice = createSchedulerDataSlice(set as any, get as any, {} as any);
      await slice.loadInitialData();

      expect(state.nodeLabels).toEqual([]);
      expect(state.nodes).toEqual([]);
      expect(state.nodeToLabels).toEqual([]);
    });

    it('should handle undefined/null nodeLabelInfo', async () => {
      const mockApiClient = createMockApiClient();
      mockApiClient.getScheduler.mockResolvedValue(createMockSchedulerResponse());
      mockApiClient.getSchedulerConf.mockResolvedValue(createMockConfigResponse());
      mockApiClient.getNodeLabels.mockResolvedValue({});
      mockApiClient.getNodes.mockResolvedValue({ nodes: {} });
      mockApiClient.getNodeToLabels.mockResolvedValue({});
      mockApiClient.getSchedulerConfVersion.mockResolvedValue(createMockVersionResponse());

      const state: any = {
        schedulerData: null,
        isLoading: false,
        error: null,
        errorContext: null,
        apiClient: mockApiClient,
        configData: new Map(),
        nodeLabels: [],
        nodes: [],
        nodeToLabels: [],
      };

      const set = vi.fn((fn) => fn(state));
      const get = vi.fn(() => state);

      const slice = createSchedulerDataSlice(set as any, get as any, {} as any);
      await slice.loadInitialData();

      expect(state.nodeLabels).toEqual([]);
      expect(state.nodes).toEqual([]);
    });

    it('should normalize single node label object to array', async () => {
      const mockApiClient = createMockApiClient();
      mockApiClient.getScheduler.mockResolvedValue(createMockSchedulerResponse());
      mockApiClient.getSchedulerConf.mockResolvedValue(createMockConfigResponse());
      mockApiClient.getNodeLabels.mockResolvedValue({
        nodeLabelInfo: { name: 'gpu', exclusivity: true },
      } as any);
      mockApiClient.getNodes.mockResolvedValue(createMockNodesResponse());
      mockApiClient.getNodeToLabels.mockResolvedValue({});
      mockApiClient.getSchedulerConfVersion.mockResolvedValue(createMockVersionResponse());

      const state: any = {
        schedulerData: null,
        isLoading: false,
        error: null,
        errorContext: null,
        apiClient: mockApiClient,
        configData: new Map(),
        nodeLabels: [],
        nodes: [],
        nodeToLabels: [],
      };

      const set = vi.fn((fn) => fn(state));
      const get = vi.fn(() => state);

      const slice = createSchedulerDataSlice(set as any, get as any, {} as any);
      await slice.loadInitialData();

      expect(state.nodeLabels).toHaveLength(1);
      expect(state.nodeLabels[0].name).toBe('gpu');
      expect(state.nodeLabels[0].exclusivity).toBe(true);
    });

    it('should parse exclusivity defaults to true when not specified', async () => {
      const mockApiClient = createMockApiClient();
      mockApiClient.getScheduler.mockResolvedValue(createMockSchedulerResponse());
      mockApiClient.getSchedulerConf.mockResolvedValue(createMockConfigResponse());
      mockApiClient.getNodeLabels.mockResolvedValue({
        nodeLabelInfo: [{ name: 'gpu' }],
      } as any);
      mockApiClient.getNodes.mockResolvedValue(createMockNodesResponse());
      mockApiClient.getNodeToLabels.mockResolvedValue({});
      mockApiClient.getSchedulerConfVersion.mockResolvedValue(createMockVersionResponse());

      const state: any = {
        schedulerData: null,
        isLoading: false,
        error: null,
        errorContext: null,
        apiClient: mockApiClient,
        configData: new Map(),
        nodeLabels: [],
        nodes: [],
        nodeToLabels: [],
      };

      const set = vi.fn((fn) => fn(state));
      const get = vi.fn(() => state);

      const slice = createSchedulerDataSlice(set as any, get as any, {} as any);
      await slice.loadInitialData();

      expect(state.nodeLabels[0].exclusivity).toBe(true);
    });

    it('should parse exclusivity string "true" as boolean true', async () => {
      const mockApiClient = createMockApiClient();
      mockApiClient.getScheduler.mockResolvedValue(createMockSchedulerResponse());
      mockApiClient.getSchedulerConf.mockResolvedValue(createMockConfigResponse());
      mockApiClient.getNodeLabels.mockResolvedValue({
        nodeLabelInfo: [{ name: 'gpu', exclusivity: 'true' as any }],
      });
      mockApiClient.getNodes.mockResolvedValue(createMockNodesResponse());
      mockApiClient.getNodeToLabels.mockResolvedValue({});
      mockApiClient.getSchedulerConfVersion.mockResolvedValue(createMockVersionResponse());

      const state: any = {
        schedulerData: null,
        isLoading: false,
        error: null,
        errorContext: null,
        apiClient: mockApiClient,
        configData: new Map(),
        nodeLabels: [],
        nodes: [],
        nodeToLabels: [],
      };

      const set = vi.fn((fn) => fn(state));
      const get = vi.fn(() => state);

      const slice = createSchedulerDataSlice(set as any, get as any, {} as any);
      await slice.loadInitialData();

      expect(state.nodeLabels[0].exclusivity).toBe(true);
    });

    it('should parse exclusivity string "false" as boolean false', async () => {
      const mockApiClient = createMockApiClient();
      mockApiClient.getScheduler.mockResolvedValue(createMockSchedulerResponse());
      mockApiClient.getSchedulerConf.mockResolvedValue(createMockConfigResponse());
      mockApiClient.getNodeLabels.mockResolvedValue({
        nodeLabelInfo: [{ name: 'gpu', exclusivity: 'false' as any }],
      });
      mockApiClient.getNodes.mockResolvedValue(createMockNodesResponse());
      mockApiClient.getNodeToLabels.mockResolvedValue({});
      mockApiClient.getSchedulerConfVersion.mockResolvedValue(createMockVersionResponse());

      const state: any = {
        schedulerData: null,
        isLoading: false,
        error: null,
        errorContext: null,
        apiClient: mockApiClient,
        configData: new Map(),
        nodeLabels: [],
        nodes: [],
        nodeToLabels: [],
      };

      const set = vi.fn((fn) => fn(state));
      const get = vi.fn(() => state);

      const slice = createSchedulerDataSlice(set as any, get as any, {} as any);
      await slice.loadInitialData();

      expect(state.nodeLabels[0].exclusivity).toBe(false);
    });

    it('should parse exclusivity string "FALSE" (uppercase) as boolean false', async () => {
      const mockApiClient = createMockApiClient();
      mockApiClient.getScheduler.mockResolvedValue(createMockSchedulerResponse());
      mockApiClient.getSchedulerConf.mockResolvedValue(createMockConfigResponse());
      mockApiClient.getNodeLabels.mockResolvedValue({
        nodeLabelInfo: [{ name: 'gpu', exclusivity: 'FALSE' as any }],
      });
      mockApiClient.getNodes.mockResolvedValue(createMockNodesResponse());
      mockApiClient.getNodeToLabels.mockResolvedValue({});
      mockApiClient.getSchedulerConfVersion.mockResolvedValue(createMockVersionResponse());

      const state: any = {
        schedulerData: null,
        isLoading: false,
        error: null,
        errorContext: null,
        apiClient: mockApiClient,
        configData: new Map(),
        nodeLabels: [],
        nodes: [],
        nodeToLabels: [],
      };

      const set = vi.fn((fn) => fn(state));
      const get = vi.fn(() => state);

      const slice = createSchedulerDataSlice(set as any, get as any, {} as any);
      await slice.loadInitialData();

      expect(state.nodeLabels[0].exclusivity).toBe(false);
    });

    it('should normalize nodeToLabels with array entry', async () => {
      const mockApiClient = createMockApiClient();
      mockApiClient.getScheduler.mockResolvedValue(createMockSchedulerResponse());
      mockApiClient.getSchedulerConf.mockResolvedValue(createMockConfigResponse());
      mockApiClient.getNodeLabels.mockResolvedValue(createMockNodeLabelsResponse());
      mockApiClient.getNodes.mockResolvedValue(createMockNodesResponse());
      mockApiClient.getNodeToLabels.mockResolvedValue(createMockNodeToLabelsResponse());
      mockApiClient.getSchedulerConfVersion.mockResolvedValue(createMockVersionResponse());

      const state: any = {
        schedulerData: null,
        isLoading: false,
        error: null,
        errorContext: null,
        apiClient: mockApiClient,
        configData: new Map(),
        nodeLabels: [],
        nodes: [],
        nodeToLabels: [],
      };

      const set = vi.fn((fn) => fn(state));
      const get = vi.fn(() => state);

      const slice = createSchedulerDataSlice(set as any, get as any, {} as any);
      await slice.loadInitialData();

      expect(state.nodeToLabels).toHaveLength(2);
      expect(state.nodeToLabels[0].nodeId).toBe('node1:8041');
      expect(state.nodeToLabels[0].nodeLabels).toContain('gpu');
      expect(state.nodeToLabels[1].nodeId).toBe('node2:8041');
      expect(state.nodeToLabels[1].nodeLabels).toContain('cpu');
    });

    it('should normalize nodeToLabels with single entry object', async () => {
      const mockApiClient = createMockApiClient();
      mockApiClient.getScheduler.mockResolvedValue(createMockSchedulerResponse());
      mockApiClient.getSchedulerConf.mockResolvedValue(createMockConfigResponse());
      mockApiClient.getNodeLabels.mockResolvedValue(createMockNodeLabelsResponse());
      mockApiClient.getNodes.mockResolvedValue(createMockNodesResponse());
      mockApiClient.getNodeToLabels.mockResolvedValue({
        nodeToLabels: {
          entry: {
            key: 'node1:8041',
            value: {
              nodeLabelInfo: { name: 'gpu' },
            },
          },
        },
      } as any);
      mockApiClient.getSchedulerConfVersion.mockResolvedValue(createMockVersionResponse());

      const state: any = {
        schedulerData: null,
        isLoading: false,
        error: null,
        errorContext: null,
        apiClient: mockApiClient,
        configData: new Map(),
        nodeLabels: [],
        nodes: [],
        nodeToLabels: [],
      };

      const set = vi.fn((fn) => fn(state));
      const get = vi.fn(() => state);

      const slice = createSchedulerDataSlice(set as any, get as any, {} as any);
      await slice.loadInitialData();

      expect(state.nodeToLabels).toHaveLength(1);
      expect(state.nodeToLabels[0].nodeId).toBe('node1:8041');
      expect(state.nodeToLabels[0].nodeLabels).toContain('gpu');
    });

    it('should handle nested nodeLabelInfo structure', async () => {
      const mockApiClient = createMockApiClient();
      mockApiClient.getScheduler.mockResolvedValue(createMockSchedulerResponse());
      mockApiClient.getSchedulerConf.mockResolvedValue(createMockConfigResponse());
      mockApiClient.getNodeLabels.mockResolvedValue(createMockNodeLabelsResponse());
      mockApiClient.getNodes.mockResolvedValue(createMockNodesResponse());
      mockApiClient.getNodeToLabels.mockResolvedValue({
        nodeToLabels: {
          entry: {
            key: 'node1:8041',
            value: {
              nodeLabelInfo: {
                nodeLabelInfo: { name: 'gpu' },
              },
            },
          },
        },
      } as any);
      mockApiClient.getSchedulerConfVersion.mockResolvedValue(createMockVersionResponse());

      const state: any = {
        schedulerData: null,
        isLoading: false,
        error: null,
        errorContext: null,
        apiClient: mockApiClient,
        configData: new Map(),
        nodeLabels: [],
        nodes: [],
        nodeToLabels: [],
      };

      const set = vi.fn((fn) => fn(state));
      const get = vi.fn(() => state);

      const slice = createSchedulerDataSlice(set as any, get as any, {} as any);
      await slice.loadInitialData();

      expect(state.nodeToLabels).toHaveLength(1);
      expect(state.nodeToLabels[0].nodeLabels).toContain('gpu');
    });

    it('should handle network error during loadInitialData', async () => {
      const mockApiClient = createMockApiClient();
      mockApiClient.getScheduler.mockRejectedValue(new Error('network error'));

      const state: any = {
        schedulerData: null,
        isLoading: false,
        error: null,
        errorContext: null,
        apiClient: mockApiClient,
        configData: new Map(),
        nodeLabels: [],
        nodes: [],
        nodeToLabels: [],
      };

      const set = vi.fn((fn) => fn(state));
      const get = vi.fn(() => state);

      const slice = createSchedulerDataSlice(set as any, get as any, {} as any);

      await expect(slice.loadInitialData()).rejects.toThrow();

      expect(state.error).toBeDefined();
      expect(state.errorContext).toBe('load');
      expect(state.isLoading).toBe(false);
    });

    it('should handle non-network error during loadInitialData', async () => {
      const mockApiClient = createMockApiClient();
      mockApiClient.getScheduler.mockRejectedValue(new Error('some other error'));

      const state: any = {
        schedulerData: null,
        isLoading: false,
        error: null,
        errorContext: null,
        apiClient: mockApiClient,
        configData: new Map(),
        nodeLabels: [],
        nodes: [],
        nodeToLabels: [],
      };

      const set = vi.fn((fn) => fn(state));
      const get = vi.fn(() => state);

      const slice = createSchedulerDataSlice(set as any, get as any, {} as any);

      await expect(slice.loadInitialData()).rejects.toThrow();

      expect(state.error).toBeDefined();
      expect(state.errorContext).toBe('load');
      expect(state.isLoading).toBe(false);
    });

    it('should convert config property array to Map', async () => {
      const mockApiClient = createMockApiClient();
      mockApiClient.getScheduler.mockResolvedValue(createMockSchedulerResponse());
      mockApiClient.getSchedulerConf.mockResolvedValue({
        property: [
          { name: 'prop1', value: 'value1' },
          { name: 'prop2', value: 'value2' },
          { name: 'prop3', value: 'value3' },
        ] as ConfigProperty[],
      });
      mockApiClient.getNodeLabels.mockResolvedValue({ nodeLabelInfo: [] });
      mockApiClient.getNodes.mockResolvedValue({ nodes: { node: [] } });
      mockApiClient.getNodeToLabels.mockResolvedValue({});
      mockApiClient.getSchedulerConfVersion.mockResolvedValue(createMockVersionResponse());

      const state: any = {
        schedulerData: null,
        isLoading: false,
        error: null,
        errorContext: null,
        apiClient: mockApiClient,
        configData: new Map(),
        nodeLabels: [],
        nodes: [],
        nodeToLabels: [],
      };

      const set = vi.fn((fn) => fn(state));
      const get = vi.fn(() => state);

      const slice = createSchedulerDataSlice(set as any, get as any, {} as any);
      await slice.loadInitialData();

      expect(state.configData.size).toBe(3);
      expect(state.configData.get('prop1')).toBe('value1');
      expect(state.configData.get('prop2')).toBe('value2');
      expect(state.configData.get('prop3')).toBe('value3');
    });

    it('should detect read-only mode from API client', async () => {
      const mockApiClient = createMockApiClient();
      mockApiClient.getScheduler.mockResolvedValue(createMockSchedulerResponse());
      mockApiClient.getSchedulerConf.mockResolvedValue(createMockConfigResponse());
      mockApiClient.getNodeLabels.mockResolvedValue({ nodeLabelInfo: [] });
      mockApiClient.getNodes.mockResolvedValue({ nodes: { node: [] } });
      mockApiClient.getNodeToLabels.mockResolvedValue({});
      mockApiClient.getSchedulerConfVersion.mockResolvedValue(createMockVersionResponse());
      mockApiClient.getIsReadOnly.mockReturnValue(true);

      const state: any = {
        schedulerData: null,
        isLoading: false,
        error: null,
        errorContext: null,
        apiClient: mockApiClient,
        configData: new Map(),
        nodeLabels: [],
        nodes: [],
        nodeToLabels: [],
        isReadOnly: false,
      };

      const set = vi.fn((fn) => fn(state));
      const get = vi.fn(() => state);

      const slice = createSchedulerDataSlice(set as any, get as any, {} as any);
      await slice.loadInitialData();

      expect(state.isReadOnly).toBe(true);
    });
  });

  describe('refreshSchedulerData', () => {
    it('should successfully refresh scheduler data', async () => {
      const mockApiClient = createMockApiClient();
      const newSchedulerData = createMockSchedulerResponse();
      mockApiClient.getScheduler.mockResolvedValue(newSchedulerData);

      const state: any = {
        schedulerData: null,
        isLoading: false,
        error: null,
        errorContext: null,
        apiClient: mockApiClient,
      };

      const set = vi.fn((fn) => fn(state));
      const get = vi.fn(() => state);

      const slice = createSchedulerDataSlice(set as any, get as any, {} as any);
      await slice.refreshSchedulerData();

      expect(mockApiClient.getScheduler).toHaveBeenCalledTimes(1);
      expect(state.schedulerData).toEqual(newSchedulerData.scheduler.schedulerInfo);
      expect(state.isLoading).toBe(false);
      expect(state.error).toBe(null);
    });

    it('should set isLoading to true at the start', async () => {
      const mockApiClient = createMockApiClient();
      mockApiClient.getScheduler.mockResolvedValue(createMockSchedulerResponse());

      const state: any = {
        schedulerData: null,
        isLoading: false,
        error: null,
        errorContext: null,
        apiClient: mockApiClient,
      };

      const set = vi.fn((fn) => fn(state));
      const get = vi.fn(() => state);

      const slice = createSchedulerDataSlice(set as any, get as any, {} as any);
      await slice.refreshSchedulerData();

      // Check that the first call to set() updated isLoading
      const firstSetCall = set.mock.calls[0][0];
      const tempState = { ...state, isLoading: false, error: null, errorContext: null };
      firstSetCall(tempState);
      expect(tempState.isLoading).toBe(true);
    });

    it('should clear load errors when starting refresh', async () => {
      const mockApiClient = createMockApiClient();
      mockApiClient.getScheduler.mockResolvedValue(createMockSchedulerResponse());

      const state: any = {
        schedulerData: null,
        isLoading: false,
        error: 'Previous load error',
        errorContext: 'load',
        apiClient: mockApiClient,
      };

      const set = vi.fn((fn) => fn(state));
      const get = vi.fn(() => state);

      const slice = createSchedulerDataSlice(set as any, get as any, {} as any);
      await slice.refreshSchedulerData();

      expect(state.error).toBe(null);
      expect(state.errorContext).toBe(null);
    });

    it('should handle network error during refresh', async () => {
      const mockApiClient = createMockApiClient();
      mockApiClient.getScheduler.mockRejectedValue(new Error('network error'));

      const state: any = {
        schedulerData: null,
        isLoading: false,
        error: null,
        errorContext: null,
        apiClient: mockApiClient,
      };

      const set = vi.fn((fn) => fn(state));
      const get = vi.fn(() => state);

      const slice = createSchedulerDataSlice(set as any, get as any, {} as any);

      await expect(slice.refreshSchedulerData()).rejects.toThrow();

      expect(state.error).toBeDefined();
      expect(state.errorContext).toBe('load');
      expect(state.isLoading).toBe(false);
    });

    it('should handle non-network error during refresh', async () => {
      const mockApiClient = createMockApiClient();
      mockApiClient.getScheduler.mockRejectedValue(new Error('some other error'));

      const state: any = {
        schedulerData: null,
        isLoading: false,
        error: null,
        errorContext: null,
        apiClient: mockApiClient,
      };

      const set = vi.fn((fn) => fn(state));
      const get = vi.fn(() => state);

      const slice = createSchedulerDataSlice(set as any, get as any, {} as any);

      await expect(slice.refreshSchedulerData()).rejects.toThrow();

      expect(state.error).toBeDefined();
      expect(state.errorContext).toBe('load');
      expect(state.isLoading).toBe(false);
    });

    it('should not clear mutation errors when refreshing', async () => {
      const mockApiClient = createMockApiClient();
      mockApiClient.getScheduler.mockResolvedValue(createMockSchedulerResponse());

      const state: any = {
        schedulerData: null,
        isLoading: false,
        error: 'Mutation error',
        errorContext: 'mutation',
        apiClient: mockApiClient,
      };

      const set = vi.fn((fn) => fn(state));
      const get = vi.fn(() => state);

      const slice = createSchedulerDataSlice(set as any, get as any, {} as any);
      await slice.refreshSchedulerData();

      // The error should remain because errorContext is not 'load'
      expect(state.error).toBe('Mutation error');
      expect(state.errorContext).toBe('mutation');
    });
  });
});
