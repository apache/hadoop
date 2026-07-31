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


import { describe, it, expect, vi, beforeEach, afterEach } from 'vitest';
import { create } from 'zustand';
import { immer } from 'zustand/middleware/immer';
import type { SchedulerStore } from '~/stores/slices/types';
import { createNodeLabelsSlice } from '~/stores/slices/nodeLabelsSlice';
import { ERROR_CODES } from '~/lib/errors';
import type { NodeLabelsResponse, NodeToLabelsResponse, NodeLabelInfoItem } from '~/types';

// Mock dependencies
vi.mock('~/features/node-labels/utils/labelValidation', () => ({
  validateLabelRemoval: vi.fn(),
}));

vi.mock('~/lib/errors', async () => {
  const actual = await vi.importActual('~/lib/errors');
  return {
    ...actual,
    createDetailedErrorMessage: vi.fn((operation, error) => {
      const message = error instanceof Error ? error.message : String(error);
      return `Failed to ${operation}: ${message}`;
    }),
  };
});

import { validateLabelRemoval } from '~/features/node-labels/utils/labelValidation';

describe('nodeLabelsSlice', () => {
  let store: ReturnType<typeof createTestStore>;

  function createTestStore(initialState?: Partial<SchedulerStore>) {
    const apiClient = {
      addNodeLabels: vi.fn(),
      removeNodeLabels: vi.fn(),
      getNodeLabels: vi.fn(),
      getNodeToLabels: vi.fn(),
      replaceNodeToLabels: vi.fn(),
    };

    return create<SchedulerStore>()(
      immer((set, get, api) => ({
        // Shared properties must come first so the slice can access them
        apiClient: apiClient as any,
        isReadOnly: false,
        isLoading: false,
        error: null,
        errorContext: null,
        schedulerData: null,
        configData: new Map(),
        configVersion: 0,
        loadInitialData: vi.fn(async () => {}),
        refreshSchedulerData: vi.fn(async () => {}),
        stagedChanges: [],
        applyError: null,
        ...initialState,
        // Slice properties and methods
        ...createNodeLabelsSlice(set, get, api),
        // Stub implementations for methods we don't test
        ...({} as any),
      })),
    );
  }

  beforeEach(() => {
    store = createTestStore();
    vi.clearAllMocks();
  });

  afterEach(() => {
    vi.clearAllMocks();
  });

  describe('initial state', () => {
    it('should initialize with empty arrays and null selection', () => {
      const state = store.getState();

      expect(state.nodeLabels).toEqual([]);
      expect(state.nodes).toEqual([]);
      expect(state.nodeToLabels).toEqual([]);
      expect(state.selectedNodeLabel).toBeNull();
    });
  });

  describe('selectNodeLabel', () => {
    it('should set selected node label', () => {
      store.getState().selectNodeLabel('gpu');

      expect(store.getState().selectedNodeLabel).toBe('gpu');
    });

    it('should update to different label', () => {
      store.getState().selectNodeLabel('gpu');
      store.getState().selectNodeLabel('cpu');

      expect(store.getState().selectedNodeLabel).toBe('cpu');
    });

    it('should set to null', () => {
      store.getState().selectNodeLabel('gpu');
      store.getState().selectNodeLabel(null);

      expect(store.getState().selectedNodeLabel).toBeNull();
    });
  });

  describe('addNodeLabel', () => {
    it('should successfully add a node label', async () => {
      const mockResponse: NodeLabelsResponse = {
        nodeLabelInfo: [
          { name: 'gpu', exclusivity: true },
          { name: 'ssd', exclusivity: false },
        ],
      };

      vi.mocked(store.getState().apiClient.addNodeLabels).mockResolvedValue(undefined);
      vi.mocked(store.getState().apiClient.getNodeLabels).mockResolvedValue(mockResponse);

      await store.getState().addNodeLabel('ssd', false);

      expect(store.getState().apiClient.addNodeLabels).toHaveBeenCalledWith([
        { name: 'ssd', exclusivity: false },
      ]);
      expect(store.getState().nodeLabels).toEqual([
        { name: 'gpu', exclusivity: true },
        { name: 'ssd', exclusivity: false },
      ]);
      expect(store.getState().isLoading).toBe(false);
      expect(store.getState().error).toBeNull();
    });

    it('should set loading state during operation', async () => {
      const mockResponse: NodeLabelsResponse = {
        nodeLabelInfo: [{ name: 'gpu', exclusivity: true }],
      };

      let loadingDuringCall = false;

      vi.mocked(store.getState().apiClient.addNodeLabels).mockImplementation(async () => {
        loadingDuringCall = store.getState().isLoading;
      });
      vi.mocked(store.getState().apiClient.getNodeLabels).mockResolvedValue(mockResponse);

      await store.getState().addNodeLabel('gpu', true);

      expect(loadingDuringCall).toBe(true);
      expect(store.getState().isLoading).toBe(false);
    });

    it('should throw error in read-only mode', async () => {
      store = createTestStore({ isReadOnly: true });

      await expect(store.getState().addNodeLabel('gpu', true)).rejects.toThrow();

      const state = store.getState();
      expect(state.error).toContain('read-only mode');
      expect(state.errorContext).toBe('nodeLabels');
      expect(state.apiClient.addNodeLabels).not.toHaveBeenCalled();
    });

    it('should handle network errors', async () => {
      const networkError = new Error('Network request failed');

      vi.mocked(store.getState().apiClient.addNodeLabels).mockRejectedValue(networkError);

      await expect(store.getState().addNodeLabel('gpu', true)).rejects.toThrow();

      const state = store.getState();
      expect(state.error).toContain('add node label "gpu"');
      expect(state.errorContext).toBe('nodeLabels');
      expect(state.isLoading).toBe(false);
    });

    it('should handle API errors', async () => {
      const apiError = new Error('Label already exists');

      vi.mocked(store.getState().apiClient.addNodeLabels).mockRejectedValue(apiError);

      await expect(store.getState().addNodeLabel('gpu', true)).rejects.toThrow();

      const state = store.getState();
      expect(state.error).toContain('add node label "gpu"');
      expect(state.errorContext).toBe('nodeLabels');
      expect(state.isLoading).toBe(false);
    });

    it('should clear previous nodeLabels errors', async () => {
      store.setState({ error: 'Previous error', errorContext: 'nodeLabels' });

      const mockResponse: NodeLabelsResponse = {
        nodeLabelInfo: [{ name: 'gpu', exclusivity: true }],
      };

      vi.mocked(store.getState().apiClient.addNodeLabels).mockResolvedValue(undefined);
      vi.mocked(store.getState().apiClient.getNodeLabels).mockResolvedValue(mockResponse);

      await store.getState().addNodeLabel('gpu', true);

      expect(store.getState().error).toBeNull();
      expect(store.getState().errorContext).toBeNull();
    });

    it('should not clear errors from other contexts', async () => {
      store.setState({ error: 'Other error', errorContext: 'load' });

      const mockResponse: NodeLabelsResponse = {
        nodeLabelInfo: [{ name: 'gpu', exclusivity: true }],
      };

      vi.mocked(store.getState().apiClient.addNodeLabels).mockResolvedValue(undefined);
      vi.mocked(store.getState().apiClient.getNodeLabels).mockResolvedValue(mockResponse);

      await store.getState().addNodeLabel('gpu', true);

      expect(store.getState().error).toBe('Other error');
      expect(store.getState().errorContext).toBe('load');
    });
  });

  describe('removeNodeLabel', () => {
    beforeEach(() => {
      store.setState({
        nodeLabels: [
          { name: 'gpu', exclusivity: true },
          { name: 'ssd', exclusivity: false },
        ],
        nodeToLabels: [
          { nodeId: 'node1', nodeLabels: ['gpu'] },
          { nodeId: 'node2', nodeLabels: [] },
        ],
      });
    });

    it('should successfully remove a node label', async () => {
      vi.mocked(validateLabelRemoval).mockReturnValue({ valid: true });

      const mockLabelsResponse: NodeLabelsResponse = {
        nodeLabelInfo: [{ name: 'ssd', exclusivity: false }],
      };

      const mockNodeToLabelsResponse: NodeToLabelsResponse = {
        nodeToLabels: {
          entry: [
            { key: 'node1', value: { nodeLabelInfo: [] } },
            { key: 'node2', value: { nodeLabelInfo: [] } },
          ],
        },
      };

      vi.mocked(store.getState().apiClient.removeNodeLabels).mockResolvedValue(undefined);
      vi.mocked(store.getState().apiClient.getNodeLabels).mockResolvedValue(mockLabelsResponse);
      vi.mocked(store.getState().apiClient.getNodeToLabels).mockResolvedValue(
        mockNodeToLabelsResponse,
      );

      await store.getState().removeNodeLabel('gpu');

      expect(store.getState().apiClient.removeNodeLabels).toHaveBeenCalledWith(['gpu']);
      expect(store.getState().nodeLabels).toEqual([{ name: 'ssd', exclusivity: false }]);
      expect(store.getState().isLoading).toBe(false);
    });

    it('should clear selected label if removed label was selected', async () => {
      store.setState({ selectedNodeLabel: 'gpu' });
      vi.mocked(validateLabelRemoval).mockReturnValue({ valid: true });

      const mockLabelsResponse: NodeLabelsResponse = {
        nodeLabelInfo: [{ name: 'ssd', exclusivity: false }],
      };

      const mockNodeToLabelsResponse: NodeToLabelsResponse = {
        nodeToLabels: { entry: [] },
      };

      vi.mocked(store.getState().apiClient.removeNodeLabels).mockResolvedValue(undefined);
      vi.mocked(store.getState().apiClient.getNodeLabels).mockResolvedValue(mockLabelsResponse);
      vi.mocked(store.getState().apiClient.getNodeToLabels).mockResolvedValue(
        mockNodeToLabelsResponse,
      );

      await store.getState().removeNodeLabel('gpu');

      expect(store.getState().selectedNodeLabel).toBeNull();
    });

    it('should not clear selected label if different label was removed', async () => {
      store.setState({ selectedNodeLabel: 'gpu' });
      vi.mocked(validateLabelRemoval).mockReturnValue({ valid: true });

      const mockLabelsResponse: NodeLabelsResponse = {
        nodeLabelInfo: [{ name: 'gpu', exclusivity: true }],
      };

      const mockNodeToLabelsResponse: NodeToLabelsResponse = {
        nodeToLabels: { entry: [] },
      };

      vi.mocked(store.getState().apiClient.removeNodeLabels).mockResolvedValue(undefined);
      vi.mocked(store.getState().apiClient.getNodeLabels).mockResolvedValue(mockLabelsResponse);
      vi.mocked(store.getState().apiClient.getNodeToLabels).mockResolvedValue(
        mockNodeToLabelsResponse,
      );

      await store.getState().removeNodeLabel('ssd');

      expect(store.getState().selectedNodeLabel).toBe('gpu');
    });

    it('should throw error in read-only mode', async () => {
      store = createTestStore({ isReadOnly: true });

      await expect(store.getState().removeNodeLabel('gpu')).rejects.toThrow();

      const state = store.getState();
      expect(state.error).toContain('read-only mode');
      expect(state.errorContext).toBe('nodeLabels');
      expect(state.apiClient.removeNodeLabels).not.toHaveBeenCalled();
    });

    it('should throw error when validation fails', async () => {
      vi.mocked(validateLabelRemoval).mockReturnValue({
        valid: false,
        error: 'Label "gpu" is assigned to 3 nodes',
      });

      await expect(store.getState().removeNodeLabel('gpu')).rejects.toThrow();

      const state = store.getState();
      expect(state.error).toContain('Label "gpu" is assigned to 3 nodes');
      expect(state.errorContext).toBe('nodeLabels');
      expect(state.apiClient.removeNodeLabels).not.toHaveBeenCalled();
    });

    it('should handle network errors', async () => {
      vi.mocked(validateLabelRemoval).mockReturnValue({ valid: true });

      const networkError = new Error('Network timeout');
      vi.mocked(store.getState().apiClient.removeNodeLabels).mockRejectedValue(networkError);

      await expect(store.getState().removeNodeLabel('gpu')).rejects.toThrow();

      const state = store.getState();
      expect(state.error).toContain('remove node label "gpu"');
      expect(state.errorContext).toBe('nodeLabels');
      expect(state.isLoading).toBe(false);
    });

    it('should set loading state during operation', async () => {
      vi.mocked(validateLabelRemoval).mockReturnValue({ valid: true });

      let loadingDuringCall = false;

      vi.mocked(store.getState().apiClient.removeNodeLabels).mockImplementation(async () => {
        loadingDuringCall = store.getState().isLoading;
      });

      const mockLabelsResponse: NodeLabelsResponse = {
        nodeLabelInfo: [],
      };

      const mockNodeToLabelsResponse: NodeToLabelsResponse = {
        nodeToLabels: { entry: [] },
      };

      vi.mocked(store.getState().apiClient.getNodeLabels).mockResolvedValue(mockLabelsResponse);
      vi.mocked(store.getState().apiClient.getNodeToLabels).mockResolvedValue(
        mockNodeToLabelsResponse,
      );

      await store.getState().removeNodeLabel('gpu');

      expect(loadingDuringCall).toBe(true);
      expect(store.getState().isLoading).toBe(false);
    });
  });

  describe('assignNodeToLabel', () => {
    it('should successfully assign node to label', async () => {
      const mockResponse: NodeToLabelsResponse = {
        nodeToLabels: {
          entry: [
            {
              key: 'node1',
              value: { nodeLabelInfo: { name: 'gpu' } },
            },
          ],
        },
      };

      vi.mocked(store.getState().apiClient.replaceNodeToLabels).mockResolvedValue(undefined);
      vi.mocked(store.getState().apiClient.getNodeToLabels).mockResolvedValue(mockResponse);

      await store.getState().assignNodeToLabel('node1', 'gpu');

      expect(store.getState().apiClient.replaceNodeToLabels).toHaveBeenCalledWith([
        { nodeId: 'node1', labels: ['gpu'] },
      ]);
      expect(store.getState().nodeToLabels).toEqual([{ nodeId: 'node1', nodeLabels: ['gpu'] }]);
      expect(store.getState().isLoading).toBe(false);
    });

    it('should handle assigning null label (unassignment)', async () => {
      const mockResponse: NodeToLabelsResponse = {
        nodeToLabels: {
          entry: [
            {
              key: 'node1',
              value: { nodeLabelInfo: [] },
            },
          ],
        },
      };

      vi.mocked(store.getState().apiClient.replaceNodeToLabels).mockResolvedValue(undefined);
      vi.mocked(store.getState().apiClient.getNodeToLabels).mockResolvedValue(mockResponse);

      await store.getState().assignNodeToLabel('node1', null);

      expect(store.getState().apiClient.replaceNodeToLabels).toHaveBeenCalledWith([
        { nodeId: 'node1', labels: [] },
      ]);
      expect(store.getState().nodeToLabels).toEqual([{ nodeId: 'node1', nodeLabels: [] }]);
    });

    it('should throw error in read-only mode', async () => {
      store = createTestStore({ isReadOnly: true });

      await expect(store.getState().assignNodeToLabel('node1', 'gpu')).rejects.toThrow();

      const state = store.getState();
      expect(state.error).toContain('read-only mode');
      expect(state.errorContext).toBe('nodeLabels');
      expect(state.apiClient.replaceNodeToLabels).not.toHaveBeenCalled();
    });

    it('should handle network errors', async () => {
      const networkError = new Error('Connection lost');

      vi.mocked(store.getState().apiClient.replaceNodeToLabels).mockRejectedValue(networkError);

      await expect(store.getState().assignNodeToLabel('node1', 'gpu')).rejects.toThrow();

      const state = store.getState();
      expect(state.error).toContain('assign node "node1" to label "gpu"');
      expect(state.errorContext).toBe('nodeLabels');
      expect(state.isLoading).toBe(false);
    });

    it('should set loading state during operation', async () => {
      let loadingDuringCall = false;

      vi.mocked(store.getState().apiClient.replaceNodeToLabels).mockImplementation(async () => {
        loadingDuringCall = store.getState().isLoading;
      });

      const mockResponse: NodeToLabelsResponse = {
        nodeToLabels: { entry: [] },
      };

      vi.mocked(store.getState().apiClient.getNodeToLabels).mockResolvedValue(mockResponse);

      await store.getState().assignNodeToLabel('node1', 'gpu');

      expect(loadingDuringCall).toBe(true);
      expect(store.getState().isLoading).toBe(false);
    });

    it('should clear previous nodeLabels errors', async () => {
      store.setState({ error: 'Previous error', errorContext: 'nodeLabels' });

      const mockResponse: NodeToLabelsResponse = {
        nodeToLabels: { entry: [] },
      };

      vi.mocked(store.getState().apiClient.replaceNodeToLabels).mockResolvedValue(undefined);
      vi.mocked(store.getState().apiClient.getNodeToLabels).mockResolvedValue(mockResponse);

      await store.getState().assignNodeToLabel('node1', 'gpu');

      expect(store.getState().error).toBeNull();
      expect(store.getState().errorContext).toBeNull();
    });
  });

  describe('normalizeNodeLabels', () => {
    it('should handle array of labels', async () => {
      const mockResponse: NodeLabelsResponse = {
        nodeLabelInfo: [
          { name: 'gpu', exclusivity: true },
          { name: 'ssd', exclusivity: false },
        ],
      };

      vi.mocked(store.getState().apiClient.addNodeLabels).mockResolvedValue(undefined);
      vi.mocked(store.getState().apiClient.getNodeLabels).mockResolvedValue(mockResponse);

      await store.getState().addNodeLabel('test', true);

      expect(store.getState().nodeLabels).toEqual([
        { name: 'gpu', exclusivity: true },
        { name: 'ssd', exclusivity: false },
      ]);
    });

    it('should handle single label object', async () => {
      const mockResponse: NodeLabelsResponse = {
        nodeLabelInfo: { name: 'gpu', exclusivity: true } as any,
      };

      vi.mocked(store.getState().apiClient.addNodeLabels).mockResolvedValue(undefined);
      vi.mocked(store.getState().apiClient.getNodeLabels).mockResolvedValue(mockResponse);

      await store.getState().addNodeLabel('test', true);

      expect(store.getState().nodeLabels).toEqual([{ name: 'gpu', exclusivity: true }]);
    });

    it('should handle empty response', async () => {
      const mockResponse: NodeLabelsResponse = {};

      vi.mocked(store.getState().apiClient.addNodeLabels).mockResolvedValue(undefined);
      vi.mocked(store.getState().apiClient.getNodeLabels).mockResolvedValue(mockResponse);

      await store.getState().addNodeLabel('test', true);

      expect(store.getState().nodeLabels).toEqual([]);
    });

    it('should handle undefined response', async () => {
      vi.mocked(store.getState().apiClient.addNodeLabels).mockResolvedValue(undefined);
      vi.mocked(store.getState().apiClient.getNodeLabels).mockResolvedValue({
        nodeLabelInfo: [],
      });

      await store.getState().addNodeLabel('test', true);

      expect(store.getState().nodeLabels).toEqual([]);
    });

    it('should parse exclusivity string "true" as true', async () => {
      const mockResponse: NodeLabelsResponse = {
        nodeLabelInfo: [{ name: 'gpu', exclusivity: 'true' as any }],
      };

      vi.mocked(store.getState().apiClient.addNodeLabels).mockResolvedValue(undefined);
      vi.mocked(store.getState().apiClient.getNodeLabels).mockResolvedValue(mockResponse);

      await store.getState().addNodeLabel('test', true);

      expect(store.getState().nodeLabels[0].exclusivity).toBe(true);
    });

    it('should parse exclusivity string "false" as false', async () => {
      const mockResponse: NodeLabelsResponse = {
        nodeLabelInfo: [{ name: 'gpu', exclusivity: 'false' as any }],
      };

      vi.mocked(store.getState().apiClient.addNodeLabels).mockResolvedValue(undefined);
      vi.mocked(store.getState().apiClient.getNodeLabels).mockResolvedValue(mockResponse);

      await store.getState().addNodeLabel('test', true);

      expect(store.getState().nodeLabels[0].exclusivity).toBe(false);
    });

    it('should parse exclusivity string "FALSE" as false (case-insensitive)', async () => {
      const mockResponse: NodeLabelsResponse = {
        nodeLabelInfo: [{ name: 'gpu', exclusivity: 'FALSE' as any }],
      };

      vi.mocked(store.getState().apiClient.addNodeLabels).mockResolvedValue(undefined);
      vi.mocked(store.getState().apiClient.getNodeLabels).mockResolvedValue(mockResponse);

      await store.getState().addNodeLabel('test', true);

      expect(store.getState().nodeLabels[0].exclusivity).toBe(false);
    });

    it('should default exclusivity to true when undefined', async () => {
      const mockResponse: NodeLabelsResponse = {
        nodeLabelInfo: [{ name: 'gpu' }],
      };

      vi.mocked(store.getState().apiClient.addNodeLabels).mockResolvedValue(undefined);
      vi.mocked(store.getState().apiClient.getNodeLabels).mockResolvedValue(mockResponse);

      await store.getState().addNodeLabel('test', true);

      expect(store.getState().nodeLabels[0].exclusivity).toBe(true);
    });
  });

  describe('normalizeNodeToLabels', () => {
    it('should handle array of entries', async () => {
      vi.mocked(validateLabelRemoval).mockReturnValue({ valid: true });

      const mockLabelsResponse: NodeLabelsResponse = { nodeLabelInfo: [] };

      const mockNodeToLabelsResponse: NodeToLabelsResponse = {
        nodeToLabels: {
          entry: [
            {
              key: 'node1',
              value: { nodeLabelInfo: { name: 'gpu' } },
            },
            {
              key: 'node2',
              value: { nodeLabelInfo: [{ name: 'ssd' }, { name: 'cpu' }] },
            },
          ],
        },
      };

      vi.mocked(store.getState().apiClient.removeNodeLabels).mockResolvedValue(undefined);
      vi.mocked(store.getState().apiClient.getNodeLabels).mockResolvedValue(mockLabelsResponse);
      vi.mocked(store.getState().apiClient.getNodeToLabels).mockResolvedValue(
        mockNodeToLabelsResponse,
      );

      await store.getState().removeNodeLabel('test');

      expect(store.getState().nodeToLabels).toEqual([
        { nodeId: 'node1', nodeLabels: ['gpu'] },
        { nodeId: 'node2', nodeLabels: ['ssd', 'cpu'] },
      ]);
    });

    it('should handle single entry object', async () => {
      vi.mocked(validateLabelRemoval).mockReturnValue({ valid: true });

      const mockLabelsResponse: NodeLabelsResponse = { nodeLabelInfo: [] };

      const mockNodeToLabelsResponse: NodeToLabelsResponse = {
        nodeToLabels: {
          entry: {
            key: 'node1',
            value: { nodeLabelInfo: { name: 'gpu' } },
          } as any,
        },
      };

      vi.mocked(store.getState().apiClient.removeNodeLabels).mockResolvedValue(undefined);
      vi.mocked(store.getState().apiClient.getNodeLabels).mockResolvedValue(mockLabelsResponse);
      vi.mocked(store.getState().apiClient.getNodeToLabels).mockResolvedValue(
        mockNodeToLabelsResponse,
      );

      await store.getState().removeNodeLabel('test');

      expect(store.getState().nodeToLabels).toEqual([{ nodeId: 'node1', nodeLabels: ['gpu'] }]);
    });

    it('should handle empty entries', async () => {
      vi.mocked(validateLabelRemoval).mockReturnValue({ valid: true });

      const mockLabelsResponse: NodeLabelsResponse = { nodeLabelInfo: [] };

      const mockNodeToLabelsResponse: NodeToLabelsResponse = {
        nodeToLabels: {} as any,
      };

      vi.mocked(store.getState().apiClient.removeNodeLabels).mockResolvedValue(undefined);
      vi.mocked(store.getState().apiClient.getNodeLabels).mockResolvedValue(mockLabelsResponse);
      vi.mocked(store.getState().apiClient.getNodeToLabels).mockResolvedValue(
        mockNodeToLabelsResponse,
      );

      await store.getState().removeNodeLabel('test');

      expect(store.getState().nodeToLabels).toEqual([]);
    });

    it('should handle undefined response', async () => {
      vi.mocked(validateLabelRemoval).mockReturnValue({ valid: true });

      const mockLabelsResponse: NodeLabelsResponse = { nodeLabelInfo: [] };

      vi.mocked(store.getState().apiClient.removeNodeLabels).mockResolvedValue(undefined);
      vi.mocked(store.getState().apiClient.getNodeLabels).mockResolvedValue(mockLabelsResponse);
      vi.mocked(store.getState().apiClient.getNodeToLabels).mockResolvedValue({
        nodeToLabels: {
          entry: [],
        },
      });

      await store.getState().removeNodeLabel('test');

      expect(store.getState().nodeToLabels).toEqual([]);
    });

    it('should handle node with no labels', async () => {
      vi.mocked(validateLabelRemoval).mockReturnValue({ valid: true });

      const mockLabelsResponse: NodeLabelsResponse = { nodeLabelInfo: [] };

      const mockNodeToLabelsResponse: NodeToLabelsResponse = {
        nodeToLabels: {
          entry: {
            key: 'node1',
            value: {},
          } as any,
        },
      };

      vi.mocked(store.getState().apiClient.removeNodeLabels).mockResolvedValue(undefined);
      vi.mocked(store.getState().apiClient.getNodeLabels).mockResolvedValue(mockLabelsResponse);
      vi.mocked(store.getState().apiClient.getNodeToLabels).mockResolvedValue(
        mockNodeToLabelsResponse,
      );

      await store.getState().removeNodeLabel('test');

      expect(store.getState().nodeToLabels).toEqual([{ nodeId: 'node1', nodeLabels: [] }]);
    });

    it('should filter out labels with undefined names', async () => {
      vi.mocked(validateLabelRemoval).mockReturnValue({ valid: true });

      const mockLabelsResponse: NodeLabelsResponse = { nodeLabelInfo: [] };

      const mockNodeToLabelsResponse: NodeToLabelsResponse = {
        nodeToLabels: {
          entry: {
            key: 'node1',
            value: {
              nodeLabelInfo: [{ name: 'gpu' }, { name: undefined } as any, { name: 'ssd' }],
            },
          } as any,
        },
      };

      vi.mocked(store.getState().apiClient.removeNodeLabels).mockResolvedValue(undefined);
      vi.mocked(store.getState().apiClient.getNodeLabels).mockResolvedValue(mockLabelsResponse);
      vi.mocked(store.getState().apiClient.getNodeToLabels).mockResolvedValue(
        mockNodeToLabelsResponse,
      );

      await store.getState().removeNodeLabel('test');

      expect(store.getState().nodeToLabels).toEqual([
        { nodeId: 'node1', nodeLabels: ['gpu', 'ssd'] },
      ]);
    });
  });
});
