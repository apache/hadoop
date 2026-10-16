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


import { describe, it, expect, beforeEach, vi } from 'vitest';
import { createSchedulerStore } from '~/stores/schedulerStore';
import type { YarnApiClient } from '~/lib/api/YarnApiClient';
import { ERROR_CODES } from '~/lib/errors';

// Create mock API client
const createMockApiClient = (isReadOnly: boolean) => ({
  getScheduler: vi.fn(),
  getSchedulerConf: vi.fn(),
  getNodeLabels: vi.fn(),
  getNodes: vi.fn(),
  getNodeToLabels: vi.fn(),
  getSchedulerConfVersion: vi.fn(),
  updateSchedulerConf: vi.fn(),
  validateSchedulerConf: vi.fn(),
  addNodeLabels: vi.fn(),
  removeNodeLabels: vi.fn(),
  replaceNodeToLabels: vi.fn(),
  getIsReadOnly: vi.fn(() => isReadOnly),
});

// Mock responses
const mockSchedulerResponse = {
  scheduler: {
    schedulerInfo: {
      type: 'capacityScheduler' as const,
      capacity: 100,
      usedCapacity: 50,
      maxCapacity: 100,
      queueName: 'root',
      queues: {
        queue: [
          {
            queueType: 'leaf' as const,
            queueName: 'default',
            capacity: 100,
            usedCapacity: 0,
            maxCapacity: 100,
            absoluteCapacity: 100,
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
            state: 'RUNNING' as const,
          },
        ],
      },
    },
  },
};

const mockConfigResponse = {
  property: [
    { name: 'yarn.scheduler.capacity.root.queues', value: 'default' },
    { name: 'yarn.scheduler.capacity.root.default.capacity', value: '100' },
  ],
};

const mockNodeLabelsResponse = { nodeLabelInfo: [] };
const mockNodesResponse = { nodes: { node: [] } };
const mockNodeToLabelsResponse = { nodeToLabels: { entry: [] } };
const mockVersionResponse = { versionId: 1 };

describe('Read-Only Mode', () => {
  describe('when isReadOnly is false (writable mode)', () => {
    let store: ReturnType<typeof createSchedulerStore>;

    beforeEach(async () => {
      const mockApiClient = createMockApiClient(false);
      store = createSchedulerStore(mockApiClient as unknown as YarnApiClient);

      // Mock API responses
      vi.mocked(store.getState().apiClient.getScheduler).mockResolvedValue(mockSchedulerResponse);
      vi.mocked(store.getState().apiClient.getSchedulerConf).mockResolvedValue(mockConfigResponse);
      vi.mocked(store.getState().apiClient.getNodeLabels).mockResolvedValue(mockNodeLabelsResponse);
      vi.mocked(store.getState().apiClient.getNodes).mockResolvedValue(mockNodesResponse);
      vi.mocked(store.getState().apiClient.getNodeToLabels).mockResolvedValue(
        mockNodeToLabelsResponse,
      );
      vi.mocked(store.getState().apiClient.getSchedulerConfVersion).mockResolvedValue(
        mockVersionResponse,
      );

      await store.getState().loadInitialData();
    });

    it('should set isReadOnly to false after loading initial data', () => {
      expect(store.getState().isReadOnly).toBe(false);
    });

    it('should allow applying changes', async () => {
      // Stage a change
      store.getState().stageQueueChange('root.default', 'capacity', '50');
      expect(store.getState().stagedChanges).toHaveLength(1);

      // Mock successful validation and update
      vi.mocked(store.getState().apiClient.validateSchedulerConf).mockResolvedValue({
        validation: 'success',
      });
      vi.mocked(store.getState().apiClient.updateSchedulerConf).mockResolvedValue(undefined);

      // Should not throw
      await expect(store.getState().applyChanges()).resolves.not.toThrow();
    });

    it('should allow adding node labels', async () => {
      vi.mocked(store.getState().apiClient.addNodeLabels).mockResolvedValue(undefined);
      vi.mocked(store.getState().apiClient.getNodeLabels).mockResolvedValue(mockNodeLabelsResponse);

      // Should not throw
      await expect(store.getState().addNodeLabel('test-label', true)).resolves.not.toThrow();
    });

    it('should allow removing node labels', async () => {
      vi.mocked(store.getState().apiClient.removeNodeLabels).mockResolvedValue(undefined);
      vi.mocked(store.getState().apiClient.getNodeLabels).mockResolvedValue(mockNodeLabelsResponse);
      vi.mocked(store.getState().apiClient.getNodeToLabels).mockResolvedValue(
        mockNodeToLabelsResponse,
      );

      // Should not throw
      await expect(store.getState().removeNodeLabel('test-label')).resolves.not.toThrow();
    });

    it('should allow assigning nodes to labels', async () => {
      vi.mocked(store.getState().apiClient.replaceNodeToLabels).mockResolvedValue(undefined);
      vi.mocked(store.getState().apiClient.getNodeToLabels).mockResolvedValue(
        mockNodeToLabelsResponse,
      );

      // Should not throw
      await expect(
        store.getState().assignNodeToLabel('node1:8041', 'test-label'),
      ).resolves.not.toThrow();
    });
  });

  describe('when isReadOnly is true (read-only mode)', () => {
    let store: ReturnType<typeof createSchedulerStore>;

    beforeEach(async () => {
      const mockApiClient = createMockApiClient(true);
      store = createSchedulerStore(mockApiClient as unknown as YarnApiClient);

      // Mock API responses
      vi.mocked(store.getState().apiClient.getScheduler).mockResolvedValue(mockSchedulerResponse);
      vi.mocked(store.getState().apiClient.getSchedulerConf).mockResolvedValue(mockConfigResponse);
      vi.mocked(store.getState().apiClient.getNodeLabels).mockResolvedValue(mockNodeLabelsResponse);
      vi.mocked(store.getState().apiClient.getNodes).mockResolvedValue(mockNodesResponse);
      vi.mocked(store.getState().apiClient.getNodeToLabels).mockResolvedValue(
        mockNodeToLabelsResponse,
      );
      vi.mocked(store.getState().apiClient.getSchedulerConfVersion).mockResolvedValue(
        mockVersionResponse,
      );

      await store.getState().loadInitialData();
    });

    it('should set isReadOnly to true after loading initial data', () => {
      expect(store.getState().isReadOnly).toBe(true);
    });

    it('should allow staging changes', () => {
      // Staging should still work in read-only mode
      store.getState().stageQueueChange('root.default', 'capacity', '50');
      expect(store.getState().stagedChanges).toHaveLength(1);
    });

    it('should block applying changes with MUTATION_BLOCKED error', async () => {
      // Stage a change
      store.getState().stageQueueChange('root.default', 'capacity', '50');
      expect(store.getState().stagedChanges).toHaveLength(1);

      // Attempt to apply should throw
      await expect(store.getState().applyChanges()).rejects.toThrow(/read-only mode/i);
      await expect(store.getState().applyChanges()).rejects.toMatchObject({
        code: ERROR_CODES.MUTATION_BLOCKED,
      });
    });

    it('should block adding node labels with MUTATION_BLOCKED error', async () => {
      await expect(store.getState().addNodeLabel('test-label', true)).rejects.toThrow(
        /read-only mode/i,
      );
      await expect(store.getState().addNodeLabel('test-label', true)).rejects.toMatchObject({
        code: ERROR_CODES.MUTATION_BLOCKED,
      });

      // Verify API was never called
      expect(store.getState().apiClient.addNodeLabels).not.toHaveBeenCalled();
    });

    it('should block removing node labels with MUTATION_BLOCKED error', async () => {
      await expect(store.getState().removeNodeLabel('test-label')).rejects.toThrow(
        /read-only mode/i,
      );
      await expect(store.getState().removeNodeLabel('test-label')).rejects.toMatchObject({
        code: ERROR_CODES.MUTATION_BLOCKED,
      });

      // Verify API was never called
      expect(store.getState().apiClient.removeNodeLabels).not.toHaveBeenCalled();
    });

    it('should block assigning nodes to labels with MUTATION_BLOCKED error', async () => {
      await expect(store.getState().assignNodeToLabel('node1:8041', 'test-label')).rejects.toThrow(
        /read-only mode/i,
      );
      await expect(
        store.getState().assignNodeToLabel('node1:8041', 'test-label'),
      ).rejects.toMatchObject({
        code: ERROR_CODES.MUTATION_BLOCKED,
      });

      // Verify API was never called
      expect(store.getState().apiClient.replaceNodeToLabels).not.toHaveBeenCalled();
    });

    it('should allow clearing staged changes', () => {
      // Stage some changes
      store.getState().stageQueueChange('root.default', 'capacity', '50');
      store.getState().stageQueueChange('root.default', 'maximum-capacity', '80');
      expect(store.getState().stagedChanges).toHaveLength(2);

      // Clearing should work
      store.getState().clearAllChanges();
      expect(store.getState().stagedChanges).toHaveLength(0);
    });

    it('should allow reverting individual staged changes', () => {
      // Stage a change
      store.getState().stageQueueChange('root.default', 'capacity', '50');
      const changeId = store.getState().stagedChanges[0].id;
      expect(store.getState().stagedChanges).toHaveLength(1);

      // Reverting should work
      store.getState().revertChange(changeId);
      expect(store.getState().stagedChanges).toHaveLength(0);
    });
  });
});
