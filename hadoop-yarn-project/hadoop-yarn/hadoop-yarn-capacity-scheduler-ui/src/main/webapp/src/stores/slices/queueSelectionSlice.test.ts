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
import type { SchedulerInfo, QueueInfo } from '~/types';

const createMockApiClient = () => ({
  getScheduler: vi.fn(),
  getSchedulerConf: vi.fn(),
  getNodeLabels: vi.fn(),
  getNodes: vi.fn(),
  getNodeToLabels: vi.fn(),
  getSchedulerConfVersion: vi.fn(),
  updateSchedulerConf: vi.fn(),
  getIsReadOnly: vi.fn(() => false),
});

const createQueue = (
  queuePath: string,
  queueName: string,
  children: QueueInfo[] = [],
): QueueInfo => ({
  queuePath,
  queueName,
  queueType: children.length > 0 ? 'parent' : 'leaf',
  capacity: 100,
  usedCapacity: 0,
  maxCapacity: 100,
  absoluteCapacity: 100,
  absoluteMaxCapacity: 100,
  absoluteUsedCapacity: 0,
  numApplications: 0,
  numActiveApplications: 0,
  numPendingApplications: 0,
  state: 'RUNNING',
  resourcesUsed: {
    memory: 0,
    vCores: 0,
  },
  ...(children.length > 0 ? { queues: { queue: children } } : {}),
});

const createSchedulerData = (): SchedulerInfo => ({
  type: 'capacityScheduler',
  capacity: 100,
  usedCapacity: 0,
  maxCapacity: 100,
  queueName: 'root',
  queues: {
    queue: [createQueue('root.default', 'default')],
  },
});

const createTestStore = () => {
  const mockApiClient = createMockApiClient();
  return createSchedulerStore(mockApiClient as unknown as YarnApiClient);
};

describe('queueSelectionSlice', () => {
  beforeEach(() => {
    vi.clearAllMocks();
  });

  it('tracks template dialog requests and clears them explicitly', () => {
    const store = createTestStore();

    expect(store.getState().shouldOpenTemplateConfig).toBe(false);

    store.getState().requestTemplateConfigOpen();
    expect(store.getState().shouldOpenTemplateConfig).toBe(true);

    store.getState().clearTemplateConfigRequest();
    expect(store.getState().shouldOpenTemplateConfig).toBe(false);
  });

  it('resets template dialog request when selecting a queue', () => {
    const store = createTestStore();
    store.setState({
      schedulerData: createSchedulerData(),
      shouldOpenTemplateConfig: true,
    });

    store.getState().selectQueue('root.default');

    expect(store.getState().selectedQueuePath).toBe('root.default');
    expect(store.getState().shouldOpenTemplateConfig).toBe(false);
    expect(store.getState().isPropertyPanelOpen).toBe(true);
  });

  it('clears template dialog request when closing the property panel', () => {
    const store = createTestStore();
    store.setState({
      shouldOpenTemplateConfig: true,
      isPropertyPanelOpen: true,
      selectedQueuePath: 'root.default',
    });

    store.getState().setPropertyPanelOpen(false);

    expect(store.getState().shouldOpenTemplateConfig).toBe(false);
    expect(store.getState().selectedQueuePath).toBeNull();
    expect(store.getState().isPropertyPanelOpen).toBe(false);
  });
});
