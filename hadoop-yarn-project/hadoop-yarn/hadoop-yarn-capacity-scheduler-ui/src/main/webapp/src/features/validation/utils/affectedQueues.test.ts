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


import { describe, it, expect } from 'vitest';
import { getAffectedQueuesForValidation } from './affectedQueues';
import type { SchedulerInfo, QueueInfo, StagedChange } from '~/types';

describe('affectedQueues', () => {
  const createMockSchedulerData = (queues: QueueInfo[]): SchedulerInfo => ({
    type: 'capacityScheduler',
    capacity: 100,
    usedCapacity: 50,
    maxCapacity: 100,
    queueName: 'root',
    queues: {
      queue: queues,
    },
  });

  const createMockQueue = (
    queuePath: string,
    queueName: string,
    children?: QueueInfo[],
  ): QueueInfo => ({
    queuePath,
    queueName,
    queueType: children ? 'parent' : 'leaf',
    capacity: 50,
    usedCapacity: 0,
    maxCapacity: 100,
    absoluteCapacity: 50,
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
    ...(children && { queues: { queue: children } }),
  });

  describe('getAffectedQueuesForValidation', () => {
    it('should always include the current queue', () => {
      const affected = getAffectedQueuesForValidation('someProp', 'root.a', null);
      expect(affected).toEqual(['root.a']);
    });

    it('should not traverse templates when queue path targets template scope', () => {
      const mockData = createMockSchedulerData([
        createMockQueue('root.default', 'default'),
        createMockQueue('root.analytics', 'analytics'),
      ]);

      const affected = getAffectedQueuesForValidation(
        'capacity',
        'root.default.auto-queue-creation-v2.template',
        mockData,
      );

      expect(affected).toEqual(['root.default.auto-queue-creation-v2.template']);
    });

    it('should include parent queue for capacity changes', () => {
      const mockData = createMockSchedulerData([
        createMockQueue('root.parent', 'parent', [createMockQueue('root.parent.child', 'child')]),
      ]);

      const affected = getAffectedQueuesForValidation('capacity', 'root.parent.child', mockData);

      expect(affected).toContain('root.parent.child');
      expect(affected).toContain('root.parent');
    });

    it('should include parent queue for capacity changes when queue is staged', () => {
      const mockData = createMockSchedulerData([createMockQueue('root.parent', 'parent')]);

      const affected = getAffectedQueuesForValidation('capacity', 'root.parent.new', mockData);

      expect(affected).toContain('root.parent');
      expect(affected).toContain('root.parent.new');
    });

    it('should include staged siblings for capacity validation', () => {
      const mockData = createMockSchedulerData([createMockQueue('root.parent', 'parent')]);

      const stagedChanges: StagedChange[] = [
        {
          id: 'add-1',
          type: 'add',
          queuePath: 'root.parent.first',
          property: 'capacity',
          oldValue: undefined,
          newValue: '10',
          timestamp: Date.now(),
        },
      ];

      const affected = getAffectedQueuesForValidation(
        'capacity',
        'root.parent.second',
        mockData,
        stagedChanges,
      );

      expect(affected).toContain('root.parent');
      expect(affected).toContain('root.parent.first');
      expect(affected).toContain('root.parent.second');
    });

    it('should include child queues for capacity changes on parent', () => {
      const mockData = createMockSchedulerData([
        createMockQueue('root.parent', 'parent', [
          createMockQueue('root.parent.child1', 'child1'),
          createMockQueue('root.parent.child2', 'child2'),
        ]),
      ]);

      const affected = getAffectedQueuesForValidation('capacity', 'root.parent', mockData);

      expect(affected).toContain('root.parent');
      expect(affected).toContain('root.parent.child1');
      expect(affected).toContain('root.parent.child2');
    });

    it('should not include parent for root queue', () => {
      const mockData = createMockSchedulerData([createMockQueue('root', 'root')]);

      const affected = getAffectedQueuesForValidation('capacity', 'root', mockData);

      expect(affected).toEqual(['root']);
    });

    it('should include parent and children for state changes', () => {
      const mockData = createMockSchedulerData([
        createMockQueue('root.parent', 'parent', [
          createMockQueue('root.parent.middle', 'middle', [
            createMockQueue('root.parent.middle.child', 'child'),
          ]),
        ]),
      ]);

      const affected = getAffectedQueuesForValidation('state', 'root.parent.middle', mockData);

      expect(affected).toContain('root.parent.middle');
      expect(affected).toContain('root.parent');
      expect(affected).toContain('root.parent.middle.child');
    });

    it('should handle non-capacity properties without adding extra queues', () => {
      const mockData = createMockSchedulerData([
        createMockQueue('root.parent', 'parent', [createMockQueue('root.parent.child', 'child')]),
      ]);

      const affected = getAffectedQueuesForValidation(
        'user-limit-factor',
        'root.parent.child',
        mockData,
      );

      expect(affected).toEqual(['root.parent.child']);
    });
  });
});
