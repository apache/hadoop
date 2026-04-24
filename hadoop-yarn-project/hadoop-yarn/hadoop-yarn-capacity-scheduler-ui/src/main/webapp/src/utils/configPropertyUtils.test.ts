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
import { getQueueProperties } from './configPropertyUtils';

describe('configPropertyUtils', () => {
  const createTestConfigData = () => {
    return new Map([
      // Queue properties
      ['yarn.scheduler.capacity.root.capacity', '100'],
      ['yarn.scheduler.capacity.root.queues', 'default,production'],
      ['yarn.scheduler.capacity.root.default.capacity', '40'],
      ['yarn.scheduler.capacity.root.default.maximum-capacity', '60'],
      ['yarn.scheduler.capacity.root.default.state', 'RUNNING'],
      ['yarn.scheduler.capacity.root.production.capacity', '60'],
      ['yarn.scheduler.capacity.root.production.state', 'RUNNING'],
      // Nested properties (should be skipped for queue properties)
      ['yarn.scheduler.capacity.root.production.accessible-node-labels.gpu.capacity', '50'],
      // Global properties
      ['yarn.scheduler.capacity.maximum-applications', '10000'],
      ['yarn.scheduler.capacity.resource-calculator', 'DefaultResourceCalculator'],
      ['yarn.scheduler.capacity.node-locality-delay', '40'],
      ['yarn.resourcemanager.scheduler.class', 'CapacityScheduler'],
      ['yarn.acl.enable', 'true'],
    ]);
  };

  describe('getQueueProperties', () => {
    it('should return all direct properties for a queue', () => {
      const configData = createTestConfigData();
      const props = getQueueProperties(configData, 'root.default');

      expect(props).toEqual({
        capacity: '40',
        'maximum-capacity': '60',
        state: 'RUNNING',
      });
    });

    it('should return properties for root queue', () => {
      const configData = createTestConfigData();
      const props = getQueueProperties(configData, 'root');

      expect(props).toEqual({
        capacity: '100',
        queues: 'default,production',
      });
    });

    it('should skip nested properties', () => {
      const configData = createTestConfigData();
      const props = getQueueProperties(configData, 'root.production');

      expect(props).toEqual({
        capacity: '60',
        state: 'RUNNING',
      });
      // Should not include accessible-node-labels.gpu.capacity
      expect(props['accessible-node-labels.gpu.capacity']).toBeUndefined();
    });

    it('should return empty object for non-existent queue', () => {
      const configData = createTestConfigData();
      const props = getQueueProperties(configData, 'root.nonexistent');

      expect(props).toEqual({});
    });
  });
});
