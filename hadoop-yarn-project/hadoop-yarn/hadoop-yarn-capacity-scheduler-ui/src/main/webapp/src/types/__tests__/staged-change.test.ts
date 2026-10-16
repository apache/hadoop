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
import type { StagedChange, StagedChangeType } from '~/types/staged-change';

describe('StagedChange interface', () => {
  it('should accept add queue change', () => {
    const addChange: StagedChange = {
      id: 'change-123',
      type: 'add',
      queuePath: 'root.production.batch',
      property: 'capacity',
      newValue: '50',
      timestamp: Date.now(),
    };

    expect(addChange.type).toBe('add');
    expect(addChange.queuePath).toBe('root.production.batch');
    expect(addChange.property).toBe('capacity');
    expect(addChange.oldValue).toBeUndefined();
    expect(addChange.newValue).toBe('50');
  });

  it('should accept update property change', () => {
    const updateChange: StagedChange = {
      id: 'change-456',
      type: 'update',
      queuePath: 'root.production',
      property: 'capacity',
      oldValue: '70',
      newValue: '80',
      timestamp: Date.now(),
    };

    expect(updateChange.type).toBe('update');
    expect(updateChange.property).toBe('capacity');
    expect(updateChange.oldValue).toBe('70');
    expect(updateChange.newValue).toBe('80');
  });

  it('should accept remove queue change', () => {
    const removeChange: StagedChange = {
      id: 'change-789',
      type: 'remove',
      queuePath: 'root.development.experimental',
      property: '__queue__',
      oldValue: 'exists',
      timestamp: Date.now(),
    };

    expect(removeChange.type).toBe('remove');
    expect(removeChange.queuePath).toBe('root.development.experimental');
    expect(removeChange.property).toBe('__queue__');
  });

  it('should handle node label property changes', () => {
    const labelChange: StagedChange = {
      id: 'change-label-001',
      type: 'update',
      queuePath: 'root.production',
      property: 'accessible-node-labels.gpu.capacity',
      oldValue: '50',
      newValue: '80',
      timestamp: Date.now(),
      label: 'gpu',
    };

    expect(labelChange.property).toContain('accessible-node-labels');
    expect(labelChange.label).toBe('gpu');
  });

  it('should handle global configuration changes', () => {
    const globalChange: StagedChange = {
      id: 'change-global-001',
      type: 'update',
      queuePath: 'global',
      property: 'yarn.scheduler.capacity.maximum-applications',
      oldValue: '10000',
      newValue: '20000',
      timestamp: Date.now(),
    };

    expect(globalChange.queuePath).toBe('global');
    expect(globalChange.property).toBe('yarn.scheduler.capacity.maximum-applications');
  });

  it('should handle adding new property without old value', () => {
    const newPropertyChange: StagedChange = {
      id: 'change-new-prop',
      type: 'update',
      queuePath: 'root.production',
      property: 'user-limit-factor',
      newValue: '2',
      timestamp: Date.now(),
    };

    expect(newPropertyChange.oldValue).toBeUndefined();
    expect(newPropertyChange.newValue).toBe('2');
  });

  it('should handle removing property with only old value', () => {
    const removePropertyChange: StagedChange = {
      id: 'change-remove-prop',
      type: 'update',
      queuePath: 'root.production',
      property: 'default-node-label-expression',
      oldValue: 'gpu',
      timestamp: Date.now(),
    };

    expect(removePropertyChange.oldValue).toBe('gpu');
    expect(removePropertyChange.newValue).toBeUndefined();
  });
});

describe('StagedChangeType', () => {
  it('should only accept valid change types', () => {
    const addType: StagedChangeType = 'add';
    const updateType: StagedChangeType = 'update';
    const removeType: StagedChangeType = 'remove';

    expect(addType).toBe('add');
    expect(updateType).toBe('update');
    expect(removeType).toBe('remove');

    // TypeScript should prevent invalid values at compile time
    // const invalidType: StagedChangeType = 'invalid';
  });
});
