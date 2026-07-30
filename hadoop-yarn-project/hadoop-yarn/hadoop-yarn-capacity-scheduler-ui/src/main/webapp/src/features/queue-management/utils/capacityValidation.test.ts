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
import type { CapacityRowDraft } from '~/stores/slices/capacityEditorSlice';
import {
  getLabelPartitionAccessIssues,
  getAccessibleLabelRemovalIssues,
  isLabelListedInQueue,
} from './capacityValidation';

describe('isLabelListedInQueue', () => {
  it('returns false when the queue has no accessible-node-labels property', () => {
    const store = {
      hasQueueProperty: () => false,
      getQueuePropertyValue: () => ({ value: '', isStaged: false }),
    };

    expect(isLabelListedInQueue('root.default', 'gpu', store)).toBe(false);
  });

  it('returns false when accessible-node-labels is empty', () => {
    const store = {
      hasQueueProperty: () => true,
      getQueuePropertyValue: () => ({ value: '', isStaged: false }),
    };

    expect(isLabelListedInQueue('root.default', 'gpu', store)).toBe(false);
  });

  it('returns true when the label is listed on the queue', () => {
    const store = {
      hasQueueProperty: () => true,
      getQueuePropertyValue: () => ({ value: 'gpu,label3', isStaged: false }),
    };

    expect(isLabelListedInQueue('root.default', 'label3', store)).toBe(true);
  });

  it('returns true when the queue lists all labels via wildcard', () => {
    const store = {
      hasQueueProperty: () => true,
      getQueuePropertyValue: () => ({ value: '*', isStaged: false }),
    };

    expect(isLabelListedInQueue('root.default', 'label3', store)).toBe(true);
  });
});

describe('getLabelPartitionAccessIssues', () => {
  const createRow = (overrides: Partial<CapacityRowDraft> = {}): CapacityRowDraft => ({
    queuePath: 'root.default',
    queueName: 'default',
    isOrigin: false,
    isNew: false,
    hasStagedChange: false,
    mode: 'simple',
    baseMode: 'simple',
    baseCapacityValue: '',
    baseMaxCapacityValue: '',
    capacityValue: '',
    maxCapacityValue: '',
    vectorCapacity: [],
    vectorMaxCapacity: [],
    ...overrides,
  });

  const queueWithLabel = {
    hasQueueProperty: () => true,
    getQueuePropertyValue: () => ({ value: 'gpu,label3', isStaged: false }),
  };

  const queueWithoutLabel = {
    hasQueueProperty: () => false,
    getQueuePropertyValue: () => ({ value: '', isStaged: false }),
  };

  it('returns no issues for the default partition', () => {
    const issues = getLabelPartitionAccessIssues(
      [createRow({ capacityValue: '50' })],
      null,
      queueWithoutLabel,
    );

    expect(issues).toEqual([]);
  });

  it('returns no issues when the queue lists the label in accessible-node-labels', () => {
    const issues = getLabelPartitionAccessIssues(
      [createRow({ capacityValue: '50' })],
      'gpu',
      queueWithLabel,
    );

    expect(issues).toEqual([]);
  });

  it('blocks non-empty label partition capacity when the queue does not list the label', () => {
    const issues = getLabelPartitionAccessIssues(
      [createRow({ capacityValue: '50', maxCapacityValue: '100' })],
      'gpu',
      queueWithoutLabel,
    );

    expect(issues).toHaveLength(2);
    expect(issues[0]?.field).toBe('accessible-node-labels.gpu.capacity');
    expect(issues[1]?.field).toBe('accessible-node-labels.gpu.maximum-capacity');
  });
});

describe('getAccessibleLabelRemovalIssues', () => {
  it('blocks removing a label that still has partition capacity configured', () => {
    const config = new Map<string, string>([
      [
        'yarn.scheduler.capacity.root.default.accessible-node-labels.gpu.capacity',
        '50',
      ],
    ]);

    const issues = getAccessibleLabelRemovalIssues('root.default', 'fpga', config);

    expect(issues).toHaveLength(1);
    expect(issues[0]?.field).toBe('accessible-node-labels');
    expect(issues[0]?.message).toContain('gpu');
  });

  it('allows removing access when partition capacity is cleared', () => {
    const issues = getAccessibleLabelRemovalIssues('root.default', 'fpga', new Map());

    expect(issues).toEqual([]);
  });

  it('allows wildcard access even when partition capacity is configured', () => {
    const config = new Map<string, string>([
      [
        'yarn.scheduler.capacity.root.default.accessible-node-labels.gpu.capacity',
        '50',
      ],
    ]);

    const issues = getAccessibleLabelRemovalIssues('root.default', '*', config);

    expect(issues).toEqual([]);
  });
});
