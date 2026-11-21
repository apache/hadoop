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
import { buildPropertyKey } from '~/utils/propertyUtils';
import type { QueuePropertyReader } from './capacityValidation';
import {
  getLabelPartitionAccessIssues,
  getAccessibleLabelRemovalIssues,
  isLabelListedInQueueHierarchy,
} from './capacityValidation';

const createStoreFromConfig = (
  config: Record<string, string>,
): QueuePropertyReader => ({
  hasQueueProperty: (queuePath, property) =>
    Object.prototype.hasOwnProperty.call(config, buildPropertyKey(queuePath, property)),
  getQueuePropertyValue: (queuePath, property) => ({
    value: config[buildPropertyKey(queuePath, property)] ?? '',
    isStaged: false,
  }),
});

describe('isLabelListedInQueueHierarchy', () => {
  it('returns false when no queue in the path declares accessible-node-labels', () => {
    const store = createStoreFromConfig({});

    expect(isLabelListedInQueueHierarchy('root.default', 'gpu', store)).toBe(false);
  });

  it('returns false when accessible-node-labels is explicitly empty', () => {
    const store = createStoreFromConfig({
      [buildPropertyKey('root.default', 'accessible-node-labels')]: '',
    });

    expect(isLabelListedInQueueHierarchy('root.default', 'gpu', store)).toBe(false);
  });

  it('does not inherit when the queue explicitly sets accessible-node-labels to empty', () => {
    const store = createStoreFromConfig({
      [buildPropertyKey('root.team', 'accessible-node-labels')]: 'gpu',
      [buildPropertyKey('root.team.child', 'accessible-node-labels')]: '',
    });

    expect(isLabelListedInQueueHierarchy('root.team.child', 'gpu', store)).toBe(false);
  });

  it('inherits gpu access from the parent when accessible-node-labels is unset on the child', () => {
    const store = createStoreFromConfig({
      [buildPropertyKey('root.team', 'accessible-node-labels')]: 'gpu',
    });

    expect(isLabelListedInQueueHierarchy('root.team.child', 'gpu', store)).toBe(true);
  });

  it('returns false for a label not granted by the inherited accessible-node-labels', () => {
    const store = createStoreFromConfig({
      [buildPropertyKey('root.team', 'accessible-node-labels')]: 'gpu',
    });

    expect(isLabelListedInQueueHierarchy('root.team.child', 'fpga', store)).toBe(false);
  });

  it('returns true when the label is listed on the queue', () => {
    const store = createStoreFromConfig({
      [buildPropertyKey('root.default', 'accessible-node-labels')]: 'gpu,label3',
    });

    expect(isLabelListedInQueueHierarchy('root.default', 'label3', store)).toBe(true);
  });

  it('returns true when the queue lists all labels via wildcard', () => {
    const store = createStoreFromConfig({
      [buildPropertyKey('root.default', 'accessible-node-labels')]: '*',
    });

    expect(isLabelListedInQueueHierarchy('root.default', 'label3', store)).toBe(true);
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

  it('blocks filling queue-partition capacity when the queue does not have the label', () => {
    const issues = getLabelPartitionAccessIssues(
      [createRow({ capacityValue: '50', maxCapacityValue: '100' })],
      'gpu',
      queueWithoutLabel,
    );

    expect(issues).toHaveLength(2);
    expect(issues[0]?.field).toBe('accessible-node-labels.gpu.capacity');
    expect(issues[1]?.field).toBe('accessible-node-labels.gpu.maximum-capacity');
  });

  it('allows label partition capacity when the child inherits access from its parent', () => {
    const store = createStoreFromConfig({
      [buildPropertyKey('root.team', 'accessible-node-labels')]: 'gpu',
    });

    const issues = getLabelPartitionAccessIssues(
      [createRow({ queuePath: 'root.team.child', capacityValue: '50' })],
      'gpu',
      store,
    );

    expect(issues).toEqual([]);
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

  it('allows removing access when queue-partition capacity is not there', () => {
    const issues = getAccessibleLabelRemovalIssues('root.default', 'fpga', new Map());

    expect(issues).toEqual([]);
  });

  it('allows wildcard access even when queue-partition capacity is configured', () => {
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
