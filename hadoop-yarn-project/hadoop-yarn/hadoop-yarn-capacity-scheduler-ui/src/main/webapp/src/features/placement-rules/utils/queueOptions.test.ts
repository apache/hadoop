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
import { getAllParentQueues, type QueuePropertyAccessor } from './queueOptions';
import { AUTO_CREATION_PROPS } from '~/types/constants/auto-creation';
import type { QueueInfo, SchedulerInfo } from '~/types';

function makeQueue(overrides: Partial<QueueInfo> & { queuePath: string }): QueueInfo {
  return {
    queueType: 'leaf',
    queueName: overrides.queuePath.split('.').pop() ?? overrides.queuePath,
    capacity: 0,
    usedCapacity: 0,
    maxCapacity: 100,
    absoluteCapacity: 0,
    absoluteMaxCapacity: 100,
    absoluteUsedCapacity: 0,
    numApplications: 0,
    numActiveApplications: 0,
    numPendingApplications: 0,
    state: 'RUNNING',
    ...overrides,
  } as QueueInfo;
}

function makeScheduler(children: QueueInfo[]): SchedulerInfo {
  return {
    queueName: 'root',
    queues: { queue: children },
  } as unknown as SchedulerInfo;
}

/**
 * Build a QueuePropertyAccessor from a map of `${queuePath}::${property}` to
 * `{ value, isStaged }`. Anything not present resolves to an unset, non-staged
 * value, mirroring the store's getQueuePropertyValue.
 */
function makeAccessor(
  entries: Record<string, { value: string; isStaged: boolean }>,
): QueuePropertyAccessor {
  return (queuePath, property) =>
    entries[`${queuePath}::${property}`] ?? { value: '', isStaged: false };
}

describe('getAllParentQueues', () => {
  it('includes queues that already have static children', () => {
    const scheduler = makeScheduler([
      makeQueue({
        queuePath: 'root.withKids',
        queueType: 'parent',
        queues: { queue: [makeQueue({ queuePath: 'root.withKids.child' })] },
      }),
      makeQueue({ queuePath: 'root.withKids.child' }),
    ]);

    const values = getAllParentQueues(scheduler).map((o) => o.value);
    expect(values).toContain('root');
    expect(values).toContain('root.withKids');
    expect(values).not.toContain('root.withKids.child');
  });

  it('excludes a plain leaf queue with no children', () => {
    const scheduler = makeScheduler([makeQueue({ queuePath: 'root.sibling' })]);
    const values = getAllParentQueues(scheduler).map((o) => o.value);
    expect(values).not.toContain('root.sibling');
  });

  it('includes a leaf queue whose live autoCreationEligibility is flexible', () => {
    const scheduler = makeScheduler([
      makeQueue({
        queuePath: 'root.sibling',
        autoCreationEligibility: AUTO_CREATION_PROPS.ELIGIBILITY_FLEXIBLE,
      }),
    ]);
    const values = getAllParentQueues(scheduler).map((o) => o.value);
    expect(values).toContain('root.sibling');
  });

  it('includes a leaf queue whose live autoCreationEligibility is legacy', () => {
    const scheduler = makeScheduler([
      makeQueue({
        queuePath: 'root.sibling',
        autoCreationEligibility: AUTO_CREATION_PROPS.ELIGIBILITY_LEGACY,
      }),
    ]);
    const values = getAllParentQueues(scheduler).map((o) => o.value);
    expect(values).toContain('root.sibling');
  });

  it('includes a leaf queue whose flexible Dynamic Queue Creation is staged on but not applied', () => {
    const scheduler = makeScheduler([makeQueue({ queuePath: 'root.sibling' })]);
    const accessor = makeAccessor({
      [`root.sibling::${AUTO_CREATION_PROPS.FLEXIBLE_ENABLED}`]: { value: 'true', isStaged: true },
    });

    const values = getAllParentQueues(scheduler, accessor).map((o) => o.value);
    expect(values).toContain('root.sibling');
  });

  it('includes a leaf queue whose legacy Dynamic Queue Creation is staged on but not applied', () => {
    const scheduler = makeScheduler([makeQueue({ queuePath: 'root.sibling' })]);
    const accessor = makeAccessor({
      [`root.sibling::${AUTO_CREATION_PROPS.LEGACY_ENABLED}`]: { value: 'true', isStaged: true },
    });

    const values = getAllParentQueues(scheduler, accessor).map((o) => o.value);
    expect(values).toContain('root.sibling');
  });

  it('excludes a queue whose staged disable overrides live flexible eligibility', () => {
    // Live scheduler still reports the queue as flexible, but the operator has
    // staged both auto-creation properties to false. The staged view must win.
    const scheduler = makeScheduler([
      makeQueue({
        queuePath: 'root.sibling',
        autoCreationEligibility: AUTO_CREATION_PROPS.ELIGIBILITY_FLEXIBLE,
      }),
    ]);
    const accessor = makeAccessor({
      [`root.sibling::${AUTO_CREATION_PROPS.FLEXIBLE_ENABLED}`]: { value: 'false', isStaged: true },
      [`root.sibling::${AUTO_CREATION_PROPS.LEGACY_ENABLED}`]: { value: 'false', isStaged: true },
    });

    const values = getAllParentQueues(scheduler, accessor).map((o) => o.value);
    expect(values).not.toContain('root.sibling');
  });

  it('falls back to live eligibility when neither property has a staged override', () => {
    // Accessor returns non staged values only, so the live flexible eligibility
    // remains the source of truth and the queue stays selectable.
    const scheduler = makeScheduler([
      makeQueue({
        queuePath: 'root.sibling',
        autoCreationEligibility: AUTO_CREATION_PROPS.ELIGIBILITY_FLEXIBLE,
      }),
    ]);
    const accessor = makeAccessor({});

    const values = getAllParentQueues(scheduler, accessor).map((o) => o.value);
    expect(values).toContain('root.sibling');
  });
});
