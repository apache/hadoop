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
import { DagreLayout } from './DagreLayout';
import type { QueueInfo } from '~/types';

const buildQueue = (overrides: Partial<QueueInfo> = {}): QueueInfo => ({
  queueType: 'parent',
  queueName: 'root',
  queuePath: 'root',
  capacity: 0,
  usedCapacity: 0,
  maxCapacity: 100,
  absoluteCapacity: 0,
  absoluteUsedCapacity: 0,
  absoluteMaxCapacity: 100,
  numApplications: 0,
  numActiveApplications: 0,
  numPendingApplications: 0,
  state: 'RUNNING',
  creationMethod: 'static',
  queues: undefined,
  ...overrides,
});

const sampleTree: QueueInfo = buildQueue({
  queues: {
    queue: [
      buildQueue({
        queueName: 'engineering',
        queuePath: 'root.engineering',
        queues: {
          queue: [
            buildQueue({
              queueName: 'frontend',
              queuePath: 'root.engineering.frontend',
              queueType: 'leaf',
              queues: undefined,
            }),
          ],
        },
      }),
      buildQueue({
        queueName: 'marketing',
        queuePath: 'root.marketing',
        queueType: 'leaf',
      }),
    ],
  },
});

describe('DagreLayout', () => {
  it('produces consistent horizontal positions and bounds', () => {
    const layout = new DagreLayout({
      nodeWidth: 120,
      nodeHeight: 60,
      horizontalSpacing: 80,
      verticalSpacing: 70,
    });

    const positions = layout.calculatePositions(sampleTree);

    expect(positions.size).toBe(4);

    const root = positions.get('root');
    const engineering = positions.get('root.engineering');
    const marketing = positions.get('root.marketing');

    expect(root).toBeDefined();
    expect(engineering).toBeDefined();
    expect(marketing).toBeDefined();

    expect(engineering!.x).toBeGreaterThan(root!.x);
    expect(marketing!.x).toBeGreaterThan(root!.x);

    const bounds = layout.getBounds(positions);
    expect(bounds.minX).toBeLessThanOrEqual(root!.x);
    expect(bounds.maxX).toBeGreaterThanOrEqual(marketing!.x + marketing!.width);
    expect(bounds.minY).toBeLessThanOrEqual(root!.y);
    expect(bounds.maxY).toBeGreaterThanOrEqual(engineering!.y + engineering!.height);
  });

  it('supports vertical orientation', () => {
    const layout = new DagreLayout({
      nodeWidth: 100,
      nodeHeight: 50,
      orientation: 'vertical',
    });

    const positions = layout.calculatePositions(sampleTree);
    const root = positions.get('root');
    const child = positions.get('root.engineering');

    expect(root).toBeDefined();
    expect(child).toBeDefined();
    expect(child!.y).toBeGreaterThan(root!.y);
  });
});
