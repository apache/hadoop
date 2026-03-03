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
import type { NodeLabel } from '~/types/node-label';

describe('NodeLabel interface', () => {
  it('should accept basic node label definition', () => {
    const label: NodeLabel = {
      name: 'gpu',
      exclusivity: true,
    };

    expect(label.name).toBe('gpu');
    expect(label.exclusivity).toBe(true);
  });

  it('should accept node label with partition info', () => {
    const labelWithPartition: NodeLabel = {
      name: 'fpga',
      exclusivity: false,
      partitionName: 'fpga-partition',
      activeNMs: 5,
      totalResource: {
        memory: 32768,
        vCores: 16,
      },
    };

    expect(labelWithPartition.partitionName).toBe('fpga-partition');
    expect(labelWithPartition.activeNMs).toBe(5);
    expect(labelWithPartition.totalResource?.memory).toBe(32768);
  });

  it('should handle non-exclusive label', () => {
    const nonExclusiveLabel: NodeLabel = {
      name: 'ssd',
      exclusivity: false,
    };

    expect(nonExclusiveLabel.exclusivity).toBe(false);
  });
});
