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
import type { NodeToLabelMapping } from '~/types';
import { getNodeIdsToClearOnUnassign } from '../nodeLabelHostWildcard';

describe('nodeLabelHostWildcard', () => {
  describe('getNodeIdsToClearOnUnassign', () => {
    it('clears host:0 when it mirrors the only labeled NM on the host', () => {
      const nodeToLabels: NodeToLabelMapping[] = [
        { nodeId: 'localhost:8041', nodeLabels: ['label3'] },
        { nodeId: 'localhost:0', nodeLabels: ['label3'] },
      ];

      expect(getNodeIdsToClearOnUnassign('localhost:8041', nodeToLabels)).toEqual([
        'localhost:8041',
        'localhost:0',
      ]);
    });

    it('clears host:0 and sibling NMs when unassigning a host-level label', () => {
      const nodeToLabels: NodeToLabelMapping[] = [
        { nodeId: 'ccycloud-2.example.com:8041', nodeLabels: ['label4'] },
        { nodeId: 'ccycloud-2.example.com:8042', nodeLabels: ['label4'] },
        { nodeId: 'ccycloud-2.example.com:0', nodeLabels: ['label4'] },
      ];

      expect(getNodeIdsToClearOnUnassign('ccycloud-2.example.com:8041', nodeToLabels)).toEqual([
        'ccycloud-2.example.com:8041',
        'ccycloud-2.example.com:0',
        'ccycloud-2.example.com:8042',
      ]);
    });

    it('only clears the NM when it has a different label than the host-level mapping', () => {
      const nodeToLabels: NodeToLabelMapping[] = [
        { nodeId: 'localhost:8041', nodeLabels: ['p2'] },
        { nodeId: 'localhost:8042', nodeLabels: ['p1'] },
        { nodeId: 'localhost:0', nodeLabels: ['p1'] },
      ];

      expect(getNodeIdsToClearOnUnassign('localhost:8041', nodeToLabels)).toEqual([
        'localhost:8041',
      ]);
    });

    it('clears host:0 but not NMs with a different label on the same host', () => {
      const nodeToLabels: NodeToLabelMapping[] = [
        { nodeId: 'localhost:8041', nodeLabels: ['p2'] },
        { nodeId: 'localhost:8042', nodeLabels: ['p1'] },
        { nodeId: 'localhost:0', nodeLabels: ['p1'] },
      ];

      expect(getNodeIdsToClearOnUnassign('localhost:8042', nodeToLabels)).toEqual([
        'localhost:8042',
        'localhost:0',
      ]);
    });

    it('only clears the NM when only that NM is labeled via the UI', () => {
      const nodeToLabels: NodeToLabelMapping[] = [
        { nodeId: 'localhost:8041', nodeLabels: ['label3'] },
      ];

      expect(getNodeIdsToClearOnUnassign('localhost:8041', nodeToLabels)).toEqual([
        'localhost:8041',
      ]);
    });

    it('only clears the NM when it has no labels', () => {
      const nodeToLabels: NodeToLabelMapping[] = [
        { nodeId: 'localhost:8041', nodeLabels: [] },
        { nodeId: 'localhost:0', nodeLabels: ['label3'] },
      ];

      expect(getNodeIdsToClearOnUnassign('localhost:8041', nodeToLabels)).toEqual([
        'localhost:8041',
      ]);
    });

    it('supports bracketed IPv6 node ids', () => {
      const nodeToLabels: NodeToLabelMapping[] = [
        { nodeId: '[2001:db8::1]:8041', nodeLabels: ['label3'] },
        { nodeId: '[2001:db8::1]:8042', nodeLabels: ['label3'] },
        { nodeId: '[2001:db8::1]:0', nodeLabels: ['label3'] },
      ];

      expect(getNodeIdsToClearOnUnassign('[2001:db8::1]:8041', nodeToLabels)).toEqual([
        '[2001:db8::1]:8041',
        '[2001:db8::1]:0',
        '[2001:db8::1]:8042',
      ]);
    });

    it('supports unbracketed IPv6 node ids', () => {
      const nodeToLabels: NodeToLabelMapping[] = [
        { nodeId: '2001:db8::1:8041', nodeLabels: ['label3'] },
        { nodeId: '2001:db8::1:8042', nodeLabels: ['label3'] },
        { nodeId: '2001:db8::1:0', nodeLabels: ['label3'] },
      ];

      expect(getNodeIdsToClearOnUnassign('2001:db8::1:8041', nodeToLabels)).toEqual([
        '2001:db8::1:8041',
        '2001:db8::1:0',
        '2001:db8::1:8042',
      ]);
    });
  });
});
