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
import { getHostWildcardToClearOnUnassign } from './hostWildcardValidation';

describe('hostWildcardLabels', () => {
  describe('getHostWildcardToClearOnUnassign', () => {
    it('returns host:0 when it mirrors the only labeled NM on the host', () => {
      const nodeToLabels: NodeToLabelMapping[] = [
        { nodeId: 'localhost:8041', nodeLabels: ['label3'] },
        { nodeId: 'localhost:0', nodeLabels: ['label3'] },
      ];

      expect(getHostWildcardToClearOnUnassign('localhost:8041', nodeToLabels)).toBe(
        'localhost:0',
      );
    });

    it('returns null when the NM has a different label than the wildcard', () => {
      const nodeToLabels: NodeToLabelMapping[] = [
        { nodeId: 'localhost:8041', nodeLabels: ['p2'] },
        { nodeId: 'localhost:8042', nodeLabels: ['p1'] },
        { nodeId: 'localhost:0', nodeLabels: ['p1'] },
      ];

      expect(getHostWildcardToClearOnUnassign('localhost:8041', nodeToLabels)).toBeNull();
    });

    it('returns null when other labeled NMs remain on the same host', () => {
      const nodeToLabels: NodeToLabelMapping[] = [
        { nodeId: 'localhost:8041', nodeLabels: ['p1'] },
        { nodeId: 'localhost:8042', nodeLabels: ['p1'] },
        { nodeId: 'localhost:0', nodeLabels: ['p1'] },
      ];

      expect(getHostWildcardToClearOnUnassign('localhost:8041', nodeToLabels)).toBeNull();
    });

    it('returns null when only the NM is labeled via the UI', () => {
      const nodeToLabels: NodeToLabelMapping[] = [
        { nodeId: 'localhost:8041', nodeLabels: ['label3'] },
      ];

      expect(getHostWildcardToClearOnUnassign('localhost:8041', nodeToLabels)).toBeNull();
    });

    it('returns null when the NM being unassigned has no labels', () => {
      const nodeToLabels: NodeToLabelMapping[] = [
        { nodeId: 'localhost:8041', nodeLabels: [] },
        { nodeId: 'localhost:0', nodeLabels: ['label3'] },
      ];

      expect(getHostWildcardToClearOnUnassign('localhost:8041', nodeToLabels)).toBeNull();
    });

    it('supports bracketed IPv6 node ids', () => {
      const nodeToLabels: NodeToLabelMapping[] = [
        { nodeId: '[2001:db8::1]:8041', nodeLabels: ['label3'] },
        { nodeId: '[2001:db8::1]:0', nodeLabels: ['label3'] },
      ];

      expect(getHostWildcardToClearOnUnassign('[2001:db8::1]:8041', nodeToLabels)).toBe(
        '[2001:db8::1]:0',
      );
    });

    it('supports unbracketed IPv6 node ids', () => {
      const nodeToLabels: NodeToLabelMapping[] = [
        { nodeId: '2001:db8::1:8041', nodeLabels: ['label3'] },
        { nodeId: '2001:db8::1:0', nodeLabels: ['label3'] },
      ];

      expect(getHostWildcardToClearOnUnassign('2001:db8::1:8041', nodeToLabels)).toBe(
        '2001:db8::1:0',
      );
    });
  });
});
