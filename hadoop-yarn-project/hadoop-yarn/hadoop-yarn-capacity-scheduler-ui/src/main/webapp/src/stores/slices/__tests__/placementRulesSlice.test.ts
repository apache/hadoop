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
import { createSchedulerStore } from '~/stores/schedulerStore';
import { YarnApiClient } from '~/lib/api/YarnApiClient';
import type { PlacementRule } from '~/types/features/placement-rules';

describe('placementRulesSlice - reorderRules', () => {
  const createTestStore = () => {
    const mockApiClient = new YarnApiClient('http://test.com', {});
    return createSchedulerStore(mockApiClient);
  };

  const createTestRule = (index: number): PlacementRule => ({
    type: 'user',
    matches: `user${index}`,
    policy: 'specified',
    value: `queue${index}`,
    create: false,
    fallbackResult: 'skip',
  });

  it('should reorder rules correctly when moving an item down', () => {
    const store = createTestStore();
    const initialRules = [
      createTestRule(0),
      createTestRule(1),
      createTestRule(2),
      createTestRule(3),
    ];

    // Set initial rules
    store.setState({ rules: initialRules });

    // Move item from index 0 to index 2
    store.getState().reorderRules(0, 2);

    const newRules = store.getState().rules;
    expect(newRules).toHaveLength(4);
    expect(newRules[0].matches).toBe('user1');
    expect(newRules[1].matches).toBe('user0'); // moved item
    expect(newRules[2].matches).toBe('user2');
    expect(newRules[3].matches).toBe('user3');
  });

  it('should reorder rules correctly when moving an item up', () => {
    const store = createTestStore();
    const initialRules = [
      createTestRule(0),
      createTestRule(1),
      createTestRule(2),
      createTestRule(3),
    ];

    // Set initial rules
    store.setState({ rules: initialRules });

    // Move item from index 3 to index 1
    store.getState().reorderRules(3, 1);

    const newRules = store.getState().rules;
    expect(newRules).toHaveLength(4);
    expect(newRules[0].matches).toBe('user0');
    expect(newRules[1].matches).toBe('user3'); // moved item
    expect(newRules[2].matches).toBe('user1');
    expect(newRules[3].matches).toBe('user2');
  });

  it('should handle edge cases correctly', () => {
    const store = createTestStore();
    const initialRules = [createTestRule(0), createTestRule(1), createTestRule(2)];

    // Set initial rules
    store.setState({ rules: initialRules });

    // Move first to last
    store.getState().reorderRules(0, 3);
    expect(store.getState().rules[0].matches).toBe('user1');
    expect(store.getState().rules[1].matches).toBe('user2');
    expect(store.getState().rules[2].matches).toBe('user0');

    // Reset and move last to first
    store.setState({ rules: [createTestRule(0), createTestRule(1), createTestRule(2)] });
    store.getState().reorderRules(2, 0);
    expect(store.getState().rules[0].matches).toBe('user2');
    expect(store.getState().rules[1].matches).toBe('user0');
    expect(store.getState().rules[2].matches).toBe('user1');
  });

  it('should not change rules when indices are the same', () => {
    const store = createTestStore();
    const initialRules = [createTestRule(0), createTestRule(1), createTestRule(2)];

    store.setState({ rules: initialRules });
    const rulesBefore = [...store.getState().rules];

    store.getState().reorderRules(1, 1);

    expect(store.getState().rules).toEqual(rulesBefore);
  });

  it('should handle invalid indices gracefully', () => {
    const store = createTestStore();
    const initialRules = [createTestRule(0), createTestRule(1)];

    store.setState({ rules: initialRules });
    const rulesBefore = [...store.getState().rules];

    // Test negative indices
    store.getState().reorderRules(-1, 1);
    expect(store.getState().rules).toEqual(rulesBefore);

    // Test out of bounds indices
    store.getState().reorderRules(0, 10);
    expect(store.getState().rules).toEqual(rulesBefore);

    store.getState().reorderRules(10, 0);
    expect(store.getState().rules).toEqual(rulesBefore);
  });

  it('should update selected index correctly when reordering', () => {
    const store = createTestStore();
    const initialRules = [
      createTestRule(0),
      createTestRule(1),
      createTestRule(2),
      createTestRule(3),
    ];

    store.setState({ rules: initialRules, selectedRuleIndex: 1 });

    // Move selected item down
    store.getState().reorderRules(1, 3);
    expect(store.getState().selectedRuleIndex).toBe(2); // Adjusted for the move

    // Reset and move item before selection
    store.setState({ rules: initialRules, selectedRuleIndex: 2 });
    store.getState().reorderRules(0, 4);
    expect(store.getState().selectedRuleIndex).toBe(1); // Selection shifts down

    // Reset and move item after selection
    store.setState({ rules: initialRules, selectedRuleIndex: 1 });
    store.getState().reorderRules(3, 0);
    expect(store.getState().selectedRuleIndex).toBe(2); // Selection shifts up
  });
});
