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


import { describe, it, expect, beforeEach, vi } from 'vitest';
import { createSchedulerStore } from '~/stores/schedulerStore';
import type { YarnApiClient } from '~/lib/api/YarnApiClient';
import type { PlacementRule } from '~/types/features/placement-rules';
import { SPECIAL_VALUES } from '~/types/constants/special-values';
import { extractPlacementRulesFromConfig } from '~/features/placement-rules/utils/placementRulesUtils';
import { mergeStagedConfig } from '~/utils/configUtils';

// Mock the utils module
vi.mock('~/features/placement-rules/utils/placementRulesUtils');
vi.mock('~/utils/configUtils');

// Create mock API client
const createMockApiClient = () => ({
  getScheduler: vi.fn(),
  getSchedulerConf: vi.fn(),
  getNodeLabels: vi.fn(),
  getNodes: vi.fn(),
  getNodeToLabels: vi.fn(),
  getSchedulerConfVersion: vi.fn(),
  updateSchedulerConf: vi.fn(),
  getIsReadOnly: vi.fn(() => false),
});

// Helper to create store with mock API client
const createTestStore = () => {
  const mockApiClient = createMockApiClient();
  return createSchedulerStore(mockApiClient as unknown as YarnApiClient);
};

// Mock placement rules data
const mockPlacementRules: PlacementRule[] = [
  {
    type: 'user',
    matches: 'alice',
    policy: 'specified',
    value: 'root.users.alice',
    fallbackResult: 'skip',
  },
  {
    type: 'application',
    matches: 'spark-*',
    policy: 'specified',
    value: 'root.spark',
    fallbackResult: 'placeDefault',
  },
  {
    type: 'group',
    matches: 'production',
    policy: 'primaryGroup',
    fallbackResult: 'skip',
  },
];

describe('placementRulesSlice', () => {
  beforeEach(() => {
    vi.clearAllMocks();
  });

  describe('initial state', () => {
    it('should have correct initial state', () => {
      const store = createTestStore();
      const state = store.getState();

      expect(state.rules).toEqual([]);
      expect(state.originalRules).toEqual([]);
      expect(state.isLoadingRules).toBe(false);
      expect(state.rulesError).toBeNull();
      expect(state.selectedRuleIndex).toBeNull();
      expect(state.legacyRules).toBeNull();
      expect(state.formatWarning).toBeNull();
    });
  });

  describe('loadPlacementRules', () => {
    beforeEach(() => {
      // Mock mergeStagedConfig to return the input config as-is by default
      vi.mocked(mergeStagedConfig).mockImplementation((config) => config);
    });

    it('should load JSON format rules successfully', () => {
      const store = createTestStore();
      const mockExtract = vi.mocked(extractPlacementRulesFromConfig);

      // Mock config data
      store.setState({
        configData: new Map([[SPECIAL_VALUES.MAPPING_RULE_FORMAT_PROPERTY, 'json']]),
      });

      // Mock successful extraction
      mockExtract.mockReturnValue({
        format: 'json',
        rules: mockPlacementRules,
      });

      store.getState().loadPlacementRules();

      expect(store.getState().rules).toEqual(mockPlacementRules);
      expect(store.getState().originalRules).toEqual(mockPlacementRules);
      expect(store.getState().isLoadingRules).toBe(false);
      expect(store.getState().rulesError).toBeNull();
      expect(store.getState().isLegacyMode).toBe(false);
      expect(store.getState().formatWarning).toBeNull();
    });

    it('should handle legacy format and set legacy mode', () => {
      const store = createTestStore();
      const mockExtract = vi.mocked(extractPlacementRulesFromConfig);

      store.setState({ configData: new Map() });

      mockExtract.mockReturnValue({
        format: 'legacy',
        requiresMigration: true,
        legacyRules: 'u:alice:root.users.alice\\ng:production:root.production',
      });

      store.getState().loadPlacementRules();

      expect(store.getState().rules).toEqual([]);
      expect(store.getState().originalRules).toEqual([]);
      expect(store.getState().isLoadingRules).toBe(false);
      expect(store.getState().legacyRules).toBe(
        'u:alice:root.users.alice\\ng:production:root.production',
      );
      expect(store.getState().isLegacyMode).toBe(true);
      expect(store.getState().formatWarning).toBeNull();
    });

    it('should handle no rules configured', () => {
      const store = createTestStore();
      const mockExtract = vi.mocked(extractPlacementRulesFromConfig);

      store.setState({ configData: new Map() });

      mockExtract.mockReturnValue({
        format: 'none',
      });

      store.getState().loadPlacementRules();

      expect(store.getState().rules).toEqual([]);
      expect(store.getState().originalRules).toEqual([]);
      expect(store.getState().isLoadingRules).toBe(false);
    });

    it('should handle extraction errors', () => {
      const store = createTestStore();
      const mockExtract = vi.mocked(extractPlacementRulesFromConfig);

      store.setState({ configData: new Map() });

      mockExtract.mockImplementation(() => {
        throw new Error('Failed to parse rules');
      });

      store.getState().loadPlacementRules();

      expect(store.getState().rules).toEqual([]);
      expect(store.getState().isLoadingRules).toBe(false);
      expect(store.getState().rulesError).toBe('Failed to parse rules');
    });

    it('should set loading state correctly', () => {
      const store = createTestStore();
      const mockExtract = vi.mocked(extractPlacementRulesFromConfig);

      store.setState({ configData: new Map() });

      mockExtract.mockReturnValue({
        format: 'json',
        rules: mockPlacementRules,
      });

      // Check that loading state is set initially
      const loadFn = store.getState().loadPlacementRules;

      // Manually check the loading state transition
      store.setState({ isLoadingRules: false });
      loadFn();

      // After calling load, it should complete synchronously
      expect(store.getState().isLoadingRules).toBe(false);
      expect(store.getState().rules).toEqual(mockPlacementRules);
    });

    it('should consider staged changes when loading placement rules', () => {
      const store = createTestStore();
      const mockExtract = vi.mocked(extractPlacementRulesFromConfig);
      const mockGetMergedConfig = vi.mocked(mergeStagedConfig);

      // Mock original config with legacy format
      const originalConfig = new Map([
        ['yarn.scheduler.capacity.queue-mappings', 'u:alice:root.users.alice'],
      ]);

      // Mock staged changes that include migration to JSON
      const stagedChanges = [
        {
          id: 'change1',
          type: 'update' as const,
          queuePath: SPECIAL_VALUES.GLOBAL_QUEUE_PATH,
          property: 'yarn.scheduler.capacity.mapping-rule-format',
          oldValue: undefined,
          newValue: 'json',
          timestamp: Date.now(),
        },
        {
          id: 'change2',
          type: 'update' as const,
          queuePath: SPECIAL_VALUES.GLOBAL_QUEUE_PATH,
          property: SPECIAL_VALUES.MAPPING_RULE_JSON_PROPERTY,
          oldValue: undefined,
          newValue: JSON.stringify({ rules: mockPlacementRules }),
          timestamp: Date.now(),
        },
      ];

      // Mock merged config with staged changes applied
      const mergedConfig = new Map([
        ['yarn.scheduler.capacity.mapping-rule-format', 'json'],
        [SPECIAL_VALUES.MAPPING_RULE_JSON_PROPERTY, JSON.stringify({ rules: mockPlacementRules })],
      ]);

      store.setState({
        configData: originalConfig,
        stagedChanges,
      });

      mockGetMergedConfig.mockReturnValue(mergedConfig);
      mockExtract.mockReturnValue({
        format: 'json',
        rules: mockPlacementRules,
      });

      store.getState().loadPlacementRules();

      // Verify mergeStagedConfig was called with correct parameters
      expect(mockGetMergedConfig).toHaveBeenCalledWith(originalConfig, stagedChanges);

      // Verify that JSON format was detected and rules loaded
      expect(store.getState().rules).toEqual(mockPlacementRules);
      expect(store.getState().isLegacyMode).toBe(false);
    });
  });

  describe('addRule', () => {
    it('should add new rule and stage global change', () => {
      const store = createTestStore();
      const stageGlobalChangeSpy = vi.spyOn(store.getState(), 'stageGlobalChange');

      // Set initial rules
      store.setState({ rules: mockPlacementRules.slice(0, 2) });

      const newRule: PlacementRule = {
        type: 'user',
        matches: 'bob',
        policy: 'specified',
        value: 'root.users.bob',
        fallbackResult: 'skip',
      };

      store.getState().addRule(newRule);

      expect(store.getState().rules).toHaveLength(3);
      expect(store.getState().rules[2]).toEqual(newRule);

      expect(stageGlobalChangeSpy).toHaveBeenCalledWith(SPECIAL_VALUES.MAPPING_RULE_JSON_PROPERTY, {
        rules: [...mockPlacementRules.slice(0, 2), newRule],
      });
    });

    it('should handle adding rule to empty list', () => {
      const store = createTestStore();
      const stageGlobalChangeSpy = vi.spyOn(store.getState(), 'stageGlobalChange');

      store.setState({ rules: [] });

      const newRule: PlacementRule = {
        type: 'user',
        matches: '*',
        policy: 'primaryGroup',
        fallbackResult: 'placeDefault',
      };

      store.getState().addRule(newRule);

      expect(store.getState().rules).toHaveLength(1);
      expect(store.getState().rules[0]).toEqual(newRule);

      expect(stageGlobalChangeSpy).toHaveBeenCalledWith(SPECIAL_VALUES.MAPPING_RULE_JSON_PROPERTY, {
        rules: [newRule],
      });
    });
  });

  describe('updateRule', () => {
    it('should update existing rule and stage global change', () => {
      const store = createTestStore();
      const stageGlobalChangeSpy = vi.spyOn(store.getState(), 'stageGlobalChange');

      store.setState({ rules: [...mockPlacementRules] });

      const updates: Partial<PlacementRule> = {
        matches: 'alice,bob',
        value: 'root.users',
      };

      store.getState().updateRule(0, updates);

      expect(store.getState().rules[0]).toEqual({
        ...mockPlacementRules[0],
        ...updates,
      });

      const expectedRules = [...mockPlacementRules];
      expectedRules[0] = { ...expectedRules[0], ...updates };

      expect(stageGlobalChangeSpy).toHaveBeenCalledWith(SPECIAL_VALUES.MAPPING_RULE_JSON_PROPERTY, {
        rules: expectedRules,
      });
    });

    it('should handle updating all fields of a rule', () => {
      const store = createTestStore();
      store.setState({ rules: [...mockPlacementRules] });

      const completeUpdate: Partial<PlacementRule> = {
        type: 'group',
        matches: 'dev-team',
        policy: 'specified',
        value: 'root.development',
        fallbackResult: 'skip',
      };

      store.getState().updateRule(1, completeUpdate);

      expect(store.getState().rules[1]).toEqual({
        ...mockPlacementRules[1],
        ...completeUpdate,
      });
    });
  });

  describe('deleteRule', () => {
    it('should delete rule and stage global change', () => {
      const store = createTestStore();
      const stageGlobalChangeSpy = vi.spyOn(store.getState(), 'stageGlobalChange');

      store.setState({ rules: [...mockPlacementRules] });

      store.getState().deleteRule(1);

      expect(store.getState().rules).toHaveLength(2);
      expect(store.getState().rules).toEqual([mockPlacementRules[0], mockPlacementRules[2]]);

      expect(stageGlobalChangeSpy).toHaveBeenCalledWith(SPECIAL_VALUES.MAPPING_RULE_JSON_PROPERTY, {
        rules: [mockPlacementRules[0], mockPlacementRules[2]],
      });
    });

    it('should clear selection if deleted rule was selected', () => {
      const store = createTestStore();
      store.setState({
        rules: [...mockPlacementRules],
        selectedRuleIndex: 1,
      });

      store.getState().deleteRule(1);

      expect(store.getState().selectedRuleIndex).toBeNull();
    });

    it('should adjust selection index if after deleted rule', () => {
      const store = createTestStore();
      store.setState({
        rules: [...mockPlacementRules],
        selectedRuleIndex: 2,
      });

      store.getState().deleteRule(1);

      expect(store.getState().selectedRuleIndex).toBe(1);
    });

    it('should not adjust selection index if before deleted rule', () => {
      const store = createTestStore();
      store.setState({
        rules: [...mockPlacementRules],
        selectedRuleIndex: 0,
      });

      store.getState().deleteRule(2);

      expect(store.getState().selectedRuleIndex).toBe(0);
    });
  });

  describe('reorderRules', () => {
    it('should reorder rules and stage global change', () => {
      const store = createTestStore();
      const stageGlobalChangeSpy = vi.spyOn(store.getState(), 'stageGlobalChange');

      store.setState({ rules: [...mockPlacementRules] });

      // Move first rule to last position
      store.getState().reorderRules(0, 2);

      expect(store.getState().rules).toEqual([
        mockPlacementRules[1],
        mockPlacementRules[0],
        mockPlacementRules[2],
      ]);

      expect(stageGlobalChangeSpy).toHaveBeenCalledWith(SPECIAL_VALUES.MAPPING_RULE_JSON_PROPERTY, {
        rules: [mockPlacementRules[1], mockPlacementRules[0], mockPlacementRules[2]],
      });
    });

    it('should update selection index when moving selected rule', () => {
      const store = createTestStore();
      store.setState({
        rules: [...mockPlacementRules],
        selectedRuleIndex: 0,
      });

      store.getState().reorderRules(0, 2);

      expect(store.getState().selectedRuleIndex).toBe(1);
    });

    it('should adjust selection when moving rule before selected', () => {
      const store = createTestStore();
      store.setState({
        rules: [...mockPlacementRules],
        selectedRuleIndex: 2,
      });

      // Move first rule to drop zone 2
      store.getState().reorderRules(0, 2);

      expect(store.getState().selectedRuleIndex).toBe(2);
    });

    it('should adjust selection when moving rule after selected', () => {
      const store = createTestStore();
      store.setState({
        rules: [...mockPlacementRules],
        selectedRuleIndex: 0,
      });

      // Move last rule to position before selected
      store.getState().reorderRules(2, 0);

      expect(store.getState().selectedRuleIndex).toBe(1);
    });
  });

  describe('selectRule', () => {
    it('should select rule by index', () => {
      const store = createTestStore();
      store.setState({ rules: [...mockPlacementRules] });

      store.getState().selectRule(1);

      expect(store.getState().selectedRuleIndex).toBe(1);
    });

    it('should clear selection when null is passed', () => {
      const store = createTestStore();
      store.setState({
        rules: [...mockPlacementRules],
        selectedRuleIndex: 1,
      });

      store.getState().selectRule(null);

      expect(store.getState().selectedRuleIndex).toBeNull();
    });
  });

  describe('resetRulesChanges', () => {
    it('should reset rules to original state', () => {
      const store = createTestStore();
      const originalRules = mockPlacementRules.slice(0, 2);
      store.setState({
        rules: [...mockPlacementRules],
        originalRules: originalRules,
        selectedRuleIndex: 1,
      });

      store.getState().resetRulesChanges();

      expect(store.getState().rules).toEqual(originalRules);
      expect(store.getState().selectedRuleIndex).toBeNull();
    });
  });

  describe('migrateLegacyRules', () => {
    it('should migrate legacy rules successfully', async () => {
      const store = createTestStore();
      const stageGlobalChangeSpy = vi.spyOn(store.getState(), 'stageGlobalChange');

      store.setState({
        legacyRules: 'u:alice:root.users.alice',
      });

      await store.getState().migrateLegacyRules();

      // Should convert the legacy rule to JSON format
      expect(stageGlobalChangeSpy).toHaveBeenCalledWith(SPECIAL_VALUES.MAPPING_RULE_JSON_PROPERTY, {
        rules: [
          {
            type: 'user',
            matches: 'alice',
            policy: 'custom',
            customPlacement: 'root.users.alice',
            fallbackResult: 'placeDefault',
            create: true,
          },
        ],
      });

      // Should also stage the format change to JSON
      expect(stageGlobalChangeSpy).toHaveBeenCalledWith(
        'yarn.scheduler.capacity.mapping-rule-format',
        'json',
      );

      expect(store.getState().legacyRules).toBeNull();
      expect(store.getState().rules).toHaveLength(1);
      expect(store.getState().rules[0].matches).toBe('alice');
    });

    it('should handle migration errors', async () => {
      const store = createTestStore();

      store.setState({
        legacyRules: 'invalid-format',
      });

      await store.getState().migrateLegacyRules();

      expect(store.getState().rulesError).toBe(
        'Failed to convert rule "invalid-format": Invalid rule format',
      );
    });
  });

  describe('integration with staged changes', () => {
    it('should properly format rules object for staging', () => {
      const store = createTestStore();
      const stageGlobalChangeSpy = vi.spyOn(store.getState(), 'stageGlobalChange');

      const rule: PlacementRule = {
        type: 'user',
        matches: 'test',
        policy: 'primaryGroup',
        fallbackResult: 'skip',
      };

      store.getState().addRule(rule);

      expect(stageGlobalChangeSpy).toHaveBeenCalledWith(SPECIAL_VALUES.MAPPING_RULE_JSON_PROPERTY, {
        rules: [rule],
      });

      // Verify the staged change is properly formatted
      const stagedChanges = store.getState().stagedChanges;
      expect(stagedChanges).toHaveLength(1);
      expect(stagedChanges[0]).toMatchObject({
        type: 'update',
        queuePath: 'global',
        property: SPECIAL_VALUES.MAPPING_RULE_JSON_PROPERTY,
      });
    });

    it('should handle multiple operations in sequence', () => {
      const store = createTestStore();
      store.setState({ rules: [...mockPlacementRules] });

      // Add a rule
      const newRule: PlacementRule = {
        type: 'user',
        matches: 'test',
        policy: 'primaryGroup',
        fallbackResult: 'skip',
      };
      store.getState().addRule(newRule);

      // Update a rule
      store.getState().updateRule(0, { matches: 'alice,charlie' });

      // Delete a rule
      store.getState().deleteRule(2);

      // Reorder rules
      store.getState().reorderRules(0, 1);

      // Should have only one staged change (latest state)
      const stagedChanges = store.getState().stagedChanges;
      expect(stagedChanges).toHaveLength(1);
      expect(stagedChanges[0].property).toBe(SPECIAL_VALUES.MAPPING_RULE_JSON_PROPERTY);
    });
  });

  describe('error handling', () => {
    it('should clear error when loading rules successfully', () => {
      const store = createTestStore();
      const mockExtract = vi.mocked(extractPlacementRulesFromConfig);

      store.setState({
        configData: new Map(),
        rulesError: 'Previous error',
      });

      mockExtract.mockReturnValue({
        format: 'json',
        rules: mockPlacementRules,
      });

      store.getState().loadPlacementRules();

      expect(store.getState().rulesError).toBeNull();
    });
  });

  describe('auto-staging format when rules are changed', () => {
    beforeEach(() => {
      // Mock mergeStagedConfig to return the input config as-is by default
      vi.mocked(mergeStagedConfig).mockImplementation((config) => config);
    });

    it('should auto-stage format to json when updating a rule with legacy format but no legacy rules', () => {
      const store = createTestStore();
      const stageGlobalChangeSpy = vi.spyOn(store.getState(), 'stageGlobalChange');

      // Set up state with legacy format but no legacy rules
      store.setState({
        rules: [...mockPlacementRules],
        configData: new Map([['yarn.scheduler.capacity.mapping-rule-format', 'legacy']]),
        legacyRules: null, // No legacy rules exist
      });

      store.getState().updateRule(0, { matches: 'updated-user' });

      // Should have staged the format change
      expect(stageGlobalChangeSpy).toHaveBeenCalledWith(
        'yarn.scheduler.capacity.mapping-rule-format',
        'json',
      );

      // Should also stage the rule update
      expect(stageGlobalChangeSpy).toHaveBeenCalledWith(
        SPECIAL_VALUES.MAPPING_RULE_JSON_PROPERTY,
        expect.any(Object),
      );
    });

    it('should auto-stage format to json when deleting a rule with legacy format but no legacy rules', () => {
      const store = createTestStore();
      const stageGlobalChangeSpy = vi.spyOn(store.getState(), 'stageGlobalChange');

      // Set up state with legacy format but no legacy rules
      store.setState({
        rules: [...mockPlacementRules],
        configData: new Map([['yarn.scheduler.capacity.mapping-rule-format', 'legacy']]),
        legacyRules: null, // No legacy rules exist
      });

      store.getState().deleteRule(0);

      // Should have staged the format change
      expect(stageGlobalChangeSpy).toHaveBeenCalledWith(
        'yarn.scheduler.capacity.mapping-rule-format',
        'json',
      );

      // Should also stage the rule deletion
      expect(stageGlobalChangeSpy).toHaveBeenCalledWith(
        SPECIAL_VALUES.MAPPING_RULE_JSON_PROPERTY,
        expect.any(Object),
      );
    });

    it('should auto-stage format to json when reordering rules with legacy format but no legacy rules', () => {
      const store = createTestStore();
      const stageGlobalChangeSpy = vi.spyOn(store.getState(), 'stageGlobalChange');

      // Set up state with legacy format but no legacy rules
      store.setState({
        rules: [...mockPlacementRules],
        configData: new Map([['yarn.scheduler.capacity.mapping-rule-format', 'legacy']]),
        legacyRules: null, // No legacy rules exist
      });

      store.getState().reorderRules(0, 2);

      // Should have staged the format change
      expect(stageGlobalChangeSpy).toHaveBeenCalledWith(
        'yarn.scheduler.capacity.mapping-rule-format',
        'json',
      );

      // Should also stage the reordered rules
      expect(stageGlobalChangeSpy).toHaveBeenCalledWith(
        SPECIAL_VALUES.MAPPING_RULE_JSON_PROPERTY,
        expect.any(Object),
      );
    });

    it('should auto-stage format to json when format is missing and adding a rule', () => {
      const store = createTestStore();
      const stageGlobalChangeSpy = vi.spyOn(store.getState(), 'stageGlobalChange');

      // Set up state with no format specified
      store.setState({
        rules: [],
        configData: new Map(),
        legacyRules: null,
      });

      const newRule: PlacementRule = {
        type: 'user',
        matches: 'test',
        policy: 'primaryGroup',
        fallbackResult: 'skip',
      };

      store.getState().addRule(newRule);

      // Should have staged the format change
      expect(stageGlobalChangeSpy).toHaveBeenCalledWith(
        'yarn.scheduler.capacity.mapping-rule-format',
        'json',
      );
    });

    it('should NOT auto-stage format when actual legacy rules exist', () => {
      const store = createTestStore();
      const stageGlobalChangeSpy = vi.spyOn(store.getState(), 'stageGlobalChange');

      // Set up state with legacy format AND legacy rules present
      store.setState({
        rules: [...mockPlacementRules],
        configData: new Map([['yarn.scheduler.capacity.mapping-rule-format', 'legacy']]),
        legacyRules: 'u:alice:root.users.alice', // Legacy rules exist
      });

      store.getState().updateRule(0, { matches: 'updated-user' });

      // Should NOT have staged the format change (only the rule update)
      expect(stageGlobalChangeSpy).not.toHaveBeenCalledWith(
        'yarn.scheduler.capacity.mapping-rule-format',
        'json',
      );

      // Should still stage the rule update
      expect(stageGlobalChangeSpy).toHaveBeenCalledWith(
        SPECIAL_VALUES.MAPPING_RULE_JSON_PROPERTY,
        expect.any(Object),
      );
    });

    it('should NOT auto-stage format when format is already json', () => {
      const store = createTestStore();
      const stageGlobalChangeSpy = vi.spyOn(store.getState(), 'stageGlobalChange');

      // Set up state with json format
      store.setState({
        rules: [...mockPlacementRules],
        configData: new Map([['yarn.scheduler.capacity.mapping-rule-format', 'json']]),
        legacyRules: null,
      });

      store.getState().updateRule(0, { matches: 'updated-user' });

      // Should NOT have staged the format change
      expect(stageGlobalChangeSpy).not.toHaveBeenCalledWith(
        'yarn.scheduler.capacity.mapping-rule-format',
        'json',
      );

      // Should only stage the rule update
      expect(stageGlobalChangeSpy).toHaveBeenCalledWith(
        SPECIAL_VALUES.MAPPING_RULE_JSON_PROPERTY,
        expect.any(Object),
      );
    });
  });
});
