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


/**
 * Placement rules slice - handles state management for placement rules configuration
 */

import type { StateCreator } from 'zustand';
import type { WritableDraft } from 'immer';
import type { PlacementRule } from '~/types/features/placement-rules';
import { extractPlacementRulesFromConfig } from '~/features/placement-rules/utils/placementRulesUtils';
import { mergeStagedConfig } from '~/utils/configUtils';
import { SPECIAL_VALUES } from '~/types/constants/special-values';
import { migrateLegacyRules } from '~/features/placement-rules/utils/migration';
import type { SchedulerStore } from './types';

export interface PlacementRulesSlice {
  // State
  rules: PlacementRule[];
  originalRules: PlacementRule[];
  isLoadingRules: boolean;
  rulesError: string | null;
  selectedRuleIndex: number | null;
  legacyRules: string | null;
  isLegacyMode: boolean;
  formatWarning: string | null;

  // Actions
  loadPlacementRules: () => void;
  addRule: (rule: PlacementRule) => void;
  updateRule: (index: number, updates: Partial<PlacementRule>) => void;
  deleteRule: (index: number) => void;
  reorderRules: (fromIndex: number, toIndex: number) => void;
  selectRule: (index: number | null) => void;
  resetRulesChanges: () => void;
  migrateLegacyRules: () => Promise<void>;
}

const MAPPING_RULE_FORMAT_WARNING_MISSING =
  'Placement rule format is not set. Adding a rule will automatically stage Mapping Rule Format: JSON setting.';
const MAPPING_RULE_FORMAT_WARNING_LEGACY =
  'Placement rule format is set to "legacy". Adding a rule will automatically stage Mapping Rule Format: JSON setting, or you can manually update it.';

function getFormatWarningMessage(formatValue?: string | null): string | null {
  if (!formatValue || formatValue.trim() === '') {
    return MAPPING_RULE_FORMAT_WARNING_MISSING;
  }

  const normalized = formatValue.trim().toLowerCase();
  if (normalized === 'json') {
    return null;
  }

  if (normalized === 'legacy') {
    return MAPPING_RULE_FORMAT_WARNING_LEGACY;
  }

  return `Placement rule format is set to "${formatValue}". Update it to "json" so newly staged rules are applied when you commit the configuration.`;
}

/**
 * Helper function to auto-stage format to 'json' if needed
 * This is called when rules are modified (add, update, delete, reorder)
 */
function autoStageFormatIfNeeded(
  get: () => SchedulerStore,
  set: (fn: (state: WritableDraft<SchedulerStore>) => void) => void,
): void {
  const { stageGlobalChange, configData, stagedChanges, legacyRules } = get();

  const mergedConfig = mergeStagedConfig(configData, stagedChanges);
  const formatValue = mergedConfig.get(SPECIAL_VALUES.MAPPING_RULE_FORMAT_PROPERTY);
  const normalizedFormat = formatValue?.trim().toLowerCase() ?? '';

  if (!normalizedFormat) {
    stageGlobalChange(SPECIAL_VALUES.MAPPING_RULE_FORMAT_PROPERTY, 'json');
    set((state: WritableDraft<SchedulerStore>) => {
      state.formatWarning = null;
    });
  } else if (normalizedFormat === 'legacy' && !legacyRules) {
    // Format is legacy but no actual legacy rules exist - auto-stage to json
    stageGlobalChange(SPECIAL_VALUES.MAPPING_RULE_FORMAT_PROPERTY, 'json');
    set((state: WritableDraft<SchedulerStore>) => {
      state.formatWarning = null;
    });
  } else if (normalizedFormat !== 'json') {
    set((state: WritableDraft<SchedulerStore>) => {
      state.formatWarning = getFormatWarningMessage(formatValue);
    });
  }
}

export const createPlacementRulesSlice: StateCreator<
  SchedulerStore,
  [['zustand/immer', never]],
  [],
  PlacementRulesSlice
> = (set, get) => ({
  // Initial state
  rules: [],
  originalRules: [],
  isLoadingRules: false,
  rulesError: null,
  selectedRuleIndex: null,
  legacyRules: null,
  isLegacyMode: false,
  formatWarning: null,

  // Load rules from existing config data
  loadPlacementRules: () => {
    set((state) => {
      state.isLoadingRules = true;
      state.rulesError = null;
    });

    try {
      const configData = get().configData;
      const stagedChanges = get().stagedChanges;
      const mergedConfig = mergeStagedConfig(configData, stagedChanges);

      const formatValue = mergedConfig.get(SPECIAL_VALUES.MAPPING_RULE_FORMAT_PROPERTY);
      const formatWarning = getFormatWarningMessage(formatValue);

      // First, check the ORIGINAL config to detect if legacy rules exist
      const originalResponse = extractPlacementRulesFromConfig(configData);

      if (originalResponse.format === 'legacy' && originalResponse.requiresMigration) {
        // Legacy rules exist in original config
        const formatStaged = stagedChanges.some(
          (change) =>
            change.queuePath === SPECIAL_VALUES.GLOBAL_QUEUE_PATH &&
            change.property === SPECIAL_VALUES.MAPPING_RULE_FORMAT_PROPERTY &&
            change.newValue === 'json' &&
            change.type === 'update',
        );

        if (formatStaged) {
          // Migration is already staged, load staged JSON rules
          const jsonRulesStaged = stagedChanges.find(
            (change) =>
              change.queuePath === SPECIAL_VALUES.GLOBAL_QUEUE_PATH &&
              change.property === SPECIAL_VALUES.MAPPING_RULE_JSON_PROPERTY,
          );

          if (jsonRulesStaged && jsonRulesStaged.newValue) {
            try {
              const rulesData =
                typeof jsonRulesStaged.newValue === 'string'
                  ? JSON.parse(jsonRulesStaged.newValue)
                  : jsonRulesStaged.newValue;
              const rules = rulesData.rules || [];

              set((state) => {
                state.rules = rules;
                state.originalRules = rules;
                state.isLoadingRules = false;
                state.legacyRules = null;
                state.isLegacyMode = false;
                state.formatWarning = formatWarning;
              });
            } catch {
              set((state) => {
                state.rules = [];
                state.originalRules = [];
                state.isLoadingRules = false;
                state.isLegacyMode = false;
                state.formatWarning = formatWarning;
              });
            }
          } else {
            set((state) => {
              state.rules = [];
              state.originalRules = [];
              state.isLoadingRules = false;
              state.isLegacyMode = false;
              state.formatWarning = formatWarning;
            });
          }
        } else {
          // Legacy rules exist but migration not staged
          set((state) => {
            state.rules = [];
            state.originalRules = [];
            state.isLoadingRules = false;
            state.legacyRules = originalResponse.legacyRules || null;
            state.isLegacyMode = true;
            state.formatWarning = null;
          });
        }
      } else {
        // No legacy rules in original config - load rules from merged config
        const response = extractPlacementRulesFromConfig(mergedConfig);

        if (response.format === 'json' && response.rules) {
          const rules = response.rules;
          set((state) => {
            state.rules = rules;
            state.originalRules = rules;
            state.isLoadingRules = false;
            state.legacyRules = null;
            state.isLegacyMode = false;
            state.formatWarning = formatWarning;
          });
        } else {
          set((state) => {
            state.rules = [];
            state.originalRules = [];
            state.isLoadingRules = false;
            state.legacyRules = null;
            state.isLegacyMode = false;
            state.formatWarning = formatWarning;
          });
        }
      }
    } catch (error) {
      set((state) => {
        state.isLoadingRules = false;
        state.rulesError =
          error instanceof Error ? error.message : 'Failed to load placement rules';
        state.formatWarning = null;
      });
    }
  },

  // Add new rule
  addRule: (rule) => {
    const { rules, stageGlobalChange } = get();

    // Auto-stage format to json if needed
    autoStageFormatIfNeeded(get, set);

    const newRules = [...rules, rule];

    set((state) => {
      state.rules = newRules;
    });

    // Stage as global change with proper format
    const rulesConfig = { rules: newRules };
    stageGlobalChange(SPECIAL_VALUES.MAPPING_RULE_JSON_PROPERTY, rulesConfig);
  },

  // Update existing rule
  updateRule: (index, updates) => {
    const { rules, stageGlobalChange } = get();

    // Auto-stage format to json if needed
    autoStageFormatIfNeeded(get, set);

    const newRules = [...rules];
    newRules[index] = { ...newRules[index], ...updates };

    set((state) => {
      state.rules = newRules;
    });

    // Stage as global change with proper format
    const rulesConfig = { rules: newRules };
    stageGlobalChange(SPECIAL_VALUES.MAPPING_RULE_JSON_PROPERTY, rulesConfig);
  },

  // Delete rule
  deleteRule: (index) => {
    const { rules, stageGlobalChange } = get();

    // Auto-stage format to json if needed
    autoStageFormatIfNeeded(get, set);

    const newRules = rules.filter((_, i) => i !== index);

    set((state) => {
      state.rules = newRules;
      // Clear selection if the deleted rule was selected
      if (state.selectedRuleIndex === index) {
        state.selectedRuleIndex = null;
      } else if (state.selectedRuleIndex !== null && state.selectedRuleIndex > index) {
        // Adjust selection index if it's after the deleted item
        state.selectedRuleIndex = state.selectedRuleIndex - 1;
      }
    });

    // Stage as global change with proper format
    const rulesConfig = { rules: newRules };
    stageGlobalChange(SPECIAL_VALUES.MAPPING_RULE_JSON_PROPERTY, rulesConfig);
  },

  // Reorder rules
  // sourceIndex: the current position of the item being moved
  // destinationIndex: the drop zone index (position where item should be inserted)
  reorderRules: (sourceIndex, destinationIndex) => {
    const { rules, selectedRuleIndex, stageGlobalChange } = get();

    // Auto-stage format to json if needed
    autoStageFormatIfNeeded(get, set);

    // Validate indices
    if (
      sourceIndex < 0 ||
      sourceIndex >= rules.length ||
      destinationIndex < 0 ||
      destinationIndex > rules.length
    ) {
      console.warn('Invalid reorder indices:', {
        sourceIndex,
        destinationIndex,
        rulesLength: rules.length,
      });
      return;
    }

    // For drag and drop, the destination represents a drop zone
    // If dragging to a higher index, we need to adjust because the item will be removed first
    let targetIndex = destinationIndex;
    if (sourceIndex < destinationIndex) {
      targetIndex = destinationIndex - 1;
    }

    // Don't do anything if the item would end up in the same position
    if (sourceIndex === targetIndex) return;

    // Create a new array and move the item
    const newRules = [...rules];
    const [movedItem] = newRules.splice(sourceIndex, 1);
    newRules.splice(targetIndex, 0, movedItem);

    // Calculate new selection index
    let newSelectedIndex = selectedRuleIndex;

    if (selectedRuleIndex !== null) {
      if (selectedRuleIndex === sourceIndex) {
        // The selected item was moved
        newSelectedIndex = targetIndex;
      } else {
        // Adjust selection for other items affected by the move
        const minIndex = Math.min(sourceIndex, targetIndex);
        const maxIndex = Math.max(sourceIndex, targetIndex);

        if (selectedRuleIndex >= minIndex && selectedRuleIndex <= maxIndex) {
          if (sourceIndex < targetIndex) {
            // Item moved down, shift items in between up
            newSelectedIndex = selectedRuleIndex - 1;
          } else {
            // Item moved up, shift items in between down
            newSelectedIndex = selectedRuleIndex + 1;
          }
        }
      }
    }

    set((state) => {
      state.rules = newRules;
      state.selectedRuleIndex = newSelectedIndex;
    });

    // Stage as global change with proper format
    const rulesConfig = { rules: newRules };
    stageGlobalChange(SPECIAL_VALUES.MAPPING_RULE_JSON_PROPERTY, rulesConfig);
  },

  // Select rule for editing
  selectRule: (index) => {
    set((state) => {
      state.selectedRuleIndex = index;
    });
  },

  // Reset to original state
  resetRulesChanges: () => {
    const { originalRules } = get();
    set((state) => {
      state.rules = originalRules;
      state.selectedRuleIndex = null;
    });
  },

  // Migrate legacy rules to JSON format
  migrateLegacyRules: async () => {
    const { stageGlobalChange, legacyRules } = get();

    if (!legacyRules) {
      set((state) => {
        state.rulesError = null;
      });
      return;
    }

    try {
      // Use migration utility to convert legacy rules
      const result = migrateLegacyRules(legacyRules);

      if (result.success) {
        // Stage the migration as a global change with proper format
        const rulesConfig = { rules: result.rules };
        stageGlobalChange(SPECIAL_VALUES.MAPPING_RULE_JSON_PROPERTY, rulesConfig);

        // Also stage the format change to JSON
        stageGlobalChange(SPECIAL_VALUES.MAPPING_RULE_FORMAT_PROPERTY, 'json');

        // Update local state
        set((state) => {
          state.rules = result.rules;
          state.originalRules = result.rules;
          state.legacyRules = null;
          state.rulesError = null;
          state.isLegacyMode = false;
        });
      } else {
        // Show errors but don't close dialog
        const errorMessage = result.errors.join('\n');
        set((state) => {
          state.rulesError = errorMessage;
        });
      }
    } catch (error) {
      set((state) => {
        state.rulesError = error instanceof Error ? error.message : 'Failed to migrate rules';
      });
    }
  },
});
