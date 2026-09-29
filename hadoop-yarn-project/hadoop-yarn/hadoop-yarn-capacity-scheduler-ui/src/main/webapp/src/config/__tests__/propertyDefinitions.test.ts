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
import { queuePropertyDefinitions } from '~/config/properties/queue-properties';
import {
  getPropertiesByCategory,
  getPropertyCategories,
  getPropertyDefinition,
} from '~/config/properties/helpers';
import { globalPropertyDefinitions } from '~/config/properties/global-properties';
import { CONFIG_PREFIXES } from '~/types';
import {
  capacityValueSchema,
  percentageSchema,
  positiveNumberSchema,
  integerSchema,
  aclFormatSchema,
} from '~/config/schemas/validation';
import { shouldShowProperty } from '~/utils/propertyConditions';

describe('propertyDefinitions', () => {
  describe('queuePropertyDefinitions', () => {
    const LEGACY_MODE_PROPERTY = 'yarn.scheduler.capacity.legacy-queue-mode.enabled';

    const createConditionOptions = ({
      property,
      capacity = '50',
      legacyMode = 'true',
      values: overrideValues = {},
    }: {
      property: (typeof queuePropertyDefinitions)[number];
      capacity?: string;
      legacyMode?: string;
      values?: Record<string, string>;
    }) => {
      const values: Record<string, string> = {
        capacity,
        ...overrideValues,
      };
      const globalValues: Record<string, string> = {
        [LEGACY_MODE_PROPERTY]: legacyMode,
      };

      return {
        scope: 'queue' as const,
        property,
        propertyValue: values[property.name] ?? '',
        values,
        globalValues,
        queuePath: 'root.test',
        queueInfo: null,
        schedulerInfo: null,
        stagedChanges: [],
        configData: new Map(),
        getValue: (name: string) => values[name],
        getGlobalValue: (name: string) => globalValues[name],
        getQueueValue: (queuePath: string, name: string) =>
          queuePath === 'root.test' ? values[name] : undefined,
        getConfigValue: () => undefined,
      };
    };

    it('includes essential YARN queue properties', () => {
      const propertyNames = queuePropertyDefinitions.map((p) => p.name);

      // Core properties
      expect(propertyNames).toContain('capacity');
      expect(propertyNames).toContain('maximum-capacity');
      expect(propertyNames).toContain('state');

      // User limits
      expect(propertyNames).toContain('minimum-user-limit-percent');
      expect(propertyNames).toContain('user-limit-factor');

      // Application control
      expect(propertyNames).toContain('maximum-applications');
      expect(propertyNames).toContain('maximum-am-resource-percent');

      // Security
      expect(propertyNames).toContain('acl_submit_applications');
      expect(propertyNames).toContain('acl_administer_queue');

      // Node label access control
      expect(propertyNames).toContain('accessible-node-labels');
      expect(propertyNames).toContain('default-node-label-expression');
    });

    it('has required capacity property', () => {
      const capacityProperty = queuePropertyDefinitions.find((p) => p.name === 'capacity');
      expect(capacityProperty).toBeDefined();
      expect(capacityProperty?.required).toBe(true);
      expect(capacityProperty?.category).toBe('capacity');
    });

    it('has proper categories for all properties', () => {
      const validCategories = [
        'capacity',
        'resource',
        'application-limits',
        'dynamic-queues',
        'node-labels',
        'scheduling',
        'security',
        'preemption',
      ];

      queuePropertyDefinitions.forEach((property) => {
        expect(validCategories).toContain(property.category);
      });
    });

    it('has validation rules for properties that need them', () => {
      const capacityProperty = queuePropertyDefinitions.find((p) => p.name === 'capacity');
      expect(capacityProperty?.validationRules).toBeDefined();
      expect(capacityProperty?.validationRules?.length).toBeGreaterThan(0);

      const userLimitProperty = queuePropertyDefinitions.find(
        (p) => p.name === 'minimum-user-limit-percent',
      );
      expect(userLimitProperty?.validationRules).toBeDefined();
    });

    it('has enum values for enum type properties', () => {
      const stateProperty = queuePropertyDefinitions.find((p) => p.name === 'state');
      expect(stateProperty?.type).toBe('enum');
      expect(stateProperty?.enumValues?.some((option) => option.value === 'RUNNING')).toBe(true);
      expect(stateProperty?.enumValues?.some((option) => option.value === 'STOPPED')).toBe(true);

      const orderingPolicy = queuePropertyDefinitions.find((p) => p.name === 'ordering-policy');
      expect(orderingPolicy?.type).toBe('enum');
      expect(orderingPolicy?.enumValues?.some((option) => option.value === 'fifo')).toBe(true);
      expect(orderingPolicy?.enumValues?.some((option) => option.value === 'fair')).toBe(true);
    });

    it('has conditional enableWhen for dependent properties', () => {
      const fairWeightProperty = queuePropertyDefinitions.find(
        (p) => p.name === 'ordering-policy.fair.enable-size-based-weight',
      );
      expect(Array.isArray(fairWeightProperty?.enableWhen)).toBe(true);
      const fairCondition = fairWeightProperty?.enableWhen?.[0];
      expect(fairCondition).toBeInstanceOf(Function);
      if (fairCondition && fairWeightProperty) {
        const baseValues: Record<string, string> = { 'ordering-policy': 'fair' };
        const result = fairCondition({
          scope: 'queue',
          property: fairWeightProperty,
          propertyValue: '',
          values: baseValues,
          globalValues: {},
          queuePath: 'root.a',
          queueInfo: null,
          schedulerInfo: null,
          stagedChanges: [],
          configData: new Map(),
          getValue: (name: string) => baseValues[name],
          getGlobalValue: () => undefined,
          getQueueValue: () => undefined,
          getConfigValue: () => undefined,
        });
        expect(result).toBe(true);

        const negative = fairCondition({
          scope: 'queue',
          property: fairWeightProperty,
          propertyValue: '',
          values: { 'ordering-policy': 'fifo' },
          globalValues: {},
          queuePath: 'root.a',
          queueInfo: null,
          schedulerInfo: null,
          stagedChanges: [],
          configData: new Map(),
          getValue: (name: string) => (name === 'ordering-policy' ? 'fifo' : undefined),
          getGlobalValue: () => undefined,
          getQueueValue: () => undefined,
          getConfigValue: () => undefined,
        });
        expect(negative).toBe(false);
      }

      const templateSupported = queuePropertyDefinitions.filter((p) => p.templateSupport);
      expect(templateSupported.length).toBeGreaterThan(0);
      expect(templateSupported.some((p) => p.name === 'capacity')).toBe(true);
    });

    it('disables state property for root queue', () => {
      const stateProperty = queuePropertyDefinitions.find((p) => p.name === 'state');
      expect(stateProperty).toBeDefined();
      if (!stateProperty) {
        return;
      }

      expect(Array.isArray(stateProperty.enableWhen)).toBe(true);
      const enableCondition = stateProperty.enableWhen?.[0];
      expect(enableCondition).toBeInstanceOf(Function);

      if (enableCondition) {
        // Test root queue - should be disabled
        const rootOptions = createConditionOptions({
          property: stateProperty,
          values: { state: 'RUNNING' },
        });
        rootOptions.queuePath = 'root';
        expect(enableCondition(rootOptions)).toBe(false);

        // Test non-root queue - should be enabled
        const nonRootOptions = createConditionOptions({
          property: stateProperty,
          values: { state: 'RUNNING' },
        });
        nonRootOptions.queuePath = 'root.default';
        expect(enableCondition(nonRootOptions)).toBe(true);

        // Test nested non-root queue - should be enabled
        const nestedOptions = createConditionOptions({
          property: stateProperty,
          values: { state: 'STOPPED' },
        });
        nestedOptions.queuePath = 'root.production.critical';
        expect(enableCondition(nestedOptions)).toBe(true);
      }
    });

    it('disables legacy auto-creation for queues with children', () => {
      const legacyAutoCreate = queuePropertyDefinitions.find(
        (p) => p.name === 'auto-create-child-queue.enabled',
      );

      expect(legacyAutoCreate).toBeDefined();
      if (!legacyAutoCreate) {
        return;
      }

      expect(Array.isArray(legacyAutoCreate.enableWhen)).toBe(true);
      const enableCondition = legacyAutoCreate.enableWhen?.[0];
      expect(enableCondition).toBeInstanceOf(Function);

      if (enableCondition) {
        // Test parent queue with children - should be disabled
        const parentWithChildrenOptions = {
          ...createConditionOptions({
            property: legacyAutoCreate,
            capacity: '50',
            values: { 'auto-create-child-queue.enabled': 'false' },
          }),
          queuePath: 'root.parent',
          queueInfo: {
            queueType: 'parent' as const,
            queueName: 'parent',
            queuePath: 'root.parent',
            capacity: 50,
            usedCapacity: 0,
            maxCapacity: 100,
            absoluteCapacity: 50,
            absoluteMaxCapacity: 100,
            absoluteUsedCapacity: 0,
            numApplications: 0,
            numActiveApplications: 0,
            numPendingApplications: 0,
            state: 'RUNNING' as const,
            queues: {
              queue: [
                {
                  queueType: 'leaf' as const,
                  queueName: 'child1',
                  queuePath: 'root.parent.child1',
                  capacity: 50,
                  usedCapacity: 0,
                  maxCapacity: 100,
                  absoluteCapacity: 25,
                  absoluteMaxCapacity: 100,
                  absoluteUsedCapacity: 0,
                  numApplications: 0,
                  numActiveApplications: 0,
                  numPendingApplications: 0,
                  state: 'RUNNING' as const,
                },
              ],
            },
          },
        };
        expect(enableCondition(parentWithChildrenOptions)).toBe(false);

        // Test parent queue without children - should be enabled
        const parentNoChildrenOptions = {
          ...createConditionOptions({
            property: legacyAutoCreate,
            capacity: '50',
            values: { 'auto-create-child-queue.enabled': 'false' },
          }),
          queuePath: 'root.parent',
          queueInfo: {
            queueType: 'parent' as const,
            queueName: 'parent',
            queuePath: 'root.parent',
            capacity: 50,
            usedCapacity: 0,
            maxCapacity: 100,
            absoluteCapacity: 50,
            absoluteMaxCapacity: 100,
            absoluteUsedCapacity: 0,
            numApplications: 0,
            numActiveApplications: 0,
            numPendingApplications: 0,
            state: 'RUNNING' as const,
            queues: {
              queue: [],
            },
          },
        };
        expect(enableCondition(parentNoChildrenOptions)).toBe(true);

        // Test with null queueInfo - should be enabled (default behavior)
        const nullQueueInfoOptions = createConditionOptions({
          property: legacyAutoCreate,
          capacity: '50',
        });
        expect(enableCondition(nullQueueInfoOptions)).toBe(true);

        // Test with undefined queues property - should be enabled
        const undefinedQueuesOptions = {
          ...createConditionOptions({
            property: legacyAutoCreate,
            capacity: '50',
          }),
          queueInfo: {
            queueType: 'parent' as const,
            queueName: 'parent',
            queuePath: 'root.parent',
            capacity: 50,
            usedCapacity: 0,
            maxCapacity: 100,
            absoluteCapacity: 50,
            absoluteMaxCapacity: 100,
            absoluteUsedCapacity: 0,
            numApplications: 0,
            numActiveApplications: 0,
            numPendingApplications: 0,
            state: 'RUNNING' as const,
          },
        };
        expect(enableCondition(undefinedQueuesOptions)).toBe(true);
      }
    });

    it('enables flexible auto-creation based on root queue children capacity mode', () => {
      const flexibleAutoCreate = queuePropertyDefinitions.find(
        (p) => p.name === 'auto-queue-creation-v2.enabled',
      );

      expect(flexibleAutoCreate).toBeDefined();
      if (!flexibleAutoCreate) {
        return;
      }

      const isEnabled = flexibleAutoCreate.enableWhen?.[0];
      expect(isEnabled).toBeInstanceOf(Function);
      if (!isEnabled) {
        return;
      }

      // Non-root queue: always enabled
      const nonRootOptions = createConditionOptions({
        property: flexibleAutoCreate,
        capacity: '2w',
        legacyMode: 'false',
      });
      expect(isEnabled(nonRootOptions)).toBe(true);

      // Root queue with no children: enabled
      const rootNoChildrenOptions = {
        ...createConditionOptions({
          property: flexibleAutoCreate,
          capacity: '100',
          legacyMode: 'false',
        }),
        queuePath: 'root',
        queueInfo: {
          queueType: 'parent' as const,
          queueName: 'root',
          queuePath: 'root',
          capacity: 100,
          usedCapacity: 0,
          maxCapacity: 100,
          absoluteCapacity: 100,
          absoluteMaxCapacity: 100,
          absoluteUsedCapacity: 0,
          numApplications: 0,
          numActiveApplications: 0,
          numPendingApplications: 0,
          state: 'RUNNING' as const,
          queues: {
            queue: [],
          },
        },
      };
      expect(isEnabled(rootNoChildrenOptions)).toBe(true);

      // LEGACY MODE TESTS - restrictions apply
      // Root queue with all weight-mode children: enabled
      const legacyRootWeightChildrenOptions = {
        ...createConditionOptions({
          property: flexibleAutoCreate,
          capacity: '100',
          legacyMode: 'true',
        }),
        queuePath: 'root',
        queueInfo: {
          queueType: 'parent' as const,
          queueName: 'root',
          queuePath: 'root',
          capacity: 100,
          usedCapacity: 0,
          maxCapacity: 100,
          absoluteCapacity: 100,
          absoluteMaxCapacity: 100,
          absoluteUsedCapacity: 0,
          numApplications: 0,
          numActiveApplications: 0,
          numPendingApplications: 0,
          state: 'RUNNING' as const,
          queues: {
            queue: [
              {
                queueType: 'leaf' as const,
                queueName: 'child1',
                queuePath: 'root.child1',
                capacity: 2,
                usedCapacity: 0,
                maxCapacity: 100,
                absoluteCapacity: 2,
                absoluteMaxCapacity: 100,
                absoluteUsedCapacity: 0,
                numApplications: 0,
                numActiveApplications: 0,
                numPendingApplications: 0,
                state: 'RUNNING' as const,
              },
              {
                queueType: 'leaf' as const,
                queueName: 'child2',
                queuePath: 'root.child2',
                capacity: 3,
                usedCapacity: 0,
                maxCapacity: 100,
                absoluteCapacity: 3,
                absoluteMaxCapacity: 100,
                absoluteUsedCapacity: 0,
                numApplications: 0,
                numActiveApplications: 0,
                numPendingApplications: 0,
                state: 'RUNNING' as const,
              },
            ],
          },
        },
        getQueueValue: (queuePath: string, name: string) => {
          if (name === 'capacity' && queuePath === 'root.child1') return '2w';
          if (name === 'capacity' && queuePath === 'root.child2') return '3w';
          return undefined;
        },
      };
      expect(isEnabled(legacyRootWeightChildrenOptions)).toBe(true);

      // Legacy mode: Root queue with percentage-mode children: disabled
      const legacyRootPercentChildrenOptions = {
        ...legacyRootWeightChildrenOptions,
        getQueueValue: (queuePath: string, name: string) => {
          if (name === 'capacity' && queuePath === 'root.child1') return '40';
          if (name === 'capacity' && queuePath === 'root.child2') return '60';
          return undefined;
        },
      };
      expect(isEnabled(legacyRootPercentChildrenOptions)).toBe(false);

      // Legacy mode: Root queue with mixed-mode children: disabled
      const legacyRootMixedChildrenOptions = {
        ...legacyRootWeightChildrenOptions,
        getQueueValue: (queuePath: string, name: string) => {
          if (name === 'capacity' && queuePath === 'root.child1') return '2w';
          if (name === 'capacity' && queuePath === 'root.child2') return '60';
          return undefined;
        },
      };
      expect(isEnabled(legacyRootMixedChildrenOptions)).toBe(false);

      // NON-LEGACY MODE TESTS - no restrictions
      // Non-legacy mode: enabled for root with percentage-mode children
      const nonLegacyRootPercentOptions = {
        ...createConditionOptions({
          property: flexibleAutoCreate,
          capacity: '100',
          legacyMode: 'false',
        }),
        queuePath: 'root',
        queueInfo: legacyRootWeightChildrenOptions.queueInfo,
        getQueueValue: (queuePath: string, name: string) => {
          // Children using percentage mode
          if (name === 'capacity' && queuePath === 'root.child1') return '40';
          if (name === 'capacity' && queuePath === 'root.child2') return '60';
          return undefined;
        },
      };
      expect(isEnabled(nonLegacyRootPercentOptions)).toBe(true);

      // Non-legacy mode: also enabled with weight-mode children
      const nonLegacyRootWeightOptions = {
        ...nonLegacyRootPercentOptions,
        getQueueValue: (queuePath: string, name: string) => {
          // Children using weight mode
          if (name === 'capacity' && queuePath === 'root.child1') return '2w';
          if (name === 'capacity' && queuePath === 'root.child2') return '3w';
          return undefined;
        },
      };
      expect(isEnabled(nonLegacyRootWeightOptions)).toBe(true);

      // Non-legacy mode: enabled even with mixed-mode children
      const nonLegacyRootMixedOptions = {
        ...nonLegacyRootPercentOptions,
        getQueueValue: (queuePath: string, name: string) => {
          if (name === 'capacity' && queuePath === 'root.child1') return '2w';
          if (name === 'capacity' && queuePath === 'root.child2') return '60';
          return undefined;
        },
      };
      expect(isEnabled(nonLegacyRootMixedOptions)).toBe(true);
    });

    it('shows correct auto-creation properties based on capacity and legacy mode', () => {
      const legacyAutoCreate = queuePropertyDefinitions.find(
        (p) => p.name === 'auto-create-child-queue.enabled',
      );
      const flexibleAutoCreate = queuePropertyDefinitions.find(
        (p) => p.name === 'auto-queue-creation-v2.enabled',
      );
      const flexibleMaxQueues = queuePropertyDefinitions.find(
        (p) => p.name === 'auto-queue-creation-v2.max-queues',
      );

      expect(legacyAutoCreate).toBeDefined();
      expect(flexibleAutoCreate).toBeDefined();
      expect(flexibleMaxQueues).toBeDefined();
      if (!legacyAutoCreate || !flexibleAutoCreate || !flexibleMaxQueues) {
        return;
      }

      // Legacy AQC should never show for root queue (only creates leaf queues)
      const rootLegacyOptions = createConditionOptions({
        property: legacyAutoCreate,
        capacity: '100',
        legacyMode: 'true',
      });
      rootLegacyOptions.queuePath = 'root';
      expect(shouldShowProperty(legacyAutoCreate, rootLegacyOptions)).toBe(false);

      // Flexible AQC should show for root in legacy mode (even with percentage capacity)
      const rootFlexibleLegacyOptions = createConditionOptions({
        property: flexibleAutoCreate,
        capacity: '100',
        legacyMode: 'true',
      });
      rootFlexibleLegacyOptions.queuePath = 'root';
      expect(shouldShowProperty(flexibleAutoCreate, rootFlexibleLegacyOptions)).toBe(true);

      // Legacy mode with weight capacity -> show only flexible auto-creation
      const legacyWeightOptions = createConditionOptions({
        property: flexibleAutoCreate,
        capacity: '2w',
        legacyMode: 'true',
      });
      expect(shouldShowProperty(flexibleAutoCreate, legacyWeightOptions)).toBe(true);
      expect(
        shouldShowProperty(
          legacyAutoCreate,
          createConditionOptions({
            property: legacyAutoCreate,
            capacity: '2w',
            legacyMode: 'true',
          }),
        ),
      ).toBe(false);

      // Legacy mode with percentage capacity -> show only legacy auto-creation
      const legacyPercentOptions = createConditionOptions({
        property: legacyAutoCreate,
        capacity: '50',
        legacyMode: 'true',
      });
      expect(shouldShowProperty(legacyAutoCreate, legacyPercentOptions)).toBe(true);
      expect(
        shouldShowProperty(
          flexibleAutoCreate,
          createConditionOptions({
            property: flexibleAutoCreate,
            capacity: '50',
            legacyMode: 'true',
          }),
        ),
      ).toBe(false);

      // Flexible max-queues aligns with flexible toggle visibility
      expect(
        shouldShowProperty(
          flexibleMaxQueues,
          createConditionOptions({
            property: flexibleMaxQueues,
            capacity: '2w',
            legacyMode: 'true',
            values: { 'auto-queue-creation-v2.enabled': 'true' },
          }),
        ),
      ).toBe(true);
      expect(
        shouldShowProperty(
          flexibleMaxQueues,
          createConditionOptions({
            property: flexibleMaxQueues,
            capacity: '50',
            legacyMode: 'true',
            values: { 'auto-queue-creation-v2.enabled': 'true' },
          }),
        ),
      ).toBe(false);

      // Non-legacy mode always shows flexible auto-creation and hides legacy
      expect(
        shouldShowProperty(
          flexibleAutoCreate,
          createConditionOptions({
            property: flexibleAutoCreate,
            capacity: '50',
            legacyMode: 'false',
          }),
        ),
      ).toBe(true);
      expect(
        shouldShowProperty(
          legacyAutoCreate,
          createConditionOptions({
            property: legacyAutoCreate,
            capacity: '50',
            legacyMode: 'false',
          }),
        ),
      ).toBe(false);
    });

    it('has accessible node labels properties', () => {
      const accessibleLabelsProperty = queuePropertyDefinitions.find(
        (p) => p.name === 'accessible-node-labels',
      );
      expect(accessibleLabelsProperty).toBeDefined();
      expect(accessibleLabelsProperty?.category).toBe('node-labels');
      expect(accessibleLabelsProperty?.required).toBe(false);
      expect(accessibleLabelsProperty?.type).toBe('string');
      expect(accessibleLabelsProperty?.validationRules).toBeDefined();

      const defaultExpressionProperty = queuePropertyDefinitions.find(
        (p) => p.name === 'default-node-label-expression',
      );
      expect(defaultExpressionProperty).toBeDefined();
      expect(defaultExpressionProperty?.category).toBe('node-labels');
      expect(defaultExpressionProperty?.required).toBe(false);
      expect(defaultExpressionProperty?.type).toBe('string');
    });

    it('validates accessible node labels correctly', () => {
      const accessibleLabelsProperty = queuePropertyDefinitions.find(
        (p) => p.name === 'accessible-node-labels',
      );
      const validator = accessibleLabelsProperty?.validationRules?.[0]?.validator;

      expect(validator).toBeDefined();
      if (validator) {
        // Valid cases
        expect(validator('')).toBe(true); // Empty for default partition
        expect(validator('*')).toBe(true); // All labels
        expect(validator('gpu')).toBe(true); // Single label
        expect(validator('gpu,cpu')).toBe(true); // Multiple labels
        expect(validator('gpu, cpu, fpga')).toBe(true); // With spaces

        // Invalid cases
        expect(validator('gpu,cpu,')).toBe(false); // Trailing comma
        expect(validator(',gpu')).toBe(false); // Leading comma
        expect(validator('gpu.cpu')).toBe(false); // Invalid character
        expect(validator('gpu cpu')).toBe(false); // Space instead of comma
      }
    });
  });

  describe('globalPropertyDefinitions', () => {
    it('includes global YARN properties', () => {
      const propertyNames = globalPropertyDefinitions.map((p) => p.name);

      expect(propertyNames).toContain(`${CONFIG_PREFIXES.BASE}.maximum-applications`);
      expect(propertyNames).toContain(`${CONFIG_PREFIXES.BASE}.maximum-am-resource-percent`);
      expect(propertyNames).toContain(`${CONFIG_PREFIXES.BASE}.resource-calculator`);
    });

    it('has correct enum values for resource calculator', () => {
      const resourceCalcProperty = globalPropertyDefinitions.find(
        (p) => p.name === `${CONFIG_PREFIXES.BASE}.resource-calculator`,
      );
      expect(resourceCalcProperty?.type).toBe('enum');
      expect(
        resourceCalcProperty?.enumValues?.some(
          (option) =>
            option.value === 'org.apache.hadoop.yarn.util.resource.DefaultResourceCalculator',
        ),
      ).toBe(true);
      expect(
        resourceCalcProperty?.enumValues?.some(
          (option) =>
            option.value === 'org.apache.hadoop.yarn.util.resource.DominantResourceCalculator',
        ),
      ).toBe(true);
    });
  });

  describe('helper functions', () => {
    describe('getPropertiesByCategory', () => {
      it('returns properties for a valid category', () => {
        const capacityProperties = getPropertiesByCategory('capacity');
        expect(capacityProperties.length).toBeGreaterThan(0);
        capacityProperties.forEach((prop) => {
          expect(prop.category).toBe('capacity');
        });
      });

      it('returns empty array for categories with no properties', () => {
        const nonExistentProperties = getPropertiesByCategory('nonexistent' as any);
        expect(nonExistentProperties).toEqual([]);
      });
    });

    describe('getPropertyCategories', () => {
      it('returns all valid categories', () => {
        const categories = getPropertyCategories();
        expect(categories).toContain('capacity');
        expect(categories).toContain('resource');
        expect(categories).toContain('application-limits');
        expect(categories).toContain('dynamic-queues');
        expect(categories).toContain('node-labels');
        expect(categories).toContain('scheduling');
        expect(categories).toContain('security');
        expect(categories).toContain('preemption');
      });
    });

    describe('getPropertyDefinition', () => {
      it('returns property definition for valid name', () => {
        const capacityProperty = getPropertyDefinition('capacity');
        expect(capacityProperty).toBeDefined();
        expect(capacityProperty?.name).toBe('capacity');
      });

      it('returns undefined for invalid name', () => {
        const invalidProperty = getPropertyDefinition('nonexistent-property');
        expect(invalidProperty).toBeUndefined();
      });
    });
  });

  describe('validation schemas', () => {
    describe('capacityValueSchema', () => {
      it('validates percentage values', () => {
        expect(capacityValueSchema.safeParse('50').success).toBe(true);
        expect(capacityValueSchema.safeParse('50%').success).toBe(true);
        expect(capacityValueSchema.safeParse('100').success).toBe(true);
        expect(capacityValueSchema.safeParse('0').success).toBe(true);

        expect(capacityValueSchema.safeParse('101').success).toBe(false);
        expect(capacityValueSchema.safeParse('-1').success).toBe(false);
        expect(capacityValueSchema.safeParse('150%').success).toBe(false);
      });

      it('validates weight values', () => {
        expect(capacityValueSchema.safeParse('2w').success).toBe(true);
        expect(capacityValueSchema.safeParse('10w').success).toBe(true);
        expect(capacityValueSchema.safeParse('0.5w').success).toBe(true);

        expect(capacityValueSchema.safeParse('0w').success).toBe(false);
        expect(capacityValueSchema.safeParse('-1w').success).toBe(false);
        expect(capacityValueSchema.safeParse('w').success).toBe(false);
      });

      it('validates absolute resource values', () => {
        expect(capacityValueSchema.safeParse('[memory=1024,vcores=2]').success).toBe(true);
        expect(capacityValueSchema.safeParse('[memory=2048]').success).toBe(true);
        expect(capacityValueSchema.safeParse('[vcores=4]').success).toBe(true);

        expect(capacityValueSchema.safeParse('[]').success).toBe(false);
        expect(capacityValueSchema.safeParse('[memory=]').success).toBe(false);
        expect(capacityValueSchema.safeParse('[=1024]').success).toBe(false);
        expect(capacityValueSchema.safeParse('[memory=abc]').success).toBe(false);
      });

      it('allows empty values', () => {
        expect(capacityValueSchema.safeParse('').success).toBe(true);
        expect(capacityValueSchema.safeParse('   ').success).toBe(true);
      });
    });

    describe('percentageSchema', () => {
      it('validates percentage values', () => {
        expect(percentageSchema.safeParse('0').success).toBe(true);
        expect(percentageSchema.safeParse('50').success).toBe(true);
        expect(percentageSchema.safeParse('100').success).toBe(true);
        expect(percentageSchema.safeParse('25.5').success).toBe(true);

        expect(percentageSchema.safeParse('101').success).toBe(false);
        expect(percentageSchema.safeParse('-1').success).toBe(false);
        expect(percentageSchema.safeParse('abc').success).toBe(false);
      });

      it('allows empty values', () => {
        expect(percentageSchema.safeParse('').success).toBe(true);
      });
    });

    describe('positiveNumberSchema', () => {
      it('validates positive numbers', () => {
        expect(positiveNumberSchema.safeParse('1').success).toBe(true);
        expect(positiveNumberSchema.safeParse('0.1').success).toBe(true);
        expect(positiveNumberSchema.safeParse('100').success).toBe(true);

        expect(positiveNumberSchema.safeParse('0').success).toBe(false);
        expect(positiveNumberSchema.safeParse('-1').success).toBe(false);
        expect(positiveNumberSchema.safeParse('abc').success).toBe(false);
      });

      it('allows empty values', () => {
        expect(positiveNumberSchema.safeParse('').success).toBe(true);
      });
    });

    describe('integerSchema', () => {
      it('validates positive integers', () => {
        expect(integerSchema.safeParse('1').success).toBe(true);
        expect(integerSchema.safeParse('100').success).toBe(true);
        expect(integerSchema.safeParse('1000').success).toBe(true);

        expect(integerSchema.safeParse('0').success).toBe(false);
        expect(integerSchema.safeParse('-1').success).toBe(false);
        expect(integerSchema.safeParse('1.5').success).toBe(false);
        expect(integerSchema.safeParse('abc').success).toBe(false);
      });

      it('allows empty values', () => {
        expect(integerSchema.safeParse('').success).toBe(true);
      });
    });

    describe('aclFormatSchema', () => {
      it('validates ACL format', () => {
        expect(aclFormatSchema.safeParse('*').success).toBe(true);
        expect(aclFormatSchema.safeParse(' ').success).toBe(true);
        expect(aclFormatSchema.safeParse('user1,user2 group1,group2').success).toBe(true);
        expect(aclFormatSchema.safeParse('user1 group1').success).toBe(true);
        expect(aclFormatSchema.safeParse('user1').success).toBe(true);
        expect(aclFormatSchema.safeParse('user1,user2').success).toBe(true);

        expect(aclFormatSchema.safeParse('user1 group1 extra').success).toBe(false);
        expect(aclFormatSchema.safeParse('user@domain').success).toBe(false);
        expect(aclFormatSchema.safeParse('user with spaces').success).toBe(false);
      });

      it('allows empty values', () => {
        expect(aclFormatSchema.safeParse('').success).toBe(true);
      });
    });
  });

  describe('property completeness', () => {
    it('covers major YARN configuration categories', () => {
      const categories = new Set(queuePropertyDefinitions.map((p) => p.category));

      expect(categories.has('capacity')).toBe(true);
      expect(categories.has('resource')).toBe(true);
      expect(categories.has('application-limits')).toBe(true);
      expect(categories.has('dynamic-queues')).toBe(true);
      expect(categories.has('node-labels')).toBe(true);
      expect(categories.has('scheduling')).toBe(true);
      expect(categories.has('security')).toBe(true);
      expect(categories.has('preemption')).toBe(true);
    });

    it('has properties for auto-queue creation', () => {
      const autoQueueProperties = queuePropertyDefinitions.filter(
        (p) => p.name.includes('auto-') || p.name.includes('template'),
      );
      expect(autoQueueProperties.length).toBeGreaterThan(0);
    });

    it('has properties for preemption control', () => {
      const preemptionProperties = queuePropertyDefinitions.filter((p) =>
        p.name.includes('preemption'),
      );
      expect(preemptionProperties.length).toBeGreaterThan(0);
    });

    it('has properties for application lifetime management', () => {
      const lifetimeProperties = queuePropertyDefinitions.filter((p) =>
        p.name.includes('lifetime'),
      );
      expect(lifetimeProperties.length).toBeGreaterThan(0);
    });
  });
});
