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


import { buildPropertyKey, getParentQueuePath } from '~/utils/propertyUtils';
import { parseCapacityValue, getCapacityType } from '~/utils/capacityUtils';
import { SPECIAL_VALUES } from '~/types/constants/special-values';
import { AUTO_CREATION_PROPS } from '~/types/constants/auto-creation';
import type { StagedChange, ValidationIssue, SchedulerInfo } from '~/types';
import { findQueueByPath, getSiblingQueues } from '~/utils/treeUtils';
import { isTemplateQueuePath } from '~/utils/templateUtils';

export interface ValidationContext {
  queuePath: string;
  fieldName: string;
  fieldValue: unknown;
  config: Map<string, string>;
  schedulerData?: SchedulerInfo | null;
  stagedChanges: StagedChange[];
  legacyModeEnabled: boolean;
}

export interface ValidationRule {
  id: string;
  description: string;
  level: 'error' | 'warning';
  triggers: string[];
  evaluate: (context: ValidationContext) => ValidationIssue[];
}

export const QUEUE_VALIDATION_RULES: ValidationRule[] = [
  {
    id: 'CAPACITY_SUM',
    description: 'Ensures child capacities sum correctly under a parent queue.',
    level: 'error',
    triggers: ['capacity'],
    evaluate: (context) => evaluateChildCapacitySum(context),
  },
  {
    id: 'MAX_CAPACITY_CONSTRAINT',
    description: 'Ensures maximum capacity is not less than capacity and uses consistent units.',
    level: 'error',
    triggers: ['capacity', 'maximum-capacity'],
    evaluate: (context) => evaluateMaxCapacityRelationship(context),
  },
  {
    id: 'CONSISTENT_CAPACITY_MODE',
    description: 'Ensures all sibling queues use the same capacity mode in legacy mode.',
    level: 'error',
    triggers: ['capacity'],
    evaluate: (context) => evaluateCapacityTypeConsistency(context),
  },
  {
    id: 'PARENT_CHILD_CAPACITY_CONSTRAINT',
    description: 'Warns when a child absolute resource exceeds its parent allocation.',
    level: 'warning',
    triggers: ['capacity'],
    evaluate: (context) => evaluateParentChildCapacityConstraints(context),
  },
  {
    id: 'PARENT_CHILD_CAPACITY_MODE',
    description:
      'Ensures that if parent uses absolute resources, children must also use absolute resources (legacy mode only).',
    level: 'error',
    triggers: ['capacity'],
    evaluate: (context) => evaluateParentChildCapacityMode(context),
  },
  {
    id: 'WEIGHT_MODE_TRANSITION_FLEXIBLE_AQC',
    description:
      'Ensures flexible auto-queue creation is disabled when transitioning from weight mode (legacy mode only).',
    level: 'error',
    triggers: ['capacity'],
    evaluate: (context) => evaluateWeightModeTransitionFlexibleAQC(context),
  },
];

export function runFieldValidation(context: ValidationContext): ValidationIssue[] {
  const applicableRules = QUEUE_VALIDATION_RULES.filter((rule) =>
    rule.triggers.includes(context.fieldName),
  );
  return applicableRules.flatMap((rule) => rule.evaluate(context));
}

// --- Rule evaluators -------------------------------------------------------

function evaluateCapacityTypeConsistency(context: ValidationContext): ValidationIssue[] {
  if (isTemplateQueuePath(context.queuePath)) {
    return [];
  }
  if (!context.legacyModeEnabled) {
    return [];
  }

  const siblings = getSiblingQueues(context.schedulerData, context.queuePath);
  if (!siblings.length) {
    return [];
  }

  const currentValue = typeof context.fieldValue === 'string' ? context.fieldValue : undefined;
  const currentType = getCapacityType(currentValue);
  if (!currentType) {
    return [];
  }

  const inconsistentSiblings = siblings
    .filter((sibling) => sibling.queuePath !== context.queuePath)
    .map((sibling) => {
      const value = context.config.get(buildPropertyKey(sibling.queuePath, 'capacity'));
      const type = getCapacityType(value);
      return type && type !== currentType ? sibling.queueName : null;
    })
    .filter((name): name is string => Boolean(name));

  if (inconsistentSiblings.length === 0) {
    return [];
  }

  return [
    {
      queuePath: context.queuePath,
      field: 'capacity',
      message: `All sibling queues must use the same capacity type (legacy mode requirement). Inconsistent siblings: ${inconsistentSiblings.join(', ')}`,
      severity: 'error',
      rule: 'capacity-type-consistency',
    },
  ];
}

function evaluateChildCapacitySum(context: ValidationContext): ValidationIssue[] {
  if (isTemplateQueuePath(context.queuePath)) {
    return [];
  }
  if (!context.legacyModeEnabled) {
    return [];
  }

  const queue = findQueueByPath(context.schedulerData, context.queuePath);

  // Track if parent originally had no children
  const originallyHadNoChildren = !queue?.queues?.queue?.length;

  // Initialize child queue paths (empty if no existing children)
  const childQueuePaths = new Set(queue?.queues?.queue?.map((child) => child.queuePath) ?? []);

  // Process staged changes to build the complete set of children
  context.stagedChanges.forEach((change) => {
    if (change.queuePath === SPECIAL_VALUES.GLOBAL_QUEUE_PATH) {
      return;
    }
    const parentPath = getParentQueuePath(change.queuePath);
    if (parentPath !== context.queuePath) {
      return;
    }

    if (change.type === 'remove') {
      childQueuePaths.delete(change.queuePath);
    } else if (change.type === 'add') {
      childQueuePaths.add(change.queuePath);
    }
  });

  // If no children after processing staged changes, skip validation
  if (childQueuePaths.size === 0) {
    return [];
  }

  const childCapacities: number[] = [];
  let allPercentages = true;

  childQueuePaths.forEach((childPath) => {
    const key = buildPropertyKey(childPath, 'capacity');
    const rawValue =
      childPath === context.queuePath && context.fieldName === 'capacity'
        ? (context.fieldValue as string)
        : context.config.get(key);

    if (!rawValue) {
      return;
    }

    const parsed = parseCapacityValue(rawValue);
    if (!parsed) {
      return;
    }

    if (parsed.type !== 'percentage') {
      allPercentages = false;
      return;
    }

    childCapacities.push(parsed.value);
  });

  if (!allPercentages || childCapacities.length === 0) {
    return [];
  }

  const sum = childCapacities.reduce((total, value) => total + value, 0);
  const tolerance = 0.001;

  if (Math.abs(sum - 100) <= tolerance) {
    return [];
  }

  // Use warning for parents that originally had no children (user is building structure)
  // Use error for existing queue structures being modified
  const severity = originallyHadNoChildren ? 'warning' : 'error';
  const verb = originallyHadNoChildren ? 'should' : 'must';

  return [
    {
      queuePath: context.queuePath,
      field: 'capacity',
      message: `Child queue capacities ${verb} sum to 100% (legacy mode requirement, current: ${sum.toFixed(1)}%)`,
      severity,
      rule: 'child-capacity-sum',
    },
  ];
}

function evaluateMaxCapacityRelationship(context: ValidationContext): ValidationIssue[] {
  if (isTemplateQueuePath(context.queuePath)) {
    return [];
  }
  if (!context.legacyModeEnabled) {
    return [];
  }

  const queuePath = context.queuePath;
  const capacityKey = buildPropertyKey(queuePath, 'capacity');
  const maxCapacityKey = buildPropertyKey(queuePath, 'maximum-capacity');

  const capacityValue =
    context.fieldName === 'capacity'
      ? (context.fieldValue as string)
      : context.config.get(capacityKey) || '';
  const maxCapacityValue =
    context.fieldName === 'maximum-capacity'
      ? (context.fieldValue as string)
      : context.config.get(maxCapacityKey) || '';

  if (!maxCapacityValue || maxCapacityValue.trim() === '' || maxCapacityValue === '-1') {
    return [];
  }

  const parsedCapacity = parseCapacityValue(capacityValue);
  const parsedMaxCapacity = parseCapacityValue(maxCapacityValue);

  if (!parsedCapacity || !parsedMaxCapacity) {
    return [];
  }

  if (parsedCapacity.type === 'absolute') {
    if (parsedMaxCapacity.type !== 'absolute') {
      return [
        {
          queuePath,
          field: 'maximum-capacity',
          message:
            'Maximum capacity must use an absolute resource vector when capacity is absolute',
          severity: 'error',
          rule: 'max-capacity-format-match',
        },
      ];
    }

    const capacityResources = parsedCapacity.resources ?? {};
    const maxCapacityResources = parsedMaxCapacity.resources ?? {};
    const resourceIssues: ValidationIssue[] = [];

    Object.entries(capacityResources).forEach(([resource, value]) => {
      const maxValue = maxCapacityResources[resource];
      if (maxValue === undefined || maxValue < value) {
        resourceIssues.push({
          queuePath,
          field: 'maximum-capacity',
          message: `Maximum capacity ${resource} allocation (${maxValue ?? 'unset'}) must be greater than or equal to capacity allocation (${value})`,
          severity: 'error',
          rule: 'max-capacity-minimum',
        });
      }
    });

    return resourceIssues;
  }

  if (parsedMaxCapacity.type !== 'percentage') {
    return [
      {
        queuePath,
        field: 'maximum-capacity',
        message:
          'Maximum capacity must be expressed as a percentage when capacity uses percentage or weight',
        severity: 'error',
        rule: 'max-capacity-format-match',
      },
    ];
  }

  if (parsedCapacity.type === 'percentage' && parsedMaxCapacity.value < parsedCapacity.value) {
    return [
      {
        queuePath,
        field: 'maximum-capacity',
        message: 'Maximum capacity must be greater than or equal to capacity',
        severity: 'error',
        rule: 'max-capacity-minimum',
      },
    ];
  }

  return [];
}

function evaluateParentChildCapacityConstraints(context: ValidationContext): ValidationIssue[] {
  if (!context.legacyModeEnabled) {
    return [];
  }

  const parentPath = getParentQueuePath(context.queuePath);
  if (!parentPath) {
    return [];
  }

  const childValue =
    context.fieldName === 'capacity'
      ? (context.fieldValue as string)
      : context.config.get(buildPropertyKey(context.queuePath, 'capacity')) || '';

  if (!childValue) {
    return [];
  }

  const childParsed = parseCapacityValue(childValue);
  if (!childParsed) {
    return [];
  }

  const parentValue = context.config.get(buildPropertyKey(parentPath, 'capacity'));
  if (!parentValue) {
    return [];
  }

  const parentParsed = parseCapacityValue(parentValue);
  if (!parentParsed) {
    return [];
  }

  if (childParsed.type !== 'absolute' || parentParsed.type !== 'absolute') {
    return [];
  }

  const childResources = childParsed.resources;
  const parentResources = parentParsed.resources;

  if (!childResources || !parentResources) {
    return [];
  }

  const issues: ValidationIssue[] = [];

  Object.entries(childResources).forEach(([resource, childAmount]) => {
    const parentAmount = parentResources[resource];
    if (parentAmount === undefined) {
      return;
    }
    if (childAmount > parentAmount) {
      issues.push({
        queuePath: context.queuePath,
        field: 'capacity',
        message: `Child queue ${resource} allocation (${childAmount}) cannot exceed parent queue ${resource} allocation (${parentAmount})`,
        severity: 'warning',
        rule: 'parent-child-capacity-constraint',
      });
    }
  });

  return issues;
}

function evaluateParentChildCapacityMode(context: ValidationContext): ValidationIssue[] {
  if (isTemplateQueuePath(context.queuePath)) {
    return [];
  }
  if (!context.legacyModeEnabled) {
    return [];
  }

  // Root queue is always in percentage mode, skip it
  if (context.queuePath === 'root') {
    return [];
  }

  const parentPath = getParentQueuePath(context.queuePath);
  if (!parentPath) {
    return [];
  }

  // Skip validation when parent is root - root's children can use any capacity mode
  if (parentPath === 'root') {
    return [];
  }

  // Get the child's capacity value (current field being edited)
  const childValue =
    context.fieldName === 'capacity'
      ? (context.fieldValue as string)
      : context.config.get(buildPropertyKey(context.queuePath, 'capacity')) || '';

  if (!childValue) {
    return [];
  }

  const childType = getCapacityType(childValue);
  if (!childType) {
    return [];
  }

  // Get the parent's capacity value
  const parentValue = context.config.get(buildPropertyKey(parentPath, 'capacity'));
  if (!parentValue) {
    return [];
  }

  const parentType = getCapacityType(parentValue);
  if (!parentType) {
    return [];
  }

  // Parent and child must use the same capacity mode (both absolute or both percentage/weight)
  // Check both directions to catch all mismatches

  if (parentType === 'absolute' && childType !== 'absolute') {
    return [
      {
        queuePath: context.queuePath,
        field: 'capacity',
        message: `Parent queue uses absolute resources, child queue must also use absolute resources (legacy mode requirement)`,
        severity: 'error',
        rule: 'parent-child-capacity-mode',
      },
    ];
  }

  if (parentType !== 'absolute' && childType === 'absolute') {
    return [
      {
        queuePath: context.queuePath,
        field: 'capacity',
        message: `Parent queue uses ${parentType} mode, child queue cannot use absolute resources (legacy mode requirement)`,
        severity: 'error',
        rule: 'parent-child-capacity-mode',
      },
    ];
  }

  return [];
}

function evaluateWeightModeTransitionFlexibleAQC(context: ValidationContext): ValidationIssue[] {
  if (isTemplateQueuePath(context.queuePath)) {
    return [];
  }
  if (!context.legacyModeEnabled) {
    return [];
  }

  // Only check when the capacity field is being changed
  if (context.fieldName !== 'capacity') {
    return [];
  }

  // Get the old capacity value from config
  const oldValue = context.config.get(buildPropertyKey(context.queuePath, 'capacity'));
  const oldType = getCapacityType(oldValue);

  // Get the new capacity value from the field being edited
  const newValue = context.fieldValue as string;
  const newType = getCapacityType(newValue);

  // Check if we're transitioning from weight mode to percentage or absolute mode
  if (oldType === 'weight' && (newType === 'percentage' || newType === 'absolute')) {
    // Check if flexible auto-queue creation is enabled for this queue
    const flexibleAQCKey = buildPropertyKey(
      context.queuePath,
      AUTO_CREATION_PROPS.FLEXIBLE_ENABLED,
    );
    const flexibleAQCValue = context.config.get(flexibleAQCKey);

    if (flexibleAQCValue === 'true') {
      return [
        {
          queuePath: context.queuePath,
          field: 'capacity',
          message: `Cannot change from weight mode to ${newType} mode while flexible auto-queue creation is enabled. Please disable "${AUTO_CREATION_PROPS.FLEXIBLE_ENABLED}" first (legacy mode requirement)`,
          severity: 'error',
          rule: 'weight-mode-transition-flexible-aqc',
        },
      ];
    }
  }

  return [];
}
