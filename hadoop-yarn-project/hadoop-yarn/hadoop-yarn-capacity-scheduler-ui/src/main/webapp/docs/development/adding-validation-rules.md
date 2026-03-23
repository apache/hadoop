<!--
  Licensed to the Apache Software Foundation (ASF) under one
  or more contributor license agreements.  See the NOTICE file
  distributed with this work for additional information
  regarding copyright ownership.  The ASF licenses this file
  to you under the Apache License, Version 2.0 (the
  "License"); you may not use this file except in compliance
  with the License.  You may obtain a copy of the License at

      http://www.apache.org/licenses/LICENSE-2.0

  Unless required by applicable law or agreed to in writing, software
  distributed under the License is distributed on an "AS IS" BASIS,
  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
  See the License for the specific language governing permissions and
  limitations under the License.
-->


# Adding Validation Rules

This guide explains how to add new validation rules to the Capacity Scheduler UI. Validation rules enforce business logic constraints across queue configurations, ensuring that changes are valid before being applied to the YARN cluster. The general idea is to have little frontend validation to rule out the straightforward issues and let YARN validate the more complicated configs.

## Overview

The validation system has two layers:

1. **Schema-level validation** (`validationRules` in property descriptors) - Basic format checks, ranges, and patterns for individual fields
2. **Business validation rules** (`src/config/validation-rules.ts`) - Complex cross-field and cross-queue logic

This guide focuses on the second layer: business validation rules that enforce YARN scheduler constraints.

## Key Files

- `src/config/validation-rules.ts` - Rule definitions and evaluator functions
- `src/features/validation/service.ts` - Orchestration layer that invokes rules
- `src/features/validation/crossQueue.ts` - Cross-queue validation logic
- `src/features/validation/ruleCategories.ts` - Rule categorization for UI behavior
- `src/features/validation/utils/` - Helper utilities for validation logic
- `src/config/__tests__/validation-rules.test.ts` - Rule tests

## When to Add a New Rule

Add a business validation rule when:

- The constraint involves multiple properties (e.g., max-capacity >= capacity)
- The constraint involves multiple queues (e.g., sibling capacities must sum to 100%)
- The constraint depends on scheduler state (e.g., legacy mode requirements)
- The constraint enforces YARN scheduler semantics (e.g., absolute capacity mode inheritance)
- The validation logic is too complex for schema-level rules

Do **not** add a business rule when:

- A simple schema validation rule suffices (use `validationRules` in property descriptor instead)
- The validation is purely formatting/parsing (use Zod schemas in `src/config/schemas/validation.ts`)

## Step-by-Step Guide

### 1. Define the Rule in `validation-rules.ts`

Add a new entry to the `QUEUE_VALIDATION_RULES` array:

```typescript
{
  id: 'MY_NEW_RULE',
  description: 'Brief explanation of what this rule enforces',
  level: 'error', // or 'warning'
  triggers: ['property-name'], // Fields that trigger this rule
  evaluate: (context) => evaluateMyNewRule(context),
}
```

**Field definitions:**

- `id` - Uppercase identifier for the rule (used in logs and debugging)
- `description` - Human-readable explanation of the constraint
- `level` - Default severity (`'error'` blocks changes, `'warning'` allows with notification)
- `triggers` - Array of property names that should cause this rule to evaluate
- `evaluate` - Function that implements the validation logic

### 2. Implement the Evaluator Function

Create an evaluator function below the `QUEUE_VALIDATION_RULES` array:

```typescript
function evaluateMyNewRule(context: ValidationContext): ValidationIssue[] {
  // Skip validation for template queues if not applicable
  if (isTemplateQueuePath(context.queuePath)) {
    return [];
  }

  // Skip if rule only applies in legacy mode
  if (!context.legacyModeEnabled) {
    return [];
  }

  // Get relevant values from context
  const value = context.fieldValue as string;
  const relatedKey = buildPropertyKey(context.queuePath, 'related-field');
  const relatedValue = context.config.get(relatedKey);

  // Perform validation logic
  if (/* invalid condition */) {
    return [
      {
        queuePath: context.queuePath,
        field: context.fieldName,
        message: 'User-friendly error message explaining the problem',
        severity: 'error',
        rule: 'my-new-rule', // Lowercase kebab-case ID
      },
    ];
  }

  return []; // No issues
}
```

**ValidationContext fields:**

- `queuePath` - The queue being validated (e.g., `'root.production'`)
- `fieldName` - The property being changed (e.g., `'capacity'`)
- `fieldValue` - The new value for the property
- `config` - Full configuration map including staged changes
- `schedulerData` - Current scheduler state (queue tree, etc.)
- `stagedChanges` - All pending changes
- `legacyModeEnabled` - Whether legacy mode is active

**ValidationIssue fields:**

- `queuePath` - Which queue has the issue
- `field` - Which property has the issue
- `message` - User-facing error message
- `severity` - `'error'` or `'warning'`
- `rule` - Lowercase kebab-case rule identifier

### 3. Add Rule to Categories

Update `src/features/validation/ruleCategories.ts` to categorize your rule:

```typescript
// If the rule affects multiple queues (parent/children/siblings)
export const CROSS_QUEUE_RULES = [
  // ... existing rules
  'my-new-rule',
] as const;

// If the rule only validates a single queue
export const QUEUE_SPECIFIC_RULES = [
  // ... existing rules
  'my-new-rule',
] as const;

// If the rule should only produce warnings (never block)
export const WARNING_ONLY_RULES = [
  // ... existing rules
  'my-new-rule',
] as const;
```

**Rule categories:**

- `CROSS_QUEUE_RULES` - Affects multiple queues; shown for relevant queues
- `QUEUE_SPECIFIC_RULES` - Only validates the queue being edited
- `WARNING_ONLY_RULES` - Never blocks applying changes (informational only)

### 4. Write Tests

Add test cases in `src/config/__tests__/validation-rules.test.ts`:

```typescript
describe('MY_NEW_RULE', () => {
  it('should pass when condition is valid', () => {
    const config = new Map([
      ['yarn.scheduler.capacity.root.test.related-field', 'compatible-value'],
    ]);

    const context: ValidationContext = {
      queuePath: 'root.test',
      fieldName: 'my-property',
      fieldValue: 'valid-value',
      config,
      schedulerData: mockSchedulerData,
      stagedChanges: [],
      legacyModeEnabled: true,
    };

    const issues = runFieldValidation(context);

    expect(issues).toHaveLength(0);
  });

  it('should return error when condition is violated', () => {
    const config = new Map([
      ['yarn.scheduler.capacity.root.test.related-field', 'incompatible-value'],
    ]);

    const context: ValidationContext = {
      queuePath: 'root.test',
      fieldName: 'my-property',
      fieldValue: 'invalid-value',
      config,
      schedulerData: mockSchedulerData,
      stagedChanges: [],
      legacyModeEnabled: true,
    };

    const issues = runFieldValidation(context);

    expect(issues).toHaveLength(1);
    expect(issues[0]).toMatchObject({
      queuePath: 'root.test',
      field: 'my-property',
      severity: 'error',
      rule: 'my-new-rule',
    });
    expect(issues[0].message).toContain('expected error message snippet');
  });

  it('should skip validation when not in legacy mode', () => {
    const context: ValidationContext = {
      queuePath: 'root.test',
      fieldName: 'my-property',
      fieldValue: 'invalid-value',
      config: new Map(),
      schedulerData: mockSchedulerData,
      stagedChanges: [],
      legacyModeEnabled: false,
    };

    const issues = runFieldValidation(context);

    expect(issues).toHaveLength(0);
  });
});
```

### 5. Update Affected Queues Logic (if needed)

If your rule affects multiple queues, update `src/features/validation/utils/affectedQueues.ts`:

```typescript
export function getAffectedQueuesForValidation(
  propertyName: string,
  queuePath: string,
  schedulerData: SchedulerInfo,
  stagedChanges: StagedChange[],
): string[] {
  const affected = new Set<string>([queuePath]);

  // Add logic to determine which other queues are affected
  if (propertyName === 'my-property') {
    // Example: Validate parent queue when child property changes
    const parentPath = getParentPath(queuePath);
    if (parentPath) {
      affected.add(parentPath);
    }
  }

  return Array.from(affected);
}
```

## Common Patterns

### Validating Sibling Queues

```typescript
function evaluateSiblingRule(context: ValidationContext): ValidationIssue[] {
  const siblings = getSiblingQueues(context.schedulerData, context.queuePath);

  const issues: ValidationIssue[] = [];

  siblings.forEach((sibling) => {
    const siblingValue = context.config.get(
      buildPropertyKey(sibling.queuePath, 'property-name')
    );

    if (/* sibling violates constraint */) {
      issues.push({
        queuePath: context.queuePath,
        field: 'property-name',
        message: `Sibling queue ${sibling.queueName} has incompatible value`,
        severity: 'error',
        rule: 'sibling-rule',
      });
    }
  });

  return issues;
}
```

### Validating Parent-Child Relationships

```typescript
function evaluateParentChildRule(context: ValidationContext): ValidationIssue[] {
  const parentPath = getParentPath(context.queuePath);
  if (!parentPath) {
    return [];
  }

  const parentValue = context.config.get(
    buildPropertyKey(parentPath, 'property-name')
  );
  const childValue = context.fieldValue as string;

  if (/* child incompatible with parent */) {
    return [
      {
        queuePath: context.queuePath,
        field: context.fieldName,
        message: 'Child must be compatible with parent configuration',
        severity: 'error',
        rule: 'parent-child-rule',
      },
    ];
  }

  return [];
}
```

### Validating Numeric Relationships

```typescript
function evaluateNumericRule(context: ValidationContext): ValidationIssue[] {
  const maxKey = buildPropertyKey(context.queuePath, 'maximum-value');
  const minValue = parseFloat(context.fieldValue as string);
  const maxValue = parseFloat(context.config.get(maxKey) || '');

  if (isNaN(minValue) || isNaN(maxValue)) {
    return [];
  }

  if (minValue > maxValue) {
    return [
      {
        queuePath: context.queuePath,
        field: context.fieldName,
        message: 'Minimum value cannot exceed maximum value',
        severity: 'error',
        rule: 'numeric-range',
      },
    ];
  }

  return [];
}
```

### Handling Staged Changes

```typescript
function evaluateWithStagedChanges(context: ValidationContext): ValidationIssue[] {
  // Check if there are pending changes that affect validation
  const hasRelatedChanges = context.stagedChanges.some(
    (change) => change.queuePath === context.queuePath && change.property === 'related-property',
  );

  // Use merged config to see the final state including staged changes
  const finalValue = context.config.get(buildPropertyKey(context.queuePath, 'related-property'));

  // Validate against the merged state
  // ...
}
```

## Best Practices

### Error Messages

- Be specific and actionable: "Child queue capacities must sum to 100% (current: 95%)"
- Explain the constraint: "Maximum capacity must be greater than or equal to capacity"
- Include context when helpful: "Parent queue uses absolute resources, child queue must also use absolute resources (legacy mode requirement)"
- Don't be vague: "Invalid configuration"
- Don't use technical jargon: "Constraint violation in capacity vector normalization"

### Rule IDs

- Use kebab-case for rule IDs: `parent-child-capacity-mode`
- Make them descriptive: `weight-mode-transition-flexible-aqc` (not just `weight-rule`)
- Keep them consistent with similar rules: `max-capacity-minimum`, `max-capacity-format-match`

### Performance

- Return early when validation doesn't apply (template queues, wrong mode, etc.)
- Cache expensive computations when possible
- Avoid redundant config lookups - store in variables

### Inherited Values

Some queue properties inherit their effective value from a parent queue or a global default when not set explicitly (see `inheritanceResolver` in `docs/development/extending-scheduler-properties.md`). The `context.config` map already contains staged changes, but it does **not** resolve inheritance automatically. If your rule needs the effective value of a property that may be inherited, look up the config map for the queue first, and if absent, consider falling back to the parent or global key as YARN would. The helper functions `getQueueValue` and `getGlobalValue` from `src/utils/resolveInheritedValue.ts` can simplify these lookups.

### Legacy Mode Handling

Many rules only apply in legacy mode. Always check:

```typescript
if (!context.legacyModeEnabled) {
  return [];
}
```

### Template Queue Handling

Template queue properties shouldn't trigger most validations:

```typescript
if (isTemplateQueuePath(context.queuePath)) {
  return [];
}
```

## Testing Checklist

- [ ] Rule passes when constraint is satisfied
- [ ] Rule returns correct error/warning when violated
- [ ] Rule correctly skips when not applicable (mode, template queues, etc.)
- [ ] Rule handles missing/undefined values gracefully
- [ ] Rule handles edge cases (empty values, special characters, etc.)
- [ ] Error messages are clear and helpful
- [ ] Rule is categorized correctly in `ruleCategories.ts`
- [ ] Cross-queue rules update `affectedQueues.ts` if needed
- [ ] All tests pass: `npm run test`

## Example: Complete Implementation

Here's a complete example of adding a new rule:

```typescript
// In src/config/validation-rules.ts

export const QUEUE_VALIDATION_RULES: ValidationRule[] = [
  // ... existing rules
  {
    id: 'DEFAULT_LIFETIME_CONSTRAINT',
    description: 'Ensures default application lifetime does not exceed maximum lifetime',
    level: 'error',
    triggers: ['default-application-lifetime', 'maximum-application-lifetime'],
    evaluate: (context) => evaluateDefaultLifetimeConstraint(context),
  },
];

function evaluateDefaultLifetimeConstraint(context: ValidationContext): ValidationIssue[] {
  // This rule applies to both legacy and flexible modes

  const queuePath = context.queuePath;
  const defaultKey = buildPropertyKey(queuePath, 'default-application-lifetime');
  const maxKey = buildPropertyKey(queuePath, 'maximum-application-lifetime');

  const defaultValue =
    context.fieldName === 'default-application-lifetime'
      ? (context.fieldValue as string)
      : context.config.get(defaultKey) || '';
  const maxValue =
    context.fieldName === 'maximum-application-lifetime'
      ? (context.fieldValue as string)
      : context.config.get(maxKey) || '';

  // Skip if either value is not set
  if (!defaultValue || !maxValue) {
    return [];
  }

  const defaultSeconds = parseInt(defaultValue, 10);
  const maxSeconds = parseInt(maxValue, 10);

  // Skip if values are not valid numbers
  if (isNaN(defaultSeconds) || isNaN(maxSeconds)) {
    return [];
  }

  if (defaultSeconds > maxSeconds) {
    return [
      {
        queuePath,
        field: context.fieldName,
        message: `Default application lifetime (${defaultSeconds}s) cannot exceed maximum application lifetime (${maxSeconds}s)`,
        severity: 'error',
        rule: 'default-lifetime-constraint',
      },
    ];
  }

  return [];
}
```

```typescript
// In src/features/validation/ruleCategories.ts

export const QUEUE_SPECIFIC_RULES = [
  // ... existing rules
  'default-lifetime-constraint',
] as const;
```

```typescript
// In src/config/__tests__/validation-rules.test.ts

describe('DEFAULT_LIFETIME_CONSTRAINT', () => {
  it('should pass when default lifetime is less than maximum', () => {
    const config = new Map([
      ['yarn.scheduler.capacity.root.test.maximum-application-lifetime', '7200'],
    ]);

    const context: ValidationContext = {
      queuePath: 'root.test',
      fieldName: 'default-application-lifetime',
      fieldValue: '3600',
      config,
      schedulerData: mockSchedulerData,
      stagedChanges: [],
      legacyModeEnabled: true,
    };

    const issues = runFieldValidation(context);

    expect(issues).toHaveLength(0);
  });

  it('should return error when default exceeds maximum', () => {
    const config = new Map([
      ['yarn.scheduler.capacity.root.test.maximum-application-lifetime', '3600'],
    ]);

    const context: ValidationContext = {
      queuePath: 'root.test',
      fieldName: 'default-application-lifetime',
      fieldValue: '7200',
      config,
      schedulerData: mockSchedulerData,
      stagedChanges: [],
      legacyModeEnabled: true,
    };

    const issues = runFieldValidation(context);

    expect(issues).toHaveLength(1);
    expect(issues[0]).toMatchObject({
      queuePath: 'root.test',
      field: 'default-application-lifetime',
      severity: 'error',
      rule: 'default-lifetime-constraint',
    });
    expect(issues[0].message).toContain('cannot exceed maximum');
  });

  it('should skip validation when maximum is not set', () => {
    const context: ValidationContext = {
      queuePath: 'root.test',
      fieldName: 'default-application-lifetime',
      fieldValue: '7200',
      config: new Map(),
      schedulerData: mockSchedulerData,
      stagedChanges: [],
      legacyModeEnabled: true,
    };

    const issues = runFieldValidation(context);

    expect(issues).toHaveLength(0);
  });
});
```

## Troubleshooting

### Rule not triggering

- Check that the property name is in the `triggers` array
- Verify the rule is included in `QUEUE_VALIDATION_RULES`
- Ensure early returns aren't skipping the validation logic

### Rule showing for wrong queues

- Update `affectedQueues.ts` to correctly identify affected queues
- Check the `queuePath` in returned `ValidationIssue` objects
- Review filtering logic in `crossQueue.ts`

### Rule blocking when it shouldn't

- Check `ruleCategories.ts` - add to `WARNING_ONLY_RULES` if needed
- Verify `severity` is set correctly in returned issues
- Review `isBlockingError()` logic

### Tests failing

- Ensure test context includes all required config values
- Check that `legacyModeEnabled` is set correctly
- Verify scheduler data structure matches expectations
- Construct `ValidationContext` objects with all required fields (see examples above)

## Further Reading

- `docs/development/extending-scheduler-properties.md` - Adding new properties with schema validation
- `src/features/validation/` - Validation feature implementation (crossQueue.ts, service.ts, ruleCategories.ts)
- `src/features/validation/README.md` - Validation feature architecture overview
- YARN Capacity Scheduler documentation - Official YARN scheduler constraints
