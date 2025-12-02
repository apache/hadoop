<!---
  Licensed under the Apache License, Version 2.0 (the "License");
  you may not use this file except in compliance with the License.
  You may obtain a copy of the License at

   http://www.apache.org/licenses/LICENSE-2.0

  Unless required by applicable law or agreed to in writing, software
  distributed under the License is distributed on an "AS IS" BASIS,
  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
  See the License for the specific language governing permissions and
  limitations under the License. See accompanying LICENSE file.
-->

# Validation Feature

This directory contains the validation engine for the YARN Capacity Scheduler UI. The validation system ensures that configuration changes meet YARN scheduler requirements before they are applied to the cluster.

## Architecture Overview

The validation system is a **multi-layered architecture** that progressively validates configurations:

1. **Schema-level validation** (property descriptors) - Basic format checks and ranges
2. **Business validation rules** (validation-rules.ts) - Complex cross-field and cross-queue logic
3. **Cross-queue validation** (this feature) - Dependency-aware validation across the queue hierarchy

## Key Components

### service.ts

The **validation service** provides core validation functions:

- `validateField()` - Validates a single field change with full context
- `validateQueue()` - Validates all properties of a queue
- `hasBlockingIssues()` - Checks if any validation issues are blocking errors
- `splitIssues()` - Separates issues into errors and warnings

The service coordinates between individual validation rules and the cross-queue logic in `crossQueue.ts`.

### crossQueue.ts

Contains the **cross-queue validation engine** that:

- Detects affected queues when a change occurs
- Runs validation rules across multiple queues
- Handles parent-child and sibling queue relationships
- Manages dependency-aware validation

Key functions:

- `validatePropertyChange()` - Validates a single property change with cross-queue awareness
- `validateStagedChanges()` - Validates all staged changes, optionally filtering by affected queues/properties

### ruleCategories.ts

Defines **validation rule categories** that control validation behavior:

- `CROSS_QUEUE_RULES` - Rules that affect multiple queues (parent/children/siblings)
- `QUEUE_SPECIFIC_RULES` - Rules that only validate a single queue
- `WARNING_ONLY_RULES` - Rules that produce warnings but never block changes

These categories are used by the validation service to determine which queues need re-validation and whether errors should block applying changes.

### utils/affectedQueues.ts

Implements the **affected queue detection** logic:

- `getAffectedQueuesForValidation()` - Determines which queues are affected by a property change
- Handles cascading effects (e.g., parent capacity changes affect all descendants)
- Manages sibling relationships (e.g., capacity changes require sibling re-validation)

### utils/dedupeIssues.ts

Provides **validation issue deduplication**:

- `dedupeIssues()` - Removes duplicate validation issues based on queuePath, field, rule, message, and severity
- Used by both `service.ts` and `crossQueue.ts` to ensure unique issues are reported

## Validation Flow

### Single Field Validation

```
User edits property
    ↓
validateField(queuePath, fieldName, value)
    ↓
Apply schema-level validation (property descriptor rules)
    ↓
Apply business validation rules (validation-rules.ts)
    ↓
Detect affected queues
    ↓
Run cross-queue validation
    ↓
Return validation issues
```

### Staged Changes Validation

```
User stages changes
    ↓
validateStagedChanges({ stagedChanges, schedulerData, configData })
    ↓
For each staged change:
  - Merge staged changes with current config
  - Validate affected queues with cross-queue awareness
    ↓
Return Map of change ID → validation issues
    ↓
Block "Apply" if blocking errors exist
```

## Validation Context

The `ValidationContext` object provides all information needed for validation:

```typescript
interface ValidationContext {
  queuePath: string; // Queue being validated
  fieldName: string; // Property being validated
  fieldValue: unknown; // New value for the property
  config: Map<string, string>; // Full config including staged changes
  schedulerData: SchedulerInfo; // Current scheduler state
  stagedChanges: StagedChange[]; // All pending changes
  legacyModeEnabled: boolean; // Whether legacy mode is active
}
```

This context is passed to all validation rules, providing full visibility into the scheduler state.

## Validation Rules

Validation rules are defined in `src/config/validation-rules.ts`. Each rule:

- Has a unique identifier
- Specifies which properties trigger it
- Implements validation logic via an evaluator function
- Returns validation issues when constraints are violated

See `docs/development/adding-validation-rules.md` for detailed guidance on adding new rules.

## Rule Categories

### Cross-Queue Rules

Rules that validate relationships between multiple queues:

- Sibling capacity summation
- Parent-child capacity constraints
- Resource mode inheritance
- Auto-creation compatibility

These rules trigger re-validation of affected queues when changes occur.

### Queue-Specific Rules

Rules that only validate the queue being edited:

- Max-capacity >= capacity
- Application lifetime constraints
- ACL format validation
- State transition rules

These rules don't trigger validation of other queues.

### Warning-Only Rules

Rules that provide helpful feedback but never block changes:

- Preemption recommendations
- Performance warnings
- Best practice suggestions

These rules show as warnings in the UI but don't prevent applying changes.

## Performance Considerations

The validation system is designed for efficiency:

- **Lazy evaluation** - Only affected queues are re-validated
- **Early returns** - Rules skip validation when not applicable
- **Cached computations** - Affected queues are computed once per change
- **Incremental validation** - Only changed properties trigger validation

## Testing

Tests for the validation system are in `__tests__/`:

- `crossQueue.test.ts` - Tests for cross-queue validation logic
- `service.test.ts` - Tests for validation service orchestration
- `utils/affectedQueues.test.ts` - Tests for affected queue detection

Additional rule-specific tests are in `src/config/__tests__/validation-rules.test.ts`.

## Integration Points

The validation system integrates with:

- **Property Editor** (`src/features/property-editor/`) - Real-time validation as users edit
- **Staged Changes** (`src/features/staged-changes/`) - Validation before applying changes
- **Store** (`src/stores/`) - Validation state management
- **UI Components** - Display of validation errors and warnings

## Further Reading

- `docs/development/adding-validation-rules.md` - Guide for adding new validation rules
- `docs/development/extending-scheduler-properties.md` - Adding new properties with schema validation
- `src/config/validation-rules.ts` - Validation rule definitions
