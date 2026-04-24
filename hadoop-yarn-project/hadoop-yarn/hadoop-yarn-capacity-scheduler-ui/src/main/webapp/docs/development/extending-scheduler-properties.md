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


# Extending Scheduler Properties

This guide explains how to make new Capacity Scheduler properties editable in the UI and how to plug them into the validation system. Follow the relevant section depending on whether you are working with global scheduler settings or queue-level configuration.

## Key modules

- `src/config/properties/global-properties.ts`: property descriptors that drive the Global Settings page.
- `src/config/properties/queue-properties.ts`: queue-level descriptors used by the property editor, queue dialogs, and staged changes.
- `src/config/schemas/validation.ts`: shared Zod helpers for common formats (capacity values, ACLs, percentages, etc.).
- `src/config/validation-rules.ts`: declarative business-validation rules evaluated for both global and queue properties.
- `src/contexts/ValidationContext.tsx`: React provider that keeps validation issues in sync with staged edits.
- `src/features/validation/service.ts`: utility entry points (`validateField`, `validateQueue`) used by hooks and slices.
- `src/features/validation/ruleCategories.ts`: maps rule IDs to blocking/non-blocking behavior.
- `src/config/__tests__/propertyDefinitions.test.ts`: regression tests that assert descriptor consistency.

## Adding a global property

1. **Define the descriptor** in `src/config/properties/global-properties.ts`.
   - Use the fully qualified key (for example, `yarn.scheduler.capacity.maximum-applications`).
   - Populate `displayName`, `description`, `type`, `category`, `defaultValue`, and `required`.
   - Use `validationRules` for field-level checks (`range`, `pattern`, or `custom`). Import helpers from `src/config/schemas/validation.ts` when possible so rules stay consistent.
   - Supply `enumValues` when `type` is `enum`. Each option should provide `{ value, label, description? }`. Use `enumDisplay` to control the visual presentation: `choiceCard` for prominent cards or `toggle` for compact buttons (see below for guidance).

- Use `displayFormat` to add user-friendly suffixes to numeric inputs.
- Add conditional logic with `showWhen` / `enableWhen` when the property should only appear or be interactive under specific scheduler states. Each condition receives the merged configuration context (global + queue values, staged changes, scheduler metadata).

**Available property categories** (defined in `src/types/property-descriptor.ts`):

- `'resource'` - Resource allocation settings
- `'scheduling'` - Scheduling policies and behavior
- `'security'` - ACLs and permissions
- `'core'` - Core scheduler settings
- `'application-limits'` - Application count and resource limits
- `'placement'` - Placement rules and policies
- `'container-allocation'` - Container sizing and allocation
- `'async-scheduling'` - Asynchronous scheduling configuration
- `'capacity'` - Capacity values and modes
- `'dynamic-queues'` - Auto-created queue settings
- `'node-labels'` - Node label and partition configuration
- `'preemption'` - Preemption policies

```ts
{
  name: 'yarn.scheduler.capacity.sample-property',
  displayName: 'Sample Property',
  description: 'What this property controls.',
  type: 'number',
  category: 'core',
  defaultValue: '0',
  required: false,
  validationRules: [
    { type: 'range', message: 'Must be between 0 and 10', min: 0, max: 10 },
  ],
},
```

### Choosing the right `enumDisplay` variant

For `enum` type properties, you can control the visual presentation using the `enumDisplay` field:

- **`choiceCard`** - Large bordered radio cards with labels, descriptions, and "Selected" badge. Best for 2-3 options where descriptions are important and visual prominence is desired.

```ts
{
  name: 'yarn.scheduler.capacity.resource-calculator',
  type: 'enum',
  enumValues: [
    {
      value: 'org.apache.hadoop.yarn.util.resource.DefaultResourceCalculator',
      label: 'Default (Memory Only)',
      description: 'Memory-based calculator suitable for clusters without CPU enforcement.',
    },
    {
      value: 'org.apache.hadoop.yarn.util.resource.DominantResourceCalculator',
      label: 'Dominant Resource',
      description: 'Considers the dominant resource usage across memory and CPU.',
    },
  ],
  enumDisplay: 'choiceCard',  // Renders as large cards in a grid
}
```

- **`toggle`** (default) - Compact toggle group (pill buttons) in horizontal layout. Space-efficient for 2-4 options with self-explanatory labels. This is the default when `enumDisplay` is omitted.

```ts
{
  name: 'yarn.scheduler.capacity.queue-state',
  type: 'enum',
  enumValues: [
    { value: 'RUNNING', label: 'Running' },
    { value: 'STOPPED', label: 'Stopped' },
    { value: 'DRAINING', label: 'Draining' },
  ],
  // enumDisplay defaults to 'toggle' - no need to specify
}
```

### Using `displayFormat` for numeric inputs

The `displayFormat` object adds visual hints and formatting to numeric input fields:

```ts
export type DisplayFormat = {
  suffix?: string; // Text appended inside input (e.g., "(0.0-1.0)")
  prefix?: string; // Text prepended to input
  multiplier?: number; // Value multiplier for display
  decimals?: number; // Number of decimal places (affects step)
};
```

**Example:**

```ts
{
  name: 'maximum-am-resource-percent',
  type: 'number',
  displayFormat: {
    suffix: ' (0.0-1.0)',  // Shows range hint inside the input
    decimals: 2,           // Allows 0.01 step increments
  },
}
```

The `suffix` renders as muted gray text positioned inside the right side of the input field, providing an inline hint about the expected range or format. The `decimals` value controls the input's `step` attribute (0.01 for 2 decimals, 0.001 for 3 decimals, etc.).

### Declaring inheritance behavior

Queue-level properties can declare how their value is inherited when a queue does not set it explicitly. Adding an `inheritanceResolver` to the descriptor enables the UI to show an "inherited from …" badge (when no explicit value is set) or an "overrides …" badge (when the queue overrides an inherited value).

The types are defined in `src/types/property-descriptor.ts`:

```ts
type InheritanceResolverContext = {
  queuePath: string;
  propertyName: string;
  configData: Map<string, string>;
  stagedChanges?: StagedChange[];
};

type InheritedValueInfo = {
  value: string;
  source: 'queue' | 'global';
  sourcePath?: string;   // Parent queue path when source is 'queue'
  isScaled?: boolean;     // True when the inherited value is scaled by queue capacity
};

type InheritanceResolver = (context: InheritanceResolverContext) => InheritedValueInfo | null;
```

`src/utils/resolveInheritedValue.ts` provides two built-in resolvers:

- **`parentChainResolver`** — walks up the parent queue hierarchy and returns the nearest ancestor that has the property set. Use for properties that cascade down the queue tree (e.g. `disable_preemption`, `priority`).

```ts
import { parentChainResolver } from '~/utils/resolveInheritedValue';

{
  name: 'disable_preemption',
  // ...
  inheritanceResolver: parentChainResolver,
}
```

- **`globalOnlyResolver`** — checks only the global `yarn.scheduler.capacity.<property>` key, skipping parent queues entirely. Use for properties where YARN falls back to a scheduler-wide default (e.g. `minimum-user-limit-percent`, `user-limit-factor`).

```ts
import { globalOnlyResolver } from '~/utils/resolveInheritedValue';

{
  name: 'minimum-user-limit-percent',
  // ...
  inheritanceResolver: globalOnlyResolver,
}
```

For properties with non-standard inheritance logic, supply a custom inline function. For example, `maximum-applications` checks two different global keys, with the second marked as scaled:

```ts
import { getGlobalValue } from '~/utils/resolveInheritedValue';

{
  name: 'maximum-applications',
  // ...
  inheritanceResolver: ({ configData, stagedChanges }) => {
    const perQueueGlobal = getGlobalValue('global-queue-max-application', configData, stagedChanges);
    if (perQueueGlobal !== undefined) {
      return { value: perQueueGlobal, source: 'global' };
    }
    const globalMax = getGlobalValue('maximum-applications', configData, stagedChanges);
    if (globalMax !== undefined) {
      return { value: globalMax, source: 'global', isScaled: true };
    }
    return null;
  },
}
```

> **Note:** `inheritanceResolver` is only meaningful for queue-level properties. Global properties do not inherit.

2. **Adjust the UI if needed.** The global settings form renders inputs based on `PropertyDescriptor.type`. For bespoke widgets, extend `src/features/global-settings/components/PropertyInput.tsx`.
3. **Update tests.** Extend `src/config/__tests__/propertyDefinitions.test.ts` if you need coverage for descriptor metadata.
4. **Verify** by running the app or unit tests (`npm run test`) and confirming the new field renders with the expected validation feedback.

### Using the `useGlobalPropertyValidation` hook

The `useGlobalPropertyValidation` hook provides validation for global-level properties. It's a simple wrapper around the validation context that uses the special `GLOBAL_QUEUE_PATH` identifier to validate properties at the scheduler level rather than the queue level.

**API:**

```ts
const { validateGlobalProperty } = useGlobalPropertyValidation();
const issues = validateGlobalProperty(propertyKey, value);
```

**Usage Example** (from `src/features/global-settings/components/GlobalSettings.tsx`):

```ts
import { useGlobalPropertyValidation } from '~/features/global-settings/hooks/useGlobalPropertyValidation';

const { validateGlobalProperty } = useGlobalPropertyValidation();

const handlePropertyChange = (propertyKey: string, value: string) => {
  const validationErrors = validateGlobalProperty(propertyKey, value);
  stageGlobalChange(propertyKey, value, validationErrors);
};
```

The hook returns a `validateGlobalProperty` function that takes a property key and value, and returns an array of `ValidationIssue` objects.

## Adding a queue-level property

1. **Add a descriptor** in `src/config/properties/queue-properties.ts`.
   - Queue descriptors use the short key (`capacity`, `maximum-capacity`, etc.).

- Populate the same core fields as global descriptors. Add `showWhen` when the field should be hidden until prerequisites are met and `enableWhen` to keep the field visible but read-only. Both take arrays of predicates that receive the current queue/global context.
- For enum properties, keep the `{ value, label, description? }` shape and select an `enumDisplay` variant if the default toggle group is not ideal.
- Set `required: true` if the field must be provided when adding new queues.

```ts
{
  name: 'example-threshold',
  displayName: 'Example Threshold',
  description: 'Upper bound applied per queue.',
  type: 'number',
  category: 'application-limits',
  defaultValue: '',
  required: false,
  validationRules: [
    {
      type: 'custom',
      message: 'Must be zero or greater',
      validator: (value) => {
        if (!value.trim()) return true;
        const parsed = Number(value);
        return !Number.isNaN(parsed) && parsed >= 0;
      },
    },
  ],
  inheritanceResolver: globalOnlyResolver, // Falls back to the global scheduler default
},
```

2. **Wire dependent UI.** Components such as `PropertyFormField` and `PropertyEditorTab` already read descriptors; only extend them if you need new interaction patterns.
3. **Tests.** Update `propertyDefinitions.test.ts` or add targeted tests under `src/features/property-editor` / `src/stores` when the new field affects staged-change flows or reducers.

## Working with validation

The validation pipeline has two layers that run automatically once descriptors and rules are defined.

### Form-level checks

- The `validationRules` array on a descriptor is compiled into Zod validators inside `src/features/property-editor/hooks/usePropertyEditor.ts`.
- Reuse the helpers in `src/config/schemas/validation.ts` whenever possible. Create new helpers there if the same rule will be reused by multiple properties.
- For global properties, `useGlobalPropertyValidation` invokes the same pipeline using the `global` queue path, so no extra wiring is required.

### Declarative business rules

Cross-field and cross-queue logic lives in `src/config/validation-rules.ts`. To add or modify a rule:

1. **Declare the rule** in the `QUEUE_VALIDATION_RULES` array (the name is historical; the same engine runs for global settings).
   ```ts
   {
     id: 'EXAMPLE_RULE',
     description: 'Describe the constraint',
     level: 'error',             // or 'warning'
     triggers: ['capacity'],     // fields that should cause this rule to re-run
     evaluate: (context) => {
       // context includes queuePath, fieldName, fieldValue, merged config, stagedChanges, etc.
       if (/* invalid */) {
         return [
           {
             queuePath: context.queuePath,
             field: 'capacity',
             message: 'Explain the problem.',
             severity: 'error',
             rule: 'example-rule',
           },
         ];
       }
       return [];
     },
   }
   ```
2. **Share utilities** by adding helpers in `src/features/validation/utils` when the logic is complex.
3. **Categorize severity (optional).** If the rule’s outcome should be treated as non-blocking despite returning `severity: 'error'`, update `src/features/validation/ruleCategories.ts` so `isBlockingError` reflects the desired behavior.
4. **Surface to the UI.** The `ValidationContext` automatically merges new rule output. Field-level components read `ValidationIssue[]` through `useValidation`, so no extra wiring is required beyond returning the correct `rule` ID and `severity`.
5. **Test it.** Add unit tests near the rule implementation (for example, under `src/config/__tests__` or a new `*.test.ts` beside the helper) and run `npm run test`.

### Property condition utilities

The `src/utils/propertyConditions.ts` module provides two utility functions for evaluating `showWhen` and `enableWhen` conditions outside of the form rendering pipeline. These are useful when you need to programmatically check property visibility or enabled state:

#### `shouldShowProperty(property, options)`

Evaluates all `showWhen` conditions for a property. Returns `true` if the property should be visible.

```ts
import { shouldShowProperty } from '~/utils/propertyConditions';

const visible = shouldShowProperty(property, {
  scope: 'global',
  property,
  propertyValue: currentValue,
  values: formValues,
  globalValues,
  stagedChanges,
  configData,
  schedulerInfo,
  // ... other context
});
```

#### `isPropertyEnabled(property, options)`

Evaluates all `enableWhen` conditions for a property. Returns `true` if the property should be interactive (not disabled).

```ts
import { isPropertyEnabled } from '~/utils/propertyConditions';

const enabled = isPropertyEnabled(property, {
  scope: 'queue',
  queuePath: 'root.production',
  property,
  propertyValue: currentValue,
  // ... other context
});
```

**Error handling:** Both functions catch and log errors from condition evaluation, defaulting to `true` (show/enable) on failure to ensure the UI remains functional even with misconfigured conditions.

**When to use these utilities:**

- In component logic that needs to conditionally render UI based on property visibility
- When computing available properties for autocomplete or validation
- For debugging or testing property condition behavior
- Use inline predicates in property definitions when the condition is simple and self-contained

## Sanity checklist

- [ ] Descriptor added to the appropriate file with accurate metadata.
- [ ] UI renders the expected input type (extend components only if necessary).
- [ ] Form-level `validationRules` cover formatting and basic range checks.
- [ ] Declarative rule added to `src/config/validation-rules.ts` (and helper utilities or categories updated when needed).
- [ ] `inheritanceResolver` set on queue-level properties that inherit from a parent queue or global default.
- [ ] Tests updated or added, and `npm run test` completes successfully.
