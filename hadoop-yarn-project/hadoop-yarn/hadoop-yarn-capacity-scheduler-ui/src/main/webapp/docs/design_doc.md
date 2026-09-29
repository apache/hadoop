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


# YARN Scheduler UI - Design Document

> **JIRA**: [YARN-11885](https://issues.apache.org/jira/browse/YARN-11885)

---

## 1. What is this?

The **YARN Capacity Scheduler UI** is a modern web interface for managing Apache Hadoop YARN Capacity Scheduler configurations. It provides visual tools for queue management, placement rules, capacity planning, and staged configuration changes with validation before applying them to a live YARN cluster.

### Main Features

1. **Visual Queue Tree Management**
    - Interactive, draggable, zoomable tree visualization using XYFlow
    - Create, edit, and delete queues with real-time validation
    - Configure queue properties: capacities (percentage/weight/absolute), states, ACLs, resource limits
    - Visual capacity indicators and resource statistics
    - Queue search with highlighting

2. **Placement Rules Editor**
    - Guided forms for authoring placement rules
    - Support for multiple rule types (user, group, application name, etc.)
    - Drag-and-drop rule ordering and priority management
    - Migration from legacy placement rules to new format
    - Validation before applying updates

3. **Staged Changes System**
    - Review all pending configuration edits in a unified panel
    - Apply changes in batches or revert individual changes
    - Side-by-side comparison of staged vs live configuration
    - Visual highlighting of configuration deltas
    - Validation errors displayed per change

4. **Node Labels & Partitions**
    - Create and manage node labels (resource partitions)
    - Assign nodes to labels with bulk operations
    - Configure per-label capacity settings for queues
    - View node-to-label and label-to-node mappings

5. **Real-Time Validation System**
    - Property-level validation during editing
    - Cross-queue dependency validation (sibling capacities, parent-child constraints)
    - Warning and error severity levels
    - Blocks applying changes until errors are resolved
    - Clear error messages with affected queues highlighted

6. **Global Scheduler Settings**
    - Configure cluster-wide scheduler properties
    - Resource calculator selection (memory-only vs dominant resource)
    - Application limits and scheduling behavior
    - Preemption policies and async scheduling settings
    - Legacy mode toggle and capacity configuration modes

7. **Read-Only Mode Support**
    - Configurable read-only mode via YARN property (`yarn.webapp.scheduler-ui.read-only.enable`)
    - Users can stage and validate changes but cannot apply them
    - Visual indicators (lock icon, disabled buttons) in UI
    - Useful for production environments with restricted access

---

## 2. Architecture

### 2.1 High-Level Overview

The application follows a **client-side SPA architecture** with clear separation of concerns:

```
┌─────────────────────────────────────────────────────────────┐
│                    React UI Layer                           │
│  (Routes, Components, Forms, Tree Visualization)            │
│  - Queue tree with XYFlow                                   │
│  - Property editors with React Hook Form                    │
│  - Placement rules builder                                  │
│  - Node labels management                                   │
└──────────────────────────┬──────────────────────────────────┘
                           │
                           ▼
┌─────────────────────────────────────────────────────────────┐
│              State Management Layer                         │
│    (Zustand Store with Immer - Sliced Architecture)         │
│  - 8 feature slices sharing single state tree               │
│  - Staged changes before apply                              │
│  - Cross-queue validation                                   │
│  - Shared API client instance                               │
└──────────────────────────┬──────────────────────────────────┘
                           │
                           ▼
┌─────────────────────────────────────────────────────────────┐
│               API Client Layer                              │
│   (YarnApiClient + MSW for development mocking)             │
│  - Auto-detects YARN security mode                          │
│  - Handles authentication (simple vs kerberos)              │
│  - MSW: static fixtures / cluster proxy / off               │
└──────────────────────────┬──────────────────────────────────┘
                           │
                           ▼
┌─────────────────────────────────────────────────────────────┐
│         YARN ResourceManager REST API                       │
│        /ws/v1/cluster/* endpoints                           │
│  - Scheduler data and configuration                         │
│  - Mutation via PUT /scheduler-conf                         │
│  - Node labels management                                   │
└─────────────────────────────────────────────────────────────┘
```

**Key Architectural Principles**:

- **Client-Side SPA**: Entire application runs in the browser; no server-side rendering
- **Staged Changes Pattern**: Never apply changes immediately; always stage → validate → review → apply
- **Configuration as Code**: All YARN properties defined in TypeScript with validation rules and metadata
- **Immutable State Updates**: Zustand + Immer for clean state mutations
- **Development Mocking**: MSW enables local development without a YARN cluster

---

### 2.2 Detailed Technical Architecture

#### State Management - Zustand with Slices Pattern

**Location**: `src/stores/schedulerStore.ts`

The application uses a **single Zustand store** with **Immer middleware** for immutable updates. The store is composed of multiple feature slices that share a single state tree and API client instance:

**Store Slices** (`src/stores/slices/`):

1. **schedulerDataSlice** - Core scheduler data
    - Queue tree structure (`SchedulerInfo`)
    - Configuration properties (key-value map)
    - Node labels and partitions
    - Read-only mode flag
    - Loading and error states

2. **queueDataSlice** - Queue hierarchy utilities
    - Find queue by path
    - Get parent/siblings/children
    - Path manipulation utilities
    - Queue tree traversal functions

3. **queueSelectionSlice** - UI selection state
    - Currently selected queue(s)
    - Multi-select support
    - Selection history

4. **stagedChangesSlice** - Pending changes management
    - Stage queue property changes
    - Stage queue add/remove operations
    - Revert individual or all changes
    - Apply changes to YARN (converts to mutations)

5. **placementRulesSlice** - Placement rule authoring
    - Rule CRUD operations
    - Rule ordering and validation
    - Migration from legacy format

6. **nodeLabelsSlice** - Node label operations
    - Create/delete labels
    - Assign nodes to labels
    - Node-to-label mappings

7. **capacityEditorSlice** - Capacity editing
    - Bulk capacity operations
    - Capacity mode switching
    - Validation helpers

8. **searchSlice** - Queue search
    - Search query state
    - Match highlighting
    - Search results

**Pattern**: All slices use Immer middleware, allowing direct mutation syntax:

```typescript
set((state) => {
  state.stagedChanges.push(newChange);
  state.configData.set(key, value);
});
```

---

#### API Client - YarnApiClient

**Location**: `src/lib/api/YarnApiClient.ts`

HTTP client for YARN ResourceManager REST APIs with the following features:

**Key Capabilities**:

- Auto-detects YARN security mode (simple vs kerberos) on first request
- Handles username injection for simple auth mode (`?user.name=yarn`)
- Detects read-only mode from YARN config (`yarn.scheduler.capacity.ui.readonly`)
- Timeout handling (30 seconds default)
- Error parsing and standardization
- Credentials included for cross-origin requests

**Primary Methods**:

- `getScheduler()` - Fetch queue hierarchy with live metrics
- `getSchedulerConf()` - Fetch configuration properties
- `updateSchedulerConf(updateInfo)` - Update configuration (mutation)
- `validateSchedulerConf(updateInfo)` - Validate changes before applying
- `getNodeLabels()` / `addNodeLabels()` / `removeNodeLabels()`
- `getNodeToLabels()` / `replaceNodeToLabels()`
- `getNodes()` - Cluster nodes information

**YARN API Endpoints Used**:

- `GET /ws/v1/cluster/scheduler`
- `GET /ws/v1/cluster/scheduler-conf`
- `PUT /ws/v1/cluster/scheduler-conf`
- `POST /ws/v1/cluster/scheduler-conf/validate`
- `GET /ws/v1/cluster/scheduler-conf/version`
- `GET /ws/v1/cluster/get-node-labels`
- `POST /ws/v1/cluster/add-node-labels`
- `POST /ws/v1/cluster/remove-node-labels`
- `GET /ws/v1/cluster/get-node-to-labels`
- `POST /ws/v1/cluster/replace-node-to-labels`
- `GET /ws/v1/cluster/nodes`
- `GET /conf?name=<property>`

---

#### Configuration System

**Property Descriptors** (`src/config/properties/`):

The UI has extensive metadata about YARN scheduler properties:

- **queue-properties.ts** - All queue-level properties
- **global-properties.ts** - Global scheduler properties

Each property descriptor includes:

```typescript
{
  name: 'capacity',                    // Short name without prefix
  displayName: 'Capacity',             // UI label
  description: 'Queue capacity...',    // Help text
  type: 'number' | 'string' | 'enum' | 'boolean',
  category: 'capacity',                // Property category
  defaultValue: '0',                   // Default if not set
  required: false,                     // Validation
  validationRules: [                   // Schema validation
    { type: 'range', min: 0, max: 100, message: 'Must be 0-100' }
  ],
  showWhen: (context) => boolean,      // Conditional visibility
  enableWhen: (context) => boolean,    // Conditional enable
  enumValues: [...],                   // For enum types
  enumDisplay: 'choiceCard' | 'toggle', // UI style
  displayFormat: {                     // Numeric formatting
    suffix: ' MB',
    decimals: 2
  }
}
```

**Property Key Format**: Hierarchical `yarn.scheduler.capacity.<queue-path>.<property>`

Examples:

- `yarn.scheduler.capacity.root.production.capacity`
- `yarn.scheduler.capacity.maximum-applications`
- `yarn.scheduler.capacity.root.dev.accessible-node-labels.gpu.capacity`

**Schemas** (`src/config/schemas/`): Zod schemas for common formats (capacities, ACLs, percentages)

**Validation Rules** (`src/config/validation-rules.ts`): Business validation rules with cross-queue logic

---

#### Staged Changes & Mutation System

**Core Principle**: Changes are **never applied immediately**. All modifications go through staging.

**Workflow**:

```
1. User edits property
        ↓
2. stageQueueChange() creates StagedChange object
        ↓
3. Validation runs (property + cross-queue)
        ↓
4. User reviews in "Staged Changes" panel
        ↓
5. User clicks "Apply"
        ↓
6. applyStagedChanges() converts to YARN mutations
        ↓
7. POST /scheduler-conf/validate (optional)
        ↓
8. PUT /scheduler-conf with SchedConfUpdateInfo
```

**StagedChange Structure** (`src/types/staged-change.ts`):

```typescript
{
  id: string;                    // Unique identifier
  type: 'add' | 'update' | 'remove';
  queuePath: string | 'global';  // Queue path or 'global'
  property: string;              // Property name
  oldValue?: string;             // Previous value
  newValue?: string;             // New value
  timestamp: number;             // When staged
  label?: string;                // For node label changes
  validationErrors?: ValidationIssue[];
}
```

**Mutation Builder** (`src/features/staged-changes/utils/mutationBuilder.ts`):

Translates staged changes into YARN's `SchedConfUpdateInfo` format:

```typescript
{
  "add-queue": [{
    "queue-name": "root.new-queue",
    "params": {
      "entry": [
        { "key": "capacity", "value": "50" },
        { "key": "state", "value": "RUNNING" }
      ]
    }
  }],
  "update-queue": [{
    "queue-name": "root.existing",
    "params": {
      "entry": [{ "key": "capacity", "value": "75" }]
    }
  }],
  "remove-queue": "root.old-queue",
  "global-updates": [{
    "entry": [
      { "key": "yarn.scheduler.capacity.maximum-applications", "value": "5000" }
    ]
  }]
}
```

---

#### Validation System

**Multi-Layered Architecture** (`src/features/validation/`):

```
┌────────────────────────────────────────────────────┐
│         Layer 1: Schema Validation                 │
│  (Property descriptors - format, range, regex)     │
└──────────────────┬─────────────────────────────────┘
                   ▼
┌────────────────────────────────────────────────────┐
│      Layer 2: Business Validation Rules            │
│  (validation-rules.ts - cross-field logic)         │
└──────────────────┬─────────────────────────────────┘
                   ▼
┌────────────────────────────────────────────────────┐
│     Layer 3: Cross-Queue Validation                │
│  (crossQueue.ts - dependency-aware validation)     │
│  - Detects affected queues                         │
│  - Validates parent/children/siblings              │
│  - Ensures capacity sums, mode consistency         │
└────────────────────────────────────────────────────┘
```

**Key Components**:

1. **service.ts** - Main validation orchestration
    - `validateField()` - Single property validation with context
    - `validateQueue()` - All properties in a queue
    - `hasBlockingIssues()` - Check for blocking errors

2. **crossQueue.ts** - Cross-queue validation engine
    - `validatePropertyChange()` - Validates a property change with cross-queue awareness
    - `validateStagedChanges()` - Validates all staged changes (or a filtered subset)
    - Handles parent/children/siblings relationships

3. **ruleCategories.ts** - Rule categorization
    - `CROSS_QUEUE_RULES` - Affects multiple queues (re-validate dependencies)
    - `QUEUE_SPECIFIC_RULES` - Only validates single queue
    - `WARNING_ONLY_RULES` - Never blocks applying changes

4. **utils/affectedQueues.ts** - Dependency detection
    - Determines which queues need re-validation when a property changes
    - Example: changing parent capacity affects all children

**Validation Rules Examples** (from `validation-rules.ts`):

- `CAPACITY_SUM` - Sibling capacities must sum correctly
- `MAX_CAPACITY_CONSTRAINT` - Maximum capacity >= capacity
- `CONSISTENT_CAPACITY_MODE` - Siblings use same mode (legacy mode)
- `PARENT_CHILD_CAPACITY_CONSTRAINT` - Child resources ≤ parent
- `PARENT_CHILD_CAPACITY_MODE` - Absolute mode inheritance (legacy)
- `WEIGHT_MODE_TRANSITION_FLEXIBLE_AQC` - Auto-queue compatibility

**Validation Context**:

```typescript
{
  queuePath: string;
  fieldName: string;
  fieldValue: unknown;
  config: Map<string, string>;      // Full config + staged
  schedulerData?: SchedulerInfo;
  stagedChanges: StagedChange[];
  legacyModeEnabled: boolean;
}
```

---

#### Queue Structure Representation

**Data Model** (`src/types/scheduler.ts`):

- **SchedulerInfo** - Root scheduler with queue tree
- **QueueInfo** - Individual queue:
    - Properties (capacity, state, ACLs, limits)
    - Children (recursive hierarchy)
    - Live metrics (used capacity, running apps, pending apps)
    - Per-partition capacities

**Queue Path Format**: Dot-separated hierarchical identifiers

- Root: `root`
- Children: `root.production`, `root.production.critical`
- Always start with `root`

**Queue Traversal Utilities** (`queueDataSlice.ts`):

- Find queue by path
- Get parent path
- Get siblings
- Get all descendants
- Path validation

---

#### Routing

**Framework**: React Router v7 in SPA mode (`ssr: false`)

**Routes** (file-based in `src/app/routes/`):

- `layout.tsx` - Root layout with navigation
- `home.tsx` - Queue tree visualization (XYFlow)
- `placement-rules.tsx` - Placement rules editor
- `node-labels.tsx` - Node labels management
- `global-settings.tsx` - Global scheduler settings

---

#### Development Support - Mock Service Worker (MSW)

**Location**: `src/lib/api/mocks/handlers.ts`

MSW enables three modes controlled by `VITE_API_MOCK_MODE`:

1. **`static`** (default in dev)
    - Serves JSON fixtures from `public/mock/ws/v1/cluster/*.json`
    - No YARN cluster required
    - Consistent data for development and testing

2. **`cluster`**
    - Proxies requests to real YARN cluster via `VITE_CLUSTER_PROXY_TARGET`
    - Example: `http://rm-host:8088`
    - Live data from actual cluster

3. **`off`**
    - Disables mocking entirely
    - Production builds use this mode

MSW boots automatically in dev mode via `src/app/entry.client.tsx`.

---

## 3. Used Libraries

### Core Framework & Language

- **React 19.1.0** - UI framework
- **TypeScript 5.8.3** - Strict mode type checking
- **React Router 7.5.3** - Routing and bundler (SPA mode, Vite-powered)
- **React Compiler** (`babel-plugin-react-compiler`) - Automatic memoization and optimization

### State Management

- **Zustand 5.0.6** - Lightweight state management with minimal boilerplate
- **Immer 10.0.0** - Immutable state updates with mutable syntax

### UI Framework & Styling

- **Tailwind CSS 4.1.4** - Utility-first CSS framework
- **Radix UI** - Headless accessible component primitives:
    - Accordion, Checkbox, Dialog, Dropdown Menu, Label, Popover, Progress
    - Scroll Area, Select, Separator, Switch, Tabs, Toggle, Tooltip
    - Context Menu, Collapsible
- **shadcn/ui** - Pre-built components using Radix + Tailwind
- **Lucide React 0.525.0** - Icon library
- **class-variance-authority 0.7.1** - Component variant styling
- **clsx 2.1.1** + **tailwind-merge 3.3.1** - Conditional className utilities

### Data Visualization

- **XYFlow (React Flow) 12.8.1** - Interactive node-based diagrams for queue tree
- **Dagre 0.8.5** - Graph layout algorithm for auto-arranging tree

### Forms & Validation

- **React Hook Form 7.59.0** - Performant form state management
- **Zod 3.25.71** - TypeScript-first schema validation
- **@hookform/resolvers 5.1.1** - Integration between React Hook Form and Zod

### Drag & Drop

- **@atlaskit/pragmatic-drag-and-drop 1.7.4** - Modern drag-and-drop library
- **@atlaskit/pragmatic-drag-and-drop-auto-scroll 2.1.1** - Auto-scroll during drag
- **@atlaskit/pragmatic-drag-and-drop-hitbox 1.1.0** - Hit detection utilities

### UI Components & Utilities

- **cmdk 1.1.1** - Command menu component (Cmd+K interface)
- **Sonner 2.0.5** - Toast notifications
- **Vaul 1.1.2** - Drawer component
- **react-resizable-panels 3.0.3** - Resizable layout panels
- **es-toolkit 1.39.6** - Modern utility library (replaces lodash)
- **nanoid 5.1.5** - Compact URL-safe unique ID generation
- **isbot 5.1.27** - Bot detection

### Testing

- **Vitest 3.2.4** - Vite-native test runner (Jest-compatible API)
- **@vitest/coverage-v8 3.2.4** - Code coverage reporting
- **@testing-library/react 16.3.0** - React component testing utilities
- **@testing-library/jest-dom 6.6.3** - Custom Jest matchers for DOM
- **@testing-library/user-event 14.6.1** - User interaction simulation
- **Happy DOM 20.0.2** - Lightweight DOM implementation for testing
- **MSW (Mock Service Worker) 2.10.2** - API mocking for tests and development

### Build & Development Tools

- **Vite 6.4.1** - Fast build tool and dev server with HMR
- **@react-router/dev 7.5.3** - React Router bundler integration
- **@vitejs/plugin-react 4.4.1** - React plugin for Vite
- **vite-tsconfig-paths 5.1.4** - Support for TypeScript path aliases (`~/`)
- **vite-plugin-babel 1.3.2** - Babel integration for React Compiler

### Code Quality

- **ESLint 9.18.0** - Linting with TypeScript and React rules
    - **typescript-eslint 8.20.0**
    - **@eslint-react/eslint-plugin 2.2.4**
    - **eslint-plugin-react-compiler 19.1.0-rc.2**
    - **eslint-plugin-react-hooks 5.1.0**
- **Prettier 3.5.0** - Code formatting
- **Husky 9.1.7** - Git hooks
- **lint-staged 16.1.2** - Run linters on staged files

---

## 4. Extending with Properties and Validations

### 4.1 Adding New Queue Properties

**Documentation**: `docs/development/extending-scheduler-properties.md`

**Steps**:

1. **Add descriptor** to `src/config/properties/queue-properties.ts`:

```typescript
{
  name: 'my-new-property',           // Short name without prefix
  displayName: 'My New Property',    // UI label
  description: 'What this property controls and how it affects scheduling',
  type: 'number',                    // 'string' | 'number' | 'boolean' | 'enum'
  category: 'scheduling',            // See PropertyCategory type
  defaultValue: '10',
  required: false,
  validationRules: [
    {
      type: 'range',
      message: 'Must be between 0 and 100',
      min: 0,
      max: 100
    }
  ],
  // Optional: Conditional visibility
  showWhen: (context) => {
    return context.legacyModeEnabled;
  },
  // Optional: Conditional enable
  enableWhen: (context) => {
    const state = context.config.get(
      buildPropertyKey(context.queuePath, 'state')
    );
    return state === 'RUNNING';
  },
}
```

2. **Property automatically appears** in the property editor UI - no additional wiring needed

3. **Add validation rules** if needed (see section 4.2)

4. **Update tests** in `src/config/__tests__/propertyDefinitions.test.ts`

**Available Categories**:

- `resource` - CPU, memory limits
- `scheduling` - Ordering, priorities
- `security` - ACLs, user/group permissions
- `core` - Fundamental queue settings
- `application-limits` - Application count limits
- `placement` - Queue selection rules
- `container-allocation` - Container sizing
- `async-scheduling` - Async mode settings
- `capacity` - Capacity and max-capacity
- `dynamic-queues` - Auto-queue creation
- `node-labels` - Label-specific settings
- `preemption` - Preemption policies

**For enum types**:

```typescript
{
  name: 'ordering-policy',
  type: 'enum',
  enumValues: [
    { value: 'fifo', label: 'FIFO', description: 'First-in, first-out' },
    { value: 'fair', label: 'Fair', description: 'Fair sharing' },
    { value: 'priority', label: 'Priority', description: 'By priority' }
  ],
  enumDisplay: 'toggle',  // 'toggle' (pills) or 'choiceCard' (large cards)
}
```

**For numeric inputs with formatting**:

```typescript
{
  name: 'maximum-allocation-mb',
  type: 'number',
  displayFormat: {
    suffix: ' MB',
    decimals: 0
  },
}
```

---

### 4.2 Adding Validation Rules

**Documentation**: `docs/development/adding-validation-rules.md`

**Steps**:

1. **Define rule** in `src/config/validation-rules.ts`:

```typescript
{
  id: 'MY_NEW_RULE',
  description: 'Brief explanation of the constraint being enforced',
  level: 'error',  // or 'warning'
  triggers: ['my-new-property', 'related-property'],  // Properties that trigger
  evaluate: (context) => evaluateMyNewRule(context),
}
```

2. **Implement evaluator function**:

```typescript
function evaluateMyNewRule(context: ValidationContext): ValidationIssue[] {
  const issues: ValidationIssue[] = [];

  // Skip for template queues if not applicable
  if (isTemplateQueuePath(context.queuePath)) {
    return issues;
  }

  // Skip if rule only applies in legacy mode
  if (!context.legacyModeEnabled) {
    return issues;
  }

  // Get current value
  const myValue = context.fieldValue as number;

  // Get related value from config
  const relatedKey = buildPropertyKey(context.queuePath, 'related-property');
  const relatedValue = parseFloat(context.config.get(relatedKey) || '0');

  // Validation logic
  if (myValue > relatedValue) {
    issues.push({
      queuePath: context.queuePath,
      field: context.fieldName,
      message: `My property (${myValue}) must not exceed related property (${relatedValue})`,
      severity: 'error',
      rule: 'my-new-rule', // lowercase kebab-case
    });
  }

  return issues;
}
```

3. **Categorize rule** in `src/features/validation/ruleCategories.ts`:

```typescript
// If rule affects multiple queues (parent, children, or siblings)
export const CROSS_QUEUE_RULES = [
  // ... existing
  'my-new-rule',
];

// If rule only validates the single queue being edited
export const QUEUE_SPECIFIC_RULES = [
  // ... existing
  'my-new-rule',
];

// If rule should never block applying changes (informational only)
export const WARNING_ONLY_RULES = [
  // ... existing
  'my-new-rule',
];
```

4. **Implement affected queues logic** if cross-queue (in `src/features/validation/utils/affectedQueues.ts`):

```typescript
export function getAffectedQueuesForRule(
  rule: string,
  changedQueuePath: string,
  schedulerData?: SchedulerInfo,
): string[] {
  switch (rule) {
    case 'my-new-rule':
      // Return list of queue paths that need re-validation
      // Example: parent and all siblings
      const parent = getParentQueuePath(changedQueuePath);
      const siblings = getSiblingQueues(changedQueuePath, schedulerData);
      return [parent, ...siblings.map((q) => q.queuePath)];

    // ... other rules
  }
}
```

5. **Write tests** in `src/config/__tests__/validation-rules.test.ts`

**Key Validation Patterns**:

- **Sibling validation**: Iterate over siblings and sum/compare values
- **Parent-child validation**: Get parent value and compare with children
- **Mode-dependent validation**: Check `context.legacyModeEnabled`
- **Template queues**: Use `isTemplateQueuePath()` to skip if not applicable
- **Conditional rules**: Check if related properties are set before validating

**Validation Issue Structure**:

```typescript
{
  queuePath: string; // Which queue has the issue
  field: string; // Which property is invalid
  message: string; // User-friendly error message
  severity: 'error' | 'warning';
  rule: string; // Rule identifier (kebab-case)
}
```

---

### 4.3 Adding Global Properties

Similar to queue properties, but in `src/config/properties/global-properties.ts`:

```typescript
{
  name: 'yarn.scheduler.capacity.my-global-setting',  // Full property key
  displayName: 'My Global Setting',
  description: 'What this global setting controls',
  type: 'boolean',
  category: 'core',
  defaultValue: 'false',
  required: false,
}
```

Property automatically appears in the Global Settings page (`src/app/routes/global-settings.tsx`).

---

### 4.4 Key Files for Developers

**Configuration System**:

- `src/config/properties/queue-properties.ts` - Queue property definitions
- `src/config/properties/global-properties.ts` - Global property definitions
- `src/config/validation-rules.ts` - Business validation rules
- `src/config/schemas/` - Zod validation schemas

**Validation System**:

- `src/features/validation/service.ts` - Main validation entry point
- `src/features/validation/crossQueue.ts` - Cross-queue validation engine
- `src/features/validation/ruleCategories.ts` - Rule categorization
- `src/features/validation/utils/affectedQueues.ts` - Dependency detection
- `src/features/validation/utils/dedupeIssues.ts` - Issue deduplication

**State Management**:

- `src/stores/schedulerStore.ts` - Main Zustand store
- `src/stores/slices/` - Feature slices (8 slices)

**API & Types**:

- `src/lib/api/YarnApiClient.ts` - YARN REST API client
- `src/types/` - TypeScript type definitions

**Utilities**:

- `src/utils/propertyUtils.ts` - Property key construction
- `src/utils/capacityUtils.ts` - Capacity parsing and validation
- `src/utils/treeUtils.ts` - Queue tree traversal (`flattenQueueTree`, `traverseQueueTree`, `findQueueByPath`)
- `src/utils/nodeLabelUtils.ts` - Node label name normalization
- `src/lib/errors/readOnlyGuard.ts` - Read-only mode enforcement helpers

---

## Appendix: Development Quick Reference

### Environment Setup

```bash
npm install
# Create .env file (see .env.example)
npm run dev  # Starts at http://localhost:5173
```

### Development Commands

```bash
# Development
npm run dev                 # Start dev server

# Testing
npm run test                # Run tests in watch mode
npm run test:run            # Single test run (CI mode)
npm run test:coverage       # Generate coverage report

# Type Checking & Building
npm run typecheck           # Generate types and type check
npm run build               # Production build to ./build
npm start                   # Serve production build

# Code Quality
npm run lint                # Lint TypeScript files
npm run lint:fix            # Auto-fix linting issues
npm run format              # Format code with Prettier
npm run format:check        # Check formatting
```

### Environment Variables

```env
# API Mock Mode (static, cluster, or off)
VITE_API_MOCK_MODE=static

# For cluster mode: proxy target
VITE_CLUSTER_PROXY_TARGET=http://rm-host:8088

# YARN username for simple auth
VITE_YARN_USER_NAME=yarn

# Read-only mode testing (development only)
VITE_READONLY_MODE=false
```

### Project Structure

```
yarn-scheduler-ui/
├── src/
│   ├── app/                      # React Router entry and routes
│   ├── components/               # Shared UI components
│   ├── config/                   # Configuration system
│   │   ├── properties/           # Property descriptors
│   │   ├── schemas/              # Zod schemas
│   │   └── validation-rules.ts   # Business validation
│   ├── features/                 # Feature modules
│   │   ├── queue-management/     # Queue tree visualization
│   │   │   ├── components/       # QueueCardNode, CapacityEditorDialog, etc.
│   │   │   ├── hooks/            # useCapacityEditor, useQueueActions
│   │   │   └── utils/            # capacityDisplay, capacityEditor, etc.
│   │   ├── property-editor/      # Queue property editing
│   │   │   └── components/       # PropertyPanel, PropertyFormField, etc.
│   │   ├── staged-changes/       # Change review and mutation
│   │   ├── validation/           # Cross-queue validation engine
│   │   ├── placement-rules/      # Placement rule builder
│   │   ├── node-labels/          # Node label management
│   │   ├── template-config/      # Auto-queue templates
│   │   ├── queue-comparison/     # Queue comparison tool
│   │   └── global-settings/      # Global scheduler settings
│   ├── hooks/                    # Shared React hooks
│   ├── lib/                      # Libraries and utilities
│   │   ├── api/                  # YarnApiClient + MSW
│   │   └── errors/               # Error handling + readOnlyGuard
│   ├── stores/                   # Zustand state management
│   │   ├── schedulerStore.ts     # Main store
│   │   └── slices/               # 8 feature slices
│   ├── types/                    # TypeScript definitions
│   ├── utils/                    # Utility functions (treeUtils, nodeLabelUtils, etc.)
│   └── testing/                  # Test utilities
├── public/mock/                  # Mock API responses
├── docs/                         # Documentation
└── [config files]                # Vite, React Router, Vitest, etc.
```

---

## Summary

The YARN Scheduler UI is a modern web application that provides a visual interface for managing YARN Capacity Scheduler configurations. Key highlights:

- **Client-side SPA** with React 19 and TypeScript (strict mode)
- **Staged changes pattern** ensures all edits are reviewed and validated before applying
- **Comprehensive validation** with cross-queue dependency detection
- **Extensible configuration system** with property descriptors and validation rules
- **Development-friendly** with MSW for local development without a YARN cluster
- **Well-tested** with Vitest, Testing Library, and MSW integration
- **Modern tech stack** leveraging React Compiler, Zustand, Tailwind, and Radix UI
