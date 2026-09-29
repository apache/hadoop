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


import type { QueueInfo } from './queue';
import type { SchedulerInfo } from './scheduler';
import type { StagedChange } from './staged-change';

export type PropertyType = 'string' | 'number' | 'boolean' | 'enum' | 'list';

export type PropertyCategory =
  | 'resource'
  | 'scheduling'
  | 'security'
  | 'core'
  | 'application-limits'
  | 'placement'
  | 'container-allocation'
  | 'async-scheduling'
  | 'capacity'
  | 'dynamic-queues'
  | 'node-labels'
  | 'preemption';

export type ComparisonOperator = '<' | '<=' | '>' | '>=' | '==' | '!=';

export type ValidationRule = {
  type: 'range' | 'pattern' | 'comparison' | 'custom';
  message: string;
  min?: number;
  max?: number;
  pattern?: string;
  field?: string;
  operator?: ComparisonOperator;
  validator?: (value: string) => boolean;
};

export type DisplayFormat = {
  suffix?: string;
  prefix?: string;
  multiplier?: number;
  decimals?: number;
};

export type PropertyEnumOption = {
  value: string;
  label: string;
  description?: string;
};

export type PropertyEvaluationScope = 'global' | 'queue';

export type PropertyEvaluationContext = {
  scope: PropertyEvaluationScope;
  property: PropertyDescriptor;
  propertyValue: string;
  values: Record<string, string>;
  globalValues: Record<string, string>;
  queuePath?: string;
  queueInfo?: QueueInfo | null;
  schedulerInfo?: SchedulerInfo | null;
  stagedChanges: StagedChange[];
  configData: Map<string, string>;
  getValue: (name: string) => string | undefined;
  getGlobalValue: (name: string) => string | undefined;
  getQueueValue: (queuePath: string, name: string) => string | undefined;
  getConfigValue: (key: string) => string | undefined;
};

export type PropertyCondition = (context: PropertyEvaluationContext) => boolean;

export type InheritedValueInfo = {
  value: string;
  source: 'queue' | 'global';
  sourcePath?: string;
  isScaled?: boolean;
};

export type InheritanceResolverContext = {
  queuePath: string;
  propertyName: string;
  configData: Map<string, string>;
  stagedChanges?: StagedChange[];
};

export type InheritanceResolver = (context: InheritanceResolverContext) => InheritedValueInfo | null;

export type PropertyDescriptor = {
  name: string;
  displayName: string;
  description: string;
  type: PropertyType;
  category: PropertyCategory;
  defaultValue: string;
  required: boolean;
  templateSupport?: boolean;
  validationRules?: ValidationRule[];
  enumValues?: PropertyEnumOption[];
  enumDisplay?: 'toggle' | 'choiceCard';
  showWhen?: PropertyCondition[];
  enableWhen?: PropertyCondition[];
  displayFormat?: DisplayFormat;
  deprecated?: boolean;
  deprecationMessage?: string;
  formFieldName?: string; // Escaped name for React Hook Form
  originalName?: string; // Original name before escaping
  inheritanceResolver?: InheritanceResolver;
};
