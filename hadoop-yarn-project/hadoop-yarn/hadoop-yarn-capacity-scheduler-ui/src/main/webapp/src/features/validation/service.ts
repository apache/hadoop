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


import {
  mergeStagedConfig,
  applyFieldPreview,
  buildEffectivePropertyKey,
} from '~/utils/configUtils';
import {
  runFieldValidation,
  type ValidationContext as RuleContext,
} from '~/config/validation-rules';
import type { ValidationIssue, SchedulerInfo, StagedChange } from '~/types';
import { SPECIAL_VALUES } from '~/types/constants/special-values';
import { dedupeIssues } from './utils/dedupeIssues';

type Severity = ValidationIssue['severity'];

export interface FieldValidationOptions {
  queuePath: string;
  fieldName: string;
  value: unknown;
  configData: Map<string, string>;
  stagedChanges: StagedChange[];
  schedulerData?: SchedulerInfo | null;
}

export interface QueueValidationOptions {
  queuePath: string;
  properties: Record<string, string>;
  configData: Map<string, string>;
  stagedChanges: StagedChange[];
  schedulerData?: SchedulerInfo | null;
}

export interface ValidationResult {
  valid: boolean;
  issues: ValidationIssue[];
}

export function validateField(options: FieldValidationOptions): ValidationResult {
  const { queuePath, fieldName, value, configData, stagedChanges, schedulerData } = options;

  const withStaged = mergeStagedConfig(configData, stagedChanges);
  const effectiveConfig = applyFieldPreview(withStaged, queuePath, fieldName, value);

  const issues = runFieldValidation(
    buildRuleContext({
      queuePath,
      fieldName,
      fieldValue: value,
      config: effectiveConfig,
      schedulerData,
      stagedChanges,
    }),
  );

  return {
    valid: issues.every((issue) => issue.severity !== 'error'),
    issues,
  };
}

export function validateQueue(options: QueueValidationOptions): ValidationResult {
  const { queuePath, properties, configData, stagedChanges, schedulerData } = options;

  const withStaged = mergeStagedConfig(configData, stagedChanges);
  const effectiveConfig = new Map(withStaged);

  Object.entries(properties).forEach(([field, value]) => {
    const key = buildEffectivePropertyKey(queuePath, field);
    if (value === '' || value === null || value === undefined) {
      effectiveConfig.delete(key);
    } else {
      effectiveConfig.set(key, value);
    }
  });

  const fieldsToValidate = new Set<string>(Object.keys(properties));
  if (!fieldsToValidate.has('capacity')) {
    fieldsToValidate.add('capacity');
  }

  const issues: ValidationIssue[] = [];

  fieldsToValidate.forEach((field) => {
    const key = buildEffectivePropertyKey(queuePath, field);
    const fieldValue = Object.prototype.hasOwnProperty.call(properties, field)
      ? properties[field]
      : (effectiveConfig.get(key) ?? '');

    const context = buildRuleContext({
      queuePath,
      fieldName: field,
      fieldValue,
      config: effectiveConfig,
      schedulerData,
      stagedChanges,
    });

    issues.push(...runFieldValidation(context));
  });

  return {
    valid: issues.every((issue) => issue.severity !== 'error'),
    issues: dedupeIssues(issues),
  };
}

function buildRuleContext(params: {
  queuePath: string;
  fieldName: string;
  fieldValue: unknown;
  config: Map<string, string>;
  schedulerData?: SchedulerInfo | null;
  stagedChanges: StagedChange[];
}): RuleContext {
  const { queuePath, fieldName, fieldValue, config, schedulerData, stagedChanges } = params;
  const legacyModeEnabled = config.get(SPECIAL_VALUES.LEGACY_MODE_PROPERTY) !== 'false';

  return {
    queuePath,
    fieldName,
    fieldValue,
    config,
    schedulerData,
    stagedChanges,
    legacyModeEnabled,
  };
}

export function hasBlockingIssues(issues: ValidationIssue[]): boolean {
  return issues.some((issue) => issue.severity === 'error');
}

export function splitIssues(issues: ValidationIssue[]): {
  errors: ValidationIssue[];
  warnings: ValidationIssue[];
} {
  const errors: ValidationIssue[] = [];
  const warnings: ValidationIssue[] = [];

  issues.forEach((issue) => {
    if (issue.severity === 'error') {
      errors.push(issue);
    } else {
      warnings.push(issue);
    }
  });

  return { errors, warnings };
}
