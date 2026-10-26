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


import { createContext, use, useState, type PropsWithChildren } from 'react';
import type { JSX } from 'react';
import { SPECIAL_VALUES } from '~/types/constants/special-values';
import { mergeStagedConfig, applyFieldPreview } from '~/utils/configUtils';
import { useSchedulerStore } from '~/stores/schedulerStore';
import {
  runFieldValidation,
  type ValidationContext as RuleContext,
} from '~/config/validation-rules';
import type { ValidationIssue } from '~/types';

type FieldIssues = ValidationIssue[];
type QueueIssues = Record<string, FieldIssues>;
type ValidationState = Record<string, QueueIssues>;

interface PendingFieldValue {
  queuePath?: string;
  fieldName: string;
  value: unknown;
}

interface ValidationContextValue {
  errors: ValidationState;
  validateField: (
    queuePath: string,
    fieldName: string,
    fieldValue: unknown,
    options?: { pendingValues?: PendingFieldValue[] },
  ) => ValidationIssue[];
  replaceQueueIssues: (queuePath: string, issues: ValidationIssue[]) => void;
  clearFieldErrors: (queuePath: string, fieldName: string) => void;
  clearQueueErrors: (queuePath: string) => void;
  clearAllErrors: () => void;
}

const ValidationContext = createContext<ValidationContextValue | null>(null);

export const ValidationProvider = ({ children }: PropsWithChildren): JSX.Element => {
  const [errors, setErrors] = useState<ValidationState>({});
  const schedulerData = useSchedulerStore((state) => state.schedulerData);
  const configData = useSchedulerStore((state) => state.configData);
  const stagedChanges = useSchedulerStore((state) => state.stagedChanges);

  const baseMergedConfig = mergeStagedConfig(configData, stagedChanges);

  const updateIssuesState = (
    issues: ValidationIssue[],
    impactedKeys: Array<{ queuePath: string; fieldName: string }>,
  ) => {
    setErrors((prev) => {
      const next: ValidationState = { ...prev };

      impactedKeys.forEach(({ queuePath, fieldName }) => {
        if (!next[queuePath]) {
          next[queuePath] = {};
        } else {
          next[queuePath] = { ...next[queuePath] };
        }
        // Clear existing issues for impacted keys before repopulating
        delete next[queuePath][fieldName];
      });

      issues.forEach((issue) => {
        if (!next[issue.queuePath]) {
          next[issue.queuePath] = {};
        }

        const queueIssues = (next[issue.queuePath] = { ...next[issue.queuePath] });
        const fieldIssues = queueIssues[issue.field] ? [...queueIssues[issue.field]] : [];

        const duplicate = fieldIssues.some(
          (existing) => existing.rule === issue.rule && existing.message === issue.message,
        );

        if (!duplicate) {
          fieldIssues.push(issue);
        }

        queueIssues[issue.field] = fieldIssues;
      });

      Object.entries(next).forEach(([queuePath, fields]) => {
        if (Object.keys(fields).length === 0) {
          delete next[queuePath];
        }
      });

      return next;
    });
  };

  const replaceQueueIssues = (queuePath: string, issues: ValidationIssue[]) => {
    setErrors((prev) => {
      const next: ValidationState = { ...prev };

      const affectedQueuePaths = new Set<string>([queuePath]);
      issues.forEach((issue) => {
        affectedQueuePaths.add(issue.queuePath);
      });

      affectedQueuePaths.forEach((qp) => {
        delete next[qp];
      });

      issues.forEach((issue) => {
        const queueIssues = next[issue.queuePath] ? { ...next[issue.queuePath] } : {};
        const fieldIssues = queueIssues[issue.field] ? [...queueIssues[issue.field]] : [];
        fieldIssues.push(issue);
        queueIssues[issue.field] = fieldIssues;
        next[issue.queuePath] = queueIssues;
      });

      return next;
    });
  };

  const validateField = (
    queuePath: string,
    fieldName: string,
    fieldValue: unknown,
    options?: { pendingValues?: PendingFieldValue[] },
  ) => {
    let effectiveConfig = applyFieldPreview(baseMergedConfig, queuePath, fieldName, fieldValue);

    if (options?.pendingValues?.length) {
      options.pendingValues.forEach(
        ({ queuePath: overrideQueuePath, fieldName: overrideField, value }) => {
          const targetQueuePath = overrideQueuePath ?? queuePath;
          if (targetQueuePath === queuePath && overrideField === fieldName) {
            return;
          }
          effectiveConfig = applyFieldPreview(
            effectiveConfig,
            targetQueuePath,
            overrideField,
            value,
          );
        },
      );
    }

    const legacyValue = effectiveConfig.get(SPECIAL_VALUES.LEGACY_MODE_PROPERTY);

    const ruleContext: RuleContext = {
      queuePath,
      fieldName,
      fieldValue,
      config: effectiveConfig,
      schedulerData,
      stagedChanges,
      legacyModeEnabled: legacyValue !== 'false',
    };

    const issues = runFieldValidation(ruleContext);

    const impactedKeys: Array<{ queuePath: string; fieldName: string }> = [];
    const addImpact = (qp: string, fn: string) => {
      if (!impactedKeys.some((entry) => entry.queuePath === qp && entry.fieldName === fn)) {
        impactedKeys.push({ queuePath: qp, fieldName: fn });
      }
    };

    addImpact(queuePath, fieldName);
    issues.forEach((issue) => addImpact(issue.queuePath, issue.field));

    updateIssuesState(issues, impactedKeys);

    return issues;
  };

  const clearFieldErrors = (queuePath: string, fieldName: string) => {
    setErrors((prev) => {
      if (!prev[queuePath]?.[fieldName]) {
        return prev;
      }

      const next = { ...prev };
      const queueIssues = { ...(next[queuePath] ?? {}) };
      delete queueIssues[fieldName];

      if (Object.keys(queueIssues).length === 0) {
        delete next[queuePath];
      } else {
        next[queuePath] = queueIssues;
      }

      return next;
    });
  };

  const clearQueueErrors = (queuePath: string) => {
    setErrors((prev) => {
      if (!prev[queuePath]) {
        return prev;
      }

      const next = { ...prev };
      delete next[queuePath];
      return next;
    });
  };

  const clearAllErrors = () => {
    setErrors({});
  };

  const value: ValidationContextValue = {
    errors,
    validateField,
    replaceQueueIssues,
    clearFieldErrors,
    clearQueueErrors,
    clearAllErrors,
  };

  return <ValidationContext value={value}>{children}</ValidationContext>;
};

// eslint-disable-next-line react-refresh/only-export-components
export const useValidation = (): ValidationContextValue => {
  const context = use(ValidationContext);
  if (!context) {
    throw new Error('useValidation must be used within a ValidationProvider');
  }
  return context;
};
