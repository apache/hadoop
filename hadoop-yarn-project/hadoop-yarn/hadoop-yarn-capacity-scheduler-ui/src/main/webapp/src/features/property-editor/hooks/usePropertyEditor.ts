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


import { useForm, useWatch } from 'react-hook-form';
import { zodResolver } from '@hookform/resolvers/zod';
import { z } from 'zod';
import { useEffect, useMemo, useRef } from 'react';
import { useSchedulerStore } from '~/stores/schedulerStore';
import type { PropertyDescriptor, ValidationRule } from '~/types/property-descriptor';
import { queuePropertyDefinitions } from '~/config/properties/queue-properties';
import { toast } from 'sonner';
import { useValidation } from '~/contexts/ValidationContext';
import { validateQueue, hasBlockingIssues, splitIssues } from '~/features/validation/service';
import type { ValidationIssue } from '~/types';
import { isBlockingError } from '~/features/validation/ruleCategories';
import type { FieldErrors } from 'react-hook-form';

type CombinedError = { type: string; message: string };

function mergeFormAndValidationErrors(
  zodErrors: FieldErrors,
  queueIssues: Record<string, ValidationIssue[]>,
  normalizeFieldName: (field: string) => string,
): Record<string, CombinedError> {
  const combined: Record<string, CombinedError> = {};

  // Add Zod errors
  Object.entries(zodErrors).forEach(([field, error]) => {
    if (!error) return;
    const normalizedField = normalizeFieldName(field);
    combined[normalizedField] = {
      type: typeof error.type === 'string' ? error.type : 'validation',
      message: typeof error.message === 'string' ? error.message : 'Validation error',
    };
  });

  // Merge validation errors
  Object.entries(queueIssues).forEach(([field, issues]) => {
    const { errors } = splitIssues(issues);
    if (errors.length === 0) return;

    const errorMessage = errors.map((e) => e.message).join('. ');
    const existing = combined[field];

    if (existing) {
      combined[field] = {
        ...existing,
        message: existing.message ? `${existing.message}. ${errorMessage}` : errorMessage,
      };
    } else {
      combined[field] = { type: 'business', message: errorMessage };
    }
  });

  return combined;
}
import { validatePropertyChange } from '~/features/validation/crossQueue';
import { buildPropertyKey } from '~/utils/propertyUtils';
import { CONFIG_PREFIXES } from '~/types';
import { resolveInheritedValue, type InheritedValueInfo } from '~/utils/resolveInheritedValue';

function createFormSchema(
  properties: Array<
    PropertyDescriptor & {
      formFieldName?: string;
      originalName?: string;
    }
  >,
) {
  const schemaFields: Record<string, z.ZodType> = {};

  properties.forEach((property) => {
    let fieldSchema: z.ZodType = z.string();

    if (property.validationRules) {
      property.validationRules.forEach((rule: ValidationRule) => {
        switch (rule.type) {
          case 'range':
            if (property.type === 'number') {
              fieldSchema = z.string().refine(
                (value) => {
                  if (!value.trim()) return !property.required;
                  const num = parseFloat(value);
                  return (
                    !isNaN(num) &&
                    (rule.min === undefined || num >= rule.min) &&
                    (rule.max === undefined || num <= rule.max)
                  );
                },
                { message: rule.message },
              );
            }
            break;
          case 'pattern':
            if (rule.pattern) {
              fieldSchema = z.string().regex(new RegExp(rule.pattern), rule.message);
            }
            break;
          case 'custom':
            if (rule.validator) {
              fieldSchema = z.string().refine(rule.validator, { message: rule.message });
            }
            break;
        }
      });
    }

    if (!property.required) {
      fieldSchema = fieldSchema.optional().or(z.literal(''));
    }

    // Use escaped field name for React Hook Form to prevent dot notation conflicts
    const fieldName = property.formFieldName || property.name;
    schemaFields[fieldName] = fieldSchema;
  });

  return z.object(schemaFields);
}

interface UsePropertyEditorOptions {
  queuePath: string;
  properties?: PropertyDescriptor[];
}

export function usePropertyEditor({
  queuePath,
  properties = queuePropertyDefinitions,
}: UsePropertyEditorOptions) {
  const { getQueuePropertyValue, stageQueueChange, clearQueueChanges, schedulerData, configData } =
    useSchedulerStore();

  const stagedChanges = useSchedulerStore((state) => state.stagedChanges);
  const cleanResetRef = useRef(false);
  const previousQueuePathRef = useRef<string | null>(null);

  useEffect(() => {
    if (previousQueuePathRef.current !== queuePath) {
      cleanResetRef.current = true;
      previousQueuePathRef.current = queuePath;
    }
  }, [queuePath]);

  // Escape dot notation in property names to prevent React Hook Form from treating them as nested paths
  // Note: This must be memoized because it's used as a dependency in useEffect below.
  // Without memoization, it creates a new array on every render, causing infinite loops.
  const allProperties = useMemo(
    () =>
      properties.map((property) => ({
        ...property,
        formFieldName: property.formFieldName ?? property.name.replace(/\./g, '__DOT__'),
        originalName: property.originalName ?? property.name,
      })),
    [properties],
  );

  const knownFieldNames = useMemo(
    () => new Set(allProperties.map((property) => property.originalName || property.name)),
    [allProperties],
  );

  const formSchema = useMemo(() => createFormSchema(allProperties), [allProperties]);

  const {
    errors: validationState,
    validateField: runContextValidation,
    replaceQueueIssues,
    clearQueueErrors,
  } = useValidation();

  const form = useForm({
    resolver: zodResolver(formSchema),
    defaultValues: {},
    mode: 'onBlur', // Changed from 'onChange' for better performance
    criteriaMode: 'all', // Show all validation errors
  });

  const { control, handleSubmit, reset, getValues } = form;

  const watchedValues = useWatch({ control });

  const normalizeFieldName = (field: string): string => {
    const property = allProperties.find(
      (p) => p.formFieldName === field || p.originalName === field || p.name === field,
    );

    if (property) {
      return property.originalName || property.name;
    }

    return field.replace(/__DOT__/g, '.');
  };

  const getFieldIssues = (field: string): ValidationIssue[] => {
    const normalized = normalizeFieldName(field);
    const queueIssues = validationState[queuePath];
    return queueIssues?.[normalized] ?? [];
  };

  const getFieldErrors = (field: string): string[] => {
    return getFieldIssues(field)
      .filter((issue) => issue.severity === 'error')
      .map((issue) => issue.message);
  };

  const getFieldWarnings = (field: string): string[] => {
    return getFieldIssues(field)
      .filter((issue) => issue.severity === 'warning')
      .map((issue) => issue.message);
  };

  useEffect(() => {
    const initialValues: Record<string, string> = {};

    allProperties.forEach((property) => {
      const { value } = getQueuePropertyValue(queuePath, property.originalName || property.name);

      // Use escaped field name for React Hook Form
      const fieldName = property.formFieldName || property.name;
      initialValues[fieldName] = value;
    });

    const shouldForceClean = cleanResetRef.current;
    reset(initialValues, {
      keepDirty: !shouldForceClean,
      keepDirtyValues: !shouldForceClean,
    });
    if (shouldForceClean) {
      cleanResetRef.current = false;
    }
  }, [queuePath, allProperties, getQueuePropertyValue, reset, stagedChanges]);

  const getStagedStatus = (propertyName: string): 'new' | 'modified' | 'deleted' | undefined => {
    const { isStaged } = getQueuePropertyValue(queuePath, propertyName);
    return isStaged ? 'modified' : undefined;
  };

  const getInheritanceInfo = (propertyName: string): InheritedValueInfo | null => {
    const property = allProperties.find(
      (p) => p.originalName === propertyName || p.name === propertyName,
    );
    if (!property?.inheritanceResolver) {
      return null;
    }

    return resolveInheritedValue({
      queuePath,
      propertyName,
      configData,
      stagedChanges,
      inheritanceResolver: property.inheritanceResolver,
    });
  };

  const stageChange = (
    propertyName: string,
    value: string,
    validationErrors?: ValidationIssue[],
  ) => {
    stageQueueChange(queuePath, propertyName, value, validationErrors);
  };

  const collectTemplateMatches = <T>(
    fromConfig: (key: string) => T | null | undefined,
    fromChange: (change: (typeof stagedChanges)[number]) => T | null | undefined,
  ): T[] => {
    const matches = new Set<T>();

    for (const key of configData.keys()) {
      const match = fromConfig(key);
      if (match != null) {
        matches.add(match);
      }
    }

    stagedChanges.forEach((change) => {
      const match = fromChange(change);
      if (match != null) {
        matches.add(match);
      }
    });

    return Array.from(matches);
  };

  const collectTemplateProperties = (templateQueuePath: string): string[] => {
    const prefix = `${CONFIG_PREFIXES.BASE}.${templateQueuePath}.`;

    return collectTemplateMatches(
      (key) => {
        if (!key.startsWith(prefix)) {
          return null;
        }
        const propertyName = key.slice(prefix.length);
        return propertyName || null;
      },
      (change) => {
        if (change.queuePath === templateQueuePath && change.property) {
          return change.property;
        }
        return null;
      },
    );
  };

  const collectTemplateQueuePaths = (suffix: string): string[] => {
    const configPrefix = `${CONFIG_PREFIXES.BASE}.`;
    const basePrefix = `${queuePath}.`;
    const suffixToken = `.${suffix}`;

    const matches = collectTemplateMatches(
      (key) => {
        if (!key.startsWith(configPrefix)) {
          return null;
        }
        const remainder = key.slice(configPrefix.length);
        if (!remainder.startsWith(basePrefix)) {
          return null;
        }
        const index = remainder.indexOf(suffixToken);
        if (index === -1) {
          return null;
        }
        return remainder.slice(0, index + suffix.length);
      },
      (change) => {
        const changePath = change.queuePath;
        if (!changePath) {
          return null;
        }
        if (
          changePath === `${queuePath}.${suffix}` ||
          (changePath.startsWith(basePrefix) && changePath.includes(suffixToken))
        ) {
          return changePath;
        }
        return null;
      },
    );

    const defaultPath = `${queuePath}.${suffix}`;
    if (!matches.includes(defaultPath)) {
      matches.push(defaultPath);
    }

    return Array.from(new Set(matches));
  };

  const stageTemplatePropertyRemovals = (templateQueuePaths: string[]): number => {
    let removals = 0;

    templateQueuePaths.forEach((templatePath) => {
      const properties = collectTemplateProperties(templatePath);
      properties.forEach((property) => {
        stageQueueChange(templatePath, property, '');
        removals += 1;
      });
    });

    return removals;
  };

  const handleFieldBlur = (
    propertyName: string,
    value: string,
    options?: {
      validationOverrides?: Array<{ queuePath: string; field: string; value: string }>;
    },
  ) => {
    const normalizedName = normalizeFieldName(propertyName);
    if (!knownFieldNames.has(normalizedName)) {
      return;
    }
    const normalizedValue = typeof value === 'string' ? value : value == null ? '' : String(value);

    const pendingValues: Array<{ queuePath?: string; fieldName: string; value: unknown }> = [];

    const collectOverrides = (dirtyEntry: unknown, fieldKey: string) => {
      if (!dirtyEntry) {
        return;
      }

      if (dirtyEntry === true) {
        const normalizedField = normalizeFieldName(fieldKey);
        if (!knownFieldNames.has(normalizedField)) {
          return;
        }
        if (normalizedField === normalizedName) {
          return;
        }
        const currentValue = getValues(fieldKey);
        const currentValueAsString =
          typeof currentValue === 'string'
            ? currentValue
            : currentValue == null
              ? ''
              : String(currentValue);
        pendingValues.push({
          queuePath,
          fieldName: normalizedField,
          value: currentValueAsString,
        });
        return;
      }

      if (typeof dirtyEntry === 'object') {
        Object.entries(dirtyEntry as Record<string, unknown>).forEach(([childKey, childValue]) => {
          const nextKey = fieldKey ? `${fieldKey}.${childKey}` : childKey;
          collectOverrides(childValue, nextKey);
        });
      }
    };

    Object.entries(form.formState.dirtyFields).forEach(([fieldKey, dirtyEntry]) => {
      collectOverrides(dirtyEntry, fieldKey);
    });

    options?.validationOverrides?.forEach(({ queuePath: overrideQueuePath, field, value }) => {
      pendingValues.push({
        queuePath: overrideQueuePath,
        fieldName: field,
        value,
      });
    });

    runContextValidation(queuePath, normalizedName, normalizedValue, {
      pendingValues,
    });
  };

  const onSubmit = async (data: Record<string, string>) => {
    try {
      const fieldNameMapping: Record<string, string> = {};
      const changedData: Record<string, string> = {};

      allProperties.forEach((property) => {
        const escapedName = property.formFieldName || property.name;
        const originalName = property.originalName || property.name;
        fieldNameMapping[escapedName] = originalName;
      });

      Object.entries(form.formState.dirtyFields).forEach(([escapedFieldName, isDirty]) => {
        if (isDirty && typeof data[escapedFieldName] === 'string') {
          const originalName = fieldNameMapping[escapedFieldName] || escapedFieldName;
          changedData[originalName] = data[escapedFieldName];
        }
      });

      const pendingEntries = Object.entries(changedData);

      const previewConfigData = new Map(configData);
      pendingEntries.forEach(([propertyName, value]) => {
        const propertyKey = buildPropertyKey(queuePath, propertyName);
        if (!value.trim()) {
          previewConfigData.delete(propertyKey);
        } else {
          previewConfigData.set(propertyKey, value);
        }
      });

      const queueValidation = validateQueue({
        queuePath,
        properties: changedData,
        configData,
        stagedChanges,
        schedulerData,
      });

      replaceQueueIssues(queuePath, queueValidation.issues);

      const blockingIssues = queueValidation.issues.filter((issue) =>
        isBlockingError(issue.rule, issue.severity),
      );

      const nonBlockingIssues = queueValidation.issues.filter(
        (issue) => !isBlockingError(issue.rule, issue.severity),
      );

      if (blockingIssues.length > 0) {
        toast.error(`Cannot stage changes: ${blockingIssues[0].message}`);
        return { success: false, message: blockingIssues[0].message };
      }

      let stagedCount = 0;
      let flexibleTemplatesDisabled = false;

      pendingEntries.forEach(([propertyName, value]) => {
        const fieldIssues = nonBlockingIssues.filter((issue) => issue.field === propertyName);

        const crossQueueIssues = validatePropertyChange({
          propertyName,
          propertyValue: value,
          queuePath,
          schedulerData,
          configData: previewConfigData,
          stagedChanges,
          includeBlockingErrors: false,
        });

        const allIssues = [...fieldIssues, ...crossQueueIssues];

        const uniqueIssues = allIssues.filter(
          (issue, index, self) =>
            index ===
            self.findIndex(
              (candidate) =>
                candidate.queuePath === issue.queuePath &&
                candidate.field === issue.field &&
                candidate.message === issue.message &&
                candidate.severity === issue.severity,
            ),
        );

        stageChange(propertyName, value, uniqueIssues.length > 0 ? uniqueIssues : undefined);
        stagedCount += 1;

        if (propertyName === 'auto-queue-creation-v2.enabled' && value !== 'true') {
          flexibleTemplatesDisabled = true;
        }
      });

      if (flexibleTemplatesDisabled) {
        const flexiblePaths = new Set<string>([
          ...collectTemplateQueuePaths('auto-queue-creation-v2.template'),
          ...collectTemplateQueuePaths('auto-queue-creation-v2.parent-template'),
          ...collectTemplateQueuePaths('auto-queue-creation-v2.leaf-template'),
        ]);
        if (flexiblePaths.size > 0) {
          stagedCount += stageTemplatePropertyRemovals(Array.from(flexiblePaths));
        }
      }

      const result = {
        success: true,
        message: `${stagedCount} change${stagedCount !== 1 ? 's' : ''} staged successfully!`,
      };

      if (stagedCount > 0) {
        const latestValues = form.getValues();
        reset(latestValues, {
          keepDirty: false,
          keepDirtyValues: false,
        });
        cleanResetRef.current = true;
      }

      if (nonBlockingIssues.length > 0) {
        toast.warning(
          `${result.message} (with ${nonBlockingIssues.length} validation warning${nonBlockingIssues.length !== 1 ? 's' : ''})`,
        );
      } else {
        toast.success(result.message);
      }

      return result;
    } catch (error) {
      const errorMessage = error instanceof Error ? error.message : 'Failed to stage changes';
      toast.error(errorMessage);
      throw error;
    }
  };

  const handleReset = () => {
    // Clear only the changes for the current queue
    clearQueueChanges(queuePath);
    clearQueueErrors(queuePath);

    // Reset form to original values
    const currentValues: Record<string, string> = {};
    allProperties.forEach((property) => {
      const { value } = getQueuePropertyValue(queuePath, property.originalName || property.name);
      const fieldName = property.formFieldName || property.name;
      currentValues[fieldName] = value;
    });
    cleanResetRef.current = true;
    reset(currentValues);
  };

  const hasChangesCheck = !Array.isArray(stagedChanges)
    ? false
    : stagedChanges.filter((c) => c.queuePath === queuePath).length > 0;
  const hasChanges = hasChangesCheck;

  const propertiesByCategoryTemp: Record<string, PropertyDescriptor[]> = {};

  allProperties.forEach((property) => {
    if (!propertiesByCategoryTemp[property.category]) {
      propertiesByCategoryTemp[property.category] = [];
    }
    propertiesByCategoryTemp[property.category].push(property);
  });

  const propertiesByCategory = propertiesByCategoryTemp;

  // Get combined errors and validity state
  const zodErrors = form.formState.errors;
  const queueIssues = validationState[queuePath] ?? {};
  const combinedErrors = mergeFormAndValidationErrors(zodErrors, queueIssues, normalizeFieldName);

  const hasZodErrors = !form.formState.isValid;
  const hasValidationErrors = Object.values(queueIssues).some(hasBlockingIssues);
  const isFormValid = !hasZodErrors && !hasValidationErrors;

  return {
    form,
    control,
    handleSubmit: handleSubmit(onSubmit),
    handleReset,
    handleFieldBlur,
    stageChange,
    errors: combinedErrors,
    isValid: isFormValid,

    hasChanges,
    watchedValues,
    propertiesByCategory,

    getStagedStatus,
    getInheritanceInfo,

    properties: allProperties,
    formState: form.formState,

    getFieldErrors,
    getFieldWarnings,
  };
}
