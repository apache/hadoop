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


import React, { useState, useImperativeHandle, useMemo, useCallback } from 'react';
import { Badge } from '~/components/ui/badge';
import { Button } from '~/components/ui/button';
import {
  Accordion,
  AccordionContent,
  AccordionItem,
  AccordionTrigger,
} from '~/components/ui/accordion';
import { usePropertyEditor } from '~/features/property-editor/hooks/usePropertyEditor';
import { PropertyFormField } from './PropertyFormField';
import type { QueueInfo } from '~/types';
import type { PropertyCategory } from '~/types';
import { toast } from 'sonner';
import { Form } from '~/components/ui/form';
import { useSchedulerStore } from '~/stores/schedulerStore';
import { shouldShowProperty, isPropertyEnabled } from '~/utils/propertyConditions';
import { globalPropertyDefinitions } from '~/config/properties/global-properties';
import {
  queueCategoryOrder,
  categoryConfig,
} from '~/features/property-editor/constants/categoryConfig';

export interface PropertyEditorTabHandle {
  submit: () => Promise<void>;
  reset: () => void;
  isValid: () => boolean;
  getErrors: () => Record<string, unknown>;
}

interface PropertyEditorTabProps {
  queue: QueueInfo;
  ref?: React.Ref<PropertyEditorTabHandle>;
  onHasChangesChange?: (hasChanges: boolean) => void;
  onIsSubmittingChange?: (isSubmitting: boolean) => void;
  onFormDirtyChange?: (isDirty: boolean) => void;
  templateConfigControls?: {
    canManageTemplates: boolean;
    legacyAvailable: boolean;
    flexibleAvailable: boolean;
    onOpenTemplateConfig: () => void;
  };
}

export const PropertyEditorTab = ({
  queue,
  ref,
  onHasChangesChange,
  onIsSubmittingChange,
  onFormDirtyChange,
  templateConfigControls,
}: PropertyEditorTabProps) => {
  const [isSubmitting, setIsSubmitting] = useState(false);

  // Get default expanded categories from categoryConfig
  const defaultExpandedCategories = useMemo(() => {
    return queueCategoryOrder.filter(
      (category) => categoryConfig[category]?.defaultExpanded === true,
    );
  }, []);

  const [expandedAccordions, setExpandedAccordions] = useState<string[]>(defaultExpandedCategories);

  const getGlobalPropertyValue = useSchedulerStore((state) => state.getGlobalPropertyValue);
  const getQueuePropertyValue = useSchedulerStore((state) => state.getQueuePropertyValue);
  const stagedChanges = useSchedulerStore((state) => state.stagedChanges);
  const configData = useSchedulerStore((state) => state.configData);
  const schedulerInfo = useSchedulerStore((state) => state.schedulerData);
  const hasPendingDeletion = useSchedulerStore((state) => state.hasPendingDeletion);

  const isPendingDeletion = hasPendingDeletion(queue.queuePath);

  const {
    form,
    control,
    handleSubmit,
    handleReset,
    errors,
    isValid,
    hasChanges,
    watchedValues,
    propertiesByCategory,
    getStagedStatus,
    formState,
    handleFieldBlur,
    getFieldErrors,
    getFieldWarnings,
    properties,
    getInheritanceInfo,
  } = usePropertyEditor({
    queuePath: queue.queuePath,
  });

  // Check if form is still initializing
  const isFormInitializing =
    !control || !propertiesByCategory || Object.keys(propertiesByCategory).length === 0;

  const parts = queue.queuePath.split('.');
  const parentQueuePath = parts.length <= 1 ? undefined : parts.slice(0, -1).join('.');

  // Notify parent about hasChanges state
  React.useEffect(() => {
    onHasChangesChange?.(hasChanges);
  }, [hasChanges, onHasChangesChange]);

  // Notify parent about submission state
  React.useEffect(() => {
    onIsSubmittingChange?.(isSubmitting);
  }, [isSubmitting, onIsSubmittingChange]);

  // Notify parent about form dirty state
  React.useEffect(() => {
    onFormDirtyChange?.(formState.isDirty);
  }, [formState.isDirty, onFormDirtyChange]);

  // Handle form submission (staging)
  const onSubmit = useCallback(async () => {
    setIsSubmitting(true);
    try {
      await handleSubmit();
      // Toast is now handled in the actual onSubmit callback
    } catch (error) {
      // Error toast is also handled in the callback
      console.error('Form submission error:', error);
    } finally {
      setIsSubmitting(false);
    }
  }, [handleSubmit]);

  // Handle form reset
  const onReset = useCallback(() => {
    handleReset();
    toast.success('Form reset to current values');
  }, [handleReset]);

  // Expose handlers to parent via ref
  useImperativeHandle(
    ref,
    () => ({
      submit: onSubmit,
      reset: onReset,
      isValid: () => isValid,
      getErrors: () => errors,
    }),
    [onSubmit, onReset, isValid, errors],
  );

  const categoryOrder: PropertyCategory[] = [...queueCategoryOrder];

  const queueValuesTemp: Record<string, string> = {};
  const watchedRecord = (watchedValues ?? {}) as Record<string, unknown>;

  properties.forEach((property) => {
    const fieldName = property.formFieldName || property.name;
    const rawValue = watchedRecord[fieldName];
    let normalized = '';
    if (typeof rawValue === 'string') {
      normalized = rawValue;
    } else if (rawValue != null) {
      normalized = String(rawValue);
    } else if (property.defaultValue) {
      normalized = property.defaultValue;
    }
    queueValuesTemp[property.originalName || property.name] = normalized;
  });

  const queueValues = queueValuesTemp;

  const globalValuesTemp: Record<string, string> = {};
  globalPropertyDefinitions.forEach((property) => {
    const { value } = getGlobalPropertyValue(property.name);
    globalValuesTemp[property.name] = value;
  });
  const globalValues = globalValuesTemp;

  const queueValueCache = new Map<string, string | undefined>();
  const globalValueCache = new Map<string, string | undefined>();

  const getQueueValue = (targetQueuePath: string, name: string) => {
    if (!targetQueuePath) return undefined;

    if (targetQueuePath === queue.queuePath) {
      return queueValues[name];
    }

    const cacheKey = `${targetQueuePath}::${name}`;
    if (!queueValueCache.has(cacheKey)) {
      const { value } = getQueuePropertyValue(targetQueuePath, name);
      queueValueCache.set(cacheKey, value);
    }
    return queueValueCache.get(cacheKey);
  };

  const getValue = (name: string) => {
    if (name in queueValues) {
      return queueValues[name];
    }
    return getQueueValue(queue.queuePath, name);
  };

  const getGlobalValue = (name: string) => {
    if (name in globalValues) {
      return globalValues[name];
    }
    if (!globalValueCache.has(name)) {
      const { value } = getGlobalPropertyValue(name);
      globalValueCache.set(name, value);
    }
    return globalValueCache.get(name);
  };

  const conditionBase = {
    scope: 'queue' as const,
    values: queueValues,
    globalValues,
    queuePath: queue.queuePath,
    queueInfo: queue,
    schedulerInfo,
    stagedChanges,
    configData,
    getValue,
    getGlobalValue,
    getQueueValue,
    getConfigValue: (key: string) => configData.get(key),
  };

  const propertyStates = new Map<
    string,
    {
      visible: boolean;
      enabled: boolean;
    }
  >();

  properties.forEach((property) => {
    const propertyName = property.originalName || property.name;
    const propertyValue = conditionBase.getValue(propertyName) ?? '';
    const options = {
      ...conditionBase,
      property,
      propertyValue,
    };
    const visible = shouldShowProperty(property, options);
    const enabled = visible ? isPropertyEnabled(property, options) : false;

    propertyStates.set(propertyName, { visible, enabled });
  });

  const visiblePropertiesByCategory: Partial<Record<PropertyCategory, typeof properties>> = {};

  Object.entries(propertiesByCategory).forEach(([categoryKey, props]) => {
    const typedCategory = categoryKey as PropertyCategory;
    const filtered = props.filter((property) => {
      const propertyName = property.originalName || property.name;
      return propertyStates.get(propertyName)?.visible ?? true;
    }) as typeof properties;

    if (filtered.length === 0) {
      return;
    }

    visiblePropertiesByCategory[typedCategory] = filtered;
  });

  // Memoize fieldCategoryMap to prevent infinite loops in error categorization
  // Build from all properties (not just visible) so errors can be mapped even for hidden fields
  const fieldCategoryMap = useMemo(() => {
    const categoryMap = new Map<string, Set<PropertyCategory>>();

    Object.entries(propertiesByCategory).forEach(([categoryKey, props]) => {
      const typedCategory = categoryKey as PropertyCategory;

      props.forEach((property) => {
        const keys = new Set<string>([property.originalName || property.name, property.name]);

        keys.forEach((key) => {
          if (!categoryMap.has(key)) {
            categoryMap.set(key, new Set<PropertyCategory>());
          }
          categoryMap.get(key)!.add(typedCategory);
        });
      });
    });

    return categoryMap;
  }, [propertiesByCategory]);

  const availableCategories = categoryOrder.filter(
    (category) => (visiblePropertiesByCategory[category]?.length ?? 0) > 0,
  );

  // Memoize error info and categoriesWithErrors to prevent infinite loops in useEffect
  const categoriesWithErrors = useMemo(() => {
    const categoryErrorInfoCounts: Partial<Record<PropertyCategory, number>> = {};

    if (errors && Object.keys(errors).length > 0) {
      Object.keys(errors).forEach((fieldName) => {
        const categories = fieldCategoryMap.get(fieldName);
        if (!categories) {
          return;
        }
        categories.forEach((category) => {
          categoryErrorInfoCounts[category] = (categoryErrorInfoCounts[category] ?? 0) + 1;
        });
      });
    }

    return new Set(
      (Object.entries(categoryErrorInfoCounts) as Array<[PropertyCategory, number]>)
        .filter(([, count]) => count > 0)
        .map(([category]) => category),
    );
  }, [errors, fieldCategoryMap]);

  // Compute categoryErrorInfo for rendering (error counts per category)
  const categoryErrorInfo = useMemo(() => {
    const counts: Partial<Record<PropertyCategory, number>> = {};
    if (errors && Object.keys(errors).length > 0) {
      Object.keys(errors).forEach((fieldName) => {
        const categories = fieldCategoryMap.get(fieldName);
        if (!categories) {
          return;
        }
        categories.forEach((category) => {
          counts[category] = (counts[category] ?? 0) + 1;
        });
      });
    }
    return counts;
  }, [errors, fieldCategoryMap]);

  // Auto-expand categories with errors
  React.useEffect(() => {
    if (categoriesWithErrors.size > 0) {
      setExpandedAccordions((prev) => {
        const newExpanded = new Set(prev);
        categoriesWithErrors.forEach((cat) => newExpanded.add(cat));
        return Array.from(newExpanded);
      });
    }
  }, [categoriesWithErrors]);

  return (
    <Form {...form}>
      <div className="flex flex-col h-full">
        {/* Loading State */}
        {isFormInitializing && (
          <div className="flex justify-center items-center min-h-[200px] p-4">
            <div className="h-8 w-8 animate-spin rounded-full border-2 border-primary border-t-transparent" />
          </div>
        )}

        {/* Property Categories */}
        {!isFormInitializing && (
          <Accordion
            type="multiple"
            value={expandedAccordions}
            onValueChange={setExpandedAccordions}
            className="p-4 pb-20"
          >
            {availableCategories.map((category) => {
              const categoryProps = visiblePropertiesByCategory[category] ?? [];
              const config = categoryConfig[category];
              const hasErrors = categoriesWithErrors.has(category);
              const errorCount = categoryErrorInfo[category] ?? 0;

              return (
                <AccordionItem key={category} value={category} className="border rounded-lg mb-2">
                  <AccordionTrigger className="px-4 py-3 hover:no-underline">
                    <div className="flex items-center gap-3 flex-1">
                      {config.icon}
                      <div className="text-left flex-1">
                        <div className="text-sm font-medium">{config.label}</div>
                        <div className="text-xs text-muted-foreground">{config.description}</div>
                      </div>
                      {hasErrors && (
                        <Badge variant="destructive" className="text-xs px-1.5 py-0">
                          {errorCount}
                        </Badge>
                      )}
                    </div>
                  </AccordionTrigger>
                  <AccordionContent className="p-4">
                    <div className="space-y-3">
                      {categoryProps.map((prop) => {
                        const propertyKey = prop.originalName || prop.name;
                        const propertyState = propertyStates.get(propertyKey);
                        if (propertyState && !propertyState.visible) {
                          return null;
                        }
                        const supportsLegacyButton =
                          propertyKey === 'auto-create-child-queue.enabled';
                        const supportsFlexibleButton =
                          propertyKey === 'auto-queue-creation-v2.enabled';
                        const shouldRenderTemplateButton =
                          Boolean(templateConfigControls?.canManageTemplates) &&
                          ((supportsLegacyButton && templateConfigControls?.legacyAvailable) ||
                            (supportsFlexibleButton && templateConfigControls?.flexibleAvailable));

                        return (
                          <div key={prop.name} className="space-y-2">
                            <PropertyFormField
                              property={prop}
                              control={control}
                              stagedStatus={getStagedStatus(prop.originalName || prop.name)}
                              isEnabled={(propertyState?.enabled ?? true) && !isPendingDeletion}
                              onBlur={handleFieldBlur}
                              errors={getFieldErrors(prop.formFieldName || prop.name)}
                              warnings={getFieldWarnings(prop.formFieldName || prop.name)}
                              queuePath={queue.queuePath}
                              queueName={queue.queueName}
                              parentQueuePath={parentQueuePath}
                              currentValues={watchedValues}
                              inheritanceInfo={getInheritanceInfo(prop.originalName || prop.name)}
                            />
                            {shouldRenderTemplateButton && (
                              <div className="pt-1">
                                <Button
                                  type="button"
                                  variant="outline"
                                  size="sm"
                                  className="text-xs"
                                  onClick={templateConfigControls?.onOpenTemplateConfig}
                                >
                                  Manage template properties
                                </Button>
                              </div>
                            )}
                          </div>
                        );
                      })}
                    </div>
                  </AccordionContent>
                </AccordionItem>
              );
            })}
          </Accordion>
        )}
      </div>
    </Form>
  );
};

PropertyEditorTab.displayName = 'PropertyEditorTab';
