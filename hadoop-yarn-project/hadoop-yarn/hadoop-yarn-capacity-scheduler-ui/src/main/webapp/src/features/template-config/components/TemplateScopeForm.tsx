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


import React, { useState } from 'react';
import {
  Card,
  CardContent,
  CardDescription,
  CardFooter,
  CardHeader,
  CardTitle,
} from '~/components/ui/card';
import { Badge } from '~/components/ui/badge';
import { Button } from '~/components/ui/button';
import { Form } from '~/components/ui/form';
import { ScrollArea } from '~/components/ui/scroll-area';
import { Kbd } from '~/components/ui/kbd';
import {
  PropertyFormField,
  usePropertyEditor,
  queueCategoryOrder,
  categoryConfig,
} from '~/features/property-editor';
import { shouldShowProperty, isPropertyEnabled } from '~/utils/propertyConditions';
import { getTemplatePropertyDefinitions } from '~/config/properties/helpers';
import { globalPropertyDefinitions } from '~/config/properties/global-properties';
import type { PropertyCategory } from '~/types';
import type { PropertyDescriptor } from '~/types/property-descriptor';
import { CONFIG_PREFIXES } from '~/types';
import { useSchedulerStore } from '~/stores/schedulerStore';
import type { TemplateScope } from '~/features/template-config/types';
import { formatQueuePathLabel } from '~/features/template-config/utils/queuePathLabel';
import { useKeyboardShortcuts, getModifierKey } from '~/hooks/useKeyboardShortcuts';

interface TemplateScopeFormProps {
  scope: TemplateScope;
  baseQueuePath: string;
}

const templateProperties = getTemplatePropertyDefinitions();

export const TemplateScopeForm: React.FC<TemplateScopeFormProps> = ({ scope, baseQueuePath }) => {
  const [isSubmitting, setIsSubmitting] = useState(false);

  const getQueuePropertyValue = useSchedulerStore((state) => state.getQueuePropertyValue);
  const getGlobalPropertyValue = useSchedulerStore((state) => state.getGlobalPropertyValue);
  const stagedChanges = useSchedulerStore((state) => state.stagedChanges);
  const configData = useSchedulerStore((state) => state.configData);
  const schedulerInfo = useSchedulerStore((state) => state.schedulerData);

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
  } = usePropertyEditor({
    queuePath: scope.queuePath,
    properties: templateProperties,
  });

  const queueValues: Record<string, string> = {};
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
    queueValues[property.originalName || property.name] = normalized;
  });

  const globalValues: Record<string, string> = {};
  globalPropertyDefinitions.forEach((property) => {
    const { value } = getGlobalPropertyValue(property.name);
    globalValues[property.name] = value;
  });

  const queueValueCache = new Map<string, string | undefined>();
  const globalValueCache = new Map<string, string | undefined>();

  const getQueueValue = (targetQueuePath: string, name: string) => {
    if (!targetQueuePath) return undefined;

    if (targetQueuePath === scope.queuePath) {
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
    return getQueueValue(scope.queuePath, name);
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
    queuePath: scope.queuePath,
    queueInfo: null,
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

  const visiblePropertiesByCategory: Partial<Record<PropertyCategory, PropertyDescriptor[]>> = {};

  Object.entries(propertiesByCategory).forEach(([categoryKey, props]) => {
    const typedCategory = categoryKey as PropertyCategory;
    const filtered = props.filter((property) => {
      const propertyName = property.originalName || property.name;
      return propertyStates.get(propertyName)?.visible ?? true;
    });

    if (filtered.length > 0) {
      visiblePropertiesByCategory[typedCategory] = filtered;
    }
  });

  const availableCategories = queueCategoryOrder.filter(
    (category) => (visiblePropertiesByCategory[category]?.length ?? 0) > 0,
  );

  const categoriesWithErrors: Set<PropertyCategory> = new Set();

  if (errors && Object.keys(errors).length > 0) {
    Object.keys(errors).forEach((fieldName) => {
      availableCategories.forEach((category) => {
        const categoryProps = visiblePropertiesByCategory[category] ?? [];
        if (
          categoryProps.some(
            (prop) => (prop.originalName || prop.name) === fieldName || prop.name === fieldName,
          )
        ) {
          categoriesWithErrors.add(category);
        }
      });
    });
  }

  const onSubmit = async () => {
    setIsSubmitting(true);
    try {
      await handleSubmit();
    } finally {
      setIsSubmitting(false);
    }
  };

  // Keyboard shortcuts
  useKeyboardShortcuts([
    {
      key: 's',
      ctrl: true,
      meta: true,
      preventDefault: true,
      handler: () => {
        if (!isSubmitting && formState.isDirty) {
          void onSubmit();
        }
      },
    },
    {
      key: 'k',
      ctrl: true,
      meta: true,
      preventDefault: true,
      handler: () => {
        if (!isSubmitting) {
          handleReset();
        }
      },
    },
  ]);

  return (
    <Card className="border-border flex h-full min-h-0 flex-col">
      <CardHeader className="px-6 pt-6 pb-4 shrink-0">
        <CardTitle className="flex items-center gap-2 text-base">
          {scope.displayName}
          {scope.isWildcard && (
            <Badge variant="outline" className="text-xs uppercase tracking-wide">
              wildcard
            </Badge>
          )}
          {hasChanges && (
            <Badge variant="secondary" className="text-xs uppercase tracking-wide">
              staged
            </Badge>
          )}
        </CardTitle>
        <CardDescription className="space-y-1">
          <div>{scope.description}</div>
          <div className="text-xs font-mono text-muted-foreground">
            {CONFIG_PREFIXES.BASE}.{scope.queuePath}
          </div>
          {scope.displayQueuePath && scope.displayQueuePath !== baseQueuePath && (
            <div className="text-xs text-muted-foreground">
              Applies to queues matching{' '}
              <span className="font-mono" title={scope.displayQueuePath}>
                {scope.isWildcard
                  ? formatQueuePathLabel(scope.displayQueuePath)
                  : scope.displayQueuePath}
              </span>
            </div>
          )}
        </CardDescription>
      </CardHeader>
      <ScrollArea className="flex-1 min-h-0" style={{ scrollbarGutter: 'stable both-edges' }}>
        <CardContent className="px-6 pb-8">
          <Form {...form}>
            <form
              className="space-y-6"
              onSubmit={(event) => {
                event.preventDefault();
                onSubmit();
              }}
            >
              {availableCategories.map((category) => {
                const categoryMeta = categoryConfig[category];
                const categoryProps = visiblePropertiesByCategory[category] ?? [];
                if (categoryProps.length === 0) {
                  return null;
                }
                const hasCategoryErrors = categoriesWithErrors.has(category);

                return (
                  <div key={category} className="space-y-3">
                    <div className="flex items-center gap-2">
                      {categoryMeta.icon}
                      <div>
                        <div className="text-sm font-medium">{categoryMeta.label}</div>
                        <div className="text-xs text-muted-foreground">
                          {categoryMeta.description}
                        </div>
                      </div>
                      {hasCategoryErrors && (
                        <Badge variant="destructive" className="text-xs uppercase tracking-wide">
                          errors
                        </Badge>
                      )}
                    </div>

                    <div className="space-y-3 pl-6">
                      {categoryProps.map((property) => {
                        const propertyKey = property.originalName || property.name;
                        const propertyState = propertyStates.get(propertyKey);
                        if (propertyState && !propertyState.visible) {
                          return null;
                        }
                        return (
                          <PropertyFormField
                            key={`${scope.id}:${property.name}`}
                            property={property}
                            control={control}
                            stagedStatus={getStagedStatus(propertyKey)}
                            isEnabled={propertyState?.enabled ?? true}
                            onBlur={handleFieldBlur}
                            errors={getFieldErrors(property.formFieldName || property.name)}
                            warnings={getFieldWarnings(property.formFieldName || property.name)}
                            queuePath={scope.queuePath}
                            parentQueuePath={baseQueuePath}
                            currentValues={watchedValues}
                            setFormValue={form.setValue}
                          />
                        );
                      })}
                    </div>
                  </div>
                );
              })}
            </form>
          </Form>
        </CardContent>
      </ScrollArea>
      <CardFooter className="sticky bottom-0 z-10 flex items-center justify-between gap-3 border-t bg-background/95 px-6 py-4 backdrop-blur supports-[backdrop-filter]:bg-background/70 shrink-0">
        <div className="flex items-center gap-2 text-xs text-muted-foreground">
          {!isValid && formState.isSubmitted && (
            <span className="text-destructive">Resolve validation errors</span>
          )}
          {formState.isDirty && <span>Unsaved changes</span>}
        </div>
        <div className="flex items-center gap-2">
          <Button variant="outline" onClick={() => handleReset()} disabled={isSubmitting}>
            Reset
            <Kbd className="ml-auto">{getModifierKey()}+K</Kbd>
          </Button>
          <Button onClick={onSubmit} disabled={isSubmitting || !formState.isDirty}>
            {isSubmitting ? 'Staging…' : 'Stage changes'}
            {!isSubmitting && <Kbd className="ml-auto">{getModifierKey()}+S</Kbd>}
          </Button>
        </div>
      </CardFooter>
    </Card>
  );
};
