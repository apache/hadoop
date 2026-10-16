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


import React from 'react';
import { useShallow } from 'zustand/react/shallow';
import { useSchedulerStore } from '~/stores/schedulerStore';
import { globalPropertyDefinitions } from '~/config/properties/global-properties';
import { SPECIAL_VALUES, type PropertyCategory } from '~/types';
import { Alert, AlertDescription, AlertTitle } from '~/components/ui/alert';
import { Card, CardContent } from '~/components/ui/card';
import { Badge } from '~/components/ui/badge';
import {
  Accordion,
  AccordionContent,
  AccordionItem,
  AccordionTrigger,
} from '~/components/ui/accordion';
import { AlertCircle } from 'lucide-react';
import { PropertyInput } from './PropertyInput';
import { LegacyModeToggle } from './LegacyModeToggle';
import { shouldShowProperty, isPropertyEnabled } from '~/utils/propertyConditions';
import { useGlobalPropertyValidation } from '~/features/global-settings/hooks/useGlobalPropertyValidation';
import {
  categoryConfig,
  globalCategoryOrder,
} from '~/features/property-editor/constants/categoryConfig';

export const GlobalSettings: React.FC = () => {
  // State values (trigger re-renders only when these specific values change)
  const { stagedChanges, searchQuery, applyError, configData, schedulerData } = useSchedulerStore(
    useShallow((s) => ({
      stagedChanges: s.stagedChanges,
      searchQuery: s.searchQuery,
      applyError: s.applyError,
      configData: s.configData,
      schedulerData: s.schedulerData,
    })),
  );

  // Actions (stable references, never trigger re-renders)
  const getGlobalPropertyValue = useSchedulerStore((s) => s.getGlobalPropertyValue);
  const getQueuePropertyValue = useSchedulerStore((s) => s.getQueuePropertyValue);
  const stageGlobalChange = useSchedulerStore((s) => s.stageGlobalChange);
  const getFilteredSettings = useSchedulerStore((s) => s.getFilteredSettings);
  const { validateGlobalProperty } = useGlobalPropertyValidation();

  // Use filtered settings if search is active
  const requestedPropertyDefinitions = searchQuery
    ? getFilteredSettings()
    : globalPropertyDefinitions;

  const values: Record<string, string> = {};
  globalPropertyDefinitions.forEach((property) => {
    const { value } = getGlobalPropertyValue(property.name);
    values[property.name] = value;
  });
  const globalValues = values;

  const queueValueCache = new Map<string, string | undefined>();

  const getGlobalValue = (name: string) => {
    if (name in globalValues) {
      return globalValues[name];
    }
    return getGlobalPropertyValue(name).value;
  };

  const getValue = (name: string) => getGlobalValue(name);

  const getQueueValue = (queuePath: string, property: string) => {
    if (!queuePath) return undefined;
    const cacheKey = `${queuePath}::${property}`;
    if (!queueValueCache.has(cacheKey)) {
      const { value } = getQueuePropertyValue(queuePath, property);
      queueValueCache.set(cacheKey, value);
    }
    return queueValueCache.get(cacheKey);
  };

  const conditionBase = {
    scope: 'global' as const,
    values: globalValues,
    globalValues,
    queuePath: undefined,
    queueInfo: undefined,
    schedulerInfo: schedulerData,
    stagedChanges,
    configData,
    getValue,
    getGlobalValue,
    getQueueValue,
    getConfigValue: (key: string) => configData.get(key),
  };

  const states = new Map<
    string,
    {
      visible: boolean;
      enabled: boolean;
    }
  >();

  requestedPropertyDefinitions.forEach((property) => {
    const propertyValue = conditionBase.getValue(property.name) ?? '';
    const options = {
      ...conditionBase,
      property,
      propertyValue,
    };
    const visible = shouldShowProperty(property, options);
    const enabled = visible ? isPropertyEnabled(property, options) : false;
    states.set(property.name, { visible, enabled });
  });

  const propertyStates = states;

  const activePropertyDefinitions = requestedPropertyDefinitions.filter((property) => {
    const state = propertyStates.get(property.name);
    return state ? state.visible : true;
  });

  const getGlobalPropertyCategories = () => {
    const categories: PropertyCategory[] = [];

    activePropertyDefinitions.forEach((prop) => {
      if (!categories.includes(prop.category)) {
        categories.push(prop.category);
      }
    });

    // Sort categories based on globalCategoryOrder
    return categories.sort((a, b) => {
      const indexA = globalCategoryOrder.indexOf(a);
      const indexB = globalCategoryOrder.indexOf(b);

      // If both are in the order list, sort by their position
      if (indexA !== -1 && indexB !== -1) {
        return indexA - indexB;
      }
      // If only one is in the order list, prioritize it
      if (indexA !== -1) return -1;
      if (indexB !== -1) return 1;
      // If neither is in the order list, maintain original order
      return 0;
    });
  };

  const getGlobalPropertiesByCategory = (category: PropertyCategory) => {
    return activePropertyDefinitions.filter((prop) => prop.category === category);
  };

  const categories = getGlobalPropertyCategories();
  const globalStagedChanges = stagedChanges.filter(
    (c) => c.queuePath === SPECIAL_VALUES.GLOBAL_QUEUE_PATH,
  );

  const handlePropertyChange = (propertyKey: string, value: string) => {
    const validationErrors = validateGlobalProperty(propertyKey, value);
    stageGlobalChange(propertyKey, value, validationErrors);
  };

  return (
    <div className="space-y-6">
      {applyError && (
        <Alert variant="destructive">
          <AlertCircle className="h-4 w-4" />
          <AlertTitle>Failed to Apply Changes</AlertTitle>
          <AlertDescription>{applyError}</AlertDescription>
        </Alert>
      )}

      {globalStagedChanges.length > 0 && (
        <Alert>
          <AlertDescription>
            You have {globalStagedChanges.length} unsaved global setting
            {globalStagedChanges.length !== 1 ? 's' : ''}. Apply changes to make them active.
          </AlertDescription>
        </Alert>
      )}

      {categories.length > 1 && (
        <nav className="flex flex-wrap gap-2" aria-label="Settings sections">
          {categories.map((category) => {
            const label = categoryConfig[category]?.label || `${category} Settings`;
            return (
              <a
                key={category}
                href={`#section-${category}`}
                onClick={(e) => {
                  e.preventDefault();
                  document
                    .getElementById(`section-${category}`)
                    ?.scrollIntoView({ behavior: 'smooth' });
                }}
                className="inline-flex items-center gap-1.5 rounded-md border px-3 py-1.5 text-xs font-medium text-muted-foreground transition-colors hover:bg-accent hover:text-foreground"
              >
                {categoryConfig[category]?.icon}
                {label}
              </a>
            );
          })}
        </nav>
      )}

      {categories.length > 0 ? (
        <Accordion type="multiple" defaultValue={categories} className="space-y-4">
          {categories.map((category) => {
            const categoryProperties = getGlobalPropertiesByCategory(category);
            const hasChanges = categoryProperties.some((property) =>
              globalStagedChanges.some((c) => c.property === property.name),
            );

            return (
              <AccordionItem
                key={category}
                value={category}
                className="border rounded-lg"
                id={`section-${category}`}
              >
                <AccordionTrigger className="px-6 hover:no-underline">
                  <div className="flex items-center gap-2">
                    {categoryConfig[category]?.icon}
                    <h3 className="text-lg font-medium">
                      {categoryConfig[category]?.label || `${category} Settings`}
                    </h3>
                    {hasChanges && (
                      <Badge variant="outline" className="border-warning text-warning">
                        Has Changes
                      </Badge>
                    )}
                  </div>
                </AccordionTrigger>
                <AccordionContent className="px-6 pt-6 pb-6">
                  <div className="space-y-6">
                    {categoryProperties.map((property, index) => {
                      const { value, isStaged } = getGlobalPropertyValue(property.name);
                      const propertyState = propertyStates.get(property.name);
                      const isEnabled = propertyState?.enabled ?? true;

                      return (
                        <div key={property.name}>
                          {property.name === SPECIAL_VALUES.LEGACY_MODE_PROPERTY ? (
                            <LegacyModeToggle
                              property={property}
                              value={value}
                              isStaged={isStaged}
                              onChange={(newValue) => handlePropertyChange(property.name, newValue)}
                              disabled={!isEnabled}
                              searchQuery={searchQuery}
                            />
                          ) : (
                            <PropertyInput
                              property={property}
                              value={value}
                              isStaged={isStaged}
                              onChange={(newValue) => handlePropertyChange(property.name, newValue)}
                              searchQuery={searchQuery}
                              disabled={!isEnabled}
                            />
                          )}
                          {index < categoryProperties.length - 1 && <hr className="mt-6" />}
                        </div>
                      );
                    })}
                  </div>
                </AccordionContent>
              </AccordionItem>
            );
          })}
        </Accordion>
      ) : (
        <Card>
          <CardContent className="pt-6">
            <h3 className="mb-2 text-lg font-medium">
              {searchQuery ? 'No Matching Settings' : 'No Global Properties Available'}
            </h3>
            <p className="text-sm text-muted-foreground">
              {searchQuery
                ? `No settings match your search for "${searchQuery}". Try a different search term.`
                : 'Global properties configuration is not available. Please check the configuration setup.'}
            </p>
          </CardContent>
        </Card>
      )}
    </div>
  );
};
