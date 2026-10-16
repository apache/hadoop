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
import type { Control, ControllerRenderProps, FormState, UseFormSetValue } from 'react-hook-form';
import { cn } from '~/utils/cn';
import { Input } from '~/components/ui/input';
import { FieldSwitch } from '~/components/ui/field-switch';
import { Badge } from '~/components/ui/badge';
import { TooltipProvider } from '~/components/ui/tooltip';
import { FormField } from '~/components/ui/form';
import { Field, FieldControl, FieldDescription } from '~/components/ui/field';
import type { PropertyDescriptor } from '~/types/property-descriptor';
import { SPECIAL_VALUES } from '~/types';
import type { InheritedValueInfo } from '~/utils/resolveInheritedValue';
import { EnumPropertyField } from './EnumPropertyField';
import { CapacityPropertyField } from './CapacityPropertyField';
import {
  PropertyLabel,
  PropertyWarnings,
  BusinessErrorsList,
  FieldErrorMessage,
  InheritedValueIndicator,
} from './PropertyFieldHelpers';
import { getCommonFieldClassName, parseFieldErrors } from '../utils/fieldHelpers';

interface PropertyFormFieldProps {
  property: PropertyDescriptor;
  control: Control<Record<string, string>>;
  stagedStatus?: 'new' | 'modified' | 'deleted';
  isEnabled?: boolean;
  onBlur?: (
    propertyName: string,
    value: string,
    options?: {
      validationOverrides?: Array<{ queuePath: string; field: string; value: string }>;
    },
  ) => void;
  errors?: string[];
  warnings?: string[];
  queuePath?: string;
  queueName?: string;
  parentQueuePath?: string;
  currentValues?: Partial<Record<string, string>>;
  setFormValue?: UseFormSetValue<Record<string, string>>;
  inheritanceInfo?: InheritedValueInfo | null;
}

export const PropertyFormField: React.FC<PropertyFormFieldProps> = ({
  property,
  control,
  stagedStatus,
  isEnabled = true,
  onBlur,
  errors = [],
  warnings = [],
  queuePath,
  queueName,
  parentQueuePath,
  currentValues,
  setFormValue: _setFormValue,
  inheritanceInfo,
}) => {
  void _setFormValue;

  const renderInput = (
    field: ControllerRenderProps<Record<string, string>, string>,
    formState: FormState<Record<string, string>>,
  ): React.ReactElement => {
    const fieldName = property.formFieldName || property.name;
    const error = formState.errors?.[fieldName];
    const hasFormError = Boolean(error);
    const { inline: inlineBusinessError, remaining: remainingBusinessErrors } =
      parseFieldErrors(errors);
    const effectiveInlineError = hasFormError ? undefined : inlineBusinessError;
    const effectiveRemainingErrors = hasFormError ? errors : remainingBusinessErrors;
    const commonClassName = getCommonFieldClassName(stagedStatus, Boolean(error));

    switch (property.type) {
      case 'boolean': {
        const isLegacyAutoCreationToggle = property.name === 'auto-create-child-queue.enabled';
        const isLockedLegacyToggle = isLegacyAutoCreationToggle && field.value === 'true';
        const switchDisabled = !isEnabled || isLockedLegacyToggle;

        const descriptionContent = isLockedLegacyToggle ? (
          <>
            {property.description ? <span>{property.description}</span> : null}
            <span className="block text-muted-foreground">
              Legacy auto-created queues cannot be disabled. Remove and recreate the queue to turn
              off auto-creation.
            </span>
          </>
        ) : (
          (property.description ?? null)
        );

        return (
          <>
            <FieldSwitch
              id={fieldName}
              fieldName={fieldName}
              label={`${property.displayName}${property.required ? ' *' : ''}`}
              labelSuffix={
                stagedStatus === 'modified' ? (
                  <Badge variant="default" className="text-xs h-4 px-1 shrink-0">
                    Staged
                  </Badge>
                ) : null
              }
              description={descriptionContent}
              labelProps={{
                className: cn(!isEnabled && 'text-muted-foreground'),
              }}
              disabled={switchDisabled}
              checked={field.value === 'true'}
              onCheckedChange={(checked) => {
                const nextValue = checked ? 'true' : 'false';
                field.onChange(nextValue);
                onBlur?.(property.name, nextValue);
              }}
              switchClassName={cn(
                commonClassName,
                isLockedLegacyToggle && 'disabled:opacity-100 disabled:bg-input',
              )}
              message={error ? String(error.message ?? '') : effectiveInlineError}
            />
            <InheritedValueIndicator
              inheritanceInfo={inheritanceInfo ?? null}
            />
            <BusinessErrorsList fieldName={fieldName} messages={effectiveRemainingErrors} />
          </>
        );
      }

      case 'enum':
        return (
          <EnumPropertyField
            property={property}
            field={field}
            error={error}
            stagedStatus={stagedStatus}
            isEnabled={isEnabled}
            inlineBusinessError={effectiveInlineError}
            remainingBusinessErrors={effectiveRemainingErrors}
            onBlur={onBlur}
          />
        );

      case 'number':
        return (
          <Field>
            <PropertyLabel property={property} stagedStatus={stagedStatus} isEnabled={isEnabled} />
            <FieldControl>
              <div className="relative">
                <Input
                  type="number"
                  value={field.value || ''}
                  onChange={(e) => field.onChange(e.target.value)}
                  onBlur={(e) => {
                    field.onBlur();
                    onBlur?.(property.name, e.target.value);
                  }}
                  step={property.displayFormat?.decimals ? 0.01 : 1}
                  min={property.validationRules?.find((r) => r.type === 'range')?.min}
                  max={property.validationRules?.find((r) => r.type === 'range')?.max}
                  disabled={!isEnabled}
                  aria-invalid={Boolean(error)}
                  className={commonClassName}
                  placeholder={inheritanceInfo?.value || undefined}
                />
                {property.displayFormat?.suffix && (
                  <span className="absolute right-3 top-1/2 -translate-y-1/2 text-xs text-muted-foreground">
                    {property.displayFormat.suffix}
                  </span>
                )}
              </div>
            </FieldControl>
            <InheritedValueIndicator
              inheritanceInfo={inheritanceInfo ?? null}
              hasExplicitValue={Boolean(field.value)}
            />
            {property.description && (
              <FieldDescription className="text-xs text-muted-foreground">
                {property.description}
              </FieldDescription>
            )}
            <FieldErrorMessage error={error} inlineBusinessError={effectiveInlineError} />
            <BusinessErrorsList fieldName={fieldName} messages={effectiveRemainingErrors} />
          </Field>
        );

      default: {
        // string, capacity, and ACL fields
        const fieldValue = typeof field.value === 'string' ? field.value : '';
        const isCapacityField = property.name === 'capacity';
        const isMaxCapacityField = property.name === 'maximum-capacity';

        // Handle capacity fields
        if (isCapacityField || isMaxCapacityField) {
          const capacityFieldValue = isCapacityField
            ? fieldValue
            : (currentValues?.['capacity'] ?? '');
          const maxCapacityFieldValue = isMaxCapacityField
            ? fieldValue
            : (currentValues?.['maximum-capacity'] ?? '');

          return (
            <CapacityPropertyField
              property={property}
              value={isCapacityField ? capacityFieldValue : maxCapacityFieldValue}
              error={error}
              stagedStatus={stagedStatus}
              isEnabled={isEnabled}
              inlineBusinessError={effectiveInlineError}
              remainingBusinessErrors={effectiveRemainingErrors}
              warnings={warnings}
              queuePath={queuePath}
              queueName={queueName}
              parentQueuePath={parentQueuePath}
              capacityValue={capacityFieldValue}
              maxCapacityValue={maxCapacityFieldValue}
            />
          );
        }

        // Handle ACL fields
        const isAclField = property.name.includes('acl');
        const aclValue = isAclField ? field.value || '' : '';
        const showAllUsersIndicator = aclValue === SPECIAL_VALUES.ALL_USERS_ACL;
        const showNoAccessIndicator = aclValue === SPECIAL_VALUES.NO_USERS_ACL;

        return (
          <Field>
            <PropertyLabel
              property={property}
              stagedStatus={stagedStatus}
              isEnabled={isEnabled}
              className="justify-between gap-2"
            />
            <FieldControl>
              {isAclField ? (
                <textarea
                  value={field.value || ''}
                  onChange={(e) => field.onChange(e.target.value)}
                  onBlur={(e) => {
                    field.onBlur();
                    onBlur?.(property.name, e.target.value);
                  }}
                  rows={2}
                  placeholder={inheritanceInfo?.value || property.defaultValue || undefined}
                  className={cn(
                    'flex w-full rounded-md border border-input bg-background px-3 py-2 text-sm ring-offset-background placeholder:text-muted-foreground focus-visible:outline-none focus-visible:ring-2 focus-visible:ring-ring focus-visible:ring-offset-2 disabled:cursor-not-allowed disabled:opacity-50',
                    commonClassName,
                  )}
                  disabled={!isEnabled}
                  aria-invalid={Boolean(error)}
                />
              ) : (
                <Input
                  type="text"
                  value={field.value || ''}
                  onChange={(e) => field.onChange(e.target.value)}
                  onBlur={(e) => {
                    field.onBlur();
                    onBlur?.(property.name, e.target.value);
                  }}
                  placeholder={inheritanceInfo?.value || property.defaultValue || undefined}
                  disabled={!isEnabled}
                  aria-invalid={Boolean(error)}
                  className={commonClassName}
                />
              )}
            </FieldControl>
            {isAclField && (showAllUsersIndicator || showNoAccessIndicator) && (
              <div className="flex items-center gap-2 mt-1">
                {showAllUsersIndicator && (
                  <Badge
                    variant="outline"
                    className="text-xs border-blue-500 text-blue-600 dark:text-blue-400"
                  >
                    All users
                  </Badge>
                )}
                {showNoAccessIndicator && (
                  <Badge
                    variant="outline"
                    className="text-xs border-red-500 text-red-600 dark:text-red-400"
                  >
                    No access
                  </Badge>
                )}
              </div>
            )}
            <InheritedValueIndicator
              inheritanceInfo={inheritanceInfo ?? null}
              hasExplicitValue={Boolean(field.value)}
            />
            {property.description && (
              <FieldDescription className="text-xs text-muted-foreground">
                {property.description}
              </FieldDescription>
            )}
            <FieldErrorMessage error={error} inlineBusinessError={effectiveInlineError} />
            <BusinessErrorsList fieldName={fieldName} messages={effectiveRemainingErrors} />
            <PropertyWarnings warnings={warnings} />
          </Field>
        );
      }
    }
  };

  return (
    <TooltipProvider>
      <FormField
        control={control}
        name={property.formFieldName || property.name}
        render={({ field, formState }) => (
          <div className="space-y-1" data-field-id={property.originalName || property.name}>
            {renderInput(field, formState)}

            {/* Status badges and helper text */}
            {(property.deprecated || property.deprecationMessage) && (
              <div className="flex items-center flex-wrap gap-1 mt-2">
                {property.deprecated && (
                  <Badge
                    variant="outline"
                    className="text-xs h-5 border-orange-500 text-orange-500"
                  >
                    Deprecated
                  </Badge>
                )}
                {property.deprecated && property.deprecationMessage && (
                  <span className="text-xs text-orange-500">{property.deprecationMessage}</span>
                )}
              </div>
            )}
          </div>
        )}
      />
    </TooltipProvider>
  );
};
