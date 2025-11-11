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
import { FieldSelect } from '~/components/ui/field-select';
import { Badge } from '~/components/ui/badge';
import { ToggleGroup, ToggleGroupItem } from '~/components/ui/toggle-group';
import { Tooltip, TooltipContent, TooltipProvider, TooltipTrigger } from '~/components/ui/tooltip';
import { FormField } from '~/components/ui/form';
import {
  Field,
  FieldControl,
  FieldDescription,
  FieldLabel,
  FieldMessage,
} from '~/components/ui/field';
import { Info, AlertTriangle } from 'lucide-react';
import { Button } from '~/components/ui/button';
import { useCapacityEditor } from '~/features/queue-management/hooks/useCapacityEditor';
import type { PropertyDescriptor } from '~/types/property-descriptor';
import { SPECIAL_VALUES } from '~/types';

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
}

interface PropertyLabelProps {
  property: PropertyDescriptor;
  stagedStatus?: 'new' | 'modified' | 'deleted';
  isEnabled: boolean;
  className?: string;
  contentClassName?: string;
  children?: React.ReactNode;
}

const PropertyLabel: React.FC<PropertyLabelProps> = ({
  property,
  stagedStatus,
  isEnabled,
  className,
  contentClassName,
  children,
}) => (
  <FieldLabel
    className={cn('flex items-center gap-1', className, !isEnabled && 'text-muted-foreground')}
  >
    <div className={cn('flex items-center gap-1 min-w-0', contentClassName)}>
      <span className="truncate">
        {property.displayName}
        {property.required ? ' *' : ''}
      </span>
      {stagedStatus === 'modified' && (
        <Badge variant="default" className="text-xs h-4 px-1 shrink-0">
          Staged
        </Badge>
      )}
    </div>
    {children}
  </FieldLabel>
);

const renderBusinessErrorsList = (fieldName: string, messages: string[]) => {
  if (messages.length === 0) {
    return null;
  }

  return (
    <div className="mt-1 space-y-1">
      {messages.map((message) => (
        <div key={`business-error-${fieldName}-${message}`} className="text-xs text-destructive">
          {message}
        </div>
      ))}
    </div>
  );
};

const PropertyWarnings: React.FC<{ warnings: string[] }> = ({ warnings }) => {
  if (warnings.length === 0) {
    return null;
  }

  return (
    <div className="mt-1 space-y-1">
      {warnings.map((warning) => {
        const isLegacyMode = warning.includes('legacy mode requirement');
        return (
          <div key={`warning-${warning}`} className="flex items-start gap-1.5">
            <AlertTriangle className="mt-0.5 h-3.5 w-3.5 flex-shrink-0 text-yellow-600 dark:text-yellow-500" />
            <p className="text-sm text-yellow-600 dark:text-yellow-500">{warning}</p>
            {isLegacyMode && (
              <Tooltip>
                <TooltipTrigger asChild>
                  <Info className="mt-0.5 h-3.5 w-3.5 cursor-help flex-shrink-0 text-muted-foreground" />
                </TooltipTrigger>
                <TooltipContent className="max-w-xs">
                  <p className="text-xs">
                    This validation is enforced because legacy queue mode is enabled. You can
                    disable legacy mode in Global Settings for more flexible capacity configuration.
                  </p>
                </TooltipContent>
              </Tooltip>
            )}
          </div>
        );
      })}
    </div>
  );
};

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
}) => {
  void _setFormValue;
  const { openCapacityEditor } = useCapacityEditor();

  // Render different input types based on property type
  const renderInput = (
    field: ControllerRenderProps<Record<string, string>, string>,
    formState: FormState<Record<string, string>>,
  ): React.ReactElement => {
    const fieldName = property.formFieldName || property.name;
    const error = formState.errors?.[fieldName];
    const hasFormError = Boolean(error);
    const fieldErrors = errors
      .map((message) => (typeof message === 'string' ? message.trim() : ''))
      .filter((message) => message.length > 0);
    const inlineBusinessError = hasFormError ? undefined : fieldErrors[0];
    const remainingBusinessErrors = hasFormError
      ? fieldErrors
      : inlineBusinessError
        ? fieldErrors.slice(1)
        : [];
    const commonProps = {
      className: cn(
        stagedStatus === 'modified' && 'ring-2 ring-primary ring-offset-1',
        error && 'ring-2 ring-destructive ring-offset-1',
      ),
    };

    switch (property.type) {
      case 'boolean':
        return (() => {
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

          const switchControl = (
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
                commonProps.className,
                isLockedLegacyToggle && 'disabled:opacity-100 disabled:bg-input',
              )}
              message={
                error
                  ? String(error.message ?? '')
                  : inlineBusinessError
                    ? inlineBusinessError
                    : undefined
              }
            />
          );

          return (
            <>
              {switchControl}
              {renderBusinessErrorsList(fieldName, remainingBusinessErrors)}
            </>
          );
        })();

      case 'enum': {
        const enumOptions = property.enumValues ?? [];

        if (!enumOptions.length) {
          return (
            <Field>
              <FieldLabel>{property.displayName}</FieldLabel>
              <FieldDescription className="text-xs text-muted-foreground">
                No options available.
              </FieldDescription>
            </Field>
          );
        }

        const renderChoiceCards = () => (
          <Field>
            <PropertyLabel property={property} stagedStatus={stagedStatus} isEnabled={isEnabled} />
            <FieldControl>
              <div className="grid gap-3 sm:grid-cols-2">
                {enumOptions.map((option) => {
                  const isSelected = field.value === option.value;
                  return (
                    <label
                      key={option.value}
                      className={cn(
                        'relative flex cursor-pointer flex-col gap-2 rounded-lg border p-4 text-left transition',
                        'focus-within:outline-none focus-within:ring-2 focus-within:ring-ring focus-within:ring-offset-2',
                        isSelected
                          ? 'border-primary ring-2 ring-primary'
                          : 'border-border hover:border-primary/60',
                        !isEnabled && 'cursor-not-allowed opacity-60',
                      )}
                    >
                      <div className="flex items-start gap-3">
                        <input
                          type="radio"
                          name={fieldName}
                          value={option.value}
                          checked={isSelected}
                          onChange={() => {
                            field.onChange(option.value);
                            onBlur?.(property.name, option.value);
                          }}
                          disabled={!isEnabled}
                          className="mt-0.5 h-4 w-4 rounded-full border border-input text-primary focus-visible:outline-none focus-visible:ring-2 focus-visible:ring-ring"
                        />
                        <div className="flex-1 space-y-1">
                          <div className="flex items-start justify-between gap-2">
                            <span className="text-sm font-medium leading-none">{option.label}</span>
                            {isSelected && (
                              <Badge variant="secondary" className="text-[10px] px-1.5 py-0">
                                Selected
                              </Badge>
                            )}
                          </div>
                          {option.description && (
                            <p className="text-xs text-muted-foreground">{option.description}</p>
                          )}
                        </div>
                      </div>
                    </label>
                  );
                })}
              </div>
            </FieldControl>
            {property.description && (
              <FieldDescription className="text-xs text-muted-foreground">
                {property.description}
              </FieldDescription>
            )}
            {(error || inlineBusinessError) && (
              <FieldMessage>
                {error ? String(error.message ?? '') : inlineBusinessError}
              </FieldMessage>
            )}
            {renderBusinessErrorsList(fieldName, remainingBusinessErrors)}
          </Field>
        );

        const renderSelect = () => {
          const selectOptions = enumOptions.map((option) => ({
            value: option.value,
            label: option.label,
            // Omit descriptions to prevent overflow in the dropdown
          }));

          return (
            <>
              <FieldSelect
                id={fieldName}
                fieldName={fieldName}
                label={
                  <PropertyLabel
                    property={property}
                    stagedStatus={stagedStatus}
                    isEnabled={isEnabled}
                  />
                }
                description={property.description}
                options={selectOptions}
                value={field.value || ''}
                onValueChange={(value) => {
                  if (value) {
                    field.onChange(value);
                    onBlur?.(property.name, value);
                  }
                }}
                placeholder="Select an option..."
                disabled={!isEnabled}
                fieldClassName="space-y-2"
                triggerClassName={cn('w-full', commonProps.className)}
                selectClassName="w-[var(--radix-select-trigger-width)]"
                message={
                  error
                    ? String(error.message ?? '')
                    : inlineBusinessError
                      ? inlineBusinessError
                      : undefined
                }
              />
              {renderBusinessErrorsList(fieldName, remainingBusinessErrors)}
            </>
          );
        };

        const renderToggleGroup = () => (
          <Field>
            <PropertyLabel property={property} stagedStatus={stagedStatus} isEnabled={isEnabled} />
            <FieldControl>
              <ToggleGroup
                type="single"
                value={field.value || ''}
                onValueChange={(value) => {
                  if (value) {
                    field.onChange(value);
                    onBlur?.(property.name, value);
                  }
                }}
                disabled={!isEnabled}
                className="justify-start flex-wrap"
                variant="outline"
              >
                {enumOptions.map((option) => (
                  <ToggleGroupItem key={option.value} value={option.value} className="text-xs">
                    {option.label}
                  </ToggleGroupItem>
                ))}
              </ToggleGroup>
            </FieldControl>
            {property.description && (
              <FieldDescription className="text-xs text-muted-foreground">
                {property.description}
              </FieldDescription>
            )}
            {(error || inlineBusinessError) && (
              <FieldMessage>
                {error ? String(error.message ?? '') : inlineBusinessError}
              </FieldMessage>
            )}
            {renderBusinessErrorsList(fieldName, remainingBusinessErrors)}
          </Field>
        );

        // Use choiceCard for explicit display preference
        if (property.enumDisplay === 'choiceCard') {
          return renderChoiceCards();
        }
        // Use select dropdown for 4 or more options
        if (enumOptions.length >= 4) {
          return renderSelect();
        }
        // Use toggle group for 2-3 options
        return renderToggleGroup();
      }

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
                  className={cn(
                    stagedStatus === 'modified' && 'ring-2 ring-primary ring-offset-1',
                    error && 'ring-2 ring-destructive ring-offset-1',
                  )}
                />
                {property.displayFormat?.suffix && (
                  <span className="absolute right-3 top-1/2 -translate-y-1/2 text-xs text-muted-foreground">
                    {property.displayFormat.suffix}
                  </span>
                )}
              </div>
            </FieldControl>
            {property.description && (
              <FieldDescription className="text-xs text-muted-foreground">
                {property.description}
              </FieldDescription>
            )}
            {(error || inlineBusinessError) && (
              <FieldMessage>
                {error ? String(error.message ?? '') : inlineBusinessError}
              </FieldMessage>
            )}
            {renderBusinessErrorsList(fieldName, remainingBusinessErrors)}
          </Field>
        );

      default: {
        // string, capacity, and ACL fields
        const fieldValue = typeof field.value === 'string' ? field.value : '';
        const isCapacityField = property.name === 'capacity';
        const isMaxCapacityField = property.name === 'maximum-capacity';
        const capacityFieldValue = isCapacityField
          ? fieldValue
          : (currentValues?.['capacity'] ?? '');
        const maxCapacityFieldValue = isMaxCapacityField
          ? fieldValue
          : (currentValues?.['maximum-capacity'] ?? '');

        const handleOpenCapacityEditor = () => {
          if (!parentQueuePath || !queuePath) {
            return;
          }

          const safeQueueName =
            queueName ?? queuePath?.split('.').pop() ?? parentQueuePath.split('.').pop() ?? 'Queue';

          openCapacityEditor({
            origin: 'property-editor',
            parentQueuePath,
            originQueuePath: queuePath,
            originQueueName: safeQueueName,
            capacityValue: capacityFieldValue,
            maxCapacityValue: maxCapacityFieldValue,
          });
        };

        if (isCapacityField || isMaxCapacityField) {
          const displayValue =
            (isCapacityField ? capacityFieldValue : maxCapacityFieldValue) || 'Not set';

          return (
            <Field>
              <PropertyLabel
                property={property}
                stagedStatus={stagedStatus}
                isEnabled={isEnabled}
                className="flex-wrap gap-2"
                contentClassName="flex-1 gap-1"
              >
                <div className="ml-auto flex-shrink-0">
                  {isCapacityField ? (
                    <Button
                      type="button"
                      size="sm"
                      variant="outline"
                      className="text-xs"
                      onClick={handleOpenCapacityEditor}
                      disabled={!parentQueuePath || !isEnabled}
                    >
                      Capacity Editor
                    </Button>
                  ) : (
                    <span className="text-xs text-muted-foreground">
                      Managed in Capacity Editor
                    </span>
                  )}
                </div>
              </PropertyLabel>
              <div className="mt-2 w-full break-all rounded-md border border-dashed bg-muted/40 px-3 py-2 text-sm font-mono text-foreground">
                {displayValue}
              </div>
              {property.description && !(isCapacityField || isMaxCapacityField) && (
                <FieldDescription className="text-xs text-muted-foreground">
                  {property.description}
                </FieldDescription>
              )}
              {(error || inlineBusinessError) && (
                <FieldMessage>
                  {error ? String(error.message ?? '') : inlineBusinessError}
                </FieldMessage>
              )}
              {renderBusinessErrorsList(fieldName, remainingBusinessErrors)}
              <PropertyWarnings warnings={warnings} />
            </Field>
          );
        }

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
                  placeholder={property.defaultValue || undefined}
                  className={cn(
                    'flex w-full rounded-md border border-input bg-background px-3 py-2 text-sm ring-offset-background placeholder:text-muted-foreground focus-visible:outline-none focus-visible:ring-2 focus-visible:ring-ring focus-visible:ring-offset-2 disabled:cursor-not-allowed disabled:opacity-50',
                    commonProps.className,
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
                  placeholder={property.defaultValue || undefined}
                  disabled={!isEnabled}
                  aria-invalid={Boolean(error)}
                  className={cn(
                    stagedStatus === 'modified' && 'ring-2 ring-primary ring-offset-1',
                    error && 'ring-2 ring-destructive ring-offset-1',
                  )}
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
            {property.description && (
              <FieldDescription className="text-xs text-muted-foreground">
                {property.description}
              </FieldDescription>
            )}
            {(error || inlineBusinessError) && (
              <FieldMessage>
                {error ? String(error.message ?? '') : inlineBusinessError}
              </FieldMessage>
            )}
            {renderBusinessErrorsList(fieldName, remainingBusinessErrors)}
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
