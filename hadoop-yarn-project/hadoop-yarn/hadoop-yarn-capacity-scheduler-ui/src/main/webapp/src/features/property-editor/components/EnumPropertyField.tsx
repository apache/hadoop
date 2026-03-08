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


/**
 * Enum property field
 *
 * Renders enum fields as choice cards, select dropdown, or toggle group.
 */

import React from 'react';
import type { ControllerRenderProps, FieldError } from 'react-hook-form';
import { cn } from '~/utils/cn';
import { Badge } from '~/components/ui/badge';
import { ToggleGroup, ToggleGroupItem } from '~/components/ui/toggle-group';
import { FieldSelect } from '~/components/ui/field-select';
import { Field, FieldControl, FieldDescription } from '~/components/ui/field';
import type { PropertyDescriptor } from '~/types/property-descriptor';
import { PropertyLabel, BusinessErrorsList, FieldErrorMessage } from './PropertyFieldHelpers';
import { getCommonFieldClassName } from '../utils/fieldHelpers';

interface EnumPropertyFieldProps {
  property: PropertyDescriptor;
  field: ControllerRenderProps<Record<string, string>, string>;
  error?: FieldError;
  stagedStatus?: 'new' | 'modified' | 'deleted';
  isEnabled: boolean;
  inlineBusinessError?: string;
  remainingBusinessErrors: string[];
  onBlur?: (propertyName: string, value: string) => void;
}

export const EnumPropertyField: React.FC<EnumPropertyFieldProps> = ({
  property,
  field,
  error,
  stagedStatus,
  isEnabled,
  inlineBusinessError,
  remainingBusinessErrors,
  onBlur,
}) => {
  const fieldName = property.formFieldName || property.name;
  const enumOptions = property.enumValues ?? [];
  const commonClassName = getCommonFieldClassName(stagedStatus, Boolean(error));

  if (!enumOptions.length) {
    return (
      <Field>
        <PropertyLabel property={property} stagedStatus={stagedStatus} isEnabled={isEnabled} />
        <FieldDescription className="text-xs text-muted-foreground">
          No options available.
        </FieldDescription>
      </Field>
    );
  }

  const handleValueChange = (value: string) => {
    if (value) {
      field.onChange(value);
      onBlur?.(property.name, value);
    }
  };

  // Choice cards for explicit display preference
  if (property.enumDisplay === 'choiceCard') {
    return (
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
                      onChange={() => handleValueChange(option.value)}
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
        <FieldErrorMessage error={error} inlineBusinessError={inlineBusinessError} />
        <BusinessErrorsList fieldName={fieldName} messages={remainingBusinessErrors} />
      </Field>
    );
  }

  // Select dropdown for 4 or more options
  if (enumOptions.length >= 4) {
    const selectOptions = enumOptions.map((option) => ({
      value: option.value,
      label: option.label,
    }));

    return (
      <>
        <FieldSelect
          id={fieldName}
          fieldName={fieldName}
          label={
            <PropertyLabel property={property} stagedStatus={stagedStatus} isEnabled={isEnabled} />
          }
          description={property.description}
          options={selectOptions}
          value={field.value || ''}
          onValueChange={handleValueChange}
          placeholder="Select an option..."
          disabled={!isEnabled}
          fieldClassName="space-y-2"
          triggerClassName={cn('w-full', commonClassName)}
          selectClassName="w-[var(--radix-select-trigger-width)]"
          message={error ? String(error.message ?? '') : inlineBusinessError}
        />
        <BusinessErrorsList fieldName={fieldName} messages={remainingBusinessErrors} />
      </>
    );
  }

  // Toggle group for 2-3 options
  return (
    <Field>
      <PropertyLabel property={property} stagedStatus={stagedStatus} isEnabled={isEnabled} />
      <FieldControl>
        <ToggleGroup
          type="single"
          value={field.value || ''}
          onValueChange={handleValueChange}
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
      <FieldErrorMessage error={error} inlineBusinessError={inlineBusinessError} />
      <BusinessErrorsList fieldName={fieldName} messages={remainingBusinessErrors} />
    </Field>
  );
};
