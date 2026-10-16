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
import {
  Select,
  SelectContent,
  SelectItem,
  SelectTrigger,
  SelectValue,
} from '~/components/ui/select';
import {
  Field,
  FieldControl,
  FieldDescription,
  FieldLabel,
  FieldMessage,
  type FieldProps,
} from '~/components/ui/field';
import { cn } from '~/utils/cn';

type SelectBaseProps = React.ComponentProps<typeof Select>;
type FieldLabelProps = React.ComponentPropsWithoutRef<typeof FieldLabel>;
type FieldDescriptionProps = React.ComponentPropsWithoutRef<typeof FieldDescription>;
type FieldMessageProps = React.ComponentPropsWithoutRef<typeof FieldMessage>;

export interface SelectOption {
  value: string;
  label: string;
  description?: string;
}

interface FieldSelectProps extends Omit<SelectBaseProps, 'children'> {
  label?: React.ReactNode;
  description?: React.ReactNode;
  message?: React.ReactNode;
  options: SelectOption[];
  id?: string;
  fieldName?: FieldProps['name'];
  fieldClassName?: string;
  selectClassName?: string;
  triggerClassName?: string;
  labelProps?: FieldLabelProps;
  descriptionProps?: FieldDescriptionProps;
  messageProps?: FieldMessageProps;
  placeholder?: string;
  disabled?: boolean;
}

export const FieldSelect: React.FC<FieldSelectProps> = ({
  label,
  description,
  message,
  options,
  id,
  fieldName,
  fieldClassName,
  selectClassName,
  triggerClassName,
  labelProps,
  descriptionProps,
  messageProps,
  placeholder,
  disabled,
  ...selectProps
}) => {
  return (
    <Field id={id} name={fieldName} className={cn('space-y-2', fieldClassName)}>
      {label ? (
        <FieldLabel
          {...labelProps}
          htmlFor={id}
          className={cn(disabled && 'text-muted-foreground', labelProps?.className)}
        >
          {label}
        </FieldLabel>
      ) : null}
      <Select {...selectProps} disabled={disabled}>
        <FieldControl>
          <SelectTrigger id={id} disabled={disabled} className={cn(triggerClassName)}>
            <SelectValue placeholder={placeholder} />
          </SelectTrigger>
        </FieldControl>
        <SelectContent className={selectClassName}>
          {options.map((option) => (
            <SelectItem key={option.value} value={option.value} disabled={disabled}>
              <div className="flex flex-col gap-1 text-left">
                <span>{option.label}</span>
                {option.description && (
                  <span className="text-xs text-muted-foreground">{option.description}</span>
                )}
              </div>
            </SelectItem>
          ))}
        </SelectContent>
      </Select>
      {description ? (
        <FieldDescription
          {...descriptionProps}
          className={cn('text-xs text-muted-foreground', descriptionProps?.className)}
        >
          {description}
        </FieldDescription>
      ) : null}
      {message ? (
        <FieldMessage {...messageProps} className={cn(messageProps?.className)}>
          {message}
        </FieldMessage>
      ) : null}
    </Field>
  );
};
