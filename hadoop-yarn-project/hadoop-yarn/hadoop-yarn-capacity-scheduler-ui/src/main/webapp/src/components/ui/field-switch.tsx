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
import { Switch } from '~/components/ui/switch';
import {
  Field,
  FieldControl,
  FieldDescription,
  FieldLabel,
  FieldMessage,
  type FieldProps,
} from '~/components/ui/field';
import { cn } from '~/utils/cn';

type SwitchBaseProps = React.ComponentProps<typeof Switch>;
type FieldLabelProps = React.ComponentPropsWithoutRef<typeof FieldLabel>;
type FieldDescriptionProps = React.ComponentPropsWithoutRef<typeof FieldDescription>;
type FieldMessageProps = React.ComponentPropsWithoutRef<typeof FieldMessage>;

interface FieldSwitchProps extends Omit<SwitchBaseProps, 'id' | 'className'> {
  label?: React.ReactNode;
  labelSuffix?: React.ReactNode;
  description?: React.ReactNode;
  message?: React.ReactNode;
  addon?: React.ReactNode;
  id?: string;
  fieldName?: FieldProps['name'];
  fieldClassName?: string;
  wrapperClassName?: string;
  switchWrapperClassName?: string;
  controlClassName?: string;
  switchClassName?: string;
  labelProps?: FieldLabelProps;
  descriptionProps?: FieldDescriptionProps;
  messageProps?: FieldMessageProps;
  align?: 'start' | 'center';
}

export const FieldSwitch: React.FC<FieldSwitchProps> = ({
  label,
  labelSuffix,
  description,
  addon,
  message,
  id,
  fieldName,
  fieldClassName,
  wrapperClassName,
  switchWrapperClassName,
  controlClassName,
  switchClassName,
  labelProps,
  descriptionProps,
  messageProps,
  align = 'center',
  ...switchProps
}) => {
  return (
    <Field id={id} name={fieldName} className={cn('!flex flex-col gap-2', fieldClassName)}>
      <div
        className={cn(
          'flex justify-between gap-4',
          align === 'center' ? 'items-center' : 'items-start',
          wrapperClassName,
        )}
      >
        <div className="flex-1 min-w-0 space-y-1">
          {label ? (
            <FieldLabel
              {...labelProps}
              className={cn('flex items-center gap-2', labelProps?.className)}
            >
              <span className="truncate">{label}</span>
              {labelSuffix ? <span className="shrink-0">{labelSuffix}</span> : null}
            </FieldLabel>
          ) : null}
          {description ? (
            <FieldDescription
              {...descriptionProps}
              className={cn('text-xs text-muted-foreground', descriptionProps?.className)}
            >
              {description}
            </FieldDescription>
          ) : null}
        </div>
        <div
          className={cn(
            'flex items-center gap-2',
            align === 'start' && 'pt-1',
            switchWrapperClassName,
          )}
        >
          {addon}
          <FieldControl className={controlClassName}>
            <Switch {...switchProps} className={switchClassName} />
          </FieldControl>
        </div>
      </div>
      {message ? (
        <FieldMessage {...messageProps} className={cn(messageProps?.className)}>
          {message}
        </FieldMessage>
      ) : null}
    </Field>
  );
};
