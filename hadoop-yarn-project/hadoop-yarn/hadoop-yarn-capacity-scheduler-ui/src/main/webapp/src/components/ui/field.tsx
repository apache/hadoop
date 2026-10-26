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


'use client';

import * as React from 'react';
import * as LabelPrimitive from '@radix-ui/react-label';
import { Slot } from '@radix-ui/react-slot';

import { cn } from '~/utils/cn';
import { Label } from '~/components/ui/label';

type FieldContextValue = {
  id: string;
  name?: string;
  formItemId: string;
  formDescriptionId: string;
  formMessageId: string;
};

const FieldContext = React.createContext<FieldContextValue | undefined>(undefined);

export const useFieldContext = () => {
  const context = React.useContext(FieldContext);

  if (!context) {
    throw new Error('useFieldContext must be used within a <Field>');
  }

  return context;
};

export interface FieldProps extends React.HTMLAttributes<HTMLDivElement> {
  name?: string;
}

export const Field = React.forwardRef<HTMLDivElement, FieldProps>(
  ({ name, id: idProp, className, ...props }, ref) => {
    const id = React.useId();
    const fieldId = idProp ?? `${id}-field`;

    const contextValue = React.useMemo<FieldContextValue>(
      () => ({
        id: fieldId,
        name,
        formItemId: fieldId,
        formDescriptionId: `${fieldId}-description`,
        formMessageId: `${fieldId}-message`,
      }),
      [fieldId, name],
    );

    return (
      <FieldContext.Provider value={contextValue}>
        <div ref={ref} data-slot="field" className={cn('grid gap-2', className)} {...props} />
      </FieldContext.Provider>
    );
  },
);
Field.displayName = 'Field';

export const Fieldset = React.forwardRef<
  HTMLFieldSetElement,
  React.HTMLAttributes<HTMLFieldSetElement>
>(({ className, ...props }, ref) => (
  <fieldset
    ref={ref}
    data-slot="fieldset"
    className={cn('grid gap-4 rounded-lg border p-4', className)}
    {...props}
  />
));
Fieldset.displayName = 'Fieldset';

export const FieldsetLegend = React.forwardRef<
  HTMLLegendElement,
  React.HTMLAttributes<HTMLLegendElement>
>(({ className, ...props }, ref) => (
  <legend
    ref={ref}
    data-slot="fieldset-legend"
    className={cn('text-sm font-medium', className)}
    {...props}
  />
));
FieldsetLegend.displayName = 'FieldsetLegend';

export const FieldLabel = React.forwardRef<
  React.ElementRef<typeof LabelPrimitive.Root>,
  React.ComponentPropsWithoutRef<typeof LabelPrimitive.Root>
>(({ className, ...props }, ref) => {
  const { formItemId } = useFieldContext();

  return (
    <Label
      ref={ref}
      data-slot="field-label"
      className={className}
      htmlFor={props.htmlFor ?? formItemId}
      {...props}
    />
  );
});
FieldLabel.displayName = 'FieldLabel';

export const FieldControl = React.forwardRef<
  React.ElementRef<typeof Slot>,
  React.ComponentPropsWithoutRef<typeof Slot>
>(({ className, ...props }, ref) => {
  const { formItemId, formDescriptionId, formMessageId } = useFieldContext();

  return (
    <Slot
      ref={ref}
      data-slot="field-control"
      id={formItemId}
      aria-describedby={[formDescriptionId, formMessageId].join(' ')}
      className={className}
      {...props}
    />
  );
});
FieldControl.displayName = 'FieldControl';

export const FieldDescription = React.forwardRef<
  HTMLParagraphElement,
  React.HTMLAttributes<HTMLParagraphElement>
>(({ className, ...props }, ref) => {
  const { formDescriptionId } = useFieldContext();

  return (
    <p
      ref={ref}
      data-slot="field-description"
      id={formDescriptionId}
      className={cn('text-sm text-muted-foreground', className)}
      {...props}
    />
  );
});
FieldDescription.displayName = 'FieldDescription';

export const FieldMessage = React.forwardRef<
  HTMLParagraphElement,
  React.HTMLAttributes<HTMLParagraphElement>
>(({ className, children, ...props }, ref) => {
  const { formMessageId } = useFieldContext();

  if (!children) {
    return null;
  }

  return (
    <p
      ref={ref}
      data-slot="field-message"
      id={formMessageId}
      className={cn('text-sm text-destructive', className)}
      {...props}
    >
      {children}
    </p>
  );
});
FieldMessage.displayName = 'FieldMessage';
