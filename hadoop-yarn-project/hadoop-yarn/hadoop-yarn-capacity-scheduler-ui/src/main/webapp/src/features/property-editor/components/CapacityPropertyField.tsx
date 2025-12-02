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
 * Capacity property field
 *
 * Displays capacity and maximum-capacity fields with a button to open the capacity editor.
 */

import React from 'react';
import type { FieldError } from 'react-hook-form';
import { Button } from '~/components/ui/button';
import { Field } from '~/components/ui/field';
import type { PropertyDescriptor } from '~/types/property-descriptor';
import { useCapacityEditor } from '~/features/queue-management/hooks/useCapacityEditor';
import {
  PropertyLabel,
  PropertyWarnings,
  BusinessErrorsList,
  FieldErrorMessage,
} from './PropertyFieldHelpers';

interface CapacityPropertyFieldProps {
  property: PropertyDescriptor;
  value: string;
  error?: FieldError;
  stagedStatus?: 'new' | 'modified' | 'deleted';
  isEnabled: boolean;
  inlineBusinessError?: string;
  remainingBusinessErrors: string[];
  warnings: string[];
  queuePath?: string;
  queueName?: string;
  parentQueuePath?: string;
  capacityValue: string;
  maxCapacityValue: string;
}

export const CapacityPropertyField: React.FC<CapacityPropertyFieldProps> = ({
  property,
  value,
  error,
  stagedStatus,
  isEnabled,
  inlineBusinessError,
  remainingBusinessErrors,
  warnings,
  queuePath,
  queueName,
  parentQueuePath,
  capacityValue,
  maxCapacityValue,
}) => {
  const fieldName = property.formFieldName || property.name;
  const { openCapacityEditor } = useCapacityEditor();
  const isCapacityField = property.name === 'capacity';
  const displayValue = value || 'Not set';

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
      capacityValue,
      maxCapacityValue,
    });
  };

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
              Edit Capacity
            </Button>
          ) : (
            <span className="text-xs text-muted-foreground">Managed in Capacity Editor</span>
          )}
        </div>
      </PropertyLabel>
      <div className="mt-2 w-full break-all rounded-md border border-dashed bg-muted/40 px-3 py-2 text-sm font-mono text-foreground">
        {displayValue}
      </div>
      <FieldErrorMessage error={error} inlineBusinessError={inlineBusinessError} />
      <BusinessErrorsList fieldName={fieldName} messages={remainingBusinessErrors} />
      <PropertyWarnings warnings={warnings} />
    </Field>
  );
};
