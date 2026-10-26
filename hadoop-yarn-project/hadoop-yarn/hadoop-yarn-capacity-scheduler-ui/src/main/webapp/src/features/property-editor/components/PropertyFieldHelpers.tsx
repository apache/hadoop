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
 * Property field helper components
 *
 * Shared components for property form fields.
 */

import React from 'react';
import { Info, AlertTriangle, ArrowDown, ArrowUp } from 'lucide-react';
import type { InheritedValueInfo } from '~/utils/resolveInheritedValue';
import { Badge } from '~/components/ui/badge';
import { FieldLabel, FieldMessage } from '~/components/ui/field';
import { Tooltip, TooltipContent, TooltipTrigger } from '~/components/ui/tooltip';
import { cn } from '~/utils/cn';
import type { PropertyDescriptor } from '~/types/property-descriptor';

export interface PropertyLabelProps {
  property: PropertyDescriptor;
  stagedStatus?: 'new' | 'modified' | 'deleted';
  isEnabled: boolean;
  className?: string;
  contentClassName?: string;
  children?: React.ReactNode;
}

export const PropertyLabel: React.FC<PropertyLabelProps> = ({
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

export interface BusinessErrorsListProps {
  fieldName: string;
  messages: string[];
}

export const BusinessErrorsList: React.FC<BusinessErrorsListProps> = ({ fieldName, messages }) => {
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

export interface PropertyWarningsProps {
  warnings: string[];
}

export const PropertyWarnings: React.FC<PropertyWarningsProps> = ({ warnings }) => {
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

export interface FieldErrorMessageProps {
  error?: { message?: string };
  inlineBusinessError?: string;
}

export const FieldErrorMessage: React.FC<FieldErrorMessageProps> = ({
  error,
  inlineBusinessError,
}) => {
  if (!error && !inlineBusinessError) {
    return null;
  }
  return <FieldMessage>{error ? String(error.message ?? '') : inlineBusinessError}</FieldMessage>;
};

export interface InheritedValueIndicatorProps {
  inheritanceInfo: InheritedValueInfo | null;
  hasExplicitValue?: boolean;
}

export const InheritedValueIndicator: React.FC<InheritedValueIndicatorProps> = ({
  inheritanceInfo,
  hasExplicitValue = false,
}) => {
  if (!inheritanceInfo) return null;

  const sourceLabel =
    inheritanceInfo.source === 'queue'
      ? inheritanceInfo.sourcePath
      : 'global default';

  const scaledSuffix = inheritanceInfo.isScaled ? ' (scaled by queue capacity)' : '';

  if (hasExplicitValue) {
    return (
      <div
        className={cn(
          'mt-1 flex items-center gap-1.5 rounded-sm border-l-2 px-2 py-1 text-xs',
          'border-l-amber-500 bg-amber-500/10 text-amber-700 dark:text-amber-400',
        )}
      >
        <ArrowUp className="h-3 w-3 shrink-0" />
        <span>
          Overrides {sourceLabel}:{' '}
          <span className="font-medium">{inheritanceInfo.value}</span>
          {scaledSuffix}
        </span>
      </div>
    );
  }

  return (
    <div
      className={cn(
        'mt-1 flex items-center gap-1.5 rounded-sm border-l-2 px-2 py-1 text-xs',
        'border-l-blue-500 bg-blue-500/10 text-blue-700 dark:text-blue-400',
      )}
    >
      <ArrowDown className="h-3 w-3 shrink-0" />
      <span>
        <span className="font-medium">{inheritanceInfo.value}</span>
        {' '}&mdash; inherited from {sourceLabel}
        {scaledSuffix}
      </span>
    </div>
  );
};
