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
import { Trash2, AlertTriangle, AlertCircle } from 'lucide-react';
import { cn } from '~/utils/cn';
import { Button } from '~/components/ui/button';
import { Tooltip, TooltipContent, TooltipProvider, TooltipTrigger } from '~/components/ui/tooltip';
import type { StagedChange } from '~/types';
import { SPECIAL_VALUES } from '~/types';
import {
  buildPropertyKey,
  buildGlobalPropertyKey,
  buildNodeLabelPropertyKey,
} from '~/utils/propertyUtils';

interface DiffViewProps {
  change: StagedChange;
  onRevert: () => void;
  timestamp: string;
}

/**
 * Builds the full property key for display
 */
function buildFullPropertyKey(change: StagedChange): string {
  const { queuePath, property, label } = change;

  // Handle global properties
  if (queuePath === SPECIAL_VALUES.GLOBAL_QUEUE_PATH) {
    return buildGlobalPropertyKey(property);
  }

  // Handle node label properties
  if (label) {
    return buildNodeLabelPropertyKey(queuePath, label, property);
  }

  // Handle regular queue properties
  return buildPropertyKey(queuePath, property);
}

/**
 * Formats the property=value string for display
 */
function formatPropertyValue(propertyKey: string, value: string | undefined): string {
  if (value === undefined || value === null || value === '') {
    return `${propertyKey}=(empty)`;
  }
  return `${propertyKey}=${value}`;
}

/**
 * Git-style diff line component
 */
const DiffLine: React.FC<{
  propertyKey: string;
  value: string | undefined;
  type: 'add' | 'remove';
}> = ({ propertyKey, value, type }) => {
  const isAdd = type === 'add';
  const prefix = isAdd ? '+' : '-';
  const formattedLine = formatPropertyValue(propertyKey, value);

  return (
    <div
      className={cn(
        'font-mono text-xs px-3 py-1.5 border-l-2',
        isAdd
          ? 'bg-green-500/10 border-l-green-600 dark:border-l-green-500 text-green-700 dark:text-green-300'
          : 'bg-red-500/10 border-l-red-600 dark:border-l-red-500 text-red-700 dark:text-red-300',
      )}
    >
      <span className="select-none mr-2">{prefix}</span>
      <span className="break-all">{formattedLine}</span>
    </div>
  );
};

export const DiffView: React.FC<DiffViewProps> = ({ change, onRevert, timestamp }) => {
  // Check if this is a queue removal operation (special marker property)
  const isQueueRemoval = change.property === SPECIAL_VALUES.QUEUE_MARKER;
  const propertyKey = !isQueueRemoval ? buildFullPropertyKey(change) : '';

  return (
    <div className="border rounded-md bg-card overflow-hidden">
      {/* Header with revert button and timestamp */}
      <div className="flex items-center justify-between px-3 py-2 border-b bg-muted/30">
        <span className="text-xs text-muted-foreground">{timestamp}</span>
        <TooltipProvider>
          <Tooltip>
            <TooltipTrigger asChild>
              <Button
                variant="ghost"
                size="icon"
                className="h-6 w-6"
                onClick={onRevert}
                aria-label="Revert change"
              >
                <Trash2 className="h-3 w-3" />
              </Button>
            </TooltipTrigger>
            <TooltipContent>
              <p>Revert this change</p>
            </TooltipContent>
          </Tooltip>
        </TooltipProvider>
      </div>

      {/* Diff content */}
      <div className="bg-background">
        {/* Queue removal - always show the special message */}
        {isQueueRemoval && (
          <div className="px-3 py-2 text-sm text-destructive italic bg-destructive/5">
            Queue will be removed
          </div>
        )}

        {/* Regular property changes */}
        {!isQueueRemoval && change.type === 'update' && (
          <>
            <DiffLine propertyKey={propertyKey} value={change.oldValue} type="remove" />
            <DiffLine propertyKey={propertyKey} value={change.newValue} type="add" />
          </>
        )}

        {!isQueueRemoval && change.type === 'add' && (
          <DiffLine propertyKey={propertyKey} value={change.newValue} type="add" />
        )}

        {!isQueueRemoval && change.type === 'remove' && (
          <DiffLine propertyKey={propertyKey} value={change.oldValue} type="remove" />
        )}
      </div>

      {/* Validation errors/warnings */}
      {change.validationErrors && change.validationErrors.length > 0 && (
        <div className="border-t-2 border-dashed border-muted-foreground/20 mt-3">
          <div className="px-3 pt-3 pb-2 space-y-2">
            {change.validationErrors.map((error) => (
              <div
                key={`${error.queuePath}-${error.field}-${error.message}`}
                className={cn(
                  'flex items-start gap-2 text-xs px-3 py-2 rounded-md border',
                  error.severity === 'error'
                    ? 'bg-destructive/10 text-destructive border-destructive/30'
                    : 'bg-amber-500/10 text-amber-700 dark:text-amber-400 border-amber-500/30',
                )}
              >
                {error.severity === 'error' ? (
                  <AlertCircle className="h-3.5 w-3.5 flex-shrink-0 mt-0.5" />
                ) : (
                  <AlertTriangle className="h-3.5 w-3.5 flex-shrink-0 mt-0.5" />
                )}
                <div className="flex-1">
                  <div className="font-medium mb-0.5">
                    {error.severity === 'error' ? 'Validation Error' : 'Warning'}
                  </div>
                  <div>{error.message}</div>
                  {error.queuePath !== change.queuePath && (
                    <div className="text-xs opacity-70 mt-1">Affects: {error.queuePath}</div>
                  )}
                </div>
              </div>
            ))}
          </div>
        </div>
      )}
    </div>
  );
};
