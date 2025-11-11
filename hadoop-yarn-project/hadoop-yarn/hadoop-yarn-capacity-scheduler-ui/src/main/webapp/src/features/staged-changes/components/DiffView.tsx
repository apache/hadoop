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
import { Plus, Edit2, Minus, Trash2, AlertTriangle, AlertCircle } from 'lucide-react';
import { cn } from '~/utils/cn';
import { Badge } from '~/components/ui/badge';
import { Button } from '~/components/ui/button';
import { Card, CardContent, CardHeader } from '~/components/ui/card';
import { Tooltip, TooltipContent, TooltipProvider, TooltipTrigger } from '~/components/ui/tooltip';
import type { StagedChange } from '~/types';
import { SPECIAL_VALUES } from '~/types';
import { formatPropertyName, formatAclValue } from '~/utils/formatUtils';

interface DiffViewProps {
  change: StagedChange;
  onRevert: () => void;
  timestamp: string;
}

const getChangeTypeIcon = (type: StagedChange['type']) => {
  switch (type) {
    case 'add':
      return <Plus className="h-3 w-3" />;
    case 'update':
      return <Edit2 className="h-3 w-3" />;
    case 'remove':
      return <Minus className="h-3 w-3" />;
    default:
      return <Edit2 className="h-3 w-3" />;
  }
};

const getChangeTypeVariant = (
  type: StagedChange['type'],
): 'default' | 'secondary' | 'destructive' | 'outline' | 'success' => {
  switch (type) {
    case 'add':
      return 'success';
    case 'update':
      return 'default';
    case 'remove':
      return 'destructive';
    default:
      return 'default';
  }
};

const DiffValue: React.FC<{
  value: string | undefined;
  type: 'old' | 'new';
  changeType: StagedChange['type'];
  propertyName: string;
}> = ({ value, type, changeType, propertyName }) => {
  if (!value && value !== '') return null;

  const isOld = type === 'old';
  const isNew = type === 'new';
  const isAclProperty = propertyName.includes('acl');

  const prefix = changeType === 'add' ? '+ ' : changeType === 'remove' ? '- ' : isOld ? '- ' : '+ ';

  const displayValue = isAclProperty ? formatAclValue(value) : value || '(empty)';

  return (
    <div
      className={cn(
        'px-3 py-1.5 rounded-md border font-mono text-xs flex items-center gap-2',
        'bg-muted/50',
        changeType === 'add' && isNew && 'border-green-500 dark:border-green-700',
        changeType === 'remove' && isOld && 'border-destructive',
        changeType === 'update' && isOld && 'border-destructive',
        changeType === 'update' && isNew && 'border-green-500 dark:border-green-700',
        (changeType === 'remove' || (changeType === 'update' && isOld)) &&
          'line-through opacity-70',
      )}
    >
      <span
        className={cn(
          'font-semibold',
          isOld ? 'text-destructive' : 'text-green-600 dark:text-green-400',
        )}
      >
        {prefix}
      </span>
      <span className="break-all">{displayValue}</span>
    </div>
  );
};

export const DiffView: React.FC<DiffViewProps> = ({ change, onRevert, timestamp }) => {
  return (
    <Card>
      <CardHeader className="p-3">
        <div className="flex items-center justify-between">
          <div className="flex items-center gap-2 flex-1">
            <Badge variant={getChangeTypeVariant(change.type)} className="text-xs h-5">
              {getChangeTypeIcon(change.type)}
              {change.type.toUpperCase()}
            </Badge>
            <span className="font-medium text-sm">{formatPropertyName(change.property)}</span>
            <span className="text-xs text-muted-foreground ml-auto">{timestamp}</span>
          </div>
          <TooltipProvider>
            <Tooltip>
              <TooltipTrigger asChild>
                <Button variant="ghost" size="icon" className="h-7 w-7" onClick={onRevert}>
                  <Trash2 className="h-3.5 w-3.5" />
                </Button>
              </TooltipTrigger>
              <TooltipContent>
                <p>Revert this change</p>
              </TooltipContent>
            </Tooltip>
          </TooltipProvider>
        </div>
      </CardHeader>

      <CardContent className="p-3 pt-0 space-y-2">
        {change.type === 'update' && (
          <>
            <DiffValue
              value={change.oldValue}
              type="old"
              changeType={change.type}
              propertyName={change.property}
            />
            <DiffValue
              value={change.newValue}
              type="new"
              changeType={change.type}
              propertyName={change.property}
            />
          </>
        )}

        {change.type === 'add' && change.newValue && (
          <DiffValue
            value={change.newValue}
            type="new"
            changeType={change.type}
            propertyName={change.property}
          />
        )}

        {change.type === 'remove' && change.oldValue && (
          <DiffValue
            value={change.oldValue}
            type="old"
            changeType={change.type}
            propertyName={change.property}
          />
        )}

        {change.type === 'remove' && !change.oldValue && (
          <p className="text-sm text-destructive italic">Queue will be removed</p>
        )}

        {/* Validation errors/warnings */}
        {change.validationErrors && change.validationErrors.length > 0 && (
          <div className="space-y-1 mt-2">
            {change.validationErrors.map((error) => (
              <div
                key={`${error.queuePath}-${error.field}-${error.message}`}
                className={cn(
                  'flex items-center gap-2 text-xs p-2 rounded-md',
                  error.severity === 'error'
                    ? 'bg-destructive/10 text-destructive'
                    : 'bg-amber-500/10 text-amber-700 dark:text-amber-400',
                )}
              >
                {error.severity === 'error' ? (
                  <AlertCircle className="h-3 w-3 flex-shrink-0" />
                ) : (
                  <AlertTriangle className="h-3 w-3 flex-shrink-0" />
                )}
                <span>{error.message}</span>
              </div>
            ))}
          </div>
        )}
      </CardContent>
    </Card>
  );
};
