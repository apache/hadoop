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
 * Capacity row editor
 *
 * Edits capacity values for a single queue row.
 */

import React from 'react';
import { Badge } from '~/components/ui/badge';
import { ToggleGroup, ToggleGroupItem } from '~/components/ui/toggle-group';
import { Input } from '~/components/ui/input';
import {
  Field,
  FieldLabel,
  FieldDescription,
  FieldControl,
  FieldMessage,
} from '~/components/ui/field';
import { VectorCapacityEditor } from './VectorCapacityEditor';
import type { CapacityResourceMode, CapacityRowDraft } from '~/stores/slices/capacityEditorSlice';
import type { ValidationIssue } from '~/types';
import { cn } from '~/utils/cn';

interface CapacityRowEditorProps {
  row: CapacityRowDraft;
  capacityIssues: ValidationIssue[];
  maxCapacityIssues: ValidationIssue[];
  isLegacyMode: boolean;
  onModeChange: (mode: CapacityResourceMode) => void;
  onCapacityChange: (value: string) => void;
  onMaxCapacityChange: (value: string) => void;
  onVectorEntryChange: (
    target: 'capacity' | 'maxCapacity',
    entryId: string,
    field: 'key' | 'value',
    value: string,
  ) => void;
  onAddVectorEntry: (target: 'capacity' | 'maxCapacity') => void;
  onRemoveVectorEntry: (target: 'capacity' | 'maxCapacity', entryId: string) => void;
}

export const CapacityRowEditor: React.FC<CapacityRowEditorProps> = ({
  row,
  capacityIssues,
  maxCapacityIssues,
  isLegacyMode,
  onModeChange,
  onCapacityChange,
  onMaxCapacityChange,
  onVectorEntryChange,
  onAddVectorEntry,
  onRemoveVectorEntry,
}) => {
  const hasRowError =
    capacityIssues.some((issue) => issue.severity === 'error') ||
    maxCapacityIssues.some((issue) => issue.severity === 'error');

  return (
    <div
      className={cn(
        'rounded-md border p-4 transition',
        row.isOrigin && 'border-primary/70 bg-primary/5',
        row.hasStagedChange && !row.isOrigin && 'border-amber-500/60 bg-amber-50/60',
        row.isNew && !row.isOrigin && 'border-dashed',
        hasRowError && 'border-destructive/70 bg-destructive/5',
      )}
    >
      <div className="flex flex-wrap items-start justify-between gap-3">
        <div className="space-y-1">
          <div className="flex flex-wrap items-center gap-2">
            <span className="text-sm font-medium text-foreground">{row.queueName}</span>
            {row.isOrigin && (
              <Badge variant="secondary" className="h-4 px-1 text-[10px]">
                Active queue
              </Badge>
            )}
            {row.hasStagedChange && !row.isOrigin && (
              <Badge variant="outline" className="h-4 px-1 text-[10px]">
                Staged
              </Badge>
            )}
            {row.isNew && !row.isOrigin && (
              <Badge variant="secondary" className="h-4 px-1 text-[10px]">
                New
              </Badge>
            )}
          </div>
          <p className="break-all text-[11px] text-muted-foreground">{row.queuePath}</p>
        </div>

        <ToggleGroup
          type="single"
          value={row.mode}
          onValueChange={(value) => onModeChange(value as CapacityResourceMode)}
          className="shrink-0"
          variant="outline"
        >
          <ToggleGroupItem value="simple" className="text-xs px-3 py-1.5">
            Simple value
          </ToggleGroupItem>
          <ToggleGroupItem value="vector" className="text-xs px-3 py-1.5">
            Resource vector
          </ToggleGroupItem>
        </ToggleGroup>
      </div>

      {row.mode === 'simple' ? (
        <div className="mt-4 grid gap-3 md:grid-cols-2">
          <Field>
            <FieldLabel className="text-xs uppercase tracking-wide text-muted-foreground">
              Capacity
            </FieldLabel>
            <FieldControl>
              <Input
                value={row.capacityValue}
                onChange={(event) => onCapacityChange(event.target.value)}
                placeholder="e.g. 50, 10w"
                className="h-8 text-sm"
              />
            </FieldControl>
            {capacityIssues.length === 0 && (
              <>
                <FieldDescription className="text-[11px] text-muted-foreground">
                  Use numbers for percentages (e.g. 50) or append w for weights (e.g. 10w).
                </FieldDescription>
                <FieldDescription className="text-[11px] text-muted-foreground">
                  Base: {row.baseCapacityValue || '—'}
                </FieldDescription>
              </>
            )}
            {capacityIssues.length > 0 && (
              <div className="mt-2 space-y-1">
                {capacityIssues.map((issue) => (
                  <FieldMessage
                    key={`${issue.rule}-${issue.field}`}
                    className={
                      issue.severity === 'error'
                        ? 'text-[11px] text-destructive'
                        : 'text-[11px] text-amber-600'
                    }
                  >
                    {issue.message}
                  </FieldMessage>
                ))}
              </div>
            )}
          </Field>
          <Field>
            <FieldLabel className="text-xs uppercase tracking-wide text-muted-foreground">
              Maximum capacity
            </FieldLabel>
            <FieldControl>
              <Input
                value={row.maxCapacityValue}
                onChange={(event) => onMaxCapacityChange(event.target.value)}
                placeholder="e.g. 100, 20w"
                className="h-8 text-sm"
              />
            </FieldControl>
            {maxCapacityIssues.length === 0 && (
              <>
                <FieldDescription className="text-[11px] text-muted-foreground">
                  Maximum value the queue can reach.
                </FieldDescription>
                <FieldDescription className="text-[11px] text-muted-foreground">
                  Base: {row.baseMaxCapacityValue || '—'}
                </FieldDescription>
              </>
            )}
            {maxCapacityIssues.length > 0 && (
              <div className="mt-2 space-y-1">
                {maxCapacityIssues.map((issue) => (
                  <FieldMessage
                    key={`${issue.rule}-${issue.field}`}
                    className={
                      issue.severity === 'error'
                        ? 'text-[11px] text-destructive'
                        : 'text-[11px] text-amber-600'
                    }
                  >
                    {issue.message}
                  </FieldMessage>
                ))}
              </div>
            )}
          </Field>
        </div>
      ) : (
        <div className="mt-4 space-y-4">
          <VectorCapacityEditor
            entries={row.vectorCapacity}
            target="capacity"
            baseValue={row.baseCapacityValue}
            issues={capacityIssues}
            isLegacyMode={isLegacyMode}
            onEntryChange={(entryId, field, value) =>
              onVectorEntryChange('capacity', entryId, field, value)
            }
            onAddEntry={() => onAddVectorEntry('capacity')}
            onRemoveEntry={(entryId) => onRemoveVectorEntry('capacity', entryId)}
          />
          <VectorCapacityEditor
            entries={row.vectorMaxCapacity}
            target="maxCapacity"
            baseValue={row.baseMaxCapacityValue}
            issues={maxCapacityIssues}
            isLegacyMode={isLegacyMode}
            onEntryChange={(entryId, field, value) =>
              onVectorEntryChange('maxCapacity', entryId, field, value)
            }
            onAddEntry={() => onAddVectorEntry('maxCapacity')}
            onRemoveEntry={(entryId) => onRemoveVectorEntry('maxCapacity', entryId)}
          />
        </div>
      )}
    </div>
  );
};
