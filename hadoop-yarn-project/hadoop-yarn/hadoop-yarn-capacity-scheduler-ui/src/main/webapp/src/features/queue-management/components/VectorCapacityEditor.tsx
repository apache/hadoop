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
 * Vector capacity editor
 *
 * Edits capacity values as resource vectors (memory, vcores, etc.).
 */

import React from 'react';
import { Plus, X } from 'lucide-react';
import { Button } from '~/components/ui/button';
import { Field, FieldLabel, FieldDescription, FieldMessage } from '~/components/ui/field';
import { Input } from '~/components/ui/input';
import type { CapacityVectorEntryDraft } from '~/stores/slices/capacityEditorSlice';
import type { ValidationIssue } from '~/types';

interface VectorCapacityEditorProps {
  entries: CapacityVectorEntryDraft[];
  target: 'capacity' | 'maxCapacity';
  baseValue: string;
  issues: ValidationIssue[];
  isLegacyMode: boolean;
  onEntryChange: (entryId: string, field: 'key' | 'value', value: string) => void;
  onAddEntry: () => void;
  onRemoveEntry: (entryId: string) => void;
}

export const VectorCapacityEditor: React.FC<VectorCapacityEditorProps> = ({
  entries,
  target,
  baseValue,
  issues,
  isLegacyMode,
  onEntryChange,
  onAddEntry,
  onRemoveEntry,
}) => {
  const headline = target === 'capacity' ? 'Capacity vector' : 'Maximum capacity vector';

  return (
    <Field>
      <FieldLabel className="text-xs uppercase tracking-wide text-muted-foreground">
        {headline}
      </FieldLabel>
      {issues.length === 0 && (
        <>
          <FieldDescription className="text-xs text-muted-foreground">
            {isLegacyMode
              ? 'Legacy mode: enter numeric values per resource.'
              : 'Use numeric values for counts, append w for weights and % for percentages.'}
          </FieldDescription>
          <FieldDescription className="text-[11px] text-muted-foreground">
            Base: {baseValue || '—'}
          </FieldDescription>
        </>
      )}
      <div className="mt-2 space-y-2">
        {entries.map((entry, index) => {
          const isCoreResource = entry.key === 'memory' || entry.key === 'vcores' || index < 2;
          return (
            <div key={entry.id} className="flex items-center gap-2">
              <Input
                value={entry.key}
                onChange={(event) => onEntryChange(entry.id, 'key', event.target.value)}
                placeholder="resource"
                className="h-8 w-32 text-sm"
              />
              <Input
                value={entry.value}
                onChange={(event) => onEntryChange(entry.id, 'value', event.target.value)}
                placeholder={isLegacyMode ? '0' : '0 | 50% | 10w'}
                className="h-8 text-sm"
              />
              {!isCoreResource && (
                <Button
                  type="button"
                  size="icon"
                  variant="ghost"
                  className="h-8 w-8 text-muted-foreground"
                  onClick={() => onRemoveEntry(entry.id)}
                >
                  <X className="h-4 w-4" />
                </Button>
              )}
            </div>
          );
        })}
      </div>
      <div className="mt-2">
        <Button type="button" variant="ghost" size="sm" className="text-xs" onClick={onAddEntry}>
          <Plus className="mr-2 h-3.5 w-3.5" />
          Add resource
        </Button>
      </div>
      {issues.length > 0 && (
        <div className="mt-2 space-y-1">
          {issues.map((issue) => (
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
  );
};
