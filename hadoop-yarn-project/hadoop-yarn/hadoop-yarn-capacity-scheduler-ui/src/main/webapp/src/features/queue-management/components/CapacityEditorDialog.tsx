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
import { Loader2, Plus, X } from 'lucide-react';
import {
  Dialog,
  DialogContent,
  DialogHeader,
  DialogTitle,
  DialogDescription,
} from '~/components/ui/dialog';
import { Button } from '~/components/ui/button';
import { Kbd } from '~/components/ui/kbd';
import {
  Field,
  FieldLabel,
  FieldDescription,
  FieldControl,
  FieldMessage,
} from '~/components/ui/field';
import { Badge } from '~/components/ui/badge';
import { ToggleGroup, ToggleGroupItem } from '~/components/ui/toggle-group';
import { Input } from '~/components/ui/input';
import { ScrollArea } from '~/components/ui/scroll-area';
import {
  Select,
  SelectContent,
  SelectItem,
  SelectTrigger,
  SelectValue,
} from '~/components/ui/select';
import { useSchedulerStore } from '~/stores/schedulerStore';
import type { SchedulerStore } from '~/stores/schedulerStore';
import { cn } from '~/utils/cn';
import { useKeyboardShortcuts, getModifierKey } from '~/hooks/useKeyboardShortcuts';
import {
  DEFAULT_PARTITION_VALUE,
  createEmptyVectorEntry,
  convertVectorDraftToString,
  ensureCoreEntries,
  parseVectorDraft,
} from '~/features/queue-management/utils/capacityEditor';
import type { CapacityResourceMode, CapacityRowDraft } from '~/stores/slices/capacityEditorSlice';
import { SPECIAL_VALUES } from '~/types';
import type { QueueCapacitiesByPartition } from '~/types';
import { parseCapacityValue } from '~/utils/capacityUtils';
import type { ValidationIssue } from '~/types';

type VectorTarget = 'capacity' | 'maxCapacity';

const SUPPORTED_ABSOLUTE_RESOURCES = ['memory', 'vcores'] as const;
type SupportedAbsoluteResource = (typeof SUPPORTED_ABSOLUTE_RESOURCES)[number];

type RemainingHelper =
  | {
      kind: 'percentage-legacy';
      remaining: number;
      target: number;
      isOverOrUnder: boolean;
    }
  | {
      kind: 'weight-legacy';
      sum: number;
    }
  | {
      kind: 'absolute-legacy';
      resources: Array<{
        resource: SupportedAbsoluteResource;
        allocated: number;
        remaining: number;
        total: number;
      }>;
    };

const formatNumber = (value: number): string => {
  if (!Number.isFinite(value)) {
    return '0';
  }
  const rounded = Math.round((value + Number.EPSILON) * 100) / 100;
  const formatted = rounded.toString();
  if (formatted.includes('.')) {
    return formatted.replace(/\.0+$/, '').replace(/(\.\d*[1-9])0+$/, '$1');
  }
  return formatted;
};

const computeRemainingHelper = (
  rows: CapacityRowDraft[],
  parentCapacityValue: string,
  isLegacyMode: boolean,
  parentQueuePath: string | null,
  selectedNodeLabel: string | null,
  getQueuePartitionCapacities: (
    path: string,
    partition: string,
  ) => QueueCapacitiesByPartition | null,
): RemainingHelper | null => {
  if (rows.length === 0) {
    return null;
  }

  const allSimple = rows.every((row) => row.mode === 'simple');
  const allVector = rows.every((row) => row.mode === 'vector');

  // Legacy Mode
  if (isLegacyMode) {
    if (allSimple) {
      let determinedType: 'percentage' | 'weight' | null = null;
      let currentTotal = 0;

      for (const row of rows) {
        const currentParsed = parseCapacityValue(row.capacityValue);
        const baseParsed = parseCapacityValue(row.baseCapacityValue);

        const candidateType =
          currentParsed && (currentParsed.type === 'percentage' || currentParsed.type === 'weight')
            ? currentParsed.type
            : baseParsed && (baseParsed.type === 'percentage' || baseParsed.type === 'weight')
              ? baseParsed.type
              : null;

        if (!candidateType) {
          return null;
        }

        if (!determinedType) {
          determinedType = candidateType;
        } else if (determinedType !== candidateType) {
          return null;
        }

        if (currentParsed?.type === determinedType) {
          currentTotal += currentParsed.value;
        }
      }

      if (!determinedType) {
        return null;
      }

      if (determinedType === 'percentage') {
        const target = 100;
        const remaining = target - currentTotal;
        return {
          kind: 'percentage-legacy',
          remaining,
          target,
          isOverOrUnder: remaining !== 0,
        };
      }

      if (determinedType === 'weight') {
        return {
          kind: 'weight-legacy',
          sum: currentTotal,
        };
      }
    }

    if (allVector) {
      // Legacy mode with absolute resources: show remaining capacity
      if (!parentQueuePath) {
        return null;
      }

      const partitionName = selectedNodeLabel || '';
      const partition = getQueuePartitionCapacities(parentQueuePath, partitionName);

      if (!partition) {
        return null;
      }

      // Determine if parent is root to choose the appropriate total
      const isParentRoot = parentQueuePath === SPECIAL_VALUES.ROOT_QUEUE_NAME;

      // Calculate allocated resources from all rows
      const allocatedResources = new Map<string, number>();
      rows.forEach((row) => {
        row.vectorCapacity.forEach(({ key, value }) => {
          if (key.trim().length === 0) {
            return;
          }
          const numeric = Number.parseFloat(value);
          if (!Number.isNaN(numeric)) {
            const current = allocatedResources.get(key) ?? 0;
            allocatedResources.set(key, current + numeric);
          }
        });
      });

      const resources: Array<{
        resource: SupportedAbsoluteResource;
        allocated: number;
        remaining: number;
        total: number;
      }> = [];

      SUPPORTED_ABSOLUTE_RESOURCES.forEach((resource) => {
        const resourceKey = resource === 'vcores' ? 'vCores' : resource;

        // For root, use effectiveMaxResource; for non-root, use configuredMinResource
        const total = isParentRoot
          ? (partition.effectiveMaxResource?.[resourceKey] ?? 0)
          : (partition.configuredMinResource?.[resourceKey] ?? 0);

        const allocated = allocatedResources.get(resource) ?? 0;
        const remaining = total - allocated;

        // Only include resources that have a total or allocation
        if (total > 0 || allocated > 0) {
          resources.push({
            resource,
            allocated,
            remaining,
            total,
          });
        }
      });

      if (resources.length === 0) {
        return null;
      }

      return {
        kind: 'absolute-legacy',
        resources,
      };
    }

    return null;
  }

  // Non-Legacy Mode: no strict validation rules, don't show helper
  return null;
};

const useLegacyMode = (): boolean => {
  return useSchedulerStore(
    (state: SchedulerStore) =>
      state.getGlobalPropertyValue(SPECIAL_VALUES.LEGACY_MODE_PROPERTY).value?.toLowerCase() !==
      'false',
  );
};

export const CapacityEditorDialog: React.FC = () => {
  const isOpen = useSchedulerStore((state) => state.capacityEditor.isOpen);
  const drafts = useSchedulerStore((state) => state.capacityEditor.drafts);
  const draftOrder = useSchedulerStore((state) => state.capacityEditor.draftOrder);
  const parentQueuePath = useSchedulerStore((state) => state.capacityEditor.parentQueuePath);
  const selectedNodeLabel = useSchedulerStore((state) => state.capacityEditor.selectedNodeLabel);
  const labelOptions = useSchedulerStore((state) => state.capacityEditor.labelOptions);
  const labelsWithoutAccess = useSchedulerStore(
    (state) => state.capacityEditor.labelsWithoutAccess,
  );
  const validationIssues = useSchedulerStore((state) => state.capacityEditor.validationIssues);
  const isSaving = useSchedulerStore((state) => state.capacityEditor.isSaving);
  const saveError = useSchedulerStore((state) => state.capacityEditor.saveError);

  const closeCapacityEditor = useSchedulerStore((state) => state.closeCapacityEditor);
  const updateCapacityDraft = useSchedulerStore((state) => state.updateCapacityDraft);
  const setCapacityEditorLabel = useSchedulerStore((state) => state.setCapacityEditorLabel);
  const resetCapacityDrafts = useSchedulerStore((state) => state.resetCapacityDrafts);
  const saveCapacityDrafts = useSchedulerStore((state) => state.saveCapacityDrafts);
  const isLegacyMode = useLegacyMode();

  const parentCapacityValue = useSchedulerStore((state) => {
    const parentPath = state.capacityEditor.parentQueuePath;
    const label = state.capacityEditor.selectedNodeLabel;
    if (!parentPath) {
      return '';
    }

    const capacityProperty = label ? `accessible-node-labels.${label}.capacity` : 'capacity';

    return state.getQueuePropertyValue(parentPath, capacityProperty).value;
  });

  const getQueuePartitionCapacities = useSchedulerStore(
    (state) => state.getQueuePartitionCapacities,
  );

  const rows = draftOrder
    .map((queuePath) => drafts[queuePath])
    .filter((row): row is CapacityRowDraft => Boolean(row));

  const remainingHelper = computeRemainingHelper(
    rows,
    parentCapacityValue,
    isLegacyMode,
    parentQueuePath,
    selectedNodeLabel,
    getQueuePartitionCapacities,
  );

  const hasBlockingIssues = validationIssues.some((issue) => issue.severity === 'error');

  const handleSave = async (force: boolean) => {
    const success = await saveCapacityDrafts({ force });
    if (success) {
      closeCapacityEditor();
    }
  };

  const handleModeChange = (queuePath: string, mode: CapacityResourceMode) => {
    updateCapacityDraft(queuePath, (draft) => {
      if (draft.mode === mode) {
        return;
      }

      if (mode === 'simple') {
        draft.capacityValue = convertVectorDraftToString(draft.vectorCapacity);
        draft.maxCapacityValue = convertVectorDraftToString(draft.vectorMaxCapacity);
        draft.vectorCapacity = [];
        draft.vectorMaxCapacity = [];
        draft.mode = 'simple';
        return;
      }

      const nextCapacityVector = ensureCoreEntries(parseVectorDraft(draft.capacityValue), true);
      const nextMaxVector = ensureCoreEntries(parseVectorDraft(draft.maxCapacityValue), true);

      draft.vectorCapacity = nextCapacityVector;
      draft.vectorMaxCapacity = nextMaxVector;
      draft.mode = 'vector';
    });
  };

  const handleVectorEntryChange = (
    queuePath: string,
    target: VectorTarget,
    entryId: string,
    field: 'key' | 'value',
    value: string,
  ) => {
    updateCapacityDraft(queuePath, (draft) => {
      const key = target === 'capacity' ? 'vectorCapacity' : 'vectorMaxCapacity';
      const entries = draft[key];
      const index = entries.findIndex((entry) => entry.id === entryId);
      if (index === -1) {
        return;
      }
      entries[index] = {
        ...entries[index],
        [field]: value,
      };
    });
  };

  const handleAddVectorEntry = (queuePath: string, target: VectorTarget) => {
    updateCapacityDraft(queuePath, (draft) => {
      const key = target === 'capacity' ? 'vectorCapacity' : 'vectorMaxCapacity';
      draft[key] = [...draft[key], createEmptyVectorEntry()];
    });
  };

  const handleRemoveVectorEntry = (queuePath: string, target: VectorTarget, entryId: string) => {
    updateCapacityDraft(queuePath, (draft) => {
      const key = target === 'capacity' ? 'vectorCapacity' : 'vectorMaxCapacity';
      draft[key] = draft[key].filter((entry) => entry.id !== entryId);
      if (draft[key].length === 0) {
        draft[key] = ensureCoreEntries([], true);
      }
    });
  };

  // Keyboard shortcuts
  useKeyboardShortcuts(
    isOpen
      ? [
          {
            key: 's',
            ctrl: true,
            meta: true,
            preventDefault: true,
            handler: () => {
              if (!isSaving) {
                void handleSave(false);
              }
            },
          },
          {
            key: 'k',
            ctrl: true,
            meta: true,
            preventDefault: true,
            handler: () => {
              if (!isSaving) {
                resetCapacityDrafts();
              }
            },
          },
        ]
      : [],
  );

  const selectValue = selectedNodeLabel ?? DEFAULT_PARTITION_VALUE;

  const renderVectorEntries = (
    row: CapacityRowDraft,
    target: VectorTarget,
    issues: ValidationIssue[],
  ) => {
    const key = target === 'capacity' ? 'vectorCapacity' : 'vectorMaxCapacity';
    const entries = row[key];
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
              Base:{' '}
              {target === 'capacity'
                ? row.baseCapacityValue || '—'
                : row.baseMaxCapacityValue || '—'}
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
                  onChange={(event) =>
                    handleVectorEntryChange(
                      row.queuePath,
                      target,
                      entry.id,
                      'key',
                      event.target.value,
                    )
                  }
                  placeholder="resource"
                  className="h-8 w-32 text-sm"
                />
                <Input
                  value={entry.value}
                  onChange={(event) =>
                    handleVectorEntryChange(
                      row.queuePath,
                      target,
                      entry.id,
                      'value',
                      event.target.value,
                    )
                  }
                  placeholder={isLegacyMode ? '0' : '0 | 50% | 10w'}
                  className="h-8 text-sm"
                />
                {!isCoreResource && (
                  <Button
                    type="button"
                    size="icon"
                    variant="ghost"
                    className="h-8 w-8 text-muted-foreground"
                    onClick={() => handleRemoveVectorEntry(row.queuePath, target, entry.id)}
                  >
                    <X className="h-4 w-4" />
                  </Button>
                )}
              </div>
            );
          })}
        </div>
        <div className="mt-2">
          <Button
            type="button"
            variant="ghost"
            size="sm"
            className="text-xs"
            onClick={() => handleAddVectorEntry(row.queuePath, target)}
          >
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

  if (!isOpen) {
    return null;
  }

  return (
    <Dialog open={isOpen} onOpenChange={(open) => !open && closeCapacityEditor()}>
      <DialogContent className="max-w-6xl sm:max-w-6xl w-[960px]">
        <DialogHeader>
          <DialogTitle>Capacity Editor</DialogTitle>
          <DialogDescription>
            Adjust capacities for queues under{' '}
            <strong className="font-medium">{parentQueuePath ?? 'selected parent'}</strong>
          </DialogDescription>
          <div className="mt-3 flex flex-wrap items-center gap-3">
            <Field className="min-w-[220px]">
              <FieldLabel className="text-xs uppercase tracking-wide text-muted-foreground">
                Node label
              </FieldLabel>
              <Select
                value={selectValue}
                onValueChange={(value) =>
                  setCapacityEditorLabel(value === DEFAULT_PARTITION_VALUE ? null : value)
                }
              >
                <FieldControl>
                  <SelectTrigger className="h-8">
                    <SelectValue placeholder="Default partition" />
                  </SelectTrigger>
                </FieldControl>
                <SelectContent>
                  {labelOptions.map((option) => (
                    <SelectItem key={option.value} value={option.value}>
                      {option.label}
                    </SelectItem>
                  ))}
                </SelectContent>
              </Select>
              {selectedNodeLabel && labelsWithoutAccess.has(selectedNodeLabel) && (
                <FieldDescription className="text-xs text-amber-600 mt-1">
                  Warning: This queue doesn't have access to the {selectedNodeLabel} label. You can
                  edit capacities to remove them.
                </FieldDescription>
              )}
            </Field>
          </div>

          {remainingHelper && (
            <div
              className={cn(
                'mt-3 rounded-md border border-dashed px-3 py-2 text-xs text-left space-y-1',
                (remainingHelper.kind === 'percentage-legacy' && remainingHelper.isOverOrUnder) ||
                  (remainingHelper.kind === 'absolute-legacy' &&
                    remainingHelper.resources.some((r) => r.remaining < 0))
                  ? 'bg-amber-50/60 border-amber-500/60 text-amber-900'
                  : 'bg-muted/40 text-muted-foreground',
              )}
            >
              {remainingHelper.kind === 'percentage-legacy' && (
                <p>
                  {remainingHelper.remaining >= 0
                    ? `${formatNumber(remainingHelper.remaining)}% capacity remaining`
                    : `${formatNumber(Math.abs(remainingHelper.remaining))}% over target`}{' '}
                  (target {formatNumber(remainingHelper.target)}%)
                </p>
              )}
              {remainingHelper.kind === 'weight-legacy' && (
                <p>Sum of weights: {formatNumber(remainingHelper.sum)}</p>
              )}
              {remainingHelper.kind === 'absolute-legacy' &&
                remainingHelper.resources.map((resource) => (
                  <p key={resource.resource}>
                    {resource.resource}:{' '}
                    {resource.remaining >= 0
                      ? `${formatNumber(resource.remaining)} remaining`
                      : `${formatNumber(Math.abs(resource.remaining))} over target`}{' '}
                    (total {formatNumber(resource.total)})
                  </p>
                ))}
            </div>
          )}
        </DialogHeader>

        <ScrollArea className="max-h-[60vh] pr-3">
          <div className="space-y-4 pb-4">
            {rows.map((row) => {
              const capacityFieldName = selectedNodeLabel
                ? `accessible-node-labels.${selectedNodeLabel}.capacity`
                : 'capacity';
              const maxFieldName = selectedNodeLabel
                ? `accessible-node-labels.${selectedNodeLabel}.maximum-capacity`
                : 'maximum-capacity';

              const capacityIssuesForRow = validationIssues.filter(
                (issue) => issue.queuePath === row.queuePath && issue.field === capacityFieldName,
              );
              const maxIssuesForRow = validationIssues.filter(
                (issue) => issue.queuePath === row.queuePath && issue.field === maxFieldName,
              );

              const hasRowError =
                capacityIssuesForRow.some((issue) => issue.severity === 'error') ||
                maxIssuesForRow.some((issue) => issue.severity === 'error');

              return (
                <div
                  key={row.queuePath}
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
                      onValueChange={(value) =>
                        handleModeChange(row.queuePath, value as CapacityResourceMode)
                      }
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
                            onChange={(event) => {
                              updateCapacityDraft(row.queuePath, (draft) => {
                                draft.capacityValue = event.target.value;
                              });
                            }}
                            placeholder="e.g. 50, 10w"
                            className="h-8 text-sm"
                          />
                        </FieldControl>
                        {capacityIssuesForRow.length === 0 && (
                          <>
                            <FieldDescription className="text-[11px] text-muted-foreground">
                              {`Use numbers for percentages (e.g. 50) or append w for weights (e.g. 10w).`}
                            </FieldDescription>
                            <FieldDescription className="text-[11px] text-muted-foreground">
                              Base: {row.baseCapacityValue || '—'}
                            </FieldDescription>
                          </>
                        )}
                        {capacityIssuesForRow.length > 0 && (
                          <div className="mt-2 space-y-1">
                            {capacityIssuesForRow.map((issue) => (
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
                            onChange={(event) => {
                              updateCapacityDraft(row.queuePath, (draft) => {
                                draft.maxCapacityValue = event.target.value;
                              });
                            }}
                            placeholder="e.g. 100, 20w"
                            className="h-8 text-sm"
                          />
                        </FieldControl>
                        {maxIssuesForRow.length === 0 && (
                          <>
                            <FieldDescription className="text-[11px] text-muted-foreground">
                              {`Maximum value the queue can reach.`}
                            </FieldDescription>
                            <FieldDescription className="text-[11px] text-muted-foreground">
                              Base: {row.baseMaxCapacityValue || '—'}
                            </FieldDescription>
                          </>
                        )}
                        {maxIssuesForRow.length > 0 && (
                          <div className="mt-2 space-y-1">
                            {maxIssuesForRow.map((issue) => (
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
                      {renderVectorEntries(row, 'capacity', capacityIssuesForRow)}
                      {renderVectorEntries(row, 'maxCapacity', maxIssuesForRow)}
                    </div>
                  )}
                </div>
              );
            })}
          </div>
        </ScrollArea>

        <div className="flex flex-wrap justify-end gap-3 border-t border-border pt-4">
          <div className="flex items-center gap-2">
            <Button
              type="button"
              variant="ghost"
              size="sm"
              className="text-xs"
              onClick={() => resetCapacityDrafts()}
              disabled={isSaving}
            >
              Reset
              <Kbd className="ml-auto">{getModifierKey()}+K</Kbd>
            </Button>
            <Button
              type="button"
              variant="outline"
              size="sm"
              className="text-xs"
              onClick={() => {
                void handleSave(true);
              }}
              disabled={isSaving || !hasBlockingIssues}
            >
              Stage anyway
            </Button>
            <Button
              type="button"
              size="sm"
              className="text-xs"
              onClick={() => {
                void handleSave(false);
              }}
              disabled={isSaving}
            >
              {isSaving ? (
                <>
                  <Loader2 className="mr-2 h-3.5 w-3.5 animate-spin" />
                  Saving…
                </>
              ) : (
                <>
                  Save changes
                  <Kbd className="ml-auto">{getModifierKey()}+S</Kbd>
                </>
              )}
            </Button>
          </div>
        </div>
        {saveError && <p className="mt-2 text-xs text-destructive">{saveError}</p>}
      </DialogContent>
    </Dialog>
  );
};
