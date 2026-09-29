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
import { Loader2 } from 'lucide-react';
import {
  Dialog,
  DialogContent,
  DialogHeader,
  DialogTitle,
  DialogDescription,
} from '~/components/ui/dialog';
import { Button } from '~/components/ui/button';
import { Kbd } from '~/components/ui/kbd';
import { Field, FieldLabel, FieldDescription, FieldControl } from '~/components/ui/field';
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
import { useKeyboardShortcuts, getModifierKey } from '~/hooks/useKeyboardShortcuts';
import {
  DEFAULT_PARTITION_VALUE,
  createEmptyVectorEntry,
  convertVectorDraftToString,
  ensureCoreEntries,
  parseVectorDraft,
} from '~/features/queue-management/utils/capacityEditor';
import { computeRemainingHelper } from '../utils/capacityRemainingHelper';
import { RemainingHelperDisplay } from './RemainingHelperDisplay';
import { CapacityRowEditor } from './CapacityRowEditor';
import type { CapacityResourceMode, CapacityRowDraft } from '~/stores/slices/capacityEditorSlice';
import { SPECIAL_VALUES } from '~/types';

type VectorTarget = 'capacity' | 'maxCapacity';

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

  const remainingHelper = computeRemainingHelper({
    rows,
    parentCapacityValue,
    isLegacyMode,
    parentQueuePath,
    selectedNodeLabel,
    getQueuePartitionCapacities,
  });

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

          {remainingHelper && <RemainingHelperDisplay helper={remainingHelper} />}
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

              return (
                <CapacityRowEditor
                  key={row.queuePath}
                  row={row}
                  capacityIssues={capacityIssuesForRow}
                  maxCapacityIssues={maxIssuesForRow}
                  isLegacyMode={isLegacyMode}
                  onModeChange={(mode) => handleModeChange(row.queuePath, mode)}
                  onCapacityChange={(value) => {
                    updateCapacityDraft(row.queuePath, (draft) => {
                      draft.capacityValue = value;
                    });
                  }}
                  onMaxCapacityChange={(value) => {
                    updateCapacityDraft(row.queuePath, (draft) => {
                      draft.maxCapacityValue = value;
                    });
                  }}
                  onVectorEntryChange={(target, entryId, field, value) =>
                    handleVectorEntryChange(row.queuePath, target, entryId, field, value)
                  }
                  onAddVectorEntry={(target) => handleAddVectorEntry(row.queuePath, target)}
                  onRemoveVectorEntry={(target, entryId) =>
                    handleRemoveVectorEntry(row.queuePath, target, entryId)
                  }
                />
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
