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


import React, { useState } from 'react';
import { Handle, Position, type NodeProps } from '@xyflow/react';

import {
  Card,
  CardContent,
  CardHeader,
  CardDescription,
  CardTitle,
  CardAction,
} from '~/components/ui/card';
import { Checkbox } from '~/components/ui/checkbox';
import {
  ContextMenu,
  ContextMenuContent,
  ContextMenuItem,
  ContextMenuSeparator,
  ContextMenuTrigger,
} from '~/components/ui/context-menu';
import { Tooltip, TooltipContent, TooltipProvider, TooltipTrigger } from '~/components/ui/tooltip';
import { Popover, PopoverContent, PopoverTrigger } from '~/components/ui/popover';
import {
  Plus,
  Trash2,
  Edit,
  Play,
  Pause,
  AlertCircle,
  AlertTriangle,
  SlidersHorizontal,
  FileCog,
} from 'lucide-react';
import type { QueueCardData } from '~/features/queue-management/hooks/useQueueTreeData';
import { useQueueActions } from '~/features/queue-management/hooks/useQueueActions';
import { useSchedulerStore } from '~/stores/schedulerStore';
import { cn } from '~/utils/cn';
import { HighlightedText } from '~/components/search/HighlightedText';
import { AddQueueDialog } from './dialogs/AddQueueDialog';
import { DeleteQueueDialog } from './dialogs/DeleteQueueDialog';
import { QueueCapacityProgress } from './QueueCapacityProgress';
import { QueueStatusBadges } from './QueueStatusBadges';
import { QueueResourceStats } from './QueueResourceStats';
import { QUEUE_STATES, SPECIAL_VALUES } from '~/types';
import { Badge } from '~/components/ui/badge';
import { parseCapacityValue as parseCapacityValueUtil } from '~/utils/capacityUtils';
import { useCapacityEditor } from '~/features/queue-management/hooks/useCapacityEditor';
import { QUEUE_CARD_HEIGHT, QUEUE_CARD_WIDTH } from '~/features/queue-management/constants';

type CapacityDisplay =
  | { type: 'vector'; entries: ResourceVectorEntry[]; raw: string }
  | { type: 'percentage'; formatted: string; raw: string }
  | { type: 'weight'; formatted: string; raw: string }
  | { type: 'unknown'; raw?: string };

type ResourceVectorEntry = {
  resource: string;
  value: string;
};

const PRIORITY_RESOURCES = ['memory', 'vcores'];
const INLINE_RESOURCE_LIMIT = 2;
const normalizeResourceKey = (resource: string) => resource.toLowerCase();
const createEntryMap = (entries: ResourceVectorEntry[]) => {
  const map = new Map<string, ResourceVectorEntry>();
  entries.forEach((entry) => {
    map.set(normalizeResourceKey(entry.resource), entry);
  });
  return map;
};
const getResourceOrder = (
  capacityEntries: ResourceVectorEntry[],
  maxEntries: ResourceVectorEntry[],
) => {
  const ordered: string[] = [];
  const seen = new Set<string>();
  const register = (entry?: ResourceVectorEntry) => {
    if (!entry) {
      return;
    }
    const key = normalizeResourceKey(entry.resource);
    if (seen.has(key)) {
      return;
    }
    seen.add(key);
    ordered.push(entry.resource);
  };
  PRIORITY_RESOURCES.forEach((priority) => {
    const match =
      capacityEntries.find((entry) => normalizeResourceKey(entry.resource) === priority) ??
      maxEntries.find((entry) => normalizeResourceKey(entry.resource) === priority);
    register(match);
  });
  capacityEntries.forEach((entry) => register(entry));
  maxEntries.forEach((entry) => register(entry));
  return ordered;
};

const parseResourceVector = (value: string): ResourceVectorEntry[] => {
  const trimmed = value.trim();
  if (!trimmed.startsWith('[') || !trimmed.endsWith(']')) {
    return [];
  }

  const inner = trimmed.slice(1, -1).trim();
  if (!inner) {
    return [];
  }

  return inner
    .split(',')
    .map((pair) => {
      const [resource, val] = pair.split('=');
      const resourceName = resource?.trim();
      const resourceValue = val?.trim();

      if (!resourceName || !resourceValue) {
        return null;
      }

      return {
        resource: resourceName,
        value: resourceValue,
      };
    })
    .filter((entry): entry is ResourceVectorEntry => entry !== null);
};

const getCapacityDisplay = (input?: string): CapacityDisplay => {
  if (!input) {
    return { type: 'unknown', raw: input };
  }

  const trimmed = input.trim();
  if (!trimmed) {
    return { type: 'unknown', raw: trimmed };
  }

  const parsed = parseCapacityValueUtil(trimmed);

  if (trimmed.startsWith('[') && trimmed.endsWith(']')) {
    return {
      type: 'vector',
      entries: parseResourceVector(trimmed),
      raw: trimmed,
    };
  }

  if (!parsed) {
    return { type: 'unknown', raw: trimmed };
  }

  switch (parsed.type) {
    case 'percentage': {
      const formatted = trimmed.endsWith('%') ? trimmed : `${parsed.value}%`;
      return { type: 'percentage', formatted, raw: trimmed };
    }
    case 'weight': {
      const formatted = trimmed.endsWith('w') ? trimmed : `${parsed.value}w`;
      return { type: 'weight', formatted, raw: trimmed };
    }
    case 'absolute': {
      return {
        type: 'vector',
        entries: parseResourceVector(trimmed),
        raw: trimmed,
      };
    }
    default:
      return { type: 'unknown', raw: trimmed };
  }
};

export const QueueCardNode: React.FC<NodeProps> = ({ data }) => {
  const [addDialogOpen, setAddDialogOpen] = useState(false);
  const [deleteDialogOpen, setDeleteDialogOpen] = useState(false);

  // Cast data to QueueCardData type
  const queueData = data as QueueCardData;

  const {
    comparisonQueues,
    selectedQueuePath,
    selectQueue,
    setPropertyPanelOpen,
    isPropertyPanelOpen,
    setPropertyPanelInitialTab,
    requestTemplateConfigOpen,
    toggleComparisonQueue,
    selectedNodeLabelFilter,
    getQueueLabelCapacity,
    clearQueueChanges,
    hasPendingDeletion,
    searchQuery,
  } = useSchedulerStore();

  const { canAddChildQueue, canDeleteQueue, updateQueueProperty } = useQueueActions();
  const { openCapacityEditor } = useCapacityEditor();

  const {
    queuePath,
    queueName,
    capacity,
    maxCapacity,
    state,
    usedCapacity,
    numApplications,
    resourcesUsed,
    stagedStatus,
    capacityConfig,
    maxCapacityConfig,
    stagedState,
    autoCreationStatus,
    validationErrors,
    isAffectedByErrors,
    errorSource,
    creationMethod,
    isAutoCreatedQueue,
  } = queueData;

  const isSelectedForComparison = comparisonQueues.includes(queuePath);
  const isSelectedQueue = selectedQueuePath === queuePath;

  // Get label-specific capacity information
  const labelCapacityInfo = getQueueLabelCapacity(queuePath, selectedNodeLabelFilter);
  const isAccessible = labelCapacityInfo?.canUseLabel ?? true; // For DEFAULT label
  const isRoot = queuePath === 'root';
  const shouldGrayOut = !isRoot && !isAccessible && selectedNodeLabelFilter !== '';

  // Use label-specific capacity if a label is selected, otherwise use default
  const displayCapacity = labelCapacityInfo?.isLabelSpecific
    ? labelCapacityInfo.capacity
    : capacityConfig;
  const displayMaxCapacity = labelCapacityInfo?.isLabelSpecific
    ? labelCapacityInfo.maxCapacity
    : maxCapacityConfig;

  const parsedCapacityMode = parseCapacityValueUtil(displayCapacity);
  const capacityMode: 'percentage' | 'weight' | 'absolute' =
    parsedCapacityMode?.type ?? 'percentage';
  const parsedCapacityDisplay = getCapacityDisplay(displayCapacity);
  const parsedMaxCapacityDisplay = getCapacityDisplay(displayMaxCapacity);
  const showVectorCapacity =
    parsedCapacityDisplay.type === 'vector' || parsedMaxCapacityDisplay.type === 'vector';
  const canAdd = canAddChildQueue(queuePath);
  const canDelete = canDeleteQueue(queuePath);
  const canToggleState = queuePath !== SPECIAL_VALUES.ROOT_QUEUE_NAME;
  const isRunning = state === QUEUE_STATES.RUNNING;
  const isTemplateManageable =
    autoCreationStatus?.status === 'legacy' || autoCreationStatus?.status === 'flexible';
  const isTemplateActionDisabled = stagedStatus === 'new' || isAutoCreatedQueue;

  const capacityEntries: ResourceVectorEntry[] =
    parsedCapacityDisplay.type === 'vector' ? parsedCapacityDisplay.entries : [];
  const maxCapacityEntries: ResourceVectorEntry[] =
    parsedMaxCapacityDisplay.type === 'vector' ? parsedMaxCapacityDisplay.entries : [];
  const capacityEntryMap = createEntryMap(capacityEntries);
  const maxCapacityEntryMap = createEntryMap(maxCapacityEntries);
  const resourceOrder = getResourceOrder(capacityEntries, maxCapacityEntries);
  const inlineResourceNames = resourceOrder.slice(0, INLINE_RESOURCE_LIMIT);
  const overflowResourceNames = resourceOrder.slice(INLINE_RESOURCE_LIMIT);
  const hasOverflowResources = overflowResourceNames.length > 0;
  const getInlineBadges = (entryMap: Map<string, ResourceVectorEntry>) => {
    const badges: React.ReactNode[] = [];
    inlineResourceNames.forEach((resourceName) => {
      const entry = entryMap.get(normalizeResourceKey(resourceName));
      if (!entry) {
        return;
      }
      badges.push(
        <Badge
          key={`inline-${entry.resource}-${entry.value}`}
          variant="outline"
          className="px-1.5 py-0.5 text-[11px] leading-tight font-medium whitespace-normal break-all"
        >
          {entry.resource}: {entry.value}
        </Badge>,
      );
    });
    return badges;
  };
  const capacityInlineBadges = getInlineBadges(capacityEntryMap);
  const maxCapacityInlineBadges = getInlineBadges(maxCapacityEntryMap);
  const overflowSummaryBadge = hasOverflowResources ? (
    <Popover>
      <PopoverTrigger asChild>
        <Badge
          asChild
          variant="outline"
          className="px-1.5 py-0.5 text-[11px] leading-tight font-medium cursor-pointer"
        >
          <button
            type="button"
            onClick={(event) => event.stopPropagation()}
            onMouseDown={(event) => event.stopPropagation()}
            onPointerDown={(event) => event.stopPropagation()}
            aria-label={`Show ${overflowResourceNames.length} additional resource${
              overflowResourceNames.length === 1 ? '' : 's'
            }`}
          >
            +{overflowResourceNames.length} more
          </button>
        </Badge>
      </PopoverTrigger>
      <PopoverContent align="end" className="w-80">
        <div className="space-y-3">
          <div>
            <p className="text-sm font-medium">Resource capacity details</p>
            <p className="text-xs text-muted-foreground">
              Review the full capacity and maximum capacity values.
            </p>
          </div>
          <div className="grid grid-cols-[1.2fr_1fr_1fr] gap-x-3 gap-y-2 text-sm">
            <span className="text-xs uppercase tracking-wide text-muted-foreground">resource</span>
            <span className="text-xs uppercase tracking-wide text-muted-foreground text-right">
              capacity
            </span>
            <span className="text-xs uppercase tracking-wide text-muted-foreground text-right">
              max
            </span>
            {resourceOrder.map((resourceName) => {
              const key = normalizeResourceKey(resourceName);
              const capacityEntry = capacityEntryMap.get(key);
              const maxEntry = maxCapacityEntryMap.get(key);
              const displayName = capacityEntry?.resource ?? maxEntry?.resource ?? resourceName;
              return (
                <React.Fragment key={`resource-${key}`}>
                  <span className="font-medium text-foreground">{displayName}</span>
                  <span className="text-right tabular-nums">{capacityEntry?.value ?? '—'}</span>
                  <span className="text-right tabular-nums">{maxEntry?.value ?? '—'}</span>
                </React.Fragment>
              );
            })}
          </div>
        </div>
      </PopoverContent>
    </Popover>
  ) : null;

  const openPropertyPanel = (
    event: React.MouseEvent,
    initialTab: 'overview' | 'info' | 'settings' = 'overview',
  ) => {
    event.stopPropagation();

    // Don't allow clicking on newly added queues that haven't been applied yet
    if (stagedStatus === 'new') {
      return;
    }

    const tabToOpen = isAutoCreatedQueue && initialTab === 'settings' ? 'overview' : initialTab;
    setPropertyPanelInitialTab(tabToOpen);
    // Set selected queue and open property panel
    selectQueue(queuePath);
    setPropertyPanelOpen(true);
  };

  const handleOpenCapacityEditor = (event: React.MouseEvent) => {
    event.stopPropagation();
    if (!queuePath || queuePath === SPECIAL_VALUES.ROOT_QUEUE_NAME) {
      return;
    }

    const parentPath = queuePath.split('.').slice(0, -1).join('.');
    if (!parentPath) {
      return;
    }

    openCapacityEditor({
      origin: 'context-menu',
      parentQueuePath: parentPath,
      originQueuePath: queuePath,
      originQueueName: queueName,
      capacityValue: capacityConfig,
      maxCapacityValue: maxCapacityConfig,
      queueState: state,
      markOriginAsNew: stagedStatus === 'new',
    });
  };

  const handleRemoveStagedQueue = (event: React.MouseEvent) => {
    event.stopPropagation();
    event.preventDefault();
    if (queuePath) {
      clearQueueChanges(queuePath);
    }
  };

  const handleComparisonToggle = () => {
    toggleComparisonQueue(queuePath);
  };

  const handleToggleState = () => {
    const newState = isRunning ? QUEUE_STATES.STOPPED : QUEUE_STATES.RUNNING;
    updateQueueProperty(queuePath, 'state', newState);
  };

  const cardContent = (
    <Card
      className={cn(
        'relative transition-all duration-200 flex flex-col',
        // Enhanced background and border for better contrast
        'bg-gray-50 dark:bg-gray-900 border-gray-300 dark:border-gray-700',
        isAutoCreatedQueue &&
          'border-amber-400 dark:border-amber-500 border-2 border-dashed bg-amber-50/70 dark:bg-amber-900/30',
        // Shadow for depth - stronger in light mode
        'shadow-lg hover:shadow-xl dark:shadow-md dark:hover:shadow-lg',
        // Cursor styling - not clickable for new queues
        stagedStatus === 'new' ? 'opacity-75 cursor-default' : 'cursor-pointer',
        // Border styling based on status
        stagedStatus === 'new' && 'ring-2 ring-queue-new',
        stagedStatus === 'deleted' && 'ring-2 ring-queue-deleted',
        stagedStatus === 'modified' && 'ring-2 ring-queue-modified',
        !stagedStatus && isSelectedQueue && 'ring-2 ring-primary',
        // Validation error styling
        validationErrors &&
          validationErrors.some((e) => e.severity === 'error') &&
          'ring-2 ring-destructive',
        isAffectedByErrors && !validationErrors && 'ring-2 ring-amber-500',
        // Background styling for states
        isSelectedQueue && 'bg-blue-200 dark:bg-gray-800',
        isSelectedForComparison && !isSelectedQueue && 'bg-gray-200 dark:bg-gray-700',
        // Gray out inaccessible queues when filtered by label
        shouldGrayOut && 'opacity-50 grayscale',
        'gap-4 py-5',
      )}
      onClick={(event) => openPropertyPanel(event, 'overview')}
      style={{ width: QUEUE_CARD_WIDTH, height: QUEUE_CARD_HEIGHT }}
    >
      <CardHeader className="px-5 pb-3 gap-1">
        <div className="flex items-start justify-between">
          <div className="flex-1">
            <CardTitle className="text-base truncate">
              {searchQuery ? (
                <HighlightedText text={queueName} highlight={searchQuery} />
              ) : (
                queueName
              )}
            </CardTitle>
            <CardDescription>
              {searchQuery ? (
                <HighlightedText text={queuePath} highlight={searchQuery} />
              ) : (
                queuePath
              )}
            </CardDescription>

            <CardDescription>
              <QueueStatusBadges
                capacityMode={capacityMode}
                state={state}
                stagedState={stagedState}
                stagedStatus={stagedStatus}
                autoCreationStatus={autoCreationStatus}
                creationMethod={creationMethod}
                labelInfo={
                  labelCapacityInfo
                    ? {
                        isLabelSpecific: labelCapacityInfo.isLabelSpecific,
                        label: labelCapacityInfo.label,
                      }
                    : undefined
                }
              />
            </CardDescription>
          </div>

          {/* Validation error indicators */}
          {(validationErrors || isAffectedByErrors) && (
            <div className="flex items-center gap-1.5 ml-2">
              {/* Direct errors badge */}
              {validationErrors &&
                validationErrors.filter((e) => e.severity === 'error').length > 0 && (
                  <TooltipProvider>
                    <Tooltip>
                      <TooltipTrigger>
                        <Badge variant="destructive" className="h-6 px-2">
                          <AlertCircle className="h-3 w-3 mr-1" />
                          {validationErrors.filter((e) => e.severity === 'error').length}
                        </Badge>
                      </TooltipTrigger>
                      <TooltipContent className="max-w-xs">
                        <p className="font-semibold mb-1">Validation Errors</p>
                        <ul className="text-sm space-y-1">
                          {validationErrors
                            .filter((e) => e.severity === 'error')
                            .map((error) => (
                              <li key={`${error.field}-${error.message}`}>• {error.message}</li>
                            ))}
                        </ul>
                      </TooltipContent>
                    </Tooltip>
                  </TooltipProvider>
                )}

              {/* Direct warnings badge */}
              {validationErrors &&
                validationErrors.filter((e) => e.severity === 'warning').length > 0 && (
                  <TooltipProvider>
                    <Tooltip>
                      <TooltipTrigger>
                        <Badge
                          variant="outline"
                          className="h-6 px-2 border-amber-500 text-amber-600 dark:text-amber-400 bg-amber-50 dark:bg-amber-950/30"
                        >
                          <AlertTriangle className="h-3 w-3 mr-1" />
                          {validationErrors.filter((e) => e.severity === 'warning').length}
                        </Badge>
                      </TooltipTrigger>
                      <TooltipContent className="max-w-xs">
                        <p className="font-semibold mb-1">Validation Warnings</p>
                        <ul className="text-sm space-y-1">
                          {validationErrors
                            .filter((e) => e.severity === 'warning')
                            .map((warning) => (
                              <li key={`${warning.field}-${warning.message}`}>
                                • {warning.message}
                              </li>
                            ))}
                        </ul>
                      </TooltipContent>
                    </Tooltip>
                  </TooltipProvider>
                )}

              {/* Affected by child issues badge */}
              {isAffectedByErrors && (
                <TooltipProvider>
                  <Tooltip>
                    <TooltipTrigger>
                      <Badge
                        variant="outline"
                        className="h-6 px-2 border-orange-500 text-orange-600 dark:text-orange-400 bg-orange-50 dark:bg-orange-950/30"
                      >
                        <AlertTriangle className="h-3 w-3 mr-1" />
                        Child
                      </Badge>
                    </TooltipTrigger>
                    <TooltipContent className="max-w-xs">
                      <p className="font-semibold mb-1">Affected by Child Queue Changes</p>
                      <p className="text-sm">
                        This queue is affected by validation issues from{' '}
                        {errorSource ? `queue "${errorSource}"` : 'child queues'}.
                      </p>
                    </TooltipContent>
                  </Tooltip>
                </TooltipProvider>
              )}
            </div>
          )}
        </div>

        <CardAction>
          <Checkbox
            checked={isSelectedForComparison}
            onCheckedChange={handleComparisonToggle}
            onClick={(e) => e.stopPropagation()}
            className="h-5 w-5 border-2"
            disabled={false}
          />
        </CardAction>
      </CardHeader>

      <CardContent className="px-5 pt-0 pb-4 flex-1 flex flex-col">
        <div className="space-y-2">
          {/* Capacity info */}
          <div>
            {showVectorCapacity ? (
              <div className="flex flex-col gap-1">
                <div className="flex flex-wrap items-center gap-1.5">
                  <span className="text-xs uppercase tracking-wide text-muted-foreground">
                    capacity
                  </span>
                  <div className="flex-1 min-w-[120px]">
                    {parsedCapacityDisplay.type === 'vector' ? (
                      capacityInlineBadges.length > 0 ? (
                        <div className="flex flex-wrap gap-1">{capacityInlineBadges}</div>
                      ) : (
                        <span className="text-xs text-muted-foreground">N/A</span>
                      )
                    ) : (
                      <span className="text-sm font-medium">
                        {parsedCapacityDisplay.type === 'percentage' ||
                        parsedCapacityDisplay.type === 'weight'
                          ? parsedCapacityDisplay.formatted
                          : 'N/A'}
                      </span>
                    )}
                  </div>
                </div>
                <div className="flex flex-wrap items-center gap-1.5">
                  <span className="text-xs uppercase tracking-wide text-muted-foreground">
                    max capacity
                  </span>
                  <div className="flex-1 min-w-[120px]">
                    {parsedMaxCapacityDisplay.type === 'vector' ? (
                      maxCapacityInlineBadges.length > 0 ? (
                        <div className="flex flex-wrap gap-1">{maxCapacityInlineBadges}</div>
                      ) : (
                        <span className="text-xs text-muted-foreground">N/A</span>
                      )
                    ) : (
                      <span className="text-sm font-medium text-muted-foreground">
                        {parsedMaxCapacityDisplay.type === 'percentage' ||
                        parsedMaxCapacityDisplay.type === 'weight'
                          ? parsedMaxCapacityDisplay.formatted
                          : 'N/A'}
                      </span>
                    )}
                  </div>
                </div>
                {overflowSummaryBadge && (
                  <div className="flex justify-end pt-0.5">{overflowSummaryBadge}</div>
                )}
              </div>
            ) : (
              <>
                <div className="flex items-baseline gap-1">
                  <span className="text-2xl font-bold">
                    {parsedCapacityDisplay.type === 'percentage' ||
                    parsedCapacityDisplay.type === 'weight'
                      ? parsedCapacityDisplay.formatted
                      : 'N/A'}
                  </span>
                  <span className="text-sm text-muted-foreground">capacity</span>
                </div>
                <div className="text-xs text-muted-foreground">
                  Maximum capacity:{' '}
                  {parsedMaxCapacityDisplay.type === 'percentage' ||
                  parsedMaxCapacityDisplay.type === 'weight'
                    ? parsedMaxCapacityDisplay.formatted
                    : 'N/A'}
                </div>
              </>
            )}
          </div>

          {/* Show why queue is inaccessible */}
          {shouldGrayOut && (
            <div className="text-xs text-muted-foreground">
              {labelCapacityInfo?.hasAccess && parseFloat(labelCapacityInfo.capacity) === 0
                ? `No capacity allocated for partition: ${selectedNodeLabelFilter}`
                : `No access to partition: ${selectedNodeLabelFilter}`}
            </div>
          )}
        </div>

        <div className="mt-auto space-y-2.5 pt-2">
          <QueueCapacityProgress
            capacity={capacity}
            maxCapacity={maxCapacity}
            usedCapacity={usedCapacity}
          />

          <div className="border-t border-border" />

          <QueueResourceStats numApplications={numApplications} resourcesUsed={resourcesUsed} />
        </div>
      </CardContent>

      <Handle
        type="target"
        position={Position.Left}
        className="!bg-transparent !border-none !w-0.5 h-full !left-[-1px] !top-1/2 !-translate-y-1/2"
      />
      <Handle
        type="source"
        position={Position.Right}
        className="!bg-transparent !border-none !w-0.5 h-full !right-[-1px] !top-1/2 !-translate-y-1/2"
      />
    </Card>
  );

  return (
    <>
      <ContextMenu
        onOpenChange={(open) => {
          // Deselect queue when context menu closes
          if (!open && isSelectedQueue && !isPropertyPanelOpen) {
            selectQueue(null);
          }
        }}
      >
        {stagedStatus === 'new' ? (
          <TooltipProvider>
            <Tooltip>
              <ContextMenuTrigger asChild>
                <TooltipTrigger asChild>{cardContent}</TooltipTrigger>
              </ContextMenuTrigger>
              <TooltipContent>
                <p>This queue must be applied before it can be edited</p>
              </TooltipContent>
            </Tooltip>
          </TooltipProvider>
        ) : (
          <ContextMenuTrigger asChild>{cardContent}</ContextMenuTrigger>
        )}

        <ContextMenuContent className="w-48">
          <ContextMenuItem
            onClick={(e) => {
              e.stopPropagation();
              openPropertyPanel(e, 'settings');
            }}
            disabled={stagedStatus === 'new' || isAutoCreatedQueue}
          >
            <Edit className="mr-2 h-4 w-4" />
            Edit Properties
          </ContextMenuItem>

          {isTemplateManageable && (
            <ContextMenuItem
              onClick={(e) => {
                e.stopPropagation();
                setPropertyPanelInitialTab('settings');
                selectQueue(queuePath);
                requestTemplateConfigOpen();
              }}
              disabled={isTemplateActionDisabled}
            >
              <FileCog className="mr-2 h-4 w-4" />
              Manage Template Properties
            </ContextMenuItem>
          )}

          {queuePath !== SPECIAL_VALUES.ROOT_QUEUE_NAME && (
            <ContextMenuItem onClick={(e) => handleOpenCapacityEditor(e)}>
              <SlidersHorizontal className="mr-2 h-4 w-4" />
              Capacity Editor
            </ContextMenuItem>
          )}

          {stagedStatus === 'new' && queuePath !== SPECIAL_VALUES.ROOT_QUEUE_NAME && (
            <ContextMenuItem
              onClick={handleRemoveStagedQueue}
              className="text-red-600 focus:text-red-600"
            >
              <Trash2 className="mr-2 h-4 w-4" />
              Remove Staged Queue
            </ContextMenuItem>
          )}

          <ContextMenuItem
            onClick={(e) => {
              e.stopPropagation();
              handleToggleState();
            }}
            disabled={!canToggleState}
          >
            {isRunning ? (
              <>
                <Pause className="mr-2 h-4 w-4" />
                Stop Queue
              </>
            ) : (
              <>
                <Play className="mr-2 h-4 w-4" />
                Start Queue
              </>
            )}
          </ContextMenuItem>

          {canAdd && stagedStatus !== 'new' && (
            <TooltipProvider>
              <Tooltip>
                <TooltipTrigger asChild>
                  <ContextMenuItem
                    onClick={(e) => {
                      e.stopPropagation();
                      setAddDialogOpen(true);
                    }}
                    disabled={hasPendingDeletion(queuePath)}
                  >
                    <Plus className="mr-2 h-4 w-4" />
                    Add Child Queue
                  </ContextMenuItem>
                </TooltipTrigger>
                {hasPendingDeletion(queuePath) && (
                  <TooltipContent>
                    <p>Cannot add children to queue pending deletion</p>
                  </TooltipContent>
                )}
              </Tooltip>
            </TooltipProvider>
          )}

          {canDelete && stagedStatus !== 'new' && (
            <>
              <ContextMenuSeparator />
              <ContextMenuItem
                onClick={(e) => {
                  e.stopPropagation();
                  setDeleteDialogOpen(true);
                }}
                className="text-red-600 focus:text-red-600"
              >
                <Trash2 className="mr-2 h-4 w-4" />
                Delete Queue
              </ContextMenuItem>
            </>
          )}
        </ContextMenuContent>
      </ContextMenu>

      <AddQueueDialog
        open={addDialogOpen}
        parentQueuePath={queuePath}
        onClose={() => setAddDialogOpen(false)}
      />

      <DeleteQueueDialog
        open={deleteDialogOpen}
        queuePath={queuePath}
        onClose={() => setDeleteDialogOpen(false)}
      />
    </>
  );
};
