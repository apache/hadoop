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
import { useShallow } from 'zustand/react/shallow';

import {
  Card,
  CardContent,
  CardHeader,
  CardDescription,
  CardTitle,
  CardAction,
} from '~/components/ui/card';
import { Checkbox } from '~/components/ui/checkbox';
import type { QueueCardData } from '~/features/queue-management/hooks/useQueueTreeData';
import { useQueueActions } from '~/features/queue-management/hooks/useQueueActions';
import { useSchedulerStore } from '~/stores/schedulerStore';
import { HighlightedText } from '~/components/search/HighlightedText';
import { AddQueueDialog } from './dialogs/AddQueueDialog';
import { DeleteQueueDialog } from './dialogs/DeleteQueueDialog';
import { QueueCapacityProgress } from './QueueCapacityProgress';
import { QueueStatusBadges } from './QueueStatusBadges';
import { QueueResourceStats } from './QueueResourceStats';
import { QueueValidationBadges } from './QueueValidationBadges';
import { QueueVectorCapacityDisplay } from './QueueVectorCapacityDisplay';
import { QueueCardContextMenu } from './QueueCardContextMenu';
import { getCapacityDisplay } from '../utils/capacityDisplay';
import { parseCapacityValue } from '~/utils/capacityUtils';
import { getQueueCardClassName } from '../utils/queueCardStyles';
import { useQueueCardHandlers } from '../hooks/useQueueCardHandlers';
import { QUEUE_CARD_HEIGHT, QUEUE_CARD_WIDTH } from '~/features/queue-management/constants';

export const QueueCardNode: React.FC<NodeProps> = ({ data }) => {
  const [addDialogOpen, setAddDialogOpen] = useState(false);
  const [deleteDialogOpen, setDeleteDialogOpen] = useState(false);

  // Cast data to QueueCardData type
  const queueData = data as QueueCardData;

  // State values (trigger re-renders only when these specific values change)
  const {
    comparisonQueues,
    selectedQueuePath,
    isPropertyPanelOpen,
    selectedNodeLabelFilter,
    searchQuery,
    isComparisonModeActive,
  } = useSchedulerStore(
    useShallow((s) => ({
      comparisonQueues: s.comparisonQueues,
      selectedQueuePath: s.selectedQueuePath,
      isPropertyPanelOpen: s.isPropertyPanelOpen,
      selectedNodeLabelFilter: s.selectedNodeLabelFilter,
      searchQuery: s.searchQuery,
      isComparisonModeActive: s.isComparisonModeActive,
    })),
  );

  // Actions (stable references, never trigger re-renders)
  const getQueueLabelCapacity = useSchedulerStore((s) => s.getQueueLabelCapacity);
  const revertQueueDeletion = useSchedulerStore((state) => state.revertQueueDeletion);

  // Derived state (re-renders when deletion status changes)
  const isPendingDeletion = useSchedulerStore((s) =>
    s.hasPendingDeletion((data as QueueCardData).queuePath),
  );

  const { canAddChildQueue, canDeleteQueue } = useQueueActions();

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

  const parsedCapacityMode = parseCapacityValue(displayCapacity);
  const capacityMode: 'percentage' | 'weight' | 'absolute' =
    parsedCapacityMode?.type ?? 'percentage';
  const parsedCapacityDisplay = getCapacityDisplay(displayCapacity);
  const parsedMaxCapacityDisplay = getCapacityDisplay(displayMaxCapacity);
  const showVectorCapacity =
    parsedCapacityDisplay.type === 'vector' || parsedMaxCapacityDisplay.type === 'vector';

  const canAdd = canAddChildQueue(queuePath);
  const canDelete = canDeleteQueue(queuePath);
  const isTemplateManageable =
    autoCreationStatus?.status === 'legacy' || autoCreationStatus?.status === 'flexible';

  const {
    openPropertyPanel,
    handleOpenCapacityEditor,
    handleRemoveStagedQueue,
    handleComparisonToggle,
    handleToggleState,
    handleManageTemplate,
    handleContextMenuOpenChange,
    handleCardClick,
  } = useQueueCardHandlers({
    queuePath,
    queueName,
    state,
    capacityConfig,
    maxCapacityConfig,
    stagedStatus,
    isAutoCreatedQueue,
    isComparisonModeActive,
    isSelectedQueue,
    isPropertyPanelOpen,
  });

  const cardClassName = getQueueCardClassName({
    isAutoCreatedQueue,
    stagedStatus,
    isSelectedQueue,
    isSelectedForComparison,
    validationErrors,
    isAffectedByErrors,
    shouldGrayOut,
  });

  const cardContent = (
    <Card
      className={cardClassName}
      onClick={handleCardClick}
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

          <QueueValidationBadges
            validationErrors={validationErrors}
            isAffectedByErrors={isAffectedByErrors}
            errorSource={errorSource}
          />
        </div>

        {isComparisonModeActive && (
          <CardAction>
            <Checkbox
              checked={isSelectedForComparison}
              onCheckedChange={handleComparisonToggle}
              onClick={(e) => e.stopPropagation()}
              className="h-5 w-5 border-2"
              disabled={false}
            />
          </CardAction>
        )}
      </CardHeader>

      <CardContent className="px-5 pt-0 pb-4 flex-1 flex flex-col">
        <div className="space-y-2">
          {/* Capacity info */}
          <div>
            {showVectorCapacity ? (
              <QueueVectorCapacityDisplay
                capacityDisplay={parsedCapacityDisplay}
                maxCapacityDisplay={parsedMaxCapacityDisplay}
              />
            ) : (
              <>
                <div className="flex items-baseline gap-1.5">
                  <span className="text-2xl font-bold tracking-tight">
                    {parsedCapacityDisplay.type === 'percentage' ||
                    parsedCapacityDisplay.type === 'weight'
                      ? parsedCapacityDisplay.formatted
                      : 'N/A'}
                  </span>
                  <span className="text-sm text-muted-foreground font-medium">capacity</span>
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
      <QueueCardContextMenu
        queuePath={queuePath}
        state={state}
        stagedStatus={stagedStatus}
        isAutoCreatedQueue={isAutoCreatedQueue}
        isTemplateManageable={isTemplateManageable}
        canAdd={canAdd}
        canDelete={canDelete}
        hasPendingDeletion={isPendingDeletion}
        isSelectedQueue={isSelectedQueue}
        isPropertyPanelOpen={isPropertyPanelOpen}
        onEditProperties={(e) => openPropertyPanel(e, 'settings')}
        onManageTemplate={handleManageTemplate}
        onEditCapacity={handleOpenCapacityEditor}
        onToggleState={handleToggleState}
        onAddChild={() => setAddDialogOpen(true)}
        onDelete={() => setDeleteDialogOpen(true)}
        onUndoDelete={() => revertQueueDeletion(queuePath)}
        onRemoveStaged={handleRemoveStagedQueue}
        onOpenChange={handleContextMenuOpenChange}
      >
        {cardContent}
      </QueueCardContextMenu>

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
