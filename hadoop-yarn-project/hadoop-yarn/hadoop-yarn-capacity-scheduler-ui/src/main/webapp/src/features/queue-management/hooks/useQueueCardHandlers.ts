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
import { useSchedulerStore } from '~/stores/schedulerStore';
import { useQueueActions } from '~/features/queue-management/hooks/useQueueActions';
import { useCapacityEditor } from '~/features/queue-management/hooks/useCapacityEditor';
import { QUEUE_STATES, SPECIAL_VALUES } from '~/types';

interface UseQueueCardHandlersParams {
  queuePath: string;
  queueName: string;
  state: string;
  capacityConfig: string;
  maxCapacityConfig: string;
  stagedStatus: string | undefined;
  isAutoCreatedQueue: boolean;
  isComparisonModeActive: boolean;
  isSelectedQueue: boolean;
  isPropertyPanelOpen: boolean;
}

export function useQueueCardHandlers(params: UseQueueCardHandlersParams) {
  const {
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
  } = params;

  const selectQueue = useSchedulerStore((s) => s.selectQueue);
  const setPropertyPanelOpen = useSchedulerStore((s) => s.setPropertyPanelOpen);
  const setPropertyPanelInitialTab = useSchedulerStore((s) => s.setPropertyPanelInitialTab);
  const requestTemplateConfigOpen = useSchedulerStore((s) => s.requestTemplateConfigOpen);
  const toggleComparisonQueue = useSchedulerStore((s) => s.toggleComparisonQueue);
  const clearQueueChanges = useSchedulerStore((s) => s.clearQueueChanges);

  const { updateQueueProperty } = useQueueActions();
  const { openCapacityEditor } = useCapacityEditor();

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
    const newState = state === QUEUE_STATES.RUNNING ? QUEUE_STATES.STOPPED : QUEUE_STATES.RUNNING;
    updateQueueProperty(queuePath, 'state', newState);
  };

  const handleManageTemplate = (event: React.MouseEvent) => {
    event.stopPropagation();
    setPropertyPanelInitialTab('settings');
    selectQueue(queuePath);
    requestTemplateConfigOpen();
  };

  const handleContextMenuOpenChange = (open: boolean) => {
    if (!open && isSelectedQueue && !isPropertyPanelOpen) {
      selectQueue(null);
    }
  };

  const handleCardClick = (event: React.MouseEvent) => {
    if (isComparisonModeActive) {
      handleComparisonToggle();
    } else {
      openPropertyPanel(event, 'overview');
    }
  };

  return {
    openPropertyPanel,
    handleOpenCapacityEditor,
    handleRemoveStagedQueue,
    handleComparisonToggle,
    handleToggleState,
    handleManageTemplate,
    handleContextMenuOpenChange,
    handleCardClick,
  };
}
